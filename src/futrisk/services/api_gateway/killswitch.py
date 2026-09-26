"""Manual kill switch: one persisted desired state, fanned out and reconciled (02 §6.3, §9.6).

``POST /risk/firm/kill-switch`` sends ``firm.kill_switch`` to every risk shard
**and** trade-gateway. Targets answer separately; one of them may be down.
The rules:

* there is exactly **one desired state** (on/off + reason + actor + role +
  ``command_id``), persisted in api.db (``kill_switch``) together with the
  confirmation of every target (``PENDING`` / ``CONFIRMED`` / ``FAILED``)
  **before** the command goes out;
* a new command **supersedes** every pending retry: the retry task is
  cancelled before the new fan-out, so an old decision can never overwrite a
  newer one (review H1);
* only retryable answers (``TIMEOUT`` / ``UNAVAILABLE`` / ``BUSY`` /
  ``RISK_BUSY``) are retried, every ``interval_s``, at most ``max_attempts``
  times per target, always with the **current** desired state (same
  ``command_id``, so targets deduplicate); any other answer, or retries running
  out, marks the target ``FAILED``: logged CRITICAL, notified CRITICAL (02 §9.8)
  and published as a WS ``system`` event;
* at start-up the persisted desired state is pushed again to every target that
  has not confirmed it (``PENDING`` or ``FAILED``).

The REST answer carries the per-target results; ``/system/status`` shows the
state (``kill_switch_control``).
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Awaitable, Callable
from typing import Any

from loguru import logger

from futrisk.common.clock import Clock
from futrisk.messages import codes
from futrisk.messages.risk import KillSwitchRequest
from futrisk.services.api_gateway.router import CommandRouter, TargetResult
from futrisk.services.api_gateway.store import ApiStore, KillSwitchRow, KillSwitchTarget, TargetStatus

COMMAND = "firm.kill_switch"
RETRY_INTERVAL_S = 2.0
MAX_ATTEMPTS = 30

EventHook = Callable[[dict[str, Any]], None]
FailedHook = Callable[[KillSwitchRow, str], Awaitable[None] | None]


def request_of(row: KillSwitchRow) -> KillSwitchRequest:
    """The ``firm.kill_switch`` payload of the desired state (same ``command_id`` on every resend)."""
    return KillSwitchRequest(command_id=row.command_id, on=row.desired_on, reason=row.reason, actor=row.actor,
                             role=row.role)


class KillSwitchManager:
    """Owns the desired kill-switch state, its fan-out and its retries."""

    def __init__(
        self,
        *,
        store: ApiStore,
        router: CommandRouter,
        clock: Clock,
        targets: Callable[[], list[str]],
        interval_s: float = RETRY_INTERVAL_S,
        max_attempts: int = MAX_ATTEMPTS,
        sleep: Callable[[float], Awaitable[Any]] = asyncio.sleep,
    ) -> None:
        self._store = store
        self._router = router
        self._clock = clock
        self._targets = targets
        self.interval_s = interval_s
        self.max_attempts = max(1, max_attempts)
        self._sleep = sleep
        self._lock = asyncio.Lock()
        self._task: asyncio.Task[None] | None = None
        self.row: KillSwitchRow | None = None
        self.on_event: EventHook | None = None
        self.on_failed: FailedHook | None = None

    # ------------------------------------------------------------------ state
    @property
    def retrying(self) -> bool:
        return self._task is not None and not self._task.done()

    def pending(self) -> list[str]:
        return self.row.with_status(TargetStatus.PENDING) if self.row is not None else []

    def failed(self) -> list[str]:
        return self.row.with_status(TargetStatus.FAILED) if self.row is not None else []

    def status(self) -> dict[str, Any] | None:
        """``/system/status`` view (None before the first command)."""
        if self.row is None:
            return None
        return self.row.to_dict() | {"retrying": self.retrying}

    async def load(self) -> KillSwitchRow | None:
        """Read the persisted desired state (start-up)."""
        self.row = await self._store.load_kill_switch()
        return self.row

    # ---------------------------------------------------------------- command
    async def command(self, req: KillSwitchRequest) -> list[TargetResult]:
        """Make ``req`` the desired state, fan it out and retry the targets that did not confirm."""
        async with self._lock:
            await self._cancel()  # a new decision supersedes every pending retry (H1)
            now = self._clock.now_ns()
            targets = list(dict.fromkeys(self._targets()))
            row = KillSwitchRow(
                desired_on=req.on,
                command_id=req.command_id,
                reason=req.reason,
                actor=req.actor,
                role=req.role,
                requested_ns=now,
                updated_ns=now,
                targets={t: KillSwitchTarget(updated_ns=now) for t in targets},
            )
            self.row = row
            await self._store.save_kill_switch(row)  # durable before it is announced
            logger.warning("kill_switch.command", on=req.on, command_id=req.command_id, actor=req.actor,
                           targets=targets)
            results = await self._router.fan_out(targets, COMMAND, req)
            failed = self._apply(row, results)
            await self._store.save_kill_switch(row)
            self._emit(row, "command")
            if row.with_status(TargetStatus.PENDING):
                self._start(row, self.interval_s)
        for target in failed:
            await self._target_failed(row, target)
        return results

    async def reconcile(self, *, first_delay_s: float = 1.0) -> list[str]:
        """Start-up: resend the persisted desired state to every target that has not confirmed it."""
        async with self._lock:
            row = self.row
            if row is None:
                return []
            todo = row.with_status(TargetStatus.PENDING, TargetStatus.FAILED)
            if not todo:
                return []
            now = self._clock.now_ns()
            for t in todo:
                old = row.targets[t]
                row.targets[t] = KillSwitchTarget(status=TargetStatus.PENDING, code=old.code, message=old.message,
                                                  updated_ns=now)
            row.updated_ns = now
            await self._store.save_kill_switch(row)
            logger.warning("kill_switch.reconcile", on=row.desired_on, command_id=row.command_id, targets=todo)
            self._emit(row, "reconcile")
            self._start(row, first_delay_s)
            return todo

    # ---------------------------------------------------------------- retries
    def _apply(self, row: KillSwitchRow, results: list[TargetResult]) -> list[str]:
        """Record the answers; returns the targets that became FAILED."""
        now = self._clock.now_ns()
        failed: list[str] = []
        for res in results:
            t = row.targets.setdefault(res.target, KillSwitchTarget())
            t.attempts += 1
            t.code = res.reply.code
            t.message = res.reply.message or ""
            t.updated_ns = now
            if res.reply.ok:
                t.status = TargetStatus.CONFIRMED
            elif res.reply.code in codes.RETRYABLE and t.attempts < self.max_attempts:
                t.status = TargetStatus.PENDING
                logger.error("kill_switch.target_unconfirmed", target=res.target, code=res.reply.code,
                             attempt=t.attempts)
            else:
                t.status = TargetStatus.FAILED
                failed.append(res.target)
        row.updated_ns = now
        return failed

    def _start(self, row: KillSwitchRow, first_delay_s: float) -> None:
        task = asyncio.ensure_future(self._retry(row, first_delay_s))
        task.set_name("kill-switch-retry")
        self._task = task

    async def _retry(self, row: KillSwitchRow, first_delay_s: float) -> None:
        delay = first_delay_s
        while True:
            await self._sleep(delay)
            delay = self.interval_s
            if self.row is not row:
                return  # superseded (the task is normally cancelled first)
            pending = row.with_status(TargetStatus.PENDING)
            if not pending:
                return
            results = await self._router.fan_out(pending, COMMAND, request_of(row))  # the CURRENT desired state
            if self.row is not row:
                return
            failed = self._apply(row, results)
            for res in results:
                if res.ok:
                    logger.info("kill_switch.retry_confirmed", target=res.target,
                                attempts=row.targets[res.target].attempts)
            await self._store.save_kill_switch(row)
            self._emit(row, "retry")
            for target in failed:
                await self._target_failed(row, target)

    async def _target_failed(self, row: KillSwitchRow, target: str) -> None:
        t = row.targets[target]
        logger.critical("kill_switch.target_failed", target=target, on=row.desired_on, command_id=row.command_id,
                        code=t.code, attempts=t.attempts)
        hook = self.on_failed
        if hook is None:
            return
        try:
            res = hook(row, target)
            if res is not None:
                await res
        except Exception:
            logger.exception("kill_switch.alert_failed", target=target)

    def _emit(self, row: KillSwitchRow, event: str) -> None:
        hook = self.on_event
        if hook is None:
            return
        try:
            hook({"kind": "kill_switch", "event": event, **row.to_dict()})
        except Exception:
            logger.exception("kill_switch.event_failed")

    async def _cancel(self) -> None:
        task, self._task = self._task, None
        if task is not None and not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task

    async def close(self) -> None:
        await self._cancel()


__all__ = ["COMMAND", "MAX_ATTEMPTS", "RETRY_INTERVAL_S", "KillSwitchManager", "request_of"]
