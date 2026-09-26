"""SOD pushes: ``eod.rollover`` to every shard, ``pushed_ns`` and re-push (补推) (02 §6.3, §6.5, §8.4 step 6).

A push delivers one snapshot version ``(next_trading_day, sod_version)`` to all
risk-engine shards with ``eod.rollover{trading_day, next_trading_day,
sod_version}``. Each shard fetches that version with ``settle.sod_snapshot``
(served concurrently by the clearing-worker while it waits here, 02 §8.4
"避免死锁"), rolls, rebases or records it (catch-up) and answers with a
``DayRoll``.

* Only when **every** shard confirmed does the push complete: the caller's
  ``finalize`` then writes ``sod_versions.pushed_ns`` (and, for EOD snapshots,
  ``eod_runs`` ROLLED plus ``sys.eod``) in one transaction (03 §7 推送完成).
  A confirmation is a ``DayRoll`` for ``next_trading_day`` with this (or a
  newer) version in mode ``ROLL`` (normal day change), ``REBASE`` (the shard is
  already on that day: seed correction, new settlement version, or the official
  SOD replacing its provisional one) or ``CATCH_UP`` (the shard is PROVISIONAL
  on a later day and records the official SOD of a skipped day).
* **One trading day at a time, oldest first**: the shards chain official SODs
  day by day (catch-up after missed EODs), so a push for a later day waits
  until every earlier day's push completed. Pushes are never superseded across
  days.
* Within one day the newest version wins: a higher version of the same day
  (``force_new_version``, a seed correction) supersedes the pending one. The
  superseded caller gets :attr:`PushOutcome.SUPERSEDED` once the newer version
  reached every shard (so "superseded" always means "the day is pushed").
* A shard that fails (timeout after the 02 §4.7 retries, not connected, error)
  stays pending: it is pushed again as soon as its heartbeat comes back
  (``watch_peers`` UP) and, as a safety net, every ``repush_interval_s``, until
  it confirms. The first failure calls the caller's ``on_stuck`` once (the
  saga turns it into a ``sys.eod`` WARN). Shards handle a version
  idempotently, so repeating a push has no side effect.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

import msgspec
from loguru import logger

from futrisk.messages.enums import DayRollMode
from futrisk.messages.eod import DayRoll, EodRolloverRequest
from futrisk.runtime import AsyncServiceRuntime
from futrisk.services.clearing.settings import ClearingSettings

Finalize = Callable[[int, list[str]], Awaitable[None]]
CONFIRMING_MODES = frozenset({DayRollMode.ROLL, DayRollMode.REBASE, DayRollMode.CATCH_UP})


class PushOutcome(StrEnum):
    PUSHED = "PUSHED"
    SUPERSEDED = "SUPERSEDED"


@dataclass
class PushTarget:
    """One snapshot version being pushed."""

    from_td: str
    next_td: str
    version: int
    source: str
    shards: list[str]
    finalize: Finalize
    done: asyncio.Future[PushOutcome]
    on_stuck: Callable[[PushTarget], None] | None = None
    confirmed: dict[str, DayRoll] = field(default_factory=dict)
    errors: dict[str, str] = field(default_factory=dict)
    attempts: dict[str, int] = field(default_factory=dict)
    inflight: dict[str, asyncio.Task[None]] = field(default_factory=dict)
    resend: set[str] = field(default_factory=set)
    superseded: list[asyncio.Future[PushOutcome]] = field(default_factory=list)
    started: bool = False
    finalizing: bool = False
    warned: bool = False

    @property
    def key(self) -> tuple[str, int]:
        return (self.next_td, self.version)

    @property
    def pending(self) -> list[str]:
        return [s for s in self.shards if s not in self.confirmed]

    def view(self) -> dict[str, Any]:
        return {
            "from_trading_day": self.from_td,
            "next_trading_day": self.next_td,
            "sod_version": self.version,
            "source": self.source,
            "confirmed": sorted(self.confirmed),
            "pending": self.pending,
            "errors": dict(self.errors),
            "modes": {s: r.mode.value for s, r in sorted(self.confirmed.items())},
        }


class PushManager:
    """Pushes SOD versions to every shard, day by day, and re-pushes to returning shards."""

    def __init__(self, rt: AsyncServiceRuntime, shards: list[str], settings: ClearingSettings) -> None:
        self._rt = rt
        self._shards = list(shards)
        self._settings = settings
        self._targets: dict[str, PushTarget] = {}  # next trading day -> newest version being pushed
        self._timer: asyncio.Task[None] | None = None
        self._finishers: set[asyncio.Task[None]] = set()
        self._closed = False

    # ----------------------------------------------------------------- state
    @property
    def shards(self) -> list[str]:
        return list(self._shards)

    @property
    def target(self) -> PushTarget | None:
        """The push in progress (the oldest pending trading day)."""
        pending = [t for d, t in sorted(self._targets.items()) if not t.done.done()]
        return pending[0] if pending else None

    def status(self) -> dict[str, Any] | None:
        t = self.target
        if t is None:
            return None
        view = t.view()
        queued = [f"{d}:v{x.version}" for d, x in sorted(self._targets.items()) if x is not t and not x.done.done()]
        if queued:
            view["queued"] = queued
        return view

    def start(self) -> None:
        if self._timer is None:
            self._timer = asyncio.create_task(self._repush_loop(), name="clearing:repush")

    async def stop(self) -> None:
        self._closed = True
        tasks: list[asyncio.Task[Any]] = []
        if self._timer is not None:
            self._timer.cancel()
            tasks.append(self._timer)
        for t in self._targets.values():
            for task in t.inflight.values():
                task.cancel()
                tasks.append(task)
        tasks.extend(self._finishers)
        for task in tasks:
            task.cancel()
        for task in tasks:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task

    # ------------------------------------------------------------------ push
    async def push(
        self,
        from_td: str,
        next_td: str,
        version: int,
        *,
        source: str,
        finalize: Finalize,
        on_stuck: Callable[[PushTarget], None] | None = None,
    ) -> PushOutcome:
        """Push ``(next_td, version)`` to every shard; returns once all confirmed or a newer version did."""
        cur = self._targets.get(next_td)
        while cur is not None and not cur.done.done():
            if cur.version == version:
                return await asyncio.shield(cur.done)
            if version < cur.version:
                logger.info("push.older_version", next_trading_day=next_td, version=version, current=cur.key)
                await asyncio.shield(cur.done)
                return PushOutcome.SUPERSEDED
            if not cur.finalizing:
                break
            await asyncio.shield(cur.done)  # every shard confirmed already: let pushed_ns commit first
            cur = self._targets.get(next_td)
        loop = asyncio.get_running_loop()
        target = PushTarget(
            from_td=from_td,
            next_td=next_td,
            version=version,
            source=source,
            shards=list(self._shards),
            finalize=finalize,
            done=loop.create_future(),
            on_stuck=on_stuck,
        )
        if cur is not None and not cur.done.done():
            self._supersede(cur, target)
        self._targets[next_td] = target
        logger.info("push.queued", from_trading_day=from_td, next_trading_day=next_td, version=version, source=source)
        self._pump()
        return await asyncio.shield(target.done)

    def _supersede(self, cur: PushTarget, new: PushTarget) -> None:
        """A newer version of the same day replaces ``cur``; its waiters complete with the newer push."""
        logger.warning("push.superseded", old=cur.key, new=new.key, pending=cur.pending)
        for task in cur.inflight.values():
            task.cancel()
        cur.inflight.clear()
        new.superseded.extend([cur.done, *cur.superseded])
        cur.superseded.clear()

    def _pump(self) -> None:
        """Send the oldest pending day; later days wait for it (02 §8.4: SODs chain day by day)."""
        if self._closed:
            return
        active = self.target
        if active is None:
            return
        for _day, t in sorted(self._targets.items()):
            if t is not active and t.started and not t.done.done():
                logger.info("push.paused_for_earlier_day", paused=t.key, earlier=active.key)
                for task in t.inflight.values():
                    task.cancel()
                t.inflight.clear()
                t.started = False
        if active.started:
            return
        active.started = True
        logger.info("push.start", from_trading_day=active.from_td, next_trading_day=active.next_td,
                    version=active.version, source=active.source)
        if not active.shards:
            self._spawn_finish(active)
        for shard in active.shards:
            self._send(active, shard)

    def _spawn_finish(self, target: PushTarget) -> None:
        task = asyncio.create_task(self._maybe_finish(target), name=f"clearing:push-finish:{target.next_td}")
        self._finishers.add(task)
        task.add_done_callback(self._finishers.discard)

    def on_peer_change(self, peer: str, state: str) -> None:
        """Heartbeat back (UP): re-push to that shard if it has not confirmed yet (补推)."""
        if state != "UP":
            return
        t = self.target
        if t is None or not t.started or peer not in t.shards or peer in t.confirmed:
            return
        logger.info("push.repush_on_heartbeat", shard=peer, next_trading_day=t.next_td, version=t.version)
        if peer in t.inflight:
            t.resend.add(peer)  # the running attempt may target the old process: send again when it ends
            return
        self._send(t, peer)

    async def _repush_loop(self) -> None:
        while True:
            await asyncio.sleep(self._settings.repush_interval_s)
            t = self.target
            if t is None or not t.started:
                continue
            for shard in t.pending:
                self._send(t, shard)

    def _send(self, target: PushTarget, shard: str) -> None:
        if (
            self._closed
            or not target.started
            or target.done.done()
            or shard in target.confirmed
            or shard in target.inflight
        ):
            return
        task = asyncio.create_task(self._send_one(target, shard), name=f"clearing:rollover:{shard}")
        target.inflight[shard] = task

        def _done(t: asyncio.Task[None]) -> None:
            if target.inflight.get(shard) is t:
                del target.inflight[shard]
            if not t.cancelled() and t.exception() is not None:
                logger.opt(exception=t.exception()).error("push.send_failed", shard=shard)
            if shard in target.resend:
                target.resend.discard(shard)
                if not t.cancelled() and shard not in target.confirmed:
                    self._send(target, shard)

        task.add_done_callback(_done)

    def _failed(self, target: PushTarget, shard: str, error: str) -> None:
        target.errors[shard] = error
        logger.warning("push.shard_failed", shard=shard, key=target.key, error=error)
        if not target.warned and target.on_stuck is not None:
            target.warned = True
            try:
                target.on_stuck(target)
            except Exception:
                logger.exception("push.on_stuck_failed")

    async def _send_one(self, target: PushTarget, shard: str) -> None:
        req = EodRolloverRequest(trading_day=target.from_td, next_trading_day=target.next_td, sod_version=target.version)
        target.attempts[shard] = target.attempts.get(shard, 0) + 1
        reply = await self._rt.request(
            shard,
            "eod.rollover",
            req,
            timeout_ms=self._settings.rollover_timeout_ms,
            retries=self._settings.rollover_retries,
        )
        if target.done.done() or self._targets.get(target.next_td) is not target or not target.started:
            return
        if not reply.ok:
            self._failed(target, shard, f"{reply.code}: {reply.message or ''}".strip())
            return
        roll: DayRoll | None = None
        if reply.data is not None:
            try:
                roll = reply.open_as(DayRoll)
            except (msgspec.DecodeError, msgspec.ValidationError):
                roll = None
        if roll is not None and not _confirms(roll, target):
            self._failed(
                target, shard, f"unexpected DayRoll mode={roll.mode} to={roll.to_trading_day} version={roll.sod_version}"
            )
            return
        if roll is None:
            logger.warning("push.confirm_without_dayroll", shard=shard, key=target.key)
            roll = DayRoll(
                shard=-1,
                mode=DayRollMode.ROLL,
                from_trading_day=target.from_td,
                to_trading_day=target.next_td,
                sod_version=target.version,
                accounts=[],
                ts_ns=self._rt.market_clock.now_ns(),
            )
        target.confirmed[shard] = roll
        target.errors.pop(shard, None)
        logger.info("push.shard_confirmed", shard=shard, mode=roll.mode.value, key=target.key)
        await self._maybe_finish(target)

    async def _maybe_finish(self, target: PushTarget) -> None:
        if target.finalizing or target.done.done() or target.pending:
            return
        target.finalizing = True
        ns = self._rt.market_clock.now_ns()
        try:
            await target.finalize(ns, sorted(target.confirmed))
        except asyncio.CancelledError:
            target.finalizing = False
            raise
        except Exception as exc:  # the DB write failed: the service must restart (01 §8.6)
            self._rt.fatal(exc, "push finalize")
            return
        logger.info("push.done", key=target.key, shards=sorted(target.confirmed))
        if not target.done.done():
            target.done.set_result(PushOutcome.PUSHED)
        for fut in target.superseded:
            if not fut.done():
                fut.set_result(PushOutcome.SUPERSEDED)
        target.superseded.clear()
        if self._targets.get(target.next_td) is target:
            del self._targets[target.next_td]
        self._pump()


def _confirms(roll: DayRoll, target: PushTarget) -> bool:
    """A ``DayRoll`` confirms the push when the shard holds ``next_td`` with this (or a newer) version."""
    if roll.mode not in CONFIRMING_MODES:
        return False
    return roll.to_trading_day == target.next_td and roll.sod_version >= target.version


__all__ = ["CONFIRMING_MODES", "Finalize", "PushManager", "PushOutcome", "PushTarget"]
