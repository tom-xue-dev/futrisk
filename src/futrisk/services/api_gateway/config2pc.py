"""ConfigCoordinator: config hot reload with two-phase commit, coordinator side (02 §8.5, §7.10; 01 §6.7).

Steps of one ``POST /config/reload`` (one round at a time):

1. copy the working directory ``<home>/config`` to the immutable snapshot
   ``config/versions/{N}/`` (``N`` = max(existing dirs, ``config_versions``) + 1),
   compute ``config_hash`` and persist ``config_versions`` = ``PREPARING``;
   the snapshot is validated locally first (pydantic), an invalid one is
   aborted without contacting anybody;
2. ``ctl.config.prepare`` to every internal instance of ``services.yaml``
   (including api-gateway itself, over its own ROUTER), 5 s timeout;
3. participants answer ``ConfigPrepared``;
4a. all ``ok`` -> **persist ``COMMITTING`` first**, then rewrite
    ``config/active.json`` (with the per-section ``effective`` of 02 §7.10: a
    ``NEXT_TRADING_DAY`` section records the trading day it applies from),
    then ``ctl.config.commit`` to every instance; all confirmed -> ``COMMITTED``;
4b. any failure or timeout -> **persist ``ABORTED`` first**, then
    ``ctl.config.abort`` to every instance that was asked to prepare;
5. participants that voted yes and heard nothing ask ``config.decision``
   (answered by :meth:`ConfigCoordinator.decision`): ``COMMITTING`` /
   ``COMMITTED`` / ``FAILED`` mean commit, ``ABORTED`` abort, ``PREPARING`` wait;
6. after a restart (:meth:`ConfigCoordinator.recover`): the newest
   ``COMMITTING`` row is committed again (commit resent, ``active.json``
   re-pointed when it still names an older version, e.g. after a crash between
   persisting ``COMMITTING`` and writing it); older ``COMMITTING`` rows were
   superseded and become ``COMMITTED`` without resending anything;
   ``PREPARING`` rows are persisted as ``ABORTED`` and then aborted;
7. after the commit the heartbeats of every instance must report the new
   ``config_version``/``config_hash`` (or a newer version) within 30 s
   (commits are resent to the laggards meanwhile); otherwise the row becomes
   ``FAILED`` and the ``alert`` hook fires (WS ``system`` event + CRITICAL
   notification). ``active.json`` already points to the new version, so a
   lagging instance converges when it restarts. A newer committed version
   supersedes the check of an older one (no resends, no ``FAILED``).
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from loguru import logger

from futrisk.common.calendar import TradingCalendar
from futrisk.common.clock import Clock, td_str
from futrisk.common.config import (
    ConfigError,
    FullConfig,
    compute_config_hash,
    file_hashes,
    load_config_dir,
    read_active,
    snapshot_working_config,
    write_active,
)
from futrisk.common.config.loader import existing_versions
from futrisk.common.paths import HomeLayout
from futrisk.messages import codes
from futrisk.messages.enums import ConfigEffect, ConfigStatus
from futrisk.messages.envelope import Reply
from futrisk.messages.system import ConfigAbort, ConfigCommit, ConfigDecision, ConfigPrepare, ConfigPrepared
from futrisk.services.api_gateway.ports import BusFacade
from futrisk.services.api_gateway.store import ApiStore, ConfigVersionRow

HeartbeatConfig = Callable[[str], tuple[int, str] | None]
Alert = Callable[[str, dict[str, Any]], Awaitable[None] | None]


@dataclass
class ReloadOutcome:
    """Result of one reload round: the persisted row and the REST code to answer with."""

    row: ConfigVersionRow
    code: str = codes.OK
    message: str = ""

    @property
    def ok(self) -> bool:
        return self.code == codes.OK


@dataclass
class _Round:
    row: ConfigVersionRow
    targets: list[str]
    commit_ok: set[str] = field(default_factory=set)


def effective_sections(
    effects: dict[str, ConfigEffect], calendar: TradingCalendar | None, now_ns: int
) -> dict[str, dict[str, str]]:
    """``active.json.effective``: per section its effect; NEXT_TRADING_DAY sections get the trading day."""
    next_td: str | None = None
    if calendar is not None and any(e == ConfigEffect.NEXT_TRADING_DAY for e in effects.values()):
        try:
            next_td = td_str(calendar.next_trading_day(calendar.current_or_next_trading_day(now_ns)))
        except LookupError:
            next_td = None
    out: dict[str, dict[str, str]] = {}
    for section, eff in sorted(effects.items()):
        entry = {"effect": ConfigEffect(str(eff)).value}
        if ConfigEffect(str(eff)) == ConfigEffect.NEXT_TRADING_DAY and next_td is not None:
            entry["trading_day"] = next_td
        out[section] = entry
    return out


class ConfigCoordinator:
    """2PC coordinator; one reload at a time; all state transitions persisted before being announced."""

    def __init__(
        self,
        *,
        store: ApiStore,
        bus: BusFacade,
        layout: HomeLayout,
        config: Callable[[], FullConfig],
        clock: Clock,
        instances: Callable[[], list[str]],
        heartbeat_config: HeartbeatConfig,
        alert: Alert | None = None,
        prepare_timeout_ms: int = 5_000,
        commit_timeout_ms: int = 2_000,
        verify_timeout_s: float = 30.0,
        resend_s: float = 5.0,
        poll_s: float = 0.25,
    ) -> None:
        self._store = store
        self._bus = bus
        self._layout = layout
        self._config = config
        self._clock = clock
        self._instances = instances
        self._hb = heartbeat_config
        self.alert = alert
        self._prepare_timeout_ms = prepare_timeout_ms
        self._commit_timeout_ms = commit_timeout_ms
        self._verify_timeout_s = verify_timeout_s
        self._resend_s = resend_s
        self._poll_s = poll_s
        self._lock = asyncio.Lock()
        self._tasks: set[asyncio.Task[Any]] = set()
        self._latest_committed = 0  # newest version whose decision is COMMIT (supersedes older checks)

    # ---------------------------------------------------------------- reload
    @property
    def busy(self) -> bool:
        return self._lock.locked()

    async def reload(self, requested_by: str) -> ReloadOutcome:
        """Run one complete 2PC round for the current working directory."""
        async with self._lock:
            return await self._reload(requested_by)

    async def _reload(self, requested_by: str) -> ReloadOutcome:
        loop = asyncio.get_running_loop()
        layout = self._layout
        dirs = await loop.run_in_executor(None, existing_versions, layout)
        n = max([0, *dirs, await self._store.max_config_version()]) + 1
        n, path = await loop.run_in_executor(None, snapshot_working_config, layout, n)
        files = await loop.run_in_executor(None, file_hashes, path)
        config_hash = compute_config_hash(files)
        rel = path.relative_to(layout.home).as_posix()
        row = ConfigVersionRow(
            config_version=n,
            config_hash=config_hash,
            path=rel,
            files=files,
            effective={},
            status=ConfigStatus.PREPARING,
            requested_by=requested_by,
            requested_ns=self._clock.now_ns(),
        )
        await self._store.insert_config_version(row)
        logger.info("config2pc.preparing", version=n, hash=config_hash, requested_by=requested_by)

        # local validation first: a snapshot that does not even parse is aborted at once
        try:
            await loop.run_in_executor(None, load_config_dir, path, n)
        except (ConfigError, OSError) as exc:
            await self._abort(row, [], f"invalid configuration: {exc}")
            return ReloadOutcome(row, codes.BAD_REQUEST, row.error or "")

        targets = sorted(self._instances())
        req = ConfigPrepare(config_version=n, config_hash=config_hash, path=rel, files=files, requested_by=requested_by)
        deadline = self._clock.monotonic_ns() + self._prepare_timeout_ms * 1_000_000
        replies = await asyncio.gather(
            *(
                self._bus.request(t, "ctl.config.prepare", req, timeout_ms=self._prepare_timeout_ms, deadline_ns=deadline)
                for t in targets
            )
        )
        acks: dict[str, Any] = {}
        failures: list[tuple[str, str]] = []
        rejected = False
        merged: dict[str, ConfigEffect] = {}
        for target, reply in zip(targets, replies, strict=True):
            prepared = self._prepared(reply)
            if prepared is None:
                acks[target] = {"ok": False, "code": reply.code, "errors": [reply.message or reply.code], "effects": {}}
                failures.append((target, reply.code))
                continue
            acks[target] = {
                "ok": prepared.ok,
                "code": codes.OK if prepared.ok else codes.BAD_REQUEST,
                "errors": list(prepared.errors),
                "effects": {k: str(v) for k, v in prepared.effects.items()},
                "computed_hash": prepared.computed_hash,
            }
            if not prepared.ok:
                rejected = True
                failures.append((target, codes.BAD_REQUEST))
            for sec, eff in prepared.effects.items():
                merged.setdefault(sec, ConfigEffect(str(eff)))
        row.acks = acks
        if failures:
            detail = "; ".join(
                f"{t}: {', '.join(acks[t]['errors']) or code}" for t, code in failures
            )
            await self._abort(row, targets, f"prepare failed: {detail}")
            if rejected:
                code = codes.BAD_REQUEST
            elif any(c == codes.TIMEOUT for _, c in failures):
                code = codes.TIMEOUT
            else:
                code = codes.UNAVAILABLE if any(c == codes.UNAVAILABLE for _, c in failures) else failures[0][1]
            return ReloadOutcome(row, code, row.error or "")

        # 4a: persist the decision, then active.json, then announce
        cfg = self._config()
        row.effective = effective_sections(merged, cfg.calendar, self._clock.now_ns())
        row.status = ConfigStatus.COMMITTING
        await self._store.update_config_version(row)
        self._latest_committed = max(self._latest_committed, n)
        await loop.run_in_executor(None, write_active, layout, n, path, row.effective)
        logger.info("config2pc.committing", version=n, effective=row.effective)
        rnd = _Round(row, targets)
        await self._commit(rnd, targets)
        self._spawn(self._verify(rnd), f"config-verify-{n}")
        return ReloadOutcome(row, codes.OK, f"config version {n} {row.status.value.lower()}")

    @staticmethod
    def _prepared(reply: Reply) -> ConfigPrepared | None:
        if not reply.ok:
            return None
        try:
            got = reply.open_as(ConfigPrepared)
        except Exception:
            return None
        return got if isinstance(got, ConfigPrepared) else None

    async def _abort(self, row: ConfigVersionRow, targets: list[str], error: str) -> None:
        """4b: persist ABORTED first, then tell the participants."""
        row.status = ConfigStatus.ABORTED
        row.error = error
        row.finished_ns = self._clock.now_ns()
        await self._store.update_config_version(row)
        logger.warning("config2pc.aborted", version=row.config_version, error=error)
        await self._alert_async("CONFIG_ABORTED", {"config_version": row.config_version, "error": error})
        if targets:
            abort = ConfigAbort(config_version=row.config_version, reason=error[:500])
            await asyncio.gather(
                *(self._bus.request(t, "ctl.config.abort", abort, timeout_ms=self._commit_timeout_ms) for t in targets)
            )

    async def _commit(self, rnd: _Round, targets: list[str]) -> None:
        commit = ConfigCommit(config_version=rnd.row.config_version)
        replies = await asyncio.gather(
            *(self._bus.request(t, "ctl.config.commit", commit, timeout_ms=self._commit_timeout_ms) for t in targets)
        )
        for target, reply in zip(targets, replies, strict=True):
            ack = rnd.row.acks.setdefault(target, {})
            ack["committed"] = reply.ok
            if reply.ok:
                rnd.commit_ok.add(target)
            else:
                ack["commit_code"] = reply.code
                logger.warning("config2pc.commit_failed", version=rnd.row.config_version, target=target, code=reply.code)
        if rnd.row.status == ConfigStatus.COMMITTING and rnd.commit_ok >= set(rnd.targets):
            rnd.row.status = ConfigStatus.COMMITTED
            rnd.row.finished_ns = self._clock.now_ns()
            logger.info("config2pc.committed", version=rnd.row.config_version)
        await self._store.update_config_version(rnd.row)

    def _converged(self, rnd: _Round) -> list[str]:
        """Instances whose heartbeat reports neither the new version/hash nor a newer version."""
        version, want_hash = rnd.row.config_version, rnd.row.config_hash
        lagging = []
        for t in rnd.targets:
            hb = self._hb(t)
            if hb is None or hb[0] < version or (hb[0] == version and hb[1] != want_hash):
                lagging.append(t)
        return lagging

    def _superseded(self, rnd: _Round) -> bool:
        return self._latest_committed > rnd.row.config_version

    async def _verify(self, rnd: _Round) -> None:
        """Step 7: heartbeats must converge within 30 s; resend commits to laggards meanwhile."""
        start = self._clock.monotonic_ns()
        deadline = start + int(self._verify_timeout_s * 1e9)
        next_resend = start + int(self._resend_s * 1e9)
        while True:
            if self._superseded(rnd):
                if rnd.row.status == ConfigStatus.COMMITTING:
                    rnd.row.status = ConfigStatus.COMMITTED  # the decision was COMMIT; a newer one replaced it
                    rnd.row.finished_ns = self._clock.now_ns()
                    await self._store.update_config_version(rnd.row)
                logger.info("config2pc.superseded", version=rnd.row.config_version, by=self._latest_committed)
                return
            lagging = self._converged(rnd)
            now = self._clock.monotonic_ns()
            if not lagging:
                if rnd.row.status == ConfigStatus.COMMITTING:
                    rnd.row.status = ConfigStatus.COMMITTED
                    rnd.row.finished_ns = self._clock.now_ns()
                    await self._store.update_config_version(rnd.row)
                logger.info("config2pc.converged", version=rnd.row.config_version)
                return
            if now >= deadline:
                rnd.row.status = ConfigStatus.FAILED
                rnd.row.error = f"heartbeats did not report config {rnd.row.config_version} within " \
                                f"{self._verify_timeout_s:g}s: {', '.join(lagging)}"
                rnd.row.finished_ns = self._clock.now_ns()
                await self._store.update_config_version(rnd.row)
                logger.error("config2pc.failed", version=rnd.row.config_version, lagging=lagging)
                await self._alert_async(
                    "CONFIG_FAILED", {"config_version": rnd.row.config_version, "lagging": lagging}
                )
                return
            if now >= next_resend:
                next_resend = now + int(self._resend_s * 1e9)
                await self._commit(rnd, lagging)
            await asyncio.sleep(self._poll_s)

    async def _alert_async(self, kind: str, detail: dict[str, Any]) -> None:
        if self.alert is None:
            return
        try:
            res = self.alert(kind, detail)
            if res is not None:
                await res
        except Exception:
            logger.exception("config2pc.alert_failed", kind=kind)

    # --------------------------------------------------------------- decision
    async def decision(self, version: int) -> ConfigDecision | None:
        """``config.decision`` (02 §6.7): the persisted status of ``version`` (None when unknown)."""
        row = await self._store.get_config_version(version)
        if row is None:
            return None
        return ConfigDecision(config_version=version, status=row.status)

    # --------------------------------------------------------------- recovery
    async def recover(self) -> list[int]:
        """Step 6 after a coordinator restart; returns the versions handled."""
        loop = asyncio.get_running_loop()
        handled: list[int] = []
        rows = await self._store.config_versions_with_status([ConfigStatus.PREPARING, ConfigStatus.COMMITTING])
        decided = await self._store.config_versions_with_status(
            [ConfigStatus.COMMITTING, ConfigStatus.COMMITTED, ConfigStatus.FAILED]
        )
        newest = max((r.config_version for r in decided), default=0)
        self._latest_committed = max(self._latest_committed, newest)
        targets = sorted(self._instances())
        for row in rows:
            handled.append(row.config_version)
            if row.status == ConfigStatus.PREPARING:
                logger.warning("config2pc.recover_abort", version=row.config_version)
                await self._abort(row, targets, "coordinator restarted before a decision was made")
                continue
            if row.config_version < newest:
                # superseded by a newer decided version: never resend an old commit
                row.status = ConfigStatus.COMMITTED
                row.finished_ns = self._clock.now_ns()
                await self._store.update_config_version(row)
                logger.warning("config2pc.recover_superseded", version=row.config_version, by=newest)
                continue
            logger.warning("config2pc.recover_commit", version=row.config_version)
            path = self._layout.home / row.path
            active = await loop.run_in_executor(None, read_active, self._layout)
            if active is None or int(active.get("config_version", 0)) < row.config_version:
                await loop.run_in_executor(None, write_active, self._layout, row.config_version, path, row.effective)
            rnd = _Round(row, targets)
            self._spawn(self._recommit(rnd), f"config-recommit-{row.config_version}")
        return handled

    async def _recommit(self, rnd: _Round) -> None:
        await self._commit(rnd, rnd.targets)
        await self._verify(rnd)

    # ------------------------------------------------------------------ tasks
    def _spawn(self, coro: Awaitable[Any], name: str) -> None:
        task = asyncio.ensure_future(coro)
        task.set_name(name)
        self._tasks.add(task)

        def done(t: asyncio.Task[Any]) -> None:
            self._tasks.discard(t)
            if not t.cancelled() and t.exception() is not None:
                logger.opt(exception=t.exception()).error("config2pc.task_failed", task=name)

        task.add_done_callback(done)

    async def wait_idle(self) -> None:
        """Wait for background verification/recommit tasks (tests)."""
        while self._tasks:
            await asyncio.gather(*list(self._tasks), return_exceptions=True)

    async def close(self) -> None:
        for t in list(self._tasks):
            t.cancel()
        if self._tasks:
            await asyncio.gather(*list(self._tasks), return_exceptions=True)

    def version_path(self, row: ConfigVersionRow) -> Path:
        return self._layout.home / row.path


__all__ = ["ConfigCoordinator", "ReloadOutcome", "effective_sections"]
