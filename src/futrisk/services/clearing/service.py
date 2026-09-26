"""The clearing-worker service: wires clearing.db, the saga, pushes, reports and jobs to the runtime.

Start-up (02 §8.6): check that the report template is readable (fail fast) ->
open clearing.db (migrations, ``synchronous=FULL``) -> register the commands
(``settle.sod_snapshot`` must be served early: the risk-engine shards start
after the clearing-worker and fetch their snapshot; state-changing commands
answer ``UNAVAILABLE`` until ready) -> watch the shards' heartbeats (re-push)
-> interrupted backtests FAILED -> wait for the market clock (``sys.clock``)
-> record the running config version, subscribe the copy streams, resume the
unfinished EOD runs and an unpushed seed snapshot (all of them stamp market
time) -> ``RUNNING`` and start the EOD scheduler.

Concurrency (01 §8.12): asyncio for commands, scheduling and the saga; the
database writer thread; a ``spawn`` process pool for settlement, reports and
backtests (never blocking the loop, so heartbeats keep flowing).
"""

from __future__ import annotations

import asyncio
import contextlib
from concurrent.futures import Executor
from typing import Any

from loguru import logger
from sqlalchemy import Connection

from futrisk.common.clock import parse_td, td_str
from futrisk.core import InstrumentRegistry
from futrisk.db import Database
from futrisk.messages import codes
from futrisk.messages.enums import EodPhase, ServiceState
from futrisk.runtime import AsyncServiceRuntime
from futrisk.services.clearing import schema, store
from futrisk.services.clearing.backtests import BacktestManager
from futrisk.services.clearing.commands import ClearingCommands, prev_trading_day
from futrisk.services.clearing.confighist import ConfigHistory
from futrisk.services.clearing.copies import TRADE_GATEWAY, LocalCopies
from futrisk.services.clearing.maintenance import Maintenance
from futrisk.services.clearing.pool import WorkerPool
from futrisk.services.clearing.pushes import PushManager, PushTarget
from futrisk.services.clearing.reports.gather import ReportService
from futrisk.services.clearing.reports.render import check_template
from futrisk.services.clearing.saga import MD_GATEWAY, EodSaga
from futrisk.services.clearing.scheduler import EodScheduler
from futrisk.services.clearing.schema import SodSource
from futrisk.services.clearing.settings import ClearingSettings
from futrisk.services.clearing.store import EodRun

DB_NAME = "clearing"
REPAIR_MAX_DAYS = 15


class ClearingService:
    """clearing-worker business logic on top of an :class:`AsyncServiceRuntime` (already started)."""

    def __init__(
        self,
        rt: AsyncServiceRuntime,
        settings: ClearingSettings | None = None,
        *,
        executor: Executor | None = None,
    ) -> None:
        self.rt = rt
        self.settings = settings or ClearingSettings()
        self.pool = WorkerPool(self.settings.pool_workers, executor=executor)
        self._registry: tuple[Any, InstrumentRegistry] | None = None
        self._tasks: set[asyncio.Task[Any]] = set()
        self._regenerating: dict[tuple[str, int], asyncio.Task[Any]] = {}
        self._blocked_logged: set[str] = set()
        self._db: Database | None = None
        self.shards: list[str] = []
        self.configs: ConfigHistory
        self.pushes: PushManager
        self.reports: ReportService
        self.saga: EodSaga
        self.backtests: BacktestManager
        self.maintenance: Maintenance
        self.copies: LocalCopies
        self.commands: ClearingCommands
        self.scheduler: EodScheduler
        self.ready = asyncio.Event()

    # --------------------------------------------------------------- access
    @property
    def db(self) -> Database:
        if self._db is None:
            raise RuntimeError("clearing service not started")
        return self._db

    def registry(self) -> InstrumentRegistry:
        """Instrument registry of the current config (rebuilt after a config commit)."""
        cfg = self.rt.config
        if self._registry is None or self._registry[0] is not cfg:
            self._registry = (cfg, InstrumentRegistry(cfg.products, cfg.calendar))
        return self._registry[1]

    def current_trading_day(self) -> str:
        """Trading day of market now (the next trading day outside sessions)."""
        try:
            return td_str(self.rt.config.calendar.current_or_next_trading_day(self.rt.market_clock.now_ns()))
        except LookupError:
            return td_str(self.rt.config.calendar.last_day)

    def _spawn(self, coro: Any, name: str) -> asyncio.Task[Any]:
        task = asyncio.create_task(coro, name=name)
        self._tasks.add(task)

        def done(t: asyncio.Task[Any]) -> None:
            self._tasks.discard(t)
            if not t.cancelled() and t.exception() is not None:
                logger.opt(exception=t.exception()).error("clearing.task_failed", task=name)

        task.add_done_callback(done)
        return task

    # --------------------------------------------------------------- start
    async def start(self) -> None:
        rt = self.rt
        size = await asyncio.to_thread(check_template)  # H-4: a missing template must stop the start
        self._db = await rt.open_database(DB_NAME, schema.METADATA, migrations=schema.MIGRATIONS, synchronous="FULL")
        rt.on_stop(self.stop)
        cfg = rt.config
        self.shards = cfg.services.risk_instances()
        self.configs = ConfigHistory(rt, self.db)
        rt.on_config_commit(self.configs.on_commit)
        self.pushes = PushManager(rt, self.shards, self.settings)
        self.reports = ReportService(rt, self.db, self.settings, shards=self.shards, pool=self.pool)
        self.saga = EodSaga(
            rt=rt,
            db=self.db,
            settings=self.settings,
            pushes=self.pushes,
            shards=self.shards,
            pool=self.pool,
            configs=self.configs,
            reports=self.reports.generate,
            on_done=self._on_done,
        )
        self.backtests = BacktestManager(
            rt, self.db, self.settings, pool=self.pool, reports=self.reports, trading_day=self.current_trading_day
        )
        assert rt.layout is not None and rt.outbox is not None
        self.maintenance = Maintenance(
            self.db,
            rt.outbox,
            self.settings,
            backup_dir=rt.layout.backup_dir,
            calendar=cfg.calendar,
            wall_ns=rt.market_clock.wall.now_ns,
        )
        interrupted = await self.backtests.recover()  # before backtest.run is served
        self.commands = ClearingCommands(self)
        self.commands.register()
        self.copies = LocalCopies(rt, self.db, self.settings, repair_days=self.unsettled_days)
        rt.watch_peers([*self.shards, TRADE_GATEWAY, MD_GATEWAY], on_change=self._on_peer)
        rt.heartbeat_provider(self._heartbeat)
        self.scheduler = EodScheduler(
            config=lambda: self.rt.config,
            now_ns=rt.market_clock.now_ns,
            latest_sod_day=self._latest_sod_day,
            has_run=self._has_run,
            start_run=self._scheduled_run,
            interval_s=self.settings.scheduler_interval_s,
        )
        logger.info("clearing.started", shards=self.shards, interrupted_backtests=interrupted, template_bytes=size)
        self._spawn(self._become_ready(), "clearing:ready")

    async def _become_ready(self) -> None:
        # L8：订阅副本、恢复运行、补推都要写入市场时间：先等 sys.clock
        if not await self.rt.wait_clock_ready():
            return  # stopping before the market clock was known
        try:
            await self.configs.start()
            await self.copies.start(self.shards)
            resumed = await self.saga.resume_all()
            await self._resume_seed_push()
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # never stay STARTING forever: the supervisor restarts us (01 §8.6)
            self.rt.fatal(exc, "clearing start-up")
            return
        self.pushes.start()
        self.rt.set_heartbeat_fields(trading_day=self.current_trading_day())
        self.rt.set_state(ServiceState.RUNNING)
        if self.settings.scheduler_enabled:
            self.scheduler.start()
        self.ready.set()
        logger.info("clearing.ready", resumed=[(r.trading_day, r.version, r.phase.value) for r in resumed])

    async def stop(self) -> None:
        """Stop hook (runs before the runtime closes sockets and databases)."""
        with contextlib.suppress(AttributeError):
            await self.scheduler.stop()
        for part in ("saga", "pushes", "copies", "backtests"):
            obj = getattr(self, part, None)
            if obj is not None:
                with contextlib.suppress(Exception):
                    await obj.stop()
        for t in list(self._tasks):
            t.cancel()
        for t in list(self._tasks):
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await t
        await self.pool.close(self.settings.pool_stop_grace_s)

    # ------------------------------------------------------------ callbacks
    def _on_peer(self, peer: str, state: str) -> None:
        if peer in self.shards:
            self.pushes.on_peer_change(peer, state)

    def _heartbeat(self) -> dict[str, Any]:
        detail: dict[str, Any] = {"eod": self.saga.heartbeat_detail()}
        push = self.pushes.status()
        if push is not None:
            detail["push"] = push
        detail["copies"] = {"trades": self.copies.trades_applied, "cash": self.copies.cash_applied}
        return {"trading_day": self.current_trading_day(), "detail": detail}

    def _on_done(self, run: EodRun) -> None:
        if not self.settings.maintenance_enabled:
            return
        self._spawn(self._maintain(run), f"clearing:maintenance:{run.trading_day}")

    async def _maintain(self, run: EodRun) -> None:
        latest = await self.db.run_read(lambda c: store.effective_version(c, run.trading_day))
        if latest != run.version:
            return
        await self.maintenance.run(run.trading_day)

    async def unsettled_days(self) -> list[str]:
        """Trading days whose copies still matter (repairs): the newest SOD day up to today, at most 15 days."""
        cal = self.rt.config.calendar
        today = self.current_trading_day()
        latest = await self.db.run_read(store.latest_sod_day)
        days = {today}
        if latest is not None and latest <= today:
            try:
                span = cal.trading_days_between(parse_td(latest), parse_td(today))
            except (ValueError, LookupError):
                span = []
            days.update(td_str(d) for d in span[-REPAIR_MAX_DAYS:])
        elif latest is not None:
            days.add(latest)
        return sorted(days)

    # ------------------------------------------------------------ scheduler
    async def _latest_sod_day(self) -> str | None:
        return await self.db.run_read(store.latest_sod_day)

    async def _has_run(self, trading_day: str) -> bool:
        return await self.db.run_read(lambda c: store.latest_run(c, trading_day) is not None)

    async def _scheduled_run(self, trading_day: str) -> bool:
        now = self.rt.market_clock.now_ns()
        decision = await self.db.run_write(
            lambda c: self.saga.decide_run_tx(c, trading_day, force_new=False, triggered_by="scheduler", now_ns=now)
        )
        if decision.ok and decision.run is not None:
            self._blocked_logged.discard(trading_day)
            self.saga.spawn(decision.run)
            return True
        if decision.code == codes.PRECONDITION_FAILED and "not rolled yet" in (decision.message or ""):
            if trading_day not in self._blocked_logged:  # waiting for the previous day's push: retried every tick
                self._blocked_logged.add(trading_day)
                logger.info("scheduler.waiting_for_previous_day", trading_day=trading_day, reason=decision.message)
            return False
        logger.warning("scheduler.not_started", trading_day=trading_day, code=decision.code, message=decision.message)
        return False

    # -------------------------------------------------------------- reports
    def regenerate_reports(self, trading_day: str, version: int) -> bool:
        """Generate the reports of a finished run again (its REPORTED step logged errors); one at a time (L1)."""
        key = (trading_day, version)
        running = self._regenerating.get(key)
        if running is not None and not running.done():
            logger.info("clearing.reports_regeneration_running", trading_day=trading_day, version=version)
            return False

        async def run() -> None:
            produced = await self.reports.generate(trading_day, version)
            now = self.rt.market_clock.now_ns()

            def clear(conn: Connection) -> None:
                r = store.get_run(conn, trading_day, version)
                if r is None:
                    return
                rep = r.log(EodPhase.REPORTED)
                if not rep.get("errors"):
                    return
                rep["errors_cleared"] = rep.pop("errors")
                rep["regenerated_ns"] = now
                rep["reports"] = len(produced)
                r.step_log = {**r.step_log, EodPhase.REPORTED.value: rep}
                store.update_step_log(conn, r)

            await self.db.run_write(clear)
            logger.info("clearing.reports_regenerated", trading_day=trading_day, version=version, files=len(produced))

        task = self._spawn(run(), f"clearing:reports:{trading_day}:v{version}")
        self._regenerating[key] = task
        task.add_done_callback(lambda t: self._regenerating.pop(key, None) if self._regenerating.get(key) is t else None)
        return True

    # --------------------------------------------------------------- pushes
    def push_seed(self, trading_day: str, version: int) -> None:
        """Push a SEED snapshot to every shard (``trading_day`` = previous trading day, 02 §6.5)."""

        async def finalize(ns: int, shards: list[str]) -> None:
            await self.db.run_write(lambda c: store.set_pushed(c, trading_day, version, ns))

        def on_stuck(target: PushTarget) -> None:
            logger.warning(
                "clearing.seed_push_incomplete",
                trading_day=trading_day,
                version=version,
                pending=target.pending,
                errors=dict(target.errors),
            )

        async def run() -> None:
            outcome = await self.pushes.push(
                prev_trading_day(self, trading_day),
                trading_day,
                version,
                source=SodSource.SEED.value,
                finalize=finalize,
                on_stuck=on_stuck,
            )
            logger.info("clearing.seed_push", trading_day=trading_day, version=version, outcome=outcome.value)

        self._spawn(run(), f"clearing:push:{trading_day}:v{version}")

    async def _resume_seed_push(self) -> None:
        """Re-push the newest snapshot when it was never confirmed by all shards (补推 after a restart)."""

        def q(c: Any) -> tuple[Any, bool]:
            day = store.latest_sod_day(c)
            if day is None:
                return None, False
            row = store.latest_sod_version(c, day)
            owned = False
            if row is not None and row.source == SodSource.EOD.value and row.from_trading_day is not None:
                run = store.get_run(c, row.from_trading_day, row.from_version or 0)
                owned = run is not None and run.phase == EodPhase.SETTLED
            return row, owned

        row, owned = await self.db.run_read(q)
        if row is None or row.pushed_ns is not None or owned:
            return
        logger.info("clearing.repush_pending", trading_day=row.trading_day, version=row.version, source=row.source)
        self.push_seed(row.trading_day, row.version)


__all__ = ["DB_NAME", "ClearingService"]
