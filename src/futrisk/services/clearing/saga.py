"""The EOD saga (01 §6.6 R6, 02 §8.4, 03 §4.4 ``eod_runs``, §7).

One run per ``(trading_day, version)``; ``eod_runs.phase`` holds the last
completed step, so a restarted clearing-worker continues from there:

====  ===================  =====================================================================
step  phase                what happens (all state changes + ``sys.eod`` in one transaction)
====  ===================  =====================================================================
1     BEGIN                run row created by ``settle.run`` or the scheduler; the SOD version
                           and the config version in force for the day are pinned here
2     FROZEN               ``eod.freeze{trading_day, version}`` to every shard (30 s x 3 retries)
3     TRADES_RECONCILED    three-way trade digests + cash digests; on mismatch pull
                           ``td.query.trades`` / ``risk.query.cash`` and re-freeze (at most 3
                           re-freezes per run); the per-account digests are pinned
4     PRICES_FIXED         ``md.settle_inputs`` (outside the price lock) -> manual > official >
                           VWAP > fallback chain decided and committed under the price lock; the
                           decided prices are pinned in ``step_log`` so a resumed run reuses them
5     SETTLED              the copy rows must still match the pinned digests (else back to
                           FROZEN, counted as a re-freeze); :func:`futrisk.core.settle_day` in the
                           process pool (retried once when a worker died); ALL result tables,
                           ``recon_checks``, the next-day SOD version, ``eod_runs`` and the
                           ``clearing.settled`` + ``sys.eod`` outbox rows in ONE transaction;
                           any FAIL check -> FAILED, no roll
6     ROLLED               ``eod.rollover`` to every shard (shards fetch the version with
                           ``settle.sod_snapshot``); ``pushed_ns`` + ROLLED once all confirmed
                           (ROLL, REBASE or CATCH_UP), re-push to returning shards (:mod:`.pushes`)
7     REPORTED             reports in the process pool, one ``report.ready`` per file; a failure
                           is only a WARN (02 §8.4)
8     DONE                 ``sys.eod`` DONE, then the daily maintenance (03 §8)
====  ===================  =====================================================================

Days are settled and rolled **in order**: a run starts only when no earlier
day's run is still before ROLLED (the shards chain the official SODs day by
day; after missed EODs the scheduler settles the skipped days one by one and
the shards answer with ``CATCH_UP`` / ``REBASE``).

Re-settlement after the roll (``force_new_version`` while an earlier version
of the day is effective and was pushed, 02 §8.4 换日之后又重新结算): the shards
already trade the next day, so step 2 does not freeze them; step 3 reconciles
the copy with trade-gateway two-way and compares it with the digests pinned by
the settled version (differences are WARN checks); step 6 pushes the new SOD
version and the shards REBASE. BEGIN records ``resettle_of``.

Unrecoverable errors before SETTLED end the run in ``FAILED`` with the reason
(``sys.eod`` FAILED, api-gateway notifies). After SETTLED the settlement is
valid and the run never fails: pushes are retried and report errors are WARNs.
Facts that reach the copy after SETTLED are flagged as ``LATE_FACT`` WARNs
(:mod:`.eodwarn`).
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Callable, Iterable, Mapping, Sequence
from concurrent.futures.process import BrokenProcessPool
from dataclasses import dataclass, replace
from datetime import datetime, time
from decimal import Decimal
from typing import Any

import msgspec
from loguru import logger
from sqlalchemy import Connection

from futrisk.common.clock import BJ, bj_to_ns, parse_td, td_str
from futrisk.common.config import FullConfig
from futrisk.common.decimals import D
from futrisk.core import DaySettlement, InstrumentRegistry, SettlementError
from futrisk.core._num import money_str
from futrisk.db import Database
from futrisk.messages import codes
from futrisk.messages.enums import CheckStatus, EodPhase, ReconCheck
from futrisk.messages.eod import (
    CheckResult,
    EodFreezeAck,
    EodFreezeRequest,
    EodPhaseEvent,
    SettlementPriceView,
)
from futrisk.messages.md import SettleInput, SettleInputsBatch, SettleInputsRequest
from futrisk.messages.orders import DigestRequest, Trade, TradeBatch, TradeDigest, TradeQuery
from futrisk.messages.risk import (
    AccountSnapshot,
    AccountSnapshotBatch,
    CashBatch,
    CashMovement,
    QueryAccounts,
    QueryCash,
)
from futrisk.messages.topics import CLEARING_SETTLED, SYS_EOD
from futrisk.runtime import AsyncServiceRuntime
from futrisk.services.clearing import digests, eodwarn, jobs, store
from futrisk.services.clearing.confighist import ConfigHistory
from futrisk.services.clearing.copies import _insert_cash_job, _insert_trades_job
from futrisk.services.clearing.pool import WorkerPool
from futrisk.services.clearing.prices import decide_prices, fallback_pre_settle
from futrisk.services.clearing.pushes import PushManager, PushOutcome, PushTarget
from futrisk.services.clearing.schema import SodSource
from futrisk.services.clearing.settings import ClearingSettings
from futrisk.services.clearing.store import EodRun, phase_index

TRADE_GATEWAY = "trade-gateway"
MD_GATEWAY = "md-gateway"
DIGEST_KINDS = (ReconCheck.TRADE_DIGEST, ReconCheck.CASH_DIGEST)
SETTLE_KINDS = (
    ReconCheck.EQUITY_IDENTITY,
    ReconCheck.POSITION_IDENTITY,
    ReconCheck.EXCH_FORMULA,
    ReconCheck.INTRADAY_VS_SETTLE,
    ReconCheck.FIRM_SUM,
    ReconCheck.MTM_VS_FIFO,
)
PRICE_REMEDY = (
    "set the price with settle.override_price (RISK_MANAGER/ADMIN) or import official prices with "
    "settle.import_official (ADMIN), then run settle.run again; for a contract without a previous "
    "settlement price (first traded today) the manual/official price also serves as its pre_settle"
)


class SagaFailure(Exception):
    """An unrecoverable step failure: the run ends in FAILED (02 §8.4)."""

    def __init__(self, error: str, checks: Sequence[CheckResult] | None = None) -> None:
        super().__init__(error)
        self.error = error
        self.checks = list(checks or [])


class _CopyChanged(Exception):
    """Raised inside the SETTLED transaction when facts reached the copy while settling."""


@dataclass(frozen=True)
class RunDecision:
    """Outcome of :meth:`EodSaga.decide_run_tx`: reply code, the run, whether it was created."""

    code: str
    message: str | None = None
    run: EodRun | None = None
    created: bool = False

    @property
    def ok(self) -> bool:
        return self.code == codes.OK


@dataclass
class _Ctx:
    run: EodRun
    acks: dict[str, EodFreezeAck] | None = None


ReportsFn = Callable[[str, int], Any]
TxHook = Callable[[Connection, dict[str, Any]], None]


def pinned_sod_version(run: EodRun) -> int | None:
    """SOD snapshot version pinned at BEGIN (H-1: a seed correction never changes a run in progress)."""
    v = run.log(EodPhase.BEGIN).get("sod_version")
    if v is None:
        v = run.log(EodPhase.PRICES_FIXED).get("sod_version")
    return int(v) if v is not None else None


def resettle_of(run: EodRun) -> int | None:
    """The settled version this run re-settles after the roll (``None`` for a first settlement)."""
    v = run.log(EodPhase.BEGIN).get("resettle_of")
    return int(v) if v is not None else None


def pinned_config_version(run: EodRun) -> int | None:
    """Config version recorded at BEGIN (the version in force for the trading day)."""
    v = run.log(EodPhase.BEGIN).get("config_version")
    return int(v) if v is not None else None


def copy_counts_of(trades: Iterable[Trade], cash: Iterable[CashMovement]) -> dict[str, list[int]]:
    """Per account ``[trades, cash movements]`` (same shape as :func:`store.copy_counts`)."""
    out: dict[str, list[int]] = {}
    for t in trades:
        out.setdefault(t.account_id, [0, 0])[0] += 1
    for c in cash:
        out.setdefault(c.account_id, [0, 0])[1] += 1
    return dict(sorted(out.items()))


class EodSaga:
    """Creates, drives and resumes EOD runs."""

    def __init__(
        self,
        *,
        rt: AsyncServiceRuntime,
        db: Database,
        settings: ClearingSettings,
        pushes: PushManager,
        shards: Sequence[str],
        pool: WorkerPool,
        configs: ConfigHistory,
        reports: ReportsFn,
        on_done: Callable[[EodRun], None] | None = None,
    ) -> None:
        self._rt = rt
        self._db = db
        self._settings = settings
        self._pushes = pushes
        self._shards = list(shards)
        self._pool = pool
        self._configs = configs
        self._reports = reports
        self._on_done = on_done
        self._tasks: dict[tuple[str, int], asyncio.Task[None]] = {}
        self._contexts: dict[tuple[str, int], _Ctx] = {}
        self._side: set[asyncio.Task[Any]] = set()
        self._price_locks: dict[str, asyncio.Lock] = {}
        self._close_cache: dict[str, int] = {}
        self.latest: dict[str, tuple[int, EodPhase]] = {}

    # ------------------------------------------------------------ properties
    def running(self) -> list[tuple[str, int]]:
        return sorted(k for k, t in self._tasks.items() if not t.done())

    def task(self, trading_day: str, version: int) -> asyncio.Task[None] | None:
        return self._tasks.get((trading_day, version))

    def price_lock(self, trading_day: str) -> asyncio.Lock:
        """Serializes step 4's read-decide-commit with ``settle.override_price`` / ``import_official`` of the day."""
        lock = self._price_locks.get(trading_day)
        if lock is None:
            lock = asyncio.Lock()
            self._price_locks[trading_day] = lock
        return lock

    def heartbeat_detail(self) -> dict[str, Any]:
        if not self.latest:
            return {}
        td = max(self.latest)
        version, phase = self.latest[td]
        return {"trading_day": td, "version": version, "phase": phase.value}

    # ------------------------------------------------------------ run control
    def day_closed_ns(self, trading_day: str) -> int:
        """End of the last session of ``trading_day`` over all configured products (15:15 with T)."""
        cached = self._close_cache.get(trading_day)
        if cached is not None:
            return cached
        cfg = self._rt.config
        d = parse_td(trading_day)
        closes = [cfg.calendar.day_close(spec.sessions, d) for spec in cfg.products.products.values()]
        valid = [c for c in closes if c is not None]
        end = max(valid) if valid else datetime.combine(d, time(15, 0), tzinfo=BJ)
        ns = bj_to_ns(end)
        self._close_cache[trading_day] = ns
        return ns

    def decide_run_tx(
        self, conn: Connection, trading_day: str, *, force_new: bool, triggered_by: str, now_ns: int
    ) -> RunDecision:
        """Start a new run or return the current one (inside a writer transaction).

        * an in-progress run is continued (``force_new_version`` -> CONFLICT);
        * an effective run is returned unchanged unless ``force_new_version``;
        * a FAILED run is re-run as a new version;
        * a new version needs the SOD of the day, the day to be closed, no later
          day settled (``CONFLICT``) and every earlier day's run rolled
          (``PRECONDITION_FAILED``, 02 §11.1). It pins the SOD version and the
          config version in force for the day.
        """
        cfg = self._rt.config
        try:
            day = parse_td(trading_day)
        except (ValueError, IndexError):
            return RunDecision(codes.BAD_REQUEST, f"bad trading_day {trading_day!r}")
        if len(trading_day) != 8 or not cfg.calendar.is_trading_day(day):
            return RunDecision(codes.BAD_REQUEST, f"{trading_day} is not a trading day")
        latest = store.latest_run(conn, trading_day)
        if latest is not None and latest.in_progress:
            if force_new:
                return RunDecision(
                    codes.CONFLICT, f"run {trading_day} v{latest.version} is in progress ({latest.phase})", latest
                )
            return RunDecision(codes.OK, "continuing the run in progress", latest)
        if latest is not None and latest.effective and not force_new:
            return RunDecision(codes.OK, f"already settled (v{latest.version}, {latest.phase})", latest)
        later = store.later_settlement_exists(conn, trading_day)
        if later is not None:
            return RunDecision(codes.CONFLICT, f"trading day {later} is already settled; cannot settle {trading_day}")
        earlier = store.unrolled_run_before(conn, trading_day)
        if earlier is not None:
            return RunDecision(
                codes.PRECONDITION_FAILED,
                f"{earlier.trading_day} v{earlier.version} is not rolled yet ({earlier.phase.value}); "
                "trading days are settled and rolled in order",
            )
        sod = store.latest_sod_version(conn, trading_day)
        if sod is None:
            return RunDecision(
                codes.PRECONDITION_FAILED,
                f"no SOD snapshot for {trading_day}: seed it (settle.seed_sod) or settle the previous day first",
            )
        if now_ns < self.day_closed_ns(trading_day):
            return RunDecision(codes.PRECONDITION_FAILED, f"trading day {trading_day} is not closed yet")
        version = store.max_run_version(conn, trading_day) + 1
        next_td = td_str(cfg.calendar.next_trading_day(day))
        config_version, config_hash = self._configs.for_day_tx(conn, trading_day)
        detail: dict[str, Any] = {
            "triggered_by": triggered_by,
            "sod_version": sod.version,
            "config_version": config_version,
            "config_hash": config_hash,
        }
        if force_new:
            detail["force_new_version"] = True
        effective = store.effective_version(conn, trading_day)
        eff_run = store.get_run(conn, trading_day, effective) if effective is not None else None
        if eff_run is not None and phase_index(eff_run.phase) >= phase_index(EodPhase.ROLLED):
            detail["resettle_of"] = effective  # the shards already hold its SOD and trade the next day
        run = EodRun(
            trading_day=trading_day,
            version=version,
            phase=EodPhase.BEGIN,
            next_trading_day=next_td,
            triggered_by=triggered_by,
            config_hash=config_hash,
            step_log={EodPhase.BEGIN.value: {"ts_ns": now_ns, **detail}},
            started_ns=now_ns,
            updated_ns=now_ns,
        )
        store.insert_run(conn, run)
        self._rt.append_fact(
            conn,
            SYS_EOD,
            EodPhaseEvent(
                trading_day=trading_day,
                version=version,
                phase=EodPhase.BEGIN,
                next_trading_day=next_td,
                detail=detail,
                ts_ns=now_ns,
            ),
            td=trading_day,
        )
        return RunDecision(codes.OK, None, run, created=True)

    def spawn(self, run: EodRun) -> None:
        """Drive ``run`` in a background task (no-op when it is already running or finished)."""
        key = (run.trading_day, run.version)
        self.latest[run.trading_day] = (run.version, run.phase)
        existing = self._tasks.get(key)
        if existing is not None and not existing.done():
            return
        if run.terminal:
            return
        ctx = _Ctx(run=run)
        self._contexts[key] = ctx
        task = asyncio.create_task(self._drive(ctx), name=f"clearing:eod:{run.trading_day}:v{run.version}")
        self._tasks[key] = task

        def _done(t: asyncio.Task[None]) -> None:
            if t.cancelled():
                return
            exc = t.exception()
            if exc is not None:
                self._rt.fatal(exc, f"eod saga {run.trading_day} v{run.version}")

        task.add_done_callback(_done)

    async def resume_all(self) -> list[EodRun]:
        """Restart every unfinished run (02 §8.4 恢复); older unfinished runs of a day are superseded."""
        runs = await self._db.run_read(store.unfinished_runs)
        latest_by_day: dict[str, EodRun] = {}
        for r in runs:
            cur = latest_by_day.get(r.trading_day)
            if cur is None or r.version > cur.version:
                latest_by_day[r.trading_day] = r
        for r in runs:
            newest = latest_by_day[r.trading_day]
            if newest is not r and phase_index(r.phase) < phase_index(EodPhase.SETTLED):
                await self._fail(_Ctx(run=r), f"superseded by version {newest.version}")
        resumed: list[EodRun] = []
        for r in sorted(latest_by_day.values(), key=lambda x: (x.trading_day, x.version)):
            logger.info("eod.resume", trading_day=r.trading_day, version=r.version, phase=r.phase.value)
            self.spawn(r)
            resumed.append(r)
        return resumed

    async def stop(self) -> None:
        tasks = [t for t in (*self._tasks.values(), *self._side) if not t.done()]
        for t in tasks:
            t.cancel()
        for t in tasks:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await t

    def _spawn_side(self, coro: Any, name: str) -> None:
        task = asyncio.create_task(coro, name=name)
        self._side.add(task)

        def _done(t: asyncio.Task[Any]) -> None:
            self._side.discard(t)
            if not t.cancelled() and t.exception() is not None:
                logger.opt(exception=t.exception()).error("eod.side_task_failed", task=name)

        task.add_done_callback(_done)

    # ------------------------------------------------------------------ drive
    async def _drive(self, ctx: _Ctx) -> None:
        steps: dict[EodPhase, Callable[[_Ctx], Any]] = {
            EodPhase.BEGIN: self._step_freeze,
            EodPhase.FROZEN: self._step_reconcile,
            EodPhase.TRADES_RECONCILED: self._step_prices,
            EodPhase.PRICES_FIXED: self._step_settle,
            EodPhase.SETTLED: self._step_roll,
            EodPhase.ROLLED: self._step_report,
            EodPhase.REPORTED: self._step_done,
        }
        while not ctx.run.terminal:
            phase = ctx.run.phase
            try:
                await steps[phase](ctx)
            except asyncio.CancelledError:
                raise
            except SagaFailure as f:
                await self._fail(ctx, f.error, f.checks)
                return
            except Exception as exc:
                logger.opt(exception=exc).error(
                    "eod.step_error", trading_day=ctx.run.trading_day, version=ctx.run.version, phase=phase.value
                )
                if phase_index(phase) < phase_index(EodPhase.SETTLED):
                    await self._fail(ctx, f"internal error after {phase.value}: {type(exc).__name__}: {exc}")
                    return
                raise
            if ctx.run.phase == phase:  # every step moves the run (forward, or back to FROZEN)
                raise RuntimeError(f"EOD step after {phase} did not advance the run")

    async def _advance(
        self,
        ctx: _Ctx,
        phase: EodPhase,
        detail: dict[str, Any],
        *,
        extra: dict[str, Any] | None = None,
        checks: Sequence[CheckResult] | None = None,
        check_kinds: Iterable[ReconCheck] = (),
        extra_tx: TxHook | None = None,
        error: str | None = None,
        ts_ns: int | None = None,
    ) -> None:
        """Persist ``phase`` (+ step log, checks, ``extra_tx`` writes) and ``sys.eod`` in ONE transaction.

        ``extra_tx(conn, detail)`` runs first inside the transaction and may add
        entries to ``detail`` (e.g. the snapshot version actually allocated).
        """
        now = ts_ns if ts_ns is not None else self._rt.market_clock.now_ns()
        run = ctx.run
        kinds = list(check_kinds)

        def tx(conn: Connection) -> EodRun:
            if extra_tx is not None:
                extra_tx(conn, detail)
            if checks is not None:
                store.replace_checks(conn, run.trading_day, run.version, checks, kinds=kinds, created_ns=now)
            updated = replace(
                run,
                phase=phase,
                step_log={**run.step_log, phase.value: {"ts_ns": now, **detail, **(extra or {})}},
                updated_ns=now,
                error=error if error is not None else run.error,
                finished_ns=now if phase in (EodPhase.DONE, EodPhase.FAILED) else run.finished_ns,
            )
            store.update_run(conn, updated)
            self._rt.append_fact(
                conn,
                SYS_EOD,
                EodPhaseEvent(
                    trading_day=run.trading_day,
                    version=run.version,
                    phase=phase,
                    next_trading_day=run.next_trading_day,
                    detail=dict(detail),
                    error=error,
                    ts_ns=now,
                ),
                td=run.trading_day,
            )
            return updated

        ctx.run = await self._db.run_write(tx)
        self.latest[run.trading_day] = (run.version, phase)
        logger.info("eod.phase", trading_day=run.trading_day, version=run.version, phase=phase.value, error=error)

    async def _fail(self, ctx: _Ctx, error: str, checks: Sequence[CheckResult] | None = None) -> None:
        run = ctx.run
        if phase_index(run.phase) >= phase_index(EodPhase.SETTLED):
            logger.error("eod.error_after_settled", trading_day=run.trading_day, version=run.version, error=error)
            return
        kinds = sorted({c.check for c in checks or []})
        detail: dict[str, Any] = {"failed_after": run.phase.value}
        failures = [c for c in checks or [] if c.status == CheckStatus.FAIL]
        if failures:  # 02 §8.4 第 3/5 步：FAILED 的 detail 中列出差异
            detail["differences"] = [
                {
                    "check": c.check.value,
                    "account_id": c.account_id,
                    "expected": (c.detail or {}).get("expected"),
                    "actual": (c.detail or {}).get("actual"),
                }
                for c in failures[:50]
            ]
        await self._advance(
            ctx,
            EodPhase.FAILED,
            detail,
            checks=checks if checks else None,
            check_kinds=kinds,
            error=error,
        )
        logger.error("eod.failed", trading_day=run.trading_day, version=run.version, error=error)

    def warn_later(self, trading_day: str, version: int, kind: str, message: str, extra: Mapping[str, Any]) -> None:
        """Record a WARN on a run from a callback (sys.eod event + step log, :mod:`.eodwarn`)."""
        now = self._rt.market_clock.now_ns()

        def tx(conn: Connection) -> None:
            run = store.get_run(conn, trading_day, version)
            if run is not None:
                eodwarn.warn_tx(conn, self._rt, run, kind, message, now_ns=now, extra=extra)

        async def write() -> None:
            await self._db.run_write(tx)

        self._spawn_side(write(), f"clearing:warn:{trading_day}:v{version}")

    # ---------------------------------------------------------- step 2: freeze
    async def _freeze_one(self, run: EodRun, shard: str) -> EodFreezeAck:
        reply = await self._rt.request(
            shard,
            "eod.freeze",
            EodFreezeRequest(trading_day=run.trading_day, version=run.version),
            timeout_ms=self._settings.freeze_timeout_ms,
            retries=self._settings.freeze_retries,
        )
        if not reply.ok:
            raise SagaFailure(f"eod.freeze {shard}: {reply.code} {reply.message or ''}".strip())
        try:
            ack = reply.open_as(EodFreezeAck)
        except (msgspec.DecodeError, msgspec.ValidationError) as exc:
            raise SagaFailure(f"eod.freeze {shard}: bad EodFreezeAck ({exc})") from exc
        if not isinstance(ack, EodFreezeAck) or ack.trading_day != run.trading_day:
            raise SagaFailure(f"eod.freeze {shard}: no ack for {run.trading_day}")
        return ack

    async def _freeze_all(self, run: EodRun) -> dict[str, EodFreezeAck]:
        acks = await asyncio.gather(*(self._freeze_one(run, s) for s in self._shards))
        return dict(zip(self._shards, acks, strict=True))

    async def _wait_earlier_rolled(self, run: EodRun) -> None:
        """A resumed run of a later day waits until every earlier day's run is rolled (days chain in order)."""
        while True:
            earlier = await self._db.run_read(lambda c: store.unrolled_run_before(c, run.trading_day))
            if earlier is None:
                return
            logger.info("eod.waiting_for_earlier_day", trading_day=run.trading_day, earlier=earlier.trading_day)
            await asyncio.sleep(0.5)

    async def _step_freeze(self, ctx: _Ctx) -> None:
        await self._wait_earlier_rolled(ctx.run)
        ref = resettle_of(ctx.run)
        if ref is not None:  # 换日之后重新结算：分片已在下一交易日交易，不再冻结
            ctx.acks = {}
            await self._advance(
                ctx, EodPhase.FROZEN, {"resettle_of": ref, "shards": f"not frozen: the day was rolled by v{ref}"}
            )
            return
        acks = await self._freeze_all(ctx.run)
        ctx.acks = acks
        detail = {
            "shards": {
                inst: {
                    "shard": ack.shard,
                    "accounts": len(ack.accounts),
                    "trades": sum(d.count for d in ack.accounts),
                    "cash": sum(c.count for c in ack.cash),
                    "open_orders": ack.open_orders,
                }
                for inst, ack in sorted(acks.items())
            }
        }
        await self._advance(ctx, EodPhase.FROZEN, detail)

    # ------------------------------------------------------- step 3: reconcile
    async def _td_digest(self, td: str) -> TradeDigest:
        reply = await self._rt.request(
            TRADE_GATEWAY,
            "td.digest",
            DigestRequest(trading_day=td),
            timeout_ms=self._settings.query_timeout_ms,
            retries=self._settings.query_retries,
        )
        if not reply.ok:
            raise SagaFailure(f"td.digest: {reply.code} {reply.message or ''}".strip())
        dig = reply.open_as(TradeDigest)
        if not isinstance(dig, TradeDigest):
            raise SagaFailure("td.digest: empty reply")
        return dig

    async def _pull_trades(self, td: str, accounts: Iterable[str]) -> int:
        """Page ``td.query.trades`` for ``accounts`` into the local copy; returns new rows."""
        inserted = 0
        for acct in sorted(accounts):
            cursor: int | None = None
            for _ in range(10_000):
                reply = await self._rt.request(
                    TRADE_GATEWAY,
                    "td.query.trades",
                    TradeQuery(trading_day=td, account_id=acct, from_trade_seq=cursor, limit=500),
                    timeout_ms=self._settings.query_timeout_ms,
                    retries=self._settings.query_retries,
                )
                if not reply.ok:
                    raise SagaFailure(f"td.query.trades {acct}: {reply.code} {reply.message or ''}".strip())
                batch = reply.open_as(TradeBatch)
                rows = [t for t in batch.trades if t.trading_day == td]
                if rows:
                    inserted += await self._db.run_write(_insert_trades_job(self._rt, rows))
                if batch.complete or not batch.trades or batch.next_trade_seq == cursor:
                    break
                cursor = batch.next_trade_seq
        return inserted

    async def _pull_cash(self, td: str, accounts: Iterable[str]) -> int:
        """``risk.query.cash`` of the owning shard for ``accounts`` into the local copy; returns new rows."""
        cfg = self._rt.config
        targets: dict[str, list[str]] = {}
        for acct in sorted(accounts):
            spec = cfg.accounts.accounts.get(acct)
            inst = cfg.services.shard_instance(spec.shard) if spec is not None else None
            for s in [inst] if inst in self._shards else self._shards:
                assert s is not None
                targets.setdefault(s, []).append(acct)
        inserted = 0
        for inst, accts in sorted(targets.items()):
            for acct in accts:
                reply = await self._rt.request(
                    inst,
                    "risk.query.cash",
                    QueryCash(trading_day=td, account_id=acct),
                    timeout_ms=self._settings.query_timeout_ms,
                    retries=self._settings.query_retries,
                )
                if not reply.ok:
                    raise SagaFailure(f"risk.query.cash {inst}/{acct}: {reply.code} {reply.message or ''}".strip())
                batch = reply.open_as(CashBatch)
                items = [c for c in batch.items if c.trading_day == td]
                if items:
                    inserted += await self._db.run_write(_insert_cash_job(self._rt, items, inst))
        return inserted

    async def _load_copy(self, td: str) -> tuple[list[Trade], list[CashMovement]]:
        await self._db.run_write(lambda c: None)  # writer barrier: queued FACT copies are committed
        return await self._db.run_read(lambda c: (store.load_trades(c, td), store.load_cash(c, td)))

    async def _sod_accounts(self, run: EodRun) -> list[str]:
        version = pinned_sod_version(run)

        def q(c: Connection) -> list[str]:
            v = version
            if v is None:
                row = store.latest_sod_version(c, run.trading_day)
                v = row.version if row is not None else None
            if v is None:
                return []
            snap = store.load_sod(c, run.trading_day, v)
            return [a.account_id for a in snap.accounts] if snap is not None else []

        return await self._db.run_read(q)

    async def _step_reconcile(self, ctx: _Ctx) -> None:
        run = ctx.run
        td = run.trading_day
        used = int(run.log(EodPhase.FROZEN).get("refreezes_used", 0))
        budget = max(0, self._settings.recon_refreezes - used)
        ref_version = resettle_of(run)
        reference: dict[str, Any] | None = None
        acks: dict[str, EodFreezeAck]
        if ref_version is not None:
            ref_run = await self._db.run_read(lambda c: store.get_run(c, td, ref_version))
            reference = ref_run.log(EodPhase.TRADES_RECONCILED).get("digests") if ref_run is not None else None
            acks = {}
        else:
            acks = ctx.acks if ctx.acks is not None else await self._freeze_all(run)
        sod_accounts = await self._sod_accounts(run)
        pulled_trades = pulled_cash = refreezes = 0
        comparison: digests.DigestComparison | None = None
        trades: list[Trade] = []
        cash: list[CashMovement] = []
        for attempt in range(budget + 1):
            td_digest = await self._td_digest(td)
            trades, cash = await self._load_copy(td)
            if ref_version is None:
                comparison = digests.compare(
                    accounts=sod_accounts,
                    local_trades=digests.local_trade_digests(trades),
                    td_digest=td_digest,
                    acks=acks,
                    local_cash=digests.cash_digests(cash),
                )
            else:
                comparison = digests.compare_resettle(
                    accounts=sod_accounts,
                    local_trades=digests.local_trade_digests(trades),
                    td_digest=td_digest,
                    local_cash=digests.cash_digests(cash),
                    reference=reference,
                    reference_version=ref_version,
                )
            if comparison.ok:
                break
            logger.warning(
                "eod.digest_mismatch",
                trading_day=td,
                version=run.version,
                attempt=attempt,
                trades=sorted(comparison.trade_mismatch),
                cash=sorted(comparison.cash_mismatch),
            )
            if attempt >= budget:
                bad = sorted(comparison.trade_mismatch | comparison.cash_mismatch)
                raise SagaFailure(
                    f"trade/cash digests still differ after {used + refreezes} re-freezes: {', '.join(bad)}",
                    checks=comparison.checks,
                )
            # 先补拉缺失的数据，再重新 eod.freeze 取最新摘要（02 §8.4 第 3 步）
            if comparison.local_missing:
                pulled_trades += await self._pull_trades(td, comparison.local_missing)
            if comparison.cash_mismatch:
                pulled_cash += await self._pull_cash(td, comparison.cash_mismatch)
            if ref_version is None:
                acks = await self._freeze_all(run)
                ctx.acks = acks
            refreezes += 1
        assert comparison is not None
        checks = comparison.checks + digests.open_order_checks(acks)
        detail = {
            "trades": len(trades),
            "cash_movements": len(cash),
            "pulled_trades": pulled_trades,
            "pulled_cash": pulled_cash,
            "refreezes": refreezes,
            "refreezes_used": used + refreezes,
            "open_orders": sum(a.open_orders for a in acks.values()),
        }
        # H-2：固定对账通过时各账户的成交/出入金摘要，第 5 步只结算与之相同的数据
        extra = {"digests": digests.pin(trades, cash)}
        await self._advance(
            ctx, EodPhase.TRADES_RECONCILED, detail, extra=extra, checks=checks, check_kinds=DIGEST_KINDS
        )

    async def _rewind(self, ctx: _Ctx, reason: str, accounts: Sequence[str]) -> None:
        """Back to FROZEN (re-freeze and reconcile again), counted against the re-freeze budget (02 §8.4)."""
        run = ctx.run
        used = int(run.log(EodPhase.TRADES_RECONCILED).get("refreezes_used", 0))
        if used >= self._settings.recon_refreezes:
            raise SagaFailure(
                f"{reason} ({', '.join(accounts)}); no re-freeze left after {used} re-freezes"
            )
        logger.warning(
            "eod.rewind", trading_day=run.trading_day, version=run.version, reason=reason, accounts=list(accounts)
        )
        ctx.acks = None
        await self._advance(
            ctx,
            EodPhase.FROZEN,
            {"rewind": {"from": run.phase.value, "reason": reason, "accounts": list(accounts)}, "refreezes_used": used + 1},
        )

    # ---------------------------------------------------------- step 4: prices
    async def _settle_inputs(self, td: str) -> dict[str, SettleInput] | None:
        reply = await self._rt.request(
            MD_GATEWAY,
            "md.settle_inputs",
            SettleInputsRequest(trading_day=td),
            timeout_ms=self._settings.md_timeout_ms,
            retries=self._settings.md_retries,
        )
        if not reply.ok:
            logger.warning("eod.md_settle_inputs_failed", trading_day=td, code=reply.code, message=reply.message)
            return None
        batch = reply.open_as(SettleInputsBatch)
        return {i.instrument_id: i for i in batch.items if i.trading_day == td}

    async def _run_config(self, run: EodRun) -> tuple[FullConfig, InstrumentRegistry, str | None]:
        cfg, note = await self._configs.config(pinned_config_version(run))
        return cfg, self._configs.registry(cfg), note

    async def _step_prices(self, ctx: _Ctx) -> None:
        run = ctx.run
        td = run.trading_day
        cfg, registry, note = await self._run_config(run)
        sod_version = pinned_sod_version(run)
        # md.settle_inputs may take the whole md timeout: never under the price lock (M-4)
        fetched = await self._settle_inputs(td)
        async with self.price_lock(td):

            def q(c: Connection) -> tuple[Any, ...]:
                v = sod_version
                if v is None:
                    row = store.latest_sod_version(c, td)
                    v = row.version if row is not None else None
                snap = store.load_sod(c, td, v) if v is not None else None
                traded = {t.instrument_id for t in store.load_trades(c, td)}
                earlier: dict[str, str] = {}
                for r in store.runs_of(c, td):  # md inputs of earlier runs (newest wins)
                    if r.version != run.version:
                        earlier.update(r.log(EodPhase.PRICES_FIXED).get("input_pre_settle") or {})
                return snap, traded, store.active_overrides(c, td), store.latest_official(c, td), earlier

            snap, traded, overrides, official, earlier = await self._db.run_read(q)
            if snap is None:
                raise SagaFailure(f"no SOD snapshot for {td} (v{sod_version})")
            held = {lot.instrument_id for a in snap.accounts for lot in a.lots}
            needed = held | traded
            inputs: dict[str, SettleInput] = fetched or {}
            if fetched is None:
                uncovered = sorted(i for i in needed if i not in overrides and i not in official)
                if uncovered:
                    raise SagaFailure(
                        f"md-gateway unavailable for md.settle_inputs; no manual/official price for "
                        f"{', '.join(uncovered)}: {PRICE_REMEDY}"
                    )
            decision = decide_prices(
                registry=registry,
                trading_day=td,
                version=run.version,
                needed=needed,
                inputs=inputs,
                overrides=overrides,
                official=official,
                sod_pre=snap.pre_settle,
            )
            if decision.missing:
                raise SagaFailure(
                    f"no settlement price for {', '.join(decision.missing)} (no md input and no previous "
                    f"settlement price): {PRICE_REMEDY}"
                )
            fallback = fallback_pre_settle(
                touched=needed, sod_pre=snap.pre_settle, inputs=inputs, earlier_inputs=earlier, views=decision.views
            )
            views = [msgspec.to_builtins(v) for _, v in sorted(decision.views.items())]
            detail: dict[str, Any] = {
                "instruments": len(decision.views),
                "needed": len(needed),
                "sources": decision.sources(),
                "md_inputs": len(inputs),
                "md_available": fetched is not None,
                "config_version": cfg.config_version,
            }
            if note:
                detail["config_note"] = note
            if fallback:
                detail["pre_settle_fallback"] = {i: f["source"] for i, f in fallback.items()}
            warn_entries: list[dict[str, Any]] = []
            if decision.pre_mismatch:
                message = "md-gateway pre_settle differs from the SOD (SOD used): " + ", ".join(
                    f"{i} SOD {m['sod']} / md {m['md']}" for i, m in sorted(decision.pre_mismatch.items())
                )
                detail["pre_settle_mismatch"] = decision.pre_mismatch
                warn_entries.append({"kind": eodwarn.PRE_SETTLE_MISMATCH, "message": message})
            if warn_entries:
                detail["warnings"] = warn_entries
            # 固定本次结算用到的全部输入（价格视图、SOD 版本、md 给出的昨结算、补位的昨结算），恢复后原样复用
            extra = {
                "prices": views,
                "sod_version": snap.version,
                "input_pre_settle": {inst: inp.pre_settle for inst, inp in sorted(inputs.items())},
                "pre_settle_fallback_prices": {i: f["price"] for i, f in fallback.items()},
                "skipped": decision.skipped,
            }
            now = self._rt.market_clock.now_ns()

            def flag(conn: Connection, d: dict[str, Any]) -> None:
                for w in warn_entries:
                    store.add_run_warning(conn, td, run.version, {**w, "ts_ns": now})

            await self._advance(ctx, EodPhase.PRICES_FIXED, detail, extra=extra, extra_tx=flag, ts_ns=now)

    # ---------------------------------------------------------- step 5: settle
    async def _intraday_checks(self, run: EodRun, ds: DaySettlement, registry: InstrumentRegistry) -> list[CheckResult]:
        """``INTRADAY_VS_SETTLE`` (WARN only): shards' intraday equity vs settled equity (03 §6)."""
        cfg = self._rt.config
        by_shard: dict[str, list[str]] = {}
        for acct in ds.accounts:
            spec = cfg.accounts.accounts.get(acct)
            if spec is not None:
                by_shard.setdefault(cfg.services.shard_instance(spec.shard), []).append(acct)
        snapshots: list[AccountSnapshot] = []
        for inst, accts in sorted(by_shard.items()):
            if inst not in self._shards:
                continue
            reply = await self._rt.request(
                inst,
                "risk.query.accounts",
                QueryAccounts(account_ids=accts),
                timeout_ms=self._settings.report_fetch_timeout_ms,
                retries=0,
            )
            if not reply.ok:
                logger.info("eod.intraday_check_skipped", shard=inst, code=reply.code)
                continue
            try:
                batch = reply.open_as(AccountSnapshotBatch)
            except (msgspec.DecodeError, msgspec.ValidationError):
                continue
            snapshots.extend(s for s in batch.items if s.trading_day == run.trading_day)
        return intraday_checks(snapshots, ds, registry)

    async def _settle_in_pool(self, cfg: FullConfig, params: dict[str, Any]) -> DaySettlement:
        """:func:`jobs.settle_job` in the pool; a worker that died is retried once in a new pool (L7)."""
        for attempt in (1, 2):
            try:
                ds: DaySettlement = await self._pool.run(jobs.settle_job, cfg.products, cfg.calendar, params)
                return ds
            except SettlementError as exc:
                raise SagaFailure(f"settlement failed: {exc}") from exc
            except BrokenProcessPool as exc:
                if attempt == 2:
                    raise SagaFailure(f"settlement worker process died twice ({exc})") from exc
                logger.warning("eod.settle_worker_died_retry", trading_day=params["trading_day"])
        raise AssertionError("unreachable")  # pragma: no cover

    async def _step_settle(self, ctx: _Ctx) -> None:
        run = ctx.run
        td, version = run.trading_day, run.version
        pf = run.log(EodPhase.PRICES_FIXED)
        tr = run.log(EodPhase.TRADES_RECONCILED)
        sod_version = pinned_sod_version(run)
        if sod_version is None:
            sod_version = int(pf["sod_version"])
        views = [msgspec.convert(v, SettlementPriceView) for v in pf.get("prices", [])]
        next_td = run.next_trading_day
        cfg, registry, note = await self._run_config(run)

        def q(c: Connection) -> tuple[Any, ...]:
            return (
                store.load_sod(c, td, sod_version),
                store.load_trades(c, td),
                store.load_cash(c, td),
                store.next_sod_version(c, next_td, seed=False),
                [k for k in store.load_checks(c, td, version) if k.check in DIGEST_KINDS],
            )

        await self._db.run_write(lambda c: None)  # writer barrier: queued FACT copies are committed
        sod, trades, cash, next_version, digest_checks = await self._db.run_read(q)
        if sod is None:
            raise SagaFailure(f"SOD snapshot {td} v{sod_version} is missing")
        pinned = tr.get("digests")
        if pinned is not None:
            changed = digests.changed_accounts(pinned, trades, cash)
            if changed:
                await self._rewind(ctx, "the local copy changed after TRADES_RECONCILED", changed)
                return
        pre_settle = {inst: D(px) for inst, px in (pf.get("input_pre_settle") or {}).items()}
        for inst, px in (pf.get("pre_settle_fallback_prices") or {}).items():
            pre_settle.setdefault(inst, D(px))
        params = {
            "trading_day": td,
            "version": version,
            "sod": sod,
            "trades": trades,  # arrival order (trade_seq ascending): settle_day sorts stably
            "cash_movements": cash,
            "settle_prices": {v.instrument_id: v for v in views},
            "next_trading_day": next_td,
            "sod_version": next_version,
            "pre_settle": pre_settle,
        }
        ds = await self._settle_in_pool(cfg, params)
        settle_checks = list(ds.checks) + await self._intraday_checks(run, ds, registry)
        if not ds.ok:
            failed = sorted({f"{c.check.value}:{c.account_id or 'firm'}" for c in ds.failed_checks()})
            raise SagaFailure(f"settlement checks failed: {', '.join(failed)}", checks=settle_checks)
        all_checks = digest_checks + settle_checks
        statuses: dict[str, int] = {}
        for c in all_checks:
            statuses[c.status.value] = statuses.get(c.status.value, 0) + 1
        now = self._rt.market_clock.now_ns()
        completed = msgspec.structs.replace(ds.completed(now), checks=all_checks)
        detail: dict[str, Any] = {
            "accounts": len(ds.result.accounts),
            "positions": len(ds.result.positions),
            "trades": len(ds.result.trades),
            "checks": dict(sorted(statuses.items())),
            "equity": money_str(sum((a.equity for a in ds.accounts.values()), Decimal(0))),
            "config_version": cfg.config_version,
        }
        if note:
            detail["config_note"] = note
        counts = copy_counts_of(trades, cash)

        def extra_tx(conn: Connection, d: dict[str, Any]) -> None:
            if store.copy_counts(conn, td) != counts:  # a fact committed while the pool was settling
                raise _CopyChanged()
            store.save_settlement(conn, ds.result, created_ns=now)
            next_sod = ds.next_sod
            fresh = store.next_sod_version(conn, next_td, seed=False)
            if fresh != next_sod.version:  # a seed of next_td was written meanwhile
                next_sod = msgspec.structs.replace(next_sod, version=fresh)
            store.save_sod(conn, next_sod, source=SodSource.EOD, created_ns=now)
            self._rt.append_fact(conn, CLEARING_SETTLED, completed, td=td)
            d["sod_version"] = next_sod.version

        try:
            await self._advance(
                ctx,
                EodPhase.SETTLED,
                detail,
                checks=settle_checks,
                check_kinds=SETTLE_KINDS,
                extra_tx=extra_tx,
                ts_ns=now,
            )
        except _CopyChanged:
            fresh_counts = await self._db.run_read(lambda c: store.copy_counts(c, td))
            changed = sorted(a for a in set(fresh_counts) | set(counts) if fresh_counts.get(a) != counts.get(a))
            await self._rewind(ctx, "facts reached the local copy while settling", changed)

    # ------------------------------------------------------------ step 6: roll
    def _push_stuck(self, run: EodRun) -> Callable[[PushTarget], None]:
        def on_stuck(target: PushTarget) -> None:
            message = (
                f"eod.rollover {target.next_td} v{target.version} not confirmed by "
                f"{', '.join(target.pending)} after the retries; re-pushed when the shard comes back"
            )
            self.warn_later(
                run.trading_day,
                run.version,
                eodwarn.PUSH_INCOMPLETE,
                message,
                {"shards": target.pending, "errors": dict(target.errors), "next_trading_day": target.next_td},
            )

        return on_stuck

    async def _step_roll(self, ctx: _Ctx) -> None:
        run = ctx.run
        sod_version = int(run.log(EodPhase.SETTLED)["sod_version"])
        next_td = run.next_trading_day

        async def finalize(ns: int, shards: list[str]) -> None:
            # 所有分片都确认后：pushed_ns + eod_runs ROLLED + sys.eod，同一事务（03 §7 推送完成）
            if ctx.run.phase != EodPhase.SETTLED:
                return

            def mark(conn: Connection, d: dict[str, Any]) -> None:
                store.set_pushed(conn, next_td, sod_version, ns)

            await self._advance(
                ctx, EodPhase.ROLLED, {"sod_version": sod_version, "shards": shards}, extra_tx=mark, ts_ns=ns
            )

        outcome = await self._pushes.push(
            run.trading_day,
            next_td,
            sod_version,
            source=SodSource.EOD.value,
            finalize=finalize,
            on_stuck=self._push_stuck(run),
        )
        if outcome == PushOutcome.SUPERSEDED and ctx.run.phase == EodPhase.SETTLED:
            # a newer version of next_td (seed correction) reached every shard
            await self._advance(ctx, EodPhase.ROLLED, {"sod_version": sod_version, "superseded": True})

    # ---------------------------------------------------------- step 7: report
    async def _step_report(self, ctx: _Ctx) -> None:
        run = ctx.run
        errors: list[str] = []
        produced = 0
        try:
            result = await self._reports(run.trading_day, run.version)
            produced = len(result or [])
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # 02 §8.4 step 7: report failures are only a WARN
            logger.opt(exception=exc).warning("eod.reports_failed", trading_day=run.trading_day, version=run.version)
            errors.append(f"{type(exc).__name__}: {exc}")
        detail: dict[str, Any] = {"reports": produced}
        if errors:
            detail["errors"] = errors
        await self._advance(ctx, EodPhase.REPORTED, detail)

    # ------------------------------------------------------------ step 8: done
    async def _step_done(self, ctx: _Ctx) -> None:
        run = ctx.run
        now = self._rt.market_clock.now_ns()
        elapsed = max(0, (now - (run.started_ns or now)) // 1_000_000)
        await self._advance(ctx, EodPhase.DONE, {"elapsed_ms": elapsed}, ts_ns=now)
        if self._on_done is not None:
            try:
                self._on_done(ctx.run)
            except Exception:
                logger.exception("eod.on_done_failed")


def intraday_checks(
    snapshots: Sequence[AccountSnapshot], ds: DaySettlement, registry: InstrumentRegistry | None = None
) -> list[CheckResult]:
    """``INTRADAY_VS_SETTLE``: intraday (last-price) equity vs settled equity; WARN outside the explainable range.

    Explainable = Σ |settle − last| × qty × multiplier over the snapshot's
    positions + |intraday fee − settled fee| + 1 yuan of rounding (03 §6:
    差异超过"价格差异 × 持仓"的解释范围时记为 WARN).
    """
    prices = {p.instrument_id: D(p.price) for p in ds.result.prices}
    mult = {p.instrument_id: p.multiplier for p in ds.positions}
    out: list[CheckResult] = []
    for snap in sorted(snapshots, key=lambda s: s.account_id):
        acct = ds.accounts.get(snap.account_id)
        if acct is None:
            continue
        explain = Decimal(0)
        unpriced: list[str] = []
        for pos in snap.positions:
            qty = pos.td_qty + pos.yd_qty
            if qty == 0:
                continue
            px = prices.get(pos.instrument_id)
            m = mult.get(pos.instrument_id)
            if m is None and registry is not None and registry.has(pos.instrument_id):
                m = registry.multiplier(pos.instrument_id)
            if px is None or m is None or pos.last is None:
                unpriced.append(pos.instrument_id)
                continue
            explain += abs(px - D(pos.last)) * qty * m
        explain += abs(D(snap.funds.fee) - acct.fee)
        intraday = D(snap.funds.equity)
        diff = intraday - acct.equity
        ok = abs(diff) <= explain + Decimal(1) and not unpriced
        out.append(
            CheckResult(
                check=ReconCheck.INTRADAY_VS_SETTLE,
                status=CheckStatus.PASS if ok else CheckStatus.WARN,
                account_id=snap.account_id,
                detail={
                    "expected": money_str(acct.equity),
                    "actual": money_str(intraday),
                    "diff": money_str(diff),
                    "explained": money_str(explain),
                    "unpriced": unpriced,
                },
            )
        )
    return out


__all__ = [
    "DIGEST_KINDS",
    "PRICE_REMEDY",
    "SETTLE_KINDS",
    "EodSaga",
    "RunDecision",
    "SagaFailure",
    "copy_counts_of",
    "intraday_checks",
    "pinned_config_version",
    "pinned_sod_version",
    "resettle_of",
]
