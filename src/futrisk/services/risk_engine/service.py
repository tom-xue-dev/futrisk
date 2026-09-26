"""risk-engine shell: sockets, SQLite, recovery, EOD, coordinator duties around the EngineCore (01 §5.3.1, 02 §8.1).

The business logic lives in :class:`futrisk.core.engine.EngineCore` (pure, deterministic; see
``futrisk/core/ENGINE.md``). This module is the process around it, on a
:class:`~futrisk.runtime.SyncServiceRuntime`: one business thread owns every socket it polls and
the core; the runtime adds the publisher thread (PUB + heartbeats) and the risk-{shard}.db
writer thread.

One loop iteration (02 §8.1)::

    poll(min(runtime timers, core.next_timer_ns, shell timers))
    pump_system()         0) sys.clock + heartbeats: peer liveness (md / trade-gateway), FACT heads;
                             coordinator: running shards' heartbeats -> note_shard_alive
    md clone pump         1) live md.tick.* conflated per instrument (latest wins); md.status applied (the
                             quality authority); ctl.snapshot ticks applied at once as prices only (live=False)
    shard / sys.eod pump     coordinator: other shards' risk.snapshot.* -> ingest_snapshot
    pump_fact()           2) td.exec.{acct} (FROM_BEGINNING) -> apply_exec; risk.control.firm -> apply_firm_control
    pump_replies()        3) route.* / td.query.* / settle.sod_snapshot / ctl.snapshot results
    pump_requests(256)    4) commands -> EngineCore.handle (history queries answered from risk.db)
    apply marks           5) slots -> apply_mark
    evaluate_dirty()      6) once per loop (no-op while LOADING)
    on_timer()            7) when due; then the shell timers (retries, start-up steps) and run_timers()

Effects are executed in the order the core returns them (ENGINE.md §3), with a group commit: the effects of one
pump (steps 2, 3, 4 and the timers) are collected, their ``Persist`` rows, outbox messages (``created_ns =
OutboxMsg.ts_ns``) and FACT cursors go into ONE writer transaction (``synchronous=FULL`` when any is critical) and
the loop waits for it; then the ``Route`` / ``CancelRoute`` / ``ResolveRoute`` / ``Request``s are sent, the LV
``Publish``es published and the ``ReplyTo``s answered, in their order - never before the commit of their batch.

Crash-only: a failed commit (:class:`PersistFailed`, a ``BaseException`` no framework handler swallows) or an engine
error latches :attr:`RiskEngineService.poisoned`: from then on no effect leaves the process and every command is
answered ``UNAVAILABLE`` / ``REJECT_SYSTEM_NOT_READY`` without reaching the core (the commands of the failed batch
get ``UNAVAILABLE``); the process exits 1 with the database closed unclean and restarts from the durable state.
``ctl.shutdown`` (or a signal) sets the core ``STOPPING``; commands still queued behind it are refused the same way.

Start-up (01 §6.5, 02 §8.6, ENGINE.md §8), phases :class:`Phase`:

* ``CLOCK``: waiting for the market clock (``sys.clock`` snapshot); orders ``REJECT_SYSTEM_NOT_READY`` (the core,
  LOADING), state-changing commands ``UNAVAILABLE`` (retryable), ``eod.*`` held;
* ``SOD``: fresh database: ``settle.sod_snapshot`` (no version) every 5 s (30 s after ``NOT_FOUND``) until it or the
  seed push ``eod.rollover`` delivers a SOD; the catch-up starts in the same loop step (nothing is evaluated on the
  bare SOD);
* ``CATCHUP``: SOD restored or loaded, core ``LOADING`` (orders ``REJECT_SYSTEM_NOT_READY``, no evaluation):
  ``td.exec`` FACT from the persisted cursors plus ``td.query.trades`` of the whole day per account (trade-key
  dedupe); ``eod.freeze`` / ``eod.rollover`` are held until caught up (the clearing-worker fails fast on retryable
  errors, so they are answered late, not refused);
* ``WARMUP``: core ``WARMUP`` (``SYSTEM:STARTING``: opens blocked, closes allowed) until the position recon passed
  (``td.query.positions``) and every held instrument in session has a LIVE tick received by this process with
  quality OK (snapshot prices never count; at most ``warmup_marks_timeout_s``);
* ``RUNNING``: ``evaluate_all`` then core ``RUNNING``; service state RUNNING (DEGRADED while md-gateway or
  trade-gateway is down). Every phase change is in ``ctl.status`` / the next heartbeat at once (``detail.phase``).

A HALTED (latched) account stays HALTED across restarts: ``account_states`` / ``rule_states`` / ``alerts`` are
restored and the deterministic re-evaluation raises no alert twice (alert keys are idempotent).

Fact repair (02 §4.6, §8.2): a failed replay sets ``SYSTEM:FILL_GAP``; an expired replay window opens a non-blocking
REPLAY_EXPIRED break, a stream change (announced by the publisher's heartbeat) triggers the same domain fallback
(``td.query.trades`` + ``td.query.orders``), after which the cursor is stored where the tracker continues (no second
fallback at the next start); an undecodable fill opens a blocking POSITION_MISMATCH break plus the fallback.
"""

from __future__ import annotations

import gc
import heapq
import itertools
import math
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

import msgspec
from loguru import logger
from sqlalchemy import select

from futrisk.bus.fact import CursorUpdate, FactKey
from futrisk.bus.heartbeat import DOWN, UP
from futrisk.bus.rpc_client import Completion
from futrisk.bus.rpc_common import DEFERRED, HandlerResult, RequestContext
from futrisk.common.clock import td_str
from futrisk.common.config import FullConfig
from futrisk.common.ids import Uuid7Gen
from futrisk.core.alerts import audit
from futrisk.core.effects import (
    CancelRoute,
    CommandCtx,
    Effect,
    Persist,
    Publish,
    ReplyTo,
    Request,
    ResolveRoute,
    Route,
    Tx,
    merge_effects,
)
from futrisk.core.engine import EngineCore
from futrisk.core.eod import cash_from_row, digest_only_ack, trade_from_fill_row
from futrisk.core.instruments import InstrumentRegistry
from futrisk.core.metrics import registered_metrics
from futrisk.core.orders import TERMINAL
from futrisk.core.risk_account import Lifecycle
from futrisk.core.rules import RuleBook
from futrisk.db import Database, FactCursors, PreparedSql
from futrisk.messages import codes, topics
from futrisk.messages.base import Msg, decode
from futrisk.messages.common import SnapshotBatch, SnapshotRequest
from futrisk.messages.enums import (
    ConfigEffect,
    ControlOrigin,
    EodPhase,
    FactStartPolicy,
    FirmAction,
    MdQuality,
    OrderStatus,
    ReconBreakKind,
    ServiceState,
)
from futrisk.messages.envelope import Envelope, Reply, decode_envelope, err_reply, ok_reply
from futrisk.messages.eod import (
    DayRoll,
    EodFreezeRequest,
    EodPhaseEvent,
    EodRolloverRequest,
    ForceUnfreezeRequest,
    SodRequest,
    SodSnapshot,
)
from futrisk.messages.md import MdStatus, Tick
from futrisk.messages.orders import (
    BrokerOrderBatch,
    BrokerOrderUpdate,
    CancelAllRequest,
    CancelRequest,
    OrderAck,
    OrderQuery,
    OrderRequest,
    RouteCancel,
    RouteOrder,
    RouteResolve,
    Trade,
    TradeBatch,
    TradeQuery,
)
from futrisk.messages.risk import (
    AccountSnapshot,
    AckAlertRequest,
    CashBatch,
    CashMovement,
    CashMoveRequest,
    FillBatch,
    FirmControl,
    HaltRequest,
    KillSwitchRequest,
    LiquidationConfirm,
    LiquidationPlanBatch,
    OrderBatch,
    QueryAccounts,
    QueryAlerts,
    QueryCash,
    QueryFills,
    QueryLiquidationPlans,
    QueryMetrics,
    QueryOrders,
    QueryPositions,
    QueryReconBreaks,
    QueryRules,
    ReconResolveRequest,
    ResetRequest,
    RiskEventBatch,
)
from futrisk.messages.system import Heartbeat
from futrisk.runtime import SyncServiceRuntime
from futrisk.runtime.sync_runtime import SyncSubscription
from futrisk.services.risk_engine import schema as S
from futrisk.services.risk_engine.maintenance import run_maintenance
from futrisk.services.risk_engine.schema import METADATA, MIGRATIONS, SYNCHRONOUS, db_name
from futrisk.services.risk_engine.stats import LatencyWindow, RateMeter
from futrisk.services.risk_engine.store import RiskStore

TRADE_GATEWAY = "trade-gateway"
MD_GATEWAY = "md-gateway"
CLEARING = "clearing-worker"
NS_PER_MS = 1_000_000
NS_PER_S = 1_000_000_000
MAINTENANCE_MARGIN_NS = 10 * 60 * NS_PER_S  # no maintenance within 10 minutes of the next session
STOP_DRAIN_MS = 20  # at stop: wait this long for requests still arriving, then refuse whatever reached the socket

#: every command of 02 §6.3 handled by the core (payload types checked by the ROUTER)
CORE_COMMANDS: dict[str, type[Msg]] = {
    "order.submit": OrderRequest,
    "order.cancel": CancelRequest,
    "order.cancel_all": CancelAllRequest,
    "risk.reset": ResetRequest,
    "risk.halt": HaltRequest,
    "risk.ack_alert": AckAlertRequest,
    "risk.liquidation.confirm": LiquidationConfirm,
    "risk.recon.resolve": ReconResolveRequest,
    "cash.move": CashMoveRequest,
    "firm.kill_switch": KillSwitchRequest,
    "eod.freeze": EodFreezeRequest,
    "eod.rollover": EodRolloverRequest,
    "eod.force_unfreeze": ForceUnfreezeRequest,
    "risk.query.accounts": QueryAccounts,
    "risk.query.positions": QueryPositions,
    "risk.query.orders": QueryOrders,
    "risk.query.fills": QueryFills,
    "risk.query.cash": QueryCash,
    "risk.query.recon_breaks": QueryReconBreaks,
    "risk.query.alerts": QueryAlerts,
    "risk.query.metrics": QueryMetrics,
    "risk.query.rules": QueryRules,
    "risk.query.liquidation_plans": QueryLiquidationPlans,
}
#: answered from risk.db: metrics always, the others for a trading day other than the current one (ENGINE.md §5)
HISTORY_COMMANDS = frozenset(
    {
        "risk.query.metrics",
        "risk.query.orders",
        "risk.query.fills",
        "risk.query.alerts",
        "risk.query.cash",
        "risk.query.liquidation_plans",
    }
)
QUERY_PREFIX = "risk.query."
_ORDER_DAY = PreparedSql("SELECT trading_day FROM orders WHERE client_order_id = ?")


class Phase(StrEnum):
    """Start-up phases of the shell (see the module docstring)."""

    CLOCK = "CLOCK"
    SOD = "SOD"
    CATCHUP = "CATCHUP"
    WARMUP = "WARMUP"
    RUNNING = "RUNNING"


_EARLY = (Phase.CLOCK, Phase.SOD, Phase.CATCHUP)


@dataclass(order=True)
class _Timer:
    due_ns: int
    seq: int
    fn: Callable[[], None] = field(compare=False)
    name: str = field(compare=False, default="")


@dataclass
class _Deferred:
    deadline_ns: int
    command: str
    payload: Msg
    ctx: RequestContext


class PersistFailed(BaseException):
    """A risk.db commit failed: the core's memory moved on without its durable record (review C1).

    A ``BaseException`` so that no framework ``except Exception`` (RPC server, reply callbacks, peer
    callbacks) turns it into an error reply and carries on: it ends the loop, the process exits 1 with the
    database closed unclean and restarts from the durable state. The shell's poison latch
    (:attr:`RiskEngineService.poisoned`) already stops every further effect and command.
    """


@dataclass
class _EffectBatch:
    """Effects of one pump (commands, facts, replies): ONE commit, then the external effects in their order."""

    persists: list[Persist] = field(default_factory=list)
    cursors: list[CursorUpdate] = field(default_factory=list)
    tail: list[Effect] = field(default_factory=list)
    orders: list[int] = field(default_factory=list)  # start (mono ns) of each order.submit handled in the batch

    def __bool__(self) -> bool:
        return bool(self.persists or self.cursors or self.tail)


class RiskEngineService:
    """risk-engine-{shard} on a :class:`SyncServiceRuntime`; see the module docstring.

    Args:
        rt: the (not yet started) runtime of instance ``risk-engine-{shard}``.
        shard: shard number (accounts of ``accounts.yaml`` with this ``shard``).
        warmup_marks_timeout_s: longest wait for fresh LIVE marks of held instruments in WARMUP;
            afterwards the per-instrument quality rules of the core keep stale instruments blocked.
        defer_limit_s: how long ``eod.freeze`` / ``eod.rollover`` are held while starting (below the
            clearing-worker's 30 s timeout) before ``UNAVAILABLE`` is answered.
        max_wait_ms: poll timeout cap of the loop.
        maintenance_retry_s: how often a postponed daily maintenance checks whether the market is quiet.
    """

    def __init__(
        self,
        rt: SyncServiceRuntime,
        *,
        shard: int,
        warmup_marks_timeout_s: float = 10.0,
        defer_limit_s: float = 25.0,
        sod_retry_s: float = 5.0,
        sod_not_found_retry_s: float = 30.0,
        recon_retry_s: float = 5.0,
        max_wait_ms: int = 100,
        heartbeat_fields_s: float = 1.0,
        maintenance_retry_s: float = 60.0,
    ) -> None:
        self.rt = rt
        self.shard = shard
        self.warmup_marks_timeout_ns = int(warmup_marks_timeout_s * NS_PER_S)
        self.defer_limit_ns = int(defer_limit_s * NS_PER_S)
        self.sod_retry_ns = int(sod_retry_s * NS_PER_S)
        self.sod_not_found_retry_ns = int(sod_not_found_retry_s * NS_PER_S)
        self.recon_retry_ns = int(recon_retry_s * NS_PER_S)
        self.max_wait_ms = max(1, max_wait_ms)
        self.heartbeat_fields_ns = int(heartbeat_fields_s * NS_PER_S)
        self.maintenance_retry_ns = int(maintenance_retry_s * NS_PER_S)
        self.phase = Phase.CLOCK
        self.core: EngineCore | None = None
        self.db: Database | None = None
        self.store: RiskStore | None = None
        self.cursors: FactCursors | None = None
        self.coordinator_instance = "risk-engine-0"
        self.is_coordinator = False
        self.on_step: Callable[[], None] | None = None  # test hook, runs on the business thread
        self._started = False
        self._slots: dict[str, Envelope] = {}
        self._timers: list[_Timer] = []
        self._timer_seq = itertools.count()
        self._deferred: list[_Deferred] = []
        self._md_sub: SyncSubscription | None = None
        self._subs: list[SyncSubscription] = []
        self._exec_subscribed = False
        self._firm_subscribed = False
        self._fresh_keys: set[FactKey] = set()
        self._catchup_pending: set[str] = set()
        self._gap_reasons: dict[str, set[str]] = {}
        self._md_up = True
        self._md_resync_from: int | None = None
        self._tg_up = True
        self._shard_of: dict[str, int] = {}  # coordinator: other risk-engine instances -> shard
        self._sod_warned = False
        self._sod_inflight = False
        self._recon_replies = 0
        self._recon_last_ok = False
        self._recon_mark: int | None = None
        self._recon_ok = False
        self._recon_retry_at: int | None = None
        self._warmup_since_ns = 0
        self._timer_floor_ns = 0
        self._last_timer_call_ns = 0
        self._clock_version = -1
        self._next_hb_ns = 0
        self._maint_days: set[str] = set()
        self._maint_waiting: set[str] = set()
        self._maint_running = False
        self._staged: dict[str, Any] = {}
        self.maintenance_reports: list[Any] = []
        self.poisoned: BaseException | None = None  # set by the first fatal error: nothing leaves the process after it
        self._batch: _EffectBatch | None = None
        self._stopping = False
        self._md_live = False  # True while the md SUB socket is pumped (live stream, not a ctl.snapshot item)
        self._own_sockets: set[Any] = set()  # SUB sockets of this service (md clone, shard snapshots, sys.eod)
        self._core_next_ns: int | None = None  # cached core.next_timer_ns() (see _core_next_due)
        self._core_next_dirty = True
        self._live_ticks: set[str] = set()  # instruments with a live tick in this process (fresh-marks check)
        self._last_md_status: MdStatus | None = None
        self.lat = {
            "pretrade": LatencyWindow(),
            "order_submit": LatencyWindow(),
            "tick_eval": LatencyWindow(),
            "persist": LatencyWindow(),
            "batch": LatencyWindow(),
        }
        self.rates = RateMeter()

    # ================================================================== lifecycle
    @property
    def instance(self) -> str:
        return self.rt.instance

    def _mono(self) -> int:
        return self.rt.market_clock.monotonic_ns()

    def _now(self) -> int:
        return self.rt.market_clock.now_ns()

    def start(self) -> None:
        """Start the runtime, open risk-{shard}.db, build the core, register commands and subscriptions."""
        if self._started:
            return
        self._started = True
        rt = self.rt
        rt.start()
        cfg = rt.config
        services = cfg.services
        inst = services.instance(rt.instance)
        self.coordinator_instance = services.coordinator()
        self.is_coordinator = bool(inst.coordinator) or rt.instance == self.coordinator_instance
        self.db = rt.open_database(db_name(self.shard), METADATA, migrations=MIGRATIONS, synchronous=SYNCHRONOUS)
        self.store = RiskStore(rt.outbox, epoch=rt.epoch, instance=rt.instance)
        self.cursors = FactCursors(self.db)
        self.core = EngineCore(
            shard=self.shard,
            registry=InstrumentRegistry(cfg.products, cfg.calendar),
            rules=cfg.risk_rules,
            accounts=cfg.accounts,
            clock=rt.market_clock,
            ids=Uuid7Gen(),
            calendar=cfg.calendar,
            instance=rt.instance,
            coordinator=self.is_coordinator,
            config_hash=cfg.config_hash,
            config_version=cfg.config_version,
            coordinator_instance=self.coordinator_instance,
        )
        for command, ptype in CORE_COMMANDS.items():
            rt.add_handler(command, self._on_command, payload_type=ptype)
        rt.add_config_validator(self._validate_config)
        rt.on_config_commit(self._on_config_commit)
        peers = [MD_GATEWAY, TRADE_GATEWAY]
        if not self.is_coordinator and services.instance(self.coordinator_instance).pub:
            peers.append(self.coordinator_instance)
        rt.watch_peers(peers, on_change=self._on_peer)
        # 1) market data: late-joiner Clone (ctl.snapshot, then live), ticks conflated per instrument
        self._md_sub = rt.subscribe(MD_GATEWAY, [topics.MD_TICK, topics.MD_STATUS], self._on_md, snapshot=True)
        rt.subscribe_fact(
            MD_GATEWAY,
            [topics.MD_EVENT],
            policy=FactStartPolicy.FROM_NOW,
            apply=self._apply_md_event,
            cursors=self.cursors.load_all(),
        )
        if self.is_coordinator:
            for other in services.risk_instances():
                ic = services.instance(other)
                if other != rt.instance and ic.pub:
                    self._subs.append(rt.subscribe(other, [topics.RISK_SNAPSHOT], self._on_shard_snapshot, snapshot=True))
                    self._shard_of[other] = ic.shard if ic.shard is not None else int(other.rsplit("-", 1)[1])
            if self._shard_of:
                rt.on_heartbeat(self._on_shard_heartbeat, publishers=sorted(self._shard_of))
        if services.instance(CLEARING).pub:
            self._subs.append(rt.subscribe(CLEARING, [topics.SYS_EOD], self._on_sys_eod))
        for target in (TRADE_GATEWAY, CLEARING):
            if services.instance(target).router:
                rt.connect(target)
        self._own_sockets = {s.socket for s in (self._md_sub, *self._subs) if s is not None}
        rt.set_heartbeat_fields(detail=self._detail())  # deps come from the peer liveness of the runtime
        logger.info(
            "risk.started",
            instance=rt.instance,
            shard=self.shard,
            coordinator=self.is_coordinator,
            accounts=list(self.core.owned),
            db=str(self.db.path),
        )

    def run(self, *, install_signals: bool = False) -> int:
        """Run until stopped (``ctl.shutdown``, signal, parent exit, fatal error); returns the exit code."""
        rt = self.rt
        try:
            if install_signals:
                rt.install_signal_handlers()
            self.start()
            while rt.running:
                self.step()
        except BaseException as exc:
            rt.fatal(exc, "risk-engine business loop")
        finally:
            self.stop()
        return rt.exit_code

    def stop(self) -> None:
        self._stopping = True
        for d in self._deferred:  # held eod.* commands: retryable, the clearing-worker asks again after the restart
            self.rt.reply(d.ctx, err_reply(codes.UNAVAILABLE, "risk engine stopping"))
        self._deferred = []
        if self.rt.server is not None:  # requests that reached the socket after the last batch: refused, not dropped
            try:
                self.rt.poll(STOP_DRAIN_MS)
                self.rt.pump_requests(limit=1024)
            except Exception:
                logger.exception("risk.stop_drain_failed")
        self.rt.stop()

    def crash(self, reason: str = "simulated crash") -> None:
        """Stop as after an unexpected error (exit code 1, database closed unclean); tests only."""
        self._fatal(RuntimeError(reason), "test crash")

    def _fatal(self, exc: BaseException, where: str) -> None:
        """Crash-only (review C1): latch the poison first - from now on no effect leaves the process and no command
        reaches the core - then let the runtime stop the loop (exit code 1, databases closed unclean)."""
        if self.poisoned is None:
            self.poisoned = exc
        self.rt.fatal(exc, where)

    # ================================================================== the loop
    def step(self) -> None:
        """One iteration of the 02 §8.1 business loop."""
        rt = self.rt
        core = self.core
        assert core is not None
        ready = rt.poll(self._poll_timeout_ms())
        if self.poisoned is not None:
            rt.pump_requests(limit=256)  # commands still queued get UNAVAILABLE / REJECT_SYSTEM_NOT_READY (_on_command)
            return
        t_drain = self._mono()
        self._check_clock()
        # only the sockets the poller reported readable are drained (an empty socket costs a zmq.Again each time)
        server = rt.server
        server_sock = server.socket if server is not None else None
        rpc_socks = rt.rpc.sockets()
        runtime_ready = bool(ready) and any(
            s is not server_sock and s not in self._own_sockets and s not in rpc_socks for s in ready)
        if runtime_ready:  # the runtime's own SUBs: sys.clock + heartbeats, FACT subscriptions (and its wake-up pipe)
            rt.pump_system()
        md = self._md_sub
        if md is not None and md.socket in ready:
            self._md_live = True
            try:
                md.pump()
            finally:
                self._md_live = False
        for sub in self._subs:
            if sub.socket in ready:
                sub.pump()
        # group commit: every input of one pump -> ONE transaction, then routes / publishes / replies in order
        if runtime_ready:
            with self._batching():
                rt.pump_fact()
        completions: list[Completion] = []
        if any(s in ready for s in rpc_socks):
            with self._batching():
                completions = rt.pump_replies()
            self._sod_loaded()
        n_cmd = 0
        if server is not None and (server_sock in ready or server.backlog_size):
            with self._batching():
                n_cmd = rt.pump_requests(limit=256)
            self._sod_loaded()
        if n_cmd:
            self.rates.add("commands", n_cmd)
        applied = self._apply_marks()
        if core.lifecycle != Lifecycle.LOADING:
            self.execute(core.evaluate_dirty())
            if applied:
                self.lat["tick_eval"].add_ns(self._mono() - t_drain)
        self._core_timers()
        with self._batching():
            self._run_shell_timers()
            completions.extend(rt.run_timers())
        for c in completions:
            logger.debug("risk.completion", command=c.command, target=c.target, code=c.reply.code)
        with self._batching():
            self._phase_step()
        if self.on_step is not None:
            self.on_step()
        self._heartbeat_tick()
        rt.loop_done()

    def _sod_loaded(self) -> None:
        """A fresh database got its SOD (``settle.sod_snapshot`` or the seed ``eod.rollover``): catch up at once,
        before this step evaluates anything on the bare SOD book (review M1: the fills of today come first)."""
        core = self.core
        if self.phase == Phase.SOD and core is not None and core.trading_day:
            self._enter_catchup()

    def _poll_timeout_ms(self) -> int:
        t = self.rt.next_timer_ms(cap_ms=self.max_wait_ms)
        if t <= 0 or self._slots:
            return 0
        if self._timers:
            t = min(t, max(0, math.ceil((self._timers[0].due_ns - self._mono()) / NS_PER_MS)))
        core = self.core
        if core is not None and core.lifecycle != Lifecycle.LOADING:
            nt = self._core_next_due()
            if nt is not None:
                due = max(nt, self._timer_floor_ns)
                t = min(t, max(0, math.ceil((due - self._now()) / NS_PER_MS)))
        return max(0, t)

    def _core_next_due(self) -> int | None:
        """``core.next_timer_ns()`` - a scan over accounts x rules x levels - recomputed only after something could
        have moved it (an executed effect list, applied marks, md status, a peer / clock / phase change, a timer
        round), never on an idle poll. Due times are absolute, so a cached value stays valid while time passes."""
        if self._core_next_dirty:
            self._core_next_dirty = False
            core = self.core
            self._core_next_ns = core.next_timer_ns() if core is not None else None
        return self._core_next_ns

    def _check_clock(self) -> None:
        v = self.rt.market_clock.version
        if v != self._clock_version:
            if self._clock_version >= 0:
                logger.info("risk.clock_changed", version=v, market_now_ns=self._now(), offset_ns=self.rt.market_clock.offset_ns)
            self._clock_version = v
            self._timer_floor_ns = 0
            self._last_timer_call_ns = 0
            self._core_next_dirty = True

    def _core_timers(self) -> None:
        core = self.core
        assert core is not None
        if core.lifecycle == Lifecycle.LOADING:
            return
        now = self._now()
        nt = self._core_next_due()
        due = nt is not None and nt <= now and now >= self._timer_floor_ns
        if not due and now - self._last_timer_call_ns < 250 * NS_PER_MS:
            return
        self._last_timer_call_ns = now
        self.execute(core.on_timer(now))
        self._core_next_dirty = True
        after = self._core_next_due()
        # a timer that stays due (e.g. a PENDING level frozen by the quality gate) must not spin the loop
        self._timer_floor_ns = now + 20 * NS_PER_MS if after is not None and after <= now else 0

    # ------------------------------------------------------------ shell timers
    def _after(self, delay_ns: int, fn: Callable[[], None], name: str = "") -> None:
        heapq.heappush(self._timers, _Timer(self._mono() + max(0, delay_ns), next(self._timer_seq), fn, name))

    def _run_shell_timers(self) -> None:
        now = self._mono()
        while self._timers and self._timers[0].due_ns <= now:
            t = heapq.heappop(self._timers)
            t.fn()

    # ================================================================== market data
    def _on_md(self, env: Envelope) -> None:
        """md-gateway LV topics.

        Live ticks (the SUB socket, :attr:`_md_live`) are conflated per instrument and applied at step 5. Items of
        a ``ctl.snapshot`` (Clone sync / resync after md-gateway came back; possibly old ticks) are applied at once
        as prices only (``apply_mark(live=False)``: no quality upgrade, no new LIVE state, not a fresh mark for the
        warm-up), after the pending live ticks, and the latest ``md.status`` is applied again (review H1).
        """
        core = self.core
        assert core is not None
        self._core_next_dirty = True  # quality / marks: gated rule timers may start or stop
        if env.topic.startswith(topics.MD_TICK):
            if env.type != "Tick":
                return
            if self._md_live:
                self._slots[env.topic] = env
                self.rates.add("ticks")
                return
            self._apply_marks()
            tick = self._decode_tick(env)
            if tick is not None:
                core.apply_mark(tick, live=False)
                self.rates.add("snapshot_ticks")
                if self._last_md_status is not None:
                    core.apply_md_status(self._last_md_status)
            return
        if env.topic == topics.MD_STATUS and env.type == "MdStatus":
            self._apply_marks()  # ticks received before this status first (arrival order)
            try:
                status = decode(env.payload, MdStatus)
            except (msgspec.DecodeError, msgspec.ValidationError):
                logger.warning("risk.md_status_undecodable", src=env.src, seq=env.seq)
                return
            self._last_md_status = status
            core.apply_md_status(status)  # the authority on quality; never lifts set_md_down (heartbeat + resync do)

    @staticmethod
    def _decode_tick(env: Envelope) -> Tick | None:
        try:
            return decode(env.payload, Tick)
        except (msgspec.DecodeError, msgspec.ValidationError):
            logger.warning("risk.tick_undecodable", topic=env.topic, seq=env.seq)
            return None

    def _apply_marks(self) -> int:
        """Step 5: the conflated live ticks (the newest per instrument) -> ``apply_mark``."""
        if not self._slots:
            return 0
        slots, self._slots = self._slots, {}
        core = self.core
        assert core is not None
        n = 0
        for env in slots.values():
            tick = self._decode_tick(env)
            if tick is None:
                continue
            core.apply_mark(tick)
            self._live_ticks.add(tick.instrument_id)
            n += 1
        if n:
            self._core_next_dirty = True
        self.rates.add("marks", n)
        return n

    def _apply_md_event(self, env: Envelope, cur: CursorUpdate) -> None:
        """``md.event`` (FACT): no engine input today (RETRO is not implemented); log and advance the cursor."""
        logger.debug("risk.md_event", seq=env.seq, type=env.type)
        self.execute([], cursor=cur)

    # ================================================================== peers
    def _on_peer(self, peer: str, state: str) -> None:
        # PeerLiveness swallows callback errors: a failure here must still stop the service (crash-only)
        try:
            self._peer_change(peer, state)
        except Exception as exc:
            self._fatal(exc, f"peer {peer} {state}")

    def _peer_change(self, peer: str, state: str) -> None:
        core = self.core
        if core is None:
            return
        self._core_next_dirty = True
        if peer == MD_GATEWAY:
            if state == DOWN and self._md_up:
                self._md_up = False
                self._md_resync_from = None
                core.set_md_down(True)
                logger.warning("risk.md_down", detail="every instrument STALE: opens blocked, closes allowed")
            elif state == UP and not self._md_up:
                self._md_up = True
                sub = self._md_sub
                if sub is not None and sub.clone is not None:
                    self._md_resync_from = sub.clone.syncs
                    sub.clone.resync()  # 01 §8.5: back to normal after the heartbeat AND a fresh snapshot
                else:
                    core.set_md_down(False)
                logger.info("risk.md_up", detail="resyncing the md snapshot")
        elif peer == TRADE_GATEWAY:
            up = state == UP
            if up != self._tg_up:
                self._tg_up = up
                self.execute(core.set_gateway_status(up))
                logger.log("INFO" if up else "WARNING", "risk.gateway_status", up=up)
        elif peer == self.coordinator_instance and state == DOWN:
            logger.warning("risk.coordinator_down", coordinator=peer, detail="last known firm controls stay in force")
        self._update_service_state()

    def _check_md_resync(self) -> None:
        if self._md_resync_from is None:
            return
        sub = self._md_sub
        if sub is None or sub.clone is None or sub.clone.syncs > self._md_resync_from:
            self._md_resync_from = None
            if self._md_up and self.core is not None:
                self.core.set_md_down(False)
                logger.info("risk.md_resynced")

    # ================================================================== effects
    def execute(self, effects: Sequence[Effect], *, cursor: CursorUpdate | None = None) -> None:
        """Execute engine effects in their order (ENGINE.md §3); ``cursor`` joins the Persist transaction.

        Inside :meth:`_batching` (one pump of commands / facts / replies) the effects are collected: the batch is
        ONE transaction, and its routes, requests, publishes and replies follow the commit in their order (persist
        before route / reply per batch). After a fatal error nothing is executed any more (the poison latch).
        """
        if self.poisoned is not None:
            return
        if effects or cursor is not None:
            self._core_next_dirty = True
        b = self._batch
        if b is not None:
            for e in effects:
                if isinstance(e, Persist):
                    b.persists.append(e)
                else:
                    b.tail.append(e)
            if cursor is not None:
                b.cursors.append(cursor)
            return
        persists = [e for e in effects if isinstance(e, Persist)]
        if persists or cursor is not None:
            self._commit(persists, [cursor] if cursor is not None else [])
        self._external(effects)

    @contextmanager
    def _batching(self) -> Iterator[None]:
        """Collect the effects of the enclosed pump and run them as one batch (group commit)."""
        if self._batch is not None or self.poisoned is not None:
            yield
            return
        b = _EffectBatch()
        t0 = self._mono()
        self._batch = b
        try:
            yield
        finally:
            self._batch = None
        if not b:
            return
        if self.poisoned is not None:  # an engine error during the pump: nothing of the batch is executed
            self._refuse_batch(b)
            return
        if b.persists or b.cursors:
            try:
                self._commit(b.persists, b.cursors)
            except PersistFailed:
                self._refuse_batch(b)
                raise
        self._external(b.tail)
        end = self._mono()
        for t in b.orders:
            self.lat["order_submit"].add_ns(end - t)
        self.lat["batch"].add_ns(end - t0)

    def _commit(self, persists: Sequence[Persist], cursors: Sequence[CursorUpdate]) -> None:
        """ONE risk.db transaction: the rows and outbox messages of every Persist in order, then the newest FACT cursor
        of each key; ``synchronous=FULL`` when any Persist is critical. A failure is fatal (:class:`PersistFailed`)."""
        db, store = self.db, self.store
        assert db is not None and store is not None
        now = self._now()
        newest = {(c.src, c.topic): c for c in cursors}

        def job(conn: Any) -> None:
            for p in persists:
                store.apply_persist(conn, p, now_ns=now)
            for c in newest.values():
                store.save_cursor(conn, c, now_ns=now)

        t0 = self._mono()
        try:
            db.write(job, critical=any(p.critical for p in persists))
        except BaseException as exc:
            labels = ",".join(sorted({p.label for p in persists})) or "cursor"
            self._fatal(exc, f"persist {labels}")
            raise PersistFailed(f"risk.db commit of {labels} failed: {exc}") from exc
        self.lat["persist"].add_ns(self._mono() - t0)
        if not persists:
            return
        self.rates.add("persist", len(persists))
        for p in persists:
            for m in p.outbox:
                if m.topic == topics.SYS_DAYROLL and isinstance(m.payload, DayRoll):
                    dr = m.payload  # (the core asks td.query.trades of the new day itself: ENGINE.md §9)
                    logger.info("risk.dayroll", mode=dr.mode.value, from_td=dr.from_trading_day,
                                to_td=dr.to_trading_day, sod_version=dr.sod_version)

    def _refuse_batch(self, b: _EffectBatch) -> None:
        """The batch's commit failed: its commands are answered UNAVAILABLE (retryable: nothing of them was stored,
        the shard restarts from the durable state) instead of with replies about state that was never written."""
        for e in b.tail:
            if isinstance(e, ReplyTo) and isinstance(e.token, RequestContext):
                self.rt.reply(e.token, err_reply(codes.UNAVAILABLE, "risk engine failed to commit; it restarts"))

    def _external(self, effects: Sequence[Effect]) -> None:
        for e in effects:
            if isinstance(e, Route):
                self._route(e.order)
            elif isinstance(e, CancelRoute):
                self._cancel(e, 0)
            elif isinstance(e, ResolveRoute):
                self._resolve(e)
            elif isinstance(e, Request):
                self._request(e)
            elif isinstance(e, Publish):
                self._publish(e)
            elif isinstance(e, ReplyTo):
                self._reply(e)

    def _route(self, order: RouteOrder) -> None:
        coid = order.client_order_id
        remaining = max(0, order.expire_ns - self._now())
        timeout_ms = int(min(1000, max(100, remaining // NS_PER_MS)))

        def on_reply(reply: Reply) -> None:
            core = self.core
            assert core is not None
            self.execute(core.on_route_ack(coid, reply))
            if reply.code in codes.RETRYABLE:
                self.execute(core.on_route_timeout(coid))  # no-op before expire_ns (the core's timer then resolves)

        # 02 §4.7 / §8.3: retried with the same client_order_id until expire_ns, never re-sent afterwards
        self.rt.request(
            TRADE_GATEWAY,
            "route.order",
            order,
            on_reply=on_reply,
            timeout_ms=timeout_ms,
            retries=10,
            deadline_ns=self._mono() + remaining,
        )
        self.rates.add("routes")

    def _cancel(self, e: CancelRoute, attempt: int) -> None:
        req = RouteCancel(command_id=e.command_id, client_order_id=e.client_order_id, reason=e.reason)

        def on_reply(reply: Reply) -> None:
            core = self.core
            assert core is not None
            self.execute(core.on_cancel_result(e.client_order_id, reply))
            if reply.code in codes.RETRYABLE:
                rec = core.orders.get(e.client_order_id)
                if rec is not None and rec.status == OrderStatus.CANCEL_PENDING and rec.cancel_command_id == e.command_id:
                    # transport failure: the shell retries the same command_id (idempotent at trade-gateway)
                    delay = min(5 * NS_PER_S, int(0.5 * NS_PER_S) << min(attempt, 4))
                    self._after(delay, lambda: self._cancel(e, attempt + 1), "cancel-retry")

        self.rt.request(TRADE_GATEWAY, "route.cancel", req, on_reply=on_reply)

    def _resolve(self, e: ResolveRoute) -> None:
        def on_reply(reply: Reply) -> None:
            core = self.core
            assert core is not None
            self.execute(core.on_resolve_result(e.client_order_id, reply))

        self.rt.request(
            TRADE_GATEWAY,
            "route.resolve",
            RouteResolve(client_order_id=e.client_order_id, trading_day=e.trading_day),
            on_reply=on_reply,
        )

    def _request(self, e: Request) -> None:
        def on_reply(reply: Reply) -> None:
            core = self.core
            assert core is not None
            if e.command == "td.query.positions":
                self._recon_replies += 1
                self._recon_last_ok = reply.ok
            self.execute(core.on_request_result(e.token, reply))

        self.rt.request(e.target, e.command, e.payload, timeout_ms=e.timeout_ms, on_reply=on_reply)

    def _publish(self, e: Publish) -> None:
        core = self.core
        td = core.trading_day if core is not None and core.trading_day else None
        self.rt.publish(e.topic, e.payload, td=td)
        self.rates.add("publishes")

    def _reply(self, e: ReplyTo) -> None:
        token = e.token
        if isinstance(token, RequestContext):
            self.rt.reply(token, e.reply)
        elif callable(token):
            token(e.reply)

    # ================================================================== commands
    def _on_command(self, command: str, payload: Msg, ctx: RequestContext) -> HandlerResult:
        """Shell gating before the core (ENGINE.md §5): history from risk.db, EOD commands held while starting,
        older-day freezes digest-only from risk.db. ``order.submit`` always reaches the core, which answers
        ``REJECT_SYSTEM_NOT_READY`` without a row while LOADING; duplicate rollovers are answered by the core
        (``restore(dayrolls=...)`` after a restart)."""
        core = self.core
        if core is None:
            return err_reply(codes.UNAVAILABLE, "risk engine starting")
        if self.poisoned is not None:  # review C1: nothing reaches the core after a fatal error
            return self._refuse(command, payload, "risk engine stopping after an error")
        if self._stopping or not self.rt.running:  # review M7: ctl.shutdown (or a signal) earlier in this batch
            self._enter_stopping()
            return self._refuse(command, payload, "risk engine stopping")
        if command in HISTORY_COMMANDS and self._is_history(command, payload):
            return self._history(command, payload, ctx)
        if command == "order.submit":
            assert isinstance(payload, OrderRequest)
            reused = self._reused_order_id(payload)
            if reused is not None:
                return reused
        elif command == "eod.freeze":
            assert isinstance(payload, EodFreezeRequest)
            if self.phase in _EARLY:
                return self._defer(command, payload, ctx)
            if core.freeze_digest_source(payload.trading_day) == "STORE":
                return self._freeze_past_day(payload, ctx)
        elif command == "eod.rollover":
            if self.phase in (Phase.CLOCK, Phase.CATCHUP):
                return self._defer(command, payload, ctx)
        elif self.phase == Phase.CLOCK and not command.startswith(QUERY_PREFIX):
            return err_reply(codes.UNAVAILABLE, "risk engine starting: market clock not synchronised")
        return self._dispatch(command, payload, ctx)

    def _dispatch(self, command: str, payload: Msg, ctx: RequestContext) -> HandlerResult:
        core = self.core
        assert core is not None
        cctx = CommandCtx(command, token=ctx, requester=ctx.requester, received_ns=ctx.received_ns)
        t0 = self._mono()
        try:
            effects = core.handle(payload, cctx)
            t1 = self._mono()
            self.execute(effects)
        except Exception as exc:
            self._fatal(exc, f"command {command}")
            raise
        if command == "order.submit":
            self.lat["pretrade"].add_ns(t1 - t0)
            self.rates.add("orders")
            if self._batch is not None:
                self._batch.orders.append(t0)  # measured when its batch is committed, routed and answered
            else:
                self.lat["order_submit"].add_ns(self._mono() - t0)
        return DEFERRED

    def _refuse(self, command: str, payload: Msg, why: str) -> Reply:
        """Answer without the core: orders ``REJECT_SYSTEM_NOT_READY`` (an OrderAck, no row), others ``UNAVAILABLE``
        (retryable: the client asks again once the shard is back)."""
        if command == "order.submit" and isinstance(payload, OrderRequest):
            core = self.core
            ack = OrderAck(client_order_id=payload.client_order_id, accepted=False, status=OrderStatus.REJECTED,
                           reject_code=codes.REJECT_SYSTEM_NOT_READY, reject_detail={"reason": why},
                           trading_day=core.trading_day if core is not None else "")
            return err_reply(codes.REJECT_SYSTEM_NOT_READY, why, data=ack)
        return err_reply(codes.UNAVAILABLE, why)

    def _enter_stopping(self) -> None:
        """Once per process: the core rejects new orders (``Lifecycle.STOPPING``), held commands are answered."""
        if self._stopping:
            return
        self._stopping = True
        core = self.core
        if core is not None and self.poisoned is None:
            self.execute(core.set_lifecycle(Lifecycle.STOPPING))
        for d in self._deferred:
            self.rt.reply(d.ctx, err_reply(codes.UNAVAILABLE, "risk engine stopping"))
        self._deferred = []

    def _reused_order_id(self, req: OrderRequest) -> Reply | None:
        """A ``client_order_id`` of an order of another trading day (review H4): the core only knows today's and
        working orders, and its row UPSERT would overwrite the permanent history of the old order (its fills still
        reference it). One primary-key lookup on a reader, only for ids the core does not know."""
        core, db = self.core, self.db
        assert core is not None and db is not None
        if core.orders.get(req.client_order_id) is not None:
            return None
        row = db.read(lambda c: _ORDER_DAY(c, (req.client_order_id,)).fetchone())
        if row is None or row[0] == core.trading_day:
            return None
        day = str(row[0])
        ack = OrderAck(client_order_id=req.client_order_id, accepted=False, status=OrderStatus.REJECTED,
                       reject_code=codes.REJECT_DUPLICATE_ORDER_ID, reject_detail={"trading_day": day},
                       trading_day=core.trading_day)
        return err_reply(codes.REJECT_DUPLICATE_ORDER_ID, f"client_order_id already used on {day}", data=ack)

    def _defer(self, command: str, payload: Msg, ctx: RequestContext) -> HandlerResult:
        self._deferred.append(_Deferred(self._mono() + self.defer_limit_ns, command, payload, ctx))
        logger.info("risk.command_held", command=command, phase=self.phase.value)
        return DEFERRED

    def _drain_deferred(self) -> None:
        if not self._deferred:
            return
        now = self._mono()
        keep: list[_Deferred] = []
        pending, self._deferred = self._deferred, []
        for d in pending:
            held = (d.command == "eod.freeze" and self.phase in _EARLY) or (
                d.command == "eod.rollover" and self.phase in (Phase.CLOCK, Phase.CATCHUP)
            )
            if held:
                if now >= d.deadline_ns:
                    self.rt.reply(d.ctx, err_reply(codes.UNAVAILABLE, f"risk engine starting ({self.phase.value})"))
                else:
                    keep.append(d)
                continue
            try:
                res = self._on_command(d.command, d.payload, d.ctx)
            except Exception as exc:  # the dispatcher already escalated to fatal
                self.rt.reply(d.ctx, err_reply(codes.INTERNAL, f"{type(exc).__name__}: {exc}"))
                continue
            if isinstance(res, Reply):
                self.rt.reply(d.ctx, res)
        self._deferred = keep + self._deferred

    # ------------------------------------------------------------ history (risk.db)
    def _is_history(self, command: str, q: Msg) -> bool:
        if command == "risk.query.metrics":
            return True
        core = self.core
        assert core is not None
        if self.phase in (Phase.CLOCK, Phase.SOD) or not core.trading_day:
            return True
        td = getattr(q, "trading_day", None)
        return td is not None and td != core.trading_day

    def _history(self, command: str, q: Msg, ctx: RequestContext) -> HandlerResult:
        db, store = self.db, self.store
        assert db is not None and store is not None

        def work() -> Reply:
            if isinstance(q, QueryMetrics):
                return ok_reply(db.read(lambda c: store.query_metrics(c, q.account_id, q.trading_day, q.step_s)))
            if isinstance(q, QueryOrders):
                statuses = [s.value for s in q.status] if q.status else None
                return ok_reply(OrderBatch(items=db.read(lambda c: store.query_orders(c, q.account_id, q.trading_day, statuses))))
            if isinstance(q, QueryFills):
                return ok_reply(FillBatch(items=db.read(lambda c: store.query_fills(c, q.account_id, q.trading_day))))
            if isinstance(q, QueryAlerts):
                return ok_reply(RiskEventBatch(items=db.read(lambda c: store.query_events(c, q.trading_day, q.account_id))))
            if isinstance(q, QueryCash):
                return ok_reply(CashBatch(items=db.read(lambda c: store.query_cash(c, q.trading_day, q.account_id))))
            if isinstance(q, QueryLiquidationPlans) and q.trading_day is not None:
                day, status = q.trading_day, (q.status.value if q.status is not None else None)
                return ok_reply(LiquidationPlanBatch(items=db.read(
                    lambda c: store.query_liquidation_plans(c, day, q.account_id, status))))
            return err_reply(codes.BAD_REQUEST, f"{command} is not a history query")

        def done(result: Reply | None, error: BaseException | None) -> None:
            if error is not None or result is None:
                logger.opt(exception=error).error("risk.history_query_failed", command=command)
                self.rt.reply(ctx, err_reply(codes.INTERNAL, f"history query failed: {error}"))
            else:
                self.rt.reply(ctx, result)

        self.rt.offload(work, done)
        return DEFERRED

    def _freeze_past_day(self, req: EodFreezeRequest, ctx: RequestContext) -> HandlerResult:
        """``eod.freeze{X}`` with ``freeze_digest_source(X) == "STORE"`` (an older day: re-settlement or missed EODs):
        a digest-only ``EodFreezeAck`` from the persisted fills (by ``fill_seq``) and cash of X, ``open_orders`` 0,
        nothing frozen (ENGINE.md §9)."""
        core, db = self.core, self.db
        assert core is not None and db is not None
        accounts = list(core.owned)
        td = req.trading_day
        now = self._now()

        def work() -> Reply:
            def q(conn: Any) -> tuple[list[Trade], list[CashMovement], int | None]:
                fills = conn.execute(
                    select(S.fills).where(S.fills.c.trading_day == td).order_by(S.fills.c.fill_seq)).mappings()
                trades = [trade_from_fill_row({str(k): v for k, v in r.items()}) for r in fills]
                cash_rows = conn.execute(
                    select(S.cash_movements).where(S.cash_movements.c.trading_day == td)
                    .order_by(S.cash_movements.c.ts_ns, S.cash_movements.c.movement_id)).mappings()
                cash = [cash_from_row({str(k): v for k, v in r.items()}) for r in cash_rows]
                frozen = conn.execute(select(S.eod_states.c.frozen_ns).where(S.eod_states.c.trading_day == td)).scalar()
                return trades, cash, (int(frozen) if frozen is not None else None)

            trades, cash, frozen_ns = db.read(q)
            ack = digest_only_ack(trading_day=td, shard=self.shard, account_ids=accounts, trades=trades, cash=cash,
                                  frozen_ns=frozen_ns or now)
            return ok_reply(ack, message="digests of a closed trading day (no state change)")

        def done(result: Reply | None, error: BaseException | None) -> None:
            if error is not None or result is None:
                logger.opt(exception=error).error("risk.freeze_past_day_failed", trading_day=td)
                self.rt.reply(ctx, err_reply(codes.INTERNAL, f"freeze digests failed: {error}"))
            else:
                self.rt.reply(ctx, result)

        self.rt.offload(work, done)
        return DEFERRED

    # ================================================================== start-up phases
    def _phase_step(self) -> None:
        self._check_md_resync()
        if self.phase == Phase.CLOCK and self.rt.clock_ready:
            self._on_clock_ready()
        if self.phase == Phase.SOD:
            core = self.core
            assert core is not None
            if core.trading_day:
                self._enter_catchup()
        if self.phase == Phase.CATCHUP and not self._catchup_pending:
            self._enter_warmup()
        if self.phase == Phase.WARMUP:
            self._check_warmup()
        self._drain_deferred()

    def _on_clock_ready(self) -> None:
        # the restore builds ~10 objects per stored row (fills, orders, events, lots...) and every one of them
        # survives: collections meanwhile would only traverse them again and again (a third of the restore time)
        enabled = gc.isenabled()
        gc.disable()
        try:
            self._restore()
        finally:
            if enabled:
                gc.enable()

    def _restore(self) -> None:
        core, db, store = self.core, self.db, self.store
        assert core is not None and db is not None and store is not None
        loaded = db.read(store.load_restore)
        if loaded is None:
            self._set_phase(Phase.SOD)
            logger.info("risk.no_sod", detail="fresh database: requesting settle.sod_snapshot")
            self._request_sod()
            return
        tables, peaks = loaded
        rolls = db.read(store.dayrolls)  # exact replies of re-pushed rollovers (the core rebuilds older ones)
        core.restore(tables, lifecycle=Lifecycle.LOADING, intraday_peaks=peaks, dayrolls=rolls)
        self._core_next_dirty = True
        logger.info(
            "risk.restored",
            trading_day=core.trading_day,
            sod_version=core.sod_version,
            mode=core.trading_mode.value,
            accounts={a: core.account(a).state.value for a in core.account_ids()},
            latched=[a for a in core.account_ids() if core.account(a).halted_latched],
            fills=len(tables.get("fills", ())),
            orders=len(tables.get("orders", ())),
        )
        self.execute(core.post_restore())
        self._enter_catchup()

    def _request_sod(self) -> None:
        core = self.core
        assert core is not None
        if core.trading_day or self._sod_inflight:
            return
        cal = self.rt.config.calendar
        td = td_str(cal.current_or_next_trading_day(self._now()))
        self._sod_inflight = True

        def on_reply(reply: Reply) -> None:
            self._sod_inflight = False
            if core.trading_day:
                return  # the seed eod.rollover was faster
            if reply.ok and reply.data is not None:
                sod = reply.open_as(SodSnapshot)
                logger.info("risk.sod_loaded", trading_day=sod.trading_day, version=sod.version)
                self.execute(core.load_sod(sod))
                return
            if reply.code == codes.NOT_FOUND:
                if not self._sod_warned:
                    self._sod_warned = True
                    logger.warning("risk.sod_not_found", trading_day=td,
                                   detail="no SOD yet: waiting for the seed push (eod.rollover), retry every 30 s")
                delay = self.sod_not_found_retry_ns
            else:
                logger.info("risk.sod_unavailable", code=reply.code, message=reply.message)
                delay = self.sod_retry_ns
            self._after(delay, self._request_sod, "sod")

        self.rt.request(
            CLEARING,
            "settle.sod_snapshot",
            SodRequest(trading_day=td, account_ids=list(core.owned), version=None),
            on_reply=on_reply,
        )

    def _enter_catchup(self) -> None:
        core = self.core
        assert core is not None
        self._set_phase(Phase.CATCHUP)
        if core.lifecycle != Lifecycle.LOADING:
            self.execute(core.set_lifecycle(Lifecycle.LOADING))
        self._subscribe_exec()
        self._subscribe_firm_control()
        accounts = core.account_ids()
        self._catchup_pending = set(accounts)
        td = core.trading_day
        for a in accounts:
            # the whole day, not from the highest applied trade_seq: next-day fills buffered in memory before a crash
            # have lower broker seqs than trades already applied (review L3); apply_exec dedupes by trade key
            self._catchup_query(a, None, td, self._catchup_account_done, purpose="startup")
        logger.info("risk.catchup", trading_day=td, accounts=accounts)

    def _catchup_account_done(self, account: str, ok: bool) -> None:
        self._catchup_pending.discard(account)

    def _catchup_query(
        self,
        account: str,
        from_seq: int | None,
        trading_day: str,
        on_done: Callable[[str, bool], None],
        *,
        purpose: str,
        attempt: int = 0,
    ) -> None:
        """``td.query.trades`` of one account (paged); every trade goes through ``apply_exec`` (dedupe by trade key)."""
        req = TradeQuery(trading_day=trading_day, account_id=account, from_trade_seq=from_seq, limit=500)

        def on_reply(reply: Reply) -> None:
            core = self.core
            assert core is not None
            if reply.ok and reply.data is not None:
                batch = reply.open_as(TradeBatch)
                n = 0
                for t in sorted(batch.trades, key=lambda x: x.trade_seq):
                    if t.account_id != account:
                        continue
                    self.execute(core.apply_exec(t))
                    n += 1
                if n:
                    logger.info("risk.catchup_trades", account=account, trading_day=trading_day, trades=n, purpose=purpose)
                if not batch.complete and batch.trades:
                    self._catchup_query(account, batch.next_trade_seq, trading_day, on_done, purpose=purpose)
                    return
                on_done(account, True)
                return
            delay = min(5 * NS_PER_S, (NS_PER_S // 2) << min(attempt, 3))
            logger.info("risk.catchup_retry", account=account, code=reply.code, purpose=purpose, attempt=attempt)
            self._after(
                delay,
                lambda: self._catchup_query(account, from_seq, trading_day, on_done, purpose=purpose, attempt=attempt + 1),
                "catchup",
            )

        self.rt.request(TRADE_GATEWAY, "td.query.trades", req, on_reply=on_reply)

    def _enter_warmup(self) -> None:
        core = self.core
        assert core is not None
        self._set_phase(Phase.WARMUP)
        self._warmup_since_ns = self._mono()
        for a in core.account_ids():
            # a FILL_GAP restored from account_states (set before a restart) is obsolete: every fill of the
            # day was just re-read; gaps still open in this process keep theirs (_gap_reasons)
            if not self._gap_reasons.get(a) and any(r.source == "SYSTEM:FILL_GAP" for r in core.account(a).restrictions):
                logger.info("risk.fill_gap_cleared_after_catchup", account=a)
                self.execute(core.clear_system_restriction(a, "FILL_GAP"))
        self.execute(core.set_lifecycle(Lifecycle.WARMUP))
        self._start_recon()
        logger.info("risk.warmup", trading_day=core.trading_day)

    def _start_recon(self) -> None:
        core = self.core
        assert core is not None
        self._recon_retry_at = None
        self._recon_mark = self._recon_replies
        effects = core.request_position_recon()
        if not effects and "recon_positions" not in core.pending_requests().values():
            self._recon_mark = None
        self.execute(effects)

    def _check_warmup(self) -> None:
        core = self.core
        assert core is not None
        now = self._mono()
        if not self._recon_ok:
            if self._recon_retry_at is not None:
                if now >= self._recon_retry_at:
                    self._start_recon()
            elif self._recon_mark is None or self._recon_replies > self._recon_mark:
                if self._recon_mark is not None and self._recon_last_ok:
                    streaks = [core.account(a).recon_mismatch_streak for a in core.account_ids()]
                    if all(s == 0 or s >= 2 for s in streaks):
                        self._recon_ok = True  # matched, or a blocking POSITION_MISMATCH break took over
                        logger.info("risk.warmup_recon_ok", streaks=streaks)
                    else:
                        self._recon_retry_at = now + self.recon_retry_ns  # one mismatch: confirm after the quiet period
                else:
                    self._recon_retry_at = now + self.recon_retry_ns
        if not self._recon_ok:
            return
        if not self._marks_fresh() and now - self._warmup_since_ns < self.warmup_marks_timeout_ns:
            return
        self._enter_running()

    def _marks_fresh(self) -> bool:
        """Every held instrument in session has a LIVE tick received by this process and quality OK (a price from a
        ``ctl.snapshot`` may be old: it never counts, review H1)."""
        core = self.core
        assert core is not None
        now = self._now()
        for a in core.account_ids():
            for inst, _d, _q in core.account(a).ledger.held():
                if core.session_open(inst, now) and (inst not in self._live_ticks or core.quality_of(inst) != MdQuality.OK):
                    return False
        return True

    def _enter_running(self) -> None:
        core = self.core
        assert core is not None
        self.execute(core.evaluate_all())
        self.execute(core.set_lifecycle(Lifecycle.RUNNING))
        self._set_phase(Phase.RUNNING)
        logger.info("risk.running", trading_day=core.trading_day, mode=core.trading_mode.value,
                    warmup_ms=(self._mono() - self._warmup_since_ns) // NS_PER_MS)
        self._update_service_state()  # RUNNING / DEGRADED: set_state sends a heartbeat at once

    def _set_phase(self, phase: Phase) -> None:
        """Enter ``phase`` and refresh the heartbeat fields at once: ``ctl.status`` and the next heartbeat show
        ``detail.phase`` without the ``heartbeat_fields_s`` delay (supervisor, api-gateway and tests watch it)."""
        self.phase = phase
        self._core_next_dirty = True
        if self.core is not None:
            self._next_hb_ns = 0
            self._heartbeat_tick()

    def _update_service_state(self) -> None:
        if self.phase != Phase.RUNNING:
            return
        degraded = not (self._md_up and self._tg_up)
        self.rt.set_state(ServiceState.DEGRADED if degraded else ServiceState.RUNNING)

    # ================================================================== FACT: td.exec
    def _subscribe_exec(self) -> None:
        if self._exec_subscribed:
            return
        core, cursors = self.core, self.cursors
        assert core is not None and cursors is not None
        self._exec_subscribed = True
        persisted = cursors.load_all()
        exec_topics = [topics.td_exec(a) for a in core.owned]
        self._fresh_keys = {(TRADE_GATEWAY, t) for t in exec_topics if (TRADE_GATEWAY, t) not in persisted}
        self.rt.subscribe_fact(
            TRADE_GATEWAY,
            exec_topics,
            policy=FactStartPolicy.FROM_BEGINNING,
            apply=self._apply_exec_env,
            cursors=persisted,
            on_expired=self._exec_expired,
            on_stream_change=self._exec_stream_change,
            on_repair_failed=self._exec_repair_failed,
            on_repair_ok=self._exec_repair_ok,
        )

    def _apply_exec_env(self, env: Envelope, cur: CursorUpdate) -> None:
        core = self.core
        assert core is not None
        msg: Trade | BrokerOrderUpdate | None = None
        undecodable = False
        try:
            if env.type == "Trade":
                msg = decode(env.payload, Trade)
            elif env.type == "BrokerOrderUpdate":
                msg = decode(env.payload, BrokerOrderUpdate)
            else:
                logger.warning("risk.exec_unexpected_type", type=env.type, topic=env.topic, seq=env.seq)
        except (msgspec.DecodeError, msgspec.ValidationError):
            logger.error("risk.exec_undecodable", topic=env.topic, seq=env.seq, type=env.type)
            undecodable = True
        effects: list[Effect] = []
        key = (env.src, env.topic)
        if undecodable:
            self._undecodable_exec(key, env)
        if msg is not None:
            if key in self._fresh_keys and core.trading_day and msg.trading_day < core.trading_day:
                # first subscription of a fresh database: history of settled days is already in the SOD
                self.rates.add("exec_history_skipped")
            else:
                if key in self._fresh_keys and msg.trading_day >= core.trading_day:
                    self._fresh_keys.discard(key)
                effects = core.apply_exec(msg)
        self.execute(effects, cursor=cur)
        self.rates.add("exec")

    @staticmethod
    def _account_of(key: FactKey) -> str:
        return topics.key_of(key[1], topics.TD_EXEC)

    def _undecodable_exec(self, key: FactKey, env: Envelope) -> None:
        """A fill or broker update that cannot be read (review L4): a blocking POSITION_MISMATCH break (opens blocked
        until ``risk.recon.resolve``) and the domain catch-up (FILL_GAP while it runs), which recovers a missed fill
        from ``td.query.trades``."""
        core = self.core
        assert core is not None
        acct = self._account_of(key)
        if acct not in core.account_ids():
            return
        detail = {"reason": "UNDECODABLE_FACT", "src": env.src, "topic": env.topic, "seq": env.seq, "type": env.type}
        self.execute(core.open_recon_break(acct, ReconBreakKind.POSITION_MISMATCH, detail, blocking=True))
        self._reconcile_account(acct, "undecodable", lambda: None)

    def _persist_cursor(self, key: FactKey) -> None:
        """Store the tracker's position of ``key`` after a fallback moved it past facts that were never applied (review
        M4): otherwise the same range expires / the same stream changes again at every restart."""
        tracker = self.rt.fact_tracker
        stream, expected = tracker.stream_of(*key), tracker.expected(*key)
        if stream is None or expected is None:
            return
        self.execute([], cursor=CursorUpdate(key[0], key[1], stream, expected - 1, FactStartPolicy.FROM_BEGINNING))

    def _set_gap(self, account: str, reason: str, on: bool) -> None:
        core = self.core
        assert core is not None
        if account not in core.account_ids():
            return
        reasons = self._gap_reasons.setdefault(account, set())
        before = bool(reasons)
        if on:
            reasons.add(reason)
        else:
            reasons.discard(reason)
        on = bool(reasons)
        if before != on:
            # restored from account_states it may already be in force: no second FILL_GAP event (review L7)
            has = any(r.source == "SYSTEM:FILL_GAP" for r in core.account(account).restrictions)
            if on != has:
                self.execute(core.set_system_restriction(account, "FILL_GAP", on))

    def _exec_repair_failed(self, key: FactKey, lo: int, hi: int, reply: Reply) -> None:
        acct = self._account_of(key)
        logger.warning("risk.fill_gap", account=acct, lo=lo, hi=hi, code=reply.code, detail="opens blocked until repaired")
        self._set_gap(acct, "repair", True)

    def _exec_repair_ok(self, key: FactKey) -> None:
        self._set_gap(self._account_of(key), "repair", False)

    def _exec_expired(self, key: FactKey, lo: int, hi: int, done: Callable[[], None]) -> None:
        """02 §4.6: the replay window is gone: REPLAY_EXPIRED (non-blocking) + domain catch-up by query."""
        acct = self._account_of(key)
        logger.warning("risk.exec_replay_expired", account=acct, lo=lo, hi=hi)
        self._open_replay_break(acct, key, lo, hi)
        self._reconcile_account(acct, "replay_expired", lambda: self._fallback_done(key, done))

    def _exec_stream_change(self, key: FactKey, old: str | None, new: str, next_seq: int, done: Callable[[], None]) -> None:
        acct = self._account_of(key)
        logger.warning("risk.exec_stream_changed", account=acct, old=old, new=new, next_seq=next_seq)
        self._reconcile_account(acct, "stream_change", lambda: self._fallback_done(key, done))

    def _fallback_done(self, key: FactKey, done: Callable[[], None]) -> None:
        done()  # the tracker moves past the range / onto the new stream and drains what it buffered
        self._persist_cursor(key)

    def _open_replay_break(self, account: str, key: FactKey, lo: int, hi: int) -> None:
        core = self.core
        assert core is not None
        if account not in core.account_ids():
            return
        detail = {"src": key[0], "topic": key[1], "from_seq": lo, "to_seq": hi}
        self.execute(core.open_recon_break(account, ReconBreakKind.REPLAY_EXPIRED, detail, blocking=False))

    def _reconcile_account(self, account: str, reason: str, done: Callable[[], None]) -> None:
        """Domain fallback of 02 §8.2: all trades of the day, then the broker state of the account's orders."""
        core = self.core
        assert core is not None
        td = core.trading_day
        if not td or account not in core.account_ids():
            done()
            return
        self._set_gap(account, reason, True)

        def orders_done() -> None:
            self._set_gap(account, reason, False)
            logger.info("risk.reconciled", account=account, reason=reason)
            done()

        def query_orders(attempt: int = 0) -> None:
            def on_reply(reply: Reply) -> None:
                if not (reply.ok and reply.data is not None):
                    self._after(min(5 * NS_PER_S, (NS_PER_S // 2) << min(attempt, 3)),
                                lambda: query_orders(attempt + 1), "reconcile-orders")
                    return
                for u in reply.open_as(BrokerOrderBatch).items:
                    rec = core.orders.get(u.client_order_id) if u.client_order_id else None
                    if rec is not None and rec.status not in TERMINAL:
                        self.execute(core.apply_exec(u))
                orders_done()

            self.rt.request(TRADE_GATEWAY, "td.query.orders", OrderQuery(trading_day=td, account_id=account),
                            on_reply=on_reply)

        self._catchup_query(account, None, td, lambda _a, _ok: query_orders(), purpose=reason)

    # ================================================================== FACT: risk.control.firm
    def _subscribe_firm_control(self) -> None:
        if self._firm_subscribed or self.is_coordinator:
            return  # the coordinator applies its own controls inside the engine (ENGINE.md §10)
        cursors = self.cursors
        assert cursors is not None
        coord = self.coordinator_instance
        if not self.rt.endpoints.has(coord) or not self.rt.endpoints.instance(coord).pub:
            logger.warning("risk.no_coordinator", coordinator=coord)
            return
        self._firm_subscribed = True
        self.rt.subscribe_fact(
            coord,
            [topics.RISK_CONTROL_FIRM],
            policy=FactStartPolicy.FROM_BEGINNING,
            apply=self._apply_firm_env,
            cursors=cursors.load_all(),
            on_expired=lambda key, lo, hi, done: self._firm_snapshot(key[0], done),
            on_stream_change=lambda key, old, new, nxt, done: self._firm_snapshot(key[0], done),
        )

    def _apply_firm_env(self, env: Envelope, cur: CursorUpdate) -> None:
        core = self.core
        assert core is not None
        effects: list[Effect] = []
        if env.type == "FirmControl":
            try:
                effects = core.apply_firm_control(decode(env.payload, FirmControl))
            except (msgspec.DecodeError, msgspec.ValidationError):
                logger.error("risk.firm_control_undecodable", src=env.src, seq=env.seq)
        self.execute(effects, cursor=cur)

    def _firm_snapshot(self, src: str, done: Callable[[], None], attempt: int = 0) -> None:
        """Replay window exceeded or coordinator database changed: take the latest control by snapshot."""

        def on_reply(reply: Reply) -> None:
            core = self.core
            assert core is not None
            if not reply.ok:
                self._after(min(5 * NS_PER_S, NS_PER_S << min(attempt, 2)),
                            lambda: self._firm_snapshot(src, done, attempt + 1), "firm-snapshot")
                return
            batch = reply.open_as(SnapshotBatch)
            for item in batch.items if batch is not None else []:
                if item.topic != topics.RISK_CONTROL_FIRM:
                    continue
                env = decode_envelope(item.envelope)
                if env.type != "FirmControl":
                    continue
                control = decode(env.payload, FirmControl)
                if not self._firm_control_known(control):
                    self.execute(core.apply_firm_control(control))
            done()

        self.rt.request(src, "ctl.snapshot", SnapshotRequest(prefixes=[topics.RISK_CONTROL_FIRM]), on_reply=on_reply,
                        retries=1)

    def _firm_control_known(self, control: FirmControl) -> bool:
        """The control's state is already in force (the snapshot fallback then writes no row): RULE_STATE compares the
        whole RULE layer (rule kill flag + firm restrictions), KILL_SWITCH_ON/OFF the flag of their origin."""
        core = self.core
        assert core is not None
        firm = core.firm
        if control.action in (FirmAction.KILL_SWITCH_ON, FirmAction.KILL_SWITCH_OFF):
            want = control.action == FirmAction.KILL_SWITCH_ON
            have = firm.manual_kill if control.origin == ControlOrigin.MANUAL else firm.rule_kill
            return want == have
        mine = sorted((r.kind.value, r.scope_key, r.value) for r in firm.restrictions)
        theirs = sorted((r.kind.value, r.scope_key, r.value) for r in control.restrictions
                        if r.scope_key == "firm" or r.scope_key.startswith("firm/"))
        if control.action == FirmAction.RULE_STATE:
            return firm.rule_kill == bool(control.active) and mine == theirs
        return mine == theirs

    # ================================================================== coordinator
    def _on_shard_snapshot(self, env: Envelope) -> None:
        if env.type != "AccountSnapshot":
            return
        try:
            snap = decode(env.payload, AccountSnapshot)
        except (msgspec.DecodeError, msgspec.ValidationError):
            logger.warning("risk.shard_snapshot_undecodable", src=env.src, topic=env.topic)
            return
        core = self.core
        assert core is not None
        core.ingest_snapshot(snap)
        self.rates.add("shard_snapshots")

    def _on_shard_heartbeat(self, hb: Heartbeat) -> None:
        """Coordinator: a running shard's heartbeat keeps its accounts fresh between snapshots (ENGINE.md §10)."""
        shard = self._shard_of.get(hb.instance)
        core = self.core
        if shard is None or core is None or hb.state not in (ServiceState.RUNNING, ServiceState.DEGRADED):
            return  # (a shard still starting evaluates nothing: its last snapshots must age out)
        core.note_shard_alive(shard)

    # ================================================================== sys.eod maintenance
    def _on_sys_eod(self, env: Envelope) -> None:
        if env.type != "EodPhaseEvent":
            return
        try:
            ev = decode(env.payload, EodPhaseEvent)
        except (msgspec.DecodeError, msgspec.ValidationError):
            return
        if ev.phase == EodPhase.DONE:
            self.start_maintenance(ev.trading_day)

    def market_quiet(self, now_ns: int | None = None) -> bool:
        """No session of any product is open and none starts within :data:`MAINTENANCE_MARGIN_NS` (review M3)."""
        cal = self.rt.config.calendar
        names = sorted(cal.templates)
        now = self._now() if now_ns is None else now_ns
        if not names:
            return True
        if cal.phase_at(names, now).is_open:
            return False
        nxt = cal.next_window_start(names, now)
        return nxt is None or nxt.start_ns - now >= MAINTENANCE_MARGIN_NS

    def start_maintenance(self, trading_day: str, *, force: bool = False) -> bool:
        """03 §8 maintenance after ``sys.eod`` DONE, once per trading day, on a worker thread, and only while the market
        is quiet (review M3: a backup and purges never compete with trading commits); postponed otherwise."""
        if self._maint_running or trading_day in self._maint_days:
            return False
        if not force and not self.market_quiet():
            if trading_day not in self._maint_waiting:
                self._maint_waiting.add(trading_day)
                logger.info("risk.maintenance_postponed", trading_day=trading_day, detail="a session is open or near")

                def retry() -> None:
                    self._maint_waiting.discard(trading_day)
                    self.start_maintenance(trading_day)

                self._after(self.maintenance_retry_ns, retry, "maintenance")
            return False
        db = self.db
        layout = self.rt.layout
        assert db is not None and layout is not None
        self._maint_running = True
        self._maint_days.add(trading_day)
        outbox = self.rt.outbox
        calendar = self.rt.config.calendar

        def work() -> Any:
            return run_maintenance(db, outbox, trading_day=trading_day, backup_dir=layout.backup_dir, calendar=calendar)

        def done(result: Any, error: BaseException | None) -> None:
            self._maint_running = False
            if error is not None:
                logger.opt(exception=error).error("risk.maintenance_failed", trading_day=trading_day)
            else:
                self.maintenance_reports.append(result)

        self.rt.offload(work, done)
        return True

    # ================================================================== config
    def _validate_config(self, current: FullConfig, new: FullConfig) -> list[str]:
        """``ctl.config.prepare`` check (worker thread): the new rules must compile and use known metrics."""
        errors: list[str] = []
        try:
            RuleBook(new.risk_rules, config_hash=new.config_hash)
        except Exception as exc:
            errors.append(f"risk_rules.yaml: {exc}")
        known = registered_metrics()
        for r in new.risk_rules.rules:
            if r.enabled and r.metric not in known:
                errors.append(f"risk_rules.yaml: rule {r.id} uses unknown metric {r.metric}")
        if new.accounts.accounts_of_shard(self.shard) != current.accounts.accounts_of_shard(self.shard):
            errors.append(f"accounts.yaml: the accounts of shard {self.shard} changed; ownership changes need a restart")
        return errors

    def _on_config_commit(self, new: FullConfig, effects: Mapping[str, ConfigEffect]) -> None:
        """``ctl.config.commit`` (loop thread): risk_rules IMMEDIATE; product parameters and the calendar are staged."""
        core = self.core
        assert core is not None
        # the reload's own effects (state refresh, CONFIG_COMMIT audit of kept latches, ALERT_CLEARED) and the
        # shard's commit audit row go into one transaction (ENGINE.md §4: the shell must execute them)
        rule_effects = core.update_rules(new.risk_rules, config_hash=new.config_hash, config_version=new.config_version)
        staged = sorted(s for s, e in effects.items() if e == ConfigEffect.NEXT_TRADING_DAY)
        if staged:
            # the instrument registry (margins, fees, limits, calendar) is fixed per process: the new
            # version is already in active.json and takes effect at the next start of this process
            self._staged = {"config_version": new.config_version, "sections": staged, "effective": "after restart"}
            logger.warning("risk.config_staged", version=new.config_version, sections=staged)
        tx = Tx(label="config.commit")
        audit(tx, ts_ns=self._now(), trading_day=core.trading_day or td_str(new.calendar.current_or_next_trading_day(self._now())),
              actor="api-gateway", action="CONFIG_COMMIT", target=f"shard:{self.shard}",
              after={"config_version": new.config_version, "config_hash": new.config_hash,
                     "sections": {k: str(v) for k, v in effects.items()}})
        tx.mark_critical()
        self.execute(merge_effects(rule_effects, tx.effects(), label="config.commit"))
        logger.info("risk.config_committed", version=new.config_version, hash=new.config_hash,
                    sections=sorted(effects))

    # ================================================================== heartbeat
    def _detail(self) -> dict[str, Any]:
        core = self.core
        d: dict[str, Any] = {"phase": self.phase.value, "shard": self.shard, "coordinator": self.is_coordinator}
        if core is not None:
            d.update(
                lifecycle=core.lifecycle.value,
                accounts=core.account_ids(),
                sod_version=core.sod_version,
                kill_switch=core.kill_switch_effective(),
                gateway_up=core.gateway_up,
                md_up=self._md_up,
                halted=[a for a in core.account_ids() if core.account(a).state.value == "HALTED"],
            )
        if self._staged:
            d["staged_config"] = dict(self._staged)
        return d

    def _heartbeat_tick(self) -> None:
        now = self._mono()
        if now < self._next_hb_ns:
            return
        self._next_hb_ns = now + self.heartbeat_fields_ns
        core = self.core
        assert core is not None
        latency = {name: s for name, w in self.lat.items() if (s := w.summary()) is not None}
        self.rt.set_heartbeat_fields(
            trading_day=core.trading_day or None,
            trading_mode=core.trading_mode,
            queues={
                "md_slots": len(self._slots),
                "dirty_accounts": len(core.dirty_accounts()),
                "pending_requests": len(core.pending_requests()),
                "resting_orders": len(core.orders.resting()),
                "held_commands": len(self._deferred),
                "shell_timers": len(self._timers),
            },
            rates=self.rates.rates(now, names=("ticks", "marks", "exec", "commands", "orders", "routes", "persist")),
            latency_us=latency,
            detail=self._detail(),
        )


__all__ = ["CORE_COMMANDS", "HISTORY_COMMANDS", "Phase", "RiskEngineService"]
