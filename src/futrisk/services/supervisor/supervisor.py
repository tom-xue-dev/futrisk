"""Supervisor: lifecycle of every service process (01 §5.3.6, §6.8; 02 §4.8, §6.6, §8.6; 03 §4.6, §7).

Per instance the supervisor keeps a small state machine over ``ProcessState``; every
transition is persisted in ``service_events`` and published on ``sys.service`` (FACT):

====================================  =====================================================
transition                            reason
====================================  =====================================================
STOPPED -> STARTING -> RUNNING        ``STARTUP`` (start sequence) / ``MANUAL`` (``svc.start``);
                                      reaching RUNNING = liveness, always ``STARTUP``
RUNNING -> EXITED -> RESTARTING       ``EXITED`` (process ended), ``HEARTBEAT_LOST`` (3 missed
  -> STARTING -> RUNNING              beats, killed), ``LOOP_HANG`` (``loop_lag_ms`` above
                                      ``hang_threshold_ms`` on 2 beats in a row, killed),
                                      ``KILLED_BY_DEMO`` (``svc.kill``)
STARTING -> FAILED -> RESTARTING      same reasons when the process never became live
                                      (spawn error, a port held by another process,
                                      early exit, no liveness in ``liveness_timeout_s``)
EXITED/FAILED -> FAILED               ``GAVE_UP``: 5 failures within 5 minutes; the unit is
                                      parked until ``svc.start``/``svc.restart``
RUNNING -> STOPPING -> STOPPED        ``MANUAL`` (``svc.stop``) / ``SHUTDOWN``
RUNNING -> STOPPING -> RESTARTING     ``MANUAL`` (``svc.restart``: immediate respawn)
  -> STARTING
RUNNING/... -> EXITED                 ``SUPERVISOR_RESTART``: recorded at start-up for the
                                      episodes a crashed supervisor left open
====================================  =====================================================

Liveness (02 §4.8, §8.6): bus services - the first ``sys.heartbeat.{instance}`` of the new
process whose ``pid`` is the spawned process or one of its descendants (a heartbeat from any
other process - e.g. another home's service owning the same port - is ignored);
sim-exchange - process alive, ``GET /v1/health`` = 200 and the health port held by the
child's own process tree; strategy-sim - process alive for one heartbeat period. The start
sequence follows ``supervisor.startup_order`` and waits only for liveness; an instance
that fails to start is retried by the restart policy and does not block the others beyond
``liveness_timeout_s``. Before each spawn the instance's ports are checked: a port another
process listens on fails the attempt with ``port N in use by pid X (name)``.

Restarts use exponential backoff (``backoff_initial_ms`` -> ``backoff_max_ms``, reset after
``stable_reset_s`` of RUNNING); ``restarts`` counts RESTARTING transitions per trading
day (restored from ``service_events`` after a supervisor restart).

Shutdown runs in reverse start order: ``ctl.shutdown`` (only to a bus service whose own
heartbeat proved it live) -> wait ``shutdown_grace_s`` -> ``CTRL_BREAK_EVENT`` ->
terminate -> kill; descendants that outlive a child (process-pool workers) are reaped.
The supervisor stops last.

``svc.*`` commands (02 §6.6; the bus whitelist admits api-gateway and ``cli``) are
idempotent by ``command_id`` (``processed_commands``). ``svc.start``/``svc.kill`` reply
when the process is spawned/killed; ``svc.stop``/``svc.restart`` reply at once with the
``STOPPING`` state and finish in the background (progress on ``sys.service``).
``instance="*"`` answers a ``ServiceList``; ``svc.stop *`` stops everything including
the supervisor (``futrisk down``). Every ``ServiceList`` carries this home's id
(``HomeLayout.home_id()``) so that a CLI never acts on another home's supervisor.

Clock-only mode (``clock_only=True``; ``futrisk up --clock-only`` or
``FUTRISK_SUPERVISOR_CLOCK_ONLY=1``, e.g. one container per service under docker compose, whose
restart policies replace the process management): ``startup_order`` is ignored and no process is
managed. The supervisor still owns the market clock - ``sys.clock`` (LV, every service and
sim-exchange at its start follow it), ``svc.clock`` - and answers ``svc.ps`` (no items),
``svc.history``, ``ctl.*``, runs the daily maintenance of supervisor.db and stops on
``svc.stop *``. ``svc.start/stop/restart/kill`` of an instance answer ``PRECONDITION_FAILED``.
Its heartbeat says ``detail.clock_only = true``.

Per-instance environment overrides (``Unit.env``) come from ``futrisk up --svc-env``
(``instance_env``) or from ``SvcRequest.env`` of ``svc.start``/``svc.restart``; only the
names of :data:`~futrisk.services.supervisor.process.ENV_ALLOW` are accepted (strategy-sim
``FUTRISK_BOTS_*``), never a name that holds a secret in the configuration; ``null``
unsets. They apply to every later (re)spawn; their *names* are recorded with every event
(``env_keys``) and shown by ``svc.ps``. ``svc.start`` with an ``env`` that changes the
environment of a running instance restarts it (``MANUAL``).
"""

from __future__ import annotations

import asyncio
import contextlib
import os
from collections import deque
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from typing import Any, TypeVar

import httpx
import msgspec
from loguru import logger
from sqlalchemy import Connection

from futrisk.bus.rpc_common import RequestContext
from futrisk.common.clock import ns_to_bj
from futrisk.db import Database, ProcessedCommands, request_hash
from futrisk.messages import codes
from futrisk.messages.base import Msg
from futrisk.messages.common import ShutdownRequest
from futrisk.messages.enums import ProcessState, ServiceChangeReason, ServiceState
from futrisk.messages.envelope import Reply, err_reply, ok_reply
from futrisk.messages.system import (
    ClockJumpRequest,
    ClockState,
    Heartbeat,
    PsRequest,
    ServiceEventBatch,
    ServiceInfo,
    ServiceList,
    ServiceStateChanged,
    SvcHistoryRequest,
    SvcRequest,
)
from futrisk.messages.topics import SYS_CLOCK, SYS_EOD
from futrisk.runtime import AsyncServiceRuntime
from futrisk.services.supervisor.clock_keeper import ClockKeeper, market_trading_day, session_templates
from futrisk.services.supervisor.events import ServiceEventLog
from futrisk.services.supervisor.maintenance import Maintenance
from futrisk.services.supervisor.policy import RestartBackoff
from futrisk.services.supervisor.process import (
    KIND_HTTP,
    KIND_PROCESS,
    SIM_EXCHANGE,
    ArgvFactory,
    ChildProcess,
    InstanceSpec,
    ProcessManager,
    apply_env,
    build_specs,
    endpoint_port,
    loggable_env,
    port_conflicts,
    secret_env_names,
    validate_env,
)

T = TypeVar("T")

PS = ProcessState
Reason = ServiceChangeReason

TICK_S = 0.05
HANG_CONFIRM_BEATS = 2
KILL_RETRY_S = 5.0
FULL_STOP_REPLY_DELAY_S = 0.3
SHUTDOWN_REQUEST_TIMEOUT_MS = 2_000
CTRL_BREAK_WAIT_MAX_S = 5.0
TERMINATE_WAIT_S = 2.0
KILL_WAIT_S = 5.0
MAX_STALE_EPOCHS = 32
MAX_EPOCH_VERDICTS = 64
GIVE_UP_FAILURES = 5
GIVE_UP_WINDOW_S = 300.0
DESCENDANT_REFRESH_S = 10.0
REAP_GRACE_S = 1.0
CLEARING_WORKER = "clearing-worker"
ALL = "*"
SVC_START = "svc.start"
SVC_STOP = "svc.stop"
SVC_RESTART = "svc.restart"
SVC_KILL = "svc.kill"
SVC_COMMANDS = (SVC_START, SVC_STOP, SVC_RESTART, SVC_KILL)
_OPEN_STATES = (PS.STARTING, PS.RUNNING, PS.STOPPING, PS.RESTARTING)  # an episode a crash may leave open
_NS_PER_S = 1_000_000_000
_NS_PER_MS = 1_000_000


class CommandError(Exception):
    """A ``svc.*`` request that cannot be executed; ``code`` is a 02 §11.1 code."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


@dataclass(eq=False)
class Unit:
    """Runtime state of one managed instance."""

    spec: InstanceSpec
    backoff: RestartBackoff
    state: ProcessState = PS.STOPPED
    proc: ChildProcess | None = None
    prev_proc: ChildProcess | None = None  # the previous process (its tree may still be dying)
    desired: bool = False  # keep it running (restart when it dies)
    parked: bool = False  # gave up after repeated failures: no restart until svc.start/svc.restart
    live: bool = False  # the current process reached liveness (verified: its own heartbeat / port)
    busy: bool = False  # a stop/kill operation owns the process right now
    spawn_mono: int = 0
    live_mono: int = 0
    started_ns: int | None = None
    svc_pid: int | None = None
    restarts: int = 0
    restarts_td: str | None = None
    last_exit_code: int | None = None
    last_error: str | None = None
    restart_at_mono: int | None = None
    restart_reason: ServiceChangeReason = Reason.EXITED
    restart_actor: str | None = None
    pending_reason: ServiceChangeReason | None = None
    pending_actor: str | None = None
    pending_detail: str | None = None
    kill_mono: int = 0
    fail_times: deque[int] = field(default_factory=deque)  # monotonic ns of recent failures (give-up)
    epoch: str | None = None
    stale_epochs: dict[str, None] = field(default_factory=dict)
    epoch_verdicts: dict[str, bool] = field(default_factory=dict)  # heartbeat epoch -> from our child tree?
    last_hb_mono: int | None = None
    last_hb_ns: int | None = None
    hb_state: ServiceState | None = None
    loop_lag_ms: int = 0
    lag_over: int = 0
    health_fail: int = 0
    probe_task: asyncio.Task[None] | None = None
    env: dict[str, str] = field(default_factory=dict)  # per-instance overrides, kept across restarts
    spawned_env: dict[str, str] | None = None  # overrides the current process was started with
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    @property
    def instance(self) -> str:
        return self.spec.instance

    def current_pid(self) -> int | None:
        if self.svc_pid is not None:
            return self.svc_pid
        return self.proc.pid if self.proc is not None else None

    def env_keys(self) -> list[str] | None:
        return sorted(self.env) or None

    def mark_stale(self, epoch: str | None) -> None:
        if not epoch:
            return
        self.stale_epochs[epoch] = None
        while len(self.stale_epochs) > MAX_STALE_EPOCHS:
            self.stale_epochs.pop(next(iter(self.stale_epochs)))

    def remember_verdict(self, epoch: str, own: bool) -> None:
        self.epoch_verdicts[epoch] = own
        while len(self.epoch_verdicts) > MAX_EPOCH_VERDICTS:
            self.epoch_verdicts.pop(next(iter(self.epoch_verdicts)))


class Supervisor:
    """Process manager + health monitor + ``svc.*`` commands + ClockKeeper of one supervisor.

    Args:
        rt: the started ``supervisor`` runtime (owns PUB/ROUTER, heartbeats, the market clock).
        db: the opened supervisor.db (:data:`~futrisk.services.supervisor.schema.METADATA`).
        argv_factory: replaces the ``futrisk svc`` command line (tests run dummy children).
        extra_env: added to every child's environment.
        instance_env: ``{instance: {NAME: value}}`` overrides of single instances
            (``futrisk up --svc-env``), kept across their restarts; raises ``ValueError``
            for names outside the per-service allow-list.
        start_at_override: ``--start-at`` of ``futrisk up`` / ``futrisk demo``.
        tick_s: monitor period.
        give_up_failures / give_up_window_s: park a unit after this many failures within
            this window (``GAVE_UP``).
        clock_only: manage no process at all (clock-only mode, see the module docstring).
    """

    def __init__(
        self,
        rt: AsyncServiceRuntime,
        db: Database,
        *,
        argv_factory: ArgvFactory | None = None,
        extra_env: Mapping[str, str] | None = None,
        instance_env: Mapping[str, Mapping[str, str | None]] | None = None,
        start_at_override: str | None = None,
        tick_s: float = TICK_S,
        give_up_failures: int = GIVE_UP_FAILURES,
        give_up_window_s: float = GIVE_UP_WINDOW_S,
        clock_only: bool = False,
    ) -> None:
        if rt.layout is None:
            raise RuntimeError("start the runtime before creating the Supervisor")
        cfg = rt.config
        self.rt = rt
        self.db = db
        self.cfg = cfg
        self.sup_cfg = cfg.services.supervisor
        self.bus_cfg = cfg.services.bus
        self.demo = cfg.app.mode == "demo"
        self.calendar = cfg.calendar
        self.home_id = rt.layout.home_id()
        self.clock_only = clock_only
        self.specs = [] if clock_only else build_specs(cfg)
        self.units: dict[str, Unit] = {s.instance: Unit(s, RestartBackoff(self.sup_cfg.restart)) for s in self.specs}
        self._secret_env = secret_env_names(cfg)
        for inst, overrides in (instance_env or {}).items():
            unit = self.units.get(inst)
            if unit is None:
                logger.warning("supervisor.env_for_unmanaged_instance", svc=inst)
                continue
            unit.env = apply_env({}, validate_env(unit.spec.service, overrides, forbidden=self._secret_env))
        self.pm = ProcessManager(
            home=rt.layout.home,
            logs_dir=rt.layout.logs_dir,
            parent_pid=os.getpid(),
            argv_factory=argv_factory,
            extra_env=extra_env,
            clock=rt.market_clock.wall,
        )
        self.events = ServiceEventLog(rt, db)
        self.processed = ProcessedCommands(db)
        self.clock = ClockKeeper(
            db=db,
            app=cfg.app,
            calendar=cfg.calendar,
            templates=session_templates(cfg.products),
            market_clock=rt.market_clock,
            publish=self._publish_clock,
            start_at_override=start_at_override,
        )
        self._tick_s = tick_s
        hb_ms = self.bus_cfg.heartbeat_ms
        self._hb_timeout_ns = hb_ms * max(1, self.bus_cfg.peer_timeout_missed) * _NS_PER_MS
        self._hang_ms = self.bus_cfg.hang_threshold_ms
        self._liveness_ns = int(self.sup_cfg.liveness_timeout_s * _NS_PER_S)
        self._settle_ns = hb_ms * _NS_PER_MS
        self._stable_ns = int(self.sup_cfg.restart.stable_reset_s * _NS_PER_S)
        self._probe_interval_s = hb_ms / 1000
        self._probe_timeout_s = min(2.0, max(0.5, 2 * hb_ms / 1000))
        self._give_up_failures = max(1, give_up_failures)
        self._give_up_window_ns = int(give_up_window_s * _NS_PER_S)
        self._tasks: set[asyncio.Task[Any]] = set()
        self._monitor_task: asyncio.Task[None] | None = None
        self._clock_task: asyncio.Task[None] | None = None
        self._startup_task: asyncio.Task[None] | None = None
        self._inflight: dict[str, tuple[str, asyncio.Future[Reply]]] = {}
        self._observers: list[Callable[[ServiceStateChanged], None]] = []
        self._http: httpx.AsyncClient | None = None
        self._started = False
        self._shutting_down = False
        self._shutdown_done = asyncio.Event()
        self.shutdown_actor: str | None = None
        self.maintenance = Maintenance(rt, db, spawn=lambda coro, name: self._spawn_task(coro, name))

    # ================================================================ lifecycle
    async def start(self) -> None:
        """ClockKeeper, handlers, heartbeat watch, monitor; then the start sequence (background)."""
        if self._started:
            raise RuntimeError("supervisor already started")
        await self.clock.start()  # sys.clock is in the LV cache before any child starts (02 §8.6)
        td = self.trading_day()
        if td is not None:
            counts = await self.events.restart_counts(td)
            for unit in self.units.values():
                unit.restarts = counts.get(unit.instance, 0)
                unit.restarts_td = td
        await self._close_open_episodes()
        self._http = httpx.AsyncClient(trust_env=False, timeout=self._probe_timeout_s)
        self._register_handlers()
        bus = [u.instance for u in self.units.values() if u.spec.is_bus]
        if bus:
            self.rt.on_heartbeat(self._on_heartbeat, bus)
        self.rt.heartbeat_provider(self._heartbeat_extras)
        clearing = self.cfg.services.instances.get(CLEARING_WORKER)
        if clearing is not None and clearing.pub:  # sys.eod DONE -> daily maintenance (03 §8)
            self.rt.subscribe(CLEARING_WORKER, [SYS_EOD], self.maintenance.on_eod)
        self._monitor_task = self._spawn_task(self._monitor_loop(), "monitor", fatal=True)
        self._clock_task = self._spawn_task(self.clock.publish_loop(), "clock-publisher", fatal=True)
        self._spawn_task(self._descendants_loop(), "descendants")
        self._started = True
        self.rt.set_state(ServiceState.RUNNING)  # the supervisor has no readiness condition (02 §8.6)
        logger.info(
            "supervisor.started",
            instances=[u.instance for u in self.units.values()],
            mode=self.cfg.app.mode,
            home_id=self.home_id,
            clock_mode=str(self.clock.mode),
            clock_version=self.clock.version,
            clock_only=self.clock_only,
        )
        if self.clock_only:
            logger.info(
                "supervisor.clock_only",
                detail="market clock only (sys.clock, svc.clock): no service process is managed here",
                ignored_startup_order=list(self.sup_cfg.startup_order),
            )
        self._startup_task = self._spawn_task(self.start_all(Reason.STARTUP, None), "startup")

    async def _close_open_episodes(self) -> None:
        """A previous supervisor crashed: close the episodes it left open (``SUPERVISOR_RESTART``).

        Its children died with it (``--parent-pid``), so each unit whose last recorded state was
        STARTING/RUNNING/STOPPING/RESTARTING gets a synthetic ``-> EXITED`` event.
        """
        last_states = await self.events.last_states()
        for unit in self.units.values():  # start order
            last = last_states.get(unit.instance)
            if last is None or last.to_state not in _OPEN_STATES:
                continue
            unit.state = last.to_state
            self._transition(
                unit,
                PS.EXITED,
                Reason.SUPERVISOR_RESTART,
                pid=last.pid,
                detail="the supervisor restarted; the process ended with the previous supervisor",
            )

    async def shutdown(self, reason: ServiceChangeReason = Reason.SHUTDOWN, actor: str | None = None) -> None:
        """Stop every instance in reverse start order, then the background tasks."""
        if self._shutting_down:
            await self._shutdown_done.wait()
            return
        self._shutting_down = True
        logger.info("supervisor.shutdown", reason=str(reason), actor=actor)
        try:
            if self._startup_task is not None and not self._startup_task.done():
                self._startup_task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await self._startup_task
            await self.stop_all(reason, actor)
            await self.events.flush()
        finally:
            current = asyncio.current_task()
            others = [t for t in self._tasks if t is not current]
            for t in others:
                t.cancel()
            if others:
                await asyncio.gather(*others, return_exceptions=True)
            for unit in self.units.values():
                self._cancel_probe(unit)
            if self._http is not None:
                await self._http.aclose()
                self._http = None
            self._shutdown_done.set()
            logger.info("supervisor.children_stopped", states={i: str(u.state) for i, u in self.units.items()})

    @property
    def shutting_down(self) -> bool:
        return self._shutting_down

    def add_observer(self, callback: Callable[[ServiceStateChanged], None]) -> None:
        """``callback(event)`` for every state transition (called on the loop, in order)."""
        self._observers.append(callback)

    # ================================================================ helpers
    def _spawn_task(self, coro: Awaitable[T], name: str, *, fatal: bool = False) -> asyncio.Task[T]:
        task: asyncio.Task[T] = asyncio.ensure_future(coro)
        task.set_name(f"supervisor:{name}")
        self._tasks.add(task)

        def done(t: asyncio.Task[T]) -> None:
            self._tasks.discard(t)
            if t.cancelled():
                return
            exc = t.exception()
            if exc is None:
                return
            if fatal:
                self.rt.fatal(exc, name)
            else:
                logger.opt(exception=exc).error("supervisor.task_failed", task=name)

        task.add_done_callback(done)
        return task

    def _mono(self) -> int:
        return self.rt.market_clock.monotonic_ns()

    def _market_ns(self) -> int:
        return self.rt.market_clock.now_ns()

    def trading_day(self, market_ns: int | None = None) -> str | None:
        """Trading day of a market time (default: now); None outside the calendar."""
        return market_trading_day(self.calendar, self._market_ns() if market_ns is None else market_ns)

    def _td_or_date(self) -> str:
        now = self._market_ns()
        return self.trading_day(now) or ns_to_bj(now).strftime("%Y%m%d")

    def _publish_clock(self, state: ClockState) -> None:
        self.rt.publish(SYS_CLOCK, state)

    def _refresh_day(self, unit: Unit, td: str | None) -> None:
        if unit.restarts_td != td:
            unit.restarts = 0
            unit.restarts_td = td

    def _bump_restarts(self, unit: Unit) -> None:
        self._refresh_day(unit, self.trading_day())
        unit.restarts += 1

    def _forget_epoch(self, unit: Unit) -> None:
        unit.mark_stale(unit.epoch)
        unit.epoch = None

    def _cancel_probe(self, unit: Unit) -> None:
        task, unit.probe_task = unit.probe_task, None
        if task is not None and not task.done():
            task.cancel()

    # ================================================================ views
    def info(self, unit: Unit) -> ServiceInfo:
        running = unit.proc is not None
        return ServiceInfo(
            instance=unit.instance,
            service=unit.spec.service,
            state=unit.state,
            pid=unit.current_pid() if running else None,
            started_ns=unit.started_ns if running else None,
            restarts=unit.restarts,
            last_exit_code=unit.last_exit_code,
            heartbeat_state=unit.hb_state if (running and unit.spec.is_bus) else None,
            last_heartbeat_ns=unit.last_hb_ns,
            env_keys=unit.env_keys(),
            last_error=unit.last_error,
        )

    def service_infos(self) -> list[ServiceInfo]:
        """``ServiceInfo`` of every managed instance, in start order (``svc.ps``)."""
        return [self.info(u) for u in self.units.values()]

    def service_list(self) -> ServiceList:
        return ServiceList(items=self.service_infos(), home_id=self.home_id)

    def _heartbeat_extras(self) -> dict[str, Any]:
        rec = self.clock.record if self.clock.started else None
        return {
            "trading_day": self.trading_day(),
            "deps": {i: ("UP" if u.state == PS.RUNNING else "DOWN") for i, u in self.units.items()},
            "detail": {
                "home_id": self.home_id,
                "clock_only": self.clock_only,
                "services": {i: str(u.state) for i, u in self.units.items()},
                "restarts": {i: u.restarts for i, u in self.units.items()},
                "clock": None if rec is None else {"mode": str(rec.mode), "offset_ns": rec.offset_ns, "version": rec.version},
            },
        }

    # ================================================================ transitions
    def _transition(
        self,
        unit: Unit,
        to: ProcessState,
        reason: ServiceChangeReason,
        *,
        actor: str | None = None,
        exit_code: int | None = None,
        pid: int | None = None,
        detail: str | None = None,
        force: bool = False,
    ) -> ServiceStateChanged | None:
        frm = unit.state
        if frm == to and not force:
            return None
        unit.state = to
        now = self._market_ns()
        td = self.trading_day(now)
        self._refresh_day(unit, td)
        ev = ServiceStateChanged(
            instance=unit.instance,
            service=unit.spec.service,
            pid=pid if pid is not None else unit.current_pid(),
            from_state=frm,
            to_state=to,
            exit_code=exit_code,
            restarts=unit.restarts,
            reason=reason,
            actor=actor,
            trading_day=td,
            ts_ns=now,
            env_keys=unit.env_keys(),
            detail=detail,
        )
        self.events.record(ev)
        log = logger.warning if to in (PS.EXITED, PS.FAILED) else logger.info
        log(
            "svc.state",
            svc=unit.instance,  # ``instance`` is the log record's own field (01 §8.7)
            from_state=str(frm),
            to_state=str(to),
            reason=str(reason),
            exit_code=exit_code,
            restarts=unit.restarts,
            actor=actor,
            pid=ev.pid,
            env_keys=ev.env_keys,
            detail=detail,
        )
        for cb in list(self._observers):
            try:
                cb(ev)
            except Exception:
                logger.exception("supervisor.observer_failed")
        return ev

    def _spawn(self, unit: Unit, reason: ServiceChangeReason, actor: str | None) -> bool:
        """Start a new process for ``unit`` (sync); False when the attempt failed at once."""
        unit.restart_at_mono = None
        unit.live = False
        unit.epoch = None
        unit.svc_pid = None
        unit.hb_state = None
        unit.last_hb_mono = None
        unit.lag_over = 0
        unit.loop_lag_ms = 0
        unit.health_fail = 0
        unit.pending_reason = None
        unit.pending_actor = None
        unit.pending_detail = None
        conflicts = port_conflicts(unit.spec.ports)
        prev = unit.prev_proc
        if conflicts and prev is not None:
            ours = prev.tree_pids()
            if any(c.pid in ours for c in conflicts):  # the previous process's tree is still dying
                killed = prev.reap_descendants()
                logger.info("svc.port_held_by_previous", svc=unit.instance, killed=killed)
                conflicts = [c for c in conflicts if c.pid not in ours]
        if conflicts:
            detail = "; ".join(c.describe() for c in conflicts)
            logger.error("svc.port_in_use", svc=unit.instance, detail=detail)
            self._attempt_failed(unit, reason, actor, detail)
            return False
        offset = version = None
        if unit.spec.service == SIM_EXCHANGE and self.clock.started:
            offset, version = self.clock.offset_ns, self.clock.version
        try:
            proc = self.pm.spawn(unit.spec, clock_offset_ns=offset, clock_version=version, env=unit.env)
        except Exception as exc:
            logger.opt(exception=exc).error("svc.spawn_failed", svc=unit.instance)
            self._attempt_failed(unit, reason, actor, f"spawn failed: {type(exc).__name__}: {exc}")
            return False
        unit.proc = proc
        unit.spawned_env = dict(unit.env)
        unit.spawn_mono = self._mono()
        unit.started_ns = self._market_ns()
        self._transition(unit, PS.STARTING, reason, actor=actor, pid=proc.pid)
        if unit.spec.kind == KIND_HTTP:
            unit.probe_task = self._spawn_task(self._probe_loop(unit, proc), f"probe:{unit.instance}")
        return True

    def _attempt_failed(self, unit: Unit, reason: ServiceChangeReason, actor: str | None, detail: str) -> None:
        """A start attempt failed before a process ran: FAILED, then retry or give up."""
        unit.proc = None
        unit.spawned_env = None
        unit.last_exit_code = None
        unit.last_error = detail
        self._transition(unit, PS.FAILED, reason, actor=actor, detail=detail, force=unit.state == PS.FAILED)
        self._after_failure(unit, reason, actor)

    def _mark_live(self, unit: Unit) -> None:
        unit.live = True
        unit.live_mono = self._mono()
        unit.last_error = None
        startup_ms = (unit.live_mono - unit.spawn_mono) // _NS_PER_MS
        self._transition(unit, PS.RUNNING, Reason.STARTUP)
        logger.info("svc.live", svc=unit.instance, pid=unit.current_pid(), startup_ms=startup_ms)
        if unit.proc is not None:
            unit.proc.refresh_descendants()

    def _on_exit(self, unit: Unit, code: int) -> None:
        """The process of ``unit`` ended: EXITED (was live) or FAILED, then restart per policy."""
        proc = unit.proc
        pid = unit.current_pid()
        unit.proc = None
        unit.last_exit_code = code
        self._cancel_probe(unit)
        reason = unit.pending_reason or Reason.EXITED
        actor = unit.pending_actor
        detail = unit.pending_detail
        unit.pending_reason = None
        unit.pending_actor = None
        unit.pending_detail = None
        was_live = unit.live
        unit.live = False
        self._forget_epoch(unit)
        unit.last_error = detail or f"exit code {code}"
        self._transition(
            unit, PS.EXITED if was_live else PS.FAILED, reason, actor=actor, exit_code=code, pid=pid, detail=detail
        )
        unit.svc_pid = None
        if proc is not None:
            unit.prev_proc = proc
            logger.info("svc.console_log", svc=unit.instance, path=str(proc.log_path))
            self._spawn_task(self._reap(proc), f"reap:{unit.instance}")
        self._after_failure(unit, reason, actor)

    def _after_failure(self, unit: Unit, reason: ServiceChangeReason, actor: str | None) -> None:
        """Restart per policy, or park the unit after too many failures (``GAVE_UP``)."""
        if not unit.desired or self._shutting_down:
            return
        if reason != Reason.KILLED_BY_DEMO:  # demo kills are deliberate: they never park a unit
            now = self._mono()
            unit.fail_times.append(now)
            while unit.fail_times and now - unit.fail_times[0] > self._give_up_window_ns:
                unit.fail_times.popleft()
            if len(unit.fail_times) >= self._give_up_failures:
                self._give_up(unit)
                return
        self._schedule_restart(unit, reason, actor)

    def _give_up(self, unit: Unit) -> None:
        window_s = self._give_up_window_ns / _NS_PER_S
        detail = (
            f"gave up after {len(unit.fail_times)} failures within {window_s:.0f}s "
            f"(last: {unit.last_error or 'unknown'}); start it again with svc.start"
        )
        unit.parked = True
        unit.desired = False
        unit.restart_at_mono = None
        unit.last_error = detail
        logger.error("svc.gave_up", svc=unit.instance, detail=detail)
        self._transition(unit, PS.FAILED, Reason.GAVE_UP, detail=detail, force=True)

    def _unpark(self, unit: Unit) -> None:
        """An operator starts the unit again: forget the failure history."""
        unit.parked = False
        unit.fail_times.clear()
        unit.backoff.reset()

    def _schedule_restart(self, unit: Unit, reason: ServiceChangeReason, actor: str | None) -> None:
        delay_ms = unit.backoff.next_delay_ms()
        unit.restart_at_mono = self._mono() + delay_ms * _NS_PER_MS
        unit.restart_reason = reason
        unit.restart_actor = actor
        self._bump_restarts(unit)
        self._transition(unit, PS.RESTARTING, reason, actor=actor)
        logger.info("svc.restart_scheduled", svc=unit.instance, delay_ms=delay_ms, failures=unit.backoff.failures)

    def _fail_kill(self, unit: Unit, reason: ServiceChangeReason, detail: str) -> None:
        """Unhealthy process: hard kill; the exit is handled by the monitor (then restart)."""
        proc = unit.proc
        if proc is None:
            return
        now = self._mono()
        if unit.pending_reason is not None and now - unit.kill_mono < KILL_RETRY_S * _NS_PER_S:
            return
        unit.pending_reason = reason
        unit.pending_actor = None
        unit.pending_detail = detail
        unit.kill_mono = now
        logger.warning("svc.unhealthy", svc=unit.instance, reason=str(reason), detail=detail, pid=unit.current_pid())
        proc.kill_tree()

    async def _reap(self, proc: ChildProcess, grace_s: float = REAP_GRACE_S) -> None:
        """Kill descendants that outlived ``proc`` (process-pool workers, the frozen build)."""
        if not proc.descendant_pids():
            return
        if not await asyncio.to_thread(proc.alive_descendants):
            return
        await asyncio.sleep(grace_s)  # let them exit on their own first (parent watch, job objects)
        killed = await asyncio.to_thread(proc.reap_descendants)
        if killed:
            logger.warning("svc.orphans_killed", svc=proc.spec.instance, pids=killed)

    async def _descendants_loop(self) -> None:
        """Keep each child's descendant list fresh, so orphans can be reaped after a crash."""
        while True:
            await asyncio.sleep(DESCENDANT_REFRESH_S)
            for unit in list(self.units.values()):
                proc = unit.proc
                if proc is not None and proc.poll() is None:
                    await asyncio.to_thread(proc.refresh_descendants)

    # ================================================================ health monitor
    async def _monitor_loop(self) -> None:
        while True:
            await asyncio.sleep(self._tick_s)
            now = self._mono()
            for unit in self.units.values():
                try:
                    self._check_unit(unit, now)
                except Exception:
                    logger.exception("supervisor.check_failed", svc=unit.instance)

    def _check_unit(self, unit: Unit, now: int) -> None:
        if unit.busy:
            return
        proc = unit.proc
        if proc is not None:
            code = proc.poll()
            if code is not None:
                self._on_exit(unit, code)
                return
            if unit.state == PS.STARTING:
                if unit.spec.kind == KIND_PROCESS and now - unit.spawn_mono >= self._settle_ns:
                    self._mark_live(unit)
                elif now - unit.spawn_mono > self._liveness_ns:
                    self._fail_kill(unit, Reason.HEARTBEAT_LOST, f"not live within {self.sup_cfg.liveness_timeout_s}s")
            elif unit.state == PS.RUNNING:
                if unit.spec.is_bus:
                    if unit.last_hb_mono is not None and now - unit.last_hb_mono > self._hb_timeout_ns:
                        self._fail_kill(
                            unit,
                            Reason.HEARTBEAT_LOST,
                            f"no heartbeat for {(now - unit.last_hb_mono) // _NS_PER_MS} ms",
                        )
                    elif unit.lag_over >= HANG_CONFIRM_BEATS:
                        self._fail_kill(unit, Reason.LOOP_HANG, f"loop_lag_ms {unit.loop_lag_ms} > {self._hang_ms}")
                elif unit.spec.kind == KIND_HTTP and unit.health_fail >= max(1, self.bus_cfg.peer_timeout_missed):
                    self._fail_kill(unit, Reason.HEARTBEAT_LOST, f"{unit.health_fail} failed health checks")
                if now - unit.live_mono >= self._stable_ns:
                    if unit.backoff.stable_for((now - unit.live_mono) / _NS_PER_S):
                        logger.info("svc.backoff_reset", svc=unit.instance)
                    unit.fail_times.clear()
            return
        if (
            unit.state == PS.RESTARTING
            and unit.desired
            and not self._shutting_down
            and unit.restart_at_mono is not None
            and now >= unit.restart_at_mono
        ):
            self._spawn(unit, unit.restart_reason, unit.restart_actor)

    def _on_heartbeat(self, hb: Heartbeat) -> None:
        """``sys.heartbeat.{instance}`` of a managed bus service (liveness, loss, loop lag).

        Only heartbeats sent by the spawned process or one of its descendants count: another
        process publishing on the configured PUB address (e.g. another home's service owning
        the same port) is ignored, whatever instance name it uses.
        """
        unit = self.units.get(hb.instance)
        if unit is None or not unit.spec.is_bus or hb.epoch in unit.stale_epochs:
            return
        proc = unit.proc
        if proc is None:
            unit.mark_stale(hb.epoch)  # a late beat of a process that is gone
            return
        if unit.state not in (PS.STARTING, PS.RUNNING):
            return
        own = unit.epoch_verdicts.get(hb.epoch)
        if own is None:
            hb_home = (hb.detail or {}).get("home_id")
            own = (hb_home is None or hb_home == self.home_id) and proc.is_own_pid(hb.pid)
            unit.remember_verdict(hb.epoch, own)
            if not own:
                logger.warning(
                    "svc.foreign_heartbeat",
                    svc=unit.instance,
                    pid=hb.pid,
                    epoch=hb.epoch,
                    hb_home_id=hb_home,
                    child_pid=proc.pid,
                    detail="heartbeat from a process outside the child's tree ignored (another home on these ports?)",
                )
        if not own:
            return
        if unit.epoch is None:
            unit.epoch = hb.epoch
        elif hb.epoch != unit.epoch:
            unit.mark_stale(unit.epoch)
            unit.epoch = hb.epoch
        unit.last_hb_mono = self._mono()
        unit.last_hb_ns = self._market_ns()
        unit.hb_state = hb.state
        unit.svc_pid = hb.pid
        unit.loop_lag_ms = hb.loop_lag_ms
        unit.lag_over = unit.lag_over + 1 if hb.loop_lag_ms > self._hang_ms else 0
        if unit.state == PS.STARTING:
            self._mark_live(unit)

    async def _probe_loop(self, unit: Unit, proc: ChildProcess) -> None:
        """sim-exchange: ``GET /v1/health`` every heartbeat period while the process runs."""
        url = unit.spec.health_url
        assert url is not None
        owner_ok = False
        warned = False
        while unit.proc is proc and proc.poll() is None:
            ok = await self._probe(url)
            if unit.proc is not proc:
                return
            if ok and not owner_ok:
                owner_ok = await asyncio.to_thread(self._health_port_is_ours, url, proc)
                if not owner_ok:
                    ok = False
                    if not warned:
                        warned = True
                        logger.warning(
                            "svc.foreign_health",
                            svc=unit.instance,
                            url=url,
                            detail="another process answers on the health port; not taken as liveness",
                        )
            if ok:
                unit.health_fail = 0
                unit.last_hb_mono = self._mono()
                unit.last_hb_ns = self._market_ns()
                if unit.state == PS.STARTING and not unit.busy:
                    self._mark_live(unit)
            elif unit.state == PS.RUNNING:
                unit.health_fail += 1
            await asyncio.sleep(self._probe_interval_s)

    @staticmethod
    def _health_port_is_ours(url: str, proc: ChildProcess) -> bool:
        """The listening socket of the health URL belongs to ``proc``'s tree (psutil)."""
        where = endpoint_port(url)
        if where is None:
            return True
        proc.refresh_descendants()
        others = port_conflicts([where], ignore_pids=proc.tree_pids())
        return all(c.pid is None for c in others)  # pid None: no socket table (cannot tell) - accept

    async def _probe(self, url: str) -> bool:
        client = self._http
        if client is None:
            return False
        try:
            resp = await client.get(url, timeout=self._probe_timeout_s)
        except (httpx.HTTPError, OSError):
            return False
        return resp.status_code == 200

    # ================================================================ operations
    async def start_all(self, reason: ServiceChangeReason, actor: str | None) -> None:
        """Start every instance in start order, each waiting only for liveness (01 §6.8)."""
        units = list(self.units.values())
        for unit in units:
            if not self._shutting_down:
                unit.desired = True
        for unit in units:
            if self._shutting_down:
                return
            if not unit.desired:  # stopped by an operator meanwhile
                continue
            await self.start_unit(unit, reason, actor)
            await self._wait_live(unit)
        logger.info("supervisor.start_sequence_done", states={i: str(u.state) for i, u in self.units.items()})

    async def _wait_live(self, unit: Unit) -> None:
        deadline = self._mono() + self._liveness_ns + _NS_PER_S
        while unit.state == PS.STARTING and unit.proc is not None and not unit.live:
            if self._mono() > deadline:
                return
            await asyncio.sleep(self._tick_s)

    async def stop_all(self, reason: ServiceChangeReason, actor: str | None) -> None:
        """Stop every instance in reverse start order (01 §6.8)."""
        units = list(self.units.values())
        for unit in units:
            unit.desired = False
        for unit in reversed(units):
            await self.stop_unit(unit, reason, actor)

    async def restart_all(self, actor: str | None, previous: asyncio.Task[Any] | None = None) -> None:
        """``svc.restart *``: cancel a running start sequence, stop all (reverse order), start all."""
        if previous is None:
            previous = self._startup_task
        if previous is not None and not previous.done() and previous is not asyncio.current_task():
            previous.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await previous
        await self.stop_all(Reason.MANUAL, actor)
        if not self._shutting_down:
            await self.start_all(Reason.MANUAL, actor)

    async def start_unit(self, unit: Unit, reason: ServiceChangeReason, actor: str | None) -> bool:
        """Spawn ``unit`` unless a process is already there (idempotent); skips a pending backoff."""
        async with unit.lock:
            if self._shutting_down:
                return False
            unit.desired = True
            if unit.parked or unit.state == PS.FAILED:
                self._unpark(unit)
            if unit.proc is not None:
                return False
            return self._spawn(unit, reason, actor)

    async def stop_unit(self, unit: Unit, reason: ServiceChangeReason, actor: str | None, *, restart: bool = False) -> None:
        """Graceful stop; with ``restart`` the instance is respawned at once (``svc.restart``)."""
        async with unit.lock:
            if not restart:
                unit.desired = False
            else:
                self._unpark(unit)
            proc = unit.proc
            if proc is None:
                unit.restart_at_mono = None
                if restart and not self._shutting_down:
                    unit.desired = True
                    self._spawn(unit, reason, actor)
                elif unit.state in (PS.RESTARTING, PS.EXITED, PS.FAILED):
                    self._transition(unit, PS.STOPPED, reason, actor=actor)
                return
            unit.busy = True
            try:
                self._cancel_probe(unit)
                self._transition(unit, PS.STOPPING, reason, actor=actor)
                code = await self._terminate(unit, proc, reason)
            finally:
                unit.busy = False
            pid = unit.current_pid()
            if code is None:
                logger.critical("svc.stop_failed", svc=unit.instance, pid=pid, detail="process survived kill")
            unit.proc = None
            unit.prev_proc = proc
            unit.live = False
            unit.last_exit_code = code
            unit.pending_reason = None
            unit.pending_actor = None
            unit.pending_detail = None
            self._forget_epoch(unit)
            await self._reap(proc, grace_s=0.2)
            if restart and not self._shutting_down:
                unit.desired = True
                self._bump_restarts(unit)
                self._transition(unit, PS.RESTARTING, reason, actor=actor, exit_code=code, pid=pid)
                self._spawn(unit, reason, actor)
            else:
                self._transition(unit, PS.STOPPED, reason, actor=actor, exit_code=code, pid=pid)
                unit.svc_pid = None

    async def kill_unit(self, unit: Unit, actor: str | None) -> None:
        """``svc.kill``: hard kill to simulate a crash; the restart policy brings it back."""
        async with unit.lock:
            proc = unit.proc
            if proc is None or proc.poll() is not None:
                raise CommandError(codes.CONFLICT, f"{unit.instance} is not running (state {unit.state})")
            unit.busy = True
            try:
                unit.pending_reason = Reason.KILLED_BY_DEMO
                unit.pending_actor = actor
                unit.pending_detail = None
                unit.kill_mono = self._mono()
                logger.warning("svc.kill", svc=unit.instance, pid=unit.current_pid(), actor=actor)
                proc.kill_tree()
                code = await proc.wait(KILL_WAIT_S)
            finally:
                unit.busy = False
            if code is None:
                logger.error("svc.kill_pending", svc=unit.instance, detail="process still alive after kill")
            elif unit.proc is proc:
                self._on_exit(unit, code)

    async def _terminate(self, unit: Unit, proc: ChildProcess, reason: ServiceChangeReason) -> int | None:
        """``ctl.shutdown`` (verified bus service) -> grace -> ``CTRL_BREAK_EVENT`` -> terminate -> kill (01 §6.8).

        ``ctl.shutdown`` goes to the configured ROUTER address, so it is only sent when this
        unit's own process proved it owns that endpoint (a heartbeat from its process tree,
        which the runtimes emit only after binding PUB and ROUTER). Otherwise the stop goes
        straight to the signal ladder of the child itself.
        """
        grace = max(0.0, float(self.sup_cfg.shutdown_grace_s))
        if unit.spec.is_bus and unit.live and unit.proc is proc and proc.poll() is None:
            reply = await self.rt.request(
                unit.instance,
                "ctl.shutdown",
                ShutdownRequest(reason=f"supervisor {str(reason).lower()}", deadline_ms=int(grace * 1000)),
                timeout_ms=SHUTDOWN_REQUEST_TIMEOUT_MS,
                retries=0,
            )
            if reply.ok:
                code = await proc.wait(grace)
                if code is not None:
                    return code
                logger.warning("svc.stop_grace_exceeded", svc=unit.instance, grace_s=grace)
            else:
                logger.warning("svc.shutdown_request_failed", svc=unit.instance, code=reply.code, message=reply.message)
            break_wait = min(grace, CTRL_BREAK_WAIT_MAX_S)
        else:
            break_wait = grace if not unit.spec.is_bus else min(grace, CTRL_BREAK_WAIT_MAX_S)
        code = proc.poll()
        if code is not None:
            return code
        if proc.send_ctrl_break():
            code = await proc.wait(break_wait)
            if code is not None:
                return code
        logger.warning("svc.terminate", svc=unit.instance, pid=unit.current_pid())
        proc.terminate()
        code = await proc.wait(TERMINATE_WAIT_S)
        if code is not None:
            return code
        proc.kill_tree()
        return await proc.wait(KILL_WAIT_S)

    # ================================================================ commands
    def _register_handlers(self) -> None:
        add = self.rt.add_handler
        add("svc.ps", self._h_ps, payload_type=PsRequest)
        add("svc.history", self._h_history, payload_type=SvcHistoryRequest)
        for command in SVC_COMMANDS:
            add(command, self._h_svc, payload_type=SvcRequest)
        add("svc.clock", self._h_clock, payload_type=ClockJumpRequest)

    def _h_ps(self, command: str, payload: Msg, ctx: RequestContext) -> Reply:
        return ok_reply(self.service_list())

    async def _h_history(self, command: str, payload: Msg, ctx: RequestContext) -> Reply:
        assert isinstance(payload, SvcHistoryRequest)
        items = await self.events.history(trading_day=payload.trading_day, since_ns=payload.since_ns)
        return ok_reply(ServiceEventBatch(items=items))

    async def _h_clock(self, command: str, payload: Msg, ctx: RequestContext) -> Reply:
        assert isinstance(payload, ClockJumpRequest)
        return await self.clock.jump(payload, requester=ctx.requester, received_ns=ctx.received_ns)

    async def _h_svc(self, command: str, payload: Msg, ctx: RequestContext) -> Reply:
        assert isinstance(payload, SvcRequest)
        return await self._idempotent(command, payload, ctx)

    async def _idempotent(self, command: str, req: SvcRequest, ctx: RequestContext) -> Reply:
        """Run a ``svc.*`` command once per ``command_id`` (02 §4.7, 03 §4.0)."""
        cid = req.command_id
        if not cid:
            return err_reply(codes.BAD_REQUEST, "command_id is required")
        req_hash = request_hash(req)
        running = self._inflight.get(cid)
        if running is not None:
            first_hash, first_fut = running
            if first_hash != req_hash:
                return err_reply(codes.CONFLICT, f"idempotency key {cid} reused with different content")
            first = await asyncio.shield(first_fut)
            return msgspec.structs.replace(first, duplicate=True)
        # checked on the writer: ordered after every record submitted before this request
        chk = await self.db.run_write(lambda c: self.processed.check(c, cid, req_hash))
        if chk.reply is not None:
            return chk.reply
        fut: asyncio.Future[Reply] = asyncio.get_running_loop().create_future()
        self._inflight[cid] = (req_hash, fut)
        try:
            try:
                reply = await self._execute(command, req)
            except CommandError as exc:
                reply = err_reply(exc.code, exc.message)
            except Exception as exc:
                logger.opt(exception=exc).error("svc.command_failed", command=command, svc=req.instance)
                reply = err_reply(codes.INTERNAL, f"{type(exc).__name__}: {exc}")
                fut.set_result(reply)
                return reply
            td = self._td_or_date()

            def record(c: Connection) -> Reply:
                # check-and-record in ONE writer job: a retry that slipped past the in-flight
                # map gets the stored first reply instead of a primary-key error
                again = self.processed.check(c, cid, req_hash)
                if again.reply is not None:
                    return again.reply
                self.processed.record(
                    c,
                    command_id=cid,
                    command=command,
                    requester=ctx.requester,
                    request_hash=req_hash,
                    reply=reply,
                    trading_day=td,
                    received_ns=ctx.received_ns,
                )
                return reply

            reply = await self.db.run_write(record)
            fut.set_result(reply)
            return reply
        finally:
            self._inflight.pop(cid, None)
            if not fut.done():
                fut.set_result(err_reply(codes.INTERNAL, "command interrupted"))

    async def _execute(self, command: str, req: SvcRequest) -> Reply:
        if self._shutting_down or self.rt.is_stopping:
            raise CommandError(codes.CONFLICT, "the supervisor is shutting down")
        inst = req.instance.strip()
        actor = req.actor or None
        if req.env is not None:
            if command not in (SVC_START, SVC_RESTART):
                raise CommandError(codes.BAD_REQUEST, "env is accepted by svc.start and svc.restart only")
            if inst == ALL:
                raise CommandError(codes.BAD_REQUEST, "env needs a single instance, not '*'")
        if inst == ALL:
            return await self._execute_all(command, actor)
        if self.clock_only:
            raise CommandError(
                codes.PRECONDITION_FAILED,
                f"{command} {inst}: this supervisor runs in clock-only mode and manages no processes",
            )
        unit = self.units.get(inst)
        if unit is None:
            raise CommandError(codes.NOT_FOUND, f"unknown instance {inst!r}; managed: {', '.join(self.units)}")
        changes: dict[str, str | None] | None = None
        if req.env is not None:
            try:
                changes = validate_env(unit.spec.service, req.env, forbidden=self._secret_env)
            except ValueError as exc:
                raise CommandError(codes.BAD_REQUEST, str(exc)) from exc
        if command == SVC_KILL:
            if not self.demo:
                raise CommandError(codes.PRECONDITION_FAILED, "svc.kill is only available in demo mode (app.mode=demo)")
            await self.kill_unit(unit, actor)
        elif command == SVC_START:
            if changes and self._apply_env(unit, changes, actor):
                # running with another environment: restart it so that the new one applies
                self._spawn_task(self.stop_unit(unit, Reason.MANUAL, actor, restart=True), f"restart:{inst}")
                await asyncio.sleep(0)
            else:
                await self.start_unit(unit, Reason.MANUAL, actor)
        elif command == SVC_STOP:
            self._spawn_task(self.stop_unit(unit, Reason.MANUAL, actor), f"stop:{inst}")
            await asyncio.sleep(0)  # let it reach STOPPING before answering
        elif command == SVC_RESTART:
            if changes:
                self._apply_env(unit, changes, actor)
            self._spawn_task(self.stop_unit(unit, Reason.MANUAL, actor, restart=True), f"restart:{inst}")
            await asyncio.sleep(0)
        else:  # pragma: no cover - handlers are registered for SVC_COMMANDS only
            raise CommandError(codes.BAD_REQUEST, f"unknown command {command}")
        return ok_reply(self.info(unit))

    def _apply_env(self, unit: Unit, changes: Mapping[str, str | None], actor: str | None) -> bool:
        """Apply overrides to ``unit.env`` (``None`` unsets); True when its running process differs."""
        unit.env = apply_env(unit.env, changes)
        logger.info("svc.env", svc=unit.instance, actor=actor, env=loggable_env(unit.env), changed=sorted(changes))
        return unit.proc is not None and unit.spawned_env != unit.env

    async def _execute_all(self, command: str, actor: str | None) -> Reply:
        if command == SVC_KILL:
            raise CommandError(codes.BAD_REQUEST, "svc.kill takes a single instance")
        if command == SVC_STOP:
            # futrisk down: everything, the supervisor last; answer before the ROUTER closes
            self.shutdown_actor = actor
            asyncio.get_running_loop().call_later(
                FULL_STOP_REPLY_DELAY_S, self.rt.request_stop, f"svc.stop * by {actor or 'unknown'}"
            )
        elif command == SVC_START:
            if self._startup_task is None or self._startup_task.done():
                self._startup_task = self._spawn_task(self.start_all(Reason.MANUAL, actor), "start-all")
        elif command == SVC_RESTART:
            previous = self._startup_task
            self._startup_task = self._spawn_task(self.restart_all(actor, previous), "restart-all")
        await asyncio.sleep(0)
        return ok_reply(self.service_list())


__all__ = ["ALL", "CommandError", "Supervisor", "Unit"]
