"""State and plumbing shared by the sync and asyncio service runtimes (02 §4.8, §8.5, §8.6).

A runtime owns, for one service instance: the active configuration, logging,
the process epoch, the market clock (following ``sys.clock``), the service
state (``STARTING``/``RUNNING``/``DEGRADED``/``STOPPING``), the databases and
the outbox wired to the publisher, heartbeat content, peer liveness, the
standard ``ctl.*`` handlers and the config 2PC participant.
"""

from __future__ import annotations

import threading
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

from loguru import logger
from sqlalchemy import Connection, MetaData

from futrisk import __version__
from futrisk.bus.control import ControlHandlers
from futrisk.bus.endpoints import Endpoints, service_of
from futrisk.bus.fact import FactTracker
from futrisk.bus.heartbeat import HeartbeatBuilder, HeartbeatExtras, PeerLiveness
from futrisk.bus.lv import CloneSync
from futrisk.bus.lvcache import LastValueCache
from futrisk.bus.publisher import PublisherCore
from futrisk.bus.rpc_common import Handler
from futrisk.bus.rpc_server import BusyCode, risk_busy_code
from futrisk.bus.sockets import FACT_LINGER_MS
from futrisk.bus.whitelist import Whitelist
from futrisk.common.clock import Clock, MarketClock, SystemClock
from futrisk.common.config import FullConfig, ensure_home_config, load_active_config
from futrisk.common.ids import new_epoch
from futrisk.common.logging import setup_logging
from futrisk.common.paths import HomeLayout, resolve_home
from futrisk.db.database import Database, Synchronous, reject_async
from futrisk.db.migrations import Migration
from futrisk.db.outbox import Outbox
from futrisk.db.processed import ProcessedCommands
from futrisk.messages import codes
from futrisk.messages.base import Msg
from futrisk.messages.common import ShutdownRequest
from futrisk.messages.enums import ConfigEffect, ServiceState, TradingMode
from futrisk.messages.envelope import Reply
from futrisk.messages.system import Heartbeat, LatencySummary
from futrisk.messages.topics import sys_heartbeat
from futrisk.runtime import gcpolicy
from futrisk.runtime.clock_sync import SUPERVISOR, ClockFollower
from futrisk.runtime.config2pc import CommitCallback, ConfigParticipant, Validator

API_GATEWAY = "api-gateway"
_NO_FACT_PUBLISHERS = frozenset({API_GATEWAY})


class RuntimeBase:
    """Common part of :class:`AsyncServiceRuntime` and :class:`SyncServiceRuntime`.

    Args:
        instance: instance id (``risk-engine-1``); must exist in ``services.yaml``.
        service: service name (default derived from ``instance``).
        home: ``<home>`` directory (default: ``resolve_home()``); holds db/, logs/, config/.
        config: inject a :class:`FullConfig` instead of loading ``active.json``.
        market_clock: inject a market clock; disables following ``sys.clock``
            unless ``follow_clock=True``.
        follow_clock: follow the supervisor's ``sys.clock`` (default: not for
            the supervisor itself, not when a clock is injected).
        setup_logs: configure loguru for this instance (tests pass ``False``).
        heartbeat_ms: override ``services.bus.heartbeat_ms``.
        whitelist: enforce the 02 §4.11 caller whitelist on the ROUTER.
        pub_linger_ms: PUB LINGER (default 1000 ms for FACT publishers, 0 for api-gateway).
        parent_pid: pid of the process that spawned this service (``futrisk svc
            --parent-pid``); the runtime stops itself when that process is gone
            (checked every ``parent_poll_s`` seconds, 01 §5.3.6).
    """

    def __init__(
        self,
        instance: str,
        *,
        service: str | None = None,
        home: str | Path | None = None,
        config: FullConfig | None = None,
        market_clock: MarketClock | None = None,
        follow_clock: bool | None = None,
        setup_logs: bool = True,
        log_console: bool = True,
        log_level: str | None = None,
        app_version: str = __version__,
        heartbeat_ms: int | None = None,
        whitelist: bool = True,
        pub_linger_ms: int | None = None,
        wall_clock: Clock | None = None,
        parent_pid: int | None = None,
        parent_poll_s: float = 2.0,
    ) -> None:
        self.instance = instance
        self.service = service or service_of(instance)
        self.epoch = new_epoch()
        self.app_version = app_version
        self._home_arg = home
        self._config_arg = config
        self._wall: Clock = wall_clock or SystemClock()
        self.market_clock = market_clock or MarketClock(self._wall)
        if follow_clock is None:
            follow_clock = market_clock is None and instance != SUPERVISOR
        self._follow_clock = follow_clock
        self.clock_follower = ClockFollower(self.market_clock, ready=not follow_clock)
        self._setup_logs = setup_logs
        self._log_console = log_console
        self._log_level = log_level
        self._heartbeat_ms_override = heartbeat_ms
        self._use_whitelist = whitelist
        self._pub_linger_override = pub_linger_ms

        self._state = ServiceState.STARTING
        self._state_lock = threading.Lock()
        self.started_ns = self.market_clock.now_ns()
        self._extras_lock = threading.Lock()
        self._extras = HeartbeatExtras()
        self._extras_provider: Callable[[], HeartbeatExtras | Mapping[str, Any]] | None = None
        self._last_loop_mono = self._wall.monotonic_ns()

        self.layout: HomeLayout | None = None
        self._home_id: str | None = None
        self._gc: gcpolicy.GcSettings | None = None
        self._config: FullConfig | None = None
        self._endpoints: Endpoints | None = None
        self.databases: list[Database] = []
        self.outbox: Outbox | None = None
        self._core = PublisherCore(instance, self.epoch, self.market_clock)
        self.peers = PeerLiveness(clock=self._wall, on_change=self._dispatch_peer_change)
        self._peer_callbacks: list[Callable[[str, str], None]] = []
        self._hb_handlers: list[Callable[[Heartbeat], None]] = []
        self._fact: FactTracker | None = None
        self._clones: list[CloneSync] = []  # snapshot=True subscriptions (fed their publisher's heartbeats)
        self._commit_callbacks: list[CommitCallback] = []
        self._validators: list[Validator] = []
        self._stop_callbacks: list[Callable[[], Any]] = []
        self._config_participant: ConfigParticipant | None = None
        self._heartbeat_builder: HeartbeatBuilder | None = None
        self.exit_code = 0
        self.stop_reason = ""
        self.parent_pid = parent_pid
        self._parent_poll_s = max(0.05, parent_poll_s)
        self._fatal_error: BaseException | None = None

    # ----------------------------------------------------------- config
    @property
    def config(self) -> FullConfig:
        if self._config is None:
            raise RuntimeError("runtime not started (config not loaded)")
        return self._config

    @property
    def endpoints(self) -> Endpoints:
        if self._endpoints is None:
            raise RuntimeError("runtime not started (endpoints unknown)")
        return self._endpoints

    @property
    def home_id(self) -> str | None:
        """``HomeLayout.home_id()`` of this runtime's home (``ctl.ping`` ``Pong.home_id``, heartbeat
        ``detail["home_id"]``); ``None`` before start or with an injected config and no ``home``."""
        return self._home_id

    @property
    def heartbeat_ms(self) -> int:
        if self._heartbeat_ms_override is not None:
            return self._heartbeat_ms_override
        return self.config.services.bus.heartbeat_ms

    def _load_config(self) -> None:
        layout = HomeLayout(resolve_home(self._home_arg))
        if self._config_arg is not None:
            layout.ensure()
            self._config = self._config_arg
        else:
            ensure_home_config(layout)
            self._config = load_active_config(layout)
        self.layout = layout
        if self._config_arg is None or self._home_arg is not None:  # injected config without a home: no id
            try:
                self._home_id = layout.home_id()
            except OSError:
                logger.exception("home_id.unavailable", home=str(layout.home))
        self._endpoints = Endpoints(self._config.services)
        self._endpoints.instance(self.instance)  # KeyError for unknown instances
        self._gc = gcpolicy.GcSettings.from_config(getattr(self._config.services.bus, "gc", None))
        gcpolicy.apply_at_start(self._gc)
        self.peers = PeerLiveness(
            heartbeat_ms=self.heartbeat_ms,
            missed=self._config.services.bus.peer_timeout_missed,
            clock=self._wall,
            on_change=self._dispatch_peer_change,
        )
        self._config_participant = ConfigParticipant(
            instance=self.instance,
            layout=layout,
            get_config=lambda: self.config,
            apply_config=self._apply_config,
            validators=self._validators,
            clock=self._wall,
        )

    def _setup_logging(self) -> None:
        if not self._setup_logs:
            return
        cfg = self.config.app.logging
        assert self.layout is not None
        setup_logging(
            self.instance,
            self.layout.logs_dir,
            level=self._log_level or cfg.level,
            retention_days=cfg.retention_days,
            rotation_mb=cfg.rotation_mb,
            console=self._log_console,
        )

    def on_config_commit(self, callback: CommitCallback) -> None:
        """Register ``callback(new_config, effects)`` run when a config version is committed.

        Runs on the thread that handles commands (loop thread); the runtime has
        already swapped ``self.config``. ``NEXT_TRADING_DAY`` sections should be
        staged by the service until that trading day (02 §8.5).
        """
        self._commit_callbacks.append(callback)

    def add_config_validator(self, validator: Validator) -> None:
        """``validator(current, new) -> errors`` consulted by ``ctl.config.prepare``."""
        self._validators.append(validator)
        if self._config_participant is not None:
            self._config_participant.add_validator(validator)

    def _apply_config(self, new: FullConfig, effects: dict[str, ConfigEffect]) -> None:
        self._config = new
        for cb in self._commit_callbacks:
            cb(new, effects)

    @property
    def config_participant(self) -> ConfigParticipant:
        if self._config_participant is None:
            raise RuntimeError("runtime not started")
        return self._config_participant

    # ------------------------------------------------------------ state
    @property
    def state(self) -> ServiceState:
        return self._state

    def set_state(self, state: ServiceState | str) -> None:
        """Set the self-reported service state (published in the next heartbeat, sent at once).

        The first RUNNING of the process freezes the start-up objects out of
        the garbage collector first (:mod:`futrisk.runtime.gcpolicy`).
        """
        new = ServiceState(str(state))
        if new == ServiceState.RUNNING and self._gc is not None:
            gcpolicy.freeze_once(self._gc)
        with self._state_lock:
            old = self._state
            if old == ServiceState.STOPPING and new != ServiceState.STOPPING:
                return  # never leave STOPPING
            self._state = new
        if old != new:
            logger.info("service.state", instance=self.instance, state=new.value, previous=old.value)
            self._heartbeat_now()

    def _heartbeat_now(self) -> None:
        """Emit a heartbeat immediately (overridden by the runtimes)."""

    @property
    def is_stopping(self) -> bool:
        return self._state == ServiceState.STOPPING

    @property
    def fatal_error(self) -> BaseException | None:
        """The first unexpected error that stopped the service (``None`` for a normal stop)."""
        return self._fatal_error

    def _record_fatal(self, exc: BaseException) -> bool:
        """Remember the first fatal error; returns True if this was the first."""
        if self._fatal_error is not None:
            return False
        self._fatal_error = exc
        self.exit_code = 1
        return True

    def _close_clean(self, clean: bool | None) -> bool:
        """Close databases with ``clean_shutdown=1`` only after a normal stop."""
        return (self._fatal_error is None) if clean is None else clean

    def on_stop(self, callback: Callable[[], Any]) -> None:
        """Register a shutdown hook (called in reverse order; async runtimes may pass coroutines functions)."""
        self._stop_callbacks.append(callback)

    # --------------------------------------------------- loop lag / clock
    def loop_done(self) -> None:
        """Mark the end of one business-loop iteration (``loop_lag_ms`` in heartbeats)."""
        self._last_loop_mono = self._wall.monotonic_ns()

    def loop_lag_ms(self) -> int:
        return max(0, (self._wall.monotonic_ns() - self._last_loop_mono) // 1_000_000)

    @property
    def clock_ready(self) -> bool:
        return self.clock_follower.ready

    # ------------------------------------------------------- heartbeats
    def set_heartbeat_fields(
        self,
        *,
        trading_day: str | None = None,
        trading_mode: TradingMode | str | None = None,
        queues: Mapping[str, int] | None = None,
        rates: Mapping[str, float] | None = None,
        latency_us: Mapping[str, LatencySummary] | None = None,
        deps: Mapping[str, str] | None = None,
        detail: Mapping[str, Any] | None = None,
    ) -> None:
        """Push service fields into the heartbeat (thread-safe; ``None`` leaves a field unchanged)."""
        with self._extras_lock:
            ex = self._extras
            if trading_day is not None:
                ex.trading_day = trading_day
            if trading_mode is not None:
                ex.trading_mode = TradingMode(str(trading_mode))
            if queues is not None:
                ex.queues = dict(queues)
            if rates is not None:
                ex.rates = dict(rates)
            if latency_us is not None:
                ex.latency_us = dict(latency_us)
            if deps is not None:
                ex.deps = dict(deps)
            if detail is not None:
                ex.detail = dict(detail)

    def heartbeat_provider(self, provider: Callable[[], HeartbeatExtras | Mapping[str, Any]] | None) -> None:
        """Pull-style fields: ``provider()`` is called whenever a heartbeat is built.

        Sync runtime: called from the heartbeat thread (must be thread-safe).
        Asyncio runtime: called from the event loop.
        """
        self._extras_provider = provider

    def _builtin_queues(self) -> dict[str, int]:
        return {f"db.{db.name}": db.queue_size() for db in self.databases if db.is_open}

    def _extras_snapshot(self) -> HeartbeatExtras:
        with self._extras_lock:
            ex = HeartbeatExtras(
                trading_day=self._extras.trading_day,
                trading_mode=self._extras.trading_mode,
                queues=dict(self._extras.queues or {}),
                rates=dict(self._extras.rates or {}),
                latency_us=dict(self._extras.latency_us or {}),
                deps=dict(self._extras.deps or {}),
                detail=dict(self._extras.detail or {}),
            )
        if self._extras_provider is not None:
            try:
                got = self._extras_provider()
            except Exception:
                logger.exception("heartbeat.provider_failed")
                got = None
            if isinstance(got, HeartbeatExtras):
                got = {k: v for k, v in got.__dict__.items() if v is not None}
            if got:
                for k, v in got.items():
                    if k in ("queues", "rates", "latency_us", "deps", "detail"):
                        getattr(ex, k).update(v or {})
                    elif k == "trading_mode" and v is not None:
                        ex.trading_mode = TradingMode(str(v))
                    elif hasattr(ex, k):
                        setattr(ex, k, v)
        if self._home_id is not None:
            ex.detail = {**(ex.detail or {}), "home_id": self._home_id}  # cross-home guard
        gc_stats = gcpolicy.stats_snapshot()
        if gc_stats is not None:
            ex.detail = {**(ex.detail or {}), "gc": gc_stats}
        queues = self._builtin_queues()
        queues.update(ex.queues or {})
        ex.queues = queues
        deps = self.peers.deps()
        deps.update(ex.deps or {})
        ex.deps = deps
        return ex

    def build_heartbeat(self) -> Heartbeat:
        """The current :class:`Heartbeat` (also the ``ctl.status`` reply)."""
        if self._heartbeat_builder is None:
            self._heartbeat_builder = HeartbeatBuilder(
                instance=self.instance,
                service=self.service,
                epoch=self.epoch,
                started_ns=self.started_ns,
                app_version=self.app_version,
                clock=self.market_clock,
                state=lambda: self._state,
                config=lambda: (self.config.config_version, self.config.config_hash),
                clock_version=lambda: self.market_clock.version,
                loop_lag_ms=self.loop_lag_ms,
                stream=lambda: self.outbox.stream if self.outbox is not None else None,
                fact_heads=self._core.heads,
                extras=self._extras_snapshot,
            )
        return self._heartbeat_builder.build()

    @property
    def heartbeat_topic(self) -> str:
        return sys_heartbeat(self.instance)

    @property
    def lv_cache(self) -> LastValueCache:
        return self._core.cache

    @property
    def publisher_core(self) -> PublisherCore:
        return self._core

    # --------------------------------------------------------- ROUTER
    def _whitelist(self) -> Whitelist | None:
        return Whitelist(internal=self.endpoints.instances()) if self._use_whitelist else None

    def _busy_code(self) -> BusyCode:
        return risk_busy_code if self.service == "risk-engine" else codes.BUSY

    def _pub_linger_ms(self) -> int:
        if self._pub_linger_override is not None:
            return self._pub_linger_override
        return 0 if self.service in _NO_FACT_PUBLISHERS else FACT_LINGER_MS

    def _control_handlers(self) -> dict[str, tuple[Handler, type[Msg]]]:
        ctl = ControlHandlers(
            instance=self.instance,
            clock=self.market_clock,
            state=lambda: self._state,
            status=self.build_heartbeat,
            lv_cache=lambda: self._core.cache,
            outbox=lambda: self.outbox,
            on_shutdown=self._on_shutdown_request,
            home_id=self._home_id,
        )
        handlers = ctl.handlers()
        handlers.update(self.config_participant.handlers())
        return handlers

    def _on_shutdown_request(self, req: ShutdownRequest) -> None:
        raise NotImplementedError

    # -------------------------------------------------------- databases
    def _create_database(
        self,
        name: str,
        metadata: MetaData,
        *,
        migrations: Sequence[Migration] | None,
        synchronous: Synchronous,
        path: str | Path | None,
    ) -> Database:
        if self.layout is None:
            raise RuntimeError("runtime not started")
        db_path = Path(path) if path is not None else self.layout.db_path(name)
        return Database(db_path, metadata, name=name, synchronous=synchronous, migrations=migrations, clock=self.market_clock)

    def _register_outbox(self, db: Database) -> Outbox:
        if self.outbox is not None:
            raise RuntimeError(f"{self.instance}: only one outbox database per service")
        ob = Outbox(db, instance=self.instance)
        self.outbox = ob
        self._core.set_heads(ob.heads())
        self._core.seed_facts(ob.last_rows())  # ctl.snapshot serves FACT topics right after a restart
        return ob

    def append_fact(
        self,
        conn: Connection,
        topic: str,
        body: Msg,
        *,
        td: str | None = None,
        corr_id: str | None = None,
    ) -> int:
        """Append a FACT to the outbox inside a writer job; published after the commit.

        Returns its ``topic_seq``. Use it inside ``db.write``/``db.submit``/``db.run_write``.
        """
        if self.outbox is None:
            raise RuntimeError(f"{self.instance}: no outbox database opened")
        return self.outbox.append_msg(
            conn, topic, body, epoch=self.epoch, created_ns=self.market_clock.now_ns(), td=td, corr_id=corr_id
        )

    def _idempotent_job(
        self,
        db: Database,
        command: str,
        requester: str,
        command_id: str,
        payload: Msg,
        handler: Callable[[Connection], Reply],
        *,
        trading_day: str,
        received_ns: int | None,
    ) -> Callable[[Connection], Reply]:
        reject_async(handler, "idempotent handler")
        pc = ProcessedCommands(db)
        rns = received_ns if received_ns is not None else self.market_clock.now_ns()

        def job(conn: Connection) -> Reply:
            return pc.run_once(
                conn,
                command_id=command_id,
                command=command,
                requester=requester,
                payload=payload,
                trading_day=trading_day,
                received_ns=rns,
                handler=handler,
            )

        return job

    def _close_databases(self, *, clean: bool = True) -> None:
        for db in reversed(self.databases):
            try:
                db.close(clean=clean)
            except Exception:
                logger.exception("db.close_failed", db=db.name)

    # --------------------------------------------------------- FACT
    @property
    def fact_tracker(self) -> FactTracker:
        """The runtime's FACT tracker (created by the first ``subscribe_fact``)."""
        if self._fact is None:
            raise RuntimeError("no FACT subscription yet")
        return self._fact

    def _dispatch_peer_change(self, peer: str, state: str) -> None:
        for cb in list(self._peer_callbacks):
            try:
                cb(peer, state)
            except Exception:
                logger.exception("peer.callback_failed", peer=peer)

    def _route_heartbeat(self, hb: Heartbeat) -> None:
        """Heartbeat from a watched peer: liveness, FACT tracker, Clone syncs, service observers.

        The heartbeat's ``epoch``/``stream`` tell the trackers which publisher
        incarnation is current (no UUIDv7 ordering, see ``bus/incarnation.py``).
        """
        self.peers.on_heartbeat(hb)
        if self._fact is not None and hb.instance in self._fact.publishers():
            self._fact.on_heartbeat(hb)
        for clone in self._clones:
            if clone.publisher == hb.instance:
                clone.on_heartbeat(hb)
        for handler in list(self._hb_handlers):
            handler(hb)


__all__ = ["API_GATEWAY", "SUPERVISOR", "RuntimeBase"]
