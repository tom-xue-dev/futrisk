"""trade-gateway service shell: sync runtime wiring and the business loop (01 §5.3.3, 02 §8.1, §8.6).

Threads (02 §8.1): the **business thread** runs :meth:`TradeGatewayService.run` and owns
every socket it polls (ROUTER, SUBs, DEALERs of the runtime and the broker DEALER of
the adapter); the runtime adds the publisher thread (PUB + heartbeats) and the td.db
writer thread. One loop iteration::

    poll(min(runtime, broker, core timers))
    pump_system()          sys.clock + heartbeats (risk-engine-0 liveness, FACT heads)
    pump_fact()            risk.control.firm (RULE kill switch) through the FactTracker
    pump_replies()         replies of our own requests (ctl.snapshot fallbacks)
    adapter.pump()         broker reports -> core (one td.db transaction each)
    pump_requests(256)     route.*, td.*, firm.kill_switch, ctl.*
    adapter.poll_timers()  login / liveness / reconnect / query timeouts
    core.on_timer()        resync retries, held-order sweep, td.status (1 Hz and on change)
    run_timers()           rpc timeouts, FACT repairs, peers, clock, config 2PC, offload completions

Commands (02 §6.4; the caller whitelist of 02 §4.11 is enforced by the runtime's ROUTER):
``route.order`` / ``route.cancel`` / ``route.resolve`` (``risk-engine-*``),
``td.query.positions`` / ``td.query.trades`` / ``td.query.orders`` / ``td.digest``,
``td.import_trades`` and ``firm.kill_switch`` (``api-gateway``). ``route.*`` answer
``UNAVAILABLE`` (retryable) until the market clock is synchronised (02 §8.6 step 3),
because route expiry is judged in market time.

Readiness (02 §8.6): ``STARTING`` until the broker is ``LOGGED_IN``, the login resync is
done and the kill switch state is loaded; then ``RUNNING``, or ``DEGRADED`` while the
broker dependency is unusable (not logged in, or a broker sequence reset detected, 02
§4.8). Short gap / tail resyncs keep ``RUNNING``: they show in the heartbeat ``detail``.

Maintenance (03 §8): on clearing-worker's ``sys.eod`` ``DONE`` (a plain subscription, no
cursor) td.db is checkpointed, backed up and purged on the offload pool
(:mod:`futrisk.services.trade_gateway.maintenance`). The parent-process watch
(``--parent-pid``) is the runtime's.
"""

from __future__ import annotations

import contextlib
from collections.abc import Callable
from typing import Any

import msgspec
import zmq
from loguru import logger
from sqlalchemy import Connection

from futrisk.bus.fact import CursorUpdate, FactKey
from futrisk.bus.rpc_common import DEFERRED, HandlerResult, RequestContext
from futrisk.db import Database, FactCursors
from futrisk.messages import codes, topics
from futrisk.messages.base import Msg, decode
from futrisk.messages.common import SnapshotBatch, SnapshotRequest
from futrisk.messages.enums import BrokerConnState, EodPhase, FactStartPolicy, ServiceState
from futrisk.messages.envelope import Envelope, Reply, decode_envelope, err_reply
from futrisk.messages.eod import EodPhaseEvent
from futrisk.messages.orders import (
    BrokerStatus,
    DigestRequest,
    OrderQuery,
    PositionQuery,
    RouteCancel,
    RouteOrder,
    RouteResolve,
    TradeImport,
    TradeQuery,
)
from futrisk.messages.risk import FirmControl, KillSwitchRequest
from futrisk.runtime import SyncServiceRuntime, SyncSubscription
from futrisk.services.trade_gateway.broker import BrokerAdapter, SimBrokerAdapter
from futrisk.services.trade_gateway.core import TradeGatewayCore
from futrisk.services.trade_gateway.maintenance import MaintenanceResult, run_maintenance
from futrisk.services.trade_gateway.schema import DB_NAME, METADATA, MIGRATIONS

_NS_PER_MS = 1_000_000
CLEARING = "clearing-worker"
SNAPSHOT_RETRY_MS = (500, 10_000)  # risk.control.firm snapshot fallback: first retry delay, cap
AdapterFactory = Callable[["TradeGatewayService"], BrokerAdapter]


class TradeGatewayService:
    """trade-gateway on a :class:`SyncServiceRuntime`; see the module docstring.

    Args:
        rt: the (not yet started) runtime of instance ``trade-gateway``.
        adapter_factory: builds the broker adapter (default: :class:`SimBrokerAdapter` on
            ``sim.yaml`` ``broker.endpoint``); called after the runtime started.
        broker_heartbeat_timeout_ms: broker silence that drops the session.
        broker_login_timeout_ms: time allowed to connect and log in.
        status_interval_ms: ``td.status`` period (it is also sent on every state change).
        maintenance: run the 03 §8 maintenance on ``sys.eod`` DONE.
    """

    def __init__(
        self,
        rt: SyncServiceRuntime,
        *,
        adapter_factory: AdapterFactory | None = None,
        broker_heartbeat_timeout_ms: int = 3_500,
        broker_login_timeout_ms: int = 3_000,
        status_interval_ms: int = 1_000,
        maintenance: bool = True,
    ) -> None:
        self.rt = rt
        self._adapter_factory = adapter_factory or _default_adapter
        self.broker_heartbeat_timeout_ms = broker_heartbeat_timeout_ms
        self.broker_login_timeout_ms = broker_login_timeout_ms
        self._status_interval_ms = status_interval_ms
        self._maintenance = maintenance
        self.db: Database | None = None
        self.cursors: FactCursors | None = None
        self.adapter: BrokerAdapter | None = None
        self.core: TradeGatewayCore | None = None
        self.coordinator = "risk-engine-0"
        self._adapter_started = False
        self._was_ready = False
        self._started = False
        self._eod_sub: SyncSubscription | None = None
        self._maintained: set[str] = set()
        self.last_maintenance: MaintenanceResult | None = None
        self._snapshot_retries: list[tuple[int, str, Callable[[], None], int]] = []  # (due mono ns, src, done, attempt)

    # ------------------------------------------------------------ lifecycle
    def start(self) -> None:
        """Start the runtime, open td.db, load state, register commands and subscriptions (02 §8.6)."""
        rt = self.rt
        if self._started:
            return
        self._started = True
        rt.start()
        self.db = rt.open_database(DB_NAME, METADATA, migrations=MIGRATIONS, synchronous="FULL")
        self.cursors = FactCursors(self.db)
        self.adapter = self._adapter_factory(self)
        self.core = TradeGatewayCore(
            db=self.db,
            adapter=self.adapter,
            clock=rt.market_clock,
            calendar=rt.config.calendar,
            append_fact=rt.append_fact,
            instance=rt.instance,
            publish_status=self._publish_status,
            status_interval_ms=self._status_interval_ms,
        )
        self.adapter.bind(self.core)
        self.core.load()
        self._register_handlers()
        self.coordinator = rt.config.services.coordinator()
        if rt.endpoints.has(self.coordinator) and rt.endpoints.instance(self.coordinator).pub:
            # FROM_BEGINNING: a fresh td.db replays the RULE kill switch history (7-day window) instead of
            # missing a switch that is already on; the apply is idempotent by business key
            rt.subscribe_fact(
                self.coordinator,
                [topics.RISK_CONTROL_FIRM],
                policy=FactStartPolicy.FROM_BEGINNING,
                apply=self._apply_firm_control,
                cursors=self.cursors.load_all(),
                on_expired=self._firm_control_fallback,
                on_stream_change=self._firm_stream_changed,
            )
            rt.watch_peers([self.coordinator])
        else:
            logger.warning("tg.no_coordinator", coordinator=self.coordinator,
                           detail="risk.control.firm not subscribed: only the manual kill switch applies")
        if self._maintenance and rt.endpoints.has(CLEARING) and rt.endpoints.instance(CLEARING).pub:
            # 02 §4.1: only to trigger the 03 §8 maintenance, so no cursor and no heartbeat tracking
            self._eod_sub = rt.subscribe(CLEARING, [topics.SYS_EOD], self._on_eod)
        rt.set_heartbeat_fields(detail=self.core.detail(), deps={"broker": "DOWN"})
        logger.info("tg.started", instance=rt.instance, broker=self.adapter.broker, coordinator=self.coordinator)

    def run(self, *, install_signals: bool = False) -> int:
        """Run until stopped (``ctl.shutdown``, signal, parent exit); returns the exit code."""
        rt = self.rt
        try:
            if install_signals:
                rt.install_signal_handlers()
            self.start()
            while rt.running:
                self.step()
        except Exception:
            logger.exception("tg.fatal")
            rt.request_stop("fatal error", exit_code=1)
        finally:
            self.stop()
        return rt.exit_code

    def stop(self) -> None:
        if self.adapter is not None and self._adapter_started:
            try:
                self.adapter.stop()
            except Exception:
                logger.exception("tg.adapter_stop_failed")
            self._adapter_started = False
        self.rt.stop()

    def step(self, max_wait_ms: int = 100) -> None:
        """One iteration of the business loop (02 §8.1)."""
        rt = self.rt
        adapter, core = self.adapter, self.core
        assert adapter is not None and core is not None
        timeout = rt.next_timer_ms(cap_ms=max_wait_ms)
        now = rt.market_clock.monotonic_ns()
        retry_at = min((r[0] for r in self._snapshot_retries), default=None)
        for t in (adapter.next_timer_ns() if self._adapter_started else None, core.next_timer_ns(), retry_at):
            if t is not None:
                timeout = min(timeout, max(0, (t - now + _NS_PER_MS - 1) // _NS_PER_MS))
        rt.poll(timeout)
        rt.pump_system()
        rt.pump_fact()
        if self._eod_sub is not None:
            self._eod_sub.pump()
        completions = rt.pump_replies()
        if self._adapter_started:
            adapter.pump()
        rt.pump_requests(limit=256)
        now = rt.market_clock.monotonic_ns()
        if not self._adapter_started and rt.clock_ready:
            adapter.start()
            self._adapter_started = True
        if self._adapter_started:
            adapter.poll_timers(now)
        core.on_timer(now)
        self._run_snapshot_retries(now)
        completions.extend(rt.run_timers())
        for c in completions:
            logger.debug("tg.completion", command=c.command, target=c.target, code=c.reply.code)
        self._update_state()
        rt.loop_done()

    # ------------------------------------------------------------ state, status, maintenance
    def _update_state(self) -> None:
        """STARTING until first ready (02 §8.6); then RUNNING, or DEGRADED while the broker is unusable (02 §4.8)."""
        core = self.core
        assert core is not None
        if self.rt.is_stopping:
            return
        if not self._was_ready:
            if core.ready:
                self.rt.set_state(ServiceState.RUNNING)
                self._was_ready = True
            return
        self.rt.set_state(ServiceState.RUNNING if core.healthy else ServiceState.DEGRADED)

    def _publish_status(self, status: BrokerStatus) -> None:
        """``td.status`` (LV) plus the heartbeat fields derived from it (02 §5, §7.9)."""
        core = self.core
        self.rt.publish(topics.TD_STATUS, status, td=status.trading_day)
        broker_up = status.state in (BrokerConnState.LOGGED_IN, BrokerConnState.RESYNCING)
        self.rt.set_heartbeat_fields(
            trading_day=status.trading_day,
            deps={"broker": "UP" if broker_up else "DOWN"},
            queues={"pending_orders": status.pending_orders},
            detail=core.detail() if core is not None else None,
        )

    def _on_eod(self, env: Envelope) -> None:
        """``sys.eod``: run the 03 §8 maintenance once per trading day on ``DONE`` (never raises)."""
        if env.type != "EodPhaseEvent":
            return
        try:
            ev = decode(env.payload, EodPhaseEvent)
        except (msgspec.DecodeError, msgspec.ValidationError):
            logger.warning("tg.bad_eod_event", src=env.src, seq=env.seq)
            return
        if ev.phase is not EodPhase.DONE or ev.trading_day in self._maintained or self.db is None:
            return
        self._maintained.add(ev.trading_day)
        rt, db = self.rt, self.db
        backup_dir = rt.layout.backup_dir if rt.layout is not None else None
        window = float(rt.config.services.bus.replay_window_days)
        wall_now = rt.market_clock.wall.now_ns()

        def job() -> MaintenanceResult:
            return run_maintenance(db, rt.outbox, trading_day=ev.trading_day, calendar=rt.config.calendar,
                                   backup_dir=backup_dir, replay_window_days=window, wall_now_ns=wall_now)

        def done(result: MaintenanceResult | None, error: BaseException | None) -> None:
            if error is not None:
                logger.opt(exception=error).error("tg.maintenance_failed", trading_day=ev.trading_day)
            self.last_maintenance = result

        logger.info("tg.maintenance_start", trading_day=ev.trading_day)
        rt.offload(job, done)

    # ------------------------------------------------------------ commands
    def _register_handlers(self) -> None:
        rt = self.rt
        rt.add_handler("route.order", self._route_order, payload_type=RouteOrder)
        rt.add_handler("route.cancel", self._route_cancel, payload_type=RouteCancel)
        rt.add_handler("route.resolve", self._route_resolve, payload_type=RouteResolve)
        rt.add_handler("td.query.positions", self._query_positions, payload_type=PositionQuery)
        rt.add_handler("td.query.trades", self._query_trades, payload_type=TradeQuery)
        rt.add_handler("td.query.orders", self._query_orders, payload_type=OrderQuery)
        rt.add_handler("td.digest", self._digest, payload_type=DigestRequest)
        rt.add_handler("td.import_trades", self._import_trades, payload_type=TradeImport)
        rt.add_handler("firm.kill_switch", self._kill_switch, payload_type=KillSwitchRequest)

    def _clock_gate(self) -> Reply | None:
        if not self.rt.clock_ready:
            return err_reply(codes.UNAVAILABLE, "market clock not synchronised yet (02 §8.6)")
        return None

    def _route_order(self, command: str, payload: Msg, ctx: RequestContext) -> HandlerResult:
        assert isinstance(payload, RouteOrder) and self.core is not None
        return self._clock_gate() or self.core.route_order(payload, ctx.requester)

    def _route_cancel(self, command: str, payload: Msg, ctx: RequestContext) -> HandlerResult:
        assert isinstance(payload, RouteCancel) and self.core is not None
        return self._clock_gate() or self.core.route_cancel(payload, ctx.requester)

    def _route_resolve(self, command: str, payload: Msg, ctx: RequestContext) -> HandlerResult:
        assert isinstance(payload, RouteResolve) and self.core is not None
        return self._clock_gate() or self.core.route_resolve(payload, ctx.requester)

    def _query_positions(self, command: str, payload: Msg, ctx: RequestContext) -> HandlerResult:
        assert isinstance(payload, PositionQuery) and self.core is not None
        reply = self.core.query_positions(payload, lambda r: self.rt.reply(ctx, r))
        return DEFERRED if reply is None else reply

    def _query_trades(self, command: str, payload: Msg, ctx: RequestContext) -> HandlerResult:
        assert isinstance(payload, TradeQuery) and self.core is not None
        return self.core.query_trades(payload)

    def _query_orders(self, command: str, payload: Msg, ctx: RequestContext) -> HandlerResult:
        assert isinstance(payload, OrderQuery) and self.core is not None
        return self.core.query_orders(payload)

    def _digest(self, command: str, payload: Msg, ctx: RequestContext) -> HandlerResult:
        assert isinstance(payload, DigestRequest) and self.core is not None
        return self.core.digest(payload)

    def _import_trades(self, command: str, payload: Msg, ctx: RequestContext) -> HandlerResult:
        assert isinstance(payload, TradeImport) and self.core is not None
        return self.core.import_trades(payload, ctx.requester)

    def _kill_switch(self, command: str, payload: Msg, ctx: RequestContext) -> HandlerResult:
        assert isinstance(payload, KillSwitchRequest) and self.core is not None
        return self.core.kill_switch(payload, ctx.requester)

    # ------------------------------------------------------------ risk.control.firm (FACT)
    def _apply_firm_control(self, env: Envelope, cur: CursorUpdate) -> None:
        """FactTracker ``apply``: effects and cursor in one transaction (02 §8.2, 03 §7 kill switch)."""
        assert self.core is not None and self.cursors is not None
        cursors = self.cursors
        control: FirmControl | None = None
        if env.type == "FirmControl":
            try:
                control = decode(env.payload, FirmControl)
            except (msgspec.DecodeError, msgspec.ValidationError):
                logger.error("tg.firm_control_undecodable", src=env.src, seq=env.seq)
        else:
            logger.warning("tg.firm_control_unexpected_type", type=env.type, src=env.src, seq=env.seq)
        now = self.rt.market_clock.now_ns()

        def save(conn: Connection) -> None:
            cur.save(conn, cursors, updated_ns=now)

        self.core.apply_firm_control(control, src=env.src, save_cursor=save)

    def _firm_control_fallback(self, key: FactKey, lo: int, hi: int, done: Callable[[], None]) -> None:
        """Replay window exceeded (02 §4.6): take the coordinator's latest ``risk.control.firm`` by snapshot."""
        logger.warning("tg.firm_control_replay_expired", src=key[0], lo=lo, hi=hi)
        self._snapshot_firm_control(key[0], done)

    def _firm_stream_changed(
        self, key: FactKey, old: str | None, new: str, next_seq: int, done: Callable[[], None]
    ) -> None:
        """The coordinator's database was recreated / restored (02 §8.2): reconcile by snapshot."""
        logger.warning("tg.firm_control_stream_changed", src=key[0], old=old, new=new, next_seq=next_seq)
        self._snapshot_firm_control(key[0], done)

    def _snapshot_firm_control(self, src: str, done: Callable[[], None], attempt: int = 1) -> None:
        """``ctl.snapshot`` of ``risk.control.firm``, applied without a cursor; ``done()`` releases the FACT key.

        A failed request is retried with backoff (``SNAPSHOT_RETRY_MS``) while the tracker keeps
        holding (buffering) the key: the RULE kill switch state must not be skipped. Facts are
        only published while the coordinator is up, and then its snapshot answers.
        """

        def on_reply(reply: Reply) -> None:
            if not reply.ok:
                first, cap = SNAPSHOT_RETRY_MS
                delay_ms = min(cap, first << min(attempt - 1, 10))
                logger.warning("tg.firm_control_snapshot_failed", code=reply.code, message=reply.message, attempt=attempt,
                               retry_in_ms=delay_ms)
                due = self.rt.market_clock.monotonic_ns() + delay_ms * _NS_PER_MS
                self._snapshot_retries.append((due, src, done, attempt + 1))
                return
            try:
                batch = reply.open_as(SnapshotBatch)
                for item in batch.items if batch is not None else []:
                    if item.topic != topics.RISK_CONTROL_FIRM:
                        continue
                    env = decode_envelope(item.envelope)
                    if env.type == "FirmControl" and self.core is not None:
                        self.core.apply_firm_control(decode(env.payload, FirmControl), src=src, save_cursor=None)
            except Exception:
                logger.exception("tg.firm_control_snapshot_apply_failed")
            finally:
                done()

        self.rt.request(src, "ctl.snapshot", SnapshotRequest(prefixes=[topics.RISK_CONTROL_FIRM]), on_reply=on_reply,
                        retries=1)

    def _run_snapshot_retries(self, now_mono: int) -> None:
        if not self._snapshot_retries:
            return
        due = [r for r in self._snapshot_retries if r[0] <= now_mono]
        if not due:
            return
        self._snapshot_retries = [r for r in self._snapshot_retries if r[0] > now_mono]
        for _, src, done, attempt in due:
            self._snapshot_firm_control(src, done, attempt)


def _default_adapter(svc: TradeGatewayService) -> BrokerAdapter:
    """``SimBrokerAdapter`` on the configured SimBroker endpoint; its socket joins the runtime poller."""
    rt = svc.rt

    def on_socket(sock: zmq.Socket[Any], created: bool) -> None:
        if created:
            rt.poller.register(sock, zmq.POLLIN)
        else:
            with contextlib.suppress(KeyError):
                rt.poller.unregister(sock)

    return SimBrokerAdapter(
        rt.config.sim.broker.endpoint,
        identity=rt.instance,
        clock=rt.market_clock,
        heartbeat_timeout_ms=svc.broker_heartbeat_timeout_ms,
        login_timeout_ms=svc.broker_login_timeout_ms,
        on_socket=on_socket,
    )


__all__ = ["AdapterFactory", "TradeGatewayService"]
