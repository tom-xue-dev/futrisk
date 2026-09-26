"""Business core of trade-gateway: routing, execution reports, resync, cancels, kill switch (01 §5.3.3, 02 §6.4, §8.3).

The core is transport-free and single-threaded: the service shell
(:mod:`futrisk.services.trade_gateway.service`) calls it from the business thread of
the sync runtime; unit tests drive it with a fake broker adapter.

Routing (02 §6.4 ``route.order``), checked in this exact order inside one transaction:

1. a ``routed_orders`` row exists -> its status is returned (``duplicate=true``), the
   order is never submitted twice;
2. a tombstone exists -> ``ROUTE_LOST`` (no row written);
3. ``now > expire_ns`` (market time) -> ``ROUTE_EXPIRED`` + ``EXPIRED`` row;
4. kill switch effective (manual OR rule) and ``offset=OPEN`` -> ``ROUTE_KILL_SWITCH`` +
   ``EXPIRED`` row;

otherwise the row is persisted first (``synchronous=FULL``) and then:

* broker ``LOGGED_IN`` and in sync -> sent, ``RouteAck(SUBMITTED, broker_order_ref)``;
* broker ``LOGGED_IN`` but resyncing -> held as ``RECEIVED`` (``RouteAck(RECEIVED)``) and
  sent when the resync completes - but never after ``expire_ns`` (then ``EXPIRED`` /
  ``ROUTE_EXPIRED``) nor as an open while the kill switch is on (``ROUTE_KILL_SWITCH``);
  ``route.cancel`` cancels a held order locally;
* broker down, or a broker sequence reset detected -> ``REJECTED`` row,
  ``REJECT_GATEWAY_UNAVAILABLE``.

Routed-order statuses owned by trade-gateway: ``RECEIVED`` (held, never sent),
``SUBMITTED`` (sent on the current session, no broker report yet), ``UNKNOWN`` (sent on a
lost session / before a restart, fate unknown until the next resync), ``CANCEL_PENDING``
(cancel requested). Every other status comes from broker reports.

Cancels (02 §6.4 ``route.cancel``, §8.3 熔断撤单) are **durable**: an accepted
``route.cancel`` stamps ``routed_orders.cancel_requested_ns`` in the same writer job as
its ``processed_commands`` record and answers ``RouteAck(CANCEL_PENDING)`` whether or not
the broker is reachable. The cancel is sent at once when possible and otherwise (broker
down, resync running, broker reference not known yet, session lost with the cancel in
flight) re-sent after the next resync or on the order's next broker report, until the
order is finished. Only a genuine refusal (``BROKER_ORDER_FINISHED`` /
``BROKER_ORDER_NOT_FOUND`` for an order the broker had reported) ends the request with a
restating ``BrokerOrderUpdate`` (``broker_seq`` 0). An ``UNKNOWN`` order with a pending
cancel that the broker never reported is closed as ``CANCELED``.

Execution reports (02 §5, §7.4-§7.5, 03 §7 柜台回报): each report is ONE transaction
writing ``broker_order_events`` (dedupe ``(trading_day, broker_seq)``), the
``routed_orders`` update, ``trades`` (dedupe by trade key, ``trade_seq`` autoincrement),
``broker_cursors`` and the ``td.exec.{account_id}`` outbox row (``BrokerOrderUpdate`` or
``Trade``), published after the commit. An order report older than the newest event
already stored for the same order is ignored (status monotonicity).

Resync (02 §9.4 补回报): after every login (for every trading day from the last one seen
to the login day), on a live ``broker_seq`` gap, on a tail gap revealed by
``Heartbeat.broker_seq_head`` and on a trading-day roll. Per trading day the core asks
for orders first and trades second from ``last_broker_seq + 1``: the broker serves
them in order, so every trade up to the second snapshot is in the trade answer and
every order event after the first snapshot was pushed live. Buffered live reports up
to the highest queried ``broker_seq`` are merged with the answers after checking that
nothing between the order snapshot and that seq is missing (a missing seq means a lost
live frame: query again from it); the cursor becomes the highest queried seq, and the
buffered reports above it go through the normal ``seq == last + 1`` path, so a gap there
starts another resync. Then pending orders are reconciled: held ``RECEIVED`` orders are
sent (or expired), ``UNKNOWN`` orders the broker never reported are closed
(``REJECTED`` / ``BROKER_DISCONNECTED``, or ``CANCELED`` when a cancel was pending), and
pending cancels are re-sent.
"""

from __future__ import annotations

import dataclasses
import math
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import msgspec
from loguru import logger
from sqlalchemy import Connection

from futrisk.common.calendar import TradingCalendar
from futrisk.common.clock import Clock, ns_to_bj, parse_td, td_str
from futrisk.db import Database, ProcessedCommands
from futrisk.messages import codes, topics
from futrisk.messages.base import Msg
from futrisk.messages.enums import (
    BrokerConnState,
    BrokerOrderStatus,
    ControlOrigin,
    FirmAction,
    Offset,
    OrderType,
    Role,
    TimeInForce,
    TradeOrigin,
)
from futrisk.messages.envelope import Reply, err_reply, ok_reply
from futrisk.messages.orders import (
    BrokerOrderBatch,
    BrokerOrderUpdate,
    BrokerPosition,
    BrokerPositionBatch,
    BrokerStatus,
    DigestRequest,
    OrderQuery,
    PositionQuery,
    RouteAck,
    RouteCancel,
    RouteOrder,
    RouteResolve,
    Trade,
    TradeBatch,
    TradeDigest,
    TradeImport,
    TradeImportAck,
    TradeQuery,
    compute_account_digest,
)
from futrisk.messages.risk import FirmControl, KillSwitchRequest
from futrisk.services.trade_gateway import store
from futrisk.services.trade_gateway.broker.base import (
    ActionError,
    BrokerAdapter,
    BrokerReport,
    BrokerSession,
    BrokerUnavailable,
    InsertError,
    InvalidReport,
    OrderReport,
    PositionReport,
    QueryResult,
    TradeReport,
    parse_broker_order_ref,
)
from futrisk.services.trade_gateway.store import RoutedOrder

FactAppender = Callable[..., int]
"""``append_fact(conn, topic, body, *, td=None, corr_id=None) -> topic_seq`` (``rt.append_fact``)."""

_NS_PER_MS = 1_000_000
BROKER_NOT_LOGGED_IN = "BROKER_NOT_LOGGED_IN"
#: cancel refusals that only mean "not now": the request stays pending and is re-sent after the next resync
TRANSIENT_CANCEL_ERRORS = frozenset({BROKER_NOT_LOGGED_IN, codes.BROKER_DISCONNECTED})
#: cancel refusals that end the request (02 §11.3)
FINAL_CANCEL_ERRORS = frozenset({codes.BROKER_ORDER_FINISHED, codes.BROKER_ORDER_NOT_FOUND})
#: statuses whose pending cancel can be sent to the broker (UNKNOWN ones wait for the resync reconcile)
_CANCEL_SENDABLE = (
    BrokerOrderStatus.SUBMITTED,
    BrokerOrderStatus.ACCEPTED,
    BrokerOrderStatus.PARTIALLY_FILLED,
    BrokerOrderStatus.CANCEL_PENDING,
)
_KILL_ROLES = frozenset({Role.RISK_MANAGER, Role.ADMIN})
MAX_RESYNC_BUFFER = 50_000
MAX_CATCHUP_DAYS = 30

CMD_ROUTE_ORDER = "route.order"
CMD_ROUTE_CANCEL = "route.cancel"
CMD_ROUTE_RESOLVE = "route.resolve"
CMD_IMPORT = "td.import_trades"
CMD_KILL_SWITCH = "firm.kill_switch"


@dataclass
class KillSwitch:
    """trade-gateway's copy of the kill switch (03 §4.2 ``firm_controls``): effective = manual OR rule."""

    manual: bool = False
    rule: bool = False
    loaded: bool = False

    @property
    def effective(self) -> bool:
        return self.manual or self.rule

    def detail(self) -> dict[str, bool]:
        return {"manual": self.manual, "rule": self.rule, "effective": self.effective}


@dataclass
class CoreStats:
    routes: int = 0
    submitted: int = 0
    held: int = 0
    rejected_expired: int = 0
    rejected_kill_switch: int = 0
    rejected_lost: int = 0
    gateway_unavailable: int = 0
    duplicates: int = 0
    tombstones: int = 0
    order_reports: int = 0
    trade_reports: int = 0
    dup_orders: int = 0
    dup_trades: int = 0
    stale_orders: int = 0
    dup_live: int = 0
    invalid_reports: int = 0
    resyncs: int = 0
    gap_resyncs: int = 0
    tail_resyncs: int = 0
    lost_frames: int = 0
    buffer_overflows: int = 0
    local_updates: int = 0
    cancels_sent: int = 0
    cancels_deferred: int = 0  # refused with BROKER_NOT_LOGGED_IN / BROKER_DISCONNECTED: re-sent later
    imported: int = 0
    reconstructed_orders: int = 0


@dataclass
class _Resync:
    gen: int
    reason: str
    steps: list[tuple[str, int]]
    attempt: int = 0
    orders: QueryResult[OrderReport] | None = None
    trades: QueryResult[TradeReport] | None = None
    failed: bool = False
    retry_at: int | None = None
    buffer: list[BrokerReport] = field(default_factory=list)


def _validate_route(req: RouteOrder) -> str | None:
    if not req.client_order_id:
        return "client_order_id is required"
    if not req.account_id or not req.instrument_id:
        return "account_id and instrument_id are required"
    if req.qty <= 0:
        return "qty must be positive"
    if req.offset is Offset.FORCE_CLOSE:
        return "FORCE_CLOSE is broker-only and cannot be routed (02 §7.3)"
    if req.order_type is OrderType.LIMIT and (req.price is None or not math.isfinite(req.price) or req.price <= 0):
        return "LIMIT orders need a positive price"
    if len(req.trading_day) != 8 or not req.trading_day.isdigit():
        return "trading_day must be YYYYMMDD"
    return None


def _broker_code(r: OrderReport) -> tuple[str | None, str | None]:
    """``(broker_code, broker_msg)`` of an order report: rejects and cancels carry a reason code."""
    msg = r.status_msg or None
    if r.status in (BrokerOrderStatus.REJECTED, BrokerOrderStatus.CANCELED) and msg:
        return msg, msg
    return None, msg


def _report_key(r: BrokerReport) -> tuple[str, int]:
    return (r.trading_day or "", r.broker_seq if r.broker_seq is not None else -1)


def _has_broker_ref(row: RoutedOrder) -> bool:
    """True when a cancel can address the order (``order_sys_id`` or a ``front:session:ref``)."""
    return bool(row.order_sys_id) or (row.broker_order_ref is not None and parse_broker_order_ref(row.broker_order_ref) is not None)


class TradeGatewayCore:
    """See the module docstring. All methods run on the business thread."""

    def __init__(
        self,
        *,
        db: Database,
        adapter: BrokerAdapter,
        clock: Clock,
        calendar: TradingCalendar,
        append_fact: FactAppender,
        instance: str = "trade-gateway",
        publish_status: Callable[[BrokerStatus], None] | None = None,
        status_interval_ms: int = 1_000,
        tail_check_interval_ms: int = 2_000,
        resync_retry_ms: int = 1_000,
        apply_chunk: int = 200,
        max_resync_buffer: int = MAX_RESYNC_BUFFER,
    ) -> None:
        self._db = db
        self._adapter = adapter
        self._clock = clock
        self._calendar = calendar
        self._append_fact = append_fact
        self._instance = instance
        self._publish_status_cb = publish_status
        self._broker = adapter.broker
        self._processed = ProcessedCommands(db)
        self._status_interval_ns = status_interval_ms * _NS_PER_MS
        self._tail_interval_ns = tail_check_interval_ms * _NS_PER_MS
        self._resync_retry_ns = resync_retry_ms * _NS_PER_MS
        self._chunk = max(1, apply_chunk)
        self._max_buffer = max(1, max_resync_buffer)
        self.kill = KillSwitch()
        self.stats = CoreStats()
        self._cursors: dict[str, int] = {}
        self._current_td: str | None = None
        self._session: BrokerSession | None = None
        self._resync: _Resync | None = None
        self._resync_gen = 0
        self._synced_once = False
        self._seq_conflict_td: str | None = None
        self._held: dict[str, int] = {}
        self._cancel_inflight: set[str] = set()
        self._cancel_due: set[str] = set()
        self._deferred_errors: list[InsertError | ActionError] = []
        self._next_tail_check = 0
        self._next_status = 0
        self._next_held_sweep = 0
        self._status_dirty = True
        self._state_since = clock.now_ns()
        self._last_state: BrokerConnState | None = None
        self.last_status: BrokerStatus | None = None

    # ------------------------------------------------------------ lifecycle
    def load(self) -> None:
        """Load the kill switch and cursors; orders left in flight by a previous run become ``UNKNOWN``."""
        now = self._now()

        def tx(conn: Connection) -> tuple[dict[ControlOrigin, bool], dict[str, int], list[str]]:
            ks = store.load_kill_switch(conn)
            cursors = store.load_cursors(conn, self._broker)
            unknown = store.mark_unknown(conn, now, include_received=True)
            return ks, cursors, unknown

        ks, cursors, unknown = self._db.write(tx)
        self.kill.manual = ks[ControlOrigin.MANUAL]
        self.kill.rule = ks[ControlOrigin.RULE]
        self.kill.loaded = True
        self._cursors = dict(cursors)
        self._current_td = max(cursors) if cursors else None
        logger.info("tg.loaded", kill_switch=self.kill.detail(), cursors=self._cursors, unknown_orders=len(unknown))

    # ------------------------------------------------------------ properties
    @property
    def resyncing(self) -> bool:
        return self._resync is not None

    @property
    def synced_once(self) -> bool:
        return self._synced_once

    @property
    def seq_conflict(self) -> str | None:
        """Trading day on which the broker's ``broker_seq`` was found behind our cursor (sim.db reset)."""
        return self._seq_conflict_td

    @property
    def healthy(self) -> bool:
        """The broker dependency is usable: ``LOGGED_IN``, kill switch loaded, no sequence reset (02 §4.8)."""
        return (
            self._adapter.state is BrokerConnState.LOGGED_IN
            and self.kill.loaded
            and self._seq_conflict_td is None
        )

    @property
    def ready(self) -> bool:
        """02 §8.6 start-up readiness: healthy and the login resync completed."""
        return self.healthy and self._synced_once and self._resync is None

    @property
    def current_trading_day(self) -> str | None:
        return self._current_td

    def cursor(self, trading_day: str) -> int:
        return self._cursors.get(trading_day, 0)

    def published_state(self) -> BrokerConnState:
        state = self._adapter.state
        if state is BrokerConnState.LOGGED_IN and self._resync is not None:
            return BrokerConnState.RESYNCING
        return state

    def trading_day(self) -> str:
        """Current trading day by the exchange calendar at market time."""
        now = self._now()
        try:
            return td_str(self._calendar.current_or_next_trading_day(now))
        except LookupError:
            if self._current_td is not None:
                return self._current_td
            return ns_to_bj(now).strftime("%Y%m%d")

    def detail(self) -> dict[str, Any]:
        """Heartbeat ``detail`` block (a running resync is visible here, the service stays RUNNING)."""
        s = self._session
        rs = self._resync
        return {
            "broker": self._broker,
            "broker_state": self.published_state().value,
            "session": {"front_id": s.front_id, "session_id": s.session_id, "trading_day": s.trading_day} if s else None,
            "trading_day": self._current_td,
            "cursors": dict(self._cursors),
            "resync": {"reason": rs.reason, "steps": [list(x) for x in rs.steps], "buffered": len(rs.buffer)} if rs else None,
            "broker_seq_conflict": self._seq_conflict_td,
            "kill_switch": self.kill.detail(),
            "held_orders": len(self._held),
            "cancels_in_flight": len(self._cancel_inflight),
            "stats": dataclasses.asdict(self.stats),
        }

    def _now(self) -> int:
        return self._clock.now_ns()

    def _mono(self) -> int:
        return self._clock.monotonic_ns()

    def _append(self, conn: Connection, account_id: str, body: Msg, *, td: str | None, corr_id: str | None) -> int:
        return int(self._append_fact(conn, topics.td_exec(account_id), body, td=td, corr_id=corr_id))

    def _can_send(self) -> bool:
        """Orders and cancels may go to the broker right now."""
        return self._adapter.state is BrokerConnState.LOGGED_IN and self._resync is None

    # ------------------------------------------------------------ replies
    @staticmethod
    def _ack_reply(row: RoutedOrder, *, duplicate: bool = False) -> Reply:
        """Reply stating an existing routed order's status (02 §7.4 ``RouteAck``)."""
        ack = RouteAck(client_order_id=row.client_order_id, status=row.status, broker_order_ref=row.broker_order_ref)
        if row.status is BrokerOrderStatus.EXPIRED:
            reply = err_reply(row.broker_code or codes.ROUTE_EXPIRED, row.broker_msg, ack)
        elif row.status is BrokerOrderStatus.REJECTED:
            reply = err_reply(row.broker_code or codes.REJECT_GATEWAY_UNAVAILABLE, row.broker_msg, ack)
        else:
            reply = ok_reply(ack)
        return msgspec.structs.replace(reply, duplicate=True) if duplicate else reply

    @staticmethod
    def _lost_reply(client_order_id: str, message: str) -> Reply:
        return err_reply(codes.ROUTE_LOST, message, RouteAck(client_order_id=client_order_id, status=BrokerOrderStatus.EXPIRED))

    # ------------------------------------------------------------ route.order
    def route_order(self, req: RouteOrder, requester: str) -> Reply:
        """02 §6.4 ``route.order``; see the module docstring for the check order."""
        problem = _validate_route(req)
        if problem is not None:
            return err_reply(codes.BAD_REQUEST, problem)
        if req.src_instance != requester:
            logger.warning("tg.route_src_mismatch", src_instance=req.src_instance, requester=requester,
                           client_order_id=req.client_order_id)
        self.stats.routes += 1
        now = self._now()
        coid = req.client_order_id
        kill = self.kill.effective
        state = self._adapter.state
        usable = state is BrokerConnState.LOGGED_IN and self._seq_conflict_td is None
        hold = usable and self._resync is not None

        def tx(conn: Connection) -> tuple[Reply | None, RoutedOrder | None, str]:
            existing = store.get_routed(conn, coid)
            if existing is not None:
                return self._ack_reply(existing, duplicate=True), None, "duplicate"
            if store.has_tombstone(conn, coid):
                return self._lost_reply(coid, "order declared lost earlier (tombstone)"), None, "lost"
            if now > req.expire_ns:
                msg = f"route arrived {(now - req.expire_ns) / _NS_PER_MS:.1f} ms after expire_ns"
                store.insert_routed(conn, req, broker=self._broker, status=BrokerOrderStatus.EXPIRED, now_ns=now,
                                    broker_code=codes.ROUTE_EXPIRED, broker_msg=msg)
                return (err_reply(codes.ROUTE_EXPIRED, msg, RouteAck(client_order_id=coid, status=BrokerOrderStatus.EXPIRED)),
                        None, "expired")
            if kill and req.offset is Offset.OPEN:
                msg = f"kill switch active {self.kill.detail()}"
                store.insert_routed(conn, req, broker=self._broker, status=BrokerOrderStatus.EXPIRED, now_ns=now,
                                    broker_code=codes.ROUTE_KILL_SWITCH, broker_msg=msg)
                return (err_reply(codes.ROUTE_KILL_SWITCH, msg, RouteAck(client_order_id=coid, status=BrokerOrderStatus.EXPIRED)),
                        None, "kill_switch")
            if not usable:
                msg = (f"broker sequence reset detected on {self._seq_conflict_td}" if self._seq_conflict_td is not None
                       else f"broker {state.value}")
                store.insert_routed(conn, req, broker=self._broker, status=BrokerOrderStatus.REJECTED, now_ns=now,
                                    broker_code=codes.REJECT_GATEWAY_UNAVAILABLE, broker_msg=msg)
                return (err_reply(codes.REJECT_GATEWAY_UNAVAILABLE, msg,
                                  RouteAck(client_order_id=coid, status=BrokerOrderStatus.REJECTED)), None, "unavailable")
            row = store.insert_routed(conn, req, broker=self._broker, status=BrokerOrderStatus.RECEIVED, now_ns=now)
            return None, row, "received"

        reply, row, outcome = self._db.write(tx)
        if reply is not None:
            self._count_outcome(outcome)
            logger.info("tg.route", client_order_id=coid, account_id=req.account_id, outcome=outcome, code=reply.code)
            return reply
        assert row is not None
        if hold:
            self._held[coid] = req.expire_ns
            self.stats.held += 1
            self._status_dirty = True
            logger.info("tg.route", client_order_id=coid, account_id=req.account_id, outcome="held_during_resync")
            return ok_reply(RouteAck(client_order_id=coid, status=BrokerOrderStatus.RECEIVED))
        reply = self._submit(row, held=False)
        logger.info("tg.route", client_order_id=coid, account_id=req.account_id, outcome="submitted" if reply.ok else "refused",
                    code=reply.code)
        return reply

    def _count_outcome(self, outcome: str) -> None:
        if outcome == "duplicate":
            self.stats.duplicates += 1
        elif outcome == "lost":
            self.stats.rejected_lost += 1
        elif outcome == "expired":
            self.stats.rejected_expired += 1
        elif outcome == "kill_switch":
            self.stats.rejected_kill_switch += 1
        elif outcome == "unavailable":
            self.stats.gateway_unavailable += 1

    def _submit(self, row: RoutedOrder, *, held: bool) -> Reply:
        """Send a persisted ``RECEIVED`` order to the broker and record the outcome.

        Never after ``expire_ns`` (02 §6.4, §7.4): an order still waiting then is closed
        ``EXPIRED`` / ``ROUTE_EXPIRED``; a held open is also re-checked against the kill
        switch. A held order already acknowledged as ``RECEIVED`` gets its refusal
        published on td.exec (``held=True``); a direct route learns it from the reply.
        """
        coid = row.client_order_id
        now = self._now()
        if now > row.expire_ns:
            self.stats.rejected_expired += 1
            return self._refuse(row, BrokerOrderStatus.EXPIRED, codes.ROUTE_EXPIRED,
                                f"not sent: expire_ns passed {(now - row.expire_ns) / _NS_PER_MS:.1f} ms ago", publish=held)
        if held and self.kill.effective and row.offset is Offset.OPEN:
            self.stats.rejected_kill_switch += 1
            return self._refuse(row, BrokerOrderStatus.EXPIRED, codes.ROUTE_KILL_SWITCH,
                                "kill switch turned on while the order was held", publish=True)
        if self._seq_conflict_td is not None:  # held before the reset was detected: its reports could not be sequenced
            self.stats.gateway_unavailable += 1
            return self._refuse(row, BrokerOrderStatus.REJECTED, codes.REJECT_GATEWAY_UNAVAILABLE,
                                f"broker sequence reset detected on {self._seq_conflict_td}", publish=held)
        try:
            ref = self._adapter.insert_order(row.to_route_order())
        except BrokerUnavailable as exc:
            self.stats.gateway_unavailable += 1
            return self._refuse(row, BrokerOrderStatus.REJECTED, codes.REJECT_GATEWAY_UNAVAILABLE, str(exc), publish=held)
        now = self._now()
        self._db.write(lambda c: store.update_routed(c, coid, status=BrokerOrderStatus.SUBMITTED, broker_order_ref=ref,
                                                     submitted_ns=now, updated_ns=now))
        self.stats.submitted += 1
        return ok_reply(RouteAck(client_order_id=coid, status=BrokerOrderStatus.SUBMITTED, broker_order_ref=ref))

    def _refuse(self, row: RoutedOrder, status: BrokerOrderStatus, code: str, msg: str, *, publish: bool) -> Reply:
        """Close a ``RECEIVED`` order that could not be sent (optionally published, ``broker_seq`` 0)."""
        if publish:
            self._close_local(row, status, code, msg)
        else:
            now = self._now()
            self._db.write(lambda c: store.update_routed(c, row.client_order_id, status=status, broker_code=code,
                                                         broker_msg=msg, updated_ns=now))
        self._held.pop(row.client_order_id, None)
        return err_reply(code, msg, RouteAck(client_order_id=row.client_order_id, status=status))

    # ------------------------------------------------------------ route.cancel
    def route_cancel(self, req: RouteCancel, requester: str) -> Reply:
        """02 §6.4 ``route.cancel``: idempotent by ``command_id``, durable until the broker answers.

        * never received -> tombstone, ``ROUTE_LOST`` / ``EXPIRED``;
        * finished -> the current status;
        * held (``RECEIVED``, never sent) -> cancelled locally, published on td.exec;
        * otherwise -> ``cancel_requested_ns`` stamped, ``RouteAck(CANCEL_PENDING)``; the
          cancel is sent now when the broker can take it, else queued (see the module
          docstring). The reply never depends on the broker being reachable.
        """
        if not req.command_id or not req.client_order_id:
            return err_reply(codes.BAD_REQUEST, "command_id and client_order_id are required")
        td = self.trading_day()
        now = self._now()
        coid = req.client_order_id
        outcome: dict[str, Any] = {}

        def handler(conn: Connection) -> Reply:
            row = store.get_routed(conn, coid)
            if row is None:
                if not store.has_tombstone(conn, coid):
                    store.insert_tombstone(conn, coid, trading_day=td, command=CMD_ROUTE_CANCEL, command_id=req.command_id,
                                           requested_by=requester, now_ns=now)
                    outcome["tombstone"] = True
                return self._lost_reply(coid, "order never received: tombstone written")
            if row.is_terminal:
                return self._ack_reply(row)
            if row.status is BrokerOrderStatus.RECEIVED:
                msg = f"canceled before submission: {req.reason or 'route.cancel'}"
                store.update_routed(conn, coid, status=BrokerOrderStatus.CANCELED, broker_msg=msg, cancel_requested_ns=now,
                                    updated_ns=now)
                self._append(conn, row.account_id, row.update_msg(status=BrokerOrderStatus.CANCELED, broker_msg=msg,
                             updated_ns=now), td=row.trading_day, corr_id=coid)
                outcome["local"] = True
                return ok_reply(RouteAck(client_order_id=coid, status=BrokerOrderStatus.CANCELED,
                                         broker_order_ref=row.broker_order_ref))
            values: dict[str, Any] = {"cancel_requested_ns": row.cancel_requested_ns or now, "updated_ns": now}
            if row.status is not BrokerOrderStatus.UNKNOWN:  # UNKNOWN rows keep their marker for the reconcile
                values["status"] = BrokerOrderStatus.CANCEL_PENDING
            store.update_routed(conn, coid, **values)
            outcome["queued"] = dataclasses.replace(row, status=values.get("status", row.status),
                                                    cancel_requested_ns=values["cancel_requested_ns"])
            return ok_reply(RouteAck(client_order_id=coid, status=BrokerOrderStatus.CANCEL_PENDING,
                                     broker_order_ref=row.broker_order_ref))

        reply = self._db.write(lambda c: self._processed.run_once(
            c, command_id=req.command_id, command=CMD_ROUTE_CANCEL, requester=requester, payload=req, trading_day=td,
            received_ns=now, handler=handler))
        if outcome.get("tombstone"):
            self.stats.tombstones += 1
        if outcome.get("local"):
            self.stats.local_updates += 1
            if self._held.pop(coid, None) is not None:
                self._status_dirty = True
        queued: RoutedOrder | None = outcome.get("queued")
        sent = False
        if queued is not None and self._can_send() and queued.status in _CANCEL_SENDABLE and _has_broker_ref(queued):
            sent = self._send_cancel(queued)
        logger.info("tg.cancel", client_order_id=coid, command_id=req.command_id, code=reply.code, duplicate=reply.duplicate,
                    sent_to_broker=sent, queued=queued is not None and not sent)
        return reply

    def _send_cancel(self, row: RoutedOrder) -> bool:
        """Send one pending cancel (at most one in flight per order and session)."""
        coid = row.client_order_id
        if coid in self._cancel_inflight:
            return True
        try:
            self._adapter.cancel(coid, broker_order_ref=row.broker_order_ref, order_sys_id=row.order_sys_id)
        except BrokerUnavailable as exc:
            logger.info("tg.cancel_queued", client_order_id=coid, reason=str(exc))
            return False
        except ValueError as exc:  # no usable broker reference yet
            logger.warning("tg.cancel_queued", client_order_id=coid, reason=str(exc))
            return False
        self._cancel_inflight.add(coid)
        self.stats.cancels_sent += 1
        return True

    def _send_pending_cancels(self, only: set[str] | None = None) -> None:
        """(Re-)send every pending cancel the broker can address (after a resync, or on new reports)."""
        if not self._can_send():
            return
        rows = self._db.read(store.pending_cancels)
        due = [
            r for r in rows
            if (only is None or r.client_order_id in only)
            and r.status in _CANCEL_SENDABLE
            and r.client_order_id not in self._cancel_inflight
            and _has_broker_ref(r)
        ]
        if not due:
            return
        now = self._now()
        flip = [r.client_order_id for r in due if r.status is not BrokerOrderStatus.CANCEL_PENDING]
        if flip:

            def mark(conn: Connection) -> None:
                for coid in flip:
                    store.update_routed(conn, coid, status=BrokerOrderStatus.CANCEL_PENDING, updated_ns=now)

            self._db.write(mark)
        for r in due:
            self._send_cancel(r)
        logger.info("tg.cancels_resent", count=len(due))

    # ------------------------------------------------------------ route.resolve
    def route_resolve(self, req: RouteResolve, requester: str) -> Reply:
        """02 §6.4 ``route.resolve``: current status, or a tombstone for a never-received order."""
        if not req.client_order_id:
            return err_reply(codes.BAD_REQUEST, "client_order_id is required")
        now = self._now()
        td = req.trading_day if len(req.trading_day) == 8 and req.trading_day.isdigit() else self.trading_day()
        coid = req.client_order_id

        def tx(conn: Connection) -> tuple[Reply, bool]:
            row = store.get_routed(conn, coid)
            if row is not None:
                return self._ack_reply(row), False
            if store.has_tombstone(conn, coid):
                return self._lost_reply(coid, "order declared lost earlier (tombstone)"), False
            store.insert_tombstone(conn, coid, trading_day=td, command=CMD_ROUTE_RESOLVE, command_id=None,
                                   requested_by=requester, now_ns=now)
            return self._lost_reply(coid, "order never received: tombstone written"), True

        reply, written = self._db.write(tx)
        if written:
            self.stats.tombstones += 1
        logger.info("tg.resolve", client_order_id=coid, code=reply.code)
        return reply

    # ------------------------------------------------------------ queries
    def query_positions(self, req: PositionQuery, deliver: Callable[[Reply], None]) -> Reply | None:
        """``td.query.positions``: broker positions; ``None`` means the reply is delivered later.

        ``UNAVAILABLE`` while the broker is down or a resync is running: the broker's
        view may then include fills not yet published on td.exec (02 §8.7 compares it
        with the applied fills).
        """
        if self._adapter.state is not BrokerConnState.LOGGED_IN:
            return err_reply(codes.UNAVAILABLE, f"broker {self._adapter.state.value}")
        if self._resync is not None or not self._synced_once:
            return err_reply(codes.UNAVAILABLE, "broker resync in progress")
        session = self._adapter.session
        td = req.trading_day or self._current_td or (session.trading_day if session else self.trading_day())

        def done(res: QueryResult[PositionReport]) -> None:
            if not res.ok:
                deliver(err_reply(codes.UNAVAILABLE, f"broker position query failed: {res.error} {res.message or ''}".strip()))
                return
            items = [
                BrokerPosition(account_id=p.account_id, instrument_id=p.instrument_id, direction=p.direction,
                               td_qty=p.td_qty, yd_qty=p.yd_qty)
                for p in res.items
                if req.account_id is None or p.account_id == req.account_id
            ]
            deliver(ok_reply(BrokerPositionBatch(trading_day=td, as_of_ns=self._now(), items=items)))

        self._adapter.query_positions(td, done, account_id=req.account_id)
        return None

    def query_trades(self, req: TradeQuery) -> Reply:
        """``td.query.trades``: trades of a day in ``trade_seq`` order, paged."""
        limit = max(1, min(req.limit or 500, 1_000))
        start = req.from_trade_seq if req.from_trade_seq is not None and req.from_trade_seq > 0 else 1
        page, more = self._db.read(
            lambda c: store.trades_page(c, req.trading_day, account_id=req.account_id, from_trade_seq=start, limit=limit)
        )
        next_seq = page[-1].trade_seq + 1 if page else start
        return ok_reply(TradeBatch(trades=page, next_trade_seq=next_seq, complete=not more))

    def query_orders(self, req: OrderQuery) -> Reply:
        """``td.query.orders``: read-only, never writes a tombstone (02 §6.4)."""
        items = self._db.read(
            lambda c: store.order_updates(c, req.trading_day, account_id=req.account_id,
                                          client_order_ids=req.client_order_ids)
        )
        return ok_reply(BrokerOrderBatch(items=items))

    def digest(self, req: DigestRequest) -> Reply:
        """``td.digest``: per-account ``compute_account_digest`` over the day's trades (02 §7.5)."""
        by_account = self._db.read(lambda c: store.trades_by_account(c, req.trading_day))
        accounts = [compute_account_digest(acct, rows) for acct, rows in sorted(by_account.items())]
        return ok_reply(TradeDigest(trading_day=req.trading_day, accounts=accounts,
                                    total_count=sum(a.count for a in accounts), generated_ns=self._now()))

    # ------------------------------------------------------------ td.import_trades
    def import_trades(self, req: TradeImport, requester: str) -> Reply:
        """``td.import_trades`` (ADMIN, idempotent by ``command_id``): dedupe by trade key, publish on td.exec.

        Role and content checks answer ``FORBIDDEN`` / ``BAD_REQUEST`` without recording;
        otherwise check, business writes and record run in ONE writer job
        (:meth:`ProcessedCommands.run_once`, 03 §7 导入成交).
        """
        if not req.command_id:
            return err_reply(codes.BAD_REQUEST, "command_id is required")
        if req.role is not Role.ADMIN:
            return err_reply(codes.FORBIDDEN, f"td.import_trades requires ADMIN, got {req.role.value}")
        problem = self._validate_import(req)
        if problem is not None:
            return err_reply(codes.BAD_REQUEST, problem)
        now = self._now()
        ref = f"IMPORT:{req.command_id}"
        counts: dict[str, int] = {}

        def handler(conn: Connection) -> Reply:
            inserted = duplicates = 0
            for t in req.trades:
                td = t.trading_day or req.trading_day
                if store.find_trade(conn, td, t.exchange.value, t.trade_id, t.direction.value) is not None:
                    duplicates += 1
                    continue
                coid = t.client_order_id if t.client_order_id and store.get_routed(conn, t.client_order_id) else None
                trade = Trade(
                    trading_day=td,
                    exchange=t.exchange,
                    trade_id=t.trade_id,
                    direction=t.direction,
                    account_id=t.account_id,
                    instrument_id=t.instrument_id,
                    client_order_id=coid,
                    origin=TradeOrigin.EXTERNAL,
                    broker_order_ref=ref,
                    order_sys_id=t.order_sys_id,
                    offset=t.offset,
                    price=t.price,
                    qty=t.qty,
                    trade_ts_ns=t.trade_ts_ns or now,
                    broker_seq=t.broker_seq,
                    trade_seq=0,
                    received_ns=now,
                )
                seq = store.insert_trade(conn, trade, import_batch=req.command_id)
                self._append(conn, trade.account_id, msgspec.structs.replace(trade, trade_seq=seq), td=td, corr_id=coid)
                inserted += 1
            counts["inserted"] = inserted
            return ok_reply(TradeImportAck(inserted=inserted, duplicates=duplicates))

        reply = self._db.write(lambda c: self._processed.run_once(
            c, command_id=req.command_id, command=CMD_IMPORT, requester=requester, payload=req,
            trading_day=req.trading_day, received_ns=now, handler=handler))
        self.stats.imported += counts.get("inserted", 0)
        logger.info("tg.import_trades", command_id=req.command_id, source=req.source, actor=req.actor, code=reply.code,
                    duplicate=reply.duplicate)
        return reply

    @staticmethod
    def _validate_import(req: TradeImport) -> str | None:
        if len(req.trading_day) != 8 or not req.trading_day.isdigit():
            return "trading_day must be YYYYMMDD"
        for t in req.trades:
            if t.trading_day and t.trading_day != req.trading_day:
                return f"trade {t.trade_id} belongs to {t.trading_day}, not {req.trading_day}"
            if t.qty <= 0 or not math.isfinite(t.price) or not t.trade_id or not t.account_id or not t.instrument_id:
                return f"trade {t.trade_id!r}: trade_id, account_id, instrument_id, qty>0 and a finite price are required"
        return None

    # ------------------------------------------------------------ kill switch
    def kill_switch(self, req: KillSwitchRequest, requester: str) -> Reply:
        """``firm.kill_switch`` from api-gateway: the MANUAL flag (idempotent by ``command_id``).

        ``role`` must be RISK_MANAGER or ADMIN (02 §4.11 RBAC re-check, §9.6); the
        ``firm_controls`` row and the ``processed_commands`` record are one writer job.
        """
        if not req.command_id:
            return err_reply(codes.BAD_REQUEST, "command_id is required")
        if req.role not in _KILL_ROLES:
            return err_reply(codes.FORBIDDEN, f"firm.kill_switch requires RISK_MANAGER or ADMIN, got {req.role.value}")
        now = self._now()
        td = self.trading_day()
        applied: dict[str, bool] = {}

        def handler(conn: Connection) -> Reply:
            action = FirmAction.KILL_SWITCH_ON if req.on else FirmAction.KILL_SWITCH_OFF
            store.insert_firm_control(conn, action=action, origin=ControlOrigin.MANUAL, active=req.on, restrictions=[],
                                      trading_day=td, actor=req.actor, reason=req.reason, cause_alert_key=None,
                                      src_instance=requester, ts_ns=now)
            applied["on"] = req.on
            return ok_reply(FirmControl(action=action, origin=ControlOrigin.MANUAL, active=req.on, restrictions=[],
                                        trading_day=td, actor=req.actor, reason=req.reason, ts_ns=now))

        reply = self._db.write(lambda c: self._processed.run_once(
            c, command_id=req.command_id, command=CMD_KILL_SWITCH, requester=requester, payload=req,
            trading_day=td, received_ns=now, handler=handler), critical=True)
        if "on" in applied:
            self.kill.manual = applied["on"]
            self._status_dirty = True
            logger.warning("tg.kill_switch", origin="MANUAL", on=applied["on"], actor=req.actor, reason=req.reason,
                           effective=self.kill.effective)
        return reply

    def apply_firm_control(
        self, control: FirmControl | None, *, src: str, save_cursor: Callable[[Connection], None] | None
    ) -> None:
        """Apply one ``risk.control.firm`` FACT together with its FACT cursor, in one transaction (03 §7).

        * ``RULE_STATE`` (origin RULE): the whole RULE layer in one fact - the RULE kill flag
          becomes ``active``; its ``restrictions`` are kept in the same row as an audit copy
          (trade-gateway enforces only the kill switch). The newest fact alone restores the
          state, which is what the ``ctl.snapshot`` fallback delivers.
        * legacy ``KILL_SWITCH_ON`` / ``KILL_SWITCH_OFF`` with origin RULE: set the RULE flag;
          ``SET_RESTRICTIONS`` rows are an audit copy only (``active=0``, 03 §4.3).
        * MANUAL records are ignored here: the manual flag arrives by ``firm.kill_switch``.

        Idempotent by business key ``(origin, action, ts_ns, source)``, and a kill switch state
        older than the newest stored one (any of ON / OFF / RULE_STATE) never overrides it, so a
        replay from seq 1 (publisher stream change, lost cursor) cannot flip the state back.
        """
        now = self._now()
        td_fallback = self.trading_day()
        outcome: dict[str, bool | None] = {"rule": None}

        def tx(conn: Connection) -> None:
            if control is not None:
                td = control.trading_day if len(control.trading_day) == 8 else td_fallback
                ts = control.ts_ns or now
                restrictions = [msgspec.to_builtins(r) for r in control.restrictions]
                seen = store.firm_control_seen(conn, origin=control.origin, action=control.action, ts_ns=ts, src_instance=src)
                if seen:
                    logger.info("tg.firm_control_duplicate", action=control.action.value, ts_ns=ts)
                elif control.action in store.KILL_STATE_ACTIONS:
                    newest = store.latest_kill_ts(conn, ControlOrigin.RULE)
                    if control.origin is not ControlOrigin.RULE:
                        logger.warning("tg.firm_control_ignored", origin=control.origin.value, action=control.action.value,
                                       reason="manual kill switch arrives by firm.kill_switch, not risk.control.firm")
                    elif newest is not None and ts < newest:
                        logger.warning("tg.firm_control_stale", action=control.action.value, ts_ns=ts, newest_ns=newest)
                    else:
                        if control.action is FirmAction.RULE_STATE:
                            active = control.active
                        else:
                            active = control.action is FirmAction.KILL_SWITCH_ON
                            if active != control.active:
                                logger.warning("tg.firm_control_inconsistent", action=control.action.value,
                                               active=control.active)
                        store.insert_firm_control(conn, action=control.action, origin=ControlOrigin.RULE, active=active,
                                                  restrictions=restrictions, trading_day=td, actor=control.actor,
                                                  reason=control.reason, cause_alert_key=control.cause_alert_key,
                                                  src_instance=src, ts_ns=ts)
                        outcome["rule"] = active
                else:
                    store.insert_firm_control(conn, action=control.action, origin=control.origin, active=False,
                                              restrictions=restrictions, trading_day=td, actor=control.actor,
                                              reason=control.reason, cause_alert_key=control.cause_alert_key,
                                              src_instance=src, ts_ns=ts)
            if save_cursor is not None:
                save_cursor(conn)

        self._db.write(tx, critical=True)
        rule = outcome["rule"]
        if rule is not None:
            self.kill.rule = rule
            self._status_dirty = True
            logger.warning("tg.kill_switch", origin="RULE", on=rule, src=src, effective=self.kill.effective)

    # ------------------------------------------------------------ broker callbacks
    def on_status(self, state: BrokerConnState, session: BrokerSession | None) -> None:
        if state is BrokerConnState.LOGGED_IN and session is not None:
            self._session = session
            self._begin_login_resync(session)
        else:
            if self._resync is not None:
                logger.warning("tg.resync_aborted", reason=f"broker {state.value}", resync=self._resync.reason)
                self._resync = None
            if self._session is not None:  # a logged-in session was lost
                now = self._now()
                ids = self._db.write(lambda c: store.mark_unknown(c, now))
                if ids:
                    logger.warning("tg.orders_unknown", count=len(ids), reason="broker link lost")
            self._cancel_inflight.clear()  # their fate is unknown: pending cancels are re-sent after the resync
            self._cancel_due.clear()
            self._session = None
        self._status_dirty = True

    def on_order(self, report: OrderReport) -> None:
        self._on_live_report(report)

    def on_trade(self, report: TradeReport) -> None:
        self._on_live_report(report)

    def on_invalid(self, report: InvalidReport) -> None:
        self.stats.invalid_reports += 1
        self._on_live_report(report)

    def on_insert_error(self, error: InsertError) -> None:
        if self._resync is not None:
            self._deferred_errors.append(error)
            return
        self._handle_insert_error(error)

    def on_action_error(self, error: ActionError) -> None:
        if self._resync is not None:
            self._deferred_errors.append(error)
            return
        self._handle_action_error(error)

    def on_heartbeat(self, trading_day: str, broker_seq_head: int) -> None:
        if self._resync is not None:
            return
        if self._current_td is None or trading_day > self._current_td:
            self._day_roll(trading_day)
            return
        now = self._mono()
        if (
            trading_day == self._current_td
            and self._seq_conflict_td != trading_day
            and broker_seq_head > self.cursor(trading_day)
            and now >= self._next_tail_check
        ):
            self._next_tail_check = now + self._tail_interval_ns
            self.stats.tail_resyncs += 1
            logger.warning("tg.tail_gap", trading_day=trading_day, broker_seq_head=broker_seq_head,
                           last_broker_seq=self.cursor(trading_day))
            self._start_resync("tail_gap", [(trading_day, self.cursor(trading_day) + 1)])

    # ------------------------------------------------------------ live report sequencing
    def _on_live_report(self, r: BrokerReport) -> None:
        rs = self._resync
        if rs is not None:
            if len(rs.buffer) >= self._max_buffer:
                self._buffer_overflow(rs)
                return  # dropped with the buffer: the restarted resync fetches it again
            rs.buffer.append(r)
            return
        td, seq = r.trading_day, r.broker_seq
        if td is None or seq is None:
            return  # unreadable sequence number: counted by the adapter, nothing to sequence
        if self._current_td is not None and td > self._current_td:
            self._day_roll(td, buffer=[r])  # first report of a new trading day: check the old day's tail first
            return
        last = self.cursor(td)
        if seq <= last:
            self.stats.dup_live += 1
            return
        if seq == last + 1:
            self._apply_reports([r])
            self._flush_cancel_due()
            return
        self.stats.gap_resyncs += 1
        logger.warning("tg.seq_gap", trading_day=td, expected=last + 1, got=seq)
        self._start_resync("gap", [(td, last + 1)], buffer=[r])

    def _buffer_overflow(self, rs: _Resync) -> None:
        """The resync buffer is full: drop it and restart the resync from the cursors (nothing is lost)."""
        self.stats.buffer_overflows += 1
        steps = [(td, self.cursor(td) + 1) for td, _ in rs.steps]
        if not steps and self._current_td is not None:
            steps = [(self._current_td, self.cursor(self._current_td) + 1)]
        logger.error("tg.resync_buffer_overflow", limit=self._max_buffer, reason=rs.reason, restart_steps=steps)
        if steps:
            self._start_resync("buffer_overflow", steps)
        else:
            self._resync = None

    # ------------------------------------------------------------ resync
    def _catchup_steps(self, last_td: str | None, to_td: str) -> list[tuple[str, int]]:
        """Every trading day after ``last_td`` up to ``to_td`` (from their cursors), ``last_td`` included."""
        steps: list[tuple[str, int]] = []
        if last_td is not None and last_td < to_td:
            try:
                days = [td_str(d) for d in self._calendar.trading_days_between(parse_td(last_td), parse_td(to_td))]
            except (LookupError, ValueError):
                days = []
            days = [d for d in days if last_td <= d < to_td]
            if last_td not in days:
                days.insert(0, last_td)
            if len(days) > MAX_CATCHUP_DAYS:
                logger.warning("tg.catchup_truncated", days=len(days), kept=MAX_CATCHUP_DAYS)
                days = days[-MAX_CATCHUP_DAYS:]
            steps.extend((d, self.cursor(d) + 1) for d in days)
        steps.append((to_td, self.cursor(to_td) + 1))
        return steps

    def _day_roll(self, new_td: str, buffer: list[BrokerReport] | None = None) -> None:
        """The broker moved to a new trading day: fetch the tail of every day since, then the new day."""
        old = self._current_td
        steps = self._catchup_steps(old, new_td)
        logger.info("tg.trading_day_roll", old=old, new=new_td)
        self._current_td = new_td
        if self._seq_conflict_td is not None and self._seq_conflict_td != new_td:
            logger.warning("tg.broker_seq_conflict_cleared", trading_day=self._seq_conflict_td, new_trading_day=new_td)
            self._seq_conflict_td = None
        self._start_resync("day_roll", steps, buffer=buffer)

    def _begin_login_resync(self, session: BrokerSession) -> None:
        td = session.trading_day
        known = sorted(self._cursors)
        last = known[-1] if known else None
        steps = self._catchup_steps(last, td)
        if session.broker_seq_head < self.cursor(td):
            self._seq_conflict_td = td
            logger.error("tg.broker_seq_backwards", trading_day=td, broker_seq_head=session.broker_seq_head,
                         last_broker_seq=self.cursor(td),
                         detail="broker sequence reset (sim.db wiped?): routing refused, service DEGRADED until the day rolls")
        elif self._seq_conflict_td is not None and self._seq_conflict_td != td:
            self._seq_conflict_td = None
        if self._current_td is None or td > self._current_td:
            self._current_td = td
        now = self._now()
        cur = self.cursor(td)
        self._db.write(lambda c: store.upsert_cursor(c, broker=self._broker, trading_day=td, last_broker_seq=cur,
                                                     session_id=str(session.session_id), front_id=session.front_id,
                                                     now_ns=now))
        self._start_resync("login", steps)

    def _start_resync(self, reason: str, steps: list[tuple[str, int]], buffer: list[BrokerReport] | None = None) -> None:
        self._resync_gen += 1
        self._resync = _Resync(gen=self._resync_gen, reason=reason, steps=list(steps), buffer=list(buffer or []))
        self.stats.resyncs += 1
        self._status_dirty = True
        logger.info("tg.resync_start", reason=reason, steps=steps)
        self._issue_step()

    def _issue_step(self) -> None:
        rs = self._resync
        if rs is None:
            return
        if not rs.steps:
            self._finish_resync()
            return
        td, from_seq = rs.steps[0]
        rs.attempt += 1
        rs.orders = None
        rs.trades = None
        rs.failed = False
        rs.retry_at = None
        gen, attempt = rs.gen, rs.attempt
        # orders FIRST, trades second: the broker serves requests in order, so the trade snapshot is the later one
        self._adapter.query_orders(td, from_seq, lambda res: self._on_step_result(gen, attempt, "orders", res))
        self._adapter.query_trades(td, from_seq, lambda res: self._on_step_result(gen, attempt, "trades", res))

    def _on_step_result(self, gen: int, attempt: int, kind: str, res: QueryResult[Any]) -> None:
        rs = self._resync
        if rs is None or rs.gen != gen or rs.attempt != attempt:
            return  # superseded
        if not res.ok:
            logger.warning("tg.resync_query_failed", kind=kind, error=res.error, message=res.message, reason=rs.reason)
            if not rs.failed:
                rs.failed = True
                rs.retry_at = self._mono() + self._resync_retry_ns
            return
        if kind == "orders":
            rs.orders = res
        else:
            rs.trades = res
        if rs.failed or rs.orders is None or rs.trades is None:
            return
        self._complete_step(rs, rs.orders, rs.trades)
        if self._resync is rs:
            self._issue_step()

    def _complete_step(self, rs: _Resync, orders: QueryResult[OrderReport], trades: QueryResult[TradeReport]) -> None:
        """Merge one trading day's answers with the buffered live reports (see the module docstring)."""
        td, from_seq = rs.steps[0]
        self.stats.invalid_reports += len(orders.invalid) + len(trades.invalid)
        queried: list[BrokerReport] = [*orders.items, *orders.invalid, *trades.items, *trades.invalid]
        q_seqs = {r.broker_seq for r in queried if r.broker_seq is not None}
        top = max(q_seqs, default=from_seq - 1)  # the cursor after this step
        order_reports: list[BrokerReport] = [*orders.items, *orders.invalid]
        order_top = max((r.broker_seq for r in order_reports if r.broker_seq is not None), default=from_seq - 1)
        mine = [b for b in rs.buffer if b.trading_day == td and b.broker_seq is not None]
        rs.buffer = [b for b in rs.buffer if not (b.trading_day == td and b.broker_seq is not None)]
        below = [b for b in mine if b.broker_seq is not None and b.broker_seq <= top]
        above = [b for b in mine if b.broker_seq is not None and b.broker_seq > top]
        # (order_top, top] holds no superseded order state: trades are all answered (later snapshot) and order
        # events after the order snapshot were pushed live, so a hole there is a lost live frame.
        present = q_seqs | {b.broker_seq for b in below if b.broker_seq is not None}
        lo = max(order_top, from_seq - 1)
        missing = next((s for s in range(lo + 1, top + 1) if s not in present), None)
        if missing is not None:
            self.stats.lost_frames += 1
            logger.warning("tg.resync_lost_frame", trading_day=td, missing=missing, order_snapshot_top=order_top, top=top)
            self._apply_merged([r for r in (*queried, *below) if r.broker_seq is not None and r.broker_seq < missing])
            rs.buffer.extend(b for b in (*below, *above) if b.broker_seq is not None and b.broker_seq > missing)
            rs.steps[0] = (td, missing)
            return
        self._apply_merged([*queried, *below])
        rs.steps.pop(0)
        rs.buffer.extend(above)  # fed through the live path (seq == last + 1) when the resync ends

    def _apply_merged(self, items: list[BrokerReport]) -> None:
        """Dedupe by ``(trading_day, kind, broker_seq)`` and apply in ``broker_seq`` order."""
        unique: dict[tuple[str, str, int], BrokerReport] = {}
        for r in items:
            if r.trading_day is None or r.broker_seq is None:
                continue
            kind = "order" if isinstance(r, OrderReport) else "trade" if isinstance(r, TradeReport) else r.kind
            key = (r.trading_day, kind, r.broker_seq)
            if key not in unique or isinstance(unique[key], InvalidReport):
                unique[key] = r
        ordered = sorted(unique.values(), key=_report_key)
        if ordered:
            self._apply_reports(ordered)

    def _finish_resync(self) -> None:
        rs = self._resync
        if rs is None:
            return
        self._resync = None
        self._synced_once = True
        self._status_dirty = True
        logger.info("tg.resync_done", reason=rs.reason, cursors=dict(self._cursors), leftover=len(rs.buffer))
        for r in sorted(rs.buffer, key=_report_key):
            self._on_live_report(r)
        if self._resync is not None:
            return  # a leftover report revealed a new gap
        errors, self._deferred_errors = self._deferred_errors, []
        for err in errors:
            if isinstance(err, InsertError):
                self._handle_insert_error(err)
            else:
                self._handle_action_error(err)
        self._reconcile()
        self._cancel_due.clear()
        self._send_pending_cancels()

    def _reconcile(self) -> None:
        """After a resync: send held orders, close orders the broker never saw (02 §8.3)."""
        rows = self._db.read(lambda c: store.routed_with_status(c, (BrokerOrderStatus.RECEIVED, BrokerOrderStatus.UNKNOWN)))
        if not rows:
            return
        for row in rows:
            coid = row.client_order_id
            if row.status is BrokerOrderStatus.RECEIVED:
                if self._adapter.state is BrokerConnState.LOGGED_IN or self._now() > row.expire_ns:
                    reply = self._submit(row, held=True)  # expired / kill switch / sent, published when refused
                    logger.info("tg.held_released", client_order_id=coid, code=reply.code)
                    self._held.pop(coid, None)
            elif row.cancel_requested_ns is not None:
                self._close_local(row, BrokerOrderStatus.CANCELED, codes.BROKER_ORDER_NOT_FOUND,
                                  "cancel requested; the broker never received the order")
            else:
                self._close_local(row, BrokerOrderStatus.REJECTED, codes.BROKER_DISCONNECTED,
                                  "sent before the broker link was lost; the broker never reported it")
        self._status_dirty = True

    def _close_local(self, row: RoutedOrder, status: BrokerOrderStatus, code: str, msg: str) -> None:
        """Terminal status decided by trade-gateway itself, published on td.exec (``broker_seq`` 0)."""
        now = self._now()

        def tx(conn: Connection) -> bool:
            cur = store.get_routed(conn, row.client_order_id)
            if cur is None or cur.status is not row.status:
                return False  # changed meanwhile
            store.update_routed(conn, row.client_order_id, status=status, broker_code=code, broker_msg=msg, updated_ns=now)
            self._append(conn, cur.account_id, cur.update_msg(status=status, broker_code=code, broker_msg=msg, updated_ns=now),
                         td=cur.trading_day, corr_id=cur.client_order_id)
            return True

        if self._db.write(tx):
            self.stats.local_updates += 1
            self._held.pop(row.client_order_id, None)
            logger.warning("tg.order_closed_locally", client_order_id=row.client_order_id, status=status.value, code=code)

    # ------------------------------------------------------------ report persistence
    def _apply_reports(self, reports: list[BrokerReport]) -> None:
        """Persist reports in ``broker_seq`` order, ``apply_chunk`` per transaction (03 §7 柜台回报).

        ``td.status`` picks the new ``last_broker_seq`` up on its next 1 Hz publication (it is
        sent immediately only when the connection state changes, 02 §5).
        """
        for i in range(0, len(reports), self._chunk):
            self._apply_chunk(reports[i : i + self._chunk])

    def _apply_chunk(self, chunk: list[BrokerReport]) -> None:
        now = self._now()
        session = self._session

        def tx(conn: Connection) -> dict[str, int]:
            touched: dict[str, int] = {}
            self._ensure_routed(conn, chunk, now)
            for r in chunk:
                if isinstance(r, OrderReport):
                    self._store_order(conn, r, now)
                elif isinstance(r, TradeReport):
                    self._store_trade(conn, r, now)
                if r.trading_day is not None and r.broker_seq is not None:
                    touched[r.trading_day] = max(touched.get(r.trading_day, 0), r.broker_seq)
            for td, seq in touched.items():
                store.upsert_cursor(conn, broker=self._broker, trading_day=td, last_broker_seq=seq,
                                    session_id=str(session.session_id) if session else None,
                                    front_id=session.front_id if session else None, now_ns=now)
            return touched

        touched = self._db.write(tx)
        for td, seq in touched.items():
            if seq > self._cursors.get(td, 0):
                self._cursors[td] = seq
            if self._current_td is None or td > self._current_td:
                self._current_td = td

    def _flush_cancel_due(self) -> None:
        """Cancels waiting for an order's broker acknowledgement go out once it arrived."""
        if self._cancel_due and self._can_send():
            due, self._cancel_due = self._cancel_due, set()
            self._send_pending_cancels(only=due)

    def _ensure_routed(self, conn: Connection, chunk: list[BrokerReport], now: int) -> None:
        """Reports of orders not routed here (lost td.db row, another client): reconstruct the row (FK)."""
        best: dict[str, OrderReport | TradeReport] = {}
        for r in chunk:
            if isinstance(r, OrderReport | TradeReport) and r.client_order_id:
                prev = best.get(r.client_order_id)
                if prev is None or (isinstance(r, OrderReport) and not isinstance(prev, OrderReport)):
                    best[r.client_order_id] = r
        for coid, r in best.items():
            if store.get_routed(conn, coid) is not None:
                continue
            if isinstance(r, OrderReport):
                order_type, tif, price, qty, status = r.order_type, r.tif, store.price_str(r.price), max(1, r.qty), r.status
            else:
                order_type, tif, price, qty, status = (OrderType.LIMIT, TimeInForce.GFD, store.price_str(r.price), r.qty,
                                                       BrokerOrderStatus.ACCEPTED)
            store.insert_routed_row(conn, RoutedOrder(
                client_order_id=coid, account_id=r.account_id, instrument_id=r.instrument_id, exchange=r.exchange,
                direction=r.direction, offset=r.offset, order_type=order_type, tif=tif, price=price, qty=qty,
                trading_day=r.trading_day, src_instance="unknown", expire_ns=0, broker=self._broker,
                broker_order_ref=r.broker_order_ref, order_sys_id=r.order_sys_id, status=status, filled_qty=0,
                broker_code=None, broker_msg="reconstructed from a broker report (not routed by this gateway)",
                received_ns=now, submitted_ns=None, updated_ns=now,
            ))
            self.stats.reconstructed_orders += 1
            logger.warning("tg.order_reconstructed", client_order_id=coid, account_id=r.account_id,
                           broker_order_ref=r.broker_order_ref)

    def _store_order(self, conn: Connection, r: OrderReport, now: int) -> None:
        if store.order_event_exists(conn, r.trading_day, r.broker_seq):
            self.stats.dup_orders += 1
            return
        if r.client_order_id:
            latest = store.latest_order_event(conn, client_order_id=r.client_order_id)
        else:
            latest = store.latest_order_event(conn, broker_order_ref=r.broker_order_ref)
        if latest is not None and (latest[0], latest[1]) > (r.trading_day, r.broker_seq):
            self.stats.stale_orders += 1
            logger.info("tg.stale_order_report", broker_seq=r.broker_seq, newest=latest[1], client_order_id=r.client_order_id)
            return
        code, msg = _broker_code(r)
        store.insert_order_event(
            conn,
            trading_day=r.trading_day,
            broker_seq=r.broker_seq,
            client_order_id=r.client_order_id,
            broker_order_ref=r.broker_order_ref,
            origin=r.origin,
            status=r.status,
            filled_qty=r.filled_qty,
            remaining_qty=r.remaining_qty,
            broker_code=code,
            broker_msg=msg,
            detail={
                "account_id": r.account_id,
                "instrument_id": r.instrument_id,
                "exchange": r.exchange.value,
                "direction": r.direction.value,
                "offset": r.offset.value,
                "order_type": r.order_type.value,
                "tif": r.tif.value,
                "price": r.price,
                "qty": r.qty,
                "order_sys_id": r.order_sys_id,
                "status_msg": r.status_msg,
            },
            ts_ns=r.update_ns or now,
        )
        if r.client_order_id:
            row = store.get_routed(conn, r.client_order_id)
            if row is not None:
                coid = row.client_order_id
                if row.is_terminal and row.status is not r.status:
                    logger.warning("tg.terminal_status_changed", client_order_id=coid, old=row.status.value,
                                   new=r.status.value, broker_seq=r.broker_seq)
                store.update_routed(conn, coid, status=r.status, filled_qty=max(row.filled_qty, r.filled_qty),
                                    order_sys_id=r.order_sys_id or row.order_sys_id, broker_order_ref=r.broker_order_ref,
                                    broker_code=code, broker_msg=msg, updated_ns=now)
                if r.status.is_terminal:
                    self._cancel_inflight.discard(coid)
                    self._cancel_due.discard(coid)
                    if self._held.pop(coid, None) is not None:
                        self._status_dirty = True
                elif row.cancel_requested_ns is not None and coid not in self._cancel_inflight:
                    self._cancel_due.add(coid)  # a cancel waits for this order: send it after the commit
        upd = BrokerOrderUpdate(
            client_order_id=r.client_order_id,
            origin=r.origin,
            account_id=r.account_id,
            instrument_id=r.instrument_id,
            broker_order_ref=r.broker_order_ref,
            order_sys_id=r.order_sys_id,
            status=r.status,
            filled_qty=r.filled_qty,
            remaining_qty=r.remaining_qty,
            broker_code=code,
            broker_msg=msg,
            broker_seq=r.broker_seq,
            trading_day=r.trading_day,
            updated_ns=r.update_ns or now,
        )
        self._append(conn, r.account_id, upd, td=r.trading_day, corr_id=r.client_order_id)
        self.stats.order_reports += 1

    def _store_trade(self, conn: Connection, r: TradeReport, now: int) -> None:
        if store.find_trade(conn, r.trading_day, r.exchange.value, r.trade_id, r.direction.value) is not None:
            self.stats.dup_trades += 1
            return
        trade = Trade(
            trading_day=r.trading_day,
            exchange=r.exchange,
            trade_id=r.trade_id,
            direction=r.direction,
            account_id=r.account_id,
            instrument_id=r.instrument_id,
            client_order_id=r.client_order_id,
            origin=r.origin,
            broker_order_ref=r.broker_order_ref,
            order_sys_id=r.order_sys_id,
            offset=r.offset,
            price=r.price,
            qty=r.qty,
            trade_ts_ns=r.trade_ts_ns,
            broker_seq=r.broker_seq,
            trade_seq=0,
            received_ns=now,
        )
        seq = store.insert_trade(conn, trade)
        if r.client_order_id:
            row = store.get_routed(conn, r.client_order_id)
            if row is not None:
                store.update_routed(conn, row.client_order_id, filled_qty=row.filled_qty + r.qty,
                                    order_sys_id=row.order_sys_id or r.order_sys_id, updated_ns=now)
        self._append(conn, r.account_id, msgspec.structs.replace(trade, trade_seq=seq), td=r.trading_day,
                     corr_id=r.client_order_id)
        self.stats.trade_reports += 1

    # ------------------------------------------------------------ broker errors
    def _handle_insert_error(self, err: InsertError) -> None:
        coid = err.client_order_id
        if not coid:
            logger.warning("tg.insert_error_unmatched", error_id=err.error_id, error_msg=err.error_msg)
            return
        now = self._now()

        def tx(conn: Connection) -> str:
            row = store.get_routed(conn, coid)
            if row is None:
                return "unknown_order"
            if err.order_sys_id is not None:  # the broker created a REJECTED order: its RtnOrder carries the status
                if err.error_msg:
                    store.update_routed(conn, coid, broker_msg=err.error_msg, updated_ns=now)
                return "annotated"
            if err.error_id == BROKER_NOT_LOGGED_IN:
                return "pending"  # the session is gone; the order is reconciled after the next resync
            if row.order_sys_id is None and row.status in (
                BrokerOrderStatus.SUBMITTED, BrokerOrderStatus.UNKNOWN, BrokerOrderStatus.CANCEL_PENDING
            ):
                store.update_routed(conn, coid, status=BrokerOrderStatus.REJECTED, broker_code=err.error_id,
                                    broker_msg=err.error_msg, updated_ns=now)
                self._append(conn, row.account_id, row.update_msg(status=BrokerOrderStatus.REJECTED,
                             broker_code=err.error_id, broker_msg=err.error_msg, updated_ns=now),
                             td=row.trading_day, corr_id=coid)
                return "rejected"
            return "ignored"

        outcome = self._db.write(tx)
        if outcome == "rejected":
            self.stats.local_updates += 1
            self._cancel_inflight.discard(coid)
            self._cancel_due.discard(coid)
        logger.info("tg.insert_error", client_order_id=coid, error_id=err.error_id, outcome=outcome)

    def _handle_action_error(self, err: ActionError) -> None:
        """A cancel refused by the broker.

        * ``BROKER_NOT_LOGGED_IN`` / ``BROKER_DISCONNECTED``: "not now" - the request stays
          pending and is re-sent after the next resync;
        * ``BROKER_ORDER_NOT_FOUND`` before the broker ever reported the order: the insert
          has not reached the broker yet - the cancel is re-sent on its first report;
        * ``BROKER_ORDER_FINISHED`` / ``BROKER_ORDER_NOT_FOUND`` otherwise: the request ends;
          the order's last broker state is restated with the refusal code (``broker_seq`` 0);
        * anything else: logged as an error, the request stays pending.
        """
        coid = err.client_order_id
        if not coid:
            logger.warning("tg.action_error_unmatched", error_id=err.error_id, error_msg=err.error_msg)
            return
        self._cancel_inflight.discard(coid)
        now = self._now()

        def tx(conn: Connection) -> str:
            row = store.get_routed(conn, coid)
            if row is None or row.is_terminal:
                return "ignored"  # finished meanwhile: its terminal report says it all
            if err.error_id in TRANSIENT_CANCEL_ERRORS:
                return "pending"
            last = store.latest_order_event(conn, client_order_id=coid)
            if last is None and err.error_id == codes.BROKER_ORDER_NOT_FOUND:
                if row.status is BrokerOrderStatus.CANCEL_PENDING:
                    store.update_routed(conn, coid, status=BrokerOrderStatus.SUBMITTED, updated_ns=now)
                return "awaiting_ack"
            if err.error_id in FINAL_CANCEL_ERRORS:
                revert = last[2] if last is not None else BrokerOrderStatus.SUBMITTED
                store.update_routed(conn, coid, status=revert, broker_code=err.error_id, broker_msg=err.error_msg,
                                    cancel_requested_ns=None, updated_ns=now)
                self._append(conn, row.account_id, row.update_msg(status=revert, broker_code=err.error_id,
                             broker_msg=err.error_msg, broker_seq=0, updated_ns=now), td=row.trading_day, corr_id=coid)
                return "refused"
            return "pending_unexpected"

        outcome = self._db.write(tx)
        if outcome == "refused":
            self.stats.local_updates += 1
        elif outcome == "pending":
            self.stats.cancels_deferred += 1
        if outcome == "pending_unexpected":
            logger.error("tg.action_error_unexpected", client_order_id=coid, error_id=err.error_id, error_msg=err.error_msg)
        else:
            logger.info("tg.action_error", client_order_id=coid, error_id=err.error_id, outcome=outcome)

    # ------------------------------------------------------------ timers and status
    def on_timer(self, now_mono: int) -> None:
        rs = self._resync
        if (
            rs is not None
            and rs.retry_at is not None
            and now_mono >= rs.retry_at
            and self._adapter.state is BrokerConnState.LOGGED_IN
        ):
            self._issue_step()
        if self._held and now_mono >= self._next_held_sweep:
            self._next_held_sweep = now_mono + 1_000 * _NS_PER_MS
            self._sweep_held()
        state = self.published_state()
        if state != self._last_state:
            self._last_state = state
            self._state_since = self._now()
            self._status_dirty = True
        if self._status_dirty or now_mono >= self._next_status:
            self._status_dirty = False
            self._next_status = now_mono + self._status_interval_ns
            self._publish_status(state)

    def next_timer_ns(self) -> int | None:
        times = [self._next_status]
        rs = self._resync
        if rs is not None and rs.retry_at is not None:
            times.append(rs.retry_at)
        if self._held:
            times.append(self._next_held_sweep)
        if self._status_dirty:
            return self._mono()
        return min(times)

    def _sweep_held(self) -> None:
        """Held orders expire exactly at ``expire_ns`` when the resync has not completed by then (02 §6.4)."""
        now = self._now()
        late = [coid for coid, exp in self._held.items() if now > exp]
        rows = self._db.read(lambda c: [store.get_routed(c, coid) for coid in late]) if late else []
        for coid, row in zip(late, rows, strict=True):
            self._held.pop(coid, None)
            if row is not None and row.status is BrokerOrderStatus.RECEIVED:
                self.stats.rejected_expired += 1
                self._close_local(row, BrokerOrderStatus.EXPIRED, codes.ROUTE_EXPIRED,
                                  "held while the broker resynced, expire_ns passed")
        if late:
            self._status_dirty = True

    def _publish_status(self, state: BrokerConnState) -> None:
        pending = self._db.read(store.count_pending)
        session = self._session if state in (BrokerConnState.LOGGED_IN, BrokerConnState.RESYNCING) else None
        td = self._current_td or (session.trading_day if session is not None else None)
        status = BrokerStatus(
            broker=self._broker,
            state=state,
            trading_day=td,
            session_id=str(session.session_id) if session is not None else None,
            front_id=session.front_id if session is not None else None,
            last_broker_seq=self.cursor(td) if td else 0,
            pending_orders=pending,
            since_ns=self._state_since,
        )
        self.last_status = status
        if self._publish_status_cb is not None:
            self._publish_status_cb(status)


__all__ = [
    "FINAL_CANCEL_ERRORS",
    "TRANSIENT_CANCEL_ERRORS",
    "CoreStats",
    "FactAppender",
    "KillSwitch",
    "TradeGatewayCore",
]
