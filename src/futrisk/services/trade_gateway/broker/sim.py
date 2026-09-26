"""``SimBrokerAdapter``: the SimBroker client of trade-gateway (02 §9.4, §10; sim README).

Transport: one ZeroMQ DEALER (identity = the trade-gateway instance, ``IMMEDIATE=1``,
``LINGER=0``) connected to the sim-exchange ROUTER; one JSON object per frame. The
socket is **owned by the business thread**: the service registers it in its
``zmq.Poller`` (``on_socket`` hook) and calls :meth:`SimBrokerAdapter.pump` /
:meth:`SimBrokerAdapter.poll_timers` from its loop (02 §8.1). Sockets are never shared
across threads (02 §4.1).

Connection state machine (public ``BrokerConnState`` in brackets)::

    IDLE [DISCONNECTED] --attempt--> CONNECTING [DISCONNECTED] --login sent--> LOGIN_SENT [CONNECTED]
    LOGIN_SENT --RspUserLogin ok--> LOGGED_IN [LOGGED_IN]
    LOGIN_SENT --error / timeout--> IDLE (+ exponential backoff with jitter)
    LOGGED_IN --no message for heartbeat_timeout / send failed / BROKER_NOT_LOGGED_IN--> IDLE

* **Liveness**: the counter sends ``Heartbeat`` every second to logged-in sessions;
  any received message refreshes the link, silence longer than
  ``heartbeat_timeout_ms`` (default 3.5 s, three missed heartbeats) drops the session
  (``BROKER_DISCONNECT`` / ``SIM_RESTART`` faults end sessions silently).
* **Reconnect**: every attempt uses a fresh socket (stale queued frames are discarded,
  ``ROUTER_HANDOVER`` on the sim side lets the new socket take the identity over); the
  first retry after a lost session is quick, failed attempts back off exponentially
  (``backoff_initial_ms`` .. ``backoff_max_ms``, +/-20 % jitter).
* **Orders never queue while disconnected**: ``IMMEDIATE=1`` makes a send fail at once
  when no connection exists; :meth:`insert_order` then raises
  :class:`~futrisk.services.trade_gateway.broker.base.BrokerUnavailable`.
* ``order_ref`` values continue from ``max_order_ref + 1`` of each login (CTP).
* Unknown enum values in reports are refused here (02 §9.4): ``on_invalid`` is called,
  the report is logged and counted, nothing reaches the database.
"""

from __future__ import annotations

import random
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import msgspec
import zmq
from loguru import logger

from futrisk.common.clock import Clock, SystemClock
from futrisk.messages import codes
from futrisk.messages.enums import BrokerConnState
from futrisk.messages.orders import RouteOrder
from futrisk.services.trade_gateway.broker.base import (
    ActionError,
    BrokerCallbacks,
    BrokerSession,
    BrokerUnavailable,
    InsertError,
    InvalidReport,
    OrderReport,
    PositionReport,
    QueryDone,
    QueryResult,
    TradeReport,
    make_broker_order_ref,
    register_broker,
)
from futrisk.services.trade_gateway.broker.wire import (
    WireError,
    login_msg,
    order_action_msg,
    order_insert_msg,
    order_or_invalid,
    parse_position,
    query_msg,
    trade_or_invalid,
)

_NS_PER_MS = 1_000_000
_CONNECT_POLL_NS = 20 * _NS_PER_MS
_MAX_TRACKED = 10_000

# sim extensions of the broker reason codes (02 §11.3)
BROKER_NOT_LOGGED_IN = "BROKER_NOT_LOGGED_IN"
BROKER_LOGIN_FAILED = "BROKER_LOGIN_FAILED"
QUERY_TIMEOUT = codes.TIMEOUT

IDLE, CONNECTING, LOGIN_SENT, LOGGED_IN = "IDLE", "CONNECTING", "LOGIN_SENT", "LOGGED_IN"
_PUBLIC: dict[str, BrokerConnState] = {
    IDLE: BrokerConnState.DISCONNECTED,
    CONNECTING: BrokerConnState.DISCONNECTED,
    LOGIN_SENT: BrokerConnState.CONNECTED,
    LOGGED_IN: BrokerConnState.LOGGED_IN,
}

_encoder = msgspec.json.Encoder()


@dataclass
class AdapterStats:
    messages: int = 0
    bad_frames: int = 0
    invalid_reports: int = 0
    logins: int = 0
    login_failures: int = 0
    disconnects: int = 0
    heartbeats: int = 0
    orders_sent: int = 0
    cancels_sent: int = 0
    queries_sent: int = 0


@dataclass
class _Query:
    kind: str  # "orders" | "trades" | "positions"
    on_done: Callable[[QueryResult[Any]], None]
    deadline_ns: int
    result: QueryResult[Any] = field(default_factory=QueryResult)


class _NullCallbacks:
    """Receiver used until :meth:`SimBrokerAdapter.bind` is called."""

    def on_status(self, state: BrokerConnState, session: BrokerSession | None) -> None:
        pass

    def on_order(self, report: OrderReport) -> None:
        pass

    def on_trade(self, report: TradeReport) -> None:
        pass

    def on_invalid(self, report: InvalidReport) -> None:
        pass

    def on_insert_error(self, error: InsertError) -> None:
        pass

    def on_action_error(self, error: ActionError) -> None:
        pass

    def on_heartbeat(self, trading_day: str, broker_seq_head: int) -> None:
        pass


@register_broker("sim")
class SimBrokerAdapter:
    """SimBroker client; see the module docstring.

    Args:
        endpoint: the SimBroker ROUTER (``sim.yaml`` ``broker.endpoint``).
        identity: DEALER identity (02 §9.4: ``trade-gateway``).
        broker_id / user_id / app_id: ``ReqUserLogin`` fields.
        clock: monotonic time source for the deadlines (tests may inject one).
        heartbeat_timeout_ms: silence after which a logged-in session is considered lost.
        login_timeout_ms: time allowed to connect and receive ``RspUserLogin``.
        query_timeout_ms: time allowed for all pages of a query.
        backoff_initial_ms / backoff_max_ms: reconnect backoff bounds.
        on_socket: ``on_socket(sock, created)`` so the owner registers / unregisters the
            socket in its poller.
    """

    def __init__(
        self,
        endpoint: str,
        *,
        identity: str = "trade-gateway",
        broker_id: str = "9999",
        user_id: str = "tg",
        app_id: str = "futrisk",
        clock: Clock | None = None,
        heartbeat_timeout_ms: int = 3_500,
        login_timeout_ms: int = 3_000,
        query_timeout_ms: int = 10_000,
        backoff_initial_ms: int = 200,
        backoff_max_ms: int = 5_000,
        on_socket: Callable[[zmq.Socket[Any], bool], None] | None = None,
        rng: random.Random | None = None,
    ) -> None:
        self.endpoint = endpoint
        self.identity = identity
        self._broker_id = broker_id
        self._user_id = user_id
        self._app_id = app_id
        self._clock: Clock = clock or SystemClock()
        self._hb_timeout_ns = max(100, heartbeat_timeout_ms) * _NS_PER_MS
        self._login_timeout_ns = max(100, login_timeout_ms) * _NS_PER_MS
        self._query_timeout_ns = max(100, query_timeout_ms) * _NS_PER_MS
        self._backoff_initial_ns = max(10, backoff_initial_ms) * _NS_PER_MS
        self._backoff_max_ns = max(backoff_initial_ms, backoff_max_ms) * _NS_PER_MS
        self._on_socket = on_socket
        self._rng = rng or random.Random()
        self._cb: BrokerCallbacks = _NullCallbacks()
        self._ctx: zmq.Context[Any] | None = None
        self._sock: zmq.Socket[Any] | None = None
        self._phase = IDLE
        self._emitted = BrokerConnState.DISCONNECTED
        self._session: BrokerSession | None = None
        self._started = False
        self._attempt_at: int | None = None
        self._phase_deadline: int | None = None
        self._backoff_ns = self._backoff_initial_ns
        self._last_rx = 0
        self._req_id = 0
        self._login_req: int | None = None
        self._next_ref = 1
        self._queries: dict[int, _Query] = {}
        self._inserts: dict[int, str] = {}
        self._cancels: dict[int, tuple[str, str | None, str | None]] = {}
        self._deferred: list[Callable[[], None]] = []
        self._pending_loss: str | None = None
        self.broker_trading_day: str | None = None
        self.broker_seq_head = 0
        self.stats = AdapterStats()

    # ------------------------------------------------------------ properties
    @property
    def broker(self) -> str:
        return "sim"

    @property
    def state(self) -> BrokerConnState:
        return _PUBLIC[self._phase]

    @property
    def session(self) -> BrokerSession | None:
        return self._session

    @property
    def socket(self) -> zmq.Socket[Any] | None:
        return self._sock

    # ------------------------------------------------------------ lifecycle
    def bind(self, callbacks: BrokerCallbacks) -> None:
        self._cb = callbacks

    def start(self) -> None:
        """Begin connecting; the first attempt runs on the next :meth:`poll_timers`."""
        if self._ctx is None:
            self._ctx = zmq.Context()
        self._started = True
        self._attempt_at = self._clock.monotonic_ns()

    def stop(self) -> None:
        """Close the socket and the context; pending queries are dropped."""
        self._started = False
        self._queries.clear()
        self._deferred.clear()
        self._close_socket()
        self._session = None
        self._phase = IDLE
        if self._ctx is not None:
            self._ctx.term()
            self._ctx = None

    def login(self) -> None:
        """Drop the current session (if any) and log in again (handled on the next timer pass)."""
        self._pending_loss = "re-login requested"

    # ------------------------------------------------------------ orders
    def insert_order(self, order: RouteOrder) -> str:
        sess = self._require_session()
        ref = str(self._next_ref)
        req = self._next_req()
        try:
            msg = order_insert_msg(order, req_id=req, order_ref=ref)
        except WireError as exc:
            raise ValueError(str(exc)) from exc
        self._send_or_raise(msg)
        self._next_ref += 1
        self._track(self._inserts, req, order.client_order_id)
        self.stats.orders_sent += 1
        return make_broker_order_ref(sess.front_id, sess.session_id, ref)

    def cancel(self, client_order_id: str, *, broker_order_ref: str | None = None, order_sys_id: str | None = None) -> None:
        self._require_session()
        req = self._next_req()
        try:
            msg = order_action_msg(req, broker_order_ref=broker_order_ref, order_sys_id=order_sys_id)
        except WireError as exc:
            raise ValueError(str(exc)) from exc
        self._send_or_raise(msg)
        self._track(self._cancels, req, (client_order_id, broker_order_ref, order_sys_id))
        self.stats.cancels_sent += 1

    # ------------------------------------------------------------ queries
    def query_orders(self, trading_day: str, from_broker_seq: int, on_done: QueryDone[OrderReport]) -> None:
        self._query("orders", query_msg("ReqQryOrder", 0, trading_day=trading_day, from_broker_seq=from_broker_seq), on_done)

    def query_trades(self, trading_day: str, from_broker_seq: int, on_done: QueryDone[TradeReport]) -> None:
        self._query("trades", query_msg("ReqQryTrade", 0, trading_day=trading_day, from_broker_seq=from_broker_seq), on_done)

    def query_positions(
        self, trading_day: str, on_done: QueryDone[PositionReport], *, account_id: str | None = None
    ) -> None:
        msg = query_msg("ReqQryInvestorPosition", 0, trading_day=trading_day, investor_id=account_id)
        self._query("positions", msg, on_done)

    def _query(self, kind: str, msg: dict[str, Any], on_done: Callable[[QueryResult[Any]], None]) -> None:
        if self._phase != LOGGED_IN or self._sock is None:
            self._defer(lambda: on_done(QueryResult(error=BROKER_NOT_LOGGED_IN, message="broker not logged in")))
            return
        req = self._next_req()
        msg["req_id"] = req
        try:
            self._sock.send(_encoder.encode(msg), zmq.NOBLOCK)
        except zmq.ZMQError:
            self._pending_loss = "send failed"
            self._defer(lambda: on_done(QueryResult(error=codes.BROKER_DISCONNECTED, message="broker link down")))
            return
        self.stats.queries_sent += 1
        self._queries[req] = _Query(kind, on_done, self._clock.monotonic_ns() + self._query_timeout_ns)

    # ------------------------------------------------------------ loop integration
    def pump(self) -> int:
        """Read every pending frame and dispatch it (callbacks run here)."""
        sock = self._sock
        if sock is None:
            return 0
        n = 0
        while self._sock is sock:
            try:
                frames = sock.recv_multipart(zmq.NOBLOCK)
            except zmq.Again:
                break
            except zmq.ZMQError:
                break
            n += 1
            self.stats.messages += 1
            self._last_rx = self._clock.monotonic_ns()
            try:
                msg = msgspec.json.decode(frames[-1])
            except (msgspec.DecodeError, IndexError):
                self.stats.bad_frames += 1
                logger.warning("broker.bad_frame", broker="sim")
                continue
            if not isinstance(msg, dict):
                self.stats.bad_frames += 1
                continue
            self._dispatch(msg)
        return n

    def poll_timers(self, now_mono_ns: int) -> None:
        if not self._started:
            return
        now = now_mono_ns
        self._run_deferred()
        if self._pending_loss is not None:
            reason, self._pending_loss = self._pending_loss, None
            if self._phase != IDLE:
                self._lose(reason, now)
            elif self._attempt_at is None:
                self._attempt_at = now
        if self._phase == IDLE:
            if self._attempt_at is not None and now >= self._attempt_at:
                self._connect(now)
        elif self._phase == CONNECTING:
            if not self._try_send_login(now) and self._phase_deadline is not None and now >= self._phase_deadline:
                self._fail_attempt("connect timeout", now)
        elif self._phase == LOGIN_SENT:
            if self._phase_deadline is not None and now >= self._phase_deadline:
                self._fail_attempt("login timeout", now)
        elif self._phase == LOGGED_IN and now - self._last_rx > self._hb_timeout_ns:
            self._lose("heartbeat timeout", now)
        for req, q in list(self._queries.items()):
            if now >= q.deadline_ns:
                del self._queries[req]
                q.result.error = QUERY_TIMEOUT
                q.result.message = f"{q.kind} query timed out"
                q.on_done(q.result)

    def next_timer_ns(self) -> int | None:
        now = self._clock.monotonic_ns()
        if not self._started:
            return None
        if self._deferred or self._pending_loss is not None:
            return now
        times: list[int] = [q.deadline_ns for q in self._queries.values()]
        if self._phase == IDLE and self._attempt_at is not None:
            times.append(self._attempt_at)
        elif self._phase == CONNECTING:
            times.append(now + _CONNECT_POLL_NS)
        elif self._phase == LOGIN_SENT and self._phase_deadline is not None:
            times.append(self._phase_deadline)
        elif self._phase == LOGGED_IN:
            times.append(self._last_rx + self._hb_timeout_ns + _NS_PER_MS)
        return min(times) if times else None

    # ------------------------------------------------------------ internals
    def _next_req(self) -> int:
        self._req_id += 1
        return self._req_id

    @staticmethod
    def _track(table: dict[int, Any], req: int, value: Any) -> None:
        table[req] = value
        if len(table) > _MAX_TRACKED:  # bounded: drop the oldest correlation entries
            for key in list(table)[: len(table) - _MAX_TRACKED]:
                del table[key]

    def _defer(self, fn: Callable[[], None]) -> None:
        self._deferred.append(fn)

    def _run_deferred(self) -> None:
        while self._deferred:
            batch, self._deferred = self._deferred, []
            for fn in batch:
                fn()

    def _require_session(self) -> BrokerSession:
        if self._phase != LOGGED_IN or self._session is None or self._sock is None:
            raise BrokerUnavailable(f"broker {self.state.value.lower()}")
        return self._session

    def _send_or_raise(self, msg: dict[str, Any]) -> None:
        assert self._sock is not None
        try:
            self._sock.send(_encoder.encode(msg), zmq.NOBLOCK)
        except zmq.ZMQError as exc:  # IMMEDIATE=1: no connected peer -> nothing is queued
            self._pending_loss = "send failed"
            raise BrokerUnavailable(f"broker link down: {exc}") from exc

    def _emit_state(self) -> None:
        public = _PUBLIC[self._phase]
        if public != self._emitted:
            self._emitted = public
            self._cb.on_status(public, self._session if public == BrokerConnState.LOGGED_IN else None)

    def _close_socket(self) -> None:
        sock, self._sock = self._sock, None
        if sock is not None:
            if self._on_socket is not None:
                try:
                    self._on_socket(sock, False)
                except Exception:
                    logger.exception("broker.socket_hook_failed")
            sock.close(0)

    def _connect(self, now: int) -> None:
        if self._ctx is None:
            self._ctx = zmq.Context()
        self._close_socket()
        sock: zmq.Socket[Any] = self._ctx.socket(zmq.DEALER)
        sock.setsockopt(zmq.IDENTITY, self.identity.encode("utf-8"))
        sock.setsockopt(zmq.IMMEDIATE, 1)
        sock.setsockopt(zmq.LINGER, 0)
        sock.setsockopt(zmq.RECONNECT_IVL, 100)
        sock.setsockopt(zmq.RECONNECT_IVL_MAX, 1_000)
        sock.connect(self.endpoint)
        self._sock = sock
        if self._on_socket is not None:
            self._on_socket(sock, True)
        self._phase = CONNECTING
        self._attempt_at = None
        self._phase_deadline = now + self._login_timeout_ns
        logger.debug("broker.connecting", broker="sim", endpoint=self.endpoint)
        self._try_send_login(now)

    def _try_send_login(self, now: int) -> bool:
        sock = self._sock
        if sock is None or self._phase != CONNECTING:
            return False
        if not sock.poll(0, zmq.POLLOUT):
            return False
        req = self._next_req()
        data = _encoder.encode(login_msg(req, broker_id=self._broker_id, user_id=self._user_id, app_id=self._app_id))
        try:
            sock.send(data, zmq.NOBLOCK)
        except zmq.ZMQError:
            return False
        self._login_req = req
        self._phase = LOGIN_SENT
        self._phase_deadline = now + self._login_timeout_ns
        self._emit_state()
        return True

    def _fail_attempt(self, reason: str, now: int) -> None:
        self.stats.login_failures += 1
        delay = int(self._backoff_ns * (0.8 + 0.4 * self._rng.random()))
        self._backoff_ns = min(self._backoff_max_ns, self._backoff_ns * 2)
        logger.warning("broker.login_failed", broker="sim", endpoint=self.endpoint, reason=reason, retry_ms=delay // _NS_PER_MS)
        self._close_socket()
        self._session = None
        self._login_req = None
        self._phase = IDLE
        self._phase_deadline = None
        self._attempt_at = now + delay
        self._emit_state()

    def _lose(self, reason: str, now: int) -> None:
        was_logged_in = self._phase == LOGGED_IN
        if was_logged_in:
            self.stats.disconnects += 1
        logger.warning("broker.link_lost", broker="sim", reason=reason, logged_in=was_logged_in)
        self._close_socket()
        self._session = None
        self._login_req = None
        self._phase = IDLE
        self._phase_deadline = None
        self._attempt_at = now + (self._backoff_initial_ns if was_logged_in else self._backoff_ns)
        self._inserts.clear()
        self._cancels.clear()
        self._emit_state()
        pending, self._queries = self._queries, {}
        for q in pending.values():
            q.result.error = codes.BROKER_DISCONNECTED
            q.result.message = f"broker link lost: {reason}"
            q.on_done(q.result)

    # ------------------------------------------------------------ dispatch
    def _dispatch(self, msg: dict[str, Any]) -> None:
        kind = msg.get("msg")
        if kind == "RtnOrder":
            self._report(order_or_invalid(msg))
        elif kind == "RtnTrade":
            self._report(trade_or_invalid(msg))
        elif kind == "Heartbeat":
            self._on_heartbeat(msg)
        elif kind == "RspUserLogin":
            self._on_login(msg)
        elif kind in ("RspQryOrder", "RspQryTrade", "RspQryInvestorPosition"):
            self._on_query_page(msg)
        elif kind == "ErrRtnOrderInsert":
            self._on_insert_error(msg)
        elif kind == "ErrRtnOrderAction":
            self._on_action_error(msg)
        elif kind == "RspError":
            self._on_rsp_error(msg)
        else:
            logger.debug("broker.unknown_msg", broker="sim", msg=kind)

    def _report(self, report: OrderReport | TradeReport | InvalidReport) -> None:
        if isinstance(report, InvalidReport):
            self.stats.invalid_reports += 1
            logger.error("broker.invalid_report", broker="sim", kind=report.kind, reason=report.reason,
                         broker_seq=report.broker_seq, trading_day=report.trading_day)
            self._cb.on_invalid(report)
        elif isinstance(report, OrderReport):
            self._cb.on_order(report)
        else:
            self._cb.on_trade(report)

    def _on_heartbeat(self, msg: dict[str, Any]) -> None:
        self.stats.heartbeats += 1
        head = msg.get("broker_seq_head")
        td = msg.get("trading_day")
        if isinstance(head, int) and not isinstance(head, bool):
            self.broker_seq_head = head
        if isinstance(td, str) and len(td) == 8:
            self.broker_trading_day = td
        if self._phase == LOGGED_IN and isinstance(td, str) and len(td) == 8 and isinstance(head, int):
            self._cb.on_heartbeat(td, head)

    def _on_login(self, msg: dict[str, Any]) -> None:
        if self._phase != LOGIN_SENT or msg.get("req_id") != self._login_req:
            logger.debug("broker.stale_login_reply", broker="sim")
            return
        now = self._clock.monotonic_ns()
        err = msg.get("error")
        if err:
            detail = err.get("error_id") if isinstance(err, dict) else str(err)
            self._fail_attempt(f"{BROKER_LOGIN_FAILED}: {detail}", now)
            return
        try:
            td = str(msg["trading_day"])
            front_id = int(msg["front_id"])
            session_id = int(msg["session_id"])
            max_ref = str(msg.get("max_order_ref") or "0")
            head = int(msg.get("broker_seq_head") or 0)
        except (KeyError, TypeError, ValueError):
            self._fail_attempt("malformed RspUserLogin", now)
            return
        self._session = BrokerSession(
            trading_day=td,
            front_id=front_id,
            session_id=session_id,
            max_order_ref=max_ref,
            broker_seq_head=head,
            login_ns=now,
        )
        self._next_ref = int(max_ref) + 1 if max_ref.isdigit() else 1
        self.broker_trading_day = td
        self.broker_seq_head = head
        self._phase = LOGGED_IN
        self._phase_deadline = None
        self._last_rx = now
        self._backoff_ns = self._backoff_initial_ns
        self.stats.logins += 1
        logger.info("broker.logged_in", broker="sim", trading_day=td, front_id=front_id, session_id=session_id,
                    broker_seq_head=head, max_order_ref=max_ref)
        self._emit_state()

    def _on_query_page(self, msg: dict[str, Any]) -> None:
        req = msg.get("req_id")
        q = self._queries.get(req) if isinstance(req, int) else None
        if q is None:
            logger.debug("broker.late_query_page", broker="sim", req_id=req)
            return
        items = msg.get("items") or []
        for raw in items if isinstance(items, list) else []:
            if not isinstance(raw, dict):
                continue
            if q.kind == "orders":
                rep: Any = order_or_invalid(raw)
            elif q.kind == "trades":
                rep = trade_or_invalid(raw)
            else:
                try:
                    q.result.items.append(parse_position(raw))
                except WireError as exc:
                    self.stats.invalid_reports += 1
                    logger.error("broker.invalid_position", broker="sim", reason=str(exc))
                continue
            if isinstance(rep, InvalidReport):
                self.stats.invalid_reports += 1
                logger.error("broker.invalid_report", broker="sim", kind=rep.kind, reason=rep.reason,
                             broker_seq=rep.broker_seq, source="query")
                q.result.invalid.append(rep)
            else:
                q.result.items.append(rep)
        if msg.get("is_last", True):
            del self._queries[req]  # type: ignore[arg-type]
            q.on_done(q.result)

    def _on_insert_error(self, msg: dict[str, Any]) -> None:
        req = msg.get("req_id")
        coid = msg.get("client_order_id") or (self._inserts.pop(req, None) if isinstance(req, int) else None)
        if isinstance(req, int):
            self._inserts.pop(req, None)
        err = InsertError(
            client_order_id=coid if isinstance(coid, str) and coid else None,
            order_ref=str(msg["order_ref"]) if msg.get("order_ref") is not None else None,
            order_sys_id=str(msg["order_sys_id"]) if msg.get("order_sys_id") else None,
            error_id=str(msg.get("error_id") or "BROKER_REJECT"),
            error_msg=str(msg.get("error_msg") or ""),
        )
        if err.error_id == BROKER_NOT_LOGGED_IN:
            self._pending_loss = "broker session lost (BROKER_NOT_LOGGED_IN)"
        self._cb.on_insert_error(err)

    def _on_action_error(self, msg: dict[str, Any]) -> None:
        req = msg.get("req_id")
        tracked = self._cancels.pop(req, None) if isinstance(req, int) else None
        coid, ref, sys_id = tracked if tracked is not None else (None, None, None)
        err = ActionError(
            client_order_id=coid,
            order_sys_id=str(msg["order_sys_id"]) if msg.get("order_sys_id") else sys_id,
            broker_order_ref=ref,
            error_id=str(msg.get("error_id") or "BROKER_ACTION_REJECTED"),
            error_msg=str(msg.get("error_msg") or ""),
        )
        if err.error_id == BROKER_NOT_LOGGED_IN:
            self._pending_loss = "broker session lost (BROKER_NOT_LOGGED_IN)"
        self._cb.on_action_error(err)

    def _on_rsp_error(self, msg: dict[str, Any]) -> None:
        req = msg.get("req_id")
        error_id = str(msg.get("error_id") or codes.BAD_REQUEST)
        text = str(msg.get("error_msg") or "")
        logger.warning("broker.rsp_error", broker="sim", req_id=req, error_id=error_id, error_msg=text)
        q = self._queries.pop(req, None) if isinstance(req, int) else None
        if q is not None:
            q.result.error = error_id
            q.result.message = text
            q.on_done(q.result)
        if isinstance(req, int) and req == self._login_req and self._phase == LOGIN_SENT:
            self._fail_attempt(f"{error_id}: {text}", self._clock.monotonic_ns())
        elif error_id == BROKER_NOT_LOGGED_IN and self._phase == LOGGED_IN:
            self._pending_loss = "broker session lost (BROKER_NOT_LOGGED_IN)"


__all__ = ["AdapterStats", "SimBrokerAdapter"]
