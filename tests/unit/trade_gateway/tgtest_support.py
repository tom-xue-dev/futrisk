"""Helpers for the trade-gateway tests: fake broker adapter, core harness, report builders."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from futrisk.common.clock import BJ, SimClock, bj_to_ns
from futrisk.common.config import FullConfig
from futrisk.db import Database, Outbox, OutboxRow
from futrisk.messages import codes
from futrisk.messages.base import Msg, decode_named
from futrisk.messages.enums import (
    BrokerConnState,
    BrokerOrderStatus,
    Direction,
    Exchange,
    Offset,
    OrderType,
    TimeInForce,
)
from futrisk.messages.envelope import Reply
from futrisk.messages.orders import BrokerStatus, RouteOrder
from futrisk.services.trade_gateway.broker.base import (
    ActionError,
    BrokerCallbacks,
    BrokerSession,
    BrokerUnavailable,
    OrderReport,
    PositionReport,
    QueryDone,
    QueryResult,
    TradeReport,
    make_broker_order_ref,
    parse_broker_order_ref,
)
from futrisk.services.trade_gateway.core import TradeGatewayCore
from futrisk.services.trade_gateway.schema import METADATA, MIGRATIONS

TD = "20260928"
T0 = bj_to_ns(datetime(2026, 9, 28, 9, 35, tzinfo=BJ))
MS = 1_000_000
SEC = 1_000_000_000


class FakeBroker:
    """In-memory ``BrokerAdapter``: records requests, the test drives callbacks explicitly."""

    def __init__(self) -> None:
        self._state = BrokerConnState.DISCONNECTED
        self._session: BrokerSession | None = None
        self.cb: BrokerCallbacks | None = None
        self.inserts: list[RouteOrder] = []
        self.refs: list[str] = []
        self.cancels: list[tuple[str, str | None, str | None]] = []
        self.queries: list[tuple[str, str, Any, QueryDone[Any]]] = []
        self.fail_insert = False
        self.fail_cancel = False
        self._next_ref = 1

    # ---- BrokerAdapter
    @property
    def broker(self) -> str:
        return "sim"

    @property
    def state(self) -> BrokerConnState:
        return self._state

    @property
    def session(self) -> BrokerSession | None:
        return self._session

    def bind(self, callbacks: BrokerCallbacks) -> None:
        self.cb = callbacks

    def start(self) -> None:
        pass

    def stop(self) -> None:
        pass

    def login(self) -> None:
        pass

    def insert_order(self, order: RouteOrder) -> str:
        if self._state is not BrokerConnState.LOGGED_IN or self.fail_insert or self._session is None:
            raise BrokerUnavailable("fake broker down")
        ref = make_broker_order_ref(self._session.front_id, self._session.session_id, str(self._next_ref))
        self._next_ref += 1
        self.inserts.append(order)
        self.refs.append(ref)
        return ref

    def cancel(self, client_order_id: str, *, broker_order_ref: str | None = None, order_sys_id: str | None = None) -> None:
        if self._state is not BrokerConnState.LOGGED_IN or self.fail_cancel:
            raise BrokerUnavailable("fake broker down")
        if not order_sys_id and (broker_order_ref is None or parse_broker_order_ref(broker_order_ref) is None):
            raise ValueError("a cancel needs broker_order_ref or order_sys_id")  # like the real adapter's wire check
        self.cancels.append((client_order_id, broker_order_ref, order_sys_id))

    def query_orders(self, trading_day: str, from_broker_seq: int, on_done: QueryDone[OrderReport]) -> None:
        self.queries.append(("orders", trading_day, from_broker_seq, on_done))

    def query_trades(self, trading_day: str, from_broker_seq: int, on_done: QueryDone[TradeReport]) -> None:
        self.queries.append(("trades", trading_day, from_broker_seq, on_done))

    def query_positions(
        self, trading_day: str, on_done: QueryDone[PositionReport], *, account_id: str | None = None
    ) -> None:
        self.queries.append(("positions", trading_day, account_id, on_done))

    def pump(self) -> int:
        return 0

    def poll_timers(self, now_mono_ns: int) -> None:
        pass

    def next_timer_ns(self) -> int | None:
        return None

    # ---- test drivers
    def login_ok(self, trading_day: str = TD, session_id: int = 7, head: int = 0) -> BrokerSession:
        assert self.cb is not None
        self._state = BrokerConnState.LOGGED_IN
        self._session = BrokerSession(trading_day=trading_day, front_id=1, session_id=session_id, max_order_ref="0",
                                      broker_seq_head=head)
        self.cb.on_status(BrokerConnState.LOGGED_IN, self._session)
        return self._session

    def drop(self) -> None:
        """Lose the session like the real adapter: state change first, then every pending query fails."""
        assert self.cb is not None
        self._state = BrokerConnState.DISCONNECTED
        self._session = None
        self.cb.on_status(BrokerConnState.DISCONNECTED, None)
        pending, self.queries = self.queries, []
        for q in pending:
            q[3](QueryResult(items=[], error=codes.BROKER_DISCONNECTED, message="broker link lost"))

    def pending(self, kind: str) -> list[tuple[str, str, Any, QueryDone[Any]]]:
        return [q for q in self.queries if q[0] == kind]

    def answer(self, kind: str, items: list[Any] | None = None, *, error: str | None = None) -> tuple[str, Any]:
        """Complete the oldest pending query of ``kind``; returns its ``(trading_day, from/account)``."""
        for i, q in enumerate(self.queries):
            if q[0] == kind:
                del self.queries[i]
                q[3](QueryResult(items=list(items or []), error=error))
                return q[1], q[2]
        raise AssertionError(f"no pending {kind} query")

    def answer_resync(self, orders: list[OrderReport] | None = None, trades: list[TradeReport] | None = None) -> None:
        """Answer one resync step: the core asks for orders first, then trades (02 §9.4)."""
        assert [q[0] for q in self.queries[:2]] == ["orders", "trades"], [q[:3] for q in self.queries]
        self.answer("orders", orders or [])
        self.answer("trades", trades or [])

    def refuse_cancel(self, client_order_id: str, error_id: str) -> None:
        """Deliver an ``ErrRtnOrderAction`` for a cancel of ``client_order_id``."""
        assert self.cb is not None
        self.cb.on_action_error(ActionError(client_order_id=client_order_id, order_sys_id=None, broker_order_ref=None,
                                            error_id=error_id, error_msg=error_id.lower()))


class Capture:
    """Outbox sink recording committed FACT rows (``td.exec.*``)."""

    def __init__(self) -> None:
        self.rows: list[OutboxRow] = []

    def __call__(self, rows: list[OutboxRow]) -> None:
        self.rows.extend(rows)

    def messages(self, topic: str | None = None) -> list[tuple[str, int, Msg]]:
        return [(r.topic, r.topic_seq, decode_named(r.payload, r.type)) for r in self.rows if topic is None or r.topic == topic]

    def types(self, topic: str) -> list[str]:
        return [type(m).__name__ for _, _, m in self.messages(topic)]


@dataclass
class Harness:
    """A core over a real td.db with a fake broker and a controllable clock."""

    core: TradeGatewayCore
    db: Database
    broker: FakeBroker
    clock: SimClock
    capture: Capture
    statuses: list[BrokerStatus] = field(default_factory=list)
    replies: list[Reply] = field(default_factory=list)

    def advance(self, ns: int) -> None:
        self.clock.advance(ns)

    def rows(self, sql: str, *params: Any) -> list[tuple[Any, ...]]:
        return self.db.read(lambda c: [tuple(r) for r in c.exec_driver_sql(sql, params).fetchall()])

    def scalar(self, sql: str, *params: Any) -> Any:
        rows = self.rows(sql, *params)
        return rows[0][0] if rows else None

    def tick(self) -> None:
        self.core.on_timer(self.clock.monotonic_ns())


def open_db(path: Path, clock: SimClock) -> Database:
    db = Database(path, METADATA, name="td", synchronous="FULL", migrations=MIGRATIONS, clock=clock)
    db.open()
    return db


def make_harness(path: Path, config: FullConfig, *, clock: SimClock | None = None, broker: FakeBroker | None = None,
                 max_resync_buffer: int = 50_000) -> Harness:
    clock = clock or SimClock(T0)
    db = open_db(path, clock)
    outbox = Outbox(db, instance="trade-gateway")
    capture = Capture()
    outbox.attach_sink(capture).result()

    def append_fact(conn: Any, topic: str, body: Msg, *, td: str | None = None, corr_id: str | None = None) -> int:
        return outbox.append_msg(conn, topic, body, epoch="test-epoch", created_ns=clock.now_ns(), td=td, corr_id=corr_id)

    broker = broker or FakeBroker()
    statuses: list[BrokerStatus] = []
    core = TradeGatewayCore(db=db, adapter=broker, clock=clock, calendar=config.calendar, append_fact=append_fact,
                            publish_status=statuses.append, max_resync_buffer=max_resync_buffer)
    broker.bind(core)
    core.load()
    return Harness(core=core, db=db, broker=broker, clock=clock, capture=capture, statuses=statuses)


def route(
    coid: str,
    *,
    account: str = "ACC3",
    instrument: str = "rb2701",
    exchange: Exchange = Exchange.SHFE,
    direction: Direction = Direction.BUY,
    offset: Offset = Offset.OPEN,
    qty: int = 1,
    price: float | None = 3120.0,
    order_type: OrderType = OrderType.LIMIT,
    tif: TimeInForce = TimeInForce.GFD,
    now: int = T0,
    ttl_ns: int = 2 * SEC,
    src: str = "risk-engine-1",
) -> RouteOrder:
    return RouteOrder(
        client_order_id=coid,
        account_id=account,
        instrument_id=instrument,
        exchange=exchange,
        direction=direction,
        offset=offset,
        order_type=order_type,
        tif=tif,
        price=price,
        qty=qty,
        trading_day=TD,
        risk_passed_ns=now,
        expire_ns=now + ttl_ns,
        src_instance=src,
    )


def order_report(
    seq: int,
    coid: str | None,
    status: BrokerOrderStatus,
    *,
    filled: int = 0,
    qty: int = 1,
    account: str = "ACC3",
    instrument: str = "rb2701",
    exchange: Exchange = Exchange.SHFE,
    direction: Direction = Direction.BUY,
    offset: Offset = Offset.OPEN,
    session_id: int = 7,
    order_ref: str = "1",
    sys_id: str | None = "0000000001",
    status_msg: str = "",
    td: str = TD,
) -> OrderReport:
    return OrderReport(
        broker_seq=seq,
        trading_day=td,
        front_id=1,
        session_id=session_id,
        order_ref=order_ref,
        order_sys_id=sys_id,
        client_order_id=coid,
        account_id=account,
        instrument_id=instrument,
        exchange=exchange,
        direction=direction,
        offset=offset,
        order_type=OrderType.LIMIT,
        tif=TimeInForce.GFD,
        price=3120.0,
        qty=qty,
        filled_qty=filled,
        remaining_qty=qty - filled,
        status=status,
        status_msg=status_msg or status.value,
        update_ns=T0 + seq * MS,
    )


def trade_report(
    seq: int,
    coid: str | None,
    trade_id: str,
    *,
    qty: int = 1,
    price: float = 3119.0,
    account: str = "ACC3",
    instrument: str = "rb2701",
    exchange: Exchange = Exchange.SHFE,
    direction: Direction = Direction.BUY,
    offset: Offset = Offset.OPEN,
    session_id: int = 7,
    order_ref: str = "1",
    sys_id: str | None = "0000000001",
    td: str = TD,
) -> TradeReport:
    return TradeReport(
        broker_seq=seq,
        trading_day=td,
        trade_id=trade_id,
        front_id=1,
        session_id=session_id,
        order_ref=order_ref,
        order_sys_id=sys_id,
        client_order_id=coid,
        account_id=account,
        instrument_id=instrument,
        exchange=exchange,
        direction=direction,
        offset=offset,
        price=price,
        qty=qty,
        trade_ts_ns=T0 + seq * MS,
    )


def logged_in(h: Harness, *, head: int = 0, session_id: int = 7) -> None:
    """Log the fake broker in and complete the (empty) login resync."""
    h.broker.login_ok(head=head, session_id=session_id)
    h.broker.answer_resync()
    assert not h.core.resyncing and h.core.ready


__all__ = [
    "MS",
    "SEC",
    "T0",
    "TD",
    "Capture",
    "FakeBroker",
    "Harness",
    "logged_in",
    "make_harness",
    "open_db",
    "order_report",
    "route",
    "trade_report",
]
