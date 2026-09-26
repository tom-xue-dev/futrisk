"""``BrokerAdapter`` interface of trade-gateway and the normalised broker events (02 §10, 01 §5.3.3).

02 §10 lists the adapter surface: ``login()``, ``insert_order(RouteOrder)``,
``cancel(client_order_id)``, ``query_orders(td, from_broker_seq)``,
``query_trades(td, from_broker_seq)``, ``query_positions(td)`` and the callbacks
``on_order``, ``on_trade``, ``on_status``. The concrete shape used here:

* the adapter is **driven by the service's business thread** (02 §8.1): its socket
  sits in the loop's poller, :meth:`BrokerAdapter.pump` reads what arrived and
  :meth:`BrokerAdapter.poll_timers` runs deadlines (login, heartbeat liveness,
  reconnect backoff, query timeouts). Nothing ever blocks and **no callback is ever
  invoked from inside an API call** (``insert_order`` & co.), only from ``pump`` /
  ``poll_timers``, so callers never see re-entrant callbacks;
* callbacks (:class:`BrokerCallbacks`) receive already-validated events. Reports with
  unknown enum values are turned into :class:`InvalidReport` by the adapter and never
  reach the database (02 §9.4 映射到内部定义);
* queries are asynchronous: ``on_done(QueryResult)`` runs later from ``pump`` /
  ``poll_timers`` once every page arrived (or on error / timeout / disconnect).

Implementations register themselves with :func:`register_broker` (02 §10 注册表):
``sim`` (:class:`~futrisk.services.trade_gateway.broker.sim.SimBrokerAdapter`) and the
``ctp`` placeholder (:class:`~futrisk.services.trade_gateway.broker.ctp.CtpBrokerAdapter`).
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Generic, Protocol, TypeVar, runtime_checkable

from futrisk.messages.enums import (
    BrokerConnState,
    BrokerOrderStatus,
    Direction,
    Exchange,
    Offset,
    OrderType,
    PosDirection,
    TimeInForce,
    TradeOrigin,
)
from futrisk.messages.orders import RouteOrder

T = TypeVar("T")


class BrokerUnavailable(RuntimeError):
    """The broker link cannot take a request right now (not logged in, or the send failed)."""


def make_broker_order_ref(front_id: int, session_id: int, order_ref: str) -> str:
    """``{front_id}:{session_id}:{order_ref}`` (02 §7.4 ``BrokerOrderUpdate.broker_order_ref``)."""
    return f"{front_id}:{session_id}:{order_ref}"


def parse_broker_order_ref(ref: str) -> tuple[int, int, str] | None:
    """Inverse of :func:`make_broker_order_ref`; ``None`` when ``ref`` is not of that form."""
    parts = ref.split(":", 2)
    if len(parts) != 3:
        return None
    try:
        return int(parts[0]), int(parts[1]), parts[2]
    except ValueError:
        return None


@dataclass(frozen=True, slots=True)
class BrokerSession:
    """A logged-in broker session (``RspUserLogin``)."""

    trading_day: str
    front_id: int
    session_id: int
    max_order_ref: str
    broker_seq_head: int
    login_ns: int = 0


@dataclass(frozen=True, slots=True)
class OrderReport:
    """An order report (``RtnOrder`` or an item of ``RspQryOrder``), validated and mapped."""

    broker_seq: int
    trading_day: str
    front_id: int
    session_id: int
    order_ref: str
    order_sys_id: str | None
    client_order_id: str | None
    account_id: str
    instrument_id: str
    exchange: Exchange
    direction: Direction
    offset: Offset
    order_type: OrderType
    tif: TimeInForce
    price: float | None
    qty: int
    filled_qty: int
    remaining_qty: int
    status: BrokerOrderStatus
    status_msg: str = ""
    update_ns: int = 0

    @property
    def broker_order_ref(self) -> str:
        return make_broker_order_ref(self.front_id, self.session_id, self.order_ref)

    @property
    def origin(self) -> TradeOrigin:
        """``EXTERNAL`` when the broker order has no ``client_order_id`` (02 §9.4)."""
        return TradeOrigin.INTERNAL if self.client_order_id else TradeOrigin.EXTERNAL


@dataclass(frozen=True, slots=True)
class TradeReport:
    """A trade report (``RtnTrade`` or an item of ``RspQryTrade``), validated and mapped."""

    broker_seq: int
    trading_day: str
    trade_id: str
    front_id: int
    session_id: int
    order_ref: str
    order_sys_id: str | None
    client_order_id: str | None
    account_id: str
    instrument_id: str
    exchange: Exchange
    direction: Direction
    offset: Offset
    price: float
    qty: int
    trade_ts_ns: int

    @property
    def broker_order_ref(self) -> str:
        return make_broker_order_ref(self.front_id, self.session_id, self.order_ref)

    @property
    def origin(self) -> TradeOrigin:
        """``EXTERNAL`` for broker force closes / terminal orders without ``client_order_id`` (02 §9.4)."""
        return TradeOrigin.INTERNAL if self.client_order_id else TradeOrigin.EXTERNAL

    @property
    def trade_key(self) -> tuple[str, str, str, str]:
        """``(trading_day, exchange, trade_id, direction)`` (02 §7.5)."""
        return (self.trading_day, self.exchange.value, self.trade_id, self.direction.value)


@dataclass(frozen=True, slots=True)
class InvalidReport:
    """A report the adapter refused (unknown enum value, missing field): logged, counted, never stored.

    Its ``broker_seq`` (when readable) is still consumed so that resync does not
    fetch it over and over.
    """

    kind: str  # "order" | "trade"
    reason: str
    broker_seq: int | None = None
    trading_day: str | None = None
    raw: dict[str, Any] = field(default_factory=dict)


BrokerReport = OrderReport | TradeReport | InvalidReport


@dataclass(frozen=True, slots=True)
class PositionReport:
    """One ``RspQryInvestorPosition`` item (02 §9.4)."""

    account_id: str
    instrument_id: str
    direction: PosDirection
    td_qty: int
    yd_qty: int


@dataclass(frozen=True, slots=True)
class InsertError:
    """``ErrRtnOrderInsert``. ``order_sys_id`` is set when the broker also created a REJECTED order
    (then a ``RtnOrder`` with a ``broker_seq`` carries the status)."""

    client_order_id: str | None
    order_ref: str | None
    order_sys_id: str | None
    error_id: str
    error_msg: str


@dataclass(frozen=True, slots=True)
class ActionError:
    """``ErrRtnOrderAction`` (cancel refused), mapped back to the order it targeted."""

    client_order_id: str | None
    order_sys_id: str | None
    broker_order_ref: str | None
    error_id: str
    error_msg: str


@dataclass
class QueryResult(Generic[T]):
    """Outcome of an asynchronous broker query (all pages)."""

    items: list[T] = field(default_factory=list)
    invalid: list[InvalidReport] = field(default_factory=list)
    error: str | None = None
    message: str | None = None

    @property
    def ok(self) -> bool:
        return self.error is None


QueryDone = Callable[[QueryResult[T]], None]


class BrokerCallbacks(Protocol):
    """Events delivered by an adapter (always on the thread that drives it)."""

    def on_status(self, state: BrokerConnState, session: BrokerSession | None) -> None:
        """Connection state changed (``LOGGED_IN`` carries the new session)."""

    def on_order(self, report: OrderReport) -> None:
        """A live order report."""

    def on_trade(self, report: TradeReport) -> None:
        """A live trade report."""

    def on_invalid(self, report: InvalidReport) -> None:
        """A live report refused by the adapter."""

    def on_insert_error(self, error: InsertError) -> None:
        """An order insert was refused."""

    def on_action_error(self, error: ActionError) -> None:
        """A cancel was refused."""

    def on_heartbeat(self, trading_day: str, broker_seq_head: int) -> None:
        """Broker heartbeat: current trading day and the highest ``broker_seq`` issued."""


@runtime_checkable
class BrokerAdapter(Protocol):
    """Broker connection used by trade-gateway (02 §10); see the module docstring."""

    @property
    def broker(self) -> str:
        """``sim`` or ``ctp`` (``BrokerStatus.broker``, ``routed_orders.broker``)."""
        ...

    @property
    def state(self) -> BrokerConnState:
        """``DISCONNECTED``, ``CONNECTED`` (login in flight) or ``LOGGED_IN``."""
        ...

    @property
    def session(self) -> BrokerSession | None:
        """The logged-in session, if any."""
        ...

    def bind(self, callbacks: BrokerCallbacks) -> None:
        """Set the event receiver (before :meth:`start`)."""
        ...

    def start(self) -> None:
        """Start connecting and logging in (non-blocking)."""
        ...

    def stop(self) -> None:
        """Close the connection; pending queries complete with an error."""
        ...

    def login(self) -> None:
        """(Re)connect and log in now (drops the current session)."""
        ...

    def insert_order(self, order: RouteOrder) -> str:
        """Send an order; returns its ``broker_order_ref``. Raises :class:`BrokerUnavailable`."""
        ...

    def cancel(self, client_order_id: str, *, broker_order_ref: str | None = None, order_sys_id: str | None = None) -> None:
        """Ask the broker to cancel an order. Raises :class:`BrokerUnavailable`."""
        ...

    def query_orders(self, trading_day: str, from_broker_seq: int, on_done: QueryDone[OrderReport]) -> None:
        """Current state of every order whose latest report has ``broker_seq >= from_broker_seq``."""
        ...

    def query_trades(self, trading_day: str, from_broker_seq: int, on_done: QueryDone[TradeReport]) -> None:
        """Every trade with ``broker_seq >= from_broker_seq``."""
        ...

    def query_positions(
        self, trading_day: str, on_done: QueryDone[PositionReport], *, account_id: str | None = None
    ) -> None:
        """Broker positions (today / yesterday lots) of one or every account."""
        ...

    def pump(self) -> int:
        """Read and dispatch everything that arrived; returns the number of messages."""
        ...

    def poll_timers(self, now_mono_ns: int) -> None:
        """Deadlines: login / heartbeat liveness / reconnect backoff / query timeouts."""
        ...

    def next_timer_ns(self) -> int | None:
        """Monotonic time of the next deadline (sizes the poller timeout)."""
        ...


# ------------------------------------------------------------------ registry (02 §10)
BrokerFactory = Callable[..., BrokerAdapter]
_BROKERS: dict[str, BrokerFactory] = {}


def register_broker(kind: str) -> Callable[[BrokerFactory], BrokerFactory]:
    """Class decorator registering an adapter under ``kind`` (``@register_broker("sim")``)."""

    def deco(factory: BrokerFactory) -> BrokerFactory:
        existing = _BROKERS.get(kind)
        if existing is not None and existing is not factory:
            raise ValueError(f"broker kind {kind!r} registered twice")
        _BROKERS[kind] = factory
        return factory

    return deco


def broker_factory(kind: str) -> BrokerFactory:
    """The adapter factory registered for ``kind``."""
    try:
        return _BROKERS[kind]
    except KeyError:
        raise KeyError(f"unknown broker kind {kind!r}; registered: {sorted(_BROKERS)}") from None


def available_brokers() -> list[str]:
    return sorted(_BROKERS)


__all__ = [
    "ActionError",
    "BrokerAdapter",
    "BrokerCallbacks",
    "BrokerFactory",
    "BrokerReport",
    "BrokerSession",
    "BrokerUnavailable",
    "InsertError",
    "InvalidReport",
    "OrderReport",
    "PositionReport",
    "QueryDone",
    "QueryResult",
    "TradeReport",
    "available_brokers",
    "broker_factory",
    "make_broker_order_ref",
    "parse_broker_order_ref",
    "register_broker",
]
