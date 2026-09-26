"""Deterministic in-process broker: the backtest's trade-gateway on the :class:`InprocBus` (01 §8.16).

The engine's ``Route`` / ``CancelRoute`` / ``ResolveRoute`` effects become
``route.order`` / ``route.cancel`` / ``route.resolve`` requests to this
instance; executions come back as ``td.exec.{account}`` FACT messages
(``BrokerOrderUpdate`` and ``Trade``), exactly as the real trade-gateway
publishes them, and the replay shell feeds them to ``EngineCore.apply_exec``.

Fill model (documented in the backtest analysis):

- the broker knows the current market state of every contract (the last
  replayed price point and whether it is limit-locked);
- **first match** of a new order: a market order, or a limit order that is
  marketable against the current price P (buy ≥ P, sell ≤ P), fills **in full
  at P** - no slippage beyond the price point, no partial fills, no queue;
- a non-marketable limit order **rests** (GFD) and fills in full **at its
  own limit price** as soon as a later price point of that contract crosses it;
- **limit lock**: a state at the limit-down price has no bids (sells do not
  fill), a state at the limit-up price has no offers (buys do not fill); such
  orders rest and fill at their limit when a later state is no longer locked;
- orders still working at the end of the trading day are cancelled
  (``SESSION_CLOSE``), as the exchanges do with GFD orders;
- trade ids, order refs and broker sequence numbers are per-day counters, so a
  replay is deterministic.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field

from futrisk.bus.inproc import InprocEndpoint
from futrisk.bus.rpc_common import RequestContext
from futrisk.common.clock import Clock
from futrisk.core.instruments import InstrumentRegistry
from futrisk.messages import codes, topics
from futrisk.messages.base import Msg
from futrisk.messages.enums import BrokerOrderStatus, Direction, OrderType, TradeOrigin
from futrisk.messages.envelope import Reply, err_reply, ok_reply
from futrisk.messages.orders import BrokerOrderUpdate, RouteAck, RouteCancel, RouteOrder, RouteResolve, Trade

SESSION_CLOSE = "SESSION_CLOSE"
USER_CANCELED = "USER_CANCELED"
EPS = 1e-9


@dataclass
class MarketState:
    price: float
    ts_ns: int
    locked_up: bool = False
    locked_down: bool = False


@dataclass
class BrokerOrder:
    route: RouteOrder
    ref: str
    seq_no: int
    status: BrokerOrderStatus = BrokerOrderStatus.RECEIVED
    filled: int = 0
    first_match_done: bool = False
    accepted_sent: bool = False

    @property
    def remaining(self) -> int:
        return self.route.qty - self.filled

    @property
    def live(self) -> bool:
        return not self.status.is_terminal


@dataclass
class BrokerStats:
    orders: int = 0
    fills: int = 0
    rested: int = 0
    locked_waits: int = 0
    session_cancels: int = 0
    user_cancels: int = 0


@dataclass
class InprocBroker:
    """The trade-gateway of one backtest arm (see the module docstring)."""

    endpoint: InprocEndpoint
    registry: InstrumentRegistry
    clock: Clock
    trading_day: str = ""
    market: dict[str, MarketState] = field(default_factory=dict)
    orders: dict[str, BrokerOrder] = field(default_factory=dict)
    # live orders per contract in submission order (matching touches only the contract that moved)
    by_instrument: dict[str, list[BrokerOrder]] = field(default_factory=dict)
    trades: list[Trade] = field(default_factory=list)  # today's trades in execution order
    stats: BrokerStats = field(default_factory=BrokerStats)
    on_trade: Callable[[Trade], None] | None = None
    _order_no: int = 0
    _trade_no: int = 0
    _seq: int = 0

    def __post_init__(self) -> None:
        self.endpoint.serve(
            {"route.order": self._route_order, "route.cancel": self._route_cancel, "route.resolve": self._route_resolve}
        )

    # ----------------------------------------------------------- day handling
    def begin_day(self, trading_day: str) -> None:
        self.trading_day = trading_day
        self.orders = {}
        self.by_instrument = {}
        self.trades = []
        self.market = {}
        self._order_no = 0
        self._trade_no = 0
        self._seq = 0

    def end_day(self) -> int:
        """Cancel every working order (GFD orders die at the close); returns the count."""
        n = 0
        for o in list(self.orders.values()):
            if o.live:
                o.status = BrokerOrderStatus.CANCELED
                self._publish_update(o, broker_code=SESSION_CLOSE)
                n += 1
        self.stats.session_cancels += n
        return n

    def set_market(self, instrument_id: str, price: float, ts_ns: int, *, locked_up: bool = False, locked_down: bool = False) -> None:
        self.market[instrument_id] = MarketState(price, ts_ns, locked_up, locked_down)

    # --------------------------------------------------------------- handlers
    def _route_order(self, command: str, payload: Msg, ctx: RequestContext) -> Reply:
        assert isinstance(payload, RouteOrder)
        o = self.orders.get(payload.client_order_id)
        if o is not None:
            return ok_reply(RouteAck(client_order_id=o.route.client_order_id, status=o.status, broker_order_ref=o.ref))
        if payload.trading_day != self.trading_day:
            return err_reply(codes.PRECONDITION_FAILED, f"broker trading day {self.trading_day}, order {payload.trading_day}")
        self._order_no += 1
        self.stats.orders += 1
        ref = f"{self.trading_day}-O{self._order_no:07d}"
        o = BrokerOrder(payload, ref, self._order_no)
        self.orders[payload.client_order_id] = o
        self.by_instrument.setdefault(payload.instrument_id, []).append(o)
        return ok_reply(RouteAck(client_order_id=payload.client_order_id, status=BrokerOrderStatus.RECEIVED, broker_order_ref=ref))

    def _route_cancel(self, command: str, payload: Msg, ctx: RequestContext) -> Reply:
        assert isinstance(payload, RouteCancel)
        o = self.orders.get(payload.client_order_id)
        if o is None:
            return err_reply(codes.BROKER_ORDER_NOT_FOUND, "order not found",
                             data=RouteAck(client_order_id=payload.client_order_id, status=BrokerOrderStatus.EXPIRED))
        if not o.live:
            return err_reply(codes.BROKER_ORDER_FINISHED, f"order is {o.status.value}")
        o.status = BrokerOrderStatus.CANCELED
        self.stats.user_cancels += 1
        self._publish_update(o, broker_code=USER_CANCELED)
        return ok_reply(RouteAck(client_order_id=o.route.client_order_id, status=BrokerOrderStatus.CANCEL_PENDING, broker_order_ref=o.ref))

    def _route_resolve(self, command: str, payload: Msg, ctx: RequestContext) -> Reply:
        assert isinstance(payload, RouteResolve)
        o = self.orders.get(payload.client_order_id)
        if o is None:
            return ok_reply(RouteAck(client_order_id=payload.client_order_id, status=BrokerOrderStatus.EXPIRED))
        return ok_reply(RouteAck(client_order_id=o.route.client_order_id, status=o.status, broker_order_ref=o.ref))

    # --------------------------------------------------------------- matching
    def match(self, instrument_id: str | None = None) -> int:
        """Match working orders (all, or of one contract) against the current market; returns fills."""
        if instrument_id is None:
            return sum(self.match(inst) for inst in sorted(self.by_instrument))
        queue = self.by_instrument.get(instrument_id)
        if not queue:
            return 0
        n = 0
        for o in list(queue):  # submission order; fills publish FACTs that may add orders (appended, next call)
            if not o.live:
                continue
            if not o.accepted_sent:
                o.accepted_sent = True
                o.status = BrokerOrderStatus.ACCEPTED
                self._publish_update(o)
            m = self.market.get(o.route.instrument_id)
            if m is None:
                o.first_match_done = True
                continue
            price = self._fill_price(o, m)
            first = not o.first_match_done
            o.first_match_done = True
            if price is None:
                if first:
                    self.stats.rested += 1
                continue
            self._fill(o, price, o.remaining)
            n += 1
        live = [o for o in self.by_instrument.get(instrument_id, []) if o.live]
        if live:
            self.by_instrument[instrument_id] = live
        else:
            self.by_instrument.pop(instrument_id, None)
        return n

    def _fill_price(self, o: BrokerOrder, m: MarketState) -> float | None:
        r = o.route
        buy = r.direction == Direction.BUY
        if (buy and m.locked_up) or (not buy and m.locked_down):
            self.stats.locked_waits += 1
            return None  # one-sided book at the limit: no liquidity for this side
        if r.order_type == OrderType.MARKET or r.price is None:
            return m.price
        marketable = r.price >= m.price - EPS if buy else r.price <= m.price + EPS
        if not marketable:
            return None
        if not o.first_match_done:
            return m.price  # aggressive first match: the current price point
        return r.price  # a resting order is filled at its own limit when the market crosses it

    def _fill(self, o: BrokerOrder, price: float, qty: int) -> None:
        r = o.route
        now = self.clock.now_ns()
        self._trade_no += 1
        self._seq += 1
        info = self.registry.get(r.instrument_id)
        t = Trade(
            trading_day=self.trading_day,
            exchange=info.exchange,
            trade_id=f"{self.trading_day}-T{self._trade_no:07d}",
            direction=r.direction,
            account_id=r.account_id,
            instrument_id=r.instrument_id,
            client_order_id=r.client_order_id,
            origin=TradeOrigin.INTERNAL,
            broker_order_ref=o.ref,
            offset=r.offset,
            price=price,
            qty=qty,
            trade_ts_ns=now,
            broker_seq=self._seq,
            trade_seq=self._trade_no,
            received_ns=now,
        )
        o.filled += qty
        o.status = BrokerOrderStatus.FILLED if o.remaining <= 0 else BrokerOrderStatus.PARTIALLY_FILLED
        self.trades.append(t)
        self.stats.fills += 1
        self.endpoint.publish(topics.td_exec(r.account_id), t, fact=True, td=self.trading_day, corr_id=r.client_order_id)
        if self.on_trade is not None:
            self.on_trade(t)
        self._publish_update(o)

    def _publish_update(self, o: BrokerOrder, *, broker_code: str | None = None) -> None:
        self._seq += 1
        r = o.route
        u = BrokerOrderUpdate(
            client_order_id=r.client_order_id,
            origin=TradeOrigin.INTERNAL,
            account_id=r.account_id,
            instrument_id=r.instrument_id,
            broker_order_ref=o.ref,
            order_sys_id=o.ref,
            status=o.status,
            filled_qty=o.filled,
            remaining_qty=o.remaining if o.live else 0,
            broker_code=broker_code,
            broker_seq=self._seq,
            trading_day=self.trading_day,
            updated_ns=self.clock.now_ns(),
        )
        self.endpoint.publish(topics.td_exec(r.account_id), u, fact=True, td=self.trading_day, corr_id=r.client_order_id)

    def working(self, instrument_id: str | None = None) -> list[BrokerOrder]:
        if instrument_id is not None:
            return [o for o in self.by_instrument.get(instrument_id, ()) if o.live]
        return [o for q in self.by_instrument.values() for o in q if o.live]

    def trades_of(self, account_id: str) -> list[Trade]:
        return [t for t in self.trades if t.account_id == account_id]


__all__ = ["BrokerOrder", "BrokerStats", "InprocBroker", "MarketState"]
