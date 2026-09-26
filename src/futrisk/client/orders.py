"""Client-side order book: request, ack and ``oms.order.{account}`` updates per order (02 §7.4, §8.3).

Every order is keyed by its ``client_order_id``. The book combines three sources:

1. the request sent by this client (``add_request``);
2. the ``order.submit`` reply (``on_submit_reply``): ``OrderAck`` with ``ROUTING`` or
   ``REJECTED``; a transport failure after the retries (``TIMEOUT``/``UNAVAILABLE``/
   ``BUSY``/``RISK_BUSY``) leaves the order **in doubt** - it may have been accepted,
   so it keeps counting as working until an update or a resubmission with the same id
   resolves it (01 §6.5, 02 §4.7: never re-send the same intent with a new id);
3. ``OrderUpdate`` FACTs (``apply_update``), the authority on status and fills.

Updates are applied idempotently and monotonically: ``filled_qty`` only grows (each
increase is returned as a :class:`FillDelta` for the position tracker), a terminal
order never goes back to a working status, and older updates (replays after a gap)
cannot overwrite newer ones. Orders placed by others on the same account (e.g.
manual orders through api-gateway) are tracked too ("foreign" orders).

Times: ``updated_ns`` of the book only ever holds engine times (``OrderUpdate.updated_ns``,
the risk-engine's market clock); an own request starts at 0, so a client clock (wall
time, or a demo market clock with an offset) is never compared with engine times. An
order given up as LOST is revived by a later non-terminal update from the engine.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from futrisk.client.positions import PendingCloses, side_closed
from futrisk.messages import codes
from futrisk.messages.enums import (
    Direction,
    Offset,
    OrderSource,
    OrderStatus,
    OrderType,
    PosDirection,
    TimeInForce,
)
from futrisk.messages.envelope import Reply
from futrisk.messages.orders import OrderAck, OrderRequest, OrderUpdate


class ClientState(StrEnum):
    """Client-side lifecycle of an order (beside the risk-engine ``OrderStatus``)."""

    PENDING = "PENDING"  # order.submit in flight
    ACKED = "ACKED"  # the risk-engine answered (or an update arrived)
    IN_DOUBT = "IN_DOUBT"  # no answer after the retries: maybe accepted
    DONE = "DONE"  # terminal status reached
    LOST = "LOST"  # in doubt and given up by the owner


WORKING_STATUSES = frozenset(
    {
        OrderStatus.ROUTING,
        OrderStatus.ROUTED,
        OrderStatus.ACCEPTED,
        OrderStatus.PARTIALLY_FILLED,
        OrderStatus.CANCEL_PENDING,
        OrderStatus.UNKNOWN,
    }
)
#: statuses from which order.cancel makes sense
CANCELLABLE_STATUSES = frozenset(
    {OrderStatus.ROUTING, OrderStatus.ROUTED, OrderStatus.ACCEPTED, OrderStatus.PARTIALLY_FILLED, OrderStatus.UNKNOWN}
)


@dataclass(frozen=True, slots=True)
class FillDelta:
    """Newly filled quantity of one order and its (estimated) average price.

    ``ts_ns`` is the engine-side time of the latest fill (``last_fill.trade_ts_ns``, else
    the update's ``updated_ns``); ``first_seen`` marks an order the book did not know,
    whose fills may predate a position snapshot, and ``last_qty`` is the size of its
    latest fill (``last_fill.qty``) - see the snapshot rule of :class:`FutriskClient`.
    """

    qty: int
    price: float
    ts_ns: int = 0
    first_seen: bool = False
    last_qty: int | None = None


@dataclass
class TrackedOrder:
    """What the client knows about one order."""

    client_order_id: str
    account_id: str
    instrument_id: str
    direction: Direction
    offset: Offset
    qty: int
    price: float | None
    order_type: OrderType = OrderType.LIMIT
    tif: TimeInForce = TimeInForce.GFD
    source: OrderSource = OrderSource.BOT
    strategy_tag: str | None = None
    request: OrderRequest | None = None
    state: ClientState = ClientState.PENDING
    status: OrderStatus | None = None
    filled_qty: int = 0
    baseline_filled: int = 0
    avg_fill_price: float | None = None
    reject_code: str | None = None
    reject_detail: dict[str, Any] | None = None
    reason: str | None = None
    trading_day: str | None = None
    created_ns: int = 0
    updated_ns: int = 0
    submit_attempts: int = 0
    cancel_requested: bool = False
    fills: list[FillDelta] = field(default_factory=list)

    @property
    def own(self) -> bool:
        """True when this client submitted the order."""
        return self.request is not None

    @property
    def terminal(self) -> bool:
        if self.state in (ClientState.DONE, ClientState.LOST):
            return True
        return self.status is not None and self.status.is_terminal

    @property
    def working(self) -> bool:
        return not self.terminal

    @property
    def remaining(self) -> int:
        return max(0, self.qty - self.filled_qty)

    @property
    def is_open(self) -> bool:
        return self.offset == Offset.OPEN

    @property
    def closes_side(self) -> PosDirection | None:
        return None if self.is_open else side_closed(self.direction)

    @property
    def cancellable(self) -> bool:
        return self.working and self.status in CANCELLABLE_STATUSES and not self.cancel_requested


class OrderBook:
    """Orders of every account seen by the client."""

    def __init__(self, max_terminal: int = 5_000) -> None:
        self._orders: dict[str, TrackedOrder] = {}
        self._max_terminal = max_terminal

    def __len__(self) -> int:
        return len(self._orders)

    def get(self, client_order_id: str) -> TrackedOrder | None:
        return self._orders.get(client_order_id)

    def orders(self, account_id: str | None = None) -> list[TrackedOrder]:
        return [o for o in self._orders.values() if account_id is None or o.account_id == account_id]

    def working(self, account_id: str | None = None, instrument_id: str | None = None) -> list[TrackedOrder]:
        return [
            o
            for o in self._orders.values()
            if o.working
            and (account_id is None or o.account_id == account_id)
            and (instrument_id is None or o.instrument_id == instrument_id)
        ]

    def pending_closes(self, account_id: str, instrument_id: str, side: PosDirection) -> PendingCloses:
        """Remaining lots of working close orders on ``side``, by offset bucket."""
        td = yd = anyq = 0
        for o in self.working(account_id, instrument_id):
            if o.is_open or o.closes_side != side:
                continue
            if o.offset == Offset.CLOSE_TODAY:
                td += o.remaining
            elif o.offset == Offset.CLOSE_YESTERDAY:
                yd += o.remaining
            else:
                anyq += o.remaining
        return PendingCloses(td=td, yd=yd, any=anyq)

    # ------------------------------------------------------------ updates
    def add_request(self, req: OrderRequest) -> TrackedOrder:
        o = self._orders.get(req.client_order_id)
        if o is not None:
            return o
        o = TrackedOrder(
            client_order_id=req.client_order_id,
            account_id=req.account_id,
            instrument_id=req.instrument_id,
            direction=req.direction,
            offset=req.offset,
            qty=req.qty,
            price=req.price,
            order_type=req.order_type,
            tif=req.tif,
            source=req.source,
            strategy_tag=req.strategy_tag,
            request=req,
            created_ns=req.sent_ns,
            updated_ns=0,  # engine times only (02 §7.4 updated_ns); never the client clock
        )
        self._orders[req.client_order_id] = o
        self._prune()
        return o

    def on_submit_reply(self, client_order_id: str, reply: Reply, ack: OrderAck | None) -> TrackedOrder | None:
        """Record the ``order.submit`` outcome."""
        o = self._orders.get(client_order_id)
        if o is None:
            return None
        o.submit_attempts += 1
        if ack is not None and ack.trading_day:
            o.trading_day = o.trading_day or ack.trading_day
        if reply.code in codes.RETRYABLE:
            if o.state in (ClientState.PENDING, ClientState.IN_DOUBT):
                o.state = ClientState.IN_DOUBT
            return o
        if o.state == ClientState.PENDING or o.state == ClientState.IN_DOUBT:
            o.state = ClientState.ACKED
        if ack is not None and not ack.accepted:
            o.status = OrderStatus.REJECTED
            o.reject_code = ack.reject_code or reply.code
            o.reject_detail = ack.reject_detail
        elif ack is not None:
            if o.status is None:
                o.status = ack.status
        elif not reply.ok:
            # a non-retryable error without OrderAck (BAD_REQUEST, FORBIDDEN, NOT_OWNER, ...)
            o.status = OrderStatus.REJECTED
            o.reject_code = reply.code
        if o.status is not None and o.status.is_terminal:
            o.state = ClientState.DONE
        return o

    def apply_update(self, upd: OrderUpdate, *, baseline: bool = False) -> tuple[TrackedOrder, FillDelta | None]:
        """Apply an ``OrderUpdate``; returns the order and the newly filled quantity (if any).

        ``baseline``: the order is new to the book and its fills so far are already part
        of a position snapshot, so they are recorded without producing a delta.
        """
        o = self._orders.get(upd.client_order_id)
        first_seen = o is None
        if o is None:
            o = TrackedOrder(
                client_order_id=upd.client_order_id,
                account_id=upd.account_id,
                instrument_id=upd.instrument_id,
                direction=upd.direction,
                offset=upd.offset,
                qty=upd.qty,
                price=upd.price,
                order_type=upd.order_type,
                tif=upd.tif,
                source=upd.source,
                state=ClientState.ACKED,
                created_ns=upd.created_ns,
            )
            if baseline:
                o.filled_qty = o.baseline_filled = upd.filled_qty
                o.avg_fill_price = upd.avg_fill_price
            self._orders[upd.client_order_id] = o
            self._prune()
        if o.state in (ClientState.PENDING, ClientState.IN_DOUBT):
            o.state = ClientState.ACKED
        elif o.state == ClientState.LOST and not upd.status.is_terminal:
            o.state = ClientState.ACKED  # given up too early: the engine says it is alive (revive)
        # the risk-engine normalises the offset (02 §7.4): keep its value for fills
        o.offset = upd.offset
        o.trading_day = upd.trading_day or o.trading_day
        delta = upd.filled_qty - o.filled_qty
        fill: FillDelta | None = None
        if delta > 0:
            fill = FillDelta(
                delta,
                _fill_price(o, upd, delta),
                ts_ns=upd.last_fill.trade_ts_ns if upd.last_fill is not None else upd.updated_ns,
                first_seen=first_seen,
                last_qty=upd.last_fill.qty if upd.last_fill is not None else None,
            )
            if upd.avg_fill_price is not None:
                o.avg_fill_price = upd.avg_fill_price
            elif fill.price > 0:
                prev = o.avg_fill_price or 0.0
                o.avg_fill_price = (prev * o.filled_qty + fill.price * delta) / upd.filled_qty
            o.filled_qty = upd.filled_qty
            o.fills.append(fill)
        stale = delta < 0 or (delta == 0 and upd.updated_ns < o.updated_ns)
        if not stale:
            regress = o.status is not None and o.status.is_terminal and not upd.status.is_terminal
            if not regress or delta > 0:
                o.status = upd.status
            if upd.reject_code is not None:
                o.reject_code = upd.reject_code
                o.reject_detail = upd.reject_detail
            if upd.reason is not None:
                o.reason = upd.reason
            o.updated_ns = max(o.updated_ns, upd.updated_ns)
        if o.status is not None and o.status.is_terminal:
            o.state = ClientState.DONE
        return o, fill

    def mark_lost(self, client_order_id: str, reason: str = "IN_DOUBT_EXPIRED") -> TrackedOrder | None:
        """Give up on an in-doubt order (it no longer counts as working)."""
        o = self._orders.get(client_order_id)
        if o is not None and o.working:
            o.state = ClientState.LOST
            o.reason = reason
        return o

    def _prune(self) -> None:
        if len(self._orders) <= self._max_terminal * 2:
            return
        done = [k for k, o in self._orders.items() if o.terminal]
        for k in done[: max(0, len(done) - self._max_terminal)]:
            del self._orders[k]


def _fill_price(o: TrackedOrder, upd: OrderUpdate, delta: int) -> float:
    if upd.last_fill is not None and upd.last_fill.qty == delta:
        return upd.last_fill.price
    if upd.avg_fill_price is not None:
        prev_avg = o.avg_fill_price or 0.0
        value = upd.avg_fill_price * upd.filled_qty - prev_avg * o.filled_qty
        px = value / delta
        if px > 0:
            return px
    if upd.last_fill is not None:
        return upd.last_fill.price
    return upd.price or 0.0


__all__ = [
    "CANCELLABLE_STATUSES",
    "WORKING_STATUSES",
    "ClientState",
    "FillDelta",
    "OrderBook",
    "TrackedOrder",
]
