"""Order lifecycle from the risk engine's point of view (02 §8.3 OrderStatus state machine; 03 §4.3 orders / order_events).

States: ``ROUTING → ROUTED → ACCEPTED → PARTIALLY_FILLED → FILLED``, plus
``CANCEL_PENDING``, ``CANCELED``, ``BROKER_REJECTED``, ``UNKNOWN`` and the
pre-trade ``REJECTED``. Terminal: REJECTED, FILLED, CANCELED, BROKER_REJECTED.

- fills are accepted in every non-terminal state (the earlier states count as
  "accepted by the broker"); a fill on a terminal order is still applied, the
  status is corrected from the cumulative quantity and a non-blocking
  ``ORDER_STATE_CONFLICT`` recon break is recorded by the engine;
- ``ROUTING`` past ``expire_ns`` without a successful RouteAck, and every
  ``ROUTING`` order after a restart, becomes ``UNKNOWN`` and is confirmed with
  ``route.resolve``: ``EXPIRED`` (tombstone) → BROKER_REJECTED (``ROUTE_LOST``),
  or CANCELED when a cancel was pending;
- frozen margin / fee are held while ROUTING, ROUTED, UNKNOWN, ACCEPTED,
  PARTIALLY_FILLED and CANCEL_PENDING and released on terminal states;
- ``CANCEL_PENDING`` is durable at the trade-gateway: the outcome arrives on
  ``td.exec``. A live broker state restated *with* a ``broker_code`` (refused
  cancel, ``broker_seq`` 0) reverts the order to its working status; the
  engine re-checks a pending cancel with ``route.resolve`` after a TTL (5 s
  doubling to 60 s) and re-sends it with a new command id while the order is
  still working (bookkeeping persisted in ``orders.cancel_*``).

Every status change writes the ``orders`` row, an ``order_events`` row and an
``OrderUpdate`` FACT on ``oms.order.{account_id}`` (same transaction). The
``orders`` row is INSERTed once when the order is created and afterwards
UPDATEd with the changed columns only (:func:`persist_order`).
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from futrisk.common.decimals import D, dec_str_strip
from futrisk.core._num import PRICE_QUANTUM, money_str
from futrisk.core.effects import Tx
from futrisk.messages import topics
from futrisk.messages.enums import (
    BrokerOrderStatus,
    Direction,
    Exchange,
    Offset,
    OrderSource,
    OrderStatus,
    OrderType,
    TimeInForce,
)
from futrisk.messages.orders import FillRef, OrderAck, OrderUpdate

TERMINAL = frozenset({OrderStatus.REJECTED, OrderStatus.FILLED, OrderStatus.CANCELED, OrderStatus.BROKER_REJECTED})
FROZEN_STATES = frozenset(
    {
        OrderStatus.ROUTING,
        OrderStatus.ROUTED,
        OrderStatus.UNKNOWN,
        OrderStatus.ACCEPTED,
        OrderStatus.PARTIALLY_FILLED,
        OrderStatus.CANCEL_PENDING,
    }
)
# states in which a cancel can be sent (CANCEL_PENDING already has one in flight)
CANCELLABLE = FROZEN_STATES - {OrderStatus.CANCEL_PENDING}

# OrderUpdate / order_events reasons (02 §7.4 OrderUpdate.reason)
R_CIRCUIT_BREAK_CANCEL = "CIRCUIT_BREAK_CANCEL"
R_ROUTE_TIMEOUT = "ROUTE_TIMEOUT"
R_ROUTE_LOST = "ROUTE_LOST"
R_RESTART = "RESTART"
R_MANUAL_CANCEL = "MANUAL_CANCEL"
R_CANCEL_REJECTED = "CANCEL_REJECTED"
R_FILL_AFTER_TERMINAL = "FILL_AFTER_TERMINAL"
R_SESSION_CLOSE = "SESSION_CLOSE"
R_CANCEL_RETRY = "CANCEL_RETRY"
R_CANCEL_REFUSED = "CANCEL_REFUSED"


def price_dec(price: float | None) -> str | None:
    return None if price is None else dec_str_strip(D(price))


@dataclass
class OrderRecord:
    """One order (a row of ``orders`` plus in-memory routing bookkeeping)."""

    client_order_id: str
    account_id: str
    instrument_id: str
    exchange: Exchange
    direction: Direction
    offset: Offset  # normalised (02 §7.4 RouteOrder.offset)
    order_type: OrderType
    tif: TimeInForce
    price: float | None
    qty: int
    trading_day: str
    source: OrderSource
    status: OrderStatus
    created_ns: int
    updated_ns: int
    request_hash: str
    config_hash: str
    strategy_tag: str | None = None
    actor: str | None = None
    filled_qty: int = 0
    fill_value: Decimal = field(default_factory=Decimal)  # Σ price × qty (exact)
    last_fill: FillRef | None = None
    reject_code: str | None = None
    reject_detail: dict[str, Any] | None = None
    broker_order_ref: str | None = None
    frozen_margin: float = 0.0
    frozen_fee: float = 0.0
    risk_check_us: int = 0
    reason: str | None = None
    risk_passed_ns: int = 0
    expire_ns: int = 0
    cancel_command_id: str | None = None  # last route.cancel command id (persisted)
    cancel_sent_ns: int | None = None  # when the last route.cancel was sent (persisted)
    cancel_attempts: int = 0  # route.cancel sends for this order (persisted)
    cancel_error: bool = False  # the last route.cancel reply was an error (memory)
    cancel_escalated: bool = False  # a stuck cancel already opened its ORDER_STATE_CONFLICT break (memory)
    cancel_due_ns: int | None = None  # scheduled re-cancel after a refused cancel while HALTED (memory)
    status_before_cancel: OrderStatus | None = None
    resolve_inflight: bool = False
    resolve_sent_ns: int | None = None  # when the in-flight route.resolve was sent (memory)
    resolve_next_ns: int | None = None
    session_expired: bool = False  # expired at the day roll (CANCELED, SESSION_CLOSE), confirm pending (memory)
    written: dict[str, Any] | None = None  # mutable columns as last persisted (None: no row yet) (memory)
    close_reserved: int = 0  # lots reserved on the position for a resting close

    # ------------------------------------------------------------ helpers
    @property
    def remaining(self) -> int:
        return max(0, self.qty - self.filled_qty)

    @property
    def is_open(self) -> bool:
        return self.offset == Offset.OPEN

    @property
    def is_terminal(self) -> bool:
        return self.status in TERMINAL

    @property
    def holds_frozen(self) -> bool:
        return self.status in FROZEN_STATES and self.remaining > 0

    @property
    def avg_fill_price(self) -> float | None:
        if self.filled_qty <= 0:
            return None
        return float(self.fill_value / self.filled_qty)

    def avg_fill_dec(self) -> str | None:
        if self.filled_qty <= 0:
            return None
        return dec_str_strip((self.fill_value / self.filled_qty).quantize(PRICE_QUANTUM))

    def ack(self) -> OrderAck:
        """``OrderAck`` of the submit (also returned for duplicate submits)."""
        accepted = self.status != OrderStatus.REJECTED
        return OrderAck(
            client_order_id=self.client_order_id,
            accepted=accepted,
            status=OrderStatus.ROUTING if accepted else OrderStatus.REJECTED,
            reject_code=None if accepted else self.reject_code,
            reject_detail=None if accepted else self.reject_detail,
            risk_check_us=self.risk_check_us,
            trading_day=self.trading_day,
        )

    def to_update(self) -> OrderUpdate:
        return OrderUpdate(
            client_order_id=self.client_order_id,
            account_id=self.account_id,
            instrument_id=self.instrument_id,
            exchange=self.exchange,
            direction=self.direction,
            offset=self.offset,
            order_type=self.order_type,
            tif=self.tif,
            price=self.price,
            qty=self.qty,
            status=self.status,
            filled_qty=self.filled_qty,
            avg_fill_price=self.avg_fill_price,
            last_fill=self.last_fill,
            reject_code=self.reject_code,
            reject_detail=self.reject_detail,
            broker_order_ref=self.broker_order_ref,
            source=self.source,
            trading_day=self.trading_day,
            created_ns=self.created_ns,
            updated_ns=self.updated_ns,
            reason=self.reason,
        )

    def row(self) -> dict[str, Any]:
        """``orders`` row (03 §4.3); money as DecStr, JSON as dict."""
        return {
            "client_order_id": self.client_order_id,
            "account_id": self.account_id,
            "instrument_id": self.instrument_id,
            "exchange": self.exchange.value,
            "direction": self.direction.value,
            "offset": self.offset.value,
            "order_type": self.order_type.value,
            "tif": self.tif.value,
            "price": price_dec(self.price),
            "qty": self.qty,
            "trading_day": self.trading_day,
            "source": self.source.value,
            "strategy_tag": self.strategy_tag,
            "actor": self.actor,
            "risk_check_us": self.risk_check_us,
            "request_hash": self.request_hash,
            "config_hash": self.config_hash,
            "created_ns": self.created_ns,
            **self.mutable_values(),
        }

    def mutable_values(self) -> dict[str, Any]:
        """The ``orders`` columns that change after creation (see :func:`persist_order`)."""
        return {
            "status": self.status.value,
            "filled_qty": self.filled_qty,
            "avg_fill_price": self.avg_fill_dec(),
            "reject_code": self.reject_code,
            "reject_detail": self.reject_detail,
            "broker_order_ref": self.broker_order_ref,
            "frozen_margin": money_str(D(round(self.frozen_margin, 2))),
            "frozen_fee": money_str(D(round(self.frozen_fee, 2))),
            "updated_ns": self.updated_ns,
            # 撤单重试簿记（可空的新增列）：重启后据此继续撤单（02 §8.3）
            "cancel_command_id": self.cancel_command_id,
            "cancel_sent_ns": self.cancel_sent_ns,
            "cancel_attempts": self.cancel_attempts,
        }

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> OrderRecord:
        """Rebuild from an ``orders`` row (restore after restart)."""
        filled = int(row.get("filled_qty") or 0)
        avg = row.get("avg_fill_price")
        fill_value = D(str(avg)) * filled if avg is not None and filled else Decimal(0)
        price = row.get("price")
        detail = row.get("reject_detail")
        if isinstance(detail, str):
            detail = json.loads(detail)
        rec = cls(
            client_order_id=str(row["client_order_id"]),
            account_id=str(row["account_id"]),
            instrument_id=str(row["instrument_id"]),
            exchange=Exchange(str(row["exchange"])),
            direction=Direction(str(row["direction"])),
            offset=Offset(str(row["offset"])),
            order_type=OrderType(str(row["order_type"])),
            tif=TimeInForce(str(row["tif"])),
            price=None if price is None else float(D(str(price))),
            qty=int(row["qty"]),
            trading_day=str(row["trading_day"]),
            source=OrderSource(str(row["source"])),
            status=OrderStatus(str(row["status"])),
            created_ns=int(row["created_ns"]),
            updated_ns=int(row["updated_ns"]),
            request_hash=str(row.get("request_hash") or ""),
            config_hash=str(row.get("config_hash") or ""),
            strategy_tag=row.get("strategy_tag"),
            actor=row.get("actor"),
            filled_qty=filled,
            fill_value=fill_value,
            reject_code=row.get("reject_code"),
            reject_detail=detail,
            broker_order_ref=row.get("broker_order_ref"),
            frozen_margin=float(D(str(row.get("frozen_margin") or "0"))),
            frozen_fee=float(D(str(row.get("frozen_fee") or "0"))),
            risk_check_us=int(row.get("risk_check_us") or 0),
            cancel_command_id=row.get("cancel_command_id"),
            cancel_sent_ns=None if row.get("cancel_sent_ns") is None else int(row["cancel_sent_ns"]),
            cancel_attempts=int(row.get("cancel_attempts") or 0),
        )
        rec.written = rec.mutable_values()  # the row exists: later changes are UPDATEs
        return rec


def persist_order(tx: Tx, rec: OrderRecord) -> None:
    """Write the ``orders`` row: INSERT when the order has no row yet, else UPDATE of the changed columns only.

    A new ``client_order_id`` never overwrites an existing row: the engine
    rejects ids it has seen (also pruned ones) and the shell checks older days
    in risk.db, so an INSERT conflict cannot silently rewrite history.
    """
    cur = rec.mutable_values()
    if rec.written is None:
        tx.insert("orders", rec.row())
    else:
        changed = {k: v for k, v in cur.items() if rec.written.get(k) != v}
        if changed:
            tx.update("orders", ("client_order_id",), {"client_order_id": rec.client_order_id, **changed})
    rec.written = cur


def emit_order(
    tx: Tx,
    rec: OrderRecord,
    *,
    from_status: OrderStatus | None,
    now_ns: int,
    reason: str | None = None,
    detail: dict[str, Any] | None = None,
) -> None:
    """Persist an order change: ``orders`` row (INSERT / UPDATE of changed columns) + ``order_events`` +
    ``oms.order`` FACT (03 §7 事务表)."""
    rec.updated_ns = now_ns
    rec.reason = reason
    persist_order(tx, rec)
    tx.insert(
        "order_events",
        {
            "client_order_id": rec.client_order_id,
            "ts_ns": now_ns,
            "from_status": from_status.value if from_status is not None else None,
            "to_status": rec.status.value,
            "reason": reason,
            "detail": detail or {},
        },
    )
    tx.emit(topics.oms_order(rec.account_id), rec.to_update(), ts_ns=now_ns, corr_id=rec.client_order_id, td=rec.trading_day)


def set_status(
    tx: Tx,
    rec: OrderRecord,
    status: OrderStatus,
    *,
    now_ns: int,
    reason: str | None = None,
    detail: dict[str, Any] | None = None,
) -> bool:
    """Change the status and persist it; returns False when nothing changed."""
    if rec.status == status:
        return False
    prev = rec.status
    rec.status = status
    if status in TERMINAL:
        rec.frozen_margin = 0.0
        rec.frozen_fee = 0.0
        rec.resolve_inflight = False
        rec.resolve_next_ns = None
        rec.cancel_due_ns = None
    emit_order(tx, rec, from_status=prev, now_ns=now_ns, reason=reason, detail=detail)
    return True


def status_after_fill(rec: OrderRecord) -> OrderStatus:
    """Status implied by the cumulative fill (02 §8.3)."""
    if rec.filled_qty >= rec.qty:
        return OrderStatus.FILLED
    if rec.status == OrderStatus.CANCEL_PENDING:
        return OrderStatus.CANCEL_PENDING  # 撤单仍在途，部分成交不改变撤单中状态
    if rec.status in TERMINAL:
        return rec.status  # 终态委托收到部分成交：保持终态，由对账异常记录冲突
    return OrderStatus.PARTIALLY_FILLED if rec.filled_qty > 0 else OrderStatus.ACCEPTED


def status_after_cancel_rejected(rec: OrderRecord) -> OrderStatus:
    """CANCEL_PENDING → ACCEPTED / PARTIALLY_FILLED when the cancel is refused (02 §8.3)."""
    if rec.filled_qty >= rec.qty:
        return OrderStatus.FILLED
    prev = rec.status_before_cancel
    if prev in (OrderStatus.ROUTING, OrderStatus.UNKNOWN, OrderStatus.ROUTED) and rec.filled_qty == 0:
        return prev
    return OrderStatus.PARTIALLY_FILLED if rec.filled_qty > 0 else OrderStatus.ACCEPTED


WORKING_BROKER_STATES = frozenset(
    {BrokerOrderStatus.RECEIVED, BrokerOrderStatus.SUBMITTED, BrokerOrderStatus.ACCEPTED, BrokerOrderStatus.PARTIALLY_FILLED}
)


def working_status(rec: OrderRecord, status: BrokerOrderStatus) -> OrderStatus:
    """Working risk-side status for a broker state that is still live (fills decide PARTIALLY_FILLED)."""
    if rec.filled_qty >= rec.qty:
        return OrderStatus.FILLED
    if rec.filled_qty > 0:
        return OrderStatus.PARTIALLY_FILLED
    return OrderStatus.ROUTED if status in (BrokerOrderStatus.RECEIVED, BrokerOrderStatus.SUBMITTED) else OrderStatus.ACCEPTED


def broker_status_target(rec: OrderRecord, status: BrokerOrderStatus, broker_code: str | None = None) -> OrderStatus | None:
    """Risk-side status for a broker order update (None: no change).

    A live broker state with a ``broker_code`` for a CANCEL_PENDING order is the
    trade-gateway's restatement after a refused cancel (``BROKER_ORDER_NOT_FOUND``
    …, ``broker_seq=0``): the order is working again. Without a code it is an
    ordinary report racing the cancel and is ignored.
    """
    cur = rec.status
    if cur == OrderStatus.CANCEL_PENDING and broker_code and status in WORKING_BROKER_STATES:
        return working_status(rec, status)  # 撤单被拒：回到工作状态，由撤单超时 / 熔断守护继续处理
    if status == BrokerOrderStatus.FILLED and cur == OrderStatus.CANCEL_PENDING:
        return OrderStatus.FILLED if rec.filled_qty >= rec.qty else None  # 成交以 Trade 为准
    if status in (BrokerOrderStatus.RECEIVED, BrokerOrderStatus.SUBMITTED):
        return OrderStatus.ROUTED if cur in (OrderStatus.ROUTING, OrderStatus.UNKNOWN) else None
    if status == BrokerOrderStatus.ACCEPTED:
        if cur in (OrderStatus.ROUTING, OrderStatus.ROUTED, OrderStatus.UNKNOWN):
            return OrderStatus.PARTIALLY_FILLED if rec.filled_qty > 0 else OrderStatus.ACCEPTED
        return None
    if status in (BrokerOrderStatus.PARTIALLY_FILLED, BrokerOrderStatus.FILLED):
        # 成交数量以 Trade 为准；这里只确认"已受理"
        if cur in (OrderStatus.ROUTING, OrderStatus.ROUTED, OrderStatus.UNKNOWN):
            return OrderStatus.PARTIALLY_FILLED if rec.filled_qty > 0 else OrderStatus.ACCEPTED
        return None
    if status == BrokerOrderStatus.CANCEL_PENDING:
        return OrderStatus.CANCEL_PENDING if cur in CANCELLABLE else None
    if status == BrokerOrderStatus.CANCELED:
        if cur in TERMINAL:
            return None
        return OrderStatus.FILLED if rec.filled_qty >= rec.qty else OrderStatus.CANCELED
    if status == BrokerOrderStatus.REJECTED:
        return None if cur in TERMINAL else OrderStatus.BROKER_REJECTED
    if status == BrokerOrderStatus.EXPIRED:
        if cur in TERMINAL:
            return None
        return OrderStatus.CANCELED if cur == OrderStatus.CANCEL_PENDING else OrderStatus.BROKER_REJECTED
    return None  # UNKNOWN: nothing learnt


class OrderBook:
    """All orders known to one shard, indexed by account."""

    def __init__(self) -> None:
        self._orders: dict[str, OrderRecord] = {}
        self._by_account: dict[str, list[str]] = {}
        # non-terminal orders per account and per (account, instrument); terminal ones are purged lazily
        # (terminal is absorbing, 02 §8.3)
        self._live: dict[str, dict[str, OrderRecord]] = {}
        self._live_inst: dict[tuple[str, str], dict[str, OrderRecord]] = {}
        self._retired: set[str] = set()  # ids of pruned orders: a client_order_id is never reused

    def __contains__(self, client_order_id: str) -> bool:
        return client_order_id in self._orders

    def __len__(self) -> int:
        return len(self._orders)

    def get(self, client_order_id: str) -> OrderRecord | None:
        return self._orders.get(client_order_id)

    def seen(self, client_order_id: str) -> bool:
        """True for an order known now or pruned earlier in this process (its ``orders`` row exists)."""
        return client_order_id in self._orders or client_order_id in self._retired

    def add(self, rec: OrderRecord) -> None:
        if rec.client_order_id in self._orders:
            raise ValueError(f"order {rec.client_order_id} already exists")
        self._orders[rec.client_order_id] = rec
        self._by_account.setdefault(rec.account_id, []).append(rec.client_order_id)
        if not rec.is_terminal:
            self._live.setdefault(rec.account_id, {})[rec.client_order_id] = rec
            self._live_inst.setdefault((rec.account_id, rec.instrument_id), {})[rec.client_order_id] = rec

    def all(self) -> list[OrderRecord]:
        return list(self._orders.values())

    def of_account(self, account_id: str, trading_day: str | None = None) -> list[OrderRecord]:
        out = [self._orders[c] for c in self._by_account.get(account_id, [])]
        if trading_day is not None:
            out = [o for o in out if o.trading_day == trading_day]
        return out

    def resting(self, account_id: str | None = None) -> list[OrderRecord]:
        """Non-terminal orders in creation order; O(live orders), terminal ones are dropped lazily."""
        if account_id is None:
            return [o for a in sorted(self._live) for o in self._live_of(a)]
        return self._live_of(account_id)

    def resting_on(self, account_id: str, instrument_id: str) -> list[OrderRecord]:
        """Non-terminal orders of one account on one instrument (self-trade, in-flight checks); O(those orders)."""
        live = self._live_inst.get((account_id, instrument_id))
        if not live:
            return []
        return self._purge(live)

    def _live_of(self, account_id: str) -> list[OrderRecord]:
        live = self._live.get(account_id)
        if not live:
            return []
        return self._purge(live)

    @staticmethod
    def _purge(live: dict[str, OrderRecord]) -> list[OrderRecord]:
        out: list[OrderRecord] = []
        dead: list[str] = []
        for c, o in live.items():
            if o.is_terminal:
                dead.append(c)
            else:
                out.append(o)
        for c in dead:
            del live[c]
        return out

    def prune(self, keep_trading_day: str) -> int:
        """Drop terminal orders of other trading days (after a day roll); O(orders).

        Orders with a ``route.resolve`` in flight (the confirmation of an order
        expired at the roll) are kept until the answer arrives.
        """
        drop = {
            c for c, o in self._orders.items()
            if o.is_terminal and o.trading_day != keep_trading_day and not o.resolve_inflight
        }
        if not drop:
            return 0
        for c in drop:
            del self._orders[c]
        self._retired |= drop
        for a, ids in self._by_account.items():
            self._by_account[a] = [c for c in ids if c not in drop]
        for live in (*self._live.values(), *self._live_inst.values()):
            for c in [c for c in live if c in drop]:
                del live[c]
        return len(drop)


__all__ = [
    "CANCELLABLE",
    "FROZEN_STATES",
    "R_CANCEL_REFUSED",
    "R_CANCEL_REJECTED",
    "R_CANCEL_RETRY",
    "R_CIRCUIT_BREAK_CANCEL",
    "R_FILL_AFTER_TERMINAL",
    "R_MANUAL_CANCEL",
    "R_RESTART",
    "R_ROUTE_LOST",
    "R_ROUTE_TIMEOUT",
    "R_SESSION_CLOSE",
    "TERMINAL",
    "OrderBook",
    "OrderRecord",
    "WORKING_BROKER_STATES",
    "broker_status_target",
    "emit_order",
    "persist_order",
    "price_dec",
    "set_status",
    "status_after_cancel_rejected",
    "status_after_fill",
    "working_status",
]
