"""Order, routing, execution and broker payloads (02 §6.3-§6.4, §7.4-§7.5)."""

from __future__ import annotations

from typing import Any

import msgspec

from futrisk.messages.base import Msg, payload
from futrisk.messages.enums import (
    BrokerConnState,
    BrokerOrderStatus,
    Direction,
    Exchange,
    Offset,
    OrderSource,
    OrderStatus,
    OrderType,
    PosDirection,
    Role,
    TimeInForce,
    TradeOrigin,
)


@payload()
class OrderRequest(Msg, kw_only=True):
    client_order_id: str
    account_id: str
    instrument_id: str
    direction: Direction
    offset: Offset
    order_type: OrderType = OrderType.LIMIT
    tif: TimeInForce = TimeInForce.GFD
    price: float | None = None
    qty: int
    source: OrderSource = OrderSource.BOT
    strategy_tag: str | None = None
    actor: str | None = None
    sent_ns: int = 0


@payload()
class OrderAck(Msg, kw_only=True):
    client_order_id: str
    accepted: bool
    status: OrderStatus
    reject_code: str | None = None
    reject_detail: dict[str, Any] | None = None
    risk_check_us: int = 0
    trading_day: str = ""


class FillRef(msgspec.Struct, kw_only=True, omit_defaults=True):
    trade_id: str
    price: float
    qty: int
    trade_ts_ns: int


@payload()
class OrderUpdate(Msg, kw_only=True):
    client_order_id: str
    account_id: str
    instrument_id: str
    exchange: Exchange
    direction: Direction
    offset: Offset
    order_type: OrderType
    tif: TimeInForce
    price: float | None = None
    qty: int
    status: OrderStatus
    filled_qty: int = 0
    avg_fill_price: float | None = None
    last_fill: FillRef | None = None
    reject_code: str | None = None
    reject_detail: dict[str, Any] | None = None
    broker_order_ref: str | None = None
    source: OrderSource
    trading_day: str
    created_ns: int
    updated_ns: int
    reason: str | None = None


@payload()
class CancelRequest(Msg, kw_only=True):
    command_id: str
    client_order_id: str
    account_id: str
    reason: str | None = None


@payload()
class CancelAllRequest(Msg, kw_only=True):
    command_id: str
    account_id: str
    scope: str = "ALL"  # "ALL" | "OPEN_ONLY"
    product: str | None = None
    reason: str = ""
    actor: str = ""


@payload()
class CancelAllAck(Msg, kw_only=True):
    count: int


@payload()
class RouteOrder(Msg, kw_only=True):
    client_order_id: str
    account_id: str
    instrument_id: str
    exchange: Exchange
    direction: Direction
    offset: Offset
    order_type: OrderType
    tif: TimeInForce
    price: float | None = None
    qty: int
    trading_day: str
    risk_passed_ns: int
    expire_ns: int
    src_instance: str


@payload()
class RouteAck(Msg, kw_only=True):
    client_order_id: str
    status: BrokerOrderStatus
    broker_order_ref: str | None = None


@payload()
class RouteCancel(Msg, kw_only=True):
    command_id: str
    client_order_id: str
    reason: str | None = None


@payload()
class RouteResolve(Msg, kw_only=True):
    client_order_id: str
    trading_day: str


@payload()
class BrokerOrderUpdate(Msg, kw_only=True):
    client_order_id: str | None = None
    origin: TradeOrigin = TradeOrigin.INTERNAL
    account_id: str
    instrument_id: str
    broker_order_ref: str
    order_sys_id: str | None = None
    status: BrokerOrderStatus
    filled_qty: int = 0
    remaining_qty: int = 0
    broker_code: str | None = None
    broker_msg: str | None = None
    broker_seq: int
    trading_day: str
    updated_ns: int


@payload()
class Trade(Msg, kw_only=True):
    trading_day: str
    exchange: Exchange
    trade_id: str
    direction: Direction
    account_id: str
    instrument_id: str
    client_order_id: str | None = None
    origin: TradeOrigin = TradeOrigin.INTERNAL
    broker_order_ref: str
    order_sys_id: str | None = None
    offset: Offset
    price: float
    qty: int
    trade_ts_ns: int
    broker_seq: int | None = None
    trade_seq: int = 0
    received_ns: int = 0

    @property
    def trade_key(self) -> str:
        return f"{self.trading_day}|{self.exchange}|{self.trade_id}|{self.direction}"


class AccountDigest(msgspec.Struct, kw_only=True, omit_defaults=True):
    account_id: str
    count: int
    qty_sum: int
    max_trade_seq: int
    sha256: str


@payload()
class TradeDigest(Msg, kw_only=True):
    trading_day: str
    accounts: list[AccountDigest]
    total_count: int
    generated_ns: int


class BrokerPosition(msgspec.Struct, kw_only=True, omit_defaults=True):
    account_id: str
    instrument_id: str
    direction: PosDirection
    td_qty: int
    yd_qty: int


@payload()
class BrokerPositionBatch(Msg, kw_only=True):
    trading_day: str
    as_of_ns: int
    items: list[BrokerPosition]


@payload()
class BrokerStatus(Msg, kw_only=True):
    broker: str
    state: BrokerConnState
    trading_day: str | None = None
    session_id: str | None = None
    front_id: int | None = None
    last_broker_seq: int = 0
    pending_orders: int = 0
    since_ns: int


# ---------------------------------------------------------------- queries
@payload()
class PositionQuery(Msg, kw_only=True):
    trading_day: str
    account_id: str | None = None


@payload()
class TradeQuery(Msg, kw_only=True):
    trading_day: str
    account_id: str | None = None
    from_trade_seq: int | None = None
    limit: int = 500


@payload()
class TradeBatch(Msg, kw_only=True):
    trades: list[Trade]
    next_trade_seq: int
    complete: bool


@payload()
class OrderQuery(Msg, kw_only=True):
    trading_day: str
    account_id: str | None = None
    client_order_ids: list[str] | None = None


@payload()
class BrokerOrderBatch(Msg, kw_only=True):
    items: list[BrokerOrderUpdate]


@payload()
class DigestRequest(Msg, kw_only=True):
    trading_day: str


@payload()
class TradeImport(Msg, kw_only=True):
    command_id: str
    trading_day: str
    trades: list[Trade]
    source: str
    actor: str
    role: Role


@payload()
class TradeImportAck(Msg, kw_only=True):
    inserted: int
    duplicates: int


def trade_digest_line(t: Trade) -> str:
    """One line of the trade digest (02 §7.5): ``{trade_key}|{price}|{qty}|{offset}``."""
    from futrisk.common.decimals import dec_str_strip

    return f"{t.trade_key}|{dec_str_strip(t.price)}|{t.qty}|{t.offset}"


def compute_account_digest(account_id: str, trades: list[Trade]) -> AccountDigest:
    """Digest of one account's trades for one trading day (same algorithm everywhere)."""
    import hashlib

    rows = sorted(trades, key=lambda t: t.trade_key)
    text = "\n".join(trade_digest_line(t) for t in rows)
    return AccountDigest(
        account_id=account_id,
        count=len(rows),
        qty_sum=sum(t.qty for t in rows),
        max_trade_seq=max((t.trade_seq for t in rows), default=0),
        sha256=hashlib.sha256(text.encode("utf-8")).hexdigest(),
    )
