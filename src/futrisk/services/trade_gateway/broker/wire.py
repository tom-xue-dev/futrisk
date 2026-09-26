"""SimBroker wire format: CTP-style codes, request builders and report parsing (02 §9.4).

Mapping (02 §9.4 映射到内部定义):

=====================  =====================================================
wire                   internal
=====================  =====================================================
``direction``          ``"0"`` BUY, ``"1"`` SELL
``comb_offset_flag``   ``"0"`` OPEN, ``"1"`` CLOSE, ``"2"`` FORCE_CLOSE,
/ ``offset_flag``      ``"3"`` CLOSE_TODAY, ``"4"`` CLOSE_YESTERDAY
``order_price_type``   ``LIMIT`` LIMIT, ``ANY`` MARKET
``time_condition`` +   ``GFD`` GFD, ``IOC+AV`` FAK, ``IOC+CV`` FOK
``volume_condition``
``order_status``       ``ACCEPTED`` ACCEPTED, ``PART_TRADED`` PARTIALLY_FILLED,
                       ``ALL_TRADED`` FILLED, ``CANCELED`` CANCELED,
                       ``REJECTED`` REJECTED
``posi_direction``     ``LONG`` / ``SHORT`` (= ``PosDirection``)
=====================  =====================================================

Any unknown value raises :class:`WireError`; the adapter turns it into an
:class:`~futrisk.services.trade_gateway.broker.base.InvalidReport` (logged, counted,
never written to td.db).
"""

from __future__ import annotations

import math
from typing import Any

from futrisk.messages.enums import (
    BrokerOrderStatus,
    Direction,
    Exchange,
    Offset,
    OrderType,
    PosDirection,
    TimeInForce,
)
from futrisk.messages.orders import RouteOrder
from futrisk.services.trade_gateway.broker.base import (
    InvalidReport,
    OrderReport,
    PositionReport,
    TradeReport,
    parse_broker_order_ref,
)

DIRECTION_TO_WIRE: dict[Direction, str] = {Direction.BUY: "0", Direction.SELL: "1"}
DIRECTION_FROM_WIRE: dict[str, Direction] = {v: k for k, v in DIRECTION_TO_WIRE.items()}

OFFSET_TO_WIRE: dict[Offset, str] = {
    Offset.OPEN: "0",
    Offset.CLOSE: "1",
    Offset.FORCE_CLOSE: "2",
    Offset.CLOSE_TODAY: "3",
    Offset.CLOSE_YESTERDAY: "4",
}
OFFSET_FROM_WIRE: dict[str, Offset] = {v: k for k, v in OFFSET_TO_WIRE.items()}

PRICE_TYPE_TO_WIRE: dict[OrderType, str] = {OrderType.LIMIT: "LIMIT", OrderType.MARKET: "ANY"}
PRICE_TYPE_FROM_WIRE: dict[str, OrderType] = {v: k for k, v in PRICE_TYPE_TO_WIRE.items()}

TIF_TO_WIRE: dict[TimeInForce, tuple[str, str]] = {
    TimeInForce.GFD: ("GFD", "AV"),
    TimeInForce.FAK: ("IOC", "AV"),
    TimeInForce.FOK: ("IOC", "CV"),
}

STATUS_FROM_WIRE: dict[str, BrokerOrderStatus] = {
    "ACCEPTED": BrokerOrderStatus.ACCEPTED,
    "PART_TRADED": BrokerOrderStatus.PARTIALLY_FILLED,
    "ALL_TRADED": BrokerOrderStatus.FILLED,
    "CANCELED": BrokerOrderStatus.CANCELED,
    "REJECTED": BrokerOrderStatus.REJECTED,
}


class WireError(ValueError):
    """A broker message field has a missing or unknown value."""


# ------------------------------------------------------------------ field helpers
def _str(d: dict[str, Any], key: str, *, required: bool = True) -> str | None:
    v = d.get(key)
    if isinstance(v, bool):
        v = None
    if isinstance(v, int):
        v = str(v)
    if isinstance(v, str) and v != "":
        return v
    if required:
        raise WireError(f"{key} missing")
    return None


def _req_str(d: dict[str, Any], key: str) -> str:
    v = _str(d, key)
    assert v is not None
    return v


def _int(d: dict[str, Any], key: str) -> int:
    v = d.get(key)
    if isinstance(v, bool):
        raise WireError(f"{key} is not an integer")
    if isinstance(v, int):
        return v
    if isinstance(v, float) and math.isfinite(v) and v == int(v):
        return int(v)
    if isinstance(v, str) and v.strip().lstrip("-").isdigit():
        return int(v.strip())
    raise WireError(f"{key} is not an integer: {v!r}")


def _float(d: dict[str, Any], key: str) -> float:
    v = d.get(key)
    if isinstance(v, bool) or not isinstance(v, int | float) or not math.isfinite(float(v)):
        raise WireError(f"{key} is not a finite number: {v!r}")
    return float(v)


def _enum(mapping: dict[str, Any], d: dict[str, Any], key: str) -> Any:
    raw = d.get(key)
    try:
        return mapping[str(raw)]
    except KeyError:
        raise WireError(f"unknown {key} {raw!r}") from None


def _exchange(d: dict[str, Any]) -> Exchange:
    raw = d.get("exchange_id")
    try:
        return Exchange(str(raw))
    except ValueError:
        raise WireError(f"unknown exchange_id {raw!r}") from None


def _tif(d: dict[str, Any]) -> TimeInForce:
    tc = str(d.get("time_condition") or "GFD").upper()
    vc = str(d.get("volume_condition") or "AV").upper()
    if tc == "GFD" and vc in ("AV", "CV"):
        return TimeInForce.GFD
    if tc == "IOC" and vc == "AV":
        return TimeInForce.FAK
    if tc == "IOC" and vc == "CV":
        return TimeInForce.FOK
    raise WireError(f"unknown time/volume condition {tc}/{vc}")


def _seq_td(d: dict[str, Any]) -> tuple[int | None, str | None]:
    try:
        seq: int | None = _int(d, "broker_seq")
    except WireError:
        seq = None
    td = d.get("trading_day")
    return seq, (td if isinstance(td, str) and len(td) == 8 else None)


# ------------------------------------------------------------------ reports
def parse_order(d: dict[str, Any]) -> OrderReport:
    """``RtnOrder`` fields -> :class:`OrderReport` (raises :class:`WireError`)."""
    trading_day = _req_str(d, "trading_day")
    if len(trading_day) != 8 or not trading_day.isdigit():
        raise WireError(f"bad trading_day {trading_day!r}")
    raw_type = str(d.get("order_price_type") or "LIMIT").upper()
    order_type = PRICE_TYPE_FROM_WIRE.get(raw_type)
    if order_type is None:
        raise WireError(f"unknown order_price_type {raw_type!r}")
    raw_price = d.get("limit_price")
    price: float | None = None
    if order_type is OrderType.LIMIT:
        price = _float(d, "limit_price")
    elif isinstance(raw_price, int | float) and not isinstance(raw_price, bool) and raw_price:
        price = float(raw_price)
    qty = _int(d, "volume_total_original")
    traded = _int(d, "volume_traded")
    remaining = _int(d, "volume_total") if d.get("volume_total") is not None else max(0, qty - traded)
    if qty < 0 or traded < 0 or remaining < 0:
        raise WireError("negative volume")
    update_ns = d.get("update_ns")
    return OrderReport(
        broker_seq=_int(d, "broker_seq"),
        trading_day=trading_day,
        front_id=_int(d, "front_id"),
        session_id=_int(d, "session_id"),
        order_ref=_req_str(d, "order_ref"),
        order_sys_id=_str(d, "order_sys_id", required=False),
        client_order_id=_str(d, "client_order_id", required=False),
        account_id=_req_str(d, "investor_id"),
        instrument_id=_req_str(d, "instrument_id"),
        exchange=_exchange(d),
        direction=_enum(DIRECTION_FROM_WIRE, d, "direction"),
        offset=_enum(OFFSET_FROM_WIRE, d, "comb_offset_flag"),
        order_type=order_type,
        tif=_tif(d),
        price=price,
        qty=qty,
        filled_qty=traded,
        remaining_qty=remaining,
        status=_enum(STATUS_FROM_WIRE, d, "order_status"),
        status_msg=str(d.get("status_msg") or ""),
        update_ns=int(update_ns) if isinstance(update_ns, int) and not isinstance(update_ns, bool) else 0,
    )


def parse_trade(d: dict[str, Any]) -> TradeReport:
    """``RtnTrade`` fields -> :class:`TradeReport` (raises :class:`WireError`)."""
    trading_day = _req_str(d, "trading_day")
    if len(trading_day) != 8 or not trading_day.isdigit():
        raise WireError(f"bad trading_day {trading_day!r}")
    qty = _int(d, "volume")
    if qty <= 0:
        raise WireError(f"volume must be positive: {qty}")
    return TradeReport(
        broker_seq=_int(d, "broker_seq"),
        trading_day=trading_day,
        trade_id=_req_str(d, "trade_id"),
        front_id=_int(d, "front_id"),
        session_id=_int(d, "session_id"),
        order_ref=_req_str(d, "order_ref"),
        order_sys_id=_str(d, "order_sys_id", required=False),
        client_order_id=_str(d, "client_order_id", required=False),
        account_id=_req_str(d, "investor_id"),
        instrument_id=_req_str(d, "instrument_id"),
        exchange=_exchange(d),
        direction=_enum(DIRECTION_FROM_WIRE, d, "direction"),
        offset=_enum(OFFSET_FROM_WIRE, d, "offset_flag"),
        price=_float(d, "price"),
        qty=qty,
        trade_ts_ns=_int(d, "trade_ts_ns"),
    )


def order_or_invalid(d: dict[str, Any]) -> OrderReport | InvalidReport:
    try:
        return parse_order(d)
    except WireError as exc:
        seq, td = _seq_td(d)
        return InvalidReport(kind="order", reason=str(exc), broker_seq=seq, trading_day=td, raw=dict(d))


def trade_or_invalid(d: dict[str, Any]) -> TradeReport | InvalidReport:
    try:
        return parse_trade(d)
    except WireError as exc:
        seq, td = _seq_td(d)
        return InvalidReport(kind="trade", reason=str(exc), broker_seq=seq, trading_day=td, raw=dict(d))


def parse_position(d: dict[str, Any]) -> PositionReport:
    """``RspQryInvestorPosition`` item -> :class:`PositionReport` (raises :class:`WireError`)."""
    raw_dir = str(d.get("posi_direction") or "")
    try:
        direction = PosDirection(raw_dir)
    except ValueError:
        raise WireError(f"unknown posi_direction {raw_dir!r}") from None
    return PositionReport(
        account_id=_req_str(d, "investor_id"),
        instrument_id=_req_str(d, "instrument_id"),
        direction=direction,
        td_qty=_int(d, "today_position"),
        yd_qty=_int(d, "yd_position"),
    )


# ------------------------------------------------------------------ requests
def login_msg(req_id: int, *, broker_id: str, user_id: str, app_id: str) -> dict[str, Any]:
    return {"msg": "ReqUserLogin", "req_id": req_id, "broker_id": broker_id, "user_id": user_id, "app_id": app_id}


def order_insert_msg(order: RouteOrder, *, req_id: int, order_ref: str) -> dict[str, Any]:
    """``ReqOrderInsert`` for a routed order (``FORCE_CLOSE`` is broker-only: refused)."""
    if order.offset is Offset.FORCE_CLOSE:
        raise WireError("FORCE_CLOSE cannot be used by clients (02 §7.3)")
    tc, vc = TIF_TO_WIRE[order.tif]
    return {
        "msg": "ReqOrderInsert",
        "req_id": req_id,
        "order_ref": order_ref,
        "client_order_id": order.client_order_id,
        "investor_id": order.account_id,
        "instrument_id": order.instrument_id,
        "exchange_id": order.exchange.value,
        "direction": DIRECTION_TO_WIRE[order.direction],
        "comb_offset_flag": OFFSET_TO_WIRE[order.offset],
        "order_price_type": PRICE_TYPE_TO_WIRE[order.order_type],
        "limit_price": float(order.price) if order.price is not None else 0.0,
        "volume_total_original": order.qty,
        "time_condition": tc,
        "volume_condition": vc,
    }


def order_action_msg(req_id: int, *, broker_order_ref: str | None, order_sys_id: str | None) -> dict[str, Any]:
    """``ReqOrderAction`` (``DELETE``) by ``order_sys_id`` when known, else by front/session/order_ref."""
    msg: dict[str, Any] = {"msg": "ReqOrderAction", "req_id": req_id, "action": "DELETE"}
    parsed = parse_broker_order_ref(broker_order_ref) if broker_order_ref else None
    if parsed is not None:
        msg["front_id"], msg["session_id"], msg["order_ref"] = parsed
    if order_sys_id:
        msg["order_sys_id"] = order_sys_id
    if parsed is None and not order_sys_id:
        raise WireError("a cancel needs broker_order_ref or order_sys_id")
    return msg


def query_msg(kind: str, req_id: int, *, trading_day: str, from_broker_seq: int | None = None,
              investor_id: str | None = None) -> dict[str, Any]:
    """``ReqQryOrder`` / ``ReqQryTrade`` / ``ReqQryInvestorPosition``."""
    msg: dict[str, Any] = {"msg": kind, "req_id": req_id, "trading_day": trading_day}
    if from_broker_seq is not None:
        msg["from_broker_seq"] = from_broker_seq
    if investor_id is not None:
        msg["investor_id"] = investor_id
    return msg


__all__ = [
    "DIRECTION_FROM_WIRE",
    "DIRECTION_TO_WIRE",
    "OFFSET_FROM_WIRE",
    "OFFSET_TO_WIRE",
    "PRICE_TYPE_FROM_WIRE",
    "PRICE_TYPE_TO_WIRE",
    "STATUS_FROM_WIRE",
    "TIF_TO_WIRE",
    "WireError",
    "login_msg",
    "order_action_msg",
    "order_insert_msg",
    "order_or_invalid",
    "parse_order",
    "parse_position",
    "parse_trade",
    "query_msg",
    "trade_or_invalid",
]
