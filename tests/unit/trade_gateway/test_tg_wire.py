"""SimBroker wire mapping (02 §9.4): report parsing, unknown enums, request builders."""

from __future__ import annotations

import pytest

from futrisk.messages.enums import (
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
from futrisk.services.trade_gateway.broker import (
    InvalidReport,
    OrderReport,
    TradeReport,
    parse_broker_order_ref,
)
from futrisk.services.trade_gateway.broker.wire import (
    WireError,
    order_action_msg,
    order_insert_msg,
    order_or_invalid,
    parse_order,
    parse_position,
    parse_trade,
    trade_or_invalid,
)

RTN_ORDER = {
    "msg": "RtnOrder", "broker_seq": 42, "trading_day": "20260928", "front_id": 1, "session_id": 7,
    "order_ref": "13", "order_sys_id": "0000000031", "client_order_id": "01a0", "investor_id": "ACC3",
    "instrument_id": "rb2701", "exchange_id": "SHFE", "direction": "0", "comb_offset_flag": "0",
    "order_price_type": "LIMIT", "time_condition": "GFD", "volume_condition": "AV", "limit_price": 3120.0,
    "volume_total_original": 2, "volume_traded": 0, "volume_total": 2, "order_status": "ACCEPTED",
    "status_msg": "ACCEPTED", "insert_time": "09:35:01", "update_ns": 1790559301000000000,
}
RTN_TRADE = {
    "msg": "RtnTrade", "broker_seq": 43, "trading_day": "20260928", "trade_id": "0000000019", "front_id": 1,
    "session_id": 7, "order_ref": "13", "order_sys_id": "0000000031", "client_order_id": "01a0",
    "investor_id": "ACC3", "instrument_id": "rb2701", "exchange_id": "SHFE", "direction": "0",
    "offset_flag": "0", "price": 3119.0, "volume": 2, "trade_date": "20260928", "trade_time": "09:35:01",
    "trade_ts_ns": 1790559301020000000,
}


def route(**kw: object) -> RouteOrder:
    base: dict[str, object] = dict(
        client_order_id="c1", account_id="ACC3", instrument_id="rb2701", exchange=Exchange.SHFE, direction=Direction.SELL,
        offset=Offset.CLOSE_YESTERDAY, order_type=OrderType.LIMIT, tif=TimeInForce.GFD, price=3120.0, qty=3,
        trading_day="20260928", risk_passed_ns=1, expire_ns=2, src_instance="risk-engine-1",
    )
    base.update(kw)
    return RouteOrder(**base)  # type: ignore[arg-type]


class TestReports:
    def test_rtn_order_maps_to_an_order_report(self) -> None:
        r = parse_order(RTN_ORDER)
        assert isinstance(r, OrderReport)
        assert (r.broker_seq, r.trading_day, r.broker_order_ref, r.order_sys_id) == (42, "20260928", "1:7:13", "0000000031")
        assert (r.account_id, r.exchange, r.direction, r.offset, r.order_type, r.tif) == (
            "ACC3", Exchange.SHFE, Direction.BUY, Offset.OPEN, OrderType.LIMIT, TimeInForce.GFD)
        assert (r.price, r.qty, r.filled_qty, r.remaining_qty, r.status) == (3120.0, 2, 0, 2, BrokerOrderStatus.ACCEPTED)
        assert r.origin is TradeOrigin.INTERNAL and r.update_ns == 1790559301000000000

    @pytest.mark.parametrize(("wire", "status"), [("PART_TRADED", BrokerOrderStatus.PARTIALLY_FILLED),
                                                  ("ALL_TRADED", BrokerOrderStatus.FILLED),
                                                  ("CANCELED", BrokerOrderStatus.CANCELED),
                                                  ("REJECTED", BrokerOrderStatus.REJECTED)])
    def test_order_status_mapping(self, wire: str, status: BrokerOrderStatus) -> None:
        assert parse_order({**RTN_ORDER, "order_status": wire}).status is status

    def test_market_fak_fok_and_external_orders(self) -> None:
        fak = parse_order({**RTN_ORDER, "order_price_type": "ANY", "limit_price": 0.0, "time_condition": "IOC"})
        assert fak.order_type is OrderType.MARKET and fak.price is None and fak.tif is TimeInForce.FAK
        fok = parse_order({**RTN_ORDER, "time_condition": "IOC", "volume_condition": "CV"})
        assert fok.tif is TimeInForce.FOK
        ext = parse_order({**RTN_ORDER, "client_order_id": None, "comb_offset_flag": "2", "session_id": 0,
                           "order_ref": "FC31"})
        assert ext.origin is TradeOrigin.EXTERNAL and ext.offset is Offset.FORCE_CLOSE and ext.broker_order_ref == "1:0:FC31"

    def test_rtn_trade_maps_to_a_trade_report(self) -> None:
        t = parse_trade(RTN_TRADE)
        assert isinstance(t, TradeReport)
        assert (t.broker_seq, t.trade_id, t.price, t.qty, t.offset, t.trade_ts_ns) == (
            43, "0000000019", 3119.0, 2, Offset.OPEN, 1790559301020000000)
        assert t.trade_key == ("20260928", "SHFE", "0000000019", "BUY")
        ext = parse_trade({**RTN_TRADE, "client_order_id": None, "offset_flag": "2"})
        assert ext.origin is TradeOrigin.EXTERNAL and ext.offset is Offset.FORCE_CLOSE

    @pytest.mark.parametrize("field,value", [("direction", "9"), ("comb_offset_flag", "7"), ("order_status", "LIMBO"),
                                             ("exchange_id", "NYSE"), ("time_condition", "GTC"), ("investor_id", None)])
    def test_unknown_values_become_invalid_reports(self, field: str, value: object) -> None:
        r = order_or_invalid({**RTN_ORDER, field: value})
        assert isinstance(r, InvalidReport) and r.kind == "order"
        assert (r.broker_seq, r.trading_day) == (42, "20260928")

    def test_invalid_trade_keeps_its_sequence_number(self) -> None:
        r = trade_or_invalid({**RTN_TRADE, "offset_flag": "X"})
        assert isinstance(r, InvalidReport) and r.kind == "trade" and r.broker_seq == 43
        no_seq = trade_or_invalid({**RTN_TRADE, "broker_seq": "abc"})
        assert isinstance(no_seq, InvalidReport) and no_seq.broker_seq is None

    def test_positions(self) -> None:
        p = parse_position({"investor_id": "ACC3", "instrument_id": "rb2701", "posi_direction": "SHORT",
                            "today_position": 1, "yd_position": 2, "position": 3})
        assert (p.direction, p.td_qty, p.yd_qty) == (PosDirection.SHORT, 1, 2)
        with pytest.raises(WireError):
            parse_position({"investor_id": "ACC3", "instrument_id": "rb2701", "posi_direction": "2",
                            "today_position": 1, "yd_position": 0})


class TestRequests:
    def test_insert_maps_offsets_and_time_conditions(self) -> None:
        m = order_insert_msg(route(), req_id=5, order_ref="13")
        assert m == {
            "msg": "ReqOrderInsert", "req_id": 5, "order_ref": "13", "client_order_id": "c1", "investor_id": "ACC3",
            "instrument_id": "rb2701", "exchange_id": "SHFE", "direction": "1", "comb_offset_flag": "4",
            "order_price_type": "LIMIT", "limit_price": 3120.0, "volume_total_original": 3,
            "time_condition": "GFD", "volume_condition": "AV",
        }
        fak = order_insert_msg(route(order_type=OrderType.MARKET, price=None, tif=TimeInForce.FAK, offset=Offset.CLOSE_TODAY),
                               req_id=6, order_ref="14")
        assert (fak["order_price_type"], fak["limit_price"], fak["time_condition"], fak["volume_condition"],
                fak["comb_offset_flag"]) == ("ANY", 0.0, "IOC", "AV", "3")
        fok = order_insert_msg(route(tif=TimeInForce.FOK, offset=Offset.OPEN, direction=Direction.BUY), req_id=7, order_ref="15")
        assert (fok["time_condition"], fok["volume_condition"], fok["comb_offset_flag"], fok["direction"]) == ("IOC", "CV", "0", "0")
        with pytest.raises(WireError):
            order_insert_msg(route(offset=Offset.FORCE_CLOSE), req_id=8, order_ref="16")

    def test_cancel_by_reference_or_sys_id(self) -> None:
        assert order_action_msg(3, broker_order_ref="1:7:13", order_sys_id=None) == {
            "msg": "ReqOrderAction", "req_id": 3, "action": "DELETE", "front_id": 1, "session_id": 7, "order_ref": "13"}
        by_sys = order_action_msg(4, broker_order_ref=None, order_sys_id="0000000031")
        assert by_sys == {"msg": "ReqOrderAction", "req_id": 4, "action": "DELETE", "order_sys_id": "0000000031"}
        with pytest.raises(WireError):
            order_action_msg(5, broker_order_ref="IMPORT:x", order_sys_id=None)
        assert parse_broker_order_ref("1:0:FC31") == (1, 0, "FC31") and parse_broker_order_ref("IMPORT:x") is None
