"""The in-process broker's fill rules (the backtest's trade-gateway on the InprocBus)."""

from __future__ import annotations

from typing import Any

from btsupport import base, ts

from futrisk.backtest.broker import InprocBroker
from futrisk.bus.inproc import InprocBus
from futrisk.common.clock import SimClock
from futrisk.core.instruments import InstrumentRegistry
from futrisk.messages import codes
from futrisk.messages.enums import BrokerOrderStatus, Direction, Exchange, Offset, OrderType, TimeInForce
from futrisk.messages.envelope import Envelope, open_payload
from futrisk.messages.orders import BrokerOrderUpdate, RouteAck, RouteCancel, RouteOrder, Trade

TD = "20260929"


class Rig:
    def __init__(self) -> None:
        self.clock = SimClock(ts("2026-09-29 09:35"))
        self.bus = InprocBus("t", clock=self.clock)
        self.reg = InstrumentRegistry(base().products, base().calendar)
        self.broker = InprocBroker(self.bus.endpoint("trade-gateway"), self.reg, self.clock)
        self.broker.begin_day(TD)
        self.execs: list[Any] = []
        self.bus.endpoint("shell").subscribe("trade-gateway", ["td.exec."], self._on)
        self.n = 0

    def _on(self, env: Envelope) -> None:
        self.execs.append(open_payload(env))

    def route(self, direction: Direction, price: float | None, qty: int = 2, *, inst: str = "rb2701",
              order_type: OrderType = OrderType.LIMIT, offset: Offset = Offset.OPEN) -> str:
        self.n += 1
        coid = f"c{self.n}"
        r = RouteOrder(client_order_id=coid, account_id="ACC3", instrument_id=inst, exchange=self.reg.exchange(inst),
                       direction=direction, offset=offset, order_type=order_type, tif=TimeInForce.GFD, price=price, qty=qty,
                       trading_day=TD, risk_passed_ns=self.clock.now_ns(), expire_ns=self.clock.now_ns() + 2_000_000_000,
                       src_instance="risk-engine-bt")
        reply = self.bus.request("trade-gateway", "route.order", r)
        assert reply.ok and reply.open_as(RouteAck).status == BrokerOrderStatus.RECEIVED
        self.broker.match(inst)
        return coid

    def trades(self) -> list[Trade]:
        return [m for m in self.execs if isinstance(m, Trade)]

    def updates(self, coid: str) -> list[BrokerOrderUpdate]:
        return [m for m in self.execs if isinstance(m, BrokerOrderUpdate) and m.client_order_id == coid]


def test_marketable_limit_fills_in_full_at_the_price_point() -> None:
    rig = Rig()
    rig.broker.set_market("rb2701", 3100.0, rig.clock.now_ns())
    coid = rig.route(Direction.BUY, 3102.0, qty=7)
    (t,) = rig.trades()
    assert (t.price, t.qty, t.client_order_id, t.exchange) == (3100.0, 7, coid, Exchange.SHFE)
    assert [u.status for u in rig.updates(coid)] == [BrokerOrderStatus.ACCEPTED, BrokerOrderStatus.FILLED]
    sell = rig.route(Direction.SELL, 3099.0, qty=3, offset=Offset.CLOSE_TODAY)
    assert rig.trades()[-1].price == 3100.0 and rig.trades()[-1].offset == Offset.CLOSE_TODAY
    assert rig.updates(sell)[-1].status == BrokerOrderStatus.FILLED


def test_market_order_fills_at_the_price_point() -> None:
    rig = Rig()
    rig.broker.set_market("rb2701", 3100.0, rig.clock.now_ns())
    rig.route(Direction.SELL, None, order_type=OrderType.MARKET)
    assert rig.trades()[0].price == 3100.0


def test_non_marketable_limit_rests_then_fills_at_its_limit_when_crossed() -> None:
    rig = Rig()
    rig.broker.set_market("rb2701", 3100.0, rig.clock.now_ns())
    coid = rig.route(Direction.BUY, 3090.0)
    assert rig.trades() == [] and rig.broker.working("rb2701")
    rig.clock.advance(60_000_000_000)
    rig.broker.set_market("rb2701", 3095.0, rig.clock.now_ns())
    rig.broker.match("rb2701")
    assert rig.trades() == []
    rig.clock.advance(60_000_000_000)
    rig.broker.set_market("rb2701", 3080.0, rig.clock.now_ns())
    rig.broker.match("rb2701")
    (t,) = rig.trades()
    assert t.price == 3090.0 and t.client_order_id == coid  # the resting order's own price, not 3080
    assert rig.broker.stats.rested == 1


def test_limit_lock_blocks_the_locked_side_until_the_market_reopens() -> None:
    rig = Rig()
    rig.broker.set_market("rb2701", 2945.0, rig.clock.now_ns(), locked_down=True)
    coid = rig.route(Direction.SELL, 2945.0, offset=Offset.CLOSE_YESTERDAY)
    assert rig.trades() == [] and rig.broker.stats.locked_waits == 1
    buy = rig.route(Direction.BUY, 2945.0)  # buyers are welcome at the limit-down price
    assert [t.client_order_id for t in rig.trades()] == [buy]
    rig.clock.advance(60_000_000_000)
    rig.broker.set_market("rb2701", 2960.0, rig.clock.now_ns())
    rig.broker.match("rb2701")
    assert rig.trades()[-1].client_order_id == coid and rig.trades()[-1].price == 2945.0


def test_cancel_and_end_of_day() -> None:
    rig = Rig()
    rig.broker.set_market("rb2701", 3100.0, rig.clock.now_ns())
    a = rig.route(Direction.BUY, 3000.0)
    b = rig.route(Direction.BUY, 3001.0)
    reply = rig.bus.request("trade-gateway", "route.cancel", RouteCancel(command_id="k1", client_order_id=a))
    assert reply.ok and rig.updates(a)[-1].status == BrokerOrderStatus.CANCELED
    again = rig.bus.request("trade-gateway", "route.cancel", RouteCancel(command_id="k2", client_order_id=a))
    assert again.code == codes.BROKER_ORDER_FINISHED
    assert rig.broker.end_day() == 1
    last = rig.updates(b)[-1]
    assert last.status == BrokerOrderStatus.CANCELED and last.broker_code == "SESSION_CLOSE"


def test_duplicate_route_is_idempotent_and_ids_are_deterministic() -> None:
    rig = Rig()
    rig.broker.set_market("rb2701", 3100.0, rig.clock.now_ns())
    coid = rig.route(Direction.BUY, 3100.0)
    r = RouteOrder(client_order_id=coid, account_id="ACC3", instrument_id="rb2701", exchange=Exchange.SHFE,
                   direction=Direction.BUY, offset=Offset.OPEN, order_type=OrderType.LIMIT, tif=TimeInForce.GFD, price=3100.0,
                   qty=2, trading_day=TD, risk_passed_ns=0, expire_ns=0, src_instance="x")
    dup = rig.bus.request("trade-gateway", "route.order", r)
    assert dup.ok and dup.open_as(RouteAck).status == BrokerOrderStatus.FILLED
    assert len(rig.trades()) == 1
    t = rig.trades()[0]
    assert t.trade_id == f"{TD}-T0000001" and t.broker_order_ref == f"{TD}-O0000001" and t.trade_seq == 1
