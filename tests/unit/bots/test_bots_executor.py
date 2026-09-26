"""Executor: targets -> orders, closes first, SHFE offsets, slicing, rate, cancels, in-doubt, reject gating."""

from __future__ import annotations

import random
from typing import Any

import pytest
from botstest_support import T0, S, tick

from futrisk.client.orders import ClientState, OrderBook
from futrisk.client.positions import PositionTracker
from futrisk.client.pricing import ContractTable
from futrisk.common.ids import uuid7
from futrisk.messages import codes
from futrisk.messages.enums import (
    AccountState,
    Direction,
    Exchange,
    Offset,
    OrderSource,
    OrderStatus,
    OrderType,
    PosDirection,
    RestrictionKind,
    TimeInForce,
)
from futrisk.messages.envelope import Reply, err_reply, ok_reply
from futrisk.messages.orders import OrderAck, OrderRequest, OrderUpdate
from futrisk.messages.risk import Restriction
from futrisk.sim.bots.config import ExecutionConfig, GatesConfig
from futrisk.sim.bots.executor import CancelAction, Executor, SubmitAction, TokenBucket
from futrisk.sim.bots.gates import Gates

BUY, SELL = Direction.BUY, Direction.SELL
LONG, SHORT = PosDirection.LONG, PosDirection.SHORT


def new_order(account: str, instrument: str, direction: Any, offset: Any, qty: int, price: float | None, *,
              strategy_tag: str | None = None, source: OrderSource = OrderSource.BOT) -> OrderRequest:
    return OrderRequest(client_order_id=uuid7(), account_id=account, instrument_id=instrument,
                        direction=Direction(direction), offset=Offset(offset), price=price, qty=qty, source=source,
                        strategy_tag=strategy_tag, sent_ns=1)


class Rig:
    """Executor + book + tracker; ``submit`` mimics the host/client round trip."""

    def __init__(self, contracts: ContractTable, tracker: PositionTracker, *, account: str = "ACC3", **cfg: Any) -> None:
        base = {"passive_prob": 0.0, "aggression_ticks": [1], "max_orders_per_min": 600, "burst": 100}
        base.update(cfg)
        self.book = OrderBook()
        self.tracker = tracker
        self.gates = Gates(account, GatesConfig())
        self.ex = Executor(account, contracts=contracts, positions=tracker, orders=self.book, gates=self.gates,
                           cfg=ExecutionConfig(**base), new_order=new_order, rng=random.Random(7))
        self.account = account

    def decide(self, instrument: str, target: int | None, t_s: float, price: float = 3120.0, **kw: Any) -> Any:
        return self.ex.decide(instrument, target, tick(instrument, price, t_s, **kw), T0 + int(t_s * S))

    def result(self, act: SubmitAction, reply: Reply, t_s: float) -> None:
        req = act.request
        self.book.add_request(req)
        ack = reply.open_as(OrderAck) if reply.data_type == "OrderAck" else None
        self.book.on_submit_reply(req.client_order_id, reply, ack)
        self.ex.on_submit_result(req, reply, ack, T0 + int(t_s * S))

    def accept(self, act: SubmitAction, t_s: float) -> None:
        req = act.request
        self.result(act, ok_reply(OrderAck(client_order_id=req.client_order_id, accepted=True,
                                           status=OrderStatus.ROUTING, trading_day="20260928")), t_s)

    def reject(self, act: SubmitAction, code: str, t_s: float, detail: dict | None = None) -> None:
        req = act.request
        ack = OrderAck(client_order_id=req.client_order_id, accepted=False, status=OrderStatus.REJECTED,
                       reject_code=code, reject_detail=detail, trading_day="20260928")
        self.result(act, err_reply(code, data=ack), t_s)

    def fill(self, act: SubmitAction, qty: int | None = None, status: OrderStatus = OrderStatus.FILLED) -> None:
        req = act.request
        n = req.qty if qty is None else qty
        upd = OrderUpdate(client_order_id=req.client_order_id, account_id=req.account_id,
                          instrument_id=req.instrument_id, exchange=Exchange.SHFE, direction=req.direction,
                          offset=req.offset, order_type=OrderType.LIMIT, tif=TimeInForce.GFD, price=req.price,
                          qty=req.qty, status=status, filled_qty=n, avg_fill_price=req.price, source=req.source,
                          trading_day="20260928", created_ns=1, updated_ns=T0)
        o, f = self.book.apply_update(upd)
        if f is not None:
            self.tracker.apply_fill(req.account_id, req.instrument_id, req.direction, req.offset, f.qty, f.price)
        self.ex.on_order_event(o, upd, f.qty if f else 0, T0)


@pytest.fixture
def rig(contracts: ContractTable, tracker: PositionTracker) -> Rig:
    return Rig(contracts, tracker)


def test_open_then_hold_then_ttl_cancel(rig: Rig) -> None:
    act = rig.decide("rb2701", 10, 0)
    assert isinstance(act, SubmitAction) and act.kind == "open"
    r = act.request
    assert (r.direction, r.offset, r.qty, r.price, r.order_type) == (BUY, Offset.OPEN, 10, 3122.0, OrderType.LIMIT)
    assert rig.decide("rb2701", 10, 0.5) is None  # RPC in flight
    rig.accept(act, 0.5)
    assert rig.decide("rb2701", 10, 5) is None  # young working order: wait
    cancel = rig.decide("rb2701", 10, 21)
    assert isinstance(cancel, CancelAction) and cancel.client_order_id == r.client_order_id and "ttl" in cancel.why


def test_wrong_side_or_oversized_working_order_is_cancelled(rig: Rig) -> None:
    act = rig.decide("rb2701", 10, 0)
    rig.accept(act, 0)
    c = rig.decide("rb2701", -5, 1)
    assert isinstance(c, CancelAction) and "target moved" in c.why
    rig.ex.inflight.clear()
    rig.book.get(act.request.client_order_id).cancel_requested = False
    c = rig.decide("rb2701", 4, 2)
    assert isinstance(c, CancelAction) and "oversized" in c.why


def test_close_first_and_shfe_offsets(rig: Rig) -> None:
    rig.tracker.set_leg("ACC3", "rb2701", SHORT, td=5)
    act = rig.decide("rb2701", 10, 0)
    assert isinstance(act, SubmitAction) and act.kind == "close"
    assert (act.request.direction, act.request.offset, act.request.qty) == (BUY, Offset.CLOSE_TODAY, 5)
    rig.accept(act, 0)
    rig.fill(act)
    nxt = rig.decide("rb2701", 10, 1)
    assert (nxt.request.offset, nxt.request.qty) == (Offset.OPEN, 10)
    # long 5 yesterday + 3 today, go flat: CLOSE_YESTERDAY first, then CLOSE_TODAY (SHFE 平昨/平今)
    rig2 = Rig(rig.ex.contracts, PositionTracker(rig.ex.contracts, "20260928"))
    rig2.tracker.set_leg("ACC3", "rb2701", LONG, td=3, yd=5)
    a1 = rig2.decide("rb2701", 0, 0)
    assert (a1.request.direction, a1.request.offset, a1.request.qty, a1.request.price) == (
        SELL, Offset.CLOSE_YESTERDAY, 5, 3118.0)
    rig2.accept(a1, 0)
    rig2.fill(a1)
    a2 = rig2.decide("rb2701", 0, 1)
    assert (a2.request.offset, a2.request.qty) == (Offset.CLOSE_TODAY, 3)
    # DCE: a single CLOSE
    rig2.tracker.set_leg("ACC3", "i2701", LONG, td=2, yd=2)
    a3 = rig2.decide("i2701", 1, 2, price=780.0)
    assert (a3.request.offset, a3.request.qty) == (Offset.CLOSE, 3)


def test_slicing_deadband_and_rate(contracts: ContractTable, tracker: PositionTracker) -> None:
    rig = Rig(contracts, tracker, max_orders_per_min=6, burst=2)
    act = rig.decide("rb2701", 200, 0)
    assert act.request.qty == 48  # min(max_slice_lots 50, floor(1.5M / 31,200))
    rig.accept(act, 0)
    rig.fill(act)
    a2 = rig.decide("hc2701", 20, 0, price=3260.0)
    assert a2 is not None
    rig.accept(a2, 0)
    rig.fill(a2)
    assert rig.decide("i2701", 10, 0.5, price=780.0) is None  # bucket empty (burst 2)
    assert rig.decide("i2701", 10, 10.5, price=780.0) is not None  # 6/min refills one token in 10 s
    tracker.set_leg("ACC3", "j2701", LONG, td=95)
    assert rig.decide("j2701", 100, 30, price=1650.0) is None  # 5 < 10% of the target: skip
    assert rig.decide("j2701", 0, 30, price=1650.0) is not None  # exits are never skipped


def test_halted_stops_opening_closes_continue_halt_probes(rig: Rig) -> None:
    act = rig.decide("rb2701", 20, 0)
    rig.reject(act, codes.REJECT_ACCOUNT_HALTED, 0.1, {"halted_latched": True})
    assert rig.gates.halted
    assert rig.decide("rb2701", 20, 1) is None  # no more opening orders ...
    assert rig.decide("hc2701", 5, 1, price=3260.0) is None
    rig.tracker.set_leg("ACC3", "i2701", LONG, td=8)
    close = rig.decide("i2701", 4, 2, price=780.0)  # ... but closes go out (02 §11.2 exemption)
    assert isinstance(close, SubmitAction) and close.request.offset == Offset.CLOSE and close.request.qty == 4
    rig.accept(close, 2)
    rig.fill(close)
    assert rig.gates.halted  # an accepted close does not prove the halt is gone
    # one 1-lot opening probe per halt_probe_s (20 s) after the reject, in the target's direction - also
    # when the target is already held (after the halt trim): the halt stays visible, not a storm
    probe = rig.decide("i2701", 4, 20.2, price=780.0)
    assert isinstance(probe, SubmitAction) and probe.kind == "halt_probe" and probe.request.qty == 1
    assert (probe.request.direction, probe.request.offset) == (BUY, Offset.OPEN)
    rig.reject(probe, codes.REJECT_ACCOUNT_HALTED, 20.3)
    assert rig.decide("rb2701", 20, 30) is None and rig.decide("hc2701", 5, 39, price=3260.0) is None
    probe2 = rig.decide("rb2701", 20, 40.5)
    assert probe2 is not None and probe2.kind == "halt_probe"
    rig.accept(probe2, 40.6)  # released (risk.reset / new day): the probe went through
    rig.fill(probe2)
    nxt = rig.decide("rb2701", 20, 41)
    assert nxt.kind == "open" and nxt.request.qty == 19 and not rig.gates.halted
    assert rig.ex.stats["halt_probe"] == 2


def test_halt_from_published_state_probes_at_once_and_ends_with_a_newer_state(rig: Rig) -> None:
    rig.gates.apply_risk(AccountState.HALTED, True, [], T0, version=7)  # risk.state: no opening attempt needed
    probe = rig.decide("rb2701", 30, 0)
    assert probe is not None and probe.kind == "halt_probe" and probe.request.qty == 1  # visible at once
    rig.reject(probe, codes.REJECT_ACCOUNT_HALTED, 0.1)
    assert rig.decide("rb2701", 30, 10) is None
    rig.gates.apply_risk(AccountState.NORMAL, False, [], T0 + 15 * S, version=8)  # risk.reset
    act = rig.decide("rb2701", 30, 15)
    assert act is not None and act.kind == "open" and act.request.qty == 30


def test_published_size_factor_and_blocks(rig: Rig) -> None:
    lim = [Restriction(kind=RestrictionKind.ORDER_SIZE_FACTOR, scope_key="acct:ACC3", value=0.5, source="daily_loss/restrict"),
           Restriction(kind=RestrictionKind.BLOCK_OPEN, scope_key="acct:ACC3/prod:hc", source="product_exposure/block")]
    rig.gates.apply_risk(AccountState.RESTRICTED, False, lim, T0, version=3)
    act = rig.decide("rb2701", 30, 0)
    assert act is not None and act.request.qty == 15 and "ORDER_SIZE_FACTOR" in act.why  # visibly halved
    assert rig.decide("hc2701", 10, 0, price=3260.0) is None  # the engine would say REJECT_PRODUCT_BLOCKED
    rig.tracker.set_leg("ACC3", "hc2701", LONG, td=10)
    close = rig.decide("hc2701", 4, 1, price=3260.0)
    assert close is not None and close.request.qty == 6  # closes are never scaled nor blocked


def test_add_to_loser_and_size_restriction(rig: Rig) -> None:
    act = rig.decide("rb2701", 30, 0)
    rig.reject(act, codes.REJECT_ADD_TO_LOSER, 0)
    assert rig.decide("rb2701", 30, 1) is None
    other = rig.decide("hc2701", 30, 1, price=3260.0)
    assert other is not None and other.request.qty == 30
    rig.reject(other, codes.REJECT_ORDER_SIZE_RESTRICTED, 1, {"max": 7})
    again = rig.decide("hc2701", 30, 4, price=3260.0)
    assert again is not None and again.request.qty == 7


def test_in_doubt_resubmitted_with_same_id_then_given_up(rig: Rig) -> None:
    act = rig.decide("rb2701", 10, 0)
    rig.result(act, Reply(ok=False, code=codes.TIMEOUT), 0.6)
    o = rig.book.get(act.request.client_order_id)
    assert o.state == ClientState.IN_DOUBT
    assert rig.decide("rb2701", 10, 1) is None  # transport back-off / wait
    again = rig.decide("rb2701", 10, 4)
    assert isinstance(again, SubmitAction) and again.kind == "resubmit" and again.request is act.request
    rig.result(again, Reply(ok=False, code=codes.UNAVAILABLE), 4.1)
    assert rig.decide("rb2701", 10, 16) is None  # given up (lost) on this pass ...
    assert rig.book.get(act.request.client_order_id).state == ClientState.LOST
    assert rig.ex.stats["lost"] == 1
    fresh = rig.decide("rb2701", 10, 17)  # ... the strategy re-decides afterwards with a new id
    assert isinstance(fresh, SubmitAction) and fresh.request.client_order_id != act.request.client_order_id


def test_broker_reject_and_breaker_cancel_updates(rig: Rig) -> None:
    act = rig.decide("rb2701", 10, 0)
    rig.accept(act, 0)
    upd = OrderUpdate(client_order_id=act.request.client_order_id, account_id="ACC3", instrument_id="rb2701",
                      exchange=Exchange.SHFE, direction=BUY, offset=Offset.OPEN, order_type=OrderType.LIMIT,
                      tif=TimeInForce.GFD, qty=10, status=OrderStatus.BROKER_REJECTED, reject_code=codes.ROUTE_KILL_SWITCH,
                      source=OrderSource.BOT, trading_day="20260928", created_ns=1, updated_ns=T0)
    o, _ = rig.book.apply_update(upd)
    rig.ex.on_order_event(o, upd, 0, T0)
    assert not rig.gates.open_state("rb", "rb2701", T0 + S).allowed
    brk = OrderUpdate(**{**{f: getattr(upd, f) for f in upd.__struct_fields__}, "client_order_id": "foreign",
                         "status": OrderStatus.CANCELED, "reject_code": None, "reason": "CIRCUIT_BREAK_CANCEL"})
    o2, _ = rig.book.apply_update(brk)
    rig.ex.on_order_event(o2, brk, 0, T0)
    assert rig.gates.halted


def test_no_open_into_a_locked_market(rig: Rig) -> None:
    locked = tick("rb2701", 3276.0, 0, bid=3276.0, upper=3276.0, lower=2964.0)
    import msgspec

    locked = msgspec.structs.replace(locked, ask1=None)
    assert rig.ex.decide("rb2701", 10, locked, T0) is None


def test_token_bucket() -> None:
    b = TokenBucket(6, 2)
    assert b.take(0) and b.take(1) and not b.take(2)
    assert b.take(2 + 10 * S) and not b.available(2 + 10 * S)


def test_foreign_working_close_counts_before_opening(rig: Rig) -> None:
    """A manual close already working (api-gateway order) is netted out; the bot never touches it."""
    rig.tracker.set_leg("ACC3", "rb2701", LONG, td=10)
    manual = OrderUpdate(client_order_id="manual-close", account_id="ACC3", instrument_id="rb2701",
                         exchange=Exchange.SHFE, direction=SELL, offset=Offset.CLOSE_TODAY, order_type=OrderType.LIMIT,
                         tif=TimeInForce.GFD, price=3119.0, qty=10, status=OrderStatus.ACCEPTED,
                         source=OrderSource.MANUAL, trading_day="20260928", created_ns=1, updated_ns=T0)
    rig.book.apply_update(manual)
    act = rig.decide("rb2701", -5, 100)  # ancient foreign order: never cancelled by the bot
    assert isinstance(act, SubmitAction) and (act.request.direction, act.request.offset, act.request.qty) == (
        SELL, Offset.OPEN, 5)
    assert rig.decide("hc2701", 0, 100, price=3260.0) is None
