"""Order lifecycle from the risk engine's view (02 §8.3; 03 §4.3 orders / order_events / fills)."""

from __future__ import annotations

import pytest
from enginetest_support import CT, CY, RM, TD, B, Harness, O, S, rules_only, sod_snapshot

from futrisk.core.effects import CancelRoute, Effect, Persist, Request, ResolveRoute, Route
from futrisk.core.risk_account import Lifecycle
from futrisk.messages import codes
from futrisk.messages.enums import (
    AccountState,
    BrokerOrderStatus,
    Offset,
    OrderSource,
    OrderStatus,
    PosDirection,
    ReconBreakKind,
    RiskEventKind,
    Severity,
    TradeOrigin,
)
from futrisk.messages.envelope import err_reply, ok_reply
from futrisk.messages.eod import SodLot
from futrisk.messages.orders import (
    BrokerOrderBatch,
    BrokerOrderUpdate,
    CancelAllAck,
    CancelAllRequest,
    CancelRequest,
    OrderUpdate,
    RouteAck,
)
from futrisk.messages.risk import HaltRequest


def book() -> Harness:
    sod = sod_snapshot(lots={"ACC3": [SodLot(instrument_id="rb2701", direction=PosDirection.LONG, lot_no=1,
                                             open_td="20260928", open_ts_ns=1, open_price="3100", qty=10,
                                             basis_price="3100")]})
    h = Harness(rules_only("daily_loss"), sod=sod)
    h.ticks({"rb2701": 3100.0, "hc2701": 3300.0, "IF2612": 4500.0})
    return h


def status(h: Harness, coid: str) -> OrderStatus:
    o = h.eng.orders.get(coid)
    assert o is not None
    return o.status


def updates(h: Harness, coid: str) -> list[OrderUpdate]:
    return [p for p in h.outbox_payloads() if isinstance(p, OrderUpdate) and p.client_order_id == coid]


class TestHappyPath:
    def test_submit_route_accept_partial_fill_frozen_release(self) -> None:
        h = book()
        rep, effs = h.submit("ACC3", "IF2612", B, O, 4, 4500.0, coid="c1")
        assert rep.ok and status(h, "c1") == OrderStatus.ROUTING
        persist = [e for e in effs if isinstance(e, Persist)][0]
        assert persist.tables() == ["order_events", "orders"]
        route = [e for e in effs if isinstance(e, Route)][0].order
        assert route.expire_ns - route.risk_passed_ns == 2_000_000_000 and route.src_instance == "risk-engine-1"
        f = h.eng.snapshot("ACC3").funds
        assert f.frozen_margin == pytest.approx(4 * 4500 * 300 * 0.12) and f.frozen_fee > 0
        h.route_ok("c1")
        assert status(h, "c1") == OrderStatus.ROUTED
        h.broker("c1", BrokerOrderStatus.ACCEPTED, remaining=4)
        assert status(h, "c1") == OrderStatus.ACCEPTED
        h.fill("c1", 4500.0, 1)
        assert status(h, "c1") == OrderStatus.PARTIALLY_FILLED
        assert h.eng.snapshot("ACC3").funds.frozen_margin == pytest.approx(3 * 4500 * 300 * 0.12)
        h.fill("c1", 4500.2, 3)
        o = h.eng.orders.get("c1")
        assert o.status == OrderStatus.FILLED and o.filled_qty == 4
        assert o.avg_fill_price == pytest.approx((4500 + 3 * 4500.2) / 4)
        f = h.eng.snapshot("ACC3").funds
        assert f.frozen_margin == 0 and f.frozen_fee == 0 and f.margin > 0
        assert [u.status for u in updates(h, "c1")] == [OrderStatus.ROUTING, OrderStatus.ROUTED, OrderStatus.ACCEPTED,
                                                         OrderStatus.PARTIALLY_FILLED, OrderStatus.FILLED]
        fills = [r for r in h.db.tables["fills"] if r["client_order_id"] == "c1"]
        assert len(fills) == 2 and fills[0]["fill_seq"] < fills[1]["fill_seq"] and fills[0]["fee_est"]
        ev = [r for r in h.db.tables["order_events"] if r["client_order_id"] == "c1"]
        assert ev[0]["from_status"] is None and ev[-1]["to_status"] == "FILLED"

    def test_fill_in_routing_state_counts_as_accepted(self) -> None:
        h = book()
        h.submit("ACC3", "IF2612", B, O, 2, 4500.0, coid="c1")
        h.fill("c1", 4500.0, 2)
        assert status(h, "c1") == OrderStatus.FILLED
        h.route_ok("c1")  # late RouteAck on a terminal order changes nothing
        assert status(h, "c1") == OrderStatus.FILLED

    def test_duplicate_trade_is_ignored(self) -> None:
        h = book()
        h.submit("ACC3", "IF2612", B, O, 2, 4500.0, coid="c1")
        t = h.trade("ACC3", "IF2612", B, O, 4500.0, 1, coid="c1")
        h.run(h.eng.apply_exec(t))
        assert h.run(h.eng.apply_exec(t)) == []
        assert h.eng.orders.get("c1").filled_qty == 1


class TestRouting:
    def test_route_rejected_releases_frozen(self) -> None:
        h = book()
        h.submit("ACC3", "IF2612", B, O, 2, 4500.0, coid="c1")
        h.route_err("c1", codes.ROUTE_KILL_SWITCH)
        o = h.eng.orders.get("c1")
        assert o.status == OrderStatus.BROKER_REJECTED and o.reject_code == codes.ROUTE_KILL_SWITCH
        assert h.eng.snapshot("ACC3").funds.frozen_margin == 0

    def test_transport_errors_do_not_change_the_order(self) -> None:
        h = book()
        h.submit("ACC3", "IF2612", B, O, 2, 4500.0, coid="c1")
        assert h.run(h.eng.on_route_ack("c1", err_reply(codes.TIMEOUT))) == []
        assert status(h, "c1") == OrderStatus.ROUTING
        assert h.run(h.eng.on_route_timeout("c1")) == []  # expire_ns not reached yet

    def test_expiry_unknown_resolve_expired_is_route_lost(self) -> None:
        h = book()
        h.submit("ACC3", "IF2612", B, O, 2, 4500.0, coid="c1")
        h.advance(2001)
        effs = h.timer()
        assert status(h, "c1") == OrderStatus.UNKNOWN
        assert [e.client_order_id for e in effs if isinstance(e, ResolveRoute)] == ["c1"]
        assert updates(h, "c1")[-1].reason == "ROUTE_TIMEOUT"
        assert h.eng.snapshot("ACC3").funds.frozen_margin > 0  # UNKNOWN 仍冻结
        h.run(h.eng.on_resolve_result("c1", ok_reply(RouteAck(client_order_id="c1", status=BrokerOrderStatus.EXPIRED))))
        o = h.eng.orders.get("c1")
        assert o.status == OrderStatus.BROKER_REJECTED and o.reject_code == codes.ROUTE_LOST
        assert h.eng.snapshot("ACC3").funds.frozen_margin == 0

    def test_resolve_found_and_retry(self) -> None:
        h = book()
        h.submit("ACC3", "IF2612", B, O, 2, 4500.0, coid="c1")
        h.advance(2001)
        h.timer()
        h.run(h.eng.on_resolve_result("c1", err_reply(codes.TIMEOUT)))
        assert status(h, "c1") == OrderStatus.UNKNOWN
        h.advance(5001)
        effs = h.timer()
        assert [e.client_order_id for e in effs if isinstance(e, ResolveRoute)] == ["c1"]
        h.run(h.eng.on_resolve_result("c1", ok_reply(RouteAck(client_order_id="c1", status=BrokerOrderStatus.RECEIVED))))
        assert status(h, "c1") == OrderStatus.ROUTED

    def test_gateway_down_marks_in_flight_orders_unknown(self) -> None:
        h = book()
        h.submit("ACC3", "IF2612", B, O, 2, 4500.0, coid="c1")
        h.run(h.eng.set_gateway_status(False))
        assert status(h, "c1") == OrderStatus.UNKNOWN
        h.advance(10_000)
        assert not [e for e in h.timer() if isinstance(e, ResolveRoute)]  # 网关不可用：不做无用的确认
        effs = h.run(h.eng.set_gateway_status(True))  # 恢复时立即重新驱动
        assert [e.client_order_id for e in effs if isinstance(e, ResolveRoute)] == ["c1"]
        assert not [e for e in h.timer() if isinstance(e, ResolveRoute)]  # 已在途


class TestCancel:
    def test_cancel_flow_and_idempotency(self) -> None:
        h = book()
        h.submit("ACC3", "IF2612", B, O, 2, 4500.0, coid="c1")
        h.route_ok("c1")
        req = CancelRequest(command_id="k1", client_order_id="c1", account_id="ACC3", reason="user")
        rep, effs = h.cmd("order.cancel", req)
        assert rep.ok and status(h, "c1") == OrderStatus.CANCEL_PENDING
        assert [(e.client_order_id, e.command_id) for e in effs if isinstance(e, CancelRoute)] == [("c1", "k1")]
        rep2, effs2 = h.cmd("order.cancel", req)
        assert rep2.duplicate and not [e for e in effs2 if isinstance(e, CancelRoute)]
        h.broker("c1", BrokerOrderStatus.CANCELED, remaining=0)
        assert status(h, "c1") == OrderStatus.CANCELED
        assert h.eng.snapshot("ACC3").funds.frozen_margin == 0
        rep3, _ = h.cmd("order.cancel", CancelRequest(command_id="k2", client_order_id="c1", account_id="ACC3"))
        assert rep3.code == codes.CONFLICT
        rep4, _ = h.cmd("order.cancel", CancelRequest(command_id="k3", client_order_id="nope", account_id="ACC3"))
        assert rep4.code == codes.NOT_FOUND
        assert h.eng.account("ACC3").cancels_today == 1

    def test_cancel_refused_restatement_returns_to_the_working_state(self) -> None:
        h = book()
        h.submit("ACC3", "IF2612", B, O, 2, 4500.0, coid="c1")
        h.route_ok("c1")
        h.broker("c1", BrokerOrderStatus.ACCEPTED, remaining=2)
        h.cmd("order.cancel", CancelRequest(command_id="k1", client_order_id="c1", account_id="ACC3"))
        assert h.run(h.eng.on_cancel_result("c1", ok_reply(RouteAck(client_order_id="c1",
                                                                     status=BrokerOrderStatus.CANCEL_PENDING)))) == []
        # a normal report racing the cancel (no broker_code) is ignored
        h.broker("c1", BrokerOrderStatus.ACCEPTED, remaining=2)
        assert status(h, "c1") == OrderStatus.CANCEL_PENDING
        # the gateway's restatement of a refused cancel: last broker state + broker_code, broker_seq 0
        h.broker("c1", BrokerOrderStatus.ACCEPTED, remaining=2, code=codes.BROKER_ORDER_NOT_FOUND)
        assert status(h, "c1") == OrderStatus.ACCEPTED
        u = updates(h, "c1")[-1]
        assert u.reason == codes.BROKER_ORDER_NOT_FOUND and u.status == OrderStatus.ACCEPTED
        ev = [r for r in h.db.tables["order_events"] if r["client_order_id"] == "c1"][-1]
        assert ev["detail"]["cancel_refused"] is True
        assert h.eng.snapshot("ACC3").funds.frozen_margin > 0  # still working: still frozen

    def test_cancel_of_never_received_order_is_canceled_on_tombstone(self) -> None:
        h = book()
        h.submit("ACC3", "IF2612", B, O, 2, 4500.0, coid="c1")  # still ROUTING
        h.cmd("order.cancel", CancelRequest(command_id="k1", client_order_id="c1", account_id="ACC3"))
        h.run(h.eng.on_cancel_result("c1", ok_reply(RouteAck(client_order_id="c1", status=BrokerOrderStatus.EXPIRED))))
        assert status(h, "c1") == OrderStatus.CANCELED
        h.route_err("c1", codes.ROUTE_LOST)  # the late route.order is refused: nothing changes
        assert status(h, "c1") == OrderStatus.CANCELED

    def test_cancel_all_open_only(self) -> None:
        h = book()
        h.submit("ACC3", "IF2612", B, O, 1, 4500.0, coid="o1")
        h.submit("ACC3", "hc2701", B, O, 1, 3300.0, coid="o2")
        h.submit("ACC3", "rb2701", S, CY, 1, 3100.0, coid="x1")
        rep, effs = h.cmd("order.cancel_all", CancelAllRequest(command_id="ca", account_id="ACC3", scope="OPEN_ONLY",
                                                                 reason="flat", actor="t"))
        assert rep.open_as(CancelAllAck).count == 2
        assert {e.client_order_id for e in effs if isinstance(e, CancelRoute)} == {"o1", "o2"}
        assert status(h, "x1") == OrderStatus.ROUTING


class TestFillsAndReservations:
    def test_fill_after_terminal_is_applied_with_a_non_blocking_break(self) -> None:
        h = book()
        h.submit("ACC3", "IF2612", B, O, 2, 4500.0, coid="c1")
        h.route_ok("c1")
        h.cmd("order.cancel", CancelRequest(command_id="k1", client_order_id="c1", account_id="ACC3"))
        h.broker("c1", BrokerOrderStatus.CANCELED)
        effs = h.fill("c1", 4500.0, 2)
        o = h.eng.orders.get("c1")
        assert o.status == OrderStatus.FILLED and o.filled_qty == 2  # 按累计成交量改正
        breaks = [b for b in h.eng.breaks.values() if b.kind == ReconBreakKind.ORDER_STATE_CONFLICT]
        assert len(breaks) == 1 and not breaks[0].blocking
        ev = h.events(RiskEventKind.RECON_BREAK, effs)
        assert ev and ev[0].detail["break_id"] == breaks[0].break_id and ev[0].detail["blocking"] is False
        pos = [p for p in h.eng.snapshot("ACC3").positions if p.instrument_id == "IF2612"]
        assert pos[0].td_qty == 2
        assert not any(r.source == "SYSTEM:RECON_BREAK" for r in h.eng.account("ACC3").restrictions)

    def test_external_force_close(self) -> None:
        h = book()
        t = h.trade("ACC3", "rb2701", S, Offset.FORCE_CLOSE, 3090.0, 4, origin=TradeOrigin.EXTERNAL)
        effs = h.run(h.eng.apply_exec(t))
        ev = h.events(RiskEventKind.EXTERNAL_FILL, effs)
        assert len(ev) == 1 and ev[0].severity == Severity.HIGH
        pos = [p for p in h.eng.snapshot("ACC3").positions if p.instrument_id == "rb2701"][0]
        assert pos.yd_qty == 6
        row = h.db.tables["fills"][-1]
        assert row["origin"] == "EXTERNAL" and row["close_yd_qty"] == 4 and row["client_order_id"] is None

    def test_unknown_internal_order_is_rebuilt_from_the_broker(self) -> None:
        h = book()
        t = h.trade("ACC3", "IF2612", B, O, 4500.0, 1, coid="lost-1")
        effs = h.run(h.eng.apply_exec(t))
        req = [e for e in effs if isinstance(e, Request)][0]
        assert req.command == "td.query.orders" and req.target == "trade-gateway"
        batch = BrokerOrderBatch(items=[BrokerOrderUpdate(client_order_id="lost-1", account_id="ACC3", instrument_id="IF2612",
                                                           broker_order_ref="r", status=BrokerOrderStatus.PARTIALLY_FILLED,
                                                           filled_qty=1, remaining_qty=1, broker_seq=9, trading_day=TD,
                                                           updated_ns=1)])
        h.run(h.eng.on_request_result(req.token, ok_reply(batch)))
        o = h.eng.orders.get("lost-1")
        assert o is not None and o.qty == 2 and o.filled_qty == 1 and o.source == OrderSource.API
        assert o.status == OrderStatus.PARTIALLY_FILLED

    def test_close_reservation_follows_the_order(self) -> None:
        h = book()
        rep, _ = h.submit("ACC3", "rb2701", S, CY, 6, coid="x1")
        pv = lambda: [p for p in h.eng.snapshot("ACC3").positions if p.instrument_id == "rb2701"][0]  # noqa: E731
        assert pv().closable_yd_qty == 4
        h.route_ok("x1")
        h.fill("x1", 3100.0, 2)
        assert (pv().yd_qty, pv().closable_yd_qty) == (8, 4)
        h.cmd("order.cancel", CancelRequest(command_id="k", client_order_id="x1", account_id="ACC3"))
        h.broker("x1", BrokerOrderStatus.CANCELED)
        assert pv().closable_yd_qty == 8
        assert h.submit("ACC3", "rb2701", S, CT, 1)[0].code == codes.REJECT_CLOSE_TODAY_EXCEEDS


# ---------------------------------------------------------------------------- C1: pending cancels
S_ = 1000  # ms


def cancels(effs: list[Effect]) -> list[CancelRoute]:
    return [e for e in effs if isinstance(e, CancelRoute)]


def resolves(effs: list[Effect]) -> list[str]:
    return [e.client_order_id for e in effs if isinstance(e, ResolveRoute)]


def ack(coid: str, st: BrokerOrderStatus) -> RouteAck:
    return RouteAck(client_order_id=coid, status=st, broker_order_ref=f"ref-{coid}")


def working(h: Harness, coid: str = "c1", qty: int = 2) -> None:
    h.submit("ACC3", "IF2612", B, O, qty, 4500.0, coid=coid)
    h.route_ok(coid)
    h.broker(coid, BrokerOrderStatus.ACCEPTED, remaining=qty)


def halt(h: Harness, account: str = "ACC3", cid: str = "h1") -> list[Effect]:
    return h.cmd("risk.halt", HaltRequest(command_id=cid, account_id=account, reason="test", actor="rm", role=RM))[1]


class TestPendingCancel:
    def test_ttl_resolve_then_resend_with_a_new_command_id(self) -> None:
        h = book()
        working(h)
        _, effs = h.cmd("order.cancel", CancelRequest(command_id="k1", client_order_id="c1", account_id="ACC3"))
        assert [(c.command_id, c.reason) for c in cancels(effs)] == [("k1", "MANUAL_CANCEL")]
        assert h.run(h.eng.on_cancel_result("c1", ok_reply(ack("c1", BrokerOrderStatus.CANCEL_PENDING)))) == []
        row = [r for r in h.db.tables["orders"] if r["client_order_id"] == "c1"][0]
        assert (row["cancel_command_id"], row["cancel_attempts"]) == ("k1", 1) and row["cancel_sent_ns"]
        h.advance(5 * S_ - 1)
        assert not resolves(h.timer())
        assert h.eng.next_timer_ns() is not None and h.eng.next_timer_ns() <= h.clock.now_ns() + 1_000_000  # type: ignore[operator]
        h.advance(1)
        assert resolves(h.timer()) == ["c1"]  # TTL: confirm the broker state first
        assert not resolves(h.timer())  # in flight
        effs = h.run(h.eng.on_resolve_result("c1", ok_reply(ack("c1", BrokerOrderStatus.CANCEL_PENDING))))
        (c2,) = cancels(effs)  # still working at the broker: re-sent with a NEW command id
        assert c2.command_id not in ("k1",) and c2.reason == "CANCEL_RETRY"
        row = [r for r in h.db.tables["orders"] if r["client_order_id"] == "c1"][0]
        assert (row["cancel_command_id"], row["cancel_attempts"]) == (c2.command_id, 2)
        note = [r for r in h.db.tables["order_events"] if r["client_order_id"] == "c1"][-1]
        assert (note["from_status"], note["to_status"], note["reason"]) == ("CANCEL_PENDING", "CANCEL_PENDING", "CANCEL_RETRY")
        assert h.eng.account("ACC3").cancels_today == 1  # a re-send is not a new cancel
        h.advance(10 * S_ - 1)  # backoff: the second TTL is 10 s
        assert not resolves(h.timer())
        h.advance(1)
        assert resolves(h.timer()) == ["c1"]
        h.run(h.eng.on_resolve_result("c1", ok_reply(ack("c1", BrokerOrderStatus.CANCELED))))
        assert status(h, "c1") == OrderStatus.CANCELED and h.eng.snapshot("ACC3").funds.frozen_margin == 0

    def test_resolve_outcomes_of_a_pending_cancel(self) -> None:
        h = book()
        for coid in ("a", "b", "c"):
            working(h, coid, 1)
            h.cmd("order.cancel", CancelRequest(command_id=f"k-{coid}", client_order_id=coid, account_id="ACC3"))
        h.run(h.eng.on_resolve_result("a", err_reply(codes.ROUTE_LOST, data=ack("a", BrokerOrderStatus.EXPIRED))))
        assert status(h, "a") == OrderStatus.CANCELED
        h.run(h.eng.on_resolve_result("b", err_reply("REJECT_GATEWAY_UNAVAILABLE", data=ack("b", BrokerOrderStatus.REJECTED))))
        assert status(h, "b") == OrderStatus.BROKER_REJECTED
        # FILLED at the broker: wait for the trades of td.exec (no cancel re-sent)
        effs = h.run(h.eng.on_resolve_result("c", ok_reply(ack("c", BrokerOrderStatus.FILLED))))
        assert status(h, "c") == OrderStatus.CANCEL_PENDING and not cancels(effs)
        h.fill("c", 4500.0, 1)
        assert status(h, "c") == OrderStatus.FILLED
        assert h.eng.snapshot("ACC3").funds.frozen_margin == 0

    def test_stuck_cancel_escalates_once_and_keeps_retrying(self) -> None:
        h = book()
        working(h)
        h.cmd("order.cancel", CancelRequest(command_id="k1", client_order_id="c1", account_id="ACC3"))
        sent = ["k1"]
        for _ in range(8):
            o = h.eng.orders.get("c1")
            assert o is not None
            h.clock.set(h.eng._cancel_check_ns(o))
            assert resolves(h.timer()) == ["c1"]
            effs = h.run(h.eng.on_resolve_result("c1", ok_reply(ack("c1", BrokerOrderStatus.CANCEL_PENDING))))
            sent += [c.command_id for c in cancels(effs)]
        assert len(sent) == 9 and len(set(sent)) == 9  # every re-send has its own command id
        o = h.eng.orders.get("c1")
        assert o is not None and o.cancel_attempts == 9
        assert h.eng._cancel_ttl(o.cancel_attempts) == 60 * S_ * 1_000_000  # capped backoff
        stuck = [b for b in h.eng.breaks.values() if b.detail.get("reason") == "CANCEL_STUCK"]
        assert len(stuck) == 1 and stuck[0].kind == ReconBreakKind.ORDER_STATE_CONFLICT and not stuck[0].blocking
        assert stuck[0].detail["attempts"] == 5
        ev = [e for e in h.events(RiskEventKind.RECON_BREAK) if e.detail.get("reason") == "CANCEL_STUCK"]
        assert len(ev) == 1 and ev[0].severity == Severity.WARNING

    def test_order_cancel_resends_a_stale_pending_cancel(self) -> None:
        h = book()
        working(h)
        h.cmd("order.cancel", CancelRequest(command_id="k1", client_order_id="c1", account_id="ACC3"))
        rep, effs = h.cmd("order.cancel", CancelRequest(command_id="k2", client_order_id="c1", account_id="ACC3"))
        assert rep.ok and rep.message == "cancel already pending" and not cancels(effs)
        h.advance(5 * S_)
        rep, effs = h.cmd("order.cancel", CancelRequest(command_id="k3", client_order_id="c1", account_id="ACC3"))
        assert rep.ok and rep.message == "cancel re-sent" and [c.command_id for c in cancels(effs)] == ["k3"]
        # an error answer to route.cancel also makes the pending cancel stale
        h.run(h.eng.on_cancel_result("c1", err_reply(codes.BAD_REQUEST)))
        assert status(h, "c1") == OrderStatus.CANCEL_PENDING
        rep, effs = h.cmd("order.cancel", CancelRequest(command_id="k4", client_order_id="c1", account_id="ACC3"))
        assert [c.command_id for c in cancels(effs)] == ["k4"]
        assert h.eng.account("ACC3").cancels_today == 1

    def test_cancel_error_reply_is_checked_at_once(self) -> None:
        h = book()
        working(h)
        h.cmd("order.cancel", CancelRequest(command_id="k1", client_order_id="c1", account_id="ACC3"))
        assert h.run(h.eng.on_cancel_result("c1", err_reply(codes.TIMEOUT))) == []  # transport: the shell retries
        assert not resolves(h.timer())
        h.run(h.eng.on_cancel_result("c1", err_reply(codes.BAD_REQUEST)))
        assert status(h, "c1") == OrderStatus.CANCEL_PENDING  # never reverted by a route.cancel error
        assert resolves(h.timer()) == ["c1"]

    def test_restatement_of_a_finished_order_is_applied(self) -> None:
        h = book()
        working(h, "c1", 2)
        h.cmd("order.cancel", CancelRequest(command_id="k1", client_order_id="c1", account_id="ACC3"))
        h.fill("c1", 4500.0, 2)
        assert status(h, "c1") == OrderStatus.FILLED
        working(h, "c2", 2)
        h.cmd("order.cancel", CancelRequest(command_id="k2", client_order_id="c2", account_id="ACC3"))
        h.broker("c2", BrokerOrderStatus.CANCELED, code=codes.BROKER_ORDER_FINISHED)
        assert status(h, "c2") == OrderStatus.CANCELED

    def test_cancel_reported_by_the_broker_counts_and_is_checked(self) -> None:
        h = book()
        working(h)
        h.broker("c1", BrokerOrderStatus.CANCEL_PENDING, remaining=2)  # cancelled from another terminal
        assert status(h, "c1") == OrderStatus.CANCEL_PENDING
        assert h.eng.account("ACC3").cancels_by_inst == {"IF2612": 1}
        h.advance(5 * S_)
        assert resolves(h.timer()) == ["c1"]

    def test_crash_before_the_cancel_went_out_is_redriven(self) -> None:
        """p02: CANCEL_PENDING committed, CancelRoute never sent; after the restart: resolve, then re-send."""
        a = book()
        working(a)
        halt(a)
        assert status(a, "c1") == OrderStatus.CANCEL_PENDING
        b = Harness(a.rules, load=False)
        b.clock.set(a.clock.now_ns())
        b.eng.restore(a.db.snapshot(), lifecycle=Lifecycle.WARMUP)
        o = b.eng.orders.get("c1")
        assert o is not None and o.cancel_attempts == 1 and o.cancel_command_id
        effs = b.run(b.eng.post_restore())
        assert resolves(effs) == ["c1"] and not cancels(effs)
        effs = b.run(b.eng.on_resolve_result("c1", ok_reply(ack("c1", BrokerOrderStatus.ACCEPTED))))
        (c,) = cancels(effs)  # at once: whether the first cancel went out is unknown
        assert c.command_id != o.cancel_command_id or o.cancel_attempts == 2
        b.run(b.eng.on_resolve_result("c1", ok_reply(ack("c1", BrokerOrderStatus.CANCELED))))
        assert status(b, "c1") == OrderStatus.CANCELED and b.eng.snapshot("ACC3").funds.frozen_margin == 0

    def test_gateway_back_redrives_pending_cancels(self) -> None:
        h = book()
        working(h)
        h.cmd("order.cancel", CancelRequest(command_id="k1", client_order_id="c1", account_id="ACC3"))
        h.run(h.eng.set_gateway_status(False))
        h.advance(30 * S_)
        assert not resolves(h.timer())
        effs = h.run(h.eng.set_gateway_status(True))
        assert resolves(effs) == ["c1"]
        effs = h.run(h.eng.on_resolve_result("c1", ok_reply(ack("c1", BrokerOrderStatus.ACCEPTED))))
        assert len(cancels(effs)) == 1

    def test_resolve_without_an_answer_is_sent_again(self) -> None:
        h = book()
        h.submit("ACC3", "IF2612", B, O, 2, 4500.0, coid="c1")
        h.advance(2001)
        assert resolves(h.timer()) == ["c1"]
        h.advance(29_999)
        assert not resolves(h.timer())
        h.advance(1)
        assert resolves(h.timer()) == ["c1"]  # resolve_inflight timed out


class TestHaltGuard:
    def test_refused_cancel_on_a_halted_account_is_cancelled_again(self) -> None:
        """p01: the revert of a refused cancel while HALTED re-cancels at once, repeated refusals back off."""
        h = book()
        working(h)
        effs = halt(h)
        assert h.eng.account("ACC3").state == AccountState.HALTED and len(cancels(effs)) == 1
        effs = h.broker("c1", BrokerOrderStatus.ACCEPTED, remaining=2, code="BROKER_DISCONNECTED")
        (c2,) = cancels(effs)
        assert c2.reason == "CIRCUIT_BREAK_CANCEL" and status(h, "c1") == OrderStatus.CANCEL_PENDING
        effs = h.broker("c1", BrokerOrderStatus.ACCEPTED, remaining=2, code=codes.BROKER_ORDER_NOT_FOUND)
        assert not cancels(effs) and status(h, "c1") == OrderStatus.ACCEPTED  # refused twice: back off
        o = h.eng.orders.get("c1")
        assert o is not None and o.cancel_due_ns is not None
        assert h.eng.next_timer_ns() is not None and h.eng.next_timer_ns() <= o.cancel_due_ns  # type: ignore[operator]
        h.clock.set(o.cancel_due_ns - 1)
        assert not cancels(h.timer())
        h.clock.set(o.cancel_due_ns)
        (c3,) = cancels(h.timer())
        assert c3.reason == "CIRCUIT_BREAK_CANCEL" and status(h, "c1") == OrderStatus.CANCEL_PENDING

    def test_close_orders_and_normal_accounts_are_not_guarded(self) -> None:
        h = book()
        working(h)
        h.cmd("order.cancel", CancelRequest(command_id="k1", client_order_id="c1", account_id="ACC3"))
        effs = h.broker("c1", BrokerOrderStatus.ACCEPTED, remaining=2, code=codes.BROKER_ORDER_NOT_FOUND)
        assert not cancels(effs) and status(h, "c1") == OrderStatus.ACCEPTED  # NORMAL account: the refusal stands

    def test_rebuilt_open_order_on_a_halted_account_is_cancelled_and_audited(self) -> None:
        h = book()
        halt(h)
        t = h.trade("ACC3", "IF2612", B, O, 4500.0, 1, coid="lost-1")
        effs = h.run(h.eng.apply_exec(t))
        req = [e for e in effs if isinstance(e, Request)][0]
        batch = BrokerOrderBatch(items=[BrokerOrderUpdate(client_order_id="lost-1", account_id="ACC3", instrument_id="IF2612",
                                                           broker_order_ref="r", status=BrokerOrderStatus.PARTIALLY_FILLED,
                                                           filled_qty=1, remaining_qty=2, broker_seq=9, trading_day=TD,
                                                           updated_ns=1)])
        effs = h.run(h.eng.on_request_result(req.token, ok_reply(batch)))
        assert [c.client_order_id for c in cancels(effs)] == ["lost-1"]
        assert status(h, "lost-1") == OrderStatus.CANCEL_PENDING
        audit = [r for r in h.db.tables["audit_events"] if r["target"] == "order:lost-1"]
        assert len(audit) == 1 and audit[0]["action"] == "RECON_RESOLVE" and audit[0]["after"]["kind"] == "ORDER_REBUILD"

    def test_resolved_working_order_on_a_halted_account_is_cancelled(self) -> None:
        h = book()
        h.submit("ACC3", "IF2612", B, O, 2, 4500.0, coid="c1")
        h.advance(2001)
        h.timer()  # ROUTING -> UNKNOWN, resolve in flight
        ra = h.eng.account("ACC3")
        ra.state = AccountState.HALTED  # unit-level: the account is HALTED while the resolve is in flight
        effs = h.run(h.eng.on_resolve_result("c1", ok_reply(ack("c1", BrokerOrderStatus.ACCEPTED))))
        assert [c.reason for c in cancels(effs)] == ["CIRCUIT_BREAK_CANCEL"]


class TestFillBookkeeping:
    def test_frozen_margin_of_the_order_after_partial_fills(self) -> None:
        """L3: the order's frozen margin follows the remaining quantity (no compounding)."""
        h = book()
        working(h, "c1", 4)
        o = h.eng.orders.get("c1")
        assert o is not None
        f0 = o.frozen_margin
        h.fill("c1", 4500.0, 1)
        h.fill("c1", 4500.0, 1)
        assert o.frozen_margin == pytest.approx(f0 * 2 / 4)
        row = [r for r in h.db.tables["orders"] if r["client_order_id"] == "c1"][0]
        assert float(row["frozen_margin"]) == pytest.approx(f0 / 2, abs=0.01)
        assert h.eng.snapshot("ACC3").funds.frozen_margin == pytest.approx(f0 / 2)

    def test_unbookable_trade_redelivered_opens_one_break(self) -> None:
        """L2: a close larger than the book is not applied; re-deliveries reuse the open break (by trade_key)."""
        h = book()
        t = h.trade("ACC3", "IF2612", S, Offset.CLOSE, 4500.0, 5, origin=TradeOrigin.EXTERNAL)
        for _ in range(3):
            h.run(h.eng.apply_exec(t))
        brk = [b for b in h.eng.breaks.values() if b.kind == ReconBreakKind.POSITION_MISMATCH]
        assert len(brk) == 1 and brk[0].detail["trade_key"] == t.trade_key and brk[0].blocking
        assert not h.db.tables.get("fills")


def test_resolve_answer_does_not_resend_a_fresh_cancel() -> None:
    """A resolve sent while the order was UNKNOWN answers after a new cancel went out: no duplicate re-send."""
    h = book()
    h.submit("ACC3", "IF2612", B, O, 2, 4500.0, coid="c1")
    h.advance(2001)
    assert resolves(h.timer()) == ["c1"]  # UNKNOWN, resolve in flight
    h.cmd("order.cancel", CancelRequest(command_id="k1", client_order_id="c1", account_id="ACC3"))
    effs = h.run(h.eng.on_resolve_result("c1", ok_reply(ack("c1", BrokerOrderStatus.ACCEPTED))))
    assert not cancels(effs) and status(h, "c1") == OrderStatus.CANCEL_PENDING
    h.advance(5 * S_)
    assert resolves(h.timer()) == ["c1"]  # the cancel's own TTL check
