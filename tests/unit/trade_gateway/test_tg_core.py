"""trade-gateway core with a fake broker (02 §6.4, §8.3, §9.4; 03 §4.2, §7): route checks, tombstones,
resolve, durable cancels, kill switch OR semantics, report persistence + dedupe, resync, import, queries."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import msgspec
import pytest
from tgtest_support import (
    MS,
    SEC,
    T0,
    TD,
    Harness,
    logged_in,
    make_harness,
    order_report,
    route,
    trade_report,
)

from futrisk.common.clock import SimClock
from futrisk.common.config import FullConfig
from futrisk.messages import codes
from futrisk.messages.enums import (
    BrokerConnState,
    BrokerOrderStatus,
    ControlOrigin,
    Direction,
    Exchange,
    FirmAction,
    Offset,
    PosDirection,
    RestrictionKind,
    Role,
    TradeOrigin,
)
from futrisk.messages.envelope import Reply
from futrisk.messages.orders import (
    BrokerOrderBatch,
    BrokerOrderUpdate,
    BrokerPositionBatch,
    DigestRequest,
    OrderQuery,
    PositionQuery,
    RouteAck,
    RouteCancel,
    RouteResolve,
    Trade,
    TradeBatch,
    TradeDigest,
    TradeImport,
    TradeImportAck,
    TradeQuery,
    compute_account_digest,
)
from futrisk.messages.risk import FirmControl, KillSwitchRequest, Restriction
from futrisk.services.trade_gateway import store
from futrisk.services.trade_gateway.broker.base import (
    InsertError,
    InvalidReport,
    OrderReport,
    PositionReport,
    TradeReport,
)

RE = "risk-engine-1"
API = "api-gateway"
EXEC3 = "td.exec.ACC3"
TD2 = "20260929"
TD3 = "20260930"


def ack(reply: Reply) -> RouteAck:
    a = reply.open_as(RouteAck)
    assert isinstance(a, RouteAck)
    return a


def status_of(h: Harness, coid: str) -> str | None:
    return h.scalar("SELECT status FROM routed_orders WHERE client_order_id=?", coid)


def cancel_flag(h: Harness, coid: str) -> bool:
    return h.scalar("SELECT cancel_requested_ns IS NOT NULL FROM routed_orders WHERE client_order_id=?", coid) == 1


def kill(h: Harness, command_id: str, on: bool, role: Role = Role.RISK_MANAGER) -> Reply:
    return h.core.kill_switch(KillSwitchRequest(command_id=command_id, on=on, reason="test", actor="rm", role=role), API)


def cancel(h: Harness, command_id: str, coid: str) -> Reply:
    return h.core.route_cancel(RouteCancel(command_id=command_id, client_order_id=coid, reason="halt"), RE)


def feed(h: Harness, report: object) -> None:
    """Deliver a live report through the broker callbacks."""
    cb = h.broker.cb
    assert cb is not None
    if isinstance(report, TradeReport):
        cb.on_trade(report)
    elif isinstance(report, OrderReport):
        cb.on_order(report)
    else:
        raise TypeError(report)


def rule(h: Harness, on: bool, ts: int) -> None:
    """A legacy RULE kill switch record (KILL_SWITCH_ON / OFF)."""
    fc = FirmControl(action=FirmAction.KILL_SWITCH_ON if on else FirmAction.KILL_SWITCH_OFF, origin=ControlOrigin.RULE,
                     active=on, trading_day=TD, reason="firm daily loss", ts_ns=ts)
    h.core.apply_firm_control(fc, src="risk-engine-0", save_cursor=None)


FIRM_RESTRICTIONS = [
    Restriction(kind=RestrictionKind.BLOCK_OPEN, scope_key="firm", source="firm.daily_loss", since_ns=T0),
    Restriction(kind=RestrictionKind.ORDER_SIZE_FACTOR, scope_key="firm", value=0.5, source="firm.drawdown", since_ns=T0),
]


def rule_state(h: Harness, active: bool, ts: int, restrictions: list[Restriction] | None = None,
               origin: ControlOrigin = ControlOrigin.RULE) -> None:
    """The coordinator's whole RULE layer as one fact (FirmAction.RULE_STATE)."""
    fc = FirmControl(action=FirmAction.RULE_STATE, origin=origin, active=active, restrictions=list(restrictions or []),
                     trading_day=TD, reason="firm rules", cause_alert_key="firm.daily_loss" if active else None, ts_ns=ts)
    h.core.apply_firm_control(fc, src="risk-engine-0", save_cursor=None)


def exec_msgs(h: Harness, topic: str = EXEC3) -> list[Any]:
    return [m for _, _, m in h.capture.messages(topic)]


def steps(h: Harness) -> list[tuple[str, str, int]]:
    return [(q[0], q[1], q[2]) for q in h.broker.queries]


# ---------------------------------------------------------------------------- route.order
class TestRouteOrder:
    def test_new_route_is_persisted_then_submitted(self, harness: Harness) -> None:
        h = harness
        logged_in(h)
        r = h.core.route_order(route("c1"), RE)
        assert r.ok and not r.duplicate
        a = ack(r)
        assert a.status is BrokerOrderStatus.SUBMITTED and a.broker_order_ref == "1:7:1"
        assert [o.client_order_id for o in h.broker.inserts] == ["c1"]
        row = h.rows("SELECT status, broker_order_ref, broker, src_instance, price, qty, submitted_ns, cancel_requested_ns "
                     "FROM routed_orders")
        assert row == [("SUBMITTED", "1:7:1", "sim", RE, "3120.0", 1, T0, None)]
        assert h.capture.rows == []  # nothing published before the broker reports

    def test_duplicate_route_returns_existing_status_and_never_resubmits(self, harness: Harness) -> None:
        h = harness
        logged_in(h)
        h.core.route_order(route("c1"), RE)
        r2 = h.core.route_order(route("c1"), RE)
        assert r2.ok and r2.duplicate and ack(r2).status is BrokerOrderStatus.SUBMITTED
        assert len(h.broker.inserts) == 1

    def test_expired_route_writes_expired_row_and_is_never_sent(self, harness: Harness) -> None:
        h = harness
        logged_in(h)
        r = h.core.route_order(route("c1", now=T0 - 3 * SEC), RE)  # expire_ns = T0 - 1 s
        assert not r.ok and r.code == codes.ROUTE_EXPIRED and ack(r).status is BrokerOrderStatus.EXPIRED
        assert h.rows("SELECT status, broker_code FROM routed_orders") == [("EXPIRED", "ROUTE_EXPIRED")]
        assert h.broker.inserts == []
        again = h.core.route_order(route("c1"), RE)  # a late retry: the EXPIRED row acts as a tombstone
        assert again.code == codes.ROUTE_EXPIRED and again.duplicate and h.broker.inserts == []

    def test_expire_boundary_is_strict(self, harness: Harness) -> None:
        h = harness
        logged_in(h)
        assert h.core.route_order(route("c1", now=T0 - 2 * SEC), RE).ok  # now == expire_ns: still valid

    def test_tombstone_beats_expiry_and_writes_no_row(self, harness: Harness) -> None:
        h = harness
        logged_in(h)
        h.core.route_resolve(RouteResolve(client_order_id="c1", trading_day=TD), RE)
        r = h.core.route_order(route("c1", now=T0 - 10 * SEC), RE)  # expired AND tombstoned
        assert r.code == codes.ROUTE_LOST and ack(r).status is BrokerOrderStatus.EXPIRED
        assert h.scalar("SELECT count(*) FROM routed_orders") == 0

    def test_existing_row_beats_every_other_check(self, harness: Harness) -> None:
        h = harness
        logged_in(h)
        h.core.route_order(route("c1"), RE)
        assert kill(h, "k1", True).ok
        r = h.core.route_order(route("c1", now=T0 - 10 * SEC), RE)
        assert r.ok and ack(r).status is BrokerOrderStatus.SUBMITTED

    def test_expiry_is_checked_before_the_kill_switch(self, harness: Harness) -> None:
        h = harness
        logged_in(h)
        kill(h, "k1", True)
        r = h.core.route_order(route("c1", now=T0 - 10 * SEC), RE)
        assert r.code == codes.ROUTE_EXPIRED

    def test_kill_switch_blocks_opens_not_closes(self, harness: Harness) -> None:
        h = harness
        logged_in(h)
        kill(h, "k1", True)
        r = h.core.route_order(route("o1"), RE)
        assert r.code == codes.ROUTE_KILL_SWITCH and ack(r).status is BrokerOrderStatus.EXPIRED
        assert h.rows("SELECT status, broker_code FROM routed_orders WHERE client_order_id='o1'") == [("EXPIRED", "ROUTE_KILL_SWITCH")]
        c = h.core.route_order(route("x1", direction=Direction.SELL, offset=Offset.CLOSE_TODAY), RE)
        assert c.ok and ack(c).status is BrokerOrderStatus.SUBMITTED
        assert [o.client_order_id for o in h.broker.inserts] == ["x1"]

    def test_broker_down_rejects_with_gateway_unavailable_and_remembers_it(self, harness: Harness) -> None:
        h = harness
        r = h.core.route_order(route("c1"), RE)  # never logged in
        assert r.code == codes.REJECT_GATEWAY_UNAVAILABLE and ack(r).status is BrokerOrderStatus.REJECTED
        logged_in(h)
        again = h.core.route_order(route("c1"), RE)  # the risk-engine already saw the reject: never sent later
        assert again.code == codes.REJECT_GATEWAY_UNAVAILABLE and again.duplicate and h.broker.inserts == []

    def test_send_failure_after_persist_marks_the_row_rejected(self, harness: Harness) -> None:
        h = harness
        logged_in(h)
        h.broker.fail_insert = True
        r = h.core.route_order(route("c1"), RE)
        assert r.code == codes.REJECT_GATEWAY_UNAVAILABLE
        assert status_of(h, "c1") == "REJECTED"

    @pytest.mark.parametrize("kw", [{"qty": 0}, {"price": None}, {"offset": Offset.FORCE_CLOSE}])
    def test_invalid_routes_are_bad_requests_without_rows(self, harness: Harness, kw: dict[str, object]) -> None:
        h = harness
        logged_in(h)
        r = h.core.route_order(route("c1", **kw), RE)  # type: ignore[arg-type]
        assert r.code == codes.BAD_REQUEST
        assert h.scalar("SELECT count(*) FROM routed_orders") == 0

    def test_routes_are_held_during_resync_and_sent_when_it_completes(self, harness: Harness) -> None:
        h = harness
        h.broker.login_ok()
        assert h.core.resyncing and h.core.published_state() is BrokerConnState.RESYNCING
        r = h.core.route_order(route("c1"), RE)
        assert r.ok and ack(r).status is BrokerOrderStatus.RECEIVED and h.broker.inserts == []
        assert status_of(h, "c1") == "RECEIVED"
        h.broker.answer_resync()
        assert [o.client_order_id for o in h.broker.inserts] == ["c1"]
        assert status_of(h, "c1") == "SUBMITTED" and h.core.ready

    def test_held_order_inside_the_old_grace_window_expires(self, harness: Harness) -> None:
        h = harness
        h.broker.login_ok()
        h.core.route_order(route("c1"), RE)  # expire_ns = T0 + 2 s
        h.advance(3 * SEC)  # past expire_ns (the former 5 s hold grace would still have sent it)
        h.broker.answer_resync()
        assert h.broker.inserts == []
        assert h.rows("SELECT status, broker_code FROM routed_orders") == [("EXPIRED", "ROUTE_EXPIRED")]
        upd = exec_msgs(h)[-1]
        assert isinstance(upd, BrokerOrderUpdate) and upd.status is BrokerOrderStatus.EXPIRED
        assert upd.broker_code == codes.ROUTE_EXPIRED and upd.broker_seq == 0

    def test_held_order_expires_when_the_resync_never_completes(self, harness: Harness) -> None:
        h = harness
        h.broker.login_ok()
        h.core.route_order(route("c1"), RE)
        h.advance(2 * SEC + 1)  # just past expire_ns: no grace
        h.tick()
        assert h.rows("SELECT status, broker_code FROM routed_orders") == [("EXPIRED", "ROUTE_EXPIRED")]
        h.broker.answer_resync()
        assert h.broker.inserts == []

    def test_kill_switch_turned_on_while_held_expires_held_opens(self, harness: Harness) -> None:
        h = harness
        h.broker.login_ok()
        h.core.route_order(route("o1"), RE)
        h.core.route_order(route("x1", direction=Direction.SELL, offset=Offset.CLOSE_TODAY), RE)
        kill(h, "k1", True)
        h.broker.answer_resync()
        assert status_of(h, "o1") == "EXPIRED" and status_of(h, "x1") == "SUBMITTED"
        assert [o.client_order_id for o in h.broker.inserts] == ["x1"]

    def test_rule_kill_switch_arriving_while_orders_are_held(self, harness: Harness) -> None:
        h = harness
        h.broker.login_ok()
        h.core.route_order(route("o1"), RE)
        h.core.route_order(route("x1", direction=Direction.SELL, offset=Offset.CLOSE_TODAY), RE)
        rule(h, True, T0 + 1)
        h.broker.answer_resync()
        assert h.rows("SELECT status, broker_code FROM routed_orders WHERE client_order_id='o1'") == [
            ("EXPIRED", "ROUTE_KILL_SWITCH")]
        upd = exec_msgs(h)[0]
        assert isinstance(upd, BrokerOrderUpdate) and upd.client_order_id == "o1" and upd.broker_code == "ROUTE_KILL_SWITCH"
        assert [o.client_order_id for o in h.broker.inserts] == ["x1"]


# ---------------------------------------------------------------------------- resolve
class TestResolve:
    def test_resolve_never_received_writes_tombstone_then_late_route_is_lost(self, harness: Harness) -> None:
        h = harness
        logged_in(h)
        r = h.core.route_resolve(RouteResolve(client_order_id="c1", trading_day=TD), RE)
        assert r.code == codes.ROUTE_LOST and ack(r).status is BrokerOrderStatus.EXPIRED
        assert h.rows("SELECT trading_day, command, command_id, requested_by FROM route_tombstones") == [
            (TD, "route.resolve", None, RE)
        ]
        late = h.core.route_order(route("c1"), RE)
        assert late.code == codes.ROUTE_LOST and h.broker.inserts == []
        assert h.scalar("SELECT count(*) FROM routed_orders") == 0
        again = h.core.route_resolve(RouteResolve(client_order_id="c1", trading_day=TD), RE)
        assert again.code == codes.ROUTE_LOST and h.scalar("SELECT count(*) FROM route_tombstones") == 1

    def test_resolve_of_a_received_order_returns_its_status_without_side_effect(self, harness: Harness) -> None:
        h = harness
        logged_in(h)
        h.core.route_order(route("c1"), RE)
        r = h.core.route_resolve(RouteResolve(client_order_id="c1", trading_day=TD), RE)
        assert r.ok and ack(r).status is BrokerOrderStatus.SUBMITTED and ack(r).broker_order_ref == "1:7:1"
        assert h.scalar("SELECT count(*) FROM route_tombstones") == 0

    def test_resolve_of_an_expired_route_returns_the_route_code(self, harness: Harness) -> None:
        h = harness
        logged_in(h)
        h.core.route_order(route("c1", now=T0 - 5 * SEC), RE)
        r = h.core.route_resolve(RouteResolve(client_order_id="c1", trading_day=TD), RE)
        assert r.code == codes.ROUTE_EXPIRED and ack(r).status is BrokerOrderStatus.EXPIRED

    def test_query_orders_is_read_only(self, harness: Harness) -> None:
        h = harness
        logged_in(h)
        r = h.core.query_orders(OrderQuery(trading_day=TD, client_order_ids=["ghost"]))
        assert r.ok and r.open_as(BrokerOrderBatch).items == []
        assert h.scalar("SELECT count(*) FROM route_tombstones") == 0
        assert h.core.route_order(route("ghost"), RE).ok  # still routable


# ---------------------------------------------------------------------------- route.cancel (durable)
class TestCancel:
    def test_cancel_of_a_never_received_order_tombstones_and_is_idempotent(self, harness: Harness) -> None:
        h = harness
        logged_in(h)
        r = cancel(h, "x1", "c1")
        assert r.code == codes.ROUTE_LOST and ack(r).status is BrokerOrderStatus.EXPIRED
        assert h.rows("SELECT command, command_id FROM route_tombstones") == [("route.cancel", "x1")]
        dup = cancel(h, "x1", "c1")
        assert dup.duplicate and dup.code == codes.ROUTE_LOST
        conflict = h.core.route_cancel(RouteCancel(command_id="x1", client_order_id="c2"), RE)
        assert conflict.code == codes.CONFLICT
        assert h.core.route_order(route("c1"), RE).code == codes.ROUTE_LOST

    def test_cancel_of_a_held_order_cancels_locally_and_publishes(self, harness: Harness) -> None:
        h = harness
        h.broker.login_ok()  # resync pending: new routes are held
        h.core.route_order(route("c1"), RE)
        r = cancel(h, "x1", "c1")
        assert r.ok and ack(r).status is BrokerOrderStatus.CANCELED
        assert status_of(h, "c1") == "CANCELED"
        msgs = exec_msgs(h)
        assert len(msgs) == 1
        upd = msgs[0]
        assert isinstance(upd, BrokerOrderUpdate) and upd.status is BrokerOrderStatus.CANCELED
        assert upd.broker_seq == 0 and upd.client_order_id == "c1" and upd.remaining_qty == 1
        h.broker.answer_resync()
        assert h.broker.inserts == []  # never sent

    def test_cancel_of_an_accepted_order_goes_to_the_broker(self, harness: Harness) -> None:
        h = harness
        logged_in(h)
        h.core.route_order(route("c1"), RE)
        feed(h, order_report(1, "c1", BrokerOrderStatus.ACCEPTED))
        r = cancel(h, "x1", "c1")
        assert r.ok and ack(r).status is BrokerOrderStatus.CANCEL_PENDING
        assert h.broker.cancels == [("c1", "1:7:1", "0000000001")]
        assert status_of(h, "c1") == "CANCEL_PENDING" and cancel_flag(h, "c1")
        assert h.scalar("SELECT result_code FROM processed_commands WHERE command_id='x1'") == "OK"
        dup = cancel(h, "x1", "c1")
        assert dup.duplicate and len(h.broker.cancels) == 1
        feed(h, order_report(2, "c1", BrokerOrderStatus.CANCELED, status_msg="USER_CANCELED"))
        assert status_of(h, "c1") == "CANCELED"
        last = exec_msgs(h)[-1]
        assert isinstance(last, BrokerOrderUpdate) and last.status is BrokerOrderStatus.CANCELED
        assert last.broker_code == "USER_CANCELED" and last.broker_seq == 2

    def test_cancel_while_the_broker_is_down_is_queued_and_sent_after_the_resync(self, harness: Harness) -> None:
        h = harness
        logged_in(h)
        h.core.route_order(route("c1"), RE)
        feed(h, order_report(1, "c1", BrokerOrderStatus.ACCEPTED))
        h.broker.drop()
        r = cancel(h, "x1", "c1")
        assert r.ok and ack(r).status is BrokerOrderStatus.CANCEL_PENDING and h.broker.cancels == []
        assert status_of(h, "c1") == "CANCEL_PENDING" and cancel_flag(h, "c1")
        assert cancel(h, "x1", "c1").duplicate  # recorded at once: the request is durable
        h.broker.login_ok(session_id=8)
        assert h.broker.cancels == []  # not during the resync
        h.broker.answer_resync()
        assert h.broker.cancels == [("c1", "1:7:1", "0000000001")]
        feed(h, order_report(2, "c1", BrokerOrderStatus.CANCELED, status_msg="USER_CANCELED"))
        assert status_of(h, "c1") == "CANCELED"

    def test_cancel_refused_by_a_lost_session_stays_pending_and_is_resent(self, harness: Harness) -> None:
        h = harness
        logged_in(h)
        h.core.route_order(route("c1"), RE)
        feed(h, order_report(1, "c1", BrokerOrderStatus.ACCEPTED))
        cancel(h, "x1", "c1")
        h.broker.refuse_cancel("c1", "BROKER_NOT_LOGGED_IN")
        assert status_of(h, "c1") == "CANCEL_PENDING" and cancel_flag(h, "c1")
        assert len(exec_msgs(h)) == 1 and h.core.stats.cancels_deferred == 1  # no "revert" update
        h.broker.drop()
        h.broker.login_ok(session_id=8)
        h.broker.answer_resync()
        assert len(h.broker.cancels) == 2  # re-sent after the re-login

    def test_cancel_in_flight_when_the_link_drops_is_resent(self, harness: Harness) -> None:
        h = harness
        logged_in(h)
        h.core.route_order(route("c1"), RE)
        feed(h, order_report(1, "c1", BrokerOrderStatus.ACCEPTED))
        cancel(h, "x1", "c1")
        h.broker.drop()  # no answer to the cancel
        assert status_of(h, "c1") == "CANCEL_PENDING"  # the broker knows the order: not UNKNOWN
        h.broker.login_ok(session_id=8)
        h.broker.answer_resync()
        assert len(h.broker.cancels) == 2

    def test_a_genuine_refusal_ends_the_request_and_restates_the_order(self, harness: Harness) -> None:
        h = harness
        logged_in(h)
        h.core.route_order(route("c1"), RE)
        feed(h, order_report(1, "c1", BrokerOrderStatus.ACCEPTED))
        cancel(h, "x1", "c1")
        h.broker.refuse_cancel("c1", "BROKER_ORDER_FINISHED")
        assert status_of(h, "c1") == "ACCEPTED" and not cancel_flag(h, "c1")
        last = exec_msgs(h)[-1]
        assert isinstance(last, BrokerOrderUpdate) and last.status is BrokerOrderStatus.ACCEPTED
        assert last.broker_code == "BROKER_ORDER_FINISHED" and last.broker_seq == 0  # gateway-originated
        h.broker.drop()
        h.broker.login_ok(session_id=8)
        h.broker.answer_resync()
        assert len(h.broker.cancels) == 1  # never re-sent

    def test_not_found_before_the_first_report_waits_for_the_order(self, harness: Harness) -> None:
        h = harness
        logged_in(h)
        h.core.route_order(route("c1"), RE)  # SUBMITTED, no broker report yet
        cancel(h, "x1", "c1")
        assert h.broker.cancels == [("c1", "1:7:1", None)]
        h.broker.refuse_cancel("c1", "BROKER_ORDER_NOT_FOUND")
        assert status_of(h, "c1") == "SUBMITTED" and cancel_flag(h, "c1") and exec_msgs(h) == []
        feed(h, order_report(1, "c1", BrokerOrderStatus.ACCEPTED))  # the insert finally reached the broker
        assert len(h.broker.cancels) == 2 and status_of(h, "c1") == "CANCEL_PENDING"

    def test_an_unexpected_refusal_keeps_the_request_pending(self, harness: Harness) -> None:
        h = harness
        logged_in(h)
        h.core.route_order(route("c1"), RE)
        feed(h, order_report(1, "c1", BrokerOrderStatus.ACCEPTED))
        cancel(h, "x1", "c1")
        h.broker.refuse_cancel("c1", "BROKER_BAD_REQUEST")
        assert status_of(h, "c1") == "CANCEL_PENDING" and cancel_flag(h, "c1") and len(exec_msgs(h)) == 1

    def test_cancel_of_an_unknown_order_without_reference_is_queued(
        self, tmp_path: Path, default_config: FullConfig
    ) -> None:
        clock = SimClock(T0)
        h1 = make_harness(tmp_path / "td.db", default_config, clock=clock)
        h1.db.write(lambda c: c.exec_driver_sql(
            "INSERT INTO routed_orders (client_order_id, account_id, instrument_id, exchange, direction, offset, order_type,"
            " tif, price, qty, trading_day, src_instance, expire_ns, broker, status, filled_qty, received_ns, updated_ns)"
            " VALUES ('c1','ACC3','rb2701','SHFE','BUY','OPEN','LIMIT','GFD','3120',1,?,'risk-engine-1',?,'sim',"
            " 'RECEIVED', 0, ?, ?)", (TD, T0 + SEC, T0, T0)))
        h1.db.close()  # crash right after persisting: no broker reference was ever assigned
        h = make_harness(tmp_path / "td.db", default_config, clock=clock)
        try:
            assert status_of(h, "c1") == "UNKNOWN"
            r = cancel(h, "x1", "c1")  # broker down and no reference: queued, not CONFLICT / UNAVAILABLE
            assert r.ok and ack(r).status is BrokerOrderStatus.CANCEL_PENDING and h.broker.cancels == []
            h.broker.login_ok()
            h.broker.answer_resync()  # the broker never saw it
            assert h.broker.cancels == []
            assert h.rows("SELECT status, broker_code FROM routed_orders") == [("CANCELED", "BROKER_ORDER_NOT_FOUND")]
            upd = exec_msgs(h)[-1]
            assert isinstance(upd, BrokerOrderUpdate) and upd.status is BrokerOrderStatus.CANCELED and upd.broker_seq == 0
        finally:
            h.db.close()

    def test_cancel_with_an_unusable_reference_is_queued(self, harness: Harness) -> None:
        h = harness
        logged_in(h)
        h.core.route_order(route("c1"), RE)
        h.db.write(lambda c: c.exec_driver_sql(
            "UPDATE routed_orders SET status='ACCEPTED', broker_order_ref='bogus', order_sys_id=NULL"))
        r = cancel(h, "x1", "c1")
        assert r.ok and h.broker.cancels == [] and cancel_flag(h, "c1")  # FakeBroker would refuse the reference

    def test_cancel_of_a_finished_order_returns_its_status(self, harness: Harness) -> None:
        h = harness
        logged_in(h)
        h.core.route_order(route("c1"), RE)
        feed(h, trade_report(1, "c1", "T1"))
        feed(h, order_report(2, "c1", BrokerOrderStatus.FILLED, filled=1))
        r = cancel(h, "x1", "c1")
        assert r.ok and ack(r).status is BrokerOrderStatus.FILLED and h.broker.cancels == []

    def test_insert_refused_by_a_lost_session_is_closed_after_the_resync(self, harness: Harness) -> None:
        h = harness
        logged_in(h)
        h.core.route_order(route("c1", ttl_ns=60 * SEC), RE)
        h.broker.cb.on_insert_error(InsertError(client_order_id="c1", order_ref="1", order_sys_id=None,  # type: ignore[union-attr]
                                                error_id="BROKER_NOT_LOGGED_IN", error_msg="login first"))
        assert status_of(h, "c1") == "SUBMITTED" and exec_msgs(h) == []
        h.broker.drop()
        assert status_of(h, "c1") == "UNKNOWN"
        h.broker.login_ok(session_id=8)
        h.broker.answer_resync()
        assert h.rows("SELECT status, broker_code FROM routed_orders") == [("REJECTED", codes.BROKER_DISCONNECTED)]
        assert h.broker.inserts[0].client_order_id == "c1" and len(h.broker.inserts) == 1  # never re-sent

    def test_crash_after_the_insert_before_the_submitted_write_ends_accepted(
        self, tmp_path: Path, default_config: FullConfig, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        clock = SimClock(T0)
        h1 = make_harness(tmp_path / "td.db", default_config, clock=clock)
        logged_in(h1)
        real = store.update_routed

        def crash_on_submitted(conn: Any, coid: str, **values: Any) -> None:
            if values.get("status") is BrokerOrderStatus.SUBMITTED:
                raise RuntimeError("process died")
            real(conn, coid, **values)

        monkeypatch.setattr(store, "update_routed", crash_on_submitted)
        with pytest.raises(RuntimeError):
            h1.core.route_order(route("c1", ttl_ns=60 * SEC), RE)
        monkeypatch.undo()
        assert [o.client_order_id for o in h1.broker.inserts] == ["c1"]  # the broker has it
        h1.db.close()
        h = make_harness(tmp_path / "td.db", default_config, clock=clock)
        try:
            assert status_of(h, "c1") == "UNKNOWN"
            h.broker.login_ok(session_id=8)
            h.broker.answer_resync(orders=[order_report(1, "c1", BrokerOrderStatus.ACCEPTED)])
            assert status_of(h, "c1") == "ACCEPTED"
            assert [m.status for m in exec_msgs(h)] == [BrokerOrderStatus.ACCEPTED]  # no REJECTED update
        finally:
            h.db.close()


# ---------------------------------------------------------------------------- kill switch
class TestKillSwitch:
    def test_effective_state_is_manual_or_rule(self, harness: Harness) -> None:
        h = harness
        logged_in(h)
        r = kill(h, "k1", True)
        fc = r.open_as(FirmControl)
        assert fc.action is FirmAction.KILL_SWITCH_ON and fc.origin is ControlOrigin.MANUAL and fc.active
        rule(h, True, T0 + 1)
        assert h.core.kill.detail() == {"manual": True, "rule": True, "effective": True}
        kill(h, "k2", False)  # manual off: the rule flag still holds
        assert h.core.kill.effective and h.core.route_order(route("o1"), RE).code == codes.ROUTE_KILL_SWITCH
        rule(h, False, T0 + 2)
        assert not h.core.kill.effective and h.core.route_order(route("o2"), RE).ok
        kill(h, "k3", True)
        rule(h, False, T0 + 3)  # a rule OFF never clears the manual flag
        assert h.core.kill.effective

    def test_state_is_loaded_at_start(self, tmp_path: Path, default_config: FullConfig) -> None:
        clock = SimClock(T0)
        h1 = make_harness(tmp_path / "td.db", default_config, clock=clock)
        kill(h1, "k1", True)
        rule(h1, True, T0 + 1)
        kill(h1, "k2", False)
        h1.db.close()
        h2 = make_harness(tmp_path / "td.db", default_config, clock=clock)
        try:
            assert h2.core.kill.detail() == {"manual": False, "rule": True, "effective": True}
            assert h2.core.kill.loaded
        finally:
            h2.db.close()

    def test_manual_command_is_idempotent_and_role_checked(self, harness: Harness) -> None:
        h = harness
        assert kill(h, "k1", True, Role.VIEWER).code == codes.FORBIDDEN
        assert not h.core.kill.manual
        assert kill(h, "k1", True).ok  # same key, now valid: the refusal was not recorded
        dup = kill(h, "k1", True)
        assert dup.duplicate and dup.ok
        assert kill(h, "k1", False).code == codes.CONFLICT
        assert h.rows("SELECT action, origin, active, src_instance FROM firm_controls") == [
            ("KILL_SWITCH_ON", "MANUAL", 1, API)
        ]

    def test_rule_controls_are_idempotent_and_never_go_back_in_time(self, harness: Harness) -> None:
        h = harness
        rule(h, True, T0 + 10)
        rule(h, True, T0 + 10)  # replayed (e.g. FACT stream restarted from seq 1)
        rule(h, False, T0 + 5)  # older than the stored ON
        assert h.core.kill.rule
        assert h.scalar("SELECT count(*) FROM firm_controls") == 1
        restrictions = FirmControl(action=FirmAction.SET_RESTRICTIONS, origin=ControlOrigin.RULE, trading_day=TD,
                                   reason="firm", ts_ns=T0 + 11)
        h.core.apply_firm_control(restrictions, src="risk-engine-0", save_cursor=None)
        assert h.core.kill.rule and h.scalar("SELECT active FROM firm_controls WHERE action='SET_RESTRICTIONS'") == 0

    def test_rule_state_sets_the_rule_flag_and_keeps_the_restrictions(self, harness: Harness) -> None:
        h = harness
        logged_in(h)
        rule_state(h, True, T0 + 1, FIRM_RESTRICTIONS)
        assert h.core.kill.detail() == {"manual": False, "rule": True, "effective": True}
        assert h.core.route_order(route("o1"), RE).code == codes.ROUTE_KILL_SWITCH
        assert h.core.route_order(route("x1", direction=Direction.SELL, offset=Offset.CLOSE_TODAY), RE).ok
        rule_state(h, False, T0 + 2, FIRM_RESTRICTIONS[1:])  # kill switch off, one restriction still in force
        assert h.core.kill.detail() == {"manual": False, "rule": False, "effective": False}
        assert h.core.route_order(route("o2"), RE).ok
        rows = h.rows("SELECT action, origin, active, restrictions, cause_alert_key, src_instance FROM firm_controls "
                      "ORDER BY control_id")
        assert [r[:3] for r in rows] == [("RULE_STATE", "RULE", 1), ("RULE_STATE", "RULE", 0)]
        assert json.loads(rows[0][3]) == msgspec.to_builtins(FIRM_RESTRICTIONS)  # the audit copy of the rule layer
        assert json.loads(rows[1][3]) == msgspec.to_builtins(FIRM_RESTRICTIONS[1:])
        assert rows[0][4:] == ("firm.daily_loss", "risk-engine-0") and rows[1][4] is None

    def test_rule_state_is_loaded_at_start(self, tmp_path: Path, default_config: FullConfig) -> None:
        clock = SimClock(T0)
        h1 = make_harness(tmp_path / "td.db", default_config, clock=clock)
        rule(h1, False, T0 + 1)  # a legacy record first
        rule_state(h1, True, T0 + 2, FIRM_RESTRICTIONS)
        h1.db.close()
        h2 = make_harness(tmp_path / "td.db", default_config, clock=clock)
        try:
            assert h2.core.kill.detail() == {"manual": False, "rule": True, "effective": True}
        finally:
            h2.db.close()

    def test_rule_state_and_legacy_records_are_one_rule_layer(self, harness: Harness) -> None:
        h = harness
        rule(h, True, T0 + 1)  # legacy ON
        rule_state(h, False, T0 + 2)  # the new protocol turns it off
        assert not h.core.kill.rule
        rule(h, True, T0 + 1)  # the legacy ON replayed: duplicate
        rule_state(h, True, T0 + 1, FIRM_RESTRICTIONS)  # older than the newest state: stale
        rule(h, True, T0 + 1_000 - 1)  # a legacy record newer than RULE_STATE still applies (mixed producers)
        assert h.core.kill.rule
        rule_state(h, False, T0 + 1_000)
        rule_state(h, False, T0 + 1_000)  # replayed RULE_STATE: duplicate
        assert not h.core.kill.rule
        assert h.rows("SELECT action, active FROM firm_controls ORDER BY control_id") == [
            ("KILL_SWITCH_ON", 1), ("RULE_STATE", 0), ("KILL_SWITCH_ON", 1), ("RULE_STATE", 0)]

    def test_manual_or_rule_state(self, harness: Harness) -> None:
        h = harness
        logged_in(h)
        kill(h, "k1", True)
        rule_state(h, False, T0 + 1)  # the rule layer is off: the manual flag still holds
        assert h.core.kill.effective and h.core.route_order(route("o1"), RE).code == codes.ROUTE_KILL_SWITCH
        kill(h, "k2", False)
        assert not h.core.kill.effective and h.core.route_order(route("o2"), RE).ok
        rule_state(h, True, T0 + 2, FIRM_RESTRICTIONS)
        kill(h, "k3", True)
        kill(h, "k4", False)  # manual ON / OFF never clears the rule flag
        assert h.core.kill.detail() == {"manual": False, "rule": True, "effective": True}
        assert h.core.route_order(route("o3"), RE).code == codes.ROUTE_KILL_SWITCH
        rule_state(h, False, T0 + 3)
        assert not h.core.kill.effective and h.core.route_order(route("o4"), RE).ok

    def test_a_manual_origin_rule_state_is_ignored(self, harness: Harness) -> None:
        h = harness
        rule_state(h, True, T0 + 1, origin=ControlOrigin.MANUAL)  # manual flags arrive by firm.kill_switch only
        assert not h.core.kill.effective and h.scalar("SELECT count(*) FROM firm_controls") == 0


# ---------------------------------------------------------------------------- execution reports
class TestReports:
    def test_fill_is_persisted_and_published_in_broker_order(self, harness: Harness) -> None:
        h = harness
        logged_in(h)
        h.core.route_order(route("c1", qty=2), RE)
        feed(h, order_report(1, "c1", BrokerOrderStatus.ACCEPTED, qty=2))
        feed(h, trade_report(2, "c1", "T1", qty=2))
        feed(h, order_report(3, "c1", BrokerOrderStatus.FILLED, qty=2, filled=2))
        msgs = h.capture.messages(EXEC3)
        assert [m[1] for m in msgs] == [1, 2, 3]
        assert h.capture.types(EXEC3) == ["BrokerOrderUpdate", "Trade", "BrokerOrderUpdate"]
        trade = msgs[1][2]
        assert isinstance(trade, Trade)
        assert (trade.trade_seq, trade.origin, trade.client_order_id, trade.offset, trade.price, trade.broker_seq) == (
            1, TradeOrigin.INTERNAL, "c1", Offset.OPEN, 3119.0, 2)
        assert trade.broker_order_ref == "1:7:1" and trade.received_ns == T0
        assert h.rows("SELECT status, filled_qty, order_sys_id FROM routed_orders") == [("FILLED", 2, "0000000001")]
        assert h.rows("SELECT trading_day, broker_seq, status FROM broker_order_events ORDER BY broker_seq") == [
            (TD, 1, "ACCEPTED"), (TD, 3, "FILLED")]
        assert h.rows("SELECT broker, trading_day, last_broker_seq, session_id, front_id FROM broker_cursors") == [
            ("sim", TD, 3, "7", 1)]
        assert h.rows("SELECT price, trade_seq FROM trades") == [("3119.0", 1)]

    def test_duplicate_reports_are_ignored(self, harness: Harness) -> None:
        h = harness
        logged_in(h)
        h.core.route_order(route("c1"), RE)
        reports: list[OrderReport | TradeReport] = [
            order_report(1, "c1", BrokerOrderStatus.ACCEPTED), trade_report(2, "c1", "T1"),
            order_report(3, "c1", BrokerOrderStatus.FILLED, filled=1)]
        for r in reports:
            feed(h, r)
        for r in reports:  # replayed live (e.g. duplicated frames)
            feed(h, r)
        h.core._apply_reports(list(reports))  # and forced through the store (resync-style)
        assert len(h.capture.messages(EXEC3)) == 3
        assert h.scalar("SELECT count(*) FROM trades") == 1 and h.scalar("SELECT count(*) FROM broker_order_events") == 2
        assert h.core.stats.dup_trades == 1 and h.core.stats.dup_orders == 2

    def test_an_older_order_state_never_overrides_a_newer_one(self, harness: Harness) -> None:
        h = harness
        logged_in(h)
        h.core.route_order(route("c1"), RE)
        h.core._apply_reports([order_report(5, "c1", BrokerOrderStatus.CANCELED, status_msg="USER_CANCELED")])
        h.core._apply_reports([order_report(4, "c1", BrokerOrderStatus.ACCEPTED)])  # older, different seq
        assert status_of(h, "c1") == "CANCELED" and h.core.stats.stale_orders == 1
        assert h.capture.types(EXEC3) == ["BrokerOrderUpdate"]

    def test_broker_force_close_is_an_external_trade(self, harness: Harness) -> None:
        h = harness
        logged_in(h)
        fc = dict(session_id=0, order_ref="FC9", sys_id="0000000009", direction=Direction.SELL, offset=Offset.FORCE_CLOSE)
        feed(h, order_report(1, None, BrokerOrderStatus.ACCEPTED, status_msg="FORCE_CLOSE", **fc))  # type: ignore[arg-type]
        feed(h, trade_report(2, None, "T9", **fc))  # type: ignore[arg-type]
        feed(h, order_report(3, None, BrokerOrderStatus.FILLED, filled=1, **fc))  # type: ignore[arg-type]
        msgs = exec_msgs(h)
        assert [type(m).__name__ for m in msgs] == ["BrokerOrderUpdate", "Trade", "BrokerOrderUpdate"]
        t = msgs[1]
        assert isinstance(t, Trade) and t.origin is TradeOrigin.EXTERNAL and t.client_order_id is None
        assert t.offset is Offset.FORCE_CLOSE and t.broker_order_ref == "1:0:FC9"
        assert all(isinstance(m, BrokerOrderUpdate) and m.origin is TradeOrigin.EXTERNAL for m in (msgs[0], msgs[2]))
        assert h.rows("SELECT origin, client_order_id, offset FROM trades") == [("EXTERNAL", None, "FORCE_CLOSE")]
        assert h.scalar("SELECT count(*) FROM routed_orders") == 0
        ext = h.core.query_orders(OrderQuery(trading_day=TD)).open_as(BrokerOrderBatch).items
        assert len(ext) == 1 and ext[0].status is BrokerOrderStatus.FILLED and ext[0].account_id == "ACC3"

    def test_reports_of_orders_not_routed_here_reconstruct_the_order(self, harness: Harness) -> None:
        h = harness
        logged_in(h)
        h.core._apply_reports([trade_report(1, "foreign", "T1"),
                               order_report(2, "foreign", BrokerOrderStatus.FILLED, filled=1)])
        assert h.rows("SELECT status, filled_qty, src_instance FROM routed_orders") == [("FILLED", 1, "unknown")]
        assert h.core.stats.reconstructed_orders == 1
        t = exec_msgs(h)[0]
        assert isinstance(t, Trade) and t.origin is TradeOrigin.INTERNAL and t.client_order_id == "foreign"

    def test_invalid_report_consumes_its_seq_but_writes_nothing(self, harness: Harness) -> None:
        h = harness
        logged_in(h)
        h.broker.cb.on_invalid(InvalidReport(kind="order", reason="unknown direction '9'", broker_seq=1,  # type: ignore[union-attr]
                                             trading_day=TD))
        assert h.core.cursor(TD) == 1 and h.scalar("SELECT count(*) FROM broker_order_events") == 0
        assert h.core.stats.invalid_reports == 1 and not h.core.resyncing

    def test_insert_error_without_a_broker_order_rejects_locally(self, harness: Harness) -> None:
        h = harness
        logged_in(h)
        h.core.route_order(route("c1"), RE)
        h.broker.cb.on_insert_error(InsertError(client_order_id="c1", order_ref="1", order_sys_id=None,  # type: ignore[union-attr]
                                                error_id="BROKER_BAD_REQUEST", error_msg="bad"))
        assert h.rows("SELECT status, broker_code FROM routed_orders") == [("REJECTED", "BROKER_BAD_REQUEST")]
        upd = exec_msgs(h)[0]
        assert isinstance(upd, BrokerOrderUpdate) and upd.status is BrokerOrderStatus.REJECTED and upd.broker_seq == 0

    def test_insert_error_with_a_broker_order_only_annotates(self, harness: Harness) -> None:
        h = harness
        logged_in(h)
        h.core.route_order(route("c1"), RE)
        feed(h, order_report(1, "c1", BrokerOrderStatus.REJECTED, status_msg="BROKER_REJECT_CLOSE_TODAY"))
        h.broker.cb.on_insert_error(InsertError(client_order_id="c1", order_ref="1", order_sys_id="0000000001",  # type: ignore[union-attr]
                                                error_id="BROKER_REJECT_CLOSE_TODAY", error_msg="close-today 1 > available 0"))
        assert h.rows("SELECT status, broker_code, broker_msg FROM routed_orders") == [
            ("REJECTED", "BROKER_REJECT_CLOSE_TODAY", "close-today 1 > available 0")]
        assert h.capture.types(EXEC3) == ["BrokerOrderUpdate"]
        resolved = h.core.route_resolve(RouteResolve(client_order_id="c1", trading_day=TD), RE)
        assert resolved.code == "BROKER_REJECT_CLOSE_TODAY" and ack(resolved).status is BrokerOrderStatus.REJECTED


# ---------------------------------------------------------------------------- resync
class TestResync:
    def test_login_resyncs_from_the_cursor_orders_first(self, harness: Harness) -> None:
        h = harness
        logged_in(h)
        h.core._apply_reports([trade_report(1, None, "T1"), trade_report(2, None, "T2")])
        h.broker.drop()
        h.broker.login_ok(session_id=8, head=5)
        assert steps(h) == [("orders", TD, 3), ("trades", TD, 3)]
        h.broker.answer_resync(orders=[order_report(4, None, BrokerOrderStatus.FILLED, filled=1, order_ref="FC1")],
                               trades=[trade_report(3, None, "T3"), trade_report(5, None, "T5")])
        assert h.core.cursor(TD) == 5 and h.core.ready
        assert [m.trade_seq for m in exec_msgs(h) if isinstance(m, Trade)] == [1, 2, 3, 4]

    def test_failed_resync_query_is_retried(self, harness: Harness) -> None:
        h = harness
        h.broker.login_ok()
        h.broker.answer("orders", [])
        h.broker.answer("trades", error="TIMEOUT")
        assert h.core.resyncing and h.broker.queries == []
        h.advance(2 * SEC)
        h.tick()
        assert len(h.broker.queries) == 2
        h.broker.answer_resync()
        assert h.core.ready

    def test_live_seq_gap_triggers_a_resync_and_a_merged_apply(self, harness: Harness) -> None:
        h = harness
        logged_in(h)
        h.core.route_order(route("c1"), RE)
        feed(h, order_report(1, "c1", BrokerOrderStatus.ACCEPTED))
        feed(h, order_report(3, "c1", BrokerOrderStatus.FILLED, filled=1))  # seq 2 (the trade) went missing
        assert h.core.resyncing and h.core.stats.gap_resyncs == 1
        assert steps(h) == [("orders", TD, 2), ("trades", TD, 2)]
        assert h.core.healthy and not h.core.ready  # a short resync keeps the service RUNNING
        feed(h, order_report(4, "c2x", BrokerOrderStatus.REJECTED, order_ref="2", sys_id="2", status_msg="BROKER_REJECT_PRICE"))
        h.broker.answer_resync(orders=[order_report(3, "c1", BrokerOrderStatus.FILLED, filled=1)],
                               trades=[trade_report(2, "c1", "T1")])
        assert not h.core.resyncing
        assert h.capture.types(EXEC3) == ["BrokerOrderUpdate", "Trade", "BrokerOrderUpdate", "BrokerOrderUpdate"]
        assert [m.broker_seq for m in exec_msgs(h)] == [1, 2, 3, 4]
        assert h.core.cursor(TD) == 4

    def test_the_same_seq_buffered_and_queried_is_applied_once(self, harness: Harness) -> None:
        h = harness
        logged_in(h)
        h.core.route_order(route("c1", qty=2), RE)
        feed(h, order_report(1, "c1", BrokerOrderStatus.ACCEPTED, qty=2))
        feed(h, trade_report(3, "c1", "T3"))  # seq 2 lost: resync, T3 buffered
        feed(h, order_report(4, "c1", BrokerOrderStatus.FILLED, qty=2, filled=2))  # buffered as well
        h.broker.answer_resync(orders=[order_report(4, "c1", BrokerOrderStatus.FILLED, qty=2, filled=2)],
                               trades=[trade_report(2, "c1", "T2"), trade_report(3, "c1", "T3")])
        assert [(type(m).__name__, m.broker_seq) for m in exec_msgs(h)] == [
            ("BrokerOrderUpdate", 1), ("Trade", 2), ("Trade", 3), ("BrokerOrderUpdate", 4)]
        assert h.scalar("SELECT count(*) FROM trades") == 2 and h.core.cursor(TD) == 4

    def test_a_frame_lost_between_the_two_snapshots_is_queried_again(self, harness: Harness) -> None:
        h = harness
        logged_in(h)
        h.core.route_order(route("c1"), RE)
        h.core.route_order(route("c2"), RE)
        feed(h, order_report(1, "c1", BrokerOrderStatus.ACCEPTED))
        feed(h, order_report(3, "c1", BrokerOrderStatus.FILLED, filled=1))  # 2 (trade) lost -> resync from 2
        # broker, after the order snapshot: 4 = c2 ACCEPTED (its live frame is lost), 5 = trade of c2
        feed(h, trade_report(5, "c2", "T5", order_ref="2", sys_id="2"))
        h.broker.answer_resync(orders=[order_report(3, "c1", BrokerOrderStatus.FILLED, filled=1)],
                               trades=[trade_report(2, "c1", "T2"), trade_report(5, "c2", "T5", order_ref="2", sys_id="2")])
        assert h.core.stats.lost_frames == 1 and h.core.resyncing
        assert h.core.cursor(TD) == 3  # nothing beyond the hole was trusted
        assert steps(h) == [("orders", TD, 4), ("trades", TD, 4)]
        h.broker.answer_resync(orders=[order_report(4, "c2", BrokerOrderStatus.ACCEPTED, order_ref="2", sys_id="2")],
                               trades=[trade_report(5, "c2", "T5", order_ref="2", sys_id="2")])
        assert not h.core.resyncing and h.core.cursor(TD) == 5
        assert [(type(m).__name__, m.broker_seq) for m in exec_msgs(h)] == [
            ("BrokerOrderUpdate", 1), ("Trade", 2), ("BrokerOrderUpdate", 3), ("BrokerOrderUpdate", 4), ("Trade", 5)]

    def test_a_gap_above_the_queried_seqs_goes_through_the_live_path(self, harness: Harness) -> None:
        h = harness
        logged_in(h)
        feed(h, trade_report(1, None, "T1"))
        feed(h, trade_report(3, None, "T3"))  # 2 lost -> resync from 2
        feed(h, trade_report(5, None, "T5"))  # after both snapshots; 4 lost live too
        h.broker.answer_resync(trades=[trade_report(2, None, "T2"), trade_report(3, None, "T3")])
        assert h.core.cursor(TD) == 3 and h.core.resyncing and steps(h) == [("orders", TD, 4), ("trades", TD, 4)]
        h.broker.answer_resync(trades=[trade_report(4, None, "T4"), trade_report(5, None, "T5")])
        assert h.core.cursor(TD) == 5 and not h.core.resyncing
        assert [m.trade_id for m in exec_msgs(h)] == ["T1", "T2", "T3", "T4", "T5"]

    def test_buffer_overflow_restarts_the_resync(self, tmp_path: Path, default_config: FullConfig) -> None:
        h = make_harness(tmp_path / "td.db", default_config, max_resync_buffer=2)
        try:
            logged_in(h)
            feed(h, trade_report(1, None, "T1"))
            feed(h, trade_report(3, None, "T3"))  # resync from 2, buffer [3]
            feed(h, trade_report(4, None, "T4"))  # buffer [3, 4]: full
            feed(h, trade_report(5, None, "T5"))  # overflow: dropped with the buffer, resync restarted
            assert h.core.stats.buffer_overflows == 1 and h.core.resyncing
            assert steps(h) == [("orders", TD, 2), ("trades", TD, 2), ("orders", TD, 2), ("trades", TD, 2)]
            h.broker.answer_resync()  # the superseded first attempt: ignored
            assert h.core.resyncing
            h.broker.answer_resync(trades=[trade_report(i, None, f"T{i}") for i in (2, 3, 4, 5)])
            assert h.core.cursor(TD) == 5 and [m.trade_id for m in exec_msgs(h)] == ["T1", "T2", "T3", "T4", "T5"]
        finally:
            h.db.close()

    def test_heartbeat_tail_gap_triggers_a_resync(self, harness: Harness) -> None:
        h = harness
        logged_in(h)
        h.broker.cb.on_heartbeat(TD, 0)  # type: ignore[union-attr]
        assert not h.core.resyncing
        h.broker.cb.on_heartbeat(TD, 2)  # type: ignore[union-attr]
        assert h.core.resyncing and h.core.stats.tail_resyncs == 1
        h.broker.answer_resync(orders=[order_report(1, None, BrokerOrderStatus.CANCELED, order_ref="FC1")],
                               trades=[trade_report(2, None, "T2")])
        assert h.core.cursor(TD) == 2 and h.capture.types(EXEC3) == ["BrokerOrderUpdate", "Trade"]

    def test_orders_lost_with_the_link_are_rejected_after_resync(self, harness: Harness) -> None:
        h = harness
        logged_in(h)
        h.core.route_order(route("lost"), RE)
        h.core.route_order(route("kept", account="ACC4"), RE)
        h.broker.drop()
        assert status_of(h, "lost") == "UNKNOWN" and status_of(h, "kept") == "UNKNOWN"
        h.broker.login_ok(session_id=8)
        h.broker.answer_resync(orders=[order_report(1, "kept", BrokerOrderStatus.ACCEPTED, account="ACC4", order_ref="2")])
        assert status_of(h, "kept") == "ACCEPTED"
        assert h.rows("SELECT status, broker_code FROM routed_orders WHERE client_order_id='lost'") == [
            ("REJECTED", codes.BROKER_DISCONNECTED)]
        upd = exec_msgs(h)[-1]
        assert isinstance(upd, BrokerOrderUpdate) and upd.status is BrokerOrderStatus.REJECTED and upd.broker_seq == 0

    def test_restart_turns_in_flight_orders_into_unknown(self, tmp_path: Path, default_config: FullConfig) -> None:
        clock = SimClock(T0)
        h1 = make_harness(tmp_path / "td.db", default_config, clock=clock)
        logged_in(h1)
        h1.core.route_order(route("c1"), RE)
        h1.db.write(lambda c: c.exec_driver_sql(
            "INSERT INTO routed_orders (client_order_id, account_id, instrument_id, exchange, direction, offset, order_type,"
            " tif, price, qty, trading_day, src_instance, expire_ns, broker, status, filled_qty, received_ns, updated_ns)"
            " VALUES ('c2','ACC3','rb2701','SHFE','BUY','OPEN','LIMIT','GFD','3120',1,?, 'risk-engine-1', ?, 'sim',"
            " 'RECEIVED', 0, ?, ?)", (TD, T0 + SEC, T0, T0)))
        h1.db.close()  # crash-like: persisted RECEIVED (never sent) and SUBMITTED rows
        h2 = make_harness(tmp_path / "td.db", default_config, clock=clock)
        try:
            assert status_of(h2, "c1") == "UNKNOWN" and status_of(h2, "c2") == "UNKNOWN"
            h2.tick()
            assert h2.statuses[-1].pending_orders == 2
            logged_in(h2)
            assert status_of(h2, "c1") == "REJECTED" and status_of(h2, "c2") == "REJECTED"
            assert h2.broker.inserts == []  # a stale order is never re-sent after a restart
        finally:
            h2.db.close()

    def test_trading_day_roll_resyncs_the_old_day_tail(self, harness: Harness) -> None:
        h = harness
        logged_in(h)
        h.core._apply_reports([trade_report(1, None, "T1")])
        h.broker.cb.on_heartbeat(TD2, 0)  # type: ignore[union-attr]
        assert steps(h) == [("orders", TD, 2), ("trades", TD, 2)]
        h.broker.answer_resync(trades=[trade_report(2, None, "T2")])
        assert steps(h) == [("orders", TD2, 1), ("trades", TD2, 1)]
        h.broker.answer_resync()
        assert h.core.cursor(TD) == 2 and h.core.current_trading_day == TD2

    def test_first_report_of_a_new_day_also_checks_the_old_day_tail(self, harness: Harness) -> None:
        h = harness
        logged_in(h)
        h.core._apply_reports([trade_report(1, None, "T1")])
        feed(h, trade_report(1, None, "N1", td=TD2))
        assert h.core.resyncing and steps(h) == [("orders", TD, 2), ("trades", TD, 2)]
        h.broker.answer_resync(trades=[trade_report(2, None, "T2")])
        h.broker.answer_resync()  # new day: nothing beyond the buffered live report
        assert h.core.cursor(TD) == 2 and h.core.cursor(TD2) == 1
        assert [m.trade_id for m in exec_msgs(h)] == ["T1", "T2", "N1"]

    def test_login_after_a_skipped_trading_day_queries_every_day(self, harness: Harness) -> None:
        h = harness
        logged_in(h)
        h.core._apply_reports([trade_report(1, None, "T1")])
        h.broker.drop()
        h.broker.login_ok(trading_day=TD3, session_id=8)
        assert steps(h) == [("orders", TD, 2), ("trades", TD, 2)]
        h.broker.answer_resync()
        assert steps(h) == [("orders", TD2, 1), ("trades", TD2, 1)]  # 20260929 was never seen: queried too
        h.broker.answer_resync(trades=[trade_report(1, None, "M1", td=TD2)])
        assert steps(h) == [("orders", TD3, 1), ("trades", TD3, 1)]
        h.broker.answer_resync()
        assert h.core.ready and h.core.cursor(TD2) == 1 and h.core.current_trading_day == TD3
        assert [m.trade_id for m in exec_msgs(h)] == ["T1", "M1"]

    def test_day_roll_with_unknown_held_and_cancel_pending_orders(self, harness: Harness) -> None:
        h = harness
        logged_in(h)
        h.core.route_order(route("a", ttl_ns=600 * SEC), RE)
        feed(h, order_report(1, "a", BrokerOrderStatus.ACCEPTED))
        cancel(h, "xa", "a")  # sent, then the link drops before the broker answers
        h.core.route_order(route("b", ttl_ns=600 * SEC), RE)  # sent, never reported
        h.broker.drop()
        assert (status_of(h, "a"), status_of(h, "b")) == ("CANCEL_PENDING", "UNKNOWN")
        h.broker.login_ok(trading_day=TD2, session_id=8)  # the broker rolled to the next trading day meanwhile
        assert steps(h) == [("orders", TD, 2), ("trades", TD, 2)]
        h.core.route_order(route("c", ttl_ns=600 * SEC), RE)  # held while resyncing
        assert status_of(h, "c") == "RECEIVED"
        h.broker.answer_resync()  # old day tail: a still live, b never received
        h.broker.answer_resync()  # new day: nothing yet
        assert status_of(h, "b") == "REJECTED"
        assert status_of(h, "c") == "SUBMITTED" and [o.client_order_id for o in h.broker.inserts] == ["a", "b", "c"]
        assert [x[0] for x in h.broker.cancels] == ["a", "a"]  # the pending cancel went out again
        assert status_of(h, "a") == "CANCEL_PENDING"

    def test_broker_sequence_reset_refuses_routing_until_the_day_rolls(self, harness: Harness) -> None:
        h = harness
        logged_in(h)
        h.core._apply_reports([trade_report(i, None, f"T{i}") for i in range(1, 6)])
        h.broker.drop()
        h.broker.login_ok(session_id=8, head=2)  # the broker's broker_seq went backwards (sim.db wiped)
        h.broker.answer_resync()
        assert h.core.seq_conflict == TD and not h.core.healthy and not h.core.ready
        r = h.core.route_order(route("c9"), RE)
        assert r.code == codes.REJECT_GATEWAY_UNAVAILABLE and "sequence reset" in (r.message or "")
        assert h.core.detail()["broker_seq_conflict"] == TD
        h.broker.cb.on_heartbeat(TD2, 1)  # type: ignore[union-attr]
        assert h.core.seq_conflict is None
        h.broker.answer_resync()
        h.broker.answer_resync(trades=[trade_report(1, None, "N1", td=TD2)])
        assert h.core.healthy and h.core.ready

    def test_orders_held_before_a_sequence_reset_are_rejected_not_sent(self, harness: Harness) -> None:
        h = harness
        logged_in(h)
        h.core._apply_reports([trade_report(i, None, f"T{i}") for i in range(1, 6)])
        h.broker.drop()
        h.broker.login_ok(session_id=8, head=5)
        h.core.route_order(route("c1", ttl_ns=600 * SEC), RE)  # held during the login resync
        h.broker.drop()  # lost before the resync completed: still held
        h.broker.login_ok(session_id=9, head=2)  # and now the broker's sequence went backwards
        h.broker.answer_resync()
        assert h.broker.inserts == []
        assert h.rows("SELECT status, broker_code FROM routed_orders") == [("REJECTED", codes.REJECT_GATEWAY_UNAVAILABLE)]
        upd = exec_msgs(h)[-1]
        assert isinstance(upd, BrokerOrderUpdate) and upd.status is BrokerOrderStatus.REJECTED and upd.broker_seq == 0


# ---------------------------------------------------------------------------- queries, digest, import
class TestQueries:
    def _fill(self, h: Harness, n: int) -> None:
        reports = []
        for i in range(1, n + 1):
            acct = "ACC3" if i % 2 else "ACC4"
            reports.append(trade_report(i, None, f"T{i}", account=acct, price=3100.0 + i, qty=i))
        h.core._apply_reports(reports)

    def test_query_trades_pages_by_trade_seq(self, harness: Harness) -> None:
        h = harness
        logged_in(h)
        self._fill(h, 5)
        b1 = h.core.query_trades(TradeQuery(trading_day=TD, limit=2)).open_as(TradeBatch)
        assert [t.trade_seq for t in b1.trades] == [1, 2] and b1.next_trade_seq == 3 and not b1.complete
        b2 = h.core.query_trades(TradeQuery(trading_day=TD, from_trade_seq=b1.next_trade_seq, limit=10)).open_as(TradeBatch)
        assert [t.trade_seq for t in b2.trades] == [3, 4, 5] and b2.complete and b2.next_trade_seq == 6
        acc4 = h.core.query_trades(TradeQuery(trading_day=TD, account_id="ACC4")).open_as(TradeBatch)
        assert [t.trade_seq for t in acc4.trades] == [2, 4] and acc4.complete
        empty = h.core.query_trades(TradeQuery(trading_day=TD2)).open_as(TradeBatch)
        assert empty.trades == [] and empty.complete and empty.next_trade_seq == 1

    def test_digest_matches_the_published_trades(self, harness: Harness) -> None:
        h = harness
        logged_in(h)
        self._fill(h, 5)
        published: dict[str, list[Trade]] = {}
        for topic, _, m in h.capture.messages():
            if isinstance(m, Trade):
                published.setdefault(topic.split(".")[-1], []).append(m)
        d = h.core.digest(DigestRequest(trading_day=TD)).open_as(TradeDigest)
        assert d.total_count == 5 and [a.account_id for a in d.accounts] == ["ACC3", "ACC4"]
        for a in d.accounts:
            assert a == compute_account_digest(a.account_id, published[a.account_id])

    def test_query_positions_is_answered_later(self, harness: Harness) -> None:
        h = harness
        replies: list[Reply] = []
        assert h.core.query_positions(PositionQuery(trading_day=TD), replies.append).code == codes.UNAVAILABLE  # type: ignore[union-attr]
        logged_in(h)
        assert h.core.query_positions(PositionQuery(trading_day=TD, account_id="ACC3"), replies.append) is None
        assert h.broker.queries[0][:3] == ("positions", TD, "ACC3")
        h.broker.answer("positions", [PositionReport(account_id="ACC3", instrument_id="rb2701",
                                                     direction=PosDirection.LONG, td_qty=2, yd_qty=1)])
        batch = replies[0].open_as(BrokerPositionBatch)
        assert batch.trading_day == TD and len(batch.items) == 1
        assert (batch.items[0].direction, batch.items[0].td_qty, batch.items[0].yd_qty) == (PosDirection.LONG, 2, 1)

    def test_query_positions_is_unavailable_during_a_resync(self, harness: Harness) -> None:
        h = harness
        h.broker.login_ok()  # login resync still running: the broker's view may hold unpublished fills
        r = h.core.query_positions(PositionQuery(trading_day=TD), lambda _: None)
        assert r is not None and r.code == codes.UNAVAILABLE and "resync" in (r.message or "")
        assert h.broker.pending("positions") == []

    def test_import_dedupes_publishes_and_is_idempotent(self, harness: Harness) -> None:
        h = harness
        logged_in(h)
        h.core._apply_reports([trade_report(1, None, "T1")])  # already known from the broker
        known = Trade(trading_day=TD, exchange=Exchange.SHFE, trade_id="T1", direction=Direction.BUY, account_id="ACC3",
                      instrument_id="rb2701", broker_order_ref="x", offset=Offset.OPEN, price=3119.0, qty=1,
                      trade_ts_ns=T0)
        new = Trade(trading_day=TD, exchange=Exchange.SHFE, trade_id="M1", direction=Direction.SELL, account_id="ACC3",
                    instrument_id="rb2701", broker_order_ref="x", offset=Offset.CLOSE_TODAY, price=3125.5, qty=1,
                    trade_ts_ns=T0 + SEC)
        req = TradeImport(command_id="imp1", trading_day=TD, trades=[known, new], source="broker.csv", actor="admin",
                          role=Role.ADMIN)
        r = h.core.import_trades(req, API)
        assert r.ok and r.open_as(TradeImportAck) == TradeImportAck(inserted=1, duplicates=1)
        imported = exec_msgs(h)[-1]
        assert isinstance(imported, Trade) and imported.trade_id == "M1" and imported.origin is TradeOrigin.EXTERNAL
        assert imported.broker_order_ref == "IMPORT:imp1" and imported.broker_seq is None and imported.trade_seq == 2
        assert h.rows("SELECT import_batch, origin FROM trades WHERE trade_id='M1'") == [("imp1", "EXTERNAL")]
        dup = h.core.import_trades(req, API)
        assert dup.duplicate and dup.open_as(TradeImportAck).inserted == 1
        assert h.scalar("SELECT count(*) FROM trades") == 2
        viewer = TradeImport(command_id="imp2", trading_day=TD, trades=[new], source="x", actor="v", role=Role.VIEWER)
        assert h.core.import_trades(viewer, API).code == codes.FORBIDDEN
        wrong_day = TradeImport(command_id="imp3", trading_day=TD2, trades=[new], source="x", actor="a", role=Role.ADMIN)
        assert h.core.import_trades(wrong_day, API).code == codes.BAD_REQUEST


# ---------------------------------------------------------------------------- td.status
class TestStatus:
    def test_status_follows_the_connection_and_resync(self, harness: Harness) -> None:
        h = harness
        h.tick()
        assert h.statuses[-1].state is BrokerConnState.DISCONNECTED and h.statuses[-1].broker == "sim"
        h.broker.login_ok()
        h.core.route_order(route("c1"), RE)  # held
        h.tick()
        st = h.statuses[-1]
        assert st.state is BrokerConnState.RESYNCING and st.pending_orders == 1
        assert (st.trading_day, st.session_id, st.front_id) == (TD, "7", 1)
        h.broker.answer_resync()
        h.tick()  # state change RESYNCING -> LOGGED_IN: sent at once
        assert h.statuses[-1].state is BrokerConnState.LOGGED_IN
        n = len(h.statuses)
        h.core._apply_reports([order_report(1, "c1", BrokerOrderStatus.ACCEPTED)])
        h.tick()  # a report is not a state change: no immediate td.status
        assert len(h.statuses) == n
        h.advance(1_100 * MS)
        h.tick()  # 1 Hz
        st = h.statuses[-1]
        assert len(h.statuses) == n + 1
        assert st.state is BrokerConnState.LOGGED_IN and st.last_broker_seq == 1 and st.pending_orders == 0
        h.broker.drop()
        h.tick()
        assert h.statuses[-1].state is BrokerConnState.DISCONNECTED and h.statuses[-1].session_id is None

    def test_status_follows_the_rolled_trading_day(self, harness: Harness) -> None:
        h = harness
        logged_in(h)
        h.core._apply_reports([trade_report(1, None, "T1"), trade_report(2, None, "T2")])
        h.broker.cb.on_heartbeat(TD2, 1)  # type: ignore[union-attr]
        h.broker.answer_resync()
        h.broker.answer_resync(trades=[trade_report(1, None, "N1", td=TD2)])
        h.advance(1_100 * MS)
        h.tick()
        st = h.statuses[-1]
        assert (st.trading_day, st.last_broker_seq) == (TD2, 1)
