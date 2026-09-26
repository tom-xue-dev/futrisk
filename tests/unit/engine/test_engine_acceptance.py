"""Acceptance scenario QS-03 / QS-04 at core level (01 §6.3, §8.14; 02 §8.3, §11.2).

ACC3 (initial capital 10,000,000) holds rb / hc / i longs opened today; marks fall so that
``daily_pnl_pct_init`` crosses −2% (ALERT), −2.5% (RESTRICT: size factor 0.5 + no add to
losers) and −3% (CIRCUIT_BREAK: HALTED at the first breaching state, latched after a second
distinct LIVE state or 1s).
"""

from __future__ import annotations

import pytest
from enginetest_support import (
    CT,
    RM,
    TD,
    B,
    Harness,
    O,
    S,
    rules_only,
)

from futrisk.core.effects import CancelRoute, ResolveRoute
from futrisk.core.risk_account import Lifecycle
from futrisk.messages import codes
from futrisk.messages.enums import AccountState, Action, MdQuality, OrderStatus, RiskEventKind, RuleSmState
from futrisk.messages.risk import ResetAck, ResetRequest

HALT_KEY_1 = f"{TD}|daily_loss|halt|acct:ACC3|1"
HALT_KEY_2 = f"{TD}|daily_loss|halt|acct:ACC3|2"

# price states (rb, hc, i) and the resulting daily P&L of the ACC3 book (80 rb, 75 hc, 32 i)
P0 = {"rb2701": 3100.0, "hc2701": 3300.0, "i2701": 800.0}
P_WARN = {"rb2701": 3013.0, "hc2701": 3208.0, "i2701": 777.5}  # ≈ −2.11%
P_RESTRICT = {"rb2701": 2990.0, "hc2701": 3190.0, "i2701": 772.0}  # ≈ −2.60%
P_HALT = {"rb2701": 2970.0, "hc2701": 3170.0, "i2701": 767.0}  # ≈ −3.07%
P_HALT_2 = {"rb2701": 2969.0, "hc2701": 3170.0, "i2701": 767.0}  # next distinct state, still beyond −3%
P_MID = {"rb2701": 2980.0, "hc2701": 3180.0, "i2701": 769.0}  # ≈ −2.90%
P_DEEP = {"rb2701": 2960.0, "hc2701": 3160.0, "i2701": 764.0}  # ≈ −3.31%
P_REARM = {"rb2701": 2946.0, "hc2701": 3136.0, "i2701": 753.0}  # ≈ −3.97%


def acc3_book(metric: str = "account.daily_pnl_pct_init") -> Harness:
    rules = rules_only("daily_loss", daily_loss={"metric": metric})
    h = Harness(rules)
    h.ticks(P0)
    h.open_position("ACC3", "rb2701", B, 80, 3100.0)
    h.open_position("ACC3", "hc2701", B, 75, 3300.0)
    h.open_position("ACC3", "i2701", B, 32, 800.0)
    return h


def pct(h: Harness, account: str = "ACC3") -> float:
    return h.eng.snapshot(account).metrics.daily_pnl_pct_init


def machine(h: Harness, level: str, account: str = "ACC3") -> RuleSmState:
    m = h.eng.account(account).machines.get(("daily_loss", level, f"acct:{account}"))
    return m.state if m is not None else RuleSmState.OK


class TestAcceptanceBreaker:
    def test_full_path_alert_restrict_halt_latch_reset_rearm(self) -> None:
        h = acc3_book()
        ra = h.eng.account("ACC3")
        assert ra.state == AccountState.NORMAL
        assert -0.0001 < pct(h) < 0  # only the opening fees

        # resting orders before the shock: a close (must survive the breaker) and an open that becomes UNKNOWN
        r_close, _ = h.submit("ACC3", "rb2701", S, CT, 5, 3150.0)
        assert r_close.ok
        close_id = r_close.open().client_order_id
        h.route_ok(close_id)
        r_unk, _ = h.submit("ACC3", "rb2701", B, O, 1, 3050.0)
        unk_id = r_unk.open().client_order_id
        h.advance(2100)
        timer = h.timer()
        assert h.eng.orders.get(unk_id).status == OrderStatus.UNKNOWN
        assert [e.client_order_id for e in timer if isinstance(e, ResolveRoute)] == [unk_id]

        # −2%: ALERT → WARNING
        h.ticks(P_WARN)
        assert pct(h) == pytest.approx(-0.02106, abs=1e-4)
        assert ra.state == AccountState.WARNING
        warn = h.events(RiskEventKind.ALERT_RAISED)
        assert [e.level for e in warn] == ["warn"] and warn[0].action == Action.ALERT

        # −2.5%: RESTRICT → order size factor 0.5 and no add to losers
        h.ticks(P_RESTRICT)
        assert ra.state == AccountState.RESTRICTED
        kinds = sorted(r.kind.value for r in ra.restrictions)
        assert kinds == ["NO_ADD_TO_LOSERS", "ORDER_SIZE_FACTOR"]
        rep, _ = h.submit("ACC3", "rb2701", B, O, 300)
        assert rep.code == codes.REJECT_ORDER_SIZE_RESTRICTED  # 500 × 0.5 = 250
        rep, _ = h.submit("ACC3", "rb2701", B, O, 1)
        assert rep.code == codes.REJECT_ADD_TO_LOSER
        # resting opens that the breaker must cancel: one ROUTED, one still ROUTING
        rep_i, _ = h.submit("ACC3", "i2701", S, O, 2, 790.0)
        assert rep_i.ok, rep_i.code
        routed_id = rep_i.open().client_order_id
        h.route_ok(routed_id)
        rep_hc, _ = h.submit("ACC3", "hc2701", S, O, 1, 3230.0)
        assert rep_hc.ok, rep_hc.code
        routing_id = rep_hc.open().client_order_id
        assert h.eng.orders.get(routing_id).status == OrderStatus.ROUTING

        # −3%: first breaching state → HALTED immediately (not latched) + cancel every resting open order
        effs = h.ticks(P_HALT)
        assert pct(h) < -0.03
        assert ra.state == AccountState.HALTED and ra.halted_latched is False
        assert machine(h, "halt") == RuleSmState.PENDING
        cancelled = {e.client_order_id for e in effs if isinstance(e, CancelRoute)}
        assert cancelled == {unk_id, routed_id, routing_id}  # 02 §8.3：包括 ROUTING 和 UNKNOWN
        assert close_id not in cancelled
        for cid in cancelled:
            assert h.eng.orders.get(cid).status == OrderStatus.CANCEL_PENDING
        assert not [e for e in h.events(RiskEventKind.ALERT_RAISED) if e.level == "halt"]
        halted = h.state_changes("ACC3", effs)
        assert halted[-1].to_state == AccountState.HALTED and halted[-1].halted_latched is False
        # from this moment opens are rejected, closes still pass
        rep, _ = h.submit("ACC3", "hc2701", S, O, 1, 3170.0)
        assert rep.code == codes.REJECT_ACCOUNT_HALTED
        rep, _ = h.submit("ACC3", "hc2701", S, CT, 5, 3170.0)
        assert rep.ok

        # second distinct LIVE state → confirmed, latched, exactly one ALERT_RAISED (latched=true)
        h.advance(500)
        effs = h.ticks(P_HALT_2)
        assert machine(h, "halt") == RuleSmState.ACTIVE
        assert ra.halted_latched is True
        raised = [e for e in h.events(RiskEventKind.ALERT_RAISED) if e.level == "halt"]
        assert len(raised) == 1
        assert raised[0].alert_key == HALT_KEY_1 and raised[0].latched is True
        assert raised[0].action == Action.CIRCUIT_BREAK and raised[0].value is not None and raised[0].value < -0.03
        assert h.state_changes("ACC3", effs)[-1].halted_latched is True
        row = next(r for r in h.db.tables["account_states"] if r["account_id"] == "ACC3")
        assert (row["state"], row["halted_latched"], row["state_version"]) == ("HALTED", True, ra.state_version)
        # more bad states and recovery do not raise again and do not release the latch
        h.advance(500)
        h.ticks(P_DEEP)
        h.advance(500)
        h.ticks(P_MID)
        h.advance(5000)
        h.timer()
        assert ra.state == AccountState.HALTED and ra.halted_latched
        assert len([e for e in h.events(RiskEventKind.ALERT_RAISED) if e.level == "halt"]) == 1

        # risk.reset at ≈ −2.9% → REARMED, rearm line = min(−3%, value − 1%)
        value = pct(h)
        rep, effs = h.cmd("risk.reset", ResetRequest(command_id="c-reset", account_id="ACC3", reason="reviewed", actor="rm", role=RM))
        assert rep.ok
        ack = rep.open_as(ResetAck)
        assert machine(h, "halt") == RuleSmState.REARMED
        assert ack.rearm_threshold == pytest.approx(min(-0.03, value - 0.01), abs=1e-9)
        assert ra.state == AccountState.RESTRICTED  # restrict level is still active (not latched)
        resets = h.events(RiskEventKind.RESET, effs)
        assert [e.alert_key for e in resets] == [HALT_KEY_1]
        assert h.db.critical_labels[-1] == "risk.reset"

        # deterioration between −3% and the rearm line does not trigger
        h.advance(500)
        h.ticks(P_DEEP)
        assert machine(h, "halt") == RuleSmState.REARMED and ra.state != AccountState.HALTED
        # beyond the rearm line (≈ 1% worse) → new episode
        h.advance(500)
        h.ticks(P_REARM)
        assert ra.state == AccountState.HALTED and not ra.halted_latched
        h.advance(500)
        h.ticks({"rb2701": 2945.0})
        raised = [e for e in h.events(RiskEventKind.ALERT_RAISED) if e.level == "halt"]
        assert [e.alert_key for e in raised] == [HALT_KEY_1, HALT_KEY_2]
        assert ra.halted_latched

    def test_dirty_account_is_evaluated_before_the_pretrade_check(self) -> None:
        h = acc3_book()
        h.ticks(P_RESTRICT)
        for inst, px in P_HALT.items():
            h.tick(inst, px, evaluate=False)  # marks applied, evaluation not run yet in this loop
        assert h.eng.account("ACC3").dirty
        rep, effs = h.submit("ACC3", "hc2701", S, O, 1, 3170.0)
        assert rep.code == codes.REJECT_ACCOUNT_HALTED
        assert h.state_changes("ACC3", effs)[-1].to_state == AccountState.HALTED

    def test_single_bad_state_reverting_before_confirmation_revokes_the_halt(self) -> None:
        h = acc3_book()
        h.ticks(P_RESTRICT)
        rep, _ = h.submit("ACC3", "i2701", S, O, 2, 790.0)
        oid = rep.open().client_order_id
        h.route_ok(oid)
        h.ticks(P_HALT)
        ra = h.eng.account("ACC3")
        assert ra.state == AccountState.HALTED and not ra.halted_latched
        h.advance(300)
        effs = h.ticks(P_RESTRICT)  # back inside the threshold before the second state / 1s
        assert machine(h, "halt") == RuleSmState.OK
        assert ra.state == AccountState.RESTRICTED
        assert h.state_changes("ACC3", effs)[-1].from_state == AccountState.HALTED
        assert h.eng.orders.get(oid).status == OrderStatus.CANCEL_PENDING  # cancelled orders stay cancelled
        h.advance(2000)
        h.timer()
        assert not [e for e in h.events(RiskEventKind.ALERT_RAISED) if e.level == "halt"]
        rep, _ = h.submit("ACC3", "i2701", S, O, 1, 772.0)
        assert rep.ok  # opening is possible again (the short side is not a loser)

    def test_confirmation_by_time_without_a_second_state(self) -> None:
        h = acc3_book()
        h.ticks(P_HALT)
        assert machine(h, "halt") == RuleSmState.PENDING
        h.advance(999)
        h.timer()
        assert machine(h, "halt") == RuleSmState.PENDING
        assert h.eng.next_timer_ns() is not None
        h.advance(1)
        h.timer()
        assert machine(h, "halt") == RuleSmState.ACTIVE
        assert h.eng.account("ACC3").halted_latched
        assert [e.alert_key for e in h.events(RiskEventKind.ALERT_RAISED) if e.level == "halt"] == [HALT_KEY_1]

    def test_floating_only_variant_also_triggers(self) -> None:
        h = acc3_book("account.floating_pnl_pct_init")
        snap = h.eng.snapshot("ACC3")
        assert snap.metrics.floating_pnl_pct_init == pytest.approx(0.0)
        assert snap.metrics.daily_pnl_pct_init < 0  # fees only in the default metric
        h.ticks(P_HALT)
        h.advance(200)
        h.ticks(P_HALT_2)
        ra = h.eng.account("ACC3")
        assert ra.state == AccountState.HALTED and ra.halted_latched
        raised = [e for e in h.events(RiskEventKind.ALERT_RAISED) if e.level == "halt"]
        assert len(raised) == 1 and raised[0].metric == "account.floating_pnl_pct_init"
        rep, _ = h.submit("ACC3", "rb2701", B, O, 1)
        assert rep.code == codes.REJECT_ACCOUNT_HALTED

    def test_quality_gating_freezes_the_machines(self) -> None:
        h = acc3_book()
        h.ticks(P_RESTRICT)
        for inst, px in P_HALT.items():
            h.tick(inst, px, evaluate=False)
        h.md_status({"rb2701": MdQuality.STALE})  # evaluated while STALE: no transition at all
        assert machine(h, "halt") == RuleSmState.OK
        assert h.eng.account("ACC3").state == AccountState.RESTRICTED
        rs = [r for r in h.eng.snapshot("ACC3").rules if r.rule_id == "daily_loss"][0]
        assert rs.value is None  # 02 §7.6：行情质量不是 OK 时为 null
        h.md_status({"rb2701": MdQuality.OK})
        assert machine(h, "halt") == RuleSmState.PENDING
        # timers are frozen too
        h.md_status({"rb2701": MdQuality.STALE})
        h.advance(3000)
        h.timer()
        assert machine(h, "halt") == RuleSmState.PENDING
        h.md_status({"rb2701": MdQuality.OK})
        assert machine(h, "halt") == RuleSmState.ACTIVE  # 1s elapsed: confirmed as soon as quality is OK

    def test_close_orders_pass_while_halted_and_latched(self) -> None:
        h = acc3_book()
        h.ticks(P_HALT)
        h.advance(100)
        h.ticks(P_HALT_2)
        assert h.eng.account("ACC3").halted_latched
        for inst, qty in (("rb2701", 10), ("hc2701", 10)):
            rep, effs = h.submit("ACC3", inst, S, CT, qty, P_HALT_2[inst])
            assert rep.ok, rep.code
            assert h.routes(effs)[0].order.offset == CT
        rep, _ = h.submit("ACC3", "i2701", S, O, 1, 767.0)
        assert rep.code == codes.REJECT_ACCOUNT_HALTED


class TestRestartRestore:
    def test_restore_rebuilds_identical_state_and_no_duplicate_alerts(self) -> None:
        a = acc3_book()
        a.ticks(P_RESTRICT)
        rep, _ = a.submit("ACC3", "i2701", S, O, 2, 790.0)
        a.route_ok(rep.open().client_order_id)
        a.ticks(P_HALT)
        a.advance(200)
        a.ticks(P_HALT_2)
        ra = a.eng.account("ACC3")
        assert ra.halted_latched

        b = Harness(a.rules, load=False)
        b.clock.set(a.clock.now_ns())
        b.eng.restore(a.db.snapshot(), lifecycle=Lifecycle.RUNNING)
        rb = b.eng.account("ACC3")
        assert (rb.state, rb.halted_latched, rb.state_version) == (ra.state, ra.halted_latched, ra.state_version)
        assert {k: m.st.persisted() for k, m in rb.machines.items()} == {k: m.st.persisted() for k, m in ra.machines.items()}
        assert set(b.eng.alerts) == set(a.eng.alerts)
        assert sorted(map(repr, rb.restrictions)) == sorted(map(repr, ra.restrictions))
        assert rb.ledger.book.positions("ACC3")[0].qty == ra.ledger.book.positions("ACC3")[0].qty
        # the market snapshot after restart delivers the same prices: no new alert, same funds
        b.ticks(P_HALT_2)
        b.run(b.eng.post_restore())
        assert b.events(RiskEventKind.ALERT_RAISED) == []
        fa, fb = a.eng.snapshot("ACC3").funds, b.eng.snapshot("ACC3").funds
        assert fb.equity == pytest.approx(fa.equity) and fb.frozen_margin == pytest.approx(fa.frozen_margin)
        # both engines keep reacting identically afterwards
        for h in (a, b):
            h.advance(500)
            h.ticks(P_DEEP)
        keys_a = {e.alert_key for e in a.events(RiskEventKind.ALERT_RAISED)}
        keys_b = {e.alert_key for e in b.events(RiskEventKind.ALERT_RAISED)}
        assert keys_b <= keys_a and HALT_KEY_1 not in keys_b
        assert rb.state == AccountState.HALTED and rb.halted_latched
        rep, _ = b.submit("ACC3", "rb2701", B, O, 1, coid="after-restart-1")
        assert rep.code == codes.REJECT_ACCOUNT_HALTED
        rep, _ = b.submit("ACC3", "rb2701", B, O, 1, coid="o1")  # same id as before the crash, other content
        assert rep.code == codes.REJECT_DUPLICATE_ORDER_ID

    def test_restart_turns_routing_into_unknown_and_resolves(self) -> None:
        a = acc3_book()
        rep, _ = a.submit("ACC3", "rb2701", B, O, 1, 3050.0)
        coid = rep.open().client_order_id
        b = Harness(a.rules, load=False)
        b.clock.set(a.clock.now_ns())
        b.eng.restore(a.db.snapshot(), lifecycle=Lifecycle.WARMUP)
        effs = b.run(b.eng.post_restore())
        assert b.eng.orders.get(coid).status == OrderStatus.UNKNOWN
        assert [e.client_order_id for e in effs if isinstance(e, ResolveRoute)] == [coid]
        assert all(any(r.source == "SYSTEM:STARTING" for r in b.eng.account(x).restrictions) for x in ("ACC3", "ACC4"))
        # warm-up: opens blocked, closes allowed (02 §8.6)
        b.ticks(P0)
        rep, _ = b.submit("ACC3", "rb2701", B, O, 1, coid="w1")
        assert rep.code == codes.REJECT_ACCOUNT_BLOCKED and rep.open().reject_detail["source"] == "SYSTEM:STARTING"
        rep, _ = b.submit("ACC3", "rb2701", S, CT, 1, coid="w2")
        assert rep.ok
