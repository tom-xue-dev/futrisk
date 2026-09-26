"""Effects, rule compilation and the alert state machine (01 §8.14; 02 §7.3, §8.1, §10)."""

from __future__ import annotations

import pytest
from enginetest_support import ADMIN, CT, B, Harness, S, config, rules_only, sod_snapshot

from futrisk.common.config.risk_rules import RuleDef
from futrisk.core.effects import (
    CancelRoute,
    Persist,
    Publish,
    ReplyTo,
    Route,
    Tx,
    is_ordered,
    merge_effects,
    request_hash,
)
from futrisk.core.rules import RuleBook, alert_key, compile_rule, parse_scope, split_alert_key
from futrisk.core.state_machine import LevelMachine, SmState, Tr, state_from_row
from futrisk.messages.enums import (
    AccountState,
    Action,
    CashType,
    Direction,
    Exchange,
    MdQuality,
    Offset,
    OrderType,
    PosDirection,
    RiskEventKind,
    RuleSmState,
    TimeInForce,
    TradeOrigin,
)
from futrisk.messages.envelope import ok_reply
from futrisk.messages.eod import SodLot
from futrisk.messages.orders import OrderRequest, RouteOrder
from futrisk.messages.risk import CashMovement, CashMoveRequest

MS = 1_000_000
T0 = 1_000_000_000_000


def rule(**kw: object) -> RuleDef:
    base: dict[str, object] = {
        "id": "daily_loss",
        "metric": "account.daily_pnl_pct_init",
        "op": "<=",
        "levels": [
            {"name": "warn", "threshold": -0.02, "action": "ALERT"},
            {"name": "halt", "threshold": -0.03, "action": "CIRCUIT_BREAK", "latch": True, "rearm": {"extra": 0.01}},
        ],
    }
    base.update(kw)
    return RuleDef.model_validate(base)


def machine(level: str = "halt", **kw: object) -> LevelMachine:
    r = compile_rule(rule(**kw))
    lv = r.level("ACC3", level)
    assert lv is not None
    return LevelMachine(r, lv, "acct:ACC3")


class TestEffects:
    def test_tx_orders_effects_persist_actions_publish_reply(self) -> None:
        tx = Tx(label="t")
        tx.reply("tok", ok_reply())
        tx.publish("risk.snapshot.ACC3", CashMovement(movement_id="m", account_id="A", trading_day="20260929",
                                                        type="DEPOSIT", amount="1.00", actor="a", reason="r", ts_ns=1))
        tx.cancel("c1", "cmd1", "why")
        tx.upsert("orders", ("client_order_id",), {"client_order_id": "c1"})
        effs = tx.effects()
        assert [type(e) for e in effs] == [Persist, CancelRoute, Publish, ReplyTo]
        assert is_ordered(effs)
        assert not is_ordered(list(reversed(effs)))

    def test_merge_effects_keeps_a_single_persist(self) -> None:
        a, b = Tx(label="a"), Tx(label="b")
        a.insert("fills", {"x": 1})
        b.insert("fills", {"x": 2})
        b.mark_critical()
        route = RouteOrder(client_order_id="c", account_id="A", instrument_id="rb2701", exchange=Exchange.SHFE,
                           direction=Direction.BUY, offset=Offset.OPEN, order_type=OrderType.LIMIT, tif=TimeInForce.GFD,
                           price=1.0, qty=1, trading_day="20260929", risk_passed_ns=1, expire_ns=2, src_instance="x")
        b.route(route)
        merged = merge_effects(a.effects(), b.effects())
        persists = [e for e in merged if isinstance(e, Persist)]
        assert len(persists) == 1 and persists[0].critical and len(persists[0].rows) == 2
        assert isinstance(merged[1], Route)
        assert persists[0].rows_by_table()["fills"][1].values == {"x": 2}

    def test_request_hash_ignores_time_fields(self) -> None:
        r1 = OrderRequest(client_order_id="c", account_id="A", instrument_id="rb2701", direction=Direction.BUY,
                          offset=Offset.OPEN, price=3100.0, qty=1, sent_ns=1)
        r2 = OrderRequest(client_order_id="c", account_id="A", instrument_id="rb2701", direction=Direction.BUY,
                          offset=Offset.OPEN, price=3100.0, qty=1, sent_ns=999)
        r3 = OrderRequest(client_order_id="c", account_id="A", instrument_id="rb2701", direction=Direction.BUY,
                          offset=Offset.OPEN, price=3100.0, qty=2, sent_ns=1)
        assert request_hash(r1) == request_hash(r2) != request_hash(r3)


class TestRules:
    def test_breaker_defaults_and_overrides(self, default_config: object) -> None:
        book = RuleBook(default_config.risk_rules)  # type: ignore[attr-defined]
        dl = book.get("daily_loss")
        assert dl is not None and dl.pretrade and dl.lower_is_worse
        halt = dl.level("ACC3", "halt")
        assert halt is not None and halt.latch and halt.action == Action.CIRCUIT_BREAK
        assert (halt.confirm_states, halt.confirm_ms, halt.rearm_extra) == (2, 1000, 0.01)
        warn = dl.level("ACC3", "warn")
        assert warn is not None and (warn.confirm_states, warn.confirm_ms) == (1, 0)
        assert halt.blocks_open and not warn.blocks_open
        # a circuit break without explicit confirm gets the 01 §8.14 default (2 states or 1s)
        r = compile_rule(rule(overrides={"ACC2": {"halt": {"threshold": -0.04}}}))
        h = r.level(None, "halt")
        assert h is not None and (h.confirm_states, h.confirm_ms) == (2, 1000)
        assert r.level("ACC2", "halt").threshold == -0.04  # type: ignore[union-attr]
        assert r.level("ACC3", "halt").threshold == -0.03  # type: ignore[union-attr]
        with pytest.raises(ValueError):
            compile_rule(rule(overrides={"ACC2": {"nope": {"threshold": 1}}}))

    def test_clear_line_rearm_line_and_direction(self) -> None:
        loss = compile_rule(rule())
        assert loss.clear_line(-0.03) == pytest.approx(-0.024)
        assert loss.rearm_line(-0.03, -0.035, 0.01) == pytest.approx(-0.045)
        assert loss.rearm_line(-0.03, -0.021, 0.01) == pytest.approx(-0.031)
        dd = compile_rule(RuleDef.model_validate({"id": "dd", "metric": "account.intraday_dd", "op": ">=",
                                                  "levels": [{"name": "w", "threshold": 0.025, "action": "ALERT"}]}))
        assert dd.clear_line(0.025) == pytest.approx(0.02)
        assert dd.rearm_line(0.025, 0.03, 0.01) == pytest.approx(0.04)
        nav = compile_rule(RuleDef.model_validate({"id": "nav", "metric": "account.nav", "op": "<=", "hysteresis": 0.02,
                                                   "levels": [{"name": "w", "threshold": 0.9, "action": "ALERT"}]}))
        assert nav.clear_line(0.9) == pytest.approx(0.918)  # 越线方向的内侧，按规则自己的 hysteresis
        assert loss.utilization(-0.015, -0.02) == pytest.approx(0.75)
        assert nav.utilization(0.95, 0.9) == pytest.approx(0.9 / 0.95)

    def test_configured_hysteresis_is_per_rule(self) -> None:
        """M8: every rule uses its own configured hysteresis (risk_rules.yaml), not the 0.2 default."""
        cfg = config().risk_rules
        book = RuleBook(cfg)
        for rd in cfg.rules:
            r = book.get(rd.id)
            assert r is not None and r.hysteresis == pytest.approx(rd.hysteresis)
            for lv in r.base_levels:
                band = abs(lv.threshold) * rd.hysteresis
                expected = lv.threshold + band if r.lower_is_worse else lv.threshold - band
                assert r.clear_line(lv.threshold) == pytest.approx(expected)
        nav = book.get("nav_floor")
        assert nav is not None
        h = cfg.rule("nav_floor").hysteresis  # type: ignore[union-attr]
        assert nav.clear_line(0.9) == pytest.approx(0.9 * (1 + h)) and nav.clear_line(0.85) == pytest.approx(0.85 * (1 + h))
        assert compile_rule(rule(hysteresis=0.05)).clear_line(-0.03) == pytest.approx(-0.0285)  # explicit value wins
        assert compile_rule(rule()).clear_line(-0.03) == pytest.approx(-0.024)  # default 0.2

    def test_latch_only_on_circuit_break_or_liquidate(self) -> None:
        for action in ("ALERT", "RESTRICT"):
            with pytest.raises(ValueError, match="latch"):
                compile_rule(rule(levels=[{"name": "x", "threshold": -0.02, "action": action, "latch": True}]))
        for action in ("CIRCUIT_BREAK", "LIQUIDATE"):
            assert compile_rule(rule(levels=[{"name": "x", "threshold": -0.02, "action": action, "latch": True}]))
        with pytest.raises(ValueError, match="latch"):  # also through a per-account override
            compile_rule(rule(overrides={"ACC3": {"warn": {"latch": True}}}))

    def test_confirm_states_zero_is_time_only(self) -> None:
        lv = {"name": "warn", "threshold": -0.02, "action": "ALERT", "confirm": {"states": 0, "ms": 30000}}
        m = machine("warn", levels=[lv])
        assert m.level.confirm_states == 0 and m.level.confirm_ms == 30000
        assert m.evaluate(-0.021, quality_ok=True, state_key=1, now_ns=T0) == [Tr.PENDING]
        for k in range(2, 12):  # any number of distinct LIVE states does not confirm
            assert m.evaluate(-0.025, quality_ok=True, state_key=k, now_ns=T0 + k * MS) == []
        assert m.state == RuleSmState.PENDING and m.st.confirm_count == 1
        assert m.due_ns() == T0 + 30_000 * MS
        assert m.reevaluate(T0 + 29_999 * MS, quality_ok=True) == []
        assert m.reevaluate(T0 + 30_000 * MS, quality_ok=True) == [Tr.ACTIVATED]
        with pytest.raises(ValueError, match="confirm.ms"):
            compile_rule(rule(levels=[{**lv, "confirm": {"states": 0, "ms": 0}}]))
        one = machine("warn", levels=[{**lv, "confirm": {"states": 1, "ms": 30000}}])  # states >= 1 unchanged
        assert one.evaluate(-0.021, quality_ok=True, state_key=1, now_ns=T0) == [Tr.ACTIVATED]

    def test_scope_and_alert_keys(self) -> None:
        sk = parse_scope("acct:ACC3/prod:rb")
        assert (sk.account_id, sk.kind, sk.value, sk.is_firm) == ("ACC3", "prod", "rb", False)
        assert parse_scope("firm/sector:black").is_firm
        key = alert_key("20260928", "daily_loss", "halt", "acct:ACC3", 1)
        assert key == "20260928|daily_loss|halt|acct:ACC3|1"
        assert split_alert_key(key) == ("20260928", "daily_loss", "halt", "acct:ACC3", 1)


class TestStateMachine:
    def test_confirm_needs_distinct_states(self) -> None:
        m = machine(levels=[{"name": "halt", "threshold": -0.03, "action": "CIRCUIT_BREAK", "latch": True,
                             "confirm": {"states": 2, "ms": 0}}])
        assert m.evaluate(-0.031, quality_ok=True, state_key=1, now_ns=T0) == [Tr.PENDING]
        assert m.evaluate(-0.032, quality_ok=True, state_key=1, now_ns=T0 + 10 * MS) == []  # same LIVE state
        assert m.state == RuleSmState.PENDING and m.st.confirm_count == 1
        assert m.evaluate(-0.032, quality_ok=True, state_key=2, now_ns=T0 + 20 * MS) == [Tr.ACTIVATED]
        assert m.state == RuleSmState.ACTIVE and m.st.latched and m.st.episode == 1

    def test_confirm_by_time(self) -> None:
        m = machine()
        m.evaluate(-0.031, quality_ok=True, state_key=1, now_ns=T0)
        assert m.due_ns() == T0 + 1000 * MS
        assert m.reevaluate(T0 + 999 * MS, quality_ok=True) == []
        assert m.reevaluate(T0 + 1000 * MS, quality_ok=True) == [Tr.ACTIVATED]

    def test_immediate_confirmation_reports_no_pending(self) -> None:
        m = machine("warn")
        assert m.evaluate(-0.021, quality_ok=True, state_key=1, now_ns=T0) == [Tr.ACTIVATED]

    def test_revoked_before_confirmation(self) -> None:
        m = machine()
        m.evaluate(-0.031, quality_ok=True, state_key=1, now_ns=T0)
        assert m.evaluate(-0.029, quality_ok=True, state_key=2, now_ns=T0 + MS) == [Tr.REVOKED]
        assert m.state == RuleSmState.OK and m.st.episode == 0

    def test_hysteresis_hold_and_cooldown(self) -> None:
        m = machine("warn", hold_ms=3000, cooldown_s=60)
        m.evaluate(-0.021, quality_ok=True, state_key=1, now_ns=T0)
        # inside the threshold but not beyond the clear line (−1.6%): stays ACTIVE
        assert m.evaluate(-0.018, quality_ok=True, state_key=2, now_ns=T0 + MS) == []
        assert m.evaluate(-0.015, quality_ok=True, state_key=3, now_ns=T0 + 2 * MS) == [Tr.PROGRESS]
        assert m.st.clear_since_ns == T0 + 2 * MS
        # back above the clear line resets the hold timer
        m.evaluate(-0.017, quality_ok=True, state_key=4, now_ns=T0 + 3 * MS)
        assert m.st.clear_since_ns is None
        m.evaluate(-0.01, quality_ok=True, state_key=5, now_ns=T0 + 4 * MS)
        assert m.reevaluate(T0 + 3003 * MS, quality_ok=True) == []
        assert m.reevaluate(T0 + 3004 * MS, quality_ok=True) == [Tr.CLEARED]
        assert m.st.cooldown_until_ns == T0 + 3004 * MS + 60_000 * MS
        # cooldown: the same level does not re-trigger ...
        assert m.evaluate(-0.025, quality_ok=True, state_key=6, now_ns=T0 + 4000 * MS) == []
        # ... unless a more severe level of the same rule is breached (escalation)
        assert m.evaluate(-0.031, quality_ok=True, state_key=7, now_ns=T0 + 5000 * MS, escalating=True) == [Tr.ACTIVATED]
        assert m.st.episode == 2

    def test_latch_reset_rearm_new_episode(self) -> None:
        m = machine()
        m.evaluate(-0.031, quality_ok=True, state_key=1, now_ns=T0)
        m.evaluate(-0.035, quality_ok=True, state_key=2, now_ns=T0 + MS)
        assert m.state == RuleSmState.ACTIVE and m.st.latched
        assert m.evaluate(-0.001, quality_ok=True, state_key=3, now_ns=T0 + 10_000 * MS) == []  # 锁存不随恢复解除
        m.evaluate(-0.035, quality_ok=True, state_key=4, now_ns=T0 + 10_001 * MS)
        assert m.reset(T0 + 10_002 * MS)
        assert m.state == RuleSmState.REARMED and m.st.rearm_threshold == pytest.approx(-0.045)
        assert not m.reset(T0)  # nothing latched any more
        assert m.evaluate(-0.04, quality_ok=True, state_key=5, now_ns=T0 + 11_000 * MS) == []
        assert m.evaluate(-0.046, quality_ok=True, state_key=6, now_ns=T0 + 12_000 * MS) == [Tr.PENDING]
        assert m.evaluate(-0.044, quality_ok=True, state_key=7, now_ns=T0 + 12_001 * MS) == [Tr.REVOKED]
        assert m.state == RuleSmState.REARMED  # 撤销后回到重新布防状态
        m.evaluate(-0.046, quality_ok=True, state_key=8, now_ns=T0 + 13_000 * MS)
        assert m.evaluate(-0.047, quality_ok=True, state_key=9, now_ns=T0 + 13_001 * MS) == [Tr.ACTIVATED]
        assert m.st.episode == 2 and m.st.latched and m.alert_key("20260929") == "20260929|daily_loss|halt|acct:ACC3|2"

    def test_rearmed_clears_after_hold(self) -> None:
        m = machine(hold_ms=1000)
        m.evaluate(-0.031, quality_ok=True, state_key=1, now_ns=T0)
        m.evaluate(-0.032, quality_ok=True, state_key=2, now_ns=T0 + MS)
        m.reset(T0 + 2 * MS)
        m.evaluate(-0.01, quality_ok=True, state_key=3, now_ns=T0 + 3 * MS)
        assert m.reevaluate(T0 + 1003 * MS, quality_ok=True) == [Tr.REARM_CLEARED]
        assert m.state == RuleSmState.OK

    def test_quality_gating(self) -> None:
        m = machine()
        assert m.evaluate(-0.05, quality_ok=False, state_key=1, now_ns=T0) == []
        assert m.state == RuleSmState.OK
        m.evaluate(-0.031, quality_ok=True, state_key=2, now_ns=T0)
        assert m.reevaluate(T0 + 5000 * MS, quality_ok=False) == []
        assert m.state == RuleSmState.PENDING

    def test_quality_gating_resets_the_hold_timer(self) -> None:
        """L1: the clear hold (hold_ms) never spans a period of untrusted market data."""
        m = machine("warn", hold_ms=3000)
        assert m.evaluate(-0.021, quality_ok=True, state_key=1, now_ns=T0) == [Tr.ACTIVATED]
        assert m.evaluate(-0.01, quality_ok=True, state_key=2, now_ns=T0 + MS) == [Tr.PROGRESS]
        assert m.st.clear_since_ns == T0 + MS
        assert m.evaluate(-0.01, quality_ok=False, state_key=3, now_ns=T0 + 2 * MS) == [Tr.PROGRESS]  # persisted
        assert m.st.clear_since_ns is None and m.state == RuleSmState.ACTIVE
        assert m.reevaluate(T0 + 10_000 * MS, quality_ok=False) == []
        assert m.evaluate(-0.01, quality_ok=True, state_key=4, now_ns=T0 + 10_001 * MS) == [Tr.PROGRESS]
        assert m.reevaluate(T0 + 13_000 * MS, quality_ok=True) == []  # the hold restarted when quality came back
        assert m.reevaluate(T0 + 13_001 * MS, quality_ok=True) == [Tr.CLEARED]

    def test_cooldown_only_for_alert_levels(self) -> None:
        """M2: RESTRICT / CIRCUIT_BREAK / LIQUIDATE re-activate on a re-breach inside the cooldown."""
        for action, params in (("RESTRICT", {"no_add_to_losers": True}), ("LIQUIDATE", {}), ("CIRCUIT_BREAK", {})):
            lv = {"name": "x", "threshold": -0.025, "action": action, "params": params, "confirm": {"states": 1}}
            m = machine("x", hold_ms=0, cooldown_s=60, levels=[lv])
            assert m.evaluate(-0.026, quality_ok=True, state_key=1, now_ns=T0) == [Tr.ACTIVATED]
            assert m.evaluate(-0.001, quality_ok=True, state_key=2, now_ns=T0 + MS) == [Tr.CLEARED]
            assert m.st.cooldown_until_ns is None and m.due_ns() is None
            assert m.evaluate(-0.026, quality_ok=True, state_key=3, now_ns=T0 + 2 * MS) == [Tr.ACTIVATED]
            assert m.st.episode == 2
        alert = machine("warn", hold_ms=0, cooldown_s=60)
        alert.evaluate(-0.021, quality_ok=True, state_key=1, now_ns=T0)
        alert.evaluate(-0.001, quality_ok=True, state_key=2, now_ns=T0 + MS)
        assert alert.evaluate(-0.021, quality_ok=True, state_key=3, now_ns=T0 + 2 * MS) == []  # ALERT still cools down

    def test_none_value_is_never_a_breach(self) -> None:
        m = machine("warn")
        assert m.evaluate(None, quality_ok=True, state_key=1, now_ns=T0) == []

    def test_day_roll(self) -> None:
        m = machine()
        m.evaluate(-0.031, quality_ok=True, state_key=1, now_ns=T0)
        m.evaluate(-0.032, quality_ok=True, state_key=2, now_ns=T0 + MS)
        assert m.day_roll(T0 + 10 * MS) is True
        assert m.state == RuleSmState.OK and m.st.episode == 0 and not m.st.latched
        keep = machine(reset_on_day_roll=False)
        keep.evaluate(-0.031, quality_ok=True, state_key=1, now_ns=T0)
        keep.evaluate(-0.032, quality_ok=True, state_key=2, now_ns=T0 + MS)
        assert keep.day_roll(T0 + 10 * MS) is False
        assert keep.state == RuleSmState.ACTIVE and keep.st.latched and keep.st.episode == 1

    def test_row_round_trip(self) -> None:
        m = machine()
        m.evaluate(-0.031, quality_ok=True, state_key=1, now_ns=T0)
        row = m.to_row("20260929", "h", T0)
        st = state_from_row(row)
        assert st.persisted() == m.st.persisted() and st.last_value == pytest.approx(-0.031)
        assert st.last_state_key is None
        assert SmState().persisted()[0] == RuleSmState.OK


# ---------------------------------------------------------------------------- engine level
P0 = {"rb2701": 3100.0, "hc2701": 3300.0, "i2701": 800.0}
P_HALT = {"rb2701": 2970.0, "hc2701": 3170.0, "i2701": 767.0}  # about -3.1 % on the ACC3 book below
P_RESTRICT = {"rb2701": 2990.0, "hc2701": 3190.0, "i2701": 772.0}  # about -2.6 %
P_OK = {"rb2701": 3080.0, "hc2701": 3280.0, "i2701": 796.0}  # about -0.4 %


def acc3_book(*rule_ids: str) -> Harness:
    h = Harness(rules_only(*(rule_ids or ("daily_loss",))))
    h.ticks(P0)
    h.open_position("ACC3", "rb2701", B, 80, 3100.0)
    h.open_position("ACC3", "hc2701", B, 75, 3300.0)
    h.open_position("ACC3", "i2701", B, 32, 800.0)
    return h


class TestEngineRuleSemantics:
    def test_facts_do_not_confirm_a_pending_breaker(self) -> None:
        """M1: fills and cash movements are not new LIVE market states (confirm.states)."""
        h = acc3_book()
        halt = lambda: h.eng.account("ACC3").machines[("daily_loss", "halt", "acct:ACC3")]  # noqa: E731
        h.ticks(P_HALT)
        assert halt().state == RuleSmState.PENDING
        h.advance(100)
        h.cmd("cash.move", CashMoveRequest(movement_id="m1", account_id="ACC3", type=CashType.DEPOSIT, amount="1.00",
                                           reason="r", actor="a", role=ADMIN, requested_ns=1))
        h.evaluate()
        t = h.trade("ACC3", "rb2701", S, CT, 2970.0, 1, origin=TradeOrigin.EXTERNAL)
        h.run(h.eng.apply_exec(t))
        h.evaluate()
        assert halt().state == RuleSmState.PENDING and not h.eng.account("ACC3").halted_latched
        h.ticks({"rb2701": 2969.0})  # a second distinct LIVE market state confirms
        assert halt().state == RuleSmState.ACTIVE and h.eng.account("ACC3").halted_latched

    def test_restrict_reactivates_inside_the_cooldown(self) -> None:
        """M2 / p23: a RESTRICT level re-breached during the cooldown restricts again (new episode, one event)."""
        h = acc3_book()
        ra = h.eng.account("ACC3")
        h.ticks(P_RESTRICT)
        assert ra.state == AccountState.RESTRICTED
        h.advance(100)
        h.ticks(P_OK)
        h.advance(3100)
        h.timer()
        assert ra.state == AccountState.NORMAL and not ra.restrictions
        h.advance(5000)
        h.ticks(P_RESTRICT)
        assert ra.state == AccountState.RESTRICTED
        assert sorted(r.kind.value for r in ra.restrictions) == ["NO_ADD_TO_LOSERS", "ORDER_SIZE_FACTOR"]
        raised = [e for e in h.events(RiskEventKind.ALERT_RAISED) if e.level == "restrict"]
        assert [e.episode for e in raised] == [1, 2]
        assert h.submit("ACC3", "hc2701", B, Offset.OPEN, 1, 3190.0)[0].code == "REJECT_ADD_TO_LOSER"

    def test_nav_floor_clears_on_its_configured_line(self) -> None:
        """M8: NAV warn (0.90) clears above 0.90 × (1 + 0.02) = 0.918, not at the 0.2-default 1.08."""
        lot = SodLot(instrument_id="rb2701", direction=PosDirection.LONG, lot_no=1, open_td="20260928", open_ts_ns=1,
                     open_price="3100", qty=1000, basis_price="3100")
        sod = sod_snapshot(lots={"ACC3": [lot]})
        sod.accounts[0].pre_balance = "8900000.00"
        sod.accounts[0].hwm_equity = "8900000.00"
        h = Harness(rules_only("nav_floor"), sod=sod)
        h.ticks({"rb2701": 3100.0})
        ra = h.eng.account("ACC3")
        assert ra.state == AccountState.WARNING
        h.ticks({"rb2701": 3140.0})  # NAV 0.93 > 0.918
        h.advance(3001)
        h.timer()
        assert ra.state == AccountState.NORMAL
        assert [e.level for e in h.events(RiskEventKind.ALERT_CLEARED)] == ["warn"]


class TestMinGrossPct:
    """``min_gross_pct`` + time-only confirmation of the default concentration rule (ACC3: warn N < 2, restrict < 1.5)."""

    def test_small_book_is_never_evaluated(self) -> None:
        h = Harness(rules_only("concentration"))
        h.ticks(P0)
        h.open_position("ACC3", "rb2701", B, 100, 3100.0)  # N = 1 but gross 31 % < 50 %
        h.advance(60_000)
        h.timer()
        ra = h.eng.account("ACC3")
        assert not ra.machines and ra.state == AccountState.NORMAL
        assert not h.events(RiskEventKind.ALERT_RAISED)
        st = [r for r in h.eng.snapshot("ACC3").rules if r.rule_id == "concentration"]
        assert st and st[0].value is None  # not applicable below min_gross_pct

    def test_build_transient_raises_nothing(self) -> None:
        h = Harness(rules_only("concentration"))
        h.ticks(P0)
        h.open_position("ACC3", "rb2701", B, 170, 3100.0)  # N = 1 at gross 52.7 %
        ra = h.eng.account("ACC3")
        assert {k[1]: m.state for k, m in ra.machines.items()} == {"warn": RuleSmState.PENDING,
                                                                     "restrict": RuleSmState.PENDING}
        assert ra.state == AccountState.NORMAL and not ra.restrictions  # PENDING restrict is not in force
        h.advance(5000)
        h.open_position("ACC3", "hc2701", B, 100, 3300.0)
        h.open_position("ACC3", "i2701", B, 60, 800.0)  # N ≈ 2.9 before the 30 s confirmation
        h.advance(40_000)
        h.timer()
        assert not ra.machines and ra.state == AccountState.NORMAL
        assert not h.events(RiskEventKind.ALERT_RAISED)

    def test_persistent_concentration_activates_after_30s(self) -> None:
        h = Harness(rules_only("concentration"))
        h.ticks(P0)
        h.open_position("ACC3", "rb2701", B, 170, 3100.0)
        h.advance(29_000)
        h.ticks({"rb2701": 3101.0})
        h.timer()
        assert not h.events(RiskEventKind.ALERT_RAISED)
        h.advance(1000)
        h.timer()
        assert sorted(e.level for e in h.events(RiskEventKind.ALERT_RAISED)) == ["restrict", "warn"]
        ra = h.eng.account("ACC3")
        assert ra.state == AccountState.RESTRICTED
        assert [r.scope_key for r in ra.restrictions] == ["acct:ACC3/prod:rb"]
        # the book shrinks below min_gross_pct: not applicable = not breached, clears through the normal path
        t = h.trade("ACC3", "rb2701", S, CT, 3101.0, 100, origin=TradeOrigin.EXTERNAL)
        h.run(h.eng.apply_exec(t))
        h.evaluate()
        h.advance(3001)
        h.timer()
        assert ra.state == AccountState.NORMAL
        assert sorted(e.level for e in h.events(RiskEventKind.ALERT_CLEARED)) == ["restrict", "warn"]

    def test_quality_freeze_is_not_the_gate(self) -> None:
        """Below min_gross_pct is a normal observation; STALE data still freezes the machine."""
        h = Harness(rules_only("concentration"))
        h.ticks(P0)
        h.open_position("ACC3", "rb2701", B, 170, 3100.0)
        h.md_status({"rb2701": MdQuality.STALE})
        h.advance(60_000)
        h.timer()
        m = h.eng.account("ACC3").machines[("concentration", "warn", "acct:ACC3")]
        assert m.state == RuleSmState.PENDING  # frozen, neither confirmed nor revoked


def test_request_hash_is_the_one_system_algorithm() -> None:
    """L8: the core, futrisk.db and futrisk.messages share one request_hash (top-level time fields only)."""
    from futrisk.db import request_hash as db_hash
    from futrisk.db.processed import request_hash as processed_hash
    from futrisk.messages.base import request_hash as base_hash

    assert db_hash is base_hash and processed_hash is base_hash
    a = CashMovement(movement_id="m", account_id="A", trading_day="20260929", type=CashType.DEPOSIT, amount="1.00",
                     actor="a", reason="r", ts_ns=1)
    b = CashMovement(movement_id="m", account_id="A", trading_day="20260929", type=CashType.DEPOSIT, amount="1.00",
                     actor="a", reason="r", ts_ns=2)
    assert request_hash(a) == base_hash(a) == request_hash(b)  # top-level ts_ns ignored
    assert base_hash({"x": 1, "inner": {"ts_ns": 5}}) != base_hash({"x": 1, "inner": {"ts_ns": 9}})  # nested counts
