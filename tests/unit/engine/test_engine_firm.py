"""Firm controls on every shard and the coordinator's aggregation (01 §5.3.1; 02 §7.6 FirmSnapshot / FirmControl)."""

from __future__ import annotations

import pytest
from enginetest_support import ADMIN, CY, RM, TD, B, Harness, O, S, rules_only, rules_with, sod_snapshot

from futrisk.common.config.risk_rules import RuleDef
from futrisk.core.effects import Persist
from futrisk.messages import codes
from futrisk.messages.enums import (
    AccountState,
    ControlOrigin,
    FirmAction,
    PosDirection,
    RestrictionKind,
    RiskEventKind,
    Role,
)
from futrisk.messages.eod import SodLot
from futrisk.messages.risk import FirmControl, FirmSnapshot, KillSwitchRequest, Restriction

P = {"rb2701": 3100.0, "hc2701": 3300.0, "i2701": 800.0}


def rule_ctl(on: bool) -> FirmControl:
    return FirmControl(action=FirmAction.KILL_SWITCH_ON if on else FirmAction.KILL_SWITCH_OFF, origin=ControlOrigin.RULE,
                       active=on, trading_day=TD, reason="firm rule", cause_alert_key=f"{TD}|firm_loss|kill|firm|1", ts_ns=1)


def kill(h: Harness, cid: str, on: bool) -> None:
    rep, _ = h.cmd("firm.kill_switch", KillSwitchRequest(command_id=cid, on=on, reason="ops", actor="rm", role=RM))
    assert rep.ok


class TestKillSwitchOrSemantics:
    def test_manual_and_rule_flags_are_separate(self) -> None:
        h = Harness(rules_only("daily_loss"))
        h.ticks(P)
        kill(h, "k1", True)
        eng = h.eng
        assert eng.firm.kill_switch and eng.firm.manual_kill and not eng.firm.rule_kill
        assert h.submit("ACC3", "rb2701", B, O, 1)[0].code == codes.REJECT_FIRM_KILL_SWITCH
        ra = eng.account("ACC3")
        assert ra.state == AccountState.RESTRICTED and ra.cause == "KILL_SWITCH"
        row = h.db.tables["firm_controls"][-1]
        assert (row["origin"], row["action"], row["active"], row["src_instance"]) == ("MANUAL", "KILL_SWITCH_ON", True, "api-gateway")
        assert h.events(RiskEventKind.KILL_SWITCH_ON)[-1].detail["effective"] is True
        h.run(eng.apply_firm_control(rule_ctl(True)))
        kill(h, "k2", False)  # 人工关闭关不掉规则开启的 kill switch
        assert eng.firm.kill_switch and not eng.firm.manual_kill and eng.firm.rule_kill
        assert h.submit("ACC3", "rb2701", B, O, 1)[0].code == codes.REJECT_FIRM_KILL_SWITCH
        kill(h, "k3", True)
        h.run(eng.apply_firm_control(rule_ctl(False)))  # 规则关闭关不掉人工开启的
        assert eng.firm.kill_switch
        kill(h, "k4", False)
        assert not eng.firm.kill_switch
        assert h.submit("ACC3", "rb2701", B, O, 1)[0].ok
        assert eng.account("ACC3").state == AccountState.NORMAL
        assert [r["origin"] for r in h.db.tables["firm_controls"]] == ["MANUAL", "RULE", "MANUAL", "MANUAL", "RULE", "MANUAL"]

    def test_kill_switch_command_checks_role_and_is_idempotent(self) -> None:
        h = Harness(rules_only("daily_loss"))
        rep, _ = h.cmd("firm.kill_switch", KillSwitchRequest(command_id="k", on=True, reason="x", actor="v", role=Role.VIEWER))
        assert rep.code == codes.FORBIDDEN and not h.eng.firm.kill_switch
        req = KillSwitchRequest(command_id="k2", on=True, reason="x", actor="a", role=ADMIN)
        h.cmd("firm.kill_switch", req)
        rep2, effs = h.cmd("firm.kill_switch", req)
        assert rep2.duplicate and not [e for e in effs if isinstance(e, Persist)]

    def test_firm_restrictions_apply_to_every_account(self) -> None:
        h = Harness(rules_only("daily_loss"))
        h.ticks(P)
        ctl = FirmControl(action=FirmAction.SET_RESTRICTIONS, origin=ControlOrigin.RULE, trading_day=TD, ts_ns=1,
                          restrictions=[Restriction(kind=RestrictionKind.BLOCK_OPEN, scope_key="firm/sector:black",
                                                    source="firm_black/block")])
        h.run(h.eng.apply_firm_control(ctl))
        for acct in ("ACC3", "ACC4", "ACC5"):
            assert any(r.scope_key == "firm/sector:black" for r in h.eng.account(acct).restrictions)
        assert h.submit("ACC4", "hc2701", B, O, 1)[0].code == codes.REJECT_PRODUCT_BLOCKED
        assert h.submit("ACC4", "IF2612", B, O, 1, 4500.0)[0].code != codes.REJECT_PRODUCT_BLOCKED
        cleared = FirmControl(action=FirmAction.SET_RESTRICTIONS, origin=ControlOrigin.RULE, trading_day=TD, ts_ns=2)
        h.run(h.eng.apply_firm_control(cleared))
        assert h.submit("ACC4", "hc2701", B, O, 1)[0].ok


class TestCoordinator:
    def _shard1(self) -> Harness:
        lots = {"ACC3": [SodLot(instrument_id="rb2701", direction=PosDirection.LONG, lot_no=1, open_td="20260928",
                                open_ts_ns=1, open_price="3100", qty=300, basis_price="3100")]}
        h1 = Harness(rules_only("daily_loss"), sod=sod_snapshot(lots=lots))
        h1.ticks(P)
        return h1

    def test_firm_snapshot_aggregation(self) -> None:
        h1 = self._shard1()
        h0 = Harness(rules_only("daily_loss"), shard=0, coordinator=True,
                     sod=sod_snapshot({"ACC1": "10000000", "ACC2": "20000000"}))
        h0.ticks(P)
        h0.eng.ingest_snapshot(h1.eng.snapshot("ACC3"))
        h0.eng.ingest_snapshot(h1.eng.snapshot("ACC4"))
        effs = h0.timer()
        firm = [p.payload for p in h0.publishes(effs) if p.topic == "risk.firm"]
        assert len(firm) == 1
        fs = firm[0]
        assert isinstance(fs, FirmSnapshot)
        assert fs.equity == pytest.approx(10e6 + 20e6 + 10e6 + 10e6)
        assert fs.accounts_by_state["NORMAL"] == 4 and fs.kill_switch is False
        rb = [p for p in fs.product_net if p.product == "rb"][0]
        assert (rb.net_lots, rb.accounts) == (300, 1) and rb.net_notional == pytest.approx(300 * 3100 * 10)
        assert [s.shard for s in fs.shards] == [0, 1]
        assert fs.risk_degree == pytest.approx(fs.margin / fs.equity)

    def test_firm_rule_switches_the_rule_kill_switch_and_publishes_controls(self) -> None:
        firm_loss = RuleDef.model_validate({"id": "firm_loss", "metric": "firm.daily_pnl_pct_init", "op": "<=", "scope": "firm",
                                            "levels": [{"name": "kill", "threshold": -0.005, "action": "CIRCUIT_BREAK",
                                                        "confirm": {"states": 1, "ms": 0}}]})
        firm_rb = RuleDef.model_validate({"id": "firm_rb", "metric": "product.net_pct_equity", "op": ">=", "scope": "firm/product",
                                          "levels": [{"name": "block", "threshold": 0.01, "action": "RESTRICT",
                                                      "params": {"block_open": True}}]})
        h1 = self._shard1()
        h0 = Harness(rules_with([*rules_only("daily_loss").rules, firm_loss, firm_rb]), shard=0, coordinator=True,
                     sod=sod_snapshot({"ACC1": "10000000", "ACC2": "20000000"}))
        h0.ticks(P)
        h1.ticks({"rb2701": 3000.0})  # ACC3 亏损 300,000 → 公司层 −0.6%
        h0.eng.ingest_snapshot(h1.eng.snapshot("ACC3"))
        effs = h0.timer()
        controls = [m.payload for p in effs if isinstance(p, Persist) for m in p.outbox if m.topic == "risk.control.firm"]
        # the whole RULE layer in ONE fact: kill switch + every firm rule restriction in force
        assert [c.action for c in controls] == [FirmAction.RULE_STATE]  # type: ignore[attr-defined]
        kill_ctl = controls[0]
        assert isinstance(kill_ctl, FirmControl) and kill_ctl.active
        assert [r.scope_key for r in kill_ctl.restrictions] == ["firm/prod:rb"]
        assert kill_ctl.origin == ControlOrigin.RULE and kill_ctl.cause_alert_key == f"{TD}|firm_loss|kill|firm|1"
        assert h0.eng.firm.rule_kill and h0.eng.firm.kill_switch
        firm_alerts = [e for e in h0.events(RiskEventKind.ALERT_RAISED, effs) if e.scope_key == "firm"]
        assert firm_alerts and firm_alerts[0].account_id is None
        assert h0.submit("ACC1", "hc2701", B, O, 1)[0].code == codes.REJECT_FIRM_KILL_SWITCH
        # 其他分片应用同一个控制：规则标志与人工标志分开保存
        h1.run(h1.eng.apply_firm_control(kill_ctl))  # type: ignore[arg-type]
        assert h1.eng.firm.rule_kill and not h1.eng.firm.manual_kill
        assert h1.submit("ACC4", "hc2701", B, O, 1)[0].code == codes.REJECT_FIRM_KILL_SWITCH
        assert h1.submit("ACC3", "rb2701", S, CY, 1, 3000.0)[0].ok
        row = h1.db.tables["firm_controls"][-1]
        assert (row["origin"], row["src_instance"]) == ("RULE", "risk-engine-0")


FIRM_LOSS = RuleDef.model_validate({"id": "firm_loss", "metric": "firm.daily_pnl_pct_init", "op": "<=", "scope": "firm",
                                    "levels": [{"name": "kill", "threshold": -0.005, "action": "CIRCUIT_BREAK",
                                                "confirm": {"states": 1, "ms": 0}}]})


def coordinator(*extra: RuleDef) -> Harness:
    return Harness(rules_with([*rules_only("daily_loss").rules, *extra]), shard=0, coordinator=True,
                   sod=sod_snapshot({"ACC1": "10000000", "ACC2": "20000000"}))


class TestFirmFreshness:
    def test_quiet_accounts_are_kept_alive_and_losses_still_trip(self) -> None:
        """H1 / p05: unchanged accounts are re-published every second, so firm rules are not frozen."""
        h0 = coordinator(FIRM_LOSS)
        h0.ticks(P)
        h0.eng.note_shard_alive(1, h0.clock.now_ns())
        h0.open_position("ACC1", "rb2701", B, 300, 3100.0)
        h0.timer()
        acc2 = 0
        for _ in range(12):
            h0.advance(1000)
            acc2 += sum(1 for p in h0.publishes(h0.timer()) if p.topic == "risk.snapshot.ACC2")
            h0.eng.note_shard_alive(1, h0.clock.now_ns())
        assert acc2 >= 10  # keep-alive of a flat, quiet account
        agg = h0.eng.aggregator
        assert agg is not None and agg.valuation is not None and agg.valuation.quality.value == "OK"
        h0.tick("rb2701", 2900.0)  # −600,000 on a 30M firm: −2 %
        h0.advance(1000)
        h0.timer()
        assert h0.eng.firm.rule_kill

    def test_shard_freshness_and_never_reported_shards(self) -> None:
        h1 = Harness(rules_only("daily_loss"))
        h1.ticks(P)
        h1.open_position("ACC3", "rb2701", B, 10, 3100.0)
        h0 = coordinator()
        h0.ticks(P)
        fs = h0.eng.firm_snapshot()
        assert {s.shard: s.quality.value for s in fs.shards} == {0: "STALE", 1: "STALE"}  # nothing published yet
        h0.timer()
        fs = h0.eng.firm_snapshot()
        assert {s.shard: s.quality.value for s in fs.shards} == {0: "OK", 1: "STALE"}  # shard 1 never reported
        assert [s.as_of_ns for s in fs.shards if s.shard == 1] == [0]
        h0.eng.ingest_snapshot(h1.eng.snapshot("ACC3"))
        assert {s.shard: s.quality.value for s in h0.eng.firm_snapshot().shards}[1] == "OK"
        h0.advance(6000)
        h1.advance(6000)
        h0.timer()
        fs = h0.eng.firm_snapshot()
        assert {s.shard: s.quality.value for s in fs.shards}[1] == "STALE"
        agg = h0.eng.aggregator
        assert agg is not None and agg.valuation is not None and agg.valuation.quality.value == "STALE"  # ACC3 holds rb
        h0.eng.note_shard_alive(1, h0.clock.now_ns())  # a heartbeat of shard 1 is enough
        h0.eng.firm_snapshot()
        assert agg.valuation.quality.value == "OK"

    def test_stale_shard_of_flat_accounts_does_not_downgrade(self) -> None:
        h1 = Harness(rules_only("daily_loss"))
        h1.ticks(P)
        h0 = coordinator()
        h0.ticks(P)
        h0.eng.ingest_snapshot(h1.eng.snapshot("ACC4"))  # flat account
        h0.advance(6000)
        h0.timer()
        fs = h0.eng.firm_snapshot()
        agg = h0.eng.aggregator
        assert agg is not None and agg.valuation is not None
        assert {s.shard: s.quality.value for s in fs.shards}[1] == "STALE" and agg.valuation.quality.value == "OK"

    def test_firm_peak_is_tracked_from_firm_equity(self) -> None:
        """Offsetting accounts: the sum of account peaks would show a false firm drawdown."""
        h1 = Harness(rules_only("daily_loss"))
        h1.ticks(P)
        h1.open_position("ACC3", "rb2701", S, 100, 3100.0)
        h0 = coordinator()
        h0.ticks(P)
        h0.open_position("ACC1", "rb2701", B, 100, 3100.0)
        for px in (3150.0, 3050.0, 3150.0, 3100.0):
            h0.tick("rb2701", px)
            h1.tick("rb2701", px)
            h0.eng.ingest_snapshot(h1.eng.snapshot("ACC3"))
            h0.timer()
            h0.advance(1000)
            h1.advance(1000)
        agg = h0.eng.aggregator
        assert agg is not None and agg.valuation is not None
        m = agg.valuation.metrics
        assert m.intraday_dd < 1e-4  # firm perf equity never fell (only the opening fees, ~600 on 40M)
        acc_peaks = sum(h.eng.snapshot(a).metrics.intraday_peak_equity for h, a in ((h0, "ACC1"), (h0, "ACC2"), (h1, "ACC3")))
        assert m.intraday_peak_equity == pytest.approx(40_000_000.0)  # the firm's own SOD performance equity
        assert acc_peaks > 40_090_000.0  # the old sum of the accounts' peaks: a false 0.2 % firm drawdown


class TestRuleState:
    """H2: the coordinator's RULE layer travels as ONE RULE_STATE fact, so the newest fact alone restores it."""

    FIRM_TWO = RuleDef.model_validate({"id": "firm_loss", "metric": "firm.daily_pnl_pct_init", "op": "<=", "scope": "firm",
                                       "levels": [{"name": "restrict", "threshold": -0.002, "action": "RESTRICT",
                                                   "params": {"max_order_factor": 0.5}, "confirm": {"states": 1}},
                                                  {"name": "kill", "threshold": -0.003, "action": "CIRCUIT_BREAK",
                                                   "confirm": {"states": 1, "ms": 0}}]})

    def _crossed(self) -> tuple[Harness, Harness, list[FirmControl]]:
        h1 = Harness(rules_only("daily_loss"))
        h1.ticks(P)
        h1.open_position("ACC3", "rb2701", B, 300, 3100.0)
        h0 = coordinator(self.FIRM_TWO)
        h0.ticks(P)
        h1.ticks({"rb2701": 3060.0})  # ACC3 -120,000 on a 50M firm: -0.24 %... plus fees
        h1.ticks({"rb2701": 3040.0})  # -180,000: restrict (-0.2 %) and kill (-0.3 %) crossed in one firm tick
        h0.eng.ingest_snapshot(h1.eng.snapshot("ACC3"))
        effs = h0.timer()
        controls = [m.payload for p in effs if isinstance(p, Persist) for m in p.outbox if m.topic == "risk.control.firm"]
        return h0, h1, controls  # type: ignore[return-value]

    def test_one_fact_carries_the_kill_switch_and_the_restrictions(self) -> None:
        h0, _h1, controls = self._crossed()
        assert len(controls) == 1
        c = controls[0]
        assert (c.action, c.origin, c.active) == (FirmAction.RULE_STATE, ControlOrigin.RULE, True)
        assert [(r.kind, r.scope_key, r.value) for r in c.restrictions] == [(RestrictionKind.ORDER_SIZE_FACTOR, "firm", 0.5)]
        row = h0.db.tables["firm_controls"][-1]
        assert (row["action"], row["origin"], row["active"]) == ("RULE_STATE", "RULE", True) and row["restrictions"]

    def test_a_shard_rebuilds_the_rule_layer_from_the_newest_fact_only(self) -> None:
        """P7: the ctl.snapshot fallback delivers only the newest fact; it must be enough."""
        h0, _h1, controls = self._crossed()
        shard = Harness(rules_only("daily_loss"))  # never saw anything before
        shard.ticks(P)
        effs = shard.run(shard.eng.apply_firm_control(controls[-1]))
        assert shard.eng.firm.rule_kill and not shard.eng.firm.manual_kill
        assert [r.scope_key for r in shard.eng.firm.restrictions] == ["firm"]
        assert shard.submit("ACC3", "rb2701", B, O, 1, 3100.0)[0].code == codes.REJECT_FIRM_KILL_SWITCH
        assert len(shard.events(RiskEventKind.KILL_SWITCH_ON, effs)) == 1
        again = shard.run(shard.eng.apply_firm_control(controls[-1]))  # the same state again: no new event
        assert not shard.events(RiskEventKind.KILL_SWITCH_ON, again)
        # the firm recovers: one RULE_STATE lifts the kill switch AND every rule restriction
        off = FirmControl(action=FirmAction.RULE_STATE, origin=ControlOrigin.RULE, active=False, restrictions=[],
                          trading_day=TD, reason="firm rules", ts_ns=2)
        effs = shard.run(shard.eng.apply_firm_control(off))
        assert not shard.eng.firm.kill_switch and not shard.eng.firm.restrictions
        assert len(shard.events(RiskEventKind.KILL_SWITCH_OFF, effs)) == 1
        assert shard.submit("ACC3", "rb2701", B, O, 1, 3100.0)[0].ok
        # a restart replays the firm_controls rows (RULE_STATE included)
        b = Harness(shard.rules, load=False)
        b.clock.set(shard.clock.now_ns())
        from futrisk.core.risk_account import Lifecycle

        b.eng.restore(shard.db.snapshot(), lifecycle=Lifecycle.RUNNING)
        assert not b.eng.firm.kill_switch and not b.eng.firm.restrictions

    def test_manual_kill_switch_is_independent_of_rule_state(self) -> None:
        h = Harness(rules_only("daily_loss"))
        h.ticks(P)
        kill(h, "m1", True)
        on = FirmControl(action=FirmAction.RULE_STATE, origin=ControlOrigin.RULE, active=True, trading_day=TD, ts_ns=1)
        off = FirmControl(action=FirmAction.RULE_STATE, origin=ControlOrigin.RULE, active=False, trading_day=TD, ts_ns=2)
        h.run(h.eng.apply_firm_control(on))
        h.run(h.eng.apply_firm_control(off))
        assert h.eng.firm.kill_switch and h.eng.firm.manual_kill  # the rule layer never turns MANUAL off
        _, effs = h.cmd("firm.kill_switch", KillSwitchRequest(command_id="m2", on=True, reason="again", actor="rm", role=RM))
        assert not h.events(RiskEventKind.KILL_SWITCH_ON, effs)  # no change, no event
        rep, _ = h.cmd("firm.kill_switch", KillSwitchRequest(command_id="m3", on=False, reason="done", actor="rm", role=RM))
        assert rep.open_as(FirmControl).restrictions == [] and not h.eng.firm.kill_switch
