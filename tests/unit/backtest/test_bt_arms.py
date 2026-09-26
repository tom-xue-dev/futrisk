"""Experiment arms: rule sets per tier, block-open behaviour after a halt, determinism of the outputs."""

from __future__ import annotations

from pathlib import Path

import pytest
from btsupport import DaySpec, base, scripted_replay

from futrisk.backtest.arms import arm_rules, reduce_fraction
from futrisk.backtest.config import ArmSpec, BacktestConfig, load_backtest_config
from futrisk.backtest.runner import run_config
from futrisk.messages import codes
from futrisk.messages.enums import Action


def _cfg() -> BacktestConfig:
    return BacktestConfig(name="t")


def test_rule_sets_per_tier() -> None:
    rules = base().risk_rules
    cfg = _cfg()
    assert arm_rules(rules, cfg, ArmSpec(name="n", tier="none")).rules == []
    alert = arm_rules(rules, cfg, ArmSpec(name="a", tier="alert_only")).rules
    assert [r.id for r in alert] == ["daily_loss"]
    assert [lv.action for lv in alert[0].levels] == [Action.ALERT] * 4
    assert [lv.threshold for lv in alert[0].levels] == pytest.approx([-0.02, -0.025, -0.03, -0.05])
    assert alert[0].levels[2].confirm.states == 2 and alert[0].levels[2].confirm.ms == 1000
    block = arm_rules(rules, cfg, ArmSpec(name="b", tier="block_open", threshold=0.015)).rules[0]
    assert [lv.action for lv in block.levels] == [Action.ALERT, Action.RESTRICT, Action.CIRCUIT_BREAK, Action.LIQUIDATE]
    assert [lv.threshold for lv in block.levels] == pytest.approx([-0.01, -0.0125, -0.015, -0.025])
    assert block.levels[2].latch is True
    prod = arm_rules(rules, cfg, ArmSpec(name="p", tier="production"))
    assert len(prod.rules) == len(rules.rules)
    sysc = arm_rules(rules, cfg, ArmSpec(name="b", tier="block_open")).system
    # the engine's periodic timers are off (0): time is driven by EngineCore.next_timer_ns alone
    assert sysc.snapshot_hz == 0 and sysc.metric_sample_s == 0 and sysc.recon_interval_s == 0
    assert reduce_fraction(cfg, ArmSpec(name="h", tier="halve")) == 0.5
    assert reduce_fraction(cfg, ArmSpec(name="f", tier="flatten")) == 1.0
    assert reduce_fraction(cfg, ArmSpec(name="b", tier="block_open")) == 0.0


def test_halt_only_keeps_the_breaker_and_turns_restrict_into_an_alert() -> None:
    rules = base().risk_rules
    cfg = _cfg()
    ho = arm_rules(rules, cfg, ArmSpec(name="h", tier="halt_only")).rules[0]
    assert [lv.name for lv in ho.levels] == ["warn", "restrict", "halt", "derisk"]
    assert [lv.action for lv in ho.levels] == [Action.ALERT, Action.ALERT, Action.CIRCUIT_BREAK, Action.LIQUIDATE]
    assert ho.levels[1].params == {} and ho.levels[2].latch is True
    assert [lv.threshold for lv in ho.levels] == pytest.approx([-0.02, -0.025, -0.03, -0.05])
    assert arm_rules(rules, cfg, ArmSpec(name="p", tier="placebo", placebo_of="x")).rules == []


def test_halt_only_does_not_block_adds_but_halts() -> None:
    # day 1 crosses -2.5 % (restrict) then -3 % (halt): both tiers halt (the add at 10:00 is rejected as an open
    # while HALTED); the restrict level is a RESTRICT in block_open and only an ALERT in halt_only
    days = crash_days()
    _r1, blk = scripted_replay(days, capital=10_000_000.0, tier="block_open", label="b")
    _r2, ho = scripted_replay(days, capital=10_000_000.0, tier="halt_only", label="h")
    assert {o["label"]: o["code"] for o in ho.scripted}["add"] == codes.REJECT_ACCOUNT_HALTED
    lv_blk = [(e["level"], e["action"]) for e in blk.events if e["kind"] == "ALERT_RAISED"]
    lv_ho = [(e["level"], e["action"]) for e in ho.events if e["kind"] == "ALERT_RAISED"]
    assert ("restrict", "RESTRICT") in lv_blk and ("restrict", "ALERT") in lv_ho
    assert ("halt", "CIRCUIT_BREAK") in lv_ho


RB = "rb2701"


def crash_days() -> list[DaySpec]:
    """A 10M book of 250 rb lots: day 1 falls 4% (halt), day 2 opens normally (the day roll releases the latch)."""
    d1 = DaySpec(
        "20260922",
        states=[("2026-09-22 09:30", RB, 3100.0), ("2026-09-22 09:40", RB, 3050.0), ("2026-09-22 09:50", RB, 2980.0),
                ("2026-09-22 09:50:00.5", RB, 2976.0), ("2026-09-22 10:00", RB, 2976.0)],
        settle={RB: 2990.0},
        pre_settle={RB: 3100.0},
        orders={"2026-09-22 09:30": [{"instrument": RB, "side": "BUY", "offset": "OPEN", "qty": 250, "label": "book"}],
                "2026-09-22 10:00": [{"instrument": RB, "side": "BUY", "offset": "OPEN", "qty": 5, "label": "add"},
                                     {"instrument": RB, "side": "SELL", "offset": "CLOSE_TODAY", "qty": 5, "label": "trim"}]},
    )
    d2 = DaySpec(
        "20260923",
        states=[("2026-09-23 09:30", RB, 2995.0)],
        settle={RB: 2995.0},
        pre_settle={RB: 2990.0},
        orders={"2026-09-23 09:30": [{"instrument": RB, "side": "BUY", "offset": "OPEN", "qty": 5, "label": "next_day_add"}]},
    )
    return [d1, d2]


def test_block_open_rejects_opens_after_the_halt_and_releases_at_the_roll() -> None:
    _rep, res = scripted_replay(crash_days(), capital=10_000_000.0, tier="block_open", label="blk")
    by = {o["label"]: o for o in res.scripted}
    assert by["book"]["code"] is None
    assert by["add"]["code"] == codes.REJECT_ACCOUNT_HALTED
    assert by["trim"]["code"] is None and by["trim"]["statuses"] == ["FILLED"]
    assert by["next_day_add"]["code"] is None  # reset_on_day_roll: the latch is released by the day roll
    halts = [e for e in res.events if e["kind"] == "ALERT_RAISED" and e["level"] == "halt"]
    assert len(halts) == 1 and halts[0]["latched"] is True and halts[0]["trading_day"] == "20260922"


def test_no_rules_lets_everything_through() -> None:
    _rep, res = scripted_replay(crash_days(), capital=10_000_000.0, tier="none", label="none")
    assert all(o["code"] is None for o in res.scripted)
    assert res.events == []


def test_determinism_same_config_same_hashes(tmp_path: Path) -> None:
    cfg, path = load_backtest_config("breaker_synthetic", {"workers": 1})
    a = run_config(cfg, tmp_path / "a", config_path=path)
    b = run_config(cfg, tmp_path / "b", config_path=path)
    assert a.manifest["events_hash"] == b.manifest["events_hash"]
    assert a.manifest["summary_hash"] == b.manifest["summary_hash"]
    assert a.manifest["config_hash"] == b.manifest["config_hash"]
    for name in ("events.parquet", "daily.parquet", "orders.parquet", "states.parquet"):
        assert (tmp_path / "a" / name).read_bytes() == (tmp_path / "b" / name).read_bytes(), name


def test_determinism_of_a_multi_day_replay() -> None:
    r1 = scripted_replay(crash_days(), capital=10_000_000.0, tier="halve", label="x")[1]
    r2 = scripted_replay(crash_days(), capital=10_000_000.0, tier="halve", label="x")[1]
    assert r1.events == r2.events and r1.daily == r2.daily and r1.orders == r2.orders and r1.actions == r2.actions
    assert r1.actions and r1.actions[0]["qty"] == 125  # halve: ceil(250 * 0.5)
