"""Experiment arms: rule sets per tier, block-open behaviour after a halt, determinism of the outputs."""

from __future__ import annotations

from pathlib import Path

import pytest
from btsupport import DaySpec, base, scripted_replay

from futrisk.backtest.arms import arm_rules
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
    block = arm_rules(rules, cfg, ArmSpec(name="b", tier="block_open", threshold=0.015)).rules
    assert [r.id for r in block] == ["daily_loss"]
    assert [lv.action for lv in block[0].levels] == [Action.ALERT, Action.RESTRICT, Action.CIRCUIT_BREAK, Action.LIQUIDATE]
    assert [lv.threshold for lv in block[0].levels] == pytest.approx([-0.01, -0.0125, -0.015, -0.025])
    assert block[0].levels[2].latch is True
    default = arm_rules(rules, cfg, ArmSpec(name="b", tier="block_open")).rules[0]
    assert [lv.threshold for lv in default.levels] == pytest.approx([-0.02, -0.025, -0.03, -0.05])
    sysc = arm_rules(rules, cfg, ArmSpec(name="b", tier="block_open")).system
    # the engine's periodic timers are off (0): time is driven by EngineCore.next_timer_ns alone
    assert sysc.snapshot_hz == 0 and sysc.metric_sample_s == 0 and sysc.recon_interval_s == 0


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
    for name in ("events.csv", "daily.csv", "orders.csv", "summary.json"):
        assert (tmp_path / "a" / name).read_bytes() == (tmp_path / "b" / name).read_bytes(), name


def test_determinism_of_a_multi_day_replay() -> None:
    r1 = scripted_replay(crash_days(), capital=10_000_000.0, tier="block_open", label="x")[1]
    r2 = scripted_replay(crash_days(), capital=10_000_000.0, tier="block_open", label="x")[1]
    assert r1.events and r1.events == r2.events and r1.daily == r2.daily and r1.orders == r2.orders
