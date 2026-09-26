"""Run statistics and the rule's trigger days (hand-made daily rows with known answers)."""

from __future__ import annotations

from typing import Any

import pytest

from futrisk.backtest.analysis import TOTAL, analyze_arms, by_account, series_stats, trigger_days
from futrisk.backtest.config import BacktestConfig
from futrisk.backtest.replay import LEVELS, ArmResult


def _row(day: str, acct: str, pnl: float, *, cap: float = 1000.0, **flags: Any) -> dict[str, Any]:
    row: dict[str, Any] = {"trading_day": day, "account_id": acct, "capital": cap, "day_pnl": pnl, "fee": 1.0,
                           "orders": 2, "rejects": 0, "blocked_opens": 0, "blocked_adds": 0, "halt_latched": False}
    for lv in LEVELS:
        row[f"trig_{lv}"] = False
        row[f"trig_{lv}_value"] = None
    row.update(flags)
    return row


def _res(arm: str, tier: str, rows: list[dict[str, Any]], caps: dict[str, float]) -> ArmResult:
    r = ArmResult(arm=arm, tier=tier, threshold=0.03, label=arm, layer="A", accounts=sorted(caps), capital=caps)
    r.daily = rows
    r.days = sorted({x["trading_day"] for x in rows})
    return r


def test_series_stats_known_values() -> None:
    rows = [_row("d1", "A", 100.0), _row("d2", "A", -300.0), _row("d3", "A", 50.0), _row("d4", "A", -100.0)]
    st = series_stats(rows, 1000.0)
    assert st["total_ret"] == pytest.approx(-0.25)
    # equity 1100 (peak), 800, 850, 750: the drawdown from the peak is 350 / 1100
    assert st["max_dd"] == pytest.approx(350 / 1100)
    assert st["worst_day"] == pytest.approx(-0.3) and st["worst_day_date"] == "d2"
    assert st["fee"] == 4.0 and st["orders"] == 8 and st["latched_days"] == 0


def test_total_sums_the_pnl_and_ors_the_flags() -> None:
    rows = [_row("d1", "A", 10.0), _row("d1", "B", -30.0, trig_halt=True, halt_latched=True), _row("d2", "A", 5.0)]
    r = _res("x", "block_open", rows, {"A": 1000.0, "B": 500.0})
    tot = by_account(r)[TOTAL]
    assert [(t["trading_day"], t["day_pnl"], t["halt_latched"], t["trig_halt"]) for t in tot] == [
        ("d1", -20.0, True, True), ("d2", 5.0, False, False)]


def test_trigger_days_measure_the_same_day_and_the_next_day_effect() -> None:
    caps = {"A": 1000.0}
    base = _res("no_rules", "none", [_row("d1", "A", -40.0), _row("d2", "A", 20.0), _row("d3", "A", 5.0)], caps)
    arm = _res("block_open", "block_open", [
        _row("d1", "A", -35.0, trig_restrict=True, trig_halt=True, halt_latched=True, trig_halt_value=-0.031,
             blocked_opens=1),
        _row("d2", "A", 12.0),
        _row("d3", "A", 5.0),
    ], caps)
    (day,) = trigger_days(base, arm)
    assert day["trading_day"] == "d1" and day["level"] == "halt" and day["latched"] and day["value"] == -0.031
    assert day["effect"] == pytest.approx(0.005)  # 5 yuan less lost on 1000
    assert day["next_effect"] == pytest.approx(-0.008)  # the rebound it partly missed
    s = analyze_arms(BacktestConfig(name="t"), {"no_rules": base, "block_open": arm})
    assert s["baseline"] == "no_rules" and s["period"] == {"start": "d1", "end": "d3", "days": 3}
    assert s["triggers"]["block_open"]["summary"] == {"days": 1, "latched": 1, "effect_sum": pytest.approx(0.005),
                                                      "effect_positive": 1, "effect_negative": 0,
                                                      "next_effect_sum": pytest.approx(-0.008)}
    # totals: base -40 + 20 + 5 = -15, arm -35 + 12 + 5 = -18
    assert s["deltas"]["block_open"]["A"]["total_ret"] == pytest.approx(-0.003)
    assert s["deltas"]["block_open"]["TOTAL"]["total_ret"] == pytest.approx(-0.003)
    h = s["headline"]
    assert h["baseline"] == "no_rules" and h["days"] == 3
    assert h["block_open"]["latched_days"] == 1 and h["block_open"]["rule_days"] == 1
    assert "rule_days" not in h["no_rules"] and h["no_rules"]["total_ret"] == pytest.approx(-0.015)
