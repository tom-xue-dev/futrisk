"""Statistics definitions on hand-made series and results."""

from __future__ import annotations

import math

import numpy as np
import pytest

from futrisk.backtest.analysis import plateau_test
from futrisk.backtest.metrics import (
    adjust_pvalues,
    avoided_missed,
    block_bootstrap,
    cvar,
    effect_interval,
    episodes,
    leave_one_out,
    max_drawdown,
    path_day_bootstrap,
    perf_path,
    series_stats,
    true_positives,
)
from futrisk.backtest.pathmodel import average_ranks, spearman
from futrisk.backtest.replay import ArmResult


def test_series_stats_known_values() -> None:
    st = series_stats([0.01, -0.02, 0.01, 0.0], 1_000_000.0)
    assert st["total_pnl"] == pytest.approx(0.0, abs=1e-6)
    assert st["max_dd"] == pytest.approx(0.02 / 1.01)
    assert st["worst_day"] == -0.02 and st["best_day"] == 0.01
    r = np.array([-0.05, -0.04, -0.03, -0.02, -0.01] + [0.001] * 95)
    assert cvar(r) == pytest.approx(0.03)  # mean of the worst 5%
    assert max_drawdown(perf_path(np.array([0.1, -0.2, 0.05]), 1.0), 1.0) == pytest.approx(0.2 / 1.1)


def test_block_bootstrap_is_paired_and_seeded() -> None:
    rng = np.random.default_rng(1)
    a = rng.normal(0, 0.01, 500)
    same = block_bootstrap(a, a.copy(), lambda x: float(x.sum()), samples=200, block=20, seed=3)
    assert same["delta"] == 0.0 and same["lo"] == 0.0 and same["hi"] == 0.0
    assert same["p"] == 1.0  # an arm that never differs from its baseline is not "significant"
    b = a - 0.001
    one = block_bootstrap(a, b, lambda x: float(x.sum()), samples=200, block=20, seed=3)
    two = block_bootstrap(a, b, lambda x: float(x.sum()), samples=200, block=20, seed=3)
    assert one == two and one["delta"] == pytest.approx(0.5) and one["lo"] == pytest.approx(0.5)
    assert one["p"] < 0.01


def test_path_day_bootstrap_combines_paths_and_days() -> None:
    rng = np.random.default_rng(7)
    base = [rng.normal(0, 0.01, 300) for _ in range(5)]
    pairs = [(b + 0.0002 * (k + 1), b) for k, b in enumerate(base)]  # path k shifts every day by 0.02 % × (k + 1)
    out = path_day_bootstrap(pairs, lambda x: float(x.sum()), samples=400, block=20, seed=11)
    per_path = sorted(300 * 0.0002 * (k + 1) for k in range(5))
    assert out["delta"] == pytest.approx(per_path[2]) and out["paths"] == 5 and out["paths_positive"] == 5
    assert out["p5"] == pytest.approx(np.percentile(per_path, 5)) and out["p95"] == pytest.approx(np.percentile(per_path, 95))
    assert out["lo"] <= out["p5"] + 1e-12 and out["hi"] >= out["p95"] - 1e-12  # the combined interval covers the path range
    assert path_day_bootstrap(pairs, lambda x: float(x.sum()), samples=400, block=20, seed=11) == out
    zero = path_day_bootstrap([(b, b.copy()) for b in base], lambda x: float(x.sum()), samples=100, block=20, seed=1)
    assert zero["delta"] == 0.0 and zero["p"] == 1.0


def test_multiplicity_adjustments() -> None:
    p = {"a": 0.01, "b": 0.04, "c": 0.03, "d": float("nan")}
    holm = adjust_pvalues(p, "holm")
    assert holm["a"] == pytest.approx(0.03) and holm["c"] == pytest.approx(0.06) and holm["b"] == pytest.approx(0.06)
    assert math.isnan(holm["d"])
    bh = adjust_pvalues(p, "bh")
    assert bh["a"] == pytest.approx(0.03) and bh["c"] == pytest.approx(0.04) and bh["b"] == pytest.approx(0.04)


def _res(arm: str, tier: str, pnl: list[float], events: list[dict], close: list[float] | None = None,
         open_: list[float] | None = None, cls: str = "certain") -> ArmResult:
    days = [f"202601{d:02d}" for d in range(5, 5 + len(pnl))]
    r = ArmResult(arm=arm, tier=tier, threshold=0.03, label=arm, layer="A", accounts=["A1"], capital={"A1": 1_000_000.0})
    r.days = days
    eq = 1_000_000.0
    rows = []
    for i, (d, p) in enumerate(zip(days, pnl, strict=True)):
        c = close[i] if close is not None else p
        o = open_[i] if open_ is not None else 0.0
        rows.append({"trading_day": d, "account_id": "A1", "day_pnl": p, "roll_day": False, "est_settle": False,
                     "class_halt": cls, "engine_equity_sod": eq, "engine_equity_close": eq + c, "engine_equity_open": eq + o})
        eq += p
    r.daily = rows
    r.events = events
    return r


def _ev(day: str, level: str, value: float, at_open: bool = False, ts: str = "10:00:00") -> dict:
    return {"kind": "ALERT_RAISED", "rule_id": "daily_loss", "level": level, "account_id": "A1", "trading_day": day,
            "ts": f"{day}T{ts}", "value": value, "at_open": at_open}


def test_episodes_per_level_and_per_basis() -> None:
    base = _res("no_rules", "none", [0, -40_000, 10_000, 5_000, 0], [],
                close=[0, -52_000, 9_000, 5_000, 0], open_=[0, 0, -9_000, 1_000, 0])
    arm = _res("block_open", "block_open", [0, -32_000, 12_000, 5_000, 0],
               [_ev("20260106", "restrict", -0.026, ts="09:50:00"), _ev("20260106", "halt", -0.031, ts="10:05:00")],
               close=[0, -38_000, 11_000, 5_000, 0], open_=[0, 0, -4_000, 1_000, 0])
    eps = episodes(arm, base, [0, 1, 5])
    e = eps[0]
    assert len(eps) == 1 and e["level"] == "halt"
    # the time and value of each level come from that level's own alert (not the first alert of the day)
    assert (e["value"], e["ts"][-8:]) == (-0.031, "10:05:00")
    assert (e["restrict_value"], e["restrict_ts"][-8:], e["halt_value"]) == (-0.026, "09:50:00", -0.031)
    # settle: day 2 +8 000, day 3 +2 000; close: day 2 +14 000 (the settle - close unwind stays on day 2)
    assert (e["delta_settle_0"], e["delta_settle_1"], e["delta_settle_5"]) == (8_000.0, 10_000.0, 10_000.0)
    assert (e["delta_close_0"], e["delta_close_1"]) == (14_000.0, 8_000.0 + 2_000.0)
    # next open: settle through day 2 + day 3's SOD book at its open (+5 000)
    assert e["delta_open_0"] == 8_000.0 + 5_000.0
    assert e["delta_0"] == e["delta_settle_0"]
    am = avoided_missed(eps, [0, 1], basis="close")
    assert am["0"]["avoided"] == 14_000.0 and am["0"]["missed"] == 0.0 and am["1"]["net"] == 10_000.0
    assert avoided_missed(eps, [0], basis="settle")["0"]["net"] == 8_000.0


def test_true_positives_settle_versus_close() -> None:
    # trigger at -3.1 % on day 2: the day closes at -3.4 % but settles at -2.5 % (a VWAP settle above the close):
    # a TP at the close, an FP at the settlement price; after day 3 the loss is -4.5 % either way
    alert = _res("alert_only", "alert_only", [0, -25_000, -20_000, 30_000, 0], [_ev("20260106", "halt", -0.031, at_open=True)],
                 close=[0, -34_000, -20_000, 30_000, 0])
    tp = true_positives(alert, 0.03, [0, 1, 5])
    assert tp["triggers"] == 1 and tp["gap_triggers"] == 1 and tp["gap_share"] == 1.0
    assert tp["settle"]["0"]["tp"] == 0 and tp["close"]["0"]["tp"] == 1
    assert tp["settle"]["1"]["tp"] == 1 and tp["close"]["1"]["tp"] == 1 and tp["settle"]["5"]["tp"] == 0
    assert tp["close"]["0"]["deepened"] == 1 and tp["settle"]["0"]["deepened"] == 0
    assert tp["0"] == tp["settle"]["0"]  # the old keys are the settle basis


def test_true_positives_leave_roll_days_out_of_the_rates() -> None:
    alert = _res("alert_only", "alert_only", [0, -40_000, 0], [_ev("20260106", "halt", -0.031)])
    alert.daily[1]["roll_day"] = True
    tp = true_positives(alert, 0.03, [0], exclude_roll_days=True)
    assert tp["triggers"] == 1 and tp["used"] == 0 and tp["excluded_roll_days"] == 1 and tp["on_roll_days"] == 1
    assert true_positives(alert, 0.03, [0], exclude_roll_days=False)["settle"]["0"]["tp"] == 1


def test_effect_interval_counts_uncertain_episodes_between_zero_and_their_value() -> None:
    eps = [{"class_halt": "certain", "delta_close_0": 100.0}, {"class_halt": "uncertain", "delta_close_0": -30.0},
           {"class_halt": "uncertain", "delta_close_0": 50.0}, {"class_halt": "impossible", "delta_close_0": -10.0}]
    iv = effect_interval(eps, 0, "close")
    assert (iv["low"], iv["high"]) == (100.0 - 40.0, 150.0)  # low: every negative non-certain episode, high: every positive
    assert iv["certain_part"] == 100.0 and iv["certain_episodes"] == 1 and iv["other_episodes"] == 3
    assert iv["low"] <= iv["certain_part"] + iv["uncertain_part"] <= iv["high"]


def test_spearman_average_ranks_and_constants() -> None:
    assert average_ranks([1, 1, 0, 0]) == [2.5, 2.5, 0.5, 0.5]
    assert spearman([1, 1, 0, 0], [0, 0, 1, 1]) == pytest.approx(-1.0)  # not -0.6: ties share their average rank
    assert spearman([1, 2, 3, 4], [10, 20, 30, 40]) == pytest.approx(1.0)
    assert math.isnan(spearman([1, 1, 1, 1], [1, 2, 3, 4]))  # constant input: undefined, not 1.00
    assert math.isnan(spearman([1.0], [2.0]))


def test_plateau_test_rejects_a_straight_slope() -> None:
    ths = [0.0075, 0.01, 0.015, 0.02, 0.025, 0.03, 0.04, 0.05]
    line = {t: 1.0 - 10.0 * t for t in ths}  # monotone: tighter is always better
    v = plateau_test(line, 0.03)
    assert v["plateau"] is False and v["edge_optimum"] is True and v["best_threshold"] == 0.0075
    assert v["monotone"] == "decreasing"
    top = {0.0075: -0.4, 0.01: -0.2, 0.015: 0.05, 0.02: 0.10, 0.025: 0.11, 0.03: 0.11, 0.04: 0.105, 0.05: 0.0}
    w = plateau_test(top, 0.03)
    assert w["plateau"] is True and w["edge_optimum"] is False
    knife = {**top, 0.04: -0.3}
    assert plateau_test(knife, 0.03)["plateau"] is False
    flat = dict.fromkeys(ths, 0.0)
    assert plateau_test(flat, 0.03)["plateau"] is True


def test_leave_one_out() -> None:
    days = ["20150105", "20150106", "20160104", "20160105"]
    base = np.array([0.0, -1000.0, 500.0, 0.0])
    delta = np.array([0.0, 800.0, -100.0, 50.0])
    out = leave_one_out(delta, base, days, 100_000.0, {"year 2015": days[:2], "year 2016": days[2:]})
    assert out["full"]["d_total_ret"] == pytest.approx(0.0075)
    assert out["drops"]["year 2015"]["d_total_ret"] == pytest.approx(-0.0005)
    assert out["drops"]["year 2016"]["d_total_ret"] == pytest.approx(0.008)
    assert out["min_drop"] == "year 2015" and out["sign_flips"] == ["year 2015"]
