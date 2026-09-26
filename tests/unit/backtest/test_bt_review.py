"""Review items: look-ahead, DCE roll factors, bounds, pairing invariants, caches, controls, placebo, ensembles."""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest
from btsupport import base, write_hist
from test_bt_hist import D1, D2, D3, FixedTarget, rb_files

from futrisk.backtest.config import AccountRun, ArmSpec, BacktestConfig
from futrisk.backtest.data import DailyBar, HistData
from futrisk.backtest.experiment import placebo_schedule, relabel
from futrisk.backtest.pathmodel import fixed_model, realize
from futrisk.backtest.paths import CERTAIN, IMPOSSIBLE, UNCERTAIN, Leg, classify, pnl_bounds
from futrisk.backtest.replay import ArmResult, Replay
from futrisk.backtest.runner import get_hist, load_base, run_config
from futrisk.backtest.sources import DailySource
from futrisk.backtest.strategies import CounterTrendAdd, CounterTrendParams, Strategy, StrategyContext

D0 = "20260918"


# ------------------------------------------------------------------ fixtures
def crash_files(root: Path) -> None:
    """rb: a quiet day, then a crash day (-10 % at the low, recovering to -1.7 %), then a quiet day."""
    rows = [
        {"td": D0, "inst": "rb2701", "o": 3000, "h": 3010, "l": 2995, "c": 3000, "s": 3000, "ps": 3000, "cps": 3000},
        {"td": D1, "inst": "rb2701", "o": 3000, "h": 3010, "l": 2700, "c": 2950, "s": 2960, "ps": 3000, "cps": 3000},
        {"td": D2, "inst": "rb2701", "o": 2950, "h": 2980, "l": 2940, "c": 2970, "s": 2965, "ps": 2960, "cps": 2960},
    ]
    write_hist(root, "rb", "SHFE", rows, [dict(r, main=True) for r in rows])


def _cfg(root: Path, **kw: Any) -> BacktestConfig:
    return BacktestConfig(name="t", layer="A", data={"dir": str(root)}, products=["rb"], workers=1, **kw)


def _replay(cfg: BacktestConfig, arm: ArmSpec, strategies: dict[str, Strategy], **kw: Any) -> ArmResult:
    hist = HistData(cfg.data_dir(), ["rb"], base().products)
    src = DailySource(hist, cfg, base().calendar, **kw)
    rep = Replay(cfg=cfg, arm=arm, source=src, base_rules=base().risk_rules, base_accounts=base().accounts,
                 strategies=strategies, label=f"t-{arm.name}")
    return rep.run()


def _ladder(max_adds: int = 5, account: str = "ACC3") -> CounterTrendAdd:
    return CounterTrendAdd(CounterTrendParams(weights={"rb": 0.9}, max_adds=max_adds), account_id=account, products=["rb"])


def _pnl(r: ArmResult) -> dict[tuple[str, str], float]:
    return {(x["trading_day"], x["account_id"]): x["day_pnl"] for x in r.daily}


# ------------------------------------------------------------------ look-ahead
class Recorder(Strategy):
    """Holds nothing; records which daily bars it has seen whenever it is asked for targets."""

    name = "recorder"

    def __init__(self) -> None:
        super().__init__(Strategy.params_model(), account_id="ACC2", products=["rb"])
        self.seen: list[str] = []
        self.asked: list[tuple[str, tuple[str, ...]]] = []

    def on_bar(self, product: str, bar: DailyBar) -> None:
        self.seen.append(bar.trading_day)

    def targets(self, ctx: StrategyContext) -> dict[str, int] | None:
        self.asked.append((ctx.trading_day, tuple(self.seen)))
        return None


def test_no_look_ahead_a_strategy_sees_a_day_bar_only_after_that_day(tmp_path: Path) -> None:
    rb_files(tmp_path)
    cfg = _cfg(tmp_path, accounts={"ACC2": AccountRun(strategy="multi_trend", capital=5_000_000)})
    rec = Recorder()
    _replay(cfg, ArmSpec(name="none", tier="none"), {"ACC2": rec})
    assert rec.asked and rec.seen == [D1, D2, D3]
    for td, seen in rec.asked:
        assert all(d < td for d in seen), (td, seen)  # during day d only bars before d


def test_far_leg_is_chosen_by_the_previous_days_open_interest(tmp_path: Path) -> None:
    rows = [{"td": D1, "inst": "rb2701", "o": 3000, "h": 3010, "l": 2990, "c": 3000, "s": 3000, "ps": 3000, "cps": 3000},
            {"td": D2, "inst": "rb2701", "o": 3000, "h": 3010, "l": 2990, "c": 3000, "s": 3000, "ps": 3000, "cps": 3000}]
    contracts = [dict(r, main=True) for r in rows] + [
        {"td": D1, "inst": "rb2705", "o": 3050, "h": 3060, "l": 3040, "c": 3050, "s": 3050, "ps": 3050, "oi": 100},
        {"td": D1, "inst": "rb2709", "o": 3060, "h": 3070, "l": 3050, "c": 3060, "s": 3060, "ps": 3060, "oi": 900},
        # on D2 rb2705's open interest jumps: known only at the end of D2, so D2's far leg is still rb2709
        {"td": D2, "inst": "rb2705", "o": 3050, "h": 3060, "l": 3040, "c": 3050, "s": 3050, "ps": 3050, "oi": 5000},
        {"td": D2, "inst": "rb2709", "o": 3060, "h": 3070, "l": 3050, "c": 3060, "s": 3060, "ps": 3060, "oi": 800},
    ]
    write_hist(tmp_path, "rb", "SHFE", rows, contracts)
    hist = HistData(tmp_path, ["rb"], base().products, all_contract_products={"rb"})
    assert hist.far_contract("rb", D2, "rb2701") == "rb2709"
    assert hist.far_contract("rb", D1, "rb2701") == "rb2705"  # first day: no previous day, the nearest later contract


# ------------------------------------------------------------------ DCE roll factor
def test_dce_roll_factor_uses_the_estimated_previous_settle(tmp_path: Path) -> None:
    rows = [
        {"td": D1, "inst": "i2701", "o": 700, "h": 710, "l": 695, "c": 705, "s": 702, "ps": 698, "cps": 698, "src": "sina"},
        {"td": D2, "inst": "i2705", "o": 660, "h": 668, "l": 655, "c": 664, "s": 662, "ps": 702, "cps": None, "roll": True,
         "src": "sina"},
        {"td": D3, "inst": "i2705", "o": 663, "h": 668, "l": 660, "c": 665, "s": 664, "ps": 662, "cps": 662, "src": "sina"},
    ]
    # DCE per-contract rows carry no previous settle; the new main's row on the day before the roll has no settle
    # either -> (H + L) / 2 = 659 is its estimated settle, i.e. the new main's previous settle on the roll day
    contracts = [dict(r, main=True, ps=None) for r in rows] + [
        {"td": D1, "inst": "i2705", "o": 657, "h": 664, "l": 654, "c": 660, "s": None, "ps": None},
    ]
    write_hist(tmp_path, "i", "DCE", rows, contracts)
    hist = HistData(tmp_path, ["i"], base().products)
    s = hist.series["i"]
    assert s.roll_source(D2) == "estimated"
    assert s.roll_factor(D2) == pytest.approx(659 / 702) and s.roll_factor(D2) != 1.0
    assert s.roll_factor(D1) == 1.0 and s.roll_source(D1) == "none"
    assert [b.roll_factor for b in hist.daily_bars("i")][1] == pytest.approx(659 / 702)


def test_dce_roll_after_the_archive_freeze_uses_the_new_mains_open(tmp_path: Path) -> None:
    rows = [
        {"td": D1, "inst": "i2701", "o": 700, "h": 710, "l": 695, "c": 705, "s": 702, "ps": 698, "cps": 698, "src": "sina"},
        {"td": D2, "inst": "i2705", "o": 660, "h": 668, "l": 655, "c": 664, "s": 662, "ps": 702, "cps": None, "roll": True,
         "src": "sina"},
    ]
    write_hist(tmp_path, "i", "DCE", rows, [dict(r, main=True, ps=None) for r in rows])
    s = HistData(tmp_path, ["i"], base().products).series["i"]
    assert s.roll_source(D2) == "open" and s.roll_factor(D2) == pytest.approx(660 / 702)


# ------------------------------------------------------------------ bounds
def _book_paths(rng: np.random.Generator, legs: list[Leg], m: int) -> np.ndarray:
    """Independent random paths of every contract over a common time grid (open at 0, close at m)."""
    out = []
    for g in legs:
        first = "H" if rng.random() < 0.5 else "L"
        j1 = int(rng.integers(1, m - 2))
        j2 = int(rng.integers(j1 + 1, m))
        from futrisk.backtest.pathmodel import fine_path

        out.append(fine_path(g.open, g.high, g.low, g.close, m, first, j1, j2, float(rng.uniform(0, 1.5)),
                             rng.standard_normal(m)))
    return np.vstack(out)


def test_bounds_hold_for_random_paths_of_a_static_same_session_book() -> None:
    rng = np.random.default_rng(20260928)
    for _ in range(300):
        legs = []
        for _k in range(int(rng.integers(1, 4))):
            o = float(rng.uniform(90, 110))
            h = o + float(rng.uniform(0, 5))
            lo = o - float(rng.uniform(0, 5))
            c = float(rng.uniform(lo, h))
            legs.append(Leg(int(rng.integers(-3, 4)), float(rng.choice([1, 10])), o, h, lo, c))
        base_pnl = float(rng.normal(0, 3))
        opt, pess = pnl_bounds(base_pnl, legs, common_open=True)
        paths = _book_paths(rng, legs, 60)
        pnl = base_pnl + sum(g.net * g.multiplier * (paths[i] - g.open) for i, g in enumerate(legs))
        realized = float(np.min(pnl))
        assert pess - 1e-9 <= realized <= opt + 1e-9
        # a line this path reached is never "impossible"; a line it did not reach is never "certain"
        assert classify(opt, pess, realized + 1e-6) in (CERTAIN, UNCERTAIN)
        assert classify(opt, pess, realized - 1e-6) in (UNCERTAIN, IMPOSSIBLE)


def test_mixed_session_book_has_no_joint_opening_state() -> None:
    # long rb (opens 21:00 at -1.5 % then rallies), short IF (opens 09:30 one point up): the same-session formula
    # treats "both at their opens" (-2.5) as a forced state and calls a -2.0 line certain, yet this feasible path
    # never goes below -1.5: 21:00 rb 98.5 / IF unopened (0) -> -1.5; rb 101 -> +1; 09:30 IF 101 -> 0; IF 100 -> +1
    rb = Leg(1, 1.0, 98.5, 101.0, 98.5, 101.0, net_sod=1, pre=100.0)
    iff = Leg(-1, 1.0, 101.0, 101.0, 100.0, 100.0, net_sod=-1, pre=100.0)
    base_open = (98.5 - 100.0) + (-1) * (101.0 - 100.0)
    old_opt, old_pess = pnl_bounds(base_open, [rb, iff], common_open=True)
    assert classify(old_opt, old_pess, -2.0) == CERTAIN  # the wrong claim
    feasible = [-1.5, 1.0, 0.0, 1.0]  # the path above, state by state
    assert min(feasible) > -2.0
    opt, pess = pnl_bounds(0.0, [rb, iff], common_open=False)
    assert pess <= min(feasible) <= opt
    assert classify(opt, pess, -2.0) == UNCERTAIN
    assert classify(opt, pess, -3.0) == IMPOSSIBLE


# ------------------------------------------------------------------ paths and pairing
def test_ensemble_paths_are_seeded_exact_at_the_anchors_and_stepwise(tmp_path: Path) -> None:
    crash_files(tmp_path)
    cfg = _cfg(tmp_path)
    hist = HistData(tmp_path, ["rb"], base().products)
    model = fixed_model({"rb": "black"}, seed=7, sigma=0.8)
    a = DailySource(hist, cfg, base().calendar, path_model=model, path_index=0).plan(D1, {"rb2701": 3000.0}, set(), {})
    b = DailySource(hist, cfg, base().calendar, path_model=model, path_index=0).plan(D1, {"rb2701": 3000.0}, set(), {})
    c = DailySource(hist, cfg, base().calendar, path_model=model, path_index=1).plan(D1, {"rb2701": 3000.0}, set(), {})
    assert [(p.ts_ns, p.price) for p in a.points] == [(p.ts_ns, p.price) for p in b.points]
    assert [(p.ts_ns, p.price) for p in a.points] != [(p.ts_ns, p.price) for p in c.points]
    prices = [p.price for p in a.points]
    assert prices[0] == 3000 and prices[-1] == 2950 and max(prices) == 3010 and min(prices) == 2700
    assert max(abs(y - x) for x, y in zip(prices, prices[1:], strict=False)) <= 0.0025 * 3000 + 1.0
    other_seed = fixed_model({"rb": "black"}, seed=8, sigma=0.8)
    assert realize(other_seed, 0, "rb", D1, 3000, 3010, 2700, 2950, 6 * 3600 * 10**9)[1].tolist() != \
        realize(model, 0, "rb", D1, 3000, 3010, 2700, 2950, 6 * 3600 * 10**9)[1].tolist()


def test_a_contracts_path_does_not_depend_on_the_other_contracts_of_the_plan(tmp_path: Path) -> None:
    rb_files(tmp_path)
    crash = tmp_path / "hc"
    rows = [{"td": d, "inst": "hc2701", "o": 3300, "h": 3350 + i, "l": 3250, "c": 3320, "s": 3310, "ps": 3300, "cps": 3300}
            for i, d in enumerate((D1, D2, D3))]
    write_hist(tmp_path, "hc", "SHFE", rows, [dict(r, main=True) for r in rows])
    del crash
    hist = HistData(tmp_path, ["rb", "hc"], base().products)
    for model, k in ((None, None), (fixed_model({"rb": "black", "hc": "black"}, seed=3, sigma=0.7), 0)):
        cfg = BacktestConfig(name="t", layer="A", data={"dir": str(tmp_path)}, products=["rb", "hc"], workers=1)
        alone = DailySource(hist, cfg, base().calendar, products=["rb"], path_model=model, path_index=k)
        both = DailySource(hist, cfg, base().calendar, products=["rb", "hc"], path_model=model, path_index=k)
        pa = alone.plan(D2, {"rb2610": 3005.0, "rb2701": 3095.0}, set(), {})
        pb = both.plan(D2, {"rb2610": 3005.0, "rb2701": 3095.0, "hc2701": 3300.0}, set(), {})
        ra = [(p.ts_ns, p.price) for p in pa.points if p.instrument_id == "rb2701"]
        rb = [(p.ts_ns, p.price) for p in pb.points if p.instrument_id == "rb2701"]
        assert ra == rb and ra


def test_accounts_are_independent_so_a_task_per_account_replays_the_same(tmp_path: Path) -> None:
    crash_files(tmp_path)
    cfg = _cfg(tmp_path, accounts={"ACC3": AccountRun(strategy="counter_trend_add", capital=10_000_000),
                                   "ACC2": AccountRun(strategy="multi_trend", capital=5_000_000)})
    arm = ArmSpec(name="block_open", tier="block_open")
    together = _replay(cfg, arm, {"ACC3": _ladder(), "ACC2": FixedTarget(["rb"], lots=40)})
    alone3 = _replay(cfg, arm, {"ACC3": _ladder()})
    alone2 = _replay(cfg, arm, {"ACC2": FixedTarget(["rb"], lots=40)})
    t = _pnl(together)
    assert {k: v for k, v in t.items() if k[1] == "ACC3"} == _pnl(alone3)
    assert {k: v for k, v in t.items() if k[1] == "ACC2"} == _pnl(alone2)
    assert any(e["level"] == "halt" for e in alone3.events)


# ------------------------------------------------------------------ controls
def test_alert_only_trades_exactly_like_no_rules(tmp_path: Path) -> None:
    crash_files(tmp_path)
    cfg = _cfg(tmp_path, accounts={"ACC3": AccountRun(strategy="counter_trend_add", capital=10_000_000)})
    none = _replay(cfg, ArmSpec(name="none", tier="none"), {"ACC3": _ladder()})
    alert = _replay(cfg, ArmSpec(name="alert_only", tier="alert_only"), {"ACC3": _ladder()})
    assert _pnl(none) == _pnl(alert)
    assert [(o["instrument_id"], o["qty"], o["ok"]) for o in none.orders] == \
        [(o["instrument_id"], o["qty"], o["ok"]) for o in alert.orders]
    assert not none.events and any(e["level"] == "halt" for e in alert.events)


def test_block_open_does_nothing_to_a_static_book_but_cuts_the_ladder(tmp_path: Path) -> None:
    crash_files(tmp_path)
    cfg = _cfg(tmp_path, accounts={"ACC3": AccountRun(strategy="counter_trend_add", capital=10_000_000)})
    base_only = {a: _pnl(_replay(cfg, ArmSpec(name=a, tier=a), {"ACC3": _ladder(max_adds=0)})) for a in ("none", "block_open")}
    assert base_only["none"] == base_only["block_open"]  # no intraday opens after the halt: nothing to block
    ladder = {a: _replay(cfg, ArmSpec(name=a, tier=a), {"ACC3": _ladder(max_adds=5)}) for a in ("none", "block_open")}
    assert _pnl(ladder["none"]) != _pnl(ladder["block_open"])
    assert sum(r["blocked_opens"] + r["blocked_adds"] for r in ladder["block_open"].daily) > 0


def test_a_copied_account_run_equals_the_replay_it_stands_for(tmp_path: Path) -> None:
    rb_files(tmp_path)  # quiet days: no restrict / halt alert
    cfg = _cfg(tmp_path, accounts={"ACC2": AccountRun(strategy="multi_trend", capital=5_000_000)})
    alert = _replay(cfg, ArmSpec(name="alert_only", tier="alert_only"), {"ACC2": FixedTarget(["rb"], lots=10)})
    real = _replay(cfg, ArmSpec(name="flatten", tier="flatten"), {"ACC2": FixedTarget(["rb"], lots=10)})
    copy = relabel(alert, ArmSpec(name="flatten", tier="flatten"), cfg, "copy")
    strip = ("arm", "tier", "threshold")
    assert [{k: v for k, v in r.items() if k not in strip} for r in copy.daily] == \
        [{k: v for k, v in r.items() if k not in strip} for r in real.daily]
    assert all(r["arm"] == "flatten" for r in copy.daily)


def test_placebo_schedule_matches_the_count_and_the_times_of_day() -> None:
    days = [f"202601{d:02d}" for d in range(5, 25)]
    ref = ArmResult(arm="flatten", tier="flatten", threshold=0.03, label="f", layer="A", accounts=["A"], capital={"A": 1e7})
    latch = {days[2]: 0.25, days[7]: 0.5, days[11]: 0.9}
    ref.daily = [{"trading_day": d, "account_id": "A", "active": True, "halt_latched": d in latch, "first_ts": 1000,
                  "last_ts": 2000, "latch_ts": 1000 + int(latch[d] * 1000) if d in latch else None} for d in days]
    alert = ArmResult(arm="alert_only", tier="alert_only", threshold=0.03, label="a", layer="A", accounts=["A"],
                      capital={"A": 1e7})
    alert.daily = [{"trading_day": d, "account_id": "A", "trig_halt": d in latch or d == days[15]} for d in days]
    s1 = placebo_schedule(ref, alert, "A", seed=1, key="g|0|placebo")
    s2 = placebo_schedule(ref, alert, "A", seed=1, key="g|0|placebo")
    assert s1 == s2 and len(s1) == 3
    assert not {d for d, _f in s1} & (set(latch) | {days[15]})  # never a trigger day
    assert sorted(f for _d, f in s1) == pytest.approx(sorted(latch.values()))
    assert placebo_schedule(ref, alert, "A", seed=2, key="g|0|placebo") != s1


# ------------------------------------------------------------------ caches and hashes
def test_caches_reload_changed_files_and_hashes_are_taken_at_read(tmp_path: Path) -> None:
    rb_files(tmp_path)
    cfg = _cfg(tmp_path, accounts={"ACC2": AccountRun(strategy="multi_trend", capital=5_000_000)})
    b = load_base(cfg)
    h1 = get_hist(cfg, b)
    import hashlib

    path = tmp_path / "daily" / "rb.parquet"
    sha1 = hashlib.sha256(path.read_bytes()).hexdigest()
    assert h1.file_hashes()["daily/rb.parquet"] == sha1 and get_hist(cfg, b) is h1
    df = pd.read_parquet(path)
    df.iloc[:-1].to_parquet(path, index=False)  # the data is refreshed on disk
    sha2 = hashlib.sha256(path.read_bytes()).hexdigest()
    h2 = get_hist(cfg, b)
    assert h2 is not h1 and h2.file_hashes()["daily/rb.parquet"] == sha2 != sha1
    assert h1.file_hashes()["daily/rb.parquet"] == sha1  # what the old instance read
    assert len(h2.series["rb"].days) == len(h1.series["rb"].days) - 1
    shutil.rmtree(tmp_path / "daily")


# ------------------------------------------------------------------ end to end
def test_small_ensemble_run_end_to_end_and_deterministic(tmp_path: Path) -> None:
    crash_files(tmp_path / "data")
    raw: dict[str, Any] = {
        "name": "small", "kind": "arms", "layer": "A", "data": {"dir": str(tmp_path / "data")}, "products": ["rb"],
        "accounts": {"ACC3": {"strategy": "counter_trend_add", "params": {"weights": {"rb": 0.9}}, "capital": 10_000_000},
                     "ACC3B": {"strategy": "counter_trend_add", "params": {"weights": {"rb": 0.9}, "max_adds": 0},
                               "capital": 10_000_000, "role": "control"},
                     "ACC2": {"strategy": "multi_trend", "capital": 5_000_000}},
        "arms": [{"name": "no_rules", "tier": "none"}, {"name": "alert_only", "tier": "alert_only"},
                 {"name": "block_open", "tier": "block_open"}, {"name": "halt_only", "tier": "halt_only"},
                 {"name": "flatten", "tier": "flatten"},
                 {"name": "placebo_flatten", "tier": "placebo", "placebo_of": "flatten"}],
        "ensemble": {"paths": 2, "sigma": 0.5, "rho": 0.5, "bootstrap": 50},
        "sensitivity": [{"name": "low_first", "set": {"path.rule": "low_first"}},
                        {"name": "flat", "set": {"ensemble.sigma": 0.0}, "paths": 2,
                         "arms": ["alert_only", "block_open"]}],
        "analysis": {"bootstrap_samples": 50, "hit_rate": False},
        "workers": 1,
    }
    cfg = BacktestConfig.model_validate(raw)
    a = run_config(cfg, tmp_path / "a")
    b = run_config(cfg, tmp_path / "b")
    s = a.summary
    assert s["model"]["kind"] == "ensemble" and s["paths"] == 2 and s["baseline"] == "alert_only"
    d = s["deltas"]["block_open"]["ACC3"]["total_ret"]
    assert {"delta", "p5", "p95", "lo", "hi", "p", "p_adj"} <= set(d)
    assert "TOTAL" in s["deltas"]["block_open"] and "ACC3B" in s["deltas"]["block_open"]
    assert s["sets"]["TOTAL"] == ["ACC2", "ACC3"]  # the control is not in TOTAL
    assert "decomposition" in s["comparisons"] and "placebo" in s["comparisons"] and "ladder" in s["comparisons"]
    assert [r["group"] for r in s["sensitivity"]["rows"]].count("var:low_first") >= 3
    assert (tmp_path / "a" / "ensemble.parquet").exists() and (tmp_path / "a" / "report.html").stat().st_size > 20_000
    assert a.manifest["events_hash"] == b.manifest["events_hash"]
    assert a.manifest["summary_hash"] == b.manifest["summary_hash"]
    assert a.manifest["path_model"]["hash"] == b.manifest["path_model"]["hash"]
    # a variant's ensemble settings give it its own path model
    assert a.manifest["path_models"]["var:flat"] != a.manifest["path_models"]["main"]
    ens = pd.read_parquet(tmp_path / "a" / "ensemble.parquet")
    assert sorted(ens["realization"].unique()) == [0, 1]
