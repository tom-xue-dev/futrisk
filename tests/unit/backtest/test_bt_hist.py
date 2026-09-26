"""Layer A on synthetic data/hist files: loader, main-contract roll handling, DCE estimates, limit locks."""

from __future__ import annotations

import math
from pathlib import Path
from typing import Any

import pytest
from btsupport import base, write_hist

from futrisk.backtest.config import AccountRun, ArmSpec, BacktestConfig
from futrisk.backtest.data import Bar, HistData
from futrisk.backtest.metrics import roll_gap_analysis
from futrisk.backtest.paths import TickGrid
from futrisk.backtest.replay import Replay
from futrisk.backtest.sources import DailySource, lock_prices
from futrisk.backtest.strategies import Strategy, StrategyContext
from futrisk.core.instruments import InstrumentRegistry

D1, D2, D3 = "20260921", "20260922", "20260923"


class FixedTarget(Strategy):
    """Always +``lots`` in every product's main contract."""

    name = "fixed_target"

    def __init__(self, products: list[str], lots: int = 10) -> None:
        super().__init__(Strategy.params_model(), account_id="ACC2", products=products)
        self.lots = lots

    def targets(self, ctx: StrategyContext) -> dict[str, int] | None:
        return {q.instrument_id: self.lots for q in ctx.quotes.values() if q.price is not None}


def rb_files(root: Path) -> None:
    rows = [
        {"td": D1, "inst": "rb2610", "o": 3000, "h": 3020, "l": 2990, "c": 3010, "s": 3005, "ps": 2998, "cps": 2998},
        {"td": D2, "inst": "rb2701", "o": 3100, "h": 3120, "l": 3090, "c": 3110, "s": 3108, "ps": 3005, "cps": 3095,
         "roll": True},
        {"td": D3, "inst": "rb2701", "o": 3110, "h": 3130, "l": 3100, "c": 3120, "s": 3115, "ps": 3108, "cps": 3108},
    ]
    contracts = [
        {"td": D1, "inst": "rb2610", "o": 3000, "h": 3020, "l": 2990, "c": 3010, "s": 3005, "ps": 2998, "main": True},
        {"td": D1, "inst": "rb2701", "o": 3090, "h": 3100, "l": 3080, "c": 3095, "s": 3095, "ps": 3088},
        {"td": D2, "inst": "rb2610", "o": 3010, "h": 3015, "l": 3000, "c": 3005, "s": 3008, "ps": 3005},
        {"td": D2, "inst": "rb2701", "o": 3100, "h": 3120, "l": 3090, "c": 3110, "s": 3108, "ps": 3095, "main": True},
        {"td": D3, "inst": "rb2701", "o": 3110, "h": 3130, "l": 3100, "c": 3120, "s": 3115, "ps": 3108, "main": True},
    ]
    write_hist(root, "rb", "SHFE", rows, contracts)


def i_files(root: Path) -> None:
    """DCE after 2024-07-17: only main rows; the new main's previous settle is unknown on the roll day."""
    rows = [
        {"td": D1, "inst": "i2701", "o": 700, "h": 710, "l": 695, "c": 705, "s": 702, "ps": 698, "cps": 698, "src": "sina"},
        {"td": D2, "inst": "i2705", "o": 690, "h": 700, "l": 685, "c": 695, "s": 693, "ps": 702, "cps": None, "roll": True,
         "src": "sina"},
        {"td": D3, "inst": "i2705", "o": 694, "h": 699, "l": 690, "c": 697, "s": 696.5, "ps": 693, "cps": 693, "src": "estimated"},
    ]
    contracts = [
        {"td": D1, "inst": "i2701", "o": 700, "h": 710, "l": 695, "c": 705, "s": 702, "ps": 698, "main": True, "src": "sina"},
        {"td": D2, "inst": "i2705", "o": 690, "h": 700, "l": 685, "c": 695, "s": 693, "ps": None, "main": True, "src": "sina"},
        {"td": D3, "inst": "i2705", "o": 694, "h": 699, "l": 690, "c": 697, "s": 696.5, "ps": 693, "main": True,
         "src": "estimated"},
    ]
    write_hist(root, "i", "DCE", rows, contracts)


def run(root: Path, products: list[str]) -> tuple[Replay, Any, DailySource]:
    cfg = BacktestConfig(name="t", data={"dir": str(root)}, products=products,
                         accounts={"ACC2": AccountRun(strategy="multi_trend", capital=5_000_000)},
                         arms=[ArmSpec(name="no_rules", tier="none")], workers=1)
    hist = HistData(cfg.data_dir(), products, base().products)
    src = DailySource(hist, cfg, base().calendar)
    rep = Replay(cfg=cfg, arm=cfg.arms[0], source=src, base_rules=base().risk_rules, base_accounts=base().accounts,
                 strategies={"ACC2": FixedTarget(products)}, label="hist")
    return rep, rep.run(), src


def test_loader_reads_the_documented_schema(tmp_path: Path) -> None:
    rb_files(tmp_path)
    hist = HistData(tmp_path, ["rb", "cu"], base().products)
    assert hist.products == ["rb"] and any("cu: data missing" in n for n in hist.notes)
    assert hist.main("rb", D2) == "rb2701" and hist.series["rb"].rolls == [False, True, False]
    assert hist.series["rb"].roll_factor(D2) == pytest.approx(3095 / 3005)
    old = hist.bar(D2, "rb2610")
    assert old is not None and old.settle == 3008 and not old.is_main
    assert set(hist.file_hashes()) == {"daily/rb.parquet", "daily/SHFE/rb.parquet"}


def test_roll_closes_the_old_main_and_opens_the_new_one_at_the_open(tmp_path: Path) -> None:
    rb_files(tmp_path)
    rep, res, src = run(tmp_path, ["rb"])
    plan = src.plan(D2, {"rb2610": 3005.0}, {"rb2610"}, {})
    assert plan.rolls == {"rb": ("rb2610", "rb2701")} and set(plan.bars) == {"rb2610", "rb2701"}
    trades = [(o["trading_day"], o["instrument_id"], o["direction"], o["offset"], o["qty"], o["price"]) for o in res.orders]
    assert trades == [
        (D1, "rb2610", "BUY", "OPEN", 10, 3000.0),
        (D2, "rb2610", "SELL", "CLOSE_YESTERDAY", 10, 3010.0),
        (D2, "rb2701", "BUY", "OPEN", 10, 3100.0),
    ]
    rows = {r["trading_day"]: r for r in res.daily}
    # day 1: (3005-3000)*100 - fee 30.00; day 2 exact: old (3010-3005)*100 + new (3108-3100)*100 - fees 30.10 - 31.00
    assert rows[D1]["day_pnl"] == pytest.approx(500 - 30.0)
    assert rows[D2]["day_pnl"] == pytest.approx(500 + 800 - 30.10 - 31.00)
    assert rows[D3]["day_pnl"] == pytest.approx(700)
    assert rows[D2]["roll_day"] and not rows[D3]["roll_day"]
    # a naive continuous series would have booked the +3.0% roll gap: (3108 - 3005) * 100
    naive = (3108 - 3005) * 100
    assert naive - rows[D2]["day_pnl"] > 9000
    rg = roll_gap_analysis(res, src.hist.series, {"rb": 10}, 0.03)
    assert rg["rolls"] == {"rb": 1} and rg["roll_account_days"] == 1
    assert rg["gap_mean_abs_pct"]["rb"] == pytest.approx(abs(3095 / 3005 - 1) * 100)


def test_dce_old_main_without_data_exits_flat_at_its_previous_settle(tmp_path: Path) -> None:
    i_files(tmp_path)
    rep, res, src = run(tmp_path, ["i"])
    d2 = [o for o in res.orders if o["trading_day"] == D2]
    assert [(o["instrument_id"], o["direction"], o["price"]) for o in d2] == [("i2701", "SELL", 702.0), ("i2705", "BUY", 690.0)]
    plan = src.plan(D2, {"i2701": 702.0}, {"i2701"}, {})
    assert plan.bars["i2701"].settle_source == "flat" and plan.bars["i2701"].estimated
    assert plan.bars["i2705"].pre_settle_source == "open" and plan.bars["i2705"].estimated
    rows = {r["trading_day"]: r for r in res.daily}
    assert rows[D2]["est_settle"] and rows[D3]["est_settle"]  # D3 settles on an estimated DCE price
    # D2: old exit at its previous settle (0 overnight move), new (693-690)*1000 lots... i multiplier 100
    assert rows[D2]["day_pnl"] == pytest.approx((693 - 690) * 100 * 10 - (702 * 1000 * 0.0001 + 690 * 1000 * 0.0001))


def test_limit_lock_detection() -> None:
    reg = InstrumentRegistry(base().products, base().calendar)
    g = TickGrid("1")
    # opened and closed at the low, a move far beyond today's 5% limit (history had a wider limit)
    b = Bar("20200203", "rb2701", "rb", 3000.0, 3040.0, 3000.0, 3000.0, 3040.0, "exchange", 3261.0, volume=1)
    upper, lower, up_lock, down_lock = lock_prices(b, reg, g)
    assert down_lock == 3000.0 and up_lock is None and lower == 3000.0 and upper >= 3040.0
    # touched the configured limit only
    b2 = Bar("20260929", "rb2701", "rb", 3100.0, 3150.0, 2945.0, 3000.0, 3010.0, "exchange", 3100.0, volume=1)
    _u, lo2, _ul, dl2 = lock_prices(b2, reg, g)
    assert dl2 == 2945.0 and lo2 == 2945.0
    # an ordinary day: no lock, limits from the configured band
    b3 = Bar("20260929", "rb2701", "rb", 3100.0, 3150.0, 3050.0, 3120.0, 3110.0, "exchange", 3100.0, volume=1)
    u3, l3, ul3, dl3 = lock_prices(b3, reg, g)
    assert (ul3, dl3) == (None, None) and (u3, l3) == (3255.0, 2945.0)
    assert not math.isnan(u3)
