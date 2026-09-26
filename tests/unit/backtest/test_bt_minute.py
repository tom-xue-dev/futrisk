"""Layer B on synthetic minute files: bars expanded to O/E1/E2/C, official daily settlement, hit rate."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
from btsupport import base, ts, write_hist
from test_bt_hist import FixedTarget

from futrisk.backtest.config import AccountRun, ArmSpec, BacktestConfig
from futrisk.backtest.data import HistData
from futrisk.backtest.replay import Replay
from futrisk.backtest.runner import hit_rates
from futrisk.backtest.sources import MinuteSource

D1, D2 = "20260922", "20260923"


def files(root: Path) -> None:
    rows = [
        {"td": D1, "inst": "rb2701", "o": 3100, "h": 3130, "l": 3080, "c": 3120, "s": 3110, "ps": 3095, "cps": 3095},
        {"td": D2, "inst": "rb2701", "o": 3120, "h": 3140, "l": 3100, "c": 3105, "s": 3118, "ps": 3110, "cps": 3110},
    ]
    contracts = [dict(r, main=True) for r in rows]
    write_hist(root, "rb", "SHFE", rows, contracts)
    # the file starts with a partial day (as Sina's windows do): it is left out of the replay days
    bars = [{"ts": "2026-09-21 15:00", "ts_ns": ts("2026-09-21 15:00"), "trading_day": "20260921", "open": 3090.0,
             "high": 3095.0, "low": 3088.0, "close": 3094.0, "volume": 100, "open_interest": 1000,
             "instrument_id": "rb2701", "roll": False, "quality": None}]
    for td, day in ((D1, "2026-09-22"), (D2, "2026-09-23")):
        prev = "2026-09-21" if td == D1 else "2026-09-22"
        seq = [(f"{prev} 22:00", 3100, 3130, 3095, 3125), (f"{prev} 23:00", 3125, 3128, 3080, 3090),
               (f"{day} 10:00", 3090, 3110, 3085, 3105), (f"{day} 11:15", 3105, 3115, 3100, 3110),
               (f"{day} 14:15", 3110, 3118, 3100, 3112), (f"{day} 15:00", 3112, 3121, 3108, 3120)]
        if td == D2:
            seq = [(t, o + 20, h + 10, lo + 20, c - 15) for t, o, h, lo, c in seq]
        for t, o, h, lo, c in seq:
            bars.append({"ts": t, "ts_ns": ts(t), "trading_day": td, "open": float(o), "high": float(max(h, o, c)),
                         "low": float(min(lo, o, c)), "close": float(c), "volume": 100, "open_interest": 1000,
                         "instrument_id": "rb2701", "roll": False, "quality": None})
    (root / "minute").mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(bars)
    df["volume"] = df["volume"].astype("Int64")
    df["open_interest"] = df["open_interest"].astype("Int64")
    df.to_parquet(root / "minute" / "rb_60m.parquet", index=False)


def test_minute_bars_drive_the_path_and_the_day_settles_officially(tmp_path: Path) -> None:
    files(tmp_path)
    cfg = BacktestConfig(name="m", layer="B", data={"dir": str(tmp_path), "minute_period": 60}, products=["rb"],
                         accounts={"ACC2": AccountRun(strategy="multi_trend", capital=5_000_000)},
                         arms=[ArmSpec(name="no_rules", tier="none")], workers=1)
    hist = HistData(cfg.data_dir(), ["rb"], base().products)
    src = MinuteSource(hist, cfg, base().calendar)
    assert src.days() == [D1, D2]
    plan = src.plan(D1, {"rb2701": 3095.0}, set(), {})
    assert len(plan.points) == 6 * 4
    assert [p.kind for p in plan.points[:4]] == ["O", "H", "L", "C"] or [p.kind for p in plan.points[:4]] == ["O", "L", "H", "C"]
    # the night bar labelled 22:00 covers 21:00-22:00: its states are inside that hour
    assert ts("2026-09-21 21:00") < plan.points[0].ts_ns < plan.points[3].ts_ns < ts("2026-09-21 22:00")
    rep = Replay(cfg=cfg, arm=cfg.arms[0], source=src, base_rules=base().risk_rules, base_accounts=base().accounts,
                 strategies={"ACC2": FixedTarget(["rb"])}, label="m")
    res = rep.run()
    rows = {r["trading_day"]: r for r in res.daily}
    buy = res.orders[0]
    assert buy["price"] == 3100.0 and buy["qty"] == 10
    # official settle 3110 on day 1 (not a VWAP of Sina's unreliable minute volumes)
    assert rows[D1]["position_pnl"] == pytest.approx((3110 - 3100) * 100)
    assert rows[D2]["position_pnl"] == pytest.approx((3118 - 3110) * 100)


def test_calendar_spread_far_leg_is_synthesized_in_layer_b(tmp_path: Path) -> None:
    files(tmp_path)
    # a later rb contract next to the main: the per-contract file has both, the minute file only the main
    rows = [
        {"td": D1, "inst": "rb2701", "o": 3100, "h": 3130, "l": 3080, "c": 3120, "s": 3110, "ps": 3095, "cps": 3095},
        {"td": D2, "inst": "rb2701", "o": 3120, "h": 3140, "l": 3100, "c": 3105, "s": 3118, "ps": 3110, "cps": 3110},
    ]
    far = [{"td": D1, "inst": "rb2705", "o": 3050, "h": 3070, "l": 3030, "c": 3060, "s": 3055, "ps": 3040},
           {"td": D2, "inst": "rb2705", "o": 3060, "h": 3075, "l": 3045, "c": 3050, "s": 3058, "ps": 3055}]
    write_hist(tmp_path, "rb", "SHFE", rows, [dict(r, main=True) for r in rows] + far)
    cfg = BacktestConfig(name="m", layer="B", data={"dir": str(tmp_path), "minute_period": 60}, products=["rb"],
                         accounts={"ACC5": AccountRun(strategy="rb_calendar_spread", capital=3_000_000)},
                         arms=[ArmSpec(name="no_rules", tier="none")], workers=1)
    hist = HistData(cfg.data_dir(), ["rb"], base().products, all_contract_products={"rb"})
    src = MinuteSource(hist, cfg, base().calendar, far_products=["rb"])
    plan = src.plan(D1, {"rb2701": 3095.0, "rb2705": 3040.0}, set(), {"ACC5": {"far": "far:rb"}})
    assert plan.extras == {"ACC5": {"far": "rb2705"}}
    near = [p for p in plan.points if p.instrument_id == "rb2701"]
    far_pts = [p for p in plan.points if p.instrument_id == "rb2705"]
    assert len(near) == 6 * 4  # the main keeps its minute path
    assert far_pts and [p.kind for p in far_pts if p.kind != "I"] in (["O", "H", "L", "C"], ["O", "L", "H", "C"])
    assert {p.price for p in far_pts} >= {3050.0, 3070.0, 3030.0, 3060.0}


def test_hit_rate_on_minute_files(tmp_path: Path) -> None:
    files(tmp_path)
    hist = HistData(tmp_path, ["rb"], base().products)
    hr = hit_rates(hist)
    st = hr["by_product"]["rb"]["60"]
    assert st["days"] == 2  # the first (partial) day of the file is dropped: D1 and D2 remain
    assert st["determined"] + st["same_bar"] == 2
    days = [[(108, 110, 105, 106), (106, 107, 90, 95)], [(92, 95, 90, 94), (94, 110, 93, 105)]]
    from futrisk.backtest.paths import order_hit_rate

    assert order_hit_rate(days).hit_rate == 1.0
