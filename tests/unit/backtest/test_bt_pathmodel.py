"""The path ensemble: calibration on (synthetic) minute bars, generator properties, determinism."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from btsupport import base
from test_bt_hist import rb_files

from futrisk.backtest.data import HistData
from futrisk.backtest.pathmodel import (
    ProductPathParams,
    anchor_plan,
    calibrate,
    draws,
    fine_path,
    fixed_model,
    quantile_of,
    thin,
)
from futrisk.backtest.paths import TickGrid
from futrisk.core.instruments import InstrumentRegistry


def _minute_file(root: Path, days: list[str], period: int) -> None:
    reg = InstrumentRegistry(base().products, base().calendar)
    rows = []
    for k, td in enumerate(days):
        ends: list[int] = []
        for a, b in reg.session_intervals("rb2701", td):
            t = a + period * 60 * 10**9
            while t <= b:
                ends.append(t)
                t += period * 60 * 10**9
        n = len(ends)
        # open near the high, the high early, the low late, some oscillation in between (deterministic per day)
        rng = np.random.default_rng(k)
        path = np.interp(np.arange(n + 1), [0, 2, n - 3, n], [3000, 3030, 2950, 2980]) + rng.normal(0, 6, n + 1)
        path[0], path[2], path[n - 3], path[n] = 3000, 3030, 2950, 2980
        for i, end in enumerate(ends):
            o, c = float(path[i]), float(path[i + 1])
            h = max(o, c) + (0.0 if i != 1 else 0.0)
            lo = min(o, c)
            rows.append({"ts": "", "ts_ns": end, "trading_day": td, "open": o, "high": max(h, o, c), "low": lo, "close": c,
                         "volume": 100, "open_interest": 1000, "instrument_id": "rb2701", "roll": False, "quality": None})
    df = pd.DataFrame(rows)
    df["volume"] = df["volume"].astype("Int64")
    df["open_interest"] = df["open_interest"].astype("Int64")
    (root / "minute").mkdir(parents=True, exist_ok=True)
    df.to_parquet(root / "minute" / f"rb_{period}m.parquet", index=False)


def test_calibration_on_minute_bars(tmp_path: Path) -> None:
    rb_files(tmp_path)
    cal = base().calendar
    from datetime import date

    days = [d.strftime("%Y%m%d") for d in cal.trading_days_between(date(2026, 7, 1), date(2026, 9, 18))][:30]
    _minute_file(tmp_path, days, 15)
    _minute_file(tmp_path, days, 60)
    hist = HistData(tmp_path, ["rb"], base().products)
    reg = InstrumentRegistry(base().products, base().calendar)
    m1 = calibrate(hist, reg, ["rb"], seed=5, min_days=20, check_periods=(60,))
    m2 = calibrate(hist, reg, ["rb"], seed=5, min_days=20, check_periods=(60,))
    assert m1.products == m2.products and m1.sector_rho == m2.sector_rho  # deterministic
    p = m1.params("rb")
    rep = m1.report["products"]["rb"]
    assert rep["hit_rate"] == 1.0 and p.p_near_first == pytest.approx(1.0)  # every day: open near the high, high first
    assert rep["timing"].startswith("rb 15m") and rep["timing_days"] == len(days) - 1
    assert quantile_of(p.t_first, 0.5) < 0.2  # the high comes early
    eff = rep["efficiency_15m"]
    assert eff["model"] == pytest.approx(eff["empirical"], rel=0.03) and p.sigma > 0
    assert eff["linear"] < eff["empirical"]  # the straight path oscillates less than the bars
    assert "60m" in rep["checks"] and m1.report["products"]["rb"]["calibration_days"] == len(days) - 1
    assert m1.params("zz") is m1.default


def test_draws_are_seeded_and_correlated_within_a_sector() -> None:
    model = fixed_model({"rb": "black", "hc": "black", "IF": "equity_index"}, seed=1, rho=0.9)
    assert draws(model, 0, "rb", "20260105") == draws(model, 0, "rb", "20260105")
    assert draws(model, 0, "rb", "20260105") != draws(model, 1, "rb", "20260105")
    rb = np.array([draws(model, k, "rb", "20260105") for k in range(400)])
    hc = np.array([draws(model, k, "hc", "20260105") for k in range(400)])
    iff = np.array([draws(model, k, "IF", "20260105") for k in range(400)])
    assert np.corrcoef(rb[:, 1], hc[:, 1])[0, 1] > 0.7  # same sector: correlated timing
    assert abs(np.corrcoef(rb[:, 1], iff[:, 1])[0, 1]) < 0.2  # other sector: independent


def test_fine_path_keeps_the_bar_and_the_order() -> None:
    p = ProductPathParams(1.0, 0.8, (0.3,), (0.5,))
    first, j1, j2 = anchor_plan(3000, 3030, 2950, 2980, 300, p, (0.0, 0.5, 0.5))
    assert first == "H" and 0 < j1 < j2 < 300  # open nearer the high, p = 1: the high comes first
    px = fine_path(3000, 3030, 2950, 2980, 300, first, j1, j2, 0.8, np.random.default_rng(0).standard_normal(300))
    assert px[0] == 3000 and px[-1] == 2980 and px.max() == 3030 and px.min() == 2950
    assert px[j1] == 3030 and px[j2] == 2950
    far = anchor_plan(3000, 3030, 2950, 2980, 300, ProductPathParams(0.0, 0.0, (0.3,), (0.5,)), (0.5, 0.5, 0.5))[0]
    assert far == "L"
    # an open at the high: the first extreme is the open state itself
    assert anchor_plan(3030, 3030, 2950, 2980, 300, p, (0.0, 0.5, 0.5))[1] == 0


def test_thin_keeps_anchors_and_limits_every_step() -> None:
    fr = np.linspace(0.0, 1.0, 11)
    px = np.array([3000, 3001, 3002, 3060, 3061, 3010, 3009, 3008, 2900, 2901, 2950], dtype=float)
    st = thin(fr, px, {0: "O", 3: "H", 8: "L", 10: "C"}, TickGrid("1"), 7.5)
    kinds = [k for _f, _p, k in st]
    prices = [p for _f, p, _k in st]
    assert kinds[0] == "O" and kinds[-1] == "C" and "H" in kinds and "L" in kinds
    assert max(abs(b - a) for a, b in zip(prices, prices[1:], strict=False)) <= 7.5 + 1.0  # + one tick of rounding
    assert [f for f, _p, _k in st] == sorted(f for f, _p, _k in st)
