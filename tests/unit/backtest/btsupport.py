"""Helpers for the backtest tests: a hand-built multi-day data source and synthetic layer-A files."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Any

import pandas as pd

from futrisk.backtest.config import AccountRun, ArmSpec, BacktestConfig
from futrisk.backtest.data import Bar, DailyBar
from futrisk.backtest.paths import Point
from futrisk.backtest.replay import ArmResult, Replay
from futrisk.backtest.sources import DataSource, DayPlan, _build_universe
from futrisk.common.clock import BJ, bj_to_ns
from futrisk.common.config import FullConfig, load_default_config
from futrisk.common.config.products import product_code


@lru_cache(maxsize=1)
def base() -> FullConfig:
    return load_default_config()


def ts(text: str) -> int:
    """Beijing ``YYYY-MM-DD HH:MM[:SS]`` -> UTC ns."""
    return bj_to_ns(datetime.fromisoformat(text).replace(tzinfo=BJ))


@dataclass
class DaySpec:
    """One hand-built trading day: states ``(time, instrument, price)``, settle prices, scripted orders."""

    trading_day: str
    states: list[tuple[str, str, float]]
    settle: dict[str, float]
    pre_settle: dict[str, float]
    orders: dict[str, list[dict[str, Any]]] = field(default_factory=dict)  # time -> scripted orders
    mains: dict[str, str] = field(default_factory=dict)


class PlanSource(DataSource):
    """A :class:`DataSource` replaying hand-built days (official settle prices from the spec)."""

    layer = "T"

    def __init__(self, days: Sequence[DaySpec]) -> None:
        super().__init__()
        cfg = base()
        self.specs = {d.trading_day: d for d in days}
        self.calendar = cfg.calendar
        insts = {i for d in days for (_t, i, _p) in d.states}
        self.products_cfg, self.registry = _build_universe(cfg.products, self.calendar, insts, None)

    def products(self) -> list[str]:
        return sorted({product_code(i) for d in self.specs.values() for (_t, i, _p) in d.states})

    def days(self) -> list[str]:
        return sorted(self.specs)

    def initial_pre_settle(self, td: str) -> dict[str, float]:
        return dict(self.specs[td].pre_settle)

    def plan(self, td: str, pre_settle: Mapping[str, float], held: set[str], extras: Mapping[str, Mapping[str, str]]) -> DayPlan:
        spec = self.specs[td]
        pts: list[Point] = []
        vol: dict[str, int] = {}
        scripted: dict[int, list[dict[str, Any]]] = {}
        labels: dict[int, str] = {}
        for t, inst, px in spec.states:
            vol[inst] = vol.get(inst, 0) + 1
            pts.append(Point(ts(t), inst, px, "S", vol[inst], 0.0))
            labels[ts(t)] = t
        for t, orders in spec.orders.items():
            scripted[ts(t)] = [dict(o, step=t) for o in orders]
        pts.sort(key=lambda p: (p.ts_ns, p.instrument_id))
        bars: dict[str, Bar] = {}
        limits: dict[str, tuple[float, float]] = {}
        for inst in sorted({p.instrument_id for p in pts} | set(held)):
            prices = [p.price for p in pts if p.instrument_id == inst]
            ps = spec.pre_settle.get(inst, pre_settle.get(inst))
            if prices:
                bars[inst] = Bar(td, inst, product_code(inst), prices[0], max(prices), min(prices), prices[-1],
                                 spec.settle[inst], "exchange", ps, "exchange", len(prices))
                limits[inst] = (max(prices) * 1.2, min(prices) * 0.8)
            elif ps is not None:
                bars[inst] = Bar(td, inst, product_code(inst), None, None, None, None, spec.settle.get(inst, ps), "exchange",
                                 ps, "exchange", 0)
        mains = dict(spec.mains) or {product_code(i): i for i in bars}
        daily = {p: DailyBar(td, i, bars[i].open or 0.0, bars[i].high or 0.0, bars[i].low or 0.0, bars[i].close or 0.0,
                             bars[i].settle or 0.0, 1.0, False) for p, i in mains.items() if i in bars and bars[i].traded}
        nxt = {}
        for other in sorted(self.specs):
            if other > td:
                nxt = {k: v for k, v in self.specs[other].pre_settle.items() if k not in bars}
                break
        return DayPlan(trading_day=td, points=pts, bars=bars, mains=mains, limits=limits, labels=labels, scripted=scripted,
                       next_pre_settle=nxt, daily_bars=daily)


def scripted_replay(days: Sequence[DaySpec], *, account: str = "ACC3", capital: float = 200_000.0, tier: str = "none",
                    threshold: float = 0.03, label: str = "t") -> tuple[Replay, ArmResult]:
    cfg = BacktestConfig(name="t", accounts={account: AccountRun(strategy="counter_trend_add", capital=capital)},
                         arms=[ArmSpec(name=tier, tier=tier, threshold=threshold)], workers=1)
    src = PlanSource(days)
    rep = Replay(cfg=cfg, arm=cfg.arms[0], source=src, base_rules=base().risk_rules, base_accounts=base().accounts,
                 strategies={}, label=label, use_strategies=False, scenario_account=account)
    return rep, rep.run()


# ------------------------------------------------------------------ synthetic data/hist files
def write_hist(root: Path, product: str, exchange: str, rows: Sequence[Mapping[str, Any]],
               contracts: Sequence[Mapping[str, Any]]) -> None:
    """``daily/{product}.parquet`` + ``daily/{EXCHANGE}/{product}.parquet`` with the documented columns."""
    (root / "daily" / exchange).mkdir(parents=True, exist_ok=True)
    cont = pd.DataFrame([{
        "trading_day": r["td"], "instrument_id": r["inst"], "open": r["o"], "high": r["h"], "low": r["l"], "close": r["c"],
        "settle": r["s"], "pre_settle": r["ps"], "contract_pre_settle": r.get("cps"), "volume": r.get("v", 1000),
        "open_interest": r.get("oi", 5000), "roll": r.get("roll", False), "settle_source": r.get("src", "exchange"),
        "instrument_source": "exchange", "sina_instrument_id": r["inst"],
    } for r in rows])
    cont["volume"] = cont["volume"].astype("Int64")
    cont["open_interest"] = cont["open_interest"].astype("Int64")
    cont.to_parquet(root / "daily" / f"{product}.parquet", index=False)
    pc = pd.DataFrame([{
        "trading_day": r["td"], "instrument_id": r["inst"], "open": r["o"], "high": r["h"], "low": r["l"], "close": r["c"],
        "settle": r.get("s"), "pre_settle": r.get("ps"), "volume": r.get("v", 1000), "open_interest": r.get("oi", 5000),
        "turnover": None, "is_main": r.get("main", False), "settle_source": r.get("src", "exchange"),
    } for r in contracts])
    pc["volume"] = pc["volume"].astype("Int64")
    pc["open_interest"] = pc["open_interest"].astype("Int64")
    pc["turnover"] = pc["turnover"].astype("float64")
    pc.to_parquet(root / "daily" / exchange / f"{product}.parquet", index=False)
