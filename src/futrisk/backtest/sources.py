"""Day plans for the replay: the market states of one trading day per data layer (01 §8.16 数据分层).

A :class:`DayPlan` holds everything the day loop needs for one trading day: the
market states to replay in time order, the contracts' daily bars (settlement
price, previous settlement), the main contract of each product, roll
information, limit prices / limit locks and the previous settlement prices of
tomorrow's contracts. The four layers produce it differently:

- **A** (:class:`DailySource`): synthesized paths from daily bars
  (:mod:`futrisk.backtest.paths`), official settlement prices;
- **B** (:class:`MinuteSource`): every Sina minute bar expanded to O, E1, E2, C;
  Sina's minute volumes are unreliable, so the day still settles on the official
  daily settlement price (``data/hist/SOURCES.md``); contracts without minute
  bars (an old main on a roll day) fall back to the synthesized path;
- **C** (:class:`TickSource`): recorded ticks (``<home>/ticks/raw/<td>/sina``,
  LIVE rows only) replayed as recorded; the settlement price is computed from the
  replayed tape with the 01 §8.15 rules (``core.settle_price``: SHFE/DCE full-day
  VWAP, CFFEX last hour, exchange rounding);
- **D** (:class:`ScenarioSource`): synthetic scenarios with known answers.

Limit locks: history's limits differ from today's ``products.yaml`` (widened
before holidays and in stress), so a state is treated as limit-locked when its
price is the day's extreme and that extreme is at or beyond the configured
limit, or is a move of at least 3.5 % at which the market opened or closed.
The tick's limit prices are widened to the day's range (a historical limit may
be wider than today's).
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from futrisk.backtest.config import BacktestConfig, ScenarioSpec
from futrisk.backtest.data import Bar, DailyBar, HistData, list_instruments
from futrisk.backtest.pathmodel import PathModel, ensemble_points
from futrisk.backtest.paths import (
    KIND_CLOSE,
    KIND_OPEN,
    Point,
    TickGrid,
    Timeline,
    leg_counts,
    minute_bar_points,
    synthesize,
)
from futrisk.common.calendar import TradingCalendar, parse_templates
from futrisk.common.clock import BJ, bj_to_ns, parse_td, td_str
from futrisk.common.config.products import ProductsConfig, product_code
from futrisk.common.decimals import D
from futrisk.core.instruments import InstrumentRegistry
from futrisk.core.settle_price import build_settle_input, determine_settlement_price
from futrisk.messages.enums import LimitLock, Rounding, SettlePriceSource
from futrisk.messages.eod import SettlementPriceView
from futrisk.messages.md import FLAG_LIMIT_DOWN_LOCKED, FLAG_LIMIT_UP_LOCKED

LOCK_MIN_MOVE = 0.035  # a move of at least 3.5% at which the market opened or closed counts as a limit lock


@dataclass
class DayPlan:
    """Everything the day loop replays and settles for one trading day."""

    trading_day: str
    points: list[Point]  # sorted by (ts, instrument)
    bars: dict[str, Bar]  # instrument -> bar of the day (official settle / previous settle)
    mains: dict[str, str]  # product -> main contract
    roll_factors: dict[str, float] = field(default_factory=dict)  # product -> back-adjustment factor
    rolls: dict[str, tuple[str, str]] = field(default_factory=dict)  # product -> (old main, new main)
    limits: dict[str, tuple[float, float]] = field(default_factory=dict)  # instrument -> (upper, lower)
    extras: dict[str, dict[str, str]] = field(default_factory=dict)  # account -> alias -> instrument
    next_pre_settle: dict[str, float] = field(default_factory=dict)  # tomorrow's contracts -> today's settle
    daily_bars: dict[str, DailyBar] = field(default_factory=dict)  # product -> completed bar (strategy history)
    labels: dict[int, str] = field(default_factory=dict)  # ts -> scenario step label (layer D)
    scripted: dict[int, list[dict[str, Any]]] = field(default_factory=dict)  # ts -> scripted orders (layer D)
    compute_settle: bool = False  # C/D: settle from the replayed tape

    @property
    def estimated(self) -> set[str]:
        return {i for i, b in self.bars.items() if b.estimated}

    def first_ts(self) -> dict[str, int]:
        out: dict[str, int] = {}
        for p in self.points:
            out.setdefault(p.instrument_id, p.ts_ns)
        return out


# ------------------------------------------------------------------ helpers
def lock_prices(bar: Bar, registry: InstrumentRegistry, grid: TickGrid) -> tuple[float, float, float | None, float | None]:
    """``(upper, lower, up_lock, down_lock)`` of a traded bar (see the module docstring)."""
    assert bar.traded
    o, h, lo, c = bar.open or 0.0, bar.high or 0.0, bar.low or 0.0, bar.close or 0.0
    ps = bar.pre_settle if bar.pre_settle else o
    up_d, lo_d = registry.limit_prices(bar.instrument_id, D(ps), bar.trading_day)
    cfg_up, cfg_lo = float(up_d), float(lo_d)
    half = grid.tick / 2
    up_lock: float | None = None
    down_lock: float | None = None
    if lo <= cfg_lo + half or ((ps - lo) / ps >= LOCK_MIN_MOVE and (abs(c - lo) < half or abs(o - lo) < half)):
        down_lock = lo
    if h >= cfg_up - half or ((h - ps) / ps >= LOCK_MIN_MOVE and (abs(c - h) < half or abs(o - h) < half)):
        up_lock = h
    upper = grid.ceil(up_lock if up_lock is not None else max(cfg_up, h))
    lower = grid.floor(down_lock if down_lock is not None else min(cfg_lo, lo))
    return upper, lower, up_lock, down_lock


def mark_locks(points: Iterable[Point], up_lock: float | None, down_lock: float | None, grid: TickGrid) -> None:
    half = grid.tick / 2
    for p in points:
        if up_lock is not None and abs(p.price - up_lock) < half:
            p.flags |= FLAG_LIMIT_UP_LOCKED
        if down_lock is not None and abs(p.price - down_lock) < half:
            p.flags |= FLAG_LIMIT_DOWN_LOCKED


def official_view(bar: Bar, registry: InstrumentRegistry) -> SettlementPriceView:
    """The day's settlement price as a price row; estimated prices are marked MANUAL with the reason."""
    assert bar.settle is not None
    price = registry.round_price(bar.instrument_id, D(bar.settle), Rounding.ROUND_TICK) if bar.estimated else D(bar.settle)
    if bar.settle_source in ("exchange", "sina", ""):
        return SettlementPriceView(trading_day=bar.trading_day, instrument_id=bar.instrument_id, price=f"{price}",
                                   source=SettlePriceSource.OFFICIAL, rounding=Rounding.NONE)
    return SettlementPriceView(trading_day=bar.trading_day, instrument_id=bar.instrument_id, price=f"{price}",
                               source=SettlePriceSource.MANUAL, rounding=Rounding.NONE, operator="backtest-data",
                               reason=f"estimated settle ({bar.settle_source})")


def tape_view(
    registry: InstrumentRegistry, instrument_id: str, trading_day: str, points: Sequence[Point], pre_settle: float
) -> SettlementPriceView:
    """Settlement price from a replayed tape with the 01 §8.15 rules (layers C and D)."""
    mult = registry.multiplier(instrument_id)
    tape: list[tuple[int, Decimal, int]] = []
    prev_v, prev_t = 0, 0.0
    for p in points:
        dv = p.volume - prev_v
        if dv > 0:
            dt = p.turnover - prev_t
            px = D(dt / (dv * mult)) if dt > 0 else D(p.price)
            tape.append((p.ts_ns, px, dv))
        prev_v, prev_t = p.volume, p.turnover
    last = points[-1] if points else None
    lock = LimitLock.NONE
    if last is not None and last.flags & FLAG_LIMIT_UP_LOCKED:
        lock = LimitLock.UP
    elif last is not None and last.flags & FLAG_LIMIT_DOWN_LOCKED:
        lock = LimitLock.DOWN
    inp = build_settle_input(
        registry, instrument_id, trading_day, tape, pre_settle=D(pre_settle),
        last=D(last.price) if last else None,
        bid1=D(last.bid1) if last is not None and last.bid1 else None,
        ask1=D(last.ask1) if last is not None and last.ask1 else None,
        limit_lock=lock,
    )
    return determine_settlement_price(inp, registry)


def session_timeline(registry: InstrumentRegistry, instrument_id: str, trading_day: str) -> Timeline | None:
    ivs = registry.session_intervals(instrument_id, trading_day)
    return Timeline(ivs) if ivs else None


def minute_common_days(hist: HistData, period: int) -> set[str]:
    """Trading days every loaded product with a ``period``-minute file covers (the layer B window)."""
    common: set[str] | None = None
    for p in hist.products:
        try:
            bars = hist.minute_bars(p, period)
        except FileNotFoundError:
            continue
        ds = {b.trading_day for b in bars if b.instrument_id is not None}
        ds.discard(min(ds, default=""))  # a file's first day is usually partial
        common = ds if common is None else common & ds
    return common or set()


# ------------------------------------------------------------------ base
class DataSource:
    """Common interface of the four layers."""

    layer = "?"

    def __init__(self) -> None:
        self.notes: list[str] = []
        self.registry: InstrumentRegistry
        self.calendar: TradingCalendar
        self.products_cfg: ProductsConfig

    # universe -----------------------------------------------------------
    def products(self) -> list[str]:
        raise NotImplementedError

    def days(self) -> list[str]:
        raise NotImplementedError

    def warmup_bars(self, product: str, before: str, n: int) -> list[DailyBar]:
        return []

    def plan(self, td: str, pre_settle: Mapping[str, float], held: set[str], extras: Mapping[str, Mapping[str, str]]) -> DayPlan:
        raise NotImplementedError

    def next_trading_day(self, td: str) -> str:
        d = parse_td(td)
        try:
            return td_str(self.calendar.next_trading_day(d))
        except LookupError:
            nd = d + timedelta(days=1)
            while nd.weekday() >= 5:
                nd += timedelta(days=1)
            return td_str(nd)

    def initial_pre_settle(self, td: str) -> dict[str, float]:
        return {}

    def file_hashes(self) -> dict[str, str]:
        return {}

    def settle_views(self, plan: DayPlan, replayed: Mapping[str, list[Point]]) -> dict[str, SettlementPriceView]:
        """Settlement price rows of every contract of the plan (official, or computed from the tape)."""
        out: dict[str, SettlementPriceView] = {}
        for inst, bar in plan.bars.items():
            pts = replayed.get(inst, [])
            if plan.compute_settle and pts:
                out[inst] = tape_view(self.registry, inst, plan.trading_day, pts, bar.pre_settle or pts[0].price)
            elif bar.settle is not None:
                out[inst] = official_view(bar, self.registry)
            else:
                ps = bar.pre_settle if bar.pre_settle is not None else (pts[-1].price if pts else None)
                if ps is None:
                    continue
                out[inst] = SettlementPriceView(trading_day=plan.trading_day, instrument_id=inst, price=f"{D(ps)}",
                                                source=SettlePriceSource.PRE_SETTLE, rounding=Rounding.NONE)
        return out


def _build_universe(
    base: ProductsConfig, calendar: TradingCalendar, instruments: Iterable[str], hist: HistData | None
) -> tuple[ProductsConfig, InstrumentRegistry]:
    listed = list_instruments(
        base,
        instruments,
        calendar,
        first_day=hist.contract_first_day if hist is not None else None,
        last_day=hist.contract_last_day if hist is not None else None,
    )
    return listed, InstrumentRegistry(listed, calendar)


# ------------------------------------------------------------------ layer A
class DailySource(DataSource):
    """Layer A: synthesized intraday paths from daily bars, official settlement prices.

    ``path_model`` + ``path_index`` select realization ``path_index`` of the stochastic path ensemble
    (:mod:`futrisk.backtest.pathmodel`); without them every contract replays the deterministic path of
    ``cfg.path.rule``. ``products`` restricts the replay to the products a task's accounts trade (the path of a
    contract never depends on the other contracts, so a subset replays the same paths).
    """

    layer = "A"

    def __init__(self, hist: HistData, cfg: BacktestConfig, base_calendar: TradingCalendar | None, *,
                 far_products: Iterable[str] = (), products: Iterable[str] | None = None,
                 path_model: PathModel | None = None, path_index: int | None = None) -> None:
        super().__init__()
        self.hist = hist
        self.cfg = cfg
        self.only = set(products) if products is not None else None
        self.path_model = path_model
        self.path_index = path_index
        self.notes.extend(hist.notes)
        self.calendar = hist.calendar(base_calendar)
        insts: set[str] = set(hist.all_mains())
        for p in far_products:
            if p in hist.series:
                insts.update(hist.instruments_of(p))
        self.products_cfg, self.registry = _build_universe(hist.base_products, self.calendar, insts, hist)
        self._grid: dict[str, TickGrid] = {}
        self._timelines: dict[tuple[str, str], Timeline | None] = {}

    @property
    def ensemble(self) -> bool:
        return self.path_model is not None and self.path_index is not None

    def products(self) -> list[str]:
        return [p for p in self.hist.products if self.only is None or p in self.only]

    def days(self) -> list[str]:
        days = self.hist.trading_days(self.cfg.start, self.cfg.end)
        period = self.cfg.data.minute_window
        if period:
            common = minute_common_days(self.hist, period)
            days = [d for d in days if d in common]
        return days

    def grid(self, inst: str) -> TickGrid:
        g = self._grid.get(inst)
        if g is None:
            g = TickGrid(self.registry.tick_size(inst))
            self._grid[inst] = g
        return g

    def timeline(self, inst: str, td: str) -> Timeline | None:
        spec = self.registry.product_spec(inst)
        key = ("|".join(spec.sessions), td)
        if key not in self._timelines:
            self._timelines[key] = session_timeline(self.registry, inst, td)
        return self._timelines[key]

    def warmup_bars(self, product: str, before: str, n: int) -> list[DailyBar]:
        if product not in self.hist.series or n <= 0:
            return []
        bars = self.hist.daily_bars(product, until=None)
        prior = [b for b in bars if b.trading_day < before]
        return prior[-n:]

    def initial_pre_settle(self, td: str) -> dict[str, float]:
        out: dict[str, float] = {}
        for p in self.products():
            m = self.hist.main(p, td)
            if m is None:
                continue
            b = self.hist.bar(td, m)
            if b is not None and b.pre_settle is not None:
                out[m] = b.pre_settle
        return out

    def file_hashes(self) -> dict[str, str]:
        return self.hist.file_hashes()

    def _mains_and_rolls(self, td: str) -> tuple[dict[str, str], dict[str, float], dict[str, tuple[str, str]]]:
        mains: dict[str, str] = {}
        factors: dict[str, float] = {}
        rolls: dict[str, tuple[str, str]] = {}
        for p in self.products():
            s = self.hist.series[p]
            i = s.index.get(td)
            if i is None:
                continue
            mains[p] = s.mains[i]
            factors[p] = s.roll_factor(td)
            if s.rolls[i] and i > 0:
                rolls[p] = (s.mains[i - 1], s.mains[i])
        return mains, factors, rolls

    def _bars_for(self, td: str, insts: Iterable[str], pre_settle: Mapping[str, float]) -> dict[str, Bar]:
        out: dict[str, Bar] = {}
        for inst in sorted(set(insts)):
            try:
                out[inst] = self.hist.instrument_bar(td, inst, pre_settle.get(inst))
            except KeyError:
                self.notes.append(f"{td} {inst}: no bar and no previous settle; not replayed")
        return out

    def _next_pre(self, td: str, bars: Mapping[str, Bar]) -> dict[str, float]:
        """Today's settle of tomorrow's mains (their previous settlement price tomorrow)."""
        out: dict[str, float] = {}
        for p in self.products():
            s = self.hist.series[p]
            nd = s.next_day(td)
            if nd is None or td not in s.index:
                continue
            nm = s.main_of(nd)
            if nm is None or nm in bars:
                continue
            tb = self.hist.bar(td, nm)
            if tb is not None and tb.settle is not None:
                out[nm] = tb.settle
                continue
            nb = self.hist.bar(nd, nm)
            if nb is not None and nb.pre_settle is not None:
                out[nm] = nb.pre_settle
        return out

    def _extras(self, td: str, mains: Mapping[str, str], extras: Mapping[str, Mapping[str, str]]) -> dict[str, dict[str, str]]:
        """Resolve ``{alias: "far:<product>"}`` requests to today's far contract of that product."""
        out: dict[str, dict[str, str]] = {}
        for acct, req in extras.items():
            for alias, what in req.items():
                kind, _, prod = what.partition(":")
                if kind == "far" and prod in mains:
                    far = self.hist.far_contract(prod, td, mains[prod])
                    if far is not None:
                        out.setdefault(acct, {})[alias] = far
        return out

    def _legs(self, bar: Bar) -> tuple[int, int, int]:
        pp = self.cfg.path
        return leg_counts(bar.open or 0.0, bar.high or 0.0, bar.low or 0.0, bar.close or 0.0, points_per_leg=pp.points_per_leg,
                          max_step=pp.max_step, max_points=pp.max_points_per_leg, rule=pp.rule)

    def _synth(self, bar: Bar) -> tuple[list[Point], tuple[float, float]]:
        """The states of one contract-day: a function of this contract's bar (and the realization) only."""
        inst, td = bar.instrument_id, bar.trading_day
        tl = self.timeline(inst, td)
        if tl is None or not bar.traded:
            return [], (0.0, 0.0)
        grid = self.grid(inst)
        o, h, lo, c = bar.open or 0.0, bar.high or 0.0, bar.low or 0.0, bar.close or 0.0
        if self.path_model is not None and self.path_index is not None:
            pts = ensemble_points(self.path_model, self.path_index, inst, bar.product, td, o, h, lo, c, timeline=tl,
                                  grid=grid, multiplier=self.registry.multiplier(inst), max_step=self.cfg.path.max_step)
        else:
            pts = synthesize(inst, tl, o, h, lo, c, grid=grid, multiplier=self.registry.multiplier(inst),
                             points_per_leg=self.cfg.path.points_per_leg, rule=self.cfg.path.rule, legs=self._legs(bar))
        upper, lower, up_lock, down_lock = lock_prices(bar, self.registry, grid)
        mark_locks(pts, up_lock, down_lock, grid)
        return pts, (upper, lower)

    def plan(self, td: str, pre_settle: Mapping[str, float], held: set[str], extras: Mapping[str, Mapping[str, str]]) -> DayPlan:
        mains, factors, rolls = self._mains_and_rolls(td)
        ex = self._extras(td, mains, extras)
        insts = set(mains.values()) | set(held) | {i for d in ex.values() for i in d.values()}
        bars = self._bars_for(td, insts, pre_settle)
        points: list[Point] = []
        limits: dict[str, tuple[float, float]] = {}
        for inst, bar in bars.items():
            pts, lim = self._synth(bar)
            if pts:
                points.extend(pts)
                limits[inst] = lim
        points.sort(key=lambda p: (p.ts_ns, p.instrument_id))
        daily = {}
        for p in mains:
            s = self.hist.series[p]
            i = s.index[td]
            daily[p] = DailyBar(td, s.mains[i], s.open[i], s.high[i], s.low[i], s.close[i], s.settle[i], s.roll_factor(td), s.rolls[i])
        return DayPlan(
            trading_day=td,
            points=points,
            bars=bars,
            mains=mains,
            roll_factors=factors,
            rolls=rolls,
            limits=limits,
            extras=ex,
            next_pre_settle=self._next_pre(td, bars),
            daily_bars=daily,
        )


# ------------------------------------------------------------------ layer B
class MinuteSource(DailySource):
    """Layer B: minute bars expanded to O, E1, E2, C; official daily settlement prices."""

    layer = "B"

    def __init__(self, hist: HistData, cfg: BacktestConfig, base_calendar: TradingCalendar | None, *,
                 far_products: Iterable[str] = (), products: Iterable[str] | None = None) -> None:
        self.period = cfg.data.minute_period
        self._minute: dict[str, dict[str, list[Any]]] = {}
        minute_insts: set[str] = set()
        far_products = list(far_products)
        for p in hist.products:
            try:
                bars = hist.minute_bars(p, self.period)
            except FileNotFoundError:
                hist.notes.append(f"{p}: minute/{p}_{self.period}m.parquet missing; product skipped in layer B")
                continue
            by_day: dict[str, list[Any]] = {}
            for b in bars:
                if b.instrument_id is None:
                    continue
                by_day.setdefault(b.trading_day, []).append(b)
                minute_insts.add(b.instrument_id)
            self._minute[p] = by_day
        super().__init__(hist, cfg, base_calendar, far_products=far_products, products=products)
        if minute_insts - set(self.products_cfg.instrument_ids()):
            insts = set(hist.all_mains()) | minute_insts
            for p in far_products:  # calendar-spread far legs (synthesized: Sina has minute bars of the main only)
                if p in hist.series:
                    insts.update(hist.instruments_of(p))
            self.products_cfg, self.registry = _build_universe(hist.base_products, self.calendar, insts, hist)

    def products(self) -> list[str]:
        return [p for p in self.hist.products if p in self._minute and (self.only is None or p in self.only)]

    def days(self) -> list[str]:
        lo = self.cfg.start or "00000000"
        hi = self.cfg.end or "99999999"
        # only days that every loaded product with minute data covers (a common window; the same for every task,
        # whatever products its accounts trade); a file's first day is usually partial and is left out
        common = None
        for by_day in self._minute.values():
            ds = set(by_day)
            ds.discard(min(ds, default=""))
            common = ds if common is None else common & ds
        out = sorted(d for d in (common or set()) if lo <= d <= hi and d in self.hist.all_days)
        return out

    def plan(self, td: str, pre_settle: Mapping[str, float], held: set[str], extras: Mapping[str, Mapping[str, str]]) -> DayPlan:
        mains: dict[str, str] = {}
        factors: dict[str, float] = {}
        rolls: dict[str, tuple[str, str]] = {}
        minute_pts: dict[str, list[Point]] = {}
        for p in self.products():
            day_bars = self._minute[p].get(td, [])
            if not day_bars:
                continue
            inst = day_bars[0].instrument_id
            mains[p] = inst
            prev = self.hist.series[p].prev_day(td)
            prev_bars = self._minute[p].get(prev, []) if prev else []
            prev_inst = prev_bars[0].instrument_id if prev_bars else None
            if prev_inst is not None and prev_inst != inst:
                rolls[p] = (prev_inst, inst)
                pb = self.hist.bar(td, inst)
                ob = self.hist.bar(prev, prev_inst) if prev else None
                if pb is not None and pb.pre_settle and ob is not None and ob.settle:
                    factors[p] = pb.pre_settle / ob.settle
            tl = self.timeline(inst, td)
            if tl is None:
                continue
            minute_pts[inst] = minute_bar_points(
                inst, tl, [(b.ts_ns, b.open, b.high, b.low, b.close) for b in day_bars if b.instrument_id == inst],
                period_min=self.period, multiplier=self.registry.multiplier(inst), rule=self.cfg.path.rule,
            )
        ex = self._extras(td, mains, extras)
        insts = set(mains.values()) | set(held) | {i for d in ex.values() for i in d.values()}
        bars = self._bars_for(td, insts, pre_settle)
        points: list[Point] = []
        limits: dict[str, tuple[float, float]] = {}
        for inst, bar in bars.items():
            pts = minute_pts.get(inst)
            if pts:
                grid = self.grid(inst)
                if bar.traded:
                    upper, lower, up_lock, down_lock = lock_prices(bar, self.registry, grid)
                    lo = min(p.price for p in pts)
                    hi = max(p.price for p in pts)
                    upper, lower = max(upper, hi), min(lower, lo)
                    mark_locks(pts, up_lock, down_lock, grid)
                else:
                    upper, lower = max(p.price for p in pts), min(p.price for p in pts)
                points.extend(pts)
                limits[inst] = (upper, lower)
            else:
                spts, lim = self._synth(bar)
                if spts:
                    points.extend(spts)
                    limits[inst] = lim
        points.sort(key=lambda p: (p.ts_ns, p.instrument_id))
        daily: dict[str, DailyBar] = {}
        for p, inst in mains.items():
            b = self.hist.bar(td, inst)
            if b is not None and b.traded and b.settle is not None:
                daily[p] = DailyBar(td, inst, b.open or 0.0, b.high or 0.0, b.low or 0.0, b.close or 0.0, b.settle,
                                    factors.get(p, 1.0), p in rolls)
        nxt: dict[str, float] = {}
        for p in self.products():
            nd = self.hist.series[p].next_day(td)
            nb = self._minute[p].get(nd, []) if nd else []
            if nb and nb[0].instrument_id not in bars:
                tb = self.hist.bar(td, nb[0].instrument_id)
                if tb is not None and tb.settle is not None:
                    nxt[nb[0].instrument_id] = tb.settle
        return DayPlan(trading_day=td, points=points, bars=bars, mains=mains, roll_factors=factors, rolls=rolls,
                       limits=limits, extras=ex, next_pre_settle=nxt, daily_bars=daily)

    def minute_days(self, product: str) -> dict[str, list[tuple[float, float, float, float]]]:
        """``{trading_day: [(o, h, l, c), ...]}`` of the Sina main (hit-rate input)."""
        out: dict[str, list[tuple[float, float, float, float]]] = {}
        for td, bars in self._minute.get(product, {}).items():
            inst = bars[0].instrument_id
            out[td] = [(b.open, b.high, b.low, b.close) for b in bars if b.instrument_id == inst]
        return out


# ------------------------------------------------------------------ layer C
class TickSource(DataSource):
    """Layer C: recorded Sina ticks (LIVE rows) replayed as recorded; settlement computed from the tape."""

    layer = "C"

    def __init__(self, raw_dir: Path, base: ProductsConfig, base_calendar: TradingCalendar, *,
                 days: Sequence[str] = (), products: Sequence[str] = (), hist: HistData | None = None) -> None:
        super().__init__()
        from futrisk.marketdata.recorder import list_trading_days

        self.raw_dir = Path(raw_dir)
        self.hist = hist
        found = list_trading_days(self.raw_dir)
        self._days = [d for d in found if not days or d in set(days)]
        if not self._days:
            self.notes.append(f"no recorded ticks under {self.raw_dir}")
        self._products_filter = set(products)
        self._ticks: dict[str, dict[str, list[Point]]] = {}
        self._pre: dict[str, dict[str, float]] = {}
        self._files: dict[str, Path] = {}
        self._file_sha: dict[str, str] = {}
        insts: set[str] = set()
        for td in self._days:
            per = self._load_day(td)
            self._ticks[td] = per
            insts.update(per)
        tdays = {parse_td(d) for d in self._days}
        tdays.update(base_calendar.trading_days_between(base_calendar.first_day, base_calendar.last_day))
        self.calendar = TradingCalendar(sorted(tdays), parse_templates(base.session_templates))
        self.products_cfg, self.registry = _build_universe(base, self.calendar, insts, hist)

    def _load_day(self, td: str) -> dict[str, list[Point]]:
        from futrisk.backtest.data import sha256_file
        from futrisk.marketdata.recorder import list_shards, read_raw_ticks

        for p in list_shards(self.raw_dir, td):
            rel = f"ticks/raw/{td}/sina/{p.name}"
            self._files[rel] = p
            self._file_sha[rel] = sha256_file(p)  # hashed when read (a recorder may still be appending shards)
        df = read_raw_ticks(self.raw_dir, td)
        out: dict[str, list[Point]] = {}
        pre: dict[str, float] = {}
        if not len(df):
            return out
        df = df[df["mode"].astype(str) == "LIVE"]
        df = df.sort_values(["exch_ts_ns", "recv_ns"], kind="stable")
        for r in df.itertuples(index=False):
            inst = str(r.instrument_id)
            if self._products_filter and product_code(inst) not in self._products_filter:
                continue
            flags = int(r.flags) if r.flags is not None and not (isinstance(r.flags, float) and math.isnan(r.flags)) else 0
            bid = float(r.bid1) if r.bid1 is not None and r.bid1 == r.bid1 else None
            ask = float(r.ask1) if r.ask1 is not None and r.ask1 == r.ask1 else None
            out.setdefault(inst, []).append(
                Point(int(r.exch_ts_ns), inst, float(r.last), "T", int(r.volume), float(r.turnover), flags, bid, ask)
            )
            if r.pre_settle is not None and r.pre_settle == r.pre_settle:
                pre[inst] = float(r.pre_settle)
        self._pre[td] = pre
        return out

    def products(self) -> list[str]:
        return sorted({product_code(i) for per in self._ticks.values() for i in per})

    def days(self) -> list[str]:
        return list(self._days)

    def warmup_bars(self, product: str, before: str, n: int) -> list[DailyBar]:
        if self.hist is None or product not in self.hist.series or n <= 0:
            return []
        return [b for b in self.hist.daily_bars(product) if b.trading_day < before][-n:]

    def initial_pre_settle(self, td: str) -> dict[str, float]:
        return dict(self._pre.get(td, {}))

    def file_hashes(self) -> dict[str, str]:
        return dict(sorted(self._file_sha.items()))

    def main_contracts(self, td: str) -> dict[str, str]:
        """Main contract per product on ``td``, chosen with information from before the open (no look-ahead):
        the largest final volume of the previous recorded day; on the first recorded day the daily data's main
        (``data/hist``) when it was recorded, else the first contract id."""
        per = self._ticks.get(td, {})
        i = self._days.index(td) if td in self._days else -1
        prev = self._ticks.get(self._days[i - 1], {}) if i > 0 else {}
        prev_vol = {inst: pts[-1].volume for inst, pts in prev.items() if pts}
        by_product: dict[str, list[str]] = {}
        for inst in per:
            by_product.setdefault(product_code(inst), []).append(inst)
        mains: dict[str, str] = {}
        for p, cands in sorted(by_product.items()):
            known = [(prev_vol[c], c) for c in cands if c in prev_vol]
            if known:
                mains[p] = max(known)[1]
                continue
            hm = self.hist.main(p, td) if self.hist is not None else None
            mains[p] = hm if hm in cands else min(cands)
        return mains

    def plan(self, td: str, pre_settle: Mapping[str, float], held: set[str], extras: Mapping[str, Mapping[str, str]]) -> DayPlan:
        per = self._ticks.get(td, {})
        mains = self.main_contracts(td)
        bars: dict[str, Bar] = {}
        points: list[Point] = []
        limits: dict[str, tuple[float, float]] = {}
        pre = self._pre.get(td, {})
        for inst, pts in per.items():
            prices = [x.price for x in pts]
            ps = pre.get(inst, pre_settle.get(inst, prices[0]))
            bars[inst] = Bar(td, inst, product_code(inst), prices[0], max(prices), min(prices), prices[-1], None, "tape",
                             ps, "exchange", pts[-1].volume)
            up, lo = self.registry.limit_prices(inst, D(ps), td)
            limits[inst] = (max(float(up), max(prices)), min(float(lo), min(prices)))
            points.extend(pts)
        for inst in held - set(per):
            flat = pre_settle.get(inst)
            if flat is not None:
                bars[inst] = Bar(td, inst, product_code(inst), None, None, None, None, flat, "flat", flat, "series", 0)
        points.sort(key=lambda p: (p.ts_ns, p.instrument_id))
        daily: dict[str, DailyBar] = {}
        for p, inst in mains.items():
            b = bars[inst]
            daily[p] = DailyBar(td, inst, b.open or 0.0, b.high or 0.0, b.low or 0.0, b.close or 0.0, b.close or 0.0, 1.0, False)
        return DayPlan(trading_day=td, points=points, bars=bars, mains=mains, limits=limits, daily_bars=daily,
                       compute_settle=True)


# ------------------------------------------------------------------ layer D
class ScenarioSource(DataSource):
    """Layer D: a synthetic scenario (one trading day of explicit market states)."""

    layer = "D"

    def __init__(self, spec: ScenarioSpec, base: ProductsConfig, base_calendar: TradingCalendar) -> None:
        super().__init__()
        self.spec = spec
        self.calendar = base_calendar
        insts = set(spec.reference)
        self.products_cfg, self.registry = _build_universe(base, self.calendar, insts, None)

    def products(self) -> list[str]:
        return sorted({product_code(i) for i in self.spec.reference})

    def days(self) -> list[str]:
        return [self.spec.trading_day]

    def initial_pre_settle(self, td: str) -> dict[str, float]:
        return dict(self.spec.reference)

    def start_ns(self) -> int:
        d = parse_td(self.spec.trading_day)
        hh, mm, *ss = (int(x) for x in self.spec.start.split(":"))
        return bj_to_ns(datetime(d.year, d.month, d.day, hh, mm, ss[0] if ss else 0, tzinfo=BJ))

    def _move_of(self, inst: str, move: Mapping[str, float]) -> float | None:
        info = self.registry.get(inst)
        for key in (inst, info.product, info.sector, "all"):
            if key in move:
                return move[key]
        return None

    def plan(self, td: str, pre_settle: Mapping[str, float], held: set[str], extras: Mapping[str, Mapping[str, str]]) -> DayPlan:
        t0 = self.start_ns()
        points: list[Point] = []
        labels: dict[int, str] = {}
        scripted: dict[int, list[dict[str, Any]]] = {}
        level = dict.fromkeys(self.spec.reference, 0.0)
        vol = dict.fromkeys(self.spec.reference, 0)
        turn = dict.fromkeys(self.spec.reference, 0.0)
        for k, step in enumerate(self.spec.steps):
            ts = t0 + int(step.at_s * 1_000_000_000)
            labels[ts] = step.label
            orders = [dict(o, step=step.label) for o in step.orders]
            if k == 0 and self.spec.book and not self.spec.use_strategy:
                book = [{"account": self.spec.account, "instrument": inst, "side": "BUY" if q > 0 else "SELL",
                         "offset": "OPEN", "qty": abs(q), "label": f"book:{inst}", "step": step.label}
                        for inst, q in sorted(self.spec.book.items())]
                orders = book + orders
            if orders:
                scripted[ts] = orders
            for inst, ref in sorted(self.spec.reference.items()):
                mv = self._move_of(inst, step.move)
                if mv is not None:
                    level[inst] = mv
                grid = TickGrid(self.registry.tick_size(inst))
                px = grid.round(ref * (1.0 + level[inst]))
                vol[inst] += 1
                turn[inst] += px * self.registry.multiplier(inst)
                bid, ask = px - grid.tick, px + grid.tick
                kind = KIND_OPEN if vol[inst] == 1 else ("S" if step is not self.spec.steps[-1] else KIND_CLOSE)
                points.append(Point(ts, inst, px, kind, vol[inst], turn[inst], 0, bid, ask))
        points.sort(key=lambda p: (p.ts_ns, p.instrument_id))
        bars: dict[str, Bar] = {}
        limits: dict[str, tuple[float, float]] = {}
        for inst, ref in self.spec.reference.items():
            prices = [p.price for p in points if p.instrument_id == inst]
            bars[inst] = Bar(td, inst, product_code(inst), prices[0], max(prices), min(prices), prices[-1], None, "tape",
                             ref, "exchange", len(prices))
            up, lo = self.registry.limit_prices(inst, D(ref), td)
            limits[inst] = (max(float(up), max(prices)), min(float(lo), min(prices)))
        mains = {product_code(i): i for i in sorted(self.spec.reference)}
        return DayPlan(trading_day=td, points=points, bars=bars, mains=mains, limits=limits, labels=labels,
                       scripted=scripted, compute_settle=True)


__all__ = [
    "LOCK_MIN_MOVE",
    "DailySource",
    "DataSource",
    "DayPlan",
    "MinuteSource",
    "ScenarioSource",
    "TickSource",
    "lock_prices",
    "mark_locks",
    "official_view",
    "tape_view",
]
