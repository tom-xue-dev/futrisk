"""Bar-based strategies of the five demo accounts for daily / minute replays (01 §7.1 account styles).

The live bots (:mod:`futrisk.sim.bots.strategies`) run on demo time scales
(20-second bars, a five-minute book build); these are their equivalents on
trading days, driven by the replayed market states. Every strategy returns
**target net positions** (signed lots per contract); the
:class:`~futrisk.backtest.executor.Executor` turns them into orders through the
engine's pre-trade path. Signals use the main series back-adjusted at every roll
(``DailyBar.roll_factor`` = new contract's previous settle ÷ old contract's
settle; for DCE, whose series has no new-contract previous settle, the loader's
estimate - :meth:`futrisk.backtest.data.ProductSeries.roll_source`), so a roll
gap does not look like a breakout or a moving-average cross; positions are
always held in the day's main contract.

========  ======================  ===================================================
account   strategy                logic (daily bars)
========  ======================  ===================================================
ACC1      ``channel_breakout``    Donchian breakout: long above the ``entry_n``-day
                                  high, short below the low, checked at every market
                                  state (stop entries); exit on the ``exit_n`` channel
ACC2      ``multi_trend``         SMA(fast) vs SMA(slow) of daily closes per product,
                                  rebalanced at the next open (daily-rebalanced trend)
ACC3      ``counter_trend_add``   long black-sector book (rb .39 / hc .17 / i .22 of
                                  equity) re-established at every open; adds 10% of the
                                  base per 0.4% drop from the open (max 5 adds), sells
                                  an add back half a step higher (the breaker account)
ACC4      ``index_directional``   IF/IC momentum (SMA fast/slow), leverage 0.3-0.55x
                                  scaled by the signal strength, rebalanced at the open
ACC5      ``rb_calendar_spread``  main / next contract spread z-score over 60 days,
                                  permanent two-sided book (base 20 lots per leg)
========  ======================  ===================================================

Sizing keeps every product under ``cap_util`` × its exposure cap
``0.03 / limit_pct`` of equity, as the live bots do (01 §8.14). The sizing base
(``StrategyContext.equity``) is the account's initial capital by default
(``BacktestConfig.sizing = "initial"``): the paired arms then hold the same book
again after every rebalance, so a difference between arms is caused by the rule
and not by a book size that drifted with an earlier intervention.
"""

from __future__ import annotations

import math
from collections import deque
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, ClassVar

from pydantic import BaseModel, ConfigDict, Field

from futrisk.backtest.data import DailyBar
from futrisk.core.instruments import InstrumentRegistry
from futrisk.core.metrics import LIMIT_LOSS_BUDGET
from futrisk.messages.enums import AccountState


def _sign(x: float) -> int:
    return (x > 0) - (x < 0)


@dataclass
class Quote:
    """What a strategy sees of one product at one market state."""

    product: str
    instrument_id: str  # today's main contract
    price: float | None  # latest price of the main today (None before its first state)
    open: float | None  # today's first price of the main
    live: bool  # the main has a state at the current timestamp (orders can trade now)
    first: bool  # this is the main's first state of the day


@dataclass
class StrategyContext:
    """Read-only view for a strategy at one market state (engine numbers, no recomputation)."""

    account_id: str
    trading_day: str
    now_ns: int
    equity: float
    registry: InstrumentRegistry
    quotes: dict[str, Quote]
    positions: dict[str, int]  # instrument -> net lots (engine book)
    state: AccountState = AccountState.NORMAL
    halted_since_ns: int | None = None
    extra: dict[str, Quote] = field(default_factory=dict)  # non-main contracts the strategy asked for (far legs)

    def per_lot(self, instrument_id: str, price: float) -> float:
        return price * self.registry.multiplier(instrument_id)

    def lots_for(self, instrument_id: str, notional: float, price: float) -> int:
        per = self.per_lot(instrument_id, price)
        return max(0, math.floor(notional / per + 1e-9)) if per > 0 and notional > 0 else 0

    def cap_frac(self, instrument_id: str) -> float:
        pct = float(self.registry.product_spec(instrument_id).limit_pct_on(self.trading_day))
        return LIMIT_LOSS_BUDGET / pct if pct > 0 else 1.0

    def cap_lots(self, instrument_id: str, price: float, util: float) -> int:
        return self.lots_for(instrument_id, util * self.cap_frac(instrument_id) * self.equity, price)

    def net_product(self, product: str) -> int:
        return sum(n for inst, n in self.positions.items() if self.registry.product(inst) == product)

    @property
    def halted(self) -> bool:
        return self.state == AccountState.HALTED


def sized_lots(ctx: StrategyContext, instrument_id: str, price: float, frac: float, cap_util: float) -> int:
    """Lots for ``frac`` of equity, clipped to the product cap, at least 1 when the cap allows it."""
    lots = min(ctx.lots_for(instrument_id, frac * ctx.equity, price), ctx.cap_lots(instrument_id, price, cap_util))
    if lots == 0 and frac > 0 and ctx.cap_lots(instrument_id, price, cap_util) >= 1:
        lots = 1
    return lots


class _Params(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class Strategy:
    """Base class. ``products`` are traded in their main contract; targets are keyed by contract."""

    name: ClassVar[str] = "strategy"
    params_model: ClassVar[type[_Params]] = _Params

    def __init__(self, params: _Params, *, account_id: str, products: list[str]) -> None:
        self.params = params
        self.account_id = account_id
        self.products = products

    # history ------------------------------------------------------------
    def on_roll(self, product: str, factor: float) -> None:  # noqa: B027 - optional hook
        """A new main contract today: rescale price history by ``factor`` (back-adjustment)."""

    def on_bar(self, product: str, bar: DailyBar) -> None:  # noqa: B027 - optional hook
        """A completed daily bar of the main series (also used for the warm-up)."""

    # trading ------------------------------------------------------------
    def begin_day(self, trading_day: str) -> None:  # noqa: B027 - optional hook
        """Before the first market state of a trading day."""

    def targets(self, ctx: StrategyContext) -> dict[str, int] | None:
        """Target net lots per contract at this market state (None: no change)."""
        raise NotImplementedError

    def extra_instruments(self, trading_day: str, mains: Mapping[str, str]) -> dict[str, str]:
        """Non-main contracts needed today, ``{alias: instrument}`` (e.g. a spread's far leg)."""
        return {}

    def describe(self) -> dict[str, Any]:
        return {}


class _Hist:
    """Back-adjusted price history of one product (multiplicative adjustment at every roll)."""

    def __init__(self, n: int) -> None:
        self.highs: deque[float] = deque(maxlen=n)
        self.lows: deque[float] = deque(maxlen=n)
        self.closes: deque[float] = deque(maxlen=n)

    def rescale(self, k: float) -> None:
        if k == 1.0:
            return
        for q in (self.highs, self.lows, self.closes):
            vals = [v * k for v in q]
            q.clear()
            q.extend(vals)

    def add(self, bar: DailyBar) -> None:
        self.highs.append(bar.high)
        self.lows.append(bar.low)
        self.closes.append(bar.close)


# ====================================================================== ACC1
class BreakoutParams(_Params):
    entry_n: int = 20
    exit_n: int = 10
    notional_frac: float = 0.12
    cap_util: float = 0.85
    allow_short: bool = True


class ChannelBreakout(Strategy):
    """ACC1 黑色系通道突破: Donchian channel on daily bars, entries checked at every market state."""

    name = "channel_breakout"
    params_model = BreakoutParams

    def __init__(self, params: BreakoutParams, *, account_id: str, products: list[str]) -> None:
        super().__init__(params, account_id=account_id, products=products)
        self.p = params
        self.hist = {pr: _Hist(params.entry_n) for pr in products}
        self.side = dict.fromkeys(products, 0)
        self.lots = dict.fromkeys(products, 0)

    def on_roll(self, product: str, factor: float) -> None:
        if product in self.hist:
            self.hist[product].rescale(factor)

    def on_bar(self, product: str, bar: DailyBar) -> None:
        if product in self.hist:
            self.hist[product].add(bar)

    def targets(self, ctx: StrategyContext) -> dict[str, int] | None:
        out: dict[str, int] = {}
        for pr in self.products:
            q = ctx.quotes.get(pr)
            h = self.hist[pr]
            if q is None or q.price is None or len(h.highs) < self.p.entry_n:
                continue
            price = q.price
            upper, lower = max(h.highs), min(h.lows)
            ex = list(h.highs)[-self.p.exit_n :], list(h.lows)[-self.p.exit_n :]
            exit_hi, exit_lo = max(ex[0]), min(ex[1])
            new = self.side[pr]
            if price > upper:
                new = 1
            elif price < lower and self.p.allow_short:
                new = -1
            elif self.side[pr] > 0 and price < exit_lo or self.side[pr] < 0 and price > exit_hi:
                new = 0
            if new != self.side[pr]:
                self.lots[pr] = sized_lots(ctx, q.instrument_id, price, self.p.notional_frac, self.p.cap_util) if new else 0
                self.side[pr] = new
            out[q.instrument_id] = self.side[pr] * self.lots[pr]
        return out

    def describe(self) -> dict[str, Any]:
        return {"side": dict(self.side), "lots": dict(self.lots)}


# ====================================================================== ACC2
class TrendParams(_Params):
    fast: int = 8
    slow: int = 24
    band: float = 0.0005
    notional_frac: float = 0.10
    cap_util: float = 0.85


class MultiTrend(Strategy):
    """ACC2 多品种趋势: daily SMA crossover per product, the target is set at the next open."""

    name = "multi_trend"
    params_model = TrendParams

    def __init__(self, params: TrendParams, *, account_id: str, products: list[str]) -> None:
        super().__init__(params, account_id=account_id, products=products)
        if params.fast >= params.slow:
            raise ValueError("multi_trend: fast must be < slow")
        self.p = params
        self.hist = {pr: _Hist(params.slow) for pr in products}
        self.regime = dict.fromkeys(products, 0)
        self.lots = dict.fromkeys(products, 0)
        self.pending: dict[str, bool] = dict.fromkeys(products, False)

    def on_roll(self, product: str, factor: float) -> None:
        if product in self.hist:
            self.hist[product].rescale(factor)

    def on_bar(self, product: str, bar: DailyBar) -> None:
        h = self.hist.get(product)
        if h is None:
            return
        h.add(bar)
        if len(h.closes) < self.p.slow:
            return
        closes = list(h.closes)
        fast = sum(closes[-self.p.fast :]) / self.p.fast
        slow = sum(closes) / self.p.slow
        new = self.regime[product]
        if fast > slow * (1 + self.p.band):
            new = 1
        elif fast < slow * (1 - self.p.band):
            new = -1
        if new != self.regime[product]:
            self.regime[product] = new
            self.pending[product] = True  # size at the next open

    def targets(self, ctx: StrategyContext) -> dict[str, int] | None:
        out: dict[str, int] = {}
        for pr in self.products:
            q = ctx.quotes.get(pr)
            if q is None or q.price is None:
                continue
            if self.pending[pr] and q.live:
                self.lots[pr] = sized_lots(ctx, q.instrument_id, q.price, self.p.notional_frac, self.p.cap_util)
                self.pending[pr] = False
            out[q.instrument_id] = self.regime[pr] * self.lots[pr]
        return out

    def describe(self) -> dict[str, Any]:
        return {"regime": dict(self.regime), "lots": dict(self.lots)}


# ====================================================================== ACC3
class CounterTrendParams(_Params):
    weights: dict[str, float] = Field(default_factory=lambda: {"rb": 0.39, "hc": 0.17, "i": 0.22})
    dip_step: float = 0.004
    add_frac: float = 0.10
    max_adds: int = 5
    cap_util: float = 0.9
    sector_cap: float = 1.1
    halt_trim_frac: float = 0.0  # the live bot trims 10% once HALTED; the tiers are compared without it
    halt_trim_delay_s: float = 30.0


class CounterTrendAdd(Strategy):
    """ACC3 日内逆势加仓: a long black-sector book re-established at every open, ladder adds on intraday dips."""

    name = "counter_trend_add"
    params_model = CounterTrendParams

    def __init__(self, params: CounterTrendParams, *, account_id: str, products: list[str]) -> None:
        prods = [p for p in params.weights if p in products] if products else list(params.weights)
        super().__init__(params, account_id=account_id, products=prods)
        self.p = params
        self.base: dict[str, int] = dict.fromkeys(self.products, 0)
        self.ref: dict[str, float | None] = dict.fromkeys(self.products, None)
        self.adds: dict[str, int] = dict.fromkeys(self.products, 0)
        self.trim_done = False

    def begin_day(self, trading_day: str) -> None:
        self.base = dict.fromkeys(self.products, 0)
        self.ref = dict.fromkeys(self.products, None)
        self.adds = dict.fromkeys(self.products, 0)
        self.trim_done = False

    def _ladder(self, pr: str, price: float) -> None:
        ref = self.ref[pr]
        if ref is None or ref <= 0:
            return
        drop = (ref - price) / ref / self.p.dip_step
        down, up = math.floor(drop + 1e-9), math.floor(drop + 0.5 + 1e-9)
        cur = self.adds[pr]
        if down > cur:
            self.adds[pr] = min(down, self.p.max_adds)
        elif up < cur:
            self.adds[pr] = max(up, 0)

    def targets(self, ctx: StrategyContext) -> dict[str, int] | None:
        out: dict[str, int] = {}
        notional: dict[str, float] = {}
        for pr in self.products:
            q = ctx.quotes.get(pr)
            if q is None or q.price is None:
                continue
            if self.ref[pr] is None and q.live:
                # the day's reference and base book at the product's first state (the open)
                self.ref[pr] = q.price
                want = ctx.lots_for(q.instrument_id, self.p.weights[pr] * ctx.equity, q.price)
                self.base[pr] = min(want, ctx.cap_lots(q.instrument_id, q.price, self.p.cap_util))
            if self.ref[pr] is None:
                continue
            self._ladder(pr, q.price)
            t = self.base[pr]
            if self.adds[pr] > 0:
                t = math.floor(self.base[pr] * (1 + self.adds[pr] * self.p.add_frac))
                t = min(t, max(ctx.cap_lots(q.instrument_id, q.price, self.p.cap_util), self.base[pr]))
            out[q.instrument_id] = max(0, t)
            notional[q.instrument_id] = ctx.per_lot(q.instrument_id, q.price)
        # sector cap: scale the adds (never the base) so that the black book stays under sector_cap × equity
        limit = self.p.sector_cap * ctx.equity
        total = sum(notional[i] * n for i, n in out.items())
        if total > limit:
            base_of = {q.instrument_id: self.base[pr] for pr, q in ctx.quotes.items() if pr in self.base}
            base_total = sum(notional[i] * min(n, base_of.get(i, 0)) for i, n in out.items())
            extra = total - base_total
            k = max(0.0, (limit - base_total) / extra) if extra > 0 else 0.0
            for i, n in out.items():
                b = min(n, base_of.get(i, 0))
                out[i] = b + math.floor((n - b) * k)
        if (self.p.halt_trim_frac > 0 and ctx.halted and ctx.halted_since_ns is not None and not self.trim_done
                and ctx.now_ns - ctx.halted_since_ns >= self.p.halt_trim_delay_s * 1e9):
            self.trim_done = True
            for i in list(out):
                held = ctx.positions.get(i, 0)
                out[i] = min(out[i], math.floor(held * (1 - self.p.halt_trim_frac)))
        return out

    def describe(self) -> dict[str, Any]:
        return {"base": dict(self.base), "adds": dict(self.adds), "ref": dict(self.ref)}


# ====================================================================== ACC4
class IndexParams(_Params):
    weights: dict[str, float] = Field(default_factory=lambda: {"IF": 0.5, "IC": 0.5})
    fast: int = 6
    slow: int = 20
    band: float = 0.0003
    leverage_min: float = 0.3
    leverage_max: float = 0.55
    strength_scale: float = 0.002
    leverage_step: float = 0.05
    cap_util: float = 0.9


class IndexDirectional(Strategy):
    """ACC4 股指杠杆方向: one direction for the index book from daily momentum; sized at the open."""

    name = "index_directional"
    params_model = IndexParams

    def __init__(self, params: IndexParams, *, account_id: str, products: list[str]) -> None:
        prods = [p for p in params.weights if p in products] if products else list(params.weights)
        super().__init__(params, account_id=account_id, products=prods)
        if params.fast >= params.slow:
            raise ValueError("index_directional: fast must be < slow")
        self.p = params
        self.hist = {pr: _Hist(params.slow) for pr in self.products}
        self.mom: dict[str, float] = {}
        self.direction = 0
        self.leverage = 0.0
        self.lots: dict[str, int] = dict.fromkeys(self.products, 0)
        self.resize: dict[str, bool] = dict.fromkeys(self.products, False)

    def on_roll(self, product: str, factor: float) -> None:
        if product in self.hist:
            self.hist[product].rescale(factor)

    def on_bar(self, product: str, bar: DailyBar) -> None:
        h = self.hist.get(product)
        if h is None:
            return
        h.add(bar)
        if len(h.closes) >= self.p.slow:
            closes = list(h.closes)
            f = sum(closes[-self.p.fast :]) / self.p.fast
            s = sum(closes) / self.p.slow
            self.mom[product] = (f - s) / s if s > 0 else 0.0

    def begin_day(self, trading_day: str) -> None:
        if not self.mom:
            return
        m = sum(self.mom.values()) / len(self.mom)
        new_dir = self.direction
        if m > self.p.band:
            new_dir = 1
        elif m < -self.p.band:
            new_dir = -1
        strength = min(1.0, abs(m) / self.p.strength_scale) if self.p.strength_scale > 0 else 1.0
        lev = self.p.leverage_min + (self.p.leverage_max - self.p.leverage_min) * strength
        if self.p.leverage_step > 0:
            lev = round(lev / self.p.leverage_step) * self.p.leverage_step
        if new_dir != self.direction or abs(lev - self.leverage) >= self.p.leverage_step - 1e-9:
            self.direction, self.leverage = new_dir, lev
            self.resize = dict.fromkeys(self.products, True)

    def targets(self, ctx: StrategyContext) -> dict[str, int] | None:
        out: dict[str, int] = {}
        for pr in self.products:
            q = ctx.quotes.get(pr)
            if q is None or q.price is None:
                continue
            if self.resize.get(pr) and q.live:
                w = self.p.weights[pr] / sum(self.p.weights[x] for x in self.products)
                lots = ctx.lots_for(q.instrument_id, self.leverage * w * ctx.equity * len(self.products) / len(self.p.weights),
                                    q.price)
                self.lots[pr] = min(lots, ctx.cap_lots(q.instrument_id, q.price, self.p.cap_util))
                self.resize[pr] = False
            out[q.instrument_id] = self.direction * self.lots[pr]
        return out

    def describe(self) -> dict[str, Any]:
        return {"direction": self.direction, "leverage": self.leverage, "lots": dict(self.lots)}


# ====================================================================== ACC5
class SpreadParams(_Params):
    product: str = "rb"
    window: int = 60
    min_samples: int = 20
    entry_z: float = 1.5
    exit_z: float = 0.3
    base_lots: int = 20
    step_lots: int = 10
    max_lots: int = 60
    default_side: int = 1  # +1: long near / short far


class CalendarSpread(Strategy):
    """ACC5 螺纹跨期套利: (far − near) / near mean reversion with a permanent two-sided book."""

    name = "rb_calendar_spread"
    params_model = SpreadParams

    def __init__(self, params: SpreadParams, *, account_id: str, products: list[str]) -> None:
        super().__init__(params, account_id=account_id, products=[params.product])
        self.p = params
        self.spreads: deque[float] = deque(maxlen=params.window)
        self.side = 1 if params.default_side >= 0 else -1
        self.units = params.base_lots
        self.near_settle: float | None = None
        self.z: float | None = None
        self._far_px: dict[str, float] = {}

    def extra_instruments(self, trading_day: str, mains: Mapping[str, str]) -> dict[str, str]:
        return {"far": f"far:{self.p.product}"}  # resolved by the data source (the most active later contract)

    def on_spread(self, near_settle: float, far_settle: float) -> None:
        """End of day: record the settle spread of the pair held today and update the signal."""
        if near_settle <= 0:
            return
        s = (far_settle - near_settle) / near_settle
        self.spreads.append(s)
        if len(self.spreads) < self.p.min_samples:
            return
        mean = sum(self.spreads) / len(self.spreads)
        var = sum((x - mean) ** 2 for x in self.spreads) / len(self.spreads)
        sd = math.sqrt(var)
        if sd <= 1e-12:
            return
        z = (s - mean) / sd
        self.z = z
        if abs(z) >= self.p.entry_z:
            self.side = 1 if z > 0 else -1
            extra = self.p.step_lots * (1 + math.floor(abs(z) - self.p.entry_z))
            self.units = min(self.p.max_lots, self.p.base_lots + extra)
        elif abs(z) <= self.p.exit_z:
            self.units = self.p.base_lots

    def targets(self, ctx: StrategyContext) -> dict[str, int] | None:
        q = ctx.quotes.get(self.p.product)
        far = ctx.extra.get("far")
        if q is None or far is None or q.price is None or far.price is None:
            return None
        return {q.instrument_id: self.side * self.units, far.instrument_id: -self.side * self.units}

    def describe(self) -> dict[str, Any]:
        return {"side": self.side, "units": self.units, "z": self.z}


STRATEGIES: dict[str, type[Strategy]] = {
    ChannelBreakout.name: ChannelBreakout,
    MultiTrend.name: MultiTrend,
    CounterTrendAdd.name: CounterTrendAdd,
    IndexDirectional.name: IndexDirectional,
    CalendarSpread.name: CalendarSpread,
}

# the demo accounts' products (accounts.yaml) per strategy, when a config does not list them
DEFAULT_PRODUCTS: dict[str, list[str]] = {
    "channel_breakout": ["rb", "hc", "i", "j"],
    "multi_trend": ["rb", "cu", "au", "m", "IF", "T"],
    "counter_trend_add": ["rb", "hc", "i"],
    "index_directional": ["IF", "IC"],
    "rb_calendar_spread": ["rb"],
}


def make_strategy(name: str, params: Mapping[str, Any], *, account_id: str, products: list[str]) -> Strategy:
    cls = STRATEGIES.get(name)
    if cls is None:
        raise KeyError(f"unknown backtest strategy {name!r}; known: {', '.join(sorted(STRATEGIES))}")
    p = cls.params_model.model_validate(dict(params))
    return cls(p, account_id=account_id, products=products)


__all__ = [
    "DEFAULT_PRODUCTS",
    "STRATEGIES",
    "CalendarSpread",
    "ChannelBreakout",
    "CounterTrendAdd",
    "IndexDirectional",
    "MultiTrend",
    "Quote",
    "Strategy",
    "StrategyContext",
    "make_strategy",
    "sized_lots",
]
