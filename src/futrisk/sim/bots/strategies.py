"""Strategy logic of the five demo accounts (01 §7.1), as pure functions of the tick stream.

Each strategy turns ticks into **target net positions** (signed lots per instrument);
the :class:`~futrisk.sim.bots.executor.Executor` turns targets into orders. An
instrument missing from :meth:`Strategy.targets` means "no opinion, hold". Time is
market time from the ticks, randomness comes from the account's seeded RNG, so the
decisions are deterministic given the tick stream (and the fills).

Sizing stays inside the default risk rules in normal operation (``risk_rules.yaml``,
01 §8.14): the net notional of a product is kept below ``cap_util`` times its cap
``0.03 / limit_pct`` of equity (rb 60%, IF 30%, i 50% -> 37.5% from 2026-09-30), and the
total leverage well below 3x.

========  ======================  ===================================================
account   strategy                logic
========  ======================  ===================================================
ACC1      ``channel_breakout``    Donchian breakout on rb/hc/i/j bars: long above the
                                  ``entry_n``-bar high, short below the low, exit on
                                  the opposite ``exit_n``-bar channel
ACC2      ``multi_trend``         dual SMA crossover per product (rb/cu/au/m/IF/T)
ACC3      ``counter_trend_add``   builds a long black book early in the session
                                  (target 0.78x equity: rb 39%, hc 17%, i 22%) and adds
                                  on dips on a price ladder (6.25% of the base per
                                  0.5% down, to -4%); sells the adds back on rebounds;
                                  trims 10% once while HALTED and keeps the trimmed
                                  book until the day roll
ACC4      ``index_directional``   momentum direction on IF/IC with a 0.3-0.55x leverage
                                  target, inside the product caps
ACC5      ``rb_calendar_spread``  rb2701/rb2705 mean reversion on the spread z-score,
                                  always holding a two-sided book (single-side margin)
========  ======================  ===================================================
"""

from __future__ import annotations

import math
import random
from collections.abc import Mapping
from typing import Any, ClassVar

from loguru import logger
from pydantic import BaseModel, ConfigDict, Field

from futrisk.client.positions import PositionTracker
from futrisk.client.pricing import ContractTable, mid_price
from futrisk.messages.enums import PosDirection
from futrisk.messages.md import Tick
from futrisk.sim.bots.gates import STRUCTURAL_BLOCKS, Gates
from futrisk.sim.bots.indicators import NS_PER_S, SMA, BarBuilder, Channel, RollingStats


class StrategyContext:
    """What a strategy may look at: account state, latest prices, risk gates (read-only)."""

    def __init__(
        self,
        account_id: str,
        contracts: ContractTable,
        positions: PositionTracker,
        *,
        equity: float,
        gates: Gates | None = None,
        rng: random.Random | None = None,
    ) -> None:
        self.account_id = account_id
        self.contracts = contracts
        self.positions = positions
        self.equity = equity
        self.gates = gates or Gates(account_id)
        self.rng = rng or random.Random(0)
        self.now_ns = 0
        self.trading_day: str | None = None
        self._ticks: dict[str, Tick] = {}

    def update_tick(self, tick: Tick) -> None:
        self._ticks[tick.instrument_id] = tick
        self.now_ns = max(self.now_ns, tick.exch_ts_ns)
        if tick.trading_day and (self.trading_day is None or tick.trading_day > self.trading_day):
            self.trading_day = tick.trading_day

    def tick(self, instrument_id: str) -> Tick | None:
        return self._ticks.get(instrument_id)

    def price(self, instrument_id: str) -> float | None:
        t = self._ticks.get(instrument_id)
        return t.last if t is not None and t.last > 0 else None

    def net(self, instrument_id: str) -> int:
        return self.positions.net(self.account_id, instrument_id)

    def long(self, instrument_id: str) -> int:
        return self.positions.qty(self.account_id, instrument_id, PosDirection.LONG)

    def short(self, instrument_id: str) -> int:
        return self.positions.qty(self.account_id, instrument_id, PosDirection.SHORT)

    @property
    def halted(self) -> bool:
        return self.gates.halted

    @property
    def halted_since_ns(self) -> int | None:
        return self.gates.halted_since_ns

    def product_of(self, instrument_id: str) -> str:
        return self.contracts.get(instrument_id).product

    def product_block(self, product: str) -> tuple[str, int] | None:
        """``(reject code, strikes)`` of an opening block on ``product`` (``None`` when free)."""
        b = self.gates.product_block(product)
        return (b.code, b.strikes) if b is not None else None

    def per_lot(self, instrument_id: str, price: float) -> float:
        return self.contracts.get(instrument_id).notional(price, 1)

    def lots_for(self, instrument_id: str, notional: float, price: float) -> int:
        per = self.per_lot(instrument_id, price)
        return max(0, math.floor(notional / per + 1e-9)) if per > 0 and notional > 0 else 0

    def cap_frac(self, instrument_id: str) -> float:
        """Product exposure cap / equity, scaled by a published EXPOSURE_CAP_FACTOR (hwm_dd halve)."""
        info = self.contracts.get(instrument_id)
        return info.cap_frac(self.trading_day) * self.gates.exposure_cap_factor(info.product)

    def cap_lots(self, instrument_id: str, price: float, util: float) -> int:
        """Max lots keeping the product under ``util`` x its exposure cap (01 §8.14)."""
        return self.lots_for(instrument_id, util * self.cap_frac(instrument_id) * self.equity, price)


class _Params(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class Strategy:
    """Base class: tick-driven state, target positions, optional hooks."""

    name: ClassVar[str] = "strategy"
    params_model: ClassVar[type[_Params]] = _Params

    def __init__(self, params: _Params, *, account_id: str, rng: random.Random | None = None) -> None:
        self.params = params
        self.account_id = account_id
        self.rng = rng or random.Random(0)

    @property
    def instruments(self) -> tuple[str, ...]:
        raise NotImplementedError

    def on_tick(self, tick: Tick, ctx: StrategyContext) -> None:
        raise NotImplementedError

    def targets(self, ctx: StrategyContext) -> dict[str, int]:
        raise NotImplementedError

    def on_day_roll(self, trading_day: str, ctx: StrategyContext) -> None:  # noqa: B027 - optional hook
        """New trading day (first tick of the day)."""

    def describe(self) -> dict[str, Any]:
        return {}

    def _log(self, event: str, **kw: Any) -> None:
        logger.bind(corr_id=self.account_id).info(event, account_id=self.account_id, strategy=self.name, **kw)


def _sign(x: float) -> int:
    return (x > 0) - (x < 0)


def _sized_lots(ctx: StrategyContext, instrument_id: str, price: float, frac: float, cap_util: float) -> int:
    """Lots for ``frac`` of equity, clipped to the product cap, at least 1 when the cap allows it."""
    lots = ctx.lots_for(instrument_id, frac * ctx.equity, price)
    cap = ctx.cap_lots(instrument_id, price, cap_util)
    lots = min(lots, cap)
    if lots == 0 and frac > 0 and cap >= 1:
        lots = 1
    return lots


# ====================================================================== ACC1
class BreakoutParams(_Params):
    instruments: list[str] = Field(default_factory=lambda: ["rb2701", "hc2701", "i2701", "j2701"])
    bar_s: float = 20.0  # demo time scale: the 20-bar channel is warm after ~7 minutes
    entry_n: int = 20
    exit_n: int = 10
    notional_frac: float = 0.12
    cap_util: float = 0.85
    allow_short: bool = True


class _BreakoutState:
    def __init__(self, p: BreakoutParams) -> None:
        self.bars = BarBuilder(p.bar_s)
        self.channel = Channel(p.entry_n)
        self.side = 0
        self.lots = 0
        self.adopted = False


class ChannelBreakout(Strategy):
    """ACC1 黑色系通道突破: Donchian channel breakout on bars of market time."""

    name = "channel_breakout"
    params_model = BreakoutParams

    def __init__(self, params: BreakoutParams, *, account_id: str, rng: random.Random | None = None) -> None:
        super().__init__(params, account_id=account_id, rng=rng)
        self.p = params
        self.state = {i: _BreakoutState(params) for i in params.instruments}

    @property
    def instruments(self) -> tuple[str, ...]:
        return tuple(self.p.instruments)

    def on_tick(self, tick: Tick, ctx: StrategyContext) -> None:
        st = self.state.get(tick.instrument_id)
        if st is None or tick.last <= 0:
            return
        done = st.bars.update(tick.exch_ts_ns, tick.last)
        if done is not None:
            st.channel.add(done)
        if not st.channel.ready:
            return
        if not st.adopted:  # restart: keep a position opened earlier until the exit rule says otherwise
            st.adopted = True
            net = ctx.net(tick.instrument_id)
            st.side, st.lots = _sign(net), abs(net)
        price = tick.last
        upper, lower = st.channel.upper(), st.channel.lower()
        exit_hi, exit_lo = st.channel.upper(self.p.exit_n), st.channel.lower(self.p.exit_n)
        new = st.side
        why = ""
        if price > upper:
            new, why = 1, f"close {price} > {self.p.entry_n}-bar high {upper}"
        elif price < lower and self.p.allow_short:
            new, why = -1, f"close {price} < {self.p.entry_n}-bar low {lower}"
        elif st.side > 0 and price < exit_lo:
            new, why = 0, f"exit long: {price} < {self.p.exit_n}-bar low {exit_lo}"
        elif st.side < 0 and price > exit_hi:
            new, why = 0, f"exit short: {price} > {self.p.exit_n}-bar high {exit_hi}"
        if new != st.side:
            st.lots = _sized_lots(ctx, tick.instrument_id, price, self.p.notional_frac, self.p.cap_util) if new else 0
            st.side = new
            self._log("bot.signal", instrument_id=tick.instrument_id, side=new, lots=st.lots, why=why)

    def targets(self, ctx: StrategyContext) -> dict[str, int]:
        return {i: st.side * st.lots for i, st in self.state.items() if st.adopted}

    def describe(self) -> dict[str, Any]:
        return {i: {"side": st.side, "lots": st.lots, "bars": len(st.channel)} for i, st in self.state.items()}


# ====================================================================== ACC2
class TrendParams(_Params):
    instruments: list[str] = Field(
        default_factory=lambda: ["rb2701", "cu2611", "au2612", "m2701", "IF2612", "T2612"]
    )
    bar_s: float = 30.0
    fast: int = 8
    slow: int = 24
    band: float = 0.0005
    notional_frac: float = 0.10
    cap_util: float = 0.85


class _TrendState:
    def __init__(self, p: TrendParams) -> None:
        self.bars = BarBuilder(p.bar_s)
        self.fast = SMA(p.fast)
        self.slow = SMA(p.slow)
        self.regime = 0
        self.lots = 0
        self.adopted = False


class MultiTrend(Strategy):
    """ACC2 多品种趋势: dual moving-average crossover per product with a hysteresis band."""

    name = "multi_trend"
    params_model = TrendParams

    def __init__(self, params: TrendParams, *, account_id: str, rng: random.Random | None = None) -> None:
        super().__init__(params, account_id=account_id, rng=rng)
        if params.fast >= params.slow:
            raise ValueError("multi_trend: fast must be < slow")
        self.p = params
        self.state = {i: _TrendState(params) for i in params.instruments}

    @property
    def instruments(self) -> tuple[str, ...]:
        return tuple(self.p.instruments)

    def on_tick(self, tick: Tick, ctx: StrategyContext) -> None:
        st = self.state.get(tick.instrument_id)
        if st is None or tick.last <= 0:
            return
        done = st.bars.update(tick.exch_ts_ns, tick.last)
        if done is None:
            return
        fast = st.fast.add(done.close)
        slow = st.slow.add(done.close)
        if fast is None or slow is None:
            return
        if not st.adopted:
            st.adopted = True
            net = ctx.net(tick.instrument_id)
            st.regime, st.lots = _sign(net), abs(net)
        new = st.regime
        if fast > slow * (1 + self.p.band):
            new = 1
        elif fast < slow * (1 - self.p.band):
            new = -1
        if new != st.regime:
            st.lots = _sized_lots(ctx, tick.instrument_id, done.close, self.p.notional_frac, self.p.cap_util)
            st.regime = new
            self._log(
                "bot.signal",
                instrument_id=tick.instrument_id,
                side=new,
                lots=st.lots,
                why=f"SMA{self.p.fast}={fast:.4f} vs SMA{self.p.slow}={slow:.4f}",
            )

    def targets(self, ctx: StrategyContext) -> dict[str, int]:
        return {i: st.regime * st.lots for i, st in self.state.items() if st.adopted}

    def describe(self) -> dict[str, Any]:
        return {i: {"regime": st.regime, "lots": st.lots, "bars": len(st.slow)} for i, st in self.state.items()}


# ====================================================================== ACC3
class CounterTrendParams(_Params):
    weights: dict[str, float] = Field(default_factory=lambda: {"rb2701": 0.39, "hc2701": 0.17, "i2701": 0.22})
    build_s: float = 300.0  # ramp the base book over this much market time
    build_steps: int = 5  # ... in this many steps
    warmup_ticks: int = 3  # ticks seen (per day) before the build starts
    dip_step: float = 0.005  # one ladder level per 0.5% below the reference price ...
    add_frac: float = 0.0625  # ... each adding 6.25% of the base lots ...
    max_adds: int = 8  # ... down to -4%: +50% in total, spread over the whole breaker shock
    cap_util: float = 0.9  # stay under 90% of each product's exposure cap
    sector_cap: float = 1.1  # black-sector net notional / equity (warn 0.8, block 1.2)
    complete_ratio: float = 0.9  # book complete when every product holds 90% of its base (<= 1 - deadband)
    realloc_blocked: bool = True  # move the budget of a persistently blocked product elsewhere
    realloc_after_strikes: int = 2
    halt_trim_delay_s: float = 30.0  # once HALTED (latched by then), trim ...
    halt_trim_frac: float = 0.10  # ... 10% of every position, once per halt; kept until the day roll
    aggressive: bool = False  # scenario mode: build the whole book at once


class CounterTrendAdd(Strategy):
    """ACC3 日内逆势加仓: long black-sector book built early, ladder adds on dips (the breaker demo account)."""

    name = "counter_trend_add"
    params_model = CounterTrendParams

    def __init__(self, params: CounterTrendParams, *, account_id: str, rng: random.Random | None = None) -> None:
        super().__init__(params, account_id=account_id, rng=rng)
        if not params.weights:
            raise ValueError("counter_trend_add: weights must not be empty")
        self.p = params
        self.day: str | None = None
        self.t0_ns: int | None = None
        self.ticks_today = 0
        self.eff_w: dict[str, float] = dict(params.weights)
        self.base: dict[str, int] = {i: 0 for i in params.weights}
        self.ref: dict[str, float | None] = {i: None for i in params.weights}
        self.adds: dict[str, int] = {i: 0 for i in params.weights}
        self.abandoned: set[str] = set()
        self.build_done = False
        self.trim: dict[str, int] = {}
        self.trim_episode: int | None = None

    @property
    def instruments(self) -> tuple[str, ...]:
        return tuple(self.p.weights)

    # ------------------------------------------------------------ ticks
    def on_tick(self, tick: Tick, ctx: StrategyContext) -> None:
        inst = tick.instrument_id
        if inst not in self.p.weights or tick.last <= 0:
            return
        if tick.trading_day != self.day:
            self._new_day(tick.trading_day, ctx)
        self.ticks_today += 1
        if self.t0_ns is None and self.ticks_today >= self.p.warmup_ticks:
            self.t0_ns = tick.exch_ts_ns
            self._log("bot.build_start", trading_day=self.day, aggressive=self.p.aggressive,
                      target_gross=round(sum(self.p.weights.values()), 3), equity=round(ctx.equity))
        self._ladder(inst, tick.last)

    def on_day_roll(self, trading_day: str, ctx: StrategyContext) -> None:
        if trading_day != self.day:
            self._new_day(trading_day, ctx)

    def _new_day(self, trading_day: str, ctx: StrategyContext) -> None:
        self.day = trading_day
        self.t0_ns = None
        self.ticks_today = 0
        self.eff_w = dict(self.p.weights)
        self.base = {i: ctx.long(i) for i in self.p.weights}  # carry the book over; the build only adds
        self.ref = {i: None for i in self.p.weights}
        self.adds = {i: 0 for i in self.p.weights}
        self.abandoned = set()
        self.build_done = False
        self.trim = {}
        self.trim_episode = None

    def _ladder(self, inst: str, price: float) -> None:
        """Ladder level with hysteresis: add at each full step down, sell back half a step higher."""
        ref = self.ref.get(inst)
        if ref is None or ref <= 0:
            return
        drop = (ref - price) / ref / self.p.dip_step
        down, up = math.floor(drop), math.floor(drop + 0.5)
        cur = self.adds[inst]
        new = cur
        if down > cur:
            new = min(down, self.p.max_adds)
        elif up < cur:
            new = max(up, 0)
        if new != cur:
            self.adds[inst] = new
            self._log("bot.signal", instrument_id=inst, ladder=new, ref=ref, price=price,
                      why="dip: add" if new > cur else "rebound: sell back")

    # ------------------------------------------------------------ targets
    def build_fraction(self, ctx: StrategyContext) -> float:
        if self.t0_ns is None:
            return 0.0
        if self.p.aggressive or self.p.build_s <= 0:
            return 1.0
        x = (ctx.now_ns - self.t0_ns) / (self.p.build_s * NS_PER_S)
        steps = max(1, self.p.build_steps)
        return min(1.0, math.floor(x * steps + 1) / steps)  # first step right away

    def targets(self, ctx: StrategyContext) -> dict[str, int]:
        if self.t0_ns is None:
            return {}
        prices = {i: ctx.price(i) for i in self.p.weights}
        if not self.build_done:
            self._build(ctx, prices)
        out: dict[str, int] = {}
        for i in self.p.weights:
            px = prices[i]
            if px is None:
                continue
            t = self.base[i]
            if self.build_done and self.adds[i] > 0:
                t = math.floor(self.base[i] * (1 + self.adds[i] * self.p.add_frac))
                t = min(t, max(ctx.cap_lots(i, px, self.p.cap_util), self.base[i]))
            out[i] = max(0, t)
        self._sector_cap(ctx, prices, out)
        return self._halt(ctx, out)

    def _build(self, ctx: StrategyContext, prices: Mapping[str, float | None]) -> None:
        f = self.build_fraction(ctx)
        if self.p.realloc_blocked:
            self._reallocate(ctx, prices)
        for i in self.p.weights:
            px = prices[i]
            if px is None or i in self.abandoned:
                continue
            want = ctx.lots_for(i, self.eff_w[i] * ctx.equity * f, px)
            self.base[i] = max(self.base[i], min(want, ctx.cap_lots(i, px, self.p.cap_util)))
        if f >= 1.0 and all(px is not None for px in prices.values()) and all(
            ctx.long(i) >= math.floor(self.p.complete_ratio * self.base[i]) for i in self.p.weights
        ):
            self.build_done = True
            for i, px in prices.items():
                self.ref[i] = px
            notional = sum(ctx.per_lot(i, prices[i] or 0.0) * ctx.long(i) for i in self.p.weights)
            self._log("bot.book_complete", base=dict(self.base), ref=dict(self.ref),
                      gross_pct=round(notional / ctx.equity, 3) if ctx.equity else None)

    def _reallocate(self, ctx: StrategyContext, prices: Mapping[str, float | None]) -> None:
        """A product blocked by structure (e.g. concentration/exposure) gives its unfilled budget to the others."""
        for i in self.p.weights:
            px = prices[i]
            if i in self.abandoned or px is None:
                continue
            blk = ctx.product_block(ctx.product_of(i))
            if blk is None or blk[0] not in STRUCTURAL_BLOCKS or blk[1] < self.p.realloc_after_strikes:
                continue
            held = ctx.per_lot(i, px) * ctx.long(i) / ctx.equity if ctx.equity > 0 else 0.0
            freed = max(0.0, self.eff_w[i] - held)
            self.abandoned.add(i)
            self.eff_w[i] = held
            self.base[i] = ctx.long(i)
            free = [j for j in self.p.weights if j not in self.abandoned and prices[j] is not None
                    and ctx.product_block(ctx.product_of(j)) is None]
            wsum = sum(self.p.weights[j] for j in free)
            moved: dict[str, float] = {}
            for j in free:
                room = self.p.cap_util * ctx.cap_frac(j) - self.eff_w[j]
                add = max(0.0, min(freed * self.p.weights[j] / wsum, room)) if wsum > 0 else 0.0
                self.eff_w[j] += add
                moved[j] = round(add, 4)
            self._log("bot.realloc", instrument_id=i, reason=blk[0], freed=round(freed, 4), moved=moved)

    def _sector_cap(self, ctx: StrategyContext, prices: Mapping[str, float | None], out: dict[str, int]) -> None:
        limit = self.p.sector_cap * ctx.equity
        notional = {i: ctx.per_lot(i, prices[i] or 0.0) * n for i, n in out.items()}
        total = sum(notional.values())
        if total <= limit:
            return
        base_total = sum(ctx.per_lot(i, prices[i] or 0.0) * min(n, self.base[i]) for i, n in out.items())
        extra = total - base_total
        k = max(0.0, (limit - base_total) / extra) if extra > 0 else 0.0
        for i, n in out.items():
            b = min(n, self.base[i])
            out[i] = b + math.floor((n - b) * k)

    def _halt(self, ctx: StrategyContext, out: dict[str, int]) -> dict[str, int]:
        """Once per halt, 30 s after it (latched by then): trim every position by 10%.

        The trimmed book stays the ceiling until the day roll (or a new build), also after a
        manual reset: the bot does not buy the trimmed lots back. Opening intents while halted
        are the executor's 1-lot halt probes, which do not rebuild the book either.
        """
        since = ctx.halted_since_ns
        if (
            ctx.halted
            and since is not None
            and self.trim_episode != since
            and self.p.halt_trim_frac > 0
            and ctx.now_ns - since >= self.p.halt_trim_delay_s * NS_PER_S
        ):
            self.trim_episode = since
            trim = {i: math.floor(ctx.long(i) * (1 - self.p.halt_trim_frac)) for i in self.p.weights if ctx.long(i) > 0}
            self.trim = {i: min(t, self.trim.get(i, t)) for i, t in trim.items()}
            self._log("bot.halt_trim", trim=dict(self.trim), why="account HALTED: reduce risk, closes stay allowed")
        for i, t in self.trim.items():
            if i in out:
                out[i] = min(out[i], t)
        return out

    def describe(self) -> dict[str, Any]:
        return {
            "build_done": self.build_done,
            "base": dict(self.base),
            "adds": dict(self.adds),
            "ref": dict(self.ref),
            "abandoned": sorted(self.abandoned),
            "trim": dict(self.trim),
        }


# ====================================================================== ACC4
class IndexParams(_Params):
    weights: dict[str, float] = Field(default_factory=lambda: {"IF2612": 0.5, "IC2612": 0.5})
    bar_s: float = 10.0  # warm after slow x bar_s = ~3 minutes
    fast: int = 6
    slow: int = 20
    band: float = 0.0003  # relative SMA gap needed to take/flip a direction
    leverage_min: float = 0.3
    leverage_max: float = 0.55  # IF/IC product caps (30% of equity each, 01 §8.14) bind above ~0.55x
    strength_scale: float = 0.002  # SMA gap at which the leverage target reaches its maximum
    leverage_step: float = 0.05
    cap_util: float = 0.9
    respect_product_cap: bool = True


class IndexDirectional(Strategy):
    """ACC4 股指杠杆方向: one direction for the index book, leverage scaled by signal strength.

    Note: with the default ``product_exposure`` rule the IF/IC caps (30% of equity each)
    bind long before 1.5x, so the realised leverage is ~0.5x unless the caps are raised
    for ACC4 or ``respect_product_cap`` is turned off (then the risk-engine blocks it).
    """

    name = "index_directional"
    params_model = IndexParams

    def __init__(self, params: IndexParams, *, account_id: str, rng: random.Random | None = None) -> None:
        super().__init__(params, account_id=account_id, rng=rng)
        if params.fast >= params.slow:
            raise ValueError("index_directional: fast must be < slow")
        self.p = params
        self.bars = {i: BarBuilder(params.bar_s) for i in params.weights}
        self.fast = {i: SMA(params.fast) for i in params.weights}
        self.slow = {i: SMA(params.slow) for i in params.weights}
        self.mom: dict[str, float] = {}
        self.direction = 0
        self.leverage = 0.0
        self.lots: dict[str, int] = {}
        self.adopted = False

    @property
    def instruments(self) -> tuple[str, ...]:
        return tuple(self.p.weights)

    def on_tick(self, tick: Tick, ctx: StrategyContext) -> None:
        inst = tick.instrument_id
        if inst not in self.bars or tick.last <= 0:
            return
        done = self.bars[inst].update(tick.exch_ts_ns, tick.last)
        if done is None:
            return
        f, s = self.fast[inst].add(done.close), self.slow[inst].add(done.close)
        if f is None or s is None or s <= 0:
            return
        self.mom[inst] = (f - s) / s
        if len(self.mom) < len(self.p.weights):
            return
        if not self.adopted:
            self.adopted = True
            nets = {i: ctx.net(i) for i in self.p.weights}
            self.direction = _sign(sum(_sign(n) for n in nets.values()))
            self.lots = {i: abs(n) for i, n in nets.items()}
        m = sum(self.mom.values()) / len(self.mom)
        new_dir = self.direction
        if m > self.p.band:
            new_dir = 1
        elif m < -self.p.band:
            new_dir = -1
        strength = min(1.0, abs(m) / self.p.strength_scale) if self.p.strength_scale > 0 else 1.0
        lev = self.p.leverage_min + (self.p.leverage_max - self.p.leverage_min) * strength
        lev = round(lev / self.p.leverage_step) * self.p.leverage_step if self.p.leverage_step > 0 else lev
        if new_dir != self.direction or (new_dir != 0 and abs(lev - self.leverage) >= self.p.leverage_step - 1e-9):
            lots = self._size(ctx, lev)
            if new_dir != self.direction or lots != self.lots:
                self._log("bot.signal", side=new_dir, leverage=lev, lots=lots, momentum=round(m, 6),
                          why="index momentum")
            self.direction, self.leverage, self.lots = new_dir, lev, lots

    def _size(self, ctx: StrategyContext, leverage: float) -> dict[str, int]:
        out: dict[str, int] = {}
        for i, w in self.p.weights.items():
            px = ctx.price(i)
            if px is None:
                out[i] = self.lots.get(i, 0)
                continue
            lots = ctx.lots_for(i, leverage * w * ctx.equity, px)
            if self.p.respect_product_cap:
                lots = min(lots, ctx.cap_lots(i, px, self.p.cap_util))
            out[i] = lots
        return out

    def targets(self, ctx: StrategyContext) -> dict[str, int]:
        if not self.adopted:
            return {}
        return {i: self.direction * self.lots.get(i, 0) for i in self.p.weights}

    def describe(self) -> dict[str, Any]:
        return {"direction": self.direction, "leverage": self.leverage, "lots": dict(self.lots)}


# ====================================================================== ACC5
class SpreadParams(_Params):
    near: str = "rb2701"
    far: str = "rb2705"
    sample_s: float = 5.0
    window: int = 60
    min_samples: int = 12
    entry_z: float = 1.5
    exit_z: float = 0.3
    base_lots: int = 20
    step_lots: int = 10
    max_lots: int = 60
    default_side: int = 1  # +1: long near / short far (bet the far-near spread narrows)


class CalendarSpread(Strategy):
    """ACC5 螺纹跨期套利: mean reversion of ``far - near`` with a permanent two-sided book.

    ``side = +1`` holds long ``near`` / short ``far`` (profits when the spread falls),
    ``side = -1`` the reverse. The book never goes flat: ``base_lots`` per leg at least,
    ``+step_lots`` per z unit beyond ``entry_z``, up to ``max_lots``; back to the base
    when ``|z| <= exit_z``. Holding both legs of rb shows SHFE single-side margin.
    """

    name = "rb_calendar_spread"
    params_model = SpreadParams

    def __init__(self, params: SpreadParams, *, account_id: str, rng: random.Random | None = None) -> None:
        super().__init__(params, account_id=account_id, rng=rng)
        if params.near == params.far:
            raise ValueError("rb_calendar_spread: near and far must differ")
        self.p = params
        self.stats = RollingStats(params.window)
        self.last_sample_ns: int | None = None
        self.side = 0
        self.units = 0
        self.z: float | None = None
        self.spread: float | None = None

    @property
    def instruments(self) -> tuple[str, ...]:
        return (self.p.near, self.p.far)

    def on_tick(self, tick: Tick, ctx: StrategyContext) -> None:
        if tick.instrument_id not in (self.p.near, self.p.far):
            return
        tn, tf = ctx.tick(self.p.near), ctx.tick(self.p.far)
        if tn is None or tf is None:
            return
        now = tick.exch_ts_ns
        if self.last_sample_ns is not None and now - self.last_sample_ns < self.p.sample_s * NS_PER_S:
            return
        self.last_sample_ns = now
        self.spread = mid_price(tf) - mid_price(tn)
        self.stats.add(self.spread)
        self._decide(ctx)

    def _decide(self, ctx: StrategyContext) -> None:
        assert self.spread is not None
        if self.side == 0:
            near = ctx.net(self.p.near)
            if near != 0:  # restart: adopt the book already held
                self.side, self.units = _sign(near), abs(near)
            else:
                self.side, self.units = (1 if self.p.default_side >= 0 else -1), self.p.base_lots
            self._log("bot.signal", side=self.side, units=self.units, why="open the base two-sided book")
            return
        if len(self.stats) < self.p.min_samples:
            return
        z = self.stats.zscore(self.spread)
        self.z = z
        if z is None:
            return
        side, units = self.side, self.units
        if abs(z) >= self.p.entry_z:
            side = 1 if z > 0 else -1  # spread rich -> long near/short far; cheap -> the reverse
            extra = self.p.step_lots * (1 + math.floor(abs(z) - self.p.entry_z))
            units = min(self.p.max_lots, self.p.base_lots + extra)
        elif abs(z) <= self.p.exit_z:
            units = self.p.base_lots
        if (side, units) != (self.side, self.units):
            self._log("bot.signal", side=side, units=units, z=round(z, 3), spread=round(self.spread, 3),
                      why="spread mean reversion")
            self.side, self.units = side, units

    def targets(self, ctx: StrategyContext) -> dict[str, int]:
        if self.side == 0:
            return {}
        return {self.p.near: self.side * self.units, self.p.far: -self.side * self.units}

    def describe(self) -> dict[str, Any]:
        return {"side": self.side, "units": self.units, "z": None if self.z is None else round(self.z, 3),
                "spread": self.spread, "samples": len(self.stats)}


# ====================================================================== registry
STRATEGIES: dict[str, type[Strategy]] = {
    ChannelBreakout.name: ChannelBreakout,
    MultiTrend.name: MultiTrend,
    CounterTrendAdd.name: CounterTrendAdd,
    IndexDirectional.name: IndexDirectional,
    CalendarSpread.name: CalendarSpread,
}


def make_strategy(name: str, params: Mapping[str, Any], *, account_id: str, rng: random.Random) -> Strategy:
    """Instantiate a registered strategy with validated parameters."""
    cls = STRATEGIES.get(name)
    if cls is None:
        raise KeyError(f"unknown strategy {name!r}; known: {', '.join(sorted(STRATEGIES))}")
    p = cls.params_model.model_validate(dict(params))
    return cls(p, account_id=account_id, rng=rng)


__all__ = [
    "STRATEGIES",
    "BreakoutParams",
    "CalendarSpread",
    "ChannelBreakout",
    "CounterTrendAdd",
    "CounterTrendParams",
    "IndexDirectional",
    "IndexParams",
    "MultiTrend",
    "SpreadParams",
    "Strategy",
    "StrategyContext",
    "TrendParams",
    "make_strategy",
]
