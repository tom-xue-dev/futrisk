"""The backtest strategy: a dual moving average per product, traded in the day's main contract (01 §8.16).

Every strategy returns **target net positions** (signed lots per contract); the
:class:`~futrisk.backtest.executor.Executor` turns them into orders through the
engine's pre-trade path, so every order meets the same rules as in production.

``dual_ma``: the fast and slow simple moving averages of the daily closes, with
today's latest price standing in for today's close, are compared at **every**
market state: long while the fast average is ``band`` above the slow one, short
(or flat without ``allow_short``) while it is ``band`` below, unchanged in
between. The size is ``notional_frac`` of the account's initial capital per
product, capped at ``cap_util`` × the product's exposure cap ``0.03 / limit_pct``
of equity (01 §8.14). Because the signal is live, a flip can happen during a
bad day - after the circuit breaker has latched, the flip's closing leg still
goes through but its opening leg is rejected; that is the difference the two
arms measure.

The history is the main series back-adjusted at every roll
(``DailyBar.roll_factor`` = new contract's previous settle ÷ old contract's
settle), so a roll gap does not look like a cross; positions are always held in
the day's main contract.
"""

from __future__ import annotations

import math
from collections import deque
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, ClassVar

from pydantic import BaseModel, ConfigDict

from futrisk.backtest.data import DailyBar
from futrisk.core.instruments import InstrumentRegistry
from futrisk.core.metrics import LIMIT_LOSS_BUDGET
from futrisk.messages.enums import AccountState


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
    equity: float  # the sizing base: the account's initial capital (paired arms hold the same book size)
    registry: InstrumentRegistry
    quotes: dict[str, Quote]
    positions: dict[str, int]  # instrument -> net lots (engine book)
    state: AccountState = AccountState.NORMAL

    def lots_for(self, instrument_id: str, notional: float, price: float) -> int:
        per = price * self.registry.multiplier(instrument_id)
        return max(0, math.floor(notional / per + 1e-9)) if per > 0 and notional > 0 else 0

    def cap_lots(self, instrument_id: str, price: float, util: float) -> int:
        pct = float(self.registry.product_spec(instrument_id).limit_pct_on(self.trading_day))
        cap_frac = LIMIT_LOSS_BUDGET / pct if pct > 0 else 1.0
        return self.lots_for(instrument_id, util * cap_frac * self.equity, price)


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

    def on_roll(self, product: str, factor: float) -> None:  # noqa: B027 - optional hook
        """A new main contract today: rescale price history by ``factor`` (back-adjustment)."""

    def on_bar(self, product: str, bar: DailyBar) -> None:  # noqa: B027 - optional hook
        """A completed daily bar of the main series (also used for the warm-up)."""

    def begin_day(self, trading_day: str) -> None:  # noqa: B027 - optional hook
        """Before the first market state of a trading day."""

    def targets(self, ctx: StrategyContext) -> dict[str, int] | None:
        """Target net lots per contract at this market state (None: no change)."""
        raise NotImplementedError

    def describe(self) -> dict[str, Any]:
        return {}


class DualMAParams(_Params):
    fast: int = 5
    slow: int = 20
    band: float = 0.002  # hysteresis: the averages must differ by this fraction of the slow one to flip
    notional_frac: float = 0.15  # target notional per product, fraction of the initial capital
    cap_util: float = 0.85
    allow_short: bool = True


class DualMA(Strategy):
    """Dual moving average per product, evaluated at every market state (see the module docstring)."""

    name = "dual_ma"
    params_model = DualMAParams

    def __init__(self, params: DualMAParams, *, account_id: str, products: list[str]) -> None:
        super().__init__(params, account_id=account_id, products=products)
        if not 1 <= params.fast < params.slow:
            raise ValueError("dual_ma: need 1 <= fast < slow")
        self.p = params
        self.closes = {pr: deque[float](maxlen=params.slow - 1) for pr in products}  # completed days, back-adjusted
        self.regime = dict.fromkeys(products, 0)
        self.lots = dict.fromkeys(products, 0)

    def on_roll(self, product: str, factor: float) -> None:
        q = self.closes.get(product)
        if q is not None and factor != 1.0:
            vals = [v * factor for v in q]
            q.clear()
            q.extend(vals)

    def on_bar(self, product: str, bar: DailyBar) -> None:
        q = self.closes.get(product)
        if q is not None:
            q.append(bar.close)

    def signal(self, product: str, price: float) -> int:
        """The regime with ``price`` as today's close (the current regime while the history is too short)."""
        hist = self.closes[product]
        if len(hist) < self.p.slow - 1:
            return self.regime[product]
        closes = [*hist, price]
        fast = sum(closes[-self.p.fast:]) / self.p.fast
        slow = sum(closes) / self.p.slow
        if fast > slow * (1 + self.p.band):
            return 1
        if fast < slow * (1 - self.p.band):
            return -1 if self.p.allow_short else 0
        return self.regime[product]

    def targets(self, ctx: StrategyContext) -> dict[str, int] | None:
        out: dict[str, int] = {}
        for pr in self.products:
            q = ctx.quotes.get(pr)
            if q is None or q.price is None:
                continue
            if q.live:
                new = self.signal(pr, q.price)
                if new != self.regime[pr] or (new != 0 and self.lots[pr] == 0):
                    self.regime[pr] = new
                    self.lots[pr] = sized_lots(ctx, q.instrument_id, q.price, self.p.notional_frac, self.p.cap_util)
            out[q.instrument_id] = self.regime[pr] * self.lots[pr]
        return out

    def describe(self) -> dict[str, Any]:
        return {"regime": dict(self.regime), "lots": dict(self.lots)}


STRATEGIES: dict[str, type[Strategy]] = {DualMA.name: DualMA}


def make_strategy(name: str, params: Mapping[str, Any], *, account_id: str, products: list[str]) -> Strategy:
    cls = STRATEGIES.get(name)
    if cls is None:
        raise KeyError(f"unknown backtest strategy {name!r}; known: {', '.join(sorted(STRATEGIES))}")
    p = cls.params_model.model_validate(dict(params))
    return cls(p, account_id=account_id, products=products)


__all__ = [
    "STRATEGIES",
    "DualMA",
    "DualMAParams",
    "Quote",
    "Strategy",
    "StrategyContext",
    "make_strategy",
    "sized_lots",
]
