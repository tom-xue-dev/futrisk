"""Helpers for the bot tests: synthetic tick sequences and ready-made strategy contexts."""

from __future__ import annotations

import random
from collections.abc import Iterable

from futrisk.client.positions import PositionTracker
from futrisk.client.pricing import ContractTable
from futrisk.client.testing import make_tick
from futrisk.messages.md import Tick
from futrisk.sim.bots.gates import Gates
from futrisk.sim.bots.strategies import Strategy, StrategyContext

T0 = 1_790_559_300_000_000_000  # 2026-09-28 09:35 Beijing, a continuous session
S = 1_000_000_000
TD = "20260928"


def tick(instrument: str, price: float, t_s: float, *, trading_day: str = TD, **kw: object) -> Tick:
    return make_tick(instrument, price, ts_ns=T0 + int(t_s * S), trading_day=trading_day, **kw)  # type: ignore[arg-type]


def context(account: str, contracts: ContractTable, tracker: PositionTracker, *, equity: float = 10_000_000.0,
            gates: Gates | None = None) -> StrategyContext:
    return StrategyContext(account, contracts, tracker, equity=equity, gates=gates or Gates(account),
                           rng=random.Random(1))


def feed(strategy: Strategy, ctx: StrategyContext, ticks: Iterable[Tick]) -> dict[str, int]:
    """Feed ticks (updating the context like the bot does) and return the final targets."""
    for t in ticks:
        ctx.update_tick(t)
        strategy.on_tick(t, ctx)
    return strategy.targets(ctx)


def bars(instrument: str, prices: Iterable[float], *, start_s: float = 0.0, bar_s: float = 30.0,
         per_bar: int = 2) -> list[Tick]:
    """``per_bar`` ticks per bar at each price (one bar per price), in market time."""
    out: list[Tick] = []
    t = start_s
    step = bar_s / per_bar
    for px in prices:
        for _ in range(per_bar):
            out.append(tick(instrument, px, t))
            t += step
    return out


def expected_book(contracts: ContractTable, weights: dict[str, float], prices: dict[str, float], *,
                  equity: float = 10_000_000.0, frac: float = 1.0, cap_util: float = 0.9,
                  trading_day: str = TD) -> dict[str, int]:
    """Base lots of a weight book: ``floor(w x equity x frac / per-lot notional)``, clipped to
    ``cap_util`` x the product exposure cap (01 §8.14) - what the strategies size to."""
    out: dict[str, int] = {}
    for i, w in weights.items():
        spec = contracts.get(i)
        per = spec.notional(prices[i], 1)
        want = int(w * equity * frac / per + 1e-9)
        cap = int(cap_util * spec.cap_frac(trading_day) * equity / per + 1e-9)
        out[i] = min(want, cap)
    return out
