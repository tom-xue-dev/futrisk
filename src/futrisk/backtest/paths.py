"""The intraday price path of a daily bar (01 §8.16 日内路径).

Layer A only has daily bars, so the intraday path is synthesized:

- **order rule** (``open_near_extreme``): an open nearer the high goes
  O→H→L→C, an open nearer the low goes O→L→H→C; on a tie an up day
  (C ≥ O) visits the low first. ``high_first`` / ``low_first`` force one order;
- **states**: every leg (O→E1, E1→E2, E2→C) is split into linearly
  interpolated states on the tick grid (at least ``points_per_leg``, more when a
  leg moves more than ``max_step`` × open per state, at most
  ``max_points_per_leg``), so the strategy and the breaker see a gradual move
  rather than one jump. Interpolated states never leave [low, high]. The state
  count of a contract depends on its own bar only, so its path does not change
  with the other contracts replayed that day;
- **time**: the states are spread evenly over the contract's continuous
  trading time of that trading day (session templates of ``products.yaml``,
  night session first): the open, the extremes and the close sit at 0, 1/3, 2/3
  and 1 of the trading time, so contracts with the same sessions reach their
  extremes at the same time (a synchronised, pessimistic model). The open is 1 s
  after the first session opens, the close 1 s before the last one ends.

A daily bar admits many paths; the results of layer A are therefore conditional
on this path model (10 文档).
"""

from __future__ import annotations

import bisect
import math
from collections.abc import Sequence
from dataclasses import dataclass
from decimal import Decimal

NS_PER_S = 1_000_000_000
NS_PER_MIN = 60 * NS_PER_S

KIND_OPEN = "O"
KIND_HIGH = "H"
KIND_LOW = "L"
KIND_CLOSE = "C"
KIND_INTERP = "I"


@dataclass(slots=True)
class Point:
    """One market state of one contract."""

    ts_ns: int
    instrument_id: str
    price: float
    kind: str  # O / H / L / C / I (interpolated) / B (bar-derived) / T (recorded tick) / S (scenario)
    volume: int = 0  # cumulative day volume after this state
    turnover: float = 0.0  # cumulative day turnover (yuan)
    flags: int = 0
    bid1: float | None = None
    ask1: float | None = None


class TickGrid:
    """Float rounding to a tick grid whose repr is the grid value (``3671.4``, not ``3671.4000000000005``)."""

    __slots__ = ("digits", "tick")

    def __init__(self, tick: Decimal | float | str) -> None:
        t = Decimal(str(tick))
        self.tick = float(t)
        exp = t.normalize().as_tuple().exponent
        self.digits = max(0, -int(exp)) if isinstance(exp, int) else 0

    def round(self, price: float) -> float:
        return round(round(price / self.tick) * self.tick, self.digits)

    def floor(self, price: float) -> float:
        return round(math.floor(price / self.tick + 1e-9) * self.tick, self.digits)

    def ceil(self, price: float) -> float:
        return round(math.ceil(price / self.tick - 1e-9) * self.tick, self.digits)


class Timeline:
    """Continuous trading intervals of one contract on one trading day, as trading time."""

    __slots__ = ("_cum", "intervals", "total_ns")

    def __init__(self, intervals: Sequence[tuple[int, int]]) -> None:
        ivs = sorted((a, b) for a, b in intervals if b > a)
        if not ivs:
            raise ValueError("no trading intervals")
        self.intervals = ivs
        self._cum: list[int] = []
        acc = 0
        for a, b in ivs:
            self._cum.append(acc)
            acc += b - a
        self.total_ns = acc

    @property
    def start_ns(self) -> int:
        return self.intervals[0][0]

    @property
    def end_ns(self) -> int:
        return self.intervals[-1][1]

    def at(self, offset_ns: int) -> int:
        """Wall time of a trading-time offset (clamped to the day)."""
        off = min(max(0, offset_ns), self.total_ns)
        i = bisect.bisect_right(self._cum, off) - 1
        i = max(0, min(i, len(self.intervals) - 1))
        a, b = self.intervals[i]
        t = a + (off - self._cum[i])
        return min(t, b)

    def offset_of(self, ts_ns: int) -> int:
        """Trading time elapsed at wall time ``ts_ns`` (breaks count as zero)."""
        acc = 0
        for a, b in self.intervals:
            if ts_ns <= a:
                return acc
            if ts_ns < b:
                return acc + (ts_ns - a)
            acc += b - a
        return acc

    def contains(self, ts_ns: int) -> bool:
        return any(a <= ts_ns < b for a, b in self.intervals)

    def spread(self, n: int, *, margin_ns: int = NS_PER_S) -> list[int]:
        """``n`` wall times spread evenly over the trading time, ``margin_ns`` inside both ends."""
        if n <= 0:
            return []
        span = max(0, self.total_ns - 2 * margin_ns)
        if n == 1:
            return [self.at(margin_ns)]
        return [self.at(margin_ns + span * j // (n - 1)) for j in range(n)]


def extreme_order(o: float, h: float, lo: float, c: float, rule: str = "open_near_extreme") -> tuple[str, str]:
    """``("H", "L")`` for O→H→L→C, ``("L", "H")`` for O→L→H→C."""
    if rule == "high_first":
        return KIND_HIGH, KIND_LOW
    if rule == "low_first":
        return KIND_LOW, KIND_HIGH
    up, dn = h - o, o - lo
    if up < dn:
        return KIND_HIGH, KIND_LOW  # open near the high: the high comes first
    if dn < up:
        return KIND_LOW, KIND_HIGH
    return (KIND_LOW, KIND_HIGH) if c >= o else (KIND_HIGH, KIND_LOW)


ANCHOR_FRACTIONS = (0.0, 1.0 / 3.0, 2.0 / 3.0, 1.0)  # O, first extreme, second extreme, C over the trading day


def anchors_of(o: float, h: float, lo: float, c: float, rule: str = "open_near_extreme") -> list[tuple[float, str]]:
    e1, e2 = extreme_order(o, h, lo, c, rule)
    val = {KIND_HIGH: h, KIND_LOW: lo}
    return [(o, KIND_OPEN), (val[e1], e1), (val[e2], e2), (c, KIND_CLOSE)]


def leg_counts(
    o: float, h: float, lo: float, c: float, *, points_per_leg: int, max_step: float = 0.0, max_points: int = 40,
    rule: str = "open_near_extreme",
) -> tuple[int, int, int]:
    """States per leg: at least ``points_per_leg``, more when a leg moves more than ``max_step`` × open per state."""
    anchors = anchors_of(o, h, lo, c, rule)
    out: list[int] = []
    step = abs(max_step * o)
    for (a, _ka), (b, _kb) in zip(anchors, anchors[1:], strict=False):
        k = max(1, points_per_leg)
        if step > 0:
            k = max(k, math.ceil(abs(b - a) / step - 1e-9))
        out.append(min(k, max(max_points, points_per_leg)))
    return out[0], out[1], out[2]


def bar_path(
    o: float, h: float, lo: float, c: float, grid: TickGrid, legs: Sequence[int], rule: str = "open_near_extreme"
) -> list[tuple[float, str, float]]:
    """``(price, kind, day fraction)`` of the synthesized states; the anchors sit at :data:`ANCHOR_FRACTIONS`."""
    anchors = anchors_of(o, h, lo, c, rule)
    out: list[tuple[float, str, float]] = [(o, KIND_OPEN, 0.0)]
    for i, ((a, _ka), (b, kb)) in enumerate(zip(anchors, anchors[1:], strict=False)):
        k = max(1, legs[i])
        fa, fb = ANCHOR_FRACTIONS[i], ANCHOR_FRACTIONS[i + 1]
        for j in range(1, k + 1):
            f = fa + (fb - fa) * j / k
            if j == k:
                out.append((b, kb, f))
            else:
                p = grid.round(a + (b - a) * j / k)
                out.append((min(max(p, lo), h), KIND_INTERP, f))
    return out


def synthesize(
    instrument_id: str,
    timeline: Timeline,
    o: float,
    h: float,
    lo: float,
    c: float,
    *,
    grid: TickGrid,
    multiplier: int,
    points_per_leg: int,
    rule: str = "open_near_extreme",
    legs: Sequence[int] | None = None,
) -> list[Point]:
    """The synthesized states of one daily bar, timed over the contract's trading day.

    ``legs`` gives the number of states of each leg (default ``points_per_leg``
    each); the open, the extremes and the close always sit at the same day
    fractions, so contracts with the same sessions visit their extremes at the
    same time. Volume is a state counter (one lot per state): only its
    monotonicity matters (``Tick.order_key``); layer A settles on the official
    settlement price.
    """
    k = max(1, points_per_leg)
    path = bar_path(o, h, lo, c, grid, tuple(legs) if legs is not None else (k, k, k), rule)
    margin = NS_PER_S
    span = max(0, timeline.total_ns - 2 * margin)
    out: list[Point] = []
    cum_t = 0.0
    last = -1
    for j, (p, kind, f) in enumerate(path):
        ts = timeline.at(margin + int(round(span * f)))
        if ts <= last:
            ts = last + 1
        last = ts
        cum_t += p * multiplier
        out.append(Point(ts, instrument_id, p, kind, j + 1, cum_t))
    return out


__all__ = [
    "ANCHOR_FRACTIONS",
    "KIND_CLOSE",
    "KIND_HIGH",
    "KIND_INTERP",
    "KIND_LOW",
    "KIND_OPEN",
    "Point",
    "TickGrid",
    "Timeline",
    "anchors_of",
    "bar_path",
    "extreme_order",
    "leg_counts",
    "synthesize",
]
