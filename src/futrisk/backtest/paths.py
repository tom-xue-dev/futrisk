"""Intraday price paths and their uncertainty (01 §8.16 日内路径; plan §7.1).

Layer A only has daily bars, so the intraday path is synthesized:

- **order rule** (``open_near_extreme``): an open nearer the high goes
  O→H→L→C, an open nearer the low goes O→L→H→C; on a tie an up day
  (C ≥ O) visits the low first. ``high_first`` / ``low_first`` force one order
  (used for sensitivity runs);
- **states**: every leg (O→E1, E1→E2, E2→C) is split into linearly
  interpolated states on the tick grid (at least ``points_per_leg``, more when a
  leg moves more than ``max_step`` × open per state, at most
  ``max_points_per_leg``), so a strategy that trades on the way (the
  counter-trend ladder) and the breaker see a gradual move rather than one jump.
  Interpolated states never leave [low, high]. The state count of a contract
  depends on its own bar only, so its path does not change with the other
  contracts replayed that day;
- **time**: the states are spread evenly over the contract's continuous
  trading time of that trading day (session templates of ``products.yaml``,
  night session first): the open, the extremes and the close sit at 0, 1/3, 2/3
  and 1 of the trading time, so contracts with the same sessions reach their
  extremes at the same time (a synchronised, pessimistic model; the stochastic
  ensemble of :mod:`futrisk.backtest.pathmodel` relaxes it). The open is 1 s
  after the first session opens, the close 1 s before the last one ends.

Because a daily bar admits many paths, the backtest also classifies every
account-day (:func:`pnl_bounds`, :func:`classify`) for the book held after the
open: the day's minimum P&L lies between the **pessimistic** bound (every
contract at its adverse extreme at the same time) and the **optimistic** bound
(the highest minimum a path consistent with the bars can have). A rule level is
*certain* when even the optimistic bound breaches it, *impossible* when not
even the pessimistic bound does, and *uncertain* otherwise. The classes concern
whether the static book **reaches** a line; the size of a rule's effect still
depends on the path after it.

:func:`order_hit_rate` measures the order rule against minute bars (layer B):
which extreme came first in the minute data vs which one the rule predicts.
"""

from __future__ import annotations

import bisect
import math
from collections.abc import Iterable, Sequence
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


def bar_prices(
    o: float, h: float, lo: float, c: float, grid: TickGrid, points_per_leg: int, rule: str = "open_near_extreme"
) -> list[tuple[float, str]]:
    """Prices and kinds of the synthesized states: O, then ``points_per_leg`` per leg (the leg end included)."""
    k = max(1, points_per_leg)
    return [(p, kind) for p, kind, _f in bar_path(o, h, lo, c, grid, (k, k, k), rule)]


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


def minute_bar_points(
    instrument_id: str,
    timeline: Timeline,
    bars: Iterable[tuple[int, float, float, float, float]],
    *,
    period_min: int,
    multiplier: int,
    rule: str = "open_near_extreme",
) -> list[Point]:
    """Layer B: each minute bar ``(end_ns, o, h, l, c)`` becomes O, E1, E2, C inside its trading time.

    The bar covers the ``period_min`` trading minutes before its end label
    (Sina bars span session breaks); the four states sit at 1 s, 1/3, 2/3 and
    ``period`` − 1 s of that span; the high/low order follows ``rule``. Sina's
    minute volumes are unreliable (SOURCES.md), so volume is a state counter
    and layer B settles on the official daily settlement price.
    """
    out: list[Point] = []
    cum_v = 0
    cum_t = 0.0
    last_ts = -1
    period = period_min * NS_PER_MIN
    for end_ns, o, h, lo, c in bars:
        off_end = timeline.offset_of(end_ns)
        if off_end <= 0:
            continue
        off_start = max(0, off_end - period)
        span = off_end - off_start
        e1, e2 = extreme_order(o, h, lo, c, rule)
        val = {KIND_HIGH: h, KIND_LOW: lo}
        seq = [(o, KIND_OPEN), (val[e1], e1), (val[e2], e2), (c, KIND_CLOSE)]
        offs = [off_start + min(NS_PER_S, span // 4), off_start + span // 3, off_start + 2 * span // 3,
                off_end - min(NS_PER_S, span // 4)]
        for (p, kind), off in zip(seq, offs, strict=True):
            ts = timeline.at(off)
            if ts <= last_ts:
                ts = last_ts + 1
            last_ts = ts
            cum_v += 1
            cum_t += p * multiplier
            out.append(Point(ts, instrument_id, p, kind, cum_v, cum_t))
    return out


# ----------------------------------------------------------------- bounding
@dataclass(frozen=True, slots=True)
class Leg:
    """One contract of a book for the bound computation: net lots (signed) and the day's prices.

    ``net`` is the static book after the open; ``net_sod`` / ``pre`` (the SOD lots and the previous settle, the
    SOD mark) are only needed for books whose contracts do not open at the same time (``common_open=False``).
    """

    net: int
    multiplier: float
    open: float
    high: float
    low: float
    close: float
    net_sod: int | None = None
    pre: float | None = None


def pnl_bounds(base: float, legs: Iterable[Leg], *, common_open: bool = True) -> tuple[float, float]:
    """``(optimistic_min, pessimistic_min)`` of the intraday P&L of a static book.

    ``pessimistic_min`` is a lower bound of the day's minimum P&L over every path consistent with the bars;
    ``optimistic_min`` an upper bound of it (every path goes at least as low), so a loss line below the
    optimistic bound is reached on every path (*certain*) and one above the pessimistic bound on none.

    - ``common_open`` (every contract opens in the same first market state): ``base`` is the P&L of that
      opening state and P&L(P) = base + Σ net·mult·(P − O). Forced states: the joint open, the joint close and
      each contract's adverse extreme (the others at best at their favourable extremes);
    - otherwise (e.g. a night-session contract and a day-only index future): contract i contributes 0 before its
      own open (marked at its previous settle) and c_i(P) = net_sod·mult·(O − pre) + net·mult·(P − O) after it.
      There is no joint opening state: the forced states are the joint close (end of the day), each contract's
      adverse extreme and each contract's open, the other contracts at the best of {0, c_j(L), c_j(H)}.
      ``base`` is ignored (every contract's opening gap is inside its c_i).
    """
    items = [g for g in legs if g.net != 0 or (g.net_sod or 0) != 0]
    if common_open:
        adv: list[float] = []
        fav: list[float] = []
        close_delta = 0.0
        for g in items:
            k = g.net * g.multiplier
            lo_d = k * (g.low - g.open)
            hi_d = k * (g.high - g.open)
            adv.append(min(lo_d, hi_d))
            fav.append(max(lo_d, hi_d))
            close_delta += k * (g.close - g.open)
        pess = base + sum(adv)
        cands = [base, base + close_delta]
        total_fav = sum(fav)
        for a, f in zip(adv, fav, strict=True):
            cands.append(base + a + (total_fav - f))
        return min(cands), pess
    gap: list[float] = []
    adv_m: list[float] = []
    fav_m: list[float] = []
    final = 0.0
    for g in items:
        n_sod = g.net if g.net_sod is None else g.net_sod
        pre = g.open if g.pre is None else g.pre
        k = g.net * g.multiplier
        g0 = n_sod * g.multiplier * (g.open - pre)
        c_lo, c_hi = g0 + k * (g.low - g.open), g0 + k * (g.high - g.open)
        gap.append(g0)
        adv_m.append(min(c_lo, c_hi))
        fav_m.append(max(0.0, c_lo, c_hi))
        final += g0 + k * (g.close - g.open)
    pess = sum(min(0.0, a) for a in adv_m)
    total_fav = sum(fav_m)
    cands = [final]
    for g0, a, f in zip(gap, adv_m, fav_m, strict=True):
        others = total_fav - f
        cands.append(a + others)
        cands.append(g0 + others)
    return min(cands), min(pess, min(cands))


CERTAIN = "certain"
IMPOSSIBLE = "impossible"
UNCERTAIN = "uncertain"


def classify(optimistic_min: float, pessimistic_min: float, threshold: float) -> str:
    """Trigger class of a loss threshold (``threshold`` < 0, same unit as the bounds)."""
    if optimistic_min <= threshold:
        return CERTAIN
    if pessimistic_min > threshold:
        return IMPOSSIBLE
    return UNCERTAIN


# ----------------------------------------------------------------- hit rate
@dataclass
class HitStats:
    days: int = 0
    determined: int = 0
    hits: int = 0
    ties: int = 0  # open exactly half-way (tie-break rule used)
    same_bar: int = 0  # high and low in the same minute bar: order unknown

    @property
    def hit_rate(self) -> float | None:
        return self.hits / self.determined if self.determined else None

    def add(self, other: HitStats) -> None:
        self.days += other.days
        self.determined += other.determined
        self.hits += other.hits
        self.ties += other.ties
        self.same_bar += other.same_bar


def order_hit_rate(days: Iterable[Sequence[tuple[float, float, float, float]]], rule: str = "open_near_extreme") -> HitStats:
    """Hit rate of the order rule on intraday bar sequences (one ``[(o, h, l, c), ...]`` per day)."""
    st = HitStats()
    for bars in days:
        if not bars:
            continue
        st.days += 1
        o = bars[0][0]
        c = bars[-1][3]
        h = max(b[1] for b in bars)
        lo = min(b[2] for b in bars)
        ih = next(i for i, b in enumerate(bars) if b[1] == h)
        il = next(i for i, b in enumerate(bars) if b[2] == lo)
        if ih == il:
            st.same_bar += 1
            continue
        actual = KIND_HIGH if ih < il else KIND_LOW
        if h - o == o - lo:
            st.ties += 1
        predicted = extreme_order(o, h, lo, c, rule)[0]
        st.determined += 1
        if predicted == actual:
            st.hits += 1
    return st


__all__ = [
    "CERTAIN",
    "IMPOSSIBLE",
    "KIND_CLOSE",
    "KIND_HIGH",
    "KIND_INTERP",
    "KIND_LOW",
    "KIND_OPEN",
    "UNCERTAIN",
    "HitStats",
    "Leg",
    "Point",
    "TickGrid",
    "Timeline",
    "ANCHOR_FRACTIONS",
    "anchors_of",
    "bar_path",
    "bar_prices",
    "classify",
    "leg_counts",
    "extreme_order",
    "minute_bar_points",
    "order_hit_rate",
    "pnl_bounds",
    "synthesize",
]
