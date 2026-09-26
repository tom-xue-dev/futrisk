"""Stochastic intraday paths for layer A: the path ensemble (01 §8.16 日内路径).

A daily bar (O, H, L, C) admits many intraday paths, and a rule effect measured on one deterministic path
depends on that choice: the order of the extremes, their timing, the oscillation between them and whether the
contracts of a book reach their extremes at the same time. The ensemble replays K realizations of a
stochastic path model calibrated on the layer B minute bars instead:

- **order of the extremes**: the extreme nearer the open comes first with probability p (per product: the hit
  rate of the open-near-extreme rule on the ``order_period`` minute bars, shrunk toward the pooled rate with
  ``shrink_days`` pseudo-days);
- **timing**: the first extreme's time and the gap to the second one (fractions of the contract's trading
  time) are drawn from their empirical distribution on the ``calibration_period`` bars (per product with
  enough days, else pooled over the products with the same session shape);
- **oscillation**: between the anchors (O at 0, E1 at t1, E2 at t2, C at 1) the path is the straight line plus
  a Brownian bridge of volatility σ × (H − L) per √(trading day), clipped to [L, H]. σ is calibrated per product
  so that the model reproduces the minute bars' mean Σ|Δclose| / (H − L) at the calibration period; the fit
  at the other periods is reported as a check;
- **dependence**: the draws (order, t1, gap) of the products of one sector are correlated through a Gaussian
  copula with the within-sector correlation ρ (the rank correlation of the extremes' times on common days,
  converted with ρ = 2 sin(π ρ_S / 6)); different sectors are independent; the contracts of one product share
  the product's draws and noise, so a near/far pair keeps moving together;
- **determinism and pairing**: every draw is seeded by (seed, realization, sector or product, trading day)
  only, so a contract's path does not depend on the other contracts of a plan, and every arm of a realization
  replays exactly the same paths.

A path keeps the bar's O, H, L and C exactly. The replayed states are the fine path (one point per ``grid_s``
seconds of trading time) thinned to moves of at least ``max_step`` × open, with extra states inserted where
one fine step moves more, so a threshold is crossed in steps of at most ``max_step`` (plus one tick of
rounding to the price grid).

The calibration uses the minute bars that exist (recent months, ``data/hist/SOURCES.md``) and applies them to
the whole history: the ensemble's results are conditional on that model.
"""

from __future__ import annotations

import math
import zlib
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

import numpy as np

from futrisk.backtest.paths import (
    KIND_CLOSE,
    KIND_HIGH,
    KIND_INTERP,
    KIND_LOW,
    KIND_OPEN,
    NS_PER_S,
    Point,
    TickGrid,
    Timeline,
    order_hit_rate,
)

SQRT2 = math.sqrt(2.0)


def norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / SQRT2))


def stable_id(text: str) -> int:
    """A process-independent integer for seeding (``hash()`` of a str is salted per process)."""
    return zlib.crc32(text.encode("utf-8")) & 0x7FFFFFFF


def rng_for(*parts: int | str) -> np.random.Generator:
    return np.random.default_rng([p if isinstance(p, int) else stable_id(p) for p in parts])


def quantile_of(sorted_values: Sequence[float], u: float) -> float:
    """Linear interpolation between order statistics (``u`` in [0, 1])."""
    n = len(sorted_values)
    if n == 0:
        return u
    if n == 1:
        return float(sorted_values[0])
    x = min(max(u, 0.0), 1.0) * (n - 1)
    i = int(math.floor(x))
    if i >= n - 1:
        return float(sorted_values[-1])
    f = x - i
    return float(sorted_values[i] + f * (sorted_values[i + 1] - sorted_values[i]))


def spearman(a: Sequence[float], b: Sequence[float]) -> float:
    """Spearman rank correlation with average ranks for ties; NaN when either input is constant (or n < 2)."""
    n = len(a)
    if n != len(b) or n < 2:
        return float("nan")
    ra, rb = average_ranks(a), average_ranks(b)
    ma, mb = sum(ra) / n, sum(rb) / n
    va = sum((x - ma) ** 2 for x in ra)
    vb = sum((y - mb) ** 2 for y in rb)
    if va <= 1e-15 or vb <= 1e-15:
        return float("nan")
    cov = sum((x - ma) * (y - mb) for x, y in zip(ra, rb, strict=True))
    return cov / math.sqrt(va * vb)


def average_ranks(x: Sequence[float]) -> list[float]:
    order = sorted(range(len(x)), key=lambda i: x[i])
    r = [0.0] * len(x)
    i = 0
    while i < len(order):
        j = i
        while j + 1 < len(order) and x[order[j + 1]] == x[order[i]]:
            j += 1
        avg = (i + j) / 2.0
        for k in range(i, j + 1):
            r[order[k]] = avg
        i = j + 1
    return r


# ------------------------------------------------------------------ model
@dataclass(frozen=True)
class ProductPathParams:
    """Calibrated path parameters of one product."""

    p_near_first: float  # probability that the extreme nearer the open comes first
    sigma: float  # bridge volatility, in units of the day's range per sqrt(trading day)
    t_first: tuple[float, ...]  # sorted: time of the first extreme (fraction of trading time)
    t_gap: tuple[float, ...]  # sorted: (t_second - t_first) / (1 - t_first)
    source: str = ""


@dataclass(frozen=True)
class PathModel:
    """The ensemble's path model (picklable: shipped to the spawn workers with every task)."""

    products: dict[str, ProductPathParams]
    default: ProductPathParams
    sector_of: dict[str, str]
    sector_rho: dict[str, float]
    seed: int
    grid_s: float = 60.0
    report: dict[str, Any] = field(default_factory=dict, compare=False)

    def params(self, product: str) -> ProductPathParams:
        return self.products.get(product, self.default)

    def rho(self, product: str) -> float:
        return float(self.sector_rho.get(self.sector_of.get(product, product), 0.0))


# 0 < t1 < t2 < 1 on the empirical support; the uninformed default: extremes anywhere, the gap uniform
_UNIFORM = tuple(float(x) for x in np.linspace(0.02, 0.98, 49))
DEFAULT_PARAMS = ProductPathParams(p_near_first=0.8, sigma=0.0, t_first=_UNIFORM, t_gap=_UNIFORM, source="default")


def draws(model: PathModel, k: int, product: str, trading_day: str) -> tuple[float, float, float]:
    """Correlated uniforms (order, t1, gap) of one product-day of realization ``k``."""
    rho = model.rho(product)
    sector = model.sector_of.get(product, product)
    td = int(trading_day)
    zs = rng_for(model.seed, k, f"sector:{sector}", td).standard_normal(3)
    zp = rng_for(model.seed, k, f"product:{product}", td).standard_normal(3)
    a, b = math.sqrt(max(0.0, rho)), math.sqrt(max(0.0, 1.0 - rho))
    return norm_cdf(a * zs[0] + b * zp[0]), norm_cdf(a * zs[1] + b * zp[1]), norm_cdf(a * zs[2] + b * zp[2])


def near_extreme(o: float, h: float, lo: float, c: float) -> str:
    up, dn = h - o, o - lo
    if up < dn:
        return KIND_HIGH
    if dn < up:
        return KIND_LOW
    return KIND_LOW if c >= o else KIND_HIGH


def anchor_plan(
    o: float, h: float, lo: float, c: float, n_fine: int, params: ProductPathParams, u: tuple[float, float, float]
) -> tuple[str, int, int]:
    """(first extreme kind, fine index of E1, fine index of E2) of one path."""
    near = near_extreme(o, h, lo, c)
    far = KIND_LOW if near == KIND_HIGH else KIND_HIGH
    first = near if u[0] < params.p_near_first else far
    t1 = quantile_of(params.t_first, u[1])
    gap = quantile_of(params.t_gap, u[2])
    t2 = t1 + gap * (1.0 - t1)
    m = n_fine
    e1 = h if first == KIND_HIGH else lo
    e2 = lo if first == KIND_HIGH else h
    j1 = 0 if e1 == o else min(max(1, int(round(t1 * m))), m - 2)
    j2 = m if e2 == c and j1 < m else min(max(j1 + 1, int(round(t2 * m))), m - 1)
    if j2 <= j1:
        j2 = min(m, j1 + 1)
    return first, j1, j2


def fine_path(
    o: float, h: float, lo: float, c: float, n_fine: int, first: str, j1: int, j2: int, sigma: float,
    normals: np.ndarray,
) -> np.ndarray:
    """Prices at the fine indices 0..n_fine: straight legs through the anchors plus clipped Brownian bridges.

    ``normals`` holds ``n_fine`` standard normal increments (one per fine step, the legs take their slices).
    """
    m = n_fine
    e1 = h if first == KIND_HIGH else lo
    e2 = lo if first == KIND_HIGH else h
    anchors = [(0, o), (j1, e1), (j2, e2), (m, c)]
    out = np.empty(m + 1)
    scale = sigma * (h - lo) / math.sqrt(m) if m > 0 else 0.0
    for (ja, pa), (jb, pb) in zip(anchors, anchors[1:], strict=False):
        n = jb - ja
        if n <= 0:
            out[jb] = pb
            continue
        steps = np.arange(n + 1, dtype=float)
        base = pa + (pb - pa) * steps / n
        if scale > 0 and n > 1:
            w = np.concatenate([[0.0], np.cumsum(normals[ja:jb] * scale)])
            bridge = w - steps / n * w[-1]
            base = base + bridge
        out[ja : jb + 1] = base
    np.clip(out, lo, h, out=out)
    out[0] = o
    out[j1] = e1
    out[j2] = e2
    out[m] = c
    return out


def realize(
    model: PathModel, k: int, product: str, trading_day: str, o: float, h: float, lo: float, c: float, total_ns: int,
) -> tuple[np.ndarray, np.ndarray, str, int, int]:
    """``(fractions, prices, first, j1, j2)`` of the fine path of one product-day of realization ``k``."""
    params = model.params(product)
    m = max(8, int(round(total_ns / (model.grid_s * NS_PER_S))))
    u = draws(model, k, product, trading_day)
    first, j1, j2 = anchor_plan(o, h, lo, c, m, params, u)
    normals = rng_for(model.seed, k, f"noise:{product}", int(trading_day)).standard_normal(m)
    prices = fine_path(o, h, lo, c, m, first, j1, j2, params.sigma, normals)
    return np.arange(m + 1, dtype=float) / m, prices, first, j1, j2


def thin(
    fractions: np.ndarray, prices: np.ndarray, anchors: Mapping[int, str], grid: TickGrid, step: float
) -> list[tuple[float, float, str]]:
    """``(fraction, price, kind)`` states: moves of at least ``step`` (one tick minimum), anchors always kept,
    intermediate states inserted where one fine step moves more than ``step``.

    Works in integer ticks (the prices rounded half-to-even to the grid once, vectorised).
    """
    tick = grid.tick
    step_t = max(step, tick) / tick
    ticks = np.rint(np.asarray(prices, dtype=float) / tick).astype(np.int64).tolist()
    fr = np.asarray(fractions, dtype=float).tolist()
    digits = grid.digits

    def px(t: int) -> float:
        return round(t * tick, digits)

    out: list[tuple[float, float, str]] = [(fr[0], px(ticks[0]), anchors.get(0, KIND_OPEN))]
    last = ticks[0]
    n = len(ticks)
    lo_t, hi_t = step_t - 1e-9, step_t + 1e-9
    for j in range(1, n):
        t = ticks[j]
        move = t - last if t >= last else last - t
        if move < lo_t and j not in anchors:
            continue
        if move > hi_t:
            # a fine step larger than the resolution: walk it in steps of at most ``step``
            k = int(math.ceil(move / step_t - 1e-9))
            f0, f1 = fr[j - 1], fr[j]
            for i in range(1, k):
                q = int(round(last + (t - last) * i / k))
                qp = px(q)
                if qp != out[-1][1]:
                    out.append((max(f0 + (f1 - f0) * i / k, out[-1][0]), qp, KIND_INTERP))
        out.append((fr[j], px(t), anchors.get(j) or KIND_INTERP))
        last = t
    return out


def ensemble_points(
    model: PathModel,
    k: int,
    instrument_id: str,
    product: str,
    trading_day: str,
    o: float,
    h: float,
    lo: float,
    c: float,
    *,
    timeline: Timeline,
    grid: TickGrid,
    multiplier: int,
    max_step: float,
) -> list[Point]:
    """The replayed states of one contract-day of realization ``k`` (bar O, H, L, C kept exactly)."""
    fr, px, first, j1, j2 = realize(model, k, product, trading_day, o, h, lo, c, timeline.total_ns)
    m = len(px) - 1
    e2 = KIND_LOW if first == KIND_HIGH else KIND_HIGH
    anchors = {0: KIND_OPEN, j1: first if j1 > 0 else KIND_OPEN, j2: e2 if j2 < m else KIND_CLOSE, m: KIND_CLOSE}
    states = thin(fr, px, anchors, grid, max_step * o)
    margin = NS_PER_S
    span = max(0, timeline.total_ns - 2 * margin)
    pts: list[Point] = []
    last = -1
    cum_t = 0.0
    for j, (f, p, kind) in enumerate(states):
        ts = timeline.at(margin + int(round(span * f)))
        if ts <= last:
            ts = last + 1
        last = ts
        cum_t += p * multiplier
        pts.append(Point(ts, instrument_id, p, kind, j + 1, cum_t))
    return pts


# ------------------------------------------------------------------ calibration
@dataclass
class _Day:
    td: str
    o: float
    h: float
    lo: float
    c: float
    total_ns: int
    ends: list[int]  # trading-time offsets of the bar ends
    closes: list[float]
    f_high: float | None = None  # time of the high (bar midpoint, fraction of the trading day)
    f_low: float | None = None


def _minute_days(hist: Any, registry: Any, product: str, period: int) -> list[_Day]:
    try:
        bars = hist.minute_bars(product, period)
    except FileNotFoundError:
        return []
    by_day: dict[str, list[Any]] = {}
    inst_of: dict[str, str | None] = {}
    for b in bars:
        if b.trading_day not in inst_of:
            inst_of[b.trading_day] = b.instrument_id
        if b.instrument_id is not None and b.instrument_id == inst_of[b.trading_day]:
            by_day.setdefault(b.trading_day, []).append(b)
    out: list[_Day] = []
    per = period * 60 * NS_PER_S
    for td in sorted(by_day)[1:]:  # the first day of a file is usually partial
        bs = by_day[td]
        if len(bs) < 3:
            continue
        inst = bs[0].instrument_id
        try:
            ivs = registry.session_intervals(inst, td)
        except Exception:
            continue
        if not ivs:
            continue
        tl = Timeline(ivs)
        ends = [tl.offset_of(b.ts_ns) for b in bs]
        if tl.total_ns <= 0 or ends[-1] <= 0:
            continue
        h = max(b.high for b in bs)
        lo = min(b.low for b in bs)
        if h <= lo:
            continue
        ih = next(i for i, b in enumerate(bs) if b.high == h)
        il = next(i for i, b in enumerate(bs) if b.low == lo)
        d = _Day(td, bs[0].open, h, lo, bs[-1].close, tl.total_ns, ends, [b.close for b in bs])
        if ih != il:
            d.f_high = max(0.0, min(1.0, (ends[ih] - per / 2) / tl.total_ns))
            d.f_low = max(0.0, min(1.0, (ends[il] - per / 2) / tl.total_ns))
        out.append(d)
    return out


def _timing(days: Iterable[_Day]) -> tuple[list[float], list[float]]:
    t1: list[float] = []
    gap: list[float] = []
    for d in days:
        if d.f_high is None or d.f_low is None:
            continue
        a, b = sorted((d.f_high, d.f_low))
        a = min(max(a, 0.0), 0.999)
        t1.append(a)
        gap.append(min(max((b - a) / (1.0 - a), 1e-3), 1.0) if a < 1.0 else 1.0)
    return sorted(t1), sorted(gap)


def _efficiency(d: _Day) -> float:
    """Σ|Δclose| / (H − L) with the open as the first close."""
    prev = d.o
    s = 0.0
    for c in d.closes:
        s += abs(c - prev)
        prev = c
    return s / (d.h - d.lo)


def _simulated_efficiency(days: Sequence[_Day], params: ProductPathParams, sigma: float, grid_s: float, reps: int,
                          seed: int, tag: str) -> float:
    vals: list[float] = []
    p = ProductPathParams(params.p_near_first, sigma, params.t_first, params.t_gap)
    for i, d in enumerate(days):
        m = max(8, int(round(d.total_ns / (grid_s * NS_PER_S))))
        idx = [min(m, max(0, int(round(e / d.total_ns * m)))) for e in d.ends]
        for r in range(reps):
            rng = rng_for(seed, "calib", tag, i, r)
            u = (float(rng.random()), float(rng.random()), float(rng.random()))
            first, j1, j2 = anchor_plan(d.o, d.h, d.lo, d.c, m, p, u)
            px = fine_path(d.o, d.h, d.lo, d.c, m, first, j1, j2, sigma, rng.standard_normal(m))
            closes = px[idx]
            s = abs(float(closes[0]) - d.o) + float(np.abs(np.diff(closes)).sum())
            vals.append(s / (d.h - d.lo))
    return float(np.mean(vals)) if vals else float("nan")


def _fit_sigma(days: Sequence[_Day], params: ProductPathParams, target: float, grid_s: float, seed: int, tag: str,
               reps: int = 4, hi: float = 4.0, iters: int = 18) -> tuple[float, float, float]:
    """σ with simulated mean efficiency = ``target`` (bisection with common random numbers); (σ, fit, linear)."""
    lin = _simulated_efficiency(days, params, 0.0, grid_s, reps, seed, tag)
    if not math.isfinite(target) or target <= lin:
        return 0.0, lin, lin
    top = _simulated_efficiency(days, params, hi, grid_s, reps, seed, tag)
    if top < target:
        return hi, top, lin
    a, b = 0.0, hi
    for _ in range(iters):
        mid = (a + b) / 2
        v = _simulated_efficiency(days, params, mid, grid_s, reps, seed, tag)
        if v < target:
            a = mid
        else:
            b = mid
    s = (a + b) / 2
    return s, _simulated_efficiency(days, params, s, grid_s, reps, seed, tag), lin


def calibrate(
    hist: Any,
    registry: Any,
    products: Sequence[str],
    *,
    seed: int,
    calibration_period: int = 15,
    order_period: int = 60,
    check_periods: Sequence[int] = (5, 60),
    grid_s: float = 60.0,
    shrink_days: int = 20,
    min_days: int = 20,
    sigma: float | None = None,
    rho: float | None = None,
) -> PathModel:
    """Calibrate the path model on the minute bars of ``hist`` (see the module docstring)."""
    sector_of = {p: registry.products.products[p].sector for p in products if p in registry.products.products}
    shape_of = {p: tuple(registry.products.products[p].sessions) for p in sector_of}
    order: dict[str, tuple[int, int]] = {}
    cal_days: dict[str, list[_Day]] = {}
    for p in sector_of:
        try:
            bars = hist.minute_bars(p, order_period)
        except FileNotFoundError:
            bars = []
        by_day: dict[str, list[tuple[float, float, float, float]]] = {}
        inst_of: dict[str, str | None] = {}
        for b in bars:
            if b.trading_day not in inst_of:
                inst_of[b.trading_day] = b.instrument_id
            if b.instrument_id == inst_of[b.trading_day]:
                by_day.setdefault(b.trading_day, []).append((b.open, b.high, b.low, b.close))
        st = order_hit_rate(by_day[d] for d in sorted(by_day)[1:])
        order[p] = (st.hits, st.determined)
        cal_days[p] = _minute_days(hist, registry, p, calibration_period)
    hits = sum(h for h, _n in order.values())
    det = sum(n for _h, n in order.values())
    p_pool = hits / det if det else DEFAULT_PARAMS.p_near_first
    pooled_shape: dict[tuple[str, ...], list[_Day]] = {}
    for p, ds in cal_days.items():
        pooled_shape.setdefault(shape_of[p], []).extend(ds)
    all_days = [d for ds in cal_days.values() for d in ds]
    t1_all, gap_all = _timing(all_days)
    params: dict[str, ProductPathParams] = {}
    report: dict[str, Any] = {"calibration_period": calibration_period, "order_period": order_period, "grid_s": grid_s,
                              "p_pooled": p_pool, "products": {}, "sectors": {}}
    for p in sector_of:
        h_, n_ = order[p]
        p_near = (h_ + shrink_days * p_pool) / (n_ + shrink_days)
        own_t1, own_gap = _timing(cal_days[p])
        if len(own_t1) >= min_days:
            t1, gap, tsrc = own_t1, own_gap, f"{p} {calibration_period}m ({len(own_t1)} d)"
        else:
            sh_t1, sh_gap = _timing(pooled_shape[shape_of[p]])
            if len(sh_t1) >= min_days:
                t1, gap, tsrc = sh_t1, sh_gap, f"pooled session shape ({len(sh_t1)} d)"
            else:
                t1, gap, tsrc = t1_all, gap_all, f"pooled all ({len(t1_all)} d)"
        if not t1:
            t1, gap = list(DEFAULT_PARAMS.t_first), list(DEFAULT_PARAMS.t_gap)
            tsrc = "default (no minute data)"
        base = ProductPathParams(p_near, 0.0, tuple(t1), tuple(gap), tsrc)
        days = cal_days[p] if len(cal_days[p]) >= min_days else pooled_shape.get(shape_of[p], [])
        eff = [_efficiency(d) for d in days]
        target = float(np.mean(eff)) if eff else float("nan")
        if sigma is not None:
            sig = float(sigma)
            fit = _simulated_efficiency(days, base, sig, grid_s, 4, seed, p) if days else float("nan")
            lin = _simulated_efficiency(days, base, 0.0, grid_s, 4, seed, p) if days else float("nan")
        elif days:
            sig, fit, lin = _fit_sigma(days, base, target, grid_s, seed, p)
        else:
            sig, fit, lin = 0.0, float("nan"), float("nan")
        params[p] = ProductPathParams(p_near, sig, tuple(t1), tuple(gap), tsrc)
        checks: dict[str, Any] = {}
        for cp in check_periods:
            cd = _minute_days(hist, registry, p, cp)
            if len(cd) < 5:
                continue
            checks[f"{cp}m"] = {
                "days": len(cd),
                "empirical": float(np.mean([_efficiency(d) for d in cd])),
                "model": _simulated_efficiency(cd, params[p], sig, grid_s, 2, seed, f"{p}:{cp}"),
                "linear": _simulated_efficiency(cd, params[p], 0.0, grid_s, 2, seed, f"{p}:{cp}"),
            }
        report["products"][p] = {
            "sector": sector_of[p], "order_days": n_, "order_hits": h_,
            "hit_rate": h_ / n_ if n_ else None, "p_near_first": p_near, "timing": tsrc,
            "timing_days": len(own_t1), "calibration_days": len(days), "sigma": sig,
            f"efficiency_{calibration_period}m": {"empirical": target, "model": fit, "linear": lin},
            "checks": checks,
            "t_first_quartiles": [quantile_of(t1, q) for q in (0.25, 0.5, 0.75)] if t1 else None,
        }
    # within-sector dependence of the extremes' timing
    sector_rho: dict[str, float] = {}
    by_sector: dict[str, list[str]] = {}
    for p, s in sector_of.items():
        by_sector.setdefault(s, []).append(p)
    for s, ps in sorted(by_sector.items()):
        rs: list[float] = []
        pairs: list[dict[str, Any]] = []
        for i, a in enumerate(sorted(ps)):
            for b in sorted(ps)[i + 1 :]:
                da = {d.td: d for d in cal_days.get(a, []) if d.f_low is not None}
                db = {d.td: d for d in cal_days.get(b, []) if d.f_low is not None}
                common = sorted(set(da) & set(db))
                if len(common) < 15:
                    continue
                r_low = spearman([da[t].f_low or 0.0 for t in common], [db[t].f_low or 0.0 for t in common])
                r_high = spearman([da[t].f_high or 0.0 for t in common], [db[t].f_high or 0.0 for t in common])
                vals = [x for x in (r_low, r_high) if math.isfinite(x)]
                if vals:
                    rs.append(sum(vals) / len(vals))
                    pairs.append({"pair": f"{a}/{b}", "days": len(common), "spearman_low": r_low, "spearman_high": r_high})
        rho_s = sum(rs) / len(rs) if rs else 0.0
        g = 2.0 * math.sin(math.pi * rho_s / 6.0) if rs else 0.0
        g = min(max(g, 0.0), 0.95)
        if rho is not None:
            g = float(rho)
        sector_rho[s] = g
        report["sectors"][s] = {"products": sorted(ps), "rank_correlation": rho_s if rs else None, "rho": g, "pairs": pairs}
    default = ProductPathParams(p_pool, float(np.median([x.sigma for x in params.values()])) if params else 0.0,
                                tuple(t1_all) or DEFAULT_PARAMS.t_first, tuple(gap_all) or DEFAULT_PARAMS.t_gap, "pooled")
    return PathModel(products=params, default=default, sector_of=sector_of, sector_rho=sector_rho, seed=seed,
                     grid_s=grid_s, report=report)


def fixed_model(products: Mapping[str, str], *, seed: int, p_near_first: float = 0.8, sigma: float = 0.5,
                rho: float = 0.5, grid_s: float = 60.0) -> PathModel:
    """An uncalibrated model (tests, and runs without minute data): uniform timing, fixed p / σ / ρ."""
    base = ProductPathParams(p_near_first, sigma, DEFAULT_PARAMS.t_first, DEFAULT_PARAMS.t_gap, "fixed")
    return PathModel(products={p: base for p in products}, default=base, sector_of=dict(products),
                     sector_rho=dict.fromkeys(set(products.values()), rho), seed=seed, grid_s=grid_s,
                     report={"fixed": True, "p_near_first": p_near_first, "sigma": sigma, "rho": rho})


__all__ = [
    "DEFAULT_PARAMS",
    "PathModel",
    "ProductPathParams",
    "anchor_plan",
    "average_ranks",
    "calibrate",
    "draws",
    "ensemble_points",
    "fine_path",
    "fixed_model",
    "near_extreme",
    "norm_cdf",
    "quantile_of",
    "realize",
    "rng_for",
    "spearman",
    "stable_id",
    "thin",
]
