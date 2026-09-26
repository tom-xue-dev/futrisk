"""Settlement-price inputs from canonical ticks (01 §8.13 last bullet, §8.15, 02 §6.2/§7.1, 03 §4.1).

Everything is derived from **cumulative** values of the day's states, so gaps in the
middle of the day do not matter:

- **corroborated states only** (C1): a state followed (in exchange time) by a state with
  a lower cumulative volume was an outlier and is ignored, so one bad state can never
  become the day's "last" state; turnover-based values prefer states with exact turnover
  (Sina only estimates it, ``COARSE_TS`` rows are used only when nothing else exists);
- full day (SHFE/DCE ``VWAP_FULL_DAY``): ``day_volume`` / ``day_turnover`` of the day's
  last state, ``vwap_raw = turnover / (volume × multiplier)``;
- last hour (CFFEX ``VWAP_LAST_HOUR``, the product's ``settle_window``: IF 14:00-15:00,
  T 14:15-15:15): window values are the difference between the last state of the window
  and the **boundary state** (the last state before the window start, as the exchange
  snapshot would have it). ``quality = APPROX`` when a boundary state is missing, when it
  lies more than one feed interval (500 ms) before the window start while a significant
  volume (> 0.2 % of the window) traded between it and the first state in the window (the
  split is unknown), when no state is near the window end yet (e.g. intraday), or when a
  boundary state comes from BACKFILL (01 §8.13, 02 §7.1);
- ``hourly`` (CFFEX): ``{segment_start_ns: [volume, turnover]}`` over the trading-hour
  segments of :func:`futrisk.core.settle_price.trading_hour_segments`, for the
  hour-by-hour fallback: every state-to-state increment is attributed to the segment of
  the later state; a closing snapshot stamped within the close grace after a segment end
  belongs to the segment that just ended; opening-auction states belong to the first
  segment (the same attribution as :func:`futrisk.core.settle_price.trading_hour_buckets`
  for a trade tape). The buckets always sum to ``day_volume`` / ``day_turnover``;
- ``limit_lock``: UP / DOWN when every state of the last 5 minutes before the close (and
  the last one before them) was locked at that limit;
- ``last`` / ``bid1`` / ``ask1`` / ``pre_settle`` from the last corroborated state;
  ``NO_TRADE`` when the relevant volume (day, or window for the last-hour method) is zero.
"""

from __future__ import annotations

import bisect
from dataclasses import dataclass
from decimal import Decimal, localcontext

import pandas as pd

from futrisk.common.clock import NS_PER_MS, NS_PER_S
from futrisk.common.decimals import D
from futrisk.core._num import CLEARING_CTX, PRICE_QUANTUM, div, price_str
from futrisk.core.instruments import InstrumentRegistry
from futrisk.core.settle_price import trading_hour_segments
from futrisk.messages.enums import LimitLock, SettleInputQuality, SettleMethod, TickMode
from futrisk.messages.md import FLAG_COARSE_TS, FLAG_LIMIT_DOWN_LOCKED, FLAG_LIMIT_UP_LOCKED, SettleInput

#: the pre-window boundary state must be within one Feed A interval of the window start ...
BOUNDARY_TOL_NS = 500 * NS_PER_MS
#: ... unless the volume traded between it and the first in-window state is insignificant
STRADDLE_FRACTION = 0.002
#: the window end counts as observed when a state exists this close to it
END_TOL_NS = 60 * NS_PER_S
#: closing snapshots stamped this long after a segment end belong to that segment
CLOSE_GRACE_NS = 60 * NS_PER_S
LOCK_WINDOW_NS = 5 * 60 * NS_PER_S


@dataclass(frozen=True, slots=True)
class _St:
    ts: int
    volume: int
    turnover: float
    last: float
    bid1: float | None
    ask1: float | None
    pre_settle: float
    flags: int
    backfill: bool

    @property
    def coarse(self) -> bool:
        return bool(self.flags & FLAG_COARSE_TS)


class _Series:
    """States of one instrument ordered by exchange time (cumulative values non-decreasing)."""

    def __init__(self, states: list[_St]) -> None:
        self.states = sorted(states, key=lambda s: (s.ts, s.volume))
        self.ts = [s.ts for s in self.states]

    def last_before(self, t: int) -> _St | None:
        i = bisect.bisect_left(self.ts, t)
        return self.states[i - 1] if i > 0 else None

    def last_at_or_before(self, t: int) -> _St | None:
        i = bisect.bisect_right(self.ts, t)
        return self.states[i - 1] if i > 0 else None

    def first_at_or_after(self, t: int) -> _St | None:
        i = bisect.bisect_left(self.ts, t)
        return self.states[i] if i < len(self.states) else None

    def between(self, lo: int, hi: int) -> list[_St]:
        return self.states[bisect.bisect_left(self.ts, lo) : bisect.bisect_right(self.ts, hi)]


def _states(df: pd.DataFrame) -> list[_St]:
    out: list[_St] = []
    for r in df.itertuples(index=False):
        bid = r.bid1
        ask = r.ask1
        out.append(
            _St(
                ts=int(r.exch_ts_ns),
                volume=int(r.volume),
                turnover=float(r.turnover),
                last=float(r.last),
                bid1=None if pd.isna(bid) else float(bid),
                ask1=None if pd.isna(ask) else float(ask),
                pre_settle=float(r.pre_settle),
                flags=0 if pd.isna(r.flags) else int(r.flags),
                backfill=str(r.mode) == str(TickMode.BACKFILL),
            )
        )
    return out


def corroborated(states: list[_St]) -> list[_St]:
    """The largest set of states (sorted by time) whose cumulative volume never decreases (C1).

    Cumulative volume only grows in exchange time, so a state contradicted by the states
    around it was an outlier; keeping the longest non-decreasing subsequence drops the
    fewest states whatever the direction of the outlier (a high or a low volume).
    """
    ordered = sorted(states, key=lambda s: (s.ts, s.volume))
    n = len(ordered)
    if n <= 1:
        return ordered
    tails: list[int] = []  # volume at the end of the best subsequence of each length
    tail_idx: list[int] = []
    prev = [-1] * n
    for i, s in enumerate(ordered):
        j = bisect.bisect_right(tails, s.volume)
        if j == len(tails):
            tails.append(s.volume)
            tail_idx.append(i)
        else:
            tails[j] = s.volume
            tail_idx[j] = i
        prev[i] = tail_idx[j - 1] if j > 0 else -1
    out: list[_St] = []
    k = tail_idx[-1]
    while k >= 0:
        out.append(ordered[k])
        k = prev[k]
    out.reverse()
    return out


def _dec(x: float | None) -> str | None:
    return None if x is None else price_str(D(x))


def compute_settle_input(
    df: pd.DataFrame,
    *,
    instrument_id: str,
    trading_day: str,
    registry: InstrumentRegistry,
    computed_ns: int,
    day_close_ns: int | None = None,
    version: int = 1,
) -> SettleInput | None:
    """``SettleInput`` of one instrument from its canonical rows (``None`` without any state)."""
    raw_states = _states(df)
    if not raw_states:
        return None
    good = corroborated(raw_states)
    exact = [s for s in good if not s.coarse] or good
    series = _Series(exact)
    last = series.states[-1]
    info = registry.get(instrument_id)
    mult = info.multiplier
    method = info.spec.settle_method
    with localcontext(CLEARING_CTX):
        day_turn = D(last.turnover)
        raw = div(day_turn, Decimal(last.volume * mult)) if last.volume > 0 else None
        relevant_volume = last.volume
        approx = False
        w_start: int | None = None
        w_vol: int | None = None
        w_turn: str | None = None
        hourly: dict[str, list[str]] | None = None
        close_ns = day_close_ns if day_close_ns is not None else last.ts
        if method == SettleMethod.VWAP_LAST_HOUR:
            window = registry.settle_window_ns(instrument_id, trading_day)
            if window is not None:
                w_start, w_end = window
                if day_close_ns is None:
                    close_ns = w_end
                pre = series.last_before(w_start)
                post = series.first_at_or_after(w_start)
                end = series.last_at_or_before(w_end + END_TOL_NS)
                if post is None or end is None or end.ts < w_start:
                    w_vol, w_turn_d = 0, Decimal(0)  # 窗口内没有任何状态
                else:
                    base_vol = pre.volume if pre is not None else 0
                    base_turn = D(pre.turnover) if pre is not None else Decimal(0)
                    w_vol = max(0, end.volume - base_vol)
                    w_turn_d = D(end.turnover) - base_turn
                    # 01 §8.13 / 02 §7.1：窗口边界状态缺失、离窗口起点太远且跨界成交量显著、或来自补数时为 APPROX
                    if pre is None or (
                        w_start - pre.ts > BOUNDARY_TOL_NS and post.volume - pre.volume > STRADDLE_FRACTION * max(w_vol, 1)
                    ):
                        approx = True
                    if (pre is not None and pre.backfill) or post.backfill or end.backfill:
                        approx = True
                    if end.ts < w_end - END_TOL_NS:
                        approx = True  # 窗口终点尚无状态（盘中计算或收盘段缺失）
                w_turn = price_str(w_turn_d)
                raw = div(w_turn_d, Decimal(w_vol * mult)) if w_vol > 0 else None
                relevant_volume = w_vol
            hourly = _hourly(series, registry, instrument_id, trading_day) or None
        if relevant_volume <= 0:
            quality = SettleInputQuality.NO_TRADE
        elif approx:
            quality = SettleInputQuality.APPROX
        else:
            quality = SettleInputQuality.OK
        return SettleInput(
            trading_day=trading_day,
            instrument_id=instrument_id,
            exchange=info.exchange,
            method=method,
            multiplier=mult,
            tick_size=price_str(info.tick_size),
            day_volume=last.volume,
            day_turnover=price_str(day_turn),
            window_start_ns=w_start,
            window_volume=w_vol,
            window_turnover=w_turn,
            hourly=hourly,
            vwap_raw=price_str(raw.quantize(PRICE_QUANTUM)) if raw is not None else None,
            last=_dec(last.last),
            bid1=_dec(last.bid1),
            ask1=_dec(last.ask1),
            limit_lock=_limit_lock(_Series(good), close_ns),
            pre_settle=price_str(D(last.pre_settle)),
            quality=quality,
            version=version,
            computed_ns=computed_ns,
        )


def segment_of(segs: list[tuple[int, int]], ts: int, grace_ns: int = CLOSE_GRACE_NS) -> int:
    """Index of the trading-hour segment a state stamped ``ts`` reports on.

    Inside a segment: that segment. Within ``grace_ns`` after a segment end (closing
    snapshot): the segment that just ended. Otherwise (opening auction, a break): the
    first segment ending after ``ts``, the last one after the close.
    """
    for i, (start, end) in enumerate(segs):
        if start <= ts < end:
            return i
    ended = [i for i, (_, end) in enumerate(segs) if end <= ts <= end + grace_ns]
    if ended:
        return ended[-1]
    for i, (_, end) in enumerate(segs):
        if ts < end:
            return i
    return len(segs) - 1


def _hourly(series: _Series, registry: InstrumentRegistry, instrument_id: str, trading_day: str) -> dict[str, list[str]]:
    """Hourly buckets: each state-to-state increment goes to the segment of the later state (M5)."""
    segs = trading_hour_segments(registry, instrument_id, trading_day)
    if not segs:
        return {}
    acc: dict[int, tuple[int, Decimal]] = {}
    prev_v = 0
    prev_t = Decimal(0)
    for s in series.states:
        turn = D(s.turnover)
        dv = s.volume - prev_v
        dt = turn - prev_t
        if dv or dt:
            start = segs[segment_of(segs, s.ts)][0]
            v, t = acc.get(start, (0, Decimal(0)))
            acc[start] = (v + dv, t + dt)
        prev_v, prev_t = s.volume, turn
    return {str(k): [str(v), price_str(t)] for k, (v, t) in sorted(acc.items()) if v > 0}


def _limit_lock(series: _Series, close_ns: int) -> LimitLock:
    """UP/DOWN when every state of the last 5 minutes (and the one before them) was locked (01 §8.15)."""
    start = close_ns - LOCK_WINDOW_NS
    window = series.between(start, close_ns)
    before = series.last_before(start)
    if before is not None:
        window = [before, *window]
    if not window:
        return LimitLock.NONE
    if all(s.flags & FLAG_LIMIT_UP_LOCKED for s in window):
        return LimitLock.UP
    if all(s.flags & FLAG_LIMIT_DOWN_LOCKED for s in window):
        return LimitLock.DOWN
    return LimitLock.NONE


__all__ = [
    "BOUNDARY_TOL_NS",
    "CLOSE_GRACE_NS",
    "END_TOL_NS",
    "STRADDLE_FRACTION",
    "compute_settle_input",
    "corroborated",
    "segment_of",
]
