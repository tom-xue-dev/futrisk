"""1-minute bars from LIVE + BACKFILL states (01 §5.3.2 BarBuilder, 02 §7.1 Bar, 03 §4.1 bars_1m).

- Bars are aligned to exchange-time minutes and belong to the state's trading day.
- ``volume`` / ``turnover`` are **differences of the cumulative values** at the minute
  boundaries: ``cum(close of m) - cum(close of the previous minute with data)``. A data
  gap therefore never loses volume: the first minute after it absorbs the missing part,
  and when a backfill later supplies the missing minutes the bars are revised.
- OHLC come from the states of the minute; when the day's high/low (``Tick.high/low``)
  moved beyond what the previous minute had seen, the bar's high/low is extended to it
  (02 §7.1: 缺口期间日内最高、最低价发生变化时扩展受影响的 K 线).
- Session-aware minutes (L9): a closing snapshot stamped within the close grace after a
  session window ends belongs to the window's **last** minute; a state of an opening
  auction belongs to the **first** minute of the continuous window that follows; the
  span of the first bar after a session break starts at the window start, so it never
  covers the break.
- A bar is published ``bar_close_grace_s`` after its minute ends; any later change
  (backfill, re-attributed volume, gap status) publishes it again with ``version + 1``.
  :meth:`BarBuilder.due` with ``final=True`` (service stop) publishes only minutes that
  have ended: an incomplete current minute is never published as if final.
- ``quality``: ``GAP_UNFILLED`` when the bar's span (its minute plus the minutes it
  absorbed) overlaps an open or unrecoverable data gap, ``GAP_FILLED`` when it overlaps
  only repaired gaps, ``OK`` otherwise.
"""

from __future__ import annotations

import bisect
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field

from futrisk.messages.enums import BarQuality, GapStatus
from futrisk.messages.md import Bar, Tick
from futrisk.services.md_gateway.sessions import IntWindow

MINUTE_NS = 60_000_000_000
_KEEP_DAYS = 2

GapQuery = Callable[[int, int], GapStatus | None]
WindowsOf = Callable[[str, str], Sequence[IntWindow]]
"""``(instrument_id, trading_day) -> session windows`` (sorted ``(start_ns, end_ns, continuous)``)."""


@dataclass
class _Minute:
    minute_ns: int
    open: float
    open_key: tuple[int, int]
    high: float
    low: float
    close: float
    close_key: tuple[int, int]
    cum_vol: int
    cum_turn: float
    oi: int
    day_high: float | None = None
    day_low: float | None = None
    version: int = 0
    published: tuple[object, ...] | None = None


@dataclass
class _Day:
    instrument_id: str
    trading_day: str
    minutes: dict[int, _Minute] = field(default_factory=dict)
    keys: list[int] = field(default_factory=list)

    def prev(self, minute_ns: int) -> _Minute | None:
        i = bisect.bisect_left(self.keys, minute_ns)
        return self.minutes[self.keys[i - 1]] if i > 0 else None

    def next(self, minute_ns: int) -> _Minute | None:
        i = bisect.bisect_right(self.keys, minute_ns)
        return self.minutes[self.keys[i]] if i < len(self.keys) else None


class BarBuilder:
    """Builds and revises 1-minute bars (event-loop thread only).

    Args:
        gap_status: data-gap overlap query (:meth:`GapBook.overlap`).
        close_grace_ns: publish delay after a minute ends (``bar_close_grace_s``).
        windows_of: session windows of an instrument's trading day (session-aware minutes);
            without it minutes are plain clock minutes.
        session_grace_ns: closing snapshots this long after a window end belong to its last minute.
    """

    def __init__(
        self,
        gap_status: GapQuery,
        *,
        close_grace_ns: int = 3_000_000_000,
        windows_of: WindowsOf | None = None,
        session_grace_ns: int = 60_000_000_000,
    ) -> None:
        self._gap_status = gap_status
        self._grace = close_grace_ns
        self._windows_of = windows_of
        self._session_grace = session_grace_ns
        self._days: dict[tuple[str, str], _Day] = {}
        self._dirty: set[tuple[str, str, int]] = set()

    def minute_of(self, instrument_id: str, trading_day: str, ts_ns: int) -> int:
        """Bar minute of a state stamped ``ts_ns`` (session-aware, see the module docstring)."""
        plain = ts_ns - ts_ns % MINUTE_NS
        if self._windows_of is None:
            return plain
        windows = self._windows_of(instrument_id, trading_day)
        for i, (start, end, cont) in enumerate(windows):
            if start <= ts_ns < end:
                if cont:
                    return plain
                for s2, _e2, c2 in windows[i + 1 :]:
                    if c2 and s2 >= end - MINUTE_NS:
                        return s2 - s2 % MINUTE_NS  # 集合竞价的成交归入随后连续交易的第一分钟
                return plain
            if cont and end <= ts_ns <= end + self._session_grace:
                last = end - 1
                return last - last % MINUTE_NS  # 收盘后迟到的快照归入刚结束的最后一分钟
        return plain

    def _span_floor(self, instrument_id: str, trading_day: str, minute_ns: int) -> int | None:
        """Start of the continuous window containing ``minute_ns`` (the first bar after a break starts there)."""
        if self._windows_of is None:
            return None
        for start, end, cont in self._windows_of(instrument_id, trading_day):
            if cont and start - start % MINUTE_NS <= minute_ns < end:
                return start
        return None

    # ------------------------------------------------------------------ input
    def load(self, bars: list[Bar]) -> None:
        """Restore published bars of the current day (restart continuity)."""
        for b in sorted(bars, key=lambda x: (x.instrument_id, x.trading_day, x.minute_start_ns)):
            day = self._day(b.instrument_id, b.trading_day)
            prev = day.minutes[day.keys[-1]] if day.keys else None
            cum_vol = (prev.cum_vol if prev else 0) + b.volume
            cum_turn = (prev.cum_turn if prev else 0.0) + b.turnover
            m = _Minute(
                minute_ns=b.minute_start_ns,
                open=b.open,
                open_key=(cum_vol - b.volume, b.minute_start_ns),
                high=b.high,
                low=b.low,
                close=b.close,
                close_key=(cum_vol, b.minute_start_ns + MINUTE_NS - 1),
                cum_vol=cum_vol,
                cum_turn=cum_turn,
                oi=b.open_interest,
                day_high=b.high,
                day_low=b.low,
                version=b.version,
            )
            m.published = _signature(b.open, b.high, b.low, b.close, b.volume, b.turnover, b.open_interest, b.quality)
            day.minutes[b.minute_start_ns] = m
            bisect.insort(day.keys, b.minute_start_ns)

    def update(self, tick: Tick) -> None:
        """Add one accepted (LIVE or BACKFILL) state."""
        day = self._day(tick.instrument_id, tick.trading_day)
        minute = self.minute_of(tick.instrument_id, tick.trading_day, tick.exch_ts_ns)
        key = (tick.volume, tick.exch_ts_ns)
        m = day.minutes.get(minute)
        if m is None:
            m = _Minute(
                minute_ns=minute,
                open=tick.last,
                open_key=key,
                high=tick.last,
                low=tick.last,
                close=tick.last,
                close_key=key,
                cum_vol=tick.volume,
                cum_turn=tick.turnover,
                oi=tick.open_interest,
                day_high=tick.high,
                day_low=tick.low,
            )
            day.minutes[minute] = m
            bisect.insort(day.keys, minute)
            nxt = day.next(minute)
            if nxt is not None:
                self._dirty.add((day.instrument_id, day.trading_day, nxt.minute_ns))
        else:
            if key < m.open_key:
                m.open, m.open_key = tick.last, key
            if key > m.close_key:
                changed_cum = tick.volume != m.cum_vol or tick.turnover != m.cum_turn
                m.close, m.close_key = tick.last, key
                m.cum_vol, m.cum_turn, m.oi = tick.volume, tick.turnover, tick.open_interest
                if changed_cum:
                    nxt = day.next(minute)
                    if nxt is not None:
                        self._dirty.add((day.instrument_id, day.trading_day, nxt.minute_ns))
            m.high = max(m.high, tick.last)
            m.low = min(m.low, tick.last)
            if tick.high is not None:
                m.day_high = tick.high if m.day_high is None else max(m.day_high, tick.high)
            if tick.low is not None:
                m.day_low = tick.low if m.day_low is None else min(m.day_low, tick.low)
        self._dirty.add((day.instrument_id, day.trading_day, minute))

    def touch(self, from_ns: int, to_ns: int) -> None:
        """A gap overlapping ``[from_ns, to_ns)`` changed status: re-evaluate the bars around it."""
        lo = from_ns - MINUTE_NS
        for day in self._days.values():
            i = bisect.bisect_left(day.keys, lo)
            while i < len(day.keys) and day.keys[i] <= to_ns + MINUTE_NS:
                self._dirty.add((day.instrument_id, day.trading_day, day.keys[i]))
                i += 1

    # ----------------------------------------------------------------- output
    def due(self, now_ns: int, *, force: bool = False, final: bool = False) -> list[Bar]:
        """Bars to publish now: minutes closed for ``grace`` that are new or changed.

        ``final`` (service stop): minutes that have ended, without waiting for the grace; the
        current, incomplete minute stays unpublished. ``force``: everything (tests, diagnostics).
        """
        out: list[Bar] = []
        for iid, td, minute in sorted(self._dirty):
            day = self._days.get((iid, td))
            if day is None or minute not in day.minutes:
                self._dirty.discard((iid, td, minute))
                continue
            end = minute + MINUTE_NS
            if not force and now_ns < end + (0 if final else self._grace):
                continue
            self._dirty.discard((iid, td, minute))
            bar = self._build(day, day.minutes[minute])
            if bar is not None:
                out.append(bar)
        return out

    def bars(self, instrument_id: str, trading_day: str) -> list[Bar]:
        """Current (possibly unpublished) bars of one instrument and day (tests, diagnostics)."""
        day = self._days.get((instrument_id, trading_day))
        if day is None:
            return []
        out = []
        for k in day.keys:
            m = day.minutes[k]
            o, h, lo, c, v, t, oi, q = self._values(day, m)
            out.append(_bar(day, m, o, h, lo, c, v, t, oi, q, m.version))
        return out

    # --------------------------------------------------------------- internals
    def _day(self, instrument_id: str, trading_day: str) -> _Day:
        day = self._days.get((instrument_id, trading_day))
        if day is None:
            day = _Day(instrument_id, trading_day)
            self._days[(instrument_id, trading_day)] = day
            mine = sorted(td for (iid, td) in self._days if iid == instrument_id)
            for old in mine[: max(0, len(mine) - _KEEP_DAYS)]:
                del self._days[(instrument_id, old)]
                self._dirty = {d for d in self._dirty if not (d[0] == instrument_id and d[1] == old)}
        return day

    def _values(self, day: _Day, m: _Minute) -> tuple[float, float, float, float, int, float, int, BarQuality]:
        prev = day.prev(m.minute_ns)
        volume = m.cum_vol - (prev.cum_vol if prev else 0)
        turnover = m.cum_turn - (prev.cum_turn if prev else 0.0)
        high, low = m.high, m.low
        prev_high = prev.day_high if prev is not None else None
        prev_low = prev.day_low if prev is not None else None
        if m.day_high is not None and (prev_high is None or m.day_high > prev_high):
            high = max(high, m.day_high)
        if m.day_low is not None and (prev_low is None or m.day_low < prev_low):
            low = min(low, m.day_low)
        span_start = prev.minute_ns + MINUTE_NS if prev is not None else m.minute_ns
        floor = self._span_floor(day.instrument_id, day.trading_day, m.minute_ns)
        if floor is not None and span_start < floor:
            span_start = floor  # 休市后的第一根 K 线不跨越休市时段
        status = self._gap_status(min(span_start, m.minute_ns), m.minute_ns + MINUTE_NS)
        if status is None:
            quality = BarQuality.OK
        elif status == GapStatus.FILLED:
            quality = BarQuality.GAP_FILLED
        else:
            quality = BarQuality.GAP_UNFILLED
        return m.open, high, low, m.close, max(0, volume), max(0.0, turnover), m.oi, quality

    def _build(self, day: _Day, m: _Minute) -> Bar | None:
        o, h, lo, c, v, t, oi, q = self._values(day, m)
        sig = _signature(o, h, lo, c, v, t, oi, q)
        if sig == m.published:
            return None
        m.version += 1
        m.published = sig
        return _bar(day, m, o, h, lo, c, v, t, oi, q, m.version)


def _signature(o: float, h: float, lo: float, c: float, v: int, t: float, oi: int, q: BarQuality) -> tuple[object, ...]:
    return (round(o, 6), round(h, 6), round(lo, 6), round(c, 6), v, round(t, 2), oi, str(q))


def _bar(day: _Day, m: _Minute, o: float, h: float, lo: float, c: float, v: int, t: float, oi: int, q: BarQuality, version: int) -> Bar:
    return Bar(
        instrument_id=day.instrument_id,
        trading_day=day.trading_day,
        minute_start_ns=m.minute_ns,
        open=o,
        high=h,
        low=lo,
        close=c,
        volume=v,
        turnover=round(t, 2),
        open_interest=oi,
        quality=q,
        version=max(1, version),
    )


__all__ = ["MINUTE_NS", "BarBuilder"]
