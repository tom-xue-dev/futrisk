"""Session-window helpers for market-data sources (01 §8.2, 01 §8.13).

- :class:`SessionGate` answers "is it worth polling now?": inside any session
  window (auction or continuous) of the given templates, widened by a margin
  before the open and after the close. Sina only has fresh data during
  sessions, and polling outside them wastes requests and risks an IP ban
  (02 §9.3).
- :func:`in_session` answers "does this exchange timestamp lie in a session of
  its trading day?" with a grace period after each window end, so that the
  closing snapshot (``15:00:00``, or DCE's late ``15:04:39`` broadcast) is
  not treated as out-of-session.

All decisions come from the trading calendar and the session templates of
``products.yaml``; there is no weekday logic here.
"""

from __future__ import annotations

import contextlib
from collections.abc import Iterable, Sequence
from datetime import date

from futrisk.common.calendar import AbsWindow, TradingCalendar
from futrisk.common.clock import NS_PER_S


class WindowCache:
    """Memoizes ``calendar.windows(templates, td)``; windows never change for a given config."""

    def __init__(self, calendar: TradingCalendar, max_entries: int = 256) -> None:
        self._calendar = calendar
        self._max = max_entries
        self._cache: dict[tuple[tuple[str, ...], date], list[AbsWindow]] = {}

    @property
    def calendar(self) -> TradingCalendar:
        return self._calendar

    def windows(self, templates: Sequence[str], td: date) -> list[AbsWindow]:
        key = (tuple(templates), td)
        hit = self._cache.get(key)
        if hit is None:
            if len(self._cache) >= self._max:
                self._cache.clear()
            hit = self._calendar.windows(list(templates), td)
            self._cache[key] = hit
        return hit


def in_session(
    windows: WindowCache,
    templates: Sequence[str],
    trading_day: date,
    ts_ns: int,
    *,
    close_grace_s: float = 0.0,
) -> bool:
    """True when ``ts_ns`` lies in ``[start, end + grace]`` of a window of ``trading_day``."""
    grace_ns = int(close_grace_s * NS_PER_S)
    return any(w.start_ns <= ts_ns <= w.end_ns + grace_ns for w in windows.windows(templates, trading_day))


class SessionGate:
    """Polling gate: active inside any widened session window of the given templates."""

    def __init__(
        self,
        calendar: TradingCalendar,
        templates: Iterable[str],
        *,
        pre_margin_s: float = 180.0,
        post_margin_s: float = 300.0,
    ) -> None:
        self._calendar = calendar
        self._templates = tuple(dict.fromkeys(templates))
        if not self._templates:
            raise ValueError("SessionGate needs at least one session template")
        self._pre_ns = int(pre_margin_s * NS_PER_S)
        self._post_ns = int(post_margin_s * NS_PER_S)
        self._cache = WindowCache(calendar)

    @property
    def templates(self) -> tuple[str, ...]:
        return self._templates

    def _trading_days_around(self, now_ns: int) -> list[date]:
        cal = self._calendar
        try:
            td = cal.current_or_next_trading_day(now_ns)
        except LookupError:
            return []
        days = [td]
        with contextlib.suppress(LookupError):
            days.insert(0, cal.prev_trading_day(td))
        with contextlib.suppress(LookupError):
            days.append(cal.next_trading_day(td))
        return days

    def windows_around(self, now_ns: int) -> list[tuple[int, int]]:
        """Widened, merged ``(start_ns, end_ns)`` spans of the previous, current and next trading day."""
        spans: list[tuple[int, int]] = []
        for td in self._trading_days_around(now_ns):
            for w in self._cache.windows(self._templates, td):
                spans.append((w.start_ns - self._pre_ns, w.end_ns + self._post_ns))
        spans.sort()
        merged: list[tuple[int, int]] = []
        for start, end in spans:
            if merged and start <= merged[-1][1]:
                merged[-1] = (merged[-1][0], max(merged[-1][1], end))
            else:
                merged.append((start, end))
        return merged

    def is_active(self, now_ns: int) -> bool:
        return any(start <= now_ns < end for start, end in self.windows_around(now_ns))

    def active_until_ns(self, now_ns: int) -> int | None:
        """End of the widened span containing ``now_ns`` (``None`` when inactive)."""
        for start, end in self.windows_around(now_ns):
            if start <= now_ns < end:
                return end
        return None

    def next_active_ns(self, now_ns: int) -> int | None:
        """``now_ns`` when active, else the start of the next widened span (``None`` past the calendar)."""
        future: list[int] = []
        for start, end in self.windows_around(now_ns):
            if start <= now_ns < end:
                return now_ns
            if start > now_ns:
                future.append(start)
        if future:
            return min(future)
        w = self._calendar.next_window_start(list(self._templates), now_ns)
        return None if w is None else w.start_ns - self._pre_ns
