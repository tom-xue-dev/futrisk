"""Cached trading-day / session-window lookups for the md-gateway hot path (01 §8.2, §8.13).

The trading calendar builds absolute windows on every call; md-gateway asks the same
questions for every state (trading day of a timestamp, phase at a timestamp, "is this
contract in session now?"), so :class:`SessionIndex` memoizes per second and per
``(templates, trading_day)``, with the windows precomputed as plain integers
``(start_ns, end_ns, continuous)``. All answers come from the calendar and the session
templates of ``products.yaml`` (no weekday logic).
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date, timedelta

from futrisk.common.calendar import AbsWindow, TradingCalendar
from futrisk.common.clock import NS_PER_S, ns_to_bj, parse_td, td_str
from futrisk.marketdata.sessions import WindowCache

_TD_CACHE_MAX = 20_000
_BJ_OFFSET_S = 8 * 3600
_DAY_S = 86_400

CONTINUOUS = "CONTINUOUS"
AUCTION = "AUCTION"

IntWindow = tuple[int, int, bool]
"""``(start_ns, end_ns, continuous)`` of one session window."""


class SessionIndex:
    """Memoized calendar queries (not thread-safe; used from the event loop)."""

    def __init__(self, calendar: TradingCalendar) -> None:
        self.reload(calendar)

    def reload(self, calendar: TradingCalendar) -> None:
        """Switch to another calendar (staged config applied at a trading-day roll); clears the caches."""
        self._cal = calendar
        self._windows = WindowCache(calendar, max_entries=1024)
        self._td_by_sec: dict[int, str | None] = {}
        self._date_by_td: dict[str, date] = {}
        self._iw: dict[tuple[tuple[str, ...], str], tuple[IntWindow, ...]] = {}
        self._day_str: dict[int, str] = {}
        self._prev: dict[str, str | None] = {}

    @property
    def calendar(self) -> TradingCalendar:
        return self._cal

    # ------------------------------------------------------------ trading day
    def trading_day(self, ts_ns: int) -> str | None:
        """Trading day (``YYYYMMDD``) a timestamp belongs to; ``None`` on non-trading time."""
        sec = ts_ns // NS_PER_S
        hit = self._td_by_sec.get(sec, "")
        if hit != "":
            return hit
        try:
            d = self._cal.trading_day_for_ns(ts_ns)
        except LookupError:
            d = None
        td = td_str(d) if d is not None else None
        if len(self._td_by_sec) >= _TD_CACHE_MAX:
            self._td_by_sec.clear()
        self._td_by_sec[sec] = td
        return td

    def current_or_next(self, ts_ns: int) -> str:
        """Trading day of ``ts_ns``; on non-trading daytime the next one (bucket of received rows)."""
        td = self.trading_day(ts_ns)
        if td is not None:
            return td
        try:
            return td_str(self._cal.current_or_next_trading_day(ts_ns))
        except LookupError:
            return ns_to_bj(ts_ns).strftime("%Y%m%d")

    def next_trading_day(self, td: str) -> str | None:
        try:
            return td_str(self._cal.next_trading_day(self._date(td)))
        except LookupError:
            return None

    def prev_trading_day(self, td: str) -> str | None:
        hit = self._prev.get(td, "")
        if hit != "":
            return hit
        try:
            prev: str | None = td_str(self._cal.prev_trading_day(self._date(td)))
        except (LookupError, ValueError):
            prev = None
        self._prev[td] = prev
        return prev

    def bj_date(self, ts_ns: int) -> str:
        """Natural Beijing date ``YYYYMMDD`` of a timestamp (``Tick.action_day``), cached per day."""
        day = (ts_ns // NS_PER_S + _BJ_OFFSET_S) // _DAY_S
        hit = self._day_str.get(day)
        if hit is None:
            hit = (date(1970, 1, 1) + timedelta(days=day)).strftime("%Y%m%d")
            self._day_str[day] = hit
        return hit

    def _date(self, td: str) -> date:
        d = self._date_by_td.get(td)
        if d is None:
            d = parse_td(td)
            self._date_by_td[td] = d
        return d

    # ---------------------------------------------------------------- windows
    def windows(self, templates: Sequence[str], td: str) -> list[AbsWindow]:
        return self._windows.windows(templates, self._date(td))

    def int_windows(self, templates: Sequence[str], td: str) -> tuple[IntWindow, ...]:
        """The session windows of ``td`` as sorted ``(start_ns, end_ns, continuous)`` integers."""
        key = (templates if isinstance(templates, tuple) else tuple(templates), td)
        hit = self._iw.get(key)
        if hit is None:
            hit = tuple(
                sorted((w.start_ns, w.end_ns, w.kind == "continuous") for w in self._windows.windows(key[0], self._date(td)))
            )
            if len(self._iw) >= 4096:
                self._iw.clear()
            self._iw[key] = hit
        return hit

    def phase(self, templates: Sequence[str], td: str, ts_ns: int) -> str | None:
        """``CONTINUOUS`` / ``AUCTION`` when ``ts_ns`` lies in a window of ``td``, else ``None``."""
        for start, end, cont in self.int_windows(templates, td):
            if start <= ts_ns < end:
                return CONTINUOUS if cont else AUCTION
        return None

    def in_session(self, templates: Sequence[str], td: str, ts_ns: int, grace_ns: int = 0) -> bool:
        """``ts_ns`` in ``[start, end + grace]`` of a window of trading day ``td``."""
        for start, end, _ in self.int_windows(templates, td):  # noqa: SIM110 - hot path, no generator
            if start <= ts_ns <= end + grace_ns:
                return True
        return False

    def window_at(self, templates: Sequence[str], td: str, ts_ns: int, grace_ns: int = 0) -> IntWindow | None:
        """The window of ``td`` containing ``ts_ns`` (a closing grace after its end included)."""
        for w in self.int_windows(templates, td):
            if w[0] <= ts_ns <= w[1] + grace_ns:
                return w
        return None

    def open_now(self, templates: Sequence[str], now_ns: int, grace_ns: int = 0) -> str | None:
        """Trading day when a window (auction or continuous, + grace) of ``templates`` is open now."""
        td = self.trading_day(now_ns)
        if td is None:
            return None
        return td if self.in_session(templates, td, now_ns, grace_ns) else None

    def judging(self, templates: Sequence[str], now_ns: int, open_grace_ns: int) -> bool:
        """Staleness is judged only in continuous sessions, ``open_grace`` after each open (01 §8.13)."""
        td = self.trading_day(now_ns)
        if td is None:
            return False
        for start, end, cont in self.int_windows(templates, td):
            if cont and start + open_grace_ns <= now_ns < end:
                return True
        return False

    def day_close_ns(self, templates: Sequence[str], td: str) -> int | None:
        ws = self.int_windows(templates, td)
        return max((w[1] for w in ws), default=None)


__all__ = ["AUCTION", "CONTINUOUS", "IntWindow", "SessionIndex"]
