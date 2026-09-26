"""Exchange trading calendar and session windows (01 §8.2).

Rules implemented here (verified against SHFE's night-session FAQ):
- a trading day runs from the previous trading day's 21:00 to 15:00;
- timestamps from 20:00 belong to the next trading day; 00:00-03:00 belongs to
  "the next trading day after the previous natural date", so Friday night and
  Saturday early morning both belong to Monday;
- there is no night session when a public holiday sits between the previous
  trading day and the trading day (only weekends may lie in between); the
  commodity opening auction then moves to 08:55-09:00.

Everything is driven by the trading-day list, never by weekday logic alone.
Sessions are described by named templates (``products.yaml``), each a list of
windows ``[start, end, kind]`` in Beijing time. ``kind`` is ``auction``,
``auction_no_night`` (only when the trading day has no night session) or
``continuous``. Night windows are flagged by a start time at or after 20:00.
"""

from __future__ import annotations

import bisect
import csv
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path

from futrisk.common.clock import BJ, bj_to_ns, ns_to_bj, parse_td

NIGHT_START = time(20, 0)
EARLY_MORNING_END = time(3, 0)


@dataclass(frozen=True)
class Window:
    start: time
    end: time
    kind: str  # "auction" | "auction_no_night" | "continuous"

    @property
    def night(self) -> bool:
        return self.start >= NIGHT_START

    @property
    def is_auction(self) -> bool:
        return self.kind.startswith("auction")


@dataclass(frozen=True)
class AbsWindow:
    start: datetime
    end: datetime
    kind: str  # "auction" | "continuous"
    trading_day: date

    @property
    def start_ns(self) -> int:
        return bj_to_ns(self.start)

    @property
    def end_ns(self) -> int:
        return bj_to_ns(self.end)


@dataclass(frozen=True)
class Phase:
    """Session state of a product at an instant."""

    trading_day: date | None
    state: str  # "CONTINUOUS" | "AUCTION" | "CLOSED"
    window: AbsWindow | None = None

    @property
    def is_continuous(self) -> bool:
        return self.state == "CONTINUOUS"

    @property
    def is_open(self) -> bool:
        return self.state in ("CONTINUOUS", "AUCTION")


def _parse_hhmm(s: str) -> time:
    hh, mm = s.split(":")[:2]
    return time(int(hh), int(mm))


def parse_templates(raw: Mapping[str, Sequence[Sequence[str]]]) -> dict[str, list[Window]]:
    """Parse ``{name: [[start, end, kind], ...]}`` into Window lists."""
    out: dict[str, list[Window]] = {}
    for name, rows in raw.items():
        out[name] = [Window(_parse_hhmm(r[0]), _parse_hhmm(r[1]), r[2] if len(r) > 2 else "continuous") for r in rows]
    return out


class TradingCalendar:
    """Trading days plus session templates."""

    def __init__(self, trading_days: Iterable[date], templates: Mapping[str, list[Window]] | None = None) -> None:
        self._days: list[date] = sorted(set(trading_days))
        if not self._days:
            raise ValueError("trading calendar is empty")
        self._set = set(self._days)
        self._templates: dict[str, list[Window]] = dict(templates or {})

    # ------------------------------------------------------------------ loading
    @classmethod
    def from_csv(cls, path: str | Path, templates: Mapping[str, list[Window]] | None = None) -> TradingCalendar:
        days: list[date] = []
        with open(path, encoding="utf-8") as f:
            for row in csv.reader(f):
                if not row or row[0].startswith("#") or row[0] == "trading_day":
                    continue
                days.append(parse_td(row[0].strip()))
        return cls(days, templates)

    @property
    def first_day(self) -> date:
        return self._days[0]

    @property
    def last_day(self) -> date:
        return self._days[-1]

    @property
    def templates(self) -> dict[str, list[Window]]:
        return self._templates

    # ------------------------------------------------------------- day algebra
    def is_trading_day(self, d: date) -> bool:
        return d in self._set

    def next_trading_day(self, d: date) -> date:
        """First trading day strictly after ``d``."""
        i = bisect.bisect_right(self._days, d)
        if i >= len(self._days):
            raise LookupError(f"calendar has no trading day after {d}")
        return self._days[i]

    def prev_trading_day(self, d: date) -> date:
        """Last trading day strictly before ``d``."""
        i = bisect.bisect_left(self._days, d)
        if i == 0:
            raise LookupError(f"calendar has no trading day before {d}")
        return self._days[i - 1]

    def trading_days_between(self, start: date, end: date) -> list[date]:
        """Trading days in [start, end]."""
        lo = bisect.bisect_left(self._days, start)
        hi = bisect.bisect_right(self._days, end)
        return self._days[lo:hi]

    def nth_trading_day_before(self, d: date, n: int) -> date:
        """The trading day ``n`` trading days before ``d`` (``d`` itself if n == 0)."""
        i = bisect.bisect_left(self._days, d)
        if i >= len(self._days) or self._days[i] != d:
            raise LookupError(f"{d} is not a trading day")
        if i - n < 0:
            raise LookupError("not enough history in calendar")
        return self._days[i - n]

    def has_night_session(self, td: date) -> bool:
        """True when trading day ``td`` has a night session (on the previous trading day's evening)."""
        try:
            prev = self.prev_trading_day(td)
        except LookupError:
            return False
        d = prev + timedelta(days=1)
        while d < td:
            if d.weekday() < 5:  # a weekday holiday sits in between
                return False
            d += timedelta(days=1)
        return True

    def trading_day_for(self, dt: datetime) -> date | None:
        """Trading day a Beijing timestamp belongs to (None on non-trading daytime)."""
        local = dt.astimezone(BJ) if dt.tzinfo else dt.replace(tzinfo=BJ)
        d, t = local.date(), local.time()
        if t >= NIGHT_START:
            return self.next_trading_day(d)
        if t < EARLY_MORNING_END:
            return self.next_trading_day(d - timedelta(days=1))
        return d if self.is_trading_day(d) else None

    def trading_day_for_ns(self, ns: int) -> date | None:
        return self.trading_day_for(ns_to_bj(ns))

    # ------------------------------------------------------------------ windows
    def windows(self, template_names: Sequence[str], td: date) -> list[AbsWindow]:
        """Absolute session windows of trading day ``td`` for the given templates, sorted."""
        if not self.is_trading_day(td):
            return []
        night = self.has_night_session(td)
        prev = self.prev_trading_day(td) if night else None
        out: list[AbsWindow] = []
        for name in template_names:
            for w in self._templates.get(name, []):
                kind = "auction" if w.is_auction else "continuous"
                if w.night:
                    if not night or prev is None:
                        continue
                    start = datetime.combine(prev, w.start, tzinfo=BJ)
                    end_day = prev if w.end > w.start else prev + timedelta(days=1)
                    end = datetime.combine(end_day, w.end, tzinfo=BJ)
                else:
                    if w.kind == "auction_no_night" and night:
                        continue
                    start = datetime.combine(td, w.start, tzinfo=BJ)
                    end = datetime.combine(td, w.end, tzinfo=BJ)
                out.append(AbsWindow(start, end, kind, td))
        out.sort(key=lambda x: (x.start, x.end))
        return out

    def phase_at(self, template_names: Sequence[str], ts_ns: int) -> Phase:
        dt = ns_to_bj(ts_ns)
        td = self.trading_day_for(dt)
        if td is None:
            return Phase(None, "CLOSED")
        for w in self.windows(template_names, td):
            if w.start <= dt < w.end:
                return Phase(td, "CONTINUOUS" if w.kind == "continuous" else "AUCTION", w)
        return Phase(td, "CLOSED")

    def is_continuous(self, template_names: Sequence[str], ts_ns: int) -> bool:
        return self.phase_at(template_names, ts_ns).is_continuous

    def day_close(self, template_names: Sequence[str], td: date) -> datetime | None:
        """End of the last window of ``td`` (e.g. 15:00, or 15:15 for T)."""
        ws = self.windows(template_names, td)
        return max((w.end for w in ws), default=None)

    def first_open(self, template_names: Sequence[str], td: date) -> datetime | None:
        """Start of the first window of ``td`` (night auction, or day auction/open)."""
        ws = self.windows(template_names, td)
        return min((w.start for w in ws), default=None)

    def next_window_start(self, template_names: Sequence[str], ts_ns: int, include_auction: bool = True) -> AbsWindow | None:
        """First window starting strictly after ``ts_ns`` (searches up to 30 trading days ahead)."""
        dt = ns_to_bj(ts_ns)
        td = self.trading_day_for(dt)
        if td is None:
            try:
                td = self.next_trading_day(dt.date())
            except LookupError:
                return None
        for _ in range(30):
            for w in self.windows(template_names, td):
                if w.start > dt and (include_auction or w.kind == "continuous"):
                    return w
            try:
                td = self.next_trading_day(td)
            except LookupError:
                return None
        return None

    def current_or_next_trading_day(self, ts_ns: int) -> date:
        """Trading day of ``ts_ns``; on non-trading daytime, the next trading day."""
        dt = ns_to_bj(ts_ns)
        td = self.trading_day_for(dt)
        if td is not None:
            return td
        return self.next_trading_day(dt.date())


def weekday_calendar(start: date, end: date, holidays: Iterable[date] = ()) -> list[date]:
    """Helper for tests: weekdays in [start, end] minus ``holidays``."""
    hs = set(holidays)
    out: list[date] = []
    d = start
    while d <= end:
        if d.weekday() < 5 and d not in hs:
            out.append(d)
        d += timedelta(days=1)
    return out
