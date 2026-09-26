"""Clocks: wall/monotonic time and the market clock (01 §8.2, 02 §8.8).

Two kinds of time are used everywhere:
- **market time** decides trading days, sessions, settlement windows, EOD
  scheduling and the provisional-SOD trigger. In production it equals the
  wall clock; in demo mode it is wall clock + a shared offset published by the
  supervisor on ``sys.clock``;
- **monotonic time** drives heartbeats, timeouts, staleness timers and latency
  statistics and is never offset.

On Windows ``time.time()`` only has ~15.6ms resolution, so :class:`SystemClock`
anchors the wall clock once and advances it with ``perf_counter_ns``.
"""

from __future__ import annotations

import threading
import time
from datetime import date, datetime
from typing import Protocol
from zoneinfo import ZoneInfo

BJ = ZoneInfo("Asia/Shanghai")
NS_PER_S = 1_000_000_000
NS_PER_MS = 1_000_000


class Clock(Protocol):
    def now_ns(self) -> int:
        """UTC epoch nanoseconds (market time for MarketClock)."""
        ...

    def monotonic_ns(self) -> int:
        """Monotonic nanoseconds, only meaningful as differences."""
        ...


class SystemClock:
    """Wall clock advanced with ``perf_counter_ns`` and re-anchored periodically.

    ``perf_counter`` gives sub-microsecond resolution but drifts from the
    NTP-disciplined wall clock (tens of ppm) and may not follow sleep/resume, so
    the anchor is refreshed every ``reanchor_s`` (0 disables). Returned values
    never go backwards across a re-anchor.
    """

    def __init__(self, reanchor_s: float = 60.0) -> None:
        self._reanchor_ns = int(reanchor_s * NS_PER_S) if reanchor_s > 0 else 0
        self._lock = threading.Lock()
        self._anchor_wall = time.time_ns()
        self._anchor_perf = time.perf_counter_ns()
        self._last = 0

    def now_ns(self) -> int:
        perf = time.perf_counter_ns()
        if self._reanchor_ns and perf - self._anchor_perf >= self._reanchor_ns:
            with self._lock:
                if perf - self._anchor_perf >= self._reanchor_ns:
                    self._anchor_wall = time.time_ns()
                    self._anchor_perf = time.perf_counter_ns()
        value = self._anchor_wall + (time.perf_counter_ns() - self._anchor_perf)
        if value < self._last:
            value = self._last
        self._last = value
        return value

    def monotonic_ns(self) -> int:
        return time.perf_counter_ns()


class SimClock:
    """Settable clock for tests and backtests (jumps straight to event times)."""

    def __init__(self, start_ns: int = 0) -> None:
        self._now = start_ns

    def now_ns(self) -> int:
        return self._now

    def monotonic_ns(self) -> int:
        return self._now

    def set(self, ns: int) -> None:
        if ns < self._now:
            raise ValueError("SimClock cannot move backwards")
        self._now = ns

    def advance(self, delta_ns: int) -> None:
        self.set(self._now + delta_ns)


class MarketClock:
    """Market time = wall time + offset; the offset comes from ``sys.clock``.

    ``mode`` is ``REAL`` (offset must be 0) or ``DEMO``. Updates are applied
    atomically; readers on other threads always see a consistent pair.
    """

    def __init__(self, wall: Clock | None = None, mode: str = "REAL", offset_ns: int = 0, version: int = 0) -> None:
        self._wall = wall or SystemClock()
        self._lock = threading.Lock()
        self._mode = mode
        self._offset = offset_ns
        self._version = version
        if mode == "REAL" and offset_ns != 0:
            raise ValueError("REAL clock mode requires offset 0")

    @property
    def wall(self) -> Clock:
        return self._wall

    @property
    def mode(self) -> str:
        return self._mode

    @property
    def offset_ns(self) -> int:
        return self._offset

    @property
    def version(self) -> int:
        return self._version

    def now_ns(self) -> int:
        return self._wall.now_ns() + self._offset

    def monotonic_ns(self) -> int:
        return self._wall.monotonic_ns()

    def apply(self, mode: str, offset_ns: int, version: int) -> bool:
        """Apply a clock state; ignores stale versions. Returns True if changed."""
        with self._lock:
            if version < self._version:
                return False
            if mode == "REAL" and offset_ns != 0:
                raise ValueError("REAL clock mode requires offset 0")
            changed = (mode, offset_ns, version) != (self._mode, self._offset, self._version)
            self._mode, self._offset, self._version = mode, offset_ns, version
            return changed


def ns_to_bj(ns: int) -> datetime:
    """UTC ns -> aware Beijing datetime."""
    return datetime.fromtimestamp(ns / NS_PER_S, tz=BJ)


def bj_to_ns(dt: datetime) -> int:
    """Aware or naive (assumed Beijing) datetime -> UTC ns."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=BJ)
    return int(dt.timestamp()) * NS_PER_S + dt.microsecond * 1000


def td_str(d: date) -> str:
    """date -> 'YYYYMMDD'."""
    return d.strftime("%Y%m%d")


def parse_td(s: str) -> date:
    """'YYYYMMDD' -> date."""
    return date(int(s[0:4]), int(s[4:6]), int(s[6:8]))
