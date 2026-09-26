"""Heartbeat statistics of md-gateway: latency summaries and message rates (02 §4.8, §7.9).

The hot path only increments plain integer counters (:class:`RateCounters`); rates are
derived from the counter deltas when the heartbeat / status asks for them.
"""

from __future__ import annotations

from collections import deque

from futrisk.messages.system import LatencySummary


class LatencyWindow:
    """The last ``size`` latency samples (µs) summarized as ``{p50, p99, max, n}``."""

    def __init__(self, size: int = 4096) -> None:
        self._samples: deque[float] = deque(maxlen=size)
        self.total = 0

    def add(self, us: float) -> None:
        self._samples.append(us)
        self.total += 1

    def summary(self) -> LatencySummary | None:
        if not self._samples:
            return None
        data = sorted(self._samples)
        n = len(data)

        def pct(p: float) -> float:
            return round(data[min(n - 1, int(p * (n - 1) + 0.5))], 1)

        return LatencySummary(p50=pct(0.50), p99=pct(0.99), max=round(data[-1], 1), n=self.total)


class RateMeter:
    """Events per second over a sliding window of ``window_s`` seconds."""

    def __init__(self, window_s: float = 5.0) -> None:
        self._window_ns = int(window_s * 1e9)
        self._events: deque[tuple[int, int]] = deque()
        self._count = 0

    def add(self, mono_ns: int, n: int = 1) -> None:
        self._events.append((mono_ns, n))
        self._count += n
        self._trim(mono_ns)

    def rate(self, mono_ns: int) -> float:
        self._trim(mono_ns)
        return round(self._count / (self._window_ns / 1e9), 2)

    def _trim(self, mono_ns: int) -> None:
        horizon = mono_ns - self._window_ns
        while self._events and self._events[0][0] < horizon:
            _, n = self._events.popleft()
            self._count -= n


class RateCounters:
    """Named monotonic counters; :meth:`rates` turns their growth since the previous call into per-second rates."""

    def __init__(self) -> None:
        self.counts: dict[str, int] = {}
        self._mark: tuple[int, dict[str, int]] | None = None
        self._rates: dict[str, float] = {}

    def inc(self, name: str, n: int = 1) -> None:
        c = self.counts
        c[name] = c.get(name, 0) + n

    def rates(self, now_mono: int, min_interval_ns: int = 500_000_000) -> dict[str, float]:
        """Per-second rates since the previous call (the previous result when called again too soon)."""
        mark = self._mark
        if mark is None:
            self._mark = (now_mono, dict(self.counts))
            return {name: 0.0 for name in self.counts}
        t0, c0 = mark
        dt = now_mono - t0
        if dt < min_interval_ns:
            return dict(self._rates)
        secs = dt / 1e9
        self._rates = {name: round((n - c0.get(name, 0)) / secs, 2) for name, n in self.counts.items()}
        self._mark = (now_mono, dict(self.counts))
        return dict(self._rates)


__all__ = ["LatencyWindow", "RateCounters", "RateMeter"]
