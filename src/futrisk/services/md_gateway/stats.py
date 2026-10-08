"""Heartbeat statistics of md-gateway: latency summaries and message rates (02 §4.8, §7.9)."""

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


class RateCounters:
    """Named monotonic counters; :meth:`rates` turns their growth since the previous call into per-second rates."""

    def __init__(self) -> None:
        self.counts: dict[str, int] = {}
        self._mark: tuple[int, dict[str, int]] | None = None
        self._rates: dict[str, float] = {}

    def inc(self, name: str, n: int = 1) -> None:
        self.counts[name] = self.counts.get(name, 0) + n

    def rates(self, now_mono: int, min_interval_ns: int = 500_000_000) -> dict[str, float]:
        """Per-second rates since the previous call (the previous result when called again too soon)."""
        mark = self._mark
        if mark is None:
            self._mark = (now_mono, dict(self.counts))
            return dict.fromkeys(self.counts, 0.0)
        t0, c0 = mark
        dt = now_mono - t0
        if dt < min_interval_ns:
            return dict(self._rates)
        self._rates = {name: round((n - c0.get(name, 0)) / (dt / 1e9), 2) for name, n in self.counts.items()}
        self._mark = (now_mono, dict(self.counts))
        return dict(self._rates)


__all__ = ["LatencyWindow", "RateCounters"]
