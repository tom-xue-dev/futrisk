"""Loop statistics exported in the risk-engine heartbeat (02 §7.9 ``Heartbeat.latency_us`` / ``rates`` / ``queues``).

* :class:`LatencyWindow` keeps the last ``size`` samples (µs) and summarises them as
  ``LatencySummary{p50, p99, max, n}``; ``n`` counts every sample ever recorded.
* :class:`RateMeter` counts events and reports per-second rates over the interval since the
  previous report.

Both are used from the business thread only; the heartbeat thread receives immutable copies
through ``rt.set_heartbeat_fields``.
"""

from __future__ import annotations

from collections import deque

from futrisk.messages.system import LatencySummary


class LatencyWindow:
    """Sliding window of latency samples in microseconds."""

    def __init__(self, size: int = 2048) -> None:
        self._samples: deque[float] = deque(maxlen=max(1, size))
        self.count = 0

    def add(self, us: float) -> None:
        self._samples.append(max(0.0, float(us)))
        self.count += 1

    def add_ns(self, ns: int) -> None:
        self.add(ns / 1000.0)

    def samples(self) -> list[float]:
        return list(self._samples)

    def summary(self) -> LatencySummary | None:
        if not self._samples:
            return None
        data = sorted(self._samples)
        n = len(data)

        def pct(q: float) -> float:
            idx = min(n - 1, max(0, int(round(q * (n - 1)))))
            return round(data[idx], 1)

        return LatencySummary(p50=pct(0.50), p99=pct(0.99), max=round(data[-1], 1), n=self.count)


class RateMeter:
    """Event counters turned into per-second rates at each report."""

    def __init__(self) -> None:
        self._counts: dict[str, int] = {}
        self.totals: dict[str, int] = {}
        self._last_ns: int | None = None

    def add(self, name: str, n: int = 1) -> None:
        if n:
            self._counts[name] = self._counts.get(name, 0) + n
            self.totals[name] = self.totals.get(name, 0) + n

    def rates(self, now_mono_ns: int, names: tuple[str, ...] = ()) -> dict[str, float]:
        """Rates since the previous call (every name in ``names`` is reported, zero included)."""
        last = self._last_ns
        self._last_ns = now_mono_ns
        counts, self._counts = self._counts, {}
        if last is None or now_mono_ns <= last:
            return {k: 0.0 for k in (*names, *counts)}
        secs = (now_mono_ns - last) / 1e9
        out = {k: 0.0 for k in names}
        for k, v in counts.items():
            out[k] = round(v / secs, 2)
        return out


__all__ = ["LatencyWindow", "RateMeter"]
