"""Wall-clock stopwatch for performance statistics only (never for business time: that is the SimClock)."""

from __future__ import annotations

from futrisk.common.clock import SystemClock

_CLOCK = SystemClock(reanchor_s=0)


def wall_s() -> float:
    """Monotonic seconds (``perf_counter`` resolution) for measuring durations."""
    return _CLOCK.monotonic_ns() / 1e9


__all__ = ["wall_s"]
