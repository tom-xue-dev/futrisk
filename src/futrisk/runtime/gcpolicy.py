"""Garbage-collector policy of a service process (01 §11 RK-01).

CPython's cyclic GC stops the whole process while it runs. A full (gen2)
collection traverses every tracked object - the restored book, caches,
config, module objects - and took 56-250 ms in the risk-engine,
md-gateway-ingest and backtest benches. Both runtimes apply this policy:

* **at start** (``RuntimeBase`` right after loading the config):
  ``gc.set_threshold(*threshold)`` - by default ``(10_000, 20, 100)``: 10,000
  net container allocations per young collection instead of 700. Measured in
  ``bench order_rtt`` (30 s windows): 700 -> ~160-230 young, 15-21 middle and
  1-2 full collections per window even with the freeze; 10,000 -> ~7 young and
  none else; 50,000 -> ~1, but a young collection then walks up to 50,000
  objects (~1 ms synthetic, 17-35 ms observed on the loaded machine) and a
  middle one up to 20 x 50,000. The thresholds are process-wide and set by
  every runtime start;
* **when the service first reports RUNNING** (``set_state("RUNNING")``, before
  that heartbeat goes out): ``gc.collect()`` then ``gc.freeze()`` - everything
  allocated during start-up moves to the permanent generation and is never
  traversed again, so later full collections only walk what was created
  since. Once per process;
* **statistics** from ``gc.callbacks``: collections per generation, last / max /
  total pause, published in the heartbeat ``detail["gc"]``.

No idle-time manual collection: a collection that starts in an idle moment
still blocks the request that arrives during it, and with the start-up state
frozen the remaining full collections are short.

Configuration: ``services.yaml`` ``bus.gc`` (``enabled``, ``threshold``,
``freeze_on_running``; defaults on). The environment variable ``FUTRISK_GC``
overrides it for measurements: ``off``; ``nofreeze``; ``t0,t1,t2`` (thresholds);
``t0,t1,t2,nofreeze``.
"""

from __future__ import annotations

import gc
import os
import threading
from dataclasses import dataclass
from typing import Any

from loguru import logger

from futrisk.common.clock import SystemClock

DEFAULT_THRESHOLD = (10_000, 20, 100)


@dataclass(frozen=True)
class GcSettings:
    enabled: bool = True
    threshold: tuple[int, int, int] = DEFAULT_THRESHOLD
    freeze_on_running: bool = True

    @classmethod
    def from_config(cls, cfg: Any | None) -> GcSettings:
        """From ``services.bus.gc`` (any object with the three attributes) and ``FUTRISK_GC``."""
        s = cls() if cfg is None else cls(
            enabled=bool(getattr(cfg, "enabled", True)),
            threshold=tuple(int(x) for x in getattr(cfg, "threshold", DEFAULT_THRESHOLD))[:3],  # type: ignore[arg-type]
            freeze_on_running=bool(getattr(cfg, "freeze_on_running", True)),
        )
        env = os.environ.get("FUTRISK_GC", "").strip().lower()
        if not env:
            return s
        if env in ("off", "0", "false", "no"):
            return cls(enabled=False, threshold=s.threshold, freeze_on_running=False)
        parts = [p.strip() for p in env.split(",") if p.strip()]
        freeze = s.freeze_on_running and "nofreeze" not in parts
        nums = [p for p in parts if p != "nofreeze"]
        threshold = s.threshold
        if nums:
            try:
                t = tuple(int(p) for p in nums)
            except ValueError:
                logger.warning("gc.bad_env", value=env)
                return s
            if len(t) == 3 and t[0] > 0:
                threshold = (t[0], t[1], t[2])
        return cls(enabled=True, threshold=threshold, freeze_on_running=freeze)


class GcStats:
    """Pause statistics of the process' collections (``gc.callbacks``; installed once per process)."""

    def __init__(self) -> None:
        self._clock = SystemClock()
        self._t0 = 0
        self.collections = [0, 0, 0]
        self.last_pause_ms = 0.0
        self.max_pause_ms = 0.0
        self.total_pause_ms = 0.0
        self.last_generation = -1

    def callback(self, phase: str, info: dict[str, Any]) -> None:
        if phase == "start":
            self._t0 = self._clock.monotonic_ns()
            return
        if not self._t0:
            return
        ms = (self._clock.monotonic_ns() - self._t0) / 1e6
        self._t0 = 0
        gen = int(info.get("generation", 0))
        if 0 <= gen <= 2:
            self.collections[gen] += 1
        self.last_generation = gen
        self.last_pause_ms = ms
        self.total_pause_ms += ms
        if ms > self.max_pause_ms:
            self.max_pause_ms = ms

    def snapshot(self) -> dict[str, Any]:
        return {
            "collections": list(self.collections),
            "last_pause_ms": round(self.last_pause_ms, 3),
            "last_generation": self.last_generation,
            "max_pause_ms": round(self.max_pause_ms, 3),
            "total_pause_ms": round(self.total_pause_ms, 1),
            "frozen": gc.get_freeze_count(),
            "threshold": list(gc.get_threshold()),
            "enabled": gc.isenabled(),
        }


_lock = threading.Lock()
_stats: GcStats | None = None
_frozen = False


def install_stats() -> GcStats:
    """The process' :class:`GcStats` (its callback is registered on the first call)."""
    global _stats
    with _lock:
        if _stats is None:
            _stats = GcStats()
            gc.callbacks.append(_stats.callback)
        return _stats


def apply_at_start(settings: GcSettings) -> None:
    """Thresholds (process-wide) and pause statistics."""
    install_stats()
    if not settings.enabled:
        return
    t0, t1, t2 = settings.threshold
    if gc.get_threshold() != (t0, t1, t2):
        gc.set_threshold(t0, t1, t2)
        logger.info("gc.threshold", threshold=[t0, t1, t2])


def freeze_once(settings: GcSettings) -> bool:
    """``gc.collect()`` + ``gc.freeze()`` the first time a service of this process reports RUNNING."""
    global _frozen
    if not (settings.enabled and settings.freeze_on_running):
        return False
    with _lock:
        if _frozen:
            return False
        _frozen = True
    clock = SystemClock()
    t = clock.monotonic_ns()
    collected = gc.collect()
    gc.freeze()
    logger.info("gc.frozen", objects=gc.get_freeze_count(), collected=collected,
                ms=round((clock.monotonic_ns() - t) / 1e6, 1))
    return True


def stats_snapshot() -> dict[str, Any] | None:
    st = _stats
    return st.snapshot() if st is not None else None


__all__ = ["DEFAULT_THRESHOLD", "GcSettings", "GcStats", "apply_at_start", "freeze_once", "install_stats", "stats_snapshot"]
