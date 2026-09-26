"""Heartbeats and peer liveness (02 §4.8, §7.9).

* Every internal service publishes ``sys.heartbeat.{instance}`` (LV,
  ``Heartbeat``) every ``heartbeat_ms``, **outside** the business loop: sync
  services from :class:`HeartbeatThread`, asyncio services from
  :func:`heartbeat_loop` (a timer task in the event loop, so a blocked loop
  stops heartbeats and the supervisor notices). ``loop_lag_ms`` reports how
  long ago the business loop last completed an iteration.
* :class:`HeartbeatBuilder` assembles the payload from runtime callbacks:
  state, epoch, pid, versions, config version/hash, clock version, loop lag,
  outbox ``stream`` and ``fact_heads``, plus service-provided queues, rates,
  latencies, deps and detail.
* :class:`PeerLiveness` marks a peer ``DOWN`` after ``peer_timeout_missed``
  heartbeat periods without a heartbeat (and ``UP`` on the next one).
"""

from __future__ import annotations

import asyncio
import os
import threading
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from loguru import logger

from futrisk.common.clock import Clock, SystemClock
from futrisk.messages.base import registered_types, type_version
from futrisk.messages.enums import ServiceState, TradingMode
from futrisk.messages.envelope import PROTO
from futrisk.messages.system import Heartbeat, LatencySummary

UP = "UP"
DOWN = "DOWN"
UNKNOWN = "UNKNOWN"


def payload_versions() -> dict[str, list[int]]:
    """``Heartbeat.versions``: supported ``[min, max]`` version per payload type (02 §4.10)."""
    return {name: [type_version(name), type_version(name)] for name in sorted(registered_types())}


@dataclass
class HeartbeatExtras:
    """Service-provided part of the heartbeat."""

    trading_day: str | None = None
    trading_mode: TradingMode | None = None
    queues: dict[str, int] | None = None
    rates: dict[str, float] | None = None
    latency_us: dict[str, LatencySummary] | None = None
    deps: dict[str, str] | None = None
    detail: dict[str, Any] | None = None


class HeartbeatBuilder:
    """Builds :class:`Heartbeat` payloads from callbacks (thread-agnostic)."""

    def __init__(
        self,
        *,
        instance: str,
        service: str,
        epoch: str,
        started_ns: int,
        app_version: str,
        clock: Clock,
        state: Callable[[], ServiceState],
        config: Callable[[], tuple[int, str]],
        clock_version: Callable[[], int],
        loop_lag_ms: Callable[[], int],
        stream: Callable[[], str | None],
        fact_heads: Callable[[], dict[str, int]],
        extras: Callable[[], HeartbeatExtras] | None = None,
    ) -> None:
        self.instance = instance
        self.service = service
        self.epoch = epoch
        self.started_ns = started_ns
        self.app_version = app_version
        self._clock = clock
        self._state = state
        self._config = config
        self._clock_version = clock_version
        self._loop_lag_ms = loop_lag_ms
        self._stream = stream
        self._fact_heads = fact_heads
        self._extras = extras
        self._versions = payload_versions()
        self._pid = os.getpid()

    def build(self) -> Heartbeat:
        version, config_hash = self._config()
        ex = self._extras() if self._extras is not None else HeartbeatExtras()
        return Heartbeat(
            instance=self.instance,
            service=self.service,
            state=self._state(),
            pid=self._pid,
            epoch=self.epoch,
            app_version=self.app_version,
            proto=PROTO,
            versions=self._versions,
            config_version=version,
            config_hash=config_hash,
            clock_version=self._clock_version(),
            loop_lag_ms=max(0, int(self._loop_lag_ms())),
            stream=self._stream(),
            fact_heads=self._fact_heads(),
            started_ns=self.started_ns,
            trading_day=ex.trading_day,
            trading_mode=ex.trading_mode,
            queues=dict(ex.queues or {}),
            rates=dict(ex.rates or {}),
            latency_us=dict(ex.latency_us or {}),
            deps=dict(ex.deps or {}),
            detail=dict(ex.detail or {}),
        )


class HeartbeatThread:
    """Calls ``emit()`` immediately and then every ``interval_ms`` from its own thread."""

    def __init__(self, emit: Callable[[], None], interval_ms: int, *, name: str = "heartbeat") -> None:
        self._emit = emit
        self._interval = max(10, interval_ms) / 1000
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name=name, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self, timeout: float = 2.0) -> None:
        self._stop.set()
        if self._thread.is_alive():
            self._thread.join(timeout)

    def trigger(self) -> None:
        """Emit one heartbeat now (e.g. right after a state change)."""
        self._safe_emit()

    def _safe_emit(self) -> None:
        try:
            self._emit()
        except Exception:
            logger.exception("heartbeat.emit_failed")

    def _run(self) -> None:
        next_at = time.monotonic()
        while not self._stop.is_set():
            self._safe_emit()
            next_at += self._interval
            delay = next_at - time.monotonic()
            if delay < 0:
                next_at = time.monotonic()
                delay = 0
            if self._stop.wait(delay):
                break


async def heartbeat_loop(emit: Callable[[], None], interval_ms: int) -> None:
    """Asyncio heartbeat task: ``emit()`` now and every ``interval_ms`` (cancel to stop)."""
    loop = asyncio.get_running_loop()
    interval = max(10, interval_ms) / 1000
    next_at = loop.time()
    while True:
        try:
            emit()
        except Exception:
            logger.exception("heartbeat.emit_failed")
        next_at += interval
        delay = next_at - loop.time()
        if delay < 0:
            next_at = loop.time()
            delay = 0
        await asyncio.sleep(delay)


class PeerLiveness:
    """Heartbeat-based UP/DOWN tracking of peers (02 §4.8). Thread-safe; callbacks run outside the lock."""

    def __init__(
        self,
        *,
        heartbeat_ms: int = 1_000,
        missed: int = 3,
        clock: Clock | None = None,
        on_change: Callable[[str, str], None] | None = None,
    ) -> None:
        self._timeout_ns = heartbeat_ms * missed * 1_000_000
        self._clock = clock or SystemClock()
        self._on_change = on_change
        self._lock = threading.Lock()
        self._state: dict[str, str] = {}
        self._last_ns: dict[str, int] = {}
        self._since_ns: dict[str, int] = {}
        self._last_hb: dict[str, Heartbeat] = {}

    @property
    def timeout_ns(self) -> int:
        return self._timeout_ns

    def set_on_change(self, cb: Callable[[str, str], None] | None) -> None:
        self._on_change = cb

    def watch(self, peers: Iterable[str]) -> None:
        now = self._clock.monotonic_ns()
        with self._lock:
            for peer in peers:
                if peer not in self._state:
                    self._state[peer] = UNKNOWN
                    self._since_ns[peer] = now

    def watched(self) -> list[str]:
        with self._lock:
            return sorted(self._state)

    def on_heartbeat(self, hb: Heartbeat, now_ns: int | None = None) -> None:
        now = self._clock.monotonic_ns() if now_ns is None else now_ns
        peer = hb.instance
        with self._lock:
            self._last_ns[peer] = now
            self._last_hb[peer] = hb
            changed = self._set(peer, UP, now)
        if changed is not None:
            self._notify(peer, UP, changed)

    def _silent(self, peer: str, st: str, now: int) -> bool:
        silent_up = st == UP and now - self._last_ns.get(peer, 0) > self._timeout_ns
        never_seen = st == UNKNOWN and now - self._since_ns.get(peer, now) > self._timeout_ns
        return silent_up or never_seen

    def overdue(self, now_ns: int | None = None) -> list[str]:
        """Peers that :meth:`check` would mark DOWN now (no state change)."""
        now = self._clock.monotonic_ns() if now_ns is None else now_ns
        with self._lock:
            return [peer for peer, st in self._state.items() if self._silent(peer, st, now)]

    def check(self, now_ns: int | None = None) -> list[tuple[str, str]]:
        """Mark silent peers DOWN; returns the transitions made."""
        now = self._clock.monotonic_ns() if now_ns is None else now_ns
        changes: list[tuple[str, str, str]] = []
        with self._lock:
            for peer, st in list(self._state.items()):
                if self._silent(peer, st, now):
                    old = self._set(peer, DOWN, now)
                    if old is not None:
                        changes.append((peer, DOWN, old))
        for peer, state, old in changes:
            self._notify(peer, state, old)
        return [(peer, state) for peer, state, _ in changes]

    def _set(self, peer: str, state: str, now: int) -> str | None:
        """Set the state (lock held); returns the previous state when it changed, else None."""
        old = self._state.get(peer)
        if old == state:
            return None
        self._state[peer] = state
        self._since_ns[peer] = now
        return old if old is not None else "NEW"

    def _notify(self, peer: str, state: str, old: str) -> None:
        logger.info("peer.state", peer=peer, state=state, previous=old)
        cb = self._on_change
        if cb is not None:
            try:
                cb(peer, state)
            except Exception:
                logger.exception("peer.on_change_failed", peer=peer)

    def state(self, peer: str) -> str:
        with self._lock:
            return self._state.get(peer, UNKNOWN)

    def is_up(self, peer: str) -> bool:
        with self._lock:
            return self._state.get(peer) == UP

    def last_heartbeat(self, peer: str) -> Heartbeat | None:
        with self._lock:
            return self._last_hb.get(peer)

    def deps(self) -> dict[str, str]:
        """``Heartbeat.deps``: ``UP``/``DOWN`` per watched peer (never seen counts as DOWN)."""
        with self._lock:
            return {p: (UP if s == UP else DOWN) for p, s in sorted(self._state.items())}

    def snapshot(self) -> Mapping[str, str]:
        with self._lock:
            return dict(self._state)


__all__ = [
    "DOWN",
    "UNKNOWN",
    "UP",
    "HeartbeatBuilder",
    "HeartbeatExtras",
    "HeartbeatThread",
    "PeerLiveness",
    "heartbeat_loop",
    "payload_versions",
]
