"""Market clock follower: applies ``sys.clock`` to a :class:`MarketClock` (02 §8.6, §8.8).

Every internal service except the supervisor (which owns the offset) follows
the market clock: at start-up it fetches the latest ``ClockState`` with
``ctl.snapshot{prefixes: ["sys.clock"]}`` from the supervisor (retrying while
unavailable and staying ``STARTING``), then applies every ``sys.clock``
message it receives. Stale versions are ignored by :meth:`MarketClock.apply`.
"""

from __future__ import annotations

import threading
from collections.abc import Callable

import msgspec
from loguru import logger

from futrisk.common.clock import MarketClock
from futrisk.messages.base import decode
from futrisk.messages.common import SnapshotBatch
from futrisk.messages.envelope import Envelope, Reply, decode_envelope
from futrisk.messages.system import ClockState
from futrisk.messages.topics import SYS_CLOCK

SUPERVISOR = "supervisor"


class ClockFollower:
    """Applies ``ClockState`` messages and tracks whether one was received."""

    def __init__(self, market_clock: MarketClock, *, ready: bool = False) -> None:
        self.market_clock = market_clock
        self._ready = threading.Event()
        if ready:
            self._ready.set()
        self.applied = 0
        self._on_ready: list[Callable[[], None]] = []

    @property
    def ready(self) -> bool:
        return self._ready.is_set()

    def add_ready_callback(self, cb: Callable[[], None]) -> None:
        if self.ready:
            cb()
        else:
            self._on_ready.append(cb)

    def mark_ready(self) -> None:
        was = self.ready
        self._ready.set()
        if not was:
            callbacks, self._on_ready = self._on_ready, []
            for cb in callbacks:
                cb()

    def wait(self, timeout: float | None = None) -> bool:
        return self._ready.wait(timeout)

    def apply(self, state: ClockState) -> bool:
        """Apply ``state``; returns True when the clock changed."""
        try:
            changed = self.market_clock.apply(str(state.mode), state.offset_ns, state.version)
        except ValueError:
            logger.error("clock.invalid_state", mode=str(state.mode), offset_ns=state.offset_ns)
            return False
        self.applied += 1
        if changed:
            logger.info("clock.applied", mode=str(state.mode), offset_ns=state.offset_ns, version=state.version)
        self.mark_ready()
        return changed

    def on_envelope(self, env: Envelope) -> bool:
        """Apply a ``sys.clock`` envelope (other topics are ignored)."""
        if env.topic != SYS_CLOCK or env.type != "ClockState":
            return False
        try:
            return self.apply(decode(env.payload, ClockState))
        except (msgspec.DecodeError, msgspec.ValidationError):
            logger.warning("clock.bad_payload")
            return False

    def on_snapshot_reply(self, reply: Reply) -> bool:
        """Apply the ``sys.clock`` item of a ``ctl.snapshot`` reply; True if one was applied."""
        if not reply.ok:
            return False
        try:
            batch = reply.open_as(SnapshotBatch)
        except (msgspec.DecodeError, msgspec.ValidationError):
            return False
        if batch is None:
            return False
        for item in batch.items:
            if item.topic == SYS_CLOCK:
                try:
                    env = decode_envelope(item.envelope)
                except (msgspec.DecodeError, msgspec.ValidationError):
                    continue
                self.on_envelope(env)
                return True
        return False


__all__ = ["SUPERVISOR", "ClockFollower"]
