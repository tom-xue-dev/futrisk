"""Publisher incarnations — process epochs and database streams — without clock ordering (02 §4.5, §4.6, §8.2).

``Envelope.epoch`` (one per process start) and ``Envelope.stream`` (one per
outbox database) are UUIDv7 strings. Their order follows the wall clock at
the moment they were generated, and Windows time synchronisation can step
that clock back: a publisher restarted right after such a step gets an epoch
(a recreated database a stream id) that sorts *before* its predecessor.
Subscribers therefore never compare these ids by value. :class:`Incarnations`
decides, per publisher and per id kind, which id is current:

* **The heartbeat is the authority.** ``sys.heartbeat.{instance}`` announces
  the publisher's current ``epoch`` and ``stream``. A heartbeat announcing an
  id that was never seen switches to it at once (the previous id is retired).
  Once a heartbeat of a publisher has been accepted, envelopes carrying any
  other id are stale — including envelopes of a new incarnation that arrive
  before its first heartbeat; subscribers recover those by re-snapshot (LV) or
  replay / reconciliation (FACT) when the heartbeat switches them.
* **Revert debounce.** A heartbeat announcing a *retired* id is normally one of
  the previous process still queued on the SUB socket (the old and the new
  process are different connections, so their heartbeats can interleave). It
  is ignored unless :data:`REVERT_AFTER` consecutive heartbeats of the
  publisher announce that id, which heals a wrong switch.
* **Arrival order before the first heartbeat.** An id never seen before is a
  new incarnation: it replaces the current id, which is retired. A retired id
  is stale. Consumers that never receive heartbeats keep this rule.

Two processes cannot run as the same instance at the same time (they would
bind the same PUB endpoint) and epochs / stream ids are never reused, so a
replaced incarnation never becomes current again except through the revert
debounce.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum

REVERT_AFTER = 2
"""Consecutive heartbeats needed to switch back to a retired id."""

MAX_RETIRED = 32
"""Retired ids remembered per publisher (oldest forgotten first)."""


class Verdict(StrEnum):
    """Classification of an id seen in an envelope or announced by a heartbeat."""

    FIRST = "FIRST"  # first id known for this publisher: adopted
    CURRENT = "CURRENT"  # the current id
    NEW = "NEW"  # a new incarnation: it replaced the current id (switch)
    STALE = "STALE"  # drop / ignore: a retired id, or any id but the announced one


@dataclass
class _Series:
    current: str | None = None
    announced: bool = False
    retired: dict[str, None] = field(default_factory=dict)
    revert: str | None = None
    revert_count: int = 0


class Incarnations:
    """Current incarnation id per publisher, for one kind of id (epochs or streams)."""

    def __init__(self, *, revert_after: int = REVERT_AFTER, max_retired: int = MAX_RETIRED) -> None:
        self._series: dict[str, _Series] = {}
        self._revert_after = max(1, revert_after)
        self._max_retired = max(1, max_retired)

    # ---------------------------------------------------------- queries
    def current(self, src: str) -> str | None:
        s = self._series.get(src)
        return s.current if s is not None else None

    def announced(self, src: str) -> bool:
        """True once a heartbeat of ``src`` was accepted (from then on envelopes cannot switch)."""
        s = self._series.get(src)
        return s is not None and s.announced

    def is_retired(self, src: str, value: str) -> bool:
        s = self._series.get(src)
        return s is not None and value in s.retired

    # ----------------------------------------------------------- inputs
    def observe(self, src: str, value: str) -> Verdict:
        """Classify the id carried by an envelope (before the first heartbeat it may switch)."""
        s = self._series.setdefault(src, _Series())
        if s.current is None:
            s.current = value
            s.retired.pop(value, None)
            return Verdict.FIRST
        if value == s.current:
            return Verdict.CURRENT
        if s.announced or value in s.retired:
            return Verdict.STALE
        self._switch(s, value)
        return Verdict.NEW

    def announce(self, src: str, value: str) -> Verdict:
        """Apply the id announced by a heartbeat of ``src``; ``STALE`` means: ignore that heartbeat."""
        s = self._series.setdefault(src, _Series())
        if s.current is None:
            s.current = value
            s.retired.pop(value, None)
            s.announced = True
            return Verdict.FIRST
        if value == s.current:
            s.announced = True
            s.revert, s.revert_count = None, 0
            return Verdict.CURRENT
        if value in s.retired:
            if s.revert == value:
                s.revert_count += 1
            else:
                s.revert, s.revert_count = value, 1
            if s.revert_count < self._revert_after:
                return Verdict.STALE
        self._switch(s, value)
        s.announced = True
        return Verdict.NEW

    def retire(self, src: str, value: str) -> None:
        """Mark ``value`` as replaced (e.g. the stream a persisted cursor was on); no-op for the current id."""
        s = self._series.setdefault(src, _Series())
        if value != s.current:
            self._add_retired(s, value)

    def forget(self, src: str) -> None:
        self._series.pop(src, None)

    # -------------------------------------------------------- internals
    def _switch(self, s: _Series, value: str) -> None:
        if s.current is not None:
            self._add_retired(s, s.current)
        s.retired.pop(value, None)
        s.current = value
        s.revert, s.revert_count = None, 0

    def _add_retired(self, s: _Series, value: str) -> None:
        s.retired.pop(value, None)
        s.retired[value] = None
        while len(s.retired) > self._max_retired:
            del s.retired[next(iter(s.retired))]


__all__ = ["MAX_RETIRED", "REVERT_AFTER", "Incarnations", "Verdict"]
