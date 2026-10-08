"""``md.event`` facts and the gap ledger (02 §5, §7.1 MdEvent/GapInfo, 03 §4.1 md_events/gaps, §7).

:class:`EventLog` builds :class:`MdEvent` payloads (UUIDv7 ``event_id``, market-time ``ts_ns``)
and hands them to a persister that writes ``md_events`` (+ ``gaps``) and the outbox row of
``md.event`` in one transaction. :class:`GapBook` keeps the open / closed gaps (``GAP_OPEN`` ->
``GAP_FILLED`` / ``GAP_UNRECOVERABLE``); :class:`RateLimiter` throttles informational events
per key. Gap ids: Feed A sequence gaps ``feed_a:{epoch}:{from_seq}-{to_seq}``; time-window gaps
``{source lower | all}:{from_ns}`` (``all:`` while no TRUSTED source is left in market hours,
``feed_b:`` / ``sina:`` while a polled source cannot be polled), whose ``from_ns`` / ``to_ns``
are receive times in market time.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import msgspec

from futrisk.common.ids import uuid7
from futrisk.messages.enums import GapStatus, MdEventKind, SourceId
from futrisk.messages.md import GapInfo, MdEvent


@dataclass
class GapRecord:
    """A row of ``gaps`` (03 §4.1)."""

    info: GapInfo
    trading_day: str
    opened_ns: int
    closed_ns: int | None = None

    @property
    def gap_id(self) -> str:
        return self.info.gap_id


Persister = Callable[[MdEvent, GapRecord | None, str | None], None]


class EventLog:
    """Creates ``md.event`` facts and forwards them to the persister (DB + outbox)."""

    def __init__(self, persist: Persister, *, now_ns: Callable[[], int], trading_day: Callable[[], str]) -> None:
        self._persist = persist
        self._now = now_ns
        self._td = trading_day
        self.counts: dict[str, int] = {}

    def build(
        self,
        kind: MdEventKind,
        *,
        trading_day: str | None = None,
        source: SourceId | None = None,
        instrument_id: str | None = None,
        from_state: str | None = None,
        to_state: str | None = None,
        gap: GapInfo | None = None,
        detail: dict[str, Any] | None = None,
        ts_ns: int | None = None,
    ) -> MdEvent:
        return MdEvent(
            event_id=uuid7(),
            kind=kind,
            ts_ns=ts_ns if ts_ns is not None else self._now(),
            trading_day=trading_day or self._td(),
            source=source,
            instrument_id=instrument_id,
            from_state=from_state,
            to_state=to_state,
            gap=gap,
            detail=_jsonable(detail or {}),
        )

    def emit(
        self,
        kind: MdEventKind,
        *,
        trading_day: str | None = None,
        source: SourceId | None = None,
        instrument_id: str | None = None,
        from_state: str | None = None,
        to_state: str | None = None,
        gap: GapRecord | None = None,
        detail: dict[str, Any] | None = None,
        actor: str | None = None,
        ts_ns: int | None = None,
    ) -> MdEvent:
        ev = self.build(
            kind,
            trading_day=trading_day or (gap.trading_day if gap is not None else None),
            source=source,
            instrument_id=instrument_id,
            from_state=from_state,
            to_state=to_state,
            gap=gap.info if gap is not None else None,
            detail=detail,
            ts_ns=ts_ns,
        )
        self.counts[str(kind)] = self.counts.get(str(kind), 0) + 1
        self._persist(ev, gap, actor)
        return ev


class GapBook:
    """Open and recently closed gaps; emits the gap events through :class:`EventLog`."""

    def __init__(self, events: EventLog, *, now_ns: Callable[[], int], keep_closed: int = 512) -> None:
        self._events = events
        self._now = now_ns
        self._keep = keep_closed
        self.gaps: dict[str, GapRecord] = {}

    def open(
        self,
        gap_id: str,
        *,
        trading_day: str,
        source: SourceId | None,
        from_ns: int,
        to_ns: int | None = None,
        epoch: str | None = None,
        from_seq: int | None = None,
        to_seq: int | None = None,
        detail: dict[str, Any] | None = None,
    ) -> GapRecord:
        rec = self.gaps.get(gap_id)
        if rec is not None and rec.info.status == GapStatus.OPEN:
            return rec
        info = GapInfo(
            gap_id=gap_id, source=source, epoch=epoch, from_seq=from_seq, to_seq=to_seq, from_ns=from_ns, to_ns=to_ns,
            status=GapStatus.OPEN,
        )
        rec = GapRecord(info, trading_day, self._now())
        self.gaps[gap_id] = rec
        self._events.emit(MdEventKind.GAP_OPEN, source=source, gap=rec, detail=detail)
        self._trim()
        return rec

    def close(
        self,
        gap_id: str,
        *,
        status: GapStatus,
        filled: int = 0,
        duplicates: int = 0,
        unrecoverable: int = 0,
        to_ns: int | None = None,
        detail: dict[str, Any] | None = None,
        trading_day: str | None = None,
        source: SourceId | None = None,
        from_ns: int | None = None,
        epoch: str | None = None,
        from_seq: int | None = None,
        to_seq: int | None = None,
    ) -> GapRecord:
        """Close a gap (``GAP_FILLED`` / ``GAP_UNRECOVERABLE``); an unknown id is created closed."""
        rec = self.gaps.get(gap_id)
        now = self._now()
        if rec is None:
            info = GapInfo(
                gap_id=gap_id, source=source, epoch=epoch, from_seq=from_seq, to_seq=to_seq,
                from_ns=from_ns if from_ns is not None else now, to_ns=to_ns,
            )
            rec = GapRecord(info, trading_day or "", now)
            self.gaps[gap_id] = rec
        elif rec.info.status != GapStatus.OPEN:
            return rec
        rec.info = msgspec.structs.replace(
            rec.info,
            status=status,
            filled=filled,
            duplicates=duplicates,
            unrecoverable=unrecoverable,
            to_ns=to_ns if to_ns is not None else (rec.info.to_ns or now),
        )
        rec.closed_ns = now
        kind = MdEventKind.GAP_FILLED if status == GapStatus.FILLED else MdEventKind.GAP_UNRECOVERABLE
        self._events.emit(kind, source=rec.info.source, gap=rec, detail=detail)
        self._trim()
        return rec

    def open_gaps(self) -> list[GapRecord]:
        return [g for g in self.gaps.values() if g.info.status == GapStatus.OPEN]

    def open_window(self, source: SourceId | None, *, trading_day: str, from_ns: int, detail: dict[str, Any] | None = None) -> GapRecord:
        """Open a time-window gap ``{source lower | all}:{from_ns}`` (02 §7.1)."""
        prefix = source.lower() if source is not None else "all"
        return self.open(f"{prefix}:{from_ns}", trading_day=trading_day, source=source, from_ns=from_ns, detail=detail)

    def open_window_of(self, source: SourceId | None) -> GapRecord | None:
        """The open time-window gap of ``source`` (``None`` = the ``all:`` gap), if any."""
        prefix = (source.lower() if source is not None else "all") + ":"
        for g in self.gaps.values():
            if g.info.status == GapStatus.OPEN and g.gap_id.startswith(prefix) and g.info.from_seq is None:
                return g
        return None

    def _trim(self) -> None:
        closed = [k for k, g in self.gaps.items() if g.info.status != GapStatus.OPEN]
        for k in closed[: max(0, len(closed) - self._keep)]:
            del self.gaps[k]


@dataclass
class RateLimiter:
    """At most one event per key and interval; counts what was suppressed.

    The key set is bounded (``max_keys``): keys come partly from vendor data (instrument ids
    of malformed records), so expired keys are purged when the bound is reached and, if
    every key is still live, the oldest ones are forgotten (``evicted`` counts them).
    """

    interval_ns: int
    max_keys: int = 4096
    last: dict[Any, int] = field(default_factory=dict)
    suppressed: dict[Any, int] = field(default_factory=dict)
    evicted: int = 0

    def allow(self, key: Any, now_mono: int) -> tuple[bool, int]:
        t = self.last.get(key)
        if t is not None and now_mono - t < self.interval_ns:
            self.suppressed[key] = self.suppressed.get(key, 0) + 1
            return False, 0
        if t is None and len(self.last) >= self.max_keys:
            self._purge(now_mono)
        self.last[key] = now_mono
        return True, self.suppressed.pop(key, 0)

    def _purge(self, now_mono: int) -> None:
        expired = [k for k, t in self.last.items() if now_mono - t >= self.interval_ns]
        for k in expired:
            del self.last[k]
            self.suppressed.pop(k, None)
        excess = len(self.last) - self.max_keys + 1
        if excess > 0:
            for k in list(self.last)[:excess]:  # insertion order = oldest first
                del self.last[k]
                self.suppressed.pop(k, None)
            self.evicted += excess


def _jsonable(obj: Any) -> Any:
    """Detail dicts are stored as JSON and sent as msgpack: keep them to plain builtins."""
    return msgspec.to_builtins(obj, str_keys=True)


__all__ = ["EventLog", "GapBook", "GapRecord", "Persister", "RateLimiter"]
