"""Last-value cache serving ``ctl.snapshot`` (02 §4.5, §4.6, §7.2).

The publisher stores the last envelope of every topic it sends (LV and FACT).
``ctl.snapshot`` returns, for every cached topic matching one of the request
prefixes, that envelope unchanged (with its ``seq`` and ``epoch``). Results are
paged: topics are returned in lexicographic order, at most ``limit`` items and
roughly 900 KB per page; ``next_cursor`` is the last topic of the page.

Prefix semantics follow :func:`futrisk.messages.topics.matches`: a prefix
ending with ``.`` matches every topic below it, anything else is an exact
topic.
"""

from __future__ import annotations

import threading
from collections.abc import Sequence
from dataclasses import dataclass

from futrisk.messages.common import SnapshotBatch, SnapshotItem
from futrisk.messages.topics import matches

MAX_SNAPSHOT_LIMIT = 1000
PAGE_MAX_BYTES = 900_000


@dataclass(slots=True)
class CachedEnvelope:
    """One cached envelope (read-only by convention: not frozen, a frozen dataclass costs ~3x to build and
    one is built for every published message)."""

    topic: str
    seq: int
    epoch: str
    envelope: bytes
    fact: bool


class LastValueCache:
    """Thread-safe ``topic -> last envelope`` map."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._items: dict[str, CachedEnvelope] = {}

    def put(self, topic: str, seq: int, epoch: str, envelope: bytes, *, fact: bool) -> None:
        item = CachedEnvelope(topic, seq, epoch, envelope, fact)
        with self._lock:
            self._items[topic] = item

    def get(self, topic: str) -> CachedEnvelope | None:
        with self._lock:
            return self._items.get(topic)

    def topics(self) -> list[str]:
        with self._lock:
            return sorted(self._items)

    def __len__(self) -> int:
        with self._lock:
            return len(self._items)

    def snapshot(
        self,
        prefixes: Sequence[str],
        *,
        as_of_ns: int,
        include_fact: bool = True,
        cursor: str | None = None,
        limit: int = 500,
    ) -> SnapshotBatch:
        """One page of the snapshot for ``prefixes`` after ``cursor``."""
        n = max(1, min(int(limit), MAX_SNAPSHOT_LIMIT))
        with self._lock:
            candidates = sorted(
                (item for t, item in self._items.items() if any(matches(t, p) for p in prefixes)),
                key=lambda i: i.topic,
            )
        items: list[SnapshotItem] = []
        size = 0
        more = False
        for item in candidates:
            if cursor is not None and item.topic <= cursor:
                continue
            if item.fact and not include_fact:
                continue
            if len(items) >= n or (items and size + len(item.envelope) > PAGE_MAX_BYTES):
                more = True
                break
            items.append(SnapshotItem(topic=item.topic, seq=item.seq, epoch=item.epoch, envelope=item.envelope))
            size += len(item.envelope)
        next_cursor = items[-1].topic if more and items else None
        return SnapshotBatch(items=items, as_of_ns=as_of_ns, next_cursor=next_cursor)


__all__ = ["CachedEnvelope", "LastValueCache", "MAX_SNAPSHOT_LIMIT"]
