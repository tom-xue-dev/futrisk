"""Generic control payloads (02 §6.1, §7.2)."""

from __future__ import annotations

from futrisk.messages.base import Msg, payload


@payload()
class Ping(Msg, kw_only=True):
    pass


@payload()
class Pong(Msg, kw_only=True):
    instance: str
    state: str
    ts_ns: int
    home_id: str | None = None  # HomeLayout.home_id() of the answering process (cross-home guard)
    pid: int | None = None


@payload()
class StatusRequest(Msg, kw_only=True):
    pass


@payload()
class SnapshotRequest(Msg, kw_only=True):
    prefixes: list[str]
    include_fact: bool = True
    next_cursor: str | None = None
    limit: int = 500


@payload()
class SnapshotItem(Msg, kw_only=True):
    topic: str
    seq: int
    epoch: str
    envelope: bytes


@payload()
class SnapshotBatch(Msg, kw_only=True):
    items: list[SnapshotItem]
    as_of_ns: int
    next_cursor: str | None = None


@payload()
class ReplayRequest(Msg, kw_only=True):
    topic: str
    from_seq: int
    to_seq: int | None = None
    limit: int = 500


@payload()
class ReplayBatch(Msg, kw_only=True):
    topic: str
    envelopes: list[bytes]
    next_seq: int
    complete: bool


@payload()
class ShutdownRequest(Msg, kw_only=True):
    reason: str = ""
    deadline_ms: int = 10_000
