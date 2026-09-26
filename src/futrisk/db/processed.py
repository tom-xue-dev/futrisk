"""Command idempotency records (03 §4.0 ``processed_commands``, 02 §4.7).

Server-side dedupe for state-changing commands keyed by ``command_id`` /
``movement_id`` / REST ``Idempotency-Key``::

    def tx(conn):
        h = request_hash(payload)
        chk = processed.check(conn, payload.command_id, h)
        if chk.reply is not None:          # DUPLICATE (duplicate=True) or CONFLICT
            return chk.reply
        reply = ...business writes...
        processed.record(conn, command_id=payload.command_id, command=ctx.command,
                         requester=ctx.requester, request_hash=h, reply=reply,
                         trading_day=td, received_ns=ctx.received_ns)
        return reply

The check, the business writes and the record belong to the same writer
transaction; :meth:`ProcessedCommands.run_once` does exactly that (and the
runtimes' ``idempotent`` helpers add the in-flight map for concurrent
duplicates). ``request_hash`` covers the business content: the request's own
time fields ``sent_ns``, ``requested_ns``, ``ts_ns`` and ``received_ns`` are
ignored **at the top level of the payload only**, so a retry with a fresh
``sent_ns`` is still a duplicate, while nested data keeps its times (a
``td.import_trades`` whose trades differ only in ``ts_ns`` is a ``CONFLICT``,
not a replay). Bytes are hashed as hex, whether the payload is a Struct or its
wire bytes.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum

import msgspec
from sqlalchemy import Connection, delete, insert, select

from futrisk.db.common_tables import CommonTables
from futrisk.db.database import Database
from futrisk.messages import codes
from futrisk.messages.base import TIME_FIELDS, decode, encode, request_hash  # request_hash: the one algorithm
from futrisk.messages.envelope import Reply, err_reply


@dataclass(frozen=True)
class ProcessedEntry:
    """A stored first result."""

    command_id: str
    command: str
    requester: str
    request_hash: str
    result_code: str
    reply_bytes: bytes
    trading_day: str
    received_ns: int

    def reply(self) -> Reply:
        return decode(self.reply_bytes, Reply)

    def duplicate_reply(self) -> Reply:
        """The first reply with ``duplicate=True`` (02 §4.7)."""
        r = self.reply()
        return msgspec.structs.replace(r, duplicate=True)


class DedupeOutcome(StrEnum):
    NEW = "NEW"
    DUPLICATE = "DUPLICATE"
    CONFLICT = "CONFLICT"


@dataclass(frozen=True)
class DedupeCheck:
    """Result of :meth:`ProcessedCommands.check`; ``reply`` is set unless ``NEW``."""

    outcome: DedupeOutcome
    entry: ProcessedEntry | None = None
    reply: Reply | None = None


class ProcessedCommands:
    """Access to ``processed_commands`` (all calls take the caller's connection)."""

    def __init__(self, source: Database | CommonTables) -> None:
        tables = source.tables if isinstance(source, Database) else source
        if tables.processed_commands is None:
            raise ValueError("schema has no processed_commands table")
        self._t = tables.processed_commands

    def lookup(self, conn: Connection, command_id: str) -> ProcessedEntry | None:
        t = self._t
        m = conn.execute(select(t).where(t.c.command_id == command_id)).mappings().first()
        if m is None:
            return None
        return ProcessedEntry(
            command_id=str(m["command_id"]),
            command=str(m["command"]),
            requester=str(m["requester"]),
            request_hash=str(m["request_hash"]),
            result_code=str(m["result_code"]),
            reply_bytes=bytes(m["reply"]),
            trading_day=str(m["trading_day"]),
            received_ns=int(m["received_ns"]),
        )

    def check(self, conn: Connection, command_id: str, req_hash: str) -> DedupeCheck:
        """NEW, DUPLICATE (first reply, ``duplicate=True``) or CONFLICT (same key, other content)."""
        entry = self.lookup(conn, command_id)
        if entry is None:
            return DedupeCheck(DedupeOutcome.NEW)
        if entry.request_hash != req_hash:
            return DedupeCheck(
                DedupeOutcome.CONFLICT,
                entry,
                err_reply(codes.CONFLICT, f"idempotency key {command_id} reused with different content"),
            )
        return DedupeCheck(DedupeOutcome.DUPLICATE, entry, entry.duplicate_reply())

    def record(
        self,
        conn: Connection,
        *,
        command_id: str,
        command: str,
        requester: str,
        request_hash: str,
        reply: Reply | bytes,
        trading_day: str,
        received_ns: int,
    ) -> None:
        """Store the first result (same transaction as the business writes)."""
        if isinstance(reply, Reply):
            reply_bytes = encode(reply)
            code = reply.code
        else:
            reply_bytes = bytes(reply)
            code = decode(reply_bytes, Reply).code
        conn.execute(
            insert(self._t).values(
                command_id=command_id,
                command=command,
                requester=requester,
                request_hash=request_hash,
                result_code=code,
                reply=reply_bytes,
                trading_day=trading_day,
                received_ns=received_ns,
            )
        )

    def run_once(
        self,
        conn: Connection,
        *,
        command_id: str,
        command: str,
        requester: str,
        payload: object,
        trading_day: str,
        received_ns: int,
        handler: Callable[[Connection], Reply],
    ) -> Reply:
        """Check, execute and record in the caller's writer transaction (02 §4.7).

        Returns the stored first reply (``duplicate=True``) for a repeat, a
        ``CONFLICT`` reply when the key was used with other content, otherwise
        ``handler(conn)``'s reply after recording it. ``handler`` does the
        business writes on ``conn``; if it raises, nothing is recorded.
        """
        h = request_hash(payload)
        chk = self.check(conn, command_id, h)
        if chk.reply is not None:
            return chk.reply
        reply = handler(conn)
        if not isinstance(reply, Reply):
            raise TypeError(f"idempotent handler must return a Reply, got {type(reply).__name__}")
        self.record(
            conn,
            command_id=command_id,
            command=command,
            requester=requester,
            request_hash=h,
            reply=reply,
            trading_day=trading_day,
            received_ns=received_ns,
        )
        return reply

    def purge_before(self, conn: Connection, trading_day: str) -> int:
        """Delete records of trading days before ``trading_day`` (03 §8: keep today and yesterday)."""
        t = self._t
        res = conn.execute(delete(t).where(t.c.trading_day < trading_day))
        return int(res.rowcount or 0)


__all__ = [
    "TIME_FIELDS",
    "DedupeCheck",
    "DedupeOutcome",
    "ProcessedCommands",
    "ProcessedEntry",
    "request_hash",
]
