"""FACT subscription cursors (03 §4.0 ``fact_cursors``, 02 §4.5, §8.2).

A cursor records, per ``(src, topic)``, the publisher stream and the last
contiguous sequence number applied. It is written **in the same transaction**
as the business effects of the message (the ``apply`` callback of
:class:`futrisk.bus.fact.FactTracker` receives the new values), and only when
the first message is applied, so ``stream`` is never NULL.
"""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import Connection, select

from futrisk.db.common_tables import CommonTables
from futrisk.db.database import Database, PreparedSql
from futrisk.messages.enums import FactStartPolicy


@dataclass(frozen=True)
class FactCursor:
    src: str
    topic: str
    stream: str
    last_seq: int
    start_policy: FactStartPolicy
    updated_ns: int

    @property
    def key(self) -> tuple[str, str]:
        return (self.src, self.topic)

    @property
    def expected(self) -> int:
        """Next sequence number expected (``last_seq + 1``)."""
        return self.last_seq + 1


class FactCursors:
    """Access to ``fact_cursors``."""

    def __init__(self, source: Database | CommonTables) -> None:
        tables = source.tables if isinstance(source, Database) else source
        if tables.fact_cursors is None:
            raise ValueError("schema has no fact_cursors table")
        self._t = tables.fact_cursors
        self._db = source if isinstance(source, Database) else None
        self._upsert = PreparedSql(
            f"INSERT INTO {self._t.name} (src, topic, stream, last_seq, start_policy, updated_ns) "
            "VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT (src, topic) DO UPDATE SET stream = excluded.stream, "
            "last_seq = excluded.last_seq, start_policy = excluded.start_policy, updated_ns = excluded.updated_ns"
        )

    def load_all(self, conn: Connection | None = None) -> dict[tuple[str, str], FactCursor]:
        """All cursors keyed by ``(src, topic)`` (uses a read connection when ``conn`` is None)."""

        def q(c: Connection) -> dict[tuple[str, str], FactCursor]:
            out: dict[tuple[str, str], FactCursor] = {}
            for m in c.execute(select(self._t)).mappings():
                cur = FactCursor(
                    src=str(m["src"]),
                    topic=str(m["topic"]),
                    stream=str(m["stream"]),
                    last_seq=int(m["last_seq"]),
                    start_policy=FactStartPolicy(str(m["start_policy"])),
                    updated_ns=int(m["updated_ns"]),
                )
                out[cur.key] = cur
            return out

        if conn is not None:
            return q(conn)
        if self._db is None:
            raise ValueError("pass a connection (FactCursors built from tables)")
        return self._db.read(q)

    def get(self, conn: Connection, src: str, topic: str) -> FactCursor | None:
        t = self._t
        m = conn.execute(select(t).where(t.c.src == src, t.c.topic == topic)).mappings().first()
        if m is None:
            return None
        return FactCursor(
            src=str(m["src"]),
            topic=str(m["topic"]),
            stream=str(m["stream"]),
            last_seq=int(m["last_seq"]),
            start_policy=FactStartPolicy(str(m["start_policy"])),
            updated_ns=int(m["updated_ns"]),
        )

    def upsert(
        self,
        conn: Connection,
        src: str,
        topic: str,
        stream: str,
        last_seq: int,
        start_policy: FactStartPolicy | str,
        *,
        updated_ns: int,
    ) -> None:
        """Insert or update one cursor inside the caller's business transaction.

        Hot path (every applied FACT): one prepared ``INSERT ... ON CONFLICT DO
        UPDATE`` on the job's sqlite3 connection (~15-40 µs instead of ~150 µs).
        """
        policy = start_policy if isinstance(start_policy, FactStartPolicy) else FactStartPolicy(str(start_policy))
        self._upsert(conn, (src, topic, stream, int(last_seq), policy.value, int(updated_ns)))


__all__ = ["FactCursor", "FactCursors"]
