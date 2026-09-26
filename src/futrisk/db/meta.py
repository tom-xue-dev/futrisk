"""``db_meta`` bookkeeping: stream id, clean-shutdown flag, host boot time (03 §4.0, DB-11).

Rules implemented here:

* a new database gets ``stream_id = uuid7()``;
* on every start ``clean_shutdown`` is set to 0 and ``host_boot_ns`` to the
  current host boot time; a graceful close sets ``clean_shutdown = 1``;
* on start, if the previous run did not shut down cleanly **and** the host
  boot time changed (power loss / OS crash in between) **and** the database
  runs with ``synchronous=NORMAL`` by default, the last commits may have been
  lost, so a new ``stream_id`` is generated. Subscribers see the new stream in
  FACT envelopes and heartbeats and reconcile by domain queries (02 §8.2).
  A plain process crash loses nothing and keeps the stream.
* restore scripts call :func:`regenerate_stream_id` (03 §8).
"""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import Connection, insert, select, update

from futrisk.common.ids import uuid7
from futrisk.db.common_tables import CommonTables

BOOT_TOLERANCE_NS = 10 * 1_000_000_000


@dataclass(frozen=True)
class MetaState:
    """Outcome of :func:`bootstrap_meta`."""

    new_db: bool
    stream_id: str
    stream_regenerated: bool
    previous_clean_shutdown: bool | None
    boot_changed: bool
    created_ns: int
    restored_from: str | None


def read_meta(conn: Connection, tables: CommonTables) -> dict[str, object] | None:
    t = tables.db_meta
    row = conn.execute(select(t).where(t.c.id == 1)).mappings().first()
    return dict(row) if row is not None else None


def bootstrap_meta(
    conn: Connection,
    tables: CommonTables,
    *,
    now_ns: int,
    boot_ns: int,
    synchronous: str,
    tolerance_ns: int = BOOT_TOLERANCE_NS,
) -> MetaState:
    """Create or update the single ``db_meta`` row at service start (inside a write tx)."""
    t = tables.db_meta
    row = read_meta(conn, tables)
    if row is None:
        stream = uuid7()
        conn.execute(
            insert(t).values(
                id=1,
                stream_id=stream,
                created_ns=now_ns,
                restored_from=None,
                clean_shutdown=0,
                host_boot_ns=boot_ns,
            )
        )
        return MetaState(
            new_db=True,
            stream_id=stream,
            stream_regenerated=False,
            previous_clean_shutdown=None,
            boot_changed=False,
            created_ns=now_ns,
            restored_from=None,
        )
    prev_clean = bool(row["clean_shutdown"])
    prev_boot = row["host_boot_ns"]
    boot_changed = prev_boot is not None and abs(int(str(prev_boot)) - boot_ns) > tolerance_ns
    stream = str(row["stream_id"])
    regenerate = (not prev_clean) and boot_changed and synchronous.upper() == "NORMAL"
    if regenerate:
        stream = uuid7()
    conn.execute(
        update(t).where(t.c.id == 1).values(stream_id=stream, clean_shutdown=0, host_boot_ns=boot_ns)
    )
    restored = row.get("restored_from")
    return MetaState(
        new_db=False,
        stream_id=stream,
        stream_regenerated=regenerate,
        previous_clean_shutdown=prev_clean,
        boot_changed=boot_changed,
        created_ns=int(str(row["created_ns"])),
        restored_from=str(restored) if restored is not None else None,
    )


def mark_clean_shutdown(conn: Connection, tables: CommonTables) -> None:
    """Set ``clean_shutdown = 1`` (last transaction of a graceful close)."""
    t = tables.db_meta
    conn.execute(update(t).where(t.c.id == 1).values(clean_shutdown=1))


def regenerate_stream_id(conn: Connection, tables: CommonTables, *, restored_from: str | None = None) -> str:
    """Assign a new ``stream_id`` (restore from backup, 03 §8). Returns the new id."""
    t = tables.db_meta
    stream = uuid7()
    values: dict[str, object] = {"stream_id": stream}
    if restored_from is not None:
        values["restored_from"] = restored_from
    conn.execute(update(t).where(t.c.id == 1).values(**values))
    return stream


__all__ = [
    "BOOT_TOLERANCE_NS",
    "MetaState",
    "bootstrap_meta",
    "mark_clean_shutdown",
    "read_meta",
    "regenerate_stream_id",
]
