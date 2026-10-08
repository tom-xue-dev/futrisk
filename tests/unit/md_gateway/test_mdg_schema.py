"""md.db schema (03 §9): the frozen migration checksum, unsigned fingerprints and every current enum value storable."""

from __future__ import annotations

import sqlite3
from enum import StrEnum
from pathlib import Path

from futrisk.common.clock import SimClock
from futrisk.db import Database
from futrisk.messages.enums import (
    Exchange,
    GapStatus,
    LimitLock,
    MdEventKind,
    SettleInputQuality,
    SettleMethod,
    SourceId,
)
from futrisk.services.md_gateway import store
from futrisk.services.md_gateway.schema import METADATA, MIGRATIONS, SCHEMA_VERSION

from .helpers import T0, TD

#: released migrations must never change: databases record these checksums
FROZEN_CHECKSUMS = {1: "f58aff75b9ded2501425516252485edcb4a2149e9c91189f4d3d1539950e0177"}


def open_md(path: Path) -> Database:
    db = Database(path, METADATA, name="md", synchronous="NORMAL", migrations=MIGRATIONS, clock=SimClock(T0))
    db.open()
    return db


def test_released_migration_checksums_are_frozen() -> None:
    assert SCHEMA_VERSION == MIGRATIONS[-1].version == 1
    assert {m.version: m.checksum for m in MIGRATIONS} == FROZEN_CHECKSUMS


def test_watermark_fingerprints_round_trip_as_unsigned_64_bit(tmp_path: Path) -> None:
    path = tmp_path / "md.db"
    db = open_md(path)
    try:
        assert db.read(lambda c: [tuple(r) for r in c.exec_driver_sql(
            "SELECT version, checksum FROM schema_version ORDER BY version").fetchall()]) == sorted(FROZEN_CHECKSUMS.items())
        db.write(lambda c: store.save_watermarks(c, [("rb2701", (TD, 100, T0), None)], T0))
        assert db.read(store.load_watermarks) == {"rb2701": ((TD, 100, T0), None)}
        big = (1 << 64) - 5  # fingerprints are unsigned 64-bit, SQLite integers are signed
        db.write(lambda c: store.save_watermarks(c, [("rb2701", (TD, 101, T0 + 1), big)], T0))
        assert db.read(store.load_watermarks) == {"rb2701": ((TD, 101, T0 + 1), big)}
        db.write(lambda c: store.save_watermarks(c, [("rb2701", None, None)], T0))
        assert db.read(store.load_watermarks) == {}
    finally:
        db.close()
    open_md(path).close()  # reopening applies nothing and the checksum still matches


_BASE_ROWS: dict[str, dict[str, object]] = {
    "feed_cursors": dict(epoch=None, last_contiguous_seq=None, last_exch_ts_ns=None, trading_day=TD, updated_ns=T0),
    "gaps": dict(from_ns=T0, trading_day=TD, status="OPEN", opened_ns=T0),
    "md_events": dict(kind="BAD_TICK", trading_day=TD, ts_ns=T0, detail="{}"),
    "settle_inputs": dict(trading_day=TD, exchange="SHFE", method="VWAP_FULL_DAY", multiplier=10, tick_size="1",
                          day_volume=0, day_turnover="0", limit_lock="NONE", pre_settle="3120", quality="OK", computed_ns=T0),
}
_ENUM_COLUMNS: list[tuple[str, str, type[StrEnum]]] = [
    ("feed_cursors", "source", SourceId),
    ("gaps", "source", SourceId),
    ("gaps", "status", GapStatus),
    ("md_events", "kind", MdEventKind),
    ("md_events", "source", SourceId),
    ("settle_inputs", "exchange", Exchange),
    ("settle_inputs", "method", SettleMethod),
    ("settle_inputs", "limit_lock", LimitLock),
    ("settle_inputs", "quality", SettleInputQuality),
]


def _unique(table: str, n: int) -> dict[str, object]:
    return {
        "feed_cursors": {"source": None},
        "gaps": {"gap_id": f"g{n}"},
        "md_events": {"event_id": f"e{n}"},
        "settle_inputs": {"instrument_id": "rb2701", "version": n},
    }[table]


def test_every_current_enum_value_can_be_stored(tmp_path: Path) -> None:
    """A value added to a shared enum that md.db cannot store fails here, not in production (needs a migration)."""
    path = tmp_path / "md.db"
    open_md(path).close()
    con = sqlite3.connect(path)
    refused: list[str] = []
    try:
        n = 0
        for table, column, enum in _ENUM_COLUMNS:
            for value in enum:
                n += 1
                row = {**_BASE_ROWS[table], **_unique(table, n), column: value.value}
                cols = ", ".join(row)
                try:
                    con.execute(f"INSERT INTO {table} ({cols}) VALUES ({', '.join('?' * len(row))})", tuple(row.values()))
                except sqlite3.IntegrityError as exc:
                    refused.append(f"{table}.{column}={value.value}: {exc}")
                if table == "feed_cursors":
                    con.execute("DELETE FROM feed_cursors")
        con.rollback()
    finally:
        con.close()
    assert refused == []
