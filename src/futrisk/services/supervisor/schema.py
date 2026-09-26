"""supervisor.db schema and migrations (03 §4.0, §4.6, §9).

=================  ===========================================================
table              content
=================  ===========================================================
common tables      ``schema_version``, ``db_meta``, ``outbox`` + ``outbox_topics``
                   (``sys.service`` FACTs), ``processed_commands`` (``svc.*``
                   idempotency); no ``fact_cursors`` (the supervisor consumes no FACT)
``service_events`` every process state transition (``sys.service`` business table,
                   data source of the daily report's "system" section)
``clock_state``    the market clock, single row (``id = 1``)
=================  ===========================================================

Migrations (forward only, 03 §9):

1. ``initial schema`` - the v1 tables exactly as first shipped (:func:`v1_metadata`), so
   databases created by earlier builds keep their recorded checksum;
2. ``service events v2`` - rebuilds ``service_events`` (SQLite cannot alter a CHECK):
   ``reason`` also admits ``GAVE_UP`` and ``SUPERVISOR_RESTART``, and two nullable columns
   are added: ``env_keys`` (JSON list of per-instance environment override *names*,
   never values; audit of ``svc.start``/``svc.restart`` env and ``--svc-env``) and
   ``detail`` (human-readable cause, e.g. a port conflict or why a unit was parked).

The whole database runs with ``synchronous=FULL`` (03 DB-03).
"""

from __future__ import annotations

from sqlalchemy import CheckConstraint, Column, Connection, Index, MetaData, Table
from sqlalchemy.schema import CreateTable

from futrisk.db import Migration, add_common_tables
from futrisk.db.types import BIGINT, enum_col, id_col, int_col, json_col, td_col, text_col, ts_col
from futrisk.messages.enums import ClockMode, ProcessState, ServiceChangeReason

DB_NAME = "supervisor"

# ``reason`` values of the v1 CHECK constraint (before GAVE_UP / SUPERVISOR_RESTART)
_V1_REASONS = ("STARTUP", "SHUTDOWN", "EXITED", "HEARTBEAT_LOST", "LOOP_HANG", "KILLED_BY_DEMO", "MANUAL")


def _clock_state(md: MetaData) -> Table:
    return Table(
        "clock_state",
        md,
        int_col("id", CheckConstraint("id=1"), primary_key=True, autoincrement=False),
        enum_col("mode", ClockMode, nullable=False),
        int_col("offset_ns", nullable=False),
        int_col("version", nullable=False),
        ts_col("updated_ns", nullable=False),
        text_col("actor", nullable=True),
        text_col("reason", nullable=True),
        CheckConstraint("mode='DEMO' OR offset_ns=0", name="ck_clock_state__real_offset"),
    )


def _service_events(md: MetaData, *, name: str = "service_events", v2: bool = True) -> Table:
    columns = [
        Column("event_id", BIGINT, primary_key=True, autoincrement=True),
        ts_col("ts_ns", nullable=False),
        td_col("trading_day", nullable=True),
        id_col("instance", nullable=False),
        id_col("service", nullable=False),
        int_col("pid", nullable=True),
        enum_col("from_state", ProcessState, nullable=False),
        enum_col("to_state", ProcessState, nullable=False),
        int_col("exit_code", nullable=True),
        int_col("restarts", nullable=False),
        enum_col("reason", ServiceChangeReason if v2 else _V1_REASONS, nullable=False),
        text_col("actor", nullable=True),
    ]
    if v2:
        columns += [json_col("env_keys", nullable=True), text_col("detail", nullable=True)]
    return Table(name, md, *columns, sqlite_autoincrement=True)


def v1_metadata() -> MetaData:
    """The schema of migration 1, frozen (its fingerprint is the recorded checksum)."""
    md = MetaData()
    add_common_tables(md, outbox=True, processed_commands=True, fact_cursors=False)
    events = _service_events(md, v2=False)
    Index("ix_service_events__ts", events.c.ts_ns)
    _clock_state(md)
    return md


# ---------------------------------------------------------------- current schema (after migration 2)
METADATA = MetaData()
COMMON = add_common_tables(METADATA, outbox=True, processed_commands=True, fact_cursors=False)
service_events = _service_events(METADATA)
Index("ix_service_events__ts", service_events.c.ts_ns)
clock_state = _clock_state(METADATA)

_V1_COLUMNS = (
    "event_id, ts_ns, trading_day, instance, service, pid, from_state, to_state, exit_code, restarts, reason, actor"
)


def _migrate_service_events_v2(conn: Connection) -> None:
    """Rebuild ``service_events`` with the v2 ``reason`` CHECK and the ``env_keys``/``detail`` columns."""
    scratch = MetaData()
    new = _service_events(scratch, name="service_events__v2")
    conn.exec_driver_sql(str(CreateTable(new).compile(dialect=conn.dialect)))
    conn.exec_driver_sql(f"INSERT INTO service_events__v2 ({_V1_COLUMNS}) SELECT {_V1_COLUMNS} FROM service_events")
    conn.exec_driver_sql("DROP TABLE service_events")
    conn.exec_driver_sql("ALTER TABLE service_events__v2 RENAME TO service_events")
    conn.exec_driver_sql("CREATE INDEX ix_service_events__ts ON service_events (ts_ns)")


MIGRATIONS: list[Migration] = [
    Migration.create_all(1, "initial schema", v1_metadata()),
    Migration.from_callable(
        2,
        "service events v2",
        _migrate_service_events_v2,
        checksum_source="supervisor.service_events v2: reason += GAVE_UP, SUPERVISOR_RESTART; + env_keys JSON, detail TEXT",
    ),
]

__all__ = ["COMMON", "DB_NAME", "METADATA", "MIGRATIONS", "clock_state", "service_events", "v1_metadata"]
