"""Per-service SQLite access layer (03 §2 DB-01..DB-11, §4.0, §7, §9).

* :class:`Database`: pragmas, one writer thread (``submit``/``write``/``run_write``),
  ``query_only`` read pool, critical (``synchronous=FULL``) transactions,
  migrations and ``db_meta`` start/stop bookkeeping.
* :func:`add_common_tables`: the 03 §4.0 common tables for a service MetaData.
* :class:`Outbox`, :class:`ProcessedCommands`, :class:`FactCursors`: APIs over
  the common tables (the outbox append and the cursor upsert are prepared
  statements: hot path).
* :class:`PreparedSql`: a fixed statement run straight on the job's sqlite3
  connection, for services' own hot statements.
* :mod:`futrisk.db.types`: 03 §3.1 logical column types.
"""

from futrisk.db.common_tables import CommonTables, add_common_tables, common_tables_of
from futrisk.db.cursors import FactCursor, FactCursors
from futrisk.db.database import (
    Database,
    DbOpenInfo,
    PreparedSql,
    Synchronous,
    TxState,
    after_commit,
    current_tx,
    driver_connection,
    tx_state,
)
from futrisk.db.meta import MetaState, regenerate_stream_id
from futrisk.db.migrations import (
    Migration,
    MigrationError,
    MigrationRunner,
    freeze_schema,
    load_sql_migrations,
    render_ddl,
    schema_fingerprint,
)
from futrisk.db.outbox import Outbox, OutboxRow, ReplayNotFound, ReplayPage, row_envelope
from futrisk.db.processed import DedupeCheck, DedupeOutcome, ProcessedCommands, ProcessedEntry, request_hash

__all__ = [
    "CommonTables",
    "Database",
    "DbOpenInfo",
    "DedupeCheck",
    "DedupeOutcome",
    "FactCursor",
    "FactCursors",
    "MetaState",
    "Migration",
    "MigrationError",
    "MigrationRunner",
    "Outbox",
    "OutboxRow",
    "PreparedSql",
    "ProcessedCommands",
    "ProcessedEntry",
    "ReplayNotFound",
    "ReplayPage",
    "Synchronous",
    "TxState",
    "add_common_tables",
    "after_commit",
    "common_tables_of",
    "current_tx",
    "driver_connection",
    "freeze_schema",
    "load_sql_migrations",
    "regenerate_stream_id",
    "render_ddl",
    "request_hash",
    "row_envelope",
    "schema_fingerprint",
    "tx_state",
]
