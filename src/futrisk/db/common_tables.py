"""Common tables present in every service database (03 §4.0).

=====================  ==  ==  ====  ========  ===  ==========
table                  md  td  risk  clearing  api  supervisor
=====================  ==  ==  ====  ========  ===  ==========
schema_version         Y   Y   Y     Y         Y    Y
db_meta                Y   Y   Y     Y         Y    Y
outbox, outbox_topics  Y   Y   Y     Y         -    Y
processed_commands     Y   Y   Y     Y         Y    Y
fact_cursors           -   Y   Y     Y         Y    -
=====================  ==  ==  ====  ========  ===  ==========

A service adds them to its own :class:`~sqlalchemy.MetaData` and then defines
its business tables next to them::

    metadata = MetaData()
    common = add_common_tables(metadata, outbox=True, fact_cursors=False)   # md.db
    watermarks = Table("watermarks", metadata, ...)

Column names, types, keys and index names follow 03 §4.0 exactly.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from sqlalchemy import CheckConstraint, Column, Index, MetaData, PrimaryKeyConstraint, Table, UniqueConstraint

from futrisk.db.types import (
    BIGINT,
    blob_col,
    bool_col,
    enum_col,
    hash_col,
    id_col,
    int_col,
    td_col,
    text_col,
    ts_col,
    uuid_col,
)
from futrisk.messages.enums import FactStartPolicy

SCHEMA_VERSION = "schema_version"
DB_META = "db_meta"
OUTBOX = "outbox"
OUTBOX_TOPICS = "outbox_topics"
PROCESSED_COMMANDS = "processed_commands"
FACT_CURSORS = "fact_cursors"


@dataclass(frozen=True)
class CommonTables:
    """Handles to the common tables of one database (``None`` when absent)."""

    metadata: MetaData
    schema_version: Table
    db_meta: Table
    outbox: Table | None
    outbox_topics: Table | None
    processed_commands: Table | None
    fact_cursors: Table | None


def _schema_version(md: MetaData) -> Table:
    return Table(
        SCHEMA_VERSION,
        md,
        int_col("version", primary_key=True, autoincrement=False),
        text_col("description", nullable=False),
        hash_col("checksum", nullable=False),
        ts_col("applied_ns", nullable=False),
    )


def _db_meta(md: MetaData) -> Table:
    return Table(
        DB_META,
        md,
        int_col("id", CheckConstraint("id=1"), primary_key=True, autoincrement=False),
        uuid_col("stream_id", nullable=False),
        ts_col("created_ns", nullable=False),
        text_col("restored_from", nullable=True),
        bool_col("clean_shutdown", nullable=False, default=False),
        ts_col("host_boot_ns", nullable=True),
    )


def _outbox_topics(md: MetaData) -> Table:
    return Table(
        OUTBOX_TOPICS,
        md,
        text_col("topic", primary_key=True),
        int_col("last_seq", nullable=False),
        ts_col("updated_ns", nullable=False),
    )


def _outbox(md: MetaData) -> Table:
    t = Table(
        OUTBOX,
        md,
        # INTEGER PRIMARY KEY AUTOINCREMENT: ids are never reused, even after purges.
        Column("outbox_id", BIGINT, primary_key=True, autoincrement=True),
        text_col("topic", nullable=False),
        int_col("topic_seq", nullable=False),
        text_col("type", nullable=False),
        int_col("v", nullable=False),
        text_col("epoch", nullable=False),
        td_col("td", nullable=True),
        text_col("corr_id", nullable=True),
        blob_col("payload", nullable=False),
        ts_col("created_ns", nullable=False),
        ts_col("published_ns", nullable=True),
        UniqueConstraint("topic", "topic_seq", name="uq_outbox__topic_seq"),
        sqlite_autoincrement=True,
    )
    Index(
        "ix_outbox__unpublished",
        t.c.published_ns,
        sqlite_where=t.c.published_ns.is_(None),
        postgresql_where=t.c.published_ns.is_(None),
    )
    return t


def _processed_commands(md: MetaData) -> Table:
    t = Table(
        PROCESSED_COMMANDS,
        md,
        text_col("command_id", primary_key=True),
        text_col("command", nullable=False),
        text_col("requester", nullable=False),
        hash_col("request_hash", nullable=False),
        text_col("result_code", nullable=False),
        blob_col("reply", nullable=False),
        td_col("trading_day", nullable=False),
        ts_col("received_ns", nullable=False),
    )
    Index("ix_processed_commands__td", t.c.trading_day)
    return t


def _fact_cursors(md: MetaData) -> Table:
    return Table(
        FACT_CURSORS,
        md,
        id_col("src", nullable=False),
        text_col("topic", nullable=False),
        uuid_col("stream", nullable=False),
        int_col("last_seq", nullable=False),
        enum_col("start_policy", FactStartPolicy, nullable=False),
        ts_col("updated_ns", nullable=False),
        PrimaryKeyConstraint("src", "topic"),
    )


def add_common_tables(
    metadata: MetaData,
    *,
    outbox: bool = True,
    processed_commands: bool = True,
    fact_cursors: bool = True,
) -> CommonTables:
    """Add the 03 §4.0 common tables to ``metadata`` (idempotent) and return handles.

    ``schema_version`` and ``db_meta`` are always added. Pass ``outbox=False``
    for api.db, ``fact_cursors=False`` for md.db and supervisor.db.
    """

    def get_or(name: str, factory: Callable[[MetaData], Table]) -> Table:
        existing = metadata.tables.get(name)
        if existing is not None:
            return existing
        return factory(metadata)

    sv = get_or(SCHEMA_VERSION, _schema_version)
    meta = get_or(DB_META, _db_meta)
    ob = get_or(OUTBOX, _outbox) if outbox else metadata.tables.get(OUTBOX)
    obt = get_or(OUTBOX_TOPICS, _outbox_topics) if outbox else metadata.tables.get(OUTBOX_TOPICS)
    pc = get_or(PROCESSED_COMMANDS, _processed_commands) if processed_commands else metadata.tables.get(PROCESSED_COMMANDS)
    fc = get_or(FACT_CURSORS, _fact_cursors) if fact_cursors else metadata.tables.get(FACT_CURSORS)
    return CommonTables(
        metadata=metadata,
        schema_version=sv,
        db_meta=meta,
        outbox=ob,
        outbox_topics=obt,
        processed_commands=pc,
        fact_cursors=fc,
    )


def common_tables_of(metadata: MetaData) -> CommonTables:
    """Look up the common tables already present in ``metadata``.

    Raises ``ValueError`` when ``schema_version`` or ``db_meta`` is missing
    (call :func:`add_common_tables` when defining the schema).
    """
    sv = metadata.tables.get(SCHEMA_VERSION)
    meta = metadata.tables.get(DB_META)
    if sv is None or meta is None:
        raise ValueError("metadata lacks schema_version/db_meta; call add_common_tables(metadata) first")
    return CommonTables(
        metadata=metadata,
        schema_version=sv,
        db_meta=meta,
        outbox=metadata.tables.get(OUTBOX),
        outbox_topics=metadata.tables.get(OUTBOX_TOPICS),
        processed_commands=metadata.tables.get(PROCESSED_COMMANDS),
        fact_cursors=metadata.tables.get(FACT_CURSORS),
    )


def schema_version_table() -> Table:
    """A standalone ``schema_version`` definition used by the migration runner."""
    return _schema_version(MetaData())


__all__ = [
    "DB_META",
    "FACT_CURSORS",
    "OUTBOX",
    "OUTBOX_TOPICS",
    "PROCESSED_COMMANDS",
    "SCHEMA_VERSION",
    "CommonTables",
    "add_common_tables",
    "common_tables_of",
    "schema_version_table",
]
