"""api.db schema and migrations (03 §4.5, §4.0, §9).

api.db belongs to api-gateway alone (single instance, the only writer, 03 §1).
Besides the common tables ``schema_version``, ``db_meta``, ``processed_commands``
(REST ``Idempotency-Key``) and ``fact_cursors`` (notifier cursors) it holds:

* ``api_keys`` - API keys (SHA-256 only, never the plaintext) and their role;
* ``api_audit`` - one append-only row per REST request / WebSocket session;
* ``notifications`` - webhook notifications deduplicated by ``(notify_key, channel)``;
* ``config_versions`` - config versions and the persisted 2PC decisions (02 §8.5);
* ``kill_switch`` - the one desired manual kill-switch state (on/off, reason,
  actor, command id) with the per-target confirmation (02 §6.3; schema v2).

api.db has no outbox: api-gateway publishes nothing but its heartbeat (02 §3).
The whole database runs with ``synchronous=FULL`` (03 DB-03).

Migrations are frozen SQL (03 §9: forward-only, checksummed). The DDL below is
:func:`futrisk.db.render_ddl` of :data:`METADATA` at release time (v1 + the
tables added by later versions); a unit test keeps the two in sync. The SQL lives in this module (not in ``.sql`` data files)
so that a frozen executable needs no extra data files.
"""

from __future__ import annotations

from enum import StrEnum

from sqlalchemy import (
    CheckConstraint,
    Column,
    Index,
    MetaData,
    PrimaryKeyConstraint,
    Table,
    UniqueConstraint,
    text,
)

from futrisk.db import Migration, add_common_tables
from futrisk.db.types import (
    BIGINT,
    bool_col,
    enum_col,
    hash_col,
    id_col,
    int_col,
    json_col,
    text_col,
    ts_col,
)
from futrisk.messages.enums import ConfigStatus, Role, Severity


class NotificationStatus(StrEnum):
    """Storage-only enum of ``notifications.status`` (03 §3.3)."""

    PENDING = "PENDING"
    SENT = "SENT"
    FAILED = "FAILED"
    SUPPRESSED = "SUPPRESSED"


METADATA = MetaData()
COMMON = add_common_tables(METADATA, outbox=False, processed_commands=True, fact_cursors=True)

api_keys = Table(
    "api_keys",
    METADATA,
    id_col("key_id", primary_key=True),
    hash_col("key_hash", nullable=False),
    text_col("name", nullable=False),
    enum_col("role", Role, nullable=False),
    ts_col("created_ns", nullable=False),
    ts_col("expires_ns", nullable=True),
    bool_col("disabled", nullable=False, default=False),
    UniqueConstraint("key_hash", name="uq_api_keys__key_hash"),
)

api_audit = Table(
    "api_audit",
    METADATA,
    Column("audit_id", BIGINT, primary_key=True, autoincrement=True),
    ts_col("ts_ns", nullable=False),
    id_col("key_id", nullable=True),
    enum_col("role", Role, nullable=True),
    text_col("method", nullable=False),
    text_col("path", nullable=False),
    text_col("command", nullable=True),
    text_col("command_id", nullable=True),
    text_col("target", nullable=True),
    int_col("http_status", nullable=False),
    text_col("result_code", nullable=False),
    int_col("latency_us", nullable=False),
    text_col("client_ip", nullable=False),
    sqlite_autoincrement=True,
)
Index("ix_api_audit__ts", api_audit.c.ts_ns)

notifications = Table(
    "notifications",
    METADATA,
    text_col("notify_key", nullable=False),
    text_col("channel", nullable=False),
    text_col("kind", nullable=False),
    enum_col("severity", Severity, nullable=False),
    enum_col("status", NotificationStatus, nullable=False),
    int_col("attempts", nullable=False, server_default=text("0")),
    text_col("last_error", nullable=True),
    json_col("payload", nullable=False),
    ts_col("first_seen_ns", nullable=False),
    ts_col("sent_ns", nullable=True),
    PrimaryKeyConstraint("notify_key", "channel"),
)

config_versions = Table(
    "config_versions",
    METADATA,
    int_col("config_version", primary_key=True, autoincrement=False),
    hash_col("config_hash", nullable=False),
    text_col("path", nullable=False),
    json_col("files", nullable=False),
    json_col("effective", nullable=False),
    enum_col("status", ConfigStatus, nullable=False),
    json_col("acks", nullable=False, server_default=text("'{}'")),
    text_col("requested_by", nullable=False),
    ts_col("requested_ns", nullable=False),
    ts_col("finished_ns", nullable=True),
    text_col("error", nullable=True),
)

# One row (id=1): the operator's latest manual kill-switch decision and which
# targets (risk shards + trade-gateway) confirmed it (02 §6.3). ``targets`` is
# ``{target: {status: PENDING|CONFIRMED|FAILED, code, message, attempts, updated_ns}}``.
kill_switch = Table(
    "kill_switch",
    METADATA,
    int_col("id", CheckConstraint("id=1"), primary_key=True, autoincrement=False),
    bool_col("desired_on", nullable=False),
    text_col("command_id", nullable=False),
    text_col("reason", nullable=False),
    text_col("actor", nullable=False),
    enum_col("role", Role, nullable=False),
    ts_col("requested_ns", nullable=False),
    json_col("targets", nullable=False, server_default=text("'{}'")),
    ts_col("updated_ns", nullable=False),
)

# --------------------------------------------------------------------- migrations
# Frozen output of ``render_ddl(METADATA)`` for schema version 1 (03 §9), minus
# ``schema_version`` which the migration runner creates itself.
V1_SQL = """\
CREATE TABLE api_audit (
	audit_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
	ts_ns INTEGER NOT NULL,
	key_id VARCHAR(64),
	role TEXT CHECK (role IN ('VIEWER','TRADER','RISK_MANAGER','ADMIN')),
	method TEXT NOT NULL,
	path TEXT NOT NULL,
	command TEXT,
	command_id TEXT,
	target TEXT,
	http_status INTEGER NOT NULL,
	result_code TEXT NOT NULL,
	latency_us INTEGER NOT NULL,
	client_ip TEXT NOT NULL
);

CREATE INDEX ix_api_audit__ts ON api_audit (ts_ns);

CREATE TABLE api_keys (
	key_id VARCHAR(64) NOT NULL,
	key_hash VARCHAR(64) NOT NULL,
	name TEXT NOT NULL,
	role TEXT NOT NULL CHECK (role IN ('VIEWER','TRADER','RISK_MANAGER','ADMIN')),
	created_ns INTEGER NOT NULL,
	expires_ns INTEGER,
	disabled INTEGER DEFAULT 0 NOT NULL CHECK (disabled IN (0,1)),
	PRIMARY KEY (key_id),
	CONSTRAINT uq_api_keys__key_hash UNIQUE (key_hash)
);

CREATE TABLE config_versions (
	config_version INTEGER NOT NULL,
	config_hash VARCHAR(64) NOT NULL,
	path TEXT NOT NULL,
	files TEXT NOT NULL CHECK (json_valid(files)),
	effective TEXT NOT NULL CHECK (json_valid(effective)),
	status TEXT NOT NULL CHECK (status IN ('PREPARING','COMMITTING','COMMITTED','ABORTED','FAILED')),
	acks TEXT DEFAULT '{}' NOT NULL CHECK (json_valid(acks)),
	requested_by TEXT NOT NULL,
	requested_ns INTEGER NOT NULL,
	finished_ns INTEGER,
	error TEXT,
	PRIMARY KEY (config_version)
);

CREATE TABLE db_meta (
	id INTEGER NOT NULL CHECK (id=1),
	stream_id VARCHAR(36) NOT NULL,
	created_ns INTEGER NOT NULL,
	restored_from TEXT,
	clean_shutdown INTEGER DEFAULT 0 NOT NULL CHECK (clean_shutdown IN (0,1)),
	host_boot_ns INTEGER,
	PRIMARY KEY (id)
);

CREATE TABLE fact_cursors (
	src VARCHAR(64) NOT NULL,
	topic TEXT NOT NULL,
	stream VARCHAR(36) NOT NULL,
	last_seq INTEGER NOT NULL,
	start_policy TEXT NOT NULL CHECK (start_policy IN ('FROM_BEGINNING','FROM_NOW')),
	updated_ns INTEGER NOT NULL,
	PRIMARY KEY (src, topic)
);

CREATE TABLE notifications (
	notify_key TEXT NOT NULL,
	channel TEXT NOT NULL,
	kind TEXT NOT NULL,
	severity TEXT NOT NULL CHECK (severity IN ('INFO','WARNING','HIGH','CRITICAL')),
	status TEXT NOT NULL CHECK (status IN ('PENDING','SENT','FAILED','SUPPRESSED')),
	attempts INTEGER DEFAULT 0 NOT NULL,
	last_error TEXT,
	payload TEXT NOT NULL CHECK (json_valid(payload)),
	first_seen_ns INTEGER NOT NULL,
	sent_ns INTEGER,
	PRIMARY KEY (notify_key, channel)
);

CREATE TABLE processed_commands (
	command_id TEXT NOT NULL,
	command TEXT NOT NULL,
	requester TEXT NOT NULL,
	request_hash VARCHAR(64) NOT NULL,
	result_code TEXT NOT NULL,
	reply BLOB NOT NULL,
	trading_day VARCHAR(8) NOT NULL CHECK (length(trading_day)=8),
	received_ns INTEGER NOT NULL,
	PRIMARY KEY (command_id)
);

CREATE INDEX ix_processed_commands__td ON processed_commands (trading_day);
"""

# Schema version 2: the persisted kill-switch desired state (review H1/M1).
V2_SQL = """\
CREATE TABLE kill_switch (
	id INTEGER NOT NULL CHECK (id=1),
	desired_on INTEGER DEFAULT 0 NOT NULL CHECK (desired_on IN (0,1)),
	command_id TEXT NOT NULL,
	reason TEXT NOT NULL,
	actor TEXT NOT NULL,
	role TEXT NOT NULL CHECK (role IN ('VIEWER','TRADER','RISK_MANAGER','ADMIN')),
	requested_ns INTEGER NOT NULL,
	targets TEXT DEFAULT '{}' NOT NULL CHECK (json_valid(targets)),
	updated_ns INTEGER NOT NULL,
	PRIMARY KEY (id)
);
"""

MIGRATIONS: list[Migration] = [
    Migration.sql_script(1, "api.db initial schema", V1_SQL),
    Migration.sql_script(2, "kill switch desired state", V2_SQL),
]

__all__ = [
    "COMMON",
    "METADATA",
    "MIGRATIONS",
    "V1_SQL",
    "V2_SQL",
    "NotificationStatus",
    "api_audit",
    "api_keys",
    "config_versions",
    "kill_switch",
    "notifications",
]
