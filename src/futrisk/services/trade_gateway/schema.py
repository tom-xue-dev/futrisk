"""td.db schema of trade-gateway (03 §4.2, common tables 03 §4.0, DB-03 ``synchronous=FULL``, migrations 03 §9).

Tables and columns follow 03 §4.2:

* ``routed_orders``: one row per ``client_order_id`` that reached trade-gateway,
  including the ``EXPIRED`` rows of routes rejected at arrival (expired or kill
  switch), which double as tombstones; ``cancel_requested_ns`` (schema version 2)
  records a ``route.cancel`` durably until the broker has answered it;
* ``route_tombstones``: orders trade-gateway never received, declared lost by
  ``route.resolve`` / ``route.cancel`` (02 §8.3);
* ``broker_order_events``: every broker order report, unique by
  ``(trading_day, broker_seq)``;
* ``trades``: the source of truth for fills, unique by the trade key
  ``(trading_day, exchange, trade_id, direction)``; ``trade_seq`` is an
  ``AUTOINCREMENT`` key so it is never reused (continuous across restarts);
* ``broker_cursors``: resync cursor per ``(broker, trading_day)``;
* ``firm_controls``: trade-gateway's copy of the kill switch records (``action``
  ``RULE_STATE`` since schema version 3).

Besides ``cancel_requested_ns``, the only addition to 03 §4.2 is the plain index
``ix_broker_order_events__client_order_id`` on the foreign-key column.

Migrations (03 §9, forward only, checksummed):

1. the original schema, generated from :data:`METADATA_V1`. Its enum ``CHECK`` lists are
   frozen literals (``_V1_ENUM_VALUES``), so its checksum does not move when an enum
   grows (pinned by the tests);
2. the nullable ``routed_orders.cancel_requested_ns`` column (additive);
3. ``firm_controls`` rebuilt so that ``action`` accepts ``RULE_STATE`` (SQLite cannot alter
   a ``CHECK``; rows, ids and the ``AUTOINCREMENT`` mark are kept).

:data:`METADATA` describes the current schema. An enum value td.db must store needs a
new migration: the tests insert every current enum value into a freshly migrated td.db.
"""

from __future__ import annotations

from enum import StrEnum

from sqlalchemy import (
    CheckConstraint,
    Column,
    ForeignKey,
    Index,
    MetaData,
    PrimaryKeyConstraint,
    Table,
    UniqueConstraint,
    text,
)

from futrisk.db import CommonTables, Migration, add_common_tables
from futrisk.db.types import (
    BIGINT,
    bool_col,
    dec_price_col,
    enum_col,
    id_col,
    int_col,
    json_col,
    td_col,
    text_col,
    ts_col,
    uuid_col,
)
from futrisk.messages.enums import (
    BrokerOrderStatus,
    ControlOrigin,
    Direction,
    Exchange,
    FirmAction,
    Offset,
    OrderType,
    TimeInForce,
    TradeOrigin,
)

DB_NAME = "td"
SYNCHRONOUS = "FULL"  # DB-03: td.db runs with synchronous=FULL for every transaction
SCHEMA_VERSION = 3

#: enum value lists as of schema version 1, frozen: migration 1 is generated from them, so its
#: checksum never changes when an enum grows (a new value td.db must store needs a new migration)
_V1_ENUM_VALUES: dict[type[StrEnum], tuple[str, ...]] = {
    Exchange: ("SHFE", "DCE", "CFFEX", "INE", "CZCE", "GFEX"),
    Direction: ("BUY", "SELL"),
    Offset: ("OPEN", "CLOSE", "CLOSE_TODAY", "CLOSE_YESTERDAY", "FORCE_CLOSE"),
    OrderType: ("LIMIT", "MARKET"),
    TimeInForce: ("GFD", "FAK", "FOK"),
    BrokerOrderStatus: (
        "RECEIVED", "SUBMITTED", "ACCEPTED", "PARTIALLY_FILLED", "FILLED", "CANCEL_PENDING", "CANCELED", "REJECTED",
        "UNKNOWN", "EXPIRED",
    ),
    TradeOrigin: ("INTERNAL", "EXTERNAL"),
    FirmAction: ("KILL_SWITCH_ON", "KILL_SWITCH_OFF", "SET_RESTRICTIONS"),
    ControlOrigin: ("MANUAL", "RULE"),
}
#: ``firm_controls.action`` values since schema version 3 (migration 3)
FIRM_ACTIONS_V3: tuple[str, ...] = ("KILL_SWITCH_ON", "KILL_SWITCH_OFF", "SET_RESTRICTIONS", "RULE_STATE")

#: statuses in which a routed order is finished (no more broker activity expected)
TERMINAL_STATUSES: tuple[BrokerOrderStatus, ...] = (
    BrokerOrderStatus.FILLED,
    BrokerOrderStatus.CANCELED,
    BrokerOrderStatus.REJECTED,
    BrokerOrderStatus.EXPIRED,
)


def _define(md: MetaData, *, version: int) -> CommonTables:
    """Declare every td.db table on ``md`` as of schema ``version`` (1, or the current one)."""
    common = add_common_tables(md, outbox=True, processed_commands=True, fact_cursors=True)

    def ev(enum: type[StrEnum]) -> tuple[str, ...] | type[StrEnum]:
        return _V1_ENUM_VALUES[enum] if version == 1 else enum

    extra: list[Column[object]] = []
    if version >= 2:
        extra.append(ts_col("cancel_requested_ns", nullable=True))  # migration 2 (ALTER TABLE ... ADD COLUMN)
    routed = Table(
        "routed_orders",
        md,
        uuid_col("client_order_id", primary_key=True),
        id_col("account_id", nullable=False),
        id_col("instrument_id", nullable=False),
        enum_col("exchange", ev(Exchange), nullable=False),
        enum_col("direction", ev(Direction), nullable=False),
        enum_col("offset", ev(Offset), nullable=False),
        enum_col("order_type", ev(OrderType), nullable=False),
        enum_col("tif", ev(TimeInForce), nullable=False),
        dec_price_col("price", nullable=True),
        int_col("qty", CheckConstraint("qty>0"), nullable=False),
        td_col("trading_day", nullable=False),
        id_col("src_instance", nullable=False),
        ts_col("expire_ns", nullable=False),
        text_col("broker", nullable=False),
        text_col("broker_order_ref", nullable=True),
        text_col("order_sys_id", nullable=True),
        enum_col("status", ev(BrokerOrderStatus), nullable=False),
        int_col("filled_qty", nullable=False, server_default=text("0")),
        text_col("broker_code", nullable=True),
        text_col("broker_msg", nullable=True),
        ts_col("received_ns", nullable=False),
        ts_col("submitted_ns", nullable=True),
        ts_col("updated_ns", nullable=False),
        *extra,
    )
    Index("ix_routed_orders__td_acct", routed.c.trading_day, routed.c.account_id)
    Index(
        "uq_routed_orders__broker_ref",
        routed.c.broker_order_ref,
        unique=True,
        sqlite_where=routed.c.broker_order_ref.is_not(None),
        postgresql_where=routed.c.broker_order_ref.is_not(None),
    )
    Index(
        "ix_routed_orders__open",
        routed.c.status,
        sqlite_where=routed.c.status.not_in([s.value for s in TERMINAL_STATUSES]),
        postgresql_where=routed.c.status.not_in([s.value for s in TERMINAL_STATUSES]),
    )

    tomb = Table(
        "route_tombstones",
        md,
        uuid_col("client_order_id", primary_key=True),
        td_col("trading_day", nullable=False),
        text_col("command", nullable=False),
        text_col("command_id", nullable=True),
        id_col("requested_by", nullable=False),
        ts_col("created_ns", nullable=False),
    )
    Index("ix_route_tombstones__td", tomb.c.trading_day)

    events = Table(
        "broker_order_events",
        md,
        Column("event_id", BIGINT, primary_key=True, autoincrement=True),
        td_col("trading_day", nullable=False),
        int_col("broker_seq", nullable=False),
        uuid_col("client_order_id", ForeignKey("routed_orders.client_order_id"), nullable=True),
        text_col("broker_order_ref", nullable=False),
        enum_col("origin", ev(TradeOrigin), nullable=False),
        enum_col("status", ev(BrokerOrderStatus), nullable=False),
        int_col("filled_qty", nullable=False),
        int_col("remaining_qty", nullable=False),
        text_col("broker_code", nullable=True),
        text_col("broker_msg", nullable=True),
        json_col("detail", nullable=False, server_default="{}"),
        ts_col("ts_ns", nullable=False),
        UniqueConstraint("trading_day", "broker_seq", name="uq_broker_order_events__td_seq"),
        sqlite_autoincrement=True,
    )
    Index("ix_broker_order_events__client_order_id", events.c.client_order_id)

    fills = Table(
        "trades",
        md,
        # INTEGER PRIMARY KEY AUTOINCREMENT: trade_seq values are never reused (Trade.trade_seq)
        Column("trade_seq", BIGINT, primary_key=True, autoincrement=True),
        td_col("trading_day", nullable=False),
        enum_col("exchange", ev(Exchange), nullable=False),
        text_col("trade_id", nullable=False),
        enum_col("direction", ev(Direction), nullable=False),
        id_col("account_id", nullable=False),
        id_col("instrument_id", nullable=False),
        uuid_col("client_order_id", ForeignKey("routed_orders.client_order_id"), nullable=True),
        enum_col("origin", ev(TradeOrigin), nullable=False),
        text_col("import_batch", nullable=True),
        text_col("broker_order_ref", nullable=False),
        text_col("order_sys_id", nullable=True),
        enum_col("offset", ev(Offset), nullable=False),
        dec_price_col("price", nullable=False),
        int_col("qty", CheckConstraint("qty>0"), nullable=False),
        ts_col("trade_ts_ns", nullable=False),
        int_col("broker_seq", nullable=True),
        ts_col("received_ns", nullable=False),
        UniqueConstraint("trading_day", "exchange", "trade_id", "direction", name="uq_trades__key"),
        sqlite_autoincrement=True,
    )
    Index("ix_trades__td_acct_seq", fills.c.trading_day, fills.c.account_id, fills.c.trade_seq)

    Table(
        "broker_cursors",
        md,
        text_col("broker", nullable=False),
        td_col("trading_day", nullable=False),
        int_col("last_broker_seq", nullable=False),
        text_col("session_id", nullable=True),
        int_col("front_id", nullable=True),
        ts_col("updated_ns", nullable=False),
        PrimaryKeyConstraint("broker", "trading_day"),
    )

    controls = Table(
        "firm_controls",
        md,
        Column("control_id", BIGINT, primary_key=True, autoincrement=True),
        enum_col("action", _V1_ENUM_VALUES[FirmAction] if version < 3 else FIRM_ACTIONS_V3, nullable=False),
        enum_col("origin", ev(ControlOrigin), nullable=False),
        bool_col("active", nullable=False),
        json_col("restrictions", nullable=False, server_default="[]"),
        td_col("trading_day", nullable=False),
        text_col("actor", nullable=True),
        text_col("reason", nullable=False),
        text_col("cause_alert_key", nullable=True),
        id_col("src_instance", nullable=False),
        ts_col("ts_ns", nullable=False),
        sqlite_autoincrement=True,
    )
    Index("ix_firm_controls__origin_action", controls.c.origin, controls.c.action, controls.c.control_id)
    return common


#: schema version 1 (frozen: migration 1 is generated from it, so its DDL must never change)
METADATA_V1 = MetaData()
_define(METADATA_V1, version=1)

#: the current schema
METADATA = MetaData()
COMMON = _define(METADATA, version=SCHEMA_VERSION)

routed_orders = METADATA.tables["routed_orders"]
route_tombstones = METADATA.tables["route_tombstones"]
broker_order_events = METADATA.tables["broker_order_events"]
trades = METADATA.tables["trades"]
broker_cursors = METADATA.tables["broker_cursors"]
firm_controls = METADATA.tables["firm_controls"]

_MIGRATION_3_SQL = """\
-- risk.control.firm carries the whole RULE layer as one RULE_STATE fact; SQLite cannot alter a CHECK,
-- so firm_controls is rebuilt (rows, control_id values and the AUTOINCREMENT mark are kept)
CREATE TABLE firm_controls__v3 (
    control_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    action TEXT NOT NULL CHECK (action IN ('KILL_SWITCH_ON','KILL_SWITCH_OFF','SET_RESTRICTIONS','RULE_STATE')),
    origin TEXT NOT NULL CHECK (origin IN ('MANUAL','RULE')),
    active INTEGER DEFAULT 0 NOT NULL CHECK (active IN (0,1)),
    restrictions TEXT DEFAULT '[]' NOT NULL CHECK (json_valid(restrictions)),
    trading_day VARCHAR(8) NOT NULL CHECK (length(trading_day)=8),
    actor TEXT,
    reason TEXT NOT NULL,
    cause_alert_key TEXT,
    src_instance VARCHAR(64) NOT NULL,
    ts_ns INTEGER NOT NULL
);
INSERT INTO firm_controls__v3 (control_id, action, origin, active, restrictions, trading_day, actor, reason,
                               cause_alert_key, src_instance, ts_ns)
SELECT control_id, action, origin, active, restrictions, trading_day, actor, reason, cause_alert_key, src_instance, ts_ns
FROM firm_controls;
DROP TABLE firm_controls;
ALTER TABLE firm_controls__v3 RENAME TO firm_controls;
CREATE INDEX ix_firm_controls__origin_action ON firm_controls (origin, action, control_id);
"""

#: 03 §9 forward-only migrations of td.db
MIGRATIONS: list[Migration] = [
    Migration.create_all(1, "td.db initial schema (03 §4.0, §4.2)", METADATA_V1),
    Migration.sql_script(
        2,
        "routed_orders.cancel_requested_ns: durable route.cancel requests",
        "ALTER TABLE routed_orders ADD COLUMN cancel_requested_ns INTEGER;\n",
    ),
    Migration.sql_script(3, "firm_controls.action accepts RULE_STATE (table rebuilt)", _MIGRATION_3_SQL),
]

__all__ = [
    "COMMON",
    "DB_NAME",
    "FIRM_ACTIONS_V3",
    "METADATA",
    "METADATA_V1",
    "MIGRATIONS",
    "SCHEMA_VERSION",
    "SYNCHRONOUS",
    "TERMINAL_STATUSES",
    "broker_cursors",
    "broker_order_events",
    "firm_controls",
    "route_tombstones",
    "routed_orders",
    "trades",
]
