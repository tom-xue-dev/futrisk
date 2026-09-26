"""risk-{shard}.db schema of the risk engine (03 §4.3, common tables 03 §4.0, DB-03 ``synchronous=NORMAL``).

One database per shard (``<home>/db/risk-{shard}.db``); the tables and columns follow 03 §4.3
exactly, and the column names are the ones the EngineCore writes in its ``Persist`` rows
(``futrisk/core/ENGINE.md`` §6). The database runs with ``synchronous=NORMAL``; the critical
transactions (latch / reset, manual halt, kill switch, cash, EOD, recon breaks, liquidation
plans, manual commands) are committed with ``synchronous=FULL`` by the shell (``Persist.critical``).

Schema version 2 adds three nullable columns to ``orders`` (``cancel_command_id``, ``cancel_sent_ns``,
``cancel_attempts``: the EngineCore's cancel retry bookkeeping) with an ``ALTER TABLE`` migration; version 3 adds
``RULE_STATE`` to the ``firm_controls.action`` CHECK list (table rebuild). Enum CHECK lists are frozen per schema
version (:func:`enum_values`), never taken from the live message enums.

Other additions to 03 §4.3 (plain indexes, no columns): ``ix_cash_movements__td_acct`` for the history
query ``risk.query.cash`` and ``ix_liquidation_plans__status`` for the restore of pending plans.
"""

from __future__ import annotations

from typing import Final

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
    dec_money_col,
    dec_price_col,
    enum_col,
    hash_col,
    id_col,
    int_col,
    json_col,
    real_col,
    td_col,
    text_col,
    ts_col,
    uuid_col,
)
from futrisk.messages.enums import OrderStatus

SYNCHRONOUS: Final = "NORMAL"  # DB-03: md and risk databases default to NORMAL, critical transactions use FULL

# storage-only enums (03 §3.3)
ALERT_STATUSES = ("ACTIVE", "CLEARED", "RESET", "ROLLED")
RISK_EOD_STATUSES = ("OPEN", "FROZEN", "PROVISIONAL", "ROLLED", "FORCE_UNFROZEN")
AUDIT_ACTIONS = (
    "RISK_RESET",
    "RISK_HALT",
    "ALERT_ACK",
    "KILL_SWITCH_ON",
    "KILL_SWITCH_OFF",
    "FIRM_RESTRICTIONS",
    "CASH_MOVE",
    "LIQUIDATION_CONFIRM",
    "LIQUIDATION_CANCEL",
    "EOD_FORCE_UNFREEZE",
    "RECON_RESOLVE",
    "DAYROLL",
    "DAYROLL_REBASE",
    "PROVISIONAL_SOD",
    "CONFIG_COMMIT",
)
TERMINAL_ORDER_STATUSES = (
    OrderStatus.REJECTED.value,
    OrderStatus.FILLED.value,
    OrderStatus.CANCELED.value,
    OrderStatus.BROKER_REJECTED.value,
)


#: Allowed values of every enum column as of schema v1. Frozen on purpose: migration 1's checksum is computed from
#: the CHECK text, so these lists must not follow later additions to the message enums (a database created by an
#: earlier build would refuse to open). A new enum value that is stored needs a migration (see v3) and an entry in
#: :func:`enum_values`; ``test_every_stored_enum_value_is_allowed`` fails until it has one.
_ENUMS_V1: Final[dict[str, tuple[str, ...]]] = {
    "AccountState": ("NORMAL", "WARNING", "RESTRICTED", "HALTED"),
    "Action": ("ALERT", "RESTRICT", "CIRCUIT_BREAK", "LIQUIDATE"),
    "CashType": ("DEPOSIT", "WITHDRAW"),
    "ControlOrigin": ("MANUAL", "RULE"),
    "Direction": ("BUY", "SELL"),
    "Exchange": ("SHFE", "DCE", "CFFEX", "INE", "CZCE", "GFEX"),
    "FirmAction": ("KILL_SWITCH_ON", "KILL_SWITCH_OFF", "SET_RESTRICTIONS"),
    "LiquidationStatus": ("PENDING_CONFIRM", "CONFIRMED", "CANCELED", "EXPIRED"),
    "MdQuality": ("OK", "STALE", "UNKNOWN"),
    "Offset": ("OPEN", "CLOSE", "CLOSE_TODAY", "CLOSE_YESTERDAY", "FORCE_CLOSE"),
    "OrderSource": ("BOT", "MANUAL", "API", "LIQUIDATION"),
    "OrderStatus": ("ROUTING", "ROUTED", "ACCEPTED", "PARTIALLY_FILLED", "FILLED", "CANCEL_PENDING", "CANCELED",
                    "REJECTED", "BROKER_REJECTED", "UNKNOWN"),
    "OrderType": ("LIMIT", "MARKET"),
    "PosDirection": ("LONG", "SHORT"),
    "ReconBreakKind": ("POSITION_MISMATCH", "REPLAY_EXPIRED", "ORDER_STATE_CONFLICT"),
    "ReconBreakStatus": ("OPEN", "RESOLVED"),
    "ReconResolution": ("RESYNCED", "RESEEDED", "ACCEPTED"),
    "RiskEventKind": ("ALERT_RAISED", "ALERT_CLEARED", "ALERT_ACKED", "RESET", "MANUAL_HALT", "KILL_SWITCH_ON",
                      "KILL_SWITCH_OFF", "RETRO_BREACH", "LIQUIDATION_PLAN", "RECON_BREAK", "FILL_GAP", "EXTERNAL_FILL",
                      "EOD_FORCE_UNFREEZE"),
    "Role": ("VIEWER", "TRADER", "RISK_MANAGER", "ADMIN"),
    "RuleSmState": ("OK", "PENDING", "ACTIVE", "REARMED"),
    "Severity": ("INFO", "WARNING", "HIGH", "CRITICAL"),
    "TimeInForce": ("GFD", "FAK", "FOK"),
    "TradeOrigin": ("INTERNAL", "EXTERNAL"),
}


def enum_values(version: int) -> dict[str, tuple[str, ...]]:
    """Allowed values of the enum columns (by enum class name) as of schema ``version``."""
    e = dict(_ENUMS_V1)
    if version >= 3:
        e["FirmAction"] = (*e["FirmAction"], "RULE_STATE")  # the coordinator's complete rule-layer state (one fact)
    return e


SCHEMA_VERSION = 3


def db_name(shard: int) -> str:
    """Database name of a shard: ``risk-{shard}`` (file ``<home>/db/risk-{shard}.db``)."""
    return f"risk-{shard}"


def _define(md: MetaData, *, version: int) -> CommonTables:
    """Declare every risk-{shard}.db table on ``md`` as of schema ``version``.

    Version 1 is the 03 §4.3 schema. Later versions add columns or enum values with forward-only
    migrations (03 §9), declared here under ``if version >= N`` / :func:`enum_values` so that
    :data:`METADATA_V1` (the source of migration 1) never changes.
    """
    common = add_common_tables(md, outbox=True, processed_commands=True, fact_cursors=True)
    E = enum_values(version)

    # ------------------------------------------------------------------ SOD copy
    Table(
        "sod_accounts",
        md,
        td_col("trading_day", nullable=False),
        id_col("account_id", nullable=False),
        int_col("sod_version", nullable=False),
        dec_money_col("pre_balance", nullable=False),
        dec_money_col("initial_capital", nullable=False),
        dec_money_col("cum_net_deposit", nullable=False),
        dec_money_col("hwm_equity", nullable=False),
        bool_col("provisional", nullable=False),
        ts_col("loaded_ns", nullable=False),
        PrimaryKeyConstraint("trading_day", "account_id"),
    )

    Table(
        "sod_lots",
        md,
        td_col("trading_day", nullable=False),
        id_col("account_id", nullable=False),
        id_col("instrument_id", nullable=False),
        enum_col("direction", E["PosDirection"], nullable=False),
        int_col("lot_no", nullable=False),
        int_col("sod_version", nullable=False),
        td_col("open_td", nullable=False),
        ts_col("open_ts_ns", nullable=False),
        dec_price_col("open_price", nullable=False),
        int_col("qty", CheckConstraint("qty>0"), nullable=False),
        dec_price_col("basis_price", nullable=False),
        PrimaryKeyConstraint("trading_day", "account_id", "instrument_id", "direction", "lot_no"),
    )

    Table(
        "sod_prices",
        md,
        td_col("trading_day", nullable=False),
        id_col("instrument_id", nullable=False),
        dec_price_col("pre_settle", nullable=False),
        int_col("sod_version", nullable=False),
        PrimaryKeyConstraint("trading_day", "instrument_id"),
    )

    # ------------------------------------------------------------------ orders and fills
    # migration 2: the core's cancel retry bookkeeping, restored after a restart (02 §8.3)
    order_cancel_cols: list[Column[object]] = []
    if version >= 2:
        order_cancel_cols = [
            text_col("cancel_command_id", nullable=True),
            ts_col("cancel_sent_ns", nullable=True),
            int_col("cancel_attempts", nullable=True, server_default=text("0")),
        ]
    orders = Table(
        "orders",
        md,
        uuid_col("client_order_id", primary_key=True),
        id_col("account_id", nullable=False),
        id_col("instrument_id", nullable=False),
        enum_col("exchange", E["Exchange"], nullable=False),
        enum_col("direction", E["Direction"], nullable=False),
        enum_col("offset", E["Offset"], nullable=False),
        enum_col("order_type", E["OrderType"], nullable=False),
        enum_col("tif", E["TimeInForce"], nullable=False),
        dec_price_col("price", nullable=True),
        int_col("qty", CheckConstraint("qty>0"), nullable=False),
        td_col("trading_day", nullable=False),
        enum_col("source", E["OrderSource"], nullable=False),
        text_col("strategy_tag", nullable=True),
        text_col("actor", nullable=True),
        enum_col("status", E["OrderStatus"], nullable=False),
        int_col("filled_qty", nullable=False, server_default=text("0")),
        dec_price_col("avg_fill_price", nullable=True),
        text_col("reject_code", nullable=True),
        json_col("reject_detail", nullable=True),
        text_col("broker_order_ref", nullable=True),
        dec_money_col("frozen_margin", nullable=False, server_default=text("'0'")),
        dec_money_col("frozen_fee", nullable=False, server_default=text("'0'")),
        int_col("risk_check_us", nullable=False),
        hash_col("request_hash", nullable=False),
        hash_col("config_hash", nullable=False),
        ts_col("created_ns", nullable=False),
        ts_col("updated_ns", nullable=False),
        *order_cancel_cols,
    )
    Index("ix_orders__td_acct", orders.c.trading_day, orders.c.account_id)
    Index(
        "ix_orders__open",
        orders.c.status,
        sqlite_where=orders.c.status.not_in(TERMINAL_ORDER_STATUSES),
        postgresql_where=orders.c.status.not_in(TERMINAL_ORDER_STATUSES),
    )

    order_events = Table(
        "order_events",
        md,
        Column("event_id", BIGINT, primary_key=True, autoincrement=True),
        uuid_col("client_order_id", ForeignKey("orders.client_order_id"), nullable=False),
        ts_col("ts_ns", nullable=False),
        enum_col("from_status", E["OrderStatus"], nullable=True),
        enum_col("to_status", E["OrderStatus"], nullable=False),
        text_col("reason", nullable=True),
        json_col("detail", nullable=False, server_default="{}"),
        sqlite_autoincrement=True,
    )
    Index("ix_order_events__coid", order_events.c.client_order_id)

    fills = Table(
        "fills",
        md,
        Column("fill_seq", BIGINT, primary_key=True, autoincrement=True),
        td_col("trading_day", nullable=False),
        enum_col("exchange", E["Exchange"], nullable=False),
        text_col("trade_id", nullable=False),
        enum_col("direction", E["Direction"], nullable=False),
        id_col("account_id", nullable=False),
        id_col("instrument_id", nullable=False),
        uuid_col("client_order_id", ForeignKey("orders.client_order_id"), nullable=True),
        enum_col("origin", E["TradeOrigin"], nullable=False),
        text_col("broker_order_ref", nullable=False),
        enum_col("offset", E["Offset"], nullable=False),
        dec_price_col("price", nullable=False),
        int_col("qty", nullable=False),
        ts_col("trade_ts_ns", nullable=False),
        int_col("trade_seq", nullable=False),
        int_col("broker_seq", nullable=True),
        ts_col("received_ns", nullable=False),
        int_col("close_today_qty", nullable=False, server_default=text("0")),
        int_col("close_yd_qty", nullable=False, server_default=text("0")),
        dec_money_col("fee_est", nullable=False),
        ts_col("applied_ns", nullable=False),
        UniqueConstraint("trading_day", "exchange", "trade_id", "direction", name="uq_fills__key"),
        sqlite_autoincrement=True,
    )
    Index("ix_fills__td_acct", fills.c.trading_day, fills.c.account_id)

    cash_movements = Table(
        "cash_movements",
        md,
        uuid_col("movement_id", primary_key=True),
        id_col("account_id", nullable=False),
        td_col("trading_day", nullable=False),
        enum_col("type", E["CashType"], nullable=False),
        dec_money_col(
            "amount",
            CheckConstraint("amount GLOB '[0-9]*' AND amount NOT GLOB '*[^0-9.]*' AND amount NOT GLOB '*.*.*'"),
            nullable=False,
        ),
        text_col("actor", nullable=False),
        text_col("reason", nullable=False),
        ts_col("ts_ns", nullable=False),
    )
    Index("ix_cash_movements__td_acct", cash_movements.c.trading_day, cash_movements.c.account_id)

    # ------------------------------------------------------------------ rule machines, alerts, events
    Table(
        "rule_states",
        md,
        td_col("trading_day", nullable=False),
        id_col("rule_id", nullable=False),
        id_col("level", nullable=False),
        text_col("scope_key", nullable=False),
        enum_col("state", E["RuleSmState"], nullable=False),
        int_col("episode", nullable=False, server_default=text("0")),
        bool_col("latched", nullable=False),
        int_col("confirm_count", nullable=False, server_default=text("0")),
        ts_col("pending_since_ns", nullable=True),
        ts_col("active_since_ns", nullable=True),
        ts_col("clear_since_ns", nullable=True),
        ts_col("cooldown_until_ns", nullable=True),
        real_col("rearm_threshold", nullable=True),
        real_col("last_value", nullable=True),
        hash_col("config_hash", nullable=False),
        ts_col("updated_ns", nullable=False),
        PrimaryKeyConstraint("trading_day", "rule_id", "level", "scope_key"),
    )

    alerts = Table(
        "alerts",
        md,
        text_col("alert_key", primary_key=True),
        td_col("trading_day", nullable=False),
        id_col("rule_id", nullable=False),
        id_col("level", nullable=False),
        text_col("scope_key", nullable=False),
        int_col("episode", nullable=False),
        id_col("account_id", nullable=True),
        enum_col("action", E["Action"], nullable=False),
        enum_col("severity", E["Severity"], nullable=False),
        text_col("metric", nullable=False),
        real_col("value", nullable=False),
        real_col("threshold", nullable=False),
        bool_col("latched", nullable=False),
        enum_col("status", ALERT_STATUSES, nullable=False),
        ts_col("raised_ns", nullable=False),
        ts_col("cleared_ns", nullable=True),
        ts_col("reset_ns", nullable=True),
        ts_col("acked_ns", nullable=True),
        text_col("reset_by", nullable=True),
        text_col("acked_by", nullable=True),
        text_col("message", nullable=False),
        json_col("detail", nullable=False, server_default="{}"),
    )
    Index("ix_alerts__td", alerts.c.trading_day)

    risk_events = Table(
        "risk_events",
        md,
        uuid_col("event_id", primary_key=True),
        enum_col("kind", E["RiskEventKind"], nullable=False),
        td_col("trading_day", nullable=False),
        ts_col("ts_ns", nullable=False),
        id_col("account_id", nullable=True),
        text_col("scope_key", nullable=True),
        text_col("alert_key", nullable=True),
        id_col("rule_id", nullable=True),
        id_col("level", nullable=True),
        int_col("episode", nullable=True),
        enum_col("action", E["Action"], nullable=True),
        enum_col("severity", E["Severity"], nullable=False),
        bool_col("latched", nullable=True, server_default=None),
        text_col("metric", nullable=True),
        real_col("value", nullable=True),
        real_col("threshold", nullable=True),
        text_col("actor", nullable=True),
        text_col("message", nullable=False),
        json_col("detail", nullable=False, server_default="{}"),
    )
    Index("ix_risk_events__td_kind", risk_events.c.trading_day, risk_events.c.kind)
    Index("ix_risk_events__alert_key", risk_events.c.alert_key)

    Table(
        "retro_breaches",
        md,
        text_col("retro_key", primary_key=True),
        td_col("trading_day", nullable=False),
        id_col("rule_id", nullable=False),
        id_col("level", nullable=False),
        text_col("scope_key", nullable=False),
        text_col("gap_id", nullable=False),
        ts_col("first_breach_ns", nullable=False),
        real_col("worst_value", nullable=False),
        real_col("threshold", nullable=False),
        ts_col("detected_ns", nullable=False),
        json_col("detail", nullable=False),
    )

    # ------------------------------------------------------------------ account state, firm, plans, audit
    Table(
        "account_states",
        md,
        id_col("account_id", primary_key=True),
        td_col("trading_day", nullable=False),
        enum_col("state", E["AccountState"], nullable=False),
        bool_col("halted_latched", nullable=False),
        json_col("restrictions", nullable=False, server_default="[]"),
        text_col("cause", nullable=False),
        text_col("cause_alert_key", nullable=True),
        int_col("state_version", nullable=False),
        ts_col("since_ns", nullable=False),
        ts_col("updated_ns", nullable=False),
    )

    firm_controls = Table(
        "firm_controls",
        md,
        Column("control_id", BIGINT, primary_key=True, autoincrement=True),
        enum_col("action", E["FirmAction"], nullable=False),
        enum_col("origin", E["ControlOrigin"], nullable=False),
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
    Index("ix_firm_controls__origin_action", firm_controls.c.origin, firm_controls.c.action, firm_controls.c.control_id)

    liquidation_plans = Table(
        "liquidation_plans",
        md,
        uuid_col("plan_id", primary_key=True),
        id_col("account_id", nullable=False),
        td_col("trading_day", nullable=False),
        text_col("trigger_alert_key", ForeignKey("alerts.alert_key"), nullable=False),
        enum_col("status", E["LiquidationStatus"], nullable=False),
        json_col("steps", nullable=False),
        ts_col("created_ns", nullable=False),
        text_col("confirmed_by", nullable=True),
        ts_col("confirmed_ns", nullable=True),
        ts_col("finished_ns", nullable=True),
    )
    Index("ix_liquidation_plans__status", liquidation_plans.c.status)

    audit_events = Table(
        "audit_events",
        md,
        Column("audit_id", BIGINT, primary_key=True, autoincrement=True),
        ts_col("ts_ns", nullable=False),
        td_col("trading_day", nullable=False),
        text_col("actor", nullable=False),
        enum_col("role", E["Role"], nullable=True),
        enum_col("action", AUDIT_ACTIONS, nullable=False),
        text_col("target", nullable=False),
        json_col("before", nullable=True),
        json_col("after", nullable=True),
        text_col("reason", nullable=True),
        text_col("command_id", nullable=True),
        text_col("corr_id", nullable=True),
        sqlite_autoincrement=True,
    )
    Index("ix_audit_events__td", audit_events.c.trading_day)

    metric_samples = Table(
        "metric_samples",
        md,
        id_col("account_id", nullable=False),
        ts_col("ts_ns", nullable=False),
        td_col("trading_day", nullable=False),
        real_col("equity", nullable=False),
        real_col("perf_equity", nullable=False),
        real_col("daily_pnl", nullable=False),
        real_col("gross_notional", nullable=False),
        real_col("net_notional", nullable=False),
        json_col("rule_util", nullable=False, server_default="{}"),
        real_col("nav", nullable=False),
        real_col("daily_pnl_pct_init", nullable=False),
        real_col("intraday_dd", nullable=False),
        real_col("hwm_dd", nullable=False),
        real_col("risk_degree", nullable=False),
        real_col("leverage", nullable=False),
        enum_col("state", E["AccountState"], nullable=False),
        enum_col("quality", E["MdQuality"], nullable=False),
        PrimaryKeyConstraint("account_id", "ts_ns"),
    )
    Index("ix_metric_samples__td_acct", metric_samples.c.trading_day, metric_samples.c.account_id)

    Table(
        "eod_states",
        md,
        td_col("trading_day", primary_key=True),
        enum_col("status", RISK_EOD_STATUSES, nullable=False),
        int_col("freeze_version", nullable=True),
        ts_col("frozen_ns", nullable=True),
        json_col("digest", nullable=True),
        ts_col("provisional_ns", nullable=True),
        td_col("next_trading_day", nullable=True),
        int_col("sod_version", nullable=True),
        ts_col("rolled_ns", nullable=True),
        text_col("forced_by", nullable=True),
        text_col("forced_reason", nullable=True),
    )

    recon_breaks = Table(
        "recon_breaks",
        md,
        uuid_col("break_id", primary_key=True),
        td_col("trading_day", nullable=False),
        id_col("account_id", nullable=False),
        enum_col("kind", E["ReconBreakKind"], nullable=False),
        enum_col("status", E["ReconBreakStatus"], nullable=False),
        bool_col("blocking", nullable=False),
        enum_col("resolution", E["ReconResolution"], nullable=True),
        text_col("note", nullable=True),
        json_col("detail", nullable=False),
        ts_col("created_ns", nullable=False),
        ts_col("resolved_ns", nullable=True),
        text_col("resolved_by", nullable=True),
    )
    Index("ix_recon_breaks__open", recon_breaks.c.account_id, recon_breaks.c.status)
    return common


#: schema version 1 (frozen: migration 1 is generated from it, so its DDL must never change)
METADATA_V1 = MetaData()
_define(METADATA_V1, version=1)

#: the current schema
METADATA = MetaData()
COMMON = _define(METADATA, version=SCHEMA_VERSION)

sod_accounts = METADATA.tables["sod_accounts"]
sod_lots = METADATA.tables["sod_lots"]
sod_prices = METADATA.tables["sod_prices"]
orders = METADATA.tables["orders"]
order_events = METADATA.tables["order_events"]
fills = METADATA.tables["fills"]
cash_movements = METADATA.tables["cash_movements"]
rule_states = METADATA.tables["rule_states"]
alerts = METADATA.tables["alerts"]
risk_events = METADATA.tables["risk_events"]
retro_breaches = METADATA.tables["retro_breaches"]
account_states = METADATA.tables["account_states"]
firm_controls = METADATA.tables["firm_controls"]
liquidation_plans = METADATA.tables["liquidation_plans"]
audit_events = METADATA.tables["audit_events"]
metric_samples = METADATA.tables["metric_samples"]
eod_states = METADATA.tables["eod_states"]
recon_breaks = METADATA.tables["recon_breaks"]

#: business tables written by the EngineCore's Persist rows (ENGINE.md §6)
BUSINESS_TABLES: tuple[str, ...] = (
    "sod_accounts",
    "sod_lots",
    "sod_prices",
    "orders",
    "order_events",
    "fills",
    "cash_movements",
    "rule_states",
    "alerts",
    "risk_events",
    "retro_breaches",
    "account_states",
    "firm_controls",
    "liquidation_plans",
    "audit_events",
    "metric_samples",
    "eod_states",
    "recon_breaks",
    "processed_commands",
)

# SQLite cannot alter a CHECK constraint: firm_controls is rebuilt (same columns, rows, control_ids and index)
_V3_SQL: Final = """\
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
FROM firm_controls ORDER BY control_id;
DROP TABLE firm_controls;
ALTER TABLE firm_controls__v3 RENAME TO firm_controls;
CREATE INDEX ix_firm_controls__origin_action ON firm_controls (origin, action, control_id);
"""

#: 03 §9 forward-only migrations of risk-{shard}.db (a new column: bump SCHEMA_VERSION, declare it under
#: ``if version >= N`` in :func:`_define` and append ``Migration.sql_script(N, ..., "ALTER TABLE ... ADD COLUMN ...")``;
#: a new stored enum value: add it to :func:`enum_values` for version N and rebuild the table's CHECK as in v3)
MIGRATIONS: list[Migration] = [
    Migration.create_all(1, "risk-{shard}.db initial schema (03 §4.0, §4.3)", METADATA_V1),
    Migration.sql_script(
        2,
        "orders: cancel retry bookkeeping of the EngineCore (cancel_command_id, cancel_sent_ns, cancel_attempts)",
        "ALTER TABLE orders ADD COLUMN cancel_command_id TEXT;\n"
        "ALTER TABLE orders ADD COLUMN cancel_sent_ns INTEGER;\n"
        "ALTER TABLE orders ADD COLUMN cancel_attempts INTEGER DEFAULT 0;\n",
    ),
    Migration.sql_script(3, "firm_controls.action: RULE_STATE (the coordinator's complete rule-layer state)", _V3_SQL),
]

__all__ = [
    "ALERT_STATUSES",
    "AUDIT_ACTIONS",
    "BUSINESS_TABLES",
    "COMMON",
    "METADATA",
    "METADATA_V1",
    "MIGRATIONS",
    "RISK_EOD_STATUSES",
    "SCHEMA_VERSION",
    "SYNCHRONOUS",
    "TERMINAL_ORDER_STATUSES",
    "account_states",
    "alerts",
    "audit_events",
    "cash_movements",
    "db_name",
    "enum_values",
    "eod_states",
    "fills",
    "firm_controls",
    "liquidation_plans",
    "metric_samples",
    "order_events",
    "orders",
    "recon_breaks",
    "retro_breaches",
    "risk_events",
    "rule_states",
    "sod_accounts",
    "sod_lots",
    "sod_prices",
]
