"""md.db schema (03 §4.1): common tables + feed_cursors, watermarks, md_events, gaps, settle_inputs.

Column names, types, keys and index names follow 03 §4.1 and the logical types of 03 §3.1;
md.db has an outbox (``md.event`` is a FACT) and ``processed_commands`` (``md.source.set``)
but no ``fact_cursors`` (md-gateway consumes no FACT stream).

One migration (03 §9, forward only; its checksum is pinned by the tests). The enum CHECK
lists are **frozen literals** (``_V1_ENUM_VALUES``), not the shared StrEnums: a value added
to ``SourceId`` / ``MdEventKind`` / ... must neither change this migration's checksum
(existing databases would refuse to start) nor pass silently - storing it needs a new
migration that rebuilds the table's CHECK.
"""

from __future__ import annotations

from enum import StrEnum

from sqlalchemy import ForeignKey, Index, MetaData, PrimaryKeyConstraint, Table, text

from futrisk.db.common_tables import CommonTables, add_common_tables
from futrisk.db.migrations import Migration
from futrisk.db.types import (
    dec_amount_col,
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
    Exchange,
    GapStatus,
    LimitLock,
    MdEventKind,
    SettleInputQuality,
    SettleMethod,
    SourceId,
)

SCHEMA_VERSION = 1

#: enum value lists as of schema version 1, frozen (see the module docstring)
_V1_ENUM_VALUES: dict[type[StrEnum], tuple[str, ...]] = {
    SourceId: ("FEED_A", "FEED_B", "SINA"),
    GapStatus: ("OPEN", "FILLED", "UNRECOVERABLE"),
    MdEventKind: (
        "SOURCE_STATE", "QUALITY_CHANGE", "GAP_OPEN", "GAP_FILLED", "GAP_UNRECOVERABLE", "MD_DOWN", "MD_RECOVERED",
        "MD_QUIET", "MD_INTEGRITY", "BAD_TICK",
    ),
    Exchange: ("SHFE", "DCE", "CFFEX", "INE", "CZCE", "GFEX"),
    SettleMethod: ("VWAP_FULL_DAY", "VWAP_LAST_HOUR"),
    LimitLock: ("NONE", "UP", "DOWN"),
    SettleInputQuality: ("OK", "APPROX", "NO_TRADE"),
}


def _define(md: MetaData) -> tuple[CommonTables, dict[str, Table]]:
    """Declare every md.db table on ``md``."""
    common = add_common_tables(md, outbox=True, processed_commands=True, fact_cursors=False)

    def ev(enum: type[StrEnum]) -> tuple[str, ...]:
        return _V1_ENUM_VALUES[enum]

    feed_cursors = Table(
        "feed_cursors",
        md,
        enum_col("source", ev(SourceId), primary_key=True),
        text_col("epoch", nullable=True),
        int_col("last_contiguous_seq", nullable=True),
        ts_col("last_exch_ts_ns", nullable=True),
        td_col("trading_day", nullable=False),
        ts_col("updated_ns", nullable=False),
    )
    watermarks = Table(
        "watermarks",
        md,
        id_col("instrument_id", primary_key=True),
        td_col("trading_day", nullable=False),
        int_col("wm_volume", nullable=False),
        ts_col("wm_exch_ts_ns", nullable=False),
        ts_col("updated_ns", nullable=False),
        int_col("wm_fingerprint", nullable=True),
    )
    gaps = Table(
        "gaps",
        md,
        text_col("gap_id", primary_key=True),
        enum_col("source", ev(SourceId), nullable=True),
        text_col("epoch", nullable=True),
        int_col("from_seq", nullable=True),
        int_col("to_seq", nullable=True),
        ts_col("from_ns", nullable=False),
        ts_col("to_ns", nullable=True),
        td_col("trading_day", nullable=False),
        enum_col("status", ev(GapStatus), nullable=False),
        int_col("filled", nullable=False, server_default=text("0")),
        int_col("duplicates", nullable=False, server_default=text("0")),
        int_col("unrecoverable", nullable=False, server_default=text("0")),
        ts_col("opened_ns", nullable=False),
        ts_col("closed_ns", nullable=True),
    )
    md_events = Table(
        "md_events",
        md,
        uuid_col("event_id", primary_key=True),
        enum_col("kind", ev(MdEventKind), nullable=False),
        td_col("trading_day", nullable=False),
        ts_col("ts_ns", nullable=False),
        enum_col("source", ev(SourceId), nullable=True),
        id_col("instrument_id", nullable=True),
        text_col("from_state", nullable=True),
        text_col("to_state", nullable=True),
        text_col("gap_id", ForeignKey("gaps.gap_id"), nullable=True),
        text_col("actor", nullable=True),
        json_col("detail", nullable=False, server_default=text("'{}'")),
    )
    Index("ix_md_events__td_kind", md_events.c.trading_day, md_events.c.kind)
    Index("ix_md_events__inst_ts", md_events.c.instrument_id, md_events.c.ts_ns)
    settle_inputs = Table(
        "settle_inputs",
        md,
        td_col("trading_day", nullable=False),
        id_col("instrument_id", nullable=False),
        int_col("version", nullable=False),
        enum_col("exchange", ev(Exchange), nullable=False),
        enum_col("method", ev(SettleMethod), nullable=False),
        int_col("multiplier", nullable=False),
        dec_price_col("tick_size", nullable=False),
        int_col("day_volume", nullable=False),
        dec_amount_col("day_turnover", nullable=False),
        ts_col("window_start_ns", nullable=True),
        int_col("window_volume", nullable=True),
        dec_amount_col("window_turnover", nullable=True),
        json_col("hourly", nullable=True),
        dec_price_col("vwap_raw", nullable=True),
        dec_price_col("last", nullable=True),
        dec_price_col("bid1", nullable=True),
        dec_price_col("ask1", nullable=True),
        enum_col("limit_lock", ev(LimitLock), nullable=False),
        dec_price_col("pre_settle", nullable=False),
        enum_col("quality", ev(SettleInputQuality), nullable=False),
        ts_col("computed_ns", nullable=False),
        PrimaryKeyConstraint("trading_day", "instrument_id", "version"),
    )
    tables = {
        "feed_cursors": feed_cursors,
        "watermarks": watermarks,
        "gaps": gaps,
        "md_events": md_events,
        "settle_inputs": settle_inputs,
    }
    return common, tables


METADATA = MetaData()
COMMON, _T = _define(METADATA)
feed_cursors = _T["feed_cursors"]
watermarks = _T["watermarks"]
gaps = _T["gaps"]
md_events = _T["md_events"]
settle_inputs = _T["settle_inputs"]

MIGRATIONS: list[Migration] = [Migration.create_all(1, "md.db initial schema (03 §4.1)", METADATA)]

__all__ = [
    "COMMON",
    "METADATA",
    "MIGRATIONS",
    "SCHEMA_VERSION",
    "feed_cursors",
    "gaps",
    "md_events",
    "settle_inputs",
    "watermarks",
]
