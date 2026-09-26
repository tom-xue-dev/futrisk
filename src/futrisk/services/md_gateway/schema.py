"""md.db schema (03 §4.1): common tables + feed_cursors, watermarks, md_events, gaps, bars_1m, settle_inputs.

Column names, types, keys and index names follow 03 §4.1 and the logical types of 03 §3.1;
md.db has an outbox (``md.event`` is a FACT) and ``processed_commands``
(``md.source.set``) but no ``fact_cursors`` (md-gateway consumes no FACT stream).

Migrations (03 §9, forward only; the checksums are pinned by the tests):

1. the initial schema (``METADATA_V1``, the 03 §4.1 tables as first released). Its enum
   CHECK lists are **frozen literals** (``_V1_ENUM_VALUES``), not the shared StrEnums: a
   value added to ``SourceId`` / ``MdEventKind`` / ... must neither change this migration's
   checksum (existing databases would refuse to start) nor pass silently - storing it
   needs a new migration that rebuilds the table's CHECK;
2. ``watermarks.wm_fingerprint`` (nullable): the fingerprint of the last LIVE state, so
   that after a restart a copy of that state carrying a later timestamp (a polled source
   serving it again) is not published a second time (01 §8.13 dedupe, L1).
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
    real_col,
    td_col,
    text_col,
    ts_col,
    uuid_col,
)
from futrisk.messages.enums import (
    BarQuality,
    Exchange,
    GapStatus,
    LimitLock,
    MdEventKind,
    SettleInputQuality,
    SettleMethod,
    SourceId,
)

SCHEMA_VERSION = 2

#: enum value lists as of schema version 1, frozen (see the module docstring)
_V1_ENUM_VALUES: dict[type[StrEnum], tuple[str, ...]] = {
    SourceId: ("FEED_A", "FEED_B", "SINA"),
    GapStatus: ("OPEN", "FILLED", "UNRECOVERABLE"),
    MdEventKind: (
        "SOURCE_STATE", "QUALITY_CHANGE", "GAP_OPEN", "GAP_FILLED", "GAP_UNRECOVERABLE", "MD_DOWN", "MD_RECOVERED",
        "MD_QUIET", "MD_INTEGRITY", "MD_FLAPPING", "BAD_TICK", "QUARANTINE_CONFIRMED", "QUARANTINE_DROPPED",
    ),
    BarQuality: ("OK", "GAP_FILLED", "GAP_UNFILLED"),
    Exchange: ("SHFE", "DCE", "CFFEX", "INE", "CZCE", "GFEX"),
    SettleMethod: ("VWAP_FULL_DAY", "VWAP_LAST_HOUR"),
    LimitLock: ("NONE", "UP", "DOWN"),
    SettleInputQuality: ("OK", "APPROX", "NO_TRADE"),
}


def _define(md: MetaData, version: int) -> tuple[CommonTables, dict[str, Table]]:
    """Declare every md.db table on ``md`` as of schema ``version``.

    Every version so far stores exactly the v1 enum values; a migration that widens a CHECK
    adds its own frozen list here.
    """
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
    wm_cols = [
        id_col("instrument_id", primary_key=True),
        td_col("trading_day", nullable=False),
        int_col("wm_volume", nullable=False),
        ts_col("wm_exch_ts_ns", nullable=False),
        ts_col("updated_ns", nullable=False),
    ]
    if version >= 2:
        wm_cols.append(int_col("wm_fingerprint", nullable=True))
    watermarks = Table("watermarks", md, *wm_cols)
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
    bars_1m = Table(
        "bars_1m",
        md,
        id_col("instrument_id", nullable=False),
        ts_col("minute_start_ns", nullable=False),
        td_col("trading_day", nullable=False),
        real_col("open", nullable=False),
        real_col("high", nullable=False),
        real_col("low", nullable=False),
        real_col("close", nullable=False),
        int_col("volume", nullable=False),
        real_col("turnover", nullable=False),
        int_col("open_interest", nullable=False),
        enum_col("quality", ev(BarQuality), nullable=False),
        int_col("version", nullable=False),
        ts_col("updated_ns", nullable=False),
        PrimaryKeyConstraint("instrument_id", "minute_start_ns"),
    )
    Index("ix_bars_1m__td_inst", bars_1m.c.trading_day, bars_1m.c.instrument_id)
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
        "bars_1m": bars_1m,
        "settle_inputs": settle_inputs,
    }
    return common, tables


METADATA_V1 = MetaData()
_define(METADATA_V1, 1)

METADATA = MetaData()
COMMON, _T = _define(METADATA, SCHEMA_VERSION)
feed_cursors = _T["feed_cursors"]
watermarks = _T["watermarks"]
gaps = _T["gaps"]
md_events = _T["md_events"]
bars_1m = _T["bars_1m"]
settle_inputs = _T["settle_inputs"]

MIGRATIONS: list[Migration] = [
    Migration.create_all(1, "md.db initial schema (03 §4.1)", METADATA_V1),
    Migration.sql_script(
        2,
        "watermarks: fingerprint of the last LIVE state (restart dedupe)",
        "ALTER TABLE watermarks ADD COLUMN wm_fingerprint INTEGER;\n",
    ),
]

__all__ = [
    "COMMON",
    "METADATA",
    "METADATA_V1",
    "MIGRATIONS",
    "SCHEMA_VERSION",
    "bars_1m",
    "feed_cursors",
    "gaps",
    "md_events",
    "settle_inputs",
    "watermarks",
]
