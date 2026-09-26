"""Arrow schemas of the tick files and row builders (03 §5.1, 02 §7.1).

**Raw tick shards** (``ticks/raw/{trading_day}/{source}/part-{epoch_ms}-{n}.parquet``):
one row per received, normalized state -- including rejected, duplicate,
quarantined and backfilled ones. Column names equal the ``Tick`` fields plus
``epoch``, ``mode`` and ``reject_reason``; low-cardinality strings are
dictionary encoded. Rows that could not be parsed at all (``mode=REJECTED``,
``reject_reason=MD_REJECT_MALFORMED``) carry only the identifying columns;
their market columns are null.

**Wire shards** (``ticks/wire/{trading_day}/{source}/wire-{epoch_ms}-{n}.parquet``,
an addition to 03 §5.1): one row per HTTP poll with the untouched response
bytes, so tick shards can be rebuilt if the parser is ever corrected.
"""

from __future__ import annotations

from typing import Any, Final

import pyarrow as pa

from futrisk.messages.enums import TickMode
from futrisk.messages.md import Tick

SCHEMA_VERSION: Final = "1"
_DICT = pa.dictionary(pa.int32(), pa.string())

TICK_SCHEMA: Final = pa.schema(
    [
        pa.field("instrument_id", _DICT),
        pa.field("exchange", _DICT),
        pa.field("trading_day", _DICT),
        pa.field("action_day", _DICT),
        pa.field("exch_ts_ns", pa.int64()),
        pa.field("recv_ns", pa.int64()),
        pa.field("last", pa.float64()),
        pa.field("bid1", pa.float64()),
        pa.field("ask1", pa.float64()),
        pa.field("open", pa.float64()),
        pa.field("high", pa.float64()),
        pa.field("low", pa.float64()),
        pa.field("upper_limit", pa.float64()),
        pa.field("lower_limit", pa.float64()),
        pa.field("pre_settle", pa.float64()),
        pa.field("pre_close", pa.float64()),
        pa.field("avg_price", pa.float64()),
        pa.field("turnover", pa.float64()),
        pa.field("volume", pa.int64()),
        pa.field("open_interest", pa.int64()),
        pa.field("bid_vol1", pa.int64()),
        pa.field("ask_vol1", pa.int64()),
        pa.field("pre_open_interest", pa.int64()),
        pa.field("source", _DICT),
        pa.field("source_seq", pa.int64()),
        pa.field("epoch", pa.string()),
        pa.field("fingerprint", pa.uint64()),
        pa.field("flags", pa.int32()),
        pa.field("mode", _DICT),
        pa.field("reject_reason", pa.string()),
    ],
    metadata={"futrisk.kind": "ticks.raw", "futrisk.schema_version": SCHEMA_VERSION},
)
TICK_COLUMNS: Final[tuple[str, ...]] = tuple(TICK_SCHEMA.names)

WIRE_SCHEMA: Final = pa.schema(
    [
        pa.field("recv_ns", pa.int64()),
        pa.field("sent_ns", pa.int64()),
        pa.field("poll_seq", pa.int64()),
        pa.field("elapsed_ms", pa.float64()),
        pa.field("http_status", pa.int32()),
        pa.field("error", pa.string()),
        pa.field("codes", _DICT),
        pa.field("body", pa.binary()),
    ],
    metadata={"futrisk.kind": "ticks.wire", "futrisk.schema_version": SCHEMA_VERSION},
)
WIRE_COLUMNS: Final[tuple[str, ...]] = tuple(WIRE_SCHEMA.names)


def tick_row(
    tick: Tick, mode: TickMode, reject_reason: str | None = None, *, epoch: str | None = None
) -> dict[str, Any]:
    """Row dict for a normalized :class:`Tick` with its processing result."""
    return {
        "instrument_id": tick.instrument_id,
        "exchange": str(tick.exchange),
        "trading_day": tick.trading_day,
        "action_day": tick.action_day,
        "exch_ts_ns": tick.exch_ts_ns,
        "recv_ns": tick.recv_ns,
        "last": tick.last,
        "bid1": tick.bid1,
        "ask1": tick.ask1,
        "open": tick.open,
        "high": tick.high,
        "low": tick.low,
        "upper_limit": tick.upper_limit,
        "lower_limit": tick.lower_limit,
        "pre_settle": tick.pre_settle,
        "pre_close": tick.pre_close,
        "avg_price": tick.avg_price,
        "turnover": tick.turnover,
        "volume": tick.volume,
        "open_interest": tick.open_interest,
        "bid_vol1": tick.bid_vol1,
        "ask_vol1": tick.ask_vol1,
        "pre_open_interest": tick.pre_open_interest,
        "source": str(tick.source),
        "source_seq": tick.source_seq,
        "epoch": epoch,
        "fingerprint": tick.fingerprint,
        "flags": tick.flags,
        "mode": str(mode),
        "reject_reason": reject_reason,
    }


def reject_row(
    *,
    instrument_id: str,
    exchange: str | None,
    trading_day: str,
    action_day: str,
    recv_ns: int,
    source: str,
    reject_reason: str,
) -> dict[str, Any]:
    """Row for a state that could not be normalized: identifiers only, market columns null."""
    row: dict[str, Any] = dict.fromkeys(TICK_COLUMNS)
    row.update(
        instrument_id=instrument_id,
        exchange=exchange,
        trading_day=trading_day,
        action_day=action_day,
        recv_ns=recv_ns,
        source=source,
        mode=str(TickMode.REJECTED),
        reject_reason=reject_reason,
    )
    return row


def wire_row(
    *,
    recv_ns: int,
    sent_ns: int,
    poll_seq: int,
    elapsed_ms: float,
    http_status: int | None,
    error: str | None,
    codes: str,
    body: bytes | None,
) -> dict[str, Any]:
    return {
        "recv_ns": recv_ns,
        "sent_ns": sent_ns,
        "poll_seq": poll_seq,
        "elapsed_ms": elapsed_ms,
        "http_status": http_status,
        "error": error,
        "codes": codes,
        "body": body,
    }
