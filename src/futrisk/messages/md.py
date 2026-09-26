"""Market-data payloads (02 §6.2, §7.1)."""

from __future__ import annotations

from typing import Any

import msgspec

from futrisk.messages.base import Msg, payload
from futrisk.messages.enums import (
    BarQuality,
    Exchange,
    GapStatus,
    LimitLock,
    MdEventKind,
    MdQuality,
    SettleInputQuality,
    SettleMethod,
    SourceId,
    SourceState,
    TickMode,
)

# Tick.flags bits
FLAG_COARSE_TS = 1 << 0
FLAG_AUCTION = 1 << 1
FLAG_LIMIT_UP_LOCKED = 1 << 2
FLAG_LIMIT_DOWN_LOCKED = 1 << 3
#: md-gateway rolled its watermark back (``MD_INTEGRITY`` ``action=WM_ROLLBACK``): this LIVE tick may carry an
#: ``order_key`` not greater than the previous one and must be applied anyway (consumers reset their ordering)
FLAG_WM_RESET = 1 << 4


@payload()
class Tick(Msg, kw_only=True):
    instrument_id: str
    exchange: Exchange
    trading_day: str
    action_day: str
    exch_ts_ns: int
    last: float
    volume: int
    turnover: float
    open_interest: int
    bid1: float | None = None
    ask1: float | None = None
    bid_vol1: int = 0
    ask_vol1: int = 0
    open: float | None = None
    high: float | None = None
    low: float | None = None
    upper_limit: float
    lower_limit: float
    pre_settle: float
    pre_close: float | None = None
    pre_open_interest: int | None = None
    avg_price: float | None = None
    source: SourceId
    source_seq: int | None = None
    recv_ns: int
    fingerprint: int = 0
    flags: int = 0

    @property
    def order_key(self) -> tuple[str, int, int]:
        """(trading_day, volume, exch_ts_ns): cross-source ordering key (01 §8.13)."""
        return (self.trading_day, self.volume, self.exch_ts_ns)


@payload()
class Bar(Msg, kw_only=True):
    instrument_id: str
    trading_day: str
    minute_start_ns: int
    open: float
    high: float
    low: float
    close: float
    volume: int
    turnover: float
    open_interest: int
    quality: BarQuality = BarQuality.OK
    version: int = 1


class SourceStatus(msgspec.Struct, kw_only=True, omit_defaults=True):
    source: SourceId
    state: SourceState
    since_ns: int
    connected: bool
    epoch: str | None = None
    last_seq: int | None = None
    last_msg_ns: int | None = None
    lag_ms: float | None = None
    reject_rate: float = 0.0
    msgs_per_s: float = 0.0
    integrity_votes: int = 0


class InstrumentStatus(msgspec.Struct, kw_only=True, omit_defaults=True):
    instrument_id: str
    quality: MdQuality
    live_source: SourceId | None = None
    wm_volume: int = 0
    wm_exch_ts_ns: int = 0
    last_live_ns: int | None = None
    quiet: bool = False
    limit_lock: LimitLock = LimitLock.NONE


@payload()
class MdStatus(Msg, kw_only=True):
    as_of_ns: int
    trading_day: str
    sources: list[SourceStatus]
    instruments: list[InstrumentStatus]


class GapInfo(msgspec.Struct, kw_only=True, omit_defaults=True):
    gap_id: str
    source: SourceId | None = None
    epoch: str | None = None
    from_seq: int | None = None
    to_seq: int | None = None
    from_ns: int
    to_ns: int | None = None
    status: GapStatus = GapStatus.OPEN
    filled: int = 0
    duplicates: int = 0
    unrecoverable: int = 0


@payload()
class MdEvent(Msg, kw_only=True):
    event_id: str
    kind: MdEventKind
    ts_ns: int
    trading_day: str
    source: SourceId | None = None
    instrument_id: str | None = None
    from_state: str | None = None
    to_state: str | None = None
    gap: GapInfo | None = None
    detail: dict[str, Any] = {}


@payload()
class SettleInput(Msg, kw_only=True):
    trading_day: str
    instrument_id: str
    exchange: Exchange
    method: SettleMethod
    multiplier: int
    tick_size: str
    day_volume: int
    day_turnover: str
    window_start_ns: int | None = None
    window_volume: int | None = None
    window_turnover: str | None = None
    hourly: dict[str, list[str]] | None = None  # CFFEX: {hour_start_ns: [volume, turnover]} (03 §4.1)
    vwap_raw: str | None = None
    last: str | None = None
    bid1: str | None = None
    ask1: str | None = None
    limit_lock: LimitLock = LimitLock.NONE
    pre_settle: str
    quality: SettleInputQuality = SettleInputQuality.OK
    version: int = 1
    computed_ns: int


# ---------------------------------------------------------------- commands
@payload()
class SettleInputsRequest(Msg, kw_only=True):
    trading_day: str
    instruments: list[str] | None = None


@payload()
class SettleInputsBatch(Msg, kw_only=True):
    items: list[SettleInput]


@payload()
class MdEventsQuery(Msg, kw_only=True):
    trading_day: str
    kinds: list[MdEventKind] | None = None
    instrument_id: str | None = None


@payload()
class MdEventBatch(Msg, kw_only=True):
    items: list[MdEvent]


@payload()
class TicksQuery(Msg, kw_only=True):
    instrument_id: str
    trading_day: str
    from_ns: int
    to_ns: int
    modes: list[TickMode] | None = None


@payload()
class TickBatch(Msg, kw_only=True):
    ticks: list[Tick]
    next_from_ns: int | None = None


@payload()
class BarsRequest(Msg, kw_only=True):
    instrument_id: str
    trading_day: str
    from_ns: int | None = None
    to_ns: int | None = None


@payload()
class BarsBatch(Msg, kw_only=True):
    bars: list[Bar]


@payload()
class SourcesRequest(Msg, kw_only=True):
    pass


@payload()
class SourceControl(Msg, kw_only=True):
    command_id: str
    source: SourceId
    action: str  # "EXCLUDE" | "INCLUDE"
    reason: str
    actor: str
