"""Typed vendor messages (02 §9.1, §9.2) for the decode fast path.

Feed A frames are decoded straight into one tagged union on ``op``, several times faster than
building dictionaries; a frame that does not fit (an unknown ``op``, a field of the wrong type
such as a ``MALFORMED`` fault's ``"LastPrice": "N/A"``) is handled by the generic ``dict`` path
of the client. Feed B records are converted into :class:`VendorQuote` one by one (strings are
coerced, ``strict=False``), so one malformed record only rejects itself.
"""

from __future__ import annotations

import msgspec


class CtpTick(msgspec.Struct, frozen=True):
    """``FeedATick`` with the CTP ``CThostFtdcDepthMarketDataField`` names (02 §9.1)."""

    InstrumentID: str
    ActionDay: str
    UpdateTime: str
    LastPrice: float
    Volume: int
    Turnover: float
    OpenInterest: float
    ExchangeID: str | None = None
    TradingDay: str | None = None
    UpdateMillisec: int = 0
    BidPrice1: float | None = None
    BidVolume1: int = 0
    AskPrice1: float | None = None
    AskVolume1: int = 0
    OpenPrice: float | None = None
    HighestPrice: float | None = None
    LowestPrice: float | None = None
    UpperLimitPrice: float | None = None
    LowerLimitPrice: float | None = None
    PreSettlementPrice: float | None = None
    PreClosePrice: float | None = None
    PreOpenInterest: float | None = None
    AveragePrice: float | None = None


class VendorQuote(msgspec.Struct, frozen=True):
    """One record of Feed B's ``GET /v1/quotes`` (02 §9.2); prices arrive as decimal strings."""

    sym: str
    t: int
    px: float
    vol: int
    amt: float
    oi: float
    ex: str | None = None
    bp: float | None = None
    ap: float | None = None
    bv: int = 0
    av: int = 0
    o: float | None = None
    h: float | None = None
    l: float | None = None  # noqa: E741 - vendor field name
    up: float | None = None
    dn: float | None = None
    ps: float | None = None
    pc: float | None = None


class TickFrame(msgspec.Struct, tag="tick", tag_field="op"):
    seq: int
    d: CtpTick
    replay: bool = False


class HeartbeatFrame(msgspec.Struct, tag="heartbeat", tag_field="op"):
    ts: int | None = None
    head_seq: int | None = None


class WelcomeFrame(msgspec.Struct, tag="welcome", tag_field="op"):
    epoch: str = ""
    head_seq: int = 0
    ring_low: int = 1
    hb_ms: int | None = None
    req: int | None = None
    trading_day: str | None = None


class SnapshotFrame(msgspec.Struct, tag="snapshot", tag_field="op"):
    ticks: list[CtpTick] = []
    as_of_seq: int | None = None


class ReplayBeginFrame(msgspec.Struct, tag="replay_begin", tag_field="op"):
    req: int | None = None
    from_seq: int = 0
    to_seq: int = 0


class ReplayEndFrame(msgspec.Struct, tag="replay_end", tag_field="op"):
    req: int | None = None
    to_seq: int = 0


class ResumeRejectedFrame(msgspec.Struct, tag="resume_rejected", tag_field="op"):
    reason: str = ""
    ring_low: int = 1
    head_seq: int = 0
    req: int | None = None


class ErrorFrame(msgspec.Struct, tag="error", tag_field="op"):
    code: str = ""
    req: int | None = None
    message: str | None = None
    ring_low: int | None = None
    head_seq: int | None = None


Frame = TickFrame | HeartbeatFrame | WelcomeFrame | SnapshotFrame | ReplayBeginFrame | ReplayEndFrame | ResumeRejectedFrame | ErrorFrame

decode_frame = msgspec.json.Decoder(Frame).decode

__all__ = [
    "CtpTick",
    "ErrorFrame",
    "Frame",
    "HeartbeatFrame",
    "ReplayBeginFrame",
    "ReplayEndFrame",
    "ResumeRejectedFrame",
    "SnapshotFrame",
    "TickFrame",
    "VendorQuote",
    "WelcomeFrame",
    "decode_frame",
]
