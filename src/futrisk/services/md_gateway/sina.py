"""Sina futures quotes from ``hq.sinajs.cn``: request, parsing and the decoder (02 §9.3, §7.1).

**Request**: ``GET https://hq.sinajs.cn/list={code},{code},...`` with ``Referer:
https://finance.sina.com.cn`` (403 without it); all codes in one request, polls >= 1 s apart.
**Response**: one line per code, ``var hq_str_{code}="f0,f1,...";`` in GB18030; an empty
string means an unknown or delisted code.

Layouts (0-based), verified on the 2026-09-24 closing snapshot (``tests/fixtures/sina``)
against the official SHFE / CFFEX daily data:

- commodity ``nf_{CODE}``: 0 name, 1 time ``HHMMSS``, 2-4 open/high/low, 5 previous close
  (today's close after the close), 6/7 bid1/ask1, 8 last, 9 dynamic settlement, 10
  pre-settlement, 11/12 bid/ask volume 1, 13 open interest, 14 cumulative volume, 15/16
  exchange short name / product name, 17 natural date, 27 unrounded VWAP. No turnover and no
  limits: ``turnover = avg_price × volume × multiplier``, limits from ``pre_settle``;
- CFFEX ``CFF_RE_{CODE}``: 0-2 open/high/low, 3 last, 4 volume, 5 turnover (multiplier not
  applied), 6 open interest, 9/10 limits, 13 previous close, 14 pre-settlement, 15 previous
  open interest, 16-25 bid1..5 (price, volume), 26-35 ask1..5, 36/37 date / time, 48 VWAP,
  49 name.

Times are Beijing time with one-second resolution (every tick carries ``FLAG_COARSE_TS``);
the date is the natural date, except that some night quotes carry the *trading day* instead
and are re-anchored to the receive date (:func:`resolve_exch_datetime`). The trading day is
recomputed from the calendar by the shared tick builder (:class:`~.normalizer.Normalizer`).
"""

from __future__ import annotations

import math
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Final, Literal

from loguru import logger

from futrisk.common.calendar import EARLY_MORNING_END, NIGHT_START
from futrisk.common.clock import BJ, bj_to_ns, ns_to_bj
from futrisk.common.config.products import ProductsConfig
from futrisk.messages.codes import MD_REJECT_MALFORMED
from futrisk.messages.enums import Exchange, SourceId
from futrisk.messages.md import Tick
from futrisk.services.md_gateway.model import Incoming
from futrisk.services.md_gateway.normalizer import (
    InstrumentMeta,
    NormalizeError,
    Normalizer,
    instrument_meta,
    positive_or_none,
)

SINA_URL: Final = "https://hq.sinajs.cn/list="
SINA_REFERER: Final = "https://finance.sina.com.cn"
SINA_ENCODING: Final = "gb18030"
SINA_USER_AGENT: Final = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) futrisk/0.1"
COMMODITY_PREFIX: Final = "nf_"
CFFEX_PREFIX: Final = "CFF_RE_"
REANCHOR_AHEAD: Final = timedelta(hours=12)  # see resolve_exch_datetime
DEFAULT_FUTURE_TOLERANCE_S: Final = 120.0
COMMODITY_MIN_FIELDS: Final = 18  # through the date (17); the VWAP (27) is optional
CFFEX_MIN_FIELDS: Final = 38  # through the time (37); VWAP (48) and name (49) optional
QuoteKind = Literal["commodity", "cffex"]
#: Sina's one-character exchange names (commodity field 15) and the exchanges they may denote
EXCHANGE_SHORT_NAMES: Final[Mapping[str, frozenset[Exchange]]] = {
    "沪": frozenset({Exchange.SHFE, Exchange.INE}),
    "连": frozenset({Exchange.DCE}),
    "郑": frozenset({Exchange.CZCE}),
    "广": frozenset({Exchange.GFEX}),
}
_CODE_RE = re.compile(r"^[A-Za-z0-9_]+$")
_LINE_RE = re.compile(r'^var\s+hq_str_([A-Za-z0-9_]*)\s*=\s*"([^"]*)"\s*;?\s*$')


# ---------------------------------------------------------------- request helpers
def sina_code_for(instrument_id: str, exchange: Exchange) -> str:
    """Default Sina code of a contract: ``nf_RB2701`` (commodities) or ``CFF_RE_IF2612`` (CFFEX)."""
    return f"{CFFEX_PREFIX if exchange is Exchange.CFFEX else COMMODITY_PREFIX}{instrument_id.upper()}"


def build_url(codes: Sequence[str], base: str = SINA_URL) -> str:
    """Single request URL for all ``codes`` (02 §9.3: one merged request)."""
    if not codes:
        raise ValueError("at least one Sina code is required")
    for c in codes:
        if not _CODE_RE.match(c):
            raise ValueError(f"invalid Sina code {c!r}")
    return base + ",".join(codes)


def request_headers(referer: str = SINA_REFERER) -> dict[str, str]:
    """Headers Sina requires; the ``Referer`` is mandatory (403 without it)."""
    return {"Referer": referer, "User-Agent": SINA_USER_AGENT, "Accept": "*/*"}


def decode_body(body: bytes) -> str:
    """Decode a response body with GB18030 (never raises; undecodable bytes become U+FFFD)."""
    return body.decode(SINA_ENCODING, errors="replace")


@dataclass(frozen=True, slots=True)
class SplitResponse:
    """A response body split into ``code -> payload`` plus lines that did not parse."""

    payloads: dict[str, str]
    junk: tuple[str, ...]


def split_response(text: str) -> SplitResponse:
    """Split ``var hq_str_{code}="...";`` lines; tolerant to CRLF and blank lines."""
    payloads: dict[str, str] = {}
    junk: list[str] = []
    for line in text.splitlines():
        s = line.strip()
        if not s:
            continue
        m = _LINE_RE.match(s)
        if m is None:
            junk.append(s)
        elif m.group(1):
            payloads[m.group(1)] = m.group(2)
        elif m.group(2):
            junk.append(s)
    return SplitResponse(payloads, tuple(junk))


# ---------------------------------------------------------------- quote parsing
class SinaFormatError(ValueError):
    """A payload that cannot be parsed; ``reason`` is an 02 §11.3 code, ``empty`` marks an unknown code."""

    def __init__(self, message: str, reason: str = MD_REJECT_MALFORMED, *, empty: bool = False) -> None:
        super().__init__(message)
        self.reason = reason
        self.empty = empty


@dataclass(frozen=True, slots=True)
class SinaQuote:
    """One parsed Sina quote (raw values, no normalization): prices as sent (``0.0`` = none)."""

    code: str
    kind: QuoteKind
    name: str
    quote_date: date | None
    quote_time: time
    open: float
    high: float
    low: float
    last: float
    bid1: float
    ask1: float
    bid_vol1: int
    ask_vol1: int
    volume: int
    open_interest: int
    pre_settle: float
    pre_close: float
    avg_price: float
    settle_estimate: float | None = None  # commodity 9
    turnover_raw: float | None = None  # CFFEX 5 (multiplier not applied)
    upper_limit: float | None = None  # CFFEX 9
    lower_limit: float | None = None  # CFFEX 10
    pre_open_interest: int | None = None  # CFFEX 15
    exchange_short: str | None = None  # commodity 15
    product_name: str | None = None  # commodity 16
    bids: tuple[tuple[float, int], ...] = ()
    asks: tuple[tuple[float, int], ...] = ()
    fields: tuple[str, ...] = ()


def _field(f: Sequence[str], i: int, name: str) -> str:
    try:
        return f[i].strip()
    except IndexError:
        raise SinaFormatError(f"missing field {i} ({name})") from None


def _num(f: Sequence[str], i: int, name: str, *, default: float | None = None) -> float:
    s = _field(f, i, name) if (default is None or i < len(f)) else ""
    if s == "":
        if default is not None:
            return default
        raise SinaFormatError(f"empty field {i} ({name})")
    try:
        v = float(s)
    except ValueError:
        raise SinaFormatError(f"field {i} ({name}) is not a number: {s!r}") from None
    if not math.isfinite(v):
        raise SinaFormatError(f"field {i} ({name}) is not finite: {s!r}")
    return v


def _int(f: Sequence[str], i: int, name: str, *, default: int | None = None) -> int:
    v = _num(f, i, name, default=None if default is None else float(default))
    if v < 0:
        raise SinaFormatError(f"field {i} ({name}) is negative: {v}")
    return int(round(v))


def parse_time(s: str) -> time:
    """``HHMMSS`` (zero-padded or not) or ``HH:MM:SS[.ffffff]`` -> ``time``."""
    s = s.strip()
    try:
        if ":" in s:
            parts = s.split(":")
            if len(parts) != 3:
                raise ValueError(s)
            sec = float(parts[2])
            whole = int(sec)
            return time(int(parts[0]), int(parts[1]), whole, int(round((sec - whole) * 1_000_000)))
        if not s.isdigit() or len(s) > 6:
            raise ValueError(s)
        s = s.zfill(6)
        return time(int(s[0:2]), int(s[2:4]), int(s[4:6]))
    except ValueError:
        raise SinaFormatError(f"bad time {s!r}") from None


def parse_date(s: str) -> date | None:
    """``YYYY-MM-DD`` or ``YYYYMMDD`` -> ``date``; empty -> ``None``."""
    s = s.strip()
    if not s:
        return None
    try:
        return date(int(s[0:4]), int(s[4:6]), int(s[6:8])) if len(s) == 8 and s.isdigit() else date.fromisoformat(s)
    except ValueError:
        raise SinaFormatError(f"bad date {s!r}") from None


def parse_commodity(code: str, payload: str) -> SinaQuote:
    """Parse an ``nf_`` payload (layout in the module docstring)."""
    f = payload.split(",")
    if len(f) < COMMODITY_MIN_FIELDS:
        raise SinaFormatError(f"commodity payload has {len(f)} fields, expected >= {COMMODITY_MIN_FIELDS}")
    bid1, ask1 = _num(f, 6, "bid1"), _num(f, 7, "ask1")
    bid_vol1, ask_vol1 = _int(f, 11, "bid_vol1"), _int(f, 12, "ask_vol1")
    return SinaQuote(
        code=code, kind="commodity", name=f[0].strip(), quote_date=parse_date(f[17]), quote_time=parse_time(f[1]),
        open=_num(f, 2, "open"), high=_num(f, 3, "high"), low=_num(f, 4, "low"), pre_close=_num(f, 5, "pre_close"),
        bid1=bid1, ask1=ask1, last=_num(f, 8, "last"), settle_estimate=_num(f, 9, "settle_estimate", default=0.0),
        pre_settle=_num(f, 10, "pre_settle"), bid_vol1=bid_vol1, ask_vol1=ask_vol1, open_interest=_int(f, 13, "open_interest"),
        volume=_int(f, 14, "volume"), exchange_short=f[15].strip() or None, product_name=f[16].strip() or None,
        avg_price=_num(f, 27, "avg_price", default=0.0), bids=((bid1, bid_vol1),), asks=((ask1, ask_vol1),), fields=tuple(f),
    )


def parse_cffex(code: str, payload: str) -> SinaQuote:
    """Parse a ``CFF_RE_`` payload (layout in the module docstring)."""
    f = payload.split(",")
    if len(f) < CFFEX_MIN_FIELDS:
        raise SinaFormatError(f"CFFEX payload has {len(f)} fields, expected >= {CFFEX_MIN_FIELDS}")
    bids = tuple((_num(f, 16 + 2 * k, f"bid{k + 1}"), _int(f, 17 + 2 * k, f"bid_vol{k + 1}")) for k in range(5))
    asks = tuple((_num(f, 26 + 2 * k, f"ask{k + 1}"), _int(f, 27 + 2 * k, f"ask_vol{k + 1}")) for k in range(5))
    return SinaQuote(
        code=code, kind="cffex", name=f[49].strip() if len(f) > 49 else "", quote_date=parse_date(f[36]),
        quote_time=parse_time(f[37]), open=_num(f, 0, "open"), high=_num(f, 1, "high"), low=_num(f, 2, "low"),
        last=_num(f, 3, "last"), volume=_int(f, 4, "volume"), turnover_raw=_num(f, 5, "turnover"),
        open_interest=_int(f, 6, "open_interest"), upper_limit=_num(f, 9, "upper_limit"), lower_limit=_num(f, 10, "lower_limit"),
        pre_close=_num(f, 13, "pre_close"), pre_settle=_num(f, 14, "pre_settle"), pre_open_interest=_int(f, 15, "pre_open_interest"),
        bid1=bids[0][0], bid_vol1=bids[0][1], ask1=asks[0][0], ask_vol1=asks[0][1], avg_price=_num(f, 48, "avg_price", default=0.0),
        bids=bids, asks=asks, fields=tuple(f),
    )


def parse_quote(code: str, payload: str) -> SinaQuote:
    """Parse one payload; raises :class:`SinaFormatError` (``empty=True`` for unknown codes)."""
    if not payload.strip():
        raise SinaFormatError("empty payload: unknown or delisted code", empty=True)
    if code.startswith(CFFEX_PREFIX):
        return parse_cffex(code, payload)
    if code.startswith(COMMODITY_PREFIX):
        return parse_commodity(code, payload)
    raise SinaFormatError(f"unsupported Sina code prefix: {code!r}")


def resolve_exch_datetime(quote_date: date | None, quote_time: time, recv_ns: int, *, future_tolerance_s: float = DEFAULT_FUTURE_TOLERANCE_S) -> datetime:
    """Beijing datetime of a quote.

    The quote's own date is trusted (stale snapshots keep their old date) unless it is missing
    or looks like a trading day sent in place of the natural date: a night time of day (>=
    20:00 or < 03:00) more than :data:`REANCHOR_AHEAD` after the receive time. Re-anchoring
    picks the latest of tomorrow / today / yesterday at the quote's time of day that is not more
    than ``future_tolerance_s`` after the receive time.
    """
    recv_dt = ns_to_bj(recv_ns)
    if quote_date is not None:
        dt = datetime.combine(quote_date, quote_time, tzinfo=BJ)
        night = quote_time >= NIGHT_START or quote_time < EARLY_MORNING_END
        if not (night and dt - recv_dt > REANCHOR_AHEAD):
            return dt
    limit = recv_dt + timedelta(seconds=future_tolerance_s)
    base = recv_dt.date()
    return max(c for c in (datetime.combine(base + timedelta(days=k), quote_time, tzinfo=BJ) for k in (1, 0, -1)) if c <= limit)


def resolve_sina_instruments(products: ProductsConfig, sina_symbols: Mapping[str, str], symbols: Iterable[str] | None = None) -> dict[str, InstrumentMeta]:
    """``{sina_code: InstrumentMeta}`` for ``symbols`` (default: every ``feeds.yaml`` sina symbol), in any case.

    Codes come from the ``sina.symbols`` map, falling back to :func:`sina_code_for`; raises
    ``ValueError`` for products missing from ``products.yaml``.
    """
    out: dict[str, InstrumentMeta] = {}
    for raw in list(symbols) if symbols else list(sina_symbols):
        meta = instrument_meta(products, raw)
        out.setdefault(sina_symbols.get(meta.instrument_id) or sina_code_for(meta.instrument_id, meta.exchange), meta)
    return out


# ---------------------------------------------------------------- decoder
class SinaDecoder:
    """Turns one Sina response into :class:`Incoming` records (the body decoder of ``PollTransport``)."""

    source_id = SourceId.SINA

    def __init__(self, normalizer: Normalizer, instruments: Mapping[str, InstrumentMeta]) -> None:
        if not instruments:
            raise ValueError("SinaDecoder needs at least one instrument")
        self._n = normalizer
        self._metas = dict(instruments)
        self._warned: set[tuple[str, str]] = set()

    @property
    def codes(self) -> list[str]:
        return list(self._metas)

    @property
    def instruments(self) -> dict[str, InstrumentMeta]:
        return dict(self._metas)

    def meta_for(self, code: str) -> InstrumentMeta | None:
        return self._metas.get(code)

    def _warn_once(self, code: str, key: str, message: str) -> None:
        if (code, key) not in self._warned:
            self._warned.add((code, key))
            logger.warning(message)

    def decode(self, body: bytes, recv_ns: int, mono: int) -> tuple[list[Incoming], float | None]:
        """One record per configured code, in request order (Sina carries no server time: no lag)."""
        split = split_response(decode_body(body))
        for line in split.junk:
            self._warn_once("", line[:40], f"sina.junk_line {line[:120]!r}")
        return [self.incoming(code, split.payloads.get(code), recv_ns, mono) for code in self._metas], None

    def incoming(self, code: str, payload: str | None, recv_ns: int, mono: int) -> Incoming:
        meta = self._metas.get(code)
        iid = meta.instrument_id if meta is not None else ""
        ex = str(meta.exchange) if meta is not None else None
        if payload is None:
            return Incoming(SourceId.SINA, recv_ns, mono, reject_reason=MD_REJECT_MALFORMED, detail="code missing from response", instrument_id=iid, exchange=ex)
        try:
            quote = parse_quote(code, payload)
            if meta is None:
                raise SinaFormatError("code not configured")
            tick = self.to_tick(quote, meta, recv_ns)
        except SinaFormatError as exc:
            if exc.empty:
                self._warn_once(code, "empty", f"sina.empty_quote code={code}: unknown or delisted contract")
            return Incoming(SourceId.SINA, recv_ns, mono, reject_reason=exc.reason, detail=str(exc), instrument_id=iid, exchange=ex)
        except NormalizeError as exc:
            return Incoming(SourceId.SINA, recv_ns, mono, tick=exc.tick, reject_reason=exc.reason, detail=exc.detail, instrument_id=iid, exchange=ex)
        except Exception as exc:  # defensive: a parser bug must not stop the poll
            logger.opt(exception=True).error(f"sina.parse_bug code={code}")
            return Incoming(SourceId.SINA, recv_ns, mono, reject_reason=MD_REJECT_MALFORMED, detail=f"internal: {exc!r}", instrument_id=iid, exchange=ex)
        return Incoming(SourceId.SINA, recv_ns, mono, tick=tick)

    def to_tick(self, quote: SinaQuote, meta: InstrumentMeta, recv_ns: int) -> Tick:
        """Canonical tick of a parsed quote through the shared builder; raises :class:`NormalizeError`."""
        iid, ex = meta.instrument_id, str(meta.exchange)
        is_cffex = meta.exchange is Exchange.CFFEX
        if (quote.kind == "cffex") != is_cffex:
            raise NormalizeError(MD_REJECT_MALFORMED, f"{quote.kind} quote for {meta.exchange} contract {iid}", instrument_id=iid, exchange=ex)
        if quote.exchange_short:
            allowed = EXCHANGE_SHORT_NAMES.get(quote.exchange_short)
            if allowed is not None and meta.exchange not in allowed:
                raise NormalizeError(MD_REJECT_MALFORMED, f"exchange mismatch: Sina says {quote.exchange_short!r}, config says {meta.exchange}", instrument_id=iid, exchange=ex)
        digits = "".join(ch for ch in iid if ch.isdigit())
        if quote.name and digits and digits not in quote.name:
            self._warn_once(quote.code, "name", f"sina.name_mismatch code={quote.code} name={quote.name!r} instrument={iid}")
        exch_ns = bj_to_ns(resolve_exch_datetime(quote.quote_date, quote.quote_time, recv_ns, future_tolerance_s=self._n.future_tolerance_s))
        upper = lower = None
        if (quote.upper_limit or 0.0) > 0 and (quote.lower_limit or 0.0) > 0:
            upper, lower = quote.upper_limit, quote.lower_limit
        if is_cffex:
            turnover = (quote.turnover_raw or 0.0) * meta.multiplier
            pre_close = positive_or_none(quote.pre_close)
        else:
            avg = positive_or_none(quote.avg_price)
            turnover = avg * quote.volume * meta.multiplier if avg is not None else 0.0
            pre_close = self._commodity_pre_close(quote, meta, exch_ns)
        return self._n.build(
            meta, exch_ns, recv_ns, SourceId.SINA, None, float(quote.last), quote.volume, round(turnover, 2), quote.open_interest,
            positive_or_none(quote.bid1), positive_or_none(quote.ask1), quote.bid_vol1, quote.ask_vol1, positive_or_none(quote.open),
            positive_or_none(quote.high), positive_or_none(quote.low), upper, lower, positive_or_none(quote.pre_settle), pre_close,
            quote.pre_open_interest, positive_or_none(quote.avg_price), "", False, True,
        )

    def _commodity_pre_close(self, quote: SinaQuote, meta: InstrumentMeta, exch_ns: int) -> float | None:
        """Field 5 as ``pre_close`` only inside a session window of the day (ends included) and before the final close."""
        sessions = self._n.sessions
        td = sessions.trading_day(exch_ns)
        windows = sessions.int_windows(meta.sessions, td) if td is not None else ()
        if not windows or exch_ns >= max(w[1] for w in windows):
            return None
        return positive_or_none(quote.pre_close) if any(start <= exch_ns <= end for start, end, _ in windows) else None


__all__ = [
    "DEFAULT_FUTURE_TOLERANCE_S",
    "SINA_REFERER",
    "SINA_URL",
    "SinaDecoder",
    "SinaFormatError",
    "SinaQuote",
    "SplitResponse",
    "build_url",
    "decode_body",
    "parse_quote",
    "parse_time",
    "request_headers",
    "resolve_exch_datetime",
    "resolve_sina_instruments",
    "sina_code_for",
    "split_response",
]
