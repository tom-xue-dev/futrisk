"""Sina futures quotes from ``hq.sinajs.cn``: request, decoding, parsing, normalization (02 §9.3, §7.1).

**Request**: ``GET https://hq.sinajs.cn/list={code},{code},...`` with the header
``Referer: https://finance.sina.com.cn``; without it Sina answers
``403 Forbidden``. All codes go into one request; polls must be >= 1s apart.

**Response**: one line per requested code, ``var hq_str_{code}="f0,f1,...";``,
encoded in GB18030 (a GBK superset). An empty string means the code is unknown
or the contract has been delisted; an empty ``list=`` yields an empty body.

**Field layouts** (0-based). Verified on the 2026-09-24 closing snapshot
(``tests/fixtures/sina``) against the official SHFE and CFFEX daily data:

Commodity futures, code ``nf_{CODE}`` (e.g. ``nf_RB2701``):

====  =====================================================  ======================
idx   meaning                                                -> Tick
====  =====================================================  ======================
0     contract name (GBK), e.g. ``螺纹钢2701``                 name check only
1     time ``HHMMSS`` (Beijing)                               exch_ts_ns (with 17)
2-4   open / high / low                                       open / high / low
5     previous close; equals today's close after the close   pre_close (in session only)
6/7   bid1 / ask1                                             bid1 / ask1
8     last                                                    last
9     dynamic settlement price (VWAP floored to the tick)     --
10    pre-settlement price                                    pre_settle
11/12 bid volume 1 / ask volume 1                             bid_vol1 / ask_vol1
13    open interest                                           open_interest
14    cumulative volume                                       volume
15/16 exchange short name (沪/连/郑) / product name           exchange check
17    natural date ``YYYY-MM-DD``                             action_day
27    unrounded VWAP                                          avg_price
====  =====================================================  ======================

There is no turnover and no limit price: ``turnover = avg_price * volume *
multiplier`` (within 1e-6 of the official figure on the sample) and the limits
come from ``pre_settle`` with inward rounding.

CFFEX futures, code ``CFF_RE_{CODE}`` (e.g. ``CFF_RE_IF2612``):

====  =====================================================  ======================
idx   meaning                                                -> Tick
====  =====================================================  ======================
0-2   open / high / low                                       open / high / low
3     last                                                    last
4     cumulative volume                                       volume
5     turnover = sum(price * lots), multiplier NOT applied   turnover (* multiplier)
6     open interest                                           open_interest
9/10  upper / lower limit                                     upper_limit / lower_limit
13    previous close                                          pre_close
14    pre-settlement price                                    pre_settle
15    previous open interest                                  pre_open_interest
16-25 bid1..bid5 as (price, volume) pairs                     bid1 / bid_vol1
26-35 ask1..ask5 as (price, volume) pairs                     ask1 / ask_vol1
36/37 natural date ``YYYY-MM-DD`` / time ``HH:MM:SS``          action_day / exch_ts_ns
48    unrounded VWAP                                          avg_price
49    contract name (GBK), e.g. ``沪深300指数期货2612``          name check only
====  =====================================================  ======================

Evidence for fields 13/14: the official CFFEX data for 2026-09-23 has IF2612
close 4461.8 (field 13) and settlement 4462.2 (field 14), and the limit prices
(fields 9/10) equal ``field14 * (1 ± limit)`` rounded inward for IF2612,
IC2612, TS2612 and the expired IF2609 (±20% on its last day).

**Time**: Sina times are Beijing time with one-second resolution, so every
tick carries ``FLAG_COARSE_TS``. The date is the natural date; the trading day
is recomputed from the calendar (night sessions belong to the next trading
day). Some feeds put the *trading day* into the date field during night
sessions (DCE via CTP does); such a quote carries a night time of day and a
date 1-3 days after the receive date, and only then is its time of day
re-anchored to the receive date (see :func:`resolve_exch_datetime`).

**Future timestamps**: any other quote stamped more than
``future_tolerance_s`` (default 120 s) after its receive time must not advance
LIVE; the normalizer still builds its ``Tick`` (so the recorder keeps every
field) but rejects it with ``MD_REJECT_FUTURE_TS`` (detail ``future_ts``).
If every symbol shows ``future_ts``, the local clock is slow (``w32tm /resync``).
"""

from __future__ import annotations

import math
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Final, Literal

from loguru import logger

from futrisk.common.calendar import EARLY_MORNING_END, NIGHT_START, TradingCalendar
from futrisk.common.clock import BJ, NS_PER_S, bj_to_ns, ns_to_bj, td_str
from futrisk.common.config.products import ProductsConfig
from futrisk.marketdata.fingerprint import compute_fingerprint
from futrisk.marketdata.normalize import (
    InstrumentMeta,
    derive_limits,
    instrument_meta,
    limit_lock_flags,
    positive_or_none,
)
from futrisk.marketdata.sessions import WindowCache
from futrisk.messages.codes import MD_REJECT_FUTURE_TS, MD_REJECT_MALFORMED, MD_REJECT_TRADING_DAY
from futrisk.messages.enums import Exchange, SourceId
from futrisk.messages.md import FLAG_AUCTION, FLAG_COARSE_TS, Tick

SINA_URL: Final = "https://hq.sinajs.cn/list="
SINA_REFERER: Final = "https://finance.sina.com.cn"
SINA_ENCODING: Final = "gb18030"
SINA_USER_AGENT: Final = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) futrisk/0.1"
COMMODITY_PREFIX: Final = "nf_"
CFFEX_PREFIX: Final = "CFF_RE_"

REANCHOR_AHEAD: Final = timedelta(hours=12)  # see resolve_exch_datetime
DEFAULT_FUTURE_TOLERANCE_S: Final = 120.0
FUTURE_TS: Final = "future_ts"  # SinaResult.detail prefix for quotes stamped in the future
COMMODITY_MIN_FIELDS: Final = 18  # through the date (17); the VWAP (27) is optional
CFFEX_MIN_FIELDS: Final = 38  # through the time (37); VWAP (48) and name (49) optional

QuoteKind = Literal["commodity", "cffex"]

# Sina's one-character exchange names (commodity field 15) and the exchanges they may denote.
EXCHANGE_SHORT_NAMES: Final[Mapping[str, frozenset[Exchange]]] = {
    "沪": frozenset({Exchange.SHFE, Exchange.INE}),
    "连": frozenset({Exchange.DCE}),
    "郑": frozenset({Exchange.CZCE}),
    "广": frozenset({Exchange.GFEX}),
}

_CODE_RE = re.compile(r"^[A-Za-z0-9_]+$")
_LINE_RE = re.compile(r'^var\s+hq_str_([A-Za-z0-9_]*)\s*=\s*"([^"]*)"\s*;?\s*$')


# ---------------------------------------------------------------------------
# Request helpers
# ---------------------------------------------------------------------------


def sina_code_for(instrument_id: str, exchange: Exchange) -> str:
    """Default Sina code of a contract: ``nf_RB2701`` (commodities) or ``CFF_RE_IF2612`` (CFFEX)."""
    if exchange is Exchange.CFFEX:
        return f"{CFFEX_PREFIX}{instrument_id.upper()}"
    return f"{COMMODITY_PREFIX}{instrument_id.upper()}"


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
            continue
        code, payload = m.group(1), m.group(2)
        if code:
            payloads[code] = payload
        elif payload:
            junk.append(s)
    return SplitResponse(payloads, tuple(junk))


# ---------------------------------------------------------------------------
# Quote parsing
# ---------------------------------------------------------------------------


class SinaFormatError(ValueError):
    """A quote that cannot be turned into a :class:`Tick`; ``reason`` is an 02 §11.3 code."""

    def __init__(self, message: str, reason: str = MD_REJECT_MALFORMED, *, empty: bool = False) -> None:
        super().__init__(message)
        self.reason = reason
        self.empty = empty


@dataclass(frozen=True, slots=True)
class SinaQuote:
    """One parsed Sina quote with every field the two layouts carry (raw values, no normalization).

    Prices are as sent (``0.0`` means "none"); fields that one layout lacks are ``None``.
    ``fields`` keeps the raw comma-separated values for audit and re-parsing.
    """

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
        if len(s) == 8 and s.isdigit():
            return date(int(s[0:4]), int(s[4:6]), int(s[6:8]))
        return date.fromisoformat(s)
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
        code=code,
        kind="commodity",
        name=f[0].strip(),
        quote_date=parse_date(f[17]),
        quote_time=parse_time(f[1]),
        open=_num(f, 2, "open"),
        high=_num(f, 3, "high"),
        low=_num(f, 4, "low"),
        pre_close=_num(f, 5, "pre_close"),
        bid1=bid1,
        ask1=ask1,
        last=_num(f, 8, "last"),
        settle_estimate=_num(f, 9, "settle_estimate", default=0.0),
        pre_settle=_num(f, 10, "pre_settle"),
        bid_vol1=bid_vol1,
        ask_vol1=ask_vol1,
        open_interest=_int(f, 13, "open_interest"),
        volume=_int(f, 14, "volume"),
        exchange_short=f[15].strip() or None,
        product_name=f[16].strip() or None,
        avg_price=_num(f, 27, "avg_price", default=0.0),
        bids=((bid1, bid_vol1),),
        asks=((ask1, ask_vol1),),
        fields=tuple(f),
    )


def parse_cffex(code: str, payload: str) -> SinaQuote:
    """Parse a ``CFF_RE_`` payload (layout in the module docstring)."""
    f = payload.split(",")
    if len(f) < CFFEX_MIN_FIELDS:
        raise SinaFormatError(f"CFFEX payload has {len(f)} fields, expected >= {CFFEX_MIN_FIELDS}")
    bids = tuple(
        (_num(f, 16 + 2 * k, f"bid{k + 1}"), _int(f, 17 + 2 * k, f"bid_vol{k + 1}")) for k in range(5)
    )
    asks = tuple(
        (_num(f, 26 + 2 * k, f"ask{k + 1}"), _int(f, 27 + 2 * k, f"ask_vol{k + 1}")) for k in range(5)
    )
    return SinaQuote(
        code=code,
        kind="cffex",
        name=f[49].strip() if len(f) > 49 else "",
        quote_date=parse_date(f[36]),
        quote_time=parse_time(f[37]),
        open=_num(f, 0, "open"),
        high=_num(f, 1, "high"),
        low=_num(f, 2, "low"),
        last=_num(f, 3, "last"),
        volume=_int(f, 4, "volume"),
        turnover_raw=_num(f, 5, "turnover"),
        open_interest=_int(f, 6, "open_interest"),
        upper_limit=_num(f, 9, "upper_limit"),
        lower_limit=_num(f, 10, "lower_limit"),
        pre_close=_num(f, 13, "pre_close"),
        pre_settle=_num(f, 14, "pre_settle"),
        pre_open_interest=_int(f, 15, "pre_open_interest"),
        bid1=bids[0][0],
        bid_vol1=bids[0][1],
        ask1=asks[0][0],
        ask_vol1=asks[0][1],
        avg_price=_num(f, 48, "avg_price", default=0.0),
        bids=bids,
        asks=asks,
        fields=tuple(f),
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


def resolve_exch_datetime(
    quote_date: date | None,
    quote_time: time,
    recv_ns: int,
    *,
    future_tolerance_s: float = DEFAULT_FUTURE_TOLERANCE_S,
) -> datetime:
    """Beijing datetime of a quote.

    The quote's own date is trusted (stale snapshots keep their old date)
    unless it is missing, or it looks like a trading day sent in place of the
    natural date: a night time of day (>= 20:00 or < 03:00) more than
    :data:`REANCHOR_AHEAD` (12h) after the receive time. Such dates are 1-3
    days ahead, whereas a slow local clock is off by minutes, so ordinary
    quotes are never shifted. Re-anchoring picks the latest of
    tomorrow/today/yesterday at the quote's time of day that is not more than
    ``future_tolerance_s`` after the receive time.
    """
    recv_dt = ns_to_bj(recv_ns)
    if quote_date is not None:
        dt = datetime.combine(quote_date, quote_time, tzinfo=BJ)
        night = quote_time >= NIGHT_START or quote_time < EARLY_MORNING_END
        if not (night and dt - recv_dt > REANCHOR_AHEAD):
            return dt
    limit = recv_dt + timedelta(seconds=future_tolerance_s)
    base = recv_dt.date()
    candidates = [datetime.combine(base + timedelta(days=k), quote_time, tzinfo=BJ) for k in (1, 0, -1)]
    return max(c for c in candidates if c <= limit)


# ---------------------------------------------------------------------------
# Normalization
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class SinaResult:
    """Outcome of normalizing one requested code in one poll.

    ``ok`` (``tick`` set, ``reject_reason`` ``None``): a usable canonical :class:`Tick`.

    Otherwise ``reject_reason`` is set and ``detail`` explains why:

    - ``MD_REJECT_MALFORMED``: unparsable, empty or missing payload (``tick`` is ``None``);
    - ``MD_REJECT_TRADING_DAY``: the timestamp lies outside any trading day
      (``tick`` is ``None``), or it is more than ``future_tolerance_s`` after the
      receive time (detail ``future_ts ...``; ``tick`` is kept so the recorder
      stores every field, but it must never advance LIVE).

    Session, limit and monotonicity checks belong to the consumer (md-gateway
    validator, recorder classifier).
    """

    code: str
    instrument_id: str
    recv_ns: int
    quote: SinaQuote | None = None
    tick: Tick | None = None
    reject_reason: str | None = None
    detail: str = ""
    empty: bool = False  # Sina returned "" (unknown or delisted code)

    @property
    def ok(self) -> bool:
        return self.tick is not None and self.reject_reason is None


def resolve_sina_instruments(
    products: ProductsConfig,
    sina_symbols: Mapping[str, str],
    symbols: Iterable[str] | None = None,
) -> dict[str, InstrumentMeta]:
    """``{sina_code: InstrumentMeta}`` for ``symbols`` (default: every ``feeds.yaml`` sina symbol).

    Symbols may be typed in any case (``RB2701`` -> ``rb2701``). Codes come from
    the ``sina.symbols`` map, falling back to :func:`sina_code_for`.
    Raises ``ValueError`` for products missing from ``products.yaml``.
    """
    wanted = list(symbols) if symbols else list(sina_symbols)
    out: dict[str, InstrumentMeta] = {}
    for raw in wanted:
        meta = instrument_meta(products, raw)
        code = sina_symbols.get(meta.instrument_id) or sina_code_for(meta.instrument_id, meta.exchange)
        out.setdefault(code, meta)
    return out


class SinaNormalizer:
    """Turns Sina payloads into canonical :class:`Tick` objects (02 §7.1, §9.3)."""

    def __init__(
        self,
        instruments: Mapping[str, InstrumentMeta],
        calendar: TradingCalendar,
        *,
        future_tolerance_s: float = DEFAULT_FUTURE_TOLERANCE_S,
    ) -> None:
        """``instruments`` maps Sina code -> :class:`InstrumentMeta`, in request order.

        ``future_tolerance_s``: quotes stamped further than this after their
        receive time are rejected (``MD_REJECT_FUTURE_TS``, detail ``future_ts``).
        """
        if not instruments:
            raise ValueError("SinaNormalizer needs at least one instrument")
        self._metas = dict(instruments)
        self._calendar = calendar
        self._windows = WindowCache(calendar)
        self._future_tolerance_s = future_tolerance_s
        self._future_tolerance_ns = int(future_tolerance_s * NS_PER_S)
        self._warned: set[tuple[str, str]] = set()

    @property
    def future_tolerance_s(self) -> float:
        return self._future_tolerance_s

    @property
    def codes(self) -> list[str]:
        return list(self._metas)

    @property
    def instruments(self) -> dict[str, InstrumentMeta]:
        return dict(self._metas)

    @property
    def calendar(self) -> TradingCalendar:
        return self._calendar

    def meta_for(self, code: str) -> InstrumentMeta | None:
        return self._metas.get(code)

    def _warn_once(self, code: str, key: str, message: str) -> None:
        if (code, key) not in self._warned:
            self._warned.add((code, key))
            logger.warning(message)

    # ----------------------------------------------------------------- batch
    def normalize_text(self, text: str, recv_ns: int, codes: Iterable[str] | None = None) -> list[SinaResult]:
        """One result per requested code (default: all configured codes), in request order."""
        split = split_response(text)
        for line in split.junk:
            self._warn_once("", line[:40], f"sina.junk_line {line[:120]!r}")
        wanted = list(codes) if codes is not None else self.codes
        return [self.normalize(code, split.payloads.get(code), recv_ns) for code in wanted]

    # ------------------------------------------------------------ one quote
    def normalize(self, code: str, payload: str | None, recv_ns: int) -> SinaResult:
        meta = self._metas.get(code)
        iid = meta.instrument_id if meta is not None else ""
        if payload is None:
            return SinaResult(
                code, iid, recv_ns, reject_reason=MD_REJECT_MALFORMED, detail="code missing from response"
            )
        try:
            quote = parse_quote(code, payload)
        except SinaFormatError as exc:
            if exc.empty:
                self._warn_once(code, "empty", f"sina.empty_quote code={code}: unknown or delisted contract")
            return SinaResult(code, iid, recv_ns, reject_reason=exc.reason, detail=str(exc), empty=exc.empty)
        except Exception as exc:  # defensive: a parser bug must not stop the recorder
            logger.opt(exception=True).error(f"sina.parse_bug code={code}")
            return SinaResult(
                code, iid, recv_ns, reject_reason=MD_REJECT_MALFORMED, detail=f"internal: {exc!r}"
            )
        if meta is None:
            return SinaResult(
                code,
                iid,
                recv_ns,
                quote=quote,
                reject_reason=MD_REJECT_MALFORMED,
                detail="code not configured",
            )
        try:
            tick = self.to_tick(quote, meta, recv_ns)
        except SinaFormatError as exc:
            return SinaResult(code, iid, recv_ns, quote=quote, reject_reason=exc.reason, detail=str(exc))
        except Exception as exc:  # defensive, see above
            logger.opt(exception=True).error(f"sina.normalize_bug code={code}")
            return SinaResult(
                code,
                iid,
                recv_ns,
                quote=quote,
                reject_reason=MD_REJECT_MALFORMED,
                detail=f"internal: {exc!r}",
            )
        ahead_ns = tick.exch_ts_ns - recv_ns
        if ahead_ns > self._future_tolerance_ns:
            detail = f"{FUTURE_TS}: stamped {ahead_ns / NS_PER_S:.0f}s after the receive time"
            self._warn_once(
                code,
                FUTURE_TS,
                f"sina.{FUTURE_TS} code={code} {detail} (if every symbol shows this, the local clock is slow)",
            )
            return SinaResult(
                code, iid, recv_ns, quote=quote, tick=tick, reject_reason=MD_REJECT_FUTURE_TS, detail=detail
            )
        return SinaResult(code, iid, recv_ns, quote=quote, tick=tick)

    def _check_identity(self, quote: SinaQuote, meta: InstrumentMeta) -> None:
        is_cffex = meta.exchange is Exchange.CFFEX
        if (quote.kind == "cffex") != is_cffex:
            raise SinaFormatError(f"{quote.kind} quote for {meta.exchange} contract {meta.instrument_id}")
        if quote.exchange_short:
            allowed = EXCHANGE_SHORT_NAMES.get(quote.exchange_short)
            if allowed is not None and meta.exchange not in allowed:
                raise SinaFormatError(
                    f"exchange mismatch: Sina says {quote.exchange_short!r}, config says {meta.exchange}"
                )
        digits = "".join(ch for ch in meta.instrument_id if ch.isdigit())
        if quote.name and digits and digits not in quote.name:
            self._warn_once(
                quote.code,
                "name",
                f"sina.name_mismatch code={quote.code} name={quote.name!r} instrument={meta.instrument_id}",
            )

    def _commodity_pre_close(
        self, quote: SinaQuote, meta: InstrumentMeta, td: date, exch_ns: int
    ) -> float | None:
        """Field 5 as ``pre_close``: only inside a session window of ``td`` (window ends included,
        e.g. 10:15:00, 11:30:00, 23:00:00) and before the day's final close, after which Sina
        replaces it with today's close."""
        windows = self._windows.windows(meta.sessions, td)
        if not windows or exch_ns >= max(w.end_ns for w in windows):
            return None
        if any(w.start_ns <= exch_ns <= w.end_ns for w in windows):
            return positive_or_none(quote.pre_close)
        return None

    def to_tick(self, quote: SinaQuote, meta: InstrumentMeta, recv_ns: int) -> Tick:
        """Canonical :class:`Tick` for a parsed quote; raises :class:`SinaFormatError`.

        The future-timestamp check is applied by :meth:`normalize`, not here.
        """
        self._check_identity(quote, meta)
        if quote.pre_settle <= 0:
            raise SinaFormatError("pre-settlement price missing")
        exch_dt = resolve_exch_datetime(
            quote.quote_date, quote.quote_time, recv_ns, future_tolerance_s=self._future_tolerance_s
        )
        exch_ns = bj_to_ns(exch_dt)
        try:
            td = self._calendar.trading_day_for(exch_dt)
        except LookupError:
            td = None
        if td is None:
            detail = f"{exch_dt:%Y-%m-%d %H:%M:%S} is not in a trading day"
            if exch_ns - recv_ns > self._future_tolerance_ns:
                raise SinaFormatError(f"{FUTURE_TS}: {detail}", MD_REJECT_FUTURE_TS)
            raise SinaFormatError(detail, MD_REJECT_TRADING_DAY)
        trading_day = td_str(td)
        phase = self._calendar.phase_at(list(meta.sessions), exch_ns)

        bid1, ask1 = positive_or_none(quote.bid1), positive_or_none(quote.ask1)
        if (quote.upper_limit or 0.0) > 0 and (quote.lower_limit or 0.0) > 0:
            upper, lower = float(quote.upper_limit or 0.0), float(quote.lower_limit or 0.0)
        else:
            upper, lower = derive_limits(quote.pre_settle, meta.limit_pct_on(trading_day), meta.tick_size)

        if quote.kind == "cffex":
            turnover = (quote.turnover_raw or 0.0) * meta.multiplier
            pre_close = positive_or_none(quote.pre_close)
        else:
            avg = positive_or_none(quote.avg_price)
            turnover = avg * quote.volume * meta.multiplier if avg is not None else 0.0
            pre_close = self._commodity_pre_close(quote, meta, td, exch_ns)
        turnover = round(turnover, 2)

        flags = FLAG_COARSE_TS | limit_lock_flags(bid1, ask1, upper, lower)
        if phase.state == "AUCTION":
            flags |= FLAG_AUCTION
        last = float(quote.last)
        fingerprint = compute_fingerprint(
            trading_day=trading_day,
            volume=quote.volume,
            turnover=turnover,
            open_interest=quote.open_interest,
            last=last,
            bid1=bid1,
            ask1=ask1,
            bid_vol1=quote.bid_vol1,
            ask_vol1=quote.ask_vol1,
        )
        return Tick(
            instrument_id=meta.instrument_id,
            exchange=meta.exchange,
            trading_day=trading_day,
            action_day=exch_dt.strftime("%Y%m%d"),
            exch_ts_ns=exch_ns,
            last=last,
            volume=quote.volume,
            turnover=turnover,
            open_interest=quote.open_interest,
            bid1=bid1,
            ask1=ask1,
            bid_vol1=quote.bid_vol1,
            ask_vol1=quote.ask_vol1,
            open=positive_or_none(quote.open),
            high=positive_or_none(quote.high),
            low=positive_or_none(quote.low),
            upper_limit=float(upper),
            lower_limit=float(lower),
            pre_settle=float(quote.pre_settle),
            pre_close=pre_close,
            pre_open_interest=quote.pre_open_interest,
            avg_price=positive_or_none(quote.avg_price),
            source=SourceId.SINA,
            source_seq=None,
            recv_ns=recv_ns,
            fingerprint=fingerprint,
            flags=flags,
        )
