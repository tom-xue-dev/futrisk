"""Normalizer: vendor records -> canonical :class:`Tick` (01 §5.3.2, 02 §7.1, §9.1-§9.3, docs/README §3.3).

Shared by every source. Instrument ids take the exchange's case from ``products.yaml``
(``RB2701`` -> ``rb2701``); unknown products are ``MD_REJECT_MALFORMED``. The tick builder
(:meth:`Normalizer.build`) applies the rules every decoder relies on: the exchange timestamp is
UTC ns and the **trading day is recomputed with the calendar** (night session -> next trading
day); the vendor's own day is only checked (Feed A's must equal it, Feed B's must not be
older: ``MD_REJECT_TRADING_DAY``); timestamps more than ``future_tolerance_s`` ahead of the
receive time are ``MD_REJECT_FUTURE_TS``; non-positive or non-finite optional prices become
``None``; missing limits are ``pre_settle × (1 ± limit_pct)`` rounded inward; ``flags`` carry
AUCTION, limit locks and COARSE_TS; the fingerprint follows 02 §7.1 (msgpack of the normalized
``[trading_day, volume, turnover, open_interest, last, bid1, ask1, bid_vol1, ask_vol1]``,
BLAKE2b 8 bytes, big-endian). The Feed A decoder (CTP names, typed fast path, DCE night
``ActionDay`` quirk repaired) and the Feed B decoder (vendor names) live here; the Sina
decoder in :mod:`.sina` uses the same builder.
"""

from __future__ import annotations

import hashlib
import math
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Any

import msgspec

from futrisk.common.clock import BJ, NS_PER_MS, NS_PER_S, bj_to_ns
from futrisk.common.config.products import ProductsConfig, ProductSpec, product_code
from futrisk.common.decimals import D, limit_prices
from futrisk.messages.codes import MD_REJECT_FUTURE_TS, MD_REJECT_MALFORMED, MD_REJECT_TRADING_DAY
from futrisk.messages.enums import Exchange, SourceId
from futrisk.messages.md import (
    FLAG_AUCTION,
    FLAG_COARSE_TS,
    FLAG_LIMIT_DOWN_LOCKED,
    FLAG_LIMIT_UP_LOCKED,
    Tick,
)
from futrisk.services.md_gateway.sessions import SessionIndex
from futrisk.services.md_gateway.wire import CtpTick, VendorQuote

#: CTP marks missing doubles with DBL_MAX (1.7976931348623157e308); anything this large is "no value"
_NO_VALUE = 1e300
_PRICE_EPS = 1e-6
_CACHE_MAX = 50_000
_ID_RE = re.compile(r"^([A-Za-z]+)(\d+)$")
_F_AUCTION = int(FLAG_AUCTION)
_F_COARSE = int(FLAG_COARSE_TS)
_F_UP = int(FLAG_LIMIT_UP_LOCKED)
_F_DOWN = int(FLAG_LIMIT_DOWN_LOCKED)
LIMIT_LOCK_FLAGS = _F_UP | _F_DOWN

_fp_encode = msgspec.msgpack.Encoder().encode
_blake2b = hashlib.blake2b


# ---------------------------------------------------------------- fingerprint (02 §7.1)
def fast_fingerprint(
    trading_day: str, volume: int, turnover: float, open_interest: int, last: float, bid1: float | None, ask1: float | None, bid_vol1: int, ask_vol1: int
) -> int:
    """64-bit unsigned fingerprint of a normalized L1 state; timestamps are excluded on purpose."""
    b = None if bid1 is None else (round(float(bid1), 6) or 0.0)
    a = None if ask1 is None else (round(float(ask1), 6) or 0.0)
    key = [trading_day, int(volume), round(float(turnover), 2) or 0.0, int(open_interest), round(float(last), 6) or 0.0, b, a, int(bid_vol1), int(ask_vol1)]
    return int.from_bytes(_blake2b(_fp_encode(key), digest_size=8).digest(), "big")


def tick_fingerprint(tick: Tick) -> int:
    """Fingerprint of a :class:`Tick` (its own ``fingerprint`` field is ignored)."""
    return fast_fingerprint(tick.trading_day, tick.volume, tick.turnover, tick.open_interest, tick.last, tick.bid1, tick.ask1, tick.bid_vol1, tick.ask_vol1)


# ---------------------------------------------------------------- instrument metadata
@dataclass(frozen=True, slots=True)
class InstrumentMeta:
    """Static contract data a decoder needs to build a :class:`Tick`."""

    instrument_id: str
    product: str
    exchange: Exchange
    multiplier: int
    tick_size: Decimal
    limit_pct: Decimal
    sessions: tuple[str, ...]
    spec: ProductSpec | None = field(default=None, compare=False, repr=False)

    def limit_pct_on(self, trading_day: str | None) -> Decimal:
        """Limit percentage effective on ``trading_day`` (the product's ``limit_schedule``)."""
        return self.spec.limit_pct_on(trading_day) if self.spec is not None else self.limit_pct


def canonical_instrument_id(products: ProductsConfig, raw: str) -> str:
    """Restore the exchange case of a contract code (``RB2701`` -> ``rb2701``, ``if2612`` -> ``IF2612``)."""
    m = _ID_RE.match(raw.strip())
    if m is None:
        raise ValueError(f"bad instrument id {raw!r}")
    letters, digits = m.groups()
    for code in products.products:
        if code.lower() == letters.lower():
            return f"{code}{digits}"
    raise ValueError(f"unknown product {letters!r} in {raw!r} (not in products.yaml)")


def instrument_meta(products: ProductsConfig, instrument_id: str) -> InstrumentMeta:
    """Build :class:`InstrumentMeta` from ``products.yaml`` (the instrument need not be listed)."""
    iid = canonical_instrument_id(products, instrument_id)
    inst = products.instrument(iid)
    product = inst.product if inst is not None else product_code(iid)
    spec = products.products[product]
    return InstrumentMeta(iid, product, spec.exchange, spec.multiplier, spec.tick_size, spec.limit_pct, tuple(spec.sessions), spec)


def derive_limits(pre_settle: float, limit_pct: Decimal, tick_size: Decimal) -> tuple[float, float]:
    """``(upper, lower)`` from the pre-settlement price, rounded inward to the tick."""
    upper, lower = limit_prices(D(pre_settle), limit_pct, tick_size)
    return float(upper), float(lower)


def positive_or_none(x: float | None) -> float | None:
    """Optional price: ``None`` for null, NaN / inf, DBL_MAX or non-positive values."""
    return float(x) if x is not None and 0.0 < x < _NO_VALUE else None


def lock_flags(bid1: float | None, ask1: float | None, upper: float, lower: float) -> int:
    """``LIMIT_UP_LOCKED`` / ``LIMIT_DOWN_LOCKED``: a one-sided book pinned at the limit (02 §7.1 flags)."""
    flags = 0
    if upper > 0 and ask1 is None and bid1 is not None and bid1 >= upper - _PRICE_EPS:
        flags |= _F_UP
    if lower > 0 and bid1 is None and ask1 is not None and ask1 <= lower + _PRICE_EPS:
        flags |= _F_DOWN
    return flags


class NormalizeError(ValueError):
    """A record that cannot become a valid :class:`Tick`; ``reason`` is an 02 §11.3 code.

    ``tick`` is set when the record could be parsed but is rejected for its trading day or
    timestamp (the recorder then keeps every column).
    """

    def __init__(self, reason: str, detail: str = "", *, instrument_id: str = "", exchange: str | None = None, tick: Tick | None = None) -> None:
        super().__init__(f"{reason}: {detail}" if detail else reason)
        self.reason = reason
        self.detail = detail
        self.instrument_id = instrument_id
        self.exchange = exchange
        self.tick = tick


# ---------------------------------------------------------------- the builder and the JSON decoders
class Normalizer:
    """Instrument metadata cache, the tick builder and the Feed A / Feed B decoders."""

    def __init__(self, products: ProductsConfig, sessions: SessionIndex, *, future_tolerance_s: float = 120.0) -> None:
        self._sessions = sessions
        self._future_s = future_tolerance_s
        self._future_ns = int(future_tolerance_s * NS_PER_S)
        self._products = products
        self._metas: dict[str, InstrumentMeta] = {}
        self._sec_cache: dict[tuple[str, str], int] = {}
        self._limits: dict[tuple[str, str, float], tuple[float, float]] = {}
        self.dce_night_fixes = 0

    @property
    def sessions(self) -> SessionIndex:
        return self._sessions

    @property
    def products(self) -> ProductsConfig:
        return self._products

    @property
    def future_tolerance_s(self) -> float:
        return self._future_s

    def meta(self, raw_id: str) -> InstrumentMeta:
        """Contract metadata for a vendor id in any case; ``NormalizeError`` for unknown products."""
        hit = self._metas.get(raw_id)
        if hit is not None:
            return hit
        try:
            iid = canonical_instrument_id(self._products, raw_id)
            meta = self._metas.get(iid) or instrument_meta(self._products, iid)
        except (ValueError, KeyError) as exc:
            raise NormalizeError(MD_REJECT_MALFORMED, f"unknown instrument {raw_id!r}: {exc}", instrument_id=raw_id) from exc
        if len(self._metas) >= _CACHE_MAX:
            self._metas.clear()
        self._metas[raw_id] = self._metas[meta.instrument_id] = meta
        return meta

    def known_meta(self, instrument_id: str) -> InstrumentMeta | None:
        try:
            return self.meta(instrument_id)
        except NormalizeError:
            return None

    # ------------------------------------------------------------------ Feed A
    def _bj_second_ns(self, action_day: str, update_time: str) -> int:
        key = (action_day, update_time)
        hit = self._sec_cache.get(key)
        if hit is not None:
            return hit
        d = date(int(action_day[0:4]), int(action_day[4:6]), int(action_day[6:8]))
        hh, mm, ss = update_time.split(":")
        ns = bj_to_ns(datetime.combine(d, time(int(hh), int(mm), int(ss)), tzinfo=BJ))
        if len(self._sec_cache) >= _CACHE_MAX:
            self._sec_cache.clear()
        self._sec_cache[key] = ns
        return ns

    def _exch_ns(self, action_day: str, update_time: str, millis: int, vendor_td: str | None) -> int:
        """Exchange time of a CTP record, with the DCE night ``ActionDay == TradingDay`` quirk repaired."""
        exch_ns = self._bj_second_ns(action_day, update_time) + millis * NS_PER_MS
        if vendor_td and action_day == vendor_td and self._sessions.trading_day(exch_ns) != vendor_td:
            prev = self._sessions.prev_trading_day(vendor_td)
            if prev is not None:
                d = date(int(prev[0:4]), int(prev[4:6]), int(prev[6:8]))
                if int(update_time[0:2]) < 12:
                    d += timedelta(days=1)  # past midnight: the day after the previous trading day's evening
                alt = self._bj_second_ns(d.strftime("%Y%m%d"), update_time) + millis * NS_PER_MS
                if self._sessions.trading_day(alt) == vendor_td:
                    self.dce_night_fixes += 1
                    return alt
        return exch_ns

    def feed_a_struct(self, c: CtpTick, *, recv_ns: int, seq: int | None) -> Tick:
        """Typed ``FeedATick`` (CTP field names, 02 §9.1) -> :class:`Tick`; raises :class:`NormalizeError`."""
        meta = self.meta(c.InstrumentID)
        ex = str(meta.exchange)
        bad = _check(c.ExchangeID, ex, c.LastPrice, c.Volume, c.Turnover, c.OpenInterest, c.BidVolume1, c.AskVolume1)
        if bad is None and not 0 <= c.UpdateMillisec < 1000:
            bad = f"UpdateMillisec {c.UpdateMillisec} out of range"
        if bad is not None:
            raise NormalizeError(MD_REJECT_MALFORMED, bad, instrument_id=meta.instrument_id, exchange=ex)
        try:
            exch_ns = self._exch_ns(c.ActionDay, c.UpdateTime, c.UpdateMillisec, c.TradingDay)
        except (ValueError, IndexError) as exc:
            raise NormalizeError(MD_REJECT_MALFORMED, f"bad time {c.ActionDay} {c.UpdateTime}: {exc}", instrument_id=meta.instrument_id, exchange=ex) from exc
        pre_oi = c.PreOpenInterest
        return self.build(
            meta, exch_ns, recv_ns, SourceId.FEED_A, seq, c.LastPrice, c.Volume, c.Turnover, int(round(c.OpenInterest)),
            positive_or_none(c.BidPrice1), positive_or_none(c.AskPrice1), c.BidVolume1, c.AskVolume1, positive_or_none(c.OpenPrice),
            positive_or_none(c.HighestPrice), positive_or_none(c.LowestPrice), positive_or_none(c.UpperLimitPrice),
            positive_or_none(c.LowerLimitPrice), positive_or_none(c.PreSettlementPrice), positive_or_none(c.PreClosePrice),
            int(round(pre_oi)) if pre_oi is not None and 0.0 <= pre_oi < _NO_VALUE else None, positive_or_none(c.AveragePrice),
            c.TradingDay or "", True, False,
        )

    def feed_a(self, d: Mapping[str, Any], *, recv_ns: int, seq: int | None) -> Tick:
        """Generic ``dict`` path of :meth:`feed_a_struct` (frames that failed the typed decode)."""
        meta = self.meta(str(d.get("InstrumentID") or ""))
        try:
            c = msgspec.convert(d, CtpTick, strict=False)
        except msgspec.ValidationError as exc:
            raise NormalizeError(MD_REJECT_MALFORMED, str(exc), instrument_id=meta.instrument_id, exchange=str(meta.exchange)) from exc
        return self.feed_a_struct(c, recv_ns=recv_ns, seq=seq)

    # ------------------------------------------------------------------ Feed B
    def feed_b(self, rec: Mapping[str, Any], *, vendor_td: str | None, recv_ns: int) -> Tick:
        """Vendor record of ``GET /v1/quotes`` (02 §9.2) -> :class:`Tick`; raises :class:`NormalizeError`."""
        meta = self.meta(str(rec.get("sym") or ""))
        ex = str(meta.exchange)
        try:
            q = msgspec.convert(rec, VendorQuote, strict=False)
        except msgspec.ValidationError as exc:
            raise NormalizeError(MD_REJECT_MALFORMED, str(exc), instrument_id=meta.instrument_id, exchange=ex) from exc
        bad = _check(q.ex, ex, q.px, q.vol, q.amt, q.oi, q.bv, q.av)
        if bad is not None:
            raise NormalizeError(MD_REJECT_MALFORMED, bad, instrument_id=meta.instrument_id, exchange=ex)
        return self.build(
            meta, q.t * NS_PER_MS, recv_ns, SourceId.FEED_B, None, q.px, q.vol, q.amt, int(round(q.oi)), positive_or_none(q.bp),
            positive_or_none(q.ap), q.bv, q.av, positive_or_none(q.o), positive_or_none(q.h), positive_or_none(q.l), positive_or_none(q.up),
            positive_or_none(q.dn), positive_or_none(q.ps), positive_or_none(q.pc), None, None, vendor_td or "", False, False,
        )

    # ------------------------------------------------------------------ the builder
    def build(  # noqa: PLR0913 - positional hot path shared by every decoder
        self, meta: InstrumentMeta, exch_ns: int, recv_ns: int, source: SourceId, seq: int | None, last: float, volume: int,
        turnover: float, open_interest: int, bid1: float | None, ask1: float | None, bid_vol1: int, ask_vol1: int,
        open_: float | None, high: float | None, low: float | None, upper: float | None, lower: float | None,
        pre_settle: float | None, pre_close: float | None, pre_oi: int | None, avg: float | None, vendor_td: str,
        td_equal: bool, coarse_ts: bool,
    ) -> Tick:
        """Assemble the canonical tick from normalized field values; raises :class:`NormalizeError`."""
        iid = meta.instrument_id
        ex = str(meta.exchange)
        if pre_settle is None:
            raise NormalizeError(MD_REJECT_MALFORMED, "pre-settlement price missing", instrument_id=iid, exchange=ex)
        sessions = self._sessions
        td = sessions.trading_day(exch_ns)
        action_day = sessions.bj_date(exch_ns)
        if upper is None or lower is None:
            # 02 §7.1：数据源未提供涨跌停价时，按昨结算 ×(1 ± 当日生效的涨跌停幅度) 向内取整
            lkey = (iid, td or action_day, pre_settle)
            lim = self._limits.get(lkey)
            if lim is None:
                lim = derive_limits(pre_settle, meta.limit_pct_on(td), meta.tick_size)
                if len(self._limits) >= 4096:
                    self._limits.clear()
                self._limits[lkey] = lim
            upper, lower = lim
        flags = lock_flags(bid1, ask1, upper, lower) if ask1 is None or bid1 is None else 0
        if coarse_ts:
            flags |= _F_COARSE
        day = td or action_day
        if td is not None:
            for start, end, cont in sessions.int_windows(meta.sessions, td):
                if start <= exch_ns < end:
                    if not cont:
                        flags |= _F_AUCTION
                    break
        tick = Tick(
            instrument_id=iid, exchange=meta.exchange, trading_day=day, action_day=action_day, exch_ts_ns=exch_ns, last=last,
            volume=volume, turnover=turnover, open_interest=open_interest, bid1=bid1, ask1=ask1, bid_vol1=bid_vol1,
            ask_vol1=ask_vol1, open=open_, high=high, low=low, upper_limit=float(upper), lower_limit=float(lower),
            pre_settle=pre_settle, pre_close=pre_close, pre_open_interest=pre_oi, avg_price=avg, source=source,
            source_seq=seq, recv_ns=recv_ns,
            fingerprint=fast_fingerprint(day, volume, turnover, open_interest, last, bid1, ask1, bid_vol1, ask_vol1), flags=flags,
        )
        ahead = exch_ns - recv_ns
        if ahead > self._future_ns:
            # 02 §11.3：交易所时间戳比收到时刻晚得多（本机时钟偏慢或日期字段异常）
            detail = f"future_ts: stamped {ahead / NS_PER_S:.0f}s after the receive time"
            raise NormalizeError(MD_REJECT_FUTURE_TS, detail, instrument_id=iid, exchange=ex, tick=tick)
        # 02 §9.1/§9.2：交易日由 md-gateway 按日历重算，数据源给出的交易日只作校验
        if td is None:
            raise NormalizeError(MD_REJECT_TRADING_DAY, "exchange time is not in a trading day", instrument_id=iid, exchange=ex, tick=tick)
        if vendor_td and ((td_equal and vendor_td != td) or (not td_equal and td > vendor_td)):
            raise NormalizeError(MD_REJECT_TRADING_DAY, f"vendor trading day {vendor_td} vs calendar {td}", instrument_id=iid, exchange=ex, tick=tick)
        return tick


def _check(ex: str | None, expected_ex: str, last: float, volume: int, turnover: float, oi: float, bid_vol: int, ask_vol: int) -> str | None:
    """Structural checks shared by the JSON feeds: exchange, finite non-negative market fields."""
    if ex is not None and ex != expected_ex:
        return f"exchange {ex} != configured {expected_ex}"
    if not 0.0 <= last < _NO_VALUE or not 0.0 <= turnover < _NO_VALUE or volume < 0 or not 0.0 <= oi < _NO_VALUE or math.isnan(last):
        return f"last={last} volume={volume} turnover={turnover} open_interest={oi}"
    if bid_vol < 0 or ask_vol < 0:
        return "negative book volume"
    return None


__all__ = [
    "LIMIT_LOCK_FLAGS",
    "InstrumentMeta",
    "NormalizeError",
    "Normalizer",
    "canonical_instrument_id",
    "derive_limits",
    "fast_fingerprint",
    "instrument_meta",
    "lock_flags",
    "positive_or_none",
    "tick_fingerprint",
]
