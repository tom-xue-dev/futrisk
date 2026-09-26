"""Normalizer: vendor records -> canonical :class:`Tick` (01 §5.3.2, 02 §7.1, §9.1, §9.2).

Rules shared by every source (the Sina adapter reuses :mod:`futrisk.marketdata.sina`,
which follows the same rules):

- instrument ids take the exchange's case from ``products.yaml`` (``RB2701`` -> ``rb2701``,
  README §3.3); unknown products are ``MD_REJECT_MALFORMED``;
- the exchange timestamp is converted from Beijing time to UTC ns; the **trading day is
  recomputed with the calendar** at that exchange time (night session -> next trading
  day). The vendor's own trading day is only checked: Feed A's ``TradingDay`` must equal
  it, Feed B's top-level ``td`` must not be older than it; otherwise
  ``MD_REJECT_TRADING_DAY`` (also for timestamps outside every trading day).
  CTP quirk (DCE night session): ``ActionDay`` carries the *trading* day instead of the
  natural date; when ``ActionDay == TradingDay`` and the timestamp built from it does not
  belong to that trading day, the natural date is recomputed from the calendar (the
  previous trading day's evening, or the day after it past midnight) and the tick is kept
  with the corrected ``action_day``. Timestamps more than ``future_tolerance_s`` ahead of
  the receive time are ``MD_REJECT_FUTURE_TS``;
- ``DBL_MAX`` (CTP "no value"), non-positive or non-finite optional prices become ``None``;
- limit prices come from the source when present, otherwise from
  ``pre_settle × (1 ± limit_pct_on(trading_day))`` rounded **inward** to the tick;
- ``flags``: ``AUCTION`` when the exchange time lies in an auction window, limit locks
  from a one-sided book at the limit, ``COARSE_TS`` for second-resolution sources;
- ``recv_ns`` is the md-gateway receive time in market time (01 §8.2);
- ``fingerprint`` per 02 §7.1 over the normalized values (:func:`fast_fingerprint` is a
  positional re-implementation of :func:`futrisk.marketdata.fingerprint.compute_fingerprint`
  with identical output).

Feed A frames usually arrive as typed :class:`~.wire.CtpTick` structs
(:meth:`Normalizer.feed_a_struct`, the hot path); anything else goes through the generic
``dict`` path (:meth:`Normalizer.feed_a`).
"""

from __future__ import annotations

import hashlib
import math
from collections.abc import Mapping
from datetime import date, datetime, time, timedelta
from typing import Any

import msgspec

from futrisk.common.clock import BJ, NS_PER_MS, NS_PER_S, bj_to_ns
from futrisk.common.config.products import ProductsConfig
from futrisk.marketdata.normalize import (
    InstrumentMeta,
    canonical_instrument_id,
    derive_limits,
    instrument_meta,
)
from futrisk.messages.codes import MD_REJECT_FUTURE_TS, MD_REJECT_MALFORMED, MD_REJECT_TRADING_DAY
from futrisk.messages.enums import SourceId
from futrisk.messages.md import (
    FLAG_AUCTION,
    FLAG_COARSE_TS,
    FLAG_LIMIT_DOWN_LOCKED,
    FLAG_LIMIT_UP_LOCKED,
    Tick,
)
from futrisk.services.md_gateway.sessions import SessionIndex
from futrisk.services.md_gateway.wire import CtpTick

#: CTP marks missing doubles with DBL_MAX (1.7976931348623157e308); anything this large is "no value"
_NO_VALUE = 1e300
_PRICE_EPS = 1e-6
_TS_CACHE_MAX = 50_000
_LIMIT_CACHE_MAX = 4096
# plain ints on the hot path
_F_AUCTION = int(FLAG_AUCTION)
_F_COARSE = int(FLAG_COARSE_TS)
_F_UP = int(FLAG_LIMIT_UP_LOCKED)
_F_DOWN = int(FLAG_LIMIT_DOWN_LOCKED)

_fp_encode = msgspec.msgpack.Encoder().encode
_blake2b = hashlib.blake2b


def fast_fingerprint(
    trading_day: str,
    volume: int,
    turnover: float,
    open_interest: int,
    last: float,
    bid1: float | None,
    ask1: float | None,
    bid_vol1: int,
    ask_vol1: int,
) -> int:
    """02 §7.1 fingerprint (same bytes as ``compute_fingerprint``) for already normalized values."""
    t = round(turnover, 2) or 0.0
    lp = round(last, 6) or 0.0
    b = None if bid1 is None else (round(bid1, 6) or 0.0)
    a = None if ask1 is None else (round(ask1, 6) or 0.0)
    key = [trading_day, volume, t, open_interest, lp, b, a, bid_vol1, ask_vol1]
    return int.from_bytes(_blake2b(_fp_encode(key), digest_size=8).digest(), "big")


class NormalizeError(ValueError):
    """A record that cannot become a valid :class:`Tick`; ``reason`` is an 02 §11.3 code.

    ``tick`` is set when the record could be fully parsed but is rejected for its
    trading day (the recorder then keeps every column).
    """

    def __init__(
        self,
        reason: str,
        detail: str = "",
        *,
        instrument_id: str = "",
        exchange: str | None = None,
        tick: Tick | None = None,
    ) -> None:
        super().__init__(f"{reason}: {detail}" if detail else reason)
        self.reason = reason
        self.detail = detail
        self.instrument_id = instrument_id
        self.exchange = exchange
        self.tick = tick


def opt_price(value: Any) -> float | None:
    """Optional price: ``None`` for null, empty, DBL_MAX, non-positive or non-finite values."""
    if value is None:
        return None
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        value = s
    f = float(value)  # ValueError for "N/A": the caller turns it into MD_REJECT_MALFORMED
    if not math.isfinite(f) or f <= 0 or f >= _NO_VALUE:
        return None
    return f


def _fopt(x: float | None) -> float | None:
    """Typed fast path of :func:`opt_price` (NaN / inf / DBL_MAX / non-positive -> None)."""
    if x is None or not 0.0 < x < _NO_VALUE:
        return None
    return x


def req_float(value: Any, name: str) -> float:
    if value is None:
        raise ValueError(f"{name} missing")
    f = float(value.strip() if isinstance(value, str) else value)
    if not math.isfinite(f) or f >= _NO_VALUE:
        raise ValueError(f"{name} is not a finite number")
    return f


def req_int(value: Any, name: str) -> int:
    if value is None or isinstance(value, bool):
        raise ValueError(f"{name} missing")
    f = float(value.strip() if isinstance(value, str) else value)
    if not math.isfinite(f) or f < 0 or f != math.floor(f):
        raise ValueError(f"{name} must be a non-negative integer, got {value!r}")
    return int(f)


def lock_flags(bid1: float | None, ask1: float | None, upper: float, lower: float) -> int:
    """``LIMIT_UP_LOCKED`` / ``LIMIT_DOWN_LOCKED``: one-sided book pinned at the limit (02 §7.1 flags)."""
    flags = 0
    if upper > 0 and ask1 is None and bid1 is not None and bid1 >= upper - _PRICE_EPS:
        flags |= _F_UP
    if lower > 0 and bid1 is None and ask1 is not None and ask1 <= lower + _PRICE_EPS:
        flags |= _F_DOWN
    return flags


class Normalizer:
    """Builds canonical ticks from Feed A (CTP-style) and Feed B (vendor-style) records."""

    def __init__(
        self,
        products: ProductsConfig,
        sessions: SessionIndex,
        *,
        future_tolerance_s: float = 120.0,
    ) -> None:
        self._sessions = sessions
        self._future_ns = int(future_tolerance_s * NS_PER_S)
        self.reload(products)

    def reload(self, products: ProductsConfig) -> None:
        """Switch to another ``products.yaml`` (staged config applied at a trading-day roll)."""
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

    # ------------------------------------------------------------------ metadata
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
        if len(self._metas) >= _TS_CACHE_MAX:
            self._metas.clear()
        self._metas[raw_id] = meta
        self._metas[meta.instrument_id] = meta
        return meta

    def known_meta(self, instrument_id: str) -> InstrumentMeta | None:
        hit = self._metas.get(instrument_id)
        if hit is not None:
            return hit
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
        if len(self._sec_cache) >= _TS_CACHE_MAX:
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
        """Typed fast path of :meth:`feed_a` (same rules); raises :class:`NormalizeError`."""
        meta = self.meta(c.InstrumentID)
        millis = c.UpdateMillisec
        last = c.LastPrice
        turnover = c.Turnover
        oi = c.OpenInterest
        volume = c.Volume
        bad = None
        if c.ExchangeID is not None and c.ExchangeID != meta.exchange:
            bad = f"exchange {c.ExchangeID} != configured {meta.exchange}"
        elif not 0 <= millis < 1000:
            bad = f"UpdateMillisec {millis} out of range"
        elif not -_NO_VALUE < last < _NO_VALUE or not 0.0 <= turnover < _NO_VALUE or volume < 0 or not 0.0 <= oi < _NO_VALUE:
            bad = f"LastPrice={last} Volume={volume} Turnover={turnover} OpenInterest={oi}"
        elif c.BidVolume1 < 0 or c.AskVolume1 < 0:
            bad = "negative book volume"
        if bad is not None:
            raise NormalizeError(MD_REJECT_MALFORMED, bad, instrument_id=meta.instrument_id, exchange=str(meta.exchange))
        try:
            exch_ns = self._exch_ns(c.ActionDay, c.UpdateTime, millis, c.TradingDay)
        except (ValueError, IndexError) as exc:
            raise NormalizeError(MD_REJECT_MALFORMED, f"bad time {c.ActionDay} {c.UpdateTime}: {exc}", instrument_id=meta.instrument_id, exchange=str(meta.exchange)) from exc
        pre_oi = c.PreOpenInterest
        return self._make(
            meta,
            exch_ns,
            recv_ns,
            SourceId.FEED_A,
            seq,
            last,
            volume,
            turnover,
            int(round(oi)),
            _fopt(c.BidPrice1),
            _fopt(c.AskPrice1),
            c.BidVolume1,
            c.AskVolume1,
            _fopt(c.OpenPrice),
            _fopt(c.HighestPrice),
            _fopt(c.LowestPrice),
            _fopt(c.UpperLimitPrice),
            _fopt(c.LowerLimitPrice),
            _fopt(c.PreSettlementPrice),
            _fopt(c.PreClosePrice),
            int(round(pre_oi)) if pre_oi is not None and 0.0 <= pre_oi < _NO_VALUE else None,
            _fopt(c.AveragePrice),
            c.TradingDay or "",
            True,
            False,
        )

    def feed_a(self, d: Mapping[str, Any], *, recv_ns: int, seq: int | None, source: SourceId = SourceId.FEED_A) -> Tick:
        """``FeedATick`` (CTP field names, 02 §9.1) -> :class:`Tick`; raises :class:`NormalizeError`."""
        raw_id = str(d.get("InstrumentID") or "")
        meta = self.meta(raw_id)
        try:
            exch_field = d.get("ExchangeID")
            if exch_field is not None and str(exch_field) != str(meta.exchange):
                raise ValueError(f"exchange {exch_field} != configured {meta.exchange}")
            action_day = str(d["ActionDay"])
            millis = req_int(d.get("UpdateMillisec", 0), "UpdateMillisec")
            if millis >= 1000:
                raise ValueError(f"UpdateMillisec {millis} out of range")
            vendor_td = str(d.get("TradingDay") or "")
            exch_ns = self._exch_ns(action_day, str(d["UpdateTime"]), millis, vendor_td)
            fields: dict[str, Any] = {
                "last": req_float(d.get("LastPrice"), "LastPrice"),
                "volume": req_int(d.get("Volume"), "Volume"),
                "turnover": req_float(d.get("Turnover"), "Turnover"),
                "open_interest": req_int(round(req_float(d.get("OpenInterest"), "OpenInterest")), "OpenInterest"),
                "bid1": opt_price(d.get("BidPrice1")),
                "ask1": opt_price(d.get("AskPrice1")),
                "bid_vol1": req_int(d.get("BidVolume1", 0) or 0, "BidVolume1"),
                "ask_vol1": req_int(d.get("AskVolume1", 0) or 0, "AskVolume1"),
                "open": opt_price(d.get("OpenPrice")),
                "high": opt_price(d.get("HighestPrice")),
                "low": opt_price(d.get("LowestPrice")),
                "upper": opt_price(d.get("UpperLimitPrice")),
                "lower": opt_price(d.get("LowerLimitPrice")),
                "pre_settle": opt_price(d.get("PreSettlementPrice")),
                "pre_close": opt_price(d.get("PreClosePrice")),
                "pre_oi": _opt_int(d.get("PreOpenInterest")),
                "avg": opt_price(d.get("AveragePrice")),
            }
        except (KeyError, TypeError, ValueError, IndexError) as exc:
            raise NormalizeError(
                MD_REJECT_MALFORMED, str(exc), instrument_id=meta.instrument_id, exchange=str(meta.exchange)
            ) from exc
        return self.build(
            meta,
            exch_ns=exch_ns,
            recv_ns=recv_ns,
            source=source,
            seq=seq,
            vendor_td=vendor_td,
            vendor_td_rule="equal",
            **fields,
        )

    # ------------------------------------------------------------------ Feed B
    def feed_b(self, rec: Mapping[str, Any], *, vendor_td: str | None, recv_ns: int) -> Tick:
        """Vendor record of ``GET /v1/quotes`` (02 §9.2) -> :class:`Tick`; raises :class:`NormalizeError`."""
        raw_id = str(rec.get("sym") or "")
        meta = self.meta(raw_id)
        try:
            ex = rec.get("ex")
            if ex is not None and str(ex) != str(meta.exchange):
                raise ValueError(f"exchange {ex} != configured {meta.exchange}")
            t_ms = req_int(rec.get("t"), "t")
            fields: dict[str, Any] = {
                "last": req_float(rec.get("px"), "px"),
                "volume": req_int(rec.get("vol"), "vol"),
                "turnover": req_float(rec.get("amt"), "amt"),
                "open_interest": req_int(rec.get("oi"), "oi"),
                "bid1": opt_price(rec.get("bp")),
                "ask1": opt_price(rec.get("ap")),
                "bid_vol1": req_int(rec.get("bv", 0) or 0, "bv"),
                "ask_vol1": req_int(rec.get("av", 0) or 0, "av"),
                "open": opt_price(rec.get("o")),
                "high": opt_price(rec.get("h")),
                "low": opt_price(rec.get("l")),
                "upper": opt_price(rec.get("up")),
                "lower": opt_price(rec.get("dn")),
                "pre_settle": opt_price(rec.get("ps")),
                "pre_close": opt_price(rec.get("pc")),
                "pre_oi": None,
                "avg": None,
            }
        except (KeyError, TypeError, ValueError) as exc:
            raise NormalizeError(
                MD_REJECT_MALFORMED, str(exc), instrument_id=meta.instrument_id, exchange=str(meta.exchange)
            ) from exc
        return self.build(
            meta,
            exch_ns=t_ms * NS_PER_MS,
            recv_ns=recv_ns,
            source=SourceId.FEED_B,
            seq=None,
            vendor_td=vendor_td,
            vendor_td_rule="not_older",
            **fields,
        )

    # ------------------------------------------------------------------ common
    def build(
        self,
        meta: InstrumentMeta,
        *,
        exch_ns: int,
        recv_ns: int,
        source: SourceId,
        seq: int | None,
        last: float,
        volume: int,
        turnover: float,
        open_interest: int,
        bid1: float | None,
        ask1: float | None,
        bid_vol1: int,
        ask_vol1: int,
        open: float | None,
        high: float | None,
        low: float | None,
        upper: float | None,
        lower: float | None,
        pre_settle: float | None,
        pre_close: float | None,
        pre_oi: int | None,
        avg: float | None,
        vendor_td: str | None = None,
        vendor_td_rule: str = "equal",
        coarse_ts: bool = False,
    ) -> Tick:
        """Validate the structural fields and assemble the canonical tick."""
        return self._make(
            meta,
            exch_ns,
            recv_ns,
            source,
            seq,
            last,
            volume,
            turnover,
            open_interest,
            bid1,
            ask1,
            bid_vol1,
            ask_vol1,
            open,
            high,
            low,
            upper,
            lower,
            pre_settle,
            pre_close,
            pre_oi,
            avg,
            vendor_td or "",
            vendor_td_rule == "equal",
            coarse_ts,
        )

    def _make(
        self,
        meta: InstrumentMeta,
        exch_ns: int,
        recv_ns: int,
        source: SourceId,
        seq: int | None,
        last: float,
        volume: int,
        turnover: float,
        open_interest: int,
        bid1: float | None,
        ask1: float | None,
        bid_vol1: int,
        ask_vol1: int,
        open_: float | None,
        high: float | None,
        low: float | None,
        upper: float | None,
        lower: float | None,
        pre_settle: float | None,
        pre_close: float | None,
        pre_oi: int | None,
        avg: float | None,
        vendor_td: str,
        td_equal: bool,
        coarse_ts: bool,
    ) -> Tick:
        iid = meta.instrument_id
        if not last > 0 or turnover < 0:
            raise NormalizeError(MD_REJECT_MALFORMED, f"last={last} turnover={turnover}", instrument_id=iid, exchange=str(meta.exchange))
        if pre_settle is None:
            raise NormalizeError(MD_REJECT_MALFORMED, "pre-settlement price missing", instrument_id=iid, exchange=str(meta.exchange))
        sessions = self._sessions
        td = sessions.trading_day(exch_ns)
        action_day = sessions.bj_date(exch_ns)
        if upper is None or lower is None:
            # 02 §7.1：数据源未提供涨跌停价时，按昨结算 ×(1 ± 当日生效的涨跌停幅度) 向内取整
            lkey = (iid, td or action_day, pre_settle)
            lim = self._limits.get(lkey)
            if lim is None:
                lim = derive_limits(pre_settle, meta.limit_pct_on(td), meta.tick_size)
                if len(self._limits) >= _LIMIT_CACHE_MAX:
                    self._limits.clear()
                self._limits[lkey] = lim
            upper, lower = lim
        flags = 0
        if ask1 is None or bid1 is None:
            flags = lock_flags(bid1, ask1, upper, lower)
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
            instrument_id=iid,
            exchange=meta.exchange,
            trading_day=day,
            action_day=action_day,
            exch_ts_ns=exch_ns,
            last=last,
            volume=volume,
            turnover=turnover,
            open_interest=open_interest,
            bid1=bid1,
            ask1=ask1,
            bid_vol1=bid_vol1,
            ask_vol1=ask_vol1,
            open=open_,
            high=high,
            low=low,
            upper_limit=float(upper),
            lower_limit=float(lower),
            pre_settle=pre_settle,
            pre_close=pre_close,
            pre_open_interest=pre_oi,
            avg_price=avg,
            source=source,
            source_seq=seq,
            recv_ns=recv_ns,
            fingerprint=fast_fingerprint(day, volume, turnover, open_interest, last, bid1, ask1, bid_vol1, ask_vol1),
            flags=flags,
        )
        # 02 §9.1/§9.2：交易日由 md-gateway 按日历重算，数据源给出的交易日只作校验
        if td is None:
            raise NormalizeError(MD_REJECT_TRADING_DAY, "exchange time is not in a trading day", instrument_id=iid, exchange=str(meta.exchange), tick=tick)
        if vendor_td:
            if td_equal and vendor_td != td:
                raise NormalizeError(
                    MD_REJECT_TRADING_DAY, f"vendor trading day {vendor_td} != calendar {td}", instrument_id=iid, exchange=str(meta.exchange), tick=tick
                )
            if not td_equal and td > vendor_td:
                raise NormalizeError(
                    MD_REJECT_TRADING_DAY, f"state of {td} is newer than the vendor day {vendor_td}", instrument_id=iid, exchange=str(meta.exchange), tick=tick
                )
        if exch_ns - recv_ns > self._future_ns:
            # 02 §11.3：交易所时间戳比收到时刻晚得多（本机时钟偏慢或日期字段异常）
            raise NormalizeError(
                MD_REJECT_FUTURE_TS,
                f"stamped {(exch_ns - recv_ns) / NS_PER_S:.0f}s after the receive time",
                instrument_id=iid,
                exchange=str(meta.exchange),
                tick=tick,
            )
        return tick


def _opt_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(f) or f < 0 or f >= _NO_VALUE:
        return None
    return int(round(f))


LIMIT_LOCK_FLAGS = _F_UP | _F_DOWN

__all__ = ["LIMIT_LOCK_FLAGS", "NormalizeError", "Normalizer", "fast_fingerprint", "lock_flags", "opt_price"]
