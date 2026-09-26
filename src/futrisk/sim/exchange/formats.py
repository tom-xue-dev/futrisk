"""Wire formats of the two simulated market-data feeds.

- :func:`feed_a_tick`: ``FeedATick`` with CTP ``CThostFtdcDepthMarketDataField`` field
  names (02 §9.1). ``TradingDay`` is the exchange trading day (night session belongs to
  the next trading day), ``ActionDay`` the natural Beijing date of ``UpdateTime``;
  missing prices are ``DBL_MAX`` exactly like CTP; ``Turnover`` includes the multiplier
  and ``AveragePrice`` is already divided by it.
- :func:`feed_b_record`: vendor-style record of ``GET /v1/quotes`` (02 §9.2): upper-case
  symbol, prices as decimal strings, epoch-millisecond timestamp.

Both are rendered from the same :class:`~futrisk.sim.exchange.engine.InstrumentState`, so
an identical state normalises to the same fingerprint on either feed.
"""

from __future__ import annotations

import sys
from datetime import datetime
from typing import Any

import msgspec

from futrisk.common.clock import BJ, NS_PER_MS, NS_PER_S
from futrisk.common.decimals import dec_str, dec_str_strip
from futrisk.sim.exchange.engine import InstrumentMeta, InstrumentState

#: CTP's "no value" marker for double fields (02 §9.1).
DBL_MAX = sys.float_info.max

_encoder = msgspec.json.Encoder()


def dumps(obj: Any) -> str:
    """Compact JSON text (WebSocket text frames must carry ``str``)."""
    return _encoder.encode(obj).decode("utf-8")


def dumps_bytes(obj: Any) -> bytes:
    return _encoder.encode(obj)


def _pf(meta: InstrumentMeta, ticks: int | None) -> float:
    return DBL_MAX if ticks is None else float(meta.price(ticks))


def _ps(meta: InstrumentMeta, ticks: int | None) -> str | None:
    return None if ticks is None else format(meta.price(ticks), "f")


def bj_parts(ts_ns: int) -> tuple[str, str, int]:
    """``(YYYYMMDD, HH:MM:SS, millis)`` of a UTC-ns timestamp in Beijing time."""
    sec, rem = divmod(ts_ns, NS_PER_S)
    dt = datetime.fromtimestamp(sec, tz=BJ)
    return dt.strftime("%Y%m%d"), dt.strftime("%H:%M:%S"), rem // NS_PER_MS


def feed_a_tick(st: InstrumentState) -> dict[str, Any]:
    """Render a state as a ``FeedATick`` (02 §9.1 field table)."""
    m = st.meta
    action_day, update_time, millis = bj_parts(st.ts_ns)
    vwap = st.vwap()
    return {
        "InstrumentID": m.instrument_id,
        "ExchangeID": m.exchange,
        "TradingDay": st.trading_day,
        "ActionDay": action_day,
        "UpdateTime": update_time,
        "UpdateMillisec": millis,
        "LastPrice": float(m.price(st.last)),
        "Volume": st.volume,
        "Turnover": float(st.turnover),
        "OpenInterest": float(st.open_interest),
        "BidPrice1": _pf(m, st.bid),
        "BidVolume1": st.bid_vol,
        "AskPrice1": _pf(m, st.ask),
        "AskVolume1": st.ask_vol,
        "OpenPrice": _pf(m, st.open),
        "HighestPrice": _pf(m, st.high),
        "LowestPrice": _pf(m, st.low),
        "UpperLimitPrice": float(m.price(st.upper)),
        "LowerLimitPrice": float(m.price(st.lower)),
        "PreSettlementPrice": float(st.pre_settle),
        "PreClosePrice": float(st.pre_close),
        "PreOpenInterest": float(st.pre_open_interest),
        "AveragePrice": float(vwap) if vwap is not None else 0.0,
    }


def feed_b_record(st: InstrumentState) -> dict[str, Any]:
    """Render a state as a vendor record of ``GET /v1/quotes`` (02 §9.2)."""
    m = st.meta
    return {
        "sym": m.instrument_id.upper(),
        "ex": m.exchange,
        "px": _ps(m, st.last),
        "bp": _ps(m, st.bid),
        "ap": _ps(m, st.ask),
        "bv": st.bid_vol,
        "av": st.ask_vol,
        "vol": st.volume,
        "amt": dec_str_strip(st.turnover),
        "oi": st.open_interest,
        "up": _ps(m, st.upper),
        "dn": _ps(m, st.lower),
        "ps": dec_str(st.pre_settle),
        "pc": dec_str(st.pre_close),
        "o": _ps(m, st.open),
        "h": _ps(m, st.high),
        "l": _ps(m, st.low),
        "t": st.ts_ns // NS_PER_MS,
    }


def corrupted_state(st: InstrumentState, offset_pct: float, with_volume: bool, ts_ns: int) -> InstrumentState:
    """A bad tick for the ``BAD_TICK`` fault (02 §9.5): last price moved by ``offset_pct``.

    The price stays on the tick grid (so it tests spike handling, not the grid check)
    but is not clamped to the limits. ``with_volume`` adds one lot so the bad price
    comes "with a trade"; otherwise it violates "price only moves with volume".
    """
    bad = st.copy()
    ticks = max(1, round(st.last * (1.0 + offset_pct)))
    if ticks == st.last:
        ticks = st.last + (1 if offset_pct >= 0 else -1)
    bad.last = max(1, ticks)
    if with_volume:
        bad.volume += 1
        bad.turnover_ticks += bad.last
    bad.ts_ns = ts_ns
    return bad
