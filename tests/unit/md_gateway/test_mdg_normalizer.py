"""Normalizer: Feed A / Feed B records -> canonical Tick (02 §7.1, §9.1, §9.2)."""

from __future__ import annotations

import sys
from datetime import datetime
from typing import Any

import pytest

from futrisk.common.clock import BJ, NS_PER_MS, bj_to_ns
from futrisk.messages.codes import MD_REJECT_FUTURE_TS, MD_REJECT_MALFORMED, MD_REJECT_TRADING_DAY
from futrisk.messages.enums import Exchange, SourceId
from futrisk.messages.md import FLAG_AUCTION, FLAG_LIMIT_UP_LOCKED
from futrisk.services.md_gateway.normalizer import NormalizeError, Normalizer

DBL_MAX = sys.float_info.max


def feed_a(**over: Any) -> dict[str, Any]:
    d: dict[str, Any] = {
        "InstrumentID": "rb2701",
        "ExchangeID": "SHFE",
        "TradingDay": "20260928",
        "ActionDay": "20260928",
        "UpdateTime": "09:35:01",
        "UpdateMillisec": 500,
        "LastPrice": 3118.0,
        "Volume": 570630,
        "Turnover": 17771239560.0,
        "OpenInterest": 1615888.0,
        "BidPrice1": 3117.0,
        "BidVolume1": 12,
        "AskPrice1": 3118.0,
        "AskVolume1": 5,
        "OpenPrice": 3115.0,
        "HighestPrice": 3120.0,
        "LowestPrice": 3100.0,
        "UpperLimitPrice": 3276.0,
        "LowerLimitPrice": 2964.0,
        "PreSettlementPrice": 3120.0,
        "PreClosePrice": 3118.0,
        "PreOpenInterest": 1600000.0,
        "AveragePrice": 3114.3,
    }
    d.update(over)
    return d


def feed_b(**over: Any) -> dict[str, Any]:
    ts = bj_to_ns(datetime(2026, 9, 28, 9, 35, 1, 500_000, tzinfo=BJ)) // NS_PER_MS
    rec: dict[str, Any] = {
        "sym": "RB2701",
        "ex": "SHFE",
        "px": "3118",
        "bp": "3117",
        "ap": "3118",
        "bv": 12,
        "av": 5,
        "vol": 570630,
        "amt": "17771239560",
        "oi": 1615888,
        "up": "3276",
        "dn": "2964",
        "ps": "3120",
        "pc": "3118",
        "o": "3115",
        "h": "3120",
        "l": "3100",
        "t": ts,
    }
    rec.update(over)
    return rec


RECV = bj_to_ns(datetime(2026, 9, 28, 9, 35, 1, 520_000, tzinfo=BJ))


def test_feed_a_maps_ctp_fields(normalizer: Normalizer) -> None:
    t = normalizer.feed_a(feed_a(), recv_ns=RECV, seq=42)
    assert t.instrument_id == "rb2701" and t.exchange == Exchange.SHFE
    assert t.trading_day == "20260928" and t.action_day == "20260928"
    assert t.exch_ts_ns == bj_to_ns(datetime(2026, 9, 28, 9, 35, 1, 500_000, tzinfo=BJ))
    assert t.open_interest == 1615888 and isinstance(t.open_interest, int)
    assert t.pre_open_interest == 1600000
    assert t.source == SourceId.FEED_A and t.source_seq == 42 and t.recv_ns == RECV
    assert t.fingerprint != 0 and t.flags == 0


def test_feed_a_and_feed_b_of_the_same_state_share_order_key_and_fingerprint(normalizer: Normalizer) -> None:
    a = normalizer.feed_a(feed_a(), recv_ns=RECV, seq=1)
    b = normalizer.feed_b(feed_b(), vendor_td="20260928", recv_ns=RECV)
    assert b.instrument_id == "rb2701"  # RB2701 -> exchange case (README §3.3)
    assert (a.trading_day, a.volume, a.exch_ts_ns) == (b.trading_day, b.volume, b.exch_ts_ns)
    assert a.fingerprint == b.fingerprint


def test_dbl_max_means_no_value_and_limit_lock_flag(normalizer: Normalizer) -> None:
    t = normalizer.feed_a(
        feed_a(AskPrice1=DBL_MAX, AskVolume1=0, BidPrice1=3276.0, LastPrice=3276.0, OpenPrice=DBL_MAX), recv_ns=RECV, seq=1
    )
    assert t.ask1 is None and t.open is None
    assert t.flags & FLAG_LIMIT_UP_LOCKED


def test_night_session_belongs_to_next_trading_day(normalizer: Normalizer) -> None:
    # Friday 2026-09-25 21:30 -> Monday 2026-09-28
    recv = bj_to_ns(datetime(2026, 9, 25, 21, 30, 0, 100_000, tzinfo=BJ))
    t = normalizer.feed_a(feed_a(TradingDay="20260928", ActionDay="20260925", UpdateTime="21:30:00", UpdateMillisec=0), recv_ns=recv, seq=1)
    assert t.trading_day == "20260928" and t.action_day == "20260925"


def test_vendor_trading_day_mismatch_is_rejected_with_the_tick_kept(normalizer: Normalizer) -> None:
    with pytest.raises(NormalizeError) as ei:
        normalizer.feed_a(feed_a(TradingDay="20260925"), recv_ns=RECV, seq=1)
    assert ei.value.reason == MD_REJECT_TRADING_DAY
    assert ei.value.tick is not None and ei.value.tick.trading_day == "20260928"


def test_timestamp_outside_trading_days_is_rejected(normalizer: Normalizer) -> None:
    sat = bj_to_ns(datetime(2026, 9, 26, 10, 0, tzinfo=BJ)) // NS_PER_MS
    with pytest.raises(NormalizeError) as ei:
        normalizer.feed_b(feed_b(t=sat), vendor_td="20260928", recv_ns=RECV)
    assert ei.value.reason == MD_REJECT_TRADING_DAY


def test_feed_b_state_newer_than_vendor_day_is_rejected(normalizer: Normalizer) -> None:
    with pytest.raises(NormalizeError) as ei:
        normalizer.feed_b(feed_b(), vendor_td="20260925", recv_ns=RECV)
    assert ei.value.reason == MD_REJECT_TRADING_DAY


def test_future_timestamp(normalizer: Normalizer) -> None:
    early_recv = RECV - 600 * 1_000_000_000
    with pytest.raises(NormalizeError) as ei:
        normalizer.feed_a(feed_a(), recv_ns=early_recv, seq=1)
    assert ei.value.reason == MD_REJECT_FUTURE_TS


def test_malformed_values(normalizer: Normalizer) -> None:
    with pytest.raises(NormalizeError) as ei:
        normalizer.feed_b(feed_b(px="N/A"), vendor_td="20260928", recv_ns=RECV)
    assert ei.value.reason == MD_REJECT_MALFORMED and ei.value.instrument_id == "rb2701"
    with pytest.raises(NormalizeError) as ei:
        normalizer.feed_b(feed_b(vol=-1), vendor_td="20260928", recv_ns=RECV)
    assert ei.value.reason == MD_REJECT_MALFORMED
    with pytest.raises(NormalizeError) as ei:
        normalizer.feed_b(feed_b(sym="ZZ2701"), vendor_td="20260928", recv_ns=RECV)
    assert ei.value.reason == MD_REJECT_MALFORMED


def test_missing_limits_are_derived_with_the_date_effective_band(normalizer: Normalizer) -> None:
    # DCE i: 6% normally, 8% from trading day 20260930 (products.yaml limit_schedule)
    base = {"sym": "I2701", "ex": "DCE", "px": "780.0", "ps": "780.0", "up": None, "dn": None, "amt": "0", "vol": 0}
    t = normalizer.feed_b(feed_b(**base), vendor_td="20260928", recv_ns=RECV)
    assert (t.upper_limit, t.lower_limit) == (826.5, 733.5)  # 780 x 1.06 = 826.8 -> 826.5 inward
    ts30 = bj_to_ns(datetime(2026, 9, 30, 9, 35, tzinfo=BJ)) // NS_PER_MS
    recv30 = ts30 * NS_PER_MS + 10 * NS_PER_MS
    t30 = normalizer.feed_b(feed_b(**base, t=ts30), vendor_td="20260930", recv_ns=recv30)
    assert (t30.upper_limit, t30.lower_limit) == (842.0, 718.0)  # 780 x 1.08 = 842.4 -> 842.0; x 0.92 = 717.6 -> 718.0


def test_auction_flag(normalizer: Normalizer) -> None:
    ts = bj_to_ns(datetime(2026, 9, 28, 9, 27, tzinfo=BJ))  # IF opening auction 09:25-09:30
    rec = feed_b(sym="IF2612", ex="CFFEX", px="4414.8", bp="4414.6", ap="4414.8", ps="4414.8", up="4856.2", dn="3973.4", amt="0", vol=0, t=ts // NS_PER_MS)
    t = normalizer.feed_b(rec, vendor_td="20260928", recv_ns=ts + 10 * NS_PER_MS)
    assert t.flags & FLAG_AUCTION


# ---------------------------------------------------------------- round 2 (L7, M6)
def test_dce_night_action_day_quirk_is_repaired_not_rejected(normalizer: Normalizer) -> None:
    """L7: CTP DCE night ticks carry ActionDay = TradingDay; the natural date comes from the calendar."""
    evening = feed_a(InstrumentID="m2701", ExchangeID="DCE", TradingDay="20260929", ActionDay="20260929", UpdateTime="21:05:00",
                     UpdateMillisec=0, LastPrice=3000.0, UpperLimitPrice=3240.0, LowerLimitPrice=2760.0, PreSettlementPrice=3000.0,
                     BidPrice1=2999.0, AskPrice1=3000.0, OpenPrice=3000.0, HighestPrice=3001.0, LowestPrice=2999.0, AveragePrice=3000.0)
    recv = bj_to_ns(datetime(2026, 9, 28, 21, 5, 0, 100_000, tzinfo=BJ))
    t = normalizer.feed_a(evening, recv_ns=recv, seq=1)
    assert t.trading_day == "20260929" and t.action_day == "20260928"
    assert t.exch_ts_ns == bj_to_ns(datetime(2026, 9, 28, 21, 5, tzinfo=BJ))
    # the same repair on the typed fast path
    import msgspec

    from futrisk.services.md_gateway.wire import CtpTick

    c = msgspec.convert(evening, CtpTick)
    assert normalizer.feed_a_struct(c, recv_ns=recv, seq=1) == t
    # a genuinely wrong trading day is still rejected
    with pytest.raises(NormalizeError) as err:
        normalizer.feed_a(feed_a(TradingDay="20260930", ActionDay="20260928"), recv_ns=bj_to_ns(datetime(2026, 9, 28, 9, 35, 2, tzinfo=BJ)), seq=2)
    assert err.value.reason == MD_REJECT_TRADING_DAY


def test_typed_fast_path_equals_the_dict_path(normalizer: Normalizer) -> None:
    """M6: feed_a_struct (typed frames) builds exactly the tick of the generic dict path."""
    import msgspec

    from futrisk.services.md_gateway.wire import CtpTick

    recv = bj_to_ns(datetime(2026, 9, 28, 9, 35, 2, tzinfo=BJ))
    for over in ({}, {"BidPrice1": DBL_MAX, "AskVolume1": 0}, {"UpperLimitPrice": None, "LowerLimitPrice": None}, {"PreOpenInterest": None}):
        d = feed_a(**over)
        c = msgspec.convert({k: v for k, v in d.items()}, CtpTick)
        assert normalizer.feed_a_struct(c, recv_ns=recv, seq=7) == normalizer.feed_a(d, recv_ns=recv, seq=7)
    with pytest.raises(NormalizeError) as err:
        normalizer.feed_a_struct(msgspec.convert(feed_a(UpdateMillisec=1000), CtpTick), recv_ns=recv, seq=8)
    assert err.value.reason == MD_REJECT_MALFORMED


def test_fast_fingerprint_matches_the_shared_algorithm() -> None:
    import random

    from futrisk.marketdata.fingerprint import compute_fingerprint
    from futrisk.services.md_gateway.normalizer import fast_fingerprint

    rng = random.Random(7)
    for _ in range(500):
        args = dict(
            trading_day="20260928",
            volume=rng.randint(0, 10**7),
            turnover=rng.choice([0.0, -0.0, rng.random() * 1e11]),
            open_interest=rng.randint(0, 10**7),
            last=rng.choice([rng.random() * 1e5, 0.1 + 0.2]),
            bid1=rng.choice([None, rng.random() * 1e5]),
            ask1=rng.choice([None, rng.random() * 1e5]),
            bid_vol1=rng.randint(0, 999),
            ask_vol1=rng.randint(0, 999),
        )
        assert fast_fingerprint(**args) == compute_fingerprint(**args)  # type: ignore[arg-type]
