"""Settlement inputs from canonical ticks: full-day VWAP, CFFEX last hour with APPROX, hourly, limit lock (01 §8.15)."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

import pandas as pd
import pytest

from futrisk.common.clock import BJ, NS_PER_S, bj_to_ns
from futrisk.common.config import FullConfig
from futrisk.core.instruments import InstrumentRegistry
from futrisk.core.settle_price import determine_settlement_price
from futrisk.messages.enums import LimitLock, SettleInputQuality, SettleMethod, SettlePriceSource
from futrisk.messages.md import FLAG_LIMIT_UP_LOCKED
from futrisk.services.md_gateway.settle import compute_settle_input

TD = "20260928"


def at(h: int, m: int, s: float = 0.0) -> int:
    whole = int(s)
    return bj_to_ns(datetime(2026, 9, 28, h, m, whole, int((s - whole) * 1e6), tzinfo=BJ))


def frame(rows: list[dict[str, object]]) -> pd.DataFrame:
    base = {"bid1": None, "ask1": None, "pre_settle": 4414.8, "flags": 0, "mode": "LIVE"}
    return pd.DataFrame([{**base, **r} for r in rows])


@pytest.fixture(scope="module")
def registry(default_config: FullConfig) -> InstrumentRegistry:
    return InstrumentRegistry(default_config.products, default_config.calendar)


def if_state(ts: int, vol: int, px: float, mode: str = "LIVE", **kw: object) -> dict[str, object]:
    # turnover grows by px * dv * 300 per step
    return {"instrument_id": "IF2612", "exch_ts_ns": ts, "volume": vol, "last": px, "mode": mode, **kw}


def with_turnover(rows: list[dict[str, object]], mult: int) -> list[dict[str, object]]:
    turn = 0.0
    prev = 0
    for r in rows:
        dv = int(r["volume"]) - prev  # type: ignore[call-overload]
        turn += float(r["last"]) * dv * mult  # type: ignore[arg-type]
        r["turnover"] = turn
        prev = int(r["volume"])  # type: ignore[call-overload]
    return rows


def test_full_day_vwap_from_last_state(registry: InstrumentRegistry) -> None:
    rows = with_turnover(
        [
            {"instrument_id": "rb2701", "exch_ts_ns": at(21, 0, 1) - 3 * 86_400 * NS_PER_S, "volume": 100, "last": 3120.0},
            {"instrument_id": "rb2701", "exch_ts_ns": at(9, 30), "volume": 300, "last": 3130.0},
            {"instrument_id": "rb2701", "exch_ts_ns": at(14, 59, 59.5), "volume": 400, "last": 3110.0, "bid1": 3109.0, "ask1": 3110.0},
        ],
        10,
    )
    df = frame(rows)
    df["pre_settle"] = 3120.0
    item = compute_settle_input(df, instrument_id="rb2701", trading_day=TD, registry=registry, computed_ns=1, day_close_ns=at(15, 0))
    assert item is not None
    assert item.method == SettleMethod.VWAP_FULL_DAY and item.multiplier == 10 and item.tick_size == "1"
    assert item.day_volume == 400
    expected = (3120 * 100 + 3130 * 200 + 3110 * 100) / 400
    assert Decimal(item.vwap_raw or "0") == Decimal(str(expected))
    assert (item.last, item.bid1, item.ask1, item.pre_settle) == ("3110", "3109", "3110", "3120")
    assert item.quality == SettleInputQuality.OK and item.hourly is None and item.limit_lock == LimitLock.NONE
    view = determine_settlement_price(item, registry)
    assert view.source == SettlePriceSource.VWAP and view.price == "3122"  # 3122.5 floored (SHFE)


def test_last_hour_window_from_boundary_states(registry: InstrumentRegistry) -> None:
    rows = with_turnover(
        [
            if_state(at(9, 30, 0.5), 500, 4400.0),
            if_state(at(13, 59, 59.5), 1000, 4410.0),
            if_state(at(14, 0), 1010, 4412.0),
            if_state(at(14, 30), 1500, 4420.0),
            if_state(at(14, 59, 59.5), 2000, 4416.0),
        ],
        300,
    )
    item = compute_settle_input(frame(rows), instrument_id="IF2612", trading_day=TD, registry=registry, computed_ns=1, day_close_ns=at(15, 0))
    assert item is not None and item.method == SettleMethod.VWAP_LAST_HOUR
    assert item.window_start_ns == at(14, 0)
    assert item.window_volume == 1000
    w_turn = (4412.0 * 10 + 4420.0 * 490 + 4416.0 * 500) * 300
    assert Decimal(item.window_turnover or "0") == Decimal(str(w_turn))
    assert item.quality == SettleInputQuality.OK
    assert item.hourly is not None and sum(int(v[0]) for v in item.hourly.values()) == 2000
    view = determine_settlement_price(item, registry)
    assert view.source == SettlePriceSource.VWAP


def test_missing_boundary_state_is_approx(registry: InstrumentRegistry) -> None:
    rows = with_turnover(
        [
            if_state(at(13, 50), 1000, 4410.0),
            if_state(at(14, 10), 1100, 4412.0),  # 20 minutes without data around 14:00 while trades happened
            if_state(at(14, 59, 59.5), 2000, 4416.0),
        ],
        300,
    )
    item = compute_settle_input(frame(rows), instrument_id="IF2612", trading_day=TD, registry=registry, computed_ns=1, day_close_ns=at(15, 0))
    assert item is not None and item.quality == SettleInputQuality.APPROX and item.window_volume == 1000


def test_backfill_boundary_state_is_approx(registry: InstrumentRegistry) -> None:
    rows = with_turnover(
        [
            if_state(at(13, 59, 59.5), 1000, 4410.0, mode="BACKFILL"),
            if_state(at(14, 0), 1010, 4412.0),
            if_state(at(14, 59, 59.5), 2000, 4416.0),
        ],
        300,
    )
    item = compute_settle_input(frame(rows), instrument_id="IF2612", trading_day=TD, registry=registry, computed_ns=1, day_close_ns=at(15, 0))
    assert item is not None and item.quality == SettleInputQuality.APPROX


def test_intraday_request_is_approx_and_no_window_is_no_trade(registry: InstrumentRegistry) -> None:
    rows = with_turnover([if_state(at(13, 59, 59.5), 1000, 4410.0), if_state(at(14, 5), 1100, 4412.0)], 300)
    item = compute_settle_input(frame(rows), instrument_id="IF2612", trading_day=TD, registry=registry, computed_ns=1, day_close_ns=at(15, 0))
    assert item is not None and item.quality == SettleInputQuality.APPROX and item.window_volume == 100
    morning = with_turnover([if_state(at(9, 35), 100, 4410.0)], 300)
    item2 = compute_settle_input(frame(morning), instrument_id="IF2612", trading_day=TD, registry=registry, computed_ns=1, day_close_ns=at(15, 0))
    assert item2 is not None and item2.quality == SettleInputQuality.NO_TRADE and item2.window_volume == 0
    assert item2.vwap_raw is None and item2.day_volume == 100
    view = determine_settlement_price(item2, registry)
    assert view.source in (SettlePriceSource.FALLBACK_FULLDAY, SettlePriceSource.FALLBACK_HOURLY)


def test_no_trade_and_limit_lock(registry: InstrumentRegistry) -> None:
    rows = [
        {"instrument_id": "rb2701", "exch_ts_ns": at(14, 54), "volume": 0, "turnover": 0.0, "last": 3276.0, "bid1": 3276.0, "flags": FLAG_LIMIT_UP_LOCKED},
        {"instrument_id": "rb2701", "exch_ts_ns": at(14, 57), "volume": 0, "turnover": 0.0, "last": 3276.0, "bid1": 3276.0, "flags": FLAG_LIMIT_UP_LOCKED},
    ]
    df = frame(rows)
    df["pre_settle"] = 3120.0
    item = compute_settle_input(df, instrument_id="rb2701", trading_day=TD, registry=registry, computed_ns=1, day_close_ns=at(15, 0))
    assert item is not None and item.quality == SettleInputQuality.NO_TRADE and item.limit_lock == LimitLock.UP
    assert compute_settle_input(frame([]).head(0), instrument_id="rb2701", trading_day=TD, registry=registry, computed_ns=1) is None


# ---------------------------------------------------------------- round 2: M5 hourly, boundary tolerance, C1 outliers
def snapshots(iid: str, trades: list[tuple[int, float, int]], snap_times: list[int], mult: int) -> pd.DataFrame:
    """Cumulative snapshot states (as a feed would publish them) of a trade tape."""
    rows = []
    for ts in snap_times:
        done = [t for t in trades if t[0] <= ts]
        vol = sum(q for _, _, q in done)
        rows.append({"instrument_id": iid, "exch_ts_ns": ts, "volume": vol, "turnover": sum(p * q * mult for _, p, q in done),
                     "last": done[-1][1] if done else 4400.0})
    return frame(rows)


def test_hourly_buckets_sum_to_the_day_and_match_the_trade_tape_attribution(registry: InstrumentRegistry) -> None:
    """M5: increments go to the segment of the later state; closing snapshot -> segment just ended; auction -> first."""
    from futrisk.core.settle_price import trading_hour_buckets

    trades = [(at(9, 29, 0), 4400.0, 50),  # opening auction
              (at(9, 45), 4401.0, 10), (at(10, 29, 50.0), 4403.0, 4), (at(11, 29, 59.8), 4402.0, 7),
              (at(13, 30), 4405.0, 3), (at(14, 30), 4410.0, 20), (at(14, 59, 59.9), 4411.0, 5)]
    # the 11:30 and 15:00 closing snapshots report trades of the segment that just ended
    snaps = sorted({at(9, 29, 0.1), at(9, 45, 0.5), at(10, 29, 59.5), at(11, 30, 0.3), at(13, 30, 0.5), at(14, 30, 0.5), at(15, 0, 0.3)})
    item = compute_settle_input(snapshots("IF2612", trades, snaps, 300), instrument_id="IF2612", trading_day=TD,
                                registry=registry, computed_ns=1, day_close_ns=at(15, 0))
    assert item is not None and item.hourly is not None
    md = {int(k): int(v[0]) for k, v in item.hourly.items()}
    assert sum(md.values()) == item.day_volume == 99
    assert sum(Decimal(v[1]) for v in item.hourly.values()) == Decimal(item.day_turnover)
    core = {b.start_ns: b.volume for b in trading_hour_buckets(registry, "IF2612", TD, [(t, Decimal(str(p)), q) for t, p, q in trades])}
    assert md == core


def test_window_boundary_tolerance_is_one_feed_interval(registry: InstrumentRegistry) -> None:
    trades = [(at(13, 30), 4400.0, 10), (at(13, 59, 59.0), 4400.0, 30), (at(14, 0, 0.2), 4420.0, 30), (at(14, 30), 4420.0, 10)]
    far = [at(13, 30, 0.5), at(13, 59, 58.5), at(14, 0, 1.0), at(14, 30, 0.5), at(15, 0, 0.2)]
    item = compute_settle_input(snapshots("IF2612", trades, far, 300), instrument_id="IF2612", trading_day=TD,
                                registry=registry, computed_ns=1, day_close_ns=at(15, 0))
    assert item is not None and item.quality == SettleInputQuality.APPROX  # 1.5 s early and 60 lots straddle 14:00
    near = [at(13, 30, 0.5), at(13, 59, 59.6), at(14, 0, 1.0), at(14, 30, 0.5), at(15, 0, 0.2)]
    item = compute_settle_input(snapshots("IF2612", trades, near, 300), instrument_id="IF2612", trading_day=TD,
                                registry=registry, computed_ns=1, day_close_ns=at(15, 0))
    assert item is not None and item.quality == SettleInputQuality.OK and item.window_volume == 40
    # far from the boundary, but only 1 lot of 10k straddles it: not significant
    big = [(at(13, 30), 4400.0, 10), (at(13, 59, 59.0), 4400.0, 1), (at(14, 30), 4420.0, 10_000)]
    item = compute_settle_input(snapshots("IF2612", big, [at(13, 30, 0.5), at(13, 59, 58.0), at(14, 0, 1.0), at(14, 30, 0.5), at(15, 0, 0.2)], 300),
                                instrument_id="IF2612", trading_day=TD, registry=registry, computed_ns=1, day_close_ns=at(15, 0))
    assert item is not None and item.quality == SettleInputQuality.OK


def test_outlier_states_never_become_the_days_last_state(registry: InstrumentRegistry) -> None:
    """C1: day volume / turnover / last come from states corroborated by the ones around them."""
    rows = with_turnover(
        [
            {"instrument_id": "rb2701", "exch_ts_ns": at(9, 30), "volume": 300, "last": 3130.0},
            {"instrument_id": "rb2701", "exch_ts_ns": at(10, 0), "volume": 100_300, "last": 3130.0},  # volume outlier
            {"instrument_id": "rb2701", "exch_ts_ns": at(10, 0, 1.0), "volume": 5, "last": 3131.0},  # low outlier
            {"instrument_id": "rb2701", "exch_ts_ns": at(14, 59, 59.5), "volume": 400, "last": 3110.0},
        ],
        10,
    )
    rows[-1]["turnover"] = 3130.0 * 300 * 10 + 3110.0 * 100 * 10
    df = frame(rows)
    df["pre_settle"] = 3120.0
    item = compute_settle_input(df, instrument_id="rb2701", trading_day=TD, registry=registry, computed_ns=1, day_close_ns=at(15, 0))
    assert item is not None and item.day_volume == 400 and item.last == "3110"
    assert Decimal(item.vwap_raw or "0") == Decimal("3125")
