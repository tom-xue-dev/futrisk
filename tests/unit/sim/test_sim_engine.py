"""Price engine of the virtual exchange: grid, limits, volume rules, sessions, shocks, determinism."""

from __future__ import annotations

import math
from datetime import datetime
from decimal import Decimal

import numpy as np
import pytest

from futrisk.common.clock import BJ, bj_to_ns
from futrisk.common.config import FullConfig
from futrisk.common.decimals import limit_prices
from futrisk.messages.enums import LimitLock
from futrisk.sim.exchange.engine import (
    SAME_PRODUCT_CORRELATION,
    InstrumentState,
    PriceEngine,
    correlation_matrix,
)
from futrisk.sim.exchange.formats import DBL_MAX, feed_a_tick, feed_b_record

from .helpers import MARKET_OPEN, make_cfg


def _t(h: int, m: int, s: int = 0, day: int = 28) -> int:
    return bj_to_ns(datetime(2026, 9, day, h, m, s, tzinfo=BJ))


def _engine(base: FullConfig, start: int | None = None, **kw: object) -> PriceEngine:
    return PriceEngine(make_cfg(base, tick_ms=500, **kw), start if start is not None else bj_to_ns(MARKET_OPEN))  # type: ignore[arg-type]


def _run(eng: PriceEngine, start: int, steps: int) -> int:
    t = start - start % eng.interval_ns
    for _ in range(steps):
        t += eng.interval_ns
        eng.step(t)
    return t


def _on_grid(price: float, tick: Decimal) -> bool:
    q = Decimal(str(price)) / tick
    return q == q.to_integral_value()


# ---------------------------------------------------------------------------
# Invariants used by md-gateway's validator
# ---------------------------------------------------------------------------


class TestInvariants:
    def test_grid_limits_and_price_only_moves_with_volume(self, default_config: FullConfig) -> None:
        eng = _engine(default_config)
        prev = {iid: st.copy() for iid, st in eng.states.items()}
        t = bj_to_ns(MARKET_OPEN)
        seen_change = 0
        for _ in range(2500):
            t += eng.interval_ns
            for st in eng.step(t):
                p = prev[st.instrument_id]
                m = st.meta
                assert st.lower <= st.last <= st.upper
                d = feed_a_tick(st)
                for key in ("LastPrice", "UpperLimitPrice", "LowerLimitPrice"):
                    assert _on_grid(d[key], m.tick), (key, d[key])
                for key in ("BidPrice1", "AskPrice1"):
                    if d[key] != DBL_MAX:
                        assert _on_grid(d[key], m.tick)
                        assert d["LowerLimitPrice"] <= d[key] <= d["UpperLimitPrice"]
                if st.bid is not None and st.ask is not None:
                    assert st.bid < st.ask
                assert st.trading_day == p.trading_day
                assert st.ts_ns > p.ts_ns
                assert st.volume >= p.volume
                assert st.turnover_ticks >= p.turnover_ticks
                assert st.open_interest >= 1
                if st.last != p.last:
                    seen_change += 1
                    assert st.volume > p.volume, "last price changed without a trade"
                if st.volume > 0:
                    assert st.low is not None and st.high is not None
                    assert st.low <= st.last <= st.high
                    vwap = st.vwap()
                    assert vwap is not None and m.price(st.low) <= vwap <= m.price(st.high)
                prev[st.instrument_id] = st.copy()
        assert seen_change > 1000

    def test_turnover_includes_multiplier(self, default_config: FullConfig) -> None:
        eng = _engine(default_config)
        _run(eng, bj_to_ns(MARKET_OPEN), 50)
        st = eng.states["IF2612"]
        assert st.volume > 0
        vwap = float(st.turnover) / (st.volume * st.meta.multiplier)
        assert st.meta.price(st.low) <= Decimal(str(vwap)) <= st.meta.price(st.high)  # type: ignore[arg-type]
        assert feed_a_tick(st)["AveragePrice"] == pytest.approx(vwap)

    def test_feed_formats_agree(self, default_config: FullConfig) -> None:
        eng = _engine(default_config)
        _run(eng, bj_to_ns(MARKET_OPEN), 20)
        st = eng.states["au2612"]
        a, b = feed_a_tick(st), feed_b_record(st)
        assert b["sym"] == "AU2612" and a["InstrumentID"] == "au2612"
        assert Decimal(b["px"]) == Decimal(str(a["LastPrice"]))
        assert b["vol"] == a["Volume"] and Decimal(b["amt"]) == Decimal(str(a["Turnover"]))
        assert b["t"] * 1_000_000 == st.ts_ns
        assert a["UpdateMillisec"] in (0, 500)
        assert a["TradingDay"] == a["ActionDay"] == "20260928"


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------


class TestDeterminism:
    def test_same_seed_same_path(self, default_config: FullConfig) -> None:
        a, b = _engine(default_config), _engine(default_config)
        _run(a, bj_to_ns(MARKET_OPEN), 300)
        _run(b, bj_to_ns(MARKET_OPEN), 300)
        assert a.dump_states() == b.dump_states()

    def test_other_seed_other_path(self, default_config: FullConfig) -> None:
        a, c = _engine(default_config), _engine(default_config, seed=7)
        _run(a, bj_to_ns(MARKET_OPEN), 300)
        _run(c, bj_to_ns(MARKET_OPEN), 300)
        assert a.prices() != c.prices()

    def test_restored_engine_continues_the_same_stream(self, default_config: FullConfig) -> None:
        a = _engine(default_config)
        t = _run(a, bj_to_ns(MARKET_OPEN), 100)
        b = PriceEngine(make_cfg(default_config, tick_ms=500), t, restored=a.dump_states(),
                        rng_td=a.rng_td, rng_step=a.rng_step)
        assert b.dump_states() == a.dump_states()
        for _ in range(100):
            t += a.interval_ns
            a.step(t)
            b.step(t)
        assert b.dump_states() == a.dump_states()

    def test_state_dict_round_trip(self, default_config: FullConfig) -> None:
        eng = _engine(default_config)
        _run(eng, bj_to_ns(MARKET_OPEN), 30)
        st = eng.states["T2612"]
        back = InstrumentState.from_dict(st.meta, st.to_dict())
        assert back.to_dict() == st.to_dict()


# ---------------------------------------------------------------------------
# Sessions and trading days
# ---------------------------------------------------------------------------


class TestSessions:
    def test_only_continuous_sessions_step(self, default_config: FullConfig) -> None:
        eng = _engine(default_config)
        assert eng.step(_t(12, 0)) == []  # lunch break for everybody
        assert {s.instrument_id for s in eng.step(_t(10, 20))} == {"IF2612", "IH2612", "IC2612", "T2612"}
        commodities = {s.instrument_id for s in eng.step(_t(9, 27))}  # CFFEX opening auction
        assert commodities and not commodities & {"IF2612", "IC2612", "T2612"}
        assert {s.instrument_id for s in eng.step(_t(15, 10))} == {"T2612"}  # T closes 15:15
        assert eng.step(_t(15, 20)) == []

    def test_night_session_rolls_the_trading_day(self, default_config: FullConfig) -> None:
        eng = _engine(default_config)
        _run(eng, bj_to_ns(MARKET_OPEN), 200)
        rb = eng.states["rb2701"]
        expected_settle = eng.settlement_price(rb)
        last_px = rb.meta.price(rb.last)
        oi = rb.open_interest
        # SHFE: settlement = full-day VWAP floored to the tick (01 §8.15)
        vwap_ticks = Decimal(rb.turnover_ticks) / rb.volume
        assert expected_settle == rb.meta.price(int(math.floor(vwap_ticks)))
        changed = {s.instrument_id for s in eng.step(_t(21, 0, 1))}  # night session of 2026-09-29
        assert "rb2701" in changed and "IF2612" not in changed
        assert rb.trading_day == "20260929"
        assert rb.pre_settle == expected_settle and rb.pre_close == last_px and rb.pre_open_interest == oi
        assert rb.volume < 200  # cumulative volume restarted
        up = int((expected_settle * Decimal("1.05")) // 1)
        assert rb.meta.price(rb.upper) == Decimal(up)
        assert eng.states["IF2612"].trading_day == "20260928"
        a = feed_a_tick(rb)
        assert a["TradingDay"] == "20260929" and a["ActionDay"] == "20260928"

    def test_cffex_settles_on_the_last_hour(self, default_config: FullConfig) -> None:
        eng = _engine(default_config, start=_t(13, 50))
        _run(eng, _t(13, 50), 20 * 60 * 2)  # 13:50 -> 14:10
        st = eng.states["IF2612"]
        assert st.win_volume is not None and 0 < st.win_volume < st.volume
        v = st.volume - st.win_volume
        tv = st.turnover_ticks - (st.win_turnover_ticks or 0)
        q = (Decimal(tv) / v).quantize(Decimal(1), rounding="ROUND_HALF_UP")
        assert eng.settlement_price(st) == st.meta.price(int(q))


# ---------------------------------------------------------------------------
# Correlation, shocks and limit locks
# ---------------------------------------------------------------------------


class TestShocksAndLimits:
    def test_correlation_blocks(self, default_config: FullConfig) -> None:
        eng = _engine(default_config)
        ids = eng.instrument_ids
        c = correlation_matrix(eng.metas, 0.6, 0.2)
        assert c[ids.index("rb2701"), ids.index("rb2705")] == SAME_PRODUCT_CORRELATION
        assert c[ids.index("rb2701"), ids.index("hc2701")] == 0.6
        assert c[ids.index("rb2701"), ids.index("IF2612")] == 0.2
        chol = eng._chol  # noqa: SLF001 - checking the factor reproduces the matrix
        assert np.allclose(chol @ chol.T, c)

    def test_step_shock_moves_the_sector(self, default_config: FullConfig) -> None:
        eng = _engine(default_config)
        t = _run(eng, bj_to_ns(MARKET_OPEN), 5)
        before = eng.prices()
        eng.add_shock("s1", sector="black", pct=-0.03, shape="STEP")
        eng.step(t + eng.interval_ns)
        after = eng.prices()
        for iid in ("rb2701", "rb2705", "hc2701", "i2701", "j2701"):
            assert after[iid] / before[iid] - 1 == pytest.approx(-0.03, abs=0.004), iid
        for iid in ("cu2611", "IF2612", "m2701"):
            assert abs(after[iid] / before[iid] - 1) < 0.004, iid
        assert eng.shocks["s1"].done

    def test_linear_shock_spreads_over_duration(self, default_config: FullConfig) -> None:
        eng = _engine(default_config)
        t = _run(eng, bj_to_ns(MARKET_OPEN), 5)
        x0 = eng.states["hc2701"].x
        eng.add_shock("s2", instruments=["hc2701"], pct=-0.02, duration_s=10, shape="LINEAR")
        assert eng.shocks["s2"].n_steps == 20
        for _ in range(10):
            t += eng.interval_ns
            eng.step(t)
        assert not eng.shocks["s2"].done
        mid = eng.states["hc2701"].x - x0
        for _ in range(10):
            t += eng.interval_ns
            eng.step(t)
        assert eng.shocks["s2"].done
        full = eng.states["hc2701"].x - x0
        assert mid == pytest.approx(math.log(0.98) / 2, abs=0.004)
        assert full == pytest.approx(math.log(0.98), abs=0.005)

    def test_limit_lock_gives_a_one_sided_book(self, default_config: FullConfig) -> None:
        eng = _engine(default_config)
        t = _run(eng, bj_to_ns(MARKET_OPEN), 5)
        eng.add_shock("down", instruments=["rb2701"], pct=-0.2)
        eng.add_shock("up", instruments=["IF2612"], pct=0.3)
        vol_before = eng.states["rb2701"].volume
        t += eng.interval_ns
        eng.step(t)
        rb, if_ = eng.states["rb2701"], eng.states["IF2612"]
        assert rb.lock is LimitLock.DOWN and rb.last == rb.lower
        assert rb.bid is None and rb.ask == rb.lower and rb.ask_vol > 0 and rb.bid_vol == 0
        assert rb.volume > vol_before  # the move to the limit came with a trade
        assert if_.lock is LimitLock.UP and if_.last == if_.upper and if_.ask is None and if_.bid == if_.upper
        d = feed_a_tick(rb)
        assert d["BidPrice1"] == DBL_MAX and d["AskPrice1"] == d["LowerLimitPrice"]
        for _ in range(20):  # stays locked, never trades beyond the limit
            t += eng.interval_ns
            eng.step(t)
            assert rb.last == rb.lower and rb.bid is None

    def test_bad_shock_arguments(self, default_config: FullConfig) -> None:
        eng = _engine(default_config)
        with pytest.raises(ValueError, match="ratio"):
            eng.add_shock("x", pct=-4)
        with pytest.raises(ValueError, match="unknown sector"):
            eng.add_shock("x", sector="nope", pct=-0.01)
        with pytest.raises(ValueError, match="unknown instrument"):
            eng.add_shock("x", instruments=["zz9999"], pct=-0.01)

    def test_frozen_instrument_does_not_move(self, default_config: FullConfig) -> None:
        eng = _engine(default_config)
        t = _run(eng, bj_to_ns(MARKET_OPEN), 3)
        eng.frozen = {"m2701"}
        snap = eng.states["m2701"].to_dict()
        for _ in range(10):
            t += eng.interval_ns
            assert "m2701" not in {s.instrument_id for s in eng.step(t)}
        assert eng.states["m2701"].to_dict() == snap



class TestLimitScheduleAndRoll:
    def test_limit_schedule_follows_the_trading_day(self, default_config: FullConfig) -> None:
        # products.yaml: DCE widens the i/m limits from 6% to 8% on 20260930 (holiday notice)
        eng = _engine(default_config, start=_t(10, 0, day=29))
        i, rb = eng.states["i2701"], eng.states["rb2701"]
        assert i.trading_day == "20260929"
        assert (i.meta.price(i.upper), i.meta.price(i.lower)) == limit_prices(
            Decimal("780.0"), Decimal("0.06"), Decimal("0.5"))
        _run(eng, _t(10, 0, day=29), 20)
        assert eng.step(_t(20, 57, day=29)) == []  # opening auction of 20260930: rolled, nothing published
        assert i.trading_day == "20260930" and i.volume == 0
        assert (i.meta.price(i.upper), i.meta.price(i.lower)) == limit_prices(
            i.pre_settle, Decimal("0.08"), Decimal("0.5"))
        assert (rb.meta.price(rb.upper), rb.meta.price(rb.lower)) == limit_prices(
            rb.pre_settle, Decimal("0.05"), Decimal("1"))  # rb has no schedule
        back = InstrumentState.from_dict(i.meta, i.to_dict())  # a restored 20260930 state keeps 8%
        assert (back.upper, back.lower) == (i.upper, i.lower)

    def test_sync_day_rolls_when_the_first_window_opens(self, default_config: FullConfig) -> None:
        eng = _engine(default_config, start=_t(14, 0))
        _run(eng, _t(14, 0), 20)
        assert eng.sync_day("rb2701", _t(20, 50)).trading_day == "20260928"  # before the night auction
        st = eng.sync_day("rb2701", _t(20, 56))  # night auction of 20260929
        assert st.trading_day == "20260929" and st.volume == 0
        assert eng.sync_day("rb2701", _t(20, 58)) is st and st.trading_day == "20260929"  # idempotent
        assert eng.sync_day("IF2612", _t(20, 56)).trading_day == "20260928"  # CFFEX opens at 09:25
