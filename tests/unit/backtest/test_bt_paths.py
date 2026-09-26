"""Path synthesis, trading timeline, bounding classification and the order-rule hit rate (hand-made bars)."""

from __future__ import annotations

import pytest
from btsupport import base, ts

from futrisk.backtest.paths import (
    CERTAIN,
    IMPOSSIBLE,
    UNCERTAIN,
    Leg,
    TickGrid,
    Timeline,
    bar_prices,
    classify,
    extreme_order,
    minute_bar_points,
    order_hit_rate,
    pnl_bounds,
    synthesize,
)
from futrisk.core.instruments import InstrumentRegistry

H, L = "H", "L"


class TestOrderRule:
    def test_open_near_high_goes_high_first(self) -> None:
        assert extreme_order(108, 110, 90, 95) == (H, L)

    def test_open_near_low_goes_low_first(self) -> None:
        assert extreme_order(92, 110, 90, 105) == (L, H)

    def test_tie_up_day_visits_the_low_first(self) -> None:
        assert extreme_order(100, 110, 90, 104) == (L, H)
        assert extreme_order(100, 110, 90, 96) == (H, L)

    def test_forced_orders(self) -> None:
        assert extreme_order(92, 110, 90, 105, "high_first") == (H, L)
        assert extreme_order(108, 110, 90, 95, "low_first") == (L, H)

    def test_2016_11_14_rb_opens_near_the_high(self) -> None:
        # rb1701 on 2016-11-14: pre-settle 3043, open 3147 (+3.4%), high 3220, low 2897 (-4.8%), close 2945
        assert extreme_order(3147, 3220, 2897, 2945) == (H, L)


class TestBarPrices:
    def test_anchors_legs_and_grid(self) -> None:
        g = TickGrid("0.5")
        out = bar_prices(708.0, 712.5, 690.0, 695.5, g, 4)
        prices = [p for p, _k in out]
        kinds = [k for _p, k in out]
        assert len(out) == 1 + 3 * 4
        assert kinds[0] == "O" and kinds[4] == "H" and kinds[8] == "L" and kinds[12] == "C"
        assert prices[0] == 708.0 and prices[4] == 712.5 and prices[8] == 690.0 and prices[12] == 695.5
        assert all(690.0 <= p <= 712.5 for p in prices)
        assert all(abs(p / 0.5 - round(p / 0.5)) < 1e-9 for p in prices)
        # monotone legs
        assert prices[0:5] == sorted(prices[0:5])
        assert prices[4:9] == sorted(prices[4:9], reverse=True)
        assert prices[8:13] == sorted(prices[8:13])

    def test_tick_grid_repr_is_clean(self) -> None:
        g = TickGrid("0.2")
        assert repr(g.round(3671.4000000001)) == "3671.4"
        assert g.ceil(379.35) == 379.4 and g.floor(379.35) == 379.2
        assert TickGrid("0.02").ceil(379.35) == 379.36


class TestTimeline:
    def test_rb_day_is_night_first_and_inside_sessions(self) -> None:
        reg = InstrumentRegistry(base().products, base().calendar)
        ivs = reg.session_intervals("rb2701", "20260929")
        tl = Timeline(ivs)
        assert tl.total_ns == (2 * 60 + 75 + 60 + 90) * 60 * 1_000_000_000  # 21-23, 9-10:15, 10:30-11:30, 13:30-15
        pts = synthesize("rb2701", tl, 3100, 3150, 3000, 3020, grid=TickGrid("1"), multiplier=10, points_per_leg=4)
        assert pts[0].ts_ns == ts("2026-09-28 21:00:01")
        assert pts[-1].ts_ns == ts("2026-09-29 14:59:59")
        assert all(tl.contains(p.ts_ns) for p in pts)
        assert [p.ts_ns for p in pts] == sorted(p.ts_ns for p in pts)
        assert [p.volume for p in pts] == list(range(1, len(pts) + 1))
        night = [p for p in pts if p.ts_ns < ts("2026-09-29 00:00")]
        assert night and night[0].kind == "O"

    def test_offset_roundtrip(self) -> None:
        tl = Timeline([(0, 100), (200, 300)])
        assert tl.total_ns == 200
        assert tl.at(50) == 50 and tl.at(150) == 250 and tl.at(500) == 300
        assert tl.offset_of(250) == 150 and tl.offset_of(150) == 100 and tl.offset_of(-5) == 0

    def test_minute_bar_spanning_the_break_stays_in_session(self) -> None:
        reg = InstrumentRegistry(base().products, base().calendar)
        tl = Timeline(reg.session_intervals("rb2701", "20260929"))
        # Sina's 60-minute bar labelled 11:15 covers 10:00-10:15 and 10:30-11:15
        pts = minute_bar_points("rb2701", tl, [(ts("2026-09-29 11:15"), 3100.0, 3110.0, 3090.0, 3105.0)],
                                period_min=60, multiplier=10)
        assert [p.kind for p in pts] == ["O", "L", "H", "C"]
        assert pts[0].ts_ns == ts("2026-09-29 10:00:01") and pts[-1].ts_ns == ts("2026-09-29 11:14:59")
        assert all(tl.contains(p.ts_ns) for p in pts)


class TestBounds:
    def test_single_long_leg_is_certain_at_its_low(self) -> None:
        opt, pess = pnl_bounds(0.0, [Leg(1, 10, 100, 110, 90, 105)])
        assert (opt, pess) == (-100.0, -100.0)
        assert classify(opt, pess, -100.0) == CERTAIN
        assert classify(opt, pess, -101.0) == IMPOSSIBLE

    def test_long_short_book_known_answers(self) -> None:
        legs = [Leg(1, 10, 100, 110, 90, 100), Leg(-1, 10, 50, 55, 45, 50)]
        opt, pess = pnl_bounds(0.0, legs)
        assert pess == -150.0  # A at its low and B at its high at the same time
        assert opt == -50.0  # A alone at its low while B sits at its low (favourable for the short)
        assert classify(opt, pess, -120.0) == UNCERTAIN
        assert classify(opt, pess, -40.0) == CERTAIN
        assert classify(opt, pess, -200.0) == IMPOSSIBLE

    def test_forced_open_and_close_states(self) -> None:
        # the open state itself is already below the line: certain whatever the path
        opt, pess = pnl_bounds(-300.0, [Leg(1, 10, 100, 110, 90, 105)])
        assert opt == -400.0 and pess == -400.0
        opt2, _ = pnl_bounds(-30.0, [])
        assert opt2 == -30.0


class TestHitRate:
    def test_counts(self) -> None:
        days = [
            [(108, 110, 105, 106), (106, 107, 90, 95)],  # open near high, high first: hit
            [(92, 95, 90, 94), (94, 110, 93, 105)],  # open near low, low first: hit
            [(108, 108, 100, 101), (101, 111, 101, 110)],  # open near high (low first actually): miss
            [(100, 120, 80, 100)],  # both extremes in one bar: undetermined
        ]
        st = order_hit_rate(days)
        assert (st.days, st.determined, st.hits, st.same_bar) == (4, 3, 2, 1)
        assert st.hit_rate == pytest.approx(2 / 3)


class TestAdaptiveDensity:
    def test_crash_leg_is_crossed_in_small_steps(self) -> None:
        from futrisk.backtest.paths import bar_path, leg_counts

        # rb 2016-11-14 shape: open 3147, high 3220, low 2897, close 2945
        legs = leg_counts(3147, 3220, 2897, 2945, points_per_leg=4, max_step=0.0025, max_points=60)
        assert legs == (10, 42, 7)  # |73|, |323|, |48| over 0.25% x 3147 = 7.87 per state
        path = bar_path(3147, 3220, 2897, 2945, TickGrid("1"), legs)
        prices = [p for p, _k, _f in path]
        assert max(abs(b - a) for a, b in zip(prices, prices[1:], strict=False)) <= 0.0025 * 3147 + 1
        fr = {k: f for _p, k, f in path if k in ("O", "H", "L", "C")}
        assert fr == {"O": 0.0, "H": pytest.approx(1 / 3), "L": pytest.approx(2 / 3), "C": 1.0}

    def test_quiet_leg_keeps_the_minimum_and_the_cap_holds(self) -> None:
        from futrisk.backtest.paths import leg_counts

        assert leg_counts(3100, 3105, 3098, 3101, points_per_leg=4, max_step=0.0025) == (4, 4, 4)
        assert leg_counts(100, 150, 50, 100, points_per_leg=4, max_step=0.0025, max_points=40) == (40, 40, 40)
        assert leg_counts(100, 150, 50, 100, points_per_leg=4, max_step=0.0) == (4, 4, 4)

    def test_joint_legs_keep_contracts_of_a_session_in_the_same_states(self) -> None:
        reg = InstrumentRegistry(base().products, base().calendar)
        tl = Timeline(reg.session_intervals("rb2701", "20260929"))
        legs = (10, 42, 7)
        a = synthesize("rb2701", tl, 3147, 3220, 2897, 2945, grid=TickGrid("1"), multiplier=10, points_per_leg=4, legs=legs)
        b = synthesize("hc2701", tl, 3480, 3596, 3290, 3350, grid=TickGrid("1"), multiplier=10, points_per_leg=4, legs=legs)
        assert [p.ts_ns for p in a] == [p.ts_ns for p in b] and len(a) == 1 + sum(legs)
        assert [p.kind for p in a if p.kind != "I"] == ["O", "H", "L", "C"]
