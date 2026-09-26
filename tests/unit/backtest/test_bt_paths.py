"""Path synthesis and the trading timeline (hand-made bars)."""

from __future__ import annotations

import pytest
from btsupport import base, ts

from futrisk.backtest.paths import TickGrid, Timeline, bar_path, extreme_order, leg_counts, synthesize
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
        out = bar_path(708.0, 712.5, 690.0, 695.5, g, (4, 4, 4))
        prices = [p for p, _k, _f in out]
        kinds = [k for _p, k, _f in out]
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


class TestAdaptiveDensity:
    def test_crash_leg_is_crossed_in_small_steps(self) -> None:
        # rb 2016-11-14 shape: open 3147, high 3220, low 2897, close 2945
        legs = leg_counts(3147, 3220, 2897, 2945, points_per_leg=4, max_step=0.0025, max_points=60)
        assert legs == (10, 42, 7)  # |73|, |323|, |48| over 0.25% x 3147 = 7.87 per state
        path = bar_path(3147, 3220, 2897, 2945, TickGrid("1"), legs)
        prices = [p for p, _k, _f in path]
        assert max(abs(b - a) for a, b in zip(prices, prices[1:], strict=False)) <= 0.0025 * 3147 + 1
        fr = {k: f for _p, k, f in path if k in ("O", "H", "L", "C")}
        assert fr == {"O": 0.0, "H": pytest.approx(1 / 3), "L": pytest.approx(2 / 3), "C": 1.0}

    def test_quiet_leg_keeps_the_minimum_and_the_cap_holds(self) -> None:
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
