"""Session gate and in-session tests on the real calendar (01 §8.2)."""

from __future__ import annotations

from datetime import date, datetime

import pytest

from futrisk.common.clock import BJ, bj_to_ns
from futrisk.common.config import FullConfig
from futrisk.marketdata.sessions import SessionGate, WindowCache, in_session

RB = ["night_2100_2300", "day_commodity"]
AU = ["night_2100_0230", "day_commodity"]
IF = ["day_index"]


def bj_ns(*args: int) -> int:
    return bj_to_ns(datetime(*args, tzinfo=BJ))


@pytest.fixture
def rb_gate(default_config: FullConfig) -> SessionGate:
    return SessionGate(default_config.calendar, RB, pre_margin_s=180, post_margin_s=300)


class TestSessionGate:
    @pytest.mark.parametrize(
        ("ts", "active"),
        [
            ((2026, 9, 28, 8, 40), False),  # before the 08:55 auction (no night session after the holiday)
            ((2026, 9, 28, 8, 53), True),  # 3 min pre-margin
            ((2026, 9, 28, 10, 19), True),  # 10:15 close + 5 min post-margin
            ((2026, 9, 28, 10, 22), False),  # morning break
            ((2026, 9, 28, 10, 28), True),  # 3 min before 10:30
            ((2026, 9, 28, 15, 4), True),
            ((2026, 9, 28, 15, 6), False),
            ((2026, 9, 28, 20, 53), True),  # night auction 20:55 of trading day 09-29
            ((2026, 9, 28, 23, 4), True),
            ((2026, 9, 28, 23, 6), False),
            ((2026, 9, 30, 20, 53), False),  # no night session before the National Day holiday
            ((2026, 9, 26, 10, 0), False),  # Saturday
        ],
    )
    def test_commodity_windows(self, rb_gate: SessionGate, ts: tuple[int, ...], active: bool) -> None:
        assert rb_gate.is_active(bj_ns(*ts)) is active

    def test_night_session_until_0230_for_gold(self, default_config: FullConfig) -> None:
        gate = SessionGate(default_config.calendar, AU)
        assert gate.is_active(bj_ns(2026, 9, 29, 2, 33))
        assert not gate.is_active(bj_ns(2026, 9, 29, 2, 36))

    def test_index_futures_lunch_break(self, default_config: FullConfig) -> None:
        gate = SessionGate(default_config.calendar, IF)
        assert not gate.is_active(bj_ns(2026, 9, 28, 9, 21))
        assert gate.is_active(bj_ns(2026, 9, 28, 9, 23))
        assert gate.is_active(bj_ns(2026, 9, 28, 11, 34))
        assert not gate.is_active(bj_ns(2026, 9, 28, 11, 36))
        assert gate.is_active(bj_ns(2026, 9, 28, 12, 58))

    def test_union_of_templates(self, default_config: FullConfig) -> None:
        gate = SessionGate(default_config.calendar, RB + IF)
        assert gate.is_active(bj_ns(2026, 9, 28, 10, 22))  # commodity break, but IF trades

    def test_next_active_skips_the_holiday(self, rb_gate: SessionGate) -> None:
        assert rb_gate.next_active_ns(bj_ns(2026, 9, 30, 15, 30)) == bj_ns(2026, 10, 8, 8, 52)
        assert rb_gate.next_active_ns(bj_ns(2026, 9, 28, 15, 30)) == bj_ns(2026, 9, 28, 20, 52)
        now = bj_ns(2026, 9, 28, 10, 0)
        assert rb_gate.next_active_ns(now) == now
        assert rb_gate.active_until_ns(now) == bj_ns(2026, 9, 28, 10, 20)
        assert rb_gate.active_until_ns(bj_ns(2026, 9, 28, 10, 22)) is None

    def test_first_trading_day_before_open(self, rb_gate: SessionGate) -> None:
        # Friday 2026-09-25 (holiday): the next window is Monday's 08:55 auction
        assert rb_gate.next_active_ns(bj_ns(2026, 9, 25, 12, 0)) == bj_ns(2026, 9, 28, 8, 52)

    def test_requires_templates(self, default_config: FullConfig) -> None:
        with pytest.raises(ValueError):
            SessionGate(default_config.calendar, [])


class TestInSession:
    def test_close_grace(self, default_config: FullConfig) -> None:
        wc = WindowCache(default_config.calendar)
        td = date(2026, 9, 24)
        assert in_session(wc, RB, td, bj_ns(2026, 9, 24, 15, 0, 0))  # closing snapshot, window end inclusive
        assert not in_session(wc, RB, td, bj_ns(2026, 9, 24, 15, 4, 39))
        assert in_session(
            wc, RB, td, bj_ns(2026, 9, 24, 15, 4, 39), close_grace_s=300
        )  # DCE's late broadcast
        assert not in_session(wc, RB, td, bj_ns(2026, 9, 24, 15, 6), close_grace_s=300)
        assert in_session(wc, RB, td, bj_ns(2026, 9, 23, 21, 30))  # night session of trading day 09-24
        assert not in_session(wc, RB, td, bj_ns(2026, 9, 24, 10, 25))  # morning break


class TestCopperNightSession:
    CU = ["night_2100_0100", "day_commodity"]

    def test_friday_night_runs_past_midnight_into_saturday(self, default_config: FullConfig) -> None:
        gate = SessionGate(default_config.calendar, self.CU)
        assert gate.is_active(bj_ns(2026, 9, 18, 23, 30))  # Friday night
        assert gate.is_active(bj_ns(2026, 9, 19, 0, 30))  # Saturday 00:30, still trading
        assert gate.is_active(bj_ns(2026, 9, 19, 1, 4))  # post-close margin
        assert not gate.is_active(bj_ns(2026, 9, 19, 1, 6))
        monday_open = bj_ns(
            2026, 9, 21, 8, 57
        )  # 09:00 minus the margin; no 08:55 auction after a night session
        assert gate.next_active_ns(bj_ns(2026, 9, 19, 1, 6)) == monday_open

    def test_saturday_0030_is_in_mondays_session(self, default_config: FullConfig) -> None:
        wc = WindowCache(default_config.calendar)
        saturday_0030 = datetime(2026, 9, 19, 0, 30, tzinfo=BJ)
        assert default_config.calendar.trading_day_for(saturday_0030) == date(2026, 9, 21)
        assert in_session(wc, self.CU, date(2026, 9, 21), bj_ns(2026, 9, 19, 0, 30))
        assert not in_session(wc, RB, date(2026, 9, 21), bj_ns(2026, 9, 19, 0, 30))  # rb stops at 23:00
