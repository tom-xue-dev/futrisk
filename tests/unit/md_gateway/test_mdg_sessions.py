"""SessionIndex on the real calendar: in-session grace, night sessions past midnight, day lookups (01 §8.2)."""

from __future__ import annotations

from datetime import datetime

from futrisk.common.clock import BJ, NS_PER_S, bj_to_ns
from futrisk.services.md_gateway.sessions import SessionIndex

RB = ("night_2100_2300", "day_commodity")
CU = ("night_2100_0100", "day_commodity")


def bj_ns(*args: int) -> int:
    return bj_to_ns(datetime(*args, tzinfo=BJ))


def test_close_grace_and_night_session(sessions: SessionIndex) -> None:
    td = "20260924"
    assert sessions.in_session(RB, td, bj_ns(2026, 9, 24, 15, 0, 0))  # closing snapshot, window end inclusive
    assert not sessions.in_session(RB, td, bj_ns(2026, 9, 24, 15, 4, 39))
    assert sessions.in_session(RB, td, bj_ns(2026, 9, 24, 15, 4, 39), 300 * NS_PER_S)  # DCE's late broadcast
    assert not sessions.in_session(RB, td, bj_ns(2026, 9, 24, 15, 6), 300 * NS_PER_S)
    assert sessions.in_session(RB, td, bj_ns(2026, 9, 23, 21, 30))  # night session of trading day 09-24
    assert not sessions.in_session(RB, td, bj_ns(2026, 9, 24, 10, 25))  # morning break
    assert sessions.day_close_ns(RB, td) == bj_ns(2026, 9, 24, 15, 0)


def test_copper_saturday_early_morning_belongs_to_monday(sessions: SessionIndex) -> None:
    saturday_0030 = bj_ns(2026, 9, 19, 0, 30)
    assert sessions.trading_day(saturday_0030) == "20260921"
    assert sessions.in_session(CU, "20260921", saturday_0030)
    assert not sessions.in_session(RB, "20260921", saturday_0030)  # rb stops at 23:00
    assert sessions.open_now(CU, saturday_0030) == "20260921" and sessions.open_now(RB, saturday_0030) is None


def test_day_lookups(sessions: SessionIndex) -> None:
    assert sessions.current_or_next(bj_ns(2026, 9, 25, 12, 0)) == "20260928"  # holiday -> next trading day
    assert sessions.current_or_next(bj_ns(2026, 9, 24, 15, 30)) == "20260924"
    assert sessions.current_or_next(bj_ns(2026, 9, 28, 20, 30)) == "20260929"  # night -> next trading day
    assert sessions.trading_day(bj_ns(2026, 9, 26, 10, 0)) is None  # Saturday daytime
    assert sessions.next_trading_day("20260930") == "20261008" and sessions.prev_trading_day("20260928") == "20260924"
    assert sessions.bj_date(bj_ns(2026, 9, 28, 23, 59)) == "20260928" and sessions.bj_date(bj_ns(2026, 9, 29, 0, 1)) == "20260929"
    assert not sessions.judging(RB, bj_ns(2026, 9, 28, 9, 0, 10), 30 * NS_PER_S) and sessions.judging(RB, bj_ns(2026, 9, 28, 9, 0, 40), 30 * NS_PER_S)
