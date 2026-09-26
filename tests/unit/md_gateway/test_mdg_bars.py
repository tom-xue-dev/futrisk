"""BarBuilder: cumulative-volume bars, gap absorption and revision, versions, quality (02 §7.1 Bar)."""

from __future__ import annotations

from datetime import datetime

import pytest

from futrisk.common.clock import BJ, bj_to_ns
from futrisk.messages.enums import BarQuality, Exchange, GapStatus
from futrisk.services.md_gateway.bars import MINUTE_NS, BarBuilder
from futrisk.services.md_gateway.normalizer import Normalizer
from futrisk.services.md_gateway.sessions import SessionIndex

from .helpers import T0, S, tick

M0 = T0 - T0 % MINUTE_NS  # 09:35:00


class Gaps:
    def __init__(self) -> None:
        self.windows: list[tuple[int, int, GapStatus]] = []

    def __call__(self, lo: int, hi: int) -> GapStatus | None:
        worst = None
        for a, b, st in self.windows:
            if a < hi and b >= lo:
                if st != GapStatus.FILLED:
                    return st
                worst = GapStatus.FILLED
        return worst


def state(minute: int, sec: int, volume: int, last: float, turnover: float | None = None, high: float | None = None) -> object:
    return tick(ts=M0 + minute * MINUTE_NS + sec * S, volume=volume, last=last, turnover=turnover if turnover is not None else volume * 1000.0, high=high)


def test_volume_is_cumulative_difference_and_bars_publish_after_grace() -> None:
    b = BarBuilder(Gaps(), close_grace_ns=3 * S)
    for m, s, v, px in [(0, 1, 10, 3120.0), (0, 30, 14, 3122.0), (0, 59, 20, 3119.0), (1, 5, 25, 3121.0)]:
        b.update(state(m, s, v, px))  # type: ignore[arg-type]
    assert b.due(M0 + MINUTE_NS + 2 * S) == []  # grace not over
    bars = b.due(M0 + MINUTE_NS + 3 * S)
    assert len(bars) == 1
    bar = bars[0]
    assert (bar.open, bar.high, bar.low, bar.close) == (3120.0, 3122.0, 3119.0, 3119.0)
    assert bar.volume == 20 and bar.turnover == 20_000.0 and bar.version == 1 and bar.quality == BarQuality.OK
    nxt = b.due(M0 + 2 * MINUTE_NS + 3 * S)
    assert nxt[0].volume == 5  # 25 - 20


def test_gap_volume_is_absorbed_then_revised_by_backfill() -> None:
    gaps = Gaps()
    b = BarBuilder(gaps, close_grace_ns=3 * S)
    b.update(state(0, 50, 100, 3120.0))  # type: ignore[arg-type]
    # outage from 09:36 to 09:38: the next observed state is at 09:38
    gaps.windows.append((M0 + MINUTE_NS, M0 + 3 * MINUTE_NS, GapStatus.OPEN))
    b.update(state(3, 10, 190, 3130.0))  # type: ignore[arg-type]
    first = {x.minute_start_ns: x for x in b.due(M0 + 4 * MINUTE_NS + 5 * S)}
    assert first[M0 + 3 * MINUTE_NS].volume == 90  # nothing lost: the absorbing bar holds it
    assert first[M0 + 3 * MINUTE_NS].quality == BarQuality.GAP_UNFILLED
    assert sum(x.volume for x in first.values()) == 190
    # the replay arrives (BACKFILL) and the gap is filled
    gaps.windows[0] = (M0 + MINUTE_NS, M0 + 3 * MINUTE_NS, GapStatus.FILLED)
    b.update(state(1, 20, 130, 3124.0))  # type: ignore[arg-type]
    b.update(state(2, 20, 160, 3127.0))  # type: ignore[arg-type]
    b.touch(M0 + MINUTE_NS, M0 + 3 * MINUTE_NS)
    revised = {x.minute_start_ns: x for x in b.due(M0 + 4 * MINUTE_NS + 6 * S)}
    assert revised[M0 + MINUTE_NS].volume == 30 and revised[M0 + MINUTE_NS].version == 1
    assert revised[M0 + 2 * MINUTE_NS].volume == 30
    assert revised[M0 + 3 * MINUTE_NS].volume == 30 and revised[M0 + 3 * MINUTE_NS].version == 2
    assert revised[M0 + 3 * MINUTE_NS].quality == BarQuality.GAP_FILLED
    total = {m: x.volume for m, x in {**first, **revised}.items()}
    assert sum(total.values()) == 190


def test_day_high_seen_after_a_gap_extends_the_absorbing_bar() -> None:
    b = BarBuilder(Gaps(), close_grace_ns=0)
    b.update(state(0, 10, 10, 3120.0, high=3121.0))  # type: ignore[arg-type]
    b.update(state(2, 10, 30, 3122.0, high=3140.0))  # type: ignore[arg-type]  # the high of 3140 traded in the gap
    bars = {x.minute_start_ns: x for x in b.due(M0 + 5 * MINUTE_NS)}
    assert bars[M0 + 2 * MINUTE_NS].high == 3140.0


def test_unchanged_bar_is_not_republished_and_load_restores_versions() -> None:
    b = BarBuilder(Gaps(), close_grace_ns=0)
    b.update(state(0, 10, 10, 3120.0))  # type: ignore[arg-type]
    first = b.due(M0 + MINUTE_NS)
    assert len(first) == 1
    b.update(state(0, 10, 10, 3120.0))  # type: ignore[arg-type]  # duplicate content
    assert b.due(M0 + MINUTE_NS) == []
    b2 = BarBuilder(Gaps(), close_grace_ns=0)
    b2.load(first)
    b2.update(state(1, 5, 15, 3121.0))  # type: ignore[arg-type]
    after = b2.due(M0 + 2 * MINUTE_NS)
    assert after[0].volume == 5  # prefix volume restored from the loaded bars


# ---------------------------------------------------------------- L9: session-aware minutes


def at(d: int, h: int, m: int, s: float = 0.0) -> int:
    return bj_to_ns(datetime(2026, 9, d, h, m, tzinfo=BJ)) + int(s * S)


@pytest.fixture
def session_bars(sessions: SessionIndex, normalizer: Normalizer) -> tuple[BarBuilder, Gaps]:
    def windows_of(iid: str, td: str):  # type: ignore[no-untyped-def]
        meta = normalizer.known_meta(iid)
        return sessions.int_windows(meta.sessions, td) if meta is not None else ()

    gaps = Gaps()
    return BarBuilder(gaps, close_grace_ns=0, windows_of=windows_of, session_grace_ns=60 * S), gaps


def test_closing_snapshot_and_auction_belong_to_session_minutes(session_bars: tuple[BarBuilder, Gaps]) -> None:
    b, _ = session_bars
    assert b.minute_of("rb2701", "20260928", at(28, 11, 30, 0.3)) == at(28, 11, 29)  # closing snapshot
    assert b.minute_of("rb2701", "20260929", at(28, 20, 59, 0.5)) == at(28, 21, 0)  # opening auction
    assert b.minute_of("rb2701", "20260928", at(28, 10, 0, 12.0)) == at(28, 10, 0)
    b.update(tick(ts=at(28, 11, 29, 50.0), volume=100, last=3120.0))
    b.update(tick(ts=at(28, 11, 30, 0.3), volume=104, last=3121.0))
    bars = b.due(at(28, 11, 31), force=True)
    assert [(x.minute_start_ns, x.volume, x.close) for x in bars] == [(at(28, 11, 29), 104, 3121.0)]


def test_first_bar_after_a_break_does_not_span_it(session_bars: tuple[BarBuilder, Gaps]) -> None:
    b, gaps = session_bars
    b.update(tick(ts=at(28, 11, 29, 30.0), volume=100, last=3120.0))
    gaps.windows.append((at(28, 12, 0), at(28, 13, 20), GapStatus.UNRECOVERABLE))  # something during the lunch break
    b.update(tick(ts=at(28, 13, 30, 5.0), volume=130, last=3122.0))
    bars = {x.minute_start_ns: x for x in b.due(at(28, 13, 32), force=True)}
    first = bars[at(28, 13, 30)]
    assert first.volume == 30 and first.quality == BarQuality.OK


def test_au_night_session_across_midnight_and_the_morning_open(session_bars: tuple[BarBuilder, Gaps]) -> None:
    """au trades 21:00-02:30 (natural date +1 after midnight) for the next trading day, then 09:00."""
    b, gaps = session_bars
    td = "20260929"
    kw = {"iid": "au2612", "td": td, "exchange": Exchange.SHFE, "upper": 969.0, "lower": 731.0, "pre_settle": 850.0}
    b.update(tick(ts=at(28, 20, 59, 0.0), volume=40, last=850.0, **kw))  # auction match
    b.update(tick(ts=at(28, 21, 0, 30.0), volume=55, last=850.2, **kw))
    b.update(tick(ts=at(29, 0, 30, 10.0), volume=300, last=851.0, **kw))  # after midnight: same trading day
    b.update(tick(ts=at(29, 2, 29, 59.0), volume=420, last=852.0, **kw))
    b.update(tick(ts=at(29, 2, 30, 0.4), volume=425, last=852.1, **kw))  # closing snapshot of the night
    gaps.windows.append((at(29, 5, 0), at(29, 6, 0), GapStatus.OPEN))  # between the sessions
    b.update(tick(ts=at(29, 9, 0, 3.0), volume=470, last=853.0, **kw))
    bars = {x.minute_start_ns: x for x in b.due(at(29, 9, 5), force=True)}
    assert bars[at(28, 21, 0)].volume == 55 and bars[at(28, 21, 0)].trading_day == td
    assert bars[at(29, 2, 29)].volume == 425 - 300 and bars[at(29, 2, 29)].close == 852.1
    morning = bars[at(29, 9, 0)]
    assert morning.volume == 45 and morning.quality == BarQuality.OK  # 21:00 -> 09:00 without spanning the night break
    assert sum(x.volume for x in bars.values()) == 470


def test_final_flush_never_publishes_the_incomplete_minute(session_bars: tuple[BarBuilder, Gaps]) -> None:
    b, _ = session_bars
    b.update(tick(ts=at(28, 10, 0, 10.0), volume=10, last=3120.0))
    b.update(tick(ts=at(28, 10, 1, 10.0), volume=20, last=3121.0))
    out = b.due(at(28, 10, 1, 30.0), final=True)
    assert [x.minute_start_ns for x in out] == [at(28, 10, 0)]  # 10:01 is still running
