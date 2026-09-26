"""Event helpers: bounded rate limiter (L4), time-window gaps and the bar gap query (M7, 02 §7.1 GapInfo)."""

from __future__ import annotations

from futrisk.messages.enums import GapStatus, MdEventKind, SourceId
from futrisk.messages.md import MdEvent
from futrisk.services.md_gateway.events import EventLog, GapBook, GapRecord, RateLimiter

from .helpers import T0, TD, S


def test_rate_limiter_is_bounded() -> None:
    rl = RateLimiter(10 * S, max_keys=3)
    for k in range(3):
        assert rl.allow(k, T0) == (True, 0)
    assert rl.allow(0, T0 + S) == (False, 0)  # suppressed and counted
    assert rl.allow(99, T0 + 2 * S) == (True, 0)  # a new key when full: the oldest one is forgotten
    assert len(rl.last) == 3 and rl.evicted == 1
    assert rl.allow(98, T0 + 20 * S) == (True, 0)  # expired keys are purged first
    assert len(rl.last) <= 3


def test_time_window_gaps_and_the_bar_query() -> None:
    events: list[tuple[MdEvent, GapRecord | None]] = []
    now = [T0]
    log = EventLog(lambda ev, gap, actor: events.append((ev, gap)), now_ns=lambda: now[0], trading_day=lambda: TD)
    book = GapBook(log, now_ns=lambda: now[0])
    all_gap = book.open_window(None, trading_day=TD, from_ns=T0)
    fb_gap = book.open_window(SourceId.FEED_B, trading_day=TD, from_ns=T0 + S)
    assert all_gap.gap_id == f"all:{T0}" and all_gap.info.source is None
    assert fb_gap.gap_id == f"feed_b:{T0 + S}"
    assert book.open_window_of(None) is all_gap and book.open_window_of(SourceId.FEED_B) is fb_gap
    now[0] = T0 + 10 * S
    # a polled source's own outage does not degrade bars; the all-sources gap does
    book.close(fb_gap.gap_id, status=GapStatus.UNRECOVERABLE)
    assert book.overlap(T0, T0 + 5 * S) == GapStatus.OPEN
    book.close(all_gap.gap_id, status=GapStatus.FILLED)
    assert book.overlap(T0, T0 + 5 * S) == GapStatus.FILLED
    kinds = [ev.kind for ev, _ in events]
    assert kinds == [MdEventKind.GAP_OPEN, MdEventKind.GAP_OPEN, MdEventKind.GAP_UNRECOVERABLE, MdEventKind.GAP_FILLED]
