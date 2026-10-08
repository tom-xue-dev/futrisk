"""Event helpers: bounded rate limiter and the time-window gap ledger (02 §7.1 GapInfo)."""

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
    assert rl.allow(98, T0 + 21 * S) == (False, 0) and rl.allow(98, T0 + 31 * S) == (True, 1)  # suppressed count reported


def test_time_window_gaps_open_close_once_and_unknown_ids_close_at_once() -> None:
    events: list[tuple[MdEvent, GapRecord | None]] = []
    now = [T0]
    log = EventLog(lambda ev, gap, actor: events.append((ev, gap)), now_ns=lambda: now[0], trading_day=lambda: TD)
    book = GapBook(log, now_ns=lambda: now[0])
    all_gap = book.open_window(None, trading_day=TD, from_ns=T0)
    fb_gap = book.open_window(SourceId.FEED_B, trading_day=TD, from_ns=T0 + S)
    assert all_gap.gap_id == f"all:{T0}" and all_gap.info.source is None
    assert fb_gap.gap_id == f"feed_b:{T0 + S}" and fb_gap.info.source is SourceId.FEED_B
    assert book.open_window_of(None) is all_gap and book.open_window_of(SourceId.FEED_B) is fb_gap
    assert book.open_window(None, trading_day=TD, from_ns=T0) is all_gap  # already open: no second event
    assert len(book.open_gaps()) == 2
    now[0] = T0 + 10 * S
    book.close(fb_gap.gap_id, status=GapStatus.UNRECOVERABLE)
    assert book.open_window_of(SourceId.FEED_B) is None and fb_gap.info.to_ns == T0 + 10 * S and fb_gap.closed_ns == T0 + 10 * S
    book.close(all_gap.gap_id, status=GapStatus.FILLED)
    assert book.close(all_gap.gap_id, status=GapStatus.FILLED) is all_gap  # closing twice is a no-op
    # a Feed A range given up before it was ever opened (SEQ_EVICTED) is created closed
    lost = book.close(
        "feed_a:E1:5-9", status=GapStatus.UNRECOVERABLE, unrecoverable=5, trading_day=TD, source=SourceId.FEED_A, epoch="E1",
        from_seq=5, to_seq=9,
    )
    assert lost.info.status is GapStatus.UNRECOVERABLE and lost.info.unrecoverable == 5 and lost.info.from_ns == T0 + 10 * S
    kinds = [ev.kind for ev, _ in events]
    assert kinds == [
        MdEventKind.GAP_OPEN, MdEventKind.GAP_OPEN, MdEventKind.GAP_UNRECOVERABLE, MdEventKind.GAP_FILLED, MdEventKind.GAP_UNRECOVERABLE
    ]
    assert events[2][0].gap is not None and events[2][0].gap.status is GapStatus.UNRECOVERABLE
    assert events[2][0].trading_day == TD and events[2][1] is fb_gap
    assert log.counts == {"GAP_OPEN": 2, "GAP_UNRECOVERABLE": 2, "GAP_FILLED": 1}
