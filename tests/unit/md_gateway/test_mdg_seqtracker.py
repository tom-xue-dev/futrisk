"""SeqTracker: duplicates, reorder buffer, hole replay, tail gaps, resume, eviction, epoch change (02 §9.1)."""

from __future__ import annotations

from typing import Any

from futrisk.messages.enums import GapStatus
from futrisk.services.md_gateway.seqtracker import LOST, RESUME, RING, TAIL, Hole, SeqTracker

from .helpers import MS, Clock


class Sink:
    def __init__(self) -> None:
        self.delivered: list[tuple[int, bool]] = []
        self.requests: list[tuple[int, int, int]] = []
        self.gaps: list[tuple[str, str, GapStatus, int, int, int]] = []
        self.reconnects: list[str] = []
        self.resumes = 0
        self._req = 100

    def deliver(self, seq: int, replay: bool, payload: Any) -> None:
        self.delivered.append((seq, replay))

    def request_replay(self, lo: int, hi: int) -> int | None:
        self._req += 1
        self.requests.append((self._req, lo, hi))
        return self._req

    def gap_event(self, hole: Hole, event: str) -> None:
        self.gaps.append((event, hole.kind, hole.status, hole.filled, hole.duplicates, hole.unrecoverable))

    def reconnect(self, reason: str) -> None:
        self.reconnects.append(reason)

    def resume_finished(self) -> None:
        self.resumes += 1


def fresh(sink: Sink, clock: Clock, head: int = 10, **kw: Any) -> SeqTracker:
    tr = SeqTracker(sink, clock, reorder_max=4, reorder_ns=100 * MS, **kw)
    tr.on_subscribe_sent(1, None)
    tr.on_welcome("E1", head, 1)
    return tr


def seqs(sink: Sink) -> list[int]:
    return [s for s, _ in sink.delivered]


def test_in_order_and_duplicates_dropped() -> None:
    sink, clock = Sink(), Clock()
    tr = fresh(sink, clock)
    for s in (11, 12, 12, 13, 11):
        tr.on_tick(s, False, None)
    assert seqs(sink) == [11, 12, 13]
    assert tr.stats["duplicates"] == 2
    assert tr.contiguous == 13 and tr.frontier == 14 and tr.complete


def test_reorder_within_window_is_delivered_in_seq_order() -> None:
    sink, clock = Sink(), Clock()
    tr = fresh(sink, clock)
    for s in (11, 13, 14, 12, 15):
        tr.on_tick(s, False, None)
    assert seqs(sink) == [11, 12, 13, 14, 15]
    assert not sink.requests and not sink.gaps
    assert all(not replay for _, replay in sink.delivered)


def test_hole_after_reorder_timeout_is_replayed_without_moving_live_seq() -> None:
    sink, clock = Sink(), Clock()
    tr = fresh(sink, clock)
    tr.on_tick(11, False, None)
    tr.on_tick(14, False, None)  # 12, 13 missing
    tr.on_tick(15, False, None)
    clock.advance(150 * MS)
    tr.poll()
    assert seqs(sink) == [11, 14, 15]  # live keeps flowing
    assert sink.gaps[0][:2] == ("open", "hole")
    req, lo, hi = sink.requests[0]
    assert (lo, hi) == (12, 13)
    assert tr.contiguous == 11 and tr.frontier == 16
    # interleaved: live 16 arrives before the replay block
    tr.on_tick(16, False, None)
    tr.on_replay_begin(req, 12, 13)
    tr.on_tick(12, True, None)
    tr.on_tick(13, True, None)
    tr.on_replay_end(req, 13)
    assert tr.frontier == 17  # replay never resets the live expectation
    assert tr.contiguous == 16 and tr.complete
    assert sink.delivered[-2:] == [(12, True), (13, True)]
    assert sink.gaps[-1] == ("close", "hole", GapStatus.FILLED, 2, 0, 0)


def test_reorder_overflow_declares_hole_at_once() -> None:
    sink, clock = Sink(), Clock()
    tr = fresh(sink, clock)
    for s in (12, 13, 14, 15, 16):  # 11 never arrives, buffer max 4
        tr.on_tick(s, False, None)
    assert seqs(sink) == [12, 13, 14, 15, 16]
    assert sink.requests[0][1:] == (11, 11)


def test_late_live_tick_fills_hole() -> None:
    sink, clock = Sink(), Clock()
    tr = fresh(sink, clock)
    tr.on_tick(12, False, None)
    clock.advance(150 * MS)
    tr.poll()
    tr.on_tick(11, False, None)  # late (reordered by more than the window)
    assert sink.delivered[-1] == (11, True)  # below the frontier: backfill only
    assert sink.gaps[-1][:3] == ("close", "hole", GapStatus.FILLED)
    assert tr.contiguous == 12


def test_tail_gap_detected_from_heartbeats() -> None:
    sink, clock = Sink(), Clock()
    tr = fresh(sink, clock, tail_heartbeats=2)
    tr.on_tick(11, False, None)
    tr.on_heartbeat(14)
    assert not sink.requests
    tr.on_heartbeat(14)
    assert sink.requests[0][1:] == (12, 14)
    assert sink.gaps[0][:2] == ("open", TAIL)
    assert tr.frontier == 15


def test_resume_block_is_backfill_and_counts_filled_without_duplicates() -> None:
    sink, clock = Sink(), Clock()
    tr = SeqTracker(sink, clock)
    tr.restore("E1", 20)
    resume = tr.resume_request()
    assert resume == {"epoch": "E1", "from_seq": 21}
    tr.on_subscribe_sent(7, resume)
    tr.on_welcome("E1", 25, 1)
    tr.on_replay_begin(7, 21, 25)
    for s in range(21, 26):
        tr.on_tick(s, True, None)
    tr.on_replay_end(7, 25)
    tr.on_tick(26, False, None)
    assert sink.delivered == [(21, True), (22, True), (23, True), (24, True), (25, True), (26, False)]
    assert sink.gaps[0][:2] == ("open", RESUME)
    assert sink.gaps[-1] == ("close", RESUME, GapStatus.FILLED, 5, 0, 0)
    assert tr.contiguous == 26 and tr.complete


def test_empty_resume_block() -> None:
    sink, clock = Sink(), Clock()
    tr = SeqTracker(sink, clock)
    tr.restore("E1", 20)
    tr.on_subscribe_sent(7, tr.resume_request())
    tr.on_welcome("E1", 20, 1)
    tr.on_replay_begin(7, 21, 20)
    tr.on_replay_end(7, 20)
    tr.on_tick(21, False, None)
    assert sink.delivered == [(21, False)] and not sink.gaps and tr.complete


def test_resume_seq_evicted_gives_up_evicted_part_and_replays_the_ring() -> None:
    sink, clock = Sink(), Clock()
    tr = SeqTracker(sink, clock)
    tr.restore("E1", 20)
    tr.on_subscribe_sent(7, tr.resume_request())
    tr.on_welcome("E1", 500, 100)
    tr.on_resume_rejected("SEQ_EVICTED", 100, 500)
    lost = [g for g in sink.gaps if g[1] == LOST]
    assert lost == [("lost", LOST, GapStatus.UNRECOVERABLE, 0, 0, 79)]  # seqs 21..99
    req, lo, hi = sink.requests[0]
    assert (lo, hi) == (100, 500)
    assert tr.contiguous == 99 and tr.frontier == 501
    tr.on_tick(501, False, None)  # live arrives before the requested replay
    tr.on_replay_begin(req, 100, 500)
    for s in range(100, 501):
        tr.on_tick(s, True, None)
    tr.on_replay_end(req, 500)
    assert sink.gaps[-1] == ("close", RING, GapStatus.FILLED, 401, 0, 0)
    assert tr.contiguous == 501


def test_epoch_changed_loses_known_old_tail_and_backfills_new_ring() -> None:
    sink, clock = Sink(), Clock()
    tr = fresh(sink, clock, head=10)
    tr.on_tick(11, False, None)
    tr.on_heartbeat(15)  # the old epoch had reached 15
    tr.on_disconnect()
    tr.on_subscribe_sent(8, tr.resume_request())
    tr.on_welcome("E2", 30, 1)
    tr.on_resume_rejected("EPOCH_CHANGED", 1, 30)
    assert ("lost", LOST, GapStatus.UNRECOVERABLE, 0, 0, 4) in sink.gaps  # E1 seqs 12..15
    assert tr.epoch == "E2" and tr.frontier == 31
    assert sink.requests[-1][1:] == (1, 30)


def test_replay_of_evicted_range_is_unrecoverable_and_cursor_moves_on() -> None:
    sink, clock = Sink(), Clock()
    tr = fresh(sink, clock)
    tr.on_tick(11, False, None)
    tr.on_tick(14, False, None)
    clock.advance(150 * MS)
    tr.poll()
    req = sink.requests[0][0]
    tr.on_error(req, "SEQ_EVICTED", 5000, 9000)
    assert sink.gaps[-1] == ("close", "hole", GapStatus.UNRECOVERABLE, 0, 0, 2)
    assert tr.contiguous == 14 and tr.complete


def test_clamped_replay_gives_up_the_part_below_ring_low() -> None:
    sink, clock = Sink(), Clock()
    tr = fresh(sink, clock)
    tr.on_tick(11, False, None)
    tr.on_tick(15, False, None)
    clock.advance(150 * MS)
    tr.poll()
    req = sink.requests[0][0]  # 12..14
    tr.on_replay_begin(req, 13, 14)  # 12 evicted meanwhile
    tr.on_tick(13, True, None)
    tr.on_tick(14, True, None)
    tr.on_replay_end(req, 14)
    assert sink.gaps[-1] == ("close", "hole", GapStatus.UNRECOVERABLE, 2, 0, 1)
    assert tr.contiguous == 15


def test_disconnect_releases_held_ticks_and_resume_covers_the_hole() -> None:
    sink, clock = Sink(), Clock()
    tr = fresh(sink, clock)
    tr.on_tick(11, False, None)
    tr.on_tick(13, False, None)  # held
    tr.on_disconnect()
    assert seqs(sink) == [11, 13]
    assert tr.resume_request() == {"epoch": "E1", "from_seq": 12}
    tr.on_subscribe_sent(9, tr.resume_request())
    tr.on_welcome("E1", 14, 1)
    tr.on_replay_begin(9, 12, 14)
    for s in (12, 13, 14):
        tr.on_tick(s, True, None)
    tr.on_replay_end(9, 14)
    assert seqs(sink) == [11, 13, 12, 14]
    closes = [g for g in sink.gaps if g[0] == "close"]
    assert closes and all(g[2] == GapStatus.FILLED for g in closes)
    assert tr.contiguous == 14 and tr.complete


def test_unanswered_replay_is_retried_then_reconnects() -> None:
    sink, clock = Sink(), Clock()
    tr = fresh(sink, clock, replay_timeout_ns=1000 * MS, max_attempts=2)
    tr.on_tick(11, False, None)
    tr.on_tick(13, False, None)
    clock.advance(150 * MS)
    tr.poll()
    assert len(sink.requests) == 1
    clock.advance(1100 * MS)
    tr.poll()
    assert len(sink.requests) == 2
    clock.advance(1100 * MS)
    tr.poll()
    assert sink.reconnects


def test_seq_evicted_inside_an_open_hole_keeps_its_rest_and_backfills_the_ring() -> None:
    sink, clock = Sink(), Clock()
    tr = fresh(sink, clock)
    tr.on_tick(11, False, None)
    tr.on_tick(14, False, None)
    clock.advance(150 * MS)
    tr.poll()  # hole 12..13
    tr.on_disconnect()
    tr.on_subscribe_sent(9, tr.resume_request())
    tr.on_welcome("E1", 30, 13)
    tr.on_resume_rejected("SEQ_EVICTED", 13, 30)
    ring_req = sink.requests[-1]
    assert ring_req[1:] == (15, 30)
    tr.poll()  # the old hole asks for what is still missing (13)
    hole_req = sink.requests[-1]
    assert hole_req[1:] == (13, 13)
    tr.on_replay_begin(hole_req[0], 13, 13)
    tr.on_tick(13, True, None)
    tr.on_replay_end(hole_req[0], 13)
    tr.on_replay_begin(ring_req[0], 15, 30)
    for s in range(15, 31):
        tr.on_tick(s, True, None)
    tr.on_replay_end(ring_req[0], 30)
    closes = [g for g in sink.gaps if g[0] == "close"]
    assert ("close", "hole", GapStatus.UNRECOVERABLE, 1, 0, 1) in closes  # 12 evicted, 13 replayed
    assert ("close", RING, GapStatus.FILLED, 16, 0, 0) in closes
    assert tr.contiguous == 30 and tr.complete


def test_resume_finished_is_reported_and_given_up_seqs_are_counted() -> None:
    """Every resume ends with resume_finished; the seqs given up are counted."""
    sink, clock = Sink(), Clock()
    tr = SeqTracker(sink, clock, reorder_max=4, reorder_ns=100 * MS)
    tr.restore("E1", 10, since_ns=clock.now_ns() - 5_000 * MS)
    tr.on_subscribe_sent(7, {"epoch": "E1", "from_seq": 11})
    tr.on_welcome("E1", 30, 20)
    tr.on_resume_rejected("SEQ_EVICTED", 20, 30)
    assert sink.resumes == 1
    assert tr.stats["unrecoverable"] == 9 and tr.contiguous == 19  # seqs 11..19 were evicted
    # the resume gap starts when the cursor was persisted (a restart), not at the reconnection
    ring = [h for h in tr.holes if h.kind == RING][0]
    assert ring.from_ns == clock.now_ns() - 5_000 * MS
    # an in-connection resume block is reported at its replay_end, even when nothing was missing
    sink2, clock2 = Sink(), Clock()
    tr2 = SeqTracker(sink2, clock2, reorder_max=4, reorder_ns=100 * MS)
    tr2.restore("E1", 10)
    tr2.on_subscribe_sent(8, {"epoch": "E1", "from_seq": 11})
    tr2.on_welcome("E1", 10, 1)
    tr2.on_replay_begin(8, 11, 10)
    tr2.on_replay_end(8, 10)
    assert sink2.resumes == 1 and tr2.complete
    assert tr2.oldest_open_hole_mono() is None
