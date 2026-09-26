"""FactTracker (02 §8.2): ordering, duplicates, gap repair, NOT_FOUND fallback, tail gaps,
start policies with buffering before the first heartbeat, stream changes, retries."""

from __future__ import annotations

import random
from collections.abc import Callable
from dataclasses import dataclass, field

import pytest

from futrisk.bus import FactTracker, InprocBus
from futrisk.bus.fact import CursorUpdate, FactKey
from futrisk.common.clock import SimClock
from futrisk.db.cursors import FactCursor
from futrisk.messages import codes, encode, encode_envelope
from futrisk.messages.common import Ping, ReplayBatch, ReplayRequest
from futrisk.messages.enums import FactStartPolicy, ServiceState
from futrisk.messages.envelope import Envelope, Reply, err_reply, ok_reply
from futrisk.messages.system import Heartbeat

SRC = "tg"
TOPIC = "td.exec.ACC1"
KEY = (SRC, TOPIC)
MS = 1_000_000


def env(seq: int, *, topic: str = TOPIC, stream: str | None = "S1", src: str = SRC) -> Envelope:
    return Envelope(topic=topic, type="Ping", src=src, epoch="ep", stream=stream, seq=seq, ts_ns=seq, payload=encode(Ping()))


def hb(heads: dict[str, int], *, stream: str | None = "S1", instance: str = SRC) -> Heartbeat:
    return Heartbeat(
        instance=instance, service="trade-gateway", state=ServiceState.RUNNING, pid=1, epoch="ep",
        app_version="t", stream=stream, fact_heads=heads, started_ns=0,
    )


@dataclass
class Harness:
    """Scripted publisher: records replay requests and answers them on demand."""

    policy: FactStartPolicy = FactStartPolicy.FROM_BEGINNING
    prefix: str = "td.exec."
    cursors: dict[FactKey, FactCursor] | None = None
    hold_fallback: bool = False
    clock: SimClock = field(default_factory=lambda: SimClock(0))
    requests: list[tuple[str, ReplayRequest, Callable[[Reply], None]]] = field(default_factory=list)
    applied: list[tuple[str, int, str]] = field(default_factory=list)
    updates: list[CursorUpdate] = field(default_factory=list)
    expired: list[tuple[FactKey, int, int]] = field(default_factory=list)
    stream_changes: list[tuple[FactKey, str | None, str, int]] = field(default_factory=list)
    failed: list[tuple[FactKey, int, int, str]] = field(default_factory=list)
    recovered: list[FactKey] = field(default_factory=list)
    pending_done: list[Callable[[], None]] = field(default_factory=list)
    fail_apply_at: int | None = None

    def __post_init__(self) -> None:
        self.tracker = FactTracker(self._request, clock=self.clock, cursors=self.cursors, rng=random.Random(7), replay_page=500)
        self.tracker.subscribe(
            SRC,
            self.prefix,
            policy=self.policy,
            apply=self._apply,
            on_expired=self._expired,
            on_stream_change=self._stream_change,
            on_repair_failed=lambda k, lo, hi, r: self.failed.append((k, lo, hi, r.code)),
            on_repair_ok=self.recovered.append,
        )

    # callbacks ----------------------------------------------------------
    def _request(self, src: str, req: ReplayRequest, cb: Callable[[Reply], None]) -> None:
        self.requests.append((src, req, cb))

    def _apply(self, e: Envelope, cur: CursorUpdate) -> None:
        if self.fail_apply_at == e.seq:
            raise RuntimeError("db down")
        assert cur.last_seq == e.seq and cur.topic == e.topic and cur.src == e.src
        self.applied.append((e.topic, e.seq, cur.stream))
        self.updates.append(cur)

    def _expired(self, key: FactKey, lo: int, hi: int, done: Callable[[], None]) -> None:
        self.expired.append((key, lo, hi))
        if self.hold_fallback:
            self.pending_done.append(done)
        else:
            done()

    def _stream_change(self, key: FactKey, old: str | None, new: str, nxt: int, done: Callable[[], None]) -> None:
        self.stream_changes.append((key, old, new, nxt))
        if self.hold_fallback:
            self.pending_done.append(done)
        else:
            done()

    # publisher side -----------------------------------------------------
    def serve(self, *, stream: str = "S1", purged_below: int = 0, code: str | None = None, short_by: int = 0) -> ReplayRequest:
        src, req, cb = self.requests.pop(0)
        if code is not None:
            cb(err_reply(code, "scripted"))
            return req
        if req.from_seq < purged_below:
            cb(err_reply(codes.NOT_FOUND, "purged"))
            return req
        hi = req.to_seq if req.to_seq is not None else req.from_seq
        seqs = list(range(req.from_seq, hi + 1 - short_by))[: req.limit]
        batch = ReplayBatch(
            topic=req.topic,
            envelopes=[encode_envelope(env(s, topic=req.topic, stream=stream)) for s in seqs],
            next_seq=(seqs[-1] + 1) if seqs else req.from_seq,
            complete=not seqs or seqs[-1] >= hi or short_by > 0,  # short_by: a short but 'complete' answer
        )
        cb(ok_reply(batch))
        return req

    def seqs(self) -> list[int]:
        return [s for _, s, _ in self.applied]


def cursor(last_seq: int, stream: str = "S1", topic: str = TOPIC) -> dict[FactKey, FactCursor]:
    return {(SRC, topic): FactCursor(SRC, topic, stream, last_seq, FactStartPolicy.FROM_BEGINNING, 0)}


# ---------------------------------------------------------------------------
# Ordering with a persisted cursor
# ---------------------------------------------------------------------------


class TestOrdering:
    def test_in_order_duplicate_and_cursor_values(self) -> None:
        h = Harness(cursors=cursor(5))
        h.tracker.on_fact(env(6))
        h.tracker.on_fact(env(7))
        h.tracker.on_fact(env(6))  # duplicate
        h.tracker.on_fact(env(3))  # old duplicate
        assert h.seqs() == [6, 7] and h.requests == []
        assert h.tracker.stats.duplicates == 2
        assert h.updates[-1] == CursorUpdate(SRC, TOPIC, "S1", 7, FactStartPolicy.FROM_BEGINNING)
        assert h.tracker.expected(SRC, TOPIC) == 8

    def test_gap_triggers_replay_then_drains_buffer(self) -> None:
        h = Harness(cursors=cursor(5))
        h.tracker.on_fact(env(8))
        h.tracker.on_fact(env(9))
        assert h.seqs() == [] and h.tracker.buffered(SRC, TOPIC) == [8, 9]
        assert len(h.requests) == 1  # a second out-of-order message does not duplicate the request
        req = h.serve()
        assert (req.topic, req.from_seq, req.to_seq) == (TOPIC, 6, 7)
        assert h.seqs() == [6, 7, 8, 9] and not h.tracker.repairing(SRC, TOPIC)
        assert h.requests == []

    def test_apply_failure_does_not_advance_cursor(self) -> None:
        h = Harness(cursors=cursor(5), fail_apply_at=6)
        with pytest.raises(RuntimeError):
            h.tracker.on_fact(env(6))
        assert h.tracker.expected(SRC, TOPIC) == 6
        h.fail_apply_at = None
        h.tracker.on_fact(env(6))
        assert h.seqs() == [6]

    def test_unsubscribed_and_invalid_messages_ignored(self) -> None:
        h = Harness(prefix=TOPIC, cursors=cursor(0))
        h.tracker.on_fact(env(1, topic="td.exec.ACC10"))  # exact subscription: ACC10 is not ACC1
        h.tracker.on_fact(env(1, stream=None))
        assert h.seqs() == [] and h.tracker.stats.unsubscribed == 1 and h.tracker.stats.invalid == 1

    def test_multi_page_replay(self) -> None:
        h = Harness(cursors=cursor(0))
        h.tracker.on_fact(env(1201))
        pages = 0
        while h.requests:
            h.serve()
            pages += 1
        assert pages == 3 and h.seqs() == list(range(1, 1202))


# ---------------------------------------------------------------------------
# NOT_FOUND domain fallback
# ---------------------------------------------------------------------------


class TestExpired:
    def test_not_found_holds_until_done_then_skips_gap(self) -> None:
        h = Harness(cursors=cursor(5), hold_fallback=True)
        h.tracker.on_fact(env(10))
        h.serve(purged_below=100)
        assert h.expired == [(KEY, 6, 9)] and h.tracker.holding(SRC, TOPIC)
        h.tracker.on_fact(env(11))  # held: buffered, not applied
        h.tracker.on_heartbeat(hb({TOPIC: 11}))  # no repair while holding
        assert h.seqs() == [] and h.requests == []
        h.pending_done.pop()()
        assert h.seqs() == [10, 11] and h.tracker.expected(SRC, TOPIC) == 12
        assert h.requests == []

    def test_immediate_fallback(self) -> None:
        h = Harness(cursors=cursor(5))
        h.tracker.on_fact(env(10))
        h.serve(purged_below=100)
        assert h.seqs() == [10]


# ---------------------------------------------------------------------------
# Heartbeats: tail gaps and start positions
# ---------------------------------------------------------------------------


class TestHeartbeatAndStart:
    def test_tail_gap_detected_from_fact_heads(self) -> None:
        h = Harness(cursors=cursor(5))
        h.tracker.on_heartbeat(hb({TOPIC: 5, "td.exec.ACC2": 0}))
        assert h.requests == []  # up to date; ACC2 head 0 -> nothing to fetch
        h.tracker.on_heartbeat(hb({TOPIC: 8}))
        assert h.requests == []  # 6..8 may still be in flight on the FACT socket
        h.tracker.on_heartbeat(hb({TOPIC: 8}))  # still missing one heartbeat later: a real tail gap
        req = h.serve()
        assert (req.from_seq, req.to_seq) == (6, 8) and h.seqs() == [6, 7, 8]
        h.tracker.on_heartbeat(hb({TOPIC: 8}))
        assert h.requests == []

    def test_heartbeat_overtaking_facts_causes_no_replay(self) -> None:
        h = Harness(cursors=cursor(5))
        h.tracker.on_heartbeat(hb({TOPIC: 5}))
        for head in range(6, 40):
            h.tracker.on_heartbeat(hb({TOPIC: head}))  # heartbeat announces `head` first ...
            h.tracker.on_fact(env(head))  # ... the fact arrives right after on its own socket
        assert h.requests == [] and h.seqs() == list(range(6, 40))

    def test_restart_catch_up_on_next_heartbeat(self) -> None:
        h = Harness(cursors=cursor(5))  # persisted cursor, publisher moved on while we were down
        h.tracker.on_heartbeat(hb({TOPIC: 9}))
        assert h.requests == []  # 6..9 could still be in flight
        h.tracker.on_heartbeat(hb({TOPIC: 9}))
        req = h.serve()
        assert (req.from_seq, req.to_seq) == (6, 9) and h.seqs() == [6, 7, 8, 9]

    def test_restart_catch_up_on_first_live_message(self) -> None:
        h = Harness(cursors=cursor(5))
        h.tracker.on_heartbeat(hb({TOPIC: 9}))
        h.tracker.on_fact(env(10))  # ordered socket: 6..9 are not coming live
        req = h.serve()
        assert (req.from_seq, req.to_seq) == (6, 9) and h.seqs() == [6, 7, 8, 9, 10]

    def test_from_beginning_replays_from_one(self) -> None:
        h = Harness(policy=FactStartPolicy.FROM_BEGINNING)
        h.tracker.on_heartbeat(hb({TOPIC: 3}))
        assert h.requests == [] and h.tracker.expected(SRC, TOPIC) == 1
        h.tracker.on_heartbeat(hb({TOPIC: 3}))  # history did not arrive live: fetch it
        req = h.serve()
        assert (req.from_seq, req.to_seq) == (1, 3)
        assert h.seqs() == [1, 2, 3] and h.updates[0].start_policy == FactStartPolicy.FROM_BEGINNING

    def test_from_now_buffers_until_first_heartbeat(self) -> None:
        h = Harness(policy=FactStartPolicy.FROM_NOW)
        h.tracker.on_fact(env(11))
        h.tracker.on_fact(env(12))
        assert h.seqs() == [] and h.tracker.expected(SRC, TOPIC) is None
        # heartbeat built before 11/12 were published: head 10
        h.tracker.on_heartbeat(hb({TOPIC: 10}))
        assert h.seqs() == [11, 12] and h.requests == []
        assert h.updates[0].start_policy == FactStartPolicy.FROM_NOW

    def test_from_now_min_of_head_and_buffer(self) -> None:
        h = Harness(policy=FactStartPolicy.FROM_NOW)
        h.tracker.on_fact(env(11))
        h.tracker.on_heartbeat(hb({TOPIC: 12}))  # heartbeat already counts 12: min(13, 11) = 11
        assert h.seqs() == [11] and h.requests == []
        h.tracker.on_heartbeat(hb({TOPIC: 12}))  # 12 still missing one heartbeat later -> tail gap repair
        req = h.serve()
        assert (req.from_seq, req.to_seq) == (12, 12) and h.seqs() == [11, 12]

    def test_from_now_without_messages_starts_after_head(self) -> None:
        h = Harness(policy=FactStartPolicy.FROM_NOW)
        h.tracker.on_heartbeat(hb({TOPIC: 40}))
        assert h.requests == [] and h.tracker.expected(SRC, TOPIC) == 41
        h.tracker.on_fact(env(41))
        assert h.seqs() == [41]

    def test_first_message_after_heartbeat_known(self) -> None:
        h = Harness(policy=FactStartPolicy.FROM_NOW)
        h.tracker.on_heartbeat(hb({"td.exec.ACC2": 7}))  # other topic only
        h.tracker.on_fact(env(1))  # new topic: head unknown (0) -> starts at 1
        assert h.seqs() == [1]

    def test_heartbeat_without_stream_is_ignored(self) -> None:
        h = Harness()
        h.tracker.on_fact(env(1))
        h.tracker.on_heartbeat(hb({}, stream=None))
        assert h.seqs() == [] and h.tracker.buffered(SRC, TOPIC) == [1]


# ---------------------------------------------------------------------------
# Stream changes (publisher DB recreated / restored)
# ---------------------------------------------------------------------------


class TestStreamChange:
    def test_new_stream_in_message_resets_cursor(self) -> None:
        h = Harness(cursors=cursor(50))
        h.tracker.on_fact(env(1, stream="S2"))
        assert h.stream_changes == [(KEY, "S1", "S2", 1)]
        assert h.applied == [(TOPIC, 1, "S2")] and h.updates[-1].stream == "S2"
        h.tracker.on_fact(env(2, stream="S2"))
        assert h.seqs() == [1, 2]

    def test_new_stream_in_heartbeat(self) -> None:
        h = Harness(cursors=cursor(50))
        h.tracker.on_heartbeat(hb({TOPIC: 4}, stream="S2"))
        assert h.stream_changes == [(KEY, "S1", "S2", 5)] and h.requests == []
        h.tracker.on_fact(env(5, stream="S2"))
        assert h.seqs() == [5] and h.tracker.stream_of(SRC, TOPIC) == "S2"

    def test_stream_change_cancels_repair_and_holds(self) -> None:
        h = Harness(cursors=cursor(5), hold_fallback=True)
        h.tracker.on_fact(env(9))  # gap -> replay in flight
        assert len(h.requests) == 1
        h.tracker.on_fact(env(1, stream="S2"))
        assert h.tracker.holding(SRC, TOPIC)
        h.tracker.on_fact(env(2, stream="S2"))
        h.serve()  # stale reply of the old stream's repair
        assert h.tracker.stats.stale_replies == 1 and h.seqs() == []
        h.pending_done.pop()()
        assert h.applied == [(TOPIC, 1, "S2"), (TOPIC, 2, "S2")]


# ---------------------------------------------------------------------------
# Replay failures: backoff, alarms
# ---------------------------------------------------------------------------


class TestRepairRetry:
    def test_timeout_backs_off_and_recovers(self) -> None:
        h = Harness(cursors=cursor(5))
        h.tracker.on_fact(env(8))
        h.serve(code=codes.TIMEOUT)
        assert h.failed == [(KEY, 6, 7, codes.TIMEOUT)]
        due = h.tracker.next_timer_ns()
        assert due is not None and due > h.clock.now_ns()
        h.tracker.poll_timers()
        assert h.requests == []  # still backing off
        h.tracker.on_heartbeat(hb({TOPIC: 8}))
        assert h.requests == []  # heartbeat does not bypass the backoff
        h.clock.set(due)
        h.tracker.poll_timers()
        h.serve(code=codes.UNAVAILABLE)
        assert len(h.failed) == 1  # alarm fired once per gap
        h.clock.advance(10_000 * MS)
        h.tracker.poll_timers()
        h.serve()
        assert h.seqs() == [6, 7, 8] and h.recovered == [KEY]

    def test_short_replay_retries_later(self) -> None:
        h = Harness(cursors=cursor(0))
        h.tracker.on_fact(env(5))
        h.serve(short_by=2)  # publisher returned 1..2 only, complete
        assert h.seqs() == [1, 2] and h.requests == []
        assert h.tracker.next_timer_ns() is not None
        h.clock.advance(10_000 * MS)
        h.tracker.poll_timers()
        req = h.serve()
        assert (req.from_seq, req.to_seq) == (3, 4) and h.seqs() == [1, 2, 3, 4, 5]

    def test_synchronous_requester_exception(self) -> None:
        clock = SimClock(0)
        applied: list[int] = []

        def broken(src: str, req: ReplayRequest, cb: Callable[[Reply], None]) -> None:
            raise ConnectionError("no route")

        t = FactTracker(broken, clock=clock, cursors=cursor(0))
        t.subscribe(SRC, "td.exec.", policy="FROM_BEGINNING", apply=lambda e, c: applied.append(e.seq))
        t.on_fact(env(3))
        assert t.stats.replay_failures == 1 and t.next_timer_ns() is not None and applied == []


# ---------------------------------------------------------------------------
# End to end with the in-process bus as the publisher
# ---------------------------------------------------------------------------


class TestWithInprocBus:
    def test_lossy_link_repaired_by_replay_and_expired_by_purge(self) -> None:
        bus = InprocBus("clearing-worker")
        tg = bus.endpoint("trade-gateway")
        applied: list[int] = []
        expired: list[tuple[int, int]] = []

        def requester(src: str, req: ReplayRequest, cb: Callable[[Reply], None]) -> None:
            cb(bus.request(src, "ctl.replay", req))

        tracker = FactTracker(requester, clock=bus.clock)

        def on_expired(key: FactKey, lo: int, hi: int, done: Callable[[], None]) -> None:
            expired.append((lo, hi))
            done()

        tracker.subscribe(
            "trade-gateway", "td.exec.", policy="FROM_BEGINNING", apply=lambda e, c: applied.append(e.seq), on_expired=on_expired
        )
        drop = {2, 3}
        bus.subscribe("trade-gateway", ["td.exec."], lambda e: None if e.seq in drop else tracker.on_fact(e))
        for _ in range(4):
            tg.publish("td.exec.ACC1", Ping(), fact=True)
        assert applied == []  # no heartbeat yet: buffered
        tracker.on_heartbeat(hb({"td.exec.ACC1": 4}, stream=tg.stream, instance="trade-gateway"))
        assert applied == [1, 2, 3, 4]
        # 5..7 are lost on the wire and purged from the publisher's log (retention)
        drop = {5, 6, 7}
        for _ in range(3):
            tg.publish("td.exec.ACC1", Ping(), fact=True)
        tg.purge_fact_log("td.exec.ACC1", keep_from_seq=8)
        assert expired == [] and applied == [1, 2, 3, 4]
        tg.publish("td.exec.ACC1", Ping(), fact=True)  # 8 arrives live -> gap 5..7 -> NOT_FOUND
        assert expired == [(5, 7)] and applied == [1, 2, 3, 4, 8]
        # tail gap: 9 lost, detected from the heartbeat and replayed
        drop = {9}
        tg.publish("td.exec.ACC1", Ping(), fact=True)
        tracker.on_heartbeat(hb({"td.exec.ACC1": 9}, stream=tg.stream, instance="trade-gateway"))
        assert applied == [1, 2, 3, 4, 8]  # not yet: 9 may still be in flight
        tracker.on_heartbeat(hb({"td.exec.ACC1": 9}, stream=tg.stream, instance="trade-gateway"))
        assert applied == [1, 2, 3, 4, 8, 9]


# ---------------------------------------------------------------------------
# Randomised network: drops, duplicates, reordering, late replay answers
# ---------------------------------------------------------------------------


def _random_scenario(seed: int, policy: FactStartPolicy) -> tuple[list[int], int]:
    """Lossy, duplicating, reordering delivery with late (sometimes failing) replay answers."""
    rng = random.Random(seed)
    clock = SimClock(0)
    log: list[Envelope] = []
    pending_replays: list[tuple[ReplayRequest, Callable[[Reply], None]]] = []
    applied: list[int] = []

    def request(src: str, req: ReplayRequest, cb: Callable[[Reply], None]) -> None:
        pending_replays.append((req, cb))

    tracker = FactTracker(request, clock=clock, rng=random.Random(seed), replay_page=7)
    tracker.subscribe(SRC, TOPIC, policy=policy, apply=lambda e, c: applied.append(e.seq))
    delayed: list[Envelope] = []

    def serve_one() -> None:
        req, cb = pending_replays.pop(rng.randrange(len(pending_replays)))
        if rng.random() < 0.15:
            cb(err_reply(codes.TIMEOUT, "lost"))
            return
        hi = min(req.to_seq or len(log), len(log))
        items = log[req.from_seq - 1 : hi][: req.limit]
        last = items[-1].seq if items else req.from_seq - 1
        batch = ReplayBatch(topic=TOPIC, envelopes=[encode_envelope(e) for e in items], next_seq=last + 1, complete=last >= hi)
        cb(ok_reply(batch))

    for step in range(300):
        clock.advance(20 * MS)
        r = rng.random()
        if r < 0.5:
            e = env(len(log) + 1)
            log.append(e)
            fate = rng.random()
            if fate < 0.15:
                continue  # dropped
            if fate < 0.3:
                delayed.append(e)
                continue
            tracker.on_fact(e)
            if fate < 0.4:
                tracker.on_fact(e)  # duplicate
        elif r < 0.65 and delayed:
            tracker.on_fact(delayed.pop(rng.randrange(len(delayed))))
        elif r < 0.8 and pending_replays:
            serve_one()
        elif r < 0.9 or step == 0:
            tracker.on_heartbeat(hb({TOPIC: len(log)}))
        tracker.poll_timers()
    # drain: late messages, heartbeats, replays and retries until quiet
    for e in delayed:
        tracker.on_fact(e)
    for _ in range(500):
        tracker.on_heartbeat(hb({TOPIC: len(log)}))
        while pending_replays:
            serve_one()
        clock.advance(10_000 * MS)
        tracker.poll_timers()
        if not pending_replays and tracker.expected(SRC, TOPIC) == len(log) + 1:
            break
    return applied, len(log)


class TestRandomisedDelivery:
    @pytest.mark.parametrize("policy", [FactStartPolicy.FROM_BEGINNING, FactStartPolicy.FROM_NOW])
    def test_exactly_once_in_order(self, policy: FactStartPolicy) -> None:
        for seed in range(30):
            applied, n = _random_scenario(seed, policy)
            start = applied[0] if applied else 1
            assert applied == list(range(start, n + 1)), f"seed {seed}: {applied[:20]}"
            if policy == FactStartPolicy.FROM_BEGINNING:
                assert start == 1, f"seed {seed}"
