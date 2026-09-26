"""Publisher restarts after the wall clock stepped back (api-gateway review follow-up, probe p18).

Epochs and stream ids are UUIDv7: a publisher restarted right after Windows time sync stepped the
clock back gets ids that sort *lower* than its predecessor's. The trackers never compare them by
value: the publisher's heartbeat is the authority, and before any heartbeat arrival order decides."""

from __future__ import annotations

from collections.abc import Callable

from futrisk.bus import CloneSync, FactTracker, Incarnations, LvTracker
from futrisk.bus.fact import CursorUpdate, FactKey
from futrisk.bus.incarnation import MAX_RETIRED, REVERT_AFTER, Verdict
from futrisk.common.clock import SimClock
from futrisk.common.ids import uuid7
from futrisk.db import FactCursor
from futrisk.messages import encode, encode_envelope
from futrisk.messages.common import (
    Ping,
    ReplayBatch,
    ReplayRequest,
    SnapshotBatch,
    SnapshotItem,
    SnapshotRequest,
)
from futrisk.messages.enums import FactStartPolicy, ServiceState
from futrisk.messages.envelope import Envelope, Reply, ok_reply
from futrisk.messages.system import Heartbeat
from futrisk.runtime.testing import ids_clock_stepped_back

PUB = "risk-engine-1"
LV_TOPIC = "risk.snapshot.ACC3"
FACT_TOPIC = "risk.state.ACC3"
KEY: FactKey = (PUB, FACT_TOPIC)


def restarted_ids() -> tuple[str, str]:
    """(id of the running incarnation, id of the incarnation started after a 2-minute clock step back)."""
    old = uuid7()
    with ids_clock_stepped_back(120):
        new = uuid7()
    assert new < old  # the whole point: the newer incarnation sorts lower
    return old, new


def lv(seq: int, epoch: str, topic: str = LV_TOPIC) -> Envelope:
    return Envelope(topic=topic, type="Ping", src=PUB, epoch=epoch, seq=seq, ts_ns=0, payload=encode(Ping()))


def fact(seq: int, stream: str, *, epoch: str = "ep", topic: str = FACT_TOPIC) -> Envelope:
    return Envelope(topic=topic, type="Ping", src=PUB, epoch=epoch, stream=stream, seq=seq, ts_ns=0,
                    payload=encode(Ping()))


def hb(epoch: str, stream: str | None = None, heads: dict[str, int] | None = None) -> Heartbeat:
    return Heartbeat(instance=PUB, service="risk-engine", state=ServiceState.RUNNING, pid=1, epoch=epoch,
                     app_version="t", stream=stream, fact_heads=heads or {}, started_ns=0)


# ---------------------------------------------------------------------------
# Incarnations: the decision rules
# ---------------------------------------------------------------------------


class TestIncarnations:
    def test_arrival_order_before_any_heartbeat(self) -> None:
        inc = Incarnations()
        assert inc.observe("p", "b") is Verdict.FIRST
        assert inc.observe("p", "a") is Verdict.NEW  # never seen: a new incarnation, although "a" < "b"
        assert inc.observe("p", "b") is Verdict.STALE  # replaced
        assert inc.observe("p", "a") is Verdict.CURRENT
        assert inc.observe("p", "0") is Verdict.NEW and inc.current("p") == "0"
        assert not inc.announced("p") and inc.is_retired("p", "a")

    def test_heartbeat_is_the_authority(self) -> None:
        inc = Incarnations()
        assert inc.announce("p", "b") is Verdict.FIRST and inc.announced("p")
        assert inc.observe("p", "a") is Verdict.STALE  # not announced (yet): envelopes cannot switch any more
        assert inc.announce("p", "a") is Verdict.NEW  # the heartbeat switches, whatever the order
        assert inc.observe("p", "a") is Verdict.CURRENT
        assert inc.observe("p", "b") is Verdict.STALE

    def test_queued_heartbeat_of_the_old_incarnation_is_ignored(self) -> None:
        inc = Incarnations()
        inc.announce("p", "b")
        assert inc.announce("p", "a") is Verdict.NEW
        assert inc.announce("p", "b") is Verdict.STALE  # the old process' last heartbeat, delivered late
        assert inc.announce("p", "a") is Verdict.CURRENT  # the live one keeps heartbeating: resets the count
        assert inc.announce("p", "b") is Verdict.STALE
        assert inc.current("p") == "a"

    def test_repeated_heartbeats_heal_a_wrong_switch(self) -> None:
        inc = Incarnations()
        assert inc.observe("p", "live") is Verdict.FIRST
        assert inc.observe("p", "residual") is Verdict.NEW  # a queued message of a dead process seen first...
        for _ in range(REVERT_AFTER - 1):
            assert inc.announce("p", "live") is Verdict.STALE
        assert inc.announce("p", "live") is Verdict.NEW  # ...corrected by the live heartbeats
        assert inc.observe("p", "residual") is Verdict.STALE and inc.current("p") == "live"

    def test_retire_and_bounded_memory(self) -> None:
        inc = Incarnations()
        inc.retire("p", "persisted-old")
        assert inc.observe("p", "persisted-old") is Verdict.FIRST  # nothing current yet: adopted
        inc.retire("p", "persisted-old")  # current: no-op
        assert inc.observe("p", "persisted-old") is Verdict.CURRENT
        for i in range(MAX_RETIRED + 5):
            inc.observe("p", f"id-{i:03d}")
        assert not inc.is_retired("p", "persisted-old") and inc.is_retired("p", f"id-{MAX_RETIRED:03d}")
        inc.forget("p")
        assert inc.current("p") is None


# ---------------------------------------------------------------------------
# LV consumers
# ---------------------------------------------------------------------------


class TestLvRestartWithClockSteppedBack:
    def test_probe_p18_without_heartbeats(self) -> None:
        old, new = restarted_ids()
        t = LvTracker()
        assert t.on_lv(lv(50, old)).apply
        applied = sum(t.on_lv(lv(s, new)).apply for s in range(1, 101))
        assert applied == 100 and t.stale_epoch == 0 and t.epoch(PUB) == new
        assert not t.on_lv(lv(51, old)).apply and t.stale_epoch == 1  # late message of the old process

    def test_heartbeat_announces_the_lower_epoch(self) -> None:
        old, new = restarted_ids()
        t = LvTracker()
        assert not t.on_heartbeat(hb(old))
        assert t.on_lv(lv(50, old)).apply
        # restart: the new process' first messages overtake its first heartbeat -> not applied yet
        assert not t.on_lv(lv(1, new)).apply and t.stale_epoch == 1
        assert t.on_heartbeat(hb(new))  # switch (re-snapshot)
        assert t.expected(PUB, LV_TOPIC) is None
        assert [t.on_lv(lv(s, new)).apply for s in (2, 3)] == [True, True]
        assert not t.on_lv(lv(51, old)).apply  # queued message of the old process
        assert not t.on_heartbeat(hb(old)) and t.stale_heartbeats == 1  # queued heartbeat of the old process
        assert t.on_lv(lv(4, new)).apply and t.epoch(PUB) == new

    def test_fact_topics_follow_the_announced_stream(self) -> None:
        (e1, e2), (s1, s2) = restarted_ids(), restarted_ids()
        t = LvTracker()
        t.on_heartbeat(hb(e1, s1))
        assert t.on_lv(fact(10, s1, epoch=e1)).apply
        assert not t.on_lv(fact(1, s2, epoch=e2)).apply  # database recreated: new stream before its heartbeat
        assert t.on_heartbeat(hb(e2, s2))
        r = t.on_lv(fact(2, s2, epoch=e2))
        assert r.apply and r.stream_changed and t.expected(PUB, FACT_TOPIC) == 3
        assert not t.on_lv(fact(11, s1, epoch=e1)).apply and t.stale_stream == 2
        assert t.accept_snapshot_item(fact(3, s2, epoch=e2)) and not t.accept_snapshot_item(fact(12, s1, epoch=e1))


class _Snapshots:
    """Scripted ``ctl.snapshot`` transport for CloneSync."""

    def __init__(self) -> None:
        self.pending: list[tuple[SnapshotRequest, Callable[[Reply], None]]] = []

    def __call__(self, publisher: str, req: SnapshotRequest, cb: Callable[[Reply], None]) -> None:
        self.pending.append((req, cb))

    def serve(self, *envs: Envelope) -> None:
        _, cb = self.pending.pop(0)
        items = [SnapshotItem(topic=e.topic, seq=e.seq, epoch=e.epoch, envelope=encode_envelope(e)) for e in envs]
        cb(ok_reply(SnapshotBatch(items=items, as_of_ns=0)))


class TestCloneSyncRestart:
    def test_lower_epoch_after_restart_resyncs_on_heartbeat(self) -> None:
        old, new = restarted_ids()
        snaps = _Snapshots()
        got: list[tuple[str, int, str]] = []
        clone = CloneSync(PUB, ["risk.snapshot."], lambda e: got.append((e.topic, e.seq, e.epoch)), snaps,
                          clock=SimClock(0))
        clone.start()
        snaps.serve(lv(5, old))
        clone.on_heartbeat(hb(old))
        clone.on_message(lv(6, old))
        assert clone.state == "LIVE" and got == [(LV_TOPIC, 5, old), (LV_TOPIC, 6, old)]
        # publisher restarted with an epoch that sorts lower
        clone.on_message(lv(1, new))  # before its heartbeat: held back
        assert len(got) == 2 and not snaps.pending
        clone.on_heartbeat(hb(new))
        assert clone.state == "RESYNC" and len(snaps.pending) == 1
        snaps.serve(lv(1, new), lv(2, new, "risk.snapshot.ACC4"))
        clone.on_message(lv(3, new))
        clone.on_message(lv(7, old))  # queued message of the old process
        clone.on_heartbeat(hb(old))  # queued heartbeat of the old process: no resync
        assert clone.state == "LIVE" and not snaps.pending
        assert got[2:] == [(LV_TOPIC, 1, new), ("risk.snapshot.ACC4", 2, new), (LV_TOPIC, 3, new)]

    def test_restart_while_syncing_asks_again(self) -> None:
        old, new = restarted_ids()
        snaps = _Snapshots()
        got: list[tuple[int, str]] = []
        clone = CloneSync(PUB, ["risk.snapshot."], lambda e: got.append((e.seq, e.epoch)), snaps, clock=SimClock(0))
        clone.on_heartbeat(hb(old))
        clone.start()
        clone.on_heartbeat(hb(new))  # restarted before the first snapshot was answered
        assert len(snaps.pending) == 2
        stale_req = snaps.pending.pop(0)
        old_item = SnapshotItem(topic=LV_TOPIC, seq=9, epoch=old, envelope=encode_envelope(lv(9, old)))
        stale_req[1](ok_reply(SnapshotBatch(items=[old_item], as_of_ns=0)))  # reply of the old process: ignored
        snaps.serve(lv(1, new))
        assert clone.state == "LIVE" and got == [(1, new)]


# ---------------------------------------------------------------------------
# FACT consumers
# ---------------------------------------------------------------------------


class _Replays:
    """Scripted ``ctl.replay`` transport serving one stream's log."""

    def __init__(self) -> None:
        self.pending: list[tuple[ReplayRequest, Callable[[Reply], None]]] = []

    def __call__(self, src: str, req: ReplayRequest, cb: Callable[[Reply], None]) -> None:
        self.pending.append((req, cb))

    def serve(self, stream: str) -> ReplayRequest:
        req, cb = self.pending.pop(0)
        hi = req.to_seq or req.from_seq
        envs = [encode_envelope(fact(s, stream, topic=req.topic)) for s in range(req.from_seq, hi + 1)]
        cb(ok_reply(ReplayBatch(topic=req.topic, envelopes=envs, next_seq=hi + 1, complete=True)))
        return req


def _tracker(
    replays: _Replays, applied: list[tuple[str, int]], *, persisted: tuple[str, int] | None = None,
    changes: list[tuple[str | None, str, int]] | None = None,
) -> FactTracker:
    cursors = {}
    if persisted is not None:
        cursors = {KEY: FactCursor(PUB, FACT_TOPIC, persisted[0], persisted[1], FactStartPolicy.FROM_BEGINNING, 0)}
    t = FactTracker(replays, clock=SimClock(0), cursors=cursors)

    def apply(e: Envelope, c: CursorUpdate) -> None:
        applied.append((c.stream, e.seq))

    def on_change(key: FactKey, old: str | None, new: str, nxt: int, done: Callable[[], None]) -> None:
        assert changes is not None
        changes.append((old, new, nxt))
        done()

    t.subscribe(PUB, "risk.state.", policy=FactStartPolicy.FROM_BEGINNING, apply=apply,
                on_stream_change=on_change if changes is not None else None)
    return t


class TestFactRestartWithClockSteppedBack:
    def test_heartbeat_announced_lower_stream_runs_stream_change(self) -> None:
        s1, s2 = restarted_ids()
        rp, applied, changes = _Replays(), [], []
        t = _tracker(rp, applied, persisted=(s1, 10), changes=changes)
        t.on_heartbeat(hb("e1", s1, {FACT_TOPIC: 10}))
        t.on_fact(fact(3, s2))  # restored database: new stream, its facts overtake its heartbeat
        assert applied == [] and t.stats.stale_stream == 1 and not rp.pending
        t.on_heartbeat(hb("e2", s2, {FACT_TOPIC: 3}))
        assert changes == [(s1, s2, 4)] and t.stream_of(PUB, FACT_TOPIC) == s2
        t.on_fact(fact(4, s2))
        t.on_fact(fact(11, s1))  # queued fact of the old process
        t.on_heartbeat(hb("e1", s1, {FACT_TOPIC: 11}))  # queued heartbeat of the old process
        t.on_fact(fact(5, s2))
        assert applied == [(s2, 4), (s2, 5)] and changes == [(s1, s2, 4)] and t.stats.stale_stream == 3

    def test_without_callback_the_lower_stream_is_replayed_from_1(self) -> None:
        s1, s2 = restarted_ids()
        rp, applied = _Replays(), []
        t = _tracker(rp, applied, persisted=(s1, 10))
        t.on_heartbeat(hb("e1", s1, {FACT_TOPIC: 10}))
        t.on_heartbeat(hb("e2", s2, {FACT_TOPIC: 3}))
        req = rp.serve(s2)
        assert (req.from_seq, req.to_seq) == (1, 3) and applied == [(s2, 1), (s2, 2), (s2, 3)]

    def test_before_any_heartbeat_arrival_order_switches(self) -> None:
        s1, s2 = restarted_ids()
        rp, applied = _Replays(), []
        t = _tracker(rp, applied, persisted=(s1, 10))
        t.on_fact(fact(2, s2))  # subscriber (re)started, publisher now on a lower stream
        rp.serve(s2)
        t.on_fact(fact(11, s1))  # queued fact of the replaced stream
        assert applied == [(s2, 1), (s2, 2)] and t.stats.stale_stream == 1 and t.stream_of(PUB, FACT_TOPIC) == s2

    def test_keys_missing_from_fact_heads_follow_the_switch(self) -> None:
        s1, s2 = restarted_ids()
        rp, applied, changes = _Replays(), [], []
        t = _tracker(rp, applied, persisted=(s1, 10), changes=changes)
        t.on_fact(fact(11, s1))
        t.on_heartbeat(hb("e2", s2, {}))  # new stream has no row of this topic yet
        assert changes == [(s1, s2, 1)]
        t.on_fact(fact(1, s2))
        assert applied == [(s1, 11), (s2, 1)]
