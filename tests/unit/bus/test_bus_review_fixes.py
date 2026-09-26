"""Regression tests for the code-review fixes in the bus layer: sync-only FACT callbacks, partial
NOT_FOUND, default stream reset, stale streams, FACT ordering in LV consumers, NOT_FOUND detail,
and a randomised run with subscriber restarts and publisher stream changes."""

from __future__ import annotations

import random
import warnings
from collections.abc import Callable
from pathlib import Path

import pytest
from sqlalchemy import MetaData

from futrisk.bus import ControlHandlers, FactTracker, InprocBus, LvTracker
from futrisk.bus.fact import CursorUpdate, FactKey
from futrisk.bus.rpc_common import RequestContext
from futrisk.common.clock import SimClock
from futrisk.db import Database, FactCursor, Outbox, add_common_tables
from futrisk.messages import codes, encode, encode_envelope
from futrisk.messages.common import Ping, ReplayBatch, ReplayRequest
from futrisk.messages.enums import FactStartPolicy, ServiceState
from futrisk.messages.envelope import Envelope, Reply, ok_reply
from futrisk.messages.system import Heartbeat

SRC = "tg"
TOPIC = "td.exec.ACC1"
KEY = (SRC, TOPIC)
MS = 1_000_000


def env(seq: int, *, stream: str | None = "S1", epoch: str = "ep", topic: str = TOPIC) -> Envelope:
    return Envelope(topic=topic, type="Ping", src=SRC, epoch=epoch, stream=stream, seq=seq, ts_ns=seq, payload=encode(Ping()))


def hb(heads: dict[str, int], *, stream: str | None = "S1") -> Heartbeat:
    return Heartbeat(
        instance=SRC, service="trade-gateway", state=ServiceState.RUNNING, pid=1, epoch="ep",
        app_version="t", stream=stream, fact_heads=heads, started_ns=0,
    )


def cursor(last_seq: int, stream: str = "S1") -> dict[FactKey, FactCursor]:
    return {KEY: FactCursor(SRC, TOPIC, stream, last_seq, FactStartPolicy.FROM_BEGINNING, 0)}


# ---------------------------------------------------------------------------
# Item 3: callbacks must be synchronous
# ---------------------------------------------------------------------------


class TestSyncCallbacks:
    def test_async_apply_rejected_at_subscribe(self) -> None:
        async def apply(e: Envelope, c: CursorUpdate) -> None:
            return None

        t = FactTracker(lambda *a: None, clock=SimClock(0))
        with pytest.raises(TypeError, match="synchronous"):
            t.subscribe(SRC, "td.exec.", policy="FROM_BEGINNING", apply=apply)
        with pytest.raises(TypeError):
            t.subscribe(SRC, "td.exec.", policy="FROM_BEGINNING", apply=lambda e, c: None, on_expired=_async_expired)

    def test_awaitable_result_rejected_and_cursor_kept(self) -> None:
        async def work() -> None:
            return None

        t = FactTracker(lambda *a: None, clock=SimClock(0), cursors=cursor(0))
        t.subscribe(SRC, "td.exec.", policy="FROM_BEGINNING", apply=lambda e, c: work())  # type: ignore[arg-type,return-value]
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            with pytest.raises(TypeError, match="awaitable"):
                t.on_fact(env(1))
        assert t.expected(SRC, TOPIC) == 1 and t.stats.applied == 0


async def _async_expired(key: FactKey, lo: int, hi: int, done: Callable[[], None]) -> None:
    return None


# ---------------------------------------------------------------------------
# Item 4: NOT_FOUND expires only the purged part (real outbox + ctl.replay)
# ---------------------------------------------------------------------------


def _ctx() -> RequestContext:
    return RequestContext(
        command="ctl.replay", requester="x", identity=b"x", corr_id=None, received_ns=0, received_mono_ns=0,
        envelope=Envelope(topic="ctl.replay", type="ReplayRequest", src="x", epoch="e", ts_ns=0),
    )


class TestPartialExpiry:
    def test_only_purged_part_is_expired(self, tmp_path: Path) -> None:
        md = MetaData()
        add_common_tables(md)
        db = Database(tmp_path / "td.db", md, synchronous="FULL", clock=SimClock(1))
        db.open()
        try:
            ob = Outbox(db, instance=SRC)
            for i in range(10):
                db.write(lambda c, i=i: ob.append_msg(c, TOPIC, Ping(), epoch="e", created_ns=i + 1))
            ids = [r.outbox_id for r in ob.fetch_unpublished()]
            db.write(lambda c: ob.mark_published(c, ids, 100))
            db.write(lambda c: ob.purge_published_before(c, 101, limit=5))  # retention removed 1..5
            status = Heartbeat(instance=SRC, service="tg", state=ServiceState.RUNNING, pid=1, epoch="e", app_version="t", started_ns=0)
            ctl = ControlHandlers(
                instance=SRC, clock=SimClock(0), state=lambda: ServiceState.RUNNING, status=lambda: status,
                lv_cache=lambda: None, outbox=lambda: ob, on_shutdown=lambda r: None,
            )
            nf = ctl.replay("ctl.replay", ReplayRequest(topic=TOPIC, from_seq=2, to_seq=8), _ctx())
            assert nf.code == codes.NOT_FOUND and nf.open_as(ReplayBatch).next_seq == 6
            applied: list[int] = []
            expired: list[tuple[int, int]] = []
            pending: list[Callable[[], None]] = []
            tr = FactTracker(lambda src, req, cb: cb(ctl.replay("ctl.replay", req, _ctx())), clock=SimClock(0))
            tr.subscribe(
                SRC, "td.exec.", policy="FROM_BEGINNING", apply=lambda e, c: applied.append(e.seq),
                on_expired=lambda k, lo, hi, done: (expired.append((lo, hi)), pending.append(done)),
            )
            tr.on_heartbeat(hb({TOPIC: 10}, stream=db.stream_id))
            tr.on_heartbeat(hb({TOPIC: 10}, stream=db.stream_id))  # history not arriving live: repair
            assert expired == [(1, 5)] and applied == [] and tr.holding(SRC, TOPIC)
            pending.pop()()  # domain fallback for 1..5 done
            assert applied == [6, 7, 8, 9, 10] and tr.expected(SRC, TOPIC) == 11
        finally:
            db.close()

    def test_inproc_not_found_detail(self) -> None:
        bus = InprocBus("clearing-worker")
        tg = bus.endpoint("trade-gateway")
        for _ in range(6):
            tg.publish(TOPIC, Ping(), fact=True)
        tg.purge_fact_log(TOPIC, keep_from_seq=4)
        r = bus.request("trade-gateway", "ctl.replay", ReplayRequest(topic=TOPIC, from_seq=1))
        assert r.code == codes.NOT_FOUND and r.open_as(ReplayBatch).next_seq == 4


# ---------------------------------------------------------------------------
# Items 5 and 11: stream changes
# ---------------------------------------------------------------------------


class ScriptedReplay:
    def __init__(self) -> None:
        self.requests: list[tuple[ReplayRequest, Callable[[Reply], None]]] = []

    def __call__(self, src: str, req: ReplayRequest, cb: Callable[[Reply], None]) -> None:
        self.requests.append((req, cb))

    def serve(self, stream: str) -> ReplayRequest:
        req, cb = self.requests.pop(0)
        hi = req.to_seq or req.from_seq
        seqs = list(range(req.from_seq, hi + 1))
        cb(ok_reply(ReplayBatch(topic=req.topic, envelopes=[encode_envelope(env(s, stream=stream)) for s in seqs], next_seq=hi + 1, complete=True)))
        return req


class TestStreams:
    def test_new_stream_without_callback_replays_from_seq_1(self) -> None:
        rp = ScriptedReplay()
        applied: list[tuple[str, int]] = []
        t = FactTracker(rp, clock=SimClock(0), cursors=cursor(50))
        t.subscribe(SRC, "td.exec.", policy="FROM_BEGINNING", apply=lambda e, c: applied.append((c.stream, e.seq)))
        t.on_fact(env(3, stream="S2"))  # restored publisher: new stream, seqs restart
        req = rp.serve("S2")
        assert (req.from_seq, req.to_seq) == (1, 2)
        assert applied == [("S2", 1), ("S2", 2), ("S2", 3)]

    def test_new_stream_in_heartbeat_without_callback(self) -> None:
        rp = ScriptedReplay()
        applied: list[int] = []
        t = FactTracker(rp, clock=SimClock(0), cursors=cursor(50))
        t.subscribe(SRC, "td.exec.", policy="FROM_BEGINNING", apply=lambda e, c: applied.append(e.seq))
        t.on_heartbeat(hb({TOPIC: 4}, stream="S2"))
        req = rp.serve("S2")
        assert (req.from_seq, req.to_seq) == (1, 4) and applied == [1, 2, 3, 4]

    def test_stale_old_stream_message_and_heartbeat_dropped(self) -> None:
        changes: list[tuple[str | None, str, int]] = []
        applied: list[tuple[str, int]] = []
        rp = ScriptedReplay()
        t = FactTracker(rp, clock=SimClock(0))
        t.subscribe(
            SRC, "td.exec.", policy="FROM_BEGINNING", apply=lambda e, c: applied.append((e.stream or "", e.seq)),
            on_stream_change=lambda k, old, new, nxt, done: (changes.append((old, new, nxt)), done()),
        )
        t.on_heartbeat(hb({TOPIC: 0}, stream="A"))
        for s in range(1, 11):
            t.on_fact(env(s, stream="A"))
        t.on_heartbeat(hb({TOPIC: 8}, stream="B"))  # restored from backup: stream B, head 8
        t.on_fact(env(10, stream="B"))  # B/9 lost -> buffered, repair 9
        t.on_fact(env(11, stream="A"))  # stale message of the dead incarnation
        t.on_heartbeat(hb({TOPIC: 10}, stream="A"))  # stale heartbeat
        assert changes == [("A", "B", 9)] and t.stats.stale_stream == 2
        assert t.stream_of(SRC, TOPIC) == "B"
        rp.serve("B")
        assert applied[10:] == [("B", 9), ("B", 10)]


# ---------------------------------------------------------------------------
# Item 2: FACT envelopes in LV consumers are ordered by (stream, seq)
# ---------------------------------------------------------------------------


class TestLvFactOrdering:
    def test_fact_envelopes_ignore_epochs(self) -> None:
        t = LvTracker()
        assert t.on_lv(env(5, stream=None, epoch="e2", topic="risk.snapshot.ACC1")).apply  # LV, new epoch e2
        # FACT rows appended by the previous incarnation (epoch e1) and republished after the restart
        assert t.on_lv(env(7, stream="S1", epoch="e1", topic="risk.state.ACC1")).apply
        assert t.on_lv(env(8, stream="S1", epoch="e1", topic="risk.state.ACC1")).apply
        assert not t.on_lv(env(8, stream="S1", epoch="e2", topic="risk.state.ACC1")).apply  # duplicate
        # an LV epoch change does not reset FACT positions
        r = t.on_lv(env(1, stream=None, epoch="e3", topic="risk.snapshot.ACC1"))
        assert r.apply and r.epoch_changed
        assert not t.on_lv(env(8, stream="S1", epoch="e3", topic="risk.state.ACC1")).apply
        # newer stream resets, older stream is stale
        r2 = t.on_lv(env(1, stream="S2", epoch="e3", topic="risk.state.ACC1"))
        assert r2.apply and r2.stream_changed
        assert not t.on_lv(env(9, stream="S1", epoch="e3", topic="risk.state.ACC1")).apply
        assert t.stale_stream == 1

    def test_snapshot_fact_item_with_older_epoch_is_accepted(self) -> None:
        t = LvTracker()
        t.on_lv(env(3, stream=None, epoch="e9", topic="risk.snapshot.ACC1"))  # publisher now at epoch e9
        assert t.accept_snapshot_item(env(42, stream="S1", epoch="e1", topic="risk.state.ACC1"))
        assert not t.accept_snapshot_item(env(42, stream="S1", epoch="e1", topic="risk.state.ACC1"))
        assert t.expected(SRC, "risk.state.ACC1") == 43


# ---------------------------------------------------------------------------
# Randomised: drops/dups/reorders + subscriber restarts + publisher stream changes
# ---------------------------------------------------------------------------


def _stream_name(no: int, descending: bool) -> str:
    """Stream ids in creation order; ``descending``: each new one sorts lower (wall clock stepped back)."""
    return f"S{(999 - no) if descending else no:03d}"


def _scenario(seed: int, descending: bool = False) -> tuple[list[tuple[str, int]], str, int]:
    rng = random.Random(seed)
    clock = SimClock(0)
    stream_no = 1
    stream = _stream_name(stream_no, descending)
    log: list[Envelope] = []
    pending: list[tuple[ReplayRequest, Callable[[Reply], None]]] = []
    applied: list[tuple[str, int]] = []
    persisted: dict[FactKey, FactCursor] = {}
    delayed: list[Envelope] = []

    def request(src: str, req: ReplayRequest, cb: Callable[[Reply], None]) -> None:
        pending.append((req, cb))

    def apply(e: Envelope, c: CursorUpdate) -> None:  # effects + cursor "committed" together
        applied.append((c.stream, e.seq))
        persisted[c.key] = FactCursor(c.src, c.topic, c.stream, c.last_seq, c.start_policy, 0)

    def new_tracker() -> FactTracker:
        t = FactTracker(request, clock=clock, cursors=dict(persisted), rng=random.Random(seed), replay_page=7)
        t.subscribe(SRC, TOPIC, policy=FactStartPolicy.FROM_BEGINNING, apply=apply)
        return t

    tracker = new_tracker()

    def serve_one() -> None:
        req, cb = pending.pop(rng.randrange(len(pending)))
        if rng.random() < 0.1:
            cb(Reply(ok=False, code=codes.TIMEOUT))
            return
        hi = min(req.to_seq or len(log), len(log))
        items = log[req.from_seq - 1 : hi][: req.limit]
        last = items[-1].seq if items else req.from_seq - 1
        cb(ok_reply(ReplayBatch(topic=TOPIC, envelopes=[encode_envelope(e) for e in items], next_seq=last + 1, complete=last >= hi)))

    for step in range(400):
        clock.advance(20 * MS)
        r = rng.random()
        if r < 0.45:
            e = env(len(log) + 1, stream=stream)
            log.append(e)
            fate = rng.random()
            if fate < 0.15:
                pass  # lost
            elif fate < 0.3:
                delayed.append(e)
            else:
                tracker.on_fact(e)
                if fate < 0.4:
                    tracker.on_fact(e)
        elif r < 0.58 and delayed:
            tracker.on_fact(delayed.pop(rng.randrange(len(delayed))))  # may be a stale old-stream message
        elif r < 0.72 and pending:
            serve_one()
        elif r < 0.85 or step == 0:
            tracker.on_heartbeat(hb({TOPIC: len(log)}, stream=stream))
        elif r < 0.9:
            tracker = new_tracker()  # subscriber crash/restart: resumes from the persisted cursor
            pending.clear()
            delayed.clear()
        elif r < 0.92:
            stream_no += 1  # publisher database restored/recreated: new stream, seqs restart
            stream = _stream_name(stream_no, descending)
            log = []
        tracker.poll_timers()
    for e in delayed:
        tracker.on_fact(e)
    for _ in range(500):
        tracker.on_heartbeat(hb({TOPIC: len(log)}, stream=stream))
        while pending:
            serve_one()
        clock.advance(10_000 * MS)
        tracker.poll_timers()
        if not pending and tracker.expected(SRC, TOPIC) == len(log) + 1 and tracker.stream_of(SRC, TOPIC) == stream:
            break
    return applied, stream, len(log)


class TestRandomisedRestartsAndStreams:
    @pytest.mark.parametrize("descending", [False, True], ids=["ids-ascending", "clock-stepped-back"])
    def test_exactly_once_per_stream(self, descending: bool) -> None:
        for seed in range(40):
            applied, final_stream, n = _scenario(seed, descending)
            final = [s for st, s in applied if st == final_stream]
            assert final == list(range(1, n + 1)), f"seed {seed}: final stream {final[:15]} n={n}"
            # never an older stream after a newer one (creation order, not id order); every stream
            # applied as exactly 1..k (no duplicates across subscriber restarts, no gaps, no reordering)
            created = {_stream_name(i, descending): i for i in range(1, 500)}
            prev = 0
            per_stream: dict[str, list[int]] = {}
            for st, s in applied:
                assert created[st] >= prev, f"seed {seed}: stream went back to {st}"
                prev = created[st]
                per_stream.setdefault(st, []).append(s)
            for st, seqs in per_stream.items():
                assert seqs == list(range(1, len(seqs) + 1)), f"seed {seed}: {st} {seqs[:15]}"


# ---------------------------------------------------------------------------
# Found while re-running the whole suite: concurrent requests to a peer that is down
# ---------------------------------------------------------------------------


class TestAsyncClientDownPeer:
    async def test_concurrent_requests_fail_fast_and_never_wedge_the_dealer(self) -> None:
        import asyncio
        import time

        import zmq.asyncio

        from futrisk.bus import AsyncRpcClient, AsyncRpcServer
        from futrisk.messages.common import Pong
        from futrisk.runtime.testing import free_tcp_port

        actx = zmq.asyncio.Context()
        addr = f"tcp://127.0.0.1:{free_tcp_port()}"  # nobody listens yet
        client = AsyncRpcClient(actx, lambda t: addr, identity="api-gateway")
        calls: list[str] = []
        server: AsyncRpcServer | None = None
        try:
            t0 = time.monotonic()
            replies = await asyncio.wait_for(
                asyncio.gather(*(client.request("risk-engine-0", "ctl.ping", Ping(), retries=0) for _ in range(6))), 3
            )
            assert [r.code for r in replies] == [codes.UNAVAILABLE] * 6 and time.monotonic() - t0 < 1.5
            server = AsyncRpcServer(actx, addr, instance="risk-engine-0", epoch="e")
            server.add_handler("ctl.ping", lambda c, p, x: (calls.append("ping"), ok_reply(Pong(instance="re0", state="RUNNING", ts_ns=1)))[1])
            await server.start()
            r = await asyncio.wait_for(client.request("risk-engine-0", "ctl.ping", Ping(), retries=5), 5)
            assert r.ok
            await asyncio.sleep(0.2)
            assert calls == ["ping"]  # none of the failed requests was delivered late
        finally:
            await client.close()
            if server is not None:
                await server.close()
            actx.destroy(linger=0)
