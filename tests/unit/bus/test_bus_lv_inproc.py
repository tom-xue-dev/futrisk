"""LV tracking and late-joiner Clone (02 §4.5-§4.6), InprocBus (02 §10), whitelist (02 §4.11),
heartbeat builder and peer liveness (02 §4.8, §7.9)."""

from __future__ import annotations

import random
from collections.abc import Callable

import pytest

from futrisk.bus import (
    CloneSync,
    HeartbeatBuilder,
    HeartbeatExtras,
    InprocBus,
    LvTracker,
    PeerLiveness,
    Whitelist,
)
from futrisk.bus.heartbeat import DOWN, UNKNOWN, UP, HeartbeatThread, payload_versions
from futrisk.bus.rpc_common import DEFERRED
from futrisk.bus.timeouts import default_timeout_ms, retry_delay_ms
from futrisk.common.clock import SimClock
from futrisk.messages import codes, encode, open_payload
from futrisk.messages.common import Ping, Pong, ReplayBatch, ReplayRequest, SnapshotBatch, SnapshotRequest
from futrisk.messages.enums import ServiceState
from futrisk.messages.envelope import Envelope, Reply, ok_reply
from futrisk.messages.system import Heartbeat, LatencySummary


def lv(topic: str, seq: int, epoch: str = "e1", src: str = "md") -> Envelope:
    return Envelope(topic=topic, type="Ping", src=src, epoch=epoch, seq=seq, ts_ns=seq, payload=encode(Ping()))


# ---------------------------------------------------------------------------
# LvTracker
# ---------------------------------------------------------------------------


class TestLvTracker:
    def test_rules(self) -> None:
        t = LvTracker()
        assert t.on_lv(lv("md.tick.a", 1)).first
        assert t.on_lv(lv("md.tick.a", 2)).apply
        r = t.on_lv(lv("md.tick.a", 5))
        assert r.apply and r.skipped == 2  # LV: gaps are fine
        assert not t.on_lv(lv("md.tick.a", 4)).apply  # older -> duplicate
        assert not t.on_lv(lv("md.tick.a", 5)).apply
        assert t.on_lv(lv("md.tick.b", 1)).apply
        assert t.expected("md", "md.tick.a") == 6

    def test_epoch_change_resets_and_stale_epoch_dropped(self) -> None:
        t = LvTracker()
        t.on_lv(lv("md.tick.a", 50, "e1"))
        t.on_lv(lv("md.tick.b", 70, "e1"))
        r = t.on_lv(lv("md.tick.a", 1, "e2"))  # publisher restarted: seqs restart at 1
        assert r.apply and r.epoch_changed
        assert t.expected("md", "md.tick.b") is None and t.epoch("md") == "e2"
        assert not t.on_lv(lv("md.tick.b", 71, "e1")).apply  # late message of the old incarnation
        assert t.stale_epoch == 1

    def test_snapshot_items(self) -> None:
        t = LvTracker()
        t.on_lv(lv("md.tick.a", 7))
        assert t.expected("md", "md.tick.a") == 8
        assert not t.accept_snapshot_item(lv("md.tick.a", 6))
        assert not t.accept_snapshot_item(lv("md.tick.a", 7))  # the value already applied
        assert t.accept_snapshot_item(lv("md.tick.a", 8))
        assert t.accept_snapshot_item(lv("md.tick.z", 3))  # unknown topic
        assert t.accept_snapshot_item(lv("md.tick.a", 1, "e9"))  # newer epoch resets


# ---------------------------------------------------------------------------
# CloneSync over the in-process bus
# ---------------------------------------------------------------------------


class TestCloneSync:
    def _setup(self) -> tuple[InprocBus, list[tuple[str, int]], list[Callable[[], None]]]:
        return InprocBus("api-gateway"), [], []

    def test_late_joiner_buffer_snapshot_apply(self) -> None:
        bus = InprocBus("api-gateway")
        md = bus.endpoint("md-gateway")
        for _ in range(3):
            md.publish("md.tick.a", Ping())
        md.publish("md.tick.b", Ping())
        md.publish("md.status", Ping())
        applied: list[tuple[str, int]] = []
        held: list[tuple[SnapshotRequest, Callable[[Reply], None]]] = []

        def request(pub: str, req: SnapshotRequest, cb: Callable[[Reply], None]) -> None:
            held.append((req, cb))  # answer later: messages arrive meanwhile

        clone = CloneSync("md-gateway", ["md.tick."], lambda e: applied.append((e.topic, e.seq)), request)
        bus.subscribe("md-gateway", ["md.tick."], clone.on_message)
        clone.start()
        md.publish("md.tick.a", Ping())  # seq 4, buffered
        assert applied == [] and clone.state == "SYNCING"
        req, cb = held.pop()
        snap = bus.request("md-gateway", "ctl.snapshot", req)  # taken now: contains a@4, b@1
        md.publish("md.tick.b", Ping())  # seq 2, buffered
        cb(snap)
        assert applied == [("md.tick.a", 4), ("md.tick.b", 1), ("md.tick.b", 2)]
        assert clone.synced
        md.publish("md.tick.a", Ping())
        assert applied[-1] == ("md.tick.a", 5)

    def test_paging_retry_and_resync_on_restart(self) -> None:
        bus = InprocBus("api-gateway")
        md = bus.endpoint("md-gateway")
        for i in range(5):
            md.publish(f"md.tick.i{i}", Ping())
        applied: list[tuple[str, int, str]] = []
        fail = {"n": 1}

        def request(pub: str, req: SnapshotRequest, cb: Callable[[Reply], None]) -> None:
            if fail["n"]:
                fail["n"] -= 1
                cb(Reply(ok=False, code=codes.UNAVAILABLE))
                return
            req.limit = 2
            cb(bus.request(pub, "ctl.snapshot", req))

        clock = SimClock(0)
        clone = CloneSync("md-gateway", ["md.tick."], lambda e: applied.append((e.topic, e.seq, e.epoch)), request, clock=clock)
        bus.subscribe("md-gateway", ["md.tick."], clone.on_message)
        clone.start()
        assert not clone.synced and clone.next_timer_ns() == 1_000_000_000
        clock.advance(1_000_000_000)
        clone.poll_timers()
        assert clone.synced and len(applied) == 5  # three pages of 2
        md.restart()  # new epoch, seqs restart at 1
        md.publish("md.tick.i0", Ping())
        assert applied[-1] == ("md.tick.i0", 1, md.epoch)
        assert clone.syncs == 2  # re-snapshot after the epoch change


# ---------------------------------------------------------------------------
# InprocBus
# ---------------------------------------------------------------------------


class TestInprocBus:
    def test_fifo_delivery_with_nested_publish(self) -> None:
        bus = InprocBus("risk-engine-0")
        md = bus.endpoint("md-gateway")
        order: list[str] = []

        def on_tick(e: Envelope) -> None:
            order.append(f"tick{e.seq}")
            if e.seq == 1:
                bus.publish("risk.event", Ping(), fact=True)  # queued behind remaining deliveries

        bus.subscribe("md-gateway", ["md.tick."], on_tick)
        bus.endpoint("api-gateway").subscribe("md-gateway", ["md.tick."], lambda e: order.append(f"api{e.seq}"))
        bus.endpoint("api-gateway").subscribe("risk-engine-0", ["risk.event"], lambda e: order.append(f"evt{e.seq}"))
        md.publish("md.tick.rb2701", Ping())
        assert order == ["tick1", "api1", "evt1"]

    def test_exact_topics_seq_and_stream(self) -> None:
        bus = InprocBus("clearing-worker")
        tg = bus.endpoint("trade-gateway")
        got: list[Envelope] = []
        bus.subscribe("trade-gateway", ["td.exec.ACC1"], got.append)
        tg.publish("td.exec.ACC10", Ping(), fact=True)
        assert tg.publish("td.exec.ACC1", Ping(), fact=True) == 1
        assert tg.publish("td.exec.ACC1", Ping(), fact=True, td="20260928") == 2
        assert [e.seq for e in got] == [1, 2] and got[0].stream == tg.stream and got[1].td == "20260928"
        assert isinstance(open_payload(got[0]), Ping)

    def test_request_serve_and_errors(self) -> None:
        bus = InprocBus("strategy-sim", whitelist=Whitelist(["risk-engine-0", "trade-gateway"]))
        calls: list[str] = []

        def submit(cmd: str, payload: object, ctx: object) -> Reply:
            calls.append(cmd)
            return ok_reply(Pong(instance="risk-engine-0", state="RUNNING", ts_ns=1))

        bus.serve("risk-engine-0", {"order.submit": submit})
        r = bus.request("risk-engine-0", "order.submit", Ping())
        assert r.ok and isinstance(r.open(), Pong) and calls == ["order.submit"]
        assert bus.request("nobody", "order.submit", Ping()).code == codes.UNAVAILABLE
        assert bus.request("risk-engine-0", "order.nope", Ping()).code == codes.BAD_REQUEST
        assert bus.request("risk-engine-0", "risk.reset", Ping()).code == codes.FORBIDDEN
        pong = bus.request("risk-engine-0", "ctl.ping", Ping()).open()
        assert isinstance(pong, Pong) and pong.instance == "risk-engine-0"

        bus.serve("risk-engine-0", {"risk.query.accounts": lambda c, p, x: DEFERRED})
        with pytest.raises(RuntimeError, match="deferred"):
            bus.request("risk-engine-0", "risk.query.accounts", Ping())

    def test_builtin_snapshot_and_replay(self) -> None:
        bus = InprocBus("api-gateway")
        re0 = bus.endpoint("risk-engine-0")
        for _ in range(3):
            re0.publish("risk.event", Ping(), fact=True)
        re0.publish("risk.snapshot.ACC1", Ping())
        snap = bus.request("risk-engine-0", "ctl.snapshot", SnapshotRequest(prefixes=["risk."])).open_as(SnapshotBatch)
        assert [(i.topic, i.seq) for i in snap.items] == [("risk.event", 3), ("risk.snapshot.ACC1", 1)]
        rb = bus.request("risk-engine-0", "ctl.replay", ReplayRequest(topic="risk.event", from_seq=2)).open_as(ReplayBatch)
        assert len(rb.envelopes) == 2 and rb.complete and rb.next_seq == 4
        re0.purge_fact_log("risk.event", keep_from_seq=3)
        assert bus.request("risk-engine-0", "ctl.replay", ReplayRequest(topic="risk.event", from_seq=1)).code == codes.NOT_FOUND

    def test_deterministic(self) -> None:
        def run() -> list[tuple[str, int, int]]:
            bus = InprocBus("x", clock=SimClock(100))
            out: list[tuple[str, int, int]] = []
            bus.subscribe("md-gateway", ["md."], lambda e: out.append((e.topic, e.seq, e.ts_ns)))
            md = bus.endpoint("md-gateway")
            for i in range(3):
                md.publish(f"md.tick.{i % 2}", Ping())
            return out

        assert run() == run()


# ---------------------------------------------------------------------------
# Whitelist and timeouts
# ---------------------------------------------------------------------------


class TestWhitelistAndPolicy:
    def test_table(self) -> None:
        w = Whitelist(["risk-engine-0", "clearing-worker", "api-gateway", "md-gateway"])
        assert w.allowed("route.order", "risk-engine-1") and not w.allowed("route.order", "api-gateway")
        assert w.allowed("eod.freeze", "clearing-worker") and not w.allowed("eod.freeze", "risk-engine-0")
        assert w.allowed("order.submit", "strategy-sim") and w.allowed("order.submit", "api-gateway")
        assert not w.allowed("order.submit", "md-gateway")
        assert w.allowed("config.decision", "md-gateway") and not w.allowed("config.decision", "cli")
        assert w.allowed("ctl.shutdown", "cli") and w.allowed("ctl.shutdown", "supervisor")
        assert not w.allowed("ctl.shutdown", "api-gateway")
        assert w.allowed("ctl.config.prepare", "api-gateway") and not w.allowed("ctl.config.commit", "supervisor")
        assert w.allowed("svc.kill", "cli") and not w.allowed("svc.clock", "cli")
        assert w.allowed("ctl.ping", "anyone") and w.allowed("risk.query.accounts", "clearing-worker")

    def test_timeouts_and_backoff(self) -> None:
        assert default_timeout_ms("order.submit") == 500
        assert default_timeout_ms("route.order") == 1000
        assert default_timeout_ms("ctl.replay") == 5000 and default_timeout_ms("ctl.config.prepare") == 5000
        assert default_timeout_ms("eod.freeze") == 30000 and default_timeout_ms("settle.run") == 30000
        assert default_timeout_ms("risk.query.accounts") == 2000
        rng = random.Random(1)
        d0, d1, d2 = (retry_delay_ms(i, rng=rng) for i in range(3))
        assert 100 <= d0 < 150 and 200 <= d1 < 250 and 400 <= d2 < 450
        assert retry_delay_ms(0, retry_after_ms=900) == 900


# ---------------------------------------------------------------------------
# Heartbeat content and liveness
# ---------------------------------------------------------------------------


class TestHeartbeat:
    def test_builder_content(self) -> None:
        state = {"s": ServiceState.STARTING}
        b = HeartbeatBuilder(
            instance="risk-engine-1",
            service="risk-engine",
            epoch="ep",
            started_ns=5,
            app_version="0.1.0",
            clock=SimClock(0),
            state=lambda: state["s"],
            config=lambda: (3, "abc"),
            clock_version=lambda: 7,
            loop_lag_ms=lambda: 12,
            stream=lambda: "S1",
            fact_heads=lambda: {"risk.event": 9},
            extras=lambda: HeartbeatExtras(
                trading_day="20260928",
                queues={"cmd": 2},
                rates={"ticks": 30.5},
                latency_us={"pretrade": LatencySummary(p50=40, p99=90, max=120, n=10)},
                deps={"md-gateway": "UP"},
                detail={"shard": 1},
            ),
        )
        hb = b.build()
        assert (hb.instance, hb.service, hb.state, hb.epoch, hb.proto) == ("risk-engine-1", "risk-engine", "STARTING", "ep", 1)
        assert (hb.config_version, hb.config_hash, hb.clock_version, hb.loop_lag_ms) == (3, "abc", 7, 12)
        assert hb.stream == "S1" and hb.fact_heads == {"risk.event": 9} and hb.trading_day == "20260928"
        assert hb.queues == {"cmd": 2} and hb.latency_us["pretrade"].p99 == 90 and hb.pid > 0
        assert hb.versions["Heartbeat"] == [1, 1] and "Reply" in payload_versions()
        state["s"] = ServiceState.RUNNING
        assert b.build().state == ServiceState.RUNNING

    def test_peer_liveness(self) -> None:
        clock = SimClock(0)
        changes: list[tuple[str, str]] = []
        p = PeerLiveness(heartbeat_ms=100, missed=3, clock=clock, on_change=lambda peer, s: changes.append((peer, s)))
        p.watch(["md-gateway", "trade-gateway"])
        assert p.state("md-gateway") == UNKNOWN and p.deps() == {"md-gateway": DOWN, "trade-gateway": DOWN}

        def beat(peer: str) -> None:
            p.on_heartbeat(Heartbeat(instance=peer, service=peer, state=ServiceState.RUNNING, pid=1, epoch="e", app_version="t", started_ns=0))

        beat("md-gateway")
        assert p.is_up("md-gateway") and changes == [("md-gateway", UP)]
        clock.advance(250_000_000)
        assert p.check() == []
        beat("md-gateway")
        clock.advance(301_000_000)
        assert ("md-gateway", DOWN) in p.check()
        assert ("trade-gateway", DOWN) in changes  # never seen within the timeout
        beat("md-gateway")
        assert changes[-1] == ("md-gateway", UP) and p.last_heartbeat("md-gateway") is not None

    def test_heartbeat_thread(self) -> None:
        import threading

        n = {"c": 0}
        ev = threading.Event()

        def emit() -> None:
            n["c"] += 1
            if n["c"] >= 3:
                ev.set()

        t = HeartbeatThread(emit, 20)
        t.start()
        assert ev.wait(2)
        t.stop()
        assert n["c"] >= 3
