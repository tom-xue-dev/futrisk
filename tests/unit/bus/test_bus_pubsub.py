"""PUB/SUB over tcp loopback: LV seqs, FACT rows, exact-topic filtering, frame checks, LV cache."""

from __future__ import annotations

import asyncio
import threading
from typing import Any

import msgspec
import zmq
import zmq.asyncio
from bustest_support import addr, outbox_row, wait_until, warm_up

from futrisk.bus import (
    AsyncPublisher,
    AsyncSubscriber,
    LastValueCache,
    PublisherCore,
    PublisherThread,
    Subscriber,
)
from futrisk.bus.subscriber import TopicFilter
from futrisk.common.clock import SimClock
from futrisk.messages import decode_envelope, encode_envelope, make_envelope, open_payload
from futrisk.messages.common import Ping, Pong

# ---------------------------------------------------------------------------
# Sync publisher thread
# ---------------------------------------------------------------------------


class TestPublisherThread:
    def test_lv_seq_per_topic_and_exact_filter(self, zctx: zmq.Context[Any]) -> None:
        a = addr()
        core = PublisherCore("trade-gateway", "epoch-1", SimClock(5))
        pub = PublisherThread(zctx, a, core, sndhwm=1000, linger_ms=0)
        pub.start()
        sub = Subscriber(zctx, name="t")
        sub.connect("trade-gateway", a, ["td.exec.ACC1", "sys.heartbeat."])
        try:
            warm_up(lambda: pub.publish("sys.heartbeat.trade-gateway", Ping()), sub)
            for _ in range(3):
                pub.publish("td.exec.ACC10", Ping())  # prefix-matches at the zmq level, must be dropped
                pub.publish("td.exec.ACC1", Pong(instance="x", state="RUNNING", ts_ns=1))
            assert pub.flush()
            got: list[Any] = []

            def collect() -> bool:
                got.extend(sub.drain())
                return len([e for e in got if e.topic.startswith("td.exec.")]) >= 3

            assert wait_until(collect)
            envs = [e for e in got if e.topic.startswith("td.exec.")]
            assert [e.topic for e in envs] == ["td.exec.ACC1"] * 3
            assert [e.seq for e in envs] == [1, 2, 3]
            assert all(e.src == "trade-gateway" and e.epoch == "epoch-1" and e.stream is None for e in envs)
            assert isinstance(open_payload(envs[0]), Pong)
            assert sub.stats.filtered >= 3
            assert core.lv_seq("td.exec.ACC10") == 3
            cached = core.cache.get("td.exec.ACC1")
            assert cached is not None and cached.seq == 3 and not cached.fact
        finally:
            sub.close()
            pub.stop()

    def test_fact_rows_keep_outbox_seq_and_stream(self, zctx: zmq.Context[Any]) -> None:
        a = addr()
        core = PublisherCore("trade-gateway", "epoch-2", SimClock(5))
        published: list[list[int]] = []
        pub = PublisherThread(zctx, a, core, linger_ms=1000, on_fact_published=lambda ids, ns: published.append(ids))
        pub.start()
        sub = Subscriber(zctx)
        sub.connect("trade-gateway", a, ["td.exec.", "sys.heartbeat."])
        try:
            warm_up(lambda: pub.publish("sys.heartbeat.trade-gateway", Ping()), sub)
            pub.publish_fact_rows([outbox_row("td.exec.ACC3", 41, outbox_id=7), outbox_row("td.exec.ACC3", 42, outbox_id=8)])
            got: list[Any] = []

            def collect() -> bool:
                got.extend(e for e in sub.drain() if e.topic == "td.exec.ACC3")
                return len(got) >= 2

            assert wait_until(collect)
            assert [(e.seq, e.stream, e.epoch) for e in got] == [(41, "stream-1", "pub-epoch"), (42, "stream-1", "pub-epoch")]
            assert wait_until(lambda: published == [[7, 8]])
            assert core.heads() == {"td.exec.ACC3": 42}
            cached = core.cache.get("td.exec.ACC3")
            assert cached is not None and cached.fact and cached.seq == 42
        finally:
            sub.close()
            pub.stop()

    def test_bind_error_is_raised(self, zctx: zmq.Context[Any]) -> None:
        a = addr()
        first = PublisherThread(zctx, a, PublisherCore("x", "e"))
        first.start()
        try:
            second = PublisherThread(zctx, a, PublisherCore("y", "e"))
            try:
                second.start()
                raise AssertionError("expected a bind error")
            except zmq.ZMQError:
                pass
        finally:
            first.stop()


# ---------------------------------------------------------------------------
# Frame validation (02 §4.2) and topic filter (02 §4.4)
# ---------------------------------------------------------------------------


class TestFrameValidation:
    def test_mismatch_bad_proto_and_garbage_are_dropped(self, zctx: zmq.Context[Any]) -> None:
        a = addr()
        raw = zctx.socket(zmq.PUB)
        raw.bind(a)
        sub = Subscriber(zctx)
        sub.connect("md-gateway", a, ["md."])
        good = make_envelope("md.status", Ping(), src="md-gateway", epoch="e", ts_ns=1, seq=1)
        try:
            warm_up(lambda: raw.send_multipart([b"md.status", encode_envelope(good)]), sub)
            wrong_topic = make_envelope("md.event", Ping(), src="md-gateway", epoch="e", ts_ns=1, seq=2)
            raw.send_multipart([b"md.status", encode_envelope(wrong_topic)])
            bad_proto = msgspec.structs.replace(good, proto=2)
            raw.send_multipart([b"md.status", encode_envelope(bad_proto)])
            raw.send_multipart([b"md.status", b"\x01garbage"])
            raw.send_multipart([b"md.status"])
            other_src = make_envelope("md.status", Ping(), src="intruder", epoch="e", ts_ns=1, seq=3)
            raw.send_multipart([b"md.status", encode_envelope(other_src)])
            raw.send_multipart([b"md.status", encode_envelope(good)])

            got: list[Any] = []
            assert wait_until(lambda: bool(got.extend(sub.drain())) or sub.stats.received >= 7)
            s = sub.stats
            assert s.topic_mismatch >= 1 and s.bad_proto >= 1 and s.decode_error >= 1 and s.bad_frames >= 1
            assert s.filtered >= 1
            assert all(e.src == "md-gateway" and e.topic == "md.status" for e in got)
        finally:
            sub.close()
            raw.close(0)

    def test_topic_filter_semantics(self) -> None:
        f = TopicFilter()
        assert f.add("tg", ["td.exec.ACC1", "td.status"]) == ["td.exec.ACC1", "td.status"]
        assert f.add("tg", ["td.exec.ACC1"]) == []
        assert f.add("tg2", ["td.status"]) == []  # already subscribed on the socket
        assert f.accepts("tg", "td.exec.ACC1") and not f.accepts("tg", "td.exec.ACC10")
        assert f.accepts("tg", "td.status") and not f.accepts("tg", "td.status2")
        f.add("md", ["md.tick."])
        assert f.accepts("md", "md.tick.rb2701") and not f.accepts("tg", "md.tick.rb2701")


# ---------------------------------------------------------------------------
# Last-value cache (ctl.snapshot)
# ---------------------------------------------------------------------------


class TestLastValueCache:
    def test_prefix_paging_and_fact_filter(self) -> None:
        cache = LastValueCache()
        for i in range(7):
            cache.put(f"md.tick.i{i}", i + 1, "e", f"env{i}".encode(), fact=False)
        cache.put("md.event", 9, "e", b"fact", fact=True)
        cache.put("md.status", 3, "e", b"st", fact=False)
        b1 = cache.snapshot(["md.tick."], as_of_ns=1, limit=3)
        assert [i.topic for i in b1.items] == ["md.tick.i0", "md.tick.i1", "md.tick.i2"] and b1.next_cursor == "md.tick.i2"
        b2 = cache.snapshot(["md.tick."], as_of_ns=1, limit=3, cursor=b1.next_cursor)
        b3 = cache.snapshot(["md.tick."], as_of_ns=1, limit=3, cursor=b2.next_cursor)
        assert [i.topic for i in b3.items] == ["md.tick.i6"] and b3.next_cursor is None
        exact = cache.snapshot(["md.status", "md.event"], as_of_ns=1)
        assert [i.topic for i in exact.items] == ["md.event", "md.status"]
        no_fact = cache.snapshot(["md."], as_of_ns=1, include_fact=False, limit=100)
        assert "md.event" not in [i.topic for i in no_fact.items] and len(no_fact.items) == 8
        assert cache.snapshot(["md.statu"], as_of_ns=1).items == []  # exact topic, not a raw prefix


# ---------------------------------------------------------------------------
# asyncio publisher / subscriber
# ---------------------------------------------------------------------------


class TestAsyncPubSub:
    async def test_async_roundtrip_and_fact_from_other_thread(self) -> None:
        actx = zmq.asyncio.Context()
        sctx: zmq.Context[Any] = zmq.Context.shadow(actx.underlying)
        a = addr()
        core = PublisherCore("clearing-worker", "e3", SimClock(1))
        marked: list[list[int]] = []
        pub = AsyncPublisher(sctx, a, core, on_fact_published=lambda ids, ns: marked.append(ids))
        pub.start()
        sub = AsyncSubscriber(actx)
        sub.connect("clearing-worker", a, ["sys.eod", "sys.heartbeat.clearing-worker"])
        try:
            async def first() -> Any:
                while True:
                    pub.publish("sys.heartbeat.clearing-worker", Ping())
                    try:
                        return await asyncio.wait_for(sub.recv(), 0.05)
                    except TimeoutError:
                        continue

            env = await asyncio.wait_for(first(), 3)
            assert env.topic == "sys.heartbeat.clearing-worker"
            rows = [outbox_row("sys.eod", i, outbox_id=100 + i) for i in range(1, 6)]
            t = threading.Thread(target=lambda: [pub.publish_fact_rows([r]) for r in rows])
            t.start()
            t.join()
            seqs: list[int] = []
            while len(seqs) < 5:
                e = await asyncio.wait_for(sub.recv(), 3)
                if e.topic == "sys.eod":
                    seqs.append(e.seq)
                    assert decode_envelope(encode_envelope(e)).stream == "stream-1"
            assert seqs == [1, 2, 3, 4, 5]
            assert marked == [[101], [102], [103], [104], [105]]
        finally:
            sub.close()
            pub.close()
            actx.destroy(linger=0)
