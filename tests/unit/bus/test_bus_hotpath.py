"""Publisher / subscriber hot paths (bench follow-up): two-frame sends with plain-int flags through pyzmq's
Cython ``send``, cached topic bytes, the flat LV queue item, and the fast two-frame receive."""

from __future__ import annotations

import time
from typing import Any

import pytest
import zmq
from bustest_support import addr, outbox_row, wait_until

import futrisk.bus.publisher as publisher_mod
from futrisk.bus import PublisherCore, PublisherThread, Subscriber
from futrisk.bus.publisher import send_pub
from futrisk.common.clock import SimClock
from futrisk.messages import decode_envelope, encode
from futrisk.messages.common import Ping


def _pair(zctx: zmq.Context[Any]) -> tuple[zmq.Socket[Any], zmq.Socket[Any]]:
    pub = zctx.socket(zmq.PUB)
    port = pub.bind_to_random_port("tcp://127.0.0.1")
    sub = zctx.socket(zmq.SUB)
    sub.setsockopt(zmq.SUBSCRIBE, b"")
    sub.connect(f"tcp://127.0.0.1:{port}")
    return pub, sub


class TestSendPub:
    def test_two_frames(self, zctx: zmq.Context[Any]) -> None:
        pub, sub = _pair(zctx)
        try:
            got: list[list[bytes]] = []

            def receive() -> bool:
                send_pub(pub, b"md.tick.rb2701", b"payload")
                if sub.poll(20):
                    got.append(sub.recv_multipart())
                return bool(got)

            assert wait_until(receive, timeout=5)
            assert got[0] == [b"md.tick.rb2701", b"payload"]
        finally:
            pub.close(0)
            sub.close(0)


class TestPublisherHotPath:
    def test_lv_and_fact_frames_keep_topics_and_order(self, zctx: zmq.Context[Any]) -> None:
        a = addr()
        core = PublisherCore("trade-gateway", "epoch-1", SimClock(5))
        pub = PublisherThread(zctx, a, core, sndhwm=10_000, linger_ms=0)
        pub.start()
        sub = Subscriber(zctx, name="t")
        sub.connect("trade-gateway", a, ["td.", "sys."])
        try:
            assert wait_until(lambda: (pub.publish("sys.x", Ping()), sub.recv(20) is not None)[1], timeout=5)
            assert pub.flush(5)
            time.sleep(0.05)
            sub.drain()
            lv_before = core.sent_lv
            for i in range(50):
                pub.publish_raw(f"td.status.{i % 3}", "Ping", 1, encode(Ping()))
            pub.publish_fact_rows([outbox_row("td.exec.ACC1", s, stream="S1") for s in (1, 2)])
            pub.publish("td.status.0", Ping())
            assert pub.flush(5)
            got: list[Any] = []
            assert wait_until(lambda: (got.extend(sub.drain()), len(got) >= 53)[1], timeout=5)
            assert [e.topic for e in got[:50]] == [f"td.status.{i % 3}" for i in range(50)]
            assert [(e.topic, e.seq, e.stream) for e in got[50:52]] == [("td.exec.ACC1", 1, "S1"), ("td.exec.ACC1", 2, "S1")]
            assert (got[52].topic, got[52].seq) == ("td.status.0", 18)
            assert core.sent_lv - lv_before == 51 and core.sent_fact == 2
            cached = core.cache.get("td.status.0")
            assert cached is not None and cached.seq == 18 and decode_envelope(cached.envelope).seq == 18
        finally:
            sub.close()
            pub.stop()

    def test_topic_bytes_cache_is_bounded(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(publisher_mod, "_TOPIC_CACHE_MAX", 4)
        core = PublisherCore("md-gateway", "epoch-1", SimClock(5))
        for i in range(10):
            (tb, _), _seq = core.lv_frames_raw(f"md.tick.i{i}", "Ping", 1, encode(Ping()))
            assert tb == f"md.tick.i{i}".encode()
        assert len(core._topic_bytes) <= 4
        (tb, _), seq = core.lv_frames_raw("md.tick.i9", "Ping", 1, encode(Ping()))
        assert tb == b"md.tick.i9" and seq == 2


class TestFastReceive:
    def test_drain_counts_bad_frames_and_keeps_going(self, zctx: zmq.Context[Any]) -> None:
        pub = zctx.socket(zmq.PUB)
        port = pub.bind_to_random_port("tcp://127.0.0.1")
        sub = Subscriber(zctx, name="t")
        sub.connect("x", f"tcp://127.0.0.1:{port}", ["md."])
        core = PublisherCore("x", "epoch-1", SimClock(5))
        try:
            def warm() -> bool:
                (tb, data), _ = core.lv_frames_raw("md.w", "Ping", 1, encode(Ping()))
                send_pub(pub, tb, data)
                return bool(sub.drain())

            assert wait_until(warm, timeout=5)
            (tb, data), _ = core.lv_frames_raw("md.a", "Ping", 1, encode(Ping()))
            pub.send_multipart([b"md.bad", data, b"extra"])  # three frames
            pub.send(b"md.one")  # one frame
            send_pub(pub, tb, data)
            time.sleep(0.1)
            got: list[Any] = []
            assert wait_until(lambda: (got.extend(sub.drain()), bool(got))[1], timeout=5)
            assert [e.topic for e in got if e.topic != "md.w"] == ["md.a"]
            assert sub.stats.bad_frames == 2
        finally:
            pub.close(0)
            sub.close()
