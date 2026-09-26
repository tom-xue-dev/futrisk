"""Feed A WebSocket protocol end to end (02 §9.1): snapshot, live ticks, resume, replay, epochs."""

from __future__ import annotations

import asyncio
import json

import pytest
from websockets.exceptions import ConnectionClosed, InvalidStatus

from futrisk.common.config import FullConfig
from futrisk.common.paths import HomeLayout
from futrisk.sim.exchange import SimExchange
from futrisk.sim.exchange import feed_a as feed_a_module

from .helpers import MARKET_OPEN, FeedAClient, make_cfg, offset_for, tick_ns, wait_for

pytestmark = pytest.mark.timeout(30)

FEED_A_FIELDS = {
    "InstrumentID", "ExchangeID", "TradingDay", "ActionDay", "UpdateTime", "UpdateMillisec", "LastPrice",
    "Volume", "Turnover", "OpenInterest", "BidPrice1", "BidVolume1", "AskPrice1", "AskVolume1", "OpenPrice",
    "HighestPrice", "LowestPrice", "UpperLimitPrice", "LowerLimitPrice", "PreSettlementPrice",
    "PreClosePrice", "PreOpenInterest", "AveragePrice",
}


class TestSubscribe:
    async def test_welcome_snapshot_live_ticks_and_heartbeats(self, exchange: SimExchange) -> None:
        async with FeedAClient(exchange.feed_a_url) as c:
            welcome = await c.subscribe()
            assert welcome["epoch"] == exchange.epoch
            assert welcome["trading_day"] == "20260928"
            assert welcome["hb_ms"] == 200 and welcome["req"] == 1
            assert welcome["ring_low"] == 1 and welcome["head_seq"] >= 1
            snap = await c.recv()
            assert snap["op"] == "snapshot"
            assert snap["as_of_seq"] == welcome["head_seq"]
            assert len(snap["ticks"]) == len(exchange.engine.metas)
            for d in snap["ticks"]:
                assert set(d) == FEED_A_FIELDS
            msgs = await c.recv_until(lambda m: m["op"] == "heartbeat", timeout=3)
            msgs += await c.recv_until(lambda m: m["op"] == "tick" and m["seq"] > snap["as_of_seq"] + 30)
            ticks = [m for m in msgs if m["op"] == "tick"]
            now = exchange.clock.now_ns()
            assert all(tick_ns(m["d"]) <= now for m in ticks)  # never stamped in the future
            seqs = [m["seq"] for m in ticks]
            assert seqs[0] == snap["as_of_seq"] + 1
            assert seqs == list(range(seqs[0], seqs[0] + len(seqs)))  # one global, gap-free seq
            assert all(m["replay"] is False for m in ticks)
            d = ticks[-1]["d"]
            assert set(d) == FEED_A_FIELDS and d["TradingDay"] == "20260928"
            hb = next(m for m in msgs if m["op"] == "heartbeat")
            assert hb["head_seq"] >= snap["as_of_seq"] and isinstance(hb["ts"], int)

    async def test_resume_within_ring_replays_the_gap(self, exchange: SimExchange) -> None:
        async with FeedAClient(exchange.feed_a_url) as c:
            await c.subscribe()
            await c.recv()  # snapshot
            last = (await c.ticks(5))[-1]["seq"]
        await wait_for(lambda: exchange.feed_a.head_seq > last + 25)
        async with FeedAClient(exchange.feed_a_url) as c:
            welcome = await c.subscribe(resume={"epoch": exchange.epoch, "from_seq": last + 1}, req=2)
            head = welcome["head_seq"]
            begin = await c.recv()
            assert begin == {"op": "replay_begin", "req": 2, "from_seq": last + 1, "to_seq": head}
            msgs = await c.recv_until(lambda m: m["op"] == "replay_end")
            replayed = [m for m in msgs if m["op"] == "tick"]
            assert [m["seq"] for m in replayed] == list(range(last + 1, head + 1))
            assert all(m["replay"] for m in replayed)
            assert msgs[-1] == {"op": "replay_end", "req": 2, "to_seq": head}
            first_live = (await c.ticks(1))[0]
            assert first_live["seq"] == head + 1 and first_live["replay"] is False

    async def test_resume_with_nothing_missing_gets_an_empty_replay(self, exchange: SimExchange) -> None:
        await exchange.pause()  # freeze head_seq
        head = exchange.feed_a.head_seq
        async with FeedAClient(exchange.feed_a_url) as c:
            await c.subscribe(resume={"epoch": exchange.epoch, "from_seq": head + 1})
            assert await c.recv() == {"op": "replay_begin", "req": 1, "from_seq": head + 1, "to_seq": head}
            assert await c.recv() == {"op": "replay_end", "req": 1, "to_seq": head}

    async def test_epoch_changed_is_rejected_with_a_snapshot(self, exchange: SimExchange) -> None:
        async with FeedAClient(exchange.feed_a_url) as c:
            await c.subscribe(resume={"epoch": "01890000-0000-7000-8000-000000000000", "from_seq": 5})
            rej = await c.recv()
            assert rej["op"] == "resume_rejected" and rej["reason"] == "EPOCH_CHANGED"
            assert {"ring_low", "head_seq", "req"} <= set(rej)
            assert (await c.recv())["op"] == "snapshot"

    async def test_in_connection_replay(self, exchange: SimExchange) -> None:
        async with FeedAClient(exchange.feed_a_url) as c:
            await c.subscribe()
            await c.recv()
            await c.ticks(3)
            await c.send({"op": "replay", "req": 7, "from_seq": 2, "to_seq": 6})
            msgs = await c.recv_until(lambda m: m["op"] == "replay_end")
            begin = next(m for m in msgs if m["op"] == "replay_begin")
            assert begin == {"op": "replay_begin", "req": 7, "from_seq": 2, "to_seq": 6}
            after_begin = msgs[msgs.index(begin) + 1:]
            assert [m["seq"] for m in after_begin if m["op"] == "tick"] == [2, 3, 4, 5, 6]
            assert all(m["replay"] for m in after_begin if m["op"] == "tick")
            live = await c.ticks(3)  # live data buffered during the replay continues afterwards
            assert all(not m["replay"] for m in live)

    async def test_protocol_errors(self, exchange: SimExchange) -> None:
        async with FeedAClient(exchange.feed_a_url) as c:
            await c.send({"op": "replay", "req": 3, "from_seq": 1, "to_seq": 2})
            err = await c.recv()
            assert err["op"] == "error" and err["code"] == "NOT_SUBSCRIBED" and err["req"] == 3
            await c.send({"op": "bogus", "req": 4})
            assert (await c.recv())["code"] == "BAD_REQUEST"
            await c.subscribe()
            await c.send({"op": "subscribe", "req": 5, "instruments": "*", "resume": None})
            msgs = await c.recv_until(lambda m: m["op"] == "error")
            assert msgs[-1]["code"] == "ALREADY_SUBSCRIBED"
        with pytest.raises(InvalidStatus) as exc:
            async with FeedAClient(exchange.feed_a_url.replace("/feed", "/other")):
                pass
        assert exc.value.response.status_code == 404


class TestRingAndEpochs:
    async def test_seq_evicted(self, default_config: FullConfig, sim_home: HomeLayout) -> None:
        cfg = make_cfg(default_config, ring_size=5)
        async with SimExchange(cfg, sim_home, offset_for(MARKET_OPEN)) as ex:
            await wait_for(lambda: ex.feed_a.head_seq > 20)
            async with FeedAClient(ex.feed_a_url) as c:
                await c.subscribe(resume={"epoch": ex.epoch, "from_seq": 1})
                rej = await c.recv()
                assert rej["op"] == "resume_rejected" and rej["reason"] == "SEQ_EVICTED"
                assert rej["ring_low"] == rej["head_seq"] - 4
                snap = await c.recv()
                assert snap["op"] == "snapshot"
                # the part still in the ring can be replayed on the same connection
                low = ex.feed_a.ring_low
                await c.send({"op": "replay", "req": 9, "from_seq": 1, "to_seq": low + 1})
                msgs = await c.recv_until(lambda m: m["op"] == "replay_end")
                begin = next(m for m in msgs if m["op"] == "replay_begin")
                assert begin["from_seq"] >= low  # clamped to the ring
                await c.send({"op": "replay", "req": 10, "from_seq": 1, "to_seq": 2})
                msgs = await c.recv_until(lambda m: m["op"] == "error")
                assert msgs[-1]["code"] == "SEQ_EVICTED" and msgs[-1]["req"] == 10

    async def test_restart_gets_a_new_epoch_and_keeps_volume(
        self, sim_cfg: FullConfig, sim_home: HomeLayout
    ) -> None:
        async with SimExchange(sim_cfg, sim_home, offset_for(MARKET_OPEN)) as ex1:
            await wait_for(lambda: ex1.engine.rounds >= 5)
            epoch1 = ex1.epoch
        _, body = ex1.feed_b.build_body("RB2701")
        published = json.loads(body)["data"][0]["vol"]  # the last volume clients could have seen
        vol1 = ex1.engine.states["rb2701"].volume
        assert published > 0
        assert vol1 == published  # stop() lets the running round finish: persisted == published
        async with SimExchange(sim_cfg, sim_home, offset_for(MARKET_OPEN)) as ex2:
            assert ex2.epoch != epoch1
            restored = ex2.engine.states["rb2701"].volume  # restored before the first round
            assert restored >= published and restored == vol1
            await wait_for(lambda: ex2.engine.rounds >= 2)
            async with FeedAClient(ex2.feed_a_url) as c:
                welcome = await c.subscribe(resume={"epoch": epoch1, "from_seq": 10})
                assert welcome["epoch"] == ex2.epoch
                rej = await c.recv()
                assert rej["reason"] == "EPOCH_CHANGED"
                snap = await c.recv()
                rb = next(d for d in snap["ticks"] if d["InstrumentID"] == "rb2701")
                assert rb["Volume"] >= vol1

    async def test_instrument_filter(self, exchange: SimExchange) -> None:
        async with FeedAClient(exchange.feed_a_url) as c:
            await c.subscribe(instruments=["IF2612"])
            snap = await c.recv()
            assert [d["InstrumentID"] for d in snap["ticks"]] == ["IF2612"]
            ticks = await c.ticks(3)
            assert {m["d"]["InstrumentID"] for m in ticks} == {"IF2612"}


class TestReplayDetails:
    async def test_bad_resume_is_rejected_with_a_snapshot(self, exchange: SimExchange) -> None:
        await exchange.pause()
        head, low = exchange.feed_a.head_seq, exchange.feed_a.ring_low
        async with FeedAClient(exchange.feed_a_url) as c:
            await c.subscribe(resume={"epoch": exchange.epoch, "from_seq": head + 50}, req=4)
            rej = await c.recv()
            assert rej == {"op": "resume_rejected", "req": 4, "reason": "BAD_RESUME", "ring_low": low,
                           "head_seq": head}
            snap = await c.recv()
            assert snap["op"] == "snapshot" and snap["as_of_seq"] == head

    async def test_replay_after_head_is_an_error_and_partial_ranges_are_clamped(
        self, exchange: SimExchange
    ) -> None:
        await exchange.pause()
        head = exchange.feed_a.head_seq
        async with FeedAClient(exchange.feed_a_url) as c:
            await c.subscribe()
            await c.recv()
            await c.send({"op": "replay", "req": 8, "from_seq": head + 5, "to_seq": head + 10})
            msgs = await c.recv_until(lambda m: m["op"] in ("error", "replay_begin"))
            assert msgs[-1]["op"] == "error" and msgs[-1]["code"] == "BAD_REQUEST"
            assert msgs[-1]["req"] == 8 and msgs[-1]["head_seq"] == head
            await c.send({"op": "replay", "req": 9, "from_seq": head - 2, "to_seq": head + 10})
            msgs = await c.recv_until(lambda m: m["op"] == "replay_end")
            begin = next(m for m in msgs if m["op"] == "replay_begin")
            assert (begin["from_seq"], begin["to_seq"]) == (head - 2, head)
            assert [m["seq"] for m in msgs if m["op"] == "tick" and m["replay"]] == [head - 2, head - 1, head]

    async def test_long_resume_replay_interleaves_heartbeats(
        self, default_config: FullConfig, sim_home: HomeLayout
    ) -> None:
        async with SimExchange(make_cfg(default_config, hb_ms=50), sim_home, offset_for(MARKET_OPEN)) as ex:
            await wait_for(lambda: ex.engine.rounds >= 1)
            await ex.pause()
            first = ex.feed_a.head_seq + 1
            states = list(ex.engine.states.values())
            for _ in range(1500):  # ~16k ticks straight into the ring
                ex.feed_a.publish(states)
            head = ex.feed_a.head_seq
            async with FeedAClient(ex.feed_a_url) as c:
                await c.subscribe(resume={"epoch": ex.epoch, "from_seq": first})
                msgs = await c.recv_until(lambda m: m["op"] == "replay_end", timeout=25)
        begin = next(i for i, m in enumerate(msgs) if m["op"] == "replay_begin")
        inside = msgs[begin + 1:-1]
        assert [m["seq"] for m in inside if m["op"] == "tick"] == list(range(first, head + 1))
        assert all(m["replay"] for m in inside if m["op"] == "tick")
        assert any(m["op"] == "heartbeat" for m in inside)  # liveness during a long replay

    async def test_in_connection_replay_interleaves_with_live_ticks(
        self, default_config: FullConfig, sim_home: HomeLayout
    ) -> None:
        async with SimExchange(make_cfg(default_config), sim_home, offset_for(MARKET_OPEN)) as ex:
            await wait_for(lambda: ex.engine.rounds >= 1)
            await ex.pause()
            first = ex.feed_a.head_seq + 1
            states = list(ex.engine.states.values())
            for _ in range(1500):
                ex.feed_a.publish(states)
            head = ex.feed_a.head_seq
            ex.resume()
            async with FeedAClient(ex.feed_a_url) as c:
                await c.subscribe()
                await c.recv()
                await c.send({"op": "replay", "req": 5, "from_seq": first, "to_seq": head})
                msgs = await c.recv_until(lambda m: m["op"] == "replay_end", timeout=25)
        begin = next(i for i, m in enumerate(msgs) if m["op"] == "replay_begin")
        inside = msgs[begin + 1:-1]
        replayed = [m["seq"] for m in inside if m["op"] == "tick" and m["replay"]]
        live = [m["seq"] for m in inside if m["op"] == "tick" and not m["replay"]]
        assert replayed == list(range(first, head + 1))
        assert live, "live ticks must keep flowing during an in-connection replay"
        assert live == sorted(live) and live[0] > head  # the live seq just continues


class TestManyClients:
    async def test_many_clients_and_a_slow_consumer(self, exchange: SimExchange) -> None:
        slow = FeedAClient(exchange.feed_a_url)
        await slow.__aenter__()
        await slow.subscribe()  # ... and never reads again
        clients = [FeedAClient(exchange.feed_a_url) for _ in range(6)]
        for c in clients:
            await c.__aenter__()
            await c.subscribe()
            await c.recv()
        try:
            results = await asyncio.gather(*(c.ticks(40, timeout=10) for c in clients))
            now = exchange.clock.now_ns()
            for ticks in results:
                seqs = [m["seq"] for m in ticks]
                assert seqs == sorted(seqs)
                assert now - tick_ns(ticks[-1]["d"]) < 1_000_000_000  # fresh: the slow client blocks no one
            assert exchange.feed_a.clients == 7
        finally:
            for c in [*clients, slow]:
                await c.__aexit__()

    async def test_a_backlogged_consumer_is_closed(
        self, exchange: SimExchange, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(feed_a_module, "MAX_SESSION_QUEUE", 20)
        async with FeedAClient(exchange.feed_a_url) as c:
            await c.subscribe()
            await c.recv()
            # a huge latency keeps every new message queued: the backlog passes the limit
            await exchange.faults.add("FEED_LATENCY", "feed_a", {"ms": 60_000})
            with pytest.raises(ConnectionClosed) as exc:
                while True:
                    await c.recv(timeout=5)
            assert exc.value.rcvd is not None and exc.value.rcvd.code == 1008
