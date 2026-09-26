"""Control API (02 §9.5): faults on both feeds, shocks, scenarios, pause/clock/resume, broker faults."""

from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime

import httpx
import pytest
from websockets.exceptions import ConnectionClosed, InvalidStatus

from futrisk.common.clock import BJ
from futrisk.common.config import FullConfig
from futrisk.common.paths import HomeLayout
from futrisk.sim.exchange import SimExchange
from futrisk.sim.exchange.scenario import load_scenario

from .helpers import BrokerClient, FeedAClient, make_cfg, offset_for, wait_for

pytestmark = pytest.mark.timeout(30)


def _client() -> httpx.AsyncClient:
    return httpx.AsyncClient(trust_env=False, timeout=5)


async def post(ex: SimExchange, path: str, body: dict[str, object] | None = None) -> httpx.Response:
    async with _client() as c:
        return await c.post(f"{ex.control_url}{path}", json=body or {})


async def get(ex: SimExchange, path: str) -> httpx.Response:
    async with _client() as c:
        return await c.get(f"{ex.control_url}{path}")


async def add_fault(ex: SimExchange, body: dict[str, object]) -> dict[str, object]:
    r = await post(ex, "/faults", body)
    assert r.status_code == 200, r.text
    return r.json()


class TestState:
    async def test_state(self, exchange: SimExchange) -> None:
        doc = (await get(exchange, "/state")).json()
        assert doc["epoch"] == exchange.epoch and doc["trading_day"] == "20260928"
        assert doc["faults"] == [] and doc["scenario"] is None and doc["paused"] is False
        assert set(doc["prices"]) == set(exchange.engine.instrument_ids)
        assert doc["clock"]["mode"] == "DEMO" and doc["broker"]["trading_day"] == "20260928"

    async def test_control_api_is_demo_only(self, default_config: FullConfig, sim_home: HomeLayout) -> None:
        cfg = make_cfg(default_config, mode="prod")
        ex = SimExchange(cfg, sim_home)
        assert ex.control is None and ex.control_url is None
        with pytest.raises(ValueError):
            SimExchange(cfg, sim_home, clock_offset_ns=5)


class TestFeedFaults:
    async def test_feed_gap_drops_ticks_that_replay_recovers(self, exchange: SimExchange) -> None:
        async with FeedAClient(exchange.feed_a_url) as c:
            await c.subscribe()
            await c.recv()
            last_before = (await c.ticks(2))[-1]["seq"]
            fault = await add_fault(exchange, {"type": "FEED_GAP", "target": "feed_a", "params": {"drop_count": 3}})
            head_at_fault = exchange.feed_a.head_seq
            seqs: list[int] = []
            while not seqs or seqs[-1] < head_at_fault + 30:
                m = await c.recv()
                if m["op"] == "tick":
                    seqs.append(m["seq"])
            missing = sorted(set(range(last_before + 1, seqs[-1] + 1)) - set(seqs))
            assert len(missing) == 3 and missing == list(range(missing[0], missing[0] + 3))
            f = exchange.faults.get(str(fault["fault_id"]))
            assert f is not None and f.state == "DONE" and f.detail["dropped_seqs"] == missing
            await c.send({"op": "replay", "req": 11, "from_seq": missing[0], "to_seq": missing[-1]})
            msgs = await c.recv_until(lambda m: m["op"] == "replay_end")
            got = [m["seq"] for m in msgs if m["op"] == "tick" and m["replay"]]
            assert got == missing

    async def test_feed_duplicate(self, exchange: SimExchange) -> None:
        async with FeedAClient(exchange.feed_a_url) as c:
            await c.subscribe()
            await c.recv()
            await add_fault(exchange, {"type": "FEED_DUPLICATE", "params": {"ratio": 1.0}})
            ticks = await c.ticks(40)
            seqs = [m["seq"] for m in ticks]
            assert len(set(seqs)) < len(seqs)  # every tick after the fault arrives twice

    async def test_feed_reorder_and_latency(self, exchange: SimExchange) -> None:
        async with FeedAClient(exchange.feed_a_url) as c:
            await c.subscribe()
            await c.recv()
            f = await add_fault(exchange, {"type": "FEED_REORDER", "params": {"window": 30}})
            head = exchange.feed_a.head_seq
            seqs = [m["seq"] for m in await c.ticks(120)]
            after = [s for s in seqs if s > head]
            assert after != sorted(after)  # shuffled windows
            async with _client() as http:
                await http.delete(f"{exchange.control_url}/faults/{f['fault_id']}")
            # nothing is lost: once the fault ends every seq shows up
            await wait_for(lambda: exchange.feed_a.head_seq > head + 150)
            seqs += [m["seq"] for m in await c.ticks(80)]
            window = set(range(head + 1, head + 100))
            assert window <= set(seqs)
            await add_fault(exchange, {"type": "FEED_LATENCY", "params": {"ms": 400}})
            # every later message is held 400ms: the stream shows one silence of that length
            arrivals = [time.monotonic()]
            while arrivals[-1] - arrivals[0] < 1.2:
                await c.recv(timeout=3)
                arrivals.append(time.monotonic())
            gaps = [b - a for a, b in zip(arrivals, arrivals[1:], strict=False)]
            assert max(gaps) >= 0.3

    async def test_feed_a_disconnect_refuses_reconnects(self, exchange: SimExchange) -> None:
        async with FeedAClient(exchange.feed_a_url) as c:
            await c.subscribe()
            await add_fault(exchange, {"type": "FEED_DISCONNECT", "target": "feed_a",
                                       "params": {"refuse_reconnect_s": 0.6}})
            with pytest.raises(ConnectionClosed):
                while True:
                    await c.recv(timeout=3)
        with pytest.raises(InvalidStatus) as exc:
            async with FeedAClient(exchange.feed_a_url):
                pass
        assert exc.value.response.status_code == 503
        await asyncio.sleep(0.8)
        async with FeedAClient(exchange.feed_a_url) as c:
            welcome = await c.subscribe()
            assert welcome["epoch"] == exchange.epoch  # same process: the epoch is unchanged

    async def test_feed_a_silent_disconnect_stops_heartbeats(self, exchange: SimExchange) -> None:
        async with FeedAClient(exchange.feed_a_url) as c:
            await c.subscribe()
            fault = await add_fault(exchange, {"type": "FEED_DISCONNECT", "target": "feed_a",
                                               "params": {"mode": "silent"}})
            await asyncio.sleep(0.1)
            while True:  # drain what was already queued
                try:
                    await c.recv(timeout=0.3)
                except TimeoutError:
                    break
            with pytest.raises(TimeoutError):
                await c.recv(timeout=0.7)  # > 3 heartbeat periods of 200ms: the client must call it dead
            async with _client() as http:
                await http.delete(f"{exchange.control_url}/faults/{fault['fault_id']}")
            with pytest.raises(ConnectionClosed):
                while True:
                    await c.recv(timeout=3)

    async def test_feed_b_faults(self, exchange: SimExchange) -> None:
        async with _client() as http:
            f = await add_fault(exchange, {"type": "FEED_DISCONNECT", "target": "feed_b", "duration_s": 0.4})
            assert (await http.get(exchange.feed_b_url)).status_code == 503
            health = await http.get(exchange.feed_b_url.replace("/quotes", "/health"))
            assert health.status_code == 200  # process health stays green
            await wait_for(lambda: exchange.faults.get(str(f["fault_id"])).state == "EXPIRED")  # type: ignore[union-attr]
            assert (await http.get(exchange.feed_b_url)).status_code == 200
            m = await add_fault(exchange, {"type": "MALFORMED", "params": {"ratio": 1.0}})
            r = await http.get(exchange.feed_b_url)
            with pytest.raises(json.JSONDecodeError):
                json.loads(r.content)
            await http.delete(f"{exchange.control_url}/faults/{m['fault_id']}")
            await add_fault(exchange, {"type": "FEED_LATENCY", "target": "feed_b", "params": {"ms": 300}})
            t0 = time.monotonic()
            r = await http.get(exchange.feed_b_url)
            assert r.status_code == 200 and time.monotonic() - t0 >= 0.29

    async def test_bad_tick_and_freeze(self, exchange: SimExchange) -> None:
        async with FeedAClient(exchange.feed_a_url) as c:
            await c.subscribe()
            await c.recv()
            await add_fault(exchange, {"type": "FEED_FREEZE", "target": "feed_a", "params": {"instrument": "cu2611"}})
            await add_fault(exchange, {"type": "BAD_TICK", "target": "feed_a",
                                       "params": {"instrument": "rb2701", "offset_pct": 0.05}})
            ticks = await c.ticks(60)
        rb = [m["d"] for m in ticks if m["d"]["InstrumentID"] == "rb2701"]
        jumps = [(a, b) for a, b in zip(rb, rb[1:], strict=False) if b["LastPrice"] > a["LastPrice"] * 1.03]
        assert jumps, "the +5% bad tick was not published"
        bad = jumps[0][1]
        assert bad["Volume"] == jumps[0][0]["Volume"]  # with_volume=false: price moved without a trade
        assert not [m for m in ticks[5:] if m["d"]["InstrumentID"] == "cu2611"]  # frozen on Feed A

    async def test_fault_validation(self, exchange: SimExchange) -> None:
        r = await post(exchange, "/faults", {"type": "NOPE"})
        assert r.status_code == 400 and r.json()["code"] == "BAD_REQUEST"
        r = await post(exchange, "/faults", {"type": "FEED_GAP", "target": "feed_a", "params": {}})
        assert r.status_code == 400 and "drop_count" in r.json()["message"]
        r = await post(exchange, "/faults", {"type": "FEED_GAP", "target": "feed_b", "params": {"drop_count": 1}})
        assert r.status_code == 400
        async with _client() as http:
            r = await http.delete(f"{exchange.control_url}/faults/unknown")
        assert r.status_code == 404 and r.json()["code"] == "NOT_FOUND"


class TestClockAndShocks:
    async def test_pause_clock_resume(self, exchange: SimExchange) -> None:
        target = offset_for(datetime(2026, 9, 28, 14, 58, tzinfo=BJ))
        r = await post(exchange, "/clock", {"offset_ns": target, "version": 1})
        assert r.status_code == 409  # only while paused
        async with FeedAClient(exchange.feed_a_url) as c:
            await c.subscribe()
            await c.recv()
            assert (await post(exchange, "/pause")).json() == {"ok": True}
            await asyncio.sleep(0.15)  # let in-flight messages arrive
            head = exchange.feed_a.head_seq
            msgs = await c.recv_until(lambda m: m["op"] == "heartbeat", timeout=2)
            msgs += await c.recv_until(lambda m: m["op"] == "heartbeat", timeout=2)
            assert all(m["op"] != "tick" or m["seq"] <= head for m in msgs)
            assert exchange.feed_a.head_seq == head  # nothing published while paused
            r = await post(exchange, "/clock", {"offset_ns": target, "version": 1})
            assert r.status_code == 200
            now = datetime.fromtimestamp(r.json()["market_now_ns"] / 1e9, tz=BJ)
            assert (now.hour, now.minute) == (14, 58)
            assert (await post(exchange, "/clock", {"offset_ns": target, "version": 0})).status_code == 409
            assert (await post(exchange, "/resume")).json() == {"ok": True}
            tick = next(m for m in await c.ticks(3) if m["seq"] > head)
            assert tick["d"]["UpdateTime"] >= "14:58:00"
        state = (await get(exchange, "/state")).json()
        assert state["clock"]["version"] == 1 and state["paused"] is False

    async def test_shock_endpoint(self, exchange: SimExchange) -> None:
        before = exchange.engine.prices()["rb2701"]
        r = await post(exchange, "/shock", {"sector": "black", "pct": -0.03, "duration_s": 0, "shape": "STEP"})
        assert r.status_code == 200
        shock_id = r.json()["shock_id"]
        await wait_for(lambda: exchange.engine.shocks.get(shock_id) is None or exchange.engine.shocks[shock_id].done)
        await asyncio.sleep(0.12)
        assert exchange.engine.prices()["rb2701"] / before - 1 == pytest.approx(-0.03, abs=0.006)
        r = await post(exchange, "/shock", {"sector": "black", "pct": -3})
        assert r.status_code == 400 and "ratio" in r.json()["message"]
        r = await post(exchange, "/shock", {"sector": "nope", "pct": -0.01})
        assert r.status_code == 400


class TestScenarios:
    async def test_list_run_and_cancel(self, exchange: SimExchange) -> None:
        names = {s["name"] for s in (await get(exchange, "/scenarios")).json()}
        assert {"breaker", "feed_outage"} <= names
        r = await post(exchange, "/scenario", {"name": "breaker", "speed": 1000})
        assert r.status_code == 200 and r.json()["run_id"]
        await wait_for(lambda: exchange.scenarios.current is not None and exchange.scenarios.current.status == "DONE")
        state = (await get(exchange, "/state")).json()["scenario"]
        defaults = load_scenario(exchange.scenarios.directories, "breaker").vars
        assert state["name"] == "breaker" and len(state["shocks"]) == 3
        assert state["vars"]["leg1_pct"] == defaults["leg1_pct"] < 0
        # override variables; a slow run is still RUNNING -> a second start conflicts
        r = await post(exchange, "/scenario", {"name": "feed_outage", "speed": 1, "vars": {"outage_s": 1}})
        assert r.status_code == 200
        await wait_for(lambda: exchange.faults.first("FEED_DISCONNECT", "feed_a") is not None
                       or bool(exchange.scenarios.current and exchange.scenarios.current.faults))
        assert (await post(exchange, "/scenario", {"name": "breaker"})).status_code == 409
        async with _client() as http:
            canceled = (await http.delete(f"{exchange.control_url}/scenario")).json()
        assert canceled["status"] == "CANCELED"
        assert (await post(exchange, "/scenario", {"name": "missing"})).status_code == 404
        assert (await post(exchange, "/scenario", {"name": "../etc"})).status_code == 400


class TestBrokerFaults:
    async def test_force_close_creates_an_external_trade(self, exchange: SimExchange) -> None:
        st = exchange.engine.states["rb2701"]
        up = float(st.meta.price(st.upper))
        with BrokerClient(exchange.broker_endpoint) as b:
            await b.login()
            ref = b.insert("rb2701", "SHFE", "0", "0", up, 2)
            await b.order_msg(ref, "ALL_TRADED")
            r = await post(exchange, "/faults", {"type": "BROKER_FORCE_CLOSE",
                                                 "params": {"account_id": "ACC3", "instrument_id": "rb2701", "qty": 1}})
            assert r.status_code == 200, r.text
            result = r.json()["fault"]["detail"]["result"]
            trade = await b.wait(lambda m: m["msg"] == "RtnTrade" and m["offset_flag"] == "2")
            assert trade["client_order_id"] is None and trade["direction"] == "1" and trade["volume"] == 1
            assert trade["trade_id"] == result["trade_id"] and trade["order_ref"].startswith("FC")
            done = await b.wait(lambda m: m["msg"] == "RtnOrder" and m["order_sys_id"] == trade["order_sys_id"]
                                and m["order_status"] == "ALL_TRADED")
            assert done["client_order_id"] is None and done["comb_offset_flag"] == "2"
            items = await b.positions("ACC3")
            assert [(p["today_position"], p["yd_position"]) for p in items if p["instrument_id"] == "rb2701"] == [(1, 0)]
            r = await post(exchange, "/faults", {"type": "BROKER_FORCE_CLOSE",
                                                 "params": {"account_id": "ACC9", "instrument_id": "rb2701"}})
            assert r.status_code == 409 and r.json()["code"] == "CONFLICT"

    async def test_sim_restart(self, exchange: SimExchange) -> None:
        epoch = exchange.epoch
        with BrokerClient(exchange.broker_endpoint) as b:
            login = await b.login()
            async with FeedAClient(exchange.feed_a_url) as c:
                await c.subscribe()
                await add_fault(exchange, {"type": "SIM_RESTART"})
                with pytest.raises(ConnectionClosed):
                    while True:
                        await c.recv(timeout=3)
            assert exchange.epoch != epoch
            async with FeedAClient(exchange.feed_a_url) as c:
                welcome = await c.subscribe(resume={"epoch": epoch, "from_seq": 5})
                assert welcome["epoch"] == exchange.epoch
                assert (await c.recv())["reason"] == "EPOCH_CHANGED"
            st = exchange.engine.states["rb2701"]
            ref = b.insert("rb2701", "SHFE", "0", "0", float(st.meta.price(st.upper)), 1)
            err = await b.wait(lambda m: m["msg"] == "ErrRtnOrderInsert" and m["order_ref"] == ref)
            assert err["error_id"] == "BROKER_NOT_LOGGED_IN"  # sessions do not survive a restart
            relog = await b.login()
            assert relog["session_id"] > login["session_id"]

    async def test_broker_disconnect(self, exchange: SimExchange) -> None:
        with BrokerClient(exchange.broker_endpoint) as b:
            await b.login()
            await add_fault(exchange, {"type": "BROKER_DISCONNECT", "params": {"seconds": 0.5}})
            assert not exchange.broker.state()["bound"]
            await wait_for(lambda: exchange.broker.state()["bound"], timeout=5)
            rsp = await b.login()  # the DEALER reconnects by itself; a new login is required
            assert rsp["error"] is None
