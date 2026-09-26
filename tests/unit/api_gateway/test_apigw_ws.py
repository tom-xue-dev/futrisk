"""WebSocket push protocol (02 §9.7) over a real uvicorn server: auth (query / first message / timeout / throttle),
sub/unsub/resync/ping, per-topic seq, throttled upserts with latest-value coalescing, immediate events,
history backfill on subscribe, heartbeats, frame fields (ts / mts), the system snapshot shape, stale accounts,
bounded queues (slow consumer evicted, floods coalesced, message size), key revocation / expiry, no key in
logs or audit."""

from __future__ import annotations

import asyncio
import dataclasses
import json
import time
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest
from apigw_support import MONDAY_10AM, ROLE_KEYS, Harness, make_gateway, snapshot, with_keys
from loguru import logger
from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed

from futrisk.common.config import FullConfig
from futrisk.messages.base import Msg
from futrisk.messages.enums import (
    AccountState,
    Exchange,
    OrderStatus,
    RiskEventKind,
    Role,
    ServiceState,
    Severity,
    SourceId,
)
from futrisk.messages.envelope import Reply, make_envelope, ok_reply
from futrisk.messages.md import Tick
from futrisk.messages.orders import OrderUpdate
from futrisk.messages.risk import MetricPoint, MetricSeries, OrderBatch, RiskEvent, RiskEventBatch
from futrisk.messages.system import Heartbeat
from futrisk.services.api_gateway.auth import Principal
from futrisk.services.api_gateway.service import HttpServer
from futrisk.services.api_gateway.wshub import CONTROL_LIMIT, QUEUE_LIMIT, WsClient, frame

KEY = ROLE_KEYS[Role.VIEWER]
# a generous auth timeout keeps the tests stable on a loaded machine; test_timeout_closes shortens it itself
HUB_KW: dict[str, Any] = {"auth_timeout_s": 3.0, "heartbeat_s": 0.4, "tick_s": 0.02}


async def serve(tmp_path: Path, cfg: FullConfig, keys: dict[str, str],
                **hub_kw: Any) -> tuple[Harness, HttpServer, str]:
    h = await make_gateway(tmp_path, cfg, env=keys, hub_kw=HUB_KW | hub_kw)
    h.gw.hub.start()
    server = HttpServer(h.app, "127.0.0.1", 0)
    await server.start()
    return h, server, f"ws://127.0.0.1:{server.port}/ws"


@pytest.fixture
async def env(tmp_path: Path, default_config: FullConfig) -> AsyncIterator[tuple[Harness, str]]:
    cfg, keys = with_keys(default_config)
    h, server, url = await serve(tmp_path, cfg, keys)
    try:
        yield h, url
    finally:
        await server.stop()
        await h.close()


seq_counter = {"n": 0}


def feed(h: Harness, topic: str, body: Msg, src: str = "md-gateway") -> None:
    seq_counter["n"] += 1
    h.gw.readmodel.apply(make_envelope(topic, body, src=src, epoch="e1", ts_ns=MONDAY_10AM, seq=seq_counter["n"]))


def tick(last: float, volume: int) -> Tick:
    return Tick(instrument_id="rb2701", exchange=Exchange.SHFE, trading_day="20260928", action_day="20260928",
                exch_ts_ns=MONDAY_10AM + volume, last=last, volume=volume, turnover=last * volume * 10,
                open_interest=1000, upper_limit=3276.0, lower_limit=2964.0, pre_settle=3120.0,
                source=SourceId.FEED_A, recv_ns=MONDAY_10AM + volume + 2_000_000)


def risk_event(event_id: str, message: str = "halt", ts: int = 0) -> RiskEvent:
    return RiskEvent(event_id=event_id, kind=RiskEventKind.ALERT_RAISED, trading_day="20260928",
                     ts_ns=MONDAY_10AM + ts, account_id="ACC3", severity=Severity.CRITICAL, message=message)


async def recv(ws: ClientConnection, timeout: float = 2.0, skip_heartbeats: bool = True) -> dict[str, Any]:
    """Next frame within ``timeout`` overall (heartbeat frames skipped by default)."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        remaining = deadline - loop.time()
        if remaining <= 0:
            raise TimeoutError
        msg = json.loads(await asyncio.wait_for(ws.recv(), remaining))
        if skip_heartbeats and msg["type"] == "heartbeat":
            continue
        return msg


async def closed_code(ws: ClientConnection, timeout: float = 5.0) -> int | None:
    """Read (and drop) frames until the server closes; the close code."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    with pytest.raises(ConnectionClosed):
        while True:
            await asyncio.wait_for(ws.recv(), max(0.01, deadline - loop.time()))
    return ws.close_code


async def authed(url: str, key: str = KEY) -> ClientConnection:
    ws = await connect(url)
    await ws.send(json.dumps({"op": "auth", "key": key}))
    return ws


async def eventually(pred: Any, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while not pred():
        if time.monotonic() > deadline:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(0.02)


class TestAuth:
    async def test_timeout_closes(self, env: tuple[Harness, str]) -> None:
        h, url = env
        h.gw.hub._auth_timeout_s = 0.3
        async with connect(url) as ws:
            msg = await recv(ws)
            assert msg["type"] == "error" and msg["data"]["code"] == "UNAUTHORIZED"
            with pytest.raises(ConnectionClosed):
                await asyncio.wait_for(ws.recv(), 2)
            assert ws.close_code == 4401

    async def test_bad_token_and_bad_first_message(self, env: tuple[Harness, str]) -> None:
        h, url = env
        async with connect(url + "?token=wrong") as ws:
            assert (await recv(ws))["type"] == "error"
            with pytest.raises(ConnectionClosed):
                await asyncio.wait_for(ws.recv(), 2)
        async with connect(url) as ws:
            await ws.send(json.dumps({"op": "sub", "topics": ["quotes"]}))
            assert (await recv(ws))["data"]["code"] == "UNAUTHORIZED"

    async def test_token_query_auth(self, env: tuple[Harness, str]) -> None:
        h, url = env
        async with connect(f"{url}?token={KEY}") as ws:
            await ws.send(json.dumps({"op": "sub", "topics": ["accounts"]}))
            msg = await recv(ws)
            assert msg["type"] == "snapshot" and msg["topic"] == "accounts" and msg["seq"] == 1
        await asyncio.to_thread(h.db.flush)
        rows = await h.gw.store.audit_rows()
        row = next(r for r in rows if r["method"] == "WS" and r["key_id"] == "viewer")
        assert row["client_ip"] == "127.0.0.1" and row["path"] == "/ws"  # L4; never the ?token= query

    async def test_failed_auths_are_throttled(self, env: tuple[Harness, str]) -> None:
        h, url = env
        h.gw.auth_throttle.burst = 2
        for _ in range(2):
            async with connect(url + "?token=wrong") as ws:
                assert (await recv(ws))["data"]["code"] == "UNAUTHORIZED"
        async with connect(f"{url}?token={KEY}") as ws:  # even a valid key waits while throttled (no oracle)
            msg = await recv(ws)
            assert msg["type"] == "error" and msg["data"]["code"] == "BUSY"

    async def test_key_never_in_logs_or_audit(self, env: tuple[Harness, str]) -> None:
        h, url = env
        lines: list[str] = []
        sink = logger.add(lambda m: lines.append(str(m) + " " + json.dumps(m.record["extra"], default=str)),
                          level="DEBUG")
        try:
            async with connect(f"{url}?token={KEY}") as ws:
                await ws.send(json.dumps({"op": "sub", "topics": ["accounts", "nope"]}))
                await recv(ws)
                await recv(ws)
            async with connect(url) as ws:
                await ws.send(json.dumps({"op": "auth", "key": KEY + "-wrong"}))
                await recv(ws)
            await asyncio.sleep(0.1)
        finally:
            logger.remove(sink)
        await asyncio.to_thread(h.db.flush)
        rows = await h.gw.store.audit_rows()
        assert not any(KEY in line for line in lines)
        assert not any(KEY in json.dumps(r, default=str) for r in rows)


class TestProtocol:
    async def test_quotes_throttled_upserts(self, env: tuple[Harness, str]) -> None:
        h, url = env
        feed(h, "md.tick.rb2701", tick(3100.0, 10))
        ws = await authed(url)
        try:
            await ws.send(json.dumps({"op": "sub", "topics": ["quotes"]}))
            snap = await recv(ws)
            assert snap["type"] == "snapshot" and snap["seq"] == 1 and snap["td"] == "20260928"
            assert snap["data"][0]["instrument_id"] == "rb2701" and snap["data"][0]["last"] == 3100.0
            # three ticks inside one 500 ms window -> one upsert carrying the latest value
            feed(h, "md.tick.rb2701", tick(3101.0, 11))
            feed(h, "md.tick.rb2701", tick(3102.0, 12))
            feed(h, "md.tick.rb2701", tick(3103.0, 13))
            up = await recv(ws)
            assert up["type"] == "upsert" and up["topic"] == "quotes" and up["seq"] == 2
            assert up["data"]["last"] == 3103.0 and up["data"]["volume"] == 13
            assert up["data"]["change_pct"] == pytest.approx((3103.0 - 3120.0) / 3120.0)
            with pytest.raises(TimeoutError):
                await recv(ws, timeout=0.7)  # nothing else queued
            feed(h, "md.tick.rb2701", tick(3104.0, 14))
            assert (await recv(ws))["seq"] == 3
            # a lost frame (simulated skip on the server) -> the client sees a gap and asks for a resync
            next(iter(h.gw.hub.clients)).seq["quotes"] += 5
            feed(h, "md.tick.rb2701", tick(3105.0, 15))
            gap = await recv(ws)
            assert gap["type"] == "upsert" and gap["seq"] == 9
            await ws.send(json.dumps({"op": "resync", "topics": ["quotes"]}))
            again = await recv(ws)
            assert again["type"] == "snapshot" and again["seq"] == 10 and again["data"][0]["last"] == 3105.0
        finally:
            await ws.close()

    async def test_events_immediate_and_errors(self, env: tuple[Harness, str]) -> None:
        h, url = env
        ws = await authed(url)
        try:
            await ws.send(json.dumps({"op": "sub", "topics": ["events", "nope", "positions:ACC9"]}))
            got = [await recv(ws) for _ in range(2)]
            errors = [m for m in got if m["type"] == "error"]
            snaps = [m for m in got if m["type"] == "snapshot"]
            # one error frame lists every unknown topic of the op
            assert len(errors) == 1 and errors[0]["data"]["topics"] == ["nope", "positions:ACC9"]
            assert snaps[0]["topic"] == "events" and snaps[0]["seq"] == 1
            for i in range(2):
                feed(h, "risk.event", risk_event(f"e{i}", ts=i), src="risk-engine-1")
            e1, e2 = await recv(ws, timeout=0.5), await recv(ws, timeout=0.5)
            assert (e1["type"], e1["seq"], e1["data"]["event_id"]) == ("event", 2, "e0")
            assert (e2["seq"], e2["data"]["event_id"]) == (3, "e1")
            await ws.send(json.dumps({"op": "ping", "ts": 123}))
            pong = await recv(ws)
            assert pong["type"] == "pong" and pong["data"] == {"ts": 123} and "seq" not in pong
            await ws.send(json.dumps({"op": "unsub", "topics": ["events"]}))
            await ws.send(json.dumps({"op": "ping", "ts": 1}))
            assert (await recv(ws))["type"] == "pong"
            feed(h, "risk.event", RiskEvent(event_id="e9", kind=RiskEventKind.RESET, trading_day="20260928",
                                            ts_ns=MONDAY_10AM + 9, account_id="ACC3"), src="risk-engine-1")
            with pytest.raises(TimeoutError):
                await recv(ws, timeout=0.3)
            await ws.send("not json")
            assert (await recv(ws))["data"]["code"] == "BAD_REQUEST"
        finally:
            await ws.close()

    async def test_events_snapshot_backfills_from_the_shards(self, env: tuple[Harness, str]) -> None:
        h, url = env

        def alerts(target: str, payload: Any) -> Reply:
            n = 0 if target == "risk-engine-0" else 1
            return ok_reply(RiskEventBatch(items=[risk_event(f"q{n}-{i}", ts=10 * n + i) for i in range(3)]))

        h.bus.on("risk.query.alerts", alerts)
        feed(h, "risk.event", risk_event("live", ts=100), src="risk-engine-1")  # only this came with the FACT snapshot
        ws = await authed(url)
        try:
            await ws.send(json.dumps({"op": "sub", "topics": ["events"]}))
            snap = await recv(ws)
            ids = [e["event_id"] for e in snap["data"]]
            assert ids == ["q0-0", "q0-1", "q0-2", "q1-0", "q1-1", "q1-2", "live"]  # L9, ordered by ts
            assert {c.payload.trading_day for c in h.bus.of("risk.query.alerts")} == {"20260928"}
        finally:
            await ws.close()

    async def test_heartbeat_and_frame_clocks(self, env: tuple[Harness, str]) -> None:
        h, url = env
        h.clock.set(MONDAY_10AM + 3_600_000_000_000)  # demo market time one hour ahead of the wall clock
        ws = await authed(url)
        try:
            hb = await recv(ws, timeout=2.0, skip_heartbeats=False)
            assert hb["type"] == "heartbeat" and "seq" not in hb and "topic" not in hb and "td" not in hb
            assert hb["mts"] == (MONDAY_10AM + 3_600_000_000_000) // 1_000_000
            assert abs(hb["ts"] - int(time.time() * 1000)) < 60_000  # wall clock (staleness), not market time
            await ws.send(json.dumps({"op": "sub", "topics": ["firm"]}))
            snap = await recv(ws)
            assert snap["mts"] == hb["mts"] and {"v", "type", "topic", "seq", "ts", "mts", "td", "data"} == set(snap)
        finally:
            await ws.close()

    async def test_system_snapshot_shape(self, env: tuple[Harness, str]) -> None:
        h, url = env
        h.gw.readmodel.on_heartbeat(Heartbeat(instance="md-gateway", service="md-gateway",
                                              state=ServiceState.RUNNING, pid=11, epoch="e", app_version="0",
                                              started_ns=1, clock_version=3))
        ws = await authed(url)
        try:
            await ws.send(json.dumps({"op": "sub", "topics": ["system"]}))
            snap = await recv(ws)
            items = {i["instance"]: i for i in snap["data"]}
            assert items["md-gateway"]["kind"] == "service" and items["md-gateway"]["state"] == "RUNNING"
            clock = items["sys.clock"]
            assert clock["kind"] == "clock" and clock["market_now_ns"] == MONDAY_10AM and "version" in clock
            assert {i["kind"] for i in snap["data"]} == {"service", "clock"}
            h.gw.system_event({"kind": "kill_switch", "event": "retry", "pending": ["trade-gateway"]})
            ev = await recv(ws)
            assert ev["type"] == "event" and ev["data"]["kind"] == "kill_switch"
        finally:
            await ws.close()

    async def test_accounts_positions_and_state_events(self, env: tuple[Harness, str]) -> None:
        h, url = env
        h.gw.readmodel.seed_snapshots([snapshot("ACC3", equity=9_700_000.0, daily_pnl_pct=-0.03)])
        ws = await authed(url)
        try:
            await ws.send(json.dumps({"op": "sub", "topics": ["accounts", "positions:ACC3"]}))
            got = {m["topic"]: m for m in [await recv(ws), await recv(ws)]}
            acc = got["accounts"]["data"][0]
            assert acc["account_id"] == "ACC3" and acc["equity"] == 9_700_000.0 and acc["name"]
            assert acc["stale"] is False
            assert got["positions:ACC3"]["data"] == []
            feed(h, "risk.snapshot.ACC3", snapshot("ACC3", equity=9_600_000.0, state=AccountState.HALTED,
                                                   state_version=2, as_of_ns=MONDAY_10AM + 1), src="risk-engine-1")
            frames = [await recv(ws), await recv(ws)]
            kinds = {(f["topic"], f["type"]) for f in frames}
            assert kinds == {("accounts", "upsert"), ("positions:ACC3", "snapshot")}
            up = next(f for f in frames if f["topic"] == "accounts")
            assert up["data"]["state"] == "HALTED" and up["seq"] == 2
            # the owning shard goes down: the item is re-pushed as stale (M4)
            h.set_peer("risk-engine-1", "DOWN")
            stale = await recv(ws)
            assert stale["topic"] == "accounts" and stale["data"]["stale"] is True
        finally:
            await ws.close()

    async def test_orders_and_equity_backfill(self, env: tuple[Harness, str]) -> None:
        h, url = env
        order = OrderUpdate(client_order_id="O1", account_id="ACC3", instrument_id="rb2701", exchange=Exchange.SHFE,
                            direction="BUY", offset="OPEN", order_type="LIMIT", tif="GFD", qty=1,  # type: ignore[arg-type]
                            status=OrderStatus.ACCEPTED, source="BOT", trading_day="20260928",  # type: ignore[arg-type]
                            created_ns=1, updated_ns=1)

        def orders(target: str, payload: Any) -> Reply:
            assert target == "risk-engine-1" and payload.account_id == "ACC3" and payload.trading_day == "20260928"
            return ok_reply(OrderBatch(items=[order]))

        def metrics(target: str, payload: Any) -> Reply:
            pts = [MetricPoint(ts_ns=MONDAY_10AM + i, equity=1e7 - i, nav=1.0, daily_pnl_pct_init=0.0,
                               intraday_dd=0.0, risk_degree=0.1, gross_notional=0.0, state=AccountState.NORMAL)
                   for i in range(3)]
            return ok_reply(MetricSeries(account_id="ACC3", trading_day="20260928", step_s=5, points=pts))

        h.bus.on("risk.query.orders", orders)
        h.bus.on("risk.query.metrics", metrics)
        ws = await authed(url)
        try:
            await ws.send(json.dumps({"op": "sub", "topics": ["orders:ACC3", "equity:ACC3"]}))
            got = {m["topic"]: m for m in [await recv(ws), await recv(ws)]}
            assert [o["client_order_id"] for o in got["orders:ACC3"]["data"]] == ["O1"]
            assert [p["ts_ns"] for p in got["equity:ACC3"]["data"]] == [MONDAY_10AM, MONDAY_10AM + 1, MONDAY_10AM + 2]
            feed(h, "oms.order.ACC3", OrderUpdate(**{**_fields(order), "status": OrderStatus.FILLED, "filled_qty": 1,
                                                     "updated_ns": 2}), src="risk-engine-1")
            up = await recv(ws, timeout=0.5)
            assert up["type"] == "upsert" and up["topic"] == "orders:ACC3" and up["data"]["status"] == "FILLED"
            assert up["seq"] == 2
        finally:
            await ws.close()


def _fields(o: OrderUpdate) -> dict[str, Any]:
    return {f: getattr(o, f) for f in o.__struct_fields__}


class TestLimits:
    async def test_slow_consumer_is_bounded_and_evicted(self, tmp_path: Path, default_config: FullConfig) -> None:
        cfg, keys = with_keys(default_config)
        budget = 256 * 1024
        h, server, url = await serve(tmp_path, cfg, keys, byte_budget=budget, send_timeout_s=30.0)
        try:
            ws = await authed(url)
            await ws.send(json.dumps({"op": "sub", "topics": ["events"]}))
            await recv(ws)  # the snapshot; from now on the client does not read
            await eventually(lambda: bool(h.gw.hub.subs.get("events")))
            client = next(iter(h.gw.hub.clients))
            big = "x" * 50_000
            peak = 0
            for i in range(600):  # ~30 MB offered to a client that reads nothing
                feed(h, "risk.event", risk_event(f"big{i}", message=big, ts=i), src="risk-engine-1")
                peak = max(peak, client.queued_bytes)
                if client.closed:
                    break
                if i % 20 == 0:
                    await asyncio.sleep(0.01)  # let the sender fill the socket buffers
            assert client.closed and client.close_code == 1013  # evicted, not buffered without bound
            assert peak <= budget + 60_000 and client.queued_bytes == 0
            assert client not in h.gw.hub.clients and not h.gw.hub.subs.get("events")
            assert h.gw.hub.evictions == 1
            assert await closed_code(ws, timeout=20) == 1013  # the close arrives once the client drains
        finally:
            await server.stop()
            await h.close()

    async def test_ping_flood_is_coalesced(self, env: tuple[Harness, str]) -> None:
        h, url = env
        ws = await authed(url)
        try:
            await ws.send(json.dumps({"op": "ping", "ts": -1}))
            assert (await recv(ws, timeout=5.0))["data"]["ts"] == -1  # the session is authenticated
            client = next(iter(h.gw.hub.clients))
            for i in range(3000):
                await ws.send(json.dumps({"op": "ping", "ts": i}))
            assert len([f for f in client.control if f.type == "pong"]) <= 1  # only the latest pong waits
            last = None
            while True:
                msg = await recv(ws, timeout=3.0)
                if msg["type"] == "pong":
                    last = msg["data"]["ts"]
                    if last == 2999:
                        break
            assert not client.closed and h.gw.hub.evictions == 0
        finally:
            await ws.close()

    async def test_sub_floods(self, env: tuple[Harness, str]) -> None:
        h, url = env
        ws = await authed(url)
        try:
            await ws.send(json.dumps({"op": "sub", "topics": [f"bogus{i}" for i in range(100)]}))
            err = await recv(ws)
            assert err["type"] == "error" and len(err["data"]["topics"]) == 20  # one frame, not 100
            with pytest.raises(TimeoutError):
                await recv(ws, timeout=0.3)
            await ws.send(json.dumps({"op": "sub", "topics": [f"t{i}" for i in range(101)]}))
            assert "at most 100 topics" in (await recv(ws))["data"]["message"]
            # p19: one huge message is refused as a whole (close 1009), never parsed topic by topic
            await ws.send(json.dumps({"op": "sub", "topics": ["x" * 40] * 5000}))
            assert await closed_code(ws) == 1009
        finally:
            await ws.close()

    async def test_resync_flood_keeps_one_snapshot_queued(self, env: tuple[Harness, str]) -> None:
        h, url = env
        for i in range(50):
            feed(h, "risk.event", risk_event(f"e{i}", message="m" * 2000, ts=i), src="risk-engine-1")
        hub = h.gw.hub
        # a session whose sender is blocked (the peer reads nothing): p04 queued 3000 snapshots (130 MB)
        client = WsClient(ws=None, principal=Principal("viewer", "viewer", Role.VIEWER), hub=hub)  # type: ignore[arg-type]
        hub.clients.add(client)
        await hub._on_op(client, json.dumps({"op": "sub", "topics": ["events"]}))
        one = client.queued_bytes
        assert one > 100_000 and [f.type for f in client.events] == ["snapshot"]
        for _ in range(3000):
            await hub._on_op(client, json.dumps({"op": "resync", "topics": ["events"]}))
            await hub._on_op(client, json.dumps({"op": "ping", "ts": 1}))
        assert [f.type for f in client.events] == ["snapshot"] and client.queued_bytes <= one + 1000
        assert [f.type for f in client.control] == ["pong"] and not client.closed
        hub.clients.discard(client)
        hub._drop(client)

    async def test_revoked_and_expired_keys_close_sessions(self, tmp_path: Path, default_config: FullConfig) -> None:
        cfg, keys = with_keys(default_config)
        h, server, url = await serve(tmp_path, cfg, keys, revalidate_s=0.1)
        try:
            viewer = await authed(url)
            trader = await authed(url, ROLE_KEYS[Role.TRADER])
            for ws in (viewer, trader):
                await ws.send(json.dumps({"op": "sub", "topics": ["accounts"]}))
                await recv(ws)
            # the viewer key disappears from app.yaml: its session is closed right after the key sync (M8)
            app = h.cfg.app.model_copy(update={"api_keys": [k for k in h.cfg.app.api_keys if k.name != "viewer"]})
            h.cfg_ref["cfg"] = dataclasses.replace(h.cfg, app=app)
            await h.gw.resync_keys(h.cfg.app)
            err = await recv(viewer)
            assert err["type"] == "error" and err["data"]["code"] == "UNAUTHORIZED"
            assert await closed_code(viewer) == 4401
            # the trader key expires: closed by the periodic re-check
            rec = next(r for r in await h.gw.store.load_keys() if r.key_id == "trader")
            await h.gw.store.upsert_key(dataclasses.replace(rec, expires_ns=1))
            await h.gw.keys.reload()
            assert await closed_code(trader) == 4401
            assert h.gw.hub.revoked == 2 and not h.gw.hub.clients
        finally:
            await server.stop()
            await h.close()


class TestQueues:
    """Unit level: queue rules of one session (no socket)."""

    class Hub:
        byte_budget = 8 * 1024 * 1024
        send_timeout_s = 10.0

        def __init__(self) -> None:
            self.evicted_clients: list[WsClient] = []

        def trading_day(self) -> str:
            return "20260928"

        def market_ms(self) -> int:
            return MONDAY_10AM // 1_000_000

        def evicted(self, client: WsClient) -> None:
            self.evicted_clients.append(client)

    def client(self, hub: Any = None) -> WsClient:
        c = WsClient(ws=None, principal=Principal("k", "k", Role.VIEWER), hub=hub or self.Hub())  # type: ignore[arg-type]
        c.topics |= {"events", "quotes"}
        return c

    def test_event_overflow_sends_resync_and_state_keeps_latest(self) -> None:
        c = self.client()
        for i in range(QUEUE_LIMIT):
            c.put_event(frame("event", "events", {"i": i}))
        assert len(c.events) == QUEUE_LIMIT and not c.control
        c.put_event(frame("event", "events", {"i": "overflow"}))
        assert not c.events and c.control[0].type == "resync" and c.control[0].topic == "events"
        # state topics: only the latest value per key survives, a snapshot supersedes queued upserts
        c.put_state("rb2701", frame("upsert", "quotes", {"last": 1}))
        c.put_state("rb2701", frame("upsert", "quotes", {"last": 2}))
        c.put_state("cu2611", frame("upsert", "quotes", {"last": 3}))
        assert len(c.state) == 2 and json.loads(c.state[("quotes", "rb2701")].data) == {"last": 2}
        c.put_state("*", frame("snapshot", "quotes", []))
        assert list(c.state) == [("quotes", "*")]
        text = c._render(c.control.popleft())
        assert json.loads(text) | {"ts": 0} == {"v": 1, "type": "resync", "topic": "events", "seq": 1, "ts": 0,
                                                 "mts": MONDAY_10AM // 1_000_000, "td": "20260928", "data": None}

    def test_snapshot_supersedes_queued_events_and_resync(self) -> None:
        c = self.client()
        for i in range(10):
            c.put_event(frame("event", "events", {"i": i}))
        c.put_control(frame("resync", "events", None))
        c.put_snapshot("events", frame("snapshot", "events", [{"i": "all"}]))
        assert [f.type for f in c.events] == ["snapshot"] and not c.control
        assert c.queued_bytes == c.events[0].size and "events" in c.pending_snapshots

    def test_unsub_then_resub_sends_no_stale_frames(self) -> None:
        c = self.client()
        for i in range(5):
            c.put_event(frame("event", "events", {"i": i}))
        c.put_state("rb2701", frame("upsert", "quotes", {"last": 1}))
        c.topics.discard("events")
        c.purge_topic("events")  # unsub (L3)
        c.topics.add("events")
        c.put_snapshot("events", frame("snapshot", "events", []))  # resub
        order = []
        while (f := c._next()) is not None:
            order.append((f.topic, f.type))
        assert order == [("events", "snapshot"), ("quotes", "upsert")] and c.queued_bytes == 0

    def test_control_frames_are_coalesced_and_capped(self) -> None:
        hub = self.Hub()
        c = self.client(hub)
        for _ in range(10_000):
            c.put_control(frame("pong", None, {"ts": 1}))
            c.put_control(frame("heartbeat", None, None))
        assert sorted(f.type for f in c.control) == ["heartbeat", "pong"]
        for _ in range(100):
            c.put_control(frame("error", None, {"code": "BAD_REQUEST"}))
        assert sum(1 for f in c.control if f.type == "error") == 16 and not c.closed
        for i in range(CONTROL_LIMIT):
            c.put_control(frame("resync", f"t{i}", None))
        assert c.closed and c.close_code == 1008 and hub.evicted_clients == [c]

    def test_byte_budget_evicts(self) -> None:
        hub = self.Hub()
        hub.byte_budget = 10_000
        c = self.client(hub)
        c.put_event(frame("event", "events", "x" * 6000))
        assert not c.closed
        c.put_event(frame("event", "events", "x" * 6000))
        assert c.closed and c.close_code == 1013 and c.queued_bytes == 0 and hub.evicted_clients == [c]

    async def test_send_timeout_evicts(self) -> None:
        hub = self.Hub()
        hub.send_timeout_s = 0.1

        class StuckWs:
            async def send_text(self, text: str) -> None:
                await asyncio.Event().wait()  # the peer never reads

        c = WsClient(ws=StuckWs(), principal=Principal("k", "k", Role.VIEWER), hub=hub)  # type: ignore[arg-type]
        c.topics.add("events")
        task = asyncio.ensure_future(c.sender())
        c.put_event(frame("event", "events", {"i": 1}))
        await asyncio.wait_for(task, 2)
        assert c.closed and c.close_code == 1013 and hub.evicted_clients == [c]
