"""Runtimes over tcp loopback: a SyncServiceRuntime (trade-gateway) and an AsyncServiceRuntime
(clearing-worker) exchanging FACTs, LV snapshots, requests (incl. deferred replies), heartbeats and
control commands; restart republish; clock following; config 2PC over the bus; fatal errors."""

from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path
from typing import Any

import yaml
from rttest_support import SyncHarness, cw_schema, eventually, td_schema, wait_sync
from sqlalchemy import Connection, func, insert, select

from futrisk.bus import DEFERRED, BlockingRpcClient, Subscriber
from futrisk.bus.fact import CursorUpdate
from futrisk.bus.rpc_common import RequestContext
from futrisk.common.clock import MarketClock
from futrisk.common.config import (
    FullConfig,
    ensure_home_config,
    file_hashes,
    load_config_dir,
    snapshot_working_config,
    write_active,
)
from futrisk.common.paths import HomeLayout
from futrisk.db import Database, FactCursors, Outbox
from futrisk.messages import codes, decode, decode_envelope
from futrisk.messages.common import (
    Ping,
    Pong,
    ReplayBatch,
    ReplayRequest,
    ShutdownRequest,
    SnapshotBatch,
    SnapshotRequest,
    StatusRequest,
)
from futrisk.messages.enums import ClockMode, ConfigEffect, FactStartPolicy, ServiceState
from futrisk.messages.envelope import Envelope, Reply, ok_reply
from futrisk.messages.system import ClockState, ConfigCommit, ConfigPrepare, ConfigPrepared, Heartbeat
from futrisk.runtime import AsyncServiceRuntime, SyncServiceRuntime

SYNC = "trade-gateway"
ASYNC = "clearing-worker"


async def call(cfg: FullConfig, identity: str, target: str, command: str, payload: Any, **kw: Any) -> Reply:
    """Blocking client in a worker thread (the event loop must keep serving)."""

    def run() -> Reply:
        c = BlockingRpcClient(lambda t: cfg.services.instance(t).router or "", identity=identity)
        try:
            return c.request(target, command, payload, **kw)
        finally:
            c.close()

    return await asyncio.to_thread(run)


def db_meta_clean(path: Path) -> int:
    con = sqlite3.connect(path)
    try:
        return int(con.execute("SELECT clean_shutdown FROM db_meta WHERE id=1").fetchone()[0])
    finally:
        con.close()


class TestSyncAsyncPair:
    async def test_facts_requests_heartbeats_control(self, tmp_path: Path, bus_config: FullConfig) -> None:
        # ------------------------------------------------------ sync side: trade-gateway
        tg = SyncServiceRuntime(SYNC, config=bus_config, home=tmp_path / "tg", market_clock=MarketClock(), setup_logs=False)
        digests: list[RequestContext] = []

        def on_digest(cmd: str, payload: object, ctx: RequestContext) -> object:
            digests.append(ctx)
            return DEFERRED  # answered later from the business loop

        tg.serve(
            {
                "td.query.trades": lambda c, p, x: ok_reply(Pong(instance=SYNC, state="trades", ts_ns=1)),
                "td.digest": on_digest,
            }
        )
        h = SyncHarness(tg)
        h.start()
        cw = AsyncServiceRuntime(ASYNC, config=bus_config, home=tmp_path / "cw", market_clock=MarketClock(), setup_logs=False)
        try:
            db_tg = await h.acall(lambda: tg.open_database("td", td_schema(), synchronous="FULL"))

            def emit(n: int) -> list[int]:
                def tx(conn: Connection) -> list[int]:
                    return [tg.append_fact(conn, "td.exec.ACC1", Pong(instance=SYNC, state="fill", ts_ns=i)) for i in range(n)]

                return db_tg.write(tx)

            assert await h.acall(lambda: emit(3)) == [1, 2, 3]  # nobody listens yet
            await h.acall(lambda: tg.publish("td.status", Pong(instance=SYNC, state="LOGGED_IN", ts_ns=0)))
            await h.acall(lambda: tg.set_state(ServiceState.RUNNING))

            # ------------------------------------------------ async side: clearing-worker
            await cw.start()
            db_cw = await cw.open_database("clearing", cw_schema(), synchronous="FULL")
            copies = db_cw.metadata.tables["copies"]
            cursors = FactCursors(db_cw)

            def apply(env: Envelope, cur: CursorUpdate) -> None:
                body = decode(env.payload, Pong)

                def tx(conn: Connection) -> None:
                    conn.execute(insert(copies).values(k=f"{env.topic}:{env.seq}", v=body.ts_ns))
                    cur.save(conn, cursors, updated_ns=env.ts_ns)

                cw.submit_write(db_cw, tx)

            cw.subscribe_fact(SYNC, ["td.exec."], policy=FactStartPolicy.FROM_BEGINNING, apply=apply, cursors=cursors.load_all())
            status_seen: list[Envelope] = []
            cw.subscribe(SYNC, ["td.status"], status_seen.append, snapshot=True)
            peer_changes: list[tuple[str, str]] = []
            cw.watch_peers([SYNC], on_change=lambda p, s: peer_changes.append((p, s)))
            beats: list[Heartbeat] = []
            cw.on_heartbeat(beats.append, [SYNC])

            async def n_copies() -> int:
                return int(await db_cw.run_read(lambda c: c.execute(select(func.count()).select_from(copies)).scalar()))

            # FROM_BEGINNING: history 1..3 recovered through ctl.replay
            await eventually(lambda: _eq(n_copies(), 3))
            await h.acall(lambda: emit(2))  # live delivery
            await eventually(lambda: _eq(n_copies(), 5))
            cur = cursors.load_all()[(SYNC, "td.exec.ACC1")]
            assert cur.last_seq == 5 and cur.stream == db_tg.stream_id and cur.start_policy == FactStartPolicy.FROM_BEGINNING
            vals = await db_cw.run_read(lambda c: [r[0] for r in c.execute(select(copies.c.v).order_by(copies.c.k))])
            assert vals == [0, 1, 2, 0, 1]

            # LV snapshot for the late joiner, liveness and heartbeat observers
            await eventually(lambda: bool(status_seen))
            assert status_seen[0].topic == "td.status" and decode(status_seen[0].payload, Pong).state == "LOGGED_IN"
            await eventually(lambda: cw.peers.state(SYNC) == "UP" and bool(beats))
            assert (SYNC, "UP") in peer_changes and beats[-1].instance == SYNC

            # async -> sync requests, including a deferred reply
            r = await cw.request(SYNC, "td.query.trades", Ping())
            assert r.ok and r.open_as(Pong).state == "trades"
            fut = asyncio.ensure_future(cw.request(SYNC, "td.digest", Ping(), timeout_ms=3000, retries=0))
            await eventually(lambda: bool(digests))
            await h.acall(lambda: tg.reply(digests[0], ok_reply(message="digest")))
            assert (await fut).message == "digest"

            # sync -> async request (non-blocking on the sync side, async handler on the other)
            async def sod(cmd: str, payload: object, ctx: RequestContext) -> Reply:
                await asyncio.sleep(0.01)
                return ok_reply(message=f"sod for {ctx.requester}")

            cw.add_handler("settle.sod_snapshot", sod)
            got: list[Reply] = []
            await h.acall(lambda: tg.request(ASYNC, "settle.sod_snapshot", Ping(), on_reply=got.append))
            await eventually(lambda: bool(got))
            assert got[0].message == "sod for trade-gateway"

            # control commands against both runtimes
            st = (await call(bus_config, "api-gateway", SYNC, "ctl.status", StatusRequest())).open_as(Heartbeat)
            assert st.state == ServiceState.RUNNING and st.instance == SYNC and st.service == SYNC
            assert st.stream == db_tg.stream_id and st.fact_heads == {"td.exec.ACC1": 5}
            assert "publisher" in st.queues and "db.td" in st.queues and st.epoch == tg.epoch
            st2 = (await call(bus_config, "api-gateway", ASYNC, "ctl.status", StatusRequest())).open_as(Heartbeat)
            assert st2.state == ServiceState.STARTING and st2.deps == {SYNC: "UP"} and st2.stream == db_cw.stream_id
            snap = (await call(bus_config, "api-gateway", SYNC, "ctl.snapshot", SnapshotRequest(prefixes=["td."]))).open_as(SnapshotBatch)
            assert {(i.topic, i.seq) for i in snap.items} == {("td.exec.ACC1", 5), ("td.status", 1)}
            rb = (await call(bus_config, ASYNC, SYNC, "ctl.replay", ReplayRequest(topic="td.exec.ACC1", from_seq=4))).open_as(ReplayBatch)
            assert [decode_envelope(b).seq for b in rb.envelopes] == [4, 5]
            ping = await call(bus_config, "cli", SYNC, "ctl.ping", Ping())
            assert ping.open_as(Pong).state == "RUNNING"

            # graceful shutdown: whitelist first, then supervisor
            forbidden = await call(bus_config, "api-gateway", SYNC, "ctl.shutdown", ShutdownRequest(reason="x"))
            assert forbidden.code == codes.FORBIDDEN
            ok = await call(bus_config, "supervisor", SYNC, "ctl.shutdown", ShutdownRequest(reason="down"))
            assert ok.ok
            assert await asyncio.to_thread(h.stopped.wait, 10)
            assert h.error is None and tg.stop_reason == "down"
            assert db_meta_clean(tmp_path / "tg" / "db" / "td.db") == 1
            await eventually(lambda: cw.peers.state(SYNC) == "DOWN", timeout=3)

            ok2 = await call(bus_config, "supervisor", ASYNC, "ctl.shutdown", ShutdownRequest(reason="bye"))
            assert ok2.ok
            assert await asyncio.wait_for(cw.run_until_stopped(), 10) == 0
            assert db_meta_clean(tmp_path / "cw" / "db" / "clearing.db") == 1
        finally:
            h.stop()
            await cw.stop()


async def _eq(coro: Any, value: int) -> bool:
    return bool((await coro) == value)


class TestRestartRepublish:
    def test_unpublished_rows_are_republished_on_open(self, tmp_path: Path, bus_config: FullConfig) -> None:
        path = tmp_path / "tg" / "db" / "td.db"
        db = Database(path, td_schema(), synchronous="FULL")
        db.open()
        ob = Outbox(db, instance=SYNC)
        db.write(lambda c: [ob.append_msg(c, "td.exec.ACC2", Ping(), epoch="old-epoch", created_ns=1) for _ in range(3)])
        stream = db.stream_id
        db.close(clean=False)  # committed but never published: crash

        tg = SyncServiceRuntime(SYNC, config=bus_config, home=tmp_path / "tg", market_clock=MarketClock(), setup_logs=False)
        h = SyncHarness(tg)
        h.start()
        import zmq

        ctx: Any = zmq.Context()
        sub = Subscriber(ctx)
        sub.connect(SYNC, bus_config.services.instance(SYNC).pub or "", ["td.exec.", f"sys.heartbeat.{SYNC}"])
        try:
            assert sub.recv(3000) is not None  # heartbeat: subscription established
            db2 = h.call(lambda: tg.open_database("td", td_schema(), synchronous="FULL"))
            got: list[Envelope] = []

            def collect() -> bool:
                got.extend(e for e in sub.drain() if e.topic == "td.exec.ACC2")
                return len(got) >= 3

            wait_sync(collect)
            assert [(e.seq, e.stream, e.epoch) for e in got] == [(1, stream, "old-epoch")] * 1 + [(2, stream, "old-epoch"), (3, stream, "old-epoch")]
            assert tg.publisher_core.heads() == {"td.exec.ACC2": 3}
            wait_sync(lambda: Outbox(db2, instance=SYNC).count_unpublished() == 0)
            assert db2.info.previous_clean_shutdown is False and db2.stream_id == stream
        finally:
            sub.close()
            ctx.destroy(linger=0)
            h.stop()


class TestClockFollowing:
    async def test_async_and_sync_followers(self, tmp_path: Path, bus_config: FullConfig) -> None:
        sup = AsyncServiceRuntime("supervisor", config=bus_config, home=tmp_path / "sup", setup_logs=False)
        md = AsyncServiceRuntime("md-gateway", config=bus_config, home=tmp_path / "md", setup_logs=False)
        re0 = SyncServiceRuntime("risk-engine-0", config=bus_config, home=tmp_path / "re0", setup_logs=False)
        h = SyncHarness(re0)
        await sup.start()
        try:
            assert sup.clock_ready  # the supervisor owns the clock
            hour = 3_600 * 1_000_000_000
            sup.publish("sys.clock", ClockState(mode=ClockMode.DEMO, offset_ns=hour, version=4, market_now_ns=0))
            await md.start()
            assert not md.clock_ready or md.market_clock.version == 4
            assert await md.wait_clock_ready(5)
            assert md.market_clock.version == 4 and md.market_clock.offset_ns == hour and md.market_clock.mode == "DEMO"
            sup.publish("sys.clock", ClockState(mode=ClockMode.DEMO, offset_ns=2 * hour, version=5, market_now_ns=0))
            await eventually(lambda: md.market_clock.version == 5)
            h.start()
            await eventually(lambda: re0.clock_ready and re0.market_clock.version == 5)
            st = (await call(bus_config, "api-gateway", "md-gateway", "ctl.status", StatusRequest())).open_as(Heartbeat)
            assert st.clock_version == 5
            unsynced = AsyncServiceRuntime("api-gateway", config=bus_config, home=tmp_path / "api", setup_logs=False, follow_clock=True)
            assert unsynced.clock_follower.ready is False
        finally:
            h.stop()
            await md.stop()
            await sup.stop()


class TestConfigOverBus:
    async def test_prepare_commit_changes_heartbeat(self, tmp_path: Path, bus_config: FullConfig) -> None:
        layout = HomeLayout(tmp_path / "home")
        ensure_home_config(layout)
        # make the committed version use the test's free ports
        services_yaml = yaml.safe_dump(bus_config.services.model_dump(mode="json"), sort_keys=False)
        (layout.config_dir / "services.yaml").write_text(services_yaml, encoding="utf-8")
        n1, p1 = snapshot_working_config(layout)
        write_active(layout, n1, p1)
        commits: list[tuple[int, dict[str, ConfigEffect]]] = []
        rt = AsyncServiceRuntime("md-gateway", home=layout.home, market_clock=MarketClock(), setup_logs=False)
        rt.on_config_commit(lambda cfg, eff: commits.append((cfg.config_version, eff)))
        await rt.start()
        try:
            assert rt.config.config_version == n1
            rules = layout.config_dir / "risk_rules.yaml"
            rules.write_text(rules.read_text(encoding="utf-8").replace("threshold: -0.030", "threshold: -0.031", 1), encoding="utf-8")
            n2, p2 = snapshot_working_config(layout)
            req = ConfigPrepare(
                config_version=n2,
                config_hash=load_config_dir(p2, n2).config_hash,
                path=p2.relative_to(layout.home).as_posix(),
                files=file_hashes(p2),
                requested_by="api-gateway",
            )
            denied = await call(bus_config, "risk-engine-0", "md-gateway", "ctl.config.prepare", req)
            assert denied.code == codes.FORBIDDEN
            prepared = (await call(bus_config, "api-gateway", "md-gateway", "ctl.config.prepare", req)).open_as(ConfigPrepared)
            assert prepared.ok and prepared.effects == {"risk_rules": ConfigEffect.IMMEDIATE}
            assert (await call(bus_config, "api-gateway", "md-gateway", "ctl.config.commit", ConfigCommit(config_version=n2))).ok
            assert commits == [(n2, {"risk_rules": ConfigEffect.IMMEDIATE})]
            st = (await call(bus_config, "api-gateway", "md-gateway", "ctl.status", StatusRequest())).open_as(Heartbeat)
            assert st.config_version == n2 and st.config_hash == req.config_hash

            # the sync runtime validates on its offload pool and answers with a deferred reply
            sync_commits: list[int] = []
            tg = SyncServiceRuntime(SYNC, home=layout.home, market_clock=MarketClock(), setup_logs=False)
            tg.on_config_commit(lambda cfg, eff: sync_commits.append(cfg.config_version))
            h = SyncHarness(tg)
            h.start()
            try:
                assert tg.config.config_version == n1  # active.json was not updated by this test
                prepared2 = (await call(bus_config, "api-gateway", SYNC, "ctl.config.prepare", req)).open_as(ConfigPrepared)
                assert prepared2.ok and prepared2.instance == SYNC and prepared2.computed_hash == req.config_hash
                assert tg.config_participant.prepared is not None
                assert (await call(bus_config, "api-gateway", SYNC, "ctl.config.commit", ConfigCommit(config_version=n2))).ok
                assert sync_commits == [n2]
            finally:
                h.stop()
        finally:
            await rt.stop()


class TestFatal:
    async def test_handler_error_stops_with_exit_code_1(self, tmp_path: Path, bus_config: FullConfig) -> None:
        rt = AsyncServiceRuntime(ASYNC, config=bus_config, home=tmp_path / "cw", market_clock=MarketClock(), setup_logs=False)
        await rt.start()

        def boom(env: Envelope) -> None:
            raise RuntimeError("unexpected")

        rt.subscribe(ASYNC, [f"sys.heartbeat.{ASYNC}"], boom)  # our own heartbeat triggers it
        assert await asyncio.wait_for(rt.run_until_stopped(), 5) == 1
        assert rt.stop_reason.startswith("fatal") and rt.state == ServiceState.STOPPING
