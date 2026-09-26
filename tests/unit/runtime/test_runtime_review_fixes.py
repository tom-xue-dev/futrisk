"""Regression tests for the code-review fixes in the runtimes (probes p1, p3-p6 of the review)."""

from __future__ import annotations

import asyncio
import sqlite3
import subprocess
import sys
import textwrap
import time
from pathlib import Path
from typing import Any

import pytest
from rttest_support import SyncHarness, cw_schema, eventually, td_schema, wait_sync
from sqlalchemy import Connection, func, insert, select

from futrisk.bus import AsyncRpcClient, BlockingRpcClient
from futrisk.bus.fact import CursorUpdate
from futrisk.bus.rpc_common import RequestContext
from futrisk.common.clock import MarketClock
from futrisk.common.config import FullConfig
from futrisk.db import FactCursors
from futrisk.messages import codes, encode
from futrisk.messages.common import Ping, Pong, SnapshotBatch, SnapshotRequest
from futrisk.messages.enums import FactStartPolicy, ServiceState
from futrisk.messages.envelope import Envelope, Reply, ok_reply
from futrisk.messages.system import Heartbeat
from futrisk.runtime import AsyncServiceRuntime, SyncServiceRuntime
from futrisk.runtime.async_runtime import WriteChainBroken

TG = "trade-gateway"
CW = "clearing-worker"


def clean_flag(path: Path) -> int:
    con = sqlite3.connect(path)
    try:
        return int(con.execute("SELECT clean_shutdown FROM db_meta WHERE id=1").fetchone()[0])
    finally:
        con.close()


def fact_env(seq: int) -> Envelope:
    return Envelope(topic="td.exec.ACC1", type="Ping", src=TG, epoch="ep", stream="S1", seq=seq, ts_ns=seq, payload=encode(Ping()))


def fact_hb(head: int) -> Heartbeat:
    return Heartbeat(
        instance=TG, service=TG, state=ServiceState.RUNNING, pid=1, epoch="ep", app_version="t",
        stream="S1", fact_heads={"td.exec.ACC1": head}, started_ns=0,
    )


# ---------------------------------------------------------------------------
# Item 1 (critical): a failed submit_write must never be skipped (probe p1)
# ---------------------------------------------------------------------------


class TestWriteChain:
    async def test_failed_apply_poisons_later_writes_and_closes_unclean(self, tmp_path: Path, bus_config: FullConfig) -> None:
        rt = AsyncServiceRuntime(CW, config=bus_config, home=tmp_path, market_clock=MarketClock(), setup_logs=False)
        await rt.start()
        db = await rt.open_database("clearing", cw_schema(), synchronous="FULL")
        copies = db.metadata.tables["copies"]
        cursors = FactCursors(db)

        def apply(e: Envelope, cur: CursorUpdate) -> None:
            def tx(conn: Connection) -> None:
                if e.seq == 2:
                    raise sqlite3.OperationalError("disk I/O error (transient)")
                conn.execute(insert(copies).values(k=f"{e.topic}:{e.seq}", v=e.seq))
                cur.save(conn, cursors, updated_ns=e.ts_ns)

            rt.submit_write(db, tx)

        tracker = rt._ensure_fact()  # drive the tracker directly, as the SUB task would
        tracker.subscribe(TG, "td.exec.", policy=FactStartPolicy.FROM_BEGINNING, apply=apply)
        tracker.on_heartbeat(fact_hb(0))
        for s in (1, 2, 3, 4):
            try:
                tracker.on_fact(fact_env(s))
            except WriteChainBroken:
                break  # the chain was already poisoned when this apply ran
        assert await asyncio.wait_for(rt.run_until_stopped(), 10) == 1
        assert rt.stop_reason.startswith("fatal") and rt.write_chain_broken
        assert isinstance(rt.fatal_error, sqlite3.OperationalError)
        path = tmp_path / "db" / "clearing.db"
        con = sqlite3.connect(path)
        try:
            assert [r[0] for r in con.execute("SELECT v FROM copies ORDER BY v")] == [1]
            assert con.execute("SELECT last_seq FROM fact_cursors").fetchall() == [(1,)]
        finally:
            con.close()
        assert clean_flag(path) == 0  # never claims a clean shutdown after a fatal error
        with pytest.raises(WriteChainBroken):
            rt.submit_write(db, lambda c: None)

    def test_sync_fatal_closes_unclean(self, tmp_path: Path, bus_config: FullConfig) -> None:
        rt = SyncServiceRuntime(TG, config=bus_config, home=tmp_path, market_clock=MarketClock(), setup_logs=False)
        with pytest.raises(RuntimeError, match="loop crashed"), rt:
            rt.open_database("td", td_schema(), synchronous="FULL")
            raise RuntimeError("business loop crashed")
        assert rt.exit_code == 1 and clean_flag(tmp_path / "db" / "td.db") == 0

    async def test_async_context_exit_with_error_closes_unclean(self, tmp_path: Path, bus_config: FullConfig) -> None:
        with pytest.raises(ValueError):
            async with AsyncServiceRuntime(CW, config=bus_config, home=tmp_path, market_clock=MarketClock(), setup_logs=False) as rt:
                await rt.open_database("clearing", cw_schema())
                raise ValueError("service bug")
        assert clean_flag(tmp_path / "db" / "clearing.db") == 0


# ---------------------------------------------------------------------------
# Item 2: ctl.snapshot serves FACT topics right after a publisher restart (probe p6)
# ---------------------------------------------------------------------------


def snapshot(cfg: FullConfig, prefix: str) -> list[tuple[str, int]]:
    c = BlockingRpcClient(lambda t: cfg.services.instance(t).router or "", identity="api-gateway")
    try:
        r = c.request(TG, "ctl.snapshot", SnapshotRequest(prefixes=[prefix]))
        return [(i.topic, i.seq) for i in r.open_as(SnapshotBatch).items]
    finally:
        c.close()


async def _publisher_run(run: int, tmp_path: Path, cfg: FullConfig) -> None:
    tg = SyncServiceRuntime(TG, config=cfg, home=tmp_path / "tg", market_clock=MarketClock(), setup_logs=False)
    h = SyncHarness(tg)
    h.start()
    try:
        db = await h.acall(lambda: tg.open_database("td", td_schema(), synchronous="FULL"))
        if run == 1:
            def emit() -> None:
                for i in range(3):
                    db.write(lambda c, i=i: tg.append_fact(c, "td.exec.ACC1", Pong(instance="x", state="fill", ts_ns=i)))
                if tg.publisher is not None:
                    tg.publisher.flush()

            await h.acall(emit)
        assert await asyncio.to_thread(snapshot, cfg, "td.") == [("td.exec.ACC1", 3)]
        if run == 2:  # a late-joining read model gets it through the Clone snapshot
            cw = AsyncServiceRuntime(CW, config=cfg, home=tmp_path / "cw", market_clock=MarketClock(), setup_logs=False)
            await cw.start()
            got: list[Envelope] = []
            cw.subscribe(TG, ["td.exec."], got.append, snapshot=True)
            await eventually(lambda: bool(got))
            assert got[0].seq == 3 and got[0].stream == db.stream_id and got[0].epoch != tg.epoch
            await cw.stop()
    finally:
        h.stop()


class TestSnapshotAfterRestart:
    async def test_fact_topic_in_snapshot_after_restart(self, tmp_path: Path, bus_config: FullConfig) -> None:
        await _publisher_run(1, tmp_path, bus_config)
        await _publisher_run(2, tmp_path, bus_config)  # fresh process: nothing published yet in this run


# ---------------------------------------------------------------------------
# Item 7: a stop during start-up interrupts wait_clock_ready (probe p4-a)
# ---------------------------------------------------------------------------


class TestStopDuringStartup:
    async def test_wait_clock_ready_returns_on_stop(self, tmp_path: Path, bus_config: FullConfig) -> None:
        rt = AsyncServiceRuntime("md-gateway", config=bus_config, home=tmp_path, setup_logs=False)  # supervisor is down
        await rt.start()
        asyncio.get_running_loop().call_later(0.2, rt.request_stop, "Ctrl-C during start-up")
        t0 = time.monotonic()
        assert await asyncio.wait_for(rt.wait_clock_ready(), 3.0) is False
        assert time.monotonic() - t0 < 1.5 and rt.is_stopping
        assert await rt.wait_clock_ready() is False  # already stopping: returns at once
        await rt.stop()


# ---------------------------------------------------------------------------
# Item 6: heartbeats overtaking facts cause no replays in the sync runtime (probe p3)
# ---------------------------------------------------------------------------


class TestSyncHeartbeatRace:
    async def test_no_spurious_replays(self, tmp_path: Path, bus_config: FullConfig) -> None:
        from futrisk.runtime.testing import config_with_free_ports

        cfg = config_with_free_ports(bus_config, heartbeat_ms=20)  # many heartbeats
        tg = SyncServiceRuntime(TG, config=cfg, home=tmp_path / "tg", market_clock=MarketClock(), setup_logs=False)
        re1 = SyncServiceRuntime("risk-engine-1", config=cfg, home=tmp_path / "re", market_clock=MarketClock(), setup_logs=False)
        h, hr = SyncHarness(tg), SyncHarness(re1)
        h.start()
        hr.start()
        try:
            db_tg = await h.acall(lambda: tg.open_database("td", td_schema(), synchronous="NORMAL"))
            topics = [f"td.exec.ACC{i}" for i in range(1, 5)]

            def setup() -> None:
                db = re1.open_database("risk-1", cw_schema(), synchronous="NORMAL", outbox=False)
                rc = FactCursors(db)
                copies = db.metadata.tables["copies"]

                def apply(env: Envelope, cur: CursorUpdate) -> None:
                    def tx(conn: Connection) -> None:
                        conn.execute(insert(copies).values(k=f"{env.topic}:{env.seq}", v=env.seq))
                        cur.save(conn, rc, updated_ns=env.ts_ns)

                    db.write(tx)

                re1.subscribe_fact(TG, topics, policy=FactStartPolicy.FROM_BEGINNING, apply=apply, cursors=rc.load_all())

            await hr.acall(setup)
            await asyncio.sleep(0.3)
            n = 0
            for _ in range(40):
                def burst(start: int = n) -> None:
                    def tx(conn: Connection) -> None:
                        for j in range(start, start + 40):
                            tg.append_fact(conn, topics[j % 4], Pong(instance="tg", state="fill", ts_ns=j))

                    db_tg.write(tx)

                await h.acall(burst)
                n += 40
                await asyncio.sleep(0.005)
            await eventually(lambda: re1.fact_tracker.stats.applied == n, timeout=15)
            await asyncio.sleep(0.2)  # a few more heartbeats
            stats = re1.fact_tracker.stats
            assert stats.applied == n and stats.replays_sent == 0 and stats.duplicates == 0
        finally:
            hr.stop()
            h.stop()


# ---------------------------------------------------------------------------
# Item 9: idempotency helper with concurrent duplicates
# ---------------------------------------------------------------------------


class TestIdempotent:
    async def test_concurrent_duplicates_run_once(self, tmp_path: Path, bus_config: FullConfig) -> None:
        rt = AsyncServiceRuntime(CW, config=bus_config, home=tmp_path, market_clock=MarketClock(), setup_logs=False)
        await rt.start()
        try:
            db = await rt.open_database("clearing", cw_schema())
            copies = db.metadata.tables["copies"]
            calls: list[str] = []

            def handler(conn: Connection) -> Reply:
                calls.append("x")
                time.sleep(0.05)  # widen the window for concurrent duplicates
                conn.execute(insert(copies).values(k="settle-1", v=1))
                return ok_reply(message="settled")

            replies = await asyncio.gather(
                *(rt.idempotent(db, "settle.run", "api-gateway", "cmd-1", Ping(), handler, trading_day="20260928") for _ in range(5))
            )
            assert calls == ["x"] and [r.duplicate for r in replies].count(False) == 1
            assert all(r.ok and r.message == "settled" for r in replies)
            later = await rt.idempotent(db, "settle.run", "api-gateway", "cmd-1", Ping(), handler, trading_day="20260928")
            assert later.duplicate and calls == ["x"]
            other = Pong(instance="x", state="y", ts_ns=1)
            a, b = await asyncio.gather(
                rt.idempotent(db, "settle.run", "api-gateway", "cmd-2", Ping(), handler_2 := _counting(calls), trading_day="20260928"),
                rt.idempotent(db, "settle.run", "api-gateway", "cmd-2", other, handler_2, trading_day="20260928"),
            )
            assert a.ok and b.code == codes.CONFLICT
            conflict = await rt.idempotent(db, "settle.run", "api-gateway", "cmd-1", other, handler, trading_day="20260928")
            assert conflict.code == codes.CONFLICT
            n = await db.run_read(lambda c: c.execute(select(func.count()).select_from(copies)).scalar())
            assert n == 1

            # through the ROUTER: one caller (one DEALER identity) sending concurrent retries
            rpc_calls: list[str] = []

            def rpc_handler(conn: Connection) -> Reply:
                rpc_calls.append("z")
                time.sleep(0.05)
                conn.execute(insert(copies).values(k="settle-9", v=9))
                return ok_reply(message="settled-9")

            async def settle(cmd: str, payload: Any, ctx: RequestContext) -> Reply:
                return await rt.idempotent(db, cmd, ctx.requester, "cmd-9", payload, rpc_handler, trading_day="20260928")

            rt.add_handler("settle.run", settle)
            import zmq.asyncio

            actx = zmq.asyncio.Context()
            client = AsyncRpcClient(actx, lambda t: bus_config.services.instance(t).router or "", identity="api-gateway")
            try:
                rpc_replies = await asyncio.gather(*(client.request(CW, "settle.run", Ping()) for _ in range(3)))
            finally:
                await client.close()
                actx.destroy(linger=0)
            assert all(r.ok and r.message == "settled-9" for r in rpc_replies)
            assert [r.duplicate for r in rpc_replies].count(False) == 1 and rpc_calls == ["z"]
        finally:
            await rt.stop()

    def test_sync_idempotent(self, tmp_path: Path, bus_config: FullConfig) -> None:
        rt = SyncServiceRuntime(TG, config=bus_config, home=tmp_path, market_clock=MarketClock(), setup_logs=False)
        h = SyncHarness(rt)
        h.start()
        try:
            db = h.call(lambda: rt.open_database("td", td_schema(), synchronous="FULL"))
            first = h.call(lambda: rt.idempotent(db, "td.import_trades", "api-gateway", "k", Ping(), lambda c: ok_reply(), trading_day="20260928"))
            again = h.call(lambda: rt.idempotent(db, "td.import_trades", "api-gateway", "k", Ping(), lambda c: ok_reply(), trading_day="20260928"))
            assert first.ok and not first.duplicate and again.duplicate
        finally:
            h.stop()


def _counting(calls: list[str]):  # type: ignore[no-untyped-def]
    def handler(conn: Connection) -> Reply:
        calls.append("y")
        time.sleep(0.05)
        return ok_reply()

    return handler


# ---------------------------------------------------------------------------
# Item 8: parent death stops the service
# ---------------------------------------------------------------------------


def _sleeper() -> subprocess.Popen[bytes]:
    return subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])


class TestParentWatch:
    async def test_async_runtime_stops_when_parent_dies(self, tmp_path: Path, bus_config: FullConfig) -> None:
        parent = _sleeper()
        try:
            rt = AsyncServiceRuntime(
                CW, config=bus_config, home=tmp_path, market_clock=MarketClock(), setup_logs=False,
                parent_pid=parent.pid, parent_poll_s=0.05,
            )
            await rt.start()
            await asyncio.sleep(0.2)
            assert not rt.is_stopping
            parent.kill()
            parent.wait(5)
            assert await asyncio.wait_for(rt.run_until_stopped(), 5) == 0
            assert "parent process" in rt.stop_reason
        finally:
            if parent.poll() is None:
                parent.kill()

    def test_sync_runtime_stops_when_parent_dies(self, tmp_path: Path, bus_config: FullConfig) -> None:
        parent = _sleeper()
        try:
            rt = SyncServiceRuntime(
                TG, config=bus_config, home=tmp_path, market_clock=MarketClock(), setup_logs=False,
                parent_pid=parent.pid, parent_poll_s=0.05,
            )
            h = SyncHarness(rt)
            h.start()
            time.sleep(0.2)
            assert not h.stopped.is_set()
            parent.kill()
            parent.wait(5)
            assert h.stopped.wait(5) and "parent process" in rt.stop_reason
        finally:
            if parent.poll() is None:
                parent.kill()

    async def test_parent_already_gone_at_start(self, tmp_path: Path, bus_config: FullConfig) -> None:
        dead = subprocess.Popen([sys.executable, "-c", "pass"])
        dead.wait(10)
        rt = AsyncServiceRuntime(
            CW, config=bus_config, home=tmp_path, market_clock=MarketClock(), setup_logs=False,
            parent_pid=dead.pid, parent_poll_s=0.05,
        )
        await rt.start()
        assert await asyncio.wait_for(rt.run_until_stopped(), 5) == 0


# ---------------------------------------------------------------------------
# Item 10: signal handler cannot deadlock the sync runtime (probe p5)
# ---------------------------------------------------------------------------


SIGNAL_SCRIPT = textwrap.dedent(
    """
    import signal, sys, tempfile
    from pathlib import Path
    from loguru import logger
    from futrisk.common.clock import MarketClock
    from futrisk.common.config import load_default_config
    from futrisk.runtime import SyncServiceRuntime
    from futrisk.runtime.testing import config_with_free_ports

    logger.remove()
    with tempfile.TemporaryDirectory() as d:
        cfg = config_with_free_ports(load_default_config(), heartbeat_ms=100)
        rt = SyncServiceRuntime("trade-gateway", config=cfg, home=Path(d), market_clock=MarketClock(), setup_logs=False)
        rt.start()
        rt.install_signal_handlers()
        with rt.publisher._order_lock:          # what publish() holds for a few microseconds
            signal.raise_signal(signal.SIGINT)   # the handler runs on this thread at the next bytecode
            x = 1 + 1
        assert not rt.running
        rt.run()                                 # loop exits at once and converts the signal
        rt.stop()
        print("OK", rt.stop_reason)
    """
)


class TestSignals:
    @pytest.mark.timeout(60)
    def test_signal_while_publish_lock_held(self, tmp_path: Path) -> None:
        script = tmp_path / "sig.py"
        script.write_text(SIGNAL_SCRIPT, encoding="utf-8")
        res = subprocess.run([sys.executable, str(script)], capture_output=True, text=True, timeout=45)
        assert res.returncode == 0, res.stderr[-2000:]
        assert "OK signal" in res.stdout


# ---------------------------------------------------------------------------
# Item 14: offloaded results wake the poller
# ---------------------------------------------------------------------------


class TestOffloadWake:
    def test_offload_result_wakes_poll(self, tmp_path: Path, bus_config: FullConfig) -> None:
        rt = SyncServiceRuntime(TG, config=bus_config, home=tmp_path, market_clock=MarketClock(), setup_logs=False)
        rt.start()
        try:
            got: list[tuple[object, object]] = []
            rt.offload(lambda: 42, lambda res, err: got.append((res, err)))
            t0 = time.monotonic()
            rt.poll(3_000)
            assert time.monotonic() - t0 < 1.0  # woken by the offload completion, not the timeout
            wait_sync(lambda: rt.next_timer_ms() == 0 or bool(got), timeout=2)
            rt.run_timers()
            assert got == [(42, None)]
        finally:
            rt.stop()
