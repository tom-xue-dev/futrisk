"""Runtime follow-ups of the api-gateway / supervisor / packaging reviews:

* a publisher restarted after the wall clock stepped back (its epoch and its recreated database's
  stream sort lower than before) is still followed by snapshot and FACT subscriptions;
* ``ctl.ping`` answers ``home_id`` / ``pid`` and heartbeats carry ``detail["home_id"]``."""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
from pathlib import Path

import psutil
import pytest
from rttest_support import SyncHarness, cw_schema, eventually, td_schema
from sqlalchemy import Connection

from futrisk.bus import BlockingRpcClient
from futrisk.bus.fact import CursorUpdate
from futrisk.common.clock import MarketClock
from futrisk.common.config import FullConfig
from futrisk.common.paths import HomeLayout
from futrisk.db import FactCursors
from futrisk.messages import decode
from futrisk.messages.common import Ping, Pong
from futrisk.messages.envelope import Envelope, Reply
from futrisk.messages.system import Heartbeat
from futrisk.runtime import AsyncServiceRuntime, SyncServiceRuntime
from futrisk.runtime.parent import ParentWatch
from futrisk.runtime.testing import ids_clock_stepped_back

SYNC = "trade-gateway"
ASYNC = "clearing-worker"
EXEC = "td.exec.ACC1"


def _tg(cfg: FullConfig, home: Path) -> SyncServiceRuntime:
    return SyncServiceRuntime(SYNC, config=cfg, home=home, market_clock=MarketClock(), setup_logs=False)


class TestPublisherRestartWithClockSteppedBack:
    async def test_snapshot_and_fact_subscriptions_follow_the_lower_incarnation(
        self, tmp_path: Path, bus_config: FullConfig
    ) -> None:
        tg1 = _tg(bus_config, tmp_path / "tg")
        h1 = SyncHarness(tg1)
        h1.start()
        h2: SyncHarness | None = None
        try:
            db1 = await h1.acall(lambda: tg1.open_database("td", td_schema(), synchronous="FULL"))
            e1, s1 = tg1.epoch, db1.stream_id
            async with AsyncServiceRuntime(
                ASYNC, config=bus_config, home=tmp_path / "cw", market_clock=MarketClock(), setup_logs=False
            ) as cw:
                db = await cw.open_database("cw", cw_schema())
                cursors = FactCursors(db)
                status: list[tuple[int, str]] = []
                facts: list[tuple[str, int]] = []

                def on_status(env: Envelope) -> None:
                    status.append((env.seq, env.epoch))

                def apply(env: Envelope, cur: CursorUpdate) -> None:
                    facts.append((cur.stream, env.seq))

                    def tx(conn: Connection) -> None:
                        cur.save(conn, cursors, updated_ns=1)

                    cw.submit_write(db, tx)

                cw.subscribe(SYNC, ["td.status"], on_status, snapshot=True)
                cw.subscribe_fact(SYNC, [EXEC], policy="FROM_BEGINNING", apply=apply, cursors=cursors.load_all())

                async def publish_status(h: SyncHarness, rt: SyncServiceRuntime, epoch: str) -> bool:
                    await h.acall(lambda: rt.publish("td.status", Ping()))
                    return any(e == epoch for _, e in status)

                await h1.acall(lambda: db1.write(lambda c: [tg1.append_fact(c, EXEC, Ping()) for _ in range(2)]))
                await eventually(lambda: publish_status(h1, tg1, e1), timeout=10, step=0.05)
                await eventually(lambda: facts == [(s1, 1), (s1, 2)], timeout=10)

                # restart: database recreated and process started right after a 1 h clock step back
                h1.stop()
                for p in (tmp_path / "tg" / "db").glob("td.db*"):
                    p.unlink()
                with ids_clock_stepped_back(3600):
                    tg2 = _tg(bus_config, tmp_path / "tg")
                    h2 = SyncHarness(tg2)
                    h2.start()
                    db2 = await h2.acall(lambda: tg2.open_database("td", td_schema(), synchronous="FULL"))
                e2, s2 = tg2.epoch, db2.stream_id
                assert e2 < e1 and s2 < s1  # the new incarnation sorts lower on both ids

                await h2.acall(lambda: db2.write(lambda c: [tg2.append_fact(c, EXEC, Ping()) for _ in range(3)]))
                harness = h2
                await eventually(lambda: publish_status(harness, tg2, e2), timeout=10, step=0.05)
                await eventually(lambda: facts[2:] == [(s2, 1), (s2, 2), (s2, 3)], timeout=10)
                assert cw.fact_tracker.stream_of(SYNC, EXEC) == s2
                await db.run_write(lambda c: None)  # writer barrier: the submit_write jobs committed
                saved = await db.run_read(lambda c: cursors.get(c, SYNC, EXEC))
                assert saved is not None and (saved.stream, saved.last_seq) == (s2, 3)
        finally:
            h1.stop()
            if h2 is not None:
                h2.stop()


def _ping(cfg: FullConfig, target: str) -> Pong:
    c = BlockingRpcClient(lambda t: cfg.services.instance(t).router or "", identity="cli")
    try:
        reply: Reply = c.request(target, "ctl.ping", Ping(), timeout_ms=3_000)
    finally:
        c.close()
    assert reply.ok, reply
    pong = reply.open_as(Pong)
    assert pong is not None
    return pong


class TestPingHomeId:
    async def test_sync_and_async_runtimes_report_home_id_and_pid(self, tmp_path: Path, bus_config: FullConfig) -> None:
        tg = _tg(bus_config, tmp_path / "tg")
        h = SyncHarness(tg)
        h.start()
        try:
            async with AsyncServiceRuntime(
                ASYNC, config=bus_config, home=tmp_path / "cw", market_clock=MarketClock(), setup_logs=False
            ) as cw:
                sync_pong = await asyncio.to_thread(_ping, bus_config, SYNC)
                async_pong = await asyncio.to_thread(_ping, bus_config, ASYNC)
                assert sync_pong.home_id == HomeLayout(tmp_path / "tg").home_id() == tg.home_id
                assert async_pong.home_id == HomeLayout(tmp_path / "cw").home_id() == cw.home_id
                assert sync_pong.home_id != async_pong.home_id
                assert sync_pong.pid == async_pong.pid == os.getpid()
                hb: Heartbeat = cw.build_heartbeat()
                assert hb.detail["home_id"] == cw.home_id and hb.pid == os.getpid()
                assert tg.build_heartbeat().detail["home_id"] == tg.home_id
        finally:
            h.stop()

    async def test_injected_config_without_home_has_no_home_id(
        self, tmp_path: Path, bus_config: FullConfig, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("FUTRISK_HOME", str(tmp_path / "default-home"))
        async with AsyncServiceRuntime(ASYNC, config=bus_config, market_clock=MarketClock(), setup_logs=False) as cw:
            pong = await asyncio.to_thread(_ping, bus_config, ASYNC)
            assert pong.home_id is None and cw.home_id is None and pong.pid == os.getpid()
            assert "home_id" not in cw.build_heartbeat().detail
        assert not (tmp_path / "default-home" / "home_id").exists()


class TestParentWatchClockSteps:
    """Docker Desktop smoke run: a stepped wall clock moved the parent's ``create_time()`` on Linux and the
    child looped on "parent gone" while its parent was alive (psutil's identity check is used now)."""

    def _child(self) -> subprocess.Popen[bytes]:
        return subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])

    def test_create_time_drift_is_not_a_dead_parent(self, monkeypatch: pytest.MonkeyPatch) -> None:
        parent = self._child()
        try:
            watch = ParentWatch(parent.pid)
            assert watch.alive()
            real = psutil.Process.create_time

            def drifted(self: psutil.Process, *args: object, **kw: object) -> float:
                return float(real(self, *args, **kw)) + 3.7  # the clock was stepped after the child started

            monkeypatch.setattr(psutil.Process, "create_time", drifted)
            assert watch.alive() and watch.alive()
            assert ParentWatch(os.getpid()).alive()
        finally:
            parent.kill()
            parent.wait(10)
        assert not watch.alive()

    def test_recycled_pid_is_not_the_parent(self) -> None:
        parent = self._child()
        try:
            watch = ParentWatch(parent.pid)
            assert watch.alive()
            proc = watch._proc
            assert proc is not None
            proc._ident = (proc.pid, "an earlier process with the same pid")  # psutil's captured identity
            assert not watch.alive()
        finally:
            parent.kill()
            parent.wait(10)

    def test_gone_at_start(self) -> None:
        dead = self._child()
        dead.kill()
        dead.wait(10)
        assert not ParentWatch(dead.pid).alive()


def test_pong_round_trip() -> None:
    from futrisk.messages import encode

    p = Pong(instance="x", state="RUNNING", ts_ns=1, home_id="h", pid=7)
    assert decode(encode(p), Pong) == p
