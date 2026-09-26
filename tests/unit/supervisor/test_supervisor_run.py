"""Supervisor with dummy children over tcp loopback: start sequence and liveness, restart policy,
heartbeat loss / loop hang, svc.* commands with the cli identity, reverse-order shutdown, sys.service
FACTs, persistence across supervisor restarts, maintenance (01 §5.3.6, §6.8; 02 §4.8, §6.6; 03 §4.6, §8)."""

from __future__ import annotations

import asyncio
import sqlite3
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import pytest
import zmq
from sqlalchemy import Connection, insert
from suptest_support import SupHarness, bus_call, fast_config

from futrisk.common.config import FullConfig
from futrisk.db import ProcessedCommands
from futrisk.messages import codes, decode, decode_envelope, encode_envelope, make_envelope
from futrisk.messages.common import ReplayBatch, ReplayRequest
from futrisk.messages.enums import ClockTarget, EodPhase, ServiceChangeReason, ServiceState
from futrisk.messages.envelope import ok_reply
from futrisk.messages.eod import EodPhaseEvent
from futrisk.messages.system import (
    ClockJumpRequest,
    ClockState,
    PsRequest,
    ServiceEventBatch,
    ServiceInfo,
    ServiceList,
    ServiceStateChanged,
    SvcHistoryRequest,
    SvcRequest,
)
from futrisk.services.supervisor.main import supervise
from futrisk.services.supervisor.schema import COMMON, service_events

HOUR = 3_600 * 1_000_000_000
MS = 1_000_000


def req(instance: str, cid: str, actor: str = "cli:tester") -> SvcRequest:
    return SvcRequest(command_id=cid, instance=instance, actor=actor)


async def test_start_sequence_liveness_facts_and_reverse_shutdown(tmp_path: Path, default_config: FullConfig) -> None:
    order = ["sim-exchange", "md-gateway", "trade-gateway", "strategy-sim"]
    cfg = fast_config(default_config, startup_order=order)
    h = SupHarness(cfg, tmp_path / "home")
    try:
        await h.start()
        for inst in order:
            await h.wait_state(inst, "RUNNING", 20)
        # each instance was live before the next one was spawned (01 §6.8)
        seq = [(e.instance, str(e.to_state)) for e in h.events]
        assert seq == [(i, s) for i in order for s in ("STARTING", "RUNNING")]
        assert {str(e.reason) for e in h.events} == {"STARTUP"} and {e.trading_day for e in h.events} == {"20260928"}
        assert h.rt is not None and h.rt.market_clock.mode == "DEMO" and h.rt.market_clock.version == 1

        ps = await bus_call(cfg, "cli", "svc.ps", PsRequest())
        items = ps.open_as(ServiceList).items
        assert [(i.instance, str(i.state)) for i in items] == [(i, "RUNNING") for i in order]
        md = items[1]
        assert md.heartbeat_state == ServiceState.RUNNING and md.pid and md.last_heartbeat_ns and md.started_ns
        assert items[0].heartbeat_state is None and items[0].pid  # sim-exchange: /v1/health, no bus heartbeat

        # sys.service is a FACT: the outbox replays exactly the transitions, in order
        replay = await bus_call(cfg, "api-gateway", "ctl.replay", ReplayRequest(topic="sys.service", from_seq=1))
        envs = [decode_envelope(b) for b in replay.open_as(ReplayBatch).envelopes]
        assert [e.seq for e in envs] == list(range(1, len(h.events) + 1))
        assert [decode(e.payload, ServiceStateChanged) for e in envs] == h.events
        hist = await bus_call(cfg, "clearing-worker", "svc.history", SvcHistoryRequest(trading_day="20260928"))
        assert hist.open_as(ServiceEventBatch).items == h.events

        assert h.sup is not None
        await h.sup.shutdown()
        assert [e.instance for e in h.events if str(e.to_state) == "STOPPING"] == list(reversed(order))
        assert all(str(h.unit(i).state) == "STOPPED" for i in order)
        stops = [e for e in h.events if str(e.to_state) == "STOPPED"]
        assert {str(e.reason) for e in stops} == {"SHUTDOWN"}
        # bus services got ctl.shutdown (in reverse order) and exited cleanly
        asked = [inst for _, inst, ev in h.trace_lines() if ev == "shutdown-request"]
        assert asked == ["trade-gateway", "md-gateway"]
        assert h.unit("md-gateway").last_exit_code == 0 and h.unit("trade-gateway").last_exit_code == 0
    finally:
        await h.stop()


async def test_exit_and_failed_start_restart_with_backoff(tmp_path: Path, default_config: FullConfig) -> None:
    cfg = fast_config(default_config, startup_order=["md-gateway"], backoff_initial_ms=200, backoff_max_ms=1000, stable_reset_s=1)
    h = SupHarness(cfg, tmp_path / "home", {"md-gateway": "exit:1.0:3,crash:2,ok"})
    try:
        await h.start()
        await h.wait_for(lambda: h.to_states("md-gateway").count("RUNNING") == 2, 20, "second RUNNING")
        assert h.moves("md-gateway") == [
            ("STOPPED", "STARTING", "STARTUP"),
            ("STARTING", "RUNNING", "STARTUP"),
            ("RUNNING", "EXITED", "EXITED"),
            ("EXITED", "RESTARTING", "EXITED"),
            ("RESTARTING", "STARTING", "EXITED"),
            ("STARTING", "FAILED", "EXITED"),  # died before liveness: failure to start
            ("FAILED", "RESTARTING", "EXITED"),
            ("RESTARTING", "STARTING", "EXITED"),
            ("STARTING", "RUNNING", "STARTUP"),
        ]
        evs = h.events
        assert (evs[2].exit_code, evs[5].exit_code) == (3, 2)
        assert [e.restarts for e in evs if str(e.to_state) == "RESTARTING"] == [1, 2]
        # exponential backoff: 200 ms, then 400 ms (RESTARTING -> STARTING)
        d1 = (evs[4].ts_ns - evs[3].ts_ns) / MS
        d2 = (evs[7].ts_ns - evs[6].ts_ns) / MS
        assert 180 <= d1 < 450 and 380 <= d2 < 800, (d1, d2)
        # after stable_reset_s of RUNNING the failure counter is reset
        await h.wait_for(lambda: h.unit("md-gateway").backoff.failures == 0, 5, "backoff reset")
        info = (await bus_call(cfg, "cli", "svc.ps", PsRequest())).open_as(ServiceList).items[0]
        assert (info.restarts, info.last_exit_code) == (2, 2)
    finally:
        await h.stop()


async def test_heartbeat_loss_and_loop_hang_are_killed_and_restarted(tmp_path: Path, default_config: FullConfig) -> None:
    cfg = fast_config(default_config, startup_order=["md-gateway", "trade-gateway"], hang_ms=400)
    h = SupHarness(cfg, tmp_path / "home", {"md-gateway": "hang:0.8,ok", "trade-gateway": "lag:0.8,ok"})
    try:
        await h.start()
        await h.wait_for(
            lambda: h.to_states("md-gateway").count("RUNNING") == 2 and h.to_states("trade-gateway").count("RUNNING") == 2,
            25,
            "both restarted",
        )
        assert ("RUNNING", "EXITED", "HEARTBEAT_LOST") in h.moves("md-gateway")
        assert ("EXITED", "RESTARTING", "HEARTBEAT_LOST") in h.moves("md-gateway")
        assert ("RUNNING", "EXITED", "LOOP_HANG") in h.moves("trade-gateway")
        # the hung processes were killed (they would never have exited by themselves)
        exited = [e for e in h.events if str(e.to_state) == "EXITED"]
        assert all(e.exit_code not in (None, 0) for e in exited)
    finally:
        await h.stop()


async def test_liveness_timeout_does_not_block_the_next_instance(tmp_path: Path, default_config: FullConfig) -> None:
    cfg = fast_config(default_config, startup_order=["md-gateway", "trade-gateway"], liveness_s=1.0, backoff_initial_ms=300)
    h = SupHarness(cfg, tmp_path / "home", {"md-gateway": "silent,ok"})
    try:
        await h.start()
        await h.wait_state("trade-gateway", "RUNNING", 15)
        await h.wait_for(lambda: "RUNNING" in h.to_states("md-gateway"), 15, "md-gateway RUNNING")
        assert h.moves("md-gateway")[:4] == [
            ("STOPPED", "STARTING", "STARTUP"),
            ("STARTING", "FAILED", "HEARTBEAT_LOST"),
            ("FAILED", "RESTARTING", "HEARTBEAT_LOST"),
            ("RESTARTING", "STARTING", "HEARTBEAT_LOST"),
        ]
        order = [(e.instance, str(e.to_state)) for e in h.events]
        assert order.index(("trade-gateway", "STARTING")) > order.index(("md-gateway", "FAILED"))
    finally:
        await h.stop()


async def test_svc_commands_over_the_bus(tmp_path: Path, default_config: FullConfig) -> None:
    cfg = fast_config(default_config, startup_order=["md-gateway", "trade-gateway"])
    h = SupHarness(cfg, tmp_path / "home")
    try:
        await h.start()
        await h.wait_state("trade-gateway", "RUNNING", 15)

        # svc.stop answers at once (STOPPING); the instance stays down
        r = await bus_call(cfg, "cli", "svc.stop", req("md-gateway", "s1"))
        assert r.ok and str(r.open_as(ServiceInfo).state) == "STOPPING"
        await h.wait_state("md-gateway", "STOPPED")
        ev = h.events[-1]
        assert (str(ev.reason), ev.actor, ev.exit_code) == ("MANUAL", "cli:tester", 0)
        n = len(h.events)
        dup = await bus_call(cfg, "cli", "svc.stop", req("md-gateway", "s1"))  # retry: first reply, nothing new
        assert dup.ok and dup.duplicate and str(dup.open_as(ServiceInfo).state) == "STOPPING" and len(h.events) == n
        clash = await bus_call(cfg, "cli", "svc.stop", req("trade-gateway", "s1"))
        assert clash.code == codes.CONFLICT
        await asyncio.sleep(0.4)
        assert str(h.unit("md-gateway").state) == "STOPPED"

        r = await bus_call(cfg, "cli", "svc.start", req("md-gateway", "s2"))
        assert r.ok and str(r.open_as(ServiceInfo).state) == "STARTING"
        await h.wait_state("md-gateway", "RUNNING")

        r = await bus_call(cfg, "api-gateway", "svc.restart", req("trade-gateway", "s3", actor="admin"))
        assert r.ok
        await h.wait_for(lambda: h.moves("trade-gateway")[-1] == ("STARTING", "RUNNING", "STARTUP") and len(h.moves("trade-gateway")) == 6)
        assert h.moves("trade-gateway")[2:] == [
            ("RUNNING", "STOPPING", "MANUAL"),
            ("STOPPING", "RESTARTING", "MANUAL"),
            ("RESTARTING", "STARTING", "MANUAL"),
            ("STARTING", "RUNNING", "STARTUP"),
        ]
        assert h.unit("trade-gateway").restarts == 1

        # svc.kill (demo): crash simulation, then the restart policy brings it back
        r = await bus_call(cfg, "cli", "svc.kill", req("md-gateway", "s4"), timeout_ms=10_000)
        info = r.open_as(ServiceInfo)
        assert r.ok and str(info.state) == "RESTARTING" and info.last_exit_code not in (None, 0)
        await h.wait_for(lambda: h.moves("md-gateway")[-1] == ("STARTING", "RUNNING", "STARTUP") and h.unit("md-gateway").restarts == 1)
        killed = [e for e in h.events if e.instance == "md-gateway" and str(e.to_state) == "EXITED"][-1]
        assert (killed.reason, killed.actor) == (ServiceChangeReason.KILLED_BY_DEMO, "cli:tester")

        # errors and the bus whitelist
        assert (await bus_call(cfg, "cli", "svc.start", req("nope", "s5"))).code == codes.NOT_FOUND
        assert (await bus_call(cfg, "cli", "svc.kill", req("*", "s6"))).code == codes.BAD_REQUEST
        assert (await bus_call(cfg, "risk-engine-0", "svc.stop", req("md-gateway", "s7"))).code == codes.FORBIDDEN
        now = h.rt.market_clock.now_ns() if h.rt is not None else 0
        jump = ClockJumpRequest(command_id="k1", target=ClockTarget.AT, at_ns=now + HOUR, actor="ops")
        assert (await bus_call(cfg, "cli", "svc.clock", jump)).code == codes.FORBIDDEN

        # svc.clock (api-gateway only): forward, persisted, applied to the supervisor's clock, published
        r = await bus_call(cfg, "api-gateway", "svc.clock", jump)
        assert r.ok and r.open_as(ClockState).version == 2 and h.rt is not None and h.rt.market_clock.version == 2
        back = ClockJumpRequest(command_id="k2", target=ClockTarget.AT, at_ns=now, actor="ops")
        assert (await bus_call(cfg, "api-gateway", "svc.clock", back)).code == codes.CONFLICT
        snap = await bus_call(cfg, "md-gateway", "ctl.snapshot", _clock_snapshot())
        assert _snapshot_clock(snap).version == 2

        # svc.kill of a stopped instance is a conflict
        await bus_call(cfg, "cli", "svc.stop", req("md-gateway", "s8"))
        await h.wait_state("md-gateway", "STOPPED")
        assert (await bus_call(cfg, "cli", "svc.kill", req("md-gateway", "s9"))).code == codes.CONFLICT
    finally:
        await h.stop()


async def test_instance_env_reaches_its_child_and_survives_restarts(tmp_path: Path, default_config: FullConfig) -> None:
    """``futrisk demo --scenario``: strategy-sim (only) runs with FUTRISK_BOTS_MODE=scenario, also after restarts."""
    cfg = fast_config(default_config, startup_order=["md-gateway", "strategy-sim"])
    h = SupHarness(
        cfg,
        tmp_path / "home",
        instance_env={"strategy-sim": {"FUTRISK_BOTS_MODE": "scenario"}},
        env_trace=("FUTRISK_BOTS_MODE",),
    )

    def seen(inst: str) -> list[str]:
        return [ev for _, i, ev in h.trace_lines() if i == inst and ev.startswith("env ")]

    def svc(cid: str, command_env: dict[str, str], instance: str = "strategy-sim") -> SvcRequest:
        return SvcRequest(command_id=cid, instance=instance, actor="cli:tester", env=command_env)

    try:
        await h.start()
        await h.wait_state("strategy-sim", "RUNNING", 15)
        # a process-only child is live before its Python code runs: wait for its trace
        await h.wait_for(lambda: len(seen("strategy-sim")) == 1, 10, "strategy-sim env trace")
        assert seen("strategy-sim") == ["env FUTRISK_BOTS_MODE=scenario"]
        assert seen("md-gateway") == ["env FUTRISK_BOTS_MODE=-"]  # the override is per instance

        # a crash and the restart policy keep the environment
        r = await bus_call(cfg, "cli", "svc.kill", req("strategy-sim", "e1"), timeout_ms=10_000)
        assert r.ok
        await h.wait_for(lambda: len(seen("strategy-sim")) == 2, 15, "strategy-sim restarted")
        assert seen("strategy-sim") == ["env FUTRISK_BOTS_MODE=scenario"] * 2
        await h.wait_state("strategy-sim", "RUNNING")

        # svc.start with the environment it already has: nothing happens
        n = len(h.events)
        r = await bus_call(cfg, "cli", "svc.start", svc("e2", {"FUTRISK_BOTS_MODE": "scenario"}))
        assert r.ok and str(r.open_as(ServiceInfo).state) == "RUNNING"
        await asyncio.sleep(0.3)
        assert len(h.events) == n

        # another environment for a running instance: restarted (MANUAL) with it
        r = await bus_call(cfg, "cli", "svc.start", svc("e3", {"FUTRISK_BOTS_MODE": "normal"}))
        assert r.ok and str(r.open_as(ServiceInfo).state) == "STOPPING"
        await h.wait_for(lambda: len(seen("strategy-sim")) == 3, 15, "strategy-sim switched")
        assert seen("strategy-sim")[-1] == "env FUTRISK_BOTS_MODE=normal"
        await h.wait_for(lambda: h.to_states("strategy-sim").count("RUNNING") == 3, 15, "strategy-sim RUNNING")
        assert h.moves("strategy-sim")[-4:] == [
            ("RUNNING", "STOPPING", "MANUAL"),
            ("STOPPING", "RESTARTING", "MANUAL"),
            ("RESTARTING", "STARTING", "MANUAL"),
            ("STARTING", "RUNNING", "STARTUP"),
        ]
        assert h.unit("strategy-sim").env == {"FUTRISK_BOTS_MODE": "normal"}

        # over the bus: allow-listed names only (strategy-sim FUTRISK_BOTS_*), svc.start/svc.restart only, one instance
        assert (await bus_call(cfg, "cli", "svc.start", svc("e4", {"PYTHONPATH": "x"}))).code == codes.BAD_REQUEST
        assert (await bus_call(cfg, "cli", "svc.stop", svc("e5", {"FUTRISK_X": "1"}))).code == codes.BAD_REQUEST
        assert (await bus_call(cfg, "cli", "svc.start", svc("e6", {"FUTRISK_X": "1"}, instance="*"))).code == codes.BAD_REQUEST
        assert h.unit("strategy-sim").env == {"FUTRISK_BOTS_MODE": "normal"}
    finally:
        await h.stop()


def _clock_snapshot() -> object:
    from futrisk.messages.common import SnapshotRequest

    return SnapshotRequest(prefixes=["sys.clock"])


def _snapshot_clock(reply: object) -> ClockState:
    from futrisk.messages.common import SnapshotBatch

    batch = reply.open_as(SnapshotBatch)  # type: ignore[attr-defined]
    env = decode_envelope(batch.items[0].envelope)
    return decode(env.payload, ClockState)


async def test_prod_mode_refuses_demo_commands_and_maintenance_runs(tmp_path: Path, default_config: FullConfig) -> None:
    cfg = fast_config(default_config, startup_order=[], mode="prod", start_at=None)
    h = SupHarness(cfg, tmp_path / "home")
    pub_ctx: zmq.Context[zmq.Socket[bytes]] = zmq.Context()
    try:
        sup = await h.start()
        assert h.rt is not None and h.rt.market_clock.mode == "REAL" and h.rt.market_clock.offset_ns == 0
        assert (await bus_call(cfg, "cli", "svc.kill", req("md-gateway", "p1"))).code == codes.NOT_FOUND
        jump = ClockJumpRequest(command_id="p2", target=ClockTarget.NEXT_OPEN, actor="ops")
        assert (await bus_call(cfg, "api-gateway", "svc.clock", jump)).code == codes.PRECONDITION_FAILED
        assert (await bus_call(cfg, "cli", "svc.ps", PsRequest())).open_as(ServiceList).items == []

        # maintenance after sys.eod DONE: purge by retention, back up supervisor.db (03 §8)
        db, processed = sup.db, ProcessedCommands(sup.db)
        old_ns = h.rt.market_clock.now_ns() - 100 * 24 * HOUR

        def seed(c: object) -> None:
            conn = c  # sqlalchemy Connection
            conn.execute(  # type: ignore[attr-defined]
                insert(service_events).values(
                    ts_ns=old_ns, trading_day="20260601", instance="md-gateway", service="md-gateway",
                    from_state="RUNNING", to_state="EXITED", restarts=0, reason="EXITED",
                )
            )
            processed.record(conn, command_id="old", command="svc.stop", requester="cli", request_hash="0" * 64,  # type: ignore[arg-type]
                             reply=ok_reply(), trading_day="20260601", received_ns=old_ns)
            wall = time.time_ns()  # outbox retention runs on the wall clock (published_ns)
            for seq, published in ((1, wall - 8 * 24 * HOUR), (2, wall - HOUR)):
                conn.execute(  # type: ignore[attr-defined]
                    insert(COMMON.outbox).values(
                        topic="sys.test", topic_seq=seq, type="Ping", v=1, epoch="e", payload=b"\x80",
                        created_ns=published, published_ns=published,
                    )
                )

        await db.run_write(seed)
        pub = pub_ctx.socket(zmq.PUB)
        pub.bind(str(cfg.services.instance("clearing-worker").pub))
        done = EodPhaseEvent(trading_day="20260928", version=1, phase=EodPhase.DONE, ts_ns=1)
        backup = h.home / "backup" / "20260928" / "supervisor.db"
        deadline = time.monotonic() + 10
        while not backup.exists():  # re-publish until the late subscription is established
            assert time.monotonic() < deadline, "maintenance did not run"
            env = make_envelope("sys.eod", done, src="clearing-worker", epoch="e", ts_ns=1, seq=1, stream="s")
            pub.send_multipart([b"sys.eod", encode_envelope(env)])
            await asyncio.sleep(0.1)
        pub.close(0)

        def live(c: Connection) -> tuple[int, int, list[int]]:
            return (
                c.exec_driver_sql("SELECT COUNT(*) FROM service_events WHERE trading_day='20260601'").scalar_one(),
                c.exec_driver_sql("SELECT COUNT(*) FROM processed_commands WHERE command_id='old'").scalar_one(),
                [r[0] for r in c.exec_driver_sql("SELECT topic_seq FROM outbox WHERE topic='sys.test'")],
            )

        deadline = time.monotonic() + 10
        while (got := await db.run_read(live)) != (0, 0, [2]):  # purged by retention after the backup
            assert time.monotonic() < deadline, f"not purged: {got}"
            await asyncio.sleep(0.05)
        con = sqlite3.connect(backup)
        try:  # 03 §8 order: the backup was taken before the purge, so it still has the old rows
            assert con.execute("SELECT COUNT(*) FROM service_events WHERE trading_day='20260601'").fetchone()[0] == 1
            assert con.execute("SELECT COUNT(*) FROM processed_commands WHERE command_id='old'").fetchone()[0] == 1
        finally:
            con.close()
    finally:
        pub_ctx.destroy(0)
        await h.stop()


async def test_maintenance_order_and_backup_retention(
    tmp_path: Path, default_config: FullConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    """03 §8: wal_checkpoint(TRUNCATE) -> VACUUM INTO backup -> purge -> keep 20 trading days of backups."""
    from futrisk.services.supervisor import maintenance as mt

    calls: list[str] = []

    def spy(name: str, fn: Any) -> Any:
        def wrapper(*a: Any, **kw: Any) -> Any:
            calls.append(name)
            return fn(*a, **kw)

        return wrapper

    for name, attr in (("checkpoint", "wal_checkpoint"), ("backup", "backup_database"),
                       ("purge", "_purge_events"), ("prune", "prune_backups")):
        monkeypatch.setattr(mt, attr, spy(name, getattr(mt, attr)))
    cfg = fast_config(default_config, startup_order=[], mode="prod", start_at=None)
    h = SupHarness(cfg, tmp_path / "home")
    try:
        sup = await h.start()
        backups = h.home / "backup"
        days = [f"202608{d:02d}" for d in range(1, 26)]  # 25 older backups
        for day in days:
            (backups / day).mkdir(parents=True)
            (backups / day / "supervisor.db").write_bytes(b"old")
        (backups / days[0] / "clearing.db").write_bytes(b"other service")  # never touched
        (backups / "notes").mkdir()

        result = await sup.maintenance.run("20260928")
        assert result["errors"] == {} and result["steps"] == list(mt.STEPS)
        assert calls == ["checkpoint", "backup", "purge", "prune"]
        assert len(result["checkpoint"]) == 3 and result["checkpoint"][0] == 0  # not busy
        assert Path(result["backup"]) == backups / "20260928" / "supervisor.db"
        # 20 newest trading days keep their supervisor.db: the new backup + 19 of the old ones
        kept = sorted(p.name for p in backups.iterdir() if (p / "supervisor.db").exists())
        assert kept == [*days[-19:], "20260928"] and result["pruned_backups"] == days[:6]
        assert (backups / days[0] / "clearing.db").read_bytes() == b"other service"
        assert not (backups / days[1]).exists() and (backups / "notes").is_dir()  # emptied day folders go
        con = sqlite3.connect(backups / "20260928" / "supervisor.db")
        try:
            assert con.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        finally:
            con.close()
    finally:
        await h.stop()


async def test_supervise_stops_everything_when_the_parent_dies(tmp_path: Path, default_config: FullConfig) -> None:
    """``futrisk up --parent-pid`` (interactive ``futrisk demo``): no orphans when the demo CLI goes away."""
    cfg = fast_config(default_config, startup_order=["md-gateway"])
    home = tmp_path / "home"
    h = SupHarness(cfg, home)
    parent = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    task = asyncio.ensure_future(
        supervise(config=cfg, home=home, setup_logs=False, argv_factory=h.argv, install_signals=False, parent_pid=parent.pid)
    )
    try:
        deadline = time.monotonic() + 20
        while True:
            r = await bus_call(cfg, "cli", "svc.ps", PsRequest(), timeout_ms=1000, retries=0)
            if r.ok and str(r.open_as(ServiceList).items[0].state) == "RUNNING":
                break
            assert time.monotonic() < deadline and not task.done()
            await asyncio.sleep(0.1)
        parent.kill()
        assert await asyncio.wait_for(task, 20) == 0
        con = sqlite3.connect(home / "db" / "supervisor.db")
        try:
            last = con.execute("SELECT to_state, reason, exit_code FROM service_events ORDER BY event_id DESC LIMIT 1").fetchone()
        finally:
            con.close()
        assert last == ("STOPPED", "SHUTDOWN", 0)
    finally:
        if parent.poll() is None:
            parent.kill()
        if not task.done():
            task.cancel()


async def test_supervise_end_to_end_down_and_persistence(tmp_path: Path, default_config: FullConfig) -> None:
    """``supervise()`` twice on one home: svc.stop * (futrisk down) stops everything and the supervisor;
    the market clock and the day's restart counters survive the supervisor restart."""
    cfg = fast_config(default_config, startup_order=["md-gateway", "strategy-sim"])
    home = tmp_path / "home"
    h = SupHarness(cfg, home, {"md-gateway": "exit:1.0:3,ok"})  # only for its argv factory

    async def ps_items() -> list[ServiceInfo]:
        r = await bus_call(cfg, "cli", "svc.ps", PsRequest(), timeout_ms=1000, retries=0)
        return r.open_as(ServiceList).items if r.ok else []

    async def run_once(wait_restart: bool) -> tuple[int, list[ServiceInfo]]:
        task = asyncio.ensure_future(supervise(config=cfg, home=home, setup_logs=False, argv_factory=h.argv, install_signals=False))
        deadline = time.monotonic() + 25
        while True:
            items = await ps_items()
            ok = items and all(str(i.state) == "RUNNING" for i in items)
            if ok and (not wait_restart or items[0].restarts >= 1):
                break
            assert time.monotonic() < deadline and not task.done(), f"not running: {items}"
            await asyncio.sleep(0.1)
        r = await bus_call(cfg, "cli", "svc.stop", req("*", f"down-{wait_restart}"))
        assert r.ok and r.data_type == "ServiceList"
        return await asyncio.wait_for(task, 30), items

    code, items = await run_once(wait_restart=True)
    assert code == 0 and items[0].restarts == 1
    code2, items2 = await run_once(wait_restart=False)
    assert code2 == 0 and items2[0].restarts == 1  # restored from service_events of the same trading day

    con = sqlite3.connect(home / "db" / "supervisor.db")
    try:
        rows = con.execute("SELECT instance, to_state, reason, actor FROM service_events ORDER BY event_id").fetchall()
        clock = con.execute("SELECT mode, version FROM clock_state").fetchall()
    finally:
        con.close()
    stops = [(r[0], r[2], r[3]) for r in rows if r[1] == "STOPPING"]
    assert stops == [("strategy-sim", "SHUTDOWN", "cli:tester"), ("md-gateway", "SHUTDOWN", "cli:tester")] * 2
    assert clock == [("DEMO", 1)]  # created once, reused by the second run
