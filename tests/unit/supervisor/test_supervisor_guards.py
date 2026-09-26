"""Supervisor guards of the review round: pid-verified liveness and ``ctl.shutdown`` only to a verified
process (A-HIGH-1), the env allow-list and its audit (A-MED-1), the port pre-check and give-up
(A-MED-2), synthetic ``SUPERVISOR_RESTART`` events and the v1 -> v2 schema migration (A-LOW-4),
reaping of grandchildren (A-LOW-5)."""

from __future__ import annotations

import asyncio
import dataclasses
import json
import os
import socket
import sqlite3
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import psutil
import pytest
from sqlalchemy import Connection, insert
from suptest_support import FAKE_CHILD, SupHarness, bus_call, fast_config

from futrisk.common.config import FullConfig
from futrisk.common.config.app import ApiKeyConfig
from futrisk.common.paths import HomeLayout
from futrisk.db import Database
from futrisk.messages import codes
from futrisk.messages.enums import ServiceState
from futrisk.messages.system import (
    Heartbeat,
    PsRequest,
    ServiceEventBatch,
    ServiceInfo,
    ServiceList,
    SvcHistoryRequest,
    SvcRequest,
)
from futrisk.services.supervisor import supervisor as supervisor_module
from futrisk.services.supervisor.process import endpoint_port
from futrisk.services.supervisor.schema import MIGRATIONS, v1_metadata

MD = "md-gateway"


def svc(command_id: str, instance: str, env: dict[str, str | None] | None = None) -> SvcRequest:
    return SvcRequest(command_id=command_id, instance=instance, actor="cli:tester", env=env)


def trace_events(path: Path) -> list[str]:
    if not path.exists():
        return []
    return [line.split(" ", 2)[2].rsplit(" ", 1)[0] for line in path.read_text(encoding="utf-8").splitlines()]


def heartbeat(instance: str, pid: int, epoch: str, home_id: str | None = None) -> Heartbeat:
    return Heartbeat(
        instance=instance,
        service=instance,
        state=ServiceState.RUNNING,
        pid=pid,
        epoch=epoch,
        app_version="test",
        started_ns=time.time_ns(),
        detail={"home_id": home_id} if home_id is not None else {},
    )


async def wait_until(pred: Any, timeout: float, what: str) -> None:
    deadline = time.monotonic() + timeout
    while not pred():
        assert time.monotonic() < deadline, f"timeout waiting for {what}"
        await asyncio.sleep(0.02)


# ------------------------------------------------------------------------------ A-HIGH-1
async def test_a_foreign_owner_of_the_ports_is_neither_liveness_nor_sent_ctl_shutdown(
    tmp_path: Path, default_config: FullConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Another home's md-gateway owns the configured PUB/ROUTER and heartbeats as ``md-gateway``. Our
    md-gateway crash-loops (its bind fails), then a process of it hangs around without ever being live:
    it must not be taken as live, and stopping it must not send ``ctl.shutdown`` to the foreign ROUTER -
    the stop goes straight to our child's CTRL_BREAK ladder."""
    cfg = fast_config(default_config, startup_order=[MD], liveness_s=20.0)
    ic = cfg.services.instances[MD]
    foreign_trace = tmp_path / "foreign.trace"
    foreign = subprocess.Popen(
        [sys.executable, str(FAKE_CHILD), "--instance", MD, "--plan", "ok", "--counter", str(tmp_path / "foreign.count"),
         "--trace", str(foreign_trace), "--parent-pid", str(os.getpid()), "--hb-ms", "100",
         "--pub", str(ic.pub), "--router", str(ic.router)]
    )
    # the pre-check would refuse the spawn (tested below): simulate a port taken right after it
    monkeypatch.setattr(supervisor_module, "port_conflicts", lambda *a, **k: [])
    h = SupHarness(cfg, tmp_path / "home", {MD: "crash:1,crash:1,proc"})
    try:
        await wait_until(lambda: "start ok" in trace_events(foreign_trace), 15, "the foreign md-gateway")
        await asyncio.sleep(0.5)
        sup = await h.start()
        unit = h.unit(MD)
        await h.wait_for(lambda: h.to_states(MD).count("STARTING") == 3, 15, "the third incarnation")
        assert h.to_states(MD).count("FAILED") == 2
        await asyncio.sleep(1.0)  # ten foreign beats later: still not live
        assert str(unit.state) == "STARTING" and not unit.live and unit.svc_pid is None and unit.last_hb_mono is None
        assert all(v is False for v in unit.epoch_verdicts.values())
        assert unit.epoch_verdicts or unit.stale_epochs  # the foreign beats did arrive (and were ignored)
        ps = (await bus_call(cfg, "cli", "svc.ps", PsRequest())).open_as(ServiceList)
        assert ps.home_id == HomeLayout(h.home).home_id() == sup.home_id
        assert ps.items[0].heartbeat_state is None and ps.items[0].pid == unit.proc.pid  # type: ignore[union-attr]

        r = await bus_call(cfg, "cli", "svc.stop", svc("f1", MD))
        assert r.ok
        await h.wait_state(MD, "STOPPED", 20)
        assert "shutdown-request" not in trace_events(foreign_trace)
        assert foreign.poll() is None  # the other home's service is untouched
        assert (MD, "break") in [(i, e) for _, i, e in h.trace_lines()]  # our child: CTRL_BREAK ladder
        assert h.unit(MD).last_exit_code == 0
    finally:
        await h.stop()
        foreign.kill()
        foreign.wait(10)


async def test_only_heartbeats_of_the_child_tree_and_this_home_mean_liveness(
    tmp_path: Path, default_config: FullConfig
) -> None:
    cfg = fast_config(default_config, startup_order=[MD], liveness_s=20.0)
    h = SupHarness(cfg, tmp_path / "home", {MD: "silent"})  # alive, bound, never heartbeats
    try:
        sup = await h.start()
        unit = h.unit(MD)
        await wait_until(lambda: unit.proc is not None and str(unit.state) == "STARTING", 10, "spawn")
        proc = unit.proc
        assert proc is not None
        sup._on_heartbeat(heartbeat(MD, os.getpid(), "e-foreign"))  # outside the child's process tree
        sup._on_heartbeat(heartbeat(MD, proc.pid, "e-other-home", home_id="another-home"))
        sup._on_heartbeat(heartbeat(MD, proc.pid, "e-foreign"))  # the verdict per epoch is kept
        assert str(unit.state) == "STARTING" and not unit.live
        sup._on_heartbeat(heartbeat(MD, proc.pid, "e-own", home_id=sup.home_id))
        assert str(unit.state) == "RUNNING" and unit.live and unit.svc_pid == proc.pid
        assert unit.epoch_verdicts == {"e-foreign": False, "e-other-home": False, "e-own": True}
    finally:
        await h.stop()


# ------------------------------------------------------------------------------ A-MED-2
async def test_a_busy_port_fails_the_start_and_repeated_failures_park_the_unit(
    tmp_path: Path, default_config: FullConfig
) -> None:
    cfg = fast_config(default_config, startup_order=[MD], backoff_initial_ms=100, backoff_max_ms=400)
    host, port = endpoint_port(cfg.services.instances[MD].router) or ("", 0)
    squatter = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    squatter.bind((host, port))
    squatter.listen()
    h = SupHarness(cfg, tmp_path / "home")
    try:
        await h.start()
        await h.wait_for(lambda: any(str(e.reason) == "GAVE_UP" for e in h.events), 20, "GAVE_UP")
        moves = h.moves(MD)
        failed = [e for e in h.events if str(e.to_state) == "FAILED" and str(e.reason) != "GAVE_UP"]
        assert moves[0] == ("STOPPED", "FAILED", "STARTUP") and len(failed) == 5  # K = 5 within M = 5 min
        assert [m for m in moves if m[1] == "RESTARTING"] == [("FAILED", "RESTARTING", "STARTUP")] * 4
        assert moves[-1] == ("FAILED", "FAILED", "GAVE_UP")
        owner = f"port {port} in use by pid {os.getpid()} ({psutil.Process().name()})"
        assert all(e.detail == owner and e.pid is None for e in failed)
        gave_up = h.events[-1]
        assert gave_up.detail is not None and "gave up after 5 failures" in gave_up.detail and owner in gave_up.detail
        # parked: no retries; svc.ps says why
        n = len(h.events)
        await asyncio.sleep(1.0)
        assert len(h.events) == n and not h.unit(MD).desired
        info = (await bus_call(cfg, "cli", "svc.ps", PsRequest())).open_as(ServiceList).items[0]
        assert str(info.state) == "FAILED" and info.last_error == gave_up.detail and info.pid is None
        # the events (with their detail) are persisted and published like any transition
        hist = (await bus_call(cfg, "api-gateway", "svc.history", SvcHistoryRequest())).open_as(ServiceEventBatch)
        assert hist.items == h.events

        # svc.start un-parks it; still busy -> it fails again (and is retried)
        r = await bus_call(cfg, "cli", "svc.start", svc("p1", MD))
        assert r.ok and str(r.open_as(ServiceInfo).state) == "RESTARTING"
        assert h.moves(MD)[n : n + 2] == [("FAILED", "FAILED", "MANUAL"), ("FAILED", "RESTARTING", "MANUAL")]
        assert h.unit(MD).desired and not h.unit(MD).parked
        squatter.close()
        await h.wait_state(MD, "RUNNING", 20)
        assert h.unit(MD).last_error is None
    finally:
        squatter.close()
        await h.stop()


# ------------------------------------------------------------------------------ A-MED-1
async def test_env_overrides_follow_the_allow_list_and_are_audited(tmp_path: Path, default_config: FullConfig) -> None:
    base = fast_config(default_config, startup_order=[MD, "strategy-sim"])
    # FUTRISK_BOTS_SEED carries a secret in this configuration: never overridable
    cfg = dataclasses.replace(
        base, app=base.app.model_copy(update={"api_keys": [ApiKeyConfig(name="k", role="VIEWER", key_env="FUTRISK_BOTS_SEED")]})
    )
    h = SupHarness(cfg, tmp_path / "home", env_trace=("FUTRISK_BOTS_MODE",))

    def seen() -> list[str]:
        return [e for _, i, e in h.trace_lines() if i == "strategy-sim" and e.startswith("env ")]

    try:
        sup = await h.start()
        await h.wait_state("strategy-sim", "RUNNING", 15)
        bad = [
            ("strategy-sim", {"PYTHONPATH": "x"}, "accepts only"),
            ("strategy-sim", {"FUTRISK_X": "1"}, "accepts only"),
            ("strategy-sim", {"FUTRISK_BOTS_SEED": "1"}, "holds a secret"),
            (MD, {"FUTRISK_BOTS_MODE": "scenario"}, "md-gateway accepts no environment overrides"),
        ]
        for n, (inst, env, message) in enumerate(bad):
            r = await bus_call(cfg, "cli", "svc.start", svc(f"b{n}", inst, env))
            assert r.code == codes.BAD_REQUEST and message in (r.message or ""), r
        assert h.unit("strategy-sim").env == {} and h.unit(MD).env == {}

        r = await bus_call(cfg, "api-gateway", "svc.restart", svc("e1", "strategy-sim", {"FUTRISK_BOTS_MODE": "scenario"}))
        assert r.ok
        await h.wait_for(lambda: seen()[-1:] == ["env FUTRISK_BOTS_MODE=scenario"], 15, "scenario mode")
        await h.wait_state("strategy-sim", "RUNNING", 15)
        items = {i.instance: i for i in (await bus_call(cfg, "cli", "svc.ps", PsRequest())).open_as(ServiceList).items}
        assert items["strategy-sim"].env_keys == ["FUTRISK_BOTS_MODE"] and items[MD].env_keys is None

        # null unsets: back to the inherited environment
        r = await bus_call(cfg, "cli", "svc.start", svc("e2", "strategy-sim", {"FUTRISK_BOTS_MODE": None}))
        assert r.ok and str(r.open_as(ServiceInfo).state) == "STOPPING"
        await h.wait_for(lambda: seen()[-1:] == ["env FUTRISK_BOTS_MODE=-"], 15, "normal mode")
        await h.wait_state("strategy-sim", "RUNNING", 15)
        assert h.unit("strategy-sim").env == {}
        await sup.events.flush()
    finally:
        await h.stop()
    con = sqlite3.connect(h.home / "db" / "supervisor.db")
    try:
        rows = con.execute(
            "SELECT to_state, reason, actor, env_keys FROM service_events WHERE instance='strategy-sim' ORDER BY event_id"
        ).fetchall()
    finally:
        con.close()
    audit = [(to, reason, actor, json.loads(keys) if keys else None) for to, reason, actor, keys in rows]
    # names only (never values) with every event, and who changed them
    assert ("STOPPING", "MANUAL", "cli:tester", ["FUTRISK_BOTS_MODE"]) in audit
    assert ("STARTING", "MANUAL", "cli:tester", ["FUTRISK_BOTS_MODE"]) in audit
    assert audit[-1][3] is None and ("STARTING", "MANUAL", "cli:tester", None) in audit
    assert all("scenario" not in json.dumps(a) for a in audit)


async def test_instance_env_of_futrisk_up_follows_the_allow_list(tmp_path: Path, default_config: FullConfig) -> None:
    cfg = fast_config(default_config, startup_order=[MD])
    h = SupHarness(cfg, tmp_path / "home", instance_env={MD: {"FUTRISK_BOTS_MODE": "scenario"}})
    with pytest.raises(ValueError, match="md-gateway accepts no environment overrides"):
        await h.start()
    assert h.rt is not None
    await h.rt.stop()


# ------------------------------------------------------------------------------ A-LOW-4 + migration
def _v1_row(ts_ns: int, instance: str, frm: str, to: str, pid: int | None, reason: str) -> dict[str, Any]:
    return {
        "ts_ns": ts_ns, "trading_day": "20260928", "instance": instance, "service": instance.rsplit("-", 1)[0]
        if instance.startswith("risk-engine") else instance, "pid": pid, "from_state": frm, "to_state": to,
        "exit_code": None, "restarts": 0, "reason": reason, "actor": None,
    }


async def test_v1_database_is_migrated_and_open_episodes_are_closed(tmp_path: Path, default_config: FullConfig) -> None:
    """A supervisor.db of the previous build (schema v1) whose supervisor crashed with units running:
    migration 2 keeps the rows, and the new supervisor records EXITED (SUPERVISOR_RESTART) first."""
    home = tmp_path / "home"
    path = home / "db" / "supervisor.db"
    path.parent.mkdir(parents=True)
    db = Database(path, v1_metadata(), synchronous="FULL", migrations=MIGRATIONS[:1])
    db.open()
    ts = time.time_ns() - 60 * 10**9
    rows = [
        _v1_row(ts, MD, "STOPPED", "STARTING", 4242, "STARTUP"),
        _v1_row(ts + 1, MD, "STARTING", "RUNNING", 4242, "STARTUP"),
        _v1_row(ts + 2, "trade-gateway", "RUNNING", "STOPPING", 4343, "MANUAL"),
        _v1_row(ts + 3, "legacy-svc", "STARTING", "RUNNING", 4444, "STARTUP"),  # no longer managed: left alone
        _v1_row(ts + 4, "clearing-worker", "RUNNING", "EXITED", 4545, "EXITED"),  # closed already
    ]

    def seed(conn: Connection) -> None:
        conn.execute(insert(v1_metadata().tables["service_events"]), rows)

    db.write(seed)
    db.close()

    cfg = fast_config(default_config, startup_order=[MD, "trade-gateway", "clearing-worker"])
    h = SupHarness(cfg, home)
    try:
        await h.start()
        synthetic = [(e.instance, str(e.from_state), str(e.to_state), str(e.reason), e.pid) for e in h.events[:2]]
        assert synthetic == [
            (MD, "RUNNING", "EXITED", "SUPERVISOR_RESTART", 4242),
            ("trade-gateway", "STOPPING", "EXITED", "SUPERVISOR_RESTART", 4343),
        ]
        assert all(e.detail and "supervisor restarted" in e.detail for e in h.events[:2])
        await h.wait_state("clearing-worker", "RUNNING", 20)
        assert h.moves(MD)[1] == ("EXITED", "STARTING", "STARTUP")
        assert h.moves("clearing-worker")[0] == ("STOPPED", "STARTING", "STARTUP")
    finally:
        await h.stop()
    con = sqlite3.connect(path)
    try:
        assert [r[0] for r in con.execute("SELECT version FROM schema_version ORDER BY version")] == [1, 2]
        cols = [r[1] for r in con.execute("PRAGMA table_info(service_events)")]
        assert cols[-2:] == ["env_keys", "detail"]
        assert con.execute("SELECT name FROM sqlite_master WHERE name='ix_service_events__ts'").fetchone()
        old = con.execute("SELECT event_id, instance, pid, env_keys, detail FROM service_events ORDER BY event_id LIMIT 5").fetchall()
        assert [(r[0], r[1], r[2], r[3], r[4]) for r in old] == [
            (i + 1, row["instance"], row["pid"], None, None) for i, row in enumerate(rows)
        ]
        con.execute("INSERT INTO service_events (ts_ns, instance, service, from_state, to_state, restarts, reason) "
                    "VALUES (1, 'x', 'x', 'FAILED', 'FAILED', 0, 'GAVE_UP')")
        with pytest.raises(sqlite3.IntegrityError):
            con.execute("INSERT INTO service_events (ts_ns, instance, service, from_state, to_state, restarts, reason) "
                        "VALUES (1, 'x', 'x', 'FAILED', 'FAILED', 0, 'BOGUS')")
        con.rollback()
    finally:
        con.close()


# ------------------------------------------------------------------------------ A-LOW-5
async def test_grandchildren_are_reaped_after_stop_and_after_a_crash(tmp_path: Path, default_config: FullConfig) -> None:
    cfg = fast_config(default_config, startup_order=[MD, "trade-gateway"])
    h = SupHarness(cfg, tmp_path / "home", {MD: "ok+pool", "trade-gateway": "exit:1.5:3+pool,ok"})

    def grandchildren(inst: str) -> list[int]:
        return [int(e.split()[1]) for _, i, e in h.trace_lines() if i == inst and e.startswith("grandchild ")]

    def running(pid: int) -> bool:
        try:
            p = psutil.Process(pid)
            return p.is_running() and p.status() != psutil.STATUS_ZOMBIE
        except psutil.Error:
            return False

    try:
        await h.start()
        await h.wait_state("trade-gateway", "RUNNING", 20)
        [md_pool] = grandchildren(MD)
        assert running(md_pool)
        # trade-gateway exits by itself: its orphan is reaped after the grace period, then it restarts
        await h.wait_for(lambda: "EXITED" in h.to_states("trade-gateway"), 20, "trade-gateway exit")
        [td_pool] = grandchildren("trade-gateway")[:1]
        await h.wait_for(lambda: not running(td_pool), 10, "trade-gateway's orphan reaped")
        # a graceful svc.stop: the child exits after ctl.shutdown, its pool worker would stay behind
        r = await bus_call(cfg, "cli", "svc.stop", svc("r1", MD))
        assert r.ok
        await h.wait_state(MD, "STOPPED", 20)
        assert h.unit(MD).last_exit_code == 0
        await h.wait_for(lambda: not running(md_pool), 10, "md-gateway's orphan reaped")
    finally:
        await h.stop()
        for pid in grandchildren(MD) + grandchildren("trade-gateway"):
            if running(pid):
                psutil.Process(pid).kill()
