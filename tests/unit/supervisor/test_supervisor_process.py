"""ProcessManager: instance specs, command lines, environment and its allow-list, port pre-check,
console logs (ANSI, rotation while running), spawn / kill / CTRL_BREAK, descendants (01 §6.8, §7.1)."""

from __future__ import annotations

import asyncio
import dataclasses
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import psutil
import pytest

from futrisk.common.config import FullConfig
from futrisk.common.config.app import ApiKeyConfig
from futrisk.services.supervisor.parent import ParentWatcher
from futrisk.services.supervisor.process import (
    CLOCK_VERSION_ENV,
    ConsolePump,
    InstanceSpec,
    ProcessManager,
    apply_env,
    build_specs,
    endpoint_port,
    loggable_env,
    port_conflicts,
    rotate_console_log,
    secret_env_names,
    service_argv,
    strip_ansi,
    validate_env,
)


def _with_order(cfg: FullConfig, order: list[str]) -> FullConfig:
    sup = cfg.services.supervisor.model_copy(update={"startup_order": order})
    return dataclasses.replace(cfg, services=cfg.services.model_copy(update={"supervisor": sup}))


def test_specs_follow_the_startup_order(default_config: FullConfig) -> None:
    specs = build_specs(default_config)
    assert [s.instance for s in specs] == [
        "sim-exchange",
        "md-gateway",
        "trade-gateway",
        "clearing-worker",
        "risk-engine-0",
        "risk-engine-1",
        "api-gateway",
        "strategy-sim",
    ]
    by = {s.instance: s for s in specs}
    assert by["sim-exchange"].kind == "http" and by["sim-exchange"].health_url == "http://127.0.0.1:7002/v1/health"
    assert by["strategy-sim"].kind == "process" and by["md-gateway"].kind == "bus"
    assert (by["risk-engine-1"].service, by["risk-engine-1"].shard) == ("risk-engine", 1)
    assert by["md-gateway"].shard is None


def test_unknown_duplicate_and_self_entries_are_skipped(default_config: FullConfig) -> None:
    cfg = _with_order(default_config, ["md-gateway", "nope", "md-gateway", "supervisor", "risk-engine-1"])
    assert [s.instance for s in build_specs(cfg)] == ["md-gateway", "risk-engine-1"]


def test_service_argv_dev_and_frozen() -> None:
    re1 = InstanceSpec("risk-engine-1", "risk-engine", "bus", shard=1)
    assert service_argv(re1, home=Path("H"), parent_pid=42, executable="py", frozen=False) == [
        "py", "-m", "futrisk.main", "svc", "risk-engine", "--home", "H", "--parent-pid", "42", "--shard", "1",
    ]
    sim = InstanceSpec("sim-exchange", "sim-exchange", "http")
    assert service_argv(sim, home="H", parent_pid=42, clock_offset_ns=-123, frozen=True, executable="futrisk.exe") == [
        "futrisk.exe", "svc", "sim-exchange", "--home", "H", "--parent-pid", "42", "--clock-offset-ns=-123",
    ]
    md = service_argv(InstanceSpec("md-gateway", "md-gateway"), home="H", parent_pid=1, clock_offset_ns=5, executable="py", frozen=False)
    assert md[-2:] == ["--parent-pid", "1"]  # neither a shard nor a clock offset


def test_child_environment(tmp_path: Path) -> None:
    pm = ProcessManager(home=tmp_path, logs_dir=tmp_path / "logs", parent_pid=7, extra_env={"X_TEST": "1"})
    env = pm.env(InstanceSpec("sim-exchange", "sim-exchange", "http"), clock_version=3)
    assert env["PYTHONUTF8"] == "1" and env["PYTHONUNBUFFERED"] == "1" and env["PYTHONIOENCODING"] == "utf-8"
    assert env[CLOCK_VERSION_ENV] == "3" and env["X_TEST"] == "1" and env["FUTRISK_HOME"] == str(tmp_path)
    assert CLOCK_VERSION_ENV not in pm.env(InstanceSpec("md-gateway", "md-gateway"))


def test_instance_env_precedence_and_validation(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FUTRISK_BOTS_MODE", "inherited")
    monkeypatch.setenv("X_SHARED", "inherited")
    monkeypatch.setenv("TERM", "xterm-256color")  # loguru would colour the child's redirected stderr
    pm = ProcessManager(home=tmp_path, logs_dir=tmp_path / "logs", extra_env={"X_SHARED": "extra"})
    spec = InstanceSpec("strategy-sim", "strategy-sim", "process")
    env = pm.env(spec, None, {"FUTRISK_BOTS_MODE": "scenario", "X_SHARED": "instance", "PYTHONUTF8": "0"})
    # inherited < extra_env < per-instance overrides < the supervisor's mandatory keys
    assert (env["FUTRISK_BOTS_MODE"], env["X_SHARED"], env["PYTHONUTF8"]) == ("scenario", "instance", "1")
    assert pm.env(spec)["FUTRISK_BOTS_MODE"] == "inherited" and "TERM" not in env
    assert loggable_env({"FUTRISK_API_KEY": "k", "FUTRISK_BOTS_MODE": "scenario"}) == {
        "FUTRISK_API_KEY": "***",
        "FUTRISK_BOTS_MODE": "scenario",
    }


def test_env_override_allow_list(default_config: FullConfig) -> None:
    """A-MED-1: per-service allow-list (strategy-sim FUTRISK_BOTS_*), never a secret's name, null unsets."""
    ok = {"FUTRISK_BOTS_MODE": "scenario", "FUTRISK_BOTS_SEED": "7", "FUTRISK_BOTS_ACCOUNTS": None}
    assert validate_env("strategy-sim", ok) == ok
    assert validate_env("strategy-sim", {}) == {}
    cases = [
        ("strategy-sim", {"PYTHONPATH": "x"}, "accepts only FUTRISK_BOTS_ACCOUNTS, FUTRISK_BOTS_MODE, FUTRISK_BOTS_SEED"),
        ("strategy-sim", {"FUTRISK_X": "1"}, "not 'FUTRISK_X'"),
        ("md-gateway", {"FUTRISK_BOTS_MODE": "scenario"}, "md-gateway accepts no environment overrides"),
        ("risk-engine", {"FUTRISK_HOME": "elsewhere"}, "accepts no environment overrides"),
        ("strategy-sim", {"FUTRISK_BOTS_MODE": "x" * 5000}, "at most 1024 characters"),
        ("strategy-sim", {"FUTRISK_BOTS_MODE": "a\x00b"}, "at most 1024 characters"),
    ]
    for service, env, message in cases:
        with pytest.raises(ValueError, match=message):
            validate_env(service, env)
    # a name that carries a secret in the configuration is never accepted
    cfg = dataclasses.replace(
        default_config,
        app=default_config.app.model_copy(
            update={
                "api_keys": [ApiKeyConfig(name="ops", role="ADMIN", key_env="FUTRISK_BOTS_SEED")],
                "notify": default_config.app.notify.model_copy(update={"webhook_url_env": "FUTRISK_WEBHOOK"}),
            }
        ),
    )
    secrets = secret_env_names(cfg)
    assert secrets == {"FUTRISK_BOTS_SEED", "FUTRISK_WEBHOOK"}
    with pytest.raises(ValueError, match="FUTRISK_BOTS_SEED holds a secret"):
        validate_env("strategy-sim", {"FUTRISK_BOTS_SEED": "1"}, forbidden=secrets)
    assert apply_env({"A": "1", "B": "2"}, {"A": None, "C": "3", "Z": None}) == {"B": "2", "C": "3"}


def test_specs_carry_the_ports_to_check(default_config: FullConfig) -> None:
    by = {s.instance: s for s in build_specs(default_config)}
    md = default_config.services.instances["md-gateway"]
    assert by["md-gateway"].ports == tuple(p for p in (endpoint_port(md.pub), endpoint_port(md.router)) if p)
    api = default_config.services.instances["api-gateway"]
    assert endpoint_port(api.http) in by["api-gateway"].ports
    sim = default_config.sim
    assert (sim.feed_a.host, sim.feed_a.port) in by["sim-exchange"].ports
    assert (sim.feed_b.host, sim.feed_b.port) in by["sim-exchange"].ports
    assert endpoint_port("tcp://*:7501") == ("0.0.0.0", 7501) and endpoint_port("inproc://x") is None
    assert endpoint_port("http://127.0.0.1:8000/api") == ("127.0.0.1", 8000)


def test_port_conflicts_name_the_owner() -> None:
    """A-MED-2: a port another process listens on is reported as 'port N in use by pid X (name)'."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
        assert port_conflicts([("127.0.0.1", port)]) == []  # bound but not listening
        s.listen()
        [c] = port_conflicts([("127.0.0.1", port), ("127.0.0.1", 1)])
        assert (c.port, c.pid) == (port, os.getpid())
        assert c.describe() == f"port {port} in use by pid {os.getpid()} ({psutil.Process().name()})"
        assert port_conflicts([("0.0.0.0", port)]) != []  # a wildcard bind would clash as well
        assert port_conflicts([("127.0.0.1", port)], ignore_pids={os.getpid()}) == []
    assert port_conflicts([("127.0.0.1", port)]) == []


async def test_spawn_applies_the_instance_env(tmp_path: Path) -> None:
    code = "import os, time; print('mode', os.environ.get('FUTRISK_BOTS_MODE'), flush=True); time.sleep(60)"
    pm = ProcessManager(home=tmp_path, logs_dir=tmp_path / "logs", argv_factory=lambda s, o: [sys.executable, "-c", code])
    child = pm.spawn(InstanceSpec("strategy-sim", "strategy-sim", "process"), env={"FUTRISK_BOTS_MODE": "scenario"})
    try:
        content = await _wait_text(tmp_path / "logs" / "strategy-sim.console.log", "mode scenario")
        assert "spawn strategy-sim (env FUTRISK_BOTS_MODE=scenario)" in content
    finally:
        child.kill_tree()
        await child.wait(10)


def test_console_log_rotation(tmp_path: Path) -> None:
    log = tmp_path / "x.console.log"
    log.write_bytes(b"x" * 200)
    rotate_console_log(log, max_bytes=100)
    assert not log.exists() and (tmp_path / "x.console.log.1").read_bytes() == b"x" * 200
    log.write_bytes(b"small")
    rotate_console_log(log, max_bytes=100)
    assert log.read_bytes() == b"small"


def test_console_pump_strips_ansi_and_rotates_while_the_child_runs(tmp_path: Path) -> None:
    """A-LOW-3: size-capped console logs without escape sequences, even with a reader holding the file."""
    assert strip_ansi(b"\x1b[31mred\x1b[0m \x1b]0;title\x07ok") == b"red ok"
    log = tmp_path / "x.console.log"
    r, w = os.pipe()
    reader = os.fdopen(r, "rb", buffering=0)
    pump = ConsolePump(reader, log, max_bytes=1024, name="x")
    pump.start()
    line = b"\x1b[32m2026-09-28 INFO\x1b[0m " + b"y" * 60 + b"\n"
    older = tmp_path / "x.console.log.1"
    deadline = time.monotonic() + 10
    while not older.exists():  # line by line until the cap moves the file to .1
        assert time.monotonic() < deadline, "never rotated"
        os.write(w, line)
        time.sleep(0.01)
    assert pump.rotations >= 1
    with open(log, "rb"):  # a reader without delete sharing (e.g. an editor, or tail on Windows)
        for _ in range(40):  # ~3 KB more: past the cap again while the file is held
            os.write(w, line)
        os.write(w, b"last line\n")
        os.close(w)
        pump.join(10)
    assert not pump.running and pump.rotations + pump.truncations >= 2
    if sys.platform == "win32":
        assert pump.truncations >= 1  # rename refused while held: truncated in place instead
    current = log.read_bytes()
    assert b"\x1b" not in current + older.read_bytes()
    assert current.endswith(b"last line\n")  # the newest output always lands in the current file
    if sys.platform == "win32":  # .1 is still the file of the first rotation, written line by line
        assert len(older.read_bytes()) <= 1024  # rotated before a write would pass the cap
    assert len(current) <= 1024 + 65536 and len(older.read_bytes()) <= 1024 + 65536  # a read (64 KiB) is never split


async def test_descendants_are_recorded_and_reaped(tmp_path: Path) -> None:
    """A-LOW-5: a grandchild (process-pool worker) that outlives the child is killed afterwards."""
    code = (
        "import subprocess, sys, time\n"
        "flags = getattr(subprocess, 'CREATE_NEW_PROCESS_GROUP', 0)\n"
        "p = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'], creationflags=flags)\n"
        "print('grandchild', p.pid, flush=True)\n"
        "time.sleep(60)\n"
    )
    pm = ProcessManager(home=tmp_path, logs_dir=tmp_path / "logs", argv_factory=lambda s, o: [sys.executable, "-c", code])
    child = pm.spawn(InstanceSpec("pool", "pool", "process"))
    grandchild: psutil.Process | None = None
    try:
        content = await _wait_text(tmp_path / "logs" / "pool.console.log", "grandchild ")
        pid = int(content.split("grandchild ", 1)[1].split()[0])
        grandchild = psutil.Process(pid)
        assert child.refresh_descendants() >= 1
        assert child.is_own_pid(child.pid) and child.is_own_pid(pid) and not child.is_own_pid(os.getpid())
        assert pid in child.tree_pids()
        child.popen.kill()  # only the spawned process: its descendants survive it
        assert await child.wait(10) is not None
        assert pid in child.alive_descendants()
        assert pid in child.reap_descendants()
        grandchild.wait(10)
        assert not grandchild.is_running() and child.descendant_pids() == set()
        assert not child.is_own_pid(pid)  # the child is gone: nothing is its descendant any more
    finally:
        child.kill_tree()
        if grandchild is not None and grandchild.is_running():
            grandchild.kill()


async def _wait_text(path: Path, text: str, timeout: float = 10.0) -> str:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            content = path.read_text(encoding="utf-8", errors="replace")
            if text in content:
                return content
        await asyncio.sleep(0.02)
    raise AssertionError(f"{text!r} not in {path}")


async def test_spawn_console_log_and_kill_tree(tmp_path: Path) -> None:
    code = "import os, time; print('hello', os.environ['PYTHONUTF8'], flush=True); time.sleep(60)"
    pm = ProcessManager(home=tmp_path, logs_dir=tmp_path / "logs", argv_factory=lambda s, o: [sys.executable, "-c", code])
    child = pm.spawn(InstanceSpec("dummy", "dummy", "process"))
    assert child.alive and child.poll() is None
    content = await _wait_text(tmp_path / "logs" / "dummy.console.log", "hello 1")
    assert "spawn dummy" in content
    child.kill_tree()
    assert await child.wait(10) is not None
    assert not child.alive and child.send_ctrl_break() is False


async def test_ctrl_break_reaches_the_child_group(tmp_path: Path) -> None:
    code = (
        "import signal, sys, time\n"
        "signal.signal(getattr(signal, 'SIGBREAK', signal.SIGTERM), lambda *a: sys.exit(7))\n"
        "print('ready', flush=True)\n"
        "while True: time.sleep(0.02)\n"
    )
    pm = ProcessManager(home=tmp_path, logs_dir=tmp_path / "logs", argv_factory=lambda s, o: [sys.executable, "-c", code])
    child = pm.spawn(InstanceSpec("breaker", "breaker", "process"))
    try:
        await _wait_text(tmp_path / "logs" / "breaker.console.log", "ready")
        # without a parent console (CI, IDE runners) every child gets a fresh console host,
        # which drops ctrl events sent within its first few hundred ms
        await asyncio.sleep(0.8)
        assert child.send_ctrl_break()
        assert await child.wait(10) == 7
    finally:
        child.kill_tree()


async def test_parent_watcher(tmp_path: Path) -> None:
    proc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(0.3)"])
    watcher = ParentWatcher(proc.pid)
    assert watcher.alive()
    await asyncio.to_thread(proc.wait)
    await asyncio.wait_for(watcher.wait_gone(interval_s=0.05), 10)
    assert not watcher.alive()
    assert not ParentWatcher(2**22 + 12345).alive()
