"""Real stack (slow): hard-kill a real supervisor running the real services, then restart it on the
same home and stop everything with ``futrisk down`` (the reviewer's probe A1, kept as a test).

Checks: every service process exits on its own once the supervisor is gone (parent watch); the
restarted supervisor reuses the market clock, records ``EXITED`` (``SUPERVISOR_RESTART``) for the
episodes the crash left open and gets every service RUNNING again (pid-verified heartbeats through
the venv launcher); ``svc.ps`` reports this home's id; ``down`` leaves no process behind.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import psutil
import pytest
import yaml

from futrisk.cli.ops_cmd import SupervisorClient, load_context
from futrisk.common.config import ensure_home_config
from futrisk.common.paths import HomeLayout, bundled_config_dir
from futrisk.messages.system import ServiceEventBatch, ServiceList, SvcHistoryRequest
from futrisk.runtime.testing import free_tcp_port

pytestmark = [pytest.mark.slow, pytest.mark.timeout(600)]

_used: set[int] = set()


def _port() -> int:
    while True:
        p = free_tcp_port()
        if p not in _used and not (7000 <= p <= 8100):  # stay clear of the default ports
            _used.add(p)
            return p


def make_home(home: Path) -> Path:
    """A home whose every port is a fresh free loopback port (the reviewer's mkhome.py)."""
    cfg_dir = home / "config"
    shutil.copytree(bundled_config_dir(), cfg_dir)

    def edit(name: str, fn: Any) -> None:
        path = cfg_dir / name
        doc = yaml.safe_load(path.read_text(encoding="utf-8"))
        fn(doc)
        path.write_text(yaml.safe_dump(doc, sort_keys=False, allow_unicode=True), encoding="utf-8")

    def services(doc: dict[str, Any]) -> None:
        for inst in doc["instances"].values():
            inst["pub"] = f"tcp://127.0.0.1:{_port()}"
            inst["router"] = f"tcp://127.0.0.1:{_port()}"
            if "http" in inst:
                inst["http"] = f"http://127.0.0.1:{_port()}"

    fa, fb, br, ct = _port(), _port(), _port(), _port()

    def sim(doc: dict[str, Any]) -> None:
        doc["feed_a"]["port"], doc["feed_b"]["port"] = fa, fb
        doc["broker"]["endpoint"] = f"tcp://127.0.0.1:{br}"
        doc["control"]["port"] = ct

    def feeds(doc: dict[str, Any]) -> None:
        doc["feed_a"]["url"] = f"ws://127.0.0.1:{fa}/feed"
        doc["feed_b"]["url"] = f"http://127.0.0.1:{fb}/v1/quotes"

    edit("services.yaml", services)
    edit("sim.yaml", sim)
    edit("feeds.yaml", feeds)
    ensure_home_config(HomeLayout(home))
    return home


def start_up(home: Path, tag: str) -> subprocess.Popen[bytes]:
    out = open(home / f"up_{tag}.out", "ab")  # noqa: SIM115 - owned by the child
    flags = subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == "win32" else 0
    env = dict(os.environ, PYTHONUTF8="1")
    env.pop("TERM", None)
    return subprocess.Popen(
        [sys.executable, "-m", "futrisk.main", "up", "--home", str(home), "--headless"],
        stdout=out, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL, creationflags=flags, env=env,
    )


def ps(sc: SupervisorClient) -> ServiceList | None:
    return sc.ps(timeout_ms=2_000)


def wait_all_running(sc: SupervisorClient, up: subprocess.Popen[bytes], timeout: float) -> ServiceList:
    deadline = time.monotonic() + timeout
    last: ServiceList | None = None
    while time.monotonic() < deadline:
        assert up.poll() is None, f"futrisk up exited with {up.returncode}"
        last = ps(sc)
        if last is not None and last.items and all(str(i.state) == "RUNNING" for i in last.items):
            return last
        time.sleep(1.0)
    raise AssertionError(f"not all RUNNING after {timeout}s: {last}")


def alive(p: psutil.Process) -> bool:
    try:
        return p.is_running() and p.status() != psutil.STATUS_ZOMBIE
    except psutil.Error:
        return False


def test_hard_killed_supervisor_takes_its_services_along_and_restarts_cleanly(tmp_path: Path) -> None:
    home = make_home(tmp_path / "home")
    ctx = load_context(str(home))
    up = start_up(home, "first")
    up2: subprocess.Popen[bytes] | None = None
    leftovers: list[psutil.Process] = []
    try:
        with SupervisorClient(ctx.cfg) as sc:
            first = wait_all_running(sc, up, 240)
            assert first.home_id == HomeLayout(home).home_id()
            assert len(first.items) == 8 and all(i.pid for i in first.items)

            # hard kill of the supervisor interpreter (the venv launcher's child, or `up` itself)
            procs = psutil.Process(up.pid).children(recursive=True)
            direct = [p for p in procs if p.ppid() == up.pid]
            supervisor = direct[0] if direct else psutil.Process(up.pid)
            victims = [p for p in procs if p.pid != supervisor.pid]
            assert len(victims) >= 8
            t0 = time.monotonic()
            supervisor.kill()
            while any(alive(p) for p in victims) and time.monotonic() - t0 < 45:
                time.sleep(0.25)
            leftovers = [p for p in victims if alive(p)]
            assert leftovers == [], f"still running {time.monotonic() - t0:.1f}s after the crash: {leftovers}"
            up.wait(30)

            # the same home again: the clock is reused, the crash's open episodes are closed first
            up2 = start_up(home, "second")
            second = wait_all_running(sc, up2, 240)
            assert second.home_id == first.home_id
            assert {i.instance for i in second.items} == {i.instance for i in first.items}
            clock = sc.clock()
            assert clock is not None and clock.version == 1
            reply = sc.request("svc.history", SvcHistoryRequest(), timeout_ms=5_000)
            events = reply.open_as(ServiceEventBatch).items
            crash = [e for e in events if str(e.reason) == "SUPERVISOR_RESTART"]
            assert {e.instance for e in crash} == {i.instance for i in first.items}
            assert all(str(e.from_state) == "RUNNING" and str(e.to_state) == "EXITED" for e in crash)
            pids = {i.instance: i.pid for i in first.items}
            assert {e.instance: e.pid for e in crash} == pids  # the processes that died with the supervisor

        tree = psutil.Process(up2.pid).children(recursive=True)
        down = subprocess.run(
            [sys.executable, "-m", "futrisk.main", "down", "--home", str(home)],
            capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=300,
            env=dict(os.environ, PYTHONUTF8="1", COLUMNS="200"),
        )
        assert down.returncode == 0, down.stdout + down.stderr
        assert "all services stopped" in down.stdout
        assert up2.wait(60) == 0
        deadline = time.monotonic() + 30
        while any(alive(p) for p in tree) and time.monotonic() < deadline:
            time.sleep(0.25)
        leftovers = [p for p in tree if alive(p)]
        assert leftovers == []
    finally:
        for proc in (up, up2):
            if proc is not None and proc.poll() is None:
                for p in psutil.Process(proc.pid).children(recursive=True):
                    leftovers.append(p)
                proc.kill()
        for p in leftovers:
            if alive(p):
                p.kill()
