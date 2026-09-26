"""``launch_sim.run``: sim-exchange as a supervised child - committed config, market-clock offset and
version from the supervisor, ``/v1/health``, graceful CTRL_BREAK, exit when ``--parent-pid`` dies
(01 §7.1; 02 §9.2, §9.5). Started on its own (one container per service), it adopts the clock of a
running (clock-only) supervisor, or exits 1 when a configured supervisor gives none."""

from __future__ import annotations

import dataclasses
import os
import signal
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import httpx
import pytest
import yaml

from futrisk.bus import BlockingRpcClient
from futrisk.cli.ops_cmd import SupervisorClient, load_context
from futrisk.common.clock import BJ, bj_to_ns
from futrisk.common.config import FullConfig, ensure_home_config, snapshot_working_config, write_active
from futrisk.common.config.services import ServicesConfig
from futrisk.common.ids import uuid7
from futrisk.common.paths import HomeLayout
from futrisk.messages.enums import ClockMode, ClockTarget
from futrisk.messages.system import ClockJumpRequest, ClockState, SvcRequest
from futrisk.runtime.testing import config_with_free_ports, free_tcp_port
from futrisk.services.supervisor import launch_sim
from futrisk.services.supervisor.process import CLOCK_VERSION_ENV

_WIN = sys.platform == "win32"


def sim_home(root: Path, cfg: FullConfig, services: ServicesConfig | None = None) -> tuple[Path, int, int]:
    """A home whose committed sim.yaml (and ``services``) use free ports; returns (home, feed_b port, control port)."""
    layout = HomeLayout(root)
    ensure_home_config(layout)
    if services is not None:
        text = yaml.safe_dump(services.model_dump(mode="json"), sort_keys=False)
        (layout.config_dir / "services.yaml").write_text(text, encoding="utf-8")
    feed_b, control = free_tcp_port(), free_tcp_port()
    sim = cfg.sim.model_copy(
        update={
            "feed_a": cfg.sim.feed_a.model_copy(update={"port": free_tcp_port()}),
            "feed_b": cfg.sim.feed_b.model_copy(update={"port": feed_b}),
            "control": cfg.sim.control.model_copy(update={"port": control}),
            "broker": cfg.sim.broker.model_copy(update={"endpoint": f"tcp://127.0.0.1:{free_tcp_port()}"}),
        }
    )
    (layout.config_dir / "sim.yaml").write_text(yaml.safe_dump(sim.model_dump(mode="json"), sort_keys=False), encoding="utf-8")
    n, path = snapshot_working_config(layout)
    write_active(layout, n, path)
    return layout.home, feed_b, control


def spawn_sim(
    home: Path, parent_pid: int, offset_ns: int | None, clock_version: int | None, **extra_env: str
) -> subprocess.Popen[bytes]:
    """``launch_sim.run`` in a child; ``offset_ns`` None: started on its own (no --clock-offset-ns)."""
    code = (
        "import sys\n"
        "from futrisk.services.supervisor.launch_sim import run\n"
        f"sys.exit(run(home={str(home)!r}, parent_pid={parent_pid}, clock_offset_ns={offset_ns!r}))\n"
    )
    env = dict(os.environ, PYTHONUTF8="1", **extra_env)
    env.pop(CLOCK_VERSION_ENV, None)
    if clock_version is not None:
        env[CLOCK_VERSION_ENV] = str(clock_version)
    kwargs: dict[str, object] = {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP} if _WIN else {"start_new_session": True}
    log = open(home / "sim.out", "ab")  # noqa: SIM115 - handed to the child
    try:
        return subprocess.Popen([sys.executable, "-c", code], stdout=log, stderr=subprocess.STDOUT, env=env, **kwargs)  # type: ignore[call-overload]
    finally:
        log.close()


def wait_health(proc: subprocess.Popen[bytes], port: int, timeout: float = 30.0) -> None:
    deadline = time.monotonic() + timeout
    with httpx.Client(trust_env=False, timeout=1.0) as client:
        while time.monotonic() < deadline:
            assert proc.poll() is None, "sim-exchange exited early"
            try:
                if client.get(f"http://127.0.0.1:{port}/v1/health").status_code == 200:
                    return
            except httpx.HTTPError:
                pass
            time.sleep(0.1)
    raise AssertionError("sim-exchange health never became ready")


def test_launch_sim_clock_health_and_graceful_break(tmp_path: Path, default_config: FullConfig) -> None:
    home, feed_b, control = sim_home(tmp_path / "home", default_config)
    offset = bj_to_ns(datetime(2026, 9, 28, 9, 35, tzinfo=BJ)) - time.time_ns()
    proc = spawn_sim(home, os.getpid(), offset, clock_version=4)
    try:
        wait_health(proc, feed_b)
        state = httpx.get(f"http://127.0.0.1:{control}/control/state", timeout=5.0, trust_env=False).json()
        assert state["clock"]["version"] == 4 and state["clock"]["mode"] == "DEMO"
        assert abs(state["clock"]["offset_ns"] - offset) == 0
        assert state["trading_day"] == "20260928"
        time.sleep(0.8)  # see test_supervisor_process: a fresh console drops early ctrl events
        if _WIN:
            os.kill(proc.pid, signal.CTRL_BREAK_EVENT)
        else:
            os.killpg(proc.pid, signal.SIGTERM)
        assert proc.wait(20) == 0
        logs = list((home / "logs").glob("sim-exchange_*.log"))
        assert logs and "sim-exchange.launch" in logs[0].read_text(encoding="utf-8")
    finally:
        if proc.poll() is None:
            proc.kill()


def test_launch_sim_exits_when_the_parent_dies(tmp_path: Path, default_config: FullConfig) -> None:
    home, feed_b, _ = sim_home(tmp_path / "home", default_config)
    parent = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    proc = spawn_sim(home, parent.pid, 0, clock_version=0)
    try:
        wait_health(proc, feed_b)
        parent.kill()
        parent.wait(10)
        assert proc.wait(20) == 0
    finally:
        for p in (parent, proc):
            if p.poll() is None:
                p.kill()


def sim_state(control: int) -> dict[str, object]:
    return dict(httpx.get(f"http://127.0.0.1:{control}/control/state", timeout=5.0, trust_env=False).json())


def test_sim_started_on_its_own_adopts_the_running_supervisors_clock(tmp_path: Path, default_config: FullConfig) -> None:
    """docker compose: a clock-only supervisor keeps the demo clock; sim-exchange, started by the
    container runtime without --clock-offset-ns, runs on that clock (offset and version)."""
    cfg = config_with_free_ports(default_config, heartbeat_ms=200)
    home, feed_b, control = sim_home(tmp_path / "home", default_config, services=cfg.services)
    ctx = load_context(str(home))
    flags = subprocess.CREATE_NEW_PROCESS_GROUP if _WIN else 0
    with open(home / "supervisor.out", "ab") as out:
        sup = subprocess.Popen(
            [sys.executable, "-m", "futrisk.main", "up", "--home", str(home), "--headless", "--clock-only"],
            stdout=out, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL, creationflags=flags,
            env=dict(os.environ, PYTHONUTF8="1"),
        )
    sim: subprocess.Popen[bytes] | None = None
    try:
        with SupervisorClient(ctx.cfg) as sc:
            deadline = time.monotonic() + 60
            clock = None
            while clock is None:
                assert sup.poll() is None and time.monotonic() < deadline, (home / "supervisor.out").read_text("utf-8", "replace")
                clock = sc.clock(timeout_ms=1_000)
            # a jump first, so that the version to adopt is not the initial one
            api = BlockingRpcClient(lambda t: ctx.cfg.services.instance(t).router or "", identity="api-gateway")
            try:
                jump = ClockJumpRequest(command_id=uuid7(), target=ClockTarget.AT, at_ns=clock.market_now_ns + 3_600 * 10**9)
                assert api.request("supervisor", "svc.clock", jump, timeout_ms=5_000).ok
            finally:
                api.close()
            clock = sc.clock()
            assert clock is not None and clock.version == 2 and clock.mode == ClockMode.DEMO

            sim = spawn_sim(home, os.getpid(), None, None)
            wait_health(sim, feed_b, timeout=60)
            state = sim_state(control)
            assert state["clock"] == {**state["clock"], "mode": "DEMO", "version": 2, "offset_ns": clock.offset_ns}  # type: ignore[dict-item]
            logs = "".join(p.read_text(encoding="utf-8") for p in (home / "logs").glob("sim-exchange_*.log"))
            assert "sim-exchange.clock_from_supervisor" in logs and "supervisor sys.clock" in logs

            stop = sc.request("svc.stop", SvcRequest(command_id=uuid7(), instance="*", actor="test"), timeout_ms=5_000)
            assert stop.ok
        assert sup.wait(60) == 0
    finally:
        for proc in (sim, sup):
            if proc is not None and proc.poll() is None:
                proc.kill()


def test_sim_started_on_its_own_exits_without_the_supervisors_clock(tmp_path: Path, default_config: FullConfig) -> None:
    """A supervisor is configured but gives no clock: exit 1 (a restart policy tries again) rather
    than trading on a clock the services do not share."""
    cfg = config_with_free_ports(default_config)
    home, _, _ = sim_home(tmp_path / "home", default_config, services=cfg.services)
    t0 = time.monotonic()
    sim = spawn_sim(home, os.getpid(), None, None, FUTRISK_SIM_CLOCK_WAIT_S="1")
    try:
        assert sim.wait(60) == 1
        logs = "".join(p.read_text(encoding="utf-8") for p in (home / "logs").glob("sim-exchange_*.log"))
        assert "sim-exchange.no_market_clock" in logs and "FUTRISK_SIM_CLOCK_WAIT_S" in logs
        assert time.monotonic() - t0 < 50
    finally:
        if sim.poll() is None:
            sim.kill()


def test_start_clock_sources(default_config: FullConfig, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(CLOCK_VERSION_ENV, "7")
    assert launch_sim.start_clock(default_config, 123) == launch_sim.StartClock(123, 7, "--clock-offset-ns")
    prod = dataclasses.replace(default_config, app=default_config.app.model_copy(update={"mode": "prod"}))
    assert launch_sim.start_clock(prod, None) == launch_sim.StartClock(0, 7, "prod: the wall clock")
    services = default_config.services
    no_sup = dataclasses.replace(
        default_config,
        services=services.model_copy(update={"instances": {k: v for k, v in services.instances.items() if k != "supervisor"}}),
    )
    assert launch_sim.start_clock(no_sup, None) == launch_sim.StartClock(0, 0, "no supervisor configured: the wall clock")

    asked: list[float] = []

    def fake_fetch(cfg: FullConfig, *, timeout_s: float, poll_s: float = 0.5) -> ClockState | None:
        asked.append(timeout_s)
        return ClockState(mode=ClockMode.DEMO, offset_ns=-5_000, version=4, market_now_ns=1) if len(asked) == 1 else None

    monkeypatch.setattr(launch_sim, "fetch_supervisor_clock", fake_fetch)
    monkeypatch.setenv(launch_sim.CLOCK_WAIT_ENV, "2.5")
    assert launch_sim.start_clock(default_config, None) == launch_sim.StartClock(-5_000, 4, "supervisor sys.clock")
    assert launch_sim.start_clock(default_config, None, wait_s=0.1) is None
    assert asked == [2.5, 0.1]

