"""QS-04 with a real process: ``futrisk svc risk-engine --shard 1`` hard-killed while an account is HALTED.

The fakes (trade-gateway, md-gateway, clearing-worker and a supervisor serving ``sys.clock``) run in this test
process on the endpoints of a home directory prepared with ``ensure_home_config``; the service loads that
configuration from ``<home>/config/active.json`` like under the supervisor.
"""

from __future__ import annotations

import contextlib
import functools
import os
import shutil
import subprocess
import sys
import threading
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
import yaml

from futrisk.common.config import FullConfig, ensure_home_config, load_active_config
from futrisk.common.paths import HomeLayout, bundled_config_dir
from futrisk.messages import codes
from futrisk.messages.common import ShutdownRequest, StatusRequest
from futrisk.messages.enums import (
    AccountState,
    ClockMode,
    Direction,
    Offset,
    OrderType,
    RiskEventKind,
)
from futrisk.messages.orders import OrderRequest
from futrisk.messages.risk import AccountSnapshotBatch, QueryAccounts, RiskEvent
from futrisk.messages.system import ClockState, Heartbeat
from futrisk.runtime import SyncServiceRuntime
from futrisk.runtime.testing import config_with_free_ports

from .re_support import (
    PEER_TIMEOUT_MISSED,
    TD,
    FakeClearing,
    FakeMdGateway,
    FakeTradeGateway,
    ServiceThread,
    all_sod,
    body,
    client,
    daily_loss_rule,
    demo_clock,
    lv_seq,
    replay,
    request,
    status,
    uuid7,
    wait_for,
    wait_ready,
    yd_positions,
)

pytestmark = [pytest.mark.slow, pytest.mark.timeout(240)]
RE1 = "risk-engine-1"


def prepare_home(home: Path, default_config: FullConfig) -> FullConfig:
    """``<home>/config`` = bundled config with free ports, 100 ms heartbeats and the daily_loss rule only."""
    cfg_dir = home / "config"
    shutil.copytree(bundled_config_dir(), cfg_dir)
    ports = config_with_free_ports(default_config, heartbeat_ms=100, peer_timeout_missed=PEER_TIMEOUT_MISSED)
    (cfg_dir / "services.yaml").write_text(yaml.safe_dump(ports.services.model_dump(mode="json")), encoding="utf-8")
    rr = default_config.risk_rules.model_dump(mode="json")
    rr["rules"] = [daily_loss_rule().model_dump(mode="json")]
    rr["pretrade"].update(throttle_rate_per_s=10_000.0, throttle_burst=10_000)  # the latency probe sends 300 orders
    (cfg_dir / "risk_rules.yaml").write_text(yaml.safe_dump(rr, allow_unicode=True), encoding="utf-8")
    layout = HomeLayout(home)
    ensure_home_config(layout)
    return load_active_config(layout)


def launch(home: Path, log: Path) -> subprocess.Popen[bytes]:
    flags = subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == "win32" else 0
    env = dict(os.environ, PYTHONUTF8="1", PYTHONUNBUFFERED="1", FUTRISK_HOME=str(home))
    with open(log, "ab") as out:
        return subprocess.Popen(
            [sys.executable, "-m", "futrisk.main", "svc", "risk-engine", "--shard", "1", "--home", str(home)],
            stdout=out, stderr=subprocess.STDOUT, cwd=str(home), env=env, creationflags=flags,
        )


class World:
    def __init__(self, tmp: Path, default_config: FullConfig) -> None:
        self.home = tmp / "home"
        self.cfg = prepare_home(self.home, default_config)
        self.clock = demo_clock()
        self.tg = FakeTradeGateway(self.cfg, tmp / "tg", self.clock, positions=yd_positions()).start()
        self.md = FakeMdGateway(self.cfg, tmp / "md", self.clock).start()
        self.cw = FakeClearing(self.cfg, tmp / "cw", self.clock, sods=[all_sod()]).start()
        self.sup_rt = SyncServiceRuntime("supervisor", config=self.cfg, home=tmp / "sup", market_clock=self.clock,
                                         setup_logs=False)
        self.sup = ServiceThread(self.sup_rt).start()
        self.sup_rt.publish("sys.clock", ClockState(mode=ClockMode.DEMO, offset_ns=self.clock.offset_ns, version=1,
                                                    market_now_ns=self.clock.now_ns()))
        self.md.ticks({"rb2701": 3100.0, "IF2612": 4500.0})
        self.api = client(self.cfg, "api-gateway")
        self.bot = client(self.cfg, "strategy-sim")
        self.supervisor = client(self.cfg, "supervisor")
        self.log = tmp / "risk-engine-1.console.log"
        self.proc: subprocess.Popen[bytes] | None = None
        self.running_s = 0.0
        self.running_at = 0.0

    def start(self) -> subprocess.Popen[bytes]:
        t0 = time.monotonic()
        self.proc = launch(self.home, self.log)

        def running() -> bool:
            assert self.proc is not None
            if self.proc.poll() is not None:
                raise AssertionError(f"risk-engine exited {self.proc.returncode}: {self.log.read_text(errors='replace')[-3000:]}")
            r = self.api.request(RE1, "ctl.status", StatusRequest(), timeout_ms=500, retries=0)
            if not r.ok:
                return False
            hb = r.open_as(Heartbeat)
            return bool(hb.detail.get("phase") == "RUNNING")

        wait_for(running, 60, 0.05, "risk-engine subprocess RUNNING")
        self.running_at = time.monotonic()
        self.running_s = self.running_at - t0  # launch -> RUNNING (interpreter start, restore, catch-up, warm-up)
        wait_ready([self.api, self.bot, self.supervisor], RE1)  # every client's DEALER reconnected to the new process
        return self.proc

    def acc3(self) -> Any:
        r = request(self.api, RE1, "risk.query.accounts", QueryAccounts(account_ids=["ACC3"]))
        assert r.ok, r
        return r.open_as(AccountSnapshotBatch).items[0]

    def raised(self) -> list[tuple[str | None, str | None, int | None]]:
        evs = [body(e) for e in replay(self.api, RE1, "risk.event")]
        return sorted((e.rule_id, e.level, e.episode) for e in evs
                      if isinstance(e, RiskEvent) and e.kind == RiskEventKind.ALERT_RAISED and e.account_id == "ACC3")

    def submit_open(self) -> Any:
        req = OrderRequest(client_order_id=uuid7(), account_id="ACC3", instrument_id="rb2701", direction=Direction.BUY,
                           offset=Offset.OPEN, order_type=OrderType.LIMIT, price=2994.0, qty=1, sent_ns=time.time_ns())
        return request(self.bot, RE1, "order.submit", req)

    def close(self) -> None:
        if self.proc is not None and self.proc.poll() is None:
            self.supervisor.request(RE1, "ctl.shutdown", ShutdownRequest(reason="test"), timeout_ms=2000, retries=0)
            try:
                self.proc.wait(15)
            except subprocess.TimeoutExpired:
                self.proc.kill()
        for c in (self.api, self.bot, self.supervisor):
            c.close()
        for f in (self.tg, self.md, self.cw):
            f.stop()
        self.sup.stop()



@contextlib.contextmanager
def live_feed(md: FakeMdGateway, prices: dict[str, float], interval_s: float = 0.2) -> Iterator[None]:
    """md-gateway ticks continuously: re-publish ``prices`` while a process starts (its warm-up waits for LIVE ticks)."""
    stop = threading.Event()

    def run() -> None:
        while not stop.wait(interval_s):
            md.ticks(prices)

    th = threading.Thread(target=run, name="live-feed", daemon=True)
    th.start()
    try:
        yield
    finally:
        stop.set()
        th.join(5)


@pytest.fixture
def world(tmp_path: Path, default_config: FullConfig) -> Iterator[World]:
    w = World(tmp_path, default_config)
    try:
        yield w
    finally:
        w.close()


def test_hard_killed_process_restarts_still_halted_without_duplicate_alerts(world: World) -> None:
    import json

    from .test_re_latency import submit_rtts, summary

    with live_feed(world.md, {"rb2701": 3100.0, "IF2612": 4500.0}):
        proc = world.start()
    first_start_s = world.running_s
    # latency probe against a separate process (the fakes still share this test process)
    world.tg.auto_fill = True
    submit_rtts(world.bot, RE1, "ACC5", 50, 0.005)
    rtts = submit_rtts(world.bot, RE1, "ACC5", 250, 0.005)
    world.tg.auto_fill = False
    hb = status(world.api, RE1)
    probe = {"order_submit_rtt_us_spaced": summary(rtts),
             "shell_latency_us": {k: [v.p50, v.p99, v.max, v.n] for k, v in hb.latency_us.items()}}
    print("risk-engine subprocess latency:", json.dumps(probe))
    for px, state in ((3030.0, AccountState.WARNING), (3015.0, AccountState.RESTRICTED), (2995.0, AccountState.HALTED)):
        world.md.tick("rb2701", px)
        wait_for(functools.partial(lambda s: world.acc3().state == s, state), step=0.05, what=str(state))
    world.md.tick("rb2701", 2994.0)
    wait_for(lambda: world.acc3().halted_latched, step=0.05, what="latched")
    before = world.raised()
    assert before == [("daily_loss", "halt", 1), ("daily_loss", "restrict", 1), ("daily_loss", "warn", 1)]
    assert world.submit_open().code == codes.REJECT_ACCOUNT_HALTED

    t_kill = time.monotonic()
    proc.kill()  # TerminateProcess on Windows: no shutdown hook, no clean close of the database
    assert proc.wait(15) != 0
    with live_feed(world.md, {"rb2701": 2994.0, "IF2612": 4500.0}):
        world.start()
    recovery = {"first_start_to_running_s": round(first_start_s, 2), "restart_to_running_s": round(world.running_s, 2),
                "kill_to_running_s": round(world.running_at - t_kill, 2)}
    print("risk-engine subprocess recovery (QS-04):", json.dumps(recovery))
    assert world.running_s < 30.0
    snap = world.acc3()
    assert snap.state == AccountState.HALTED and snap.halted_latched and snap.trading_day == TD
    seq = lv_seq(world.api, RE1, "risk.snapshot.ACC3")  # timers and snapshot rounds of the new process
    wait_for(lambda: lv_seq(world.api, RE1, "risk.snapshot.ACC3") >= seq + 2, step=0.05, what="two snapshot rounds")
    assert world.raised() == before  # no alert raised twice
    assert world.submit_open().code == codes.REJECT_ACCOUNT_HALTED
    hb = status(world.api, RE1)
    assert "ACC3" in hb.detail["halted"]  # (ACC5 carries the latency probe's 300 lots through the same shock)
