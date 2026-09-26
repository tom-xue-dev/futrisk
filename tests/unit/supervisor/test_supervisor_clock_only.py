"""Clock-only supervisor (one container per service under docker compose): no process is managed, the
market clock is kept and published, nothing complains about the missing units."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest
from loguru import logger
from suptest_support import SupHarness, bus_call, fast_config

from futrisk.common.config import FullConfig
from futrisk.messages import codes, decode
from futrisk.messages.common import SnapshotBatch, SnapshotRequest, StatusRequest
from futrisk.messages.enums import ClockTarget
from futrisk.messages.envelope import decode_envelope
from futrisk.messages.system import (
    ClockJumpRequest,
    ClockState,
    Heartbeat,
    PsRequest,
    ServiceList,
    SvcRequest,
)
from futrisk.services.supervisor import main

HOUR = 3_600 * 1_000_000_000
EVERYTHING = ["sim-exchange", "md-gateway", "trade-gateway", "clearing-worker", "risk-engine-0", "risk-engine-1",
              "api-gateway", "strategy-sim"]


async def test_clock_only_supervisor_keeps_the_clock_and_manages_nothing(tmp_path: Path, default_config: FullConfig) -> None:
    cfg = fast_config(default_config, startup_order=EVERYTHING)  # ignored in clock-only mode
    h = SupHarness(cfg, tmp_path / "home", sup_kwargs={"clock_only": True})
    complaints: list[str] = []
    # a loaded machine can stall the event loop at start-up (loop.stall); that is not about the units
    sink = logger.add(lambda m: complaints.append(str(m)), level="WARNING",
                      format="{level} {name} {message} {extra}", filter=lambda r: r["message"] != "loop.stall")
    try:
        sup = await h.start()
        assert sup.units == {} and sup.clock_only
        await asyncio.sleep(1.0)  # ten heartbeat periods and monitor ticks with no unit
        assert h.events == [] and not (tmp_path / "home" / "trace.log").exists()  # nothing spawned

        ps = (await bus_call(cfg, "cli", "svc.ps", PsRequest())).open_as(ServiceList)
        assert ps.items == [] and ps.home_id == sup.home_id
        hb = (await bus_call(cfg, "cli", "ctl.status", StatusRequest())).open_as(Heartbeat)
        assert hb.detail["clock_only"] is True and hb.detail["home_id"] == sup.home_id

        # the market clock: svc.clock (api-gateway) moves it, sys.clock (LV) carries it
        assert h.rt is not None
        now = h.rt.market_clock.now_ns()
        jump = ClockJumpRequest(command_id="j1", target=ClockTarget.AT, at_ns=now + HOUR, actor="ops")
        r = await bus_call(cfg, "api-gateway", "svc.clock", jump)
        assert r.ok and r.open_as(ClockState).version == 2
        snap = (await bus_call(cfg, "md-gateway", "ctl.snapshot", SnapshotRequest(prefixes=["sys.clock"]))).open_as(SnapshotBatch)
        state = decode(decode_envelope(snap.items[0].envelope).payload, ClockState)
        assert (state.version, state.offset_ns) == (2, h.rt.market_clock.offset_ns)

        # no processes to act on: a clear refusal, not NOT_FOUND
        for n, command in enumerate(("svc.start", "svc.stop", "svc.restart", "svc.kill")):
            r = await bus_call(cfg, "cli", command, SvcRequest(command_id=f"c{n}", instance="md-gateway", actor="t"))
            assert r.code == codes.PRECONDITION_FAILED and "clock-only mode" in (r.message or ""), r
        r = await bus_call(cfg, "cli", "svc.restart", SvcRequest(command_id="all", instance="*", actor="t"))
        assert r.ok and r.open_as(ServiceList).items == []
        await asyncio.sleep(0.3)
        assert h.events == []
    finally:
        await h.stop()
        logger.remove(sink)
    assert complaints == [], complaints  # no warning or error about the missing units


def test_run_takes_clock_only_from_the_flag_or_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    got: list[bool] = []

    async def fake_supervise(**kw: Any) -> int:
        got.append(kw["clock_only"])
        return 0

    monkeypatch.setattr(main, "supervise", fake_supervise)
    monkeypatch.delenv(main.CLOCK_ONLY_ENV, raising=False)
    assert main.run(home="H") == 0
    monkeypatch.setenv(main.CLOCK_ONLY_ENV, "1")  # `futrisk svc supervisor` in a container
    assert main.run(home="H") == 0
    assert main.run(home="H", clock_only=False) == 0  # an explicit argument wins
    monkeypatch.setenv(main.CLOCK_ONLY_ENV, "no")
    assert main.run(home="H") == 0
    assert got == [False, True, False, False]


def test_clock_only_env_values(monkeypatch: pytest.MonkeyPatch) -> None:
    for value, expected in (("1", True), ("true", True), ("YES", True), ("on", True), ("0", False), ("", False)):
        monkeypatch.setenv(main.CLOCK_ONLY_ENV, value)
        assert main.clock_only_from_env() is expected
    monkeypatch.delenv(main.CLOCK_ONLY_ENV)
    assert main.clock_only_from_env() is False
