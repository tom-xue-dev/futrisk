"""Market-clock jump orchestration (02 §8.8): step order, heartbeat wait, failure handling (ambiguous svc.clock
TIMEOUT, sim /clock retries, keep-paused), REST body and idempotent replay."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest
from apigw_support import FakeBus, FakeSim, FixedClock, Harness, err, hdr, make_gateway, new_key, with_keys

from futrisk.common.config import FullConfig
from futrisk.messages import codes
from futrisk.messages.enums import ClockMode, ClockTarget, Role
from futrisk.messages.envelope import Reply, ok_reply
from futrisk.messages.system import ClockJumpRequest, ClockState
from futrisk.services.api_gateway.clockjump import ClockJumpOrchestrator
from futrisk.services.api_gateway.ports import SimControlError

HOUR = 3_600_000_000_000


class World:
    """Records the global order of sim and bus calls; heartbeats catch up after the sim clock step."""

    def __init__(self, lag_forever: bool = False) -> None:
        self.clock = FixedClock()  # the sys.clock this process follows (version/offset)
        self.clock.version = 4
        self.order: list[str] = []
        self.versions = {"md-gateway": 4, "risk-engine-1": 4, "api-gateway": 4}
        self.lag_forever = lag_forever
        self.bus = FakeBus()
        self.sim = FakeSim()
        self.bus.on("svc.clock", self.svc_clock)
        orig = self.sim.call

        async def call(method: str, path: str, body: Any = None) -> Any:
            self.order.append(f"sim{path}")
            if path == "/clock" and not self.lag_forever:
                for k in self.versions:
                    self.versions[k] = int(body["version"])
            return await orig(method, path, body)

        self.sim.call = call  # type: ignore[method-assign]

    def svc_clock(self, target: str, req: ClockJumpRequest) -> Reply:
        self.order.append(f"bus:{target}:svc.clock")
        return ok_reply(ClockState(mode=ClockMode.DEMO, offset_ns=5 * HOUR, version=5, market_now_ns=123,
                                   actor=req.actor))

    def orchestrator(self, wait_s: float = 0.3) -> ClockJumpOrchestrator:
        return ClockJumpOrchestrator(bus=self.bus, sim=self.sim, clock_versions=lambda: dict(self.versions),
                                     clock=self.clock, wait_timeout_s=wait_s, poll_s=0.01, confirm_s=0.3,
                                     sim_backoff_s=0.01)


class TestOrchestrator:
    async def test_order_of_steps(self) -> None:
        w = World()
        res = await w.orchestrator().jump(command_id="c1", target=ClockTarget.PRE_CLOSE, at_ns=None, actor="admin")
        assert res.ok and res.code == codes.OK and not res.lagging
        assert w.order == ["sim/pause", "bus:supervisor:svc.clock", "sim/clock", "sim/resume"]
        assert [s["step"] for s in res.steps] == [1, 2, 3, 4, 5] and all(s["ok"] for s in res.steps)
        assert w.sim.calls[1] == ("POST", "/clock", {"offset_ns": 5 * HOUR, "version": 5})
        req = w.bus.of("svc.clock")[0].payload
        assert req.command_id == "c1" and req.target == ClockTarget.PRE_CLOSE and req.actor == "admin"
        assert res.to_dict()["clock"]["version"] == 5 and res.to_dict()["converged"] is True

    async def test_lagging_instances_do_not_block(self) -> None:
        w = World(lag_forever=True)
        res = await w.orchestrator(wait_s=0.2).jump(command_id="c2", target=ClockTarget.NEXT_OPEN, at_ns=None,
                                                    actor="admin")
        assert res.ok and res.lagging == ["api-gateway", "md-gateway", "risk-engine-1"]
        assert 150 <= res.waited_ms < 2000 and w.order[-1] == "sim/resume"
        assert res.steps[3]["ok"] is False

    async def test_supervisor_failure_still_resumes(self) -> None:
        w = World()
        w.bus.on("svc.clock", err(codes.FORBIDDEN, "svc.clock only in demo mode"))
        res = await w.orchestrator().jump(command_id="c3", target=ClockTarget.AT, at_ns=1, actor="admin")
        assert not res.ok and res.code == codes.FORBIDDEN
        assert w.order == ["sim/pause", "sim/resume"]

    async def test_pause_failure_changes_nothing(self) -> None:
        w = World()
        w.sim.fail[("POST", "/pause")] = SimControlError(codes.UNAVAILABLE, "sim down")
        res = await w.orchestrator().jump(command_id="c4", target=ClockTarget.PRE_CLOSE, at_ns=None, actor="admin")
        assert not res.ok and res.code == codes.UNAVAILABLE
        assert not w.bus.calls and [c[1] for c in w.sim.calls] == ["/pause"]

    async def test_sim_clock_failure_keeps_the_sim_paused(self) -> None:
        w = World()
        w.sim.fail[("POST", "/clock")] = SimControlError(codes.UNAVAILABLE, "sim control down")
        res = await w.orchestrator(wait_s=0.05).jump(command_id="c5", target=ClockTarget.PRE_CLOSE, at_ns=None,
                                                     actor="admin")
        # the supervisor moved the clock but the sim could not follow: resuming would run it 5 h behind
        assert not res.ok and res.code == codes.UNAVAILABLE and res.sim_paused and "PAUSED" in res.message
        assert w.order == ["sim/pause", "bus:supervisor:svc.clock", "sim/clock", "sim/clock", "sim/clock"]
        assert "/resume" not in [c[1] for c in w.sim.calls]
        assert res.to_dict()["sim_paused"] is True and res.steps[-1]["action"] == "sim.resume"

    async def test_sim_clock_is_retried(self) -> None:
        w = World()
        fails = [SimControlError(codes.TIMEOUT, "slow")]
        orig = w.sim.call

        async def flaky(method: str, path: str, body: Any = None) -> Any:
            if path == "/clock" and fails:
                w.order.append("sim/clock!")
                raise fails.pop()
            return await orig(method, path, body)

        w.sim.call = flaky  # type: ignore[method-assign]
        res = await w.orchestrator().jump(command_id="c6", target=ClockTarget.PRE_CLOSE, at_ns=None, actor="admin")
        assert res.ok and not res.sim_paused
        assert w.order == ["sim/pause", "bus:supervisor:svc.clock", "sim/clock!", "sim/clock", "sim/resume"]
        assert [(s["step"], s["ok"]) for s in res.steps if s["step"] == 3] == [(3, False), (3, True)]

    async def test_ambiguous_timeout_but_clock_advanced(self) -> None:
        w = World()

        def lost_reply(target: str, req: ClockJumpRequest) -> Reply:
            w.order.append("bus:supervisor:svc.clock")
            w.clock.version, w.clock.offset_ns = 5, 7 * HOUR  # applied and published; the reply was lost
            return err(codes.TIMEOUT, "no answer")

        w.bus.on("svc.clock", lost_reply)
        res = await w.orchestrator().jump(command_id="c7", target=ClockTarget.NEXT_OPEN, at_ns=None, actor="admin")
        # the sim follows the observed sys.clock instead of resuming on the old offset (p09)
        assert res.ok and res.clock is not None and res.clock.version == 5 and "advanced" in res.message
        assert ("POST", "/clock", {"offset_ns": 7 * HOUR, "version": 5}) in w.sim.calls
        assert w.order[-1] == "sim/resume"

    async def test_ambiguous_timeout_without_change_resumes(self) -> None:
        w = World()
        w.bus.on("svc.clock", err(codes.TIMEOUT, "no answer"))
        started = asyncio.get_running_loop().time()
        res = await w.orchestrator().jump(command_id="c8", target=ClockTarget.NEXT_OPEN, at_ns=None, actor="admin")
        waited = asyncio.get_running_loop().time() - started
        assert not res.ok and res.code == codes.TIMEOUT and "did not change" in res.message
        assert waited >= 0.25  # waited for a late sys.clock before concluding nothing happened
        assert [c[1] for c in w.sim.calls] == ["/pause", "/resume"]  # still in sync: resume


@pytest.fixture
async def h(tmp_path: Path, default_config: FullConfig) -> AsyncIterator[Harness]:
    cfg, env = with_keys(default_config)
    w = World()
    harness = await make_gateway(tmp_path, cfg, env=env, bus=w.bus, sim=w.sim, clock_versions=lambda: w.versions)
    try:
        yield harness
    finally:
        await harness.close()


class TestRest:
    async def test_demo_clock_at_iso(self, h: Harness) -> None:
        async with h.client() as c:
            r = await c.post("/api/v1/demo/clock", json={"target": "AT", "at": "2026-09-28T15:31:00+08:00"},
                             headers=hdr(Role.ADMIN, idem=new_key()))
        assert r.status_code == 200, r.json()
        data = r.json()["data"]
        assert data["clock"]["version"] == 5 and [s["step"] for s in data["steps"]] == [1, 2, 3, 4, 5]
        req = h.bus.of("svc.clock")[0].payload
        assert req.target == ClockTarget.AT and req.at_ns == 1790580660000000000 and req.actor == "admin"

    async def test_demo_clock_at_ns_and_validation(self, h: Harness) -> None:
        async with h.client() as c:
            r = await c.post("/api/v1/demo/clock", json={"target": "AT", "at_ns": 42},
                             headers=hdr(Role.ADMIN, idem=new_key()))
            assert r.status_code == 200 and h.bus.of("svc.clock")[-1].payload.at_ns == 42
            r = await c.post("/api/v1/demo/clock", json={"target": "AT", "at": "tomorrow"},
                             headers=hdr(Role.ADMIN, idem=new_key()))
            assert r.status_code == 400
            r = await c.post("/api/v1/demo/clock", json={"target": "SOON"}, headers=hdr(Role.ADMIN, idem=new_key()))
            assert r.status_code == 400
            r = await c.post("/api/v1/demo/clock", json={"target": "PRE_CLOSE"},
                             headers=hdr(Role.RISK_MANAGER, idem=new_key()))
            assert r.status_code == 403

    async def test_failed_jump_is_recorded_for_the_key(self, h: Harness) -> None:
        # the sim does not deduplicate: even a TIMEOUT answer is recorded, a retry with the key replays it (L10)
        h.bus.on("svc.clock", err(codes.TIMEOUT, "no answer"))
        key = new_key()
        async with h.client() as c:
            r1 = await c.post("/api/v1/demo/clock", json={"target": "PRE_CLOSE"}, headers=hdr(Role.ADMIN, idem=key))
            r2 = await c.post("/api/v1/demo/clock", json={"target": "PRE_CLOSE"}, headers=hdr(Role.ADMIN, idem=key))
        assert r1.status_code == 504 and r2.status_code == 504 and r2.json()["duplicate"] is True
        assert len(h.bus.of("svc.clock")) == 1 and [c[1] for c in h.sim.calls].count("/pause") == 1
