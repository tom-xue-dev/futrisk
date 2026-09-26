"""Helpers for the supervisor tests: fast configs and a Supervisor harness with dummy children."""

from __future__ import annotations

import asyncio
import dataclasses
import os
import sys
import time
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any

from futrisk.bus import BlockingRpcClient
from futrisk.common.config import FullConfig
from futrisk.common.config.services import RestartPolicy
from futrisk.messages.envelope import Reply
from futrisk.messages.system import ServiceStateChanged
from futrisk.runtime import AsyncServiceRuntime
from futrisk.runtime.testing import config_with_free_ports, free_tcp_port
from futrisk.services.supervisor.process import KIND_HTTP, KIND_PROCESS, InstanceSpec
from futrisk.services.supervisor.schema import DB_NAME, METADATA, MIGRATIONS
from futrisk.services.supervisor.supervisor import Supervisor, Unit

FAKE_CHILD = Path(__file__).with_name("sup_fake_child.py")
MARKET_START = "2026-09-28T09:35:00"  # Monday, continuous session for every product


def fast_config(
    base: FullConfig,
    *,
    startup_order: Iterable[str],
    external: Iterable[str] = ("sim-exchange", "strategy-sim"),
    mode: str = "demo",
    start_at: str | None = MARKET_START,
    hang_ms: int = 400,
    liveness_s: float = 3.0,
    grace_s: float = 1.0,
    backoff_initial_ms: int = 100,
    backoff_max_ms: int = 400,
    stable_reset_s: int = 1,
) -> FullConfig:
    """Default config on free ports with 100 ms heartbeats and fast supervision timings."""
    cfg = config_with_free_ports(base, heartbeat_ms=100)
    services = cfg.services
    sup = services.supervisor.model_copy(
        update={
            "startup_order": list(startup_order),
            "external": list(external),
            "restart": RestartPolicy(
                backoff_initial_ms=backoff_initial_ms, backoff_max_ms=backoff_max_ms, stable_reset_s=stable_reset_s
            ),
            "liveness_timeout_s": liveness_s,
            "shutdown_grace_s": grace_s,
        }
    )
    bus = services.bus.model_copy(update={"hang_threshold_ms": hang_ms})
    services = services.model_copy(update={"supervisor": sup, "bus": bus})
    app = cfg.app.model_copy(update={"mode": mode, "demo": cfg.app.demo.model_copy(update={"start_at": start_at})})
    sim = cfg.sim.model_copy(update={"feed_b": cfg.sim.feed_b.model_copy(update={"port": free_tcp_port()})})
    return dataclasses.replace(cfg, services=services, app=app, sim=sim)


class SupHarness:
    """A real ``supervisor`` runtime + :class:`Supervisor` whose children are dummy processes."""

    def __init__(
        self,
        cfg: FullConfig,
        home: Path,
        plans: dict[str, str] | None = None,
        *,
        tick_s: float = 0.02,
        instance_env: dict[str, dict[str, str | None]] | None = None,
        env_trace: tuple[str, ...] = (),
        sup_kwargs: dict[str, Any] | None = None,
    ) -> None:
        self.cfg = cfg
        self.home = home
        self.plans = dict(plans or {})
        self.trace = home / "trace.log"
        self.tick_s = tick_s
        self.instance_env = instance_env
        self.env_trace = env_trace
        self.sup_kwargs = dict(sup_kwargs or {})
        self.rt: AsyncServiceRuntime | None = None
        self.sup: Supervisor | None = None
        self.events: list[ServiceStateChanged] = []

    def argv(self, spec: InstanceSpec, clock_offset_ns: int | None) -> list[str]:
        inst = spec.instance
        plan = self.plans.get(inst)
        if plan is None:
            if spec.kind == KIND_HTTP:
                plan = f"http:{self.cfg.sim.feed_b.port}"
            elif spec.kind == KIND_PROCESS:
                plan = "proc"
            else:
                plan = "ok"
        argv = [
            sys.executable,
            str(FAKE_CHILD),
            "--instance",
            inst,
            "--plan",
            plan,
            "--counter",
            str(self.home / f"{inst}.count"),
            "--trace",
            str(self.trace),
            "--parent-pid",
            str(os.getpid()),
            "--hb-ms",
            str(self.cfg.services.bus.heartbeat_ms),
        ]
        if spec.is_bus:
            ic = self.cfg.services.instances[inst]
            argv += ["--pub", str(ic.pub), "--router", str(ic.router)]
        for name in self.env_trace:
            argv += ["--env-trace", name]
        return argv

    async def start(self, *, start_at_override: str | None = None) -> Supervisor:
        self.home.mkdir(parents=True, exist_ok=True)
        self.rt = AsyncServiceRuntime("supervisor", config=self.cfg, home=self.home, setup_logs=False)
        await self.rt.start()
        db = await self.rt.open_database(DB_NAME, METADATA, migrations=MIGRATIONS, synchronous="FULL")
        self.sup = Supervisor(
            self.rt,
            db,
            argv_factory=self.argv,
            instance_env=self.instance_env,
            start_at_override=start_at_override,
            tick_s=self.tick_s,
            **self.sup_kwargs,
        )
        self.sup.add_observer(self.events.append)
        await self.sup.start()
        return self.sup

    async def stop(self) -> None:
        if self.sup is not None:
            await self.sup.shutdown()
        if self.rt is not None:
            await self.rt.stop()

    def unit(self, inst: str) -> Unit:
        assert self.sup is not None
        return self.sup.units[inst]

    def moves(self, inst: str) -> list[tuple[str, str, str]]:
        return [(str(e.from_state), str(e.to_state), str(e.reason)) for e in self.events if e.instance == inst]

    def to_states(self, inst: str) -> list[str]:
        return [str(e.to_state) for e in self.events if e.instance == inst]

    async def wait_for(self, pred: Callable[[], bool], timeout: float = 10.0, what: str = "condition") -> None:
        deadline = time.monotonic() + timeout
        while not pred():
            if time.monotonic() > deadline:
                raise AssertionError(f"timeout waiting for {what}; events: {[(e.instance, str(e.to_state), str(e.reason)) for e in self.events]}")
            await asyncio.sleep(0.02)

    async def wait_state(self, inst: str, state: str, timeout: float = 10.0) -> None:
        await self.wait_for(lambda: str(self.unit(inst).state) == state, timeout, f"{inst} {state}")

    async def wait_moves(self, inst: str, count: int, timeout: float = 10.0) -> None:
        await self.wait_for(lambda: len(self.moves(inst)) >= count, timeout, f"{count} transitions of {inst}")

    def trace_lines(self) -> list[tuple[float, str, str]]:
        """``(wall s, instance, event)`` lines written by the dummy children (pid stripped)."""
        if not self.trace.exists():
            return []
        out = []
        for line in self.trace.read_text(encoding="utf-8").splitlines():
            ts, inst, rest = line.split(" ", 2)
            out.append((float(ts), inst, rest.rsplit(" ", 1)[0]))
        return out


async def bus_call(cfg: FullConfig, identity: str, command: str, payload: Any, *, target: str = "supervisor", **kw: Any) -> Reply:
    """Blocking DEALER request run in a worker thread (the event loop keeps serving)."""

    def run() -> Reply:
        client = BlockingRpcClient(lambda t: cfg.services.instance(t).router or "", identity=identity)
        try:
            return client.request(target, command, payload, **kw)
        finally:
            client.close()

    return await asyncio.to_thread(run)
