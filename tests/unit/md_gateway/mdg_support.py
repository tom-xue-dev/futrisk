"""Integration-test support: a sim-exchange + a real md-gateway runtime + a bus SUB collector."""

from __future__ import annotations

import asyncio
import dataclasses
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import zmq
import zmq.asyncio

from futrisk.common.clock import MarketClock
from futrisk.common.config import FullConfig
from futrisk.common.config.feeds import ArbitrationConfig
from futrisk.messages.base import decode
from futrisk.messages.enums import SourceId
from futrisk.messages.envelope import Envelope, decode_envelope
from futrisk.messages.md import MdEvent, MdStatus, Tick
from futrisk.runtime import AsyncServiceRuntime
from futrisk.runtime.testing import config_with_free_ports
from futrisk.services.md_gateway.params import MdParams
from futrisk.services.md_gateway.service import MdGatewayService
from futrisk.sim.exchange.testing import make_test_config

#: configured in products.yaml but left out of the test sim, to cover "configured but never observed"
UNSIMULATED = "rb2610"
#: simulated instruments of config/sim.yaml, minus UNSIMULATED
SIM_INSTRUMENTS = ("rb2701", "rb2705", "hc2701", "cu2611", "au2612", "i2701", "j2701", "m2701", "IF2612", "IH2612",
                   "IC2612", "T2612")


def md_config(default: FullConfig) -> FullConfig:
    """Sim on ephemeral ports (50 ms ticks, 200 ms heartbeats) + bus endpoints on free ports + fast Feed B.

    The test sim leaves out :data:`UNSIMULATED` so that one configured instrument is never observed.
    """
    cfg = config_with_free_ports(make_test_config(default), heartbeat_ms=100)
    pe = cfg.sim.price_engine
    sim = cfg.sim.model_copy(update={"price_engine": pe.model_copy(
        update={"instruments": {k: v for k, v in pe.instruments.items() if k != UNSIMULATED}})})
    cfg = dataclasses.replace(cfg, sim=sim)
    feeds = cfg.feeds.model_copy(
        update={
            "feed_a": cfg.feeds.feed_a.model_copy(update={"reconnect_backoff_ms": [50, 100, 200]}),
            "feed_b": cfg.feeds.feed_b.model_copy(update={"poll_ms": 200, "timeout_ms": 500}),
        }
    )
    return dataclasses.replace(cfg, feeds=feeds)


def fast_params(**arb: Any) -> MdParams:
    base: dict[str, Any] = {
        "probation_healthy_s": 1.0,
        "min_dwell_s": 0.5,
        "stale_s": 0.6,
        "unknown_s": 2.0,
        "open_grace_s": 1.0,
    }
    base.update(arb)
    # the start-up grace covers connect + bootstrap (probation 1 s + dwell), like 30 s vs 10 s in production
    return MdParams(arb=ArbitrationConfig(**base), startup_grace_s=5.0, raw_roll_s=1.0, record_handoff_s=0.1, bar_close_grace_s=1.0)


async def start_md(
    cfg: FullConfig, home: Path, offset_ns: int, feed_a_url: str, feed_b_url: str, params: MdParams
) -> tuple[AsyncServiceRuntime, MdGatewayService]:
    rt = AsyncServiceRuntime(
        "md-gateway",
        config=cfg,
        home=home,
        market_clock=MarketClock(mode="DEMO", offset_ns=offset_ns),
        setup_logs=False,
    )
    await rt.start()
    svc = MdGatewayService(rt, params=params, source_urls={SourceId.FEED_A: feed_a_url, SourceId.FEED_B: feed_b_url})
    await svc.start()
    return rt, svc


class Collector:
    """SUB socket on md-gateway's PUB: keeps every md.tick / md.event / md.status envelope."""

    def __init__(self, endpoint: str, prefixes: tuple[str, ...] = ("md.tick.", "md.event", "md.status", "md.bar.1m.")) -> None:
        self.ctx = zmq.asyncio.Context()
        self.sock = self.ctx.socket(zmq.SUB)
        self.sock.setsockopt(zmq.RCVHWM, 0)
        for p in prefixes:
            self.sock.setsockopt(zmq.SUBSCRIBE, p.encode())
        self.sock.connect(endpoint)
        self.ticks: list[Tick] = []
        self.events: list[MdEvent] = []
        self.status: MdStatus | None = None
        self.bars: list[Envelope] = []
        self._task = asyncio.create_task(self._run())

    async def _run(self) -> None:
        while True:
            topic, data = await self.sock.recv_multipart()
            env = decode_envelope(data)
            if env.type == "Tick":
                self.ticks.append(decode(env.payload, Tick))
            elif env.type == "MdEvent":
                self.events.append(decode(env.payload, MdEvent))
            elif env.type == "MdStatus":
                self.status = decode(env.payload, MdStatus)
            elif env.type == "Bar":
                self.bars.append(env)

    def events_of(self, kind: str, instrument_id: str | None = None) -> list[MdEvent]:
        return [e for e in self.events if str(e.kind) == kind and (instrument_id is None or e.instrument_id == instrument_id)]

    async def close(self) -> None:
        self._task.cancel()
        await asyncio.gather(self._task, return_exceptions=True)
        self.sock.close(0)
        self.ctx.term()


async def eventually(pred: Callable[[], Any], timeout: float = 10.0, step: float = 0.05, what: str = "condition") -> None:
    deadline = time.monotonic() + timeout
    while True:
        if pred():
            return
        if time.monotonic() > deadline:
            raise AssertionError(f"{what} not met within {timeout}s")
        await asyncio.sleep(step)


def assert_strictly_increasing(ticks: list[Tick]) -> None:
    """02 §5: per instrument and trading day, the order_key of md.tick is strictly increasing (no duplicates)."""
    last: dict[str, tuple[str, int, int]] = {}
    for t in ticks:
        key = (t.trading_day, t.volume, t.exch_ts_ns)
        prev = last.get(t.instrument_id)
        assert prev is None or key > prev, f"{t.instrument_id}: order_key {key} after {prev}"
        last[t.instrument_id] = key
