"""Peer liveness across business-loop stalls (risk-engine shell review M2).

A stall longer than the peer timeout (a slow commit, maintenance) used to mark live peers DOWN
(their heartbeats were waiting on the socket) and UP again right after. Heartbeats are 100 ms here,
so the peer timeout is 300 ms: a 4 s stall is more than 13 timeouts."""

from __future__ import annotations

import asyncio
import time
from pathlib import Path

from rttest_support import SyncHarness, eventually

from futrisk.common.clock import MarketClock
from futrisk.common.config import FullConfig
from futrisk.runtime import AsyncServiceRuntime, SyncServiceRuntime

PEER = "trade-gateway"


def _peer(cfg: FullConfig, tmp_path: Path) -> SyncHarness:
    rt = SyncServiceRuntime(PEER, config=cfg, home=tmp_path / "tg", market_clock=MarketClock(), setup_logs=False)
    h = SyncHarness(rt)
    h.start()
    return h


class TestSyncRuntimeStall:
    async def test_stalls_do_not_mark_live_peers_down(self, tmp_path: Path, bus_config: FullConfig) -> None:
        peer = _peer(bus_config, tmp_path)
        rt = SyncServiceRuntime("risk-engine-1", config=bus_config, home=tmp_path / "re", market_clock=MarketClock(),
                                setup_logs=False)
        h = SyncHarness(rt)
        h.start()
        changes: list[tuple[str, str]] = []
        try:
            await h.acall(lambda: rt.watch_peers([PEER], on_change=lambda p, s: changes.append((p, s))))
            await eventually(lambda: changes == [(PEER, "UP")], timeout=5)
            await h.acall(lambda: time.sleep(4.0), timeout=15)  # the business loop is stuck for 4 s
            for _ in range(5):  # shorter stalls, each longer than the heartbeat age budget
                await h.acall(lambda: time.sleep(0.28))
            await asyncio.sleep(0.6)  # > grace + several heartbeat intervals
            assert changes == [(PEER, "UP")], changes
            assert rt.loop_stalls >= 1
            peer.stop()  # a peer that really stops is still detected
            await eventually(lambda: changes[-1:] == [(PEER, "DOWN")], timeout=5)
        finally:
            h.stop()
            peer.stop()


class TestAsyncRuntimeStall:
    async def test_event_loop_stalls_do_not_mark_live_peers_down(self, tmp_path: Path, bus_config: FullConfig) -> None:
        peer = _peer(bus_config, tmp_path)
        changes: list[tuple[str, str]] = []
        try:
            async with AsyncServiceRuntime("clearing-worker", config=bus_config, home=tmp_path / "cw",
                                           market_clock=MarketClock(), setup_logs=False) as rt:
                rt.watch_peers([PEER], on_change=lambda p, s: changes.append((p, s)))
                await eventually(lambda: changes == [(PEER, "UP")], timeout=5)
                time.sleep(1.5)  # blocks the event loop (5 peer timeouts)
                await asyncio.sleep(0.05)
                for _ in range(5):
                    time.sleep(0.28)  # shorter than the timeout, longer than a heartbeat interval
                    await asyncio.sleep(0.05)
                await asyncio.sleep(0.6)
                assert changes == [(PEER, "UP")], changes
                assert rt.loop_stalls >= 1
                peer.stop()
                await eventually(lambda: changes[-1:] == [(PEER, "DOWN")], timeout=5)
        finally:
            peer.stop()
