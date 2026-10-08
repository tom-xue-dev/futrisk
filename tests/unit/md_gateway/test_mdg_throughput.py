"""Throughput regression check (M6, non-strict): Feed A frames straight into a running md-gateway.

Frames go through the whole hot path - typed JSON decode, sequence tracking, normalizer,
validator, arbiter, raw recording hand-off and the ZeroMQ publish - with the timer
loop and the recorder thread running. The bound is deliberately loose (shared CI hosts);
the measured rate is printed (``-s``) for comparison with the review numbers (~11-12k msg/s
on the development machine, up from ~4-6k).
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime
from pathlib import Path

import pytest

from futrisk.common.clock import BJ, NS_PER_MS, NS_PER_S
from futrisk.common.config import FullConfig
from futrisk.messages.enums import SourceId, SourceState, TickMode
from futrisk.sim.exchange.formats import dumps
from futrisk.sim.exchange.testing import MARKET_OPEN, offset_for

from .mdg_support import fast_params, md_config, start_md

pytestmark = [pytest.mark.timeout(120), pytest.mark.slow]

INSTR = [("rb2701", "SHFE", 3120.0, 1.0, 10), ("hc2701", "SHFE", 3260.0, 1.0, 10), ("cu2611", "SHFE", 78000.0, 10.0, 5),
         ("au2612", "SHFE", 850.0, 0.02, 1000), ("i2701", "DCE", 780.0, 0.5, 100), ("m2701", "DCE", 3000.0, 1.0, 10),
         ("IF2612", "CFFEX", 4414.8, 0.2, 300), ("T2612", "CFFEX", 108.0, 0.005, 10000)]
MIN_RATE = 1500.0  # msg/s: catches order-of-magnitude regressions only


def frames(start_seq: int, n: int, now_ns: int, vols: dict[str, int], turns: dict[str, float]) -> list[str]:
    dt = datetime.fromtimestamp(now_ns / NS_PER_S, tz=BJ)
    ad, ut, ms = dt.strftime("%Y%m%d"), dt.strftime("%H:%M:%S"), (now_ns // NS_PER_MS) % 1000
    out = []
    for k in range(n):
        iid, ex, px, tick, mult = INSTR[(start_seq + k) % len(INSTR)]
        vols[iid] += 1
        turns[iid] += px * mult
        d = {
            "InstrumentID": iid, "ExchangeID": ex, "TradingDay": "20260928", "ActionDay": ad, "UpdateTime": ut,
            "UpdateMillisec": ms, "LastPrice": px, "Volume": vols[iid], "Turnover": turns[iid], "OpenInterest": 100000.0,
            "BidPrice1": px - tick, "BidVolume1": 5 + k % 7, "AskPrice1": px, "AskVolume1": 7, "OpenPrice": px,
            "HighestPrice": px, "LowestPrice": px, "UpperLimitPrice": 1.7976931348623157e308,
            "LowerLimitPrice": 1.7976931348623157e308, "PreSettlementPrice": px, "PreClosePrice": px,
            "PreOpenInterest": 100000.0, "AveragePrice": px,
        }
        out.append(dumps({"op": "tick", "seq": start_seq + k, "replay": False, "d": d}))
    return out


async def test_ingest_rate_regression(default_config: FullConfig, tmp_path: Path) -> None:
    cfg = md_config(default_config)
    rt, svc = await start_md(cfg, tmp_path / "md", offset_for(MARKET_OPEN), "ws://127.0.0.1:9/feed", "http://127.0.0.1:9/v1/quotes",
                             fast_params())
    try:
        fa = svc.feed_a
        assert fa is not None
        await fa.stop()  # no WebSocket: frames are injected
        fa.tracker.on_subscribe_sent(1, None)
        fa.tracker.on_welcome("E1", 0, 1)
        svc.trust.on_connected(SourceId.FEED_A)
        svc.trust.sources[SourceId.FEED_A].state = SourceState.TRUSTED
        vols = {i[0]: 1000 for i in INSTR}
        turns = {i[0]: i[2] * i[4] * 1000 for i in INSTR}
        seq = 1
        for raw in frames(seq, 1000, svc.clock.now_ns() - 5 * NS_PER_MS, vols, turns):  # warm-up
            fa._on_frame(raw)  # noqa: SLF001
        seq += 1000
        total, spent = 20_000, 0.0
        while seq < total:
            batch = frames(seq, 500, svc.clock.now_ns() - 5 * NS_PER_MS, vols, turns)
            t0 = time.perf_counter()
            for raw in batch:
                fa._on_frame(raw)  # noqa: SLF001
            spent += time.perf_counter() - t0
            seq += 500
            await asyncio.sleep(0)  # timers and recorder hand-offs run as in production
        rate = (seq - 1001) / spent
        lat = svc.latency.summary()
        print(f"\nmd-gateway ingest: {rate:,.0f} msg/s on the loop thread; decode->publish {lat}")
        assert svc.pipeline.counts.get(TickMode.LIVE, 0) >= seq - 1
        assert rate > MIN_RATE
    finally:
        await rt.stop()
