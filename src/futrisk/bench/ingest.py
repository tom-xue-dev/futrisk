"""``ingest``: md-gateway decode -> normalise -> validate -> arbitrate -> publish, in process (target ≥ 10k msg/s).

A real :class:`~futrisk.services.md_gateway.service.MdGatewayService` on its asyncio runtime (production
arbitration parameters, recorder thread, bar builder, timers, ZeroMQ publisher, md.db) receives synthetic
Feed A frames straight into ``FeedAWsSource._on_frame`` - the websocket client's frame handler - so the
measured path is everything after the socket: JSON decode, sequence tracking, normalisation, validation,
arbitration, raw-tick recording hand-off, 1-minute bars and the ``md.tick`` publish. Feed B and Sina are
disabled, Feed A is set TRUSTED directly (source bootstrap is not part of the ingest path).

Frames are built just in time in batches of 500 (market-time stamps < 3 s old, volumes increasing, prices
±3 ticks around the pre-settlement), the build time is excluded; between batches the loop yields once
(``asyncio.sleep(0)``) so timers and the recorder hand-off run as in production. Reported: the sustained
rate over the whole measured loop (primary), the pipeline-only rate, the per-frame processing time, and
the service's own decode -> publish latency window.
"""

from __future__ import annotations

import asyncio
import cProfile
import dataclasses
import random
from datetime import datetime
from pathlib import Path
from typing import Any

from futrisk.bench.core import BenchContext, BenchResult, Target, latency_ns, now_ns, profile_lines, scalar
from futrisk.bench.scenario import INSTRUMENTS, MARKET_OPEN, TD, free_port, with_ports
from futrisk.bench.world import bench_dir
from futrisk.common.clock import BJ, NS_PER_MS, NS_PER_S, MarketClock
from futrisk.common.config import FullConfig, load_default_config
from futrisk.common.decimals import D
from futrisk.core.instruments import InstrumentRegistry
from futrisk.messages.enums import SourceId, SourceState
from futrisk.runtime import AsyncServiceRuntime, run_async
from futrisk.sim.exchange.formats import dumps
from futrisk.sim.exchange.testing import offset_for

NAME = "ingest"
TITLE = "md-gateway ingest: Feed A frame -> decode/normalise/validate/arbitrate -> publish (in process)"
TARGET = "≥ 10,000 msg/s"
BATCH = 500


def _config(feed_a_url: str) -> FullConfig:
    base = with_ports(load_default_config())
    feeds = base.feeds.model_copy(update={
        "feed_a": base.feeds.feed_a.model_copy(update={"url": feed_a_url}),
        "feed_b": base.feeds.feed_b.model_copy(update={"enabled": False}),
        "sina": base.feeds.sina.model_copy(update={"enabled": False}),
    })
    return dataclasses.replace(base, feeds=feeds)


class _Frames:
    """Feed A ``tick`` frames (CTP field names) for the configured instruments."""

    def __init__(self, cfg: FullConfig, seed: int = 11) -> None:
        reg = InstrumentRegistry(cfg.products, cfg.calendar)
        self.rng = random.Random(seed)
        self.seq = 0
        self.items: list[tuple[str, str, float, float, int, float, float]] = []
        for inst in cfg.products.instrument_ids():
            info = reg.get(inst)
            pre = INSTRUMENTS.get(inst, 1000.0)
            up, lo = reg.limit_prices(inst, D(f"{pre:g}"), TD)
            self.items.append((inst, info.exchange.value, pre, float(info.tick_size), int(info.multiplier),
                               float(up), float(lo)))
        self.volume = {i[0]: 10_000 for i in self.items}
        self.turnover = {i[0]: i[2] * i[4] * 10_000 for i in self.items}

    def build(self, n: int, now_ns_market: int) -> list[str]:
        dt = datetime.fromtimestamp(now_ns_market / NS_PER_S, tz=BJ)
        ad, ut, ms = dt.strftime("%Y%m%d"), dt.strftime("%H:%M:%S"), (now_ns_market // NS_PER_MS) % 1000
        out = []
        for _ in range(n):
            self.seq += 1
            iid, ex, pre, tick, mult, up, lo = self.items[self.seq % len(self.items)]
            px = round(pre + tick * self.rng.randint(-3, 3), 6)
            self.volume[iid] += 1 + self.seq % 3
            self.turnover[iid] += px * mult * (1 + self.seq % 3)
            d = {"InstrumentID": iid, "ExchangeID": ex, "TradingDay": TD, "ActionDay": ad, "UpdateTime": ut,
                 "UpdateMillisec": ms, "LastPrice": px, "Volume": self.volume[iid], "Turnover": self.turnover[iid],
                 "OpenInterest": 100_000.0, "BidPrice1": px - tick, "BidVolume1": 5 + self.seq % 7, "AskPrice1": px + tick,
                 "AskVolume1": 7, "OpenPrice": pre, "HighestPrice": max(pre, px), "LowestPrice": min(pre, px),
                 "UpperLimitPrice": up, "LowerLimitPrice": lo, "PreSettlementPrice": pre, "PreClosePrice": pre,
                 "PreOpenInterest": 100_000.0, "AveragePrice": pre * mult}
            out.append(dumps({"op": "tick", "seq": self.seq, "replay": False, "d": d}))
        return out


async def _ingest(ctx: BenchContext, home: Path, n: int, warmup: int) -> dict[str, Any]:
    from futrisk.services.md_gateway.service import MdGatewayService

    cfg = _config(f"ws://127.0.0.1:{free_port()}/feed")  # nothing listens there: frames are injected
    rt = AsyncServiceRuntime("md-gateway", config=cfg, home=home, market_clock=MarketClock(mode="DEMO",
                             offset_ns=offset_for(MARKET_OPEN)), setup_logs=False)
    await rt.start()
    out: dict[str, Any] = {}
    try:
        svc = MdGatewayService(rt)
        await svc.start()
        fa = svc.feed_a
        assert fa is not None, "Feed A is disabled in feeds.yaml"
        await fa.stop()  # no websocket: the frames are handed to the frame handler directly
        fa.tracker.on_subscribe_sent(1, None)
        fa._on_frame(dumps({"op": "welcome", "epoch": "BENCH", "head_seq": 0, "ring_low": 1, "hb_ms": 1000}))
        svc.trust.sources[SourceId.FEED_A].state = SourceState.TRUSTED  # bench shortcut: skip the bootstrap
        frames = _Frames(cfg)
        clock = svc.clock
        for raw in frames.build(warmup, clock.now_ns() - 5 * NS_PER_MS):
            fa._on_frame(raw)
        await svc.flush_recording()
        await asyncio.sleep(0)
        counts0 = dict(svc.pipeline.counts)
        sent0 = rt.publisher_core.sent_lv
        per_frame: list[int] = []
        build_ns = 0
        prof = cProfile.Profile() if ctx.profile else None
        t0 = now_ns()
        done = 0
        while done < n:
            tb = now_ns()
            batch = frames.build(min(BATCH, n - done), clock.now_ns() - 5 * NS_PER_MS)
            build_ns += now_ns() - tb
            if prof is not None:
                prof.enable()
            for raw in batch:
                a = now_ns()
                fa._on_frame(raw)
                per_frame.append(now_ns() - a)
            if prof is not None:
                prof.disable()
            done += len(batch)
            await asyncio.sleep(0)  # timers, recorder hand-off, publisher: as on the real loop
        t1 = now_ns()
        await svc.flush_recording()
        t_flush = now_ns() - t1
        counts1 = dict(svc.pipeline.counts)
        out = {
            "frames": n,
            "wall_ns": t1 - t0 - build_ns,
            "build_ns": build_ns,
            "per_frame_ns": per_frame,
            "flush_ns": t_flush,
            "counts": {k: counts1.get(k, 0) - counts0.get(k, 0) for k in counts1},
            "published": rt.publisher_core.sent_lv - sent0,
            "svc_latency": svc.latency.summary(),
            "recorder_rows_in": svc.recorder.rows_in,
            "recorder_write_errors": svc.recorder.write_errors,
            "pipeline_errors": svc.pipeline_errors,
            "profile": profile_lines(prof) if prof is not None else [],
        }
    finally:
        await rt.stop()
    return out


def run(ctx: BenchContext) -> BenchResult:
    n = int(ctx.size("n", full=100_000, quick=20_000, tiny=600))
    warmup = int(ctx.size("warmup", full=3_000, quick=2_000, tiny=200))
    res = BenchResult(name=NAME, title=TITLE, target=TARGET, quick=ctx.quick,
                      params={"frames": n, "warmup_frames": warmup, "batch": BATCH, "instruments": 13,
                              "sources": "Feed A only (TRUSTED)", "params": "production (feeds.yaml)"})
    from loguru import logger

    with bench_dir(ctx, NAME) as tmp:
        ctx.say(f"ingest: {n:,} Feed A frames through a real md-gateway")
        logger.disable("futrisk")  # the service's own logs are not part of the ingest path (production: file sink,
        try:  # enqueue=True, INFO)
            out = run_async(_ingest(ctx, tmp / "home", n, warmup))
        finally:
            logger.enable("futrisk")
    wall_s = out["wall_ns"] / 1e9
    proc_s = sum(out["per_frame_ns"]) / 1e9
    live = int(out["counts"].get("LIVE", 0))
    res.add(scalar("sustained ingest rate (whole loop incl. timers / hand-offs)", n / wall_s, "msg/s", n=n,
                   targets=[Target("value", ">=", 10_000, "msg/s")], primary=True,
                   note=f"{live:,} of {n:,} frames LIVE, {out['published']:,} md.tick published"))
    res.add(scalar("pipeline-only rate (sum of frame handler times)", n / proc_s, "msg/s", n=n))
    res.add(latency_ns("frame handler time (decode -> publish, per frame)", out["per_frame_ns"]))
    lat = out["svc_latency"]
    if lat is not None:
        res.extra["md_gateway_latency_window_us"] = {"p50": lat.p50, "p99": lat.p99, "max": lat.max, "n": lat.n}
    res.extra.update({"counts": out["counts"], "published": out["published"], "final_recorder_flush_s": out["flush_ns"] / 1e9,
                      "recorder_rows_in": out["recorder_rows_in"], "recorder_write_errors": out["recorder_write_errors"],
                      "pipeline_errors": out["pipeline_errors"]})
    if live < 0.9 * n:
        res.notes.append(f"only {live:,} of {n:,} frames were classified LIVE: the measured path is not the normal one")
    res.profile = ["(md-gateway in this process, while frames are handled)", *out["profile"]] if out["profile"] else []
    return res


__all__ = ["NAME", "TARGET", "TITLE", "run"]
