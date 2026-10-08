"""md-gateway fault scenarios of the review round with the real sim-exchange (01 §8.13, 02 §9, 03 §4.1).

Each test starts a sim-exchange (50 ms ticks) and a real md-gateway runtime:

- H2 coverage: a frozen trusted feed while an excluded source still shows the market moving -> STALE;
- M2: a quiet contract stays OK across a Feed A outage/resume, and after a restart;
- M4: a manual EXCLUDE survives a restart;
- SIM_RESTART (EPOCH_CHANGED) end to end; Feed A reorder beyond the 32-message buffer, duplicates and a gap;
  Feed B serving older snapshots and duplicate bodies;
- demo clock jumps into breaks raise no false alarms;
- M7: all sources lost -> ``all:{from_ns}`` gap, closed when a source is trusted again; a Feed B poll outage -> ``feed_b:`` gap;
- the Sina COARSE_TS rule through the whole service;
- config: only the arbitration parameters of feeds.yaml apply at run time; everything else needs a restart.
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
from collections.abc import AsyncIterator
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest

from futrisk.common.clock import BJ, NS_PER_S
from futrisk.common.config import FullConfig
from futrisk.common.ids import uuid7
from futrisk.common.paths import HomeLayout
from futrisk.messages.enums import GapStatus, MdQuality, SourceId, SourceState, TickMode
from futrisk.messages.md import FLAG_COARSE_TS, FLAG_WM_RESET, SourceControl, Tick
from futrisk.runtime import AsyncServiceRuntime
from futrisk.services.md_gateway.service import MdGatewayService
from futrisk.services.md_gateway.sina import SinaDecoder, resolve_sina_instruments, split_response
from futrisk.sim.exchange import SimExchange
from futrisk.sim.exchange.testing import MARKET_OPEN, offset_for

from .mdg_support import SIM_INSTRUMENTS, Collector, eventually, fast_params, md_config, start_md

pytestmark = [pytest.mark.timeout(120), pytest.mark.slow]

A, B, SINA = SourceId.FEED_A, SourceId.FEED_B, SourceId.SINA


def trusted(svc: MdGatewayService, s: SourceId) -> bool:
    return svc.trust.state(s) is SourceState.TRUSTED


def monotonic_or_reset(ticks: list[Tick]) -> list[tuple[str, Any, Any]]:
    """02 §5 violations: order_key not increasing per instrument, except on a FLAG_WM_RESET tick."""
    last: dict[str, tuple[str, int, int]] = {}
    bad = []
    for t in ticks:
        k = (t.trading_day, t.volume, t.exch_ts_ns)
        p = last.get(t.instrument_id)
        if p is not None and not k > p and not t.flags & FLAG_WM_RESET:
            bad.append((t.instrument_id, p, k))
        last[t.instrument_id] = k
    return bad


@dataclasses.dataclass
class Env:
    cfg: FullConfig
    home: Path
    offset: int
    ex: SimExchange
    col: Collector
    rt: AsyncServiceRuntime
    svc: MdGatewayService

    async def restart(self, **params: Any) -> None:
        await self.rt.stop()
        self.rt, self.svc = await start_md(self.cfg, self.home, self.offset, self.ex.feed_a_url, self.ex.feed_b_url, fast_params(**params))


@contextlib.asynccontextmanager
async def running(default_config: FullConfig, tmp_path: Path, *, start: datetime = MARKET_OPEN, both: bool = True,
                  cfg_edit: Any = None, **params: Any) -> AsyncIterator[Env]:
    cfg = md_config(default_config)
    if cfg_edit is not None:
        cfg = cfg_edit(cfg)
    offset = offset_for(start)
    async with SimExchange(cfg, HomeLayout(tmp_path / "sim"), offset) as ex:
        col = Collector(cfg.services.instances["md-gateway"].pub or "")
        rt, svc = await start_md(cfg, tmp_path / "md", offset, ex.feed_a_url, ex.feed_b_url, fast_params(**params))
        env = Env(cfg, tmp_path / "md", offset, ex, col, rt, svc)
        try:
            if both:
                await eventually(lambda: trusted(env.svc, A) and trusted(env.svc, B), 20, what="both trusted")
                await eventually(lambda: all(env.svc.quality.quality(i) is MdQuality.OK for i in SIM_INSTRUMENTS), 10, what="all OK")
            yield env
        finally:
            await env.rt.stop()
            await col.close()


async def test_frozen_trusted_feed_goes_stale_while_an_excluded_source_shows_the_market(default_config: FullConfig, tmp_path: Path) -> None:
    """H2 (S1): coverage needs the LIVE stream to be current, whatever an untrusted source shows."""
    async with running(default_config, tmp_path) as env:
        env.svc.trust.manual(B, "EXCLUDE", "test", "test")
        await env.ex.faults.add("FEED_FREEZE", "feed_a", {"instrument": "m2701"}, duration_s=8.0)
        await eventually(lambda: env.svc.quality.quality("m2701") is MdQuality.STALE, 4, what="m2701 STALE")
        await eventually(lambda: env.svc.quality.quality("m2701") is MdQuality.UNKNOWN, 5, what="m2701 UNKNOWN")
        assert env.svc.quality.quality("rb2701") is MdQuality.OK  # the other contracts are unaffected
        await eventually(lambda: bool(env.col.events_of("MD_DOWN", "m2701")), 3, what="MD_DOWN m2701")


async def test_quiet_contract_stays_covered_after_a_feed_a_resume_and_a_restart(default_config: FullConfig, tmp_path: Path) -> None:
    """M2 (S2): Feed A pushes only changes; a contract quiet across the outage must not stay UNKNOWN."""
    async with running(default_config, tmp_path) as env:
        await env.ex.faults.add("FEED_FREEZE", None, {"instrument": "IF2612"}, duration_s=40.0)  # the market is quiet
        await asyncio.sleep(1.0)
        env.svc.trust.manual(B, "EXCLUDE", "test", "test")
        await env.ex.faults.add("FEED_DISCONNECT", "feed_a", {"refuse_reconnect_s": 1.0})
        await eventually(lambda: env.svc.trust.state(A) is SourceState.EXCLUDED, 5, what="A excluded")
        await eventually(lambda: trusted(env.svc, A), 15, what="A trusted again")
        await eventually(lambda: env.svc.quality.quality("IF2612") is MdQuality.OK, 5, what="IF2612 OK again")
        assert "IF2612" in env.svc.delivered[A]
        # a restart: the snapshot of the new connection delivers the quiet contract again
        await env.restart()
        await eventually(lambda: trusted(env.svc, A), 15, what="A trusted after restart")
        await eventually(lambda: env.svc.quality.quality("IF2612") is MdQuality.OK, 5, what="IF2612 OK after restart")


async def test_manual_exclude_survives_a_restart(default_config: FullConfig, tmp_path: Path) -> None:
    """M4 (S13): the pin is persisted with the command and re-applied at start-up."""
    import zmq.asyncio

    from futrisk.bus.endpoints import Endpoints
    from futrisk.bus.rpc_client import AsyncRpcClient

    async with running(default_config, tmp_path) as env:
        client = AsyncRpcClient(zmq.asyncio.Context.instance(), Endpoints(env.cfg.services), identity="api-gateway")
        cmd = SourceControl(command_id=uuid7(), source=B, action="EXCLUDE", reason="bad vendor", actor="ops")
        reply = await client.request("md-gateway", "md.source.set", cmd)
        again = await client.request("md-gateway", "md.source.set", cmd)
        await client.close()
        assert reply.ok and again.ok and again.duplicate
        await env.restart()
        await eventually(lambda: trusted(env.svc, A), 15, what="A trusted after restart")
        await asyncio.sleep(2.0)  # longer than probation + dwell: B would be trusted again without the pin
        assert env.svc.trust.state(B) is SourceState.EXCLUDED and env.svc.trust.sources[B].manual


async def test_sim_restart_epoch_change_end_to_end(default_config: FullConfig, tmp_path: Path) -> None:
    """EPOCH_CHANGED: the new epoch's ring is backfilled (never LIVE), no order_key goes back."""
    async with running(default_config, tmp_path) as env:
        old = env.ex.epoch
        n0 = len(env.col.ticks)
        await env.ex.faults.add("SIM_RESTART")
        await eventually(lambda: env.svc.feed_a is not None and env.svc.feed_a.tracker.epoch != old, 10, what="new epoch")
        await eventually(lambda: trusted(env.svc, A), 20, what="A trusted again")
        await asyncio.sleep(1.0)
        ring = [e for e in env.col.events_of("GAP_FILLED") if e.detail.get("kind") == "ring"]
        assert ring and ring[-1].gap is not None
        lo, hi = ring[-1].gap.from_seq, ring[-1].gap.to_seq
        assert lo is not None and hi is not None
        new = [t for t in env.col.ticks[n0:] if t.source is A and t.source_seq is not None and lo <= t.source_seq <= hi]
        assert new == [] or all(t.exch_ts_ns > ring[-1].ts_ns - 10 * NS_PER_S for t in new)
        assert monotonic_or_reset(env.col.ticks) == []


async def test_wire_faults_on_both_feeds_never_duplicate_or_reorder_live(default_config: FullConfig, tmp_path: Path) -> None:
    """Feed A: reorder window 64 (> buffer 32), 50 % duplicates, a 40-message gap; then Feed B older snapshots + duplicates."""
    async with running(default_config, tmp_path) as env:
        n0 = len(env.col.ticks)
        await env.ex.faults.add("FEED_REORDER", "feed_a", {"window": 64}, duration_s=2.0)
        await env.ex.faults.add("FEED_DUPLICATE", "feed_a", {"ratio": 0.5}, duration_s=2.0)
        await env.ex.faults.add("FEED_GAP", "feed_a", {"drop_count": 40})
        await asyncio.sleep(3.5)
        opened = {e.gap.gap_id for e in env.col.events_of("GAP_OPEN") if e.gap}
        await eventually(lambda: opened <= {e.gap.gap_id for e in env.col.events if e.gap and str(e.kind) != "GAP_OPEN"}, 10,
                         what="every gap closed")
        new = env.col.ticks[n0:]
        keys = [(t.instrument_id, t.trading_day, t.volume, t.exch_ts_ns) for t in new]
        assert len(keys) == len(set(keys))
        assert trusted(env.svc, A)
        # Feed A down; Feed B serves reordered (older) and duplicate bodies
        n1 = len(env.col.ticks)
        await env.ex.faults.add("FEED_DISCONNECT", "feed_a", {"refuse_reconnect_s": 3.0})
        await env.ex.faults.add("FEED_REORDER", "feed_b", {"window": 8}, duration_s=3.0)
        await env.ex.faults.add("FEED_DUPLICATE", "feed_b", {"ratio": 0.4}, duration_s=3.0)
        await asyncio.sleep(3.5)
        assert any(t.source is B for t in env.col.ticks[n1:])
        assert monotonic_or_reset(env.col.ticks) == []


async def test_demo_clock_jumps_raise_no_false_alarms(default_config: FullConfig, tmp_path: Path) -> None:
    async with running(default_config, tmp_path) as env:
        e0 = len(env.col.events)
        version = 1
        for target, dwell in ((datetime(2026, 9, 28, 10, 14, 57, tzinfo=BJ), 4.0), (datetime(2026, 9, 28, 10, 29, 57, tzinfo=BJ), 4.0)):
            await env.ex.pause()
            off = offset_for(target)
            env.ex.set_clock(off, version)
            env.rt.market_clock.apply("DEMO", off, version)
            version += 1
            env.ex.resume()
            await asyncio.sleep(dwell)
        bad = [e for e in env.col.events[e0:] if str(e.kind) in ("MD_DOWN", "QUALITY_CHANGE") and e.to_state != "OK"]
        assert bad == []
        assert monotonic_or_reset(env.col.ticks) == []


async def test_all_sources_lost_opens_an_all_gap_closed_when_a_source_is_trusted_again(default_config: FullConfig, tmp_path: Path) -> None:
    """M7: ``all:{from_ns}`` (source null) while no source is trusted; a Feed B poll outage is ``feed_b:{from_ns}``."""
    async with running(default_config, tmp_path) as env:
        await env.ex.faults.add("FEED_DISCONNECT", "feed_a", {"refuse_reconnect_s": 2.0})
        await env.ex.faults.add("FEED_DISCONNECT", "feed_b", {"refuse_reconnect_s": 2.0})
        await eventually(lambda: bool([e for e in env.col.events_of("GAP_OPEN") if e.gap and e.gap.gap_id.startswith("all:")]), 8,
                         what="all: gap opened")
        opened = [e for e in env.col.events_of("GAP_OPEN") if e.gap and e.gap.gap_id.startswith("all:")][0]
        assert opened.gap is not None and opened.gap.source is None
        await eventually(lambda: any(e.gap and e.gap.gap_id == opened.gap.gap_id for e in env.col.events  # type: ignore[union-attr]
                                     if str(e.kind) in ("GAP_FILLED", "GAP_UNRECOVERABLE")), 20, what="all: gap closed")
        closed = [e for e in env.col.events if e.gap and e.gap.gap_id == opened.gap.gap_id and str(e.kind) != "GAP_OPEN"][0]
        assert closed.gap is not None and closed.gap.to_ns is not None
        assert closed.gap.status in (GapStatus.FILLED, GapStatus.UNRECOVERABLE)
        # Feed B's own outage (every body malformed) while Feed A stays healthy
        await eventually(lambda: trusted(env.svc, A), 15, what="A trusted")
        await eventually(lambda: env.svc.quality.quality("rb2701") is MdQuality.OK, 5, what="rb2701 OK again")

        def feed_b_gaps(kind: str) -> list[Any]:
            return [e for e in env.col.events_of(kind) if e.gap and e.gap.gap_id.startswith("feed_b:")]

        n_open, n_closed = len(feed_b_gaps("GAP_OPEN")), len(feed_b_gaps("GAP_UNRECOVERABLE"))
        await env.ex.faults.add("MALFORMED", "feed_b", {"ratio": 1.0, "mode": "body"}, duration_s=2.0)
        await eventually(lambda: len(feed_b_gaps("GAP_OPEN")) > n_open, 8, what="feed_b: gap opened")
        assert env.svc.gaps.open_window_of(None) is None  # Feed A still trusted: no all-sources gap
        await eventually(lambda: len(feed_b_gaps("GAP_UNRECOVERABLE")) > n_closed, 10, what="feed_b: gap closed")
        assert env.svc.quality.quality("rb2701") is MdQuality.OK  # Feed A covered it all along


def _sina_body(instruments: dict[str, Any], now: datetime, rows: dict[str, tuple[float, int]]) -> str:
    lines = []
    for code, meta in instruments.items():
        if meta.instrument_id not in rows:
            continue
        last, vol = rows[meta.instrument_id]
        f = ["NAME", now.strftime("%H%M%S"), f"{last:.3f}", f"{last + 5:.3f}", f"{last - 5:.3f}", f"{last:.3f}", f"{last - 1:.3f}",
             f"{last:.3f}", f"{last:.3f}", f"{last:.3f}", f"{last:.3f}", "5", "7", "1600000.000", str(vol), "SHFE", "RB",
             now.strftime("%Y-%m-%d"), "1", "", "", "", "", "", "", "", "", f"{last:.3f}"]
        lines.append(f'var hq_str_{code}="{",".join(f)}";')
    return "\n".join(lines)


async def test_sina_coarse_timestamps_only_advance_live_on_a_volume_increase(default_config: FullConfig, tmp_path: Path) -> None:
    """ADR-006 through the whole service: a same-volume Sina state is never LIVE, a volume increase is."""

    def with_sina(cfg: FullConfig) -> FullConfig:
        sina = cfg.feeds.sina.model_copy(update={"enabled": True, "url": "http://127.0.0.1:9/list=", "symbols": {"rb2701": "nf_RB2701"}})
        return dataclasses.replace(cfg, feeds=cfg.feeds.model_copy(update={"sina": sina}))

    async with running(default_config, tmp_path, cfg_edit=with_sina, both=False) as env:
        svc = env.svc
        await svc.sources[SINA].stop()  # no network: batches are injected
        await eventually(lambda: trusted(svc, A), 15, what="A trusted")
        await eventually(lambda: svc.arbiter.state("rb2701").wm is not None, 5, what="rb2701 LIVE")
        svc.trust.manual(A, "EXCLUDE", "test", "sina only")
        svc.trust.manual(B, "EXCLUDE", "test", "sina only")
        svc.trust.on_poll(SINA, True)
        svc.trust.sources[SINA].state = SourceState.TRUSTED  # the only source: as if bootstrapped
        wm = svc.arbiter.state("rb2701").wm
        latest = svc.arbiter.state("rb2701").latest
        assert wm is not None and latest is not None
        px = latest.last
        decoder = SinaDecoder(svc.normalizer, resolve_sina_instruments(env.cfg.products, env.cfg.feeds.sina.symbols))
        n0 = len(env.col.ticks)
        modes = []
        for dv in (0, 0, 7):
            now_ns = svc.clock.now_ns()
            now = datetime.fromtimestamp(now_ns / NS_PER_S, tz=BJ)
            text = _sina_body(decoder.instruments, now, {"rb2701": (px, wm[1] + dv)})
            inc = decoder.incoming("nf_RB2701", split_response(text).payloads["nf_RB2701"], now_ns, svc.clock.monotonic_ns())
            assert inc.reject_reason is None and inc.tick is not None and inc.tick.flags & FLAG_COARSE_TS
            svc.on_incoming(inc)
            modes.append(svc.pipeline.last_outcome)
            await asyncio.sleep(1.05)
        assert modes[0] != str(TickMode.LIVE) and modes[1] != str(TickMode.LIVE) and modes[2] == str(TickMode.LIVE)
        await eventually(lambda: any(t.source is SINA for t in env.col.ticks[n0:]), 3, what="Sina LIVE tick published")


async def test_config_changes_outside_arbitration_are_vetoed(default_config: FullConfig, tmp_path: Path) -> None:
    """feeds.yaml changes other than 'arbitration', products.yaml and the calendar need a restart."""
    async with running(default_config, tmp_path, both=False) as env:
        svc = env.svc
        cur = env.cfg
        slower = dataclasses.replace(cur, feeds=cur.feeds.model_copy(update={"feed_b": cur.feeds.feed_b.model_copy(update={"poll_ms": 500})}))
        errors = svc._validate_config(cur, slower)
        assert errors and "feeds.feed_b" in errors[0] and "restart required" in errors[0]
        arb = dataclasses.replace(cur, feeds=cur.feeds.model_copy(update={"arbitration": cur.feeds.arbitration.model_copy(update={"stale_s": 9.0})}))
        assert svc._validate_config(cur, arb) == []
        svc._on_config_commit(arb, {})
        assert svc.params.arb.stale_s == 9.0 and svc.validator._p.arb.stale_s == 9.0  # noqa: SLF001
        rb = cur.products.products["rb"]
        products = cur.products.model_copy(update={"products": {**cur.products.products, "rb": rb.model_copy(update={"multiplier": 20})}})
        errors = svc._validate_config(cur, dataclasses.replace(cur, products=products))
        assert errors and "products.yaml" in errors[0] and "restart required" in errors[0]
        errors = svc._validate_config(cur, dataclasses.replace(cur, files={**cur.files, "calendar.yaml": "changed"}))
        assert errors and "calendar" in errors[0]


async def test_restart_inside_the_watermark_flush_interval_republishes_nothing(default_config: FullConfig, tmp_path: Path) -> None:
    """L1: with a long watermark_flush_s the stop hook still persists the watermarks (and their fingerprints)."""
    async with running(default_config, tmp_path, watermark_flush_s=60.0) as env:
        await asyncio.sleep(1.0)
        before = list(env.col.ticks)
        last = {}
        for t in before:
            last[t.instrument_id] = (t.trading_day, t.volume, t.exch_ts_ns)
        await env.restart(watermark_flush_s=60.0)
        n0 = len(env.col.ticks)
        await eventually(lambda: trusted(env.svc, A), 15, what="A trusted after restart")
        await eventually(lambda: {t.instrument_id for t in env.col.ticks[n0:]} >= {"rb2701", "IF2612"}, 10, what="LIVE after restart")
        for t in env.col.ticks[n0:]:
            prev = last.get(t.instrument_id)
            assert prev is None or (t.trading_day, t.volume, t.exch_ts_ns) > prev, (t.instrument_id, prev)
