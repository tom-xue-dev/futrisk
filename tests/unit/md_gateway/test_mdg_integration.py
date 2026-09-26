"""md-gateway end to end with the real sim-exchange, a real runtime and a bus subscriber (01 §6.4 R4, QS-05/QS-06).

(a) normal flow publishes LIVE ticks for every simulated instrument;
(b) Feed A outage: Feed B takes over at once, Feed A resumes and replays the gap as BACKFILL only
    (never on md.tick), no duplicate md.tick, GAP_OPEN + GAP_FILLED (0 duplicates);
(c) all feeds down: STALE then UNKNOWN, exactly one MD_DOWN per instrument, MD_RECOVERED after;
(d) restart: resume from feed_cursors / watermarks, nothing old republished as LIVE;
(e) md.settle_inputs for rb2701 (full-day VWAP) and IF2612 (last hour from 14:00).
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pyarrow.compute as pc
import pytest
import zmq.asyncio

from futrisk.bus.endpoints import Endpoints
from futrisk.bus.rpc_client import AsyncRpcClient
from futrisk.common.clock import BJ, bj_to_ns
from futrisk.common.config import FullConfig
from futrisk.common.ids import uuid7
from futrisk.common.paths import HomeLayout
from futrisk.marketdata.recorder import read_shards
from futrisk.messages.enums import (
    GapStatus,
    MdQuality,
    ServiceState,
    SettleInputQuality,
    SettleMethod,
    SourceId,
    SourceState,
    TickMode,
)
from futrisk.messages.md import (
    BarsBatch,
    BarsRequest,
    MdEventBatch,
    MdEventsQuery,
    MdStatus,
    SettleInputsBatch,
    SettleInputsRequest,
    SourceControl,
    SourcesRequest,
    TickBatch,
    TicksQuery,
)
from futrisk.services.md_gateway.recording import day_shards
from futrisk.sim.exchange import SimExchange
from futrisk.sim.exchange.testing import MARKET_OPEN, offset_for

from .mdg_support import (
    SIM_INSTRUMENTS,
    UNSIMULATED,
    Collector,
    assert_strictly_increasing,
    eventually,
    fast_params,
    md_config,
    start_md,
)

pytestmark = [pytest.mark.timeout(90), pytest.mark.slow]

TD = "20260928"


def trusted(svc: object, source: SourceId) -> bool:
    return svc.trust.state(source) is SourceState.TRUSTED  # type: ignore[attr-defined]


async def rpc(cfg: FullConfig, identity: str = "api-gateway") -> AsyncRpcClient:
    return AsyncRpcClient(zmq.asyncio.Context.instance(), Endpoints(cfg.services), identity=identity)


async def test_normal_flow_and_feed_a_outage_failover(default_config: FullConfig, tmp_path: Path) -> None:
    cfg = md_config(default_config)
    offset = offset_for(MARKET_OPEN)
    async with SimExchange(cfg, HomeLayout(tmp_path / "sim"), offset) as ex:
        col = Collector(cfg.services.instances["md-gateway"].pub or "")
        rt, svc = await start_md(cfg, tmp_path / "md", offset, ex.feed_a_url, ex.feed_b_url, fast_params())
        try:
            # (a) both sources become trusted (bootstrap + probation) and every instrument gets LIVE ticks
            await eventually(lambda: trusted(svc, SourceId.FEED_A) and trusted(svc, SourceId.FEED_B), 15, what="both trusted")
            await eventually(lambda: {t.instrument_id for t in col.ticks} >= set(SIM_INSTRUMENTS), 10, what="LIVE ticks for all")
            assert all(t.source in (SourceId.FEED_A, SourceId.FEED_B) for t in col.ticks)
            await eventually(lambda: col.status is not None and all(
                i.quality == MdQuality.OK for i in col.status.instruments if i.instrument_id in SIM_INSTRUMENTS
            ), 5, what="quality OK")
            assert_strictly_increasing(col.ticks)
            await eventually(lambda: rt.state == ServiceState.RUNNING, 3, what="RUNNING")
            hb = rt.build_heartbeat()
            assert hb.latency_us["decode_to_publish"].n > 0 and hb.deps["FEED_A"] == "UP"
            assert {"recorder_rows", "open_gaps", "quarantined"} <= set(hb.queues)

            # (b) Feed A outage of ~2 s with Feed B up
            n_before = len(col.ticks)
            await ex.faults.add("FEED_DISCONNECT", "feed_a", {"refuse_reconnect_s": 2.0})
            await eventually(lambda: svc.trust.state(SourceId.FEED_A) is SourceState.EXCLUDED, 5, what="feed A excluded")
            t_excluded = asyncio.get_running_loop().time()
            await eventually(lambda: any(t.source is SourceId.FEED_B for t in col.ticks[n_before:]), 3, what="feed B takes over")
            assert asyncio.get_running_loop().time() - t_excluded < 1.5  # next poll: instantaneous fail-over
            # Feed A comes back, resumes and replays the gap
            await eventually(lambda: bool(col.events_of("GAP_FILLED")), 15, what="GAP_FILLED")
            filled = col.events_of("GAP_FILLED")[0]
            assert filled.gap is not None and filled.source is SourceId.FEED_A
            assert filled.gap.status == GapStatus.FILLED and filled.gap.filled > 0 and filled.gap.duplicates == 0
            opened = [e for e in col.events_of("GAP_OPEN") if e.gap and e.gap.gap_id == filled.gap.gap_id]
            assert opened, "GAP_OPEN precedes GAP_FILLED"
            lo, hi = filled.gap.from_seq, filled.gap.to_seq
            assert lo is not None and hi is not None
            # replayed states are BACKFILL only: none of the gap's seqs reached md.tick
            assert not [t for t in col.ticks if t.source is SourceId.FEED_A and t.source_seq is not None and lo <= t.source_seq <= hi]
            await eventually(lambda: trusted(svc, SourceId.FEED_A), 10, what="feed A trusted again")
            assert_strictly_increasing(col.ticks)
            keys = [(t.instrument_id, t.trading_day, t.volume, t.exch_ts_ns) for t in col.ticks]
            assert len(keys) == len(set(keys)), "duplicate md.tick"
            states = [(e.source, e.to_state) for e in col.events_of("SOURCE_STATE")]
            assert (SourceId.FEED_A, "EXCLUDED") in states and (SourceId.FEED_A, "PROBATION") in states
            # QS-05 follow-ups on the live connection: a seq gap (in-connection replay), duplicates, reordering
            n_gaps = len(col.events_of("GAP_FILLED"))
            await ex.faults.add("FEED_GAP", "feed_a", {"drop_count": 15})
            await ex.faults.add("FEED_DUPLICATE", "feed_a", {"ratio": 0.3}, duration_s=1.0)
            await ex.faults.add("FEED_REORDER", "feed_a", {"window": 8}, duration_s=1.0)
            await eventually(lambda: len(col.events_of("GAP_FILLED")) > n_gaps, 10, what="in-connection hole repaired")
            hole = col.events_of("GAP_FILLED")[-1]
            assert hole.gap is not None and hole.detail.get("kind") in ("hole", "tail") and hole.gap.filled >= 1
            # bad ticks never reach md.tick: one without a trade (rejected), one with a trade (spike, dropped)
            await ex.faults.add("BAD_TICK", "feed_a", {"instrument": "rb2701", "offset_pct": 0.04, "with_volume": False})
            await ex.faults.add("BAD_TICK", "feed_a", {"instrument": "hc2701", "offset_pct": 0.04, "with_volume": True})
            await eventually(lambda: bool(col.events_of("QUARANTINE_DROPPED", "hc2701")), 5, what="spike dropped")
            await eventually(lambda: bool(col.events_of("BAD_TICK", "rb2701")), 5, what="bad tick rejected")
            await asyncio.sleep(0.5)
            for iid in ("rb2701", "hc2701"):
                prices = [t.last for t in col.ticks if t.instrument_id == iid]
                steps = [abs(b - a) / a for a, b in zip(prices, prices[1:], strict=False)]
                assert max(steps) < 0.02, f"{iid}: a bad price reached md.tick"
            assert_strictly_increasing(col.ticks)
            keys = [(t.instrument_id, t.trading_day, t.volume, t.exch_ts_ns) for t in col.ticks]
            assert len(keys) == len(set(keys)), "duplicate md.tick"

            # the raw recording holds the replayed states as BACKFILL
            await svc.flush_recording()
            table = read_shards(day_shards(svc.layout.ticks_raw_dir, TD, [SourceId.FEED_A]))
            seqs = pc.cast(table["source_seq"], "int64")
            in_gap = table.filter(pc.and_(pc.greater_equal(seqs, lo), pc.less_equal(seqs, hi)))
            modes = set(pc.cast(in_gap["mode"], "string").to_pylist())
            assert in_gap.num_rows >= filled.gap.filled and modes <= {"BACKFILL", "DUPLICATE"}

            # md.events / md.sources / md.source.set over the bus
            client = await rpc(cfg)
            reply = await client.request("md-gateway", "md.events", MdEventsQuery(trading_day=TD, kinds=["GAP_FILLED"]))
            assert reply.ok, reply.message
            batch = reply.open_as(MdEventBatch)
            ids = [e.gap.gap_id for e in batch.items if e.gap]
            assert filled.gap.gap_id in ids and len(ids) == len(col.events_of("GAP_FILLED"))
            cmd = SourceControl(command_id=uuid7(), source=SourceId.FEED_B, action="EXCLUDE", reason="test", actor="ops")
            r1 = await client.request("md-gateway", "md.source.set", cmd)
            assert r1.ok and svc.trust.state(SourceId.FEED_B) is SourceState.EXCLUDED
            r2 = await client.request("md-gateway", "md.source.set", cmd)
            assert r2.ok and r2.duplicate
            status = r1.open_as(MdStatus)
            assert {s.source: s.state for s in status.sources}[SourceId.FEED_B] == SourceState.EXCLUDED
            await eventually(lambda: any(e.detail.get("actor") == "ops" for e in col.events_of("SOURCE_STATE")), 3, what="audit event")
            other = await rpc(cfg, identity="risk-engine-1")
            forbidden = await other.request("md-gateway", "md.source.set", cmd, retries=0)
            assert forbidden.code == "FORBIDDEN"
            await other.close()
            await client.close()
        finally:
            await rt.stop()
            await col.close()


async def test_all_feeds_down_stale_unknown_single_md_down_and_recovery(default_config: FullConfig, tmp_path: Path) -> None:
    cfg = md_config(default_config)
    offset = offset_for(MARKET_OPEN)
    async with SimExchange(cfg, HomeLayout(tmp_path / "sim"), offset) as ex:
        col = Collector(cfg.services.instances["md-gateway"].pub or "")
        rt, svc = await start_md(cfg, tmp_path / "md", offset, ex.feed_a_url, ex.feed_b_url, fast_params())
        try:
            await eventually(lambda: trusted(svc, SourceId.FEED_A), 15, what="bootstrap")
            await eventually(lambda: all(svc.quality.quality(i) is MdQuality.OK for i in SIM_INSTRUMENTS), 10, what="all OK")
            assert rt.state == ServiceState.RUNNING
            await ex.faults.add("FEED_DISCONNECT", "feed_a", {"refuse_reconnect_s": 3.5})
            await ex.faults.add("FEED_DISCONNECT", "feed_b", {"refuse_reconnect_s": 3.5})
            await eventually(lambda: all(svc.quality.quality(i) is MdQuality.UNKNOWN for i in SIM_INSTRUMENTS), 8, what="UNKNOWN")
            await eventually(lambda: rt.state == ServiceState.DEGRADED, 3, what="DEGRADED without a trusted source")
            await eventually(lambda: all(svc.quality.quality(i) is MdQuality.OK for i in SIM_INSTRUMENTS), 15, what="recovered")
            await eventually(lambda: rt.state == ServiceState.RUNNING, 3, what="RUNNING again")
            await asyncio.sleep(0.3)
            for iid in SIM_INSTRUMENTS:
                changes = [e.to_state for e in col.events_of("QUALITY_CHANGE", iid)]
                assert changes[-3:] == ["STALE", "UNKNOWN", "OK"], (iid, changes)
                assert len(col.events_of("MD_DOWN", iid)) == 1, iid
                assert len(col.events_of("MD_RECOVERED", iid)) == 1, iid
            assert not col.events_of("MD_DOWN", UNSIMULATED)  # configured but never observed: no events
            boot = [e for e in col.events_of("SOURCE_STATE") if e.detail.get("bootstrap")]
            assert len(boot) >= 2  # start-up and after the full outage
            assert_strictly_increasing(col.ticks)
        finally:
            await rt.stop()
            await col.close()


async def test_restart_resumes_from_cursor_and_watermarks(default_config: FullConfig, tmp_path: Path) -> None:
    cfg = md_config(default_config)
    offset = offset_for(MARKET_OPEN)
    home = tmp_path / "md"
    async with SimExchange(cfg, HomeLayout(tmp_path / "sim"), offset) as ex:
        col = Collector(cfg.services.instances["md-gateway"].pub or "")
        rt, svc = await start_md(cfg, home, offset, ex.feed_a_url, ex.feed_b_url, fast_params())
        try:
            await eventually(lambda: trusted(svc, SourceId.FEED_A), 15, what="bootstrap")
            await eventually(lambda: {t.instrument_id for t in col.ticks} >= set(SIM_INSTRUMENTS), 10, what="LIVE ticks")
            await asyncio.sleep(1.5)  # a durable cursor (raw shard on disk) exists
        finally:
            await rt.stop()
        before = list(col.ticks)
        last_before = {t.instrument_id: (t.trading_day, t.volume, t.exch_ts_ns) for t in before}
        epoch = ex.epoch
        await asyncio.sleep(1.0)  # the market moves on while md-gateway is down
        rt2, svc2 = await start_md(cfg, home, offset, ex.feed_a_url, ex.feed_b_url, fast_params())
        try:
            assert svc2.feed_a is not None
            await eventually(lambda: svc2.feed_a.tracker.epoch == epoch, 5, what="cursor restored")  # type: ignore[union-attr]
            n0 = len(before)
            await eventually(lambda: {t.instrument_id for t in col.ticks[n0:]} >= set(SIM_INSTRUMENTS), 15, what="LIVE after restart")
            after = col.ticks[n0:]
            for t in after:
                assert (t.trading_day, t.volume, t.exch_ts_ns) > last_before[t.instrument_id], t.instrument_id
            assert_strictly_increasing(col.ticks)
            resumed = [e for e in col.events_of("GAP_FILLED") if e.gap and e.gap.epoch == epoch and e.detail.get("kind") == "resume"]
            assert resumed and resumed[0].gap.duplicates == 0 and resumed[0].gap.filled > 0  # type: ignore[union-attr]
        finally:
            await rt2.stop()
            await col.close()


async def test_settle_inputs_for_rb_and_if(default_config: FullConfig, tmp_path: Path) -> None:
    cfg = md_config(default_config)
    offset = offset_for(datetime(2026, 9, 28, 13, 59, 58, 500_000, tzinfo=BJ))
    async with SimExchange(cfg, HomeLayout(tmp_path / "sim"), offset) as ex:
        rt, svc = await start_md(cfg, tmp_path / "md", offset, ex.feed_a_url, ex.feed_b_url, fast_params())
        client = await rpc(cfg)
        try:
            window_start = bj_to_ns(datetime(2026, 9, 28, 14, 0, tzinfo=BJ))
            await eventually(lambda: svc.clock.now_ns() > window_start + 2_000_000_000, 15, what="past 14:00:02")
            reply = await client.request(
                "md-gateway", "md.settle_inputs", SettleInputsRequest(trading_day=TD, instruments=["rb2701", "IF2612"]), timeout_ms=10_000
            )
            assert reply.ok, reply.message
            items = {i.instrument_id: i for i in reply.open_as(SettleInputsBatch).items}
            rb, fi = items["rb2701"], items["IF2612"]
            assert rb.method == SettleMethod.VWAP_FULL_DAY and rb.day_volume > 0 and rb.quality == SettleInputQuality.OK
            vwap = Decimal(rb.day_turnover) / (rb.day_volume * 10)
            assert abs(Decimal(rb.vwap_raw or "0") - vwap) < Decimal("0.000001")
            assert rb.version == 1 and rb.pre_settle == "3120"
            assert fi.method == SettleMethod.VWAP_LAST_HOUR and fi.window_start_ns == window_start
            assert fi.window_volume is not None and 0 < fi.window_volume <= fi.day_volume
            assert fi.quality == SettleInputQuality.APPROX  # the 14:00-15:00 window is not over yet
            assert fi.hourly
            assert (svc.layout.ticks_canonical_dir / f"{TD}.parquet").exists()
            # the 13:59 bar closed at 14:00:01 (bar_close_grace 1 s in the tests) and is queryable
            minute = bj_to_ns(datetime(2026, 9, 28, 13, 59, tzinfo=BJ))
            bars: list = []
            for _ in range(40):  # published within a second of 14:00:03
                bars_reply = await client.request("md-gateway", "md.bars", BarsRequest(instrument_id="rb2701", trading_day=TD))
                assert bars_reply.ok
                bars = bars_reply.open_as(BarsBatch).bars
                if bars:
                    break
                await asyncio.sleep(0.1)
            assert [b.minute_start_ns for b in bars][:1] == [minute] and bars[0].volume > 0 and bars[0].version >= 1
            ticks_reply = await client.request(
                "md-gateway",
                "md.ticks",
                TicksQuery(instrument_id="IF2612", trading_day=TD, from_ns=minute, to_ns=window_start + 10**10, modes=[TickMode.LIVE, TickMode.BACKFILL]),
            )
            assert ticks_reply.ok
            got = ticks_reply.open_as(TickBatch).ticks
            assert got and all(t.instrument_id == "IF2612" for t in got)
            assert [t.exch_ts_ns for t in got] == sorted(t.exch_ts_ns for t in got)
            src_reply = await client.request("md-gateway", "md.sources", SourcesRequest())
            assert {s.source for s in src_reply.open_as(MdStatus).sources} == {SourceId.FEED_A, SourceId.FEED_B}
            # daily maintenance (sys.eod DONE): checkpoint + backup + purges, while the service keeps running
            await svc.maintenance(TD)
            assert (svc.layout.backup_dir / TD / "md.db").exists()
        finally:
            await client.close()
            await rt.stop()


async def test_bootstrap_while_the_market_is_closed(default_config: FullConfig, tmp_path: Path) -> None:
    """At lunch nothing trades: the sources are still promoted (nothing to catch up with) and nothing is LIVE."""
    cfg = md_config(default_config)
    offset = offset_for(datetime(2026, 9, 28, 12, 0, tzinfo=BJ))
    async with SimExchange(cfg, HomeLayout(tmp_path / "sim"), offset) as ex:
        col = Collector(cfg.services.instances["md-gateway"].pub or "")
        rt, svc = await start_md(cfg, tmp_path / "md", offset, ex.feed_a_url, ex.feed_b_url, fast_params())
        try:
            await eventually(lambda: trusted(svc, SourceId.FEED_A), 10, what="bootstrap in a break")
            assert rt.state == ServiceState.RUNNING
            await asyncio.sleep(0.5)
            assert not col.ticks and not col.events_of("MD_DOWN")
            assert svc.pipeline.counts.get("REJECTED", 0) == 0  # out-of-session records are skipped, not rejected
        finally:
            await rt.stop()
            await col.close()
