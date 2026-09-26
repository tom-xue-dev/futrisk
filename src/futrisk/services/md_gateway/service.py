"""md-gateway service shell (01 §5.3.2, §6.1, §6.4, §8.13; 02 §5, §6.2; 03 §4.1, §5.1, §7, §8).

Wires the components on one asyncio loop (:class:`~futrisk.runtime.AsyncServiceRuntime`):

- **sources** (:mod:`.sources`) → **normalizer** → **validator** → **arbiter** (the
  :class:`~.pipeline.MdPipeline`) → ``md.tick.{instrument}`` (LV, LIVE only);
- **trust** manager (TRUSTED / PROBATION / EXCLUDED, bootstrap, flapping, manual pins that
  survive restarts) and **quality** monitor (OK / STALE / UNKNOWN, MD_DOWN / MD_RECOVERED /
  MD_QUIET) on a 100 ms timer. Coverage (H2) requires the LIVE stream to be *current*
  (:meth:`Arbiter.current`) and a TRUSTED source actually delivering the instrument: Feed A
  without a sequence gap older than ``stale_s`` (its delivered set survives reconnects:
  it is subscribed to ``"*"`` and pushes only changes), a polled source only with records
  of its latest successful poll that were accepted;
- ``md.status`` (LV, 1 Hz, full), ``md.event`` (FACT: ``md_events`` + outbox in one
  transaction), ``md.bar.1m.{instrument}`` (LV + ``bars_1m``);
- gaps: Feed A sequence gaps (tracker), ``all:{from_ns}`` while no TRUSTED source is left
  during market hours (closed at the next bootstrap: FILLED when Feed A replayed
  everything, else UNRECOVERABLE), ``feed_b:`` / ``sina:`` while a polled source cannot be
  polled; gaps left open by a previous run are settled one by one after the first resume;
- raw tick recording (recorder thread), watermarks with the last LIVE fingerprint (every
  ``watermark_flush_s``) and durable Feed A cursors (after the raw shard is on disk) in
  ``md.db`` (``synchronous=NORMAL``, 03 DB-03);
- commands of 02 §6.2 (:mod:`.commands`), heartbeat fields, daily maintenance on
  ``sys.eod`` ``DONE`` (03 §8);
- config (02 §7.10, §8.5): ``feeds.yaml`` arbitration parameters apply at once; any other
  ``feeds.yaml`` change is vetoed in ``prepare`` (restart required); ``products.yaml`` and the
  calendar are staged and applied at the next trading-day roll (``NEXT_TRADING_DAY``).
"""

from __future__ import annotations

import asyncio
import contextlib
import shutil
import sqlite3
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from typing import Any

from loguru import logger
from sqlalchemy import Connection

from futrisk.common.clock import NS_PER_MS, NS_PER_S, SystemClock, parse_td, td_str
from futrisk.common.config import FullConfig
from futrisk.core.instruments import InstrumentRegistry
from futrisk.db.database import Database
from futrisk.db.processed import ProcessedCommands
from futrisk.messages import topics
from futrisk.messages.base import decode, encode, type_version
from futrisk.messages.enums import (
    ConfigEffect,
    EodPhase,
    GapStatus,
    LimitLock,
    MdEventKind,
    MdQuality,
    ServiceState,
    SourceId,
    SourceState,
    TickMode,
)
from futrisk.messages.envelope import Envelope
from futrisk.messages.eod import EodPhaseEvent
from futrisk.messages.md import InstrumentStatus, MdEvent, MdStatus, SourceStatus, Tick
from futrisk.runtime.async_runtime import AsyncServiceRuntime
from futrisk.services.md_gateway import store
from futrisk.services.md_gateway.arbiter import Arbiter
from futrisk.services.md_gateway.bars import BarBuilder
from futrisk.services.md_gateway.commands import register_commands
from futrisk.services.md_gateway.events import EventLog, GapBook, GapRecord
from futrisk.services.md_gateway.model import Incoming
from futrisk.services.md_gateway.normalizer import Normalizer
from futrisk.services.md_gateway.params import MdParams
from futrisk.services.md_gateway.pipeline import HELD, SKIPPED, MdPipeline
from futrisk.services.md_gateway.quality import QualityEvent, QualityMonitor
from futrisk.services.md_gateway.recording import TickRecorder, build_canonical, reject_row
from futrisk.services.md_gateway.schema import METADATA, MIGRATIONS
from futrisk.services.md_gateway.seqtracker import Hole
from futrisk.services.md_gateway.sessions import IntWindow, SessionIndex
from futrisk.services.md_gateway.sources import (
    FeedAWsSource,
    FeedBRestSource,
    MarketDataSource,
    SinaSourceAdapter,
)
from futrisk.services.md_gateway.stats import LatencyWindow, RateCounters
from futrisk.services.md_gateway.trust import TrustChange, TrustManager
from futrisk.services.md_gateway.validator import Validator

TIMER_S = 0.05
EVAL_EVERY = 2  # trust / quality evaluation every 2 timer ticks (100 ms)
# 03 §8 retention (days)
RAW_TICK_DAYS = 30
CANONICAL_DAYS = 365
EVENT_DAYS = 90
OUTBOX_DAYS = 7
BACKUP_KEEP = 20
_NOT_DELIVERED = frozenset({str(TickMode.REJECTED), str(TickMode.QUARANTINED), SKIPPED})
_TICK_TYPE = "Tick"


class MdGatewayService:
    """The md-gateway business shell around an :class:`AsyncServiceRuntime`.

    Args:
        rt: a started runtime (config, market clock, PUB/ROUTER, databases).
        params: arbitration parameters (default: ``feeds.yaml`` + :class:`MdParams` constants).
        source_urls: override the ``feeds.yaml`` URLs (tests point them at a sim-exchange).
    """

    def __init__(
        self,
        rt: AsyncServiceRuntime,
        *,
        params: MdParams | None = None,
        source_urls: dict[SourceId, str] | None = None,
    ) -> None:
        self.rt = rt
        self._params_arg = params
        self._urls = dict(source_urls or {})
        self.clock = rt.market_clock
        self.db: Database | None = None
        self.sources: dict[SourceId, MarketDataSource] = {}
        self.feed_a: FeedAWsSource | None = None
        self.delivered: dict[SourceId, set[str]] = {}
        self.poll_ok_mono: dict[SourceId, int] = {}
        self.poll_ok_ns: dict[SourceId, int] = {}
        self.latency = LatencyWindow()
        self.counters = RateCounters()
        self._tasks: list[asyncio.Task[Any]] = []
        self._handoff: asyncio.Task[None] | None = None
        self._persisted_cursor: dict[SourceId, Any] = {}
        self._stale_gaps: list[GapRecord] = []
        self._all_gap_lossy = False
        self._ever_trusted = False
        self._all_lost_ns: int | None = None
        self._canonical_done: set[str] = set()
        self._maintained: set[str] = set()
        self._staged: tuple[FullConfig, str] | None = None
        self._ready = False
        self._stopped = False
        self._started_mono = 0
        self._topics: dict[str, str] = {}
        self._bucket: tuple[int, str] = (-1, "")
        self._tick_v = type_version(_TICK_TYPE)
        self.command_lock = asyncio.Lock()
        self.processed: ProcessedCommands | None = None
        self.pipeline_errors = 0
        self.last_query_flush_mono = 0

    # ================================================================== lifecycle
    async def start(self) -> None:
        rt = self.rt
        cfg = rt.config
        self.params = self._params_arg or MdParams.from_feeds(cfg.feeds)
        self._cfg_applied = cfg
        self.products = cfg.products
        self.calendar = cfg.calendar
        self.sessions = SessionIndex(cfg.calendar)
        self._templates = sorted({tuple(spec.sessions) for spec in cfg.products.products.values() if spec.sessions})
        self.registry = InstrumentRegistry(cfg.products, cfg.calendar)
        self.normalizer = Normalizer(cfg.products, self.sessions, future_tolerance_s=self.params.future_tolerance_s)
        self.db = await rt.open_database("md", METADATA, migrations=MIGRATIONS, synchronous="NORMAL")
        self.processed = ProcessedCommands(self.db)
        if not await self._wait_clock():
            logger.warning("md.start_aborted", reason="stopped before sys.clock was ready")
            return
        td = self.current_td()
        loaded = await self.db.run_read(lambda c: self._load(c, td))
        feeds = cfg.feeds
        priorities: dict[SourceId, int] = {}
        if feeds.feed_a.enabled:
            priorities[SourceId.FEED_A] = feeds.feed_a.priority
        if feeds.feed_b.enabled:
            priorities[SourceId.FEED_B] = feeds.feed_b.priority
        if feeds.sina.enabled:
            priorities[SourceId.SINA] = feeds.sina.priority
        self.priorities = priorities
        self.events = EventLog(self._persist_event, now_ns=self.clock.now_ns, trading_day=self.current_td)
        self.gaps = GapBook(self.events, now_ns=self.clock.now_ns)
        for g in loaded["open_gaps"]:
            self.gaps.gaps[g.gap_id] = g
            if g.info.from_seq is not None:
                self._stale_gaps.append(g)  # Feed A seq gaps: settled after the first resume (L10)
        self.trust = TrustManager(
            self.params, priorities, mono_ns=self.clock.monotonic_ns, now_ns=self.clock.now_ns, on_change=self._on_trust_change
        )
        self.trust.restore_manual({s: d for s, d in loaded["pins"].items() if s in priorities})
        self.validator = Validator(self.sessions, self.params, self.normalizer.known_meta, mono_ns=self.clock.monotonic_ns)
        self.arbiter = Arbiter(self.params, priorities)
        self.arbiter.load_watermarks(loaded["watermarks"])
        self.bars = BarBuilder(
            self.gaps.overlap,
            close_grace_ns=int(self.params.bar_close_grace_s * NS_PER_S),
            windows_of=self._windows_of,
            session_grace_ns=self.params.close_grace_ns,
        )
        self.bars.load(loaded["bars"])
        self.quality = QualityMonitor(
            self.params, self.sessions, self._sessions_of, mono_ns=self.clock.monotonic_ns, now_ns=self.clock.now_ns
        )
        for iid in cfg.products.instrument_ids():
            self.quality.ensure(iid)
        for iid in loaded["watermarks"]:
            self.quality.ensure(iid, known=True)
        self.quality.restore_down(loaded["down"])
        layout = rt.layout
        assert layout is not None
        self.layout = layout
        self.spill_dirs = [layout.home / "ticks" / "spill"]
        self.recorder = TickRecorder(
            layout.ticks_raw_dir,
            sources=list(priorities),
            wall_clock=SystemClock(),
            epoch_ms=self.clock.now_ns() // NS_PER_MS,
            roll_s=self.params.raw_roll_s,
            roll_rows=self.params.raw_roll_rows,
            spill_dirs=self.spill_dirs,
            max_pending_rows=self.params.recorder_max_pending_rows,
        )
        self.pipeline = MdPipeline(
            self.params, self.validator, self.arbiter, self.trust, self.bars, self, mono_ns=self.clock.monotonic_ns
        )
        self._persisted_cursor = dict(loaded["cursors"])
        self._build_sources(loaded["cursors"])
        register_commands(self)
        with contextlib.suppress(KeyError):
            rt.subscribe("clearing-worker", [topics.SYS_EOD], self._on_sys_eod)
        rt.heartbeat_provider(self._heartbeat)
        rt.set_heartbeat_fields(trading_day=td)
        rt.add_config_validator(self._validate_config)
        rt.on_config_commit(self._on_config_commit)
        rt.on_stop(self.stop)
        self._started_mono = self.clock.monotonic_ns()
        for src in self.sources.values():
            await src.start()
        self._tasks.append(asyncio.create_task(self._timer_loop(), name="md-timers"))
        logger.info(
            "md.started",
            sources=[str(s) for s in self.sources],
            watermarks=len(loaded["watermarks"]),
            cursors={str(k): (v.epoch, v.last_contiguous_seq) for k, v in loaded["cursors"].items()},
            manual_pins=[str(s) for s in loaded["pins"]],
        )

    async def _wait_clock(self) -> bool:
        """Wait for the market clock (``sys.clock`` snapshot, 02 §8.6) unless a stop comes first."""
        rt = self.rt
        if rt.clock_ready:
            return True
        return bool(await rt.wait_clock_ready())

    def _load(self, conn: Connection, td: str) -> dict[str, Any]:
        return {
            "cursors": store.load_feed_cursors(conn),
            "watermarks": store.load_watermarks(conn),
            "down": store.load_down_state(conn, td),
            "bars": store.load_bars(conn, td),
            "open_gaps": store.load_open_gaps(conn),
            "pins": store.load_manual_pins(conn),
        }

    def _build_sources(self, cursors: dict[SourceId, store.FeedCursor]) -> None:
        cfg = self.rt.config
        feeds = cfg.feeds
        if feeds.feed_a.enabled:
            cur = cursors.get(SourceId.FEED_A)
            cursor = (cur.epoch, cur.last_contiguous_seq or 0) if cur is not None and cur.epoch else None
            self.feed_a = FeedAWsSource(
                feeds.feed_a,
                self.params,
                self.normalizer,
                self,
                self.clock,
                url=self._urls.get(SourceId.FEED_A),
                cursor=cursor,
                cursor_ns=cur.updated_ns if cur is not None else 0,
            )
            self.sources[SourceId.FEED_A] = self.feed_a
        if feeds.feed_b.enabled:
            self.sources[SourceId.FEED_B] = FeedBRestSource(
                feeds.feed_b, self.normalizer, self, self.clock, url=self._urls.get(SourceId.FEED_B), params=self.params
            )
        if feeds.sina.enabled:
            self.sources[SourceId.SINA] = SinaSourceAdapter(feeds.sina, cfg.products, cfg.calendar, self, self.clock)
        for src in self.sources:
            self.delivered[src] = set()

    async def stop(self) -> None:
        """Stop sources and timers, then flush recording, bars, watermarks and cursors (runtime stop hook)."""
        if self._stopped:
            return
        self._stopped = True
        for src in self.sources.values():
            with contextlib.suppress(Exception):
                await src.stop()
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        if self._handoff is not None:
            await asyncio.gather(self._handoff, return_exceptions=True)
        try:
            self.pipeline.poll()
            self._flush_bars(final=True)  # L9: the incomplete current minute is not published as final
            self._flush_watermarks()
            await self.recorder.close(self._cursor_marks())
            self._flush_cursors()
            assert self.db is not None
            await self.db.run_write(lambda c: None)
        except Exception:
            logger.exception("md.stop_flush_failed")
        logger.info("md.stopped", counts={str(k): v for k, v in self.pipeline.counts.items()})

    # ================================================================== config (02 §7.10, §8.5)
    @staticmethod
    def _validate_config(current: FullConfig, new: FullConfig) -> list[str]:
        """``ctl.config.prepare``: only the arbitration parameters of ``feeds.yaml`` can change at run time (L2)."""
        changed: list[str] = []
        for name in ("feed_a", "feed_b", "sina"):
            a = getattr(current.feeds, name)
            b = getattr(new.feeds, name)
            if a != b:
                changed.extend(f"{name}.{f}" for f in type(a).model_fields if getattr(a, f) != getattr(b, f))
        if changed:
            return [f"feeds.yaml: restart required (md-gateway applies only 'arbitration' at run time): {', '.join(changed)}"]
        return []

    def _on_config_commit(self, new: FullConfig, effects: dict[str, ConfigEffect]) -> None:
        """Arbitration parameters take effect at once; products / calendar at the next trading day."""
        if new.feeds.arbitration != self.params.arb:
            self.apply_params(replace(self.params, arb=new.feeds.arbitration))
            logger.info("md.config.arbitration_applied", config_version=new.config_version)
        applied = self._cfg_applied
        cal_changed = _section_files(new, "calendar") != _section_files(applied, "calendar")
        if new.products != self.products or cal_changed:
            effective = self.sessions.next_trading_day(self.sessions.current_or_next(self.clock.now_ns())) or self.current_td()
            self._staged = (new, effective)
            logger.info("md.config.staged", config_version=new.config_version, effective_trading_day=effective)

    def _apply_staged(self) -> None:
        """Products / calendar staged by a commit, applied at their trading day (02 §7.10 NEXT_TRADING_DAY)."""
        staged = self._staged
        if staged is None or self.current_td() < staged[1]:
            return
        new, effective = staged
        self._staged = None
        self._cfg_applied = new
        self.products = new.products
        self.calendar = new.calendar
        self.sessions.reload(new.calendar)
        self.normalizer.reload(new.products)
        self.validator.clear_caches()
        self.registry = InstrumentRegistry(new.products, new.calendar)
        self._templates = sorted({tuple(spec.sessions) for spec in new.products.products.values() if spec.sessions})
        for iid in new.products.instrument_ids():
            self.quality.ensure(iid)
        logger.info("md.config.staged_applied", config_version=new.config_version, trading_day=effective)

    def apply_params(self, params: MdParams) -> None:
        """Swap the parameters of every component (they read them on each use)."""
        self.params = params
        for comp in (self.validator, self.arbiter, self.trust, self.quality, self.pipeline):
            comp._p = params  # noqa: SLF001 - components share one parameter object
        if self.feed_a is not None:
            self.feed_a.set_params(params)

    # ================================================================== helpers
    def current_td(self) -> str:
        return self.sessions.current_or_next(self.clock.now_ns())

    def _sessions_of(self, instrument_id: str) -> tuple[str, ...]:
        meta = self.normalizer.known_meta(instrument_id)
        return meta.sessions if meta is not None else ()

    def _windows_of(self, instrument_id: str, trading_day: str) -> tuple[IntWindow, ...]:
        templates = self._sessions_of(instrument_id)
        return self.sessions.int_windows(templates, trading_day) if templates else ()

    def instruments(self) -> list[str]:
        ids = set(self.products.instrument_ids()) | set(self.arbiter.instruments()) | set(self.quality.items)
        return sorted(ids)

    def _startup_done(self) -> bool:
        return self.clock.monotonic_ns() - self._started_mono >= self.params.startup_grace_ns

    # ================================================================== SourceSink
    def on_incoming(self, inc: Incoming) -> None:
        if self._stopped:
            return
        src = inc.source
        self.trust.on_message(src, mono=inc.recv_mono_ns, now=inc.recv_ns)
        counters = self.counters
        counters.inc(_IN_NAMES[src])
        pipeline = self.pipeline
        try:
            pipeline.process(inc)
        except Exception:
            # a poison record must neither stop the feed nor crash-loop the service after a replay
            self.pipeline_errors += 1
            if self.pipeline_errors <= 10 or self.pipeline_errors % 1000 == 0:
                logger.exception("md.pipeline_error", source=str(src), seq=inc.seq, errors=self.pipeline_errors)
            return
        outcome = pipeline.last_outcome
        counters.inc(_OUT_NAMES.get(outcome) or f"out.{outcome.lower()}")
        tick = inc.tick
        if tick is not None:
            iid = tick.instrument_id
            delivered = self.delivered.get(src)
            if delivered is None:
                delivered = self.delivered[src] = set()
            if outcome not in _NOT_DELIVERED and iid not in delivered:
                delivered.add(iid)
            q = self.quality.items.get(iid)
            if q is None or not q.known:
                self.quality.ensure(iid, known=True)

    def on_connected(self, source: SourceId, *, epoch: str | None = None) -> None:
        # M2：Feed A 订阅 "*" 且只推送变化的合约，重连后保留已送达的合约集合（安静的合约不会被误判为陈旧）
        self.trust.on_connected(source, epoch=epoch)
        logger.info("md.source.connected", source=str(source), epoch=epoch)

    def on_disconnected(self, source: SourceId, reason: str) -> None:
        if self._stopped:
            return  # our own shutdown, not a source failure
        self.trust.on_disconnected(source, reason)
        logger.warning("md.source.disconnected", source=str(source), reason=reason)

    def on_poll(
        self,
        source: SourceId,
        ok: bool,
        *,
        error: str | None = None,
        lag_ms: float | None = None,
        instruments: Any = (),
    ) -> None:
        if ok:
            # H2：只有本轮真正被接受的记录才算覆盖（on_incoming 填充）
            self.delivered[source] = set()
            self.poll_ok_mono[source] = self.clock.monotonic_ns()
            self.poll_ok_ns[source] = self.clock.now_ns()
            g = self.gaps.open_window_of(source)
            if g is not None:
                # 轮询源不能重放：中断期间它自己的数据无法补回（其他源可能已覆盖）
                self.gaps.close(g.gap_id, status=GapStatus.UNRECOVERABLE, detail={"kind": "poll_outage", "polls_resumed": True})
        self.trust.on_poll(source, ok, error=error, lag_ms=lag_ms)
        if not ok and not self.trust.sources[source].connected and self._market_open() and self.gaps.open_window_of(source) is None:
            now = self.clock.now_ns()
            self.gaps.open_window(
                source,
                trading_day=self.current_td(),
                from_ns=self.poll_ok_ns.get(source, now),
                detail={"kind": "poll_outage", "error": error},
            )
        self.counters.inc(f"poll.{source.lower()}.{'ok' if ok else 'fail'}")

    def on_frame(self, source: SourceId, *, lag_ms: float | None = None) -> None:
        self.trust.on_message(source, lag_ms=lag_ms, count=0)

    def on_epoch_change(self, source: SourceId, old: str | None, new: str) -> None:
        logger.warning("md.source.epoch_changed", source=str(source), old=old, new=new)
        self.pipeline.forget_source(source)  # L6: history and held states of the old epoch

    def on_gap(self, source: SourceId, hole: Hole, event: str) -> None:
        td = self.current_td()
        detail = {"kind": hole.kind, **hole.detail}
        if event == "open":
            self.gaps.open(
                hole.gap_id,
                trading_day=td,
                source=source,
                from_ns=hole.from_ns,
                to_ns=hole.to_ns,
                epoch=hole.epoch,
                from_seq=hole.lo,
                to_seq=hole.hi,
                detail=detail,
            )
        else:
            rec = self.gaps.close(
                hole.gap_id,
                status=hole.status if hole.status != GapStatus.OPEN else GapStatus.UNRECOVERABLE,
                filled=hole.filled,
                duplicates=hole.duplicates,
                unrecoverable=hole.unrecoverable,
                to_ns=hole.to_ns,
                detail=detail,
                trading_day=td,
                source=source,
                from_ns=hole.from_ns,
                epoch=hole.epoch,
                from_seq=hole.lo,
                to_seq=hole.hi,
            )
            if hole.unrecoverable:
                self._all_gap_lossy = True
            self.bars.touch(rec.info.from_ns, rec.info.to_ns or self.clock.now_ns())
            if self._stale_gaps:
                self._resolve_stale_gaps()
        self.bars.touch(hole.from_ns, hole.to_ns or self.clock.now_ns())

    def on_resume_finished(self, source: SourceId) -> None:
        if self._stale_gaps:
            self._resolve_stale_gaps()

    def _resolve_stale_gaps(self) -> None:
        """Feed A gaps left OPEN by a previous run, settled one by one once the resume is done (L10)."""
        if self.feed_a is None:
            return
        tracker = self.feed_a.tracker
        keep: list[GapRecord] = []
        for g in self._stale_gaps:
            info = g.info
            if info.status != GapStatus.OPEN or info.from_seq is None:
                continue
            lo = info.from_seq
            hi = info.to_seq if info.to_seq is not None else lo
            size = hi - lo + 1
            if tracker.epoch is not None and info.epoch != tracker.epoch:
                self.gaps.close(g.gap_id, status=GapStatus.UNRECOVERABLE, unrecoverable=size, detail={"after_restart": True, "why": "epoch changed"})
                self._all_gap_lossy = True
                continue
            if hi > tracker.contiguous:
                keep.append(g)  # still being repaired
                continue
            lost = tracker.lost_in(info.epoch or "", lo, hi)
            self.gaps.close(
                g.gap_id,
                status=GapStatus.FILLED if lost == 0 else GapStatus.UNRECOVERABLE,
                filled=size - lost,
                unrecoverable=lost,
                detail={"after_restart": True, "why": "resumed"},
            )
            self.bars.touch(info.from_ns, info.to_ns or self.clock.now_ns())
        self._stale_gaps = keep

    # ================================================================== pipeline effects
    def publish_tick(self, tick: Tick, recv_mono_ns: int) -> None:
        iid = tick.instrument_id
        topic = self._topics.get(iid)
        if topic is None:
            topic = self._topics[iid] = topics.md_tick(iid)
        pub = self.rt.publisher
        if pub is not None:
            pub.publish_raw(topic, _TICK_TYPE, self._tick_v, encode(tick), td=tick.trading_day)
        else:
            self.rt.publish(topic, tick, td=tick.trading_day)
        self.latency.add((self.clock.monotonic_ns() - recv_mono_ns) / 1000)

    def record(self, inc: Incoming, mode: TickMode, reason: str | None) -> None:
        recv = inc.recv_ns
        sec = recv // NS_PER_S
        cached = self._bucket
        if cached[0] == sec:
            bucket = cached[1]
        else:
            bucket = self.sessions.current_or_next(recv)
            self._bucket = (sec, bucket)
        tick = inc.tick
        if tick is not None:
            self.recorder.add_tick(inc.source, bucket, tick, mode, reason, inc.epoch)
        else:
            row = reject_row(
                instrument_id=inc.instrument_id or "?",
                exchange=inc.exchange,
                trading_day=bucket,
                recv_ns=recv,
                source=inc.source,
                reason=reason or "MD_REJECT_MALFORMED",
                epoch=inc.epoch,
                seq=inc.seq,
            )
            self.recorder.add(inc.source, bucket, row)

    def emit(self, kind: MdEventKind, **kw: Any) -> None:
        self.events.emit(kind, **kw)

    def _persist_event(self, ev: MdEvent, gap: GapRecord | None, actor: str | None) -> None:
        """``md_events`` (+ ``gaps``) + outbox ``md.event`` in one transaction (03 §7)."""
        snap = GapRecord(gap.info, gap.trading_day, gap.opened_ns, gap.closed_ns) if gap is not None else None
        rt = self.rt

        def job(conn: Connection) -> None:
            if snap is not None:
                store.upsert_gap(conn, snap)
            store.insert_event(conn, ev, actor=actor)
            rt.append_fact(conn, topics.MD_EVENT, ev, td=ev.trading_day, corr_id=ev.event_id)

        assert self.db is not None
        rt.submit_write(self.db, job)
        logger.info("md.event", kind=str(ev.kind), source=ev.source, instrument=ev.instrument_id, to_state=ev.to_state)

    def _on_trust_change(self, ch: TrustChange) -> None:
        if ch.flapping:
            self.events.emit(MdEventKind.MD_FLAPPING, source=ch.source, detail={"reason": ch.reason, **ch.detail})
            return
        self.events.emit(
            MdEventKind.SOURCE_STATE,
            source=ch.source,
            from_state=str(ch.from_state),
            to_state=str(ch.to_state),
            detail={"reason": ch.reason, **ch.detail},
        )
        logger.info("md.source.state", source=str(ch.source), frm=str(ch.from_state), to=str(ch.to_state), reason=ch.reason)
        if ch.from_state is SourceState.TRUSTED and not self.trust.any_trusted():
            self._all_lost_ns = ch.ts_ns or self.clock.now_ns()  # the last trusted source is gone (M7)
        if ch.to_state is SourceState.TRUSTED:
            self._ever_trusted = True
            self._all_lost_ns = None
            g = self.gaps.open_window_of(None)
            if g is not None:
                # M7：全源中断缺口在下一次自举时关闭；只有 Feed A 能按序号补回中断期间的数据
                filled = ch.source is SourceId.FEED_A and not self._all_gap_lossy
                self.gaps.close(
                    g.gap_id,
                    status=GapStatus.FILLED if filled else GapStatus.UNRECOVERABLE,
                    detail={"kind": "all_sources", "recovered_by": str(ch.source), "reason": ch.reason},
                )
                self.bars.touch(g.info.from_ns, self.clock.now_ns())
        self._update_state()

    # ================================================================== timers
    async def _timer_loop(self) -> None:
        n = 0
        last_status = 0
        last_handoff = 0
        last_wm = 0
        interval_ns = int(self.params.status_interval_s * NS_PER_S)
        handoff_ns = int(self.params.record_handoff_s * NS_PER_S)
        while True:
            await asyncio.sleep(TIMER_S)
            n += 1
            mono = self.clock.monotonic_ns()
            try:
                self.pipeline.poll()
                if n % EVAL_EVERY == 0:
                    self.trust.evaluate(caught_up=self._caught_up, complete=self._complete)
                    self.pipeline.check_holds()
                    self._check_all_gap()
                    for qe in self.quality.evaluate(self._covered, self._last_trade, self.arbiter.lag_ns):
                        self._emit_quality(qe)
                if mono - last_handoff >= handoff_ns and (self._handoff is None or self._handoff.done()):
                    last_handoff = mono
                    self._handoff = asyncio.create_task(self.recorder.handoff(self._cursor_marks()), name="md-handoff")
                if mono - last_wm >= self.params.watermark_flush_ns:
                    last_wm = mono
                    self._flush_watermarks()  # L1: feeds.yaml watermark_flush_s
                if mono - last_status >= interval_ns:
                    last_status = mono
                    self._each_second()
            except Exception as exc:  # unexpected: crash and let the supervisor restart us (01 §8.6)
                self.rt.fatal(exc, "md timers")
                return

    def _each_second(self) -> None:
        self._apply_staged()
        self.trust.update_rates(self.clock.monotonic_ns())
        self.rt.publish(topics.MD_STATUS, self.status())
        self._flush_cursors()
        self._flush_bars()
        self._update_state()
        self.rt.set_heartbeat_fields(trading_day=self.current_td())
        self._maybe_build_canonical()

    def _emit_quality(self, qe: QualityEvent) -> None:
        self.events.emit(
            qe.kind,
            instrument_id=qe.instrument_id,
            from_state=qe.from_state,
            to_state=qe.to_state,
            detail=qe.detail,
        )

    def _check_all_gap(self) -> None:
        """M7: ``all:{from_ns}`` gap while no TRUSTED source is left during market hours.

        Opened as soon as the last trusted source is lost (from that moment), or - when none has
        been trusted since the start - once the start-up grace is over.
        """
        if self.trust.any_trusted() or not self._market_open():
            return
        if not self._ever_trusted and not self._startup_done():
            return
        if self.gaps.open_window_of(None) is not None:
            return
        self._all_gap_lossy = False
        now = self.clock.now_ns()
        since = self._all_lost_ns or now
        self.gaps.open_window(None, trading_day=self.current_td(), from_ns=since, detail={"kind": "all_sources", "why": "no trusted source"})
        self.bars.touch(since, now)

    def _covered(self, instrument_id: str) -> bool:
        """H2: the LIVE stream is current and a TRUSTED, connected, timely source is delivering the instrument."""
        if not self.arbiter.current(instrument_id):
            return False
        now = self.clock.monotonic_ns()
        p = self.params
        max_lag_ms = p.arb.live_max_latency_ms
        for src in self.trust.trusted_sources():
            st = self.trust.sources[src]
            if not st.connected or instrument_id not in self.delivered.get(src, ()):
                continue
            if st.lag_ms is not None and st.lag_ms > max_lag_ms:
                continue
            if src is SourceId.FEED_A:
                if self.feed_a is not None:
                    oldest = self.feed_a.tracker.oldest_open_hole_mono()
                    if oldest is not None and now - oldest > p.stale_ns:
                        continue  # 序号缺口超过 stale_s 仍未补齐：Feed A 不算覆盖
            else:
                ok_at = self.poll_ok_mono.get(src)
                window = max(3 * self._poll_ms(src) * NS_PER_MS, p.live_max_latency_ns)
                if ok_at is None or now - ok_at > window:
                    continue
            return True
        return False

    def _poll_ms(self, src: SourceId) -> int:
        feeds = self.rt.config.feeds
        return feeds.feed_b.poll_ms if src is SourceId.FEED_B else feeds.sina.poll_ms

    def _last_trade(self, instrument_id: str) -> int | None:
        st = self.arbiter.get(instrument_id)
        return st.last_trade_mono if st is not None else None

    def _caught_up(self, source: SourceId) -> bool:
        if source is SourceId.FEED_A:
            window, lag = 2 * NS_PER_S, NS_PER_S
        else:
            window = 3 * self._poll_ms(source) * NS_PER_MS
            lag = 2 * self._poll_ms(source) * NS_PER_MS
        if self.arbiter.caught_up(source, self.clock.monotonic_ns(), window, lag):
            return True
        # market closed: nothing trades, so there is no watermark to catch up with
        return not self._market_open()

    def _market_open(self) -> bool:
        now = self.clock.now_ns()
        return any(self.sessions.open_now(templates, now) is not None for templates in self._templates)

    def _complete(self, source: SourceId) -> bool:
        if source is SourceId.FEED_A and self.feed_a is not None:
            return self.feed_a.tracker.complete
        return True

    def _update_state(self) -> None:
        if self.rt.is_stopping:
            return
        if self.trust.any_trusted() or not self._market_open():
            self._ready = True
            self.rt.set_state(ServiceState.RUNNING)
        elif self._ready:
            self.rt.set_state(ServiceState.DEGRADED)

    # ---------------------------------------------------------------- persistence
    def _cursor_marks(self) -> dict[SourceId, Any]:
        marks: dict[SourceId, Any] = {}
        if self.feed_a is not None and self.feed_a.tracker.epoch is not None:
            contiguous = self.feed_a.tracker.contiguous
            held = self.validator.min_held_seq(SourceId.FEED_A)
            if held is not None:
                contiguous = min(contiguous, held - 1)
            marks[SourceId.FEED_A] = (self.feed_a.tracker.epoch, contiguous)
        for src in self.sources:
            if src is not SourceId.FEED_A and src in self.pipeline.last_exch_ns:
                marks[src] = (None, self.pipeline.last_exch_ns[src])
        return marks

    def _flush_cursors(self) -> None:
        """Persist durable cursors (written raw shards only, 03 §4.1 feed_cursors.updated_ns)."""
        changed: list[store.FeedCursor] = []
        now = self.clock.now_ns()
        td = self.current_td()
        for src, mark in list(self.recorder.durable.items()):
            prev = self._persisted_cursor.get(src)
            epoch, value = mark
            if src is SourceId.FEED_A:
                cur = store.FeedCursor(src, epoch, value, None, td, now)
                if prev is not None and prev.epoch == epoch and prev.last_contiguous_seq == value:
                    continue
            else:
                cur = store.FeedCursor(src, None, None, value, td, now)
                if prev is not None and prev.last_exch_ts_ns == value:
                    continue
            changed.append(cur)
            self._persisted_cursor[src] = cur
        if changed and self.db is not None and self.db.is_open:

            def save(conn: Connection) -> None:
                for cur in changed:
                    store.save_feed_cursor(conn, cur)

            self.rt.submit_write(self.db, save)

    def _flush_watermarks(self) -> None:
        dirty = self.arbiter.dirty_watermarks()
        if dirty and self.db is not None and self.db.is_open:
            now = self.clock.now_ns()
            self.rt.submit_write(self.db, lambda c: store.save_watermarks(c, dirty, now))

    def _flush_bars(self, *, force: bool = False, final: bool = False) -> None:
        bars = self.bars.due(self.clock.now_ns(), force=force, final=final)
        if not bars:
            return
        for b in bars:
            if self.rt.publisher is not None:
                self.rt.publish(topics.md_bar_1m(b.instrument_id), b, td=b.trading_day)
        if self.db is not None and self.db.is_open:
            now = self.clock.now_ns()
            self.rt.submit_write(self.db, lambda c: store.upsert_bars(c, bars, now))

    async def flush_recording(self) -> None:
        """Write every buffered raw row to shards now (``md.settle_inputs``, ``md.ticks``)."""
        if self._handoff is not None:
            await asyncio.gather(self._handoff, return_exceptions=True)
        await self.recorder.handoff(self._cursor_marks(), flush=True)

    def _day_close_ns(self, td: str) -> int | None:
        closes = [self.sessions.day_close_ns(tuple(spec.sessions), td) for spec in self.products.products.values()]
        vals = [c for c in closes if c is not None]
        return max(vals) if vals else None

    def _maybe_build_canonical(self) -> None:
        """Build the canonical tick file of a trading day shortly after its last close (03 §5.1 日终合并)."""
        td = self.sessions.trading_day(self.clock.now_ns())
        candidates = [td] if td else []
        prev_td = self._prev_td(self.current_td())
        if prev_td:
            candidates.append(prev_td)
        now = self.clock.now_ns()
        for day in candidates:
            if day in self._canonical_done:
                continue
            close = self._day_close_ns(day)
            if close is None or now < close + int(self.params.canonical_build_delay_s * NS_PER_S):
                continue
            self._canonical_done.add(day)
            self._tasks.append(asyncio.create_task(self.build_canonical(day), name=f"md-canonical-{day}"))

    def _prev_td(self, td: str) -> str | None:
        try:
            return td_str(self.calendar.prev_trading_day(parse_td(td)))
        except LookupError:
            return None

    async def build_canonical(self, td: str) -> Path:
        await self.flush_recording()
        layout = self.layout
        sources = list(self.priorities)
        prio = {str(s): p for s, p in self.priorities.items()}
        nxt = self.sessions.next_trading_day(td)
        spill = list(self.spill_dirs)
        return await self.recorder.run(
            lambda: build_canonical(layout.ticks_raw_dir, layout.ticks_canonical_dir, td, sources, prio, next_day=nxt, spill_dirs=spill)
        )

    # ================================================================== status
    def status(self) -> MdStatus:
        now_ns = self.clock.now_ns()
        sources: list[SourceStatus] = []
        for src, st in self.trust.sources.items():
            epoch = last_seq = None
            if src is SourceId.FEED_A and self.feed_a is not None:
                epoch = self.feed_a.tracker.epoch
                last_seq = self.feed_a.tracker.frontier - 1
            sources.append(
                SourceStatus(
                    source=src,
                    state=st.state,
                    since_ns=st.since_ns,
                    connected=st.connected,
                    epoch=epoch,
                    last_seq=last_seq,
                    last_msg_ns=st.last_msg_ns,
                    lag_ms=round(st.lag_ms, 3) if st.lag_ms is not None else None,
                    reject_rate=round(st.reject_rate(), 4),
                    msgs_per_s=float(st.msgs_per_s),
                    integrity_votes=st.integrity_votes_total,
                )
            )
        instruments: list[InstrumentStatus] = []
        for iid in self.instruments():
            q = self.quality.items.get(iid)
            a = self.arbiter.get(iid)
            instruments.append(
                InstrumentStatus(
                    instrument_id=iid,
                    quality=q.quality if q is not None else MdQuality.UNKNOWN,
                    live_source=a.wm_source if a is not None else None,
                    wm_volume=a.wm[1] if a is not None and a.wm is not None else 0,
                    wm_exch_ts_ns=a.wm[2] if a is not None and a.wm is not None else 0,
                    last_live_ns=a.last_live_ns if a is not None else None,
                    quiet=q.quiet if q is not None else False,
                    limit_lock=a.limit_lock if a is not None else LimitLock.NONE,
                )
            )
        return MdStatus(as_of_ns=now_ns, trading_day=self.current_td(), sources=sources, instruments=instruments)

    def _heartbeat(self) -> dict[str, Any]:
        mono = self.clock.monotonic_ns()
        latency = self.latency.summary()
        deps = {str(src): ("UP" if st.connected else "DOWN") for src, st in self.trust.sources.items()}
        holds = sum(1 for iid in self.arbiter.instruments() if (a := self.arbiter.get(iid)) is not None and a.hold is not None)
        queues = {
            "recorder_rows": self.recorder.buffered_rows(),
            "feed_a_held": len(self.feed_a.tracker._held) if self.feed_a is not None else 0,  # noqa: SLF001
            "open_gaps": len(self.gaps.open_gaps()),
            "quarantined": self.validator.held_count(),
            "integrity_holds": holds,
        }
        return {
            "trading_day": self.current_td(),
            "queues": queues,
            "rates": self.counters.rates(mono),
            "latency_us": {"decode_to_publish": latency} if latency is not None else {},
            "deps": deps,
            "detail": {
                "sources": {str(s): str(st.state) for s, st in self.trust.sources.items()},
                "counts": {str(k): v for k, v in self.pipeline.counts.items()},
                "events": dict(self.events.counts),
                "recorder_write_errors": self.recorder.write_errors,
                "recorder_dropped_rows": self.recorder.dropped_rows,
                "pipeline_errors": self.pipeline_errors,
                "evictions": self.validator.evictions,
                "wm_rollbacks": sum(a.rollbacks for iid in self.arbiter.instruments() if (a := self.arbiter.get(iid)) is not None),
            },
        }

    # ================================================================== maintenance
    def _on_sys_eod(self, env: Envelope) -> None:
        if env.type != "EodPhaseEvent":
            return
        ev = decode(env.payload, EodPhaseEvent)
        if ev.phase != EodPhase.DONE or ev.trading_day in self._maintained:
            return
        self._maintained.add(ev.trading_day)
        self._tasks.append(asyncio.create_task(self.maintenance(ev.trading_day), name="md-maintenance"))

    async def maintenance(self, trading_day: str) -> None:
        """03 §8 after ``sys.eod`` DONE: checkpoint, backup (VACUUM INTO), retention purges."""
        assert self.db is not None
        db = self.db
        loop = asyncio.get_running_loop()
        try:
            backup = self.layout.backup_dir / trading_day / "md.db"
            await loop.run_in_executor(None, _checkpoint_and_backup, db.path, backup)
            cutoff_events = td_str(parse_td(trading_day) - timedelta(days=EVENT_DAYS))
            while await db.run_write(lambda c: store.purge_events_before(c, cutoff_events)) > 0:
                await asyncio.sleep(0)
            outbox = self.rt.outbox
            if outbox is not None:
                # bus README: outbox retention uses the wall clock (published_ns)
                while await db.run_write(lambda c: outbox.purge_older_than(c, days=OUTBOX_DAYS)) > 0:
                    await asyncio.sleep(0)
            prev = self._prev_td(trading_day) or trading_day
            processed = self.processed
            if processed is not None:
                await db.run_write(lambda c: processed.purge_before(c, prev))
            await loop.run_in_executor(None, _purge_files, self.layout, trading_day)
            logger.info("md.maintenance.done", trading_day=trading_day)
        except Exception:
            logger.exception("md.maintenance.failed", trading_day=trading_day)


_IN_NAMES = {s: f"in.{s.lower()}" for s in SourceId}
_OUT_NAMES = {str(m): f"out.{str(m).lower()}" for m in TickMode} | {HELD: "out.held", SKIPPED: "out.skipped"}


def _section_files(cfg: FullConfig, section: str) -> dict[str, str]:
    prefix = section + "/"
    return {k: v for k, v in cfg.files.items() if k.startswith(prefix) or k == f"{section}.yaml"}


def _checkpoint_and_backup(db_path: Path, backup: Path) -> None:
    """Checkpoint the WAL and ``VACUUM INTO`` a temp file, then rename (03 §8 维护 1-2)."""
    conn = sqlite3.connect(str(db_path), timeout=10)
    try:
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        backup.parent.mkdir(parents=True, exist_ok=True)
        tmp = backup.with_name(backup.name + ".tmp")
        if tmp.exists():
            tmp.unlink()
        conn.execute("VACUUM INTO ?", (str(tmp),))
    finally:
        conn.close()
    tmp.replace(backup)


def _purge_files(layout: Any, trading_day: str) -> None:
    """Retention of raw (30 d) / canonical (365 d) tick files and backups (20 trading days), 03 §8."""
    day = parse_td(trading_day)
    raw_cut = td_str(day - timedelta(days=RAW_TICK_DAYS))
    canon_cut = td_str(day - timedelta(days=CANONICAL_DAYS))
    raw_dir = Path(layout.ticks_raw_dir)
    if raw_dir.is_dir():
        for d in raw_dir.iterdir():
            if d.is_dir() and d.name.isdigit() and len(d.name) == 8 and d.name < raw_cut:
                shutil.rmtree(d, ignore_errors=True)
    canon_dir = Path(layout.ticks_canonical_dir)
    if canon_dir.is_dir():
        for f in canon_dir.glob("*.parquet"):
            if f.stem.isdigit() and f.stem < canon_cut:
                with contextlib.suppress(OSError):
                    f.unlink()
    backup_dir = Path(layout.backup_dir)
    if backup_dir.is_dir():
        days = sorted(d for d in backup_dir.iterdir() if d.is_dir() and d.name.isdigit())
        for d in days[: max(0, len(days) - BACKUP_KEEP)]:
            md = d / "md.db"
            with contextlib.suppress(OSError):
                if md.exists():
                    md.unlink()


__all__ = ["MdGatewayService"]
