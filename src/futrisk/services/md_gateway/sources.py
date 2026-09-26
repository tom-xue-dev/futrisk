"""Market-data sources behind the ``MarketDataSource`` interface (02 §10, §9.1-§9.3).

========================  =====================================================================
source                    behaviour
========================  =====================================================================
:class:`FeedAWsSource`    WebSocket client of Feed A: ``subscribe`` with
                          ``resume {epoch, from_seq = contiguous + 1}``, sequence tracking
                          (:class:`SeqTracker`: duplicates, reorder buffer, in-connection hole
                          replay, tail gaps, EPOCH_CHANGED / SEQ_EVICTED / BAD_RESUME), missed
                          heartbeats -> disconnect (any frame counts as alive), reconnect with
                          back-off + jitter (01 §8.6). Frames are decoded into typed structs
                          (:mod:`.wire`, fast path) with a ``dict`` fallback for anything that
                          does not fit; the incoming queue is bounded (TCP back-pressure)
:class:`FeedBRestSource`  fixed-rate poller of Feed B: ``poll_ms``, connect 1 s / read 1.5 s
                          timeouts (02 §9.2), never two polls at once (a slow poll skips the
                          missed slots), failed polls counted by the trust manager
:class:`SinaSourceAdapter` the Sina ``SinaSource`` of :mod:`futrisk.marketdata` (only when
                          ``feeds.sina.enabled``)
========================  =====================================================================

Every source turns records into :class:`Incoming` (normalized ``Tick`` or a reject with
its reason) and reports connection / poll outcomes to a :class:`SourceSink`.
"""

from __future__ import annotations

import asyncio
import contextlib
import random
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, Protocol

import httpx
import msgspec
from loguru import logger
from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed, ConnectionClosedOK, InvalidHandshake, InvalidURI

from futrisk.common.calendar import TradingCalendar
from futrisk.common.clock import NS_PER_MS, Clock
from futrisk.common.config.feeds import FeedAConfig, FeedBConfig, SinaConfig
from futrisk.common.config.products import ProductsConfig
from futrisk.marketdata.poller import SinaBatch, SinaPoller, SinaSource
from futrisk.marketdata.sina import SinaNormalizer, resolve_sina_instruments
from futrisk.messages.codes import MD_REJECT_MALFORMED
from futrisk.messages.enums import SourceId
from futrisk.services.md_gateway.model import Incoming
from futrisk.services.md_gateway.normalizer import NormalizeError, Normalizer
from futrisk.services.md_gateway.params import MdParams
from futrisk.services.md_gateway.seqtracker import Hole, SeqTracker
from futrisk.services.md_gateway.wire import (
    CtpTick,
    ErrorFrame,
    HeartbeatFrame,
    ReplayBeginFrame,
    ReplayEndFrame,
    ResumeRejectedFrame,
    SnapshotFrame,
    TickFrame,
    WelcomeFrame,
    decode_frame,
)

_json_decode = msgspec.json.Decoder().decode
_json_encode = msgspec.json.Encoder().encode
_FEED_A = SourceId.FEED_A
#: CTP's DBL_MAX ("no value") as JSON text; msgspec parses it about 50x slower than an ordinary number,
#: so it is turned into ``null`` (the same meaning, 02 §9.1) before decoding
_DBL_MAX_TEXT = (b"1.7976931348623157e308", b"1.7976931348623157e+308", b"1.7976931348623157E+308")


@dataclass(frozen=True, slots=True)
class SourceCapabilities:
    """``MarketDataSource.capabilities`` (02 §10)."""

    has_seq: bool
    can_replay: bool
    coarse_ts: bool


class SourceSink(Protocol):
    """Callbacks from the sources to the service (event-loop thread)."""

    def on_incoming(self, inc: Incoming) -> None: ...

    def on_connected(self, source: SourceId, *, epoch: str | None = None) -> None: ...

    def on_disconnected(self, source: SourceId, reason: str) -> None: ...

    def on_poll(
        self,
        source: SourceId,
        ok: bool,
        *,
        error: str | None = None,
        lag_ms: float | None = None,
        instruments: Iterable[str] = (),
    ) -> None: ...

    def on_frame(self, source: SourceId, *, lag_ms: float | None = None) -> None: ...

    def on_gap(self, source: SourceId, hole: Hole, event: str) -> None: ...

    def on_epoch_change(self, source: SourceId, old: str | None, new: str) -> None: ...

    def on_resume_finished(self, source: SourceId) -> None: ...


class MarketDataSource(Protocol):
    """02 §10: ``capabilities``, ``start(sink)``, ``stop()``, optional ``request_replay``."""

    source_id: SourceId
    capabilities: SourceCapabilities

    async def start(self) -> None: ...

    async def stop(self) -> None: ...


# =============================================================================
# Feed A
# =============================================================================
class FeedAWsSource:
    """Feed A WebSocket client (02 §9.1); see the module docstring."""

    source_id = SourceId.FEED_A
    capabilities = SourceCapabilities(has_seq=True, can_replay=True, coarse_ts=False)

    def __init__(
        self,
        cfg: FeedAConfig,
        params: MdParams,
        normalizer: Normalizer,
        sink: SourceSink,
        clock: Clock,
        *,
        url: str | None = None,
        cursor: tuple[str | None, int] | None = None,
        cursor_ns: int = 0,
    ) -> None:
        self._cfg = cfg
        self._p = params
        self._normalizer = normalizer
        self._sink = sink
        self._clock = clock
        self.url = url or cfg.url
        self.tracker = SeqTracker(
            self,
            clock,
            reorder_max=params.arb.reorder_buffer_max,
            reorder_ns=params.reorder_ns,
            replay_timeout_ns=int(params.replay_timeout_s * 1e9),
            max_attempts=params.replay_max_attempts,
            tail_heartbeats=params.tail_gap_heartbeats,
        )
        if cursor is not None and cursor[0]:
            self.tracker.restore(cursor[0], cursor[1], cursor_ns)
        self._task: asyncio.Task[None] | None = None
        self._ws: ClientConnection | None = None
        self._out: asyncio.Queue[bytes] | None = None
        self._req = 0
        self._hb_ms = 1000
        self._last_frame_mono = 0
        self._welcomed = False
        self._subscribed = False
        self._close_reason: str | None = None
        self._stopping = False
        self.connects = 0
        self.frames = 0
        self.slow_frames = 0

    # ------------------------------------------------------------------ API
    @property
    def connected(self) -> bool:
        return self._welcomed

    def set_params(self, params: MdParams) -> None:
        """New arbitration parameters (config hot reload): heartbeat misses and reorder buffer."""
        self._p = params
        self.tracker.configure(reorder_max=params.arb.reorder_buffer_max, reorder_ns=params.reorder_ns)

    async def start(self) -> None:
        if self._task is None:
            self._stopping = False
            self._task = asyncio.create_task(self._run(), name="md-feed-a")

    async def stop(self) -> None:
        self._stopping = True
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._task
            self._task = None

    def request_replay(self, lo: int, hi: int) -> int | None:
        """In-connection ``replay {from_seq, to_seq}`` (TrackerSink / MarketDataSource)."""
        if self._out is None or not self._welcomed:
            return None
        self._req += 1
        self._out.put_nowait(_json_encode({"op": "replay", "req": self._req, "from_seq": lo, "to_seq": hi}))
        return self._req

    # ------------------------------------------------------------ TrackerSink
    def deliver(self, seq: int, replay: bool, payload: Any) -> None:
        d, recv_ns, mono = payload
        self._sink.on_incoming(self._incoming(d, recv_ns, mono, seq=seq, replay=replay, snapshot=False))

    def gap_event(self, hole: Hole, event: str) -> None:
        self._sink.on_gap(self.source_id, hole, event)

    def resume_finished(self) -> None:
        self._sink.on_resume_finished(self.source_id)

    def reconnect(self, reason: str) -> None:
        self._close_reason = reason
        ws = self._ws
        if ws is not None:
            asyncio.ensure_future(ws.close(code=1000, reason=reason[:100]))

    # -------------------------------------------------------------- internals
    def _incoming(self, d: Any, recv_ns: int, mono: int, *, seq: int | None, replay: bool, snapshot: bool) -> Incoming:
        epoch = self.tracker.epoch
        try:
            if type(d) is CtpTick:
                tick = self._normalizer.feed_a_struct(d, recv_ns=recv_ns, seq=seq)
            elif isinstance(d, dict):
                tick = self._normalizer.feed_a(d, recv_ns=recv_ns, seq=seq)
            else:
                return Incoming(_FEED_A, recv_ns, mono, reject_reason=MD_REJECT_MALFORMED, detail="tick is not an object", seq=seq, replay=replay, snapshot=snapshot, epoch=epoch)
        except NormalizeError as exc:
            raw_id = d.InstrumentID if type(d) is CtpTick else str(d.get("InstrumentID") or "")
            return Incoming(
                _FEED_A,
                recv_ns,
                mono,
                tick=exc.tick,
                reject_reason=exc.reason,
                detail=exc.detail,
                seq=seq,
                replay=replay,
                snapshot=snapshot,
                epoch=epoch,
                instrument_id=exc.instrument_id or raw_id,
                exchange=exc.exchange,
            )
        return Incoming(_FEED_A, recv_ns, mono, tick, None, "", seq, replay, snapshot, epoch)

    async def _run(self) -> None:
        attempt = 0
        backoff = list(self._cfg.reconnect_backoff_ms) or [500]
        rng = random.Random()
        while not self._stopping:
            reason = "closed"
            try:
                async with connect(
                    self.url,
                    proxy=None,
                    compression=None,
                    open_timeout=5,
                    ping_interval=None,
                    close_timeout=1,
                    max_size=None,
                    max_queue=max(16, self._p.ws_max_queue),
                ) as ws:
                    self._ws = ws
                    self.connects += 1
                    await self._session(ws)
                    reason = self._close_reason or "connection closed"
            except asyncio.CancelledError:
                raise
            except (OSError, TimeoutError, InvalidHandshake, InvalidURI, ConnectionClosed) as exc:
                reason = self._close_reason or f"{type(exc).__name__}: {exc}"[:200]
            except Exception as exc:  # a bug in the handling must not kill the reconnect loop
                logger.exception("md.feed_a.session_failed")
                reason = f"internal: {exc!r}"[:200]
            finally:
                self._ws = None
                self._out = None
                if self._subscribed:
                    self._subscribed = False
                    self.tracker.on_disconnect()
                if self._welcomed:
                    self._welcomed = False
                    attempt = 0
                    self._sink.on_disconnected(self.source_id, reason)
            if self._stopping:
                break
            delay_ms = backoff[min(attempt, len(backoff) - 1)] * (0.8 + 0.4 * rng.random())
            attempt += 1
            logger.info("md.feed_a.reconnect", in_ms=int(delay_ms), reason=reason)
            await asyncio.sleep(delay_ms / 1000)

    async def _session(self, ws: ClientConnection) -> None:
        self._close_reason = None
        out: asyncio.Queue[bytes] = asyncio.Queue()
        self._out = out
        self._req += 1
        req = self._req
        resume = self.tracker.resume_request()
        await ws.send(_json_encode({"op": "subscribe", "req": req, "instruments": "*", "resume": resume}).decode())
        self.tracker.on_subscribe_sent(req, resume)
        self._subscribed = True
        self._last_frame_mono = self._clock.monotonic_ns()
        sender = asyncio.create_task(self._sender(ws, out), name="md-feed-a-send")
        watchdog = asyncio.create_task(self._watchdog(ws), name="md-feed-a-watchdog")
        try:
            while True:
                try:
                    raw = await ws.recv(decode=False)
                except ConnectionClosedOK:
                    return
                self._on_frame(raw)
        finally:
            for t in (sender, watchdog):
                t.cancel()
            await asyncio.gather(sender, watchdog, return_exceptions=True)

    async def _sender(self, ws: ClientConnection, out: asyncio.Queue[bytes]) -> None:
        while True:
            data = await out.get()
            await ws.send(data.decode())

    async def _watchdog(self, ws: ClientConnection) -> None:
        """Missed heartbeats -> disconnect (01 §8.13); any frame counts as alive (02 §9.1)."""
        while True:
            await asyncio.sleep(0.05)
            self.tracker.poll()
            silent_ns = self._clock.monotonic_ns() - self._last_frame_mono
            limit_ns = self._p.arb.exclude_missed_heartbeats * self._hb_ms * NS_PER_MS
            if silent_ns > limit_ns:
                self._close_reason = f"no frame for {silent_ns / 1e6:.0f} ms ({self._p.arb.exclude_missed_heartbeats} heartbeats missed)"
                logger.warning("md.feed_a.heartbeat_lost", silent_ms=int(silent_ns / 1e6))
                await ws.close(code=1000, reason="heartbeat lost")
                return

    def _on_frame(self, raw: str | bytes) -> None:
        clock = self._clock
        mono = clock.monotonic_ns()
        now = clock.now_ns()
        self._last_frame_mono = mono
        self.frames += 1
        data = raw if isinstance(raw, bytes) else raw.encode()
        if b"e308" in data or b"E+308" in data or b"e+308" in data:
            for text in _DBL_MAX_TEXT:
                data = data.replace(text, b"null")
        try:
            msg = decode_frame(data)
        except msgspec.DecodeError:
            self._on_frame_slow(data, now, mono)
            return
        tp = type(msg)
        if tp is TickFrame:
            assert isinstance(msg, TickFrame)
            self.tracker.on_tick(msg.seq, msg.replay, (msg.d, now, mono))
        elif tp is HeartbeatFrame:
            assert isinstance(msg, HeartbeatFrame)
            lag = (now - msg.ts * NS_PER_MS) / 1e6 if msg.ts is not None else None
            self._sink.on_frame(self.source_id, lag_ms=lag)
            if msg.head_seq is not None:
                self.tracker.on_heartbeat(msg.head_seq)
        elif tp is WelcomeFrame:
            assert isinstance(msg, WelcomeFrame)
            self._welcome(msg.epoch, msg.head_seq, msg.ring_low, msg.hb_ms)
        elif tp is SnapshotFrame:
            assert isinstance(msg, SnapshotFrame)
            for d in msg.ticks:
                self._sink.on_incoming(self._incoming(d, now, mono, seq=None, replay=False, snapshot=True))
        elif tp is ReplayBeginFrame:
            assert isinstance(msg, ReplayBeginFrame)
            self.tracker.on_replay_begin(msg.req, msg.from_seq, msg.to_seq)
        elif tp is ReplayEndFrame:
            assert isinstance(msg, ReplayEndFrame)
            self.tracker.on_replay_end(msg.req, msg.to_seq)
        elif tp is ResumeRejectedFrame:
            assert isinstance(msg, ResumeRejectedFrame)
            self._resume_rejected(msg.reason, msg.ring_low, msg.head_seq)
        elif tp is ErrorFrame:
            assert isinstance(msg, ErrorFrame)
            self._error(msg.req, msg.code, msg.ring_low, msg.head_seq)

    def _welcome(self, epoch: str, head: int, ring_low: int, hb_ms: int | None) -> None:
        self._hb_ms = int(hb_ms or self._hb_ms)
        old = self.tracker.epoch
        self.tracker.on_welcome(epoch, head, ring_low)
        self._welcomed = True
        self._sink.on_connected(self.source_id, epoch=epoch)
        if old is not None and old != epoch and self.tracker.epoch == epoch:
            self._sink.on_epoch_change(self.source_id, old, epoch)

    def _resume_rejected(self, reason: str, ring_low: int, head: int) -> None:
        old = self.tracker.epoch
        self.tracker.on_resume_rejected(reason, ring_low, head)
        logger.warning("md.feed_a.resume_rejected", reason=reason, ring_low=ring_low, head=head)
        if old is not None and self.tracker.epoch is not None and old != self.tracker.epoch:
            self._sink.on_epoch_change(self.source_id, old, self.tracker.epoch)

    def _error(self, req: Any, code: str, ring_low: int | None, head: int | None) -> None:
        self.tracker.on_error(req, code, ring_low, head)
        if code == "NOT_SUBSCRIBED":
            self.reconnect("server says NOT_SUBSCRIBED")

    def _on_frame_slow(self, raw: str | bytes, now: int, mono: int) -> None:
        """Generic ``dict`` path for frames that do not fit the typed schema (malformed ticks, unknown ops)."""
        self.slow_frames += 1
        try:
            msg = _json_decode(raw)
        except msgspec.DecodeError:
            logger.warning("md.feed_a.bad_frame", size=len(raw))
            self._sink.on_incoming(Incoming(_FEED_A, now, mono, reject_reason=MD_REJECT_MALFORMED, detail="undecodable frame"))
            return
        if not isinstance(msg, dict):
            return
        op = msg.get("op")
        if op == "tick":
            seq = msg.get("seq")
            if not isinstance(seq, int) or isinstance(seq, bool):
                self._sink.on_incoming(Incoming(_FEED_A, now, mono, reject_reason=MD_REJECT_MALFORMED, detail="tick without seq"))
                return
            self.tracker.on_tick(seq, bool(msg.get("replay")), (msg.get("d"), now, mono))
        elif op == "heartbeat":
            ts = msg.get("ts")
            lag = (now - int(ts) * NS_PER_MS) / 1e6 if isinstance(ts, int) else None
            self._sink.on_frame(self.source_id, lag_ms=lag)
            head = msg.get("head_seq")
            if isinstance(head, int):
                self.tracker.on_heartbeat(head)
        elif op == "welcome":
            self._welcome(str(msg.get("epoch") or ""), int(msg.get("head_seq") or 0), int(msg.get("ring_low") or 1), msg.get("hb_ms"))
        elif op == "snapshot":
            for d in msg.get("ticks") or []:
                self._sink.on_incoming(self._incoming(d, now, mono, seq=None, replay=False, snapshot=True))
        elif op == "replay_begin":
            self.tracker.on_replay_begin(msg.get("req"), int(msg.get("from_seq") or 0), int(msg.get("to_seq") or 0))
        elif op == "replay_end":
            self.tracker.on_replay_end(msg.get("req"), int(msg.get("to_seq") or 0))
        elif op == "resume_rejected":
            self._resume_rejected(str(msg.get("reason")), int(msg.get("ring_low") or 1), int(msg.get("head_seq") or 0))
        elif op == "error":
            ring_low = msg.get("ring_low")
            head = msg.get("head_seq")
            self._error(msg.get("req"), str(msg.get("code") or ""), ring_low if isinstance(ring_low, int) else None, head if isinstance(head, int) else None)
        else:
            logger.debug("md.feed_a.unknown_op", op=op)


# =============================================================================
# Feed B
# =============================================================================
class FeedBRestSource:
    """Feed B poller (02 §9.2); see the module docstring."""

    source_id = SourceId.FEED_B
    capabilities = SourceCapabilities(has_seq=False, can_replay=False, coarse_ts=False)

    def __init__(
        self,
        cfg: FeedBConfig,
        normalizer: Normalizer,
        sink: SourceSink,
        clock: Clock,
        *,
        url: str | None = None,
        params: MdParams | None = None,
    ) -> None:
        self._cfg = cfg
        self._normalizer = normalizer
        self._sink = sink
        self._clock = clock
        self._p = params or MdParams()
        self.url = url or cfg.url
        self._task: asyncio.Task[None] | None = None
        self.polls = 0
        self.failures = 0
        self.skipped = 0

    async def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name="md-feed-b")

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._task
            self._task = None

    def timeout(self) -> httpx.Timeout:
        """02 §9.2: connect 1 s, read 1.5 s."""
        p = self._p
        return httpx.Timeout(p.feed_b_read_timeout_s, connect=p.feed_b_connect_timeout_s)

    async def _run(self) -> None:
        interval = max(0.05, self._cfg.poll_ms / 1000)
        async with httpx.AsyncClient(timeout=self.timeout(), trust_env=False) as client:
            loop = asyncio.get_running_loop()
            next_at = loop.time()
            while True:
                now = loop.time()
                if next_at > now:
                    await asyncio.sleep(next_at - now)
                started = loop.time()
                try:
                    await self.poll_once(client)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("md.feed_b.poll_bug")
                next_at = started + interval
                late = loop.time() - next_at
                if late > 0:  # 02 §9.2：前一次没有完成时跳过本次，不补发
                    missed = int(late // interval) + 1
                    self.skipped += missed
                    next_at += missed * interval

    async def poll_once(self, client: httpx.AsyncClient) -> None:
        self.polls += 1
        try:
            resp = await client.get(self.url)
            recv_ns = self._clock.now_ns()
            mono = self._clock.monotonic_ns()
            if resp.status_code != 200:
                raise ValueError(f"HTTP {resp.status_code}")
            body = _json_decode(resp.content)
            if not isinstance(body, dict) or not isinstance(body.get("data"), list):
                raise ValueError("body is not {ts, td, data: [...]}")
        except asyncio.CancelledError:
            raise
        except (httpx.HTTPError, ValueError, msgspec.DecodeError, OSError) as exc:
            self.failures += 1
            self._sink.on_poll(self.source_id, False, error=f"{type(exc).__name__}: {exc}"[:200])
            return
        ts = body.get("ts")
        lag = (recv_ns - int(ts) * NS_PER_MS) / 1e6 if isinstance(ts, int) else None
        vendor_td = body.get("td") if isinstance(body.get("td"), str) else None
        records = body["data"]
        incoming: list[Incoming] = []
        for rec in records:
            if not isinstance(rec, dict):
                incoming.append(Incoming(self.source_id, recv_ns, mono, reject_reason=MD_REJECT_MALFORMED, detail="record is not an object"))
                continue
            try:
                tick = self._normalizer.feed_b(rec, vendor_td=vendor_td, recv_ns=recv_ns)
            except NormalizeError as exc:
                incoming.append(
                    Incoming(
                        self.source_id,
                        recv_ns,
                        mono,
                        tick=exc.tick,
                        reject_reason=exc.reason,
                        detail=exc.detail,
                        instrument_id=exc.instrument_id or str(rec.get("sym") or ""),
                        exchange=exc.exchange,
                    )
                )
                continue
            incoming.append(Incoming(self.source_id, recv_ns, mono, tick=tick))
        # coverage counts only the records of this poll that the pipeline accepts (on_incoming)
        self._sink.on_poll(self.source_id, True, lag_ms=lag)
        for inc in incoming:
            self._sink.on_incoming(inc)


# =============================================================================
# Sina
# =============================================================================
class SinaSourceAdapter:
    """Sina (02 §9.3) through :class:`futrisk.marketdata.poller.SinaSource` (disabled by default)."""

    source_id = SourceId.SINA
    capabilities = SourceCapabilities(has_seq=False, can_replay=False, coarse_ts=True)

    def __init__(
        self,
        cfg: SinaConfig,
        products: ProductsConfig,
        calendar: TradingCalendar,
        sink: SourceSink,
        clock: Clock,
    ) -> None:
        self._sink = sink
        self._clock = clock
        instruments = resolve_sina_instruments(products, cfg.symbols)
        normalizer = SinaNormalizer(instruments, calendar)
        poller = SinaPoller(
            list(instruments),
            url=cfg.url,
            referer=cfg.referer,
            poll_ms=cfg.poll_ms,
            timeout_ms=cfg.timeout_ms,
            trust_env=cfg.trust_env,
            clock=clock,
        )
        self._source = SinaSource(poller, normalizer)

    async def start(self) -> None:
        await self._source.start(self._on_batch)

    async def stop(self) -> None:
        await self._source.stop()

    def _on_batch(self, batch: SinaBatch) -> None:
        mono = self._clock.monotonic_ns()
        if not batch.poll.ok:
            self._sink.on_poll(self.source_id, False, error=batch.poll.error or f"HTTP {batch.poll.status}")
            return
        self._sink.on_poll(self.source_id, True)
        for r in batch.results:
            if r.ok:
                self._sink.on_incoming(Incoming(self.source_id, r.recv_ns, mono, tick=r.tick))
            else:
                self._sink.on_incoming(
                    Incoming(
                        self.source_id,
                        r.recv_ns,
                        mono,
                        tick=r.tick,
                        reject_reason=r.reject_reason or MD_REJECT_MALFORMED,
                        detail=r.detail,
                        instrument_id=r.instrument_id or r.code,
                    )
                )


__all__ = [
    "FeedAWsSource",
    "FeedBRestSource",
    "MarketDataSource",
    "SinaSourceAdapter",
    "SourceCapabilities",
    "SourceSink",
]
