"""Market-data sources: one push transport, one poll transport, one decoder per vendor format (02 §9, §10).

- :class:`FeedAWsSource`: the WebSocket client of Feed A - ``subscribe`` with ``resume {epoch,
  from_seq = contiguous + 1}``, sequence tracking (:class:`SeqTracker`), missed heartbeats ->
  disconnect (any frame counts as alive), reconnect with back-off + jitter; frames are decoded
  into typed structs (:mod:`.wire`) with a ``dict`` fallback for anything that does not fit;
- :class:`PollTransport`: fixed-rate HTTP polling shared by Feed B and Sina (and the recording
  CLI): never two polls at once, a slow poll skips the missed slots, per-request timeouts,
  optional back-off after failures (Sina: 403 / 429 double the interval up to 60 s, network
  errors up to 5 s), failed polls reported to the sink (the trust manager excludes a source
  after ``exclude_failed_polls``). A :class:`BodyDecoder` turns one response body into
  :class:`Incoming` records: :class:`FeedBDecoder` here, ``SinaDecoder`` in :mod:`.sina`.

Every source produces :class:`Incoming` (a normalized ``Tick`` or a reject with its reason)
and reports connection / poll outcomes to a :class:`SourceSink`.
"""

from __future__ import annotations

import asyncio
import contextlib
import math
import random
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any, Protocol

import httpx
import msgspec
from loguru import logger
from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed, ConnectionClosedOK, InvalidHandshake, InvalidURI

from futrisk.common.clock import NS_PER_MS, Clock
from futrisk.common.config.feeds import FeedAConfig, FeedBConfig, SinaConfig
from futrisk.messages.codes import MD_REJECT_MALFORMED
from futrisk.messages.enums import SourceId
from futrisk.services.md_gateway.model import Incoming
from futrisk.services.md_gateway.normalizer import NormalizeError, Normalizer
from futrisk.services.md_gateway.params import MdParams
from futrisk.services.md_gateway.seqtracker import Hole, SeqTracker
from futrisk.services.md_gateway.sina import SinaDecoder, build_url, request_headers
from futrisk.services.md_gateway.wire import (
    CtpTick,
    ErrorFrame,
    Frame,
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
_MAX_SLEEP_CHUNK_S = 1.0  # re-check stop / keep_running at least this often while waiting between polls
_MAX_ERROR_LEN = 200
BAN_STATUSES = frozenset({403, 429})


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
        self, source: SourceId, ok: bool, *, error: str | None = None, lag_ms: float | None = None, instruments: Iterable[str] = ()
    ) -> None: ...

    def on_frame(self, source: SourceId, *, lag_ms: float | None = None) -> None: ...

    def on_gap(self, source: SourceId, hole: Hole, event: str) -> None: ...

    def on_epoch_change(self, source: SourceId, old: str | None, new: str) -> None: ...

    def on_resume_finished(self, source: SourceId) -> None: ...


class MarketDataSource(Protocol):
    """02 §10: ``capabilities``, ``start()``, ``stop()``."""

    source_id: SourceId
    capabilities: SourceCapabilities

    async def start(self) -> None: ...

    async def stop(self) -> None: ...


# =============================================================================
# Feed A (WebSocket push)
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
            self, clock, reorder_max=params.arb.reorder_buffer_max, reorder_ns=params.reorder_ns,
            replay_timeout_ns=int(params.replay_timeout_s * 1e9), max_attempts=params.replay_max_attempts,
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
        """In-connection ``replay {from_seq, to_seq}`` (TrackerSink)."""
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
        if self._ws is not None:
            asyncio.ensure_future(self._ws.close(code=1000, reason=reason[:100]))

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
                _FEED_A, recv_ns, mono, tick=exc.tick, reject_reason=exc.reason, detail=exc.detail, seq=seq, replay=replay,
                snapshot=snapshot, epoch=epoch, instrument_id=exc.instrument_id or raw_id, exchange=exc.exchange,
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
                    self.url, proxy=None, compression=None, open_timeout=5, ping_interval=None, close_timeout=1, max_size=None,
                    max_queue=max(16, self._p.ws_max_queue),
                ) as ws:
                    self._ws = ws
                    self.connects += 1
                    await self._session(ws)
                    reason = self._close_reason or "connection closed"
            except asyncio.CancelledError:
                raise
            except (OSError, TimeoutError, InvalidHandshake, InvalidURI, ConnectionClosed) as exc:
                reason = self._close_reason or f"{type(exc).__name__}: {exc}"[:_MAX_ERROR_LEN]
            except Exception as exc:  # a bug in the handling must not kill the reconnect loop
                logger.exception("md.feed_a.session_failed")
                reason = f"internal: {exc!r}"[:_MAX_ERROR_LEN]
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
            misses = self._p.arb.exclude_missed_heartbeats
            if silent_ns > misses * self._hb_ms * NS_PER_MS:
                self._close_reason = f"no frame for {silent_ns / 1e6:.0f} ms ({misses} heartbeats missed)"
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
        self._dispatch(msg, now, mono)

    def _dispatch(self, msg: Frame, now: int, mono: int) -> None:
        if isinstance(msg, TickFrame):
            self.tracker.on_tick(msg.seq, msg.replay, (msg.d, now, mono))
        elif isinstance(msg, HeartbeatFrame):
            self._sink.on_frame(self.source_id, lag_ms=(now - msg.ts * NS_PER_MS) / 1e6 if msg.ts is not None else None)
            if msg.head_seq is not None:
                self.tracker.on_heartbeat(msg.head_seq)
        elif isinstance(msg, WelcomeFrame):
            self._hb_ms = int(msg.hb_ms or self._hb_ms)
            old = self.tracker.epoch
            self.tracker.on_welcome(msg.epoch, msg.head_seq, msg.ring_low)
            self._welcomed = True
            self._sink.on_connected(self.source_id, epoch=msg.epoch)
            if old is not None and old != msg.epoch and self.tracker.epoch == msg.epoch:
                self._sink.on_epoch_change(self.source_id, old, msg.epoch)
        elif isinstance(msg, SnapshotFrame):
            for d in msg.ticks:
                self._sink.on_incoming(self._incoming(d, now, mono, seq=None, replay=False, snapshot=True))
        elif isinstance(msg, ReplayBeginFrame):
            self.tracker.on_replay_begin(msg.req, msg.from_seq, msg.to_seq)
        elif isinstance(msg, ReplayEndFrame):
            self.tracker.on_replay_end(msg.req, msg.to_seq)
        elif isinstance(msg, ResumeRejectedFrame):
            old = self.tracker.epoch
            self.tracker.on_resume_rejected(msg.reason, msg.ring_low, msg.head_seq)
            logger.warning("md.feed_a.resume_rejected", reason=msg.reason, ring_low=msg.ring_low, head=msg.head_seq)
            if old is not None and self.tracker.epoch is not None and old != self.tracker.epoch:
                self._sink.on_epoch_change(self.source_id, old, self.tracker.epoch)
        elif isinstance(msg, ErrorFrame):
            self.tracker.on_error(msg.req, msg.code, msg.ring_low, msg.head_seq)
            if msg.code == "NOT_SUBSCRIBED":
                self.reconnect("server says NOT_SUBSCRIBED")

    def _on_frame_slow(self, raw: bytes, now: int, mono: int) -> None:
        """Generic ``dict`` path for frames that do not fit the typed schema (malformed ticks)."""
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
        elif op == "snapshot":
            for d in msg.get("ticks") or []:
                self._sink.on_incoming(self._incoming(d, now, mono, seq=None, replay=False, snapshot=True))
        else:
            try:
                self._dispatch(msgspec.convert(msg, Frame, strict=False), now, mono)
            except msgspec.ValidationError:
                logger.debug("md.feed_a.unknown_frame", op=op)


# =============================================================================
# HTTP polling (Feed B, Sina, the recording CLI)
# =============================================================================
class BodyDecoder(Protocol):
    """Turns one successful response body into records (+ the transport lag when the body carries a server time)."""

    def decode(self, body: bytes, recv_ns: int, mono: int) -> tuple[list[Incoming], float | None]: ...


@dataclass(frozen=True)
class PollSpec:
    """What a :class:`PollTransport` polls and how."""

    source_id: SourceId
    url: str
    poll_ms: int
    timeout_ms: int  # read timeout of one poll
    connect_timeout_s: float = 1.0
    headers: Mapping[str, str] = field(default_factory=dict)
    trust_env: bool = False  # honour HTTP(S)_PROXY
    min_poll_ms: int = 50
    ban_backoff_cap_s: float = 0.0  # > 0: HTTP 403 / 429 double the interval up to this cap (02 §9.3)
    network_backoff_cap_s: float = 0.0  # > 0: other failures double it from the second one up to this cap
    coarse_ts: bool = False


@dataclass(frozen=True, slots=True)
class PollResult:
    """One HTTP cycle: timing, status and the raw body (``status`` is ``None`` on transport errors)."""

    seq: int
    sent_ns: int
    recv_ns: int
    elapsed_ms: float
    status: int | None
    body: bytes | None
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.error is None and self.status == 200 and self.body is not None


@dataclass
class PollStats:
    polls: int = 0
    ok: int = 0
    failed: int = 0
    skipped: int = 0
    consecutive_failures: int = 0
    last_status: int | None = None
    last_error: str | None = None
    last_interval_s: float | None = None


class PollTransport:
    """Fixed-rate, non-overlapping poller: ``fetch`` -> decoder -> sink, with optional back-off."""

    def __init__(
        self,
        spec: PollSpec,
        decoder: BodyDecoder,
        sink: SourceSink,
        clock: Clock,
        *,
        client: httpx.AsyncClient | None = None,
        on_result: Callable[[PollResult], None] | None = None,
    ) -> None:
        self.spec = spec
        self.source_id = spec.source_id
        self.capabilities = SourceCapabilities(has_seq=False, can_replay=False, coarse_ts=spec.coarse_ts)
        self._decoder = decoder
        self._sink = sink
        self._clock = clock
        self._on_result = on_result
        poll_ms = spec.poll_ms
        if poll_ms < spec.min_poll_ms:
            logger.warning(f"md.poll_ms {poll_ms} below the minimum {spec.min_poll_ms} for {spec.source_id}; clamped")
            poll_ms = spec.min_poll_ms
        self.poll_s = poll_ms / 1000.0
        self.timeout = httpx.Timeout(spec.timeout_ms / 1000.0, connect=spec.connect_timeout_s)
        self._client = client
        self._owns_client = client is None
        self._seq = 0
        self._stop = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self.stats = PollStats()

    @property
    def running(self) -> bool:
        return self._task is not None and not self._task.done()

    def _ensure_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=self.timeout, trust_env=self.spec.trust_env, follow_redirects=False,
                limits=httpx.Limits(max_connections=2, max_keepalive_connections=1),
            )
            self._owns_client = True
        return self._client

    async def aclose(self) -> None:
        if self._client is not None and self._owns_client:
            await self._client.aclose()
            self._client = None

    # ------------------------------------------------------------------ API
    async def start(self) -> None:
        if self.running:
            return
        self._stop.clear()
        self._task = asyncio.create_task(self.run(stop=self._stop), name=f"md-poll-{self.source_id.lower()}")

    async def stop(self) -> None:
        """Stop polling (an in-flight poll is delivered first) and close the HTTP client."""
        self._stop.set()
        if self._task is not None:
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None
        await self.aclose()

    async def fetch(self) -> PollResult:
        """One request; never raises for network / HTTP problems (they are reported in the result)."""
        client = self._ensure_client()
        self._seq += 1
        sent_ns = self._clock.now_ns()
        t0 = self._clock.monotonic_ns()
        status: int | None = None
        body: bytes | None = None
        error: str | None = None
        try:
            async with asyncio.timeout(self.spec.connect_timeout_s + self.spec.timeout_ms / 1000.0):
                resp = await client.get(self.spec.url, headers=dict(self.spec.headers), timeout=self.timeout)
            status, body = resp.status_code, resp.content
            if status != 200:
                error = f"HTTP {status}"
        except (httpx.HTTPError, TimeoutError, OSError) as exc:
            error = f"{type(exc).__name__}: {exc}"[:_MAX_ERROR_LEN]
        recv_ns = self._clock.now_ns()
        return PollResult(self._seq, sent_ns, recv_ns, (self._clock.monotonic_ns() - t0) / 1e6, status, body, error)

    async def poll_once(self) -> bool:
        """``fetch`` + decode + deliver; returns whether the poll was usable (also keeps the statistics)."""
        res = await self.fetch()
        if self._on_result is not None:
            self._on_result(res)
        error = res.error
        records: list[Incoming] = []
        lag_ms: float | None = None
        if res.ok:
            try:
                records, lag_ms = self._decoder.decode(res.body or b"", res.recv_ns, self._clock.monotonic_ns())
            except Exception as exc:  # a body without the vendor shape (ValueError), or a decoder bug: a failed poll
                error = f"{type(exc).__name__}: {exc}"[:_MAX_ERROR_LEN]
                if not isinstance(exc, ValueError):
                    logger.exception("md.decode_bug", source=str(self.source_id))
        self._account(res, error)
        if error is not None:
            self._sink.on_poll(self.source_id, False, error=error)
            return False
        self._sink.on_poll(self.source_id, True, lag_ms=lag_ms)
        for inc in records:  # coverage counts only the records the pipeline accepts (on_incoming)
            self._sink.on_incoming(inc)
        return True

    def current_interval_s(self) -> float:
        """Interval before the next poll, given the outcome of the last completed one."""
        st, spec = self.stats, self.spec
        n = st.consecutive_failures
        if n == 0:
            return self.poll_s
        if st.last_status in BAN_STATUSES and spec.ban_backoff_cap_s > 0:
            return max(self.poll_s, min(spec.ban_backoff_cap_s, self.poll_s * 2.0**n))
        if spec.network_backoff_cap_s > 0:
            return max(self.poll_s, min(spec.network_backoff_cap_s, self.poll_s * 2.0 ** (n - 1)))
        return self.poll_s

    async def run(self, *, stop: asyncio.Event | None = None, keep_running: Callable[[], bool] | None = None) -> None:
        """Poll until ``stop`` is set or ``keep_running()`` turns False (checked between polls, at least every second).

        Polls run one after another; the next one starts ``interval`` after the previous one *started*,
        and slots that elapsed while a slow poll was running are skipped, never made up in a burst (02 §9.2).
        """
        loop = asyncio.get_running_loop()
        next_at = loop.time()
        while not (stop is not None and stop.is_set()) and (keep_running is None or keep_running()):
            now = loop.time()
            if next_at > now:
                delay = min(next_at - now, _MAX_SLEEP_CHUNK_S)
                if stop is None:
                    await asyncio.sleep(delay)
                else:
                    with contextlib.suppress(TimeoutError):
                        await asyncio.wait_for(stop.wait(), timeout=delay)
                continue
            started = loop.time()
            try:
                await self.poll_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("md.poll_bug", source=str(self.source_id))
            interval = self.current_interval_s()
            self.stats.last_interval_s = interval
            next_at = started + interval
            if next_at < loop.time():  # the poll overran its slot(s): skip them instead of bursting
                missed = math.ceil((loop.time() - next_at) / interval)
                self.stats.skipped += missed
                next_at += missed * interval

    def _account(self, res: PollResult, error: str | None) -> None:
        st = self.stats
        st.polls += 1
        st.last_status = res.status
        if error is None:
            st.ok += 1
            if st.consecutive_failures:
                logger.info("md.poll_recovered", source=str(self.source_id), after=st.consecutive_failures)
            st.consecutive_failures = 0
            return
        st.failed += 1
        st.consecutive_failures += 1
        st.last_error = error
        if st.consecutive_failures in (1, 3) or st.consecutive_failures % 20 == 0:
            logger.warning("md.poll_failed", source=str(self.source_id), seq=res.seq, error=error, consecutive=st.consecutive_failures)


class FeedBDecoder:
    """``GET /v1/quotes`` body ``{ts, td, data: [...]}`` (02 §9.2) -> records; one bad record only rejects itself."""

    source_id = SourceId.FEED_B

    def __init__(self, normalizer: Normalizer) -> None:
        self._n = normalizer

    def decode(self, body: bytes, recv_ns: int, mono: int) -> tuple[list[Incoming], float | None]:
        try:
            obj = _json_decode(body)
        except msgspec.DecodeError as exc:
            raise ValueError(f"bad JSON: {exc}") from exc
        if not isinstance(obj, dict) or not isinstance(obj.get("data"), list):
            raise ValueError("body is not {ts, td, data: [...]}")
        ts = obj.get("ts")
        vendor_td = obj.get("td") if isinstance(obj.get("td"), str) else None
        out: list[Incoming] = []
        for rec in obj["data"]:
            if not isinstance(rec, dict):
                out.append(Incoming(self.source_id, recv_ns, mono, reject_reason=MD_REJECT_MALFORMED, detail="record is not an object"))
                continue
            try:
                out.append(Incoming(self.source_id, recv_ns, mono, tick=self._n.feed_b(rec, vendor_td=vendor_td, recv_ns=recv_ns)))
            except NormalizeError as exc:
                out.append(
                    Incoming(
                        self.source_id, recv_ns, mono, tick=exc.tick, reject_reason=exc.reason, detail=exc.detail,
                        instrument_id=exc.instrument_id or str(rec.get("sym") or ""), exchange=exc.exchange,
                    )
                )
        return out, (recv_ns - int(ts) * NS_PER_MS) / 1e6 if isinstance(ts, int) else None


def feed_b_source(
    cfg: FeedBConfig, normalizer: Normalizer, sink: SourceSink, clock: Clock, *, url: str | None = None, params: MdParams | None = None, client: httpx.AsyncClient | None = None
) -> PollTransport:
    """Feed B (02 §9.2): ``poll_ms``, connect 1 s / read ``timeout_ms``, no back-off (the trust manager excludes it)."""
    p = params or MdParams()
    spec = PollSpec(SourceId.FEED_B, url or cfg.url, cfg.poll_ms, cfg.timeout_ms, connect_timeout_s=p.feed_b_connect_timeout_s)
    return PollTransport(spec, FeedBDecoder(normalizer), sink, clock, client=client)


def sina_source(
    cfg: SinaConfig, decoder: SinaDecoder, sink: SourceSink, clock: Clock, *, client: httpx.AsyncClient | None = None, on_result: Callable[[PollResult], None] | None = None
) -> PollTransport:
    """Sina (02 §9.3): one merged request, ``Referer``, polls >= 1 s apart, ban / network back-off, second-resolution timestamps."""
    spec = PollSpec(
        SourceId.SINA, build_url(decoder.codes, cfg.url), cfg.poll_ms, cfg.timeout_ms, connect_timeout_s=max(cfg.timeout_ms / 1000.0, 3.0),
        headers=request_headers(cfg.referer), trust_env=cfg.trust_env, min_poll_ms=1000, ban_backoff_cap_s=60.0, network_backoff_cap_s=5.0, coarse_ts=True,
    )
    return PollTransport(spec, decoder, sink, clock, client=client, on_result=on_result)


__all__ = [
    "BAN_STATUSES",
    "BodyDecoder",
    "FeedAWsSource",
    "FeedBDecoder",
    "MarketDataSource",
    "PollResult",
    "PollSpec",
    "PollStats",
    "PollTransport",
    "SourceCapabilities",
    "SourceSink",
    "feed_b_source",
    "sina_source",
]
