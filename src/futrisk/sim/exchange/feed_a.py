"""Feed A: CTP-style push feed over WebSocket (02 §9.1).

``ws://{host}:{port}/feed``; every text frame is one JSON object whose ``op`` names it.

- one global ``seq`` across all instruments; a new ``epoch`` (UUIDv7) whenever the process
  (or a ``SIM_RESTART`` fault) restarts; a ring buffer of the last ``ring_size`` ticks;
- ``subscribe {req, instruments:"*", resume}`` -> ``welcome``, then either a ``snapshot``
  (first connection, or after ``resume_rejected`` ``EPOCH_CHANGED`` / ``SEQ_EVICTED`` /
  ``BAD_RESUME``) or a ``replay_begin`` / ``tick(replay=true)`` ... / ``replay_end``
  block, then live ticks from ``to_seq + 1``: live data produced meanwhile is held back
  until ``replay_end`` (02 §9.1 顺序, 续传). A resume with nothing missing
  (``from_seq == head_seq + 1``) gets an empty block: ``replay_begin {from_seq: head_seq + 1,
  to_seq: head_seq}`` directly followed by ``replay_end {to_seq: head_seq}``;
- ``replay {req, from_seq, to_seq}`` fills holes on the same connection (连接内补洞): the
  replayed ticks (``replay=true``) are interleaved with the live stream, whose seq just
  continues. The range is clamped to ``[ring_low, head_seq]`` (``replay_begin`` shows the
  part served); a range entirely below ``ring_low`` gets ``error SEQ_EVICTED`` and a range
  starting after ``head_seq`` gets ``error BAD_REQUEST`` (nothing to replay yet);
- ``heartbeat {ts, head_seq}`` every ``hb_ms`` - also in the middle of long replays;
  ``error {req?, code, message, ring_low?, head_seq?}`` with the codes of 02 §11.3 (plus
  ``BAD_REQUEST`` for malformed or out-of-range requests);
- ``instruments`` may also be a list (the doc requires clients to use ``"*"``; with a list
  the global seq shows gaps, exactly as 02 §9.1 warns).

Wire faults (02 §9.5) are applied to live ticks only: ``FEED_GAP`` drops messages that
stay in the ring (replayable), ``FEED_DUPLICATE`` resends, ``FEED_REORDER`` shuffles a
window, ``FEED_LATENCY`` delays every queued outbound message of a connection (live
ticks, heartbeats, snapshots, resume replays; in-connection replay chunks are not
delayed), ``FEED_DISCONNECT`` closes connections (``mode=close``) or stops talking
(``mode=silent``, detected by missed heartbeats) and refuses reconnects with HTTP 503.

Every connection has its own send queue and sender task, so a slow consumer never delays
the others; a connection whose queue exceeds :data:`MAX_SESSION_QUEUE` is closed (1008).
"""

from __future__ import annotations

import asyncio
import contextlib
import itertools
import json
import time
from collections import deque
from collections.abc import Sequence
from dataclasses import dataclass
from http import HTTPStatus
from typing import Any

from loguru import logger
from websockets.asyncio.server import Server, ServerConnection, serve
from websockets.exceptions import ConnectionClosed
from websockets.http11 import Request, Response

from futrisk.common.calendar import TradingCalendar
from futrisk.common.clock import NS_PER_MS, MarketClock, td_str
from futrisk.common.ids import new_epoch
from futrisk.sim.exchange.engine import InstrumentState
from futrisk.sim.exchange.faults import FEED_A, Fault, FaultManager
from futrisk.sim.exchange.formats import corrupted_state, dumps, feed_a_tick
from futrisk.sim.exchange.tasks import spawn

#: a connection whose send queue grows beyond this is closed (slow consumer)
MAX_SESSION_QUEUE = 200_000
#: messages held by FEED_REORDER are released after this many seconds even if the window is not full
REORDER_MAX_HOLD_S = 1.0
#: replayed ticks sent between two looks at the live queue during an in-connection replay
REPLAY_CHUNK = 200
#: seconds the server waits for the closing handshake of a stalled client
CLOSE_TIMEOUT_S = 2.0

_Entry = tuple[int, str, str]  # (seq, instrument, FeedATick JSON)
_Item = tuple[str, Any, float]  # (kind, payload, due monotonic time or 0)


def _seq_arg(v: Any) -> int | None:
    return v if isinstance(v, int) and not isinstance(v, bool) else None


def _tick_text(seq: int, replay: bool, d_json: str) -> str:
    return f'{{"op":"tick","seq":{seq},"replay":{"true" if replay else "false"},"d":{d_json}}}'


@dataclass
class _Replay:
    """An in-connection replay (``replay`` op) being sent in chunks between live messages."""

    req: Any
    from_seq: int
    to_seq: int
    entries: list[_Entry]
    pos: int = 0
    begun: bool = False


class _Session:
    """One WebSocket connection."""

    __slots__ = (
        "closing", "conn", "instruments", "live_from", "muted", "queue", "replays", "sender", "server", "subscribed",
    )

    def __init__(self, server: FeedAServer, conn: ServerConnection) -> None:
        self.server = server
        self.conn = conn
        self.queue: asyncio.Queue[_Item | None] = asyncio.Queue()
        self.replays: deque[_Replay] = deque()
        self.subscribed = False
        self.live_from = 0  # 0: not live yet; else first live seq this session accepts
        self.instruments: frozenset[str] | None = None
        self.muted = False
        self.closing = False
        self.sender: asyncio.Task[None] | None = None

    def wants(self, instrument_id: str) -> bool:
        return self.instruments is None or instrument_id in self.instruments

    def put(self, kind: str, payload: Any) -> None:
        if self.closing:
            return
        if self.queue.qsize() > MAX_SESSION_QUEUE:
            logger.warning("feed A: closing a slow consumer ({} queued messages)", self.queue.qsize())
            self.server.close_session(self, 1008, "slow consumer")
            return
        self.queue.put_nowait((kind, payload, self.server.due()))

    def put_text(self, text: str) -> None:
        self.put("text", text)

    def put_live(self, items: Sequence[_Entry]) -> None:
        if not self.live_from:
            return
        for seq, iid, text in items:
            if seq >= self.live_from and self.wants(iid):
                self.put("text", text)

    def add_replay(self, job: _Replay) -> None:
        if self.closing:
            return
        self.replays.append(job)
        self.queue.put_nowait(("wake", None, 0.0))  # a sender blocked on the empty queue picks it up


class FeedAServer:
    """WebSocket server, ring buffer and fan-out of Feed A."""

    def __init__(
        self,
        *,
        host: str,
        port: int,
        ring_size: int,
        hb_ms: int,
        clock: MarketClock,
        calendar: TradingCalendar,
        faults: FaultManager,
        epoch: str | None = None,
    ) -> None:
        self.host = host
        self.port = port
        self.ring_size = max(1, int(ring_size))
        self.hb_ms = max(10, int(hb_ms))
        self._clock = clock
        self._calendar = calendar
        self._faults = faults
        self.epoch = epoch or new_epoch()
        self.head_seq = 0
        self._ring: deque[_Entry] = deque(maxlen=self.ring_size)
        self._views: dict[str, dict[str, Any]] = {}  # last FeedATick per instrument (snapshot source)
        self._last_state: dict[str, InstrumentState] = {}
        self._sessions: set[_Session] = set()
        self._reorder: list[_Entry] = []
        self._reorder_since = 0.0
        self._server: Server | None = None
        self._hb_task: asyncio.Task[None] | None = None
        self._bg: set[asyncio.Task[Any]] = set()
        self.published = 0
        self.dropped = 0

    # ------------------------------------------------------------------ lifecycle
    @property
    def url(self) -> str:
        return f"ws://{self.host}:{self.port}/feed"

    @property
    def ring_low(self) -> int:
        return self._ring[0][0] if self._ring else self.head_seq + 1

    @property
    def clients(self) -> int:
        return len(self._sessions)

    def init_views(self, states: Sequence[InstrumentState]) -> None:
        for st in states:
            self._views[st.instrument_id] = feed_a_tick(st)
            self._last_state[st.instrument_id] = st.copy()

    async def start(self) -> None:
        self._server = await serve(
            self._handler,
            self.host,
            self.port,
            process_request=self._process_request,
            compression=None,
            ping_interval=20,
            ping_timeout=20,
            close_timeout=CLOSE_TIMEOUT_S,
            max_size=1 << 20,
        )
        sock = next(iter(self._server.sockets))
        self.port = sock.getsockname()[1]
        self._hb_task = spawn("feed-a-heartbeat", self._heartbeats)
        logger.info("feed A listening on {} (epoch={}, ring={})", self.url, self.epoch, self.ring_size)

    async def stop(self) -> None:
        if self._hb_task is not None:
            self._hb_task.cancel()
            await asyncio.gather(self._hb_task, return_exceptions=True)
            self._hb_task = None
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
        for t in list(self._bg):
            t.cancel()
        await asyncio.gather(*self._bg, return_exceptions=True)

    # ------------------------------------------------------------------ fault helpers
    def due(self) -> float:
        latency = self._faults.latency_ms(FEED_A)
        return time.monotonic() + latency / 1000.0 if latency > 0 else 0.0

    def close_session(self, s: _Session, code: int, reason: str) -> None:
        if s.closing:
            return
        s.closing = True
        s.replays.clear()

        async def _close() -> None:
            with contextlib.suppress(Exception):
                await s.conn.close(code, reason)

        t = asyncio.create_task(_close())
        self._bg.add(t)
        t.add_done_callback(self._bg.discard)

    def disconnect_all(self, mode: str) -> int:
        """``FEED_DISCONNECT``: close every connection, or mute them (``silent``)."""
        n = 0
        for s in list(self._sessions):
            n += 1
            if mode == "silent":
                s.muted = True
            else:
                self.close_session(s, 1001, "FEED_DISCONNECT")
        return n

    def close_muted(self) -> None:
        for s in list(self._sessions):
            if s.muted:
                self.close_session(s, 1001, "FEED_DISCONNECT")

    def restart_epoch(self) -> str:
        """``SIM_RESTART``: new epoch, empty ring, seq from 1, every connection dropped."""
        self.epoch = new_epoch()
        self._ring.clear()
        self.head_seq = 0
        self._reorder.clear()
        for s in list(self._sessions):
            s.live_from = 0
            self.close_session(s, 1012, "SIM_RESTART")
        logger.warning("feed A restarted with new epoch {}", self.epoch)
        return self.epoch

    def flush_reorder(self) -> None:
        if self._reorder:
            items, self._reorder = self._reorder, []
            self._fan_out(items)

    # ------------------------------------------------------------------ publishing
    def publish(self, states: Sequence[InstrumentState]) -> None:
        """Assign seqs to the round's changed states, store them in the ring and fan out."""
        items: list[_Entry] = []
        for st in states:
            iid = st.instrument_id
            if self._faults.feed_frozen(FEED_A, iid):
                continue
            bad = self._faults.take_bad_tick(FEED_A, iid)
            if bad is not None:
                items.append(self._append(self._bad_tick(bad, st)))
            items.append(self._append(feed_a_tick(st)))
            self._last_state[iid] = st.copy()
        if items:
            self._fan_out(self._wire_faults(items))

    def _bad_tick(self, fault: Fault, st: InstrumentState) -> dict[str, Any]:
        base = self._last_state.get(st.instrument_id, st)
        ts = max(base.ts_ns + 1, st.ts_ns - NS_PER_MS)  # between the previous and the current state
        bad = corrupted_state(base, fault.params["offset_pct"], fault.params["with_volume"], ts)
        fault.detail["feed_a_seq"] = self.head_seq + 1
        return feed_a_tick(bad)

    def _append(self, d: dict[str, Any]) -> _Entry:
        self.head_seq += 1
        d_json = dumps(d)
        iid = str(d["InstrumentID"])
        self._ring.append((self.head_seq, iid, d_json))
        self._views[iid] = d
        self.published += 1
        return self.head_seq, iid, _tick_text(self.head_seq, False, d_json)

    def _wire_faults(self, items: list[_Entry]) -> list[_Entry]:
        out = items
        gap = self._faults.first("FEED_GAP", FEED_A)
        if gap is not None:
            left = gap.params["drop_count"] - gap.detail.get("dropped", 0)
            k = min(left, len(out))
            if k > 0:
                gap.detail["dropped"] = gap.detail.get("dropped", 0) + k
                gap.detail.setdefault("dropped_seqs", []).extend(seq for seq, _, _ in out[:k])
                self.dropped += k
                out = out[k:]
            if gap.detail.get("dropped", 0) >= gap.params["drop_count"]:
                self._faults.finish(gap)
        dup = self._faults.first("FEED_DUPLICATE", FEED_A)
        if dup is not None:
            doubled: list[_Entry] = []
            for item in out:
                doubled.append(item)
                if dup.rng.random() < dup.params["ratio"]:
                    doubled.append(item)
                    dup.detail["duplicated"] = dup.detail.get("duplicated", 0) + 1
            out = doubled
        reorder = self._faults.first("FEED_REORDER", FEED_A)
        if reorder is not None:
            if not self._reorder:
                self._reorder_since = time.monotonic()
            self._reorder.extend(out)
            if len(self._reorder) < reorder.params["window"]:
                return []
            out, self._reorder = self._reorder, []
            reorder.rng.shuffle(out)
            reorder.detail["shuffled"] = reorder.detail.get("shuffled", 0) + len(out)
        elif self._reorder:  # fault ended: release what was held, in order
            out, self._reorder = self._reorder + out, []
        return out

    def _fan_out(self, items: list[_Entry]) -> None:
        if not items:
            return
        for s in list(self._sessions):
            if not s.muted:
                s.put_live(items)

    # ------------------------------------------------------------------ connections
    def _process_request(self, connection: ServerConnection, request: Request) -> Response | None:
        path = request.path.split("?", 1)[0]
        if path != "/feed":
            return connection.respond(HTTPStatus.NOT_FOUND, "unknown path; Feed A is served at /feed\n")
        if self._faults.first("FEED_DISCONNECT", FEED_A) is not None:
            return connection.respond(HTTPStatus.SERVICE_UNAVAILABLE, "feed A disconnected (fault injection)\n")
        return None

    async def _handler(self, conn: ServerConnection) -> None:
        s = _Session(self, conn)
        self._sessions.add(s)
        s.sender = asyncio.create_task(self._sender(s), name="feed-a-sender")
        try:
            async for raw in conn:
                self._on_message(s, raw)
        except ConnectionClosed:
            pass
        finally:
            self._sessions.discard(s)
            s.queue.put_nowait(None)
            s.sender.cancel()
            await asyncio.gather(s.sender, return_exceptions=True)

    async def _sender(self, s: _Session) -> None:
        """Drain the session queue; interleave chunks of in-connection replays with it."""
        try:
            while True:
                if s.replays and s.queue.empty():
                    await self._replay_chunk(s)
                    continue
                item = await s.queue.get()
                if item is None:
                    return
                kind, payload, due = item
                if due:
                    delay = due - time.monotonic()
                    if delay > 0:
                        await asyncio.sleep(delay)
                if s.muted:
                    s.replays.clear()
                    continue
                if kind == "text":
                    await s.conn.send(payload)
                elif kind == "resume":
                    await self._send_resume_replay(s, payload)
                # kind == "wake": only makes the loop look at s.replays
                if s.replays:
                    await self._replay_chunk(s)
        except ConnectionClosed:
            return
        except Exception:  # never leave a connection open but silent
            logger.exception("feed A sender crashed; closing the connection")
            self.close_session(s, 1011, "internal error")

    def _heartbeat_text(self) -> str:
        return dumps({"op": "heartbeat", "ts": self._clock.now_ns() // NS_PER_MS, "head_seq": self.head_seq})

    async def _send_resume_replay(self, s: _Session, job: tuple[Any, int, int, list[_Entry]]) -> None:
        """Resume replay: a closed block before any live data, heartbeats every ``hb_ms`` inside."""
        req, lo, hi, entries = job
        await s.conn.send(dumps({"op": "replay_begin", "req": req, "from_seq": lo, "to_seq": hi}))
        hb_s = self.hb_ms / 1000.0
        last_hb = time.monotonic()
        for seq, iid, d_json in entries:
            if s.muted:
                return
            if s.wants(iid):
                await s.conn.send(_tick_text(seq, True, d_json))
            now = time.monotonic()
            if now - last_hb >= hb_s:
                await s.conn.send(self._heartbeat_text())
                last_hb = now
        await s.conn.send(dumps({"op": "replay_end", "req": req, "to_seq": hi}))

    async def _replay_chunk(self, s: _Session) -> None:
        """Send the next chunk of the oldest in-connection replay (02 §9.1 连接内补洞)."""
        job = s.replays[0]
        if s.muted:
            s.replays.clear()
            return
        if not job.begun:
            job.begun = True
            await s.conn.send(dumps({"op": "replay_begin", "req": job.req, "from_seq": job.from_seq,
                                     "to_seq": job.to_seq}))
        end = min(job.pos + REPLAY_CHUNK, len(job.entries))
        for seq, iid, d_json in job.entries[job.pos:end]:
            if s.wants(iid):
                await s.conn.send(_tick_text(seq, True, d_json))
        job.pos = end
        if job.pos >= len(job.entries):
            await s.conn.send(dumps({"op": "replay_end", "req": job.req, "to_seq": job.to_seq}))
            if s.replays and s.replays[0] is job:
                s.replays.popleft()

    def _slice(self, lo: int, hi: int) -> list[_Entry]:
        low = self.ring_low
        lo = max(lo, low)
        if hi < lo:
            return []
        return list(itertools.islice(self._ring, lo - low, hi - low + 1))

    def _error(self, s: _Session, req: Any, code: str, message: str, **extra: Any) -> None:
        s.put_text(dumps({"op": "error", "req": req, "code": code, "message": message, **extra}))

    def _on_message(self, s: _Session, raw: str | bytes) -> None:
        if isinstance(raw, bytes):
            self._error(s, None, "BAD_REQUEST", "binary frames are not supported; send JSON text")
            return
        try:
            msg = json.loads(raw)
        except ValueError:
            self._error(s, None, "BAD_REQUEST", "invalid JSON")
            return
        if not isinstance(msg, dict):
            self._error(s, None, "BAD_REQUEST", "message must be a JSON object")
            return
        req = msg.get("req")
        op = msg.get("op")
        if op == "subscribe":
            self._on_subscribe(s, msg, req)
        elif op == "replay":
            self._on_replay(s, msg, req)
        else:
            self._error(s, req, "BAD_REQUEST", f"unknown op {op!r}; expected subscribe or replay")

    def _snapshot(self, s: _Session) -> None:
        ticks = [d for iid, d in self._views.items() if s.wants(iid)]
        s.put_text(dumps({"op": "snapshot", "as_of_seq": self.head_seq, "ticks": ticks}))

    def _trading_day(self) -> str:
        try:
            return td_str(self._calendar.current_or_next_trading_day(self._clock.now_ns()))
        except LookupError:
            return ""

    def _reject_resume(self, s: _Session, req: Any, reason: str, low: int, head: int) -> None:
        """``resume_rejected`` followed by a snapshot (all three reasons, 02 §9.1)."""
        s.put_text(dumps({"op": "resume_rejected", "req": req, "reason": reason, "ring_low": low, "head_seq": head}))
        self._snapshot(s)

    def _on_subscribe(self, s: _Session, msg: dict[str, Any], req: Any) -> None:
        # runs without awaiting: capturing head_seq and going live is atomic w.r.t. publish()
        if s.subscribed:
            self._error(s, req, "ALREADY_SUBSCRIBED", "this connection is already subscribed")
            return
        inst = msg.get("instruments", "*")
        if inst is None or inst == "*":
            s.instruments = None
        elif isinstance(inst, list) and all(isinstance(x, str) for x in inst):
            s.instruments = frozenset(inst)
        else:
            self._error(s, req, "BAD_REQUEST", 'instruments must be "*" or a list of instrument ids')
            return
        s.subscribed = True
        head, low = self.head_seq, self.ring_low
        s.put_text(dumps({
            "op": "welcome", "req": req, "epoch": self.epoch, "head_seq": head, "ring_low": low,
            "hb_ms": self.hb_ms, "trading_day": self._trading_day(),
        }))
        resume = msg.get("resume")
        from_seq = _seq_arg(resume.get("from_seq")) if isinstance(resume, dict) else None
        if resume is None:
            self._snapshot(s)
        elif not (isinstance(resume, dict) and isinstance(resume.get("epoch"), str) and from_seq is not None):
            self._error(s, req, "BAD_REQUEST", "resume must be null or {epoch: str, from_seq: int}")
            self._snapshot(s)
        elif resume["epoch"] != self.epoch:
            self._reject_resume(s, req, "EPOCH_CHANGED", low, head)
        elif from_seq is None or from_seq < 1 or from_seq > head + 1:
            self._reject_resume(s, req, "BAD_RESUME", low, head)
        elif from_seq < low:
            self._reject_resume(s, req, "SEQ_EVICTED", low, head)
        else:
            s.put("resume", (req, from_seq, head, self._slice(from_seq, head)))
        s.live_from = head + 1

    def _on_replay(self, s: _Session, msg: dict[str, Any], req: Any) -> None:
        if not s.subscribed:
            self._error(s, req, "NOT_SUBSCRIBED", "subscribe before requesting a replay")
            return
        f, t = _seq_arg(msg.get("from_seq")), _seq_arg(msg.get("to_seq"))
        if f is None or t is None or f < 1 or f > t:
            self._error(s, req, "BAD_REQUEST", "replay needs integers 1 <= from_seq <= to_seq")
            return
        low, head = self.ring_low, self.head_seq
        if t < low:
            self._error(s, req, "SEQ_EVICTED", f"seq {f}-{t} no longer in the ring (ring_low={low})",
                        ring_low=low, head_seq=head)
            return
        if f > head:
            self._error(s, req, "BAD_REQUEST", f"from_seq {f} is after head_seq {head}: nothing to replay yet",
                        ring_low=low, head_seq=head)
            return
        lo, hi = max(f, low), min(t, head)
        s.add_replay(_Replay(req, lo, hi, self._slice(lo, hi)))

    async def _heartbeats(self) -> None:
        while True:
            await asyncio.sleep(self.hb_ms / 1000.0)
            text = self._heartbeat_text()
            for s in list(self._sessions):
                if s.subscribed and not s.muted:
                    s.put_text(text)
            if self._reorder and time.monotonic() - self._reorder_since > REORDER_MAX_HOLD_S:
                fault = self._faults.first("FEED_REORDER", FEED_A)
                items, self._reorder = self._reorder, []
                if fault is not None:
                    fault.rng.shuffle(items)
                self._fan_out(items)

    def state(self) -> dict[str, Any]:
        return {
            "url": self.url,
            "epoch": self.epoch,
            "head_seq": self.head_seq,
            "ring_low": self.ring_low,
            "ring_size": self.ring_size,
            "clients": self.clients,
            "published": self.published,
            "dropped": self.dropped,
        }


__all__ = ["FeedAServer"]
