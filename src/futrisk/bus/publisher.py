"""Publishers: LV sequence numbers, FACT outbox rows, last-value cache (02 §4.2, §4.5, §4.9, §8.1).

* **LV**: ``publish(topic, body)`` allocates ``seq`` per topic starting at 1
  in every process incarnation (epoch) and sends ``[topic, envelope]``.
* **FACT**: facts are never published directly. They are appended to the
  outbox inside a business transaction; after the commit the database writer
  hands the rows to ``publish_fact_rows`` (see :meth:`Outbox.attach_sink`),
  which sends them with their persisted ``topic_seq`` and ``stream`` and then
  reports the outbox ids back via ``on_fact_published(ids, ns)`` so that they
  are marked published (``ns`` is **wall-clock**: the publishers are built
  with the runtime's wall clock, retention compares against it).
* Every sent envelope is stored in the :class:`LastValueCache` for
  ``ctl.snapshot``; the highest FACT seq *sent* per topic (seeded from
  ``outbox_topics`` at start-up) is the heartbeat's ``fact_heads``.

Hot path (md-gateway ~10k+ ticks/s): a message is two frames sent with
plain-int flags through pyzmq's Cython ``Socket.send`` (:func:`send_pub`);
``send_multipart`` costs twice as much (a type-check pass, a list slice,
``IntFlag`` arithmetic ``SNDMORE | flags`` and the Python-level wrapper for
every frame). Topic bytes are cached per topic.

Two front-ends share :class:`PublisherCore`:

* :class:`PublisherThread` (sync services): owns the PUB socket in its own
  thread and is fed through a queue; every method is thread-safe.
* :class:`AsyncPublisher` (asyncio services): a PUB socket used from the event
  loop thread; ``publish_fact_rows`` may be called from the writer thread and
  hops to the loop with ``call_soon_threadsafe``.
"""

from __future__ import annotations

import asyncio
import queue
import threading
from collections.abc import Callable, Sequence
from typing import Any

import zmq
from loguru import logger
from zmq.backend import Socket as _BackendSocket

from futrisk.bus.lvcache import LastValueCache
from futrisk.bus.sockets import make_pub
from futrisk.common.clock import Clock, SystemClock
from futrisk.db.outbox import OutboxRow, row_envelope
from futrisk.messages.base import Msg, encode, type_name, type_version
from futrisk.messages.envelope import PROTO, Envelope, encode_envelope

Frames = tuple[bytes, bytes]
FactPublishedCallback = Callable[[list[int], int], None]

_STOP = ("stop",)
_LV = "lv"  # queue item kinds of PublisherThread (compared by identity)
_FACT = "fact"
_FLUSH = "flush"
_SNDMORE = int(zmq.SNDMORE)
_send_frame = _BackendSocket.send  # the Cython send: no Python wrapper, no enum arithmetic
_TOPIC_CACHE_MAX = 100_000


def send_pub(sock: zmq.Socket[Any], topic: bytes, data: bytes) -> None:
    """Send one ``[topic, envelope]`` PUB message (a PUB send never blocks: beyond the HWM it drops)."""
    _send_frame(sock, topic, _SNDMORE)
    _send_frame(sock, data, 0)


class PublisherCore:
    """Thread-safe sequence allocation, envelope building, LV cache and FACT heads."""

    def __init__(self, instance: str, epoch: str, clock: Clock | None = None) -> None:
        self.instance = instance
        self.epoch = epoch
        self._clock = clock or SystemClock()
        self._lock = threading.Lock()
        self._lv_seq: dict[str, int] = {}
        self._heads: dict[str, int] = {}
        self._topic_bytes: dict[str, bytes] = {}
        self.cache = LastValueCache()
        self.sent_lv = 0
        self.sent_fact = 0

    def lv_frames(self, topic: str, body: Msg, *, td: str | None = None, corr_id: str | None = None) -> tuple[Frames, int]:
        name = type_name(body)
        return self.lv_frames_raw(topic, name, type_version(name), encode(body), td=td, corr_id=corr_id)

    def lv_frames_raw(
        self,
        topic: str,
        type_: str,
        v: int,
        payload: bytes,
        *,
        td: str | None = None,
        corr_id: str | None = None,
    ) -> tuple[Frames, int]:
        """Allocate the next LV seq of ``topic`` and build ``(topic, envelope)`` frames."""
        with self._lock:
            seq = self._lv_seq.get(topic, 0) + 1
            self._lv_seq[topic] = seq
            env = Envelope(
                proto=PROTO,
                topic=topic,
                type=type_,
                v=v,
                src=self.instance,
                epoch=self.epoch,
                stream=None,
                seq=seq,
                ts_ns=self._clock.now_ns(),
                td=td,
                corr_id=corr_id,
                payload=payload,
            )
            data = encode_envelope(env)
            self.cache.put(topic, seq, self.epoch, data, fact=False)
            tb = self._topic_bytes.get(topic)
            if tb is None:
                tb = self._topic_key(topic)
        return (tb, data), seq

    def _topic_key(self, topic: str) -> bytes:
        """``topic`` as bytes, cached (lock held)."""
        if len(self._topic_bytes) >= _TOPIC_CACHE_MAX:
            self._topic_bytes.clear()
        tb = self._topic_bytes[topic] = topic.encode("utf-8")
        return tb

    def fact_frames(self, rows: Sequence[OutboxRow]) -> list[Frames]:
        """Frames for committed outbox rows (updates the LV cache; heads move after sending)."""
        out: list[Frames] = []
        with self._lock:
            for row in rows:
                data = encode_envelope(row_envelope(row, src=self.instance))
                self.cache.put(row.topic, row.topic_seq, row.epoch, data, fact=True)
                tb = self._topic_bytes.get(row.topic)
                out.append((tb if tb is not None else self._topic_key(row.topic), data))
        return out

    def seed_facts(self, rows: Sequence[OutboxRow]) -> int:
        """Put the newest stored row of each FACT topic in the LV cache (after a restart).

        ``ctl.snapshot`` then still returns the latest state of FACT topics
        such as ``risk.state.ACC3`` before anything new is published. Entries
        already cached with a higher seq are kept. Returns rows seeded.
        """
        n = 0
        with self._lock:
            for row in rows:
                cached = self.cache.get(row.topic)
                if cached is not None and cached.seq >= row.topic_seq:
                    continue
                data = encode_envelope(row_envelope(row, src=self.instance))
                self.cache.put(row.topic, row.topic_seq, row.epoch, data, fact=True)
                n += 1
        return n

    def advance_heads(self, heads: Sequence[tuple[str, int]]) -> None:
        """Record FACT seqs that reached the socket (so heartbeats never announce unsent facts)."""
        with self._lock:
            for topic, seq in heads:
                if seq > self._heads.get(topic, 0):
                    self._heads[topic] = seq

    def heads(self) -> dict[str, int]:
        """Highest FACT seq handed to the socket per topic (heartbeat ``fact_heads``)."""
        with self._lock:
            return dict(self._heads)

    def set_heads(self, heads: dict[str, int]) -> None:
        """Seed heads from ``outbox_topics`` at start-up (keeps the max)."""
        with self._lock:
            for topic, seq in heads.items():
                if seq > self._heads.get(topic, 0):
                    self._heads[topic] = seq

    def lv_seq(self, topic: str) -> int:
        with self._lock:
            return self._lv_seq.get(topic, 0)


class PublisherThread:
    """PUB socket owned by a dedicated thread, fed by a queue (sync services, 02 §8.1)."""

    def __init__(
        self,
        ctx: zmq.Context[Any],
        address: str,
        core: PublisherCore,
        *,
        sndhwm: int = 10_000,
        linger_ms: int = 0,
        on_fact_published: FactPublishedCallback | None = None,
        clock: Clock | None = None,
        name: str = "publisher",
    ) -> None:
        self._ctx = ctx
        self._address = address
        self.core = core
        self._sndhwm = sndhwm
        self._linger_ms = linger_ms
        self._on_fact_published = on_fact_published
        self._clock = clock or SystemClock()
        self._name = name
        self._q: queue.SimpleQueue[tuple[Any, ...]] = queue.SimpleQueue()
        self._order_lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._ready = threading.Event()
        self._error: BaseException | None = None
        self._stopped = False

    @property
    def address(self) -> str:
        return self._address

    def set_fact_published_callback(self, cb: FactPublishedCallback | None) -> None:
        self._on_fact_published = cb

    def start(self, timeout: float = 5.0) -> None:
        """Start the thread and bind; raises the bind error if any."""
        self._thread = threading.Thread(target=self._run, name=self._name, daemon=True)
        self._thread.start()
        if not self._ready.wait(timeout):
            raise TimeoutError(f"publisher {self._address} did not start")
        if self._error is not None:
            raise self._error

    def publish(self, topic: str, body: Msg, *, td: str | None = None, corr_id: str | None = None) -> int:
        """Publish an LV message; returns its seq. Thread-safe."""
        with self._order_lock:
            frames, seq = self.core.lv_frames(topic, body, td=td, corr_id=corr_id)
            self._q.put((_LV, frames[0], frames[1]))
        return seq

    def publish_raw(
        self, topic: str, type_: str, v: int, payload: bytes, *, td: str | None = None, corr_id: str | None = None
    ) -> int:
        with self._order_lock:
            frames, seq = self.core.lv_frames_raw(topic, type_, v, payload, td=td, corr_id=corr_id)
            self._q.put((_LV, frames[0], frames[1]))
        return seq

    def publish_fact_rows(self, rows: Sequence[OutboxRow]) -> None:
        """Send committed outbox rows (called by the DB writer after commit). Thread-safe."""
        if not rows:
            return
        with self._order_lock:
            frames = self.core.fact_frames(rows)
            self._q.put((_FACT, frames, [r.outbox_id for r in rows], [(r.topic, r.topic_seq) for r in rows]))

    def flush(self, timeout: float = 5.0) -> bool:
        """Wait until everything queued so far has been handed to the socket."""
        if self._thread is None or not self._thread.is_alive():
            return True
        ev = threading.Event()
        self._q.put((_FLUSH, ev))
        return ev.wait(timeout)

    def queue_size(self) -> int:
        return self._q.qsize()

    def stop(self, timeout: float = 5.0) -> None:
        if self._thread is None or self._stopped:
            return
        self._stopped = True
        self._q.put(_STOP)
        self._thread.join(timeout)
        if self._thread.is_alive():
            logger.error("publisher.stuck", address=self._address)

    # --------------------------------------------------------------- thread
    def _run(self) -> None:
        try:
            sock = make_pub(self._ctx, self._address, sndhwm=self._sndhwm, linger_ms=self._linger_ms)
        except BaseException as exc:
            self._error = exc
            self._ready.set()
            return
        self._ready.set()
        get = self._q.get
        core = self.core
        try:
            while True:
                item = get()
                if item[0] is _LV:  # the hot path, inline
                    try:
                        send_pub(sock, item[1], item[2])
                        core.sent_lv += 1
                    except zmq.ZMQError:
                        logger.exception("publisher.send_failed", address=self._address)
                    continue
                if item is _STOP or item[0] == "stop":
                    break
                self._handle(sock, item)
        finally:
            sock.close(self._linger_ms)

    def _handle(self, sock: zmq.Socket[Any], item: tuple[Any, ...]) -> None:
        kind = item[0]
        try:
            if kind is _LV:
                send_pub(sock, item[1], item[2])
                self.core.sent_lv += 1
            elif kind is _FACT:
                for topic, data in item[1]:
                    send_pub(sock, topic, data)
                self.core.sent_fact += len(item[1])
                self.core.advance_heads(item[3])
                cb = self._on_fact_published
                if cb is not None:
                    cb(list(item[2]), self._clock.now_ns())
            elif kind is _FLUSH:
                item[1].set()
        except zmq.ZMQError:
            logger.exception("publisher.send_failed", address=self._address)
        except Exception:
            logger.exception("publisher.callback_failed", address=self._address)


class AsyncPublisher:
    """PUB socket used from an asyncio loop (PUB sends never block, so no awaiting)."""

    def __init__(
        self,
        ctx: zmq.Context[Any],
        address: str,
        core: PublisherCore,
        *,
        sndhwm: int = 10_000,
        linger_ms: int = 0,
        on_fact_published: FactPublishedCallback | None = None,
        clock: Clock | None = None,
    ) -> None:
        self._ctx = ctx
        self._address = address
        self.core = core
        self._sndhwm = sndhwm
        self._linger_ms = linger_ms
        self._on_fact_published = on_fact_published
        self._clock = clock or SystemClock()
        self._sock: zmq.Socket[Any] | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._loop_thread: int | None = None

    @property
    def address(self) -> str:
        return self._address

    def set_fact_published_callback(self, cb: FactPublishedCallback | None) -> None:
        self._on_fact_published = cb

    def start(self, loop: asyncio.AbstractEventLoop | None = None) -> None:
        """Bind in the loop thread."""
        self._loop = loop or asyncio.get_running_loop()
        self._loop_thread = threading.get_ident()
        self._sock = make_pub(self._ctx, self._address, sndhwm=self._sndhwm, linger_ms=self._linger_ms)

    def publish(self, topic: str, body: Msg, *, td: str | None = None, corr_id: str | None = None) -> int:
        """Publish an LV message; returns its seq (other threads are marshalled onto the loop)."""
        frames, seq = self.core.lv_frames(topic, body, td=td, corr_id=corr_id)
        self._send_lv(frames)
        return seq

    def publish_raw(
        self, topic: str, type_: str, v: int, payload: bytes, *, td: str | None = None, corr_id: str | None = None
    ) -> int:
        frames, seq = self.core.lv_frames_raw(topic, type_, v, payload, td=td, corr_id=corr_id)
        self._send_lv(frames)
        return seq

    def _send_lv(self, frames: Frames) -> None:
        if threading.get_ident() == self._loop_thread:
            self._send([frames])
            self.core.sent_lv += 1
            return
        loop = self._loop
        if loop is None:
            raise RuntimeError(f"publisher {self._address} not started")
        try:
            loop.call_soon_threadsafe(self._send, [frames])
        except RuntimeError:
            logger.warning("publisher.loop_closed", address=self._address)

    def publish_fact_rows(self, rows: Sequence[OutboxRow]) -> None:
        """Send committed outbox rows; callable from any thread (order preserved)."""
        if not rows:
            return
        frames = self.core.fact_frames(rows)
        ids = [r.outbox_id for r in rows]
        heads = [(r.topic, r.topic_seq) for r in rows]
        if threading.get_ident() == self._loop_thread:
            self._send_fact(frames, ids, heads)
            return
        loop = self._loop
        if loop is None:
            logger.warning("publisher.not_started", address=self._address)
            return
        try:
            loop.call_soon_threadsafe(self._send_fact, frames, ids, heads)
        except RuntimeError:
            logger.warning("publisher.loop_closed", address=self._address, rows=len(ids))

    def _send_fact(self, frames: list[Frames], ids: list[int], heads: list[tuple[str, int]]) -> None:
        if self._sock is None:
            return
        self._send(frames)
        self.core.sent_fact += len(frames)
        self.core.advance_heads(heads)
        cb = self._on_fact_published
        if cb is not None:
            try:
                cb(ids, self._clock.now_ns())
            except Exception:
                logger.exception("publisher.callback_failed", address=self._address)

    def _send(self, frames_list: list[Frames]) -> None:
        sock = self._sock
        if sock is None:
            raise RuntimeError(f"publisher {self._address} not started")
        try:
            for topic, data in frames_list:
                send_pub(sock, topic, data)
        except zmq.ZMQError:
            logger.exception("publisher.send_failed", address=self._address)

    def flush(self, timeout: float = 5.0) -> bool:
        return True

    def queue_size(self) -> int:
        return 0

    def close(self) -> None:
        if self._sock is not None:
            self._sock.close(self._linger_ms)
            self._sock = None


__all__ = ["AsyncPublisher", "FactPublishedCallback", "PublisherCore", "PublisherThread", "send_pub"]
