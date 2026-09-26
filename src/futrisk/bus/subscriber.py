"""Subscribers: prefix subscriptions + exact topic filtering (02 §4.2, §4.4).

ZeroMQ filters by prefix, so ``td.exec.ACC1`` also receives ``td.exec.ACC10``.
Every received message is therefore checked again:

1. two frames, frame 1 (UTF-8 topic) equal to ``Envelope.topic``;
2. ``Envelope.proto`` supported;
3. the ``(src, topic)`` pair matches one of the subscriptions registered for
   that publisher with :func:`futrisk.messages.topics.matches`.

Messages failing a check are dropped and counted in :class:`SubStats`.

:class:`Subscriber` is the sync variant (register ``.socket`` in a
``zmq.Poller`` and call :meth:`Subscriber.drain`); :class:`AsyncSubscriber`
awaits messages in an asyncio loop.

Hot path: :func:`recv_frames` is ``recv_multipart(NOBLOCK)`` on pyzmq's
Cython ``recv``/``get`` with plain-int flags (no Python wrapper, no enum
conversion), and :class:`TopicFilter` caches its answer per ``(src, topic)``.
"""

from __future__ import annotations

import time
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

import msgspec
import zmq
import zmq.asyncio
from loguru import logger
from zmq.backend import Socket as _BackendSocket

from futrisk.bus.sockets import DEFAULT_SUB_HWM, make_sub
from futrisk.messages.envelope import PROTO, Envelope, decode_envelope
from futrisk.messages.topics import matches

_recv_frame = _BackendSocket.recv
_get_option = _BackendSocket.get
_NOBLOCK = int(zmq.NOBLOCK)
_RCVMORE = int(zmq.RCVMORE)
_ACCEPT_CACHE_MAX = 100_000


def recv_frames(sock: zmq.Socket[Any]) -> list[bytes]:
    """``sock.recv_multipart(zmq.NOBLOCK)`` without pyzmq's Python layer; raises ``zmq.Again`` when empty."""
    frames: list[bytes] = [_recv_frame(sock, _NOBLOCK)]
    while _get_option(sock, _RCVMORE):
        frames.append(_recv_frame(sock, _NOBLOCK))
    return frames


@dataclass
class SubStats:
    received: int = 0
    accepted: int = 0
    topic_mismatch: int = 0
    bad_frames: int = 0
    bad_proto: int = 0
    decode_error: int = 0
    filtered: int = 0


class TopicFilter:
    """Subscriptions per publisher; answers the exact-match question."""

    def __init__(self) -> None:
        self._by_src: dict[str, set[str]] = {}
        self._zmq: dict[str, int] = {}
        self._cache: dict[str, dict[str, bool]] = {}  # src -> topic -> accepted
        self._cached = 0

    def add(self, src: str, subscriptions: Iterable[str]) -> list[str]:
        """Register subscriptions of ``src``; returns zmq prefixes that are new for the socket."""
        subs = self._by_src.setdefault(src, set())
        self._cache.clear()
        self._cached = 0
        new: list[str] = []
        for s in subscriptions:
            if not s:
                raise ValueError("empty subscription (would receive every topic)")
            if s in subs:
                continue
            subs.add(s)
            count = self._zmq.get(s, 0)
            if count == 0:
                new.append(s)
            self._zmq[s] = count + 1
        return new

    def accepts(self, src: str, topic: str) -> bool:
        per_src = self._cache.get(src)
        if per_src is not None:
            hit = per_src.get(topic)
            if hit is not None:
                return hit
        ok = self._decide(src, topic)
        if self._cached >= _ACCEPT_CACHE_MAX:
            self._cache.clear()
            self._cached = 0
        self._cache.setdefault(src, {})[topic] = ok
        self._cached += 1
        return ok

    def _decide(self, src: str, topic: str) -> bool:
        subs = self._by_src.get(src)
        if not subs:
            return False
        if topic in subs:
            return True
        return any(matches(topic, s) for s in subs if s.endswith("."))

    def publishers(self) -> list[str]:
        return sorted(self._by_src)

    def subscriptions(self, src: str) -> set[str]:
        return set(self._by_src.get(src, ()))


def parse_pub_frames(frames: list[bytes], flt: TopicFilter, stats: SubStats) -> Envelope | None:
    """Validate one PUB message; returns the envelope or ``None`` (dropped and counted)."""
    stats.received += 1
    if len(frames) != 2:
        stats.bad_frames += 1
        return None
    try:
        env = decode_envelope(frames[1])
    except (msgspec.DecodeError, msgspec.ValidationError):
        stats.decode_error += 1
        return None
    if env.proto != PROTO:
        stats.bad_proto += 1
        return None
    try:
        topic = frames[0].decode("utf-8")
    except UnicodeDecodeError:
        stats.bad_frames += 1
        return None
    if topic != env.topic:
        stats.topic_mismatch += 1
        return None
    if not flt.accepts(env.src, env.topic):
        stats.filtered += 1
        return None
    stats.accepted += 1
    return env


class Subscriber:
    """Sync SUB socket connected to one or more publishers."""

    def __init__(self, ctx: zmq.Context[Any], *, rcvhwm: int = DEFAULT_SUB_HWM, name: str = "sub") -> None:
        self.name = name
        self.socket: zmq.Socket[Any] = make_sub(ctx, rcvhwm=rcvhwm)
        self.filter = TopicFilter()
        self.stats = SubStats()
        self._connected: dict[str, str] = {}

    def connect(self, publisher: str, address: str, subscriptions: Iterable[str]) -> None:
        """Connect to ``publisher`` at ``address`` (once) and subscribe ``subscriptions``."""
        if publisher not in self._connected:
            self.socket.connect(address)
            self._connected[publisher] = address
        for prefix in self.filter.add(publisher, subscriptions):
            self.socket.setsockopt(zmq.SUBSCRIBE, prefix.encode("utf-8"))

    @property
    def publishers(self) -> dict[str, str]:
        return dict(self._connected)

    def drain(self, limit: int | None = None) -> list[Envelope]:
        """Read every pending message without blocking (at most ``limit``)."""
        out: list[Envelope] = []
        while limit is None or len(out) < limit:
            try:
                frames = recv_frames(self.socket)
            except zmq.Again:
                break
            env = parse_pub_frames(frames, self.filter, self.stats)
            if env is not None:
                out.append(env)
        return out

    def recv(self, timeout_ms: int) -> Envelope | None:
        """Block up to ``timeout_ms`` for the next accepted envelope (tools/tests)."""
        poller = zmq.Poller()
        poller.register(self.socket, zmq.POLLIN)
        remaining = timeout_ms
        deadline = time.monotonic() + timeout_ms / 1000
        while remaining >= 0:
            if not poller.poll(max(0, remaining)):
                return None
            got = self.drain(limit=1)
            if got:
                return got[0]
            remaining = int((deadline - time.monotonic()) * 1000)
        return None

    def close(self) -> None:
        self.socket.close(0)


class AsyncSubscriber:
    """SUB socket for asyncio services."""

    def __init__(self, ctx: zmq.asyncio.Context, *, rcvhwm: int = DEFAULT_SUB_HWM, name: str = "sub") -> None:
        self.name = name
        self.socket: zmq.asyncio.Socket = make_sub(ctx, rcvhwm=rcvhwm)  # type: ignore[assignment]
        self.filter = TopicFilter()
        self.stats = SubStats()
        self._connected: dict[str, str] = {}

    def connect(self, publisher: str, address: str, subscriptions: Iterable[str]) -> None:
        if publisher not in self._connected:
            self.socket.connect(address)
            self._connected[publisher] = address
        for prefix in self.filter.add(publisher, subscriptions):
            self.socket.setsockopt(zmq.SUBSCRIBE, prefix.encode("utf-8"))

    @property
    def publishers(self) -> dict[str, str]:
        return dict(self._connected)

    async def recv(self) -> Envelope:
        """Wait for the next accepted envelope."""
        while True:
            frames = await self.socket.recv_multipart()
            env = parse_pub_frames(list(frames), self.filter, self.stats)
            if env is not None:
                return env

    def close(self) -> None:
        try:
            self.socket.close(0)
        except Exception:  # pragma: no cover - closing twice
            logger.debug("sub.close_failed", name=self.name)


__all__ = ["AsyncSubscriber", "SubStats", "Subscriber", "TopicFilter", "parse_pub_frames", "recv_frames"]
