"""DEALER command clients with the 02 §4.7 retry policy.

* One DEALER per target, connected to the target's ROUTER, ``IDENTITY`` = own
  instance id (the server's whitelist key), ``IMMEDIATE=1``: when the peer is
  not connected the send fails at once and the request completes with
  ``UNAVAILABLE`` instead of queueing (a freshly created socket gets a short
  ``connect_grace_ms`` to finish its TCP handshake).
* Retries only for ``TIMEOUT``/``UNAVAILABLE``/``BUSY``/``RISK_BUSY``, at most
  3, delay ``100ms * 2**n`` + jitter (or ``retry_after_ms``), resending the
  same payload (same idempotency key) with the same ``corr_id``. ``timeout_ms``
  is per attempt (default from :func:`~futrisk.bus.timeouts.default_timeout_ms`);
  ``deadline_ns`` (monotonic) stops retrying, e.g. at a route's ``expire_ns``.

Three variants:

* :class:`AsyncRpcClient` - ``await client.request(...)`` (asyncio services);
* :class:`BlockingRpcClient` - blocking ``client.request(...)`` (CLI, tools, tests);
* :class:`LoopRpcClient` - non-blocking, for sync poller loops (02 §8.1):
  ``send(...)`` returns the ``corr_id`` immediately; the loop registers
  :meth:`LoopRpcClient.sockets` in its poller, calls ``process_replies()``
  when they are readable and ``poll_timers()`` on every pass (timeouts,
  backoff). Results arrive as ``on_reply`` callbacks or as
  :class:`Completion` records (carrying the caller's ``context``).
"""

from __future__ import annotations

import asyncio
import contextlib
import random
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import msgspec
import zmq
import zmq.asyncio
from loguru import logger

from futrisk.bus.endpoints import Endpoints
from futrisk.bus.rpc_common import request_envelope, timeout, unavailable
from futrisk.bus.sockets import make_dealer
from futrisk.bus.timeouts import MAX_RETRIES, default_timeout_ms, is_retryable, retry_delay_ms
from futrisk.common.clock import Clock, SystemClock
from futrisk.common.ids import new_epoch, uuid7
from futrisk.messages.base import Msg, decode
from futrisk.messages.envelope import Envelope, Reply, decode_envelope

Resolver = Endpoints | Callable[[str], str]
ReplyCallback = Callable[[Reply], None]
DEFAULT_CONNECT_GRACE_MS = 300
_GRACE_RETRY_S = 0.005
_SEND_GUARD_S = 0.5
_NS = 1_000_000


def _resolver(resolve: Resolver) -> Callable[[str], str]:
    if isinstance(resolve, Endpoints):
        return resolve.router
    return resolve


def _decode_reply(frames: list[bytes]) -> tuple[str | None, Reply] | None:
    """DEALER side: ``[envelope]`` -> ``(corr_id, Reply)``."""
    if not frames:
        return None
    try:
        env: Envelope = decode_envelope(frames[-1])
        return env.corr_id, decode(env.payload, Reply)
    except (msgspec.DecodeError, msgspec.ValidationError):
        logger.warning("rpc.bad_reply")
        return None


@dataclass(frozen=True)
class Completion:
    """A finished request of :class:`LoopRpcClient`."""

    corr_id: str
    target: str
    command: str
    context: Any
    reply: Reply
    attempts: int


# ----------------------------------------------------------------- asyncio
@dataclass
class _AsyncPeer:
    sock: zmq.asyncio.Socket
    created_ns: int
    reader: asyncio.Task[None] | None = None


class AsyncRpcClient:
    """Request/reply client for asyncio services."""

    def __init__(
        self,
        ctx: zmq.asyncio.Context,
        resolve: Resolver,
        *,
        identity: str,
        epoch: str | None = None,
        clock: Clock | None = None,
        connect_grace_ms: int = DEFAULT_CONNECT_GRACE_MS,
        rng: random.Random | None = None,
    ) -> None:
        self._ctx = ctx
        self._resolve = _resolver(resolve)
        self.identity = identity
        self.epoch = epoch or new_epoch()
        self._clock = clock or SystemClock()
        self._grace_ns = connect_grace_ms * _NS
        self._rng = rng or random.Random()
        self._peers: dict[str, _AsyncPeer] = {}
        self._pending: dict[str, asyncio.Future[Reply]] = {}
        self._closed = False

    def connect(self, target: str) -> None:
        """Create the DEALER for ``target`` ahead of the first request."""
        self._peer(target)

    def _peer(self, target: str) -> _AsyncPeer:
        peer = self._peers.get(target)
        if peer is None:
            addr = self._resolve(target)
            sock: zmq.asyncio.Socket = make_dealer(self._ctx, addr, identity=self.identity)  # type: ignore[assignment]
            peer = _AsyncPeer(sock, self._clock.monotonic_ns())
            peer.reader = asyncio.create_task(self._reader(target, sock), name=f"rpc-client-{target}")
            self._peers[target] = peer
        return peer

    async def _reader(self, target: str, sock: zmq.asyncio.Socket) -> None:
        while True:
            frames = await sock.recv_multipart()
            got = _decode_reply(list(frames))
            if got is None:
                continue
            corr, reply = got
            fut = self._pending.get(corr) if corr is not None else None
            if fut is not None and not fut.done():
                fut.set_result(reply)
            else:
                logger.debug("rpc.late_reply", target=target, corr_id=corr)

    async def request(
        self,
        target: str,
        command: str,
        payload: Msg,
        *,
        timeout_ms: int | None = None,
        retries: int | None = None,
        deadline_ns: int | None = None,
        td: str | None = None,
    ) -> Reply:
        """Send ``command`` to ``target`` and await the reply (with retries)."""
        if self._closed:
            return unavailable(target, "client closed")
        t_ms = timeout_ms if timeout_ms is not None else default_timeout_ms(command)
        max_retries = MAX_RETRIES if retries is None else max(0, retries)
        corr = uuid7()
        attempt = 0
        while True:
            reply = await self._attempt(target, command, payload, corr, t_ms, td)
            if not is_retryable(reply.code) or attempt >= max_retries:
                return reply
            delay_ms = retry_delay_ms(attempt, reply.retry_after_ms, self._rng)
            if deadline_ns is not None and self._clock.monotonic_ns() + int(delay_ms * _NS) >= deadline_ns:
                return reply
            await asyncio.sleep(delay_ms / 1000)
            attempt += 1

    async def _attempt(self, target: str, command: str, payload: Msg, corr: str, t_ms: int, td: str | None) -> Reply:
        try:
            peer = self._peer(target)
        except KeyError as exc:
            return unavailable(target, str(exc))
        data = request_envelope(
            command, payload, src=self.identity, epoch=self.epoch, ts_ns=self._clock.now_ns(), corr_id=corr, td=td
        )
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[Reply] = loop.create_future()
        self._pending[corr] = fut
        try:
            if not await self._send(peer, data):
                return unavailable(target)
            try:
                return await asyncio.wait_for(fut, t_ms / 1000)
            except TimeoutError:
                return timeout(target, command, t_ms)
        finally:
            if self._pending.get(corr) is fut:
                del self._pending[corr]

    async def _send(self, peer: _AsyncPeer, data: bytes) -> bool:
        """Send without ever queueing: False when the peer is not connected (ZMQ_IMMEDIATE).

        No asyncio poller is used on the DEALER: a pending POLLOUT watch would make
        concurrent NOBLOCK sends queue behind it inside pyzmq and wait for a
        connection that may never come (and deliver stale requests later). A
        fresh socket gets ``connect_grace_ms`` of cheap retries instead.
        """
        deadline = peer.created_ns + self._grace_ns
        while True:
            try:
                # NOBLOCK sends complete at once; the timeout only guards against a send queued
                # inside pyzmq by third-party code (cancelling it removes it from the queue).
                await asyncio.wait_for(peer.sock.send(data, flags=zmq.NOBLOCK), _SEND_GUARD_S)
                return True
            except (zmq.Again, TimeoutError):
                if self._clock.monotonic_ns() >= deadline:
                    return False
            await asyncio.sleep(_GRACE_RETRY_S)

    async def close(self) -> None:
        self._closed = True
        for fut in self._pending.values():
            if not fut.done():
                fut.set_result(unavailable("*", "client closed"))
        self._pending.clear()
        for peer in self._peers.values():
            if peer.reader is not None:
                peer.reader.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await peer.reader
            peer.sock.close(0)
        self._peers.clear()


# ---------------------------------------------------------------- blocking
class BlockingRpcClient:
    """Blocking client for CLI, tools and tests (one thread only)."""

    def __init__(
        self,
        resolve: Resolver,
        *,
        identity: str,
        ctx: zmq.Context[Any] | None = None,
        epoch: str | None = None,
        clock: Clock | None = None,
        connect_grace_ms: int = DEFAULT_CONNECT_GRACE_MS,
        rng: random.Random | None = None,
    ) -> None:
        self._own_ctx = ctx is None
        self._ctx: zmq.Context[Any] = ctx if ctx is not None else zmq.Context()
        self._resolve = _resolver(resolve)
        self.identity = identity
        self.epoch = epoch or new_epoch()
        self._clock = clock or SystemClock()
        self._grace_ms = connect_grace_ms
        self._rng = rng or random.Random()
        self._peers: dict[str, tuple[zmq.Socket[Any], float]] = {}

    def _peer(self, target: str) -> tuple[zmq.Socket[Any], float]:
        peer = self._peers.get(target)
        if peer is None:
            sock = make_dealer(self._ctx, self._resolve(target), identity=self.identity)
            peer = (sock, time.monotonic())
            self._peers[target] = peer
        return peer

    def request(
        self,
        target: str,
        command: str,
        payload: Msg,
        *,
        timeout_ms: int | None = None,
        retries: int | None = None,
        td: str | None = None,
    ) -> Reply:
        t_ms = timeout_ms if timeout_ms is not None else default_timeout_ms(command)
        max_retries = MAX_RETRIES if retries is None else max(0, retries)
        corr = uuid7()
        attempt = 0
        while True:
            reply = self._attempt(target, command, payload, corr, t_ms, td)
            if not is_retryable(reply.code) or attempt >= max_retries:
                return reply
            time.sleep(retry_delay_ms(attempt, reply.retry_after_ms, self._rng) / 1000)
            attempt += 1

    def _attempt(self, target: str, command: str, payload: Msg, corr: str, t_ms: int, td: str | None) -> Reply:
        try:
            sock, created = self._peer(target)
        except KeyError as exc:
            return unavailable(target, str(exc))
        data = request_envelope(
            command, payload, src=self.identity, epoch=self.epoch, ts_ns=self._clock.now_ns(), corr_id=corr, td=td
        )
        try:
            sock.send(data, zmq.NOBLOCK)
        except zmq.Again:
            remaining = int(self._grace_ms - (time.monotonic() - created) * 1000)
            if remaining <= 0 or not sock.poll(remaining, zmq.POLLOUT):
                return unavailable(target)
            try:
                sock.send(data, zmq.NOBLOCK)
            except zmq.Again:
                return unavailable(target)
        deadline = time.monotonic() + t_ms / 1000
        while True:
            remaining_ms = int((deadline - time.monotonic()) * 1000)
            if remaining_ms <= 0 or not sock.poll(remaining_ms, zmq.POLLIN):
                return timeout(target, command, t_ms)
            got = _decode_reply(sock.recv_multipart(zmq.NOBLOCK))
            if got is not None and got[0] == corr:
                return got[1]

    def close(self) -> None:
        for sock, _ in self._peers.values():
            sock.close(0)
        self._peers.clear()
        if self._own_ctx:
            self._ctx.term()


# ------------------------------------------------------------ poller loop
@dataclass
class _Pending:
    corr_id: str
    target: str
    command: str
    payload: Msg
    td: str | None
    timeout_ms: int
    retries_left: int
    deadline_ns: int | None
    on_reply: ReplyCallback | None
    context: Any
    attempts: int = 0
    retries_used: int = 0
    state: str = "NEW"  # SENT | BACKOFF | WAIT_CONNECT
    due_ns: int = 0


@dataclass
class _LoopPeer:
    sock: zmq.Socket[Any]
    created_ns: int


class LoopRpcClient:
    """Non-blocking request client for a sync poller loop (02 §8.1)."""

    _CONNECT_POLL_NS = 5 * _NS

    def __init__(
        self,
        ctx: zmq.Context[Any],
        resolve: Resolver,
        *,
        identity: str,
        epoch: str | None = None,
        clock: Clock | None = None,
        connect_grace_ms: int = DEFAULT_CONNECT_GRACE_MS,
        rng: random.Random | None = None,
        on_new_socket: Callable[[zmq.Socket[Any]], None] | None = None,
    ) -> None:
        self._ctx = ctx
        self._resolve = _resolver(resolve)
        self.identity = identity
        self.epoch = epoch or new_epoch()
        self._clock = clock or SystemClock()
        self._grace_ns = connect_grace_ms * _NS
        self._rng = rng or random.Random()
        self._on_new_socket = on_new_socket
        self._peers: dict[str, _LoopPeer] = {}
        self._pending: dict[str, _Pending] = {}
        self._ready: list[tuple[_Pending, Reply]] = []

    # ----------------------------------------------------------- sockets
    def connect(self, target: str) -> zmq.Socket[Any]:
        """Create (or get) the DEALER of ``target``; do it early so it is connected when needed."""
        return self._peer(target).sock

    def _peer(self, target: str) -> _LoopPeer:
        peer = self._peers.get(target)
        if peer is None:
            sock = make_dealer(self._ctx, self._resolve(target), identity=self.identity)
            peer = _LoopPeer(sock, self._clock.monotonic_ns())
            self._peers[target] = peer
            if self._on_new_socket is not None:
                self._on_new_socket(sock)
        return peer

    def sockets(self) -> list[zmq.Socket[Any]]:
        return [p.sock for p in self._peers.values()]

    @property
    def pending_count(self) -> int:
        return len(self._pending)

    # ------------------------------------------------------------ requests
    def send(
        self,
        target: str,
        command: str,
        payload: Msg,
        *,
        timeout_ms: int | None = None,
        retries: int | None = None,
        deadline_ns: int | None = None,
        on_reply: ReplyCallback | None = None,
        context: Any = None,
        td: str | None = None,
    ) -> str:
        """Start a request; returns its ``corr_id``. Never blocks, never calls back synchronously."""
        p = _Pending(
            corr_id=uuid7(),
            target=target,
            command=command,
            payload=payload,
            td=td,
            timeout_ms=timeout_ms if timeout_ms is not None else default_timeout_ms(command),
            retries_left=MAX_RETRIES if retries is None else max(0, retries),
            deadline_ns=deadline_ns,
            on_reply=on_reply,
            context=context,
        )
        self._pending[p.corr_id] = p
        self._try_send(p, self._clock.monotonic_ns())
        return p.corr_id

    def cancel(self, corr_id: str) -> bool:
        """Forget a pending request (its reply will be ignored)."""
        return self._pending.pop(corr_id, None) is not None

    def _try_send(self, p: _Pending, now: int) -> None:
        try:
            peer = self._peer(p.target)
        except KeyError as exc:
            self._fail(p, unavailable(p.target, str(exc)), now)
            return
        data = request_envelope(
            p.command,
            p.payload,
            src=self.identity,
            epoch=self.epoch,
            ts_ns=self._clock.now_ns(),
            corr_id=p.corr_id,
            td=p.td,
        )
        try:
            peer.sock.send(data, zmq.NOBLOCK)
        except zmq.Again:
            if now < peer.created_ns + self._grace_ns:
                p.state = "WAIT_CONNECT"
                p.due_ns = peer.created_ns + self._grace_ns
            else:
                self._fail(p, unavailable(p.target), now)
            return
        p.attempts += 1
        p.state = "SENT"
        p.due_ns = now + p.timeout_ms * _NS

    def _fail(self, p: _Pending, reply: Reply, now: int) -> None:
        """An attempt failed: schedule a retry or complete with ``reply``."""
        if is_retryable(reply.code) and p.retries_left > 0:
            delay_ns = int(retry_delay_ms(p.retries_used, reply.retry_after_ms, self._rng) * _NS)
            if p.deadline_ns is None or now + delay_ns < p.deadline_ns:
                p.retries_left -= 1
                p.retries_used += 1
                p.state = "BACKOFF"
                p.due_ns = now + delay_ns
                return
        self._pending.pop(p.corr_id, None)
        self._ready.append((p, reply))

    # ------------------------------------------------------------- driving
    def process_replies(self) -> list[Completion]:
        """Read every DEALER without blocking and complete matching requests."""
        for peer in self._peers.values():
            while True:
                try:
                    frames = peer.sock.recv_multipart(zmq.NOBLOCK)
                except zmq.Again:
                    break
                got = _decode_reply(frames)
                if got is None:
                    continue
                corr, reply = got
                p = self._pending.pop(corr, None) if corr is not None else None
                if p is None:
                    logger.debug("rpc.late_reply", corr_id=corr)
                    continue
                if is_retryable(reply.code) and p.retries_left > 0:
                    self._pending[p.corr_id] = p
                    self._fail(p, reply, self._clock.monotonic_ns())
                    continue
                self._ready.append((p, reply))
        return self._deliver()

    def poll_timers(self, now_ns: int | None = None) -> list[Completion]:
        """Timeouts, backoff retries and connection waits; delivers finished requests."""
        now = self._clock.monotonic_ns() if now_ns is None else now_ns
        for p in list(self._pending.values()):
            if p.state == "SENT" and now >= p.due_ns:
                self._fail(p, timeout(p.target, p.command, p.timeout_ms), now)
            elif p.state == "BACKOFF" and now >= p.due_ns:
                self._try_send(p, now)
            elif p.state == "WAIT_CONNECT":
                peer = self._peers.get(p.target)
                if peer is None:
                    continue
                if peer.sock.poll(0, zmq.POLLOUT):
                    self._try_send(p, now)
                elif now >= p.due_ns:
                    self._fail(p, unavailable(p.target), now)
        return self._deliver()

    def next_timer_ns(self) -> int | None:
        """Monotonic time of the next timer event (``now`` when completions are ready)."""
        now = self._clock.monotonic_ns()
        if self._ready:
            return now
        times: list[int] = []
        for p in self._pending.values():
            if p.state == "WAIT_CONNECT":
                times.append(min(p.due_ns, now + self._CONNECT_POLL_NS))
            else:
                times.append(p.due_ns)
        return min(times) if times else None

    def _deliver(self) -> list[Completion]:
        ready, self._ready = self._ready, []
        out: list[Completion] = []
        errors: list[BaseException] = []
        for p, reply in ready:
            if p.on_reply is not None:
                try:
                    p.on_reply(reply)
                except Exception as exc:
                    logger.exception("rpc.callback_failed", command=p.command, target=p.target)
                    errors.append(exc)
            else:
                out.append(Completion(p.corr_id, p.target, p.command, p.context, reply, p.attempts))
        if errors:
            raise errors[0]
        return out

    def close(self) -> None:
        for peer in self._peers.values():
            peer.sock.close(0)
        self._peers.clear()
        self._pending.clear()
        self._ready.clear()


__all__ = [
    "AsyncRpcClient",
    "BlockingRpcClient",
    "Completion",
    "LoopRpcClient",
    "Resolver",
]
