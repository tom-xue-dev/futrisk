"""ROUTER command servers (02 §4.1, §4.2, §4.7, §4.9, §4.10, §4.11).

Validation of every request, in order (the failing step answers at once):

1. envelope decodes, else ``BAD_REQUEST``;
2. ``proto`` supported, else ``VERSION_UNSUPPORTED``;
3. caller identity (DEALER identity) allowed by the whitelist, else ``FORBIDDEN``;
4. a handler is registered for ``Envelope.topic``, else ``BAD_REQUEST``;
5. the payload decodes with its registered type (and matches the handler's
   declared ``payload_type``), else ``BAD_REQUEST``;
6. the pending backlog is below its limit, else ``BUSY`` (risk-engine:
   ``RISK_BUSY`` for ``order.*``) with ``retry_after_ms``.

Replies use topic ``"<command>.reply"``, type ``Reply`` and the request
``corr_id``; a handler exception becomes ``INTERNAL`` (and is logged).

:class:`RpcServer` is the sync, poller-driven variant: the service loop calls
:meth:`RpcServer.pump` (or ``receive`` + ``process``); handlers run on the
loop thread and may return :data:`~futrisk.bus.rpc_common.DEFERRED` to answer
later with :meth:`RpcServer.reply`, also from the loop thread.
:class:`AsyncRpcServer` runs a receive task in an asyncio loop; sync handlers
run inline, awaitable results are awaited in their own task (requests are
dispatched in arrival order; with ``async`` handlers, completion order is not
guaranteed).
"""

from __future__ import annotations

import asyncio
import contextlib
from collections import deque
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, cast

import msgspec
import zmq
import zmq.asyncio
from loguru import logger

from futrisk.bus.rpc_common import (
    DEFERRED,
    Handler,
    HandlerSpec,
    RequestContext,
    is_awaitable,
    reply_envelope,
)
from futrisk.bus.sockets import DEFAULT_ROUTER_HWM, make_router
from futrisk.bus.whitelist import Whitelist
from futrisk.common.clock import Clock, SystemClock
from futrisk.messages import codes
from futrisk.messages.base import Msg
from futrisk.messages.envelope import PROTO, Reply, decode_envelope, err_reply, ok_reply, open_payload

BusyCode = str | Callable[[str], str]
DEFAULT_RETRY_AFTER_MS = 100


#: LINGER (ms) when a ROUTER server closes, so queued replies still reach the peer
REPLY_LINGER_MS = 200


@dataclass
class ServerStats:
    received: int = 0
    replied: int = 0
    bad_request: int = 0
    forbidden: int = 0
    version_unsupported: int = 0
    busy: int = 0
    errors: int = 0
    deferred: int = 0


def risk_busy_code(command: str) -> str:
    """risk-engine overload code: ``RISK_BUSY`` for orders, ``BUSY`` otherwise (02 §4.9)."""
    return codes.RISK_BUSY if command.startswith("order.") else codes.BUSY


class _Dispatcher:
    """Request validation and handler registry shared by both servers."""

    def __init__(
        self,
        *,
        instance: str,
        epoch: str,
        clock: Clock,
        whitelist: Whitelist | None,
        busy_code: BusyCode,
        retry_after_ms: int,
    ) -> None:
        self.instance = instance
        self.epoch = epoch
        self.clock = clock
        self.whitelist = whitelist
        self.busy_code = busy_code
        self.retry_after_ms = retry_after_ms
        self.handlers: dict[str, HandlerSpec] = {}
        self.stats = ServerStats()

    def add(self, command: str, handler: Handler, payload_type: type[Msg] | None = None) -> None:
        self.handlers[command] = HandlerSpec(handler, payload_type)

    def reply_frames(self, identity: bytes, command: str, corr_id: str | None, reply: Reply) -> list[bytes]:
        data = reply_envelope(
            command, reply, src=self.instance, epoch=self.epoch, ts_ns=self.clock.now_ns(), corr_id=corr_id
        )
        return [identity, data]

    def parse(self, frames: list[bytes]) -> tuple[RequestContext, Msg, HandlerSpec] | list[bytes] | None:
        """Validated request, or reply frames to send at once, or ``None`` (drop)."""
        self.stats.received += 1
        if len(frames) < 2:
            return None
        identity = frames[0]
        if len(frames) != 2:
            self.stats.bad_request += 1
            return self.reply_frames(identity, "?", None, err_reply(codes.BAD_REQUEST, "expected one envelope frame"))
        try:
            env = decode_envelope(frames[1])
        except (msgspec.DecodeError, msgspec.ValidationError) as exc:
            self.stats.bad_request += 1
            return self.reply_frames(identity, "?", None, err_reply(codes.BAD_REQUEST, f"bad envelope: {exc}"))
        command = env.topic
        if env.proto != PROTO:
            self.stats.version_unsupported += 1
            return self.reply_frames(
                identity, command, env.corr_id, err_reply(codes.VERSION_UNSUPPORTED, f"proto {env.proto} unsupported")
            )
        requester = identity.decode("utf-8", errors="replace")
        if self.whitelist is not None and not self.whitelist.allowed(command, requester):
            self.stats.forbidden += 1
            logger.warning("rpc.forbidden", command=command, requester=requester)
            return self.reply_frames(
                identity, command, env.corr_id, err_reply(codes.FORBIDDEN, f"{requester} may not call {command}")
            )
        spec = self.handlers.get(command)
        if spec is None:
            self.stats.bad_request += 1
            return self.reply_frames(identity, command, env.corr_id, err_reply(codes.BAD_REQUEST, f"unknown command {command}"))
        try:
            payload = open_payload(env)
        except (KeyError, msgspec.DecodeError, msgspec.ValidationError) as exc:
            self.stats.bad_request += 1
            return self.reply_frames(identity, command, env.corr_id, err_reply(codes.BAD_REQUEST, f"bad payload: {exc}"))
        if spec.payload_type is not None and not isinstance(payload, spec.payload_type):
            self.stats.bad_request += 1
            return self.reply_frames(
                identity,
                command,
                env.corr_id,
                err_reply(codes.BAD_REQUEST, f"{command} expects {spec.payload_type.__name__}, got {env.type}"),
            )
        ctx = RequestContext(
            command=command,
            requester=requester,
            identity=identity,
            corr_id=env.corr_id,
            received_ns=self.clock.now_ns(),
            received_mono_ns=self.clock.monotonic_ns(),
            envelope=env,
        )
        return ctx, payload, spec

    def busy_reply(self, command: str) -> Reply:
        self.stats.busy += 1
        code = self.busy_code(command) if callable(self.busy_code) else self.busy_code
        return err_reply(code, "server overloaded", retry_after_ms=self.retry_after_ms)

    def internal(self, ctx: RequestContext, exc: BaseException) -> Reply:
        self.stats.errors += 1
        logger.opt(exception=exc).error("rpc.handler_failed", command=ctx.command, requester=ctx.requester)
        return err_reply(codes.INTERNAL, f"{type(exc).__name__}: {exc}")


def _normalise(result: object) -> Reply | None:
    """Handler result -> Reply to send (``None`` when deferred)."""
    if result is DEFERRED:
        return None
    if result is None:
        return ok_reply()
    if isinstance(result, Reply):
        return result
    raise TypeError(f"handler returned {type(result).__name__}, expected Reply/None/DEFERRED")


class RpcServer:
    """Sync ROUTER server driven by the service's poller loop."""

    def __init__(
        self,
        ctx: zmq.Context[Any],
        address: str,
        *,
        instance: str,
        epoch: str,
        clock: Clock | None = None,
        whitelist: Whitelist | None = None,
        rcvhwm: int = DEFAULT_ROUTER_HWM,
        max_backlog: int = 1_000,
        busy_code: BusyCode = codes.BUSY,
        retry_after_ms: int = DEFAULT_RETRY_AFTER_MS,
    ) -> None:
        self.address = address
        self.socket: zmq.Socket[Any] = make_router(ctx, address, rcvhwm=rcvhwm)
        self._d = _Dispatcher(
            instance=instance,
            epoch=epoch,
            clock=clock or SystemClock(),
            whitelist=whitelist,
            busy_code=busy_code,
            retry_after_ms=retry_after_ms,
        )
        self._backlog: deque[tuple[RequestContext, Msg, HandlerSpec]] = deque()
        self._max_backlog = max_backlog

    @property
    def stats(self) -> ServerStats:
        return self._d.stats

    @property
    def backlog_size(self) -> int:
        return len(self._backlog)

    def add_handler(self, command: str, handler: Handler, *, payload_type: type[Msg] | None = None) -> None:
        self._d.add(command, handler, payload_type)

    def serve(self, handlers: Mapping[str, Handler]) -> None:
        for command, handler in handlers.items():
            self._d.add(command, handler)

    def commands(self) -> list[str]:
        return sorted(self._d.handlers)

    def receive(self, limit: int | None = None) -> int:
        """Move pending requests from the socket into the backlog (never blocks)."""
        n = 0
        while limit is None or n < limit:
            try:
                frames = self.socket.recv_multipart(zmq.NOBLOCK)
            except zmq.Again:
                break
            n += 1
            parsed = self._d.parse(frames)
            if parsed is None:
                continue
            if isinstance(parsed, list):
                self._send(parsed)
                continue
            ctx, payload, spec = parsed
            if len(self._backlog) >= self._max_backlog:
                self.reply(ctx, self._d.busy_reply(ctx.command))
                continue
            self._backlog.append((ctx, payload, spec))
        return n

    def process(self, limit: int = 256) -> int:
        """Dispatch up to ``limit`` backlog requests to their handlers."""
        n = 0
        while self._backlog and n < limit:
            ctx, payload, spec = self._backlog.popleft()
            n += 1
            try:
                result = spec.handler(ctx.command, payload, ctx)
                if is_awaitable(result):
                    close = getattr(result, "close", None)
                    if close is not None:
                        close()
                    raise TypeError("sync RpcServer handlers must not be async")
                reply = _normalise(result)
            except Exception as exc:
                reply = self._d.internal(ctx, exc)
            if reply is None:
                self._d.stats.deferred += 1
                continue
            self.reply(ctx, reply)
        return n

    def pump(self, limit: int = 256) -> int:
        """``receive()`` then ``process(limit)``; returns requests dispatched."""
        self.receive()
        return self.process(limit)

    def reply(self, ctx: RequestContext, reply: Reply) -> None:
        """Send the reply of ``ctx`` (now or later); call from the loop thread."""
        self._send(self._d.reply_frames(ctx.identity, ctx.command, ctx.corr_id, reply))

    def _send(self, frames: list[bytes]) -> None:
        try:
            self.socket.send_multipart(frames, zmq.NOBLOCK)
            self._d.stats.replied += 1
        except zmq.Again:
            logger.warning("rpc.reply_dropped", reason="peer HWM")
        except zmq.ZMQError:
            logger.exception("rpc.reply_failed")

    def close(self) -> None:
        # a short linger flushes replies still queued (e.g. the ctl.shutdown answer); the context
        # teardown that follows waits at most this long for them
        self.socket.close(REPLY_LINGER_MS)


class AsyncRpcServer:
    """ROUTER server for asyncio services."""

    def __init__(
        self,
        ctx: zmq.asyncio.Context,
        address: str,
        *,
        instance: str,
        epoch: str,
        clock: Clock | None = None,
        whitelist: Whitelist | None = None,
        rcvhwm: int = DEFAULT_ROUTER_HWM,
        max_inflight: int = 1_000,
        busy_code: BusyCode = codes.BUSY,
        retry_after_ms: int = DEFAULT_RETRY_AFTER_MS,
    ) -> None:
        self.address = address
        self._ctx = ctx
        self._rcvhwm = rcvhwm
        self._sock: zmq.asyncio.Socket | None = None
        self._d = _Dispatcher(
            instance=instance,
            epoch=epoch,
            clock=clock or SystemClock(),
            whitelist=whitelist,
            busy_code=busy_code,
            retry_after_ms=retry_after_ms,
        )
        self._max_inflight = max_inflight
        self._inflight: set[asyncio.Task[None]] = set()
        self._recv_task: asyncio.Task[None] | None = None

    @property
    def stats(self) -> ServerStats:
        return self._d.stats

    @property
    def inflight(self) -> int:
        return len(self._inflight)

    def add_handler(self, command: str, handler: Handler, *, payload_type: type[Msg] | None = None) -> None:
        self._d.add(command, handler, payload_type)

    def serve(self, handlers: Mapping[str, Handler]) -> None:
        for command, handler in handlers.items():
            self._d.add(command, handler)

    def commands(self) -> list[str]:
        return sorted(self._d.handlers)

    async def start(self) -> None:
        self._sock = make_router(self._ctx, self.address, rcvhwm=self._rcvhwm)  # type: ignore[assignment]
        self._recv_task = asyncio.create_task(self._recv_loop(), name=f"rpc-server-{self.address}")

    async def _recv_loop(self) -> None:
        assert self._sock is not None
        while True:
            frames = await self._sock.recv_multipart()
            parsed = self._d.parse(list(frames))
            if parsed is None:
                continue
            if isinstance(parsed, list):
                self._send(parsed)
                continue
            ctx, payload, spec = parsed
            if len(self._inflight) >= self._max_inflight:
                self.reply(ctx, self._d.busy_reply(ctx.command))
                continue
            try:
                result = spec.handler(ctx.command, payload, ctx)
            except Exception as exc:
                self.reply(ctx, self._d.internal(ctx, exc))
                continue
            if is_awaitable(result):
                task = asyncio.ensure_future(self._finish(ctx, result))
                self._inflight.add(task)
                task.add_done_callback(self._inflight.discard)
            else:
                self._deliver(ctx, result)

    async def _finish(self, ctx: RequestContext, awaitable: Any) -> None:
        try:
            result = await awaitable
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.reply(ctx, self._d.internal(ctx, exc))
            return
        self._deliver(ctx, result)

    def _deliver(self, ctx: RequestContext, result: object) -> None:
        try:
            reply = _normalise(result)
        except TypeError as exc:
            reply = self._d.internal(ctx, exc)
        if reply is None:
            self._d.stats.deferred += 1
            return
        self.reply(ctx, reply)

    def reply(self, ctx: RequestContext, reply: Reply) -> None:
        """Send the reply of ``ctx`` (loop thread)."""
        self._send(self._d.reply_frames(ctx.identity, ctx.command, ctx.corr_id, reply))

    def _send(self, frames: list[bytes]) -> None:
        sock = self._sock
        if sock is None or sock.closed:
            return
        fut = cast("asyncio.Future[Any]", sock.send_multipart(frames, flags=zmq.NOBLOCK))
        self._d.stats.replied += 1

        def _check(f: asyncio.Future[Any]) -> None:
            if f.cancelled():
                return
            exc = f.exception()
            if exc is not None:
                logger.warning("rpc.reply_dropped", error=str(exc))

        fut.add_done_callback(_check)

    async def close(self, timeout: float = 2.0) -> None:
        if self._recv_task is not None:
            self._recv_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._recv_task
            self._recv_task = None
        if self._inflight:
            _, pending = await asyncio.wait(set(self._inflight), timeout=timeout)
            for t in pending:
                t.cancel()
        if self._sock is not None:
            self._sock.close(REPLY_LINGER_MS)  # flush queued replies (see RpcServer.close)
            self._sock = None


__all__ = ["AsyncRpcServer", "RpcServer", "ServerStats", "risk_busy_code"]
