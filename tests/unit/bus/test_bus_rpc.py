"""Request/reply over tcp loopback (02 §4.7, §4.9-§4.11): envelopes, whitelist, errors, timeouts,
unreachable peers, retries with the same idempotency key, backlog BUSY, loop and asyncio clients."""

from __future__ import annotations

import asyncio
import queue
import threading
import time
from collections.abc import Callable, Mapping
from typing import Any

import msgspec
import zmq
import zmq.asyncio
from bustest_support import addr

from futrisk.bus import (
    DEFERRED,
    AsyncRpcClient,
    AsyncRpcServer,
    BlockingRpcClient,
    Completion,
    LoopRpcClient,
    RpcServer,
    Whitelist,
    risk_busy_code,
)
from futrisk.bus.rpc_common import Handler, RequestContext, request_envelope
from futrisk.messages import codes, decode, decode_envelope, encode_envelope
from futrisk.messages.common import Ping, Pong
from futrisk.messages.envelope import Reply, err_reply, ok_reply
from futrisk.messages.orders import CancelRequest


def pong(cmd: str, payload: object, ctx: RequestContext) -> Reply:
    return ok_reply(Pong(instance="srv", state=ctx.requester, ts_ns=ctx.received_ns))


class ServerThread:
    """Sync RpcServer running its own poller loop (the socket lives in that thread)."""

    def __init__(
        self,
        ctx: zmq.Context[Any],
        address: str,
        handlers: Mapping[str, Handler],
        *,
        whitelist: Whitelist | None = None,
        max_backlog: int = 1000,
        busy_code: Any = codes.BUSY,
        paused: bool = False,
    ) -> None:
        self.address = address
        self.paused = threading.Event()
        if paused:
            self.paused.set()
        self._later: queue.SimpleQueue[tuple[RequestContext, Reply]] = queue.SimpleQueue()
        self._stop = threading.Event()
        self._ready = threading.Event()
        self._args = (ctx, address, handlers, whitelist, max_backlog, busy_code)
        self.server: RpcServer | None = None
        self._t = threading.Thread(target=self._run, daemon=True)
        self._t.start()
        assert self._ready.wait(5)

    def _run(self) -> None:
        ctx, address, handlers, whitelist, max_backlog, busy_code = self._args
        srv = RpcServer(ctx, address, instance="srv", epoch="srv-epoch", whitelist=whitelist, max_backlog=max_backlog, busy_code=busy_code)
        srv.serve(handlers)
        self.server = srv
        self._ready.set()
        poller = zmq.Poller()
        poller.register(srv.socket, zmq.POLLIN)
        while not self._stop.is_set():
            poller.poll(5)
            srv.receive()
            if not self.paused.is_set():
                srv.process()
            while True:
                try:
                    c, r = self._later.get_nowait()
                except queue.Empty:
                    break
                srv.reply(c, r)
        srv.close()

    def reply_later(self, ctx: RequestContext, reply: Reply) -> None:
        self._later.put((ctx, reply))

    def stop(self) -> None:
        self._stop.set()
        self._t.join(2)


# ---------------------------------------------------------------------------
# Envelope and validation
# ---------------------------------------------------------------------------


class TestServerValidation:
    def test_reply_envelope_topic_type_corr(self, zctx: zmq.Context[Any]) -> None:
        a = addr()
        srv = ServerThread(zctx, a, {"ctl.ping": pong})
        dealer = zctx.socket(zmq.DEALER)
        dealer.setsockopt(zmq.IDENTITY, b"api-gateway")
        dealer.connect(a)
        try:
            dealer.send(request_envelope("ctl.ping", Ping(), src="api-gateway", epoch="e", ts_ns=1, corr_id="req-1"))
            assert dealer.poll(3000)
            env = decode_envelope(dealer.recv())
            assert (env.topic, env.type, env.corr_id, env.seq, env.src) == ("ctl.ping.reply", "Reply", "req-1", 0, "srv")
            reply = decode(env.payload, Reply)
            assert reply.ok and reply.open_as(Pong).state == "api-gateway"
        finally:
            dealer.close(0)
            srv.stop()

    def test_errors_forbidden_unknown_bad_payload_version(self, zctx: zmq.Context[Any]) -> None:
        a = addr()
        wl = Whitelist(["risk-engine-0", "api-gateway"])

        def typed(cmd: str, payload: object, ctx: RequestContext) -> Reply:
            return ok_reply()

        srv = ServerThread(zctx, a, {"ctl.ping": pong, "order.submit": pong}, whitelist=wl)
        assert srv.server is not None
        srv.server.add_handler("order.cancel", typed, payload_type=CancelRequest)
        md = BlockingRpcClient(lambda _: a, identity="md-gateway")
        api = BlockingRpcClient(lambda _: a, identity="api-gateway")
        try:
            assert md.request("srv", "order.submit", Ping(), retries=0).code == codes.FORBIDDEN
            assert api.request("srv", "order.submit", Ping(), retries=0).ok
            assert api.request("srv", "no.such.command", Ping(), retries=0).code == codes.BAD_REQUEST
            assert api.request("srv", "order.cancel", Ping(), retries=0).code == codes.BAD_REQUEST
            assert md.request("srv", "ctl.ping", Ping(), retries=0).ok
            # unsupported envelope version
            dealer = zctx.socket(zmq.DEALER)
            dealer.setsockopt(zmq.IDENTITY, b"api-gateway")
            dealer.connect(a)
            env = decode_envelope(request_envelope("ctl.ping", Ping(), src="x", epoch="e", ts_ns=1, corr_id="c9"))
            dealer.send(encode_envelope(msgspec.structs.replace(env, proto=9)))
            assert dealer.poll(3000)
            back = decode_envelope(dealer.recv())
            assert decode(back.payload, Reply).code == codes.VERSION_UNSUPPORTED and back.corr_id == "c9"
            dealer.close(0)
        finally:
            md.close()
            api.close()
            srv.stop()


# ---------------------------------------------------------------------------
# Timeouts, unreachable peers, retries
# ---------------------------------------------------------------------------


class TestBlockingClient:
    def test_timeout_when_handler_never_replies(self, zctx: zmq.Context[Any]) -> None:
        a = addr()
        srv = ServerThread(zctx, a, {"risk.query.accounts": lambda c, p, x: DEFERRED})
        cli = BlockingRpcClient(lambda _: a, identity="api-gateway")
        try:
            t0 = time.monotonic()
            r = cli.request("srv", "risk.query.accounts", Ping(), timeout_ms=150, retries=0)
            dt = time.monotonic() - t0
            assert r.code == codes.TIMEOUT and 0.14 <= dt < 1.0
        finally:
            cli.close()
            srv.stop()

    def test_unreachable_peer_is_unavailable_fast(self) -> None:
        cli = BlockingRpcClient(lambda _: addr(), identity="api-gateway")
        try:
            t0 = time.monotonic()
            r = cli.request("nobody", "ctl.ping", Ping(), timeout_ms=5_000, retries=0)
            assert r.code == codes.UNAVAILABLE and time.monotonic() - t0 < 1.0
            # unknown instance name in the endpoint table
            cli2 = BlockingRpcClient(lambda n: {}[n], identity="api-gateway")
            assert cli2.request("ghost", "ctl.ping", Ping(), retries=0).code == codes.UNAVAILABLE
            cli2.close()
        finally:
            cli.close()

    def test_retry_keeps_idempotency_key_and_corr_id(self, zctx: zmq.Context[Any]) -> None:
        a = addr()
        seen: list[tuple[bytes, str | None]] = []

        def busy_twice(cmd: str, payload: object, ctx: RequestContext) -> Reply:
            seen.append((ctx.envelope.payload, ctx.corr_id))
            if len(seen) < 3:
                return err_reply(codes.BUSY, "full", retry_after_ms=20)
            return ok_reply(message="done")

        srv = ServerThread(zctx, a, {"order.cancel": busy_twice})
        cli = BlockingRpcClient(lambda _: a, identity="api-gateway")
        try:
            r = cli.request("srv", "order.cancel", CancelRequest(command_id="cmd-7", client_order_id="o1", account_id="ACC1"))
            assert r.ok and r.message == "done" and len(seen) == 3
            assert len({p for p, _ in seen}) == 1  # same payload bytes -> same command_id
            assert decode(seen[0][0], CancelRequest).command_id == "cmd-7"
            assert len({c for _, c in seen}) == 1
            # non-retryable codes are returned at once
            seen.clear()
            srv2 = ServerThread(zctx, addr2 := addr(), {"order.cancel": lambda c, p, x: err_reply(codes.CONFLICT)})
            cli2 = BlockingRpcClient(lambda _: addr2, identity="api-gateway")
            assert cli2.request("srv", "order.cancel", Ping()).code == codes.CONFLICT
            cli2.close()
            srv2.stop()
        finally:
            cli.close()
            srv.stop()


# ---------------------------------------------------------------------------
# Non-blocking loop client and backlog BUSY
# ---------------------------------------------------------------------------


def drive(client: LoopRpcClient, until: Callable[[], bool], timeout: float = 3.0) -> list[Completion]:
    poller = zmq.Poller()
    out: list[Completion] = []
    deadline = time.monotonic() + timeout
    while not until() and time.monotonic() < deadline:
        for s in client.sockets():
            poller.register(s, zmq.POLLIN)
        nt = client.next_timer_ns()
        wait = 10 if nt is None else max(0, min(10, (nt - time.perf_counter_ns()) // 1_000_000))
        poller.poll(wait)
        out.extend(client.process_replies())
        out.extend(client.poll_timers())
    return out


class TestLoopClient:
    def test_callbacks_contexts_deferred_timeout_unavailable(self, zctx: zmq.Context[Any]) -> None:
        a = addr()
        later: list[RequestContext] = []

        def deferred(cmd: str, payload: object, ctx: RequestContext) -> object:
            later.append(ctx)
            return DEFERRED

        srv = ServerThread(zctx, a, {"ctl.ping": pong, "eod.rollover": deferred, "never": lambda c, p, x: DEFERRED})
        dead = addr()
        targets = {"srv": a, "dead": dead}
        client = LoopRpcClient(zctx, lambda t: targets[t], identity="clearing-worker")
        got: dict[str, Reply] = {}
        comps: list[Completion] = []
        try:
            client.send("srv", "ctl.ping", Ping(), on_reply=lambda r: got.__setitem__("ping", r))
            client.send("srv", "eod.rollover", Ping(), context="roll-ctx", retries=0)
            client.send("srv", "never", Ping(), timeout_ms=100, retries=0, context="t")
            client.send("dead", "ctl.ping", Ping(), retries=0, context="d")
            comps += drive(client, lambda: "ping" in got and len(later) == 1)
            assert got["ping"].ok
            srv.reply_later(later[0], ok_reply(message="rolled"))
            comps += drive(client, lambda: client.pending_count == 0)
            by_ctx = {c.context: c for c in comps}
            assert by_ctx["roll-ctx"].reply.message == "rolled" and by_ctx["roll-ctx"].command == "eod.rollover"
            assert by_ctx["t"].reply.code == codes.TIMEOUT
            assert by_ctx["d"].reply.code == codes.UNAVAILABLE and by_ctx["d"].attempts == 0
        finally:
            client.close()
            srv.stop()

    def test_backlog_full_returns_risk_busy_and_client_retries(self, zctx: zmq.Context[Any]) -> None:
        a = addr()
        srv = ServerThread(zctx, a, {"order.submit": pong, "ctl.ping": pong}, max_backlog=2, busy_code=risk_busy_code, paused=True)
        client = LoopRpcClient(zctx, lambda _: a, identity="strategy-sim")
        replies: list[Reply] = []
        try:
            client.connect("srv")
            time.sleep(0.05)
            for _ in range(3):
                client.send("srv", "order.submit", Ping(), retries=0, on_reply=replies.append)
            drive(client, lambda: len(replies) >= 1)
            assert [r.code for r in replies] == [codes.RISK_BUSY] and replies[0].retry_after_ms == 100
            srv.paused.clear()
            drive(client, lambda: len(replies) == 3)
            assert sorted(r.code for r in replies) == [codes.OK, codes.OK, codes.RISK_BUSY]
            # with retries the BUSY answer is retried transparently
            srv.paused.set()
            replies.clear()
            for _ in range(3):
                client.send("srv", "ctl.ping", Ping(), on_reply=replies.append)
            time.sleep(0.05)
            srv.paused.clear()
            drive(client, lambda: len(replies) == 3, timeout=5)
            assert [r.code for r in replies] == [codes.OK] * 3
        finally:
            client.close()
            srv.stop()


# ---------------------------------------------------------------------------
# asyncio server and client
# ---------------------------------------------------------------------------


class TestAsyncRpc:
    async def test_async_handlers_concurrency_and_deferred(self) -> None:
        actx = zmq.asyncio.Context()
        a = addr()
        server = AsyncRpcServer(actx, a, instance="clearing-worker", epoch="e", whitelist=Whitelist(["risk-engine-0"]))
        pending: list[RequestContext] = []

        async def slow(cmd: str, payload: object, ctx: RequestContext) -> Reply:
            await asyncio.sleep(0.2)
            return ok_reply(message="slow")

        def later(cmd: str, payload: object, ctx: RequestContext) -> object:
            pending.append(ctx)
            return DEFERRED

        server.serve({"settle.sod_snapshot": slow, "ctl.ping": pong, "report.list": later})
        await server.start()
        client = AsyncRpcClient(actx, lambda _: a, identity="risk-engine-0")
        try:
            t0 = time.monotonic()
            results = await asyncio.gather(*(client.request("cw", "settle.sod_snapshot", Ping()) for _ in range(5)))
            # 5 x 200 ms handlers: a sequential server needs >= 1 s, a concurrent one ~0.2 s (slack for load)
            assert all(r.message == "slow" for r in results) and time.monotonic() - t0 < 0.8
            fut = asyncio.ensure_future(client.request("cw", "report.list", Ping(), timeout_ms=2000))
            while not pending:
                await asyncio.sleep(0.005)
            server.reply(pending[0], ok_reply(message="later"))
            assert (await fut).message == "later"
            assert (await client.request("cw", "eod.freeze", Ping(), retries=0)).code == codes.FORBIDDEN
            r = await client.request("cw", "report.list", Ping(), timeout_ms=100, retries=0)
            assert r.code == codes.TIMEOUT
            bad = AsyncRpcClient(actx, lambda _: addr(), identity="risk-engine-0")
            t1 = time.monotonic()
            assert (await bad.request("x", "ctl.ping", Ping(), retries=0)).code == codes.UNAVAILABLE
            assert time.monotonic() - t1 < 1.0
            await bad.close()
        finally:
            await client.close()
            await server.close()
            actx.destroy(linger=0)
