"""Integration helpers: sim-exchange in its own asyncio thread, trade-gateway on a business thread,
bare bus peers (a fake coordinator for the ``risk.control.firm`` fallbacks, a plain publisher)."""

from __future__ import annotations

import asyncio
import dataclasses
import os
import queue
import threading
import time
from collections.abc import Callable, Coroutine
from concurrent.futures import Future
from pathlib import Path
from typing import Any, TypeVar

import zmq

from futrisk.bus import BlockingRpcClient, PublisherCore, PublisherThread, RequestContext, RpcServer
from futrisk.common.clock import MarketClock
from futrisk.common.config import FullConfig
from futrisk.common.ids import uuid7
from futrisk.common.paths import HomeLayout
from futrisk.messages import codes, decode_envelope, topics
from futrisk.messages.base import Msg, decode_named, encode, type_version
from futrisk.messages.common import ReplayBatch, ReplayRequest, SnapshotBatch, SnapshotItem
from futrisk.messages.enums import ServiceState
from futrisk.messages.envelope import PROTO, Envelope, Reply, encode_envelope, err_reply, ok_reply
from futrisk.messages.risk import FirmControl
from futrisk.messages.system import Heartbeat
from futrisk.runtime import SyncServiceRuntime
from futrisk.services.trade_gateway.service import TradeGatewayService
from futrisk.sim.exchange import SimExchange

T = TypeVar("T")
TG = "trade-gateway"


def wait_for(pred: Callable[[], Any], timeout: float = 10.0, step: float = 0.01, what: str = "condition") -> Any:
    end = time.monotonic() + timeout
    while True:
        got = pred()
        if got:
            return got
        if time.monotonic() > end:
            raise AssertionError(f"{what} not met within {timeout}s")
        time.sleep(step)


class SimThread:
    """A running :class:`SimExchange` hosted by a private event loop thread."""

    def __init__(self, cfg: FullConfig, home: Path, offset_ns: int) -> None:
        self.loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self.loop.run_forever, name="sim-loop", daemon=True)
        self._thread.start()
        self.ex = SimExchange(cfg, HomeLayout(home), offset_ns)
        self.call(self.ex.start())
        wait_for(lambda: self.ex.engine.rounds >= 2, what="first market states")

    def call(self, coro: Coroutine[Any, Any, T], timeout: float = 10.0) -> T:
        return asyncio.run_coroutine_threadsafe(coro, self.loop).result(timeout)

    def run(self, fn: Callable[[], T], timeout: float = 10.0) -> T:
        async def wrapper() -> T:
            return fn()

        return self.call(wrapper(), timeout)

    @property
    def broker_endpoint(self) -> str:
        return self.ex.broker_endpoint

    def limits(self, instrument_id: str) -> tuple[float, float]:
        """(upper, lower) limit prices: a buy at the upper / a sell at the lower always crosses."""

        def get() -> tuple[float, float]:
            st = self.ex.engine.states[instrument_id]
            return float(st.meta.price(st.upper)), float(st.meta.price(st.lower))

        return self.run(get)

    def force_close(self, account_id: str, instrument_id: str, qty: int | None = None) -> dict[str, Any]:
        return self.call(self.ex.broker.force_close(account_id, instrument_id, qty))

    def broker_state(self) -> dict[str, Any]:
        return self.run(self.ex.broker.state)

    def close(self) -> None:
        try:
            self.call(self.ex.stop(), timeout=20)
        finally:
            self.loop.call_soon_threadsafe(self.loop.stop)
            self._thread.join(10)
            self.loop.close()


def with_broker_endpoint(cfg: FullConfig, endpoint: str) -> FullConfig:
    sim = cfg.sim.model_copy(update={"broker": cfg.sim.broker.model_copy(update={"endpoint": endpoint})})
    return dataclasses.replace(cfg, sim=sim)


class TgThread:
    """trade-gateway (real runtime + service) running on its own business thread."""

    def __init__(self, cfg: FullConfig, home: Path, clock: MarketClock, **svc_kw: Any) -> None:
        self.cfg = cfg
        self.rt = SyncServiceRuntime(TG, config=cfg, home=home, market_clock=clock, setup_logs=False)
        self.svc = TradeGatewayService(self.rt, **svc_kw)
        self.exit_code: int | None = None
        self._thread = threading.Thread(target=self._main, name="business:trade-gateway", daemon=True)

    def _main(self) -> None:
        self.exit_code = self.svc.run()

    def start(self, timeout: float = 15.0) -> TgThread:
        self._thread.start()
        wait_for(lambda: self.svc.core is not None and self.svc.core.ready, timeout, what="trade-gateway ready")
        return self

    @property
    def core(self) -> Any:
        return self.svc.core

    def stop(self, timeout: float = 15.0) -> None:
        if self._thread.is_alive():
            self.rt.request_stop("test")
            self._thread.join(timeout)


class LoopThread:
    """Any SyncServiceRuntime running ``rt.run`` on a thread; ``call`` executes code on that thread."""

    def __init__(self, rt: SyncServiceRuntime) -> None:
        self.rt = rt
        self._q: queue.SimpleQueue[tuple[Callable[[], Any], Future[Any]]] = queue.SimpleQueue()
        self._started = threading.Event()
        self.error: BaseException | None = None
        self._thread = threading.Thread(target=self._main, name=f"business:{rt.instance}", daemon=True)

    def start(self) -> LoopThread:
        self._thread.start()
        assert self._started.wait(10)
        if self.error is not None:
            raise self.error
        return self

    def _main(self) -> None:
        try:
            self.rt.start()
            self._started.set()
            self.rt.run(self._step, on_completion=lambda c: None, max_wait_ms=5)
        except BaseException as exc:
            self.error = exc
            self._started.set()
        finally:
            self.rt.stop()

    def _step(self) -> None:
        while True:
            try:
                fn, fut = self._q.get_nowait()
            except queue.Empty:
                return
            try:
                fut.set_result(fn())
            except BaseException as exc:
                fut.set_exception(exc)

    def call(self, fn: Callable[[], T], timeout: float = 10.0) -> T:
        fut: Future[T] = Future()
        self._q.put((fn, fut))
        return fut.result(timeout)

    def stop(self) -> None:
        if self._thread.is_alive():
            self.rt.request_stop("test")
            self._thread.join(10)


class FakePublisher:
    """A bare PUB socket at ``instance``'s endpoint (e.g. clearing-worker's ``sys.eod``)."""

    def __init__(self, cfg: FullConfig, instance: str) -> None:
        self._ctx: zmq.Context[Any] = zmq.Context()
        self._pub = PublisherThread(self._ctx, cfg.services.instance(instance).pub or "",
                                    PublisherCore(instance, uuid7()), name=f"fake-{instance}")
        self._pub.start()

    def publish(self, topic: str, body: Msg) -> int:
        return self._pub.publish(topic, body)

    def close(self) -> None:
        self._pub.stop()
        self._ctx.destroy(linger=0)


class FakeCoordinator:
    """``risk-engine-0`` stand-in serving ``risk.control.firm`` the way a publisher does (02 §4.5, §4.6, §8.2).

    Heartbeats announce ``stream`` and ``fact_heads``; ``ctl.replay`` serves only the *stored* facts and
    answers ``NOT_FOUND`` (with the first stored seq) below them, like a retention purge; ``ctl.snapshot``
    returns the newest fact. :meth:`new_stream` models a recreated / restored coordinator database.
    """

    TOPIC = topics.RISK_CONTROL_FIRM

    def __init__(self, cfg: FullConfig, clock: MarketClock, instance: str = "risk-engine-0", hb_ms: int = 100) -> None:
        ep = cfg.services.instance(instance)
        self.instance = instance
        self.clock = clock
        self.epoch = uuid7()
        self.stream = uuid7()
        self.requests: list[tuple[str, str, int]] = []  # (command, stream at the time, from_seq)
        self.fail_snapshots = 0  # answer the next N ctl.snapshot requests UNAVAILABLE
        self._router = ep.router or ""
        self._hb_s = hb_ms / 1000
        self._lock = threading.Lock()
        self._stored: dict[int, bytes] = {}
        self._latest: tuple[int, bytes] | None = None
        self._head = 0
        self._stop = threading.Event()
        self._ready = threading.Event()
        self._ctx: zmq.Context[Any] = zmq.Context()
        self._pub = PublisherThread(self._ctx, ep.pub or "", PublisherCore(instance, self.epoch), name="fake-coordinator-pub")
        self._thread = threading.Thread(target=self._run, name="fake-coordinator", daemon=True)

    def start(self) -> FakeCoordinator:
        self._pub.start()
        self._thread.start()
        assert self._ready.wait(5)
        return self

    def fact(self, seq: int, fc: FirmControl, *, stored: bool = True) -> None:
        """Add ``fc`` as ``risk.control.firm`` #``seq`` of the current stream (``stored=False``: purged)."""
        with self._lock:
            self._add(seq, fc, stored)

    def new_stream(self, seq: int | None = None, fc: FirmControl | None = None, *, stored: bool = False) -> None:
        """Switch to a newer stream (database recreated), optionally with its first fact, atomically."""
        with self._lock:
            self.stream = uuid7()
            self._stored, self._latest, self._head = {}, None, 0
            if seq is not None and fc is not None:
                self._add(seq, fc, stored)

    def count(self, command: str) -> int:
        with self._lock:
            return sum(1 for r in self.requests if r[0] == command)

    def _add(self, seq: int, fc: FirmControl, stored: bool) -> None:
        env = Envelope(proto=PROTO, topic=self.TOPIC, type="FirmControl", v=type_version("FirmControl"), src=self.instance,
                       epoch=self.epoch, stream=self.stream, seq=seq, ts_ns=fc.ts_ns, td=fc.trading_day, corr_id=None,
                       payload=encode(fc))
        data = encode_envelope(env)
        if stored:
            self._stored[seq] = data
        self._head = max(self._head, seq)
        if self._latest is None or seq >= self._latest[0]:
            self._latest = (seq, data)

    def _heartbeat(self) -> None:
        with self._lock:
            heads = {self.TOPIC: self._head} if self._head else {}
            stream = self.stream
        hb = Heartbeat(instance=self.instance, service="risk-engine", state=ServiceState.RUNNING, pid=os.getpid(),
                       epoch=self.epoch, app_version="test", stream=stream, fact_heads=heads, started_ns=0)
        self._pub.publish(topics.sys_heartbeat(self.instance), hb)

    def _replay(self, command: str, payload: Msg, ctx: RequestContext) -> Reply:
        assert isinstance(payload, ReplayRequest)
        with self._lock:
            self.requests.append((command, self.stream, payload.from_seq))
            first = min(self._stored) if self._stored else self._head + 1
            if payload.from_seq < first:
                return err_reply(codes.NOT_FOUND, "purged by retention",
                                 ReplayBatch(topic=payload.topic, envelopes=[], next_seq=first, complete=True))
            hi = min(payload.to_seq or self._head, self._head)
            envs = [self._stored[s] for s in range(payload.from_seq, hi + 1) if s in self._stored]
        return ok_reply(ReplayBatch(topic=payload.topic, envelopes=envs, next_seq=hi + 1, complete=True))

    def _snapshot(self, command: str, payload: Msg, ctx: RequestContext) -> Reply:
        with self._lock:
            self.requests.append((command, self.stream, 0))
            if self.fail_snapshots > 0:
                self.fail_snapshots -= 1
                return err_reply(codes.UNAVAILABLE, "snapshot not available yet")
            latest = self._latest
        items = [] if latest is None else [SnapshotItem(topic=self.TOPIC, seq=latest[0], epoch=self.epoch, envelope=latest[1])]
        return ok_reply(SnapshotBatch(items=items, as_of_ns=self.clock.now_ns()))

    def _run(self) -> None:
        rpc = RpcServer(self._ctx, self._router, instance=self.instance, epoch=self.epoch)
        rpc.serve({"ctl.replay": self._replay, "ctl.snapshot": self._snapshot})
        poller = zmq.Poller()
        poller.register(rpc.socket, zmq.POLLIN)
        self._ready.set()
        next_hb = 0.0
        try:
            while not self._stop.is_set():
                now = time.monotonic()
                if now >= next_hb:
                    self._heartbeat()
                    next_hb = now + self._hb_s
                if poller.poll(10):
                    rpc.pump()
        finally:
            rpc.close()

    def close(self) -> None:
        self._stop.set()
        self._thread.join(5)
        self._pub.stop()
        self._ctx.destroy(linger=0)


def client(cfg: FullConfig, identity: str) -> BlockingRpcClient:
    return BlockingRpcClient(lambda t: cfg.services.instance(t).router or "", identity=identity)


def exec_facts(c: BlockingRpcClient, account_id: str) -> list[Envelope]:
    """Every ``td.exec.{account_id}`` FACT so far, in seq order (``ctl.replay`` from seq 1)."""
    r = c.request(TG, "ctl.replay", ReplayRequest(topic=f"td.exec.{account_id}", from_seq=1, limit=1000), timeout_ms=5000)
    assert r.ok, r
    batch = r.open_as(ReplayBatch)
    return [decode_envelope(b) for b in batch.envelopes]


def body(env: Envelope) -> Msg:
    return decode_named(env.payload, env.type)


__all__ = [
    "TG",
    "FakeCoordinator",
    "FakePublisher",
    "LoopThread",
    "SimThread",
    "TgThread",
    "body",
    "client",
    "exec_facts",
    "wait_for",
    "with_broker_endpoint",
]
