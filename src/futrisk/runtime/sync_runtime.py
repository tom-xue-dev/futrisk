"""Runtime for sync poller services: risk-engine, trade-gateway (02 §8.1, 01 §5.3.1).

Threads: the **business thread** (the service's own loop, the only one touching
business state), the **publisher thread** (owns the PUB socket), the
**heartbeat thread** (1 Hz, never blocked by the loop) and one **DB writer
thread** per database. All sockets used by the loop (ROUTER, SUBs, DEALERs)
are registered in :attr:`SyncServiceRuntime.poller`.

The 02 §8.1 loop, written against this runtime::

    rt = SyncServiceRuntime("risk-engine-1")
    rt.start()
    db = rt.open_database("risk-1", metadata, synchronous="NORMAL")
    rt.serve({"order.submit": on_order})               # may return DEFERRED, reply later
    md = rt.subscribe("md-gateway", ["md.tick.", "md.status"])           # raw drain
    rt.subscribe_fact("trade-gateway", [td_exec(a) for a in accounts],
                      policy="FROM_BEGINNING", apply=apply_exec, cursors=FactCursors(db).load_all())
    rt.watch_peers(["md-gateway", "trade-gateway"])
    while rt.running:
        rt.poll(rt.next_timer_ms())
        rt.pump_system()                  # 0) sys.clock + heartbeats (liveness, FACT heads)
        for env in md.drain(): ...        # 1) market data (conflate per instrument)
        rt.pump_fact()                    # 2) td.exec: FactTracker -> apply (seq order, repairs)
        for c in rt.pump_replies(): ...   # 3) replies of async requests (callbacks or Completions)
        rt.pump_requests(limit=256)       # 4) commands (ctl.* handled by the runtime)
        ...                               # 5-6) marks, dirty accounts
        for c in rt.run_timers(): ...     # 7) rpc timeouts/retries, repair backoff, 2PC, clock
        rt.loop_done()                    # loop_lag_ms bookkeeping
    rt.stop()

Requests never block: ``rt.request(target, command, payload, on_reply=cb)``
returns at once; replies and timeouts arrive through ``pump_replies`` /
``run_timers``. Handlers can return ``DEFERRED`` and answer later with
``rt.reply(ctx, reply)`` (``order.submit`` after persist + route,
``eod.rollover`` after the ``settle.sod_snapshot`` sub-request).

If the loop dies with an exception, call ``rt.fatal(exc, where)`` (``rt.run``
and ``with rt:`` do it for you) so the databases close with
``clean_shutdown=0``. Signal handlers only record the signal; the loop turns it
into ``request_stop`` (``running`` becomes False at once).
"""

from __future__ import annotations

import contextlib
import queue
import signal
import sys
import threading
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TypeVar

import zmq
from loguru import logger
from sqlalchemy import Connection, MetaData

from futrisk.bus.fact import ApplyFn, ExpiredFn, FactTracker, RepairFailedFn, RepairOkFn, StreamChangeFn
from futrisk.bus.heartbeat import HeartbeatThread
from futrisk.bus.lv import CloneSync
from futrisk.bus.publisher import PublisherThread
from futrisk.bus.rpc_client import Completion, LoopRpcClient
from futrisk.bus.rpc_common import DEFERRED, Handler, HandlerResult, RequestContext
from futrisk.bus.rpc_server import RpcServer
from futrisk.bus.sockets import DEFAULT_SUB_HWM
from futrisk.bus.subscriber import Subscriber, SubStats
from futrisk.common.config import FullConfig
from futrisk.common.ids import uuid7
from futrisk.db.cursors import FactCursor
from futrisk.db.database import Database, Synchronous
from futrisk.db.migrations import Migration
from futrisk.messages import codes
from futrisk.messages.base import Msg, decode
from futrisk.messages.common import ReplayRequest, ShutdownRequest, SnapshotRequest
from futrisk.messages.enums import FactStartPolicy, ServiceState
from futrisk.messages.envelope import Envelope, Reply, err_reply, ok_reply
from futrisk.messages.system import ConfigPrepare, ConfigPrepared, DecisionQuery, Heartbeat
from futrisk.messages.topics import SYS_CLOCK, sys_heartbeat
from futrisk.runtime.base import API_GATEWAY, RuntimeBase
from futrisk.runtime.clock_sync import SUPERVISOR
from futrisk.runtime.parent import ParentWatch

T = TypeVar("T")
EnvelopeHandler = Callable[[Envelope], None]
_NS_PER_MS = 1_000_000
_CLOCK_RETRY_NS = 1_000 * _NS_PER_MS
_PEER_CHECK_NS = 100 * _NS_PER_MS


@dataclass
class SyncSubscription:
    """One SUB socket of the loop; ``drain`` for raw access, ``pump`` to dispatch."""

    subscriber: Subscriber
    dispatch: EnvelopeHandler | None = None
    clone: CloneSync | None = None

    @property
    def socket(self) -> zmq.Socket[Any]:
        return self.subscriber.socket

    @property
    def stats(self) -> SubStats:
        return self.subscriber.stats

    def drain(self, limit: int | None = None) -> list[Envelope]:
        """Accepted envelopes waiting on the socket (never blocks)."""
        return self.subscriber.drain(limit)

    def pump(self, limit: int | None = None) -> int:
        """Drain and dispatch to the handler (or Clone / FactTracker); returns count."""
        if self.dispatch is None:
            raise RuntimeError("subscription has no handler; use drain()")
        envs = self.subscriber.drain(limit)
        for env in envs:
            self.dispatch(env)
        return len(envs)


class SyncServiceRuntime(RuntimeBase):
    """Service runtime for poller-based services; see the module docstring."""

    def __init__(self, instance: str, **kwargs: Any) -> None:
        super().__init__(instance, **kwargs)
        self._ctx: zmq.Context[Any] | None = None
        self.poller = zmq.Poller()
        self.publisher: PublisherThread | None = None
        self.server: RpcServer | None = None
        self._rpc: LoopRpcClient | None = None
        self._hb_thread: HeartbeatThread | None = None
        self._subs: list[SyncSubscription] = []
        self._fact_subs: list[SyncSubscription] = []
        self._sys_sub: Subscriber | None = None
        self._hb_watched: set[str] = set()
        self._pending_handlers: list[tuple[str, Handler, type[Msg] | None]] = []
        self._stop_requested = threading.Event()
        self._pool: ThreadPoolExecutor | None = None
        self._offloaded: queue.SimpleQueue[tuple[Callable[[Any, BaseException | None], None], Any, BaseException | None]] = (
            queue.SimpleQueue()
        )
        self._signal_pending: int | None = None
        self._wake_rx: zmq.Socket[Any] | None = None
        self._wake_tx: zmq.Socket[Any] | None = None
        self._wake_lock = threading.Lock()
        self._parent_thread: threading.Thread | None = None
        self._clock_retry_at: int | None = None
        self._clock_in_flight = False
        self._next_peer_check = 0
        self._last_timers_ns: int | None = None
        self._peer_grace_until = 0
        self.loop_stalls = 0  # run_timers gaps longer than the peer timeout (stats / tests)
        self._started = False
        self._stopped = False
        self._thread_ident: int | None = None

    # --------------------------------------------------------- lifecycle
    def start(self) -> None:
        """Load config, logging, bind PUB (thread) and ROUTER, start heartbeats and clock sync."""
        if self._started:
            raise RuntimeError("runtime already started")
        self._thread_ident = threading.get_ident()
        self._load_config()
        self._setup_logging()
        self._ctx = zmq.Context()
        inst = self.endpoints.instance(self.instance)
        try:
            if inst.pub:
                self.publisher = PublisherThread(
                    self._ctx,
                    inst.pub,
                    self._core,
                    sndhwm=self.endpoints.pub_hwm(self.instance),
                    linger_ms=self._pub_linger_ms(),
                    clock=self._wall,
                    name=f"pub:{self.instance}",
                )
                self.publisher.start()
            if inst.router:
                self.server = RpcServer(
                    self._ctx,
                    inst.router,
                    instance=self.instance,
                    epoch=self.epoch,
                    clock=self.market_clock,
                    whitelist=self._whitelist(),
                    rcvhwm=self.endpoints.router_hwm,
                    max_backlog=self.endpoints.router_hwm,
                    busy_code=self._busy_code(),
                )
                for command, (handler, ptype) in self._sync_control_handlers().items():
                    self.server.add_handler(command, handler, payload_type=ptype)
                for command, handler, pending_type in self._pending_handlers:
                    self.server.add_handler(command, handler, payload_type=pending_type)
                self._pending_handlers.clear()
                self.poller.register(self.server.socket, zmq.POLLIN)
        except BaseException:
            if self.publisher is not None:
                self.publisher.stop()
            self._ctx.destroy(linger=0)
            raise
        self._rpc = LoopRpcClient(
            self._ctx,
            self.endpoints,
            identity=self.instance,
            epoch=self.epoch,
            clock=self._wall,
            on_new_socket=lambda s: self.poller.register(s, zmq.POLLIN),
        )
        wake_addr = f"inproc://wake-{uuid7()}"  # offload results wake the poller at once
        self._wake_rx = self._ctx.socket(zmq.PAIR)
        self._wake_rx.bind(wake_addr)
        self._wake_tx = self._ctx.socket(zmq.PAIR)
        self._wake_tx.connect(wake_addr)
        self.poller.register(self._wake_rx, zmq.POLLIN)
        if self.publisher is not None:
            self._hb_thread = HeartbeatThread(self._emit_heartbeat, self.heartbeat_ms, name=f"hb:{self.instance}")
            self._hb_thread.start()
        if self.parent_pid is not None:
            self._parent_thread = threading.Thread(
                target=self._parent_watch, args=(self.parent_pid,), name=f"parent-watch:{self.instance}", daemon=True
            )
            self._parent_thread.start()
        if self._follow_clock:
            self._start_clock_follow()
        self.config_participant.recover()
        self.loop_done()
        self._started = True
        logger.info(
            "runtime.started",
            instance=self.instance,
            epoch=self.epoch,
            config_version=self.config.config_version,
            pub=inst.pub,
            router=inst.router,
        )

    def _sync_control_handlers(self) -> dict[str, tuple[Handler, type[Msg]]]:
        """Control handlers; outbox reads and config loading run on the offload pool (DEFERRED)."""
        handlers = self._control_handlers()
        replay, replay_type = handlers["ctl.replay"]
        participant = self.config_participant

        def replay_offloaded(command: str, payload: Msg, ctx: RequestContext) -> HandlerResult:
            def done(result: object, error: BaseException | None) -> None:
                if error is None and isinstance(result, Reply):
                    self.reply(ctx, result)
                else:
                    self.reply(ctx, err_reply(codes.INTERNAL, f"replay failed: {error}"))

            self.offload(lambda: replay(command, payload, ctx), done)
            return DEFERRED

        def prepare_offloaded(command: str, payload: Msg, ctx: RequestContext) -> HandlerResult:
            assert isinstance(payload, ConfigPrepare)

            def done(result: tuple[ConfigPrepared, FullConfig | None] | None, error: BaseException | None) -> None:
                if error is not None or result is None:
                    self.reply(ctx, err_reply(codes.INTERNAL, f"prepare failed: {error}"))
                    return
                self.reply(ctx, ok_reply(participant.stage(payload, result[0], result[1])))

            self.offload(lambda: participant.validate(payload), done)
            return DEFERRED

        handlers["ctl.replay"] = (replay_offloaded, replay_type)
        handlers["ctl.config.prepare"] = (prepare_offloaded, ConfigPrepare)
        return handlers

    def offload(self, fn: Callable[[], T], on_done: Callable[[T | None, BaseException | None], None]) -> None:
        """Run blocking ``fn`` on a worker thread; ``on_done(result, error)`` runs later on the
        business thread (from :meth:`run_timers`). Use it for slow reads or file work."""
        if self._pool is None:
            self._pool = ThreadPoolExecutor(max_workers=2, thread_name_prefix=f"offload:{self.instance}")
        fut = self._pool.submit(fn)

        def finished(f: Future[T]) -> None:
            if f.cancelled():  # runtime stopping
                return
            err = f.exception()
            self._offloaded.put((on_done, None if err is not None else f.result(), err))
            self._wake()

        fut.add_done_callback(finished)

    def _wake(self) -> None:
        """Wake the poller (thread-safe): the business loop then runs ``run_timers`` at once."""
        with self._wake_lock:
            tx = self._wake_tx
            if tx is not None:
                with contextlib.suppress(zmq.ZMQError):
                    tx.send(b"", zmq.NOBLOCK)

    def _drain_offloaded(self) -> None:
        rx = self._wake_rx
        if rx is not None:
            while True:
                try:
                    rx.recv(zmq.NOBLOCK)
                except zmq.ZMQError:
                    break
        while True:
            try:
                on_done, result, error = self._offloaded.get_nowait()
            except queue.Empty:
                return
            on_done(result, error)

    def __enter__(self) -> SyncServiceRuntime:
        self.start()
        return self

    def __exit__(self, exc_type: object, exc: BaseException | None, tb: object) -> None:
        if exc is not None:
            self.fatal(exc, "business loop")
        self.stop()

    @property
    def running(self) -> bool:
        """False once a stop was requested (``ctl.shutdown``, signal, ``request_stop``, parent gone)."""
        return self._started and not self._stop_requested.is_set() and self._signal_pending is None

    def fatal(self, exc: BaseException, where: str) -> None:
        """Unexpected error: log, exit code 1, request the stop; databases will close unclean."""
        if self._record_fatal(exc):
            logger.opt(exception=exc).critical("runtime.fatal", instance=self.instance, where=where)
        self.request_stop(f"fatal: {where}", exit_code=1)

    def _check_signal(self) -> None:
        sig = self._signal_pending
        if sig is not None and not self._stop_requested.is_set():
            self.request_stop(f"signal {sig}")

    def _parent_watch(self, pid: int) -> None:
        watch = ParentWatch(pid)
        while not self._stop_requested.is_set():
            if not watch.alive():
                logger.warning("runtime.parent_gone", instance=self.instance, parent_pid=pid)
                self.request_stop(f"parent process {pid} gone")
                self._wake()
                return
            self._stop_requested.wait(self._parent_poll_s)

    def request_stop(self, reason: str = "", *, exit_code: int | None = None) -> None:
        """Ask the loop to exit (thread-safe); the service then calls :meth:`stop`."""
        if self._stop_requested.is_set():
            return
        self.stop_reason = reason
        if exit_code is not None:
            self.exit_code = exit_code
        logger.info("runtime.stop_requested", instance=self.instance, reason=reason)
        self.set_state(ServiceState.STOPPING)
        self._stop_requested.set()

    def _on_shutdown_request(self, req: ShutdownRequest) -> None:
        self.request_stop(req.reason or "ctl.shutdown")

    def stop(self, *, clean: bool | None = None) -> None:
        """Graceful stop: STOPPING -> hooks -> drain DB + publisher -> close DBs -> sockets.

        Databases get ``clean_shutdown=1`` unless the stop follows ``fatal()``
        (``clean=None``, the default) or ``clean=False`` is passed. Call it on
        the business thread.
        """
        if self._stopped or not self._started:
            return
        self._check_signal()
        self._stopped = True
        self._stop_requested.set()
        self.set_state(ServiceState.STOPPING)
        for cb in reversed(self._stop_callbacks):
            try:
                cb()
            except Exception:
                logger.exception("runtime.stop_hook_failed", instance=self.instance)
        if self._pool is not None:
            self._pool.shutdown(wait=True, cancel_futures=True)
            self._drain_offloaded()
        if self.server is not None:
            self.server.close()
        for _ in range(2):  # commits -> FACT publish -> mark_published commits
            for db in self.databases:
                if db.is_open:
                    try:
                        db.flush(timeout=10)
                    except Exception:
                        logger.exception("db.flush_failed", db=db.name)
            if self.publisher is not None:
                self.publisher.flush()
        self._close_databases(clean=self._close_clean(clean))
        if self._hb_thread is not None:
            self._hb_thread.trigger()
            self._hb_thread.stop()
        if self.publisher is not None:
            self.publisher.stop()
        if self._rpc is not None:
            self._rpc.close()
        for sub in self._subs:
            sub.subscriber.close()
        if self._sys_sub is not None:
            self._sys_sub.close()
        with self._wake_lock:
            for sock in (self._wake_tx, self._wake_rx):
                if sock is not None:
                    sock.close(0)
            self._wake_tx = self._wake_rx = None
        if self._ctx is not None:
            self._ctx.destroy(linger=0)
        logger.info("runtime.stopped", instance=self.instance, reason=self.stop_reason, exit_code=self.exit_code)

    def install_signal_handlers(self) -> None:
        """Ctrl-C / SIGTERM / (Windows) CTRL_BREAK -> stop. Main thread only.

        The handler only records the signal (it may interrupt code holding
        locks); ``running`` turns False at once and the loop performs
        ``request_stop`` on its next pass.
        """

        def handler(signum: int, _frame: object) -> None:
            self._signal_pending = signum  # plain store: nothing else is safe inside a signal handler

        signal.signal(signal.SIGINT, handler)
        signal.signal(signal.SIGTERM, handler)
        if sys.platform == "win32":
            signal.signal(signal.SIGBREAK, handler)

    # --------------------------------------------------------- heartbeat
    def _emit_heartbeat(self) -> None:
        if self.publisher is not None:
            self.publisher.publish(self.heartbeat_topic, self.build_heartbeat())

    def _heartbeat_now(self) -> None:
        if self._hb_thread is not None:
            self._hb_thread.trigger()

    def _builtin_queues(self) -> dict[str, int]:
        q = super()._builtin_queues()
        if self.publisher is not None:
            q["publisher"] = self.publisher.queue_size()
        if self.server is not None:
            q["router_backlog"] = self.server.backlog_size
        if self._rpc is not None:
            q["rpc_pending"] = self._rpc.pending_count
        return q

    # ------------------------------------------------------------- loop
    def poll(self, timeout_ms: int | None = None) -> dict[zmq.Socket[Any], int]:
        """Wait on every registered socket (default timeout: :meth:`next_timer_ms`)."""
        t = self.next_timer_ms() if timeout_ms is None else max(0, timeout_ms)
        ready = dict(self.poller.poll(t))
        self._check_signal()
        return ready

    def next_timer_ms(self, cap_ms: int = 100) -> int:
        """Milliseconds until the next runtime timer (capped), for ``poller.poll``."""
        now = self._wall.monotonic_ns()
        if not self._offloaded.empty():
            return 0
        candidates = [now + cap_ms * _NS_PER_MS]
        if self._rpc is not None:
            t = self._rpc.next_timer_ns()
            if t is not None:
                candidates.append(t)
        if self._fact is not None:
            t = self._fact.next_timer_ns()
            if t is not None:
                candidates.append(t)
        for clone in self._clones:
            t = clone.next_timer_ns()
            if t is not None:
                candidates.append(t)
        if self._clock_retry_at is not None and not self._clock_in_flight:
            candidates.append(self._clock_retry_at)
        t = self.config_participant.next_timer_ns()
        if t is not None:
            candidates.append(t)
        return max(0, (min(candidates) - now + _NS_PER_MS - 1) // _NS_PER_MS)

    def pump_system(self) -> int:
        """Drain the runtime's own SUB socket: ``sys.clock`` and watched heartbeats.

        Before a FACT publisher's heartbeat is evaluated (tail gaps), the FACT
        sockets are drained, so facts already received are applied first and a
        busy loop does not mistake them for lost ones.
        """
        self._check_signal()
        if self._sys_sub is None:
            return 0
        envs = self._sys_sub.drain()
        if self._fact is not None and self._fact_subs:
            publishers = self._fact.publishers()
            if any(e.type == "Heartbeat" and e.src in publishers for e in envs):
                self.pump_fact()
        for env in envs:
            if env.topic == SYS_CLOCK:
                self.clock_follower.on_envelope(env)
            elif env.type == "Heartbeat":
                self._route_heartbeat(decode(env.payload, Heartbeat))
        return len(envs)

    def pump_fact(self, limit: int | None = None) -> int:
        """Drain FACT subscriptions into the tracker (``apply`` runs here, in seq order)."""
        return sum(s.pump(limit) for s in self._fact_subs)

    def pump_subscriptions(self, limit: int | None = None) -> int:
        """Pump every subscription that has a handler (plain, snapshot and FACT)."""
        return sum(s.pump(limit) for s in self._subs if s.dispatch is not None)

    def pump_requests(self, limit: int = 256) -> int:
        """Receive commands and dispatch at most ``limit`` (``ctl.*`` handled internally)."""
        if self.server is None:
            return 0
        return self.server.pump(limit)

    def pump_replies(self) -> list[Completion]:
        """Replies to async requests: callbacks run here; callback-less ones are returned."""
        if self._rpc is None:
            return []
        return self._rpc.process_replies()

    def run_timers(self) -> list[Completion]:
        """RPC timeouts/retries, FACT repair backoff, Clone retries, peer liveness, clock sync, 2PC.

        Peer liveness survives business-loop stalls (a long commit, maintenance):
        heartbeats received meanwhile wait on the system socket, so before a
        peer is declared DOWN the socket is drained (:meth:`pump_system`), and
        after a stall longer than the peer timeout no peer is declared DOWN for
        one more heartbeat interval.
        """
        now = self._wall.monotonic_ns()
        self._check_signal()
        self._drain_offloaded()
        done = self._rpc.poll_timers(now) if self._rpc is not None else []
        if self._fact is not None:
            self._fact.poll_timers(now)
        for clone in self._clones:
            clone.poll_timers(now)
        self._peer_liveness(now)
        if (
            self._follow_clock
            and not self.clock_follower.ready
            and not self._clock_in_flight
            and self._clock_retry_at is not None
            and now >= self._clock_retry_at
        ):
            self._request_clock_snapshot()
        participant = self.config_participant
        version = participant.due_decision(now)
        if version is not None:
            participant.mark_queried(now)
            self.request(
                API_GATEWAY,
                "config.decision",
                DecisionQuery(config_version=version),
                on_reply=participant.on_decision_reply,
                retries=0,
            )
        return done

    def _peer_liveness(self, now: int) -> None:
        last, self._last_timers_ns = self._last_timers_ns, now
        if last is not None and now - last > self.peers.timeout_ns:
            self.loop_stalls += 1
            self._peer_grace_until = now + self.heartbeat_ms * _NS_PER_MS
            logger.warning("loop.stall", instance=self.instance, stalled_ms=(now - last) // _NS_PER_MS)
        if now < self._next_peer_check:
            return
        self._next_peer_check = now + _PEER_CHECK_NS
        if now < self._peer_grace_until or not self.peers.overdue(now):
            return
        self.pump_system()  # a heartbeat still waiting on the socket is not a silent peer
        self.peers.check(self._wall.monotonic_ns())

    def run(
        self,
        step: Callable[[], None] | None = None,
        *,
        request_limit: int = 256,
        on_completion: Callable[[Completion], None] | None = None,
        max_wait_ms: int = 100,
    ) -> None:
        """A standard loop until stopped: system -> subscriptions -> replies -> requests -> step -> timers.

        ``max_wait_ms`` caps the poller wait so ``step`` runs at least that often.
        """
        try:
            while self.running:
                self.poll(self.next_timer_ms(cap_ms=max_wait_ms))
                self.pump_system()
                self.pump_subscriptions()
                completions = self.pump_replies()
                self.pump_requests(request_limit)
                if step is not None:
                    step()
                completions.extend(self.run_timers())
                for c in completions:
                    if on_completion is not None:
                        on_completion(c)
                    else:
                        logger.warning("rpc.unhandled_completion", command=c.command, code=c.reply.code)
                self.loop_done()
        except BaseException as exc:
            self.fatal(exc, "business loop")
            raise

    # ------------------------------------------------------------ publish
    def publish(self, topic: str, body: Msg, *, td: str | None = None, corr_id: str | None = None) -> int:
        """Publish an LV message (thread-safe); FACTs go through ``append_fact`` in a DB transaction."""
        if self.publisher is None:
            raise RuntimeError(f"{self.instance} has no PUB endpoint")
        return self.publisher.publish(topic, body, td=td, corr_id=corr_id)

    # ----------------------------------------------------------- database
    def open_database(
        self,
        name: str,
        metadata: MetaData,
        *,
        migrations: Sequence[Migration] | None = None,
        synchronous: Synchronous = "NORMAL",
        outbox: bool = True,
        path: str | Path | None = None,
    ) -> Database:
        """Open ``<home>/db/<name>.db`` (migrations + db_meta) and wire its outbox to the publisher thread.

        Unpublished outbox rows of a previous run are republished before this returns.
        """
        db = self._create_database(name, metadata, migrations=migrations, synchronous=synchronous, path=path)
        db.open()
        self.databases.append(db)
        if outbox and db.tables.outbox is not None:
            if self.publisher is None:
                raise RuntimeError(f"{self.instance}: an outbox needs a PUB endpoint")
            ob = self._register_outbox(db)

            def mark(ids: list[int], ns: int) -> None:
                ob.submit_mark_published(ids, ns)

            self.publisher.set_fact_published_callback(mark)
            ob.attach_sink(self.publisher.publish_fact_rows).result()
        return db

    # ---------------------------------------------------------------- rpc
    @property
    def rpc(self) -> LoopRpcClient:
        if self._rpc is None:
            raise RuntimeError("runtime not started")
        return self._rpc

    def serve(self, handlers: Mapping[str, Handler]) -> None:
        """Register command handlers (synchronous; may return ``DEFERRED``); allowed before ``start``."""
        for command, handler in handlers.items():
            self.add_handler(command, handler)

    def add_handler(self, command: str, handler: Handler, *, payload_type: type[Msg] | None = None) -> None:
        if self.server is not None:
            self.server.add_handler(command, handler, payload_type=payload_type)
        else:
            self._pending_handlers.append((command, handler, payload_type))

    def reply(self, ctx: RequestContext, reply: Reply) -> None:
        """Answer a request now or later (business thread)."""
        if self.server is None:
            raise RuntimeError(f"{self.instance} has no ROUTER endpoint")
        self.server.reply(ctx, reply)

    def idempotent(
        self,
        db: Database,
        command: str,
        requester: str,
        command_id: str,
        payload: Msg,
        handler: Callable[[Connection], Reply],
        *,
        trading_day: str,
        received_ns: int | None = None,
        critical: bool = False,
    ) -> Reply:
        """Run a state-changing command exactly once per idempotency key (02 §4.7, 03 §4.0).

        Check ``processed_commands``, run ``handler(conn)`` and record its reply
        in ONE writer transaction (blocks for the commit). Duplicates get the
        stored reply with ``duplicate=True``; the same key with other content
        gets ``CONFLICT``.
        """
        job = self._idempotent_job(
            db, command, requester, command_id, payload, handler, trading_day=trading_day, received_ns=received_ns
        )
        return db.write(job, critical=critical)

    def connect(self, target: str) -> None:
        """Create the DEALER of ``target`` early so it is connected before the first request."""
        self.rpc.connect(target)

    def request(
        self,
        target: str,
        command: str,
        payload: Msg,
        *,
        on_reply: Callable[[Reply], None] | None = None,
        context: Any = None,
        timeout_ms: int | None = None,
        retries: int | None = None,
        deadline_ns: int | None = None,
    ) -> str:
        """Non-blocking request; returns the corr_id. See :class:`LoopRpcClient`."""
        return self.rpc.send(
            target,
            command,
            payload,
            timeout_ms=timeout_ms,
            retries=retries,
            deadline_ns=deadline_ns,
            on_reply=on_reply,
            context=context,
        )

    # ------------------------------------------------------ subscriptions
    def _new_sub(self, name: str, rcvhwm: int) -> Subscriber:
        if self._ctx is None:
            raise RuntimeError("runtime not started")
        sub = Subscriber(self._ctx, rcvhwm=rcvhwm, name=name)
        self.poller.register(sub.socket, zmq.POLLIN)
        return sub

    def subscribe(
        self,
        publisher: str,
        prefixes: Sequence[str],
        handler: EnvelopeHandler | None = None,
        *,
        snapshot: bool = False,
        rcvhwm: int = DEFAULT_SUB_HWM,
    ) -> SyncSubscription:
        """New SUB socket connected to ``publisher`` (exact-topic filtered).

        Without ``handler`` use ``sub.drain()``; with it ``sub.pump()``.
        ``snapshot=True`` (needs ``handler``) runs the late-joiner Clone sync;
        it also watches the publisher's heartbeat (delivered by
        :meth:`pump_system`), which tells it when the publisher restarted.
        """
        sub = self._new_sub(f"sub:{publisher}", rcvhwm)
        sub.connect(publisher, self.endpoints.pub(publisher), prefixes)
        clone: CloneSync | None = None
        dispatch = handler
        if snapshot:
            if handler is None:
                raise ValueError("snapshot=True needs a handler")
            clone = CloneSync(publisher, prefixes, handler, self._snapshot_requester, clock=self._wall)
            self._clones.append(clone)
            dispatch = clone.on_message
            clone.start()
            self._watch_heartbeats([publisher])  # its epoch/stream decide which incarnation is current
        s = SyncSubscription(sub, dispatch, clone)
        self._subs.append(s)
        return s

    def _snapshot_requester(self, publisher: str, req: SnapshotRequest, cb: Callable[[Reply], None]) -> None:
        self.request(publisher, "ctl.snapshot", req, on_reply=cb, retries=0)

    def _replay_requester(self, src: str, req: ReplayRequest, cb: Callable[[Reply], None]) -> None:
        self.request(src, "ctl.replay", req, on_reply=cb, retries=0)

    def subscribe_fact(
        self,
        publisher: str,
        prefixes: Sequence[str],
        *,
        policy: FactStartPolicy | str,
        apply: ApplyFn,
        cursors: Mapping[tuple[str, str], FactCursor] | None = None,
        on_expired: ExpiredFn | None = None,
        on_stream_change: StreamChangeFn | None = None,
        on_repair_failed: RepairFailedFn | None = None,
        on_repair_ok: RepairOkFn | None = None,
        rcvhwm: int = DEFAULT_SUB_HWM,
    ) -> FactTracker:
        """Consume FACT topics through the FactTracker (02 §8.2); pump with :meth:`pump_fact`.

        ``apply(env, cursor)`` runs on the business thread and should persist
        synchronously (``db.write``) the effects and the cursor in one transaction.
        """
        tracker = self._ensure_fact()
        if cursors:
            tracker.load_cursors(cursors)
        for prefix in prefixes:
            tracker.subscribe(
                publisher,
                prefix,
                policy=policy,
                apply=apply,
                on_expired=on_expired,
                on_stream_change=on_stream_change,
                on_repair_failed=on_repair_failed,
                on_repair_ok=on_repair_ok,
            )
        sub = self._new_sub(f"fact:{publisher}", rcvhwm)
        sub.connect(publisher, self.endpoints.pub(publisher), prefixes)
        s = SyncSubscription(sub, tracker.on_fact)
        self._subs.append(s)
        self._fact_subs.append(s)
        self._watch_heartbeats([publisher])
        self.rpc.connect(publisher)
        return tracker

    def _ensure_fact(self) -> FactTracker:
        if self._fact is None:
            self._fact = FactTracker(self._replay_requester, clock=self._wall)
        return self._fact

    def watch_peers(self, peers: Sequence[str], on_change: Callable[[str, str], None] | None = None) -> None:
        """Track peer liveness; ``on_change(peer, "UP"|"DOWN")`` runs on the business thread."""
        if on_change is not None:
            self._peer_callbacks.append(on_change)
        self.peers.watch(peers)
        self._watch_heartbeats(peers)

    def on_heartbeat(self, handler: Callable[[Heartbeat], None], publishers: Sequence[str] | None = None) -> None:
        """Observe heartbeats (delivered by :meth:`pump_system`)."""
        self._hb_handlers.append(handler)
        self._watch_heartbeats(publishers if publishers is not None else self.endpoints.publishers())

    def _system_sub(self) -> Subscriber:
        if self._sys_sub is None:
            self._sys_sub = self._new_sub("system", DEFAULT_SUB_HWM)
        return self._sys_sub

    def _watch_heartbeats(self, publishers: Sequence[str]) -> None:
        sub = self._system_sub()
        for p in publishers:
            if p in self._hb_watched:
                continue
            try:
                addr = self.endpoints.pub(p)
            except KeyError:
                logger.warning("heartbeat.no_pub_endpoint", peer=p)
                continue
            sub.connect(p, addr, [sys_heartbeat(p)])
            self._hb_watched.add(p)

    # --------------------------------------------------------------- clock
    def _start_clock_follow(self) -> None:
        if not self.endpoints.has(SUPERVISOR) or not self.endpoints.instance(SUPERVISOR).pub:
            logger.warning("clock.no_supervisor", instance=self.instance)
            self.clock_follower.mark_ready()
            return
        self._system_sub().connect(SUPERVISOR, self.endpoints.pub(SUPERVISOR), [SYS_CLOCK])
        self.rpc.connect(SUPERVISOR)
        self._clock_retry_at = self._wall.monotonic_ns()
        self._request_clock_snapshot()

    def _request_clock_snapshot(self) -> None:
        self._clock_in_flight = True

        def on_reply(reply: Reply) -> None:
            self._clock_in_flight = False
            if not self.clock_follower.on_snapshot_reply(reply) and not self.clock_follower.ready:
                self._clock_retry_at = self._wall.monotonic_ns() + _CLOCK_RETRY_NS

        self.request(
            SUPERVISOR,
            "ctl.snapshot",
            SnapshotRequest(prefixes=[SYS_CLOCK]),
            on_reply=on_reply,
            timeout_ms=2_000,
            retries=0,
        )

    def wait_clock_ready(self, timeout_s: float, *, step: Callable[[], None] | None = None) -> bool:
        """Drive the loop until the clock is synced (start-up helper; returns False on timeout)."""
        deadline = self._wall.monotonic_ns() + int(timeout_s * 1e9)
        while not self.clock_follower.ready and self._wall.monotonic_ns() < deadline and self.running:
            self.poll(min(self.next_timer_ms(), 50))
            self.pump_system()
            self.pump_replies()
            self.pump_requests()
            if step is not None:
                step()
            self.run_timers()
            self.loop_done()
        return self.clock_follower.ready


__all__ = ["SyncServiceRuntime", "SyncSubscription"]
