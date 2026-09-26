"""Runtime for asyncio services: md-gateway, clearing-worker, api-gateway, supervisor (02 §4.8, §8.5, §8.6).

Lifecycle::

    async def main() -> int:
        rt = AsyncServiceRuntime("clearing-worker")          # or config=..., home=... in tests
        await rt.start()                                      # config, logs, PUB/ROUTER, heartbeat (STARTING)
        db = await rt.open_database("clearing", metadata, synchronous="FULL")
        rt.serve({"settle.run": handle_settle_run})           # async or sync handlers
        rt.subscribe_fact("trade-gateway", ["td.exec."], policy="FROM_BEGINNING", apply=apply_trade,
                          cursors=FactCursors(db).load_all())
        await rt.wait_clock_ready()                           # sys.clock snapshot from the supervisor
        rt.set_state("RUNNING")
        return await rt.run_until_stopped()                   # ctl.shutdown / Ctrl-C -> graceful stop

Everything runs on the event loop thread except the database writers; never
block the loop (use ``db.run_write``/``run_read``, thread or process pools).

Ordered writes (``submit_write``) form one chain per runtime: if one fails,
every later ``submit_write`` raises :class:`WriteChainBroken` without writing,
the service stops with exit code 1 and its databases are closed with
``clean_shutdown=0`` - a FACT whose apply failed is never skipped by later
cursors.
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import signal
import sys
import threading
from collections.abc import Awaitable, Callable, Mapping, Sequence
from concurrent.futures import Future
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, TypeVar

import msgspec
import zmq
import zmq.asyncio
from loguru import logger
from sqlalchemy import Connection, MetaData

from futrisk.bus.fact import ApplyFn, ExpiredFn, FactTracker, RepairFailedFn, RepairOkFn, StreamChangeFn
from futrisk.bus.heartbeat import heartbeat_loop
from futrisk.bus.lv import CloneSync
from futrisk.bus.publisher import AsyncPublisher
from futrisk.bus.rpc_client import AsyncRpcClient
from futrisk.bus.rpc_common import Handler, RequestContext, is_awaitable
from futrisk.bus.rpc_server import AsyncRpcServer
from futrisk.bus.sockets import DEFAULT_SUB_HWM
from futrisk.bus.subscriber import AsyncSubscriber, SubStats
from futrisk.db.cursors import FactCursor
from futrisk.db.database import Database, Synchronous, reject_async
from futrisk.db.migrations import Migration
from futrisk.db.processed import request_hash
from futrisk.messages import codes
from futrisk.messages.base import Msg, decode
from futrisk.messages.common import ReplayRequest, ShutdownRequest, SnapshotRequest
from futrisk.messages.enums import FactStartPolicy, ServiceState
from futrisk.messages.envelope import Envelope, Reply, err_reply, ok_reply
from futrisk.messages.system import ConfigPrepare, DecisionQuery, Heartbeat
from futrisk.messages.topics import SYS_CLOCK, sys_heartbeat
from futrisk.runtime.base import API_GATEWAY, RuntimeBase
from futrisk.runtime.clock_sync import SUPERVISOR
from futrisk.runtime.loop import ensure_selector_loop
from futrisk.runtime.parent import ParentWatch

T = TypeVar("T")
EnvelopeHandler = Callable[[Envelope], Any]
_PROBE_S = 0.1
_TIMER_S = 0.05


class WriteChainBroken(RuntimeError):
    """An earlier ``submit_write`` failed; later ordered writes are refused (nothing commits past it)."""


@dataclass
class _InFlight:
    request_hash: str
    done: asyncio.Event = field(default_factory=asyncio.Event)
    reply: Reply | None = None
    error: BaseException | None = None


@dataclass
class AsyncSubscription:
    """Handle of one SUB socket and its receive task."""

    subscriber: AsyncSubscriber
    task: asyncio.Task[None]
    clone: CloneSync | None = None

    @property
    def stats(self) -> SubStats:
        return self.subscriber.stats


class AsyncServiceRuntime(RuntimeBase):
    """Service runtime for asyncio services; see the module docstring."""

    def __init__(self, instance: str, **kwargs: Any) -> None:
        super().__init__(instance, **kwargs)
        self._loop: asyncio.AbstractEventLoop | None = None
        self._loop_thread: int | None = None
        self._actx: zmq.asyncio.Context | None = None
        self._sctx: zmq.Context[Any] | None = None
        self.publisher: AsyncPublisher | None = None
        self.server: AsyncRpcServer | None = None
        self._rpc: AsyncRpcClient | None = None
        self._tasks: set[asyncio.Task[Any]] = set()
        self._hb_task: asyncio.Task[Any] | None = None
        self._subs: list[AsyncSubscriber] = []
        self._hb_sub: AsyncSubscriber | None = None
        self._hb_watched: set[str] = set()
        self._pending_handlers: list[tuple[str, Handler, type[Msg] | None]] = []
        self._stop_event: asyncio.Event | None = None
        self._clock_event: asyncio.Event | None = None
        self._stop_timeout_s = 10.0
        self._started = False
        self._stopped = False
        self._write_failure: BaseException | None = None
        self._write_lock = threading.Lock()
        self._inflight: dict[tuple[str, str], _InFlight] = {}
        self.loop_stalls = 0  # event-loop stalls longer than the peer timeout (stats / tests)

    # --------------------------------------------------------- lifecycle
    async def start(self) -> None:
        """Load config, set up logging, bind PUB/ROUTER, start heartbeat and clock sync."""
        if self._started:
            raise RuntimeError("runtime already started")
        loop = asyncio.get_running_loop()
        ensure_selector_loop(loop)
        self._loop = loop
        self._loop_thread = threading.get_ident()
        self._load_config()
        self._setup_logging()
        self._stop_event = asyncio.Event()
        self._clock_event = asyncio.Event()
        clock_event = self._clock_event
        self.clock_follower.add_ready_callback(clock_event.set)
        self._actx = zmq.asyncio.Context()
        self._sctx = zmq.Context.shadow(self._actx.underlying)
        inst = self.endpoints.instance(self.instance)
        try:
            if inst.pub:
                self.publisher = AsyncPublisher(
                    self._sctx,
                    inst.pub,
                    self._core,
                    sndhwm=self.endpoints.pub_hwm(self.instance),
                    linger_ms=self._pub_linger_ms(),
                    clock=self._wall,
                )
                self.publisher.start(loop)
            if inst.router:
                self.server = AsyncRpcServer(
                    self._actx,
                    inst.router,
                    instance=self.instance,
                    epoch=self.epoch,
                    clock=self.market_clock,
                    whitelist=self._whitelist(),
                    rcvhwm=self.endpoints.router_hwm,
                    busy_code=self._busy_code(),
                )
                for command, (handler, ptype) in self._async_control_handlers().items():
                    self.server.add_handler(command, handler, payload_type=ptype)
                for command, handler, pending_type in self._pending_handlers:
                    self.server.add_handler(command, handler, payload_type=pending_type)
                self._pending_handlers.clear()
                await self.server.start()
        except BaseException:
            if self.publisher is not None:
                self.publisher.close()
            self._actx.destroy(linger=0)
            raise
        self._rpc = AsyncRpcClient(self._actx, self.endpoints, identity=self.instance, epoch=self.epoch, clock=self._wall)
        if self.publisher is not None:
            self._hb_task = self._spawn(heartbeat_loop(self._emit_heartbeat, self.heartbeat_ms), "heartbeat")
        self._spawn(self._lag_probe(), "loop-lag")
        self._spawn(self._timer_loop(), "timers", fatal=True)
        if self._follow_clock:
            self._start_clock_follow()
        if self.parent_pid is not None:
            self._spawn(self._parent_watch(self.parent_pid), "parent-watch")
        self.config_participant.recover()
        self._started = True
        logger.info(
            "runtime.started",
            instance=self.instance,
            epoch=self.epoch,
            config_version=self.config.config_version,
            pub=inst.pub,
            router=inst.router,
        )

    def _async_control_handlers(self) -> dict[str, tuple[Handler, type[Msg]]]:
        """Control handlers with the blocking ones (outbox read, config load) moved off the loop."""
        handlers = self._control_handlers()
        replay, replay_type = handlers["ctl.replay"]
        participant = self.config_participant

        async def replay_off_loop(command: str, payload: Msg, ctx: RequestContext) -> Reply:
            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(None, replay, command, payload, ctx)
            assert isinstance(result, Reply)
            return result

        async def prepare_off_loop(command: str, payload: Msg, ctx: RequestContext) -> Reply:
            assert isinstance(payload, ConfigPrepare)
            loop = asyncio.get_running_loop()
            result, new = await loop.run_in_executor(None, participant.validate, payload)
            return ok_reply(participant.stage(payload, result, new))

        handlers["ctl.replay"] = (replay_off_loop, replay_type)
        handlers["ctl.config.prepare"] = (prepare_off_loop, ConfigPrepare)
        return handlers

    async def __aenter__(self) -> AsyncServiceRuntime:
        await self.start()
        return self

    async def __aexit__(self, exc_type: object, exc: BaseException | None, tb: object) -> None:
        if exc is not None and not isinstance(exc, asyncio.CancelledError):
            self._record_fatal(exc)  # unexpected error in the service: close databases unclean
        await self.stop()

    def request_stop(self, reason: str = "", *, deadline_ms: int | None = None, exit_code: int | None = None) -> None:
        """Ask the runtime to stop (thread-safe); ``run_until_stopped`` then performs the shutdown."""
        loop = self._loop
        if loop is None:
            return
        if threading.get_ident() == self._loop_thread:
            self._begin_stop(reason, deadline_ms, exit_code)
        else:
            with contextlib.suppress(RuntimeError):  # loop already closed
                loop.call_soon_threadsafe(self._begin_stop, reason, deadline_ms, exit_code)

    def _begin_stop(self, reason: str, deadline_ms: int | None, exit_code: int | None) -> None:
        if self._stop_event is None or self._stop_event.is_set():
            return
        self.stop_reason = reason
        if exit_code is not None:
            self.exit_code = exit_code
        if deadline_ms:
            self._stop_timeout_s = max(0.5, deadline_ms / 1000)
        logger.info("runtime.stop_requested", instance=self.instance, reason=reason)
        self.set_state(ServiceState.STOPPING)
        self._stop_event.set()

    def _on_shutdown_request(self, req: ShutdownRequest) -> None:
        self.request_stop(req.reason or "ctl.shutdown", deadline_ms=req.deadline_ms)

    def fatal(self, exc: BaseException, where: str) -> None:
        """Unexpected error: log, exit code 1, stop with ``clean_shutdown=0`` (supervisor restarts us, 01 §8.6).

        Thread-safe.
        """
        if isinstance(exc, WriteChainBroken) and self._fatal_error is not None:
            return  # consequence of the first failure, already reported
        if self._record_fatal(exc):
            logger.opt(exception=exc).critical("runtime.fatal", instance=self.instance, where=where)
        self.request_stop(f"fatal: {where}", exit_code=1)

    async def wait_stopped(self) -> None:
        assert self._stop_event is not None
        await self._stop_event.wait()

    async def run_until_stopped(self) -> int:
        """Wait for ``ctl.shutdown``/signal/``request_stop``, stop gracefully, return the exit code."""
        await self.wait_stopped()
        await self.stop()
        return self.exit_code

    async def stop(self, *, clean: bool | None = None) -> None:
        """Graceful stop: STOPPING -> service hooks -> drain -> close DBs -> close sockets.

        Databases get ``clean_shutdown=1`` unless the stop follows a fatal error
        (``clean=None``, the default) or ``clean=False`` is passed.
        """
        if self._stopped or not self._started:
            return
        self._stopped = True
        self.set_state(ServiceState.STOPPING)
        if self._stop_event is not None:
            self._stop_event.set()
        for cb in reversed(self._stop_callbacks):
            try:
                res = cb()
                if is_awaitable(res):
                    await asyncio.wait_for(res, self._stop_timeout_s)
            except Exception:
                logger.exception("runtime.stop_hook_failed", instance=self.instance)
        if self.server is not None:
            await self.server.close(timeout=min(2.0, self._stop_timeout_s))
        await self._cancel_tasks(keep_heartbeat=True)
        loop = asyncio.get_running_loop()
        for _ in range(2):  # commits -> FACT publish -> mark_published commits
            for db in self.databases:
                if db.is_open:
                    await loop.run_in_executor(None, db.flush)
            await asyncio.sleep(0)
        close_clean = self._close_clean(clean)
        for db in reversed(self.databases):
            await loop.run_in_executor(None, lambda d=db: d.close(clean=close_clean))  # type: ignore[misc]
        self._emit_heartbeat()
        await self._cancel_tasks(keep_heartbeat=False)
        if self._rpc is not None:
            await self._rpc.close()
        for sub in self._subs:
            sub.close()
        if self.publisher is not None:
            self.publisher.close()
        if self._actx is not None:
            self._actx.destroy(linger=0)
        logger.info("runtime.stopped", instance=self.instance, reason=self.stop_reason, exit_code=self.exit_code)

    def install_signal_handlers(self) -> None:
        """Ctrl-C / SIGTERM / (Windows) CTRL_BREAK -> graceful stop. Main thread only."""

        loop = self._loop
        if loop is None:
            raise RuntimeError("start the runtime before installing signal handlers")

        def handler(signum: int, _frame: object) -> None:
            # may interrupt arbitrary loop code: always hop through the loop's wake-up pipe
            with contextlib.suppress(RuntimeError):
                loop.call_soon_threadsafe(self._begin_stop, f"signal {signum}", None, None)

        signal.signal(signal.SIGINT, handler)
        signal.signal(signal.SIGTERM, handler)
        if sys.platform == "win32":
            signal.signal(signal.SIGBREAK, handler)

    # ----------------------------------------------------------- tasks
    def _spawn(self, coro: Awaitable[T], name: str, *, fatal: bool = False) -> asyncio.Task[T]:
        task: asyncio.Task[T] = asyncio.ensure_future(coro)
        task.set_name(f"{self.instance}:{name}")
        self._tasks.add(task)

        def done(t: asyncio.Task[T]) -> None:
            self._tasks.discard(t)
            if t.cancelled():
                return
            exc = t.exception()
            if exc is None:
                return
            if fatal:
                self.fatal(exc, name)
            else:
                logger.opt(exception=exc).error("runtime.task_failed", task=name)

        task.add_done_callback(done)
        return task

    async def _cancel_tasks(self, *, keep_heartbeat: bool) -> None:
        current = asyncio.current_task()
        victims = [t for t in self._tasks if t is not current and not (keep_heartbeat and t is self._hb_task)]
        for t in victims:
            t.cancel()
        if victims:
            await asyncio.gather(*victims, return_exceptions=True)

    async def _lag_probe(self) -> None:
        while True:
            await asyncio.sleep(_PROBE_S)
            self.loop_done()

    def loop_lag_ms(self) -> int:
        return max(0, super().loop_lag_ms() - int(_PROBE_S * 1000))

    async def _parent_watch(self, pid: int) -> None:
        watch = ParentWatch(pid)
        while True:
            if not watch.alive():
                logger.warning("runtime.parent_gone", instance=self.instance, parent_pid=pid)
                self.request_stop(f"parent process {pid} gone")
                return
            await asyncio.sleep(self._parent_poll_s)

    async def _timer_loop(self) -> None:
        last = self._wall.monotonic_ns()
        grace_until = 0
        while True:
            await asyncio.sleep(_TIMER_S)
            now = self._wall.monotonic_ns()
            interval_ns = self.heartbeat_ms * 1_000_000
            if now - last > interval_ns:
                # the event loop was blocked: heartbeats received meanwhile are still queued for the
                # heartbeat task, which runs after this one; no DOWN for one more heartbeat interval
                grace_until = max(grace_until, now + interval_ns)
                if now - last > self.peers.timeout_ns:
                    self.loop_stalls += 1
                    logger.warning("loop.stall", instance=self.instance, stalled_ms=(now - last) // 1_000_000)
            last = now
            if now >= grace_until:
                self.peers.check(now)
            if self._fact is not None:
                self._fact.poll_timers(now)
            for clone in self._clones:
                clone.poll_timers(now)
            participant = self.config_participant
            version = participant.due_decision(now)
            if version is not None:
                participant.mark_queried(now)
                self.request_cb(
                    API_GATEWAY,
                    "config.decision",
                    DecisionQuery(config_version=version),
                    participant.on_decision_reply,
                    retries=0,
                )

    # ------------------------------------------------------- heartbeat
    def _emit_heartbeat(self) -> None:
        if self.publisher is not None and self._actx is not None and not self._actx.closed:
            self.publisher.publish(self.heartbeat_topic, self.build_heartbeat())

    def _heartbeat_now(self) -> None:
        if not self._started or self._loop is None:
            return
        if threading.get_ident() == self._loop_thread:
            self._emit_heartbeat()
        else:
            with contextlib.suppress(RuntimeError):
                self._loop.call_soon_threadsafe(self._emit_heartbeat)

    # ---------------------------------------------------------- publish
    def publish(self, topic: str, body: Msg, *, td: str | None = None, corr_id: str | None = None) -> int:
        """Publish an LV message (loop thread); FACTs go through ``append_fact`` in a DB transaction."""
        if self.publisher is None:
            raise RuntimeError(f"{self.instance} has no PUB endpoint")
        return self.publisher.publish(topic, body, td=td, corr_id=corr_id)

    # ---------------------------------------------------------- database
    async def open_database(
        self,
        name: str,
        metadata: MetaData,
        *,
        migrations: Sequence[Migration] | None = None,
        synchronous: Synchronous = "NORMAL",
        outbox: bool = True,
        path: str | Path | None = None,
    ) -> Database:
        """Open ``<home>/db/<name>.db`` (migrations + db_meta) and wire its outbox to the publisher.

        Unpublished outbox rows of a previous run are republished before this returns.
        The runtime closes the database (``clean_shutdown=1``) on stop.
        """
        db = self._create_database(name, metadata, migrations=migrations, synchronous=synchronous, path=path)
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, db.open)
        self.databases.append(db)
        if outbox and db.tables.outbox is not None:
            if self.publisher is None:
                raise RuntimeError(f"{self.instance}: an outbox needs a PUB endpoint")
            ob = self._register_outbox(db)
            publisher = self.publisher

            def mark(ids: list[int], ns: int) -> None:
                ob.submit_mark_published(ids, ns)

            publisher.set_fact_published_callback(mark)
            await asyncio.wrap_future(ob.attach_sink(publisher.publish_fact_rows))
        return db

    def submit_write(self, db: Database, fn: Callable[[Connection], T], *, critical: bool = False) -> Future[T]:
        """Queue an ordered write without waiting (e.g. inside a FACT ``apply``).

        Jobs run in submission order on the writer thread. If one fails (in
        ``fn`` or at commit), the chain is poisoned on the writer thread before
        the next job runs: every later ``submit_write`` job - and every later
        call - raises :class:`WriteChainBroken` without writing, the failure is
        fatal (exit code 1) and the databases close with ``clean_shutdown=0``.
        ``fn`` must be synchronous.
        """
        reject_async(fn, "submit_write job")
        failed = self._write_failure
        if failed is not None:
            raise WriteChainBroken(f"an earlier submit_write failed: {failed!r}") from failed

        def guarded(conn: Connection) -> T:
            earlier = self._write_failure
            if earlier is not None:
                raise WriteChainBroken(f"an earlier submit_write failed: {earlier!r}") from earlier
            try:
                return fn(conn)
            except BaseException as exc:
                self._poison(exc)
                raise

        fut = db.submit(guarded, critical=critical)

        def check(f: Future[T]) -> None:
            exc = f.exception()
            if exc is None:
                return
            self._poison(exc)  # commit failures: runs on the writer thread before its next batch
            if not isinstance(exc, WriteChainBroken):
                self.fatal(exc, f"db write ({db.name})")

        fut.add_done_callback(check)
        return fut

    def _poison(self, exc: BaseException) -> None:
        with self._write_lock:
            if self._write_failure is None:
                self._write_failure = exc

    @property
    def write_chain_broken(self) -> bool:
        """True once a ``submit_write`` failed (the service is stopping)."""
        return self._write_failure is not None

    async def idempotent(
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

        Check ``processed_commands``, run ``handler(conn)`` (business writes,
        returns the Reply) and record the result in ONE writer transaction. A
        concurrent duplicate with the same key waits for the first and gets its
        reply with ``duplicate=True`` (``CONFLICT`` if the content differs); a
        later duplicate gets the stored reply. ``handler`` must be synchronous.
        """
        key = (db.name, command_id)
        h = request_hash(payload)
        running = self._inflight.get(key)
        if running is not None:
            await running.done.wait()
            if running.request_hash != h:
                return err_reply(codes.CONFLICT, f"idempotency key {command_id} reused with different content")
            if running.reply is None:
                return err_reply(codes.INTERNAL, f"first attempt of {command_id} failed: {running.error!r}")
            return msgspec.structs.replace(running.reply, duplicate=True)
        entry = _InFlight(h)
        self._inflight[key] = entry
        try:
            job = self._idempotent_job(
                db, command, requester, command_id, payload, handler, trading_day=trading_day, received_ns=received_ns
            )
            reply = await db.run_write(job, critical=critical)
            entry.reply = reply
            return reply
        except BaseException as exc:
            entry.error = exc
            raise
        finally:
            entry.done.set()
            if self._inflight.get(key) is entry:
                del self._inflight[key]

    # --------------------------------------------------------------- rpc
    @property
    def rpc(self) -> AsyncRpcClient:
        if self._rpc is None:
            raise RuntimeError("runtime not started")
        return self._rpc

    def serve(self, handlers: Mapping[str, Handler]) -> None:
        """Register command handlers (sync or ``async def``); allowed before ``start``."""
        for command, handler in handlers.items():
            self.add_handler(command, handler)

    def add_handler(self, command: str, handler: Handler, *, payload_type: type[Msg] | None = None) -> None:
        if self.server is not None:
            self.server.add_handler(command, handler, payload_type=payload_type)
        else:
            self._pending_handlers.append((command, handler, payload_type))

    async def request(
        self,
        target: str,
        command: str,
        payload: Msg,
        *,
        timeout_ms: int | None = None,
        retries: int | None = None,
        deadline_ns: int | None = None,
    ) -> Reply:
        """Request/reply with the 02 §4.7 retry policy."""
        return await self.rpc.request(
            target, command, payload, timeout_ms=timeout_ms, retries=retries, deadline_ns=deadline_ns
        )

    def request_cb(
        self,
        target: str,
        command: str,
        payload: Msg,
        callback: Callable[[Reply], None],
        *,
        timeout_ms: int | None = None,
        retries: int | None = None,
    ) -> None:
        """Fire a request from sync code; ``callback(reply)`` runs on the loop (errors are fatal)."""

        async def run() -> None:
            reply = await self.rpc.request(target, command, payload, timeout_ms=timeout_ms, retries=retries)
            callback(reply)

        self._spawn(run(), f"req:{command}", fatal=True)

    def reply(self, ctx: Any, reply: Reply) -> None:
        """Answer a request whose handler returned ``DEFERRED`` (loop thread)."""
        if self.server is None:
            raise RuntimeError(f"{self.instance} has no ROUTER endpoint")
        self.server.reply(ctx, reply)

    # ----------------------------------------------------- subscriptions
    def subscribe(
        self,
        publisher: str,
        prefixes: Sequence[str],
        handler: EnvelopeHandler,
        *,
        snapshot: bool = False,
        rcvhwm: int = DEFAULT_SUB_HWM,
    ) -> AsyncSubscription:
        """Subscribe to ``publisher``; ``handler(env)`` gets every exact-matching envelope in order.

        ``handler`` may be ``async`` (awaited sequentially). ``snapshot=True``
        runs the late-joiner Clone sync (02 §4.6) and re-snapshots on publisher
        restarts, which it learns from the publisher's heartbeat (watched
        automatically); its handler must be synchronous. Handler errors are fatal.
        """
        if self._actx is None:
            raise RuntimeError("runtime not started")
        if snapshot and inspect.iscoroutinefunction(handler):
            raise TypeError("snapshot=True subscription handlers must be synchronous")
        sub = AsyncSubscriber(self._actx, rcvhwm=rcvhwm, name=f"sub:{publisher}")
        sub.connect(publisher, self.endpoints.pub(publisher), prefixes)
        self._subs.append(sub)
        clone: CloneSync | None = None
        dispatch: EnvelopeHandler
        if snapshot:

            def apply(env: Envelope) -> None:
                res = handler(env)
                if is_awaitable(res):
                    raise TypeError("snapshot=True subscription handlers must be synchronous")

            clone = CloneSync(publisher, prefixes, apply, self._snapshot_requester, clock=self._wall)
            self._clones.append(clone)
            dispatch = clone.on_message
            clone.start()
            self._watch_heartbeats([publisher])  # its epoch/stream decide which incarnation is current
        else:
            dispatch = handler
        task = self._spawn(self._sub_loop(sub, dispatch), f"sub:{publisher}", fatal=True)
        return AsyncSubscription(sub, task, clone)

    async def _sub_loop(self, sub: AsyncSubscriber, dispatch: EnvelopeHandler) -> None:
        while True:
            env = await sub.recv()
            res = dispatch(env)
            if is_awaitable(res):
                await res

    def _snapshot_requester(self, publisher: str, req: SnapshotRequest, cb: Callable[[Reply], None]) -> None:
        self.request_cb(publisher, "ctl.snapshot", req, cb, retries=0)

    def _replay_requester(self, src: str, req: ReplayRequest, cb: Callable[[Reply], None]) -> None:
        self.request_cb(src, "ctl.replay", req, cb, retries=0)

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
        """Consume FACT topics with cursors, gap repair and start policy (02 §8.2).

        Also watches the publisher's heartbeat (start position, tail gaps).
        ``apply(env, cursor)`` must persist the effects and ``cursor`` in one
        transaction (e.g. ``rt.submit_write(db, tx)``).
        """
        if self._actx is None:
            raise RuntimeError("runtime not started")
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
        sub = AsyncSubscriber(self._actx, rcvhwm=rcvhwm, name=f"fact:{publisher}")
        sub.connect(publisher, self.endpoints.pub(publisher), prefixes)
        self._subs.append(sub)
        self._spawn(self._sub_loop(sub, tracker.on_fact), f"fact:{publisher}", fatal=True)
        self._watch_heartbeats([publisher])
        return tracker

    def _ensure_fact(self) -> FactTracker:
        if self._fact is None:
            self._fact = FactTracker(self._replay_requester, clock=self._wall)
        return self._fact

    def watch_peers(self, peers: Sequence[str], on_change: Callable[[str, str], None] | None = None) -> None:
        """Track peer liveness from heartbeats; ``on_change(peer, "UP"|"DOWN")``."""
        if on_change is not None:
            self._peer_callbacks.append(on_change)
        self.peers.watch(peers)
        self._watch_heartbeats(peers)

    def on_heartbeat(self, handler: Callable[[Heartbeat], None], publishers: Sequence[str] | None = None) -> None:
        """Observe heartbeats of ``publishers`` (default: every configured publisher)."""
        self._hb_handlers.append(handler)
        self._watch_heartbeats(publishers if publishers is not None else self.endpoints.publishers())

    def _watch_heartbeats(self, publishers: Sequence[str]) -> None:
        if self._actx is None:
            raise RuntimeError("runtime not started")
        if self._hb_sub is None:
            self._hb_sub = AsyncSubscriber(self._actx, name="heartbeats")
            self._subs.append(self._hb_sub)
            self._spawn(self._sub_loop(self._hb_sub, self._on_heartbeat_env), "heartbeats", fatal=True)
        for p in publishers:
            if p in self._hb_watched:
                continue
            try:
                addr = self.endpoints.pub(p)
            except KeyError:
                logger.warning("heartbeat.no_pub_endpoint", peer=p)
                continue
            self._hb_sub.connect(p, addr, [sys_heartbeat(p)])
            self._hb_watched.add(p)

    def _on_heartbeat_env(self, env: Envelope) -> None:
        if env.type == "Heartbeat":
            self._route_heartbeat(decode(env.payload, Heartbeat))

    # ------------------------------------------------------------- clock
    def _start_clock_follow(self) -> None:
        if not self.endpoints.has(SUPERVISOR) or not self.endpoints.instance(SUPERVISOR).pub:
            logger.warning("clock.no_supervisor", instance=self.instance)
            self.clock_follower.mark_ready()
            return
        assert self._actx is not None
        sub = AsyncSubscriber(self._actx, name="clock")
        sub.connect(SUPERVISOR, self.endpoints.pub(SUPERVISOR), [SYS_CLOCK])
        self._subs.append(sub)
        self._spawn(self._sub_loop(sub, self.clock_follower.on_envelope), "clock", fatal=True)
        self._spawn(self._clock_snapshot_loop(), "clock-snapshot")

    async def _clock_snapshot_loop(self) -> None:
        while not self.clock_follower.ready:
            reply = await self.rpc.request(
                SUPERVISOR, "ctl.snapshot", SnapshotRequest(prefixes=[SYS_CLOCK]), timeout_ms=2_000, retries=0
            )
            if self.clock_follower.on_snapshot_reply(reply):
                break
            await asyncio.sleep(1.0)

    async def wait_clock_ready(self, timeout: float | None = None) -> bool:
        """Wait until the market clock was synced from ``sys.clock`` (immediate when not following).

        Returns False on timeout **or when a stop is requested** meanwhile
        (``ctl.shutdown``, Ctrl-C, parent gone): check ``rt.is_stopping`` and
        return from ``main`` instead of continuing the start-up.
        """
        if self.clock_follower.ready:
            return True
        assert self._clock_event is not None and self._stop_event is not None
        if self._stop_event.is_set():
            return False
        clock_wait = asyncio.ensure_future(self._clock_event.wait())
        stop_wait = asyncio.ensure_future(self._stop_event.wait())
        try:
            await asyncio.wait({clock_wait, stop_wait}, timeout=timeout, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for t in (clock_wait, stop_wait):
                t.cancel()
        return self.clock_follower.ready


__all__ = ["AsyncServiceRuntime", "AsyncSubscription"]
