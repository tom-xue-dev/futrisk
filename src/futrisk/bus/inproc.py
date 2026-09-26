"""Deterministic synchronous in-process bus (02 §10) for backtests and unit tests.

* No threads, no sockets: ``publish`` enqueues one delivery per matching
  subscription and drains the queue FIFO before returning (publishes made by
  handlers are queued behind the current delivery, never nested).
* Envelopes are real :class:`~futrisk.messages.envelope.Envelope` objects with
  per-``(src, topic)`` seq numbers starting at 1, the instance epoch and, for
  ``fact=True``, a stream id; payloads round-trip through msgpack so handlers
  see exactly what they would see on the wire.
* ``request`` calls the target's handler synchronously and returns its
  :class:`~futrisk.messages.envelope.Reply` (unknown target -> ``UNAVAILABLE``,
  unknown command -> ``BAD_REQUEST``, optional whitelist -> ``FORBIDDEN``).
* Every instance answers ``ctl.ping``, ``ctl.snapshot`` (from its last-value
  cache) and ``ctl.replay`` (from an in-memory FACT log), so FactTracker and
  CloneSync can be exercised in-process.

Usage::

    bus = InprocBus("risk-engine-0", clock=SimClock(t0))
    bus.serve("trade-gateway", {"route.order": fake_route})       # another instance
    bus.subscribe("md-gateway", ["md.tick."], on_tick)
    md = bus.endpoint("md-gateway")
    md.publish("md.tick.rb2701", tick)                             # delivered to on_tick now
    reply = bus.request("trade-gateway", "route.order", route)
"""

from __future__ import annotations

from collections import deque
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass

from futrisk.bus.lvcache import LastValueCache
from futrisk.bus.protocol import EnvelopeHandler
from futrisk.bus.rpc_common import (
    DEFERRED,
    Handler,
    HandlerSpec,
    RequestContext,
    is_awaitable,
    request_envelope,
)
from futrisk.bus.subscriber import TopicFilter
from futrisk.bus.whitelist import Whitelist
from futrisk.common.clock import Clock, SimClock
from futrisk.messages import codes
from futrisk.messages.base import Msg, decode, encode, type_name, type_version
from futrisk.messages.common import Ping, Pong, ReplayBatch, ReplayRequest, SnapshotRequest
from futrisk.messages.envelope import (
    PROTO,
    Envelope,
    Reply,
    decode_envelope,
    encode_envelope,
    err_reply,
    ok_reply,
    open_payload,
)


@dataclass
class _Sub:
    subscriber: str
    publisher: str
    filter: TopicFilter
    handler: EnvelopeHandler


class InprocEndpoint:
    """One instance on an :class:`InprocBus` (implements the ``Bus`` protocol)."""

    def __init__(self, bus: InprocBus, instance: str) -> None:
        self._bus = bus
        self.instance = instance
        self._incarnation = 1
        self.epoch = f"{instance}-epoch-{self._incarnation:04d}"
        self.stream = f"{instance}-stream-0001"
        self._seq: dict[str, int] = {}
        self._fact_seq: dict[str, int] = {}
        self.cache = LastValueCache()
        self.fact_log: dict[str, list[bytes]] = {}
        self._handlers: dict[str, HandlerSpec] = {}
        self._handlers["ctl.ping"] = HandlerSpec(self._ping, Ping)
        self._handlers["ctl.snapshot"] = HandlerSpec(self._snapshot, SnapshotRequest)
        self._handlers["ctl.replay"] = HandlerSpec(self._replay, ReplayRequest)

    # ------------------------------------------------------------ Bus API
    def publish(
        self,
        topic: str,
        payload: Msg,
        *,
        corr_id: str | None = None,
        fact: bool = False,
        td: str | None = None,
    ) -> int:
        counters = self._fact_seq if fact else self._seq
        seq = counters.get(topic, 0) + 1
        counters[topic] = seq
        name = type_name(payload)
        env = Envelope(
            proto=PROTO,
            topic=topic,
            type=name,
            v=type_version(name),
            src=self.instance,
            epoch=self.epoch,
            stream=self.stream if fact else None,
            seq=seq,
            ts_ns=self._bus.clock.now_ns(),
            td=td,
            corr_id=corr_id,
            payload=encode(payload),
        )
        data = encode_envelope(env)
        self.cache.put(topic, seq, self.epoch, data, fact=fact)
        if fact:
            self.fact_log.setdefault(topic, []).append(data)
        self._bus._enqueue(decode_envelope(data))
        return seq

    def subscribe(self, publisher: str, prefixes: Sequence[str], handler: EnvelopeHandler) -> None:
        self._bus._add_sub(self.instance, publisher, prefixes, handler)

    def request(self, target: str, command: str, payload: Msg, *, timeout_ms: int | None = None) -> Reply:
        return self._bus._request(self.instance, target, command, payload)

    def serve(self, handlers: Mapping[str, Handler]) -> None:
        for command, handler in handlers.items():
            self._handlers[command] = HandlerSpec(handler)

    def add_handler(self, command: str, handler: Handler, *, payload_type: type[Msg] | None = None) -> None:
        self._handlers[command] = HandlerSpec(handler, payload_type)

    def restart(self) -> None:
        """Simulate a process restart: new epoch, LV seqs restart at 1 (FACT seqs continue)."""
        self._incarnation += 1
        self.epoch = f"{self.instance}-epoch-{self._incarnation:04d}"
        self._seq.clear()

    def new_stream(self, stream: str) -> None:
        """Simulate a recreated/restored database: new stream id, FACT seqs restart."""
        self.stream = stream
        self._fact_seq.clear()
        self.fact_log.clear()

    # ------------------------------------------------------- ctl handlers
    def _ping(self, command: str, payload: Msg, ctx: RequestContext) -> Reply:
        return ok_reply(Pong(instance=self.instance, state="RUNNING", ts_ns=self._bus.clock.now_ns()))

    def _snapshot(self, command: str, payload: Msg, ctx: RequestContext) -> Reply:
        assert isinstance(payload, SnapshotRequest)
        if not payload.prefixes:
            return err_reply(codes.BAD_REQUEST, "prefixes must not be empty")
        return ok_reply(
            self.cache.snapshot(
                payload.prefixes,
                as_of_ns=self._bus.clock.now_ns(),
                include_fact=payload.include_fact,
                cursor=payload.next_cursor,
                limit=payload.limit,
            )
        )

    def _replay(self, command: str, payload: Msg, ctx: RequestContext) -> Reply:
        assert isinstance(payload, ReplayRequest)
        log = self.fact_log.get(payload.topic, [])
        head = self._fact_seq.get(payload.topic, 0)
        first_kept = head - len(log) + 1
        lo = max(1, payload.from_seq)
        hi = head if payload.to_seq is None else min(payload.to_seq, head)
        if lo > hi:
            return ok_reply(ReplayBatch(topic=payload.topic, envelopes=[], next_seq=lo, complete=True))
        if lo < first_kept:
            resume = first_kept if log else hi + 1
            batch = ReplayBatch(topic=payload.topic, envelopes=[], next_seq=resume, complete=False)
            return err_reply(codes.NOT_FOUND, f"{payload.topic}: seq {lo} purged", data=batch)
        n = max(1, min(payload.limit, 1000))
        items = log[lo - first_kept : lo - first_kept + min(n, hi - lo + 1)]
        next_seq = lo + len(items)
        return ok_reply(ReplayBatch(topic=payload.topic, envelopes=list(items), next_seq=next_seq, complete=next_seq > hi))

    def purge_fact_log(self, topic: str, keep_from_seq: int) -> None:
        """Drop FACT log entries below ``keep_from_seq`` (simulates outbox retention)."""
        log = self.fact_log.get(topic, [])
        head = self._fact_seq.get(topic, 0)
        first_kept = head - len(log) + 1
        drop = max(0, keep_from_seq - first_kept)
        self.fact_log[topic] = log[drop:]


class InprocBus:
    """Synchronous FIFO bus hosting any number of instances in one process."""

    def __init__(
        self,
        instance: str = "inproc",
        *,
        clock: Clock | None = None,
        whitelist: Whitelist | None = None,
        raise_handler_errors: bool = True,
    ) -> None:
        self.clock: Clock = clock or SimClock(0)
        self.whitelist = whitelist
        self.raise_handler_errors = raise_handler_errors
        self._default = instance
        self._endpoints: dict[str, InprocEndpoint] = {}
        self._subs: list[_Sub] = []
        self._queue: deque[tuple[EnvelopeHandler, Envelope]] = deque()
        self._delivering = False
        self._req_seq = 0
        self.delivered = 0
        self.endpoint(instance)

    @property
    def instance(self) -> str:
        return self._default

    def endpoint(self, instance: str) -> InprocEndpoint:
        ep = self._endpoints.get(instance)
        if ep is None:
            ep = InprocEndpoint(self, instance)
            self._endpoints[instance] = ep
        return ep

    # ------------------------------------------------- Bus API (default)
    def publish(
        self,
        topic: str,
        payload: Msg,
        *,
        corr_id: str | None = None,
        fact: bool = False,
        td: str | None = None,
    ) -> int:
        return self.endpoint(self._default).publish(topic, payload, corr_id=corr_id, fact=fact, td=td)

    def subscribe(self, publisher: str, prefixes: Sequence[str], handler: EnvelopeHandler) -> None:
        self._add_sub(self._default, publisher, prefixes, handler)

    def request(self, target: str, command: str, payload: Msg, *, timeout_ms: int | None = None) -> Reply:
        return self._request(self._default, target, command, payload)

    def serve(
        self,
        instance_or_handlers: str | Mapping[str, Handler],
        handlers: Mapping[str, Handler] | None = None,
    ) -> None:
        """``serve(handlers)`` for the default instance or ``serve(instance, handlers)``."""
        if isinstance(instance_or_handlers, str):
            if handlers is None:
                raise TypeError("serve(instance, handlers) needs handlers")
            self.endpoint(instance_or_handlers).serve(handlers)
        else:
            self.endpoint(self._default).serve(instance_or_handlers)

    def run_pending(self) -> int:
        """Deliver queued envelopes (normally done by ``publish``); returns count."""
        return self._drain()

    # ---------------------------------------------------------- internals
    def _add_sub(self, subscriber: str, publisher: str, prefixes: Sequence[str], handler: EnvelopeHandler) -> None:
        flt = TopicFilter()
        flt.add(publisher, prefixes)
        self._subs.append(_Sub(subscriber, publisher, flt, handler))

    def _enqueue(self, env: Envelope) -> None:
        for sub in self._subs:
            if sub.publisher == env.src and sub.filter.accepts(env.src, env.topic):
                self._queue.append((sub.handler, env))
        if not self._delivering:
            self._drain()

    def _drain(self) -> int:
        if self._delivering:
            return 0
        self._delivering = True
        n = 0
        try:
            while self._queue:
                handler, env = self._queue.popleft()
                handler(env)
                n += 1
        finally:
            self._delivering = False
        self.delivered += n
        return n

    def _request(self, src: str, target: str, command: str, payload: Msg) -> Reply:
        ep = self._endpoints.get(target)
        if ep is None:
            return err_reply(codes.UNAVAILABLE, f"{target}: not on this bus")
        if self.whitelist is not None and not self.whitelist.allowed(command, src):
            return err_reply(codes.FORBIDDEN, f"{src} may not call {command}")
        spec = ep._handlers.get(command)
        if spec is None:
            return err_reply(codes.BAD_REQUEST, f"unknown command {command}")
        self._req_seq += 1
        corr = f"{src}-req-{self._req_seq:08d}"
        env = decode_envelope(
            request_envelope(command, payload, src=src, epoch=self.endpoint(src).epoch, ts_ns=self.clock.now_ns(), corr_id=corr)
        )
        decoded = open_payload(env)
        if spec.payload_type is not None and not isinstance(decoded, spec.payload_type):
            return err_reply(codes.BAD_REQUEST, f"{command} expects {spec.payload_type.__name__}")
        ctx = RequestContext(
            command=command,
            requester=src,
            identity=src.encode("utf-8"),
            corr_id=corr,
            received_ns=self.clock.now_ns(),
            received_mono_ns=self.clock.monotonic_ns(),
            envelope=env,
        )
        try:
            result = spec.handler(command, decoded, ctx)
        except Exception as exc:
            if self.raise_handler_errors:
                raise
            return err_reply(codes.INTERNAL, f"{type(exc).__name__}: {exc}")
        if is_awaitable(result):
            close: Callable[[], None] | None = getattr(result, "close", None)
            if close is not None:
                close()
            raise TypeError("InprocBus handlers must be synchronous")
        if result is DEFERRED:
            raise RuntimeError("InprocBus does not support deferred replies")
        reply = ok_reply() if result is None else result
        if not isinstance(reply, Reply):
            raise TypeError(f"handler returned {type(reply).__name__}")
        return decode(encode(reply), Reply)


__all__ = ["InprocBus", "InprocEndpoint"]
