"""FACT subscriber: ordering, gap repair, start policies, stream changes (02 §4.5, §4.6, §8.2).

:class:`FactTracker` implements the 02 §8.2 algorithm for every subscribed
``(src, topic)`` key:

* ``seq == expected`` -> ``apply``; ``seq < expected`` -> duplicate, dropped;
  ``seq > expected`` -> buffered and ``[expected, seq-1]`` repaired with
  ``ctl.replay``;
* a first subscription (no cursor) buffers messages until the publisher's
  heartbeat gives ``stream`` and ``fact_heads``; ``FROM_BEGINNING`` starts at
  1, ``FROM_NOW`` at ``min(head + 1, smallest buffered seq)``;
* heartbeats reveal tail gaps (``head >= expected``). Heartbeats travel on a
  different socket than the facts and can overtake them, so a tail gap is
  repaired when it is **still present at the next heartbeat**. A backlog that
  predates the subscription is found by the first live message (a gap on the
  ordered FACT socket is repaired at once) or by that second heartbeat;
* ``ctl.replay`` answering ``NOT_FOUND`` (retention purge) triggers the domain
  fallback for the purged part only (the reply's ``ReplayBatch.next_seq`` is
  the first seq still stored); the rest is replayed normally;
* a new ``stream`` (publisher database recreated or restored) resets the
  cursor to that stream: without ``on_stream_change`` the new stream is
  replayed from seq 1 (business keys make re-application harmless); with it,
  the callback reconciles by domain query and processing continues at the
  announced position. Which stream is current is decided by
  :class:`~futrisk.bus.incarnation.Incarnations`, never by comparing the
  UUIDv7 strings (a wall-clock step back makes a new stream sort lower): the
  publisher's heartbeat announces it (a change there switches at once, and
  every key of that publisher moves); after the first accepted heartbeat,
  envelopes of any other stream are stale and dropped (a new stream's facts
  that arrive before its heartbeat are recovered by the replay /
  reconciliation of the switch). Before any heartbeat, arrival order decides:
  a stream never seen replaces the key's stream, a replaced stream is stale.

The tracker is **transport-agnostic and non-blocking**: it never waits.

* ``request_replay(src, ReplayRequest, on_reply)`` is injected. It must send
  the request and later call ``on_reply(Reply)`` exactly once *on the thread
  that owns the tracker* (sync poller: from the loop when the DEALER reply
  arrives or times out; asyncio: from a task). Calling it synchronously is
  allowed too (in-process bus).
* ``apply(env, cursor)`` must be a **synchronous** function (``async def`` is
  rejected with ``TypeError``) that persists the business effects of ``env``
  and the cursor (``cursor.save(conn, fact_cursors, updated_ns=...)``) in ONE
  transaction. Sync services use ``db.write(...)``; asyncio services use the
  runtime's ``submit_write`` (ordered, and a failure poisons every later
  ``submit_write`` so nothing is committed after a lost fact). If ``apply``
  raises, the cursor is not advanced and the exception propagates.
* ``on_expired(key, lo, hi, done)`` and ``on_stream_change(key, old, new,
  next_seq, done)`` start the domain fallback / reconciliation; the tracker
  holds (buffers) the key until ``done()`` is called (immediately is fine).
* ``on_repair_failed(key, lo, hi, reply)`` fires once when a replay attempt
  fails with a retryable error (risk-engine: ``BLOCK_OPEN`` with
  ``SYSTEM:FILL_GAP``); ``on_repair_ok(key)`` fires when that gap is closed.
* Failed replays are retried with capped exponential backoff: call
  :meth:`FactTracker.poll_timers` from the loop timers and use
  :meth:`FactTracker.next_timer_ns` to size the poll timeout.

Feed it with :meth:`on_fact` for every FACT envelope of the subscribed topics
and :meth:`on_heartbeat` for every ``sys.heartbeat.{publisher}``.
"""

from __future__ import annotations

import inspect
import random
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import cast

import msgspec
from loguru import logger
from sqlalchemy import Connection

from futrisk.bus.incarnation import Incarnations, Verdict
from futrisk.common.clock import Clock, SystemClock
from futrisk.db.cursors import FactCursor, FactCursors
from futrisk.messages import codes
from futrisk.messages.common import ReplayBatch, ReplayRequest
from futrisk.messages.enums import FactStartPolicy
from futrisk.messages.envelope import Envelope, Reply, decode_envelope
from futrisk.messages.system import Heartbeat
from futrisk.messages.topics import matches

FactKey = tuple[str, str]
Done = Callable[[], None]
ReplyCallback = Callable[[Reply], None]
ReplayRequester = Callable[[str, ReplayRequest, ReplyCallback], None]

_NS_PER_MS = 1_000_000


def _require_sync(fn: object, what: str) -> None:
    if fn is not None and inspect.iscoroutinefunction(fn):
        raise TypeError(
            f"{what} must be a synchronous function (got an async def); in asyncio services persist "
            "with rt.submit_write(db, tx) inside a synchronous callback"
        )


def _reject_awaitable(result: object, what: str) -> None:
    if inspect.isawaitable(result):
        close = getattr(result, "close", None)
        if close is not None:
            close()
        raise TypeError(f"{what} must be synchronous but returned an awaitable")


@dataclass(frozen=True, slots=True)
class CursorUpdate:
    """New cursor values to persist together with the applied message."""

    src: str
    topic: str
    stream: str
    last_seq: int
    start_policy: FactStartPolicy

    @property
    def key(self) -> FactKey:
        return (self.src, self.topic)

    def save(self, conn: Connection, cursors: FactCursors, *, updated_ns: int) -> None:
        """Upsert into ``fact_cursors`` inside the caller's business transaction."""
        cursors.upsert(conn, self.src, self.topic, self.stream, self.last_seq, self.start_policy, updated_ns=updated_ns)


ApplyFn = Callable[[Envelope, CursorUpdate], None]
ExpiredFn = Callable[[FactKey, int, int, Done], None]
StreamChangeFn = Callable[[FactKey, str | None, str, int, Done], None]
RepairFailedFn = Callable[[FactKey, int, int, Reply], None]
RepairOkFn = Callable[[FactKey], None]


@dataclass
class FactSubscription:
    """FACT topics of one publisher handled by one set of callbacks.

    ``prefix`` is a full topic (``td.exec.ACC3``) or a prefix ending with ``.``
    (``td.exec.``), matched with :func:`futrisk.messages.topics.matches`.
    """

    src: str
    prefix: str
    policy: FactStartPolicy
    apply: ApplyFn
    on_expired: ExpiredFn | None = None
    on_stream_change: StreamChangeFn | None = None
    on_repair_failed: RepairFailedFn | None = None
    on_repair_ok: RepairOkFn | None = None


@dataclass
class FactStats:
    applied: int = 0
    duplicates: int = 0
    buffered: int = 0
    gaps: int = 0
    replays_sent: int = 0
    replay_failures: int = 0
    expired: int = 0
    stream_changes: int = 0
    stale_stream: int = 0
    unsubscribed: int = 0
    invalid: int = 0
    stale_replies: int = 0
    overflow: int = 0


@dataclass
class _Cursor:
    stream: str
    expected: int
    policy: FactStartPolicy


@dataclass
class _Repair:
    hi: int
    token: int = 0
    in_flight: bool = False
    attempts: int = 0
    next_try_ns: int | None = None


@dataclass
class _Key:
    key: FactKey
    sub: FactSubscription
    cursor: _Cursor | None = None
    buffer: dict[int, Envelope] = field(default_factory=dict)
    repair: _Repair | None = None
    holding: bool = False
    gen: int = 0
    max_seen: int = 0
    gap_alarm: bool = False
    in_batch: bool = False
    tail_seen: int | None = None  # head of the previous heartbeat that showed a tail gap


class FactTracker:
    """Per-``(src, topic)`` FACT sequencing for one subscriber service."""

    def __init__(
        self,
        request_replay: ReplayRequester,
        *,
        clock: Clock | None = None,
        cursors: Mapping[FactKey, FactCursor] | None = None,
        max_buffer: int = 10_000,
        replay_page: int = 500,
        backoff_base_ms: int = 200,
        backoff_max_ms: int = 5_000,
        rng: random.Random | None = None,
    ) -> None:
        self._request = request_replay
        self._clock = clock or SystemClock()
        self._persisted: dict[FactKey, FactCursor] = dict(cursors or {})
        self._subs: list[FactSubscription] = []
        self._keys: dict[FactKey, _Key] = {}
        self._last_hb: dict[str, Heartbeat] = {}  # last *accepted* heartbeat per publisher
        self._streams = Incarnations()
        self._max_buffer = max(1, max_buffer)
        self._page = max(1, min(replay_page, 1000))
        self._backoff_base_ns = backoff_base_ms * _NS_PER_MS
        self._backoff_max_ns = backoff_max_ms * _NS_PER_MS
        self._rng = rng or random.Random()
        self.stats = FactStats()

    # ------------------------------------------------------------ setup
    def subscribe(
        self,
        src: str,
        prefix: str,
        *,
        policy: FactStartPolicy | str,
        apply: ApplyFn,
        on_expired: ExpiredFn | None = None,
        on_stream_change: StreamChangeFn | None = None,
        on_repair_failed: RepairFailedFn | None = None,
        on_repair_ok: RepairOkFn | None = None,
    ) -> FactSubscription:
        sub = FactSubscription(
            src=src,
            prefix=prefix,
            policy=FactStartPolicy(str(policy)),
            apply=apply,
            on_expired=on_expired,
            on_stream_change=on_stream_change,
            on_repair_failed=on_repair_failed,
            on_repair_ok=on_repair_ok,
        )
        self.add_subscription(sub)
        return sub

    def add_subscription(self, sub: FactSubscription) -> None:
        if not sub.prefix:
            raise ValueError("empty FACT subscription")
        _require_sync(sub.apply, "FACT apply")
        for name in ("on_expired", "on_stream_change", "on_repair_failed", "on_repair_ok"):
            _require_sync(getattr(sub, name), name)
        self._subs.append(sub)

    def load_cursors(self, cursors: Mapping[FactKey, FactCursor]) -> None:
        """Seed persisted cursors (before the first message of those keys)."""
        self._persisted.update(cursors)
        for key, cur in cursors.items():
            st = self._keys.get(key)
            if st is not None and st.cursor is None:
                st.cursor = _Cursor(cur.stream, cur.last_seq + 1, cur.start_policy)

    def publishers(self) -> set[str]:
        return {s.src for s in self._subs}

    def subscription_for(self, src: str, topic: str) -> FactSubscription | None:
        for sub in self._subs:
            if sub.src == src and matches(topic, sub.prefix):
                return sub
        return None

    # ---------------------------------------------------- introspection
    def expected(self, src: str, topic: str) -> int | None:
        st = self._keys.get((src, topic))
        return st.cursor.expected if st is not None and st.cursor is not None else None

    def stream_of(self, src: str, topic: str) -> str | None:
        st = self._keys.get((src, topic))
        return st.cursor.stream if st is not None and st.cursor is not None else None

    def buffered(self, src: str, topic: str) -> list[int]:
        st = self._keys.get((src, topic))
        return sorted(st.buffer) if st is not None else []

    def repairing(self, src: str, topic: str) -> bool:
        st = self._keys.get((src, topic))
        return st is not None and st.repair is not None

    def holding(self, src: str, topic: str) -> bool:
        st = self._keys.get((src, topic))
        return st is not None and st.holding

    def keys(self) -> list[FactKey]:
        return sorted(self._keys)

    # ------------------------------------------------------------ input
    def on_fact(self, env: Envelope) -> None:
        """Feed one FACT envelope received from the SUB socket."""
        if env.stream is None:
            self.stats.invalid += 1
            logger.warning("fact.no_stream", src=env.src, topic=env.topic, seq=env.seq)
            return
        st = self._state(env.src, env.topic)
        if st is None:
            self.stats.unsubscribed += 1
            return
        self._process(st, env)
        self._after(st)

    def on_heartbeat(self, hb: Heartbeat) -> None:
        """Feed ``sys.heartbeat.{publisher}``: start positions, tail gaps, stream changes.

        The announced ``stream`` is the publisher's current one whatever its
        UUIDv7 order: a key on another stream switches to it. A heartbeat of a
        replaced stream still queued on the socket is ignored (``stale_stream``).
        """
        src = hb.instance
        if hb.stream is None:
            self._last_hb[src] = hb
            return
        if self._streams.announce(src, hb.stream) is Verdict.STALE:
            self.stats.stale_stream += 1  # heartbeat of a replaced incarnation still queued
            logger.info("fact.stale_heartbeat", src=src, stream=hb.stream, current=self._streams.current(src))
            return
        self._last_hb[src] = hb
        touched: dict[FactKey, _Key] = {}
        for topic, head in hb.fact_heads.items():
            st = self._state(src, topic)
            if st is None:
                continue
            touched[st.key] = st
            if st.cursor is None:
                self._start_cursor(st, hb)
            elif st.cursor.stream != hb.stream:
                self._restart_stream(st, hb.stream, head + 1, head=head)
            else:
                self._check_tail(st, head)
        for st in list(self._keys.values()):
            if st.key[0] != src or st.key in touched:
                continue
            if st.cursor is None:
                if st.buffer:
                    self._start_cursor(st, hb)
                    touched[st.key] = st
            elif st.cursor.stream != hb.stream:
                self._restart_stream(st, hb.stream, 1, head=0)  # no row of this topic in the new stream yet
                touched[st.key] = st
        for st in touched.values():
            self._after(st)

    def poll_timers(self, now_ns: int | None = None) -> None:
        """Retry failed replays whose backoff expired (monotonic ns)."""
        now = self._clock.monotonic_ns() if now_ns is None else now_ns
        for st in list(self._keys.values()):
            r = st.repair
            if (
                r is not None
                and not r.in_flight
                and not st.holding
                and r.next_try_ns is not None
                and r.next_try_ns <= now
            ):
                self._issue(st)

    def next_timer_ns(self) -> int | None:
        """Earliest pending retry time (monotonic ns) or ``None``."""
        times = [
            st.repair.next_try_ns
            for st in self._keys.values()
            if st.repair is not None and not st.repair.in_flight and st.repair.next_try_ns is not None
        ]
        return min(times) if times else None

    # -------------------------------------------------------- internals
    def _state(self, src: str, topic: str) -> _Key | None:
        st = self._keys.get((src, topic))
        if st is not None:
            return st
        sub = self.subscription_for(src, topic)
        if sub is None:
            return None
        st = _Key(key=(src, topic), sub=sub)
        persisted = self._persisted.get((src, topic))
        if persisted is not None:
            st.cursor = _Cursor(persisted.stream, persisted.last_seq + 1, persisted.start_policy)
            st.max_seen = persisted.last_seq
        self._keys[(src, topic)] = st
        return st

    def _check_tail(self, st: _Key, head: int) -> None:
        """Tail-gap detection from a heartbeat head (debounced by one heartbeat)."""
        cur = st.cursor
        assert cur is not None
        if st.holding:
            return
        if head < cur.expected:
            st.tail_seen = None
            return
        confirmed = st.tail_seen is not None and cur.expected <= st.tail_seen
        st.tail_seen = head
        if confirmed:
            self._ensure_repair(st, cur.expected, head)

    def _process(self, st: _Key, env: Envelope) -> None:
        if st.cursor is None:
            self._buffer_add(st, env)
            hb = self._last_hb.get(env.src)
            if hb is not None and hb.stream is not None:
                self._start_cursor(st, hb)
            return
        if env.stream is None:
            self.stats.invalid += 1
            return
        if self._streams.observe(env.src, env.stream) is Verdict.STALE:
            self.stats.stale_stream += 1  # replaced stream, or not the stream the heartbeat announced
            return
        if env.stream != st.cursor.stream:
            self._restart_stream(st, env.stream, env.seq)
        cur = st.cursor
        if st.holding:
            if env.seq >= cur.expected:
                self._buffer_add(st, env)
            else:
                self.stats.duplicates += 1
            return
        if env.seq < cur.expected:
            self.stats.duplicates += 1
            return
        if env.seq > cur.expected:
            self._buffer_add(st, env)
            self._ensure_repair(st, cur.expected, env.seq - 1)
            return
        self._apply(st, env)
        self._drain(st)

    def _start_cursor(self, st: _Key, hb: Heartbeat) -> None:
        if hb.stream is None:
            return
        policy = st.sub.policy
        head = hb.fact_heads.get(st.key[1], 0)
        if policy == FactStartPolicy.FROM_BEGINNING:
            start = 1
        else:
            candidates = [head + 1] + [s for s, e in st.buffer.items() if e.stream == hb.stream]
            start = min(candidates)
        st.cursor = _Cursor(hb.stream, start, policy)
        logger.info("fact.start", src=st.key[0], topic=st.key[1], policy=policy.value, expected=start)
        pending = [st.buffer[s] for s in sorted(st.buffer)]
        st.buffer = {}
        for env in pending:
            self._process(st, env)
        if st.cursor is not None and st.cursor.stream == hb.stream and not st.holding:
            # facts up to `head` may still be in flight: confirmed by the next heartbeat
            st.tail_seen = head if head >= st.cursor.expected else None

    def _restart_stream(self, st: _Key, stream: str, next_seq: int, *, head: int | None = None) -> None:
        old = st.cursor.stream if st.cursor is not None else None
        policy = st.cursor.policy if st.cursor is not None else st.sub.policy
        if old is not None and old != stream:
            self._streams.retire(st.key[0], old)  # e.g. the stream a persisted cursor was on
        sub = st.sub
        cb = sub.on_stream_change
        if cb is None:
            next_seq = 1  # no domain reconciliation: replay the new stream from its start
        st.gen += 1
        st.repair = None
        st.tail_seen = None
        st.buffer = {s: e for s, e in st.buffer.items() if e.stream == stream and s >= next_seq}
        st.max_seen = max([next_seq - 1, *st.buffer])
        st.cursor = _Cursor(stream, next_seq, policy)
        self.stats.stream_changes += 1
        logger.warning(
            "fact.stream_changed", src=st.key[0], topic=st.key[1], old=old, new=stream, next_seq=next_seq
        )
        key = st.key
        starter: Callable[[Done], None] | None = None
        if cb is not None:

            def starter(done: Done) -> None:
                cb(key, old, stream, next_seq, done)

        def released() -> None:
            self._drain(st)
            cur = st.cursor
            if head is not None and cur is not None and head >= cur.expected:
                self._ensure_repair(st, cur.expected, head)
            self._after(st)

        self._hold(st, starter, released)

    def _hold(self, st: _Key, starter: Callable[[Done], None] | None, on_done: Callable[[], None]) -> None:
        """Buffer ``st`` until the fallback calls ``done`` (``starter`` None: no hold)."""
        if starter is None:
            on_done()
            return
        gen = st.gen
        st.holding = True
        fired = False

        def done() -> None:
            nonlocal fired
            if fired:
                return
            fired = True
            if self._keys.get(st.key) is not st or st.gen != gen:
                return  # superseded by a newer stream change
            st.holding = False
            on_done()

        starter(done)

    def _ensure_repair(self, st: _Key, lo: int, hi: int) -> None:
        if st.cursor is None or hi < st.cursor.expected or hi < lo:
            return
        r = st.repair
        if r is None:
            r = _Repair(hi=hi)
            st.repair = r
            self.stats.gaps += 1
            logger.info("fact.gap", src=st.key[0], topic=st.key[1], lo=st.cursor.expected, hi=hi)
        elif hi > r.hi:
            r.hi = hi
        if r.in_flight or st.in_batch or st.holding:
            return
        if r.next_try_ns is not None and self._clock.monotonic_ns() < r.next_try_ns:
            return
        self._issue(st)

    def _issue(self, st: _Key) -> None:
        r = st.repair
        if r is None or st.cursor is None:
            return
        lo = st.cursor.expected
        if lo > r.hi:
            st.repair = None
            return
        r.in_flight = True
        r.next_try_ns = None
        r.token += 1
        token, gen, key = r.token, st.gen, st.key
        req = ReplayRequest(topic=key[1], from_seq=lo, to_seq=r.hi, limit=self._page)
        self.stats.replays_sent += 1

        def on_reply(reply: Reply) -> None:
            self._on_reply(key, gen, token, reply)

        try:
            self._request(key[0], req, on_reply)
        except Exception as exc:  # transport refused synchronously
            logger.warning("fact.replay_send_failed", src=key[0], topic=key[1], error=str(exc))
            self._on_reply(key, gen, token, Reply(ok=False, code=codes.UNAVAILABLE, message=str(exc)))

    def _backoff_ns(self, attempts: int) -> int:
        base = min(self._backoff_max_ns, self._backoff_base_ns << min(20, max(0, attempts - 1)))
        return int(base * (1 + 0.2 * self._rng.random()))

    @staticmethod
    def _first_available(reply: Reply) -> int | None:
        """``NOT_FOUND`` replies carry ``ReplayBatch.next_seq`` = first seq still stored."""
        if reply.data is None or reply.data_type != "ReplayBatch":
            return None
        try:
            batch = reply.open_as(ReplayBatch)
        except (msgspec.DecodeError, msgspec.ValidationError):
            return None
        return int(batch.next_seq) if batch is not None else None

    def _on_reply(self, key: FactKey, gen: int, token: int, reply: Reply) -> None:
        st = self._keys.get(key)
        if st is None or st.gen != gen or st.repair is None or st.repair.token != token or st.cursor is None:
            self.stats.stale_replies += 1
            return
        r = st.repair
        r.in_flight = False
        if reply.ok:
            try:
                batch = reply.open_as(ReplayBatch)
            except (msgspec.DecodeError, msgspec.ValidationError):
                batch = None
            if batch is None:
                self._repair_failed(st, r, Reply(ok=False, code=codes.INTERNAL, message="bad ReplayBatch"))
                return
            r.attempts = 0
            st.in_batch = True
            try:
                for raw in batch.envelopes:
                    try:
                        env = decode_envelope(raw)
                    except (msgspec.DecodeError, msgspec.ValidationError):
                        self.stats.invalid += 1
                        continue
                    if env.src != key[0] or env.topic != key[1]:
                        self.stats.invalid += 1
                        continue
                    self._process(st, env)
                    if st.gen != gen:
                        return  # the stream changed while applying the batch
            finally:
                st.in_batch = False
            if st.repair is not r:
                self._after(st)
                return
            if st.cursor.expected > r.hi:
                st.repair = None
            elif batch.envelopes and not batch.complete:
                self._issue(st)
                return
            else:
                # publisher returned less than requested: back off and ask again
                r.attempts += 1
                r.next_try_ns = self._clock.monotonic_ns() + self._backoff_ns(r.attempts)
            self._after(st)
        elif reply.code == codes.NOT_FOUND:
            lo, hi = st.cursor.expected, r.hi
            first = self._first_available(reply)
            if first is not None and lo < first <= hi:
                expire_hi, resume_hi = first - 1, hi  # only the purged part is expired
            else:
                expire_hi, resume_hi = hi, None
            st.repair = None
            self.stats.expired += 1
            logger.warning("fact.replay_expired", src=key[0], topic=key[1], lo=lo, hi=expire_hi, resume_to=resume_hi)
            cb = st.sub.on_expired
            starter: Callable[[Done], None] | None = None
            if cb is not None:

                def starter(done: Done) -> None:
                    cb(key, lo, expire_hi, done)

            self._hold(st, starter, lambda: self._finish_expired(st, expire_hi, resume_hi))
        else:
            self._repair_failed(st, r, reply)

    def _repair_failed(self, st: _Key, r: _Repair, reply: Reply) -> None:
        assert st.cursor is not None
        r.attempts += 1
        r.next_try_ns = self._clock.monotonic_ns() + self._backoff_ns(r.attempts)
        self.stats.replay_failures += 1
        logger.warning(
            "fact.replay_failed",
            src=st.key[0],
            topic=st.key[1],
            code=reply.code,
            attempts=r.attempts,
            lo=st.cursor.expected,
            hi=r.hi,
        )
        if not st.gap_alarm:
            st.gap_alarm = True
            cb = st.sub.on_repair_failed
            if cb is not None:
                cb(st.key, st.cursor.expected, r.hi, reply)

    def _finish_expired(self, st: _Key, hi: int, resume_hi: int | None = None) -> None:
        assert st.cursor is not None
        if st.cursor.expected <= hi:
            st.cursor.expected = hi + 1
        self._drain(st)
        if resume_hi is not None and resume_hi >= st.cursor.expected:
            self._ensure_repair(st, st.cursor.expected, resume_hi)  # replay the part still stored
        self._after(st)

    def _buffer_add(self, st: _Key, env: Envelope) -> None:
        if env.seq > st.max_seen:
            st.max_seen = env.seq
        if env.seq in st.buffer:
            self.stats.duplicates += 1
            return
        if len(st.buffer) >= self._max_buffer:
            top = max(st.buffer)
            self.stats.overflow += 1
            if env.seq > top:
                return  # will be fetched again by replay (max_seen remembers it)
            del st.buffer[top]
        st.buffer[env.seq] = env
        self.stats.buffered += 1

    def _apply(self, st: _Key, env: Envelope) -> None:
        cur = st.cursor
        assert cur is not None
        update = CursorUpdate(st.key[0], st.key[1], cur.stream, env.seq, cur.policy)
        apply_fn = cast(Callable[[Envelope, CursorUpdate], object], st.sub.apply)
        _reject_awaitable(apply_fn(env, update), "FACT apply")
        cur.expected = env.seq + 1
        self.stats.applied += 1

    def _drain(self, st: _Key) -> None:
        cur = st.cursor
        if cur is None:
            return
        while not st.holding:
            env = st.buffer.pop(cur.expected, None)
            if env is None:
                break
            self._apply(st, env)
        stale = [s for s in st.buffer if s < cur.expected]
        for s in stale:
            del st.buffer[s]

    def _after(self, st: _Key) -> None:
        """Re-check message-driven gaps after any change and fire ``on_repair_ok`` when clean.

        Heartbeat heads are *not* consulted here (they may overtake facts);
        tail gaps are handled by :meth:`_check_tail`.
        """
        cur = st.cursor
        if cur is None or st.holding or st.in_batch:
            return
        if st.repair is not None:
            return  # a repair is pending: _process/on_heartbeat extend it and it re-checks when done
        exp = cur.expected
        if st.buffer:
            lowest = min(st.buffer)
            if lowest > exp:
                self._ensure_repair(st, exp, lowest - 1)
        elif st.max_seen >= exp:
            self._ensure_repair(st, exp, st.max_seen)
        if st.gap_alarm and st.repair is None and not st.buffer and not st.holding:
            st.gap_alarm = False
            logger.info("fact.gap_closed", src=st.key[0], topic=st.key[1], expected=cur.expected)
            cb = st.sub.on_repair_ok
            if cb is not None:
                cb(st.key)


__all__ = [
    "CursorUpdate",
    "FactKey",
    "FactStats",
    "FactSubscription",
    "FactTracker",
    "ReplayRequester",
]
