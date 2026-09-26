"""LV subscriber helpers: expected seq per topic, publisher restarts, late-joiner Clone (02 §4.5, §4.6).

LV rules per ``(src, topic)``:

* ``seq >= expected`` -> apply (a gap is fine, the newest value wins);
* ``seq < expected`` -> duplicate, drop;
* a new publisher epoch (process restart; LV seqs restart at 1) -> reset every
  expectation of that publisher, apply, and re-snapshot to catch up the other
  topics.

Which epoch is current is decided by :class:`~futrisk.bus.incarnation.Incarnations`,
never by comparing the UUIDv7 strings (their order follows a wall clock that
Windows time sync can step back). The publisher's heartbeat announces its
current epoch (:meth:`LvTracker.on_heartbeat`): after the first accepted
heartbeat only envelopes of the announced epoch are applied, and a heartbeat
announcing a new epoch switches (the Clone re-snapshots, which also recovers
what the new process published before its first heartbeat arrived). Before
any heartbeat, arrival order decides: an epoch never seen replaces the current
one, a replaced epoch is stale.

FACT envelopes (``stream`` set) seen by an LV consumer (a read model) are
ordered by ``(stream, seq)`` instead: their ``epoch`` is the epoch of the
process that *appended* the row, which may be older than the publisher's
current epoch (rows republished after a restart, snapshot items served from
the outbox), and their seq continues across restarts. The current stream is
decided like the epoch (heartbeat ``stream``, else arrival order); a topic
moving to the new stream starts over at that envelope.

:class:`CloneSync` implements the late-joiner sequence of 02 §4.6: subscribe
and buffer, ``ctl.snapshot``, apply the snapshot and set ``expected = seq + 1``
per topic, apply buffered messages with ``seq >= expected``, go live. Feed it
the publisher's heartbeats with :meth:`CloneSync.on_heartbeat` (the runtimes
do for ``subscribe(..., snapshot=True)``). Like
:class:`~futrisk.bus.fact.FactTracker` it is transport-agnostic: the snapshot
request goes through an injected ``request_snapshot(publisher, req,
on_reply)``; failures are retried from :meth:`CloneSync.poll_timers`.
"""

from __future__ import annotations

import inspect
from collections import deque
from collections.abc import Callable, Sequence
from dataclasses import dataclass

import msgspec
from loguru import logger

from futrisk.bus.fact import ReplyCallback
from futrisk.bus.incarnation import Incarnations, Verdict
from futrisk.common.clock import Clock, SystemClock
from futrisk.messages.common import SnapshotBatch, SnapshotRequest
from futrisk.messages.envelope import Envelope, Reply, decode_envelope
from futrisk.messages.system import Heartbeat
from futrisk.messages.topics import matches

SnapshotRequester = Callable[[str, SnapshotRequest, ReplyCallback], None]


@dataclass(frozen=True, slots=True)
class LvResult:
    """Decision for one LV envelope."""

    apply: bool
    epoch_changed: bool = False
    first: bool = False
    skipped: int = 0
    stream_changed: bool = False


class LvTracker:
    """Expected LV sequence numbers per ``(src, topic)`` and the publishers' current incarnations."""

    def __init__(self) -> None:
        self._epochs = Incarnations()
        self._streams = Incarnations()
        self._expected: dict[tuple[str, str], int] = {}
        self._facts: dict[tuple[str, str], tuple[str, int]] = {}  # (stream, expected) of FACT topics
        self.duplicates = 0
        self.stale_epoch = 0
        self.stale_stream = 0
        self.stale_heartbeats = 0

    def epoch(self, src: str) -> str | None:
        """The publisher epoch currently followed (``None`` before anything was seen)."""
        return self._epochs.current(src)

    def stream(self, src: str) -> str | None:
        """The publisher stream currently followed for FACT topics."""
        return self._streams.current(src)

    def expected(self, src: str, topic: str) -> int | None:
        fact = self._facts.get((src, topic))
        if fact is not None:
            return fact[1]
        return self._expected.get((src, topic))

    def forget(self, src: str) -> None:
        self._epochs.forget(src)
        self._streams.forget(src)
        for key in [k for k in self._expected if k[0] == src]:
            del self._expected[key]
        for key in [k for k in self._facts if k[0] == src]:
            del self._facts[key]

    def _reset(self, src: str) -> None:
        """New publisher epoch: LV expectations restart (FACT positions are kept)."""
        for key in [k for k in self._expected if k[0] == src]:
            del self._expected[key]

    def on_heartbeat(self, hb: Heartbeat) -> bool:
        """Apply ``sys.heartbeat.{publisher}``: the epoch/stream it announces are the current ones.

        Returns True when the publisher switched to a new epoch or stream (it
        restarted): re-snapshot to catch up. A heartbeat of a replaced
        incarnation still queued on the socket is ignored (``stale_heartbeats``).
        """
        src = hb.instance
        verdict = self._epochs.announce(src, hb.epoch)
        if verdict is Verdict.STALE:
            self.stale_heartbeats += 1
            return False
        switched = verdict is Verdict.NEW
        if switched:
            self._reset(src)
        if hb.stream is not None and self._streams.announce(src, hb.stream) is Verdict.NEW:
            switched = True
        return switched

    def _fact(self, env: Envelope) -> LvResult:
        assert env.stream is not None
        if self._streams.observe(env.src, env.stream) is Verdict.STALE:
            self.stale_stream += 1
            return LvResult(apply=False)
        key = (env.src, env.topic)
        rec = self._facts.get(key)
        if rec is None:
            self._facts[key] = (env.stream, env.seq + 1)
            return LvResult(apply=True, first=True)
        stream, exp = rec
        if env.stream != stream:  # this topic moves to the publisher's current stream
            self._streams.retire(env.src, stream)
            self._facts[key] = (env.stream, env.seq + 1)
            return LvResult(apply=True, stream_changed=True)
        if env.seq >= exp:
            self._facts[key] = (stream, env.seq + 1)
            return LvResult(apply=True, skipped=env.seq - exp)
        self.duplicates += 1
        return LvResult(apply=False)

    def on_lv(self, env: Envelope) -> LvResult:
        """Decide whether a live envelope is applied."""
        if env.stream is not None:
            return self._fact(env)
        verdict = self._epochs.observe(env.src, env.epoch)
        if verdict is Verdict.STALE:
            self.stale_epoch += 1
            return LvResult(apply=False)
        key = (env.src, env.topic)
        if verdict is Verdict.NEW:
            self._reset(env.src)
            self._expected[key] = env.seq + 1
            return LvResult(apply=True, epoch_changed=True)
        exp = self._expected.get(key)
        if exp is None or env.seq >= exp:
            self._expected[key] = env.seq + 1
            return LvResult(apply=True, first=exp is None, skipped=(env.seq - exp) if exp is not None else 0)
        self.duplicates += 1
        return LvResult(apply=False)

    def accept_snapshot_item(self, env: Envelope) -> bool:
        """Snapshot envelope: apply when it is of the current incarnation and newer than what was applied."""
        if env.stream is not None:
            return self._fact(env).apply
        verdict = self._epochs.observe(env.src, env.epoch)
        if verdict is Verdict.STALE:
            self.stale_epoch += 1
            return False
        if verdict is Verdict.NEW:
            self._reset(env.src)
        key = (env.src, env.topic)
        exp = self._expected.get(key)
        if exp is None or env.seq >= exp:
            self._expected[key] = env.seq + 1
            return True
        return False


class CloneSync:
    """Late-joiner sync of one publisher's topics (02 §4.6).

    Usage: create, subscribe the SUB socket to the same prefixes, call
    :meth:`start`, then feed every received envelope to :meth:`on_message`
    and every ``sys.heartbeat.{publisher}`` to :meth:`on_heartbeat`.
    ``apply(env)`` receives snapshot items and live messages in order, each
    topic at most once per seq.
    """

    def __init__(
        self,
        publisher: str,
        prefixes: Sequence[str],
        apply: Callable[[Envelope], None],
        request_snapshot: SnapshotRequester,
        *,
        tracker: LvTracker | None = None,
        clock: Clock | None = None,
        retry_ms: int = 1_000,
        include_fact: bool = True,
        max_buffer: int = 100_000,
        on_synced: Callable[[], None] | None = None,
    ) -> None:
        if inspect.iscoroutinefunction(apply):
            raise TypeError("CloneSync apply must be a synchronous function (got an async def)")
        self.publisher = publisher
        self.prefixes = list(prefixes)
        self._apply = apply
        self._request = request_snapshot
        self.tracker = tracker or LvTracker()
        self._clock = clock or SystemClock()
        self._retry_ns = retry_ms * 1_000_000
        self._include_fact = include_fact
        self._buffer: deque[Envelope] = deque(maxlen=max_buffer)
        self._on_synced = on_synced
        self._state = "IDLE"  # IDLE -> SYNCING -> LIVE (RESYNC while live after a publisher restart)
        self._token = 0
        self._in_flight = False
        self._retry_at: int | None = None
        self.snapshots_applied = 0
        self.syncs = 0

    @property
    def state(self) -> str:
        return self._state

    @property
    def synced(self) -> bool:
        return self._state in ("LIVE", "RESYNC")

    def start(self) -> None:
        """Begin buffering and request the snapshot."""
        self._state = "SYNCING"
        self._buffer.clear()
        self._send(None)

    def resync(self) -> None:
        """Re-snapshot while staying live (publisher restarted)."""
        if self._state == "SYNCING":
            return
        self._state = "RESYNC"
        self._send(None)

    def on_message(self, env: Envelope) -> None:
        if not any(matches(env.topic, p) for p in self.prefixes) or env.src != self.publisher:
            return
        if self._state in ("IDLE", "SYNCING"):
            self._buffer.append(env)
            return
        res = self.tracker.on_lv(env)
        if res.apply:
            self._apply(env)
        if res.epoch_changed or res.stream_changed:
            logger.info("lv.epoch_changed", publisher=self.publisher, topic=env.topic)
            self.resync()

    def on_heartbeat(self, hb: Heartbeat) -> None:
        """Feed ``sys.heartbeat.{publisher}``: a new epoch/stream there re-snapshots (any id order)."""
        if hb.instance != self.publisher or not self.tracker.on_heartbeat(hb):
            return
        logger.info("lv.publisher_restarted", publisher=self.publisher, epoch=hb.epoch, stream=hb.stream)
        if self._state == "SYNCING":
            self._send(None)  # the snapshot in flight may predate the restart: ask again (its reply is ignored)
        elif self._state in ("LIVE", "RESYNC"):
            self.resync()

    def poll_timers(self, now_ns: int | None = None) -> None:
        now = self._clock.monotonic_ns() if now_ns is None else now_ns
        if self._retry_at is not None and not self._in_flight and now >= self._retry_at:
            self._retry_at = None
            self._send(None)

    def next_timer_ns(self) -> int | None:
        return None if self._in_flight else self._retry_at

    # ---------------------------------------------------------- internals
    def _send(self, cursor: str | None) -> None:
        self._token += 1
        token = self._token
        self._in_flight = True
        self._retry_at = None
        req = SnapshotRequest(prefixes=list(self.prefixes), include_fact=self._include_fact, next_cursor=cursor)

        def on_reply(reply: Reply) -> None:
            self._on_reply(token, reply)

        try:
            self._request(self.publisher, req, on_reply)
        except Exception as exc:
            self._on_reply(token, Reply(ok=False, code="UNAVAILABLE", message=str(exc)))

    def _on_reply(self, token: int, reply: Reply) -> None:
        if token != self._token:
            return
        self._in_flight = False
        batch: SnapshotBatch | None = None
        if reply.ok:
            try:
                batch = reply.open_as(SnapshotBatch)
            except (msgspec.DecodeError, msgspec.ValidationError):
                batch = None
        if batch is None:
            logger.debug("lv.snapshot_failed", publisher=self.publisher, code=reply.code)
            self._retry_at = self._clock.monotonic_ns() + self._retry_ns
            return
        for item in batch.items:
            try:
                env = decode_envelope(item.envelope)
            except (msgspec.DecodeError, msgspec.ValidationError):
                continue
            if self.tracker.accept_snapshot_item(env):
                self._apply(env)
                self.snapshots_applied += 1
        if batch.next_cursor:
            self._send(batch.next_cursor)
            return
        restarted = False
        if self._state == "SYNCING":
            buffered = list(self._buffer)
            self._buffer.clear()
            self._state = "LIVE"
            for env in buffered:
                res = self.tracker.on_lv(env)
                if res.apply:
                    self._apply(env)
                restarted = restarted or res.epoch_changed or res.stream_changed
        else:
            self._state = "LIVE"
        self.syncs += 1
        if self._on_synced is not None:
            self._on_synced()
        if restarted:  # the publisher restarted while we were syncing: catch up its other topics
            self.resync()


__all__ = ["CloneSync", "LvResult", "LvTracker", "SnapshotRequester"]
