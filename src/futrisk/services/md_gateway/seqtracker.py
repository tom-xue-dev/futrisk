"""Feed A sequence tracking: duplicates, reordering, holes, replays and resume (02 §9.1, 01 §6.4, §8.13).

Feed A numbers every tick with one global ``seq`` per ``epoch``. The tracker keeps
``contiguous`` (every seq up to it was received or given up: the resume point and, once the
raw shard is on disk, the persisted cursor), ``frontier`` (the next live seq expected) and the
**holes** (missing ranges below the frontier with their repair state). A seq already received
is a duplicate; ``seq == frontier`` is delivered at once; a seq beyond it waits in the reorder
buffer (``reorder_max`` messages / ``reorder_ns``), and when that overflows the missing seqs
become a hole, the held ticks go out in order and an in-connection ``replay`` is requested.
Replayed and late ticks only fill holes (delivered with ``replay=True`` -> BACKFILL); a
heartbeat ``head_seq`` beyond everything received, ``tail_heartbeats`` times in a row, is a
tail gap. A reconnect resumes from ``contiguous + 1`` (the resume block is a hole);
``SEQ_EVICTED`` gives up the evicted part, ``EPOCH_CHANGED`` the old epoch's tail,
``BAD_RESUME`` starts again from the server's head; every resume ends with
:meth:`TrackerSink.resume_finished`. Transport-agnostic: the WebSocket client feeds it frames.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any, Protocol

from loguru import logger

from futrisk.common.clock import Clock
from futrisk.messages.enums import GapStatus

HOLE = "hole"
TAIL = "tail"
RESUME = "resume"
RING = "ring"  # what the ring still holds after SEQ_EVICTED / EPOCH_CHANGED
LOST = "lost"  # given up at once (evicted part, old epoch tail)


@dataclass
class Hole:
    """A missing seq range of Feed A and its repair state (``GapInfo`` of 02 §7.1)."""

    epoch: str
    lo: int
    hi: int
    kind: str
    missing: set[int]
    opened_mono: int
    from_ns: int
    to_ns: int | None = None
    req: int | None = None
    requested_mono: int = 0
    attempts: int = 0
    answered: bool = False
    filled: int = 0
    duplicates: int = 0
    unrecoverable: int = 0
    status: GapStatus = GapStatus.OPEN
    detail: dict[str, Any] = field(default_factory=dict)

    @property
    def gap_id(self) -> str:
        return f"feed_a:{self.epoch}:{self.lo}-{self.hi}"

    @property
    def closed(self) -> bool:
        return self.status != GapStatus.OPEN

    @property
    def size(self) -> int:
        return self.hi - self.lo + 1


class TrackerSink(Protocol):
    """Callbacks of :class:`SeqTracker` (implemented by the Feed A client)."""

    def deliver(self, seq: int, replay: bool, payload: Any) -> None: ...

    def request_replay(self, lo: int, hi: int) -> int | None:
        """Send ``replay {from_seq, to_seq}``; returns the request id (None when not connected)."""

    def gap_event(self, hole: Hole, event: str) -> None:
        """``open``; ``close`` (FILLED / UNRECOVERABLE); ``lost`` (opened and given up at once)."""

    def reconnect(self, reason: str) -> None: ...

    def resume_finished(self) -> None: ...


@dataclass(slots=True)
class _Held:
    payload: Any
    replay: bool


class SeqTracker:
    """Sequence state machine of one Feed A client; see the module docstring."""

    def __init__(
        self,
        sink: TrackerSink,
        clock: Clock,
        *,
        reorder_max: int = 32,
        reorder_ns: int = 100_000_000,
        replay_timeout_ns: int = 5_000_000_000,
        max_attempts: int = 3,
        tail_heartbeats: int = 2,
    ) -> None:
        self._sink = sink
        self._clock = clock
        self.configure(reorder_max=reorder_max, reorder_ns=reorder_ns)
        self._replay_timeout_ns = replay_timeout_ns
        self._max_attempts = max(1, max_attempts)
        self._tail_heartbeats = max(1, tail_heartbeats)
        self.epoch: str | None = None
        self.contiguous = 0
        self.frontier = 1
        self._above: set[int] = set()
        self._held: dict[int, _Held] = {}
        self._held_since: int | None = None
        self.holes: list[Hole] = []
        self._by_req: dict[int, Hole] = {}
        self._connected = False
        self._resume_sent: dict[str, Any] | None = None
        self._resume_req: Any = None
        self._resume_range: tuple[int, int] | None = None
        self._awaiting_resume = False
        self._welcome_epoch: str | None = None
        self._last_head = 0
        self._tail_count = 0
        self._tail_head = 0
        self._disconnect_ns = 0
        self.stats: dict[str, int] = {"delivered": 0, "duplicates": 0, "held": 0, "holes": 0, "unrecoverable": 0}

    def configure(self, *, reorder_max: int, reorder_ns: int) -> None:
        """Reorder buffer limits (also on config hot reload)."""
        self._reorder_max = max(1, reorder_max)
        self._reorder_ns = reorder_ns

    # ------------------------------------------------------------------ state
    @property
    def max_seen(self) -> int:
        return max(self.frontier - 1, max(self._held, default=0))

    @property
    def last_head(self) -> int:
        return self._last_head

    @property
    def held(self) -> int:
        return len(self._held)

    @property
    def complete(self) -> bool:
        """No open hole, no held tick and no resume pending: the stream is gap-free."""
        return self._connected and not self._held and not self._awaiting_resume and self._resume_range is None and not self.open_holes

    @property
    def open_holes(self) -> list[Hole]:
        return [h for h in self.holes if not h.closed]

    def oldest_open_hole_mono(self) -> int | None:
        return min((h.opened_mono for h in self.holes if not h.closed), default=None)

    def restore(self, epoch: str | None, contiguous: int, since_ns: int = 0) -> None:
        """Load the persisted cursor (03 §4.1): the next connection resumes from ``contiguous + 1``."""
        self.epoch = epoch
        self.contiguous = max(0, contiguous)
        self.frontier = self.contiguous + 1
        self._above.clear()
        self._held.clear()
        if since_ns:
            self._disconnect_ns = since_ns

    def resume_request(self) -> dict[str, Any] | None:
        """``resume`` field of the next ``subscribe`` (02 §9.1)."""
        return None if self.epoch is None else {"epoch": self.epoch, "from_seq": self.contiguous + 1}

    # ------------------------------------------------------------ connection
    def on_subscribe_sent(self, req: Any, resume: dict[str, Any] | None) -> None:
        self._connected = True
        self._resume_sent = resume
        self._resume_req = req
        self._resume_range = None
        self._awaiting_resume = resume is not None
        self._tail_count = 0

    def on_welcome(self, epoch: str, head: int, ring_low: int) -> None:
        self._welcome_epoch = epoch
        if epoch == self.epoch:
            self._last_head = max(self._last_head, head)
        if self._resume_sent is None:
            self._start_fresh(epoch, head)  # a connection without resume starts at the server's head

    def on_disconnect(self) -> None:
        """Connection lost: release held ticks (live data) and forget pending requests."""
        self._connected = False
        self._awaiting_resume = False
        self._resume_range = None
        self._disconnect_ns = self._clock.now_ns()
        self._release_held(request=False)
        for h in self.holes:
            h.req = None
            h.answered = False
        self._by_req.clear()

    def _start_fresh(self, epoch: str, head: int) -> None:
        self.epoch = epoch
        self.contiguous = head
        self.frontier = head + 1
        self._above.clear()
        self._held.clear()
        self._held_since = None
        self._last_head = head

    # ---------------------------------------------------------------- resume
    def on_replay_begin(self, req: Any, lo: int, hi: int) -> None:
        if self._awaiting_resume and req == self._resume_req:
            self._awaiting_resume = False
            self._resume_range = (lo, hi)
            self.frontier = max(self.frontier, hi + 1)
            if lo <= hi:
                self._range_hole(lo, hi, RESUME, answered=True)
            return
        hole = self._by_req.get(req)
        if hole is None:
            logger.debug("feed_a.replay_begin_unknown_req", req=req)
            return
        hole.answered = True
        if lo > hole.lo:  # 02 §9.1：请求区间超出环形缓冲时只补回缓冲内的部分
            self._give_up(hole, [s for s in hole.missing if s < lo])

    def _finish_resume(self) -> None:
        """End of the resume block: whatever of its range is still missing is unrecoverable."""
        rng = self._resume_range
        self._resume_range = None
        if rng is not None:
            for h in self.open_holes:
                self._give_up(h, [s for s in h.missing if rng[0] <= s <= rng[1]])
                self._maybe_close(h)
            self._sink.resume_finished()

    def on_resume_rejected(self, reason: str, ring_low: int, head: int) -> None:
        """``EPOCH_CHANGED`` / ``SEQ_EVICTED`` / ``BAD_RESUME``; a snapshot follows in every case (02 §9.1)."""
        self._awaiting_resume = False
        new_epoch = self._welcome_epoch or self.epoch or ""
        if reason == "SEQ_EVICTED":
            for h in self.open_holes:
                self._give_up(h, [s for s in h.missing if s < ring_low])
                self._maybe_close(h)
            lost = sum(1 for s in range(self.contiguous + 1, ring_low) if s not in self._above)
            if lost:
                self._lost_range(self.epoch or new_epoch, self.contiguous + 1, ring_low - 1, lost, reason)
            self._skip_through(ring_low - 1)
            self._backfill_ring(ring_low, head)
        else:
            old = self.epoch
            for h in self.open_holes:
                self._give_up(h, list(h.missing))
                self._maybe_close(h)
            if reason == "EPOCH_CHANGED":
                if old is not None and self._last_head > self.contiguous:
                    lost = sum(1 for s in range(self.contiguous + 1, self._last_head + 1) if s not in self._above)
                    if lost:
                        self._lost_range(old, self.contiguous + 1, self._last_head, lost, reason)
                self._start_fresh(new_epoch, ring_low - 1)
                self._backfill_ring(ring_low, head)
                self._last_head = head
            else:  # BAD_RESUME: our cursor is ahead of the server -> handled as a new connection
                logger.warning("feed_a.bad_resume", contiguous=self.contiguous, head=head, epoch=new_epoch)
                self._start_fresh(new_epoch, head)
        self._sink.resume_finished()

    def _backfill_ring(self, ring_low: int, head: int) -> None:
        """Replay what the ring still holds of the missed range."""
        self.frontier = max(self.frontier, head + 1)
        lo = max(ring_low, self.contiguous + 1)
        hole = self._range_hole(lo, head, RING)
        if hole is not None:
            self._request(hole)
        elif not any(h.missing for h in self.open_holes):
            self._skip_through(head)

    def _range_hole(self, lo: int, hi: int, kind: str, *, answered: bool = False) -> Hole | None:
        """Open a hole for the seqs of ``[lo, hi]`` not received and not claimed by another hole."""
        claimed: set[int] = set()
        for h in self.open_holes:
            claimed |= h.missing
        missing = {s for s in range(max(lo, self.contiguous + 1), hi + 1) if s not in self._above and s not in claimed}
        if not missing:
            return None
        now = self._clock.now_ns()
        from_ns = now if kind == TAIL else self._disconnect_ns or now  # resume / ring gaps span the disconnection
        hole = Hole(self.epoch or "", lo, hi, kind, missing, self._clock.monotonic_ns(), from_ns, now, answered=answered)
        self._add_hole(hole)
        return hole

    # ----------------------------------------------------------------- ticks
    def on_tick(self, seq: int, replay: bool, payload: Any) -> None:
        if seq <= self.contiguous or seq in self._above or seq in self._held:
            self.stats["duplicates"] += 1
            for h in self.holes:
                if not h.closed and h.lo <= seq <= h.hi:
                    h.duplicates += 1
                    break
            return
        if seq == self.frontier:
            self._deliver(seq, replay, payload)
            self.frontier = seq + 1
            while self.frontier in self._held:
                held = self._held.pop(self.frontier)
                self._deliver(self.frontier, held.replay, held.payload)
                self.frontier += 1
            if not self._held:
                self._held_since = None
            return
        if seq > self.frontier:
            self._held[seq] = _Held(payload, replay)
            self.stats["held"] += 1
            if self._held_since is None:
                self._held_since = self._clock.monotonic_ns()
            if len(self._held) > self._reorder_max:
                self._release_held(request=True)
            return
        self._deliver(seq, True, payload)  # below the frontier, never received: fills a hole
        for h in self.holes:
            if not h.closed and seq in h.missing:
                h.missing.discard(seq)
                h.filled += 1
                if not h.missing and h.kind != RESUME:
                    if h.req is not None:
                        self._by_req.pop(h.req, None)
                    self._maybe_close(h)
                return

    def on_replay_end(self, req: Any, hi: int) -> None:
        if req == self._resume_req and self._resume_range is not None:
            self._finish_resume()
            return
        hole = self._by_req.pop(req, None)
        if hole is None or hole.closed:
            return
        self._give_up(hole, [s for s in hole.missing if s <= hi])
        self._maybe_close(hole)
        if not hole.closed:
            hole.req = None  # something above ``hi`` still missing: poll() asks again

    def on_error(self, req: Any, code: str, ring_low: int | None, head: int | None) -> None:
        if code == "BAD_RESUME" and self._awaiting_resume:
            self.on_resume_rejected("BAD_RESUME", ring_low or 1, head if head is not None else self.contiguous)
            return
        hole = self._by_req.pop(req, None) if req is not None else None
        if hole is None:
            logger.warning("feed_a.error", req=req, code=code)
        elif code in ("SEQ_EVICTED", "BAD_REQUEST"):  # 02 §9.1：整段已被挤出环形缓冲
            self._give_up(hole, list(hole.missing))
            self._maybe_close(hole)
        else:
            logger.warning("feed_a.replay_error", req=req, code=code, gap_id=hole.gap_id)
            hole.req = None

    def on_heartbeat(self, head: int) -> None:
        self._last_head = max(self._last_head, head)
        if self._awaiting_resume or self._resume_range is not None or head <= self.max_seen:
            self._tail_count = 0
            return
        if self._tail_count and head >= self._tail_head:
            self._tail_count += 1
        else:
            self._tail_count, self._tail_head = 1, head
        if self._tail_count >= self._tail_heartbeats:
            self._tail_count = 0
            self._release_held(request=True)
            if self._tail_head >= self.frontier:
                hole = self._range_hole(self.frontier, self._tail_head, TAIL)
                self.frontier = self._tail_head + 1
                if hole is not None:
                    self._request(hole)

    # ---------------------------------------------------------------- timers
    def poll(self) -> None:
        """Reorder timeout and replay (re)requests; called every ~50 ms by the client."""
        mono = self._clock.monotonic_ns()
        if self._held and self._held_since is not None and mono - self._held_since >= self._reorder_ns:
            self._release_held(request=True)
        if not self._connected or self._awaiting_resume or self._resume_range is not None:
            return
        for hole in list(self.holes):
            if hole.closed or hole.kind == RESUME:
                continue
            if hole.req is None:
                self._request(hole)
            elif not hole.answered and mono - hole.requested_mono >= self._replay_timeout_ns:
                if hole.attempts >= self._max_attempts:
                    logger.warning("feed_a.replay_timeout", gap_id=hole.gap_id, attempts=hole.attempts)
                    self._sink.reconnect(f"replay of {hole.gap_id} unanswered after {hole.attempts} attempts")
                    return
                self._by_req.pop(hole.req, None)
                hole.req = None
                self._request(hole)

    # ------------------------------------------------------------- internals
    def _deliver(self, seq: int, replay: bool, payload: Any) -> None:
        self.stats["delivered"] += 1
        self._mark_done(seq)
        self._sink.deliver(seq, replay, payload)

    def _mark_done(self, seq: int) -> None:
        if seq <= self.contiguous:
            return
        if seq == self.contiguous + 1:
            self.contiguous = seq
            while self.contiguous + 1 in self._above:
                self._above.remove(self.contiguous + 1)
                self.contiguous += 1
        else:
            self._above.add(seq)

    def _skip_through(self, seq: int) -> None:
        """Consider every seq ``<= seq`` done (received or given up)."""
        if seq > self.contiguous:
            self.contiguous = seq
            self._above = {s for s in self._above if s > seq}
            while self.contiguous + 1 in self._above:
                self._above.remove(self.contiguous + 1)
                self.contiguous += 1

    def _release_held(self, *, request: bool) -> None:
        """Reorder window exhausted: the missing seqs become holes, held ticks go out in order."""
        if not self._held:
            return
        top = max(self._held)
        runs: list[tuple[int, int]] = []
        start: int | None = None
        for s in range(self.frontier, top + 1):
            if s in self._held:
                if start is not None:
                    runs.append((start, s - 1))
                    start = None
            elif start is None:
                start = s
        now, mono = self._clock.now_ns(), self._clock.monotonic_ns()
        holes = [Hole(self.epoch or "", lo, hi, HOLE, set(range(lo, hi + 1)), mono, now, now) for lo, hi in runs]
        for hole in holes:
            self._add_hole(hole)
        for s in sorted(self._held):
            held = self._held.pop(s)
            self._deliver(s, held.replay, held.payload)
        self.frontier = top + 1
        self._held_since = None
        if request:
            for hole in holes:
                self._request(hole)

    def _add_hole(self, hole: Hole) -> None:
        self.holes.append(hole)
        self.stats["holes"] += 1
        self._sink.gap_event(hole, "open")
        if len(self.holes) > 64:  # closed holes are only kept for bookkeeping
            self.holes = [h for h in self.holes if not h.closed]

    def _request(self, hole: Hole) -> None:
        if hole.closed or not self._connected or not hole.missing:
            return
        req = self._sink.request_replay(min(hole.missing), max(hole.missing))
        if req is not None:
            hole.req = req
            hole.answered = False
            hole.attempts += 1
            hole.requested_mono = self._clock.monotonic_ns()
            self._by_req[req] = hole

    def _give_up(self, hole: Hole, seqs: Iterable[int]) -> None:
        n = 0
        for s in list(seqs):
            if s in hole.missing:
                hole.missing.discard(s)
                self._mark_done(s)
                n += 1
        hole.unrecoverable += n
        self.stats["unrecoverable"] += n

    def _maybe_close(self, hole: Hole) -> None:
        if hole.closed or hole.missing:
            return
        hole.status = GapStatus.UNRECOVERABLE if hole.unrecoverable else GapStatus.FILLED
        hole.to_ns = hole.to_ns or self._clock.now_ns()
        self._sink.gap_event(hole, "close")

    def _lost_range(self, epoch: str, lo: int, hi: int, count: int, reason: str) -> None:
        now = self._clock.now_ns()
        hole = Hole(epoch, lo, hi, LOST, set(), self._clock.monotonic_ns(), self._disconnect_ns or now, now, unrecoverable=count, status=GapStatus.UNRECOVERABLE)
        hole.detail["reason"] = reason
        self.stats["unrecoverable"] += count
        self.holes.append(hole)
        self._sink.gap_event(hole, "lost")


__all__ = ["HOLE", "LOST", "RESUME", "RING", "TAIL", "Hole", "SeqTracker", "TrackerSink"]
