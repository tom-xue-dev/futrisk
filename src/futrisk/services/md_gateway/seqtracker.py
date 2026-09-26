"""Feed A sequence tracking: duplicates, reordering, holes, replays and resume (02 §9.1, 01 §6.4, §8.13).

Feed A numbers every tick with one global ``seq`` per ``epoch``. :class:`SeqTracker`
keeps three positions:

- ``contiguous``: every seq ``<= contiguous`` was received (or given up as
  unrecoverable). It is the resume point (``from_seq = contiguous + 1``) and, once the
  matching raw shard is on disk, the persisted cursor (03 §4.1 ``feed_cursors``);
- ``frontier``: the next live seq expected (``max delivered + 1``);
- holes: declared missing ranges below ``frontier`` with their repair state.

Rules:

- a seq already received (``<= contiguous``, delivered above it, or held) is a duplicate
  and dropped;
- ``seq == frontier`` is delivered at once; a seq beyond it is held in the **reorder
  buffer** (at most ``reorder_max`` messages or ``reorder_ns``); when the buffer
  overflows or ages out, the still-missing seqs become a **hole**, the held ticks are
  released in seq order (live data keeps flowing) and an in-connection ``replay`` is
  requested for the hole;
- replayed ticks (``replay=true``) and late ticks below ``frontier`` only fill holes;
  they never move the live expectation (02 §9.1 连接内补洞) and are delivered with
  ``replay=True`` so the arbiter classifies them as BACKFILL;
- a heartbeat whose ``head_seq`` stays beyond everything received for
  ``tail_heartbeats`` heartbeats in a row reveals a **tail gap**, repaired the same way;
- on reconnect the client resumes from ``contiguous + 1``: the resume block becomes a
  ``resume`` hole; ``SEQ_EVICTED`` gives up the evicted part (``GAP_UNRECOVERABLE``)
  and replays what the ring still holds; ``EPOCH_CHANGED`` gives up the old epoch's
  known tail and backfills the new epoch's ring; ``BAD_RESUME`` starts again from the
  server's head (02 §9.1).

Holes report ``filled`` (seqs received through the repair), ``duplicates`` (seqs of the
range that had already arrived) and ``unrecoverable`` (seqs given up); the given-up seqs
are also kept as ranges (:attr:`SeqTracker.lost`) so that gaps left open by a previous
run can be settled one by one after the restart. The end of every resume (``replay_end``
of the resume block, or ``resume_rejected``) is reported to the sink
(:meth:`TrackerSink.resume_finished`). The tracker is transport-agnostic: the WebSocket
client feeds it messages and implements :class:`TrackerSink`; it never blocks and reads
time only from the injected clock.
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
        """``feed_a:{epoch}:{from_seq}-{to_seq}`` (02 §7.1 GapInfo)."""
        return f"feed_a:{self.epoch}:{self.lo}-{self.hi}"

    @property
    def closed(self) -> bool:
        return self.status != GapStatus.OPEN

    @property
    def size(self) -> int:
        return self.hi - self.lo + 1


class TrackerSink(Protocol):
    """Callbacks of :class:`SeqTracker` (implemented by the Feed A client)."""

    def deliver(self, seq: int, replay: bool, payload: Any) -> None:
        """Hand one tick to the pipeline (``replay`` = backfill, never LIVE)."""

    def request_replay(self, lo: int, hi: int) -> int | None:
        """Send ``replay {from_seq, to_seq}``; returns the request id (None when not connected)."""

    def gap_event(self, hole: Hole, event: str) -> None:
        """``event``: ``open``; ``close`` (FILLED / UNRECOVERABLE); ``lost`` (opened and given up at once)."""

    def reconnect(self, reason: str) -> None:
        """Drop the connection; the next one resumes from ``contiguous + 1``."""

    def resume_finished(self) -> None:
        """The resume after a (re)connection is complete: every seq up to the server head is received or given up."""


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
        self._reorder_max = max(1, reorder_max)
        self._reorder_ns = reorder_ns
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
        # connection state
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
        self.lost: list[tuple[str, int, int]] = []
        self.stats: dict[str, int] = {"delivered": 0, "duplicates": 0, "held": 0, "holes": 0, "unrecoverable": 0}

    def configure(self, *, reorder_max: int, reorder_ns: int) -> None:
        """Change the reorder buffer limits (config hot reload)."""
        self._reorder_max = max(1, reorder_max)
        self._reorder_ns = reorder_ns

    # ------------------------------------------------------------------ state
    @property
    def max_seen(self) -> int:
        top = self.frontier - 1
        if self._held:
            top = max(top, max(self._held))
        return top

    @property
    def last_head(self) -> int:
        return self._last_head

    @property
    def complete(self) -> bool:
        """No open hole, no held tick and no resume block pending: the stream is gap-free."""
        return self._connected and not self._held and not self._awaiting_resume and self._resume_range is None and not self.open_holes

    @property
    def open_holes(self) -> list[Hole]:
        return [h for h in self.holes if not h.closed]

    def oldest_open_hole_mono(self) -> int | None:
        """Monotonic time the oldest still open hole was opened (coverage: Feed A with a stale gap is not current)."""
        opened = [h.opened_mono for h in self.holes if not h.closed]
        return min(opened) if opened else None

    def lost_in(self, epoch: str, lo: int, hi: int) -> int:
        """How many seqs of ``[lo, hi]`` of ``epoch`` were given up (upper bound for ranges lost as a block)."""
        n = 0
        for e, a, b in self.lost:
            if e == epoch and a <= hi and b >= lo:
                n += min(b, hi) - max(a, lo) + 1
        return min(n, hi - lo + 1)

    def restore(self, epoch: str | None, contiguous: int, since_ns: int = 0) -> None:
        """Load the persisted cursor (03 §4.1): the next connection resumes from ``contiguous + 1``.

        ``since_ns`` (when the cursor was persisted) starts the time window of the resume gap.
        """
        self.epoch = epoch
        self.contiguous = max(0, contiguous)
        self.frontier = self.contiguous + 1
        self._above.clear()
        self._held.clear()
        if since_ns:
            self._disconnect_ns = since_ns

    def resume_request(self) -> dict[str, Any] | None:
        """``resume`` field of the next ``subscribe`` (02 §9.1: the first missing seq)."""
        if self.epoch is None:
            return None
        return {"epoch": self.epoch, "from_seq": self.contiguous + 1}

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
            # a connection without resume starts at the server's head (the snapshot follows)
            self._start_fresh(epoch, head)

    def on_disconnect(self) -> None:
        """Connection lost: release held ticks (live data) and forget pending requests."""
        self._connected = False
        self._awaiting_resume = False
        self._resume_range = None
        self._disconnect_ns = self._clock.now_ns()
        if self._held:
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
            self._on_resume_block(lo, hi)
            return
        hole = self._by_req.get(req)
        if hole is None:
            logger.debug("feed_a.replay_begin_unknown_req", req=req)
            return
        hole.answered = True
        if lo > hole.lo:
            # 02 §9.1：请求区间超出环形缓冲时只补回缓冲内的部分，其余记为无法恢复
            self._give_up(hole, [s for s in hole.missing if s < lo])

    def _on_resume_block(self, lo: int, hi: int) -> None:
        """Resume accepted: ``[lo, hi]`` is replayed now, live data continues at ``hi + 1``."""
        self._awaiting_resume = False
        self._resume_range = (lo, hi)
        if hi >= self.frontier:
            self.frontier = hi + 1
        if lo > hi:
            return
        claimed: set[int] = set()
        for h in self.open_holes:
            claimed |= h.missing
        missing = {s for s in range(max(lo, self.contiguous + 1), hi + 1) if s not in self._above and s not in claimed}
        if not missing:
            return
        now = self._clock.now_ns()
        hole = Hole(
            epoch=self.epoch or "",
            lo=lo,
            hi=hi,
            kind=RESUME,
            missing=missing,
            opened_mono=self._clock.monotonic_ns(),
            from_ns=self._disconnect_ns or now,
            to_ns=now,
            answered=True,
        )
        self._add_hole(hole)

    def _finish_resume(self) -> None:
        """End of the resume block: whatever of its range is still missing is unrecoverable."""
        rng = self._resume_range
        self._resume_range = None
        if rng is None:
            return
        lo, hi = rng
        for h in self.open_holes:
            self._give_up(h, [s for s in h.missing if lo <= s <= hi])
            self._maybe_close(h)
        self._sink.resume_finished()

    def on_resume_rejected(self, reason: str, ring_low: int, head: int) -> None:
        """``EPOCH_CHANGED`` / ``SEQ_EVICTED`` / ``BAD_RESUME``; a snapshot follows in every case (02 §9.1)."""
        self._awaiting_resume = False
        new_epoch = self._welcome_epoch or self.epoch or ""
        if reason == "SEQ_EVICTED":
            # 02 §9.1：只能补回 ring_low 之后的部分，之前的部分记为 GAP_UNRECOVERABLE
            for h in self.open_holes:
                self._give_up(h, [s for s in h.missing if s < ring_low])
                self._maybe_close(h)
            lost = [s for s in range(self.contiguous + 1, ring_low) if s not in self._above]
            if lost:
                self._lost_range(self.epoch or new_epoch, self.contiguous + 1, ring_low - 1, len(lost), reason)
            self._skip_through(ring_low - 1)
            self._backfill_ring(ring_low, head)
            self._sink.resume_finished()
            return
        if reason == "EPOCH_CHANGED":
            old = self.epoch
            for h in self.open_holes:
                self._give_up(h, list(h.missing))
                self._maybe_close(h)
            if old is not None and self._last_head > self.contiguous:
                lost_n = sum(1 for s in range(self.contiguous + 1, self._last_head + 1) if s not in self._above)
                if lost_n:
                    self._lost_range(old, self.contiguous + 1, self._last_head, lost_n, reason)
            self._start_fresh(new_epoch, ring_low - 1)
            self._backfill_ring(ring_low, head)
            self._last_head = head
            self._sink.resume_finished()
            return
        # BAD_RESUME: our cursor is ahead of the server -> handled as a new connection
        logger.warning("feed_a.bad_resume", contiguous=self.contiguous, head=head, epoch=new_epoch)
        for h in self.open_holes:
            self._give_up(h, list(h.missing))
            self._maybe_close(h)
        self._start_fresh(new_epoch, head)
        self._sink.resume_finished()

    def _backfill_ring(self, ring_low: int, head: int) -> None:
        """Replay what the ring still holds of the missed range: ``[max(ring_low, contiguous+1), head]``."""
        self.frontier = max(self.frontier, head + 1)
        lo = max(ring_low, self.contiguous + 1)
        claimed: set[int] = set()
        for h in self.open_holes:
            claimed |= h.missing  # older holes above ring_low keep their own seqs (and requests)
        missing = {s for s in range(lo, head + 1) if s not in self._above and s not in claimed}
        if not missing:
            if not claimed:
                self._skip_through(head)
            return
        now = self._clock.now_ns()
        hole = Hole(
            epoch=self.epoch or "",
            lo=lo,
            hi=head,
            kind=RING,
            missing=missing,
            opened_mono=self._clock.monotonic_ns(),
            from_ns=self._disconnect_ns or now,
            to_ns=now,
        )
        self._add_hole(hole)
        self._request(hole)

    # ----------------------------------------------------------------- ticks
    def on_tick(self, seq: int, replay: bool, payload: Any) -> None:
        if seq <= self.contiguous or seq in self._above or seq in self._held:
            self.stats["duplicates"] += 1
            hole = self._hole_covering(seq)
            if hole is not None:
                hole.duplicates += 1
            return
        if seq == self.frontier:
            self._deliver(seq, replay, payload)
            self.frontier = seq + 1
            self._drain_held()
            return
        if seq > self.frontier:
            self._held[seq] = _Held(payload, replay)
            self.stats["held"] += 1
            if self._held_since is None:
                self._held_since = self._clock.monotonic_ns()
            if len(self._held) > self._reorder_max:
                self._release_held(request=True)
            return
        # seq < frontier, never received: it fills a hole (replayed, or a late live tick)
        self._deliver(seq, True, payload)
        self._fill(seq)

    def on_replay_end(self, req: Any, hi: int) -> None:
        if req == self._resume_req and self._resume_range is not None:
            self._finish_resume()
            return
        hole = self._by_req.pop(req, None)
        if hole is None or hole.closed:
            return
        rest = [s for s in hole.missing if s <= hi]
        if rest:
            self._give_up(hole, rest)
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
            return
        if code in ("SEQ_EVICTED", "BAD_REQUEST"):
            # 02 §9.1：整段已被挤出环形缓冲 -> GAP_UNRECOVERABLE
            self._give_up(hole, list(hole.missing))
            self._maybe_close(hole)
            return
        logger.warning("feed_a.replay_error", req=req, code=code, gap_id=hole.gap_id)
        hole.req = None

    def on_heartbeat(self, head: int) -> None:
        self._last_head = max(self._last_head, head)
        if self._awaiting_resume or self._resume_range is not None:
            self._tail_count = 0
            return
        if head > self.max_seen:
            if self._tail_count and head >= self._tail_head:
                self._tail_count += 1
            else:
                self._tail_count = 1
                self._tail_head = head
            if self._tail_count >= self._tail_heartbeats:
                self._tail_count = 0
                self._tail_gap(self._tail_head)
        else:
            self._tail_count = 0

    def _tail_gap(self, head: int) -> None:
        if self._held:
            self._release_held(request=True)
        lo = self.frontier
        if head < lo:
            return
        now = self._clock.now_ns()
        hole = Hole(
            epoch=self.epoch or "",
            lo=lo,
            hi=head,
            kind=TAIL,
            missing=set(range(lo, head + 1)),
            opened_mono=self._clock.monotonic_ns(),
            from_ns=now,
            to_ns=now,
        )
        self.frontier = head + 1
        self._add_hole(hole)
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
            above = self._above
            while self.contiguous + 1 in above:
                above.remove(self.contiguous + 1)
                self.contiguous += 1
        else:
            self._above.add(seq)

    def _skip_through(self, seq: int) -> None:
        """Consider every seq ``<= seq`` done (received or given up)."""
        if seq <= self.contiguous:
            return
        self._above = {s for s in self._above if s > seq}
        self.contiguous = seq
        above = self._above
        while self.contiguous + 1 in above:
            above.remove(self.contiguous + 1)
            self.contiguous += 1

    def _drain_held(self) -> None:
        while self.frontier in self._held:
            h = self._held.pop(self.frontier)
            self._deliver(self.frontier, h.replay, h.payload)
            self.frontier += 1
        if not self._held:
            self._held_since = None

    def _release_held(self, *, request: bool) -> None:
        """Reorder window exhausted: the missing seqs become holes, held ticks go out in order."""
        if not self._held:
            return
        top = max(self._held)
        runs: list[tuple[int, int]] = []
        run_start: int | None = None
        for s in range(self.frontier, top + 1):
            if s in self._held:
                if run_start is not None:
                    runs.append((run_start, s - 1))
                    run_start = None
            elif run_start is None:
                run_start = s
        if run_start is not None:
            runs.append((run_start, top))
        now = self._clock.now_ns()
        mono = self._clock.monotonic_ns()
        new_holes: list[Hole] = []
        for lo, hi in runs:
            hole = Hole(
                epoch=self.epoch or "",
                lo=lo,
                hi=hi,
                kind=HOLE,
                missing=set(range(lo, hi + 1)),
                opened_mono=mono,
                from_ns=now,
                to_ns=now,
            )
            self._add_hole(hole)
            new_holes.append(hole)
        for s in sorted(self._held):
            h = self._held.pop(s)
            self._deliver(s, h.replay, h.payload)
        self.frontier = top + 1
        self._held_since = None
        if request:
            for hole in new_holes:
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
        if req is None:
            return
        hole.req = req
        hole.answered = False
        hole.attempts += 1
        hole.requested_mono = self._clock.monotonic_ns()
        self._by_req[req] = hole

    def _hole_covering(self, seq: int) -> Hole | None:
        for h in self.holes:
            if not h.closed and h.lo <= seq <= h.hi:
                return h
        return None

    def _fill(self, seq: int) -> None:
        for h in self.holes:
            if not h.closed and seq in h.missing:
                h.missing.discard(seq)
                h.filled += 1
                if not h.missing and h.kind != RESUME:
                    if h.req is not None:
                        self._by_req.pop(h.req, None)
                    self._maybe_close(h)
                return

    def _give_up(self, hole: Hole, seqs: Iterable[int]) -> None:
        given: list[int] = []
        for s in list(seqs):
            if s in hole.missing:
                hole.missing.discard(s)
                self._mark_done(s)
                given.append(s)
        if given:
            n = len(given)
            hole.unrecoverable += n
            self.stats["unrecoverable"] += n
            given.sort()
            start = prev = given[0]
            for s in given[1:]:
                if s != prev + 1:
                    self._note_lost(hole.epoch, start, prev)
                    start = s
                prev = s
            self._note_lost(hole.epoch, start, prev)

    def _note_lost(self, epoch: str, lo: int, hi: int) -> None:
        self.lost.append((epoch, lo, hi))
        if len(self.lost) > 512:
            del self.lost[:256]

    def _maybe_close(self, hole: Hole) -> None:
        if hole.closed or hole.missing:
            return
        hole.status = GapStatus.UNRECOVERABLE if hole.unrecoverable else GapStatus.FILLED
        hole.to_ns = hole.to_ns or self._clock.now_ns()
        self._sink.gap_event(hole, "close")

    def _lost_range(self, epoch: str, lo: int, hi: int, count: int, reason: str) -> None:
        now = self._clock.now_ns()
        hole = Hole(
            epoch=epoch,
            lo=lo,
            hi=hi,
            kind=LOST,
            missing=set(),
            opened_mono=self._clock.monotonic_ns(),
            from_ns=self._disconnect_ns or now,
            to_ns=now,
            unrecoverable=count,
            status=GapStatus.UNRECOVERABLE,
        )
        hole.detail["reason"] = reason
        self.stats["unrecoverable"] += count
        self._note_lost(epoch, lo, hi)
        self.holes.append(hole)
        self._sink.gap_event(hole, "lost")


__all__ = ["HOLE", "LOST", "RESUME", "RING", "TAIL", "Hole", "SeqTracker", "TrackerSink"]
