"""Source trust states TRUSTED / PROBATION / EXCLUDED (01 §8.13 仲裁规则与参数).

Exclusion (from TRUSTED or PROBATION, immediate): disconnect or ``exclude_missed_heartbeats``
missed; ``exclude_failed_polls`` failed polls in a row; reject rate over the last minute above
``exclude_reject_rate`` (with a minimum sample); ``exclude_integrity_votes`` cross-check votes;
manual ``md.source.set EXCLUDE`` (persisted in ``md_events`` and restored at start-up).
EXCLUDED -> PROBATION once connected again, healthy, not pinned and after ``min_dwell_s``;
PROBATION -> TRUSTED after ``probation_healthy_s`` healthy, caught up with the watermark and
gap-free (Feed A), after the dwell. Exclusions are never delayed; the dwell only delays
promotions, so a flapping source cannot oscillate faster than it. Promotions run on the timer.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from futrisk.messages.enums import SourceId, SourceState
from futrisk.services.md_gateway.params import MdParams

NS = 1_000_000_000


@dataclass
class TrustChange:
    """One state transition (-> ``SOURCE_STATE`` event)."""

    source: SourceId
    from_state: SourceState
    to_state: SourceState
    reason: str
    detail: dict[str, Any] = field(default_factory=dict)
    ts_ns: int = 0


@dataclass
class SourceTrust:
    """Trust bookkeeping of one source."""

    source: SourceId
    priority: int
    state: SourceState = SourceState.EXCLUDED
    since_mono: int = 0
    since_ns: int = 0
    initial: bool = True
    connected: bool = False
    healthy_since_mono: int | None = None
    manual: bool = False
    manual_detail: dict[str, Any] = field(default_factory=dict)
    exclude_reason: str = "not connected"
    failures: int = 0
    votes: deque[int] = field(default_factory=deque)
    outcomes: deque[tuple[int, bool]] = field(default_factory=deque)
    rejects_in_window: int = 0
    last_msg_ns: int | None = None
    last_msg_mono: int | None = None
    lag_ms: float | None = None
    msg_count: int = 0
    msgs_per_s: float = 0.0
    rate_mark: tuple[int, int] | None = None
    epoch: str | None = None
    integrity_votes_total: int = 0

    def reject_rate(self) -> float:
        n = len(self.outcomes)
        return self.rejects_in_window / n if n else 0.0


class TrustManager:
    """Trust state machine of every configured source (event-loop thread only)."""

    def __init__(
        self,
        params: MdParams,
        priorities: dict[SourceId, int],
        *,
        mono_ns: Callable[[], int],
        now_ns: Callable[[], int],
        on_change: Callable[[TrustChange], None],
    ) -> None:
        self._p = params
        self._mono = mono_ns
        self._now = now_ns
        self._on_change = on_change
        start = mono_ns()
        self.sources: dict[SourceId, SourceTrust] = {
            s: SourceTrust(s, prio, since_mono=start, since_ns=now_ns()) for s, prio in priorities.items()
        }

    # ------------------------------------------------------------------ queries
    def state(self, source: SourceId) -> SourceState:
        st = self.sources.get(source)
        return st.state if st is not None else SourceState.EXCLUDED

    def is_trusted(self, source: SourceId) -> bool:
        return self.state(source) is SourceState.TRUSTED

    def any_trusted(self) -> bool:
        return any(st.state is SourceState.TRUSTED for st in self.sources.values())

    def trusted_sources(self) -> list[SourceId]:
        return [s for s, st in self.sources.items() if st.state is SourceState.TRUSTED]

    # ------------------------------------------------------------------ signals
    def on_connected(self, source: SourceId, *, epoch: str | None = None) -> None:
        st = self.sources[source]
        st.connected = True
        st.failures = 0
        st.epoch = epoch if epoch is not None else st.epoch
        if st.healthy_since_mono is None:
            st.healthy_since_mono = self._mono()

    def on_disconnected(self, source: SourceId, reason: str) -> None:
        st = self.sources[source]
        st.connected = False
        st.healthy_since_mono = None
        self._exclude(st, "disconnected", {"why": reason})

    def on_poll(self, source: SourceId, ok: bool, *, error: str | None = None, lag_ms: float | None = None) -> None:
        """Result of one poll of a polled source (Feed B, Sina)."""
        st = self.sources[source]
        now = self._mono()
        if ok:
            st.failures = 0
            st.lag_ms = lag_ms
            st.last_msg_mono = now
            st.last_msg_ns = self._now()
            if not st.connected:
                st.connected = True
                st.healthy_since_mono = now
            elif st.healthy_since_mono is None:
                st.healthy_since_mono = now
            return
        st.failures += 1
        st.healthy_since_mono = None
        if st.failures >= self._p.arb.exclude_failed_polls:
            st.connected = False
            self._exclude(st, "failed_polls", {"failures": st.failures, "error": error})

    def on_message(
        self, source: SourceId, *, lag_ms: float | None = None, count: int = 1, mono: int | None = None, now: int | None = None
    ) -> None:
        """Any frame / record received (liveness, message rate, transport lag); cheap on the hot path."""
        st = self.sources[source]
        st.last_msg_mono = mono if mono is not None else self._mono()
        st.last_msg_ns = now if now is not None else self._now()
        if lag_ms is not None:
            st.lag_ms = lag_ms
        st.msg_count += count

    def update_rates(self, now_mono: int) -> None:
        """``msgs_per_s`` from the message counters (called about once per second)."""
        for st in self.sources.values():
            mark = st.rate_mark
            if mark is not None and now_mono > mark[0]:
                st.msgs_per_s = round((st.msg_count - mark[1]) / ((now_mono - mark[0]) / NS), 2)
            st.rate_mark = (now_mono, st.msg_count)

    def on_outcome(self, source: SourceId, rejected: bool, instrument_id: str = "") -> None:
        """One validated state (skipped ones are not counted, 02 §9.2)."""
        st = self.sources.get(source)
        if st is None:
            return
        now = self._mono()
        st.outcomes.append((now, rejected))
        if rejected:
            st.rejects_in_window += 1
        if len(st.outcomes) & 63 == 0 or rejected:
            self._trim_outcomes(st, now)
        if rejected and st.state is not SourceState.EXCLUDED and self._rate_bad(st):
            st.healthy_since_mono = None
            self._exclude(st, "reject_rate", {"reject_rate": round(st.reject_rate(), 4), "window": len(st.outcomes)})

    def on_vote(self, source: SourceId, detail: dict[str, Any] | None = None) -> None:
        """Cross-check vote against ``source`` (01 §8.13 被交叉校验投票)."""
        st = self.sources.get(source)
        if st is None:
            return
        now = self._mono()
        st.votes.append(now)
        st.integrity_votes_total += 1
        horizon = now - int(self._p.vote_window_s * NS)
        while st.votes and st.votes[0] < horizon:
            st.votes.popleft()
        st.healthy_since_mono = now if st.connected else None
        if len(st.votes) >= self._p.arb.exclude_integrity_votes and st.state is not SourceState.EXCLUDED:
            votes = len(st.votes)
            st.votes.clear()
            self._exclude(st, "integrity_votes", {"votes": votes, **(detail or {})})

    # ------------------------------------------------------------------ manual
    def manual(self, source: SourceId, action: str, actor: str, reason: str) -> TrustChange:
        """``md.source.set`` (02 §6.2): EXCLUDE pins EXCLUDED; INCLUDE lifts the pin (-> PROBATION if connected)."""
        st = self.sources[source]
        before = st.state
        detail = {"manual": True, "action": action, "actor": actor, "reason": reason}
        if action == "EXCLUDE":
            st.manual = True
            st.manual_detail = detail
            st.exclude_reason = "manual_exclude"
            if st.state is not SourceState.EXCLUDED:
                return self._transition(st, SourceState.EXCLUDED, "manual_exclude", detail, emit=False)
        else:
            st.manual = False
            st.manual_detail = {}
            if st.state is SourceState.EXCLUDED and st.connected:
                if st.healthy_since_mono is None:
                    st.healthy_since_mono = self._mono()
                return self._transition(st, SourceState.PROBATION, "manual_include", detail, emit=False)
        return TrustChange(source, before, st.state, f"manual_{action.lower()}", {**detail, "changed": False}, ts_ns=self._now())

    def restore_manual(self, pins: dict[SourceId, dict[str, Any]]) -> None:
        """Re-apply the manual EXCLUDE pins persisted before a restart."""
        for source, detail in pins.items():
            st = self.sources.get(source)
            if st is None:
                continue
            st.manual = True
            st.manual_detail = {**detail, "restored": True}
            st.exclude_reason = "manual_exclude"

    # ------------------------------------------------------------------ timer
    def evaluate(self, *, caught_up: Callable[[SourceId], bool], complete: Callable[[SourceId], bool]) -> None:
        """Periodic checks: reject-rate recovery and promotions (01 §8.13)."""
        now = self._mono()
        for st in self.sources.values():
            self._trim_outcomes(st, now)
            if st.connected and st.healthy_since_mono is None and not self._rate_bad(st):
                st.healthy_since_mono = now  # the health clock restarts after the last problem
            if st.manual or not st.connected or not self._dwell_ok(st, now) or st.healthy_since_mono is None:
                continue
            if st.state is SourceState.EXCLUDED:
                if not self._rate_bad(st):
                    self._transition(st, SourceState.PROBATION, "reconnected", {"after": st.exclude_reason})
            elif st.state is SourceState.PROBATION:
                if now - st.healthy_since_mono < self._p.probation_ns or self._rate_bad(st):
                    continue
                if complete(st.source) and caught_up(st.source):
                    self._transition(st, SourceState.TRUSTED, "probation_passed", {})

    # --------------------------------------------------------------- internals
    def _trim_outcomes(self, st: SourceTrust, now: int) -> None:
        horizon = now - int(self._p.reject_window_s * NS)
        outcomes = st.outcomes
        while outcomes and outcomes[0][0] < horizon:
            _, rej = outcomes.popleft()
            if rej:
                st.rejects_in_window -= 1

    def _rate_bad(self, st: SourceTrust) -> bool:
        return len(st.outcomes) >= self._p.reject_min_samples and st.reject_rate() > self._p.arb.exclude_reject_rate

    def _dwell_ok(self, st: SourceTrust, now: int) -> bool:
        return st.initial or now - st.since_mono >= self._p.dwell_ns

    def _exclude(self, st: SourceTrust, reason: str, detail: dict[str, Any]) -> None:
        if st.state is SourceState.EXCLUDED:
            st.exclude_reason = reason if st.exclude_reason == "not connected" else st.exclude_reason
            return
        st.exclude_reason = reason
        self._transition(st, SourceState.EXCLUDED, reason, detail)

    def _transition(
        self, st: SourceTrust, to: SourceState, reason: str, detail: dict[str, Any], *, emit: bool = True
    ) -> TrustChange:
        now = self._mono()
        before = st.state
        st.state = to
        st.since_mono = now
        st.since_ns = self._now()
        st.initial = False
        if to is SourceState.EXCLUDED:
            st.votes.clear()
        if to is SourceState.PROBATION and st.healthy_since_mono is None and st.connected:
            st.healthy_since_mono = now
        change = TrustChange(st.source, before, to, reason, dict(detail), ts_ns=st.since_ns)
        if emit:
            self._on_change(change)
        return change


__all__ = ["SourceTrust", "TrustChange", "TrustManager"]
