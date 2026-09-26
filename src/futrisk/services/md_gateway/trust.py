"""Source trust states TRUSTED / PROBATION / EXCLUDED (01 §8.13 仲裁规则与参数).

=====================================  ===================================================
transition                             condition (``feeds.yaml`` arbitration parameters)
=====================================  ===================================================
TRUSTED/PROBATION -> EXCLUDED          disconnect (or ``exclude_missed_heartbeats``
                                       missed); ``exclude_failed_polls`` failed polls in
                                       a row; reject rate over the last minute above
                                       ``exclude_reject_rate`` (with a minimum sample);
                                       ``exclude_integrity_votes`` cross-check votes;
                                       manual ``md.source.set EXCLUDE``. Immediate.
EXCLUDED -> PROBATION                  connected again (``重连成功``), healthy, not
                                       manually excluded, after ``min_dwell_s``
PROBATION -> TRUSTED                   healthy for ``probation_healthy_s`` and caught up
                                       with the watermark and gap-free (Feed A) and
                                       ``probation_agree_count`` same-volume agreements
                                       with a trusted source, after ``min_dwell_s``
bootstrap (no TRUSTED source at all)   the highest-priority PROBATION source meeting
                                       everything but the agreements is promoted
                                       directly (``detail.bootstrap = true``)
=====================================  ===================================================

**Reject rate per instrument** (C1): outcomes are counted per ``(source, instrument)``.
The source-level rate that can exclude a source is computed **without its worst
instrument** (unless it delivers a single one), so one broken contract - 1 of 13
instruments is already 7.7 % of a source's states - never excludes a whole source; two
or more bad contracts still do. ``SourceStatus.reject_rate`` reports the plain rate.

Exclusions are never delayed (safety first); ``min_dwell_s`` only delays promotions, so
a flapping source cannot oscillate faster than the dwell. More than
``flapping_max_switches`` transitions within ``flapping_window_s`` raise ``MD_FLAPPING``
once per window. Promotion checks run on the service timer. A manual EXCLUDE pin is
persisted (``md_events``) and restored at start-up (:meth:`TrustManager.restore_manual`).
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
    """One state transition (-> ``SOURCE_STATE`` event) or a flapping alert (-> ``MD_FLAPPING``)."""

    source: SourceId
    from_state: SourceState
    to_state: SourceState
    reason: str
    detail: dict[str, Any] = field(default_factory=dict)
    flapping: bool = False
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
    agreements: set[tuple[str, int]] = field(default_factory=set)
    votes: deque[int] = field(default_factory=deque)
    outcomes: deque[tuple[int, str, bool]] = field(default_factory=deque)
    rejects_in_window: int = 0
    inst_outcomes: dict[str, int] = field(default_factory=dict)
    inst_rejects: dict[str, int] = field(default_factory=dict)
    transitions: deque[int] = field(default_factory=deque)
    flapping_until_mono: int = 0
    last_msg_ns: int | None = None
    last_msg_mono: int | None = None
    lag_ms: float | None = None
    msg_count: int = 0
    msgs_per_s: float = 0.0
    rate_mark: tuple[int, int] | None = None
    epoch: str | None = None
    last_seq: int | None = None
    integrity_votes_total: int = 0

    def reject_rate(self) -> float:
        n = len(self.outcomes)
        return self.rejects_in_window / n if n else 0.0

    def worst_instrument(self) -> tuple[str | None, int, int]:
        """``(instrument, rejects, outcomes)`` of the instrument with the most rejects in the window."""
        best: tuple[str | None, int, int] = (None, 0, 0)
        for iid, rej in self.inst_rejects.items():
            if rej > best[1]:
                best = (iid, rej, self.inst_outcomes.get(iid, 0))
        return best

    def gating_rate(self, min_samples: int) -> tuple[float, int]:
        """Reject rate that can exclude the source: without its worst instrument when it delivers several.

        Returns ``(rate, sample)``; the caller needs ``sample >= min_samples`` to act on it.
        """
        n = len(self.outcomes)
        rej = self.rejects_in_window
        worst, w_rej, w_n = self.worst_instrument()
        if worst is not None and len(self.inst_outcomes) > 1:
            n -= w_n
            rej -= w_rej
        return (rej / n if n > 0 else 0.0), n


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
        st.outcomes.append((now, instrument_id, rejected))
        st.inst_outcomes[instrument_id] = st.inst_outcomes.get(instrument_id, 0) + 1
        if rejected:
            st.rejects_in_window += 1
            st.inst_rejects[instrument_id] = st.inst_rejects.get(instrument_id, 0) + 1
        if len(st.outcomes) & 63 == 0 or rejected:
            self._trim_outcomes(st, now)
        if rejected and st.state is not SourceState.EXCLUDED and self._rate_bad(st):
            st.healthy_since_mono = None
            rate, n = st.gating_rate(self._p.reject_min_samples)
            worst = st.worst_instrument()
            self._exclude(
                st,
                "reject_rate",
                {"reject_rate": round(rate, 4), "window": n, "worst_instrument": worst[0], "worst_rejects": worst[1]},
            )

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

    def on_agreement(self, source: SourceId, key: tuple[str, int]) -> None:
        st = self.sources.get(source)
        if st is not None and st.state is SourceState.PROBATION:
            st.agreements.add(key)

    def set_seq(self, source: SourceId, epoch: str | None, last_seq: int | None) -> None:
        st = self.sources[source]
        st.epoch, st.last_seq = epoch, last_seq

    # ------------------------------------------------------------------ manual
    def preview_manual(self, source: SourceId, action: str) -> tuple[SourceState, SourceState]:
        """``(from, to)`` state that :meth:`manual` would produce now (for the command's event)."""
        st = self.sources[source]
        if action == "EXCLUDE":
            return st.state, SourceState.EXCLUDED
        if st.state is SourceState.EXCLUDED and st.connected:
            return st.state, SourceState.PROBATION
        return st.state, st.state

    def manual(self, source: SourceId, action: str, actor: str, reason: str) -> TrustChange:
        """``md.source.set`` (02 §6.2): EXCLUDE pins EXCLUDED; INCLUDE lifts the pin (-> PROBATION if connected)."""
        st = self.sources[source]
        before = st.state
        detail = {"manual": True, "action": action, "actor": actor, "reason": reason}
        if action == "EXCLUDE":
            st.manual = True
            st.manual_detail = detail
            if st.state is not SourceState.EXCLUDED:
                st.exclude_reason = "manual_exclude"
                return self._transition(st, SourceState.EXCLUDED, "manual_exclude", detail, emit=False)
            st.exclude_reason = "manual_exclude"
        else:
            st.manual = False
            st.manual_detail = {}
            if st.state is SourceState.EXCLUDED and st.connected:
                if st.healthy_since_mono is None:
                    st.healthy_since_mono = self._mono()
                return self._transition(st, SourceState.PROBATION, "manual_include", detail, emit=False)
        return TrustChange(source, before, st.state, f"manual_{action.lower()}", {**detail, "changed": False}, ts_ns=self._now())

    def restore_manual(self, pins: dict[SourceId, dict[str, Any]]) -> None:
        """Re-apply the manual EXCLUDE pins persisted before a restart (M4)."""
        for source, detail in pins.items():
            st = self.sources.get(source)
            if st is None:
                continue
            st.manual = True
            st.manual_detail = {**detail, "restored": True}
            st.exclude_reason = "manual_exclude"

    # ------------------------------------------------------------------ timer
    def evaluate(
        self,
        *,
        caught_up: Callable[[SourceId], bool],
        complete: Callable[[SourceId], bool],
    ) -> None:
        """Periodic checks: reject-rate recovery, promotions, bootstrap (01 §8.13)."""
        now = self._mono()
        for st in self.sources.values():
            self._trim_outcomes(st, now)
            if st.connected and st.healthy_since_mono is None and not self._rate_bad(st):
                st.healthy_since_mono = now  # the health clock restarts after the last problem
        # EXCLUDED -> PROBATION
        for st in self.sources.values():
            if st.state is SourceState.EXCLUDED and st.connected and not st.manual and self._dwell_ok(st, now):
                if self._rate_bad(st) or st.healthy_since_mono is None:
                    continue
                self._transition(st, SourceState.PROBATION, "reconnected", {"after": st.exclude_reason})
        # PROBATION -> TRUSTED
        eligible: list[SourceTrust] = []
        for st in self.sources.values():
            if st.state is not SourceState.PROBATION or not st.connected or st.manual:
                continue
            if not self._dwell_ok(st, now) or st.healthy_since_mono is None:
                continue
            if now - st.healthy_since_mono < self._p.probation_ns or self._rate_bad(st):
                continue
            if not complete(st.source) or not caught_up(st.source):
                continue
            eligible.append(st)
        if not eligible:
            return
        if self.any_trusted():
            for st in eligible:
                if len(st.agreements) >= self._p.arb.probation_agree_count:
                    self._transition(st, SourceState.TRUSTED, "probation_passed", {"agreements": len(st.agreements)})
        else:
            # 自举：当前没有任何可信源时，优先级最高的合格 PROBATION 源直接升为 TRUSTED
            best = min(eligible, key=lambda s: s.priority)
            self._transition(best, SourceState.TRUSTED, "bootstrap", {"bootstrap": True, "agreements": len(best.agreements)})

    # --------------------------------------------------------------- internals
    def _trim_outcomes(self, st: SourceTrust, now: int) -> None:
        horizon = now - int(self._p.reject_window_s * NS)
        outcomes = st.outcomes
        while outcomes and outcomes[0][0] < horizon:
            _, iid, rej = outcomes.popleft()
            left = st.inst_outcomes.get(iid, 1) - 1
            if left > 0:
                st.inst_outcomes[iid] = left
            else:
                st.inst_outcomes.pop(iid, None)
            if rej:
                st.rejects_in_window -= 1
                r = st.inst_rejects.get(iid, 1) - 1
                if r > 0:
                    st.inst_rejects[iid] = r
                else:
                    st.inst_rejects.pop(iid, None)

    def _rate_bad(self, st: SourceTrust) -> bool:
        if len(st.outcomes) < self._p.reject_min_samples:
            return False
        rate, n = st.gating_rate(self._p.reject_min_samples)
        return n >= self._p.reject_min_samples and rate > self._p.arb.exclude_reject_rate

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
        st.agreements.clear()
        if to is SourceState.EXCLUDED:
            st.votes.clear()
        if to is SourceState.PROBATION and st.healthy_since_mono is None and st.connected:
            st.healthy_since_mono = now
        change = TrustChange(st.source, before, to, reason, dict(detail), ts_ns=st.since_ns)
        if emit:
            self._on_change(change)
        st.transitions.append(now)
        horizon = now - self._p.flapping_window_ns
        while st.transitions and st.transitions[0] < horizon:
            st.transitions.popleft()
        if len(st.transitions) > self._p.arb.flapping_max_switches and now >= st.flapping_until_mono:
            st.flapping_until_mono = now + self._p.flapping_window_ns
            self._on_change(
                TrustChange(
                    st.source,
                    before,
                    to,
                    "flapping",
                    {"switches": len(st.transitions), "window_s": self._p.arb.flapping_window_s},
                    flapping=True,
                    ts_ns=st.since_ns,
                )
            )
        return change


__all__ = ["SourceTrust", "TrustChange", "TrustManager"]
