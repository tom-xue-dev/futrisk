"""The md-gateway pipeline for one received state (01 §5.3.2, §6.1 R1, §8.13).

``Incoming`` (normalized by its source adapter) → :class:`Validator` → :class:`Arbiter`
→ effects:

- LIVE: published on ``md.tick.{instrument}`` (the only path to risk-engine);
- LIVE + BACKFILL: raw recording and the validator's ordering reference;
- DUPLICATE / REJECTED: raw recording only (with the reason);
- every validated state updates the reject statistics of its source (skipped ones do
  not, 02 §9.2); cross-check votes go to the trust manager;
- events: ``BAD_TICK`` (rejected states, rate-limited per source / instrument / reason)
  and ``MD_INTEGRITY`` (``detail.action`` ``VOTE`` / ``NO_VOTE``, rate-limited).
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Protocol

from futrisk.messages.codes import (
    MD_REJECT_FUTURE_TS,
    MD_REJECT_MALFORMED,
    MD_REJECT_NON_MONOTONIC,
    MD_REJECT_OFF_TICK,
    MD_REJECT_PRICE_LIMIT,
    MD_REJECT_PRICE_WITHOUT_TRADE,
)
from futrisk.messages.enums import MdEventKind, SourceId, SourceState, TickMode
from futrisk.messages.md import Tick
from futrisk.services.md_gateway.arbiter import Arbiter, Decision
from futrisk.services.md_gateway.events import RateLimiter
from futrisk.services.md_gateway.model import Incoming
from futrisk.services.md_gateway.params import MdParams
from futrisk.services.md_gateway.validator import REJECT, SKIP, Validator

#: reject reasons reported as ``BAD_TICK`` events (timing rejects are only counted)
BAD_TICK_REASONS = frozenset(
    {
        MD_REJECT_PRICE_LIMIT,
        MD_REJECT_OFF_TICK,
        MD_REJECT_NON_MONOTONIC,
        MD_REJECT_PRICE_WITHOUT_TRADE,
        MD_REJECT_MALFORMED,
        MD_REJECT_FUTURE_TS,
    }
)
#: ``MdPipeline.last_outcome`` value besides the TickMode names
SKIPPED = "SKIPPED"
NS = 1_000_000_000
_LIVE = TickMode.LIVE
_BACKFILL = TickMode.BACKFILL
_REJECTED = TickMode.REJECTED


class TrustView(Protocol):
    """What the pipeline needs from the trust manager (the recording CLI substitutes an always-trusted view)."""

    def state(self, source: SourceId) -> SourceState: ...

    def on_outcome(self, source: SourceId, rejected: bool, instrument_id: str = "") -> None: ...

    def on_vote(self, source: SourceId, detail: dict[str, Any] | None = None) -> None: ...


class Effects(Protocol):
    """What the pipeline asks of the service."""

    def publish_tick(self, tick: Tick, recv_mono_ns: int) -> None: ...

    def record(self, inc: Incoming, mode: TickMode, reason: str | None) -> None: ...

    def emit(self, kind: MdEventKind, **kw: Any) -> None: ...


class MdPipeline:
    """Validator + arbiter + effects for every received state (event-loop thread only)."""

    def __init__(
        self,
        params: MdParams,
        validator: Validator,
        arbiter: Arbiter,
        trust: TrustView,
        effects: Effects,
        *,
        mono_ns: Callable[[], int],
    ) -> None:
        self._p = params
        self.validator = validator
        self.arbiter = arbiter
        self.trust = trust
        self._fx = effects
        self._mono = mono_ns
        max_keys = params.rate_limiter_max_keys
        self._bad_ticks = RateLimiter(int(params.bad_tick_event_interval_s * NS), max_keys=max_keys)
        self._integrity = RateLimiter(int(params.integrity_event_interval_s * NS), max_keys=max_keys)
        self.counts: dict[str, int] = {}
        self.source_counts: dict[str, int] = {}
        self.last_exch_ns: dict[SourceId, int] = {}
        self.last_outcome: str = SKIPPED

    def process(self, inc: Incoming) -> TickMode | None:
        """Handle one received state; returns its final mode (``None`` when skipped)."""
        src = inc.source
        sc = self.source_counts
        sc[src] = sc.get(src, 0) + 1
        tick = inc.tick
        if tick is None or inc.reject_reason is not None:
            self._reject(inc, inc.reject_reason or MD_REJECT_MALFORMED, {"detail": inc.detail} if inc.detail else {})
            return _REJECTED
        verdict = self.validator.check(inc)
        if verdict.status == SKIP:
            self.last_outcome = SKIPPED
            return None
        if verdict.status == REJECT:
            self._reject(inc, verdict.reason or MD_REJECT_MALFORMED, verdict.detail)
            return _REJECTED
        return self._classify(inc)

    def forget_source(self, source: SourceId) -> None:
        """After an epoch change: the source's ordering references are dropped (its seqs restart)."""
        self.validator.forget_source(source)

    # --------------------------------------------------------------- internals
    def _classify(self, inc: Incoming) -> TickMode:
        tick = inc.tick
        assert tick is not None
        src = inc.source
        trust = self.trust
        d = self.arbiter.classify(inc, trust.state, self._mono())
        if d.events or d.votes:
            self._decision_effects(d, tick.instrument_id, tick.trading_day)
        mode = d.mode
        if mode is _REJECTED:
            self._reject(inc, d.reason or MD_REJECT_MALFORMED, {"cross_source": True}, event=False)
            return _REJECTED
        if not inc.snapshot:
            trust.on_outcome(src, False, tick.instrument_id)
        self._count(mode)
        self._fx.record(inc, mode, None)
        self.last_outcome = str(mode)
        prev = self.last_exch_ns.get(src)
        if prev is None or tick.exch_ts_ns > prev:
            self.last_exch_ns[src] = tick.exch_ts_ns
        if mode is _LIVE or mode is _BACKFILL:
            self.validator.on_accepted(inc)
            if mode is _LIVE:
                self._fx.publish_tick(tick, inc.recv_mono_ns)
        return mode

    def _decision_effects(self, d: Decision, instrument_id: str, trading_day: str | None) -> None:
        for ev in d.events:
            action = str(ev.get("action", ""))
            ok, suppressed = self._integrity.allow((instrument_id, action), self._mono())
            if not ok:
                continue
            odd = ev.get("odd")
            self._fx.emit(
                MdEventKind.MD_INTEGRITY,
                trading_day=trading_day or ev.get("trading_day"),
                source=SourceId(odd) if odd else None,
                instrument_id=instrument_id,
                detail={**ev, "suppressed": suppressed},
            )
        for source, detail in d.votes:
            self.trust.on_vote(source, detail)

    def _reject(self, inc: Incoming, reason: str, detail: dict[str, Any], *, event: bool = True) -> None:
        self._count(_REJECTED)
        self._fx.record(inc, _REJECTED, reason)
        self.last_outcome = str(_REJECTED)
        iid = inc.tick.instrument_id if inc.tick is not None else inc.instrument_id
        self.trust.on_outcome(inc.source, True, iid)
        if not event or reason not in BAD_TICK_REASONS:
            return
        ok, suppressed = self._bad_ticks.allow((inc.source, iid, reason), self._mono())
        if not ok:
            return
        info: dict[str, Any] = {"reason": reason, "seq": inc.seq, "suppressed": suppressed, **detail}
        if inc.tick is not None:
            info.update(price=inc.tick.last, volume=inc.tick.volume, exch_ts_ns=inc.tick.exch_ts_ns)
        self._fx.emit(
            MdEventKind.BAD_TICK,
            trading_day=inc.tick.trading_day if inc.tick is not None else None,
            source=inc.source,
            instrument_id=iid or None,
            detail=info,
        )

    def _count(self, mode: TickMode) -> None:
        c = self.counts
        c[mode] = c.get(mode, 0) + 1


__all__ = ["BAD_TICK_REASONS", "SKIPPED", "Effects", "MdPipeline", "TrustView"]
