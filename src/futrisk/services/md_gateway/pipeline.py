"""The md-gateway pipeline for one received state (01 §5.3.2, §6.1 R1, §8.13).

``Incoming`` (normalized by its source adapter) → :class:`Validator` → :class:`Arbiter`
→ effects:

- LIVE: published on ``md.tick.{instrument}`` (the only path to risk-engine);
- LIVE + BACKFILL: raw recording, 1-minute bars, validator references (σ and volume rate
  from the live flow only - never from snapshots or released held states);
- DUPLICATE / REJECTED / QUARANTINED: raw recording only (with the reason);
- every validated state updates the reject statistics of its ``(source, instrument)``
  (skipped ones do not, 02 §9.2); cross-check votes and agreements go to the trust
  manager;
- held states resolved later (confirmed, dropped, or waiting for the other trusted
  sources) are recorded when decided; an accepted state evicted as an outlier counts as
  a reject of its source and may roll the watermark back (:meth:`Arbiter.evicted`);
- events: ``BAD_TICK`` (price/volume anomalies, rate-limited), ``MD_INTEGRITY``
  (``detail.action``: ``VOTE`` / ``NO_VOTE`` / ``HOLD`` / ``RELEASE`` / ``EVICT`` /
  ``WM_ROLLBACK``), ``QUARANTINE_CONFIRMED`` / ``QUARANTINE_DROPPED``.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Protocol

import msgspec

from futrisk.messages.codes import (
    MD_REJECT_FUTURE_TS,
    MD_REJECT_MALFORMED,
    MD_REJECT_NON_MONOTONIC,
    MD_REJECT_OFF_TICK,
    MD_REJECT_PRICE_LIMIT,
    MD_REJECT_PRICE_WITHOUT_TRADE,
    MD_REJECT_VOLUME_JUMP,
)
from futrisk.messages.enums import MdEventKind, SourceId, TickMode
from futrisk.messages.md import FLAG_WM_RESET, Tick
from futrisk.services.md_gateway.arbiter import Arbiter, Decision
from futrisk.services.md_gateway.bars import BarBuilder
from futrisk.services.md_gateway.events import RateLimiter
from futrisk.services.md_gateway.model import Incoming
from futrisk.services.md_gateway.params import MdParams
from futrisk.services.md_gateway.trust import TrustManager
from futrisk.services.md_gateway.validator import (
    ACCEPT,
    DROP,
    HELD_DUPLICATE,
    ORDER,
    QUARANTINE,
    REJECT,
    SKIP,
    Evicted,
    Spike,
    Validator,
)

#: reject reasons reported as ``BAD_TICK`` events (timing rejects are only counted)
BAD_TICK_REASONS = frozenset(
    {
        MD_REJECT_PRICE_LIMIT,
        MD_REJECT_OFF_TICK,
        MD_REJECT_NON_MONOTONIC,
        MD_REJECT_PRICE_WITHOUT_TRADE,
        MD_REJECT_VOLUME_JUMP,
        MD_REJECT_MALFORMED,
        MD_REJECT_FUTURE_TS,
    }
)
#: ``MdPipeline.last_outcome`` values besides the TickMode names
HELD = "HELD"
SKIPPED = "SKIPPED"
NS = 1_000_000_000
_LIVE = TickMode.LIVE
_BACKFILL = TickMode.BACKFILL
_DUPLICATE = TickMode.DUPLICATE
_REJECTED = TickMode.REJECTED
_QUARANTINED = TickMode.QUARANTINED
#: MD_INTEGRITY actions that change state are never rate-limited
_STATE_ACTIONS = frozenset({"HOLD", "RELEASE", "EVICT", "WM_ROLLBACK"})


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
        trust: TrustManager,
        bars: BarBuilder,
        effects: Effects,
        *,
        mono_ns: Callable[[], int],
    ) -> None:
        self._p = params
        self.validator = validator
        self.arbiter = arbiter
        self.trust = trust
        self.bars = bars
        self._fx = effects
        self._mono = mono_ns
        max_keys = params.rate_limiter_max_keys
        self._bad_ticks = RateLimiter(int(params.bad_tick_event_interval_s * NS), max_keys=max_keys)
        self._integrity = RateLimiter(int(params.integrity_event_interval_s * NS), max_keys=max_keys)
        self.counts: dict[str, int] = {}
        self.source_counts: dict[str, int] = {}
        self.last_exch_ns: dict[SourceId, int] = {}
        self.last_outcome: str = SKIPPED

    # ------------------------------------------------------------------ main
    def process(self, inc: Incoming) -> TickMode | None:
        """Handle one received state; returns its final mode (``None`` when skipped or held)."""
        src = inc.source
        sc = self.source_counts
        sc[src] = sc.get(src, 0) + 1
        tick = inc.tick
        if tick is None or inc.reject_reason is not None:
            reason = inc.reject_reason or MD_REJECT_MALFORMED
            self._reject(inc, reason, {"detail": inc.detail} if inc.detail else {})
            return _REJECTED
        trust = self.trust
        verdict = self.validator.check(inc, trusted=trust.is_trusted, trusted_sources=trust.trusted_sources)
        if verdict.evicted or verdict.dropped or verdict.released:
            for ev in verdict.evicted:
                self._evicted(ev)
            for spike, why in verdict.dropped:
                self._drop_spike(spike, why)
            for spike, by in verdict.released:
                self._confirm_spike(spike, by)
        status = verdict.status
        if status == ACCEPT:
            return self._classify(inc)
        if status == SKIP:
            self.last_outcome = SKIPPED
            return None
        if status == QUARANTINE:
            self.last_outcome = HELD
            return None
        if status == REJECT:
            self._reject(inc, verdict.reason or MD_REJECT_MALFORMED, verdict.detail)
            return _REJECTED
        if status == DROP:
            reason = verdict.reason or ""
            self._count(_QUARANTINED)
            self._fx.record(inc, _QUARANTINED, reason)
            if not inc.snapshot:  # snapshots are not part of the live flow: never counted (02 §9.2 spirit)
                trust.on_outcome(src, True, tick.instrument_id)
            self._dropped_event(inc, reason, verdict.detail, str(verdict.detail.get("why", "")))
            self.last_outcome = str(_QUARANTINED)
            return _QUARANTINED
        assert status == HELD_DUPLICATE
        self._count(_DUPLICATE)
        self._fx.record(inc, _DUPLICATE, None)
        trust.on_outcome(src, False, tick.instrument_id)
        self.last_outcome = str(_DUPLICATE)
        return _DUPLICATE

    def poll(self) -> None:
        """Held states past ``spike_confirm_ms``: release the self-confirmed ones, drop the others."""
        released, dropped = self.validator.poll()
        for spike, why in dropped:
            self._drop_spike(spike, why)
        for spike, by in released:
            self._confirm_spike(spike, by)

    def forget_source(self, source: SourceId) -> None:
        """After an epoch change: the source's history and its held states (old seqs) are dropped (L6)."""
        for spike, why in self.validator.forget_source(source):
            self._drop_spike(spike, why)

    def check_holds(self) -> None:
        """Timer: integrity holds whose sources are no longer both TRUSTED end without a vote."""
        for iid, d in self.arbiter.check_holds(self.trust.state, self._mono()):
            self._decision_effects(d, iid, None)

    # --------------------------------------------------------------- internals
    def _classify(self, inc: Incoming, *, update_stats: bool = True) -> TickMode:
        tick = inc.tick
        assert tick is not None
        src = inc.source
        trust = self.trust
        d = self.arbiter.classify(inc, trust.state, self._mono())
        if d.events or d.votes or d.agreements or d.republish:
            self._decision_effects(d, tick.instrument_id, tick.trading_day, tick.volume)
        mode = d.mode
        if mode is _REJECTED:
            self._reject(inc, d.reason or MD_REJECT_MALFORMED, {"cross_source": True}, event=False)
            return _REJECTED
        iid = tick.instrument_id
        if not inc.snapshot:
            trust.on_outcome(src, False, iid)
        self._count(mode)
        if d.reset:
            # first LIVE state after a watermark roll-back: consumers apply it whatever its order_key
            tick = msgspec.structs.replace(tick, flags=tick.flags | FLAG_WM_RESET)
            inc = Incoming(inc.source, inc.recv_ns, inc.recv_mono_ns, tick, None, "", inc.seq, inc.replay, inc.snapshot, inc.epoch)
        self._fx.record(inc, mode, None)
        self.last_outcome = str(mode)
        prev = self.last_exch_ns.get(src)
        if prev is None or tick.exch_ts_ns > prev:
            self.last_exch_ns[src] = tick.exch_ts_ns
        if mode is _LIVE or mode is _BACKFILL:
            self.validator.on_accepted(tick, update_stats=update_stats and not inc.snapshot and not inc.replay)
            self.bars.update(tick)
            if self.validator.has_held() and trust.is_trusted(src):
                confirmed, dropped, evicted = self.validator.cross_check(tick, src)
                for ev in evicted:
                    self._evicted(ev)
                for spike, why in dropped:
                    self._drop_spike(spike, why)
                for spike, by in confirmed:
                    self._confirm_spike(spike, by)
            if mode is _LIVE:
                self._fx.publish_tick(tick, inc.recv_mono_ns)
        return mode

    def _decision_effects(self, d: Decision, instrument_id: str, trading_day: str | None, volume: int = 0) -> None:
        for ev in d.events:
            odd = ev.get("odd")
            self._integrity_event(instrument_id, SourceId(odd) if odd else None, ev, trading_day)
        for source, detail in d.votes:
            self.trust.on_vote(source, detail)
        for agreeing in d.agreements:
            self.trust.on_agreement(agreeing, (trading_day or "", volume))
        for tick in d.republish:
            # the winner of a resolved conflict / the state after a roll-back is published now (recorded again as LIVE)
            mono = self._mono()
            inc = Incoming(tick.source, tick.recv_ns, mono, tick=tick, seq=tick.source_seq)
            self._count(_LIVE)
            self._fx.record(inc, _LIVE, None)
            self._fx.publish_tick(tick, mono)

    def _integrity_event(
        self, instrument_id: str, source: SourceId | None, detail: dict[str, Any], trading_day: str | None = None
    ) -> None:
        action = str(detail.get("action", ""))
        suppressed = 0
        if action not in _STATE_ACTIONS:
            ok, suppressed = self._integrity.allow((instrument_id, action), self._mono())
            if not ok:
                return
        self._fx.emit(
            MdEventKind.MD_INTEGRITY,
            trading_day=trading_day or detail.get("trading_day"),
            source=source,
            instrument_id=instrument_id,
            detail={**detail, "suppressed": suppressed},
        )

    def _evicted(self, ev: Evicted) -> None:
        """An accepted state was evicted as an outlier: it counts as a reject; holds and the watermark follow."""
        self.trust.on_outcome(ev.source, True, ev.instrument_id)
        self._integrity_event(
            ev.instrument_id,
            ev.source,
            {
                "action": "EVICT",
                "odd": str(ev.source),
                "volume": ev.volume,
                "price": ev.last,
                "exch_ts_ns": ev.ts,
                "key": str(ev.key),
                "reason": ev.reason,
                "by": ev.by,
            },
            ev.trading_day,
        )
        d = self.arbiter.evicted(ev, self.trust.state, self._mono())
        if d.events or d.votes or d.republish:
            self._decision_effects(d, ev.instrument_id, ev.trading_day, ev.volume)

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

    def _confirm_spike(self, spike: Spike, by: str) -> None:
        """A held state (and the same-source states that waited with it) is released in order."""
        tick = spike.tick
        if spike.kind != ORDER:
            self._fx.emit(
                MdEventKind.QUARANTINE_CONFIRMED,
                trading_day=tick.trading_day,
                source=spike.inc.source,
                instrument_id=tick.instrument_id,
                detail={
                    "kind": spike.kind,
                    "price": tick.last,
                    "volume": tick.volume,
                    "ref": spike.ref_last,
                    "deviation": spike.deviation,
                    "threshold": spike.threshold,
                    "followers": len(spike.followers),
                    "by": by,
                },
            )
        for inc in spike.all_incoming():
            self._classify(inc, update_stats=False)

    def _drop_spike(self, spike: Spike, why: str) -> None:
        tick = spike.tick
        order = spike.kind == ORDER
        mode = _REJECTED if order else _QUARANTINED
        for inc in spike.all_incoming():
            self._count(mode)
            self._fx.record(inc, mode, spike.reason)
            self.trust.on_outcome(inc.source, True, tick.instrument_id)
        if order:
            ok, suppressed = self._bad_ticks.allow((spike.inc.source, tick.instrument_id, spike.reason), self._mono())
            if ok:
                self._fx.emit(
                    MdEventKind.BAD_TICK,
                    trading_day=tick.trading_day,
                    source=spike.inc.source,
                    instrument_id=tick.instrument_id,
                    detail={
                        "reason": spike.reason,
                        "seq": spike.inc.seq,
                        "price": tick.last,
                        "volume": tick.volume,
                        "exch_ts_ns": tick.exch_ts_ns,
                        "why": why,
                        "suppressed": suppressed,
                    },
                )
            return
        self._dropped_event(
            spike.inc,
            spike.reason,
            {
                "kind": spike.kind,
                "ref": spike.ref_last,
                "deviation": spike.deviation,
                "threshold": spike.threshold,
                "followers": len(spike.followers),
            },
            why,
        )

    def _dropped_event(self, inc: Incoming, reason: str, detail: dict[str, Any], why: str) -> None:
        tick = inc.tick
        assert tick is not None
        ok, suppressed = self._bad_ticks.allow((inc.source, tick.instrument_id, "dropped", why), self._mono())
        if not ok:
            return
        self._fx.emit(
            MdEventKind.QUARANTINE_DROPPED,
            trading_day=tick.trading_day,
            source=inc.source,
            instrument_id=tick.instrument_id,
            detail={
                **detail,
                "reason": reason,
                "price": tick.last,
                "volume": tick.volume,
                "seq": inc.seq,
                "why": why,
                "suppressed": suppressed,
            },
        )

    def _count(self, mode: TickMode) -> None:
        c = self.counts
        c[mode] = c.get(mode, 0) + 1


__all__ = ["BAD_TICK_REASONS", "HELD", "SKIPPED", "Effects", "MdPipeline"]
