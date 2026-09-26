"""Arbiter: one LIVE stream per instrument out of several sources (01 §8.13, 02 §5, ADR-006).

Per instrument it keeps the watermark ``wm`` = ``order_key`` of the last LIVE state,
``order_key = (trading_day, volume, exch_ts_ns)`` (the cumulative volume is the shared
clock of all sources), and the fingerprint of that state. For every validated state it
decides, in this order:

1. **cross-check**: states of different sources with the same ``(trading_day, volume)``
   must carry the identical last price. On a mismatch (``MD_INTEGRITY``):

   - one side TRUSTED, the other not: the untrusted one is voted against (a newcomer
     found odd is rejected with ``MD_REJECT_INTEGRITY``);
   - both TRUSTED: nobody can tell yet which one is wrong, so there is **no vote**: the
     instrument is **held** (no LIVE, quality STALE) until a tie-breaker decides: a source
     contradicting itself at that volume (its earlier state is evicted as an outlier by
     the validator, :meth:`Arbiter.evicted`), a third source's price at that volume, or
     both sources agreeing at a later common volume (released without a vote). Only an
     identified loser is voted against. A hold also ends when one side stops being
     TRUSTED;
   - neither trusted: an event only.

   Matching prices between a TRUSTED and a PROBATION source count as an *agreement* for
   the latter (the probation -> trusted rule needs 3 of them);
2. **dedupe**: a fingerprint seen before labels the state ``DUPLICATE`` in the raw record;
   it only *blocks LIVE* when it equals the last LIVE state's fingerprint, or when the same
   state was already seen with a later exchange time (an old copy). A quote reverting to an
   earlier state (A -> B -> A, lock/unlock/lock) is therefore published again;
3. **LIVE** only if the state is not a replay/snapshot, its source is TRUSTED, the
   instrument is not held, ``order_key > wm``, ``-future_skew <= recv_ns - exch_ts_ns <=
   live_max_latency_ms`` (two-sided) and (for second-resolution sources such as Sina) the
   volume increased; the watermark then advances. Anything else is ``BACKFILL``:
   recorded, used for bars and settlement inputs, never published to risk-engine (01 §8.14).

**Watermark sanity**: when TRUSTED sources keep delivering live states stamped more than
``stale_s`` after the watermark's time but ordered below it (a watermark poisoned by an
outlier) for ``wm_sanity_s``, the watermark is rolled back to the last LIVE key those states
corroborate (``MD_INTEGRITY`` ``action=WM_ROLLBACK``); so is it when the validator evicts the
watermark state itself. After a roll-back the next LIVE ``order_key`` may be *lower* than
the previous one: that first LIVE tick carries ``FLAG_WM_RESET`` and consumers apply it
whatever its key (race-free, unlike the event which travels on another topic).

**Currency** (quality): the LIVE stream of an instrument is *current* while it is not held
and no validated non-LIVE state carrying new information (another fingerprint) is newer
than the watermark by more than ``stale_s`` - whatever the source's trust state.

Because the newest trusted source advances ``wm``, fail-over is instantaneous; a
recovering source stays in PROBATION (BACKFILL only) until the trust manager promotes
it. Watermarks (with their fingerprint) are persisted every ``watermark_flush_s`` and
reloaded at start-up (03 §4.1), so a restart never republishes an old state as LIVE.
"""

from __future__ import annotations

import bisect
from collections import OrderedDict, deque
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import msgspec

from futrisk.messages.codes import MD_REJECT_INTEGRITY
from futrisk.messages.enums import LimitLock, SourceId, SourceState, TickMode
from futrisk.messages.md import (
    FLAG_COARSE_TS,
    FLAG_LIMIT_DOWN_LOCKED,
    FLAG_LIMIT_UP_LOCKED,
    FLAG_WM_RESET,
    Tick,
)
from futrisk.services.md_gateway.model import Incoming, OrderKey, order_key
from futrisk.services.md_gateway.params import MdParams
from futrisk.services.md_gateway.validator import Evicted

_PRICE_EPS = 1e-6
_MAX_FP_DAYS = 2
_COARSE = int(FLAG_COARSE_TS)
_UP = int(FLAG_LIMIT_UP_LOCKED)
_DOWN = int(FLAG_LIMIT_DOWN_LOCKED)
_TRUSTED = SourceState.TRUSTED
_PROBATION = SourceState.PROBATION

StateOf = Callable[[SourceId], SourceState]
Watermark = tuple[OrderKey, int | None]
"""``(order_key, fingerprint)`` of the last LIVE state (``watermarks`` row)."""


@dataclass
class Decision:
    """Arbitration result of one state (or of an eviction)."""

    mode: TickMode
    reason: str | None = None
    agreements: list[SourceId] = field(default_factory=list)
    votes: list[tuple[SourceId, dict[str, Any]]] = field(default_factory=list)
    events: list[dict[str, Any]] = field(default_factory=list)
    #: a trusted source's newest state published LIVE after a hold release / watermark roll-back
    republish: list[Tick] = field(default_factory=list)
    #: the LIVE state is the first one after a watermark roll-back: publish it with ``FLAG_WM_RESET``
    reset: bool = False


@dataclass
class Hold:
    """Two TRUSTED sources disagree on the last price at one volume (H1): no LIVE until a tie-breaker."""

    trading_day: str
    volume: int
    prices: dict[SourceId, float]
    since_mono: int


@dataclass
class InstrumentArb:
    """Arbitration state of one instrument."""

    instrument_id: str
    wm: OrderKey | None = None
    wm_source: SourceId | None = None
    wm_fp: int | None = None
    wm_dirty: bool = False
    last_live_ns: int | None = None
    last_live_mono: int | None = None
    last_trade_mono: int | None = None
    last_live_volume: int = -1
    latest: Tick | None = None
    live_hist: deque[tuple[OrderKey, int | None, SourceId | None]] = field(default_factory=lambda: deque(maxlen=64))
    fps: dict[str, OrderedDict[int, int]] = field(default_factory=dict)
    by_volume: OrderedDict[tuple[str, int], dict[SourceId, float]] = field(default_factory=OrderedDict)
    src_latest: dict[SourceId, OrderKey] = field(default_factory=dict)
    src_seen_mono: dict[SourceId, int] = field(default_factory=dict)
    src_tick: dict[SourceId, Tick] = field(default_factory=dict)
    hold: Hold | None = None
    newest_nonlive: tuple[str, int] | None = None
    ev_since: int | None = None
    ev_max: OrderKey | None = None
    ev_sources: set[SourceId] = field(default_factory=set)
    rollbacks: int = 0
    reset_pending: bool = False

    @property
    def limit_lock(self) -> LimitLock:
        t = self.latest
        if t is None:
            return LimitLock.NONE
        if t.flags & _UP:
            return LimitLock.UP
        if t.flags & _DOWN:
            return LimitLock.DOWN
        return LimitLock.NONE


class Arbiter:
    """Per-instrument arbitration (event-loop thread only)."""

    def __init__(self, params: MdParams, priorities: dict[SourceId, int] | None = None) -> None:
        self._p = params
        self._prio = dict(priorities or {})  # informational only: never used to pick a loser (H1)
        self._inst: dict[str, InstrumentArb] = {}

    # ------------------------------------------------------------------ state
    def state(self, instrument_id: str) -> InstrumentArb:
        st = self._inst.get(instrument_id)
        if st is None:
            st = InstrumentArb(instrument_id, live_hist=deque(maxlen=max(4, self._p.live_history)))
            self._inst[instrument_id] = st
        return st

    def get(self, instrument_id: str) -> InstrumentArb | None:
        return self._inst.get(instrument_id)

    def instruments(self) -> list[str]:
        return sorted(self._inst)

    def load_watermarks(self, marks: dict[str, OrderKey] | dict[str, Watermark]) -> None:
        """Restore persisted watermarks (03 §4.1): only newer states can become LIVE."""
        for iid, value in marks.items():
            if len(value) == 2 and isinstance(value[0], tuple):
                key, fp = value
            else:
                key, fp = value, None
            st = self.state(iid)
            if st.wm is None or key > st.wm:
                st.wm = key
                st.wm_fp = fp
                st.live_hist.append((key, fp, None))

    def dirty_watermarks(self) -> list[tuple[str, OrderKey | None, int | None]]:
        """Watermarks changed since the last call (persisted every ``watermark_flush_s``; ``None`` = rolled back to nothing)."""
        out: list[tuple[str, OrderKey | None, int | None]] = []
        for st in self._inst.values():
            if st.wm_dirty:
                out.append((st.instrument_id, st.wm, st.wm_fp))
                st.wm_dirty = False
        return out

    def caught_up(self, source: SourceId, now_mono: int, window_ns: int, lag_ns: int = 0) -> bool:
        """The source's latest states reached the watermark on every instrument it delivered recently.

        ``水位追上 wm`` of the probation rule (01 §8.13): per instrument the source's newest
        state is at the watermark, or at most ``lag_ns`` of exchange time behind it (a
        polled source is always up to one poll interval behind a push source). False when
        it delivered nothing in the window (nothing proves it is current).
        """
        seen = False
        for st in self._inst.values():
            t = st.src_seen_mono.get(source)
            if t is None or now_mono - t > window_ns:
                continue
            seen = True
            latest = st.src_latest.get(source)
            if st.wm is None or latest is None or latest >= st.wm:
                continue
            if latest[0] != st.wm[0] or latest[2] < st.wm[2] - lag_ns:
                return False
        return seen

    def current(self, instrument_id: str) -> bool:
        """The LIVE stream is current: not held, and no newer informative non-LIVE state by more than ``stale_s``."""
        st = self._inst.get(instrument_id)
        if st is None:
            return True
        if st.hold is not None:
            return False
        return self.lag_ns(instrument_id) <= self._p.stale_ns

    def lag_ns(self, instrument_id: str) -> int:
        """How far (exchange time) the newest validated non-LIVE state is ahead of the watermark."""
        st = self._inst.get(instrument_id)
        if st is None:
            return 0
        if st.hold is not None:
            return self._p.stale_ns + 1
        nl = st.newest_nonlive
        if nl is None or st.wm is None:
            return 0
        if nl[0] > st.wm[0]:
            return nl[1] - st.wm[2] if nl[1] > st.wm[2] else self._p.stale_ns + 1
        if nl[0] < st.wm[0]:
            return 0
        return max(0, nl[1] - st.wm[2])

    # ------------------------------------------------------------------ main
    def classify(self, inc: Incoming, state_of: StateOf, now_mono: int) -> Decision:
        t = inc.tick
        assert t is not None
        iid = t.instrument_id
        st = self._inst.get(iid)
        if st is None:
            st = self.state(iid)
        key = (t.trading_day, t.volume, t.exch_ts_ns)
        src = inc.source
        prev = st.src_latest.get(src)
        if prev is None or key > prev:
            st.src_latest[src] = key
        st.src_seen_mono[src] = now_mono

        d = Decision(TickMode.BACKFILL)
        my_state = state_of(src)
        vkey = (t.trading_day, t.volume)
        entries = st.by_volume.get(vkey)
        if entries:
            h = st.hold
            tie_break = h is not None and (h.trading_day, h.volume) == vkey and src not in h.prices
            for other, price in list(entries.items()):
                if other == src or (tie_break and h is not None and other in h.prices):
                    continue  # a third source's price at a held volume is a tie-breaker (_hold_progress), not a conflict
                other_state = state_of(other)
                if abs(price - t.last) <= _PRICE_EPS:
                    if other_state is _TRUSTED and my_state is _PROBATION:
                        d.agreements.append(src)
                    elif my_state is _TRUSTED and other_state is _PROBATION:
                        d.agreements.append(other)
                    continue
                # 01 §8.13：成交量相同，最新价必须完全一致
                info = {
                    "volume": t.volume,
                    "trading_day": t.trading_day,
                    "prices": {str(other): price, str(src): t.last},
                    "states": {str(other): str(other_state), str(src): str(my_state)},
                }
                if my_state is _TRUSTED and other_state is _TRUSTED:
                    # H1：两个可信源不一致时不按优先级投票：暂停该合约的 LIVE，等待裁决
                    self._hold(st, t, src, other, price, info, d, now_mono)
                elif my_state is _TRUSTED or other_state is _TRUSTED:
                    odd = other if my_state is _TRUSTED else src
                    d.votes.append((odd, {"instrument_id": iid, "volume": t.volume}))
                    d.events.append({**info, "action": "VOTE", "odd": str(odd)})
                    if odd == src:
                        d.mode = TickMode.REJECTED
                        d.reason = MD_REJECT_INTEGRITY
                        return d
                else:
                    d.events.append({**info, "action": "NO_VOTE", "odd": None})
        if entries is None:
            entries = {}
            st.by_volume[vkey] = entries
            if len(st.by_volume) > self._p.volume_history:
                st.by_volume.popitem(last=False)
        entries[src] = t.last
        cur = st.src_tick.get(src)
        if cur is None or key >= (cur.trading_day, cur.volume, cur.exch_ts_ns):
            st.src_tick[src] = t
        released = False
        if st.hold is not None:
            self._hold_progress(st, t, src, state_of, d)
            released = st.hold is None

        # M1：指纹只用于原始记录的 DUPLICATE 标注；只有与最后一个 LIVE 状态相同、或同一状态已以更晚时间出现过才阻止 LIVE
        fp = t.fingerprint
        ts = t.exch_ts_ns
        fps = st.fps.get(t.trading_day)
        if fps is None:
            fps = OrderedDict()
            st.fps[t.trading_day] = fps
            if len(st.fps) > _MAX_FP_DAYS:
                del st.fps[min(st.fps)]
        seen_ts = fps.get(fp)
        if seen_ts is None or ts > seen_ts:
            fps[fp] = ts
            if seen_ts is None and len(fps) > self._p.fingerprint_cap:
                fps.popitem(last=False)
        blocked = fp == st.wm_fp or (seen_ts is not None and seen_ts > ts)

        if not blocked and st.hold is None and self._is_live(inc, t, key, st, my_state):
            d.reset = self._advance(st, t, key, src, inc.recv_ns, now_mono)
            d.mode = TickMode.LIVE
            return d
        if seen_ts is not None:
            d.mode = TickMode.DUPLICATE
        if fp != st.wm_fp:
            nl = st.newest_nonlive
            if nl is None or (t.trading_day, ts) > nl:
                st.newest_nonlive = (t.trading_day, ts)
        if my_state is _TRUSTED and inc.live_eligible and st.wm is not None:
            self._sanity(st, t, key, src, inc, my_state, now_mono, d, state_of)
        if released and d.mode is not TickMode.LIVE:
            loser = next((e.get("odd") for e in reversed(d.events) if e.get("action") == "RELEASE"), None)
            self._republish_best(st, state_of, SourceId(loser) if loser else None, now_mono, d)
        return d

    def evicted(self, ev: Evicted, state_of: StateOf, now_mono: int) -> Decision:
        """The validator removed an accepted state as an outlier: resolve holds and roll the watermark back."""
        d = Decision(TickMode.REJECTED)
        st = self._inst.get(ev.instrument_id)
        if st is None:
            return d
        vkey = (ev.trading_day, ev.volume)
        entries = st.by_volume.get(vkey)
        if entries is not None and ev.source in entries and abs(entries[ev.source] - ev.last) <= _PRICE_EPS:
            del entries[ev.source]
        h = st.hold
        if h is not None and (h.trading_day, h.volume) == vkey and ev.source in h.prices:
            # H1 裁决：同源在该成交量上自相矛盾（或被另一可信源否定），它就是出错的一方
            d.votes.append((ev.source, {"instrument_id": ev.instrument_id, "volume": ev.volume}))
            d.events.append(self._release(st, "loser identified", loser=ev.source, by=ev.by))
        key = (ev.trading_day, ev.volume, ev.ts)
        rolled = False
        if st.wm == key and st.wm_source == ev.source:
            target = self._previous_live(st, key)
            d.events.append(self._rollback(st, target, "watermark state evicted as an outlier", by=ev.by))
            rolled = True
        cur = st.src_tick.get(ev.source)
        if cur is not None and (cur.trading_day, cur.volume, cur.exch_ts_ns) == key:
            del st.src_tick[ev.source]
        if rolled or d.votes:
            self._republish_best(st, state_of, ev.source, now_mono, d)
        return d

    def check_holds(self, state_of: StateOf, now_mono: int = 0) -> list[tuple[str, Decision]]:
        """Timer: a hold ends when one of its sources is no longer TRUSTED (no vote)."""
        out: list[tuple[str, Decision]] = []
        for st in self._inst.values():
            h = st.hold
            if h is not None and any(state_of(s) is not _TRUSTED for s in h.prices):
                d = Decision(TickMode.BACKFILL)
                d.events.append(self._release(st, "source no longer trusted"))
                self._republish_best(st, state_of, None, now_mono, d)
                out.append((st.instrument_id, d))
        return out

    def _republish_best(self, st: InstrumentArb, state_of: StateOf, exclude: SourceId | None, now_mono: int, d: Decision) -> None:
        """After a release / roll-back: the newest state of a TRUSTED source becomes LIVE when it qualifies."""
        if st.hold is not None:
            return
        best: Tick | None = None
        best_key: OrderKey | None = None
        for src, t in st.src_tick.items():
            if src == exclude or state_of(src) is not _TRUSTED:
                continue
            k = (t.trading_day, t.volume, t.exch_ts_ns)
            if best_key is None or k > best_key:
                best, best_key = t, k
        if best is None or best_key is None or (st.wm is not None and best_key <= st.wm) or best.fingerprint == st.wm_fp:
            return
        latency = best.recv_ns - best.exch_ts_ns
        if latency > self._p.live_max_latency_ns or latency < -self._p.future_skew_ns:
            return
        if best.flags & _COARSE and st.wm is not None and (best.trading_day, best.volume) <= (st.wm[0], st.wm[1]):
            return
        if self._advance(st, best, best_key, best.source, best.recv_ns, now_mono):
            best = msgspec.structs.replace(best, flags=best.flags | FLAG_WM_RESET)
        d.republish.append(best)

    # --------------------------------------------------------------- internals
    def _advance(self, st: InstrumentArb, t: Tick, key: OrderKey, src: SourceId, recv_ns: int, now_mono: int) -> bool:
        """Advance the watermark to a LIVE state; True when it is the first one after a roll-back."""
        st.wm = key
        st.wm_source = src
        st.wm_fp = t.fingerprint
        st.wm_dirty = True
        st.last_live_ns = recv_ns
        st.last_live_mono = now_mono
        if t.volume > st.last_live_volume or st.last_trade_mono is None:
            st.last_trade_mono = now_mono
        st.last_live_volume = t.volume
        st.live_hist.append((key, t.fingerprint, src))
        st.newest_nonlive = None
        st.ev_since = None
        st.ev_max = None
        st.ev_sources.clear()
        if st.latest is None or key >= order_key(st.latest):
            st.latest = t
        reset = st.reset_pending
        st.reset_pending = False
        return reset

    def _is_live(self, inc: Incoming, t: Tick, key: OrderKey, st: InstrumentArb, my_state: SourceState) -> bool:
        if not inc.live_eligible or my_state is not _TRUSTED:
            return False
        if st.wm is not None and key <= st.wm:
            return False
        latency = inc.recv_ns - t.exch_ts_ns
        if latency > self._p.live_max_latency_ns or latency < -self._p.future_skew_ns:
            return False  # C1：双向延迟判定，未来时间戳的状态不能成为 LIVE
        # ADR-006：时间戳只到秒的数据源（新浪）只有成交量增加时才能推进 LIVE
        return not (t.flags & _COARSE and st.wm is not None and (t.trading_day, t.volume) <= (st.wm[0], st.wm[1]))

    def _hold(
        self, st: InstrumentArb, t: Tick, src: SourceId, other: SourceId, price: float, info: dict[str, Any], d: Decision, now_mono: int
    ) -> None:
        h = st.hold
        if h is not None and (h.trading_day, h.volume) == (t.trading_day, t.volume):
            h.prices[src] = t.last
            h.prices.setdefault(other, price)
            d.events.append({**info, "action": "HOLD", "odd": None, "held_since_s": round((now_mono - h.since_mono) / 1e9, 3)})
            return
        st.hold = Hold(t.trading_day, t.volume, {other: price, src: t.last}, now_mono)
        d.events.append({**info, "action": "HOLD", "odd": None})

    def _hold_progress(self, st: InstrumentArb, t: Tick, src: SourceId, state_of: StateOf, d: Decision) -> None:
        h = st.hold
        assert h is not None
        if any(state_of(s) is not _TRUSTED for s in h.prices):
            d.events.append(self._release(st, "source no longer trusted"))
            return
        if t.trading_day != h.trading_day:
            d.events.append(self._release(st, "new trading day"))
            return
        if t.volume == h.volume and src not in h.prices and state_of(src) is not SourceState.EXCLUDED:
            # 第三个数据源裁决：与之价格一致的一方胜出
            losers = [s for s, p in h.prices.items() if abs(p - t.last) > _PRICE_EPS]
            winners = [s for s, p in h.prices.items() if abs(p - t.last) <= _PRICE_EPS]
            if winners and losers:
                for loser in losers:
                    d.votes.append((loser, {"instrument_id": st.instrument_id, "volume": t.volume}))
                d.events.append(self._release(st, "third source", loser=losers[0] if len(losers) == 1 else None, by=str(src)))
                for loser in losers:
                    self._discard_loser(st, loser, h, d)
            return
        if t.volume > h.volume and src in h.prices:
            entries = st.by_volume.get((t.trading_day, t.volume))
            if entries is None or not all(s in entries for s in h.prices):
                return
            prices = [entries[s] for s in h.prices]
            if max(prices) - min(prices) <= _PRICE_EPS:
                d.events.append(self._release(st, "sources agree at a later volume", volume=t.volume))
            else:
                st.hold = Hold(t.trading_day, t.volume, {s: entries[s] for s in h.prices}, h.since_mono)
                d.events.append(
                    {
                        "action": "HOLD",
                        "volume": t.volume,
                        "trading_day": t.trading_day,
                        "prices": {str(s): p for s, p in st.hold.prices.items()},
                        "odd": None,
                        "why": "still disagreeing at a later volume",
                    }
                )

    def _discard_loser(self, st: InstrumentArb, loser: SourceId, h: Hold, d: Decision) -> None:
        """The identified loser's state at the held volume leaves the cross-check; the watermark rolls back off it."""
        vkey = (h.trading_day, h.volume)
        entries = st.by_volume.get(vkey)
        if entries is not None:
            entries.pop(loser, None)
        cur = st.src_tick.get(loser)
        if cur is not None and (cur.trading_day, cur.volume) == vkey:
            del st.src_tick[loser]
        if st.wm is not None and st.wm_source == loser and (st.wm[0], st.wm[1]) == vkey:
            d.events.append(self._rollback(st, self._previous_live(st, st.wm), "watermark state from the losing source", by="third source"))

    def _release(self, st: InstrumentArb, why: str, *, loser: SourceId | None = None, by: str | None = None, volume: int | None = None) -> dict[str, Any]:
        h = st.hold
        st.hold = None
        ev: dict[str, Any] = {"action": "RELEASE", "why": why, "odd": str(loser) if loser is not None else None}
        if h is not None:
            ev.update(volume=h.volume, trading_day=h.trading_day, prices={str(s): p for s, p in h.prices.items()})
        if by is not None:
            ev["by"] = by
        if volume is not None:
            ev["agreed_volume"] = volume
        return ev

    def _sanity(
        self,
        st: InstrumentArb,
        t: Tick,
        key: OrderKey,
        src: SourceId,
        inc: Incoming,
        my_state: SourceState,
        now_mono: int,
        d: Decision,
        state_of: StateOf,
    ) -> None:
        """C1 watermark sanity: trusted live states newer than the watermark by ``stale_s`` but ordered below it."""
        wm = st.wm
        assert wm is not None
        if t.trading_day != wm[0] or key >= wm or t.exch_ts_ns <= wm[2] + self._p.stale_ns:
            return
        if st.ev_since is None or st.ev_max is None:
            st.ev_since = now_mono
            st.ev_max = key
        elif key > st.ev_max:
            st.ev_max = key
        st.ev_sources.add(src)
        if now_mono - st.ev_since < self._p.wm_sanity_ns:
            return
        target = self._live_at_or_below(st, st.ev_max)
        d.events.append(
            self._rollback(
                st, target, "trusted sources keep delivering newer states below the watermark", sources=sorted(str(s) for s in st.ev_sources)
            )
        )
        if self._is_live(inc, t, key, st, my_state) and t.fingerprint != st.wm_fp:
            d.reset = self._advance(st, t, key, src, inc.recv_ns, now_mono)
            d.mode = TickMode.LIVE
        else:
            self._republish_best(st, state_of, None, now_mono, d)

    def _live_at_or_below(self, st: InstrumentArb, key: OrderKey | None) -> tuple[OrderKey, int | None, SourceId | None] | None:
        if key is None:
            return None
        keys = [e[0] for e in st.live_hist]
        i = bisect.bisect_right(keys, key)
        return st.live_hist[i - 1] if i > 0 else None

    def _previous_live(self, st: InstrumentArb, key: OrderKey) -> tuple[OrderKey, int | None, SourceId | None] | None:
        keys = [e[0] for e in st.live_hist]
        i = bisect.bisect_left(keys, key)
        return st.live_hist[i - 1] if i > 0 else None

    def _rollback(
        self, st: InstrumentArb, target: tuple[OrderKey, int | None, SourceId | None] | None, why: str, **extra: Any
    ) -> dict[str, Any]:
        old = st.wm
        if target is None:
            st.wm, st.wm_fp, st.wm_source = None, None, None
        else:
            st.wm, st.wm_fp, st.wm_source = target
        while st.live_hist and (st.wm is None or st.live_hist[-1][0] > st.wm):
            st.live_hist.pop()
        st.wm_dirty = True
        st.rollbacks += 1
        st.reset_pending = True
        st.newest_nonlive = None
        st.ev_since = None
        st.ev_max = None
        st.ev_sources.clear()
        return {
            "action": "WM_ROLLBACK",
            "why": why,
            "from_key": list(old) if old is not None else None,
            "to_key": list(st.wm) if st.wm is not None else None,
            "odd": None,
            **extra,
        }


__all__ = ["Arbiter", "Decision", "Hold", "InstrumentArb", "Watermark"]
