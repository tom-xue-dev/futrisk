"""Arbiter: one LIVE stream per instrument out of several sources (01 §8.13, 02 §5, ADR-006).

Per instrument it keeps the watermark ``wm`` = ``order_key`` of the last LIVE state
(``order_key = (trading_day, volume, exch_ts_ns)``: the cumulative volume is the shared clock
of all sources) and that state's fingerprint. For every validated state: (1) **cross-check**:
states of different sources with the same ``(trading_day, volume)`` must carry the identical
last price; on a mismatch (``MD_INTEGRITY``) the side that is not TRUSTED is voted against when
exactly one side is TRUSTED (a newcomer found odd is rejected with ``MD_REJECT_INTEGRITY``);
when both or neither are TRUSTED nobody can tell: the event is recorded, the newcomer stays
BACKFILL and the earlier state keeps the watermark until the next trade; (2) **dedupe**: a
fingerprint seen before labels the state ``DUPLICATE``, and blocks LIVE only when it equals the
last LIVE fingerprint or the same state was already seen with a later time (a reverting quote
is published again); (3) **LIVE** only if not a replay / snapshot, the source is TRUSTED,
``order_key > wm``, ``-future_skew <= recv - exch_ts <= live_max_latency_ms`` and (Sina) the
volume increased; anything else is BACKFILL: recorded, used for the settlement inputs, never
published (01 §8.14). **Currency**: the LIVE stream is current while no validated non-LIVE
state with new information is newer than the watermark by more than ``stale_s``, whatever
its source's trust; the newest trusted source advances ``wm``, so fail-over is instantaneous.
Watermarks are persisted every ``watermark_flush_s`` and reloaded at start-up (03 §4.1).
"""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from futrisk.messages.codes import MD_REJECT_INTEGRITY
from futrisk.messages.enums import LimitLock, SourceId, SourceState, TickMode
from futrisk.messages.md import FLAG_COARSE_TS, FLAG_LIMIT_DOWN_LOCKED, FLAG_LIMIT_UP_LOCKED, Tick
from futrisk.services.md_gateway.model import Incoming, OrderKey, order_key
from futrisk.services.md_gateway.params import MdParams

_PRICE_EPS = 1e-6
_MAX_FP_DAYS = 2
_COARSE = int(FLAG_COARSE_TS)
_UP = int(FLAG_LIMIT_UP_LOCKED)
_DOWN = int(FLAG_LIMIT_DOWN_LOCKED)
_TRUSTED = SourceState.TRUSTED

StateOf = Callable[[SourceId], SourceState]
Watermark = tuple[OrderKey, int | None]
"""``(order_key, fingerprint)`` of the last LIVE state (``watermarks`` row)."""


@dataclass
class Decision:
    """Arbitration result of one state."""

    mode: TickMode
    reason: str | None = None
    votes: list[tuple[SourceId, dict[str, Any]]] = field(default_factory=list)
    events: list[dict[str, Any]] = field(default_factory=list)


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
    fps: dict[str, OrderedDict[int, int]] = field(default_factory=dict)
    by_volume: OrderedDict[tuple[str, int], dict[SourceId, float]] = field(default_factory=OrderedDict)
    src_latest: dict[SourceId, OrderKey] = field(default_factory=dict)
    src_seen_mono: dict[SourceId, int] = field(default_factory=dict)
    newest_nonlive: tuple[str, int] | None = None

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
        self._prio = dict(priorities or {})  # informational only: never used to pick a loser
        self._inst: dict[str, InstrumentArb] = {}

    # ------------------------------------------------------------------ state
    def state(self, instrument_id: str) -> InstrumentArb:
        st = self._inst.get(instrument_id)
        if st is None:
            st = InstrumentArb(instrument_id)
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

    def dirty_watermarks(self) -> list[tuple[str, OrderKey | None, int | None]]:
        """Watermarks changed since the last call (persisted every ``watermark_flush_s``)."""
        out: list[tuple[str, OrderKey | None, int | None]] = []
        for st in self._inst.values():
            if st.wm_dirty:
                out.append((st.instrument_id, st.wm, st.wm_fp))
                st.wm_dirty = False
        return out

    def caught_up(self, source: SourceId, now_mono: int, window_ns: int, lag_ns: int = 0) -> bool:
        """The source's latest states reached the watermark on every instrument it delivered recently.

        Per instrument the source's newest state is at the watermark, or at most ``lag_ns`` of
        exchange time behind it (a polled source is always up to one poll interval behind a
        push source). False when it delivered nothing in the window.
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
        """The LIVE stream is current: no newer informative non-LIVE state by more than ``stale_s``."""
        return self.lag_ns(instrument_id) <= self._p.stale_ns

    def lag_ns(self, instrument_id: str) -> int:
        """How far (exchange time) the newest validated non-LIVE state is ahead of the watermark."""
        st = self._inst.get(instrument_id)
        if st is None or st.newest_nonlive is None:
            return 0
        if st.wm is None:
            return self._p.stale_ns + 1  # the market moves but nothing was ever LIVE
        nl = st.newest_nonlive
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
        conflict = False
        vkey = (t.trading_day, t.volume)
        entries = st.by_volume.get(vkey)
        if entries:
            for other, price in list(entries.items()):
                if other == src or abs(price - t.last) <= _PRICE_EPS:
                    continue
                # 01 §8.13：成交量相同，最新价必须完全一致
                other_state = state_of(other)
                info = {
                    "volume": t.volume,
                    "trading_day": t.trading_day,
                    "prices": {str(other): price, str(src): t.last},
                    "states": {str(other): str(other_state), str(src): str(my_state)},
                }
                if (my_state is _TRUSTED) != (other_state is _TRUSTED):
                    odd = other if my_state is _TRUSTED else src
                    d.votes.append((odd, {"instrument_id": iid, "volume": t.volume}))
                    d.events.append({**info, "action": "VOTE", "odd": str(odd)})
                    if odd == src:
                        d.mode = TickMode.REJECTED
                        d.reason = MD_REJECT_INTEGRITY
                        return d
                else:
                    # both (or neither) trusted: nobody can tell who is wrong - no vote, the earlier state keeps the watermark
                    conflict = True
                    d.events.append({**info, "action": "NO_VOTE", "odd": None})
        if entries is None:
            entries = {}
            st.by_volume[vkey] = entries
            if len(st.by_volume) > self._p.volume_history:
                st.by_volume.popitem(last=False)
        entries[src] = t.last

        # 指纹只用于原始记录的 DUPLICATE 标注；只有与最后一个 LIVE 状态相同、或同一状态已以更晚时间出现过才阻止 LIVE
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

        if not blocked and not conflict and self._is_live(inc, t, key, st, my_state):
            self._advance(st, t, key, src, inc.recv_ns, now_mono)
            d.mode = TickMode.LIVE
            return d
        if seen_ts is not None:
            d.mode = TickMode.DUPLICATE
        if fp != st.wm_fp:
            nl = st.newest_nonlive
            if nl is None or (t.trading_day, ts) > nl:
                st.newest_nonlive = (t.trading_day, ts)
        return d

    # --------------------------------------------------------------- internals
    def _advance(self, st: InstrumentArb, t: Tick, key: OrderKey, src: SourceId, recv_ns: int, now_mono: int) -> None:
        st.wm = key
        st.wm_source = src
        st.wm_fp = t.fingerprint
        st.wm_dirty = True
        st.last_live_ns = recv_ns
        st.last_live_mono = now_mono
        if t.volume > st.last_live_volume or st.last_trade_mono is None:
            st.last_trade_mono = now_mono
        st.last_live_volume = t.volume
        st.newest_nonlive = None
        if st.latest is None or key >= order_key(st.latest):
            st.latest = t

    def _is_live(self, inc: Incoming, t: Tick, key: OrderKey, st: InstrumentArb, my_state: SourceState) -> bool:
        if not inc.live_eligible or my_state is not _TRUSTED:
            return False
        if st.wm is not None and key <= st.wm:
            return False
        latency = inc.recv_ns - t.exch_ts_ns
        if latency > self._p.live_max_latency_ns or latency < -self._p.future_skew_ns:
            return False  # 双向延迟判定：太迟或来自未来的状态不能成为 LIVE
        # ADR-006：时间戳只到秒的数据源（新浪）只有成交量增加时才能推进 LIVE
        return not (t.flags & _COARSE and st.wm is not None and (t.trading_day, t.volume) <= (st.wm[0], st.wm[1]))


__all__ = ["Arbiter", "Decision", "InstrumentArb", "Watermark"]
