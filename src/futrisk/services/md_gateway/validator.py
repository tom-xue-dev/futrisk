"""Validator: per-source bad-tick defence (01 §8.13 坏点防线, 02 §11.3).

Checks, in order, for every normalized state:

1. **skip** (not validated, not recorded, not counted): polled records and subscribe
   snapshots of contracts outside their session *now*, or still carrying a previous
   trading day's state (02 §9.2 "不在交易时段内的合约");
2. ``MD_REJECT_OUT_OF_SESSION``: the exchange time lies outside every session/auction
   window of the tick's trading day (a short grace after each window end keeps
   closing snapshots);
3. ``MD_REJECT_PRICE_LIMIT`` / ``MD_REJECT_OFF_TICK``: last price inside the limits and on
   the tick grid;
4. **future stamp** (two-sided latency): a state stamped more than ``future_skew_s``
   after its receive time is quarantined at once (``MD_REJECT_FUTURE_TS``); it is never
   LIVE and never becomes anybody's predecessor;
5. ordering within the trading day against the **same source's** neighbours (Feed A by
   ``seq``, polled sources by exchange time): ``MD_REJECT_NON_MONOTONIC`` (volume, turnover
   or timestamp going backwards), ``MD_REJECT_PRICE_WITHOUT_TRADE`` (01 §8.13: the last
   price only changes with a volume increase). When the new state contradicts its
   predecessor but agrees with the state before it, the **predecessor** is the suspect
   (one bad state accepted into the history must not poison every later one): it is
   evicted at once when another trusted source corroborates the new state (a state at or
   after the suspect with a lower volume, or the same volume at another price), or when
   the source's next state contradicts it too; the new state waits meanwhile
   (``kind="order"``) and is rejected if the suspect turns out right;
6. **volume plausibility**: an increment above ``min + factor · rate · max(Δt, 1 s) +
   oi_fraction · OI`` is quarantined like a spike (``kind="volume"``); rate and OI come
   from accepted states (the source's predecessor, else the instrument reference, else
   ``pre_open_interest``), never from the record itself;
7. **spike quarantine** (``kind="price"``): a deviation from the reference price (same
   source's previous state, else the last accepted state of any source) larger than
   ``max(spike_ticks · tick, spike_z · σ · √Δt · ref)``, capped at ``spike_band_cap`` of
   the limit band. σ is a robust per-second EWMA volatility (samples winsorized at
   ``(k·σ)²``; never fed by snapshots or released spikes), seeded from the limit band.

A held state (spike, volume jump) is confirmed by the same source's next state that
supports it, or by another trusted source; with other trusted sources around, a
same-source confirmation also needs them not to contradict: the state waits (as a
*follower*) until one of them has shown a state at or after it, or until
``spike_confirm_ms`` expires. A contradiction (a trusted state after it that stays with
the reference) drops it. Unconfirmed held states are dropped after ``spike_confirm_ms``
and their level is remembered for ``spike_memory_s``: a later state at that level
confirms it (sparse contracts whose states are further apart than the confirm window).
Subscribe snapshots get the same checks without waiting (a spike is dropped at once).
A quarantined state is not part of the source's history until confirmed, so the next
genuine state is never mistaken for a price change without a trade.
"""

from __future__ import annotations

import bisect
import math
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from typing import Any

from futrisk.common.clock import NS_PER_S
from futrisk.marketdata.normalize import InstrumentMeta
from futrisk.messages.codes import (
    MD_QUARANTINE_SPIKE,
    MD_REJECT_FUTURE_TS,
    MD_REJECT_NON_MONOTONIC,
    MD_REJECT_OFF_TICK,
    MD_REJECT_OUT_OF_SESSION,
    MD_REJECT_PRICE_LIMIT,
    MD_REJECT_PRICE_WITHOUT_TRADE,
    MD_REJECT_VOLUME_JUMP,
)
from futrisk.messages.enums import SourceId
from futrisk.messages.md import Tick
from futrisk.services.md_gateway.model import Incoming
from futrisk.services.md_gateway.params import MdParams
from futrisk.services.md_gateway.sessions import SessionIndex

ACCEPT = "ACCEPT"
REJECT = "REJECT"
QUARANTINE = "QUARANTINE"
DROP = "DROP"  # quarantined and dropped at once (future stamp, snapshot spike, refuted by a known successor)
SKIP = "SKIP"
HELD_DUPLICATE = "HELD_DUPLICATE"  # a copy of a state that is currently held

PRICE = "price"
VOLUME = "volume"
ORDER = "order"

POLLED = frozenset({SourceId.FEED_B, SourceId.SINA})
_PRICE_EPS = 1e-6
_SIGMA_ALPHA = 0.02
_RATE_ALPHA = 0.05
_MIN_DT_S = 0.1
_SESSION_SECONDS = 14_400.0  # ≈ trading seconds of a day: seeds σ from the limit band
_HIST_SLACK = 64

Key = Any  # int (Feed A seq) or (exch_ts_ns, volume)
TrustedSources = Callable[[], Iterable[SourceId]]


@dataclass(slots=True)
class _S:
    volume: int
    turnover: float
    ts: int
    last: float
    fp: int
    oi: int


@dataclass
class _Hist:
    td: str
    keys: list[Key] = field(default_factory=list)
    states: dict[Key, _S] = field(default_factory=dict)


@dataclass
class Spike:
    """A held state (``kind``: price spike, volume jump or ordering suspect) waiting for a decision."""

    inc: Incoming
    key: Key
    ref_last: float
    threshold: float
    since_mono: int
    deviation: float
    kind: str = PRICE
    ref_volume: int = 0
    reason: str = MD_QUARANTINE_SPIKE
    suspect_key: Key = None
    followers: list[tuple[Incoming, Key]] = field(default_factory=list)
    self_confirmed: bool = False

    @property
    def tick(self) -> Tick:
        t = self.inc.tick
        assert t is not None
        return t

    def all_incoming(self) -> list[Incoming]:
        return [self.inc, *(f for f, _ in self.followers)]


@dataclass(slots=True)
class Evicted:
    """An accepted state removed from its source's history as an outlier."""

    source: SourceId
    instrument_id: str
    trading_day: str
    key: Key
    volume: int
    last: float
    ts: int
    fp: int
    reason: str
    by: str


@dataclass
class Verdict:
    """Validation result; ``released``/``dropped``/``evicted`` are earlier states resolved now."""

    status: str
    reason: str | None = None
    detail: dict[str, Any] = field(default_factory=dict)
    released: list[tuple[Spike, str]] = field(default_factory=list)
    dropped: list[tuple[Spike, str]] = field(default_factory=list)
    evicted: list[Evicted] = field(default_factory=list)


@dataclass(slots=True)
class _Ref:
    td: str
    last: float
    ts: int
    volume: int
    oi: int


@dataclass(slots=True)
class _Mem:
    """Level of a dropped (unconfirmed) held state, remembered for ``spike_memory_s`` (M3)."""

    td: str
    kind: str
    last: float
    ref_last: float
    volume: int
    ts: int
    since_mono: int


class Validator:
    """Stateful validator of all sources (event-loop thread only)."""

    def __init__(
        self,
        sessions: SessionIndex,
        params: MdParams,
        meta_of: Callable[[str], InstrumentMeta | None],
        *,
        mono_ns: Callable[[], int],
    ) -> None:
        self._sessions = sessions
        self._p = params
        self._meta_of = meta_of
        self._mono = mono_ns
        self._hist: dict[tuple[SourceId, str], _Hist] = {}
        self._spikes: dict[tuple[SourceId, str], Spike] = {}
        self._orphans: list[tuple[Spike, str]] = []
        self._memory: dict[tuple[SourceId, str], list[_Mem]] = {}
        self._ref: dict[str, _Ref] = {}
        self._var: dict[str, float] = {}
        self._rate: dict[str, float] = {}
        self._tick_f: dict[str, float] = {}
        self._trusted_sources: TrustedSources = lambda: ()
        self.evictions = 0

    def clear_caches(self) -> None:
        """Contract metadata changed (staged products applied at a trading-day roll)."""
        self._tick_f.clear()

    # ------------------------------------------------------------------ queries
    def pending_spikes(self) -> list[Spike]:
        return list(self._spikes.values())

    def has_held(self) -> bool:
        return bool(self._spikes)

    def held_count(self) -> int:
        return sum(1 + len(s.followers) for s in self._spikes.values())

    def min_held_seq(self, source: SourceId) -> int | None:
        seqs = [
            inc.seq
            for (src, _), s in self._spikes.items()
            if src == source
            for inc in s.all_incoming()
            if inc.seq is not None
        ]
        return min(seqs) if seqs else None

    def sigma(self, instrument_id: str) -> float:
        var = self._var.get(instrument_id)
        if var is not None and var > 0:
            return math.sqrt(var)
        return self._seed_sigma(instrument_id)

    def _seed_sigma(self, instrument_id: str) -> float:
        meta = self._meta_of(instrument_id)
        pct = float(meta.limit_pct) if meta is not None else 0.05
        # 默认波动率：涨跌停幅度约为 3σ（日），折算为每秒
        return pct / 3.0 / math.sqrt(_SESSION_SECONDS)

    def latest(self, source: SourceId, instrument_id: str) -> tuple[str, int, int, float] | None:
        """``(trading_day, volume, ts, last)`` of the source's newest accepted state (diagnostics, tests)."""
        hist = self._hist.get((source, instrument_id))
        if hist is None or not hist.keys:
            return None
        s = hist.states[hist.keys[-1]]
        return (hist.td, s.volume, s.ts, s.last)

    # ------------------------------------------------------------------ main
    def check(
        self,
        inc: Incoming,
        *,
        trusted: Callable[[SourceId], bool],
        trusted_sources: TrustedSources | None = None,
    ) -> Verdict:
        tick = inc.tick
        assert tick is not None
        if trusted_sources is not None:
            self._trusted_sources = trusted_sources
        iid = tick.instrument_id
        meta = self._meta_of(iid)
        if meta is None:  # the normalizer only builds ticks for known contracts
            return Verdict(REJECT, MD_REJECT_OUT_OF_SESSION, {"why": "unknown instrument"})
        p = self._p
        sessions = self._sessions
        td = tick.trading_day
        src = inc.source
        if inc.snapshot or src in POLLED:
            now_td = sessions.open_now(meta.sessions, inc.recv_ns, p.poll_skip_grace_ns)
            if now_td is None or now_td != td:
                return Verdict(SKIP)
        ts = tick.exch_ts_ns
        if not sessions.in_session(meta.sessions, td, ts, p.close_grace_ns):
            return Verdict(REJECT, MD_REJECT_OUT_OF_SESSION)
        last = tick.last
        if last < tick.lower_limit - _PRICE_EPS or last > tick.upper_limit + _PRICE_EPS:
            return Verdict(REJECT, MD_REJECT_PRICE_LIMIT, {"last": last, "upper": tick.upper_limit, "lower": tick.lower_limit})
        tick_f = self._tick_size(meta)
        if not _on_grid(last, tick_f):
            return Verdict(REJECT, MD_REJECT_OFF_TICK, {"last": last, "tick": tick_f})
        ahead = ts - inc.recv_ns
        if ahead > p.future_skew_ns:
            # C1：交易所时间比收到时刻还晚（超过允许的时钟偏差）：隔离，不能成为 LIVE 或任何状态的前驱
            return Verdict(DROP, MD_REJECT_FUTURE_TS, {"ahead_ms": round(ahead / 1e6, 1), "why": "future stamp"})
        if inc.snapshot:
            return self._check_snapshot(tick, tick_f)
        hk = (src, iid)
        hist = self._hist.get(hk)
        if hist is None or td > hist.td:
            hist = _Hist(td)
            self._hist[hk] = hist
            orphan = self._spikes.pop(hk, None)
            if orphan is not None:
                self._orphans.append((orphan, "new trading day"))
        elif td < hist.td:
            return Verdict(ACCEPT)  # a previous day's state: stateless checks only
        key: Key = inc.seq if inc.seq is not None else (ts, tick.volume)
        cur = _S(tick.volume, tick.turnover, ts, last, tick.fingerprint, tick.open_interest)
        known = hist.states.get(key)
        if known is not None:
            if known.fp == cur.fp:
                return Verdict(ACCEPT, detail={"source_duplicate": True})
            return Verdict(REJECT, MD_REJECT_NON_MONOTONIC, {"why": "conflicting state for the same key"})
        held = self._spikes.get(hk)
        if held is not None:
            for h_inc, h_key in ((held.inc, held.key), *held.followers):
                if h_key == key:
                    # a polled source serving a held state again: it stays held, the copy is a duplicate
                    if h_inc.tick is not None and h_inc.tick.fingerprint == cur.fp:
                        return Verdict(HELD_DUPLICATE)
                    return Verdict(REJECT, MD_REJECT_NON_MONOTONIC, {"why": "conflicting state for the same key"})
        verdict = Verdict(ACCEPT)
        # the held state of this source is decided by its next state
        if held is not None and _gt(key, held.key):
            waiting = self._resolve_with_next(hist, hk, held, inc, key, cur, verdict)
            if waiting is not None:
                return waiting
        bad = self._order_check(hist, hk, key, cur, inc, verdict)
        if bad is not None:
            return bad
        return self._plausibility(hist, hk, key, cur, inc, tick, tick_f, verdict)

    # ------------------------------------------------------------ held states
    def _resolve_with_next(
        self, hist: _Hist, hk: tuple[SourceId, str], held: Spike, inc: Incoming, key: Key, cur: _S, verdict: Verdict
    ) -> Verdict | None:
        """Decide this source's held state with its next state; returns a verdict when ``inc`` must wait too."""
        src, iid = hk
        if held.kind == ORDER:
            pred = hist.states.get(held.suspect_key)
            held_s = _s_of(held.tick)
            if pred is not None and _vs_pred(pred, cur) is not None and _vs_pred(held_s, cur) is None:
                # 同源的下一个状态也与可疑前驱矛盾、与被扣住的状态一致：前驱是坏点
                del self._spikes[hk]
                verdict.evicted.append(self._evict(hist, hk, held.suspect_key, pred, held.reason, "self"))
                self._insert(hist, held.key, held_s)
                verdict.released.append((held, "next state"))
                return None
            del self._spikes[hk]
            verdict.dropped.append((held, "suspect predecessor confirmed"))
            return None
        if not self._supports(held, cur):
            del self._spikes[hk]
            verdict.dropped.append((held, "refuted"))
            return None
        evidence = self._other_evidence(held, src, iid)
        if evidence == "contradict":
            del self._spikes[hk]
            verdict.dropped.append((held, "contradicted by a trusted source"))
            return None
        if evidence == "none":
            # H1：有其他可信源时，同源确认要等它们给出不矛盾的数据（最多 spike_confirm_ms）
            chain_last = _s_of(held.followers[-1][0].tick) if held.followers else _s_of(held.tick)  # type: ignore[arg-type]
            bad = _vs_pred(chain_last, cur)
            if bad is not None:
                return _reject(bad[0], {**bad[1], "held_chain": True}, verdict)
            held.followers.append((inc, key))
            held.self_confirmed = True
            return Verdict(
                QUARANTINE,
                held.reason,
                {"waiting_for": "other trusted sources", "held_key": str(held.key)},
                verdict.released,
                verdict.dropped,
                verdict.evicted,
            )
        del self._spikes[hk]
        self._confirm(hist, held)
        verdict.released.append((held, "next state" if evidence == "absent" else "next state + trusted source"))
        return None

    def _other_evidence(self, spike: Spike, src: SourceId, iid: str) -> str:
        """``absent`` (no other trusted source), ``none`` (no state at/after the held one yet), ``support``, ``contradict``."""
        others = [s for s in self._trusted_sources() if s != src]
        if not others:
            return "absent"
        t = spike.tick
        found = False
        window = self._p.spike_confirm_ns
        for other in others:
            hist = self._hist.get((other, iid))
            if hist is not None and hist.td == t.trading_day:
                for k in reversed(hist.keys):
                    s = hist.states[k]
                    if s.ts < t.exch_ts_ns:
                        break
                    if spike.kind == PRICE and s.ts > t.exch_ts_ns + window:
                        continue
                    found = True
                    if not self._supports(spike, s):
                        return "contradict"
            held = self._spikes.get((other, iid))
            if held is not None and held.kind == spike.kind:
                for h_inc in held.all_incoming():
                    ht = h_inc.tick
                    if ht is not None and ht.trading_day == t.trading_day and ht.exch_ts_ns >= t.exch_ts_ns:
                        found = True
                        if not self._supports(spike, _s_of(ht)):
                            return "contradict"
        return "support" if found else "none"

    def _supports(self, spike: Spike, s: _S) -> bool:
        t = spike.tick
        if spike.kind == VOLUME:
            return s.volume >= t.volume
        if spike.kind == ORDER:
            return True
        return abs(s.last - t.last) < abs(s.last - spike.ref_last)

    def cross_check(self, tick: Tick, source: SourceId) -> tuple[list[tuple[Spike, str]], list[tuple[Spike, str]], list[Evicted]]:
        """A trusted source's accepted state confirms or contradicts other sources' held states (01 §8.13).

        Returns ``(confirmed, dropped, evicted)``.
        """
        confirmed: list[tuple[Spike, str]] = []
        dropped: list[tuple[Spike, str]] = []
        evicted: list[Evicted] = []
        iid = tick.instrument_id
        s = _s_of(tick)
        for hk, spike in list(self._spikes.items()):
            src, sid = hk
            if sid != iid or src == source:
                continue
            st = spike.tick
            if st.trading_day != tick.trading_day:
                continue
            if spike.kind == ORDER:
                hist = self._hist.get(hk)
                pred = hist.states.get(spike.suspect_key) if hist is not None else None
                if hist is None or pred is None:
                    continue
                if _contradicts_pred(pred, s, st.last):
                    # 另一可信源也与可疑前驱矛盾：前驱是坏点
                    del self._spikes[hk]
                    evicted.append(self._evict(hist, hk, spike.suspect_key, pred, spike.reason, str(source)))
                    self._insert(hist, spike.key, _s_of(st))
                    confirmed.append((spike, f"trusted source {source}"))
                continue
            after = tick.exch_ts_ns >= st.exch_ts_ns
            if spike.kind == PRICE:
                if abs(tick.last - st.last) <= spike.threshold and tick.volume >= st.volume:
                    pass  # same level: confirmation below
                elif after and tick.exch_ts_ns <= st.exch_ts_ns + self._p.spike_confirm_ns and not self._supports(spike, s):
                    del self._spikes[hk]
                    dropped.append((spike, f"contradicted by {source}"))
                    continue
                else:
                    continue
            elif spike.kind == VOLUME and tick.volume < st.volume:
                if after:
                    del self._spikes[hk]
                    dropped.append((spike, f"contradicted by {source}"))
                continue
            del self._spikes[hk]
            hist = self._hist.get(hk)
            if hist is not None and hist.td == st.trading_day:
                self._confirm(hist, spike)
            confirmed.append((spike, f"trusted source {source}"))
        return confirmed, dropped, evicted

    def poll(self) -> tuple[list[tuple[Spike, str]], list[tuple[Spike, str]]]:
        """Held states past ``spike_confirm_ms``: self-confirmed ones are released, the others dropped.

        Returns ``(released, dropped)``.
        """
        now = self._mono()
        limit = self._p.spike_confirm_ns
        released: list[tuple[Spike, str]] = []
        dropped, self._orphans = self._orphans, []
        for hk in [k for k, s in self._spikes.items() if now - s.since_mono >= limit]:
            spike = self._spikes.pop(hk)
            if spike.self_confirmed:
                hist = self._hist.get(hk)
                if hist is not None and hist.td == spike.tick.trading_day:
                    self._confirm(hist, spike)
                released.append((spike, "next state (no other trusted source reported)"))
            else:
                if spike.kind in (PRICE, VOLUME):
                    self._remember(hk, spike)
                dropped.append((spike, "timeout"))
        return released, dropped

    def forget_source(self, source: SourceId) -> list[tuple[Spike, str]]:
        """Drop the ordering history and held states of a source (after an epoch change seqs restart)."""
        for key in [k for k in self._hist if k[0] == source]:
            del self._hist[key]
        out = [(self._spikes.pop(k), "epoch change") for k in [k for k in self._spikes if k[0] == source]]
        for key in [k for k in self._memory if k[0] == source]:
            del self._memory[key]
        return out

    # ------------------------------------------------------------ references
    def on_accepted(self, tick: Tick, *, update_stats: bool = True) -> None:
        """An accepted (LIVE/BACKFILL) state: instrument reference, and (live flow only) σ and volume rate.

        Snapshots and released held states pass ``update_stats=False`` (H3: they never feed σ).
        """
        iid = tick.instrument_id
        ref = self._ref.get(iid)
        ts = tick.exch_ts_ns
        if update_stats and ref is not None and ref.td == tick.trading_day and ts > ref.ts:
            dt = max((ts - ref.ts) / NS_PER_S, _MIN_DT_S)
            if ref.last > 0 and tick.last > 0:
                var = self._var.get(iid)
                if var is None:
                    seed = self._seed_sigma(iid)
                    var = seed * seed
                sample = 0.0
                if tick.last != ref.last:
                    r = math.log(tick.last / ref.last)
                    k = self._p.sigma_winsor_k
                    sample = min(r * r / dt, k * k * var)  # H3：样本截尾，单次跳变不会放大 σ
                self._var[iid] = (1 - _SIGMA_ALPHA) * var + _SIGMA_ALPHA * sample
            if tick.volume >= ref.volume:
                rate = (tick.volume - ref.volume) / dt
                prev_rate = self._rate.get(iid)
                self._rate[iid] = rate if prev_rate is None else (1 - _RATE_ALPHA) * prev_rate + _RATE_ALPHA * rate
        if ref is None or tick.trading_day > ref.td or (ref.td == tick.trading_day and ts >= ref.ts):
            self._ref[iid] = _Ref(tick.trading_day, tick.last, ts, tick.volume, tick.open_interest)

    # --------------------------------------------------------------- internals
    def _tick_size(self, meta: InstrumentMeta) -> float:
        f = self._tick_f.get(meta.instrument_id)
        if f is None:
            f = float(meta.tick_size)
            self._tick_f[meta.instrument_id] = f
        return f

    def _threshold(self, tick: Tick, tick_f: float, ref_last: float, dt_s: float) -> float:
        p = self._p
        th = max(p.arb.spike_ticks * tick_f, p.arb.spike_z * self.sigma(tick.instrument_id) * math.sqrt(dt_s) * ref_last)
        band = tick.upper_limit - tick.lower_limit
        if band > 0:
            th = min(th, max(tick_f, p.spike_band_cap * band))  # H3：阈值不超过涨跌停区间的 1/4
        return th

    def _jump_limit(self, instrument_id: str, ref_oi: int, dt_s: float) -> float:
        p = self._p
        limit = p.volume_jump_min + p.volume_jump_oi_fraction * max(ref_oi, 0)
        rate = self._rate.get(instrument_id)
        if rate is not None and rate > 0:
            limit += p.volume_jump_factor * rate * max(dt_s, 1.0)
        return limit

    def _check_snapshot(self, tick: Tick, tick_f: float) -> Verdict:
        """Subscribe snapshots: never held; a spike or an implausible volume against the reference is dropped."""
        ref = self._ref.get(tick.instrument_id)
        if ref is None or ref.td != tick.trading_day:
            return Verdict(ACCEPT)
        dt_s = max(abs(tick.exch_ts_ns - ref.ts) / NS_PER_S, 0.05)
        threshold = self._threshold(tick, tick_f, ref.last, dt_s)
        deviation = abs(tick.last - ref.last)
        if deviation > threshold + _PRICE_EPS:
            return Verdict(DROP, MD_QUARANTINE_SPIKE, {"ref": ref.last, "deviation": deviation, "threshold": threshold, "why": "snapshot"})
        jump = tick.volume - ref.volume
        if jump > 0:
            limit = self._jump_limit(tick.instrument_id, ref.oi, dt_s)
            if jump > limit:
                return Verdict(DROP, MD_REJECT_VOLUME_JUMP, {"increment": jump, "limit": round(limit), "why": "snapshot"})
        return Verdict(ACCEPT)

    def _order_check(
        self, hist: _Hist, hk: tuple[SourceId, str], key: Key, cur: _S, inc: Incoming, verdict: Verdict
    ) -> Verdict | None:
        pred, pkey, succ = _neighbours(hist, key)
        if pred is not None:
            bad = _vs_pred(pred, cur)
            if bad is not None:
                return self._suspect_pred(hist, hk, key, cur, inc, pred, pkey, bad, verdict)
        if succ is not None:
            tol = 1e-6 * max(1.0, cur.turnover)
            if cur.volume > succ.volume or cur.turnover > succ.turnover + tol or cur.ts > succ.ts:
                return _reject(MD_REJECT_NON_MONOTONIC, {"next_volume": succ.volume, "next_ts": succ.ts}, verdict)
            if cur.volume == succ.volume and abs(cur.last - succ.last) > _PRICE_EPS:
                return _reject(MD_REJECT_PRICE_WITHOUT_TRADE, {"next_last": succ.last, "volume": cur.volume}, verdict)
        return None

    def _suspect_pred(
        self,
        hist: _Hist,
        hk: tuple[SourceId, str],
        key: Key,
        cur: _S,
        inc: Incoming,
        pred: _S,
        pkey: Key,
        bad: tuple[str, dict[str, Any]],
        verdict: Verdict,
    ) -> Verdict | None:
        """``cur`` contradicts its predecessor: evict the predecessor, hold ``cur`` or reject it."""
        reason, detail = bad
        i = bisect.bisect_left(hist.keys, pkey)
        pp = hist.states[hist.keys[i - 1]] if i > 0 else None
        if (pp is not None and _vs_pred(pp, cur) is not None) or inc.replay:
            return _reject(reason, detail, verdict)  # 与更早的状态也矛盾：新状态才是坏点
        src, iid = hk
        by = self._corroborated(src, iid, pred, cur)
        if by is not None:
            verdict.evicted.append(self._evict(hist, hk, pkey, pred, reason, by))
            nxt = _neighbours(hist, key)[0]
            if nxt is not None and _vs_pred(nxt, cur) is not None:
                return _reject(reason, detail, verdict)
            return None
        if pp is None or hk in self._spikes:
            # without an earlier state there is no 2-against-1 to wait for; one held state per source at a time
            return _reject(reason, detail, verdict)
        spike = Spike(inc, key, pred.last, 0.0, self._mono(), 0.0, kind=ORDER, reason=reason, suspect_key=pkey)
        self._spikes[hk] = spike
        return Verdict(QUARANTINE, reason, {**detail, "suspect_predecessor": str(pkey)}, verdict.released, verdict.dropped, verdict.evicted)

    def _corroborated(self, src: SourceId, iid: str, pred: _S, cur: _S) -> str | None:
        """Another trusted source contradicts ``pred`` (a state at or after it with a lower volume, or the same
        volume at ``cur``'s price). Turnover is not compared across sources (Sina only estimates it)."""
        for other in self._trusted_sources():
            if other == src:
                continue
            hist = self._hist.get((other, iid))
            if hist is None:
                continue
            for k in reversed(hist.keys):
                q = hist.states[k]
                if q.ts < pred.ts:
                    break
                if _contradicts_pred(pred, q, cur.last):
                    return str(other)
        return None

    def _evict(self, hist: _Hist, hk: tuple[SourceId, str], pkey: Key, pred: _S, reason: str, by: str) -> Evicted:
        i = bisect.bisect_left(hist.keys, pkey)
        if i < len(hist.keys) and hist.keys[i] == pkey:
            del hist.keys[i]
        hist.states.pop(pkey, None)
        self.evictions += 1
        return Evicted(hk[0], hk[1], hist.td, pkey, pred.volume, pred.last, pred.ts, pred.fp, reason, by)

    def _plausibility(
        self, hist: _Hist, hk: tuple[SourceId, str], key: Key, cur: _S, inc: Incoming, tick: Tick, tick_f: float, verdict: Verdict
    ) -> Verdict:
        """Volume plausibility (C1) and price spikes (H3) against the reference."""
        src, iid = hk
        td = hist.td
        pred, _, succ = _neighbours(hist, key)
        ref = self._ref.get(iid)
        if pred is not None:
            ref_last, ref_ts, ref_vol, ref_oi = pred.last, pred.ts, pred.volume, pred.oi
        elif ref is not None and ref.td == td:
            ref_last, ref_ts, ref_vol, ref_oi = ref.last, ref.ts, ref.volume, ref.oi
        else:
            self._insert(hist, key, cur)
            return verdict
        if ref_oi <= 0:
            ref_oi = tick.pre_open_interest or 0
        dt_s = max(abs(cur.ts - ref_ts) / NS_PER_S, 0.05)
        kind = None
        detail: dict[str, Any] = {}
        threshold = 0.0
        deviation = 0.0
        jump = cur.volume - ref_vol
        if jump > 0 and (pred is not None or cur.ts >= ref_ts):
            limit = self._jump_limit(iid, ref_oi, dt_s)
            if jump > limit:
                kind = VOLUME
                detail = {"increment": jump, "limit": round(limit), "dt_s": round(dt_s, 3), "ref_volume": ref_vol}
        if kind is None:
            threshold = self._threshold(tick, tick_f, ref_last, dt_s)
            deviation = abs(cur.last - ref_last)
            if deviation > threshold + _PRICE_EPS:
                kind = PRICE
                detail = {"ref": ref_last, "deviation": deviation, "threshold": threshold}
        if kind is None:
            self._insert(hist, key, cur)
            return verdict
        reason = MD_REJECT_VOLUME_JUMP if kind == VOLUME else MD_QUARANTINE_SPIKE
        spike = Spike(inc, key, ref_last, threshold, self._mono(), deviation, kind=kind, ref_volume=ref_vol, reason=reason)
        if succ is not None:
            # 已知后继状态（例如补洞的重放）：立即按后继判断确认或丢弃
            if self._supports(spike, succ):
                verdict.detail.update(detail, confirmed_by="successor")
                self._insert(hist, key, cur)
                return verdict
            return Verdict(DROP, reason, {**detail, "why": "successor"}, verdict.released, verdict.dropped, verdict.evicted)
        if inc.replay:
            # a replayed state is never LIVE: nothing to wait for; the next states judge it
            verdict.detail.update(detail, confirmed_by="replay")
            self._insert(hist, key, cur)
            return verdict
        by = self._confirmed_elsewhere(spike, src, iid, verdict)
        if by is not None:
            verdict.detail.update(detail, confirmed_by=by)
            self._insert(hist, key, cur)
            return verdict
        held = self._spikes.get(hk)
        if held is not None:
            if not _gt(key, held.key):
                # an older state arriving after a newer held one (reordered polls): not held twice
                return Verdict(DROP, reason, {**detail, "why": "older than a held state"}, verdict.released, verdict.dropped, verdict.evicted)
            del self._spikes[hk]
            verdict.dropped.append((held, "superseded"))
        self._spikes[hk] = spike
        return Verdict(QUARANTINE, reason, {**detail, "kind": kind}, verdict.released, verdict.dropped, verdict.evicted)

    def _confirmed_elsewhere(self, spike: Spike, src: SourceId, iid: str, verdict: Verdict) -> str | None:
        """A remembered dropped level (M3) or another trusted source's held state at the same level confirms ``spike``."""
        t = spike.tick
        now = self._mono()
        trusted = set(self._trusted_sources())
        mem_ns = self._p.spike_memory_ns
        for (msrc, miid), mems in list(self._memory.items()):
            if miid != iid or (msrc != src and msrc not in trusted):
                continue
            mems[:] = [m for m in mems if now - m.since_mono < mem_ns]
            for m in mems:
                if m.td != t.trading_day or m.kind != spike.kind or m.ts > t.exch_ts_ns:
                    continue
                if spike.kind == VOLUME:
                    ok = t.volume >= m.volume
                else:
                    near = max(spike.threshold, abs(m.last - m.ref_last) / 2)
                    ok = abs(t.last - m.last) < abs(t.last - m.ref_last) and abs(t.last - m.last) <= near
                if ok and not self._contradicted_since(src, iid, m, trusted):
                    return "remembered level" if msrc == src else f"remembered level of {msrc}"
        if src not in trusted:
            return None
        for (osrc, oiid), other in list(self._spikes.items()):
            if oiid != iid or osrc == src or osrc not in trusted or other.kind != spike.kind:
                continue
            ot = other.tick
            if ot.trading_day != t.trading_day:
                continue
            if spike.kind == VOLUME:
                first, second = (ot, t) if ot.exch_ts_ns <= t.exch_ts_ns else (t, ot)
                ok = second.volume >= first.volume
            else:
                ok = self._supports(other, _s_of(t)) and abs(t.last - ot.last) <= max(spike.threshold, other.threshold)
            if ok:
                # 两个可信源在同一水平互相确认
                del self._spikes[(osrc, oiid)]
                hist = self._hist.get((osrc, oiid))
                if hist is not None and hist.td == ot.trading_day:
                    self._confirm(hist, other)
                verdict.released.append((other, f"trusted source {src}"))
                return f"trusted source {osrc}"
        return None

    def _contradicted_since(self, src: SourceId, iid: str, m: _Mem, trusted: set[SourceId]) -> bool:
        """Another trusted source showed, at or after the remembered state, a state that stays with its reference."""
        for other in trusted:
            if other == src:
                continue
            hist = self._hist.get((other, iid))
            if hist is None or hist.td != m.td:
                continue
            for k in reversed(hist.keys):
                q = hist.states[k]
                if q.ts < m.ts:
                    break
                if m.kind == VOLUME:
                    if q.volume < m.volume:
                        return True
                elif abs(q.last - m.last) >= abs(q.last - m.ref_last):
                    return True
        return False

    def _remember(self, hk: tuple[SourceId, str], spike: Spike) -> None:
        t = spike.tick
        mems = self._memory.setdefault(hk, [])
        mems.append(_Mem(t.trading_day, spike.kind, t.last, spike.ref_last, t.volume, t.exch_ts_ns, self._mono()))
        if len(mems) > 8:
            del mems[: len(mems) - 8]

    def _confirm(self, hist: _Hist, spike: Spike) -> None:
        for inc, key in ((spike.inc, spike.key), *spike.followers):
            t = inc.tick
            if t is not None and key not in hist.states:
                self._insert(hist, key, _s_of(t))

    def _insert(self, hist: _Hist, key: Key, s: _S) -> None:
        if key in hist.states:
            return
        keys = hist.keys
        if not keys or _gt(key, keys[-1]):
            keys.append(key)
        else:
            bisect.insort(keys, key)
        hist.states[key] = s
        cap = self._p.source_history
        if len(keys) > cap + _HIST_SLACK:
            cut = len(keys) - cap
            for old in keys[:cut]:
                hist.states.pop(old, None)
            del keys[:cut]


def _s_of(t: Tick) -> _S:
    return _S(t.volume, t.turnover, t.exch_ts_ns, t.last, t.fingerprint, t.open_interest)


def _vs_pred(pred: _S, cur: _S) -> tuple[str, dict[str, Any]] | None:
    """Ordering / price-without-trade violation of ``cur`` against its predecessor."""
    tol = 1e-6 * max(1.0, cur.turnover)
    if cur.volume < pred.volume or cur.turnover < pred.turnover - tol or cur.ts < pred.ts:
        return MD_REJECT_NON_MONOTONIC, {"prev_volume": pred.volume, "prev_ts": pred.ts}
    if cur.volume == pred.volume and abs(cur.last - pred.last) > _PRICE_EPS:
        return MD_REJECT_PRICE_WITHOUT_TRADE, {"prev_last": pred.last, "volume": cur.volume}
    return None


def _contradicts_pred(pred: _S, q: _S, held_last: float) -> bool:
    """Another source's state ``q`` shows that ``pred`` was wrong (cumulative volume lower after it, or the
    same volume at the held state's price)."""
    if q.ts < pred.ts:
        return False
    if q.volume < pred.volume:
        return True
    return q.volume == pred.volume and abs(q.last - pred.last) > _PRICE_EPS and abs(q.last - held_last) <= _PRICE_EPS


def _reject(reason: str, detail: dict[str, Any], verdict: Verdict) -> Verdict:
    return Verdict(REJECT, reason, detail, verdict.released, verdict.dropped, verdict.evicted)


def _neighbours(hist: _Hist, key: Key) -> tuple[_S | None, Key, _S | None]:
    keys = hist.keys
    i = bisect.bisect_left(keys, key)
    pkey = keys[i - 1] if i > 0 else None
    pred = hist.states[pkey] if pkey is not None else None
    j = i + 1 if i < len(keys) and keys[i] == key else i
    succ = hist.states[keys[j]] if j < len(keys) else None
    return pred, pkey, succ


def _gt(a: Key, b: Key) -> bool:
    try:
        return bool(a > b)
    except TypeError:
        return False


def _on_grid(price: float, tick: float) -> bool:
    if tick <= 0:
        return True
    q = price / tick
    return abs(q - round(q)) <= 1e-9 * max(1.0, abs(q)) + 1e-7


__all__ = [
    "ACCEPT",
    "DROP",
    "HELD_DUPLICATE",
    "ORDER",
    "PRICE",
    "QUARANTINE",
    "REJECT",
    "SKIP",
    "VOLUME",
    "Evicted",
    "Spike",
    "Validator",
    "Verdict",
]
