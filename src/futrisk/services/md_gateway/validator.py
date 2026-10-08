"""Validator: per-source bad-tick defence (01 §8.13 坏点防线, 02 §11.3).

In order: **skip** (not validated, not recorded, not counted) polled records and subscribe
snapshots of contracts outside their session now or still carrying a previous trading day;
``MD_REJECT_OUT_OF_SESSION`` (a short grace after each window end keeps closing snapshots);
``MD_REJECT_PRICE_LIMIT`` / ``MD_REJECT_OFF_TICK``; ``MD_REJECT_FUTURE_TS`` (stamped more than
``future_skew_s`` after the receive time); then, for live states only (replays and snapshots
never become LIVE), ordering against the **same source's** newest accepted state of the day:
``MD_REJECT_NON_MONOTONIC`` (volume, turnover or time going backwards) and
``MD_REJECT_PRICE_WITHOUT_TRADE`` (the last price only changes with a volume increase).
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from futrisk.messages.codes import (
    MD_REJECT_FUTURE_TS,
    MD_REJECT_NON_MONOTONIC,
    MD_REJECT_OFF_TICK,
    MD_REJECT_OUT_OF_SESSION,
    MD_REJECT_PRICE_LIMIT,
    MD_REJECT_PRICE_WITHOUT_TRADE,
)
from futrisk.messages.enums import SourceId
from futrisk.services.md_gateway.model import Incoming
from futrisk.services.md_gateway.normalizer import InstrumentMeta
from futrisk.services.md_gateway.params import MdParams
from futrisk.services.md_gateway.sessions import SessionIndex

ACCEPT = "ACCEPT"
REJECT = "REJECT"
SKIP = "SKIP"

POLLED = frozenset({SourceId.FEED_B, SourceId.SINA})
_PRICE_EPS = 1e-6


@dataclass(slots=True)
class _Last:
    """The source's newest accepted live state of one instrument (ordering reference)."""

    td: str
    seq: int | None
    ts: int
    volume: int
    turnover: float
    last: float


@dataclass
class Verdict:
    """Validation result: ``ACCEPT``, ``REJECT`` (with the 02 §11.3 reason) or ``SKIP``."""

    status: str
    reason: str | None = None
    detail: dict[str, Any] = field(default_factory=dict)


class Validator:
    """Stateless checks plus one ordering reference per (source, instrument); event-loop thread only."""

    def __init__(
        self,
        sessions: SessionIndex,
        params: MdParams,
        meta_of: Callable[[str], InstrumentMeta | None],
        *,
        mono_ns: Callable[[], int] | None = None,
    ) -> None:
        self._sessions = sessions
        self._p = params
        self._meta_of = meta_of
        self._last: dict[tuple[SourceId, str], _Last] = {}
        self._tick_f: dict[str, float] = {}

    def latest(self, source: SourceId, instrument_id: str) -> tuple[str, int, int, float] | None:
        """``(trading_day, volume, ts, last)`` of the source's newest accepted state (diagnostics, tests)."""
        s = self._last.get((source, instrument_id))
        return None if s is None else (s.td, s.volume, s.ts, s.last)

    def check(self, inc: Incoming) -> Verdict:
        tick = inc.tick
        assert tick is not None
        iid = tick.instrument_id
        meta = self._meta_of(iid)
        if meta is None:  # the normalizer only builds ticks for known contracts
            return Verdict(REJECT, MD_REJECT_OUT_OF_SESSION, {"why": "unknown instrument"})
        p = self._p
        sessions = self._sessions
        td = tick.trading_day
        src = inc.source
        polled = inc.snapshot or src in POLLED
        if polled:
            now_td = sessions.open_now(meta.sessions, inc.recv_ns, p.poll_skip_grace_ns)
            if now_td is None or now_td != td:
                return Verdict(SKIP)
        ts = tick.exch_ts_ns
        if not sessions.in_session(meta.sessions, td, ts, p.close_grace_ns):
            if polled and ts <= inc.recv_ns:
                # a snapshot still carrying a time from before the open window: the contract has not updated in
                # this session yet (e.g. CFFEX during the 09:25 opening auction) - stale, not bad data
                return Verdict(SKIP)
            return Verdict(REJECT, MD_REJECT_OUT_OF_SESSION)
        last = tick.last
        if last < tick.lower_limit - _PRICE_EPS or last > tick.upper_limit + _PRICE_EPS:
            return Verdict(REJECT, MD_REJECT_PRICE_LIMIT, {"last": last, "upper": tick.upper_limit, "lower": tick.lower_limit})
        tick_f = self._tick_size(meta)
        if not _on_grid(last, tick_f):
            return Verdict(REJECT, MD_REJECT_OFF_TICK, {"last": last, "tick": tick_f})
        ahead = ts - inc.recv_ns
        if ahead > p.future_skew_ns:
            # 02 §11.3：交易所时间比收到时刻还晚（超过允许的时钟偏差）：不能成为 LIVE，也不能成为前驱
            return Verdict(REJECT, MD_REJECT_FUTURE_TS, {"ahead_ms": round(ahead / 1e6, 1)})
        if not inc.live_eligible:
            return Verdict(ACCEPT)
        prev = self._last.get((src, iid))
        if prev is None or prev.td != td or not _newer(inc.seq, ts, prev):
            return Verdict(ACCEPT)
        tol = 1e-6 * max(1.0, tick.turnover)
        if tick.volume < prev.volume or tick.turnover < prev.turnover - tol or ts < prev.ts:
            return Verdict(REJECT, MD_REJECT_NON_MONOTONIC, {"prev_volume": prev.volume, "prev_ts": prev.ts})
        if tick.volume == prev.volume and abs(last - prev.last) > _PRICE_EPS:
            # 01 §8.13：价格只能随成交变动
            return Verdict(REJECT, MD_REJECT_PRICE_WITHOUT_TRADE, {"prev_last": prev.last, "volume": tick.volume})
        return Verdict(ACCEPT)

    def on_accepted(self, inc: Incoming) -> None:
        """An accepted (LIVE / BACKFILL) live state becomes its source's ordering reference."""
        tick = inc.tick
        if tick is None or not inc.live_eligible:
            return
        key = (inc.source, tick.instrument_id)
        prev = self._last.get(key)
        if prev is None or tick.trading_day > prev.td or (tick.trading_day == prev.td and _newer(inc.seq, tick.exch_ts_ns, prev)):
            self._last[key] = _Last(tick.trading_day, inc.seq, tick.exch_ts_ns, tick.volume, tick.turnover, tick.last)

    def forget_source(self, source: SourceId) -> None:
        """Drop the ordering references of a source (after an epoch change its seqs restart)."""
        for key in [k for k in self._last if k[0] == source]:
            del self._last[key]

    def _tick_size(self, meta: InstrumentMeta) -> float:
        f = self._tick_f.get(meta.instrument_id)
        if f is None:
            f = float(meta.tick_size)
            self._tick_f[meta.instrument_id] = f
        return f


def _newer(seq: int | None, ts: int, prev: _Last) -> bool:
    """Is a state (Feed A: by seq, polled sources: by exchange time) at or after the reference?"""
    if seq is not None and prev.seq is not None:
        return seq > prev.seq
    return ts >= prev.ts


def _on_grid(price: float, tick: float) -> bool:
    if tick <= 0:
        return True
    q = price / tick
    return abs(q - round(q)) <= 1e-9 * max(1.0, abs(q)) + 1e-7


__all__ = ["ACCEPT", "REJECT", "SKIP", "Validator", "Verdict"]
