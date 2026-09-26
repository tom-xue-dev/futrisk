"""Per-instrument market-data quality OK / STALE / UNKNOWN (01 §8.13 陈旧判定, §8.5, 02 §7.3 MdQuality).

- **coverage** (H2): an instrument is covered only while its LIVE stream is *current*
  (not held by an integrity conflict, and no connected source - trusted or not - has shown
  a validated state with new information stamped more than ``stale_s`` after the
  watermark) **and** at least one TRUSTED source is connected, timely (transport lag
  within ``live_max_latency_ms``) and delivering it: Feed A only while it has no open
  sequence gap older than ``stale_s``, a polled source only for records of its latest
  successful poll that were actually accepted;
- staleness is judged **only in continuous sessions at market time**, ``open_grace_s``
  after each open, and timed with the monotonic clock: without coverage for ``stale_s``
  the quality becomes STALE, after ``unknown_s`` UNKNOWN (``QUALITY_CHANGE`` events). When
  the LIVE stream is behind the market by more than that (``lag_ns``, exchange time), the
  uncovered time counts from there at once. Coverage restores OK at once (improvements
  are never delayed);
- ``MD_DOWN`` is raised **once per outage per instrument** when it first leaves OK (and
  ``MD_RECOVERED`` when it is OK again); the open outage is reloaded from ``md_events``
  after a restart, so a restart neither repeats nor loses it;
- at start-up every instrument is UNKNOWN (nothing is known yet) without an event; the
  sources need to connect and bootstrap, so outages are not announced during
  ``startup_grace``; instruments never observed (no data, no persisted watermark) get no
  events at all (their status stays UNKNOWN, which blocks opening in risk-engine);
- ``quiet`` / ``MD_QUIET``: covered but no volume increase for ``stale_s`` (e.g. a limit
  lock), at most one event per ``quiet_event_interval_s``.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any

from futrisk.messages.enums import MdEventKind, MdQuality
from futrisk.services.md_gateway.params import MdParams
from futrisk.services.md_gateway.sessions import SessionIndex


@dataclass
class QualityEvent:
    kind: MdEventKind
    instrument_id: str
    from_state: str | None = None
    to_state: str | None = None
    detail: dict[str, Any] = field(default_factory=dict)


@dataclass
class InstrumentQuality:
    instrument_id: str
    quality: MdQuality = MdQuality.UNKNOWN
    known: bool = False
    uncovered_ns: int = 0
    down: bool = False
    quiet: bool = False
    quiet_event_mono: int | None = None
    covered_since_mono: int | None = None
    since_ns: int = 0


class QualityMonitor:
    """Quality timers of all instruments (event-loop thread only)."""

    def __init__(
        self,
        params: MdParams,
        sessions: SessionIndex,
        sessions_of: Callable[[str], Sequence[str]],
        *,
        mono_ns: Callable[[], int],
        now_ns: Callable[[], int],
    ) -> None:
        self._p = params
        self._sessions = sessions
        self._sessions_of = sessions_of
        self._mono = mono_ns
        self._now = now_ns
        self._started_mono = mono_ns()
        self._last_eval: int | None = None
        self.items: dict[str, InstrumentQuality] = {}

    # ------------------------------------------------------------------ setup
    def ensure(self, instrument_id: str, *, known: bool = False) -> InstrumentQuality:
        q = self.items.get(instrument_id)
        if q is None:
            q = InstrumentQuality(instrument_id, since_ns=self._now())
            self.items[instrument_id] = q
        if known:
            q.known = True
        return q

    def restore_down(self, down: dict[str, bool]) -> None:
        """Open outages of today from ``md_events`` (last MD_DOWN without MD_RECOVERED)."""
        for iid, is_down in down.items():
            self.ensure(iid, known=True).down = is_down

    def quality(self, instrument_id: str) -> MdQuality:
        q = self.items.get(instrument_id)
        return q.quality if q is not None else MdQuality.UNKNOWN

    # ------------------------------------------------------------------ timer
    def evaluate(
        self,
        covered: Callable[[str], bool],
        last_trade_mono: Callable[[str], int | None],
        lag_ns: Callable[[str], int] | None = None,
    ) -> list[QualityEvent]:
        now = self._mono()
        now_ns = self._now()
        dt = 0 if self._last_eval is None else max(0, now - self._last_eval)
        self._last_eval = now
        startup_done = now - self._started_mono >= self._p.startup_grace_ns
        out: list[QualityEvent] = []
        for q in self.items.values():
            templates = self._sessions_of(q.instrument_id)
            judging = bool(templates) and self._sessions.judging(templates, now_ns, self._p.open_grace_ns)
            if covered(q.instrument_id):
                q.uncovered_ns = 0
                if q.covered_since_mono is None:
                    q.covered_since_mono = now
                if q.quality is not MdQuality.OK:
                    out.append(self._set(q, MdQuality.OK, now_ns, "covered"))
                    if q.down:
                        q.down = False
                        out.append(QualityEvent(MdEventKind.MD_RECOVERED, q.instrument_id, detail={"quality": "OK"}))
                self._quiet(q, judging, now, last_trade_mono(q.instrument_id), out)
                continue
            q.covered_since_mono = None
            if q.quiet:
                q.quiet = False
            if not judging:
                continue  # 01 §8.13：只在连续交易时段内判定（开盘后有宽限）
            q.uncovered_ns += dt
            lag = lag_ns(q.instrument_id) if lag_ns is not None else 0
            if lag > self._p.stale_ns:
                q.uncovered_ns = max(q.uncovered_ns, lag)  # LIVE 流落后于市场：按落后时长计
            target = q.quality
            if q.uncovered_ns >= self._p.unknown_ns:
                target = MdQuality.UNKNOWN
            elif q.uncovered_ns >= self._p.stale_ns and q.quality is MdQuality.OK:
                target = MdQuality.STALE
            if target is not q.quality:
                reason = "LIVE stream not current" if lag > self._p.stale_ns else "no trusted source"
                out.append(self._set(q, target, now_ns, reason, uncovered_s=round(q.uncovered_ns / 1e9, 3)))
            if (
                q.known
                and not q.down
                and startup_done
                and q.quality is not MdQuality.OK
                and q.uncovered_ns >= self._p.stale_ns
            ):
                q.down = True
                out.append(
                    QualityEvent(
                        MdEventKind.MD_DOWN,
                        q.instrument_id,
                        detail={"quality": str(q.quality), "uncovered_s": round(q.uncovered_ns / 1e9, 3)},
                    )
                )
        return out

    def _set(self, q: InstrumentQuality, to: MdQuality, now_ns: int, reason: str, **extra: Any) -> QualityEvent:
        before = q.quality
        q.quality = to
        q.since_ns = now_ns
        return QualityEvent(
            MdEventKind.QUALITY_CHANGE, q.instrument_id, str(before), str(to), {"reason": reason, **extra}
        )

    def _quiet(self, q: InstrumentQuality, judging: bool, now: int, last_trade: int | None, out: list[QualityEvent]) -> None:
        ref = max(last_trade or 0, q.covered_since_mono or 0)
        quiet = judging and now - ref >= self._p.stale_ns
        if quiet and not q.quiet:
            q.quiet = True
            interval = int(self._p.quiet_event_interval_s * 1e9)
            if q.known and (q.quiet_event_mono is None or now - q.quiet_event_mono >= interval):
                q.quiet_event_mono = now
                silent_s = None if last_trade is None else round((now - last_trade) / 1e9, 3)
                out.append(QualityEvent(MdEventKind.MD_QUIET, q.instrument_id, detail={"no_trade_s": silent_s}))
        elif not quiet:
            q.quiet = False


__all__ = ["InstrumentQuality", "QualityEvent", "QualityMonitor"]
