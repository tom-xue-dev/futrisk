"""Quality timers: STALE / UNKNOWN only in continuous sessions, MD_DOWN once per outage, MD_QUIET (01 §8.13)."""

from __future__ import annotations

from datetime import datetime

from futrisk.common.clock import BJ, bj_to_ns
from futrisk.common.config.feeds import ArbitrationConfig
from futrisk.messages.enums import MdEventKind, MdQuality
from futrisk.services.md_gateway.params import MdParams
from futrisk.services.md_gateway.quality import QualityMonitor
from futrisk.services.md_gateway.sessions import SessionIndex

from .helpers import MS, Clock

RB = ("night_2100_2300", "day_commodity")


def monitor(sessions: SessionIndex, clock: Clock, **params: object) -> QualityMonitor:
    p = MdParams(arb=ArbitrationConfig(stale_s=5, unknown_s=30, open_grace_s=30), startup_grace_s=1.0, **params)  # type: ignore[arg-type]
    return QualityMonitor(p, sessions, lambda iid: RB, mono_ns=clock.monotonic_ns, now_ns=clock.now_ns)


def run(q: QualityMonitor, clock: Clock, seconds: float, covered: bool, last_trade: int | None = None) -> list:
    out = []
    steps = int(seconds * 10)
    for _ in range(steps):
        clock.advance(100 * MS)
        out += q.evaluate(lambda iid: covered, lambda iid: last_trade)
    return out


def kinds(events: list) -> list[tuple[MdEventKind, str | None]]:
    return [(e.kind, e.to_state) for e in events]


def test_outage_stale_unknown_single_md_down_and_recovery(sessions: SessionIndex) -> None:
    clock = Clock()
    q = monitor(sessions, clock)
    q.ensure("rb2701", known=True)
    ev = run(q, clock, 2, covered=True, last_trade=None)
    assert q.quality("rb2701") is MdQuality.OK
    assert kinds(ev) == [(MdEventKind.QUALITY_CHANGE, "OK")]
    ev = run(q, clock, 4.9, covered=False)
    assert q.quality("rb2701") is MdQuality.OK and not ev
    ev = run(q, clock, 0.3, covered=False)
    assert kinds(ev) == [(MdEventKind.QUALITY_CHANGE, "STALE"), (MdEventKind.MD_DOWN, None)]
    ev = run(q, clock, 25, covered=False)
    assert kinds(ev) == [(MdEventKind.QUALITY_CHANGE, "UNKNOWN")]
    ev = run(q, clock, 5, covered=False)
    assert not ev  # exactly one MD_DOWN per outage
    ev = run(q, clock, 0.2, covered=True, last_trade=clock.t)
    assert kinds(ev) == [(MdEventKind.QUALITY_CHANGE, "OK"), (MdEventKind.MD_RECOVERED, None)]


def test_no_judging_outside_continuous_session(sessions: SessionIndex) -> None:
    clock = Clock(bj_to_ns(datetime(2026, 9, 28, 11, 45, tzinfo=BJ)))  # lunch break
    q = monitor(sessions, clock)
    q.ensure("rb2701", known=True)
    run(q, clock, 1, covered=True)
    ev = run(q, clock, 60, covered=False)
    assert not ev and q.quality("rb2701") is MdQuality.OK


def test_open_grace_after_each_open(sessions: SessionIndex) -> None:
    clock = Clock(bj_to_ns(datetime(2026, 9, 28, 10, 29, 55, tzinfo=BJ)))  # before the 10:30 reopen
    q = monitor(sessions, clock)
    q.ensure("rb2701", known=True)
    run(q, clock, 1, covered=True)
    ev = run(q, clock, 30, covered=False)  # until 10:30:26: inside the 30 s grace
    assert not ev
    ev = run(q, clock, 10, covered=False)
    assert (MdEventKind.QUALITY_CHANGE, "STALE") in kinds(ev)


def test_startup_is_unknown_without_events_and_unobserved_instruments_stay_silent(sessions: SessionIndex) -> None:
    clock = Clock()
    q = monitor(sessions, clock)
    q.ensure("rb2610")  # configured, never observed
    q.ensure("rb2701", known=True)
    assert q.quality("rb2701") is MdQuality.UNKNOWN
    ev = run(q, clock, 0.5, covered=False)
    assert not ev
    ev = run(q, clock, 6, covered=False)
    assert kinds(ev) == [(MdEventKind.MD_DOWN, None)]  # known instrument only, after the startup grace
    assert all(e.instrument_id == "rb2701" for e in ev)


def test_restored_open_outage_recovers_without_new_down(sessions: SessionIndex) -> None:
    clock = Clock()
    q = monitor(sessions, clock)
    q.restore_down({"rb2701": True})
    ev = run(q, clock, 10, covered=False)
    assert MdEventKind.MD_DOWN not in [e.kind for e in ev]
    ev = run(q, clock, 0.2, covered=True, last_trade=clock.t)
    assert (MdEventKind.MD_RECOVERED, None) in kinds(ev)


def test_quiet_when_covered_without_trades(sessions: SessionIndex) -> None:
    clock = Clock()
    q = monitor(sessions, clock)
    q.ensure("rb2701", known=True)
    t0 = clock.t
    run(q, clock, 1, covered=True, last_trade=t0)
    ev = run(q, clock, 5, covered=True, last_trade=t0)
    assert (MdEventKind.MD_QUIET, None) in kinds(ev)
    assert q.items["rb2701"].quiet and q.quality("rb2701") is MdQuality.OK
    run(q, clock, 0.2, covered=True, last_trade=clock.t)
    assert not q.items["rb2701"].quiet



def test_a_lagging_live_stream_is_stale_at_once_and_holds_count(sessions: SessionIndex) -> None:
    """H2: when the LIVE stream is behind the market by more than stale_s the uncovered time starts there."""
    clock = Clock()
    q = monitor(sessions, clock)
    q.ensure("rb2701", known=True)
    run(q, clock, 1, covered=True)
    assert q.quality("rb2701") is MdQuality.OK
    lag = {"rb2701": 7 * 1_000_000_000}
    clock.advance(100 * MS)
    ev = q.evaluate(lambda iid: False, lambda iid: None, lambda iid: lag[iid])
    assert q.quality("rb2701") is MdQuality.STALE
    assert ev[0].detail["reason"] == "LIVE stream not current"
    lag["rb2701"] = 31 * 1_000_000_000
    clock.advance(100 * MS)
    q.evaluate(lambda iid: False, lambda iid: None, lambda iid: lag[iid])
    assert q.quality("rb2701") is MdQuality.UNKNOWN
    clock.advance(100 * MS)
    q.evaluate(lambda iid: True, lambda iid: None, lambda iid: 0)
    assert q.quality("rb2701") is MdQuality.OK
