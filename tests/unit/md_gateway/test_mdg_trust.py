"""Source trust: bootstrap, probation, exclusion rules, dwell, flapping, manual control (01 §8.13)."""

from __future__ import annotations

from futrisk.common.config.feeds import ArbitrationConfig
from futrisk.messages.enums import SourceId, SourceState
from futrisk.services.md_gateway.params import MdParams
from futrisk.services.md_gateway.trust import TrustChange, TrustManager

from .helpers import MS, Clock, S

A, B = SourceId.FEED_A, SourceId.FEED_B
T, P, X = SourceState.TRUSTED, SourceState.PROBATION, SourceState.EXCLUDED


def make(clock: Clock, **arb: object) -> tuple[TrustManager, list[TrustChange]]:
    changes: list[TrustChange] = []
    params = MdParams(arb=ArbitrationConfig(**arb))  # type: ignore[arg-type]
    tm = TrustManager(params, {A: 1, B: 2}, mono_ns=clock.monotonic_ns, now_ns=clock.now_ns, on_change=changes.append)
    return tm, changes


def ev(tm: TrustManager, caught: bool = True, complete: bool = True) -> None:
    tm.evaluate(caught_up=lambda s: caught, complete=lambda s: complete)


def test_startup_bootstrap_promotes_highest_priority_after_probation() -> None:
    clock = Clock()
    tm, changes = make(clock)
    tm.on_connected(A)
    tm.on_poll(B, True, lag_ms=5.0)
    ev(tm)
    assert tm.state(A) is P and tm.state(B) is P  # initial EXCLUDED is exempt from the dwell
    clock.advance(9 * S)
    ev(tm)
    assert not tm.any_trusted()
    clock.advance(int(1.1 * S))
    ev(tm)
    assert tm.state(A) is T and tm.state(B) is P
    boot = [c for c in changes if c.to_state is T]
    assert boot[0].source is A and boot[0].detail["bootstrap"] is True
    # B needs 3 agreements with a trusted source
    ev(tm)
    assert tm.state(B) is P
    for v in (100, 101, 101, 102):
        tm.on_agreement(B, ("20260928", v))
    ev(tm)
    assert tm.state(B) is T and changes[-1].reason == "probation_passed"


def test_bootstrap_requires_completeness_and_catch_up() -> None:
    clock = Clock()
    tm, _ = make(clock)
    tm.on_connected(A)
    ev(tm)
    clock.advance(11 * S)
    ev(tm, complete=False)
    assert tm.state(A) is P
    ev(tm, caught=False)
    assert tm.state(A) is P
    ev(tm)
    assert tm.state(A) is T


def test_disconnect_excludes_at_once_and_reconnect_waits_for_dwell() -> None:
    clock = Clock()
    tm, changes = make(clock)
    tm.on_connected(A)
    ev(tm)
    clock.advance(11 * S)
    ev(tm)
    assert tm.state(A) is T
    tm.on_disconnected(A, "closed")
    assert tm.state(A) is X and changes[-1].reason == "disconnected"
    clock.advance(1 * S)
    tm.on_connected(A)
    ev(tm)
    assert tm.state(A) is X  # min dwell 5 s
    clock.advance(4 * S)
    ev(tm)
    assert tm.state(A) is P
    clock.advance(10 * S)
    ev(tm)
    assert tm.state(A) is T  # bootstrap again: nothing else trusted


def test_failed_polls_exclude_after_three() -> None:
    clock = Clock()
    tm, changes = make(clock)
    tm.on_poll(B, True)
    ev(tm)
    clock.advance(11 * S)
    ev(tm)
    assert tm.state(B) is T
    tm.on_poll(B, False, error="HTTP 503")
    tm.on_poll(B, False, error="HTTP 503")
    assert tm.state(B) is T
    tm.on_poll(B, False, error="HTTP 503")
    assert tm.state(B) is X and changes[-1].detail["failures"] == 3


def test_reject_rate_excludes_and_recovers_after_the_window() -> None:
    clock = Clock()
    tm, changes = make(clock)
    tm.on_connected(A)
    ev(tm)
    clock.advance(11 * S)
    ev(tm)
    for _ in range(40):
        tm.on_outcome(A, False)
    for _ in range(3):
        tm.on_outcome(A, True)  # 3/43 = 7% > 5%
    assert tm.state(A) is X and changes[-1].reason == "reject_rate"
    clock.advance(30 * S)
    ev(tm)
    assert tm.state(A) is X  # still above the rate inside the 1-minute window
    clock.advance(31 * S)
    ev(tm)
    assert tm.state(A) is P


def test_integrity_votes_exclude() -> None:
    clock = Clock()
    tm, changes = make(clock)
    tm.on_poll(B, True)
    ev(tm)
    clock.advance(11 * S)
    ev(tm)
    assert tm.state(B) is T
    tm.on_vote(B, {"instrument_id": "rb2701"})
    assert tm.state(B) is T
    tm.on_vote(B, {"instrument_id": "rb2701"})
    assert tm.state(B) is X and changes[-1].reason == "integrity_votes"


def test_flapping_alert_once_per_window() -> None:
    clock = Clock()
    tm, changes = make(clock, min_dwell_s=0.1, probation_healthy_s=0.1)
    for _ in range(4):
        tm.on_connected(A)
        clock.advance(200 * MS)
        ev(tm)  # -> PROBATION
        clock.advance(200 * MS)
        ev(tm)  # -> TRUSTED (bootstrap)
        tm.on_disconnected(A, "drop")
    flaps = [c for c in changes if c.flapping]
    assert len(flaps) == 1 and flaps[0].detail["switches"] > 4


def test_manual_exclude_pins_until_include() -> None:
    clock = Clock()
    tm, _ = make(clock)
    tm.on_connected(A)
    ev(tm)
    clock.advance(11 * S)
    ev(tm)
    change = tm.manual(A, "EXCLUDE", "ops", "maintenance")
    assert change.to_state is X and change.detail["manual"] is True
    clock.advance(60 * S)
    ev(tm)
    assert tm.state(A) is X
    again = tm.manual(A, "EXCLUDE", "ops", "maintenance")
    assert again.detail["changed"] is False
    inc = tm.manual(A, "INCLUDE", "ops", "done")
    assert inc.to_state is P


def test_reject_rate_counts_per_instrument_and_ignores_the_worst_one() -> None:
    """C1 (4): 1 bad contract of 13 (7.7 % of the states) never excludes a source; two bad ones do."""
    clock = Clock()
    tm, changes = make(clock)
    tm.on_connected(A)
    ev(tm)
    clock.advance(11 * S)
    ev(tm)
    iids = [f"i{k}" for k in range(13)]
    for _ in range(10):
        for iid in iids:
            tm.on_outcome(A, iid == "i0", iid)
    st = tm.sources[A]
    assert st.reject_rate() > 0.07 and st.worst_instrument()[0] == "i0"
    assert tm.state(A) is T
    for _ in range(10):
        tm.on_outcome(A, True, "i1")
    assert tm.state(A) is X and changes[-1].detail["worst_instrument"] in ("i0", "i1")


def test_a_single_instrument_source_is_judged_on_it() -> None:
    clock = Clock()
    tm, _ = make(clock)
    tm.on_connected(A)
    ev(tm)
    clock.advance(11 * S)
    ev(tm)
    for _ in range(19):
        tm.on_outcome(A, False, "rb2701")
    tm.on_outcome(A, True, "rb2701")
    tm.on_outcome(A, True, "rb2701")
    assert tm.state(A) is X


def test_manual_pin_restored_after_restart_blocks_the_reconnect() -> None:
    """M4: a persisted EXCLUDE pin is re-applied at start; the source stays EXCLUDED until INCLUDE."""
    clock = Clock()
    tm, _ = make(clock, min_dwell_s=0.5, probation_healthy_s=1.0)
    tm.restore_manual({B: {"action": "EXCLUDE", "actor": "ops", "reason": "vendor"}})
    tm.on_poll(B, True)
    for _ in range(5):
        clock.advance(1 * S)
        ev(tm)
    assert tm.state(B) is X and tm.sources[B].manual and tm.sources[B].exclude_reason == "manual_exclude"
    assert tm.preview_manual(B, "INCLUDE") == (X, P)
    tm.manual(B, "INCLUDE", "ops", "fixed")
    assert tm.state(B) is P


def test_message_rates_come_from_counters() -> None:
    clock = Clock()
    tm, _ = make(clock)
    tm.update_rates(clock.monotonic_ns())
    for _ in range(50):
        tm.on_message(A, mono=clock.monotonic_ns(), now=clock.now_ns())
    clock.advance(500 * MS)
    tm.update_rates(clock.monotonic_ns())
    assert tm.sources[A].msgs_per_s == 100.0 and tm.sources[A].last_msg_mono == clock.monotonic_ns() - 500 * MS
