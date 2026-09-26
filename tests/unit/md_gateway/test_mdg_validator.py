"""Validator: session/limit/grid checks, ordering and outlier eviction, volume plausibility, spike quarantine (01 §8.13)."""

from __future__ import annotations

from datetime import datetime

import pytest

from futrisk.common.clock import BJ, bj_to_ns
from futrisk.common.config import FullConfig
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
from futrisk.services.md_gateway.normalizer import Normalizer
from futrisk.services.md_gateway.params import MdParams
from futrisk.services.md_gateway.sessions import SessionIndex
from futrisk.services.md_gateway.validator import (
    ACCEPT,
    DROP,
    HELD_DUPLICATE,
    ORDER,
    QUARANTINE,
    REJECT,
    SKIP,
    VOLUME,
    Validator,
    Verdict,
)

from .helpers import MS, T0, Clock, S, inc, tick

A, B = SourceId.FEED_A, SourceId.FEED_B


@pytest.fixture
def clock() -> Clock:
    return Clock()


@pytest.fixture
def v(default_config: FullConfig, sessions: SessionIndex, normalizer: Normalizer, clock: Clock) -> Validator:
    return Validator(sessions, MdParams(), normalizer.known_meta, mono_ns=clock.monotonic_ns)


def trusted(_: SourceId) -> bool:
    return True


def run(v: Validator, t: object, *, others: tuple[SourceId, ...] = (), **kw: object) -> Verdict:
    return v.check(inc(t, **kw), trusted=trusted, trusted_sources=lambda: [A, *others])  # type: ignore[arg-type]


def check(v: Validator, t: object, **kw: object) -> str:
    verdict = run(v, t, **kw)
    return verdict.status if verdict.status in (ACCEPT, SKIP, QUARANTINE, DROP) else f"{verdict.status}:{verdict.reason}"


def test_stateless_checks(v: Validator) -> None:
    night = bj_to_ns(datetime(2026, 9, 28, 12, 0, tzinfo=BJ))  # lunch break
    assert check(v, tick(ts=night, seq=1)) == f"{REJECT}:{MD_REJECT_OUT_OF_SESSION}"
    assert check(v, tick(last=3300.0, seq=2)) == f"{REJECT}:{MD_REJECT_PRICE_LIMIT}"
    assert check(v, tick(iid="au2612", last=850.01, upper=969.0, lower=731.0, pre_settle=850.0, seq=3)) == f"{REJECT}:{MD_REJECT_OFF_TICK}"
    assert check(v, tick(iid="au2612", last=850.02, upper=969.0, lower=731.0, pre_settle=850.0, seq=4)) == ACCEPT


def test_future_stamped_state_is_quarantined_at_once(v: Validator) -> None:
    """C1: exch_ts more than future_skew_s after the receive time -> DROP, never a predecessor."""
    assert check(v, tick(seq=1, volume=100)) == ACCEPT
    fut = run(v, tick(seq=2, volume=101, ts=T0 + 10 * S, recv=T0 + 500 * MS), recv=T0 + 500 * MS)
    assert (fut.status, fut.reason) == (DROP, MD_REJECT_FUTURE_TS)
    assert check(v, tick(seq=3, volume=102, ts=T0 + 1000 * MS)) == ACCEPT  # judged against seq 1, not the future state
    assert check(v, tick(seq=4, volume=103, ts=T0 + 1500 * MS, recv=T0 + 1500 * MS - 900 * MS)) == ACCEPT  # skew allowance


def test_ordering_and_price_without_trade(v: Validator) -> None:
    assert check(v, tick(seq=1, volume=100, ts=T0)) == ACCEPT
    assert check(v, tick(seq=2, volume=100, ts=T0 + 500 * MS, last=3121.0)) == f"{REJECT}:{MD_REJECT_PRICE_WITHOUT_TRADE}"
    assert check(v, tick(seq=3, volume=90, ts=T0 + 600 * MS)) == f"{REJECT}:{MD_REJECT_NON_MONOTONIC}"
    assert check(v, tick(seq=4, volume=100, ts=T0 + 700 * MS, bid_vol1=9)) == ACCEPT  # book change, same price
    assert check(v, tick(seq=5, volume=104, ts=T0 + 1000 * MS, last=3121.0)) == ACCEPT


def test_outlier_predecessor_is_evicted_after_two_contradictions(v: Validator) -> None:
    """One accepted outlier must not poison the source's history (no NON_MONOTONIC cascade)."""
    assert check(v, tick(seq=1, volume=500)) == ACCEPT
    assert check(v, tick(seq=2, volume=501, ts=T0 + 499 * MS, last=3127.0, turnover=3120.0 * 5000 + 31270)) == ACCEPT
    held = run(v, tick(seq=3, volume=500, ts=T0 + 500 * MS, bid_vol1=9))
    assert held.status == QUARANTINE and v.pending_spikes()[0].kind == ORDER
    nxt = run(v, tick(seq=4, volume=500, ts=T0 + 1000 * MS, bid_vol1=11))
    assert nxt.status == ACCEPT
    assert [(e.key, e.by) for e in nxt.evicted] == [(2, "self")]
    assert [s.inc.seq for s, _ in nxt.released] == [3]
    assert v.latest(A, "rb2701") == ("20260928", 500, T0 + 1000 * MS, 3120.0)


def test_suspect_that_turns_out_right_rejects_the_held_state(v: Validator) -> None:
    assert check(v, tick(seq=1, volume=100)) == ACCEPT
    assert check(v, tick(seq=2, volume=150, ts=T0 + 500 * MS)) == ACCEPT
    assert check(v, tick(seq=3, volume=120, ts=T0 + 600 * MS)) == QUARANTINE  # lower volume: seq 2 is suspected
    verdict = run(v, tick(seq=4, volume=160, ts=T0 + 1000 * MS))  # consistent with seq 2: seq 3 was the bad one
    assert verdict.status == ACCEPT and not verdict.evicted
    assert [(s.inc.seq, why) for s, why in verdict.dropped] == [(3, "suspect predecessor confirmed")]


def test_outlier_predecessor_evicted_at_once_when_another_trusted_source_contradicts_it(v: Validator) -> None:
    assert check(v, tick(seq=1, volume=1000)) == ACCEPT
    assert check(v, tick(seq=2, volume=1010, ts=T0 + 30 * S, last=3126.0, turnover=3120.0 * 10_000 + 31_260 * 10)) == ACCEPT
    b = tick(source=B, volume=1010, ts=T0 + 30 * S, last=3120.0, turnover=3120.0 * 10_100)
    assert run(v, b, others=(B,)).status == ACCEPT
    fix = run(v, tick(seq=3, volume=1010, ts=T0 + 30 * S + 500 * MS, last=3120.0, turnover=3120.0 * 10_100), others=(B,))
    assert fix.status == ACCEPT and [(e.key, e.by) for e in fix.evicted] == [(2, "FEED_B")]


def test_volume_jump_is_quarantined_against_the_trusted_reference(v: Validator) -> None:
    """C1: the OI term comes from the previous accepted state (10k lots), not from the record (which claims 10M)."""
    assert check(v, tick(seq=1, volume=100, oi=10_000)) == ACCEPT
    jump = run(v, tick(seq=2, volume=100 + 50_000, oi=10_000_000, ts=T0 + 500 * MS))
    assert (jump.status, jump.reason) == (QUARANTINE, MD_REJECT_VOLUME_JUMP)
    assert v.pending_spikes()[0].kind == VOLUME
    back = run(v, tick(seq=3, volume=105, oi=10_000, ts=T0 + 1000 * MS))  # the source's own next state refutes it
    assert back.status == ACCEPT and [why for _, why in back.dropped] == ["refuted"]


def test_genuine_volume_burst_is_confirmed_by_the_next_state(v: Validator) -> None:
    assert check(v, tick(seq=1, volume=100, oi=10_000)) == ACCEPT
    assert check(v, tick(seq=2, volume=20_000, ts=T0 + 500 * MS)) == QUARANTINE
    nxt = run(v, tick(seq=3, volume=20_050, ts=T0 + 1000 * MS))
    assert nxt.status == ACCEPT and [s.inc.seq for s, _ in nxt.released] == [2]


def test_replay_into_a_hole_is_checked_against_both_neighbours(v: Validator) -> None:
    assert check(v, tick(seq=10, volume=100, ts=T0)) == ACCEPT
    assert check(v, tick(seq=14, volume=120, ts=T0 + 2000 * MS, last=3122.0)) == ACCEPT
    # seq 12 replayed later: must lie between its neighbours
    assert check(v, tick(seq=12, volume=110, ts=T0 + 1000 * MS, last=3121.0), replay=True) == ACCEPT
    assert check(v, tick(seq=13, volume=130, ts=T0 + 1500 * MS, last=3121.0), replay=True) == f"{REJECT}:{MD_REJECT_NON_MONOTONIC}"
    assert check(v, tick(seq=11, volume=100, ts=T0 + 500 * MS, last=3125.0), replay=True) == f"{REJECT}:{MD_REJECT_PRICE_WITHOUT_TRADE}"


def test_polled_record_out_of_session_now_is_skipped(v: Validator) -> None:
    lunch = bj_to_ns(datetime(2026, 9, 28, 12, 0, tzinfo=BJ))
    t = tick(source=B, ts=T0, recv=lunch)
    assert check(v, t, recv=lunch) == SKIP
    assert check(v, tick(source=B, ts=T0)) == ACCEPT


def test_spike_refuted_by_next_state_is_dropped(v: Validator) -> None:
    assert check(v, tick(seq=1, volume=100, last=3120.0)) == ACCEPT
    verdict = run(v, tick(seq=2, volume=101, last=3200.0, ts=T0 + 500 * MS))
    assert verdict.status == QUARANTINE and verdict.detail["deviation"] == 80.0
    back = run(v, tick(seq=3, volume=102, last=3121.0, ts=T0 + 1000 * MS))
    assert back.status == ACCEPT and len(back.dropped) == 1 and not back.released
    assert v.min_held_seq(A) is None


def test_spike_confirmed_by_next_state_is_released_first(v: Validator) -> None:
    assert check(v, tick(seq=1, volume=100, last=3120.0)) == ACCEPT
    assert check(v, tick(seq=2, volume=101, last=3200.0, ts=T0 + 500 * MS)) == QUARANTINE
    assert v.min_held_seq(A) == 2
    nxt = run(v, tick(seq=3, volume=103, last=3201.0, ts=T0 + 1000 * MS))
    assert nxt.status == ACCEPT and [s.inc.seq for s, _ in nxt.released] == [2]


def test_with_another_trusted_source_the_same_source_confirmation_waits(v: Validator, clock: Clock) -> None:
    """H1: the next state of the same source supports the spike, but the other trusted source has not spoken yet."""
    assert check(v, tick(seq=1, volume=100, last=3120.0)) == ACCEPT
    assert run(v, tick(seq=2, volume=101, last=3200.0, ts=T0 + 500 * MS), others=(B,)).status == QUARANTINE
    wait = run(v, tick(seq=3, volume=103, last=3201.0, ts=T0 + 1000 * MS), others=(B,))
    assert wait.status == QUARANTINE and wait.detail["waiting_for"] == "other trusted sources"
    assert v.held_count() == 2 and v.min_held_seq(A) == 2
    # the other source's state at the new level confirms both, in order
    confirmed, dropped, _ = v.cross_check(tick(source=B, volume=103, last=3201.0, ts=T0 + 1000 * MS), B)
    assert [s.inc.seq for s, _ in confirmed] == [2] and [f.seq for f, _ in confirmed[0][0].followers] == [3] and not dropped


def test_waiting_spike_is_dropped_when_the_other_trusted_source_contradicts(v: Validator) -> None:
    assert check(v, tick(seq=1, volume=100, last=3120.0)) == ACCEPT
    run(v, tick(seq=2, volume=101, last=3200.0, ts=T0 + 500 * MS), others=(B,))
    run(v, tick(seq=3, volume=103, last=3201.0, ts=T0 + 1000 * MS), others=(B,))
    confirmed, dropped, _ = v.cross_check(tick(source=B, volume=103, last=3120.0, ts=T0 + 1000 * MS), B)
    assert not confirmed and [(s.inc.seq, why) for s, why in dropped] == [(2, "contradicted by FEED_B")]
    assert v.held_count() == 0


def test_waiting_spike_is_released_at_the_timeout(v: Validator, clock: Clock) -> None:
    assert check(v, tick(seq=1, volume=100, last=3120.0)) == ACCEPT
    run(v, tick(seq=2, volume=101, last=3200.0, ts=T0 + 500 * MS), others=(B,))
    run(v, tick(seq=3, volume=103, last=3201.0, ts=T0 + 1000 * MS), others=(B,))
    clock.advance(2100 * MS)
    released, dropped = v.poll()
    assert [s.inc.seq for s, _ in released] == [2] and not dropped


def test_unconfirmed_spike_times_out(v: Validator, clock: Clock) -> None:
    assert check(v, tick(seq=1, volume=100, last=3120.0)) == ACCEPT
    assert check(v, tick(seq=2, volume=101, last=3200.0, ts=T0 + 500 * MS)) == QUARANTINE
    clock.advance(1000 * MS)
    assert v.poll() == ([], [])
    clock.advance(1100 * MS)
    released, dropped = v.poll()
    assert not released and [(s.inc.seq, why) for s, why in dropped] == [(2, "timeout")]


def test_remembered_level_confirms_a_sparse_jump(v: Validator, clock: Clock) -> None:
    """M3: the next state of a sparse contract arrives after the confirm window but at the dropped level."""
    assert check(v, tick(seq=1, volume=100, last=3120.0)) == ACCEPT
    assert check(v, tick(seq=2, volume=101, last=3180.0, ts=T0 + 2500 * MS)) == QUARANTINE
    clock.advance(2500 * MS)
    assert [why for _, why in v.poll()[1]] == ["timeout"]
    later = run(v, tick(seq=3, volume=103, last=3181.0, ts=T0 + 5000 * MS))
    assert later.status == ACCEPT and later.detail["confirmed_by"] == "remembered level"


def test_spike_with_known_successor_is_decided_at_once(v: Validator) -> None:
    assert check(v, tick(seq=1, volume=100, last=3120.0, turnover=3_120_000.0)) == ACCEPT
    assert check(v, tick(seq=3, volume=102, last=3121.0, turnover=3_183_210.0, ts=T0 + 1000 * MS)) == ACCEPT
    spike = tick(seq=2, volume=101, last=3200.0, turnover=3_152_000.0, ts=T0 + 500 * MS)
    verdict = run(v, spike, replay=True)
    assert (verdict.status, verdict.reason) == (DROP, MD_QUARANTINE_SPIKE)


def test_small_moves_are_not_spikes(v: Validator) -> None:
    last = 3120.0
    for i in range(1, 30):
        last += 2.0 if i % 2 else -1.0
        assert check(v, tick(seq=i, volume=100 + i, last=last, ts=T0 + i * 500 * MS)) == ACCEPT


def test_robust_sigma_and_the_band_cap(v: Validator) -> None:
    """H3: one jump barely moves σ (winsorized samples); even a huge σ never lets a quarter band through."""
    px = 3120.0
    for i in range(1, 41):
        v.on_accepted(tick(seq=i, volume=100 + i, last=px, ts=T0 + i * 500 * MS))
    s0 = v.sigma("rb2701")
    v.on_accepted(tick(seq=41, volume=141, last=3214.0, ts=T0 + 41 * 500 * MS))  # +3 % in 0.5 s
    assert v.sigma("rb2701") < 1.1 * s0
    v.on_accepted(tick(seq=42, volume=142, last=3214.0, ts=T0 + 42 * 500 * MS), update_stats=False)  # snapshot/released
    v._var["rb2705"] = 1.0  # noqa: SLF001 - an absurd σ
    assert check(v, tick(iid="rb2705", seq=1, volume=100, last=3120.0)) == ACCEPT
    # band 2964..3276 -> cap 78: a +80 jump is still quarantined
    assert check(v, tick(iid="rb2705", seq=2, volume=101, last=3200.0, ts=T0 + 500 * MS)) == QUARANTINE


def test_snapshots_are_checked_against_the_reference_without_waiting(v: Validator) -> None:
    v.on_accepted(tick(seq=1, volume=1000, last=3120.0))
    spike = run(v, tick(volume=1001, last=3270.0, ts=T0 + 400 * MS), snapshot=True)
    assert (spike.status, spike.reason) == (DROP, MD_QUARANTINE_SPIKE)
    jump = run(v, tick(volume=500_000, last=3120.0, ts=T0 + 400 * MS), snapshot=True)
    assert (jump.status, jump.reason) == (DROP, MD_REJECT_VOLUME_JUMP)
    assert run(v, tick(volume=1003, last=3121.0, ts=T0 + 400 * MS), snapshot=True).status == ACCEPT


def test_polled_source_serving_the_held_spike_again_keeps_it_held(v: Validator) -> None:
    assert check(v, tick(source=B, volume=100, last=3120.0)) == ACCEPT
    spike = tick(source=B, volume=101, last=3200.0, ts=T0 + 1000 * MS)
    assert check(v, spike) == QUARANTINE
    assert run(v, spike).status == HELD_DUPLICATE  # the next poll re-serves it
    assert len(v.pending_spikes()) == 1


def test_forget_source_returns_its_held_states(v: Validator) -> None:
    """L6: after an epoch change the old seqs and their held states go away."""
    assert check(v, tick(seq=5, volume=100, last=3120.0)) == ACCEPT
    assert check(v, tick(seq=6, volume=101, last=3200.0, ts=T0 + 500 * MS)) == QUARANTINE
    dropped = v.forget_source(A)
    assert [(s.inc.seq, why) for s, why in dropped] == [(6, "epoch change")]
    assert v.min_held_seq(A) is None and v.latest(A, "rb2701") is None
