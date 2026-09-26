"""Pipeline: validator + arbiter + trust together, as md-gateway runs them (01 §6.4 R4, §8.13).

Covers the review scenarios: one outlier state never freezes LIVE nor excludes a healthy
source (C1), no priority votes while both sources are trusted (H1), a robust spike filter
(H3), sparse jumps (M3) and the fail-over / backfill flow.
"""

from __future__ import annotations

import msgspec
import pytest

from futrisk.common.config import FullConfig
from futrisk.common.config.feeds import ArbitrationConfig
from futrisk.messages.codes import (
    MD_QUARANTINE_SPIKE,
    MD_REJECT_FUTURE_TS,
    MD_REJECT_PRICE_WITHOUT_TRADE,
    MD_REJECT_VOLUME_JUMP,
)
from futrisk.messages.enums import MdEventKind, SourceId, SourceState, TickMode
from futrisk.messages.md import FLAG_WM_RESET, Tick
from futrisk.services.md_gateway.arbiter import Arbiter
from futrisk.services.md_gateway.bars import BarBuilder
from futrisk.services.md_gateway.normalizer import Normalizer
from futrisk.services.md_gateway.params import MdParams
from futrisk.services.md_gateway.pipeline import MdPipeline
from futrisk.services.md_gateway.sessions import SessionIndex
from futrisk.services.md_gateway.trust import TrustManager
from futrisk.services.md_gateway.validator import Validator

from .helpers import MS, T0, TD, Clock, FakeEffects, S, as_source, inc, tick

A, B = SourceId.FEED_A, SourceId.FEED_B


class Env:
    def __init__(self, cfg: FullConfig, sessions: SessionIndex, normalizer: Normalizer, **extra: object) -> None:
        self.clock = Clock()
        self.params = MdParams(arb=ArbitrationConfig(probation_healthy_s=1.0, min_dwell_s=0.5), **extra)  # type: ignore[arg-type]
        self.fx = FakeEffects()
        self.changes: list[object] = []
        self.trust = TrustManager(
            self.params, {A: 1, B: 2}, mono_ns=self.clock.monotonic_ns, now_ns=self.clock.now_ns, on_change=self.changes.append
        )
        self.validator = Validator(sessions, self.params, normalizer.known_meta, mono_ns=self.clock.monotonic_ns)
        self.arbiter = Arbiter(self.params)
        self.bars = BarBuilder(lambda a, b: None)
        self.pipe = MdPipeline(self.params, self.validator, self.arbiter, self.trust, self.bars, self.fx, mono_ns=self.clock.monotonic_ns)

    def trust_both(self) -> None:
        self.trust.on_connected(A)
        self.trust.on_poll(B, True)
        self.evaluate()
        self.clock.advance(1100 * MS)
        self.evaluate()  # A bootstraps
        for v in (1, 2, 3):
            self.trust.on_agreement(B, (TD, v))
        self.evaluate()
        assert self.trust.state(A) is SourceState.TRUSTED and self.trust.state(B) is SourceState.TRUSTED

    def trust_a_only(self) -> None:
        self.trust.on_connected(A)
        self.evaluate()
        self.clock.advance(1100 * MS)
        self.evaluate()
        assert self.trust.state(A) is SourceState.TRUSTED

    def evaluate(self) -> None:
        self.trust.evaluate(caught_up=lambda s: True, complete=lambda s: True)

    def feed(self, t: Tick, **kw: object) -> TickMode | None:
        return self.pipe.process(inc(t, **kw))  # type: ignore[arg-type]

    def events(self, kind: MdEventKind) -> list[dict[str, object]]:
        return [kw for k, kw in self.fx.events if k is kind]


@pytest.fixture
def env(default_config: FullConfig, sessions: SessionIndex, normalizer: Normalizer) -> Env:
    return Env(default_config, sessions, normalizer)


def seq_tick(i: int, vol: int, px: float = 3120.0, **kw: object) -> Tick:
    return tick(seq=i, volume=vol, last=px, ts=T0 + i * 500 * MS, turnover=vol * 31_200.0 + i, **kw)  # type: ignore[arg-type]


def keys(ticks: list[Tick]) -> list[tuple[str, int, int]]:
    return [(p.trading_day, p.volume, p.exch_ts_ns) for p in ticks]


def assert_monotonic_or_reset(ticks: list[Tick]) -> None:
    """02 §5 order_key strictly increasing, except on a tick flagged FLAG_WM_RESET (after a watermark roll-back)."""
    prev = None
    for t in ticks:
        k = (t.trading_day, t.volume, t.exch_ts_ns)
        assert prev is None or k > prev or t.flags & FLAG_WM_RESET, f"{k} after {prev}"
        prev = k


def test_outage_failover_backfill_never_published_and_no_duplicate_ticks(env: Env) -> None:
    env.trust_both()
    # normal flow: Feed A live, Feed B sees the same states (duplicates)
    for i in range(1, 6):
        t = seq_tick(i, 100 + i)
        env.feed(t)
        env.feed(as_source(t, B))
    assert [p.source for p in env.fx.published] == [A] * 5
    # Feed A drops; Feed B keeps polling the newest state (1 Hz) and takes over at once
    env.trust.on_disconnected(A, "closed")
    for i in (8, 10, 12):
        env.feed(as_source(seq_tick(i, 100 + i), B))
    assert [p.source for p in env.fx.published][-3:] == [B, B, B]
    # Feed A reconnects (PROBATION) and replays 6..12 as BACKFILL
    env.trust.on_connected(A)
    before = len(env.fx.published)
    for i in range(6, 13):
        mode = env.feed(seq_tick(i, 100 + i), replay=True)
        assert mode in (TickMode.BACKFILL, TickMode.DUPLICATE)
    assert len(env.fx.published) == before  # replayed states never reach md.tick
    published = keys(env.fx.published)
    assert published == sorted(set(published))  # strictly increasing, no duplicate order_key
    modes = env.fx.modes()
    assert modes.count(TickMode.BACKFILL) >= 4 and TickMode.DUPLICATE in modes


def test_bad_tick_price_without_trade_is_rejected_with_event(env: Env) -> None:
    env.trust_both()
    env.feed(seq_tick(1, 100))
    bad = seq_tick(2, 100, px=3125.0)
    assert env.feed(bad) == TickMode.REJECTED  # nothing earlier to weigh it against: rejected at once
    assert env.fx.recorded[-1][1:] == (TickMode.REJECTED, MD_REJECT_PRICE_WITHOUT_TRADE)
    assert env.fx.kinds() == [MdEventKind.BAD_TICK]
    assert len(env.fx.published) == 1


def test_spike_dropped_then_real_jump_confirmed_by_both_sources(env: Env) -> None:
    env.trust_both()
    env.feed(seq_tick(1, 100))
    assert env.feed(seq_tick(2, 101, px=3250.0)) is None  # quarantined
    env.feed(seq_tick(3, 102, px=3121.0))
    assert MdEventKind.QUARANTINE_DROPPED in env.fx.kinds()
    assert (TickMode.QUARANTINED, MD_QUARANTINE_SPIKE) in [(m, r) for _, m, r in env.fx.recorded]
    assert [p.last for p in env.fx.published] == [3120.0, 3121.0]
    # a real jump: A's next state supports it, but with Feed B trusted the confirmation waits for B (H1)
    assert env.feed(seq_tick(4, 103, px=3250.0)) is None
    assert env.feed(seq_tick(5, 104, px=3251.0)) is None
    assert [p.last for p in env.fx.published] == [3120.0, 3121.0]
    # Feed B shows the same level: both held states confirm each other, published in order
    env.feed(as_source(seq_tick(5, 104, px=3251.0), B))
    assert MdEventKind.QUARANTINE_CONFIRMED in env.fx.kinds()
    assert [p.last for p in env.fx.published][-2:] == [3250.0, 3251.0]
    assert keys(env.fx.published) == sorted(set(keys(env.fx.published)))


def test_self_confirmed_spike_is_released_when_the_other_source_stays_silent(env: Env) -> None:
    env.trust_both()
    env.feed(seq_tick(1, 100))
    env.feed(seq_tick(2, 101, px=3250.0))
    env.feed(seq_tick(3, 102, px=3251.0))
    assert len(env.fx.published) == 1
    env.clock.advance(2100 * MS)
    env.pipe.poll()
    assert [p.last for p in env.fx.published] == [3120.0, 3250.0, 3251.0]


def test_two_state_burst_contradicted_by_the_other_trusted_source_is_dropped(env: Env) -> None:
    """P5 / H1: a same-source confirmation does not survive a contradiction from the other trusted source."""
    env.trust_both()
    env.feed(seq_tick(1, 1000))
    b1 = tick(seq=2, volume=1001, ts=T0 + 500 * MS, last=3276.0, turnover=3120.0 * 1000 * 10 + 32760)
    b2 = tick(seq=3, volume=1002, ts=T0 + 1000 * MS, last=3275.0, turnover=3120.0 * 1000 * 10 + 32760 + 32750)
    assert env.feed(b1) is None and env.feed(b2) is None
    good = tick(source=B, volume=1002, ts=T0 + 1000 * MS, last=3120.0, turnover=3120.0 * 1002 * 10)
    assert env.feed(good) == TickMode.LIVE
    assert [p.last for p in env.fx.published] == [3120.0, 3120.0]
    assert env.trust.sources[B].integrity_votes_total == 0
    dropped = [m for _, m, _ in env.fx.recorded if m is TickMode.QUARANTINED]
    assert len(dropped) == 2


def test_volume_outlier_is_quarantined_and_never_freezes_live(env: Env) -> None:
    """C1 (P2): +100k lots is implausible against the trusted reference (OI of the previous state)."""
    env.trust_both()
    env.feed(tick(seq=1, volume=1000, ts=T0))
    bad = tick(seq=2, volume=101_000, ts=T0 + 500 * MS, turnover=3120.0 * 101_000 * 10)
    assert env.feed(bad) is None
    rejected = 0
    for i in range(3, 3 + 60):
        env.clock.advance(500 * MS)
        vol = 1000 + (i - 2) * 5
        t = tick(seq=i, volume=vol, ts=T0 + i * 500 * MS, turnover=3120.0 * vol * 10)
        rejected += env.feed(t) == TickMode.REJECTED
        if i % 2 == 0:
            env.feed(as_source(t, B))
        env.evaluate()
    assert rejected == 0
    assert len(env.fx.published) == 61  # no freeze
    assert env.trust.state(A) is SourceState.TRUSTED and env.trust.state(B) is SourceState.TRUSTED
    assert (TickMode.QUARANTINED, MD_REJECT_VOLUME_JUMP) in [(m, r) for _, m, r in env.fx.recorded]
    assert env.arbiter.state("rb2701").wm[1] == 1000 + 60 * 5


def test_future_stamped_state_is_quarantined_and_never_a_predecessor(env: Env) -> None:
    """C1 (P3): exch_ts > recv + future_skew -> quarantined; later genuine states flow."""
    env.trust_both()
    env.feed(tick(seq=1, volume=1000, ts=T0))
    fut = tick(seq=2, volume=1001, ts=T0 + 100 * S, recv=T0 + 500 * MS, turnover=3120.0 * 1001 * 10)
    assert env.feed(fut, recv=T0 + 500 * MS) == TickMode.QUARANTINED
    assert env.fx.recorded[-1][1:] == (TickMode.QUARANTINED, MD_REJECT_FUTURE_TS)
    for i in range(3, 43):
        env.clock.advance(500 * MS)
        now = T0 + i * 500 * MS
        vol = 1001 + (i - 2)
        assert env.feed(tick(seq=i, volume=vol, ts=now, recv=now + 5 * MS, turnover=3120.0 * vol * 10)) == TickMode.LIVE
        env.evaluate()
    assert env.trust.state(A) is SourceState.TRUSTED
    assert env.arbiter.current("rb2701")


def test_small_bad_tick_with_a_trade_is_evicted_without_cascade_or_votes(env: Env) -> None:
    """A +7-tick print with one lot (sim BAD_TICK with_volume) is LIVE, then evicted by the source's own next states."""
    env.trust_both()
    s1 = tick(seq=1, volume=500, ts=T0)
    env.feed(s1)
    env.feed(as_source(s1, B))
    bad = tick(seq=2, volume=501, ts=T0 + 499 * MS, last=3127.0, turnover=s1.turnover + 31_270.0)
    assert env.feed(bad) == TickMode.LIVE  # undetectable when it arrives
    t2 = tick(seq=3, volume=500, ts=T0 + 500 * MS, bid_vol1=9, turnover=s1.turnover)  # the true next state: no trade
    assert env.feed(t2) is None  # waits: its predecessor is the suspect
    t3 = tick(seq=4, volume=500, ts=T0 + 1000 * MS, bid_vol1=11, turnover=s1.turnover)
    assert env.feed(t3) == TickMode.LIVE
    actions = [e["detail"]["action"] for e in env.events(MdEventKind.MD_INTEGRITY)]  # type: ignore[index]
    assert "EVICT" in actions and "WM_ROLLBACK" in actions
    assert env.fx.published[-2].flags & FLAG_WM_RESET  # the first LIVE state after the roll-back
    assert_monotonic_or_reset(env.fx.published)
    assert env.trust.sources[A].integrity_votes_total == 0 and env.trust.sources[B].integrity_votes_total == 0
    assert env.trust.state(A) is SourceState.TRUSTED and env.trust.state(B) is SourceState.TRUSTED
    assert [p.bid_vol1 for p in env.fx.published][-2:] == [9, 11]
    assert not [m for _, m, _ in env.fx.recorded if m is TickMode.REJECTED]


def test_both_trusted_price_conflict_resolved_by_the_source_correcting_itself(env: Env) -> None:
    """H1 (P4): no priority vote; the source that contradicts itself at that volume is the loser, twice -> excluded."""
    env.trust_both()
    t0 = tick(seq=1, volume=1000, ts=T0)
    env.feed(t0)
    env.feed(as_source(t0, B))
    seq = 2
    for k in range(2):
        env.clock.advance(20 * S)
        base = T0 + (k + 1) * 30 * S
        v = 1000 + (k + 1) * 10
        bad = tick(seq=seq, volume=v, ts=base, last=3126.0, turnover=3120.0 * (v - 1) * 10 + 31260)
        seq += 1
        assert env.feed(bad) == TickMode.LIVE
        good = tick(volume=v, ts=base, last=3120.0, turnover=3120.0 * v * 10, source=B)
        assert env.feed(good) == TickMode.BACKFILL  # held: nobody knows yet who is wrong
        nxt = tick(seq=seq, volume=v, ts=base + 500 * MS, last=3120.0, turnover=3120.0 * v * 10)
        seq += 1
        env.feed(nxt)
        env.evaluate()
        assert env.fx.published[-1].last == 3120.0  # the corrected price is published at once
    assert env.trust.state(A) is SourceState.EXCLUDED and env.trust.state(B) is SourceState.TRUSTED
    assert env.trust.sources[B].integrity_votes_total == 0 and env.trust.sources[A].integrity_votes_total == 2
    assert_monotonic_or_reset(env.fx.published)


def test_robust_sigma_after_a_genuine_jump_and_after_a_snapshot(env: Env) -> None:
    """H3 (P7, P9): a confirmed jump barely moves σ; snapshots never feed it; the threshold is capped."""
    env.trust_a_only()
    vol, ts, turn = 1000, T0, 3120.0 * 1000 * 10
    seq = 1
    env.feed(tick(seq=seq, volume=vol, ts=ts, last=3120.0, turnover=turn))

    def nxt(px: float, dv: int = 3, snapshot: bool = False) -> TickMode | None:
        nonlocal seq, vol, ts, turn
        seq += 1
        vol += dv
        ts += 500 * MS
        turn += px * dv * 10
        env.clock.advance(500 * MS)
        return env.feed(tick(seq=None if snapshot else seq, volume=vol, ts=ts, last=px, turnover=turn), snapshot=snapshot)

    for _ in range(40):
        nxt(3120.0)
    sigma0 = env.validator.sigma("rb2701")
    snap = nxt(3270.0, snapshot=True)  # +4.8 % snapshot
    assert snap == TickMode.QUARANTINED and env.validator.sigma("rb2701") == pytest.approx(sigma0)
    assert env.bars.bars("rb2701", TD)[-1].high == 3120.0
    assert nxt(3200.0) is None and nxt(3201.0) == TickMode.LIVE  # genuine +2.6 % jump, confirmed
    assert env.validator.sigma("rb2701") < 1.5 * sigma0
    for _ in range(5):
        nxt(3201.0)
    assert nxt(3249.0, dv=1) is None  # a +1.5 % one-state print is still quarantined
    assert nxt(3201.0) == TickMode.LIVE
    assert 3249.0 not in [p.last for p in env.fx.published]


def test_sparse_contract_jump_is_confirmed_by_the_remembered_level(env: Env) -> None:
    """M3 (P6): states further apart than spike_confirm_ms still confirm a genuine move."""
    env.trust_a_only()
    env.feed(tick(seq=1, volume=1000, ts=T0))
    modes = []
    for i in range(2, 6):
        env.clock.advance(2500 * MS)
        env.pipe.poll()
        modes.append(env.feed(tick(seq=i, volume=1000 + i, ts=T0 + (i - 1) * 2500 * MS, last=3180.0 + (i % 2), turnover=3120.0 * 1000 * 10 + 31800 * i)))
    assert modes[0] is None and all(m == TickMode.LIVE for m in modes[1:])
    assert env.fx.kinds().count(MdEventKind.QUARANTINE_DROPPED) == 1


def test_epoch_change_drops_held_states_of_the_old_epoch(env: Env) -> None:
    """L6: held spikes of the old epoch are dropped (their seqs must not pin the new epoch's cursor)."""
    env.trust_a_only()
    env.feed(tick(seq=100, volume=1000, ts=T0))
    assert env.feed(tick(seq=101, volume=1001, ts=T0 + 500 * MS, last=3250.0, turnover=3120.0 * 1000 * 10 + 32500)) is None
    assert env.validator.min_held_seq(A) == 101
    env.pipe.forget_source(A)
    assert env.validator.min_held_seq(A) is None
    assert env.fx.recorded[-1][1] is TickMode.QUARANTINED


def test_one_bad_instrument_does_not_exclude_the_source(env: Env, normalizer: Normalizer) -> None:
    """C1 (4): rejects are counted per (source, instrument); the worst instrument alone never excludes a source."""
    env.trust_a_only()
    for i in range(1, 41):
        env.clock.advance(100 * MS)
        env.feed(tick(seq=2 * i, volume=100 + i, ts=T0 + i * 100 * MS))
        # hc2701 from this source is broken: every state beyond the limits
        env.feed(tick(iid="hc2701", seq=2 * i + 1, volume=100 + i, ts=T0 + i * 100 * MS, last=4000.0, upper=3500.0, lower=3000.0))
    assert env.trust.sources[A].reject_rate() == pytest.approx(0.5)
    assert env.trust.state(A) is SourceState.TRUSTED


def test_frozen_price_stream_quarantine_and_raw_records_are_consistent(env: Env) -> None:
    """Every received state is recorded exactly once, whatever its fate (raw completeness)."""
    env.trust_both()
    states = [seq_tick(1, 100), seq_tick(2, 101, px=3250.0), seq_tick(3, 102, px=3121.0), seq_tick(4, 102, px=3122.0)]
    for t in states:
        env.feed(t)
    env.clock.advance(3 * S)
    env.pipe.poll()
    seqs = sorted(inc.seq for inc, _, _ in env.fx.recorded if inc.seq is not None)  # type: ignore[type-var]
    assert seqs == [1, 2, 3, 4]


def test_released_states_keep_raw_order_key_monotonic_per_source(env: Env) -> None:
    env.trust_a_only()
    base = seq_tick(1, 100)
    env.feed(base)
    spike = msgspec.structs.replace(seq_tick(2, 101, px=3250.0))
    env.feed(spike)
    env.feed(seq_tick(3, 102, px=3251.0))
    published = env.fx.published
    assert [p.source_seq for p in published] == [1, 2, 3]
