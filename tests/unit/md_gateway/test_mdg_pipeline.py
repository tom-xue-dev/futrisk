"""Pipeline: validator + arbiter + trust together, as md-gateway runs them (01 §6.4 R4, §8.13).

Fail-over / backfill flow, bad ticks, future timestamps, cross-source conflicts with and
without a trusted side, raw-record completeness and the epoch change.
"""

from __future__ import annotations

import pytest

from futrisk.common.config.feeds import ArbitrationConfig
from futrisk.messages.codes import MD_REJECT_FUTURE_TS, MD_REJECT_INTEGRITY, MD_REJECT_PRICE_WITHOUT_TRADE
from futrisk.messages.enums import MdEventKind, SourceId, SourceState, TickMode
from futrisk.messages.md import Tick
from futrisk.services.md_gateway.arbiter import Arbiter
from futrisk.services.md_gateway.normalizer import Normalizer
from futrisk.services.md_gateway.params import MdParams
from futrisk.services.md_gateway.pipeline import MdPipeline
from futrisk.services.md_gateway.sessions import SessionIndex
from futrisk.services.md_gateway.trust import TrustManager
from futrisk.services.md_gateway.validator import Validator

from .helpers import MS, T0, Clock, FakeEffects, S, as_source, inc, tick

A, B = SourceId.FEED_A, SourceId.FEED_B


class Env:
    def __init__(self, sessions: SessionIndex, normalizer: Normalizer, **extra: object) -> None:
        self.clock = Clock()
        self.params = MdParams(arb=ArbitrationConfig(probation_healthy_s=1.0, min_dwell_s=0.5), **extra)  # type: ignore[arg-type]
        self.fx = FakeEffects()
        self.changes: list[object] = []
        self.trust = TrustManager(
            self.params, {A: 1, B: 2}, mono_ns=self.clock.monotonic_ns, now_ns=self.clock.now_ns, on_change=self.changes.append
        )
        self.validator = Validator(sessions, self.params, normalizer.known_meta)
        self.arbiter = Arbiter(self.params)
        self.pipe = MdPipeline(self.params, self.validator, self.arbiter, self.trust, self.fx, mono_ns=self.clock.monotonic_ns)

    def trust_both(self) -> None:
        self.trust.on_connected(A)
        self.trust.on_poll(B, True)
        self.evaluate()
        self.clock.advance(1100 * MS)
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

    def actions(self) -> list[object]:
        return [e["detail"]["action"] for e in self.events(MdEventKind.MD_INTEGRITY)]  # type: ignore[index]


@pytest.fixture
def env(sessions: SessionIndex, normalizer: Normalizer) -> Env:
    return Env(sessions, normalizer)


def seq_tick(i: int, vol: int, px: float = 3120.0, **kw: object) -> Tick:
    return tick(seq=i, volume=vol, last=px, ts=T0 + i * 500 * MS, turnover=vol * 31_200.0 + i, **kw)  # type: ignore[arg-type]


def keys(ticks: list[Tick]) -> list[tuple[str, int, int]]:
    return [(p.trading_day, p.volume, p.exch_ts_ns) for p in ticks]


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
    assert env.pipe.counts[TickMode.LIVE] == 8 and env.pipe.last_exch_ns[A] == T0 + 12 * 500 * MS


def test_bad_tick_price_without_trade_is_rejected_with_event(env: Env) -> None:
    env.trust_both()
    env.feed(seq_tick(1, 100))
    bad = seq_tick(2, 100, px=3125.0)
    assert env.feed(bad) == TickMode.REJECTED
    assert env.fx.recorded[-1][1:] == (TickMode.REJECTED, MD_REJECT_PRICE_WITHOUT_TRADE)
    assert env.fx.kinds() == [MdEventKind.BAD_TICK]
    ev = env.events(MdEventKind.BAD_TICK)[0]
    assert ev["source"] is A and ev["instrument_id"] == "rb2701" and ev["detail"]["reason"] == MD_REJECT_PRICE_WITHOUT_TRADE  # type: ignore[index]
    assert len(env.fx.published) == 1
    # the same reject again within the interval is counted, not reported
    assert env.feed(seq_tick(3, 100, px=3126.0)) == TickMode.REJECTED
    assert env.fx.kinds() == [MdEventKind.BAD_TICK]


def test_future_stamped_state_is_rejected_and_later_states_flow(env: Env) -> None:
    env.trust_both()
    env.feed(tick(seq=1, volume=1000, ts=T0))
    fut = tick(seq=2, volume=1001, ts=T0 + 100 * S, recv=T0 + 500 * MS, turnover=3120.0 * 1001 * 10)
    assert env.feed(fut) == TickMode.REJECTED
    assert env.fx.recorded[-1][1:] == (TickMode.REJECTED, MD_REJECT_FUTURE_TS)
    for i in range(3, 43):
        env.clock.advance(500 * MS)
        now = T0 + i * 500 * MS
        vol = 1001 + (i - 2)
        assert env.feed(tick(seq=i, volume=vol, ts=now, recv=now + 5 * MS, turnover=3120.0 * vol * 10)) == TickMode.LIVE
        env.evaluate()
    assert env.trust.state(A) is SourceState.TRUSTED and env.arbiter.current("rb2701")
    assert env.fx.kinds() == [MdEventKind.BAD_TICK]


def test_untrusted_newcomer_disagreeing_with_the_trusted_source_is_rejected_and_voted(env: Env) -> None:
    env.trust_a_only()
    env.trust.on_poll(B, True)
    env.evaluate()
    assert env.trust.state(B) is SourceState.PROBATION
    env.feed(tick(seq=1, volume=100, last=3120.0))
    assert env.feed(tick(source=B, volume=100, last=3125.0, bid_vol1=9)) == TickMode.REJECTED
    assert env.fx.recorded[-1][1:] == (TickMode.REJECTED, MD_REJECT_INTEGRITY)
    assert env.actions() == ["VOTE"] and env.fx.kinds().count(MdEventKind.BAD_TICK) == 0
    assert env.trust.sources[B].integrity_votes_total == 1 and env.trust.state(B) is SourceState.PROBATION
    assert env.feed(tick(source=B, volume=101, last=3126.0, ts=T0 + 500 * MS, bid_vol1=9)) == TickMode.BACKFILL  # nothing to compare
    # the trusted source's own state at that volume disagrees again: second vote -> excluded
    assert env.feed(tick(seq=2, volume=101, last=3121.0, ts=T0 + 600 * MS)) == TickMode.LIVE
    assert env.trust.state(B) is SourceState.EXCLUDED and env.trust.sources[B].integrity_votes_total == 2


def test_both_trusted_conflict_is_recorded_without_votes_and_nobody_is_excluded(env: Env) -> None:
    env.trust_both()
    t0 = tick(seq=1, volume=1000, ts=T0)
    env.feed(t0)
    env.feed(as_source(t0, B))
    bad = tick(seq=2, volume=1010, ts=T0 + 30 * S, last=3126.0, turnover=3120.0 * 1009 * 10 + 31260)
    assert env.feed(bad) == TickMode.LIVE
    good = tick(volume=1010, ts=T0 + 30 * S, last=3120.0, turnover=3120.0 * 1010 * 10, source=B)
    assert env.feed(good) == TickMode.BACKFILL  # nobody knows who is wrong: recorded, not published
    assert env.actions() == ["NO_VOTE"]
    assert env.trust.sources[A].integrity_votes_total == 0 and env.trust.sources[B].integrity_votes_total == 0
    nxt = tick(seq=3, volume=1011, ts=T0 + 30 * S + 500 * MS, last=3120.0, turnover=3120.0 * 1011 * 10)
    assert env.feed(nxt) == TickMode.LIVE
    assert [p.last for p in env.fx.published] == [3120.0, 3126.0, 3120.0]
    assert env.trust.state(A) is SourceState.TRUSTED and env.trust.state(B) is SourceState.TRUSTED


def test_every_received_state_is_recorded_exactly_once(env: Env) -> None:
    env.trust_both()
    states = [seq_tick(1, 100), seq_tick(2, 101, px=3250.0), seq_tick(3, 102, px=3121.0), seq_tick(4, 102, px=3122.0)]
    modes = [env.feed(t) for t in states]
    # a jump inside the limits is a market state (no spike filter); a price change without a trade is not
    assert modes == [TickMode.LIVE, TickMode.LIVE, TickMode.LIVE, TickMode.REJECTED]
    assert sorted(i.seq for i, _, _ in env.fx.recorded) == [1, 2, 3, 4]  # type: ignore[type-var]
    assert env.fx.recorded[-1][1:] == (TickMode.REJECTED, MD_REJECT_PRICE_WITHOUT_TRADE)
    assert [p.source_seq for p in env.fx.published] == [1, 2, 3]
    assert env.pipe.source_counts[A] == 4 and env.pipe.counts == {TickMode.LIVE: 3, TickMode.REJECTED: 1}


def test_epoch_change_forgets_the_ordering_references(env: Env) -> None:
    env.trust_a_only()
    env.feed(tick(seq=100, volume=1000, ts=T0))
    assert env.feed(tick(seq=101, volume=900, ts=T0 + 500 * MS, turnover=3120.0 * 900 * 10)) == TickMode.REJECTED
    env.pipe.forget_source(A)
    assert env.validator.latest(A, "rb2701") is None
    # the new epoch restarts its seqs: judged afresh, then BACKFILL because it is below the watermark
    assert env.feed(tick(seq=1, volume=900, ts=T0 + 600 * MS, turnover=3120.0 * 900 * 10)) == TickMode.BACKFILL


def test_one_bad_instrument_raises_the_reject_rate_of_its_source(env: Env) -> None:
    """Rejects are counted per source: a contract served broken by a source counts against that source."""
    env.trust_a_only()
    for i in range(1, 41):
        env.clock.advance(100 * MS)
        env.feed(tick(seq=2 * i, volume=100 + i, ts=T0 + i * 100 * MS))
        env.feed(tick(iid="hc2701", seq=2 * i + 1, volume=100 + i, ts=T0 + i * 100 * MS, last=4000.0, upper=3500.0, lower=3000.0))
    assert env.trust.sources[A].reject_rate() == pytest.approx(0.5)
    assert env.trust.state(A) is SourceState.EXCLUDED
    assert [c.reason for c in env.changes][-1] == "reject_rate"  # type: ignore[attr-defined]
