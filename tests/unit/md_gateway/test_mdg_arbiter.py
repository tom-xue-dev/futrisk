"""Arbiter: watermark, dedupe, cross-check holds, LIVE conditions, fail-over, watermark sanity (01 §8.13)."""

from __future__ import annotations

import msgspec

from futrisk.common.config.feeds import ArbitrationConfig
from futrisk.marketdata.fingerprint import tick_fingerprint
from futrisk.messages.codes import MD_REJECT_INTEGRITY
from futrisk.messages.enums import LimitLock, SourceId, SourceState, TickMode
from futrisk.messages.md import FLAG_COARSE_TS, FLAG_LIMIT_UP_LOCKED
from futrisk.services.md_gateway.arbiter import Arbiter
from futrisk.services.md_gateway.params import MdParams
from futrisk.services.md_gateway.validator import Evicted

from .helpers import MS, T0, TD, S, as_source, inc, tick

A, B, SINA = SourceId.FEED_A, SourceId.FEED_B, SourceId.SINA


class States:
    def __init__(self, **states: SourceState) -> None:
        self.s = {SourceId[k]: v for k, v in states.items()}

    def __call__(self, src: SourceId) -> SourceState:
        return self.s.get(src, SourceState.EXCLUDED)


TRUSTED, PROBATION, EXCLUDED = SourceState.TRUSTED, SourceState.PROBATION, SourceState.EXCLUDED


def fp(t):  # noqa: ANN001, ANN201
    return msgspec.structs.replace(t, fingerprint=tick_fingerprint(t))


def actions(d) -> list[str]:  # noqa: ANN001
    return [e["action"] for e in d.events]


def test_live_only_from_trusted_non_replay_with_increasing_order_key() -> None:
    arb = Arbiter(MdParams())
    st = States(FEED_A=TRUSTED, FEED_B=PROBATION)
    t1 = tick(seq=1, volume=100)
    assert arb.classify(inc(t1), st, 0).mode == TickMode.LIVE
    t2 = tick(seq=2, volume=101, ts=T0 + 500 * MS)
    assert arb.classify(inc(t2, replay=True), st, 0).mode == TickMode.BACKFILL
    t3 = tick(seq=3, volume=102, ts=T0 + 1000 * MS)
    assert arb.classify(inc(t3, snapshot=True), st, 0).mode == TickMode.BACKFILL
    tb = tick(source=B, volume=103, ts=T0 + 1500 * MS)
    assert arb.classify(inc(tb), st, 0).mode == TickMode.BACKFILL  # probation source
    t4 = tick(seq=4, volume=104, ts=T0 + 2000 * MS)
    assert arb.classify(inc(t4), st, 0).mode == TickMode.LIVE
    # older than the watermark: backfill even from a trusted live source
    t5 = tick(seq=5, volume=102, ts=T0 + 1200 * MS, bid_vol1=99)
    assert arb.classify(inc(t5), st, 0).mode == TickMode.BACKFILL
    assert arb.state("rb2701").wm == (TD, 104, T0 + 2000 * MS)


def test_same_volume_later_timestamp_is_live_but_equal_key_is_not() -> None:
    arb = Arbiter(MdParams())
    st = States(FEED_A=TRUSTED)
    assert arb.classify(inc(tick(seq=1, volume=100)), st, 0).mode == TickMode.LIVE
    book = tick(seq=2, volume=100, ts=T0 + 500 * MS, bid_vol1=11)  # book change only
    assert arb.classify(inc(book), st, 0).mode == TickMode.LIVE
    same_key = tick(seq=3, volume=100, ts=T0 + 500 * MS, bid_vol1=12)
    assert arb.classify(inc(same_key), st, 0).mode == TickMode.BACKFILL


def test_fingerprint_dedupe_across_sources() -> None:
    arb = Arbiter(MdParams())
    st = States(FEED_A=TRUSTED, FEED_B=TRUSTED)
    t = tick(seq=1, volume=100)
    assert arb.classify(inc(t), st, 0).mode == TickMode.LIVE
    assert arb.classify(inc(as_source(t, B)), st, 0).mode == TickMode.DUPLICATE
    assert arb.classify(inc(t), st, 0).mode == TickMode.DUPLICATE
    # a copy of the last LIVE state carrying a later timestamp (a polled source re-stamping it) is not published again
    later_copy = msgspec.structs.replace(as_source(t, B), exch_ts_ns=T0 + 700 * MS)
    assert arb.classify(inc(later_copy), st, 0).mode == TickMode.DUPLICATE


def test_quote_reversion_and_lock_unlock_lock_are_published_again() -> None:
    """M1 (P1): A -> B -> A at one volume is three market states; only the last LIVE fingerprint blocks LIVE."""
    arb = Arbiter(MdParams())
    st = States(FEED_A=TRUSTED)
    s1 = tick(seq=1, volume=100, ts=T0, bid_vol1=5)
    s2 = tick(seq=2, volume=100, ts=T0 + 500 * MS, bid_vol1=6)
    s3 = tick(seq=3, volume=100, ts=T0 + 1000 * MS, bid_vol1=5)
    assert s3.fingerprint == s1.fingerprint
    assert [arb.classify(inc(t), st, 0).mode for t in (s1, s2, s3)] == [TickMode.LIVE] * 3
    # an old copy of s1 (earlier timestamp than the s3 already seen) stays a duplicate
    assert arb.classify(inc(as_source(s1, B)), States(FEED_A=TRUSTED, FEED_B=TRUSTED), 0).mode == TickMode.DUPLICATE

    arb = Arbiter(MdParams())
    up = 3276.0
    l1 = fp(msgspec.structs.replace(tick(seq=1, volume=500, last=up, bid1=up, bid_vol1=900, ask_vol1=0), ask1=None, flags=FLAG_LIMIT_UP_LOCKED))
    l2 = fp(msgspec.structs.replace(l1, source_seq=2, exch_ts_ns=T0 + 500 * MS, ask1=up, ask_vol1=3, flags=0))
    l3 = fp(msgspec.structs.replace(l1, source_seq=3, exch_ts_ns=T0 + 1000 * MS))
    assert [arb.classify(inc(t), st, 0).mode for t in (l1, l2, l3)] == [TickMode.LIVE] * 3
    assert arb.state("rb2701").limit_lock is LimitLock.UP


def test_failover_is_instantaneous_and_newest_trusted_source_advances_wm() -> None:
    arb = Arbiter(MdParams())
    st = States(FEED_A=TRUSTED, FEED_B=TRUSTED)
    assert arb.classify(inc(tick(seq=1, volume=100)), st, 0).mode == TickMode.LIVE
    st.s[A] = EXCLUDED  # Feed A disconnected
    tb = tick(source=B, volume=120, ts=T0 + 1000 * MS)
    d = arb.classify(inc(tb), st, 0)
    assert d.mode == TickMode.LIVE and arb.state("rb2701").wm_source == B
    # Feed A's replay of the gap is backfill only
    st.s[A] = PROBATION
    for i, vol in enumerate((105, 110, 115), start=2):
        r = tick(seq=i, volume=vol, ts=T0 + (200 + 200 * i) * MS)
        assert arb.classify(inc(r, replay=True), st, 0).mode == TickMode.BACKFILL


def test_latency_rule_is_two_sided() -> None:
    arb = Arbiter(MdParams())
    st = States(FEED_A=TRUSTED)
    late = tick(seq=1, volume=100, recv=T0 + 3001 * MS)
    assert arb.classify(inc(late), st, 0).mode == TickMode.BACKFILL
    ok = tick(seq=2, volume=101, ts=T0 + 10 * MS, recv=T0 + 3000 * MS)
    assert arb.classify(inc(ok), st, 0).mode == TickMode.LIVE
    # C1: stamped more than future_skew_s after its receive time -> never LIVE
    future = tick(seq=3, volume=102, ts=T0 + 5 * S, recv=T0 + 3100 * MS)
    assert arb.classify(inc(future), st, 0).mode == TickMode.BACKFILL
    assert arb.state("rb2701").wm == (TD, 101, T0 + 10 * MS)


def test_watermark_reload_with_fingerprint_prevents_republishing_after_restart() -> None:
    """L1: the persisted watermark carries the last LIVE fingerprint."""
    s1 = tick(seq=1, volume=100)
    arb = Arbiter(MdParams())
    arb.load_watermarks({"rb2701": ((TD, 100, T0), s1.fingerprint)})
    st = States(FEED_A=TRUSTED, FEED_B=TRUSTED)
    assert arb.classify(inc(s1), st, 0).mode == TickMode.BACKFILL
    # Feed B serves the same state again with a later timestamp after the restart: not published twice
    copy = msgspec.structs.replace(as_source(s1, B), exch_ts_ns=T0 + 800 * MS)
    assert arb.classify(inc(copy), st, 0).mode != TickMode.LIVE
    s2 = tick(seq=2, volume=101, ts=T0 + 500 * MS)
    assert arb.classify(inc(s2), st, 0).mode == TickMode.LIVE
    assert arb.dirty_watermarks() == [("rb2701", (TD, 101, T0 + 500 * MS), s2.fingerprint)]
    assert arb.dirty_watermarks() == []
    # the old one-element format still loads
    arb2 = Arbiter(MdParams())
    arb2.load_watermarks({"rb2701": (TD, 100, T0)})
    assert arb2.state("rb2701").wm == (TD, 100, T0) and arb2.state("rb2701").wm_fp is None


def test_new_trading_day_is_live() -> None:
    arb = Arbiter(MdParams())
    arb.load_watermarks({"rb2701": (("20260925", 900_000, T0 - 3 * 86_400 * S), None)})
    st = States(FEED_A=TRUSTED)
    assert arb.classify(inc(tick(seq=1, volume=10)), st, 0).mode == TickMode.LIVE


def test_cross_check_rejects_untrusted_newcomer_with_integrity_reason_and_counts_agreements() -> None:
    arb = Arbiter(MdParams())
    st = States(FEED_A=TRUSTED, FEED_B=PROBATION)
    t = tick(seq=1, volume=100, last=3120.0)
    assert arb.classify(inc(t), st, 0).mode == TickMode.LIVE
    agree = tick(source=B, volume=100, last=3120.0, bid_vol1=8)  # same volume, same price, other book
    d = arb.classify(inc(agree), st, 0)
    assert d.agreements == [B] and not d.votes and d.mode == TickMode.BACKFILL
    odd = tick(source=B, volume=100, last=3125.0, bid_vol1=9)
    d2 = arb.classify(inc(odd), st, 0)
    assert d2.mode == TickMode.REJECTED and d2.reason == MD_REJECT_INTEGRITY  # L11
    assert [v for v, _ in d2.votes] == [B] and actions(d2) == ["VOTE"]
    assert d2.events[0]["prices"] == {"FEED_A": 3120.0, "FEED_B": 3125.0}


def test_trusted_newcomer_disagreeing_with_an_earlier_untrusted_state_votes_the_untrusted_one() -> None:
    arb = Arbiter(MdParams())
    st = States(FEED_A=PROBATION, FEED_B=TRUSTED)
    early = tick(seq=1, volume=100, last=3125.0)
    assert arb.classify(inc(early), st, 0).mode == TickMode.BACKFILL
    trusted_state = tick(source=B, volume=100, last=3120.0)
    d = arb.classify(inc(trusted_state), st, 0)
    assert [v for v, _ in d.votes] == [A] and d.mode == TickMode.LIVE


def test_both_trusted_disagreement_holds_the_instrument_without_a_vote() -> None:
    """H1: never vote by priority; hold (no LIVE, not current) until a tie-breaker."""
    arb = Arbiter(MdParams(), {A: 1, B: 2})
    st = States(FEED_A=TRUSTED, FEED_B=TRUSTED)
    assert arb.classify(inc(tick(seq=1, volume=100)), st, 0).mode == TickMode.LIVE
    bad_b = tick(source=B, volume=101, last=3126.0, ts=T0 + 400 * MS)
    assert arb.classify(inc(bad_b), st, 0).mode == TickMode.LIVE  # nothing to compare with yet
    good_a = tick(seq=2, volume=101, last=3121.0, ts=T0 + 500 * MS)
    d = arb.classify(inc(good_a), st, 0)
    assert d.mode == TickMode.BACKFILL and not d.votes and actions(d) == ["HOLD"]
    assert not arb.current("rb2701") and arb.state("rb2701").hold is not None
    # held: even a newer state is not LIVE
    newer = tick(seq=3, volume=101, last=3121.0, ts=T0 + 900 * MS, bid_vol1=9)
    assert arb.classify(inc(newer), st, 0).mode == TickMode.BACKFILL
    # both sources agree at a later common volume: released without a vote, and the newest trusted state goes LIVE
    a102 = tick(seq=4, volume=102, last=3122.0, ts=T0 + 1300 * MS)
    arb.classify(inc(a102), st, 0)
    d = arb.classify(inc(as_source(a102, B)), st, 0)
    assert "RELEASE" in actions(d) and not d.votes
    assert arb.state("rb2701").hold is None and arb.current("rb2701")
    assert arb.state("rb2701").wm == (TD, 102, T0 + 1300 * MS)


def test_hold_resolved_by_a_third_source_votes_the_loser() -> None:
    arb = Arbiter(MdParams())
    st = States(FEED_A=TRUSTED, FEED_B=TRUSTED, SINA=PROBATION)
    arb.classify(inc(tick(seq=1, volume=100)), st, 0)
    arb.classify(inc(tick(seq=2, volume=101, last=3126.0, ts=T0 + 400 * MS)), st, 0)  # A wrong
    d = arb.classify(inc(tick(source=B, volume=101, last=3121.0, ts=T0 + 400 * MS)), st, 0)
    assert actions(d) == ["HOLD"]
    third = msgspec.structs.replace(tick(source=SINA, volume=101, last=3121.0, ts=T0 + 400 * MS), flags=FLAG_COARSE_TS)
    d = arb.classify(inc(third), st, 0)
    assert [v for v, _ in d.votes] == [A]
    release = next(e for e in d.events if e["action"] == "RELEASE")
    assert release["odd"] == "FEED_A" and release["why"] == "third source"
    assert arb.state("rb2701").hold is None
    # the winner's state is published at once
    assert [t.source for t in d.republish] == [B]


def test_hold_ends_when_a_source_is_no_longer_trusted() -> None:
    arb = Arbiter(MdParams())
    st = States(FEED_A=TRUSTED, FEED_B=TRUSTED)
    arb.classify(inc(tick(seq=1, volume=100)), st, 0)
    arb.classify(inc(tick(seq=2, volume=101, last=3126.0, ts=T0 + 400 * MS)), st, 0)
    arb.classify(inc(tick(source=B, volume=101, last=3121.0, ts=T0 + 400 * MS)), st, 0)
    st.s[A] = EXCLUDED
    out = arb.check_holds(st, 0)
    assert [iid for iid, _ in out] == ["rb2701"]
    assert out[0][1].events[0]["action"] == "RELEASE" and not out[0][1].votes
    assert arb.state("rb2701").hold is None


def test_eviction_of_the_watermark_state_rolls_back_and_republishes_the_winner() -> None:
    arb = Arbiter(MdParams())
    st = States(FEED_A=TRUSTED, FEED_B=TRUSTED)
    arb.classify(inc(tick(seq=1, volume=100)), st, 0)
    bad = tick(seq=2, volume=101, last=3126.0, ts=T0 + 400 * MS)
    assert arb.classify(inc(bad), st, 0).mode == TickMode.LIVE
    good_b = tick(source=B, volume=101, last=3121.0, ts=T0 + 400 * MS)
    assert actions(arb.classify(inc(good_b), st, 0)) == ["HOLD"]
    ev = Evicted(A, "rb2701", TD, 2, 101, 3126.0, T0 + 400 * MS, bad.fingerprint, "MD_REJECT_PRICE_WITHOUT_TRADE", "FEED_B")
    d = arb.evicted(ev, st, 0)
    assert [v for v, _ in d.votes] == [A]
    assert actions(d) == ["RELEASE", "WM_ROLLBACK"]
    assert [t.last for t in d.republish] == [3121.0]
    assert arb.state("rb2701").wm == (TD, 101, T0 + 400 * MS) and arb.state("rb2701").wm_source == B


def test_watermark_sanity_rolls_back_a_poisoned_watermark() -> None:
    """C1: trusted sources keep delivering newer states below the watermark -> roll back to the last corroborated key."""
    params = MdParams(arb=ArbitrationConfig(stale_s=1.0), wm_sanity_s=1.0)
    arb = Arbiter(params)
    st = States(FEED_A=TRUSTED, FEED_B=TRUSTED)
    arb.classify(inc(tick(seq=1, volume=1000)), st, 0)
    poison = tick(seq=2, volume=101_000, ts=T0 + 100 * MS, turnover=3120.0 * 101_000 * 10)
    assert arb.classify(inc(poison), st, 0).mode == TickMode.LIVE
    modes = []
    for i in range(1, 12):
        now = i * 300 * MS
        t = tick(seq=2 + i, volume=1000 + i, ts=T0 + 1200 * MS + now)
        d = arb.classify(inc(t), st, 1200 * MS + now)
        modes.append(d.mode)
        if d.mode == TickMode.LIVE:
            assert "WM_ROLLBACK" in actions(d)
            break
    assert modes[-1] == TickMode.LIVE and arb.state("rb2701").rollbacks == 1
    assert arb.state("rb2701").wm[1] < 101_000


def test_currency_and_lag() -> None:
    """H2: a newer informative non-LIVE state by more than stale_s makes the LIVE stream not current."""
    arb = Arbiter(MdParams(arb=ArbitrationConfig(stale_s=1.0)))
    st = States(FEED_A=TRUSTED, FEED_B=EXCLUDED)
    t = tick(seq=1, volume=100)
    arb.classify(inc(t), st, 0)
    assert arb.current("rb2701") and arb.lag_ns("rb2701") == 0
    # the same state re-stamped later is no new information
    arb.classify(inc(msgspec.structs.replace(as_source(t, B), exch_ts_ns=T0 + 3 * S)), st, 0)
    assert arb.current("rb2701")
    # an excluded source still polling shows the market moving
    moved = tick(source=B, volume=130, ts=T0 + 3 * S)
    arb.classify(inc(moved), st, 0)
    assert not arb.current("rb2701") and arb.lag_ns("rb2701") == 3 * S
    arb.classify(inc(tick(seq=2, volume=131, ts=T0 + 3100 * MS)), st, 0)  # LIVE again
    assert arb.current("rb2701")


def test_coarse_timestamp_source_needs_a_volume_increase() -> None:
    arb = Arbiter(MdParams())
    st = States(FEED_A=TRUSTED, SINA=TRUSTED)
    assert arb.classify(inc(tick(seq=1, volume=100)), st, 0).mode == TickMode.LIVE
    same_vol = msgspec.structs.replace(tick(source=SINA, volume=100, ts=T0 + 1 * S, bid_vol1=3), flags=FLAG_COARSE_TS)
    assert arb.classify(inc(same_vol), st, 0).mode == TickMode.BACKFILL
    more = msgspec.structs.replace(tick(source=SINA, volume=101, ts=T0 + 2 * S), flags=FLAG_COARSE_TS)
    assert arb.classify(inc(more), st, 0).mode == TickMode.LIVE


def test_caught_up() -> None:
    arb = Arbiter(MdParams())
    st = States(FEED_A=TRUSTED, FEED_B=PROBATION)
    arb.classify(inc(tick(seq=1, volume=100)), st, 0)
    arb.classify(inc(tick(source=B, volume=90, ts=T0 - S)), st, 0)
    assert not arb.caught_up(B, 0, 2 * S)
    arb.classify(inc(tick(source=B, volume=100, ts=T0)), st, 0)
    assert arb.caught_up(B, 0, 2 * S)
    assert not arb.caught_up(B, 10 * S, 2 * S)  # nothing delivered recently
