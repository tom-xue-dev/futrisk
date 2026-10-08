"""Validator: session / limit / grid checks, future timestamps, same-source ordering (01 §8.13, 02 §11.3)."""

from __future__ import annotations

from datetime import datetime

import pytest

from futrisk.common.clock import BJ, bj_to_ns
from futrisk.messages.codes import (
    MD_REJECT_FUTURE_TS,
    MD_REJECT_NON_MONOTONIC,
    MD_REJECT_OFF_TICK,
    MD_REJECT_OUT_OF_SESSION,
    MD_REJECT_PRICE_LIMIT,
    MD_REJECT_PRICE_WITHOUT_TRADE,
)
from futrisk.messages.enums import SourceId
from futrisk.services.md_gateway.normalizer import Normalizer
from futrisk.services.md_gateway.params import MdParams
from futrisk.services.md_gateway.sessions import SessionIndex
from futrisk.services.md_gateway.validator import ACCEPT, REJECT, SKIP, Validator

from .helpers import MS, T0, TD, S, inc, tick

A, B = SourceId.FEED_A, SourceId.FEED_B


@pytest.fixture
def v(sessions: SessionIndex, normalizer: Normalizer) -> Validator:
    return Validator(sessions, MdParams(), normalizer.known_meta)


def check(v: Validator, t: object, **kw: object) -> str:
    """Run the check as the pipeline does: an accepted live state becomes the source's ordering reference."""
    i = inc(t, **kw)  # type: ignore[arg-type]
    verdict = v.check(i)
    if verdict.status == ACCEPT:
        v.on_accepted(i)
        return ACCEPT
    return verdict.status if verdict.status == SKIP else f"{verdict.status}:{verdict.reason}"


def test_stateless_checks(v: Validator) -> None:
    lunch = bj_to_ns(datetime(2026, 9, 28, 12, 0, tzinfo=BJ))
    assert check(v, tick(ts=lunch, seq=1)) == f"{REJECT}:{MD_REJECT_OUT_OF_SESSION}"
    assert check(v, tick(last=3300.0, seq=2)) == f"{REJECT}:{MD_REJECT_PRICE_LIMIT}"
    assert check(v, tick(iid="au2612", last=850.01, upper=969.0, lower=731.0, pre_settle=850.0, seq=3)) == f"{REJECT}:{MD_REJECT_OFF_TICK}"
    assert check(v, tick(iid="au2612", last=850.02, upper=969.0, lower=731.0, pre_settle=850.0, seq=4)) == ACCEPT
    # a closing snapshot stamped shortly after a window end is still in session
    close = bj_to_ns(datetime(2026, 9, 28, 15, 0, 30, tzinfo=BJ))
    assert check(v, tick(ts=close, seq=5, volume=200)) == ACCEPT


def test_future_stamped_state_is_rejected_and_never_a_reference(v: Validator) -> None:
    """exch_ts more than future_skew_s after the receive time -> REJECT; later genuine states are judged against seq 1."""
    assert check(v, tick(seq=1, volume=100)) == ACCEPT
    fut = v.check(inc(tick(seq=2, volume=101, ts=T0 + 10 * S, recv=T0 + 500 * MS)))
    assert (fut.status, fut.reason) == (REJECT, MD_REJECT_FUTURE_TS) and fut.detail["ahead_ms"] == 9500.0
    assert check(v, tick(seq=3, volume=102, ts=T0 + 1000 * MS)) == ACCEPT
    assert check(v, tick(seq=4, volume=103, ts=T0 + 1500 * MS, recv=T0 + 1500 * MS - 900 * MS)) == ACCEPT  # skew allowance
    assert v.latest(A, "rb2701") == (TD, 103, T0 + 1500 * MS, 3120.0)


def test_ordering_and_price_without_trade(v: Validator) -> None:
    assert check(v, tick(seq=1, volume=100, ts=T0)) == ACCEPT
    assert check(v, tick(seq=2, volume=100, ts=T0 + 500 * MS, last=3121.0)) == f"{REJECT}:{MD_REJECT_PRICE_WITHOUT_TRADE}"
    assert check(v, tick(seq=3, volume=90, ts=T0 + 600 * MS)) == f"{REJECT}:{MD_REJECT_NON_MONOTONIC}"
    assert check(v, tick(seq=4, volume=100, ts=T0 + 700 * MS, bid_vol1=9)) == ACCEPT  # book change, same price
    assert check(v, tick(seq=5, volume=104, ts=T0 + 1000 * MS, last=3121.0)) == ACCEPT
    assert check(v, tick(seq=6, volume=104, ts=T0 + 900 * MS)) == f"{REJECT}:{MD_REJECT_NON_MONOTONIC}"  # time going back


def test_replays_and_snapshots_are_accepted_but_never_the_reference(v: Validator) -> None:
    assert check(v, tick(seq=10, volume=100, ts=T0)) == ACCEPT
    assert check(v, tick(seq=14, volume=120, ts=T0 + 2000 * MS, last=3122.0)) == ACCEPT
    # a replay into the hole is recorded as BACKFILL; the live reference stays seq 14
    assert check(v, tick(seq=12, volume=110, ts=T0 + 1000 * MS, last=3121.0), replay=True) == ACCEPT
    assert check(v, tick(volume=119, ts=T0 + 1900 * MS), snapshot=True) == ACCEPT
    assert v.latest(A, "rb2701") == (TD, 120, T0 + 2000 * MS, 3122.0)
    assert check(v, tick(seq=15, volume=119, ts=T0 + 2500 * MS)) == f"{REJECT}:{MD_REJECT_NON_MONOTONIC}"


def test_a_new_trading_day_resets_the_reference(v: Validator) -> None:
    assert check(v, tick(seq=1, volume=500_000)) == ACCEPT
    nxt = T0 + 86_400 * S  # Tuesday 09:35
    assert check(v, tick(seq=2, volume=10, ts=nxt, td="20260929")) == ACCEPT
    assert v.latest(A, "rb2701") == ("20260929", 10, nxt, 3120.0)


def test_polled_record_out_of_session_now_is_skipped(v: Validator) -> None:
    lunch = bj_to_ns(datetime(2026, 9, 28, 12, 0, tzinfo=BJ))
    assert check(v, tick(source=B, ts=T0, recv=lunch), recv=lunch) == SKIP
    assert check(v, tick(source=B, ts=T0)) == ACCEPT
    assert check(v, tick(ts=T0, seq=1, recv=lunch), recv=lunch) == ACCEPT  # pushed live states are never skipped


def test_stale_polled_snapshot_in_an_open_session_is_skipped_not_rejected(v: Validator) -> None:
    """A polled / snapshot record stamped before the open window (no update yet in this session, e.g. CFFEX
    during the 09:25 auction) is skipped and never counts as a reject; pushed live states and records stamped
    after the receive time are still judged out of session."""
    now = bj_to_ns(datetime(2026, 9, 28, 10, 31, tzinfo=BJ))  # day session 10:30-11:30 is open
    brk = bj_to_ns(datetime(2026, 9, 28, 10, 20, tzinfo=BJ))  # the 10:15-10:30 break
    assert check(v, tick(source=B, ts=brk, recv=now), recv=now) == SKIP
    assert check(v, tick(ts=brk, recv=now), recv=now, snapshot=True) == SKIP  # Feed A subscribe snapshot
    assert check(v, tick(ts=brk, seq=1, recv=now), recv=now) == f"{REJECT}:{MD_REJECT_OUT_OF_SESSION}"  # pushed
    later = bj_to_ns(datetime(2026, 9, 28, 11, 40, tzinfo=BJ))  # after the window, later than the receive time
    assert check(v, tick(source=B, ts=later, recv=now), recv=now) == f"{REJECT}:{MD_REJECT_OUT_OF_SESSION}"


def test_sources_are_judged_separately_and_forgotten_on_epoch_change(v: Validator) -> None:
    assert check(v, tick(seq=1, volume=100, ts=T0)) == ACCEPT
    assert check(v, tick(source=B, volume=90, ts=T0 - S)) == ACCEPT  # Feed B's own history starts here
    assert check(v, tick(source=B, volume=100, ts=T0)) == ACCEPT
    assert check(v, tick(seq=2, volume=90, ts=T0 + 500 * MS)) == f"{REJECT}:{MD_REJECT_NON_MONOTONIC}"
    v.forget_source(A)
    assert v.latest(A, "rb2701") is None and v.latest(B, "rb2701") == (TD, 100, T0, 3120.0)
    assert check(v, tick(seq=1, volume=90, ts=T0 + 600 * MS)) == ACCEPT  # the new epoch restarts the seqs
