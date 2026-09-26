"""Tick-time indicators of the bots: bars, SMA, Donchian channel, rolling z-score."""

from __future__ import annotations

import pytest

from futrisk.sim.bots.indicators import NS_PER_S, SMA, BarBuilder, Channel, OhlcBar, RollingStats


def test_bar_builder_completes_on_the_next_interval() -> None:
    b = BarBuilder(30)
    t0 = 1_790_559_300 * NS_PER_S  # aligned to 30 s
    assert b.update(t0, 10.0) is None
    assert b.update(t0 + 10 * NS_PER_S, 12.0) is None
    assert b.update(t0 + 20 * NS_PER_S, 9.0) is None
    done = b.update(t0 + 31 * NS_PER_S, 11.0)
    assert done is not None and (done.open, done.high, done.low, done.close, done.ticks) == (10.0, 12.0, 9.0, 9.0, 3)
    assert b.current is not None and b.current.start_ns == t0 + 30 * NS_PER_S
    # a gap (session break) just starts a new bar
    done = b.update(t0 + 3600 * NS_PER_S, 13.0)
    assert done is not None and done.close == 11.0
    with pytest.raises(ValueError):
        BarBuilder(0)


def test_sma_and_channel() -> None:
    s = SMA(3)
    assert [s.add(x) for x in (1.0, 2.0, 3.0, 4.0)] == [None, None, 2.0, 3.0]
    assert s.ready and len(s) == 3
    ch = Channel(3)
    for i, (h, low) in enumerate([(5, 1), (7, 2), (6, 3), (4, 2)]):
        ch.add(OhlcBar(i, h, h, low, h))
    assert ch.ready and (ch.upper(), ch.lower()) == (7, 2)
    assert (ch.upper(2), ch.lower(2)) == (6, 2)


def test_rolling_zscore() -> None:
    r = RollingStats(4)
    for x in (1.0, 1.0, 1.0):
        r.add(x)
    assert r.zscore(1.0) is None  # no dispersion yet
    for x in (2.0, 3.0, 4.0):
        r.add(x)
    assert len(r) == 4 and r.mean == pytest.approx(2.5)
    assert r.zscore(2.5) == pytest.approx(0.0) and r.zscore(5.0) > 2
