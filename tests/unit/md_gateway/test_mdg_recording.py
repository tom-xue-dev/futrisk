"""Raw recording, durable cursors, canonical build and md.ticks paging (03 §5.1, 02 §6.2)."""

from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq

from futrisk.common.clock import SystemClock
from futrisk.marketdata.recorder import list_shards
from futrisk.messages.enums import SourceId, TickMode
from futrisk.messages.md import TicksQuery
from futrisk.services.md_gateway.commands import _page
from futrisk.services.md_gateway.recording import (
    TickRecorder,
    build_canonical,
    canonical_fresh,
    day_shards,
    read_canonical,
    reject_row,
    tick_row,
)

from .helpers import MS, T0, TD, as_source, tick

A, B = SourceId.FEED_A, SourceId.FEED_B


async def test_recorder_writes_shards_and_marks_cursor_durable_only_when_flushed(tmp_path: Path) -> None:
    rec = TickRecorder(tmp_path / "raw", sources=[A, B], wall_clock=SystemClock(), epoch_ms=1, roll_s=3600, roll_rows=10_000)
    t = tick(seq=1)
    rec.add(A, TD, tick_row(t, TickMode.LIVE, None, "E1"))
    rec.add(A, TD, reject_row(instrument_id="rb2701", exchange="SHFE", trading_day=TD, recv_ns=T0, source=A, reason="MD_REJECT_MALFORMED", epoch="E1", seq=2))
    await rec.handoff({A: ("E1", 2)})
    assert A not in rec.durable  # still buffered in the writer (roll not due)
    assert list_shards(tmp_path / "raw", TD, "feed_a") == []
    await rec.handoff({A: ("E1", 2)}, flush=True)
    assert rec.durable[A] == ("E1", 2)
    shards = list_shards(tmp_path / "raw", TD, "feed_a")
    assert len(shards) == 1
    table = pq.read_table(shards[0])
    assert table.num_rows == 2
    assert table.column("mode").to_pylist() == ["LIVE", "REJECTED"]
    assert table.column("reject_reason").to_pylist() == [None, "MD_REJECT_MALFORMED"]
    await rec.close({})


async def test_canonical_dedupes_by_fingerprint_preferring_live_and_is_rebuilt_when_raw_changes(tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    rec = TickRecorder(raw, sources=[A, B], wall_clock=SystemClock(), epoch_ms=1)
    t1 = tick(seq=1, volume=100)
    t2 = tick(seq=2, volume=101, ts=T0 + 500 * MS)
    rec.add(B, TD, tick_row(as_source(t1, B), TickMode.BACKFILL, None, None))  # same state seen first by B
    rec.add(A, TD, tick_row(t1, TickMode.LIVE, None, "E1"))
    rec.add(A, TD, tick_row(t2, TickMode.BACKFILL, None, "E1"))
    rec.add(B, TD, tick_row(as_source(t2, B), TickMode.DUPLICATE, None, None))
    rec.add(A, TD, tick_row(tick(seq=3, volume=90, ts=T0 + 700 * MS), TickMode.REJECTED, "MD_REJECT_NON_MONOTONIC", "E1"))
    await rec.handoff({}, flush=True)
    prio = {"FEED_A": 1, "FEED_B": 2}
    out = build_canonical(raw, tmp_path / "canonical", TD, [A, B], prio)
    df = read_canonical(out)
    assert list(df["volume"]) == [100, 101]
    assert list(df["mode"].astype(str)) == ["LIVE", "BACKFILL"]
    assert list(df["source"].astype(str)) == ["FEED_A", "FEED_A"]
    assert canonical_fresh(out, day_shards(raw, TD, [A, B]), raw)
    rec.add(A, TD, tick_row(tick(seq=4, volume=105, ts=T0 + 900 * MS), TickMode.LIVE, None, "E1"))
    await rec.handoff({}, flush=True)
    assert not canonical_fresh(out, day_shards(raw, TD, [A, B]), raw)
    df2 = read_canonical(build_canonical(raw, tmp_path / "canonical", TD, [A, B], prio))
    assert list(df2["volume"]) == [100, 101, 105]
    await rec.close({})


async def test_tick_pages_do_not_split_timestamp_groups(tmp_path: Path) -> None:
    import futrisk.services.md_gateway.commands as cmds

    raw = tmp_path / "raw"
    rec = TickRecorder(raw, sources=[A], wall_clock=SystemClock(), epoch_ms=1)
    for i in range(1, 8):
        mode = TickMode.BACKFILL if i % 2 else TickMode.LIVE
        rec.add(A, TD, tick_row(tick(seq=i, volume=100 + i, ts=T0 + i * 100 * MS), mode, None, "E1"))
    await rec.handoff({}, flush=True)
    from futrisk.marketdata.recorder import read_shards

    table = read_shards(day_shards(raw, TD, [A]))
    old = cmds.TICKS_PAGE
    cmds.TICKS_PAGE = 2
    try:
        q = TicksQuery(instrument_id="rb2701", trading_day=TD, from_ns=T0, to_ns=T0 + 10**12, modes=[TickMode.BACKFILL])
        seen: list[int] = []
        batch = _page(table, q, ["BACKFILL"])
        while True:
            seen += [t.volume for t in batch.ticks]
            if batch.next_from_ns is None:
                break
            q = TicksQuery(instrument_id="rb2701", trading_day=TD, from_ns=batch.next_from_ns, to_ns=q.to_ns, modes=q.modes)
            batch = _page(table, q, ["BACKFILL"])
        assert seen == [101, 103, 105, 107]
    finally:
        cmds.TICKS_PAGE = old
    await rec.close({})


# ---------------------------------------------------------------- round 2 (L3, L4, L5, M1)
async def test_dropped_rows_never_make_a_cursor_durable(tmp_path: Path) -> None:
    """L4 + L5: the loop-side buffer is bounded; a source that lost rows keeps its old durable cursor."""
    rec = TickRecorder(tmp_path / "raw", sources=[A, B], wall_clock=SystemClock(), epoch_ms=1, max_pending_rows=2)
    for i in range(1, 4):
        rec.add_tick(A, TD, tick(seq=i, volume=100 + i), TickMode.LIVE, None, "E1")
    rec.add_tick(B, TD, as_source(tick(volume=101), B), TickMode.DUPLICATE, None, None)
    assert rec.dropped_rows == 2 and rec.lossy == {A, B}
    await rec.handoff({A: ("E1", 3), B: (None, T0)}, flush=True)
    assert rec.durable == {}
    await rec.close({})


async def test_spilled_shards_are_visible_to_the_day_readers(tmp_path: Path) -> None:
    """L5: when the raw directory cannot be written the rows go to the spill root, and the canonical build reads them."""
    raw, spill = tmp_path / "raw", tmp_path / "spill"
    raw.mkdir()
    (raw / TD).write_text("not a directory")  # the day folder cannot be created
    rec = TickRecorder(raw, sources=[A], wall_clock=SystemClock(), epoch_ms=1, spill_dirs=[spill])
    rec.add_tick(A, TD, tick(seq=1, volume=100), TickMode.LIVE, None, "E1")
    await rec.close({A: ("E1", 1)})
    shards = day_shards(raw, TD, [A], spill_dirs=[spill])
    assert len(shards) == 1 and spill in shards[0].parents
    df = read_canonical(build_canonical(raw, tmp_path / "canonical", TD, [A], {"FEED_A": 1}, spill_dirs=[spill]))
    assert list(df["volume"]) == [100]


async def test_canonical_keeps_quote_reversions_and_drops_coarse_copies(tmp_path: Path) -> None:
    """M1: the same fingerprint at a later time is another market state; a Sina copy of a known state is redundant."""
    import msgspec

    from futrisk.messages.md import FLAG_COARSE_TS

    raw = tmp_path / "raw"
    sina = SourceId.SINA
    rec = TickRecorder(raw, sources=[A, B, sina], wall_clock=SystemClock(), epoch_ms=1)
    s1 = tick(seq=1, volume=100, ts=T0, bid_vol1=5)
    s2 = tick(seq=2, volume=100, ts=T0 + 500 * MS, bid_vol1=6)
    s3 = tick(seq=3, volume=100, ts=T0 + 1000 * MS, bid_vol1=5)
    for t in (s1, s2, s3):
        rec.add_tick(A, TD, t, TickMode.LIVE, None, "E1")
    rec.add_tick(B, TD, as_source(s1, B), TickMode.DUPLICATE, None, None)
    coarse = msgspec.structs.replace(as_source(s2, sina), exch_ts_ns=T0, flags=FLAG_COARSE_TS)
    rec.add_tick(sina, TD, coarse, TickMode.BACKFILL, None, None)
    await rec.close({})
    df = read_canonical(build_canonical(raw, tmp_path / "canonical", TD, [A, B, sina], {"FEED_A": 1, "FEED_B": 2, "SINA": 3}))
    assert list(df["exch_ts_ns"]) == [T0, T0 + 500 * MS, T0 + 1000 * MS]
    assert set(df["source"].astype(str)) == {"FEED_A"}


async def test_shard_cache_reads_only_new_shards(tmp_path: Path) -> None:
    """L3: paging md.ticks never re-reads the whole day."""
    from futrisk.services.md_gateway.recording import ShardCache

    raw = tmp_path / "raw"
    rec = TickRecorder(raw, sources=[A], wall_clock=SystemClock(), epoch_ms=1)
    rec.add_tick(A, TD, tick(seq=1, volume=100), TickMode.LIVE, None, "E1")
    await rec.handoff({}, flush=True)
    cache = ShardCache()
    assert cache.table(TD, day_shards(raw, TD, [A])).num_rows == 1 and cache.reads == 1
    assert cache.table(TD, day_shards(raw, TD, [A])).num_rows == 1 and cache.reads == 1
    rec.add_tick(A, TD, tick(seq=2, volume=101, ts=T0 + MS), TickMode.LIVE, None, "E1")
    await rec.handoff({}, flush=True)
    assert cache.table(TD, day_shards(raw, TD, [A])).num_rows == 2 and cache.reads == 2
    await rec.close({})
