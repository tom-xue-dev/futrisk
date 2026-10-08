"""Crash-safe Parquet shards and the tick-file readers (03 §5.1)."""

from __future__ import annotations

import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any

import msgspec
import pyarrow.parquet as pq
import pytest

import futrisk.services.md_gateway.shards as shards_mod
from futrisk.common.clock import BJ, NS_PER_S, SimClock, bj_to_ns
from futrisk.messages import codes
from futrisk.messages.enums import TickMode
from futrisk.messages.md import Tick
from futrisk.services.md_gateway.normalizer import tick_fingerprint
from futrisk.services.md_gateway.shards import (
    TICK_SCHEMA,
    ParquetShardWriter,
    ShardWriteError,
    list_shards,
    list_trading_days,
    read_raw_ticks,
    reject_row,
    tick_row,
)
from futrisk.services.md_gateway.sina import SinaDecoder

EPOCH_MS = 1_790_000_000_000
TD = "20260928"


def bj_ns(*args: int) -> int:
    return bj_to_ns(datetime(*args, tzinfo=BJ))


def variant(t: Tick, **changes: Any) -> Tick:
    t = msgspec.structs.replace(t, **changes)
    return msgspec.structs.replace(t, fingerprint=tick_fingerprint(t))


def boom(*args: Any, **kwargs: Any) -> None:
    raise OSError("disk full")


@pytest.fixture
def rb_tick(sina_decoder: SinaDecoder, payloads: dict[str, str]) -> Tick:
    """rb2701 at 10:30:00 on 2026-09-28 (continuous session), volume 1000."""
    fields = payloads["nf_RB2701"].split(",")
    fields[1], fields[17], fields[14] = "103000", "2026-09-28", "1000"
    tick = sina_decoder.incoming("nf_RB2701", ",".join(fields), bj_ns(2026, 9, 28, 10, 30, 1), 0).tick
    assert tick is not None
    return tick


def writer(root: Path, clock: SimClock, **kw: Any) -> ParquetShardWriter:
    return ParquetShardWriter(root, "sina", TICK_SCHEMA, prefix="part", epoch_ms=EPOCH_MS, clock=clock, **kw)


def test_rows_carry_every_column(rb_tick: Tick) -> None:
    row = tick_row(rb_tick, TickMode.LIVE, None, "E1")
    assert set(row) == set(TICK_SCHEMA.names) and row["source"] == "SINA" and row["flags"] == 1 and row["mode"] == "LIVE"
    bad = reject_row(instrument_id="rb2701", exchange="SHFE", trading_day=TD, recv_ns=1, source="SINA", reject_reason=codes.MD_REJECT_MALFORMED, seq=7)
    assert set(bad) == set(TICK_SCHEMA.names) and bad["last"] is None and bad["source_seq"] == 7 and bad["mode"] == "REJECTED"


def test_roll_by_rows_and_read_back(tmp_path: Path, rb_tick: Tick) -> None:
    w = writer(tmp_path, SimClock(rb_tick.recv_ns), roll_s=60, roll_rows=3)
    written: list[Path] = []
    for i in range(7):
        written += w.append(TD, tick_row(variant(rb_tick, recv_ns=rb_tick.recv_ns + i), TickMode.LIVE))
    assert [p.name for p in written] == [f"part-{EPOCH_MS}-000001.parquet", f"part-{EPOCH_MS}-000002.parquet"]
    assert w.pending_rows == 1
    written += w.flush()
    assert w.pending_rows == 0 and w.shards_written == 3 and w.rows_written == 7 and w.last_shard == written[-1]
    assert list_shards(tmp_path, TD) == written and all(p.parent == tmp_path / TD / "sina" for p in written)
    assert not list(tmp_path.rglob("*.tmp"))
    table = pq.read_table(written[0])
    assert table.schema.equals(TICK_SCHEMA, check_metadata=False) and table.schema.metadata[b"futrisk.kind"] == b"ticks.raw"
    assert pq.ParquetFile(written[0]).metadata.row_group(0).column(0).compression == "ZSTD"
    assert table.column("recv_ns").to_pylist() == [rb_tick.recv_ns + i for i in range(3)]
    row = table.slice(0, 1).to_pylist()[0]
    assert row["instrument_id"] == "rb2701" and row["mode"] == "LIVE" and row["fingerprint"] == rb_tick.fingerprint
    df = read_raw_ticks(tmp_path, TD)
    assert len(df) == 7 and df["recv_ns"].is_monotonic_increasing and str(df["volume"].dtype) == "Int64" and str(df["fingerprint"].dtype) == "UInt64"


def test_roll_by_time_and_trading_day_change(tmp_path: Path, rb_tick: Tick) -> None:
    clock = SimClock(rb_tick.recv_ns)
    w = writer(tmp_path, clock, roll_s=10, roll_rows=1000)
    w.append(TD, tick_row(rb_tick, TickMode.LIVE))
    clock.advance(5 * NS_PER_S)
    assert w.roll_if_due() == []
    clock.advance(6 * NS_PER_S)
    assert len(w.roll_if_due()) == 1 and w.pending_rows == 0
    assert w.append(TD, tick_row(rb_tick, TickMode.LIVE)) == []
    [closed] = w.append("20260929", tick_row(rb_tick, TickMode.REJECTED, codes.MD_REJECT_TRADING_DAY))  # the day rolled
    assert closed.parent == tmp_path / TD / "sina"
    [second] = w.flush()
    assert second.parent == tmp_path / "20260929" / "sina" and list_trading_days(tmp_path) == [TD, "20260929"]


def test_failed_write_keeps_rows_and_backs_off(tmp_path: Path, rb_tick: Tick, monkeypatch: pytest.MonkeyPatch) -> None:
    clock = SimClock(rb_tick.recv_ns)
    w = writer(tmp_path, clock)
    w.append(TD, tick_row(rb_tick, TickMode.LIVE))
    w.append(TD, tick_row(rb_tick, TickMode.DUPLICATE))
    monkeypatch.setattr(shards_mod.pq, "write_table", boom)
    with pytest.raises(ShardWriteError) as err:
        w.flush()
    assert isinstance(err.value, OSError) and err.value.failures[0][0] == TD
    assert w.pending_rows == 2 and w.consecutive_failures == 1
    assert list_shards(tmp_path, TD) == [] and not list(tmp_path.rglob("*.tmp"))
    monkeypatch.undo()
    assert w.flush() == []  # still inside the 1 s retry backoff: nothing is attempted
    clock.advance(NS_PER_S)
    [path] = w.flush()
    assert pq.read_table(path).num_rows == 2 and w.consecutive_failures == 0


def test_retry_backoff_does_not_rewrite_on_every_append(tmp_path: Path, rb_tick: Tick, monkeypatch: pytest.MonkeyPatch) -> None:
    attempts = 0

    def failing_write(*args: Any, **kwargs: Any) -> None:
        nonlocal attempts
        attempts += 1
        raise OSError("disk full")

    clock = SimClock(rb_tick.recv_ns)
    w = writer(tmp_path, clock, roll_rows=1)  # every append makes the bucket due
    monkeypatch.setattr(shards_mod.pq, "write_table", failing_write)
    with pytest.raises(ShardWriteError):
        w.append(TD, tick_row(rb_tick, TickMode.LIVE))
    for _ in range(20):
        w.append(TD, tick_row(rb_tick, TickMode.DUPLICATE))  # backing off: no write, no error
    assert attempts == 1 and w.pending_rows == 21
    clock.advance(NS_PER_S)
    with pytest.raises(ShardWriteError):
        w.append(TD, tick_row(rb_tick, TickMode.DUPLICATE))
    assert attempts == 2  # next attempt only after the backoff (now 2 s)


def test_vanished_day_folder_is_recreated(tmp_path: Path, rb_tick: Tick) -> None:
    w = writer(tmp_path, SimClock(rb_tick.recv_ns))
    w.append(TD, tick_row(rb_tick, TickMode.LIVE))
    [first] = w.flush()
    shutil.rmtree(tmp_path / TD)  # e.g. the user moved the day folder away mid-run
    w.append(TD, tick_row(rb_tick, TickMode.DUPLICATE))
    [second] = w.flush()
    assert second.parent == tmp_path / TD / "sina" and second.exists() and not first.exists()


def test_failing_bucket_does_not_block_another(tmp_path: Path, rb_tick: Tick) -> None:
    (tmp_path / TD).write_text("a file where the day folder should be", encoding="utf-8")
    clock = SimClock(rb_tick.recv_ns)
    w = writer(tmp_path, clock, roll_s=5)
    w.append(TD, tick_row(rb_tick, TickMode.LIVE))
    with pytest.raises(ShardWriteError) as err:  # the day rolls: closing out TD fails ...
        w.append("20260929", tick_row(rb_tick, TickMode.LIVE))
    assert [bucket for bucket, _ in err.value.failures] == [TD]
    [written] = w.flush("20260929")  # ... but the healthy bucket writes at once (no shared backoff)
    assert written.parent == tmp_path / "20260929" / "sina" and w.pending_rows == 1
    w.append("20260929", tick_row(rb_tick, TickMode.DUPLICATE))
    clock.advance(10 * NS_PER_S)
    with pytest.raises(ShardWriteError) as err:  # both due in one roll: TD fails again, 20260929 is still written
        w.roll_if_due()
    assert [bucket for bucket, _ in err.value.failures] == [TD]
    [written2] = err.value.written
    assert written2.parent == tmp_path / "20260929" / "sina" and w.pending_rows == 1


def test_spill_after_persistent_failures_then_recover(tmp_path: Path, rb_tick: Tick) -> None:
    raw, spill = tmp_path / "raw", tmp_path / "spill"
    raw.write_text("not a directory", encoding="utf-8")  # the primary root is unusable
    clock = SimClock(rb_tick.recv_ns)
    w = writer(raw, clock, spill_roots=[spill], spill_after=3)
    w.append(TD, tick_row(rb_tick, TickMode.LIVE))
    for delay_s in (0, 1, 2):  # three failed attempts, each after the previous backoff
        clock.advance(delay_s * NS_PER_S)
        if delay_s < 2:
            with pytest.raises(ShardWriteError):
                w.flush()
        else:
            [spilled] = w.flush()
    assert spilled.parent == spill / TD / "sina" and w.pending_rows == 0 and w.spilled_shards == 1
    assert pq.read_table(spilled).num_rows == 1
    w.append(TD, tick_row(rb_tick, TickMode.DUPLICATE))  # while the primary backs off, due shards go to the spill root
    [spilled2] = w.flush()
    assert spilled2.parent == spill / TD / "sina" and spilled2.name != spilled.name
    raw.unlink()  # the disk comes back: after the backoff the primary is used again
    clock.advance(60 * NS_PER_S)
    w.append(TD, tick_row(rb_tick, TickMode.DUPLICATE))
    [primary] = w.flush()
    assert primary.parent == raw / TD / "sina" and w.consecutive_failures == 0


def test_final_close_spills_immediately_or_raises(tmp_path: Path, rb_tick: Tick) -> None:
    raw, bad_spill, good_spill = tmp_path / "raw", tmp_path / "bad", tmp_path / "good"
    raw.write_text("x", encoding="utf-8")
    bad_spill.write_text("x", encoding="utf-8")
    w = writer(raw, SimClock(rb_tick.recv_ns), spill_roots=[bad_spill, good_spill])
    w.append(TD, tick_row(rb_tick, TickMode.LIVE))
    [path] = w.close()
    assert path.parent == good_spill / TD / "sina" and w.pending_rows == 0
    w2 = writer(raw, SimClock(rb_tick.recv_ns), spill_roots=[bad_spill])
    w2.append(TD, tick_row(rb_tick, TickMode.LIVE))
    with pytest.raises(ShardWriteError):
        w2.close()
    assert w2.pending_rows == 1  # kept: never dropped silently


def test_memory_is_bounded_and_drops_are_counted(tmp_path: Path, rb_tick: Tick) -> None:
    raw = tmp_path / "raw"
    raw.write_text("x", encoding="utf-8")
    w = writer(raw, SimClock(rb_tick.recv_ns), roll_rows=10**6, max_pending_rows=10)
    for _ in range(60):
        w.append(TD, tick_row(rb_tick, TickMode.DUPLICATE))
    assert w.pending_rows <= 40  # 4x the soft cap at most
    assert w.dropped_rows == 60 - w.pending_rows > 0


def test_temp_files_are_ignored_and_stale_ones_removed(tmp_path: Path, rb_tick: Tick) -> None:
    clock = SimClock(rb_tick.recv_ns)
    now_s = clock.now_ns() / NS_PER_S
    d = tmp_path / TD / "sina"
    d.mkdir(parents=True)
    fresh, stale = d / ".part-1-000001.parquet.tmp", d / ".part-0-000001.parquet.tmp"
    fresh.write_bytes(b"half-written shard of another process")
    stale.write_bytes(b"left over by a crash")
    os.utime(fresh, (now_s, now_s))
    os.utime(stale, (now_s - 7200, now_s - 7200))
    assert list_shards(tmp_path, TD) == [] and read_raw_ticks(tmp_path, TD).empty
    assert list(read_raw_ticks(tmp_path, TD).columns) == list(TICK_SCHEMA.names) and list_trading_days(tmp_path / "nope") == []
    w = writer(tmp_path, clock)
    w.append(TD, tick_row(rb_tick, TickMode.LIVE))
    [path] = w.flush()
    assert fresh.exists() and not stale.exists() and list_shards(tmp_path, TD) == [path]
