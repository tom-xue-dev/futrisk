"""Recorder tests: classification, crash-safe Parquet shards, read-back and rebuild (03 §5.1, 01 §8.13)."""

from __future__ import annotations

import os
import shutil
import threading
from datetime import datetime
from pathlib import Path
from typing import Any

import msgspec
import pandas as pd
import pyarrow.parquet as pq
import pytest

import futrisk.marketdata.recorder as recorder_mod
from futrisk.common.clock import BJ, NS_PER_S, SimClock, bj_to_ns
from futrisk.marketdata.fingerprint import with_fingerprint
from futrisk.marketdata.poller import SinaPoll
from futrisk.marketdata.recorder import (
    ParquetShardWriter,
    ShardWriteError,
    SinaRecorder,
    TickClassifier,
    bucket_trading_day,
    list_shards,
    list_trading_days,
    read_raw_ticks,
    read_wire,
    rebuild_ticks_from_wire,
)
from futrisk.marketdata.sina import SinaNormalizer, SinaResult, decode_body
from futrisk.marketdata.tickschema import TICK_SCHEMA, tick_row
from futrisk.messages import codes
from futrisk.messages.enums import TickMode
from futrisk.messages.md import Tick

EPOCH_MS = 1_790_000_000_000
TD = "20260928"


def bj_ns(*args: int) -> int:
    return bj_to_ns(datetime(*args, tzinfo=BJ))


def set_fields(payload: str, changes: dict[int, str]) -> str:
    fields = payload.split(",")
    for i, v in changes.items():
        fields[i] = v
    return ",".join(fields)


def variant(t: Tick, **changes: Any) -> Tick:
    return with_fingerprint(msgspec.structs.replace(t, **changes))


def make_poll(
    body: bytes,
    recv_ns: int,
    codes_: tuple[str, ...],
    seq: int = 1,
    status: int = 200,
    error: str | None = None,
) -> SinaPoll:
    return SinaPoll(seq, codes_, recv_ns - 40_000_000, recv_ns, 40.0, status, body, error)


def boom(*args: Any, **kwargs: Any) -> None:
    raise OSError("disk full")


@pytest.fixture
def rb_tick(normalizer: SinaNormalizer, payloads: dict[str, str]) -> Tick:
    """rb2701 at 10:30:00 on 2026-09-28 (continuous session), volume 1000."""
    payload = set_fields(payloads["nf_RB2701"], {1: "103000", 17: "2026-09-28", 14: "1000"})
    tick = normalizer.normalize("nf_RB2701", payload, bj_ns(2026, 9, 28, 10, 30, 1)).tick
    assert tick is not None
    return tick


@pytest.fixture
def classifier(normalizer: SinaNormalizer) -> TickClassifier:
    return TickClassifier(normalizer.calendar, normalizer.instruments.values())


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------


class TestClassifier:
    def test_live_duplicate_backfill_and_rejects(self, classifier: TickClassifier, rb_tick: Tick) -> None:
        s = NS_PER_S
        t0 = rb_tick.exch_ts_ns
        assert classifier.classify_tick(rb_tick, TD) == (TickMode.LIVE, None)
        # the same state polled again (Sina's clock moved on): duplicate
        assert classifier.classify_tick(variant(rb_tick, exch_ts_ns=t0 + s), TD) == (TickMode.DUPLICATE, None)
        # quote-only change without volume: accepted but cannot advance LIVE (01 §8.13)
        assert classifier.classify_tick(variant(rb_tick, exch_ts_ns=t0 + 2 * s, bid_vol1=5), TD) == (
            TickMode.BACKFILL,
            None,
        )
        t4 = variant(rb_tick, exch_ts_ns=t0 + 3 * s, volume=1010, last=3119.0)
        assert classifier.classify_tick(t4, TD) == (TickMode.LIVE, None)
        assert classifier.classify_tick(variant(t4, exch_ts_ns=t0 + 4 * s, volume=1005), TD) == (
            TickMode.REJECTED,
            codes.MD_REJECT_NON_MONOTONIC,
        )
        assert classifier.classify_tick(variant(t4, exch_ts_ns=t0 + 2 * s, bid_vol1=9), TD) == (
            TickMode.REJECTED,
            codes.MD_REJECT_NON_MONOTONIC,
        )
        assert classifier.classify_tick(variant(t4, exch_ts_ns=t0 + 5 * s, last=3120.0), TD) == (
            TickMode.REJECTED,
            codes.MD_REJECT_PRICE_WITHOUT_TRADE,
        )
        assert classifier.classify_tick(variant(t4, exch_ts_ns=t0 + 6 * s, volume=1020, last=3400.0), TD) == (
            TickMode.REJECTED,
            codes.MD_REJECT_PRICE_LIMIT,
        )
        assert classifier.classify_tick(variant(t4, exch_ts_ns=t0 + 7 * s, volume=1020, last=3118.5), TD) == (
            TickMode.REJECTED,
            codes.MD_REJECT_OFF_TICK,
        )
        # 10:25 is in the commodity morning break (the close grace ends at 10:20)
        assert classifier.classify_tick(variant(t4, exch_ts_ns=bj_ns(2026, 9, 28, 10, 25, 0)), TD) == (
            TickMode.REJECTED,
            codes.MD_REJECT_OUT_OF_SESSION,
        )
        assert classifier.classify_tick(t4, "20260929") == (TickMode.REJECTED, codes.MD_REJECT_TRADING_DAY)
        # rejected states never moved the state: the next real trade is LIVE again
        assert classifier.classify_tick(variant(t4, exch_ts_ns=t0 + 8 * s, volume=1011), TD) == (
            TickMode.LIVE,
            None,
        )

    def test_new_trading_day_resets_state(self, classifier: TickClassifier, rb_tick: Tick) -> None:
        assert classifier.classify_tick(rb_tick, TD)[0] is TickMode.LIVE
        night = variant(rb_tick, trading_day="20260929", exch_ts_ns=bj_ns(2026, 9, 28, 21, 5, 0), volume=10)
        assert classifier.classify_tick(night, "20260929") == (TickMode.LIVE, None)

    def test_unnormalizable_results_are_rejected(self, classifier: TickClassifier) -> None:
        bad = SinaResult("nf_RB2701", "rb2701", 1, reject_reason=codes.MD_REJECT_MALFORMED, detail="x")
        assert classifier.classify(bad, TD) == (TickMode.REJECTED, codes.MD_REJECT_MALFORMED)

    def test_bucket_is_the_receive_time_trading_day(self, normalizer: SinaNormalizer) -> None:
        cal = normalizer.calendar
        assert bucket_trading_day(cal, bj_ns(2026, 9, 25, 12, 0)) == "20260928"  # holiday -> next trading day
        assert bucket_trading_day(cal, bj_ns(2026, 9, 24, 15, 30)) == "20260924"
        assert bucket_trading_day(cal, bj_ns(2026, 9, 28, 20, 30)) == "20260929"  # night -> next trading day
        assert (
            bucket_trading_day(cal, bj_ns(2026, 9, 19, 0, 30)) == "20260921"
        )  # Saturday early morning -> Monday


# ---------------------------------------------------------------------------
# Crash-safe shard writer
# ---------------------------------------------------------------------------


class TestShardWriter:
    def _writer(self, root: Path, clock: SimClock, **kw: Any) -> ParquetShardWriter:
        return ParquetShardWriter(
            root, "sina", TICK_SCHEMA, prefix="part", epoch_ms=EPOCH_MS, clock=clock, **kw
        )

    def test_roll_by_rows_and_read_back(self, tmp_path: Path, rb_tick: Tick) -> None:
        w = self._writer(tmp_path, SimClock(rb_tick.recv_ns), roll_s=60, roll_rows=3)
        written: list[Path] = []
        for i in range(7):
            written += w.append(TD, tick_row(variant(rb_tick, recv_ns=rb_tick.recv_ns + i), TickMode.LIVE))
        assert [p.name for p in written] == [
            f"part-{EPOCH_MS}-000001.parquet",
            f"part-{EPOCH_MS}-000002.parquet",
        ]
        assert w.pending_rows == 1
        written += w.flush()
        assert w.pending_rows == 0 and w.shards_written == 3 and w.rows_written == 7
        assert list_shards(tmp_path, TD) == written
        assert all(p.parent == tmp_path / TD / "sina" for p in written)
        assert not list(tmp_path.rglob("*.tmp"))
        table = pq.read_table(written[0])
        assert table.schema.equals(TICK_SCHEMA, check_metadata=False)
        assert table.schema.metadata[b"futrisk.kind"] == b"ticks.raw"
        assert pq.ParquetFile(written[0]).metadata.row_group(0).column(0).compression == "ZSTD"
        assert table.column("recv_ns").to_pylist() == [rb_tick.recv_ns + i for i in range(3)]
        row = table.slice(0, 1).to_pylist()[0]
        assert (
            row["instrument_id"] == "rb2701"
            and row["mode"] == "LIVE"
            and row["fingerprint"] == rb_tick.fingerprint
        )

    def test_roll_by_time(self, tmp_path: Path, rb_tick: Tick) -> None:
        clock = SimClock(rb_tick.recv_ns)
        w = self._writer(tmp_path, clock, roll_s=10, roll_rows=1000)
        w.append(TD, tick_row(rb_tick, TickMode.LIVE))
        clock.advance(5 * NS_PER_S)
        assert w.roll_if_due() == []
        clock.advance(6 * NS_PER_S)
        assert len(w.roll_if_due()) == 1 and w.pending_rows == 0

    def test_trading_day_change_closes_the_old_shard(self, tmp_path: Path, rb_tick: Tick) -> None:
        w = self._writer(tmp_path, SimClock(rb_tick.recv_ns))
        assert w.append(TD, tick_row(rb_tick, TickMode.LIVE)) == []
        [closed] = w.append("20260929", tick_row(rb_tick, TickMode.REJECTED, codes.MD_REJECT_TRADING_DAY))
        assert closed.parent == tmp_path / TD / "sina"
        [second] = w.flush()
        assert second.parent == tmp_path / "20260929" / "sina"
        assert list_trading_days(tmp_path) == [TD, "20260929"]

    def test_failed_write_keeps_rows_and_leaves_no_partial_file(
        self, tmp_path: Path, rb_tick: Tick, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        clock = SimClock(rb_tick.recv_ns)
        w = self._writer(tmp_path, clock)
        w.append(TD, tick_row(rb_tick, TickMode.LIVE))
        w.append(TD, tick_row(rb_tick, TickMode.DUPLICATE))
        monkeypatch.setattr(recorder_mod.pq, "write_table", boom)
        with pytest.raises(ShardWriteError) as err:
            w.flush()
        assert isinstance(err.value, OSError) and err.value.failures[0][0] == TD
        assert w.pending_rows == 2 and w.consecutive_failures == 1
        assert list_shards(tmp_path, TD) == [] and not list(tmp_path.rglob("*.tmp"))
        monkeypatch.undo()
        assert w.flush() == []  # still inside the 1 s retry backoff: nothing is attempted
        clock.advance(NS_PER_S)
        [path] = w.flush()
        assert path.name == f"part-{EPOCH_MS}-000001.parquet"
        assert pq.read_table(path).num_rows == 2 and w.consecutive_failures == 0

    def test_retry_backoff_does_not_rewrite_on_every_append(
        self, tmp_path: Path, rb_tick: Tick, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        attempts = 0

        def failing_write(*args: Any, **kwargs: Any) -> None:
            nonlocal attempts
            attempts += 1
            raise OSError("disk full")

        clock = SimClock(rb_tick.recv_ns)
        w = self._writer(tmp_path, clock, roll_rows=1)  # every append makes the bucket due
        monkeypatch.setattr(recorder_mod.pq, "write_table", failing_write)
        with pytest.raises(ShardWriteError):
            w.append(TD, tick_row(rb_tick, TickMode.LIVE))
        for _ in range(20):
            w.append(TD, tick_row(rb_tick, TickMode.DUPLICATE))  # backing off: no write, no error
        assert attempts == 1 and w.pending_rows == 21
        clock.advance(NS_PER_S)
        with pytest.raises(ShardWriteError):
            w.append(TD, tick_row(rb_tick, TickMode.DUPLICATE))
        assert attempts == 2  # next attempt only after the backoff (now 2 s)

    def test_vanished_day_folder_is_recreated(self, tmp_path: Path, rb_tick: Tick) -> None:
        w = self._writer(tmp_path, SimClock(rb_tick.recv_ns))
        w.append(TD, tick_row(rb_tick, TickMode.LIVE))
        [first] = w.flush()
        shutil.rmtree(tmp_path / TD)  # e.g. the user moved the day folder away mid-run
        w.append(TD, tick_row(rb_tick, TickMode.DUPLICATE))
        [second] = w.flush()
        assert second.parent == tmp_path / TD / "sina" and second.exists() and not first.exists()

    def test_failing_bucket_does_not_block_another(self, tmp_path: Path, rb_tick: Tick) -> None:
        (tmp_path / TD).write_text("a file where the day folder should be", encoding="utf-8")
        clock = SimClock(rb_tick.recv_ns)
        w = self._writer(tmp_path, clock, roll_s=5)
        w.append(TD, tick_row(rb_tick, TickMode.LIVE))
        with pytest.raises(ShardWriteError) as err:  # the day rolls: closing out TD fails ...
            w.append("20260929", tick_row(rb_tick, TickMode.LIVE))
        assert [bucket for bucket, _ in err.value.failures] == [TD]
        [written] = w.flush("20260929")  # ... but the healthy bucket writes at once (no shared backoff)
        assert written.parent == tmp_path / "20260929" / "sina"
        assert w.pending_rows == 1  # TD's row is kept
        # both buckets due in one roll: TD fails again, 20260929 is still written by the same call
        w.append("20260929", tick_row(rb_tick, TickMode.DUPLICATE))
        clock.advance(10 * NS_PER_S)
        with pytest.raises(ShardWriteError) as err:
            w.roll_if_due()
        assert [bucket for bucket, _ in err.value.failures] == [TD]
        [written2] = err.value.written
        assert written2.parent == tmp_path / "20260929" / "sina" and w.pending_rows == 1

    def test_spill_after_persistent_failures_then_recover(self, tmp_path: Path, rb_tick: Tick) -> None:
        raw, spill = tmp_path / "raw", tmp_path / "spill"
        raw.write_text("not a directory", encoding="utf-8")  # the primary root is unusable
        clock = SimClock(rb_tick.recv_ns)
        w = self._writer(raw, clock, spill_roots=[spill], spill_after=3)
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
        # while the primary backs off, due shards go straight to the spill root
        w.append(TD, tick_row(rb_tick, TickMode.DUPLICATE))
        [spilled2] = w.flush()
        assert spilled2.parent == spill / TD / "sina" and spilled2.name != spilled.name
        # the disk comes back: after the backoff the primary is used again
        raw.unlink()
        clock.advance(60 * NS_PER_S)
        w.append(TD, tick_row(rb_tick, TickMode.DUPLICATE))
        [primary] = w.flush()
        assert primary.parent == raw / TD / "sina" and w.consecutive_failures == 0

    def test_final_close_spills_immediately_with_fallback_roots(self, tmp_path: Path, rb_tick: Tick) -> None:
        raw, bad_spill, good_spill = tmp_path / "raw", tmp_path / "bad", tmp_path / "good"
        raw.write_text("x", encoding="utf-8")
        bad_spill.write_text("x", encoding="utf-8")
        w = self._writer(raw, SimClock(rb_tick.recv_ns), spill_roots=[bad_spill, good_spill])
        w.append(TD, tick_row(rb_tick, TickMode.LIVE))
        [path] = w.close()
        assert path.parent == good_spill / TD / "sina" and w.pending_rows == 0

    def test_close_raises_when_nothing_is_writable(self, tmp_path: Path, rb_tick: Tick) -> None:
        raw, spill = tmp_path / "raw", tmp_path / "spill"
        raw.write_text("x", encoding="utf-8")
        spill.write_text("x", encoding="utf-8")
        w = self._writer(raw, SimClock(rb_tick.recv_ns), spill_roots=[spill])
        w.append(TD, tick_row(rb_tick, TickMode.LIVE))
        with pytest.raises(ShardWriteError):
            w.close()
        assert w.pending_rows == 1  # kept: never dropped silently

    def test_memory_is_bounded_and_drops_are_counted(self, tmp_path: Path, rb_tick: Tick) -> None:
        raw = tmp_path / "raw"
        raw.write_text("x", encoding="utf-8")
        w = self._writer(raw, SimClock(rb_tick.recv_ns), roll_rows=10**6, max_pending_rows=10)
        for _ in range(60):
            w.append(TD, tick_row(rb_tick, TickMode.DUPLICATE))
        assert w.pending_rows <= 40  # 4x the soft cap at most
        assert w.dropped_rows == 60 - w.pending_rows > 0

    def test_temp_files_are_ignored_and_stale_ones_removed(self, tmp_path: Path, rb_tick: Tick) -> None:
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
        w = self._writer(tmp_path, clock)
        w.append(TD, tick_row(rb_tick, TickMode.LIVE))
        [path] = w.flush()
        assert fresh.exists() and not stale.exists()
        assert list_shards(tmp_path, TD) == [path]


# ---------------------------------------------------------------------------
# Recorder round trip
# ---------------------------------------------------------------------------


class TestSinaRecorder:
    def test_round_trip_with_wire_archive_and_rebuild(
        self,
        tmp_path: Path,
        normalizer: SinaNormalizer,
        sina_bytes: dict[str, bytes],
        payloads: dict[str, str],
    ) -> None:
        t0 = bj_ns(2026, 9, 24, 15, 15, 30)
        clock = SimClock(t0)
        raw, wire = tmp_path / "raw", tmp_path / "wire"
        rec = SinaRecorder(raw, normalizer, wire_dir=wire, clock=clock, epoch_ms=EPOCH_MS)
        codes_ = tuple(normalizer.codes)
        body = sina_bytes["all_configured.txt"]
        half = 1_500_000_000

        out = rec.record_poll(make_poll(body, t0, codes_, 1))
        # closing snapshots (15:00, DCE 15:04:39, T 15:15) all fall inside the close grace
        assert [c.mode for c in out] == [TickMode.LIVE] * 11
        out = rec.record_poll(make_poll(body, t0 + half, codes_, 2))
        assert [c.mode for c in out] == [TickMode.DUPLICATE] * 11
        out = rec.record_poll(
            make_poll(sina_bytes["forbidden_no_referer.txt"], t0 + 2 * half, codes_, 3, 403, "HTTP 403")
        )
        assert out == []
        # rb2705 comes back empty (e.g. delisted overnight)
        text = decode_body(body).replace(f'hq_str_nf_RB2705="{payloads["nf_RB2705"]}"', 'hq_str_nf_RB2705=""')
        out = rec.record_poll(make_poll(text.encode("gb18030"), t0 + 3 * half, codes_, 4))
        modes = {c.result.instrument_id: (c.mode, c.reject_reason) for c in out}
        assert modes["rb2705"] == (TickMode.REJECTED, codes.MD_REJECT_MALFORMED)
        assert modes["rb2701"] == (TickMode.DUPLICATE, None)
        assert rec.pending_rows == 33 and rec.stats.shards == 0
        rec.close()

        st = rec.stats
        assert (st.polls, st.failed_polls, st.rows, st.shards, st.wire_rows, st.wire_shards) == (
            4,
            1,
            33,
            1,
            4,
            1,
        )
        assert st.symbols["nf_RB2701"].count(TickMode.LIVE) == 1
        assert st.symbols["nf_RB2701"].count(TickMode.DUPLICATE) == 2
        assert st.symbols["nf_RB2705"].last_reason == codes.MD_REJECT_MALFORMED
        assert st.last_shard == raw / "20260924" / "sina" / f"part-{EPOCH_MS}-000001.parquet"

        df = read_raw_ticks(raw, "20260924")
        assert len(df) == 33 and list(df.columns) == list(TICK_SCHEMA.names)
        assert df["recv_ns"].is_monotonic_increasing
        assert df["mode"].astype(str).value_counts().to_dict() == {"DUPLICATE": 21, "LIVE": 11, "REJECTED": 1}
        assert str(df["volume"].dtype) == "Int64" and str(df["fingerprint"].dtype) == "UInt64"
        bad = df[df["mode"] == "REJECTED"].iloc[0]
        assert (bad["instrument_id"], bad["exchange"], bad["trading_day"]) == ("rb2705", "SHFE", "20260924")
        assert (
            bad["reject_reason"] == codes.MD_REJECT_MALFORMED
            and pd.isna(bad["volume"])
            and pd.isna(bad["last"])
        )
        rb = df[(df["instrument_id"] == "rb2701") & (df["mode"] == "LIVE")].iloc[0]
        assert (rb["last"], rb["volume"], rb["flags"], rb["source"]) == (3118.0, 570630, 1, "SINA")
        assert rb["exch_ts_ns"] == bj_ns(2026, 9, 24, 15, 0, 0) and rb["recv_ns"] == t0
        assert pd.isna(rb["source_seq"]) and rb["epoch"] is None

        w = read_wire(wire, "20260924")
        assert w["http_status"].tolist() == [200, 200, 403, 200]
        assert w["body"].iloc[0] == body and w["body"].iloc[2] == b"Forbidden"
        assert w["codes"].astype(str).iloc[0] == ",".join(codes_)

        report = rebuild_ticks_from_wire(wire, tmp_path / "rebuilt", "20260924", normalizer, clock=clock)
        assert (report.polls, report.rows, len(report.shards)) == (3, 33, 1)
        again = read_raw_ticks(tmp_path / "rebuilt", "20260924")
        cols = ["recv_ns", "instrument_id", "mode", "reject_reason", "fingerprint", "last", "volume"]
        pd.testing.assert_frame_equal(
            again[cols].astype(str).reset_index(drop=True), df[cols].astype(str).reset_index(drop=True)
        )

    def test_stale_snapshot_before_the_open(
        self, tmp_path: Path, normalizer: SinaNormalizer, sina_bytes: dict[str, bytes]
    ) -> None:
        recv = bj_ns(2026, 9, 28, 8, 50, 0)
        rec = SinaRecorder(tmp_path, normalizer, clock=SimClock(recv), epoch_ms=EPOCH_MS)
        out = rec.record_poll(make_poll(sina_bytes["all_configured.txt"], recv, tuple(normalizer.codes)))
        assert {(c.mode, c.reject_reason) for c in out} == {(TickMode.REJECTED, codes.MD_REJECT_TRADING_DAY)}
        rec.close()
        assert list_trading_days(tmp_path) == [TD]  # filed under the receive-time trading day
        df = read_raw_ticks(tmp_path, TD)
        assert set(df["trading_day"].astype(str)) == {"20260924"} and df["last"].notna().all()

    def test_write_errors_are_logged_not_raised(
        self,
        tmp_path: Path,
        normalizer: SinaNormalizer,
        sina_bytes: dict[str, bytes],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        t0 = bj_ns(2026, 9, 24, 15, 15, 30)
        rec = SinaRecorder(tmp_path, normalizer, clock=SimClock(t0), roll_rows=5, epoch_ms=EPOCH_MS)
        monkeypatch.setattr(recorder_mod.pq, "write_table", boom)
        rec.record_poll(make_poll(sina_bytes["all_configured.txt"], t0, tuple(normalizer.codes)))
        assert rec.stats.write_errors >= 1 and rec.pending_rows == 11
        monkeypatch.undo()
        rec.close()
        assert len(read_raw_ticks(tmp_path, "20260924")) == 11

    def test_read_helpers_on_missing_days(self, tmp_path: Path) -> None:
        assert read_raw_ticks(tmp_path, "20260928").empty
        assert list(read_raw_ticks(tmp_path, "20260928").columns) == list(TICK_SCHEMA.names)
        assert list_trading_days(tmp_path / "nope") == []

    def test_close_reports_failure_loudly(
        self, tmp_path: Path, normalizer: SinaNormalizer, sina_bytes: dict[str, bytes]
    ) -> None:
        raw, spill = tmp_path / "raw", tmp_path / "spill"
        raw.write_text("x", encoding="utf-8")
        spill.write_text("x", encoding="utf-8")
        t0 = bj_ns(2026, 9, 24, 15, 15, 30)
        rec = SinaRecorder(raw, normalizer, spill_dirs=[spill], clock=SimClock(t0), epoch_ms=EPOCH_MS)
        rec.record_poll(make_poll(sina_bytes["all_configured.txt"], t0, tuple(normalizer.codes)))
        assert rec.close() is False  # logged CRITICAL; the rows are still counted as pending
        assert rec.pending_rows == 11 and rec.stats.write_errors >= 1

    def test_close_falls_back_to_spill(
        self, tmp_path: Path, normalizer: SinaNormalizer, sina_bytes: dict[str, bytes]
    ) -> None:
        raw, spill = tmp_path / "raw", tmp_path / "spill"
        raw.write_text("x", encoding="utf-8")
        t0 = bj_ns(2026, 9, 24, 15, 15, 30)
        rec = SinaRecorder(
            raw,
            normalizer,
            wire_dir=tmp_path / "wire",
            spill_dirs=[spill],
            clock=SimClock(t0),
            epoch_ms=EPOCH_MS,
        )
        rec.record_poll(make_poll(sina_bytes["all_configured.txt"], t0, tuple(normalizer.codes)))
        assert rec.close() is True
        assert rec.stats.spilled_shards == 1 and rec.stats.last_spill is not None
        assert len(read_raw_ticks(spill, "20260924")) == 11  # a normal shard, readable as usual
        assert len(read_wire(tmp_path / "wire", "20260924")) == 1  # the healthy wire directory was used

    def test_emergency_flush_from_another_thread(
        self, tmp_path: Path, normalizer: SinaNormalizer, sina_bytes: dict[str, bytes]
    ) -> None:
        t0 = bj_ns(2026, 9, 24, 15, 15, 30)
        rec = SinaRecorder(
            tmp_path, normalizer, wire_dir=tmp_path / "w", clock=SimClock(t0), epoch_ms=EPOCH_MS
        )
        rec.record_poll(make_poll(sina_bytes["all_configured.txt"], t0, tuple(normalizer.codes)))
        assert rec.pending_rows == 11
        result: list[bool] = []
        thread = threading.Thread(target=lambda: result.append(rec.emergency_flush(timeout_s=2.0)))
        thread.start()
        thread.join(5)
        assert result == [True] and rec.pending_rows == 0 and rec.pending_wire_rows == 0
        assert len(read_raw_ticks(tmp_path, "20260924")) == 11

    def test_emergency_flush_gives_up_when_the_lock_is_busy(
        self, tmp_path: Path, normalizer: SinaNormalizer
    ) -> None:
        rec = SinaRecorder(tmp_path, normalizer, clock=SimClock(0), epoch_ms=EPOCH_MS)
        rec._lock.acquire()  # the main thread is (hypothetically) stuck inside the recorder
        try:
            result: list[bool] = []
            thread = threading.Thread(target=lambda: result.append(rec.emergency_flush(timeout_s=0.1)))
            thread.start()
            thread.join(5)
            assert result == [False]
        finally:
            rec._lock.release()

    def test_cu_saturday_early_morning_is_live_for_monday(
        self, tmp_path: Path, normalizer: SinaNormalizer, payloads: dict[str, str]
    ) -> None:
        # Friday 2026-09-18 night session of cu runs 21:00 -> Saturday 01:00 and belongs to Monday 09-21
        payload = set_fields(payloads["nf_CU2611"], {1: "003000", 17: "2026-09-19"})
        body = f'var hq_str_nf_CU2611="{payload}";\n'.encode("gb18030")
        recv = bj_ns(2026, 9, 19, 0, 30, 1)
        rec = SinaRecorder(tmp_path, normalizer, clock=SimClock(recv), epoch_ms=EPOCH_MS)
        [out] = rec.record_poll(make_poll(body, recv, ("nf_CU2611",)))
        assert out.result.tick is not None and out.result.tick.trading_day == "20260921"
        assert (out.mode, out.reject_reason) == (TickMode.LIVE, None)
        rec.close()
        assert list_trading_days(tmp_path) == ["20260921"]

    def test_future_dated_quote_is_rejected_but_fully_recorded(
        self, tmp_path: Path, normalizer: SinaNormalizer, payloads: dict[str, str]
    ) -> None:
        payload = set_fields(payloads["nf_RB2701"], {1: "104000", 17: "2026-09-28", 14: "1000"})
        body = f'var hq_str_nf_RB2701="{payload}";\n'.encode("gb18030")
        recv = bj_ns(2026, 9, 28, 10, 30, 0)  # the quote claims to be 10 minutes in the future
        rec = SinaRecorder(tmp_path, normalizer, clock=SimClock(recv), epoch_ms=EPOCH_MS)
        [out] = rec.record_poll(make_poll(body, recv, ("nf_RB2701",)))
        assert (out.mode, out.reject_reason) == (TickMode.REJECTED, codes.MD_REJECT_FUTURE_TS)
        assert out.result.detail.startswith("future_ts")
        assert rec.stats.symbols["nf_RB2701"].last_detail is not None
        rec.close()
        row = read_raw_ticks(tmp_path, TD).iloc[0]
        assert row["mode"] == "REJECTED" and row["last"] == 3118.0 and row["volume"] == 1000
