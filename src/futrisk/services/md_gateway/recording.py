"""Tick recording: raw shards per source (plus an optional wire archive) and canonical day files (03 §5.1).

**Raw**: every received, normalized state with its ``mode`` (LIVE, BACKFILL, REJECTED,
DUPLICATE) and ``reject_reason``; the bucket is the trading day of the *receive* time. The
event loop only appends ``(tick, mode, reason, epoch)`` tuples (bounded by
``max_pending_rows``: beyond it rows are dropped, counted and logged); every hand-off they go to
one recorder thread that builds the rows and feeds the :class:`~.shards.ParquetShardWriter` of
their source (crash-safe shards, roll every 5 s or 10k rows, spill directory on write errors).
The optional ``wire_dir`` archives raw HTTP responses the same way (the Sina recorder CLI).

**Cursor durability**: with each hand-off the service passes the sources' cursor marks; a
mark becomes *durable* only once the source's writer has no pending rows (every row up to it
is in a closed shard), and only durable marks are persisted (03 §4.1 ``feed_cursors``: the
cursor never runs ahead of the recording). A source that lost rows never gets a durable mark
again in this process. Rows written after the last persisted cursor are replayed after a
restart, so the raw files can hold a state twice.

**Canonical** (``ticks/canonical/{trading_day}.parquet``): LIVE + BACKFILL rows of the day
(primary and spill folders, plus the next day's for late replays), deduplicated by
``(instrument_id, fingerprint, exch_ts_ns)`` keeping LIVE first, then the higher source
priority; second-resolution (Sina) rows are dropped when an exact source delivered the same
state. Sorted by ``order_key``; rebuilt when the list of raw shards changes (file metadata).
"""

from __future__ import annotations

import asyncio
import json
import os
import threading
import time
from collections.abc import Callable, Iterable, Mapping
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, TypeVar

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from loguru import logger

from futrisk.common.clock import Clock
from futrisk.messages.enums import SourceId, TickMode
from futrisk.messages.md import FLAG_COARSE_TS, Tick
from futrisk.services.md_gateway.shards import (
    TICK_COLUMNS,
    TICK_PREFIX,
    TICK_SCHEMA,
    WIRE_PREFIX,
    WIRE_SCHEMA,
    ParquetShardWriter,
    ShardWriteError,
    list_shards,
    read_shards,
    reject_row,
    tick_row,
)

T = TypeVar("T")
_SOURCES_META = b"futrisk.sources"
_PANDAS_TYPES: dict[pa.DataType, Any] = {pa.int64(): pd.Int64Dtype(), pa.int32(): pd.Int32Dtype(), pa.uint64(): pd.UInt64Dtype()}
_DROP_LOG_INTERVAL_S = 10.0

Pending = tuple[str, Any]  # (bucket, (tick, mode, reason, epoch) | row dict)


class TickRecorder:
    """Raw shard writers of every source (and the optional wire archive) behind a single recorder thread."""

    def __init__(
        self,
        raw_dir: Path,
        *,
        sources: Iterable[SourceId],
        wall_clock: Clock,
        epoch_ms: int,
        roll_s: float = 5.0,
        roll_rows: int = 10_000,
        spill_dirs: Iterable[Path] = (),
        max_pending_rows: int = 500_000,
        wire_dir: Path | None = None,
    ) -> None:
        self.raw_dir = Path(raw_dir)
        self.spill_dirs = [Path(p) for p in spill_dirs]

        def writer(root: Path, src: SourceId, schema: pa.Schema, prefix: str) -> ParquetShardWriter:
            return ParquetShardWriter(
                root, src.lower(), schema, prefix=prefix, epoch_ms=epoch_ms, clock=wall_clock, roll_s=roll_s, roll_rows=roll_rows,
                spill_roots=self.spill_dirs,
            )

        self._writers = {src: writer(self.raw_dir, src, TICK_SCHEMA, TICK_PREFIX) for src in sources}
        self._wire = {src: writer(wire_dir, src, WIRE_SCHEMA, WIRE_PREFIX) for src in self._writers} if wire_dir is not None else {}
        self._pending: dict[SourceId, list[Pending]] = {src: [] for src in self._writers}
        self._pending_wire: list[tuple[SourceId, str, dict[str, Any]]] = []
        self._pending_n = 0
        self._max_pending = max(1, max_pending_rows)
        self._lock = threading.Lock()  # loop buffer: appended by the loop, taken by hand-offs and the emergency flush
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="md-recorder")
        self.durable: dict[SourceId, Any] = {}
        self.rows_in = 0
        self.wire_rows = 0
        self.write_errors = 0
        self.writer_pending = 0
        self.dropped_rows = 0
        self.shards_written = 0
        self.spilled_shards = 0
        self.last_shard: Path | None = None
        self.last_spill: Path | None = None
        self.lossy: set[SourceId] = set()
        self._last_drop_log = 0.0
        self._closed = False

    # ---------------------------------------------------------------- loop side
    def add_tick(self, source: SourceId, bucket: str, tick: Tick, mode: TickMode, reason: str | None, epoch: str | None) -> None:
        """Buffer one normalized state (the row is built on the recorder thread)."""
        self._put(source, bucket, (tick, mode, reason, epoch))

    def add(self, source: SourceId, bucket: str, row: dict[str, Any]) -> None:
        """Buffer one ready-made row (records that could not be normalized)."""
        self._put(source, bucket, row)

    def add_wire(self, source: SourceId, bucket: str, row: dict[str, Any]) -> None:
        """Archive one raw poll (``wire_dir`` only)."""
        if self._wire:
            with self._lock:
                self._pending_wire.append((source, bucket, row))
                self.wire_rows += 1

    def _put(self, source: SourceId, bucket: str, item: Any) -> None:
        with self._lock:
            if self._pending_n < self._max_pending:
                self._pending.setdefault(source, []).append((bucket, item))
                self._pending_n += 1
                self.rows_in += 1
                return
            self.dropped_rows += 1
            self.lossy.add(source)
        now = time.monotonic()
        if now - self._last_drop_log >= _DROP_LOG_INTERVAL_S:
            self._last_drop_log = now
            logger.critical("md.recorder.rows_dropped", source=str(source), dropped=self.dropped_rows, pending=self._pending_n)

    @property
    def pending_rows(self) -> int:
        return self._pending_n

    def buffered_rows(self) -> int:
        """Rows accepted but not yet on disk (loop buffer + writer buffers as of the last hand-off)."""
        return self._pending_n + self.writer_pending

    def _take(self) -> tuple[dict[SourceId, list[Pending]], list[tuple[SourceId, str, dict[str, Any]]]]:
        with self._lock:
            batch = {src: rows for src, rows in self._pending.items() if rows}
            wire = self._pending_wire
            self._pending = {src: [] for src in self._writers}
            self._pending_wire = []
            self._pending_n = 0
        return batch, wire

    async def handoff(self, marks: Mapping[SourceId, Any], *, flush: bool = False) -> None:
        """Move buffered rows to the recorder thread; ``flush`` writes shards now."""
        if not self._closed:  # shielded: rows already taken from the buffer must be written even if the caller is cancelled
            await asyncio.shield(asyncio.get_running_loop().run_in_executor(self._executor, self._write, *self._take(), dict(marks), flush, False))

    async def run(self, fn: Callable[[], T]) -> T:
        """Run ``fn`` on the recorder thread, after every hand-off queued so far."""
        return await asyncio.get_running_loop().run_in_executor(self._executor, fn)

    async def close(self, marks: Mapping[SourceId, Any]) -> bool:
        """Final flush (primary directory, then spill directories) and stop the thread; False when rows could not be written."""
        if self._closed:
            return self.writer_pending == 0
        try:
            await asyncio.shield(asyncio.get_running_loop().run_in_executor(self._executor, self._write, *self._take(), dict(marks), True, True))
        finally:
            self._closed = True
            self._executor.shutdown(wait=False)  # jobs queued behind the final write finish on the recorder thread
        return self.writer_pending == 0

    def emergency_flush(self, timeout_s: float = 3.0) -> bool:
        """Final flush from another thread (console close / logoff); gives up after ``timeout_s``."""
        if self._closed:
            return True
        try:
            self._executor.submit(self._write, *self._take(), {}, True, True).result(timeout_s)
        except Exception as exc:
            logger.critical(f"md.recorder.emergency_flush failed: {exc!r}")
            return False
        return self.writer_pending == 0

    # -------------------------------------------------------------- thread side
    def _write(
        self, batch: dict[SourceId, list[Pending]], wire: list[tuple[SourceId, str, dict[str, Any]]], marks: dict[SourceId, Any], flush: bool, final: bool
    ) -> None:
        for src, items in batch.items():
            w = self._writers.get(src)
            if w is None:
                continue
            for bucket, item in items:
                self._guard(src, w.append, bucket, tick_row(*item) if isinstance(item, tuple) else item)
        for src, bucket, row in wire:
            self._guard(src, self._wire[src].append, bucket, row)
        for src, w in self._writers.items():
            self._guard(src, w.close if final else w.flush if flush else w.roll_if_due)
            if w.dropped_rows:
                self.lossy.add(src)
            if w.pending_rows == 0 and src in marks and src not in self.lossy:
                self.durable[src] = marks[src]
        for src, w in self._wire.items():
            self._guard(src, w.close if final else w.flush if flush else w.roll_if_due)
        writers = [*self._writers.values(), *self._wire.values()]
        self.writer_pending = sum(w.pending_rows for w in writers)  # read by the loop (heartbeat)
        self.shards_written = sum(w.shards_written for w in writers)
        self.spilled_shards = sum(w.spilled_shards for w in writers)
        self.last_shard = next((w.last_shard for w in self._writers.values() if w.last_shard is not None), None)
        self.last_spill = next((w.last_spill for w in writers if w.last_spill is not None), None)

    def _guard(self, src: SourceId, fn: Callable[..., Any], *args: Any) -> None:
        try:
            fn(*args)
        except ShardWriteError as exc:
            self.write_errors += 1
            logger.error("md.recorder.write_failed", source=str(src), error=str(exc))
        except Exception:
            self.write_errors += 1
            logger.exception("md.recorder.write_failed", source=str(src))


# ---------------------------------------------------------------- canonical day files
def day_shards(
    raw_dir: Path, trading_day: str, sources: Iterable[SourceId], next_day: str | None = None, spill_dirs: Iterable[Path] = ()
) -> list[Path]:
    """Raw shards of ``trading_day`` (plus the next day's folder, where late replays land), spill directories included."""
    srcs = list(sources)
    roots = [Path(raw_dir), *(Path(p) for p in spill_dirs)]
    days = [trading_day] + ([next_day] if next_day else [])
    return [p for td in days for root in roots for src in srcs for p in list_shards(root, td, src.lower(), TICK_PREFIX)]


def _signature(paths: Iterable[Path], root: Path) -> list[list[Any]]:
    sig: list[list[Any]] = []
    for p in paths:
        try:
            size = p.stat().st_size
        except OSError:
            continue
        try:
            name = p.relative_to(root).as_posix()
        except ValueError:
            name = p.as_posix()  # a spill directory outside the raw root
        sig.append([name, size])
    return sorted(sig)


def canonical_path(canonical_dir: Path, trading_day: str) -> Path:
    return Path(canonical_dir) / f"{trading_day}.parquet"


def canonical_fresh(path: Path, shards: list[Path], raw_dir: Path) -> bool:
    if not path.exists():
        return False
    try:
        stored = (pq.read_schema(path).metadata or {}).get(_SOURCES_META)
    except Exception:
        return False
    return stored is not None and json.loads(stored) == _signature(shards, raw_dir)


def build_canonical(
    raw_dir: Path,
    canonical_dir: Path,
    trading_day: str,
    sources: Iterable[SourceId],
    priority: Mapping[str, int],
    *,
    next_day: str | None = None,
    force: bool = False,
    spill_dirs: Iterable[Path] = (),
) -> Path:
    """Build (or keep, when up to date) ``ticks/canonical/{trading_day}.parquet``; blocking."""
    raw_dir = Path(raw_dir)
    shards = day_shards(raw_dir, trading_day, sources, next_day, spill_dirs)
    out = canonical_path(canonical_dir, trading_day)
    if not force and canonical_fresh(out, shards, raw_dir):
        return out
    table = read_shards(shards, TICK_SCHEMA)
    if table.num_rows:
        mode = pc.cast(table["mode"], pa.string())
        td_col = pc.cast(table["trading_day"], pa.string())
        table = table.filter(pc.and_(pc.is_in(mode, value_set=pa.array([str(TickMode.LIVE), str(TickMode.BACKFILL)])), pc.equal(td_col, trading_day)))
    df = table.to_pandas(types_mapper=_PANDAS_TYPES.get)
    if len(df):
        for col in ("instrument_id", "source", "mode"):
            df[col] = df[col].astype(str)
        df["_live"] = df["mode"] != str(TickMode.LIVE)
        df["_coarse"] = (df["flags"].fillna(0).astype("int64") & FLAG_COARSE_TS) != 0
        df["_prio"] = df["source"].map(lambda s: priority.get(s, 99))
        df = df.sort_values(["instrument_id", "fingerprint", "exch_ts_ns", "_live", "_coarse", "_prio", "recv_ns"], kind="stable")
        df = df.drop_duplicates(["instrument_id", "fingerprint", "exch_ts_ns"], keep="first")
        if bool(df["_coarse"].any()):
            key = df["instrument_id"] + ":" + df["fingerprint"].astype(str)
            df = df[~(df["_coarse"] & key.isin(set(key[~df["_coarse"]])))]
        df = df.sort_values(["instrument_id", "volume", "exch_ts_ns", "_prio"], kind="stable")[list(TICK_COLUMNS)]
    else:
        df = pd.DataFrame({c: [] for c in TICK_COLUMNS})
    meta = {**(TICK_SCHEMA.metadata or {}), _SOURCES_META: json.dumps(_signature(shards, raw_dir)).encode("utf-8")}
    result = pa.Table.from_pandas(df, schema=TICK_SCHEMA.with_metadata(meta), preserve_index=False)
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_name(f".{out.name}.tmp")
    with open(tmp, "wb") as fh:
        pq.write_table(result, fh, compression="zstd")
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, out)
    logger.info("md.canonical.built", trading_day=trading_day, rows=result.num_rows, shards=len(shards))
    return out


def read_canonical(path: Path) -> pd.DataFrame:
    return pq.read_table(path).to_pandas(types_mapper=_PANDAS_TYPES.get)


__all__ = ["TickRecorder", "build_canonical", "canonical_fresh", "canonical_path", "day_shards", "read_canonical", "reject_row", "tick_row"]
