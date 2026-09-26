"""Tick recording: raw shards per source, canonical day files, tick queries (03 §5.1, 02 §6.2 md.ticks).

**Raw** (``ticks/raw/{trading_day}/{source}/part-{epoch_ms}-{n}.parquet``): every received,
normalized state with its ``mode`` (LIVE, BACKFILL, QUARANTINED, REJECTED, DUPLICATE) and
``reject_reason``. The event loop only appends ``(tick, mode, reason, epoch)`` tuples; every
``record_handoff_s`` they are handed to one recorder thread, which builds the rows and
appends them to :class:`~futrisk.marketdata.recorder.ParquetShardWriter` (crash-safe
shards, roll every 5 s or 10k rows, spill directory on write errors). The bucket is the
trading day of the *receive* time, like the stand-alone recorder. The loop-side buffer is
bounded (``recorder_max_pending_rows``): beyond it rows are dropped, counted and logged.

**Cursor durability**: with each hand-off the service passes the sources' cursor marks
(Feed A ``(epoch, contiguous seq)``); a mark becomes *durable* only once the source's
writer has no pending rows, i.e. every row up to it is in a closed shard. Only durable
marks are persisted to ``feed_cursors`` (03 §4.1: the cursor never runs ahead of the
recording, so a crash always resumes before the first lost row). A source that lost rows
(loop buffer full, or the writer dropping rows because no directory is writable) never
gets a durable mark again in this process: after a restart it resumes from the last
cursor that really was on disk, and the dropped range is replayed.

**Duplicates after a crash**: rows written after the last persisted cursor are replayed
after the restart (the resume starts at the cursor), so the raw files can hold the same
state twice (the second copy usually as BACKFILL or DUPLICATE). The canonical file removes
them; raw consumers (``md.ticks``) may see both.

**Canonical** (``ticks/canonical/{trading_day}.parquet``): LIVE + BACKFILL rows of the day
(read from the day's folders - primary and spill - and the next day's, for late replays),
deduplicated by ``(instrument_id, fingerprint, exch_ts_ns)`` keeping LIVE first, then the
higher source priority; rows of a second-resolution source (Sina) are dropped when a
source with exact timestamps delivered the same state. A quote that reverted to an earlier
state (same fingerprint, later time) is a distinct state and kept. Sorted by
``order_key``. The list of raw shards it was built from is stored in the file's schema
metadata; the file is rebuilt when that list changes.
"""

from __future__ import annotations

import asyncio
import json
import os
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
from futrisk.marketdata.recorder import ParquetShardWriter, ShardWriteError, list_shards, read_shards
from futrisk.marketdata.tickschema import TICK_COLUMNS, TICK_SCHEMA
from futrisk.messages.enums import Exchange, SourceId, TickMode
from futrisk.messages.md import FLAG_COARSE_TS, Tick

T = TypeVar("T")
TICK_PREFIX = "part"
_SOURCES_META = b"futrisk.sources"
_PANDAS_TYPES: dict[pa.DataType, Any] = {
    pa.int64(): pd.Int64Dtype(),
    pa.int32(): pd.Int32Dtype(),
    pa.uint64(): pd.UInt64Dtype(),
}
_DROP_LOG_INTERVAL_S = 10.0

Pending = tuple[str, Any]  # (bucket, (tick, mode, reason, epoch) | row dict)


class TickRecorder:
    """Raw shard writers of every source behind a single recorder thread."""

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
    ) -> None:
        self.raw_dir = Path(raw_dir)
        self.spill_dirs = [Path(p) for p in spill_dirs]
        self._writers: dict[SourceId, ParquetShardWriter] = {
            src: ParquetShardWriter(
                self.raw_dir,
                src.lower(),
                TICK_SCHEMA,
                prefix=TICK_PREFIX,
                epoch_ms=epoch_ms,
                clock=wall_clock,
                roll_s=roll_s,
                roll_rows=roll_rows,
                spill_roots=self.spill_dirs,
            )
            for src in sources
        }
        self._pending: dict[SourceId, list[Pending]] = {src: [] for src in self._writers}
        self._pending_n = 0
        self._max_pending = max(1, max_pending_rows)
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="md-recorder")
        self.durable: dict[SourceId, Any] = {}
        self.rows_in = 0
        self.write_errors = 0
        self.writer_pending = 0
        self.dropped_rows = 0
        self.lossy: set[SourceId] = set()
        self._last_drop_log = 0.0
        self._closed = False

    # ---------------------------------------------------------------- loop side
    def add_tick(self, source: SourceId, bucket: str, tick: Tick, mode: TickMode, reason: str | None, epoch: str | None) -> None:
        """Buffer one normalized state (the row is built on the recorder thread)."""
        if self._pending_n >= self._max_pending:
            self._drop(source)
            return
        lst = self._pending.get(source)
        if lst is None:
            lst = self._pending[source] = []
        lst.append((bucket, (tick, mode, reason, epoch)))
        self._pending_n += 1
        self.rows_in += 1

    def add(self, source: SourceId, bucket: str, row: dict[str, Any]) -> None:
        """Buffer one ready-made row (records that could not be normalized)."""
        if self._pending_n >= self._max_pending:
            self._drop(source)
            return
        self._pending.setdefault(source, []).append((bucket, row))
        self._pending_n += 1
        self.rows_in += 1

    def _drop(self, source: SourceId) -> None:
        self.dropped_rows += 1
        self.lossy.add(source)
        now = time.monotonic()
        if now - self._last_drop_log >= _DROP_LOG_INTERVAL_S:
            self._last_drop_log = now
            logger.critical(
                "md.recorder.rows_dropped",
                source=str(source),
                dropped=self.dropped_rows,
                pending=self._pending_n,
                why="the recorder thread cannot keep up; cursors stop advancing",
            )

    @property
    def pending_rows(self) -> int:
        return self._pending_n

    def buffered_rows(self) -> int:
        """Rows accepted but not yet on disk (loop buffer + writer buffers as of the last hand-off)."""
        return self._pending_n + self.writer_pending

    def _take(self) -> dict[SourceId, list[Pending]]:
        batch = {src: rows for src, rows in self._pending.items() if rows}
        self._pending = {src: [] for src in self._writers}
        self._pending_n = 0
        return batch

    async def handoff(self, marks: Mapping[SourceId, Any], *, flush: bool = False) -> None:
        """Move buffered rows to the recorder thread; ``flush`` writes shards now."""
        if self._closed:
            return
        batch = self._take()
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(self._executor, self._write, batch, dict(marks), flush, False)

    async def run(self, fn: Callable[[], T]) -> T:
        """Run ``fn`` on the recorder thread, after every hand-off queued so far."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._executor, fn)

    async def close(self, marks: Mapping[SourceId, Any]) -> None:
        """Final flush (primary directory, then spill directories) and stop the thread (without blocking the loop)."""
        if self._closed:
            return
        batch = self._take()
        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(self._executor, self._write, batch, dict(marks), True, True)
        finally:
            self._closed = True
            # L4: jobs queued behind the final write finish on the recorder thread; never block the loop
            self._executor.shutdown(wait=False)

    # -------------------------------------------------------------- thread side
    def _write(self, batch: dict[SourceId, list[Pending]], marks: dict[SourceId, Any], flush: bool, final: bool) -> None:
        for src, items in batch.items():
            w = self._writers.get(src)
            if w is None:
                continue
            for bucket, item in items:
                row = tick_row(*item) if isinstance(item, tuple) else item
                self._guard(src, w.append, bucket, row)
        for src, w in self._writers.items():
            if final:
                self._guard(src, w.close)
            elif flush:
                self._guard(src, w.flush)
            else:
                self._guard(src, w.roll_if_due)
            if w.dropped_rows:
                self.lossy.add(src)
            if w.pending_rows == 0 and src in marks and src not in self.lossy:
                self.durable[src] = marks[src]
        self.writer_pending = sum(w.pending_rows for w in self._writers.values())  # read by the loop (heartbeat)

    def _guard(self, src: SourceId, fn: Callable[..., Any], *args: Any) -> None:
        try:
            fn(*args)
        except ShardWriteError as exc:
            self.write_errors += 1
            logger.error("md.recorder.write_failed", source=str(src), error=str(exc))
        except Exception:
            self.write_errors += 1
            logger.exception("md.recorder.write_failed", source=str(src))


# ---------------------------------------------------------------- rows
def tick_row(tick: Tick, mode: TickMode, reason: str | None, epoch: str | None) -> dict[str, Any]:
    """Raw row of a normalized state (03 §5.1 columns = ``Tick`` fields + epoch/mode/reject_reason)."""
    return {
        "instrument_id": tick.instrument_id,
        "exchange": str(tick.exchange),
        "trading_day": tick.trading_day,
        "action_day": tick.action_day,
        "exch_ts_ns": tick.exch_ts_ns,
        "recv_ns": tick.recv_ns,
        "last": tick.last,
        "bid1": tick.bid1,
        "ask1": tick.ask1,
        "open": tick.open,
        "high": tick.high,
        "low": tick.low,
        "upper_limit": tick.upper_limit,
        "lower_limit": tick.lower_limit,
        "pre_settle": tick.pre_settle,
        "pre_close": tick.pre_close,
        "avg_price": tick.avg_price,
        "turnover": tick.turnover,
        "volume": tick.volume,
        "open_interest": tick.open_interest,
        "bid_vol1": tick.bid_vol1,
        "ask_vol1": tick.ask_vol1,
        "pre_open_interest": tick.pre_open_interest,
        "source": str(tick.source),
        "source_seq": tick.source_seq,
        "epoch": epoch,
        "fingerprint": tick.fingerprint,
        "flags": tick.flags,
        "mode": str(mode),
        "reject_reason": reason,
    }


def reject_row(
    *, instrument_id: str, exchange: str | None, trading_day: str, recv_ns: int, source: SourceId, reason: str, epoch: str | None, seq: int | None
) -> dict[str, Any]:
    """Row of a record that could not be normalized: identifiers only (03 §5.1)."""
    row: dict[str, Any] = dict.fromkeys(TICK_COLUMNS)
    row.update(
        instrument_id=instrument_id,
        exchange=exchange,
        trading_day=trading_day,
        action_day=trading_day,
        recv_ns=recv_ns,
        source=str(source),
        source_seq=seq,
        epoch=epoch,
        mode=str(TickMode.REJECTED),
        reject_reason=reason,
    )
    return row


# ---------------------------------------------------------------- reading
def day_shards(
    raw_dir: Path,
    trading_day: str,
    sources: Iterable[SourceId],
    next_day: str | None = None,
    spill_dirs: Iterable[Path] = (),
) -> list[Path]:
    """Raw shards of ``trading_day`` (plus the next day's folder, where late replays land), spill directories included."""
    out: list[Path] = []
    roots = [Path(raw_dir), *(Path(p) for p in spill_dirs)]
    srcs = list(sources)
    for td in [trading_day] + ([next_day] if next_day else []):
        for root in roots:
            for src in srcs:
                out.extend(list_shards(root, td, src.lower(), TICK_PREFIX))
    return out


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
        meta = pq.read_schema(path).metadata or {}
    except Exception:
        return False
    stored = meta.get(_SOURCES_META)
    return stored is not None and json.loads(stored) == _signature(shards, raw_dir)


def read_day_frame(shards: list[Path]) -> pd.DataFrame:
    table = read_shards(shards, TICK_SCHEMA)
    return table.to_pandas(types_mapper=_PANDAS_TYPES.get)


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
        mask = pc.and_(pc.is_in(mode, value_set=pa.array([str(TickMode.LIVE), str(TickMode.BACKFILL)])), pc.equal(td_col, trading_day))
        table = table.filter(mask)
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
            exact = set(key[~df["_coarse"]])
            df = df[~(df["_coarse"] & key.isin(exact))]
        df = df.sort_values(["instrument_id", "volume", "exch_ts_ns", "_prio"], kind="stable")
        df = df[list(TICK_COLUMNS)]
    else:
        df = pd.DataFrame({c: [] for c in TICK_COLUMNS})
    schema = TICK_SCHEMA.with_metadata(
        {**(TICK_SCHEMA.metadata or {}), _SOURCES_META: json.dumps(_signature(shards, raw_dir)).encode("utf-8")}
    )
    result = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
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


class ShardCache:
    """Per-day cache of raw shards for ``md.ticks`` (L3): only shards not read yet are loaded.

    Closed shards are immutable, so the cache is keyed by path; a changed list (new shards,
    purged ones) only reads what is new. Recorder thread only.
    """

    def __init__(self, max_days: int = 1) -> None:
        self._days: dict[str, tuple[list[Path], pa.Table]] = {}
        self._max_days = max_days
        self.reads = 0

    def table(self, trading_day: str, shards: list[Path]) -> pa.Table:
        hit = self._days.get(trading_day)
        if hit is not None:
            known, table = hit
            known_set = set(known)
            if known_set.issubset(shards):
                new = [p for p in shards if p not in known_set]
                if not new:
                    return table
                table = pa.concat_tables([table, read_shards(new, TICK_SCHEMA)], promote_options="default")
                self.reads += len(new)
                self._days[trading_day] = (known + new, table)
                return table
        table = read_shards(shards, TICK_SCHEMA)
        self.reads += len(shards)
        self._days[trading_day] = (list(shards), table)
        while len(self._days) > self._max_days:
            del self._days[next(iter(self._days))]
        return table


def row_to_tick(r: Mapping[str, Any]) -> Tick | None:
    """Rebuild a :class:`Tick` from a raw/canonical row (``None`` for rows without market data)."""
    if _na(r.get("last")) or _na(r.get("volume")) or _na(r.get("exch_ts_ns")):
        return None
    return Tick(
        instrument_id=str(r["instrument_id"]),
        exchange=Exchange(str(r["exchange"])),
        trading_day=str(r["trading_day"]),
        action_day=str(r["action_day"]),
        exch_ts_ns=int(r["exch_ts_ns"]),
        last=float(r["last"]),
        volume=int(r["volume"]),
        turnover=_f(r.get("turnover")) or 0.0,
        open_interest=_i(r.get("open_interest")) or 0,
        bid1=_f(r.get("bid1")),
        ask1=_f(r.get("ask1")),
        bid_vol1=_i(r.get("bid_vol1")) or 0,
        ask_vol1=_i(r.get("ask_vol1")) or 0,
        open=_f(r.get("open")),
        high=_f(r.get("high")),
        low=_f(r.get("low")),
        upper_limit=_f(r.get("upper_limit")) or 0.0,
        lower_limit=_f(r.get("lower_limit")) or 0.0,
        pre_settle=_f(r.get("pre_settle")) or 0.0,
        pre_close=_f(r.get("pre_close")),
        pre_open_interest=_i(r.get("pre_open_interest")),
        avg_price=_f(r.get("avg_price")),
        source=SourceId(str(r["source"])),
        source_seq=_i(r.get("source_seq")),
        recv_ns=_i(r.get("recv_ns")) or 0,
        fingerprint=_i(r.get("fingerprint")) or 0,
        flags=_i(r.get("flags")) or 0,
    )


def _na(v: Any) -> bool:
    return v is None or (isinstance(v, float) and v != v) or v is pd.NA


def _f(v: Any) -> float | None:
    return None if _na(v) else float(v)


def _i(v: Any) -> int | None:
    return None if _na(v) else int(v)


__all__ = [
    "ShardCache",
    "TickRecorder",
    "build_canonical",
    "canonical_fresh",
    "canonical_path",
    "day_shards",
    "read_canonical",
    "read_day_frame",
    "reject_row",
    "row_to_tick",
    "tick_row",
]
