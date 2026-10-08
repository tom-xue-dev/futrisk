"""Tick files: Arrow schemas, row builders, the crash-safe shard writer and readers (03 §5.1, 02 §7.1).

**Raw tick shards** ``ticks/raw/{trading_day}/{source}/part-{epoch_ms}-{n}.parquet``: one row
per received, normalized state (rejected and duplicate ones included); columns = ``Tick``
fields + ``epoch``, ``mode``, ``reject_reason``. Rows that could not be parsed carry only the
identifying columns. **Wire shards** ``ticks/wire/{trading_day}/{source}/wire-*.parquet``: one
row per HTTP poll with the untouched response bytes (the Sina recorder's archive).

**Durability** (:class:`ParquetShardWriter`): a shard is written to a hidden ``.tmp`` next to
its final name, flushed, fsync'ed and renamed atomically; readers only pick up closed shards.
Buckets (trading days) are written independently; a failed write keeps its rows and is
retried with exponential backoff (1 s ... 60 s), after ``spill_after`` consecutive failures
(and always on the final close) its shards go to the spill roots; memory is bounded
(``max_pending_rows``: everything is spilled, and beyond four times that the oldest rows are
dropped with a CRITICAL log). ``epoch_ms`` (the writer's start time) and a counter make file
names sort chronologically and never collide across restarts.
"""

from __future__ import annotations

import contextlib
import math
import os
from collections.abc import Iterable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final

import msgspec
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from futrisk.common.clock import NS_PER_S, Clock
from futrisk.messages.enums import TickMode
from futrisk.messages.md import Tick

SCHEMA_VERSION: Final = "1"
TICK_PREFIX: Final = "part"
WIRE_PREFIX: Final = "wire"
DEFAULT_ROLL_S: Final = 5.0  # 03 §5.1: a shard every 5 s ...
DEFAULT_ROLL_ROWS: Final = 10_000  # ... or every 10k rows
DEFAULT_MAX_PENDING_ROWS: Final = 50_000
_STALE_TMP_S = 3600.0
_HARD_PENDING_FACTOR = 4
_BOUND_RETRY_S = 10.0
_READ_THREADS = 8
_DICT = pa.dictionary(pa.int32(), pa.string())

TICK_SCHEMA: Final = pa.schema(
    [
        pa.field("instrument_id", _DICT),
        pa.field("exchange", _DICT),
        pa.field("trading_day", _DICT),
        pa.field("action_day", _DICT),
        pa.field("exch_ts_ns", pa.int64()),
        pa.field("recv_ns", pa.int64()),
        pa.field("last", pa.float64()),
        pa.field("bid1", pa.float64()),
        pa.field("ask1", pa.float64()),
        pa.field("open", pa.float64()),
        pa.field("high", pa.float64()),
        pa.field("low", pa.float64()),
        pa.field("upper_limit", pa.float64()),
        pa.field("lower_limit", pa.float64()),
        pa.field("pre_settle", pa.float64()),
        pa.field("pre_close", pa.float64()),
        pa.field("avg_price", pa.float64()),
        pa.field("turnover", pa.float64()),
        pa.field("volume", pa.int64()),
        pa.field("open_interest", pa.int64()),
        pa.field("bid_vol1", pa.int64()),
        pa.field("ask_vol1", pa.int64()),
        pa.field("pre_open_interest", pa.int64()),
        pa.field("source", _DICT),
        pa.field("source_seq", pa.int64()),
        pa.field("epoch", pa.string()),
        pa.field("fingerprint", pa.uint64()),
        pa.field("flags", pa.int32()),
        pa.field("mode", _DICT),
        pa.field("reject_reason", pa.string()),
    ],
    metadata={"futrisk.kind": "ticks.raw", "futrisk.schema_version": SCHEMA_VERSION},
)
TICK_COLUMNS: Final[tuple[str, ...]] = tuple(TICK_SCHEMA.names)
WIRE_SCHEMA: Final = pa.schema(
    [
        pa.field("recv_ns", pa.int64()),
        pa.field("sent_ns", pa.int64()),
        pa.field("poll_seq", pa.int64()),
        pa.field("elapsed_ms", pa.float64()),
        pa.field("http_status", pa.int32()),
        pa.field("error", pa.string()),
        pa.field("codes", _DICT),
        pa.field("body", pa.binary()),
    ],
    metadata={"futrisk.kind": "ticks.wire", "futrisk.schema_version": SCHEMA_VERSION},
)
_PANDAS_TYPES: dict[pa.DataType, Any] = {pa.int64(): pd.Int64Dtype(), pa.int32(): pd.Int32Dtype(), pa.uint64(): pd.UInt64Dtype()}


# ---------------------------------------------------------------- rows
def tick_row(tick: Tick, mode: TickMode, reject_reason: str | None = None, epoch: str | None = None) -> dict[str, Any]:
    """Raw row of a normalized state: every ``Tick`` field (defaults included) + epoch / mode / reject_reason."""
    row: dict[str, Any] = msgspec.to_builtins(msgspec.structs.asdict(tick))
    row.update(epoch=epoch, mode=str(mode), reject_reason=reject_reason)
    return row


def reject_row(
    *, instrument_id: str, exchange: str | None, trading_day: str, recv_ns: int, source: str, reject_reason: str, epoch: str | None = None, seq: int | None = None
) -> dict[str, Any]:
    """Row of a record that could not be normalized: identifiers only, market columns null."""
    row: dict[str, Any] = dict.fromkeys(TICK_COLUMNS)
    row.update(
        instrument_id=instrument_id, exchange=exchange, trading_day=trading_day, action_day=trading_day, recv_ns=recv_ns, source=source,
        source_seq=seq, epoch=epoch, mode=str(TickMode.REJECTED), reject_reason=reject_reason,
    )
    return row


def wire_row(*, recv_ns: int, sent_ns: int, poll_seq: int, elapsed_ms: float, http_status: int | None, error: str | None, codes: str, body: bytes | None) -> dict[str, Any]:
    return {"recv_ns": recv_ns, "sent_ns": sent_ns, "poll_seq": poll_seq, "elapsed_ms": elapsed_ms, "http_status": http_status, "error": error, "codes": codes, "body": body}


# ---------------------------------------------------------------- crash-safe shard writer
class ShardWriteError(OSError):
    """Some buckets could not be written anywhere (their rows are still buffered); ``written`` lists the shards that were."""

    def __init__(self, failures: Sequence[tuple[str, BaseException]], written: Sequence[Path] = ()) -> None:
        self.failures = list(failures)
        self.written = list(written)
        super().__init__(f"{len(self.failures)} bucket(s) not written: " + "; ".join(f"{b}: {e!r}" for b, e in self.failures))


@dataclass
class _Buffer:
    cols: dict[str, list[Any]]
    rows: int = 0
    opened_ns: int = 0


@dataclass
class _Health:
    """Write health of one bucket's primary directory (kept across buffers until a write succeeds)."""

    failures: int = 0  # consecutive failed writes
    retry_at_ns: int | None = None  # not retried before this monotonic time


class ParquetShardWriter:
    """Buffers rows per trading-day bucket and writes immutable, atomically renamed Parquet shards (not thread-safe)."""

    def __init__(
        self,
        root: Path,
        source_dir: str,
        schema: pa.Schema,
        *,
        prefix: str,
        epoch_ms: int,
        clock: Clock,
        roll_s: float = DEFAULT_ROLL_S,
        roll_rows: int = DEFAULT_ROLL_ROWS,
        compression: str = "zstd",
        spill_roots: Sequence[Path] = (),
        spill_after: int = 3,
        retry_base_s: float = 1.0,
        retry_max_s: float = 60.0,
        max_pending_rows: int = DEFAULT_MAX_PENDING_ROWS,
    ) -> None:
        if roll_rows <= 0 or max_pending_rows <= 0:
            raise ValueError("roll_rows and max_pending_rows must be positive")
        self._root = Path(root)
        self._source_dir = source_dir
        self._schema = schema
        self._prefix = prefix
        self._epoch_ms = epoch_ms
        self._clock = clock
        self._roll_ns = math.inf if math.isinf(roll_s) else int(roll_s * NS_PER_S)
        self._roll_rows = roll_rows
        self._compression = compression
        self._spill_roots = [Path(p) for p in spill_roots]
        self._spill_after = max(1, spill_after)
        self._retry_base_s = retry_base_s
        self._retry_max_s = retry_max_s
        self._max_pending_rows = max_pending_rows
        self._buffers: dict[str, _Buffer] = {}
        self._next_n = 1
        self._cleaned: set[Path] = set()
        self._health: dict[str, _Health] = {}  # per bucket: one broken day folder never slows another
        self._next_bound_ns = 0
        self.shards_written = 0
        self.rows_written = 0
        self.spilled_shards = 0
        self.spilled_rows = 0
        self.dropped_rows = 0
        self.last_shard: Path | None = None
        self.last_spill: Path | None = None

    @property
    def root(self) -> Path:
        return self._root

    @property
    def pending_rows(self) -> int:
        return sum(b.rows for b in self._buffers.values())

    @property
    def consecutive_failures(self) -> int:
        return max((h.failures for h in self._health.values()), default=0)

    def shard_dir(self, bucket: str, root: Path | None = None) -> Path:
        return (root or self._root) / bucket / self._source_dir

    # ---------------------------------------------------------------- public
    def append(self, bucket: str, row: Mapping[str, Any]) -> list[Path]:
        """Buffer one row, then flush buckets that are due (the row is kept even if a flush fails)."""
        buf = self._buffers.get(bucket)
        if buf is None:
            buf = _Buffer({name: [] for name in self._schema.names}, 0, self._clock.monotonic_ns())
            self._buffers[bucket] = buf
        for name, col in buf.cols.items():
            col.append(row.get(name))
        buf.rows += 1
        # a row for a new bucket means the trading day rolled: close out the older buckets
        due = [b for b, other in self._buffers.items() if b != bucket and other.rows]
        if buf.rows >= self._roll_rows:
            due.append(bucket)
        return self._flush_many(due, final=False)

    def roll_if_due(self) -> list[Path]:
        """Flush every bucket whose oldest buffered row is older than ``roll_s``."""
        now = self._clock.monotonic_ns()
        return self._flush_many([b for b, buf in self._buffers.items() if buf.rows and now - buf.opened_ns >= self._roll_ns], final=False)

    def flush(self, bucket: str | None = None) -> list[Path]:
        """Write buffered rows now (one bucket or all), honouring the retry backoff."""
        return self._flush_many([bucket] if bucket is not None else list(self._buffers), final=False)

    def close(self) -> list[Path]:
        """Final flush: primary first (ignoring the backoff), then every spill root; raises when some rows could not be written."""
        return self._flush_many(list(self._buffers), final=True)

    # -------------------------------------------------------------- internals
    def _flush_many(self, buckets: Iterable[str], *, final: bool) -> list[Path]:
        written: list[Path] = []
        errors: list[tuple[str, BaseException]] = []
        for bucket in buckets:
            try:
                path = self._flush_bucket(bucket, final=final)
            except Exception as exc:  # one bucket must never block the others
                errors.append((bucket, exc))
            else:
                if path is not None:
                    written.append(path)
        self._bound_memory()
        if errors:
            raise ShardWriteError(errors, written)
        return written

    def _flush_bucket(self, bucket: str, *, final: bool) -> Path | None:
        buf = self._buffers.get(bucket)
        if buf is None or buf.rows == 0:
            return None
        now = self._clock.monotonic_ns()
        health = self._health.setdefault(bucket, _Health())
        primary_exc: BaseException | None = None
        if final or health.retry_at_ns is None or now >= health.retry_at_ns:
            try:
                path = self._write(self._root, bucket, buf)
            except Exception as exc:
                primary_exc = exc
                health.failures += 1
                delay_s = min(self._retry_max_s, self._retry_base_s * 2 ** (health.failures - 1))
                health.retry_at_ns = now + int(delay_s * NS_PER_S)
                logger.error(f"recorder.write_failed dir={self.shard_dir(bucket)} rows={buf.rows} consecutive={health.failures} retry_in_s={delay_s:.0f}: {exc!r}")
            else:
                if health.failures:
                    logger.warning(f"recorder.write_recovered dir={self.shard_dir(bucket)} after {health.failures} failures")
                del self._health[bucket]
                del self._buffers[bucket]
                return path
        if final or health.failures >= self._spill_after:
            spilled = self._spill(bucket, buf)
            if spilled is not None:
                del self._buffers[bucket]
                return spilled
            raise primary_exc or OSError(f"{self.shard_dir(bucket)} is backing off and no spill root is writable")
        if primary_exc is not None:
            raise primary_exc
        return None  # backing off: the rows stay buffered until the next attempt

    def _spill(self, bucket: str, buf: _Buffer) -> Path | None:
        for root in self._spill_roots:
            try:
                path = self._write(root, bucket, buf)
            except Exception as exc:
                logger.error(f"recorder.spill_failed dir={self.shard_dir(bucket, root)} rows={buf.rows}: {exc!r}")
                continue
            self.spilled_shards += 1
            self.spilled_rows += buf.rows
            self.last_spill = path
            logger.error(f"recorder.spilled rows={buf.rows} to {path}: {self.shard_dir(bucket)} is not writable; move the spilled files there once it is fixed")
            return path
        return None

    def _bound_memory(self) -> None:
        if self.pending_rows <= self._max_pending_rows:
            return
        now = self._clock.monotonic_ns()
        if now >= self._next_bound_ns:
            self._next_bound_ns = now + int(_BOUND_RETRY_S * NS_PER_S)
            for bucket in list(self._buffers):
                buf = self._buffers[bucket]
                if buf.rows and self._spill(bucket, buf) is not None:
                    del self._buffers[bucket]
        total = self.pending_rows
        if total <= self._max_pending_rows * _HARD_PENDING_FACTOR:
            return
        excess = total - self._max_pending_rows
        for bucket in sorted(self._buffers, key=lambda b: self._buffers[b].opened_ns):
            if excess <= 0:
                break
            buf = self._buffers[bucket]
            n = min(excess, buf.rows)
            for col in buf.cols.values():
                del col[:n]
            buf.rows -= n
            excess -= n
            self.dropped_rows += n
            logger.critical(f"recorder.rows_dropped n={n} bucket={bucket} dir={self.shard_dir(bucket)}: nothing is writable and {total} rows are pending (dropped so far: {self.dropped_rows})")
            if buf.rows == 0:
                del self._buffers[bucket]

    def _clean_stale_tmp(self, directory: Path) -> None:
        now_s = self._clock.now_ns() / NS_PER_S
        for tmp in directory.glob(".*.tmp"):
            with contextlib.suppress(OSError):
                if now_s - tmp.stat().st_mtime > _STALE_TMP_S:
                    tmp.unlink()
                    logger.info(f"recorder.stale_tmp_removed {tmp}")

    def _write(self, root: Path, bucket: str, buf: _Buffer) -> Path:
        directory = self.shard_dir(bucket, root)
        directory.mkdir(parents=True, exist_ok=True)  # on every write: the folder may have been moved away
        if directory not in self._cleaned:
            self._clean_stale_tmp(directory)
            self._cleaned.add(directory)
        table = pa.Table.from_pydict(buf.cols, schema=self._schema)
        name = f"{self._prefix}-{self._epoch_ms}-{self._next_n:06d}.parquet"
        final = directory / name
        tmp = directory / f".{name}.tmp"
        try:
            with open(tmp, "wb") as fh:
                pq.write_table(table, fh, compression=self._compression)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, final)
        except BaseException:
            with contextlib.suppress(OSError):
                tmp.unlink()
            raise
        self._next_n += 1
        self.shards_written += 1
        self.rows_written += buf.rows
        self.last_shard = final
        logger.debug(f"recorder.shard_written {final} rows={buf.rows}")
        return final


# ---------------------------------------------------------------- readers
def list_shards(root: Path, trading_day: str, source: str = "sina", prefix: str = TICK_PREFIX) -> list[Path]:
    """Closed shards of one day, in chronological (= lexicographic) order; temp files are ignored."""
    directory = Path(root) / trading_day / source
    if not directory.is_dir():
        return []
    return sorted(p for p in directory.glob(f"{prefix}-*.parquet") if p.is_file())


def list_trading_days(root: Path, source: str = "sina", prefix: str = TICK_PREFIX) -> list[str]:
    """Trading days under ``root`` that have at least one shard for ``source``."""
    root = Path(root)
    if not root.is_dir():
        return []
    return sorted(d.name for d in root.iterdir() if d.is_dir() and list_shards(root, d.name, source, prefix))


def read_shards(paths: Iterable[Path], schema: pa.Schema = TICK_SCHEMA) -> pa.Table:
    """Concatenate shards (in the given order) into one Arrow table; an empty table with ``schema`` if none."""
    paths = list(paths)
    if not paths:
        return schema.empty_table()
    if len(paths) > 32:  # 5 s shards: thousands of small files per day
        with ThreadPoolExecutor(max_workers=_READ_THREADS) as pool:
            tables = list(pool.map(pq.read_table, paths))
    else:
        tables = [pq.read_table(p) for p in paths]
    return pa.concat_tables(tables, promote_options="default")


def _frame(table: pa.Table) -> pd.DataFrame:
    """Nullable integer dtypes keep rejected rows (null market columns) from turning ints into floats."""
    df = table.to_pandas(types_mapper=_PANDAS_TYPES.get)
    return df.sort_values("recv_ns", kind="stable").reset_index(drop=True) if "recv_ns" in df.columns and len(df) else df


def read_raw_ticks(raw_dir: Path, trading_day: str, source: str = "sina") -> pd.DataFrame:
    """All tick rows of one trading day (``<home>/ticks/raw``), sorted by ``recv_ns``."""
    return _frame(read_shards(list_shards(raw_dir, trading_day, source, TICK_PREFIX), TICK_SCHEMA))


def read_wire(wire_dir: Path, trading_day: str, source: str = "sina") -> pd.DataFrame:
    """All archived polls of one trading day (``<home>/ticks/wire``), sorted by ``recv_ns``."""
    return _frame(read_shards(list_shards(wire_dir, trading_day, source, WIRE_PREFIX), WIRE_SCHEMA))


__all__ = [
    "DEFAULT_ROLL_ROWS",
    "DEFAULT_ROLL_S",
    "TICK_COLUMNS",
    "TICK_PREFIX",
    "TICK_SCHEMA",
    "WIRE_PREFIX",
    "WIRE_SCHEMA",
    "ParquetShardWriter",
    "ShardWriteError",
    "list_shards",
    "list_trading_days",
    "read_raw_ticks",
    "read_shards",
    "read_wire",
    "reject_row",
    "tick_row",
    "wire_row",
]
