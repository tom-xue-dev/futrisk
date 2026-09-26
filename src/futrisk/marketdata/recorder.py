"""Raw tick recorder: classification, crash-safe Parquet shards and readers (03 §5.1, 01 §8.13, 02 §11.3).

**Layout** (03 §5.1)::

    <home>/ticks/raw/{trading_day}/sina/part-{epoch_ms}-{n:06d}.parquet    tick rows (TICK_SCHEMA)
    <home>/ticks/wire/{trading_day}/sina/wire-{epoch_ms}-{n:06d}.parquet   raw HTTP polls (WIRE_SCHEMA)
    <home>/ticks/spill/{trading_day}/sina/{part,wire}-*.parquet             fallback when those fail

``epoch_ms`` is the recorder's start time (one value per process incarnation)
and ``n`` a zero-padded shard counter, so file names sort chronologically and
never collide across restarts or between primary and spill directories.
``{trading_day}`` is the trading day of the *receive* time
(``calendar.current_or_next_trading_day``), so stale snapshots received before
an open land in the new day's directory as rejected rows.

**Durability** -- a killed process loses at most ``roll_s`` (5 s, 03 §5.1) of rows:

- a shard is written to a hidden ``.{name}.tmp`` next to its final name,
  flushed, fsync'ed and renamed with ``os.replace`` (atomic on NTFS and
  POSIX). Readers only pick up ``part-*.parquet`` / ``wire-*.parquet``, so a
  crash leaves at most a stray ``.tmp`` and closed shards are never touched
  again. The directory is re-created (``mkdir(parents=True)``) on every write,
  so a day folder moved away mid-run comes back;
- buckets (trading days) are written independently: a failing bucket never
  blocks another; errors are collected and raised together as
  :class:`ShardWriteError` after every bucket was tried;
- a failed write keeps its rows and is retried with exponential backoff
  (1 s, 2 s, 4 s ... 60 s), tracked per bucket and not on every append. After
  ``spill_after`` consecutive failures of a bucket -- and always on the final
  close -- its shards go to the spill roots (``<home>/ticks/spill``, then the
  system temp dir), each spill logged as an ERROR; the primary is retried as
  the backoff allows;
- memory is bounded: above ``max_pending_rows`` everything is spilled at once;
  if even that fails and four times as many rows pile up, the oldest rows are
  dropped with a CRITICAL log -- never silently.

:class:`SinaRecorder` serializes all writer access with a lock, so
:meth:`SinaRecorder.emergency_flush` may run from another thread (the Windows
console-close handler, which has about 5 s before the process is killed).

**Classification** (column ``mode`` + ``reject_reason``): the standalone
recorder has no arbiter, so :class:`TickClassifier` applies the single-source
subset of 01 §8.13, in this order:

1. rejected by the adapter -> ``REJECTED`` with its reason (``MD_REJECT_MALFORMED``,
   ``MD_REJECT_TRADING_DAY`` for timestamps outside any trading day, ``MD_REJECT_FUTURE_TS`` for timestamps in the future);
2. trading day differs from the receive-time trading day -> ``MD_REJECT_TRADING_DAY``
   (e.g. the previous session's snapshot served before the open);
3. exchange time outside the instrument's session windows (with a grace period
   after each window end for closing snapshots) -> ``MD_REJECT_OUT_OF_SESSION``;
4. last price outside the limits -> ``MD_REJECT_PRICE_LIMIT``; off the tick grid -> ``MD_REJECT_OFF_TICK``;
5. same fingerprint as the previous accepted state -> ``DUPLICATE``;
6. volume or timestamp going backwards -> ``MD_REJECT_NON_MONOTONIC``;
   last price changed without volume -> ``MD_REJECT_PRICE_WITHOUT_TRADE``;
7. volume above the watermark -> ``LIVE`` (Sina may only advance LIVE on a volume
   increase, 01 §8.13); any other accepted state -> ``BACKFILL``.

Rejected rows keep all their parsed columns; md-gateway re-classifies when replaying.
"""

from __future__ import annotations

import contextlib
import math
import os
import threading
from collections.abc import Callable, Iterable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from futrisk.common.calendar import TradingCalendar
from futrisk.common.clock import NS_PER_MS, NS_PER_S, Clock, SystemClock, ns_to_bj, parse_td, td_str
from futrisk.common.decimals import on_tick
from futrisk.marketdata.normalize import InstrumentMeta
from futrisk.marketdata.poller import SinaPoll
from futrisk.marketdata.sessions import WindowCache, in_session
from futrisk.marketdata.sina import SinaNormalizer, SinaResult, decode_body
from futrisk.marketdata.tickschema import TICK_SCHEMA, WIRE_SCHEMA, reject_row, tick_row, wire_row
from futrisk.messages.codes import (
    MD_REJECT_MALFORMED,
    MD_REJECT_NON_MONOTONIC,
    MD_REJECT_OFF_TICK,
    MD_REJECT_OUT_OF_SESSION,
    MD_REJECT_PRICE_LIMIT,
    MD_REJECT_PRICE_WITHOUT_TRADE,
    MD_REJECT_TRADING_DAY,
)
from futrisk.messages.enums import SourceId, TickMode
from futrisk.messages.md import Tick

SINA_DIR = SourceId.SINA.lower()
TICK_PREFIX = "part"
WIRE_PREFIX = "wire"
DEFAULT_ROLL_S = 5.0  # 03 §5.1: a shard every 5 s ...
DEFAULT_ROLL_ROWS = 10_000  # ... or every 10k rows
DEFAULT_MAX_PENDING_ROWS = 50_000
_PRICE_EPS = 1e-6
_STALE_TMP_S = 3600.0
_HARD_PENDING_FACTOR = 4
_BOUND_RETRY_S = 10.0
_READ_THREADS = 8


# ---------------------------------------------------------------------------
# Crash-safe shard writer
# ---------------------------------------------------------------------------


class ShardWriteError(OSError):
    """Some buckets could not be written anywhere (their rows are still buffered).

    ``written`` lists the shards that *were* written by the same call.
    """

    def __init__(self, failures: Sequence[tuple[str, BaseException]], written: Sequence[Path] = ()) -> None:
        self.failures = list(failures)
        self.written = list(written)
        detail = "; ".join(f"{bucket}: {exc!r}" for bucket, exc in self.failures)
        super().__init__(f"{len(self.failures)} bucket(s) not written: {detail}")


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
    """Buffers rows per trading-day bucket and writes immutable, atomically renamed Parquet shards.

    Failure handling (retry backoff, spill roots, memory bound) is described in the module docstring.
    Not thread-safe: callers serialize access (see :class:`SinaRecorder`).
    """

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
        self.last_spill: Path | None = None

    # ------------------------------------------------------------ properties
    @property
    def root(self) -> Path:
        return self._root

    @property
    def pending_rows(self) -> int:
        return sum(b.rows for b in self._buffers.values())

    @property
    def consecutive_failures(self) -> int:
        """Largest consecutive-failure count over the buckets (0 when every write succeeds)."""
        return max((h.failures for h in self._health.values()), default=0)

    def shard_dir(self, bucket: str, root: Path | None = None) -> Path:
        return (root or self._root) / bucket / self._source_dir

    # ---------------------------------------------------------------- public
    def append(self, bucket: str, row: Mapping[str, Any]) -> list[Path]:
        """Buffer one row, then flush buckets that are due; the row is kept even if a flush fails.

        Returns the shards written; raises :class:`ShardWriteError` when a due
        bucket could not be written anywhere (its rows stay buffered).
        """
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
        due = [b for b, buf in self._buffers.items() if buf.rows and now - buf.opened_ns >= self._roll_ns]
        return self._flush_many(due, final=False)

    def flush(self, bucket: str | None = None) -> list[Path]:
        """Write buffered rows now (one bucket or all), honouring the retry backoff."""
        return self._flush_many([bucket] if bucket is not None else list(self._buffers), final=False)

    def close(self) -> list[Path]:
        """Final flush: primary first (ignoring the backoff), then every spill root.

        Raises :class:`ShardWriteError` if some rows could not be written anywhere.
        """
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
                logger.error(
                    f"recorder.write_failed dir={self.shard_dir(bucket)} rows={buf.rows} "
                    f"consecutive={health.failures} retry_in_s={delay_s:.0f}: {exc!r}"
                )
            else:
                if health.failures:
                    logger.warning(
                        f"recorder.write_recovered dir={self.shard_dir(bucket)} after {health.failures} failures"
                    )
                del self._health[bucket]
                del self._buffers[bucket]
                return path
        if final or health.failures >= self._spill_after:
            spilled = self._spill(bucket, buf)
            if spilled is not None:
                del self._buffers[bucket]
                return spilled
            raise primary_exc or OSError(
                f"{self.shard_dir(bucket)} is backing off and no spill root is writable"
            )
        if primary_exc is not None:
            raise primary_exc
        return None  # backing off: the rows stay buffered until the next attempt

    def _spill(self, bucket: str, buf: _Buffer) -> Path | None:
        for root in self._spill_roots:
            try:
                path = self._write(root, bucket, buf)
            except Exception as exc:
                logger.error(
                    f"recorder.spill_failed dir={self.shard_dir(bucket, root)} rows={buf.rows}: {exc!r}"
                )
                continue
            self.spilled_shards += 1
            self.spilled_rows += buf.rows
            self.last_spill = path
            logger.error(
                f"recorder.spilled rows={buf.rows} to {path}: {self.shard_dir(bucket)} is not writable; "
                f"move the spilled files there once it is fixed"
            )
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
            logger.critical(
                f"recorder.rows_dropped n={n} bucket={bucket} dir={self.shard_dir(bucket)}: neither the primary "
                f"nor any spill directory is writable and {total} rows are pending (dropped so far: {self.dropped_rows})"
            )
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
        logger.debug(f"recorder.shard_written {final} rows={buf.rows}")
        return final


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------


@dataclass
class _InstrumentState:
    trading_day: str
    fingerprint: int | None = None
    wm_volume: int = -1
    last_volume: int = -1
    last_exch_ts_ns: int = -1
    last_price: float | None = None


class TickClassifier:
    """Single-source ``TickMode`` classifier used by the standalone recorder (rules in the module docstring)."""

    def __init__(
        self,
        calendar: TradingCalendar,
        instruments: Iterable[InstrumentMeta],
        *,
        close_grace_s: float = 300.0,
    ) -> None:
        self._windows = WindowCache(calendar)
        self._metas = {m.instrument_id: m for m in instruments}
        self._close_grace_s = close_grace_s
        self._state: dict[str, _InstrumentState] = {}

    def classify(self, result: SinaResult, bucket_td: str) -> tuple[TickMode, str | None]:
        if result.reject_reason is not None:
            return TickMode.REJECTED, result.reject_reason
        if result.tick is None:
            return TickMode.REJECTED, MD_REJECT_MALFORMED
        return self.classify_tick(result.tick, bucket_td)

    def classify_tick(self, tick: Tick, bucket_td: str) -> tuple[TickMode, str | None]:
        if tick.trading_day != bucket_td:
            return TickMode.REJECTED, MD_REJECT_TRADING_DAY
        meta = self._metas.get(tick.instrument_id)
        if meta is not None:
            td = parse_td(tick.trading_day)
            if not in_session(
                self._windows, meta.sessions, td, tick.exch_ts_ns, close_grace_s=self._close_grace_s
            ):
                return TickMode.REJECTED, MD_REJECT_OUT_OF_SESSION
            if tick.last < tick.lower_limit - _PRICE_EPS or tick.last > tick.upper_limit + _PRICE_EPS:
                return TickMode.REJECTED, MD_REJECT_PRICE_LIMIT
            if not on_tick(tick.last, meta.tick_size):
                return TickMode.REJECTED, MD_REJECT_OFF_TICK
        st = self._state.get(tick.instrument_id)
        if st is None or st.trading_day != tick.trading_day:
            st = _InstrumentState(tick.trading_day)
            self._state[tick.instrument_id] = st
        if st.fingerprint == tick.fingerprint:
            return TickMode.DUPLICATE, None
        if tick.volume < st.last_volume or tick.exch_ts_ns < st.last_exch_ts_ns:
            return TickMode.REJECTED, MD_REJECT_NON_MONOTONIC
        if tick.volume == st.last_volume and st.last_price is not None and tick.last != st.last_price:
            return TickMode.REJECTED, MD_REJECT_PRICE_WITHOUT_TRADE
        mode = TickMode.LIVE if tick.volume > st.wm_volume else TickMode.BACKFILL
        if mode is TickMode.LIVE:
            st.wm_volume = tick.volume
        st.fingerprint = tick.fingerprint
        st.last_volume = tick.volume
        st.last_exch_ts_ns = tick.exch_ts_ns
        st.last_price = tick.last
        return mode, None


# ---------------------------------------------------------------------------
# Recorder
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ClassifiedResult:
    result: SinaResult
    mode: TickMode
    reject_reason: str | None


@dataclass
class SymbolStats:
    code: str
    instrument_id: str
    counts: dict[str, int] = field(default_factory=dict)
    last_reason: str | None = None
    last_detail: str | None = None
    last_tick: Tick | None = None
    last_lag_ms: float | None = None

    def count(self, mode: TickMode | str) -> int:
        return self.counts.get(str(mode), 0)


@dataclass
class RecorderStats:
    polls: int = 0
    failed_polls: int = 0
    rows: int = 0
    shards: int = 0
    wire_rows: int = 0
    wire_shards: int = 0
    write_errors: int = 0
    spilled_shards: int = 0
    dropped_rows: int = 0
    bucket_td: str | None = None
    last_shard: Path | None = None
    last_spill: Path | None = None
    symbols: dict[str, SymbolStats] = field(default_factory=dict)


def bucket_trading_day(calendar: TradingCalendar, recv_ns: int) -> str:
    """Trading day a receive time belongs to (the next one on non-trading time)."""
    try:
        return td_str(calendar.current_or_next_trading_day(recv_ns))
    except LookupError:
        return ns_to_bj(recv_ns).strftime("%Y%m%d")


def result_row(
    result: SinaResult,
    mode: TickMode,
    reject_reason: str | None,
    *,
    bucket_td: str,
    exchange: str | None,
    source: str = str(SourceId.SINA),
) -> dict[str, Any]:
    """Tick-shard row for one normalization result (all parsed columns whenever a tick was built)."""
    if result.tick is not None:
        return tick_row(result.tick, mode, reject_reason)
    return reject_row(
        instrument_id=result.instrument_id or result.code,
        exchange=exchange,
        trading_day=bucket_td,
        action_day=ns_to_bj(result.recv_ns).strftime("%Y%m%d"),
        recv_ns=result.recv_ns,
        source=source,
        reject_reason=reject_reason or MD_REJECT_MALFORMED,
    )


class SinaRecorder:
    """Records every Sina poll: the raw body (wire shard) and one classified row per requested code.

    Thread-safe: every method takes the recorder lock, so :meth:`emergency_flush`
    can be called from a console-control handler thread.
    """

    def __init__(
        self,
        raw_dir: Path,
        normalizer: SinaNormalizer,
        *,
        classifier: TickClassifier | None = None,
        wire_dir: Path | None = None,
        spill_dirs: Sequence[Path] = (),
        clock: Clock | None = None,
        roll_s: float = DEFAULT_ROLL_S,
        roll_rows: int = DEFAULT_ROLL_ROWS,
        epoch_ms: int | None = None,
        close_grace_s: float = 300.0,
        max_pending_rows: int = DEFAULT_MAX_PENDING_ROWS,
    ) -> None:
        self._normalizer = normalizer
        self._calendar = normalizer.calendar
        self._clock: Clock = clock or SystemClock()
        self._classifier = classifier or TickClassifier(
            self._calendar, normalizer.instruments.values(), close_grace_s=close_grace_s
        )
        self._lock = threading.RLock()
        self.epoch_ms = epoch_ms if epoch_ms is not None else self._clock.now_ns() // NS_PER_MS

        def writer(root: Path, schema: pa.Schema, prefix: str) -> ParquetShardWriter:
            return ParquetShardWriter(
                root,
                SINA_DIR,
                schema,
                prefix=prefix,
                epoch_ms=self.epoch_ms,
                clock=self._clock,
                roll_s=roll_s,
                roll_rows=roll_rows,
                spill_roots=spill_dirs,
                max_pending_rows=max_pending_rows,
            )

        self._ticks = writer(raw_dir, TICK_SCHEMA, TICK_PREFIX)
        self._wire = writer(wire_dir, WIRE_SCHEMA, WIRE_PREFIX) if wire_dir is not None else None
        self.stats = RecorderStats(
            symbols={
                code: SymbolStats(code, meta.instrument_id) for code, meta in normalizer.instruments.items()
            }
        )

    @property
    def normalizer(self) -> SinaNormalizer:
        return self._normalizer

    @property
    def pending_rows(self) -> int:
        """Tick rows buffered in memory (not yet on disk)."""
        return self._ticks.pending_rows

    @property
    def pending_wire_rows(self) -> int:
        return self._wire.pending_rows if self._wire is not None else 0

    def bucket_for(self, recv_ns: int) -> str:
        return bucket_trading_day(self._calendar, recv_ns)

    def _writers(self) -> list[tuple[str, ParquetShardWriter]]:
        out = [("ticks", self._ticks)]
        if self._wire is not None:
            out.append(("wire", self._wire))
        return out

    # ------------------------------------------------------------- recording
    def record_poll(self, poll: SinaPoll) -> list[ClassifiedResult]:
        """Archive one poll and record its normalized rows; never raises for I/O errors (they are logged)."""
        with self._lock:
            bucket = self.bucket_for(poll.recv_ns)
            st = self.stats
            st.bucket_td = bucket
            st.polls += 1
            if self._wire is not None:
                row = wire_row(
                    recv_ns=poll.recv_ns,
                    sent_ns=poll.sent_ns,
                    poll_seq=poll.seq,
                    elapsed_ms=poll.elapsed_ms,
                    http_status=poll.status,
                    error=poll.error,
                    codes=",".join(poll.codes),
                    body=poll.body,
                )
                self._guarded("wire", self._wire.append, bucket, row)
                st.wire_rows += 1
            out: list[ClassifiedResult] = []
            if not poll.ok:
                st.failed_polls += 1
            else:
                for result in self._normalizer.normalize_text(poll.text(), poll.recv_ns, poll.codes):
                    mode, reason = self._classifier.classify(result, bucket)
                    meta = self._normalizer.meta_for(result.code)
                    row = result_row(
                        result, mode, reason, bucket_td=bucket, exchange=str(meta.exchange) if meta else None
                    )
                    self._guarded("ticks", self._ticks.append, bucket, row)
                    st.rows += 1
                    self._update_symbol(result, mode, reason)
                    out.append(ClassifiedResult(result, mode, reason))
            self._roll_locked()
            return out

    def _update_symbol(self, result: SinaResult, mode: TickMode, reason: str | None) -> None:
        sym = self.stats.symbols.get(result.code)
        if sym is None:
            sym = self.stats.symbols[result.code] = SymbolStats(result.code, result.instrument_id)
        key = str(mode)
        sym.counts[key] = sym.counts.get(key, 0) + 1
        if reason is not None:
            sym.last_reason = reason
            sym.last_detail = result.detail or None
        if result.tick is not None:
            sym.last_tick = result.tick
            sym.last_lag_ms = (result.tick.recv_ns - result.tick.exch_ts_ns) / 1e6

    def _guarded(self, kind: str, fn: Callable[..., list[Path]], *args: Any) -> bool:
        try:
            paths = fn(*args)
        except ShardWriteError as exc:
            self._note(kind, exc.written)
            self.stats.write_errors += 1
            logger.error(
                f"recorder.write_failed kind={kind}: {exc} (rows kept in memory; retrying with backoff)"
            )
            return False
        except Exception:
            self.stats.write_errors += 1
            logger.opt(exception=True).error(f"recorder.write_failed kind={kind} (rows kept in memory)")
            return False
        self._note(kind, paths)
        return True

    def _note(self, kind: str, paths: Sequence[Path]) -> None:
        if paths:
            if kind == "wire":
                self.stats.wire_shards += len(paths)
            else:
                self.stats.shards += len(paths)
                self.stats.last_shard = paths[-1]
        st = self.stats
        st.spilled_shards = sum(w.spilled_shards for _, w in self._writers())
        st.dropped_rows = sum(w.dropped_rows for _, w in self._writers())
        spills = [w.last_spill for _, w in self._writers() if w.last_spill is not None]
        st.last_spill = spills[-1] if spills else None

    def _roll_locked(self) -> None:
        for kind, writer in self._writers():
            self._guarded(kind, writer.roll_if_due)

    def roll_if_due(self) -> None:
        with self._lock:
            self._roll_locked()

    def flush(self) -> bool:
        """Write every buffered row now (e.g. when a session ends); False if some rows are still pending."""
        with self._lock:
            ok = True
            for kind, writer in self._writers():
                ok = self._guarded(kind, writer.flush) and ok
            return ok

    def _final_flush_locked(self) -> bool:
        ok = True
        for kind, writer in self._writers():
            try:
                paths = writer.close()
            except ShardWriteError as exc:
                ok = False
                self._note(kind, exc.written)
                self.stats.write_errors += 1
                logger.critical(
                    f"recorder.close_failed kind={kind}: {writer.pending_rows} rows could not be saved to "
                    f"{writer.root} nor to any spill directory: {exc}"
                )
            except Exception:
                ok = False
                self.stats.write_errors += 1
                logger.opt(exception=True).critical(
                    f"recorder.close_failed kind={kind}: {writer.pending_rows} rows not saved"
                )
            else:
                self._note(kind, paths)
        return ok

    def close(self) -> bool:
        """Final flush of every bucket (primary, then the spill roots).

        Returns False -- after logging CRITICAL -- if some rows could not be saved anywhere.
        """
        with self._lock:
            return self._final_flush_locked()

    def emergency_flush(self, timeout_s: float = 3.0) -> bool:
        """Final flush from another thread (console close/logoff/shutdown); gives up after ``timeout_s``."""
        if not self._lock.acquire(timeout=timeout_s):
            logger.critical(f"recorder.emergency_flush could not take the recorder lock within {timeout_s}s")
            return False
        try:
            ok = self._final_flush_locked()
            logger.warning(f"recorder.emergency_flush done ok={ok}")
            return ok
        finally:
            self._lock.release()


# ---------------------------------------------------------------------------
# Readers and rebuild
# ---------------------------------------------------------------------------

_PANDAS_TYPES: dict[pa.DataType, Any] = {
    pa.int64(): pd.Int64Dtype(),
    pa.int32(): pd.Int32Dtype(),
    pa.uint64(): pd.UInt64Dtype(),
}


def list_shards(
    root: Path, trading_day: str, source: str = SINA_DIR, prefix: str = TICK_PREFIX
) -> list[Path]:
    """Closed shards of one day, in chronological (= lexicographic) order; temp files are ignored."""
    directory = Path(root) / trading_day / source
    if not directory.is_dir():
        return []
    return sorted(p for p in directory.glob(f"{prefix}-*.parquet") if p.is_file())


def list_trading_days(root: Path, source: str = SINA_DIR, prefix: str = TICK_PREFIX) -> list[str]:
    """Trading days under ``root`` that have at least one shard for ``source``."""
    root = Path(root)
    if not root.is_dir():
        return []
    return sorted(d.name for d in root.iterdir() if d.is_dir() and list_shards(root, d.name, source, prefix))


def read_shards(paths: Iterable[Path], schema: pa.Schema = TICK_SCHEMA) -> pa.Table:
    """Concatenate shards (in the given order) into one Arrow table; empty table with ``schema`` if none."""
    paths = list(paths)
    if not paths:
        return schema.empty_table()
    if len(paths) > 32:  # 5 s shards: thousands of small files per day
        with ThreadPoolExecutor(max_workers=_READ_THREADS) as pool:
            tables = list(pool.map(pq.read_table, paths))
    else:
        tables = [pq.read_table(p) for p in paths]
    return pa.concat_tables(tables, promote_options="default")


def _to_frame(table: pa.Table) -> pd.DataFrame:
    df = table.to_pandas(types_mapper=_PANDAS_TYPES.get)
    if "recv_ns" in df.columns and len(df):
        df = df.sort_values("recv_ns", kind="stable").reset_index(drop=True)
    return df


def read_raw_ticks(raw_dir: Path, trading_day: str, source: str = SINA_DIR) -> pd.DataFrame:
    """All tick rows of one trading day (``<home>/ticks/raw``), sorted by ``recv_ns``.

    Integer columns use pandas nullable dtypes (``Int64`` / ``Int32`` / ``UInt64``)
    so that rejected rows with null market columns do not turn them into floats;
    dictionary-encoded strings become categoricals.
    """
    return _to_frame(read_shards(list_shards(raw_dir, trading_day, source, TICK_PREFIX), TICK_SCHEMA))


def read_wire(wire_dir: Path, trading_day: str, source: str = SINA_DIR) -> pd.DataFrame:
    """All archived polls of one trading day (``<home>/ticks/wire``), sorted by ``recv_ns``."""
    return _to_frame(read_shards(list_shards(wire_dir, trading_day, source, WIRE_PREFIX), WIRE_SCHEMA))


@dataclass(frozen=True, slots=True)
class RebuildReport:
    trading_day: str
    polls: int
    rows: int
    shards: tuple[Path, ...]


def rebuild_ticks_from_wire(
    wire_dir: Path,
    out_dir: Path,
    trading_day: str,
    normalizer: SinaNormalizer,
    *,
    clock: Clock | None = None,
    close_grace_s: float = 300.0,
    roll_rows: int = 200_000,
) -> RebuildReport:
    """Re-normalize the archived polls of one trading day into fresh shards under ``out_dir/{td}/sina``.

    Used after a parser fix: the wire shards hold the untouched response
    bytes, so the tick rows can be regenerated deterministically.
    """
    clock = clock or SystemClock()
    wire = read_shards(list_shards(wire_dir, trading_day, SINA_DIR, WIRE_PREFIX), WIRE_SCHEMA)
    if wire.num_rows:
        wire = wire.sort_by("recv_ns")
    classifier = TickClassifier(
        normalizer.calendar, normalizer.instruments.values(), close_grace_s=close_grace_s
    )
    writer = ParquetShardWriter(
        out_dir,
        SINA_DIR,
        TICK_SCHEMA,
        prefix=TICK_PREFIX,
        epoch_ms=clock.now_ns() // NS_PER_MS,
        clock=clock,
        roll_s=math.inf,
        roll_rows=roll_rows,
        max_pending_rows=max(roll_rows, DEFAULT_MAX_PENDING_ROWS),
    )
    polls = rows = 0
    shards: list[Path] = []
    for rec in wire.to_pylist():
        if rec["http_status"] != 200 or rec["body"] is None:
            continue
        polls += 1
        codes = [c for c in str(rec["codes"] or "").split(",") if normalizer.meta_for(c) is not None]
        results = normalizer.normalize_text(decode_body(rec["body"]), int(rec["recv_ns"]), codes or None)
        for result in results:
            mode, reason = classifier.classify(result, trading_day)
            meta = normalizer.meta_for(result.code)
            row = result_row(
                result, mode, reason, bucket_td=trading_day, exchange=str(meta.exchange) if meta else None
            )
            shards.extend(writer.append(trading_day, row))
            rows += 1
    shards.extend(writer.close())
    return RebuildReport(trading_day, polls, rows, tuple(shards))
