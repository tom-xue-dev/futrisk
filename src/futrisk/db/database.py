"""SQLite database of one service: pragmas, single writer thread, read pool (03 DB-02..DB-04, DB-11, §7).

Design:

* **One writer per database** (DB-01/DB-02). A dedicated thread owns the only
  write connection and executes jobs from a FIFO queue. Every job runs inside
  ``BEGIN IMMEDIATE`` ... ``COMMIT``; consecutive non-critical jobs are group
  committed (each inside its own SAVEPOINT, so a failing job only rolls back
  itself). :meth:`Database.submit` returns a ``concurrent.futures.Future`` that
  resolves after the commit; :meth:`Database.write` blocks on it (sync
  services) and :meth:`Database.run_write` awaits it (asyncio services).
* **Pragmas** (DB-03): ``journal_mode=WAL``, ``foreign_keys=ON``,
  ``busy_timeout=5000`` everywhere; ``synchronous`` is the database default
  (``NORMAL`` for md/risk, ``FULL`` for the others). ``critical=True`` runs a
  single transaction with ``synchronous=FULL`` and restores the default after
  the commit (SQLite forbids changing it inside a transaction).
* **Reads** use a small pool of ``query_only=ON`` connections, each call in its
  own deferred transaction (a consistent WAL snapshot).
* **After-commit hooks**: code running inside a job can register callbacks
  with :func:`after_commit`; they run on the writer thread right after the
  commit, in commit order, and are dropped when the job rolls back. The
  :class:`~futrisk.db.outbox.Outbox` uses this to hand committed FACT rows to
  the publisher (02 §8.1 "persist, then publish").
* **WAL checkpoints in the background** (``checkpoint="background"``, the
  default): SQLite's auto-checkpoint would run inside the commit that crosses
  1000 WAL pages, on the writer thread — with ``synchronous=NORMAL`` that is
  where the fsyncs happen: 3-5 ms stalls every few hundred commits. Instead
  the writer connection has ``wal_autocheckpoint=0`` and a separate
  connection runs ``PRAGMA wal_checkpoint(PASSIVE)`` every
  ``checkpoint_interval_s`` (same ``synchronous``, so the same durability;
  PASSIVE never blocks the writer). PASSIVE cannot finish while commits keep
  arriving, so when the WAL still holds ``wal_limit_mb`` after it, a
  ``RESTART`` checkpoint (waits at most 20 ms for readers/the writer, then
  briefly blocks new commits) lets the next commit restart the WAL; the
  writer's ``journal_size_limit`` then shrinks the file. ``checkpoint="auto"``
  keeps SQLite's behaviour.
* **Start / stop bookkeeping**: :meth:`open` runs the migrations and the
  ``db_meta`` rules of :mod:`futrisk.db.meta` in one critical transaction;
  :meth:`close` drains the queue and marks ``clean_shutdown = 1``.

Job functions receive a :class:`sqlalchemy.Connection` that is already inside
a transaction: they must not call ``commit``/``rollback``; raising rolls the
job back and the exception is delivered through the future.

Hot paths (``order.submit`` persists in well under a millisecond): the
transaction frame (``BEGIN IMMEDIATE``, the per-job ``SAVEPOINT`` of a group
commit) runs straight on the sqlite3 connection, and :class:`PreparedSql`
gives services the same for their own fixed statements — the outbox append
and the cursor upsert use it. SQLAlchemy Core costs ~100-400 µs per statement
in Python (statement construction, cache key, execution context); a prepared
sqlite3 statement costs ~10-30 µs.
"""

from __future__ import annotations

import _thread
import asyncio
import inspect
import queue
import sqlite3
import threading
from collections import deque
from collections.abc import Callable, Sequence
from concurrent.futures import Future
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, TypeVar, cast

import psutil
from loguru import logger
from sqlalchemy import Connection, Engine, MetaData, create_engine, event
from sqlalchemy.engine import URL
from sqlalchemy.exc import DBAPIError
from sqlalchemy.pool import NullPool, QueuePool

from futrisk.common.clock import Clock, SystemClock
from futrisk.db.common_tables import CommonTables, common_tables_of
from futrisk.db.meta import BOOT_TOLERANCE_NS, bootstrap_meta, mark_clean_shutdown
from futrisk.db.migrations import Migration, MigrationRunner

T = TypeVar("T")
Synchronous = Literal["NORMAL", "FULL"]
Checkpoint = Literal["background", "auto"]

_TX_KEY = "futrisk.tx"
_STOP = object()


@dataclass
class TxState:
    """Per-job transaction context: after-commit hooks and scratch data."""

    hooks: list[Callable[[], None]] = field(default_factory=list)
    data: dict[str, Any] = field(default_factory=dict)


def tx_state(conn: Connection) -> TxState:
    """The :class:`TxState` of the writer job running on ``conn``."""
    st = conn.info.get(_TX_KEY)
    if st is None:
        raise RuntimeError("not inside a Database writer job (use Database.submit/write/run_write)")
    return cast(TxState, st)


def current_tx(conn: Connection) -> TxState | None:
    """The :class:`TxState` of the running writer job, or ``None`` outside the writer."""
    st = conn.info.get(_TX_KEY)
    return st if isinstance(st, TxState) else None


def after_commit(conn: Connection, fn: Callable[[], None]) -> None:
    """Run ``fn`` on the writer thread after the current job commits (dropped on rollback)."""
    tx_state(conn).hooks.append(fn)


def reject_async(fn: object, what: str) -> None:
    """Raise ``TypeError`` for ``async def`` callables where a synchronous one is required."""
    if inspect.iscoroutinefunction(fn):
        raise TypeError(f"{what} must be a synchronous function, got a coroutine function {fn!r}")


def reject_awaitable(result: object, what: str) -> None:
    """Raise ``TypeError`` when a callback returned an awaitable (it would never run)."""
    if inspect.isawaitable(result):
        close = getattr(result, "close", None)
        if close is not None:
            close()  # avoid "coroutine was never awaited"
        raise TypeError(f"{what} must be synchronous but returned an awaitable")


def host_boot_ns() -> int:
    """Host boot time in UTC ns (psutil)."""
    return int(psutil.boot_time() * 1_000_000_000)


def driver_connection(conn: Connection) -> sqlite3.Connection:
    """The ``sqlite3`` connection under ``conn`` (same transaction); see :class:`PreparedSql`."""
    return cast(sqlite3.Connection, conn.connection.driver_connection)


class PreparedSql:
    """A fixed SQL statement run straight on the sqlite3 connection of ``conn`` (hot paths).

    ``stmt(conn, params)`` executes inside the caller's transaction / savepoint
    exactly like ``conn.execute(...)``, but without SQLAlchemy's per-statement
    work (statement construction, cache key, execution context, result
    wrapping): ~10-30 µs instead of ~100-400 µs. sqlite3 keeps the compiled
    statement in its per-connection cache, so the SQL is parsed once. It
    returns the ``sqlite3.Cursor`` (``fetchone()``, ``lastrowid``, ``rowcount``).

    Parameters are positional (``?``) and bound as they are: use it for
    columns that need no SQLAlchemy type processing (int, str, bytes, None;
    convert enums / decimals / JSON yourself). Errors are raised as the usual
    ``sqlalchemy.exc`` classes (``IntegrityError``, ``OperationalError`` ...).
    Build the object once (module or instance attribute), not per call.
    """

    __slots__ = ("sql",)

    def __init__(self, sql: str) -> None:
        self.sql = sql

    def __call__(self, conn: Connection, params: Sequence[Any] = ()) -> sqlite3.Cursor:
        try:
            return cast(sqlite3.Connection, conn.connection.driver_connection).execute(self.sql, params)
        except sqlite3.Error as exc:
            raise cast(BaseException, DBAPIError.instance(self.sql, params, exc, sqlite3.Error)) from exc

    def __repr__(self) -> str:
        return f"PreparedSql({self.sql!r})"


_BEGIN_IMMEDIATE = PreparedSql("BEGIN IMMEDIATE")
_BEGIN_DEFERRED = PreparedSql("BEGIN")
_SAVEPOINT = PreparedSql("SAVEPOINT futrisk_job")
_RELEASE = PreparedSql("RELEASE SAVEPOINT futrisk_job")
_ROLLBACK_TO = PreparedSql("ROLLBACK TO SAVEPOINT futrisk_job")


@dataclass(frozen=True)
class DbOpenInfo:
    """What :meth:`Database.open` found and did."""

    path: Path
    new_db: bool
    stream_id: str
    stream_regenerated: bool
    previous_clean_shutdown: bool | None
    boot_changed: bool
    applied_migrations: tuple[int, ...]
    schema_version: int


class _Job:
    """A writer job. It completes a ``Future`` (:meth:`Database.submit`, ``run_write``) or, for
    :meth:`Database.write`, a pre-acquired raw lock: a lock hand-back costs about half of a
    ``Future`` round trip (no ``Condition``) and has a much thinner tail."""

    __slots__ = ("critical", "done", "error", "fn", "future", "result", "waiter")

    def __init__(
        self,
        fn: Callable[[Connection], Any],
        critical: bool,
        future: Future[Any] | None,
        waiter: _thread.LockType | None = None,
    ) -> None:
        self.fn = fn
        self.critical = critical
        self.future = future
        self.waiter = waiter
        self.result: Any = None
        self.error: BaseException | None = None
        self.done = False

    def start(self) -> bool:
        """False when the caller cancelled the future before the job ran."""
        return self.future.set_running_or_notify_cancel() if self.future is not None else True

    def finished(self) -> bool:
        return self.future.done() if self.future is not None else self.done

    def succeed(self, res: Any) -> None:
        if self.future is not None:
            self.future.set_result(res)
        elif not self.done:
            self.result = res
            self._release()

    def fail(self, exc: BaseException) -> None:
        if self.future is not None:
            self.future.set_exception(exc)
        elif not self.done:
            self.error = exc
            self._release()

    def _release(self) -> None:
        self.done = True
        assert self.waiter is not None
        self.waiter.release()


def _begin_immediate(conn: Connection) -> None:
    _BEGIN_IMMEDIATE(conn)


def _begin_deferred(conn: Connection) -> None:
    _BEGIN_DEFERRED(conn)


class Database:
    """One SQLite database file with a single writer thread and a read pool.

    Args:
        path: database file (``<home>/db/<name>.db``); parent dirs are created.
        metadata: schema; must contain the common tables (``add_common_tables``).
        name: label for logs/thread names (default: file stem).
        synchronous: default ``PRAGMA synchronous`` (``NORMAL`` or ``FULL``, DB-03).
        migrations: ordered migrations; default ``[Migration.create_all(1, ..., metadata)]``.
        clock: used for ``created_ns``/``applied_ns`` timestamps.
        boot_time_ns: host boot time provider (tests inject a fake).
        max_batch: maximum number of non-critical jobs group-committed together.
        checkpoint: ``"background"`` (default: WAL checkpoints on a separate
            connection, never inside a commit) or ``"auto"`` (SQLite's
            auto-checkpoint on the writer).
        checkpoint_interval_s: period of the background PASSIVE checkpoint.
        wal_limit_mb: WAL size that triggers a background RESTART checkpoint
            (and the writer's ``journal_size_limit``).
    """

    def __init__(
        self,
        path: str | Path,
        metadata: MetaData,
        *,
        name: str | None = None,
        synchronous: Synchronous = "NORMAL",
        migrations: Sequence[Migration] | None = None,
        clock: Clock | None = None,
        boot_time_ns: Callable[[], int] | None = None,
        max_batch: int = 64,
        read_pool_size: int = 4,
        boot_tolerance_ns: int = BOOT_TOLERANCE_NS,
        checkpoint: Checkpoint = "background",
        checkpoint_interval_s: float = 0.05,
        wal_limit_mb: float = 128.0,
    ) -> None:
        sync = str(synchronous).upper()
        if sync not in ("NORMAL", "FULL"):
            raise ValueError(f"synchronous must be NORMAL or FULL, got {synchronous!r}")
        if checkpoint not in ("background", "auto"):
            raise ValueError(f"checkpoint must be 'background' or 'auto', got {checkpoint!r}")
        self._checkpoint_mode: Checkpoint = checkpoint
        self._checkpoint_interval_s = max(0.005, checkpoint_interval_s)
        self._wal_limit_bytes = max(64 * 1024, int(wal_limit_mb * 1024 * 1024))
        self._ckpt_stop = threading.Event()
        self._ckpt_thread: threading.Thread | None = None
        self.checkpoints = 0  # background PASSIVE checkpoints that found WAL frames (stats / tests)
        self.wal_restarts = 0  # background RESTART checkpoints that completed
        self._path = Path(path)
        self._metadata = metadata
        self.tables: CommonTables = common_tables_of(metadata)
        self._name = name or self._path.stem
        self._sync = cast(Synchronous, sync)
        if migrations is None:
            migrations = [Migration.create_all(1, "initial schema", metadata)]
        self._runner = MigrationRunner(migrations)
        self._clock = clock or SystemClock()
        self._boot_time_ns = boot_time_ns or host_boot_ns
        self._boot_tolerance_ns = boot_tolerance_ns
        self._max_batch = max(1, max_batch)
        self._read_pool_size = max(1, read_pool_size)

        self._queue: queue.SimpleQueue[object] = queue.SimpleQueue()
        self._lock = threading.Lock()
        self._accepting = False
        self._thread: threading.Thread | None = None
        self._w_engine: Engine | None = None
        self._r_engine: Engine | None = None
        self._raw: Any = None  # sqlite3.Connection of the writer
        self._start_error: BaseException | None = None
        self._info: DbOpenInfo | None = None
        self._jobs_done = 0

    # ---------------------------------------------------------------- props
    @property
    def name(self) -> str:
        return self._name

    @property
    def path(self) -> Path:
        return self._path

    @property
    def metadata(self) -> MetaData:
        return self._metadata

    @property
    def synchronous(self) -> Synchronous:
        return self._sync

    @property
    def is_open(self) -> bool:
        return self._accepting

    @property
    def info(self) -> DbOpenInfo:
        if self._info is None:
            raise RuntimeError(f"database {self._name} is not open")
        return self._info

    @property
    def stream_id(self) -> str:
        """``db_meta.stream_id`` of this run (Envelope.stream of FACT messages)."""
        return self.info.stream_id

    def queue_size(self) -> int:
        """Jobs waiting for the writer thread."""
        return self._queue.qsize()

    @property
    def jobs_done(self) -> int:
        return self._jobs_done

    # ------------------------------------------------------------ lifecycle
    def open(self) -> DbOpenInfo:
        """Create engines, start the writer, run migrations and ``db_meta`` bookkeeping."""
        with self._lock:
            if self._thread is not None:
                raise RuntimeError(f"database {self._name} already open")
            self._path.parent.mkdir(parents=True, exist_ok=True)
            url = URL.create("sqlite", database=str(self._path))
            self._w_engine = create_engine(url, poolclass=NullPool, connect_args={"check_same_thread": False})
            event.listen(self._w_engine, "connect", self._on_connect_writer)
            event.listen(self._w_engine, "begin", _begin_immediate)
            self._r_engine = create_engine(
                url,
                poolclass=QueuePool,
                pool_size=self._read_pool_size,
                max_overflow=self._read_pool_size * 2,
                connect_args={"check_same_thread": False},
            )
            event.listen(self._r_engine, "connect", self._on_connect_reader)
            event.listen(self._r_engine, "begin", _begin_deferred)
            ready = threading.Event()
            self._thread = threading.Thread(
                target=self._writer_main, args=(ready,), name=f"db-writer-{self._name}", daemon=True
            )
            self._thread.start()
        ready.wait()
        if self._start_error is not None:
            err = self._start_error
            self._dispose()
            raise err
        with self._lock:
            self._accepting = True
        try:
            self._info = self.submit(self._bootstrap, critical=True).result()
        except BaseException:
            self._stop_writer(timeout=5.0)
            self._dispose()
            raise
        if self._checkpoint_mode == "background":
            self._ckpt_stop.clear()
            self._ckpt_thread = threading.Thread(
                target=self._checkpointer_main, name=f"db-checkpoint-{self._name}", daemon=True
            )
            self._ckpt_thread.start()
        logger.info(
            "db.open",
            db=self._name,
            path=str(self._path),
            new_db=self._info.new_db,
            stream=self._info.stream_id,
            stream_regenerated=self._info.stream_regenerated,
            migrations=list(self._info.applied_migrations),
        )
        return self._info

    def close(self, *, clean: bool = True, timeout: float = 10.0) -> None:
        """Drain pending jobs, mark ``clean_shutdown = 1`` (when ``clean``) and stop the writer.

        ``clean=False`` simulates a crash (tests) or is used when the service
        is aborting because of an unexpected error.
        """
        with self._lock:
            if not self._accepting:
                return
        try:
            self.flush(timeout=timeout)
            if clean:
                self.submit(lambda c: mark_clean_shutdown(c, self.tables), critical=True).result(timeout)
        except Exception:
            logger.exception("db.close.drain_failed", db=self._name)
        self._stop_writer(timeout=timeout)
        self._dispose()
        logger.info("db.close", db=self._name, clean=clean)

    def _stop_writer(self, *, timeout: float) -> None:
        with self._lock:
            self._accepting = False
            self._queue.put(_STOP)
            thread = self._thread
        self._stop_checkpointer(timeout)  # before the writer's final TRUNCATE checkpoint
        if thread is not None:
            thread.join(timeout)
            if thread.is_alive():
                logger.error("db.writer.stuck", db=self._name)

    def _stop_checkpointer(self, timeout: float) -> None:
        ckpt = self._ckpt_thread
        if ckpt is None:
            return
        self._ckpt_stop.set()
        ckpt.join(timeout)
        if ckpt.is_alive():
            logger.error("db.checkpointer.stuck", db=self._name)
        self._ckpt_thread = None

    def _checkpointer_main(self) -> None:
        """Background ``wal_checkpoint(PASSIVE)`` on its own connection (see the module docstring)."""
        try:
            con = sqlite3.connect(str(self._path), isolation_level=None, check_same_thread=False, timeout=5.0)
        except sqlite3.Error:
            logger.exception("db.checkpointer.connect_failed", db=self._name)
            return
        failures = 0
        try:
            # RESTART waits for readers / the writer through the busy handler while it blocks new
            # commits: never longer than 20 ms (it is retried at the next interval)
            con.execute("PRAGMA busy_timeout=20")
            con.execute(f"PRAGMA synchronous={self._sync}")  # the checkpoint's fsyncs follow this setting
            page_size = int(con.execute("PRAGMA page_size").fetchone()[0])
            restart_frames = max(64, self._wal_limit_bytes // max(512, page_size))
            real = SystemClock()  # real time for the backoff (the injected clock may be simulated)
            backoff_ns, next_restart = 1_000_000_000, 0
            while not self._ckpt_stop.wait(self._checkpoint_interval_s):
                try:
                    row = con.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchone()
                    frames = int(row[1]) if row is not None else 0
                    if frames > 0:
                        self.checkpoints += 1
                    # PASSIVE never caught up under a continuous load: RESTART (bounded by busy_timeout)
                    if frames >= restart_frames and real.monotonic_ns() >= next_restart:
                        again = con.execute("PRAGMA wal_checkpoint(RESTART)").fetchone()
                        if again is not None and int(again[0]) == 0:
                            self.wal_restarts += 1
                            backoff_ns = 1_000_000_000
                        else:  # a reader pins the WAL: do not stall the writer every interval
                            next_restart = real.monotonic_ns() + backoff_ns
                            backoff_ns = min(30_000_000_000, backoff_ns * 2)
                except sqlite3.Error as exc:
                    failures += 1
                    if failures in (1, 10, 100) or failures % 1000 == 0:
                        logger.warning("db.checkpoint_failed", db=self._name, error=str(exc), failures=failures)
        finally:
            con.close()

    def _dispose(self) -> None:
        for eng in (self._r_engine, self._w_engine):
            if eng is not None:
                try:
                    eng.dispose()
                except Exception:
                    logger.exception("db.dispose_failed", db=self._name)
        self._r_engine = None
        self._w_engine = None
        self._thread = None

    # ---------------------------------------------------------------- writes
    def submit(self, fn: Callable[[Connection], T], *, critical: bool = False) -> Future[T]:
        """Queue ``fn(conn)`` for the writer thread; the future resolves after COMMIT.

        ``critical=True`` commits with ``synchronous=FULL`` (DB-03 critical transactions).
        """
        reject_async(fn, "a Database job")
        fut: Future[T] = Future()
        with self._lock:
            if not self._accepting:
                raise RuntimeError(f"database {self._name} is not open")
            self._queue.put(_Job(fn, critical, fut))
        return fut

    def write(self, fn: Callable[[Connection], T], *, critical: bool = False, timeout: float | None = None) -> T:
        """Submit and wait for the commit (sync services). Not callable from the writer thread.

        Same queue and ordering as :meth:`submit`; the caller is woken with a
        raw lock instead of a ``Future`` (sync hot path). Raises the job's
        exception, or ``TimeoutError`` after ``timeout`` seconds (the job still
        runs later).
        """
        if threading.current_thread() is self._thread:
            raise RuntimeError("Database.write called from the writer thread (deadlock)")
        reject_async(fn, "a Database job")
        waiter = _thread.allocate_lock()
        waiter.acquire()
        job = _Job(fn, critical, None, waiter)
        with self._lock:
            if not self._accepting:
                raise RuntimeError(f"database {self._name} is not open")
            self._queue.put(job)
        if not waiter.acquire(True, -1 if timeout is None else timeout):
            raise TimeoutError(f"database {self._name}: no commit within {timeout} s")
        err = job.error
        if err is not None:
            try:
                raise err
            finally:  # break the job <-> traceback reference cycle (as Future.result does)
                err = None
                job.error = None
        return cast(T, job.result)

    async def run_write(self, fn: Callable[[Connection], T], *, critical: bool = False) -> T:
        """Submit and await the commit (asyncio services); the loop is never blocked."""
        return await asyncio.wrap_future(self.submit(fn, critical=critical))

    def flush(self, timeout: float | None = None) -> None:
        """Wait until every job submitted so far has committed and its hooks ran."""
        if threading.current_thread() is self._thread:
            return
        self.submit(lambda c: None).result(timeout)

    # ----------------------------------------------------------------- reads
    def read(self, fn: Callable[[Connection], T]) -> T:
        """Run ``fn`` on a ``query_only`` connection inside one read transaction."""
        eng = self._r_engine
        if eng is None:
            raise RuntimeError(f"database {self._name} is not open")
        with eng.connect() as conn, conn.begin():
            return fn(conn)

    async def run_read(self, fn: Callable[[Connection], T]) -> T:
        """:meth:`read` in the default thread pool (asyncio services)."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self.read, fn)

    # -------------------------------------------------------------- internals
    def _on_connect_writer(self, dbapi_conn: Any, _record: Any) -> None:
        dbapi_conn.isolation_level = None  # we emit BEGIN IMMEDIATE ourselves
        cur = dbapi_conn.cursor()
        try:
            cur.execute("PRAGMA journal_mode=WAL")
            mode = cur.fetchone()
            if mode is None or str(mode[0]).lower() != "wal":
                raise RuntimeError(f"could not enable WAL on {self._path}: {mode}")
            cur.execute("PRAGMA foreign_keys=ON")
            cur.execute("PRAGMA busy_timeout=5000")
            cur.execute(f"PRAGMA synchronous={self._sync}")
            if self._checkpoint_mode == "background":
                cur.execute("PRAGMA wal_autocheckpoint=0")  # the checkpointer thread does it, not a commit
                cur.execute(f"PRAGMA journal_size_limit={self._wal_limit_bytes}")  # shrink the WAL after a restart
        finally:
            cur.close()

    @staticmethod
    def _on_connect_reader(dbapi_conn: Any, _record: Any) -> None:
        dbapi_conn.isolation_level = None
        cur = dbapi_conn.cursor()
        try:
            cur.execute("PRAGMA foreign_keys=ON")
            cur.execute("PRAGMA busy_timeout=5000")
            cur.execute("PRAGMA query_only=ON")
        finally:
            cur.close()

    def _bootstrap(self, conn: Connection) -> DbOpenInfo:
        now = self._clock.now_ns()
        applied = self._runner.run(conn, now_ns=now)
        has_meta = conn.exec_driver_sql(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='db_meta'"
        ).first()
        if has_meta is None:
            raise RuntimeError(
                f"{self._path}: table db_meta missing after migrations; include add_common_tables() in the schema"
            )
        st = bootstrap_meta(
            conn,
            self.tables,
            now_ns=now,
            boot_ns=self._boot_time_ns(),
            synchronous=self._sync,
            tolerance_ns=self._boot_tolerance_ns,
        )
        if st.stream_regenerated:
            logger.warning("db.stream_regenerated", db=self._name, stream=st.stream_id, reason="unclean shutdown + host reboot")
        return DbOpenInfo(
            path=self._path,
            new_db=st.new_db,
            stream_id=st.stream_id,
            stream_regenerated=st.stream_regenerated,
            previous_clean_shutdown=st.previous_clean_shutdown,
            boot_changed=st.boot_changed,
            applied_migrations=tuple(applied),
            schema_version=self._runner.latest_version,
        )

    def _writer_main(self, ready: threading.Event) -> None:
        assert self._w_engine is not None
        try:
            conn = self._w_engine.connect()
            self._raw = conn.connection.driver_connection
        except BaseException as exc:  # pragma: no cover - environment failure
            self._start_error = exc
            ready.set()
            return
        ready.set()
        lookahead: deque[object] = deque()
        try:
            while True:
                item = lookahead.popleft() if lookahead else self._queue.get()
                if item is _STOP:
                    break
                job = cast(_Job, item)
                batch = [job]
                if not job.critical and self._max_batch > 1:
                    # this thread is the only consumer: not empty() means get_nowait() succeeds
                    while len(batch) < self._max_batch and not self._queue.empty():
                        nxt = self._queue.get_nowait()
                        if nxt is _STOP or cast(_Job, nxt).critical:
                            lookahead.append(nxt)
                            break
                        batch.append(cast(_Job, nxt))
                try:
                    self._run_batch(conn, batch)
                except BaseException:  # pragma: no cover - defensive, keep the writer alive
                    logger.exception("db.writer.batch_failed", db=self._name)
                    for j in batch:
                        if not j.finished():
                            j.fail(RuntimeError("writer batch failed"))
        finally:
            # fail anything still queued so no caller waits forever
            while True:
                try:
                    rest = self._queue.get_nowait()
                except queue.Empty:
                    break
                if isinstance(rest, _Job) and not rest.finished():
                    rest.fail(RuntimeError(f"database {self._name} closed"))
            try:
                self._raw.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            except Exception:
                logger.debug("db.checkpoint_skipped", db=self._name)
            conn.close()

    def _run_hooks(self, st: TxState) -> None:
        for hook in st.hooks:
            try:
                hook()
            except Exception:
                logger.exception("db.after_commit_hook_failed", db=self._name)

    def _run_batch(self, conn: Connection, batch: list[_Job]) -> None:
        live = [j for j in batch if j.start()]
        if not live:
            return
        if len(live) == 1:
            self._run_single(conn, live[0])
            return
        outcomes: list[tuple[_Job, Any, TxState | None, BaseException | None]] = []
        try:
            with conn.begin():
                for job in live:
                    st = TxState()
                    conn.info[_TX_KEY] = st
                    _SAVEPOINT(conn)  # one savepoint per job: a failing job only rolls back itself
                    try:
                        res = job.fn(conn)
                        reject_awaitable(res, "a Database job")
                    except BaseException as exc:
                        _ROLLBACK_TO(conn)
                        _RELEASE(conn)
                        outcomes.append((job, None, None, exc))
                    else:
                        _RELEASE(conn)
                        outcomes.append((job, res, st, None))
                    finally:
                        conn.info.pop(_TX_KEY, None)
        except BaseException as exc:
            for job in live:
                if not job.finished():
                    job.fail(exc)
            return
        for job, res, job_st, err in outcomes:
            self._jobs_done += 1
            if err is not None:
                job.fail(err)
                continue
            assert job_st is not None
            self._run_hooks(job_st)
            job.succeed(res)

    def _run_single(self, conn: Connection, job: _Job) -> None:
        switch = job.critical and self._sync != "FULL"
        st = TxState()
        try:
            if switch:
                self._raw.execute("PRAGMA synchronous=FULL")
            try:
                with conn.begin():
                    conn.info[_TX_KEY] = st
                    res = job.fn(conn)
                    reject_awaitable(res, "a Database job")
            finally:
                conn.info.pop(_TX_KEY, None)
                if switch:
                    try:
                        self._raw.execute(f"PRAGMA synchronous={self._sync}")
                    except Exception:
                        logger.exception("db.restore_synchronous_failed", db=self._name)
        except BaseException as exc:
            self._jobs_done += 1
            job.fail(exc)
            return
        self._jobs_done += 1
        self._run_hooks(st)
        job.succeed(res)


__all__ = [
    "Checkpoint",
    "Database",
    "DbOpenInfo",
    "PreparedSql",
    "Synchronous",
    "TxState",
    "after_commit",
    "current_tx",
    "driver_connection",
    "host_boot_ns",
    "reject_async",
    "reject_awaitable",
    "tx_state",
]
