"""Transactional outbox for FACT topics (03 §4.0 ``outbox``/``outbox_topics``, DB-05, 02 §4.5-§4.6).

Writing a fact::

    def tx(conn):
        conn.execute(insert(trades).values(...))                 # business rows
        outbox.append_msg(conn, topics.td_exec("ACC3"), trade,    # same transaction
                          epoch=rt.epoch, created_ns=clock.now_ns(), td=trade.trading_day)
    db.write(tx)            # sync service: wait for the commit
    await db.run_write(tx)  # asyncio service

* ``topic_seq`` is allocated from ``outbox_topics.last_seq + 1`` inside the
  same transaction (never ``MAX()+1``), so it keeps increasing across restarts
  and retention purges (02 §4.5).
* :meth:`Outbox.append` / :meth:`Outbox.append_msg` are the hot path of every
  FACT (``order.submit`` writes one or more per commit): they run three
  prepared statements straight on the sqlite3 connection
  (:class:`~futrisk.db.database.PreparedSql`, ~20-60 µs per append instead of
  ~350 µs through SQLAlchemy Core), same rows, same sequence allocation.
* After the commit, the writer thread hands the new rows to the *sink*
  (the service publisher) in commit order; :meth:`Outbox.attach_sink` first
  republishes rows left unpublished by a previous run (03 §4.0: "服务启动时，
  把尚未发布的消息按 outbox_id 顺序补发").
* The publisher reports sent rows back through :meth:`submit_mark_published`.
* :meth:`replay` rebuilds the original envelopes from the stored columns
  (``src`` = instance, ``stream`` = ``db_meta.stream_id``, ``ts_ns`` =
  ``created_ns``) and raises :class:`ReplayNotFound` when the start of the
  range has been purged; ``first_available`` tells the caller where replay can
  resume (``ctl.replay`` returns it as ``ReplayBatch.next_seq`` in the
  ``NOT_FOUND`` reply).

Clocks (two different ones, on purpose):

* ``created_ns`` is **market time** (``MarketClock.now_ns()``, what the
  runtime's ``append_fact`` passes). It becomes the envelope ``ts_ns``, like
  every other bus timestamp.
* ``published_ns`` is **wall-clock** UTC ns (``SystemClock``), written by the
  publisher when the row reached the PUB socket. Retention compares
  ``published_ns`` with a wall-clock cutoff (:meth:`Outbox.purge_older_than`),
  so a demo market-clock jump can never purge the 7-day replay window.
"""

from __future__ import annotations

import json
import threading
from collections.abc import Callable, Iterable, Sequence
from concurrent.futures import Future
from dataclasses import dataclass

from loguru import logger
from sqlalchemy import Connection, and_, delete, func, select

from futrisk.common.clock import SystemClock
from futrisk.db.database import Database, PreparedSql, current_tx
from futrisk.messages.base import Msg, encode, type_name, type_version
from futrisk.messages.common import ReplayBatch
from futrisk.messages.envelope import PROTO, Envelope, encode_envelope

MAX_REPLAY_LIMIT = 1000
DEFAULT_REPLAY_LIMIT = 500
REPLAY_MAX_BYTES = 900_000  # keep one ReplayBatch below the 1 MiB message limit (02 §4.3)
_TX_ROWS = "outbox.rows"
_ID_CHUNK = 5000  # ids per UPDATE (a JSON array bound as one parameter)


@dataclass(frozen=True, slots=True)
class OutboxRow:
    """One outbox row plus the database stream it belongs to."""

    outbox_id: int
    topic: str
    topic_seq: int
    type: str
    v: int
    epoch: str
    td: str | None
    corr_id: str | None
    payload: bytes
    created_ns: int
    stream: str
    published_ns: int | None = None


FactSink = Callable[[list[OutboxRow]], None]


class ReplayNotFound(LookupError):
    """The start of the requested range is no longer in the outbox (retention purge).

    ``first_available`` is the first seq still stored at or after ``from_seq``
    (``None`` when nothing is left): ``[from_seq, first_available - 1]`` is gone,
    the rest can still be replayed.
    """

    def __init__(self, topic: str, from_seq: int, to_seq: int, first_available: int | None = None) -> None:
        tail = f"; first available {first_available}" if first_available is not None else ""
        super().__init__(f"{topic}: seq {from_seq}..{to_seq} purged from the outbox{tail}")
        self.topic = topic
        self.from_seq = from_seq
        self.to_seq = to_seq
        self.first_available = first_available


@dataclass(frozen=True)
class ReplayPage:
    """One page of ``ctl.replay`` results."""

    topic: str
    envelopes: list[bytes]
    next_seq: int
    complete: bool

    def to_batch(self) -> ReplayBatch:
        return ReplayBatch(topic=self.topic, envelopes=self.envelopes, next_seq=self.next_seq, complete=self.complete)


def row_envelope(row: OutboxRow, *, src: str) -> Envelope:
    """The envelope of an outbox row; identical for live publish and replay."""
    return Envelope(
        proto=PROTO,
        topic=row.topic,
        type=row.type,
        v=row.v,
        src=src,
        epoch=row.epoch,
        stream=row.stream,
        seq=row.topic_seq,
        ts_ns=row.created_ns,
        td=row.td,
        corr_id=row.corr_id,
        payload=row.payload,
    )


class Outbox:
    """Outbox API bound to one :class:`Database` and the publishing instance id."""

    def __init__(self, db: Database, *, instance: str) -> None:
        if db.tables.outbox is None or db.tables.outbox_topics is None:
            raise ValueError(f"database {db.name} has no outbox tables (add_common_tables(outbox=True))")
        self._db = db
        self._t = db.tables.outbox
        self._tt = db.tables.outbox_topics
        self._instance = instance
        self._sink: FactSink | None = None
        t, tt = self._t.name, self._tt.name
        self._seq_next = PreparedSql(
            f"UPDATE {tt} SET last_seq = last_seq + 1, updated_ns = ? WHERE topic = ? RETURNING last_seq"
        )
        self._seq_first = PreparedSql(f"INSERT INTO {tt} (topic, last_seq, updated_ns) VALUES (?, 1, ?)")
        self._insert = PreparedSql(
            f"INSERT INTO {t} (topic, topic_seq, type, v, epoch, td, corr_id, payload, created_ns) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )
        self._mark = PreparedSql(
            f"UPDATE {t} SET published_ns = ? "
            "WHERE outbox_id IN (SELECT value FROM json_each(?)) AND published_ns IS NULL"
        )
        # coalesced mark_published: at most one queued writer job takes every pending batch
        self._mp_lock = threading.Lock()
        self._mp_pending: list[tuple[int, list[int]]] = []
        self._mp_future: Future[int] | None = None

    @property
    def db(self) -> Database:
        return self._db

    @property
    def instance(self) -> str:
        return self._instance

    @property
    def stream(self) -> str:
        return self._db.stream_id

    # -------------------------------------------------------------- envelopes
    def envelope(self, row: OutboxRow) -> Envelope:
        return row_envelope(row, src=self._instance)

    def envelope_bytes(self, row: OutboxRow) -> bytes:
        return encode_envelope(row_envelope(row, src=self._instance))

    # ------------------------------------------------------------------ write
    def append(
        self,
        conn: Connection,
        *,
        topic: str,
        type: str,
        v: int,
        payload: bytes,
        td: str | None = None,
        corr_id: str | None = None,
        epoch: str,
        created_ns: int,
    ) -> int:
        """Insert one FACT row inside the caller's transaction; returns its ``topic_seq``.

        ``created_ns`` is market time (it becomes the envelope ``ts_ns``);
        ``epoch`` is the appending process incarnation (kept for audit and in
        the envelope, also when the row is replayed by a later incarnation).
        Prepared statements on the job's sqlite3 connection (hot path); the row
        is handed to the publisher after the commit.
        """
        if not topic:
            raise ValueError("topic must not be empty")
        seq_row = self._seq_next(conn, (created_ns, topic)).fetchone()
        if seq_row is None:
            self._seq_first(conn, (topic, created_ns))
            seq = 1
        else:
            seq = int(seq_row[0])
        payload = bytes(payload)
        cur = self._insert(conn, (topic, seq, type, v, epoch, td, corr_id, payload, created_ns))
        outbox_id = cur.lastrowid
        assert outbox_id is not None
        row = OutboxRow(
            outbox_id=int(outbox_id),
            topic=topic,
            topic_seq=seq,
            type=type,
            v=v,
            epoch=epoch,
            td=td,
            corr_id=corr_id,
            payload=payload,
            created_ns=created_ns,
            stream=self._db.stream_id,
        )
        st = current_tx(conn)
        if st is not None:
            rows: list[OutboxRow] | None = st.data.get(_TX_ROWS)
            if rows is None:
                rows = []
                st.data[_TX_ROWS] = rows
                committed = rows
                st.hooks.append(lambda: self._dispatch(committed))
            rows.append(row)
        return seq

    def append_msg(
        self,
        conn: Connection,
        topic: str,
        body: Msg,
        *,
        epoch: str,
        created_ns: int,
        td: str | None = None,
        corr_id: str | None = None,
    ) -> int:
        """:meth:`append` for a registered payload object."""
        name = type_name(body)
        return self.append(
            conn,
            topic=topic,
            type=name,
            v=type_version(name),
            payload=encode(body),
            td=td,
            corr_id=corr_id,
            epoch=epoch,
            created_ns=created_ns,
        )

    def _dispatch(self, rows: list[OutboxRow]) -> None:
        sink = self._sink
        if sink is not None and rows:
            sink(list(rows))

    # ------------------------------------------------------------ publishing
    def attach_sink(self, sink: FactSink) -> Future[int]:
        """Republish unpublished rows (outbox_id order) and route future commits to ``sink``.

        Runs as one critical writer job so that no commit can slip between the
        catch-up and the switch; returns the number of republished rows.
        """

        def job(conn: Connection) -> int:
            count = 0
            after = 0
            while True:
                rows = self._select_unpublished(conn, after_id=after, limit=MAX_REPLAY_LIMIT)
                if not rows:
                    break
                sink(rows)
                count += len(rows)
                after = rows[-1].outbox_id
            self._sink = sink
            return count

        return self._db.submit(job, critical=True)

    def detach_sink(self) -> None:
        self._sink = None

    def _select_unpublished(self, conn: Connection, *, after_id: int, limit: int) -> list[OutboxRow]:
        t = self._t
        res = conn.execute(
            select(t)
            .where(t.c.published_ns.is_(None), t.c.outbox_id > after_id)
            .order_by(t.c.outbox_id)
            .limit(limit)
        ).mappings()
        stream = self._db.stream_id
        return [self._row(m, stream) for m in res]

    @staticmethod
    def _row(m: object, stream: str) -> OutboxRow:
        r = dict(m)  # type: ignore[call-overload]
        return OutboxRow(
            outbox_id=int(r["outbox_id"]),
            topic=str(r["topic"]),
            topic_seq=int(r["topic_seq"]),
            type=str(r["type"]),
            v=int(r["v"]),
            epoch=str(r["epoch"]),
            td=r["td"],
            corr_id=r["corr_id"],
            payload=bytes(r["payload"]),
            created_ns=int(r["created_ns"]),
            stream=stream,
            published_ns=r["published_ns"],
        )

    def fetch_unpublished(self, limit: int = MAX_REPLAY_LIMIT, *, conn: Connection | None = None) -> list[OutboxRow]:
        """Unpublished rows in ``outbox_id`` order."""
        if conn is not None:
            return self._select_unpublished(conn, after_id=0, limit=limit)
        return self._db.read(lambda c: self._select_unpublished(c, after_id=0, limit=limit))

    def mark_published(self, conn: Connection, ids: Iterable[int], ns: int) -> int:
        """Set ``published_ns`` (wall-clock UTC ns) for ``ids`` (writer job); returns rows updated."""
        id_list = sorted({int(i) for i in ids})
        n = 0
        for i in range(0, len(id_list), _ID_CHUNK):
            n += int(self._mark(conn, (ns, json.dumps(id_list[i : i + _ID_CHUNK]))).rowcount or 0)
        return n

    def submit_mark_published(self, ids: Sequence[int], ns: int) -> Future[int] | None:
        """Queue :meth:`mark_published` on the writer (fire and forget; failures are logged).

        Calls are coalesced: while a mark job is still queued, further batches
        join it instead of adding one writer transaction each (the returned
        future is that shared job; its result counts every row it marked).
        """
        if not ids:
            return None
        with self._mp_lock:
            self._mp_pending.append((ns, list(ids)))
            fut = self._mp_future
            if fut is not None:
                return fut
            try:
                fut = self._db.submit(self._mark_pending)
            except RuntimeError:
                self._mp_pending.clear()
                logger.debug("outbox.mark_published_skipped", db=self._db.name, reason="database closed")
                return None
            self._mp_future = fut

        def _log(f: Future[int]) -> None:
            exc = f.exception()
            if exc is not None:
                logger.opt(exception=exc).error("outbox.mark_published_failed", db=self._db.name)

        fut.add_done_callback(_log)
        return fut

    def _mark_pending(self, conn: Connection) -> int:
        with self._mp_lock:
            pending, self._mp_pending = self._mp_pending, []
            self._mp_future = None  # batches arriving from now on queue a new job
        return sum(self.mark_published(conn, ids, ns) for ns, ids in pending)

    # ----------------------------------------------------------------- reads
    def heads(self, *, conn: Connection | None = None) -> dict[str, int]:
        """``{topic: last_seq}`` from ``outbox_topics`` (heartbeat ``fact_heads``)."""

        def q(c: Connection) -> dict[str, int]:
            tt = self._tt
            return {str(r[0]): int(r[1]) for r in c.execute(select(tt.c.topic, tt.c.last_seq))}

        return q(conn) if conn is not None else self._db.read(q)

    def last_seq(self, topic: str, *, conn: Connection | None = None) -> int:
        def q(c: Connection) -> int:
            tt = self._tt
            v = c.execute(select(tt.c.last_seq).where(tt.c.topic == topic)).scalar()
            return int(v) if v is not None else 0

        return q(conn) if conn is not None else self._db.read(q)

    def last_rows(self, *, conn: Connection | None = None) -> list[OutboxRow]:
        """The newest stored row of every topic (seeds the LV cache after a restart)."""

        def q(c: Connection) -> list[OutboxRow]:
            t = self._t
            newest = select(t.c.topic, func.max(t.c.topic_seq).label("mx")).group_by(t.c.topic).subquery()
            res = c.execute(
                select(t).join(newest, and_(t.c.topic == newest.c.topic, t.c.topic_seq == newest.c.mx))
            ).mappings()
            stream = self._db.stream_id
            return [self._row(m, stream) for m in res]

        return q(conn) if conn is not None else self._db.read(q)

    def count_unpublished(self, *, conn: Connection | None = None) -> int:
        def q(c: Connection) -> int:
            t = self._t
            return int(c.execute(select(func.count()).select_from(t).where(t.c.published_ns.is_(None))).scalar() or 0)

        return q(conn) if conn is not None else self._db.read(q)

    def replay(
        self,
        topic: str,
        from_seq: int,
        to_seq: int | None = None,
        limit: int = DEFAULT_REPLAY_LIMIT,
        *,
        conn: Connection | None = None,
    ) -> ReplayPage:
        """Envelopes of ``topic`` with ``from_seq <= seq <= to_seq`` (one page).

        ``to_seq=None`` means up to the current head. Requests beyond the head
        return what exists with ``complete=True``. A page stops at the first hole
        (``complete=False``, ``next_seq`` = the hole). Raises
        :class:`ReplayNotFound` (with ``first_available``) when ``from_seq`` is
        no longer stored.
        """

        def q(c: Connection) -> ReplayPage:
            return self._replay(c, topic, from_seq, to_seq, limit)

        return q(conn) if conn is not None else self._db.read(q)

    def _replay(self, c: Connection, topic: str, from_seq: int, to_seq: int | None, limit: int) -> ReplayPage:
        lo = max(1, int(from_seq))
        last = self.last_seq(topic, conn=c)
        hi = last if to_seq is None else min(int(to_seq), last)
        if lo > hi:
            return ReplayPage(topic=topic, envelopes=[], next_seq=lo, complete=True)
        n = max(1, min(int(limit), MAX_REPLAY_LIMIT))
        t = self._t
        res = c.execute(
            select(t)
            .where(t.c.topic == topic, t.c.topic_seq >= lo, t.c.topic_seq <= hi)
            .order_by(t.c.topic_seq)
            .limit(n)
        ).mappings()
        stream = self._db.stream_id
        rows = [self._row(m, stream) for m in res]
        if not rows or rows[0].topic_seq != lo:
            first = rows[0].topic_seq if rows else self._first_stored(c, topic, lo)
            raise ReplayNotFound(topic, lo, hi, first_available=first)
        envelopes: list[bytes] = []
        size = 0
        expect = lo
        for row in rows:
            if row.topic_seq != expect:
                break  # hole: this page ends here; the next request reports NOT_FOUND for it
            b = self.envelope_bytes(row)
            if envelopes and size + len(b) > REPLAY_MAX_BYTES:
                break
            envelopes.append(b)
            size += len(b)
            expect += 1
        next_seq = expect
        return ReplayPage(topic=topic, envelopes=envelopes, next_seq=next_seq, complete=next_seq > hi)

    def _first_stored(self, c: Connection, topic: str, lo: int) -> int | None:
        t = self._t
        v = c.execute(select(func.min(t.c.topic_seq)).where(t.c.topic == topic, t.c.topic_seq >= lo)).scalar()
        return int(v) if v is not None else None

    # ------------------------------------------------------------- retention
    def purge_published_before(self, conn: Connection, ns: int, *, limit: int = 5000) -> int:
        """Delete up to ``limit`` rows published before the **wall-clock** time ``ns`` (writer job; 03 §8).

        ``published_ns`` is wall-clock, so ``ns`` must be too (see
        :meth:`purge_older_than`). ``outbox_topics`` is never purged, so
        sequence numbers never go back. Call repeatedly (one writer job per
        batch) until it returns 0.
        """
        t = self._t
        ids = (
            select(t.c.outbox_id)
            .where(t.c.published_ns.is_not(None), t.c.published_ns < ns)
            .order_by(t.c.outbox_id)
            .limit(limit)
        )
        res = conn.execute(delete(t).where(t.c.outbox_id.in_(ids)))
        return int(res.rowcount or 0)

    def purge_older_than(
        self,
        conn: Connection,
        *,
        days: float = 7.0,
        now_wall_ns: int | None = None,
        limit: int = 5000,
    ) -> int:
        """Retention helper: purge rows published more than ``days`` ago by the wall clock (03 §8)."""
        now = now_wall_ns if now_wall_ns is not None else SystemClock().now_ns()
        return self.purge_published_before(conn, now - int(days * 86_400 * 1_000_000_000), limit=limit)


__all__ = [
    "DEFAULT_REPLAY_LIMIT",
    "MAX_REPLAY_LIMIT",
    "FactSink",
    "Outbox",
    "OutboxRow",
    "ReplayNotFound",
    "ReplayPage",
    "row_envelope",
]
