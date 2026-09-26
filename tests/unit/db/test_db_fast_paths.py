"""Hot-path persistence (risk-engine review follow-up): prepared statements for the outbox append and
the cursor upsert, the public ``PreparedSql``, the lock-based ``Database.write`` hand-off, raw per-job
savepoints in group commits, coalesced ``mark_published`` and background WAL checkpoints."""

from __future__ import annotations

import threading
import time
from pathlib import Path

import pytest
import sqlalchemy.exc
from dbtest_support import T0, DbFactory, make_schema
from sqlalchemy import Connection, func, insert, select

from futrisk.common.clock import SimClock
from futrisk.db import Database, FactCursors, Outbox, OutboxRow, PreparedSql, driver_connection
from futrisk.messages import encode
from futrisk.messages.common import Ping

INSERT_ITEM = PreparedSql("INSERT INTO items (item_id, qty) VALUES (?, ?)")


class TestPreparedSql:
    def test_runs_in_the_job_transaction_and_rolls_back_with_it(self, db_factory: DbFactory) -> None:
        db = db_factory()
        db.open()

        def failing(conn: Connection) -> None:
            INSERT_ITEM(conn, ("a", 1))
            raise ValueError("business failure")

        with pytest.raises(ValueError):
            db.write(failing)
        assert db.write(lambda c: INSERT_ITEM(c, ("b", 2)).rowcount) == 1
        items = db.metadata.tables["items"]
        assert db.read(lambda c: [r[0] for r in c.execute(select(items.c.item_id))]) == ["b"]

    def test_sqlite_errors_become_sqlalchemy_errors(self, db_factory: DbFactory) -> None:
        db = db_factory()
        db.open()
        with pytest.raises(sqlalchemy.exc.IntegrityError):
            db.write(lambda c: INSERT_ITEM(c, ("neg", -1)))  # CHECK qty>0
        with pytest.raises(sqlalchemy.exc.OperationalError):
            db.write(lambda c: PreparedSql("INSERT INTO no_such_table VALUES (1)")(c))
        assert db.write(lambda c: driver_connection(c).in_transaction)

    def test_group_commit_with_prepared_writes_isolates_the_failing_job(self, db_factory: DbFactory) -> None:
        db = db_factory()
        db.open()
        gate = threading.Event()
        blocker = db.submit(lambda c: gate.wait(5))

        def nested_ok(conn: Connection) -> None:  # jobs may still use SQLAlchemy savepoints themselves
            with conn.begin_nested():
                INSERT_ITEM(conn, ("n1", 1))

        futs = [db.submit(lambda c, i=i: INSERT_ITEM(c, (f"i{i}", i + 1))) for i in range(5)]
        bad = db.submit(lambda c: (INSERT_ITEM(c, ("x", 1)), INSERT_ITEM(c, ("neg", -1))))
        nested = db.submit(nested_ok)
        tail = db.submit(lambda c: INSERT_ITEM(c, ("tail", 1)))
        gate.set()
        blocker.result(5)
        for f in [*futs, nested, tail]:
            f.result(5)
        with pytest.raises(sqlalchemy.exc.IntegrityError):
            bad.result(5)
        items = db.metadata.tables["items"]
        ids = db.read(lambda c: sorted(r[0] for r in c.execute(select(items.c.item_id))))
        assert ids == sorted([*(f"i{i}" for i in range(5)), "n1", "tail"])  # "x" rolled back with its job


class TestWriteHandOff:
    def test_result_exception_and_critical(self, db_factory: DbFactory) -> None:
        db = db_factory()
        db.open()
        assert db.write(lambda c: 42) == 42
        assert db.write(lambda c: c.exec_driver_sql("PRAGMA synchronous").scalar(), critical=True) == 2  # FULL

        class Boom(Exception):
            pass

        with pytest.raises(Boom, match="boom"):
            db.write(lambda c: (_ for _ in ()).throw(Boom("boom")))
        assert db.write(lambda c: "still fine") == "still fine"

    def test_timeout_leaves_the_job_queued(self, db_factory: DbFactory) -> None:
        db = db_factory()
        db.open()
        gate = threading.Event()
        db.submit(lambda c: gate.wait(5))
        done: list[int] = []
        with pytest.raises(TimeoutError):
            db.write(lambda c: done.append(1), timeout=0.05)
        gate.set()
        db.flush(5)
        assert done == [1]

    def test_order_with_submit_is_kept(self, db_factory: DbFactory) -> None:
        db = db_factory()
        db.open()
        seen: list[str] = []
        gate = threading.Event()
        db.submit(lambda c: gate.wait(5))
        f1 = db.submit(lambda c: seen.append("submit-1"))
        t = threading.Thread(target=lambda: db.write(lambda c: seen.append("write")))
        t.start()
        time.sleep(0.05)
        f2 = db.submit(lambda c: seen.append("submit-2"))
        gate.set()
        t.join(5)
        f1.result(5)
        f2.result(5)
        assert seen == ["submit-1", "write", "submit-2"]


class TestOutboxAndCursorFastPaths:
    def test_append_rows_and_hand_off_unchanged(self, db_factory: DbFactory) -> None:
        db = db_factory()
        db.open()
        ob = Outbox(db, instance="tg")
        got: list[OutboxRow] = []
        ob.attach_sink(got.extend).result(5)
        seqs = db.write(lambda c: [ob.append_msg(c, "td.exec.ACC1", Ping(), epoch="e1", created_ns=T0 + i, td="20260928",
                                                 corr_id=f"c{i}") for i in range(3)])
        assert seqs == [1, 2, 3]
        db.flush(5)
        assert [(r.topic_seq, r.epoch, r.td, r.corr_id, r.payload, r.created_ns, r.stream) for r in got] == [
            (i + 1, "e1", "20260928", f"c{i}", encode(Ping()), T0 + i, db.stream_id) for i in range(3)
        ]
        stored = ob.replay("td.exec.ACC1", 1)
        assert len(stored.envelopes) == 3 and stored.complete
        assert [r.outbox_id for r in got] == [r.outbox_id for r in ob.fetch_unpublished()]

    def test_cursor_upsert_insert_then_update(self, db_factory: DbFactory) -> None:
        db = db_factory()
        db.open()
        cur = FactCursors(db)
        db.write(lambda c: cur.upsert(c, "tg", "td.exec.ACC1", "S1", 5, "FROM_BEGINNING", updated_ns=1))
        db.write(lambda c: cur.upsert(c, "tg", "td.exec.ACC1", "S2", 9, "FROM_NOW", updated_ns=2))
        got = cur.load_all()[("tg", "td.exec.ACC1")]
        assert (got.stream, got.last_seq, got.start_policy.value, got.updated_ns) == ("S2", 9, "FROM_NOW", 2)
        with pytest.raises(ValueError):
            db.write(lambda c: cur.upsert(c, "tg", "t", "S", 1, "NOT_A_POLICY", updated_ns=1))

    def test_mark_published_is_coalesced(self, db_factory: DbFactory) -> None:
        db = db_factory()
        db.open()
        ob = Outbox(db, instance="tg")
        seqs = db.write(lambda c: [ob.append_msg(c, "td.exec.ACC1", Ping(), epoch="e", created_ns=i) for i in range(6)])
        rows = ob.fetch_unpublished()
        assert [r.topic_seq for r in rows] == seqs == [1, 2, 3, 4, 5, 6]
        gate = threading.Event()
        db.submit(lambda c: gate.wait(5))
        before = db.jobs_done
        f1 = ob.submit_mark_published([r.outbox_id for r in rows[:2]], 111)
        f2 = ob.submit_mark_published([r.outbox_id for r in rows[2:4]], 222)
        f3 = ob.submit_mark_published([r.outbox_id for r in rows[4:]], 333)
        assert f1 is f2 is f3 is not None  # one queued writer job for the three batches
        gate.set()
        assert f1.result(5) == 6
        assert db.jobs_done - before == 2  # the gate job + one mark job
        published = db.read(lambda c: [r[0] for r in c.execute(
            select(db.tables.outbox.c.published_ns).order_by(db.tables.outbox.c.outbox_id))])  # type: ignore[union-attr]
        assert published == [111, 111, 222, 222, 333, 333]
        f4 = ob.submit_mark_published([rows[0].outbox_id], 999)  # a new job; already marked rows keep their time
        assert f4 is not None and f4 is not f1 and f4.result(5) == 0


class TestBackgroundCheckpoint:
    def _db(self, tmp_path: Path, **kw: object) -> Database:
        return Database(tmp_path / "ck.db", make_schema(), clock=SimClock(T0), **kw)  # type: ignore[arg-type]

    def test_writer_never_checkpoints_and_the_background_thread_does(self, tmp_path: Path) -> None:
        db = self._db(tmp_path, checkpoint_interval_s=0.02)
        db.open()
        try:
            assert db.write(lambda c: c.exec_driver_sql("PRAGMA wal_autocheckpoint").scalar()) == 0
            items = db.metadata.tables["items"]
            db.write(lambda c: c.execute(insert(items), [{"item_id": f"i{i}", "qty": 1} for i in range(2000)]))
            deadline = time.monotonic() + 5
            while db.checkpoints == 0 and time.monotonic() < deadline:
                time.sleep(0.01)
            assert db.checkpoints > 0
            assert db.read(lambda c: c.execute(select(func.count()).select_from(items)).scalar()) == 2000
        finally:
            db.close()
        assert (tmp_path / "ck.db-wal").stat().st_size == 0 if (tmp_path / "ck.db-wal").exists() else True

    def test_continuous_writes_restart_the_wal(self, tmp_path: Path) -> None:
        # PASSIVE cannot catch up while commits keep coming: the RESTART escalation bounds the WAL
        db = self._db(tmp_path, checkpoint_interval_s=0.005, wal_limit_mb=0.25)
        db.open()
        items = db.metadata.tables["items"]
        try:
            deadline = time.monotonic() + 10
            i = 0
            while db.wal_restarts == 0 and time.monotonic() < deadline:
                db.write(lambda c, i=i: c.execute(insert(items).values(item_id=f"i{i}", qty=1)))
                i += 1
            assert db.wal_restarts > 0
            for j in range(i, i + 300):  # the WAL is reused from its start and shrunk to the limit
                db.write(lambda c, j=j: c.execute(insert(items).values(item_id=f"i{j}", qty=1)))
            assert (tmp_path / "ck.db-wal").stat().st_size < 8 * 1024 * 1024
        finally:
            db.close()

    def test_auto_mode_keeps_sqlite_behaviour(self, tmp_path: Path) -> None:
        db = self._db(tmp_path, checkpoint="auto")
        db.open()
        try:
            assert db.write(lambda c: c.exec_driver_sql("PRAGMA wal_autocheckpoint").scalar()) == 1000
            assert not any(t.name == "db-checkpoint-ck" for t in threading.enumerate())
        finally:
            db.close()
        with pytest.raises(ValueError):
            self._db(tmp_path, checkpoint="sometimes")

    def test_close_stops_the_checkpointer(self, tmp_path: Path) -> None:
        db = self._db(tmp_path)
        db.open()
        assert any(t.name == "db-checkpoint-ck" for t in threading.enumerate())
        db.close()
        assert not any(t.name == "db-checkpoint-ck" for t in threading.enumerate())
        (tmp_path / "ck.db").unlink()  # no handle left open (Windows)
