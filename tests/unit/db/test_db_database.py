"""Database: pragmas, single writer, group commit, critical writes, db_meta stream rules, migrations."""

from __future__ import annotations

import asyncio
import threading
from pathlib import Path

import pytest
import sqlalchemy.exc
from dbtest_support import BootClock, DbFactory, make_schema
from sqlalchemy import Connection, MetaData, Table, insert, select, text

from futrisk.db import (
    Database,
    Migration,
    MigrationError,
    MigrationRunner,
    after_commit,
    load_sql_migrations,
    regenerate_stream_id,
    render_ddl,
)
from futrisk.db.meta import read_meta
from futrisk.db.migrations import split_sql
from futrisk.db.types import id_col, int_col

# ---------------------------------------------------------------------------
# Pragmas and connections
# ---------------------------------------------------------------------------


class TestPragmas:
    def test_writer_pragmas(self, db_factory: DbFactory) -> None:
        db = db_factory()
        db.open()

        def q(conn: Connection) -> tuple[object, ...]:
            return (
                conn.exec_driver_sql("PRAGMA journal_mode").scalar(),
                conn.exec_driver_sql("PRAGMA foreign_keys").scalar(),
                conn.exec_driver_sql("PRAGMA busy_timeout").scalar(),
                conn.exec_driver_sql("PRAGMA synchronous").scalar(),
            )

        assert db.write(q) == ("wal", 1, 5000, 1)  # synchronous NORMAL = 1

    def test_full_database_default(self, db_factory: DbFactory) -> None:
        db = db_factory("td", synchronous="FULL")
        db.open()
        assert db.write(lambda c: c.exec_driver_sql("PRAGMA synchronous").scalar()) == 2

    def test_critical_transaction_uses_full_then_restores(self, db_factory: DbFactory) -> None:
        db = db_factory()
        db.open()
        sync_sql = "PRAGMA synchronous"
        assert db.write(lambda c: c.exec_driver_sql(sync_sql).scalar(), critical=True) == 2
        assert db.write(lambda c: c.exec_driver_sql(sync_sql).scalar()) == 1

    def test_read_connections_are_query_only(self, db_factory: DbFactory) -> None:
        db = db_factory()
        db.open()
        items = db.metadata.tables["items"]
        with pytest.raises(sqlalchemy.exc.OperationalError, match="readonly"):
            db.read(lambda c: c.execute(insert(items).values(item_id="x", qty=1)))
        assert db.read(lambda c: c.exec_driver_sql("PRAGMA query_only").scalar()) == 1

    def test_writes_use_begin_immediate(self, db_factory: DbFactory) -> None:
        db = db_factory()
        db.open()
        # while the writer holds a transaction, a reader still sees the last commit (WAL)
        items = db.metadata.tables["items"]
        db.write(lambda c: c.execute(insert(items).values(item_id="a", qty=1)))
        started = threading.Event()
        release = threading.Event()

        def slow(conn: Connection) -> None:
            conn.execute(insert(items).values(item_id="b", qty=2))
            started.set()
            release.wait(5)

        fut = db.submit(slow)
        assert started.wait(5)
        assert db.read(lambda c: [r[0] for r in c.execute(select(items.c.item_id))]) == ["a"]
        release.set()
        fut.result(5)
        assert db.read(lambda c: sorted(r[0] for r in c.execute(select(items.c.item_id)))) == ["a", "b"]


# ---------------------------------------------------------------------------
# Writer thread
# ---------------------------------------------------------------------------


class TestWriter:
    def test_group_commit_isolates_failing_job(self, db_factory: DbFactory) -> None:
        db = db_factory()
        db.open()
        items = db.metadata.tables["items"]
        gate = threading.Event()
        blocker = db.submit(lambda c: gate.wait(5))  # queue up jobs behind this one
        futs = [db.submit(lambda c, i=i: c.execute(insert(items).values(item_id=f"i{i}", qty=i + 1))) for i in range(10)]
        bad = db.submit(lambda c: c.execute(insert(items).values(item_id="neg", qty=-1)))  # CHECK violation
        tail = db.submit(lambda c: c.execute(insert(items).values(item_id="tail", qty=1)))
        gate.set()
        blocker.result(5)
        for f in futs:
            f.result(5)
        tail.result(5)
        with pytest.raises(sqlalchemy.exc.IntegrityError):
            bad.result(5)
        ids = db.read(lambda c: sorted(r[0] for r in c.execute(select(items.c.item_id))))
        assert "neg" not in ids and "tail" in ids and len(ids) == 11

    def test_after_commit_hooks_run_in_order_and_drop_on_rollback(self, db_factory: DbFactory) -> None:
        db = db_factory()
        db.open()
        seen: list[str] = []

        def ok(tag: str):  # type: ignore[no-untyped-def]
            def job(conn: Connection) -> None:
                after_commit(conn, lambda: seen.append(tag))

            return job

        def failing(conn: Connection) -> None:
            after_commit(conn, lambda: seen.append("never"))
            raise ValueError("boom")

        gate = threading.Event()
        db.submit(lambda c: gate.wait(5))
        f1 = db.submit(ok("a"))
        f2 = db.submit(failing)
        f3 = db.submit(ok("b"))
        gate.set()
        f1.result(5)
        f3.result(5)
        with pytest.raises(ValueError):
            f2.result(5)
        assert seen == ["a", "b"]

    def test_write_from_writer_thread_is_refused(self, db_factory: DbFactory) -> None:
        db = db_factory()
        db.open()

        def nested(conn: Connection) -> None:
            db.write(lambda c: None)

        with pytest.raises(RuntimeError, match="writer thread"):
            db.write(nested)

    def test_submit_after_close_raises(self, db_factory: DbFactory) -> None:
        db = db_factory()
        db.open()
        db.close()
        with pytest.raises(RuntimeError, match="not open"):
            db.submit(lambda c: None)

    async def test_run_write_and_run_read(self, db_factory: DbFactory) -> None:
        db = db_factory()
        db.open()
        items = db.metadata.tables["items"]
        await db.run_write(lambda c: c.execute(insert(items).values(item_id="z", qty=3)))
        got = await db.run_read(lambda c: c.execute(select(items.c.qty)).scalar())
        assert got == 3
        assert not asyncio.get_running_loop().is_closed()


# ---------------------------------------------------------------------------
# db_meta: stream id and clean shutdown (03 §4.0, DB-11)
# ---------------------------------------------------------------------------


class TestStreamRules:
    def _meta(self, db: Database) -> dict[str, object]:
        row = db.read(lambda c: read_meta(c, db.tables))
        assert row is not None
        return row

    def test_new_db_gets_stream_and_clean_flag_cycle(self, db_factory: DbFactory) -> None:
        db = db_factory()
        info = db.open()
        assert info.new_db and len(info.stream_id) == 36 and info.applied_migrations == (1,)
        assert self._meta(db)["clean_shutdown"] == 0
        db.close()
        db2 = db_factory()
        info2 = db2.open()
        assert not info2.new_db and info2.previous_clean_shutdown is True
        assert info2.stream_id == info.stream_id and not info2.stream_regenerated
        assert self._meta(db2)["clean_shutdown"] == 0

    def test_process_crash_keeps_stream(self, db_factory: DbFactory) -> None:
        db = db_factory()
        s1 = db.open().stream_id
        db.close(clean=False)  # crash: clean_shutdown stays 0, same host boot
        info = db_factory().open()
        assert info.previous_clean_shutdown is False and not info.boot_changed
        assert info.stream_id == s1 and not info.stream_regenerated

    def test_power_loss_regenerates_stream_for_normal_db(self, db_factory: DbFactory, boot: BootClock) -> None:
        db = db_factory()
        s1 = db.open().stream_id
        db.close(clean=False)
        boot.ns += 3_600 * 10**9  # host rebooted
        info = db_factory().open()
        assert info.boot_changed and info.stream_regenerated and info.stream_id != s1

    def test_power_loss_keeps_stream_for_full_db(self, db_factory: DbFactory, boot: BootClock) -> None:
        db = db_factory("td", synchronous="FULL")
        s1 = db.open().stream_id
        db.close(clean=False)
        boot.ns += 3_600 * 10**9
        info = db_factory("td", synchronous="FULL").open()
        assert info.boot_changed and not info.stream_regenerated and info.stream_id == s1

    def test_reboot_after_clean_shutdown_keeps_stream(self, db_factory: DbFactory, boot: BootClock) -> None:
        db = db_factory()
        s1 = db.open().stream_id
        db.close(clean=True)
        boot.ns += 3_600 * 10**9
        info = db_factory().open()
        assert info.stream_id == s1 and not info.stream_regenerated

    def test_restore_script_regenerates_stream(self, db_factory: DbFactory) -> None:
        db = db_factory()
        s1 = db.open().stream_id
        s2 = db.write(lambda c: regenerate_stream_id(c, db.tables, restored_from="backup/20260928/svc.db"))
        assert s2 != s1
        meta = self._meta(db)
        assert meta["stream_id"] == s2 and meta["restored_from"] == "backup/20260928/svc.db"


# ---------------------------------------------------------------------------
# Migrations (03 §9)
# ---------------------------------------------------------------------------


class TestMigrations:
    def test_checksum_mismatch_refuses_to_start(self, db_factory: DbFactory) -> None:
        db = db_factory()
        db.open()
        db.close()
        md = make_schema()
        Table("extra", md, id_col("k", primary_key=True))  # schema changed but same version 1
        with pytest.raises(MigrationError, match="checksum mismatch"):
            db_factory(metadata=md).open()

    def test_unknown_applied_version_refuses_to_start(self, db_factory: DbFactory) -> None:
        md = make_schema()
        base = Migration.create_all(1, "initial schema", md)
        v2 = Migration.sql_script(2, "add table", "CREATE TABLE t2 (a INTEGER);")
        db = db_factory(metadata=md, migrations=[base, v2])
        assert db.open().applied_migrations == (1, 2)
        db.close()
        with pytest.raises(MigrationError, match="unknown to this build"):
            db_factory(metadata=md, migrations=[base]).open()

    def test_new_migration_applies_once(self, db_factory: DbFactory) -> None:
        md = make_schema()
        base = Migration.create_all(1, "initial schema", md)
        db = db_factory(metadata=md, migrations=[base])
        db.open()
        db.close()
        v2 = Migration.sql_script(2, "add column", "ALTER TABLE items ADD COLUMN note TEXT;")
        db2 = db_factory(metadata=md, migrations=[base, v2])
        assert db2.open().applied_migrations == (2,)
        assert db2.read(lambda c: [r[1] for r in c.exec_driver_sql("PRAGMA table_info(items)")])[-1] == "note"
        db2.close()
        db3 = db_factory(metadata=md, migrations=[base, v2])
        assert db3.open().applied_migrations == ()

    def test_failed_migration_rolls_back_everything(self, db_factory: DbFactory, tmp_path: Path) -> None:
        md = make_schema()
        bad = Migration.sql_script(2, "broken", "CREATE TABLE ok_table (a INTEGER); CREATE TABLE broken (;")
        db = db_factory(metadata=md, migrations=[Migration.create_all(1, "initial schema", md), bad])
        with pytest.raises(sqlalchemy.exc.OperationalError):
            db.open()
        import sqlite3

        con = sqlite3.connect(tmp_path / "svc.db")
        try:
            names = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        finally:
            con.close()
        assert "ok_table" not in names and "items" not in names

    def test_sql_files_and_split(self, tmp_path: Path) -> None:
        d = tmp_path / "migrations"
        d.mkdir()
        (d / "0002_trigger.sql").write_text(
            "CREATE TABLE a (x TEXT);\nCREATE TRIGGER t AFTER INSERT ON a BEGIN SELECT 1; END;\n-- trailing comment\n",
            encoding="utf-8",
        )
        (d / "0001_base.sql").write_text("CREATE TABLE b (y TEXT DEFAULT 'a;b');\r\n", encoding="utf-8")
        migs = load_sql_migrations(d)
        assert [m.version for m in migs] == [1, 2] and migs[0].description == "base"
        assert len(split_sql(migs[1].sql or "")) == 2
        assert len(split_sql(migs[0].sql or "")) == 1
        # CRLF-insensitive checksum
        assert Migration.sql_script(1, "x", "A;\r\nB;").checksum == Migration.sql_script(1, "x", "A;\nB;").checksum

    def test_runner_validates_order(self) -> None:
        with pytest.raises(MigrationError):
            MigrationRunner([Migration.sql_script(2, "b", "SELECT 1;"), Migration.sql_script(1, "a", "SELECT 1;")])
        with pytest.raises(MigrationError):
            MigrationRunner([Migration.sql_script(1, "a", "SELECT 1;"), Migration.sql_script(1, "b", "SELECT 1;")])

    def test_callable_migration_and_render_ddl(self, db_factory: DbFactory) -> None:
        md = make_schema()
        calls: list[int] = []

        def seed(conn: Connection) -> None:
            calls.append(1)
            conn.execute(text("INSERT INTO items (item_id, qty) VALUES ('seed', 1)"))

        migs = [Migration.create_all(1, "initial schema", md), Migration.from_callable(2, "seed", seed)]
        db = db_factory(metadata=md, migrations=migs)
        db.open()
        assert calls == [1]
        ddl = render_ddl(md)
        assert "CREATE TABLE outbox" in ddl and "ix_outbox__unpublished" in ddl and "uq_outbox__topic_seq" in ddl

    def test_missing_db_meta_is_reported(self, db_factory: DbFactory) -> None:
        md = make_schema()
        # a schema whose migrations never create db_meta
        mig = Migration.sql_script(1, "only items", "CREATE TABLE items2 (a INTEGER);")
        with pytest.raises(RuntimeError, match="db_meta"):
            db_factory(metadata=md, migrations=[mig]).open()


class TestCommonTables:
    def test_exact_columns(self) -> None:
        md = make_schema()
        cols = {t: [c.name for c in md.tables[t].columns] for t in md.tables}
        assert cols["db_meta"] == ["id", "stream_id", "created_ns", "restored_from", "clean_shutdown", "host_boot_ns"]
        assert cols["outbox_topics"] == ["topic", "last_seq", "updated_ns"]
        assert cols["schema_version"] == ["version", "description", "checksum", "applied_ns"]
        assert cols["outbox"] == [
            "outbox_id", "topic", "topic_seq", "type", "v", "epoch", "td", "corr_id", "payload", "created_ns", "published_ns",
        ]
        assert cols["processed_commands"] == [
            "command_id", "command", "requester", "request_hash", "result_code", "reply", "trading_day", "received_ns",
        ]
        assert cols["fact_cursors"] == ["src", "topic", "stream", "last_seq", "start_policy", "updated_ns"]
        assert {i.name for i in md.tables["processed_commands"].indexes} == {"ix_processed_commands__td"}

    def test_optional_tables(self) -> None:
        md = MetaData()
        from futrisk.db import add_common_tables

        t = add_common_tables(md, outbox=False, fact_cursors=False)
        assert t.outbox is None and t.fact_cursors is None and t.processed_commands is not None
        assert add_common_tables(md, outbox=False, fact_cursors=False).db_meta is t.db_meta  # idempotent

    def test_db_meta_single_row_check(self, db_factory: DbFactory) -> None:
        db = db_factory()
        db.open()
        meta = db.tables.db_meta
        with pytest.raises(sqlalchemy.exc.IntegrityError):
            db.write(lambda c: c.execute(insert(meta).values(id=2, stream_id="x", created_ns=1, clean_shutdown=0)))

    def test_items_int_col_roundtrip(self, db_factory: DbFactory) -> None:
        md = make_schema()
        Table("big", md, id_col("k", primary_key=True), int_col("n", nullable=False))
        db = db_factory(metadata=md)
        db.open()
        big = md.tables["big"]
        db.write(lambda c: c.execute(insert(big).values(k="a", n=2**62)))
        assert db.read(lambda c: c.execute(select(big.c.n)).scalar()) == 2**62
