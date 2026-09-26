"""Regression tests for the code-review fixes in the database layer: async callables rejected,
stable migration checksums (legacy accepted + upgraded, freeze_schema), outbox NOT_FOUND detail,
newest rows per topic, wall-clock retention, request_hash over bytes, run_once."""

from __future__ import annotations

import asyncio
import hashlib
import warnings
from pathlib import Path

import pytest
from dbtest_support import T0, DbFactory, make_schema
from sqlalchemy import Column, Connection, MetaData, Table, insert, select, update

from futrisk.common.clock import SystemClock
from futrisk.db import (
    Migration,
    MigrationRunner,
    Outbox,
    ProcessedCommands,
    ReplayNotFound,
    freeze_schema,
    load_sql_migrations,
    render_ddl,
    request_hash,
    schema_fingerprint,
)
from futrisk.db.common_tables import schema_version_table
from futrisk.db.types import text_col
from futrisk.messages import codes, encode
from futrisk.messages.base import Msg, payload
from futrisk.messages.common import Ping
from futrisk.messages.envelope import Reply, err_reply, ok_reply

# ---------------------------------------------------------------------------
# Async callables (review item 3)
# ---------------------------------------------------------------------------


class TestAsyncRejected:
    def test_async_job_rejected_at_submit_and_at_run(self, db_factory: DbFactory) -> None:
        db = db_factory()
        db.open()
        items = db.metadata.tables["items"]

        async def job(conn: Connection) -> None:
            conn.execute(insert(items).values(item_id="x", qty=1))

        with pytest.raises(TypeError, match="synchronous"):
            db.submit(job)
        with warnings.catch_warnings():
            warnings.simplefilter("error")  # the coroutine must be closed, not leaked
            with pytest.raises(TypeError, match="awaitable"):
                db.write(lambda c: job(c))
        assert db.read(lambda c: c.execute(select(items)).all()) == []

    async def test_run_write_rejects_async(self, db_factory: DbFactory) -> None:
        db = db_factory()
        db.open()

        async def job(conn: Connection) -> None:
            await asyncio.sleep(0)

        with pytest.raises(TypeError):
            await db.run_write(job)


# ---------------------------------------------------------------------------
# Migration checksums (review item 14)
# ---------------------------------------------------------------------------


def _sha(text: str) -> str:
    return hashlib.sha256(text.replace("\r\n", "\n").encode("utf-8")).hexdigest()


class TestMigrationChecksums:
    def test_fingerprint_stable_and_sensitive(self) -> None:
        assert schema_fingerprint(make_schema()) == schema_fingerprint(make_schema())
        md = make_schema()
        Table("extra", md, text_col("k", primary_key=True))
        assert schema_fingerprint(md) != schema_fingerprint(make_schema())
        md2 = make_schema()
        md2.tables["items"].append_column(Column("note", text_col("note").type))
        assert schema_fingerprint(md2) != schema_fingerprint(make_schema())

    def test_legacy_ddl_checksum_accepted_and_upgraded(self, db_factory: DbFactory) -> None:
        db = db_factory()
        db.open()
        sv = schema_version_table()
        legacy = _sha(render_ddl(db.metadata))
        db.write(lambda c: c.execute(update(sv).where(sv.c.version == 1).values(checksum=legacy)))
        db.close()
        db2 = db_factory()
        db2.open()  # accepted
        recorded = db2.read(lambda c: c.execute(select(sv.c.checksum)).scalar())
        assert recorded == Migration.create_all(1, "initial schema", make_schema()).checksum

    def test_freeze_schema_keeps_dev_databases_opening(self, db_factory: DbFactory, tmp_path: Path) -> None:
        db = db_factory()
        db.open()  # created during development with create_all
        db.close()
        mig_dir = tmp_path / "migrations"
        path = freeze_schema(make_schema(), mig_dir)
        assert path.name == "0001_initial_schema.sql"
        with pytest.raises(FileExistsError):
            freeze_schema(make_schema(), mig_dir)
        frozen = load_sql_migrations(mig_dir)
        assert len(frozen[0].alt_checksums) == 2
        db2 = db_factory(migrations=frozen)
        assert db2.open().applied_migrations == ()  # already applied: accepted, record upgraded
        sv = schema_version_table()
        assert db2.read(lambda c: c.execute(select(sv.c.checksum)).scalar()) == frozen[0].checksum
        db2.close()
        fresh = db_factory("fresh", migrations=frozen)
        assert fresh.open().applied_migrations == (1,)
        assert fresh.read(lambda c: c.exec_driver_sql("SELECT count(*) FROM outbox").scalar()) == 0

    def test_runner_still_refuses_unknown_checksums(self) -> None:
        mig = Migration.sql_script(1, "x", "CREATE TABLE t (a INTEGER);")
        assert mig.alt_checksums == ()
        assert MigrationRunner([mig]).latest_version == 1


# ---------------------------------------------------------------------------
# Outbox: NOT_FOUND detail, newest rows, wall-clock retention (items 2, 4, 13)
# ---------------------------------------------------------------------------


class TestOutboxReviewFixes:
    def _filled(self, db_factory: DbFactory, n: int = 10) -> tuple[Outbox, list[int]]:
        db = db_factory()
        db.open()
        ob = Outbox(db, instance="tg")
        for i in range(n):
            db.write(lambda c, i=i: ob.append_msg(c, "td.exec.ACC1", Ping(), epoch=f"e{i}", created_ns=T0 + i))
        db.write(lambda c: ob.append_msg(c, "risk.event", Ping(), epoch="e", created_ns=T0))
        return ob, [r.outbox_id for r in ob.fetch_unpublished()]

    def test_not_found_reports_first_available(self, db_factory: DbFactory) -> None:
        ob, ids = self._filled(db_factory)
        db = ob.db
        db.write(lambda c: ob.mark_published(c, ids, 100))
        db.write(lambda c: ob.purge_published_before(c, 101, limit=5))  # seq 1..5 gone
        with pytest.raises(ReplayNotFound) as ei:
            ob.replay("td.exec.ACC1", 1, 10)
        assert ei.value.first_available == 6 and ei.value.from_seq == 1 and ei.value.to_seq == 10
        db.write(lambda c: ob.purge_published_before(c, 101, limit=100))  # everything gone
        with pytest.raises(ReplayNotFound) as ei2:
            ob.replay("td.exec.ACC1", 3, 10)
        assert ei2.value.first_available is None

    def test_last_rows(self, db_factory: DbFactory) -> None:
        ob, _ = self._filled(db_factory, n=4)
        last = {r.topic: (r.topic_seq, r.epoch) for r in ob.last_rows()}
        assert last == {"td.exec.ACC1": (4, "e3"), "risk.event": (1, "e")}

    def test_retention_uses_wall_clock(self, db_factory: DbFactory) -> None:
        ob, ids = self._filled(db_factory, n=3)
        db = ob.db
        day = 86_400 * 1_000_000_000
        wall_now = SystemClock().now_ns()
        db.write(lambda c: ob.mark_published(c, ids[:2], wall_now - 8 * day))  # older than 7 days
        db.write(lambda c: ob.mark_published(c, ids[2:], wall_now - 1 * day))
        assert db.write(lambda c: ob.purge_older_than(c, days=7, now_wall_ns=wall_now)) == 2
        assert db.write(lambda c: ob.purge_older_than(c, days=7)) == 0  # real wall clock: nothing that old


# ---------------------------------------------------------------------------
# request_hash with bytes, run_once (items 9, 12)
# ---------------------------------------------------------------------------


@payload(v=1)
class BlobReq(Msg, kw_only=True):
    command_id: str
    blob: bytes
    sent_ns: int = 0


class TestIdempotencyPrimitives:
    def test_request_hash_struct_equals_wire_bytes(self) -> None:
        req = BlobReq(command_id="c1", blob=b"\x01\x02", sent_ns=5)
        assert request_hash(req) == request_hash(encode(req))
        assert request_hash(req) == request_hash(BlobReq(command_id="c1", blob=b"\x01\x02", sent_ns=9))
        assert request_hash(req) != request_hash(BlobReq(command_id="c1", blob=b"\x01\x03"))

    def test_run_once(self, db_factory: DbFactory) -> None:
        db = db_factory()
        db.open()
        pc = ProcessedCommands(db)
        items = db.metadata.tables["items"]
        calls: list[str] = []

        def handler(conn: Connection) -> Reply:
            calls.append("x")
            conn.execute(insert(items).values(item_id="i1", qty=1))
            return ok_reply(message="done")

        def run(p: Msg) -> Reply:
            return db.write(
                lambda c: pc.run_once(
                    c, command_id="k1", command="risk.reset", requester="api-gateway", payload=p,
                    trading_day="20260928", received_ns=T0, handler=handler,
                )
            )

        first = run(Ping())
        assert first.ok and not first.duplicate and calls == ["x"]
        again = run(Ping())
        assert again.duplicate and again.message == "done" and calls == ["x"]
        other = run(BlobReq(command_id="k1", blob=b"z"))
        assert other.code == codes.CONFLICT and calls == ["x"]

        def failing(conn: Connection) -> Reply:
            conn.execute(insert(items).values(item_id="i2", qty=1))
            raise RuntimeError("business failure")

        with pytest.raises(RuntimeError):
            db.write(
                lambda c: pc.run_once(
                    c, command_id="k2", command="risk.reset", requester="api-gateway", payload=Ping(),
                    trading_day="20260928", received_ns=T0, handler=failing,
                )
            )
        assert db.read(lambda c: pc.lookup(c, "k2")) is None  # nothing recorded, nothing written
        err = db.write(
            lambda c: pc.run_once(
                c, command_id="k3", command="risk.reset", requester="api-gateway", payload=Ping(),
                trading_day="20260928", received_ns=T0, handler=lambda conn: err_reply(codes.FORBIDDEN, "role"),
            )
        )
        assert err.code == codes.FORBIDDEN
        assert db.read(lambda c: pc.lookup(c, "k3")).result_code == codes.FORBIDDEN  # type: ignore[union-attr]


def test_metadata_untouched() -> None:
    md = MetaData()
    assert schema_fingerprint(md) == "[]"
