"""Outbox (seq allocation, publish hand-off, replay, purge), processed_commands dedupe, fact_cursors."""

from __future__ import annotations

import threading

import pytest
from dbtest_support import T0, DbFactory
from sqlalchemy import Connection, insert, select

from futrisk.db import FactCursors, Outbox, OutboxRow, ProcessedCommands, ReplayNotFound, request_hash
from futrisk.db.processed import DedupeOutcome
from futrisk.messages import codes, decode_envelope, open_payload
from futrisk.messages.common import Ping, ReplayRequest
from futrisk.messages.enums import CashType, Direction, FactStartPolicy, Offset, Role
from futrisk.messages.envelope import err_reply, ok_reply
from futrisk.messages.orders import CancelRequest, OrderRequest
from futrisk.messages.risk import CashMoveRequest

TOPIC = "sys.eod"


def append(db_outbox: Outbox, conn: Connection, topic: str = TOPIC, n: int = 1, created_ns: int = T0) -> list[int]:
    return [
        db_outbox.append_msg(conn, topic, Ping(), epoch="e1", created_ns=created_ns + i, td="20260928") for i in range(n)
    ]


# ---------------------------------------------------------------------------
# Sequence allocation
# ---------------------------------------------------------------------------


class TestOutboxSeq:
    def test_seq_per_topic_monotonic(self, db_factory: DbFactory) -> None:
        db = db_factory()
        db.open()
        ob = Outbox(db, instance="svc")
        assert db.write(lambda c: append(ob, c, n=3)) == [1, 2, 3]
        assert db.write(lambda c: append(ob, c, "td.exec.ACC1", n=2)) == [1, 2]
        assert db.write(lambda c: append(ob, c)) == [4]
        assert ob.heads() == {TOPIC: 4, "td.exec.ACC1": 2}
        assert ob.last_seq("unknown.topic") == 0

    def test_rolled_back_append_does_not_consume_seq(self, db_factory: DbFactory) -> None:
        db = db_factory()
        db.open()
        ob = Outbox(db, instance="svc")
        db.write(lambda c: append(ob, c))

        def failing(conn: Connection) -> None:
            append(ob, conn)
            raise RuntimeError("business failure")

        with pytest.raises(RuntimeError):
            db.write(failing)
        assert db.write(lambda c: append(ob, c)) == [2]

    def test_seq_survives_purge_and_restart(self, db_factory: DbFactory) -> None:
        db = db_factory()
        db.open()
        ob = Outbox(db, instance="svc")
        db.write(lambda c: append(ob, c, n=5))
        rows = ob.fetch_unpublished()
        db.write(lambda c: ob.mark_published(c, [r.outbox_id for r in rows], T0 + 100))
        purged = db.write(lambda c: ob.purge_published_before(c, T0 + 1_000))
        assert purged == 5 and ob.count_unpublished() == 0
        # everything deleted from outbox, but outbox_topics keeps the counter
        assert db.write(lambda c: append(ob, c)) == [6]
        db.close()
        db2 = db_factory()
        db2.open()
        ob2 = Outbox(db2, instance="svc")
        assert db2.write(lambda c: append(ob2, c)) == [7]
        # outbox ids never reused (AUTOINCREMENT) even after the purge
        ids = [r.outbox_id for r in ob2.fetch_unpublished()]
        assert ids == sorted(ids) and min(ids) > 5

    def test_purge_keeps_unpublished_and_is_batched(self, db_factory: DbFactory) -> None:
        db = db_factory()
        db.open()
        ob = Outbox(db, instance="svc")
        db.write(lambda c: append(ob, c, n=10))
        rows = ob.fetch_unpublished()
        db.write(lambda c: ob.mark_published(c, [r.outbox_id for r in rows[:6]], T0))
        assert db.write(lambda c: ob.purge_published_before(c, T0 + 1, limit=4)) == 4
        assert db.write(lambda c: ob.purge_published_before(c, T0 + 1, limit=4)) == 2
        assert db.write(lambda c: ob.purge_published_before(c, T0 + 1, limit=4)) == 0
        assert ob.count_unpublished() == 4


# ---------------------------------------------------------------------------
# Publish hand-off
# ---------------------------------------------------------------------------


class TestOutboxSink:
    def test_sink_receives_rows_after_commit_in_order(self, db_factory: DbFactory) -> None:
        db = db_factory()
        db.open()
        ob = Outbox(db, instance="svc")
        got: list[OutboxRow] = []
        assert ob.attach_sink(got.extend).result(5) == 0
        gate = threading.Event()
        db.submit(lambda c: gate.wait(5))
        futs = [db.submit(lambda c: append(ob, c, n=2)) for _ in range(5)]
        gate.set()
        for f in futs:
            f.result(5)
        db.flush()
        assert [r.topic_seq for r in got] == list(range(1, 11))
        assert all(r.stream == db.stream_id for r in got)

    def test_nothing_published_when_tx_fails(self, db_factory: DbFactory) -> None:
        db = db_factory()
        db.open()
        ob = Outbox(db, instance="svc")
        got: list[OutboxRow] = []
        ob.attach_sink(got.extend).result(5)

        def failing(conn: Connection) -> None:
            append(ob, conn)
            raise ValueError("no")

        with pytest.raises(ValueError):
            db.write(failing)
        db.flush()
        assert got == []

    def test_attach_sink_republishes_unpublished_rows_first(self, db_factory: DbFactory) -> None:
        db = db_factory()
        db.open()
        ob = Outbox(db, instance="svc")
        db.write(lambda c: append(ob, c, n=3))
        first = ob.fetch_unpublished()
        db.write(lambda c: ob.mark_published(c, [first[0].outbox_id], T0))
        db.close(clean=False)  # crash before the rest was published
        db2 = db_factory()
        db2.open()
        ob2 = Outbox(db2, instance="svc")
        got: list[OutboxRow] = []
        assert ob2.attach_sink(got.extend).result(5) == 2
        db2.write(lambda c: append(ob2, c))
        db2.flush()
        assert [r.topic_seq for r in got] == [2, 3, 4]
        fut = ob2.submit_mark_published([r.outbox_id for r in got], T0 + 5)
        assert fut is not None and fut.result(5) == 3
        assert ob2.count_unpublished() == 0


# ---------------------------------------------------------------------------
# Replay (02 §4.6, ctl.replay)
# ---------------------------------------------------------------------------


class TestReplay:
    def test_replay_rebuilds_original_envelopes(self, db_factory: DbFactory) -> None:
        db = db_factory()
        db.open()
        ob = Outbox(db, instance="trade-gateway")
        got: list[OutboxRow] = []
        ob.attach_sink(got.extend).result(5)
        db.write(lambda c: append(ob, c, n=3))
        db.flush()
        page = ob.replay(TOPIC, 1)
        assert page.complete and page.next_seq == 4 and len(page.envelopes) == 3
        live = [ob.envelope_bytes(r) for r in got]
        assert page.envelopes == live  # byte-identical to what the publisher sends
        env = decode_envelope(page.envelopes[1])
        assert env.src == "trade-gateway" and env.stream == db.stream_id and env.seq == 2
        assert env.ts_ns == T0 + 1 and env.td == "20260928" and env.epoch == "e1"
        assert isinstance(open_payload(env), Ping)

    def test_replay_paging_and_bounds(self, db_factory: DbFactory) -> None:
        db = db_factory()
        db.open()
        ob = Outbox(db, instance="svc")
        db.write(lambda c: append(ob, c, n=12))
        p1 = ob.replay(TOPIC, 3, 10, limit=5)
        assert [decode_envelope(b).seq for b in p1.envelopes] == [3, 4, 5, 6, 7]
        assert p1.next_seq == 8 and not p1.complete
        p2 = ob.replay(TOPIC, p1.next_seq, 10, limit=5)
        assert [decode_envelope(b).seq for b in p2.envelopes] == [8, 9, 10] and p2.complete
        beyond = ob.replay(TOPIC, 11, 50)
        assert [decode_envelope(b).seq for b in beyond.envelopes] == [11, 12] and beyond.complete
        empty = ob.replay(TOPIC, 13)
        assert empty.envelopes == [] and empty.complete and empty.next_seq == 13
        assert ob.replay("never.used", 1).envelopes == []
        batch = ob.replay(TOPIC, 1, 2).to_batch()
        assert batch.topic == TOPIC and len(batch.envelopes) == 2

    def test_replay_of_purged_range_is_not_found(self, db_factory: DbFactory) -> None:
        db = db_factory()
        db.open()
        ob = Outbox(db, instance="svc")
        db.write(lambda c: append(ob, c, n=6))
        rows = ob.fetch_unpublished()
        db.write(lambda c: ob.mark_published(c, [r.outbox_id for r in rows[:4]], T0))
        db.write(lambda c: ob.purge_published_before(c, T0 + 1))
        with pytest.raises(ReplayNotFound):
            ob.replay(TOPIC, 2, 6)
        page = ob.replay(TOPIC, 5, 6)
        assert [decode_envelope(b).seq for b in page.envelopes] == [5, 6]

    def test_replay_request_default_limit(self) -> None:
        assert ReplayRequest(topic=TOPIC, from_seq=1).limit == 500


# ---------------------------------------------------------------------------
# processed_commands (02 §4.7)
# ---------------------------------------------------------------------------


def _cash(ts: int, amount: str = "1000.00") -> CashMoveRequest:
    return CashMoveRequest(
        movement_id="m-1",
        account_id="ACC1",
        type=CashType.DEPOSIT,
        amount=amount,
        reason="top up",
        actor="ops",
        role=Role.ADMIN,
        requested_ns=ts,
    )


class TestProcessedCommands:
    def test_request_hash_ignores_time_fields(self) -> None:
        a = OrderRequest(
            client_order_id="c1", account_id="ACC1", instrument_id="rb2701", direction=Direction.BUY,
            offset=Offset.OPEN, price=3100.0, qty=2, sent_ns=111,
        )
        b = OrderRequest(
            client_order_id="c1", account_id="ACC1", instrument_id="rb2701", direction=Direction.BUY,
            offset=Offset.OPEN, price=3100.0, qty=2, sent_ns=999,
        )
        c = OrderRequest(
            client_order_id="c1", account_id="ACC1", instrument_id="rb2701", direction=Direction.BUY,
            offset=Offset.OPEN, price=3101.0, qty=2, sent_ns=111,
        )
        assert request_hash(a) == request_hash(b) != request_hash(c)
        assert request_hash(_cash(1)) == request_hash(_cash(2)) != request_hash(_cash(1, "2000.00"))
        # dict / bytes inputs; only the top level loses its time fields (nested data keeps them)
        assert request_hash({"x": 1, "ts_ns": 5, "inner": {"y": 2}}) == request_hash({"inner": {"y": 2}, "x": 1, "ts_ns": 9})
        assert request_hash({"x": 1, "inner": {"ts_ns": 5, "y": 2}}) != request_hash({"inner": {"y": 2, "ts_ns": 9}, "x": 1})
        from futrisk.messages import encode

        assert request_hash(encode(a)) == request_hash(a)
        assert len(request_hash(a)) == 64

    def test_check_new_duplicate_conflict(self, db_factory: DbFactory) -> None:
        db = db_factory()
        db.open()
        pc = ProcessedCommands(db)
        req = CancelRequest(command_id="cmd-1", client_order_id="o1", account_id="ACC1")
        h = request_hash(req)
        assert db.read(lambda c: pc.check(c, "cmd-1", h)).outcome == DedupeOutcome.NEW
        first = ok_reply(message="cancelled")

        def tx(conn: Connection) -> None:
            pc.record(
                conn, command_id="cmd-1", command="order.cancel", requester="api-gateway", request_hash=h,
                reply=first, trading_day="20260928", received_ns=T0,
            )

        db.write(tx)
        dup = db.read(lambda c: pc.check(c, "cmd-1", h))
        assert dup.outcome == DedupeOutcome.DUPLICATE and dup.reply is not None
        assert dup.reply.duplicate is True and dup.reply.message == "cancelled" and dup.reply.ok
        entry = db.read(lambda c: pc.lookup(c, "cmd-1"))
        assert entry is not None and entry.request_hash == h and entry.result_code == codes.OK
        assert entry.reply().duplicate is False
        other = request_hash(CancelRequest(command_id="cmd-1", client_order_id="o2", account_id="ACC1"))
        conflict = db.read(lambda c: pc.check(c, "cmd-1", other))
        assert conflict.outcome == DedupeOutcome.CONFLICT and conflict.reply is not None
        assert conflict.reply.code == codes.CONFLICT

    def test_record_error_reply_and_purge(self, db_factory: DbFactory) -> None:
        db = db_factory()
        db.open()
        pc = ProcessedCommands(db)

        def tx(conn: Connection) -> None:
            for td, cid in (("20260925", "a"), ("20260928", "b"), ("20260929", "c")):
                pc.record(
                    conn, command_id=cid, command="risk.reset", requester="api-gateway", request_hash="0" * 64,
                    reply=err_reply(codes.FORBIDDEN, "role"), trading_day=td, received_ns=T0,
                )

        db.write(tx)
        assert db.read(lambda c: pc.lookup(c, "a")).result_code == codes.FORBIDDEN  # type: ignore[union-attr]
        assert db.write(lambda c: pc.purge_before(c, "20260928")) == 1
        assert db.read(lambda c: pc.lookup(c, "a")) is None
        assert db.read(lambda c: pc.lookup(c, "b")) is not None


# ---------------------------------------------------------------------------
# fact_cursors
# ---------------------------------------------------------------------------


class TestFactCursors:
    def test_upsert_in_business_transaction(self, db_factory: DbFactory) -> None:
        db = db_factory()
        db.open()
        cur = FactCursors(db)
        items = db.metadata.tables["items"]

        def apply(conn: Connection, seq: int) -> None:
            conn.execute(insert(items).values(item_id=f"t{seq}", qty=seq))
            cur.upsert(conn, "trade-gateway", "td.exec.ACC1", "stream-1", seq, FactStartPolicy.FROM_BEGINNING, updated_ns=T0)

        db.write(lambda c: apply(c, 1))
        db.write(lambda c: apply(c, 2))

        def failing(conn: Connection) -> None:
            apply(conn, 3)
            raise RuntimeError("crash inside apply")

        with pytest.raises(RuntimeError):
            db.write(failing)
        loaded = cur.load_all()
        c = loaded[("trade-gateway", "td.exec.ACC1")]
        assert c.last_seq == 2 and c.expected == 3 and c.stream == "stream-1"
        assert c.start_policy == FactStartPolicy.FROM_BEGINNING
        assert db.read(lambda conn: len(conn.execute(select(items)).all())) == 2
        assert db.read(lambda conn: cur.get(conn, "trade-gateway", "td.exec.ACC9")) is None
