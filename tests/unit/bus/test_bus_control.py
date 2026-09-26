"""Standard control handlers (02 §6.1): ctl.ping/status/snapshot/replay/shutdown with a real Outbox."""

from __future__ import annotations

from pathlib import Path

from sqlalchemy import MetaData

from futrisk.bus import ControlHandlers, PublisherCore
from futrisk.bus.rpc_common import RequestContext
from futrisk.common.clock import SimClock
from futrisk.db import Database, Outbox, OutboxRow, add_common_tables
from futrisk.messages import codes, decode_envelope
from futrisk.messages.common import (
    Ping,
    Pong,
    ReplayBatch,
    ReplayRequest,
    ShutdownRequest,
    SnapshotBatch,
    SnapshotRequest,
)
from futrisk.messages.enums import ServiceState
from futrisk.messages.envelope import Envelope
from futrisk.messages.system import Heartbeat


def ctx(command: str) -> RequestContext:
    return RequestContext(
        command=command, requester="api-gateway", identity=b"api-gateway", corr_id="c1", received_ns=1, received_mono_ns=1,
        envelope=Envelope(topic=command, type="Ping", src="api-gateway", epoch="e", ts_ns=1),
    )


def test_control_handlers_end_to_end(tmp_path: Path) -> None:
    md = MetaData()
    add_common_tables(md)
    db = Database(tmp_path / "svc.db", md, clock=SimClock(10))
    db.open()
    try:
        ob = Outbox(db, instance="risk-engine-0")
        core = PublisherCore("risk-engine-0", "ep", SimClock(10))
        sent: list[OutboxRow] = []
        ob.attach_sink(lambda rows: (sent.extend(rows), core.fact_frames(rows))).result(5)
        for _ in range(4):
            db.write(lambda c: ob.append_msg(c, "risk.event", Ping(), epoch="ep", created_ns=11))
        db.flush()
        core.lv_frames("risk.snapshot.ACC1", Ping())
        shutdowns: list[ShutdownRequest] = []
        hb = Heartbeat(instance="risk-engine-0", service="risk-engine", state=ServiceState.RUNNING, pid=1, epoch="ep", app_version="t", started_ns=0)
        h = ControlHandlers(
            instance="risk-engine-0",
            clock=SimClock(99),
            state=lambda: ServiceState.RUNNING,
            status=lambda: hb,
            lv_cache=lambda: core.cache,
            outbox=lambda: ob,
            on_shutdown=shutdowns.append,
        )
        table = h.handlers()
        assert set(table) == {"ctl.ping", "ctl.status", "ctl.snapshot", "ctl.replay", "ctl.shutdown"}

        pong = h.ping("ctl.ping", Ping(), ctx("ctl.ping")).open_as(Pong)
        assert pong.instance == "risk-engine-0" and pong.state == "RUNNING" and pong.ts_ns == 99
        assert h.status("ctl.status", Ping(), ctx("ctl.status")).open_as(Heartbeat) == hb

        snap = h.snapshot("ctl.snapshot", SnapshotRequest(prefixes=["risk."]), ctx("ctl.snapshot")).open_as(SnapshotBatch)
        assert [(i.topic, i.seq) for i in snap.items] == [("risk.event", 4), ("risk.snapshot.ACC1", 1)]
        assert decode_envelope(snap.items[0].envelope).stream == db.stream_id
        bad = h.snapshot("ctl.snapshot", SnapshotRequest(prefixes=[]), ctx("ctl.snapshot"))
        assert bad.code == codes.BAD_REQUEST

        rb = h.replay("ctl.replay", ReplayRequest(topic="risk.event", from_seq=2, to_seq=3), ctx("ctl.replay")).open_as(ReplayBatch)
        assert [decode_envelope(b).seq for b in rb.envelopes] == [2, 3] and rb.complete and rb.next_seq == 4
        big = h.replay("ctl.replay", ReplayRequest(topic="risk.event", from_seq=1, limit=5000), ctx("ctl.replay"))
        assert big.ok and len(big.open_as(ReplayBatch).envelopes) == 4
        assert h.replay("ctl.replay", ReplayRequest(topic="risk.snapshot.ACC1", from_seq=1), ctx("x")).code == codes.BAD_REQUEST
        assert h.replay("ctl.replay", ReplayRequest(topic="risk.event", from_seq=0), ctx("x")).code == codes.BAD_REQUEST
        assert h.replay("ctl.replay", ReplayRequest(topic="risk.event", from_seq=3, to_seq=2), ctx("x")).code == codes.BAD_REQUEST
        # purge published rows -> NOT_FOUND
        db.write(lambda c: ob.mark_published(c, [r.outbox_id for r in sent], 50))
        db.write(lambda c: ob.purge_published_before(c, 100))
        gone = h.replay("ctl.replay", ReplayRequest(topic="risk.event", from_seq=1), ctx("ctl.replay"))
        assert gone.code == codes.NOT_FOUND

        r = h.shutdown("ctl.shutdown", ShutdownRequest(reason="down", deadline_ms=500), ctx("ctl.shutdown"))
        assert r.ok and shutdowns[0].reason == "down"

        no_outbox = ControlHandlers(
            instance="api-gateway", clock=SimClock(0), state=lambda: ServiceState.RUNNING, status=lambda: hb,
            lv_cache=lambda: None, outbox=lambda: None, on_shutdown=lambda r: None,
        )
        assert no_outbox.replay("ctl.replay", ReplayRequest(topic="risk.event", from_seq=1), ctx("x")).code == codes.NOT_FOUND
        assert no_outbox.snapshot("ctl.snapshot", SnapshotRequest(prefixes=["x"]), ctx("x")).code == codes.NOT_FOUND
    finally:
        db.close()
