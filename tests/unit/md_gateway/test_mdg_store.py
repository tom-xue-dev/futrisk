"""md.db access (03 §4.1, §7) and the daily maintenance helpers (03 §8)."""

from __future__ import annotations

from pathlib import Path

import msgspec
import pytest

from futrisk.common.paths import HomeLayout
from futrisk.db.database import Database
from futrisk.messages.enums import GapStatus, MdEventKind, SettleInputQuality, SettleMethod, SourceId
from futrisk.messages.md import GapInfo, MdEvent, SettleInput
from futrisk.services.clearing.maintenance import vacuum_into, wal_checkpoint
from futrisk.services.md_gateway import store
from futrisk.services.md_gateway.events import GapRecord
from futrisk.services.md_gateway.schema import METADATA, MIGRATIONS
from futrisk.services.md_gateway.service import _purge_files

from .helpers import T0, TD


@pytest.fixture
def db(tmp_path: Path):  # type: ignore[no-untyped-def]
    d = Database(tmp_path / "md.db", METADATA, synchronous="NORMAL", migrations=MIGRATIONS)
    d.open()
    yield d
    d.close()


def ev(kind: MdEventKind, *, iid: str | None = None, gap: GapInfo | None = None, ts: int = T0, td: str = TD) -> MdEvent:
    return MdEvent(event_id=f"e-{kind}-{iid}-{ts}", kind=kind, ts_ns=ts, trading_day=td, instrument_id=iid, gap=gap, detail={"x": 1})


def test_events_gaps_and_down_state(db: Database) -> None:
    info = GapInfo(gap_id="feed_a:E1:10-20", source=SourceId.FEED_A, epoch="E1", from_seq=10, to_seq=20, from_ns=T0)
    rec = GapRecord(info, TD, T0)
    opened = ev(MdEventKind.GAP_OPEN, gap=info)
    closed_info = msgspec.structs.replace(info, status=GapStatus.FILLED, filled=11)
    closed = ev(MdEventKind.GAP_FILLED, gap=closed_info, ts=T0 + 1)

    def tx(c) -> None:  # type: ignore[no-untyped-def]
        store.upsert_gap(c, rec)
        store.insert_event(c, opened)
        assert [g.gap_id for g in store.load_open_gaps(c)] == ["feed_a:E1:10-20"]
        store.upsert_gap(c, GapRecord(closed_info, TD, T0, T0 + 1))
        store.insert_event(c, closed, actor=None)
        store.insert_event(c, ev(MdEventKind.MD_DOWN, iid="rb2701", ts=T0 + 2))
        store.insert_event(c, ev(MdEventKind.MD_DOWN, iid="IF2612", ts=T0 + 3))
        store.insert_event(c, ev(MdEventKind.MD_RECOVERED, iid="IF2612", ts=T0 + 4))

    db.write(tx)
    got = db.read(lambda c: store.query_events(c, TD, [MdEventKind.GAP_OPEN, MdEventKind.GAP_FILLED]))
    assert [e.kind for e in got] == [MdEventKind.GAP_OPEN, MdEventKind.GAP_FILLED]
    assert got[0].gap is not None and got[0].gap.status == GapStatus.OPEN  # snapshot at event time
    assert got[1].gap is not None and got[1].gap.filled == 11 and got[1].detail == {"x": 1}
    assert db.read(store.load_open_gaps) == []
    assert db.read(lambda c: store.load_down_state(c, TD)) == {"rb2701": True, "IF2612": False}
    assert db.read(lambda c: store.query_events(c, TD, None, "rb2701"))[0].kind == MdEventKind.MD_DOWN
    assert db.write(lambda c: store.purge_events_before(c, "20260929")) == 5
    assert db.read(lambda c: store.query_events(c, TD)) == []


def test_cursors_watermarks_and_settle_inputs(db: Database) -> None:
    cur = store.FeedCursor(SourceId.FEED_A, "E1", 1234, None, TD, T0)
    db.write(lambda c: store.save_feed_cursor(c, cur))
    db.write(lambda c: store.save_feed_cursor(c, store.FeedCursor(SourceId.FEED_A, "E1", 1300, None, TD, T0 + 1)))
    db.write(lambda c: store.save_feed_cursor(c, store.FeedCursor(SourceId.FEED_B, None, None, T0 + 5, TD, T0 + 1)))
    cursors = db.read(store.load_feed_cursors)
    assert cursors[SourceId.FEED_A].last_contiguous_seq == 1300 and cursors[SourceId.FEED_B].last_exch_ts_ns == T0 + 5
    db.write(lambda c: store.save_watermarks(c, [("rb2701", (TD, 100, T0), None)], T0))
    db.write(lambda c: store.save_watermarks(c, [("rb2701", (TD, 101, T0 + 5), 2**63 + 7)], T0 + 1))
    assert db.read(store.load_watermarks) == {"rb2701": ((TD, 101, T0 + 5), 2**63 + 7)}
    item = SettleInput(
        trading_day=TD, instrument_id="rb2701", exchange="SHFE", method=SettleMethod.VWAP_FULL_DAY, multiplier=10,  # type: ignore[arg-type]
        tick_size="1", day_volume=10, day_turnover="312000", vwap_raw="3120", pre_settle="3120",
        quality=SettleInputQuality.OK, version=1, computed_ns=T0, hourly={"1": ["2", "3"]},
    )
    db.write(lambda c: store.insert_settle_input(c, item))
    got = db.read(lambda c: store.latest_settle_input(c, TD, "rb2701"))
    assert got is not None and store.same_settle_values(got, msgspec.structs.replace(item, computed_ns=T0 + 9))
    assert not store.same_settle_values(got, msgspec.structs.replace(item, day_volume=11))
    db.write(lambda c: store.insert_settle_input(c, msgspec.structs.replace(item, day_volume=11, version=2)))
    latest = db.read(lambda c: store.latest_settle_input(c, TD, "rb2701"))
    assert latest is not None and latest.version == 2 and latest.day_volume == 11 and latest.hourly == {"1": ["2", "3"]}


def test_backup_and_retention_files(db: Database, tmp_path: Path) -> None:
    layout = HomeLayout(tmp_path / "home")
    layout.ensure()
    backup = layout.backup_dir / TD / "md.db"
    wal_checkpoint(db.path)
    vacuum_into(db.path, backup)
    assert backup.exists() and not backup.with_name("md.db.tmp").exists()
    for td in ("20260801", "20260920"):
        (layout.ticks_raw_dir / td / "feed_a").mkdir(parents=True)
    (layout.ticks_canonical_dir / "20250101.parquet").write_bytes(b"x")
    (layout.ticks_canonical_dir / "20260920.parquet").write_bytes(b"x")
    for n in range(25):
        d = layout.backup_dir / f"202606{n + 1:02d}"
        d.mkdir(parents=True, exist_ok=True)
        (d / "md.db").write_bytes(b"x")
    _purge_files(layout, TD)
    assert not (layout.ticks_raw_dir / "20260801").exists() and (layout.ticks_raw_dir / "20260920").exists()
    assert not (layout.ticks_canonical_dir / "20250101.parquet").exists()
    assert (layout.ticks_canonical_dir / "20260920.parquet").exists()
    kept = sorted(p.parent.name for p in layout.backup_dir.glob("*/md.db"))
    assert len(kept) == 20 and kept[-1] == TD and kept[0] == "20260607"


def test_manual_pin_is_restored_from_the_audit_events_and_survives_retention(db: Database) -> None:
    """M4: the last manual md.source.set per source (a SOURCE_STATE event with its actor) is the persisted pin."""

    def manual(source: SourceId, action: str, ts: int, td: str = TD) -> MdEvent:
        return MdEvent(
            event_id=f"m-{source}-{ts}", kind=MdEventKind.SOURCE_STATE, ts_ns=ts, trading_day=td, source=source,
            from_state="TRUSTED", to_state="EXCLUDED" if action == "EXCLUDE" else "PROBATION",
            detail={"manual": True, "action": action, "reason": "vendor issue", "actor": "ops"},
        )

    def tx(c) -> None:  # type: ignore[no-untyped-def]
        store.insert_event(c, manual(SourceId.FEED_B, "EXCLUDE", T0, td="20260801"), actor="ops")
        store.insert_event(c, manual(SourceId.FEED_A, "EXCLUDE", T0 + 1), actor="ops")
        store.insert_event(c, manual(SourceId.FEED_A, "INCLUDE", T0 + 2), actor="ops")
        store.insert_event(c, ev(MdEventKind.SOURCE_STATE, ts=T0 + 3))  # automatic transition: no actor

    db.write(tx)
    pins = db.read(store.load_manual_pins)
    assert list(pins) == [SourceId.FEED_B] and pins[SourceId.FEED_B]["actor"] == "ops"
    # the retention purge keeps manual actions (audit trail + pin), even old ones
    db.write(lambda c: store.purge_events_before(c, "20261231"))
    assert list(db.read(store.load_manual_pins)) == [SourceId.FEED_B]
