"""td.db maintenance after ``sys.eod`` DONE (03 §8): checkpoint, ``VACUUM INTO`` backup, retention purges,
backup pruning (the trigger itself is covered by the integration test)."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from sqlalchemy import Connection
from tgtest_support import SEC, T0, TD, Harness, logged_in, make_harness, order_report, route, trade_report

from futrisk.common.clock import parse_td, td_str
from futrisk.common.config import FullConfig
from futrisk.db import Outbox, ProcessedCommands
from futrisk.messages.enums import BrokerOrderStatus, TradeOrigin
from futrisk.messages.envelope import ok_reply
from futrisk.services.trade_gateway import maintenance, store
from futrisk.services.trade_gateway.maintenance import run_maintenance

RE = "risk-engine-1"
DAY = 86_400 * SEC
WALL = 1_790_000_000 * SEC  # the maintenance's wall clock: outbox ``published_ns`` is wall time


def add_events(h: Harness, trading_day: str, seqs: range) -> None:
    def tx(conn: Connection) -> None:
        for seq in seqs:
            store.insert_order_event(conn, trading_day=trading_day, broker_seq=seq, client_order_id=None,
                                     broker_order_ref=f"1:0:X{seq}", origin=TradeOrigin.EXTERNAL,
                                     status=BrokerOrderStatus.CANCELED, filled_qty=0, remaining_qty=1, broker_code=None,
                                     broker_msg=None, detail={}, ts_ns=T0)

    h.db.write(tx)


def add_commands(h: Harness, items: list[tuple[str, str]]) -> None:
    processed = ProcessedCommands(h.db)

    def tx(conn: Connection) -> None:
        for command_id, td in items:
            processed.record(conn, command_id=command_id, command="route.cancel", requester=RE, request_hash="h",
                             reply=ok_reply(), trading_day=td, received_ns=T0)

    h.db.write(tx)


def previous_days(config: FullConfig, n: int) -> list[str]:
    days, d = [], parse_td(TD)
    for _ in range(n):
        d = config.calendar.prev_trading_day(d)
        days.append(td_str(d))
    return sorted(days)


@pytest.fixture
def h(tmp_path: Path, default_config: FullConfig) -> Harness:
    harness = make_harness(tmp_path / "db" / "td.db", default_config)
    logged_in(harness)
    harness.core.route_order(route("c1"), RE)
    harness.core._apply_reports([order_report(1, "c1", BrokerOrderStatus.ACCEPTED), trade_report(2, "c1", "T1")])
    yield harness  # type: ignore[misc]
    harness.db.close()


def test_backup_purges_and_pruning(h: Harness, tmp_path: Path, default_config: FullConfig) -> None:
    add_events(h, "20260601", range(1, 4))  # older than 90 days before TD: purged
    add_events(h, "20260701", range(1, 2))  # inside the 90-day window: kept
    older, prev = previous_days(default_config, 2)  # 20260925 is a holiday (Mid-Autumn Festival)
    add_commands(h, [("old", older), ("prev", prev), ("today", TD)])  # today and the previous trading day are kept
    outbox = Outbox(h.db, instance="trade-gateway")
    ids = [r[0] for r in h.rows("SELECT outbox_id FROM outbox ORDER BY outbox_id")]
    assert len(ids) == 2  # the ACCEPTED update and the trade
    h.db.write(lambda c: outbox.mark_published(c, [ids[0]], WALL - 8 * DAY))  # beyond the 7-day replay window
    h.db.write(lambda c: outbox.mark_published(c, [ids[1]], WALL - 1 * DAY))
    h.core._apply_reports([order_report(3, "c1", BrokerOrderStatus.FILLED, filled=1)])  # never published: kept
    backup = tmp_path / "backup"
    old_days = previous_days(default_config, 21)
    for day in old_days:
        (backup / day).mkdir(parents=True)
        (backup / day / "td.db").write_bytes(b"old backup")
    (backup / old_days[0] / "clearing.db").write_bytes(b"another service's backup")

    res = run_maintenance(h.db, outbox, trading_day=TD, calendar=default_config.calendar, backup_dir=backup,
                          wall_now_ns=WALL)

    assert res.errors == [] and res.checkpoint is not None
    assert res.backup == str(backup / TD / "td.db")
    con = sqlite3.connect(res.backup)
    try:
        assert con.execute("PRAGMA integrity_check").fetchone() == ("ok",)
        assert con.execute("SELECT client_order_id, status FROM routed_orders").fetchall() == [("c1", "FILLED")]
        assert con.execute("SELECT count(*) FROM trades").fetchone() == (1,)
    finally:
        con.close()
    assert not (backup / TD / "td.db.tmp").exists()
    assert res.purged_commands == 1
    assert sorted(r[0] for r in h.rows("SELECT command_id FROM processed_commands")) == ["prev", "today"]
    assert res.purged_outbox == 1 and [r[0] for r in h.rows("SELECT outbox_id FROM outbox")] == [ids[1], ids[1] + 1]
    assert h.rows("SELECT topic, last_seq FROM outbox_topics") == [("td.exec.ACC3", 3)]  # sequences never go back
    assert res.purged_order_events == 3
    assert h.rows("SELECT DISTINCT trading_day FROM broker_order_events ORDER BY 1") == [("20260701",), (TD,)]
    assert h.scalar("SELECT count(*) FROM trades") == 1 and h.scalar("SELECT count(*) FROM routed_orders") == 1
    assert res.pruned_backups == old_days[:2]  # 22 day folders, the newest 20 are kept
    assert (backup / old_days[0] / "clearing.db").exists() and not (backup / old_days[0] / "td.db").exists()
    assert not (backup / old_days[1]).exists()  # emptied, so removed
    assert (backup / old_days[2] / "td.db").exists()


def test_purges_run_in_batches(h: Harness, default_config: FullConfig, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(maintenance, "BATCH", 2)
    add_events(h, "20260601", range(1, 6))
    res = run_maintenance(h.db, None, trading_day=TD, calendar=default_config.calendar, backup_dir=None, wall_now_ns=WALL)
    assert res.errors == [] and res.backup is None and res.pruned_backups == []
    assert res.purged_order_events == 5 and h.rows("SELECT DISTINCT trading_day FROM broker_order_events") == [(TD,)]


def test_errors_are_collected_and_the_rest_still_runs(h: Harness, tmp_path: Path, default_config: FullConfig) -> None:
    add_commands(h, [("old", "20260901")])
    not_a_dir = tmp_path / "backup"
    not_a_dir.write_text("a file where the backup folder should be")
    res = run_maintenance(h.db, Outbox(h.db, instance="trade-gateway"), trading_day=TD,
                          calendar=default_config.calendar, backup_dir=not_a_dir, wall_now_ns=WALL)
    assert [e.split(":")[0] for e in res.errors] == ["backup", "prune"]
    assert res.backup is None and res.purged_commands == 1
