"""td.db schema and migrations (03 §9): frozen checksums, the v2 -> v3 upgrade (``firm_controls.action`` accepts
``RULE_STATE``) and every current enum value storable in a freshly migrated database."""

from __future__ import annotations

import sqlite3
from enum import StrEnum
from pathlib import Path

import pytest
from tgtest_support import T0, TD

from futrisk.common.clock import SimClock
from futrisk.db import Database
from futrisk.messages.enums import (
    BrokerOrderStatus,
    ControlOrigin,
    Direction,
    Exchange,
    FirmAction,
    Offset,
    OrderType,
    TimeInForce,
    TradeOrigin,
)
from futrisk.services.trade_gateway import store
from futrisk.services.trade_gateway.schema import METADATA, MIGRATIONS, SCHEMA_VERSION

#: released migrations must never change: databases record these checksums
FROZEN_CHECKSUMS = {
    1: "d9b563923bc0b964ad588f61e8566691f8eb23f7d156d18a6abdda575c861d6a",
    2: "866026017a3d4ff28f2921ce50bb8440c0dd9b127d8de432c8f53733e56e3a54",
    3: "d847f70f741e9225de617da0d6be070c86ee7bce0f32b12c6abc38fab77667e0",
}


def open_td(path: Path, migrations: list | None = None) -> Database:  # type: ignore[type-arg]
    db = Database(path, METADATA, name="td", synchronous="FULL", migrations=migrations or MIGRATIONS, clock=SimClock(T0))
    db.open()
    return db


def insert_control(con: sqlite3.Connection, action: str, origin: str = "RULE", active: int = 1) -> int:
    cur = con.execute(
        "INSERT INTO firm_controls (action, origin, active, restrictions, trading_day, reason, src_instance, ts_ns) "
        "VALUES (?, ?, ?, '[]', ?, 'test', 'risk-engine-0', ?)", (action, origin, active, TD, T0))
    return int(cur.lastrowid or 0)


def test_released_migration_checksums_are_frozen() -> None:
    assert SCHEMA_VERSION == MIGRATIONS[-1].version == 3
    assert {m.version: m.checksum for m in MIGRATIONS} == FROZEN_CHECKSUMS


def test_a_version_2_database_is_rebuilt_to_accept_rule_state(tmp_path: Path) -> None:
    path = tmp_path / "td.db"
    open_td(path, MIGRATIONS[:2]).close()  # a td.db written by the previous build
    con = sqlite3.connect(path)
    try:
        ids = [insert_control(con, "KILL_SWITCH_ON"), insert_control(con, "SET_RESTRICTIONS", active=0),
               insert_control(con, "KILL_SWITCH_OFF", origin="MANUAL", active=0)]
        con.commit()
        with pytest.raises(sqlite3.IntegrityError):
            insert_control(con, "RULE_STATE")  # the version 2 CHECK refuses it
        con.rollback()
    finally:
        con.close()

    db = open_td(path)
    try:
        assert db.read(lambda c: [tuple(r) for r in c.exec_driver_sql(
            "SELECT version, checksum FROM schema_version ORDER BY version").fetchall()]) == sorted(FROZEN_CHECKSUMS.items())
        assert db.read(store.load_kill_switch) == {ControlOrigin.MANUAL: False, ControlOrigin.RULE: True}
    finally:
        db.close()
    con = sqlite3.connect(path)
    try:
        assert [r[0] for r in con.execute("SELECT control_id FROM firm_controls ORDER BY control_id")] == ids
        new_id = insert_control(con, "RULE_STATE", active=0)
        con.commit()
        assert new_id == ids[-1] + 1  # the AUTOINCREMENT mark survived the rebuild
        assert con.execute("PRAGMA integrity_check").fetchone() == ("ok",)
        assert con.execute("SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='firm_controls'").fetchall() == [
            ("ix_firm_controls__origin_action",)]
        assert con.execute("SELECT count(*) FROM sqlite_master WHERE name='firm_controls__v3'").fetchone() == (0,)
    finally:
        con.close()
    db = open_td(path)  # reopening applies nothing and the checksums still match
    try:
        assert db.read(store.load_kill_switch) == {ControlOrigin.MANUAL: False, ControlOrigin.RULE: False}
    finally:
        db.close()


_BASE_ROWS: dict[str, dict[str, object]] = {
    "routed_orders": dict(account_id="ACC3", instrument_id="rb2701", exchange="SHFE", direction="BUY", offset="OPEN",
                          order_type="LIMIT", tif="GFD", price="3120", qty=1, trading_day=TD, src_instance="risk-engine-1",
                          expire_ns=T0, broker="sim", status="RECEIVED", filled_qty=0, received_ns=T0, updated_ns=T0),
    "broker_order_events": dict(trading_day=TD, broker_order_ref="1:1:1", origin="INTERNAL", status="ACCEPTED",
                                filled_qty=0, remaining_qty=1, ts_ns=T0),
    "trades": dict(trading_day=TD, exchange="SHFE", direction="BUY", account_id="ACC3", instrument_id="rb2701",
                   origin="INTERNAL", broker_order_ref="1:1:1", offset="OPEN", price="3120", qty=1, trade_ts_ns=T0,
                   received_ns=T0),
    "firm_controls": dict(action="KILL_SWITCH_ON", origin="RULE", active=0, restrictions="[]", trading_day=TD,
                          reason="test", src_instance="risk-engine-0", ts_ns=T0),
}
_UNIQUE_KEY = {"routed_orders": "client_order_id", "broker_order_events": "broker_seq", "trades": "trade_id"}
_ENUM_COLUMNS: list[tuple[str, str, type[StrEnum]]] = [
    ("routed_orders", "exchange", Exchange), ("routed_orders", "direction", Direction),
    ("routed_orders", "offset", Offset), ("routed_orders", "order_type", OrderType),
    ("routed_orders", "tif", TimeInForce), ("routed_orders", "status", BrokerOrderStatus),
    ("broker_order_events", "origin", TradeOrigin), ("broker_order_events", "status", BrokerOrderStatus),
    ("trades", "exchange", Exchange), ("trades", "direction", Direction), ("trades", "origin", TradeOrigin),
    ("trades", "offset", Offset), ("firm_controls", "action", FirmAction), ("firm_controls", "origin", ControlOrigin),
]


def test_every_current_enum_value_can_be_stored(tmp_path: Path) -> None:
    """A value added to a shared enum that td.db cannot store fails here, not in production (needs a migration)."""
    path = tmp_path / "td.db"
    open_td(path).close()
    con = sqlite3.connect(path)
    refused: list[str] = []
    try:
        n = 0
        for table, column, enum in _ENUM_COLUMNS:
            for value in enum:
                n += 1
                row = {**_BASE_ROWS[table], column: value.value}
                if table in _UNIQUE_KEY:
                    row[_UNIQUE_KEY[table]] = n if table == "broker_order_events" else f"k{n}"
                cols = ", ".join(row)
                try:
                    con.execute(f"INSERT INTO {table} ({cols}) VALUES ({', '.join('?' * len(row))})", tuple(row.values()))
                except sqlite3.IntegrityError as exc:
                    refused.append(f"{table}.{column}={value.value}: {exc}")
        con.rollback()
    finally:
        con.close()
    assert refused == []
