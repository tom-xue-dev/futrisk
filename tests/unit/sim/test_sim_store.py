"""sim.db schema and migration (03 §4.7)."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from futrisk.sim.exchange.store import SCHEMA_VERSION, SimStore, StoreBatch

V1_ORDERS = """
CREATE TABLE sim_orders (
    order_sys_id TEXT PRIMARY KEY, trading_day TEXT NOT NULL, front_id INTEGER NOT NULL,
    session_id INTEGER NOT NULL, order_ref TEXT NOT NULL, client_order_id TEXT, investor_id TEXT NOT NULL,
    instrument_id TEXT NOT NULL, exchange_id TEXT NOT NULL, direction TEXT NOT NULL,
    comb_offset_flag TEXT NOT NULL, order_price_type TEXT NOT NULL, limit_price FLOAT NOT NULL,
    volume_total_original INTEGER NOT NULL, volume_traded INTEGER NOT NULL, order_status TEXT NOT NULL,
    status_msg TEXT DEFAULT '' NOT NULL, time_condition TEXT NOT NULL, volume_condition TEXT NOT NULL,
    origin TEXT DEFAULT 'INTERNAL' NOT NULL, last_broker_seq INTEGER NOT NULL, insert_ns BIGINT NOT NULL,
    updated_ns BIGINT NOT NULL
)
"""


class TestSimStore:
    async def test_version_1_database_gains_pending_match(self, tmp_path: Path) -> None:
        path = tmp_path / "sim.db"
        con = sqlite3.connect(path)
        con.execute(V1_ORDERS)
        con.execute(
            "INSERT INTO sim_orders VALUES ('0000000001', '20260928', 1, 1, '1', 'c1', 'ACC3', 'rb2701', 'SHFE',"
            " '0', '0', 'LIMIT', 3120.0, 1, 0, 'ACCEPTED', 'ACCEPTED', 'GFD', 'AV', 'INTERNAL', 1, 1, 1)"
        )
        con.execute("PRAGMA user_version=1")
        con.commit()
        con.close()
        store = SimStore(path)
        await store.open()
        try:
            rows = await store.call(store.load_orders, "20260928")
            assert rows[0]["order_sys_id"] == "0000000001" and rows[0]["pending_match"] == 0
            row = dict(rows[0], pending_match=1)
            await store.call(store.apply, StoreBatch(orders=[row]))
            assert (await store.call(store.load_orders, "20260928"))[0]["pending_match"] == 1
        finally:
            await store.close()
        con = sqlite3.connect(path)
        try:
            assert con.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
        finally:
            con.close()
