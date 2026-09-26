"""``<home>/db/sim.db``: persistent state of the virtual exchange (03 §4.7).

sim.db is not a futrisk service database; it only lets the simulated counter behave like
a real one across restarts: ``broker_seq`` and ``trade_id`` continue, positions survive
(02 §9.4). Tables (03 §4.7 lists the main columns; the extra columns are noted):

- ``sim_meta`` (single row): counters ``last_broker_seq`` (per trading day),
  ``last_trade_no``, plus ``last_order_no`` / ``last_session_id`` (so ``order_sys_id`` and
  ``session_id`` never repeat) and the market-side ``md_epoch`` / random-stream position;
- ``sim_orders`` / ``sim_trades`` / ``sim_positions``: the counter's orders, trades and
  today/yesterday positions (``ReqQryOrder`` / ``ReqQryTrade`` / ``ReqQryInvestorPosition``);
- ``sim_md_state`` (addition to 03 §4.7): per-instrument cumulative day state of the price
  engine, written *before* each round is published, so a process restart inside a trading
  day keeps volume/turnover monotonic for md-gateway's watermarks. Feed A still starts a
  new epoch with an empty ring buffer.

Access goes through one writer thread (03 DB-02: single writer, ``BEGIN IMMEDIATE``);
the asyncio side awaits :meth:`SimStore.call`.
"""

from __future__ import annotations

import asyncio
import functools
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, TypeVar

import sqlalchemy as sa
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

T = TypeVar("T")

SCHEMA_VERSION = 2  # 2: sim_orders.pending_match

metadata = sa.MetaData()

sim_meta = sa.Table(
    "sim_meta",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True, autoincrement=False),
    sa.Column("trading_day", sa.Text, nullable=False, server_default=""),
    sa.Column("last_broker_seq", sa.Integer, nullable=False, server_default="0"),
    sa.Column("last_trade_no", sa.Integer, nullable=False, server_default="0"),
    sa.Column("last_order_no", sa.Integer, nullable=False, server_default="0"),
    sa.Column("last_session_id", sa.Integer, nullable=False, server_default="0"),
    sa.Column("md_epoch", sa.Text, nullable=True),
    sa.Column("md_rng_td", sa.Text, nullable=True),
    sa.Column("md_step", sa.Integer, nullable=False, server_default="0"),
    sa.Column("updated_ns", sa.BigInteger, nullable=False, server_default="0"),
    sa.CheckConstraint("id = 1", name="ck_sim_meta__single_row"),
)

sim_orders = sa.Table(
    "sim_orders",
    metadata,
    sa.Column("order_sys_id", sa.Text, primary_key=True),
    sa.Column("trading_day", sa.Text, nullable=False),
    sa.Column("front_id", sa.Integer, nullable=False),
    sa.Column("session_id", sa.Integer, nullable=False),
    sa.Column("order_ref", sa.Text, nullable=False),
    sa.Column("client_order_id", sa.Text, nullable=True),
    sa.Column("investor_id", sa.Text, nullable=False),
    sa.Column("instrument_id", sa.Text, nullable=False),
    sa.Column("exchange_id", sa.Text, nullable=False),
    sa.Column("direction", sa.Text, nullable=False),
    sa.Column("comb_offset_flag", sa.Text, nullable=False),
    sa.Column("order_price_type", sa.Text, nullable=False),
    sa.Column("limit_price", sa.Float, nullable=False),
    sa.Column("volume_total_original", sa.Integer, nullable=False),
    sa.Column("volume_traded", sa.Integer, nullable=False),
    sa.Column("order_status", sa.Text, nullable=False),
    sa.Column("status_msg", sa.Text, nullable=False, server_default=""),
    sa.Column("time_condition", sa.Text, nullable=False),
    sa.Column("volume_condition", sa.Text, nullable=False),
    sa.Column("origin", sa.Text, nullable=False, server_default="INTERNAL"),
    # 1 while the order still waits for its first (aggressive) match attempt: after a
    # restart such an order must be matched at the opposite price, never rest
    sa.Column("pending_match", sa.Integer, nullable=False, server_default="0"),
    sa.Column("last_broker_seq", sa.Integer, nullable=False),
    sa.Column("insert_ns", sa.BigInteger, nullable=False),
    sa.Column("updated_ns", sa.BigInteger, nullable=False),
    sa.Index("ix_sim_orders__td_seq", "trading_day", "last_broker_seq"),
    sa.Index("ix_sim_orders__status", "order_status"),
    sa.Index("ix_sim_orders__client_order_id", "client_order_id"),
)

sim_trades = sa.Table(
    "sim_trades",
    metadata,
    sa.Column("trade_id", sa.Text, primary_key=True),
    sa.Column("trading_day", sa.Text, nullable=False),
    sa.Column("order_sys_id", sa.Text, nullable=False),
    sa.Column("broker_seq", sa.Integer, nullable=False),
    sa.Column("front_id", sa.Integer, nullable=False),
    sa.Column("session_id", sa.Integer, nullable=False),
    sa.Column("order_ref", sa.Text, nullable=False),
    sa.Column("client_order_id", sa.Text, nullable=True),
    sa.Column("investor_id", sa.Text, nullable=False),
    sa.Column("instrument_id", sa.Text, nullable=False),
    sa.Column("exchange_id", sa.Text, nullable=False),
    sa.Column("direction", sa.Text, nullable=False),
    sa.Column("offset_flag", sa.Text, nullable=False),
    sa.Column("price", sa.Float, nullable=False),
    sa.Column("volume", sa.Integer, nullable=False),
    sa.Column("trade_ts_ns", sa.BigInteger, nullable=False),
    sa.Index("ix_sim_trades__td_seq", "trading_day", "broker_seq"),
)

sim_positions = sa.Table(
    "sim_positions",
    metadata,
    sa.Column("investor_id", sa.Text, primary_key=True),
    sa.Column("instrument_id", sa.Text, primary_key=True),
    sa.Column("posi_direction", sa.Text, primary_key=True),
    sa.Column("today_position", sa.Integer, nullable=False),
    sa.Column("yd_position", sa.Integer, nullable=False),
    sa.Column("updated_ns", sa.BigInteger, nullable=False),
    sa.CheckConstraint("posi_direction IN ('LONG', 'SHORT')", name="ck_sim_positions__dir"),
)

sim_md_state = sa.Table(
    "sim_md_state",
    metadata,
    sa.Column("instrument_id", sa.Text, primary_key=True),
    sa.Column("trading_day", sa.Text, nullable=False),
    sa.Column("state", sa.JSON, nullable=False),
    sa.Column("updated_ns", sa.BigInteger, nullable=False),
)


@dataclass
class StoreBatch:
    """Rows written in one transaction (one broker event)."""

    orders: list[dict[str, Any]] = field(default_factory=list)
    trades: list[dict[str, Any]] = field(default_factory=list)
    positions: list[dict[str, Any]] = field(default_factory=list)
    deleted_positions: list[tuple[str, str, str]] = field(default_factory=list)
    meta: dict[str, Any] | None = None

    def empty(self) -> bool:
        return not (self.orders or self.trades or self.positions or self.deleted_positions or self.meta)


def _upsert(table: sa.Table) -> Any:
    stmt = sqlite_insert(table)
    pk = [c.name for c in table.primary_key.columns]
    return stmt.on_conflict_do_update(
        index_elements=pk,
        set_={c.name: stmt.excluded[c.name] for c in table.columns if c.name not in pk},
    )


class SimStore:
    """sim.db access through a single writer thread."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="sim-db")
        self._engine: sa.Engine | None = None
        self._conn: sa.Connection | None = None

    # ------------------------------------------------------------------ async API
    async def call(self, fn: Callable[..., T], *args: Any) -> T:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._executor, functools.partial(fn, *args))

    async def open(self) -> None:
        await self.call(self._open)

    async def close(self) -> None:
        try:
            await self.call(self._close)
        finally:
            self._executor.shutdown(wait=True)

    # ------------------------------------------------------------------ writer thread
    def _open(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        engine = sa.create_engine(f"sqlite:///{self.path.as_posix()}", poolclass=sa.pool.NullPool)

        @sa.event.listens_for(engine, "connect")
        def _on_connect(dbapi_conn: Any, _record: Any) -> None:
            dbapi_conn.isolation_level = None  # SQLAlchemy emits BEGIN IMMEDIATE itself
            cur = dbapi_conn.cursor()
            for pragma in (
                "PRAGMA journal_mode=WAL",
                "PRAGMA synchronous=NORMAL",
                "PRAGMA busy_timeout=5000",
                "PRAGMA foreign_keys=ON",
            ):
                cur.execute(pragma)
            cur.close()

        @sa.event.listens_for(engine, "begin")
        def _on_begin(conn: sa.Connection) -> None:
            conn.exec_driver_sql("BEGIN IMMEDIATE")

        self._engine = engine
        self._conn = engine.connect()
        with self._conn.begin():
            metadata.create_all(self._conn)
            self._migrate(self._conn)
            self._conn.exec_driver_sql(f"PRAGMA user_version={SCHEMA_VERSION}")
            if self._conn.execute(sa.select(sim_meta.c.id)).first() is None:
                self._conn.execute(sa.insert(sim_meta).values(id=1, trading_day=""))

    @staticmethod
    def _migrate(conn: sa.Connection) -> None:
        """Bring a sim.db created by an older version up to :data:`SCHEMA_VERSION`."""
        cols = {row[1] for row in conn.exec_driver_sql("PRAGMA table_info(sim_orders)")}
        if "pending_match" not in cols:  # version 1 -> 2
            conn.exec_driver_sql("ALTER TABLE sim_orders ADD COLUMN pending_match INTEGER NOT NULL DEFAULT 0")

    def _close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None

    @property
    def conn(self) -> sa.Connection:
        if self._conn is None:
            raise RuntimeError("sim.db is not open")
        return self._conn

    def _rows(self, stmt: Any) -> list[dict[str, Any]]:
        with self.conn.begin():
            return [dict(r._mapping) for r in self.conn.execute(stmt)]

    # reads -----------------------------------------------------------------
    def load_meta(self) -> dict[str, Any]:
        rows = self._rows(sa.select(sim_meta).where(sim_meta.c.id == 1))
        return rows[0]

    def load_orders(self, trading_day: str) -> list[dict[str, Any]]:
        """Orders of ``trading_day`` plus any still-active order of another day."""
        active = sim_orders.c.order_status.in_(("ACCEPTED", "PART_TRADED"))
        stmt = (
            sa.select(sim_orders)
            .where(sa.or_(sim_orders.c.trading_day == trading_day, active))
            .order_by(sim_orders.c.insert_ns, sim_orders.c.order_sys_id)
        )
        return self._rows(stmt)

    def load_trades(self, trading_day: str) -> list[dict[str, Any]]:
        stmt = (
            sa.select(sim_trades)
            .where(sim_trades.c.trading_day == trading_day)
            .order_by(sim_trades.c.broker_seq)
        )
        return self._rows(stmt)

    def load_positions(self) -> list[dict[str, Any]]:
        return self._rows(sa.select(sim_positions))

    def load_md_states(self) -> dict[str, dict[str, Any]]:
        return {r["instrument_id"]: r["state"] for r in self._rows(sa.select(sim_md_state))}

    def query_orders(self, trading_day: str, from_seq: int) -> list[dict[str, Any]]:
        stmt = (
            sa.select(sim_orders)
            .where(sim_orders.c.trading_day == trading_day, sim_orders.c.last_broker_seq >= from_seq)
            .order_by(sim_orders.c.last_broker_seq)
        )
        return self._rows(stmt)

    def query_trades(self, trading_day: str, from_seq: int) -> list[dict[str, Any]]:
        stmt = (
            sa.select(sim_trades)
            .where(sim_trades.c.trading_day == trading_day, sim_trades.c.broker_seq >= from_seq)
            .order_by(sim_trades.c.broker_seq)
        )
        return self._rows(stmt)

    # writes ----------------------------------------------------------------
    def apply(self, batch: StoreBatch) -> None:
        """Write one broker event atomically."""
        if batch.empty():
            return
        with self.conn.begin():
            if batch.orders:
                self.conn.execute(_upsert(sim_orders), batch.orders)
            if batch.trades:
                self.conn.execute(sa.insert(sim_trades), batch.trades)
            if batch.positions:
                self.conn.execute(_upsert(sim_positions), batch.positions)
            for inv, inst, pdir in batch.deleted_positions:
                self.conn.execute(
                    sa.delete(sim_positions).where(
                        sim_positions.c.investor_id == inv,
                        sim_positions.c.instrument_id == inst,
                        sim_positions.c.posi_direction == pdir,
                    )
                )
            if batch.meta:
                self.conn.execute(sa.update(sim_meta).where(sim_meta.c.id == 1).values(**batch.meta))

    def save_md(self, states: dict[str, dict[str, Any]], rng_td: str | None, rng_step: int, ts_ns: int) -> None:
        """Persist the engine state of the instruments that changed in a round."""
        rows: Iterable[dict[str, Any]] = [
            {"instrument_id": iid, "trading_day": st["trading_day"], "state": st, "updated_ns": ts_ns}
            for iid, st in states.items()
        ]
        with self.conn.begin():
            rows = list(rows)
            if rows:
                self.conn.execute(_upsert(sim_md_state), rows)
            self.conn.execute(
                sa.update(sim_meta).where(sim_meta.c.id == 1).values(md_rng_td=rng_td, md_step=rng_step)
            )

    def set_md_epoch(self, epoch: str, ts_ns: int) -> None:
        with self.conn.begin():
            self.conn.execute(
                sa.update(sim_meta).where(sim_meta.c.id == 1).values(md_epoch=epoch, updated_ns=ts_ns)
            )
