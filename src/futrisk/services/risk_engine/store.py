"""risk-{shard}.db access for the risk-engine shell (03 §4.3, §7, §8; ``core/ENGINE.md`` §6, §8).

* :class:`RowWriter` turns the EngineCore's :class:`~futrisk.core.effects.Row` statements into
  cached parameterised SQL (``INSERT`` / ``INSERT ... ON CONFLICT DO NOTHING`` / ``UPSERT`` /
  ``UPDATE`` / ``DELETE``) run straight on the writer's sqlite3 connection
  (:class:`~futrisk.db.database.PreparedSql`); JSON columns are encoded, booleans stored as 0/1,
  non-finite floats of monitoring columns sanitised. The Persists of one shell batch are applied
  with :meth:`RiskStore.apply_persist` inside ONE writer transaction, together with their outbox
  messages (``Outbox.append_msg``, ``outbox.created_ns`` = ``OutboxMsg.ts_ns``) and the
  ``fact_cursors`` rows of the FACT inputs.
* :meth:`RiskStore.load_restore` reads what :meth:`EngineCore.restore` needs (ENGINE.md §8): the
  latest SOD day and the rows of that day, non-terminal orders, open recon breaks, pending
  plans, the kill-switch history, idempotency records and the intraday peaks (max
  ``perf_equity`` per account from ``metric_samples``).
* History queries (``risk.query.metrics`` and ``orders`` / ``fills`` / ``alerts`` / ``cash`` /
  ``liquidation_plans`` of other trading days) read ``metric_samples``, ``orders``, ``fills``,
  ``risk_events``, ``cash_movements`` and ``liquidation_plans`` on a ``query_only`` connection.
* Retention (03 §8): ``processed_commands`` (today and the previous trading day kept),
  ``metric_samples`` (90 days), ``outbox`` (published and older than 7 days, wall clock).
"""

from __future__ import annotations

import json
import math
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal
from enum import Enum
from typing import Any

import msgspec
from loguru import logger
from sqlalchemy import Connection, Table, select, text

from futrisk.common.clock import parse_td, td_str
from futrisk.core.effects import OutboxMsg, Persist, Row, RowOp
from futrisk.core.eod import cash_from_row, trade_from_fill_row
from futrisk.core.orders import OrderRecord
from futrisk.db import PreparedSql, driver_connection
from futrisk.db.outbox import Outbox
from futrisk.messages.base import decode
from futrisk.messages.enums import AccountState
from futrisk.messages.eod import DayRoll
from futrisk.messages.orders import OrderUpdate, Trade
from futrisk.messages.risk import CashMovement, LiquidationPlan, MetricPoint, MetricSeries, RiskEvent
from futrisk.services.risk_engine import schema as S

#: JSON columns per table (03 §3.1 ``JSON``): encoded on write, decoded on read
JSON_COLUMNS: dict[str, frozenset[str]] = {
    "orders": frozenset({"reject_detail"}),
    "order_events": frozenset({"detail"}),
    "alerts": frozenset({"detail"}),
    "risk_events": frozenset({"detail"}),
    "retro_breaches": frozenset({"detail"}),
    "account_states": frozenset({"restrictions"}),
    "firm_controls": frozenset({"restrictions"}),
    "liquidation_plans": frozenset({"steps"}),
    "audit_events": frozenset({"before", "after"}),
    "metric_samples": frozenset({"rule_util"}),
    "eod_states": frozenset({"digest"}),
    "recon_breaks": frozenset({"detail"}),
}

METRIC_RETENTION_DAYS = 90
OUTBOX_RETENTION_DAYS = 7.0
PURGE_BATCH = 500  # rows per writer job: order commits interleave with a purge (review M3)

_json_encoder = msgspec.json.Encoder()


def _quote(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def encode_json(value: Any) -> str | None:
    """JSON text of a column value (Structs, enums and Decimals included); ``None`` stays NULL."""
    if value is None:
        return None
    if isinstance(value, str):
        return value if value else None
    return _json_encoder.encode(value).decode("utf-8")


def decode_json(value: Any) -> Any:
    """Decoded JSON column (text or already decoded)."""
    if isinstance(value, str):
        return json.loads(value) if value else None
    if isinstance(value, bytes | bytearray):
        return json.loads(bytes(value).decode("utf-8")) if value else None
    return value


@dataclass(frozen=True)
class _Stmt:
    run: PreparedSql  # straight on the sqlite3 connection of the writer (no SQLAlchemy per-statement work)
    columns: tuple[str, ...]
    converters: tuple[Callable[[Any], Any], ...]


class RowWriter:
    """Executes EngineCore rows as cached parameterised SQL on a writer connection."""

    def __init__(self, tables: Mapping[str, Table]) -> None:
        self._tables = dict(tables)
        self._cache: dict[tuple[str, RowOp, tuple[str, ...], tuple[str, ...]], _Stmt] = {}
        self.rows_written = 0

    # ---------------------------------------------------------------- coercion
    def _converter(self, table: str, column: str) -> Callable[[Any], Any]:
        t = self._tables.get(table)
        if t is None:
            raise KeyError(f"unknown table {table!r}")
        if column not in t.c:
            raise KeyError(f"unknown column {table}.{column}")
        col = t.c[column]
        if column in JSON_COLUMNS.get(table, frozenset()):
            return encode_json
        is_real = col.type.__class__.__name__ in ("Float", "REAL", "Double")
        nullable = bool(col.nullable)
        if is_real:

            def real(v: Any) -> Any:
                if v is None:
                    return None
                f = float(v)
                if math.isfinite(f):
                    return f
                logger.warning("risk.store.non_finite", table=table, column=column, value=str(v))
                return None if nullable else 0.0

            return real
        return _plain

    def _build(self, row: Row) -> _Stmt:
        cols = tuple(row.values.keys())
        table, op, key = row.table, row.op, tuple(row.key)
        qt = _quote(table)
        if op in (RowOp.INSERT, RowOp.INSERT_IGNORE, RowOp.UPSERT):
            names = ", ".join(_quote(c) for c in cols)
            marks = ", ".join("?" for _ in cols)
            sql = f"INSERT INTO {qt} ({names}) VALUES ({marks})"
            if op == RowOp.INSERT_IGNORE:
                sql += f" ON CONFLICT ({', '.join(_quote(k) for k in key)}) DO NOTHING"
            elif op == RowOp.UPSERT:
                changed = [c for c in cols if c not in key]
                target = ", ".join(_quote(k) for k in key)
                if changed:
                    sets = ", ".join(f"{_quote(c)} = excluded.{_quote(c)}" for c in changed)
                    sql += f" ON CONFLICT ({target}) DO UPDATE SET {sets}"
                else:
                    sql += f" ON CONFLICT ({target}) DO NOTHING"
            order = cols
        elif op == RowOp.UPDATE:
            upd = tuple(c for c in cols if c not in key)
            if not upd:
                raise ValueError(f"UPDATE of {table} without non-key columns")
            sets = ", ".join(f"{_quote(c)} = ?" for c in upd)
            where = " AND ".join(f"{_quote(k)} = ?" for k in key)
            sql = f"UPDATE {qt} SET {sets} WHERE {where}"
            order = upd + key
        elif op == RowOp.DELETE:
            if not key:
                raise ValueError(f"DELETE from {table} without key")
            where = " AND ".join(f"{_quote(k)} = ?" for k in key)
            sql = f"DELETE FROM {qt} WHERE {where}"
            order = key
        else:  # pragma: no cover - RowOp is closed
            raise ValueError(f"unknown row op {op!r}")
        return _Stmt(PreparedSql(sql), order, tuple(self._converter(table, c) for c in order))

    def execute(self, conn: Connection, row: Row) -> None:
        cache_key = (row.table, row.op, tuple(row.key), tuple(row.values.keys()))
        stmt = self._cache.get(cache_key)
        if stmt is None:
            stmt = self._build(row)
            self._cache[cache_key] = stmt
        vals = row.values
        params = tuple(conv(vals[c]) for c, conv in zip(stmt.columns, stmt.converters, strict=True))
        stmt.run(conn, params)
        self.rows_written += 1


def _plain(v: Any) -> Any:
    if isinstance(v, bool):
        return int(v)
    if isinstance(v, Enum):
        return v.value
    if isinstance(v, Decimal):
        return str(v)
    if isinstance(v, dict | list | tuple):
        return encode_json(v)
    return v


def _rows(conn: Connection, stmt: Any, table: str) -> list[dict[str, Any]]:
    """Mappings of a SELECT with JSON columns decoded."""
    jcols = JSON_COLUMNS.get(table, frozenset())
    out: list[dict[str, Any]] = []
    for m in conn.execute(stmt).mappings():
        d = {str(k): v for k, v in m.items()}  # plain str keys (msgspec.convert rejects str subclasses)
        for c in jcols:
            if c in d:
                d[c] = decode_json(d[c])
        out.append(d)
    return out


# ================================================================== restore SQL
def _fetch(conn: Connection, sql: str, params: Sequence[Any] = ()) -> list[dict[str, Any]]:
    """Rows of a SELECT straight from the sqlite3 connection, as plain dicts (column name -> value)."""
    cur = driver_connection(conn).execute(sql, params)
    names = [d[0] for d in cur.description]
    return [dict(zip(names, r, strict=False)) for r in cur.fetchall()]


_TERMINAL = ", ".join(f"'{s}'" for s in S.TERMINAL_ORDER_STATUSES)
_SQL_LATEST_SOD = "SELECT MAX(trading_day) FROM sod_accounts"
_SQL_SOD_PROVISIONAL = "SELECT MAX(provisional) FROM sod_accounts WHERE trading_day = ?"
_SQL_PROVISIONAL_SOURCE = "SELECT MAX(trading_day) FROM eod_states WHERE next_trading_day = ? AND trading_day < ?"
_SQL_PEAKS = (  # the intraday peak only moves on trusted marks (RK-09)
    "SELECT account_id, MAX(perf_equity) FROM metric_samples WHERE trading_day = ? AND quality = 'OK' "
    "GROUP BY account_id"
)
_SQL_DAYROLLS = "SELECT payload, type FROM outbox WHERE topic = 'sys.dayroll' ORDER BY topic_seq DESC LIMIT ?"
#: EngineCore.restore inputs (ENGINE.md §8): table -> (SQL, parameters: "td" = the latest SOD day, "first" = the
#: provisional source day or td)
_RESTORE_SQL: dict[str, tuple[str, tuple[str, ...]]] = {
    "sod_accounts": ("SELECT * FROM sod_accounts WHERE trading_day = ?", ("td",)),
    "sod_lots": ("SELECT * FROM sod_lots WHERE trading_day = ? ORDER BY account_id, instrument_id, direction, lot_no",
                 ("td",)),
    "sod_prices": ("SELECT * FROM sod_prices WHERE trading_day = ?", ("td",)),
    "fills": ("SELECT * FROM fills WHERE trading_day >= ? ORDER BY fill_seq", ("first",)),
    "cash_movements": ("SELECT * FROM cash_movements WHERE trading_day >= ? ORDER BY ts_ns, movement_id", ("first",)),
    "orders": (f"SELECT * FROM orders WHERE trading_day = ? OR status NOT IN ({_TERMINAL}) "
               "ORDER BY created_ns, client_order_id", ("td",)),
    # compliance cancel counters of the day (ENGINE.md §8): only the events that ENTER CANCEL_PENDING are read by the
    # core (every other event of the day - ~4 per order - would be fetched for nothing)
    "order_events": ("SELECT e.event_id, e.client_order_id, e.ts_ns, e.from_status, e.to_status FROM order_events e "
                     "JOIN orders o ON o.client_order_id = e.client_order_id WHERE o.trading_day = ? "
                     "AND e.to_status = 'CANCEL_PENDING' AND (e.from_status IS NULL OR e.from_status <> 'CANCEL_PENDING') "
                     "ORDER BY e.event_id", ("td",)),
    "rule_states": ("SELECT * FROM rule_states WHERE trading_day = ?", ("td",)),
    "alerts": ("SELECT * FROM alerts WHERE trading_day = ? ORDER BY raised_ns", ("td",)),
    "account_states": ("SELECT * FROM account_states", ()),
    "firm_controls": ("SELECT * FROM firm_controls ORDER BY control_id", ()),
    "liquidation_plans": ("SELECT * FROM liquidation_plans WHERE trading_day = ? OR status = 'PENDING_CONFIRM'",
                          ("td",)),
    "recon_breaks": ("SELECT * FROM recon_breaks WHERE trading_day = ? OR status = 'OPEN' ORDER BY created_ns",
                     ("td",)),
    "eod_states": ("SELECT * FROM eod_states ORDER BY trading_day DESC LIMIT 30", ()),
    "risk_events": ("SELECT * FROM risk_events WHERE trading_day = ? ORDER BY ts_ns", ("td",)),
    "processed_commands": ("SELECT * FROM processed_commands", ()),
}


# ================================================================== the store
CursorSaver = Callable[[Connection], None]


class RiskStore:
    """risk-{shard}.db reads and writes used by the shell (writes run in writer jobs)."""

    def __init__(self, outbox: Outbox | None = None, *, epoch: str = "", instance: str = "") -> None:
        self.writer = RowWriter(S.METADATA.tables)
        self.outbox = outbox
        self.epoch = epoch
        self.instance = instance
        self.facts_appended = 0

    # ---------------------------------------------------------------- writes
    def apply_persist(
        self,
        conn: Connection,
        persist: Persist | None,
        *,
        now_ns: int,
        cursor: CursorSaver | None = None,
        extra: Callable[[Connection], None] | None = None,
    ) -> None:
        """Rows, outbox messages and the FACT cursor of one engine call in the caller's transaction."""
        if persist is not None:
            for row in persist.rows:
                self.writer.execute(conn, row)
            for msg in persist.outbox:
                self.append_fact(conn, msg, now_ns=now_ns)
        if extra is not None:
            extra(conn)
        if cursor is not None:
            cursor(conn)

    def save_cursor(self, conn: Connection, cursor: Any, *, now_ns: int) -> None:
        """``fact_cursors`` upsert of a :class:`~futrisk.bus.fact.CursorUpdate` (same row as ``FactCursors.upsert``)."""
        self.writer.execute(conn, Row("fact_cursors", RowOp.UPSERT, ("src", "topic"), {
            "src": cursor.src,
            "topic": cursor.topic,
            "stream": cursor.stream,
            "last_seq": int(cursor.last_seq),
            "start_policy": str(cursor.start_policy),
            "updated_ns": now_ns,
        }))

    def append_fact(self, conn: Connection, msg: OutboxMsg, *, now_ns: int) -> int:
        """One FACT into the outbox (``created_ns`` = the engine time of the event, ENGINE.md §3)."""
        ob = self.outbox
        if ob is None:
            raise RuntimeError("risk store has no outbox (FACT messages cannot be written)")
        self.facts_appended += 1
        return ob.append_msg(conn, msg.topic, msg.payload, epoch=self.epoch, created_ns=msg.ts_ns or now_ns, td=msg.td,
                             corr_id=msg.corr_id)

    # --------------------------------------------------------------- restore
    # The restore reads tens of thousands of rows (a busy day: 10k fills, 10k orders, 40k order events): they are
    # fetched straight from the sqlite3 connection (``fetchall`` of plain tuples, no SQLAlchemy result objects) and
    # handed over as plain dicts; JSON columns stay text (EngineCore.restore decodes them). Column types map 1:1
    # (INTEGER / TEXT / REAL / BLOB), so the values are the ones SQLAlchemy would return.
    @staticmethod
    def latest_sod_day(conn: Connection) -> str | None:
        v = driver_connection(conn).execute(_SQL_LATEST_SOD).fetchone()[0]
        return str(v) if v is not None else None

    @staticmethod
    def provisional_source_day(conn: Connection, td: str) -> str | None:
        """Trading day whose provisional SOD is ``td`` (``None`` unless ``td`` runs on a provisional SOD)."""
        raw = driver_connection(conn)
        if not raw.execute(_SQL_SOD_PROVISIONAL, (td,)).fetchone()[0]:
            return None
        v = raw.execute(_SQL_PROVISIONAL_SOURCE, (td, td)).fetchone()[0]
        return str(v) if v is not None else None

    def load_restore(self, conn: Connection) -> tuple[dict[str, list[dict[str, Any]]], dict[str, float]] | None:
        """Tables for :meth:`EngineCore.restore` and the intraday peaks, or ``None`` without SOD rows."""
        td = self.latest_sod_day(conn)
        if td is None:
            return None
        # on a provisional SOD the day it was built from (eod_states.next_trading_day == td) is still open for
        # eod.freeze digests and cash attribution: its fills and cash come along (02 §8.4)
        first_day = self.provisional_source_day(conn, td) or td
        tables = {name: _fetch(conn, sql, tuple(td if a == "td" else first_day for a in args))
                  for name, (sql, args) in _RESTORE_SQL.items()}
        peaks = {str(a): float(v) for a, v in driver_connection(conn).execute(_SQL_PEAKS, (td,)).fetchall()
                 if v is not None}
        return tables, peaks

    def dayrolls(self, conn: Connection, limit: int = 50) -> list[DayRoll]:
        """The newest ``sys.dayroll`` facts still in the outbox, oldest first (replies of re-pushed rollovers)."""
        rows = driver_connection(conn).execute(_SQL_DAYROLLS, (limit,)).fetchall()
        return [decode(bytes(payload), DayRoll) for payload, typ in reversed(rows) if typ == "DayRoll"]

    def last_dayroll(self, conn: Connection) -> DayRoll | None:
        """The newest ``sys.dayroll`` fact still in the outbox."""
        rolls = self.dayrolls(conn, limit=1)
        return rolls[-1] if rolls else None

    # --------------------------------------------------------------- history
    @staticmethod
    def query_orders(conn: Connection, account_id: str, trading_day: str, statuses: Sequence[str] | None) -> list[OrderUpdate]:
        o = S.orders
        stmt = select(o).where(o.c.account_id == account_id, o.c.trading_day == trading_day)
        if statuses:
            stmt = stmt.where(o.c.status.in_([str(s) for s in statuses]))
        rows = _rows(conn, stmt.order_by(o.c.created_ns, o.c.client_order_id), "orders")
        return [OrderRecord.from_row(r).to_update() for r in rows]

    @staticmethod
    def query_fills(conn: Connection, account_id: str, trading_day: str) -> list[Trade]:
        f = S.fills
        rows = _rows(
            conn,
            select(f).where(f.c.account_id == account_id, f.c.trading_day == trading_day).order_by(f.c.fill_seq),
            "fills",
        )
        return [fill_to_trade(r) for r in rows]

    @staticmethod
    def query_events(conn: Connection, trading_day: str, account_id: str | None) -> list[RiskEvent]:
        from futrisk.core.alerts import event_from_row

        e = S.risk_events
        stmt = select(e).where(e.c.trading_day == trading_day)
        if account_id is not None:
            stmt = stmt.where(e.c.account_id == account_id)
        return [event_from_row(r) for r in _rows(conn, stmt.order_by(e.c.ts_ns, e.c.event_id), "risk_events")]

    @staticmethod
    def query_cash(conn: Connection, trading_day: str, account_id: str | None) -> list[CashMovement]:
        c = S.cash_movements
        stmt = select(c).where(c.c.trading_day == trading_day)
        if account_id is not None:
            stmt = stmt.where(c.c.account_id == account_id)
        return [cash_from_row(r) for r in _rows(conn, stmt.order_by(c.c.ts_ns, c.c.movement_id), "cash_movements")]

    @staticmethod
    def query_liquidation_plans(conn: Connection, trading_day: str, account_id: str | None,
                                status: str | None) -> list[LiquidationPlan]:
        """``liquidation_plans WHERE trading_day=? [AND account_id=?] [AND status=?] ORDER BY created_ns`` (ENGINE.md §5)."""
        lp = S.liquidation_plans
        stmt = select(lp).where(lp.c.trading_day == trading_day)
        if account_id is not None:
            stmt = stmt.where(lp.c.account_id == account_id)
        if status is not None:
            stmt = stmt.where(lp.c.status == status)
        out: list[LiquidationPlan] = []
        for r in _rows(conn, stmt.order_by(lp.c.created_ns, lp.c.plan_id), "liquidation_plans"):
            out.append(msgspec.convert({
                "plan_id": r["plan_id"], "account_id": r["account_id"], "trading_day": r["trading_day"],
                "trigger_alert_key": r["trigger_alert_key"], "status": r["status"], "steps": r.get("steps") or [],
                "created_ns": r["created_ns"], "confirmed_by": r.get("confirmed_by"),
                "confirmed_ns": r.get("confirmed_ns"),
            }, LiquidationPlan))
        return out

    @staticmethod
    def query_metrics(conn: Connection, account_id: str, trading_day: str, step_s: int) -> MetricSeries:
        """``metric_samples`` downsampled to ``step_s`` (the last sample of every step bucket)."""
        ms = S.metric_samples
        rows = _rows(
            conn,
            select(
                ms.c.ts_ns,
                ms.c.equity,
                ms.c.nav,
                ms.c.daily_pnl_pct_init,
                ms.c.intraday_dd,
                ms.c.risk_degree,
                ms.c.gross_notional,
                ms.c.state,
                ms.c.rule_util,
            )
            .where(ms.c.account_id == account_id, ms.c.trading_day == trading_day)
            .order_by(ms.c.ts_ns),
            "metric_samples",
        )
        step = max(1, int(step_s)) * 1_000_000_000
        points: list[MetricPoint] = []
        last_bucket: int | None = None
        for r in rows:
            bucket = int(r["ts_ns"]) // step
            util = r.get("rule_util") or {}
            p = MetricPoint(
                ts_ns=int(r["ts_ns"]),
                equity=float(r["equity"]),
                nav=float(r["nav"]),
                daily_pnl_pct_init=float(r["daily_pnl_pct_init"]),
                intraday_dd=float(r["intraday_dd"]),
                risk_degree=float(r["risk_degree"]),
                gross_notional=float(r["gross_notional"]),
                state=AccountState(str(r["state"])),
                rule_util={str(k): float(v) for k, v in dict(util).items()},
            )
            if bucket == last_bucket:
                points[-1] = p
            else:
                points.append(p)
                last_bucket = bucket
        return MetricSeries(account_id=account_id, trading_day=trading_day, step_s=max(1, int(step_s)), points=points)

    # ------------------------------------------------------------- retention
    @staticmethod
    def purge_metric_samples(conn: Connection, before_td: str, *, limit: int = PURGE_BATCH) -> int:
        """Delete one batch of ``metric_samples`` older than ``before_td`` (by rowid, 03 §8)."""
        res = conn.execute(
            text(
                "DELETE FROM metric_samples WHERE rowid IN "
                "(SELECT rowid FROM metric_samples WHERE trading_day < :td LIMIT :n)"
            ),
            {"td": before_td, "n": int(limit)},
        )
        return int(res.rowcount or 0)


def metric_cutoff(trading_day: str, days: int = METRIC_RETENTION_DAYS) -> str:
    """``trading_day`` minus ``days`` calendar days (``metric_samples`` retention, 03 §8)."""
    return td_str(parse_td(trading_day) - timedelta(days=days))


def fill_to_trade(r: Mapping[str, Any]) -> Trade:
    """``Trade`` of a ``fills`` row (``risk.query.fills`` is generated from this table, 03 §4.3)."""
    return trade_from_fill_row(r)


def table_rows(conn: Connection, table: str, where: Iterable[Any] = ()) -> list[dict[str, Any]]:
    """All rows of a business table (JSON decoded); tests and tools."""
    t = S.METADATA.tables[table]
    stmt = select(t)
    for w in where:
        stmt = stmt.where(w)
    return _rows(conn, stmt, table)


__all__ = [
    "JSON_COLUMNS",
    "METRIC_RETENTION_DAYS",
    "OUTBOX_RETENTION_DAYS",
    "CursorSaver",
    "RiskStore",
    "RowWriter",
    "decode_json",
    "encode_json",
    "fill_to_trade",
    "metric_cutoff",
    "table_rows",
]
