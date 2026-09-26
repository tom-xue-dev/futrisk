"""Typed reads and writes of clearing.db (03 §4.4, §7).

Every function takes a SQLAlchemy connection: writer jobs (``db.run_write``)
for writes, ``query_only`` connections (``db.run_read``) for reads. Rows are
converted from and to the 02 payloads whose field names they share (03 §3.2);
money and prices stay canonical decimal strings end to end (DB-06).
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

import msgspec
from sqlalchemy import Connection, and_, delete, func, insert, or_, select, update

from futrisk.common.decimals import D, dec_str_strip
from futrisk.messages.enums import (
    BacktestState,
    CheckStatus,
    EodPhase,
    ReconCheck,
    ReportType,
)
from futrisk.messages.eod import (
    AccountSettlement,
    BacktestStatus,
    CheckResult,
    CloseDetailView,
    MarginGroupView,
    PositionSettlement,
    ReportReady,
    SettlementPriceView,
    SettlementRef,
    SettlementResult,
    SodAccount,
    SodLot,
    SodSnapshot,
    TradeSettlementView,
)
from futrisk.messages.orders import Trade
from futrisk.messages.risk import CashMovement
from futrisk.services.clearing import schema as S

EFFECTIVE_PHASES: frozenset[EodPhase] = frozenset({EodPhase.SETTLED, EodPhase.ROLLED, EodPhase.REPORTED, EodPhase.DONE})
TERMINAL_PHASES: frozenset[EodPhase] = frozenset({EodPhase.DONE, EodPhase.FAILED})
PHASE_ORDER: tuple[EodPhase, ...] = (
    EodPhase.BEGIN,
    EodPhase.FROZEN,
    EodPhase.TRADES_RECONCILED,
    EodPhase.PRICES_FIXED,
    EodPhase.SETTLED,
    EodPhase.ROLLED,
    EodPhase.REPORTED,
    EodPhase.DONE,
)


def phase_index(phase: EodPhase) -> int:
    """Position of ``phase`` in the saga (FAILED sorts last)."""
    try:
        return PHASE_ORDER.index(phase)
    except ValueError:
        return len(PHASE_ORDER)


# ---------------------------------------------------------------------- helpers
def dumps(obj: Any) -> str:
    """JSON text for ``JSON`` columns (UTF-8, stable key order)."""
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, default=str)


def loads(text: Any) -> Any:
    if text is None:
        return None
    return json.loads(text)


def _plain(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    return value


def struct_row(obj: msgspec.Struct, *, json_fields: Sequence[str] = ()) -> dict[str, Any]:
    """All fields of a payload struct (defaults included, enums as values) as a row dict."""
    row: dict[str, Any] = {}
    for name in obj.__struct_fields__:
        value = getattr(obj, name)
        row[name] = dumps(value) if name in json_fields else _plain(value)
    return row


def price_text(value: float | str) -> str:
    """``DEC_PRICE`` text of a broker/md float or decimal string (``Decimal(str(x))``, 02 §4.3)."""
    return dec_str_strip(D(value))


# ------------------------------------------------------------------ trades copy
def trade_row(t: Trade) -> dict[str, Any]:
    return {
        "trading_day": t.trading_day,
        "exchange": t.exchange.value,
        "trade_id": t.trade_id,
        "direction": t.direction.value,
        "account_id": t.account_id,
        "instrument_id": t.instrument_id,
        "client_order_id": t.client_order_id,
        "origin": t.origin.value,
        "offset": t.offset.value,
        "price": price_text(t.price),
        "qty": t.qty,
        "trade_ts_ns": t.trade_ts_ns,
        "trade_seq": t.trade_seq,
        "received_ns": t.received_ns,
    }


def copy_trades(conn: Connection, items: Iterable[Trade]) -> list[Trade]:
    """Copy trades (idempotent by the trade key); returns the rows that were new."""
    out: list[Trade] = []
    for t in items:
        res = conn.execute(insert(S.trades).prefix_with("OR IGNORE").values(**trade_row(t)))
        if res.rowcount:
            out.append(t)
    return out


def insert_trades(conn: Connection, items: Iterable[Trade]) -> int:
    """Copy trades (idempotent by the trade key); returns the number of new rows."""
    return len(copy_trades(conn, items))


def _trade_of(m: Any) -> Trade:
    return msgspec.convert(
        {
            "trading_day": m["trading_day"],
            "exchange": m["exchange"],
            "trade_id": m["trade_id"],
            "direction": m["direction"],
            "account_id": m["account_id"],
            "instrument_id": m["instrument_id"],
            "client_order_id": m["client_order_id"],
            "origin": m["origin"],
            "broker_order_ref": "",  # not stored in the copy (03 §4.4)
            "offset": m["offset"],
            "price": float(m["price"]),
            "qty": int(m["qty"]),
            "trade_ts_ns": int(m["trade_ts_ns"]),
            "trade_seq": int(m["trade_seq"]),
            "received_ns": int(m["received_ns"]),
        },
        Trade,
    )


def load_trades(conn: Connection, trading_day: str, account_id: str | None = None) -> list[Trade]:
    t = S.trades
    q = select(t).where(t.c.trading_day == trading_day)
    if account_id is not None:
        q = q.where(t.c.account_id == account_id)
    rows = conn.execute(q.order_by(t.c.trade_seq, t.c.trade_id, t.c.direction)).mappings()
    return [_trade_of(m) for m in rows]


# -------------------------------------------------------------------- cash copy
def copy_cash(conn: Connection, items: Iterable[CashMovement], src_instance: str) -> list[CashMovement]:
    """Copy cash movements (idempotent by ``movement_id``); returns the movements that were new."""
    out: list[CashMovement] = []
    for c in items:
        res = conn.execute(
            insert(S.cash_movements)
            .prefix_with("OR IGNORE")
            .values(
                movement_id=c.movement_id,
                account_id=c.account_id,
                trading_day=c.trading_day,
                type=c.type.value,
                amount=str(c.amount),
                actor=c.actor,
                reason=c.reason,
                ts_ns=c.ts_ns,
                src_instance=src_instance,
            )
        )
        if res.rowcount:
            out.append(c)
    return out


def insert_cash(conn: Connection, items: Iterable[CashMovement], src_instance: str) -> int:
    """Copy cash movements (idempotent by ``movement_id``); returns the number of new rows."""
    return len(copy_cash(conn, items, src_instance))


def load_cash(conn: Connection, trading_day: str, account_id: str | None = None) -> list[CashMovement]:
    t = S.cash_movements
    q = select(t).where(t.c.trading_day == trading_day)
    if account_id is not None:
        q = q.where(t.c.account_id == account_id)
    out: list[CashMovement] = []
    for m in conn.execute(q.order_by(t.c.ts_ns, t.c.movement_id)).mappings():
        out.append(
            msgspec.convert(
                {
                    "movement_id": m["movement_id"],
                    "account_id": m["account_id"],
                    "trading_day": m["trading_day"],
                    "type": m["type"],
                    "amount": m["amount"],
                    "actor": m["actor"],
                    "reason": m["reason"],
                    "ts_ns": int(m["ts_ns"]),
                },
                CashMovement,
            )
        )
    return out


def copy_counts(conn: Connection, trading_day: str) -> dict[str, list[int]]:
    """Per account ``[trade rows, cash rows]`` of the local copies (the step-5 re-check, 02 §8.4)."""
    out: dict[str, list[int]] = {}
    t, c = S.trades, S.cash_movements
    for i, table in enumerate((t, c)):
        rows = conn.execute(
            select(table.c.account_id, func.count().label("n"))
            .where(table.c.trading_day == trading_day)
            .group_by(table.c.account_id)
        ).mappings()
        for m in rows:
            out.setdefault(str(m["account_id"]), [0, 0])[i] = int(m["n"])
    return dict(sorted(out.items()))


# --------------------------------------------------------------------- eod runs
WARNINGS_KEY = "WARNINGS"
MAX_WARNINGS = 50
@dataclass
class EodRun:
    """One ``eod_runs`` row (saga state of ``(trading_day, version)``)."""

    trading_day: str
    version: int
    phase: EodPhase
    next_trading_day: str
    triggered_by: str
    config_hash: str
    step_log: dict[str, Any] = field(default_factory=dict)
    error: str | None = None
    started_ns: int = 0
    updated_ns: int = 0
    finished_ns: int | None = None

    @property
    def effective(self) -> bool:
        return self.phase in EFFECTIVE_PHASES

    @property
    def terminal(self) -> bool:
        return self.phase in TERMINAL_PHASES

    @property
    def in_progress(self) -> bool:
        return not self.terminal

    def log(self, phase: EodPhase | str) -> dict[str, Any]:
        entry = self.step_log.get(str(phase))
        return dict(entry) if isinstance(entry, dict) else {}


def _run_of(m: Any) -> EodRun:
    return EodRun(
        trading_day=str(m["trading_day"]),
        version=int(m["version"]),
        phase=EodPhase(str(m["phase"])),
        next_trading_day=str(m["next_trading_day"]),
        triggered_by=str(m["triggered_by"]),
        config_hash=str(m["config_hash"]),
        step_log=loads(m["step_log"]) or {},
        error=m["error"],
        started_ns=int(m["started_ns"]),
        updated_ns=int(m["updated_ns"]),
        finished_ns=int(m["finished_ns"]) if m["finished_ns"] is not None else None,
    )


def get_run(conn: Connection, trading_day: str, version: int) -> EodRun | None:
    t = S.eod_runs
    m = conn.execute(select(t).where(t.c.trading_day == trading_day, t.c.version == version)).mappings().first()
    return _run_of(m) if m is not None else None


def runs_of(conn: Connection, trading_day: str) -> list[EodRun]:
    t = S.eod_runs
    rows = conn.execute(select(t).where(t.c.trading_day == trading_day).order_by(t.c.version)).mappings()
    return [_run_of(m) for m in rows]


def latest_run(conn: Connection, trading_day: str) -> EodRun | None:
    runs = runs_of(conn, trading_day)
    return runs[-1] if runs else None


def unfinished_runs(conn: Connection) -> list[EodRun]:
    t = S.eod_runs
    rows = conn.execute(
        select(t)
        .where(t.c.phase.not_in([EodPhase.DONE.value, EodPhase.FAILED.value]))
        .order_by(t.c.trading_day, t.c.version)
    ).mappings()
    return [_run_of(m) for m in rows]


def insert_run(conn: Connection, run: EodRun) -> None:
    conn.execute(
        insert(S.eod_runs).values(
            trading_day=run.trading_day,
            version=run.version,
            phase=run.phase.value,
            next_trading_day=run.next_trading_day,
            triggered_by=run.triggered_by,
            config_hash=run.config_hash,
            step_log=dumps(run.step_log),
            error=run.error,
            started_ns=run.started_ns,
            updated_ns=run.updated_ns,
            finished_ns=run.finished_ns,
        )
    )


def update_run(
    conn: Connection,
    run: EodRun,
) -> None:
    """Persist phase, step log, error and timestamps of ``run``.

    ``step_log["WARNINGS"]`` is owned by :func:`add_run_warning` (written by
    other transactions while the saga holds the run in memory): the stored
    list is kept and copied into ``run.step_log``.
    """
    t = S.eod_runs
    stored = conn.execute(
        select(t.c.step_log).where(t.c.trading_day == run.trading_day, t.c.version == run.version)
    ).scalar()
    warnings = (loads(stored) or {}).get(WARNINGS_KEY) if stored else None
    if warnings:
        run.step_log = {**run.step_log, WARNINGS_KEY: warnings}
    conn.execute(
        update(t)
        .where(t.c.trading_day == run.trading_day, t.c.version == run.version)
        .values(
            phase=run.phase.value,
            step_log=dumps(run.step_log),
            error=run.error,
            updated_ns=run.updated_ns,
            finished_ns=run.finished_ns,
        )
    )


def add_run_warning(conn: Connection, trading_day: str, version: int, entry: Mapping[str, Any]) -> EodRun | None:
    """Append ``entry`` to ``step_log["WARNINGS"]`` of a run (kept: the first and the latest entries)."""
    run = get_run(conn, trading_day, version)
    if run is None:
        return None
    items = list(run.step_log.get(WARNINGS_KEY) or [])
    items.append(dict(entry))
    if len(items) > MAX_WARNINGS:
        items = items[:1] + items[-(MAX_WARNINGS - 1) :]
    run.step_log = {**run.step_log, WARNINGS_KEY: items}
    t = S.eod_runs
    conn.execute(
        update(t)
        .where(t.c.trading_day == trading_day, t.c.version == version)
        .values(step_log=dumps(run.step_log))
    )
    return run


def update_step_log(conn: Connection, run: EodRun) -> None:
    """Write ``run.step_log`` as it is (warnings bookkeeping; the saga uses :func:`update_run`)."""
    t = S.eod_runs
    conn.execute(
        update(t)
        .where(t.c.trading_day == run.trading_day, t.c.version == run.version)
        .values(step_log=dumps(run.step_log))
    )


def latest_valid_run(conn: Connection, trading_day: str) -> EodRun | None:
    """The newest run of a day that did not fail (in progress or effective)."""
    runs = [r for r in runs_of(conn, trading_day) if r.phase != EodPhase.FAILED]
    return runs[-1] if runs else None


def unrolled_run_before(conn: Connection, trading_day: str) -> EodRun | None:
    """The oldest run of an EARLIER day that is in progress and not yet ROLLED (02 §8.4: days roll in order)."""
    t = S.eod_runs
    before_roll = [p.value for p in PHASE_ORDER[: PHASE_ORDER.index(EodPhase.ROLLED)]]
    m = (
        conn.execute(
            select(t)
            .where(t.c.trading_day < trading_day, t.c.phase.in_(before_roll))
            .order_by(t.c.trading_day, t.c.version)
            .limit(1)
        )
        .mappings()
        .first()
    )
    return _run_of(m) if m is not None else None


def max_run_version(conn: Connection, trading_day: str) -> int:
    t = S.eod_runs
    v = conn.execute(select(func.max(t.c.version)).where(t.c.trading_day == trading_day)).scalar()
    return int(v) if v is not None else 0


def effective_version(conn: Connection, trading_day: str) -> int | None:
    """The valid settlement of a day: the largest version in SETTLED/ROLLED/REPORTED/DONE (03 §4.4)."""
    t = S.eod_runs
    v = conn.execute(
        select(func.max(t.c.version)).where(
            t.c.trading_day == trading_day, t.c.phase.in_([p.value for p in EFFECTIVE_PHASES])
        )
    ).scalar()
    return int(v) if v is not None else None


def later_settlement_exists(conn: Connection, trading_day: str) -> str | None:
    """A later trading day with a non-FAILED run (blocks re-settling ``trading_day``)."""
    t = S.eod_runs
    v = conn.execute(
        select(func.min(t.c.trading_day)).where(t.c.trading_day > trading_day, t.c.phase != EodPhase.FAILED.value)
    ).scalar()
    return str(v) if v is not None else None


# ----------------------------------------------------------------------- prices
@dataclass(frozen=True)
class OverrideRow:
    override_id: str
    trading_day: str
    instrument_id: str
    price: str
    reason: str
    actor: str
    created_ns: int


def insert_official(
    conn: Connection,
    *,
    trading_day: str,
    prices: Sequence[tuple[str, str]],
    import_id: str,
    source: str,
    actor: str,
    imported_ns: int,
) -> int:
    for inst, price in prices:
        conn.execute(
            insert(S.official_prices).values(
                trading_day=trading_day,
                instrument_id=inst,
                import_id=import_id,
                price=price,
                source=source,
                imported_by=actor,
                imported_ns=imported_ns,
            )
        )
    return len(prices)


def latest_official(conn: Connection, trading_day: str) -> dict[str, str]:
    """Latest imported official price per instrument (03 §4.4: the newest import wins)."""
    t = S.official_prices
    out: dict[str, tuple[int, str, str]] = {}
    for m in conn.execute(select(t).where(t.c.trading_day == trading_day)).mappings():
        key = (int(m["imported_ns"]), str(m["import_id"]))
        cur = out.get(m["instrument_id"])
        if cur is None or key > (cur[0], cur[1]):
            out[str(m["instrument_id"])] = (key[0], key[1], str(m["price"]))
    return {inst: v[2] for inst, v in out.items()}


def insert_override(
    conn: Connection,
    *,
    override_id: str,
    trading_day: str,
    instrument_id: str,
    price: str,
    reason: str,
    actor: str,
    created_ns: int,
) -> None:
    """Record a manual price; older active overrides of the same contract are superseded."""
    t = S.price_overrides
    conn.execute(
        update(t)
        .where(t.c.trading_day == trading_day, t.c.instrument_id == instrument_id, t.c.superseded_ns.is_(None))
        .values(superseded_ns=created_ns)
    )
    conn.execute(
        insert(t).values(
            override_id=override_id,
            trading_day=trading_day,
            instrument_id=instrument_id,
            price=price,
            reason=reason,
            actor=actor,
            created_ns=created_ns,
            superseded_ns=None,
        )
    )


def active_overrides(conn: Connection, trading_day: str) -> dict[str, OverrideRow]:
    t = S.price_overrides
    out: dict[str, OverrideRow] = {}
    rows = conn.execute(
        select(t).where(t.c.trading_day == trading_day, t.c.superseded_ns.is_(None)).order_by(t.c.created_ns)
    ).mappings()
    for m in rows:
        out[str(m["instrument_id"])] = OverrideRow(
            override_id=str(m["override_id"]),
            trading_day=str(m["trading_day"]),
            instrument_id=str(m["instrument_id"]),
            price=str(m["price"]),
            reason=str(m["reason"]),
            actor=str(m["actor"]),
            created_ns=int(m["created_ns"]),
        )
    return out


def _price_view_of(m: Any) -> SettlementPriceView:
    return msgspec.convert(
        {
            "trading_day": m["trading_day"],
            "instrument_id": m["instrument_id"],
            "price": m["price"],
            "source": m["source"],
            "method": m["method"],
            "vwap_raw": m["vwap_raw"],
            "rounding": m["rounding"],
            "input_quality": m["input_quality"],
            "version": int(m["version"]),
            "operator": m["operator"],
            "reason": m["reason"],
        },
        SettlementPriceView,
    )


def load_prices(conn: Connection, trading_day: str, version: int) -> list[SettlementPriceView]:
    t = S.settlement_prices
    rows = conn.execute(
        select(t).where(t.c.trading_day == trading_day, t.c.version == version).order_by(t.c.instrument_id)
    ).mappings()
    return [_price_view_of(m) for m in rows]


# ------------------------------------------------------------------- settlement
_RESULT_TABLES = (
    S.settlement_prices,
    S.account_settlements,
    S.position_settlements,
    S.margin_groups,
    S.trade_settlements,
    S.close_details,
)


def save_settlement(conn: Connection, result: SettlementResult, *, created_ns: int) -> None:
    """Replace every result row of ``(trading_day, version)`` (re-running a version is idempotent)."""
    td, v = result.trading_day, result.version
    for t in _RESULT_TABLES:
        conn.execute(delete(t).where(t.c.trading_day == td, t.c.version == v))
    for p in result.prices:
        row = struct_row(p)
        row.update(trading_day=td, version=v, created_ns=created_ns)
        conn.execute(insert(S.settlement_prices).values(**row))
    for a in result.accounts:
        row = struct_row(a)
        row["checks_passed"] = 1 if a.checks_passed else 0
        row["created_ns"] = created_ns
        conn.execute(insert(S.account_settlements).values(**row))
    for pos in result.positions:
        conn.execute(insert(S.position_settlements).values(**struct_row(pos)))
    for g in result.margin_groups:
        conn.execute(insert(S.margin_groups).values(**struct_row(g, json_fields=("detail",))))
    for tr in result.trades:
        conn.execute(insert(S.trade_settlements).values(**struct_row(tr)))
    for cd in result.close_details:
        conn.execute(insert(S.close_details).values(**struct_row(cd)))


def _rows(conn: Connection, table: Any, td: str, version: int, account_id: str | None, order: Sequence[Any]) -> list[Any]:
    q = select(table).where(table.c.trading_day == td, table.c.version == version)
    if account_id is not None and "account_id" in table.c:
        q = q.where(table.c.account_id == account_id)
    return list(conn.execute(q.order_by(*order)).mappings())


def _convert(m: Mapping[str, Any], cls: type[Any], *, drop: Sequence[str] = (), json_fields: Sequence[str] = ()) -> Any:
    data = {str(k): v for k, v in m.items() if str(k) not in drop}
    for f in json_fields:
        data[f] = loads(data.get(f)) or {}
    return msgspec.convert(data, cls)


def load_settlement(
    conn: Connection, trading_day: str, version: int, account_id: str | None = None
) -> SettlementResult | None:
    """The persisted :class:`SettlementResult` of ``(trading_day, version)`` (optionally one account)."""
    acc_rows = _rows(conn, S.account_settlements, trading_day, version, account_id, [S.account_settlements.c.account_id])
    if not acc_rows:
        return None
    accounts = []
    for m in acc_rows:
        d = {str(k): v for k, v in m.items()}
        d.pop("created_ns", None)
        d["checks_passed"] = bool(d["checks_passed"])
        accounts.append(msgspec.convert(d, AccountSettlement))
    pt = S.position_settlements
    positions = [
        _convert(m, PositionSettlement)
        for m in _rows(conn, pt, trading_day, version, account_id, [pt.c.account_id, pt.c.instrument_id, pt.c.direction])
    ]
    gt = S.margin_groups
    groups = [
        _convert(m, MarginGroupView, json_fields=("detail",))
        for m in _rows(conn, gt, trading_day, version, account_id, [gt.c.account_id, gt.c.margin_group])
    ]
    tt = S.trade_settlements
    trade_rows = _rows(conn, tt, trading_day, version, account_id, [tt.c.account_id, tt.c.trade_id, tt.c.direction])
    trades = [_convert(m, TradeSettlementView) for m in trade_rows]
    ct = S.close_details
    details = [
        _convert(m, CloseDetailView)
        for m in _rows(conn, ct, trading_day, version, account_id, [ct.c.account_id, ct.c.detail_no])
    ]
    prices = load_prices(conn, trading_day, version)
    checks = load_checks(conn, trading_day, version, account_id=account_id)
    return SettlementResult(
        trading_day=trading_day,
        version=version,
        accounts=accounts,
        positions=positions,
        margin_groups=groups,
        trades=trades,
        close_details=details,
        prices=prices,
        checks=checks,
    )


def _scalar_text(detail: Mapping[str, Any] | None, key: str) -> str | None:
    if not detail:
        return None
    value = detail.get(key)
    if value is None or isinstance(value, dict | list):
        return None
    return str(value)


def insert_checks(conn: Connection, trading_day: str, version: int, checks: Iterable[CheckResult], *, created_ns: int) -> int:
    n = 0
    for c in checks:
        conn.execute(
            insert(S.recon_checks).values(
                trading_day=trading_day,
                version=version,
                account_id=c.account_id,
                check=c.check.value,
                status=c.status.value,
                expected=_scalar_text(c.detail, "expected"),
                actual=_scalar_text(c.detail, "actual"),
                diff=_scalar_text(c.detail, "diff"),
                detail=dumps(c.detail or {}),
                created_ns=created_ns,
            )
        )
        n += 1
    return n


def replace_checks(
    conn: Connection,
    trading_day: str,
    version: int,
    checks: Iterable[CheckResult],
    *,
    kinds: Iterable[ReconCheck],
    created_ns: int,
) -> int:
    """Delete the rows of ``kinds`` for the run, then insert ``checks`` (idempotent step re-runs)."""
    t = S.recon_checks
    kind_values = [k.value for k in kinds]
    if kind_values:
        conn.execute(
            delete(t).where(t.c.trading_day == trading_day, t.c.version == version, t.c.check.in_(kind_values))
        )
    return insert_checks(conn, trading_day, version, checks, created_ns=created_ns)


def load_checks(conn: Connection, trading_day: str, version: int, account_id: str | None = None) -> list[CheckResult]:
    t = S.recon_checks
    q = select(t).where(t.c.trading_day == trading_day, t.c.version == version)
    if account_id is not None:
        q = q.where(or_(t.c.account_id == account_id, t.c.account_id.is_(None)))
    out: list[CheckResult] = []
    for m in conn.execute(q.order_by(t.c.check_id)).mappings():
        out.append(
            CheckResult(
                check=ReconCheck(str(m["check"])),
                status=CheckStatus(str(m["status"])),
                account_id=m["account_id"],
                detail=loads(m["detail"]) or {},
            )
        )
    return out


def account_history(conn: Connection, until_td: str, days: int) -> dict[str, list[dict[str, Any]]]:
    """``account_settlements`` of the effective version of the last ``days`` settled days up to ``until_td``."""
    r = S.eod_runs
    eff = (
        select(r.c.trading_day, func.max(r.c.version).label("version"))
        .where(r.c.trading_day <= until_td, r.c.phase.in_([p.value for p in EFFECTIVE_PHASES]))
        .group_by(r.c.trading_day)
        .order_by(r.c.trading_day.desc())
        .limit(days)
        .subquery()
    )
    a = S.account_settlements
    rows = conn.execute(
        select(a)
        .join(eff, and_(a.c.trading_day == eff.c.trading_day, a.c.version == eff.c.version))
        .order_by(a.c.trading_day, a.c.account_id)
    ).mappings()
    out: dict[str, list[dict[str, Any]]] = {}
    for m in rows:
        out.setdefault(str(m["account_id"]), []).append(
            {
                "trading_day": m["trading_day"],
                "equity": m["equity"],
                "nav": m["nav"],
                "max_drawdown": m["max_drawdown"],
                "hwm_equity": m["hwm_equity"],
                "cum_net_deposit": m["cum_net_deposit"],
            }
        )
    return out


def max_drawdowns(conn: Connection, until_td: str) -> dict[str, str]:
    """Per account the largest daily ``max_drawdown`` (the current drawdown from the high-water mark of each
    settled day) over the effective versions of every day up to ``until_td``: the max drawdown since the
    first settlement, at daily (EOD) granularity."""
    r = S.eod_runs
    eff = (
        select(r.c.trading_day, func.max(r.c.version).label("version"))
        .where(r.c.trading_day <= until_td, r.c.phase.in_([p.value for p in EFFECTIVE_PHASES]))
        .group_by(r.c.trading_day)
        .subquery()
    )
    a = S.account_settlements
    out: dict[str, str] = {}
    for m in conn.execute(
        select(a.c.account_id, a.c.max_drawdown).join(
            eff, and_(a.c.trading_day == eff.c.trading_day, a.c.version == eff.c.version)
        )
    ).mappings():
        acct, value = str(m["account_id"]), str(m["max_drawdown"])
        if acct not in out or D(value) > D(out[acct]):
            out[acct] = value
    return dict(sorted(out.items()))


# ------------------------------------------------------------------ SOD versions
@dataclass(frozen=True)
class SodVersionRow:
    trading_day: str
    version: int
    source: str
    from_trading_day: str | None
    from_version: int | None
    reason: str | None
    actor: str | None
    created_ns: int
    pushed_ns: int | None


def _sod_version_of(m: Any) -> SodVersionRow:
    return SodVersionRow(
        trading_day=str(m["trading_day"]),
        version=int(m["version"]),
        source=str(m["source"]),
        from_trading_day=m["from_trading_day"],
        from_version=int(m["from_version"]) if m["from_version"] is not None else None,
        reason=m["reason"],
        actor=m["actor"],
        created_ns=int(m["created_ns"]),
        pushed_ns=int(m["pushed_ns"]) if m["pushed_ns"] is not None else None,
    )


def sod_versions_of(conn: Connection, trading_day: str) -> list[SodVersionRow]:
    t = S.sod_versions
    rows = conn.execute(select(t).where(t.c.trading_day == trading_day).order_by(t.c.version)).mappings()
    return [_sod_version_of(m) for m in rows]


def get_sod_version(conn: Connection, trading_day: str, version: int) -> SodVersionRow | None:
    t = S.sod_versions
    m = conn.execute(select(t).where(t.c.trading_day == trading_day, t.c.version == version)).mappings().first()
    return _sod_version_of(m) if m is not None else None


def latest_sod_version(conn: Connection, trading_day: str) -> SodVersionRow | None:
    rows = sod_versions_of(conn, trading_day)
    return rows[-1] if rows else None


def latest_pushed_version(conn: Connection, trading_day: str) -> SodVersionRow | None:
    rows = [r for r in sod_versions_of(conn, trading_day) if r.pushed_ns is not None]
    return rows[-1] if rows else None


def latest_sod_day(conn: Connection) -> str | None:
    t = S.sod_versions
    v = conn.execute(select(func.max(t.c.trading_day))).scalar()
    return str(v) if v is not None else None


def next_sod_version(conn: Connection, trading_day: str, *, seed: bool) -> int:
    """Next snapshot version of a day: the first seed is 0, otherwise max + 1 (03 §4.4 ``sod_versions``)."""
    rows = sod_versions_of(conn, trading_day)
    if not rows:
        return 0 if seed else 1
    return rows[-1].version + 1


def save_sod(
    conn: Connection,
    snap: SodSnapshot,
    *,
    source: S.SodSource,
    created_ns: int,
    reason: str | None = None,
    actor: str | None = None,
) -> None:
    """Insert one snapshot version (``sod_versions`` + ``sod_accounts`` + ``sod_lots`` + ``sod_prices``)."""
    td, v = snap.trading_day, snap.version
    ref = snap.from_settlement
    conn.execute(
        insert(S.sod_versions).values(
            trading_day=td,
            version=v,
            source=source.value,
            from_trading_day=ref.trading_day if ref is not None else None,
            from_version=ref.version if ref is not None else None,
            reason=reason,
            actor=actor,
            created_ns=created_ns,
            pushed_ns=None,
        )
    )
    for a in snap.accounts:
        conn.execute(
            insert(S.sod_accounts).values(
                trading_day=td,
                account_id=a.account_id,
                version=v,
                pre_balance=a.pre_balance,
                initial_capital=a.initial_capital,
                cum_net_deposit=a.cum_net_deposit,
                hwm_equity=a.hwm_equity,
                created_ns=created_ns,
            )
        )
        for lot in a.lots:
            conn.execute(
                insert(S.sod_lots).values(
                    trading_day=td,
                    account_id=a.account_id,
                    instrument_id=lot.instrument_id,
                    direction=lot.direction.value,
                    lot_no=lot.lot_no,
                    version=v,
                    open_td=lot.open_td,
                    open_ts_ns=lot.open_ts_ns,
                    open_price=lot.open_price,
                    qty=lot.qty,
                    basis_price=lot.basis_price,
                )
            )
    for inst, px in sorted(snap.pre_settle.items()):
        conn.execute(insert(S.sod_prices).values(trading_day=td, instrument_id=inst, version=v, pre_settle=px))


def load_sod(conn: Connection, trading_day: str, version: int, account_ids: Sequence[str] | None = None) -> SodSnapshot | None:
    """Snapshot ``(trading_day, version)``; ``account_ids`` filters accounts (empty/None: all)."""
    row = get_sod_version(conn, trading_day, version)
    if row is None:
        return None
    wanted = set(account_ids or ())
    at, lt, pt = S.sod_accounts, S.sod_lots, S.sod_prices
    lots: dict[str, list[SodLot]] = {}
    for m in conn.execute(
        select(lt)
        .where(lt.c.trading_day == trading_day, lt.c.version == version)
        .order_by(lt.c.account_id, lt.c.instrument_id, lt.c.direction, lt.c.lot_no)
    ).mappings():
        lots.setdefault(str(m["account_id"]), []).append(
            msgspec.convert(
                {
                    "instrument_id": m["instrument_id"],
                    "direction": m["direction"],
                    "lot_no": int(m["lot_no"]),
                    "open_td": m["open_td"],
                    "open_ts_ns": int(m["open_ts_ns"]),
                    "open_price": m["open_price"],
                    "qty": int(m["qty"]),
                    "basis_price": m["basis_price"],
                },
                SodLot,
            )
        )
    accounts: list[SodAccount] = []
    for m in conn.execute(
        select(at).where(at.c.trading_day == trading_day, at.c.version == version).order_by(at.c.account_id)
    ).mappings():
        acct = str(m["account_id"])
        if wanted and acct not in wanted:
            continue
        accounts.append(
            SodAccount(
                account_id=acct,
                pre_balance=str(m["pre_balance"]),
                initial_capital=str(m["initial_capital"]),
                cum_net_deposit=str(m["cum_net_deposit"]),
                hwm_equity=str(m["hwm_equity"]),
                lots=lots.get(acct, []),
            )
        )
    prices = {
        str(m["instrument_id"]): str(m["pre_settle"])
        for m in conn.execute(select(pt).where(pt.c.trading_day == trading_day, pt.c.version == version)).mappings()
    }
    ref = (
        SettlementRef(trading_day=row.from_trading_day, version=row.from_version)
        if row.from_trading_day is not None and row.from_version is not None
        else None
    )
    return SodSnapshot(trading_day=trading_day, version=version, from_settlement=ref, accounts=accounts, pre_settle=prices)


def set_pushed(conn: Connection, trading_day: str, version: int, ns: int) -> int:
    t = S.sod_versions
    res = conn.execute(
        update(t)
        .where(t.c.trading_day == trading_day, t.c.version == version, t.c.pushed_ns.is_(None))
        .values(pushed_ns=ns)
    )
    return int(res.rowcount or 0)


# ---------------------------------------------------------------------- reports
def _eq(col: Any, value: Any) -> Any:
    return col.is_(None) if value is None else col == value


def upsert_report(conn: Connection, r: ReportReady) -> tuple[ReportReady, bool]:
    """Register a report file, idempotent by ``(trading_day, version, type, account_id, path)``.

    A file registered again keeps its ``report_id``; its hash, size and time are
    updated when the content changed. Returns the registered row and whether it
    is new or changed (then a ``report.ready`` is due).
    """
    t = S.reports
    m = (
        conn.execute(
            select(t)
            .where(
                _eq(t.c.trading_day, r.trading_day),
                _eq(t.c.version, r.version),
                t.c.type == r.type.value,
                _eq(t.c.account_id, r.account_id),
                t.c.path == r.path,
            )
            .order_by(t.c.created_ns, t.c.report_id)
            .limit(1)
        )
        .mappings()
        .first()
    )
    if m is None:
        insert_report(conn, r)
        return r, True
    cur = _report_of(m)
    if cur.sha256 == r.sha256 and cur.size_bytes == r.size_bytes:
        return cur, False
    updated = msgspec.structs.replace(r, report_id=cur.report_id)
    conn.execute(
        update(t)
        .where(t.c.report_id == cur.report_id)
        .values(sha256=r.sha256, size_bytes=r.size_bytes, created_ns=r.created_ns)
    )
    return updated, True


def insert_report(conn: Connection, r: ReportReady) -> None:
    conn.execute(
        insert(S.reports).values(
            report_id=r.report_id,
            trading_day=r.trading_day,
            type=r.type.value,
            account_id=r.account_id,
            version=r.version,
            path=r.path,
            sha256=r.sha256,
            size_bytes=r.size_bytes,
            created_ns=r.created_ns,
        )
    )


def _report_of(m: Any) -> ReportReady:
    return ReportReady(
        report_id=str(m["report_id"]),
        trading_day=m["trading_day"],
        type=ReportType(str(m["type"])),
        account_id=m["account_id"],
        path=str(m["path"]),
        sha256=str(m["sha256"]),
        size_bytes=int(m["size_bytes"]),
        version=int(m["version"]) if m["version"] is not None else None,
        created_ns=int(m["created_ns"]),
    )


def list_reports(conn: Connection, trading_day: str | None = None) -> list[ReportReady]:
    t = S.reports
    q = select(t)
    if trading_day is not None:
        q = q.where(t.c.trading_day == trading_day)
    return [_report_of(m) for m in conn.execute(q.order_by(t.c.created_ns, t.c.report_id)).mappings()]


def get_report(conn: Connection, report_id: str) -> ReportReady | None:
    t = S.reports
    m = conn.execute(select(t).where(t.c.report_id == report_id)).mappings().first()
    return _report_of(m) if m is not None else None


# --------------------------------------------------------------- config history
@dataclass(frozen=True)
class ConfigRow:
    """One ``config_history`` row: a committed config version and when its settlement sections apply."""

    config_version: int
    config_hash: str
    settle_hash: str
    settle_from: str | None
    recorded_ns: int


def _config_of(m: Any) -> ConfigRow:
    return ConfigRow(
        config_version=int(m["config_version"]),
        config_hash=str(m["config_hash"]),
        settle_hash=str(m["settle_hash"]),
        settle_from=m["settle_from"],
        recorded_ns=int(m["recorded_ns"]),
    )


def config_rows(conn: Connection) -> list[ConfigRow]:
    t = S.config_history
    return [_config_of(m) for m in conn.execute(select(t).order_by(t.c.config_version)).mappings()]


def insert_config(conn: Connection, row: ConfigRow) -> bool:
    """Record a config version once (``INSERT OR IGNORE``); returns whether it was new."""
    res = conn.execute(
        insert(S.config_history)
        .prefix_with("OR IGNORE")
        .values(
            config_version=row.config_version,
            config_hash=row.config_hash,
            settle_hash=row.settle_hash,
            settle_from=row.settle_from,
            recorded_ns=row.recorded_ns,
        )
    )
    return bool(res.rowcount)


def config_for_day(conn: Connection, trading_day: str) -> ConfigRow | None:
    """The newest recorded version whose settlement sections are in force on ``trading_day``."""
    t = S.config_history
    m = (
        conn.execute(
            select(t)
            .where(or_(t.c.settle_from.is_(None), t.c.settle_from <= trading_day))
            .order_by(t.c.config_version.desc())
            .limit(1)
        )
        .mappings()
        .first()
    )
    return _config_of(m) if m is not None else None


# -------------------------------------------------------------------- backtests
def insert_backtest(conn: Connection, **values: Any) -> None:
    if "summary" in values and values["summary"] is not None and not isinstance(values["summary"], str):
        values["summary"] = dumps(values["summary"])
    conn.execute(insert(S.backtest_runs).values(**values))


def update_backtest(conn: Connection, run_id: str, **values: Any) -> None:
    if "summary" in values and values["summary"] is not None and not isinstance(values["summary"], str):
        values["summary"] = dumps(values["summary"])
    if "state" in values and isinstance(values["state"], Enum):
        values["state"] = values["state"].value
    t = S.backtest_runs
    conn.execute(update(t).where(t.c.run_id == run_id).values(**values))


def _backtest_of(m: Any) -> BacktestStatus:
    summary = loads(m["summary"])
    return BacktestStatus(
        run_id=str(m["run_id"]),
        name=str(m["name"]),
        state=BacktestState(str(m["state"])),
        progress=float(m["progress"] or 0.0),
        submitted_ns=int(m["submitted_ns"]),
        started_ns=int(m["started_ns"]) if m["started_ns"] is not None else None,
        finished_ns=int(m["finished_ns"]) if m["finished_ns"] is not None else None,
        report_id=m["report_id"],
        summary=summary if isinstance(summary, dict) else None,
        error=m["error"],
    )


def get_backtest(conn: Connection, run_id: str) -> BacktestStatus | None:
    t = S.backtest_runs
    m = conn.execute(select(t).where(t.c.run_id == run_id)).mappings().first()
    return _backtest_of(m) if m is not None else None


def list_backtests(conn: Connection) -> list[BacktestStatus]:
    t = S.backtest_runs
    return [_backtest_of(m) for m in conn.execute(select(t).order_by(t.c.submitted_ns.desc(), t.c.run_id)).mappings()]


def backtests_in_states(conn: Connection, states: Iterable[BacktestState]) -> list[BacktestStatus]:
    t = S.backtest_runs
    vals = [s.value for s in states]
    return [_backtest_of(m) for m in conn.execute(select(t).where(t.c.state.in_(vals))).mappings()]


__all__ = [
    "EFFECTIVE_PHASES",
    "PHASE_ORDER",
    "TERMINAL_PHASES",
    "MAX_WARNINGS",
    "WARNINGS_KEY",
    "ConfigRow",
    "EodRun",
    "OverrideRow",
    "SodVersionRow",
    "account_history",
    "active_overrides",
    "add_run_warning",
    "config_for_day",
    "config_rows",
    "copy_cash",
    "copy_counts",
    "copy_trades",
    "backtests_in_states",
    "dumps",
    "effective_version",
    "get_backtest",
    "get_report",
    "get_run",
    "get_sod_version",
    "insert_backtest",
    "insert_cash",
    "insert_checks",
    "insert_config",
    "insert_official",
    "insert_override",
    "insert_report",
    "insert_run",
    "insert_trades",
    "later_settlement_exists",
    "latest_official",
    "latest_pushed_version",
    "latest_run",
    "latest_sod_day",
    "latest_sod_version",
    "latest_valid_run",
    "list_backtests",
    "list_reports",
    "load_cash",
    "load_checks",
    "load_prices",
    "load_settlement",
    "load_sod",
    "load_trades",
    "loads",
    "max_drawdowns",
    "max_run_version",
    "next_sod_version",
    "phase_index",
    "price_text",
    "replace_checks",
    "runs_of",
    "save_settlement",
    "save_sod",
    "set_pushed",
    "sod_versions_of",
    "struct_row",
    "trade_row",
    "unfinished_runs",
    "unrolled_run_before",
    "upsert_report",
    "update_backtest",
    "update_step_log",
    "update_run",
]
