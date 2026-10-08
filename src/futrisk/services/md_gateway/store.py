"""md.db access functions (03 §4.1 tables, §7 transactions).

All functions take the caller's :class:`sqlalchemy.Connection`: writes run inside a writer
job of :class:`futrisk.db.Database` (one transaction per job), reads on a ``query_only``
connection. Rows and message structs share their field names, so they are converted with
``msgspec`` (every field, enums as their string values, ``hourly`` as JSON).
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass
from typing import Any

import msgspec
from sqlalchemy import Connection, Table, and_, delete, select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from futrisk.messages.enums import GapStatus, MdEventKind, SourceId
from futrisk.messages.md import GapInfo, MdEvent, SettleInput
from futrisk.services.md_gateway.events import GapRecord
from futrisk.services.md_gateway.model import OrderKey
from futrisk.services.md_gateway.schema import feed_cursors, gaps, md_events, settle_inputs, watermarks


@dataclass(frozen=True)
class FeedCursor:
    """A row of ``feed_cursors``."""

    source: SourceId
    epoch: str | None
    last_contiguous_seq: int | None
    last_exch_ts_ns: int | None
    trading_day: str
    updated_ns: int


def _row(r: Mapping[Any, Any]) -> dict[str, Any]:
    """A result row as a plain dict (SQLAlchemy keys are a ``str`` subclass that ``msgspec`` rejects)."""
    return {str(k): v for k, v in r.items()}


def _fields(obj: msgspec.Struct) -> dict[str, Any]:
    """Every field of a struct as builtins (``to_builtins`` alone omits fields at their defaults)."""
    return msgspec.to_builtins(msgspec.structs.asdict(obj))  # type: ignore[no-any-return]


def _upsert(conn: Connection, table: Table, key: list[str], values: dict[str, Any]) -> None:
    stmt = sqlite_insert(table).values(**values)
    conn.execute(stmt.on_conflict_do_update(index_elements=key, set_={k: v for k, v in values.items() if k not in key}))


# ---------------------------------------------------------------- cursors / watermarks
def load_feed_cursors(conn: Connection) -> dict[SourceId, FeedCursor]:
    out: dict[SourceId, FeedCursor] = {}
    for r in conn.execute(select(feed_cursors)).mappings():
        src = SourceId(r["source"])
        out[src] = FeedCursor(**{**_row(r), "source": src})
    return out


def save_feed_cursor(conn: Connection, cur: FeedCursor) -> None:
    _upsert(conn, feed_cursors, ["source"], {**asdict(cur), "source": str(cur.source)})


def _fp_to_db(fp: int | None) -> int | None:
    """Fingerprints are unsigned 64-bit; SQLite INTEGER is signed: store the two's complement."""
    return None if fp is None else fp - (1 << 64) if fp >= (1 << 63) else fp


def _fp_from_db(v: int | None) -> int | None:
    return None if v is None else int(v) + (1 << 64) if v < 0 else int(v)


def load_watermarks(conn: Connection) -> dict[str, tuple[OrderKey, int | None]]:
    """``instrument -> (order_key, fingerprint)`` of the last LIVE states (03 §4.1)."""
    rows = conn.execute(select(watermarks)).mappings()
    return {r["instrument_id"]: ((r["trading_day"], int(r["wm_volume"]), int(r["wm_exch_ts_ns"])), _fp_from_db(r["wm_fingerprint"])) for r in rows}


def save_watermarks(conn: Connection, rows: Sequence[tuple[str, OrderKey | None, int | None]], updated_ns: int) -> None:
    """Upsert watermarks (``None`` deletes the row)."""
    for iid, key, fp in rows:
        if key is None:
            conn.execute(delete(watermarks).where(watermarks.c.instrument_id == iid))
            continue
        values = {"instrument_id": iid, "trading_day": key[0], "wm_volume": key[1], "wm_exch_ts_ns": key[2], "updated_ns": updated_ns, "wm_fingerprint": _fp_to_db(fp)}
        _upsert(conn, watermarks, ["instrument_id"], values)


# ---------------------------------------------------------------- events / gaps
def upsert_gap(conn: Connection, rec: GapRecord) -> None:
    values = {**_fields(rec.info), "trading_day": rec.trading_day, "opened_ns": rec.opened_ns, "closed_ns": rec.closed_ns}
    _upsert(conn, gaps, ["gap_id"], values)


def insert_event(conn: Connection, ev: MdEvent, *, actor: str | None = None) -> None:
    """``md_events`` row; the gap snapshot at event time is kept in ``detail.gap`` for ``md.events``."""
    detail = dict(ev.detail)
    if ev.gap is not None:
        detail["gap"] = msgspec.to_builtins(ev.gap)
    row = _fields(ev)
    row.pop("gap")
    row.update(gap_id=ev.gap.gap_id if ev.gap is not None else None, actor=actor, detail=json.dumps(detail, ensure_ascii=False, default=str))
    conn.execute(md_events.insert().values(**row))


def query_events(
    conn: Connection, trading_day: str, kinds: Iterable[MdEventKind] | None = None, instrument_id: str | None = None
) -> list[MdEvent]:
    t = md_events
    cond = [t.c.trading_day == trading_day]
    if kinds:
        cond.append(t.c.kind.in_([str(k) for k in kinds]))
    if instrument_id is not None:
        cond.append(t.c.instrument_id == instrument_id)
    out: list[MdEvent] = []
    for r in conn.execute(select(t).where(and_(*cond)).order_by(t.c.ts_ns, t.c.event_id)).mappings():
        detail: dict[str, Any] = json.loads(r["detail"] or "{}")
        gap = detail.pop("gap", None) or (_gap_row(conn, r["gap_id"]) if r["gap_id"] else None)
        if r["actor"]:
            detail.setdefault("actor", r["actor"])
        out.append(msgspec.convert({**_row(r), "gap": gap, "detail": detail}, MdEvent))
    return out


def _gap_row(conn: Connection, gap_id: str) -> dict[str, Any] | None:
    r = conn.execute(select(gaps).where(gaps.c.gap_id == gap_id)).mappings().first()
    return _row(r) if r is not None else None


def load_open_gaps(conn: Connection) -> list[GapRecord]:
    rows = conn.execute(select(gaps).where(gaps.c.status == str(GapStatus.OPEN))).mappings()
    return [GapRecord(msgspec.convert(_row(r), GapInfo), r["trading_day"], int(r["opened_ns"]), r["closed_ns"]) for r in rows]


def load_down_state(conn: Connection, trading_day: str) -> dict[str, bool]:
    """Per instrument: is its last MD_DOWN / MD_RECOVERED of ``trading_day`` an MD_DOWN (open outage)?"""
    t = md_events
    rows = conn.execute(
        select(t.c.instrument_id, t.c.kind)
        .where(t.c.trading_day == trading_day, t.c.kind.in_([str(MdEventKind.MD_DOWN), str(MdEventKind.MD_RECOVERED)]), t.c.instrument_id.is_not(None))
        .order_by(t.c.ts_ns, t.c.event_id)
    )
    return {str(iid): kind == str(MdEventKind.MD_DOWN) for iid, kind in rows}


def load_manual_pins(conn: Connection) -> dict[SourceId, dict[str, Any]]:
    """Sources whose last manual ``md.source.set`` (a ``SOURCE_STATE`` event with an actor) was EXCLUDE."""
    t = md_events
    rows = conn.execute(
        select(t.c.source, t.c.detail, t.c.actor, t.c.ts_ns)
        .where(t.c.kind == str(MdEventKind.SOURCE_STATE), t.c.actor.is_not(None), t.c.source.is_not(None))
        .order_by(t.c.ts_ns, t.c.event_id)
    ).mappings()
    last: dict[SourceId, dict[str, Any]] = {}
    for r in rows:
        d = json.loads(r["detail"] or "{}")
        if d.get("manual"):
            last[SourceId(r["source"])] = {"action": d.get("action"), "actor": r["actor"], "reason": d.get("reason"), "ts_ns": int(r["ts_ns"])}
    return {src: d for src, d in last.items() if d.get("action") == "EXCLUDE"}


# ---------------------------------------------------------------- settle inputs
def latest_settle_input(conn: Connection, trading_day: str, instrument_id: str) -> SettleInput | None:
    t = settle_inputs
    q = select(t).where(t.c.trading_day == trading_day, t.c.instrument_id == instrument_id).order_by(t.c.version.desc()).limit(1)
    r = conn.execute(q).mappings().first()
    if r is None:
        return None
    return msgspec.convert({**_row(r), "hourly": json.loads(r["hourly"]) if r["hourly"] else None}, SettleInput)


def same_settle_values(a: SettleInput, b: SettleInput) -> bool:
    """Equal content ignoring ``version`` / ``computed_ns`` (no new version needed)."""
    return msgspec.structs.replace(a, version=0, computed_ns=0) == msgspec.structs.replace(b, version=0, computed_ns=0)


def insert_settle_input(conn: Connection, item: SettleInput) -> None:
    row = _fields(item)
    row["hourly"] = json.dumps(item.hourly) if item.hourly is not None else None
    conn.execute(settle_inputs.insert().values(**row))


# ---------------------------------------------------------------- retention (03 §8)
def purge_events_before(conn: Connection, trading_day: str, limit: int = 5000) -> int:
    """Delete up to ``limit`` md_events of trading days before ``trading_day`` (then orphan closed gaps)."""
    t = md_events
    # manual md.source.set events are the audit trail and the persisted EXCLUDE pin: never purged
    manual = and_(t.c.kind == str(MdEventKind.SOURCE_STATE), t.c.actor.is_not(None))
    ids = select(t.c.event_id).where(t.c.trading_day < trading_day, ~manual).limit(limit)
    n = int(conn.execute(delete(t).where(t.c.event_id.in_(ids))).rowcount or 0)
    if n < limit:
        referenced = select(t.c.gap_id).where(t.c.gap_id.is_not(None))
        conn.execute(delete(gaps).where(gaps.c.trading_day < trading_day, gaps.c.status != str(GapStatus.OPEN), gaps.c.gap_id.not_in(referenced)))
    return n


__all__ = [
    "FeedCursor",
    "insert_event",
    "insert_settle_input",
    "latest_settle_input",
    "load_down_state",
    "load_feed_cursors",
    "load_manual_pins",
    "load_open_gaps",
    "load_watermarks",
    "purge_events_before",
    "query_events",
    "same_settle_values",
    "save_feed_cursor",
    "save_watermarks",
    "upsert_gap",
]
