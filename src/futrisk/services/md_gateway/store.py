"""md.db access functions (03 §4.1 tables, §7 transactions).

All functions take the caller's :class:`sqlalchemy.Connection`: writes run inside a
writer job of :class:`futrisk.db.Database` (one transaction per job, 03 §7), reads on
a ``query_only`` connection.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Any

import msgspec
from sqlalchemy import Connection, and_, delete, select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from futrisk.messages.enums import GapStatus, MdEventKind, SourceId
from futrisk.messages.md import Bar, GapInfo, MdEvent, SettleInput
from futrisk.services.md_gateway.events import GapRecord
from futrisk.services.md_gateway.model import OrderKey
from futrisk.services.md_gateway.schema import (
    bars_1m,
    feed_cursors,
    gaps,
    md_events,
    settle_inputs,
    watermarks,
)


@dataclass(frozen=True)
class FeedCursor:
    """A row of ``feed_cursors``."""

    source: SourceId
    epoch: str | None
    last_contiguous_seq: int | None
    last_exch_ts_ns: int | None
    trading_day: str
    updated_ns: int


# ---------------------------------------------------------------- cursors / watermarks
def load_feed_cursors(conn: Connection) -> dict[SourceId, FeedCursor]:
    out: dict[SourceId, FeedCursor] = {}
    for r in conn.execute(select(feed_cursors)).mappings():
        src = SourceId(r["source"])
        out[src] = FeedCursor(
            src, r["epoch"], r["last_contiguous_seq"], r["last_exch_ts_ns"], r["trading_day"], r["updated_ns"]
        )
    return out


def save_feed_cursor(conn: Connection, cur: FeedCursor) -> None:
    values = {
        "source": str(cur.source),
        "epoch": cur.epoch,
        "last_contiguous_seq": cur.last_contiguous_seq,
        "last_exch_ts_ns": cur.last_exch_ts_ns,
        "trading_day": cur.trading_day,
        "updated_ns": cur.updated_ns,
    }
    stmt = sqlite_insert(feed_cursors).values(**values)
    conn.execute(stmt.on_conflict_do_update(index_elements=["source"], set_={k: v for k, v in values.items() if k != "source"}))


def _fp_to_db(fp: int | None) -> int | None:
    """Fingerprints are unsigned 64-bit; SQLite INTEGER is signed: store the two's complement."""
    if fp is None:
        return None
    return fp - (1 << 64) if fp >= (1 << 63) else fp


def _fp_from_db(v: int | None) -> int | None:
    if v is None:
        return None
    v = int(v)
    return v + (1 << 64) if v < 0 else v


def load_watermarks(conn: Connection) -> dict[str, tuple[OrderKey, int | None]]:
    """``instrument -> (order_key, fingerprint)`` of the last LIVE states (03 §4.1, L1)."""
    out: dict[str, tuple[OrderKey, int | None]] = {}
    for r in conn.execute(select(watermarks)).mappings():
        key = (r["trading_day"], int(r["wm_volume"]), int(r["wm_exch_ts_ns"]))
        out[r["instrument_id"]] = (key, _fp_from_db(r.get("wm_fingerprint")))
    return out


def save_watermarks(conn: Connection, rows: Sequence[tuple[str, OrderKey | None, int | None]], updated_ns: int) -> None:
    """Upsert watermarks; a watermark rolled back to nothing deletes its row."""
    for iid, key, fp in rows:
        if key is None:
            conn.execute(delete(watermarks).where(watermarks.c.instrument_id == iid))
            continue
        td, vol, ts = key
        values = {"trading_day": td, "wm_volume": vol, "wm_exch_ts_ns": ts, "updated_ns": updated_ns, "wm_fingerprint": _fp_to_db(fp)}
        stmt = sqlite_insert(watermarks).values(instrument_id=iid, **values)
        conn.execute(stmt.on_conflict_do_update(index_elements=["instrument_id"], set_=values))


# ---------------------------------------------------------------- events / gaps
def upsert_gap(conn: Connection, rec: GapRecord) -> None:
    g = rec.info
    values = {
        "gap_id": g.gap_id,
        "source": str(g.source) if g.source is not None else None,
        "epoch": g.epoch,
        "from_seq": g.from_seq,
        "to_seq": g.to_seq,
        "from_ns": g.from_ns,
        "to_ns": g.to_ns,
        "trading_day": rec.trading_day,
        "status": str(g.status),
        "filled": g.filled,
        "duplicates": g.duplicates,
        "unrecoverable": g.unrecoverable,
        "opened_ns": rec.opened_ns,
        "closed_ns": rec.closed_ns,
    }
    stmt = sqlite_insert(gaps).values(**values)
    conn.execute(stmt.on_conflict_do_update(index_elements=["gap_id"], set_={k: v for k, v in values.items() if k != "gap_id"}))


def insert_event(conn: Connection, ev: MdEvent, *, actor: str | None = None) -> None:
    """``md_events`` row; the gap snapshot at event time is kept in ``detail.gap`` for ``md.events``."""
    detail = dict(ev.detail)
    if ev.gap is not None:
        detail["gap"] = msgspec.to_builtins(ev.gap)
    conn.execute(
        md_events.insert().values(
            event_id=ev.event_id,
            kind=str(ev.kind),
            trading_day=ev.trading_day,
            ts_ns=ev.ts_ns,
            source=str(ev.source) if ev.source is not None else None,
            instrument_id=ev.instrument_id,
            from_state=ev.from_state,
            to_state=ev.to_state,
            gap_id=ev.gap.gap_id if ev.gap is not None else None,
            actor=actor,
            detail=json.dumps(detail, ensure_ascii=False, default=str),
        )
    )


def query_events(
    conn: Connection, trading_day: str, kinds: Iterable[MdEventKind] | None = None, instrument_id: str | None = None
) -> list[MdEvent]:
    t = md_events
    cond = [t.c.trading_day == trading_day]
    kind_list = [str(k) for k in kinds] if kinds else None
    if kind_list:
        cond.append(t.c.kind.in_(kind_list))
    if instrument_id is not None:
        cond.append(t.c.instrument_id == instrument_id)
    rows = conn.execute(select(t).where(and_(*cond)).order_by(t.c.ts_ns, t.c.event_id)).mappings()
    out: list[MdEvent] = []
    for r in rows:
        detail: dict[str, Any] = json.loads(r["detail"] or "{}")
        gap_raw = detail.pop("gap", None)
        gap = msgspec.convert(gap_raw, GapInfo) if gap_raw else None
        if gap is None and r["gap_id"]:
            gap = _gap_info(conn, r["gap_id"])
        if r["actor"]:
            detail.setdefault("actor", r["actor"])
        out.append(
            MdEvent(
                event_id=r["event_id"],
                kind=MdEventKind(r["kind"]),
                ts_ns=int(r["ts_ns"]),
                trading_day=r["trading_day"],
                source=SourceId(r["source"]) if r["source"] else None,
                instrument_id=r["instrument_id"],
                from_state=r["from_state"],
                to_state=r["to_state"],
                gap=gap,
                detail=detail,
            )
        )
    return out


def _gap_info(conn: Connection, gap_id: str) -> GapInfo | None:
    r = conn.execute(select(gaps).where(gaps.c.gap_id == gap_id)).mappings().first()
    if r is None:
        return None
    return GapInfo(
        gap_id=r["gap_id"],
        source=SourceId(r["source"]) if r["source"] else None,
        epoch=r["epoch"],
        from_seq=r["from_seq"],
        to_seq=r["to_seq"],
        from_ns=int(r["from_ns"]),
        to_ns=r["to_ns"],
        status=GapStatus(r["status"]),
        filled=int(r["filled"]),
        duplicates=int(r["duplicates"]),
        unrecoverable=int(r["unrecoverable"]),
    )


def load_open_gaps(conn: Connection) -> list[GapRecord]:
    rows = conn.execute(select(gaps).where(gaps.c.status == str(GapStatus.OPEN))).mappings()
    out = []
    for r in rows:
        info = _gap_info(conn, r["gap_id"])
        if info is not None:
            out.append(GapRecord(info, r["trading_day"], int(r["opened_ns"]), r["closed_ns"]))
    return out


def load_down_state(conn: Connection, trading_day: str) -> dict[str, bool]:
    """Per instrument: is its last MD_DOWN / MD_RECOVERED of ``trading_day`` an MD_DOWN (open outage)?"""
    t = md_events
    rows = conn.execute(
        select(t.c.instrument_id, t.c.kind)
        .where(
            t.c.trading_day == trading_day,
            t.c.kind.in_([str(MdEventKind.MD_DOWN), str(MdEventKind.MD_RECOVERED)]),
            t.c.instrument_id.is_not(None),
        )
        .order_by(t.c.ts_ns, t.c.event_id)
    )
    state: dict[str, bool] = {}
    for iid, kind in rows:
        state[str(iid)] = kind == str(MdEventKind.MD_DOWN)
    return state


def load_manual_pins(conn: Connection) -> dict[SourceId, dict[str, Any]]:
    """Sources whose last manual ``md.source.set`` (a ``SOURCE_STATE`` event with an actor) was EXCLUDE (M4)."""
    t = md_events
    rows = conn.execute(
        select(t.c.source, t.c.detail, t.c.actor, t.c.ts_ns)
        .where(t.c.kind == str(MdEventKind.SOURCE_STATE), t.c.actor.is_not(None), t.c.source.is_not(None))
        .order_by(t.c.ts_ns, t.c.event_id)
    ).mappings()
    last: dict[SourceId, dict[str, Any]] = {}
    for r in rows:
        d = json.loads(r["detail"] or "{}")
        if not d.get("manual"):
            continue
        last[SourceId(r["source"])] = {"action": d.get("action"), "actor": r["actor"], "reason": d.get("reason"), "ts_ns": int(r["ts_ns"])}
    return {src: d for src, d in last.items() if d.get("action") == "EXCLUDE"}


# ---------------------------------------------------------------- bars
def upsert_bars(conn: Connection, bars: Sequence[Bar], updated_ns: int) -> None:
    for b in bars:
        values = {
            "instrument_id": b.instrument_id,
            "minute_start_ns": b.minute_start_ns,
            "trading_day": b.trading_day,
            "open": b.open,
            "high": b.high,
            "low": b.low,
            "close": b.close,
            "volume": b.volume,
            "turnover": b.turnover,
            "open_interest": b.open_interest,
            "quality": str(b.quality),
            "version": b.version,
            "updated_ns": updated_ns,
        }
        stmt = sqlite_insert(bars_1m).values(**values)
        conn.execute(
            stmt.on_conflict_do_update(
                index_elements=["instrument_id", "minute_start_ns"],
                set_={k: v for k, v in values.items() if k not in ("instrument_id", "minute_start_ns")},
            )
        )


def load_bars(
    conn: Connection,
    trading_day: str,
    instrument_id: str | None = None,
    from_ns: int | None = None,
    to_ns: int | None = None,
) -> list[Bar]:
    t = bars_1m
    cond = [t.c.trading_day == trading_day]
    if instrument_id is not None:
        cond.append(t.c.instrument_id == instrument_id)
    if from_ns is not None:
        cond.append(t.c.minute_start_ns >= from_ns)
    if to_ns is not None:
        cond.append(t.c.minute_start_ns < to_ns)
    rows = conn.execute(select(t).where(and_(*cond)).order_by(t.c.instrument_id, t.c.minute_start_ns)).mappings()
    return [
        Bar(
            instrument_id=r["instrument_id"],
            trading_day=r["trading_day"],
            minute_start_ns=int(r["minute_start_ns"]),
            open=float(r["open"]),
            high=float(r["high"]),
            low=float(r["low"]),
            close=float(r["close"]),
            volume=int(r["volume"]),
            turnover=float(r["turnover"]),
            open_interest=int(r["open_interest"]),
            quality=r["quality"],
            version=int(r["version"]),
        )
        for r in rows
    ]


# ---------------------------------------------------------------- settle inputs
_SETTLE_FIELDS = (
    "exchange",
    "method",
    "multiplier",
    "tick_size",
    "day_volume",
    "day_turnover",
    "window_start_ns",
    "window_volume",
    "window_turnover",
    "hourly",
    "vwap_raw",
    "last",
    "bid1",
    "ask1",
    "limit_lock",
    "pre_settle",
    "quality",
)


def latest_settle_input(conn: Connection, trading_day: str, instrument_id: str) -> SettleInput | None:
    t = settle_inputs
    r = (
        conn.execute(
            select(t)
            .where(t.c.trading_day == trading_day, t.c.instrument_id == instrument_id)
            .order_by(t.c.version.desc())
            .limit(1)
        )
        .mappings()
        .first()
    )
    if r is None:
        return None
    return SettleInput(
        trading_day=r["trading_day"],
        instrument_id=r["instrument_id"],
        exchange=r["exchange"],
        method=r["method"],
        multiplier=int(r["multiplier"]),
        tick_size=r["tick_size"],
        day_volume=int(r["day_volume"]),
        day_turnover=r["day_turnover"],
        window_start_ns=r["window_start_ns"],
        window_volume=r["window_volume"],
        window_turnover=r["window_turnover"],
        hourly=json.loads(r["hourly"]) if r["hourly"] else None,
        vwap_raw=r["vwap_raw"],
        last=r["last"],
        bid1=r["bid1"],
        ask1=r["ask1"],
        limit_lock=r["limit_lock"],
        pre_settle=r["pre_settle"],
        quality=r["quality"],
        version=int(r["version"]),
        computed_ns=int(r["computed_ns"]),
    )


def same_settle_values(a: SettleInput, b: SettleInput) -> bool:
    """Equal content ignoring ``version`` / ``computed_ns`` (no new version needed)."""
    return all(_norm(getattr(a, f)) == _norm(getattr(b, f)) for f in _SETTLE_FIELDS)


def _norm(v: Any) -> Any:
    if isinstance(v, dict):
        return json.dumps(v, sort_keys=True)
    return str(v) if v is not None else None


def insert_settle_input(conn: Connection, item: SettleInput) -> None:
    conn.execute(
        settle_inputs.insert().values(
            trading_day=item.trading_day,
            instrument_id=item.instrument_id,
            version=item.version,
            exchange=str(item.exchange),
            method=str(item.method),
            multiplier=item.multiplier,
            tick_size=item.tick_size,
            day_volume=item.day_volume,
            day_turnover=item.day_turnover,
            window_start_ns=item.window_start_ns,
            window_volume=item.window_volume,
            window_turnover=item.window_turnover,
            hourly=json.dumps(item.hourly) if item.hourly is not None else None,
            vwap_raw=item.vwap_raw,
            last=item.last,
            bid1=item.bid1,
            ask1=item.ask1,
            limit_lock=str(item.limit_lock),
            pre_settle=item.pre_settle,
            quality=str(item.quality),
            computed_ns=item.computed_ns,
        )
    )


# ---------------------------------------------------------------- retention (03 §8)
def purge_events_before(conn: Connection, trading_day: str, limit: int = 5000) -> int:
    """Delete up to ``limit`` md_events of trading days before ``trading_day`` (then orphan closed gaps)."""
    t = md_events
    # manual md.source.set events are the audit trail and the persisted EXCLUDE pin (M4): never purged
    manual = and_(t.c.kind == str(MdEventKind.SOURCE_STATE), t.c.actor.is_not(None))
    ids = select(t.c.event_id).where(t.c.trading_day < trading_day, ~manual).limit(limit)
    n = int(conn.execute(delete(t).where(t.c.event_id.in_(ids))).rowcount or 0)
    if n < limit:
        referenced = select(t.c.gap_id).where(t.c.gap_id.is_not(None))
        conn.execute(
            delete(gaps).where(
                gaps.c.trading_day < trading_day,
                gaps.c.status != str(GapStatus.OPEN),
                gaps.c.gap_id.not_in(referenced),
            )
        )
    return n


__all__ = [
    "FeedCursor",
    "insert_event",
    "insert_settle_input",
    "latest_settle_input",
    "load_bars",
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
    "upsert_bars",
    "upsert_gap",
]
