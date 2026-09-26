"""td.db data access used inside writer jobs and read transactions (03 §4.2, §7).

Every function takes the caller's :class:`sqlalchemy.Connection`; transaction scope is
decided by the caller (03 §7: a broker report, its ``broker_order_events`` row, the
``routed_orders`` update, the ``trades`` row, the ``broker_cursors`` update and the
``td.exec`` outbox row are written in ONE transaction).
"""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from sqlalchemy import Connection, and_, delete, func, insert, select, update

from futrisk.common.decimals import D, dec_str
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
from futrisk.messages.orders import BrokerOrderUpdate, RouteOrder, Trade
from futrisk.services.trade_gateway.schema import (
    TERMINAL_STATUSES,
    broker_cursors,
    broker_order_events,
    firm_controls,
    route_tombstones,
    routed_orders,
    trades,
)


def price_str(price: float | Decimal | str | None) -> str | None:
    """DEC_PRICE storage form: ``Decimal(str(x))`` of the broker's double (02 §4.3)."""
    return None if price is None else dec_str(D(price))


def price_float(price: str | None) -> float | None:
    return None if price is None else float(Decimal(price))


# ------------------------------------------------------------------ routed orders
@dataclass(frozen=True, slots=True)
class RoutedOrder:
    """One ``routed_orders`` row."""

    client_order_id: str
    account_id: str
    instrument_id: str
    exchange: Exchange
    direction: Direction
    offset: Offset
    order_type: OrderType
    tif: TimeInForce
    price: str | None
    qty: int
    trading_day: str
    src_instance: str
    expire_ns: int
    broker: str
    broker_order_ref: str | None
    order_sys_id: str | None
    status: BrokerOrderStatus
    filled_qty: int
    broker_code: str | None
    broker_msg: str | None
    received_ns: int
    submitted_ns: int | None
    updated_ns: int
    cancel_requested_ns: int | None = None

    @property
    def remaining_qty(self) -> int:
        return max(0, self.qty - self.filled_qty)

    @property
    def is_terminal(self) -> bool:
        return self.status in TERMINAL_STATUSES

    @property
    def cancel_pending(self) -> bool:
        """A ``route.cancel`` was accepted and the broker has not finished the order yet."""
        return self.cancel_requested_ns is not None and not self.is_terminal

    def to_route_order(self) -> RouteOrder:
        """The original route (resubmission after a resync)."""
        return RouteOrder(
            client_order_id=self.client_order_id,
            account_id=self.account_id,
            instrument_id=self.instrument_id,
            exchange=self.exchange,
            direction=self.direction,
            offset=self.offset,
            order_type=self.order_type,
            tif=self.tif,
            price=price_float(self.price),
            qty=self.qty,
            trading_day=self.trading_day,
            risk_passed_ns=0,
            expire_ns=self.expire_ns,
            src_instance=self.src_instance,
        )

    def update_msg(
        self,
        *,
        status: BrokerOrderStatus | None = None,
        broker_code: str | None = None,
        broker_msg: str | None = None,
        broker_seq: int = 0,
        updated_ns: int,
    ) -> BrokerOrderUpdate:
        """A ``BrokerOrderUpdate`` stating this order's (new) status.

        Used for trade-gateway-originated changes that no broker report carries
        (local cancel of a held order, expiry of a held order, orders lost in a
        disconnect, a refused cancel): ``broker_seq`` is 0 unless it restates the last
        broker event of the order.
        """
        return BrokerOrderUpdate(
            client_order_id=self.client_order_id,
            origin=TradeOrigin.INTERNAL,
            account_id=self.account_id,
            instrument_id=self.instrument_id,
            broker_order_ref=self.broker_order_ref or "",
            order_sys_id=self.order_sys_id,
            status=status or self.status,
            filled_qty=self.filled_qty,
            remaining_qty=self.remaining_qty,
            broker_code=broker_code,
            broker_msg=broker_msg,
            broker_seq=broker_seq,
            trading_day=self.trading_day,
            updated_ns=updated_ns,
        )


def _routed_from(m: Any) -> RoutedOrder:
    return RoutedOrder(
        client_order_id=str(m["client_order_id"]),
        account_id=str(m["account_id"]),
        instrument_id=str(m["instrument_id"]),
        exchange=Exchange(m["exchange"]),
        direction=Direction(m["direction"]),
        offset=Offset(m["offset"]),
        order_type=OrderType(m["order_type"]),
        tif=TimeInForce(m["tif"]),
        price=m["price"],
        qty=int(m["qty"]),
        trading_day=str(m["trading_day"]),
        src_instance=str(m["src_instance"]),
        expire_ns=int(m["expire_ns"]),
        broker=str(m["broker"]),
        broker_order_ref=m["broker_order_ref"],
        order_sys_id=m["order_sys_id"],
        status=BrokerOrderStatus(m["status"]),
        filled_qty=int(m["filled_qty"]),
        broker_code=m["broker_code"],
        broker_msg=m["broker_msg"],
        received_ns=int(m["received_ns"]),
        submitted_ns=int(m["submitted_ns"]) if m["submitted_ns"] is not None else None,
        updated_ns=int(m["updated_ns"]),
        cancel_requested_ns=int(m["cancel_requested_ns"]) if m["cancel_requested_ns"] is not None else None,
    )


def get_routed(conn: Connection, client_order_id: str) -> RoutedOrder | None:
    t = routed_orders
    m = conn.execute(select(t).where(t.c.client_order_id == client_order_id)).mappings().first()
    return _routed_from(m) if m is not None else None


def insert_routed(
    conn: Connection,
    order: RouteOrder,
    *,
    broker: str,
    status: BrokerOrderStatus,
    now_ns: int,
    broker_code: str | None = None,
    broker_msg: str | None = None,
) -> RoutedOrder:
    """Insert a ``routed_orders`` row for a route arriving at trade-gateway."""
    values: dict[str, Any] = {
        "client_order_id": order.client_order_id,
        "account_id": order.account_id,
        "instrument_id": order.instrument_id,
        "exchange": order.exchange.value,
        "direction": order.direction.value,
        "offset": order.offset.value,
        "order_type": order.order_type.value,
        "tif": order.tif.value,
        "price": price_str(order.price),
        "qty": order.qty,
        "trading_day": order.trading_day,
        "src_instance": order.src_instance,
        "expire_ns": order.expire_ns,
        "broker": broker,
        "broker_order_ref": None,
        "order_sys_id": None,
        "status": status.value,
        "filled_qty": 0,
        "broker_code": broker_code,
        "broker_msg": broker_msg,
        "received_ns": now_ns,
        "submitted_ns": None,
        "updated_ns": now_ns,
    }
    conn.execute(insert(routed_orders).values(**values))
    row = get_routed(conn, order.client_order_id)
    assert row is not None
    return row


def insert_routed_row(conn: Connection, row: RoutedOrder) -> None:
    """Insert a fully specified row (orders reconstructed from broker reports)."""
    conn.execute(
        insert(routed_orders).values(
            client_order_id=row.client_order_id,
            account_id=row.account_id,
            instrument_id=row.instrument_id,
            exchange=row.exchange.value,
            direction=row.direction.value,
            offset=row.offset.value,
            order_type=row.order_type.value,
            tif=row.tif.value,
            price=row.price,
            qty=row.qty,
            trading_day=row.trading_day,
            src_instance=row.src_instance,
            expire_ns=row.expire_ns,
            broker=row.broker,
            broker_order_ref=row.broker_order_ref,
            order_sys_id=row.order_sys_id,
            status=row.status.value,
            filled_qty=row.filled_qty,
            broker_code=row.broker_code,
            broker_msg=row.broker_msg,
            received_ns=row.received_ns,
            submitted_ns=row.submitted_ns,
            updated_ns=row.updated_ns,
            cancel_requested_ns=row.cancel_requested_ns,
        )
    )


def update_routed(conn: Connection, client_order_id: str, **values: Any) -> None:
    """Update columns of one routed order (enum values are stored by value)."""
    clean = {k: (v.value if isinstance(v, BrokerOrderStatus) else v) for k, v in values.items()}
    conn.execute(update(routed_orders).where(routed_orders.c.client_order_id == client_order_id).values(**clean))


def routed_with_status(conn: Connection, statuses: tuple[BrokerOrderStatus, ...]) -> list[RoutedOrder]:
    t = routed_orders
    res = conn.execute(
        select(t).where(t.c.status.in_([s.value for s in statuses])).order_by(t.c.received_ns, t.c.client_order_id)
    ).mappings()
    return [_routed_from(m) for m in res]


def pending_cancels(conn: Connection) -> list[RoutedOrder]:
    """Non-terminal orders with an accepted ``route.cancel`` still to be carried out."""
    t = routed_orders
    q = (
        select(t)
        .where(t.c.cancel_requested_ns.is_not(None), t.c.status.not_in([s.value for s in TERMINAL_STATUSES]))
        .order_by(t.c.cancel_requested_ns, t.c.client_order_id)
    )
    return [_routed_from(m) for m in conn.execute(q).mappings()]


def mark_unknown(conn: Connection, now_ns: int, *, include_received: bool = False) -> list[str]:
    """Orders sent to the broker whose fate is unknown (link lost / restart) -> ``UNKNOWN``.

    ``SUBMITTED`` rows, and ``CANCEL_PENDING`` rows the broker never acknowledged
    (no ``order_sys_id``); at start-up also ``RECEIVED`` rows left by a crash between
    persisting and sending (``include_received``). Returns the affected ids.
    """
    t = routed_orders
    cond = (t.c.status == BrokerOrderStatus.SUBMITTED.value) | and_(
        t.c.status == BrokerOrderStatus.CANCEL_PENDING.value, t.c.order_sys_id.is_(None)
    )
    if include_received:
        cond = cond | (t.c.status == BrokerOrderStatus.RECEIVED.value)
    ids = [str(r[0]) for r in conn.execute(select(t.c.client_order_id).where(cond))]
    if ids:
        conn.execute(
            update(t).where(t.c.client_order_id.in_(ids)).values(status=BrokerOrderStatus.UNKNOWN.value, updated_ns=now_ns)
        )
    return ids


def count_pending(conn: Connection) -> int:
    """``BrokerStatus.pending_orders``: routed orders in ``RECEIVED`` or ``UNKNOWN`` (02 §7.5)."""
    t = routed_orders
    q = select(func.count()).select_from(t).where(
        t.c.status.in_([BrokerOrderStatus.RECEIVED.value, BrokerOrderStatus.UNKNOWN.value])
    )
    return int(conn.execute(q).scalar() or 0)


# ------------------------------------------------------------------ tombstones
def has_tombstone(conn: Connection, client_order_id: str) -> bool:
    t = route_tombstones
    return conn.execute(select(t.c.client_order_id).where(t.c.client_order_id == client_order_id)).first() is not None


def insert_tombstone(
    conn: Connection,
    client_order_id: str,
    *,
    trading_day: str,
    command: str,
    command_id: str | None,
    requested_by: str,
    now_ns: int,
) -> None:
    conn.execute(
        insert(route_tombstones).values(
            client_order_id=client_order_id,
            trading_day=trading_day,
            command=command,
            command_id=command_id,
            requested_by=requested_by,
            created_ns=now_ns,
        )
    )


# ------------------------------------------------------------------ order events
def order_event_exists(conn: Connection, trading_day: str, broker_seq: int) -> bool:
    t = broker_order_events
    q = select(t.c.event_id).where(t.c.trading_day == trading_day, t.c.broker_seq == broker_seq)
    return conn.execute(q).first() is not None


def latest_order_event(
    conn: Connection, *, client_order_id: str | None = None, broker_order_ref: str | None = None
) -> tuple[str, int, BrokerOrderStatus, int] | None:
    """``(trading_day, broker_seq, status, filled_qty)`` of the newest broker event of an order."""
    t = broker_order_events
    if client_order_id is not None:
        cond = t.c.client_order_id == client_order_id
    elif broker_order_ref is not None:
        cond = and_(t.c.client_order_id.is_(None), t.c.broker_order_ref == broker_order_ref)
    else:
        return None
    m = conn.execute(
        select(t.c.trading_day, t.c.broker_seq, t.c.status, t.c.filled_qty)
        .where(cond)
        .order_by(t.c.trading_day.desc(), t.c.broker_seq.desc())
        .limit(1)
    ).first()
    if m is None:
        return None
    return str(m[0]), int(m[1]), BrokerOrderStatus(m[2]), int(m[3])


def insert_order_event(
    conn: Connection,
    *,
    trading_day: str,
    broker_seq: int,
    client_order_id: str | None,
    broker_order_ref: str,
    origin: TradeOrigin,
    status: BrokerOrderStatus,
    filled_qty: int,
    remaining_qty: int,
    broker_code: str | None,
    broker_msg: str | None,
    detail: dict[str, Any],
    ts_ns: int,
) -> None:
    conn.execute(
        insert(broker_order_events).values(
            trading_day=trading_day,
            broker_seq=broker_seq,
            client_order_id=client_order_id,
            broker_order_ref=broker_order_ref,
            origin=origin.value,
            status=status.value,
            filled_qty=filled_qty,
            remaining_qty=remaining_qty,
            broker_code=broker_code,
            broker_msg=broker_msg,
            detail=json.dumps(detail, ensure_ascii=False, separators=(",", ":"), default=str),
            ts_ns=ts_ns,
        )
    )


def purge_order_events_before(conn: Connection, trading_day: str, *, limit: int = 5_000) -> int:
    """Delete up to ``limit`` ``broker_order_events`` of trading days before ``trading_day`` (03 §8: 90 days)."""
    t = broker_order_events
    ids = select(t.c.event_id).where(t.c.trading_day < trading_day).order_by(t.c.event_id).limit(limit)
    return int(conn.execute(delete(t).where(t.c.event_id.in_(ids))).rowcount or 0)


def last_broker_seq_by_order(conn: Connection, client_order_ids: list[str]) -> dict[str, int]:
    t = broker_order_events
    if not client_order_ids:
        return {}
    q = (
        select(t.c.client_order_id, func.max(t.c.broker_seq))
        .where(t.c.client_order_id.in_(client_order_ids))
        .group_by(t.c.client_order_id)
    )
    return {str(r[0]): int(r[1]) for r in conn.execute(q)}


# ------------------------------------------------------------------ trades
def find_trade(conn: Connection, trading_day: str, exchange: str, trade_id: str, direction: str) -> int | None:
    """``trade_seq`` of the trade with this trade key, if stored."""
    t = trades
    q = select(t.c.trade_seq).where(
        t.c.trading_day == trading_day, t.c.exchange == exchange, t.c.trade_id == trade_id, t.c.direction == direction
    )
    v = conn.execute(q).scalar()
    return int(v) if v is not None else None


def insert_trade(conn: Connection, trade: Trade, *, import_batch: str | None = None) -> int:
    """Insert a trade (``trade_seq`` allocated by AUTOINCREMENT); returns the ``trade_seq``."""
    res = conn.execute(
        insert(trades).values(
            trading_day=trade.trading_day,
            exchange=trade.exchange.value,
            trade_id=trade.trade_id,
            direction=trade.direction.value,
            account_id=trade.account_id,
            instrument_id=trade.instrument_id,
            client_order_id=trade.client_order_id,
            origin=trade.origin.value,
            import_batch=import_batch,
            broker_order_ref=trade.broker_order_ref,
            order_sys_id=trade.order_sys_id,
            offset=trade.offset.value,
            price=price_str(trade.price),
            qty=trade.qty,
            trade_ts_ns=trade.trade_ts_ns,
            broker_seq=trade.broker_seq,
            received_ns=trade.received_ns,
        )
    )
    pk = res.inserted_primary_key
    assert pk is not None
    return int(pk[0])


def _trade_from(m: Any) -> Trade:
    return Trade(
        trading_day=str(m["trading_day"]),
        exchange=Exchange(m["exchange"]),
        trade_id=str(m["trade_id"]),
        direction=Direction(m["direction"]),
        account_id=str(m["account_id"]),
        instrument_id=str(m["instrument_id"]),
        client_order_id=m["client_order_id"],
        origin=TradeOrigin(m["origin"]),
        broker_order_ref=str(m["broker_order_ref"]),
        order_sys_id=m["order_sys_id"],
        offset=Offset(m["offset"]),
        price=float(Decimal(str(m["price"]))),
        qty=int(m["qty"]),
        trade_ts_ns=int(m["trade_ts_ns"]),
        broker_seq=int(m["broker_seq"]) if m["broker_seq"] is not None else None,
        trade_seq=int(m["trade_seq"]),
        received_ns=int(m["received_ns"]),
    )


def trades_page(
    conn: Connection, trading_day: str, *, account_id: str | None, from_trade_seq: int, limit: int
) -> tuple[list[Trade], bool]:
    """Trades of a day with ``trade_seq >= from_trade_seq`` in ``trade_seq`` order; ``(page, more)``."""
    t = trades
    q = select(t).where(t.c.trading_day == trading_day, t.c.trade_seq >= from_trade_seq)
    if account_id is not None:
        q = q.where(t.c.account_id == account_id)
    rows = [_trade_from(m) for m in conn.execute(q.order_by(t.c.trade_seq).limit(limit + 1)).mappings()]
    return rows[:limit], len(rows) > limit


def trades_by_account(conn: Connection, trading_day: str) -> dict[str, list[Trade]]:
    t = trades
    out: dict[str, list[Trade]] = defaultdict(list)
    for m in conn.execute(select(t).where(t.c.trading_day == trading_day).order_by(t.c.trade_seq)).mappings():
        tr = _trade_from(m)
        out[tr.account_id].append(tr)
    return dict(out)


# ------------------------------------------------------------------ order queries (td.query.orders)
def order_updates(
    conn: Connection, trading_day: str, *, account_id: str | None, client_order_ids: list[str] | None
) -> list[BrokerOrderUpdate]:
    """Read-only ``BrokerOrderUpdate`` view of routed (and, without an id filter, external) orders."""
    t = routed_orders
    q = select(t).where(t.c.trading_day == trading_day)
    if account_id is not None:
        q = q.where(t.c.account_id == account_id)
    if client_order_ids is not None:
        q = q.where(t.c.client_order_id.in_(client_order_ids))
    rows = [_routed_from(m) for m in conn.execute(q.order_by(t.c.received_ns, t.c.client_order_id)).mappings()]
    seqs = last_broker_seq_by_order(conn, [r.client_order_id for r in rows])
    out = [
        r.update_msg(broker_code=r.broker_code, broker_msg=r.broker_msg, broker_seq=seqs.get(r.client_order_id, 0),
                     updated_ns=r.updated_ns)
        for r in rows
    ]
    if client_order_ids is None:
        out.extend(_external_updates(conn, trading_day, account_id))
    return out


def _external_updates(conn: Connection, trading_day: str, account_id: str | None) -> list[BrokerOrderUpdate]:
    e = broker_order_events
    latest = (
        select(e.c.broker_order_ref, func.max(e.c.broker_seq).label("seq"))
        .where(e.c.trading_day == trading_day, e.c.client_order_id.is_(None))
        .group_by(e.c.broker_order_ref)
        .subquery()
    )
    q = (
        select(e)
        .join(latest, and_(e.c.broker_order_ref == latest.c.broker_order_ref, e.c.broker_seq == latest.c.seq))
        .where(e.c.trading_day == trading_day)
        .order_by(e.c.broker_seq)
    )
    out: list[BrokerOrderUpdate] = []
    for m in conn.execute(q).mappings():
        detail = json.loads(m["detail"] or "{}")
        acct = str(detail.get("account_id") or "")
        if account_id is not None and acct != account_id:
            continue
        out.append(
            BrokerOrderUpdate(
                client_order_id=None,
                origin=TradeOrigin(m["origin"]),
                account_id=acct,
                instrument_id=str(detail.get("instrument_id") or ""),
                broker_order_ref=str(m["broker_order_ref"]),
                order_sys_id=detail.get("order_sys_id"),
                status=BrokerOrderStatus(m["status"]),
                filled_qty=int(m["filled_qty"]),
                remaining_qty=int(m["remaining_qty"]),
                broker_code=m["broker_code"],
                broker_msg=m["broker_msg"],
                broker_seq=int(m["broker_seq"]),
                trading_day=str(m["trading_day"]),
                updated_ns=int(m["ts_ns"]),
            )
        )
    return out


# ------------------------------------------------------------------ cursors
def load_cursors(conn: Connection, broker: str) -> dict[str, int]:
    t = broker_cursors
    return {str(r[0]): int(r[1]) for r in conn.execute(select(t.c.trading_day, t.c.last_broker_seq).where(t.c.broker == broker))}


def upsert_cursor(
    conn: Connection,
    *,
    broker: str,
    trading_day: str,
    last_broker_seq: int,
    session_id: str | None,
    front_id: int | None,
    now_ns: int,
) -> None:
    """Set ``last_broker_seq`` (never moves backwards) for ``(broker, trading_day)``."""
    t = broker_cursors
    cur = conn.execute(
        select(t.c.last_broker_seq).where(t.c.broker == broker, t.c.trading_day == trading_day)
    ).scalar()
    if cur is None:
        conn.execute(
            insert(t).values(
                broker=broker,
                trading_day=trading_day,
                last_broker_seq=last_broker_seq,
                session_id=session_id,
                front_id=front_id,
                updated_ns=now_ns,
            )
        )
        return
    values: dict[str, Any] = {"last_broker_seq": max(int(cur), last_broker_seq), "updated_ns": now_ns}
    if session_id is not None:
        values["session_id"] = session_id
        values["front_id"] = front_id
    conn.execute(update(t).where(t.c.broker == broker, t.c.trading_day == trading_day).values(**values))


# ------------------------------------------------------------------ firm controls
def insert_firm_control(
    conn: Connection,
    *,
    action: FirmAction,
    origin: ControlOrigin,
    active: bool,
    restrictions: list[dict[str, Any]],
    trading_day: str,
    actor: str | None,
    reason: str,
    cause_alert_key: str | None,
    src_instance: str,
    ts_ns: int,
) -> None:
    conn.execute(
        insert(firm_controls).values(
            action=action.value,
            origin=origin.value,
            active=1 if active else 0,
            restrictions=json.dumps(restrictions, ensure_ascii=False, separators=(",", ":")),
            trading_day=trading_day,
            actor=actor,
            reason=reason,
            cause_alert_key=cause_alert_key,
            src_instance=src_instance,
            ts_ns=ts_ns,
        )
    )


def firm_control_seen(
    conn: Connection, *, origin: ControlOrigin, action: FirmAction, ts_ns: int, src_instance: str
) -> bool:
    """True when this control (business key: origin, action, ts_ns, source) is already stored.

    Makes the ``risk.control.firm`` apply idempotent even when the FACT stream is
    replayed from seq 1 (publisher stream change, lost cursor).
    """
    t = firm_controls
    q = select(t.c.control_id).where(
        t.c.origin == origin.value, t.c.action == action.value, t.c.ts_ns == ts_ns, t.c.src_instance == src_instance
    )
    return conn.execute(q).first() is not None


#: ``firm_controls`` actions whose ``active`` column is a kill switch state: the legacy ON/OFF pair and
#: ``RULE_STATE`` (the whole RULE layer in one fact: ``active`` = rule kill switch)
KILL_STATE_ACTIONS: tuple[FirmAction, ...] = (FirmAction.KILL_SWITCH_ON, FirmAction.KILL_SWITCH_OFF, FirmAction.RULE_STATE)


def latest_kill_ts(conn: Connection, origin: ControlOrigin) -> int | None:
    """``ts_ns`` of the newest kill switch state row of ``origin`` (an older control never overrides it)."""
    t = firm_controls
    v = conn.execute(
        select(func.max(t.c.ts_ns)).where(
            t.c.origin == origin.value,
            t.c.action.in_([a.value for a in KILL_STATE_ACTIONS]),
        )
    ).scalar()
    return int(v) if v is not None else None


def load_kill_switch(conn: Connection) -> dict[ControlOrigin, bool]:
    """Per origin, ``active`` of the newest kill switch state row (ON/OFF or RULE_STATE; 03 §4.3 当前状态)."""
    t = firm_controls
    out = {ControlOrigin.MANUAL: False, ControlOrigin.RULE: False}
    kill_actions = [a.value for a in KILL_STATE_ACTIONS]
    for origin in (ControlOrigin.MANUAL, ControlOrigin.RULE):
        v = conn.execute(
            select(t.c.active)
            .where(t.c.origin == origin.value, t.c.action.in_(kill_actions))
            .order_by(t.c.control_id.desc())
            .limit(1)
        ).scalar()
        out[origin] = bool(v) if v is not None else False
    return out


__all__ = [
    "KILL_STATE_ACTIONS",
    "RoutedOrder",
    "count_pending",
    "find_trade",
    "firm_control_seen",
    "get_routed",
    "has_tombstone",
    "insert_firm_control",
    "insert_order_event",
    "insert_routed",
    "insert_routed_row",
    "insert_tombstone",
    "insert_trade",
    "last_broker_seq_by_order",
    "latest_kill_ts",
    "latest_order_event",
    "load_cursors",
    "load_kill_switch",
    "mark_unknown",
    "order_event_exists",
    "order_updates",
    "pending_cancels",
    "price_float",
    "price_str",
    "purge_order_events_before",
    "routed_with_status",
    "trades_by_account",
    "trades_page",
    "update_routed",
    "upsert_cursor",
]
