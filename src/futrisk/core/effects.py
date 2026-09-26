"""Effect model of the EngineCore (02 §10, §8.1; 03 §4.3, DB-03).

The EngineCore never performs I/O. Every public engine method returns an
ordered ``list[Effect]`` that the risk-engine shell (or the backtest loop)
executes **in this order** (02 §8.1 "执行 Effect 的顺序"):

1. :class:`Persist` - at most one per engine call: the business rows and the
   outbox (FACT) messages of that call, committed in **one** transaction. The
   shell waits for the commit (fsync first when ``critical``) before doing
   anything else;
2. external actions - :class:`Route`, :class:`CancelRoute`,
   :class:`ResolveRoute` and :class:`Request` (outbound sub-requests), sent
   asynchronously; their results come back through ``on_route_ack``,
   ``on_cancel_result``, ``on_resolve_result`` and ``on_request_result``;
3. :class:`Publish` - LV messages (``risk.snapshot.*``, ``risk.firm``) handed to
   the publisher thread; outbox messages are published by the outbox sink
   after the commit, so they are *not* repeated here;
4. :class:`ReplyTo` - the answer to the command that caused the call.

Rows are plain column dicts whose keys are the column names of 03 §4.3;
:class:`RowOp` tells the shell which statement to generate, ``key`` names the
conflict / match columns. JSON columns carry Python lists/dicts, money and
prices are ``DecStr`` strings, timestamps are UTC ns integers.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

import msgspec

from futrisk.messages.base import TIME_FIELDS, Msg
from futrisk.messages.base import request_hash as _request_hash
from futrisk.messages.envelope import Reply
from futrisk.messages.orders import RouteOrder


class RowOp(StrEnum):
    """Statement the shell generates for a :class:`Row`."""

    INSERT = "INSERT"  # plain insert; append-only tables with autoincrement ids
    INSERT_IGNORE = "INSERT_IGNORE"  # INSERT ... ON CONFLICT(key) DO NOTHING (idempotent facts)
    UPSERT = "UPSERT"  # INSERT ... ON CONFLICT(key) DO UPDATE SET <all other given columns>
    UPDATE = "UPDATE"  # UPDATE table SET <non-key columns> WHERE <key columns> = values
    DELETE = "DELETE"  # DELETE FROM table WHERE <key columns> = values (partial keys allowed)


@dataclass(frozen=True, slots=True)
class Row:
    """One statement on one risk-{shard}.db table (03 §4.3)."""

    table: str
    op: RowOp
    key: tuple[str, ...]
    values: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class OutboxMsg:
    """A FACT message written to ``outbox`` in the same transaction (03 §4.0, DB-05).

    ``ts_ns`` is the engine time of the event; the shell should use it as
    ``outbox.created_ns`` so that live and backtest envelopes are identical.
    """

    topic: str
    payload: Msg
    corr_id: str | None = None
    td: str | None = None
    ts_ns: int = 0


@dataclass(frozen=True, slots=True)
class Persist:
    """One transaction: business rows + outbox messages (02 §8.1 step 1).

    ``critical`` asks for ``synchronous=FULL`` on this commit (03 DB-03): latch
    and reset, manual halt, kill switch / firm controls, cash, EOD freeze /
    roll / provisional, recon breaks, liquidation plans and manual commands.
    ``label`` names the transaction for logs and metrics.
    """

    rows: tuple[Row, ...] = ()
    outbox: tuple[OutboxMsg, ...] = ()
    critical: bool = False
    label: str = ""

    def rows_by_table(self) -> dict[str, list[Row]]:
        """Rows grouped by table, keeping their relative order."""
        out: dict[str, list[Row]] = {}
        for r in self.rows:
            out.setdefault(r.table, []).append(r)
        return out

    def tables(self) -> list[str]:
        return sorted({r.table for r in self.rows})


@dataclass(frozen=True, slots=True)
class Route:
    """Send ``route.order`` to the trade-gateway (retry with the same id until ``expire_ns``)."""

    order: RouteOrder


@dataclass(frozen=True, slots=True)
class CancelRoute:
    """Send ``route.cancel{command_id, client_order_id, reason}`` (02 §6.4)."""

    client_order_id: str
    command_id: str
    reason: str


@dataclass(frozen=True, slots=True)
class ResolveRoute:
    """Send ``route.resolve{client_order_id, trading_day}`` for an ``UNKNOWN`` order (02 §8.3)."""

    client_order_id: str
    trading_day: str


@dataclass(frozen=True, slots=True)
class Request:
    """Outbound asynchronous sub-request executed by the shell (02 §8.1 "对外请求一律异步").

    ``target`` is the callee instance or service (``clearing-worker``,
    ``trade-gateway``, ``md-gateway``); the result - or a ``TIMEOUT`` /
    ``UNAVAILABLE`` error Reply built by the shell - must be passed back to
    ``EngineCore.on_request_result(token, reply)``.
    """

    token: str
    target: str
    command: str
    payload: Msg
    timeout_ms: int = 2000


@dataclass(frozen=True, slots=True)
class Publish:
    """Publish an LV message (not persisted; 02 §4.5)."""

    topic: str
    payload: Msg


@dataclass(frozen=True, slots=True)
class ReplyTo:
    """Answer a command; ``token`` is the opaque request context given in ``CommandCtx``."""

    token: Any
    reply: Reply


Effect = Persist | Route | CancelRoute | ResolveRoute | Request | Publish | ReplyTo

# 02 §8.1 execution phases: persist -> external actions -> publish -> reply
_PHASE: dict[type[Any], int] = {
    Persist: 0,
    Route: 1,
    CancelRoute: 1,
    ResolveRoute: 1,
    Request: 1,
    Publish: 2,
    ReplyTo: 3,
}


def effect_phase(effect: Effect) -> int:
    """0 persist, 1 external action, 2 publish, 3 reply."""
    return _PHASE[type(effect)]


def is_ordered(effects: Iterable[Effect]) -> bool:
    """True when ``effects`` respect the 02 §8.1 order (used by tests and the shell's asserts)."""
    last = -1
    for e in effects:
        p = effect_phase(e)
        if p < last:
            return False
        last = p
    return True


@dataclass(frozen=True, slots=True)
class CommandCtx:
    """Context of one command handed to :meth:`EngineCore.handle`.

    ``token`` is opaque to the core (the shell's request context: ROUTER
    identity + ``corr_id``) and comes back in :class:`ReplyTo`. ``requester`` is
    the caller's DEALER identity (``processed_commands.requester``).
    """

    command: str
    token: Any = None
    requester: str = ""
    received_ns: int = 0


@dataclass
class Tx:
    """Builder for the effects of one engine call (one transaction, 02 §8.1)."""

    label: str = ""
    rows: list[Row] = field(default_factory=list)
    outbox: list[OutboxMsg] = field(default_factory=list)
    critical: bool = False
    actions: list[Route | CancelRoute | ResolveRoute | Request] = field(default_factory=list)
    publishes: list[Publish] = field(default_factory=list)
    replies: list[ReplyTo] = field(default_factory=list)

    # ------------------------------------------------------------- persist
    def row(self, table: str, op: RowOp, key: tuple[str, ...], values: Mapping[str, Any]) -> None:
        self.rows.append(Row(table, op, key, dict(values)))

    def insert(self, table: str, values: Mapping[str, Any]) -> None:
        self.row(table, RowOp.INSERT, (), values)

    def insert_ignore(self, table: str, key: tuple[str, ...], values: Mapping[str, Any]) -> None:
        self.row(table, RowOp.INSERT_IGNORE, key, values)

    def upsert(self, table: str, key: tuple[str, ...], values: Mapping[str, Any]) -> None:
        self.row(table, RowOp.UPSERT, key, values)

    def update(self, table: str, key: tuple[str, ...], values: Mapping[str, Any]) -> None:
        self.row(table, RowOp.UPDATE, key, values)

    def delete(self, table: str, key: tuple[str, ...], values: Mapping[str, Any]) -> None:
        self.row(table, RowOp.DELETE, key, values)

    def emit(self, topic: str, payload: Msg, *, ts_ns: int, corr_id: str | None = None, td: str | None = None) -> None:
        self.outbox.append(OutboxMsg(topic, payload, corr_id, td, ts_ns))

    def mark_critical(self) -> None:
        self.critical = True

    # ----------------------------------------------------------- actions
    def route(self, order: RouteOrder) -> None:
        self.actions.append(Route(order))

    def cancel(self, client_order_id: str, command_id: str, reason: str) -> None:
        self.actions.append(CancelRoute(client_order_id, command_id, reason))

    def resolve(self, client_order_id: str, trading_day: str) -> None:
        self.actions.append(ResolveRoute(client_order_id, trading_day))

    def request(self, token: str, target: str, command: str, payload: Msg, timeout_ms: int = 2000) -> None:
        self.actions.append(Request(token, target, command, payload, timeout_ms))

    def publish(self, topic: str, payload: Msg) -> None:
        self.publishes.append(Publish(topic, payload))

    def reply(self, token: Any, reply: Reply) -> None:
        self.replies.append(ReplyTo(token, reply))

    # ------------------------------------------------------------- result
    @property
    def empty(self) -> bool:
        return not (self.rows or self.outbox or self.actions or self.publishes or self.replies)

    def effects(self) -> list[Effect]:
        out: list[Effect] = []
        if self.rows or self.outbox:
            out.append(Persist(tuple(self.rows), tuple(self.outbox), self.critical, self.label))
        out.extend(self.actions)
        out.extend(self.publishes)
        out.extend(self.replies)
        return out


def merge_effects(*groups: Iterable[Effect], label: str = "") -> list[Effect]:
    """Merge effect lists into one ordered list with a single :class:`Persist`."""
    tx = Tx(label=label)
    for g in groups:
        for e in g:
            if isinstance(e, Persist):
                tx.rows.extend(e.rows)
                tx.outbox.extend(e.outbox)
                tx.critical = tx.critical or e.critical
                if not tx.label:
                    tx.label = e.label
            elif isinstance(e, Publish):
                tx.publishes.append(e)
            elif isinstance(e, ReplyTo):
                tx.replies.append(e)
            else:
                tx.actions.append(e)
    return tx.effects()


# --------------------------------------------------------------- hashing
_TIME_FIELDS = TIME_FIELDS


def order_request_hash(payload: Msg) -> str:
    """Fast SHA-256 of a request's business fields for ``orders.request_hash`` (03 §4.3).

    Canonical msgpack of the struct with the time fields zeroed (msgspec encodes
    struct fields in declaration order and omits defaults, so the bytes are
    stable). Only the engine compares these hashes; commands stored in
    ``processed_commands`` use :func:`request_hash`, the algorithm shared with
    ``futrisk.db.processed``.
    """
    fields = {f: 0 for f in getattr(payload, "__struct_fields__", ()) if f in _TIME_FIELDS}
    body = msgspec.structs.replace(payload, **fields) if fields else payload
    return hashlib.sha256(msgspec.msgpack.encode(body)).hexdigest()


def request_hash(payload: Msg) -> str:
    """SHA-256 of the business fields of a request (03 §4.0 ``request_hash``).

    The one algorithm of the system, :func:`futrisk.messages.base.request_hash`
    (also used by ``futrisk.db.processed``): the request's own time fields are
    ignored at the top level, nested data is hashed as it is.
    """
    return _request_hash(payload)


__all__ = [
    "CancelRoute",
    "CommandCtx",
    "Effect",
    "OutboxMsg",
    "Persist",
    "Publish",
    "ReplyTo",
    "Request",
    "ResolveRoute",
    "Route",
    "Row",
    "RowOp",
    "Tx",
    "effect_phase",
    "is_ordered",
    "merge_effects",
    "order_request_hash",
    "request_hash",
]
