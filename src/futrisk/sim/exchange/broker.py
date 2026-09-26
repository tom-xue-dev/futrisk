"""SimBroker: CTP-like trading counter of the virtual exchange (02 §9.4).

Transport: ZeroMQ ROUTER bound on ``sim.yaml`` ``broker.endpoint``; trade-gateway connects
a DEALER (identity ``trade-gateway``). One JSON object per frame, ``msg`` names it.

Processing is strictly sequential (one asyncio worker consumes an event queue), and every
event is **persisted before its reports are sent**: ``broker_seq`` (per trading day,
across sessions, covering ``RtnOrder`` and ``RtnTrade``), ``trade_id``, ``order_sys_id``
and ``session_id`` therefore never repeat, even across a sim restart (sim.db, 03 §4.7).
``ack_delay_ms`` (request latency) and ``fill_delay_ms`` (time to the first match) are
FIFO delay lines in front of the worker, so they never block it or reorder requests.

Matching (02 §9.4 撮合语义):
- a new order is first matched *aggressively*: a marketable order fills at the opposite
  best price, at most the level-1 size (sizes consumed within one market state are
  tracked); market orders (``ANY``) too. Orders waiting for that first attempt (during
  an auction, a pause or before the first book of the day) keep the flag across a
  restart (``sim_orders.pending_match``);
- a resting GFD limit order fills at its own price once the market crosses it;
- ``IOC+AV`` (FAK) cancels the unfilled rest, ``IOC+CV`` (FOK) fills completely or not
  at all; market-order remainders are cancelled; such orders never rest;
- a limit-locked side has no liquidity (no offers at limit-up, no bids at limit-down);
- prices are validated against the band of the trading day in force at arrival (the
  contract rolls when the day's first window, auction included, opens);
- at the last session close of the order's trading day every live order is cancelled
  with ``status_msg=SESSION_CLOSE``; matching pauses with ``POST /control/pause``;
- SHFE close-today / close-yesterday quantities are checked against the counter's own
  positions net of pending closes (``BROKER_REJECT_CLOSE_TODAY`` / ``_YD``), mirroring
  the lot rules of ``core.lots`` (CLOSE on SHFE = close yesterday; other exchanges close
  in ``close_style`` order); a control-API force close first cancels the pending close
  orders on that position (``FORCE_CLOSE_CANCELED``), then creates an external
  order+trade with ``offset_flag="2"`` and no ``client_order_id``.
"""

from __future__ import annotations

import asyncio
import json
import math
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, TypeVar, cast

from loguru import logger

from futrisk.common.calendar import TradingCalendar
from futrisk.common.clock import NS_PER_MS, MarketClock, bj_to_ns, parse_td, td_str
from futrisk.common.config import FullConfig
from futrisk.messages import codes
from futrisk.sim.exchange.engine import InstrumentMeta, PriceEngine, Quote
from futrisk.sim.exchange.errors import bad_request, conflict
from futrisk.sim.exchange.formats import bj_parts, dumps_bytes
from futrisk.sim.exchange.router_io import RouterIO
from futrisk.sim.exchange.store import SimStore, StoreBatch
from futrisk.sim.exchange.tasks import DelayLine, spawn

T = TypeVar("T")

# CTP-style codes on the wire (02 §9.4)
BUY, SELL = "0", "1"
OFF_OPEN, OFF_CLOSE, OFF_FORCE, OFF_CLOSE_TODAY, OFF_CLOSE_YD = "0", "1", "2", "3", "4"
CLIENT_OFFSETS = (OFF_OPEN, OFF_CLOSE, OFF_CLOSE_TODAY, OFF_CLOSE_YD)
ST_ACCEPTED, ST_PART, ST_ALL, ST_CANCELED, ST_REJECTED = (
    "ACCEPTED", "PART_TRADED", "ALL_TRADED", "CANCELED", "REJECTED",
)
ACTIVE_STATUSES = frozenset({ST_ACCEPTED, ST_PART})
LONG, SHORT = "LONG", "SHORT"

# sim-specific reason codes (extensions of 02 §11.3; see the README)
BROKER_NOT_LOGGED_IN = "BROKER_NOT_LOGGED_IN"
BROKER_BAD_REQUEST = "BROKER_BAD_REQUEST"
BROKER_LOGIN_FAILED = "BROKER_LOGIN_FAILED"
BROKER_REJECT_INVALID = "BROKER_REJECT_INVALID"
BROKER_REJECT_POSITION = "BROKER_REJECT_POSITION"
BROKER_REJECT_SESSION_CLOSED = "BROKER_REJECT_SESSION_CLOSED"
BROKER_DUPLICATE_ORDER_REF = "BROKER_DUPLICATE_ORDER_REF"

QUERY_PAGE = 500


# ---------------------------------------------------------------------------
# Records
# ---------------------------------------------------------------------------


@dataclass
class SimOrder:
    order_sys_id: str
    trading_day: str
    front_id: int
    session_id: int
    order_ref: str
    client_order_id: str | None
    investor_id: str
    instrument_id: str
    exchange_id: str
    direction: str
    comb_offset_flag: str
    order_price_type: str
    limit_price: float
    volume_total_original: int
    volume_traded: int
    order_status: str
    status_msg: str
    time_condition: str
    volume_condition: str
    origin: str
    last_broker_seq: int
    insert_ns: int
    updated_ns: int
    limit_ticks: int | None = None
    pending_match: bool = False  # waiting for its first (aggressive) match attempt

    @property
    def seq_no(self) -> int:
        return int(self.order_sys_id)

    @property
    def remaining(self) -> int:
        return self.volume_total_original - self.volume_traded

    @property
    def active(self) -> bool:
        return self.order_status in ACTIVE_STATUSES

    @property
    def ioc(self) -> bool:
        """Must not rest: FAK/FOK, and market orders."""
        return self.time_condition == "IOC" or self.order_price_type == "ANY"

    def to_row(self) -> dict[str, Any]:
        return {
            "order_sys_id": self.order_sys_id,
            "trading_day": self.trading_day,
            "front_id": self.front_id,
            "session_id": self.session_id,
            "order_ref": self.order_ref,
            "client_order_id": self.client_order_id,
            "investor_id": self.investor_id,
            "instrument_id": self.instrument_id,
            "exchange_id": self.exchange_id,
            "direction": self.direction,
            "comb_offset_flag": self.comb_offset_flag,
            "order_price_type": self.order_price_type,
            "limit_price": self.limit_price,
            "volume_total_original": self.volume_total_original,
            "volume_traded": self.volume_traded,
            "order_status": self.order_status,
            "status_msg": self.status_msg,
            "time_condition": self.time_condition,
            "volume_condition": self.volume_condition,
            "origin": self.origin,
            "pending_match": int(self.pending_match),
            "last_broker_seq": self.last_broker_seq,
            "insert_ns": self.insert_ns,
            "updated_ns": self.updated_ns,
        }

    @classmethod
    def from_row(cls, row: dict[str, Any], meta: InstrumentMeta | None) -> SimOrder:
        o = cls(**{k: row[k] for k in cls.__dataclass_fields__ if k in row and k != "pending_match"})
        o.pending_match = bool(row.get("pending_match", 0))
        if o.active and o.ioc:
            o.pending_match = True  # FAK/FOK/market orders never rest: they are always matched aggressively
        if meta is not None and o.order_price_type == "LIMIT":
            o.limit_ticks = meta.to_ticks(o.limit_price)
        return o


@dataclass
class SimTrade:
    trade_id: str
    trading_day: str
    order_sys_id: str
    broker_seq: int
    front_id: int
    session_id: int
    order_ref: str
    client_order_id: str | None
    investor_id: str
    instrument_id: str
    exchange_id: str
    direction: str
    offset_flag: str
    price: float
    volume: int
    trade_ts_ns: int

    def to_row(self) -> dict[str, Any]:
        return dict(self.__dict__)

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> SimTrade:
        return cls(**{k: row[k] for k in cls.__dataclass_fields__})


@dataclass
class Position:
    investor_id: str
    instrument_id: str
    posi_direction: str
    today: int = 0
    yd: int = 0

    @property
    def key(self) -> tuple[str, str, str]:
        return (self.investor_id, self.instrument_id, self.posi_direction)

    @property
    def total(self) -> int:
        return self.today + self.yd


@dataclass
class Session:
    identity: bytes
    front_id: int
    session_id: int
    broker_id: str
    user_id: str
    app_id: str
    login_ns: int


class _Batch:
    """Changes and outbound reports of one event (persisted, then sent)."""

    __slots__ = ("meta", "orders", "out", "positions", "trades")

    def __init__(self) -> None:
        self.orders: dict[str, SimOrder] = {}
        self.trades: list[SimTrade] = []
        self.positions: set[tuple[str, str, str]] = set()
        self.meta = False
        self.out: list[tuple[bytes | None, dict[str, Any]]] = []

    def reply(self, identity: bytes, payload: dict[str, Any]) -> None:
        self.out.append((identity, payload))

    def broadcast(self, payload: dict[str, Any]) -> None:
        self.out.append((None, payload))


# ---------------------------------------------------------------------------
# Parsing helpers (lenient on types, strict on values)
# ---------------------------------------------------------------------------


def _ref(v: Any) -> str | None:
    if isinstance(v, bool):
        return None
    if isinstance(v, int):
        return str(v)
    if isinstance(v, str) and v.strip():
        return v.strip()
    return None


def _opt_str(v: Any) -> str | None:
    if isinstance(v, str) and v:
        return v
    return None


def _as_int(v: Any) -> int | None:
    if isinstance(v, bool):
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, float) and math.isfinite(v) and v == int(v):
        return int(v)
    if isinstance(v, str) and v.strip().lstrip("-").isdigit():
        return int(v.strip())
    return None


def _as_price(v: Any) -> float | None:
    if isinstance(v, bool) or not isinstance(v, int | float):
        return None
    f = float(v)
    return f if math.isfinite(f) else None


# ---------------------------------------------------------------------------
# Broker
# ---------------------------------------------------------------------------


class SimBroker:
    """The simulated counter; see the module docstring."""

    def __init__(
        self,
        cfg: FullConfig,
        *,
        clock: MarketClock,
        store: SimStore,
        endpoint: str | None = None,
        heartbeat_ms: int = 1000,
    ) -> None:
        b = cfg.sim.broker
        self._endpoint_cfg = endpoint or b.endpoint
        self.endpoint = self._endpoint_cfg
        self.front_id = int(b.front_id)
        # request latency and first-match latency: FIFO delay lines, the worker never sleeps
        self._requests: DelayLine[tuple[Any, ...]] = DelayLine(
            "sim-broker-ack-delay", max(0, b.ack_delay_ms) / 1000.0, self._enqueue)
        self._matches: DelayLine[tuple[Any, ...]] = DelayLine(
            "sim-broker-fill-delay", max(0, b.fill_delay_ms) / 1000.0, self._enqueue)
        self._hb_s = max(0.05, heartbeat_ms / 1000.0)
        self._calendar: TradingCalendar = cfg.calendar
        self._clock = clock
        self._store = store
        self._engine: PriceEngine | None = None
        self._metas: dict[str, InstrumentMeta] = {}
        # persisted counters
        self.trading_day = ""
        self.last_broker_seq = 0
        self.last_trade_no = 0
        self.last_order_no = 0
        self.last_session_id = 0
        # in-memory state (current trading day + live orders)
        self._sessions: dict[bytes, Session] = {}
        self._orders: dict[str, SimOrder] = {}
        self._active_ids: set[str] = set()
        self._by_ref: dict[tuple[int, int, str], str] = {}
        self._by_client: dict[str, str] = {}
        self._trades: list[SimTrade] = []
        self._positions: dict[tuple[str, str, str], Position] = {}
        self._quotes: dict[str, Quote] = {}
        self._consumed: dict[str, list[int]] = {}  # [bid lots used, ask lots used] per market state
        self._close_cache: dict[tuple[tuple[str, ...], str], int] = {}
        self._paused = False
        self._io: RouterIO | None = None
        self._events: asyncio.Queue[tuple[Any, ...] | None] = asyncio.Queue()
        self._worker: asyncio.Task[None] | None = None
        self._timer: asyncio.Task[None] | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self.stats = {"orders": 0, "rejects": 0, "trades": 0}

    # ------------------------------------------------------------------ lifecycle
    async def start(self, engine: PriceEngine) -> str:
        self._engine = engine
        self._metas = {m.instrument_id: m for m in engine.metas}
        self._loop = asyncio.get_running_loop()
        await self._load()
        self._io = RouterIO(self._endpoint_cfg, self._on_frame, self._loop)
        self.endpoint = self._io.start()
        self._requests.start()
        self._matches.start()
        self._worker = spawn("sim-broker", self._work)
        self._timer = spawn("sim-broker-timer", self._tick)
        logger.info(
            "sim broker listening on {} (trading_day={}, broker_seq_head={})",
            self.endpoint, self.trading_day, self.last_broker_seq,
        )
        return self.endpoint

    async def stop(self) -> None:
        if self._timer is not None:
            self._timer.cancel()
            await asyncio.gather(self._timer, return_exceptions=True)
            self._timer = None
        await self._requests.stop()
        await self._matches.stop()
        if self._worker is not None:
            self._events.put_nowait(None)
            try:
                await asyncio.wait_for(self._worker, 5.0)
            except (TimeoutError, asyncio.CancelledError):
                self._worker.cancel()
            self._worker = None
        if self._io is not None:
            self._io.stop()
            self._io = None

    async def _load(self) -> None:
        meta = await self._store.call(self._store.load_meta)
        fresh = not meta["trading_day"]
        self.trading_day = meta["trading_day"] or self._current_td(self._clock.now_ns())
        self.last_broker_seq = 0 if fresh else int(meta["last_broker_seq"])
        self.last_trade_no = int(meta["last_trade_no"])
        self.last_order_no = int(meta["last_order_no"])
        self.last_session_id = int(meta["last_session_id"])
        orders = await self._store.call(self._store.load_orders, self.trading_day)
        trades = await self._store.call(self._store.load_trades, self.trading_day)
        positions = await self._store.call(self._store.load_positions)
        self._orders, self._active_ids, self._by_ref, self._by_client = {}, set(), {}, {}
        for row in orders:
            o = SimOrder.from_row(row, self._metas.get(row["instrument_id"]))
            self._register(o)
        self._trades = [SimTrade.from_row(r) for r in trades]
        self._positions = {}
        for r in positions:
            p = Position(r["investor_id"], r["instrument_id"], r["posi_direction"],
                         int(r["today_position"]), int(r["yd_position"]))
            self._positions[p.key] = p
        if fresh:
            await self._store.call(self._store.apply, StoreBatch(meta=self._meta_row()))

    # ------------------------------------------------------------------ public API
    def on_market(self, quotes: list[Quote]) -> None:
        """New market states from the engine (called by the exchange after each round)."""
        if quotes:
            self._events.put_nowait(("market", quotes))

    def pause(self) -> None:
        self._paused = True

    def resume(self) -> None:
        self._paused = False
        self._events.put_nowait(("resume",))

    @property
    def paused(self) -> bool:
        return self._paused

    async def force_close(
        self, account_id: str, instrument_id: str, qty: int | None = None, posi_direction: str | None = None
    ) -> dict[str, Any]:
        """02 §9.5 ``BROKER_FORCE_CLOSE``: an external liquidation order + trade."""

        async def run(batch: _Batch) -> dict[str, Any]:
            return self._do_force_close(account_id, instrument_id, qty, posi_direction, batch)

        return await self._call(run)

    async def disconnect(self) -> None:
        """``BROKER_DISCONNECT``: drop every session and close the ROUTER."""

        async def run(batch: _Batch) -> None:
            self._sessions.clear()
            if self._io is not None:
                self._io.unbind()

        await self._call(run)

    def reconnect(self) -> None:
        if self._io is not None and not self._io.bound:
            self._io.rebind()

    async def restart(self) -> None:
        """``SIM_RESTART``: forget sessions and rebuild the counter state from sim.db."""

        async def run(batch: _Batch) -> None:
            self._sessions.clear()
            await self._load()

        await self._call(run)

    def state(self) -> dict[str, Any]:
        return {
            "endpoint": self.endpoint,
            "bound": self._io.bound if self._io is not None else False,
            "trading_day": self.trading_day,
            "broker_seq_head": self.last_broker_seq,
            "last_trade_no": self.last_trade_no,
            "paused": self._paused,
            "sessions": [
                {"front_id": s.front_id, "session_id": s.session_id, "user_id": s.user_id,
                 "identity": s.identity.decode("utf-8", "replace")}
                for s in self._sessions.values()
            ],
            "orders_today": sum(1 for o in self._orders.values() if o.trading_day == self.trading_day),
            "active_orders": len(self._active_ids),
            "positions": [self._position_fields(p) for p in self._sorted_positions()],
            "stats": dict(self.stats),
        }

    def positions(self) -> list[dict[str, Any]]:
        return [self._position_fields(p) for p in self._sorted_positions()]

    # ------------------------------------------------------------------ event loop
    def _on_frame(self, identity: bytes, data: bytes) -> None:
        self._requests.put(("frame", identity, data))

    def _enqueue(self, event: tuple[Any, ...]) -> None:
        self._events.put_nowait(event)

    async def _call(self, fn: Callable[[_Batch], Awaitable[T]]) -> T:
        """Run ``fn`` inside the worker (sequential with every other event), after its commit."""
        assert self._loop is not None
        fut: asyncio.Future[Any] = self._loop.create_future()
        self._events.put_nowait(("call", fn, fut))
        return cast(T, await fut)

    async def _tick(self) -> None:
        next_hb = 0.0
        while True:
            await asyncio.sleep(0.25)
            self._events.put_nowait(("housekeeping",))
            now = time.monotonic()
            if now >= next_hb:
                self._events.put_nowait(("heartbeat",))
                next_hb = now + self._hb_s

    async def _work(self) -> None:
        while True:
            ev = await self._events.get()
            if ev is None:
                return
            batch = _Batch()
            result: Any = None
            error: BaseException | None = None
            try:
                self._housekeeping(batch)
                result = await self._dispatch(ev, batch)
            except Exception as exc:  # the event failed; what it changed is still committed below
                error = exc
            try:
                await self._commit(batch)
            except Exception as exc:
                logger.exception("sim broker failed to persist an event")
                error = error or exc
            if ev[0] == "call":
                fut: asyncio.Future[Any] = ev[2]
                if not fut.done():
                    if error is not None:
                        fut.set_exception(error)
                    else:
                        fut.set_result(result)
            elif error is not None:
                logger.opt(exception=error).error("sim broker event {} failed", ev[0])

    async def _dispatch(self, ev: tuple[Any, ...], batch: _Batch) -> Any:
        kind = ev[0]
        if kind == "frame":
            await self._on_request(ev[1], ev[2], batch)
        elif kind == "market":
            self._on_market(ev[1], batch)
        elif kind == "match":
            o = self._orders.get(ev[1])
            if o is not None and o.active and o.pending_match:
                self._try_aggressive(o, batch)
        elif kind == "resume":
            for o in self._sorted_active():
                if o.pending_match:
                    self._try_aggressive(o, batch)
        elif kind == "heartbeat":
            batch.broadcast({
                "msg": "Heartbeat",
                "ts": self._clock.now_ns() // NS_PER_MS,
                "broker_seq_head": self.last_broker_seq,
                "trading_day": self.trading_day,
            })
        elif kind == "call":
            return await ev[1](batch)
        return None

    async def _commit(self, batch: _Batch) -> None:
        sb = StoreBatch()
        now = self._clock.now_ns()
        if batch.orders:
            sb.orders = [o.to_row() for o in batch.orders.values()]
        if batch.trades:
            sb.trades = [t.to_row() for t in batch.trades]
        for key in batch.positions:
            p = self._positions.get(key)
            if p is None or p.total == 0:
                self._positions.pop(key, None)
                sb.deleted_positions.append(key)
            else:
                sb.positions.append({
                    "investor_id": p.investor_id, "instrument_id": p.instrument_id,
                    "posi_direction": p.posi_direction, "today_position": p.today,
                    "yd_position": p.yd, "updated_ns": now,
                })
        if batch.meta:
            sb.meta = self._meta_row()
        if not sb.empty():
            # shielded: once handed to the writer thread the write must happen even if the
            # worker is cancelled, or broker_seq values could be sent twice after a restart
            await asyncio.shield(self._store.call(self._store.apply, sb))
        if batch.out and self._io is not None:
            for target, payload in batch.out:
                data = dumps_bytes(payload)
                if target is None:
                    for ident in list(self._sessions):
                        self._io.send(ident, data)
                else:
                    self._io.send(target, data)

    def _meta_row(self) -> dict[str, Any]:
        return {
            "trading_day": self.trading_day,
            "last_broker_seq": self.last_broker_seq,
            "last_trade_no": self.last_trade_no,
            "last_order_no": self.last_order_no,
            "last_session_id": self.last_session_id,
            "updated_ns": self._clock.now_ns(),
        }

    # ------------------------------------------------------------------ calendar
    def _current_td(self, now_ns: int) -> str:
        try:
            return td_str(self._calendar.current_or_next_trading_day(now_ns))
        except LookupError:
            return self.trading_day

    def _order_td(self, now_ns: int) -> str:
        try:
            d = self._calendar.trading_day_for_ns(now_ns)
        except LookupError:
            d = None
        return td_str(d) if d is not None else self.trading_day

    def _day_close_ns(self, o: SimOrder) -> int:
        meta = self._metas.get(o.instrument_id)
        if meta is None:
            return 0
        key = (meta.sessions, o.trading_day)
        cached = self._close_cache.get(key)
        if cached is None:
            close = self._calendar.day_close(meta.sessions, parse_td(o.trading_day))
            cached = bj_to_ns(close) if close is not None else 0
            self._close_cache[key] = cached
        return cached

    def _housekeeping(self, batch: _Batch) -> None:
        """Session-close cancels (02 §9.5) and the trading-day roll of counters/positions."""
        now = self._clock.now_ns()
        for o in self._sorted_active():
            if now >= self._day_close_ns(o):
                o.pending_match = False
                self._set_status(o, ST_CANCELED, "SESSION_CLOSE", batch, now)
        cur = self._current_td(now)
        if cur and cur > self.trading_day:
            self._roll_day(cur, batch)

    def _roll_day(self, new_td: str, batch: _Batch) -> None:
        for key, p in self._positions.items():
            if p.today:
                p.yd += p.today
                p.today = 0
                batch.positions.add(key)
        old = self.trading_day
        self.trading_day = new_td
        self.last_broker_seq = 0
        batch.meta = True
        self._orders = {k: o for k, o in self._orders.items() if o.active or o.trading_day == new_td}
        self._by_ref = {(o.front_id, o.session_id, o.order_ref): k for k, o in self._orders.items()}
        self._by_client = {o.client_order_id: k for k, o in self._orders.items() if o.client_order_id}
        self._trades = [t for t in self._trades if t.trading_day == new_td]
        self._close_cache.clear()
        logger.info("sim broker trading day {} -> {}: positions rolled, broker_seq reset", old, new_td)

    # ------------------------------------------------------------------ bookkeeping
    def _register(self, o: SimOrder) -> None:
        self._orders[o.order_sys_id] = o
        self._by_ref[(o.front_id, o.session_id, o.order_ref)] = o.order_sys_id
        if o.client_order_id:
            self._by_client[o.client_order_id] = o.order_sys_id
        if o.active:
            self._active_ids.add(o.order_sys_id)

    def _sorted_active(self) -> list[SimOrder]:
        return sorted((self._orders[i] for i in self._active_ids if i in self._orders), key=lambda o: o.seq_no)

    def _sorted_positions(self) -> list[Position]:
        return sorted((p for p in self._positions.values() if p.total > 0), key=lambda p: p.key)

    def _next_seq(self, batch: _Batch) -> int:
        self.last_broker_seq += 1
        batch.meta = True
        return self.last_broker_seq

    def _next_order_sys_id(self) -> str:
        self.last_order_no += 1
        return f"{self.last_order_no:010d}"

    def _set_status(self, o: SimOrder, status: str, msg: str, batch: _Batch, now: int) -> None:
        o.order_status = status
        o.status_msg = msg
        o.updated_ns = now
        o.last_broker_seq = self._next_seq(batch)
        if o.active:
            self._active_ids.add(o.order_sys_id)
        else:
            self._active_ids.discard(o.order_sys_id)
        batch.orders[o.order_sys_id] = o
        batch.broadcast(self._rtn_order(o))

    def _pos(self, investor: str, instrument: str, pdir: str) -> Position:
        key = (investor, instrument, pdir)
        p = self._positions.get(key)
        if p is None:
            p = self._positions[key] = Position(investor, instrument, pdir)
        return p

    def _schedule_match(self, order_sys_id: str) -> None:
        self._matches.put(("match", order_sys_id))

    # ------------------------------------------------------------------ requests
    async def _on_request(self, identity: bytes, data: bytes, batch: _Batch) -> None:
        try:
            msg = json.loads(data.decode("utf-8"))
        except (UnicodeDecodeError, ValueError):
            batch.reply(identity, self._rsp_error(None, BROKER_BAD_REQUEST, "frame is not valid UTF-8 JSON"))
            return
        if not isinstance(msg, dict):
            batch.reply(identity, self._rsp_error(None, BROKER_BAD_REQUEST, "frame must be a JSON object"))
            return
        kind = msg.get("msg")
        req_id = msg.get("req_id")
        if kind == "ReqUserLogin":
            self._login(identity, msg, batch)
            return
        session = self._sessions.get(identity)
        if kind == "ReqOrderInsert":
            if session is None:
                batch.reply(identity, self._err_insert(msg, None, BROKER_NOT_LOGGED_IN, "login first"))
            else:
                self._insert(session, msg, batch)
        elif kind == "ReqOrderAction":
            if session is None:
                batch.reply(identity, self._err_action(msg, BROKER_NOT_LOGGED_IN, "login first"))
            else:
                self._action(session, msg, batch)
        elif kind in ("ReqQryOrder", "ReqQryTrade", "ReqQryInvestorPosition"):
            if session is None:
                batch.reply(identity, self._rsp_error(req_id, BROKER_NOT_LOGGED_IN, "login first"))
            elif kind == "ReqQryOrder":
                await self._qry_orders(identity, msg, batch)
            elif kind == "ReqQryTrade":
                await self._qry_trades(identity, msg, batch)
            else:
                self._qry_positions(identity, msg, batch)
        else:
            batch.reply(identity, self._rsp_error(req_id, BROKER_BAD_REQUEST, f"unknown msg {kind!r}"))

    def _login(self, identity: bytes, msg: dict[str, Any], batch: _Batch) -> None:
        broker_id, user_id = msg.get("broker_id"), msg.get("user_id")
        base = {
            "msg": "RspUserLogin",
            "req_id": msg.get("req_id"),
            "trading_day": self.trading_day,
            "front_id": self.front_id,
        }
        if not (isinstance(broker_id, str) and broker_id and isinstance(user_id, str) and user_id):
            batch.reply(identity, {
                **base, "session_id": 0, "max_order_ref": "0", "broker_seq_head": self.last_broker_seq,
                "error": {"error_id": BROKER_LOGIN_FAILED, "error_msg": "broker_id and user_id are required"},
            })
            return
        self.last_session_id += 1
        batch.meta = True
        s = Session(identity, self.front_id, self.last_session_id, broker_id, user_id,
                    str(msg.get("app_id") or ""), self._clock.now_ns())
        self._sessions[identity] = s
        refs = [int(o.order_ref) for o in self._orders.values()
                if o.trading_day == self.trading_day and o.order_ref.isdigit()]
        batch.reply(identity, {
            **base,
            "session_id": s.session_id,
            "max_order_ref": str(max(refs, default=0)),
            "broker_seq_head": self.last_broker_seq,
            "error": None,
        })
        logger.info("sim broker login user={} identity={!r} session_id={}", user_id, identity, s.session_id)

    def _insert(self, s: Session, msg: dict[str, Any], batch: _Batch) -> None:
        order_ref = _ref(msg.get("order_ref"))
        if order_ref is None:
            batch.reply(s.identity, self._err_insert(msg, None, BROKER_BAD_REQUEST, "order_ref is required"))
            return
        coid = _opt_str(msg.get("client_order_id"))
        if coid is not None and coid in self._by_client:
            # retransmission of a known order: resend its current state, never a second order
            batch.reply(s.identity, self._rtn_order(self._orders[self._by_client[coid]]))
            return
        if (s.front_id, s.session_id, order_ref) in self._by_ref:
            batch.reply(s.identity, self._err_insert(
                msg, None, BROKER_DUPLICATE_ORDER_REF, f"order_ref {order_ref} already used in this session"))
            return
        now = self._clock.now_ns()
        qty = _as_int(msg.get("volume_total_original"))
        o = SimOrder(
            order_sys_id=self._next_order_sys_id(),
            trading_day=self._order_td(now),
            front_id=s.front_id,
            session_id=s.session_id,
            order_ref=order_ref,
            client_order_id=coid,
            investor_id=str(msg.get("investor_id") or ""),
            instrument_id=str(msg.get("instrument_id") or ""),
            exchange_id=str(msg.get("exchange_id") or ""),
            direction=str(msg.get("direction", "")),
            comb_offset_flag=str(msg.get("comb_offset_flag", "")),
            order_price_type=str(msg.get("order_price_type") or "LIMIT").upper(),
            limit_price=_as_price(msg.get("limit_price")) or 0.0,
            volume_total_original=qty if qty is not None else 0,
            volume_traded=0,
            order_status=ST_ACCEPTED,
            status_msg="",
            time_condition=str(msg.get("time_condition") or "GFD").upper(),
            volume_condition=str(msg.get("volume_condition") or "AV").upper(),
            origin="INTERNAL",
            last_broker_seq=0,
            insert_ns=now,
            updated_ns=now,
        )
        err = self._validate(o, msg, now)
        o.order_status = ST_REJECTED if err else ST_ACCEPTED
        self._register(o)
        self.stats["orders"] += 1
        if err is not None:
            code, text = err
            self.stats["rejects"] += 1
            self._set_status(o, ST_REJECTED, code, batch, now)
            batch.reply(s.identity, self._err_insert(msg, o, code, text))
            return
        o.pending_match = True
        self._set_status(o, ST_ACCEPTED, "ACCEPTED", batch, now)
        self._schedule_match(o.order_sys_id)

    def _validate(self, o: SimOrder, msg: dict[str, Any], now: int) -> tuple[str, str] | None:
        meta = self._metas.get(o.instrument_id)
        if meta is None:
            return BROKER_REJECT_INVALID, f"instrument {o.instrument_id!r} is not traded by the simulator"
        if o.exchange_id != meta.exchange:
            return BROKER_REJECT_INVALID, f"exchange_id {o.exchange_id!r} does not match {meta.exchange}"
        if not o.investor_id:
            return BROKER_REJECT_INVALID, "investor_id is required"
        if o.direction not in (BUY, SELL):
            return BROKER_REJECT_INVALID, "direction must be '0' (buy) or '1' (sell)"
        if o.comb_offset_flag not in CLIENT_OFFSETS:
            return BROKER_REJECT_INVALID, "comb_offset_flag must be '0', '1', '3' or '4' ('2' is broker-only)"
        if o.order_price_type not in ("LIMIT", "ANY"):
            return BROKER_REJECT_INVALID, "order_price_type must be LIMIT or ANY"
        if o.time_condition not in ("GFD", "IOC"):
            return BROKER_REJECT_INVALID, "time_condition must be GFD or IOC"
        if o.volume_condition not in ("AV", "CV"):
            return BROKER_REJECT_INVALID, "volume_condition must be AV or CV"
        if o.volume_total_original <= 0:
            return BROKER_REJECT_INVALID, "volume_total_original must be a positive integer"
        max_qty = meta.max_limit_qty if o.order_price_type == "LIMIT" else meta.max_market_qty
        if o.volume_total_original > max_qty:
            return BROKER_REJECT_INVALID, f"volume {o.volume_total_original} exceeds the max order size {max_qty}"
        if o.order_price_type == "LIMIT":
            raw = _as_price(msg.get("limit_price"))
            if raw is None or raw <= 0:
                return codes.BROKER_REJECT_PRICE, "limit_price is required for LIMIT orders"
            ticks = meta.to_ticks(raw)
            if ticks is None:
                return codes.BROKER_REJECT_PRICE, f"limit_price {raw} is not on the {meta.tick} tick grid"
            assert self._engine is not None
            # the band of the trading day in force now: rolled at the first window of the
            # day, so an order sent during the opening auction sees the new limits
            st = self._engine.sync_day(o.instrument_id, now)
            if not st.lower <= ticks <= st.upper:
                return codes.BROKER_REJECT_PRICE, (
                    f"limit_price {raw} outside [{meta.price(st.lower)}, {meta.price(st.upper)}]"
                )
            o.limit_ticks = ticks
            o.limit_price = float(meta.price(ticks))
        else:
            o.limit_price = 0.0
        if not self._calendar.phase_at(meta.sessions, now).is_open:
            return BROKER_REJECT_SESSION_CLOSED, f"{o.instrument_id} is not in a trading session"
        if o.comb_offset_flag != OFF_OPEN:
            return self._check_close(o, meta)
        return None

    def _frozen(self, o: SimOrder, meta: InstrumentMeta) -> tuple[int, int, int]:
        """Pending close quantities (today, yesterday, any) on the position ``o`` closes."""
        fr_today = fr_yd = fr_any = 0
        for other in self._orders.values():
            if (
                other is o
                or not other.active
                or other.comb_offset_flag == OFF_OPEN
                or other.investor_id != o.investor_id
                or other.instrument_id != o.instrument_id
                or other.direction != o.direction
            ):
                continue
            fr_any += other.remaining
            if meta.explicit_close_today:
                if other.comb_offset_flag == OFF_CLOSE_TODAY:
                    fr_today += other.remaining
                else:
                    fr_yd += other.remaining
        return fr_today, fr_yd, fr_any

    def _check_close(self, o: SimOrder, meta: InstrumentMeta) -> tuple[str, str] | None:
        pdir = SHORT if o.direction == BUY else LONG
        p = self._positions.get((o.investor_id, o.instrument_id, pdir))
        today, yd = (p.today, p.yd) if p is not None else (0, 0)
        fr_today, fr_yd, fr_any = self._frozen(o, meta)
        qty = o.volume_total_original
        if meta.explicit_close_today:
            # 01 §8.15 / 02 §7.3: SHFE 必须区分平今平昨；CLOSE 按 CTP 语义等同平昨
            if o.comb_offset_flag == OFF_CLOSE_TODAY:
                avail = today - fr_today
                if qty > avail:
                    return codes.BROKER_REJECT_CLOSE_TODAY, (
                        f"close-today {qty} > available today {pdir} position {max(avail, 0)}")
            else:
                avail = yd - fr_yd
                if qty > avail:
                    return codes.BROKER_REJECT_CLOSE_YD, (
                        f"close-yesterday {qty} > available yesterday {pdir} position {max(avail, 0)}")
        else:
            avail = today + yd - fr_any
            if qty > avail:
                return BROKER_REJECT_POSITION, f"close {qty} > available {pdir} position {max(avail, 0)}"
        return None

    def _action(self, s: Session, msg: dict[str, Any], batch: _Batch) -> None:
        action = str(msg.get("action") or "DELETE").upper()
        if action != "DELETE":
            batch.reply(s.identity, self._err_action(msg, BROKER_BAD_REQUEST, f"unsupported action {action}"))
            return
        o = self._find_order(msg)
        if o is None:
            batch.reply(s.identity, self._err_action(msg, codes.BROKER_ORDER_NOT_FOUND, "order not found"))
            return
        if not o.active:
            batch.reply(s.identity, self._err_action(
                msg, codes.BROKER_ORDER_FINISHED, f"order {o.order_sys_id} is {o.order_status}"))
            return
        o.pending_match = False
        self._set_status(o, ST_CANCELED, "USER_CANCELED", batch, self._clock.now_ns())

    def _find_order(self, msg: dict[str, Any]) -> SimOrder | None:
        sys_id = _ref(msg.get("order_sys_id"))
        if sys_id is not None:
            return self._orders.get(sys_id)
        front, sess, ref = _as_int(msg.get("front_id")), _as_int(msg.get("session_id")), _ref(msg.get("order_ref"))
        if front is None or sess is None or ref is None:
            return None
        oid = self._by_ref.get((front, sess, ref))
        return self._orders.get(oid) if oid is not None else None

    async def _qry_orders(self, identity: bytes, msg: dict[str, Any], batch: _Batch) -> None:
        td = str(msg.get("trading_day") or self.trading_day)
        from_seq = _as_int(msg.get("from_broker_seq")) or 0
        if td == self.trading_day:
            orders = sorted(
                (o for o in self._orders.values() if o.trading_day == td and o.last_broker_seq >= from_seq),
                key=lambda o: o.last_broker_seq,
            )
        else:
            rows = await self._store.call(self._store.query_orders, td, from_seq)
            orders = [SimOrder.from_row(r, self._metas.get(r["instrument_id"])) for r in rows]
        self._pages(identity, "RspQryOrder", msg.get("req_id"), [self._order_fields(o) for o in orders], batch)

    async def _qry_trades(self, identity: bytes, msg: dict[str, Any], batch: _Batch) -> None:
        td = str(msg.get("trading_day") or self.trading_day)
        from_seq = _as_int(msg.get("from_broker_seq")) or 0
        if td == self.trading_day:
            trades = [t for t in self._trades if t.broker_seq >= from_seq]
        else:
            rows = await self._store.call(self._store.query_trades, td, from_seq)
            trades = [SimTrade.from_row(r) for r in rows]
        self._pages(identity, "RspQryTrade", msg.get("req_id"), [self._trade_fields(t) for t in trades], batch)

    def _qry_positions(self, identity: bytes, msg: dict[str, Any], batch: _Batch) -> None:
        """Positions as of the requested trading day: a later day sees today's lots as yesterday's."""
        td = str(msg.get("trading_day") or self.trading_day)
        investor = _opt_str(msg.get("investor_id"))
        rolled = td > self.trading_day
        items = []
        for p in self._sorted_positions():
            if investor is not None and p.investor_id != investor:
                continue
            f = self._position_fields(p)
            if rolled:
                f["yd_position"], f["today_position"] = p.total, 0
            items.append(f)
        batch.reply(identity, {
            "msg": "RspQryInvestorPosition", "req_id": msg.get("req_id"), "trading_day": td,
            "items": items, "is_last": True,
        })

    def _pages(self, identity: bytes, name: str, req_id: Any, items: list[dict[str, Any]], batch: _Batch) -> None:
        if not items:
            batch.reply(identity, {"msg": name, "req_id": req_id, "items": [], "is_last": True})
            return
        for i in range(0, len(items), QUERY_PAGE):
            chunk = items[i:i + QUERY_PAGE]
            batch.reply(identity, {"msg": name, "req_id": req_id, "items": chunk,
                                   "is_last": i + QUERY_PAGE >= len(items)})

    # ------------------------------------------------------------------ matching
    def _on_market(self, quotes: list[Quote], batch: _Batch) -> None:
        for q in quotes:
            self._quotes[q.instrument_id] = q
            self._consumed[q.instrument_id] = [0, 0]
        if self._paused:
            return
        ids = {q.instrument_id for q in quotes}
        now = self._clock.now_ns()
        for o in self._sorted_active():
            if o.instrument_id not in ids or not o.active:
                continue
            if o.pending_match or o.ioc:
                self._try_aggressive(o, batch)
            else:
                q = self._quotes[o.instrument_id]
                if q.trading_day == o.trading_day:
                    self._try_passive(o, q, batch, now)

    def _liquidity(self, o: SimOrder, q: Quote) -> tuple[int | None, int]:
        used = self._consumed.setdefault(q.instrument_id, [0, 0])
        if o.direction == BUY:
            return q.ask, max(0, q.ask_vol - used[1])
        return q.bid, max(0, q.bid_vol - used[0])

    def _consume(self, o: SimOrder, qty: int) -> None:
        used = self._consumed.setdefault(o.instrument_id, [0, 0])
        used[1 if o.direction == BUY else 0] += qty

    def _try_aggressive(self, o: SimOrder, batch: _Batch) -> None:
        if self._paused:
            return  # matched after POST /control/resume
        meta = self._metas.get(o.instrument_id)
        if meta is None:
            return
        now = self._clock.now_ns()
        phase = self._calendar.phase_at(meta.sessions, now)
        if not phase.is_continuous:
            if o.ioc:  # FAK/FOK/market cannot wait for the continuous session
                o.pending_match = False
                self._set_status(o, ST_CANCELED, "NOT_IN_CONTINUOUS_SESSION", batch, now)
            return
        q = self._quotes.get(o.instrument_id)
        td = td_str(phase.trading_day) if phase.trading_day is not None else None
        if q is None or q.trading_day != td or (q.bid is None and q.ask is None):
            return  # no book for this trading day yet: retried on the next market state
        o.pending_match = False
        px, avail = self._liquidity(o, q)
        crossing = px is not None and (
            o.order_price_type == "ANY"
            or (o.limit_ticks is not None and (o.limit_ticks >= px if o.direction == BUY else o.limit_ticks <= px))
        )
        qty = min(o.remaining, avail) if crossing else 0
        if o.time_condition == "IOC" and o.volume_condition == "CV" and qty < o.remaining:
            qty = 0  # FOK: all or nothing
        if qty > 0 and px is not None:
            self._fill(o, qty, px, batch, now)
        if o.active and o.ioc:
            if o.order_price_type == "ANY":
                reason = "MARKET_REMAINDER_CANCELED"
            elif o.volume_condition == "CV":
                reason = "FOK_CANCELED"
            else:
                reason = "FAK_REMAINDER_CANCELED"
            self._set_status(o, ST_CANCELED, reason, batch, now)

    def _try_passive(self, o: SimOrder, q: Quote, batch: _Batch, now: int) -> None:
        if o.order_price_type != "LIMIT" or o.limit_ticks is None:
            return
        px, avail = self._liquidity(o, q)
        if px is None or avail <= 0:
            return
        if (o.direction == BUY and px > o.limit_ticks) or (o.direction == SELL and px < o.limit_ticks):
            return
        self._fill(o, min(o.remaining, avail), o.limit_ticks, batch, now)

    def _fill(self, o: SimOrder, qty: int, px_ticks: int, batch: _Batch, now: int) -> None:
        meta = self._metas[o.instrument_id]
        o.volume_traded += qty
        self._consume(o, qty)
        self.last_trade_no += 1
        trade = SimTrade(
            trade_id=f"{self.last_trade_no:010d}",
            trading_day=o.trading_day,
            order_sys_id=o.order_sys_id,
            broker_seq=self._next_seq(batch),
            front_id=o.front_id,
            session_id=o.session_id,
            order_ref=o.order_ref,
            client_order_id=o.client_order_id,
            investor_id=o.investor_id,
            instrument_id=o.instrument_id,
            exchange_id=o.exchange_id,
            direction=o.direction,
            offset_flag=o.comb_offset_flag,
            price=float(meta.price(px_ticks)),
            volume=qty,
            trade_ts_ns=now,
        )
        self._trades.append(trade)
        batch.trades.append(trade)
        batch.broadcast(self._rtn_trade(trade))  # the trade precedes the order's new status (02 §5)
        self._apply_position(o, qty, meta, batch)
        self.stats["trades"] += 1
        status = ST_ALL if o.remaining == 0 else ST_PART
        self._set_status(o, status, status, batch, now)

    def _apply_position(self, o: SimOrder, qty: int, meta: InstrumentMeta, batch: _Batch) -> None:
        if o.comb_offset_flag == OFF_OPEN:
            p = self._pos(o.investor_id, o.instrument_id, LONG if o.direction == BUY else SHORT)
            p.today += qty
            batch.positions.add(p.key)
            return
        p = self._pos(o.investor_id, o.instrument_id, SHORT if o.direction == BUY else LONG)
        flag = o.comb_offset_flag
        order: tuple[str, ...]
        if meta.explicit_close_today:
            if flag == OFF_CLOSE_TODAY:
                order = ("today",)
            elif flag == OFF_FORCE:
                order = ("yd", "today")  # core.lots: FORCE_CLOSE follows CLOSE, spilling into today
            else:
                order = ("yd",)
        else:
            order = ("today", "yd") if meta.close_style == "TODAY_FIRST" else ("yd", "today")
        left = qty
        for bucket in order:
            have = p.today if bucket == "today" else p.yd
            take = min(have, left)
            if bucket == "today":
                p.today -= take
            else:
                p.yd -= take
            left -= take
        if left > 0:  # validated at insert; only an inconsistent external change gets here
            logger.warning("sim broker: close of {} exceeds position {}; clamped", qty, p.key)
            take = min(p.today, left)
            p.today -= take
            p.yd = max(0, p.yd - (left - take))
        batch.positions.add(p.key)

    def _do_force_close(
        self, account_id: str, instrument_id: str, qty: int | None, posi_direction: str | None, batch: _Batch
    ) -> dict[str, Any]:
        meta = self._metas.get(instrument_id)
        if meta is None:
            raise bad_request(f"unknown instrument {instrument_id}")
        p: Position | None = None
        for pdir in ([posi_direction] if posi_direction else [LONG, SHORT]):
            cand = self._positions.get((account_id, instrument_id, pdir))
            if cand is not None and cand.total > 0:
                p = cand
                break
        if p is None:
            side = f"{posi_direction} " if posi_direction else ""
            raise conflict(f"{account_id} has no {side}position in {instrument_id}")
        q = qty or p.total
        if q > p.total:
            raise conflict(f"qty {q} exceeds the {p.posi_direction} position {p.total}")
        assert self._engine is not None
        now = self._clock.now_ns()
        direction = SELL if p.posi_direction == LONG else BUY
        # 柜台强平前先撤销该持仓上的平仓挂单：它们占用的仓位会被强平拿走，之后再成交就会超平
        canceled = []
        for pending in self._sorted_active():
            if (
                pending.investor_id == account_id
                and pending.instrument_id == instrument_id
                and pending.direction == direction
                and pending.comb_offset_flag != OFF_OPEN
            ):
                pending.pending_match = False
                self._set_status(pending, ST_CANCELED, "FORCE_CLOSE_CANCELED", batch, now)
                canceled.append(pending.order_sys_id)
        quote = self._quotes.get(instrument_id) or self._engine.quote(instrument_id)
        px = quote.bid if direction == SELL else quote.ask
        if px is None:
            px = quote.last
        sys_id = self._next_order_sys_id()
        o = SimOrder(
            order_sys_id=sys_id,
            trading_day=self._order_td(now),
            front_id=self.front_id,
            session_id=0,  # the broker's own terminal
            order_ref=f"FC{int(sys_id)}",
            client_order_id=None,
            investor_id=account_id,
            instrument_id=instrument_id,
            exchange_id=meta.exchange,
            direction=direction,
            comb_offset_flag=OFF_FORCE,
            order_price_type="LIMIT",
            limit_price=float(meta.price(px)),
            volume_total_original=q,
            volume_traded=0,
            order_status=ST_ACCEPTED,
            status_msg="FORCE_CLOSE",
            time_condition="GFD",
            volume_condition="AV",
            origin="EXTERNAL",
            last_broker_seq=0,
            insert_ns=now,
            updated_ns=now,
            limit_ticks=px,
        )
        self._register(o)
        self._set_status(o, ST_ACCEPTED, "FORCE_CLOSE", batch, now)
        self._fill(o, q, px, batch, now)
        trade = self._trades[-1]
        logger.info("sim broker force close {} {} {} x{} @ {}", account_id, instrument_id, p.posi_direction, q,
                    trade.price)
        return {
            "order_sys_id": sys_id,
            "trade_id": trade.trade_id,
            "broker_seq": trade.broker_seq,
            "account_id": account_id,
            "instrument_id": instrument_id,
            "posi_direction": p.posi_direction,
            "direction": direction,
            "price": trade.price,
            "qty": q,
            "canceled_orders": canceled,
        }

    # ------------------------------------------------------------------ messages
    def _order_fields(self, o: SimOrder) -> dict[str, Any]:
        return {
            "broker_seq": o.last_broker_seq,
            "trading_day": o.trading_day,
            "front_id": o.front_id,
            "session_id": o.session_id,
            "order_ref": o.order_ref,
            "order_sys_id": o.order_sys_id,
            "client_order_id": o.client_order_id,
            "investor_id": o.investor_id,
            "instrument_id": o.instrument_id,
            "exchange_id": o.exchange_id,
            "direction": o.direction,
            "comb_offset_flag": o.comb_offset_flag,
            "order_price_type": o.order_price_type,
            "time_condition": o.time_condition,
            "volume_condition": o.volume_condition,
            "limit_price": o.limit_price,
            "volume_total_original": o.volume_total_original,
            "volume_traded": o.volume_traded,
            "volume_total": o.remaining,
            "order_status": o.order_status,
            "status_msg": o.status_msg,
            "insert_time": bj_parts(o.insert_ns)[1],
            "update_ns": o.updated_ns,
        }

    def _rtn_order(self, o: SimOrder) -> dict[str, Any]:
        return {"msg": "RtnOrder", **self._order_fields(o)}

    @staticmethod
    def _trade_fields(t: SimTrade) -> dict[str, Any]:
        trade_date, trade_time, _ = bj_parts(t.trade_ts_ns)
        return {
            "broker_seq": t.broker_seq,
            "trading_day": t.trading_day,
            "trade_id": t.trade_id,
            "front_id": t.front_id,
            "session_id": t.session_id,
            "order_ref": t.order_ref,
            "order_sys_id": t.order_sys_id,
            "client_order_id": t.client_order_id,
            "investor_id": t.investor_id,
            "instrument_id": t.instrument_id,
            "exchange_id": t.exchange_id,
            "direction": t.direction,
            "offset_flag": t.offset_flag,
            "price": t.price,
            "volume": t.volume,
            "trade_date": trade_date,
            "trade_time": trade_time,
            "trade_ts_ns": t.trade_ts_ns,
        }

    def _rtn_trade(self, t: SimTrade) -> dict[str, Any]:
        return {"msg": "RtnTrade", **self._trade_fields(t)}

    @staticmethod
    def _position_fields(p: Position) -> dict[str, Any]:
        return {
            "investor_id": p.investor_id,
            "instrument_id": p.instrument_id,
            "posi_direction": p.posi_direction,
            "today_position": p.today,
            "yd_position": p.yd,
            "position": p.total,
        }

    @staticmethod
    def _err_insert(msg: dict[str, Any], o: SimOrder | None, code: str, text: str) -> dict[str, Any]:
        return {
            "msg": "ErrRtnOrderInsert",
            "req_id": msg.get("req_id"),
            "order_ref": _ref(msg.get("order_ref")),
            "client_order_id": _opt_str(msg.get("client_order_id")),
            "order_sys_id": o.order_sys_id if o is not None else None,
            "front_id": o.front_id if o is not None else None,
            "session_id": o.session_id if o is not None else None,
            "error_id": code,
            "error_msg": text,
        }

    @staticmethod
    def _err_action(msg: dict[str, Any], code: str, text: str) -> dict[str, Any]:
        return {
            "msg": "ErrRtnOrderAction",
            "req_id": msg.get("req_id"),
            "front_id": msg.get("front_id"),
            "session_id": msg.get("session_id"),
            "order_ref": _ref(msg.get("order_ref")),
            "order_sys_id": _ref(msg.get("order_sys_id")),
            "error_id": code,
            "error_msg": text,
        }

    @staticmethod
    def _rsp_error(req_id: Any, code: str, text: str) -> dict[str, Any]:
        return {"msg": "RspError", "req_id": req_id, "error_id": code, "error_msg": text}


__all__ = ["Position", "SimBroker", "SimOrder", "SimTrade"]
