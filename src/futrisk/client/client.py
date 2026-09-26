"""``FutriskClient``: the async strategy SDK (01 §3.1, §6.2 R2; 02 §4.1, §4.4, §4.5, §4.7, §6.3, §7.4).

Strategies are external clients of the bus: every order goes through the owning
risk-engine shard (the only order entry, 01 §3.1). The client

- sends ``order.submit`` / ``order.cancel`` / ``order.cancel_all`` from a DEALER whose
  identity is ``strategy-sim`` (the ROUTER whitelist of 02 §4.11) to
  ``risk-engine-{shard}`` resolved from ``accounts.yaml`` (02 §3); per-attempt timeout
  500 ms, retries only for ``TIMEOUT``/``UNAVAILABLE``/``BUSY``/``RISK_BUSY`` with the
  *same* payload - hence the same ``client_order_id`` (02 §4.7, :mod:`futrisk.bus.rpc_client`);
- subscribes ``md.tick.`` (or ``md.tick.{instrument}``) on md-gateway and
  ``oms.order.{account}`` on the owning shards, re-checking the full topic after the
  ZeroMQ prefix filter (``oms.order.ACC1`` never delivers ``ACC10``, 02 §4.4);
- keeps the latest tick per instrument (LV: stale epochs/duplicates dropped, 02 §4.5),
  an :class:`~futrisk.client.orders.OrderBook` and a
  :class:`~futrisk.client.positions.PositionTracker` fed by the fills of the updates;
- checks the FACT sequence of ``oms.order.*`` and repairs gaps with ``ctl.replay``
  (02 §4.6, §8.2; applying updates is idempotent, so a late replay is harmless);
- follows ``risk.state.{account}`` (FACT) and ``risk.snapshot.{account}`` (LV) of the owning
  shard - with a ``ctl.snapshot`` at start (Clone, 02 §4.6) - as a
  :class:`~futrisk.client.riskview.RiskView` per account (state, restrictions, equity).

**Positions and snapshots.** The tracker is seeded from an engine snapshot
(``risk.query.accounts`` / ``risk.snapshot``: positions + ``as_of_ns``). A fill is applied
to the tracker only when it happened after the account's snapshot time (``last_fill.trade_ts_ns``
or ``updated_ns``, both engine market time); for an order first seen after the snapshot only
its latest fill can be new. Fills applied after the snapshot time are journaled and re-applied
when a later seed arrives, so a seed and the order stream may cross in any order without a
double count. Client and engine clocks are never compared (a demo clock may be offset).

**Own orders.** With ``coid_tag`` the low 16 bits of every UUIDv7 ``client_order_id`` carry a
marker derived from the tag, so a later incarnation recognises its predecessor's orders
(:meth:`FutriskClient.is_own`): :meth:`FutriskClient.cancel_own_orders` cancels those one by
one and never touches manual or liquidation orders; ``order.cancel_all`` needs an explicit
``scope``.

``client_order_id`` values are UUIDv7 and ``sent_ns`` is market time estimated from the ticks
(:class:`TickClock`). Callbacks may be plain functions or coroutines; exceptions in
callbacks are logged and never stop the client.

Example::

    async with FutriskClient(cfg) as cli:
        cli.subscribe_orders(["ACC3"], on_order)
        cli.subscribe_ticks(["rb2701"], on_tick)
        await cli.wait_ticks(["rb2701"], timeout_s=10)
        res = await cli.market_like_limit("ACC3", "rb2701", Direction.BUY, 5)
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import random
import uuid
from collections import deque
from collections.abc import Awaitable, Callable, Iterable, Sequence
from dataclasses import dataclass, field
from typing import Any, NamedTuple

import msgspec
import zmq
import zmq.asyncio
from loguru import logger

from futrisk.bus.endpoints import MD_GATEWAY, Endpoints
from futrisk.bus.lv import LvTracker
from futrisk.bus.rpc_client import AsyncRpcClient
from futrisk.bus.subscriber import AsyncSubscriber
from futrisk.client.orders import CANCELLABLE_STATUSES, WORKING_STATUSES, FillDelta, OrderBook, TrackedOrder
from futrisk.client.positions import PositionTracker, close_direction
from futrisk.client.pricing import ContractTable, aggressive_price
from futrisk.client.riskview import RiskUpdate, RiskView
from futrisk.client.routing import ShardRouter, UnknownAccountError
from futrisk.common.clock import Clock, SystemClock
from futrisk.common.config import FullConfig
from futrisk.common.config.accounts import AccountsConfig
from futrisk.common.config.products import ProductsConfig
from futrisk.common.config.services import ServicesConfig
from futrisk.common.ids import uuid7
from futrisk.messages import codes, topics
from futrisk.messages.base import decode
from futrisk.messages.common import Ping, Pong, ReplayBatch, ReplayRequest, SnapshotBatch, SnapshotRequest
from futrisk.messages.enums import Direction, Offset, OrderSource, OrderType, PosDirection, TimeInForce
from futrisk.messages.envelope import Envelope, Reply, decode_envelope
from futrisk.messages.md import FLAG_WM_RESET, Tick
from futrisk.messages.orders import (
    CancelAllAck,
    CancelAllRequest,
    CancelRequest,
    OrderAck,
    OrderRequest,
    OrderUpdate,
)
from futrisk.messages.risk import (
    AccountSnapshot,
    AccountSnapshotBatch,
    AccountStateChanged,
    OrderBatch,
    PositionBatch,
    PositionView,
    QueryAccounts,
    QueryOrders,
    QueryPositions,
)
from futrisk.runtime.loop import ensure_selector_loop

DEFAULT_IDENTITY = "strategy-sim"
JOURNAL_MAX = 5_000  # fills kept for re-application after a position seed (per account)

TickCallback = Callable[[Tick], Awaitable[None] | None]
RiskCallback = Callable[[RiskUpdate], Awaitable[None] | None]


def coid_marker(tag: str) -> int:
    """16-bit marker of an order-id tag (stored in the low bits of UUIDv7 ``client_order_id`` values)."""
    return int(hashlib.sha256(tag.encode("utf-8")).hexdigest()[:4], 16)


def tagged_uuid7(marker: int) -> str:
    """A UUIDv7 whose last 16 random bits carry ``marker`` (still a valid, time-ordered v7)."""
    value = uuid.UUID(uuid7()).int
    return str(uuid.UUID(int=(value & ~0xFFFF) | (marker & 0xFFFF)))


def has_marker(client_order_id: str, marker: int) -> bool:
    if len(client_order_id) != 36:
        return False
    try:
        return int(client_order_id[-4:], 16) == marker
    except ValueError:
        return False


class TickClock:
    """Market time for client stamps: the wall clock plus the offset seen on the latest tick.

    The demo market clock may be offset from the wall clock (01 §8.2); ticks carry market
    time (``exch_ts_ns``), so ``sent_ns`` and request envelopes follow the market clock
    without a subscription to ``sys.clock``. Never goes backwards; monotonic time is the
    wall clock's.
    """

    def __init__(self, wall: Clock | None = None) -> None:
        self._wall = wall or SystemClock()
        self._offset = 0
        self._last = 0

    @property
    def offset_ns(self) -> int:
        return self._offset

    def observe(self, market_ns: int) -> None:
        self._offset = market_ns - self._wall.now_ns()

    def now_ns(self) -> int:
        value = self._wall.now_ns() + self._offset
        if value < self._last:
            value = self._last
        self._last = value
        return value

    def monotonic_ns(self) -> int:
        return self._wall.monotonic_ns()


@dataclass(frozen=True, slots=True)
class _JournalFill:
    """A fill applied to the tracker after the account's position snapshot (see the module docstring)."""

    ts_ns: int
    instrument_id: str
    direction: Direction
    offset: Offset
    qty: int
    price: float
    trading_day: str | None


@dataclass(frozen=True, slots=True)
class OrderEvent:
    """One applied ``OrderUpdate`` with the resulting order state and the new fill (if any)."""

    update: OrderUpdate
    order: TrackedOrder
    fill: FillDelta | None = None
    replayed: bool = False

    @property
    def account_id(self) -> str:
        return self.update.account_id


OrderCallback = Callable[[OrderEvent], Awaitable[None] | None]


class OrderResult(NamedTuple):
    """Outcome of a helper that submits one order."""

    request: OrderRequest
    reply: Reply
    ack: OrderAck | None

    @property
    def accepted(self) -> bool:
        return self.reply.ok and self.ack is not None and self.ack.accepted

    @property
    def code(self) -> str:
        if self.ack is not None and self.ack.reject_code:
            return self.ack.reject_code
        return self.reply.code

    @property
    def client_order_id(self) -> str:
        return self.request.client_order_id


@dataclass
class BuildLeg:
    """Result of one leg of :meth:`FutriskClient.build_position`."""

    instrument_id: str
    direction: Direction
    requested: int
    filled: int = 0
    orders: list[str] = field(default_factory=list)
    rejects: list[str] = field(default_factory=list)

    @property
    def complete(self) -> bool:
        return self.filled >= self.requested


@dataclass
class BuildResult:
    account_id: str
    legs: list[BuildLeg]

    @property
    def complete(self) -> bool:
        return all(leg.complete for leg in self.legs)


def _call(cb: Callable[[Any], Awaitable[None] | None], arg: Any) -> Awaitable[None] | None:
    return cb(arg)


class FutriskClient:
    """Async strategy client; see the module docstring."""

    def __init__(
        self,
        config: FullConfig | None = None,
        *,
        accounts: AccountsConfig | None = None,
        services: ServicesConfig | None = None,
        products: ProductsConfig | None = None,
        identity: str = DEFAULT_IDENTITY,
        clock: Clock | None = None,
        ctx: zmq.asyncio.Context | None = None,
        rng: random.Random | None = None,
        order_timeout_ms: int | None = None,
        max_retries: int | None = None,
        repair_gaps: bool = True,
        coid_tag: str | None = None,
    ) -> None:
        accounts = accounts or (config.accounts if config is not None else None)
        services = services or (config.services if config is not None else None)
        products = products or (config.products if config is not None else None)
        if accounts is None or services is None or products is None:
            raise ValueError("FutriskClient needs a FullConfig or accounts + services + products")
        self.identity = identity
        self.router = ShardRouter(accounts, services)
        self.endpoints = Endpoints(services)
        self.contracts = ContractTable(products)
        self.positions = PositionTracker(self.contracts)
        self.orders = OrderBook()
        self._accounts_cfg = accounts
        self._clock = clock or SystemClock()
        self.market_clock = TickClock(self._clock)
        self.coid_tag = coid_tag
        self._coid_marker = coid_marker(coid_tag) if coid_tag else None
        self._ctx_arg = ctx
        self._ctx: zmq.asyncio.Context | None = None
        self._own_ctx = False
        self._rng = rng or random.Random()
        self._order_timeout_ms = order_timeout_ms
        self._max_retries = max_retries
        self._repair_gaps = repair_gaps
        self._rpc: AsyncRpcClient | None = None
        self._md_sub: AsyncSubscriber | None = None
        self._oms_sub: AsyncSubscriber | None = None
        self._tasks: set[asyncio.Task[Any]] = set()
        self._started = False
        self._closed = False
        self._changed: asyncio.Condition | None = None
        # subscriptions
        self._tick_subs: list[tuple[frozenset[str] | None, TickCallback]] = []
        self._tick_topics: set[str] = set()
        self._order_subs: list[tuple[frozenset[str], OrderCallback]] = []
        self._order_accounts: set[str] = set()
        # market data state (LV)
        self._lv = LvTracker()
        self._last: dict[str, Tick] = {}
        self._last_key: dict[str, tuple[str, int, int]] = {}
        self.ticks_received = 0
        self.last_tick_mono_ns: int | None = None
        # FACT sequence per (src, topic): (stream, expected)
        self._fact: dict[tuple[str, str], tuple[str | None, int]] = {}
        self._repairs: set[tuple[str, str, int]] = set()
        self.gaps_detected = 0
        self.duplicates_dropped = 0
        self.resync_wanted: set[str] = set()
        # risk view (risk.state / risk.snapshot)
        self._risk_subs: list[tuple[frozenset[str], RiskCallback]] = []
        self._risk_accounts: set[str] = set()
        self._risk: dict[str, RiskView] = {}
        self._risk_lv = LvTracker()
        self.clones_done: set[str] = set()
        # position snapshot rule (engine market time of the last seed per account) and fill journal
        self._snap_ns: dict[str, int] = {}
        self._journal: dict[str, deque[_JournalFill]] = {}

    # ---------------------------------------------------------------- lifecycle
    async def start(self) -> None:
        """Create the sockets and reader tasks (idempotent). Needs a selector event loop."""
        if self._started:
            return
        loop = asyncio.get_running_loop()
        ensure_selector_loop(loop)
        if self._ctx_arg is not None:
            self._ctx = self._ctx_arg
        else:
            self._ctx = zmq.asyncio.Context()
            self._own_ctx = True
        self._changed = asyncio.Condition()
        self._rpc = AsyncRpcClient(
            self._ctx, self.endpoints, identity=self.identity, clock=self.market_clock, rng=self._rng
        )
        self._md_sub = AsyncSubscriber(self._ctx, name="client-md")
        self._oms_sub = AsyncSubscriber(self._ctx, name="client-oms")
        self._started = True
        self._connect_md(self._tick_topics)
        self._connect_oms(self._order_accounts)
        self._connect_risk(self._risk_accounts)
        self._spawn(self._md_loop(), "md-reader")
        self._spawn(self._oms_loop(), "oms-reader")
        self._schedule_clone(self._risk_accounts)
        logger.info("client.started", identity=self.identity)

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        tasks = list(self._tasks)
        for t in tasks:
            t.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._tasks.clear()
        if self._rpc is not None:
            await self._rpc.close()
        for sub in (self._md_sub, self._oms_sub):
            if sub is not None:
                sub.close()
        if self._own_ctx and self._ctx is not None:
            self._ctx.destroy(linger=0)
        logger.info("client.closed", identity=self.identity)

    async def __aenter__(self) -> FutriskClient:
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    def _spawn(self, coro: Awaitable[Any], name: str) -> asyncio.Task[Any]:
        task: asyncio.Task[Any] = asyncio.ensure_future(coro)
        task.set_name(f"futrisk-client:{name}")
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    @property
    def rpc(self) -> AsyncRpcClient:
        if self._rpc is None:
            raise RuntimeError("FutriskClient not started")
        return self._rpc

    def now_ns(self) -> int:
        return self._clock.now_ns()

    # ---------------------------------------------------------------- subscriptions
    def subscribe_ticks(self, instruments: Iterable[str] | None, callback: TickCallback | None = None) -> None:
        """Receive ``md.tick.*`` from md-gateway: all instruments (``None``) or exact ones."""
        wanted = None if instruments is None else frozenset(instruments)
        new_topics = {topics.MD_TICK} if wanted is None else {topics.md_tick(i) for i in wanted}
        if callback is not None:
            self._tick_subs.append((wanted, callback))
        added = new_topics - self._tick_topics
        self._tick_topics |= added
        if self._started and added:
            self._connect_md(added)

    def subscribe_orders(self, accounts: Iterable[str], callback: OrderCallback | None = None) -> None:
        """Receive ``oms.order.{account}`` from each account's shard (exact topics, never a prefix)."""
        accts = frozenset(accounts)
        for a in accts:
            self.router.instance_of(a)  # UnknownAccountError early
        if callback is not None:
            self._order_subs.append((accts, callback))
        added = set(accts) - self._order_accounts
        self._order_accounts |= added
        if self._started and added:
            self._connect_oms(added)

    def subscribe_risk(self, accounts: Iterable[str], callback: RiskCallback | None = None) -> None:
        """Follow ``risk.state.{account}`` + ``risk.snapshot.{account}`` of the owning shards (02 §5).

        A ``ctl.snapshot`` of both topics is requested at once (Clone, 02 §4.6), so the view is
        populated without waiting for the next change; ``callback`` receives a
        :class:`~futrisk.client.riskview.RiskUpdate` for every applied message.
        """
        accts = frozenset(accounts)
        for a in accts:
            self.router.instance_of(a)  # UnknownAccountError early
            self._risk.setdefault(a, RiskView(a))
        if callback is not None:
            self._risk_subs.append((accts, callback))
        added = set(accts) - self._risk_accounts
        self._risk_accounts |= added
        if self._started and added:
            self._connect_risk(added)
            self._schedule_clone(added)

    def risk_view(self, account_id: str) -> RiskView | None:
        return self._risk.get(account_id)

    def _connect_md(self, subs: Iterable[str]) -> None:
        subs = list(subs)
        if self._md_sub is None or not subs:
            return
        self._md_sub.connect(MD_GATEWAY, self.endpoints.pub(MD_GATEWAY), subs)

    def _connect_oms(self, accounts: Iterable[str]) -> None:
        if self._oms_sub is None:
            return
        for a in sorted(accounts):
            instance = self.router.instance_of(a)
            self._oms_sub.connect(instance, self.endpoints.pub(instance), [topics.oms_order(a)])

    def _connect_risk(self, accounts: Iterable[str]) -> None:
        if self._oms_sub is None:
            return
        for a in sorted(accounts):
            instance = self.router.instance_of(a)
            self._oms_sub.connect(instance, self.endpoints.pub(instance), [topics.risk_state(a), topics.risk_snapshot(a)])

    def _schedule_clone(self, accounts: Iterable[str]) -> None:
        by_instance: dict[str, list[str]] = {}
        for a in sorted(accounts):
            by_instance.setdefault(self.router.instance_of(a), []).append(a)
        for instance, accts in by_instance.items():
            self._spawn(self._clone(instance, accts), f"clone-{instance}")

    async def _clone(self, instance: str, accounts: list[str]) -> None:
        """Start-up ``ctl.snapshot`` of the accounts' risk topics; retried until it succeeds."""
        wanted = {t for a in accounts for t in (topics.risk_state(a), topics.risk_snapshot(a))}
        delay = 0.5
        cursor: str | None = None
        while not self._closed:
            req = SnapshotRequest(prefixes=sorted(wanted), next_cursor=cursor)
            reply = await self.rpc.request(instance, "ctl.snapshot", req, retries=0)
            batch: SnapshotBatch | None = _open(reply, SnapshotBatch) if reply.ok else None
            if batch is None:
                logger.debug("client.clone_retry", instance=instance, code=reply.code)
                await asyncio.sleep(delay)
                delay = min(delay * 2, 5.0)
                continue
            for item in batch.items:
                try:
                    env = decode_envelope(item.envelope)
                except (msgspec.DecodeError, msgspec.ValidationError):
                    continue
                if env.topic in wanted:
                    await self._on_risk_env(env, source="clone")
            if batch.next_cursor:
                cursor = batch.next_cursor
                continue
            self.clones_done.update(accounts)
            logger.info("client.risk_cloned", instance=instance, accounts=accounts, items=len(batch.items))
            return

    # ---------------------------------------------------------------- market data
    def last_tick(self, instrument_id: str) -> Tick | None:
        return self._last.get(instrument_id)

    def seconds_since_last_tick(self) -> float | None:
        """Monotonic seconds since the last accepted tick (``None`` before the first one)."""
        if self.last_tick_mono_ns is None:
            return None
        return (self._clock.monotonic_ns() - self.last_tick_mono_ns) / 1e9

    def ticks(self) -> dict[str, Tick]:
        return dict(self._last)

    async def _recv(self, sub: AsyncSubscriber) -> Envelope | None:
        """Next envelope; socket errors are logged and retried (``None``) so the reader never dies."""
        try:
            return await sub.recv()
        except asyncio.CancelledError:
            raise
        except Exception:
            if self._closed:
                raise
            logger.exception("client.recv_failed", sub=sub.name)
            await asyncio.sleep(0.1)
            return None

    async def _md_loop(self) -> None:
        assert self._md_sub is not None
        while True:
            env = await self._recv(self._md_sub)
            if env is None:
                continue
            try:
                tick = self._accept_tick(env)
                if tick is None:
                    continue
                for wanted, cb in list(self._tick_subs):
                    if wanted is not None and tick.instrument_id not in wanted:
                        continue
                    try:
                        res = _call(cb, tick)
                        if res is not None:
                            await res
                    except Exception:
                        logger.exception("client.tick_callback_failed", instrument_id=tick.instrument_id)
                await self._notify()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("client.md_message_failed", topic=env.topic)

    def _accept_tick(self, env: Envelope) -> Tick | None:
        if env.type != "Tick" or not env.topic.startswith(topics.MD_TICK):
            return None
        if not self._lv.on_lv(env).apply:
            self.duplicates_dropped += 1
            return None
        try:
            tick = decode(env.payload, Tick)
        except (msgspec.DecodeError, msgspec.ValidationError):
            logger.warning("client.bad_tick", topic=env.topic)
            return None
        if topics.md_tick(tick.instrument_id) != env.topic:
            return None
        key = tick.order_key
        prev = self._last_key.get(tick.instrument_id)
        if prev is not None and key <= prev and not tick.flags & FLAG_WM_RESET:
            self.duplicates_dropped += 1
            return None  # 01 §8.13: order_key strictly increases per instrument (except after a watermark reset)
        self._last_key[tick.instrument_id] = key
        self._last[tick.instrument_id] = tick
        self.ticks_received += 1
        self.last_tick_mono_ns = self._clock.monotonic_ns()
        self.market_clock.observe(tick.exch_ts_ns)
        self.positions.roll_to(tick.trading_day)
        return tick

    async def wait_ticks(self, instruments: Iterable[str], timeout_s: float = 10.0) -> bool:
        """Wait until every instrument has a tick (subscribes them if needed)."""
        wanted = list(instruments)
        missing = [i for i in wanted if topics.md_tick(i) not in self._tick_topics and topics.MD_TICK not in self._tick_topics]
        if missing:
            self.subscribe_ticks(missing)
        return await self._wait(lambda: all(i in self._last for i in wanted), timeout_s)

    # ---------------------------------------------------------------- order updates
    async def _oms_loop(self) -> None:
        assert self._oms_sub is not None
        while True:
            env = await self._recv(self._oms_sub)
            if env is None:
                continue
            try:
                if env.topic.startswith(topics.OMS_ORDER):
                    await self._on_order_env(env, replayed=False)
                elif env.topic.startswith((topics.RISK_STATE, topics.RISK_SNAPSHOT)):
                    await self._on_risk_env(env, source="live")
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("client.order_message_failed", topic=env.topic)

    async def _on_risk_env(self, env: Envelope, *, source: str) -> None:
        """Apply ``risk.state`` / ``risk.snapshot`` to the account's :class:`RiskView` (02 §5 rule 2)."""
        snap: AccountSnapshot | None = None
        try:
            if env.type == "AccountStateChanged" and env.topic.startswith(topics.RISK_STATE):
                msg = decode(env.payload, AccountStateChanged)
                account_id = msg.account_id
                if env.topic != topics.risk_state(account_id) or account_id not in self._risk_accounts:
                    return
                view = self._risk[account_id]
                changed = view.apply_state(msg)
                kind = "state" if source == "live" else source
                ts = msg.ts_ns
            elif env.type == "AccountSnapshot" and env.topic.startswith(topics.RISK_SNAPSHOT):
                if source == "live" and not self._risk_lv.on_lv(env).apply:
                    return
                snap = decode(env.payload, AccountSnapshot)
                account_id = snap.account_id
                if env.topic != topics.risk_snapshot(account_id) or account_id not in self._risk_accounts:
                    return
                view = self._risk[account_id]
                taken, changed = view.apply_snapshot(snap)
                if not taken:
                    return
                kind = "snapshot" if source == "live" else source
                ts = snap.as_of_ns
            else:
                return
        except (msgspec.DecodeError, msgspec.ValidationError):
            logger.warning("client.bad_risk_message", topic=env.topic, type=env.type)
            return
        if changed:
            logger.info("client.risk_state", account_id=account_id, state=str(view.state), latched=view.halted_latched,
                        restrictions=[(str(r.kind), r.scope_key, r.value) for r in view.restrictions], source=kind)
        await self._dispatch_risk(RiskUpdate(account_id, view, kind, changed, snap, ts))

    async def _dispatch_risk(self, update: RiskUpdate) -> None:
        for accts, cb in list(self._risk_subs):
            if update.account_id not in accts:
                continue
            try:
                res = _call(cb, update)
                if res is not None:
                    await res
            except Exception:
                logger.exception("client.risk_callback_failed", account_id=update.account_id)
        await self._notify()

    def _fact_check(self, env: Envelope) -> tuple[bool, tuple[int, int] | None]:
        """02 §4.5: apply ``seq == expected``; drop ``seq < expected``; gap on ``seq > expected``."""
        key = (env.src, env.topic)
        st = self._fact.get(key)
        if st is None or st[0] != env.stream:
            if st is not None:
                logger.warning("client.fact_stream_changed", src=env.src, topic=env.topic)
                acct = env.topic[len(topics.OMS_ORDER):]
                self.resync_wanted.add(acct)
            self._fact[key] = (env.stream, env.seq + 1)
            return True, None
        stream, expected = st
        if env.seq < expected:
            return False, None
        self._fact[key] = (stream, env.seq + 1)
        if env.seq > expected:
            return True, (expected, env.seq - 1)
        return True, None

    async def _on_order_env(self, env: Envelope, *, replayed: bool) -> None:
        if env.type != "OrderUpdate" or not env.topic.startswith(topics.OMS_ORDER):
            return
        if not replayed:
            ok, gap = self._fact_check(env)
            if not ok:
                self.duplicates_dropped += 1
                return
            if gap is not None:
                self.gaps_detected += 1
                logger.warning("client.fact_gap", src=env.src, topic=env.topic, lo=gap[0], hi=gap[1])
                if self._repair_gaps:
                    self._spawn(self._repair(env.src, env.topic, gap[0], gap[1]), f"repair-{env.topic}")
                else:
                    self.resync_wanted.add(env.topic[len(topics.OMS_ORDER):])
        try:
            upd = decode(env.payload, OrderUpdate)
        except (msgspec.DecodeError, msgspec.ValidationError):
            logger.warning("client.bad_order_update", topic=env.topic)
            return
        if topics.oms_order(upd.account_id) != env.topic:
            logger.warning("client.order_topic_mismatch", topic=env.topic, account_id=upd.account_id)
            return
        await self._apply_update(upd, replayed=replayed)

    def _position_lots(self, upd: OrderUpdate, fill: FillDelta) -> int:
        """Lots of a new fill that the position tracker must still apply (the snapshot rule)."""
        seeded = self._snap_ns.get(upd.account_id)
        if seeded is None:
            return fill.qty
        if fill.ts_ns <= seeded:
            return 0  # already part of the position snapshot
        if fill.first_seen:
            if fill.last_qty is None:
                self.resync_wanted.add(upd.account_id)  # cannot tell which fills are new: resync
                return 0
            return min(fill.qty, fill.last_qty)  # only the latest fill can postdate the snapshot
        return fill.qty

    def _record_fill(self, upd: OrderUpdate, fill: FillDelta, qty: int) -> None:
        td = upd.trading_day or None
        self.positions.apply_fill(upd.account_id, upd.instrument_id, upd.direction, upd.offset, qty, fill.price,
                                  trading_day=td)
        if upd.account_id in self._snap_ns:
            j = self._journal.setdefault(upd.account_id, deque(maxlen=JOURNAL_MAX))
            j.append(_JournalFill(fill.ts_ns, upd.instrument_id, upd.direction, upd.offset, qty, fill.price, td))

    def seed_positions(self, account_id: str, views: Iterable[PositionView], as_of_ns: int | None) -> None:
        """Replace the account's positions with an engine snapshot taken at ``as_of_ns`` (engine time).

        Journaled fills newer than the snapshot are re-applied; later fills are counted only
        when they happened after ``as_of_ns``. Without a time (``risk.query.positions``) every
        later fill is counted.
        """
        self.positions.seed(account_id, views)
        journal = self._journal.get(account_id)
        if as_of_ns is None:
            self._snap_ns.pop(account_id, None)
            if journal:
                journal.clear()
        else:
            self._snap_ns[account_id] = as_of_ns
            newer = [f for f in journal or () if f.ts_ns > as_of_ns]
            if journal is not None:
                journal.clear()
            for f in newer:
                self.positions.apply_fill(account_id, f.instrument_id, f.direction, f.offset, f.qty, f.price,
                                          trading_day=f.trading_day)
                self._journal.setdefault(account_id, deque(maxlen=JOURNAL_MAX)).append(f)
        self.resync_wanted.discard(account_id)

    def seed_from_snapshot(self, snap: AccountSnapshot) -> None:
        """Re-seed positions from a ``risk.snapshot`` / ``risk.query.accounts`` result."""
        self.seed_positions(snap.account_id, snap.positions, snap.as_of_ns)

    async def _apply_update(self, upd: OrderUpdate, *, replayed: bool = False, baseline: bool = False) -> OrderEvent:
        order, fill = self.orders.apply_update(upd, baseline=baseline)
        if fill is not None:
            lots = self._position_lots(upd, fill)
            if lots > 0:
                self._record_fill(upd, fill, lots)
        event = OrderEvent(update=upd, order=order, fill=fill, replayed=replayed)
        for accts, cb in list(self._order_subs):
            if upd.account_id not in accts:
                continue
            try:
                res = _call(cb, event)
                if res is not None:
                    await res
            except Exception:
                logger.exception("client.order_callback_failed", client_order_id=upd.client_order_id)
        await self._notify()
        return event

    async def _repair(self, src: str, topic: str, lo: int, hi: int) -> None:
        """Fetch ``[lo, hi]`` of a FACT topic with ``ctl.replay`` and apply it (02 §4.6, §8.2)."""
        key = (src, topic, lo)
        if key in self._repairs:
            return
        self._repairs.add(key)
        acct = topic[len(topics.OMS_ORDER):]
        try:
            nxt = lo
            while nxt <= hi:
                reply = await self.rpc.request(src, "ctl.replay", ReplayRequest(topic=topic, from_seq=nxt, to_seq=hi))
                if not reply.ok:
                    logger.warning("client.replay_failed", src=src, topic=topic, code=reply.code, lo=nxt, hi=hi)
                    self.resync_wanted.add(acct)
                    return
                batch = reply.open_as(ReplayBatch)
                for raw in batch.envelopes:
                    renv = decode_envelope(raw)
                    if lo <= renv.seq <= hi and renv.topic == topic:
                        await self._on_order_env(renv, replayed=True)
                if batch.complete or batch.next_seq <= nxt:
                    break
                nxt = batch.next_seq
            logger.info("client.gap_repaired", src=src, topic=topic, lo=lo, hi=hi)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("client.replay_error", src=src, topic=topic)
            self.resync_wanted.add(acct)
        finally:
            self._repairs.discard(key)

    # ---------------------------------------------------------------- waiting
    async def _notify(self) -> None:
        cond = self._changed
        if cond is None:
            return
        async with cond:
            cond.notify_all()

    async def _wait(self, pred: Callable[[], bool], timeout_s: float) -> bool:
        if pred():
            return True
        cond = self._changed
        if cond is None:
            raise RuntimeError("FutriskClient not started")
        try:
            async with cond:
                await asyncio.wait_for(cond.wait_for(pred), timeout_s)
        except TimeoutError:
            return pred()
        return True

    async def wait_order(self, client_order_id: str, *, timeout_s: float = 5.0, filled: int | None = None) -> TrackedOrder | None:
        """Wait until the order is terminal (or has at least ``filled`` lots); returns its state."""

        def done() -> bool:
            o = self.orders.get(client_order_id)
            if o is None:
                return False
            if filled is not None and o.filled_qty >= filled:
                return True
            return o.terminal

        await self._wait(done, timeout_s)
        return self.orders.get(client_order_id)

    # ---------------------------------------------------------------- commands
    def new_order(
        self,
        account_id: str,
        instrument_id: str,
        direction: Direction | str,
        offset: Offset | str,
        qty: int,
        price: float | None,
        *,
        order_type: OrderType = OrderType.LIMIT,
        tif: TimeInForce = TimeInForce.GFD,
        source: OrderSource = OrderSource.BOT,
        strategy_tag: str | None = None,
        actor: str | None = None,
    ) -> OrderRequest:
        """Build an ``OrderRequest`` with a fresh UUIDv7 ``client_order_id`` (own marker when
        ``coid_tag`` is set) and ``sent_ns`` = market time (:class:`TickClock`)."""
        return OrderRequest(
            client_order_id=self.new_client_order_id(),
            account_id=account_id,
            instrument_id=instrument_id,
            direction=Direction(direction),
            offset=Offset(offset),
            order_type=order_type,
            tif=tif,
            price=price,
            qty=qty,
            source=source,
            strategy_tag=strategy_tag,
            actor=actor,
            sent_ns=self.market_clock.now_ns(),
        )

    def new_client_order_id(self) -> str:
        return tagged_uuid7(self._coid_marker) if self._coid_marker is not None else uuid7()

    def owns_coid(self, client_order_id: str) -> bool:
        """True when ``client_order_id`` carries this client's ``coid_tag`` marker."""
        return self._coid_marker is not None and has_marker(client_order_id, self._coid_marker)

    def is_own(self, order: OrderUpdate | TrackedOrder) -> bool:
        """An order of this strategy process (any incarnation): source BOT and the own id marker.

        Manual (``MANUAL``/``API``) and liquidation orders are never own orders.
        """
        return order.source == OrderSource.BOT and self.owns_coid(order.client_order_id)

    async def submit(self, req: OrderRequest) -> tuple[Reply, OrderAck | None]:
        """``order.submit`` to the owning shard (02 §6.3); returns ``(Reply, OrderAck | None)``.

        Rejections come back as ``ok=False`` with the reject code and still carry an
        ``OrderAck``. Transport failures after the retries leave the order in doubt in
        :attr:`orders`; resolve it with :meth:`resubmit` (same ``client_order_id``).
        """
        self.orders.add_request(req)
        try:
            target = self.router.instance_of(req.account_id)
        except UnknownAccountError as exc:
            reply = Reply(ok=False, code=codes.REJECT_UNKNOWN_ACCOUNT, message=str(exc))
            self.orders.on_submit_reply(req.client_order_id, reply, None)
            return reply, None
        reply = await self.rpc.request(
            target, "order.submit", req, timeout_ms=self._order_timeout_ms, retries=self._max_retries
        )
        ack = _open(reply, OrderAck)
        self.orders.on_submit_reply(req.client_order_id, reply, ack)
        await self._notify()
        if not reply.ok:
            logger.debug(
                "client.submit_rejected",
                client_order_id=req.client_order_id,
                account_id=req.account_id,
                code=reply.code,
                duplicate=reply.duplicate,
            )
        return reply, ack

    async def resubmit(self, client_order_id: str) -> tuple[Reply, OrderAck | None]:
        """Re-send an own order with the same id and payload (idempotent on the server)."""
        o = self.orders.get(client_order_id)
        if o is None or o.request is None:
            return Reply(ok=False, code=codes.NOT_FOUND, message=f"unknown own order {client_order_id}"), None
        return await self.submit(o.request)

    async def cancel(
        self,
        account_id: str,
        client_order_id: str,
        *,
        reason: str | None = None,
        command_id: str | None = None,
        timeout_ms: int | None = None,
        retries: int | None = None,
    ) -> tuple[Reply, OrderUpdate | None]:
        """``order.cancel`` (idempotency key ``command_id``, new UUIDv7 by default)."""
        req = CancelRequest(
            command_id=command_id or uuid7(), client_order_id=client_order_id, account_id=account_id, reason=reason
        )
        o = self.orders.get(client_order_id)
        if o is not None:
            o.cancel_requested = True
        try:
            target = self.router.instance_of(account_id)
        except UnknownAccountError as exc:
            return Reply(ok=False, code=codes.REJECT_UNKNOWN_ACCOUNT, message=str(exc)), None
        reply = await self.rpc.request(target, "order.cancel", req, timeout_ms=timeout_ms,
                                       retries=self._max_retries if retries is None else retries)
        upd = _open(reply, OrderUpdate)
        if upd is not None:
            await self._apply_update(upd)
        elif o is not None and not reply.ok and reply.code != codes.CONFLICT:
            o.cancel_requested = False  # allow another attempt
        return reply, upd

    async def cancel_all(
        self,
        account_id: str,
        *,
        scope: str,
        product: str | None = None,
        reason: str = "",
        actor: str | None = None,
        command_id: str | None = None,
        timeout_ms: int | None = None,
        retries: int | None = None,
    ) -> tuple[Reply, CancelAllAck | None]:
        """``order.cancel_all`` for one account (``scope`` ``ALL`` or ``OPEN_ONLY``, no default).

        It cancels **every** order of the account - also manual and liquidation orders -
        so strategies use :meth:`cancel_own_orders`; this is an operator tool.
        """
        req = CancelAllRequest(
            command_id=command_id or uuid7(),
            account_id=account_id,
            scope=scope,
            product=product,
            reason=reason,
            actor=actor or self.identity,
        )
        try:
            target = self.router.instance_of(account_id)
        except UnknownAccountError as exc:
            return Reply(ok=False, code=codes.REJECT_UNKNOWN_ACCOUNT, message=str(exc)), None
        reply = await self.rpc.request(
            target, "order.cancel_all", req, timeout_ms=timeout_ms,
            retries=self._max_retries if retries is None else retries,
        )
        return reply, _open(reply, CancelAllAck)

    async def query_orders(self, account_id: str, trading_day: str, *, working_only: bool = False,
                           timeout_ms: int = 2_000) -> list[OrderUpdate] | None:
        """``risk.query.orders`` of the account for ``trading_day`` (``None`` on failure)."""
        status = sorted(WORKING_STATUSES) if working_only else None
        reply = await self.rpc.request(
            self.router.instance_of(account_id), "risk.query.orders",
            QueryOrders(account_id=account_id, trading_day=trading_day, status=status), timeout_ms=timeout_ms, retries=1,
        )
        batch: OrderBatch | None = _open(reply, OrderBatch) if reply.ok else None
        if batch is None:
            logger.warning("client.query_orders_failed", account_id=account_id, code=reply.code)
            return None
        return list(batch.items)

    async def cancel_own_orders(self, account_id: str, trading_day: str, *, reason: str = "") -> list[str] | None:
        """Cancel the working orders of this strategy process (:meth:`is_own`), one by one.

        Orders of other origins (manual, liquidation, other clients) are left alone. Returns the
        ids whose cancel was accepted, or ``None`` when the order query failed.
        """
        items = await self.query_orders(account_id, trading_day, working_only=True)
        if items is None:
            return None
        mine = [u for u in items if self.is_own(u) and u.status in CANCELLABLE_STATUSES]
        done: list[str] = []
        for u in mine:
            if self.orders.get(u.client_order_id) is None:
                self.orders.apply_update(u, baseline=True)  # a predecessor's order: fills already in the snapshot
            reply, _ = await self.cancel(account_id, u.client_order_id, reason=reason or "strategy-sim")
            if reply.ok:
                done.append(u.client_order_id)
        logger.info("client.own_orders_cancelled", account_id=account_id, cancelled=len(done), own=len(mine),
                    left_alone=len(items) - len(mine))
        return done

    async def ping(self, instance: str, *, timeout_ms: int = 1_000) -> tuple[Reply, Pong | None]:
        """``ctl.ping`` (liveness and self-reported state)."""
        reply = await self.rpc.request(instance, "ctl.ping", Ping(), timeout_ms=timeout_ms, retries=0)
        return reply, _open(reply, Pong)

    async def ping_shards(self, accounts: Iterable[str] | None = None, *, timeout_ms: int = 1_000) -> dict[str, Pong | None]:
        """Ping every shard owning ``accounts``; ``None`` for shards that did not answer."""
        instances = self.router.instances(accounts)
        results = await asyncio.gather(*(self.ping(i, timeout_ms=timeout_ms) for i in instances))
        return {i: pong for i, (_, pong) in zip(instances, results, strict=True)}

    # ---------------------------------------------------------------- read-only queries (start-up sync)
    async def sync_positions(self, account_id: str, *, timeout_ms: int = 2_000) -> bool:
        """Seed the tracker from ``risk.query.positions`` (read-only; best effort)."""
        reply = await self.rpc.request(
            self.router.instance_of(account_id), "risk.query.positions", QueryPositions(account_id=account_id),
            timeout_ms=timeout_ms, retries=1,
        )
        batch: PositionBatch | None = _open(reply, PositionBatch) if reply.ok else None
        if batch is None:
            logger.warning("client.position_sync_failed", account_id=account_id, code=reply.code)
            return False
        self.seed_positions(account_id, batch.items, None)
        logger.info("client.positions_synced", account_id=account_id, positions=self.positions.snapshot(account_id))
        return True

    async def sync_account(self, account_id: str, *, timeout_ms: int = 2_000) -> AccountSnapshot | None:
        """Seed positions (with the snapshot time), equity and the risk view from ``risk.query.accounts``.

        Falls back to :meth:`sync_positions` (no snapshot time) when the account query fails.
        """
        snap = await self.query_account(account_id, timeout_ms=timeout_ms)
        if snap is None:
            await self.sync_positions(account_id, timeout_ms=timeout_ms)
            return None
        self.seed_from_snapshot(snap)
        view = self._risk.setdefault(account_id, RiskView(account_id))
        taken, changed = view.apply_snapshot(snap)
        if taken and account_id in self._risk_accounts:
            await self._dispatch_risk(RiskUpdate(account_id, view, "query", changed, snap, snap.as_of_ns))
        logger.info("client.account_synced", account_id=account_id, positions=self.positions.snapshot(account_id),
                    state=str(snap.state), equity=round(snap.funds.equity))
        return snap

    async def query_account(self, account_id: str, *, timeout_ms: int = 2_000) -> AccountSnapshot | None:
        """``risk.query.accounts`` for one account (equity, state, restrictions); ``None`` on failure."""
        reply = await self.rpc.request(
            self.router.instance_of(account_id), "risk.query.accounts", QueryAccounts(account_ids=[account_id]),
            timeout_ms=timeout_ms, retries=1,
        )
        batch: AccountSnapshotBatch | None = _open(reply, AccountSnapshotBatch) if reply.ok else None
        if batch is None:
            return None
        for snap in batch.items:
            if snap.account_id == account_id:
                return snap
        return None

    async def refresh_orders(self, account_id: str, trading_day: str, *, timeout_ms: int = 2_000) -> int:
        """Apply today's orders from ``risk.query.orders`` (resolves missed updates); returns the count."""
        reply = await self.rpc.request(
            self.router.instance_of(account_id), "risk.query.orders",
            QueryOrders(account_id=account_id, trading_day=trading_day), timeout_ms=timeout_ms, retries=1,
        )
        batch: OrderBatch | None = _open(reply, OrderBatch) if reply.ok else None
        if batch is None:
            return 0
        unknown = 0
        for upd in batch.items:
            known = self.orders.get(upd.client_order_id) is not None
            # 02 §8.3: unknown orders (a predecessor's, or updates never received) only set the
            # baseline - their fills are in the engine's positions - and trigger a resync
            await self._apply_update(upd, replayed=True, baseline=not known)
            unknown += 0 if known else 1
        if unknown:
            self.resync_wanted.add(account_id)
        return len(batch.items)

    # ---------------------------------------------------------------- order helpers
    async def place_limit(
        self,
        account_id: str,
        instrument_id: str,
        direction: Direction | str,
        offset: Offset | str,
        qty: int,
        price: float,
        *,
        tif: TimeInForce = TimeInForce.GFD,
        source: OrderSource = OrderSource.BOT,
        strategy_tag: str | None = None,
    ) -> OrderResult:
        """Submit a LIMIT order at ``price``."""
        req = self.new_order(
            account_id, instrument_id, direction, offset, qty, price, tif=tif, source=source, strategy_tag=strategy_tag
        )
        reply, ack = await self.submit(req)
        return OrderResult(req, reply, ack)

    def marketable_price(self, instrument_id: str, direction: Direction | str, ticks: int = 2) -> float | None:
        """Opposite best ± ``ticks`` ticks within the limits, from the latest tick (``None`` without one)."""
        tick = self._last.get(instrument_id)
        if tick is None:
            return None
        return aggressive_price(tick, Direction(direction), ticks, self.contracts.get(instrument_id).tick_size)

    async def market_like_limit(
        self,
        account_id: str,
        instrument_id: str,
        direction: Direction | str,
        qty: int,
        *,
        offset: Offset | str = Offset.OPEN,
        ticks: int = 2,
        tif: TimeInForce = TimeInForce.GFD,
        source: OrderSource = OrderSource.BOT,
        strategy_tag: str | None = None,
    ) -> OrderResult:
        """Aggressive LIMIT order at the opposite quote ± ``ticks`` ticks (clamped to the limits).

        Without a tick for the instrument the request is not sent and the result carries
        ``Reply(ok=False, code="REJECT_MD_UNAVAILABLE")``.
        """
        price = self.marketable_price(instrument_id, direction, ticks)
        req = self.new_order(
            account_id, instrument_id, direction, offset, qty, price, tif=tif, source=source, strategy_tag=strategy_tag
        )
        if price is None:
            return OrderResult(req, Reply(ok=False, code=codes.REJECT_MD_UNAVAILABLE, message="no tick yet"), None)
        reply, ack = await self.submit(req)
        return OrderResult(req, reply, ack)

    async def close_position(
        self,
        account_id: str,
        instrument_id: str,
        side: PosDirection | str,
        qty: int | None = None,
        *,
        ticks: int = 2,
        prefer_today: bool = False,
        source: OrderSource = OrderSource.BOT,
        strategy_tag: str | None = None,
    ) -> list[OrderResult]:
        """Close ``qty`` lots (default: all closable) of ``side`` with market-like limits.

        On SHFE the close is split into ``CLOSE_YESTERDAY`` / ``CLOSE_TODAY`` orders by the
        tracker (net of working closes); elsewhere one ``CLOSE`` order.
        """
        side = PosDirection(side)
        pending = self.orders.pending_closes(account_id, instrument_id, side)
        want = qty if qty is not None else self.positions.closable(account_id, instrument_id, side, pending)[0]
        plan = self.positions.close_plan(
            account_id, instrument_id, side, want, pending=pending, prefer_today=prefer_today
        )
        out: list[OrderResult] = []
        for offset, n in plan:
            out.append(
                await self.market_like_limit(
                    account_id, instrument_id, close_direction(side), n, offset=offset, ticks=ticks,
                    source=source, strategy_tag=strategy_tag,
                )
            )
        return out

    async def build_position(
        self,
        account_id: str,
        legs: Sequence[tuple[str, Direction | str, int]],
        *,
        ticks: int = 2,
        timeout_s: float = 30.0,
        fill_wait_s: float = 3.0,
        max_rounds: int = 5,
        source: OrderSource = OrderSource.BOT,
        strategy_tag: str | None = "scripted",
    ) -> BuildResult:
        """Open ``[(instrument, direction, qty), ...]`` with market-like limits until filled.

        For scripted end-to-end scenarios (e.g. give ACC3 its long black-sector book before
        a price shock). Each round sends one order for the unfilled remainder, waits up to
        ``fill_wait_s`` for its fills, cancels what rests, and repeats (at most
        ``max_rounds``) until ``timeout_s``. Legs run concurrently. Non-retryable rejects
        stop the leg (the codes are in :attr:`BuildLeg.rejects`).
        """
        self.subscribe_orders([account_id])
        instruments = [inst for inst, _, _ in legs]
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout_s
        await self.wait_ticks(instruments, timeout_s=max(0.1, deadline - loop.time()))
        result = BuildResult(account_id, [BuildLeg(inst, Direction(d), q) for inst, d, q in legs])

        async def run_leg(leg: BuildLeg) -> None:
            rounds = 0
            while leg.filled < leg.requested and rounds < max_rounds and loop.time() < deadline:
                rounds += 1
                res = await self.market_like_limit(
                    account_id, leg.instrument_id, leg.direction, leg.requested - leg.filled,
                    ticks=ticks, source=source, strategy_tag=strategy_tag,
                )
                leg.orders.append(res.client_order_id)
                if not res.accepted:
                    leg.rejects.append(res.code)
                    if res.reply.code in codes.RETRYABLE:
                        await asyncio.sleep(0.2)
                        continue
                    return
                wait = max(0.05, min(fill_wait_s, deadline - loop.time()))
                o = await self.wait_order(res.client_order_id, timeout_s=wait)
                if o is not None and not o.terminal and o.cancellable:
                    await self.cancel(account_id, o.client_order_id, reason="build_position round")
                    o = await self.wait_order(res.client_order_id, timeout_s=min(fill_wait_s, 2.0))
                if (o is None or not o.terminal) and res.ack is not None and res.ack.trading_day:
                    with contextlib.suppress(Exception):
                        await self.refresh_orders(account_id, res.ack.trading_day)
                    o = self.orders.get(res.client_order_id)
                leg.filled += o.filled_qty if o is not None else 0

        await asyncio.gather(*(run_leg(leg) for leg in result.legs))
        logger.info(
            "client.build_position",
            account_id=account_id,
            legs=[(leg.instrument_id, str(leg.direction), leg.requested, leg.filled) for leg in result.legs],
        )
        return result


def _open(reply: Reply, cls: type[Any]) -> Any:
    """Decode ``reply.data`` as ``cls`` when it carries that type (``None`` otherwise)."""
    if reply.data is None or reply.data_type != cls.__name__:
        return None
    try:
        return reply.open_as(cls)
    except (msgspec.DecodeError, msgspec.ValidationError):
        logger.warning("client.bad_reply_data", data_type=reply.data_type)
        return None


__all__ = [
    "DEFAULT_IDENTITY",
    "BuildLeg",
    "BuildResult",
    "FutriskClient",
    "OrderCallback",
    "OrderEvent",
    "OrderResult",
    "TickCallback",
]
