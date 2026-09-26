"""Test doubles for strategy-side code: a scripted risk-engine shard and an md-gateway publisher.

Real sockets on loopback, real envelopes (02 §4.2), no services::

    from futrisk.client.testing import FakeBus, make_tick, reject
    from futrisk.runtime.testing import config_with_free_ports

    cfg = config_with_free_ports(load_default_config(), ["md-gateway", "risk-engine-0", "risk-engine-1"])
    async with FakeBus(cfg) as bus, FutriskClient(cfg) as cli:
        cli.subscribe_ticks(["rb2701"], on_tick)
        await bus.md.wait_subscribed("md.tick.rb2701")
        await bus.md.tick(make_tick("rb2701", 3120.0, ts_ns=T0))
        bus.shard("risk-engine-1").script.append(reject("REJECT_ACCOUNT_HALTED"))

- :class:`FakePublisher` uses an XPUB socket, so tests can wait until a subscription
  has reached the publisher instead of guessing around the ZeroMQ slow-joiner.
- :class:`FakeShard` serves ``ctl.ping``, ``order.submit`` / ``order.cancel`` /
  ``order.cancel_all``, ``ctl.replay``, ``ctl.snapshot`` and ``risk.query.positions`` /
  ``risk.query.orders`` / ``risk.query.accounts`` behind the real ROUTER whitelist,
  publishes ``oms.order.{account}`` and ``risk.state.{account}`` FACTs (seq + stream) and
  ``risk.snapshot.{account}`` LVs, keeps a last-value cache for ``ctl.snapshot`` and can fill
  accepted orders by itself (``fill_mode``). ``clock_offset_ns`` stamps every engine time
  (``created_ns``/``updated_ns``/``trade_ts_ns``/``as_of_ns``) with a demo market clock.
- ``script`` holds one-shot replies for the next ``order.submit`` requests: a
  :class:`~futrisk.messages.envelope.Reply`, a callable ``(OrderRequest) -> Reply | None``
  (``None`` = default handling) or :data:`NO_REPLY` (the client times out);
  ``reject_opens = "REJECT_ACCOUNT_HALTED"`` rejects every opening order while closes pass
  (02 §11.2 close exemption).
"""

from __future__ import annotations

import asyncio
import contextlib
from collections import deque
from collections.abc import Callable
from decimal import Decimal
from typing import Any

import zmq
import zmq.asyncio

from futrisk.bus.rpc_common import DEFERRED, RequestContext
from futrisk.bus.rpc_server import AsyncRpcServer
from futrisk.bus.whitelist import Whitelist
from futrisk.common.clock import SystemClock
from futrisk.common.config import FullConfig
from futrisk.common.config.products import product_code
from futrisk.messages import codes, topics
from futrisk.messages.base import Msg
from futrisk.messages.common import (
    Pong,
    ReplayBatch,
    ReplayRequest,
    SnapshotBatch,
    SnapshotItem,
    SnapshotRequest,
)
from futrisk.messages.enums import AccountState, Exchange, MdQuality, Offset, OrderStatus, SourceId
from futrisk.messages.envelope import (
    Reply,
    decode_envelope,
    encode_envelope,
    err_reply,
    make_envelope,
    ok_reply,
)
from futrisk.messages.md import Tick
from futrisk.messages.orders import (
    CancelAllAck,
    CancelAllRequest,
    CancelRequest,
    FillRef,
    OrderAck,
    OrderRequest,
    OrderUpdate,
)
from futrisk.messages.risk import (
    AccountFunds,
    AccountMetricsView,
    AccountSnapshot,
    AccountSnapshotBatch,
    AccountStateChanged,
    OrderBatch,
    PositionBatch,
    PositionView,
    QueryAccounts,
    QueryOrders,
    QueryPositions,
    Restriction,
)
from futrisk.messages.topics import matches

TRADING_DAY = "20260928"
NO_REPLY = object()
_VOLUME_EPOCH_MS = 1_767_225_600_000  # 2026-01-01T00:00:00Z: volumes grow with the tick time

_EXCHANGE = {
    "rb": Exchange.SHFE,
    "hc": Exchange.SHFE,
    "cu": Exchange.SHFE,
    "au": Exchange.SHFE,
    "i": Exchange.DCE,
    "j": Exchange.DCE,
    "m": Exchange.DCE,
    "IF": Exchange.CFFEX,
    "IC": Exchange.CFFEX,
    "IH": Exchange.CFFEX,
    "IM": Exchange.CFFEX,
    "T": Exchange.CFFEX,
}
_TICK = {"cu": 10.0, "au": 0.02, "i": 0.5, "j": 0.5, "IF": 0.2, "IC": 0.2, "IH": 0.2, "IM": 0.2, "T": 0.005}


def make_tick(
    instrument_id: str,
    last: float,
    *,
    ts_ns: int,
    trading_day: str = TRADING_DAY,
    bid: float | None = None,
    ask: float | None = None,
    volume: int | None = None,
    pre_settle: float | None = None,
    upper: float | None = None,
    lower: float | None = None,
    exchange: Exchange | None = None,
    bid_vol: int = 50,
    ask_vol: int = 50,
) -> Tick:
    """A plausible LIVE tick; bid/ask default to one tick around ``last`` and limits to ±5%."""
    code = product_code(instrument_id)
    step = _TICK.get(code, 1.0)
    ps = pre_settle if pre_settle is not None else last

    def grid(x: float) -> float:
        return float(Decimal(round(x / step)) * Decimal(str(step)))
    return Tick(
        instrument_id=instrument_id,
        exchange=exchange or _EXCHANGE.get(code, Exchange.SHFE),
        trading_day=trading_day,
        action_day=trading_day,
        exch_ts_ns=ts_ns,
        last=last,
        volume=volume if volume is not None else max(1, ts_ns // 1_000_000 - _VOLUME_EPOCH_MS),
        turnover=0.0,
        open_interest=100_000,
        bid1=bid if bid is not None else grid(last - step),
        ask1=ask if ask is not None else grid(last + step),
        bid_vol1=bid_vol,
        ask_vol1=ask_vol,
        upper_limit=upper if upper is not None else grid(ps * 1.05),
        lower_limit=lower if lower is not None else grid(ps * 0.95),
        pre_settle=ps,
        source=SourceId.FEED_A,
        recv_ns=ts_ns,
    )


def order_ack(req: OrderRequest, *, accepted: bool = True, code: str | None = None,
              detail: dict[str, Any] | None = None, trading_day: str = TRADING_DAY) -> OrderAck:
    return OrderAck(
        client_order_id=req.client_order_id,
        accepted=accepted,
        status=OrderStatus.ROUTING if accepted else OrderStatus.REJECTED,
        reject_code=code,
        reject_detail=detail,
        risk_check_us=42,
        trading_day=trading_day,
    )


def account_snapshot(
    account_id: str,
    *,
    as_of_ns: int,
    equity: float = 10_000_000.0,
    state: AccountState = AccountState.NORMAL,
    halted_latched: bool = False,
    restrictions: list[Restriction] | None = None,
    positions: list[PositionView] | None = None,
    state_version: int = 0,
    version: int = 1,
    trading_day: str = TRADING_DAY,
    shard: int = 1,
) -> AccountSnapshot:
    """A minimal ``AccountSnapshot`` (02 §7.6): funds around ``equity``, zero metrics."""
    funds = AccountFunds(initial_capital=equity, cum_net_deposit=0.0, pre_balance=equity, net_deposit=0.0,
                         static_equity=equity, close_pnl=0.0, position_pnl=0.0, fee=0.0, equity=equity, margin=0.0,
                         frozen_margin=0.0, frozen_fee=0.0, available=equity, risk_degree=0.0)
    metrics = AccountMetricsView(daily_pnl=0.0, daily_pnl_pct_init=0.0, daily_pnl_pct_sod=0.0,
                                 floating_pnl_pct_init=0.0, perf_equity=equity, nav=1.0, intraday_peak_equity=equity,
                                 intraday_dd=0.0, hwm_equity=equity, hwm_dd=0.0, gross_notional=0.0,
                                 net_notional=0.0, leverage=0.0, effective_n=0.0, limit_stress=0.0, orders_today=0,
                                 cancels_today=0)
    return AccountSnapshot(account_id=account_id, shard=shard, trading_day=trading_day, version=version,
                           state_version=state_version, as_of_ns=as_of_ns, quality=MdQuality.OK, state=state,
                           halted_latched=halted_latched, restrictions=list(restrictions or []), funds=funds,
                           metrics=metrics, positions=list(positions or []))


def reject(code: str, detail: dict[str, Any] | None = None) -> Callable[[OrderRequest], Reply]:
    """Script entry: reject the next ``order.submit`` with ``code`` (``ok=False`` + ``OrderAck``)."""

    def fn(req: OrderRequest) -> Reply:
        return err_reply(code, f"scripted {code}", data=order_ack(req, accepted=False, code=code, detail=detail))

    return fn


def busy(code: str = codes.RISK_BUSY) -> Callable[[OrderRequest], Reply]:
    """Script entry: answer with a retryable error (the client retries with the same payload)."""

    def fn(req: OrderRequest) -> Reply:
        return err_reply(code, "scripted busy", retry_after_ms=10)

    return fn


class FakePublisher:
    """XPUB socket publishing real envelopes; remembers which topics are subscribed."""

    def __init__(self, ctx: zmq.asyncio.Context, address: str, *, src: str, epoch: str = "fake-epoch") -> None:
        self.address = address
        self.src = src
        self.epoch = epoch
        self._ctx = ctx
        self.sock: zmq.asyncio.Socket | None = None
        self.subscriptions: set[str] = set()
        self._task: asyncio.Task[None] | None = None
        self._changed = asyncio.Event()
        self._clock = SystemClock()

    async def start(self) -> None:
        sock: zmq.asyncio.Socket = self._ctx.socket(zmq.XPUB)
        sock.setsockopt(zmq.LINGER, 0)
        sock.bind(self.address)
        self.sock = sock
        self._task = asyncio.create_task(self._read_subscriptions())

    async def _read_subscriptions(self) -> None:
        assert self.sock is not None
        while True:
            msg = await self.sock.recv()
            if not msg:
                continue
            topic = msg[1:].decode("utf-8", errors="replace")
            if msg[0] == 1:
                self.subscriptions.add(topic)
            elif msg[0] == 0:
                self.subscriptions.discard(topic)
            self._changed.set()

    async def wait_subscribed(self, prefix: str, timeout: float = 5.0) -> None:
        """Wait until a subscriber subscribed exactly ``prefix``."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while prefix not in self.subscriptions:
            left = deadline - loop.time()
            if left <= 0:
                raise TimeoutError(f"{self.src}: nobody subscribed {prefix!r} (have {sorted(self.subscriptions)})")
            self._changed.clear()
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._changed.wait(), left)

    async def publish(self, topic: str, body: Msg, *, seq: int, stream: str | None = None,
                      epoch: str | None = None, td: str | None = None) -> bytes:
        """Send ``[topic, envelope]``; returns the encoded envelope."""
        assert self.sock is not None, "publisher not started"
        env = make_envelope(topic, body, src=self.src, epoch=epoch or self.epoch, ts_ns=self._clock.now_ns(),
                            seq=seq, stream=stream, td=td)
        data = encode_envelope(env)
        await self.sock.send_multipart([topic.encode("utf-8"), data])
        return data

    async def close(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._task
        if self.sock is not None:
            self.sock.close(0)
            self.sock = None


class FakeMdGateway(FakePublisher):
    """md-gateway PUB: ``md.tick.{instrument}`` LV messages with per-topic seq."""

    def __init__(self, ctx: zmq.asyncio.Context, address: str, *, epoch: str = "md-epoch") -> None:
        super().__init__(ctx, address, src="md-gateway", epoch=epoch)
        self.seq: dict[str, int] = {}

    async def tick(self, tick: Tick, *, seq: int | None = None) -> None:
        topic = topics.md_tick(tick.instrument_id)
        n = seq if seq is not None else self.seq.get(topic, 0) + 1
        self.seq[topic] = max(n, self.seq.get(topic, 0))
        await self.publish(topic, tick, seq=n)


class FakeShard:
    """A scripted ``risk-engine-{k}``: ROUTER commands + ``oms.order.*`` FACT publisher."""

    def __init__(
        self,
        ctx: zmq.asyncio.Context,
        instance: str,
        *,
        router: str,
        pub: str,
        internal: list[str] | None = None,
        trading_day: str = TRADING_DAY,
        fill_mode: str = "full",
        fill_delay_s: float = 0.01,
        clock_offset_ns: int = 0,
    ) -> None:
        self.instance = instance
        self.clock_offset_ns = clock_offset_ns
        self._clock = SystemClock()  # one instance: engine times must never go backwards
        self.trading_day = trading_day
        self.fill_mode = fill_mode  # full | none | partial
        self.fill_delay_s = fill_delay_s
        self.state = "RUNNING"
        self.stream = f"stream-{instance}"
        self.server = AsyncRpcServer(ctx, router, instance=instance, epoch=f"{instance}-epoch",
                                     whitelist=Whitelist(internal or []))
        self.pub = FakePublisher(ctx, pub, src=instance, epoch=f"{instance}-epoch")
        self.script: deque[Any] = deque()
        self.requests: list[tuple[str, Msg, RequestContext]] = []
        self.orders: dict[str, OrderUpdate] = {}
        self.seq: dict[str, int] = {}
        self.log: dict[str, dict[int, bytes]] = {}
        self.positions: list[PositionView] = []
        self.drop_seqs: set[int] = set()  # simulate lost FACT messages (still replayable)
        self.reject_opens: str | None = None  # reject every OPEN order with this code (e.g. a HALTED account)
        self.lv: dict[str, bytes] = {}  # last envelope per topic (ctl.snapshot)
        self.snapshots: dict[str, AccountSnapshot] = {}  # risk.query.accounts answers
        self.state_version = 0
        self._tasks: set[asyncio.Task[Any]] = set()
        self._trade_no = 0
        self.server.serve(
            {
                "ctl.ping": self._ping,
                "order.submit": self._submit,
                "order.cancel": self._cancel,
                "order.cancel_all": self._cancel_all,
                "ctl.replay": self._replay,
                "ctl.snapshot": self._snapshot,
                "risk.query.positions": self._positions,
                "risk.query.orders": self._query_orders,
                "risk.query.accounts": self._query_accounts,
            }
        )

    async def start(self) -> None:
        await self.server.start()
        await self.pub.start()

    async def close(self) -> None:
        for t in list(self._tasks):
            t.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        await self.server.close()
        await self.pub.close()

    # ------------------------------------------------------------ helpers
    def submits(self) -> list[OrderRequest]:
        return [p for c, p, _ in self.requests if c == "order.submit" and isinstance(p, OrderRequest)]

    def requesters(self) -> set[str]:
        return {ctx.requester for _, _, ctx in self.requests}

    def _spawn(self, coro: Any) -> None:
        t = asyncio.ensure_future(coro)
        self._tasks.add(t)
        t.add_done_callback(self._tasks.discard)

    def now_ns(self) -> int:
        """The fake engine's market time (wall clock + ``clock_offset_ns``), never going backwards."""
        return self._clock.now_ns() + self.clock_offset_ns

    def update_for(self, req: OrderRequest, status: OrderStatus, **kw: Any) -> OrderUpdate:
        now = self.now_ns()
        return OrderUpdate(
            client_order_id=req.client_order_id,
            account_id=req.account_id,
            instrument_id=req.instrument_id,
            exchange=_EXCHANGE.get(product_code(req.instrument_id), Exchange.SHFE),
            direction=req.direction,
            offset=req.offset,
            order_type=req.order_type,
            tif=req.tif,
            price=req.price,
            qty=req.qty,
            status=status,
            source=req.source,
            trading_day=self.trading_day,
            created_ns=now,
            updated_ns=now,
            **kw,
        )

    async def publish_update(self, upd: OrderUpdate, *, seq: int | None = None, stream: str | None = None) -> int:
        """Publish ``oms.order.{account}`` (FACT); stored for ``ctl.replay``; returns the seq."""
        topic = topics.oms_order(upd.account_id)
        n = seq if seq is not None else self.seq.get(topic, 0) + 1
        self.seq[topic] = max(n, self.seq.get(topic, 0))
        self.orders[upd.client_order_id] = upd
        env = make_envelope(topic, upd, src=self.instance, epoch=self.pub.epoch, ts_ns=self.now_ns(),
                            seq=n, stream=stream or self.stream, td=self.trading_day)
        data = encode_envelope(env)
        self.log.setdefault(topic, {})[n] = data
        self.lv[topic] = data
        if n in self.drop_seqs:
            return n
        assert self.pub.sock is not None
        await self.pub.sock.send_multipart([topic.encode("utf-8"), data])
        return n

    async def fill(self, req: OrderRequest, qty: int | None = None, price: float | None = None) -> OrderUpdate:
        """Publish a fill of ``qty`` lots (default: the rest) of an accepted order."""
        cur = self.orders.get(req.client_order_id)
        done = cur.filled_qty if cur is not None else 0
        n = (req.qty - done) if qty is None else qty
        px = price if price is not None else (req.price or 0.0)
        filled = done + n
        self._trade_no += 1
        avg = px if cur is None or cur.avg_fill_price is None else (cur.avg_fill_price * done + px * n) / filled
        upd = self.update_for(
            req,
            OrderStatus.FILLED if filled >= req.qty else OrderStatus.PARTIALLY_FILLED,
            filled_qty=filled,
            avg_fill_price=avg,
            last_fill=FillRef(trade_id=f"T{self._trade_no:06d}", price=px, qty=n, trade_ts_ns=self.now_ns()),
        )
        await self.publish_update(upd)
        return upd

    async def publish_state(
        self,
        account_id: str,
        state: AccountState,
        *,
        latched: bool = False,
        restrictions: list[Restriction] | None = None,
        from_state: AccountState = AccountState.NORMAL,
        publish: bool = True,
    ) -> AccountStateChanged:
        """``risk.state.{account}`` (FACT) with the next ``state_version``; kept for ``ctl.snapshot``."""
        self.state_version += 1
        msg = AccountStateChanged(account_id=account_id, trading_day=self.trading_day, from_state=from_state,
                                  to_state=state, halted_latched=latched, state_version=self.state_version,
                                  restrictions=list(restrictions or []), cause="RULE", ts_ns=self.now_ns())
        topic = topics.risk_state(account_id)
        n = self.seq.get(topic, 0) + 1
        self.seq[topic] = n
        env = make_envelope(topic, msg, src=self.instance, epoch=self.pub.epoch, ts_ns=self.now_ns(), seq=n,
                            stream=self.stream, td=self.trading_day)
        data = encode_envelope(env)
        self.lv[topic] = data
        if publish:
            assert self.pub.sock is not None
            await self.pub.sock.send_multipart([topic.encode("utf-8"), data])
        return msg

    async def publish_snapshot(self, snap: AccountSnapshot, *, publish: bool = True) -> None:
        """``risk.snapshot.{account}`` (LV); also the ``risk.query.accounts`` answer."""
        topic = topics.risk_snapshot(snap.account_id)
        n = self.seq.get(topic, 0) + 1
        self.seq[topic] = n
        env = make_envelope(topic, snap, src=self.instance, epoch=self.pub.epoch, ts_ns=self.now_ns(), seq=n)
        data = encode_envelope(env)
        self.lv[topic] = data
        self.snapshots[snap.account_id] = snap
        if publish:
            assert self.pub.sock is not None
            await self.pub.sock.send_multipart([topic.encode("utf-8"), data])

    async def _auto_fill(self, req: OrderRequest) -> None:
        await asyncio.sleep(self.fill_delay_s)
        await self.publish_update(self.update_for(req, OrderStatus.ACCEPTED))
        if self.fill_mode == "full":
            await self.fill(req)
        elif self.fill_mode == "partial" and req.qty > 1:
            await self.fill(req, req.qty // 2)

    # ------------------------------------------------------------ handlers
    def _ping(self, cmd: str, payload: Msg, ctx: RequestContext) -> Reply:
        self.requests.append((cmd, payload, ctx))
        return ok_reply(Pong(instance=self.instance, state=self.state, ts_ns=ctx.received_ns))

    def _submit(self, cmd: str, payload: Msg, ctx: RequestContext) -> Any:
        self.requests.append((cmd, payload, ctx))
        assert isinstance(payload, OrderRequest)
        req = payload
        if self.reject_opens is not None and req.offset == Offset.OPEN:
            self.script.appendleft(reject(self.reject_opens))
        if self.script:
            entry = self.script.popleft()
            if entry is NO_REPLY:
                return DEFERRED
            reply = entry(req) if callable(entry) else entry
            if reply is not None:
                if not reply.ok and reply.code not in codes.RETRYABLE:
                    ack = reply.open_as(OrderAck) if reply.data_type == "OrderAck" else None
                    self._spawn(self.publish_update(self.update_for(
                        req, OrderStatus.REJECTED, reject_code=ack.reject_code if ack else reply.code)))
                return reply
        known = self.orders.get(req.client_order_id)
        if known is not None:  # idempotent: first result again
            return ok_reply(order_ack(req, trading_day=self.trading_day), duplicate=True)
        self.orders[req.client_order_id] = self.update_for(req, OrderStatus.ROUTING)
        self._spawn(self.publish_update(self.orders[req.client_order_id]))
        if self.fill_mode != "none":
            self._spawn(self._auto_fill(req))
        return ok_reply(order_ack(req, trading_day=self.trading_day))

    def _cancel(self, cmd: str, payload: Msg, ctx: RequestContext) -> Any:
        self.requests.append((cmd, payload, ctx))
        assert isinstance(payload, CancelRequest)
        cur = self.orders.get(payload.client_order_id)
        if cur is None:
            return err_reply(codes.NOT_FOUND, "unknown order")
        if cur.status.is_terminal:
            return err_reply(codes.CONFLICT, "order finished")
        upd = OrderUpdate(**{f: getattr(cur, f) for f in cur.__struct_fields__})
        upd.status = OrderStatus.CANCELED
        upd.updated_ns = self.now_ns()
        upd.reason = payload.reason
        self._spawn(self.publish_update(upd))
        self.orders[upd.client_order_id] = upd
        return ok_reply(upd)

    def _cancel_all(self, cmd: str, payload: Msg, ctx: RequestContext) -> Any:
        self.requests.append((cmd, payload, ctx))
        assert isinstance(payload, CancelAllRequest)
        n = 0
        for cur in list(self.orders.values()):
            if cur.account_id != payload.account_id or cur.status.is_terminal:
                continue
            upd = OrderUpdate(**{f: getattr(cur, f) for f in cur.__struct_fields__})
            upd.status = OrderStatus.CANCELED
            upd.updated_ns = self.now_ns()
            self.orders[upd.client_order_id] = upd
            self._spawn(self.publish_update(upd))
            n += 1
        return ok_reply(CancelAllAck(count=n))

    def _replay(self, cmd: str, payload: Msg, ctx: RequestContext) -> Any:
        self.requests.append((cmd, payload, ctx))
        assert isinstance(payload, ReplayRequest)
        stored = self.log.get(payload.topic, {})
        hi = payload.to_seq if payload.to_seq is not None else max(stored, default=0)
        seqs = [s for s in sorted(stored) if payload.from_seq <= s <= hi][: payload.limit]
        if payload.from_seq <= hi and not seqs:
            return err_reply(codes.NOT_FOUND, "purged")
        nxt = (seqs[-1] + 1) if seqs else payload.from_seq
        return ok_reply(ReplayBatch(topic=payload.topic, envelopes=[stored[s] for s in seqs], next_seq=nxt,
                                    complete=nxt > hi))

    def _snapshot(self, cmd: str, payload: Msg, ctx: RequestContext) -> Any:
        self.requests.append((cmd, payload, ctx))
        assert isinstance(payload, SnapshotRequest)
        items = []
        for topic in sorted(self.lv):
            if any(matches(topic, p) for p in payload.prefixes):
                data = self.lv[topic]
                env = decode_envelope(data)
                items.append(SnapshotItem(topic=topic, seq=env.seq, epoch=env.epoch, envelope=data))
        return ok_reply(SnapshotBatch(items=items, as_of_ns=self.now_ns()))

    def _query_accounts(self, cmd: str, payload: Msg, ctx: RequestContext) -> Any:
        self.requests.append((cmd, payload, ctx))
        assert isinstance(payload, QueryAccounts)
        wanted = payload.account_ids or sorted({p.account_id for p in self.positions} | set(self.snapshots))
        items = []
        for a in wanted:
            snap = self.snapshots.get(a)
            if snap is None:
                snap = account_snapshot(a, as_of_ns=self.now_ns(), positions=[p for p in self.positions
                                                                                 if p.account_id == a],
                                        state_version=self.state_version, trading_day=self.trading_day)
            items.append(snap)
        return ok_reply(AccountSnapshotBatch(items=items))

    def _positions(self, cmd: str, payload: Msg, ctx: RequestContext) -> Any:
        self.requests.append((cmd, payload, ctx))
        assert isinstance(payload, QueryPositions)
        return ok_reply(PositionBatch(items=[p for p in self.positions
                                             if payload.account_id is None or p.account_id == payload.account_id]))

    def _query_orders(self, cmd: str, payload: Msg, ctx: RequestContext) -> Any:
        self.requests.append((cmd, payload, ctx))
        assert isinstance(payload, QueryOrders)
        return ok_reply(OrderBatch(items=[o for o in self.orders.values() if o.account_id == payload.account_id
                                          and (payload.status is None or o.status in payload.status)]))


class FakeBus:
    """md-gateway publisher + one :class:`FakeShard` per ``risk-engine-*`` instance of ``config``."""

    def __init__(self, config: FullConfig, *, ctx: zmq.asyncio.Context | None = None, **shard_kw: Any) -> None:
        self._own = ctx is None
        self.ctx = ctx or zmq.asyncio.Context()
        services = config.services
        md = services.instance("md-gateway")
        assert md.pub is not None
        self.md = FakeMdGateway(self.ctx, md.pub)
        internal = list(services.instances)
        self.shards: dict[str, FakeShard] = {}
        for name in services.risk_instances():
            inst = services.instance(name)
            assert inst.router is not None and inst.pub is not None
            self.shards[name] = FakeShard(self.ctx, name, router=inst.router, pub=inst.pub, internal=internal,
                                          **shard_kw)

    def shard(self, instance: str) -> FakeShard:
        return self.shards[instance]

    async def start(self) -> None:
        await self.md.start()
        for s in self.shards.values():
            await s.start()

    async def close(self) -> None:
        for s in self.shards.values():
            await s.close()
        await self.md.close()
        if self._own:
            self.ctx.destroy(linger=0)

    async def __aenter__(self) -> FakeBus:
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()


__all__ = [
    "NO_REPLY",
    "TRADING_DAY",
    "FakeBus",
    "account_snapshot",
    "FakeMdGateway",
    "FakePublisher",
    "FakeShard",
    "busy",
    "make_tick",
    "order_ack",
    "reject",
]

