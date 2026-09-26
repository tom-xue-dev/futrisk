"""Test support for the risk-engine shell: fakes on real sockets and a business-thread harness.

* :class:`ServiceThread` runs any :class:`SyncServiceRuntime` loop on its own thread; ``call`` executes
  code there.
* :class:`FakeTradeGateway` (runtime + td.db outbox): ``route.order`` / ``route.cancel`` /
  ``route.resolve`` / ``td.query.*`` / ``td.digest``; ``td.exec.{acct}`` FACTs go through the outbox, so
  ``seq`` / ``stream`` / ``ctl.replay`` / heartbeat ``fact_heads`` are the real ones.
* :class:`FakeMdGateway` (runtime, LV only): ``md.tick.*`` / ``md.status`` on the PUB (``ctl.snapshot`` served
  from the LV cache), ``md.settle_inputs``; heartbeats can be paused to simulate a dead gateway.
* :class:`FakeClearing` (runtime): ``settle.sod_snapshot`` from a dict, ``sys.eod`` publishing; the test
  sends ``eod.freeze`` / ``eod.rollover`` with a DEALER whose identity is ``clearing-worker``.
* :class:`Shell` runs the real :class:`RiskEngineService` on a business thread.

Timing rules (the suite must pass on a loaded machine): nothing is sent "right after start" — :func:`wait_ready`
waits until ``ctl.ping`` answers through every client and the heartbeat shows the phase; commands go through
:func:`request`, which retries TIMEOUT / UNAVAILABLE / BUSY like the production clients (02 §4.7, same payload);
tests wait on observable conditions (:func:`wait_for`, generous timeouts that return as soon as the condition
holds) instead of sleeping; peers are declared down after :data:`PEER_TIMEOUT_MISSED` missed heartbeats.
"""

from __future__ import annotations

import dataclasses
import functools
import itertools
import queue
import threading
import time
from collections.abc import Callable, Iterable, Sequence
from concurrent.futures import Future
from datetime import datetime
from pathlib import Path
from typing import Any, TypeVar

from sqlalchemy import MetaData, Table

from futrisk.bus import BlockingRpcClient
from futrisk.bus.heartbeat import HeartbeatThread
from futrisk.bus.rpc_common import DEFERRED, HandlerResult, RequestContext
from futrisk.bus.timeouts import is_retryable
from futrisk.common.clock import BJ, MarketClock, bj_to_ns
from futrisk.common.config import FullConfig, RiskRulesConfig, RuleDef
from futrisk.common.config.risk_rules import RuleLevel
from futrisk.common.decimals import D
from futrisk.common.ids import uuid7
from futrisk.core.instruments import InstrumentRegistry
from futrisk.db import Database, add_common_tables
from futrisk.db.types import id_col
from futrisk.messages import codes, topics
from futrisk.messages.base import Msg, decode_named
from futrisk.messages.common import (
    Ping,
    ReplayBatch,
    ReplayRequest,
    SnapshotBatch,
    SnapshotRequest,
    StatusRequest,
)
from futrisk.messages.enums import (
    BrokerOrderStatus,
    Direction,
    EodPhase,
    MdQuality,
    Offset,
    PosDirection,
    ServiceState,
    SourceId,
    TradeOrigin,
)
from futrisk.messages.envelope import Envelope, Reply, decode_envelope, err_reply, ok_reply
from futrisk.messages.eod import EodPhaseEvent, SodAccount, SodLot, SodRequest, SodSnapshot
from futrisk.messages.md import InstrumentStatus, MdStatus, SettleInputsBatch, SettleInputsRequest, Tick
from futrisk.messages.orders import (
    BrokerOrderBatch,
    BrokerOrderUpdate,
    BrokerPosition,
    BrokerPositionBatch,
    DigestRequest,
    OrderQuery,
    PositionQuery,
    RouteAck,
    RouteCancel,
    RouteOrder,
    RouteResolve,
    Trade,
    TradeBatch,
    TradeDigest,
    TradeQuery,
    compute_account_digest,
)
from futrisk.messages.system import Heartbeat
from futrisk.runtime import SyncServiceRuntime
from futrisk.runtime.testing import config_with_free_ports
from futrisk.services.risk_engine.service import Phase, RiskEngineService
from futrisk.sim.exchange.testing import offset_for

T = TypeVar("T")

TD = "20260928"  # Monday; 20260925 is the Mid-Autumn holiday, so no night session belongs to it
NTD = "20260929"
PREV_TD = "20260924"
MARKET_OPEN = datetime(2026, 9, 28, 9, 35, tzinfo=BJ)
PRE = {"rb2701": 3100.0, "hc2701": 3300.0, "i2701": 800.0, "IF2612": 4500.0, "cu2611": 80000.0}
TG, MD, CW = "trade-gateway", "md-gateway", "clearing-worker"
SEC = 1_000_000_000
WAIT_S = 20.0  # default for conditions: generous, and a met condition returns at once
RETRY_WITHIN_S = 20.0  # retrying requests give up after this
PEER_TIMEOUT_MISSED = 10  # 100 ms heartbeats: a peer is down after 1 s of silence (a stalled thread is not a dead peer)


# ---------------------------------------------------------------------------------------- helpers
def wait_for(pred: Callable[[], Any], timeout: float = WAIT_S, step: float = 0.01, what: str = "condition") -> Any:
    end = time.monotonic() + timeout
    while True:
        got = pred()
        if got:
            return got
        if time.monotonic() > end:
            raise AssertionError(f"{what} not met within {timeout}s")
        time.sleep(step)


def request(c: BlockingRpcClient, target: str, command: str, payload: Msg, *, timeout_ms: int = 10_000,
            within: float = RETRY_WITHIN_S) -> Reply:
    """``command`` with the production retry rule (02 §4.7): TIMEOUT / UNAVAILABLE / BUSY / RISK_BUSY are retried with
    the same payload (same idempotency key) until ``within`` seconds have passed; other replies return at once.

    UNAVAILABLE "peer not connected" means the request was never sent (DEALER ``IMMEDIATE``, e.g. while it reconnects
    to a restarted ROUTER), so first-attempt semantics such as ``duplicate=False`` are kept; the long per-attempt
    timeout keeps TIMEOUT retries (which may answer ``duplicate=True``) out of normal runs.
    """
    end = time.monotonic() + within
    delay = 0.05
    while True:
        r = c.request(target, command, payload, timeout_ms=timeout_ms, retries=0)
        if not is_retryable(r.code):
            return r
        pause = max(delay, (r.retry_after_ms or 0) / 1000)
        if time.monotonic() + pause > end:
            return r
        time.sleep(pause)
        delay = min(delay * 2, 0.5)


def status(c: BlockingRpcClient, target: str) -> Heartbeat:
    """The target's current heartbeat (``ctl.status``)."""
    r = request(c, target, "ctl.status", StatusRequest())
    assert r.ok, r
    hb = r.open_as(Heartbeat)
    assert isinstance(hb, Heartbeat)
    return hb


def wait_ready(clients: Sequence[BlockingRpcClient], target: str, phase: Phase = Phase.RUNNING,
               timeout: float = 30.0) -> Heartbeat:
    """After a (re)start of ``target``: ``ctl.ping`` answered through every client, then a heartbeat in ``phase``
    or later (RUNNING: state RUNNING or DEGRADED).

    Each client has its own DEALER per target; after the previous process went away it reconnects with backoff
    (up to 1 s), so a shard that is RUNNING is not yet reachable from every client.
    """
    order = list(Phase)

    def pong(c: BlockingRpcClient) -> bool:
        return bool(c.request(target, "ctl.ping", Ping(), timeout_ms=2000, retries=0).ok)

    for c in clients:
        wait_for(functools.partial(pong, c), timeout, 0.02, f"ctl.ping {target} from {c.identity}")

    def in_phase() -> Heartbeat | None:
        r = clients[0].request(target, "ctl.status", StatusRequest(), timeout_ms=2000, retries=0)
        if not r.ok:
            return None
        hb = r.open_as(Heartbeat)
        got = (hb.detail or {}).get("phase")
        if got not in {p.value for p in order} or order.index(Phase(got)) < order.index(phase):
            return None
        if phase == Phase.RUNNING and hb.state not in (ServiceState.RUNNING, ServiceState.DEGRADED):
            return None
        return hb

    got: Heartbeat = wait_for(in_phase, timeout, 0.02, f"{target} heartbeat in phase {phase.value}")
    return got


def lv_seq(c: BlockingRpcClient, target: str, topic: str) -> int:
    """Seq of the last value of ``topic`` published by the target process (0: none yet); grows with every publish."""
    r = request(c, target, "ctl.snapshot", SnapshotRequest(prefixes=[topic]))
    items = r.open_as(SnapshotBatch).items if r.ok else []
    seqs = [decode_envelope(i.envelope).seq for i in items if decode_envelope(i.envelope).topic == topic]
    return max(seqs, default=0)


def daily_loss_rule() -> RuleDef:
    return RuleDef(
        id="daily_loss",
        metric="account.daily_pnl_pct_init",
        op="<=",
        scope="account",
        hysteresis=0.2,
        cooldown_s=60,
        pretrade=True,
        levels=[
            RuleLevel(name="warn", threshold=-0.020, action="ALERT"),
            RuleLevel(name="restrict", threshold=-0.025, action="RESTRICT",
                      params={"max_order_factor": 0.5, "no_add_to_losers": True}),
            RuleLevel(name="halt", threshold=-0.030, action="CIRCUIT_BREAK", latch=True,
                      confirm={"states": 2, "ms": 1000}, rearm={"extra": 0.01}),
            RuleLevel(name="derisk", threshold=-0.050, action="LIQUIDATE", params={"step": 0.5}),
        ],
    )


def firm_loss_rule(threshold: float = -0.003) -> RuleDef:
    return RuleDef(
        id="firm_loss",
        metric="firm.daily_pnl_pct_init",
        op="<=",
        scope="firm",
        levels=[RuleLevel(name="kill", threshold=threshold, action="CIRCUIT_BREAK", confirm={"states": 1, "ms": 0})],
    )


def make_config(default_config: FullConfig, *, rules: Iterable[RuleDef] | None = None, heartbeat_ms: int = 100) -> FullConfig:
    """Default config on free loopback ports, fast heartbeats, focused risk rules (daily_loss by default)."""
    cfg = config_with_free_ports(default_config, heartbeat_ms=heartbeat_ms, peer_timeout_missed=PEER_TIMEOUT_MISSED)
    base = cfg.risk_rules
    rr = RiskRulesConfig(rules=list(rules) if rules is not None else [daily_loss_rule()], pretrade=base.pretrade,
                         system=base.system)
    return dataclasses.replace(cfg, risk_rules=rr)


def demo_clock(at: datetime = MARKET_OPEN) -> MarketClock:
    return MarketClock(mode="DEMO", offset_ns=offset_for(at))


def jump(clock: MarketClock, to: datetime) -> None:
    """Demo clock jump (02 §8.8): a new offset with a higher version, as ``sys.clock`` would deliver it."""
    clock.apply("DEMO", offset_for(to), clock.version + 1)


def lot(inst: str, qty: int, price: float, *, direction: PosDirection = PosDirection.LONG, n: int = 1) -> SodLot:
    return SodLot(instrument_id=inst, direction=direction, lot_no=n, open_td=PREV_TD,
                  open_ts_ns=bj_to_ns(datetime(2026, 9, 24, 10, 0, tzinfo=BJ)) + n, open_price=f"{price:g}",
                  qty=qty, basis_price=f"{price:g}")


def sod(accounts: dict[str, tuple[str, list[SodLot]]], *, trading_day: str = TD, version: int = 0,
        pre: dict[str, float] | None = None) -> SodSnapshot:
    return SodSnapshot(
        trading_day=trading_day,
        version=version,
        accounts=[
            SodAccount(account_id=a, pre_balance=f"{D(cap):.2f}", initial_capital=f"{D(cap):.2f}",
                       cum_net_deposit="0.00", hwm_equity=f"{D(cap):.2f}", lots=lots)
            for a, (cap, lots) in sorted(accounts.items())
        ],
        pre_settle={k: f"{v:g}" for k, v in (pre or PRE).items()},
    )


def shard1_sod(**kw: Any) -> SodSnapshot:
    """ACC3: 300 rb2701 long (3000 CNY per point: -2% at 3033.3, -3% at 3000); ACC4: 5 IF2612 long; ACC5 flat."""
    return sod({"ACC3": ("10000000", [lot("rb2701", 300, 3100.0)]),
                "ACC4": ("10000000", [lot("IF2612", 5, 4500.0)]),
                "ACC5": ("5000000", [])}, **kw)


def shard0_sod(**kw: Any) -> SodSnapshot:
    return sod({"ACC1": ("10000000", []), "ACC2": ("20000000", [])}, **kw)


def client(cfg: FullConfig, identity: str) -> BlockingRpcClient:
    return BlockingRpcClient(lambda t: cfg.services.instance(t).router or "", identity=identity)


def replay(c: BlockingRpcClient, src: str, topic: str) -> list[Envelope]:
    """Every FACT of ``topic`` published by ``src`` so far (``ctl.replay`` from seq 1)."""
    r = request(c, src, "ctl.replay", ReplayRequest(topic=topic, from_seq=1, limit=1000))
    if r.code == codes.NOT_FOUND:
        return []
    assert r.ok, r
    return [decode_envelope(b) for b in r.open_as(ReplayBatch).envelopes]


def body(env: Envelope) -> Msg:
    return decode_named(env.payload, env.type)


# ---------------------------------------------------------------------------------------- threads
class ServiceThread:
    """A SyncServiceRuntime running ``rt.run`` on its own thread; ``call`` executes code on that thread."""

    def __init__(self, rt: SyncServiceRuntime, setup: Callable[[], None] | None = None) -> None:
        self.rt = rt
        self._setup = setup
        self._q: queue.SimpleQueue[tuple[Callable[[], Any], Future[Any]]] = queue.SimpleQueue()
        self._started = threading.Event()
        self.error: BaseException | None = None
        self._thread = threading.Thread(target=self._main, name=f"business:{rt.instance}", daemon=True)

    def start(self) -> ServiceThread:
        self._thread.start()
        assert self._started.wait(15)
        if self.error is not None:
            raise self.error
        return self

    def _main(self) -> None:
        try:
            self.rt.start()
            if self._setup is not None:
                self._setup()
            self._started.set()
            self.rt.run(self._step, on_completion=lambda c: None, max_wait_ms=5)
        except BaseException as exc:
            self.error = exc
            self._started.set()
        finally:
            self.rt.stop()

    def _step(self) -> None:
        while True:
            try:
                fn, fut = self._q.get_nowait()
            except queue.Empty:
                return
            try:
                fut.set_result(fn())
            except BaseException as exc:
                fut.set_exception(exc)

    def call(self, fn: Callable[[], T], timeout: float = WAIT_S) -> T:
        fut: Future[T] = Future()
        self._q.put((fn, fut))
        return fut.result(timeout)

    def alive(self) -> bool:
        return self._thread.is_alive()

    def stop(self, timeout: float = 20.0) -> None:
        if self._thread.is_alive():
            self.rt.request_stop("test")
            self._thread.join(timeout)


class HeartbeatControl:
    """Pause / resume the heartbeat thread of a runtime (simulates a dead peer without closing sockets)."""

    def __init__(self, rt: SyncServiceRuntime) -> None:
        self.rt = rt

    def pause(self) -> None:
        hb = self.rt._hb_thread
        if hb is not None:
            hb.stop()

    def resume(self) -> None:
        rt = self.rt
        rt._hb_thread = HeartbeatThread(rt._emit_heartbeat, rt.heartbeat_ms, name=f"hb:{rt.instance}")
        rt._hb_thread.start()


# ---------------------------------------------------------------------------------------- trade-gateway
@dataclasses.dataclass
class FakeOrder:
    route: RouteOrder
    status: BrokerOrderStatus
    ref: str
    filled: int = 0

    @property
    def remaining(self) -> int:
        return max(0, int(self.route.qty) - self.filled)


class FakeTradeGateway:
    """trade-gateway double: routes into memory, td.exec FACTs through a real outbox."""

    def __init__(self, cfg: FullConfig, home: Path, clock: MarketClock, *, trading_day: str = TD,
                 positions: dict[tuple[str, str, PosDirection], int] | None = None) -> None:
        self.cfg = cfg
        self.clock = clock
        self.trading_day = trading_day
        self.rt = SyncServiceRuntime(TG, config=cfg, home=home, market_clock=clock, setup_logs=False)
        self.md = MetaData()
        add_common_tables(self.md, fact_cursors=False)
        Table("noop", self.md, id_col("k", primary_key=True))
        self.db: Database | None = None
        self.lock = threading.RLock()
        self.orders: dict[str, FakeOrder] = {}
        self.tombstones: set[str] = set()
        self.trades: list[Trade] = []
        self.yd: dict[tuple[str, str, PosDirection], int] = dict(positions or {})
        self.received: list[tuple[str, Msg]] = []
        self.auto_accept = True
        self.auto_fill = False  # fill every accepted order at its limit price right away
        self.silent = False  # every command received but never answered (a hung gateway)
        self.silent_for: set[str] = set()  # the same for these commands only
        self._trade_seq = itertools.count(1)
        self._broker_seq = itertools.count(1)
        self._ref = itertools.count(1)
        for cmd, fn in (("route.order", self._route), ("route.cancel", self._cancel), ("route.resolve", self._resolve),
                        ("td.query.trades", self._q_trades), ("td.query.orders", self._q_orders),
                        ("td.query.positions", self._q_positions), ("td.digest", self._digest)):
            self.rt.add_handler(cmd, fn)
        self.thread = ServiceThread(self.rt, self._setup)
        self.hb = HeartbeatControl(self.rt)

    def _setup(self) -> None:
        self.db = self.rt.open_database("td", self.md, synchronous="FULL")

    def start(self) -> FakeTradeGateway:
        self.thread.start()
        return self

    def stop(self) -> None:
        self.thread.stop()

    # ------------------------------------------------------------------ publishing
    def publish(self, msg: Trade | BrokerOrderUpdate) -> int:
        assert self.db is not None
        topic = topics.td_exec(msg.account_id)
        corr = msg.client_order_id
        seq: int = self.db.write(lambda c: self.rt.append_fact(c, topic, msg, td=msg.trading_day, corr_id=corr))
        return seq

    def update(self, o: FakeOrder, status: BrokerOrderStatus, *, code: str | None = None, broker_seq: int | None = None) -> None:
        r = o.route
        o.status = status
        self.publish(BrokerOrderUpdate(client_order_id=r.client_order_id, account_id=r.account_id,
                                       instrument_id=r.instrument_id, broker_order_ref=o.ref, status=status,
                                       filled_qty=o.filled, remaining_qty=o.remaining if status not in (
                                           BrokerOrderStatus.CANCELED, BrokerOrderStatus.REJECTED,
                                           BrokerOrderStatus.EXPIRED) else 0,
                                       broker_code=code,
                                       broker_seq=next(self._broker_seq) if broker_seq is None else broker_seq,
                                       trading_day=r.trading_day, updated_ns=self.clock.now_ns()))

    def trade(self, account: str, inst: str, direction: Direction, offset: Offset, price: float, qty: int, *,
              coid: str | None = None, ref: str | None = None, origin: TradeOrigin = TradeOrigin.INTERNAL,
              trading_day: str | None = None, publish: bool = True) -> Trade:
        reg = InstrumentRegistry(self.cfg.products, self.cfg.calendar)
        with self.lock:
            seq = next(self._trade_seq)
            t = Trade(trading_day=trading_day or self.trading_day, exchange=reg.get(inst).exchange, trade_id=f"T{seq:05d}",
                      direction=direction, account_id=account, instrument_id=inst, client_order_id=coid, origin=origin,
                      broker_order_ref=ref or f"EXT-{seq}", offset=offset, price=price, qty=qty,
                      trade_ts_ns=self.clock.now_ns(), broker_seq=next(self._broker_seq), trade_seq=seq,
                      received_ns=self.clock.now_ns())
            self.trades.append(t)
        if publish:
            self.publish(t)
        return t

    def fill(self, coid: str, qty: int | None = None, price: float | None = None) -> Trade:
        with self.lock:
            o = self.orders[coid]
            r = o.route
            q = o.remaining if qty is None else qty
            px = price if price is not None else float(r.price or 0.0)
            o.filled += q
        t = self.trade(r.account_id, r.instrument_id, r.direction, r.offset, px, q, coid=coid, ref=o.ref,
                       trading_day=r.trading_day)
        self.update(o, BrokerOrderStatus.FILLED if o.remaining == 0 else BrokerOrderStatus.PARTIALLY_FILLED)
        return t

    def trades_of(self, account: str, trading_day: str | None = None) -> list[Trade]:
        td = trading_day or self.trading_day
        with self.lock:
            return [t for t in self.trades if t.account_id == account and t.trading_day == td]

    def new_stream(self) -> str:
        """td.db recreated (02 §4.6): a new stream id whose FACT sequences start at 1 again; the earlier rows are gone.

        The gateway's next heartbeat announces the new ``stream`` and its ``fact_heads`` (none yet): the heartbeat is
        the authority for a publisher's current stream (bus/incarnation.py), so subscribers switch on it, not on
        envelopes. Heartbeats pause while the database is swapped, so none announces a mix of the two streams; the
        broker's memory (orders, trades) is kept, as ``td.query.*`` would still return it.
        """
        db, ob = self.db, self.rt.outbox
        assert db is not None and ob is not None
        wait_for(lambda: ob.count_unpublished() == 0, what="trade-gateway outbox drained")
        self.hb.pause()
        try:
            db.write(lambda c: [c.exec_driver_sql(f"DELETE FROM {t}") for t in ("outbox", "outbox_topics")])
            core = self.rt.publisher_core
            with core._lock:  # heads of the old stream: a recreated database starts without any
                core._heads.clear()
            stream: str = uuid7()
            db._info = dataclasses.replace(db.info, stream_id=stream)
        finally:
            self.hb.resume()  # the first heartbeat goes out at once
        return stream

    def routed(self) -> list[str]:
        with self.lock:
            return [p.client_order_id for c, p in self.received if c == "route.order" and isinstance(p, RouteOrder)]

    # ------------------------------------------------------------------ handlers (fake's loop thread)
    def _route(self, command: str, p: Msg, ctx: RequestContext) -> HandlerResult:
        assert isinstance(p, RouteOrder)
        with self.lock:
            self.received.append((command, p))
            if self.silent or command in self.silent_for:
                return DEFERRED
            coid = p.client_order_id
            if coid in self.tombstones:
                return err_reply(codes.ROUTE_LOST, data=RouteAck(client_order_id=coid, status=BrokerOrderStatus.EXPIRED))
            existing = self.orders.get(coid)
            if existing is not None:
                return ok_reply(RouteAck(client_order_id=coid, status=existing.status, broker_order_ref=existing.ref),
                                duplicate=True)
            o = FakeOrder(route=p, status=BrokerOrderStatus.SUBMITTED, ref=f"1:{next(self._ref)}")
            self.orders[coid] = o
        if self.auto_accept:
            self.update(o, BrokerOrderStatus.ACCEPTED)
            if self.auto_fill:
                self.fill(coid)
        return ok_reply(RouteAck(client_order_id=coid, status=BrokerOrderStatus.SUBMITTED, broker_order_ref=o.ref))

    def _cancel(self, command: str, p: Msg, ctx: RequestContext) -> HandlerResult:
        assert isinstance(p, RouteCancel)
        with self.lock:
            self.received.append((command, p))
            if self.silent or command in self.silent_for:
                return DEFERRED
            o = self.orders.get(p.client_order_id)
            if o is None:
                self.tombstones.add(p.client_order_id)
                return err_reply(codes.ROUTE_LOST, data=RouteAck(client_order_id=p.client_order_id,
                                                                 status=BrokerOrderStatus.EXPIRED))
            if o.status in (BrokerOrderStatus.FILLED, BrokerOrderStatus.CANCELED, BrokerOrderStatus.REJECTED):
                return err_reply(codes.BROKER_ORDER_FINISHED, data=RouteAck(client_order_id=p.client_order_id,
                                                                            status=o.status, broker_order_ref=o.ref))
        self.update(o, BrokerOrderStatus.CANCELED)
        return ok_reply(RouteAck(client_order_id=p.client_order_id, status=BrokerOrderStatus.CANCEL_PENDING,
                                 broker_order_ref=o.ref))

    def _resolve(self, command: str, p: Msg, ctx: RequestContext) -> HandlerResult:
        assert isinstance(p, RouteResolve)
        with self.lock:
            self.received.append((command, p))
            if self.silent or command in self.silent_for:
                return DEFERRED
            o = self.orders.get(p.client_order_id)
            if o is None:
                self.tombstones.add(p.client_order_id)
                return err_reply(codes.ROUTE_LOST, data=RouteAck(client_order_id=p.client_order_id,
                                                                 status=BrokerOrderStatus.EXPIRED))
            return ok_reply(RouteAck(client_order_id=p.client_order_id, status=o.status, broker_order_ref=o.ref))

    def _q_trades(self, command: str, p: Msg, ctx: RequestContext) -> HandlerResult:
        assert isinstance(p, TradeQuery)
        with self.lock:
            self.received.append((command, p))
            if self.silent or command in self.silent_for:
                return DEFERRED
            rows = [t for t in self.trades if t.trading_day == p.trading_day
                    and (p.account_id is None or t.account_id == p.account_id)
                    and (p.from_trade_seq is None or t.trade_seq >= p.from_trade_seq)]
        rows.sort(key=lambda t: t.trade_seq)
        page = rows[: p.limit]
        complete = len(rows) <= p.limit
        nxt = (page[-1].trade_seq + 1) if page else (p.from_trade_seq or 1)
        return ok_reply(TradeBatch(trades=page, next_trade_seq=nxt, complete=complete))

    def _q_orders(self, command: str, p: Msg, ctx: RequestContext) -> HandlerResult:
        assert isinstance(p, OrderQuery)
        with self.lock:
            self.received.append((command, p))
            if self.silent or command in self.silent_for:
                return DEFERRED
            items = [BrokerOrderUpdate(client_order_id=c, account_id=o.route.account_id,
                                       instrument_id=o.route.instrument_id, broker_order_ref=o.ref, status=o.status,
                                       filled_qty=o.filled, remaining_qty=o.remaining, broker_seq=0,
                                       trading_day=o.route.trading_day, updated_ns=self.clock.now_ns())
                     for c, o in self.orders.items()
                     if (p.account_id is None or o.route.account_id == p.account_id)
                     and (not p.client_order_ids or c in p.client_order_ids)]
        return ok_reply(BrokerOrderBatch(items=items))

    def positions(self, trading_day: str | None = None) -> list[BrokerPosition]:
        td_pos: dict[tuple[str, str, PosDirection], int] = {}
        yd = dict(self.yd)
        with self.lock:
            trades = [t for t in self.trades if t.trading_day == (trading_day or self.trading_day)]
        for t in trades:
            if t.offset == Offset.OPEN:
                d = PosDirection.LONG if t.direction == Direction.BUY else PosDirection.SHORT
                key = (t.account_id, t.instrument_id, d)
                td_pos[key] = td_pos.get(key, 0) + t.qty
            else:
                d = PosDirection.SHORT if t.direction == Direction.BUY else PosDirection.LONG
                key = (t.account_id, t.instrument_id, d)
                if t.offset == Offset.CLOSE_TODAY:
                    td_pos[key] = td_pos.get(key, 0) - t.qty
                else:
                    yd[key] = yd.get(key, 0) - t.qty
        out = []
        for key in sorted(set(td_pos) | set(yd), key=lambda k: (k[0], k[1], k[2].value)):
            tq, yq = td_pos.get(key, 0), yd.get(key, 0)
            if tq or yq:
                out.append(BrokerPosition(account_id=key[0], instrument_id=key[1], direction=key[2], td_qty=tq, yd_qty=yq))
        return out

    def _q_positions(self, command: str, p: Msg, ctx: RequestContext) -> HandlerResult:
        assert isinstance(p, PositionQuery)
        with self.lock:
            self.received.append((command, p))
            if self.silent or command in self.silent_for:
                return DEFERRED
        items = [x for x in self.positions(p.trading_day) if p.account_id is None or x.account_id == p.account_id]
        return ok_reply(BrokerPositionBatch(trading_day=p.trading_day, as_of_ns=self.clock.now_ns(), items=items))

    def _digest(self, command: str, p: Msg, ctx: RequestContext) -> HandlerResult:
        assert isinstance(p, DigestRequest)
        with self.lock:
            trades = [t for t in self.trades if t.trading_day == p.trading_day]
        accts = sorted({t.account_id for t in trades})
        return ok_reply(TradeDigest(trading_day=p.trading_day,
                                    accounts=[compute_account_digest(a, [t for t in trades if t.account_id == a]) for a in accts],
                                    total_count=len(trades), generated_ns=self.clock.now_ns()))


# ---------------------------------------------------------------------------------------- md-gateway
class FakeMdGateway:
    """md-gateway double: LV ticks / status on a real PUB, ``md.settle_inputs`` on the ROUTER.

    Like md-gateway, ``md.status`` (the quality authority, ENGINE.md §2) is republished in full about once a second
    and whenever it changes: an instrument becomes OK with its first tick, :meth:`status` sets qualities explicitly
    (STALE / UNKNOWN stay until a test sets them back). The status stops while the heartbeats are paused (a dead
    gateway publishes nothing).
    """

    STATUS_INTERVAL_S = 1.0

    def __init__(self, cfg: FullConfig, home: Path, clock: MarketClock) -> None:
        self.cfg = cfg
        self.clock = clock
        self.rt = SyncServiceRuntime(MD, config=cfg, home=home, market_clock=clock, setup_logs=False)
        self.registry = InstrumentRegistry(cfg.products, cfg.calendar)
        self.volume: dict[str, int] = {}
        self.last: dict[str, float] = {}
        self.quality: dict[str, MdQuality] = {}
        self.status_day = TD
        self.settle_requests: list[SettleInputsRequest] = []
        self.rt.add_handler("md.settle_inputs", self._settle_inputs)
        self.thread = ServiceThread(self.rt)
        self.hb = HeartbeatControl(self.rt)
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._status_thread = threading.Thread(target=self._status_loop, name="md-status", daemon=True)

    def start(self) -> FakeMdGateway:
        self.thread.start()
        self._status_thread.start()
        return self

    def stop(self) -> None:
        self._stop.set()
        self._status_thread.join(5)
        self.thread.stop()

    def make_tick(self, inst: str, px: float, *, trading_day: str = TD) -> Tick:
        self.volume[inst] = self.volume.get(inst, 0) + 1
        info = self.registry.get(inst)
        pre = D(PRE.get(inst, px))
        up, lo = self.registry.limit_prices(inst, pre, trading_day)
        now = self.clock.now_ns()
        return Tick(instrument_id=inst, exchange=info.exchange, trading_day=trading_day, action_day=trading_day,
                    exch_ts_ns=now, last=px, volume=self.volume[inst], turnover=0.0, open_interest=0,
                    bid1=px - float(info.tick_size), ask1=px + float(info.tick_size), upper_limit=float(up),
                    lower_limit=float(lo), pre_settle=float(pre), source=SourceId.FEED_A, recv_ns=now)

    def tick(self, inst: str, px: float, **kw: Any) -> Tick:
        t = self.make_tick(inst, px, **kw)
        self.last[inst] = px
        self.rt.publish(topics.md_tick(inst), t, td=t.trading_day)
        with self._lock:
            new = inst not in self.quality
            if new:
                self.quality[inst] = MdQuality.OK
        if new:
            self.publish_status()
        return t

    def ticks(self, prices: dict[str, float], **kw: Any) -> None:
        for inst, px in prices.items():
            self.tick(inst, px, **kw)

    def status(self, qualities: dict[str, MdQuality], *, trading_day: str | None = None) -> None:
        """Set instrument qualities and publish the full ``md.status`` now."""
        with self._lock:
            self.quality.update(qualities)
            if trading_day is not None:
                self.status_day = trading_day
        self.publish_status()

    def publish_status(self) -> None:
        with self._lock:
            items = [InstrumentStatus(instrument_id=i, quality=q) for i, q in sorted(self.quality.items())]
            day = self.status_day
        st = MdStatus(as_of_ns=self.clock.now_ns(), trading_day=day, sources=[], instruments=items)
        self.rt.publish(topics.MD_STATUS, st, td=day)

    def _status_loop(self) -> None:
        while not self._stop.wait(self.STATUS_INTERVAL_S):
            hb = self.rt._hb_thread
            if self.quality and hb is not None and not hb._stop.is_set():  # paused heartbeats: a dead gateway
                self.publish_status()

    def _settle_inputs(self, command: str, p: Msg, ctx: RequestContext) -> HandlerResult:
        assert isinstance(p, SettleInputsRequest)
        self.settle_requests.append(p)
        return ok_reply(SettleInputsBatch(items=[]))


# ---------------------------------------------------------------------------------------- clearing-worker
class FakeClearing:
    """clearing-worker double: ``settle.sod_snapshot`` from ``sods``, ``sys.eod`` publishing."""

    def __init__(self, cfg: FullConfig, home: Path, clock: MarketClock, sods: Iterable[SodSnapshot] = ()) -> None:
        self.cfg = cfg
        self.clock = clock
        self.rt = SyncServiceRuntime(CW, config=cfg, home=home, market_clock=clock, setup_logs=False)
        self.sods: dict[tuple[str, int], SodSnapshot] = {(s.trading_day, s.version): s for s in sods}
        self.requests: list[SodRequest] = []
        self.rt.add_handler("settle.sod_snapshot", self._sod)
        self.thread = ServiceThread(self.rt)

    def start(self) -> FakeClearing:
        self.thread.start()
        return self

    def stop(self) -> None:
        self.thread.stop()

    def add(self, s: SodSnapshot) -> None:
        self.sods[(s.trading_day, s.version)] = s

    def _sod(self, command: str, p: Msg, ctx: RequestContext) -> Reply:
        assert isinstance(p, SodRequest)
        self.requests.append(p)
        if p.version is not None:
            s = self.sods.get((p.trading_day, p.version))
        else:
            versions = sorted(v for (td, v) in self.sods if td == p.trading_day)
            s = self.sods.get((p.trading_day, versions[-1])) if versions else None
        if s is None:
            return err_reply(codes.NOT_FOUND, f"no SOD for {p.trading_day} v{p.version}")
        if p.account_ids:
            s = SodSnapshot(trading_day=s.trading_day, version=s.version, from_settlement=s.from_settlement,
                            accounts=[a for a in s.accounts if a.account_id in p.account_ids], pre_settle=s.pre_settle)
        return ok_reply(s)

    def eod(self, trading_day: str, phase: EodPhase = EodPhase.DONE) -> None:
        self.rt.publish(topics.SYS_EOD, EodPhaseEvent(trading_day=trading_day, version=1, phase=phase,
                                                      ts_ns=self.clock.now_ns()), td=trading_day)


# ---------------------------------------------------------------------------------------- the shell
class Shell:
    """The real risk-engine service on a business thread."""

    def __init__(self, cfg: FullConfig, home: Path, clock: MarketClock, shard: int = 1, **kw: Any) -> None:
        kw.setdefault("max_wait_ms", 10)
        # the fakes tick only on demand: a start waits this long for LIVE ticks (snapshot prices never count as fresh)
        kw.setdefault("warmup_marks_timeout_s", 0.3)
        kw.setdefault("recon_retry_s", 0.5)
        kw.setdefault("sod_retry_s", 0.3)
        kw.setdefault("sod_not_found_retry_s", 0.5)
        kw.setdefault("maintenance_retry_s", 0.2)
        self.cfg = cfg
        self.home = home
        self.rt = SyncServiceRuntime(f"risk-engine-{shard}", config=cfg, home=home, market_clock=clock, setup_logs=False)
        self.svc = RiskEngineService(self.rt, shard=shard, **kw)
        self.svc.on_step = self._drain
        self._q: queue.SimpleQueue[tuple[Callable[[], Any], Future[Any]]] = queue.SimpleQueue()
        self.exit_code: int | None = None
        self._thread = threading.Thread(target=self._main, name=f"business:risk-engine-{shard}", daemon=True)

    def _main(self) -> None:
        self.exit_code = self.svc.run()

    def _drain(self) -> None:
        while True:
            try:
                fn, fut = self._q.get_nowait()
            except queue.Empty:
                return
            try:
                fut.set_result(fn())
            except BaseException as exc:
                fut.set_exception(exc)

    def start(self, phase: Phase | None = Phase.RUNNING, timeout: float = 30.0) -> Shell:
        self._thread.start()
        if phase is not None:
            self.wait_phase(phase, timeout)
        return self

    def wait_phase(self, phase: Phase, timeout: float = 30.0) -> None:
        order = list(Phase)
        wait_for(lambda: order.index(self.svc.phase) >= order.index(phase) or not self._thread.is_alive(), timeout,
                 what=f"phase {phase}")
        assert self._thread.is_alive(), f"shell died (exit {self.exit_code})"

    def call(self, fn: Callable[[], T], timeout: float = WAIT_S) -> T:
        fut: Future[T] = Future()
        self._q.put((fn, fut))
        return fut.result(timeout)

    @property
    def core(self) -> Any:
        return self.svc.core

    def alive(self) -> bool:
        return self._thread.is_alive()

    def join(self, timeout: float = 20.0) -> None:
        self._thread.join(timeout)

    def stop(self, timeout: float = 20.0) -> None:
        if self._thread.is_alive():
            self.rt.request_stop("test")
            self._thread.join(timeout)

    def crash(self, timeout: float = 20.0) -> None:
        """Unclean stop: fatal error path (exit code 1, databases closed with clean_shutdown=0)."""
        if self._thread.is_alive():
            self._q.put((self.svc.crash, Future()))
            self._thread.join(timeout)


# ---------------------------------------------------------------------------------------- the environment
ALL_ACCOUNTS: dict[str, tuple[str, list[SodLot]]] = {
    "ACC1": ("10000000", []),
    "ACC2": ("20000000", []),
    "ACC3": ("10000000", [lot("rb2701", 300, 3100.0)]),
    "ACC4": ("10000000", [lot("IF2612", 5, 4500.0)]),
    "ACC5": ("5000000", []),
}


def all_sod(**kw: Any) -> SodSnapshot:
    return sod(ALL_ACCOUNTS, **kw)


def yd_positions(accounts: dict[str, tuple[str, list[SodLot]]] = ALL_ACCOUNTS) -> dict[tuple[str, str, PosDirection], int]:
    out: dict[tuple[str, str, PosDirection], int] = {}
    for a, (_cap, lots) in accounts.items():
        for lt in lots:
            key = (a, lt.instrument_id, lt.direction)
            out[key] = out.get(key, 0) + lt.qty
    return out


@dataclasses.dataclass
class Env:
    """Fakes on real loopback sockets, a demo market clock and DEALER clients; shells started on demand."""

    cfg: FullConfig
    clock: MarketClock
    home: Path
    tg: FakeTradeGateway
    md: FakeMdGateway
    cw: FakeClearing
    api: BlockingRpcClient  # api-gateway
    bot: BlockingRpcClient  # strategy-sim
    clr: BlockingRpcClient  # clearing-worker
    shells: list[Shell] = dataclasses.field(default_factory=list)

    @property
    def clients(self) -> list[BlockingRpcClient]:
        return [self.api, self.bot, self.clr]

    def shell(self, shard: int = 1, *, phase: Phase | None = Phase.RUNNING, home: Path | None = None, **kw: Any) -> Shell:
        """Start shard ``shard``; with ``phase``, return once it is reachable from every client in that phase."""
        sh = Shell(self.cfg, home or self.home / f"re{shard}", self.clock, shard, **kw)
        self.shells.append(sh)
        sh.start(phase)
        if phase is not None:
            wait_ready(self.clients, f"risk-engine-{shard}", phase)
        return sh

    def restart(self, sh: Shell, *, crash: bool = False, phase: Phase | None = Phase.RUNNING, **kw: Any) -> Shell:
        shard = sh.svc.shard
        if crash:
            sh.crash()
        else:
            sh.stop()
        assert not sh.alive()
        return self.shell(shard, phase=phase, home=sh.home, **kw)

    def close(self) -> None:
        for sh in self.shells:
            sh.stop()
        for c in (self.api, self.bot, self.clr):
            c.close()
        for f in (self.tg, self.md, self.cw):
            f.stop()


def build_env(tmp_path: Path, default_config: FullConfig, *, rules: list[RuleDef] | None = None,
              pretrade: dict[str, Any] | None = None) -> Env:
    cfg = make_config(default_config, rules=rules)
    if pretrade:
        rr = cfg.risk_rules.model_copy(update={"pretrade": cfg.risk_rules.pretrade.model_copy(update=pretrade)})
        cfg = dataclasses.replace(cfg, risk_rules=rr)
    clock = demo_clock()
    tg = FakeTradeGateway(cfg, tmp_path / "tg", clock, positions=yd_positions()).start()
    md = FakeMdGateway(cfg, tmp_path / "md", clock).start()
    cw = FakeClearing(cfg, tmp_path / "cw", clock, sods=[all_sod()]).start()
    md.ticks({"rb2701": 3100.0, "IF2612": 4500.0})  # (md.status: both OK)
    return Env(cfg=cfg, clock=clock, home=tmp_path, tg=tg, md=md, cw=cw, api=client(cfg, "api-gateway"),
               bot=client(cfg, "strategy-sim"), clr=client(cfg, "clearing-worker"))


__all__ = [
    "ALL_ACCOUNTS",
    "CW",
    "MARKET_OPEN",
    "MD",
    "NTD",
    "PEER_TIMEOUT_MISSED",
    "PRE",
    "PREV_TD",
    "RETRY_WITHIN_S",
    "SEC",
    "TD",
    "TG",
    "WAIT_S",
    "Env",
    "FakeClearing",
    "FakeMdGateway",
    "FakeOrder",
    "FakeTradeGateway",
    "HeartbeatControl",
    "ServiceThread",
    "Shell",
    "all_sod",
    "body",
    "build_env",
    "client",
    "daily_loss_rule",
    "demo_clock",
    "firm_loss_rule",
    "jump",
    "lot",
    "lv_seq",
    "make_config",
    "replay",
    "request",
    "shard0_sod",
    "shard1_sod",
    "sod",
    "status",
    "uuid7",
    "wait_for",
    "wait_ready",
    "yd_positions",
]
