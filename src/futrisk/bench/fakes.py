"""Fake peers of the risk-engine for the multi-process benches (run in their own process by :mod:`.probe`).

Each fake is a real :class:`SyncServiceRuntime` on the world's loopback endpoints (heartbeats, ``ctl.*``,
LV cache, outbox), so the risk-engine talks to them exactly as to the real services:

* :class:`FakeSupervisor` - publishes the demo ``sys.clock`` (served to ``ctl.snapshot``);
* :class:`FakeClearing` - ``settle.sod_snapshot`` from the bench book;
* :class:`FakeTradeGateway` - ``route.order`` answered at once (``RouteAck`` SUBMITTED), the receipt
  time of every route recorded; after the reply the order is ACCEPTED and (optionally) filled through
  ``td.exec.{account}`` FACTs in one outbox transaction; ``td.query.trades / orders / positions`` and
  ``td.digest`` answered from memory (positions = SOD + fills, so the warm-up recon matches);
* :class:`FakeMdGateway` - ``md.status`` (all OK) every second, one tick per instrument every
  ``idle_ms``, and rate-controlled bursts whose ticks carry ``corr_id = "{tag}:{perf_counter_ns}"``
  taken right before the publish (the risk-engine probe measures publish -> decision with it).

Adapted from the risk-engine test fakes (``tests/unit/risk_engine/re_support.py``).
"""

from __future__ import annotations

import contextlib
import dataclasses
import itertools
import queue
import random
import threading
import time
from collections.abc import Callable
from concurrent.futures import Future
from pathlib import Path
from typing import Any, TypeVar

from sqlalchemy import MetaData, Table

from futrisk.bench.core import now_ns
from futrisk.bench.scenario import INSTRUMENTS, TD, WorldSpec, instrument_ids, sod_snapshot, yd_positions
from futrisk.bus.rpc_common import HandlerResult, RequestContext
from futrisk.common.clock import MarketClock
from futrisk.common.config import FullConfig
from futrisk.common.decimals import D
from futrisk.core.instruments import InstrumentRegistry
from futrisk.db import Database, add_common_tables
from futrisk.db.types import id_col
from futrisk.messages import codes, topics
from futrisk.messages.base import Msg
from futrisk.messages.enums import (
    BrokerOrderStatus,
    ClockMode,
    Direction,
    MdQuality,
    Offset,
    PosDirection,
    SourceId,
    TradeOrigin,
)
from futrisk.messages.envelope import Reply, err_reply, ok_reply
from futrisk.messages.eod import SodRequest, SodSnapshot
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
from futrisk.messages.system import ClockState
from futrisk.runtime import SyncServiceRuntime

T = TypeVar("T")
TG, MD, CW, SUP = "trade-gateway", "md-gateway", "clearing-worker", "supervisor"


class ServiceThread:
    """A SyncServiceRuntime running ``rt.run`` on its own thread; ``call`` runs code on that thread."""

    def __init__(self, rt: SyncServiceRuntime, setup: Callable[[], None] | None = None,
                 step: Callable[[], None] | None = None) -> None:
        self.rt = rt
        self._setup = setup
        self._step_extra = step
        self._q: queue.SimpleQueue[tuple[Callable[[], Any], Future[Any]]] = queue.SimpleQueue()
        self._started = threading.Event()
        self.error: BaseException | None = None
        self._thread = threading.Thread(target=self._main, name=f"business:{rt.instance}", daemon=True)

    def start(self, timeout: float = 30.0) -> ServiceThread:
        self._thread.start()
        if not self._started.wait(timeout):
            raise TimeoutError(f"{self.rt.instance} did not start")
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
                break
            try:
                fut.set_result(fn())
            except BaseException as exc:
                fut.set_exception(exc)
        if self._step_extra is not None:
            self._step_extra()

    def call(self, fn: Callable[[], T], timeout: float = 30.0) -> T:
        fut: Future[T] = Future()
        self._q.put((fn, fut))
        return fut.result(timeout)

    def alive(self) -> bool:
        return self._thread.is_alive()

    def stop(self, timeout: float = 20.0) -> None:
        if self._thread.is_alive():
            self.rt.request_stop("bench")
            self._thread.join(timeout)


# ---------------------------------------------------------------------------------------- supervisor
class FakeSupervisor:
    """Publishes the demo ``sys.clock`` once; the runtime's LV cache answers the followers' ``ctl.snapshot``."""

    def __init__(self, cfg: FullConfig, home: Path, clock: MarketClock, *, parent_pid: int | None = None) -> None:
        self.clock = clock
        self.rt = SyncServiceRuntime(SUP, config=cfg, home=home, market_clock=clock, setup_logs=False,
                                     parent_pid=parent_pid)
        self.thread = ServiceThread(self.rt)

    def start(self) -> FakeSupervisor:
        self.thread.start()
        self.rt.publish(topics.SYS_CLOCK, ClockState(mode=ClockMode.DEMO, offset_ns=self.clock.offset_ns, version=1,
                                                     market_now_ns=self.clock.now_ns()))
        return self

    def stop(self) -> None:
        self.thread.stop()


# ---------------------------------------------------------------------------------------- clearing-worker
class FakeClearing:
    """``settle.sod_snapshot`` from the bench SOD."""

    def __init__(self, cfg: FullConfig, home: Path, clock: MarketClock, sod: SodSnapshot, *,
                 parent_pid: int | None = None) -> None:
        self.sod = sod
        self.rt = SyncServiceRuntime(CW, config=cfg, home=home, market_clock=clock, setup_logs=False,
                                     parent_pid=parent_pid)
        self.rt.add_handler("settle.sod_snapshot", self._sod)
        self.requests = 0
        self.thread = ServiceThread(self.rt)

    def start(self) -> FakeClearing:
        self.thread.start()
        return self

    def stop(self) -> None:
        self.thread.stop()

    def _sod(self, command: str, p: Msg, ctx: RequestContext) -> Reply:
        assert isinstance(p, SodRequest)
        self.requests += 1
        s = self.sod
        if p.trading_day != s.trading_day or (p.version is not None and p.version != s.version):
            return err_reply(codes.NOT_FOUND, f"no SOD for {p.trading_day} v{p.version}")
        if p.account_ids:
            wanted = set(p.account_ids)
            s = SodSnapshot(trading_day=s.trading_day, version=s.version, from_settlement=s.from_settlement,
                            accounts=[a for a in s.accounts if a.account_id in wanted], pre_settle=s.pre_settle)
        return ok_reply(s)


# ---------------------------------------------------------------------------------------- trade-gateway
@dataclasses.dataclass
class _Order:
    route: RouteOrder
    status: BrokerOrderStatus
    ref: str
    filled: int = 0

    @property
    def remaining(self) -> int:
        return max(0, int(self.route.qty) - self.filled)


class FakeTradeGateway:
    """A fast trade-gateway double: RouteAck at once, fills afterwards through a real outbox (``td.exec`` FACTs)."""

    def __init__(self, cfg: FullConfig, home: Path, clock: MarketClock, *, trading_day: str = TD,
                 positions: dict[tuple[str, str, PosDirection], int] | None = None, auto_fill: bool = True,
                 fill_parts: int = 1, parent_pid: int | None = None) -> None:
        self.cfg = cfg
        self.clock = clock
        self.trading_day = trading_day
        self.registry = InstrumentRegistry(cfg.products, cfg.calendar)
        self.rt = SyncServiceRuntime(TG, config=cfg, home=home, market_clock=clock, setup_logs=False,
                                     parent_pid=parent_pid)
        self.md = MetaData()
        add_common_tables(self.md, fact_cursors=False)
        Table("noop", self.md, id_col("k", primary_key=True))
        self.db: Database | None = None
        self.lock = threading.RLock()
        self.orders: dict[str, _Order] = {}
        self.trades_by_account: dict[str, list[Trade]] = {}
        self.n_trades = 0
        self.yd: dict[tuple[str, str, PosDirection], int] = dict(positions or {})
        self.auto_fill = auto_fill
        self.fill_parts = max(1, fill_parts)
        self.route_recv_ns: dict[str, int] = {}  # client_order_id -> perf_counter_ns at the route.order handler
        self.record_routes = False
        self._pending: list[str] = []  # accepted orders whose FACTs follow the reply (loop thread)
        self._trade_seq = itertools.count(1)
        self._broker_seq = itertools.count(1)
        self._ref = itertools.count(1)
        self.facts = 0
        for cmd, fn in (("route.order", self._route), ("route.cancel", self._cancel), ("route.resolve", self._resolve),
                        ("td.query.trades", self._q_trades), ("td.query.orders", self._q_orders),
                        ("td.query.positions", self._q_positions), ("td.digest", self._digest)):
            self.rt.add_handler(cmd, fn)
        self.thread = ServiceThread(self.rt, self._setup, self._after_replies)

    def _setup(self) -> None:
        self.db = self.rt.open_database("td", self.md, synchronous="NORMAL")

    def start(self) -> FakeTradeGateway:
        self.thread.start()
        return self

    def stop(self) -> None:
        self.thread.stop()

    # ------------------------------------------------------------------ after the replies (loop thread)
    def _after_replies(self) -> None:
        if not self._pending:
            return
        pending, self._pending = self._pending, []
        msgs: list[Trade | BrokerOrderUpdate] = []
        for coid in pending:
            with self.lock:
                o = self.orders[coid]
            msgs.append(self._update(o, BrokerOrderStatus.ACCEPTED))
            if self.auto_fill:
                qty = int(o.route.qty)
                parts = min(self.fill_parts, qty)
                for k in range(parts):
                    q = qty // parts + (1 if k < qty % parts else 0)
                    msgs.append(self._fill(o, q))
                    msgs.append(self._update(o, BrokerOrderStatus.FILLED if o.remaining == 0
                                             else BrokerOrderStatus.PARTIALLY_FILLED))
        self._publish(msgs)

    def _publish(self, msgs: list[Trade | BrokerOrderUpdate]) -> None:
        db = self.db
        assert db is not None
        rt = self.rt

        def job(c: Any) -> None:
            for m in msgs:
                rt.append_fact(c, topics.td_exec(m.account_id), m, td=m.trading_day, corr_id=m.client_order_id)

        db.write(job)
        self.facts += len(msgs)

    def _update(self, o: _Order, status: BrokerOrderStatus) -> BrokerOrderUpdate:
        r = o.route
        o.status = status
        return BrokerOrderUpdate(client_order_id=r.client_order_id, account_id=r.account_id, instrument_id=r.instrument_id,
                                 broker_order_ref=o.ref, status=status, filled_qty=o.filled,
                                 remaining_qty=o.remaining if status not in (BrokerOrderStatus.CANCELED,
                                                                             BrokerOrderStatus.REJECTED) else 0,
                                 broker_seq=next(self._broker_seq), trading_day=r.trading_day,
                                 updated_ns=self.clock.now_ns())

    def _fill(self, o: _Order, qty: int) -> Trade:
        r = o.route
        o.filled += qty
        seq = next(self._trade_seq)
        now = self.clock.now_ns()
        t = Trade(trading_day=r.trading_day, exchange=r.exchange, trade_id=f"T{seq:07d}", direction=r.direction,
                  account_id=r.account_id, instrument_id=r.instrument_id, client_order_id=r.client_order_id,
                  origin=TradeOrigin.INTERNAL, broker_order_ref=o.ref, offset=r.offset,
                  price=float(r.price if r.price is not None else INSTRUMENTS.get(r.instrument_id, 0.0)), qty=qty,
                  trade_ts_ns=now, broker_seq=next(self._broker_seq), trade_seq=seq, received_ns=now)
        with self.lock:
            self.trades_by_account.setdefault(t.account_id, []).append(t)
            self.n_trades += 1
        return t

    # ------------------------------------------------------------------ handlers (loop thread)
    def _route(self, command: str, p: Msg, ctx: RequestContext) -> HandlerResult:
        t = now_ns()
        assert isinstance(p, RouteOrder)
        coid = p.client_order_id
        if self.record_routes:
            self.route_recv_ns.setdefault(coid, t)
        with self.lock:
            existing = self.orders.get(coid)
            if existing is not None:
                return ok_reply(RouteAck(client_order_id=coid, status=existing.status, broker_order_ref=existing.ref),
                                duplicate=True)
            o = _Order(route=p, status=BrokerOrderStatus.SUBMITTED, ref=f"1:{next(self._ref)}")
            self.orders[coid] = o
        self._pending.append(coid)
        return ok_reply(RouteAck(client_order_id=coid, status=BrokerOrderStatus.SUBMITTED, broker_order_ref=o.ref))

    def _cancel(self, command: str, p: Msg, ctx: RequestContext) -> HandlerResult:
        assert isinstance(p, RouteCancel)
        with self.lock:
            o = self.orders.get(p.client_order_id)
        if o is None:
            return err_reply(codes.ROUTE_LOST, data=RouteAck(client_order_id=p.client_order_id,
                                                             status=BrokerOrderStatus.EXPIRED))
        if o.status in (BrokerOrderStatus.FILLED, BrokerOrderStatus.CANCELED, BrokerOrderStatus.REJECTED):
            return err_reply(codes.BROKER_ORDER_FINISHED, data=RouteAck(client_order_id=p.client_order_id,
                                                                        status=o.status, broker_order_ref=o.ref))
        self._publish([self._update(o, BrokerOrderStatus.CANCELED)])
        return ok_reply(RouteAck(client_order_id=p.client_order_id, status=BrokerOrderStatus.CANCEL_PENDING,
                                 broker_order_ref=o.ref))

    def _resolve(self, command: str, p: Msg, ctx: RequestContext) -> HandlerResult:
        assert isinstance(p, RouteResolve)
        with self.lock:
            o = self.orders.get(p.client_order_id)
        if o is None:
            return err_reply(codes.ROUTE_LOST, data=RouteAck(client_order_id=p.client_order_id,
                                                             status=BrokerOrderStatus.EXPIRED))
        return ok_reply(RouteAck(client_order_id=p.client_order_id, status=o.status, broker_order_ref=o.ref))

    def _q_trades(self, command: str, p: Msg, ctx: RequestContext) -> HandlerResult:
        assert isinstance(p, TradeQuery)
        with self.lock:
            if p.account_id is not None:
                pool = list(self.trades_by_account.get(p.account_id, ()))
            else:
                pool = [t for ts in self.trades_by_account.values() for t in ts]
        rows = [t for t in pool if t.trading_day == p.trading_day
                and (p.from_trade_seq is None or t.trade_seq >= p.from_trade_seq)]
        rows.sort(key=lambda t: t.trade_seq)
        page = rows[: p.limit]
        nxt = (page[-1].trade_seq + 1) if page else (p.from_trade_seq or 1)
        return ok_reply(TradeBatch(trades=page, next_trade_seq=nxt, complete=len(rows) <= p.limit))

    def _q_orders(self, command: str, p: Msg, ctx: RequestContext) -> HandlerResult:
        assert isinstance(p, OrderQuery)
        with self.lock:
            items = [BrokerOrderUpdate(client_order_id=c, account_id=o.route.account_id,
                                       instrument_id=o.route.instrument_id, broker_order_ref=o.ref, status=o.status,
                                       filled_qty=o.filled, remaining_qty=o.remaining, broker_seq=0,
                                       trading_day=o.route.trading_day, updated_ns=self.clock.now_ns())
                     for c, o in self.orders.items()
                     if (p.account_id is None or o.route.account_id == p.account_id)
                     and (not p.client_order_ids or c in p.client_order_ids)]
        return ok_reply(BrokerOrderBatch(items=items))

    def positions(self, trading_day: str | None = None) -> list[BrokerPosition]:
        """Positions after today's trades (in trade order): ``CLOSE_TODAY`` / ``CLOSE_YESTERDAY`` explicit; a plain
        ``CLOSE`` takes yesterday's lots on exchanges with explicit close-today (SHFE), else yesterday's first (FIFO)."""
        td_pos: dict[tuple[str, str, PosDirection], int] = {}
        yd = dict(self.yd)
        with self.lock:
            trades = sorted((t for ts in self.trades_by_account.values() for t in ts
                             if t.trading_day == (trading_day or self.trading_day)), key=lambda t: t.trade_seq)
        for t in trades:
            if t.offset == Offset.OPEN:
                key = (t.account_id, t.instrument_id, PosDirection.LONG if t.direction == Direction.BUY else PosDirection.SHORT)
                td_pos[key] = td_pos.get(key, 0) + t.qty
                continue
            key = (t.account_id, t.instrument_id, PosDirection.SHORT if t.direction == Direction.BUY else PosDirection.LONG)
            if t.offset == Offset.CLOSE_TODAY:
                td_pos[key] = td_pos.get(key, 0) - t.qty
            elif t.offset == Offset.CLOSE_YESTERDAY or self._rule(t.exchange, "explicit_close_today", False):
                yd[key] = yd.get(key, 0) - t.qty
            elif self._rule(t.exchange, "close_style", "FIFO") == "TODAY_FIRST":
                take = min(max(0, td_pos.get(key, 0)), t.qty)
                td_pos[key] = td_pos.get(key, 0) - take
                yd[key] = yd.get(key, 0) - (t.qty - take)
            else:  # FIFO: yesterday's lots first
                take = min(max(0, yd.get(key, 0)), t.qty)
                yd[key] = yd.get(key, 0) - take
                td_pos[key] = td_pos.get(key, 0) - (t.qty - take)
        out = []
        for key in sorted(set(td_pos) | set(yd), key=lambda k: (k[0], k[1], k[2].value)):
            tq, yq = td_pos.get(key, 0), yd.get(key, 0)
            if tq or yq:
                out.append(BrokerPosition(account_id=key[0], instrument_id=key[1], direction=key[2], td_qty=tq, yd_qty=yq))
        return out

    def _rule(self, exchange: Any, name: str, default: Any) -> Any:
        rules = self.cfg.products.exchanges.get(exchange)
        return getattr(rules, name, default) if rules is not None else default

    def _q_positions(self, command: str, p: Msg, ctx: RequestContext) -> HandlerResult:
        assert isinstance(p, PositionQuery)
        items = [x for x in self.positions(p.trading_day) if p.account_id is None or x.account_id == p.account_id]
        return ok_reply(BrokerPositionBatch(trading_day=p.trading_day, as_of_ns=self.clock.now_ns(), items=items))

    def _digest(self, command: str, p: Msg, ctx: RequestContext) -> HandlerResult:
        assert isinstance(p, DigestRequest)
        with self.lock:
            trades = [t for ts in self.trades_by_account.values() for t in ts if t.trading_day == p.trading_day]
        accts = sorted({t.account_id for t in trades})
        return ok_reply(TradeDigest(trading_day=p.trading_day,
                                    accounts=[compute_account_digest(a, [t for t in trades if t.account_id == a])
                                              for a in accts],
                                    total_count=len(trades), generated_ns=self.clock.now_ns()))

    def stats(self) -> dict[str, Any]:
        with self.lock:
            return {"orders": len(self.orders), "trades": self.n_trades, "facts": self.facts,
                    "routes_recorded": len(self.route_recv_ns)}


# ---------------------------------------------------------------------------------------- md-gateway
class FakeMdGateway:
    """LV ticks and ``md.status`` on a real PUB; idle ticks between rate-controlled bursts."""

    STATUS_INTERVAL_S = 1.0

    def __init__(self, cfg: FullConfig, home: Path, clock: MarketClock, instruments: list[str], *, idle_ms: int = 200,
                 seed: int = 7, parent_pid: int | None = None) -> None:
        self.clock = clock
        self.instruments = list(instruments)
        self.idle_s = max(0.0, idle_ms / 1000.0)
        self.rt = SyncServiceRuntime(MD, config=cfg, home=home, market_clock=clock, setup_logs=False,
                                     parent_pid=parent_pid)
        self.registry = InstrumentRegistry(cfg.products, cfg.calendar)
        self.rt.add_handler("md.settle_inputs", self._settle_inputs)
        self.thread = ServiceThread(self.rt)
        self._rng = random.Random(seed)
        self._volume: dict[str, int] = {}
        self._static: dict[str, tuple[Any, float, float, float, float]] = {}
        for inst in self.instruments:
            info = self.registry.get(inst)
            pre = D(f"{INSTRUMENTS[inst]:g}")
            up, lo = self.registry.limit_prices(inst, pre, TD)
            self._static[inst] = (info.exchange, float(info.tick_size), float(up), float(lo), float(pre))
        self._burst = threading.Lock()  # held while a burst publishes (idle ticks pause)
        self._stop = threading.Event()
        self._idle_thread = threading.Thread(target=self._idle_loop, name="md-idle", daemon=True)
        self.published = 0

    def start(self) -> FakeMdGateway:
        self.thread.start()
        self.publish_status()
        self._idle_thread.start()
        return self

    def stop(self) -> None:
        self._stop.set()
        self._idle_thread.join(5)
        self.thread.stop()

    def make_tick(self, inst: str) -> Tick:
        exch, tick_size, up, lo, pre = self._static[inst]
        v = self._volume.get(inst, 0) + 1
        self._volume[inst] = v
        px = round(pre + tick_size * self._rng.randint(-3, 3), 6)
        now = self.clock.now_ns()
        return Tick(instrument_id=inst, exchange=exch, trading_day=TD, action_day=TD, exch_ts_ns=now, last=px, volume=v,
                    turnover=0.0, open_interest=0, bid1=px - tick_size, ask1=px + tick_size, upper_limit=up,
                    lower_limit=lo, pre_settle=pre, source=SourceId.FEED_A, recv_ns=now)

    def publish_status(self) -> None:
        st = MdStatus(as_of_ns=self.clock.now_ns(), trading_day=TD, sources=[],
                      instruments=[InstrumentStatus(instrument_id=i, quality=MdQuality.OK) for i in self.instruments])
        self.rt.publish(topics.MD_STATUS, st, td=TD)

    def _idle_loop(self) -> None:
        """Idle ticks (skipped while a burst runs) and ``md.status`` every second (also during bursts)."""
        status_ns = int(self.STATUS_INTERVAL_S * 1e9)
        next_status = now_ns() + status_ns
        next_idle = now_ns()
        while not self._stop.is_set():
            t = now_ns()
            if self.idle_s > 0 and t >= next_idle and self._burst.acquire(blocking=False):
                try:
                    for inst in self.instruments:
                        self.rt.publish(topics.md_tick(inst), self.make_tick(inst), td=TD, corr_id="i")
                        self.published += 1
                finally:
                    self._burst.release()
                next_idle = t + int(self.idle_s * 1e9)
            if t >= next_status:
                self.publish_status()
                next_status = t + status_ns
            self._stop.wait(0.02)

    def burst(self, rate: float, n: int, tag: str = "m") -> dict[str, Any]:
        """Publish ``n`` ticks at ``rate`` per second (round robin over the instruments), stamped for the probe.

        Pacing is by schedule: every tick due is sent, then the thread sleeps ~0.2 ms (Windows timer
        granularity ~0.5 ms), so ticks leave in small groups while the mean rate is exact.
        """
        period_ns = int(1e9 / rate) if rate > 0 else 0
        insts = self.instruments
        sent = 0
        with self._burst:
            t0 = now_ns()
            first = last = t0
            k = 0
            while sent < n and not self._stop.is_set():
                due = t0 + sent * period_ns
                t = now_ns()
                if period_ns and t < due:
                    time.sleep(0.0002 if due - t < 2_000_000 else (due - t) / 2e9)
                    continue
                inst = insts[k % len(insts)]
                k += 1
                tick = self.make_tick(inst)
                ts = now_ns()
                self.rt.publish(topics.md_tick(inst), tick, td=TD, corr_id=f"{tag}:{ts}")
                if sent == 0:
                    first = ts
                last = ts
                sent += 1
            self.published += sent
        if self.rt.publisher is not None:
            self.rt.publisher.flush(5.0)
        elapsed = max(1, last - first)
        return {"sent": sent, "t_first_ns": first, "t_last_ns": last, "elapsed_s": elapsed / 1e9,
                "rate": (sent - 1) / (elapsed / 1e9) if sent > 1 else 0.0}

    def _settle_inputs(self, command: str, p: Msg, ctx: RequestContext) -> HandlerResult:
        assert isinstance(p, SettleInputsRequest)
        return ok_reply(SettleInputsBatch(items=[]))


# ---------------------------------------------------------------------------------------- the whole set
class Fakes:
    """Every fake peer of a risk-engine shard for one bench world (in one process)."""

    def __init__(self, cfg: FullConfig, work: Path, spec: WorldSpec, offset_ns: int, *,
                 parent_pid: int | None = None) -> None:
        self.cfg = cfg
        self.spec = spec
        self.clock = MarketClock(mode="DEMO", offset_ns=offset_ns)
        self.sup = FakeSupervisor(cfg, work / "sup", self.clock, parent_pid=parent_pid)
        self.cw = FakeClearing(cfg, work / "cw", self.clock, sod_snapshot(spec), parent_pid=parent_pid)
        self.tg = FakeTradeGateway(cfg, work / "tg", self.clock, positions=yd_positions(spec),
                                   auto_fill=spec.tg_auto_fill, fill_parts=spec.tg_fill_parts, parent_pid=parent_pid)
        self.md = FakeMdGateway(cfg, work / "md", self.clock, instrument_ids(spec), idle_ms=spec.md_idle_ms,
                                parent_pid=parent_pid)

    def start(self) -> Fakes:
        self.sup.start()
        self.cw.start()
        self.tg.start()
        self.md.start()
        return self

    def stop(self) -> None:
        for f in (self.md, self.tg, self.cw, self.sup):
            with contextlib.suppress(Exception):  # best effort at shutdown
                f.stop()

    def alive(self) -> bool:
        return all(t.thread.alive() for t in (self.sup, self.cw, self.tg, self.md))


__all__ = ["FakeClearing", "FakeMdGateway", "FakeSupervisor", "FakeTradeGateway", "Fakes", "ServiceThread"]
