"""Stub services and scenario data for the clearing-worker tests.

Every stub is a real :class:`AsyncServiceRuntime` bound to its configured
(free) ports, so the clearing-worker talks to it over the real bus: FACTs
through the outbox (``td.exec.*``, ``risk.cash.*``), commands through the
ROUTER with the 02 §4.11 whitelist (``eod.*`` only from ``clearing-worker``).
Behaviour is scripted through plain attributes.

The risk-engine stand-in used by the harness is :class:`StrictShard`: it
enforces the engine's trading-day rules for ``eod.freeze`` / ``eod.rollover``
(current day, previous PROVISIONAL day and older unsettled days digest-only,
``CATCH_UP`` for skipped days, ``REBASE`` onto the current day, contiguous
chain of official SODs) and records every rejected request in ``violations``.
"""

from __future__ import annotations

import asyncio
import inspect
import time
from collections.abc import Callable
from concurrent.futures import Executor
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import zmq.asyncio
from sqlalchemy import MetaData, Table

from futrisk.bus.endpoints import Endpoints
from futrisk.bus.rpc_client import AsyncRpcClient
from futrisk.bus.rpc_common import RequestContext
from futrisk.common.clock import BJ, MarketClock, SystemClock, bj_to_ns
from futrisk.common.config import FullConfig
from futrisk.common.ids import uuid7
from futrisk.core import InstrumentRegistry, build_settle_input
from futrisk.core.eod import cash_digests as engine_cash_digests
from futrisk.db import add_common_tables
from futrisk.db.types import id_col
from futrisk.messages import codes, topics
from futrisk.messages.base import Msg
from futrisk.messages.enums import (
    AccountState,
    Action,
    CashType,
    DayRollMode,
    Direction,
    Exchange,
    MdEventKind,
    Offset,
    OrderSource,
    OrderStatus,
    OrderType,
    PosDirection,
    ProcessState,
    RiskEventKind,
    Role,
    ServiceChangeReason,
    Severity,
    SourceId,
    TimeInForce,
)
from futrisk.messages.envelope import Reply, err_reply, ok_reply
from futrisk.messages.eod import (
    DayRoll,
    EodFreezeAck,
    EodFreezeRequest,
    EodRolloverRequest,
    SeedSodRequest,
    SettleRunRequest,
    SodAccount,
    SodLot,
    SodRequest,
    SodSnapshot,
)
from futrisk.messages.md import (
    GapInfo,
    MdEvent,
    MdEventBatch,
    MdEventsQuery,
    SettleInput,
    SettleInputsBatch,
    SettleInputsRequest,
)
from futrisk.messages.orders import (
    DigestRequest,
    OrderUpdate,
    Trade,
    TradeBatch,
    TradeDigest,
    TradeQuery,
    compute_account_digest,
)
from futrisk.messages.risk import (
    CashBatch,
    CashMovement,
    MetricPoint,
    MetricSeries,
    OrderBatch,
    QueryAlerts,
    QueryCash,
    QueryMetrics,
    QueryOrders,
    RiskEvent,
    RiskEventBatch,
)
from futrisk.messages.system import ServiceEventBatch, ServiceStateChanged, SvcHistoryRequest
from futrisk.runtime import AsyncServiceRuntime
from futrisk.services.clearing.service import ClearingService
from futrisk.services.clearing.settings import ClearingSettings

D = "20260928"  # Monday after the Mid-Autumn holiday (no night session)
PREV = "20260924"
NEXT = "20260929"
CW = "clearing-worker"

FAST = ClearingSettings(
    freeze_timeout_ms=2_000,
    freeze_retries=1,
    rollover_timeout_ms=3_000,
    rollover_retries=1,
    repush_interval_s=0.5,
    query_timeout_ms=2_000,
    query_retries=1,
    md_timeout_ms=2_000,
    md_retries=1,
    report_fetch_timeout_ms=1_500,
    scheduler_enabled=False,
    scheduler_interval_s=0.2,
    pool_workers=2,
    progress_poll_s=0.05,
)


def ts(text: str) -> int:
    """Beijing ``YYYY-MM-DD HH:MM[:SS]`` -> UTC ns."""
    return int(bj_to_ns(datetime.fromisoformat(text).replace(tzinfo=BJ)))


def market_clock_at(text: str) -> MarketClock:
    """A DEMO market clock whose market time is ``text`` (Beijing) now and runs in real time."""
    wall = SystemClock()
    return MarketClock(wall=wall, mode="DEMO", offset_ns=ts(text) - wall.now_ns(), version=1)


async def eventually(pred: Callable[[], Any], timeout: float = 10.0, step: float = 0.02) -> None:
    deadline = time.monotonic() + timeout
    while True:
        res = pred()
        if inspect.isawaitable(res):
            res = await res
        if res:
            return
        if time.monotonic() > deadline:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(step)


def stub_schema() -> MetaData:
    md = MetaData()
    add_common_tables(md, fact_cursors=False)
    Table("stub", md, id_col("k", primary_key=True))
    return md


# ------------------------------------------------------------------- scenario
class Scenario:
    """SOD, trades, cash and md inputs of trading day ``D`` (default config: ACC1..ACC5, two shards)."""

    def __init__(self, cfg: FullConfig) -> None:
        self.cfg = cfg
        self.registry = InstrumentRegistry(cfg.products, cfg.calendar)
        self._seq = 0
        self.pre_settle = {"rb2701": "3110", "rb2705": "3140", "IF2612": "4462.2", "cu2611": "78500"}
        self.trades: list[Trade] = [
            self.trade("ACC1", "rb2701", Direction.SELL, Offset.CLOSE_YESTERDAY, 3120.0, 4, "2026-09-28 09:30:00"),
            self.trade("ACC1", "rb2701", Direction.BUY, Offset.OPEN, 3125.0, 2, "2026-09-28 10:00:00"),
            self.trade("ACC3", "IF2612", Direction.BUY, Offset.CLOSE, 4450.0, 2, "2026-09-28 14:30:00"),
            self.trade("ACC4", "IF2612", Direction.BUY, Offset.OPEN, 4455.2, 1, "2026-09-28 10:30:00"),
            self.trade("ACC5", "rb2705", Direction.SELL, Offset.OPEN, 3150.0, 3, "2026-09-28 11:00:00"),
        ]
        self.cash: list[CashMovement] = [
            CashMovement(
                movement_id=uuid7(),
                account_id="ACC3",
                trading_day=D,
                type=CashType.DEPOSIT,
                amount="100000.00",
                actor="ops",
                reason="top up",
                ts_ns=ts("2026-09-28 09:05:00"),
            ),
            CashMovement(
                movement_id=uuid7(),
                account_id="ACC3",
                trading_day=D,
                type=CashType.WITHDRAW,
                amount="30000.00",
                actor="ops",
                reason="fee payment",
                ts_ns=ts("2026-09-28 13:05:00"),
            ),
        ]

    def trade(
        self,
        acct: str,
        inst: str,
        direction: Direction,
        offset: Offset,
        price: float,
        qty: int,
        when: str,
        td: str = D,
    ) -> Trade:
        self._seq += 1
        return Trade(
            trading_day=td,
            exchange=self.registry.exchange(inst),
            trade_id=f"T{self._seq:04d}",
            direction=direction,
            account_id=acct,
            instrument_id=inst,
            client_order_id=uuid7(),
            broker_order_ref=f"1:1:{self._seq}",
            offset=offset,
            price=price,
            qty=qty,
            trade_ts_ns=ts(when),
            broker_seq=self._seq,
            trade_seq=self._seq,
            received_ns=ts(when) + 1_000,
        )

    def seed_accounts(self) -> list[SodAccount]:
        cfg = self.cfg.accounts.accounts
        lots = {
            "ACC1": [SodLot(instrument_id="rb2701", direction=PosDirection.LONG, lot_no=1, open_td="20260924",
                            open_ts_ns=ts("2026-09-24 10:00:00"), open_price="3100", qty=10, basis_price="3110")],
            "ACC3": [SodLot(instrument_id="IF2612", direction=PosDirection.SHORT, lot_no=1, open_td="20260923",
                            open_ts_ns=ts("2026-09-23 10:00:00"), open_price="4500", qty=2, basis_price="4462.2")],
            "ACC5": [SodLot(instrument_id="rb2701", direction=PosDirection.LONG, lot_no=1, open_td="20260924",
                            open_ts_ns=ts("2026-09-24 11:00:00"), open_price="3105", qty=3, basis_price="3110")],
        }
        out = []
        for acct in sorted(cfg):
            cap = str(Decimal(cfg[acct].initial_capital).quantize(Decimal("0.01")))
            out.append(SodAccount(account_id=acct, pre_balance=cap, initial_capital=cap, cum_net_deposit="0.00",
                                  hwm_equity=cap, lots=lots.get(acct, [])))
        return out

    def seed_request(self, command_id: str | None = None, trading_day: str = D) -> SeedSodRequest:
        return SeedSodRequest(
            command_id=command_id or uuid7(),
            trading_day=trading_day,
            accounts=self.seed_accounts(),
            pre_settle=dict(self.pre_settle),
            reason="first day",
            actor="admin",
            role=Role.ADMIN,
        )

    def settle_inputs(self) -> dict[str, SettleInput]:
        def tape(start: str, prices: list[tuple[float, int]], step_s: int = 60) -> list[tuple[int, float, int]]:
            t0 = ts(start)
            return [(t0 + i * step_s * 1_000_000_000, p, q) for i, (p, q) in enumerate(prices)]

        reg = self.registry
        return {
            "rb2701": build_settle_input(
                reg, "rb2701", D, tape("2026-09-28 09:10", [(3117, 400), (3119, 300), (3121, 300)]), pre_settle="3110"
            ),
            "rb2705": build_settle_input(
                reg, "rb2705", D, tape("2026-09-28 09:20", [(3149, 200), (3152, 100)]), pre_settle="3140"
            ),
            "IF2612": build_settle_input(
                reg, "IF2612", D, tape("2026-09-28 14:05", [(4448.2, 30), (4449.6, 20), (4447.8, 50)]), pre_settle="4462.2"
            ),
            "cu2611": build_settle_input(reg, "cu2611", D, [], pre_settle="78500", bid1="78400", ask1="78700"),
        }


# ---------------------------------------------------------------------- stubs
class StubBase:
    def __init__(self, cfg: FullConfig, home: Path, instance: str) -> None:
        self.cfg = cfg
        self.instance = instance
        self.home = home
        self.rt = AsyncServiceRuntime(instance, config=cfg, home=home, market_clock=MarketClock(), setup_logs=False)
        self.db: Any = None

    def handlers(self) -> dict[str, Callable[..., Any]]:
        return {}

    async def start(self, *, with_db: bool = False) -> None:
        self.rt.serve(self.handlers())
        await self.rt.start()
        if with_db:
            self.db = await self.rt.open_database(f"stub-{self.instance}", stub_schema(), synchronous="NORMAL")

    async def stop(self) -> None:
        await self.rt.stop()

    async def publish(self, topic: str, body: Msg, td: str) -> None:
        await self.db.run_write(lambda c: self.rt.append_fact(c, topic, body, td=td))


class StubShard(StubBase):
    """risk-engine-k: ``eod.freeze`` / ``eod.rollover`` (fetching ``settle.sod_snapshot``) / ``risk.query.*``."""

    def __init__(self, cfg: FullConfig, home: Path, instance: str) -> None:
        super().__init__(cfg, home, instance)
        self.shard = int(instance.rsplit("-", 1)[1])
        self.accounts = cfg.accounts.accounts_of_shard(self.shard)
        self.fills: list[Trade] = []
        self.cash: list[CashMovement] = []
        self.missing_keys: set[str] = set()  # fills left out of the ack ...
        self.stale_acks: int | None = 0  # ... for this many freeze calls (None: always)
        self.freeze_calls: list[EodFreezeRequest] = []
        self.rollovers: list[EodRolloverRequest] = []
        self.snapshots: list[SodSnapshot] = []
        self.rolled: dict[str, int] = {}
        self.rollover_gate: asyncio.Event | None = None
        self.freeze_gate: asyncio.Event | None = None
        self.freeze_gate_skip = 0  # the gate holds only freeze calls after this many
        self.fail_rollovers = 0  # the next N eod.rollover calls answer UNAVAILABLE
        self.alerts: list[RiskEvent] = []
        self.orders: list[OrderUpdate] = []
        self.metrics: dict[str, list[MetricPoint]] = {}

    def handlers(self) -> dict[str, Callable[..., Any]]:
        return {
            "eod.freeze": self.on_freeze,
            "eod.rollover": self.on_rollover,
            "risk.query.cash": self.on_cash,
            "risk.query.alerts": self.on_alerts,
            "risk.query.orders": self.on_orders,
            "risk.query.metrics": self.on_metrics,
        }

    def mine(self, acct: str) -> bool:
        return acct in self.accounts

    async def add_cash(self, cm: CashMovement) -> None:
        self.cash.append(cm)
        await self.publish(topics.risk_cash(cm.account_id), cm, cm.trading_day)

    async def hold_freeze(self) -> None:
        gate = self.freeze_gate
        if gate is not None and len(self.freeze_calls) > self.freeze_gate_skip:
            await gate.wait()

    async def on_freeze(self, cmd: str, req: EodFreezeRequest, ctx: RequestContext) -> Reply:
        self.freeze_calls.append(req)
        await self.hold_freeze()
        return ok_reply(self.freeze_ack(req))

    def freeze_ack(self, req: EodFreezeRequest) -> EodFreezeAck:
        """Digests of ``req.trading_day`` as the engine builds its ``EodFreezeAck``."""
        stale = self.stale_acks is None or len(self.freeze_calls) <= self.stale_acks
        fills = [f for f in self.fills if f.trading_day == req.trading_day and self.mine(f.account_id)]
        if stale:
            fills = [f for f in fills if f.trade_key not in self.missing_keys]
        by_acct: dict[str, list[Trade]] = {}
        for f in fills:
            by_acct.setdefault(f.account_id, []).append(f)
        cash_by_acct: dict[str, list[CashMovement]] = {}
        for c in self.cash:
            if c.trading_day == req.trading_day:
                cash_by_acct.setdefault(c.account_id, []).append(c)
        ack = EodFreezeAck(
            trading_day=req.trading_day,
            shard=self.shard,
            accounts=[compute_account_digest(a, rows) for a, rows in sorted(by_acct.items())],
            # the risk engine's own digest function (futrisk.core.eod), as EngineCore builds its EodFreezeAck
            cash=engine_cash_digests(cash_by_acct, self.accounts),
            open_orders=0,
            frozen_ns=self.rt.market_clock.now_ns(),
        )
        return ack

    async def on_rollover(self, cmd: str, req: EodRolloverRequest, ctx: RequestContext) -> Reply:
        self.rollovers.append(req)
        if self.rollover_gate is not None:
            await self.rollover_gate.wait()
        reply = await self.rt.request(
            CW,
            "settle.sod_snapshot",
            SodRequest(trading_day=req.next_trading_day, account_ids=self.accounts, version=req.sod_version),
            timeout_ms=3_000,
            retries=1,
        )
        if not reply.ok:
            return err_reply(codes.UNAVAILABLE, f"sod_snapshot failed: {reply.code}")
        snap = reply.open_as(SodSnapshot)
        self.snapshots.append(snap)
        mode = DayRollMode.REBASE if req.next_trading_day in self.rolled else DayRollMode.ROLL
        self.rolled[req.next_trading_day] = req.sod_version
        return ok_reply(
            DayRoll(
                shard=self.shard,
                mode=mode,
                from_trading_day=req.trading_day,
                to_trading_day=req.next_trading_day,
                sod_version=req.sod_version,
                accounts=list(self.accounts),
                ts_ns=self.rt.market_clock.now_ns(),
            )
        )

    def on_cash(self, cmd: str, req: QueryCash, ctx: RequestContext) -> Reply:
        items = [c for c in self.cash if c.trading_day == req.trading_day and req.account_id in (None, c.account_id)]
        return ok_reply(CashBatch(items=items))

    def on_alerts(self, cmd: str, req: QueryAlerts, ctx: RequestContext) -> Reply:
        return ok_reply(RiskEventBatch(items=[e for e in self.alerts if e.trading_day == req.trading_day]))

    def on_orders(self, cmd: str, req: QueryOrders, ctx: RequestContext) -> Reply:
        items = [o for o in self.orders if o.account_id == req.account_id and (not req.status or o.status in req.status)]
        return ok_reply(OrderBatch(items=items))

    def on_metrics(self, cmd: str, req: QueryMetrics, ctx: RequestContext) -> Reply:
        points = self.metrics.get(req.account_id, [])
        return ok_reply(MetricSeries(account_id=req.account_id, trading_day=req.trading_day, step_s=req.step_s, points=points))


class StrictShard(StubShard):
    """A shard that enforces the engine's EOD day rules (02 §6.3, §8.4; the catch-up contract).

    * ``eod.freeze{X}``: X = current day -> frozen (full digest); X = an unsettled
      earlier day (the shard went PROVISIONAL across it) -> digest only; else
      ``PRECONDITION_FAILED``;
    * ``eod.rollover{X->Y, v}``: the official SODs must chain (X or Y is the
      newest official day); Y = current day -> ``REBASE`` (also ends PROVISIONAL);
      Y > current day with X = current day -> ``ROLL``; Y < current day (X an
      unsettled day) -> ``CATCH_UP``; the same (Y, v) again -> the same reply.
    """

    def __init__(self, cfg: FullConfig, home: Path, instance: str) -> None:
        super().__init__(cfg, home, instance)
        self.current_day: str | None = None
        self.mode = "NONE"  # OPEN / FROZEN / PROVISIONAL
        self.official: dict[str, int] = {}
        self.chain_head: str | None = None
        self.unsettled: list[str] = []
        self.violations: list[str] = []
        self.modes: list[tuple[str, str, int, str]] = []  # (from, to, version, mode)
        self._replies: dict[tuple[str, int], DayRoll] = {}
        self._roll_lock = asyncio.Lock()  # the engine handles commands one at a time

    def provisional_roll(self, to_day: str) -> None:
        """The engine's own roll at the next session when no EOD came (PROVISIONAL, 02 §8.4)."""
        assert self.current_day is not None and to_day > self.current_day
        self.unsettled.append(self.current_day)
        self.current_day = to_day
        self.mode = "PROVISIONAL"

    def _reject(self, why: str) -> Reply:
        self.violations.append(why)
        return err_reply(codes.PRECONDITION_FAILED, why)

    async def on_freeze(self, cmd: str, req: EodFreezeRequest, ctx: RequestContext) -> Reply:
        self.freeze_calls.append(req)
        await self.hold_freeze()
        x = req.trading_day
        if self.current_day is not None and x == self.current_day:
            self.mode = "FROZEN"
        elif x not in self.unsettled:
            return self._reject(f"cannot freeze {x}: current {self.current_day} {self.mode}")
        return ok_reply(self.freeze_ack(req))

    async def on_rollover(self, cmd: str, req: EodRolloverRequest, ctx: RequestContext) -> Reply:
        self.rollovers.append(req)
        if self.rollover_gate is not None:
            await self.rollover_gate.wait()
        if self.fail_rollovers > 0:
            self.fail_rollovers -= 1
            return err_reply(codes.UNAVAILABLE, "shard busy")
        async with self._roll_lock:
            return await self._rollover(req)

    async def _rollover(self, req: EodRolloverRequest) -> Reply:
        x, y, v = req.trading_day, req.next_trading_day, req.sod_version
        again = self._replies.get((y, v))
        if again is not None:
            return ok_reply(again, duplicate=True)
        head, cur = self.chain_head, self.current_day
        if head is not None and y != head and x != head:
            return self._reject(f"rollover {x}->{y} breaks the SOD chain (newest official {head})")
        if cur is None:
            mode = DayRollMode.ROLL
        elif y == cur:
            mode = DayRollMode.REBASE
        elif y > cur:
            if x != cur:
                return self._reject(f"rollover {x}->{y} while on {cur}")
            mode = DayRollMode.ROLL
        elif x in self.unsettled or y == head:
            mode = DayRollMode.CATCH_UP
        else:
            return self._reject(f"rollover {x}->{y}: {x} is not an unsettled day (current {cur})")
        reply = await self.rt.request(
            CW,
            "settle.sod_snapshot",
            SodRequest(trading_day=y, account_ids=self.accounts, version=v),
            timeout_ms=3_000,
            retries=1,
        )
        if not reply.ok:
            return err_reply(codes.UNAVAILABLE, f"sod_snapshot failed: {reply.code}")
        snap = reply.open_as(SodSnapshot)
        if (snap.trading_day, snap.version) != (y, v):
            return self._reject("SOD snapshot does not match the rollover request")
        self.snapshots.append(snap)
        if x in self.unsettled:
            self.unsettled.remove(x)
        if mode == DayRollMode.ROLL:
            self.current_day = y
            self.mode = "OPEN"
        elif mode == DayRollMode.REBASE and self.mode in ("PROVISIONAL", "NONE"):
            self.mode = "OPEN"
        self.official[y] = v
        self.chain_head = y if head is None or y > head else head
        self.rolled[y] = v
        self.modes.append((x, y, v, mode.value))
        dr = DayRoll(
            shard=self.shard,
            mode=mode,
            from_trading_day=x,
            to_trading_day=y,
            sod_version=v,
            accounts=list(self.accounts),
            ts_ns=self.rt.market_clock.now_ns(),
        )
        self._replies[(y, v)] = dr
        return ok_reply(dr)


class StubTradeGateway(StubBase):
    """trade-gateway: publishes ``td.exec.*`` and answers ``td.digest`` / ``td.query.trades``."""

    def __init__(self, cfg: FullConfig, home: Path) -> None:
        super().__init__(cfg, home, "trade-gateway")
        self.trades: list[Trade] = []
        self.digest_calls = 0
        self.digest_gate: asyncio.Event | None = None
        self.query_calls: list[TradeQuery] = []

    def handlers(self) -> dict[str, Callable[..., Any]]:
        return {"td.digest": self.on_digest, "td.query.trades": self.on_query}

    async def add_trade(self, t: Trade, *, publish: bool = True) -> None:
        self.trades.append(t)
        if publish:
            await self.publish(topics.td_exec(t.account_id), t, t.trading_day)

    async def on_digest(self, cmd: str, req: DigestRequest, ctx: RequestContext) -> Reply:
        self.digest_calls += 1
        if self.digest_gate is not None:
            await self.digest_gate.wait()
        rows = [t for t in self.trades if t.trading_day == req.trading_day]
        by_acct: dict[str, list[Trade]] = {}
        for t in rows:
            by_acct.setdefault(t.account_id, []).append(t)
        return ok_reply(
            TradeDigest(
                trading_day=req.trading_day,
                accounts=[compute_account_digest(a, r) for a, r in sorted(by_acct.items())],
                total_count=len(rows),
                generated_ns=self.rt.market_clock.now_ns(),
            )
        )

    def on_query(self, cmd: str, req: TradeQuery, ctx: RequestContext) -> Reply:
        self.query_calls.append(req)
        rows = sorted(
            (t for t in self.trades if t.trading_day == req.trading_day and req.account_id in (None, t.account_id)),
            key=lambda t: t.trade_seq,
        )
        start = req.from_trade_seq or 0
        rows = [t for t in rows if t.trade_seq >= start]
        page = rows[: req.limit]
        nxt = (page[-1].trade_seq + 1) if page else start
        return ok_reply(TradeBatch(trades=page, next_trade_seq=nxt, complete=len(rows) <= req.limit))


class StubMd(StubBase):
    """md-gateway: ``md.settle_inputs`` (optionally gated / failing) and ``md.events``."""

    def __init__(self, cfg: FullConfig, home: Path) -> None:
        super().__init__(cfg, home, "md-gateway")
        self.inputs: dict[str, SettleInput] = {}
        self.events: list[MdEvent] = []
        self.gate: asyncio.Event | None = None
        self.fail = False
        self.calls = 0

    def handlers(self) -> dict[str, Callable[..., Any]]:
        return {"md.settle_inputs": self.on_inputs, "md.events": self.on_events}

    async def on_inputs(self, cmd: str, req: SettleInputsRequest, ctx: RequestContext) -> Reply:
        self.calls += 1
        if self.gate is not None:
            await self.gate.wait()
        if self.fail:
            return err_reply(codes.UNAVAILABLE, "canonical ticks not ready")
        return ok_reply(SettleInputsBatch(items=[i for i in self.inputs.values() if i.trading_day == req.trading_day]))

    def on_events(self, cmd: str, req: MdEventsQuery, ctx: RequestContext) -> Reply:
        return ok_reply(MdEventBatch(items=[e for e in self.events if e.trading_day == req.trading_day]))


class StubSupervisor(StubBase):
    def __init__(self, cfg: FullConfig, home: Path) -> None:
        super().__init__(cfg, home, "supervisor")
        self.events: list[ServiceStateChanged] = []

    def handlers(self) -> dict[str, Callable[..., Any]]:
        return {"svc.history": self.on_history}

    def on_history(self, cmd: str, req: SvcHistoryRequest, ctx: RequestContext) -> Reply:
        return ok_reply(ServiceEventBatch(items=list(self.events)))


class Api:
    """Caller with the ``api-gateway`` identity (the only one allowed for ``settle.run`` etc.)."""

    def __init__(self, cfg: FullConfig, identity: str = "api-gateway") -> None:
        self.ctx = zmq.asyncio.Context()
        self.client = AsyncRpcClient(self.ctx, Endpoints(cfg.services), identity=identity)

    async def call(self, command: str, payload: Msg, *, timeout_ms: int = 8_000, retries: int = 1) -> Reply:
        return await self.client.request(CW, command, payload, timeout_ms=timeout_ms, retries=retries)

    async def to(self, target: str, command: str, payload: Msg, *, timeout_ms: int = 5_000) -> Reply:
        return await self.client.request(target, command, payload, timeout_ms=timeout_ms, retries=1)

    async def close(self) -> None:
        await self.client.close()
        self.ctx.destroy(linger=0)

    async def settle_run(self, td: str = D, *, force: bool = False, command_id: str | None = None) -> Reply:
        return await self.call(
            "settle.run", SettleRunRequest(command_id=command_id or uuid7(), trading_day=td, force_new_version=force, actor="ops")
        )


# ------------------------------------------------------------------- harness
async def _no_reports(trading_day: str, version: int) -> list[Any]:
    return []


class Harness:
    """All stubs + one clearing-worker on a temp home."""

    def __init__(self, cfg: FullConfig, tmp: Path, *, clock_at: str = "2026-09-28 15:31:00",
                 settings: ClearingSettings = FAST, executor: Executor | None = None,
                 render_reports: bool = True) -> None:
        self.cfg = cfg
        self.tmp = tmp
        self.clock_at = clock_at
        self.settings = settings
        self.executor = executor
        self.render_reports = render_reports  # False: EOD step 7 produces no files (tests that do not look at them)
        self.scn = Scenario(cfg)
        self.tg = StubTradeGateway(cfg, tmp / "tg")
        self.re0 = StrictShard(cfg, tmp / "re0", "risk-engine-0")
        self.re1 = StrictShard(cfg, tmp / "re1", "risk-engine-1")
        self.md = StubMd(cfg, tmp / "md")
        self.sup = StubSupervisor(cfg, tmp / "sup")
        self.api = Api(cfg)
        self.rt: AsyncServiceRuntime | None = None
        self.svc: ClearingService | None = None

    @property
    def shards(self) -> list[StrictShard]:
        return [self.re0, self.re1]

    def shard_of(self, acct: str) -> StrictShard:
        return self.re0 if self.re0.mine(acct) else self.re1

    async def start_stubs(self) -> None:
        await self.tg.start(with_db=True)
        await self.re0.start(with_db=True)
        await self.re1.start(with_db=True)
        await self.md.start()
        await self.sup.start()
        self.md.inputs = self.scn.settle_inputs()

    async def start_clearing(self, clock_at: str | None = None) -> ClearingService:
        rt = AsyncServiceRuntime(
            CW,
            config=self.cfg,
            home=self.tmp / "cw",
            market_clock=market_clock_at(clock_at or self.clock_at),
            setup_logs=False,
        )
        await rt.start()
        svc = ClearingService(rt, self.settings, executor=self.executor)
        await svc.start()
        if not self.render_reports:  # set before the ready task resumes any run
            svc.saga._reports = _no_reports
        self.rt, self.svc = rt, svc
        await asyncio.wait_for(svc.ready.wait(), 10)
        return svc

    async def stop_clearing(self) -> None:
        if self.rt is not None:
            await self.rt.stop()
        self.rt = self.svc = None

    async def seed(self, command_id: str | None = None) -> Reply:
        reply = await self.api.call("settle.seed_sod", self.scn.seed_request(command_id))
        assert reply.ok, reply
        return reply

    async def load_day(self, *, publish: bool = True) -> None:
        """Trades at trade-gateway (published), fills at the shards, the cash movement at its shard."""
        for t in self.scn.trades:
            await self.tg.add_trade(t, publish=publish)
            self.shard_of(t.account_id).fills.append(t)
        for c in self.scn.cash:
            await self.shard_of(c.account_id).add_cash(c)

    async def wait_copies(self) -> None:
        svc = self.svc
        assert svc is not None
        n_t, n_c = len(self.scn.trades), len(self.scn.cash)
        await eventually(lambda: svc.copies.trades_applied >= n_t and svc.copies.cash_applied >= n_c)
        await svc.db.run_write(lambda c: None)

    async def status(self, td: str = D) -> Any:
        from futrisk.messages.eod import EodPhaseEvent, EodStatusRequest

        reply = await self.api.call("settle.status", EodStatusRequest(trading_day=td))
        return reply.open_as(EodPhaseEvent) if reply.ok else None

    async def wait_phase(self, phase: str, td: str = D, timeout: float = 30.0) -> Any:
        async def done() -> bool:
            st = await self.status(td)
            return st is not None and st.phase.value == phase

        await eventually(done, timeout=timeout, step=0.05)
        return await self.status(td)

    async def close(self) -> None:
        await self.stop_clearing()
        for stub in (self.sup, self.md, self.re1, self.re0, self.tg):
            await stub.stop()
        await self.api.close()


# ------------------------------------------------------------ report fixtures
def sample_alerts() -> list[RiskEvent]:
    return [
        RiskEvent(event_id=uuid7(), kind=RiskEventKind.ALERT_RAISED, trading_day=D, ts_ns=ts("2026-09-28 10:32:15"),
                  account_id="ACC3", scope_key="acct:ACC3", alert_key=f"{D}|daily_loss|halt|acct:ACC3|1",
                  rule_id="daily_loss", level="halt", episode=1, action=Action.CIRCUIT_BREAK, severity=Severity.CRITICAL,
                  latched=True, metric="account.daily_pnl_pct_init", value=-0.0302, threshold=-0.03,
                  message="ACC3 当日亏损 -3.02%，已熔断禁开"),
        RiskEvent(event_id=uuid7(), kind=RiskEventKind.ALERT_RAISED, trading_day=D, ts_ns=ts("2026-09-28 10:20:00"),
                  account_id="ACC3", rule_id="daily_loss", level="warn", episode=1, action=Action.ALERT,
                  severity=Severity.WARNING, metric="account.daily_pnl_pct_init", value=-0.021, threshold=-0.02,
                  message="ACC3 当日亏损 -2.1%"),
    ]


def sample_orders(n: int = 7) -> list[OrderUpdate]:
    out = []
    for i in range(n):
        t = ts("2026-09-28 10:33:00") + i * 1_000_000_000
        out.append(OrderUpdate(client_order_id=uuid7(), account_id="ACC3", instrument_id="rb2701", exchange=Exchange.SHFE,
                               direction=Direction.BUY, offset=Offset.OPEN, order_type=OrderType.LIMIT, tif=TimeInForce.GFD,
                               price=3100.0, qty=1, status=OrderStatus.REJECTED, reject_code="REJECT_ACCOUNT_HALTED",
                               source=OrderSource.BOT, trading_day=D, created_ns=t, updated_ns=t))
    return out


def sample_metrics() -> list[MetricPoint]:
    t0 = ts("2026-09-28 09:00:00")
    return [
        MetricPoint(ts_ns=t0 + i * 600_000_000_000, equity=10_000_000 - i * 30_000, nav=1 - i * 0.003,
                    daily_pnl_pct_init=-i * 0.003, intraday_dd=i * 0.003, risk_degree=0.1 + i * 0.01,
                    gross_notional=2_000_000.0, state=AccountState.NORMAL)
        for i in range(12)
    ]


def sample_md_events() -> list[MdEvent]:
    return [
        MdEvent(event_id=uuid7(), kind=MdEventKind.SOURCE_STATE, ts_ns=ts("2026-09-28 10:00:00"), trading_day=D,
                source=SourceId.FEED_A, from_state="TRUSTED", to_state="EXCLUDED"),
        MdEvent(event_id=uuid7(), kind=MdEventKind.SOURCE_STATE, ts_ns=ts("2026-09-28 10:00:20"), trading_day=D,
                source=SourceId.FEED_A, from_state="EXCLUDED", to_state="PROBATION"),
        MdEvent(event_id=uuid7(), kind=MdEventKind.GAP_FILLED, ts_ns=ts("2026-09-28 10:00:25"), trading_day=D,
                source=SourceId.FEED_A, gap=GapInfo(gap_id="feed_a:e:1-40", source=SourceId.FEED_A, from_ns=1, filled=40)),
    ]


def sample_svc_events() -> list[ServiceStateChanged]:
    return [
        ServiceStateChanged(instance="risk-engine-1", service="risk-engine", from_state=ProcessState.RUNNING,
                            to_state=ProcessState.EXITED, restarts=1, reason=ServiceChangeReason.KILLED_BY_DEMO,
                            ts_ns=ts("2026-09-28 13:40:00")),
        ServiceStateChanged(instance="risk-engine-1", service="risk-engine", from_state=ProcessState.EXITED,
                            to_state=ProcessState.RUNNING, restarts=1, reason=ServiceChangeReason.STARTUP,
                            ts_ns=ts("2026-09-28 13:40:03")),
    ]


__all__ = [
    "CW",
    "D",
    "FAST",
    "NEXT",
    "PREV",
    "Api",
    "Harness",
    "Scenario",
    "StrictShard",
    "StubMd",
    "StubShard",
    "StubTradeGateway",
    "eventually",
    "market_clock_at",
    "sample_alerts",
    "sample_md_events",
    "sample_metrics",
    "sample_orders",
    "sample_svc_events",
    "ts",
]
