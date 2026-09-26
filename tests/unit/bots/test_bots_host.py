"""strategy-sim process level: readiness, start-up sync, ACC3 breaker flow, determinism, stop paths.

End-to-end against the fakes of :mod:`futrisk.client.testing` over loopback sockets: the bots
wait for ticks and answering shards, cancel only their own leftovers and sync the account,
build ACC3's long black book, learn a HALT from ``risk.state`` without an opening attempt, stop
opening (closes still pass: the halt trim; 1-lot probes show the halt), re-seed an idle
account from drifting snapshots, and stop on request or when the parent process is gone.
"""

from __future__ import annotations

import asyncio
import os
import random
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

import psutil
from botstest_support import T0, S, expected_book
from loguru import logger

from futrisk.client.client import FutriskClient
from futrisk.client.pricing import ContractTable
from futrisk.client.testing import FakeBus, account_snapshot, make_tick
from futrisk.common.config import FullConfig
from futrisk.messages import codes
from futrisk.messages.enums import (
    AccountState,
    Direction,
    Exchange,
    Offset,
    OrderSource,
    OrderStatus,
    OrderType,
    PosDirection,
    TimeInForce,
)
from futrisk.messages.envelope import ok_reply
from futrisk.messages.md import Tick
from futrisk.messages.orders import OrderAck, OrderRequest, OrderUpdate
from futrisk.messages.risk import PositionView
from futrisk.sim.bots.config import BotsConfig, load_bots_config
from futrisk.sim.bots.executor import SubmitAction
from futrisk.sim.bots.main import BotHost, run, run_bots, watch_parent
from futrisk.sim.bots.strategies import CounterTrendParams

PX = {"rb2701": 3120.0, "hc2701": 3260.0, "i2701": 780.0}
TICK = {"rb2701": 1.0, "hc2701": 1.0, "i2701": 0.5}


def bots_config(**top: Any) -> BotsConfig:
    base = load_bots_config(env={}, aggressive=True, accounts=["ACC3"]).model_dump()
    base.update({"status_log_s": 0, "resync_interval_s": 0, "ready_timeout_s": 5})
    base["accounts"]["ACC3"]["params"]["halt_trim_delay_s"] = 2.0
    base.update(top)
    return BotsConfig.model_validate(base)


def dead_pid() -> int:
    pid = 4_000_000
    while psutil.pid_exists(pid):
        pid += 7
    return pid


async def until(pred: Callable[[], bool], timeout: float = 5.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not pred():
        if loop.time() > deadline:
            raise AssertionError("condition not reached")
        await asyncio.sleep(0.01)


class Market:
    """Publishes one tick per instrument per round, advancing market time."""

    def __init__(self, bus: FakeBus) -> None:
        self.bus = bus
        self.t = 0.0

    async def round(self, prices: dict[str, float], dt: float = 1.0) -> None:
        self.t += dt
        for k, (i, p) in enumerate(prices.items()):
            px = round(p / TICK[i]) * TICK[i]
            await self.bus.md.tick(make_tick(i, px, ts_ns=T0 + int((self.t + 0.01 * k) * S)))

    async def pump(self, pred: Callable[[], bool], prices: dict[str, float], *, dt: float = 1.0,
                   timeout: float = 10.0) -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while not pred():
            if loop.time() > deadline:
                raise AssertionError("market pump: condition not reached")
            await self.round(prices, dt)
            await asyncio.sleep(0.02)


async def test_acc3_breaker_flow_end_to_end(bus_cfg: FullConfig) -> None:
    async with FakeBus(bus_cfg, fill_mode="full") as bus:
        stop = asyncio.Event()
        hosts: list[BotHost] = []
        task = asyncio.create_task(run_bots(bus_cfg, bots_config(), stop=stop, install_signals=False,
                                            host_ready=hosts.append))
        shard = bus.shard("risk-engine-1")
        for i in PX:
            await bus.md.wait_subscribed(f"md.tick.{i}")
        for topic in ("oms.order.ACC3", "risk.state.ACC3", "risk.snapshot.ACC3"):
            await shard.pub.wait_subscribed(topic)
        host = hosts[0]
        cli = host.client
        # no market data yet: nothing is sent but pings and the start-up Clone of ACC3's risk topics
        await asyncio.sleep(0.4)
        assert shard.submits() == [] and not host.ready
        assert {c for c, _, _ in shard.requests} <= {"ctl.ping", "ctl.snapshot"}
        mkt = Market(bus)
        # the scenario mode builds the whole ~0.78x-equity book at once
        target = expected_book(ContractTable(bus_cfg.products), CounterTrendParams().weights, PX)
        await mkt.pump(lambda: all(cli.positions.net("ACC3", i) >= 0.95 * n for i, n in target.items()), PX)
        assert host.ready
        cmds = [c for c, _, _ in shard.requests if c not in ("ctl.ping", "ctl.snapshot")]
        # start-up: the own leftovers (query + one cancel each, none here), then the account snapshot
        assert cmds[:2] == ["risk.query.orders", "risk.query.accounts"] and "order.cancel_all" not in cmds
        subs = shard.submits()
        assert shard.requesters() == {"strategy-sim"}
        assert {(r.account_id, r.direction, r.offset) for r in subs} == {("ACC3", Direction.BUY, Offset.OPEN)}
        assert all(cli.owns_coid(r.client_order_id) for r in subs)  # the own order-id marker
        assert bus.shard("risk-engine-0").submits() == []  # ACC3 belongs to shard 1
        assert all(r.qty <= 100 for r in subs) and len(subs) >= 4  # sliced
        # the engine HALTS the account: the bot learns it from risk.state, without any opening attempt
        shard.reject_opens = codes.REJECT_ACCOUNT_HALTED
        n_before = len(shard.submits())
        bot = host.bots["ACC3"]
        await shard.publish_state("ACC3", AccountState.HALTED, latched=True, from_state=AccountState.RESTRICTED)
        await until(lambda: bot.gates.halted)
        assert bot.executor.stats.get(f"reject:{codes.REJECT_ACCOUNT_HALTED}", 0) == 0
        held = {i: cli.positions.net("ACC3", i) for i in PX}
        trimmed = {i: int(n * 0.9) for i, n in held.items()}  # floor(90%): the halt trim, once
        await mkt.pump(lambda: all(cli.positions.net("ACC3", i) == trimmed[i] for i in PX), PX)
        after = shard.submits()[n_before:]
        closes = [r for r in after if r.offset != Offset.OPEN]
        assert {(r.instrument_id, r.offset, r.direction) for r in closes} == {
            ("rb2701", Offset.CLOSE_TODAY, Direction.SELL),  # SHFE, opened today: 平今
            ("hc2701", Offset.CLOSE_TODAY, Direction.SELL),
            ("i2701", Offset.CLOSE, Direction.SELL),  # DCE
        }
        probes = [r for r in after if r.offset == Offset.OPEN]
        assert probes and all(r.qty == 1 for r in probes)  # the halt shows at once: 1-lot opening probes
        assert bot.executor.stats[f"reject:{codes.REJECT_ACCOUNT_HALTED}"] == len(probes)
        # ... one per ~20 s of market time, not a storm; the trimmed book is never bought back
        n_probes = len(probes)
        await mkt.pump(lambda: bot.executor.stats["halt_probe"] >= n_probes + 2, PX, dt=5.0)
        opens = [r for r in shard.submits()[n_before:] if r.offset == Offset.OPEN]
        assert all(r.qty == 1 for r in opens) and len(opens) <= n_probes + 3
        assert all(cli.positions.net("ACC3", i) == trimmed[i] for i in PX)
        stop.set()
        assert await asyncio.wait_for(task, 10) == 0


async def test_startup_and_shutdown_cancel_only_own_orders(bus_cfg: FullConfig) -> None:
    """Never cancel manual or liquidation orders (a risk manager's close must survive a bot restart)."""
    cfg = bots_config()
    async with FakeBus(bus_cfg, fill_mode="none") as bus:
        shard = bus.shard("risk-engine-1")
        predecessor = FutriskClient(bus_cfg, coid_tag=cfg.order_tag).new_client_order_id()
        for coid, source, offset, direction in (
            (predecessor, OrderSource.BOT, Offset.OPEN, Direction.BUY),  # left by the previous incarnation
            ("manual-close", OrderSource.MANUAL, Offset.CLOSE_TODAY, Direction.SELL),
            ("liquidation-1", OrderSource.LIQUIDATION, Offset.CLOSE_TODAY, Direction.SELL),
            ("api-1", OrderSource.API, Offset.OPEN, Direction.BUY),
        ):
            req = OrderRequest(client_order_id=coid, account_id="ACC3", instrument_id="rb2701", direction=direction,
                               offset=offset, price=3000.0, qty=2, source=source)
            shard.orders[coid] = shard.update_for(req, OrderStatus.ACCEPTED)
        stop = asyncio.Event()
        hosts: list[BotHost] = []
        task = asyncio.create_task(run_bots(bus_cfg, cfg, stop=stop, install_signals=False, host_ready=hosts.append))
        for i in PX:
            await bus.md.wait_subscribed(f"md.tick.{i}")
        await shard.pub.wait_subscribed("oms.order.ACC3")
        mkt = Market(bus)
        await mkt.pump(lambda: hosts[0].ready and len(shard.submits()) >= 3, PX)
        cancels = [pl.client_order_id for c, pl, _ in shard.requests if c == "order.cancel"]
        assert cancels[:1] == [predecessor]
        own_now = [r.client_order_id for r in shard.submits()]
        stop.set()
        assert await asyncio.wait_for(task, 10) == 0
        cancels = [pl.client_order_id for c, pl, _ in shard.requests if c == "order.cancel"]
        assert set(cancels) <= {predecessor, *own_now}  # the shutdown cancels its own working orders only
        assert not any(c == "order.cancel_all" for c, _, _ in shard.requests)
        for coid in ("manual-close", "liquidation-1", "api-1"):
            assert shard.orders[coid].status == OrderStatus.ACCEPTED


async def test_idle_account_reseeded_from_drifting_snapshots(bus_cfg: FullConfig) -> None:
    cfg = bots_config()
    async with FakeBus(bus_cfg, fill_mode="full") as bus:
        shard = bus.shard("risk-engine-1")
        stop = asyncio.Event()
        hosts: list[BotHost] = []
        task = asyncio.create_task(run_bots(bus_cfg, cfg, stop=stop, install_signals=False, host_ready=hosts.append))
        for i in PX:
            await bus.md.wait_subscribed(f"md.tick.{i}")
        await shard.pub.wait_subscribed("risk.snapshot.ACC3")
        mkt = Market(bus)
        target = expected_book(ContractTable(bus_cfg.products), CounterTrendParams().weights, PX)
        host = hosts[0]
        cli = host.client
        await mkt.pump(lambda: all(cli.positions.net("ACC3", i) >= 0.95 * n for i, n in target.items()), PX)
        await until(lambda: host.bots["ACC3"].executor.idle)
        # the engine's book differs (e.g. a broker force close we never saw): rb 20 lots fewer
        engine = {i: cli.positions.net("ACC3", i) - (20 if i == "rb2701" else 0) for i in PX}
        views = [PositionView(account_id="ACC3", instrument_id=i, product=i.rstrip("0123456789"), exchange=Exchange.SHFE,
                              direction=PosDirection.LONG, td_qty=n, yd_qty=0, closable_qty=n, closable_td_qty=n,
                              closable_yd_qty=0, avg_open_price=PX[i], basis_avg_price=PX[i]) for i, n in engine.items()]
        for _ in range(2):  # drift_confirm = 2 consecutive snapshots
            await shard.publish_snapshot(account_snapshot("ACC3", as_of_ns=shard.now_ns(), positions=views))
        await until(lambda: cli.positions.net("ACC3", "rb2701") == engine["rb2701"])
        stop.set()
        assert await asyncio.wait_for(task, 10) == 0


async def test_stops_when_parent_is_gone_before_ready(bus_cfg: FullConfig) -> None:
    async with FakeBus(bus_cfg):
        code = await asyncio.wait_for(
            run_bots(bus_cfg, bots_config(), parent_pid=dead_pid(), parent_poll_s=0.05, install_signals=False), 10)
        assert code == 0


async def test_watch_parent() -> None:
    stop = asyncio.Event()
    alive = {"v": True}
    t = asyncio.create_task(watch_parent(123, stop, 0.01, alive=lambda: alive["v"]))
    await asyncio.sleep(0.05)
    assert not stop.is_set()
    alive["v"] = False
    await asyncio.wait_for(t, 2)
    assert stop.is_set()
    # the real check (ParentWatch): this very process is alive, a pid that never existed is not
    stop2 = asyncio.Event()
    t2 = asyncio.create_task(watch_parent(os.getpid(), stop2, 0.01))
    await asyncio.sleep(0.05)
    assert not stop2.is_set()
    t2.cancel()
    stop3 = asyncio.Event()
    await asyncio.wait_for(watch_parent(dead_pid(), stop3, 0.01), 2)
    assert stop3.is_set()


def test_run_entry_point_smoke(tmp_path: Path) -> None:
    """``futrisk svc strategy-sim`` contract: run(home=..., parent_pid=...) -> int, exits when the parent dies."""
    try:
        code = run(home=str(tmp_path / "home"), parent_pid=dead_pid(), parent_poll_s=0.05, console_log=False,
                   accounts="ACC5")
    finally:
        logger.remove()
        logger.add(sys.stderr, level="INFO")
    assert code == 0
    assert (tmp_path / "home" / "config" / "active.json").exists()
    assert any((tmp_path / "home" / "logs").glob("strategy-sim_*.log"))


# ---------------------------------------------------------------- determinism (no sockets)
def _walk(seed: int, n: int = 500) -> list[Tick]:
    rng = random.Random(seed)
    px = {"rb2701": 3120.0, "rb2705": 3150.0, "hc2701": 3260.0, "i2701": 780.0}
    out: list[Tick] = []
    for k in range(n):
        common = rng.gauss(0, 0.0006)
        for j, i in enumerate(px):
            step = TICK.get(i, 1.0)
            px[i] = max(step, px[i] * (1 + common + rng.gauss(0, 0.0003)))
            p = round(px[i] / step) * step
            out.append(make_tick(i, p, ts_ns=T0 + k * S // 2 + j * 1000))
    return out


def _simulate(cfg: FullConfig, bots_cfg: BotsConfig, ticks: list[Tick]) -> list[tuple[Any, ...]]:
    host = BotHost(cfg, bots_cfg)
    for b in host.bots.values():
        b.trading = True
    cli = host.client
    decisions: list[tuple[Any, ...]] = []
    for t in ticks:
        for bot in host.by_instrument.get(t.instrument_id, ()):
            for act in bot.on_tick(t):
                assert isinstance(act, SubmitAction)
                r = act.request
                decisions.append((r.account_id, r.instrument_id, str(r.direction), str(r.offset), r.qty, r.price))
                ack = OrderAck(client_order_id=r.client_order_id, accepted=True, status=OrderStatus.ROUTING,
                               trading_day="20260928")
                cli.orders.add_request(r)
                cli.orders.on_submit_reply(r.client_order_id, ok_reply(ack), ack)
                bot.executor.on_submit_result(r, ok_reply(ack), ack, t.exch_ts_ns)
                upd = OrderUpdate(client_order_id=r.client_order_id, account_id=r.account_id,
                                  instrument_id=r.instrument_id, exchange=Exchange.SHFE, direction=r.direction,
                                  offset=r.offset, order_type=OrderType.LIMIT, tif=TimeInForce.GFD, price=r.price,
                                  qty=r.qty, status=OrderStatus.FILLED, filled_qty=r.qty, avg_fill_price=r.price,
                                  source=r.source, trading_day="20260928", created_ns=1, updated_ns=t.exch_ts_ns)
                o, f = cli.orders.apply_update(upd)
                assert f is not None
                cli.positions.apply_fill(r.account_id, r.instrument_id, r.direction, r.offset, f.qty, f.price)
                bot.executor.on_order_event(o, upd, f.qty, t.exch_ts_ns)
    return decisions


def test_decisions_are_deterministic_given_the_tick_stream(default_config: FullConfig) -> None:
    ticks = _walk(5)
    cfg_a = load_bots_config(env={}, accounts=["ACC3", "ACC5"])
    first = _simulate(default_config, cfg_a, ticks)
    second = _simulate(default_config, cfg_a, ticks)
    assert first == second and len(first) >= 8
    assert {d[0] for d in first} == {"ACC3", "ACC5"}
    acc5 = [d for d in first if d[0] == "ACC5"]
    assert {d[1] for d in acc5} == {"rb2701", "rb2705"}  # the two-sided spread book
    other = _simulate(default_config, load_bots_config(env={}, accounts=["ACC3", "ACC5"], seed=99), ticks)
    assert other != first  # the seed matters (passive/aggressive choice, crossing depth)
