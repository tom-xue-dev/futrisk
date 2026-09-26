"""Breaker-scenario calibration against the real risk engine (01 §6.3 QS-03; ``config/scenarios/breaker.yaml``).

The pure :class:`~futrisk.core.engine.EngineCore` - the risk-engine's core: the same rules,
restrictions, fees and P&L as the service - runs in-process with the real ACC3 bot
(strategy, executor, gates, order book, position tracker, and its ``risk.state`` /
``risk.snapshot`` handling) in scenario mode, on a sim-style price path:

- flat (with ``noise``: correlated GBM + jumps with the sim's ``vol_annual`` / ``jump_*`` of
  ``sim.yaml``, sector correlation 0.6, 500 ms steps) until ACC3's long rb/hc/i book reaches
  ``min_book_ratio`` x equity - ``futrisk demo --scenario breaker`` waits for 0.7x, polling
  every second - then the scenario starts one second later;
- the legs exactly as sim-exchange applies a LINEAR sector shock: ``log(1 + pct)`` spread
  over ``ramp_s`` in 500 ms steps (compounding), at the scenario's ``at_s`` times;
- observed until ``tail_s`` after the scenario's last step.

:class:`ScenarioSpec` reads legs, times and ramp from ``config/scenarios/breaker.yaml``.

Each run records when the daily-loss levels were raised (warn / restrict / halt / derisk,
seconds after the scenario start), the first alerts, the worst daily P&L, the order outcomes
(adds rejected ``REJECT_ADD_TO_LOSER`` and their sizes under ``ORDER_SIZE_FACTOR``, halt probes
rejected ``REJECT_ACCOUNT_HALTED``, the halt trim) and when the bot learned the halt.

``python -m futrisk.sim.bots.calibration --seeds 20 [--legs -0.024,-0.006,-0.0075]`` prints
the table; ``tests/unit/bots/test_bots_calibration.py`` asserts the acceptance targets.
:class:`EngineHarness` is also used by the steady-state rule-noise tests.
"""

from __future__ import annotations

import argparse
import itertools
import math
import random
import sys
from collections import Counter
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any

from futrisk.client.orders import OrderBook
from futrisk.client.positions import PositionTracker
from futrisk.client.pricing import ContractTable
from futrisk.client.riskview import RiskUpdate, RiskView
from futrisk.common.clock import BJ, SimClock, bj_to_ns
from futrisk.common.config import FullConfig, load_default_config
from futrisk.common.ids import SeqIdGen, uuid7
from futrisk.core.effects import CancelRoute, CommandCtx, Effect, Persist, Publish, ReplyTo, Request
from futrisk.core.engine import EngineCore
from futrisk.core.instruments import InstrumentRegistry
from futrisk.core.risk_account import Lifecycle
from futrisk.messages import codes, topics
from futrisk.messages.base import Msg
from futrisk.messages.enums import (
    BrokerOrderStatus,
    Direction,
    Offset,
    OrderSource,
    OrderType,
    RiskEventKind,
    SourceId,
    TimeInForce,
)
from futrisk.messages.envelope import Reply, err_reply, ok_reply
from futrisk.messages.eod import SodAccount, SodSnapshot
from futrisk.messages.md import Tick
from futrisk.messages.orders import (
    BrokerOrderUpdate,
    CancelRequest,
    OrderAck,
    OrderRequest,
    OrderUpdate,
    RouteAck,
    Trade,
)
from futrisk.messages.risk import AccountSnapshot, AccountStateChanged, RiskEvent
from futrisk.sim.bots.bot import AccountBot
from futrisk.sim.bots.config import BotsConfig, load_bots_config
from futrisk.sim.bots.executor import CancelAction, Executor, SubmitAction
from futrisk.sim.bots.gates import Gates
from futrisk.sim.bots.strategies import StrategyContext, make_strategy

S = 1_000_000_000
STEP_NS = S // 2  # the sim's 500 ms market state
TRADING_DAY = "20260929"  # a Tuesday; 09:30 is in the continuous day session of rb/hc/i
START = "2026-09-29 09:30"
BLACK = ("rb2701", "hc2701", "i2701")
LEVELS = ("warn", "restrict", "halt", "derisk")
DAILY_LOSS = "daily_loss"
TRADING_SECONDS_PER_DAY = 4 * 3600  # sim-exchange engine constants
TRADING_DAYS_PER_YEAR = 252


def bj(text: str) -> int:
    """Beijing ``YYYY-MM-DD HH:MM[:SS]`` -> UTC ns."""
    return bj_to_ns(datetime.fromisoformat(text).replace(tzinfo=BJ))


class EngineHarness:
    """One risk-engine core in-process: ticks, commands, routing and fills; no sockets, no database.

    Every effect list goes through :meth:`pump`: outbox FACTs (``OrderUpdate``, ``RiskEvent``,
    ``AccountStateChanged``) and LV publishes (``risk.snapshot``) are handed to ``on_message``;
    route cancels are acknowledged by a CANCELED broker report, sub-requests answered UNAVAILABLE.
    """

    def __init__(
        self,
        cfg: FullConfig,
        capital: dict[str, float],
        *,
        pre_settle: dict[str, float],
        shard: int = 1,
        start: str = START,
        trading_day: str = TRADING_DAY,
    ) -> None:
        self.cfg = cfg
        self.clock = SimClock(bj(start))
        self.registry = InstrumentRegistry(cfg.products, cfg.calendar)
        self.eng = EngineCore(shard=shard, registry=self.registry, rules=cfg.risk_rules, accounts=cfg.accounts,
                              clock=self.clock, ids=SeqIdGen("cal"), calendar=cfg.calendar, coordinator=shard == 0,
                              config_hash="calibration")
        self.trading_day = trading_day
        self.pre = dict(pre_settle)
        self.on_message: Callable[[str, Msg], None] = lambda topic, payload: None
        self._volume: Counter[str] = Counter()
        self._bseq = itertools.count(1)
        self._trade = itertools.count(1)
        sod = SodSnapshot(
            trading_day=trading_day,
            version=1,
            accounts=[SodAccount(account_id=a, pre_balance=f"{c:.2f}", initial_capital=f"{c:.2f}",
                                 cum_net_deposit="0.00", hwm_equity=f"{c:.2f}", lots=[])
                      for a, c in sorted(capital.items())],
            pre_settle={k: str(v) for k, v in self.pre.items()},
        )
        self.pump(self.eng.load_sod(sod))
        self.pump(self.eng.set_lifecycle(Lifecycle.RUNNING))

    @property
    def now_ns(self) -> int:
        return self.clock.now_ns()

    def pump(self, effects: Iterable[Effect]) -> None:
        effects = list(effects)
        for e in effects:
            if isinstance(e, Persist):
                for m in e.outbox:
                    self.on_message(m.topic, m.payload)
            elif isinstance(e, Publish):
                self.on_message(e.topic, e.payload)
        for e in effects:
            if isinstance(e, CancelRoute):
                o = self.eng.orders.get(e.client_order_id)
                filled = o.filled_qty if o is not None else 0
                self.pump(self.broker(e.client_order_id, BrokerOrderStatus.CANCELED, filled=filled))
            elif isinstance(e, Request):
                self.pump(self.eng.on_request_result(e.token, err_reply(codes.UNAVAILABLE, "calibration: no peer")))

    # ------------------------------------------------------------ market data
    def limits(self, instrument_id: str) -> tuple[float, float]:
        up, lo = self.registry.limit_prices(instrument_id, Decimal(str(self.pre[instrument_id])), self.trading_day)
        return float(up), float(lo)

    def tick(self, instrument_id: str, price: float) -> Tick:
        """Apply one LIVE market state (like md-gateway would publish it); returns the tick."""
        self._volume[instrument_id] += 1
        info = self.registry.get(instrument_id)
        up, lo = self.limits(instrument_id)
        step = float(info.tick_size)
        t = Tick(instrument_id=instrument_id, exchange=info.exchange, trading_day=self.trading_day,
                 action_day=self.trading_day, exch_ts_ns=self.now_ns, last=price, volume=self._volume[instrument_id],
                 turnover=0.0, open_interest=0, bid1=price - step, ask1=price + step, upper_limit=up, lower_limit=lo,
                 pre_settle=self.pre[instrument_id], source=SourceId.FEED_A, recv_ns=self.now_ns)
        self.eng.apply_mark(t)
        return t

    def evaluate(self) -> None:
        self.pump(self.eng.evaluate_dirty())

    def timer(self) -> None:
        self.pump(self.eng.on_timer(self.now_ns))

    # ------------------------------------------------------------ commands and execution
    def command(self, name: str, payload: Msg, token: str) -> tuple[Reply, list[Effect]]:
        effects = self.eng.handle(payload, CommandCtx(name, token=token, requester="strategy-sim", received_ns=self.now_ns))
        replies = [e.reply for e in effects if isinstance(e, ReplyTo)]
        return (replies[-1] if replies else err_reply(codes.INTERNAL, "no reply")), effects

    def route_ok(self, coid: str) -> None:
        ack = RouteAck(client_order_id=coid, status=BrokerOrderStatus.RECEIVED, broker_order_ref=f"ref-{coid}")
        self.pump(self.eng.on_route_ack(coid, ok_reply(ack)))

    def broker(self, coid: str, status: BrokerOrderStatus, *, filled: int = 0, remaining: int = 0) -> list[Effect]:
        o = self.eng.orders.get(coid)
        if o is None:
            return []
        u = BrokerOrderUpdate(client_order_id=coid, account_id=o.account_id, instrument_id=o.instrument_id,
                              broker_order_ref=f"ref-{coid}", status=status, filled_qty=filled, remaining_qty=remaining,
                              broker_seq=next(self._bseq), trading_day=self.trading_day, updated_ns=self.now_ns)
        return self.eng.apply_exec(u)

    def fill(self, coid: str, price: float, qty: int) -> None:
        o = self.eng.orders.get(coid)
        if o is None:
            return
        n = next(self._trade)
        t = Trade(trading_day=self.trading_day, exchange=self.registry.get(o.instrument_id).exchange, trade_id=f"T{n}",
                  direction=o.direction, account_id=o.account_id, instrument_id=o.instrument_id, client_order_id=coid,
                  broker_order_ref=f"ref-{coid}", offset=o.offset, price=price, qty=qty, trade_ts_ns=self.now_ns,
                  broker_seq=next(self._bseq), trade_seq=n, received_ns=self.now_ns)
        self.pump(self.eng.apply_exec(t))

    def execute(self, req: OrderRequest, price: float) -> tuple[Reply, OrderAck | None, list[Effect]]:
        """Submit; an accepted order is routed, accepted by the broker and filled completely at ``price``."""
        reply, effects = self.command("order.submit", req, token=req.client_order_id)
        ack = reply.open_as(OrderAck) if reply.data_type == "OrderAck" else None
        return reply, ack, effects

    def complete(self, req: OrderRequest, price: float) -> None:
        self.route_ok(req.client_order_id)
        self.pump(self.broker(req.client_order_id, BrokerOrderStatus.ACCEPTED, remaining=req.qty))
        self.fill(req.client_order_id, price, req.qty)

    def open_position(self, account_id: str, instrument_id: str, direction: Direction, qty: int, price: float) -> Reply:
        """Open ``qty`` lots at ``price`` (submit + route + fill); for the steady-state books."""
        req = OrderRequest(client_order_id=uuid7(), account_id=account_id, instrument_id=instrument_id,
                           direction=direction, offset=Offset.OPEN, order_type=OrderType.LIMIT, price=price, qty=qty,
                           sent_ns=self.now_ns)
        reply, ack, effects = self.execute(req, price)
        self.pump(effects)
        if reply.ok and ack is not None and ack.accepted:
            self.complete(req, price)
        return reply


# ---------------------------------------------------------------------------- the breaker run
@dataclass(frozen=True)
class ScenarioSpec:
    """Legs (price moves), their start times and ramp, and the time of the scenario's last step."""

    legs: tuple[float, ...]
    at_s: tuple[float, ...]
    ramp_s: float
    end_s: float

    def with_legs(self, legs: Sequence[float]) -> ScenarioSpec:
        return ScenarioSpec(tuple(legs), self.at_s, self.ramp_s, self.end_s)


def _num(value: Any, env: dict[str, Any]) -> float:
    if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
        value = env[value[2:-1]]
    return float(value)


def scenario_spec(cfg: FullConfig | None = None, name: str = "breaker") -> ScenarioSpec:
    """Legs, times and ramp of ``config/scenarios/{name}.yaml`` (its ``vars`` substituted)."""
    import yaml

    cfg = cfg or load_default_config()
    with open(cfg.scenarios_dir / f"{name}.yaml", encoding="utf-8") as f:
        doc = yaml.safe_load(f)
    env = dict(doc.get("vars") or {})
    legs: list[float] = []
    at: list[float] = []
    ramp = 0.0
    for step in doc["steps"]:
        if step.get("action") == "shock":
            prm = step["params"]
            legs.append(_num(prm["pct"], env))
            at.append(float(step["at_s"]))
            ramp = _num(prm.get("duration_s", 0), env)
    end = max(float(step["at_s"]) for step in doc["steps"])
    return ScenarioSpec(tuple(legs), tuple(at), ramp, end)


@dataclass
class BreakerRun:
    """Outcome of one calibration run (times in seconds after the scenario start)."""

    seed: int | None
    spec: ScenarioSpec
    book_ratio: float = 0.0
    start_s: float | None = None
    levels: dict[str, float | None] = field(default_factory=dict)
    alerts: list[tuple[float, str]] = field(default_factory=list)
    worst_pnl: float = 0.0
    worst_t: float = 0.0
    final_pnl: float = 0.0
    learned_halt: float | None = None
    orders: Counter[str] = field(default_factory=Counter)
    add_sizes_restricted: list[int] = field(default_factory=list)
    add_reasons_restricted: list[str] = field(default_factory=list)
    trace: list[tuple[float, float, float]] = field(default_factory=list)  # (t, daily P&L, sector move)

    def level(self, name: str) -> float | None:
        return self.levels.get(name)

    def one_level_per_leg(self, *, hold_s: float = 30.0) -> bool:
        """warn in leg 1, restrict in leg 2, halt in leg 3 (by the scenario's last step), >= ``hold_s`` apart."""
        w, r, h = (self.levels.get(k) for k in ("warn", "restrict", "halt"))
        at = self.spec.at_s
        if w is None or r is None or h is None or len(at) < 3:
            return False
        return (at[0] <= w < at[1] <= r < at[2] <= h <= self.spec.end_s
                and r - w >= hold_s and h - r >= hold_s)

    def row(self) -> str:
        def f(x: float | None) -> str:
            return "   -  " if x is None else f"{x:6.1f}"

        first = self.alerts[0][1] if self.alerts else "-"
        return (f"seed {str(self.seed):>4} book {self.book_ratio:4.2f}x  warn {f(self.level('warn'))} "
                f"restrict {f(self.level('restrict'))} halt {f(self.level('halt'))}  worst {self.worst_pnl:+.4%} "
                f"@{self.worst_t:5.0f}s  halt learned {f(self.learned_halt)}  add-to-loser "
                f"{self.orders.get('open:' + codes.REJECT_ADD_TO_LOSER, 0):2d}  halted probes "
                f"{self.orders.get('halt_probe:' + codes.REJECT_ACCOUNT_HALTED, 0):2d}  trims "
                f"{self.orders.get('close:ok', 0)}  first alert {first}")


def _instrument_params(cfg: FullConfig, instrument_id: str) -> tuple[float, float, float, float]:
    """(pre_settle, vol_annual, jump_intensity_per_day, jump_sigma) of ``sim.yaml``."""
    sim = cfg.sim.price_engine.instruments[instrument_id]
    return float(sim.pre_settle), sim.vol_annual, sim.jump_intensity_per_day, sim.jump_sigma


def run_breaker(
    spec: ScenarioSpec,
    *,
    seed: int | None = 1,
    noise: bool = True,
    jumps: bool = True,
    account: str = "ACC3",
    min_book_ratio: float = 0.7,
    tail_s: float = 420.0,
    cfg: FullConfig | None = None,
    bots_cfg: BotsConfig | None = None,
    capital: float = 10_000_000.0,
    sector_correlation: float = 0.6,
) -> BreakerRun:
    """One breaker run (see the module docstring); ``seed=None`` or ``noise=False`` = no noise."""
    cfg = cfg or load_default_config()
    bots_cfg = bots_cfg or load_bots_config(env={}, aggressive=True, accounts=[account])
    pre = {i: _instrument_params(cfg, i)[0] for i in BLACK}
    h = EngineHarness(cfg, {account: capital}, pre_settle=pre)
    contracts = ContractTable(cfg.products)
    positions = PositionTracker(contracts, TRADING_DAY)
    book = OrderBook()
    rng = random.Random(f"{bots_cfg.seed}:{account}")
    gates = Gates(account, bots_cfg.gates)
    strategy = make_strategy("counter_trend_add", bots_cfg.params_for(account), account_id=account, rng=rng)
    ctx = StrategyContext(account, contracts, positions, equity=capital, gates=gates, rng=rng)

    def new_order(account_id: str, instrument_id: str, direction: Any, offset: Any, qty: int, price: float | None,
                  *, source: OrderSource = OrderSource.BOT, strategy_tag: str | None = None, **_: Any) -> OrderRequest:
        return OrderRequest(client_order_id=uuid7(), account_id=account_id, instrument_id=instrument_id,
                            direction=Direction(direction), offset=Offset(offset), order_type=OrderType.LIMIT,
                            tif=TimeInForce.GFD, price=price, qty=qty, source=source, strategy_tag=strategy_tag,
                            sent_ns=h.now_ns)

    ex = Executor(account, contracts=contracts, positions=positions, orders=book, gates=gates,
                  cfg=bots_cfg.execution_for(account), new_order=new_order, rng=rng, strategy_tag="calibration")
    bot = AccountBot(account, strategy=strategy, executor=ex, ctx=ctx)
    bot.trading = True
    view = RiskView(account)
    run = BreakerRun(seed=seed, spec=spec, levels=dict.fromkeys(LEVELS))
    t0 = h.now_ns
    scen_ns: int | None = None
    halt_seen = False

    def rel(ns: int) -> float:
        return (ns - (scen_ns if scen_ns is not None else t0)) / S

    def on_message(topic: str, payload: Msg) -> None:
        nonlocal halt_seen
        if isinstance(payload, OrderUpdate):
            o, fill = book.apply_update(payload)
            if fill is not None:
                positions.apply_fill(payload.account_id, payload.instrument_id, payload.direction, payload.offset,
                                     fill.qty, fill.price, trading_day=payload.trading_day or None)
            bot.executor.on_order_event(o, payload, fill.qty if fill else 0, ctx.now_ns)
        elif isinstance(payload, RiskEvent):
            if payload.kind == RiskEventKind.ALERT_RAISED and payload.rule_id and scen_ns is not None:
                run.alerts.append((rel(h.now_ns), f"{payload.rule_id}/{payload.level}"))
                if payload.rule_id == DAILY_LOSS and payload.level in run.levels and run.levels[payload.level] is None:
                    run.levels[payload.level] = rel(h.now_ns)
        elif isinstance(payload, AccountStateChanged) and topic == topics.risk_state(account):
            changed = view.apply_state(payload)
            bot.on_risk_update(RiskUpdate(account, view, "state", changed, None, payload.ts_ns))
        elif isinstance(payload, AccountSnapshot) and topic == topics.risk_snapshot(account):
            taken, changed = view.apply_snapshot(payload)
            if taken:
                bot.on_risk_update(RiskUpdate(account, view, "snapshot", changed, payload, payload.as_of_ns))
        if gates.halted and not halt_seen and scen_ns is not None:
            halt_seen = True
            run.learned_halt = rel(h.now_ns)

    h.on_message = on_message
    px = dict(pre)
    x = {i: math.log(p) for i, p in pre.items()}
    tick_size = {i: float(contracts.get(i).tick_size) for i in BLACK}
    year_s = TRADING_DAYS_PER_YEAR * TRADING_SECONDS_PER_DAY
    sigma, p_jump, jump_sigma = {}, {}, {}
    for i in BLACK:
        _, vol, lam, js = _instrument_params(cfg, i)
        sigma[i] = vol * math.sqrt(0.5 / year_s)
        p_jump[i] = lam * 0.5 / TRADING_SECONDS_PER_DAY
        jump_sigma[i] = js
    nrng = random.Random(seed) if noise and seed is not None else None
    shocks: list[list[float]] = []  # [per_step, steps_left]
    legs_started: set[int] = set()
    end_ns: int | None = None
    step = 0

    def fill_price(req: OrderRequest) -> float:
        p, t = px[req.instrument_id], tick_size[req.instrument_id]
        limit = req.price if req.price is not None else p
        return min(limit, p + t) if req.direction == Direction.BUY else max(limit, p - t)

    def execute(act: SubmitAction | CancelAction) -> None:
        if isinstance(act, SubmitAction):
            req = act.request
            book.add_request(req)
            reply, ack, effects = h.execute(req, fill_price(req))
            book.on_submit_reply(req.client_order_id, reply, ack)
            ex.on_submit_result(req, reply, ack, ctx.now_ns)
            accepted = reply.ok and ack is not None and ack.accepted
            code = "ok" if accepted else (ack.reject_code if ack is not None and ack.reject_code else reply.code)
            run.orders[f"{act.kind}:{code}"] += 1
            if act.kind == "open" and run.levels.get("restrict") is not None:
                run.add_sizes_restricted.append(req.qty)
                run.add_reasons_restricted.append(act.why)
            h.pump(effects)
            if accepted:
                h.complete(req, fill_price(req))
        else:
            creq = CancelRequest(command_id=uuid7(), client_order_id=act.client_order_id, account_id=act.account_id,
                                 reason=act.why)
            reply, effects = h.command("order.cancel", creq, token=creq.command_id)
            upd = reply.open_as(OrderUpdate) if reply.data_type == "OrderUpdate" else None
            ex.on_cancel_result(act, reply, upd, ctx.now_ns)
            h.pump(effects)

    while True:
        step += 1
        h.clock.set(t0 + step * STEP_NS)
        t_s = (h.now_ns - t0) / S
        if scen_ns is not None:
            for k, at in enumerate(spec.at_s):
                if k not in legs_started and h.now_ns >= scen_ns + int(at * S):
                    legs_started.add(k)
                    n = max(1, round(spec.ramp_s / 0.5))
                    shocks.append([math.log1p(spec.legs[k]) / n, float(n)])
        common = nrng.gauss(0.0, 1.0) if nrng is not None else 0.0
        for i in BLACK:
            dx = sum(sh[0] for sh in shocks if sh[1] > 0)
            if nrng is not None:
                z = math.sqrt(sector_correlation) * common + math.sqrt(1 - sector_correlation) * nrng.gauss(0.0, 1.0)
                dx += -0.5 * sigma[i] ** 2 + sigma[i] * z
                if jumps and nrng.random() < p_jump[i]:
                    dx += jump_sigma[i] * nrng.gauss(0.0, 1.0)
            x[i] += dx
            up, lo = h.limits(i)
            p = round(math.exp(x[i]) / tick_size[i]) * tick_size[i]
            px[i] = min(max(p, lo), up)
        for sh in shocks:
            sh[1] -= 1
        ticks = {i: h.tick(i, px[i]) for i in BLACK}
        h.evaluate()
        h.timer()
        for i in BLACK:
            for act in bot.on_tick(ticks[i]):
                execute(act)
        h.evaluate()
        if step % 2 == 0:  # 1 s: the demo polls the book, samples the P&L
            snap = h.eng.snapshot(account)
            pnl = snap.metrics.daily_pnl_pct_init
            if scen_ns is None:
                longs = sum(p.notional for p in snap.positions if p.instrument_id in BLACK and p.direction == "LONG")
                ratio = longs / snap.funds.equity if snap.funds.equity > 0 else 0.0
                if ratio >= min_book_ratio:
                    scen_ns = h.now_ns + S
                    run.book_ratio = ratio
                    run.start_s = t_s + 1.0
                    end_ns = scen_ns + int((spec.end_s + tail_s) * S)
                elif t_s > 900:
                    break  # the book never reached the ratio
            else:
                if pnl < run.worst_pnl:
                    run.worst_pnl, run.worst_t = pnl, rel(h.now_ns)
                run.final_pnl = pnl
                move = sum(math.log(px[i] / pre[i]) for i in BLACK) / len(BLACK)
                run.trace.append((rel(h.now_ns), pnl, move))
        if end_ns is not None and h.now_ns >= end_ns:
            break
    return run


def run_many(spec: ScenarioSpec, seeds: Iterable[int | None], **kw: Any) -> list[BreakerRun]:
    cfg = kw.pop("cfg", None) or load_default_config()
    return [run_breaker(spec, seed=s, cfg=cfg, **kw) for s in seeds]


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m futrisk.sim.bots.calibration",
                                 description="breaker scenario vs the real risk engine and the ACC3 bot")
    ap.add_argument("--legs", default=None, help="comma-separated leg moves, e.g. -0.024,-0.006,-0.0075 "
                                                 "(default: config/scenarios/breaker.yaml)")
    ap.add_argument("--at", default=None, help="comma-separated leg start times in s (default: the scenario's)")
    ap.add_argument("--ramp", type=float, default=None, help="ramp seconds (default: the scenario's)")
    ap.add_argument("--end", type=float, default=None, help="time of the scenario's last step (default: the scenario's)")
    ap.add_argument("--seeds", type=int, default=20, help="noisy runs (seeds 1..N)")
    ap.add_argument("--no-noise", action="store_true", help="one run on the deterministic path only")
    ap.add_argument("--no-jumps", action="store_true")
    ap.add_argument("--book", type=float, default=0.7, help="book ratio at which the scenario starts")
    ap.add_argument("--hold", type=float, default=30.0, help="minimum seconds between levels")
    args = ap.parse_args(argv)
    from loguru import logger

    logger.remove()
    cfg = load_default_config()
    spec = scenario_spec(cfg)
    if args.legs:
        spec = spec.with_legs([float(v) for v in args.legs.split(",")])
    if args.at or args.ramp is not None or args.end is not None:
        at = tuple(float(v) for v in args.at.split(",")) if args.at else spec.at_s
        spec = ScenarioSpec(spec.legs, at, args.ramp if args.ramp is not None else spec.ramp_s,
                            args.end if args.end is not None else spec.end_s)
    print(f"legs {spec.legs} at {spec.at_s} s, ramps {spec.ramp_s:g} s, last step {spec.end_s:g} s; "
          f"book >= {args.book}x")
    base = run_breaker(spec, seed=None, noise=False, cfg=cfg, min_book_ratio=args.book)
    print("no noise ", base.row())
    if args.no_noise:
        return 0
    runs = run_many(spec, range(1, args.seeds + 1), cfg=cfg, jumps=not args.no_jumps, min_book_ratio=args.book)
    for r in runs:
        print(("  ok " if r.one_level_per_leg(hold_s=args.hold) else "MISS ") + r.row())
    ok = sum(r.one_level_per_leg(hold_s=args.hold) for r in runs)
    worst = min(r.worst_pnl for r in runs)
    print(f"one level per leg in {ok}/{len(runs)} seeds; worst daily P&L {worst:+.4%}")
    return 0


if __name__ == "__main__":
    sys.exit(main())


__all__ = ["BreakerRun", "EngineHarness", "ScenarioSpec", "run_breaker", "run_many", "scenario_spec"]
