"""The day loop of one experiment arm (01 §8.16 逐日循环): the same EngineCore and clearing library as production.

For every trading day:

1. **SOD**: the engine rolls to the day's start-of-day snapshot (``roll_day``):
   state machines reset, latches released (``reset_on_day_roll``), positions
   carried as yesterday lots at the settlement price;
2. **replay**: the day's market states in time order become ``Tick``s
   (``apply_mark``); the engine evaluates the dirty accounts after every state
   (``evaluate_dirty``); confirm / hold / cooldown timers run at their due time
   (``on_timer``); the strategies see the engine's view (positions, account
   state) and send orders through the same pre-trade path
   (``handle(order.submit)``). Every effect goes over the :class:`InprocBus`:
   ``Route`` / ``CancelRoute`` become requests to the in-process broker
   (:mod:`futrisk.backtest.broker`), whose ``td.exec.{account}`` FACTs come back
   through ``apply_exec``; the outbox (``risk.event``, ``risk.state.*``, ...) is
   published as FACTs and recorded;
3. **settlement prices**: official prices (layer A) or the 01 §8.15 methods on
   the replayed tape (layer D);
4. **clearing**: ``core.freeze`` (EOD digest, checked against the broker's
   trades), then :func:`futrisk.core.settle_day` gives the day's settlement and
   the next SOD.

Account equity, daily P&L and the risk events come from the engine and the
clearing library; the backtest never recomputes them.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from futrisk.backtest.arms import DAILY_LOSS, HALT_LEVEL, arm_rules
from futrisk.backtest.broker import InprocBroker
from futrisk.backtest.config import ArmSpec, BacktestConfig
from futrisk.backtest.executor import Executor, pos_info
from futrisk.backtest.paths import Point, TickGrid
from futrisk.backtest.sources import DataSource, DayPlan
from futrisk.backtest.strategies import Quote, Strategy, StrategyContext
from futrisk.bus.inproc import InprocBus
from futrisk.common.clock import BJ, SimClock, ns_to_bj
from futrisk.common.config.accounts import AccountsConfig, AccountSpec
from futrisk.common.config.risk_rules import RiskRulesConfig
from futrisk.common.decimals import D
from futrisk.common.ids import SeqIdGen
from futrisk.core.effects import (
    CancelRoute,
    CommandCtx,
    Effect,
    Persist,
    Publish,
    ReplyTo,
    Request,
    ResolveRoute,
    Route,
)
from futrisk.core.engine import EngineCore
from futrisk.core.risk_account import Lifecycle
from futrisk.core.settlement import SettlementError, seed_sod, settle_day
from futrisk.messages import codes
from futrisk.messages.enums import AccountState, Direction, Offset, PosDirection, RiskEventKind, SourceId
from futrisk.messages.envelope import Envelope, Reply, err_reply, open_payload
from futrisk.messages.eod import SettlementPriceView, SodSnapshot
from futrisk.messages.md import FLAG_LIMIT_DOWN_LOCKED, FLAG_LIMIT_UP_LOCKED, Tick
from futrisk.messages.orders import (
    BrokerOrderUpdate,
    OrderRequest,
    RouteCancel,
    RouteResolve,
    Trade,
    compute_account_digest,
)
from futrisk.messages.risk import AccountStateChanged, RiskEvent

NS_PER_S = 1_000_000_000
NS_PER_DAY = 86_400 * NS_PER_S
BJ_OFFSET_NS = 8 * 3600 * NS_PER_S
ENGINE = "risk-engine-bt"
GATEWAY = "trade-gateway"
LEVELS = ("warn", "restrict", "halt", "derisk")


@dataclass
class ArmStats:
    days: int = 0
    states: int = 0
    ticks: int = 0
    orders: int = 0
    rejects: int = 0
    fills: int = 0
    timer_runs: int = 0
    unexpected_requests: int = 0
    digest_mismatches: int = 0
    settle_failed_checks: int = 0
    settle_errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        d = dict(self.__dict__)
        d["settle_errors"] = list(self.settle_errors)[:20]
        return d


@dataclass
class ArmResult:
    """Everything one arm produces (plain Python data: picklable across processes)."""

    arm: str
    tier: str
    threshold: float
    label: str
    layer: str
    accounts: list[str]
    capital: dict[str, float]
    days: list[str] = field(default_factory=list)
    daily: list[dict[str, Any]] = field(default_factory=list)
    events: list[dict[str, Any]] = field(default_factory=list)
    states: list[dict[str, Any]] = field(default_factory=list)
    orders: list[dict[str, Any]] = field(default_factory=list)
    scripted: list[dict[str, Any]] = field(default_factory=list)
    stats: dict[str, Any] = field(default_factory=dict)
    notes: list[str] = field(default_factory=list)


def action_day(ts_ns: int, cache: dict[int, str]) -> str:
    """Beijing natural date of ``ts_ns`` as YYYYMMDD (cached per day number)."""
    k = (ts_ns + BJ_OFFSET_NS) // NS_PER_DAY
    s = cache.get(k)
    if s is None:
        s = ns_to_bj(ts_ns).strftime("%Y%m%d")
        cache[k] = s
    return s


def iso_bj(ts_ns: int) -> str:
    return ns_to_bj(ts_ns).astimezone(BJ).isoformat(timespec="seconds")


class Replay:
    """One arm: EngineCore + InprocBus + in-process broker + strategies + settle_day, day after day."""

    def __init__(
        self,
        *,
        cfg: BacktestConfig,
        arm: ArmSpec,
        source: DataSource,
        base_rules: RiskRulesConfig,
        base_accounts: AccountsConfig,
        strategies: Mapping[str, Strategy],
        label: str,
        config_hash: str = "",
        progress: Callable[[float, str | None], None] | None = None,
        scenario_account: str | None = None,
        record_orders: bool = True,
    ) -> None:
        self.cfg = cfg
        self.arm = arm
        self.source = source
        self.registry = source.registry
        self.label = label
        self.progress = progress
        self.rules: RiskRulesConfig = arm_rules(base_rules, cfg, arm)
        self.threshold = cfg.threshold_of(arm)
        self.record_orders = record_orders
        self.strategies = dict(strategies)
        accts = sorted(self.strategies) if self.strategies else ([scenario_account] if scenario_account else [])
        self.accounts = accts
        self.capital: dict[str, Decimal] = {}
        specs: dict[str, AccountSpec] = {}
        for a in accts:
            spec = base_accounts.accounts.get(a)
            run = cfg.accounts.get(a)
            cap = run.capital if run is not None and run.capital is not None else (spec.initial_capital if spec else None)
            if cap is None:
                raise ValueError(f"account {a}: no initial capital (accounts.yaml or the backtest config)")
            self.capital[a] = D(cap)
            base_spec = spec or AccountSpec(name=a, shard=0, initial_capital=D(cap))
            specs[a] = base_spec.model_copy(update={"shard": 0, "initial_capital": D(cap), "initial_positions": []})
        self.accounts_cfg = AccountsConfig(accounts=specs)
        self.clock = SimClock(0)
        self.bus = InprocBus("backtest", clock=self.clock)
        self.core = EngineCore(
            shard=0,
            registry=self.registry,
            rules=self.rules,
            accounts=self.accounts_cfg,
            clock=self.clock,
            ids=SeqIdGen(label),
            calendar=source.calendar,
            instance=ENGINE,
            coordinator=False,
            config_hash=config_hash,
            auto_provisional=False,
        )
        self.engine_ep = self.bus.endpoint(ENGINE)
        self.broker = InprocBroker(self.bus.endpoint(GATEWAY), self.registry, self.clock)
        self.bus.endpoint("backtest-shell").subscribe(GATEWAY, ["td.exec."], self._on_exec)
        self.bus.endpoint("backtest-recorder").subscribe(ENGINE, ["risk.event", "risk.state."], self._on_fact)
        self.executors = {a: Executor(a, self.registry, self._submit, id_prefix=f"{label}-{a}") for a in accts}
        self.result = ArmResult(
            arm=arm.name, tier=arm.tier, threshold=self.threshold, label=label, layer=source.layer, accounts=list(accts),
            capital={a: float(c) for a, c in self.capital.items()},
        )
        self.stats = ArmStats()
        self._replies: dict[Any, Reply] = {}
        self._ad_cache: dict[int, str] = {}
        self._grids: dict[str, TickGrid] = {}
        # per-day state
        self.td = ""
        self.last_px: dict[str, float] = {}
        self.day_open: dict[str, float] = {}
        self.day_hi: dict[str, float] = {}
        self.day_lo: dict[str, float] = {}
        self.replayed: dict[str, list[Point]] = {}
        self.live_now: set[str] = set()
        self.cur_label: str | None = None
        self.day_flags: dict[str, dict[str, Any]] = {}
        self.sod_equity: dict[str, float] = {}

    # ============================================================ effects
    def _run(self, effects: list[Effect]) -> None:
        for e in effects:
            t = type(e)
            if t is Persist:
                assert isinstance(e, Persist)
                for m in e.outbox:
                    self.engine_ep.publish(m.topic, m.payload, fact=True, corr_id=m.corr_id, td=m.td)
            elif t is Route:
                assert isinstance(e, Route)
                reply = self.engine_ep.request(GATEWAY, "route.order", e.order)
                self._run(self.core.on_route_ack(e.order.client_order_id, reply))
                if reply.ok:
                    self.broker.match(e.order.instrument_id)
            elif t is CancelRoute:
                assert isinstance(e, CancelRoute)
                reply = self.engine_ep.request(
                    GATEWAY, "route.cancel", RouteCancel(command_id=e.command_id, client_order_id=e.client_order_id, reason=e.reason)
                )
                self._run(self.core.on_cancel_result(e.client_order_id, reply))
            elif t is ResolveRoute:
                assert isinstance(e, ResolveRoute)
                reply = self.engine_ep.request(GATEWAY, "route.resolve",
                                               RouteResolve(client_order_id=e.client_order_id, trading_day=e.trading_day))
                self._run(self.core.on_resolve_result(e.client_order_id, reply))
            elif t is Request:
                assert isinstance(e, Request)
                self.stats.unexpected_requests += 1
                self._run(self.core.on_request_result(e.token, err_reply(codes.UNAVAILABLE, "not available in a backtest")))
            elif t is ReplyTo:
                assert isinstance(e, ReplyTo)
                self._replies[e.token] = e.reply
            elif t is Publish:
                continue

    def _on_exec(self, env: Envelope) -> None:
        msg = open_payload(env)
        if isinstance(msg, Trade | BrokerOrderUpdate):
            if isinstance(msg, Trade):
                self.stats.fills += 1
            self._run(self.core.apply_exec(msg))

    def _on_fact(self, env: Envelope) -> None:
        msg = open_payload(env)
        if isinstance(msg, RiskEvent):
            self._record_event(msg)
        elif isinstance(msg, AccountStateChanged):
            self._record_state(msg)

    # ============================================================ recording
    def _record_event(self, ev: RiskEvent) -> None:
        acct = ev.account_id
        self.result.events.append({
            "arm": self.arm.name,
            "tier": self.arm.tier,
            "threshold": self.threshold,
            "trading_day": ev.trading_day,
            "ts_ns": ev.ts_ns,
            "ts": iso_bj(ev.ts_ns),
            "account_id": acct,
            "kind": ev.kind.value,
            "rule_id": ev.rule_id,
            "level": ev.level,
            "action": ev.action.value if ev.action is not None else None,
            "severity": ev.severity.value,
            "latched": ev.latched,
            "value": ev.value,
            "threshold_value": ev.threshold,
            "alert_key": ev.alert_key,
            "episode": ev.episode,
            "step": self.cur_label,
            "message": ev.message,
        })
        if acct is not None and ev.kind == RiskEventKind.ALERT_RAISED and ev.rule_id == DAILY_LOSS:
            f = self.day_flags.setdefault(acct, {})
            f.setdefault(f"trig_{ev.level}", ev.ts_ns)
            f.setdefault(f"trig_{ev.level}_value", ev.value)
            if ev.level == HALT_LEVEL and ev.latched:
                f.setdefault("latch_ts", ev.ts_ns)

    def _record_state(self, st: AccountStateChanged) -> None:
        self.result.states.append({
            "arm": self.arm.name,
            "trading_day": st.trading_day,
            "ts_ns": st.ts_ns,
            "ts": iso_bj(st.ts_ns),
            "account_id": st.account_id,
            "from_state": st.from_state.value,
            "to_state": st.to_state.value,
            "halted_latched": st.halted_latched,
            "cause": st.cause,
            "cause_alert_key": st.cause_alert_key,
            "restrictions": ",".join(sorted({r.kind.value for r in st.restrictions})),
            "step": self.cur_label,
        })
        f = self.day_flags.setdefault(st.account_id, {})
        if st.to_state == AccountState.HALTED:
            f["halted"] = True
            if st.halted_latched:
                f["halt_latched"] = True
        elif st.from_state == AccountState.HALTED and not st.halted_latched and not f.get("halt_latched"):
            f["halt_revoked"] = f.get("halt_revoked", 0) + 1

    # ============================================================ orders
    def _submit(self, req: OrderRequest, purpose: str) -> Reply:
        now = self.clock.now_ns()
        req = OrderRequest(**{**{f: getattr(req, f) for f in req.__struct_fields__}, "sent_ns": now})
        ctx = CommandCtx("order.submit", token=req.client_order_id, requester="strategy-bt", received_ns=now)
        self._run(self.core.handle(req, ctx))
        reply = self._replies.pop(req.client_order_id, None)
        if reply is None:
            reply = err_reply(codes.INTERNAL, "no reply")
        self.stats.orders += 1
        f = self.day_flags.setdefault(req.account_id, {})
        f["orders"] = f.get("orders", 0) + 1
        if not reply.ok:
            self.stats.rejects += 1
            f["rejects"] = f.get("rejects", 0) + 1
            if reply.code == codes.REJECT_ACCOUNT_HALTED:
                f["blocked_opens"] = f.get("blocked_opens", 0) + 1
            elif reply.code == codes.REJECT_ADD_TO_LOSER:
                f["blocked_adds"] = f.get("blocked_adds", 0) + 1
        if self.record_orders:
            self.result.orders.append({
                "arm": self.arm.name,
                "trading_day": self.td,
                "ts_ns": now,
                "account_id": req.account_id,
                "client_order_id": req.client_order_id,
                "instrument_id": req.instrument_id,
                "direction": req.direction.value,
                "offset": req.offset.value,
                "qty": req.qty,
                "price": req.price,
                "purpose": purpose,
                "ok": reply.ok,
                "code": reply.code,
                "step": self.cur_label,
            })
        return reply

    # ============================================================ market states
    def _grid(self, inst: str) -> TickGrid:
        g = self._grids.get(inst)
        if g is None:
            g = TickGrid(self.registry.tick_size(inst))
            self._grids[inst] = g
        return g

    def _tick(self, p: Point, plan: DayPlan) -> Tick:
        inst = p.instrument_id
        bar = plan.bars.get(inst)
        lim = plan.limits.get(inst)
        upper, lower = lim if lim is not None else (p.price * 1.2, p.price * 0.8)
        ps = bar.pre_settle if bar is not None and bar.pre_settle else p.price
        g = self._grid(inst)
        bid = p.bid1 if p.bid1 is not None else max(lower, round(p.price - g.tick, g.digits))
        ask = p.ask1 if p.ask1 is not None else min(upper, round(p.price + g.tick, g.digits))
        info = self.registry.get(inst)
        return Tick(
            instrument_id=inst,
            exchange=info.exchange,
            trading_day=plan.trading_day,
            action_day=action_day(p.ts_ns, self._ad_cache),
            exch_ts_ns=p.ts_ns,
            last=p.price,
            volume=p.volume,
            turnover=p.turnover,
            open_interest=0,
            bid1=bid,
            ask1=ask,
            bid_vol1=1,
            ask_vol1=1,
            open=self.day_open.get(inst, p.price),
            high=max(self.day_hi.get(inst, p.price), p.price),
            low=min(self.day_lo.get(inst, p.price), p.price),
            upper_limit=upper,
            lower_limit=lower,
            pre_settle=ps,
            source=SourceId.SINA,
            recv_ns=p.ts_ns,
            flags=p.flags,
        )

    def _timers_until(self, ts: int) -> None:
        """Run the engine's timers at their due times up to ``ts`` (``EngineCore.next_timer_ns``: with the periodic
        timers off these are the rule machines' confirm / hold / cooldown timers and the order timers)."""
        last_due: int | None = None
        while True:
            due = self.core.next_timer_ns()
            if due is None or due > ts:
                return
            if last_due is not None and due <= last_due and self.clock.now_ns() >= due:
                return  # the due machine cannot move now (e.g. quality-gated): leave it to the next state
            last_due = due
            now = max(due, self.clock.now_ns())
            self.clock.set(now)
            self.stats.timer_runs += 1
            self._run(self.core.on_timer(now))

    def _state(self, ts: int, group: list[Point], plan: DayPlan) -> None:
        self._timers_until(ts)
        if ts > self.clock.now_ns():
            self.clock.set(ts)
        live: set[str] = set()
        for p in group:
            inst = p.instrument_id
            self.core.apply_mark(self._tick(p, plan))
            self.broker.set_market(inst, p.price, ts, locked_up=bool(p.flags & FLAG_LIMIT_UP_LOCKED),
                                   locked_down=bool(p.flags & FLAG_LIMIT_DOWN_LOCKED))
            self.last_px[inst] = p.price
            self.day_open.setdefault(inst, p.price)
            self.day_hi[inst] = max(self.day_hi.get(inst, p.price), p.price)
            self.day_lo[inst] = min(self.day_lo.get(inst, p.price), p.price)
            self.replayed.setdefault(inst, []).append(p)
            live.add(inst)
        self.live_now = live
        self.cur_label = plan.labels.get(ts)
        self.stats.ticks += len(group)
        self.stats.states += 1
        self._run(self.core.evaluate_dirty())
        if self.broker.working():
            for inst in sorted(live):
                if self.broker.working(inst):
                    self.broker.match(inst)
            self._run(self.core.evaluate_dirty())
        for order in plan.scripted.get(ts, []):
            self._scripted(order)
        if self.strategies:
            for acct in self.accounts:
                self._strategy_step(acct, ts, live, plan)
            self._run(self.core.evaluate_dirty())

    def _strategy_step(self, acct: str, ts: int, live: set[str], plan: DayPlan) -> None:
        strat = self.strategies[acct]
        mains = {pr: plan.mains[pr] for pr in strat.products if pr in plan.mains}
        if not set(mains.values()) & live:
            return
        ra = self.core.account(acct)
        net: dict[str, int] = {}
        for inst, d, q in ra.ledger.held():
            net[inst] = net.get(inst, 0) + (q if d == PosDirection.LONG else -q)
        quotes = {
            pr: Quote(pr, inst, self.last_px.get(inst) if inst in self.day_open else None, self.day_open.get(inst),
                      inst in live, inst in live and len(self.replayed.get(inst, ())) == 1)
            for pr, inst in mains.items()
        }
        ctx = StrategyContext(
            account_id=acct,
            trading_day=plan.trading_day,
            now_ns=ts,
            equity=float(self.capital[acct]),
            registry=self.registry,
            quotes=quotes,
            positions=net,
            state=ra.state,
        )
        targets = strat.targets(ctx)
        if not targets:
            return
        working = {o.instrument_id for o in self.core.orders.resting(acct)}
        book = ra.ledger.book
        self.executors[acct].rebalance(targets, net, lambda i: pos_info(book, acct, i), self.last_px, live, working,
                                       ra.state)

    def _scripted(self, order: Mapping[str, Any]) -> None:
        acct = str(order.get("account") or self.accounts[0])
        inst = str(order["instrument"])
        direction = Direction(str(order.get("side", "BUY")).upper())
        offset = Offset(str(order.get("offset", "OPEN")).upper())
        price = float(order.get("price") or self.last_px[inst])
        qty = int(order["qty"])
        before = len(self.result.orders)
        accepted, code = self.executors[acct].send(inst, direction, offset, qty, price, purpose=f"scripted:{order.get('label', '')}")
        statuses = []
        for o in self.result.orders[before:]:
            rec = self.core.orders.get(o["client_order_id"])
            statuses.append(rec.status.value if rec is not None else "NONE")
        self.result.scripted.append({
            "label": order.get("label", ""), "step": order.get("step"), "account_id": acct, "instrument_id": inst,
            "direction": direction.value, "offset": offset.value, "qty": qty, "price": price, "accepted": accepted,
            "code": code, "statuses": statuses, "expect": order.get("expect"),
        })

    # ============================================================ day loop
    def run(self) -> ArmResult:
        days = self.source.days()
        if not days:
            self.result.notes.append("no trading days to replay")
            self.result.stats = self.stats.to_dict()
            return self.result
        self._warmup(days[0])
        td0 = days[0]
        sod = seed_sod(td0, {a: self.capital[a] for a in self.accounts}, version=1,
                       pre_settle={k: D(v) for k, v in self.source.initial_pre_settle(td0).items()})
        first_plan = self._plan(td0, sod)
        t0 = first_plan.points[0].ts_ns if first_plan.points else 0
        self.clock.set(max(0, t0 - 3600 * NS_PER_S))
        self._run(self.core.load_sod(sod))
        self._run(self.core.set_lifecycle(Lifecycle.RUNNING))
        n = len(days)
        report_every = max(1, n // 100)
        for i, td in enumerate(days):
            plan = first_plan if i == 0 else self._plan(td, sod)
            if i > 0:
                roll_ns = self._roll_time(plan)
                if roll_ns > self.clock.now_ns():
                    self.clock.set(roll_ns)
                self._run(self.core.roll_day(sod))
            sod = self._day(td, plan, sod, days[i + 1] if i + 1 < n else None)
            self.stats.days += 1
            if self.progress is not None and (i % report_every == 0 or i == n - 1):
                self.progress((i + 1) / n, f"{self.arm.name} {td}")
        self.result.days = list(days)
        self.result.stats = self.stats.to_dict()
        self.result.stats["broker"] = dict(self.broker.stats.__dict__)
        self.result.stats["executor"] = {a: {**ex.stats.__dict__, "rejects": dict(ex.stats.rejects)} for a, ex in self.executors.items()}
        self.result.notes.extend(self.source.notes)
        return self.result

    def _roll_time(self, plan: DayPlan) -> int:
        """Just after the previous day's settlement, before the new day's first state."""
        now = self.clock.now_ns()
        first = plan.points[0].ts_ns if plan.points else now + NS_PER_S
        return max(now + NS_PER_S, min(now + 1800 * NS_PER_S, first - 60 * NS_PER_S))

    def _warmup(self, first_td: str) -> None:
        n = self.cfg.warmup_days
        if n <= 0:
            return
        for strat in self.strategies.values():
            for pr in strat.products:
                for bar in self.source.warmup_bars(pr, first_td, n):
                    if bar.roll_factor != 1.0:
                        strat.on_roll(pr, bar.roll_factor)
                    strat.on_bar(pr, bar)

    def _plan(self, td: str, sod: SodSnapshot) -> DayPlan:
        held = {lot.instrument_id for a in sod.accounts for lot in a.lots}
        pre = {k: float(v) for k, v in sod.pre_settle.items()}
        return self.source.plan(td, pre, held)

    def _day(self, td: str, plan: DayPlan, sod: SodSnapshot, next_td: str | None) -> SodSnapshot:
        self.td = td
        self.last_px = {}
        self.day_open = {}
        self.day_hi = {}
        self.day_lo = {}
        self.replayed = {}
        self.live_now = set()
        self.day_flags = {}
        for strat in self.strategies.values():
            for pr in strat.products:
                f = plan.roll_factors.get(pr, 1.0)
                if f != 1.0:
                    strat.on_roll(pr, f)
            strat.begin_day(td)
        self.broker.begin_day(td)
        # engine equity right after the roll, before any state (continuity with yesterday's settlement)
        self.sod_equity = {a: self.core.account(a).ledger.funds().equity for a in self.accounts}
        pts = plan.points
        i = 0
        while i < len(pts):
            ts = pts[i].ts_ns
            j = i + 1
            while j < len(pts) and pts[j].ts_ns == ts:
                j += 1
            self._state(ts, pts[i:j], plan)
            i = j
        last_ts = pts[-1].ts_ns if pts else self.clock.now_ns()
        self._timers_until(last_ts + 5 * NS_PER_S)
        self.live_now = set()
        self.broker.end_day()
        self._run(self.core.evaluate_dirty())
        # ---------------------------------------------------------------- EOD
        self.clock.set(max(self.clock.now_ns(), last_ts) + 60 * NS_PER_S)
        ack = self.core.freeze(td)
        digests = {d.account_id: d for d in ack.accounts}
        for a in self.accounts:
            mine = compute_account_digest(a, self.broker.trades_of(a))
            theirs = digests.get(a)
            if theirs is None or theirs.sha256 != mine.sha256 or theirs.count != mine.count:
                self.stats.digest_mismatches += 1
        views = self.source.settle_views(plan, self.replayed)
        prices: dict[str, SettlementPriceView | Decimal] = dict(views)
        for inst, px in plan.next_pre_settle.items():
            prices.setdefault(inst, D(px))
        extra_pre = {inst: D(b.pre_settle) for inst, b in plan.bars.items() if b.pre_settle is not None and inst not in sod.pre_settle}
        nxt = next_td or self.source.next_trading_day(td)
        try:
            ds = settle_day(
                trading_day=td, version=1, sod=sod, trades=list(self.broker.trades), cash_movements=[],
                settle_prices=prices, registry=self.registry, next_trading_day=nxt, pre_settle=extra_pre,
            )
        except SettlementError as exc:
            self.stats.settle_errors.append(f"{td}: {exc}")
            raise
        failed = ds.failed_checks()
        if failed:
            self.stats.settle_failed_checks += len(failed)
            if len(self.stats.settle_errors) < 20:
                self.stats.settle_errors.append(f"{td}: {[c.check.value for c in failed]}")
        self._daily_rows(td, plan, sod, ds)
        # ---------------------------------------------------------- strategies
        for strat in self.strategies.values():
            for pr in strat.products:
                bar = plan.daily_bars.get(pr)
                if bar is not None:
                    strat.on_bar(pr, bar)
        self.engine_ep.fact_log.clear()
        self.bus.endpoint(GATEWAY).fact_log.clear()
        return ds.next_sod

    # ============================================================ daily rows
    def _daily_rows(self, td: str, plan: DayPlan, sod: SodSnapshot, ds: Any) -> None:
        sod_net: dict[str, dict[str, int]] = {}
        for sa in sod.accounts:
            d = sod_net.setdefault(sa.account_id, {})
            for lot in sa.lots:
                p = self.registry.product(lot.instrument_id)
                d[p] = d.get(p, 0) + (lot.qty if lot.direction == PosDirection.LONG else -lot.qty)
        for a in self.accounts:
            res = ds.accounts[a]
            cap = float(self.capital[a])
            flags = self.day_flags.get(a, {})
            strat = self.strategies.get(a)
            touched = {lot.instrument_id for sa in sod.accounts if sa.account_id == a for lot in sa.lots}
            touched |= {t.instrument_id for t in self.broker.trades_of(a)}
            products = set(strat.products if strat else ()) | {self.registry.product(i) for i in touched}
            row: dict[str, Any] = {
                "arm": self.arm.name,
                "tier": self.arm.tier,
                "threshold": self.threshold,
                "trading_day": td,
                "account_id": a,
                "capital": cap,
                "pre_balance": float(res.pre_balance),
                "equity": float(res.equity),
                "day_pnl": float(res.day_pnl),
                "close_pnl": float(res.close_pnl),
                "position_pnl": float(res.position_pnl),
                "fee": float(res.fee),
                "margin": float(res.margin),
                "risk_degree": float(res.risk_degree),
                "nav": float(res.nav),
                "ret": float(res.day_pnl) / cap,
                "engine_equity_sod": self.sod_equity.get(a),
                "sod_net": json.dumps(dict(sorted(sod_net.get(a, {}).items())), sort_keys=True),
                "halted": bool(flags.get("halted")),
                "halt_latched": bool(flags.get("halt_latched")),
                "halt_revoked": int(flags.get("halt_revoked", 0)),
                "latch_ts": flags.get("latch_ts"),
                "orders": int(flags.get("orders", 0)),
                "rejects": int(flags.get("rejects", 0)),
                "blocked_opens": int(flags.get("blocked_opens", 0)),
                "blocked_adds": int(flags.get("blocked_adds", 0)),
                "trades": len(self.broker.trades_of(a)),
                "roll_day": any(p in plan.rolls for p in products),  # a main-contract roll of a product it trades
                "est_settle": bool(touched & plan.estimated),  # settled on an estimated price (DCE, flat exit)
            }
            for lv in LEVELS:
                row[f"trig_{lv}"] = f"trig_{lv}" in flags
                row[f"trig_{lv}_value"] = flags.get(f"trig_{lv}_value")
            self.result.daily.append(row)


__all__ = ["ArmResult", "ArmStats", "Replay", "action_day", "iso_bj"]
