"""``pretrade``: the in-process pre-trade check of the EngineCore (target p50 ≤ 50 µs, p99 ≤ 250 µs).

Two books, both on the default 11 risk rules (throttle bucket out of the way):

* **demo book** (primary): the five demo accounts on one shard with their strategies' typical positions
  (3-6 products each, 13 instruments) and 4 resting orders per account (20) - the book the demo runs;
* **bench book**: 50 accounts × 20 instruments, 4 positions and 2 resting orders per account (the QS-07 book).

Measured, each call timed on its own with ``perf_counter_ns`` (GC enabled, as in the services):

1. ``check_order`` of opening orders - all 9 steps incl. funds and the what-if (primary);
2. ``check_order`` of closing orders (steps 4, 7, 8 are skipped for closes);
3. ``EngineCore.handle(order.submit)`` of an accepted order - the whole command: idempotency, the
   pipeline, the order record, its Persist rows, Route and reply effects (the route is then refused,
   untimed, so the book keeps its size);
4. the same after a tick of the order's instrument: the account is dirty, so ``handle`` evaluates it
   first (02 §6.2) - what the risk-engine does while market data flows.
"""

from __future__ import annotations

import cProfile
import itertools
from collections.abc import Callable
from typing import Any

from futrisk.bench.core import (
    BenchContext,
    BenchResult,
    GcMonitor,
    Metric,
    Target,
    latency_ns,
    now_ns,
    profile_lines,
)
from futrisk.bench.scenario import (
    EXTRA_INSTRUMENTS,
    INSTRUMENTS,
    MARKET_OPEN,
    TD,
    WorldSpec,
    book,
    product_of,
)
from futrisk.common.clock import SimClock, bj_to_ns
from futrisk.common.config import FullConfig, load_default_config
from futrisk.common.config.accounts import AccountsConfig, AccountSpec
from futrisk.common.config.products import InstrumentSpec
from futrisk.common.decimals import D
from futrisk.common.ids import SeqIdGen
from futrisk.core.effects import CommandCtx, ReplyTo
from futrisk.core.engine import EngineCore
from futrisk.core.instruments import InstrumentRegistry
from futrisk.core.pretrade import check_order
from futrisk.core.risk_account import Lifecycle
from futrisk.messages import codes
from futrisk.messages.enums import BrokerOrderStatus, Direction, Offset, OrderType, PosDirection, SourceId
from futrisk.messages.envelope import err_reply, ok_reply
from futrisk.messages.eod import SodAccount, SodLot, SodSnapshot
from futrisk.messages.md import Tick
from futrisk.messages.orders import OrderRequest, RouteAck

NAME = "pretrade"
TITLE = "In-process pre-trade check (EngineCore)"
TARGET = "p50 ≤ 50 µs, p99 ≤ 250 µs"
TARGETS = [Target("p50", "<=", 50, "us"), Target("p99", "<=", 250, "us")]

#: the demo accounts' typical books (instrument, +1 long / -1 short, lots) - see config/accounts.yaml strategies
DEMO_BOOK: dict[str, list[tuple[str, int, int]]] = {
    "ACC1": [("rb2701", 1, 60), ("hc2701", 1, 40), ("i2701", -1, 30), ("j2701", 1, 10)],
    "ACC2": [("rb2701", -1, 40), ("cu2611", 1, 3), ("au2612", 1, 5), ("m2701", 1, 50), ("IF2612", 1, 4),
             ("T2612", -1, 10)],
    "ACC3": [("rb2701", 1, 80), ("hc2701", 1, 75), ("i2701", 1, 32)],
    "ACC4": [("IF2612", 1, 2), ("IC2612", -1, 1)],
    "ACC5": [("rb2701", 1, 50), ("rb2705", -1, 50)],
}
SHFE_PRODUCTS = {"rb", "hc", "cu", "au"}


class EngineBook:
    """An EngineCore (SimClock at :data:`MARKET_OPEN`) with a loaded book, marks and resting orders."""

    def __init__(self, cfg: FullConfig, accounts: AccountsConfig, positions: dict[str, list[tuple[str, int, int]]],
                 capital: dict[str, str], *, resting_per_account: int) -> None:
        rr = cfg.risk_rules.model_copy(update={"pretrade": cfg.risk_rules.pretrade.model_copy(
            update={"throttle_rate_per_s": 1e9, "throttle_burst": 1_000_000_000})})
        self.registry = InstrumentRegistry(cfg.products, cfg.calendar)
        self.clock = SimClock(bj_to_ns(MARKET_OPEN))
        self.core = EngineCore(shard=1, registry=self.registry, rules=rr, accounts=accounts, clock=self.clock,
                               ids=SeqIdGen("b"), calendar=cfg.calendar, config_hash="bench")
        self.positions = positions
        self.instruments = sorted({i for rows in positions.values() for i, _s, _q in rows})
        self._volume: dict[str, int] = {}
        self._coid = itertools.count(1)
        sod_accounts = []
        for a, rows in sorted(positions.items()):
            lots = [SodLot(instrument_id=i, direction=PosDirection.LONG if s > 0 else PosDirection.SHORT, lot_no=n + 1,
                           open_td="20260924", open_ts_ns=n + 1, open_price=f"{INSTRUMENTS[i]:g}", qty=q,
                           basis_price=f"{INSTRUMENTS[i]:g}") for n, (i, s, q) in enumerate(rows)]
            cap = f"{D(capital[a]):.2f}"
            sod_accounts.append(SodAccount(account_id=a, pre_balance=cap, initial_capital=cap, cum_net_deposit="0.00",
                                           hwm_equity=cap, lots=lots))
        self.core.load_sod(SodSnapshot(trading_day=TD, version=1, accounts=sod_accounts,
                                       pre_settle={i: f"{INSTRUMENTS[i]:g}" for i in self.instruments}))
        self.core.set_lifecycle(Lifecycle.RUNNING)
        for i in self.instruments:
            self.core.apply_mark(self.tick(i, INSTRUMENTS[i]))
        self.core.evaluate_all()
        self.resting = 0
        for a, rows in sorted(positions.items()):
            for k in range(resting_per_account):
                inst, _s, _q = rows[k % len(rows)]
                ts = float(self.registry.get(inst).tick_size)
                buy = k % 2 == 0
                px = INSTRUMENTS[inst] + (-(5 + k) if buy else (5 + k)) * ts
                coid = f"rest-{a}-{k}"
                req = OrderRequest(client_order_id=coid, account_id=a, instrument_id=inst,
                                   direction=Direction.BUY if buy else Direction.SELL, offset=Offset.OPEN,
                                   order_type=OrderType.LIMIT, price=round(px, 6), qty=1)
                if self._reply(self.core.handle(req, CommandCtx("order.submit", token=coid))):
                    self.core.on_route_ack(coid, ok_reply(RouteAck(client_order_id=coid, status=BrokerOrderStatus.SUBMITTED,
                                                                   broker_order_ref=f"ref-{coid}")))
                    self.resting += 1
        self.core.evaluate_dirty()

    @staticmethod
    def _reply(effects: list[Any]) -> bool:
        replies = [e.reply for e in effects if isinstance(e, ReplyTo)]
        return bool(replies and replies[-1].ok)

    def tick(self, inst: str, px: float) -> Tick:
        v = self._volume.get(inst, 0) + 1
        self._volume[inst] = v
        info = self.registry.get(inst)
        pre = D(f"{INSTRUMENTS[inst]:g}")
        up, lo = self.registry.limit_prices(inst, pre, TD)
        now = self.clock.now_ns()
        return Tick(instrument_id=inst, exchange=info.exchange, trading_day=TD, action_day=TD, exch_ts_ns=now, last=px,
                    volume=v, turnover=0.0, open_interest=0, bid1=px - float(info.tick_size),
                    ask1=px + float(info.tick_size), upper_limit=float(up), lower_limit=float(lo), pre_settle=float(pre),
                    source=SourceId.FEED_A, recv_ns=now)

    def orders(self, kind: str) -> list[OrderRequest]:
        """One 1-lot order per position: ``open`` adds to it, ``close`` reduces it (yesterday's lots)."""
        out = []
        for a, rows in sorted(self.positions.items()):
            for inst, s, _q in rows:
                px = INSTRUMENTS[inst]
                if kind == "open":
                    d, off = (Direction.BUY, Offset.OPEN) if s > 0 else (Direction.SELL, Offset.OPEN)
                else:
                    shfe = product_of(inst) in SHFE_PRODUCTS
                    close = Offset.CLOSE_YESTERDAY if shfe else Offset.CLOSE
                    d, off = (Direction.SELL, close) if s > 0 else (Direction.BUY, close)
                out.append(OrderRequest(client_order_id="probe", account_id=a, instrument_id=inst, direction=d,
                                        offset=off, order_type=OrderType.LIMIT, price=px, qty=1))
        return out

    def check(self, req: OrderRequest) -> str | None:
        ra = self.core.account(req.account_id)
        r = check_order(self.core, ra, req, now_ns=self.clock.now_ns(), mono_ns=self.clock.monotonic_ns())
        return r.code

    def submit(self, req: OrderRequest) -> tuple[int, bool]:
        """``handle(order.submit)`` timed; the route is then refused (untimed) so the book keeps its size."""
        coid = f"b{next(self._coid)}"
        r = OrderRequest(client_order_id=coid, account_id=req.account_id, instrument_id=req.instrument_id,
                         direction=req.direction, offset=req.offset, order_type=req.order_type, price=req.price,
                         qty=req.qty)
        t0 = now_ns()
        effects = self.core.handle(r, CommandCtx("order.submit", token=coid))
        dt = now_ns() - t0
        ok = self._reply(effects)
        if ok:
            self.core.on_route_ack(coid, err_reply(codes.ROUTE_KILL_SWITCH))
        return dt, ok


def demo_book() -> EngineBook:
    cfg = load_default_config()
    accts = AccountsConfig(accounts={a: s.model_copy(update={"shard": 1}) for a, s in cfg.accounts.accounts.items()})
    capital = {a: str(s.initial_capital) for a, s in cfg.accounts.accounts.items()}
    return EngineBook(cfg, accts, DEMO_BOOK, capital, resting_per_account=4)


def bench_book(spec: WorldSpec | None = None) -> EngineBook:
    import dataclasses

    spec = spec or WorldSpec()
    base = load_default_config()
    have = set(base.products.instrument_ids())
    extra = [InstrumentSpec(instrument_id=i, product=p, last_trade_day=last, delivery_month=m)
             for i, p, last, m in EXTRA_INSTRUMENTS if i not in have]
    cfg = dataclasses.replace(base, products=base.products.model_copy(
        update={"instruments": [*base.products.instruments, *extra]}))
    positions = {a: [(i, 1 if d == PosDirection.LONG else -1, q) for i, d, q in rows] for a, rows in book(spec).items()}
    accts = AccountsConfig(accounts={a: AccountSpec(name=a, shard=1, initial_capital=D("10000000")) for a in positions})
    return EngineBook(cfg, accts, positions, {a: "10000000" for a in positions}, resting_per_account=2)


def _timed(fn: Callable[[OrderRequest], Any], reqs: list[OrderRequest], n: int, warmup: int, clock: SimClock
           ) -> tuple[list[int], int]:
    """``fn`` over ``reqs`` round robin: ``warmup`` untimed calls, then ``n`` timed ones; returns (ns, rejects)."""
    for k in range(warmup):
        fn(reqs[k % len(reqs)])
    out: list[int] = []
    rejects = 0
    for k in range(n):
        clock.advance(100_000)
        req = reqs[k % len(reqs)]
        t0 = now_ns()
        code = fn(req)
        out.append(now_ns() - t0)
        if code is not None:
            rejects += 1
    return out, rejects


def run(ctx: BenchContext) -> BenchResult:
    n = int(ctx.size("n", full=20_000, quick=4_000, tiny=200))
    warmup = int(ctx.size("warmup", full=1_000, quick=500, tiny=50))
    res = BenchResult(name=NAME, title=TITLE, target=TARGET, quick=ctx.quick,
                      params={"calls_per_metric": n, "warmup": warmup, "rules": "default risk_rules.yaml (11 rules)",
                              "gc": "enabled (as in the services)"})
    ctx.say("pretrade: building the demo and bench books")
    demo = demo_book()
    big = bench_book()
    res.params["demo_book"] = {"accounts": len(demo.positions), "instruments": len(demo.instruments),
                               "positions": sum(len(r) for r in demo.positions.values()), "resting_orders": demo.resting}
    res.params["bench_book"] = {"accounts": len(big.positions), "instruments": len(big.instruments),
                                "positions": sum(len(r) for r in big.positions.values()), "resting_orders": big.resting}
    prof = cProfile.Profile() if ctx.profile else None
    gcm = GcMonitor()
    metrics: list[Metric] = []
    with gcm:
        if prof is not None:
            prof.enable()
        all_opens = demo.orders("open")
        refused = sorted({f"{r.account_id} {r.instrument_id}: {demo.check(r)}" for r in all_opens if demo.check(r)})
        opens = [r for r in all_opens if demo.check(r) is None]  # orders the pipeline accepts (the full 9 steps)
        xs, rej = _timed(demo.check, opens, n, warmup, demo.clock)
        metrics.append(latency_ns("check_order, opening orders (demo book)", xs, targets=TARGETS, primary=True,
                                  note=f"{len(opens)} different accepted orders round robin; {rej} rejected"))
        if refused:
            res.notes.append("demo-book opening orders left out because the pipeline refuses them (what-if at step 8): "
                             + "; ".join(refused))
        if prof is not None:
            prof.disable()
        closes = demo.orders("close")
        xs, rej = _timed(demo.check, closes, n, warmup, demo.clock)
        metrics.append(latency_ns("check_order, closing orders (demo book)", xs, targets=TARGETS,
                                  note=f"{rej} rejected"))
        sub_ns: list[int] = []
        rejected = 0

        def submit(req: OrderRequest) -> None:
            nonlocal rejected
            dt, ok = demo.submit(req)
            sub_ns.append(dt)
            rejected += 0 if ok else 1

        _timed(submit, opens, n, warmup, demo.clock)
        metrics.append(latency_ns("handle(order.submit) accepted (demo book, clean account)", sub_ns[warmup:],
                                  targets=TARGETS, note=f"incl. order record, Persist rows, Route + reply effects; "
                                                        f"{rejected} rejected"))
        dirty_ns: list[int] = []
        px_step = itertools.cycle([1, 0, -1, 0])

        def submit_dirty(req: OrderRequest) -> None:
            inst = req.instrument_id
            px = INSTRUMENTS[inst] + next(px_step) * float(demo.registry.get(inst).tick_size)
            demo.core.apply_mark(demo.tick(inst, px))  # holders dirty: handle() evaluates the account first
            dt, _ok = demo.submit(req)
            dirty_ns.append(dt)

        _timed(submit_dirty, opens, n, warmup, demo.clock)
        demo.core.evaluate_dirty()
        metrics.append(latency_ns("handle(order.submit) after a tick (dirty account: evaluation first)",
                                  dirty_ns[warmup:], note="the risk-engine's case while market data flows (02 §6.2)"))
        big_opens = [r for r in big.orders("open") if big.check(r) is None]
        xs, rej = _timed(big.check, big_opens, n, warmup, big.clock)
        metrics.append(latency_ns("check_order, opening orders (bench book 50 × 20)", xs, targets=TARGETS,
                                  note=f"{rej} rejected"))
    for m in metrics:
        res.add(m)
    res.extra["gc"] = gcm.summary()
    if prof is not None:
        res.profile = ["(check_order, opening orders, demo book)", *profile_lines(prof)]
    return res


__all__ = ["NAME", "TARGET", "TITLE", "EngineBook", "bench_book", "demo_book", "run"]
