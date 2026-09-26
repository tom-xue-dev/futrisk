"""Exactness of the performance work (ENGINE.md §12): every cache and fast path equals the plain computation.

- ``next_rule_due_ns()`` (cached; only the accounts touched since the last call are rescanned) equals a full scan
  of every account's machines after any sequence of ticks, md.status changes, md-gateway loss, timers, resets,
  manual halts, fills, rule reloads and a restore;
- ``account.compliance_util`` (the counter / position part cached per version, the order-rate part live) equals a
  fresh :func:`compliance_utilization` of the account's inputs;
- :meth:`CompiledRule.edge_for`: some level is breached <=> ``breached(value, edge)`` (base levels and overrides);
- :func:`build_valuation` equals a straightforward reference implementation field by field, bit for bit
  (per-product and per-sector sums left to right, account totals with ``sum()``, which compensates in 3.12+).
"""

from __future__ import annotations

import math
from typing import Any

from enginetest_support import (
    PRE,
    RM,
    TD,
    B,
    Harness,
    O,
    S,
    config,
    registry,
    rules_with,
)
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from futrisk.common.config.risk_rules import RuleDef
from futrisk.common.decimals import D
from futrisk.core.engine import EngineCore
from futrisk.core.evaluation import next_due_ns
from futrisk.core.ledger import IntradayLedger
from futrisk.core.metrics import (
    LIMIT_LOSS_BUDGET,
    AccountValuation,
    HeldPosition,
    _ratio,
    build_valuation,
    compliance_utilization,
    effective_n_of,
    sector_exposures,
    worst_quality,
)
from futrisk.core.risk_account import Lifecycle
from futrisk.core.rules import compile_rule
from futrisk.messages.enums import BrokerOrderStatus, MdQuality, PosDirection, TradeOrigin
from futrisk.messages.eod import SodAccount, SodLot
from futrisk.messages.orders import CancelRequest
from futrisk.messages.risk import AccountMetricsView, ExposureView, HaltRequest, ResetRequest

FAST = settings(derandomize=True, deadline=None, suppress_health_check=list(HealthCheck), database=None)
P0 = {"rb2701": 3100.0, "hc2701": 3300.0, "i2701": 800.0, "IF2612": 4500.0}


# ------------------------------------------------------------------------------------------ next_rule_due_ns
def _timer_rules(warn: float = -0.002) -> Any:
    """daily_loss / intraday_dd with time-only confirmation, a hold and a cooldown: machines with timers."""
    base = config().risk_rules
    dl = base.rule("daily_loss")
    dd = base.rule("intraday_dd")
    assert dl is not None and dd is not None
    d1 = dl.model_dump()
    d1.update(hold_ms=1500, cooldown_s=2)
    th = [warn, -0.004, -0.006, -0.02]
    for lv, t in zip(d1["levels"], th, strict=True):
        lv["threshold"] = t
    d1["levels"][0]["confirm"] = {"states": 0, "ms": 800}
    d2 = dd.model_dump()
    d2.update(hold_ms=1200, cooldown_s=3)
    d2["levels"][0].update(threshold=0.002, confirm={"states": 0, "ms": 600})
    d2["levels"][1].update(threshold=0.004)
    return rules_with([RuleDef.model_validate(d1), RuleDef.model_validate(d2)])


def _full_scan(eng: EngineCore) -> int | None:
    best: int | None = None
    for a in eng.account_ids():
        ra = eng.account(a)
        d = next_due_ns(ra.machines.values(), ra.valuation)
        if d is not None and (best is None or d < best):
            best = d
    return best


def _book() -> Harness:
    h = Harness(_timer_rules())
    h.ticks(P0)
    h.open_position("ACC3", "rb2701", B, 80, 3100.0)
    h.open_position("ACC3", "hc2701", B, 75, 3300.0)
    h.open_position("ACC4", "IF2612", S, 3, 4500.0)
    h.open_position("ACC4", "i2701", B, 40, 800.0)
    return h


def _restart(a: Harness) -> Harness:
    b = Harness(a.rules, load=False)
    b.clock.set(a.clock.now_ns())
    snap = a.db.snapshot()
    b.eng.restore(snap, lifecycle=Lifecycle.RUNNING)
    b.db.tables.update({k: [dict(r) for r in v] for k, v in snap.items()})
    assert b.eng.next_rule_due_ns() == _full_scan(b.eng)  # before post_restore (the shell may ask for its timeout)
    for t in a.eng._ticks.values():
        b.eng.apply_mark(t)
    b.run(b.eng.post_restore())
    b.last_px = dict(a.last_px)
    b.volume = a.volume.copy()
    return b


_OPS = ["tick", "tick", "tick", "status", "down", "up", "timer", "timer", "reset", "halt", "fill", "rules", "restore"]


@settings(FAST, max_examples=120)
@given(ops=st.lists(st.tuples(st.sampled_from(_OPS), st.integers(-40, 15), st.integers(0, 2500),
                              st.sampled_from(list(P0)), st.sampled_from([MdQuality.OK, MdQuality.STALE])),
                    min_size=5, max_size=40))
def test_next_rule_due_cache_equals_a_full_scan(ops: list[tuple[str, int, int, str, MdQuality]]) -> None:
    h = _book()
    px = dict(P0)
    version = 1
    for i, (kind, d, dt, inst, q) in enumerate(ops):
        h.advance(dt)
        if kind == "tick":
            step = {"rb2701": 1.0, "hc2701": 1.0, "i2701": 0.5, "IF2612": 0.2}[inst]
            px[inst] = max(step, px[inst] + d * step * 5)
            h.tick(inst, px[inst])
        elif kind == "status":
            h.md_status({inst: q})
        elif kind == "down":
            h.eng.set_md_down(True)
            h.evaluate()
        elif kind == "up":
            h.eng.set_md_down(False)
            h.evaluate()
        elif kind == "timer":
            h.timer()
        elif kind == "reset":
            h.cmd("risk.reset", ResetRequest(command_id=f"r{i}", account_id="ACC3", reason="x", actor="rm", role=RM))
        elif kind == "halt":
            h.cmd("risk.halt", HaltRequest(command_id=f"h{i}", account_id="ACC4", reason="x", actor="rm", role=RM))
        elif kind == "fill":
            t = h.trade("ACC3", "rb2701", B if d >= 0 else S, O, px["rb2701"], 1 + abs(d) % 3, origin=TradeOrigin.EXTERNAL)
            h.run(h.eng.apply_exec(t))
            h.evaluate()
        elif kind == "rules":
            version += 1
            h.rules = _timer_rules(-0.002 if version % 2 else -0.003)
            h.run(h.eng.update_rules(h.rules, config_hash=f"v{version}", config_version=version))
            h.evaluate()
        else:
            h = _restart(h)
        expected = _full_scan(h.eng)
        assert h.eng.next_rule_due_ns() == expected, (i, kind)
        assert h.eng.next_rule_due_ns() == expected  # cached
    assert h.eng.next_timer_ns() is not None


def test_next_rule_due_cache_sees_a_quality_gate_and_its_release() -> None:
    """A pending time-only confirmation is gated while its scope is STALE and due again once OK."""
    h = _book()
    h.ticks({"rb2701": 3080.0, "hc2701": 3280.0})  # ACC3 daily loss about -0.31 %: warn PENDING (confirm.ms)
    due = h.eng.next_rule_due_ns()
    assert due is not None and due == _full_scan(h.eng)
    h.md_status({"rb2701": MdQuality.STALE, "hc2701": MdQuality.STALE})
    assert h.eng.next_rule_due_ns() == _full_scan(h.eng)
    assert h.eng.next_rule_due_ns() != due  # the gated timer is left out
    h.md_status({"rb2701": MdQuality.OK, "hc2701": MdQuality.OK})
    assert h.eng.next_rule_due_ns() == due == _full_scan(h.eng)


# ------------------------------------------------------------------------------------------ compliance cache
def test_compliance_util_cache_equals_a_fresh_computation() -> None:
    h = Harness(rules_with([r for r in config().risk_rules.rules if r.id == "compliance"]))
    h.ticks(P0)
    ra = h.eng.account("ACC3")

    def check() -> None:
        h.evaluate()
        val = h.eng.valuation(ra)
        fresh = compliance_utilization(registry(), ra.compliance(h.clock.monotonic_ns()))
        assert val.compliance_util == fresh
        assert ra.compliance_util(registry(), h.clock.monotonic_ns()) == fresh

    check()
    h.open_position("ACC3", "IF2612", B, 5, 4500.0)  # open volume, position, order rate
    check()
    for n in range(4):  # cancels (entries into CANCEL_PENDING) and a large cancel
        rep, _ = h.submit("ACC3", "IF2612", B, O, 16 if n == 0 else 1, 4490.0, coid=f"c{n}")
        assert rep.ok
        h.route_ok(f"c{n}")
        h.broker(f"c{n}", BrokerOrderStatus.ACCEPTED, remaining=16 if n == 0 else 1)
        h.cmd("order.cancel", _cancel(f"c{n}"))
        check()
    t1 = h.trade("ACC3", "IF2612", B, O, 4500.0, 1, trade_id="X1", origin=TradeOrigin.EXTERNAL)
    t2 = h.trade("ACC3", "IF2612", S, O, 4500.0, 1, trade_id="X1", origin=TradeOrigin.EXTERNAL)
    h.run(h.eng.apply_exec(t1))
    h.run(h.eng.apply_exec(t2))  # self trade
    check()
    assert ra.self_trades == 1
    h.advance(1500)  # the bucket refills: only the live part moves
    check()


def _cancel(coid: str) -> CancelRequest:
    return CancelRequest(command_id=f"x-{coid}", client_order_id=coid, account_id="ACC3", reason="test")


# ------------------------------------------------------------------------------------------ edge_for
_th = st.floats(-2.0, 2.0, allow_nan=False, allow_infinity=False)


@settings(FAST, max_examples=300)
@given(op=st.sampled_from(["<=", "<", ">=", ">"]), ths=st.lists(_th, min_size=1, max_size=4),
       ov=st.lists(_th, min_size=1, max_size=4), value=_th, who=st.sampled_from([None, "ACC3", "ACC4"]))
def test_edge_is_the_most_lenient_threshold(op: str, ths: list[float], ov: list[float], value: float,
                                             who: str | None) -> None:
    levels = [{"name": f"l{i}", "threshold": t, "action": "ALERT"} for i, t in enumerate(ths)]
    overrides = {"ACC3": {f"l{i}": {"threshold": t} for i, t in enumerate(ov[: len(ths)])}}
    rule = compile_rule(RuleDef.model_validate({"id": "r", "metric": "account.leverage", "op": op, "levels": levels,
                                                "overrides": overrides}))
    edge = rule.edge_for(who)
    anyb = any(rule.breached(value, lv.threshold) for lv in rule.levels_for(who))
    assert anyb == (edge is not None and rule.breached(value, edge))
    for lv in rule.levels_for(who):  # a threshold itself is breached exactly when the edge says so
        assert any(rule.breached(lv.threshold, x.threshold) for x in rule.levels_for(who)) == rule.breached(lv.threshold, edge)


# ------------------------------------------------------------------------------------------ build_valuation
def _reference(led: IntradayLedger, quality: dict[str, MdQuality], peak: float, tol: float) -> AccountValuation:
    """The straightforward valuation: positions, then per-product dicts, then sectors, qualities and sums."""
    from futrisk.core.metrics import StaticCache

    static = StaticCache(registry(), led.trading_day)
    funds = led.funds()
    equity = funds.equity
    held = []
    for inst, direction, qty in led.held():
        info = static.get(inst)
        mark = led.mark(inst)
        held.append(HeldPosition(instrument_id=inst, product=info.product, sector=info.sector, exchange=info.exchange,
                                 direction=direction, qty=qty, mark=mark, multiplier=info.multiplier,
                                 notional=abs(mark * info.multiplier * qty),
                                 position_pnl=led.position_pnl_of((led.account_id, inst, direction)),
                                 limit_pct=info.limit_pct))
    inst_q = {p.instrument_id: quality.get(p.instrument_id, MdQuality.UNKNOWN) for p in held}
    prod_q: dict[str, MdQuality] = {}
    sect_q: dict[str, MdQuality] = {}
    for p in held:
        prod_q[p.product] = worst_quality([prod_q.get(p.product, MdQuality.OK), inst_q[p.instrument_id]])
        sect_q[p.sector] = worst_quality([sect_q.get(p.sector, MdQuality.OK), inst_q[p.instrument_id]])
    per: dict[str, dict[str, Any]] = {}
    for p in held:
        d = per.setdefault(p.product, {"sector": p.sector, "exchange": p.exchange, "long": 0, "short": 0, "net": 0.0,
                                       "gross": 0.0, "lp": p.limit_pct})
        d["long" if p.direction == PosDirection.LONG else "short"] += p.qty
        d["net"] += p.signed_notional
        d["gross"] += p.notional
    exposures = {}
    for prod in sorted(per):
        d = per[prod]
        lp = float(d["lp"])
        cap = LIMIT_LOSS_BUDGET / lp if lp > 0 else None
        npct = _ratio(abs(d["net"]), equity)
        exposures[prod] = ExposureView(product=prod, sector=d["sector"], exchange=d["exchange"], long_lots=d["long"],
                                       short_lots=d["short"], net_lots=d["long"] - d["short"], net_notional=d["net"],
                                       gross_notional=d["gross"], net_pct_equity=npct, cap_pct=cap,
                                       utilization=npct / cap if cap else None, quality=prod_q[prod])
    gross = sum(p.notional for p in held)
    acct_q = worst_quality(inst_q.values())
    nonok = sum(p.notional for p in held if inst_q[p.instrument_id] != MdQuality.OK)
    if acct_q != MdQuality.OK and tol > 0 and gross > 0 and nonok / gross <= tol:
        acct_q = MdQuality.OK
    daily = equity - funds.static_equity
    perf = equity - funds.cum_net_deposit
    pk = max(peak, perf)
    hwm_sod = float(led.hwm_equity_sod)
    hwm = max(hwm_sod, pk)
    eff = effective_n_of(exposures.values())
    stress = sum(abs(ev.net_notional) * float(per[prod]["lp"]) for prod, ev in exposures.items())
    metrics = AccountMetricsView(
        daily_pnl=daily, daily_pnl_pct_init=_ratio(daily, funds.initial_capital),
        daily_pnl_pct_sod=_ratio(daily, funds.static_equity),
        floating_pnl_pct_init=_ratio(funds.position_pnl, funds.initial_capital), perf_equity=perf,
        nav=_ratio(perf, funds.initial_capital), intraday_peak_equity=pk,
        intraday_dd=_ratio(pk - perf, pk) if pk > 0 else 0.0, hwm_equity=hwm,
        hwm_dd=_ratio(hwm - perf, hwm) if hwm > 0 else 0.0, gross_notional=gross,
        net_notional=sum(p.signed_notional for p in held), leverage=_ratio(gross, equity),
        effective_n=eff if eff is not None else 0.0, limit_stress=_ratio(stress, equity), orders_today=0,
        cancels_today=0)
    return AccountValuation(
        account_id=led.account_id, funds=funds, metrics=metrics, exposures=exposures,
        sectors=sector_exposures(exposures, equity), positions=tuple(held), quality=acct_q, product_quality=prod_q,
        sector_quality=sect_q, instrument_quality=inst_q, compliance_util=0.0, hwm_sod=hwm_sod,
        limit_pct={p: float(per[p]["lp"]) for p in sorted(per)}, product_sector={p: per[p]["sector"] for p in sorted(per)})


_INSTS = ["rb2701", "hc2701", "i2701", "j2701", "IF2612", "cu2611"]
_lot = st.tuples(st.sampled_from(_INSTS), st.sampled_from([PosDirection.LONG, PosDirection.SHORT]), st.integers(1, 400))


@settings(FAST, max_examples=150)
@given(lots=st.lists(_lot, min_size=0, max_size=12), moves=st.lists(st.floats(-0.09, 0.09), min_size=6, max_size=6),
       qs=st.lists(st.sampled_from([MdQuality.OK, MdQuality.OK, MdQuality.STALE, MdQuality.UNKNOWN]), min_size=6,
                   max_size=6),
       tol=st.sampled_from([0.0, 0.05, 0.1, 0.5]), peak=st.floats(9e6, 11e6))
def test_build_valuation_equals_the_reference_bit_for_bit(lots: list[tuple[str, PosDirection, int]], moves: list[float],
                                                           qs: list[MdQuality], tol: float, peak: float) -> None:
    sod_lots = [SodLot(instrument_id=i, direction=d, lot_no=n + 1, open_td="20260925", open_ts_ns=n + 1,
                       open_price=str(PRE[i]), qty=q, basis_price=str(PRE[i])) for n, (i, d, q) in enumerate(lots)]
    sa = SodAccount(account_id="ACC3", pre_balance="10000000.00", initial_capital="10000000.00", cum_net_deposit="0.00",
                    hwm_equity="10300000.00", lots=sod_lots)
    led = IntradayLedger(registry(), TD, sa, {k: D(v) for k, v in PRE.items()})
    for inst, mv in zip(_INSTS, moves, strict=True):
        led.set_mark(inst, round(PRE[inst] * (1 + mv), 1))
    quality = dict(zip(_INSTS, qs, strict=True))
    got = build_valuation(led, registry(), quality=quality, intraday_peak=peak, compliance_util=0.0, quality_tolerance=tol)
    ref = _reference(led, quality, peak, tol)
    assert got == ref
    assert math.isfinite(got.metrics.limit_stress)
