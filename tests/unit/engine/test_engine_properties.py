"""Property tests of the EngineCore (promoted review probes p08, p09, p10) and of the scalar what-if.

- the alert state machine keeps its invariants on random value / time / quality / reset sequences (01 §8.14);
- on random ACC3 price paths with a restore at a random point: HALTED ⇔ the halt machine, one ALERT_RAISED per
  alert key, persisted ``rule_states`` = memory, opens always refused while HALTED (01 §6.5, §8.14; QS-04);
- the incremental valuation (ticks only for holders, registered resting orders) equals a full rebuild from the
  SOD + fills + marks;
- :class:`~futrisk.core.metrics.WhatIf` gives exactly the numbers of :meth:`AccountValuation.whatif`.

Hypothesis runs derandomized (a fixed example sequence) with moderate example counts.
"""

from __future__ import annotations

import contextlib
import math
from collections import Counter
from typing import Any

from enginetest_support import (
    CT,
    CY,
    RM,
    TD,
    B,
    C,
    Harness,
    O,
    S,
    registry,
    rules_only,
    sod_snapshot,
)
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from futrisk.common.config.risk_rules import RuleDef
from futrisk.common.decimals import D
from futrisk.core.ledger import IntradayLedger
from futrisk.core.metrics import WhatIf, build_valuation, metric_value, registered_metrics
from futrisk.core.pretrade import order_delta, resting_orders
from futrisk.core.risk_account import Lifecycle
from futrisk.core.rules import compile_rule
from futrisk.core.state_machine import LevelMachine, Tr
from futrisk.messages.enums import (
    AccountState,
    BrokerOrderStatus,
    MdQuality,
    PosDirection,
    RiskEventKind,
    RuleSmState,
    TradeOrigin,
)
from futrisk.messages.eod import SodLot
from futrisk.messages.orders import OrderRequest
from futrisk.messages.risk import ResetRequest

MS = 1_000_000
FAST = settings(derandomize=True, deadline=None, suppress_health_check=list(HealthCheck), database=None)


# ------------------------------------------------------------------------------------------ p08: state machine
def _machine(latch: bool, states: int, ms: int, hold: int, cool: int, op: str) -> LevelMachine:
    thr = -0.03 if op == "<=" else 0.03
    rule = compile_rule(RuleDef.model_validate({
        "id": "r", "metric": "account.daily_pnl_pct_init", "op": op, "hold_ms": hold, "cooldown_s": cool,
        "levels": [{"name": "l", "threshold": thr, "action": "CIRCUIT_BREAK" if latch else "ALERT", "latch": latch,
                    "confirm": {"states": states, "ms": ms}, "rearm": {"extra": 0.01}}]}))
    lv = rule.level("A", "l")
    assert lv is not None
    return LevelMachine(rule, lv, "acct:A")


_step = st.tuples(st.floats(-0.08, 0.08, allow_nan=False), st.booleans(), st.booleans(), st.integers(0, 3000),
                  st.sampled_from(["eval", "eval", "eval", "timer", "reset"]), st.booleans())


@settings(FAST, max_examples=1000)
@given(latch=st.booleans(), states=st.integers(0, 3), ms=st.sampled_from([0, 500, 1000]),
       hold=st.sampled_from([0, 1000, 3000]), cool=st.sampled_from([0, 1, 60]), op=st.sampled_from(["<=", ">="]),
       steps=st.lists(_step, min_size=1, max_size=40))
def test_level_machine_invariants(latch: bool, states: int, ms: int, hold: int, cool: int, op: str,
                                  steps: list[tuple[float, bool, bool, int, str, bool]]) -> None:
    if states == 0 and ms == 0:
        ms = 500  # time-only confirmation needs confirm.ms (compile rejects states=0, ms=0)
    m = _machine(latch, states, ms, hold, cool, op)
    sign = 1 if op == "<=" else -1
    now = 10**15
    key = 0
    distinct: set[int] = set()
    pending_since: int | None = None
    last_clear: int | None = None
    for v0, q, new_state, dt, kind, esc in steps:
        v = v0 * sign
        now += dt * MS
        ep0, st0, latched0 = m.st.episode, m.state, m.st.latched
        if kind == "reset":
            m.reset(now)  # only a latched ACTIVE level moves (to REARMED)
            continue
        if kind == "timer":
            sk = None
            trs = m.reevaluate(now, quality_ok=q, escalating=esc)
        else:
            if new_state:
                key += 1
            sk = key
            trs = m.evaluate(v, quality_ok=q, state_key=sk, now_ns=now, escalating=esc)
        if not q:
            # quality gating: nothing moves; only a running hold timer is reset (and persisted)
            assert trs in ([], [Tr.PROGRESS]) and m.state == st0 and m.st.episode == ep0
            assert m.st.clear_since_ns is None
        if Tr.ACTIVATED in trs:
            assert trs.count(Tr.ACTIVATED) == 1 and m.st.episode == ep0 + 1 and m.st.latched == latch
            if states != 1:
                assert st0 == RuleSmState.PENDING  # never straight from OK / REARMED
                ok_time = ms > 0 and pending_since is not None and now - pending_since >= ms * MS
                ok_states = states > 0 and sk is not None and len(distinct | {sk}) >= states
                assert ok_time or ok_states, (states, ms, len(distinct), now - (pending_since or now))
            if st0 == RuleSmState.OK and not latch and last_clear is not None and cool > 0 and not esc:
                assert now >= last_clear + cool * 1000 * MS  # ALERT cooldown
        else:
            assert m.st.episode == ep0
        if Tr.CLEARED in trs:
            assert st0 == RuleSmState.ACTIVE and not latched0
            last_clear = now
        if Tr.REVOKED in trs:
            assert st0 == RuleSmState.PENDING
        if m.state == RuleSmState.ACTIVE and m.st.latched:
            assert latched0 or Tr.ACTIVATED in trs
        if m.state == RuleSmState.PENDING:
            if st0 != RuleSmState.PENDING:
                distinct = {sk} if sk is not None else set()
                pending_since = m.st.pending_since_ns
            elif sk is not None and q:
                distinct.add(sk)
        else:
            distinct, pending_since = set(), None


# ------------------------------------------------------------------------------------------ p09: engine paths
P0 = {"rb2701": 3100.0, "hc2701": 3300.0, "i2701": 800.0}


def _acc3_book() -> Harness:
    h = Harness(rules_only("daily_loss"))
    h.ticks(P0)
    h.open_position("ACC3", "rb2701", B, 80, 3100.0)
    h.open_position("ACC3", "hc2701", B, 75, 3300.0)
    h.open_position("ACC3", "i2701", B, 32, 800.0)
    return h


def _check(h: Harness) -> None:
    ra = h.eng.account("ACC3")
    halt = ra.machines.get(("daily_loss", "halt", "acct:ACC3"))
    hs = halt.state if halt is not None else RuleSmState.OK
    assert (ra.state == AccountState.HALTED) == (hs in (RuleSmState.PENDING, RuleSmState.ACTIVE))
    assert ra.halted_latched == (halt is not None and hs == RuleSmState.ACTIVE and halt.st.latched)
    rows = {(r["rule_id"], r["level"], r["scope_key"]): r for r in h.db.tables["rule_states"] if r["trading_day"] == TD}
    for k, m in ra.machines.items():
        r = rows.get(k)
        assert r is not None and r["state"] == m.state.value and r["episode"] == m.st.episode
        assert bool(r["latched"]) == m.st.latched


def _restart(a: Harness) -> Harness:
    b = Harness(a.rules, load=False)
    b.clock.set(a.clock.now_ns())
    snap = a.db.snapshot()
    b.eng.restore(snap, lifecycle=Lifecycle.RUNNING)
    b.db.tables.update({k: [dict(r) for r in v] for k, v in snap.items()})
    for t in a.eng._ticks.values():  # ctl.snapshot of md.tick.*
        b.eng.apply_mark(t)
    b.run(b.eng.post_restore())
    b.last_px = dict(a.last_px)
    b.volume = a.volume.copy()
    return b


@settings(FAST, max_examples=150)
@given(path=st.lists(st.tuples(st.integers(-90, 30), st.integers(0, 1500), st.booleans(),
                               st.sampled_from(["tick", "tick", "timer", "reset", "submit"])), min_size=5, max_size=30),
       cut=st.integers(0, 30))
def test_engine_paths_with_a_restore(path: list[tuple[int, int, bool, str]], cut: int) -> None:
    h = _acc3_book()
    px = dict(P0)
    hb: Harness | None = None
    for i, (d, dt, both, kind) in enumerate(path):
        engines = [h] + ([hb] if hb is not None else [])
        for e in engines:
            e.advance(dt)
        if kind == "tick":
            px["rb2701"] = max(2800.0, min(3300.0, px["rb2701"] + d))
            if both:
                px["hc2701"] = max(3000.0, min(3500.0, px["hc2701"] + d))
            for e in engines:
                e.ticks({k: px[k] for k in (("rb2701", "hc2701") if both else ("rb2701",))})
        elif kind == "timer":
            for e in engines:
                e.timer()
        elif kind == "reset":
            for e in engines:
                e.cmd("risk.reset", ResetRequest(command_id=f"r{i}", account_id="ACC3", reason="x", actor="rm", role=RM))
        else:
            for n, e in enumerate(engines):
                halted = e.eng.account("ACC3").state == AccountState.HALTED
                rep, _ = e.submit("ACC3", "hc2701", S, O, 1, px["hc2701"], coid=f"s{i}-{n}")
                if halted:
                    assert not rep.ok  # opens are always refused while HALTED
        for e in engines:
            _check(e)
        if i == cut and hb is None:
            hb = _restart(h)
            _check(hb)
    live = Counter(ev.alert_key for ev in h.events(RiskEventKind.ALERT_RAISED))
    assert all(n == 1 for n in live.values())  # one ALERT_RAISED per alert key
    if hb is not None:
        restored = Counter(ev.alert_key for ev in hb.events(RiskEventKind.ALERT_RAISED))
        assert all(n == 1 for n in restored.values()) and set(restored) <= set(live)


# ------------------------------------------------------------------------------------------ p10: incremental
INSTS = {"rb2701": 3100.0, "hc2701": 3300.0, "i2701": 800.0, "IF2612": 4500.0, "cu2611": 80000.0}
TICK = {"rb2701": 1.0, "hc2701": 1.0, "i2701": 0.5, "IF2612": 0.2, "cu2611": 10.0}


def _lots() -> dict[str, list[SodLot]]:
    return {"ACC3": [SodLot(instrument_id="rb2701", direction=PosDirection.LONG, lot_no=1, open_td="20260928", open_ts_ns=1,
                            open_price="3050", qty=20, basis_price="3100"),
                     SodLot(instrument_id="IF2612", direction=PosDirection.SHORT, lot_no=2, open_td="20260928",
                            open_ts_ns=2, open_price="4550", qty=3, basis_price="4500")]}


_op = st.tuples(st.sampled_from(list(INSTS)), st.sampled_from(["tick", "tick", "open", "close", "rest", "cancel"]),
                st.integers(-20, 20), st.integers(1, 5), st.booleans())


def _drive(h: Harness, ops: list[tuple[str, str, int, int, bool]]) -> dict[str, float]:
    px = dict(INSTS)
    live: list[str] = []
    for n, (inst, kind, d, q, buy) in enumerate(ops):
        if kind == "tick":
            px[inst] = round((px[inst] + d * TICK[inst]) / TICK[inst]) * TICK[inst]
            h.tick(inst, px[inst])
        elif kind == "open":
            t = h.trade("ACC3", inst, B if buy else S, O, px[inst], q, origin=TradeOrigin.EXTERNAL)
            h.run(h.eng.apply_exec(t))
            h.evaluate()
        elif kind == "close":
            side = PosDirection.LONG if buy else PosDirection.SHORT
            pos = h.eng.account("ACC3").ledger.book.find("ACC3", inst, side)
            if pos is None or pos.qty == 0:
                continue
            off = C
            if registry().rules(inst).explicit_close_today:
                off = CT if pos.td_qty >= min(q, pos.qty) and pos.yd_qty < min(q, pos.qty) else CY
                k = min(q, pos.td_qty if off == CT else pos.yd_qty)
            else:
                k = min(q, pos.qty)
            if k <= 0:
                continue
            t = h.trade("ACC3", inst, S if buy else B, off, px[inst], k, origin=TradeOrigin.EXTERNAL)
            h.run(h.eng.apply_exec(t))
            h.evaluate()
        elif kind == "rest":
            rep, _ = h.submit("ACC3", inst, B if buy else S, O, q, px[inst], coid=f"p{n}")
            if rep.ok:
                live.append(f"p{n}")
        elif kind == "cancel" and live:
            coid = live.pop(0)
            h.route_ok(coid)
            h.broker(coid, BrokerOrderStatus.CANCELED)
    return px


@settings(FAST, max_examples=100)
@given(ops=st.lists(_op, min_size=3, max_size=30))
def test_incremental_valuation_equals_a_full_rebuild(ops: list[tuple[str, str, int, int, bool]]) -> None:
    h = Harness(rules_only("daily_loss"), sod=sod_snapshot(lots=_lots()))
    h.ticks(dict(INSTS))
    px = _drive(h, ops)
    ra = h.eng.account("ACC3")
    f_inc = ra.ledger.funds()
    sod = sod_snapshot(lots=_lots())
    sa = [a for a in sod.accounts if a.account_id == "ACC3"][0]
    led = IntradayLedger(registry(), TD, sa, {k: D(v) for k, v in sod.pre_settle.items()})
    for t in ra.trades:
        led.apply_fill(t, lenient=True)
    for inst, p in px.items():
        led.set_mark(inst, p)
    fm, ff = led.frozen_for_orders(resting_orders(h.eng, "ACC3"))
    f_full = led.funds()
    for fld in ("equity", "margin", "position_pnl", "close_pnl", "fee"):
        assert math.isclose(getattr(f_inc, fld), getattr(f_full, fld), rel_tol=1e-9, abs_tol=1e-6), fld
    assert math.isclose(f_inc.frozen_margin, fm, rel_tol=1e-9, abs_tol=1e-6)
    assert math.isclose(f_inc.frozen_fee, ff, rel_tol=1e-9, abs_tol=1e-6)
    v = h.eng.valuation(ra)
    v2 = build_valuation(led, registry(), quality={i: MdQuality.OK for i in INSTS}, intraday_peak=ra.intraday_peak)
    for fld in ("daily_pnl", "gross_notional", "net_notional", "leverage", "limit_stress", "effective_n"):
        assert math.isclose(getattr(v.metrics, fld), getattr(v2.metrics, fld), rel_tol=1e-9, abs_tol=1e-6), fld
    # the by_instrument index follows positions and working orders exactly
    held = {i for i, _d, _q in ra.ledger.held()}
    working = {o.instrument_id for o in h.eng.orders.resting("ACC3")}
    assert {i for i in INSTS if "ACC3" in h.eng.holders(i)} == held | working
    assert ra.held_insts == held


# ------------------------------------------------------------------------------------------ scalar what-if
_METRICS = sorted(n for n in registered_metrics() if n.startswith(("account.", "product.", "sector.")))


@settings(FAST, max_examples=120)
@given(ops=st.lists(_op, min_size=2, max_size=20), inst=st.sampled_from(list(INSTS)), buy=st.booleans(),
       qty=st.integers(1, 30), off=st.integers(-30, 30))
def test_scalar_whatif_equals_the_full_whatif(ops: list[tuple[str, str, int, int, bool]], inst: str, buy: bool,
                                              qty: int, off: int) -> None:
    h = Harness(rules_only("daily_loss"), sod=sod_snapshot(lots=_lots()))
    h.ticks(dict(INSTS))
    px = _drive(h, ops)
    ra = h.eng.account("ACC3")
    val = h.eng.valuation(ra)
    price = round((px[inst] + off * TICK[inst]) / TICK[inst]) * TICK[inst]
    req = OrderRequest(client_order_id="w", account_id="ACC3", instrument_id=inst, direction=B if buy else S, offset=O,
                       price=price, qty=qty, sent_ns=0)
    delta = order_delta(h.eng, req, margin=1234.5 * qty, fee=2.5 * qty)
    fast = WhatIf(val, delta)
    full = val.whatif(delta)
    info = registry().get(inst)
    other_prod = next((p for p in val.exposures if p != info.product), "zz")
    other_sector = next((s for s in val.sectors if s != info.sector), "zz")
    for name in _METRICS:
        kind = name.split(".", 1)[0]
        keys: list[Any] = [None]
        if kind == "product":
            keys = [info.product, other_prod]
        elif kind == "sector":
            keys = [info.sector, other_sector]
        for key in keys:
            a, b = fast.value(name, key), metric_value(name, full, key)
            assert (a is None and b is None) or a == b, (name, key, a, b)
    assert fast.gross_pct() == (full.metrics.gross_notional / full.funds.equity if full.funds.equity > 0 else fast.gross_pct())


# ------------------------------------------------------------------------------------------ lots aggregates
_lot_op = st.tuples(st.sampled_from(["open", "open", "close", "load", "roll"]), st.integers(1, 7),
                    st.sampled_from(["3100", "3100.5", "3099", "3101.25", "3080.2"]), st.integers(0, 50),
                    st.sampled_from(["CLOSE", "CLOSE_TODAY", "CLOSE_YESTERDAY"]))


@settings(FAST, max_examples=300)
@given(explicit=st.booleans(), style=st.sampled_from(["FIFO", "TODAY_FIRST"]), ops=st.lists(_lot_op, max_size=60))
def test_position_running_aggregates_equal_a_recount(explicit: bool, style: str,
                                                      ops: list[tuple[str, int, str, int, str]]) -> None:
    """The O(1) quantities / notionals of a Position equal summing its lots (value AND Decimal exponent)."""
    from decimal import Decimal

    from futrisk.core.lots import Lot, Position, PositionError
    from futrisk.messages.enums import Offset

    pos = Position("A", "rb2701", PosDirection.LONG, "20260929", close_style=style, explicit_close_today=explicit)
    day = 29
    for kind, qty, px, ts, off in ops:
        if kind == "open":
            pos.open(qty, Decimal(px), 1_000 + ts)
        elif kind == "load":
            pos.load_lot(Lot(lot_no=pos.next_lot_no, open_td="20260928", open_ts_ns=ts, open_price=Decimal(px), qty=qty,
                             basis_price=Decimal(px)))
        elif kind == "close":
            with contextlib.suppress(PositionError):
                pos.close(Offset(off), qty)
        else:
            day += 1
            pos.roll(f"202609{day:02d}" if day <= 30 else "20261008", Decimal(px))
        td = pos.trading_day
        assert pos.qty == sum(lot.qty for lot in pos.lots)
        assert pos.td_qty == sum(lot.qty for lot in pos.lots if lot.open_td == td)
        assert pos.yd_qty == sum(lot.qty for lot in pos.lots if lot.open_td != td)
        assert pos.split() == (pos.td_qty, pos.yd_qty)
        on = sum((lot.open_price * lot.qty for lot in pos.lots), Decimal(0))
        bn = sum((lot.basis_price * lot.qty for lot in pos.lots), Decimal(0))
        assert str(pos.open_notional) == str(on) and str(pos.basis_notional) == str(bn)
        keys = [lot.sort_key for lot in pos.lots]
        assert keys == sorted(keys)
