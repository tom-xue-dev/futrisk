"""Property tests for the clearing core (01 §10.2 QS-02, §8.16 对账).

Random multi-day, multi-account, multi-exchange trade sequences (SHFE explicit
close-today, CFFEX same-day-opens-first, DCE intraday both legs; FIFO and
TODAY_FIRST close styles; cash movements; trades handed over shuffled) must
satisfy:

- cumulative mark-to-market P&L == cumulative FIFO P&L (realized + float at the
  final settlement price) == an independent cash-flow oracle;
- every day, per account and contract: exchange formula == close + position P&L;
- the equity identity, day over day;
- position conservation: sod + open − close = eod = next-day SOD lots;
- account independence: settling accounts together or one by one gives the
  same per-account results;
- every trade fee equals an independent fee oracle (SHFE by offset, CFFEX by
  time order same-day-opens-first, DCE by the lots actually consumed), and the
  fee split agrees with the lot split wherever the rules say it must;
- the intraday ledger valued at the settlement prices agrees with the
  settlement (fees and close P&L exactly for SHFE/CFFEX);
- with several contracts per big-side group, exclusions (SHFE L−5, CFFEX
  treasury before the delivery month) and a mid-scenario margin raise, the
  settlement margin equals an independent margin oracle.

Hypothesis runs derandomized (fixed seed) without an example database, so the
suite is deterministic.
"""

from __future__ import annotations

import random
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from decimal import ROUND_HALF_UP, Decimal
from functools import lru_cache
from typing import Any

from hypothesis import HealthCheck, example, given, settings
from hypothesis import strategies as st

from futrisk.common.clock import parse_td, td_str
from futrisk.core import DaySettlement, InstrumentRegistry, IntradayLedger, seed_sod, settle_day
from futrisk.messages.enums import CashType, CheckStatus, Direction, FeeType, Offset, PosDirection
from futrisk.messages.eod import SodSnapshot
from futrisk.messages.orders import Trade
from futrisk.messages.risk import CashMovement
from tests.golden.builders import (
    TradeFactory,
    cash,
    formula_oracle,
    inst,
    lot_fee,
    make_calendar,
    make_registry,
    margin,
    product,
    rate_fee,
    views,
)

D = Decimal
PROPS = settings(
    max_examples=200,
    deadline=None,
    derandomize=True,
    database=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large],
)

ACCOUNTS = ["ACC1", "ACC2"]
CAPITAL = D("5000000")
CENT = D("0.01")


# ------------------------------------------------------------------ universes
@dataclass(frozen=True)
class Universe:
    """Contracts, their parameters as the test knows them (for oracles) and the registry factory."""

    start: str
    instruments: tuple[str, ...]
    base: dict[str, Decimal]
    tick: dict[str, Decimal]
    mult: dict[str, int]
    exchange: dict[str, str]
    # (per_lot, open, close_yd, close_today, intraday) —— 与注册表中的费率相同，由测试独立维护
    fees: dict[str, tuple[bool, Decimal, Decimal, Decimal, Decimal | None]]
    registry: Callable[[str], InstrumentRegistry]


def _index(**over: Any) -> dict[str, Any]:
    d = product("CFFEX", multiplier=300, tick="0.2", limit_pct="0.10", fees=[rate_fee("0.000023", close_today_mult="10")],
                margins=[margin("0.12")], settle_method="VWAP_LAST_HOUR", settle_window="14:00-15:00",
                big_side_group="CFFEX:INDEX")
    d.update(over)
    return d


def _treasury(rate: str) -> dict[str, Any]:
    return product("CFFEX", multiplier=10000, tick="0.005", limit_pct="0.02", fees=[lot_fee("3", close_today_mult="0")],
                   margins=[margin(rate)], settle_method="VWAP_LAST_HOUR", settle_window="14:15-15:15",
                   big_side_group="CFFEX:TREASURY", physical_delivery=True)


@lru_cache(maxsize=4)
def basic_registry(close_style: str) -> InstrumentRegistry:
    return make_registry(
        {
            "rb": product("SHFE", multiplier=10, tick="1", fees=[rate_fee("0.0001", close_today_mult="2")],
                          margins=[margin("0.07", broker_add="0.01")]),
            "IF": _index(),
            "j": product("DCE", multiplier=100, tick="0.5", fees=[lot_fee("1.5", intraday="3.1")], margins=[margin("0.12")]),
        },
        [inst("rb2701", "rb", "20270115"), inst("IF2612", "IF", "20261218"), inst("j2701", "j", "20270115")],
        rules={"CFFEX": {"close_style": close_style}, "DCE": {"close_style": close_style}},
    )


@lru_cache(maxsize=4)
def big_registry(close_style: str) -> InstrumentRegistry:
    return make_registry(
        {
            "rb": product("SHFE", multiplier=10, tick="1", fees=[rate_fee("0.0001", close_today_mult="2")],
                          margins=[margin("0.07"), margin("0.12", effective_from="20261009"),
                                   margin("0.10", contracts=["rb2610"], effective_from="20260901")]),
            "IF": _index(),
            "IH": _index(),
            "T": _treasury("0.02"),
            "TF": _treasury("0.012"),
            "j": product("DCE", multiplier=100, tick="0.5", fees=[lot_fee("1.5", intraday="3.1")], margins=[margin("0.12")]),
        },
        [
            inst("rb2610", "rb", "20261015", "202610"),
            inst("rb2701", "rb", "20270115", "202701"),
            inst("IF2612", "IF", "20261218", "202612"),
            inst("IH2612", "IH", "20261218", "202612"),
            inst("T2610", "T", "20261009", "202610"),
            inst("TF2610", "TF", "20261009", "202610"),
            inst("j2701", "j", "20270115", "202701"),
        ],
        rules={"CFFEX": {"close_style": close_style}, "DCE": {"close_style": close_style}},
    )


RB_FEE = (False, D("0.0001"), D("0.0001"), D("0.0002"), None)
IDX_FEE = (False, D("0.000023"), D("0.000023"), D("0.00023"), None)
T_FEE = (True, D("3"), D("3"), D("0"), None)
J_FEE = (True, D("1.5"), D("1.5"), D("1.5"), D("3.1"))

BASIC = Universe(
    start="20260902",
    instruments=("rb2701", "IF2612", "j2701"),
    base={"rb2701": D("3100"), "IF2612": D("4400"), "j2701": D("2000")},
    tick={"rb2701": D("1"), "IF2612": D("0.2"), "j2701": D("0.5")},
    mult={"rb2701": 10, "IF2612": 300, "j2701": 100},
    exchange={"rb2701": "SHFE", "IF2612": "CFFEX", "j2701": "DCE"},
    fees={"rb2701": RB_FEE, "IF2612": IDX_FEE, "j2701": J_FEE},
    registry=basic_registry,
)
BIG = Universe(
    start="20260929",  # 09-29, 09-30（国债退出）, 10-08（rb2610 L−5）, 10-09（保证金上调）
    instruments=("rb2610", "rb2701", "IF2612", "IH2612", "T2610", "TF2610", "j2701"),
    base={"rb2610": D("3050"), "rb2701": D("3100"), "IF2612": D("4400"), "IH2612": D("2850"),
          "T2610": D("101.5"), "TF2610": D("102"), "j2701": D("2000")},
    tick={"rb2610": D("1"), "rb2701": D("1"), "IF2612": D("0.2"), "IH2612": D("0.2"),
          "T2610": D("0.005"), "TF2610": D("0.005"), "j2701": D("0.5")},
    mult={"rb2610": 10, "rb2701": 10, "IF2612": 300, "IH2612": 300, "T2610": 10000, "TF2610": 10000, "j2701": 100},
    exchange={"rb2610": "SHFE", "rb2701": "SHFE", "IF2612": "CFFEX", "IH2612": "CFFEX", "T2610": "CFFEX",
              "TF2610": "CFFEX", "j2701": "DCE"},
    fees={"rb2610": RB_FEE, "rb2701": RB_FEE, "IF2612": IDX_FEE, "IH2612": IDX_FEE, "T2610": T_FEE, "TF2610": T_FEE,
          "j2701": J_FEE},
    registry=big_registry,
)


def trading_days(start: str, n: int) -> list[str]:
    cal = make_calendar()
    d = parse_td(start)
    out = []
    for _ in range(n + 1):
        out.append(td_str(d))
        d = cal.next_trading_day(d)
    return out


# ------------------------------------------------------------------ strategies
intent = st.tuples(
    st.integers(0, len(ACCOUNTS) - 1),  # account (opens)
    st.integers(0, 1_000),  # instrument (opens), modulo the universe
    st.integers(0, 4),  # 0 buy-open, 1 sell-open, 2..4 close one of the held positions
    st.integers(1, 6),  # qty
    st.integers(-15, 15),  # price offset in ticks from the day's settlement
    st.integers(0, 2),  # close flavour: SHFE CT/CY/C; others CLOSE/CLOSE/FORCE_CLOSE
    st.integers(0, 1_000),  # which held position a close picks
)
day_spec = st.tuples(
    st.lists(intent, max_size=12),
    st.lists(st.integers(-25, 25), min_size=8, max_size=8),  # settlement move in ticks per instrument
    st.lists(st.tuples(st.integers(0, len(ACCOUNTS) - 1), st.booleans(), st.integers(1, 200_000)), max_size=2),
    st.randoms(use_true_random=False),
)
scenario_spec = st.tuples(st.lists(day_spec, min_size=1, max_size=4), st.sampled_from(["FIFO", "TODAY_FIRST"]))


@dataclass
class SimDay:
    trading_day: str
    next_trading_day: str
    trades: list[Trade]
    cash: list[CashMovement]
    prices: dict[str, Decimal]
    pre: dict[str, Decimal]
    sod_qty: dict[tuple[str, str, PosDirection], int] = field(default_factory=dict)


@dataclass
class Scenario:
    universe: Universe
    close_style: str
    days: list[SimDay]

    @property
    def registry(self) -> InstrumentRegistry:
        return self.universe.registry(self.close_style)


def materialize(spec: tuple[list[tuple], str], u: Universe = BASIC) -> Scenario:  # type: ignore[type-arg]
    """Turn random intents into valid trades with an independent td/yd position model."""
    days_spec, style = spec
    tds = trading_days(u.start, len(days_spec))
    tf = TradeFactory()
    model: dict[tuple[str, str, PosDirection], list[int]] = {}  # key -> [td, yd]
    settle = dict(u.base)
    out: list[SimDay] = []
    for i, (intents, moves, cash_spec, rnd) in enumerate(days_spec):
        td, nxt = tds[i], tds[i + 1]
        pre = dict(settle)
        for k, inst_id in enumerate(u.instruments):
            settle[inst_id] = max(settle[inst_id] + moves[k % len(moves)] * u.tick[inst_id], u.tick[inst_id] * 100)
        sod_qty = {k: v[0] + v[1] for k, v in model.items() if v[0] + v[1] > 0}
        trades: list[Trade] = []
        for j, (a, n, action, qty, off, flavour, pick) in enumerate(intents):
            when = f"{td[:4]}-{td[4:6]}-{td[6:]} {9 + j // 30:02d}:{(j * 2) % 60:02d}:00"
            if action in (0, 1):
                acct, inst_id = ACCOUNTS[a], u.instruments[n % len(u.instruments)]
                price = float(max(settle[inst_id] + off * u.tick[inst_id], u.tick[inst_id]))
                side = PosDirection.LONG if action == 0 else PosDirection.SHORT
                model.setdefault((acct, inst_id, side), [0, 0])[0] += qty
                trades.append(tf(acct, inst_id, Direction.BUY if action == 0 else Direction.SELL, Offset.OPEN, price, qty, td, when))
                continue
            held = sorted((k for k, v in model.items() if v[0] + v[1] > 0), key=lambda k: (k[0], k[1], k[2].value))
            if not held:
                continue
            acct, inst_id, side = held[pick % len(held)]
            price = float(max(settle[inst_id] + off * u.tick[inst_id], u.tick[inst_id]))
            direction = Direction.SELL if side == PosDirection.LONG else Direction.BUY
            pos = model[(acct, inst_id, side)]
            if u.exchange[inst_id] == "SHFE":
                bucket = 0 if flavour == 0 else 1
                if pos[bucket] == 0:  # 选中的桶为空时改平另一桶，提高平今/平昨覆盖
                    bucket = 1 - bucket
                offset = Offset.CLOSE_TODAY if bucket == 0 else (Offset.CLOSE_YESTERDAY, Offset.CLOSE)[flavour % 2]
                q = min(qty, pos[bucket])
                pos[bucket] -= q
            else:
                offset = Offset.FORCE_CLOSE if flavour == 2 else Offset.CLOSE
                q = min(qty, pos[0] + pos[1])
                first, second = (1, 0) if style == "FIFO" else (0, 1)
                take = min(q, pos[first])
                pos[first] -= take
                pos[second] -= q - take
            trades.append(tf(acct, inst_id, direction, offset, price, q, td, when))
        movements = [
            cash(ACCOUNTS[a], td, CashType.DEPOSIT if dep else CashType.WITHDRAW, f"{amt}.00", f"cash-{td}-{k}")
            for k, (a, dep, amt) in enumerate(cash_spec)
        ]
        rnd.shuffle(trades)  # settle_day must order them itself
        out.append(SimDay(td, nxt, trades, movements, dict(settle), pre, sod_qty))
        for v in model.values():  # 换日：今仓转昨仓
            v[1] += v[0]
            v[0] = 0
    return Scenario(u, style, out)


def run(sc: Scenario, sod: SodSnapshot | None = None, accounts: list[str] | None = None) -> list[DaySettlement]:
    accts = accounts or ACCOUNTS
    sod = sod or seed_sod(sc.days[0].trading_day, {a: CAPITAL for a in accts}, pre_settle=sc.universe.base)
    results = []
    for d in sc.days:
        res = settle_day(
            trading_day=d.trading_day,
            version=1,
            sod=sod,
            trades=[t for t in d.trades if t.account_id in accts],
            cash_movements=[c for c in d.cash if c.account_id in accts],
            settle_prices=views(d.trading_day, d.prices),
            registry=sc.registry,
            next_trading_day=d.next_trading_day,
        )
        results.append(res)
        sod = res.next_sod
    return results


scenarios = scenario_spec.map(materialize)
big_scenarios = scenario_spec.map(lambda spec: materialize(spec, BIG))


# ------------------------------------------------------------------ oracles
def _chrono(trades: list[Trade]) -> list[Trade]:
    return sorted(trades, key=lambda t: (t.trade_ts_ns, t.trade_seq))


def fee_oracle(sc: Scenario) -> dict[str, tuple[Decimal, dict[FeeType, int]]]:
    """Independent per-trade fees: SHFE by offset, CFFEX by time order, DCE by the lots consumed."""
    u = sc.universe
    out: dict[str, tuple[Decimal, dict[FeeType, int]]] = {}
    yd: dict[tuple[str, str, PosDirection], int] = {}
    for d in sc.days:
        today: dict[tuple[str, str, PosDirection], deque[list[Any]]] = {}  # 今仓 lot 队列 [trade_key, 剩余]
        opens_fee: dict[tuple[str, str, PosDirection], deque[list[Any]]] = {}  # CFFEX 计费视角的当日未配对开仓
        legs: dict[str, dict[FeeType, int]] = {}
        prices: dict[str, Decimal] = {}
        for t in _chrono(d.trades):
            key = (t.account_id, t.instrument_id, PosDirection.LONG if (t.direction == Direction.BUY) == (t.offset == Offset.OPEN) else PosDirection.SHORT)
            exch = u.exchange[t.instrument_id]
            prices[t.trade_key] = D(str(t.price))
            if t.offset == Offset.OPEN:
                legs[t.trade_key] = {FeeType.OPEN: t.qty}
                today.setdefault(key, deque()).append([t.trade_key, t.qty])
                opens_fee.setdefault(key, deque()).append([t.trade_key, t.qty])
                continue
            q = t.qty
            # 持仓的 lot 消耗（FIFO：昨仓在前；TODAY_FIRST：今仓在前；SHFE 按显式开平）
            if exch == "SHFE":
                from_today = q if t.offset == Offset.CLOSE_TODAY else 0
            elif sc.close_style == "FIFO":
                from_today = max(0, q - yd.get(key, 0))
            else:
                from_today = min(q, sum(x[1] for x in today.get(key, ())))
            yd[key] = yd.get(key, 0) - (q - from_today)
            consumed: list[tuple[str, int]] = []
            need = from_today
            queue = today.get(key, deque())
            while need:
                take = min(need, queue[0][1])
                consumed.append((queue[0][0], take))
                queue[0][1] -= take
                need -= take
                if queue[0][1] == 0:
                    queue.popleft()
            if exch == "SHFE":
                legs[t.trade_key] = {FeeType.CLOSE_TODAY: from_today, FeeType.CLOSE_YD: q - from_today}
            elif exch == "CFFEX":
                matched, fq = 0, opens_fee.get(key, deque())
                while fq and matched < q:
                    take = min(fq[0][1], q - matched)
                    fq[0][1] -= take
                    matched += take
                    if fq[0][1] == 0:
                        fq.popleft()
                legs[t.trade_key] = {FeeType.CLOSE_TODAY: matched, FeeType.CLOSE_YD: q - matched}
            else:  # DCE：实际平掉的今仓两腿都按日内
                legs[t.trade_key] = {FeeType.INTRADAY_CLOSE: from_today, FeeType.CLOSE_YD: q - from_today}
                for open_key, n in consumed:
                    legs[open_key][FeeType.OPEN] -= n
                    legs[open_key][FeeType.INTRADAY_OPEN] = legs[open_key].get(FeeType.INTRADAY_OPEN, 0) + n
        for t in d.trades:
            per_lot, f_open, f_yd, f_today, f_intra = u.fees[t.instrument_id]
            rate = {FeeType.OPEN: f_open, FeeType.CLOSE_YD: f_yd, FeeType.CLOSE_TODAY: f_today,
                    FeeType.INTRADAY_OPEN: f_intra, FeeType.INTRADAY_CLOSE: f_intra}
            raw = D(0)
            for ft, n in legs[t.trade_key].items():
                if n:
                    r = rate[ft]
                    assert r is not None
                    raw += r * n if per_lot else prices[t.trade_key] * u.mult[t.instrument_id] * n * r
            out[t.trade_key] = (raw.quantize(CENT, rounding=ROUND_HALF_UP), {k: v for k, v in legs[t.trade_key].items() if v})
        for key, queue in today.items():  # 换日
            yd[key] = yd.get(key, 0) + sum(x[1] for x in queue)
    return out


def margin_oracle_big(td: str, positions: list[tuple[str, PosDirection, int]], prices: dict[str, Decimal]) -> Decimal:
    """Independent big-side margin for the BIG universe (rates, exclusions and groups hand-coded)."""

    def rate(i: str) -> Decimal:
        star = D("0.12") if td >= "20261009" else D("0.07")
        return {"rb2610": max(D("0.10"), star), "rb2701": star, "IF2612": D("0.12"), "IH2612": D("0.12"),
                "T2610": D("0.02"), "TF2610": D("0.012"), "j2701": D("0.12")}[i]

    group = {"rb2610": "SHFE:rb", "rb2701": "SHFE:rb", "IF2612": "CFFEX:INDEX", "IH2612": "CFFEX:INDEX",
             "T2610": "CFFEX:TREASURY", "TF2610": "CFFEX:TREASURY", "j2701": "GROSS:j2701"}
    exit_day = {"rb2610": "20261008", "T2610": "20260930", "TF2610": "20260930"}  # L−5；交割月前一交易日
    sums: dict[str, list[Decimal]] = {}
    for i, side, qty in positions:
        m = (prices[i] * BIG.mult[i] * qty * rate(i)).quantize(CENT, rounding=ROUND_HALF_UP)
        s = sums.setdefault(group[i], [D(0), D(0), D(0)])
        if i in exit_day and td >= exit_day[i]:
            s[2] += m
        else:
            s[0 if side == PosDirection.LONG else 1] += m
    total = D(0)
    for g, (lng, sht, excl) in sums.items():
        total += (lng + sht) if g.startswith("GROSS:") else max(lng, sht) + excl
    return total


# ------------------------------------------------------------------ properties
@PROPS
@given(scenarios)
def test_cumulative_mtm_equals_fifo_and_cash_flow_oracle(sc: Scenario) -> None:
    results = run(sc)
    final = sc.days[-1].prices
    for acct in ACCOUNTS:
        mtm = sum((r.accounts[acct].close_pnl + r.accounts[acct].position_pnl for r in results), D(0))
        fifo = sum((r.accounts[acct].fifo_realized_pnl for r in results), D(0)) + results[-1].accounts[acct].fifo_float_pnl
        assert mtm == fifo
        # 独立现金流口径：Σ(卖 − 买) + 期末净持仓 × 最终结算价
        oracle = D(0)
        for inst_id in sc.universe.instruments:
            flows = [t for d in sc.days for t in d.trades if t.account_id == acct and t.instrument_id == inst_id]
            cash_flow = sum(((D(str(t.price)) * t.qty) * (1 if t.direction == Direction.SELL else -1) for t in flows), D(0))
            net = sum((t.qty * (1 if t.direction == Direction.BUY else -1) for t in flows), 0)
            oracle += (cash_flow + final[inst_id] * net) * sc.universe.mult[inst_id]
        assert mtm == oracle


@PROPS
@given(scenarios)
def test_daily_exchange_formula_equals_close_plus_position(sc: Scenario) -> None:
    for d, r in zip(sc.days, run(sc), strict=True):
        assert all(c.status == CheckStatus.PASS for c in r.checks), [c for c in r.checks if c.status != CheckStatus.PASS]
        for acct in ACCOUNTS:
            total = D(0)
            for inst_id in sc.universe.instruments:
                mine = [t for t in d.trades if t.account_id == acct and t.instrument_id == inst_id]
                oracle = formula_oracle(
                    mine,
                    settle=d.prices[inst_id],
                    pre_settle=d.pre[inst_id],
                    yd_long=d.sod_qty.get((acct, inst_id, PosDirection.LONG), 0),
                    yd_short=d.sod_qty.get((acct, inst_id, PosDirection.SHORT), 0),
                    multiplier=sc.universe.mult[inst_id],
                )
                mtm = sum(
                    (p.close_pnl + p.position_pnl for p in r.positions if p.account_id == acct and p.instrument_id == inst_id),
                    D(0),
                )
                assert oracle == mtm, (acct, inst_id)
                total += oracle
            a = r.accounts[acct]
            assert a.exch_formula_pnl == total == a.close_pnl + a.position_pnl


@PROPS
@given(scenarios)
def test_equity_identity(sc: Scenario) -> None:
    results = run(sc)
    for acct in ACCOUNTS:
        prev_balance = CAPITAL
        cum_cash = D(0)
        cum_fee = D(0)
        cum_mtm = D(0)
        for d, r in zip(sc.days, results, strict=True):
            a = r.accounts[acct]
            dep = sum((D(c.amount) for c in d.cash if c.account_id == acct and c.type == CashType.DEPOSIT), D(0))
            wd = sum((D(c.amount) for c in d.cash if c.account_id == acct and c.type == CashType.WITHDRAW), D(0))
            assert a.pre_balance == prev_balance
            assert (a.deposit, a.withdraw) == (dep, wd)
            assert a.balance == a.pre_balance + dep - wd + a.close_pnl + a.position_pnl - a.fee == a.equity
            assert a.available == a.equity - a.margin
            assert a.margin_call == max(D(0), a.margin - a.equity)
            assert a.fee == sum((t.fee.fee for t in r.trades if t.trade.account_id == acct), D(0))
            cum_cash += dep - wd
            cum_fee += a.fee
            cum_mtm += a.close_pnl + a.position_pnl
            assert a.cum_net_deposit == cum_cash
            assert a.perf_equity == a.equity - cum_cash
            prev_balance = a.balance
            sod_acc = next(x for x in r.next_sod.accounts if x.account_id == acct)
            assert D(sod_acc.pre_balance) == a.balance and D(sod_acc.cum_net_deposit) == cum_cash
            assert D(sod_acc.hwm_equity) == a.hwm_equity >= a.perf_equity
        assert results[-1].accounts[acct].equity == CAPITAL + cum_cash + cum_mtm - cum_fee


@PROPS
@given(scenarios)
def test_position_conservation(sc: Scenario) -> None:
    results = run(sc)
    prev_eod: dict[tuple[str, str, PosDirection], int] = {}
    for d, r in zip(sc.days, results, strict=True):
        eod_by_key = {}
        for p in r.positions:
            opens = sum(t.qty for t in d.trades if t.account_id == p.account_id and t.instrument_id == p.instrument_id
                        and t.offset == Offset.OPEN and (t.direction == Direction.BUY) == (p.direction == PosDirection.LONG))
            closes = sum(t.qty for t in d.trades if t.account_id == p.account_id and t.instrument_id == p.instrument_id
                         and t.offset != Offset.OPEN and (t.direction == Direction.SELL) == (p.direction == PosDirection.LONG))
            assert (p.open_qty, p.close_qty) == (opens, closes)
            assert p.eod_qty == p.sod_qty + p.open_qty - p.close_qty == p.lot_qty >= 0
            assert p.close_today_qty + p.close_yd_qty == p.close_qty
            assert p.sod_qty == prev_eod.get(p.key, 0) == d.sod_qty.get(p.key, 0)
            eod_by_key[p.key] = p.eod_qty
        lots: dict[tuple[str, str, PosDirection], int] = {}
        for acc in r.next_sod.accounts:
            for lot in acc.lots:
                k = (acc.account_id, lot.instrument_id, lot.direction)
                lots[k] = lots.get(k, 0) + lot.qty
                assert D(lot.basis_price) == d.prices[lot.instrument_id]  # 换日后基准 = 今结算
        assert lots == {k: v for k, v in eod_by_key.items() if v > 0}
        prev_eod = {k: v for k, v in eod_by_key.items() if v > 0}


@PROPS
@given(scenarios)
def test_account_independence(sc: Scenario) -> None:
    together = run(sc)
    for acct in ACCOUNTS:
        alone = run(sc, accounts=[acct])
        for t_res, a_res in zip(together, alone, strict=True):
            assert t_res.accounts[acct] == a_res.accounts[acct]
            assert [p for p in t_res.result.positions if p.account_id == acct] == a_res.result.positions
            assert [g for g in t_res.result.margin_groups if g.account_id == acct] == a_res.result.margin_groups
            assert [x for x in t_res.result.trades if x.account_id == acct] == a_res.result.trades
            assert [x for x in t_res.result.close_details if x.account_id == acct] == a_res.result.close_details
            assert [x for x in t_res.next_sod.accounts if x.account_id == acct] == a_res.next_sod.accounts


@PROPS
@given(scenarios)
def test_fees_match_independent_oracle_and_lot_split(sc: Scenario) -> None:
    oracle = fee_oracle(sc)
    for r in run(sc):
        for tr in r.trades:
            fee, legs = oracle[tr.trade.trade_key]
            assert tr.fee.fee == fee, (tr.trade.trade_key, tr.fee, legs)
            assert {leg.fee_type: leg.qty for leg in tr.fee.legs} == legs
            if tr.fill.is_open:
                continue
            exch = sc.universe.exchange[tr.trade.instrument_id]
            fee_today = tr.fee.qty_of(FeeType.CLOSE_TODAY, FeeType.INTRADAY_CLOSE)
            # SHFE 与 DCE 永远按实际消耗的 lot；CFFEX 在 TODAY_FIRST 下与 lot 消耗一致
            if exch in ("SHFE", "DCE") or sc.close_style == "TODAY_FIRST":
                assert fee_today == tr.fill.close_today_qty, (exch, tr.trade.trade_key)


@PROPS
@given(scenarios)
def test_intraday_ledger_converges_to_settlement(sc: Scenario) -> None:
    """INTRADAY_VS_SETTLE: the ledger valued at the settlement prices matches the EOD numbers."""
    results = run(sc)
    sod = seed_sod(sc.days[0].trading_day, {a: CAPITAL for a in ACCOUNTS}, pre_settle=sc.universe.base)
    for d, r in zip(sc.days, results, strict=True):
        by_key = {tr.trade.trade_key: tr for tr in r.trades}
        for acc in sod.accounts:
            ledger = IntradayLedger(sc.registry, d.trading_day, acc, sod.pre_settle)
            mine = _chrono([t for t in d.trades if t.account_id == acc.account_id])
            n_dce = 0
            for t in mine:
                eff = ledger.apply_fill(t)
                tr = by_key[t.trade_key]
                assert eff.close_pnl == tr.close_pnl
                if sc.universe.exchange[t.instrument_id] == "DCE":
                    n_dce += 1
                else:
                    assert eff.fee_est == tr.fee.fee  # SHFE/CFFEX 的盘中手续费即为精确值
            for c in d.cash:
                if c.account_id == acc.account_id:
                    ledger.apply_cash(c)
            for inst_id, px in d.prices.items():
                ledger.set_mark(inst_id, float(px))
            funds = ledger.funds()
            a = r.accounts[acc.account_id]
            assert ledger.close_pnl == a.close_pnl
            # 只有 DCE 日内两腿的预估逐笔取整会与 EOD 精确值相差，最多 1 分/笔
            assert abs(ledger.fee - a.fee) <= CENT * n_dce
            assert abs(funds.position_pnl - float(a.position_pnl)) <= 1e-6 * max(1.0, abs(float(a.position_pnl)))
            assert abs(funds.margin - float(a.margin)) <= 1e-6 * max(1.0, float(a.margin))
            assert abs(funds.equity - float(a.equity)) <= 0.01 * n_dce + 1e-6 * float(a.equity)
        sod = r.next_sod


# 确定性样例：rb2610 多 / rb2701 空（按金额取大边，10-08 起 rb2610 退出），IF 多 / IH 空，T 空 / TF 多（09-30 起退出），持有过 10-09 上调
_BIG_EXAMPLE = materialize(
    (
        [
            ([(0, 0, 0, 8, 0, 0, 0), (0, 1, 1, 10, 0, 0, 0), (1, 2, 0, 1, 0, 0, 0), (1, 3, 1, 1, 0, 0, 0),
              (0, 4, 1, 2, 0, 0, 0), (0, 5, 0, 3, 0, 0, 0)], [1, -2, 3, 1, 5, -4, 2, 0], [], random.Random(0)),
            ([], [2, -1, 0, 0, 1, 1, 0, 0], [], random.Random(0)),
            ([], [3, -3, 1, -1, 0, 2, 1, 0], [], random.Random(0)),
            ([], [-1, 2, 0, 1, -2, 0, 0, 0], [], random.Random(0)),
        ],
        "TODAY_FIRST",
    ),
    BIG,
)


@PROPS
@example(_BIG_EXAMPLE)
@given(big_scenarios)
def test_big_side_groups_exclusions_and_schedule_changes(sc: Scenario) -> None:
    results = run(sc)
    oracle = fee_oracle(sc)
    for d, r in zip(sc.days, results, strict=True):
        assert r.ok, r.failed_checks()
        for acct in ACCOUNTS:
            held = [(p.instrument_id, p.direction, p.eod_qty) for p in r.positions if p.account_id == acct and p.eod_qty > 0]
            assert r.accounts[acct].margin == margin_oracle_big(d.trading_day, held, d.prices), (d.trading_day, acct, held)
        for tr in r.trades:
            assert tr.fee.fee == oracle[tr.trade.trade_key][0]


def test_materializer_is_deterministic() -> None:
    def spec() -> tuple[list[tuple], str]:  # type: ignore[type-arg]
        return ([([(0, 0, 0, 3, 1, 0, 0), (0, 0, 2, 2, 0, 0, 0), (1, 1, 1, 2, 0, 0, 0)], [1, 2, 3] * 3, [], random.Random(1))], "FIFO")

    a, b = materialize(spec()), materialize(spec())
    assert [t.trade_key for t in a.days[0].trades] == [t.trade_key for t in b.days[0].trades]
    assert len(a.days[0].trades) == 3
    big = materialize(spec(), BIG)
    assert big.days[0].trading_day == "20260929" and big.days[0].next_trading_day == "20260930"
