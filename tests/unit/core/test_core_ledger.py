"""Unit tests for futrisk.core.ledger: intraday AccountFunds / PositionView (02 §7.6), incremental hot path."""

from __future__ import annotations

from decimal import Decimal

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st
from tests.golden.builders import (
    CT,
    CY,
    B,
    C,
    O,
    S,
    TradeFactory,
    cash,
    inst,
    lot_fee,
    make_registry,
    margin,
    product,
    rate_fee,
)

from futrisk.core import IntradayLedger, PositionError, RestingOrder, seed_sod
from futrisk.core.instruments import InstrumentRegistry
from futrisk.core.lots import direction_sign, pos_direction
from futrisk.core.margin import MarginEntry, frozen_margin, intraday_margin
from futrisk.messages.enums import CashType, Direction, Offset, PosDirection
from futrisk.messages.eod import SodAccount, SodLot

D = Decimal
T2 = "20260923"


@pytest.fixture(scope="module")
def reg() -> InstrumentRegistry:
    return make_registry(
        {
            "rb": product("SHFE", multiplier=10, tick="1", fees=[rate_fee("0.0001")], margins=[margin("0.07")]),
            "j": product("DCE", multiplier=100, tick="0.5", fees=[lot_fee("1.5", intraday="3.0")], margins=[margin("0.12")]),
        },
        [inst("rb2701", "rb", "20270115"), inst("j2701", "j", "20270115")],
    )


def case1_t2_sod() -> SodAccount:
    """Golden case 1 after T1: 7 lots @3100 and 5 lots @3110, basis 3120."""
    return SodAccount(
        account_id="ACC1",
        pre_balance="202744.06",
        initial_capital="200000.00",
        hwm_equity="202744.06",
        lots=[
            SodLot(instrument_id="rb2701", direction=PosDirection.LONG, lot_no=1, open_td="20260922", open_ts_ns=1,
                   open_price="3100", qty=7, basis_price="3120"),
            SodLot(instrument_id="rb2701", direction=PosDirection.LONG, lot_no=2, open_td="20260922", open_ts_ns=2,
                   open_price="3110", qty=5, basis_price="3120"),
        ],
    )


class TestLedger:
    def test_sod_only(self, reg: InstrumentRegistry) -> None:
        led = IntradayLedger(reg, T2, case1_t2_sod(), {"rb2701": "3120"})
        f = led.funds()
        assert f.pre_balance == f.static_equity == f.equity == pytest.approx(202744.06)
        assert f.position_pnl == 0.0
        assert f.margin == pytest.approx(3120 * 10 * 12 * 0.07)
        assert f.cum_net_deposit == 0.0 and f.initial_capital == 200000.0

    def test_golden_case1_t2_at_settlement_marks(self, reg: InstrumentRegistry) -> None:
        tf = TradeFactory()
        led = IntradayLedger(reg, T2, case1_t2_sod(), {"rb2701": "3120"})
        r1 = led.apply_fill(tf("ACC1", "rb2701", B, O, 3115.0, 4, T2, "2026-09-22 21:10"))
        r2 = led.apply_fill(tf("ACC1", "rb2701", S, C, 3140.0, 6, T2, "2026-09-23 09:30"))
        r3 = led.apply_fill(tf("ACC1", "rb2701", S, CT, 3135.0, 2, T2, "2026-09-23 10:00"))
        assert [r.fee_est for r in (r1, r2, r3)] == [D("12.46"), D("18.84"), D("6.27")]
        assert (r2.close_today_qty, r2.close_yd_qty, r3.close_today_qty) == (0, 6, 2)
        assert led.close_pnl == D("1600") and led.fee == D("37.57")
        led.set_mark("rb2701", 3132.0)
        f = led.funds()
        assert f.position_pnl == pytest.approx(1060.0)
        assert f.equity == pytest.approx(205366.49)
        assert f.margin == pytest.approx(17539.20)
        assert f.available == pytest.approx(187827.29)
        assert f.risk_degree == pytest.approx(0.08540439, abs=1e-8)

    def test_position_views(self, reg: InstrumentRegistry) -> None:
        tf = TradeFactory()
        led = IntradayLedger(reg, T2, case1_t2_sod(), {"rb2701": "3120"})
        led.apply_fill(tf("ACC1", "rb2701", B, O, 3115.0, 4, T2, "2026-09-22 21:10"))
        led.book.position("ACC1", "rb2701", PosDirection.LONG).freeze_close(Offset.CLOSE_TODAY, 1)
        led.set_mark("rb2701", 3130.0)
        [v] = led.position_views(limit_locked=["rb2701"])
        assert (v.td_qty, v.yd_qty, v.closable_qty, v.closable_td_qty, v.closable_yd_qty) == (4, 12, 15, 3, 12)
        assert v.avg_open_price == pytest.approx((3100 * 7 + 3110 * 5 + 3115 * 4) / 16)
        assert v.basis_avg_price == pytest.approx((3120 * 12 + 3115 * 4) / 16)
        assert v.position_pnl == pytest.approx((3130 - 3120) * 120 + (3130 - 3115) * 40)
        assert v.float_pnl_fifo == pytest.approx((3130 - 3100) * 70 + (3130 - 3110) * 50 + (3130 - 3115) * 40)
        assert v.notional == pytest.approx(3130 * 10 * 16) and v.limit_locked
        assert v.margin == pytest.approx(3130 * 10 * 16 * 0.07)

    def test_mark_fallback(self, reg: InstrumentRegistry) -> None:
        tf = TradeFactory()
        led = IntradayLedger(reg, T2, seed_sod(T2, {"ACC1": "100000"}).accounts[0])
        with pytest.raises(KeyError):
            led.mark("j2701")
        led.apply_fill(tf("ACC1", "j2701", S, O, 2000.0, 2, T2, "2026-09-23 09:00"))
        assert led.mark("j2701") == 2000.0  # 首个行情之前按持仓成本估值
        led.set_mark("j2701", 1990.0)
        assert led.position_pnl() == pytest.approx(2000.0)  # 空头 (2000 − 1990) × 100 × 2

    def test_mark_before_first_tick_uses_lot_basis_without_pre_settle(self, reg: InstrumentRegistry) -> None:
        led = IntradayLedger(reg, T2, case1_t2_sod())  # 没有昨结算映射
        assert led.mark("rb2701") == 3120.0  # 昨仓的基准价
        assert led.funds().position_pnl == 0.0

    def test_dce_intraday_fee_estimates(self, reg: InstrumentRegistry) -> None:
        tf = TradeFactory()
        led = IntradayLedger(reg, T2, seed_sod(T2, {"ACC1": "100000"}).accounts[0])
        o = led.apply_fill(tf("ACC1", "j2701", B, O, 2000.0, 5, T2, "2026-09-23 09:00"))
        c = led.apply_fill(tf("ACC1", "j2701", S, C, 2001.0, 3, T2, "2026-09-23 09:01"))
        assert o.fee_est == D("7.50")  # 先按开仓 1.5/手
        assert c.fee_est == D("13.50")  # 3 × 3.0 + 开仓腿补差 3 × 1.5
        assert led.fee == D("21.00")

    def test_cash_frozen_and_wrong_account(self, reg: InstrumentRegistry) -> None:
        tf = TradeFactory()
        led = IntradayLedger(reg, T2, seed_sod(T2, {"ACC1": "100000"}).accounts[0])
        assert led.apply_cash(cash("ACC1", T2, CashType.DEPOSIT, "5000.00", "c1"))
        assert led.apply_cash(cash("ACC1", T2, CashType.WITHDRAW, "1000.00", "c2"))
        assert not led.apply_cash(cash("ACC1", T2, CashType.WITHDRAW, "1000.00", "c2"))  # 重复 movement_id 幂等
        led.set_frozen(margin=500.0, fee=5.0)
        f = led.funds()
        assert (f.net_deposit, f.static_equity, f.cum_net_deposit) == (4000.0, 104000.0, 4000.0)
        assert f.available == pytest.approx(104000.0 - 505.0)
        with pytest.raises(ValueError):
            led.apply_cash(cash("ACC2", T2, CashType.DEPOSIT, "1.00", "c3"))
        with pytest.raises(ValueError, match="applied to ledger of 20260923"):
            led.apply_cash(cash("ACC1", "20260924", CashType.DEPOSIT, "1.00", "c4"))
        with pytest.raises(ValueError):
            led.apply_fill(tf("ACC2", "rb2701", B, O, 3000.0, 1, T2, "2026-09-23 09:00"))
        with pytest.raises(PositionError):
            led.apply_fill(tf("ACC1", "rb2701", S, C, 3000.0, 1, T2, "2026-09-23 09:00"))

    def test_fills_of_other_days_and_duplicates(self, reg: InstrumentRegistry) -> None:
        tf = TradeFactory()
        led = IntradayLedger(reg, T2, seed_sod(T2, {"ACC1": "100000"}).accounts[0])
        with pytest.raises(ValueError, match="applied to ledger of 20260923"):
            led.apply_fill(tf("ACC1", "rb2701", B, O, 3000.0, 1, "20260922", "2026-09-22 09:00"))
        t = tf("ACC1", "rb2701", B, O, 3000.0, 2, T2, "2026-09-23 09:00")
        first = led.apply_fill(t)
        again = led.apply_fill(t)
        assert again.duplicate and not first.duplicate and again.fill == first.fill
        assert led.held() == [("rb2701", PosDirection.LONG, 2)] and led.fee == D("6.00")

    def test_frozen_for_orders(self, reg: InstrumentRegistry) -> None:
        tf = TradeFactory()
        led = IntradayLedger(reg, T2, seed_sod(T2, {"ACC1": "1000000"}).accounts[0])
        led.apply_fill(tf("ACC1", "rb2701", B, O, 3000.0, 10, T2, "2026-09-23 09:00"))
        led.set_mark("rb2701", 3000.0)
        margin_small, fee_small = led.frozen_for_orders([RestingOrder("rb2701", Direction.SELL, Offset.OPEN, 5, 3000.0)])
        assert margin_small == 0.0 and fee_small == pytest.approx(15.0)  # 小边挂单不冻结保证金
        margin_flip, _ = led.frozen_for_orders([RestingOrder("rb2701", Direction.SELL, Offset.OPEN, 15, 3000.0)])
        assert margin_flip == pytest.approx(10500.0)
        m_close, fee_close = led.frozen_for_orders([RestingOrder("rb2701", Direction.SELL, Offset.CLOSE_TODAY, 2, 3010.0)])
        assert m_close == 0.0 and fee_close == pytest.approx(6.02)

    def test_frozen_fee_of_close_orders_follows_lots_to_be_consumed(self) -> None:
        reg = make_registry(
            {"rb": product("SHFE", multiplier=10, tick="1", fees=[rate_fee("0.0001", close_today_mult="3")], margins=[margin("0.07")])},
            [inst("rb2701", "rb", "20270115")],
        )
        tf = TradeFactory()
        led = IntradayLedger(reg, T2, case1_t2_sod(), {"rb2701": "3120"})
        led.apply_fill(tf("ACC1", "rb2701", B, O, 3115.0, 4, T2, "2026-09-22 21:10"))
        _, fee_yd = led.frozen_for_orders([RestingOrder("rb2701", Direction.SELL, Offset.CLOSE_YESTERDAY, 2, 3000.0)])
        _, fee_td = led.frozen_for_orders([RestingOrder("rb2701", Direction.SELL, Offset.CLOSE_TODAY, 2, 3000.0)])
        assert fee_yd == pytest.approx(6.0)  # 平昨费率
        assert fee_td == pytest.approx(18.0)  # 平今 = 平昨 × 3

    def test_negative_equity_risk_cap(self, reg: InstrumentRegistry) -> None:
        tf = TradeFactory()
        led = IntradayLedger(reg, T2, seed_sod(T2, {"ACC1": "1000"}).accounts[0])
        led.apply_fill(tf("ACC1", "j2701", B, O, 2000.0, 1, T2, "2026-09-23 09:00"))
        led.set_mark("j2701", 1900.0)
        f = led.funds()
        assert f.equity < 0 and f.risk_degree == 9999.99999999


class TestOrderCost:
    def test_big_side_aware_cost_and_registration(self, reg: InstrumentRegistry) -> None:
        tf = TradeFactory()
        led = IntradayLedger(reg, T2, seed_sod(T2, {"ACC1": "1000000"}).accounts[0])
        led.apply_fill(tf("ACC1", "rb2701", B, O, 3000.0, 10, T2, "2026-09-23 09:00"))
        led.set_mark("rb2701", 3000.0)
        small = RestingOrder("rb2701", Direction.SELL, Offset.OPEN, 5, 3000.0)
        assert led.order_cost(small) == (0.0, pytest.approx(15.0))
        # 登记 5 手小边挂单后，再挂 10 手空单会让空头超过多头 5 手 × 3000 × 10 × 7%
        led.add_order("o1", small)
        m, fee = led.order_cost(RestingOrder("rb2701", Direction.SELL, Offset.OPEN, 10, 3000.0))
        assert m == pytest.approx(10500.0) and fee == pytest.approx(30.0)
        f = led.funds()
        assert (f.frozen_margin, f.frozen_fee) == (0.0, pytest.approx(15.0))
        led.update_order("o1", 15)
        assert led.funds().frozen_margin == pytest.approx(10500.0)
        led.set_mark("rb2701", 3100.0)  # 多头保证金随行情上涨，挂单冻结随之减少
        assert led.funds().frozen_margin == pytest.approx(15 * 3000 * 10 * 0.07 - 10 * 3100 * 10 * 0.07)
        assert led.remove_order("o1") and not led.remove_order("o1")
        f = led.funds()
        assert (f.frozen_margin, f.frozen_fee) == (0.0, 0.0)

    def test_gross_and_close_orders(self, reg: InstrumentRegistry) -> None:
        tf = TradeFactory()
        led = IntradayLedger(reg, T2, seed_sod(T2, {"ACC1": "1000000"}).accounts[0])
        led.apply_fill(tf("ACC1", "j2701", B, O, 2000.0, 5, T2, "2026-09-23 09:00"))
        m, fee = led.order_cost(RestingOrder("j2701", Direction.SELL, Offset.OPEN, 1, 2000.0))
        assert m == pytest.approx(2000 * 100 * 0.12) and fee == pytest.approx(1.5)  # DCE 不收单向大边
        m, fee = led.order_cost(RestingOrder("j2701", Direction.SELL, Offset.CLOSE, 2, 2000.0))
        assert m == 0.0 and fee == pytest.approx(6.0)  # 将平今仓：日内 3.0/手
        led.add_order("c1", RestingOrder("j2701", Direction.SELL, Offset.CLOSE, 2, 2000.0))
        assert led.funds().frozen_fee == pytest.approx(6.0) and led.orders_frozen_fee == pytest.approx(6.0)
        assert led.order_cost(RestingOrder("j2701", Direction.BUY, Offset.OPEN, 0, 2000.0)) == (0.0, 0.0)

    def test_refresh_rates_after_reload(self, reg: InstrumentRegistry) -> None:
        tf = TradeFactory()
        led = IntradayLedger(reg, T2, seed_sod(T2, {"ACC1": "1000000"}).accounts[0])
        led.apply_fill(tf("ACC1", "rb2701", B, O, 3000.0, 10, T2, "2026-09-23 09:00"))
        led.set_mark("rb2701", 3000.0)
        raised = make_registry(
            {"rb": product("SHFE", multiplier=10, tick="1", fees=[rate_fee("0.0001")], margins=[margin("0.10")]),
             "j": product("DCE", multiplier=100, tick="0.5", fees=[lot_fee("1.5", intraday="3.0")], margins=[margin("0.12")])},
            [inst("rb2701", "rb", "20270115"), inst("j2701", "j", "20270115")],
        )
        led.refresh_rates(raised)
        assert led.funds().margin == pytest.approx(3000 * 10 * 10 * 0.10)


# ---------------------------------------------------------------------------
# Equivalence: incremental hot path == straightforward recomputation
# ---------------------------------------------------------------------------

EQ_INSTRUMENTS = ["rb2610", "rb2701", "IF2612", "IH2612", "j2701"]
EQ_BASE = {"rb2610": 3050.0, "rb2701": 3110.0, "IF2612": 4400.0, "IH2612": 2850.0, "j2701": 2000.0}
EQ_TICK = {"rb2610": 1.0, "rb2701": 1.0, "IF2612": 0.2, "IH2612": 0.2, "j2701": 0.5}


def eq_registry() -> InstrumentRegistry:
    def index() -> dict[str, object]:
        return product("CFFEX", multiplier=300, tick="0.2", limit_pct="0.10", fees=[rate_fee("0.000023", close_today_mult="10")],
                       margins=[margin("0.12")], settle_method="VWAP_LAST_HOUR", big_side_group="CFFEX:INDEX")

    return make_registry(
        {
            "rb": product("SHFE", multiplier=10, tick="1", fees=[rate_fee("0.0001", close_today_mult="2")],
                          margins=[margin("0.07", broker_add="0.01"), margin("0.10", contracts=["rb2610"], broker_add="0.01")]),
            "IF": index(),
            "IH": index(),
            "j": product("DCE", multiplier=100, tick="0.5", fees=[lot_fee("1.5", intraday="3.0")], margins=[margin("0.12")]),
        },
        [
            inst("rb2610", "rb", "20261015"),
            inst("rb2701", "rb", "20270115"),
            inst("IF2612", "IF", "20261218"),
            inst("IH2612", "IH", "20261218"),
            inst("j2701", "j", "20270115"),
        ],
    )


def eq_sod(td: str) -> SodAccount:
    def lot(i: str, d: PosDirection, qty: int) -> SodLot:
        px = str(EQ_BASE[i])
        return SodLot(instrument_id=i, direction=d, lot_no=1, open_td="20260901", open_ts_ns=1, open_price=px, qty=qty, basis_price=px)

    return SodAccount(
        account_id="ACC1", pre_balance="5000000.00", initial_capital="5000000.00", hwm_equity="5000000.00",
        lots=[lot("rb2701", PosDirection.LONG, 3), lot("rb2610", PosDirection.SHORT, 2), lot("IF2612", PosDirection.LONG, 1),
              lot("j2701", PosDirection.SHORT, 2)],
    )


op = st.tuples(st.integers(0, 4), st.integers(0, 4), st.integers(0, 1), st.integers(1, 3), st.integers(-20, 20), st.integers(0, 1))


class _Ref:
    """Straightforward recomputation of the ledger's hot-path numbers from the book and the margin functions."""

    def __init__(self, led: IntradayLedger, reg: InstrumentRegistry, td: str) -> None:
        self.led, self.reg, self.td = led, reg, td
        self.marks: dict[str, float] = {}
        self.orders: dict[str, RestingOrder] = {}
        self.order_fees: dict[str, float] = {}

    def held(self) -> list[tuple[str, PosDirection, int]]:
        return [(p.instrument_id, p.direction, p.qty) for p in self.led.book.positions("ACC1") if p.qty > 0]

    def fee(self, o: RestingOrder) -> float:
        key = ("ACC1", o.instrument_id, pos_direction(o.direction, o.offset))
        if o.offset == Offset.OPEN:
            return float(self.led.fees.estimate_order(key, is_open=True, qty=o.qty, price=D(str(o.price))))
        pos = self.led.book.find(*key)
        today = pos.preview_close(o.offset, o.qty)[0] if pos is not None else 0
        return float(self.led.fees.estimate_order(key, is_open=False, qty=o.qty, price=D(str(o.price)), today_lot_qty=today))

    def opens(self, extra: RestingOrder | None = None) -> list[MarginEntry]:
        orders = [*self.orders.values(), *([extra] if extra else [])]
        return [MarginEntry(o.instrument_id, pos_direction(o.direction, o.offset), o.qty, o.price)
                for o in orders if o.offset == Offset.OPEN and o.qty > 0]

    def check(self) -> None:
        held = self.held()
        mk = {i: self.marks[i] for i, _, _ in held}
        pnl = sum(direction_sign(d) * self.reg.multiplier(i) * (mk[i] * q - float(self.led.book.find("ACC1", i, d).basis_notional))
                  for i, d, q in held)
        margin_ref = intraday_margin(held, mk, self.reg, self.td)
        frozen_ref = frozen_margin(held, mk, self.opens(), self.reg, self.td)
        f = self.led.funds()
        approx = pytest.approx
        assert f.position_pnl == approx(pnl, rel=1e-12, abs=1e-6)
        assert f.margin == approx(margin_ref.total, rel=1e-12, abs=1e-6)
        assert f.frozen_margin == approx(frozen_ref, rel=1e-12, abs=1e-6)
        assert f.frozen_fee == approx(sum(self.order_fees.values()), abs=1e-9)
        assert f.equity == approx(float(self.led.pre_balance) + float(self.led.close_pnl) + pnl - float(self.led.fee), rel=1e-12)
        assert self.led.margin().total == approx(margin_ref.total, rel=1e-12, abs=1e-6)
        gross = margin_ref.gross_by_position()
        for v in self.led.position_views():
            assert v.margin == approx(gross[(v.instrument_id, v.direction)], rel=1e-12, abs=1e-6)
            basis = float(self.led.book.find("ACC1", v.instrument_id, v.direction).basis_notional)
            q = v.td_qty + v.yd_qty
            expect = direction_sign(v.direction) * self.reg.multiplier(v.instrument_id) * (mk[v.instrument_id] * q - basis)
            assert v.position_pnl == approx(expect, rel=1e-12, abs=1e-6)

    def check_cost(self, o: RestingOrder) -> None:
        held = self.held()
        mk = {i: self.marks[i] for i, _, _ in held}
        m, fee = self.led.order_cost(o)
        if o.offset == Offset.OPEN:
            expect = frozen_margin(held, mk, self.opens(o), self.reg, self.td) - frozen_margin(held, mk, self.opens(), self.reg, self.td)
        else:
            expect = 0.0
        assert m == pytest.approx(expect, rel=1e-12, abs=1e-6)
        assert fee == pytest.approx(self.fee(o), abs=1e-9)


@pytest.mark.parametrize("td", ["20260930", "20261009"])  # 20261009：rb2610 已退出单向大边
@settings(max_examples=60, deadline=None, derandomize=True, database=None, suppress_health_check=[HealthCheck.too_slow])
@given(ops=st.lists(op, min_size=8, max_size=30))
def test_incremental_equals_straightforward(td: str, ops: list[tuple[int, int, int, int, int, int]]) -> None:
    reg = eq_registry()
    led = IntradayLedger(reg, td, eq_sod(td), {i: str(p) for i, p in EQ_BASE.items()})
    ref = _Ref(led, reg, td)
    for i, p in EQ_BASE.items():
        led.set_mark(i, p)
        ref.marks[i] = p
    tf = TradeFactory()
    day = f"{td[:4]}-{td[4:6]}-{td[6:]}"
    for n, (kind, a, b, qty, off, flag) in enumerate(ops):
        inst_id = EQ_INSTRUMENTS[a]
        px = EQ_BASE[inst_id] + off * EQ_TICK[inst_id]
        when = f"{day} {9 + n // 30:02d}:{(n * 2) % 60:02d}:00"
        if kind == 0:  # 开仓
            led.apply_fill(tf("ACC1", inst_id, B if b else S, O, px, qty, td, when))
        elif kind == 1:  # 平掉一个已有持仓
            held = ref.held()
            if not held:
                continue
            h_inst, h_dir, _ = held[(a + b) % len(held)]
            pos = led.book.find("ACC1", h_inst, h_dir)
            assert pos is not None
            side = S if h_dir == PosDirection.LONG else B
            if h_inst.startswith("rb"):
                offset, avail = (CT, pos.td_qty) if flag and pos.td_qty else (CY, pos.yd_qty)
                if avail == 0:
                    offset, avail = CT, pos.td_qty
            else:
                offset, avail = C, pos.qty
            if avail == 0:
                continue
            led.apply_fill(tf("ACC1", h_inst, side, offset, EQ_BASE[h_inst] + off * EQ_TICK[h_inst], min(qty, avail), td, when))
        elif kind == 2:  # 行情
            led.set_mark(inst_id, px)
            ref.marks[inst_id] = px
        elif kind == 3:  # 登记挂单
            oid = f"o{b}{flag}"
            order = RestingOrder(inst_id, Direction.BUY if b else Direction.SELL, Offset.OPEN if flag or qty > 1 else Offset.CLOSE, qty, px)
            ref.order_fees[oid] = ref.fee(order)
            led.add_order(oid, order)
            ref.orders[oid] = order
        else:  # 撤单
            oid = f"o{b}{flag}"
            assert led.remove_order(oid) == (oid in ref.orders)
            ref.orders.pop(oid, None)
            ref.order_fees.pop(oid, None)
        ref.check()
        ref.check_cost(RestingOrder(inst_id, Direction.BUY if flag else Direction.SELL, Offset.OPEN, qty, px))
