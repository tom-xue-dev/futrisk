"""Unit tests for futrisk.core.fees: amounts, rounding and close-today classification (01 §8.15)."""

from __future__ import annotations

from decimal import Decimal

import pytest
from tests.golden.builders import inst, lot_fee, make_registry, margin, product, rate_fee

from futrisk.common.config.products import FeeSchedule
from futrisk.core.fees import (
    INTRADAY_BOTH_LEGS,
    LOT_MATCH,
    SAME_DAY_OPENS_FIRST,
    DayFeeBook,
    DayFeeClassifier,
    FeeLegs,
    FloatFeeRates,
    compute_fee,
    leg_amount,
    leg_rate,
    round_cents,
)
from futrisk.core.instruments import InstrumentRegistry
from futrisk.messages.enums import FeeType, PosDirection

D = Decimal
KEY_L = ("ACC1", "X", PosDirection.LONG)
KEY_S = ("ACC1", "X", PosDirection.SHORT)


def sched(**kw: object) -> FeeSchedule:
    return FeeSchedule.model_validate({"type": "rate", "open": "0.0001", "close_yd": "0.0001", **kw})


# ---------------------------------------------------------------------------
# Amounts
# ---------------------------------------------------------------------------


class TestAmounts:
    def test_rates_per_type(self) -> None:
        s = sched(close_today_mult="10", intraday="0.0003")
        assert leg_rate(s, FeeType.OPEN) == D("0.0001")
        assert leg_rate(s, FeeType.CLOSE_YD) == D("0.0001")
        assert leg_rate(s, FeeType.CLOSE_TODAY) == D("0.0010")
        assert leg_rate(s, FeeType.INTRADAY_OPEN) == leg_rate(s, FeeType.INTRADAY_CLOSE) == D("0.0003")

    def test_intraday_rate_required(self) -> None:
        with pytest.raises(ValueError):
            leg_rate(sched(), FeeType.INTRADAY_OPEN)

    def test_rate_and_per_lot_amounts(self) -> None:
        assert leg_amount(sched(), FeeType.OPEN, D("3100"), 10, 10) == D("31.0000")
        per_lot = FeeSchedule.model_validate({"type": "per_lot", "open": "3", "close_yd": "3", "close_today_mult": "0"})
        assert leg_amount(per_lot, FeeType.OPEN, D("101.2"), 4, 10000) == D("12")
        assert leg_amount(per_lot, FeeType.CLOSE_TODAY, D("101.2"), 4, 10000) == D("0")  # T 平今免收

    def test_round_half_up_per_trade(self) -> None:
        s = FeeSchedule.model_validate({"type": "rate", "open": "0.000023", "close_yd": "0.000023", "close_today_mult": "10"})
        # 4450 × 300 × 0.00023 = 307.05（精确）；4415 × 300 × 0.000023 = 30.4635 -> 30.46
        assert compute_fee(s, {FeeType.CLOSE_TODAY: 1}, D("4450"), 300).fee == D("307.05")
        assert compute_fee(s, {FeeType.OPEN: 1}, D("4415"), 300).fee == D("30.46")
        # 3105 × 10 × 5 × 0.0001 = 15.525 -> 15.53（half-up，而不是银行家舍入）
        assert compute_fee(sched(), {FeeType.OPEN: 5}, D("3105"), 10).fee == D("15.53")

    def test_mixed_legs_and_primary_type(self) -> None:
        s = FeeSchedule.model_validate({"type": "rate", "open": "0.000023", "close_yd": "0.000023", "close_today_mult": "10"})
        fee = compute_fee(s, {FeeType.CLOSE_YD: 2, FeeType.CLOSE_TODAY: 8}, D("4400"), 300)
        assert fee.fee == D("2489.52")
        assert fee.fee_type == FeeType.CLOSE_TODAY
        assert fee.fee_rate == D("0.000230") and fee.fee_per_lot is None
        assert [(leg.fee_type, leg.qty) for leg in fee.legs] == [(FeeType.CLOSE_TODAY, 8), (FeeType.CLOSE_YD, 2)]
        assert fee.qty_of(FeeType.CLOSE_YD) == 2

    def test_empty_legs_rejected(self) -> None:
        with pytest.raises(ValueError):
            compute_fee(sched(), {}, D("1"), 1)


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------


class TestClassifier:
    def test_lot_match_uses_consumed_today_lots(self) -> None:
        c = DayFeeClassifier()
        c.open(KEY_L, 5, D("1"), LOT_MATCH)
        legs, up = c.close(KEY_L, 4, D("1"), LOT_MATCH, today_lot_qty=1)
        assert legs.as_dict() == {FeeType.CLOSE_TODAY: 1, FeeType.CLOSE_YD: 3} and up == []

    def test_same_day_opens_first_faq(self) -> None:
        # 持有 20 手昨仓：先卖 10 后买 8 -> 0 手平今
        c = DayFeeClassifier()
        legs, _ = c.close(KEY_L, 10, D("1"), SAME_DAY_OPENS_FIRST)
        c.open(KEY_L, 8, D("1"), SAME_DAY_OPENS_FIRST)
        assert legs.as_dict() == {FeeType.CLOSE_YD: 10}
        # 先买 8 后卖 10 -> 8 手平今 + 2 手平昨
        c = DayFeeClassifier()
        c.open(KEY_L, 8, D("1"), SAME_DAY_OPENS_FIRST)
        legs, _ = c.close(KEY_L, 10, D("1"), SAME_DAY_OPENS_FIRST)
        assert legs.as_dict() == {FeeType.CLOSE_TODAY: 8, FeeType.CLOSE_YD: 2}

    def test_sides_and_accounts_do_not_match_each_other(self) -> None:
        c = DayFeeClassifier()
        c.open(KEY_S, 5, D("1"), SAME_DAY_OPENS_FIRST)
        c.open(("ACC2", "X", PosDirection.LONG), 5, D("1"), SAME_DAY_OPENS_FIRST)
        legs, _ = c.close(KEY_L, 3, D("1"), SAME_DAY_OPENS_FIRST)
        assert legs.as_dict() == {FeeType.CLOSE_YD: 3}
        assert c.unmatched_opens(KEY_S) == 5

    def test_intraday_both_legs_example(self) -> None:
        # 2 手昨仓（FIFO 先平）：开 5 / 平 3（消耗 2 昨 + 1 今）/ 开 2 / 平 6（消耗 6 今）
        c = DayFeeClassifier()
        o1 = c.open(KEY_L, 5, D("1"), INTRADAY_BOTH_LEGS)
        c1, up1 = c.close(KEY_L, 3, D("1"), INTRADAY_BOTH_LEGS, today_lot_qty=1, intraday=True)
        o2 = c.open(KEY_L, 2, D("1"), INTRADAY_BOTH_LEGS)
        c2, up2 = c.close(KEY_L, 6, D("1"), INTRADAY_BOTH_LEGS, today_lot_qty=6, intraday=True)
        assert o1.as_dict() == {FeeType.INTRADAY_OPEN: 5}
        assert o2.as_dict() == {FeeType.INTRADAY_OPEN: 2}
        assert c1.as_dict() == {FeeType.INTRADAY_CLOSE: 1, FeeType.CLOSE_YD: 2}
        assert c2.as_dict() == {FeeType.INTRADAY_CLOSE: 6}
        assert [(legs is o1, n) for legs, n in up1] == [(True, 1)]
        assert [n for _, n in up2] == [4, 2]
        total_intraday = sum(x.get(FeeType.INTRADAY_OPEN) + x.get(FeeType.INTRADAY_CLOSE) for x in (o1, c1, o2, c2))
        assert total_intraday == 14

    def test_intraday_follows_the_lots_consumed(self) -> None:
        # 买开 5、卖平 3 且先平掉 2 手昨仓：只有 1 手今仓被平
        c = DayFeeClassifier()
        o = c.open(KEY_L, 5, D("1"), INTRADAY_BOTH_LEGS)
        legs, up = c.close(KEY_L, 3, D("1"), INTRADAY_BOTH_LEGS, today_lot_qty=1, intraday=True)
        assert o.as_dict() == {FeeType.INTRADAY_OPEN: 1, FeeType.OPEN: 4}
        assert legs.as_dict() == {FeeType.INTRADAY_CLOSE: 1, FeeType.CLOSE_YD: 2}
        assert c.unmatched_opens(KEY_L) == 4

    def test_intraday_rule_without_rate_charges_close_today(self) -> None:
        c = DayFeeClassifier()
        o = c.open(KEY_L, 2, D("1"), INTRADAY_BOTH_LEGS)
        legs, up = c.close(KEY_L, 3, D("1"), INTRADAY_BOTH_LEGS, today_lot_qty=2, intraday=False)
        assert o.as_dict() == {FeeType.OPEN: 2} and up == []
        assert legs.as_dict() == {FeeType.CLOSE_TODAY: 2, FeeType.CLOSE_YD: 1}

    def test_preview_does_not_change_state(self) -> None:
        c = DayFeeClassifier()
        c.open(KEY_L, 2, D("1"), SAME_DAY_OPENS_FIRST)
        assert c.preview_close(KEY_L, 3, SAME_DAY_OPENS_FIRST) == {FeeType.CLOSE_TODAY: 2, FeeType.CLOSE_YD: 1}
        assert c.preview_close(KEY_L, 3, INTRADAY_BOTH_LEGS, today_lot_qty=2, intraday=True) == {
            FeeType.INTRADAY_CLOSE: 2,
            FeeType.CLOSE_YD: 1,
        }
        assert c.preview_close(KEY_L, 3, INTRADAY_BOTH_LEGS, today_lot_qty=1, intraday=True) == {
            FeeType.INTRADAY_CLOSE: 1,
            FeeType.CLOSE_YD: 2,
        }
        assert c.preview_close(KEY_L, 3, LOT_MATCH, today_lot_qty=1) == {FeeType.CLOSE_TODAY: 1, FeeType.CLOSE_YD: 2}
        assert c.unmatched_opens(KEY_L) == 2

    def test_fee_legs_move_guard(self) -> None:
        legs = FeeLegs(3, D("1"), is_open=True)
        legs.add(FeeType.OPEN, 3)
        with pytest.raises(ValueError):
            legs.move(FeeType.OPEN, FeeType.INTRADAY_OPEN, 4)
        legs.move(FeeType.OPEN, FeeType.INTRADAY_OPEN, 3)
        assert list(legs) == [(FeeType.INTRADAY_OPEN, 3)]


# ---------------------------------------------------------------------------
# DayFeeBook: intraday estimates vs exact EOD fees
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def reg() -> InstrumentRegistry:
    return make_registry(
        {
            "j": product("DCE", multiplier=100, tick="0.5", fees=[rate_fee("0.0001", intraday="0.00014")], margins=[margin("0.12")]),
            "m": product("DCE", multiplier=10, tick="1", fees=[lot_fee("1.5", intraday="3.0")], margins=[margin("0.07")]),
            "IF": product("CFFEX", multiplier=300, tick="0.2", fees=[rate_fee("0.000023", close_today_mult="10")],
                          margins=[margin("0.12")], physical_delivery=False, big_side_group="CFFEX:INDEX"),
        },
        [inst("j2701", "j", "20270115"), inst("m2701", "m", "20270115"), inst("IF2612", "IF", "20261218")],
    )


class TestDayFeeBook:
    def test_rate_intraday_estimate_close_to_exact(self, reg: InstrumentRegistry) -> None:
        book = DayFeeBook(reg, "20260923")
        key = ("ACC1", "j2701", PosDirection.LONG)
        o = book.on_fill(key, is_open=True, qty=3, price=D("2000.5"))
        assert o.estimate == D("60.02")  # 2000.5 × 100 × 3 × 0.0001 = 60.015 -> 60.02（按开仓费率预估）
        c = book.on_fill(key, is_open=False, qty=2, price=D("2010"), today_lot_qty=2)
        # 平仓腿按日内费率 + 开仓腿补差 (0.00014 − 0.0001) × 2000.5 × 100 × 2
        assert c.estimate == D("56.28") + D("16.00")
        exact = book.final_fee("j2701", o.legs).fee + book.final_fee("j2701", c.legs).fee
        assert abs((o.estimate + c.estimate) - exact) <= D("0.02")
        assert book.final_fee("j2701", o.legs).fee == D("76.02")  # 2 手日内 56.014 + 1 手普通开仓 20.005

    def test_per_lot_intraday_estimate_exact(self, reg: InstrumentRegistry) -> None:
        book = DayFeeBook(reg, "20260923")
        key = ("ACC1", "m2701", PosDirection.SHORT)
        fills = [(True, 5), (False, 3), (True, 2), (False, 4)]
        est = D(0)
        legs = []
        for is_open, qty in fills:
            f = book.on_fill(key, is_open=is_open, qty=qty, price=D("3000"), today_lot_qty=0 if is_open else qty)
            est += f.estimate
            legs.append(f.legs)
        exact = sum((book.final_fee("m2701", x).fee for x in legs), D(0))
        assert est == exact == D("42.00")  # 14 手日内 × 3.0

    def test_estimate_order(self, reg: InstrumentRegistry) -> None:
        book = DayFeeBook(reg, "20260923")
        key = ("ACC1", "IF2612", PosDirection.LONG)
        assert book.estimate_order(key, is_open=True, qty=1, price=D("4400")) == D("30.36")
        book.on_fill(key, is_open=True, qty=1, price=D("4400"))
        # 已有当日开仓：再平 2 手 = 1 手平今 + 1 手平昨
        assert book.estimate_order(key, is_open=False, qty=2, price=D("4400")) == D("333.96")
        assert book.classifier.unmatched_opens(key) == 1


# ---------------------------------------------------------------------------
# Float fee model (hot path)
# ---------------------------------------------------------------------------


class TestFloatFeeRates:
    def test_round_cents_matches_decimal_half_up(self) -> None:
        assert round_cents(15.525) == 15.53
        assert round_cents(0.005) == 0.01
        assert round_cents(30.4635) == 30.46
        assert round_cents(307.05) == 307.05

    @pytest.mark.parametrize(
        ("legs", "price", "mult"),
        [
            ({FeeType.OPEN: 5}, "3105", 10),
            ({FeeType.CLOSE_TODAY: 8, FeeType.CLOSE_YD: 2}, "4400", 300),
            ({FeeType.INTRADAY_OPEN: 1, FeeType.OPEN: 4}, "2000.5", 100),
            ({FeeType.CLOSE_YD: 3}, "101.235", 10000),
        ],
    )
    def test_float_fee_equals_decimal_fee(self, legs: dict[FeeType, int], price: str, mult: int) -> None:
        s = FeeSchedule.model_validate(
            {"type": "rate", "open": "0.000023", "close_yd": "0.000023", "close_today_mult": "10", "intraday": "0.00014"}
        )
        exact = compute_fee(s, legs, D(price), mult).fee
        assert FloatFeeRates.from_schedule(s, mult).fee(legs.items(), float(price)) == float(exact)

    def test_per_lot(self) -> None:
        s = FeeSchedule.model_validate({"type": "per_lot", "open": "3", "close_yd": "3", "close_today_mult": "0"})
        rates = FloatFeeRates.from_schedule(s, 10000)
        assert rates.open_fee(101.2, 4) == 12.0
        assert rates.fee([(FeeType.CLOSE_TODAY, 4)], 101.2) == 0.0
        with pytest.raises(ValueError):
            rates.rate(FeeType.INTRADAY_OPEN)
