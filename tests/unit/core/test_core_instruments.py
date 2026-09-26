"""Unit tests for futrisk.core.instruments: schedules, limits, groups, big-side exit (01 §8.15)."""

from __future__ import annotations

from datetime import time
from decimal import Decimal

import pytest
from tests.golden.builders import (
    inst,
    lot_fee,
    make_calendar,
    make_products,
    make_registry,
    margin,
    product,
    rate_fee,
)

from futrisk.common.clock import ns_to_bj
from futrisk.common.config import FullConfig
from futrisk.core.instruments import (
    InstrumentRegistry,
    MarginMethod,
    UnknownInstrumentError,
    contract_month,
    delivery_month_of,
    parse_window,
)
from futrisk.messages.enums import Exchange, Rounding, SettleMethod

D = Decimal


@pytest.fixture(scope="module")
def reg() -> InstrumentRegistry:
    return make_registry(
        {
            "rb": product(
                "SHFE",
                multiplier=10,
                tick="1",
                fees=[
                    rate_fee("0.0001", months=[1, 5, 10], effective_from="20260101"),
                    rate_fee("0.00002", months="other", effective_from="20260101"),
                    rate_fee("0.0003", contracts=["rb2610"], effective_from="20260901"),
                    rate_fee("0.0002", months=[1, 5, 10], effective_from="20261001"),
                ],
                margins=[
                    margin("0.07", broker_add="0.01", effective_from="20260101"),
                    margin("0.10", contracts=["rb2610"], broker_add="0.01", effective_from="20260901"),
                    margin("0.09", broker_add="0.01", effective_from="20261010"),
                    margin("0.12", broker_add="0.01", effective_from="20261020"),  # 节前统一上调
                ],
            ),
            "T": product(
                "CFFEX",
                multiplier=10000,
                tick="0.005",
                limit_pct="0.02",
                fees=[lot_fee("3", close_today_mult="0")],
                margins=[margin("0.02")],
                settle_method="VWAP_LAST_HOUR",
                settle_window="14:15-15:15",
                big_side_group="CFFEX:TREASURY",
                physical_delivery=True,
            ),
            "IF": product(
                "CFFEX",
                multiplier=300,
                tick="0.2",
                limit_pct="0.10",
                fees=[rate_fee("0.000023", close_today_mult="10")],
                margins=[margin("0.12")],
                settle_method="VWAP_LAST_HOUR",
                big_side_group="CFFEX:INDEX",
                physical_delivery=False,
            ),
            "m": product("DCE", multiplier=10, tick="1", fees=[lot_fee("1.5")], margins=[margin("0.07")]),
        },
        [
            inst("rb2610", "rb", "20261015", "202610"),
            inst("rb2701", "rb", "20270115", "202701"),
            inst("rb2611", "rb", "20261116", "202611"),
            inst("T2612", "T", "20261211", "202612"),
            inst("IF2612", "IF", "20261218", "202612"),
            inst("m2701", "m", "20270115", "202701"),
        ],
    )


# ---------------------------------------------------------------------------
# Basics
# ---------------------------------------------------------------------------


class TestBasics:
    def test_accessors(self, reg: InstrumentRegistry) -> None:
        info = reg.get("rb2701")
        assert (info.product, info.exchange, info.multiplier, info.tick_size) == ("rb", Exchange.SHFE, 10, D("1"))
        assert info.listed and info.contract_month == 1 and info.delivery_month == "202701"
        assert reg.close_style("rb2701") == "FIFO"
        assert reg.explicit_close_today("rb2701") and not reg.explicit_close_today("IF2612")
        assert reg.close_today_fee_rule("m2701") == "INTRADAY_BOTH_LEGS"
        assert reg.instrument_ids()[:2] == ["rb2610", "rb2701"]

    def test_unknown_instrument(self, reg: InstrumentRegistry) -> None:
        with pytest.raises(UnknownInstrumentError):
            reg.get("zz2701")
        assert not reg.has("zz2701")
        strict = InstrumentRegistry(reg.products, make_calendar(), allow_unlisted=False)
        with pytest.raises(UnknownInstrumentError):
            strict.get("rb2705")

    def test_unlisted_contract_is_synthesized_with_estimated_last_day(self, reg: InstrumentRegistry) -> None:
        info = reg.get("rb2705")
        assert not info.listed and not reg.is_listed("rb2705") and reg.has("rb2705")
        assert info.delivery_month == "202705"
        # SHFE：合约月份 15 日；2027-05-15 为周六，顺延到周一
        assert info.last_trade_day == "20270517"
        # CFFEX 股指第三个周五、国债第二个周五
        assert reg.get("IF2703").last_trade_day == "20270319"
        assert reg.get("T2703").last_trade_day == "20270312"
        # DCE：交割月第 10 个交易日
        assert reg.get("m2703").last_trade_day == "20270312"

    def test_code_helpers(self) -> None:
        assert contract_month("IF2612") == 12 and contract_month("rb2701") == 1
        assert delivery_month_of("rb2701") == "202701"
        assert delivery_month_of("SR701") is None
        assert parse_window("14:15-15:15") == (time(14, 15), time(15, 15))
        with pytest.raises(ValueError):
            parse_window("14:15")


# ---------------------------------------------------------------------------
# Fee and margin schedules
# ---------------------------------------------------------------------------


class TestSchedules:
    def test_fee_precedence_contracts_over_months_over_other(self, reg: InstrumentRegistry) -> None:
        assert reg.fee_schedule("rb2701", "20260922").open == D("0.0001")  # months [1,5,10]
        assert reg.fee_schedule("rb2611", "20260922").open == D("0.00002")  # other
        assert reg.fee_schedule("rb2610", "20260922").open == D("0.0003")  # explicit contract list
        assert reg.fee_schedule("rb2610", "20260831").open == D("0.0001")  # contract schedule not yet in force

    def test_fee_latest_effective_within_tier(self, reg: InstrumentRegistry) -> None:
        assert reg.fee_schedule("rb2701", "20260930").open == D("0.0001")
        assert reg.fee_schedule("rb2701", "20261001").open == D("0.0002")

    def test_fee_before_any_schedule_uses_earliest(self, reg: InstrumentRegistry) -> None:
        assert reg.fee_schedule("rb2701", "20150105").open == D("0.0001")

    def test_margin_rate_is_company_rate(self, reg: InstrumentRegistry) -> None:
        assert reg.margin_rate("rb2701", "20260922") == D("0.08")  # 0.07 + 0.01
        assert reg.margin_rate("rb2610", "20260922") == D("0.11")  # contract-specific wins
        assert reg.margin_rate("rb2610", "20260831") == D("0.08")
        assert reg.margin_rate("rb2701", "20261012") == D("0.10")  # later "*" schedule
        assert reg.margin_rate("rb2610", "20261012") == D("0.11")  # contract list still has precedence

    def test_highest_rate_in_force_is_charged(self, reg: InstrumentRegistry) -> None:
        # "*" 上调到 12%+1% 后，rb2610 自己的 10%+1% 不再是最高，按 13% 收取
        assert reg.margin_rate("rb2610", "20261019") == D("0.11")
        assert reg.margin_rate("rb2610", "20261020") == D("0.13")
        assert reg.margin_rate("rb2701", "20261020") == D("0.13")
        assert reg.margin_schedule("rb2610", "20261020").contracts == "*"
        assert reg.margin_schedule("rb2610", "20261019").contracts == ["rb2610"]

    def test_schedules_are_memoized(self, reg: InstrumentRegistry) -> None:
        assert reg.fee_schedule("rb2701", "20260922") is reg.fee_schedule("rb2701", "20260922")


# ---------------------------------------------------------------------------
# Prices and settlement parameters
# ---------------------------------------------------------------------------


class TestPrices:
    def test_limit_prices_round_inward(self, reg: InstrumentRegistry) -> None:
        assert reg.limit_prices("IF2612", D("4462.2")) == (D("4908.4"), D("4016.0"))
        assert reg.limit_prices("rb2701", D("3121")) == (D("3277"), D("2965"))

    def test_tick_and_rounding(self, reg: InstrumentRegistry) -> None:
        assert reg.on_tick("T2612", 101.235) and not reg.on_tick("T2612", 101.236)
        assert reg.round_price("IF2612", D("4414.9"), Rounding.FLOOR_TICK) == D("4414.8")
        assert reg.round_price("IF2612", D("4414.9"), Rounding.ROUND_TICK) == D("4415.0")
        assert reg.round_price("IF2612", D("4414.9"), Rounding.NONE) == D("4414.9")

    def test_session_intervals(self, reg: InstrumentRegistry) -> None:
        def hm(ns: int) -> str:
            return ns_to_bj(ns).strftime("%d %H:%M")

        t = [(hm(a), hm(b)) for a, b in reg.session_intervals("T2612", "20260922")]
        assert t == [("22 09:30", "22 11:30"), ("22 13:00", "22 15:15")]
        rb = [(hm(a), hm(b)) for a, b in reg.session_intervals("rb2701", "20260922")]
        assert rb[0] == ("21 21:00", "21 23:00")  # 夜盘属于下一交易日
        assert len(rb) == 4
        # 节后第一天（09-28）没有夜盘
        assert [(hm(a), hm(b)) for a, b in reg.session_intervals("rb2701", "20260928")][0] == ("28 09:00", "28 10:15")

    def test_settle_parameters(self, reg: InstrumentRegistry) -> None:
        assert reg.settle_method("rb2701") == SettleMethod.VWAP_FULL_DAY
        assert reg.settle_window("rb2701") is None
        assert reg.settle_window("T2612") == (time(14, 15), time(15, 15))
        assert reg.settle_window("IF2612") == (time(14, 0), time(15, 0))  # default for index futures
        start, end = reg.settle_window_ns("T2612", "20260922") or (0, 0)
        assert end - start == 3600 * 10**9
        assert reg.settle_rounding("rb2701") == Rounding.FLOOR_TICK
        assert reg.settle_fallback("IF2612") == ["HOURLY", "FULLDAY", "BENCHMARK", "PRE_SETTLE"]


# ---------------------------------------------------------------------------
# Big-side groups and exits
# ---------------------------------------------------------------------------


class TestBigSide:
    def test_group_ids(self, reg: InstrumentRegistry) -> None:
        assert reg.margin_group("rb2701") == ("SHFE:rb", MarginMethod.BIG_SIDE)
        assert reg.margin_group("IF2612") == ("CFFEX:INDEX", MarginMethod.BIG_SIDE)
        assert reg.margin_group("T2612") == ("CFFEX:TREASURY", MarginMethod.BIG_SIDE)
        assert reg.margin_group("m2701") == ("GROSS:m2701", MarginMethod.GROSS)

    def test_shfe_exit_from_l_minus_5_settlement(self, reg: InstrumentRegistry) -> None:
        # rb2610 最后交易日 2026-10-15：L−5 = 10-08（跨国庆长假）
        assert reg.big_side_exit_day("rb2610") == "20261008"
        assert not reg.big_side_excluded("rb2610", "20260930")
        assert reg.big_side_excluded("rb2610", "20261008")
        assert reg.big_side_excluded("rb2610", "20261015")
        assert not reg.big_side_excluded("rb2610", "20261008", intraday=True)
        assert reg.big_side_excluded("rb2610", "20261009", intraday=True)

    def test_cffex_treasury_exits_the_day_before_delivery_month(self, reg: InstrumentRegistry) -> None:
        # 交割月 2026-12：前一个交易日为 2026-11-30（周一）
        assert reg.big_side_exit_day("T2612") == "20261130"
        assert not reg.big_side_excluded("T2612", "20261127")
        assert reg.big_side_excluded("T2612", "20261130")
        assert not reg.big_side_excluded("T2612", "20261130", intraday=True)
        assert reg.big_side_excluded("T2612", "20261201", intraday=True)

    def test_index_and_gross_never_excluded(self, reg: InstrumentRegistry) -> None:
        assert reg.big_side_exit_day("IF2612") is None
        assert reg.big_side_exit_day("m2701") is None
        assert not reg.big_side_excluded("IF2612", "20261218")

    def test_weekday_fallback_without_calendar(self) -> None:
        products = make_products(
            {"rb": product("SHFE", multiplier=10, tick="1", fees=[rate_fee("0.0001")], margins=[margin("0.07")])},
            [inst("rb2610", "rb", "20261015")],
        )
        no_cal = InstrumentRegistry(products)
        # 没有日历时只跳过周末（节假日被忽略）：10-15 往前 5 个工作日 = 10-08
        assert no_cal.big_side_exit_day("rb2610") == "20261008"


# ---------------------------------------------------------------------------
# The bundled products.yaml
# ---------------------------------------------------------------------------


class TestBundledConfig:
    def test_real_products(self, default_config: FullConfig) -> None:
        reg = InstrumentRegistry(default_config.products, default_config.calendar)
        assert reg.fee_schedule("rb2701", "20260928").open == D("0.0001")
        assert reg.fee_schedule("rb2705", "20260928").open == D("0.0001")  # month 5 listed
        assert reg.margin_rate("rb2610", "20260928") == D("0.11")
        assert reg.margin_rate("i2701", "20260929") == D("0.09")
        assert reg.margin_rate("i2701", "20260930") == D("0.11")  # 节前上调
        assert reg.big_side_exit_day("rb2610") == "20261008"
        assert reg.big_side_exit_day("T2612") == "20261130"
        assert reg.big_side_exit_day("IF2612") is None
        assert reg.fee_schedule("j2701", "20260928").intraday == D("0.00014")
        for inst_id in reg.instrument_ids():
            reg.get(inst_id)
            reg.fee_schedule(inst_id, "20260928")
            reg.margin_rate(inst_id, "20260928")
