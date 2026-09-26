"""Golden clearing cases (tests/golden/CASES.md): the acceptance criteria of QS-01 (01 §10.2).

Every number below is the exact expected value from CASES.md (Decimal, fees
rounded half-up per trade). Every golden day also checks the exchange formula
(01 §8.15 第二道校验) twice: with an independent test-side oracle and through
the settlement's own ``EXCH_FORMULA`` check.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from futrisk.common.config import FullConfig
from futrisk.core import (
    DaySettlement,
    InstrumentRegistry,
    ManualPrice,
    PositionError,
    SettlementError,
    build_settle_input,
    determine_settlement_price,
    seed_sod,
    settle_day,
)
from futrisk.core.fees import DayFeeBook
from futrisk.core.lots import PositionBook
from futrisk.messages import codes
from futrisk.messages.enums import (
    CheckStatus,
    FeeType,
    LimitLock,
    PosDirection,
    ReconCheck,
    Rounding,
    SettlePriceSource,
)
from futrisk.messages.eod import SodAccount, SodLot, SodSnapshot
from futrisk.messages.orders import Trade

from .builders import (
    CT,
    B,
    C,
    Day,
    O,
    S,
    TradeFactory,
    formula_oracle,
    inst,
    lot_fee,
    make_registry,
    margin,
    product,
    rate_fee,
    run_days,
    ts,
    views,
)

D = Decimal


def account(res: DaySettlement, account_id: str = "ACC1"):  # noqa: ANN201 - test helper
    return next(a for a in res.result.accounts if a.account_id == account_id)


def position(res: DaySettlement, instrument_id: str, direction: PosDirection, account_id: str = "ACC1"):  # noqa: ANN201
    return next(
        p
        for p in res.result.positions
        if p.account_id == account_id and p.instrument_id == instrument_id and p.direction == direction
    )


def assert_all_checks_pass(res: DaySettlement) -> None:
    bad = [(c.check, c.account_id, c.detail) for c in res.checks if c.status != CheckStatus.PASS]
    assert not bad, bad
    kinds = {c.check for c in res.checks}
    assert {
        ReconCheck.EQUITY_IDENTITY,
        ReconCheck.POSITION_IDENTITY,
        ReconCheck.EXCH_FORMULA,
        ReconCheck.MTM_VS_FIFO,
        ReconCheck.FIRM_SUM,
    } <= kinds


def assert_formula(
    res: DaySettlement,
    trades: list[Trade],
    instrument_id: str,
    *,
    settle: str,
    pre_settle: str,
    yd_long: int,
    yd_short: int,
    multiplier: int,
    expected: str,
    account_id: str = "ACC1",
) -> None:
    """Exchange-formula oracle == close + position P&L == the CASES.md number."""
    mine = [t for t in trades if t.account_id == account_id and t.instrument_id == instrument_id]
    oracle = formula_oracle(
        mine, settle=D(settle), pre_settle=D(pre_settle), yd_long=yd_long, yd_short=yd_short, multiplier=multiplier
    )
    mtm = sum(
        (
            D(p.close_pnl) + D(p.position_pnl)
            for p in res.result.positions
            if p.account_id == account_id and p.instrument_id == instrument_id
        ),
        D(0),
    )
    assert oracle == D(expected)
    assert mtm == D(expected)
    check = next(c for c in res.checks if c.check == ReconCheck.EXCH_FORMULA and c.account_id == account_id)
    assert check.status == CheckStatus.PASS


# ===========================================================================
# Case 1: SHFE rb2701
# ===========================================================================

T1, T2, T3, T4 = "20260922", "20260923", "20260924", "20260928"


def case1_registry(close_today_mult: str = "1") -> InstrumentRegistry:
    return make_registry(
        {
            "rb": product(
                "SHFE",
                multiplier=10,
                tick="1",
                fees=[rate_fee("0.0001", close_today_mult=close_today_mult)],
                margins=[margin("0.07")],
            )
        },
        [inst("rb2701", "rb", "20270115")],
    )


def case1_days(tf: TradeFactory) -> list[Day]:
    return [
        Day(
            T1,
            T2,
            {"rb2701": D("3120")},
            [
                tf("ACC1", "rb2701", B, O, 3100.0, 10, T1, "2026-09-21 21:05"),  # T0 night -> T1
                tf("ACC1", "rb2701", B, O, 3110.0, 5, T1, "2026-09-22 09:40"),
                tf("ACC1", "rb2701", S, CT, 3130.0, 3, T1, "2026-09-22 14:10"),
            ],
        ),
        Day(
            T2,
            T3,
            {"rb2701": D("3132")},
            [
                tf("ACC1", "rb2701", B, O, 3115.0, 4, T2, "2026-09-22 21:10"),  # T1 night -> T2
                tf("ACC1", "rb2701", S, C, 3140.0, 6, T2, "2026-09-23 09:30"),  # '1' = 平昨
                tf("ACC1", "rb2701", S, CT, 3135.0, 2, T2, "2026-09-23 10:00"),
            ],
        ),
        Day(T3, T4, {"rb2701": D("3095")}, [tf("ACC1", "rb2701", S, C, 3100.0, 8, T3, "2026-09-24 09:30")]),
    ]


@pytest.fixture(scope="module")
def case1() -> tuple[list[Day], list[DaySettlement]]:
    days = case1_days(TradeFactory())
    sod = seed_sod(T1, {"ACC1": "200000"}, pre_settle={"rb2701": "3100"})
    return days, run_days(case1_registry(), sod, days)


class TestCase1ShfeRb:
    EXPECTED = [
        # close, position, formula, fee, equity, margin, available, risk
        ("900.00", "1900.00", "2800.00", "55.94", "202744.06", "26208.00", "176536.06", "0.12926643"),
        ("1600.00", "1060.00", "2660.00", "37.57", "205366.49", "17539.20", "187827.29", "0.08540439"),
        ("-2560.00", "0.00", "-2560.00", "24.80", "202781.69", "0.00", "202781.69", "0.00000000"),
    ]

    @pytest.mark.parametrize("i", [0, 1, 2], ids=["T1", "T2", "T3"])
    def test_day_funds(self, case1: tuple[list[Day], list[DaySettlement]], i: int) -> None:
        _, results = case1
        a = account(results[i])
        close, pos, formula, fee, equity, marg, avail, risk = self.EXPECTED[i]
        assert (a.close_pnl, a.position_pnl, a.exch_formula_pnl, a.fee) == (close, pos, formula, fee)
        assert (a.balance, a.equity, a.margin, a.available, a.risk_degree) == (equity, equity, marg, avail, risk)
        assert a.margin_call == "0.00"
        assert a.checks_passed
        assert_all_checks_pass(results[i])

    def test_trade_fees(self, case1: tuple[list[Day], list[DaySettlement]]) -> None:
        _, results = case1
        fees = [[(t.fee, t.fee_type) for t in r.result.trades] for r in results]
        assert fees == [
            [("31.00", FeeType.OPEN), ("15.55", FeeType.OPEN), ("9.39", FeeType.CLOSE_TODAY)],
            [("12.46", FeeType.OPEN), ("18.84", FeeType.CLOSE_YD), ("6.27", FeeType.CLOSE_TODAY)],
            [("24.80", FeeType.CLOSE_YD)],
        ]

    def test_exchange_formula_every_day(self, case1: tuple[list[Day], list[DaySettlement]]) -> None:
        days, results = case1
        # T1: 无昨仓；T2: P=3120，昨多 12；T3: P=3132，昨多 8
        assert_formula(results[0], days[0].trades, "rb2701", settle="3120", pre_settle="3100", yd_long=0, yd_short=0, multiplier=10, expected="2800")
        assert_formula(results[1], days[1].trades, "rb2701", settle="3132", pre_settle="3120", yd_long=12, yd_short=0, multiplier=10, expected="2660")
        assert_formula(results[2], days[2].trades, "rb2701", settle="3095", pre_settle="3132", yd_long=8, yd_short=0, multiplier=10, expected="-2560")

    def test_close_today_matches_the_3100_lots_fifo(self, case1: tuple[list[Day], list[DaySettlement]]) -> None:
        _, results = case1
        details = results[0].result.close_details
        assert [(d.close_type, d.lot_open_price, d.qty, d.close_pnl_mtm) for d in details] == [("TODAY", "3100", 3, "900.00")]
        # T2: 平昨 6 手消耗 @3100 的昨仓（基准 3120），平今 2 手消耗 @3115 的今仓
        d2 = [(d.close_type, d.lot_open_price, d.basis_price, d.qty, d.close_pnl_mtm, d.close_pnl_fifo) for d in results[1].result.close_details]
        assert d2 == [("YD", "3100", "3120", 6, "1200.00", "2400.00"), ("TODAY", "3115", "3115", 2, "400.00", "400.00")]

    def test_sod_roll(self, case1: tuple[list[Day], list[DaySettlement]]) -> None:
        _, results = case1
        sod2 = results[0].next_sod
        assert sod2.trading_day == T2 and sod2.pre_settle == {"rb2701": "3120"}
        acc = sod2.accounts[0]
        assert acc.pre_balance == "202744.06"
        assert [(lot.lot_no, lot.open_price, lot.qty, lot.basis_price, lot.open_td) for lot in acc.lots] == [
            (1, "3100", 7, "3120", T1),
            (2, "3110", 5, "3120", T1),
        ]
        sod3 = results[1].next_sod.accounts[0]
        assert [(lot.lot_no, lot.qty, lot.basis_price) for lot in sod3.lots] == [(1, 1, "3132"), (2, 5, "3132"), (3, 2, "3132")]
        assert results[2].next_sod.accounts[0].lots == []

    def test_position_rows(self, case1: tuple[list[Day], list[DaySettlement]]) -> None:
        _, results = case1
        p = position(results[1], "rb2701", PosDirection.LONG)
        assert (p.sod_qty, p.open_qty, p.close_qty, p.close_today_qty, p.close_yd_qty, p.eod_qty) == (12, 4, 8, 2, 6, 8)
        assert (p.settle_price, p.pre_settle_price, p.margin_rate, p.margin_gross) == ("3132", "3120", "0.07", "17539.20")

    def test_invariants(self, case1: tuple[list[Day], list[DaySettlement]]) -> None:
        days, results = case1
        mtm = sum((D(account(r).close_pnl) + D(account(r).position_pnl) for r in results), D(0))
        sells = sum((D(str(t.price)) * t.qty for d in days for t in d.trades if t.direction == S), D(0))
        buys = sum((D(str(t.price)) * t.qty for d in days for t in d.trades if t.direction == B), D(0))
        assert (sells, buys) == (D("59300"), D("59010"))
        assert mtm == D("2900") == (sells - buys) * 10
        fees = sum((D(account(r).fee) for r in results), D(0))
        assert fees == D("118.31")
        assert D("200000") + mtm - fees == D(account(results[-1]).equity) == D("202781.69")

    def test_close_today_6_on_t2_rejected(self, case1: tuple[list[Day], list[DaySettlement]]) -> None:
        _, results = case1
        reg = case1_registry()
        book = PositionBook(reg, T2)
        book.load_sod_account(results[0].next_sod.accounts[0])
        tf = TradeFactory("N")
        book.apply_trade(tf("ACC1", "rb2701", B, O, 3115.0, 4, T2, "2026-09-22 21:10"))
        assert book.check_close("ACC1", "rb2701", S, CT, 6) == codes.REJECT_CLOSE_TODAY_EXCEEDS
        assert book.check_close("ACC1", "rb2701", S, CT, 4) is None
        with pytest.raises(PositionError) as err:
            book.apply_trade(tf("ACC1", "rb2701", S, CT, 3135.0, 6, T2, "2026-09-23 10:00"))
        assert err.value.code == codes.REJECT_CLOSE_TODAY_EXCEEDS
        # 结算侧同样拒绝：成交事实无法入账时 EOD 失败，而不是产出错误数字
        bad = [
            tf("ACC1", "rb2701", B, O, 3115.0, 4, T2, "2026-09-22 21:10"),
            tf("ACC1", "rb2701", S, CT, 3135.0, 6, T2, "2026-09-23 10:00"),
        ]
        with pytest.raises(SettlementError, match="REJECT_CLOSE_TODAY_EXCEEDS"):
            settle_day(
                trading_day=T2, version=1, sod=results[0].next_sod, trades=bad, cash_movements=[],
                settle_prices=views(T2, {"rb2701": D("3132")}), registry=reg, next_trading_day=T3,
            )

    def test_any_close_today_on_t3_rejected(self, case1: tuple[list[Day], list[DaySettlement]]) -> None:
        _, results = case1
        book = PositionBook(case1_registry(), T3)
        book.load_sod_account(results[1].next_sod.accounts[0])
        assert book.check_close("ACC1", "rb2701", S, CT, 1) == codes.REJECT_CLOSE_TODAY_EXCEEDS
        assert book.check_close("ACC1", "rb2701", S, C, 8) is None
        assert book.check_close("ACC1", "rb2701", S, C, 9) == codes.REJECT_CLOSE_YD_EXCEEDS

    def test_close_today_multiplier_2_variant(self) -> None:
        days = case1_days(TradeFactory())
        sod = seed_sod(T1, {"ACC1": "200000"}, pre_settle={"rb2701": "3100"})
        results = run_days(case1_registry(close_today_mult="2"), sod, days)
        ct_fees = [t.fee for r in results for t in r.result.trades if t.fee_type == FeeType.CLOSE_TODAY]
        assert ct_fees == ["18.78", "12.54"]
        assert [account(r).fee for r in results] == ["65.33", "43.84", "24.80"]
        for r in results:
            assert_all_checks_pass(r)


# ===========================================================================
# Case 2: CFFEX IF2612
# ===========================================================================

D1, D2, D3, D4 = "20260922", "20260923", "20260924", "20260928"

D1_TAPE = [
    ("2026-09-22 10:00:00", 4400.0, 2),
    ("2026-09-22 13:55:00", 4380.0, 50),
    ("2026-09-22 14:05:00", 4410.0, 10),
    ("2026-09-22 14:20:00", 4415.0, 20),
    ("2026-09-22 14:30:00", 4420.0, 5),
    ("2026-09-22 14:45:00", 4418.0, 15),
    ("2026-09-22 14:59:30", 4412.0, 10),
]
D2_TAPE = [
    ("2026-09-23 09:35:00", 4400.0, 1),
    ("2026-09-23 10:30:00", 4450.0, 1),
    ("2026-09-23 14:10:00", 4440.0, 10),
    ("2026-09-23 14:40:00", 4444.0, 10),
    ("2026-09-23 14:55:00", 4442.0, 5),
]


def index_product(**over: object) -> dict[str, object]:
    base = product(
        "CFFEX",
        multiplier=300,
        tick="0.2",
        limit_pct="0.10",
        fees=[rate_fee("0.000023", close_today_mult="10")],
        margins=[margin("0.12")],
        sector="equity_index",
        settle_method="VWAP_LAST_HOUR",
        settle_window="14:00-15:00",
        big_side_group="CFFEX:INDEX",
        physical_delivery=False,
    )
    base.update(over)
    return base


def case2_registry(close_style: str = "TODAY_FIRST", fee_rule: str = "SAME_DAY_OPENS_FIRST") -> InstrumentRegistry:
    return make_registry(
        {"IF": index_product(), "IH": index_product()},
        [inst("IF2612", "IF", "20261218", "202612"), inst("IH2612", "IH", "20261218", "202612")],
        rules={"CFFEX": {"close_style": close_style, "close_today_fee_rule": fee_rule}},
    )


def case2_run(
    close_style: str = "TODAY_FIRST", fee_rule: str = "SAME_DAY_OPENS_FIRST", registry: InstrumentRegistry | None = None
) -> tuple[list[Day], list[DaySettlement]]:
    reg = registry or case2_registry(close_style, fee_rule)
    s1 = determine_settlement_price(
        build_settle_input(reg, "IF2612", D1, [(ts(w), p, q) for w, p, q in D1_TAPE], pre_settle="4380"), reg
    )
    s2 = determine_settlement_price(
        build_settle_input(reg, "IF2612", D2, [(ts(w), p, q) for w, p, q in D2_TAPE], pre_settle=s1.price), reg
    )
    tf = TradeFactory()
    days = [
        Day(D1, D2, {"IF2612": s1}, [
            tf("ACC1", "IF2612", B, O, 4400.0, 2, D1, "2026-09-22 10:00"),
            tf("ACC1", "IF2612", S, C, 4420.0, 1, D1, "2026-09-22 14:30"),
        ]),
        Day(D2, D3, {"IF2612": s2}, [
            tf("ACC1", "IF2612", B, O, 4400.0, 1, D2, "2026-09-23 09:35"),
            tf("ACC1", "IF2612", S, C, 4450.0, 1, D2, "2026-09-23 10:30"),
        ]),
        Day(D3, D4, {"IF2612": D("4480.0")}, [tf("ACC1", "IF2612", S, C, 4500.0, 1, D3, "2026-09-24 10:00")]),
    ]
    sod = seed_sod(D1, {"ACC1": "1000000"}, pre_settle={"IF2612": "4380"})
    return days, run_days(reg, sod, days)


class TestCase2CffexIf:
    def test_settlement_price_s1_from_last_hour(self) -> None:
        reg = case2_registry()
        inp = build_settle_input(reg, "IF2612", D1, [(ts(w), p, q) for w, p, q in D1_TAPE], pre_settle="4380")
        assert (inp.window_volume, inp.window_turnover) == (60, str(264890 * 300))
        assert inp.day_volume == 112
        view = determine_settlement_price(inp, reg)
        assert (view.price, view.source, view.rounding, view.vwap_raw) == ("4414.8", SettlePriceSource.VWAP, Rounding.ROUND_TICK, "4414.833333")
        # 1 位小数、四舍五入到 tick、向下取整到 tick 三种口径结果相同
        raw = D(264890) / D(60)
        assert reg.round_price("IF2612", raw, Rounding.FLOOR_TICK) == D("4414.8")
        assert reg.round_price("IF2612", raw, Rounding.ROUND_TICK) == D("4414.8")
        assert raw.quantize(D("0.1")) == D("4414.8")
        # 全日 VWAP 会得到 4399.02：窗口选错的缺陷会被抓到
        assert (D(inp.day_turnover) / (112 * 300)).quantize(D("0.01")) == D("4399.02")

    def test_settlement_price_s2(self) -> None:
        _, results = case2_run()
        assert [p.price for r in results[:2] for p in r.result.prices] == ["4414.8", "4442"]

    def test_fees(self) -> None:
        _, results = case2_run()
        assert [[t.fee for t in r.result.trades] for r in results] == [["60.72", "304.98"], ["30.36", "307.05"], ["31.05"]]
        assert [[t.fee_type for t in r.result.trades] for r in results] == [
            [FeeType.OPEN, FeeType.CLOSE_TODAY],
            [FeeType.OPEN, FeeType.CLOSE_TODAY],  # 当日先有开仓，平仓按平今收费
            [FeeType.CLOSE_YD],  # 当日没有开仓，按平昨收费
        ]
        assert [account(r).fee for r in results] == ["365.70", "337.41", "31.05"]
        assert sum((D(account(r).fee) for r in results), D(0)) == D("734.16")

    def test_lot_match_fee_engine_would_charge_30_71(self) -> None:
        # 若按 FIFO 实际消耗的 lot 计费（昨仓），D2 平仓只收 30.71：CFFEX 规则必须与 lot 消耗分开
        _, results = case2_run(close_style="FIFO", fee_rule="LOT_MATCH")
        assert [t.fee for t in results[1].result.trades] == ["30.36", "30.71"]

    @pytest.mark.parametrize(
        ("style", "d2_close", "d2_pos"),
        [("FIFO", "10560.00", "12600.00"), ("TODAY_FIRST", "15000.00", "8160.00")],
    )
    def test_days(self, style: str, d2_close: str, d2_pos: str) -> None:
        days, results = case2_run(close_style=style)
        expected = [
            ("6000.00", "4440.00", "1010074.30", "158932.80", "851141.50", "0.15734763"),
            (d2_close, d2_pos, "1032896.89", "159912.00", "872984.89", "0.15481894"),
            ("17400.00", "0.00", "1050265.84", "0.00", "1050265.84", "0.00000000"),
        ]
        for r, (close, pos, equity, marg, avail, risk) in zip(results, expected, strict=True):
            a = account(r)
            assert (a.close_pnl, a.position_pnl, a.equity, a.margin, a.available, a.risk_degree) == (
                close, pos, equity, marg, avail, risk,
            )
            assert_all_checks_pass(r)
        assert_formula(results[0], days[0].trades, "IF2612", settle="4414.8", pre_settle="4380", yd_long=0, yd_short=0, multiplier=300, expected="10440")
        assert_formula(results[1], days[1].trades, "IF2612", settle="4442", pre_settle="4414.8", yd_long=1, yd_short=0, multiplier=300, expected="23160")
        assert_formula(results[2], days[2].trades, "IF2612", settle="4480", pre_settle="4442", yd_long=1, yd_short=0, multiplier=300, expected="17400")

    def test_today_first_columns_agree(self) -> None:
        # products.yaml 的 CFFEX close_style = TODAY_FIRST：lot 消耗与"先平当日新开仓"计费一致
        _, results = case2_run(close_style="TODAY_FIRST")
        for r in results:
            for t, tr in zip(r.result.trades, r.trades, strict=True):
                if tr.fill.is_open:
                    continue
                assert t.matched_today_qty == tr.fee.qty_of(FeeType.CLOSE_TODAY)
                assert t.matched_yd_qty == tr.fee.qty_of(FeeType.CLOSE_YD)
        d2 = results[1]
        [detail] = d2.result.close_details
        assert (detail.close_type, detail.lot_open_price, detail.close_pnl_mtm) == ("TODAY", "4400", "15000.00")
        assert d2.result.trades[1].fee_type == FeeType.CLOSE_TODAY
        p = position(d2, "IF2612", PosDirection.LONG)
        assert (p.close_today_qty, p.close_yd_qty, p.close_pnl, p.position_pnl) == (1, 0, "15000.00", "8160.00")

    def test_fifo_variant_shows_the_other_matching(self) -> None:
        _, results = case2_run(close_style="FIFO")
        d2 = results[1]
        [detail] = d2.result.close_details
        assert (detail.close_type, detail.basis_price, detail.close_pnl_mtm) == ("YD", "4414.8", "10560.00")
        assert d2.result.trades[1].fee_type == FeeType.CLOSE_TODAY  # 计费仍按先平当日新开仓
        assert (d2.result.trades[1].matched_today_qty, d2.result.trades[1].matched_yd_qty) == (0, 1)

    def test_bundled_products_yaml_path(self, default_config: FullConfig) -> None:
        # 默认配置（IF：0.23‱/平今 ×10、12%、TODAY_FIRST）跑出与黄金用例相同的 TODAY_FIRST 结果
        reg = InstrumentRegistry(default_config.products, default_config.calendar)
        assert reg.close_style("IF2612") == "TODAY_FIRST"
        _, results = case2_run(registry=reg)
        expected = [
            ("6000.00", "4440.00", "365.70", "1010074.30", "158932.80"),
            ("15000.00", "8160.00", "337.41", "1032896.89", "159912.00"),
            ("17400.00", "0.00", "31.05", "1050265.84", "0.00"),
        ]
        for r, row in zip(results, expected, strict=True):
            a = account(r)
            assert (a.close_pnl, a.position_pnl, a.fee, a.equity, a.margin) == row
            assert_all_checks_pass(r)

    def test_invariant(self) -> None:
        days, results = case2_run()
        sells = sum((D(str(t.price)) * t.qty for d in days for t in d.trades if t.direction == S), D(0))
        buys = sum((D(str(t.price)) * t.qty for d in days for t in d.trades if t.direction == B), D(0))
        assert (sells, buys) == (D("13370"), D("13200"))
        mtm = sum((D(account(r).close_pnl) + D(account(r).position_pnl) for r in results), D(0))
        assert mtm == D("51000") == (sells - buys) * 300


class TestCffexFaqCloseToday:
    """CFFEX FAQ: close-today classification follows same-day opens, not the lots consumed."""

    def _run(self, trades_spec: list[tuple[object, int, str]]) -> DaySettlement:
        reg = case2_registry()
        tf = TradeFactory()
        sod = SodSnapshot(
            trading_day=D2,
            version=1,
            accounts=[
                SodAccount(
                    account_id="ACC1",
                    pre_balance="10000000.00",
                    initial_capital="10000000.00",
                    hwm_equity="10000000.00",
                    lots=[SodLot(instrument_id="IF2612", direction=PosDirection.LONG, lot_no=1, open_td=D1,
                                 open_ts_ns=ts("2026-09-22 10:00"), open_price="4400", qty=20, basis_price="4400")],
                )
            ],
            pre_settle={"IF2612": "4400"},
        )
        trades = [
            tf("ACC1", "IF2612", side, O if side == B else C, 4400.0, qty, D2, when)
            for side, qty, when in trades_spec
        ]
        return settle_day(trading_day=D2, version=1, sod=sod, trades=trades, cash_movements=[],
                          settle_prices=views(D2, {"IF2612": D("4400")}), registry=reg, next_trading_day=D3)

    def test_sell_10_then_buy_8_has_no_close_today(self) -> None:
        res = self._run([(S, 10, "2026-09-23 10:00"), (B, 8, "2026-09-23 10:05")])
        p = position(res, "IF2612", PosDirection.LONG)
        assert (p.close_today_qty, p.close_yd_qty) == (0, 10)
        assert [t.fee_type for t in res.result.trades] == [FeeType.CLOSE_YD, FeeType.OPEN]
        assert_all_checks_pass(res)

    def test_buy_8_then_sell_10_is_8_close_today_and_2_close_yd(self) -> None:
        res = self._run([(B, 8, "2026-09-23 10:00"), (S, 10, "2026-09-23 10:05")])
        p = position(res, "IF2612", PosDirection.LONG)
        assert (p.close_today_qty, p.close_yd_qty) == (8, 2)
        close = res.trades[1]
        assert close.fee.qty_of(FeeType.CLOSE_TODAY) == 8 and close.fee.qty_of(FeeType.CLOSE_YD) == 2
        # 8 × 4400 × 300 × 0.00023 + 2 × 4400 × 300 × 0.000023 = 2428.80 + 60.72
        assert res.result.trades[1].fee == "2489.52"
        assert res.result.trades[1].fee_type == FeeType.CLOSE_TODAY
        assert_all_checks_pass(res)


# ===========================================================================
# Case 3: SHFE two-way position (big side) + CFFEX variant
# ===========================================================================

E1, E2, E3 = "20260930", "20261008", "20261009"


def case3_registry() -> InstrumentRegistry:
    return make_registry(
        {
            "rb": product(
                "SHFE",
                multiplier=10,
                tick="1",
                fees=[rate_fee("0.0001")],
                margins=[margin("0.07"), margin("0.10", contracts=["rb2610"], effective_from="20260901")],
                big_side_group="SHFE:rb",
            ),
            "IF": index_product(),
            "IH": index_product(),
        },
        [
            inst("rb2610", "rb", "20261015", "202610"),
            inst("rb2701", "rb", "20270115", "202701"),
            inst("IF2612", "IF", "20261218", "202612"),
            inst("IH2612", "IH", "20261218", "202612"),
        ],
    )


class TestCase3BigSide:
    @pytest.fixture(scope="class")
    def run(self) -> tuple[list[Day], list[DaySettlement]]:
        tf = TradeFactory()
        days = [
            Day(E1, E2, {"rb2610": D("3056"), "rb2701": D("3111")}, [
                tf("ACC1", "rb2610", B, O, 3050.0, 8, E1, "2026-09-30 10:00"),
                tf("ACC1", "rb2701", S, O, 3110.0, 10, E1, "2026-09-30 10:05"),
            ]),
            Day(E2, E3, {"rb2610": D("3060"), "rb2701": D("3100")}),
        ]
        sod = seed_sod(E1, {"ACC1": "500000"}, pre_settle={"rb2610": "3050", "rb2701": "3110"})
        return days, run_days(case3_registry(), sod, days)

    def test_exit_day_is_l_minus_5(self) -> None:
        reg = case3_registry()
        assert reg.big_side_exit_day("rb2610") == E2
        assert not reg.big_side_excluded("rb2610", E1)
        assert reg.big_side_excluded("rb2610", E2)
        assert not reg.big_side_excluded("rb2610", E2, intraday=True)  # 退出在 L−5 收盘结算时生效
        assert reg.big_side_excluded("rb2610", E3, intraday=True)

    def test_d1(self, run: tuple[list[Day], list[DaySettlement]]) -> None:
        days, results = run
        r = results[0]
        assert [t.fee for t in r.result.trades] == ["24.40", "31.10"]
        a = account(r)
        assert (a.close_pnl, a.position_pnl, a.fee) == ("0.00", "380.00", "55.50")
        assert (a.equity, a.margin, a.available, a.risk_degree) == ("500324.50", "24448.00", "475876.50", "0.04886429")
        [g] = r.result.margin_groups
        assert (g.margin_group, g.method, g.long_margin, g.short_margin, g.excluded_margin, g.charged_margin) == (
            "SHFE:rb", "BIG_SIDE", "24448.00", "21777.00", "0.00", "24448.00",
        )
        # 空头手数更多（10 > 8），但多头保证金金额更大：按手数取大边会出错
        assert position(r, "rb2701", PosDirection.SHORT).eod_qty > position(r, "rb2610", PosDirection.LONG).eod_qty
        gross = D(position(r, "rb2610", PosDirection.LONG).margin_gross) + D(position(r, "rb2701", PosDirection.SHORT).margin_gross)
        assert gross == D("46225.00")
        assert (gross / D(a.equity)).quantize(D("0.00000001")) == D("0.09239004")  # 9.2390% if charged gross
        assert_all_checks_pass(r)
        assert_formula(r, days[0].trades, "rb2610", settle="3056", pre_settle="3050", yd_long=0, yd_short=0, multiplier=10, expected="480")
        assert_formula(r, days[0].trades, "rb2701", settle="3111", pre_settle="3110", yd_long=0, yd_short=0, multiplier=10, expected="-100")

    def test_d2_rb2610_excluded(self, run: tuple[list[Day], list[DaySettlement]]) -> None:
        days, results = run
        r = results[1]
        a = account(r)
        assert (a.close_pnl, a.position_pnl, a.fee) == ("0.00", "1420.00", "0.00")
        assert (a.equity, a.margin, a.available, a.risk_degree) == ("501744.50", "46180.00", "455564.50", "0.09203888")
        [g] = r.result.margin_groups
        assert (g.long_margin, g.short_margin, g.excluded_margin, g.charged_margin) == ("0.00", "21700.00", "24480.00", "46180.00")
        excluded = {c["instrument_id"]: c["excluded"] for c in g.detail["contracts"]}
        assert excluded == {"rb2610": True, "rb2701": False}
        # 漏掉退出规则时只收 24,480.00，风险度 4.8790%
        assert (D("24480.00") / D(a.equity)).quantize(D("0.00000001")) == D("0.04878977")
        assert_all_checks_pass(r)
        assert_formula(r, days[1].trades, "rb2610", settle="3060", pre_settle="3056", yd_long=8, yd_short=0, multiplier=10, expected="320")
        assert_formula(r, days[1].trades, "rb2701", settle="3100", pre_settle="3111", yd_long=0, yd_short=10, multiplier=10, expected="1100")

    def test_cffex_index_group_across_products(self) -> None:
        reg = case3_registry()
        tf = TradeFactory()
        trades = [
            tf("ACC1", "IF2612", B, O, 4410.0, 1, D1, "2026-09-22 10:00"),
            tf("ACC1", "IH2612", S, O, 2860.0, 1, D1, "2026-09-22 10:01"),
        ]
        sod = seed_sod(D1, {"ACC1": "1000000"}, pre_settle={"IF2612": "4400", "IH2612": "2850"})
        r = settle_day(trading_day=D1, version=1, sod=sod, trades=trades, cash_movements=[],
                       settle_prices=views(D1, {"IF2612": D("4414.8"), "IH2612": D("2858.2")}), registry=reg,
                       next_trading_day=D2)
        [g] = r.result.margin_groups
        assert (g.margin_group, g.method, g.long_margin, g.short_margin, g.charged_margin) == (
            "CFFEX:INDEX", "BIG_SIDE", "158932.80", "102895.20", "158932.80",
        )
        assert account(r).margin == "158932.80"
        assert_all_checks_pass(r)
        assert_formula(r, trades, "IF2612", settle="4414.8", pre_settle="4400", yd_long=0, yd_short=0, multiplier=300, expected="1440")
        assert_formula(r, trades, "IH2612", settle="2858.2", pre_settle="2850", yd_long=0, yd_short=0, multiplier=300, expected="540")

    @pytest.mark.parametrize("day", ["20261211", "20261217", "20261218"])
    def test_index_futures_never_excluded(self, day: str) -> None:
        reg = case3_registry()
        assert reg.big_side_exit_day("IF2612") is None
        assert not reg.big_side_excluded("IF2612", day)
        assert not reg.big_side_excluded("IH2612", day, intraday=True)


# ===========================================================================
# DCE intraday fee example: open 5 / close 3 / open 2 / close 6
# ===========================================================================


class TestDceIntradayBothLegs:
    def test_14_intraday_and_2_regular_lots(self) -> None:
        reg = make_registry(
            {"j": product("DCE", multiplier=100, tick="0.5", fees=[lot_fee("1.5", intraday="3.0")], margins=[margin("0.12")])},
            [inst("j2701", "j", "20270115")],
        )
        tf = TradeFactory()
        trades = [
            tf("ACC1", "j2701", B, O, 2000.0, 5, D2, "2026-09-23 09:01"),
            tf("ACC1", "j2701", S, C, 2010.0, 3, D2, "2026-09-23 09:02"),
            tf("ACC1", "j2701", B, O, 2005.0, 2, D2, "2026-09-23 09:03"),
            tf("ACC1", "j2701", S, C, 2020.0, 6, D2, "2026-09-23 09:04"),
        ]
        sod = SodSnapshot(
            trading_day=D2,
            version=1,
            accounts=[SodAccount(account_id="ACC1", pre_balance="1000000.00", initial_capital="1000000.00",
                                 hwm_equity="1000000.00",
                                 lots=[SodLot(instrument_id="j2701", direction=PosDirection.LONG, lot_no=1, open_td=D1,
                                              open_ts_ns=ts("2026-09-22 10:00"), open_price="1990", qty=2, basis_price="1995")])],
            pre_settle={"j2701": "1995"},
        )
        r = settle_day(trading_day=D2, version=1, sod=sod, trades=trades, cash_movements=[],
                       settle_prices=views(D2, {"j2701": D("2015")}), registry=reg, next_trading_day=D3)
        # 先开先平：第一笔平仓先平 2 手昨仓，只有 1 手今仓算日内；第二笔平仓 6 手全是今仓
        assert [(t.fee, t.fee_type) for t in r.result.trades] == [
            ("15.00", FeeType.INTRADAY_OPEN),  # 5 手开仓最终全部当日平掉
            ("6.00", FeeType.INTRADAY_CLOSE),  # 1 手日内 3.0 + 2 手平昨 1.5 × 2
            ("6.00", FeeType.INTRADAY_OPEN),
            ("18.00", FeeType.INTRADAY_CLOSE),  # 6 手日内
        ]
        assert [(tr.fee.qty_of(FeeType.INTRADAY_CLOSE), tr.fee.qty_of(FeeType.CLOSE_YD)) for tr in r.trades[1::2]] == [
            (1, 2),
            (6, 0),
        ]
        assert [(t.matched_today_qty, t.matched_yd_qty) for t in r.result.trades[1::2]] == [(1, 2), (6, 0)]
        intraday = sum(t.fee.qty_of(FeeType.INTRADAY_OPEN, FeeType.INTRADAY_CLOSE) for t in r.trades)
        regular = sum(t.fee.qty_of(FeeType.OPEN, FeeType.CLOSE_YD, FeeType.CLOSE_TODAY) for t in r.trades)
        assert (intraday, regular) == (14, 2)
        assert account(r).fee == "45.00"
        assert [t.fee_per_lot for t in r.result.trades] == ["3.00", "3.00", "3.00", "3.00"]
        p = position(r, "j2701", PosDirection.LONG)
        assert (p.close_today_qty, p.close_yd_qty, p.eod_qty) == (7, 2, 0)
        assert_all_checks_pass(r)
        assert_formula(r, trades, "j2701", settle="2015", pre_settle="1995", yd_long=2, yd_short=0, multiplier=100,
                       expected=str((D("2010") - 2015) * 3 * 100 + (D("2020") - 2015) * 6 * 100
                                    + (2015 - D("2000")) * 5 * 100 + (2015 - D("2005")) * 2 * 100 + (D("1995") - 2015) * -2 * 100))
        # 盘中逐笔预估（开仓先按开仓费率，配对时补差）合计与 EOD 精确值相同
        book = PositionBook(reg, D2)
        book.load_sod_account(sod.accounts[0])
        fees = DayFeeBook(reg, D2)
        est = D(0)
        for t in trades:
            fill = book.apply_trade(t)
            est += fees.on_fill(fill.key, is_open=fill.is_open, qty=t.qty, price=D(str(t.price)),
                                today_lot_qty=fill.close_today_qty).estimate
        assert est == D("45.00")

    @staticmethod
    def _sod_two_yd_lots(pre_balance: str = "1000000.00") -> SodSnapshot:
        return SodSnapshot(
            trading_day=D2,
            version=1,
            accounts=[SodAccount(account_id="ACC1", pre_balance=pre_balance, initial_capital=pre_balance,
                                 hwm_equity=pre_balance,
                                 lots=[SodLot(instrument_id="j2701", direction=PosDirection.LONG, lot_no=1, open_td=D1,
                                              open_ts_ns=ts("2026-09-22 10:00"), open_price="1990", qty=2, basis_price="1995")])],
            pre_settle={"j2701": "1995"},
        )

    def test_close_consumes_yesterday_lots_first(self) -> None:
        # 2 手昨仓，买开 5 手，卖平 3 手：平掉 2 手昨仓 + 1 手今仓 → 1 手日内平 + 1 手日内开，2 手平昨，4 手普通开仓
        reg = make_registry(
            {"j": product("DCE", multiplier=100, tick="0.5", fees=[lot_fee("1.5", intraday="3.0")], margins=[margin("0.12")])},
            [inst("j2701", "j", "20270115")],
        )
        tf = TradeFactory()
        trades = [
            tf("ACC1", "j2701", B, O, 2000.0, 5, D2, "2026-09-23 09:01"),
            tf("ACC1", "j2701", S, C, 2010.0, 3, D2, "2026-09-23 09:02"),
        ]
        r = settle_day(trading_day=D2, version=1, sod=self._sod_two_yd_lots(), trades=trades, cash_movements=[],
                       settle_prices=views(D2, {"j2701": D("2015")}), registry=reg, next_trading_day=D3)
        legs = [dict((leg.fee_type, leg.qty) for leg in tr.fee.legs) for tr in r.trades]
        assert legs == [
            {FeeType.INTRADAY_OPEN: 1, FeeType.OPEN: 4},
            {FeeType.INTRADAY_CLOSE: 1, FeeType.CLOSE_YD: 2},
        ]
        # 1 × 3.0 + 4 × 1.5 = 9.00；1 × 3.0 + 2 × 1.5 = 6.00（按时间顺序配对的旧规则会得到 12.00 + 9.00）
        assert [t.fee for t in r.result.trades] == ["9.00", "6.00"]
        assert account(r).fee == "15.00"
        p = position(r, "j2701", PosDirection.LONG)
        assert (p.close_today_qty, p.close_yd_qty, p.eod_qty) == (1, 2, 4)
        assert_all_checks_pass(r)

    def test_close_consumes_yesterday_lots_first_bundled_rates(self, default_config: FullConfig) -> None:
        # products.yaml 的 j：开平 0.0001、日内 0.00014，乘数 100
        reg = InstrumentRegistry(default_config.products, default_config.calendar)
        tf = TradeFactory()
        trades = [
            tf("ACC1", "j2701", B, O, 2000.0, 5, D2, "2026-09-23 09:01"),
            tf("ACC1", "j2701", S, C, 2010.0, 3, D2, "2026-09-23 09:02"),
        ]
        r = settle_day(trading_day=D2, version=1, sod=self._sod_two_yd_lots(), trades=trades, cash_movements=[],
                       settle_prices=views(D2, {"j2701": D("2015")}), registry=reg, next_trading_day=D3)
        # 开仓：2000×100×1×0.00014 + 2000×100×4×0.0001 = 28.00 + 80.00
        # 平仓：2010×100×1×0.00014 + 2010×100×2×0.0001 = 28.14 + 40.20
        assert [t.fee for t in r.result.trades] == ["108.00", "68.34"]
        assert [t.fee_type for t in r.result.trades] == [FeeType.INTRADAY_OPEN, FeeType.INTRADAY_CLOSE]
        assert_all_checks_pass(r)


# ===========================================================================
# SHFE floor-to-tick settlement rounding
# ===========================================================================


def shfe_registry() -> InstrumentRegistry:
    return make_registry(
        {
            "rb": product("SHFE", multiplier=10, tick="1", fees=[rate_fee("0.0001")], margins=[margin("0.07")]),
            "au": product("SHFE", multiplier=1000, tick="0.02", limit_pct="0.14", fees=[lot_fee("10")], margins=[margin("0.16")]),
        },
        [inst("rb2701", "rb", "20270115"), inst("au2612", "au", "20261215")],
    )


class TestShfeFloorToTick:
    def test_rb_vwap_3118_7_settles_at_3118(self) -> None:
        reg = shfe_registry()
        tape = [(ts("2026-09-22 09:30"), 3118.0, 3), (ts("2026-09-22 10:30"), 3119.0, 7)]
        view = determine_settlement_price(build_settle_input(reg, "rb2701", D1, tape, pre_settle="3100"), reg)
        assert (view.price, view.source, view.rounding, view.vwap_raw) == ("3118", SettlePriceSource.VWAP, Rounding.FLOOR_TICK, "3118.7")
        assert reg.round_price("rb2701", D("3118.7"), Rounding.ROUND_TICK) == D("3119")  # ROUND 会得到 3119

    def test_au_floor_on_0_02_tick(self) -> None:
        reg = shfe_registry()
        tape = [(ts("2026-09-22 09:30"), 850.00, 1), (ts("2026-09-22 10:30"), 850.14, 2)]
        view = determine_settlement_price(build_settle_input(reg, "au2612", D1, tape, pre_settle="849"), reg)
        assert view.price == "850.08"  # raw 850.0933…；ROUND_TICK 会得到 850.10
        assert reg.round_price("au2612", D("850.0933"), Rounding.ROUND_TICK) == D("850.10")

    def test_settlement_uses_floored_price(self) -> None:
        reg = shfe_registry()
        tf = TradeFactory()
        tape = [(ts("2026-09-22 09:30"), 3118.0, 3), (ts("2026-09-22 10:30"), 3119.0, 7)]
        view = determine_settlement_price(build_settle_input(reg, "rb2701", D1, tape, pre_settle="3100"), reg)
        trades = [tf("ACC1", "rb2701", B, O, 3119.0, 7, D1, "2026-09-22 10:30")]
        sod = seed_sod(D1, {"ACC1": "100000"}, pre_settle={"rb2701": "3100"})
        r = settle_day(trading_day=D1, version=1, sod=sod, trades=trades, cash_movements=[],
                       settle_prices={"rb2701": view}, registry=reg, next_trading_day=D2)
        a = account(r)
        assert a.position_pnl == "-70.00"  # (3118 − 3119) × 10 × 7
        assert a.margin == "15278.20"  # 3118 × 10 × 7 × 7%
        assert r.result.prices == [view]
        assert r.next_sod.pre_settle == {"rb2701": "3118"}
        assert_all_checks_pass(r)
        assert_formula(r, trades, "rb2701", settle="3118", pre_settle="3100", yd_long=0, yd_short=0, multiplier=10, expected="-70")


# ===========================================================================
# Limit-lock fallback (SHFE/DCE chain: MEDIAN -> LIMIT -> PRE_SETTLE)
# ===========================================================================


class TestLimitLockFallback:
    def test_locked_limit_up_settles_at_upper_limit(self) -> None:
        reg = shfe_registry()
        inp = build_settle_input(reg, "rb2701", D1, [], pre_settle="3121", bid1="3277", limit_lock=LimitLock.UP)
        view = determine_settlement_price(inp, reg)
        # 3121 × 1.05 = 3277.05，向内取整到 3277
        assert (view.price, view.source, view.input_quality) == ("3277", SettlePriceSource.FALLBACK_LIMIT, inp.quality)

    def test_locked_limit_down_settles_at_lower_limit(self) -> None:
        reg = shfe_registry()
        inp = build_settle_input(reg, "rb2701", D1, [], pre_settle="3121", ask1="2965", limit_lock=LimitLock.DOWN)
        view = determine_settlement_price(inp, reg)
        assert (view.price, view.source) == ("2965", SettlePriceSource.FALLBACK_LIMIT)  # 2964.95 向内取整

    def test_median_before_limit_and_pre_settle_last(self) -> None:
        reg = shfe_registry()
        both = build_settle_input(reg, "rb2701", D1, [], pre_settle="3121", bid1="3125", ask1="3130")
        assert (determine_settlement_price(both, reg).price, determine_settlement_price(both, reg).source) == (
            "3125", SettlePriceSource.FALLBACK_MEDIAN,
        )
        nothing = build_settle_input(reg, "rb2701", D1, [], pre_settle="3121")
        assert determine_settlement_price(nothing, reg).source == SettlePriceSource.PRE_SETTLE
        assert determine_settlement_price(nothing, reg).price == "3121"

    def test_manual_and_official_override_the_fallback(self) -> None:
        reg = shfe_registry()
        inp = build_settle_input(reg, "rb2701", D1, [], pre_settle="3121", bid1="3277", limit_lock=LimitLock.UP)
        assert determine_settlement_price(inp, reg, official=D("3270")).source == SettlePriceSource.OFFICIAL
        manual = determine_settlement_price(inp, reg, official=D("3270"), manual=ManualPrice(D("3275"), "risk1", "fix"))
        assert (manual.price, manual.source, manual.operator, manual.rounding) == ("3275", SettlePriceSource.MANUAL, "risk1", Rounding.NONE)

    def test_locked_day_settlement(self) -> None:
        reg = shfe_registry()
        inp = build_settle_input(reg, "rb2701", D2, [], pre_settle="3121", bid1="3277", limit_lock=LimitLock.UP)
        view = determine_settlement_price(inp, reg)
        sod = SodSnapshot(
            trading_day=D2,
            version=1,
            accounts=[SodAccount(account_id="ACC1", pre_balance="100000.00", initial_capital="100000.00",
                                 hwm_equity="100000.00",
                                 lots=[SodLot(instrument_id="rb2701", direction=PosDirection.LONG, lot_no=1, open_td=D1,
                                              open_ts_ns=ts("2026-09-22 10:00"), open_price="3100", qty=5, basis_price="3121")])],
            pre_settle={"rb2701": "3121"},
        )
        r = settle_day(trading_day=D2, version=1, sod=sod, trades=[], cash_movements=[],
                       settle_prices={"rb2701": view}, registry=reg, next_trading_day=D3)
        a = account(r)
        assert (a.position_pnl, a.equity, a.margin) == ("7800.00", "107800.00", "11469.50")  # 3277 × 10 × 5 × 7%
        assert a.fifo_float_pnl == "8850.00"  # (3277 − 3100) × 50
        assert_all_checks_pass(r)
        assert_formula(r, [], "rb2701", settle="3277", pre_settle="3121", yd_long=5, yd_short=0, multiplier=10, expected="7800")
