"""Unit tests for futrisk.core.settlement: settle_day inputs, funds, checks, next SOD (01 §8.15, 03 §4.4)."""

from __future__ import annotations

import random
from decimal import Decimal
from typing import Any

import msgspec
import pytest
from tests.golden.builders import (
    CT,
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
    pv,
    rate_fee,
    ts,
    views,
)

import futrisk.core.settlement as settlement_mod
from futrisk.core import DaySettlement, SettlementError, seed_sod, settle_day
from futrisk.core.instruments import InstrumentRegistry
from futrisk.core.settle_price import build_settle_input, determine_settlement_price
from futrisk.messages import decode, encode
from futrisk.messages.enums import (
    CashType,
    Exchange,
    FeeType,
    Offset,
    PosDirection,
    ReconCheck,
    SettlePriceSource,
)
from futrisk.messages.eod import SettlementResult, SodAccount, SodLot, SodSnapshot
from futrisk.messages.orders import Trade

D = Decimal
TD, NXT = "20260922", "20260923"
PRE = {"rb2701": "3100", "IF2612": "4390"}


@pytest.fixture(scope="module")
def reg() -> InstrumentRegistry:
    def treasury(rate: str) -> dict[str, Any]:
        return product("CFFEX", multiplier=10000, tick="0.005", limit_pct="0.02", fees=[lot_fee("3", close_today_mult="0")],
                       margins=[margin(rate)], settle_method="VWAP_LAST_HOUR", settle_window="14:15-15:15",
                       big_side_group="CFFEX:TREASURY", physical_delivery=True)

    return make_registry(
        {
            "rb": product("SHFE", multiplier=10, tick="1", fees=[rate_fee("0.0001")], margins=[margin("0.07")]),
            "IF": product("CFFEX", multiplier=300, tick="0.2", limit_pct="0.10", fees=[rate_fee("0.000023", close_today_mult="10")],
                          margins=[margin("0.12")], settle_method="VWAP_LAST_HOUR", big_side_group="CFFEX:INDEX"),
            "T": treasury("0.02"),
            "TF": treasury("0.012"),
        },
        [
            inst("rb2701", "rb", "20270115"),
            inst("IF2612", "IF", "20261218"),
            inst("T2612", "T", "20261211", "202612"),
            inst("TF2612", "TF", "20261211", "202612"),
        ],
    )


def sod0(capital: dict[str, str], trading_day: str = TD) -> SodSnapshot:
    return seed_sod(trading_day, capital, pre_settle=PRE)


def settle(
    reg: InstrumentRegistry,
    sod: SodSnapshot,
    trades: list[Trade],
    prices: dict[str, Any] | None = None,
    *,
    trading_day: str = TD,
    next_trading_day: str = NXT,
    version: int = 1,
    wrap: bool = True,
    **kw: Any,
) -> DaySettlement:
    raw = prices if prices is not None else {"rb2701": D("3120"), "IF2612": D("4400")}
    return settle_day(
        trading_day=trading_day,
        version=version,
        sod=sod,
        trades=trades,
        cash_movements=kw.pop("cash_movements", []),
        settle_prices=views(trading_day, raw) if wrap else raw,
        registry=reg,
        next_trading_day=next_trading_day,
        **kw,
    )


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


class TestValidation:
    def test_day_mismatches(self, reg: InstrumentRegistry) -> None:
        tf = TradeFactory()
        sod = sod0({"ACC1": "100000"})
        with pytest.raises(SettlementError, match="SOD snapshot is for"):
            settle(reg, sod0({"ACC1": "1"}, NXT), [])
        with pytest.raises(SettlementError, match="must be after"):
            settle(reg, sod, [], next_trading_day=TD)
        with pytest.raises(SettlementError, match="belongs to"):
            settle(reg, sod, [tf("ACC1", "rb2701", B, O, 3100.0, 1, NXT, "2026-09-23 09:30")])
        with pytest.raises(SettlementError, match="cash movement"):
            settle(reg, sod, [], cash_movements=[cash("ACC1", NXT, CashType.DEPOSIT, "1.00", "c1")])

    def test_unknown_references(self, reg: InstrumentRegistry) -> None:
        tf = TradeFactory()
        sod = sod0({"ACC1": "100000"})
        with pytest.raises(SettlementError, match="has no SOD"):
            settle(reg, sod, [tf("ACC9", "rb2701", B, O, 3100.0, 1, TD, "2026-09-22 09:30")])
        with pytest.raises(SettlementError, match="unknown instrument"):
            settle(reg, sod, [tf("ACC1", "zz2701", B, O, 1.0, 1, TD, "2026-09-22 09:30", exchange=Exchange.DCE)])
        with pytest.raises(SettlementError, match="trades on SHFE"):
            settle(reg, sod, [tf("ACC1", "rb2701", B, O, 3100.0, 1, TD, "2026-09-22 09:30", exchange=Exchange.DCE)])
        with pytest.raises(SettlementError, match="has no SOD"):
            settle(reg, sod, [], cash_movements=[cash("ACC9", TD, CashType.DEPOSIT, "1.00", "c1")])

    def test_bad_facts(self, reg: InstrumentRegistry) -> None:
        tf = TradeFactory()
        sod = sod0({"ACC1": "100000"})
        t = tf("ACC1", "rb2701", B, O, 3100.0, 1, TD, "2026-09-22 09:30")
        with pytest.raises(SettlementError, match="duplicate trade key"):
            settle(reg, sod, [t, t])
        with pytest.raises(SettlementError, match="positive"):
            settle(reg, sod, [], cash_movements=[cash("ACC1", TD, CashType.DEPOSIT, "0.00", "c1")])
        dup = SodSnapshot(trading_day=TD, version=0, accounts=[*sod.accounts, *sod.accounts])
        with pytest.raises(SettlementError, match="twice"):
            settle(reg, dup, [])

    def test_cash_validation(self, reg: InstrumentRegistry) -> None:
        sod = sod0({"ACC1": "100000"})
        twice = [cash("ACC1", TD, CashType.DEPOSIT, "1.00", "c1"), cash("ACC1", TD, CashType.WITHDRAW, "1.00", "c1")]
        with pytest.raises(SettlementError, match="duplicate cash movement id c1"):
            settle(reg, sod, [], cash_movements=twice)
        with pytest.raises(SettlementError, match="not exact to the cent"):
            settle(reg, sod, [], cash_movements=[cash("ACC1", TD, CashType.DEPOSIT, "100.001", "c2")])
        with pytest.raises(SettlementError, match="bad amount"):
            settle(reg, sod, [], cash_movements=[cash("ACC1", TD, CashType.DEPOSIT, "12a", "c3")])
        ok = settle(reg, sod, [], cash_movements=[cash("ACC1", TD, CashType.DEPOSIT, "100.10", "c4")])
        assert ok.accounts["ACC1"].deposit == D("100.10")

    def test_price_validation(self, reg: InstrumentRegistry) -> None:
        tf = TradeFactory()
        sod = sod0({"ACC1": "100000"})
        trades = [tf("ACC1", "rb2701", B, O, 3100.0, 1, TD, "2026-09-22 09:30")]
        with pytest.raises(SettlementError, match="missing settlement price for rb2701"):
            settle(reg, sod, trades, {"IF2612": D("4400")})
        with pytest.raises(SettlementError, match="must be a SettlementPriceView"):
            settle(reg, sod, trades, {"rb2701": D("3120")}, wrap=False)
        view = determine_settlement_price(None, reg, instrument_id="IF2612", trading_day=TD, official=D("4400"))
        with pytest.raises(SettlementError, match="filed under"):
            settle(reg, sod, trades, {"rb2701": view}, wrap=False)
        with pytest.raises(SettlementError, match="must be positive"):
            settle(reg, sod, trades, {"rb2701": D("0")})
        with pytest.raises(SettlementError, match="must be positive"):
            settle(reg, sod, trades, {"rb2701": pv(TD, "rb2701", "3120"), "IF2612": D("-1")}, wrap=False)
        # 临时日初快照等不落库的计算可以显式放宽，裸价格不生成结算价行
        loose = settle(reg, sod, trades, {"rb2701": D("3120")}, wrap=False, require_price_views=False)
        assert loose.result.prices == [] and loose.ok
        # 未持仓、未成交的合约可以给裸价格：只进入下一日 SOD 的昨结算，不生成结算价行
        r = settle(reg, sod, trades, {"rb2701": pv(TD, "rb2701", "3120"), "IF2612": D("4400")}, wrap=False)
        assert [p.instrument_id for p in r.result.prices] == ["rb2701"]
        assert r.next_sod.pre_settle == {"IF2612": "4400", "rb2701": "3120"}

    def test_impossible_close(self, reg: InstrumentRegistry) -> None:
        tf = TradeFactory()
        with pytest.raises(SettlementError, match="REJECT_CLOSE_TODAY_EXCEEDS"):
            settle(reg, sod0({"ACC1": "100000"}), [tf("ACC1", "rb2701", S, CT, 3100.0, 1, TD, "2026-09-22 09:30")])

    def test_previous_settlement_price_is_required(self, reg: InstrumentRegistry) -> None:
        tf = TradeFactory()
        trades = [tf("ACC1", "rb2701", B, O, 3100.0, 1, TD, "2026-09-22 09:30")]
        bare = seed_sod(TD, {"ACC1": "100000"})
        with pytest.raises(SettlementError, match=r"no previous settlement price for rb2701 \(new contract\)"):
            settle(reg, bare, trades)
        r = settle(reg, bare, trades, pre_settle={"rb2701": D("3080")})
        assert r.result.positions[0].pre_settle_price == "3080"
        inp = build_settle_input(reg, "rb2701", TD, [(ts("2026-09-22 09:30"), 3100.0, 1)], pre_settle="3090")
        r = settle(reg, bare, trades, settle_inputs={"rb2701": inp})
        assert r.result.positions[0].pre_settle_price == "3090"
        held = SodSnapshot(
            trading_day=TD, version=1,
            accounts=[SodAccount(account_id="ACC1", pre_balance="1.00", initial_capital="1.00", hwm_equity="1.00",
                                 lots=[SodLot(instrument_id="rb2701", direction=PosDirection.LONG, lot_no=1, open_td="20260921",
                                              open_ts_ns=1, open_price="3000", qty=1, basis_price="3050")])],
        )
        with pytest.raises(SettlementError, match=r"rb2701 \(held yesterday position\)"):
            settle(reg, held, [])


# ---------------------------------------------------------------------------
# Ordering
# ---------------------------------------------------------------------------


class TestOrdering:
    def test_equal_timestamps_keep_input_order(self, reg: InstrumentRegistry) -> None:
        when = ts("2026-09-22 09:30")
        opened = Trade(trading_day=TD, exchange=Exchange.SHFE, trade_id="Z9", direction=B, account_id="ACC1",
                       instrument_id="rb2701", broker_order_ref="r", offset=O, price=3100.0, qty=1, trade_ts_ns=when)
        closed = Trade(trading_day=TD, exchange=Exchange.SHFE, trade_id="A1", direction=S, account_id="ACC1",
                       instrument_id="rb2701", broker_order_ref="r", offset=CT, price=3110.0, qty=1, trade_ts_ns=when)
        r = settle(reg, sod0({"ACC1": "100000"}), [opened, closed])  # 到达顺序：先开后平（trade_id 字典序相反）
        assert [t.trade_id for t in r.result.trades] == ["Z9", "A1"]
        with pytest.raises(SettlementError, match="REJECT_CLOSE_TODAY_EXCEEDS"):
            settle(reg, sod0({"ACC1": "100000"}), [closed, opened])

    def test_idempotent_and_order_independent(self, reg: InstrumentRegistry) -> None:
        tf = TradeFactory()
        trades = [
            tf("ACC2", "IF2612", S, O, 4410.0, 2, TD, "2026-09-22 10:00"),
            tf("ACC1", "rb2701", B, O, 3100.0, 3, TD, "2026-09-22 09:00"),
            tf("ACC1", "rb2701", S, CT, 3105.0, 1, TD, "2026-09-22 09:05"),
            tf("ACC2", "IF2612", B, C, 4405.0, 1, TD, "2026-09-22 11:00"),
        ]
        sod = sod0({"ACC1": "100000", "ACC2": "1000000"})
        a = settle(reg, sod, trades)
        shuffled = list(trades)
        random.Random(7).shuffle(shuffled)
        b = settle(reg, sod, shuffled)
        assert a.result == b.result and a.next_sod == b.next_sod
        assert [t.trade_id for t in a.result.trades] == ["T2", "T3", "T1", "T4"]  # 按成交时间排序
        assert [x.account_id for x in a.result.accounts] == ["ACC1", "ACC2"]
        assert [(d.account_id, d.detail_no) for d in a.result.close_details] == [("ACC1", 1), ("ACC2", 1)]


# ---------------------------------------------------------------------------
# Funds, cash, NAV, drawdown
# ---------------------------------------------------------------------------


class TestFunds:
    def test_cash_movements_do_not_move_nav(self, reg: InstrumentRegistry) -> None:
        tf = TradeFactory()
        sod = sod0({"ACC1": "100000", "ACC2": "50000"})
        trades = [tf("ACC1", "rb2701", B, O, 3100.0, 10, TD, "2026-09-22 09:30")]
        moves = [
            cash("ACC1", TD, CashType.DEPOSIT, "20000.00", "c1"),
            cash("ACC1", TD, CashType.WITHDRAW, "5000.50", "c2"),
            cash("ACC2", TD, CashType.WITHDRAW, "1000.00", "c3"),
        ]
        r = settle(reg, sod, trades, cash_movements=moves)
        a = r.accounts["ACC1"]
        assert (a.deposit, a.withdraw, a.net_deposit) == (D("20000.00"), D("5000.50"), D("14999.50"))
        assert a.position_pnl == D("2000") and a.fee == D("31.00")
        assert a.balance == D("100000") + D("14999.50") + D("2000") - D("31.00") == D("116968.50")
        assert a.static_equity == D("114999.50") and a.day_pnl == D("1969.00")
        assert a.cum_net_deposit == D("14999.50")
        assert a.perf_equity == D("101969.00")
        assert a.nav == D("1.01969000")
        assert a.hwm_equity == D("101969.00") and a.max_drawdown == D("0") == a.hwm_drawdown
        b = r.accounts["ACC2"]
        assert (b.balance, b.perf_equity, b.nav, b.max_drawdown) == (D("49000.00"), D("50000.00"), D("1.00000000"), D("0E-8"))
        sod2 = {x.account_id: x for x in r.next_sod.accounts}
        assert (sod2["ACC1"].pre_balance, sod2["ACC1"].cum_net_deposit, sod2["ACC1"].hwm_equity) == ("116968.50", "14999.50", "101969.00")

    def test_drawdown_from_high_water_mark(self, reg: InstrumentRegistry) -> None:
        sod = SodSnapshot(
            trading_day=TD, version=1,
            accounts=[SodAccount(account_id="ACC1", pre_balance="95000.00", initial_capital="100000.00",
                                 cum_net_deposit="0.00", hwm_equity="110000.00")],
        )
        a = settle(reg, sod, []).accounts["ACC1"]
        assert (a.hwm_equity, a.nav, a.max_drawdown) == (D("110000.00"), D("0.95000000"), D("0.13636364"))

    def test_margin_call_and_risk_cap(self, reg: InstrumentRegistry) -> None:
        tf = TradeFactory()
        trades = [tf("ACC1", "IF2612", B, O, 4400.0, 1, TD, "2026-09-22 10:00")]
        r = settle(reg, sod0({"ACC1": "10000"}), trades, {"IF2612": D("4300")})
        a = r.accounts["ACC1"]
        assert a.equity == D("10000") - D("30000") - D("30.36")  # 亏穿
        assert a.margin == D("154800.00")
        assert a.margin_call == a.margin - a.equity
        assert a.risk_degree == D("9999.99999999")  # DEC_RATE 上限
        assert r.result.accounts[0].risk_degree == "9999.99999999"
        assert r.ok  # 亏穿不是对账失败

    def test_day_roll_with_negative_equity(self, reg: InstrumentRegistry) -> None:
        tf = TradeFactory()
        d1 = settle(reg, sod0({"ACC1": "10000"}), [tf("ACC1", "IF2612", B, O, 4400.0, 1, TD, "2026-09-22 10:00")], {"IF2612": D("4300")})
        sod2 = d1.next_sod
        assert sod2.accounts[0].pre_balance == "-20030.36" and sod2.accounts[0].hwm_equity == "10000.00"
        d2 = settle(reg, sod2, [], {"IF2612": D("4200")}, trading_day=NXT, next_trading_day="20260924")
        a = d2.accounts["ACC1"]
        assert (a.pre_balance, a.position_pnl, a.equity) == (D("-20030.36"), D("-30000.00"), D("-50030.36"))
        assert a.nav == D("-5.00303600") and a.max_drawdown == D("6.00303600")
        assert a.risk_degree == D("9999.99999999") and a.margin_call == D("151200.00") + D("50030.36")  # 4200 × 300 × 12%
        assert d2.ok and d2.next_sod.accounts[0].lots[0].basis_price == "4200"

    def test_flat_position_row(self, reg: InstrumentRegistry) -> None:
        tf = TradeFactory()
        trades = [
            tf("ACC1", "rb2701", B, O, 3100.0, 2, TD, "2026-09-22 09:30"),
            tf("ACC1", "rb2701", S, CT, 3110.0, 2, TD, "2026-09-22 09:31"),
        ]
        r = settle(reg, sod0({"ACC1": "100000"}), trades)
        [p] = r.result.positions
        assert (p.eod_qty, p.avg_open_price, p.margin_gross, p.close_pnl) == (0, None, "0.00", "200.00")
        assert r.result.margin_groups == []
        assert r.next_sod.accounts[0].lots == []

    def test_cffex_treasury_leaves_the_group_inside_settle_day(self, reg: InstrumentRegistry) -> None:
        def run(day: str, nxt: str) -> DaySettlement:
            sod = SodSnapshot(
                trading_day=day, version=1,
                accounts=[SodAccount(account_id="ACC1", pre_balance="1000000.00", initial_capital="1000000.00",
                                     hwm_equity="1000000.00",
                                     lots=[SodLot(instrument_id="T2612", direction=PosDirection.SHORT, lot_no=1, open_td="20261120",
                                                  open_ts_ns=1, open_price="101.5", qty=2, basis_price="101.5"),
                                           SodLot(instrument_id="TF2612", direction=PosDirection.LONG, lot_no=1, open_td="20261120",
                                                  open_ts_ns=1, open_price="102", qty=3, basis_price="102")])],
                pre_settle={"T2612": "101.5", "TF2612": "102"},
            )
            return settle(reg, sod, [], {"T2612": D("101.5"), "TF2612": D("102")}, trading_day=day, next_trading_day=nxt)

        before = run("20261127", "20261130").result.margin_groups[0]
        assert (before.margin_group, before.long_margin, before.short_margin, before.charged_margin) == (
            "CFFEX:TREASURY", "36720.00", "40600.00", "40600.00",
        )
        after = run("20261130", "20261201")  # 交割月前一个交易日收盘后退出
        [g] = after.result.margin_groups
        assert (g.long_margin, g.short_margin, g.excluded_margin, g.charged_margin) == ("0.00", "0.00", "77320.00", "77320.00")
        assert after.ok


# ---------------------------------------------------------------------------
# Checks, outputs
# ---------------------------------------------------------------------------


class TestOutputs:
    def _trades(self) -> list[Trade]:
        tf = TradeFactory()
        return [
            tf("ACC2", "IF2612", S, O, 4410.0, 2, TD, "2026-09-22 10:00"),
            tf("ACC1", "rb2701", B, O, 3100.0, 3, TD, "2026-09-22 09:00"),
            tf("ACC1", "rb2701", S, CT, 3105.0, 1, TD, "2026-09-22 09:05"),
            tf("ACC2", "IF2612", B, C, 4405.0, 1, TD, "2026-09-22 11:00"),
        ]

    def test_checks_and_completed_payload(self, reg: InstrumentRegistry) -> None:
        sod = sod0({"ACC1": "100000", "ACC2": "1000000"})
        inp = build_settle_input(reg, "rb2701", TD, [(ts("2026-09-22 09:00"), 3100.0, 3)], pre_settle="3090")
        view = determine_settlement_price(inp, reg)
        prices = {"rb2701": view, "IF2612": pv(TD, "IF2612", "4400")}
        r = settle(reg, sod, self._trades(), prices, version=2, sod_version=7, wrap=False)
        assert r.ok and not r.failed_checks()
        assert [(c.check, c.account_id) for c in r.checks] == [
            (ReconCheck.EQUITY_IDENTITY, "ACC1"), (ReconCheck.POSITION_IDENTITY, "ACC1"),
            (ReconCheck.EXCH_FORMULA, "ACC1"), (ReconCheck.MTM_VS_FIFO, "ACC1"),
            (ReconCheck.EQUITY_IDENTITY, "ACC2"), (ReconCheck.POSITION_IDENTITY, "ACC2"),
            (ReconCheck.EXCH_FORMULA, "ACC2"), (ReconCheck.MTM_VS_FIFO, "ACC2"),
            (ReconCheck.FIRM_SUM, None),
        ]
        # 结算价行按结算版本盖章（view 原来的版本来自 SettleInput）
        assert [(p.instrument_id, p.version) for p in r.result.prices] == [("IF2612", 2), ("rb2701", 2)]
        assert r.result.prices[1] == msgspec.structs.replace(view, version=2)
        assert r.next_sod.version == 7
        assert r.next_sod.from_settlement is not None and r.next_sod.from_settlement.version == 2
        assert r.next_sod.pre_settle == {"IF2612": "4400", "rb2701": "3100"}
        done = r.completed(ts_ns=123)
        assert (done.version, done.next_trading_day, done.ts_ns) == (2, NXT, 123)
        assert done.price_sources == {SettlePriceSource.OFFICIAL.value: 1, SettlePriceSource.VWAP.value: 1}
        assert [s.account_id for s in done.accounts] == ["ACC1", "ACC2"]

    def test_new_version_rerun_does_not_collide(self, reg: InstrumentRegistry) -> None:
        sod = sod0({"ACC1": "100000", "ACC2": "1000000"})
        v1 = settle(reg, sod, self._trades(), version=1)
        v2 = settle(reg, sod, self._trades(), version=2)
        keys1 = {(p.trading_day, p.instrument_id, p.version) for p in v1.result.prices}
        keys2 = {(p.trading_day, p.instrument_id, p.version) for p in v2.result.prices}
        assert keys1 and not keys1 & keys2

    def test_views_roundtrip_msgpack(self, reg: InstrumentRegistry) -> None:
        r = settle(reg, sod0({"ACC1": "100000", "ACC2": "1000000"}), self._trades())
        assert decode(encode(r.result), SettlementResult) == r.result
        assert decode(encode(r.next_sod), SodSnapshot) == r.next_sod

    def test_inconsistent_sod_basis_fails_checks(self, reg: InstrumentRegistry) -> None:
        sod = SodSnapshot(
            trading_day=TD, version=1,
            accounts=[SodAccount(account_id="ACC1", pre_balance="100000.00", initial_capital="100000.00", hwm_equity="100000.00",
                                 lots=[SodLot(instrument_id="rb2701", direction=PosDirection.LONG, lot_no=1, open_td="20260921",
                                              open_ts_ns=1, open_price="3000", qty=2, basis_price="3050")])],
            pre_settle={"rb2701": "3060"},  # 与 lot 的基准价不一致
        )
        r = settle(reg, sod, [])
        # 交易所公式按昨结算 3060 计算，盯市按 lot 基准 3050：公式校验、结存校验（当日盈亏 = 公式 − 手续费）和公司汇总都失败
        assert {c.check for c in r.failed_checks()} == {ReconCheck.EXCH_FORMULA, ReconCheck.EQUITY_IDENTITY, ReconCheck.FIRM_SUM}
        assert not r.ok and not r.accounts["ACC1"].checks_passed
        assert r.result.accounts[0].checks_passed is False
        detail = next(c for c in r.checks if c.check == ReconCheck.EXCH_FORMULA).detail or {}
        assert detail["diff"] == "200.00"  # (3060 − 3050) × 2 × 10

    def test_equity_identity_reads_the_emitted_rows(self, reg: InstrumentRegistry, monkeypatch: pytest.MonkeyPatch) -> None:
        real = settlement_mod._close_detail_view

        def tampered(*args: Any, **kwargs: Any) -> Any:
            return msgspec.structs.replace(real(*args, **kwargs), close_pnl_mtm="1.00")

        monkeypatch.setattr(settlement_mod, "_close_detail_view", tampered)
        r = settle(reg, sod0({"ACC1": "100000", "ACC2": "1000000"}), self._trades())
        failed = {(c.check, c.account_id) for c in r.failed_checks()}
        assert failed == {(ReconCheck.EQUITY_IDENTITY, "ACC1"), (ReconCheck.EQUITY_IDENTITY, "ACC2")}

    def test_position_identity_reads_the_next_sod(self, reg: InstrumentRegistry, monkeypatch: pytest.MonkeyPatch) -> None:
        real = settlement_mod.PositionBook.sod_lots

        def drop_one(self: Any, account_id: str) -> list[SodLot]:
            lots = real(self, account_id)
            return lots[1:] if account_id == "ACC2" else lots

        monkeypatch.setattr(settlement_mod.PositionBook, "sod_lots", drop_one)
        r = settle(reg, sod0({"ACC1": "100000", "ACC2": "1000000"}), self._trades())
        failed = {(c.check, c.account_id) for c in r.failed_checks()}
        assert failed == {(ReconCheck.POSITION_IDENTITY, "ACC2"), (ReconCheck.FIRM_SUM, None)}

    def test_firm_sum_reads_the_margin_rows(self, reg: InstrumentRegistry, monkeypatch: pytest.MonkeyPatch) -> None:
        real = settlement_mod.MarginGroupView

        def fake(**kwargs: Any) -> Any:
            view = real(**kwargs)
            return msgspec.structs.replace(view, charged_margin="0.00") if view.account_id == "ACC1" else view

        monkeypatch.setattr(settlement_mod, "MarginGroupView", fake)
        r = settle(reg, sod0({"ACC1": "100000", "ACC2": "1000000"}), self._trades())
        failed = {(c.check, c.account_id) for c in r.failed_checks()}
        assert failed == {(ReconCheck.EQUITY_IDENTITY, "ACC1"), (ReconCheck.FIRM_SUM, None)}


class TestForceClose:
    def test_shfe_force_close_spills_into_today_lots(self, reg: InstrumentRegistry) -> None:
        tf = TradeFactory()
        sod = SodSnapshot(
            trading_day=TD, version=1,
            accounts=[SodAccount(account_id="ACC1", pre_balance="100000.00", initial_capital="100000.00", hwm_equity="100000.00",
                                 lots=[SodLot(instrument_id="rb2701", direction=PosDirection.LONG, lot_no=1, open_td="20260921",
                                              open_ts_ns=1, open_price="3000", qty=1, basis_price="3100")])],
            pre_settle={"rb2701": "3100"},
        )
        trades = [
            tf("ACC1", "rb2701", B, O, 3110.0, 2, TD, "2026-09-22 09:00"),
            tf("ACC1", "rb2701", S, Offset.FORCE_CLOSE, 3090.0, 3, TD, "2026-09-22 14:00"),  # 柜台强平
        ]
        r = settle(reg, sod, trades)
        t = r.result.trades[1]
        assert (t.matched_yd_qty, t.matched_today_qty, t.fee_type) == (1, 2, FeeType.CLOSE_TODAY)
        assert t.close_pnl == "-500.00"  # (3090 − 3100) × 10 × 1 + (3090 − 3110) × 10 × 2
        assert r.ok and r.result.positions[0].eod_qty == 0
