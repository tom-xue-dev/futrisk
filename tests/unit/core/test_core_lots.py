"""Unit tests for futrisk.core.lots: lots, close styles, SHFE offsets, frozen closes, day roll."""

from __future__ import annotations

from decimal import Decimal

import pytest
from tests.golden.builders import (
    CT,
    CY,
    FC,
    B,
    C,
    O,
    S,
    TradeFactory,
    inst,
    make_registry,
    margin,
    product,
    rate_fee,
)

from futrisk.common.config.products import ExchangeRules
from futrisk.core.instruments import InstrumentRegistry
from futrisk.core.lots import (
    CloseBucket,
    CloseType,
    Lot,
    Position,
    PositionBook,
    PositionError,
    close_bucket,
    normalize_offset,
    pos_direction,
)
from futrisk.messages import codes
from futrisk.messages.enums import Direction, Offset, PosDirection
from futrisk.messages.eod import SodAccount, SodLot

D = Decimal
TD = "20260923"
PREV = "20260922"


def pos(style: str = "FIFO", explicit: bool = False) -> Position:
    p = Position("ACC1", "X", PosDirection.LONG, TD, close_style=style, explicit_close_today=explicit)
    p.load_lot(Lot(1, PREV, 100, D("100"), 5, D("110")))  # 昨仓，基准 = 昨结算 110
    p.open(3, D("120"), 1_000)
    p.open(2, D("125"), 2_000)
    return p


# ---------------------------------------------------------------------------
# Offsets
# ---------------------------------------------------------------------------


class TestOffsets:
    def test_pos_direction(self) -> None:
        assert pos_direction(Direction.BUY, Offset.OPEN) == PosDirection.LONG
        assert pos_direction(Direction.SELL, Offset.OPEN) == PosDirection.SHORT
        assert pos_direction(Direction.SELL, Offset.CLOSE_TODAY) == PosDirection.LONG
        assert pos_direction(Direction.BUY, Offset.FORCE_CLOSE) == PosDirection.SHORT

    def test_normalize_offset(self) -> None:
        shfe = ExchangeRules(explicit_close_today=True)
        other = ExchangeRules()
        assert normalize_offset(shfe, Offset.CLOSE) == Offset.CLOSE_YESTERDAY  # CTP：SHFE 的 CLOSE = 平昨
        assert normalize_offset(shfe, Offset.CLOSE_TODAY) == Offset.CLOSE_TODAY
        assert normalize_offset(other, Offset.CLOSE_TODAY) == Offset.CLOSE
        assert normalize_offset(other, Offset.CLOSE_YESTERDAY) == Offset.CLOSE
        assert normalize_offset(other, Offset.FORCE_CLOSE) == Offset.FORCE_CLOSE
        assert normalize_offset(shfe, Offset.OPEN) == Offset.OPEN

    def test_close_bucket(self) -> None:
        shfe = ExchangeRules(explicit_close_today=True)
        assert close_bucket(shfe, Offset.CLOSE_TODAY) == CloseBucket.TODAY
        assert close_bucket(shfe, Offset.CLOSE) == CloseBucket.YD
        assert close_bucket(shfe, Offset.FORCE_CLOSE) == CloseBucket.YD
        assert close_bucket(ExchangeRules(), Offset.CLOSE_TODAY) == CloseBucket.ANY
        with pytest.raises(ValueError):
            close_bucket(shfe, Offset.OPEN)


# ---------------------------------------------------------------------------
# Position
# ---------------------------------------------------------------------------


class TestPosition:
    def test_quantities_and_averages(self) -> None:
        p = pos()
        assert (p.qty, p.td_qty, p.yd_qty) == (10, 5, 5)
        assert p.avg_open_price() == D("111")  # (500 + 360 + 250) / 10
        assert p.basis_avg_price() == D("116")  # (550 + 360 + 250) / 10
        assert [lot.lot_no for lot in p.lots] == [1, 2, 3]

    def test_fifo_consumes_yesterday_first(self) -> None:
        p = pos("FIFO")
        slices = p.close(Offset.CLOSE, 6)
        assert [(s.lot_no, s.close_type, s.qty, s.basis_price) for s in slices] == [
            (1, CloseType.YD, 5, D("110")),
            (2, CloseType.TODAY, 1, D("120")),
        ]
        assert (p.td_qty, p.yd_qty) == (4, 0)

    def test_today_first(self) -> None:
        p = pos("TODAY_FIRST")
        slices = p.close(Offset.CLOSE, 6)
        assert [(s.lot_no, s.close_type, s.qty) for s in slices] == [(2, CloseType.TODAY, 3), (3, CloseType.TODAY, 2), (1, CloseType.YD, 1)]

    def test_shfe_explicit_buckets(self) -> None:
        p = pos(explicit=True)
        assert [s.lot_no for s in p.close(Offset.CLOSE_TODAY, 4)] == [2, 3]
        assert [s.close_type for s in p.close(Offset.CLOSE, 2)] == [CloseType.YD]
        with pytest.raises(PositionError) as e:
            p.close(Offset.CLOSE_TODAY, 2)
        assert e.value.code == codes.REJECT_CLOSE_TODAY_EXCEEDS and e.value.available == 1
        with pytest.raises(PositionError) as e:
            p.close(Offset.CLOSE_YESTERDAY, 4)
        assert e.value.code == codes.REJECT_CLOSE_YD_EXCEEDS
        # FORCE_CLOSE 按 CLOSE（先平昨）处理；柜台强平是既成事实，昨仓不足时继续平今
        slices = p.close(Offset.FORCE_CLOSE, 4)
        assert [(s.close_type, s.qty) for s in slices] == [(CloseType.YD, 3), (CloseType.TODAY, 1)]
        assert p.check_close(Offset.FORCE_CLOSE, 1) == codes.REJECT_CLOSE_YD_EXCEEDS  # 不能用于报单

    def test_lenient_close_spills_into_other_bucket(self) -> None:
        p = pos(explicit=True)
        slices = p.close(Offset.CLOSE_TODAY, 7, lenient=True)
        assert [(s.close_type, s.qty) for s in slices] == [(CloseType.TODAY, 3), (CloseType.TODAY, 2), (CloseType.YD, 2)]
        with pytest.raises(PositionError) as e:
            p.close(Offset.FORCE_CLOSE, 4)
        assert e.value.code == codes.REJECT_CLOSE_EXCEEDS_POSITION

    def test_preview_close_does_not_mutate(self) -> None:
        assert pos("FIFO").preview_close(Offset.CLOSE, 6) == (1, 5)
        assert pos("TODAY_FIRST").preview_close(Offset.CLOSE, 6) == (5, 1)
        shfe = pos(explicit=True)
        assert shfe.preview_close(Offset.CLOSE_YESTERDAY, 3) == (0, 3)
        assert shfe.preview_close(Offset.CLOSE_TODAY, 9) == (5, 0)  # 以持有量为上限
        assert shfe.preview_close(Offset.FORCE_CLOSE, 7) == (2, 5)
        assert (shfe.td_qty, shfe.yd_qty) == (5, 5)

    def test_non_explicit_exceeds(self) -> None:
        p = pos()
        with pytest.raises(PositionError) as e:
            p.close(Offset.CLOSE, 11)
        assert e.value.code == codes.REJECT_CLOSE_EXCEEDS_POSITION
        assert p.qty == 10  # 失败时不改变持仓

    def test_invalid_quantities(self) -> None:
        p = pos()
        with pytest.raises(ValueError):
            p.open(0, D("1"), 0)
        with pytest.raises(ValueError):
            p.close(Offset.CLOSE, 0)
        with pytest.raises(ValueError):
            p.close(Offset.OPEN, 1)
        assert p.check_close(Offset.CLOSE, 0) == codes.REJECT_INVALID_QTY

    def test_frozen_closes_shfe(self) -> None:
        p = pos(explicit=True)
        p.freeze_close(Offset.CLOSE_TODAY, 4)
        c = p.closable()
        assert (c.total, c.today, c.yd) == (6, 1, 5)
        assert p.check_close(Offset.CLOSE_TODAY, 2) == codes.REJECT_CLOSE_TODAY_EXCEEDS
        assert p.check_close(Offset.CLOSE_YESTERDAY, 5) is None
        with pytest.raises(PositionError):
            p.freeze_close(Offset.CLOSE_TODAY, 2)
        p.unfreeze_close(Offset.CLOSE_TODAY, 4)
        assert p.closable().today == 5

    @pytest.mark.parametrize(("style", "today", "yd"), [("FIFO", 3, 0), ("TODAY_FIRST", 0, 3)])
    def test_frozen_closes_follow_close_style(self, style: str, today: int, yd: int) -> None:
        p = pos(style)
        p.freeze_close(Offset.CLOSE, 7)
        c = p.closable()
        assert (c.total, c.today, c.yd) == (3, today, yd)
        assert p.check_close(Offset.CLOSE, 4) == codes.REJECT_CLOSE_EXCEEDS_POSITION
        p.unfreeze_close(Offset.CLOSE, 100)
        assert p.closable().total == 10

    def test_roll(self) -> None:
        p = pos()
        p.freeze_close(Offset.CLOSE, 2)
        p.roll("20260924", D("130"))
        assert p.trading_day == "20260924"
        assert (p.td_qty, p.yd_qty) == (0, 10)
        assert {lot.basis_price for lot in p.lots} == {D("130")}
        assert {lot.open_price for lot in p.lots} == {D("100"), D("120"), D("125")}
        assert p.closable().total == 10  # GFD 在途平仓不跨日

    def test_fifo_orders_by_open_day_before_timestamp(self) -> None:
        # 手工建账的昨仓时间戳晚于今天的成交，FIFO 仍然先平昨仓
        p = Position("ACC1", "X", PosDirection.LONG, TD)
        p.open(2, D("120"), 1_000)
        p.load_lot(Lot(9, PREV, 10**19, D("100"), 3, D("110")))
        assert [lot.lot_no for lot in p.lots] == [9, 1]  # 昨仓在前，与时间戳无关
        slices = p.close(Offset.CLOSE, 4)
        assert [(s.close_type, s.qty) for s in slices] == [(CloseType.YD, 3), (CloseType.TODAY, 1)]

    def test_load_lot_keeps_order_and_numbering(self) -> None:
        p = Position("ACC1", "X", PosDirection.SHORT, TD)
        p.load_lot(Lot(7, PREV, 300, D("1"), 1, D("1")))
        p.load_lot(Lot(3, PREV, 100, D("1"), 1, D("1")))
        assert [lot.lot_no for lot in p.lots] == [3, 7]
        assert p.open(1, D("2"), 400).lot_no == 8
        with pytest.raises(ValueError):
            p.load_lot(Lot(9, PREV, 1, D("1"), 0, D("1")))


# ---------------------------------------------------------------------------
# PositionBook
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def reg() -> InstrumentRegistry:
    return make_registry(
        {
            "rb": product("SHFE", multiplier=10, tick="1", fees=[rate_fee("0.0001")], margins=[margin("0.07")]),
            "IF": product("CFFEX", multiplier=300, tick="0.2", fees=[rate_fee("0.000023")], margins=[margin("0.12")],
                          physical_delivery=False, big_side_group="CFFEX:INDEX"),
        },
        [inst("rb2701", "rb", "20270115"), inst("IF2612", "IF", "20261218")],
    )


class TestPositionBook:
    def test_apply_trades_and_roll(self, reg: InstrumentRegistry) -> None:
        tf = TradeFactory()
        book = PositionBook(reg, TD)
        book.load_sod_account(
            SodAccount(
                account_id="ACC1", pre_balance="0", initial_capital="0", hwm_equity="0",
                lots=[SodLot(instrument_id="rb2701", direction=PosDirection.SHORT, lot_no=4, open_td=PREV,
                             open_ts_ns=1, open_price="3000", qty=3, basis_price="3050")],
            )
        )
        r1 = book.apply_trade(tf("ACC1", "rb2701", B, O, 3060.0, 2, TD, "2026-09-23 09:30"))
        assert r1.is_open and r1.lot is not None and r1.lot.lot_no == 1 and r1.pos_direction == PosDirection.LONG
        r2 = book.apply_trade(tf("ACC1", "rb2701", B, C, 3055.0, 2, TD, "2026-09-23 09:31"))  # 平空（昨仓）
        assert r2.pos_direction == PosDirection.SHORT and (r2.close_today_qty, r2.close_yd_qty) == (0, 2)
        assert book.check_close("ACC1", "rb2701", B, CT, 1) == codes.REJECT_CLOSE_TODAY_EXCEEDS
        assert book.check_close("ACC1", "rb2701", S, CT, 2) is None
        assert book.check_close("ACC1", "IF2612", S, C, 1) == codes.REJECT_CLOSE_EXCEEDS_POSITION
        assert book.check_close("ACC1", "rb2701", S, CY, 1) == codes.REJECT_CLOSE_YD_EXCEEDS
        book.apply_trade(tf("ACC1", "rb2701", B, FC, 3055.0, 1, TD, "2026-09-23 09:32"))
        assert [p.key for p in book.positions()] == [("ACC1", "rb2701", PosDirection.LONG), ("ACC1", "rb2701", PosDirection.SHORT)]
        with pytest.raises(KeyError):
            book.roll("20260924", {})
        book.roll("20260924", {"rb2701": D("3070")})
        assert book.trading_day == "20260924"
        assert [p.key[2] for p in book.positions()] == [PosDirection.LONG]  # 平掉的空头被清理
        [lot] = book.sod_lots("ACC1")
        assert (lot.lot_no, lot.open_price, lot.basis_price, lot.open_td, lot.qty) == (1, "3060", "3070", TD, 2)

    def test_sod_lot_must_predate_trading_day(self, reg: InstrumentRegistry) -> None:
        book = PositionBook(reg, TD)
        with pytest.raises(ValueError):
            book.load_sod_account(
                SodAccount(
                    account_id="ACC1", pre_balance="0", initial_capital="0", hwm_equity="0",
                    lots=[SodLot(instrument_id="rb2701", direction=PosDirection.LONG, lot_no=1, open_td=TD,
                                 open_ts_ns=1, open_price="1", qty=1, basis_price="1")],
                )
            )

    def test_night_fill_is_a_today_lot(self, reg: InstrumentRegistry) -> None:
        tf = TradeFactory()
        book = PositionBook(reg, TD)
        book.apply_trade(tf("ACC1", "rb2701", B, O, 3060.0, 2, TD, "2026-09-22 21:05"))  # 夜盘归属 TD
        assert book.check_close("ACC1", "rb2701", S, CT, 2) is None
        assert book.check_close("ACC1", "rb2701", S, CY, 1) == codes.REJECT_CLOSE_YD_EXCEEDS
