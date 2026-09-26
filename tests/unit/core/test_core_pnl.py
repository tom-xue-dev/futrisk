"""Unit tests for futrisk.core.pnl: MTM, FIFO and exchange-formula P&L (01 §8.15)."""

from __future__ import annotations

from decimal import Decimal

from futrisk.core.lots import CloseSlice, CloseType, Lot
from futrisk.core.pnl import (
    close_pnl,
    exchange_formula_pnl,
    fifo_close_pnl,
    fifo_float_pnl,
    mtm_close_pnl,
    position_pnl,
)
from futrisk.messages.enums import Direction, PosDirection

D = Decimal
L, SH = PosDirection.LONG, PosDirection.SHORT


def piece(close_type: CloseType, open_price: str, basis: str, qty: int) -> CloseSlice:
    return CloseSlice(1, close_type, "20260922", 0, D(open_price), D(basis), qty)


class TestPnl:
    def test_close_pnl_sign(self) -> None:
        assert close_pnl(L, D("3130"), D("3100"), 3, 10) == D("900")
        assert close_pnl(SH, D("3130"), D("3100"), 3, 10) == D("-900")

    def test_mtm_today_vs_yesterday(self) -> None:
        # 平今按开仓价，平昨按昨结算
        assert mtm_close_pnl(L, piece(CloseType.TODAY, "3115", "3115", 2), D("3135"), 10) == D("400")
        assert mtm_close_pnl(L, piece(CloseType.YD, "3100", "3120", 6), D("3140"), 10) == D("1200")
        assert fifo_close_pnl(L, piece(CloseType.YD, "3100", "3120", 6), D("3140"), 10) == D("2400")

    def test_position_and_fifo_float(self) -> None:
        lots = [Lot(1, "20260921", 0, D("3100"), 1, D("3120")), Lot(2, "20260922", 1, D("3115"), 2, D("3115"))]
        assert position_pnl(L, lots, D("3132"), 10) == D("120") + D("340")
        assert fifo_float_pnl(L, lots, D("3132"), 10) == D("320") + D("340")
        assert position_pnl(SH, lots, D("3132"), 10) == D("-460")

    def test_exchange_formula_case1_t2(self) -> None:
        fills = [(Direction.BUY, D("3115"), 4), (Direction.SELL, D("3140"), 6), (Direction.SELL, D("3135"), 2)]
        assert exchange_formula_pnl(fills, settle=D("3132"), pre_settle=D("3120"), yd_long=12, yd_short=0, multiplier=10) == D("2660")

    def test_exchange_formula_short_side(self) -> None:
        # 昨空 10 手：P=3111, S=3100 -> (3111 − 3100) × 10 × 10
        assert exchange_formula_pnl([], settle=D("3100"), pre_settle=D("3111"), yd_long=0, yd_short=10, multiplier=10) == D("1100")
