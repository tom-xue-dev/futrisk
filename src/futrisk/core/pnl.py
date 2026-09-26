"""Mark-to-market and per-trade (FIFO) P&L formulas (01 §8.15 逐日盯市, 03 §4.4).

Mark-to-market (逐日盯市):

- close P&L: today lot (close − open), yesterday lot (close − pre_settle);
- position P&L: today lot (settle − open), yesterday lot (settle − pre_settle);

each × multiplier × qty × side (+1 long, −1 short). Because a today lot's basis
is its open price and a yesterday lot's basis is the previous settlement price
(:mod:`futrisk.core.lots`), both reduce to "price − lot.basis_price".

Per-trade (逐笔对冲 / FIFO) P&L always uses the open price.

The exchange formula (01 §8.15 第二道校验) is independent of open/close flags:

    [Σ(sell − S)·sell_qty + Σ(S − buy)·buy_qty + (P − S)·(yd_short − yd_long)] × multiplier

per account, contract and day, where S is today's and P the previous settlement
price and yd_* the start-of-day quantities. It must equal close + position P&L.
"""

from __future__ import annotations

from collections.abc import Iterable
from decimal import Decimal

from futrisk.core.lots import CloseSlice, CloseType, Lot, direction_sign
from futrisk.messages.enums import Direction, PosDirection


def close_pnl(direction: PosDirection, close_price: Decimal, basis_price: Decimal, qty: int, multiplier: int) -> Decimal:
    """(close − basis) × multiplier × qty × side."""
    return (close_price - basis_price) * multiplier * qty * direction_sign(direction)


def mtm_close_pnl(direction: PosDirection, piece: CloseSlice, close_price: Decimal, multiplier: int) -> Decimal:
    """Mark-to-market close P&L of one slice: 平今 vs open price, 平昨 vs pre-settlement (the lot basis)."""
    basis = piece.lot_open_price if piece.close_type == CloseType.TODAY else piece.basis_price
    return close_pnl(direction, close_price, basis, piece.qty, multiplier)


def fifo_close_pnl(direction: PosDirection, piece: CloseSlice, close_price: Decimal, multiplier: int) -> Decimal:
    """Per-trade (FIFO) close P&L of one slice: always vs the lot's open price."""
    return close_pnl(direction, close_price, piece.lot_open_price, piece.qty, multiplier)


def position_pnl(direction: PosDirection, lots: Iterable[Lot], settle_price: Decimal, multiplier: int) -> Decimal:
    """持仓盯市盈亏 of remaining lots: Σ (settle − basis) × multiplier × qty × side."""
    sign = direction_sign(direction)
    return sum(((settle_price - lot.basis_price) * lot.qty for lot in lots), Decimal(0)) * multiplier * sign


def fifo_float_pnl(direction: PosDirection, lots: Iterable[Lot], price: Decimal, multiplier: int) -> Decimal:
    """逐笔浮动盈亏 of remaining lots: Σ (price − open) × multiplier × qty × side."""
    sign = direction_sign(direction)
    return sum(((price - lot.open_price) * lot.qty for lot in lots), Decimal(0)) * multiplier * sign


def exchange_formula_pnl(
    fills: Iterable[tuple[Direction, Decimal, int]],
    *,
    settle: Decimal,
    pre_settle: Decimal,
    yd_long: int,
    yd_short: int,
    multiplier: int,
) -> Decimal:
    """Exchange-formula day P&L of one account and contract (offset-independent oracle).

    ``fills`` are ``(direction, price, qty)`` of the day's trades; ``yd_long`` /
    ``yd_short`` are the start-of-day long / short quantities.
    """
    total = Decimal(0)
    for direction, price, qty in fills:
        if direction == Direction.SELL:
            total += (price - settle) * qty
        else:
            total += (settle - price) * qty
    total += (pre_settle - settle) * (yd_short - yd_long)
    return total * multiplier


__all__ = [
    "close_pnl",
    "exchange_formula_pnl",
    "fifo_close_pnl",
    "fifo_float_pnl",
    "mtm_close_pnl",
    "position_pnl",
]
