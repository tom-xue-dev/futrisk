"""Decimal helpers private to the clearing core (01 §8.3, README §3.4, 02 §4.3).

- clearing arithmetic runs inside :data:`CLEARING_CTX` so that results never
  depend on a caller's global ``decimal`` context;
- money is quantized to 0.01 ROUND_HALF_UP (``DEC_MONEY``), exact ratios to 1e-8
  (``DEC_RATE``, 03 §3.1);
- prices are rendered as canonical ``DecStr`` without trailing zeros, money
  always with two decimals.
"""

from __future__ import annotations

from decimal import ROUND_HALF_UP, Context, Decimal

from futrisk.common.decimals import dec_str, dec_str_strip, q_money

# 34 significant digits (IEEE decimal128) is far beyond any price * multiplier * qty.
CLEARING_CTX = Context(prec=34, rounding=ROUND_HALF_UP)

RATE_QUANTUM = Decimal("0.00000001")
PRICE_QUANTUM = Decimal("0.000001")
ZERO = Decimal("0")
# DEC_RATE is NUMERIC(12,8): the largest representable ratio.
RATE_CAP = Decimal("9999.99999999")


def div(a: Decimal, b: Decimal) -> Decimal:
    """Exact-as-possible division in the clearing context (34 digits)."""
    return CLEARING_CTX.divide(a, b)


def q_rate(x: Decimal) -> Decimal:
    """Quantize an exact ratio to 1e-8, ROUND_HALF_UP (``DEC_RATE``)."""
    return x.quantize(RATE_QUANTUM, rounding=ROUND_HALF_UP, context=CLEARING_CTX)


def money_str(x: Decimal) -> str:
    """``DEC_MONEY`` DecStr with exactly two decimals, e.g. ``"-2560.00"``."""
    return dec_str(q_money(x))


def rate_str(x: Decimal) -> str:
    """``DEC_RATE`` DecStr with eight decimals, e.g. ``"0.12926604"``."""
    return dec_str(q_rate(x))


def price_str(x: Decimal) -> str:
    """``DEC_PRICE`` DecStr without trailing zeros, e.g. ``"4414.8"``, ``"3120"``."""
    return dec_str_strip(x)


def ratio(num: Decimal, den: Decimal) -> Decimal:
    """``num / den`` quantized to ``DEC_RATE``; 0 when ``num`` is 0, capped when ``den <= 0``.

    Used for risk degree, NAV and drawdown. A non-positive denominator with a
    positive numerator (e.g. margin held against negative equity) is capped at
    :data:`RATE_CAP` because ``DEC_RATE`` cannot hold infinity; ``margin_call``
    carries the real signal in that case.
    """
    if num == 0:
        return q_rate(ZERO)
    if den <= 0:
        return RATE_CAP if num > 0 else -RATE_CAP
    return min(q_rate(div(num, den)), RATE_CAP)


__all__ = [
    "CLEARING_CTX",
    "PRICE_QUANTUM",
    "RATE_CAP",
    "RATE_QUANTUM",
    "ZERO",
    "div",
    "money_str",
    "price_str",
    "q_money",
    "q_rate",
    "rate_str",
    "ratio",
]
