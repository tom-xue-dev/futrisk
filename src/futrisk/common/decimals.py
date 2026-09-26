"""Exact decimal helpers for clearing (README §3.4, 02 §4.3 ``DecStr``).

Rules:
- floats coming from market data or the broker are converted with ``Decimal(str(x))``;
- money is quantized to 0.01 with ROUND_HALF_UP, fees per trade;
- ``DecStr`` never uses exponents or a leading ``+``.
"""

from __future__ import annotations

from decimal import ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_UP, Decimal, InvalidOperation

MONEY = Decimal("0.01")
ZERO = Decimal("0")
ONE = Decimal("1")


def D(value: Decimal | str | int | float | None) -> Decimal:
    """Convert to Decimal; floats go through ``str`` so 0.1 stays 0.1."""
    if value is None:
        raise ValueError("cannot convert None to Decimal")
    if isinstance(value, Decimal):
        return value
    if isinstance(value, float):
        return Decimal(str(value))
    try:
        return Decimal(value)
    except InvalidOperation as exc:  # pragma: no cover - defensive
        raise ValueError(f"invalid decimal: {value!r}") from exc


def dec_str(value: Decimal | str | int | float) -> str:
    """Canonical DecStr: plain notation, no exponent, no '+', no negative zero."""
    d = D(value)
    if d == 0:
        d = abs(d)
    s = format(d, "f")
    return s


def dec_str_strip(value: Decimal | str | int | float) -> str:
    """DecStr with trailing zeros removed (used by trade digests, 02 §7.5)."""
    d = D(value)
    if d == 0:
        return "0"
    s = format(d.normalize(), "f")
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s


def q_money(value: Decimal | str | int | float) -> Decimal:
    """Quantize to 0.01 yuan, ROUND_HALF_UP."""
    return D(value).quantize(MONEY, rounding=ROUND_HALF_UP)


def floor_to_tick(price: Decimal, tick: Decimal) -> Decimal:
    return (price / tick).to_integral_value(rounding=ROUND_FLOOR) * tick


def ceil_to_tick(price: Decimal, tick: Decimal) -> Decimal:
    return (price / tick).to_integral_value(rounding=ROUND_CEILING) * tick


def round_to_tick(price: Decimal, tick: Decimal) -> Decimal:
    return (price / tick).to_integral_value(rounding=ROUND_HALF_UP) * tick


def on_tick(price: Decimal | float, tick: Decimal) -> bool:
    """True when ``price`` lies exactly on the tick grid."""
    p = D(price)
    q = p / tick
    return q == q.to_integral_value()


def limit_prices(pre_settle: Decimal, limit_pct: Decimal, tick: Decimal) -> tuple[Decimal, Decimal]:
    """Upper/lower limit prices rounded *inward* to the tick (CFFEX official rule)."""
    upper = floor_to_tick(pre_settle * (ONE + limit_pct), tick)
    lower = ceil_to_tick(pre_settle * (ONE - limit_pct), tick)
    return upper, lower
