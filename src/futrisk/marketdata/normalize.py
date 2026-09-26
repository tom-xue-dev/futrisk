"""Normalization helpers shared by all market-data adapters (02 §7.1, 01 §8.9, docs/README §3.3).

Every adapter (Feed A, Feed B, Sina) turns a vendor record into the canonical
:class:`~futrisk.messages.md.Tick`. These helpers keep the rules identical:

- instrument ids use the exchange's original case: SHFE/DCE/INE/GFEX lower
  case, CFFEX/CZCE upper case (``rb2701``, ``IF2612``); the product master in
  ``products.yaml`` decides;
- missing limit prices are derived from the pre-settlement price as
  ``pre_settle * (1 ± limit_pct)`` rounded **inward** to the tick
  (02 §7.1 ``upper_limit`` / ``lower_limit``), where ``limit_pct`` is the
  percentage effective on the tick's trading day
  (``ProductSpec.limit_pct_on``: exchanges widen limits before long holidays);
- non-positive or non-finite optional prices mean "no value" and map to ``None``
  (CTP uses ``DBL_MAX``, Sina uses ``0``);
- ``LIMIT_UP_LOCKED`` / ``LIMIT_DOWN_LOCKED`` (02 §7.1 ``flags`` bit2/bit3)
  describe a one-sided book pinned at the limit.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass, field
from decimal import Decimal

from futrisk.common.config.products import ProductsConfig, ProductSpec, product_code
from futrisk.common.decimals import D, limit_prices
from futrisk.messages.enums import Exchange
from futrisk.messages.md import FLAG_LIMIT_DOWN_LOCKED, FLAG_LIMIT_UP_LOCKED

_ID_RE = re.compile(r"^([A-Za-z]+)(\d+)$")
_PRICE_EPS = 1e-6


@dataclass(frozen=True, slots=True)
class InstrumentMeta:
    """Static contract data an adapter needs to build a :class:`Tick`.

    ``limit_pct`` is the product's base limit; use :meth:`limit_pct_on` for a
    given trading day, which follows the product's ``limit_schedule``.
    """

    instrument_id: str
    product: str
    exchange: Exchange
    multiplier: int
    tick_size: Decimal
    limit_pct: Decimal
    sessions: tuple[str, ...]
    spec: ProductSpec | None = field(default=None, compare=False, repr=False)

    def limit_pct_on(self, trading_day: str | None) -> Decimal:
        """Limit percentage effective on ``trading_day`` (``ProductSpec.limit_pct_on``)."""
        return self.spec.limit_pct_on(trading_day) if self.spec is not None else self.limit_pct


def canonical_instrument_id(products: ProductsConfig, raw: str) -> str:
    """Restore the exchange case of a contract code: ``RB2701`` -> ``rb2701``, ``if2612`` -> ``IF2612``.

    Raises ``ValueError`` for malformed codes or products missing from ``products.yaml``.
    """
    m = _ID_RE.match(raw.strip())
    if m is None:
        raise ValueError(f"bad instrument id {raw!r}")
    letters, digits = m.groups()
    for code in products.products:
        if code.lower() == letters.lower():
            return f"{code}{digits}"
    raise ValueError(f"unknown product {letters!r} in {raw!r} (not in products.yaml)")


def instrument_meta(products: ProductsConfig, instrument_id: str) -> InstrumentMeta:
    """Build :class:`InstrumentMeta` from ``products.yaml`` (instrument need not be listed)."""
    iid = canonical_instrument_id(products, instrument_id)
    inst = products.instrument(iid)
    product = inst.product if inst is not None else product_code(iid)
    spec = products.products[product]
    return InstrumentMeta(
        instrument_id=iid,
        product=product,
        exchange=spec.exchange,
        multiplier=spec.multiplier,
        tick_size=spec.tick_size,
        limit_pct=spec.limit_pct,
        sessions=tuple(spec.sessions),
        spec=spec,
    )


def derive_limits(pre_settle: float, limit_pct: Decimal, tick_size: Decimal) -> tuple[float, float]:
    """``(upper, lower)`` from the pre-settlement price, rounded inward to the tick."""
    upper, lower = limit_prices(D(pre_settle), limit_pct, tick_size)
    return float(upper), float(lower)


def positive_or_none(value: float | None) -> float | None:
    """``value`` if it is a finite positive price, else ``None``."""
    if value is None or not math.isfinite(value) or value <= 0:
        return None
    return float(value)


def limit_lock_flags(bid1: float | None, ask1: float | None, upper_limit: float, lower_limit: float) -> int:
    """``FLAG_LIMIT_UP_LOCKED`` when bids sit at the upper limit with no offers; symmetric for down."""
    flags = 0
    if upper_limit > 0 and ask1 is None and bid1 is not None and bid1 >= upper_limit - _PRICE_EPS:
        flags |= FLAG_LIMIT_UP_LOCKED
    if lower_limit > 0 and bid1 is None and ask1 is not None and ask1 <= lower_limit + _PRICE_EPS:
        flags |= FLAG_LIMIT_DOWN_LOCKED
    return flags
