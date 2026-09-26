"""Contract parameters and order pricing helpers for strategies (01 §8.9, §8.14; 02 §7.1, §11.2).

Everything a strategy needs to turn a decision into a valid limit order:

- :class:`ContractTable` resolves ``products.yaml`` per contract (multiplier, tick,
  exchange close rules, max order size, limit band, product exposure cap);
- :func:`aggressive_price` prices a "market-like" limit order: the opposite best quote
  plus/minus ``n`` ticks, on the tick grid and inside the daily price limits (02 §11.2
  step 1 ``REJECT_PRICE_OFF_TICK``, step 3 ``REJECT_PRICE_LIMIT``/``REJECT_FAT_FINGER``);
- :func:`passive_price` joins the best quote on the own side.

Prices are computed with :class:`~decimal.Decimal` from ``str(float)`` (02 §4.3) so
that e.g. ``4414.8 + 2 * 0.2`` stays exactly on the IF tick grid.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_UP, Decimal

from futrisk.common.config.products import ProductsConfig, ProductSpec, product_code
from futrisk.messages.enums import Direction, Exchange
from futrisk.messages.md import Tick

#: 01 §8.14 单品种净名义上限 = 3% ÷ 涨跌停幅度（一个不利涨跌停的亏损不超过权益的 3%）
CAP_LOSS_FRAC = 0.03

_ROUNDING = {"nearest": ROUND_HALF_UP, "up": ROUND_CEILING, "down": ROUND_FLOOR}


@dataclass(frozen=True, slots=True)
class ContractInfo:
    """Resolved, day-independent parameters of one contract (from ``products.yaml``)."""

    instrument_id: str
    product: str
    exchange: Exchange
    sector: str
    multiplier: int
    tick_size: Decimal
    explicit_close_today: bool
    close_style: str
    max_limit_qty: int
    spec: ProductSpec

    @property
    def tick(self) -> float:
        return float(self.tick_size)

    def notional(self, price: float, lots: int = 1) -> float:
        """Contract value of ``lots`` at ``price`` (yuan, always >= 0)."""
        if self.spec.notional_face is not None:
            # 国债期货：合约价值 = 价格 × 面值 / 100（products.yaml notional_face）
            return abs(lots) * price * float(self.spec.notional_face) / 100.0
        return abs(lots) * price * self.multiplier

    def limit_pct(self, trading_day: str | None = None) -> float:
        """Daily price limit in force on ``trading_day`` (e.g. 0.05 for rb)."""
        return float(self.spec.limit_pct_on(trading_day))

    def cap_frac(self, trading_day: str | None = None) -> float:
        """Product net-notional cap as a fraction of equity: ``0.03 / limit_pct`` (01 §8.14)."""
        pct = self.limit_pct(trading_day)
        return CAP_LOSS_FRAC / pct if pct > 0 else float("inf")


class ContractTable:
    """Contract lookup with a small cache; unknown products raise ``KeyError``."""

    def __init__(self, products: ProductsConfig) -> None:
        self._products = products
        self._cache: dict[str, ContractInfo] = {}

    @property
    def products(self) -> ProductsConfig:
        return self._products

    def has(self, instrument_id: str) -> bool:
        try:
            self.get(instrument_id)
        except (KeyError, ValueError):
            return False
        return True

    def get(self, instrument_id: str) -> ContractInfo:
        info = self._cache.get(instrument_id)
        if info is not None:
            return info
        inst = self._products.instrument(instrument_id)
        code = inst.product if inst is not None else product_code(instrument_id)
        spec = self._products.products.get(code)
        if spec is None:
            raise KeyError(f"unknown product for instrument {instrument_id!r}")
        rules = self._products.exchanges[spec.exchange]
        info = ContractInfo(
            instrument_id=instrument_id,
            product=code,
            exchange=spec.exchange,
            sector=spec.sector,
            multiplier=spec.multiplier,
            tick_size=spec.tick_size,
            explicit_close_today=rules.explicit_close_today,
            close_style=rules.close_style,
            max_limit_qty=int(spec.max_order_qty.get("LIMIT", 1000)),
            spec=spec,
        )
        self._cache[instrument_id] = info
        return info


def to_tick(price: float, tick_size: Decimal, mode: str = "nearest") -> float:
    """Round ``price`` to the tick grid: ``nearest`` (half up), ``up`` or ``down``."""
    q = (Decimal(str(price)) / tick_size).to_integral_value(rounding=_ROUNDING[mode])
    return float(q * tick_size)


def on_tick_grid(price: float, tick_size: Decimal) -> bool:
    q = Decimal(str(price)) / tick_size
    return q == q.to_integral_value()


def _clamp(price: Decimal, tick: Tick, tick_size: Decimal) -> Decimal:
    """Clamp into the daily limits, the limits themselves taken inward onto the tick grid."""
    upper = (Decimal(str(tick.upper_limit)) / tick_size).to_integral_value(rounding=ROUND_FLOOR) * tick_size
    lower = (Decimal(str(tick.lower_limit)) / tick_size).to_integral_value(rounding=ROUND_CEILING) * tick_size
    if upper > 0 and price > upper:
        price = upper
    if lower > 0 and price < lower:
        price = lower
    return price


def aggressive_price(tick: Tick, direction: Direction, ticks: int, tick_size: Decimal) -> float | None:
    """Marketable limit price: opposite best ± ``ticks`` ticks, on the grid, inside the limits.

    A BUY crosses the ask (falls back to the bid, then the last price, when the ask
    side is empty); a SELL crosses the bid. Returns ``None`` when the tick has no
    usable price at all.
    """
    step = Decimal(max(0, ticks)) * tick_size
    if direction == Direction.BUY:
        ref = tick.ask1 if tick.ask1 is not None else (tick.bid1 if tick.bid1 is not None else tick.last)
        if ref is None or ref <= 0:
            return None
        raw = (Decimal(str(ref)) + step) / tick_size
        px = raw.to_integral_value(rounding=ROUND_CEILING) * tick_size
    else:
        ref = tick.bid1 if tick.bid1 is not None else (tick.ask1 if tick.ask1 is not None else tick.last)
        if ref is None or ref <= 0:
            return None
        raw = (Decimal(str(ref)) - step) / tick_size
        px = raw.to_integral_value(rounding=ROUND_FLOOR) * tick_size
    px = _clamp(px, tick, tick_size)
    return float(px) if px > 0 else None


def passive_price(tick: Tick, direction: Direction, tick_size: Decimal) -> float | None:
    """Join the best quote on the own side (BUY at bid1, SELL at ask1); last price as fallback."""
    ref = tick.bid1 if direction == Direction.BUY else tick.ask1
    if ref is None:
        ref = tick.last
    if ref is None or ref <= 0:
        return None
    mode = "down" if direction == Direction.BUY else "up"
    px = _clamp(Decimal(str(to_tick(ref, tick_size, mode))), tick, tick_size)
    return float(px) if px > 0 else None


def mid_price(tick: Tick) -> float:
    """Mid of bid1/ask1 when both exist, else the last price."""
    if tick.bid1 is not None and tick.ask1 is not None:
        return (tick.bid1 + tick.ask1) / 2.0
    return tick.last


__all__ = [
    "CAP_LOSS_FRAC",
    "ContractInfo",
    "ContractTable",
    "aggressive_price",
    "mid_price",
    "on_tick_grid",
    "passive_price",
    "to_tick",
]
