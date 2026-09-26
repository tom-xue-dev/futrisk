"""Builders for clearing tests: in-code product configs, calendars, trades, cash and day chains.

The golden cases (``CASES.md``) use parameters that differ from
``config/products.yaml`` (no broker add-on, 7% margin, one fee on every month
...), so every test builds its own :class:`ProductsConfig` here instead of
loading the bundled configuration.
"""

from __future__ import annotations

import itertools
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from futrisk.common.calendar import TradingCalendar, weekday_calendar
from futrisk.common.clock import BJ, bj_to_ns
from futrisk.common.config.products import ProductsConfig
from futrisk.common.decimals import dec_str_strip
from futrisk.core import DaySettlement, InstrumentRegistry, settle_day
from futrisk.messages.enums import CashType, Direction, Exchange, Offset, Rounding, SettlePriceSource
from futrisk.messages.eod import SettlementPriceView, SodSnapshot
from futrisk.messages.orders import Trade
from futrisk.messages.risk import CashMovement

B = Direction.BUY
S = Direction.SELL
O = Offset.OPEN  # noqa: E741
C = Offset.CLOSE
CT = Offset.CLOSE_TODAY
CY = Offset.CLOSE_YESTERDAY
FC = Offset.FORCE_CLOSE

# ------------------------------------------------------------ exchange rules
SHFE_RULES: dict[str, Any] = {
    "explicit_close_today": True,
    "close_style": "FIFO",
    "close_today_fee_rule": "LOT_MATCH",
    "settle_rounding": "FLOOR_TICK",
    "settle_fallback": ["MEDIAN", "LIMIT", "PRE_SETTLE"],
    "big_side": "SAME_PRODUCT",
    "big_side_exit": "LAST_TRADE_DAY_MINUS_5",
}
DCE_RULES: dict[str, Any] = {
    "explicit_close_today": False,
    "close_style": "FIFO",
    "close_today_fee_rule": "INTRADAY_BOTH_LEGS",
    "settle_rounding": "ROUND_TICK",
    "settle_fallback": ["MEDIAN", "LIMIT", "PRE_SETTLE"],
    "big_side": "NONE",
    "big_side_exit": "NONE",
}
CFFEX_RULES: dict[str, Any] = {
    "explicit_close_today": False,
    "close_style": "FIFO",
    "close_today_fee_rule": "SAME_DAY_OPENS_FIRST",
    "settle_rounding": "ROUND_TICK",
    "settle_fallback": ["HOURLY", "FULLDAY", "BENCHMARK", "PRE_SETTLE"],
    "big_side": "GROUPS",
    "big_side_exit": "PHYSICAL_DELIVERY_MONTH_MINUS_1",
}
RULES = {"SHFE": SHFE_RULES, "DCE": DCE_RULES, "CFFEX": CFFEX_RULES}

# Session templates as in config/products.yaml (trading-hour windows of the CFFEX fallback use them).
SESSIONS: dict[str, list[list[str]]] = {
    "day": [["09:00", "15:00", "continuous"]],
    "night_2100_2300": [["20:55", "21:00", "auction"], ["21:00", "23:00", "continuous"]],
    "day_commodity": [
        ["08:55", "09:00", "auction_no_night"],
        ["09:00", "10:15", "continuous"],
        ["10:30", "11:30", "continuous"],
        ["13:30", "15:00", "continuous"],
    ],
    "day_index": [["09:25", "09:30", "auction"], ["09:30", "11:30", "continuous"], ["13:00", "15:00", "continuous"]],
    "day_treasury": [["09:25", "09:30", "auction"], ["09:30", "11:30", "continuous"], ["13:00", "15:15", "continuous"]],
}

EXCHANGE_OF_PRODUCT = {
    "rb": Exchange.SHFE,
    "hc": Exchange.SHFE,
    "cu": Exchange.SHFE,
    "au": Exchange.SHFE,
    "i": Exchange.DCE,
    "j": Exchange.DCE,
    "m": Exchange.DCE,
    "IF": Exchange.CFFEX,
    "IH": Exchange.CFFEX,
    "IC": Exchange.CFFEX,
    "IM": Exchange.CFFEX,
    "T": Exchange.CFFEX,
    "TF": Exchange.CFFEX,
    "TS": Exchange.CFFEX,
}


def rate_fee(
    open_: str,
    close_yd: str | None = None,
    *,
    close_today_mult: str = "1",
    intraday: str | None = None,
    months: list[int] | str | None = None,
    contracts: list[str] | None = None,
    effective_from: str = "19700101",
) -> dict[str, Any]:
    d: dict[str, Any] = {
        "type": "rate",
        "open": open_,
        "close_yd": close_yd or open_,
        "close_today_mult": close_today_mult,
        "effective_from": effective_from,
    }
    if intraday is not None:
        d["intraday"] = intraday
    if months is not None:
        d["months"] = months
    if contracts is not None:
        d["contracts"] = contracts
    return d


def lot_fee(
    open_: str,
    close_yd: str | None = None,
    *,
    close_today_mult: str = "1",
    intraday: str | None = None,
    effective_from: str = "19700101",
) -> dict[str, Any]:
    d = rate_fee(open_, close_yd, close_today_mult=close_today_mult, intraday=intraday, effective_from=effective_from)
    d["type"] = "per_lot"
    return d


def margin(
    rate: str, *, contracts: list[str] | str = "*", broker_add: str = "0", effective_from: str = "19700101"
) -> dict[str, Any]:
    return {"contracts": contracts, "exchange_rate": rate, "broker_add": broker_add, "effective_from": effective_from}


def product(
    exchange: str,
    *,
    multiplier: int,
    tick: str,
    fees: list[dict[str, Any]],
    margins: list[dict[str, Any]],
    limit_pct: str = "0.05",
    sector: str = "test",
    settle_method: str = "VWAP_FULL_DAY",
    settle_window: str | None = None,
    big_side_group: str | None = None,
    physical_delivery: bool = False,
    sessions: list[str] | None = None,
) -> dict[str, Any]:
    if sessions is None:
        if exchange == "CFFEX":
            sessions = ["day_treasury"] if settle_window == "14:15-15:15" else ["day_index"]
        else:
            sessions = ["night_2100_2300", "day_commodity"]
    d: dict[str, Any] = {
        "exchange": exchange,
        "sector": sector,
        "multiplier": multiplier,
        "tick_size": tick,
        "limit_pct": limit_pct,
        "settle_method": settle_method,
        "sessions": sessions,
        "fees": fees,
        "margin": margins,
        "physical_delivery": physical_delivery,
    }
    if settle_window is not None:
        d["settle_window"] = settle_window
    if big_side_group is not None:
        d["big_side_group"] = big_side_group
    return d


def inst(instrument_id: str, product_code: str, last_trade_day: str, delivery_month: str | None = None) -> dict[str, Any]:
    d: dict[str, Any] = {"instrument_id": instrument_id, "product": product_code, "last_trade_day": last_trade_day}
    if delivery_month is not None:
        d["delivery_month"] = delivery_month
    return d


def make_products(
    products: Mapping[str, dict[str, Any]],
    instruments: Sequence[dict[str, Any]],
    *,
    rules: Mapping[str, Mapping[str, Any]] | None = None,
) -> ProductsConfig:
    """ProductsConfig with the three exchanges; ``rules`` overrides fields per exchange."""
    ex: dict[str, dict[str, Any]] = {k: dict(v) for k, v in RULES.items()}
    for k, v in (rules or {}).items():
        ex[k].update(v)
    return ProductsConfig.model_validate(
        {"session_templates": SESSIONS, "exchanges": ex, "products": dict(products), "instruments": list(instruments)}
    )


HOLIDAYS_2026 = [date(2026, 9, 25), *[date(2026, 10, d) for d in range(1, 8)]]


def make_calendar() -> TradingCalendar:
    """Trading days 2026-09-01..2027-03-31 matching the official 2026 holidays used by the cases."""
    return TradingCalendar(weekday_calendar(date(2026, 9, 1), date(2027, 3, 31), HOLIDAYS_2026))


def make_registry(
    products: Mapping[str, dict[str, Any]],
    instruments: Sequence[dict[str, Any]],
    *,
    rules: Mapping[str, Mapping[str, Any]] | None = None,
) -> InstrumentRegistry:
    return InstrumentRegistry(make_products(products, instruments, rules=rules), make_calendar())


# ------------------------------------------------------------------ events
def ts(text: str) -> int:
    """Beijing ``YYYY-MM-DD HH:MM[:SS]`` -> UTC epoch ns."""
    return bj_to_ns(datetime.fromisoformat(text).replace(tzinfo=BJ))


class TradeFactory:
    """Creates ``Trade`` messages with increasing ``trade_seq`` and unique ids."""

    def __init__(self, prefix: str = "T") -> None:
        self._seq = itertools.count(1)
        self._prefix = prefix

    def __call__(
        self,
        account_id: str,
        instrument_id: str,
        direction: Direction,
        offset: Offset,
        price: float,
        qty: int,
        trading_day: str,
        when: str | int,
        *,
        exchange: Exchange | None = None,
        trade_id: str | None = None,
    ) -> Trade:
        n = next(self._seq)
        code = "".join(ch for ch in instrument_id if ch.isalpha())
        return Trade(
            trading_day=trading_day,
            exchange=exchange or EXCHANGE_OF_PRODUCT[code],
            trade_id=trade_id or f"{self._prefix}{n}",
            direction=direction,
            account_id=account_id,
            instrument_id=instrument_id,
            broker_order_ref=f"ref-{n}",
            offset=offset,
            price=price,
            qty=qty,
            trade_ts_ns=when if isinstance(when, int) else ts(when),
            trade_seq=n,
        )


def cash(account_id: str, trading_day: str, kind: CashType, amount: str, movement_id: str) -> CashMovement:
    return CashMovement(
        movement_id=movement_id,
        account_id=account_id,
        trading_day=trading_day,
        type=kind,
        amount=amount,
        actor="test",
        reason="test",
        ts_ns=0,
    )


@dataclass
class Day:
    trading_day: str
    next_trading_day: str
    prices: Mapping[str, Decimal | str | SettlementPriceView]
    trades: list[Trade] = field(default_factory=list)
    cash: list[CashMovement] = field(default_factory=list)


def pv(
    trading_day: str,
    instrument_id: str,
    price: Decimal | str,
    *,
    source: SettlePriceSource = SettlePriceSource.OFFICIAL,
) -> SettlementPriceView:
    """A settlement price view for tests (settle_day requires views for held/traded contracts)."""
    return SettlementPriceView(
        trading_day=trading_day,
        instrument_id=instrument_id,
        price=dec_str_strip(Decimal(str(price))),
        source=source,
        rounding=Rounding.NONE,
    )


def views(
    trading_day: str, prices: Mapping[str, Decimal | str | SettlementPriceView]
) -> dict[str, SettlementPriceView]:
    """Wrap plain prices into OFFICIAL views; views pass through unchanged."""
    return {k: v if isinstance(v, SettlementPriceView) else pv(trading_day, k, v) for k, v in prices.items()}


def run_days(registry: InstrumentRegistry, sod: SodSnapshot, days: Iterable[Day], *, version: int = 1) -> list[DaySettlement]:
    """Chain :func:`settle_day` over consecutive days (each day starts from the previous next-SOD).

    Plain prices in ``Day.prices`` are wrapped as OFFICIAL price views.
    """
    out: list[DaySettlement] = []
    for d in days:
        res = settle_day(
            trading_day=d.trading_day,
            version=version,
            sod=sod,
            trades=d.trades,
            cash_movements=d.cash,
            settle_prices=views(d.trading_day, d.prices),
            registry=registry,
            next_trading_day=d.next_trading_day,
        )
        out.append(res)
        sod = res.next_sod
    return out


def formula_oracle(
    trades: Iterable[Trade], *, settle: Decimal, pre_settle: Decimal, yd_long: int, yd_short: int, multiplier: int
) -> Decimal:
    """Independent test-side exchange formula (01 §8.15), computed without the core."""
    total = Decimal(0)
    for t in trades:
        px = Decimal(str(t.price))
        total += (px - settle) * t.qty if t.direction == Direction.SELL else (settle - px) * t.qty
    total += (pre_settle - settle) * (yd_short - yd_long)
    return total * multiplier
