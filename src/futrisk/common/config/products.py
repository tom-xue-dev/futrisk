"""``products.yaml``: exchange rules, product specs, contracts, session templates (01 §8.9).

Values are examples; production takes them from exchange notices. Fees and
margins carry effective dates and can be matched per contract month or per
explicit contract list.
"""

from __future__ import annotations

import re
from datetime import date
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from futrisk.messages.enums import Exchange, Rounding, SettleMethod


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class LargeCancelRule(_Strict):
    count: int
    min_qty: int


class ExchangeRules(_Strict):
    explicit_close_today: bool = False
    close_style: Literal["FIFO", "TODAY_FIRST"] = "FIFO"
    close_today_fee_rule: Literal["LOT_MATCH", "SAME_DAY_OPENS_FIRST", "INTRADAY_BOTH_LEGS"] = "LOT_MATCH"
    settle_rounding: Rounding = Rounding.ROUND_TICK
    settle_fallback: list[str] = Field(default_factory=lambda: ["PRE_SETTLE"])
    big_side: Literal["NONE", "SAME_PRODUCT", "GROUPS"] = "NONE"
    big_side_exit: Literal["NONE", "LAST_TRADE_DAY_MINUS_5", "PHYSICAL_DELIVERY_MONTH_MINUS_1"] = "NONE"
    cancel_limit: int | None = None
    large_cancel: LargeCancelRule | None = None
    self_trade_limit: int = 5


class FeeSchedule(_Strict):
    """Fee rule. ``type=rate`` multiplies notional; ``per_lot`` charges per lot.

    ``close_today_mult`` multiplies the close-yesterday fee for close-today legs.
    ``intraday`` (DCE): both legs of a same-day open->close pay this rate/amount.
    Matching precedence: ``contracts`` list, then ``months``, then ``"other"``.
    """

    months: list[int] | Literal["other"] | None = None
    contracts: list[str] | None = None
    effective_from: str = "19700101"
    type: Literal["rate", "per_lot"]
    open: Decimal
    close_yd: Decimal
    close_today_mult: Decimal = Decimal("1")
    intraday: Decimal | None = None


class MarginSchedule(_Strict):
    contracts: list[str] | Literal["*"] = "*"
    effective_from: str = "19700101"
    exchange_rate: Decimal
    broker_add: Decimal = Decimal("0")

    @property
    def company_rate(self) -> Decimal:
        return self.exchange_rate + self.broker_add


class LimitSchedule(_Strict):
    """Date-effective price-limit change (e.g. widened limits before a long holiday)."""

    effective_from: str  # YYYYMMDD, first trading day the new limit applies
    pct: Decimal


class ProductSpec(_Strict):
    exchange: Exchange
    name: str = ""
    sector: str
    multiplier: int
    tick_size: Decimal
    limit_pct: Decimal
    limit_schedule: list[LimitSchedule] = Field(default_factory=list)
    settle_method: SettleMethod = SettleMethod.VWAP_FULL_DAY
    settle_window: str | None = None  # "HH:MM-HH:MM" for VWAP_LAST_HOUR
    big_side_group: str | None = None
    physical_delivery: bool = False  # only CFFEX treasury-style contracts leave big-side groups (01 §8.15)
    sessions: list[str]
    fees: list[FeeSchedule]
    margin: list[MarginSchedule]
    max_order_qty: dict[str, int] = Field(default_factory=lambda: {"LIMIT": 1000, "MARKET": 1000})
    position_limit: int | None = None
    open_limit_per_day: int | None = None
    notional_face: Decimal | None = None  # T: contract value = price * face / 100


    def limit_pct_on(self, trading_day: str | None) -> Decimal:
        """Limit percentage effective on ``trading_day`` (latest schedule with effective_from <= day)."""
        if not trading_day or not self.limit_schedule:
            return self.limit_pct
        best: LimitSchedule | None = None
        for sch in self.limit_schedule:
            if sch.effective_from <= trading_day and (best is None or sch.effective_from > best.effective_from):
                best = sch
        return best.pct if best is not None else self.limit_pct


class InstrumentSpec(_Strict):
    instrument_id: str
    product: str
    last_trade_day: str  # YYYYMMDD
    listed_day: str = "20200101"
    delivery_month: str | None = None  # YYYYMM

    @property
    def contract_month(self) -> int:
        """Month number from the code (rb2701 -> 1, IF2612 -> 12)."""
        digits = re.sub(r"\D", "", self.instrument_id)
        return int(digits[-2:])


class ProductsConfig(_Strict):
    session_templates: dict[str, list[list[str]]]
    exchanges: dict[Exchange, ExchangeRules]
    products: dict[str, ProductSpec]
    instruments: list[InstrumentSpec]

    @model_validator(mode="after")
    def _check_refs(self) -> ProductsConfig:
        for code, spec in self.products.items():
            for s in spec.sessions:
                if s not in self.session_templates:
                    raise ValueError(f"product {code}: unknown session template {s}")
            if spec.exchange not in self.exchanges:
                raise ValueError(f"product {code}: exchange {spec.exchange} has no rules")
        for inst in self.instruments:
            if inst.product not in self.products:
                raise ValueError(f"instrument {inst.instrument_id}: unknown product {inst.product}")
        return self

    def instrument(self, instrument_id: str) -> InstrumentSpec | None:
        for i in self.instruments:
            if i.instrument_id == instrument_id:
                return i
        return None

    def product_of(self, instrument_id: str) -> ProductSpec:
        inst = self.instrument(instrument_id)
        code = inst.product if inst else product_code(instrument_id)
        return self.products[code]

    def instrument_ids(self) -> list[str]:
        return [i.instrument_id for i in self.instruments]


def product_code(instrument_id: str) -> str:
    """Product code from a contract id: ``rb2701`` -> ``rb``, ``IF2612`` -> ``IF``."""
    m = re.match(r"^([A-Za-z]+)", instrument_id)
    if not m:
        raise ValueError(f"bad instrument id {instrument_id!r}")
    return m.group(1)


def td_to_date(td: str) -> date:
    return date(int(td[:4]), int(td[4:6]), int(td[6:8]))
