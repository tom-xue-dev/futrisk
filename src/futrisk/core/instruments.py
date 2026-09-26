"""Instrument registry: contract parameters resolved per trading day (01 §8.1, §8.9, §8.15).

The registry is the single place where the clearing core and the intraday risk
engine read product rules from ``products.yaml`` (:class:`ProductsConfig`):

- contract spec and exchange rules (multiplier, tick, limit band, close style,
  close-today fee rule, settlement method / window / rounding / fallback chain);
- the fee schedule and the company margin rate in force on a trading day
  (when a contract-specific and a ``"*"`` margin schedule are both in force, the
  higher rate is charged, as exchanges do, e.g. a pre-holiday raise on all
  contracts also lifts a contract that already had its own higher rate);
- limit prices from the previous settlement price (rounded inward to the tick);
- the big-side margin group of a contract and the day it leaves that group;
- the continuous trading intervals of a trading day (session templates).

Everything is pure and deterministic. Lookups are memoized per
``(instrument_id, trading_day)`` because the risk engine calls them per fill.
Trading days are ``YYYYMMDD`` strings throughout, as on the wire (README §3.3).
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from enum import StrEnum
from typing import TypeVar

from futrisk.common.calendar import TradingCalendar, Window, parse_templates
from futrisk.common.clock import BJ, bj_to_ns, parse_td, td_str
from futrisk.common.config.products import (
    ExchangeRules,
    FeeSchedule,
    InstrumentSpec,
    MarginSchedule,
    ProductsConfig,
    ProductSpec,
    product_code,
)
from futrisk.common.decimals import ceil_to_tick, floor_to_tick, limit_prices, on_tick, round_to_tick
from futrisk.messages.enums import Exchange, Rounding, SettleMethod


class UnknownInstrumentError(KeyError):
    """The instrument (and its product) is not configured."""


class ScheduleLookupError(LookupError):
    """No fee or margin schedule matches the instrument at all."""


class MarginMethod(StrEnum):
    """``margin_groups.method`` (storage-only enum, 03 §3.3)."""

    GROSS = "GROSS"
    BIG_SIDE = "BIG_SIDE"


@dataclass(frozen=True, slots=True)
class InstrumentInfo:
    """Resolved, day-independent parameters of one contract."""

    instrument_id: str
    product: str
    exchange: Exchange
    sector: str
    multiplier: int
    tick_size: Decimal
    limit_pct: Decimal
    rules: ExchangeRules
    spec: ProductSpec
    contract: InstrumentSpec | None
    listed: bool
    delivery_month: str | None  # YYYYMM
    last_trade_day: str | None  # YYYYMMDD; estimated for unlisted contracts
    margin_group: str
    margin_method: MarginMethod

    @property
    def contract_month(self) -> int:
        """Month number from the code (``rb2701`` -> 1, ``IF2612`` -> 12)."""
        return contract_month(self.instrument_id)


_S = TypeVar("_S", FeeSchedule, MarginSchedule)


def contract_month(instrument_id: str) -> int:
    """Delivery month number taken from the last two digits of the code."""
    digits = re.sub(r"\D", "", instrument_id)
    if len(digits) < 2:
        raise ValueError(f"cannot derive the contract month of {instrument_id!r}")
    return int(digits[-2:])


def delivery_month_of(instrument_id: str) -> str | None:
    """``YYYYMM`` from a four-digit code (``rb2701`` -> ``202701``); None otherwise.

    Three-digit CZCE codes (``SR701``) are ambiguous about the decade and return None.
    """
    digits = re.sub(r"\D", "", instrument_id)
    if len(digits) != 4:
        return None
    month = int(digits[2:])
    if not 1 <= month <= 12:
        return None
    return f"20{digits[:2]}{digits[2:]}"


def parse_window(text: str) -> tuple[time, time]:
    """Parse a settlement window ``"HH:MM-HH:MM"`` (``ProductSpec.settle_window``)."""
    try:
        a, b = text.split("-")
        ha, ma = a.strip().split(":")
        hb, mb = b.strip().split(":")
        return time(int(ha), int(ma)), time(int(hb), int(mb))
    except ValueError as exc:
        raise ValueError(f"bad settle_window {text!r}, expected 'HH:MM-HH:MM'") from exc


def _latest_in_force(tier: list[_S], trading_day: str) -> _S | None:
    """The schedule of a tier with the latest ``effective_from <= trading_day`` (later entries win ties)."""
    best: _S | None = None
    for s in tier:
        if s.effective_from <= trading_day and (best is None or s.effective_from >= best.effective_from):
            best = s
    return best


def _earliest(tiers: list[list[_S]]) -> _S | None:
    """Fallback before any schedule is in force (e.g. a backtest before the configured history)."""
    earliest: _S | None = None
    for tier in tiers:
        for s in tier:
            if earliest is None or s.effective_from < earliest.effective_from:
                earliest = s
    return earliest


def _pick_schedule(tiers: list[list[_S]], trading_day: str) -> _S | None:
    """Most specific tier first; inside a tier the latest ``effective_from <= trading_day``.

    When no schedule of any tier is in force yet (e.g. a backtest before the
    configured history starts), the earliest schedule is used, preferring the
    most specific tier on ties; configuration only moves forward in time.
    """
    for tier in tiers:
        best = _latest_in_force(tier, trading_day)
        if best is not None:
            return best
    return _earliest(tiers)


def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    """The ``n``-th ``weekday`` (Mon=0) of a month."""
    d = date(year, month, 1)
    offset = (weekday - d.weekday()) % 7
    return d + timedelta(days=offset + 7 * (n - 1))


class InstrumentRegistry:
    """Product rules per instrument and trading day.

    ``calendar`` is needed for the big-side exit day and for estimating the last
    trading day of unlisted contracts. Without it, trading-day arithmetic falls
    back to weekdays only (holidays ignored), which is acceptable in unit tests
    but not in production.

    ``allow_unlisted``: contracts missing from ``instruments`` but whose product
    is configured (e.g. historical contracts in a backtest) are synthesized with
    the delivery month taken from the code and an estimated last trading day.
    Use :meth:`is_listed` for the pre-trade "unknown instrument" check.
    """

    def __init__(
        self,
        products: ProductsConfig,
        calendar: TradingCalendar | None = None,
        *,
        allow_unlisted: bool = True,
    ) -> None:
        self._products = products
        self._calendar = calendar
        self._allow_unlisted = allow_unlisted
        self._contracts: dict[str, InstrumentSpec] = {i.instrument_id: i for i in products.instruments}
        self._info: dict[str, InstrumentInfo] = {}
        self._fee_cache: dict[tuple[str, str], FeeSchedule] = {}
        self._margin_cache: dict[tuple[str, str], MarginSchedule] = {}
        self._exit_cache: dict[str, str | None] = {}
        self._templates: dict[str, list[Window]] | None = None

    # ------------------------------------------------------------------ basics
    @property
    def products(self) -> ProductsConfig:
        return self._products

    @property
    def calendar(self) -> TradingCalendar | None:
        return self._calendar

    def instrument_ids(self) -> list[str]:
        """Configured (listed) contracts, in configuration order."""
        return list(self._contracts)

    def is_listed(self, instrument_id: str) -> bool:
        """True when the contract is configured in ``instruments``."""
        return instrument_id in self._contracts

    def has(self, instrument_id: str) -> bool:
        """True when :meth:`get` would succeed."""
        try:
            self.get(instrument_id)
        except UnknownInstrumentError:
            return False
        return True

    def get(self, instrument_id: str) -> InstrumentInfo:
        info = self._info.get(instrument_id)
        if info is None:
            info = self._build(instrument_id)
            self._info[instrument_id] = info
        return info

    def _build(self, instrument_id: str) -> InstrumentInfo:
        contract = self._contracts.get(instrument_id)
        if contract is not None:
            code = contract.product
        else:
            if not self._allow_unlisted:
                raise UnknownInstrumentError(instrument_id)
            try:
                code = product_code(instrument_id)
            except ValueError:
                raise UnknownInstrumentError(instrument_id) from None
        spec = self._products.products.get(code)
        if spec is None:
            raise UnknownInstrumentError(instrument_id)
        rules = self._products.exchanges[spec.exchange]
        delivery = (contract.delivery_month if contract and contract.delivery_month else None) or delivery_month_of(
            instrument_id
        )
        if contract is not None:
            last_td: str | None = contract.last_trade_day
        else:
            last_td = self._estimate_last_trade_day(spec, delivery)
        group, method = self._group_of(instrument_id, code, spec, rules)
        return InstrumentInfo(
            instrument_id=instrument_id,
            product=code,
            exchange=spec.exchange,
            sector=spec.sector,
            multiplier=spec.multiplier,
            tick_size=spec.tick_size,
            limit_pct=spec.limit_pct,
            rules=rules,
            spec=spec,
            contract=contract,
            listed=contract is not None,
            delivery_month=delivery,
            last_trade_day=last_td,
            margin_group=group,
            margin_method=method,
        )

    @staticmethod
    def _group_of(instrument_id: str, code: str, spec: ProductSpec, rules: ExchangeRules) -> tuple[str, MarginMethod]:
        # 03 §4.4 margin_groups.margin_group：SHFE:{product} / CFFEX:INDEX / CFFEX:TREASURY / GROSS:{instrument_id}
        if rules.big_side == "NONE":
            return f"GROSS:{instrument_id}", MarginMethod.GROSS
        # SAME_PRODUCT（SHFE 同品种跨月）与 GROUPS（CFFEX 股指组、国债组）都由 big_side_group 命名，缺省为品种
        return spec.big_side_group or f"{spec.exchange}:{code}", MarginMethod.BIG_SIDE

    # --------------------------------------------------------- simple accessors
    def product(self, instrument_id: str) -> str:
        return self.get(instrument_id).product

    def product_spec(self, instrument_id: str) -> ProductSpec:
        return self.get(instrument_id).spec

    def exchange(self, instrument_id: str) -> Exchange:
        return self.get(instrument_id).exchange

    def rules(self, instrument_id: str) -> ExchangeRules:
        return self.get(instrument_id).rules

    def multiplier(self, instrument_id: str) -> int:
        return self.get(instrument_id).multiplier

    def tick_size(self, instrument_id: str) -> Decimal:
        return self.get(instrument_id).tick_size

    def close_style(self, instrument_id: str) -> str:
        """Lot consumption order: ``FIFO`` or ``TODAY_FIRST`` (01 §8.15 CloseStyle)."""
        return self.get(instrument_id).rules.close_style

    def explicit_close_today(self, instrument_id: str) -> bool:
        """True for SHFE/INE: orders must say CLOSE_TODAY or CLOSE_YESTERDAY."""
        return self.get(instrument_id).rules.explicit_close_today

    def close_today_fee_rule(self, instrument_id: str) -> str:
        """``LOT_MATCH`` / ``SAME_DAY_OPENS_FIRST`` / ``INTRADAY_BOTH_LEGS``."""
        return self.get(instrument_id).rules.close_today_fee_rule

    # ------------------------------------------------------------- schedules
    def fee_schedule(self, instrument_id: str, trading_day: str) -> FeeSchedule:
        """Fee schedule in force: explicit ``contracts`` > ``months`` > ``"other"``/None.

        Within the most specific matching tier, the schedule with the latest
        ``effective_from <= trading_day`` wins.
        """
        key = (instrument_id, trading_day)
        hit = self._fee_cache.get(key)
        if hit is not None:
            return hit
        info = self.get(instrument_id)
        month = info.contract_month
        tiers: list[list[FeeSchedule]] = [[], [], []]
        for s in info.spec.fees:
            if s.contracts is not None:
                if instrument_id in s.contracts:
                    tiers[0].append(s)
            elif isinstance(s.months, list):
                if month in s.months:
                    tiers[1].append(s)
            else:
                tiers[2].append(s)
        picked = _pick_schedule(tiers, trading_day)
        if picked is None:
            raise ScheduleLookupError(f"no fee schedule matches {instrument_id}")
        self._fee_cache[key] = picked
        return picked

    def margin_schedule(self, instrument_id: str, trading_day: str) -> MarginSchedule:
        """Margin schedule charged on ``trading_day``.

        In each tier (contract-specific lists, ``"*"``) the schedule with the latest
        ``effective_from <= trading_day`` is in force; when both tiers have one, the
        schedule with the **higher** company rate is charged (exchanges apply the
        highest applicable rate: a pre-holiday raise on ``"*"`` also lifts a contract
        whose own schedule is lower). Ties prefer the contract-specific schedule.
        """
        key = (instrument_id, trading_day)
        hit = self._margin_cache.get(key)
        if hit is not None:
            return hit
        info = self.get(instrument_id)
        tiers: list[list[MarginSchedule]] = [[], []]
        for s in info.spec.margin:
            if s.contracts == "*":
                tiers[1].append(s)
            elif instrument_id in s.contracts:
                tiers[0].append(s)
        picked: MarginSchedule | None = None
        for tier in tiers:
            cand = _latest_in_force(tier, trading_day)
            if cand is not None and (picked is None or cand.company_rate > picked.company_rate):
                picked = cand
        if picked is None:
            picked = _earliest(tiers)
        if picked is None:
            raise ScheduleLookupError(f"no margin schedule matches {instrument_id}")
        self._margin_cache[key] = picked
        return picked

    def margin_rate(self, instrument_id: str, trading_day: str) -> Decimal:
        """Company margin rate = exchange rate + broker add-on (01 §8.15), highest schedule in force."""
        return self.margin_schedule(instrument_id, trading_day).company_rate

    # ---------------------------------------------------------------- prices
    def limit_prices(
        self, instrument_id: str, pre_settle: Decimal, trading_day: str | None = None
    ) -> tuple[Decimal, Decimal]:
        """(upper, lower) limit prices from the previous settlement, rounded inward to the tick.

        With ``trading_day`` the product's date-effective ``limit_schedule`` applies
        (limits are often widened before long holidays).
        """
        info = self.get(instrument_id)
        pct = info.spec.limit_pct_on(trading_day) if trading_day else info.limit_pct
        return limit_prices(pre_settle, pct, info.tick_size)

    def on_tick(self, instrument_id: str, price: Decimal | float) -> bool:
        return on_tick(price, self.get(instrument_id).tick_size)

    def round_price(self, instrument_id: str, price: Decimal, rounding: Rounding) -> Decimal:
        """Apply a settlement rounding rule (01 §8.3)."""
        tick = self.get(instrument_id).tick_size
        if rounding == Rounding.FLOOR_TICK:
            return floor_to_tick(price, tick)
        if rounding == Rounding.ROUND_TICK:
            return round_to_tick(price, tick)
        return price

    def ceil_price(self, instrument_id: str, price: Decimal) -> Decimal:
        return ceil_to_tick(price, self.get(instrument_id).tick_size)

    # ------------------------------------------------------------ settlement
    def settle_method(self, instrument_id: str) -> SettleMethod:
        return self.get(instrument_id).spec.settle_method

    def settle_rounding(self, instrument_id: str) -> Rounding:
        return self.get(instrument_id).rules.settle_rounding

    def settle_fallback(self, instrument_id: str) -> list[str]:
        return list(self.get(instrument_id).rules.settle_fallback)

    def settle_window(self, instrument_id: str) -> tuple[time, time] | None:
        """Last-hour window (Beijing time) for ``VWAP_LAST_HOUR``; None for full-day VWAP."""
        spec = self.get(instrument_id).spec
        if spec.settle_method != SettleMethod.VWAP_LAST_HOUR:
            return None
        if spec.settle_window is None:
            # 01 §8.15：股指 14:00–15:00；未配置时按股指默认
            return time(14, 0), time(15, 0)
        return parse_window(spec.settle_window)

    def settle_window_ns(self, instrument_id: str, trading_day: str) -> tuple[int, int] | None:
        """``(start_ns, end_ns)`` of the settlement window on ``trading_day`` (day session)."""
        w = self.settle_window(instrument_id)
        if w is None:
            return None
        d = parse_td(trading_day)
        return (
            bj_to_ns(datetime.combine(d, w[0], tzinfo=BJ)),
            bj_to_ns(datetime.combine(d, w[1], tzinfo=BJ)),
        )

    def session_intervals(self, instrument_id: str, trading_day: str) -> list[tuple[int, int]]:
        """Continuous trading intervals ``[start_ns, end_ns)`` of ``trading_day``, ascending.

        Built from the product's session templates (01 §8.2); auctions are
        excluded. Night windows (starting at or after 20:00) belong to the
        previous trading day's evening and are dropped when the calendar says the
        trading day has no night session (after a holiday).
        """
        spec = self.get(instrument_id).spec
        if self._templates is None:
            self._templates = parse_templates(self._products.session_templates)
        d = parse_td(trading_day)
        cal = self._in_calendar(d)
        night_allowed = cal.has_night_session(d) if cal is not None else True
        prev = parse_td(self._prev_trading_day(trading_day))
        out: list[tuple[int, int]] = []
        for name in spec.sessions:
            w: Window
            for w in self._templates.get(name, []):
                if w.is_auction:
                    continue
                if w.night:
                    if not night_allowed:
                        continue
                    end_day = prev if w.end > w.start else prev + timedelta(days=1)
                    start_dt = datetime.combine(prev, w.start, tzinfo=BJ)
                    end_dt = datetime.combine(end_day, w.end, tzinfo=BJ)
                else:
                    start_dt = datetime.combine(d, w.start, tzinfo=BJ)
                    end_dt = datetime.combine(d, w.end, tzinfo=BJ)
                out.append((bj_to_ns(start_dt), bj_to_ns(end_dt)))
        out.sort()
        return out

    # ------------------------------------------------------------ big side
    def margin_group(self, instrument_id: str) -> tuple[str, MarginMethod]:
        """``(margin_group, method)`` as stored in ``margin_groups`` (03 §4.4)."""
        info = self.get(instrument_id)
        return info.margin_group, info.margin_method

    def big_side_exit_day(self, instrument_id: str) -> str | None:
        """Trading day after whose settlement the contract leaves its big-side group.

        - SHFE (``LAST_TRADE_DAY_MINUS_5``): the 5th trading day before the last
          trading day L. From the settlement of L−5 on, the contract is charged
          two-way (the last five trading days L−4..L hold no big-side benefit);
        - CFFEX (``PHYSICAL_DELIVERY_MONTH_MINUS_1``): physically delivered
          contracts (treasury futures) leave after the close of the trading day
          before the delivery month; index futures never leave;
        - otherwise None (never excluded).
        """
        if instrument_id in self._exit_cache:
            return self._exit_cache[instrument_id]
        info = self.get(instrument_id)
        exit_day: str | None = None
        if info.margin_method is MarginMethod.BIG_SIDE:
            rule = info.rules.big_side_exit
            if rule == "LAST_TRADE_DAY_MINUS_5" and info.last_trade_day:
                # 01 §8.15：SHFE 合约在最后交易日前第 5 个交易日收盘后退出单向大边
                exit_day = self._nth_trading_day_before(info.last_trade_day, 5)
            elif rule == "PHYSICAL_DELIVERY_MONTH_MINUS_1" and info.spec.physical_delivery and info.delivery_month:
                # 01 §8.15：CFFEX 实物交割合约在交割月前一个交易日收盘后退出；股指期货不退出
                first = date(int(info.delivery_month[:4]), int(info.delivery_month[4:]), 1)
                exit_day = self._prev_trading_day(td_str(first))
        self._exit_cache[instrument_id] = exit_day
        return exit_day

    def big_side_excluded(self, instrument_id: str, trading_day: str, *, intraday: bool = False) -> bool:
        """True when the contract no longer benefits from big-side margin.

        ``intraday=False`` (settlement of ``trading_day``): excluded from the exit
        day's own settlement on. ``intraday=True`` (positions held during
        ``trading_day``): excluded only on trading days after the exit day, since
        the exit takes effect with that day's closing settlement.
        """
        exit_day = self.big_side_exit_day(instrument_id)
        if exit_day is None:
            return False
        return trading_day > exit_day if intraday else trading_day >= exit_day

    # ------------------------------------------------------ calendar helpers
    # The calendar is authoritative inside its range; outside it (a backtest
    # before the calendar starts, or contracts far in the future) trading-day
    # arithmetic falls back to weekdays only.
    def _in_calendar(self, d: date) -> TradingCalendar | None:
        cal = self._calendar
        if cal is not None and cal.first_day <= d <= cal.last_day:
            return cal
        return None

    def _nth_trading_day_before(self, td: str, n: int) -> str:
        d = parse_td(td)
        cal = self._in_calendar(d)
        if cal is not None:
            try:
                if not cal.is_trading_day(d):
                    # 最后交易日未落在日历的交易日上（配置错误）时，以其前一个交易日为锚点
                    d = cal.prev_trading_day(d)
                return td_str(cal.nth_trading_day_before(d, n))
            except LookupError:
                pass
        while d.weekday() >= 5:
            d -= timedelta(days=1)
        k = 0
        while k < n:
            d -= timedelta(days=1)
            if d.weekday() < 5:
                k += 1
        return td_str(d)

    def _prev_trading_day(self, td: str) -> str:
        d = parse_td(td)
        cal = self._in_calendar(d)
        if cal is not None:
            try:
                return td_str(cal.prev_trading_day(d))
            except LookupError:
                pass
        d -= timedelta(days=1)
        while d.weekday() >= 5:
            d -= timedelta(days=1)
        return td_str(d)

    def _on_or_after(self, d: date) -> date:
        cal = self._in_calendar(d)
        if cal is not None:
            if cal.is_trading_day(d):
                return d
            try:
                return cal.next_trading_day(d)
            except LookupError:
                pass
        while d.weekday() >= 5:
            d += timedelta(days=1)
        return d

    def _estimate_last_trade_day(self, spec: ProductSpec, delivery_month: str | None) -> str | None:
        """Estimate the last trading day of an unlisted contract from exchange rules.

        - SHFE/INE: the 15th of the delivery month, postponed to the next trading day;
        - DCE/CZCE/GFEX: the 10th trading day of the delivery month;
        - CFFEX index futures: the third Friday; treasury futures: the second Friday
          (postponed to the next trading day on holidays).
        """
        if delivery_month is None:
            return None
        year, month = int(delivery_month[:4]), int(delivery_month[4:])
        if spec.exchange in (Exchange.SHFE, Exchange.INE):
            return td_str(self._on_or_after(date(year, month, 15)))
        if spec.exchange == Exchange.CFFEX:
            n = 2 if spec.physical_delivery else 3
            return td_str(self._on_or_after(_nth_weekday(year, month, 4, n)))
        d = self._on_or_after(date(year, month, 1))
        for _ in range(9):
            d = self._on_or_after(d + timedelta(days=1))
        return td_str(d)


__all__ = [
    "InstrumentInfo",
    "InstrumentRegistry",
    "MarginMethod",
    "ScheduleLookupError",
    "UnknownInstrumentError",
    "contract_month",
    "delivery_month_of",
    "parse_window",
]
