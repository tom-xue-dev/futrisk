"""Trade fees and close-today / intraday classification (01 §8.15 开平与计费, 03 §4.4 trade_settlements).

Fee amount of one leg (``FeeSchedule``):

- ``type=rate``: price × multiplier × qty × rate; ``type=per_lot``: qty × amount;
- ``OPEN`` uses ``open``; ``CLOSE_YD`` uses ``close_yd``; ``CLOSE_TODAY`` uses
  ``close_yd × close_today_mult``; ``INTRADAY_OPEN``/``INTRADAY_CLOSE`` use ``intraday``.

A trade's fee is the sum of its legs rounded ROUND_HALF_UP to 0.01 (per trade,
README §3.4). A trade can have two legs, e.g. a CFFEX close of 10 lots of which
8 close same-day opens (8 × close-today + 2 × close-yesterday).

Which closes pay the close-today rate is an exchange rule
(``ExchangeRules.close_today_fee_rule``):

- ``LOT_MATCH`` (SHFE/INE): the lots actually consumed decide; with explicit
  CLOSE_TODAY / CLOSE_YESTERDAY orders this is the order's own offset;
- ``SAME_DAY_OPENS_FIRST`` (CFFEX "先平当日新开仓"): closes match same-day opens
  of the same position side first, chronologically, independent of the lot
  consumption order (with the configured ``close_style: TODAY_FIRST`` both agree);
- ``INTRADAY_BOTH_LEGS`` (DCE): a close is intraday only for the **today lots it
  actually consumes** (DCE closes 先开先平, i.e. yesterday lots first under
  FIFO); the open legs of those consumed today lots also pay the ``intraday``
  rate. Everything else is regular: opens not closed the same day pay ``OPEN``,
  closes of yesterday lots pay ``CLOSE_YD``. Example (2 yesterday lots, buy-open
  5, sell-close 3): the close consumes 2 yesterday + 1 today lot, so 1 intraday
  close + 1 intraday open, 2 regular closes and 4 regular opens. Products
  without an ``intraday`` rate charge consumed today lots the close-today rate.

Today lots are always consumed oldest first among themselves (under FIFO,
TODAY_FIRST and explicit CLOSE_TODAY alike), so the open legs to upgrade are the
earliest same-day opens not yet consumed: a FIFO queue of same-day opens,
advanced by the number of today lots each close consumed, identifies them.

For ``INTRADAY_BOTH_LEGS`` the final class of an open depends on later closes,
so the exact fee is only known after the day's last fill
(:meth:`DayFeeBook.final_fee`). The fill-time estimate charges the open rate
and, when a later close consumes the lot, books the difference (intraday − open)
on that closing fill, so the running intraday total equals the exact EOD fees up
to per-trade rounding.

:class:`FloatFeeRates` is the float twin used on the risk engine's hot path
(frozen fees, pre-trade cost); it never touches ``Decimal``.
"""

from __future__ import annotations

import math
from collections import deque
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from decimal import Decimal

from futrisk.common.config.products import FeeSchedule
from futrisk.common.decimals import q_money
from futrisk.core.instruments import InstrumentRegistry
from futrisk.core.lots import PosKey
from futrisk.messages.enums import FeeType

LOT_MATCH = "LOT_MATCH"
SAME_DAY_OPENS_FIRST = "SAME_DAY_OPENS_FIRST"
INTRADAY_BOTH_LEGS = "INTRADAY_BOTH_LEGS"

# 一笔成交有两类腿时，fee_type 取"特殊"的那一类（平今 / 日内）
_PRIMARY_ORDER = (
    FeeType.INTRADAY_CLOSE,
    FeeType.CLOSE_TODAY,
    FeeType.INTRADAY_OPEN,
    FeeType.CLOSE_YD,
    FeeType.OPEN,
)


def leg_rate(schedule: FeeSchedule, fee_type: FeeType) -> Decimal:
    """Rate (``type=rate``) or per-lot amount (``type=per_lot``) of a fee type."""
    if fee_type == FeeType.OPEN:
        return schedule.open
    if fee_type == FeeType.CLOSE_YD:
        return schedule.close_yd
    if fee_type == FeeType.CLOSE_TODAY:
        return schedule.close_yd * schedule.close_today_mult
    if schedule.intraday is None:
        raise ValueError(f"fee schedule has no intraday rate for {fee_type}")
    return schedule.intraday


def leg_amount(schedule: FeeSchedule, fee_type: FeeType, price: Decimal, qty: int, multiplier: int) -> Decimal:
    """Unrounded fee of ``qty`` lots of one fee type."""
    rate = leg_rate(schedule, fee_type)
    if schedule.type == "per_lot":
        return rate * qty
    return price * multiplier * qty * rate


@dataclass(frozen=True, slots=True)
class FeeLeg:
    fee_type: FeeType
    qty: int
    rate: Decimal  # rate or per-lot amount, see ``TradeFee.per_lot``
    amount: Decimal  # unrounded


@dataclass(frozen=True, slots=True)
class TradeFee:
    """Exact fee of one trade (``trade_settlements.fee`` / ``fee_type`` / ``fee_rate|fee_per_lot``)."""

    fee: Decimal  # rounded HALF_UP to 0.01
    raw: Decimal  # unrounded sum of the legs
    fee_type: FeeType  # primary class, see module docstring
    per_lot: bool
    legs: tuple[FeeLeg, ...]

    @property
    def fee_rate(self) -> Decimal | None:
        """Rate of the primary leg for ``type=rate`` schedules, else None."""
        return None if self.per_lot else self._primary_rate()

    @property
    def fee_per_lot(self) -> Decimal | None:
        """Per-lot amount of the primary leg for ``type=per_lot`` schedules, else None."""
        return self._primary_rate() if self.per_lot else None

    def _primary_rate(self) -> Decimal | None:
        for leg in self.legs:
            if leg.fee_type == self.fee_type:
                return leg.rate
        return None

    def qty_of(self, *types: FeeType) -> int:
        return sum(leg.qty for leg in self.legs if leg.fee_type in types)


class FeeLegs:
    """Mutable fee classification of one fill: lots per :class:`FeeType`."""

    __slots__ = ("_by_type", "is_open", "price", "qty")

    def __init__(self, qty: int, price: Decimal, *, is_open: bool) -> None:
        self.qty = qty
        self.price = price
        self.is_open = is_open
        self._by_type: dict[FeeType, int] = {}

    def add(self, fee_type: FeeType, qty: int) -> None:
        if qty > 0:
            self._by_type[fee_type] = self._by_type.get(fee_type, 0) + qty

    def move(self, src: FeeType, dst: FeeType, qty: int) -> None:
        have = self._by_type.get(src, 0)
        if qty > have:
            raise ValueError(f"cannot move {qty} lots from {src}: only {have}")
        if qty == have:
            self._by_type.pop(src, None)
        else:
            self._by_type[src] = have - qty
        self.add(dst, qty)

    def get(self, fee_type: FeeType) -> int:
        return self._by_type.get(fee_type, 0)

    def items(self) -> list[tuple[FeeType, int]]:
        """Legs in a stable order (primary-first)."""
        return [(t, self._by_type[t]) for t in _PRIMARY_ORDER if self._by_type.get(t, 0) > 0]

    def as_dict(self) -> dict[FeeType, int]:
        return dict(self.items())

    def __iter__(self) -> Iterator[tuple[FeeType, int]]:
        return iter(self.items())

    def __repr__(self) -> str:
        inner = ", ".join(f"{t.value}={q}" for t, q in self.items())
        return f"FeeLegs({inner})"


def compute_fee(schedule: FeeSchedule, legs: FeeLegs | Mapping[FeeType, int], price: Decimal, multiplier: int) -> TradeFee:
    """Exact fee of one trade from its classification; rounded per trade."""
    items = legs.items() if isinstance(legs, FeeLegs) else [(t, q) for t, q in legs.items() if q > 0]
    fee_legs = tuple(
        FeeLeg(fee_type=t, qty=q, rate=leg_rate(schedule, t), amount=leg_amount(schedule, t, price, q, multiplier))
        for t, q in sorted(items, key=lambda x: _PRIMARY_ORDER.index(x[0]))
    )
    if not fee_legs:
        raise ValueError("a trade needs at least one fee leg")
    raw = sum((leg.amount for leg in fee_legs), Decimal(0))
    primary = next(t for t in _PRIMARY_ORDER if any(leg.fee_type == t for leg in fee_legs))
    return TradeFee(fee=q_money(raw), raw=raw, fee_type=primary, per_lot=schedule.type == "per_lot", legs=fee_legs)


@dataclass(slots=True)
class _OpenEntry:
    legs: FeeLegs
    unmatched: int


class DayFeeClassifier:
    """Chronological close-today / intraday classification of one trading day.

    Fills must be fed in chronological order (``trade_ts_ns``, then ``trade_seq``).
    Keys are position keys ``(account_id, instrument_id, PosDirection)``: a close
    only matches same-day opens of the same account and side.
    """

    def __init__(self) -> None:
        self._queues: dict[PosKey, deque[_OpenEntry]] = {}

    def open(self, key: PosKey, qty: int, price: Decimal, rule: str) -> FeeLegs:
        legs = FeeLegs(qty, price, is_open=True)
        legs.add(FeeType.OPEN, qty)
        if rule in (SAME_DAY_OPENS_FIRST, INTRADAY_BOTH_LEGS):
            self._queues.setdefault(key, deque()).append(_OpenEntry(legs, qty))
        return legs

    def unmatched_opens(self, key: PosKey) -> int:
        """Same-day opens of ``key`` not yet matched by closes (fee view)."""
        return sum(e.unmatched for e in self._queues.get(key, ()))

    def close(
        self,
        key: PosKey,
        qty: int,
        price: Decimal,
        rule: str,
        *,
        today_lot_qty: int = 0,
        intraday: bool = False,
    ) -> tuple[FeeLegs, list[tuple[FeeLegs, int]]]:
        """Classify a closing fill.

        ``today_lot_qty``: today lots consumed by the book (``FillResult.close_today_qty``);
        used by ``LOT_MATCH`` and ``INTRADAY_BOTH_LEGS``.
        ``intraday``: the schedule has an ``intraday`` rate (``INTRADAY_BOTH_LEGS``).
        Returns the fill's legs and the ``(open legs, qty)`` upgraded to
        ``INTRADAY_OPEN`` by this close.
        """
        legs = FeeLegs(qty, price, is_open=False)
        upgrades: list[tuple[FeeLegs, int]] = []
        if rule == LOT_MATCH:
            # 01 §8.15：SHFE 显式平今/平昨，按实际消耗的 lot 判定
            t = min(today_lot_qty, qty)
            legs.add(FeeType.CLOSE_TODAY, t)
            legs.add(FeeType.CLOSE_YD, qty - t)
            return legs, upgrades
        if rule == INTRADAY_BOTH_LEGS:
            # DCE 先开先平：只有实际平掉的今仓才算日内；被平今仓对应的开仓腿同样按日内收费
            target = min(today_lot_qty, qty)
        else:
            # 01 §8.15：CFFEX 先平当日新开仓（按时间顺序与当日开仓配对，与 lot 消耗顺序无关）
            target = qty
        both_legs = rule == INTRADAY_BOTH_LEGS and intraday
        matched = 0
        queue = self._queues.get(key)
        while queue and matched < target:
            entry = queue[0]
            take = min(entry.unmatched, target - matched)
            entry.unmatched -= take
            matched += take
            if both_legs:
                entry.legs.move(FeeType.OPEN, FeeType.INTRADAY_OPEN, take)
                upgrades.append((entry.legs, take))
            if entry.unmatched == 0:
                queue.popleft()
        legs.add(FeeType.INTRADAY_CLOSE if both_legs else FeeType.CLOSE_TODAY, matched)
        legs.add(FeeType.CLOSE_YD, qty - matched)
        return legs, upgrades

    def preview_close(
        self, key: PosKey, qty: int, rule: str, *, today_lot_qty: int = 0, intraday: bool = False
    ) -> dict[FeeType, int]:
        """Classification a close of ``qty`` would get right now (no state change)."""
        if rule == LOT_MATCH:
            t = min(today_lot_qty, qty)
            return {k: v for k, v in ((FeeType.CLOSE_TODAY, t), (FeeType.CLOSE_YD, qty - t)) if v > 0}
        if rule == INTRADAY_BOTH_LEGS:
            matched = min(qty, today_lot_qty, self.unmatched_opens(key))
        else:
            matched = min(qty, self.unmatched_opens(key))
        special = FeeType.INTRADAY_CLOSE if (rule == INTRADAY_BOTH_LEGS and intraday) else FeeType.CLOSE_TODAY
        return {k: v for k, v in ((special, matched), (FeeType.CLOSE_YD, qty - matched)) if v > 0}


@dataclass(frozen=True, slots=True)
class FillFee:
    """Fee view of one fill at the time it is applied."""

    legs: FeeLegs  # live object: an open may still be upgraded by later closes (DCE)
    estimate: Decimal  # fill-time estimate incl. intraday upgrades of earlier opens, rounded
    upgrades: tuple[tuple[FeeLegs, int], ...]


class DayFeeBook:
    """Fees of one trading day for any number of accounts.

    Feed fills chronologically with :meth:`on_fill`. Intraday, use
    ``FillFee.estimate`` (``fills.fee_est``); at EOD, call :meth:`final_fee` for
    every fill to get the exact per-trade fee.
    """

    def __init__(self, registry: InstrumentRegistry, trading_day: str) -> None:
        self._registry = registry
        self._trading_day = trading_day
        self._classifier = DayFeeClassifier()

    @property
    def trading_day(self) -> str:
        return self._trading_day

    @property
    def classifier(self) -> DayFeeClassifier:
        return self._classifier

    def set_registry(self, registry: InstrumentRegistry) -> None:
        """Use a reloaded registry (fee schedules) for fills from now on; the day's classification is kept."""
        self._registry = registry

    def schedule(self, instrument_id: str) -> FeeSchedule:
        return self._registry.fee_schedule(instrument_id, self._trading_day)

    def on_fill(
        self,
        key: PosKey,
        *,
        is_open: bool,
        qty: int,
        price: Decimal,
        today_lot_qty: int = 0,
    ) -> FillFee:
        """Classify one fill of position ``key`` and estimate its fee now."""
        instrument_id = key[1]
        sched = self.schedule(instrument_id)
        rule = self._registry.close_today_fee_rule(instrument_id)
        mult = self._registry.multiplier(instrument_id)
        if is_open:
            legs = self._classifier.open(key, qty, price, rule)
            est = compute_fee(sched, legs, price, mult).fee
            return FillFee(legs=legs, estimate=est, upgrades=())
        legs, upgrades = self._classifier.close(
            key, qty, price, rule, today_lot_qty=today_lot_qty, intraday=sched.intraday is not None
        )
        raw = compute_fee(sched, legs, price, mult).raw
        for open_legs, n in upgrades:
            # 日内两腿：此前按开仓费率预估的开仓腿，在配对时补记差额
            raw += leg_amount(sched, FeeType.INTRADAY_OPEN, open_legs.price, n, mult)
            raw -= leg_amount(sched, FeeType.OPEN, open_legs.price, n, mult)
        return FillFee(legs=legs, estimate=q_money(raw), upgrades=tuple(upgrades))

    def estimate_order(
        self,
        key: PosKey,
        *,
        is_open: bool,
        qty: int,
        price: Decimal,
        today_lot_qty: int = 0,
    ) -> Decimal:
        """Fee a new order would pay if filled now (frozen fee, what-if); no state change."""
        instrument_id = key[1]
        sched = self.schedule(instrument_id)
        mult = self._registry.multiplier(instrument_id)
        if is_open:
            return compute_fee(sched, {FeeType.OPEN: qty}, price, mult).fee
        rule = self._registry.close_today_fee_rule(instrument_id)
        legs = self._classifier.preview_close(
            key, qty, rule, today_lot_qty=today_lot_qty, intraday=sched.intraday is not None
        )
        return compute_fee(sched, legs, price, mult).fee

    def final_fee(self, instrument_id: str, legs: FeeLegs) -> TradeFee:
        """Exact per-trade fee once all of the day's fills have been classified."""
        return compute_fee(self.schedule(instrument_id), legs, legs.price, self._registry.multiplier(instrument_id))


def round_cents(x: float) -> float:
    """Round a non-negative float amount half-up to 0.01, robust to binary representation.

    ``15.525`` is stored as 15.52499999...; the tiny epsilon makes it round up as
    ``Decimal`` ROUND_HALF_UP of the exact value does.
    """
    return math.floor(x * 100.0 + 0.5 + 1e-7) / 100.0


@dataclass(frozen=True, slots=True)
class FloatFeeRates:
    """Float fee rates of one contract for the hot path (01 §8.3: 盘中估算用 float)."""

    per_lot: bool
    multiplier: float
    open: float
    close_yd: float
    close_today: float
    intraday: float | None

    @classmethod
    def from_schedule(cls, schedule: FeeSchedule, multiplier: int) -> FloatFeeRates:
        return cls(
            per_lot=schedule.type == "per_lot",
            multiplier=float(multiplier),
            open=float(schedule.open),
            close_yd=float(schedule.close_yd),
            close_today=float(schedule.close_yd * schedule.close_today_mult),
            intraday=float(schedule.intraday) if schedule.intraday is not None else None,
        )

    def rate(self, fee_type: FeeType) -> float:
        if fee_type == FeeType.OPEN:
            return self.open
        if fee_type == FeeType.CLOSE_YD:
            return self.close_yd
        if fee_type == FeeType.CLOSE_TODAY:
            return self.close_today
        if self.intraday is None:
            raise ValueError(f"fee schedule has no intraday rate for {fee_type}")
        return self.intraday

    def amount(self, fee_type: FeeType, price: float, qty: int) -> float:
        """Unrounded fee of one leg (same formula as :func:`leg_amount`)."""
        if self.per_lot:
            return self.rate(fee_type) * qty
        return price * self.multiplier * qty * self.rate(fee_type)

    def fee(self, legs: Iterable[tuple[FeeType, int]], price: float) -> float:
        """Per-trade fee of the legs, rounded half-up to 0.01 like :func:`compute_fee`."""
        return round_cents(sum(self.amount(t, price, q) for t, q in legs if q > 0))

    def open_fee(self, price: float, qty: int) -> float:
        return round_cents(self.amount(FeeType.OPEN, price, qty))


__all__ = [
    "INTRADAY_BOTH_LEGS",
    "LOT_MATCH",
    "SAME_DAY_OPENS_FIRST",
    "DayFeeBook",
    "DayFeeClassifier",
    "FeeLeg",
    "FeeLegs",
    "FillFee",
    "FloatFeeRates",
    "TradeFee",
    "compute_fee",
    "leg_amount",
    "leg_rate",
    "round_cents",
]
