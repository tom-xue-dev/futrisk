"""Margin: gross per contract, big side by amount, exclusions (01 §8.15 单向大边, 03 §4.4 margin_groups).

Gross margin of a position = price × multiplier × qty × company rate
(exchange rate + broker add-on, :meth:`InstrumentRegistry.margin_rate`).

Contracts are grouped as stored in ``margin_groups.margin_group``:

- ``SHFE:{product}`` (``big_side=SAME_PRODUCT``): long and short of the same
  product across months;
- ``CFFEX:INDEX`` / ``CFFEX:TREASURY`` (``big_side=GROUPS``): across products;
- ``GROSS:{instrument_id}``: exchanges without big-side margin (DCE).

Big-side groups charge ``max(long_margin, short_margin) + excluded_margin``,
compared **by amount**, not by lots (golden case 3: 8 long lots at 10% beat 10
short lots at 7%). A contract that has left the group (SHFE: from the L−5
settlement on; CFFEX: physically delivered contracts from the close of the day
before the delivery month) is charged two-way in ``excluded_margin``. GROSS
groups charge long + short.

Two numeric modes share one algorithm:

- settlement (:func:`settlement_margin`): ``Decimal``, each contract's gross
  margin quantized to 0.01 ROUND_HALF_UP before aggregation, so that
  ``position_settlements.margin_gross`` adds up to the group values;
- intraday (:func:`intraday_margin`, :func:`frozen_margin`): ``float`` for speed
  (01 §8.3), with the intraday exclusion semantics.

Frozen margin for resting open orders (:func:`frozen_margin`) defaults to the
**incremental** big-side rule: ``charged(positions + open orders) −
charged(positions)``, with each order valued at its own price. On SHFE an order
on the smaller side therefore freezes nothing as long as that side stays the
smaller one by amount, and freezes only the excess once it would flip the big
side; this mirrors what the exchange will actually charge after the fill (the
CTP behaviour for single-side margin). Because ``max`` is monotone, assuming
all resting orders fill is the worst case. ``mode="GROSS"`` freezes the full
gross margin of every open order instead (conservative).
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Generic, Literal, TypeVar

from futrisk.common.decimals import D, q_money
from futrisk.core._num import money_str, price_str
from futrisk.core.instruments import InstrumentRegistry, MarginMethod
from futrisk.messages.enums import PosDirection

N = TypeVar("N", Decimal, float)


@dataclass(frozen=True, slots=True)
class MarginEntry:
    """A priced quantity to margin: a position or a resting open order."""

    instrument_id: str
    direction: PosDirection
    qty: int
    price: Decimal | float


@dataclass(frozen=True, slots=True)
class ContractMargin(Generic[N]):
    instrument_id: str
    direction: PosDirection
    qty: int
    price: N
    rate: N
    margin: N  # gross, before big side
    excluded: bool


@dataclass(frozen=True, slots=True)
class MarginGroup(Generic[N]):
    """One ``margin_groups`` row."""

    margin_group: str
    method: MarginMethod
    long_margin: N
    short_margin: N
    excluded_margin: N
    charged_margin: N
    contracts: tuple[ContractMargin[N], ...]

    def detail(self) -> dict[str, Any]:
        """JSON-able ``margin_groups.detail``: the contracts of the group."""
        rows: list[dict[str, Any]] = []
        for c in self.contracts:
            if isinstance(c.margin, Decimal):
                price: Any = price_str(D(c.price))
                rate: Any = price_str(D(c.rate))
                margin: Any = money_str(c.margin)
            else:
                price, rate, margin = c.price, c.rate, c.margin
            rows.append(
                {
                    "instrument_id": c.instrument_id,
                    "direction": c.direction.value,
                    "qty": c.qty,
                    "price": price,
                    "rate": rate,
                    "margin": margin,
                    "excluded": c.excluded,
                }
            )
        return {"contracts": rows}


@dataclass(frozen=True, slots=True)
class AccountMargin(Generic[N]):
    """Margin of one account: groups sorted by id, plus the total charged."""

    groups: tuple[MarginGroup[N], ...]
    total: N

    def gross_by_position(self) -> dict[tuple[str, PosDirection], N]:
        """Gross (pre-big-side) margin per ``(instrument_id, direction)``."""
        out: dict[tuple[str, PosDirection], N] = {}
        for g in self.groups:
            for c in g.contracts:
                k = (c.instrument_id, c.direction)
                out[k] = out[k] + c.margin if k in out else c.margin
        return out

    def charged_by_group(self) -> dict[str, N]:
        return {g.margin_group: g.charged_margin for g in self.groups}


def group_charge(method: MarginMethod, long_margin: N, short_margin: N, excluded_margin: N) -> N:
    """Charged margin of one group: GROSS = long + short (+ excluded); BIG_SIDE = max(long, short) + excluded.

    Shared by the settlement path and the risk engine's incremental ledger so the
    two always apply the same rule (01 §8.15：单向大边按保证金金额取较大一边).
    """
    if method == MarginMethod.GROSS:
        return long_margin + short_margin + excluded_margin
    return max(long_margin, short_margin) + excluded_margin


def _compute(
    entries: Iterable[MarginEntry],
    registry: InstrumentRegistry,
    trading_day: str,
    *,
    intraday: bool,
    num: Callable[[Decimal | float | int], N],
    quantize: Callable[[N], N],
    zero: N,
) -> AccountMargin[N]:
    by_group: dict[str, tuple[MarginMethod, list[ContractMargin[N]]]] = {}
    for e in entries:
        if e.qty == 0:
            continue
        if e.qty < 0:
            raise ValueError(f"negative margin qty for {e.instrument_id}: {e.qty}")
        group, method = registry.margin_group(e.instrument_id)
        rate = num(registry.margin_rate(e.instrument_id, trading_day))
        price = num(e.price)
        gross = quantize(price * registry.multiplier(e.instrument_id) * e.qty * rate)
        excluded = method == MarginMethod.BIG_SIDE and registry.big_side_excluded(
            e.instrument_id, trading_day, intraday=intraday
        )
        by_group.setdefault(group, (method, []))[1].append(
            ContractMargin(e.instrument_id, e.direction, e.qty, price, rate, gross, excluded)
        )
    groups: list[MarginGroup[N]] = []
    total = zero
    for group in sorted(by_group):
        method, contracts = by_group[group]
        long_m, short_m, excl = zero, zero, zero
        for c in contracts:
            if c.excluded:
                # 01 §8.15：已退出单向大边的合约按双边收取
                excl += c.margin
            elif c.direction == PosDirection.LONG:
                long_m += c.margin
            else:
                short_m += c.margin
        charged = group_charge(method, long_m, short_m, excl)
        groups.append(MarginGroup(group, method, long_m, short_m, excl, charged, tuple(contracts)))
        total += charged
    return AccountMargin(tuple(groups), total)


def _dec(x: Decimal | float | int) -> Decimal:
    return D(x)


def _flt(x: Decimal | float | int) -> float:
    return float(x)


def _identity(x: float) -> float:
    return x


def margin_exact(
    entries: Iterable[MarginEntry], registry: InstrumentRegistry, trading_day: str, *, intraday: bool = False
) -> AccountMargin[Decimal]:
    """Exact (Decimal) margin of arbitrary priced entries."""
    return _compute(entries, registry, trading_day, intraday=intraday, num=_dec, quantize=q_money, zero=Decimal(0))


def margin_float(
    entries: Iterable[MarginEntry], registry: InstrumentRegistry, trading_day: str, *, intraday: bool = True
) -> AccountMargin[float]:
    """Float margin of arbitrary priced entries (intraday valuation)."""
    return _compute(entries, registry, trading_day, intraday=intraday, num=_flt, quantize=_identity, zero=0.0)


def _entries(
    positions: Iterable[tuple[str, PosDirection, int]], prices: Mapping[str, Decimal] | Mapping[str, float]
) -> list[MarginEntry]:
    out: list[MarginEntry] = []
    for instrument_id, direction, qty in positions:
        if qty == 0:
            continue
        if instrument_id not in prices:
            raise KeyError(f"no price to margin {instrument_id}")
        out.append(MarginEntry(instrument_id, direction, qty, prices[instrument_id]))
    return out


def settlement_margin(
    positions: Iterable[tuple[str, PosDirection, int]],
    settle_prices: Mapping[str, Decimal],
    registry: InstrumentRegistry,
    trading_day: str,
) -> AccountMargin[Decimal]:
    """Settlement margin of end-of-day positions ``(instrument_id, direction, qty)`` at settlement prices."""
    return margin_exact(_entries(positions, settle_prices), registry, trading_day, intraday=False)


def intraday_margin(
    positions: Iterable[tuple[str, PosDirection, int]],
    prices: Mapping[str, float],
    registry: InstrumentRegistry,
    trading_day: str,
) -> AccountMargin[float]:
    """Intraday margin at the given marks (e.g. last prices; limit price when locked)."""
    return margin_float(_entries(positions, prices), registry, trading_day, intraday=True)


def frozen_margin(
    positions: Iterable[tuple[str, PosDirection, int]],
    prices: Mapping[str, float],
    open_orders: Iterable[MarginEntry],
    registry: InstrumentRegistry,
    trading_day: str,
    *,
    mode: Literal["INCREMENTAL", "GROSS"] = "INCREMENTAL",
) -> float:
    """Margin to freeze for resting open orders (see the module docstring for the rule)."""
    orders = [o for o in open_orders if o.qty > 0]
    if not orders:
        return 0.0
    if mode == "GROSS":
        return margin_float(orders, registry, trading_day).total
    held = _entries(positions, prices)
    before = margin_float(held, registry, trading_day).total
    after = margin_float(held + orders, registry, trading_day).total
    return max(0.0, after - before)


__all__ = [
    "AccountMargin",
    "ContractMargin",
    "MarginEntry",
    "MarginGroup",
    "MarginMethod",
    "frozen_margin",
    "group_charge",
    "intraday_margin",
    "margin_exact",
    "margin_float",
    "settlement_margin",
]
