"""EOD step 4: settlement prices (01 §8.15 结算价, 02 §8.4 step 4, 03 §4.4 ``official_prices``).

Priority per contract: **manual override > official > VWAP > the exchange's
fallback chain** (:func:`futrisk.core.determine_settlement_price`). Inputs:

* ``SettleInput`` from ``md.settle_inputs`` (md-gateway recomputes them from the
  canonical ticks);
* the latest active ``price_overrides`` row per contract (``settle.override_price``);
* the latest ``official_prices`` import per contract (``settle.import_official``);
* the previous settlement prices of the pinned SOD snapshot.

Previous settlement price (``pre_settle``) of a contract: **the SOD snapshot's
value first** (clearing's own settlement, the basis of the SOD lots), md-gateway's
``SettleInput.pre_settle`` only for contracts the snapshot does not know. When
both exist and differ, the SOD value is used and the difference is reported
(``pre_mismatch``, a WARN of the run); the same value feeds the ``MEDIAN``,
``LIMIT`` and ``BENCHMARK`` fallback steps, so P&L on a day without trades and
without a price move stays zero.

The price set covers every held or traded contract (``needed``, otherwise
``missing``), every contract with an md input or a manual/official price, and
**every contract of the SOD's ``pre_settle``** (priced by the fallback chain,
usually ``PRE_SETTLE``), so that the next SOD carries the previous settlement
price forward for every tradable contract.

CFFEX ``BENCHMARK`` (中金所结算细则第四十三条): the benchmark is
the same-product contract **nearest to delivery among those that traded today**
(the contract being priced excluded), not the most traded one - near expiry the
volume moves to the next month while the benchmark stays the front month. The
fallback adds the benchmark's settlement **point** change (settle − pre_settle,
both of this run) to the contract's own previous settlement price and clamps the
result into the limit band (:func:`futrisk.core.determine_settlement_price`); it
is not a percentage move. Without a traded same-product contract the step is
skipped and the chain ends at ``PRE_SETTLE``.

A contract traded today that neither the SOD nor md-gateway knows (a new
contract while md-gateway is down) gets its ``pre_settle`` for the settlement
from :func:`fallback_pre_settle`: an earlier run's md input of the same day, or
today's manual/official price.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from decimal import Decimal

import msgspec
from loguru import logger

from futrisk.common.decimals import D
from futrisk.core import BenchmarkMove, InstrumentRegistry, ManualPrice, determine_settlement_price
from futrisk.core.instruments import delivery_month_of
from futrisk.messages.enums import Exchange, SettlePriceSource
from futrisk.messages.eod import SettlementPriceView
from futrisk.messages.md import SettleInput
from futrisk.services.clearing.store import OverrideRow

_OPERATOR_SOURCES = frozenset({SettlePriceSource.MANUAL, SettlePriceSource.OFFICIAL})


@dataclass
class PriceDecision:
    """Outcome of :func:`decide_prices`."""

    views: dict[str, SettlementPriceView] = field(default_factory=dict)
    missing: list[str] = field(default_factory=list)
    skipped: dict[str, str] = field(default_factory=dict)
    pre_mismatch: dict[str, dict[str, str]] = field(default_factory=dict)  # inst -> {"sod": x, "md": y}

    def sources(self) -> dict[str, int]:
        out: dict[str, int] = {}
        for v in self.views.values():
            out[v.source.value] = out.get(v.source.value, 0) + 1
        return dict(sorted(out.items()))


def decide_prices(
    *,
    registry: InstrumentRegistry,
    trading_day: str,
    version: int,
    needed: Iterable[str],
    inputs: Mapping[str, SettleInput],
    overrides: Mapping[str, OverrideRow],
    official: Mapping[str, str],
    sod_pre: Mapping[str, str],
) -> PriceDecision:
    """Decide the settlement price of every needed contract (and of every contract with an input or SOD price).

    ``needed`` (held at SOD or traded today) must all be priced, otherwise they
    are reported in ``missing``. Contracts unknown to the registry are skipped.
    """
    need = sorted(set(needed))
    universe = sorted(set(need) | set(inputs) | set(overrides) | set(official) | set(sod_pre))
    out = PriceDecision()

    def pre_of(inst: str) -> Decimal | None:
        sod = sod_pre.get(inst)
        inp = inputs.get(inst)
        if sod is not None:
            if inp is not None and D(inp.pre_settle) != D(sod):
                out.pre_mismatch[inst] = {"sod": sod, "md": inp.pre_settle}
            return D(sod)
        return D(inp.pre_settle) if inp is not None else None

    def decide(inst: str, benchmark: BenchmarkMove | None) -> SettlementPriceView | None:
        inp = inputs.get(inst)
        ov = overrides.get(inst)
        manual = ManualPrice(price=D(ov.price), operator=ov.actor, reason=ov.reason) if ov is not None else None
        off = D(official[inst]) if inst in official else None
        pre = pre_of(inst)
        if inp is None and manual is None and off is None and pre is None:
            return None
        view = determine_settlement_price(
            inp,
            registry,
            instrument_id=inst,
            trading_day=trading_day,
            pre_settle=pre,
            manual=manual,
            official=off,
            benchmark=benchmark,
            version=version,
        )
        # 结算版本盖章：视图的 version 是本次结算的版本（03 §4.4 settlement_prices 主键）
        if view.version != version:
            view = msgspec.structs.replace(view, version=version)
        return view

    # pass 1: contracts that trade (or carry a manual/official price); pass 2 the others with benchmarks
    deferred: list[str] = []
    for inst in universe:
        if not registry.has(inst):
            out.skipped[inst] = "unknown instrument"
            logger.warning("prices.unknown_instrument", trading_day=trading_day, instrument_id=inst)
            continue
        inp = inputs.get(inst)
        traded = inp is not None and inp.day_volume > 0
        if traded or inst in overrides or inst in official:
            view = decide(inst, None)
            if view is not None:
                out.views[inst] = view
        else:
            deferred.append(inst)
    for inst in deferred:
        view = decide(inst, _benchmark_for(inst, registry, out.views, inputs, pre_of))
        if view is not None:
            out.views[inst] = view
        elif inst not in need:
            out.skipped[inst] = "no input and no previous settlement price"
    out.missing = [i for i in need if i not in out.views]
    if out.pre_mismatch:
        logger.warning("prices.pre_settle_mismatch", trading_day=trading_day, version=version, detail=out.pre_mismatch)
    return out


def _benchmark_for(
    inst: str,
    registry: InstrumentRegistry,
    views: Mapping[str, SettlementPriceView],
    inputs: Mapping[str, SettleInput],
    pre_of: Callable[[str], Decimal | None],
) -> BenchmarkMove | None:
    """CFFEX ``FALLBACK_BENCHMARK`` move (结算细则第四十三条, see the module docstring).

    Candidates: same product, not ``inst``, traded today (``day_volume > 0``) and
    already priced in this run; the benchmark is the one with the earliest
    delivery month. Returns its point move (settle, pre_settle).
    """
    if registry.exchange(inst) != Exchange.CFFEX:
        return None
    product = registry.product(inst)
    best: tuple[str, str] | None = None
    for other in views:
        if other == inst or registry.product(other) != product:
            continue
        inp = inputs.get(other)
        if inp is None or inp.day_volume <= 0:  # 当日有成交
            continue
        key = (delivery_month_of(other) or other, other)  # 离交割月最近：交割月最早
        if best is None or key < best:
            best = key
    if best is None:
        return None
    other = best[1]
    pre = pre_of(other)
    if pre is None:
        return None
    return BenchmarkMove(settle=D(views[other].price), pre_settle=pre)


def fallback_pre_settle(
    *,
    touched: Iterable[str],
    sod_pre: Mapping[str, str],
    inputs: Mapping[str, SettleInput],
    earlier_inputs: Mapping[str, str],
    views: Mapping[str, SettlementPriceView],
) -> dict[str, dict[str, str]]:
    """``pre_settle`` for held/traded contracts that neither the SOD nor today's md inputs know.

    Order: the md input's ``pre_settle`` recorded by an earlier run of the same
    day, then today's manual or official settlement price. Returns
    ``{inst: {"price": ..., "source": ...}}``.
    """
    out: dict[str, dict[str, str]] = {}
    for inst in sorted(set(touched)):
        if inst in sod_pre or inst in inputs:
            continue
        if inst in earlier_inputs:
            out[inst] = {"price": earlier_inputs[inst], "source": "md_input_of_earlier_run"}
            continue
        view = views.get(inst)
        if view is not None and view.source in _OPERATOR_SOURCES:
            out[inst] = {"price": view.price, "source": view.source.value}
    return out


def price_changes(
    imported: Mapping[str, str], reference: Mapping[str, str]
) -> dict[str, tuple[str, str]]:
    """Contracts whose imported price differs from ``reference`` (compared as decimals)."""
    out: dict[str, tuple[str, str]] = {}
    for inst, px in sorted(imported.items()):
        ref = reference.get(inst)
        if ref is not None and D(ref) != D(px):
            out[inst] = (ref, px)
    return out


__all__ = ["PriceDecision", "decide_prices", "fallback_pre_settle", "price_changes"]
