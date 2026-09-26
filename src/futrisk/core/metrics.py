"""Account metrics and the metric registry (01 §8.14 指标, 02 §7.6 AccountMetricsView / ExposureView).

:func:`build_valuation` turns one account's :class:`IntradayLedger` (funds at
the current marks) into an :class:`AccountValuation` with exactly the 02 §7.6
definitions:

- daily P&L = equity − static equity (static = pre_balance + net deposit, cash
  is never counted as P&L); ``daily_pnl_pct_init`` (acceptance metric, ÷ initial
  capital), ``daily_pnl_pct_sod`` (÷ static equity), ``floating_pnl_pct_init``
  (position P&L ÷ initial capital, the ``floating_only`` variant);
- performance equity = equity − cumulative net deposit; NAV = perf ÷ initial;
  intraday peak / drawdown and high-water mark / drawdown on performance equity;
- gross / net notional, leverage = gross ÷ equity, effective N = 1/HHI of the
  products' |net notional|, limit stress = Σ |net_p| × limit_pct_p ÷ equity;
- per product: net % of equity and utilization = net_pct / (3% ÷ limit_pct);
  per sector: net % of equity; compliance utilization.

Metric names used by ``risk_rules.yaml`` resolve through the registry
(``@register_metric(name)``): ``account.*`` (AccountMetricsView and
AccountFunds fields, ``account.compliance_util``), ``product.*`` (ExposureView
fields, scope ``account/product``) and ``sector.*`` (scope ``account/sector``).
Metrics that do not apply return None (e.g. effective N without positions).
Each metric declares its ``baseline`` (the neutral point of a lower-is-worse
metric for the hard-limit utilization, 02 §7.6 ``RuleStatus.hard_utilization``):
0 by default, 1 for ``nav``, +inf for ``effective_n``.

Quality of a product / sector / instrument is the worst ``MdQuality`` of its
held instruments. The account quality applies a materiality tolerance
(``system.quality_stale_tolerance``): while the held instruments that are not
OK carry at most that share of the gross notional, the account quality is OK
(account-scope rules keep running on the last known marks); above it, it is
the worst quality of the held instruments.

:meth:`AccountValuation.whatif` produces the valuation "after this order" used
by the pre-trade what-if step (02 §11.2 step 8); :class:`WhatIf` gives the same
numbers metric by metric without building the whole view (fast path, with a
fallback to :meth:`AccountValuation.whatif` for the metrics it does not know).
:class:`StaticCache` keeps the per-instrument static data of one trading day
(product, sector, exchange, multiplier, limit pct) out of the hot path.
"""

from __future__ import annotations

import math
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any, NamedTuple

import msgspec

from futrisk.core.instruments import InstrumentRegistry
from futrisk.core.ledger import IntradayLedger
from futrisk.messages.enums import Exchange, MdQuality, PosDirection
from futrisk.messages.risk import AccountFunds, AccountMetricsView, ExposureView

RATIO_CAP = 9999.0
LIMIT_LOSS_BUDGET = 0.03  # 01 §8.14：单品种上限 = 3% ÷ 涨跌停幅度

_QUALITY_RANK = {MdQuality.OK: 0, MdQuality.STALE: 1, MdQuality.UNKNOWN: 2}
_BY_RANK = (MdQuality.OK, MdQuality.STALE, MdQuality.UNKNOWN)


def worst_quality(qualities: Iterable[MdQuality]) -> MdQuality:
    worst = MdQuality.OK
    for q in qualities:
        if _QUALITY_RANK[q] > _QUALITY_RANK[worst]:
            worst = q
    return worst


def _ratio(num: float, den: float) -> float:
    if den > 0:
        return num / den
    if num == 0:
        return 0.0
    return RATIO_CAP if num > 0 else -RATIO_CAP


@dataclass(frozen=True, slots=True)
class StaticInfo:
    """Static data of one instrument on one trading day (limit pct is date effective)."""

    product: str
    sector: str
    exchange: Exchange
    multiplier: int
    limit_pct: float


class StaticCache:
    """Per-instrument :class:`StaticInfo` of one trading day (``limit_pct_on`` is evaluated once)."""

    __slots__ = ("_d", "registry", "trading_day")

    def __init__(self, registry: InstrumentRegistry, trading_day: str) -> None:
        self.registry = registry
        self.trading_day = trading_day
        self._d: dict[str, StaticInfo] = {}

    def get(self, instrument_id: str) -> StaticInfo:
        s = self._d.get(instrument_id)
        if s is None:
            info = self.registry.get(instrument_id)
            s = StaticInfo(info.product, info.sector, info.exchange, info.multiplier,
                           float(info.spec.limit_pct_on(self.trading_day)))
            self._d[instrument_id] = s
        return s


class HeldPosition(NamedTuple):
    """One non-flat position valued at the current mark (a NamedTuple: built for every position per evaluation)."""

    instrument_id: str
    product: str
    sector: str
    exchange: Exchange
    direction: PosDirection
    qty: int
    mark: float
    multiplier: int
    notional: float  # mark × multiplier × qty (always positive)
    position_pnl: float
    limit_pct: float

    @property
    def signed_notional(self) -> float:
        return self.notional if self.direction == PosDirection.LONG else -self.notional


class SectorExposure(NamedTuple):
    sector: str
    net_notional: float
    gross_notional: float
    net_pct_equity: float
    gross_pct_equity: float


@dataclass(frozen=True, slots=True)
class ComplianceCounters:
    """Inputs of ``account.compliance_util`` (01 §8.14 合规维度)."""

    open_volume: Mapping[str, int] = field(default_factory=dict)  # instrument -> lots opened today
    cancels: Mapping[str, int] = field(default_factory=dict)  # instrument -> cancels today
    large_cancels: Mapping[str, int] = field(default_factory=dict)
    self_trades: int = 0
    positions: Mapping[str, int] = field(default_factory=dict)  # instrument -> position of the larger side (限仓)
    order_rate: float = 0.0  # order-rate throttle in use: 1 - tokens / burst of the token bucket


@dataclass(slots=True)
class AccountValuation:
    """Everything the rules, snapshots and what-if need about one account at one instant."""

    account_id: str
    funds: AccountFunds
    metrics: AccountMetricsView
    exposures: Mapping[str, ExposureView]  # product -> view
    sectors: Mapping[str, SectorExposure]
    positions: tuple[HeldPosition, ...]
    quality: MdQuality
    product_quality: Mapping[str, MdQuality]
    sector_quality: Mapping[str, MdQuality]
    instrument_quality: Mapping[str, MdQuality]
    compliance_util: float
    hwm_sod: float
    limit_pct: Mapping[str, float]  # product -> limit pct (date effective)
    product_sector: Mapping[str, str]

    # ------------------------------------------------------------ helpers
    def product_pnl(self, product: str, direction: PosDirection) -> float:
        """Position P&L of one side of a product (for NO_ADD_TO_LOSERS)."""
        return sum(p.position_pnl for p in self.positions if p.product == product and p.direction == direction)

    def largest_product(self) -> str | None:
        """Product with the largest |net notional| (ties: alphabetical)."""
        best: tuple[float, str] | None = None
        for prod, ev in self.exposures.items():
            cand = (abs(ev.net_notional), prod)
            if cand[0] <= 0:
                continue
            if best is None or cand[0] > best[0] or (cand[0] == best[0] and cand[1] < best[1]):
                best = cand
        return best[1] if best is not None else None

    def quality_for(self, scope_kind: str | None, key: str | None) -> MdQuality:
        if scope_kind == "prod" and key is not None:
            return self.product_quality.get(key, MdQuality.OK)
        if scope_kind == "sector" and key is not None:
            return self.sector_quality.get(key, MdQuality.OK)
        if scope_kind == "inst" and key is not None:
            return self.instrument_quality.get(key, MdQuality.OK)
        return self.quality

    def unknown_exposure_share(self) -> float:
        """Share of gross notional in instruments whose quality is UNKNOWN (01 §8.5)."""
        gross = sum(p.notional for p in self.positions)
        if gross <= 0:
            return 0.0
        unknown = sum(p.notional for p in self.positions if self.instrument_quality.get(p.instrument_id) == MdQuality.UNKNOWN)
        return unknown / gross

    def whatif(self, delta: OrderDelta) -> AccountValuation:
        """Valuation after ``delta`` fills at once (02 §11.2 step 8 "当前指标 + 本单影响").

        Account totals, the order's product and the order's sector are
        recomputed; other products / sectors keep their current views (the
        what-if step only evaluates the order's own scopes, which keeps the
        pre-trade check within its latency budget, 01 §8.12).
        """
        f = self.funds
        equity = f.equity + delta.equity_delta
        margin = f.margin + delta.margin_delta
        position_pnl = f.position_pnl + delta.pnl_delta
        fee = f.fee + delta.fee
        funds = replace_struct(
            f,
            equity=equity,
            margin=margin,
            position_pnl=position_pnl,
            fee=fee,
            available=equity - margin - f.frozen_margin - f.frozen_fee,
            risk_degree=_ratio(margin, equity),
        )
        exposures = dict(self.exposures)
        prev = exposures.get(delta.product)
        net = (prev.net_notional if prev else 0.0) + delta.signed_notional
        gross_p = (prev.gross_notional if prev else 0.0) + abs(delta.signed_notional)
        lp = self.limit_pct.get(delta.product, delta.limit_pct)
        cap = LIMIT_LOSS_BUDGET / lp if lp > 0 else None
        net_pct = _ratio(abs(net), equity)
        exposures[delta.product] = ExposureView(
            product=delta.product,
            sector=delta.sector,
            exchange=prev.exchange if prev else delta.exchange,
            long_lots=(prev.long_lots if prev else 0) + (delta.qty if delta.signed_notional > 0 else 0),
            short_lots=(prev.short_lots if prev else 0) + (delta.qty if delta.signed_notional < 0 else 0),
            net_lots=(prev.net_lots if prev else 0) + (delta.qty if delta.signed_notional > 0 else -delta.qty),
            net_notional=net,
            gross_notional=gross_p,
            net_pct_equity=net_pct,
            cap_pct=cap,
            utilization=net_pct / cap if cap else None,
        )
        sectors = dict(self.sectors)
        s_net = sum(ev.net_notional for ev in exposures.values() if ev.sector == delta.sector)
        s_gross = sum(ev.gross_notional for ev in exposures.values() if ev.sector == delta.sector)
        sectors[delta.sector] = SectorExposure(delta.sector, s_net, s_gross, _ratio(abs(s_net), equity), _ratio(s_gross, equity))
        limit_pct = dict(self.limit_pct)
        limit_pct.setdefault(delta.product, delta.limit_pct)
        m = self.metrics
        gross = m.gross_notional + abs(delta.signed_notional)
        net_total = m.net_notional + delta.signed_notional
        daily = equity - f.static_equity
        perf = equity - f.cum_net_deposit
        peak = max(m.intraday_peak_equity, perf)
        hwm = max(self.hwm_sod, peak)
        metrics = replace_struct(
            m,
            daily_pnl=daily,
            daily_pnl_pct_init=_ratio(daily, f.initial_capital),
            daily_pnl_pct_sod=_ratio(daily, f.static_equity),
            floating_pnl_pct_init=_ratio(position_pnl, f.initial_capital),
            perf_equity=perf,
            nav=_ratio(perf, f.initial_capital),
            intraday_peak_equity=peak,
            intraday_dd=_ratio(peak - perf, peak) if peak > 0 else 0.0,
            hwm_equity=hwm,
            hwm_dd=_ratio(hwm - perf, hwm) if hwm > 0 else 0.0,
            gross_notional=gross,
            net_notional=net_total,
            leverage=_ratio(gross, equity),
            effective_n=effective_n_of(exposures.values()) or 0.0,
            limit_stress=_ratio(sum(abs(ev.net_notional) * limit_pct.get(p, 0.0) for p, ev in exposures.items()), equity),
        )
        product_sector = dict(self.product_sector)
        product_sector.setdefault(delta.product, delta.sector)
        return AccountValuation(
            account_id=self.account_id,
            funds=funds,
            metrics=metrics,
            exposures=exposures,
            sectors=sectors,
            positions=self.positions,
            quality=self.quality,
            product_quality=self.product_quality,
            sector_quality=self.sector_quality,
            instrument_quality=self.instrument_quality,
            compliance_util=self.compliance_util,
            hwm_sod=self.hwm_sod,
            limit_pct=limit_pct,
            product_sector=product_sector,
        )


@dataclass(frozen=True, slots=True)
class OrderDelta:
    """Immediate effect of an opening order if it filled now (what-if)."""

    instrument_id: str
    product: str
    sector: str
    exchange: Exchange
    qty: int
    signed_notional: float  # ± mark × multiplier × qty
    pnl_delta: float  # (mark − price) × multiplier × qty × side
    fee: float
    margin_delta: float
    limit_pct: float

    @property
    def equity_delta(self) -> float:
        return self.pnl_delta - self.fee


WhatIfFn = Callable[["WhatIf", str | None], float | None]


class WhatIf:
    """Metric values "after this order" without building the whole view (02 §11.2 step 8, fast path).

    Every value equals ``metric_value(name, valuation.whatif(delta), key)``:
    the account totals, the order's product and the order's sector use the
    same expressions as :meth:`AccountValuation.whatif`; other products and
    sectors keep their current views. Metrics without a scalar formula
    (``account.effective_n``, ``account.limit_stress``, custom registrations)
    fall back to the full :meth:`AccountValuation.whatif` view (built once).
    """

    __slots__ = ("_full", "_prod", "daily", "delta", "equity", "fee", "gross", "hwm", "margin", "net", "peak",
                 "perf", "position_pnl", "val")

    def __init__(self, val: AccountValuation, delta: OrderDelta) -> None:
        f, m = val.funds, val.metrics
        self.val = val
        self.delta = delta
        self.equity = f.equity + delta.equity_delta
        self.margin = f.margin + delta.margin_delta
        self.position_pnl = f.position_pnl + delta.pnl_delta
        self.fee = f.fee + delta.fee
        self.gross = m.gross_notional + abs(delta.signed_notional)
        self.net = m.net_notional + delta.signed_notional
        self.daily = self.equity - f.static_equity
        self.perf = self.equity - f.cum_net_deposit
        self.peak = max(m.intraday_peak_equity, self.perf)
        self.hwm = max(val.hwm_sod, self.peak)
        self._full: AccountValuation | None = None
        self._prod: ExposureView | None = None

    def full(self) -> AccountValuation:
        """The complete what-if view (fallback), built on first use."""
        if self._full is None:
            self._full = self.val.whatif(self.delta)
        return self._full

    def gross_pct(self) -> float:
        """Gross notional / equity after the order (the ``min_gross_pct`` gate)."""
        if self.equity > 0:
            return self.gross / self.equity
        return 0.0 if self.gross == 0 else RATIO_CAP

    def value(self, name: str, key: str | None = None) -> float | None:
        fn = _WHATIF_FAST.get(name)
        if fn is not None:
            return fn(self, key)
        return metric_value(name, self.full(), key)

    def product_view(self) -> ExposureView:
        """The order's product after the order (same expressions as :meth:`AccountValuation.whatif`)."""
        if self._prod is None:
            d = self.delta
            prev = self.val.exposures.get(d.product)
            net = (prev.net_notional if prev else 0.0) + d.signed_notional
            gross_p = (prev.gross_notional if prev else 0.0) + abs(d.signed_notional)
            lp = self.val.limit_pct.get(d.product, d.limit_pct)
            cap = LIMIT_LOSS_BUDGET / lp if lp > 0 else None
            net_pct = _ratio(abs(net), self.equity)
            self._prod = ExposureView(
                product=d.product,
                sector=d.sector,
                exchange=prev.exchange if prev else d.exchange,
                long_lots=(prev.long_lots if prev else 0) + (d.qty if d.signed_notional > 0 else 0),
                short_lots=(prev.short_lots if prev else 0) + (d.qty if d.signed_notional < 0 else 0),
                net_lots=(prev.net_lots if prev else 0) + (d.qty if d.signed_notional > 0 else -d.qty),
                net_notional=net,
                gross_notional=gross_p,
                net_pct_equity=net_pct,
                cap_pct=cap,
                utilization=net_pct / cap if cap else None,
            )
        return self._prod

    def sector_sums(self) -> tuple[float, float]:
        """(net, gross) of the order's sector over the after-order exposures (same order of summation)."""
        d = self.delta
        pv = self.product_view()
        s_net = 0.0
        s_gross = 0.0
        seen = False
        for prod, ev in self.val.exposures.items():
            if prod == d.product:
                seen = True
                ev = pv
            if ev.sector == d.sector:
                s_net += ev.net_notional
                s_gross += ev.gross_notional
        if not seen and pv.sector == d.sector:
            s_net += pv.net_notional
            s_gross += pv.gross_notional
        return s_net, s_gross


def _wf_funds(field_name: str) -> WhatIfFn:
    def fn(w: WhatIf, key: str | None) -> float | None:
        return float(getattr(w.val.funds, field_name))

    return fn


def _wf_metrics(field_name: str) -> WhatIfFn:
    def fn(w: WhatIf, key: str | None) -> float | None:
        return float(getattr(w.val.metrics, field_name))

    return fn


def _wf_product(field_name: str) -> WhatIfFn:
    def fn(w: WhatIf, key: str | None) -> float | None:
        if key is None:
            return None
        if key != w.delta.product:
            if field_name == "gross_pct_equity":
                ev = w.val.exposures.get(key)
                return 0.0 if ev is None else _ratio(ev.gross_notional, w.equity)
            return metric_value(f"product.{field_name}", w.val, key)
        pv = w.product_view()
        if field_name == "gross_pct_equity":
            return _ratio(pv.gross_notional, w.equity)
        val = getattr(pv, field_name)
        if val is None:
            return None
        return abs(float(val)) if field_name in ("net_notional", "net_lots") else float(val)

    return fn


def _wf_sector(field_name: str) -> WhatIfFn:
    def fn(w: WhatIf, key: str | None) -> float | None:
        if key is None:
            return None
        if key != w.delta.sector:
            return metric_value(f"sector.{field_name}", w.val, key)
        s_net, s_gross = w.sector_sums()
        if field_name == "net_pct_equity":
            return _ratio(abs(s_net), w.equity)
        if field_name == "gross_pct_equity":
            return _ratio(s_gross, w.equity)
        if field_name == "net_notional":
            return abs(s_net)
        return s_gross  # gross_notional

    return fn


_WHATIF_FAST: dict[str, WhatIfFn] = {}


def _init_whatif_fast() -> None:
    f = _WHATIF_FAST
    for name in AccountFunds.__struct_fields__:
        f[f"account.{name}"] = _wf_funds(name)  # unchanged by the order unless overridden below
    for name in AccountMetricsView.__struct_fields__:
        f[f"account.{name}"] = _wf_metrics(name)
    f["account.equity"] = lambda w, k: w.equity
    f["account.margin"] = lambda w, k: w.margin
    f["account.position_pnl"] = lambda w, k: w.position_pnl
    f["account.fee"] = lambda w, k: w.fee
    f["account.available"] = lambda w, k: w.equity - w.margin - w.val.funds.frozen_margin - w.val.funds.frozen_fee
    f["account.risk_degree"] = lambda w, k: _ratio(w.margin, w.equity)
    f["account.daily_pnl"] = lambda w, k: w.daily
    f["account.daily_pnl_pct_init"] = lambda w, k: _ratio(w.daily, w.val.funds.initial_capital)
    f["account.daily_pnl_pct_sod"] = lambda w, k: _ratio(w.daily, w.val.funds.static_equity)
    f["account.floating_pnl_pct_init"] = lambda w, k: _ratio(w.position_pnl, w.val.funds.initial_capital)
    f["account.perf_equity"] = lambda w, k: w.perf
    f["account.nav"] = lambda w, k: _ratio(w.perf, w.val.funds.initial_capital)
    f["account.intraday_peak_equity"] = lambda w, k: w.peak
    f["account.intraday_dd"] = lambda w, k: _ratio(w.peak - w.perf, w.peak) if w.peak > 0 else 0.0
    f["account.hwm_equity"] = lambda w, k: w.hwm
    f["account.hwm_dd"] = lambda w, k: _ratio(w.hwm - w.perf, w.hwm) if w.hwm > 0 else 0.0
    f["account.gross_notional"] = lambda w, k: w.gross
    f["account.net_notional"] = lambda w, k: w.net
    f["account.leverage"] = lambda w, k: _ratio(w.gross, w.equity)
    f["account.compliance_util"] = lambda w, k: float(w.val.compliance_util)
    for name in ("account.effective_n", "account.limit_stress"):
        f.pop(name, None)  # need the whole exposure map: full what-if
    for name in ("net_pct_equity", "utilization", "net_notional", "gross_notional", "net_lots", "long_lots",
                 "short_lots", "gross_pct_equity"):
        f[f"product.{name}"] = _wf_product(name)
    for name in ("net_pct_equity", "gross_pct_equity", "net_notional", "gross_notional"):
        f[f"sector.{name}"] = _wf_sector(name)


def replace_struct(obj: Any, **changes: Any) -> Any:
    """``msgspec.structs.replace`` with a plain ``Any`` signature (mypy friendly)."""
    return msgspec.structs.replace(obj, **changes)


def effective_n_of(exposures: Iterable[ExposureView]) -> float | None:
    """有效品种数 = 1/HHI of |net notional| weights (01 §8.14); None when flat."""
    return _effective_n([abs(ev.net_notional) for ev in exposures])


def _effective_n(weights: list[float]) -> float | None:
    total = sum(weights)
    if total <= 0:
        return None
    hhi = sum((w / total) ** 2 for w in weights)
    return 1.0 / hhi if hhi > 0 else None


def sector_exposures(exposures: Mapping[str, ExposureView], equity: float) -> dict[str, SectorExposure]:
    acc: dict[str, list[float]] = {}
    for ev in exposures.values():
        a = acc.setdefault(ev.sector, [0.0, 0.0])
        a[0] += ev.net_notional
        a[1] += ev.gross_notional
    return {
        s: SectorExposure(s, net, gross, _ratio(abs(net), equity), _ratio(gross, equity))
        for s, (net, gross) in sorted(acc.items())
    }


def build_valuation(
    ledger: IntradayLedger,
    registry: InstrumentRegistry,
    *,
    quality: Mapping[str, MdQuality],
    intraday_peak: float,
    orders_today: int = 0,
    cancels_today: int = 0,
    compliance: ComplianceCounters | None = None,
    static: StaticCache | None = None,
    compliance_util: float | None = None,
    quality_tolerance: float = 0.0,
) -> AccountValuation:
    """Value one account at the ledger's current marks (see the module docstring).

    ``quality`` maps instrument -> MdQuality (missing instruments count as
    UNKNOWN: fail-safe). ``intraday_peak`` is the peak performance equity seen
    so far today; the returned view includes the current value in the peak.
    ``static`` is the caller's per-day :class:`StaticCache` (a throw-away one
    is used when missing or of another trading day). ``compliance_util`` is a
    precomputed :func:`compliance_utilization` (the engine caches it per
    account); otherwise it is computed from ``compliance``.

    ``quality_tolerance`` (``system.quality_stale_tolerance``): when the held
    instruments whose quality is not OK carry at most this share of the gross
    notional, the account quality is OK - a small stale position does not
    freeze the account-scope rules, which run on the last known marks. Above
    it (or with 0) the account quality is the worst of the held instruments.
    Products, sectors and instruments always take the worst quality of their
    own instruments (``ExposureView.quality`` per product).

    One pass over the held positions builds the positions, the per-product
    sums and the qualities. Every number is bit-identical to the
    straightforward version: the per-product / per-sector sums accumulate
    left to right in the same order, and the account totals (gross, net,
    limit stress) use :func:`sum` over the same sequences (Python 3.12+ sums
    floats with compensation, so a plain ``+=`` loop would differ in the last
    bits).
    """
    td = ledger.trading_day
    if static is None or static.trading_day != td:
        static = StaticCache(registry, td)
    funds = ledger.funds()
    equity = funds.equity
    held: list[HeldPosition] = []
    account_id = ledger.account_id
    qget = quality.get
    mark_of = ledger.mark
    pnl_of = ledger.position_pnl_of
    long_side = PosDirection.LONG
    unknown = MdQuality.UNKNOWN
    rank = _QUALITY_RANK
    per_prod: dict[str, list[Any]] = {}  # product -> [sector, exchange, long, short, net, gross, limit pct]
    inst_q: dict[str, MdQuality] = {}
    prod_r: dict[str, int] = {}
    sect_r: dict[str, int] = {}
    acct_r = 0
    notionals: list[float] = []  # summed with sum() below (compensated summation, 3.12+)
    signeds: list[float] = []
    nonok: list[float] = []  # notional of the positions whose instrument is not OK (materiality)
    for inst, direction, qty in ledger.held():
        info = static.get(inst)
        mark = mark_of(inst)
        notional = abs(mark * info.multiplier * qty)
        product = info.product
        sector = info.sector
        held.append(HeldPosition(inst, product, sector, info.exchange, direction, qty, mark, info.multiplier, notional,
                                 pnl_of((account_id, inst, direction)), info.limit_pct))
        d = per_prod.get(product)
        if d is None:
            d = per_prod[product] = [sector, info.exchange, 0, 0, 0.0, 0.0, info.limit_pct]
        if direction == long_side:
            d[2] += qty
            signed = notional
        else:
            d[3] += qty
            signed = -notional
        d[4] += signed
        d[5] += notional
        notionals.append(notional)
        signeds.append(signed)
        q = qget(inst, unknown)
        inst_q[inst] = q
        r = rank[q]
        if r:
            nonok.append(notional)
        cur = prod_r.get(product)
        if cur is None or r > cur:
            prod_r[product] = r
        cur = sect_r.get(sector)
        if cur is None or r > cur:
            sect_r[sector] = r
        if r > acct_r:
            acct_r = r
    # ------------------------------------------------------ per product
    exposures: dict[str, ExposureView] = {}
    limit_pct: dict[str, float] = {}
    product_sector: dict[str, str] = {}
    sect_acc: dict[str, list[float]] = {}  # same sums, same order as sector_exposures()
    weights: list[float] = []  # |net| per product, for the effective N
    stress_terms: list[float] = []  # |net| x limit pct per product
    by_rank = _BY_RANK
    for prod in sorted(per_prod):
        sector_p, exch, lng, sht, net_p, gross_p, lp_raw = per_prod[prod]
        lp = float(lp_raw)
        cap = LIMIT_LOSS_BUDGET / lp if lp > 0 else None
        abs_net = abs(net_p)
        net_pct = _ratio(abs_net, equity)
        exposures[prod] = ExposureView(
            product=prod,
            sector=sector_p,
            exchange=exch,
            long_lots=lng,
            short_lots=sht,
            net_lots=lng - sht,
            net_notional=net_p,
            gross_notional=gross_p,
            net_pct_equity=net_pct,
            cap_pct=cap,
            utilization=net_pct / cap if cap else None,
            quality=by_rank[prod_r[prod]],
        )
        limit_pct[prod] = lp
        product_sector[prod] = sector_p
        a = sect_acc.get(sector_p)
        if a is None:
            a = sect_acc[sector_p] = [0.0, 0.0]
        a[0] += net_p
        a[1] += gross_p
        weights.append(abs_net)
        stress_terms.append(abs_net * lp)
    sectors = {
        sec: SectorExposure(sec, s_net, s_gross, _ratio(abs(s_net), equity), _ratio(s_gross, equity))
        for sec, (s_net, s_gross) in sorted(sect_acc.items())
    }
    # ------------------------------------------------------- quality
    prod_q = {k: by_rank[v] for k, v in prod_r.items()}
    sect_q = {k: by_rank[v] for k, v in sect_r.items()}
    gross = sum(notionals)
    if acct_r and quality_tolerance > 0 and gross > 0 and sum(nonok) / gross <= quality_tolerance:
        acct_r = 0  # 重要性阈值：非 OK 合约的敞口占比不超过容忍度时，账户范围按 OK（用最后已知的标记价）
    acct_q = by_rank[acct_r]
    # ------------------------------------------------------- metrics
    daily = equity - funds.static_equity
    perf = equity - funds.cum_net_deposit
    peak = max(intraday_peak, perf)
    hwm_sod = float(ledger.hwm_equity_sod)
    hwm = max(hwm_sod, peak)
    net = sum(signeds)
    stress = sum(stress_terms)
    eff_n = _effective_n(weights)
    metrics = AccountMetricsView(
        daily_pnl=daily,
        daily_pnl_pct_init=_ratio(daily, funds.initial_capital),
        daily_pnl_pct_sod=_ratio(daily, funds.static_equity),
        floating_pnl_pct_init=_ratio(funds.position_pnl, funds.initial_capital),
        perf_equity=perf,
        nav=_ratio(perf, funds.initial_capital),
        intraday_peak_equity=peak,
        intraday_dd=_ratio(peak - perf, peak) if peak > 0 else 0.0,
        hwm_equity=hwm,
        hwm_dd=_ratio(hwm - perf, hwm) if hwm > 0 else 0.0,
        gross_notional=gross,
        net_notional=net,
        leverage=_ratio(gross, equity),
        effective_n=eff_n if eff_n is not None else 0.0,
        limit_stress=_ratio(stress, equity),
        orders_today=orders_today,
        cancels_today=cancels_today,
    )
    return AccountValuation(
        account_id=ledger.account_id,
        funds=funds,
        metrics=metrics,
        exposures=exposures,
        sectors=sectors,
        positions=tuple(held),
        quality=acct_q,
        product_quality=prod_q,
        sector_quality=sect_q,
        instrument_quality=inst_q,
        compliance_util=(compliance_util if compliance_util is not None
                         else compliance_utilization(registry, compliance or ComplianceCounters())),
        hwm_sod=hwm_sod,
        limit_pct=limit_pct,
        product_sector=product_sector,
    )


def compliance_utilization(registry: InstrumentRegistry, c: ComplianceCounters) -> float:
    """``account.compliance_util`` (01 §8.14 合规维度: the rule warns at 80 %, pre-trade rejects at 100 %).

    The largest of: open lots today / daily open limit, cancels / cancel limit,
    large cancels / limit (per instrument), self trades / the exchanges' limit,
    position / position limit (限仓, per instrument, the larger side) and the
    order-rate throttle in use (``order_rate``).
    """
    util = 0.0
    for inst, lots in c.open_volume.items():
        limit = registry.product_spec(inst).open_limit_per_day
        if limit:
            util = max(util, lots / limit)
    for inst, n in c.cancels.items():
        climit = registry.rules(inst).cancel_limit
        if climit:
            util = max(util, n / climit)
    for inst, n in c.large_cancels.items():
        rule = registry.rules(inst).large_cancel
        if rule is not None and rule.count:
            util = max(util, n / rule.count)
    if c.self_trades:
        limits = [registry.products.exchanges[e].self_trade_limit for e in registry.products.exchanges]
        lim = min((x for x in limits if x > 0), default=0)
        if lim:
            util = max(util, c.self_trades / lim)
    for inst, lots in c.positions.items():
        plimit = registry.product_spec(inst).position_limit
        if plimit:
            util = max(util, lots / plimit)
    return max(util, c.order_rate)


# ------------------------------------------------------------------ registry
MetricFn = Callable[[Any, str | None], float | None]


@dataclass(frozen=True, slots=True)
class MetricDef:
    """A named metric: ``scope`` is ``account``, ``product``, ``sector`` or ``firm``.

    ``money_base`` names the AccountFunds field that converts the metric into
    money for ``RuleStatus.headroom_money`` (e.g. ``initial_capital`` for
    ``daily_pnl_pct_init``). ``baseline`` is the neutral point of the metric
    when lower is worse (``RuleStatus.hard_utilization``): 0 by default
    (losses), 1 for NAV, +inf for the effective N (then u = limit / value).
    """

    name: str
    scope: str
    fn: MetricFn
    money_base: str | None = None
    baseline: float = 0.0


_METRICS: dict[str, MetricDef] = {}


def register_metric(
    name: str, *, scope: str = "account", money_base: str | None = None, baseline: float = 0.0
) -> Callable[[MetricFn], MetricFn]:
    """Decorator: ``@register_metric("account.leverage")`` (02 §10 注册表)."""

    def deco(fn: MetricFn) -> MetricFn:
        if name in _METRICS and _METRICS[name].fn is not fn:
            raise ValueError(f"metric {name} registered twice")
        _METRICS[name] = MetricDef(name, scope, fn, money_base, baseline)
        return fn

    return deco


def get_metric(name: str) -> MetricDef:
    try:
        return _METRICS[name]
    except KeyError:
        raise KeyError(f"unknown metric {name!r}; registered: {', '.join(sorted(_METRICS))}") from None


def registered_metrics() -> dict[str, MetricDef]:
    return dict(_METRICS)


def metric_scope_kind(name: str) -> str:
    return get_metric(name).scope


def metric_baseline(name: str) -> float:
    """Baseline of a metric for the hard-limit utilization (0 for an unknown name)."""
    m = _METRICS.get(name)
    return m.baseline if m is not None else 0.0


def _account_field(field_name: str, source: str) -> MetricFn:
    def fn(v: Any, key: str | None) -> float | None:
        obj = v.metrics if source == "metrics" else v.funds
        return float(getattr(obj, field_name))

    fn.__name__ = f"account_{field_name}"
    return fn


_MONEY_BASE = {
    "daily_pnl_pct_init": "initial_capital",
    "floating_pnl_pct_init": "initial_capital",
    "daily_pnl_pct_sod": "static_equity",
    "nav": "initial_capital",
}
# neutral point of lower-is-worse metrics (hard-limit utilization); every other metric: 0
METRIC_BASELINES: dict[str, float] = {"nav": 1.0, "effective_n": math.inf}

for _f in AccountMetricsView.__struct_fields__:
    if _f != "effective_n":
        register_metric(f"account.{_f}", money_base=_MONEY_BASE.get(_f), baseline=METRIC_BASELINES.get(_f, 0.0))(
            _account_field(_f, "metrics"))
for _f in AccountFunds.__struct_fields__:
    register_metric(f"account.{_f}")(_account_field(_f, "funds"))


@register_metric("account.effective_n", baseline=math.inf)
def _effective_n_metric(v: Any, key: str | None) -> float | None:
    """1/HHI; None (not applicable) without positions so that a flat account never trips the rule."""
    return effective_n_of(v.exposures.values())


@register_metric("account.compliance_util")
def _compliance_metric(v: Any, key: str | None) -> float | None:
    return float(v.compliance_util)


def _product_field(field_name: str) -> MetricFn:
    def fn(v: Any, key: str | None) -> float | None:
        if key is None:
            return None
        ev = v.exposures.get(key)
        if ev is None:
            return 0.0  # 该品种已无敞口
        val = getattr(ev, field_name)
        if val is None:
            return None
        return abs(float(val)) if field_name in ("net_notional", "net_lots") else float(val)

    fn.__name__ = f"product_{field_name}"
    return fn


for _f in ("net_pct_equity", "utilization", "net_notional", "gross_notional", "net_lots", "long_lots", "short_lots"):
    register_metric(f"product.{_f}", scope="product")(_product_field(_f))


@register_metric("product.gross_pct_equity", scope="product")
def _product_gross_pct(v: Any, key: str | None) -> float | None:
    if key is None:
        return None
    ev = v.exposures.get(key)
    return 0.0 if ev is None else _ratio(ev.gross_notional, v.funds.equity)


def _sector_field(field_name: str) -> MetricFn:
    def fn(v: Any, key: str | None) -> float | None:
        if key is None:
            return None
        se = v.sectors.get(key)
        if se is None:
            return 0.0
        val = float(getattr(se, field_name))
        return abs(val) if field_name == "net_notional" else val

    fn.__name__ = f"sector_{field_name}"
    return fn


for _f in ("net_pct_equity", "gross_pct_equity", "net_notional", "gross_notional"):
    register_metric(f"sector.{_f}", scope="sector")(_sector_field(_f))


def metric_value(name: str, valuation: Any, key: str | None = None) -> float | None:
    """Evaluate a registered metric on an :class:`AccountValuation` (or a firm view)."""
    return get_metric(name).fn(valuation, key)


_init_whatif_fast()


def money_headroom(name: str, valuation: AccountValuation, value: float, threshold: float) -> float | None:
    """Money left before ``threshold`` for percentage-of-money metrics (RuleStatus.headroom_money)."""
    base = get_metric(name).money_base
    if base is None:
        return None
    return abs(value - threshold) * float(getattr(valuation.funds, base))


__all__ = [
    "LIMIT_LOSS_BUDGET",
    "METRIC_BASELINES",
    "RATIO_CAP",
    "AccountValuation",
    "ComplianceCounters",
    "HeldPosition",
    "MetricDef",
    "OrderDelta",
    "SectorExposure",
    "StaticCache",
    "StaticInfo",
    "WhatIf",
    "build_valuation",
    "compliance_utilization",
    "effective_n_of",
    "get_metric",
    "metric_baseline",
    "metric_scope_kind",
    "metric_value",
    "money_headroom",
    "register_metric",
    "registered_metrics",
    "replace_struct",
    "sector_exposures",
    "worst_quality",
]

