"""Pure analytics behind the daily risk report (01 §5.3.4 table of sections and data sources).

Input is the JSON-able bundle assembled by :mod:`.gather` (settlement result,
trades, SOD snapshots, configuration extracts and the optional data fetched
from other services). :class:`ReportBundle` turns it back into typed payloads;
the functions below compute every table of the report with ``Decimal`` for
money. Missing optional data (a service was down) yields empty tables and the
section shows the recorded error instead.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

import msgspec

from futrisk.common.clock import BJ, ns_to_bj, parse_td
from futrisk.common.decimals import D
from futrisk.core.metrics import metric_baseline
from futrisk.core.rules import limit_utilization
from futrisk.messages.enums import (
    Action,
    CheckStatus,
    MdEventKind,
    Offset,
    OrderStatus,
    ProcessState,
    RiskEventKind,
    SourceState,
)
from futrisk.messages.eod import SettlementResult, SodSnapshot
from futrisk.messages.md import MdEvent
from futrisk.messages.orders import OrderUpdate, Trade
from futrisk.messages.risk import MetricPoint, RiskEvent
from futrisk.messages.system import ServiceStateChanged

ZERO = Decimal(0)
MINUS = "−"  # 报表中的负号用 U+2212（如"−3.02%"）
WEEKDAYS = "一二三四五六日"
SHOCKS: tuple[Decimal, ...] = (Decimal("-0.05"), Decimal("-0.03"), Decimal("0.03"), Decimal("0.05"))


# ------------------------------------------------------------------- formatting
def money(x: Decimal | str | float | None, *, signed: bool = False) -> str:
    """``12,345.67`` (``−`` for negatives, ``+`` when ``signed``)."""
    if x is None:
        return "—"
    d = D(x).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    text = f"{abs(d):,.2f}"
    if d < 0:
        return MINUS + text
    return ("+" + text) if signed and d > 0 else text


def wan(x: Decimal, *, signed: bool = True) -> str:
    """Amount in 万元 with one decimal (summary sentences)."""
    v = (x / Decimal(10_000)).quantize(Decimal("0.1"), rounding=ROUND_HALF_UP)
    text = f"{abs(v):,.1f} 万元"
    if v < 0:
        return MINUS + text
    return ("+" + text) if signed and v > 0 else text


def pct(x: Decimal | float | None, digits: int = 2, *, signed: bool = False) -> str:
    if x is None:
        return "—"
    d = (D(x) * 100).quantize(Decimal(1).scaleb(-digits), rounding=ROUND_HALF_UP)
    text = f"{abs(d):.{digits}f}%"
    if d < 0:
        return MINUS + text
    return ("+" + text) if signed and d > 0 else text


def num(x: Decimal | float | None, digits: int = 2) -> str:
    if x is None:
        return "—"
    d = D(x).quantize(Decimal(1).scaleb(-digits), rounding=ROUND_HALF_UP)
    return (MINUS + f"{abs(d):,.{digits}f}") if d < 0 else f"{d:,.{digits}f}"


def hms(ns: int | None) -> str:
    return ns_to_bj(ns).strftime("%H:%M:%S") if ns else "—"


def ymd_hms(ns: int | None) -> str:
    return ns_to_bj(ns).strftime("%Y-%m-%d %H:%M:%S") if ns else "—"


def td_label(td: str) -> str:
    d = parse_td(td)
    return f"{d:%Y-%m-%d}（周{WEEKDAYS[d.weekday()]}）"


def ratio(a: Decimal, b: Decimal) -> Decimal | None:
    return a / b if b != 0 else None


# ----------------------------------------------------------------------- bundle
def _typed(items: Iterable[Any], cls: type[Any]) -> list[Any]:
    out = []
    for it in items or []:
        try:
            out.append(msgspec.convert(it, cls))
        except (msgspec.ValidationError, TypeError):
            continue
    return out


@dataclass
class ReportBundle:
    """Typed view of the bundle built by :mod:`.gather`."""

    trading_day: str
    version: int
    next_trading_day: str
    generated_ns: int
    config_hash: str
    config_version: int
    triggered_by: str
    settlement: SettlementResult
    trades: list[Trade] = field(default_factory=list)
    sod: SodSnapshot | None = None
    next_sod: SodSnapshot | None = None
    phases: dict[str, int] = field(default_factory=dict)
    night_from: str | None = None
    open_ns: int | None = None
    close_ns: int | None = None
    accounts: dict[str, dict[str, Any]] = field(default_factory=dict)
    instruments: dict[str, dict[str, Any]] = field(default_factory=dict)
    rules: list[dict[str, Any]] = field(default_factory=list)
    history: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    max_dd: dict[str, str] = field(default_factory=dict)  # account -> max daily drawdown since the first day
    alerts: list[RiskEvent] = field(default_factory=list)
    alerts_error: str | None = None
    orders: list[OrderUpdate] = field(default_factory=list)
    orders_error: str | None = None
    metrics: dict[str, list[MetricPoint]] = field(default_factory=dict)
    metrics_error: str | None = None
    md_events: list[MdEvent] = field(default_factory=list)
    md_error: str | None = None
    svc_events: list[ServiceStateChanged] = field(default_factory=list)
    svc_error: str | None = None

    @classmethod
    def from_dict(cls, b: Mapping[str, Any]) -> ReportBundle:
        def section(key: str) -> tuple[list[Any], str | None]:
            v = b.get(key) or {}
            return list(v.get("items") or []), v.get("error")

        alerts, alerts_err = section("alerts")
        orders, orders_err = section("orders")
        md, md_err = section("md_events")
        svc, svc_err = section("svc_history")
        met = b.get("metrics") or {}
        series = {a: _typed(pts, MetricPoint) for a, pts in (met.get("series") or {}).items()}
        return cls(
            trading_day=str(b["trading_day"]),
            version=int(b["version"]),
            next_trading_day=str(b.get("next_trading_day") or ""),
            generated_ns=int(b.get("generated_ns") or 0),
            config_hash=str(b.get("config_hash") or ""),
            config_version=int(b.get("config_version") or 0),
            triggered_by=str(b.get("triggered_by") or ""),
            settlement=msgspec.convert(b["settlement"], SettlementResult),
            trades=_typed(b.get("trades") or [], Trade),
            sod=msgspec.convert(b["sod"], SodSnapshot) if b.get("sod") else None,
            next_sod=msgspec.convert(b["next_sod"], SodSnapshot) if b.get("next_sod") else None,
            phases=dict(b.get("phases") or {}),
            night_from=b.get("night_from"),
            open_ns=b.get("open_ns"),
            close_ns=b.get("close_ns"),
            accounts=dict(b.get("accounts") or {}),
            instruments=dict(b.get("instruments") or {}),
            rules=list(b.get("rules") or []),
            history=dict(b.get("history") or {}),
            max_dd={str(k): str(v) for k, v in (b.get("max_dd") or {}).items()},
            alerts=_typed(alerts, RiskEvent),
            alerts_error=alerts_err,
            orders=_typed(orders, OrderUpdate),
            orders_error=orders_err,
            metrics=series,
            metrics_error=met.get("error"),
            md_events=_typed(md, MdEvent),
            md_error=md_err,
            svc_events=_typed(svc, ServiceStateChanged),
            svc_error=svc_err,
        )

    # ------------------------------------------------------------- lookups
    def account_ids(self) -> list[str]:
        return [a.account_id for a in self.settlement.accounts]

    def account_name(self, acct: str) -> str:
        return str((self.accounts.get(acct) or {}).get("name") or "")

    def product_of(self, inst: str) -> str:
        info = self.instruments.get(inst) or {}
        return str(info.get("product") or "".join(ch for ch in inst if ch.isalpha()))

    def sector_of(self, inst: str) -> str:
        return str((self.instruments.get(inst) or {}).get("sector") or "other")

    def product_name(self, product: str) -> str:
        for info in self.instruments.values():
            if info.get("product") == product and info.get("name"):
                return str(info["name"])
        return product

    def limit_pct(self, inst: str) -> Decimal:
        return D((self.instruments.get(inst) or {}).get("limit_pct") or "0.05")

    def settle_price(self, inst: str) -> Decimal | None:
        for p in self.settlement.prices:
            if p.instrument_id == inst:
                return D(p.price)
        return None


# ------------------------------------------------------------------ funds table
@dataclass
class FundsRow:
    account_id: str
    name: str
    pre_balance: Decimal
    net_deposit: Decimal
    close_pnl: Decimal
    position_pnl: Decimal
    fee: Decimal
    equity: Decimal
    margin: Decimal
    available: Decimal
    risk_degree: Decimal | None
    nav: Decimal | None
    max_drawdown: Decimal | None  # 03 column name; the CURRENT drawdown from the high-water mark
    initial_capital: Decimal
    day_pnl: Decimal
    margin_call: Decimal
    peak_drawdown: Decimal | None = None  # max of the daily drawdowns since the first settled day

    @property
    def day_pnl_pct_init(self) -> Decimal | None:
        return ratio(self.day_pnl, self.initial_capital)


def _peak_drawdown(b: ReportBundle, acct: str, today: Decimal) -> Decimal | None:
    """Max daily drawdown since the first settled day (``max_dd`` of the bundle; today's value included)."""
    stored = b.max_dd.get(acct)
    if stored is None:
        return None
    return max(D(stored), today)


def funds_rows(b: ReportBundle) -> tuple[list[FundsRow], FundsRow]:
    """CTP-style funds per account plus the firm total (risk degree = Σ margin / Σ equity)."""
    rows: list[FundsRow] = []
    for a in b.settlement.accounts:
        net = D(a.deposit) - D(a.withdraw)
        eq = D(a.equity)
        rows.append(
            FundsRow(
                account_id=a.account_id,
                name=b.account_name(a.account_id),
                pre_balance=D(a.pre_balance),
                net_deposit=net,
                close_pnl=D(a.close_pnl),
                position_pnl=D(a.position_pnl),
                fee=D(a.fee),
                equity=eq,
                margin=D(a.margin),
                available=D(a.available),
                risk_degree=D(a.risk_degree),
                nav=D(a.nav),
                max_drawdown=D(a.max_drawdown),
                initial_capital=D(a.initial_capital),
                day_pnl=eq - D(a.pre_balance) - net,
                margin_call=D(a.margin_call),
                peak_drawdown=_peak_drawdown(b, a.account_id, D(a.max_drawdown)),
            )
        )

    def s(attr: str) -> Decimal:
        return sum((getattr(r, attr) for r in rows), ZERO)

    total = FundsRow(
        account_id="合计",
        name="公司",
        pre_balance=s("pre_balance"),
        net_deposit=s("net_deposit"),
        close_pnl=s("close_pnl"),
        position_pnl=s("position_pnl"),
        fee=s("fee"),
        equity=s("equity"),
        margin=s("margin"),
        available=s("available"),
        risk_degree=ratio(s("margin"), s("equity")),
        nav=None,
        max_drawdown=None,
        initial_capital=s("initial_capital"),
        day_pnl=s("day_pnl"),
        margin_call=s("margin_call"),
    )
    return rows, total


# ---------------------------------------------------------------- P&L, exposure
def position_net_pnl(p: Any) -> Decimal:
    return D(p.close_pnl) + D(p.position_pnl) - D(p.fee)


def pnl_by_product(b: ReportBundle) -> tuple[list[str], dict[str, dict[str, Decimal]], dict[str, Decimal]]:
    """Net P&L (close + position − fee) per account × product, plus product totals."""
    grid: dict[str, dict[str, Decimal]] = defaultdict(lambda: defaultdict(Decimal))
    for p in b.settlement.positions:
        grid[p.account_id][p.product] += position_net_pnl(p)
    products = sorted({p.product for p in b.settlement.positions})
    totals = {prod: sum((grid[a].get(prod, ZERO) for a in grid), ZERO) for prod in products}
    return products, {a: dict(v) for a, v in grid.items()}, totals


def pnl_by_sector(b: ReportBundle) -> tuple[list[str], dict[str, dict[str, Decimal]], dict[str, Decimal]]:
    grid: dict[str, dict[str, Decimal]] = defaultdict(lambda: defaultdict(Decimal))
    for p in b.settlement.positions:
        grid[p.account_id][b.sector_of(p.instrument_id)] += position_net_pnl(p)
    sectors = sorted({b.sector_of(p.instrument_id) for p in b.settlement.positions})
    totals = {s: sum((grid[a].get(s, ZERO) for a in grid), ZERO) for s in sectors}
    return sectors, {a: dict(v) for a, v in grid.items()}, totals


def signed_notional(b: ReportBundle, p: Any) -> Decimal:
    """Settlement-price notional of the EOD position, positive long, negative short."""
    value: Decimal = D(p.settle_price) * p.eod_qty * p.multiplier
    return value if str(p.direction) == "LONG" else -value


@dataclass
class Exposure:
    account_id: str
    equity: Decimal
    gross: Decimal
    net: Decimal
    leverage: Decimal | None
    product_net: dict[str, Decimal]
    sector_net: dict[str, Decimal]
    sector_gross: dict[str, Decimal]
    top5: list[tuple[str, Decimal, Decimal | None]]  # product, net notional, share of equity
    effective_n: Decimal | None
    limit_stress: Decimal | None
    max_product_util: tuple[str, Decimal] | None
    max_sector_pct: tuple[str, Decimal] | None
    top_sector_share: tuple[str, Decimal] | None


def exposures(b: ReportBundle) -> list[Exposure]:
    """Gross/net notional, leverage, top-5 products, sector concentration, effective N, limit stress."""
    equity = {a.account_id: D(a.equity) for a in b.settlement.accounts}
    per_prod: dict[str, dict[str, Decimal]] = defaultdict(lambda: defaultdict(Decimal))
    prod_limit: dict[str, Decimal] = {}
    per_sector: dict[str, dict[str, Decimal]] = defaultdict(lambda: defaultdict(Decimal))
    per_sector_gross: dict[str, dict[str, Decimal]] = defaultdict(lambda: defaultdict(Decimal))
    gross: dict[str, Decimal] = defaultdict(Decimal)
    for p in b.settlement.positions:
        if p.eod_qty == 0:
            continue
        v = signed_notional(b, p)
        per_prod[p.account_id][p.product] += v
        prod_limit[p.product] = b.limit_pct(p.instrument_id)
        sector = b.sector_of(p.instrument_id)
        per_sector[p.account_id][sector] += v
        per_sector_gross[p.account_id][sector] += abs(v)
        gross[p.account_id] += abs(v)
    out: list[Exposure] = []
    for acct in b.account_ids():
        eq = equity.get(acct, ZERO)
        prods = dict(per_prod.get(acct, {}))
        net = sum(prods.values(), ZERO)
        g = gross.get(acct, ZERO)
        abs_total = sum((abs(v) for v in prods.values()), ZERO)
        eff_n = None
        if abs_total > 0:
            hhi = sum(((abs(v) / abs_total) ** 2 for v in prods.values()), ZERO)
            eff_n = (Decimal(1) / hhi) if hhi > 0 else None
        stress = ratio(sum((abs(v) * prod_limit.get(k, Decimal("0.05")) for k, v in prods.items()), ZERO), eq)
        utils = []
        for prod, v in prods.items():
            lim = prod_limit.get(prod, Decimal("0.05"))
            cap = Decimal("0.03") / lim if lim > 0 else None
            share = ratio(abs(v), eq)
            if cap and share is not None:
                utils.append((prod, share / cap))
        sectors = dict(per_sector.get(acct, {}))
        sector_pcts = [(s, abs(v) / eq) for s, v in sectors.items() if eq > 0]
        sec_gross = dict(per_sector_gross.get(acct, {}))
        shares = [(s, v / g) for s, v in sec_gross.items() if g > 0]
        top5 = sorted(prods.items(), key=lambda kv: -abs(kv[1]))[:5]
        out.append(
            Exposure(
                account_id=acct,
                equity=eq,
                gross=g,
                net=net,
                leverage=ratio(g, eq),
                product_net=prods,
                sector_net=sectors,
                sector_gross=sec_gross,
                top5=[(k, v, ratio(v, eq)) for k, v in top5],
                effective_n=eff_n,
                limit_stress=stress,
                max_product_util=max(utils, key=lambda x: x[1]) if utils else None,
                max_sector_pct=max(sector_pcts, key=lambda x: x[1]) if sector_pcts else None,
                top_sector_share=max(shares, key=lambda x: x[1]) if shares else None,
            )
        )
    return out


def exposure_grid(b: ReportBundle, exps: Sequence[Exposure]) -> tuple[list[str], list[dict[str, Any]]]:
    """Product × account signed net notional with a firm net column (01 §5.3.4 跨账户网格)."""
    products = sorted({p for e in exps for p in e.product_net})
    rows = []
    for prod in products:
        cells = {e.account_id: e.product_net.get(prod) for e in exps}
        firm = sum((v for v in cells.values() if v is not None), ZERO)
        rows.append({"product": prod, "name": b.product_name(prod), "cells": cells, "firm": firm})
    return products, rows


# ------------------------------------------------------------------------ limits
EOD_METRICS = {
    "account.daily_pnl_pct_init",
    "account.hwm_dd",
    "account.nav",
    "account.risk_degree",
    "account.leverage",
    "account.effective_n",
    "account.limit_stress",
    "product.utilization",
    "sector.net_pct_equity",
}


def utilization(value: Decimal | None, threshold: float, op: str, metric: str = "") -> Decimal | None:
    """Utilization against a limit, the core's definition (02 §7.6 ``RuleStatus.hard_utilization``):
    ``core.rules.limit_utilization`` with the metric's baseline (0 for losses, 1 for NAV), the same
    number the dashboard shows."""
    if value is None:
        return None
    u = limit_utilization(float(value), float(threshold), op, metric_baseline(metric) if metric else 0.0)
    return None if u is None else D(u)


def hard_level_of(rule: Mapping[str, Any], account_id: str) -> dict[str, Any] | None:
    """The rule's hard limit for the account (``core.rules.CompiledRule.hard_level``): the first
    CIRCUIT_BREAK level, else the last RESTRICT level, else the last level; overrides applied."""
    overrides = (rule.get("overrides") or {}).get(account_id, {})
    levels = [{**lv, **overrides.get(lv.get("name"), {})} for lv in (rule.get("levels") or [])]
    for lv in levels:
        if lv.get("action") == "CIRCUIT_BREAK":
            return lv
    for lv in reversed(levels):
        if lv.get("action") == "RESTRICT":
            return lv
    return levels[-1] if levels else None


@dataclass
class LimitRow:
    account_id: str
    rule_id: str
    metric: str
    op: str
    eod_value: Decimal | None
    threshold: float
    level: str
    utilization: Decimal | None
    peak_value: Decimal | None
    peak_util: Decimal | None
    breaches: int
    fmt: str  # "pct" | "num"


def _eod_metric(metric: str, f: FundsRow, e: Exposure | None) -> Decimal | None:
    if metric == "account.daily_pnl_pct_init":
        return f.day_pnl_pct_init
    if metric == "account.hwm_dd":
        return f.max_drawdown
    if metric == "account.nav":
        return f.nav
    if metric == "account.risk_degree":
        return f.risk_degree
    if e is None:
        return None
    if metric == "account.leverage":
        return e.leverage if e.gross > 0 else ZERO
    if metric == "account.effective_n":
        return e.effective_n
    if metric == "account.limit_stress":
        return e.limit_stress if e.gross > 0 else ZERO
    if metric == "product.utilization":
        return e.max_product_util[1] if e.max_product_util else ZERO
    if metric == "sector.net_pct_equity":
        return e.max_sector_pct[1] if e.max_sector_pct else ZERO
    return None


def _peak(metric: str, points: Sequence[MetricPoint]) -> Decimal | None:
    """Intraday extreme of the metrics that ``MetricSeries`` carries (worst value of the day)."""
    if not points:
        return None
    if metric == "account.daily_pnl_pct_init":
        return min(D(p.daily_pnl_pct_init) for p in points)
    if metric == "account.intraday_dd":
        return max(D(p.intraday_dd) for p in points)
    if metric == "account.risk_degree":
        return max(D(p.risk_degree) for p in points)
    if metric == "account.nav":
        return min(D(p.nav) for p in points)
    if metric == "account.leverage":
        vals = [D(p.gross_notional) / D(p.equity) for p in points if p.equity]
        return max(vals) if vals else None
    return None


def _peak_util(rule_id: str, acct: str, points: Sequence[MetricPoint]) -> Decimal | None:
    """Max ``rule_util`` of the rule over the day (keys ``{rule_id}/{scope_key}``, 03 §4.3)."""
    best: Decimal | None = None
    for p in points:
        util = getattr(p, "rule_util", None) or {}
        for key, v in util.items():
            if key.split("/", 1)[0] != rule_id or v is None:
                continue
            if f"acct:{acct}" not in key and "acct:" in key:
                continue
            d = D(v)
            best = d if best is None or d > best else best
    return best


def limit_rows(b: ReportBundle, funds: Sequence[FundsRow], exps: Sequence[Exposure]) -> list[LimitRow]:
    """EOD value, intraday peak and breach count per account × rule (01 §5.3.4 限额使用)."""
    breaches = Counter(
        (e.account_id, e.rule_id) for e in b.alerts if e.kind == RiskEventKind.ALERT_RAISED and e.rule_id
    )
    exp_by = {e.account_id: e for e in exps}
    rows: list[LimitRow] = []
    for f in funds:
        points = b.metrics.get(f.account_id, [])
        for rule in b.rules:
            if not rule.get("enabled", True):
                continue
            metric = str(rule.get("metric", ""))
            levels = rule.get("levels") or []
            if not levels:
                continue
            op = str(rule.get("op", ">="))
            hard = hard_level_of(rule, f.account_id) or levels[-1]
            threshold = float(hard.get("threshold", 0))
            eod = _eod_metric(metric, f, exp_by.get(f.account_id))
            if metric == "account.intraday_dd" and points:
                eod = D(points[-1].intraday_dd)
            peak = _peak(metric, points)
            breach_n = breaches.get((f.account_id, str(rule.get("id"))), 0)
            peak_u = _peak_util(str(rule.get("id")), f.account_id, points)
            if eod is None and peak is None and peak_u is None and breach_n == 0:
                continue  # 指标无从评估（如合规计数不在结算数据中）：不列出
            rows.append(
                LimitRow(
                    account_id=f.account_id,
                    rule_id=str(rule.get("id")),
                    metric=metric,
                    op=op,
                    eod_value=eod,
                    threshold=threshold,
                    level=str(hard.get("name", "")),
                    utilization=utilization(eod, threshold, op, metric),
                    peak_value=peak,
                    peak_util=peak_u if peak_u is not None else utilization(peak, threshold, op, metric),
                    breaches=breach_n,
                    fmt="num" if metric in ("account.effective_n", "account.nav", "account.leverage") else "pct",
                )
            )
    return rows


# ------------------------------------------------------------------------ events
def rejected_orders(b: ReportBundle) -> list[OrderUpdate]:
    rej = {OrderStatus.REJECTED, OrderStatus.BROKER_REJECTED}
    return sorted((o for o in b.orders if o.status in rej), key=lambda o: (o.created_ns, o.client_order_id))


def events_sorted(b: ReportBundle) -> list[RiskEvent]:
    return sorted(b.alerts, key=lambda e: (e.ts_ns, e.event_id))


def key_sentences(b: ReportBundle, funds: Sequence[FundsRow], exps: Sequence[Exposure]) -> list[str]:
    """Auto-generated summary sentences, e.g. "ACC3 10:32:15 于 −3.02% 熔断，拒绝 7 笔开仓"."""
    out: list[str] = []
    rejected = rejected_orders(b)
    for e in events_sorted(b):
        if e.kind == RiskEventKind.ALERT_RAISED and e.action == Action.CIRCUIT_BREAK and e.account_id:
            n = sum(
                1
                for o in rejected
                if o.account_id == e.account_id and o.offset == Offset.OPEN and o.created_ns >= e.ts_ns
            )
            value = pct(e.value) if e.value is not None else "阈值"
            out.append(f"{e.account_id} {hms(e.ts_ns)} 于 {value} 熔断，拒绝 {n} 笔开仓")
        elif e.kind == RiskEventKind.MANUAL_HALT and e.account_id:
            out.append(f"{e.account_id} {hms(e.ts_ns)} 被人工熔断（{e.actor or '—'}）")
        elif e.kind == RiskEventKind.KILL_SWITCH_ON:
            out.append(f"{hms(e.ts_ns)} 全局 kill switch 开启：{e.message}")
    if funds:
        best = max(funds, key=lambda f: f.day_pnl)
        worst = min(funds, key=lambda f: f.day_pnl)
        out.append(
            f"当日盈亏最高 {best.account_id} {wan(best.day_pnl)}（{pct(best.day_pnl_pct_init, signed=True)}），"
            f"最低 {worst.account_id} {wan(worst.day_pnl)}（{pct(worst.day_pnl_pct_init, signed=True)}）"
        )
    risk_rule = next((r for r in b.rules if r.get("metric") == "account.risk_degree"), None)
    if risk_rule and risk_rule.get("levels"):
        warn = D(risk_rule["levels"][0]["threshold"])
        for f in funds:
            if f.risk_degree is not None and f.risk_degree >= warn:
                out.append(f"{f.account_id} 风险度 {pct(f.risk_degree, 1)}，超过预警线 {pct(warn, 0)}")
    for f in funds:
        if f.margin_call > 0:
            out.append(f"{f.account_id} 需追加保证金 {money(f.margin_call)} 元")
    fallback = [p for p in b.settlement.prices if p.source.value not in ("VWAP", "OFFICIAL")]
    if fallback:
        parts = ", ".join(f"{p.instrument_id}({p.source.value})" for p in fallback[:6])
        out.append(f"{len(fallback)} 个合约未按 VWAP/官方价结算：{parts}")
    warns = [c for c in b.settlement.checks if c.status == CheckStatus.WARN]
    if warns:
        out.append(f"对账 WARN {len(warns)} 项：" + ", ".join(sorted({c.check.value for c in warns})))
    dq = data_quality(b)
    outages = sum(r["outages"] for r in dq["sources"])
    filled = sum(r["filled"] for r in dq["sources"])
    if outages or filled:
        out.append(f"行情源中断 {outages} 次，补数 {filled} 条，坏点 {sum(r['bad_ticks'] for r in dq['sources'])} 个")
    restarts = sum(r["restarts"] for r in system_rows(b))
    if restarts:
        out.append(f"服务重启 {restarts} 次")
    if len(out) == (1 if funds else 0):
        out.append("全部账户运行平稳，当日无熔断与告警事件")
    return out


# ------------------------------------------------------------------ data quality
def _window(b: ReportBundle) -> tuple[int | None, int | None]:
    return b.open_ns, b.close_ns


def data_quality(b: ReportBundle) -> dict[str, Any]:
    """Per source: availability (time not EXCLUDED in the day window), outages, filled, bad ticks."""
    open_ns, close_ns = _window(b)
    by_source: dict[str, list[MdEvent]] = defaultdict(list)
    kinds = Counter(e.kind.value for e in b.md_events)
    for e in b.md_events:
        src = e.source.value if e.source is not None else (e.gap.source.value if e.gap and e.gap.source else "ALL")
        by_source[src].append(e)
    rows = []
    for src in sorted(by_source):
        evs = sorted(by_source[src], key=lambda e: e.ts_ns)
        outages = sum(
            1
            for e in evs
            if (e.kind == MdEventKind.SOURCE_STATE and e.to_state == SourceState.EXCLUDED.value)
            or e.kind == MdEventKind.MD_DOWN
        )
        filled = sum(e.gap.filled for e in evs if e.kind == MdEventKind.GAP_FILLED and e.gap is not None)
        unrecoverable = sum(
            e.gap.unrecoverable for e in evs if e.kind == MdEventKind.GAP_UNRECOVERABLE and e.gap is not None
        )
        bad = sum(
            1
            for e in evs
            if e.kind in (MdEventKind.BAD_TICK, MdEventKind.QUARANTINE_DROPPED, MdEventKind.QUARANTINE_CONFIRMED)
        )
        avail = None
        if open_ns and close_ns and close_ns > open_ns:
            down = ZERO
            since: int | None = None
            for e in evs:
                if e.kind != MdEventKind.SOURCE_STATE:
                    continue
                t = min(max(e.ts_ns, open_ns), close_ns)
                if e.to_state == SourceState.EXCLUDED.value and since is None:
                    since = t
                elif e.to_state != SourceState.EXCLUDED.value and since is not None:
                    down += Decimal(t - since)
                    since = None
            if since is not None:
                down += Decimal(close_ns - since)
            avail = Decimal(1) - down / Decimal(close_ns - open_ns)
        rows.append(
            {
                "source": src,
                "availability": avail,
                "outages": outages,
                "filled": filled,
                "unrecoverable": unrecoverable,
                "bad_ticks": bad,
                "events": len(evs),
            }
        )
    return {"sources": rows, "kinds": dict(sorted(kinds.items())), "total": len(b.md_events)}


# ------------------------------------------------------------------------ system
_DOWN = {ProcessState.EXITED, ProcessState.RESTARTING, ProcessState.FAILED, ProcessState.STOPPED}


def system_rows(b: ReportBundle) -> list[dict[str, Any]]:
    """Per instance: restarts, exits, availability within the day window (01 §5.3.4 系统运行)."""
    open_ns, close_ns = _window(b)
    by_inst: dict[str, list[ServiceStateChanged]] = defaultdict(list)
    for e in b.svc_events:
        by_inst[e.instance].append(e)
    rows = []
    for inst in sorted(by_inst):
        evs = sorted(by_inst[inst], key=lambda e: e.ts_ns)
        restarts = max((e.restarts for e in evs), default=0)
        restarts = max(restarts, sum(1 for e in evs if e.to_state == ProcessState.RESTARTING))
        exits = sum(1 for e in evs if e.to_state in (ProcessState.EXITED, ProcessState.FAILED))
        avail = None
        if open_ns and close_ns and close_ns > open_ns:
            down = ZERO
            since: int | None = None
            for e in evs:
                t = min(max(e.ts_ns, open_ns), close_ns)
                if e.to_state in _DOWN and since is None:
                    since = t
                elif e.to_state not in _DOWN and since is not None:
                    down += Decimal(t - since)
                    since = None
            if since is not None:
                down += Decimal(close_ns - since)
            avail = Decimal(1) - down / Decimal(close_ns - open_ns)
        rows.append({"instance": inst, "restarts": restarts, "exits": exits, "availability": avail, "events": len(evs)})
    return rows


# ------------------------------------------------------------------ stress test
def stress_rows(b: ReportBundle) -> list[dict[str, Any]]:
    """EOD positions under ±3% / ±5% sector shocks (01 §5.3.4 压力测试, SHOULD)."""
    net: dict[str, dict[str, Decimal]] = defaultdict(lambda: defaultdict(Decimal))  # sector -> account -> notional
    for p in b.settlement.positions:
        if p.eod_qty:
            net[b.sector_of(p.instrument_id)][p.account_id] += signed_notional(b, p)
    firm_equity = sum((D(a.equity) for a in b.settlement.accounts), ZERO)
    accounts = b.account_ids()
    rows = []
    scenarios = [(s, {s}) for s in sorted(net)] + ([("全部板块", set(net))] if len(net) > 1 else [])
    for label, sectors in scenarios:
        for shock in SHOCKS:
            per = {a: sum((net[s].get(a, ZERO) for s in sectors), ZERO) * shock for a in accounts}
            firm = sum(per.values(), ZERO)
            rows.append(
                {"sector": label, "shock": shock, "accounts": per, "firm": firm, "pct_equity": ratio(firm, firm_equity)}
            )
    return rows


# ------------------------------------------------------------------------ header
def price_rows(b: ReportBundle) -> list[dict[str, Any]]:
    rows = []
    for p in sorted(b.settlement.prices, key=lambda p: p.instrument_id):
        info = b.instruments.get(p.instrument_id) or {}
        rows.append(
            {
                "instrument_id": p.instrument_id,
                "exchange": info.get("exchange", ""),
                "price": p.price,
                "source": p.source.value,
                "method": p.method.value if p.method else "—",
                "vwap_raw": p.vwap_raw or "—",
                "quality": p.input_quality.value if p.input_quality else "—",
                "note": f"{p.operator}: {p.reason}" if p.operator else "",
            }
        )
    return rows


def price_source_counts(b: ReportBundle) -> dict[str, int]:
    return dict(sorted(Counter(p.source.value for p in b.settlement.prices).items()))


def night_label(b: ReportBundle) -> str:
    if b.night_from:
        return f"含上一晚夜盘（{b.night_from} 起）"
    return "无夜盘（节后首日，集合竞价 08:55 开始）"


def generated_label(b: ReportBundle) -> str:
    return datetime.fromtimestamp(b.generated_ns / 1e9, tz=BJ).strftime("%Y-%m-%d %H:%M:%S") if b.generated_ns else "—"


__all__ = [
    "MINUS",
    "SHOCKS",
    "Exposure",
    "FundsRow",
    "LimitRow",
    "ReportBundle",
    "data_quality",
    "events_sorted",
    "exposure_grid",
    "exposures",
    "funds_rows",
    "generated_label",
    "hms",
    "key_sentences",
    "limit_rows",
    "money",
    "night_label",
    "num",
    "pct",
    "pnl_by_product",
    "pnl_by_sector",
    "position_net_pnl",
    "price_rows",
    "price_source_counts",
    "rejected_orders",
    "signed_notional",
    "stress_rows",
    "system_rows",
    "td_label",
    "utilization",
    "wan",
    "ymd_hms",
]
