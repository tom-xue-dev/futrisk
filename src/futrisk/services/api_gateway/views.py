"""Pure view builders on top of the read model (02 §9.6 ``/risk/compare``, ``/positions``; §9.7 ``compare``).

Multi-account comparison (01 §5.3.5, task additional item 4):

* **matrix**: account x rule cells ``{value, limit, utilization, band, active_level}``
  where ``limit`` is the rule's *hard limit* (first ``CIRCUIT_BREAK`` level,
  else the last ``RESTRICT`` level, else the last level) with per-account
  overrides applied, and ``utilization`` is normalised so that bigger is more
  dangerous (``op >=``: ``value / limit``; ``op <=``: ``(b - value) / (b - limit)``
  with baseline ``b`` = 0, 1 for NAV, and ``limit / value`` for the effective
  product count). Bands: <60 % ok, 60-80 % watch, 80-100 % warn, >=100 % breach;
* **ranking**: per account x rule the closeness to the *next* level (who trips first);
* **exposure**: product x account signed net notional grid with a firm-net column.

The same shape is produced by the dashboard's client-side builder
(``web/src/utils/compare.ts``), so either side can compute it.

``/positions`` aggregation: filters by account / product / instrument /
exchange / sector and optional ``group_by`` of the same five dimensions.
"""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import Any

from futrisk.common.config.products import ProductsConfig, product_code
from futrisk.common.config.risk_rules import RuleDef
from futrisk.messages.base import to_builtins
from futrisk.messages.enums import PosDirection
from futrisk.messages.risk import AccountSnapshot, RuleStatus

METRIC_BASELINE: Mapping[str, float] = {"account.nav": 1.0, "account.effective_n": math.inf}
BAND_CUTS = (("breach", 1.0), ("warn", 0.8), ("watch", 0.6))
GROUP_BY = ("account", "product", "instrument", "exchange", "sector")


def _is_lower_worse(op: str) -> bool:
    return op in ("<=", "<")


def utilization(value: float | None, limit: float | None, op: str, metric: str | None = None) -> float | None:
    """Normalised utilization of ``value`` against ``limit`` (>= 0; None when undefined)."""
    if value is None or limit is None or not math.isfinite(value) or not math.isfinite(limit):
        return None
    if not _is_lower_worse(op):
        if limit == 0:
            return None
        u = value / limit
    else:
        base = METRIC_BASELINE.get(metric or "", 0.0)
        if not math.isfinite(base):
            if value <= 0:
                return None
            u = limit / value
        else:
            denom = base - limit
            if denom == 0:
                return None
            u = (base - value) / denom
    if not math.isfinite(u):
        return None
    return max(0.0, u)


def effective_levels(rule: RuleDef, account_id: str | None) -> list[dict[str, Any]]:
    """Levels of ``rule`` with the account's overrides applied (``overrides: {ACC2: {halt: {threshold}}}``)."""
    ov = rule.overrides.get(account_id, {}) if account_id else {}
    out = []
    for lv in rule.levels:
        d = {"name": lv.name, "threshold": float(lv.threshold), "action": str(lv.action)}
        patch = ov.get(lv.name)
        if patch:
            for k, v in patch.items():
                if k != "name":
                    d[k] = float(v) if k == "threshold" else v
        out.append(d)
    return out


def hard_limit_level(levels: Sequence[Mapping[str, Any]]) -> Mapping[str, Any] | None:
    """First CIRCUIT_BREAK level, else the last RESTRICT level, else the last level."""
    if not levels:
        return None
    for lv in levels:
        if lv["action"] == "CIRCUIT_BREAK":
            return lv
    restricts = [lv for lv in levels if lv["action"] == "RESTRICT"]
    return restricts[-1] if restricts else levels[-1]


def _marks(rule: RuleDef, account_id: str | None) -> list[tuple[float, Mapping[str, Any]]]:
    levels = effective_levels(rule, account_id)
    hard = hard_limit_level(levels)
    if hard is None:
        return []
    marks: list[tuple[float, Mapping[str, Any]]] = []
    for lv in levels:
        pos = utilization(lv["threshold"], hard["threshold"], rule.op, rule.metric)
        if pos is not None:
            marks.append((pos, lv))
    marks.sort(key=lambda m: m[0])
    return marks


def next_level(value: float | None, rule: RuleDef, account_id: str | None) -> Mapping[str, Any] | None:
    """The next level ``value`` has not crossed yet (None when all are crossed)."""
    marks = _marks(rule, account_id)
    if not marks:
        return None
    hard = hard_limit_level(effective_levels(rule, account_id))
    u = utilization(value, hard["threshold"] if hard else None, rule.op, rule.metric)
    if u is None:
        return marks[0][1]
    for pos, lv in marks:
        if pos > u + 1e-12:
            return lv
    return None


def band(u: float | None) -> str:
    if u is None:
        return "na"
    for name, cut in BAND_CUTS:
        if u >= cut:
            return name
    return "ok"


def _headroom(value: float | None, threshold: float | None) -> float | None:
    if value is None or threshold is None:
        return None
    return abs(threshold - value)


def _cell_util(rs: RuleStatus, rule: RuleDef | None, account_id: str) -> tuple[float | None, float | None]:
    """(utilization of the hard limit, hard-limit threshold) of one rule status.

    The core's ``hard_utilization`` / ``hard_level`` win when the shard sends them (the
    same number the account list shows as ``worst_utilization``); older shards: computed
    here from the rule's effective levels, else the next-level utilization.
    """
    core_u = getattr(rs, "hard_utilization", None)
    if core_u is not None:
        level = getattr(rs, "hard_level", None)
        limit: float | None = None
        if rule is not None and level is not None:
            limit = next((lv["threshold"] for lv in effective_levels(rule, account_id) if lv["name"] == level), None)
        return core_u, limit
    if rule is not None:
        hard = hard_limit_level(effective_levels(rule, account_id))
        if hard is not None:
            u = utilization(rs.value, hard["threshold"], rule.op, rule.metric or rs.metric)
            return (u if u is not None else rs.utilization), hard["threshold"]
    return rs.utilization, rs.next_threshold


def build_matrix(snapshots: Sequence[AccountSnapshot], rules: Sequence[RuleDef]) -> dict[str, Any]:
    defs = {r.id: r for r in rules}
    accounts = sorted(s.account_id for s in snapshots)
    rule_ids = [r.id for r in rules if r.enabled]
    if not rule_ids:
        seen: dict[str, None] = {}
        for s in snapshots:
            for rs in s.rules:
                seen.setdefault(rs.rule_id)
        rule_ids = list(seen)
    matrix: list[dict[str, Any]] = []
    for s in sorted(snapshots, key=lambda x: x.account_id):
        for rule_id in rule_ids:
            rule = defs.get(rule_id)
            worst: tuple[RuleStatus, float | None, float | None] | None = None
            for rs in s.rules:
                if rs.rule_id != rule_id:
                    continue
                u, limit = _cell_util(rs, rule, s.account_id)
                if worst is None or (u is not None and (worst[1] is None or u > worst[1])):
                    worst = (rs, u, limit)
            if worst is None:
                hard = hard_limit_level(effective_levels(rule, s.account_id)) if rule is not None else None
                matrix.append(
                    {
                        "account_id": s.account_id,
                        "rule_id": rule_id,
                        "scope_key": f"acct:{s.account_id}",
                        "metric": rule.metric if rule is not None else "",
                        "value": None,
                        "limit": hard["threshold"] if hard is not None else None,
                        "utilization": None,
                        "band": "na",
                        "active_level": None,
                    }
                )
                continue
            rs, u, limit = worst
            matrix.append(
                {
                    "account_id": s.account_id,
                    "rule_id": rule_id,
                    "scope_key": rs.scope_key,
                    "metric": rs.metric,
                    "value": rs.value,
                    "limit": limit,
                    "utilization": u,
                    "band": band(u),
                    "active_level": rs.active_level,
                }
            )
    return {"accounts": accounts, "rules": rule_ids, "matrix": matrix}


def build_ranking(snapshots: Sequence[AccountSnapshot], rules: Sequence[RuleDef]) -> list[dict[str, Any]]:
    """Per account x rule (worst scope): closeness to the next level, closest first."""
    defs = {r.id: r for r in rules}
    items: list[dict[str, Any]] = []
    for s in snapshots:
        best: dict[str, dict[str, Any]] = {}
        for rs in s.rules:
            rule = defs.get(rs.rule_id)
            value = rs.value
            name, thr = rs.next_level, rs.next_threshold
            if (name is None or thr is None) and rule is not None:
                nx = next_level(value, rule, s.account_id)
                name = nx["name"] if nx is not None else None
                thr = nx["threshold"] if nx is not None else None
            op = rule.op if rule is not None else ("<=" if thr is not None and thr < 0 else ">=")
            u_next = None if thr is None else utilization(value, thr, op, rule.metric if rule else rs.metric)
            item = {
                "account_id": s.account_id,
                "rule_id": rs.rule_id,
                "scope_key": rs.scope_key,
                "metric": rs.metric,
                "value": value,
                "next_level": name,
                "next_threshold": thr,
                "utilization_next": u_next,
                "headroom": _headroom(value, thr),
                "headroom_money": rs.headroom_money,
            }
            cur = best.get(rs.rule_id)
            if cur is None or (
                u_next is not None and (cur["utilization_next"] is None or u_next > cur["utilization_next"])
            ):
                best[rs.rule_id] = item
        items.extend(best.values())

    def key(it: dict[str, Any]) -> tuple[int, float, str, str]:
        u = it["utilization_next"] if it["next_level"] is not None else None
        return (1 if u is None else 0, -(u or 0.0), it["account_id"], it["rule_id"])

    return sorted(items, key=key)


def build_exposure_grid(snapshots: Sequence[AccountSnapshot]) -> dict[str, Any]:
    accounts = sorted(s.account_id for s in snapshots)
    cells: list[dict[str, Any]] = []
    firm_net: dict[str, float] = defaultdict(float)
    gross: dict[str, float] = defaultdict(float)
    for s in snapshots:
        for e in s.exposures:
            cells.append(
                {"product": e.product, "account_id": s.account_id, "net_notional": e.net_notional, "net_lots": e.net_lots}
            )
            firm_net[e.product] += e.net_notional
            gross[e.product] += abs(e.net_notional)
    products = sorted(firm_net, key=lambda p: (-gross[p], p))
    return {"products": products, "accounts": accounts, "cells": cells, "firm_net": dict(firm_net)}


def build_compare_view(
    snapshots: Sequence[AccountSnapshot],
    rules: Sequence[RuleDef],
    liveness: Callable[[str], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Body of ``GET /risk/compare`` and data of the WS ``compare`` topic.

    ``liveness`` (account -> ``{"stale", "age_ms"}`` of its owning shard) adds
    ``liveness`` per account and the ``accounts_stale`` list: the figures of a
    stale account are the last ones received before its shard went down.
    """
    m = build_matrix(snapshots, rules)
    live = {s.account_id: (liveness(s.account_id) if liveness is not None else {"stale": False, "age_ms": None})
            for s in snapshots}
    return {
        "trading_day": snapshots[0].trading_day if snapshots else "",
        "as_of_ns": max((s.as_of_ns for s in snapshots), default=0),
        "accounts": m["accounts"],
        "rules": m["rules"],
        "matrix": m["matrix"],
        "ranking": build_ranking(snapshots, rules),
        "exposure": build_exposure_grid(snapshots),
        "liveness": {a: live[a] for a in sorted(live)},
        "accounts_stale": sorted(a for a, v in live.items() if v.get("stale")),
    }


# --------------------------------------------------------------------- positions
def _sector(products: ProductsConfig | None, product: str) -> str | None:
    if products is None:
        return None
    spec = products.products.get(product)
    return spec.sector if spec is not None else None


def position_rows(
    snapshots: Iterable[AccountSnapshot],
    products: ProductsConfig | None,
    *,
    account: str | None = None,
    product: str | None = None,
    instrument: str | None = None,
    exchange: str | None = None,
    sector: str | None = None,
) -> list[dict[str, Any]]:
    """Filtered PositionView rows (as builtins, with ``sector`` added) across all accounts."""
    rows = []
    for snap in snapshots:
        if account is not None and snap.account_id != account:
            continue
        for p in snap.positions:
            prod = p.product or product_code(p.instrument_id)
            sec = _sector(products, prod)
            if product is not None and prod != product:
                continue
            if instrument is not None and p.instrument_id != instrument:
                continue
            if exchange is not None and str(p.exchange) != exchange:
                continue
            if sector is not None and sec != sector:
                continue
            row = to_builtins(p)
            row["sector"] = sec
            rows.append(row)
    rows.sort(key=lambda r: (r["account_id"], r["instrument_id"], r["direction"]))
    return rows


def _group_key(row: Mapping[str, Any], group_by: str) -> str:
    field = {"account": "account_id", "product": "product", "instrument": "instrument_id",
             "exchange": "exchange", "sector": "sector"}[group_by]
    v = row.get(field)
    return str(v) if v is not None else ""


def aggregate_positions(rows: Sequence[Mapping[str, Any]], group_by: str) -> list[dict[str, Any]]:
    """Sum positions per ``group_by`` value: lots by direction, notional (long +, short -), margin, P&L."""
    if group_by not in GROUP_BY:
        raise ValueError(f"group_by must be one of {', '.join(GROUP_BY)}")
    groups: dict[str, dict[str, Any]] = {}
    for r in rows:
        k = _group_key(r, group_by)
        g = groups.get(k)
        if g is None:
            g = groups[k] = {
                "group": k,
                "positions": 0,
                "accounts": set(),
                "instruments": set(),
                "long_qty": 0,
                "short_qty": 0,
                "net_qty": 0,
                "td_qty": 0,
                "yd_qty": 0,
                "gross_notional": 0.0,
                "net_notional": 0.0,
                "margin": 0.0,
                "position_pnl": 0.0,
                "float_pnl_fifo": 0.0,
            }
        qty = int(r.get("td_qty", 0)) + int(r.get("yd_qty", 0))
        notional = float(r.get("notional", 0.0) or 0.0)
        sign = 1 if r.get("direction") == PosDirection.LONG.value else -1
        g["positions"] += 1
        g["accounts"].add(r["account_id"])
        g["instruments"].add(r["instrument_id"])
        if sign > 0:
            g["long_qty"] += qty
        else:
            g["short_qty"] += qty
        g["net_qty"] += sign * qty
        g["td_qty"] += int(r.get("td_qty", 0))
        g["yd_qty"] += int(r.get("yd_qty", 0))
        g["gross_notional"] += abs(notional)
        g["net_notional"] += sign * abs(notional)
        g["margin"] += float(r.get("margin", 0.0) or 0.0)
        g["position_pnl"] += float(r.get("position_pnl", 0.0) or 0.0)
        g["float_pnl_fifo"] += float(r.get("float_pnl_fifo", 0.0) or 0.0)
    out = []
    for k in sorted(groups):
        g = groups[k]
        g["accounts"] = sorted(g["accounts"])
        g["instruments"] = sorted(g["instruments"])
        out.append(g)
    return out


def positions_view(
    snapshots: Iterable[AccountSnapshot],
    products: ProductsConfig | None,
    *,
    group_by: str | None = None,
    **filters: str | None,
) -> dict[str, Any]:
    """``GET /positions`` body: filtered rows plus optional groups."""
    rows = position_rows(snapshots, products, **filters)
    return {
        "group_by": group_by,
        "items": rows,
        "groups": aggregate_positions(rows, group_by) if group_by else [],
    }



__all__ = [
    "GROUP_BY",
    "aggregate_positions",
    "band",
    "build_compare_view",
    "build_exposure_grid",
    "build_matrix",
    "build_ranking",
    "effective_levels",
    "hard_limit_level",
    "next_level",
    "position_rows",
    "positions_view",
    "utilization",
]
