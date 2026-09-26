"""Results of a run: per-arm statistics, the rule's trigger days, the scenario checks (01 §8.16).

**Arms** (layer A): every arm replays the same days with the same strategies, so
the difference between an arm and the baseline (``no_rules``) is the rule's
effect. Per account and for the whole book (``TOTAL``):

- return = the sum of the daily P&L ÷ initial capital (the strategies size on
  the initial capital, so returns add up and the arms stay comparable);
- maximum drawdown of the equity (initial capital + cumulative P&L) from its
  running peak; the worst day;
- trigger days per daily-loss level, days the breaker latched, opening orders
  rejected by the breaker (``REJECT_ACCOUNT_HALTED``) and adds rejected by the
  restrict level (``REJECT_ADD_TO_LOSER``).

The **trigger days** list every account-day on which the rule acted in an arm
(the restrict or halt level fired) with the day's P&L in both arms: the
same-day difference is the loss the rule avoided (positive) or caused
(negative); the next day's difference shows what happened after the latch was
released at the day roll.

**Scenarios** (layer D): every ``expect.checks`` entry of a scenario is
evaluated against the recorded events, account states and scripted orders.
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from futrisk.backtest.config import BacktestConfig, ScenarioSpec
from futrisk.backtest.replay import LEVELS, ArmResult

TOTAL = "TOTAL"
ACTING_LEVELS = ("restrict", "halt")


def _clean(x: Any) -> Any:
    """JSON-safe copy: NaN / inf -> None, tuples -> lists."""
    if isinstance(x, float):
        return x if math.isfinite(x) else None
    if isinstance(x, Mapping):
        return {str(k): _clean(v) for k, v in x.items()}
    if isinstance(x, list | tuple):
        return [_clean(v) for v in x]
    return x


# ------------------------------------------------------------------ statistics
def by_account(result: ArmResult) -> dict[str, list[dict[str, Any]]]:
    """Daily rows per account in trading-day order, plus ``TOTAL`` (P&L summed, flags OR-ed)."""
    out: dict[str, list[dict[str, Any]]] = {}
    for row in result.daily:
        out.setdefault(row["account_id"], []).append(row)
    for rows in out.values():
        rows.sort(key=lambda r: r["trading_day"])
    total: dict[str, dict[str, Any]] = {}
    for rows in out.values():
        for r in rows:
            t = total.setdefault(r["trading_day"], {"trading_day": r["trading_day"], "account_id": TOTAL, "day_pnl": 0.0,
                                                    "fee": 0.0, "orders": 0, "rejects": 0, "blocked_opens": 0,
                                                    "blocked_adds": 0, "halt_latched": False,
                                                    **{f"trig_{lv}": False for lv in LEVELS}})
            for k in ("day_pnl", "fee", "orders", "rejects", "blocked_opens", "blocked_adds"):
                t[k] += r[k]
            for k in ("halt_latched", *(f"trig_{lv}" for lv in LEVELS)):
                t[k] = t[k] or bool(r[k])
    if out:
        out[TOTAL] = [total[d] for d in sorted(total)]
    return out


def series_stats(rows: Sequence[Mapping[str, Any]], capital: float) -> dict[str, Any]:
    """Return, drawdown, worst day and rule counts of one account's daily rows (see the module docstring)."""
    pnl = [float(r["day_pnl"]) for r in rows]
    equity = capital
    peak = capital
    max_dd = 0.0
    for x in pnl:
        equity += x
        peak = max(peak, equity)
        if peak > 0:
            max_dd = max(max_dd, (peak - equity) / peak)
    worst = min(range(len(pnl)), key=lambda i: pnl[i]) if pnl else None
    out: dict[str, Any] = {
        "days": len(rows),
        "capital": capital,
        "pnl": sum(pnl),
        "total_ret": sum(pnl) / capital if capital else None,
        "max_dd": max_dd,
        "worst_day": pnl[worst] / capital if worst is not None and capital else None,
        "worst_day_date": rows[worst]["trading_day"] if worst is not None else None,
        "fee": sum(float(r["fee"]) for r in rows),
        "orders": sum(int(r["orders"]) for r in rows),
        "rejects": sum(int(r["rejects"]) for r in rows),
        "latched_days": sum(1 for r in rows if r["halt_latched"]),
        "blocked_opens": sum(int(r["blocked_opens"]) for r in rows),
        "blocked_adds": sum(int(r["blocked_adds"]) for r in rows),
    }
    for lv in LEVELS:
        out[f"{lv}_days"] = sum(1 for r in rows if r[f"trig_{lv}"])
    return out


def arm_stats(result: ArmResult) -> dict[str, dict[str, Any]]:
    """``{account: stats}`` of one arm, ``TOTAL`` included."""
    rows = by_account(result)
    caps = dict(result.capital)
    caps[TOTAL] = sum(result.capital.values())
    return {a: series_stats(r, caps.get(a, 0.0)) for a, r in rows.items()}


def baseline_of(cfg: BacktestConfig, results: Mapping[str, ArmResult]) -> str:
    """The arm the others are compared with: the first ``none`` arm, else the first arm."""
    for a in cfg.arms:
        if a.tier == "none" and a.name in results:
            return a.name
    return next(iter(results))


def trigger_days(base: ArmResult, arm: ArmResult) -> list[dict[str, Any]]:
    """Account-days on which the rule acted in ``arm`` (restrict or halt fired), with both arms' P&L."""
    base_rows = {(r["trading_day"], r["account_id"]): r for r in base.daily}
    arm_rows = sorted(arm.daily, key=lambda r: (r["trading_day"], r["account_id"]))
    days = sorted({r["trading_day"] for r in arm.daily})
    nxt = {d: days[i + 1] for i, d in enumerate(days[:-1])}
    by_key = {(r["trading_day"], r["account_id"]): r for r in arm_rows}
    out: list[dict[str, Any]] = []
    for r in arm_rows:
        if not any(r[f"trig_{lv}"] for lv in ACTING_LEVELS):
            continue
        key = (r["trading_day"], r["account_id"])
        b = base_rows.get(key)
        if b is None:
            continue
        cap = float(r["capital"])
        level = "halt" if r["trig_halt"] else "restrict"
        row: dict[str, Any] = {
            "trading_day": r["trading_day"],
            "account_id": r["account_id"],
            "level": level,
            "latched": bool(r["halt_latched"]),
            "value": r.get(f"trig_{level}_value"),
            "base_pnl": b["day_pnl"] / cap,
            "arm_pnl": r["day_pnl"] / cap,
            "effect": (r["day_pnl"] - b["day_pnl"]) / cap,
            "blocked_opens": r["blocked_opens"],
            "blocked_adds": r["blocked_adds"],
            "next_effect": None,
        }
        nd = nxt.get(r["trading_day"])
        if nd is not None:
            na, nb = by_key.get((nd, r["account_id"])), base_rows.get((nd, r["account_id"]))
            if na is not None and nb is not None:
                row["next_effect"] = (na["day_pnl"] - nb["day_pnl"]) / cap
        out.append(row)
    return out


def summarize_triggers(rows: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    rows = list(rows)
    effects = [r["effect"] for r in rows]
    nexts = [r["next_effect"] for r in rows if r["next_effect"] is not None]
    return {
        "days": len(rows),
        "latched": sum(1 for r in rows if r["latched"]),
        "effect_sum": sum(effects),
        "effect_positive": sum(1 for x in effects if x > 0),
        "effect_negative": sum(1 for x in effects if x < 0),
        "next_effect_sum": sum(nexts),
    }


def analyze_arms(cfg: BacktestConfig, results: Mapping[str, ArmResult]) -> dict[str, Any]:
    base_name = baseline_of(cfg, results)
    base = results[base_name]
    stats = {name: arm_stats(r) for name, r in results.items()}
    deltas: dict[str, dict[str, dict[str, Any]]] = {}
    triggers: dict[str, dict[str, Any]] = {}
    for name, r in results.items():
        if name == base_name:
            continue
        d: dict[str, dict[str, Any]] = {}
        for acct, st in stats[name].items():
            bs = stats[base_name].get(acct)
            if bs is None:
                continue
            d[acct] = {k: (st[k] - bs[k]) if st[k] is not None and bs[k] is not None else None
                       for k in ("total_ret", "max_dd", "worst_day")}
        deltas[name] = d
        rows = trigger_days(base, r)
        triggers[name] = {"days": rows, "summary": summarize_triggers(rows)}
    days = base.days
    checks = {
        "digest_mismatches": sum(int(r.stats.get("digest_mismatches", 0)) for r in results.values()),
        "settle_failed_checks": sum(int(r.stats.get("settle_failed_checks", 0)) for r in results.values()),
        "settle_errors": [e for r in results.values() for e in r.stats.get("settle_errors", [])][:20],
    }
    # backtest_runs.summary: one flat object per arm (the whole book), so a metric x arm table renders directly
    headline: dict[str, Any] = {"baseline": base_name, "days": len(days)}
    for name, st in stats.items():
        tot = st.get(TOTAL, {})
        h = {k: tot.get(k) for k in ("total_ret", "max_dd", "worst_day", "latched_days", "blocked_opens")}
        if name in triggers:
            s = triggers[name]["summary"]
            h.update(rule_days=s["days"], effect_sum=s["effect_sum"], next_effect_sum=s["next_effect_sum"])
        headline[name] = h
    out = {
        "kind": "arms",
        "baseline": base_name,
        "period": {"start": days[0] if days else None, "end": days[-1] if days else None, "days": len(days)},
        "accounts": sorted(base.accounts),
        "arms": {name: {"tier": r.tier, "threshold": r.threshold, "stats": stats[name]} for name, r in results.items()},
        "deltas": deltas,
        "triggers": triggers,
        "checks": checks,
        "headline": headline,
    }
    return dict(_clean(out))


# ------------------------------------------------------------------ scenarios
def check_scenario(spec: ScenarioSpec, r: ArmResult) -> list[dict[str, Any]]:
    """Evaluate the scenario's ``expect`` list against one arm's recorded events / states / scripted orders."""
    checks: list[dict[str, Any]] = []
    acct = spec.account
    raised = [e for e in r.events if e["kind"] == "ALERT_RAISED" and e["account_id"] == acct]
    for exp in spec.expect.get("checks", []):
        t = exp.get("type")
        ok = False
        detail: Any = None
        if t == "event":
            hits = [e for e in raised if e["rule_id"] == exp.get("rule", "daily_loss") and e["level"] == exp["level"]]
            detail = [(e["step"], e["latched"]) for e in hits]
            ok = len(hits) == int(exp.get("n", 1)) and all(e["step"] == exp["step"] for e in hits)
            if "latched" in exp:
                ok = ok and all(bool(e["latched"]) == bool(exp["latched"]) for e in hits)
        elif t == "no_event":
            hits = [e for e in raised if e["rule_id"] == exp.get("rule", "daily_loss") and e["level"] == exp["level"]]
            detail = [(e["step"], e["latched"]) for e in hits]
            ok = not hits
        elif t == "state":
            hits = [s for s in r.states if s["account_id"] == acct and s["to_state"] == exp["to"]
                    and ("latched" not in exp or bool(s["halted_latched"]) == bool(exp["latched"]))]
            detail = [(s["step"], s["halted_latched"]) for s in hits]
            ok = bool(hits) and hits[0]["step"] == exp["step"]
        elif t == "order":
            hits = [o for o in r.scripted if o["label"] == exp["label"]]
            detail = [(o["code"], o["statuses"]) for o in hits]
            want = exp["result"]
            if want == "FILLED":
                ok = bool(hits) and all(o["code"] is None and o["statuses"] and all(s == "FILLED" for s in o["statuses"]) for o in hits)
            else:
                ok = bool(hits) and all(o["code"] == want for o in hits)
        elif t == "final_state":
            last = [s for s in r.states if s["account_id"] == acct]
            detail = (last[-1]["to_state"], last[-1]["halted_latched"]) if last else None
            ok = bool(last) and last[-1]["to_state"] == exp["state"] and (
                "latched" not in exp or bool(last[-1]["halted_latched"]) == bool(exp["latched"]))
        elif t == "count":
            hits = [e for e in r.events if e["account_id"] == acct and e["kind"] == exp.get("kind", "ALERT_RAISED")
                    and (exp.get("rule") is None or e["rule_id"] == exp["rule"])
                    and (exp.get("level") is None or e["level"] == exp["level"])]
            detail = len(hits)
            ok = len(hits) == int(exp["n"])
        checks.append({"scenario": spec.name, "arm": r.arm, "check": exp, "ok": ok, "detail": detail})
    return checks


def analyze_scenarios(cfg: BacktestConfig, results: Mapping[tuple[int, str], ArmResult]) -> dict[str, Any]:
    out: dict[str, Any] = {"kind": "scenario", "scenarios": [], "checks": []}
    for i, spec in enumerate(cfg.scenarios):
        arms = {name: r for (j, name), r in results.items() if j == i}
        out["scenarios"].append({
            "name": spec.name, "description": spec.description,
            "arms": {name: {"tier": r.tier, "events": r.events, "states": r.states, "scripted": r.scripted,
                            "stats": {"orders": r.stats.get("orders"), "rejects": r.stats.get("rejects"),
                                      "fills": r.stats.get("fills")}} for name, r in arms.items()},
        })
        for name, r in arms.items():
            if r.tier == spec.expect.get("arm_tier", "block_open") or name == spec.expect.get("arm"):
                out["checks"].extend(check_scenario(spec, r))
    out["passed"] = all(c["ok"] for c in out["checks"]) if out["checks"] else None
    out["headline"] = {"checks": len(out["checks"]), "passed": sum(1 for c in out["checks"] if c["ok"]),
                       "all_passed": out["passed"]}
    return dict(_clean(out))


__all__ = [
    "ACTING_LEVELS",
    "TOTAL",
    "analyze_arms",
    "analyze_scenarios",
    "arm_stats",
    "baseline_of",
    "by_account",
    "check_scenario",
    "series_stats",
    "summarize_triggers",
    "trigger_days",
]
