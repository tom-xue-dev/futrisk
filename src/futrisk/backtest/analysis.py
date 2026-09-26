"""Turn arm results into the run summary (per kind: arms, stress, sweep, scenario).

The summary is a JSON-safe dict; ``headline`` is the short version returned to
the clearing-worker (``backtest_runs.summary``), the rest feeds the report and
``metrics.csv``.

A deterministic group (one path) is analysed directly (:func:`analyze_arms`).
A path-ensemble group is reduced realization by realization to a compact
:func:`realization_digest` (per-arm statistics, effects per basis, trigger
statistics and the daily return series needed for the bootstrap) and the
digests are aggregated by :func:`aggregate_ensemble`: every effect is reported
as the median over the paths with its 5-95 % path range and the 95 % interval
of the combined path × day-block bootstrap.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from typing import Any

import numpy as np

from futrisk.backtest.config import BacktestConfig, ScenarioSpec
from futrisk.backtest.metrics import (
    ACTING_LEVELS,
    BASES,
    STAT_FNS,
    TOTAL,
    account_sets,
    adjust_pvalues,
    aligned,
    arm_summary,
    avoided_missed,
    bounding_counts,
    capital_of,
    effect_interval,
    episodes,
    leave_one_out,
    paired_deltas,
    path_day_bootstrap,
    roll_gap_analysis,
    true_positives,
    window_stats,
)
from futrisk.backtest.pathmodel import spearman
from futrisk.backtest.paths import HitStats
from futrisk.backtest.replay import ArmResult

BASELINE = "no_rules"
HEADLINE_STATS = ("total_ret", "cagr", "max_dd", "calmar", "cvar95", "vol_a", "trig_halt", "halt_at_open", "blocked_opens")
DIGEST_STATS = ("total_ret", "cagr", "max_dd", "calmar", "cvar95", "vol_a", "worst_day", "total_pnl", "trig_warn",
                "trig_restrict", "trig_halt", "halt_at_open", "halt_days", "latched_days", "blocked_opens", "blocked_adds",
                "halt_revoked", "orders", "rejects", "fees")
DELTA_STATS = ("total_ret", "max_dd", "cvar95", "calmar", "vol_a")


def _clean(x: Any) -> Any:
    if isinstance(x, float):
        return x if math.isfinite(x) else None
    if isinstance(x, np.floating):
        v = float(x)
        return v if math.isfinite(v) else None
    if isinstance(x, np.integer):
        return int(x)
    if isinstance(x, dict):
        return {str(k): _clean(v) for k, v in x.items()}
    if isinstance(x, list | tuple):
        return [_clean(v) for v in x]
    return x


def sets_of(cfg: BacktestConfig, accounts: Sequence[str]) -> dict[str, list[str]]:
    roles = {a: r.role for a, r in cfg.accounts.items()}
    strategies = {a: r.strategy for a, r in cfg.accounts.items()}
    return account_sets(accounts, roles=roles, strategies=strategies)


def baseline_of(results: Mapping[str, ArmResult]) -> ArmResult | None:
    """``no_rules`` when run, else ``alert_only`` (its trading is identical)."""
    for r in results.values():
        if r.tier == "none":
            return r
    for r in results.values():
        if r.tier == "alert_only":
            return r
    return None


def alert_arm_of(results: Mapping[str, ArmResult], threshold: float | None = None) -> ArmResult | None:
    for r in results.values():
        if r.tier == "alert_only" and (threshold is None or abs(r.threshold - threshold) < 1e-12):
            return r
    return None


def ladder_pairs(cfg: BacktestConfig) -> dict[str, str]:
    """``{book account: control account}`` where a control runs the same strategy without its ladder."""
    out: dict[str, str] = {}
    for c, rc in cfg.accounts.items():
        if rc.role != "control" or not rc.enabled:
            continue
        for b, rb in cfg.accounts.items():
            if rb.role == "book" and rb.enabled and rb.strategy == rc.strategy and b not in out:
                out[b] = c
    return out


def rejects_by_code(r: ArmResult) -> dict[str, dict[str, int]]:
    out: dict[str, dict[str, int]] = {}
    for a, ex in (r.stats.get("executor") or {}).items():
        out[a] = dict(sorted((ex.get("rejects") or {}).items()))
    return out


def position_level(r: ArmResult) -> dict[str, Any]:
    """Value of the tier's forced reductions: lots closed × (mark − exit price), sign-adjusted, per mark.

    Marks: the day's close, the settlement price, the next day's first price. Positive = the reduced lots would
    have lost further by that mark (the reduction helped), negative = the price came back.
    """
    tot = {"close": 0.0, "settle": 0.0, "next_open": 0.0}
    n = 0
    for a in r.actions:
        if not a.get("accepted") or a.get("price") is None:
            continue
        mult = a.get("multiplier", 1)
        sign = 1 if a["direction"] == "SELL" else -1  # a sell closed a long: the long would have made (mark − exit)
        n += 1
        for k in tot:
            mark = a.get(k)
            if mark is None:
                mark = a.get("settle")
            if mark is None:
                continue
            tot[k] += -sign * (mark - a["price"]) * a["accepted"] * mult
    return {"actions": n, **{f"value_{k}": v for k, v in tot.items()}, "same_day_value": tot["settle"]}


# ------------------------------------------------------------------ deterministic group
def analyze_arms(cfg: BacktestConfig, results: Mapping[str, ArmResult], *, series: Mapping[str, Any] | None = None,
                 multipliers: Mapping[str, int] | None = None, hit_rates: Mapping[str, Any] | None = None,
                 stress_windows: Sequence[Mapping[str, Any]] = (), bootstrap: bool = True) -> dict[str, Any]:
    """The paired experiment over one period on one path (deterministic, or one realization)."""
    an = cfg.analysis
    base = baseline_of(results)
    accounts = list(next(iter(results.values())).accounts) if results else []
    sets = sets_of(cfg, accounts)
    out: dict[str, Any] = {"kind": "arms", "paths": 0, "baseline": base.arm if base else None, "sets": sets, "arms": {},
                           "deltas": {}, "episodes": {}, "avoided_missed": {}, "avoided_missed_by_level": {},
                           "effect_interval": {}, "position_level": {}, "bounding": {}, "tp_fp": None, "roll_gaps": None}
    for name, r in results.items():
        out["arms"][name] = {"tier": r.tier, "threshold": r.threshold, "stats": arm_summary(r, sets), "run": r.stats,
                             "rejects_by_code": rejects_by_code(r), "notes": sorted(set(r.notes))[:50]}
        if an.bounding:
            out["bounding"][name] = bounding_counts(r)
        if base is None or r is base or r.tier == "alert_only":
            continue
        if bootstrap:
            d = paired_deltas(r, base, samples=an.bootstrap_samples, block=an.bootstrap_block, seed=cfg.seed, sets=sets)
            _adjust(d, an.multiplicity)
            out["deltas"][name] = d
        eps = episodes(r, base, an.horizons)
        out["episodes"][name] = eps
        out["avoided_missed"][name] = {b: avoided_missed(eps, an.horizons, basis=b) for b in BASES}
        out["avoided_missed_by_level"][name] = {lv: {b: avoided_missed(eps, an.horizons, lv, basis=b) for b in BASES}
                                                for lv in ACTING_LEVELS}
        halt_eps = [e for e in eps if e.get("level") == "halt"]
        out["effect_interval"][name] = {f"{b}_{h}": effect_interval(halt_eps, h, b) for b in BASES for h in (0, an.primary_horizon)}
        out["position_level"][name] = position_level(r)
    alert = alert_arm_of(results, cfg.rules.threshold) or alert_arm_of(results)
    if alert is not None:
        out["tp_fp"] = true_positives(alert, alert.threshold, an.horizons, exclude_roll_days=an.exclude_roll_days)
    if base is not None and series is not None:
        out["roll_gaps"] = roll_gap_analysis(base, series, multipliers or {}, cfg.rules.threshold)
    if hit_rates is not None:
        out["hit_rate"] = hit_rates
    if stress_windows:
        out["stress"] = stress_table(results, stress_windows, sets)
    out["comparisons"] = comparisons(cfg, results, sets)
    if base is not None and an.leave_one_out:
        out["leave_one_out"] = loo_table(cfg, results, base, sets, stress_windows)
    out["checks"] = equality_checks(results)
    out["headline"] = headline(results, sets)
    if bootstrap:
        out["headline"]["effects"] = headline_effects(out["deltas"], paths=0)
    return dict(_clean(out))


def _adjust(deltas: dict[str, dict[str, Any]], method: str) -> None:
    """Adjusted p-values across the accounts / sets of one arm, per statistic (``p_adj``, ``significant``)."""
    for stat in DELTA_STATS:
        ps = {a: d[stat].get("p") for a, d in deltas.items() if stat in d}
        adj = adjust_pvalues({k: (v if v is not None else float("nan")) for k, v in ps.items()}, method)
        for a, v in adj.items():
            deltas[a][stat]["p_adj"] = v
            deltas[a][stat]["significant"] = bool(math.isfinite(v) and v < 0.05)


def equality_checks(results: Mapping[str, ArmResult]) -> dict[str, Any]:
    """alert_only must trade exactly like no_rules (same daily P&L): checked whenever both ran."""
    none = next((r for r in results.values() if r.tier == "none"), None)
    alert = next((r for r in results.values() if r.tier == "alert_only"), None)
    if none is None or alert is None:
        return {}
    a = {(r["trading_day"], r["account_id"]): r["day_pnl"] for r in alert.daily}
    b = {(r["trading_day"], r["account_id"]): r["day_pnl"] for r in none.daily}
    diff = max((abs(a[k] - b.get(k, float("nan"))) for k in a), default=0.0)
    return {"alert_only_equals_no_rules": bool(set(a) == set(b) and diff == 0.0), "max_abs_diff": diff}


def comparisons(cfg: BacktestConfig, results: Mapping[str, ArmResult], sets: Mapping[str, Sequence[str]]
                ) -> dict[str, Any]:
    """Decomposition (block_open = restrict + circuit break), placebo vs trigger timing, ladder contribution.

    All in Δ total return vs the baseline (fraction of the account's capital), per account and set.
    """
    base = baseline_of(results)
    if base is None:
        return {}
    sb = arm_summary(base, sets)
    keys = [*base.accounts, *sets]

    def dret(r: ArmResult) -> dict[str, float]:
        s = arm_summary(r, sets)
        return {a: s[a]["total_ret"] - sb[a]["total_ret"] for a in keys}

    out: dict[str, Any] = {}
    by_tier = {r.tier: r for r in results.values()}
    if "block_open" in by_tier and "halt_only" in by_tier:
        blk, ho = dret(by_tier["block_open"]), dret(by_tier["halt_only"])
        out["decomposition"] = {a: {"block_open": blk[a], "halt_only": ho[a], "restrict_part": blk[a] - ho[a]} for a in keys}
    plc: dict[str, Any] = {}
    for name, r in results.items():
        if r.tier != "placebo":
            continue
        ref_name = next((a.placebo_of for a in cfg.arms if a.name == name), None)
        ref = results.get(ref_name or "")
        if ref is None:
            continue
        dp, dr = dret(r), dret(ref)
        plc[name] = {"of": ref_name, "accounts": {a: {"placebo": dp[a], "trigger": dr[a], "timing_value": dr[a] - dp[a]}
                                                  for a in keys}}
    if plc:
        out["placebo"] = plc
    lad = ladder_pairs(cfg)
    if lad:
        out["ladder"] = {b: {"control": c, "book_ret": sb[b]["total_ret"], "control_ret": sb[c]["total_ret"],
                             "ladder_contribution": sb[b]["total_ret"] - sb[c]["total_ret"]}
                         for b, c in lad.items() if b in sb and c in sb}
    return out


def year_blocks(days: Sequence[str]) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for d in days:
        out.setdefault(d[:4], []).append(d)
    return out


def loo_table(cfg: BacktestConfig, results: Mapping[str, ArmResult], base: ArmResult, sets: Mapping[str, Sequence[str]],
              windows: Sequence[Mapping[str, Any]] = ()) -> dict[str, Any]:
    """Leave-one-out of the effect per acting arm: without each calendar year and each stress window."""
    out: dict[str, Any] = {}
    caps = capital_of(base.capital, sets)
    for name, r in results.items():
        if r is base or r.tier in ("none", "alert_only"):
            continue
        days, pa, pb = aligned(r, base, sets)
        blocks = {f"year {y}": ds for y, ds in year_blocks(days).items()}
        for w in windows:
            blocks[f"window {w['id']}"] = [d for d in days if w["start"] <= d <= w["end"]]
        out[name] = {a: leave_one_out(pa[a] - pb[a], pb[a], days, caps[a], blocks)
                     for a in [*sets, *(x for x in base.accounts if x not in sets)]}
    return out


def headline(results: Mapping[str, ArmResult], sets: Mapping[str, Sequence[str]] | None = None) -> dict[str, Any]:
    """Compact per-arm numbers (every account and set) for ``backtest_runs.summary``."""
    out: dict[str, Any] = {}
    for name, r in results.items():
        st = arm_summary(r, sets)
        out[name] = {a: {k: st[a].get(k) for k in HEADLINE_STATS} for a in st}
    return dict(_clean(out))


def stress_table(results: Mapping[str, ArmResult], windows: Sequence[Mapping[str, Any]],
                 sets: Mapping[str, Sequence[str]] | None = None) -> list[dict[str, Any]]:
    """Per window × arm × account (and set): P&L, drawdown, worst day, halts inside the window."""
    rows: list[dict[str, Any]] = []
    for w in windows:
        for name, r in results.items():
            if not r.days or w["end"] < r.days[0] or w["start"] > r.days[-1]:
                continue
            ws = window_stats(r, w["start"], w["end"], sets=sets)
            for acct, st in ws.items():
                rows.append({"window": w["id"], "title": w.get("title", w["id"]), "arm": name, "account_id": acct, **st,
                             "focus_day": w.get("focus_day")})
    return rows


def focus_days(results: Mapping[str, ArmResult], days: Sequence[str]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for name, r in results.items():
        for row in r.daily:
            if row["trading_day"] in days:
                out.append({"arm": name, **{k: row.get(k) for k in ("trading_day", "account_id", "ret", "close_ret",
                                                                       "intraday_min_pct", "open_pct", "close_pct",
                                                                       "halted", "halt_latched", "blocked_opens",
                                                                       "class_halt", "trig_halt")}})
    return out


# ------------------------------------------------------------------ ensemble
def realization_digest(cfg: BacktestConfig, results: Mapping[str, ArmResult], *, keep_arrays: bool = True,
                       windows: Sequence[Mapping[str, Any]] = (), full: bool = True) -> dict[str, Any]:
    """The numbers of one path realization that the ensemble aggregates (compact; the arm results are dropped).

    ``full=False`` (sweep cells) keeps the per-arm statistics only.
    """
    an = cfg.analysis
    base = baseline_of(results)
    accounts = list(next(iter(results.values())).accounts) if results else []
    sets = sets_of(cfg, accounts)
    caps = capital_of(dict(next(iter(results.values())).capital) if results else {}, sets)
    dg: dict[str, Any] = {"baseline": base.arm if base else None, "sets": sets, "capital": caps, "stats": {}, "returns": {},
                          "avoided_missed": {}, "position_level": {}, "bounding": {}, "effect_interval": {},
                          "tiers": {n: r.tier for n, r in results.items()},
                          "thresholds": {n: r.threshold for n, r in results.items()}}
    days: list[str] = []
    for name, r in results.items():
        st = arm_summary(r, sets)
        dg["stats"][name] = {a: {k: st[a].get(k) for k in DIGEST_STATS} for a in st}
        if an.bounding:
            dg["bounding"][name] = {lv: {k: v for k, v in b.items() if k != "cross"} for lv, b in bounding_counts(r).items()}
        if base is not None and keep_arrays:
            days, pa, _pb = aligned(r, base, sets)
            dg["returns"][name] = {a: (pa[a] / caps[a]).astype(np.float64) for a in pa if caps.get(a, 0) > 0}
        if base is None or r is base or r.tier == "alert_only" or not full:
            continue
        eps = episodes(r, base, an.horizons)
        dg["avoided_missed"][name] = {b: avoided_missed(eps, an.horizons, basis=b) for b in BASES}
        halt_eps = [e for e in eps if e.get("level") == "halt"]
        dg["effect_interval"][name] = effect_interval(halt_eps, an.primary_horizon, an.primary_basis)
        dg["position_level"][name] = position_level(r)
    dg["days"] = days
    if not full:
        return dg
    alert = alert_arm_of(results, cfg.rules.threshold) or alert_arm_of(results)
    if alert is not None:
        tp = true_positives(alert, alert.threshold, an.horizons, exclude_roll_days=an.exclude_roll_days)
        dg["tp_fp"] = {k: v for k, v in tp.items() if k != "rows"}
    dg["comparisons"] = comparisons(cfg, results, sets)
    if windows:
        dg["windows"] = {w["id"]: {name: window_stats(r, w["start"], w["end"], sets=sets) for name, r in results.items()}
                         for w in windows}
    return dg


def _q(vals: Sequence[float]) -> dict[str, float]:
    x = np.array([v for v in vals if v is not None and math.isfinite(v)], dtype=float)
    if x.size == 0:
        return {"median": float("nan"), "p5": float("nan"), "p95": float("nan"), "mean": float("nan"), "n": 0}
    return {"median": float(np.median(x)), "p5": float(np.percentile(x, 5)), "p95": float(np.percentile(x, 95)),
            "mean": float(x.mean()), "n": int(x.size)}


def _nested_q(items: Sequence[Any]) -> Any:
    """Element-wise quantiles over a list of equally shaped nested dicts of numbers."""
    first = items[0]
    if isinstance(first, dict):
        keys = [k for k in first if all(isinstance(it, dict) and k in it for it in items)]
        return {k: _nested_q([it[k] for it in items]) for k in keys}
    if isinstance(first, bool):
        return _q([float(bool(v)) for v in items])
    if isinstance(first, int | float):
        return _q([float(v) for v in items if v is not None])
    if isinstance(first, list) and all(isinstance(v, int | float) for v in first):
        return [_q([float(it[i]) for it in items]) for i in range(len(first))]
    return first


def aggregate_ensemble(cfg: BacktestConfig, digests: Mapping[int, Mapping[str, Any]], *,
                       windows: Sequence[Mapping[str, Any]] = ()) -> dict[str, Any]:
    """Ensemble summary of a group: medians, 5-95 % path ranges and path × day bootstrap intervals."""
    an = cfg.analysis
    ks = sorted(digests)
    ds = [digests[k] for k in ks]
    d0 = ds[0]
    sets = d0["sets"]
    base = d0["baseline"]
    out: dict[str, Any] = {"kind": "arms", "paths": len(ds), "baseline": base, "sets": sets, "arms": {}, "deltas": {},
                           "avoided_missed": {}, "position_level": {}, "bounding": {}, "effect_interval": {}}
    arm_names = list(d0["stats"])
    for name in arm_names:
        per = [d["stats"][name] for d in ds if name in d["stats"]]
        agg = _nested_q(per)
        out["arms"][name] = {"tier": d0["tiers"].get(name), "threshold": d0["thresholds"].get(name),
                             "stats": {a: {k: v["median"] for k, v in st.items()} for a, st in agg.items()},
                             "stats_q": agg}
        if name in d0["bounding"]:
            out["bounding"][name] = _nested_q([d["bounding"][name] for d in ds])
        if base is None or name == base:
            continue
        if name in d0["avoided_missed"]:
            out["avoided_missed"][name] = _nested_q([d["avoided_missed"][name] for d in ds])
            out["position_level"][name] = _nested_q([d["position_level"][name] for d in ds])
            out["effect_interval"][name] = _nested_q([d["effect_interval"][name] for d in ds])
        if d0["returns"].get(name) is None or name not in d0["returns"]:
            continue
        per_acct: dict[str, dict[str, Any]] = {}
        for a in d0["returns"][name]:
            pairs = [(d["returns"][name][a], d["returns"][base][a]) for d in ds
                     if name in d["returns"] and a in d["returns"][name] and a in d["returns"].get(base, {})]
            if not pairs:
                continue
            per_acct[a] = {stat: path_day_bootstrap(pairs, STAT_FNS[stat], samples=cfg.ensemble.bootstrap,
                                                    block=an.bootstrap_block, seed=cfg.seed + i)
                           for i, stat in enumerate(DELTA_STATS)}
        _adjust(per_acct, an.multiplicity)
        out["deltas"][name] = per_acct
    if "tp_fp" in d0:
        out["tp_fp"] = _nested_q([d["tp_fp"] for d in ds if "tp_fp" in d])
    comps = [d.get("comparisons") or {} for d in ds]
    out["comparisons"] = _nested_q(comps) if comps and comps[0] else {}
    if base is not None and an.leave_one_out and d0["returns"]:
        out["leave_one_out"] = ensemble_loo(ds, base, d0["capital"], windows)
    if windows and "windows" in d0:
        out["windows"] = {w["id"]: _nested_q([d["windows"][w["id"]] for d in ds]) for w in windows if w["id"] in d0["windows"]}
    out["headline"] = {name: {a: {k: v for k, v in st.items() if k in HEADLINE_STATS} for a, st in arm["stats"].items()}
                       for name, arm in out["arms"].items()}
    out["headline"]["effects"] = headline_effects(out["deltas"], paths=len(ds))
    return dict(_clean(out))


def headline_effects(deltas: Mapping[str, Mapping[str, Any]], *, paths: int) -> dict[str, Any]:
    """Δ total return / Δ max drawdown per acting arm and account set (the clearing-worker's summary).

    For an ensemble: the median over the path realizations with the 5-95 % path range and the path × day
    bootstrap interval; ``model_conditional`` marks numbers that depend on the calibrated path model.
    """
    out: dict[str, Any] = {"paths": paths, "model_conditional": paths > 0}
    for name, per in deltas.items():
        out[name] = {a: {stat: {k: v.get(k) for k in ("delta", "p5", "p95", "lo", "hi", "p_adj") if k in v}
                         for stat, v in d.items() if stat in ("total_ret", "max_dd")}
                     for a, d in per.items()}
    return out


def ensemble_loo(ds: Sequence[Mapping[str, Any]], base: str, caps: Mapping[str, float],
                 windows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Leave-one-out on the ensemble-mean daily delta (the mean over the paths of arm − baseline)."""
    days = ds[0]["days"]
    out: dict[str, Any] = {}
    blocks = {f"year {y}": dd for y, dd in year_blocks(days).items()}
    for w in windows:
        blocks[f"window {w['id']}"] = [d for d in days if w["start"] <= d <= w["end"]]
    for name in ds[0]["returns"]:
        if name == base:
            continue
        out[name] = {}
        for a in ds[0]["returns"][name]:
            cap = caps.get(a, 0.0)
            if cap <= 0:
                continue
            arm = np.mean([d["returns"][name][a] for d in ds], axis=0) * cap
            bas = np.mean([d["returns"][base][a] for d in ds], axis=0) * cap
            out[name][a] = leave_one_out(arm - bas, bas, days, cap, blocks)
    return out


def curves(ds: Sequence[Mapping[str, Any]], accounts: Sequence[str]) -> dict[str, Any]:
    """Cumulative Δ return vs the baseline over time per arm and account: 5 / 50 / 95 % over the paths."""
    base = ds[0]["baseline"]
    out: dict[str, Any] = {"days": list(ds[0]["days"])}
    for name in ds[0]["returns"]:
        if name == base:
            continue
        per: dict[str, Any] = {}
        for a in accounts:
            cum = [np.cumsum(d["returns"][name][a] - d["returns"][base][a]) for d in ds
                   if a in d["returns"].get(name, {}) and a in d["returns"].get(base, {})]
            if not cum:
                continue
            m = np.vstack(cum)
            per[a] = {"p5": np.percentile(m, 5, axis=0), "p50": np.median(m, axis=0), "p95": np.percentile(m, 95, axis=0)}
        out[name] = per
    return out


# ------------------------------------------------------------------ sensitivity
SENS_TIERS = ("block_open", "halt_only", "halve", "flatten")


def sensitivity_rows(cfg: BacktestConfig, groups: Sequence[tuple[str, str, Mapping[str, Any]]]) -> list[dict[str, Any]]:
    """One row per group (main + variants) × acting arm: Δ total return per account set and ACC3, ΔMDD of TOTAL,
    the ladder contribution in the baseline and the number of halts - medians [5 %, 95 %] for ensembles."""
    rows: list[dict[str, Any]] = []
    for key, title, s in groups:
        paths = int(s.get("paths") or 0)
        base = s.get("baseline")
        lad = (s.get("comparisons") or {}).get("ladder") or {}
        for name, arm in (s.get("arms") or {}).items():
            tier = arm.get("tier") or _tier_of(cfg, name)
            if tier not in SENS_TIERS:
                continue
            d = (s.get("deltas") or {}).get(name) or {}
            st_q = arm.get("stats_q") or {}
            row: dict[str, Any] = {"group": key, "title": title, "paths": paths, "arm": name, "tier": tier,
                                   "baseline": base}
            for a, v in d.items():
                tr = v.get("total_ret") or {}
                row[f"d_ret|{a}"] = tr.get("delta")
                row[f"d_ret_p5|{a}"] = tr.get("p5")
                row[f"d_ret_p95|{a}"] = tr.get("p95")
                row[f"d_ret_lo|{a}"] = tr.get("lo")
                row[f"d_ret_hi|{a}"] = tr.get("hi")
                md = v.get("max_dd") or {}
                row[f"d_mdd|{a}"] = md.get("delta")
                row[f"d_mdd_p5|{a}"] = md.get("p5")
                row[f"d_mdd_p95|{a}"] = md.get("p95")
            halts = (arm.get("stats") or {}).get(TOTAL, {}).get("latched_days")
            row["latched_days"] = halts
            if st_q:
                q = (st_q.get(TOTAL) or {}).get("latched_days") or {}
                row["latched_days_p5"], row["latched_days_p95"] = q.get("p5"), q.get("p95")
            for b, v in lad.items():
                if isinstance(v, dict):
                    lc = v.get("ladder_contribution")
                    row[f"ladder|{b}"] = lc.get("median") if isinstance(lc, dict) else lc
            rows.append(row)
    return rows


def _tier_of(cfg: BacktestConfig, name: str) -> str | None:
    for a in cfg.arms:
        if a.name == name:
            return a.tier
    return None


# ------------------------------------------------------------------ stress
def analyze_stress(cfg: BacktestConfig, per_window: Mapping[str, Mapping[str, ArmResult]],
                   windows: Sequence[Mapping[str, Any]], ensembles: Mapping[str, Mapping[str, Any]] | None = None
                   ) -> dict[str, Any]:
    """Per window: the window statistics per arm and account (medians over the paths for an ensemble)."""
    rows: list[dict[str, Any]] = []
    focus: list[dict[str, Any]] = []
    deltas: dict[str, Any] = {}
    accounts_sets: dict[str, Any] = {}
    for w in windows:
        res = per_window.get(w["id"], {})
        if not res:
            continue
        accounts = list(next(iter(res.values())).accounts)
        sets = sets_of(cfg, accounts)
        accounts_sets = sets
        ens = (ensembles or {}).get(w["id"])
        if ens is not None and "windows" in ens and w["id"] in ens["windows"]:
            for name, per in ens["windows"][w["id"]].items():
                for acct, st in per.items():
                    rows.append({"window": w["id"], "title": w.get("title", w["id"]), "arm": name, "account_id": acct,
                                 **{k: (v.get("median") if isinstance(v, dict) else v) for k, v in st.items()},
                                 "ret_p5": (st.get("ret") or {}).get("p5"), "ret_p95": (st.get("ret") or {}).get("p95"),
                                 "focus_day": w.get("focus_day"), "paths": ens.get("paths")})
        else:
            rows.extend(stress_table(res, [w], sets))
        if w.get("focus_day"):
            focus.extend({"window": w["id"], **x} for x in focus_days(res, [w["focus_day"]]))
        base = baseline_of(res)
        if base is not None:
            for name, r in res.items():
                if r is base or r.tier == "alert_only":
                    continue
                eps = episodes(r, base, cfg.analysis.horizons)
                deltas.setdefault(w["id"], {})[name] = {"avoided_missed": avoided_missed(eps, cfg.analysis.horizons),
                                                         "episodes": eps}
    headline_: dict[str, Any] = {}
    for row in rows:
        if row["account_id"] == TOTAL:
            headline_.setdefault(row["window"], {})[row["arm"]] = {k: row.get(k) for k in ("ret", "max_dd", "halts", "worst_day")}
    return dict(_clean({"kind": "stress", "windows": list(windows), "table": rows, "focus": focus, "episodes": deltas,
                        "sets": accounts_sets, "headline": headline_}))


# ------------------------------------------------------------------ sweep
def sweep_cells(cfg: BacktestConfig, per_arm_stats: Mapping[str, Mapping[str, Mapping[str, float]]], base: str
                ) -> dict[str, Any]:
    """Cells (tier|threshold) of one period: statistics and deltas against the baseline, per account and set."""
    b = per_arm_stats[base]
    cells: dict[str, Any] = {}
    for name, st in per_arm_stats.items():
        tier, _, thr = name.partition("|")
        if tier == "none":
            continue
        d: dict[str, Any] = {}
        for a, s in st.items():
            bb = b.get(a, {})
            d[a] = {
                "total_ret": s.get("total_ret"),
                "d_total_ret": _sub(s.get("total_ret"), bb.get("total_ret")),
                "max_dd": s.get("max_dd"),
                "d_max_dd": _sub(s.get("max_dd"), bb.get("max_dd")),
                "cvar95": s.get("cvar95"),
                "d_cvar95": _sub(s.get("cvar95"), bb.get("cvar95")),
                "calmar": s.get("calmar"),
                # a Calmar ratio of a non-positive CAGR is not a performance measure: no delta there
                "d_calmar": _sub(s.get("calmar"), bb.get("calmar")) if (bb.get("cagr") or 0) > 0 and (s.get("cagr") or 0) > 0 else None,
                "trig_halt": s.get("trig_halt"),
                "latched_days": s.get("latched_days"),
            }
        cells[name] = d
    return cells


def analyze_sweep(cfg: BacktestConfig, periods: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    """``periods``: per period ``{"cells": {cell: {account: values}}, "baseline": {account: stats},
    "baseline_arm": name, "paths": K, "cells_q": ...}`` (for an ensemble the cells are the medians over the paths
    of the per-path cells).

    Per period: the cells, the location of the optimum per tier and objective (interior or at the grid edge),
    the plateau test at the production threshold; across periods: the rank stability of the thresholds.
    """
    sw = cfg.sweep
    assert sw is not None
    grid: dict[str, Any] = {per: dict(p) for per, p in periods.items()}
    analysis: dict[str, Any] = {}
    for per, g in grid.items():
        analysis[per] = {}
        for tier in sw.tiers:
            if tier == "alert_only":
                continue
            for acct in sw.focus_accounts:
                for obj in sw.objectives:
                    vals = {t: objective_value(g["cells"].get(f"{tier}|{t}", {}).get(acct), obj) for t in sw.thresholds}
                    if obj == "calmar" and all(v is None for v in vals.values()):
                        analysis[per][f"{tier}|{acct}|{obj}"] = {"skipped": "CAGR <= 0: Calmar deltas not meaningful"}
                        continue
                    analysis[per][f"{tier}|{acct}|{obj}"] = plateau_test(vals, sw.center)
    stability: dict[str, Any] = {}
    names = list(grid)
    if len(names) >= 2:
        p1, p2 = names[0], names[1]
        for tier in sw.tiers:
            if tier == "alert_only":
                continue
            for acct in sw.focus_accounts:
                for obj in sw.objectives:
                    v1 = [objective_value(grid[p1]["cells"].get(f"{tier}|{t}", {}).get(acct), obj) for t in sorted(sw.thresholds)]
                    v2 = [objective_value(grid[p2]["cells"].get(f"{tier}|{t}", {}).get(acct), obj) for t in sorted(sw.thresholds)]
                    pairs = [(x, y) for x, y in zip(v1, v2, strict=True) if x is not None and y is not None]
                    if len(pairs) < 3:
                        continue
                    ths = [t for t, x, y in zip(sorted(sw.thresholds), v1, v2, strict=True) if x is not None and y is not None]
                    xs, ys = [p[0] for p in pairs], [p[1] for p in pairs]
                    stability[f"{tier}|{acct}|{obj}"] = {"spearman": spearman(xs, ys), "n": len(pairs),
                                                         "best_in": ths[xs.index(max(xs))], "best_out": ths[ys.index(max(ys))]}
    head = {per: {k: {kk: v.get(kk) for kk in ("best_threshold", "edge_optimum", "plateau", "center_gap_to_best")}
                  for k, v in a.items() if isinstance(v, dict) and "skipped" not in v and k.endswith("|total_ret")}
            for per, a in analysis.items()}
    return dict(_clean({"kind": "sweep", "grid": grid, "analysis": analysis, "stability": stability,
                        "thresholds": sw.thresholds, "tiers": list(sw.tiers), "periods": sw.periods,
                        "objectives": list(sw.objectives), "center": sw.center, "headline": head}))


def _sub(a: float | None, b: float | None) -> float | None:
    if a is None or b is None or not (math.isfinite(a) and math.isfinite(b)):
        return None
    return a - b


def objective_value(cell: Mapping[str, Any] | None, objective: str) -> float | None:
    """Larger is better: Δtotal_ret, −Δmax_dd, −Δcvar95, Δcalmar (None where not meaningful)."""
    if not cell:
        return None
    key = {"calmar": "d_calmar", "max_dd": "d_max_dd", "cvar95": "d_cvar95"}.get(objective, "d_total_ret")
    v = cell.get(key)
    if v is None:
        return None
    x = float(v)
    if not math.isfinite(x):
        return None
    return -x if objective in ("max_dd", "cvar95") else x


def plateau_test(values: Mapping[float, float | None], center: float = 0.03) -> dict[str, Any]:
    """Is ``center`` on a plateau of the objective, or on a slope / knife edge?

    - the optimum's location: the best threshold, and whether it sits at the edge of the grid (then the grid does
      not bracket the optimum and nothing is known beyond it);
    - **plateau** at ``center``: the objective is flat around it compared with the whole curve - the largest slope
      to its neighbours (per unit of threshold) is at most half the average slope over the grid (range ÷ span),
      and ``center`` is within a quarter of the range of the best value. A straight line (every local slope equal
      to the average) is therefore **not** a plateau.
    """
    fin = {float(t): float(v) for t, v in values.items() if v is not None and math.isfinite(v)}
    ths = sorted(fin)
    out: dict[str, Any] = {"values": {f"{t:g}": fin[t] for t in ths}}
    if len(ths) < 3:
        out["plateau"] = None
        return out
    best = max(ths, key=lambda t: fin[t])
    rng = max(fin.values()) - min(fin.values())
    out.update({"best_threshold": best, "best_value": fin[best], "range": rng,
                "edge_optimum": best in (ths[0], ths[-1]), "monotone": _monotone([fin[t] for t in ths])})
    if center not in fin:
        out["plateau"] = None
        return out
    i = ths.index(center)
    span = ths[-1] - ths[0]
    avg_slope = rng / span if span > 0 else 0.0
    local = []
    for j in (i - 1, i + 1):
        if 0 <= j < len(ths):
            local.append(abs(fin[ths[j]] - fin[center]) / abs(ths[j] - center))
    local_slope = max(local) if local else 0.0
    gap = fin[best] - fin[center]
    out.update({"center": center, "center_value": fin[center], "local_slope": local_slope, "average_slope": avg_slope,
                "center_gap_to_best": gap})
    if rng <= 1e-12:
        out["plateau"] = True  # flat everywhere
        out["reason"] = "the objective does not depend on the threshold"
        return out
    flat = local_slope <= 0.5 * avg_slope
    near = gap <= 0.25 * rng
    out["plateau"] = bool(flat and near)
    out["reason"] = ("flat around the centre and near the best" if flat and near else
                     "not flat around the centre (a slope)" if not flat else "flat but far from the best value")
    return out


def _monotone(v: Sequence[float]) -> str:
    inc = all(b >= a - 1e-15 for a, b in zip(v, v[1:], strict=False))
    dec = all(b <= a + 1e-15 for a, b in zip(v, v[1:], strict=False))
    return "flat" if inc and dec else "increasing" if inc else "decreasing" if dec else "no"


def rank_stability(values_in: Sequence[float], values_out: Sequence[float]) -> float:
    return spearman(list(values_in), list(values_out))


# ------------------------------------------------------------------ scenario
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
            "arms": {name: {"stats": arm_summary(r), "events": r.events, "states": r.states, "scripted": r.scripted,
                            "intraday": r.intraday} for name, r in arms.items()},
        })
        for name, r in arms.items():
            if r.tier == spec.expect.get("arm_tier", "block_open") or name == spec.expect.get("arm"):
                out["checks"].extend(check_scenario(spec, r))
    out["passed"] = all(c["ok"] for c in out["checks"]) if out["checks"] else None
    out["headline"] = {"checks": len(out["checks"]), "passed": sum(1 for c in out["checks"] if c["ok"]),
                       "all_passed": out["passed"]}
    return dict(_clean(out))


def hit_rate_summary(stats: Mapping[str, Mapping[int, HitStats]]) -> dict[str, Any]:
    out: dict[str, Any] = {"by_product": {}, "by_period": {}}
    agg: dict[int, HitStats] = {}
    for p, per in stats.items():
        out["by_product"][p] = {}
        for period, st in per.items():
            out["by_product"][p][str(period)] = {"days": st.days, "determined": st.determined, "hits": st.hits,
                                                 "hit_rate": st.hit_rate, "same_bar": st.same_bar, "ties": st.ties}
            agg.setdefault(period, HitStats()).add(st)
    for period, st in sorted(agg.items()):
        out["by_period"][str(period)] = {"days": st.days, "determined": st.determined, "hits": st.hits,
                                         "hit_rate": st.hit_rate, "same_bar": st.same_bar}
    return dict(_clean(out))


__all__ = [
    "BASELINE",
    "aggregate_ensemble",
    "analyze_arms",
    "analyze_scenarios",
    "analyze_stress",
    "analyze_sweep",
    "baseline_of",
    "check_scenario",
    "comparisons",
    "curves",
    "equality_checks",
    "headline",
    "hit_rate_summary",
    "ladder_pairs",
    "objective_value",
    "plateau_test",
    "position_level",
    "realization_digest",
    "sensitivity_rows",
    "sets_of",
    "stress_table",
    "sweep_cells",
]
