"""Statistics of the paired experiment (01 §8.16 统计; plan §7.1).

All numbers come from the arms' settlement rows (``settle_day`` output), the
engine equity the replay records at the open / close of every day, and the
engine's risk events; nothing is re-valued here.

Definitions (also written into the report):

- **daily return** r_t = day P&L_t ÷ initial capital (the acceptance basis of the
  daily-loss rule); perf equity_t = initial + Σ P&L (no deposits in a backtest);
- **total P&L** Σ day P&L; **CAGR** from perf equity; **max drawdown** the
  largest (peak − value) ÷ peak of perf equity (the initial capital counts as
  the first peak); **Calmar** = CAGR ÷ max drawdown (not meaningful when CAGR
  ≤ 0); **daily vol** std(r) (annualised × √242); **CVaR95** the mean of the
  worst 5 % of r, reported as a positive loss;
- **account sets**: every account; ``TOTAL`` = the book accounts (controls such
  as ACC3 without its ladder are reported separately and are not in TOTAL);
  ``STATIC`` = the book accounts without an intraday ladder (every strategy but
  ``counter_trend_add``);
- **marks of an effect** (the ``basis``): an account-day's P&L is measured at
  the **close** (engine equity after the day's last market state − start of
  day), at the **settle** (the official settlement price, a VWAP-type price on
  SHFE/DCE: on a day that falls into the close it sits above the close, so a
  settle-based "next day" still contains the same day's last move), or at the
  **next open** (settle-based through day d+h, plus the next day's SOD book
  revalued at its first prices). The close basis is the primary one;
- **episode**: an account-day on which an acting daily-loss level (restrict
  or halt) fired in an arm; **paired delta** over horizon h = (arm − baseline)
  P&L from the start of day d to the basis mark of day d+h, cut at the next
  episode; positive = the rule avoided a loss, negative = it cost money (the
  position that was blocked or cut went on to gain);
- **true positive** (TP) of a halt trigger at horizon h, on the ``alert_only``
  arm (same positions as ``no_rules``, the trigger time known): the account's
  cumulative return from the trigger day's SOD to the basis mark of day d+h is
  still at or beyond −T ("the loss persisted beyond the threshold");
  **deepened**: it is at or below the value at the trigger. FP = not TP;
- **gap trigger**: the halt fired on the opening state of a contract the
  account holds (the opening gap alone crossed the line). A lower bound: the
  synthesized paths have no gap between the night and the day session;
- **bounding classes** (per account-day, static book after the open): *certain*
  / *impossible* / *uncertain* from :func:`futrisk.backtest.paths.pnl_bounds`;
  a trigger on a day the static book could not reach the line is
  *strategy-driven* (the account's own intraday trades did);
- **block bootstrap**: circular blocks of ``block`` days resampled jointly for
  both arms (the pairing is kept), ``samples`` draws, 2.5 %/97.5 % percentiles;
  the **path × day bootstrap** of an ensemble draws a path realization and a
  day resample per draw, so its interval covers both uncertainties.
"""

from __future__ import annotations

import json
import math
from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import Any

import numpy as np

from futrisk.backtest.paths import CERTAIN, IMPOSSIBLE, UNCERTAIN

TRADING_DAYS = 242
TOTAL = "TOTAL"
STATIC = "STATIC"
LEVELS = ("warn", "restrict", "halt", "derisk")
BASES = ("close", "settle", "open")
LADDER_STRATEGIES = ("counter_trend_add",)

AccountSets = Mapping[str, Sequence[str]]


# ------------------------------------------------------------------ series
def perf_path(rets: np.ndarray, capital: float) -> np.ndarray:
    return capital * (1.0 + np.cumsum(rets))


def max_drawdown(perf: np.ndarray, capital: float) -> float:
    if perf.size == 0:
        return 0.0
    path = np.concatenate([[capital], perf])
    peak = np.maximum.accumulate(path)
    dd = np.where(peak > 0, (peak - path) / peak, 0.0)
    return float(dd.max())


def cvar(rets: np.ndarray, alpha: float = 0.95) -> float:
    if rets.size == 0:
        return 0.0
    k = max(1, int(math.ceil(rets.size * (1.0 - alpha) - 1e-9)))  # 100 × (1 − 0.95) is 5.000000000000004
    worst = np.sort(rets)[:k]
    return float(-worst.mean())


def cagr(total_ret: float, n_days: int) -> float:
    if n_days <= 0:
        return 0.0
    years = n_days / TRADING_DAYS
    base = 1.0 + total_ret
    if base <= 0:
        return -1.0
    return float(base ** (1.0 / years) - 1.0)


def series_stats(rets: Sequence[float] | np.ndarray, capital: float) -> dict[str, float]:
    r = np.asarray(rets, dtype=float)
    n = int(r.size)
    total = float(r.sum())
    perf = perf_path(r, 1.0)
    mdd = max_drawdown(perf, 1.0)
    g = cagr(total, n)
    vol = float(r.std(ddof=1)) if n > 1 else 0.0
    mean = float(r.mean()) if n else 0.0
    return {
        "days": n,
        "total_pnl": total * capital,
        "total_ret": total,
        "cagr": g,
        "max_dd": mdd,
        "calmar": g / mdd if mdd > 1e-12 else float("nan"),
        "vol_d": vol,
        "vol_a": vol * math.sqrt(TRADING_DAYS),
        "sharpe": mean / vol * math.sqrt(TRADING_DAYS) if vol > 1e-12 else float("nan"),
        "cvar95": cvar(r, 0.95),
        "worst_day": float(r.min()) if n else 0.0,
        "best_day": float(r.max()) if n else 0.0,
    }


def _calmar(r: np.ndarray) -> float:
    m = max_drawdown(perf_path(r, 1.0), 1.0)
    return cagr(float(r.sum()), r.size) / m if m > 1e-12 else float("nan")


STAT_FNS: dict[str, Callable[[np.ndarray], float]] = {
    "total_ret": lambda r: float(r.sum()),
    "max_dd": lambda r: max_drawdown(perf_path(r, 1.0), 1.0),
    "cvar95": lambda r: cvar(r, 0.95),
    "vol_a": lambda r: float(r.std(ddof=1)) * math.sqrt(TRADING_DAYS) if r.size > 1 else 0.0,
    "calmar": _calmar,
}


def _block_indices(rng: np.random.Generator, n: int, block: int) -> np.ndarray:
    blk = max(1, min(block, n))
    nb = int(math.ceil(n / blk))
    starts = rng.integers(0, n, size=nb)
    return ((starts[:, None] + np.arange(blk)[None, :]) % n).ravel()[:n]


def block_bootstrap(
    a: np.ndarray, b: np.ndarray, stat: Callable[[np.ndarray], float], *, samples: int, block: int, seed: int
) -> dict[str, float]:
    """Paired circular block bootstrap of ``stat(a) − stat(b)``; ``p`` = two-sided bootstrap p-value of 0."""
    n = int(a.size)
    point = stat(a) - stat(b)
    if n < 2 or samples <= 0:
        return {"delta": point, "lo": float("nan"), "hi": float("nan"), "p_pos": float("nan"), "p": float("nan")}
    rng = np.random.default_rng(seed)
    deltas = np.empty(samples)
    for s in range(samples):
        idx = _block_indices(rng, n, block)
        deltas[s] = stat(a[idx]) - stat(b[idx])
    return _summarize_draws(point, deltas)


def _summarize_draws(point: float, deltas: np.ndarray) -> dict[str, float]:
    fin = deltas[np.isfinite(deltas)]
    if fin.size == 0:
        return {"delta": point, "lo": float("nan"), "hi": float("nan"), "p_pos": float("nan"), "p": float("nan")}
    p_pos = float((fin > 0).mean())
    # two-sided bootstrap p-value of "no effect": ties with 0 count on both sides (an arm that never differs
    # from its baseline has p = 1, not 0)
    p_ge = float((fin >= 0).mean())
    p_le = float((fin <= 0).mean())
    return {
        "delta": point,
        "lo": float(np.percentile(fin, 2.5)),
        "hi": float(np.percentile(fin, 97.5)),
        "p_pos": p_pos,
        "p": min(1.0, 2.0 * min(p_ge, p_le) + 1.0 / (fin.size + 1)),
    }


def path_day_bootstrap(
    pairs: Sequence[tuple[np.ndarray, np.ndarray]], stat: Callable[[np.ndarray], float], *, samples: int, block: int,
    seed: int,
) -> dict[str, float]:
    """Ensemble effect of ``stat(a) − stat(b)`` over K path realizations ``pairs`` = [(arm, baseline) per path].

    ``delta`` = median of the per-path effects, ``p5``/``p95`` their 5 %/95 % quantiles (path uncertainty alone);
    ``lo``/``hi`` the 2.5 %/97.5 % quantiles of draws that pick a random path and a day-block resample of it
    (path and day uncertainty together).
    """
    points = np.array([stat(a) - stat(b) for a, b in pairs], dtype=float)
    fin = points[np.isfinite(points)]
    out: dict[str, float] = {
        "delta": float(np.median(fin)) if fin.size else float("nan"),
        "mean": float(fin.mean()) if fin.size else float("nan"),
        "p5": float(np.percentile(fin, 5)) if fin.size else float("nan"),
        "p95": float(np.percentile(fin, 95)) if fin.size else float("nan"),
        "paths": int(len(pairs)),
        "paths_positive": int((fin > 0).sum()),
    }
    n = int(pairs[0][0].size) if pairs else 0
    if n < 2 or samples <= 0 or not pairs:
        out.update({"lo": float("nan"), "hi": float("nan"), "p_pos": float("nan"), "p": float("nan")})
        return out
    rng = np.random.default_rng(seed)
    deltas = np.empty(samples)
    for s in range(samples):
        a, b = pairs[int(rng.integers(0, len(pairs)))]
        idx = _block_indices(rng, n, block)
        deltas[s] = stat(a[idx]) - stat(b[idx])
    boot = _summarize_draws(out["delta"], deltas)
    out.update({k: boot[k] for k in ("lo", "hi", "p_pos", "p")})
    return out


def adjust_pvalues(p: Mapping[str, float], method: str = "holm") -> dict[str, float]:
    """Holm (family-wise) or Benjamini-Hochberg (FDR) adjusted p-values; NaN stays NaN."""
    items = [(k, v) for k, v in p.items() if v is not None and math.isfinite(v)]
    out = {k: float("nan") for k in p}
    m = len(items)
    if m == 0:
        return out
    order = sorted(items, key=lambda kv: kv[1])
    if method == "bh":
        adj = [0.0] * m
        prev = 1.0
        for i in range(m - 1, -1, -1):
            k, v = order[i]
            prev = min(prev, v * m / (i + 1))
            adj[i] = prev
        for i, (k, _v) in enumerate(order):
            out[k] = min(1.0, adj[i])
        return out
    run = 0.0
    for i, (k, v) in enumerate(order):
        run = max(run, min(1.0, v * (m - i)))
        out[k] = run
    return out


# ------------------------------------------------------------------ account sets
def account_sets(accounts: Sequence[str], *, roles: Mapping[str, str] | None = None,
                 strategies: Mapping[str, str] | None = None) -> dict[str, list[str]]:
    """``TOTAL`` (book accounts) and, when the book mixes ladder and other accounts, ``STATIC``."""
    roles = roles or {}
    strategies = strategies or {}
    book = [a for a in accounts if roles.get(a, "book") == "book"]
    out: dict[str, list[str]] = {TOTAL: book}
    static = [a for a in book if strategies.get(a) not in LADDER_STRATEGIES]
    if static and len(static) < len(book):
        out[STATIC] = static
    return out


def daily_matrix(daily: Iterable[Mapping[str, Any]], accounts: Sequence[str], sets: AccountSets | None = None,
                 field: str = "day_pnl") -> tuple[list[str], dict[str, np.ndarray]]:
    """Per account (and every account set) the ``field`` series aligned on the arm's trading days."""
    rows = list(daily)
    days = sorted({r["trading_day"] for r in rows})
    idx = {d: i for i, d in enumerate(days)}
    out = {a: np.zeros(len(days)) for a in accounts}
    for r in rows:
        a = r["account_id"]
        if a in out:
            v = r.get(field)
            out[a][idx[r["trading_day"]]] = 0.0 if v is None else float(v)
    for name, members in (sets or {TOTAL: list(accounts)}).items():
        out[name] = sum((out[a] for a in members if a in out), np.zeros(len(days)))
    return days, out


def capital_of(capital: Mapping[str, float], sets: AccountSets | None = None) -> dict[str, float]:
    out = dict(capital)
    for name, members in (sets or {TOTAL: list(capital)}).items():
        out[name] = float(sum(capital.get(a, 0.0) for a in members))
    return out


def arm_summary(result: Any, sets: AccountSets | None = None) -> dict[str, dict[str, Any]]:
    """Per account + account set: performance stats, trigger counts and bookkeeping counters."""
    accounts = list(result.accounts)
    sets = sets or {TOTAL: accounts}
    days, pnl = daily_matrix(result.daily, accounts, sets)
    caps = capital_of(result.capital, sets)
    out: dict[str, dict[str, Any]] = {}
    ev = [e for e in result.events if e["kind"] == "ALERT_RAISED" and e["rule_id"] == "daily_loss"]
    by_acct_rows: dict[str, list[Mapping[str, Any]]] = {}
    for r in result.daily:
        by_acct_rows.setdefault(r["account_id"], []).append(r)
    for a in [*accounts, *sets]:
        members = list(sets[a]) if a in sets else [a]
        cap = caps[a]
        st: dict[str, Any] = series_stats(pnl[a] / cap, cap) if cap > 0 else {}
        rows = [r for m in members for r in by_acct_rows.get(m, [])]
        mine = [e for e in ev if e["account_id"] in members]
        st.update({f"trig_{lv}": sum(1 for e in mine if e["level"] == lv) for lv in LEVELS})
        halts = [e for e in mine if e["level"] == "halt"]
        st["halt_at_open"] = sum(1 for e in halts if e["at_open"])
        st["halt_days"] = len({(e["trading_day"], e["account_id"]) for e in halts})
        st["halted_days"] = sum(1 for r in rows if r["halted"])
        st["latched_days"] = sum(1 for r in rows if r.get("halt_latched"))
        st["halt_revoked"] = sum(int(r["halt_revoked"]) for r in rows)
        st["blocked_opens"] = sum(int(r["blocked_opens"]) for r in rows)
        st["blocked_adds"] = sum(int(r.get("blocked_adds", 0)) for r in rows)
        st["orders"] = sum(int(r["orders"]) for r in rows)
        st["rejects"] = sum(int(r["rejects"]) for r in rows)
        st["fees"] = sum(float(r["fee"]) for r in rows)
        st["final_equity"] = cap + float(pnl[a].sum())
        st["capital"] = cap
        st["members"] = members
        out[a] = st
    return out


def aligned(arm: Any, base: Any, sets: AccountSets | None, field: str = "day_pnl"
            ) -> tuple[list[str], dict[str, np.ndarray], dict[str, np.ndarray]]:
    accounts = list(base.accounts)
    days_a, pa = daily_matrix(arm.daily, accounts, sets, field)
    days_b, pb = daily_matrix(base.daily, accounts, sets, field)
    if days_a != days_b:
        common = sorted(set(days_a) & set(days_b))
        ia = [days_a.index(d) for d in common]
        ib = [days_b.index(d) for d in common]
        pa = {k: v[ia] for k, v in pa.items()}
        pb = {k: v[ib] for k, v in pb.items()}
        days_a = common
    return days_a, pa, pb


def paired_deltas(arm: Any, base: Any, *, samples: int, block: int, seed: int, sets: AccountSets | None = None
                  ) -> dict[str, dict[str, Any]]:
    """Bootstrap CIs of arm − base per account and account set for the headline statistics."""
    sets = sets or {TOTAL: list(base.accounts)}
    _days, pa, pb = aligned(arm, base, sets)
    caps = capital_of(base.capital, sets)
    out: dict[str, dict[str, Any]] = {}
    for a in [*base.accounts, *sets]:
        cap = caps[a]
        if cap <= 0:
            continue
        ra, rb = pa[a] / cap, pb[a] / cap
        out[a] = {name: block_bootstrap(ra, rb, fn, samples=samples, block=block, seed=seed + i)
                  for i, (name, fn) in enumerate(STAT_FNS.items())}
    return out


ACTING_LEVELS = ("restrict", "halt")


def _by_key(daily: Iterable[Mapping[str, Any]]) -> dict[tuple[str, str], Mapping[str, Any]]:
    return {(r["account_id"], r["trading_day"]): r for r in daily}


def basis_series(daily: Iterable[Mapping[str, Any]], accounts: Sequence[str]
                 ) -> tuple[list[str], dict[str, dict[str, np.ndarray]]]:
    """Per account the day's P&L at the settle (``day_pnl``), at the close and of the SOD book at the open (yuan)."""
    rows = list(daily)
    days = sorted({r["trading_day"] for r in rows})
    idx = {d: i for i, d in enumerate(days)}
    out = {a: {k: np.zeros(len(days)) for k in ("settle", "close", "open")} for a in accounts}
    for r in rows:
        a = r["account_id"]
        if a not in out:
            continue
        i = idx[r["trading_day"]]
        out[a]["settle"][i] = float(r["day_pnl"])
        eq_sod = r.get("engine_equity_sod")
        eq_close = r.get("engine_equity_close")
        eq_open = r.get("engine_equity_open")
        out[a]["close"][i] = float(eq_close - eq_sod) if eq_close is not None and eq_sod is not None else float(r["day_pnl"])
        out[a]["open"][i] = float(eq_open - eq_sod) if eq_open is not None and eq_sod is not None else 0.0
    return days, out


def window_value(series: Mapping[str, np.ndarray], k: int, h: int, basis: str) -> float:
    """P&L from the start of day k to the ``basis`` mark of day k+h (settle-based through k+h−1)."""
    s = series["settle"]
    n = s.size
    end = min(k + h, n - 1)
    head = float(s[k:end].sum())
    if basis == "settle":
        return head + float(s[end])
    if basis == "close":
        return head + float(series["close"][end])
    # next open: settle through day end, plus the next day's SOD book at its open (0 past the last day)
    nxt = float(series["open"][end + 1]) if end + 1 < n else 0.0
    return head + float(s[end]) + nxt


def episodes(arm: Any, base: Any, horizons: Sequence[int], levels: Sequence[str] = ACTING_LEVELS) -> list[dict[str, Any]]:
    """Paired deltas of every day on which the rule acted in ``arm`` (> 0 avoided a loss, < 0 cost money).

    An episode is an account-day with an ALERT_RAISED of an acting daily-loss level. The row carries the time
    and value of the restrict and of the halt alert separately, the most severe ``level`` and, for every basis
    (close, settle, next open) and horizon h, the paired delta from the start of day d to the basis mark of day
    d+h, cut at the next episode of the account.
    """
    accounts = list(base.accounts)
    days, sa = basis_series(arm.daily, accounts)
    _db, sb = basis_series(base.daily, accounts)
    idx = {d: i for i, d in enumerate(days)}
    ev_days: dict[str, set[str]] = {}
    lv_ev: dict[tuple[str, str, str], Mapping[str, Any]] = {}
    order = {lv: i for i, lv in enumerate(levels)}
    for e in arm.events:
        if e["kind"] == "ALERT_RAISED" and e["rule_id"] == "daily_loss" and e["level"] in order:
            key = (e["account_id"], e["trading_day"])
            ev_days.setdefault(key[0], set()).add(key[1])
            lv_ev.setdefault((key[0], key[1], e["level"]), e)
    daily_by = _by_key(arm.daily)
    out: list[dict[str, Any]] = []
    for a, dset in ev_days.items():
        dl = sorted(dset)
        delta = {k: sa[a][k] - sb[a][k] for k in ("settle", "close", "open")}
        for j, d in enumerate(dl):
            k = idx.get(d)
            if k is None:
                continue
            stop = idx.get(dl[j + 1], len(days)) - 1 if j + 1 < len(dl) else len(days) - 1
            row = daily_by.get((a, d), {})
            worst = max((lv for lv in levels if (a, d, lv) in lv_ev), key=lambda lv: order[lv])
            rec: dict[str, Any] = {
                "account_id": a,
                "trading_day": d,
                "level": worst,
                "roll_day": bool(row.get("roll_day", False)),
                "est_settle": bool(row.get("est_settle", False)),
                "class_halt": row.get("class_halt"),
            }
            for lv in levels:
                e = lv_ev.get((a, d, lv))
                rec[f"{lv}_ts"] = e["ts"] if e else None
                rec[f"{lv}_value"] = e["value"] if e else None
                rec[f"{lv}_at_open"] = bool(e["at_open"]) if e else None
            e0 = lv_ev.get((a, d, worst))
            rec["ts"] = e0["ts"] if e0 else None  # the most severe level's own alert
            rec["value"] = e0["value"] if e0 else None
            rec["at_open"] = bool(e0["at_open"]) if e0 else False
            for h in horizons:
                end_h = min(h, stop - k)
                for basis in ("settle", "close", "open"):
                    b = basis
                    if basis == "open" and k + end_h + 1 <= len(days) - 1 and k + end_h + 1 > stop:
                        b = "settle"  # the next open belongs to the account's next episode
                    rec[f"delta_{basis}_{h}"] = window_value(delta, k, end_h, b)
                rec[f"delta_{h}"] = rec[f"delta_settle_{h}"]  # backwards-compatible name
            out.append(rec)
    return sorted(out, key=lambda r: (r["trading_day"], r["account_id"]))


def avoided_missed(eps: Sequence[Mapping[str, Any]], horizons: Sequence[int], level: str | None = None,
                   basis: str = "close") -> dict[str, dict[str, float]]:
    out: dict[str, dict[str, float]] = {}
    sel = [e for e in eps if level is None or e.get("level") == level]
    for h in horizons:
        vals = [float(e[f"delta_{basis}_{h}"]) for e in sel]
        out[str(h)] = {
            "episodes": len(vals),
            "avoided": float(sum(v for v in vals if v > 0)),
            "missed": float(sum(v for v in vals if v < 0)),
            "net": float(sum(vals)),
            "wins": sum(1 for v in vals if v > 0),
            "losses": sum(1 for v in vals if v < 0),
        }
    return out


def true_positives(alert_arm: Any, threshold: float, horizons: Sequence[int], *, exclude_roll_days: bool = True
                   ) -> dict[str, Any]:
    """TP / FP of the halt triggers of the ``alert_only`` arm per basis (definitions in the module docstring).

    Triggers on roll days are counted but left out of the rates when ``exclude_roll_days``.
    """
    accounts = list(alert_arm.accounts)
    days, ser = basis_series(alert_arm.daily, accounts)
    caps = dict(alert_arm.capital)
    idx = {d: i for i, d in enumerate(days)}
    daily_by = _by_key(alert_arm.daily)
    seen: set[tuple[str, str]] = set()
    rows: list[dict[str, Any]] = []
    for e in alert_arm.events:
        if not (e["kind"] == "ALERT_RAISED" and e["rule_id"] == "daily_loss" and e["level"] == "halt"):
            continue
        key = (e["account_id"], e["trading_day"])
        if key in seen:
            continue
        seen.add(key)
        a, d = key
        k = idx.get(d)
        if k is None:
            continue
        drow = daily_by.get(key, {})
        rec: dict[str, Any] = {"account_id": a, "trading_day": d, "value": e["value"], "at_open": bool(e["at_open"]),
                               "roll_day": bool(drow.get("roll_day")), "est_settle": bool(drow.get("est_settle"))}
        for h in horizons:
            for basis in BASES:
                cum = window_value(ser[a], k, h, basis) / caps[a]
                rec[f"cum_{basis}_{h}"] = cum
                rec[f"tp_{basis}_{h}"] = cum <= -threshold
                rec[f"deep_{basis}_{h}"] = e["value"] is not None and cum <= float(e["value"])
        rows.append(rec)
    used = [x for x in rows if not (exclude_roll_days and x["roll_day"])]
    summary: dict[str, Any] = {"triggers": len(rows), "used": len(used), "excluded_roll_days": len(rows) - len(used)}
    for basis in BASES:
        per: dict[str, Any] = {}
        for h in horizons:
            n = len(used)
            tp = sum(1 for x in used if x[f"tp_{basis}_{h}"])
            deep = sum(1 for x in used if x[f"deep_{basis}_{h}"])
            per[str(h)] = {"tp": tp, "fp": n - tp, "tp_rate": tp / n if n else float("nan"),
                           "deepened": deep, "deepened_rate": deep / n if n else float("nan")}
        summary[basis] = per
    for h in horizons:  # the settle-basis rates under the old keys
        summary[str(h)] = summary["settle"][str(h)]
    summary["gap_triggers"] = sum(1 for x in rows if x["at_open"])
    summary["gap_share"] = summary["gap_triggers"] / len(rows) if rows else float("nan")
    summary["on_roll_days"] = sum(1 for x in rows if x["roll_day"])
    summary["on_estimated_settle_days"] = sum(1 for x in rows if x["est_settle"])
    summary["rows"] = rows
    return summary


def bounding_counts(result: Any) -> dict[str, Any]:
    """Certain / uncertain / impossible account-days per level, crossed with the replayed path's triggers.

    ``interval`` = [certain, certain + uncertain]: the number of days the static book reaches the line on the
    true path lies in it. ``strategy_driven`` counts triggers on days the static book could not reach the line
    (the account's own intraday trades did): reported separately, they depend on the path model like any
    uncertain day.
    """
    out: dict[str, Any] = {}
    for lv in ("warn", "restrict", "halt"):
        c: dict[str, int] = {}
        for r in result.daily:
            cls = r.get(f"class_{lv}")
            if cls is None:
                continue
            trig = bool(r.get(f"trig_{lv}"))
            k = f"{cls}|{'trig' if trig else 'no'}"
            c[k] = c.get(k, 0) + 1
        certain = sum(v for k, v in c.items() if k.startswith(CERTAIN))
        uncertain = sum(v for k, v in c.items() if k.startswith(UNCERTAIN))
        impossible = sum(v for k, v in c.items() if k.startswith(IMPOSSIBLE))
        out[lv] = {
            "certain": certain,
            "uncertain": uncertain,
            "impossible": impossible,
            "strategy_driven": c.get(f"{IMPOSSIBLE}|trig", 0),
            "interval": [certain, certain + uncertain],
            "synth_triggers": sum(v for k, v in c.items() if k.endswith("|trig")),
            "certain_triggered": c.get(f"{CERTAIN}|trig", 0),
            "uncertain_triggered": c.get(f"{UNCERTAIN}|trig", 0),
            "mixed_session_days": sum(1 for r in result.daily if r.get("common_open") is False),
            "cross": c,
        }
    return out


def effect_interval(eps: Sequence[Mapping[str, Any]], horizon: int = 0, basis: str = "close") -> dict[str, float]:
    """[low, high] of the total paired delta over the halt episodes.

    Certain episodes count in full. An uncertain or strategy-driven episode may not have fired on the true path
    (its delta would then be about 0), so each counts between 0 and its measured value: low adds the negative
    ones, high the positive ones.
    """
    key = f"delta_{basis}_{horizon}"
    cert = sum(float(e[key]) for e in eps if e.get("class_halt") == CERTAIN)
    unsure = [float(e[key]) for e in eps if e.get("class_halt") != CERTAIN]
    neg = sum(v for v in unsure if v < 0)
    pos = sum(v for v in unsure if v > 0)
    return {"certain_part": cert, "uncertain_part": neg + pos, "low": cert + neg, "high": cert + pos,
            "certain_episodes": sum(1 for e in eps if e.get("class_halt") == CERTAIN), "other_episodes": len(unsure)}


def roll_gap_analysis(result: Any, series: Mapping[str, Any], multipliers: Mapping[str, int], threshold: float) -> dict[str, Any]:
    """What a naive continuous series (settle − previous row's settle) would have done to the daily-loss metric.

    Real contracts are traded (the old main is closed and the new one opened at
    the roll), so the engine's P&L never contains the roll basis gap. Here the
    SOD book of every account-day is re-valued with the naive series: the
    difference on a roll day is lots × multiplier × (new contract's previous
    settle − old contract's previous settle); DCE's new-contract previous settle
    is the loader's estimate (``estimated_rolls``). Counted: account-days whose
    naive return crosses −T while the true one does not (false triggers), and
    the reverse.
    """
    caps = dict(result.capital)
    false_trig = missed_trig = roll_days = 0
    gaps: dict[str, list[float]] = {}
    est_rolls: dict[str, int] = {}

    def new_pre(s: Any, i: int) -> float | None:
        cps = s.contract_pre_settle[i]
        if cps is None and i < len(getattr(s, "est_contract_pre_settle", [])):
            cps = s.est_contract_pre_settle[i]
        return None if cps is None else float(cps)

    for p, s in series.items():
        for i in range(len(s.days)):
            cps = new_pre(s, i) if s.rolls[i] else None
            if s.rolls[i] and cps and s.pre_settle[i]:
                gaps.setdefault(p, []).append(cps / s.pre_settle[i] - 1.0)
                if s.contract_pre_settle[i] is None:
                    est_rolls[p] = est_rolls.get(p, 0) + 1
    for r in result.daily:
        net = json.loads(r["sod_net"]) if r.get("sod_net") else {}
        err = 0.0
        hit = False
        for p, n in net.items():
            s = series.get(p)
            if s is None or n == 0:
                continue
            i = s.index.get(r["trading_day"])
            if i is None or not s.rolls[i]:
                continue
            cps, ps = new_pre(s, i), s.pre_settle[i]
            if cps is None or not ps:
                continue
            hit = True
            err += n * multipliers.get(p, 1) * (cps - ps)  # naive change − same-contract change
        if not hit:
            continue
        roll_days += 1
        cap = caps[r["account_id"]]
        true_ret = r["ret"]
        naive_ret = (r["day_pnl"] + err) / cap
        if naive_ret <= -threshold < true_ret:
            false_trig += 1
        if true_ret <= -threshold < naive_ret:
            missed_trig += 1
    return {
        "roll_account_days": roll_days,
        "naive_false_triggers": false_trig,
        "naive_missed_triggers": missed_trig,
        "gap_mean_abs_pct": {p: float(np.mean(np.abs(v)) * 100) for p, v in sorted(gaps.items()) if v},
        "gap_max_abs_pct": {p: float(np.max(np.abs(v)) * 100) for p, v in sorted(gaps.items()) if v},
        "rolls": {p: len(v) for p, v in sorted(gaps.items())},
        "estimated_rolls": dict(sorted(est_rolls.items())),
    }


def window_stats(result: Any, start: str, end: str, accounts: Sequence[str] | None = None,
                 sets: AccountSets | None = None) -> dict[str, dict[str, Any]]:
    """P&L, drawdown and triggers of an arm inside [start, end] (stress windows)."""
    accts = list(accounts) if accounts else list(result.accounts)
    sets = {k: [a for a in v if a in accts] for k, v in (sets or {TOTAL: accts}).items()}
    rows = [r for r in result.daily if start <= r["trading_day"] <= end and r["account_id"] in accts]
    evs = [e for e in result.events if start <= e["trading_day"] <= end and e["kind"] == "ALERT_RAISED"
           and e["rule_id"] == "daily_loss" and e["account_id"] in accts]
    caps = capital_of({a: result.capital[a] for a in accts}, sets)
    out: dict[str, dict[str, Any]] = {}
    days = sorted({r["trading_day"] for r in rows})
    for a in [*accts, *sets]:
        members = set(sets[a]) if a in sets else {a}
        sel = [r for r in rows if r["account_id"] in members]
        by_day: dict[str, float] = {}
        for r in sel:
            by_day[r["trading_day"]] = by_day.get(r["trading_day"], 0.0) + r["day_pnl"]
        pnl = np.array([by_day.get(d, 0.0) for d in days])
        cap = caps[a]
        mine = [e for e in evs if e["account_id"] in members]
        out[a] = {
            "pnl": float(pnl.sum()),
            "ret": float(pnl.sum() / cap) if cap else 0.0,
            "max_dd": max_drawdown(perf_path(pnl / cap, 1.0), 1.0) if cap else 0.0,
            "worst_day": float((pnl / cap).min()) if pnl.size and cap else 0.0,
            "worst_intraday": min((r["intraday_min_pct"] for r in sel), default=0.0),
            "halts": sum(1 for e in mine if e["level"] == "halt"),
            "warns": sum(1 for e in mine if e["level"] == "warn"),
            "days": len(days),
        }
    return out


def leave_one_out(delta: np.ndarray, base: np.ndarray, days: Sequence[str], capital: float,
                  blocks: Mapping[str, Sequence[str]]) -> dict[str, Any]:
    """The effect (Δ total return and Δ max drawdown) recomputed without each block of days (a year, a window).

    ``delta`` = arm − baseline daily P&L, ``base`` = the baseline's daily P&L (yuan).
    """
    if capital <= 0 or not days:
        return {}
    arm = base + delta
    full_ret = float(delta.sum()) / capital
    full_dd = max_drawdown(perf_path(arm / capital, 1.0), 1.0) - max_drawdown(perf_path(base / capital, 1.0), 1.0)
    rows: dict[str, Any] = {}
    idx = {d: i for i, d in enumerate(days)}
    for name, drop in blocks.items():
        cut = {idx[d] for d in drop if d in idx}
        if not cut or len(cut) == len(days):
            continue
        keep = np.array([i not in cut for i in range(len(days))])
        r = float(delta[keep].sum()) / capital
        dd = (max_drawdown(perf_path(arm[keep] / capital, 1.0), 1.0)
              - max_drawdown(perf_path(base[keep] / capital, 1.0), 1.0))
        rows[name] = {"days_dropped": len(cut), "d_total_ret": r, "d_max_dd": dd}
    if not rows:
        return {"full": {"d_total_ret": full_ret, "d_max_dd": full_dd}, "drops": {}}
    worst = min(rows.items(), key=lambda kv: kv[1]["d_total_ret"])
    return {
        "full": {"d_total_ret": full_ret, "d_max_dd": full_dd},
        "drops": rows,
        "min_d_total_ret": worst[1]["d_total_ret"],
        "min_drop": worst[0],
        "max_d_total_ret": max(v["d_total_ret"] for v in rows.values()),
        "sign_flips": sorted(k for k, v in rows.items() if full_ret != 0 and (v["d_total_ret"] > 0) != (full_ret > 0)),
    }


def fmt_pct(x: float | None, digits: int = 2) -> str:
    if x is None or (isinstance(x, float) and not math.isfinite(x)):
        return "—"
    return f"{x * 100:.{digits}f}%"


def fmt_pp(x: float | None, digits: int = 2) -> str:
    """A difference of percentages in percentage points."""
    if x is None or (isinstance(x, float) and not math.isfinite(x)):
        return "—"
    return f"{x * 100:+.{digits}f}pp"


def fmt_money(x: float | None) -> str:
    if x is None or (isinstance(x, float) and not math.isfinite(x)):
        return "—"
    return f"{x:,.0f}"


__all__ = [
    "ACTING_LEVELS",
    "BASES",
    "LADDER_STRATEGIES",
    "LEVELS",
    "STATIC",
    "STAT_FNS",
    "TOTAL",
    "TRADING_DAYS",
    "account_sets",
    "adjust_pvalues",
    "aligned",
    "arm_summary",
    "avoided_missed",
    "basis_series",
    "block_bootstrap",
    "bounding_counts",
    "capital_of",
    "cvar",
    "daily_matrix",
    "effect_interval",
    "episodes",
    "fmt_money",
    "fmt_pct",
    "fmt_pp",
    "leave_one_out",
    "max_drawdown",
    "paired_deltas",
    "path_day_bootstrap",
    "roll_gap_analysis",
    "series_stats",
    "true_positives",
    "window_stats",
    "window_value",
]
