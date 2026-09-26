"""Backtest runner: the experiment on a spawn process pool, outputs, and ``run_job`` for the clearing-worker.

``run_job(config_name, overrides, out_dir, progress)`` is the entry point the
clearing-worker calls in its ``spawn`` process pool: a module-level function
that relies on nothing but its arguments. It loads ``config/backtest/<name>.yaml``
(or a path), applies the dotted overrides, runs the experiment and writes into
``out_dir``:

- ``report.html`` - self-contained report (inline CSS, base64 charts);
- ``metrics.csv`` - per arm / window / sweep cell and account;
- ``events.parquet`` - every risk event of every arm, group and path realization;
- ``states.parquet``, ``daily.parquet``, ``orders.parquet``, ``actions.parquet`` -
  the deterministic groups and the first realization of every ensemble group;
- ``ensemble.parquet`` - the compact daily rows of every realization (the
  ensemble statistics can be recomputed from it);
- ``summary.json`` - the full analysis; ``manifest.json`` - config hash, data
  hash (+ the files, hashed when read), seed, git SHA, code hash, versions, the
  calibrated path model, output hashes.

The experiment (:mod:`futrisk.backtest.experiment`) runs the independent
replays in parallel; every replay is deterministic, so the results do not
depend on the number of workers or on the completion order.
"""

from __future__ import annotations

import json
import math
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from futrisk.backtest.analysis import (
    _nested_q,
    aggregate_ensemble,
    analyze_arms,
    analyze_scenarios,
    analyze_stress,
    analyze_sweep,
    curves,
    hit_rate_summary,
    realization_digest,
    sensitivity_rows,
    sets_of,
    sweep_cells,
)
from futrisk.backtest.config import (
    BacktestConfig,
    StressWindow,
    app_path,
    base_files_hash,
    config_hash,
    load_backtest_config,
)
from futrisk.backtest.data import HistData
from futrisk.backtest.experiment import ArmTask, Group, Scheduler, estimate_tasks, groups_for, workers_for
from futrisk.backtest.manifest import code_hash, data_hash, file_sha256, git_sha, json_sha256, versions
from futrisk.backtest.pathmodel import PathModel, calibrate
from futrisk.backtest.paths import HitStats, order_hit_rate
from futrisk.backtest.replay import ArmResult, Replay
from futrisk.backtest.sources import DailySource, DataSource, MinuteSource, ScenarioSource, TickSource
from futrisk.backtest.strategies import DEFAULT_PRODUCTS, CalendarSpread, Strategy, make_strategy
from futrisk.backtest.timing import wall_s
from futrisk.common.clock import SystemClock, ns_to_bj
from futrisk.common.config import FullConfig, load_config_dir
from futrisk.common.paths import bundled_config_dir, resolve_home
from futrisk.core.instruments import InstrumentRegistry

ProgressFn = Callable[[float, str | None], None]

# caches of a long-lived process (a clearing-pool worker runs many jobs): validated against the files on disk
_BASE_CACHE: dict[str, tuple[FullConfig, dict[str, tuple[int, int]]]] = {}
_HIST_CACHE: dict[tuple[Any, ...], HistData] = {}
_MODEL_CACHE: dict[tuple[Any, ...], PathModel] = {}


# ------------------------------------------------------------------ setup
def _stats_of(root: Path, rels: Sequence[str]) -> dict[str, tuple[int, int]]:
    out: dict[str, tuple[int, int]] = {}
    for rel in rels:
        try:
            st = (root / rel).stat()
            out[rel] = (st.st_mtime_ns, st.st_size)
        except OSError:
            out[rel] = (-1, -1)
    return out


def load_base(cfg: BacktestConfig) -> FullConfig:
    """The base configuration (cached per directory; reloaded when one of its files changed on disk)."""
    root = str(app_path(cfg.base_config).resolve()) if cfg.base_config else str(bundled_config_dir())
    hit = _BASE_CACHE.get(root)
    if hit is not None and _stats_of(Path(root), list(hit[1])) == hit[1]:
        return hit[0]
    base = load_config_dir(root)
    _BASE_CACHE[root] = (base, _stats_of(Path(root), sorted(base.files)))
    return base


def base_hash(base: FullConfig) -> str:
    return base_files_hash(dict(base.files))


def account_plan(cfg: BacktestConfig, only: Sequence[str] | None = None) -> dict[str, tuple[str, list[str], dict[str, Any]]]:
    """``{account: (strategy, products, params)}`` of the enabled accounts (``only``: a subset)."""
    out: dict[str, tuple[str, list[str], dict[str, Any]]] = {}
    for a, run in sorted(cfg.accounts.items()):
        if not run.enabled or (only is not None and a not in only):
            continue
        prods = list(run.products) if run.products else list(DEFAULT_PRODUCTS.get(run.strategy, []))
        out[a] = (run.strategy, prods, dict(run.params))
    return out


def spread_products(cfg: BacktestConfig) -> set[str]:
    out: set[str] = set()
    for strat, _prods, params in account_plan(cfg).values():
        if strat == CalendarSpread.name:
            out.add(str(params.get("product", "rb")))
    return out


def needed_products(cfg: BacktestConfig, only: Sequence[str] | None = None) -> list[str]:
    want = list(cfg.products) if only is None else []
    for strat, prods, params in account_plan(cfg, only).values():
        extra = [str(params.get("product", "rb"))] if strat == CalendarSpread.name else []
        for p in [*prods, *(params.get("weights") or {}), *extra]:
            if p not in want:
                want.append(p)
    return want


def get_hist(cfg: BacktestConfig, base: FullConfig) -> HistData:
    """The data of ``cfg`` (cached per process; reloaded when a file it read changed on disk)."""
    key = (str(cfg.data_dir()), tuple(needed_products(cfg)), tuple(sorted(spread_products(cfg))), base_hash(base))
    hist = _HIST_CACHE.get(key)
    if hist is None or hist.stale():
        hist = HistData(cfg.data_dir(), needed_products(cfg), base.products, all_contract_products=spread_products(cfg))
        _HIST_CACHE[key] = hist
    return hist


def build_source(cfg: BacktestConfig, base: FullConfig, *, start: str | None, end: str | None,
                 scenario: int | None = None, products: Sequence[str] | None = None,
                 path_model: PathModel | None = None, path_index: int | None = None) -> DataSource:
    if cfg.layer == "D":
        if scenario is None:
            raise ValueError("layer D needs a scenario index")
        return ScenarioSource(cfg.scenarios[scenario], base.products, base.calendar)
    if cfg.layer == "C":
        raw = app_path(cfg.data.ticks_home) if cfg.data.ticks_home else resolve_home()
        raw = raw / "ticks" / "raw"
        hist: HistData | None
        try:
            hist = get_hist(cfg, base)
        except Exception:  # pragma: no cover - history is optional for layer C
            hist = None
        return TickSource(raw, base.products, base.calendar, days=cfg.data.tick_days, products=cfg.products, hist=hist)
    hist = get_hist(cfg, base)
    run_cfg = cfg.model_copy(update={"start": start, "end": end})
    if cfg.layer == "B":
        return MinuteSource(hist, run_cfg, base.calendar, far_products=spread_products(cfg), products=products)
    return DailySource(hist, run_cfg, base.calendar, far_products=spread_products(cfg), products=products,
                       path_model=path_model, path_index=path_index)


def build_strategies(cfg: BacktestConfig, available: Sequence[str], *, only: Sequence[str] | None = None,
                     notes: list[str] | None = None) -> dict[str, Strategy]:
    out: dict[str, Strategy] = {}
    avail = set(available)
    for a, (strat, prods, params) in account_plan(cfg).items():
        if only is not None and a not in only:
            continue
        keep = [p for p in prods if p in avail]
        missing = [p for p in prods if p not in avail]
        if missing and notes is not None:
            notes.append(f"{a}: products without data skipped: {', '.join(missing)}")
        if not keep:
            if notes is not None:
                notes.append(f"{a}: no product with data; account skipped")
            continue
        out[a] = make_strategy(strat, params, account_id=a, products=keep)
    return out


def path_model_for(cfg: BacktestConfig, base: FullConfig) -> PathModel:
    """The calibrated path model of the ensemble (cached per data, products and ensemble settings)."""
    hist = get_hist(cfg, base)
    e = cfg.ensemble
    key = (str(cfg.data_dir()), tuple(hist.products), e.model_dump_json(), cfg.seed, tuple(sorted(hist.file_sha.items())))
    model = _MODEL_CACHE.get(key)
    if model is None:
        registry = InstrumentRegistry(hist.base_products, hist.calendar(base.calendar), allow_unlisted=True)
        model = calibrate(hist, registry, hist.products, seed=cfg.seed, calibration_period=e.calibration_period,
                          order_period=e.order_period, check_periods=e.check_periods, grid_s=e.grid_s,
                          shrink_days=e.shrink_days, min_days=e.min_days, sigma=e.sigma, rho=e.rho)
        _MODEL_CACHE[key] = model
    return model


# ------------------------------------------------------------------ one task
class _FileProgress:
    def __init__(self, path: str) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def __call__(self, frac: float, msg: str | None = None) -> None:
        import os

        # best effort: on Windows the parent may hold the file open while polling it
        tmp = self.path.with_suffix(".tmp")
        try:
            tmp.write_text(f"{frac:.6f}", encoding="utf-8")
            os.replace(tmp, self.path)
        except OSError:
            pass


def run_arm_task(task: ArmTask, progress: ProgressFn | None = None) -> ArmResult:
    """Run one task (module level: the process-pool entry point)."""
    cfg = task.cfg
    base = load_base(cfg)
    products = needed_products(cfg, task.accounts) if task.accounts is not None else None
    source = build_source(cfg, base, start=task.start, end=task.end, scenario=task.scenario, products=products,
                          path_model=task.path_model, path_index=task.realization)
    notes: list[str] = []
    use = True
    scenario_account = None
    if cfg.layer == "D":
        assert task.scenario is not None
        spec = cfg.scenarios[task.scenario]
        use = spec.use_strategy
        scenario_account = spec.account
        strategies = build_strategies(cfg, source.products(), only=[spec.account], notes=notes) if use else {}
    else:
        strategies = build_strategies(cfg, source.products(), only=task.accounts, notes=notes)
    run_cfg = cfg.model_copy(update={"warmup_days": task.warmup_days}) if task.warmup_days else cfg
    prog = progress or (_FileProgress(task.progress_file) if task.progress_file else None)
    rep = Replay(
        cfg=run_cfg,
        arm=task.arm,
        source=source,
        base_rules=base.risk_rules,
        base_accounts=base.accounts,
        strategies=strategies,
        label=task.label,
        config_hash=config_hash(cfg, base_hash(base)),
        record_days=set(task.record_days),
        progress=prog,
        use_strategies=use,
        scenario_account=scenario_account,
        placebo=task.placebo_map(),
        record_orders=task.record_orders,
    )
    res = rep.run()
    res.notes = sorted(set(notes + res.notes))
    res.stats["group"] = task.group
    res.stats["realization"] = task.realization
    res.stats["data_files"] = source.file_hashes()
    return res


# ------------------------------------------------------------------ windows
def resolve_windows(cfg: BacktestConfig, hist: HistData | None) -> list[dict[str, Any]]:
    """Stress windows with concrete dates (``auto`` windows: the largest same-contract drops of a product)."""
    out: list[dict[str, Any]] = []
    if cfg.stress is None:
        return out
    for w in cfg.stress.windows:
        out.extend(_resolve_window(w, hist))
    return out


def _resolve_window(w: StressWindow, hist: HistData | None) -> list[dict[str, Any]]:
    base = {"id": w.id, "title": w.title, "products": list(w.products), "note": w.note, "focus_day": w.focus_day}
    if w.auto != "largest_drops":
        return [{**base, "start": w.start or "00000000", "end": w.end or "99999999"}]
    if hist is None or not w.auto_product or w.auto_product not in hist.series:
        return []
    s = hist.series[w.auto_product]
    moves: list[tuple[float, str]] = []
    for d in s.days:
        m = s.same_contract_change(d)
        if m is not None and (w.start is None or d >= w.start) and (w.end is None or d <= w.end):
            moves.append((m, d))
    moves.sort()
    out: list[dict[str, Any]] = []
    for m, d in moves[: w.top_n]:
        i = s.index[d]
        lo = s.days[max(0, i - w.pad_days)]
        hi = s.days[min(len(s.days) - 1, i + w.pad_days)]
        out.append({**base, "id": f"{w.id}_{d}", "title": f"{w.title} {d} ({m * 100:+.2f}%)", "start": lo, "end": hi,
                    "focus_day": d})
    return out


# ------------------------------------------------------------------ run state
@dataclass
class RunStore:
    """What the run keeps while the realizations complete (bounded: ensemble realizations are digested)."""

    det: dict[str, dict[str, ArmResult]] = field(default_factory=dict)  # deterministic groups
    first: dict[str, dict[str, ArmResult]] = field(default_factory=dict)  # realization 0 of ensemble groups
    digests: dict[str, dict[int, dict[str, Any]]] = field(default_factory=dict)
    events: list[dict[str, Any]] = field(default_factory=list)
    compact: list[pd.DataFrame] = field(default_factory=list)
    data_files: dict[str, str] = field(default_factory=dict)
    perf: dict[str, float] = field(default_factory=dict)
    notes: set[str] = field(default_factory=set)
    copies: int = 0
    runs: list[dict[str, Any]] = field(default_factory=list)


PERF_KEYS = ("ticks", "states", "orders", "replay_s", "settle_s", "plan_s", "strategy_s")
COMPACT_COLS = ("day_pnl", "close_ret", "open_ret", "ret", "halt_latched", "trig_restrict", "trig_halt", "blocked_opens",
                "blocked_adds")


def _compact_frame(g: Group, r: int, per_arm: Mapping[str, ArmResult]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for name, res in per_arm.items():
        if not res.daily:
            continue
        cols: dict[str, Any] = {"group": g.key, "realization": r, "arm": name, "tier": res.tier}
        df = pd.DataFrame({"trading_day": [x["trading_day"] for x in res.daily],
                           "account_id": [x["account_id"] for x in res.daily],
                           **{c: [x.get(c) for x in res.daily] for c in COMPACT_COLS}})
        for k, v in cols.items():
            df.insert(0, k, v)
        frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def run_experiment(cfg: BacktestConfig, groups: Sequence[Group], *, progress: ProgressFn, path_models: Mapping[str, PathModel],
                   windows: Sequence[Mapping[str, Any]], progress_dir: Path) -> RunStore:
    store = RunStore()

    def on_realization(g: Group, r: int | None, per_arm: dict[str, ArmResult]) -> None:
        for res in per_arm.values():
            store.data_files.update(res.stats.get("data_files", {}))
            store.notes.update(res.notes)
            if res.stats.get("copied_from") is None:
                for k in PERF_KEYS:
                    store.perf[k] = store.perf.get(k, 0.0) + float(res.stats.get(k, 0) or 0)
                replayed = max(0, len(res.accounts) - len(res.stats.get("copied_accounts") or ()))
                store.perf["days"] = store.perf.get("days", 0.0) + len(res.days) * replayed
            store.copies += len(res.stats.get("copied_accounts") or ())
            store.runs.append({"group": g.key, "realization": r, "arm": res.arm, "tier": res.tier, "days": len(res.days),
                               "copied_accounts": list(res.stats.get("copied_accounts") or ())})
        for res in per_arm.values():
            for e in res.events:
                store.events.append({"run": res.label, "group": g.key, "realization": r, **e})
        if r is None:
            store.det[g.key] = per_arm
            return
        # stress windows: a window group's own window; the main group's slices of the configured windows
        wins = [dict(g.meta)] if g.kind == "window" else (list(windows) if g.kind == "main" else [])
        store.digests.setdefault(g.key, {})[r] = realization_digest(
            g.cfg, per_arm, keep_arrays=g.kind in ("main", "variant"), windows=wins, full=g.kind != "period")
        store.compact.append(_compact_frame(g, r, per_arm))
        if r == 0:
            store.first[g.key] = per_arm

    sched = Scheduler(groups, run_task=run_arm_task, workers=workers_for(cfg, estimate_tasks(groups)), progress=progress,
                      path_models=path_models, on_realization=on_realization, progress_dir=progress_dir)
    sched.run()
    return store


# ------------------------------------------------------------------ outputs
EVENT_COLUMNS = ["run", "group", "realization", "arm", "tier", "threshold", "trading_day", "ts_ns", "ts", "account_id",
                 "kind", "rule_id", "level", "action", "severity", "latched", "value", "threshold_value", "alert_key",
                 "episode", "at_open", "point_kinds", "step", "message"]


def _frame(rows: list[dict[str, Any]], columns: Sequence[str] | None = None) -> pd.DataFrame:
    df = pd.DataFrame.from_records(rows) if rows else pd.DataFrame(columns=list(columns or []))
    if columns:
        for c in columns:
            if c not in df.columns:
                df[c] = None
        df = df[list(columns) + [c for c in df.columns if c not in columns]]
    return df


def _write_parquet(df: pd.DataFrame, path: Path) -> str:
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pa.Table.from_pandas(df, preserve_index=False)
    table = table.replace_schema_metadata(None)
    pq.write_table(table, path, compression="zstd")
    return file_sha256(path)


def _rows(store: RunStore, attr: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for key, per_arm in [*store.det.items(), *store.first.items()]:
        r = None if key in store.det else 0
        for res in per_arm.values():
            for row in getattr(res, attr):
                out.append({"run": res.label, "group": key, "realization": r, **row})
    return out


def metrics_rows(summary: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    kind = summary.get("kind")
    if kind == "arms":
        for arm, a in summary.get("arms", {}).items():
            for acct, st in a["stats"].items():
                row = {"group": "main", "arm": arm, "tier": a.get("tier"), "threshold": a.get("threshold"), "account_id": acct}
                row.update({k: v for k, v in st.items() if not isinstance(v, dict | list)})
                d = summary.get("deltas", {}).get(arm, {}).get(acct)
                if d:
                    for stat, ci in d.items():
                        for k in ("delta", "lo", "hi", "p5", "p95", "p_adj"):
                            if k in ci:
                                row[f"d_{stat}" if k == "delta" else f"d_{stat}_{k}"] = ci.get(k)
                rows.append(row)
        for r in summary.get("sensitivity", {}).get("rows", []):
            rows.append({"group": r.get("group"), **{k: v for k, v in r.items() if k != "group"}})
    elif kind == "stress":
        rows.extend(dict(r) for r in summary.get("table", []))
    elif kind == "sweep":
        for per, g in summary.get("grid", {}).items():
            for cell, accts in g.get("cells", {}).items():
                tier, thr = cell.split("|")
                for acct, st in accts.items():
                    rows.append({"period": per, "tier": tier, "threshold": float(thr), "account_id": acct, **st})
    elif kind == "scenario":
        for c in summary.get("checks", []):
            rows.append({"scenario": c["scenario"], "arm": c["arm"], "check": json.dumps(c["check"], sort_keys=True),
                         "ok": c["ok"], "detail": json.dumps(c["detail"], default=str)})
    return rows


@dataclass
class RunOutput:
    out_dir: Path
    summary: dict[str, Any]
    manifest: dict[str, Any]
    results: list[ArmResult] = field(default_factory=list)

    @property
    def headline(self) -> dict[str, Any]:
        return dict(self.summary.get("headline") or {})


def _now_iso() -> str:
    return ns_to_bj(SystemClock(0).now_ns()).isoformat(timespec="seconds")


def _hist_and_models(cfg: BacktestConfig, base: FullConfig, groups: Sequence[Group]
                     ) -> tuple[HistData | None, dict[str, PathModel]]:
    """The run's data and the path model of every ensemble group (calibrated with the group's ensemble settings,
    cached: groups with the same settings and data share one model)."""
    hist: HistData | None = None
    if cfg.layer in ("A", "B") or cfg.kind == "stress":
        hist = get_hist(cfg, base)
    models = {g.key: path_model_for(g.cfg, base) for g in groups if g.paths > 0}
    return hist, models


def run_config(
    cfg: BacktestConfig,
    out_dir: Path,
    *,
    progress: ProgressFn | None = None,
    config_path: Path | None = None,
    overrides: Mapping[str, Any] | None = None,
) -> RunOutput:
    """Run a resolved config and write every output file into ``out_dir``."""
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    last = [0.0]

    def prog(frac: float, msg: str | None = None) -> None:
        f = min(1.0, max(last[0], float(frac)))
        last[0] = f
        if progress is not None:
            progress(f, msg)

    started = _now_iso()
    code = code_hash()  # the sources this run imports (the pool's workers are spawned now); re-checked at the end
    t0 = wall_s()
    prog(0.0, "loading")
    base = load_base(cfg)
    hist = get_hist(cfg, base) if (cfg.layer in ("A", "B") or cfg.kind == "stress") else None
    windows = resolve_windows(cfg, hist) if cfg.kind == "stress" else []
    groups = groups_for(cfg, windows)
    hist, models = _hist_and_models(cfg, base, groups)
    model = models.get("main") or next(iter(models.values()), None)
    prog(0.02, f"{len(groups)} groups")

    def sub(frac: float, msg: str | None = None) -> None:
        prog(0.02 + 0.86 * frac, msg)

    slices = resolve_windows(cfg, hist) if (cfg.kind == "arms" and cfg.stress) else []
    store = run_experiment(cfg, groups, progress=sub, path_models=models, windows=slices, progress_dir=out_dir / ".progress")
    prog(0.88, "analysis")
    summary, report_extra = summarize(cfg, base, groups, store, hist=hist, model=model, windows=windows)
    wall = wall_s() - t0
    summary["performance"] = performance(store, wall)
    notes = sorted(store.notes)
    summary["notes"] = notes[:200]
    prog(0.93, "writing outputs")
    # ---------------------------------------------------------------- files
    files = dict(store.data_files)
    for g in groups:  # the minute files read by the calibrations (hashed when read)
        if g.paths > 0:
            files.update(get_hist(g.cfg, base).file_hashes())
    dhash, dinfo = data_hash(files, cfg.data_dir())
    hashes = {
        "events.parquet": _write_parquet(_frame(store.events, EVENT_COLUMNS), out_dir / "events.parquet"),
        "states.parquet": _write_parquet(_frame(_rows(store, "states")), out_dir / "states.parquet"),
        "daily.parquet": _write_parquet(_frame(_rows(store, "daily")), out_dir / "daily.parquet"),
        "orders.parquet": _write_parquet(_frame(_rows(store, "orders")), out_dir / "orders.parquet"),
        "actions.parquet": _write_parquet(_frame(_rows(store, "actions")), out_dir / "actions.parquet"),
    }
    if store.compact:
        hashes["ensemble.parquet"] = _write_parquet(pd.concat(store.compact, ignore_index=True), out_dir / "ensemble.parquet")
    mrows = metrics_rows(summary)
    _frame(mrows).to_csv(out_dir / "metrics.csv", index=False, encoding="utf-8-sig")
    hashes["metrics.csv"] = file_sha256(out_dir / "metrics.csv")
    summary_hash = json_sha256(_strip_perf(summary))
    (out_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=1, default=str), encoding="utf-8")
    chash = config_hash(cfg, base_hash(base))
    code_end = code_hash()
    changed = sorted(k for k in set(code[1]) | set(code_end[1]) if code[1].get(k) != code_end[1].get(k))
    manifest: dict[str, Any] = {
        "name": cfg.name,
        "kind": cfg.kind,
        "layer": cfg.layer,
        "config_path": str(config_path) if config_path else None,
        "overrides": dict(overrides or {}),
        "config": cfg.model_dump(mode="json"),
        "config_hash": chash,
        "base_config_hash": base_hash(base),
        "base_config_files": {k: v for k, v in sorted(base.files.items()) if k in _base_file_names()},
        "data_hash": dhash,
        "data": dinfo,
        "seed": cfg.seed,
        "git_sha": git_sha(),
        "code_hash": code[0],
        "code_hash_by_package": code[1],
        # packages whose sources were edited while the run was going (workers had already imported them)
        "code_changed_during_run": changed,
        "versions": versions(),
        "path_model": _model_manifest(model),
        "path_models": {k: _model_manifest(m)["hash"] for k, m in models.items() if m is not None},  # type: ignore[index]
        "groups": [{"key": g.key, "kind": g.kind, "title": g.title, "paths": g.paths, "start": g.start, "end": g.end,
                    "arms": [a.name for a in g.arms]} for g in groups],
        "started_at": started,
        "finished_at": _now_iso(),
        "wall_s": round(wall, 3),
        "runs": store.runs,
        "outputs": hashes,
        "events_hash": hashes["events.parquet"],
        "summary_hash": summary_hash,
        "notes": notes[:200],
    }
    prog(0.95, "report")
    from futrisk.backtest.report import render_report

    main_results = store.det.get("main") or store.first.get("main") or {}
    if cfg.kind == "stress":
        results_all = [r for per in [*store.det.values(), *store.first.values()] for r in per.values()]
    elif cfg.kind == "scenario":
        results_all = [r for k in sorted(store.det) for r in store.det[k].values()]
    else:
        results_all = list(main_results.values())
    html = render_report(cfg, summary, results_all, groups, manifest, extra=report_extra)
    (out_dir / "report.html").write_text(html, encoding="utf-8")
    manifest["outputs"]["report.html"] = file_sha256(out_dir / "report.html")
    (out_dir / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=1, default=str), encoding="utf-8")
    prog(1.0, "done")
    return RunOutput(out_dir, summary, manifest, results_all)


def _base_file_names() -> tuple[str, ...]:
    from futrisk.backtest.config import BASE_FILES

    return BASE_FILES


def _model_manifest(model: PathModel | None) -> dict[str, Any] | None:
    if model is None:
        return None
    return {
        "seed": model.seed, "grid_s": model.grid_s, "sector_rho": dict(model.sector_rho),
        "products": {p: {"p_near_first": v.p_near_first, "sigma": v.sigma, "timing": v.source,
                         "t_first_n": len(v.t_first)} for p, v in model.products.items()},
        "hash": json_sha256({"products": {p: [v.p_near_first, v.sigma, list(v.t_first), list(v.t_gap)]
                                          for p, v in sorted(model.products.items())},
                             "rho": dict(sorted(model.sector_rho.items())), "seed": model.seed, "grid_s": model.grid_s}),
    }


def summarize(cfg: BacktestConfig, base: FullConfig, groups: Sequence[Group], store: RunStore, *,
              hist: HistData | None, model: PathModel | None, windows: Sequence[Mapping[str, Any]]
              ) -> tuple[dict[str, Any], dict[str, Any]]:
    """The run summary per kind (and extra data for the report's charts)."""
    extra: dict[str, Any] = {}
    if cfg.kind == "arms":
        main = next(g for g in groups if g.key == "main")
        hit = hit_rates(hist) if (cfg.analysis.hit_rate and hist is not None) else None
        mult = {p: base.products.products[p].multiplier for p in (hist.series if hist else {})}
        series = hist.series if hist is not None else None
        sw = resolve_windows(cfg, hist) if cfg.stress else []
        gsum: dict[str, dict[str, Any]] = {}
        for g in groups:
            if g.key in store.det:
                gsum[g.key] = analyze_arms(g.cfg, store.det[g.key], series=series if g.key == "main" else None,
                                           multipliers=mult, hit_rates=hit if g.key == "main" else None,
                                           stress_windows=sw if g.key == "main" else (),
                                           bootstrap=g.key == "main" or g.cfg.analysis.bootstrap_samples > 0)
            elif g.key in store.digests:
                gsum[g.key] = aggregate_ensemble(g.cfg, store.digests[g.key], windows=sw if g.key == "main" else ())
                if g.key == "main" and gsum[g.key].get("windows"):
                    gsum[g.key]["stress"] = _window_rows(gsum[g.key]["windows"], sw, len(store.digests[g.key]))
                first = store.first.get(g.key, {})
                if first and g.key == "main":
                    # path-independent parts from the first realization (roll gaps, hit rate)
                    one = analyze_arms(g.cfg, first, series=series, multipliers=mult, hit_rates=hit, bootstrap=False)
                    gsum[g.key]["roll_gaps"] = one.get("roll_gaps")
                    gsum[g.key]["hit_rate"] = one.get("hit_rate")
                    gsum[g.key]["first_path"] = {k: one.get(k) for k in ("episodes", "stress", "checks")}
                    ds = [store.digests[g.key][k] for k in sorted(store.digests[g.key])]
                    sets = sets_of(g.cfg, list(next(iter(first.values())).accounts))
                    extra["curves"] = curves(ds, [*sets, *[a for a in g.cfg.accounts if a not in sets]])
        summary = dict(gsum.get("main", {}))
        summary["kind"] = "arms"
        summary["group"] = "main"
        summary["model"] = {"kind": "ensemble" if main.paths > 0 else "deterministic", "paths": main.paths,
                            "rule": cfg.path.rule, "calibration": model.report if model is not None else None}
        sens_groups = [(g.key, g.title, gsum[g.key]) for g in groups if g.key in gsum]
        summary["sensitivity"] = {"rows": sensitivity_rows(cfg, sens_groups),
                                  "groups": {g.key: {"title": g.title, "paths": g.paths, "layer": g.cfg.layer,
                                                     "start": g.start, "end": g.end, "meta": g.meta} for g in groups},
                                  "variants": {k: _compact_group(v) for k, v in gsum.items() if k != "main"}}
        return summary, extra
    if cfg.kind == "stress":
        per_window: dict[str, dict[str, ArmResult]] = {}
        ens: dict[str, dict[str, Any]] = {}
        for g in groups:
            wid = str(g.meta.get("id"))
            if g.key in store.det:
                per_window[wid] = store.det[g.key]
            elif g.key in store.digests:
                per_window[wid] = store.first.get(g.key, {})
                ens[wid] = aggregate_ensemble(g.cfg, store.digests[g.key], windows=[g.meta])
        summary = analyze_stress(cfg, per_window, windows, ensembles=ens)
        summary["skipped"] = list(cfg.stress.skipped) if cfg.stress else []
        summary["model"] = {"kind": "ensemble" if cfg.ensemble.paths > 0 else "deterministic", "paths": cfg.ensemble.paths,
                            "calibration": model.report if model is not None else None}
        return summary, extra
    if cfg.kind == "sweep":
        periods: dict[str, Any] = {}
        for g in groups:
            per = g.meta["period"]
            if g.key in store.det:
                res = store.det[g.key]
                sets = sets_of(g.cfg, list(next(iter(res.values())).accounts))
                from futrisk.backtest.metrics import arm_summary

                stats = {name: arm_summary(r, sets) for name, r in res.items()}
                barm = g.baseline_arm()
                bname = barm.name if barm is not None else next(iter(stats))
                periods[per] = {"cells": sweep_cells(cfg, stats, bname), "baseline": stats[bname], "baseline_arm": bname,
                                "paths": 0}
            elif g.key in store.digests:
                ds = [store.digests[g.key][k] for k in sorted(store.digests[g.key])]
                bname = ds[0]["baseline"]
                cells = [sweep_cells(cfg, d["stats"], bname) for d in ds]
                q = _nested_q(cells)
                med = {c: {a: {k: (v.get("median") if isinstance(v, dict) else v) for k, v in st.items()}
                           for a, st in accts.items()} for c, accts in q.items()}
                bq = _nested_q([d["stats"][bname] for d in ds])
                periods[per] = {"cells": med, "cells_q": q, "paths": len(ds), "baseline_arm": bname,
                                "baseline": {a: {k: v.get("median") for k, v in st.items()} for a, st in bq.items()}}
        summary = analyze_sweep(cfg, periods)
        summary["model"] = {"kind": "ensemble" if cfg.ensemble.paths > 0 else "deterministic", "paths": cfg.ensemble.paths,
                            "calibration": model.report if model is not None else None}
        return summary, extra
    sc: dict[tuple[int, str], ArmResult] = {}
    for g in groups:
        for name, r in store.det.get(g.key, {}).items():
            sc[(int(g.meta.get("scenario", 0)), name)] = r
    return analyze_scenarios(cfg, sc), extra


def _window_rows(win: Mapping[str, Any], windows: Sequence[Mapping[str, Any]], paths: int) -> list[dict[str, Any]]:
    """Ensemble window statistics (medians, the return's 5-95 % path range) as stress-table rows."""
    rows: list[dict[str, Any]] = []
    for w in windows:
        for name, per in (win.get(w["id"]) or {}).items():
            for acct, st in per.items():
                rows.append({"window": w["id"], "title": w.get("title", w["id"]), "arm": name, "account_id": acct,
                             **{k: (v.get("median") if isinstance(v, dict) else v) for k, v in st.items()},
                             "ret_p5": (st.get("ret") or {}).get("p5"), "ret_p95": (st.get("ret") or {}).get("p95"),
                             "focus_day": w.get("focus_day"), "paths": paths})
    return rows


def _compact_group(s: Mapping[str, Any]) -> dict[str, Any]:
    """A variant's summary without the per-episode lists (they stay in the main group only)."""
    return {k: v for k, v in s.items() if k not in ("episodes", "first_path")}


def _strip_perf(summary: Mapping[str, Any]) -> dict[str, Any]:
    """The summary without wall-clock dependent numbers (for the determinism hash)."""
    out = {k: v for k, v in summary.items() if k != "performance"}

    def strip(arms: Any) -> Any:
        if not isinstance(arms, dict):
            return arms
        return {k: {kk: vv for kk, vv in v.items() if kk != "run"} if isinstance(v, dict) else v for k, v in arms.items()}

    if "arms" in out:
        out["arms"] = strip(out["arms"])
    sens = out.get("sensitivity")
    if isinstance(sens, dict) and isinstance(sens.get("variants"), dict):
        out["sensitivity"] = {**sens, "variants": {k: {**v, "arms": strip(v.get("arms"))} for k, v in sens["variants"].items()}}
    return out


def performance(store: RunStore, wall: float) -> dict[str, Any]:
    p = store.perf
    ticks, states = p.get("ticks", 0.0), p.get("states", 0.0)
    replay, settle, plan = p.get("replay_s", 0.0), p.get("settle_s", 0.0), p.get("plan_s", 0.0)
    days = p.get("days", 0.0)
    return {
        "arm_results": len(store.runs),  # group × realization × arm
        "copied_account_runs": store.copies,  # account runs presented from the baseline (the rule never bound)
        "replayed_account_days": days,
        "ticks": ticks,
        "states": states,
        "orders": p.get("orders", 0.0),
        "replay_s": replay,
        "settle_s": settle,
        "plan_s": plan,
        "strategy_s": p.get("strategy_s", 0.0),
        "wall_s": wall,
        "ticks_per_s_replay": ticks / replay if replay > 0 else None,
        "states_per_s_replay": states / replay if replay > 0 else None,
        "ticks_per_s_end_to_end": ticks / (replay + settle + plan) if (replay + settle + plan) > 0 else None,
        "settle_ms_per_account_day": settle / days * 1000 if days else None,
        "settle_ms_per_day": settle / days * 1000 if days else None,
    }


def hit_rates(hist: HistData) -> dict[str, Any]:
    """Hit rate of the O-near-extreme order rule on every minute file (per product and period)."""
    stats: dict[str, dict[int, HitStats]] = {}
    for p in hist.products:
        for period in (5, 15, 60):
            try:
                bars = hist.minute_bars(p, period)
            except FileNotFoundError:
                continue
            by_day: dict[str, list[tuple[float, float, float, float]]] = {}
            inst_of: dict[str, str | None] = {}
            for b in bars:
                if b.trading_day not in inst_of:
                    inst_of[b.trading_day] = b.instrument_id
                if b.instrument_id == inst_of[b.trading_day]:
                    by_day.setdefault(b.trading_day, []).append((b.open, b.high, b.low, b.close))
            # drop the first (possibly partial) day of the window
            days = sorted(by_day)[1:]
            stats.setdefault(p, {})[period] = order_hit_rate(by_day[d] for d in days)
    return hit_rate_summary(stats)


# ------------------------------------------------------------------ entry points
def default_out_dir(name: str) -> Path:
    stamp = ns_to_bj(SystemClock(0).now_ns()).strftime("%Y%m%dT%H%M%S")
    return resolve_home() / "backtests" / f"{name}-{stamp}"


def run_job(
    config_name: str,
    overrides: dict[str, Any] | None,
    out_dir: str,
    progress: Callable[[float, str | None], None],
) -> dict[str, Any]:
    """Clearing-worker entry point (spawn-safe): run ``config_name`` into ``out_dir``.

    Returns ``summary`` (headline metrics), ``config_hash``, ``data_hash``,
    ``seed``, ``git_sha``, ``report_path`` and ``events_hash``.
    """
    cfg, path = load_backtest_config(config_name, overrides)
    out = run_config(cfg, Path(out_dir), progress=progress, config_path=path, overrides=overrides)
    m = out.manifest
    return {
        "summary": out.headline,
        "config_hash": m["config_hash"],
        "data_hash": m["data_hash"],
        "seed": m["seed"],
        "git_sha": m["git_sha"],
        "report_path": "report.html",
        "events_hash": m["events_hash"],
        "summary_hash": m["summary_hash"],
    }


def finite(x: float | None) -> bool:
    return x is not None and math.isfinite(x)


def np_float(x: Any) -> float:
    return float(np.asarray(x))


__all__ = [
    "ArmTask",
    "RunOutput",
    "build_source",
    "build_strategies",
    "default_out_dir",
    "get_hist",
    "hit_rates",
    "load_base",
    "path_model_for",
    "run_arm_task",
    "run_config",
    "run_job",
]
