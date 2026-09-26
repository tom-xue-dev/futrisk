"""Backtest runner: the arms on a spawn process pool, the outputs, and ``run_job`` for the clearing-worker.

``run_job(config_name, overrides, out_dir, progress)`` is the entry point the
clearing-worker calls in its ``spawn`` process pool: a module-level function
that relies on nothing but its arguments. It loads ``config/backtest/<name>.yaml``
(or a path), applies the dotted overrides, runs every arm (every scenario × arm
for layer D) and writes into ``out_dir``:

- ``report.html`` - self-contained report (inline CSS, a base64 chart);
- ``daily.csv`` - one row per arm, account and trading day (settlement numbers, rule triggers);
- ``events.csv`` - every risk event of every arm; ``orders.csv`` - every order and its result;
- ``summary.json`` - the analysis (:mod:`futrisk.backtest.analysis`); ``manifest.json`` - config hash,
  data hash (+ the files, hashed when read), seed, git SHA, code hash, versions, output hashes.

Every replay is deterministic, so the results do not depend on the number of
workers or on the completion order.
"""

from __future__ import annotations

import json
import multiprocessing
import os
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd

from futrisk.backtest.analysis import analyze_arms, analyze_scenarios
from futrisk.backtest.config import (
    BASE_FILES,
    ArmSpec,
    BacktestConfig,
    app_path,
    base_files_hash,
    config_hash,
    load_backtest_config,
)
from futrisk.backtest.data import HistData
from futrisk.backtest.manifest import code_hash, data_hash, file_sha256, git_sha, json_sha256, versions
from futrisk.backtest.replay import ArmResult, Replay
from futrisk.backtest.sources import DailySource, DataSource, ScenarioSource
from futrisk.backtest.strategies import Strategy, make_strategy
from futrisk.common.clock import SystemClock, ns_to_bj
from futrisk.common.config import FullConfig, load_config_dir
from futrisk.common.paths import bundled_config_dir, resolve_home

ProgressFn = Callable[[float, str | None], None]

# caches of a long-lived process (a clearing-pool worker runs many jobs): validated against the files on disk
_BASE_CACHE: dict[str, tuple[FullConfig, dict[str, tuple[int, int]]]] = {}
_HIST_CACHE: dict[tuple[Any, ...], HistData] = {}


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


def account_plan(cfg: BacktestConfig, base: FullConfig) -> dict[str, tuple[str, list[str], dict[str, Any]]]:
    """``{account: (strategy, products, params)}`` of the enabled accounts (products default to accounts.yaml)."""
    out: dict[str, tuple[str, list[str], dict[str, Any]]] = {}
    for a, run in sorted(cfg.accounts.items()):
        if not run.enabled:
            continue
        spec = base.accounts.accounts.get(a)
        prods = list(run.products) if run.products else (list(spec.products) if spec is not None else [])
        out[a] = (run.strategy, prods, dict(run.params))
    return out


def needed_products(cfg: BacktestConfig, base: FullConfig) -> list[str]:
    want = list(cfg.products)
    for _strat, prods, _params in account_plan(cfg, base).values():
        want += [p for p in prods if p not in want]
    return want


def get_hist(cfg: BacktestConfig, base: FullConfig) -> HistData:
    """The data of ``cfg`` (cached per process; reloaded when a file it read changed on disk)."""
    products = needed_products(cfg, base)
    key = (str(cfg.data_dir()), tuple(products), base_hash(base))
    hist = _HIST_CACHE.get(key)
    if hist is None or hist.stale():
        hist = HistData(cfg.data_dir(), products, base.products)
        _HIST_CACHE[key] = hist
    return hist


def build_source(cfg: BacktestConfig, base: FullConfig, scenario: int | None = None) -> DataSource:
    if cfg.layer == "D":
        if scenario is None:
            raise ValueError("layer D needs a scenario index")
        return ScenarioSource(cfg.scenarios[scenario], base.products, base.calendar)
    hist = get_hist(cfg, base)
    products = {p for _s, prods, _p in account_plan(cfg, base).values() for p in prods}
    return DailySource(hist, cfg, base.calendar, products=products or None)


def build_strategies(cfg: BacktestConfig, base: FullConfig, available: Sequence[str],
                     notes: list[str] | None = None) -> dict[str, Strategy]:
    out: dict[str, Strategy] = {}
    avail = set(available)
    for a, (strat, prods, params) in account_plan(cfg, base).items():
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


# ------------------------------------------------------------------ one task
@dataclass(frozen=True)
class ArmTask:
    """One independent replay: an arm (of one scenario for layer D). Picklable: runs in a spawn worker."""

    cfg: BacktestConfig
    arm: ArmSpec
    scenario: int | None = None

    @property
    def label(self) -> str:
        return self.arm.name if self.scenario is None else f"s{self.scenario}-{self.arm.name}"


def run_arm_task(task: ArmTask, progress: ProgressFn | None = None) -> ArmResult:
    """Run one task (module level: the process-pool entry point)."""
    cfg = task.cfg
    base = load_base(cfg)
    source = build_source(cfg, base, task.scenario)
    notes: list[str] = []
    scenario_account = None
    if cfg.layer == "D":
        assert task.scenario is not None
        scenario_account = cfg.scenarios[task.scenario].account
        strategies: dict[str, Strategy] = {}
    else:
        strategies = build_strategies(cfg, base, source.products(), notes)
    rep = Replay(
        cfg=cfg,
        arm=task.arm,
        source=source,
        base_rules=base.risk_rules,
        base_accounts=base.accounts,
        strategies=strategies,
        label=task.label,
        config_hash=config_hash(cfg, base_hash(base)),
        progress=progress,
        scenario_account=scenario_account,
    )
    res = rep.run()
    res.notes = sorted(set(notes + res.notes))
    res.stats["scenario"] = task.scenario
    res.stats["data_files"] = source.file_hashes()
    return res


def tasks_of(cfg: BacktestConfig) -> list[ArmTask]:
    if cfg.kind == "scenario":
        return [ArmTask(cfg, arm, i) for i in range(len(cfg.scenarios)) for arm in cfg.arms]
    return [ArmTask(cfg, arm) for arm in cfg.arms]


def workers_for(cfg: BacktestConfig, n_tasks: int) -> int:
    if isinstance(cfg.workers, int):
        return max(1, min(cfg.workers, n_tasks))
    cpu = os.cpu_count() or 2
    return max(1, min(n_tasks, cpu - 2, 14))


def run_tasks(cfg: BacktestConfig, tasks: Sequence[ArmTask], progress: ProgressFn) -> list[ArmResult]:
    """Every task, in this process (one worker) or on a spawn process pool; results in task order."""
    n = len(tasks)
    workers = workers_for(cfg, n)
    if workers <= 1:

        def part(k: int) -> ProgressFn:
            return lambda f, msg: progress((k + f) / n, msg)

        return [run_arm_task(t, part(k)) for k, t in enumerate(tasks)]
    results: list[ArmResult | None] = [None] * n
    ctx = multiprocessing.get_context("spawn")
    with ProcessPoolExecutor(max_workers=workers, mp_context=ctx) as pool:
        futures = {pool.submit(run_arm_task, t): k for k, t in enumerate(tasks)}
        for done, fut in enumerate(as_completed(futures), start=1):
            k = futures[fut]
            results[k] = fut.result()
            progress(done / n, f"{tasks[k].label} done")
    return [r for r in results if r is not None]


# ------------------------------------------------------------------ outputs
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


def _write_csv(rows: Sequence[Mapping[str, Any]], path: Path) -> str:
    pd.DataFrame.from_records(list(rows)).to_csv(path, index=False, encoding="utf-8-sig")
    return file_sha256(path)


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
    t0 = SystemClock(0).now_ns()
    prog(0.0, "loading")
    base = load_base(cfg)
    tasks = tasks_of(cfg)
    results = run_tasks(cfg, tasks, lambda f, msg: prog(0.02 + 0.9 * f, msg))
    prog(0.93, "analysis")
    if cfg.kind == "scenario":
        summary = analyze_scenarios(cfg, {(t.scenario or 0, t.arm.name): r for t, r in zip(tasks, results, strict=True)})
    else:
        summary = analyze_arms(cfg, {r.arm: r for r in results})
    notes = sorted({n for r in results for n in r.notes})
    summary["notes"] = notes[:200]
    files: dict[str, str] = {}
    for r in results:
        files.update(r.stats.get("data_files", {}))
    dhash, dinfo = data_hash(files, cfg.data_dir())
    prog(0.95, "writing outputs")
    hashes = {
        "daily.csv": _write_csv([row for r in results for row in r.daily], out_dir / "daily.csv"),
        "events.csv": _write_csv([{"run": r.label, **e} for r in results for e in r.events], out_dir / "events.csv"),
        "orders.csv": _write_csv([row for r in results for row in r.orders], out_dir / "orders.csv"),
    }
    (out_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=1, default=str), encoding="utf-8")
    hashes["summary.json"] = file_sha256(out_dir / "summary.json")
    code_end = code_hash()
    changed = sorted(k for k in set(code[1]) | set(code_end[1]) if code[1].get(k) != code_end[1].get(k))
    manifest: dict[str, Any] = {
        "name": cfg.name,
        "kind": cfg.kind,
        "layer": cfg.layer,
        "config_path": str(config_path) if config_path else None,
        "overrides": dict(overrides or {}),
        "config": cfg.model_dump(mode="json"),
        "config_hash": config_hash(cfg, base_hash(base)),
        "base_config_hash": base_hash(base),
        "base_config_files": {k: v for k, v in sorted(base.files.items()) if k in BASE_FILES},
        "data_hash": dhash,
        "data": dinfo,
        "seed": cfg.seed,
        "git_sha": git_sha(),
        "code_hash": code[0],
        "code_hash_by_package": code[1],
        # packages whose sources were edited while the run was going (workers had already imported them)
        "code_changed_during_run": changed,
        "versions": versions(),
        "arms": [{"name": a.name, "tier": a.tier, "threshold": cfg.threshold_of(a)} for a in cfg.arms],
        "started_at": started,
        "finished_at": _now_iso(),
        "wall_s": round((SystemClock(0).now_ns() - t0) / 1e9, 3),
        "outputs": hashes,
        "events_hash": hashes["events.csv"],
        "summary_hash": json_sha256(summary),
        "notes": notes[:200],
    }
    prog(0.97, "report")
    from futrisk.backtest.report import render_report

    keyed: dict[Any, ArmResult] = ({(t.scenario, t.arm.name): r for t, r in zip(tasks, results, strict=True)}
                                   if cfg.kind == "scenario" else {r.arm: r for r in results})
    (out_dir / "report.html").write_text(render_report(cfg, summary, keyed, manifest), encoding="utf-8")
    manifest["outputs"]["report.html"] = file_sha256(out_dir / "report.html")
    (out_dir / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=1, default=str), encoding="utf-8")
    prog(1.0, "done")
    return RunOutput(out_dir, summary, manifest, results)


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
    ``seed``, ``git_sha``, ``report_path``, ``events_hash`` and ``summary_hash``.
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


__all__ = [
    "ArmTask",
    "RunOutput",
    "account_plan",
    "build_source",
    "build_strategies",
    "default_out_dir",
    "get_hist",
    "load_base",
    "run_arm_task",
    "run_config",
    "run_job",
    "run_tasks",
    "tasks_of",
]
