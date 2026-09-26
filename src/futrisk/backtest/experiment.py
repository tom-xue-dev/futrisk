"""The experiment plan: groups × path realizations × arms × accounts, scheduled on a spawn process pool.

**Groups** are the independently analysed parts of a run: the main period and the sensitivity variants
(``kind: arms``), the stress windows (``stress``), the in- and out-of-sample periods (``sweep``). A group
replays either the deterministic path (``paths`` = 0) or K realizations of the path ensemble.

**Tasks** replay one arm for one account (a *lane* = group × realization × account). Accounts are independent
when only the daily-loss rule runs (no rule couples two accounts), so a lane is scheduled in stages:

1. the baseline arms: ``alert_only`` (every threshold of the group) and ``none`` when configured;
2. the acting arms (``block_open`` / ``halt_only`` / ``halve`` / ``flatten``): an account whose ``alert_only``
   run at the same threshold raised no restrict or halt alert behaves exactly like the baseline in every acting
   arm (the rule never binds), so its result is a relabelled copy of the ``alert_only`` run instead of a replay;
3. the placebo arms: the referenced arm's latch days and times of day give the schedule (random other days,
   matched count), then the placebo replays.

When other rules run (``include_other_rules`` or the ``production`` tier) accounts are replayed together and
every arm runs (no copies). Results never depend on the number of workers or the completion order: every task
is deterministic and results are merged in a fixed order.
"""

from __future__ import annotations

import contextlib
import math
import multiprocessing
import os
import shutil
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import FIRST_COMPLETED, Future, ProcessPoolExecutor, wait
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from futrisk.backtest.arms import ACTING_TIERS, DAILY_LOSS, splittable
from futrisk.backtest.config import ArmSpec, BacktestConfig, apply_overrides
from futrisk.backtest.pathmodel import PathModel, rng_for
from futrisk.backtest.replay import ArmResult

ProgressFn = Callable[[float, str | None], None]
BASELINE_TIERS = ("none", "alert_only")


# ------------------------------------------------------------------ groups
@dataclass(frozen=True)
class Group:
    """One independently analysed set of runs."""

    key: str  # main | var:<name> | win:<id> | per:<name>
    kind: str  # main | variant | window | period | scenario
    title: str
    cfg: BacktestConfig
    start: str | None
    end: str | None
    warmup_days: int
    paths: int  # K realizations of the path ensemble (0 = the deterministic path)
    arms: tuple[ArmSpec, ...]
    record_days: tuple[str, ...] = ()
    meta: dict[str, Any] = field(default_factory=dict, hash=False, compare=False)

    @property
    def realizations(self) -> list[int | None]:
        return list(range(self.paths)) if self.paths > 0 else [None]

    def baseline_arm(self) -> ArmSpec | None:
        """``none`` when the group runs it, else ``alert_only`` at the main threshold (identical trading)."""
        for a in self.arms:
            if a.tier == "none":
                return a
        t0 = self.cfg.rules.threshold
        cands = [a for a in self.arms if a.tier == "alert_only"]
        for a in cands:
            if abs(self.cfg.threshold_of(a) - t0) < 1e-12:
                return a
        return cands[0] if cands else None


def variant_config(cfg: BacktestConfig, overrides: Mapping[str, Any], *, paths: int) -> BacktestConfig:
    raw = cfg.model_dump(mode="json")
    raw["sensitivity"] = []
    merged = apply_overrides(raw, dict(overrides))
    merged.setdefault("ensemble", {})
    merged["ensemble"] = {**merged["ensemble"], "paths": paths}
    return BacktestConfig.model_validate(merged)


def record_days_of(cfg: BacktestConfig) -> tuple[str, ...]:
    days = set(cfg.analysis.focus_days)
    if cfg.stress is not None:
        days.update(w.focus_day for w in cfg.stress.windows if w.focus_day)
    return tuple(sorted(days))


def groups_for(cfg: BacktestConfig, windows: Sequence[Mapping[str, Any]] = ()) -> list[Group]:
    """The groups of a run (``windows``: resolved stress windows of a ``stress`` run)."""
    rec = record_days_of(cfg)
    out: list[Group] = []
    if cfg.kind == "arms":
        arms = tuple(cfg.arms)
        if cfg.ensemble.paths > 0 and any(a.tier == "alert_only" for a in arms):
            arms = tuple(a for a in arms if a.tier != "none")  # alert_only is the baseline (identical trading)
        out.append(Group("main", "main", cfg.name, cfg, cfg.start, cfg.end, cfg.warmup_days, cfg.ensemble.paths, arms, rec))
        for v in cfg.sensitivity:
            vcfg = variant_config(cfg, v.set, paths=int(v.paths or 0))
            names = set(v.arms) if v.arms is not None else {a.name for a in vcfg.arms if a.tier != "placebo"}
            varms = tuple(a for a in vcfg.arms if a.name in names)
            if vcfg.ensemble.paths > 0 and any(a.tier == "alert_only" for a in varms):
                varms = tuple(a for a in varms if a.tier != "none")
            out.append(Group(f"var:{v.name}", "variant", v.title or v.name, vcfg, vcfg.start, vcfg.end, vcfg.warmup_days,
                             vcfg.ensemble.paths if vcfg.layer == "A" else 0, varms, (),
                             {"name": v.name, "set": dict(v.set)}))
    elif cfg.kind == "stress":
        wu = cfg.stress.warmup_days if cfg.stress is not None else cfg.warmup_days
        arms = tuple(cfg.arms)
        if cfg.ensemble.paths > 0 and any(a.tier == "alert_only" for a in arms):
            arms = tuple(a for a in arms if a.tier != "none")
        for w in windows:
            wrec = tuple(sorted(set(rec) | ({w["focus_day"]} if w.get("focus_day") else set())))
            out.append(Group(f"win:{w['id']}", "window", str(w.get("title", w["id"])), cfg, w["start"], w["end"], wu,
                             cfg.ensemble.paths, arms, wrec, dict(w)))
    elif cfg.kind == "sweep":
        sw = cfg.sweep
        if sw is None:
            raise ValueError("kind=sweep needs a sweep section")
        cells: list[ArmSpec] = []
        for tier in sw.tiers:
            for thr in sw.thresholds:
                cells.append(ArmSpec(name=f"{tier}|{thr}", tier=tier, threshold=thr))
        if not any(a.tier == "alert_only" for a in cells):
            cells.insert(0, ArmSpec(name="none|0", tier="none"))
        for per, rng in sw.periods.items():
            start, end = rng[0], rng[1] if len(rng) > 1 else None
            out.append(Group(f"per:{per}", "period", per, cfg, start, end, sw.warmup_days, cfg.ensemble.paths,
                             tuple(cells), (), {"period": per}))
    elif cfg.kind == "scenario":
        for i, spec in enumerate(cfg.scenarios):
            out.append(Group(f"sc:{i}", "scenario", spec.name, cfg, None, None, 0, 0, tuple(cfg.arms),
                             (spec.trading_day,), {"scenario": i}))
    return out


# ------------------------------------------------------------------ tasks
@dataclass(frozen=True)
class ArmTask:
    """One independent replay (picklable: runs in a spawn worker)."""

    cfg: BacktestConfig
    arm: ArmSpec
    start: str | None
    end: str | None
    label: str
    group: str = ""
    record_days: tuple[str, ...] = ()
    scenario: int | None = None
    warmup_days: int = 0
    progress_file: str | None = None
    realization: int | None = None
    accounts: tuple[str, ...] | None = None  # None: every enabled account
    placebo: tuple[tuple[str, tuple[tuple[str, float], ...]], ...] = ()  # account -> ((trading day, day fraction), ...)
    path_model: PathModel | None = None
    record_orders: bool = True

    def placebo_map(self) -> dict[str, dict[str, float]]:
        return {a: dict(days) for a, days in self.placebo}


@dataclass
class Lane:
    """The runs of one account (or of all accounts together) in one group × realization."""

    group: Group
    realization: int | None
    account: str | None
    stage: int = 0
    pending: int = 0
    results: dict[str, ArmResult] = field(default_factory=dict)
    copied: dict[str, str] = field(default_factory=dict)  # arm -> the arm whose result was copied

    @property
    def key(self) -> tuple[str, int | None, str | None]:
        return (self.group.key, self.realization, self.account)


def _label(g: Group, r: int | None, arm: ArmSpec, account: str | None) -> str:
    parts = [g.key.replace(":", "-"), f"r{r}" if r is not None else "d", arm.name.replace("|", "-")]
    if account:
        parts.append(account)
    return "-".join(parts)


def triggered(result: ArmResult, account: str) -> bool:
    """Did the account raise a restrict or halt alert of the daily-loss rule in this run?"""
    return any(e["account_id"] == account and e["kind"] == "ALERT_RAISED" and e["rule_id"] == DAILY_LOSS
               and e["level"] in ("restrict", "halt") for e in result.events)


def relabel(src: ArmResult, arm: ArmSpec, cfg: BacktestConfig, label: str) -> ArmResult:
    """A copy of ``src`` presented as a run of ``arm`` (for an account the arm's rule never binds)."""
    thr = cfg.threshold_of(arm)
    upd = {"arm": arm.name, "tier": arm.tier, "threshold": thr}
    out = ArmResult(arm=arm.name, tier=arm.tier, threshold=thr, label=label, layer=src.layer,
                    accounts=list(src.accounts), capital=dict(src.capital), days=list(src.days))
    out.daily = [{**r, **upd} for r in src.daily]
    out.events = [{**e, "arm": arm.name, "tier": arm.tier, "threshold": thr} for e in src.events]
    out.states = [{**s, "arm": arm.name} for s in src.states]
    out.orders = [{**o, "arm": arm.name} for o in src.orders]
    out.actions = [{**a, "arm": arm.name} for a in src.actions]
    out.intraday = dict(src.intraday)
    out.scripted = list(src.scripted)
    # a copy replays nothing: no ticks / states / timings are attributed to it
    out.stats = {"days": src.stats.get("days", len(src.days)), "copied_from": src.arm,
                 "data_files": dict(src.stats.get("data_files", {})), "group": src.stats.get("group")}
    out.notes = list(src.notes)
    return out


def merge_results(parts: Sequence[ArmResult]) -> ArmResult:
    """One arm's per-account results as one result (rows in a fixed order)."""
    if len(parts) == 1:
        return parts[0]
    first = parts[0]
    out = ArmResult(arm=first.arm, tier=first.tier, threshold=first.threshold, label=first.label, layer=first.layer,
                    accounts=sorted({a for p in parts for a in p.accounts}),
                    capital={a: c for p in parts for a, c in p.capital.items()}, days=list(first.days))
    for p in parts:
        if p.days != first.days:
            raise ValueError(f"arm {first.arm}: the account runs replayed different days")
    out.daily = sorted((r for p in parts for r in p.daily), key=lambda r: (r["trading_day"], r["account_id"]))
    out.events = [e for _k, e in sorted(((e["ts_ns"], e.get("account_id") or "", i, j), e)
                                        for j, p in enumerate(parts) for i, e in enumerate(p.events))]
    out.states = [s for _k, s in sorted(((s["ts_ns"], s["account_id"], i, j), s)
                                        for j, p in enumerate(parts) for i, s in enumerate(p.states))]
    out.orders = [o for p in parts for o in p.orders]
    out.orders.sort(key=lambda o: (o["trading_day"], o["ts_ns"], o["account_id"]))
    out.actions = [a for p in parts for a in p.actions]
    out.actions.sort(key=lambda a: (a["trading_day"], a["ts_ns"], a["account_id"]))
    for p in parts:
        out.intraday.update(p.intraday)
        out.scripted.extend(p.scripted)
    stats: dict[str, Any] = {}
    for p in parts:
        for k, v in p.stats.items():
            if k in ("days", "copied_from", "group"):
                continue
            if isinstance(v, int | float) and not isinstance(v, bool):
                stats[k] = stats.get(k, 0) + v
            elif isinstance(v, dict) and k == "broker":
                d = stats.setdefault(k, {})
                for kk, vv in v.items():
                    d[kk] = d.get(kk, 0) + vv
            elif isinstance(v, dict):
                d = stats.setdefault(k, {})
                if isinstance(d, dict):
                    d.update(v)
            elif isinstance(v, list):
                lst = stats.setdefault(k, [])
                if isinstance(lst, list):
                    lst.extend(v)
            else:
                stats.setdefault(k, v)
    stats["days"] = len(first.days)
    stats["group"] = first.stats.get("group")
    stats["copied_accounts"] = sorted(a for p in parts if p.stats.get("copied_from") for a in p.accounts)
    out.stats = stats
    out.notes = sorted({n for p in parts for n in p.notes})
    return out


def placebo_schedule(ref: ArmResult, alert: ArmResult | None, account: str, *, seed: int, key: str
                     ) -> list[tuple[str, float]]:
    """``[(trading day, fraction of the account's trading day)]``: as many random days as ``ref`` latched the
    breaker for ``account``, among the days it was active and neither latched nor triggered a halt, at the
    latches' own times of day (randomly permuted)."""
    rows = [r for r in ref.daily if r["account_id"] == account]
    fracs: list[float] = []
    latched: set[str] = set()
    for r in rows:
        if not r.get("halt_latched"):
            continue
        latched.add(r["trading_day"])
        f0, f1, lt = r.get("first_ts"), r.get("last_ts"), r.get("latch_ts")
        if f0 is not None and f1 is not None and lt is not None and f1 > f0:
            fracs.append(min(1.0, max(0.0, (lt - f0) / (f1 - f0))))
        else:
            fracs.append(0.5)
    trig = set(latched)
    if alert is not None:
        trig |= {r["trading_day"] for r in alert.daily if r["account_id"] == account and r.get("trig_halt")}
    eligible = sorted(r["trading_day"] for r in rows if r.get("active", True) and r["trading_day"] not in trig)
    n = min(len(fracs), len(eligible))
    if n == 0:
        return []
    rng = rng_for(seed, "placebo", key, account)
    pick = sorted(int(i) for i in rng.choice(len(eligible), size=n, replace=False))
    perm = [fracs[int(i)] for i in rng.permutation(len(fracs))[:n]]
    return [(eligible[i], perm[j]) for j, i in enumerate(pick)]


# ------------------------------------------------------------------ scheduler
class Scheduler:
    """Stage-by-stage lanes on a pool (see the module docstring)."""

    def __init__(self, groups: Sequence[Group], *, run_task: Callable[[ArmTask], ArmResult], workers: int,
                 progress: ProgressFn | None, path_models: Mapping[str, PathModel],
                 on_realization: Callable[[Group, int | None, dict[str, ArmResult]], None],
                 progress_dir: Path | None = None) -> None:
        self.groups = list(groups)
        self.run_task = run_task
        self.workers = workers
        self.progress = progress
        self.path_models = dict(path_models)  # group key -> the group's path model (ensemble groups)
        self.on_realization = on_realization
        self.progress_dir = progress_dir
        self.lanes: list[Lane] = []
        self.done_tasks = 0
        self.total_est = 0
        self.copies = 0
        for g in self.groups:
            split = splittable(g.cfg) and g.kind != "scenario"
            accounts = [a for a, r in sorted(g.cfg.accounts.items()) if r.enabled] if split else [None]
            for r in g.realizations:
                for a in accounts:
                    self.lanes.append(Lane(g, r, a))
        self.by_real: dict[tuple[str, int | None], list[Lane]] = {}
        for ln in self.lanes:
            self.by_real.setdefault((ln.group.key, ln.realization), []).append(ln)
        for ln in self.lanes:
            self.total_est += len(ln.group.arms)

    # ---------------------------------------------------------- stage logic
    def _arms_of_stage(self, lane: Lane, stage: int) -> list[ArmSpec]:
        g = lane.group
        split = lane.account is not None
        if not split:
            if stage == 1:
                return [a for a in g.arms if a.tier != "placebo"]
            if stage == 2:
                return [a for a in g.arms if a.tier == "placebo"]
            return []
        if stage == 1:
            return [a for a in g.arms if a.tier in BASELINE_TIERS]
        if stage == 2:
            return [a for a in g.arms if a.tier not in BASELINE_TIERS and a.tier != "placebo"]
        if stage == 3:
            return [a for a in g.arms if a.tier == "placebo"]
        return []

    def _task(self, lane: Lane, arm: ArmSpec, placebo: Sequence[tuple[str, float]] = ()) -> ArmTask:
        g = lane.group
        r = lane.realization
        return ArmTask(
            cfg=g.cfg, arm=arm, start=g.start, end=g.end, label=_label(g, r, arm, lane.account), group=g.key,
            record_days=g.record_days if (r is None or r == 0) else (), scenario=g.meta.get("scenario"),
            warmup_days=g.warmup_days, realization=r,
            accounts=(lane.account,) if lane.account is not None else None,
            placebo=((lane.account, tuple(placebo)),) if placebo and lane.account is not None else (),
            path_model=self.path_models.get(g.key) if r is not None else None,
            record_orders=r is None or r == 0,
        )

    def _alert_for(self, lane: Lane, thr: float) -> ArmResult | None:
        for a in lane.group.arms:
            if a.tier == "alert_only" and abs(lane.group.cfg.threshold_of(a) - thr) < 1e-12 and a.name in lane.results:
                return lane.results[a.name]
        return None

    def _baseline_result(self, lane: Lane) -> ArmResult | None:
        b = lane.group.baseline_arm()
        return lane.results.get(b.name) if b is not None else None

    def _plan_stage(self, lane: Lane) -> list[ArmTask]:
        """Advance ``lane`` to its next stage with work; returns the tasks to submit (copies are made here)."""
        g = lane.group
        while lane.stage < 3:
            lane.stage += 1
            arms = self._arms_of_stage(lane, lane.stage)
            tasks: list[ArmTask] = []
            for arm in arms:
                acct = lane.account
                if acct is not None and arm.tier in ACTING_TIERS:
                    alert = self._alert_for(lane, g.cfg.threshold_of(arm))
                    if alert is not None and not triggered(alert, acct):
                        lane.results[arm.name] = relabel(alert, arm, g.cfg, _label(g, lane.realization, arm, acct))
                        lane.copied[arm.name] = alert.arm
                        self.copies += 1
                        continue
                if arm.tier == "placebo":
                    ref = lane.results.get(arm.placebo_of or "")
                    if ref is None:
                        continue
                    alert = self._alert_for(lane, g.cfg.threshold_of(g.cfg.arm(arm.placebo_of or "")))
                    key = f"{g.key}|{lane.realization}|{arm.name}"
                    if acct is None:
                        # accounts replayed together: one task carrying every account's schedule
                        full = tuple((a, tuple(s)) for a in ref.accounts
                                     if (s := placebo_schedule(ref, alert, a, seed=g.cfg.seed, key=key)))
                        t = self._task(lane, arm)
                        tasks.append(ArmTask(**{**t.__dict__, "placebo": full}))
                        continue
                    sched = placebo_schedule(ref, alert, acct, seed=g.cfg.seed, key=key)
                    base = self._baseline_result(lane)
                    if not sched and base is not None:
                        lane.results[arm.name] = relabel(base, arm, g.cfg, _label(g, lane.realization, arm, acct))
                        lane.copied[arm.name] = base.arm
                        self.copies += 1
                        continue
                    tasks.append(self._task(lane, arm, sched))
                    continue
                tasks.append(self._task(lane, arm))
            if tasks:
                return tasks
        return []

    # ---------------------------------------------------------- driver
    def run(self) -> None:
        ready: list[tuple[Lane, ArmTask]] = []
        for lane in self.lanes:
            for t in self._plan_stage(lane):
                ready.append((lane, t))
                lane.pending += 1
        for key in list(self.by_real):
            self._maybe_complete(key)
        if self.workers <= 1:
            while ready:
                lane, t = ready.pop(0)
                res = self.run_task(t)
                ready.extend(self._finish(lane, t, res))
            return
        ctx = multiprocessing.get_context("spawn")
        with ProcessPoolExecutor(max_workers=self.workers, mp_context=ctx) as pool:
            futs: dict[Future[ArmResult], tuple[Lane, ArmTask]] = {}

            def submit(items: list[tuple[Lane, ArmTask]]) -> None:
                for lane, t in items:
                    if self.progress_dir is not None:
                        t = ArmTask(**{**t.__dict__, "progress_file": str(self.progress_dir / f"{t.label}.txt")})
                    futs[pool.submit(self.run_task, t)] = (lane, t)

            submit(ready)
            ready = []
            while futs:
                done, _pending = wait(list(futs), timeout=1.0, return_when=FIRST_COMPLETED)
                for f in sorted(done, key=lambda x: futs[x][1].label):
                    lane, t = futs.pop(f)
                    submit(self._finish(lane, t, f.result()))
                self._report(futs)
        if self.progress_dir is not None:
            shutil.rmtree(self.progress_dir, ignore_errors=True)

    def _finish(self, lane: Lane, task: ArmTask, res: ArmResult) -> list[tuple[Lane, ArmTask]]:
        lane.results[task.arm.name] = res
        lane.pending -= 1
        self.done_tasks += 1
        out: list[tuple[Lane, ArmTask]] = []
        if lane.pending == 0:
            for t in self._plan_stage(lane):
                out.append((lane, t))
                lane.pending += 1
            if lane.pending == 0:
                self._maybe_complete((lane.group.key, lane.realization))
        if self.workers <= 1:
            self._report({})
        return out

    def _maybe_complete(self, key: tuple[str, int | None]) -> None:
        lanes = self.by_real.get(key)
        if not lanes or any(ln.pending > 0 or ln.stage < 3 for ln in lanes):
            return
        g = lanes[0].group
        per_arm: dict[str, ArmResult] = {}
        for arm in g.arms:
            parts = [ln.results[arm.name] for ln in lanes if arm.name in ln.results]
            if parts:
                per_arm[arm.name] = merge_results(parts)
        del self.by_real[key]
        for ln in lanes:
            ln.results = {}
        self.on_realization(g, key[1], per_arm)

    def _report(self, futs: Mapping[Future[ArmResult], tuple[Lane, ArmTask]]) -> None:
        if self.progress is None:
            return
        running = 0.0
        for _f, (_lane, t) in futs.items():
            if t.progress_file:
                with contextlib.suppress(OSError, ValueError):
                    running += float(Path(t.progress_file).read_text(encoding="utf-8"))
        total = max(self.total_est - self.copies, self.done_tasks + len(futs), 1)
        self.progress((self.done_tasks + running) / total, f"{self.done_tasks}/{total} runs done")


def workers_for(cfg: BacktestConfig, n_hint: int) -> int:
    if isinstance(cfg.workers, int):
        return max(1, cfg.workers)
    cpu = os.cpu_count() or 2
    return max(1, min(n_hint, cpu - 2, 14))


def estimate_tasks(groups: Sequence[Group]) -> int:
    n = 0
    for g in groups:
        k = max(1, g.paths)
        accounts = sum(1 for r in g.cfg.accounts.values() if r.enabled) if splittable(g.cfg) else 1
        n += k * accounts * len(g.arms)
    return n


def ceil_div(a: int, b: int) -> int:
    return -(-a // b) if b else 0


__all__ = [
    "ArmTask",
    "BASELINE_TIERS",
    "Group",
    "Lane",
    "Scheduler",
    "ceil_div",
    "estimate_tasks",
    "groups_for",
    "math",
    "merge_results",
    "placebo_schedule",
    "record_days_of",
    "relabel",
    "triggered",
    "variant_config",
    "workers_for",
]
