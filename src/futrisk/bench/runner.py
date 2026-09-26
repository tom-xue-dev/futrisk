"""Run benches, sample the machine load around each one, and write the results (JSON + Markdown).

Results go to ``<home>/bench/<YYYYMMDD-HHMMSS>[-quick].json`` (``<home>`` = ``var/`` in a checkout) with
the machine description, the background load before the run, and per bench its parameters, metrics
(n / p50 / p90 / p99 / max / value), targets, verdict, the CPU load sampled while it ran and the extra
diagnostics (heartbeat windows, start-up breakdown, GC pauses, profiles). :func:`render_markdown`
turns a report into the tables used in the documentation.
"""

from __future__ import annotations

import dataclasses
import importlib
import json
import sys
import traceback
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from futrisk.bench.core import BenchContext, BenchResult, Metric, fmt_value, now_ns
from futrisk.bench.machine import LoadSampler, baseline_load, machine_info

#: bench name -> module (each has NAME, TITLE, TARGET and run(ctx) -> BenchResult)
BENCHES: dict[str, str] = {
    "bus": "futrisk.bench.bus",
    "ingest": "futrisk.bench.ingest",
    "pretrade": "futrisk.bench.pretrade",
    "replay": "futrisk.bench.replay",
    "tick2decision": "futrisk.bench.tick2decision",
    "order_rtt": "futrisk.bench.order_rtt",
    "recovery": "futrisk.bench.recovery",
}
ALL = list(BENCHES)


@dataclass
class Report:
    started_at: str
    finished_at: str = ""
    mode: str = "full"
    profile: bool = False
    command: str = ""
    machine: dict[str, Any] = field(default_factory=dict)
    baseline_load: dict[str, Any] = field(default_factory=dict)
    benches: list[BenchResult] = field(default_factory=list)
    files: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)


def wall_now() -> datetime:
    """Local wall-clock time (for file names and the report header only)."""
    from futrisk.common.clock import SystemClock

    return datetime.fromtimestamp(SystemClock(reanchor_s=0).now_ns() / 1e9).astimezone()


def resolve_names(names: Iterable[str] | None) -> list[str]:
    wanted = [n.strip().lower().replace("-", "_") for n in (names or []) if n.strip()]
    if not wanted or "all" in wanted:
        return list(ALL)
    unknown = [n for n in wanted if n not in BENCHES]
    if unknown:
        raise ValueError(f"unknown bench {', '.join(unknown)}; choose from all, {', '.join(ALL)}")
    return list(dict.fromkeys(wanted))


def run_one(name: str, ctx: BenchContext) -> BenchResult:
    mod = importlib.import_module(BENCHES[name])
    t0 = now_ns()
    with LoadSampler() as sampler:
        try:
            res: BenchResult = mod.run(ctx)
        except Exception as exc:
            res = BenchResult(name=name, title=mod.TITLE, target=mod.TARGET, quick=ctx.quick,
                              error=f"{type(exc).__name__}: {exc}")
            res.extra["traceback"] = traceback.format_exc()
    res.wall_s = round((now_ns() - t0) / 1e9, 2)
    res.load = sampler.summary()
    if res.load.get("loaded"):
        res.notes.append(f"machine under load: other programs used {res.load['background_mean_pct']}% CPU on average "
                         "while this bench ran - the numbers are pessimistic")
    if ctx.profile:
        res.notes.append("profiled run: cProfile overhead is included in the numbers")
    if ctx.mode != "full":
        res.notes.append(f"{ctx.mode} sizes: indicative only")
    res.decide()
    return res


def run_benches(names: Sequence[str], ctx: BenchContext, *, overrides: dict[str, dict[str, Any]] | None = None,
                command: str = "", on_result: Callable[[BenchResult], None] | None = None) -> Report:
    report = Report(started_at=wall_now().isoformat(timespec="seconds"), mode=ctx.mode, profile=ctx.profile,
                    command=command)
    ctx.say("machine info and a 1 s background-load sample")
    report.machine = machine_info()
    report.baseline_load = baseline_load(1.0)
    for name in names:
        bctx = dataclasses.replace(ctx, overrides=dict((overrides or {}).get(name, {})))
        ctx.say(f"=== {name} ({ctx.mode})")
        res = run_one(name, bctx)
        report.benches.append(res)
        if on_result is not None:
            on_result(res)
    report.finished_at = wall_now().isoformat(timespec="seconds")
    return report


def default_json_path(mode: str) -> Path:
    from futrisk.common.paths import resolve_home

    stamp = wall_now().strftime("%Y%m%d-%H%M%S")
    suffix = "" if mode == "full" else f"-{mode}"
    return resolve_home() / "bench" / f"{stamp}{suffix}.json"


def write_json(report: Report, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    report.files["json"] = str(path)
    path.write_text(json.dumps(report.to_dict(), indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    return path


# ---------------------------------------------------------------------------------------------- markdown
def _cell(m: Metric, stat: str) -> str:
    v = m.stat(stat)
    return fmt_value(m, v) if v is not None else ""


def _value(m: Metric) -> str:
    if m.kind == "latency":
        return ""
    if m.kind == "duration":
        return fmt_value(m, m.value) + (f" (n={m.n})" if m.n > 1 else "")
    return fmt_value(m, m.value)


def render_markdown(report: Report) -> str:
    mi = report.machine
    bl = report.baseline_load
    jit = bl.get("jitter") or {}
    lines = [
        "# futrisk benchmark results",
        "",
        f"- **Run**: {report.started_at} → {report.finished_at}, sizes `{report.mode}`"
        + (", profiled" if report.profile else "") + (f", `{report.command}`" if report.command else ""),
        f"- **Machine**: {mi.get('cpu')}, {mi.get('cores_physical')} cores / {mi.get('cores_logical')} threads, "
        f"{mi.get('memory_gb')} GB RAM; {mi.get('os')}; power plan {mi.get('power_plan') or 'n/a'}",
        f"- **Software**: {mi.get('python')}; pyzmq {mi.get('pyzmq')} (libzmq {mi.get('libzmq')}); "
        f"msgspec {mi.get('msgspec')}; SQLite {mi.get('sqlite')}; code {mi.get('git_sha')} / src hash "
        f"{mi.get('code_hash')}",
        f"- **Background load before the run**: {bl.get('system_pct')} % CPU ({bl.get('busy_cpus')} of "
        f"{bl.get('cpus')} logical CPUs > 50 %)" + (" — **machine under load, numbers are pessimistic**"
                                                    if bl.get("loaded") else ""),
        f"- **Scheduler jitter** (one thread spinning for {jit.get('seconds', '?')} s): "
        f"{jit.get('gaps_over_1ms', '?')} gaps > 1 ms, {jit.get('gaps_over_5ms', '?')} > 5 ms, longest "
        f"{jit.get('max_gap_ms', '?')} ms (a quiet machine stays below 1 ms; latency tails cannot be shorter)",
        "- Latencies: nearest-rank percentiles of `perf_counter_ns` samples (one system-wide QPC timeline, so "
        "cross-process latencies subtract timestamps of two processes). Targets are design targets (01 §8.12, "
        "§10.2 QS-07/08/11).",
        "",
        "| Bench | Metric | n | p50 | p90 | p99 | max | Value | Target | Result | CPU bg % |",
        "|---|---|---:|---:|---:|---:|---:|---:|---|---|---:|",
    ]
    for b in report.benches:
        bg = b.load.get("background_mean_pct", "")
        if b.error:
            lines.append(f"| {b.name} | **error**: {b.error.splitlines()[0][:120]} |  |  |  |  |  |  | {b.target} | "
                         f"ERROR | {bg} |")
            continue
        for m in b.metrics:
            verdict: str = m.verdict or ""
            if m.primary and verdict:
                verdict = f"**{verdict}**"
            name = f"**{m.name}**" if m.primary else m.name
            n = f"{m.n:,}" if m.n else ""
            lines.append(f"| {b.name} | {name} | {n} | {_cell(m, 'p50')} | {_cell(m, 'p90')} | {_cell(m, 'p99')} | "
                         f"{_cell(m, 'max')} | {_value(m)} | {m.target_text()} | {verdict} | "
                         f"{bg if m is b.metrics[0] else ''} |")
    lines += ["", "Bold rows are the metric each bench is judged on; other rows are diagnostics.", ""]
    for b in report.benches:
        lines.append(f"## {b.name}: {b.title}")
        lines.append("")
        lines.append(f"- Target: {b.target} → **{b.verdict}**; wall {b.wall_s:.1f} s; load while running: "
                     f"machine {b.load.get('system_mean_pct', '?')} % (max {b.load.get('system_max_pct', '?')} %), "
                     f"this bench {b.load.get('own_mean_pct', '?')} %, others {b.load.get('background_mean_pct', '?')} %")
        if b.params:
            lines.append("- Parameters: " + "; ".join(f"{k} = {v}" for k, v in b.params.items()))
        for m in b.metrics:
            if m.note:
                lines.append(f"- {m.name}: {m.note}")
        for note in b.notes:
            lines.append(f"- Note: {note}")
        if b.name == "recovery":
            for i, br in enumerate(b.extra.get("clean_restarts", []), 1):
                lines.append(f"- Clean restart {i}: " + "; ".join(f"{k} {v:.3f} s" for k, v in br.items()))
            for i, br in enumerate(b.extra.get("crash_restarts", []), 1):
                lines.append(f"- Restart after kill {i}: " + "; ".join(f"{k} {v:.3f} s" for k, v in br.items()))
        if "gc" in b.extra:
            gc = b.extra["gc"]
            lines.append(f"- GC during the bench: {gc.get('collections')}, total pause {gc.get('pause_total_ms')} ms, "
                         f"max {gc.get('pause_max_ms')}")
        for key, gc in b.extra.items():
            if key.startswith("risk_engine_gc") and isinstance(gc, dict):
                lines.append(f"- risk-engine GC while measuring{key[len('risk_engine_gc'):]}: {gc.get('collections')}, "
                             f"total pause {gc.get('pause_total_ms')} ms ({gc.get('paused_pct_of_window')} % of the "
                             f"window), max {gc.get('pause_max_ms')}")
        if b.name == "recovery" and b.extra.get("risk_db"):
            lines.append(f"- risk.db after the load phase: {b.extra['risk_db']}")
        if b.error:
            lines.append(f"- Error: `{b.error}`")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def print_json(report: Report) -> None:
    sys.stdout.write(json.dumps(report.to_dict(), indent=2, ensure_ascii=False, default=str) + "\n")


__all__ = ["ALL", "BENCHES", "Report", "default_json_path", "print_json", "render_markdown", "resolve_names",
           "run_benches", "run_one", "wall_now", "write_json"]
