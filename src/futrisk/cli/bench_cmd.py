"""``futrisk bench [all|bus|ingest|tick2decision|order_rtt|pretrade|replay|recovery ...]`` (01 §8.12, plan §10.4).

Runs the performance benchmarks of the design targets and prints, per bench, p50 / p90 / p99 / max and
throughput with the target and PASS / MISS; the machine (CPU, cores, OS, Python, pyzmq, power plan) and
the CPU load sampled while each bench ran are recorded with the numbers. Results are written to
``<home>/bench/<timestamp>.json``; ``--out FILE`` also writes the Markdown tables for the documentation;
``--json`` prints the JSON report instead of the tables.

Registered by :func:`register` (``futrisk.main.TOPLEVEL``). Imports only typer and the standard library at
import time; the benches load on demand.
"""

from __future__ import annotations

import shlex
import sys
from pathlib import Path
from typing import Any

import typer


def _parse_sets(sets: list[str] | None) -> dict[str, dict[str, Any]]:
    """``bench.key=value`` (value parsed as int / float / bool when possible) -> ``{bench: {key: value}}``."""
    out: dict[str, dict[str, Any]] = {}
    for s in sets or []:
        if "=" not in s or "." not in s.split("=", 1)[0]:
            raise typer.BadParameter(f"--set expects bench.key=value, got {s!r}")
        key, raw = s.split("=", 1)
        bench, name = key.split(".", 1)
        value: Any = raw
        if raw.lower() in ("true", "false"):
            value = raw.lower() == "true"
        else:
            for conv in (int, float):
                try:
                    value = conv(raw)
                    break
                except ValueError:
                    continue
        out.setdefault(bench.replace("-", "_"), {})[name] = value
    return out


def bench(
    names: list[str] = typer.Argument(  # noqa: B008
        None, help="all (default) or any of: bus, ingest, tick2decision, order_rtt, pretrade, replay, recovery"),
    quick: bool = typer.Option(False, "--quick", help="small sizes: a few seconds per bench, indicative only"),
    out: Path | None = typer.Option(None, "--out", help="also write the Markdown tables to FILE"),  # noqa: B008
    as_json: bool = typer.Option(False, "--json", help="print the JSON report to stdout instead of the tables"),
    profile: bool = typer.Option(False, "--profile", help="cProfile the measured sections (slower; see the JSON)"),
    keep: bool = typer.Option(False, "--keep", help="keep the temporary homes, databases and logs"),
    heartbeat_ms: int | None = typer.Option(
        None, "--heartbeat-ms", help="bus heartbeat of the multi-process benches (default: 1000 ms, as in production)"),
    sets: list[str] = typer.Option(  # noqa: B008
        None, "--set", help="size override bench.key=value, e.g. --set bus.n=200000 --set recovery.fills=5000"),
    tiny: bool = typer.Option(False, "--tiny", hidden=True, help="smoke-test sizes (plumbing only)"),
) -> None:
    """Run the performance benchmarks and compare them with the design targets."""
    from loguru import logger
    from rich.console import Console

    from futrisk.bench.core import BenchContext, SizeMode
    from futrisk.bench.runner import (
        default_json_path,
        print_json,
        render_markdown,
        resolve_names,
        run_benches,
        write_json,
    )

    err = Console(stderr=True)
    console = Console()
    try:
        chosen = resolve_names(names)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from None
    mode: SizeMode = "tiny" if tiny else "quick" if quick else "full"
    ctx = BenchContext(mode=mode, profile=profile, keep=keep, heartbeat_ms=heartbeat_ms,
                       echo=lambda s: err.print(f"[dim]{s}[/dim]"))
    argv = sys.argv[sys.argv.index("bench") + 1:] if "bench" in sys.argv else sys.argv[1:]
    command = " ".join(["futrisk bench", *(shlex.quote(a) for a in argv)])

    def show(res: Any) -> None:
        if not as_json:
            _print_result(console, res)

    logger.disable("futrisk")  # code running in this process (ingest's md-gateway, the backtester) stays quiet
    try:
        report = run_benches(chosen, ctx, overrides=_parse_sets(sets), command=command, on_result=show)
    finally:
        logger.enable("futrisk")
    path = write_json(report, default_json_path(mode))
    if out is not None:
        report.files["markdown"] = str(out)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(render_markdown(report), encoding="utf-8")
        write_json(report, path)  # with the Markdown path recorded
    if as_json:
        print_json(report)
        return
    _print_summary(console, report)
    console.print(f"results  {path}" + (f"\nmarkdown {out}" if out is not None else ""))


def _print_result(console: Any, res: Any) -> None:
    from rich.table import Table

    from futrisk.bench.core import fmt_value

    colour = {"PASS": "green", "MISS": "red", "ERROR": "red", "N/A": "yellow"}.get(res.verdict, "white")
    t = Table(title=f"{res.name}: {res.title}", title_justify="left")
    for col in ("metric", "n", "p50", "p90", "p99", "max", "value", "target", "result"):
        t.add_column(col, justify="left" if col in ("metric", "target") else "right")
    for m in res.metrics:
        cells = [fmt_value(m, m.stat(s)) if m.stat(s) is not None else "" for s in ("p50", "p90", "p99", "max")]
        value = "" if m.kind == "latency" else fmt_value(m, m.value)
        verdict = m.verdict or ""
        name = f"[bold]{m.name}[/bold]" if m.primary else m.name
        t.add_row(name, f"{m.n:,}" if m.n else "", *cells, value, m.target_text(), verdict)
    console.print(t)
    load = res.load or {}
    console.print(f"[{colour}]{res.verdict}[/{colour}] target {res.target}; wall {res.wall_s:.1f} s; CPU: machine "
                  f"{load.get('system_mean_pct', '?')} %, this bench {load.get('own_mean_pct', '?')} %, others "
                  f"{load.get('background_mean_pct', '?')} %")
    for note in res.notes:
        console.print(f"  [yellow]note[/yellow] {note}")
    if res.error:
        console.print(f"  [red]error[/red] {res.error}")
    console.print()


def _print_summary(console: Any, report: Any) -> None:
    from rich.table import Table

    from futrisk.bench.core import fmt_value

    mi = report.machine
    console.print(f"machine  {mi.get('cpu')} ({mi.get('cores_physical')}C/{mi.get('cores_logical')}T), {mi.get('os')}, "
                  f"{mi.get('python')}, pyzmq {mi.get('pyzmq')}/libzmq {mi.get('libzmq')}, power plan "
                  f"{mi.get('power_plan') or 'n/a'}; background load before the run {report.baseline_load.get('system_pct')} %")
    jit = report.baseline_load.get("jitter") or {}
    console.print(f"jitter   a spinning thread was descheduled {jit.get('gaps_over_1ms', '?')}× > 1 ms "
                  f"({jit.get('gaps_over_5ms', '?')}× > 5 ms, longest {jit.get('max_gap_ms', '?')} ms) in "
                  f"{jit.get('seconds', '?')} s - latency tails cannot be shorter than that")
    t = Table(title="summary", title_justify="left")
    for col in ("bench", "judged on", "p50", "p99", "value", "target", "result"):
        t.add_column(col)
    for b in report.benches:
        m = b.primary()
        if m is None:
            t.add_row(b.name, b.error or "-", "", "", "", b.target, b.verdict)
            continue
        p50 = fmt_value(m, m.p50) if m.p50 is not None else ""
        p99 = fmt_value(m, m.p99) if m.p99 is not None else ""
        value = "" if m.kind == "latency" else fmt_value(m, m.value)
        t.add_row(b.name, m.name, p50, p99, value, b.target, b.verdict)
    console.print(t)


def register(app: typer.Typer) -> None:
    """Add ``futrisk bench`` to the CLI (``futrisk.main``)."""
    app.command("bench")(bench)


app = typer.Typer(help="Performance benchmarks of the design targets", add_completion=False)
register(app)

if __name__ == "__main__":  # python -m futrisk.cli.bench_cmd ... (before the command is registered in main.py)
    import contextlib

    for _stream in (sys.stdout, sys.stderr):  # as futrisk.main does: µ, ≤, × on a GBK console
        with contextlib.suppress(Exception):
            _stream.reconfigure(encoding="utf-8")  # type: ignore[union-attr]
    app()


__all__ = ["app", "bench", "register"]
