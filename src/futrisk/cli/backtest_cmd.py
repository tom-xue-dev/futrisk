"""``futrisk backtest ...``: run and inspect backtests (01 §8.16).

Commands:

- ``run <config> [--set key=value ...] [--out DIR] [--workers N]``: run ``config/backtest/<config>.yaml``
  (or a YAML path) and write the report, daily rows, events, orders, summary and manifest into
  ``--out`` (default ``<home>/backtests/<name>-<timestamp>``);
- ``list``: the bundled configs and the runs under ``<home>/backtests``;
- ``show <run>``: the manifest and headline numbers of a run (directory or name).
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import typer
from rich.console import Console
from rich.table import Table

app = typer.Typer(help="Backtests: the daily-loss rule against no rules on historical / synthetic data", no_args_is_help=True)
console = Console()


def _overrides(sets: list[str] | None) -> dict[str, Any]:
    from futrisk.backtest.config import parse_override

    out: dict[str, Any] = {}
    for s in sets or []:
        k, v = parse_override(s)
        out[k] = v
    return out


def _progress() -> Any:
    state = {"last": -1.0, "t": time.perf_counter()}

    def cb(frac: float, msg: str | None = None) -> None:
        now = time.perf_counter()
        if frac >= 1.0 or frac - state["last"] >= 0.05 or now - state["t"] > 15:
            state["last"], state["t"] = frac, now
            console.print(f"[dim]{frac * 100:5.1f}%[/dim] {msg or ''}")

    return cb


def _pct(x: Any) -> str:
    return "—" if x is None else f"{float(x) * 100:.2f}%"


def _pp(x: Any) -> str:
    return "—" if x is None else f"{float(x) * 100:+.2f}pp"


def _print_headline(summary: dict[str, Any]) -> None:
    kind = summary.get("kind")
    if kind == "arms":
        h = summary.get("headline", {})
        arms = {k: v for k, v in h.items() if isinstance(v, dict)}
        t = Table(title=f"TOTAL, {h.get('days')} trading days")
        for c in ("arm", "total_ret", "max_dd", "worst_day", "latched days"):
            t.add_column(c, justify="right" if c != "arm" else "left")
        for arm, st in arms.items():
            t.add_row(arm, _pct(st.get("total_ret")), _pct(st.get("max_dd")), _pct(st.get("worst_day")),
                      str(st.get("latched_days")))
        console.print(t)
        for arm, st in arms.items():
            if "rule_days" in st:
                console.print(f"{arm} vs {h.get('baseline')}: the rule acted on {st['rule_days']} account-days, "
                              f"same-day effect {_pp(st['effect_sum'])}, next-day effect {_pp(st['next_effect_sum'])}")
    elif kind == "scenario":
        h = summary.get("headline", {})
        ok = h.get("all_passed")
        console.print(f"scenario checks: {h.get('passed')}/{h.get('checks')} passed" + (" [green]OK[/green]" if ok else " [red]FAILED[/red]"))
        for c in summary.get("checks", []):
            if not c["ok"]:
                console.print(f"  [red]FAIL[/red] {c['scenario']} {c['arm']} {c['check']} -> {c['detail']}")


@app.command("run")
def run_cmd(
    config: str = typer.Argument(..., help="config name under config/backtest, or a YAML path"),
    sets: list[str] = typer.Option(None, "--set", help="override, e.g. --set rules.threshold=0.025"),  # noqa: B008
    out: Path | None = typer.Option(None, "--out", help="output directory"),  # noqa: B008
    workers: int | None = typer.Option(None, "--workers", help="process-pool size (default: config)"),
) -> None:
    """Run a backtest config and write report.html, daily.csv, events.csv, orders.csv, summary.json, manifest.json."""
    from futrisk.backtest.config import load_backtest_config
    from futrisk.backtest.runner import default_out_dir, run_config

    ov = _overrides(sets)
    if workers is not None:
        ov["workers"] = workers
    cfg, path = load_backtest_config(config, ov)
    out_dir = out or default_out_dir(cfg.name)
    console.print(f"backtest [bold]{cfg.name}[/bold] ({cfg.kind}, layer {cfg.layer}) -> {out_dir}")
    res = run_config(cfg, out_dir, progress=_progress(), config_path=path, overrides=ov)
    _print_headline(res.summary)
    m = res.manifest
    console.print(f"report   {out_dir / 'report.html'}")
    console.print(f"hashes   config={m['config_hash'][:16]} data={m['data_hash'][:16]} events={m['events_hash'][:16]} "
                  f"summary={m['summary_hash'][:16]} git={m['git_sha']}")
    console.print(f"wall     {m['wall_s']:.1f}s")
    if res.summary.get("kind") == "scenario" and res.summary.get("passed") is False:
        raise typer.Exit(code=2)


@app.command("list")
def list_cmd(limit: int = typer.Option(20, help="runs to show")) -> None:
    """List the bundled configs and the latest runs under <home>/backtests."""
    from futrisk.backtest.config import list_configs
    from futrisk.common.paths import resolve_home

    t = Table(title="configs (config/backtest)")
    t.add_column("name")
    t.add_column("description")
    for name, desc, _p in list_configs():
        t.add_row(name, desc[:100])
    console.print(t)
    root = resolve_home() / "backtests"
    runs = sorted((p for p in root.glob("*/manifest.json")), key=lambda p: p.stat().st_mtime, reverse=True)[:limit]
    r = Table(title=f"runs ({root})")
    for c in ("run", "kind", "layer", "finished", "events_hash"):
        r.add_column(c)
    for mp in runs:
        try:
            m = json.loads(mp.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        r.add_row(mp.parent.name, str(m.get("kind")), str(m.get("layer")), str(m.get("finished_at")), str(m.get("events_hash", ""))[:16])
    console.print(r)


@app.command("show")
def show_cmd(run: str = typer.Argument(..., help="run directory, or a run name under <home>/backtests")) -> None:
    """Show the manifest and the headline numbers of a run."""
    from futrisk.common.paths import resolve_home

    p = Path(run)
    if not (p / "manifest.json").exists():
        p = resolve_home() / "backtests" / run
    mp = p / "manifest.json"
    if not mp.exists():
        console.print(f"[red]no manifest.json in {p}[/red]")
        raise typer.Exit(code=1)
    m = json.loads(mp.read_text(encoding="utf-8"))
    console.print(f"[bold]{m.get('name')}[/bold] kind={m.get('kind')} layer={m.get('layer')} finished={m.get('finished_at')}")
    for k in ("config_hash", "data_hash", "events_hash", "summary_hash", "seed", "git_sha", "wall_s"):
        console.print(f"  {k:13s} {m.get(k)}")
    sp = p / "summary.json"
    if sp.exists():
        _print_headline(json.loads(sp.read_text(encoding="utf-8")))
    console.print(f"  report        {p / 'report.html'}")
