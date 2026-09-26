"""futrisk CLI entry point (01 §7.1).

Sub-command groups live in ``futrisk.cli.*`` modules and register themselves in
:data:`SUBAPPS`; each module exposes a ``app: typer.Typer``. Missing optional
modules are skipped so partial builds still start.

Usage::

    futrisk up | down | ps | logs <instance> | kill <instance>
    futrisk svc <service> [--shard N]
    futrisk demo [--scenario breaker]
    futrisk data record
    futrisk config validate
"""

from __future__ import annotations

import importlib
import multiprocessing
import os
import sys

import typer

from futrisk import __version__

app = typer.Typer(help="futrisk - multi-product futures risk control and clearing", no_args_is_help=True)

# (module, sub-command name) pairs; modules are optional during development.
SUBAPPS: list[tuple[str, str]] = [
    ("futrisk.cli.data_cmd", "data"),
    ("futrisk.cli.config_cmd", "config"),
    ("futrisk.cli.svc_cmd", "svc"),
    ("futrisk.cli.backtest_cmd", "backtest"),
]

# modules adding top-level commands (up/down/ps/logs/kill/demo/...) via register(app)
TOPLEVEL: list[str] = [
    "futrisk.cli.ops_cmd",
    "futrisk.cli.tools_cmd",
    "futrisk.cli.bench_cmd",
]


def _load_subapps(argv: list[str] | None = None) -> None:
    # `futrisk svc <name>` (every service process the supervisor spawns) needs only the svc
    # sub-app: skip the other CLI modules and their imports (pandas, httpx, rich tables, ...)
    # so that a service starts, and restarts after a crash, as fast as possible
    args = [a for a in (sys.argv[1:] if argv is None else argv) if not a.startswith("-")]
    if args[:1] == ["svc"]:
        wanted = [(m, n) for m, n in SUBAPPS if n == "svc"]
        for module, name in wanted:
            app.add_typer(importlib.import_module(module).app, name=name)
        return
    for module, name in SUBAPPS:
        try:
            mod = importlib.import_module(module)
        except ModuleNotFoundError as exc:
            if exc.name == module:
                continue
            raise
        app.add_typer(mod.app, name=name)
    for module in TOPLEVEL:
        try:
            mod = importlib.import_module(module)
        except ModuleNotFoundError as exc:
            if exc.name == module:
                continue
            raise
        mod.register(app)


@app.command()
def version() -> None:
    """Print the version."""
    typer.echo(f"futrisk {__version__}")


def main() -> None:
    # frozen exe: spawned process-pool workers (clearing-worker, backtests) re-enter here with
    # --multiprocessing-fork arguments; freeze_support() runs the worker and exits (no-op otherwise)
    multiprocessing.freeze_support()
    os.environ.setdefault("PYTHONUTF8", "1")
    if sys.platform == "win32":
        try:
            sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[union-attr]
            sys.stderr.reconfigure(encoding="utf-8")  # type: ignore[union-attr]
        except Exception:
            pass
    _load_subapps()
    app()


if __name__ == "__main__":
    main()
