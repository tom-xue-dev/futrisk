"""``futrisk svc <service>``: run one service process in the foreground (01 §7.1).

Every service package exposes ``main.run(**opts) -> int`` with keyword options:
``home`` (str | None), ``instance`` (str | None), ``shard`` (int | None),
``parent_pid`` (int | None) and service-specific extras. The supervisor spawns
``[python, -m, futrisk.main, svc, <name>, --home, ..., --parent-pid, ...]``
(or ``[futrisk.exe, svc, ...]`` when frozen).
"""

from __future__ import annotations

import importlib
from collections.abc import Callable
from typing import Any

import typer

app = typer.Typer(help="Run a single service in the foreground", no_args_is_help=True)

# service name -> (module, function)
SERVICES: dict[str, tuple[str, str]] = {
    "md-gateway": ("futrisk.services.md_gateway.main", "run"),
    "risk-engine": ("futrisk.services.risk_engine.main", "run"),
    "trade-gateway": ("futrisk.services.trade_gateway.main", "run"),
    "clearing-worker": ("futrisk.services.clearing.main", "run"),
    "api-gateway": ("futrisk.services.api_gateway.main", "run"),
    "supervisor": ("futrisk.services.supervisor.main", "run"),
    "sim-exchange": ("futrisk.services.supervisor.launch_sim", "run"),
    "strategy-sim": ("futrisk.sim.bots.main", "run"),
}


def resolve(name: str) -> Callable[..., int]:
    try:
        module, func = SERVICES[name]
    except KeyError:
        raise typer.BadParameter(f"unknown service {name!r}; choose from {', '.join(SERVICES)}") from None
    mod = importlib.import_module(module)
    fn: Callable[..., int] = getattr(mod, func)
    return fn


def _make_command(name: str) -> None:
    def command(
        home: str | None = typer.Option(None, "--home", help="Runtime home directory"),
        instance: str | None = typer.Option(None, "--instance", help="Instance id (default derived from the service)"),
        shard: int | None = typer.Option(None, "--shard", help="risk-engine shard number"),
        parent_pid: int | None = typer.Option(None, "--parent-pid", help="Exit when this process dies"),
        clock_offset_ns: int | None = typer.Option(None, "--clock-offset-ns", help="sim-exchange market clock offset"),
    ) -> None:
        opts: dict[str, Any] = {"home": home, "instance": instance, "shard": shard, "parent_pid": parent_pid}
        if clock_offset_ns is not None:
            opts["clock_offset_ns"] = clock_offset_ns
        code = resolve(name)(**opts)
        raise typer.Exit(code or 0)

    command.__doc__ = f"Run {name}."
    app.command(name)(command)


for _name in SERVICES:
    _make_command(_name)
