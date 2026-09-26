"""``futrisk config validate|show|schema|commit`` (01 §8.9)."""

from __future__ import annotations

import json
from pathlib import Path

import typer

from futrisk.common.config import ConfigError, load_config_dir
from futrisk.common.config.loader import CONFIG_FILES
from futrisk.common.paths import HomeLayout, bundled_config_dir, resolve_home

app = typer.Typer(help="Validate, show and export the configuration", no_args_is_help=True)


def _config_dir(directory: str | None, home: str | None) -> Path:
    if directory:
        return Path(directory)
    if home:
        return HomeLayout(resolve_home(home)).config_dir
    return bundled_config_dir()


@app.command()
def validate(
    directory: str | None = typer.Option(None, "--dir", help="Config directory (default: bundled defaults)"),
    home: str | None = typer.Option(None, "--home", help="Validate <home>/config instead"),
) -> None:
    """Load and validate every config file; exit code 1 on error."""
    path = _config_dir(directory, home)
    try:
        cfg = load_config_dir(path)
    except ConfigError as exc:
        typer.secho(f"INVALID: {exc}", fg=typer.colors.RED)
        raise typer.Exit(1) from exc
    typer.secho(f"OK  {path}", fg=typer.colors.GREEN)
    typer.echo(f"config_hash {cfg.config_hash}")
    typer.echo(
        f"products {len(cfg.products.products)}  instruments {len(cfg.products.instruments)}  "
        f"accounts {len(cfg.accounts.accounts)}  rules {len(cfg.risk_rules.rules)}  "
        f"trading days {cfg.calendar.first_day}..{cfg.calendar.last_day}"
    )


@app.command()
def show(
    section: str | None = typer.Argument(None, help="One of: " + ", ".join(CONFIG_FILES)),
    directory: str | None = typer.Option(None, "--dir"),
    home: str | None = typer.Option(None, "--home"),
) -> None:
    """Print the effective (validated) configuration as JSON."""
    cfg = load_config_dir(_config_dir(directory, home))
    sections = [section] if section else list(CONFIG_FILES)
    out = {}
    for s in sections:
        if s not in CONFIG_FILES:
            typer.secho(f"unknown section {s!r}", fg=typer.colors.RED)
            raise typer.Exit(2)
        out[s] = getattr(cfg, s).model_dump(mode="json")
    typer.echo(json.dumps(out, ensure_ascii=False, indent=2))


@app.command()
def schema(out: str | None = typer.Option(None, "--out", help="Directory for <section>.schema.json files")) -> None:
    """Export JSON Schema of every config file (basis of the config reference doc)."""
    schemas = {name: model.model_json_schema() for name, (_fname, model) in CONFIG_FILES.items()}
    if out:
        target = Path(out)
        target.mkdir(parents=True, exist_ok=True)
        for name, sch in schemas.items():
            (target / f"{name}.schema.json").write_text(json.dumps(sch, ensure_ascii=False, indent=2), encoding="utf-8")
        typer.echo(f"wrote {len(schemas)} schemas to {target}")
    else:
        typer.echo(json.dumps(schemas, ensure_ascii=False, indent=2))


@app.command()
def commit(
    home: str | None = typer.Option(None, "--home", help="Runtime home (default: FUTRISK_HOME / var)"),
    force: bool = typer.Option(False, "--force", help="Commit even if this home's supervisor is running"),
) -> None:
    """Offline commit: validate ``<home>/config`` and make it the active config version.

    For changes that are restart-only (e.g. ``services.yaml`` endpoints, ``feeds.yaml`` sources,
    ``products.yaml`` / calendar in risk-engine): stop the system (``futrisk down``), edit the files
    under ``<home>/config``, run this, then start again (``futrisk up``). While the system runs, use
    the hot reload (``POST /api/v1/config/reload``) instead: it validates and commits through 2PC.
    """
    from futrisk.common.config.loader import (
        ensure_home_config,
        read_active,
        snapshot_working_config,
        write_active,
    )

    layout = HomeLayout(resolve_home(home))
    ensure_home_config(layout)
    try:
        cfg = load_config_dir(layout.config_dir)
    except ConfigError as exc:
        typer.secho(f"INVALID  {layout.config_dir}: {exc}", fg=typer.colors.RED)
        raise typer.Exit(1) from None
    if not force and _supervisor_running(cfg):
        typer.secho(
            "this home's system is running: stop it first (futrisk down), or use the hot reload "
            "(POST /api/v1/config/reload); --force commits anyway (services pick it up at their next start)",
            fg=typer.colors.RED,
        )
        raise typer.Exit(1)
    active = read_active(layout) or {}
    version, path = snapshot_working_config(layout)
    write_active(layout, version, path)
    typer.echo(
        f"committed config version {version} (was {active.get('config_version', '-')}), "
        f"hash {cfg.config_hash[:12]}: {path}"
    )


def _supervisor_running(cfg: object) -> bool:
    """True when the supervisor on the configured ports answers ``ctl.ping`` (fast check)."""
    try:
        from futrisk.cli.ops_cmd import SupervisorClient

        with SupervisorClient(cfg) as sc:  # type: ignore[arg-type]
            return sc.ping(timeout_ms=800) is not None
    except Exception:  # noqa: BLE001 - no bus / no supervisor configured: treat as not running
        return False
