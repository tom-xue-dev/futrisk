"""Configuration loading, hashing and immutable version snapshots (02 §7.10, 01 §6.7).

Layout under ``<home>/config``:
- the working directory (``app.yaml`` ... ``calendar/trading_days.csv``) edited by admins;
- ``versions/{N}/``: immutable copies created by ``POST /config/reload``;
- ``active.json``: ``{config_version, path, effective}``; services load only this
  committed version at start-up.

``config_hash`` = SHA-256 over ``"{relative_path}:{sha256}"`` lines sorted by path.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from futrisk.common.calendar import TradingCalendar, parse_templates
from futrisk.common.config.accounts import AccountsConfig
from futrisk.common.config.app import AppConfig
from futrisk.common.config.feeds import FeedsConfig
from futrisk.common.config.products import ProductsConfig
from futrisk.common.config.risk_rules import RiskRulesConfig
from futrisk.common.config.services import ServicesConfig
from futrisk.common.config.sim import SimConfig
from futrisk.common.paths import HomeLayout, bundled_config_dir

CONFIG_FILES: dict[str, tuple[str, type[Any]]] = {
    "app": ("app.yaml", AppConfig),
    "services": ("services.yaml", ServicesConfig),
    "products": ("products.yaml", ProductsConfig),
    "accounts": ("accounts.yaml", AccountsConfig),
    "risk_rules": ("risk_rules.yaml", RiskRulesConfig),
    "feeds": ("feeds.yaml", FeedsConfig),
    "sim": ("sim.yaml", SimConfig),
}
CALENDAR_FILE = "calendar/trading_days.csv"
_EXCLUDE_TOP = {"versions", "active.json"}


class ConfigError(Exception):
    """Invalid or missing configuration."""


@dataclass(frozen=True)
class FullConfig:
    root: Path
    app: AppConfig
    services: ServicesConfig
    products: ProductsConfig
    accounts: AccountsConfig
    risk_rules: RiskRulesConfig
    feeds: FeedsConfig
    sim: SimConfig
    calendar: TradingCalendar
    config_version: int = 0
    config_hash: str = ""
    files: dict[str, str] = field(default_factory=dict)

    @property
    def scenarios_dir(self) -> Path:
        return self.root / "scenarios"

    @property
    def backtest_dir(self) -> Path:
        return self.root / "backtest"


def _iter_files(root: Path) -> list[Path]:
    out: list[Path] = []
    for p in sorted(root.rglob("*")):
        if not p.is_file():
            continue
        rel = p.relative_to(root)
        if rel.parts and rel.parts[0] in _EXCLUDE_TOP:
            continue
        if p.name.startswith(".") or p.suffix in (".pyc", ".tmp"):
            continue
        out.append(p)
    return out


def file_hashes(root: Path) -> dict[str, str]:
    """``{relative posix path: sha256}`` of every config file under ``root``."""
    return {p.relative_to(root).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest() for p in _iter_files(root)}


def compute_config_hash(files: dict[str, str]) -> str:
    text = "\n".join(f"{name}:{files[name]}" for name in sorted(files))
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise ConfigError(f"missing config file {path}")
    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ConfigError(f"{path}: top level must be a mapping")
    return data


def load_config_dir(root: str | Path, config_version: int = 0) -> FullConfig:
    """Load and validate every config file under ``root``."""
    root = Path(root)
    parsed: dict[str, Any] = {}
    for key, (fname, model) in CONFIG_FILES.items():
        try:
            parsed[key] = model.model_validate(_read_yaml(root / fname))
        except ConfigError:
            raise
        except Exception as exc:  # pydantic ValidationError and yaml errors
            raise ConfigError(f"{fname}: {exc}") from exc
    products: ProductsConfig = parsed["products"]
    cal_path = root / CALENDAR_FILE
    if not cal_path.exists():
        raise ConfigError(f"missing {CALENDAR_FILE}")
    calendar = TradingCalendar.from_csv(cal_path, parse_templates(products.session_templates))
    files = file_hashes(root)
    return FullConfig(
        root=root,
        calendar=calendar,
        config_version=config_version,
        config_hash=compute_config_hash(files),
        files=files,
        **parsed,
    )


# ------------------------------------------------------------ home / versions
def _atomic_write_json(path: Path, obj: Any) -> None:
    """Write-to-temp, fsync, rename: ``active.json`` is the committed config decision (02 §8.5)."""
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False, indent=2))
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
    if os.name != "nt":  # directory fsync makes the rename durable (not supported on Windows)
        fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


def read_active(layout: HomeLayout) -> dict[str, Any] | None:
    if not layout.active_json.exists():
        return None
    data: dict[str, Any] = json.loads(layout.active_json.read_text(encoding="utf-8"))
    return data


def write_active(layout: HomeLayout, config_version: int, path: Path, effective: dict[str, Any] | None = None) -> None:
    rel = path.relative_to(layout.home).as_posix() if path.is_absolute() else str(path)
    _atomic_write_json(
        layout.active_json,
        {"config_version": config_version, "path": rel, "effective": effective or {}},
    )


def existing_versions(layout: HomeLayout) -> list[int]:
    if not layout.versions_dir.exists():
        return []
    return sorted(int(p.name) for p in layout.versions_dir.iterdir() if p.is_dir() and p.name.isdigit())


def snapshot_working_config(layout: HomeLayout, version: int | None = None) -> tuple[int, Path]:
    """Copy the working config into an immutable ``versions/{N}/`` directory."""
    versions = existing_versions(layout)
    n = version if version is not None else (versions[-1] + 1 if versions else 1)
    dest = layout.versions_dir / str(n)
    if dest.exists():
        raise ConfigError(f"config version {n} already exists")
    dest.mkdir(parents=True)
    for src in _iter_files(layout.config_dir):
        rel = src.relative_to(layout.config_dir)
        target = dest / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, target)
    return n, dest


def ensure_home_config(layout: HomeLayout, bundled: Path | None = None) -> None:
    """First start: copy the bundled defaults and create version 1 + ``active.json``."""
    layout.ensure()
    src = bundled or bundled_config_dir()
    if not (layout.config_dir / "app.yaml").exists():
        for f in _iter_files(src):
            rel = f.relative_to(src)
            target = layout.config_dir / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(f, target)
    if read_active(layout) is None:
        versions = existing_versions(layout)
        if versions:
            n, path = versions[-1], layout.versions_dir / str(versions[-1])
        else:
            n, path = snapshot_working_config(layout, 1)
        write_active(layout, n, path)


def load_active_config(layout: HomeLayout) -> FullConfig:
    """Load the committed config version pointed to by ``active.json``."""
    active = read_active(layout)
    if active is None:
        raise ConfigError(f"{layout.active_json} not found; run ensure_home_config first")
    path = Path(active["path"])
    if not path.is_absolute():
        path = layout.home / path
    return load_config_dir(path, int(active["config_version"]))


def load_default_config() -> FullConfig:
    """Load the bundled default configuration (tests, tools)."""
    return load_config_dir(bundled_config_dir(), 0)
