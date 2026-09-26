"""Backtest configuration (``config/backtest/*.yaml``, 01 §8.16).

A backtest config names the kind of run, the data layer, the period, the
accounts and their strategy, and the experiment arms. It is validated with
pydantic (extra keys are errors) and can be overridden from the command line
with dotted ``key=value`` pairs (``--set rules.threshold=0.025``); values are
parsed as YAML, so ``--set arms=[{name: x, tier: none}]`` works too.

``kind`` selects what a run does:

- ``arms`` (layer A): the same daily bars replayed once per arm - ``no_rules``
  (tier ``none``) against the production daily-loss rule (tier ``block_open``);
- ``scenario`` (layer D): synthetic scenarios with known answers and exact checks.

The config hash (manifest, ``backtest_runs.config_hash``) is the SHA-256 of the
canonical JSON of the resolved config together with the hash of the base
configuration files a backtest reads (:data:`BASE_FILES`: products, accounts,
risk rules, trading calendar), so a change in any input changes the hash and a
change in an unrelated service setting does not.
"""

from __future__ import annotations

import copy
import hashlib
import json
from decimal import Decimal
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

from futrisk.common.paths import app_dir, bundled_config_dir

TIERS = ("none", "block_open")
Tier = Literal["none", "block_open"]
Layer = Literal["A", "D"]
Kind = Literal["arms", "scenario"]
# base configuration files a backtest reads (products, accounts, risk rules, calendar): the config hash covers these
BASE_FILES = ("products.yaml", "accounts.yaml", "risk_rules.yaml", "calendar/trading_days.csv")

# 01 §8.14 default ladder of the daily-loss rule relative to the breaker threshold (3%):
# warn −2%, restrict −2.5%, halt −3%, derisk −5%.
DEFAULT_LADDER: dict[str, float] = {"warn": 2.0 / 3.0, "restrict": 5.0 / 6.0, "halt": 1.0, "derisk": 5.0 / 3.0}
DEFAULT_PRODUCTS = ["rb", "hc", "cu", "au", "i", "j", "m", "IF", "IH", "IC", "IM", "T"]


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ArmSpec(_Strict):
    """One arm of the paired experiment: ``none`` (no rule) or ``block_open`` (the production daily-loss rule)."""

    name: str
    tier: Tier = "block_open"
    threshold: float | None = None  # breaker threshold as a positive fraction of initial capital; None = rules.threshold


class RulesSpec(_Strict):
    """How the daily-loss rule of ``risk_rules.yaml`` is instantiated per arm."""

    threshold: float = 0.03
    ladder: dict[str, float] = Field(default_factory=lambda: dict(DEFAULT_LADDER))


class PathSpec(_Strict):
    """Intraday path synthesis for layer A (01 §8.16 日内路径): the deterministic path of ``rule``."""

    rule: Literal["open_near_extreme", "high_first", "low_first"] = "open_near_extreme"
    points_per_leg: int = 4  # minimum states per leg O→E1, E1→E2, E2→C (the leg end included)
    # adaptive density: a leg gets at least |move| / (max_step × open) states, so a crash leg crosses a loss
    # threshold in small steps instead of one jump (0 = fixed points_per_leg), per contract (its own bar only);
    # the cap keeps steps <= max_step for legs up to max_points_per_leg × max_step (20 %) of the open
    max_step: float = 0.0025
    max_points_per_leg: int = 80


class DataSpec(_Strict):
    # relative paths in a backtest config resolve against app_dir(): the exe's folder when frozen, else the repo
    dir: str | None = None  # default <app dir>/data/hist


class AccountRun(_Strict):
    strategy: str = "dual_ma"
    products: list[str] | None = None  # default: the account's products in accounts.yaml
    params: dict[str, Any] = Field(default_factory=dict)
    capital: Decimal | None = None  # default: accounts.yaml initial_capital
    enabled: bool = True


class ScenarioStep(_Strict):
    """One market state of a synthetic scenario: sector / instrument moves relative to the day's reference."""

    label: str
    at_s: float  # seconds after the scenario start (market time)
    move: dict[str, float] = Field(default_factory=dict)  # instrument or product or sector -> move vs reference
    orders: list[dict[str, Any]] = Field(default_factory=list)  # scripted orders submitted at this step


class ScenarioSpec(_Strict):
    """A layer D scenario with known answers (``expect`` is checked by the runner and the unit tests)."""

    name: str
    description: str = ""
    trading_day: str = "20260929"
    start: str = "09:35:00"  # Beijing time of step 0 (inside a continuous session of every instrument)
    reference: dict[str, float] = Field(default_factory=dict)  # instrument -> pre-settle = reference price
    book: dict[str, int] = Field(default_factory=dict)  # scripted opening book (lots) bought at step 0
    account: str = "ACC3"
    steps: list[ScenarioStep] = Field(default_factory=list)
    expect: dict[str, Any] = Field(default_factory=dict)


class BacktestConfig(_Strict):
    name: str
    description: str = ""
    kind: Kind = "arms"
    layer: Layer = "A"
    start: str | None = None  # YYYYMMDD, default: first data day
    end: str | None = None  # default: last data day
    products: list[str] = Field(default_factory=lambda: list(DEFAULT_PRODUCTS))
    accounts: dict[str, AccountRun] = Field(default_factory=dict)
    arms: list[ArmSpec] = Field(
        default_factory=lambda: [ArmSpec(name="no_rules", tier="none"), ArmSpec(name="block_open", tier="block_open")]
    )
    rules: RulesSpec = RulesSpec()
    path: PathSpec = PathSpec()
    data: DataSpec = DataSpec()
    scenarios: list[ScenarioSpec] = Field(default_factory=list)
    seed: int = 20260928
    workers: int | Literal["auto"] = "auto"  # process-pool size for the arms (1 = in this process)
    warmup_days: int = 0  # trading days of history fed to the strategies before ``start`` (no trading)
    base_config: str | None = None  # base config directory (default: the bundled config/; relative: app_dir())

    @model_validator(mode="after")
    def _check(self) -> BacktestConfig:
        names = [a.name for a in self.arms]
        if not names:
            raise ValueError("at least one arm is needed")
        if len(set(names)) != len(names):
            raise ValueError(f"arm names must be unique: {names}")
        for d in (self.start, self.end):
            if d is not None and (len(d) != 8 or not d.isdigit()):
                raise ValueError(f"dates are YYYYMMDD strings, got {d!r}")
        if self.kind == "scenario" and (self.layer != "D" or not self.scenarios):
            raise ValueError("kind=scenario needs layer D and scenarios")
        if self.kind == "arms" and self.layer != "A":
            raise ValueError("kind=arms replays layer A")
        if self.path.points_per_leg < 1:
            raise ValueError("path.points_per_leg must be >= 1")
        return self

    def arm(self, name: str) -> ArmSpec:
        for a in self.arms:
            if a.name == name:
                return a
        raise KeyError(name)

    def threshold_of(self, arm: ArmSpec) -> float:
        return float(arm.threshold if arm.threshold is not None else self.rules.threshold)

    def data_dir(self) -> Path:
        """``data.dir`` (relative to :func:`app_dir`), default ``<app dir>/data/hist``."""
        return app_path(self.data.dir) if self.data.dir else app_dir() / "data" / "hist"


def app_path(path: str | Path) -> Path:
    """A path from a backtest config: absolute as given, relative ones against :func:`app_dir`.

    ``app_dir()`` is the exe's folder when frozen (files shipped next to the exe, e.g. ``data/hist``) and the
    repository root in development.
    """
    p = Path(path)
    return p if p.is_absolute() else app_dir() / p


# ------------------------------------------------------------------ loading
def backtest_config_dirs() -> list[Path]:
    """Directories searched for ``<name>.yaml`` (the bundled ``config/backtest``)."""
    return [bundled_config_dir() / "backtest"]


def resolve_config_path(name_or_path: str) -> Path:
    """``default`` -> ``config/backtest/default.yaml``; an existing file path is used as is."""
    p = Path(name_or_path)
    if p.suffix in (".yaml", ".yml") and p.exists():
        return p.resolve()
    for d in backtest_config_dirs():
        for ext in (".yaml", ".yml"):
            cand = d / f"{name_or_path}{ext}"
            if cand.exists():
                return cand.resolve()
    known = sorted(q.stem for d in backtest_config_dirs() if d.exists() for q in d.glob("*.yaml"))
    raise FileNotFoundError(f"backtest config {name_or_path!r} not found (known: {', '.join(known) or 'none'})")


def list_configs() -> list[tuple[str, str, Path]]:
    """``(name, description, path)`` of every bundled backtest config."""
    out: list[tuple[str, str, Path]] = []
    for d in backtest_config_dirs():
        if not d.exists():
            continue
        for p in sorted(d.glob("*.yaml")):
            try:
                raw = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
                desc = str(raw.get("description", "")).strip().splitlines()[0] if raw.get("description") else ""
            except Exception as exc:  # pragma: no cover - broken file is still listed
                desc = f"(unreadable: {exc})"
            out.append((p.stem, desc, p))
    return out


def parse_override(text: str) -> tuple[str, Any]:
    """``a.b=value`` -> ``("a.b", yaml.safe_load(value))``."""
    if "=" not in text:
        raise ValueError(f"override must be key=value, got {text!r}")
    key, _, raw = text.partition("=")
    key = key.strip()
    if not key:
        raise ValueError(f"empty key in override {text!r}")
    try:
        value = yaml.safe_load(raw) if raw.strip() != "" else ""
    except yaml.YAMLError:
        value = raw
    if isinstance(value, int) and not isinstance(value, bool) and len(raw.strip()) == 8 and raw.strip().isdigit():
        value = raw.strip()  # dates stay strings
    return key, value


def apply_overrides(raw: dict[str, Any], overrides: dict[str, Any] | None) -> dict[str, Any]:
    """Apply dotted-key overrides to a raw config mapping (returns a new mapping)."""
    out = copy.deepcopy(raw)
    for key, value in (overrides or {}).items():
        parts = [p for p in str(key).split(".") if p]
        cur: Any = out
        for p in parts[:-1]:
            if not isinstance(cur, dict):
                raise ValueError(f"override {key}: {p} is not a mapping")
            nxt = cur.get(p)
            if nxt is None:
                nxt = {}
                cur[p] = nxt
            cur = nxt
        if not isinstance(cur, dict):
            raise ValueError(f"override {key}: parent is not a mapping")
        leaf = parts[-1]
        if isinstance(value, int) and not isinstance(value, bool) and leaf in ("start", "end", "trading_day"):
            value = str(value)
        cur[leaf] = value
    return out


def load_backtest_config(name_or_path: str, overrides: dict[str, Any] | None = None) -> tuple[BacktestConfig, Path]:
    path = resolve_config_path(name_or_path)
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(raw, dict):
        raise ValueError(f"{path}: top level must be a mapping")
    raw.setdefault("name", path.stem)
    merged = apply_overrides(raw, overrides)
    for k in ("start", "end"):
        if isinstance(merged.get(k), int):
            merged[k] = str(merged[k])
    return BacktestConfig.model_validate(merged), path


def canonical_json(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str)


def base_files_hash(files: dict[str, str]) -> str:
    """SHA-256 over ``name:sha256`` of the base configuration files a backtest reads (:data:`BASE_FILES`).

    ``files`` is ``FullConfig.files`` (hashed when the configuration was loaded); service settings
    (``app.yaml``, ``services.yaml``, ...) do not change a backtest and are left out.
    """
    text = "\n".join(f"{name}:{files.get(name, 'missing')}" for name in BASE_FILES)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def config_hash(cfg: BacktestConfig, base_hash: str) -> str:
    """SHA-256 of the resolved backtest config and the hash of the base files it reads (:func:`base_files_hash`)."""
    body = canonical_json({"backtest": cfg.model_dump(mode="json"), "base": base_hash})
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


__all__ = [
    "BASE_FILES",
    "DEFAULT_LADDER",
    "DEFAULT_PRODUCTS",
    "TIERS",
    "AccountRun",
    "ArmSpec",
    "BacktestConfig",
    "DataSpec",
    "PathSpec",
    "RulesSpec",
    "ScenarioSpec",
    "ScenarioStep",
    "app_path",
    "apply_overrides",
    "base_files_hash",
    "canonical_json",
    "config_hash",
    "list_configs",
    "load_backtest_config",
    "parse_override",
    "resolve_config_path",
]
