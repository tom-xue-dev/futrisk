"""Configuration of the ``strategy-sim`` bots (01 §7.1 account styles).

The defaults live in this module (so a frozen build without data files still runs);
``bots.yaml`` next to it documents the same values and is the file to copy and edit.
Resolution order (later wins):

1. code defaults (:class:`BotsConfig`);
2. ``src/futrisk/sim/bots/bots.yaml`` (packaged defaults);
3. ``<home>/config/bots.yaml`` when present (per-installation overrides);
4. ``FUTRISK_BOTS_CONFIG=<file>`` or the ``config_path`` option;
5. environment switches: ``FUTRISK_BOTS_AGGRESSIVE`` (``1/true/yes/on``; also
   ``FUTRISK_BOTS_MODE=aggressive|breaker|scenario``), ``FUTRISK_BOTS_SEED``,
   ``FUTRISK_BOTS_ACCOUNTS=ACC3,ACC5``; then the explicit ``run(...)`` options.

Per account: ``strategy`` (default: ``accounts.yaml`` ``strategy``), strategy
``params`` (validated by the strategy's own model), ``execution`` overrides merged on
top of the global ``execution`` block, and ``aggressive`` overrides (``params`` and
``execution`` keys) applied only in aggressive/scenario mode (ACC3 builds its book at
once for the breaker demo, 01 §6.3 QS-03).
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field

PACKAGED_CONFIG = Path(__file__).with_name("bots.yaml")
ENV_CONFIG = "FUTRISK_BOTS_CONFIG"
ENV_AGGRESSIVE = "FUTRISK_BOTS_AGGRESSIVE"
ENV_MODE = "FUTRISK_BOTS_MODE"
ENV_SEED = "FUTRISK_BOTS_SEED"
ENV_ACCOUNTS = "FUTRISK_BOTS_ACCOUNTS"
_TRUE = {"1", "true", "yes", "on", "y"}
_AGGRESSIVE_MODES = {"aggressive", "breaker", "scenario"}


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ExecutionConfig(_Strict):
    """How target positions become orders (per account; see :mod:`futrisk.sim.bots.executor`)."""

    max_orders_per_min: float = 6.0  # token bucket refill rate (market time)
    burst: int = 3  # token bucket size
    order_ttl_s: float = 20.0  # cancel an own working order older than this (market time)
    aggression_ticks: list[int] = Field(default_factory=lambda: [1, 2])  # crossing depth, picked by the RNG
    passive_prob: float = 0.2  # probability that an opening order joins the book instead of crossing
    slice_notional: float = 1_500_000.0  # max notional per order
    max_slice_lots: int = 50  # max lots per order (also capped by products.yaml max_order_qty)
    deadband_frac: float = 0.1  # skip rebalances smaller than this fraction of the target
    probe_qty: int = 1  # size of the first opening order after an account/product open block expires
    in_doubt_resubmit_s: float = 2.0  # re-send an in-doubt order (same client_order_id) after this
    in_doubt_giveup_s: float = 15.0  # then give it up (it no longer counts as working)
    cancel_retry_s: float = 5.0  # re-send a cancel whose outcome is still unknown after this
    halt_probe_s: float = 20.0  # while HALTED: one 1-lot opening probe per this many seconds (0 = off)


class GatesConfig(_Strict):
    """Back-off after rejects (02 §11.2); seconds of market time, doubled per repeat up to the max."""

    halt_backoff_s: float = 30.0  # REJECT_ACCOUNT_HALTED / kill switch: probe interval
    halt_backoff_max_s: float = 120.0
    block_backoff_s: float = 20.0  # account/product blocks, add-to-loser, exposure cap, what-if, funds
    block_backoff_max_s: float = 300.0
    transport_backoff_s: float = 3.0  # TIMEOUT/UNAVAILABLE/BUSY/RISK_BUSY after the client retries
    transport_backoff_max_s: float = 60.0
    size_restore_s: float = 120.0  # ORDER_SIZE_FACTOR: restore the full size after this quiet time


class AccountBotConfig(_Strict):
    enabled: bool = True
    strategy: str | None = None
    params: dict[str, Any] = Field(default_factory=dict)
    execution: dict[str, Any] = Field(default_factory=dict)
    aggressive: dict[str, Any] = Field(default_factory=dict)


class BotsConfig(_Strict):
    identity: str = "strategy-sim"  # DEALER identity: must be whitelisted for order.* (02 §4.11)
    seed: int = 20260928
    aggressive: bool = False
    accounts_filter: list[str] | None = None  # run only these accounts (None: every configured one)
    order_tag: str = "strategy-sim/bots"  # marker in own client_order_ids: finds a predecessor's orders
    status_log_s: float = 60.0
    resync_interval_s: float = 300.0  # idle position/equity resync (wall seconds; 0 disables)
    ready_timeout_s: float = 60.0  # wait this long for RUNNING shards, then start anyway
    startup_cancel_own: bool = True  # cancel the own working orders of a previous incarnation
    startup_sync: bool = True  # risk.query.accounts (positions, equity, state) at start-up
    shutdown_cancel_own: bool = True  # cancel own working orders on a clean stop (never others')
    snapshot_resync: bool = True  # re-seed positions from risk.snapshot when idle and drifted
    drift_confirm: int = 2  # consecutive mismatching snapshots before a re-seed
    execution: ExecutionConfig = ExecutionConfig()
    gates: GatesConfig = GatesConfig()
    accounts: dict[str, AccountBotConfig] = Field(default_factory=dict)

    def execution_for(self, account_id: str, *, aggressive: bool | None = None) -> ExecutionConfig:
        acct = self.accounts.get(account_id, AccountBotConfig())
        data = self.execution.model_dump()
        data.update(acct.execution)
        if self.aggressive if aggressive is None else aggressive:
            data.update(acct.aggressive.get("execution", {}))
        return ExecutionConfig.model_validate(data)

    def params_for(self, account_id: str, *, aggressive: bool | None = None) -> dict[str, Any]:
        acct = self.accounts.get(account_id, AccountBotConfig())
        data = dict(acct.params)
        if self.aggressive if aggressive is None else aggressive:
            data.update(acct.aggressive.get("params", {}))
        return data


def _read(path: Path) -> dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ValueError(f"{path}: top level must be a mapping")
    return data


def _merge(base: dict[str, Any], over: Mapping[str, Any]) -> dict[str, Any]:
    """Recursive dict merge (``over`` wins; lists and scalars are replaced)."""
    out = dict(base)
    for k, v in over.items():
        if isinstance(v, Mapping) and isinstance(out.get(k), Mapping):
            out[k] = _merge(dict(out[k]), v)
        else:
            out[k] = v
    return out


def env_aggressive(env: Mapping[str, str] | None = None) -> bool | None:
    """Aggressive/scenario switch from the environment (``None`` when not set)."""
    e = os.environ if env is None else env
    raw = e.get(ENV_AGGRESSIVE)
    if raw is not None and raw.strip():
        return raw.strip().lower() in _TRUE
    mode = e.get(ENV_MODE)
    if mode is not None and mode.strip():
        return mode.strip().lower() in _AGGRESSIVE_MODES
    return None


def load_bots_config(
    *,
    home_config_dir: Path | None = None,
    config_path: str | os.PathLike[str] | None = None,
    aggressive: bool | None = None,
    seed: int | None = None,
    accounts: list[str] | str | None = None,
    env: Mapping[str, str] | None = None,
    packaged: Path | None = PACKAGED_CONFIG,
) -> BotsConfig:
    """Build the effective :class:`BotsConfig` (see the module docstring for the order)."""
    e = os.environ if env is None else env
    data: dict[str, Any] = {}
    files: list[Path] = []
    if packaged is not None and packaged.exists():
        files.append(packaged)
    if home_config_dir is not None and (home_config_dir / "bots.yaml").exists():
        files.append(home_config_dir / "bots.yaml")
    explicit = config_path or e.get(ENV_CONFIG)
    if explicit:
        p = Path(explicit)
        if not p.exists():
            raise FileNotFoundError(f"bots config {p} not found")
        files.append(p)
    for f in files:
        data = _merge(data, _read(f))
    agg = env_aggressive(e)
    if agg is not None:
        data["aggressive"] = agg
    if e.get(ENV_SEED, "").strip():
        data["seed"] = int(e[ENV_SEED])
    if e.get(ENV_ACCOUNTS, "").strip():
        data["accounts_filter"] = [a.strip() for a in e[ENV_ACCOUNTS].split(",") if a.strip()]
    if aggressive is not None:
        data["aggressive"] = aggressive
    if seed is not None:
        data["seed"] = seed
    if accounts:
        items = accounts.split(",") if isinstance(accounts, str) else accounts
        data["accounts_filter"] = [a.strip() for a in items if a.strip()]
    return BotsConfig.model_validate(data)


__all__ = [
    "ENV_ACCOUNTS",
    "ENV_AGGRESSIVE",
    "ENV_CONFIG",
    "ENV_MODE",
    "ENV_SEED",
    "PACKAGED_CONFIG",
    "AccountBotConfig",
    "BotsConfig",
    "ExecutionConfig",
    "GatesConfig",
    "env_aggressive",
    "load_bots_config",
]
