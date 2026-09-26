"""Bots configuration: packaged bots.yaml == code defaults, overrides, environment switches."""

from __future__ import annotations

from pathlib import Path

import pytest

from futrisk.sim.bots.config import (
    PACKAGED_CONFIG,
    BotsConfig,
    ExecutionConfig,
    GatesConfig,
    env_aggressive,
    load_bots_config,
)
from futrisk.sim.bots.strategies import STRATEGIES


def test_packaged_yaml_matches_code_defaults() -> None:
    cfg = load_bots_config(env={})
    code = BotsConfig()
    assert cfg.execution == code.execution == ExecutionConfig()
    assert cfg.gates == code.gates == GatesConfig()
    for field in ("identity", "seed", "aggressive", "status_log_s", "resync_interval_s", "ready_timeout_s",
                  "startup_cancel_own", "startup_sync", "shutdown_cancel_own", "order_tag", "snapshot_resync",
                  "drift_confirm"):
        assert getattr(cfg, field) == getattr(code, field), field
    assert sorted(cfg.accounts) == ["ACC1", "ACC2", "ACC3", "ACC4", "ACC5"]
    for acct, acfg in cfg.accounts.items():
        cls = STRATEGIES[acfg.strategy or ""]
        # bots.yaml documents the strategy defaults: keep both in sync
        assert cls.params_model.model_validate(acfg.params) == cls.params_model(), acct
    assert PACKAGED_CONFIG.exists()


def test_aggressive_overrides_only_acc3() -> None:
    cfg = load_bots_config(env={}, aggressive=True)
    assert cfg.aggressive
    ex3 = cfg.execution_for("ACC3")
    assert (ex3.max_orders_per_min, ex3.burst, ex3.max_slice_lots) == (60, 20, 100)
    assert cfg.params_for("ACC3")["aggressive"] is True
    assert cfg.execution_for("ACC1") == ExecutionConfig()
    normal = load_bots_config(env={})
    assert normal.execution_for("ACC3").max_orders_per_min == 8 and normal.params_for("ACC3")["aggressive"] is False
    assert normal.execution_for("ACC3", aggressive=True).max_orders_per_min == 60


def test_environment_switches_and_explicit_options(tmp_path: Path) -> None:
    assert env_aggressive({}) is None
    assert env_aggressive({"FUTRISK_BOTS_AGGRESSIVE": "yes"}) is True
    assert env_aggressive({"FUTRISK_BOTS_AGGRESSIVE": "0"}) is False
    assert env_aggressive({"FUTRISK_BOTS_MODE": "breaker"}) is True
    cfg = load_bots_config(env={"FUTRISK_BOTS_AGGRESSIVE": "1", "FUTRISK_BOTS_SEED": "7",
                                "FUTRISK_BOTS_ACCOUNTS": "ACC3, ACC5"})
    assert (cfg.aggressive, cfg.seed, cfg.accounts_filter) == (True, 7, ["ACC3", "ACC5"])
    cfg = load_bots_config(env={"FUTRISK_BOTS_AGGRESSIVE": "1"}, aggressive=False, accounts="ACC1", seed=3)
    assert (cfg.aggressive, cfg.seed, cfg.accounts_filter) == (False, 3, ["ACC1"])


def test_file_overrides_merge(tmp_path: Path) -> None:
    home_cfg = tmp_path / "config"
    home_cfg.mkdir()
    (home_cfg / "bots.yaml").write_text(
        "seed: 11\naccounts:\n  ACC3:\n    params: {weights: {rb2701: 0.3, hc2701: 0.3, i2701: 0.3}}\n",
        encoding="utf-8")
    extra = tmp_path / "extra.yaml"
    extra.write_text("execution: {max_orders_per_min: 2}\n", encoding="utf-8")
    cfg = load_bots_config(home_config_dir=home_cfg, config_path=extra, env={})
    assert cfg.seed == 11 and cfg.execution.max_orders_per_min == 2 and cfg.execution.burst == 3
    p3 = cfg.params_for("ACC3")
    assert p3["weights"] == {"rb2701": 0.3, "hc2701": 0.3, "i2701": 0.3} and p3["build_s"] == 300  # deep merge
    assert cfg.accounts["ACC1"].strategy == "channel_breakout"
    with pytest.raises(FileNotFoundError):
        load_bots_config(config_path=tmp_path / "missing.yaml", env={})
    with pytest.raises(ValueError):
        BotsConfig.model_validate({"no_such_key": 1})
