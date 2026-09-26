"""Experiment arms: the risk-rule configuration each arm runs with (01 §8.16 配对实验).

Every arm replays the same data with the same strategies; only the rules differ:

- ``none`` (arm ``no_rules``): no rule at all (the pre-trade static, funds and
  session checks still apply, as in production);
- ``block_open``: the production daily-loss rule (``risk_rules.yaml``) =
  **restrict + circuit break**: warn ALERT, restrict RESTRICT (order size ×
  0.5, no adding to losers), halt CIRCUIT_BREAK (HALTED at the first breach,
  latched after 2 distinct LIVE states or 1 s; opens rejected, closes
  allowed), derisk LIQUIDATE (plan only). This is the task's requirement
  ("熔断禁止开仓").

The breaker threshold T scales the whole daily-loss ladder (default multiples
2/3, 5/6, 1, 5/3 of T give −2 %, −2.5 %, −3 %, −5 % for T = 3 %).

The backtest's ``system`` settings switch the engine's periodic timers off
(LV snapshots, metric samples, position reconciliation: ``0`` = off): the replay
reads the engine state directly, the in-process broker never diverges, and time
is driven by :meth:`EngineCore.next_timer_ns` alone.
"""

from __future__ import annotations

from typing import Any

from futrisk.backtest.config import ArmSpec, BacktestConfig
from futrisk.common.config.risk_rules import RiskRulesConfig, RuleDef, SystemRiskConfig

DAILY_LOSS = "daily_loss"
HALT_LEVEL = "halt"
BACKTEST_SYSTEM = {"snapshot_hz": 0.0, "metric_sample_s": 0.0, "recon_interval_s": 0}


def backtest_system(base: SystemRiskConfig) -> SystemRiskConfig:
    return base.model_copy(update=BACKTEST_SYSTEM)


def daily_loss_rule(base: RiskRulesConfig, threshold: float, ladder: dict[str, float]) -> RuleDef:
    """The production daily-loss rule with its levels at ``−threshold × ladder[level]``."""
    rule = base.rule(DAILY_LOSS)
    if rule is None:
        raise ValueError("risk_rules.yaml has no daily_loss rule")
    levels: list[dict[str, Any]] = []
    for lv in rule.levels:
        d = lv.model_dump(exclude_unset=True)
        mult = ladder.get(lv.name)
        if mult is not None:
            d["threshold"] = -abs(threshold) * float(mult)
        levels.append(d)
    data = rule.model_dump(exclude_unset=True)
    data["levels"] = levels
    data["overrides"] = {}
    return RuleDef.model_validate(data)


def arm_rules(base: RiskRulesConfig, cfg: BacktestConfig, arm: ArmSpec) -> RiskRulesConfig:
    """The :class:`RiskRulesConfig` an arm's engine runs with."""
    system = backtest_system(base.system)
    if arm.tier == "none":
        return RiskRulesConfig(rules=[], pretrade=base.pretrade, system=system)
    rule = daily_loss_rule(base, cfg.threshold_of(arm), cfg.rules.ladder)
    return RiskRulesConfig(rules=[rule], pretrade=base.pretrade, system=system)


__all__ = ["BACKTEST_SYSTEM", "DAILY_LOSS", "HALT_LEVEL", "arm_rules", "backtest_system", "daily_loss_rule"]
