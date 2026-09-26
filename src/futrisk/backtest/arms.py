"""Experiment arms: the risk-rule configuration each arm runs with (01 §8.16 配对实验; plan §7.1).

Every arm replays the same data with the same strategies and seeds; only the
rules differ:

- ``none`` (arm ``no_rules``): no rule at all (the pre-trade static, funds and
  session checks still apply, as in production);
- ``alert_only``: the daily-loss rule with every level turned into ``ALERT``
  (same thresholds and confirmation) - the triggers are recorded, nothing is
  blocked. Its trading equals ``no_rules`` exactly (a test and every run check
  it), so it also serves as the baseline when ``no_rules`` is not run, and it
  is the counterfactual of the true/false-positive statistics;
- ``block_open``: the production daily-loss rule (``risk_rules.yaml``) =
  **restrict + circuit break**: warn ALERT, restrict RESTRICT (order size ×
  0.5, no adding to losers), halt CIRCUIT_BREAK (HALTED at the first breach,
  latched after 2 distinct LIVE states or 1 s; opens rejected, closes
  allowed), derisk LIQUIDATE (plan only). This is the task's requirement
  ("熔断禁止开仓");
- ``halt_only``: the circuit break alone (the restrict level is an ALERT), so
  ``block_open`` − ``halt_only`` isolates the restrict level's contribution;
- ``halve`` / ``flatten``: ``block_open`` plus the backtest's own action when the
  halt level latches: closing orders for 50% / 100% of every position, built by
  the production liquidation planner (:func:`futrisk.core.actions.liquidation_steps`,
  losers first, SHFE close-yesterday before close-today) and submitted through
  the engine. The production engine only proposes such plans (TD-08);
- ``placebo``: no rule; the referenced arm's breaker action (a manual halt
  through the engine's ``risk.halt`` for the rest of the day, plus its
  reduction) on random days on which the breaker did not fire, matched count
  and times of day (the runner draws the schedule);
- ``production``: the whole ``risk_rules.yaml`` (every rule) with the
  ``block_open`` semantics.

The breaker threshold T scales the whole daily-loss ladder (default multiples
2/3, 5/6, 1, 5/3 of T give −2 %, −2.5 %, −3 %, −5 % for T = 3 %), so a threshold
sweep moves warn / restrict / halt / derisk together.

The backtest's ``system`` settings switch the engine's periodic timers off
(LV snapshots, metric samples, position reconciliation: ``0`` = off): the replay
reads the engine state directly, the in-process broker never diverges, and time
is driven by :meth:`EngineCore.next_timer_ns` alone.
"""

from __future__ import annotations

from typing import Any

from futrisk.backtest.config import ArmSpec, BacktestConfig
from futrisk.common.config.risk_rules import RiskRulesConfig, RuleDef, SystemRiskConfig
from futrisk.messages.enums import Action

DAILY_LOSS = "daily_loss"
HALT_LEVEL = "halt"
RESTRICT_LEVEL = "restrict"
BACKTEST_SYSTEM = {"snapshot_hz": 0.0, "metric_sample_s": 0.0, "recon_interval_s": 0}
ACTING_TIERS = ("block_open", "halt_only", "halve", "flatten", "production")


def backtest_system(base: SystemRiskConfig) -> SystemRiskConfig:
    return base.model_copy(update=BACKTEST_SYSTEM)


def daily_loss_rule(base: RiskRulesConfig, threshold: float, ladder: dict[str, float], *, alert_only: bool,
                    alert_levels: tuple[str, ...] = ()) -> RuleDef:
    """The production daily-loss rule with its levels at ``−threshold × ladder[level]``.

    ``alert_only`` turns every level into an ALERT (the breaker keeps its confirmation timing);
    ``alert_levels`` turns only those levels into ALERTs (``halt_only``: the restrict level).
    """
    rule = base.rule(DAILY_LOSS)
    if rule is None:
        raise ValueError("risk_rules.yaml has no daily_loss rule")
    levels: list[dict[str, Any]] = []
    for lv in rule.levels:
        d = lv.model_dump(exclude_unset=True)
        mult = ladder.get(lv.name)
        if mult is not None:
            d["threshold"] = -abs(threshold) * float(mult)
        if alert_only or lv.name in alert_levels:
            d["action"] = Action.ALERT
            d["latch"] = False
            d["params"] = {}
            if lv.action == Action.CIRCUIT_BREAK and "confirm" not in d:
                d["confirm"] = {"states": 2, "ms": 1000}  # keep the breaker's confirmation timing
        levels.append(d)
    data = rule.model_dump(exclude_unset=True)
    data["levels"] = levels
    data["overrides"] = {}
    return RuleDef.model_validate(data)


def arm_rules(base: RiskRulesConfig, cfg: BacktestConfig, arm: ArmSpec) -> RiskRulesConfig:
    """The :class:`RiskRulesConfig` an arm's engine runs with."""
    system = backtest_system(base.system)
    tier = arm.tier
    t = cfg.threshold_of(arm)
    if tier in ("none", "placebo"):
        return RiskRulesConfig(rules=[], pretrade=base.pretrade, system=system)
    if tier == "production":
        rules = [daily_loss_rule(base, t, cfg.rules.ladder, alert_only=False) if r.id == DAILY_LOSS else r for r in base.rules]
        return RiskRulesConfig(rules=rules, pretrade=base.pretrade, system=system)
    dl = daily_loss_rule(base, t, cfg.rules.ladder, alert_only=tier == "alert_only",
                         alert_levels=(RESTRICT_LEVEL,) if tier == "halt_only" else ())
    rules = [dl]
    if cfg.rules.include_other_rules:
        rules += [r for r in base.rules if r.id != DAILY_LOSS]
    return RiskRulesConfig(rules=rules, pretrade=base.pretrade, system=system)


def reduce_fraction(cfg: BacktestConfig, arm: ArmSpec) -> float:
    """Share of every position the backtest closes when the halt level latches (0 = no action).

    A placebo arm uses the fraction of the arm it imitates.
    """
    tier = arm.tier
    if tier == "placebo" and arm.placebo_of is not None:
        tier = cfg.arm(arm.placebo_of).tier
    if tier in ("halve", "flatten"):
        return float(cfg.rules.reduce_step.get(tier, 0.5 if tier == "halve" else 1.0))
    return 0.0


def splittable(cfg: BacktestConfig) -> bool:
    """Accounts can be replayed one per task (no active rule couples them): only the daily-loss rule runs."""
    return not cfg.rules.include_other_rules and all(a.tier != "production" for a in cfg.arms)


__all__ = [
    "ACTING_TIERS",
    "BACKTEST_SYSTEM",
    "DAILY_LOSS",
    "HALT_LEVEL",
    "RESTRICT_LEVEL",
    "arm_rules",
    "backtest_system",
    "daily_loss_rule",
    "reduce_fraction",
    "splittable",
]
