"""``risk_rules.yaml``: declarative rules, pre-trade parameters (01 §8.9, §8.14).

A rule evaluates one metric per scope; each level has a threshold and an
action. ``op`` gives the breach direction (``<=`` for losses). ``overrides``
maps account id -> level name -> field overrides.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from futrisk.messages.enums import Action


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class Confirm(_Strict):
    states: int = 1
    ms: int = 0


class Rearm(_Strict):
    extra: float = 0.01


class RuleLevel(_Strict):
    name: str
    threshold: float
    action: Action
    params: dict[str, Any] = Field(default_factory=dict)
    latch: bool = False
    confirm: Confirm = Confirm()
    rearm: Rearm | None = None


class RuleDef(_Strict):
    id: str
    metric: str
    op: Literal["<=", ">=", "<", ">"]
    scope: str = "account"  # account | account/product | account/sector | firm | firm/product | firm/sector
    hysteresis: float = 0.2
    cooldown_s: int = 60
    hold_ms: int = 3000
    pretrade: bool = False
    enabled: bool = True
    reset_on_day_roll: bool = True
    # evaluate only while the account's gross notional / equity >= this (e.g. concentration: a small
    # book is not a concentration risk); below it the rule counts as not breached
    min_gross_pct: float | None = None
    levels: list[RuleLevel]
    overrides: dict[str, dict[str, dict[str, Any]]] = Field(default_factory=dict)


class PretradeConfig(_Strict):
    fat_finger_ticks: int = 50
    fat_finger_pct: float = 0.03
    funds_buffer: float = 0.05
    throttle_rate_per_s: float = 20.0
    throttle_burst: int = 40
    route_ttl_ms: int = 2000
    default_max_order_qty: int = 1000


class SystemRiskConfig(_Strict):
    snapshot_hz: float = 2.0
    metric_sample_s: float = 1.0
    md_unknown_account_block_pct: float = 0.5
    recon_interval_s: int = 300
    recon_quiet_s: int = 5
    # materiality of the quality gating for account-scope rules: while the held instruments whose quality is
    # not OK carry at most this share of the account's gross notional, the account quality counts as OK
    # (valued at their last known marks); above it the worst quality applies (machines hold). 0 = off
    quality_stale_tolerance: float = Field(default=0.10, ge=0.0, le=1.0)


class RiskRulesConfig(_Strict):
    rules: list[RuleDef]
    pretrade: PretradeConfig = PretradeConfig()
    system: SystemRiskConfig = SystemRiskConfig()

    def rule(self, rule_id: str) -> RuleDef | None:
        for r in self.rules:
            if r.id == rule_id:
                return r
        return None
