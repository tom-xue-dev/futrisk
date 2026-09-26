"""Declarative risk rules compiled from ``risk_rules.yaml`` (01 §8.9, §8.14; 02 §7.3 scope keys, §7.6 RuleSet).

A :class:`RuleDef` evaluates one metric per scope; every level has a threshold
and an action. This module turns the pydantic config into immutable
:class:`CompiledRule` objects with:

- the breach direction from ``op`` (``<=``/``<``: lower is worse, e.g. losses;
  ``>=``/``>``: higher is worse, e.g. drawdown, leverage);
- per-account overrides (``overrides: {ACC2: {halt: {threshold: -0.04}}}``)
  merged into the level list of that account;
- level defaults: circuit-break levels confirm with 2 distinct LIVE states or
  1s unless configured (01 §8.14 "熔断级别默认为 2 个状态或 1s，其他级别默认为 1");
- the clear line = threshold × (1 ∓ hysteresis), taken on the inner (safe)
  side of the breach direction, and the REARMED line after ``risk.reset``;
- the hard limit of a rule (first CIRCUIT_BREAK level, else the last RESTRICT,
  else the last level) and :func:`limit_utilization`, the one definition of
  "使用率 against the hard limit" (02 §7.6 ``RuleStatus.hard_utilization``,
  ``metric_samples.rule_util``).

Scope keys follow 02 §7.3: ``acct:{A}``, ``acct:{A}/prod:{p}``,
``acct:{A}/sector:{s}``, ``acct:{A}/inst:{i}``; firm scopes ``firm``,
``firm/prod:{p}``, ``firm/sector:{s}``.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Any

from futrisk.common.config.risk_rules import RiskRulesConfig, RuleDef, RuleLevel
from futrisk.messages.enums import Action, Severity

# 02 §7.3 scope kinds
SCOPE_ACCOUNT = "account"
SCOPE_ACCOUNT_PRODUCT = "account/product"
SCOPE_ACCOUNT_SECTOR = "account/sector"
SCOPE_ACCOUNT_INSTRUMENT = "account/instrument"
SCOPE_FIRM = "firm"
SCOPE_FIRM_PRODUCT = "firm/product"
SCOPE_FIRM_SECTOR = "firm/sector"
ACCOUNT_SCOPES = frozenset({SCOPE_ACCOUNT, SCOPE_ACCOUNT_PRODUCT, SCOPE_ACCOUNT_SECTOR, SCOPE_ACCOUNT_INSTRUMENT})
FIRM_SCOPES = frozenset({SCOPE_FIRM, SCOPE_FIRM_PRODUCT, SCOPE_FIRM_SECTOR})

# 01 §8.14 默认确认：熔断级别 2 个不同的 LIVE 状态或 1s；其他级别 1（立即确认）
DEFAULT_BREAKER_CONFIRM_STATES = 2
DEFAULT_BREAKER_CONFIRM_MS = 1000

SEVERITY_OF_ACTION: dict[Action, Severity] = {
    Action.ALERT: Severity.WARNING,
    Action.RESTRICT: Severity.HIGH,
    Action.CIRCUIT_BREAK: Severity.CRITICAL,
    Action.LIQUIDATE: Severity.CRITICAL,
}

# params of RESTRICT levels that block new opens (what-if "禁开级别", 02 §11.2 step 8)
BLOCKING_PARAMS = ("block_open", "block_open_largest_product")


def acct_scope(account_id: str) -> str:
    return f"acct:{account_id}"


def product_scope(account_id: str, product: str) -> str:
    return f"acct:{account_id}/prod:{product}"


def sector_scope(account_id: str, sector: str) -> str:
    return f"acct:{account_id}/sector:{sector}"


def instrument_scope(account_id: str, instrument_id: str) -> str:
    return f"acct:{account_id}/inst:{instrument_id}"


def firm_product_scope(product: str) -> str:
    return f"firm/prod:{product}"


def firm_sector_scope(sector: str) -> str:
    return f"firm/sector:{sector}"


@dataclass(frozen=True, slots=True)
class ScopeKey:
    """Parsed scope key: ``owner`` is ``acct:{A}`` or ``firm``; ``kind`` is None/prod/sector/inst."""

    owner: str
    kind: str | None
    value: str | None

    @property
    def account_id(self) -> str | None:
        return self.owner[5:] if self.owner.startswith("acct:") else None

    @property
    def is_firm(self) -> bool:
        return self.owner == "firm"


@lru_cache(maxsize=8192)
def parse_scope(scope_key: str) -> ScopeKey:
    """``acct:ACC3/prod:rb`` -> ScopeKey("acct:ACC3", "prod", "rb") (memoized: scope keys are few)."""
    owner, _, rest = scope_key.partition("/")
    if not rest:
        return ScopeKey(owner, None, None)
    kind, _, value = rest.partition(":")
    return ScopeKey(owner, kind, value)


@dataclass(frozen=True, slots=True)
class LevelSpec:
    """One level of a rule, after per-account overrides and defaults."""

    name: str
    index: int  # 0 = least severe (levels are ordered by severity in the config)
    threshold: float
    action: Action
    params: Mapping[str, Any]
    latch: bool
    confirm_states: int
    confirm_ms: int
    rearm_extra: float
    blocks_open: bool = False  # forbids new opens when active (circuit break, block_open*): what-if levels

    @property
    def severity(self) -> Severity:
        return SEVERITY_OF_ACTION[self.action]


@dataclass(frozen=True, slots=True)
class CompiledRule:
    """A rule ready for evaluation; levels are resolved per account."""

    rule_id: str
    metric: str
    op: str
    scope: str
    hysteresis: float
    cooldown_ms: int
    hold_ms: int
    pretrade: bool
    reset_on_day_roll: bool
    base_levels: tuple[LevelSpec, ...]
    overrides: Mapping[str, tuple[LevelSpec, ...]]
    # evaluate only while gross notional / equity of the owner >= this; below it: not breached
    min_gross_pct: float | None = None
    # the most lenient threshold of the levels (base / per account): some level is breached <=> breached(value, edge)
    base_edge: float | None = None
    override_edges: Mapping[str, float | None] = field(default_factory=dict)

    @property
    def lower_is_worse(self) -> bool:
        return self.op in ("<=", "<")

    @property
    def is_firm(self) -> bool:
        return self.scope in FIRM_SCOPES

    def levels_for(self, account_id: str | None) -> tuple[LevelSpec, ...]:
        if account_id is not None:
            lv = self.overrides.get(account_id)
            if lv is not None:
                return lv
        return self.base_levels

    def edge_for(self, account_id: str | None) -> float | None:
        """Most lenient threshold of the account's levels: ``any(breached(v, t_i)) == breached(v, edge)``.

        For a lower-is-worse rule (``<=`` / ``<``) the largest threshold, else the
        smallest (exact for any threshold order); None without levels.
        """
        if account_id is not None and account_id in self.override_edges:
            return self.override_edges[account_id]
        return self.base_edge

    def level(self, account_id: str | None, name: str) -> LevelSpec | None:
        for lv in self.levels_for(account_id):
            if lv.name == name:
                return lv
        return None

    def hard_level(self, account_id: str | None) -> LevelSpec | None:
        """The rule's hard limit for the account: the first CIRCUIT_BREAK level, else the last RESTRICT, else the last.

        Per-account overrides applied; None without levels (02 §7.6 ``RuleStatus.hard_level``).
        """
        levels = self.levels_for(account_id)
        for lv in levels:
            if lv.action == Action.CIRCUIT_BREAK:
                return lv
        for lv in reversed(levels):
            if lv.action == Action.RESTRICT:
                return lv
        return levels[-1] if levels else None

    # ---------------------------------------------------------- direction
    def breached(self, value: float, threshold: float) -> bool:
        """True when ``value`` is on or beyond ``threshold`` in the breach direction (``op``)."""
        if self.op == "<=":
            return value <= threshold
        if self.op == "<":
            return value < threshold
        if self.op == ">=":
            return value >= threshold
        return value > threshold

    def clear_line(self, threshold: float) -> float:
        """解除线 = 阈值 × (1 − hysteresis)，取越线方向的内侧 (01 §8.14).

        Written as ``threshold ∓ |threshold| × hysteresis`` so the line always
        lies on the safe side: −3% → −2.4% for losses, 0.025 → 0.02 for drawdown
        (hysteresis 0.2), 0.90 → 0.918 for the NAV floor (its own hysteresis 0.02;
        lower is worse, positive threshold). ``hysteresis`` is per rule.
        """
        band = abs(threshold) * self.hysteresis
        return threshold + band if self.lower_is_worse else threshold - band

    def beyond_clear(self, value: float, threshold: float) -> bool:
        """True when ``value`` is back on the safe side of the clear line."""
        line = self.clear_line(threshold)
        return value > line if self.lower_is_worse else value < line

    def rearm_line(self, threshold: float, value: float, extra: float) -> float:
        """REARMED line = min(threshold, value − extra), direction-aware (01 §8.14)."""
        if self.lower_is_worse:
            return min(threshold, value - extra)
        return max(threshold, value + extra)

    def worse(self, a: float, b: float) -> bool:
        """True when ``a`` is strictly worse than ``b`` in the breach direction."""
        return a < b if self.lower_is_worse else a > b

    def utilization(self, value: float, threshold: float) -> float:
        """Value ÷ threshold normalised so that larger is more dangerous (02 §7.6 RuleStatus.utilization).

        The distance to the NEXT level; the hard-limit utilization shown on
        heat maps and recorded in ``metric_samples.rule_util`` is
        :func:`limit_utilization` against :meth:`hard_level`.
        """
        if self.lower_is_worse:
            if threshold < 0:
                return max(0.0, value / threshold)
            if threshold > 0:
                return threshold / value if value > 0 else 9999.0
            return 0.0 if value > 0 else 1.0
        if threshold == 0:
            return 0.0 if value < 0 else 1.0
        return value / threshold


def limit_utilization(value: float | None, limit: float | None, op: str, baseline: float = 0.0) -> float | None:
    """Utilization of ``value`` against a limit, larger = more dangerous, 1 = at the limit (02 §7.6 hard_utilization).

    * ``>=`` / ``>``: ``value / limit``;
    * ``<=`` / ``<`` with a finite ``baseline`` b (the metric's neutral point:
      0 for losses, 1 for NAV): ``(b - value) / (b - limit)``;
    * ``<=`` / ``<`` with an infinite baseline (effective N): ``limit / value``,
      None when ``value <= 0``.

    Clamped at 0 (a profitable day is 0 % of the loss limit); None when the
    value or the limit is missing or not finite, or the denominator is 0. The
    same definition as the dashboard's ``utilization()`` (web/src/utils/risk.ts).
    """
    if value is None or limit is None or not math.isfinite(value) or not math.isfinite(limit):
        return None
    if op not in ("<=", "<"):
        if limit == 0:
            return None
        u = value / limit
    elif not math.isfinite(baseline):
        if value <= 0:
            return None
        u = limit / value
    else:
        denom = baseline - limit
        if denom == 0:
            return None
        u = (baseline - value) / denom
    if not math.isfinite(u):
        return None
    return max(0.0, u)


def _level_spec(lv: RuleLevel, index: int, override: Mapping[str, Any] | None) -> LevelSpec:
    data: dict[str, Any] = lv.model_dump()
    if override:
        data.update(override)
    level = RuleLevel.model_validate(data)
    if level.latch and level.action in (Action.ALERT, Action.RESTRICT):
        # 01 §8.14：告警与限制下单不锁存，指标恢复后自动解除
        raise ValueError(f"level {level.name}: latch is only allowed for CIRCUIT_BREAK / LIQUIDATE, not {level.action.value}")
    confirm_set = "confirm" in lv.model_fields_set or bool(override and "confirm" in override)
    states, ms = level.confirm.states, level.confirm.ms
    if level.action == Action.CIRCUIT_BREAK and not confirm_set:
        states, ms = DEFAULT_BREAKER_CONFIRM_STATES, DEFAULT_BREAKER_CONFIRM_MS
    if states <= 0 and ms <= 0:
        # confirm.states = 0 表示只按时间确认：必须给出 confirm.ms
        raise ValueError(f"level {level.name}: confirm.states = 0 (time-only confirmation) needs confirm.ms > 0")
    return LevelSpec(
        name=level.name,
        index=index,
        threshold=float(level.threshold),
        action=level.action,
        params=dict(level.params),
        latch=level.latch,
        confirm_states=max(0, states),  # 0: time-only confirmation (confirm.ms)
        confirm_ms=max(0, ms),
        rearm_extra=float(level.rearm.extra) if level.rearm is not None else 0.01,
        blocks_open=level.action == Action.CIRCUIT_BREAK
        or (level.action == Action.RESTRICT and any(bool(level.params.get(p)) for p in BLOCKING_PARAMS)),
    )


def compile_rule(rule: RuleDef) -> CompiledRule:
    base = tuple(_level_spec(lv, i, None) for i, lv in enumerate(rule.levels))
    names = {lv.name for lv in rule.levels}
    overrides: dict[str, tuple[LevelSpec, ...]] = {}
    for account_id, per_level in sorted(rule.overrides.items()):
        unknown = sorted(set(per_level) - names)
        if unknown:
            raise ValueError(f"rule {rule.id}: override for {account_id} names unknown levels {unknown}")
        overrides[account_id] = tuple(_level_spec(lv, i, per_level.get(lv.name)) for i, lv in enumerate(rule.levels))
    lower_is_worse = rule.op in ("<=", "<")

    def edge(levels: tuple[LevelSpec, ...]) -> float | None:
        if not levels:
            return None
        ts = [lv.threshold for lv in levels]
        return max(ts) if lower_is_worse else min(ts)

    return CompiledRule(
        rule_id=rule.id,
        metric=rule.metric,
        op=rule.op,
        scope=rule.scope,
        hysteresis=float(rule.hysteresis),
        cooldown_ms=int(rule.cooldown_s) * 1000,
        hold_ms=int(rule.hold_ms),
        pretrade=rule.pretrade,
        reset_on_day_roll=rule.reset_on_day_roll,
        base_levels=base,
        overrides=overrides,
        min_gross_pct=None if rule.min_gross_pct is None else float(rule.min_gross_pct),
        base_edge=edge(base),
        override_edges={a: edge(lv) for a, lv in overrides.items()},
    )


class RuleBook:
    """All enabled rules of a :class:`RiskRulesConfig`, with the config hash (02 §7.6 RuleSet)."""

    def __init__(self, config: RiskRulesConfig, *, config_hash: str = "", config_version: int = 0) -> None:
        self.config = config
        self.config_version = config_version
        self.config_hash = config_hash or rules_hash(config)
        self.rules: tuple[CompiledRule, ...] = tuple(compile_rule(r) for r in config.rules if r.enabled)
        self._by_id = {r.rule_id: r for r in self.rules}
        for r in self.rules:
            if r.scope not in ACCOUNT_SCOPES and r.scope not in FIRM_SCOPES:
                raise ValueError(f"rule {r.rule_id}: unknown scope {r.scope!r}")
        # precomputed views (hot path: every evaluation and every pre-trade check)
        self._account = tuple(r for r in self.rules if not r.is_firm)
        self._firm = tuple(r for r in self.rules if r.is_firm)
        self._pretrade = tuple(r for r in self.rules if r.pretrade and not r.is_firm)

    def get(self, rule_id: str) -> CompiledRule | None:
        return self._by_id.get(rule_id)

    def account_rules(self) -> tuple[CompiledRule, ...]:
        return self._account

    def firm_rules(self) -> tuple[CompiledRule, ...]:
        return self._firm

    def pretrade_rules(self) -> tuple[CompiledRule, ...]:
        return self._pretrade

    def rule_dicts(self) -> list[dict[str, Any]]:
        """RuleDef dicts for ``risk.query.rules`` (02 §7.6 RuleSet.rules)."""
        return [r.model_dump(mode="json") for r in self.config.rules]


def rules_hash(config: RiskRulesConfig) -> str:
    """Stable hash of the rule configuration when no file hash is provided."""
    text = json.dumps(config.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def alert_key(trading_day: str, rule_id: str, level: str, scope_key: str, episode: int) -> str:
    """``{td}|{rule_id}|{level}|{scope_key}|{episode}`` (02 §7.3)."""
    return f"{trading_day}|{rule_id}|{level}|{scope_key}|{episode}"


def split_alert_key(key: str) -> tuple[str, str, str, str, int]:
    td, rule_id, level, scope_key, episode = key.split("|")
    return td, rule_id, level, scope_key, int(episode)


def sorted_levels(levels: Iterable[LevelSpec]) -> list[LevelSpec]:
    return sorted(levels, key=lambda lv: lv.index)


__all__ = [
    "ACCOUNT_SCOPES",
    "FIRM_SCOPES",
    "SCOPE_ACCOUNT",
    "SCOPE_ACCOUNT_INSTRUMENT",
    "SCOPE_ACCOUNT_PRODUCT",
    "SCOPE_ACCOUNT_SECTOR",
    "SCOPE_FIRM",
    "SCOPE_FIRM_PRODUCT",
    "SCOPE_FIRM_SECTOR",
    "SEVERITY_OF_ACTION",
    "CompiledRule",
    "LevelSpec",
    "RuleBook",
    "ScopeKey",
    "acct_scope",
    "alert_key",
    "compile_rule",
    "firm_product_scope",
    "firm_sector_scope",
    "instrument_scope",
    "limit_utilization",
    "parse_scope",
    "product_scope",
    "rules_hash",
    "sector_scope",
    "sorted_levels",
    "split_alert_key",
]
