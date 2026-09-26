"""Rule evaluation shared by accounts and the firm (01 §8.14; 02 §7.6 RuleStatus; 03 §4.3 rule_states / alerts).

For one owner (an account, or the firm on the coordinator) and one valuation:

1. :func:`scope_targets` expands each rule into scope keys: ``account`` →
   ``acct:{A}``; ``account/product`` → every product with exposure; sector and
   instrument likewise; firm scopes the same way on the firm valuation. Scopes
   whose machines are not OK are always kept so that they can clear;
2. :func:`evaluate_owner` computes the metric per scope (:func:`rule_value`:
   a rule with ``min_gross_pct`` is "not applicable" - value None, i.e. not
   breached, clearing through the normal path - while the owner's gross
   notional / equity is below it), gates on the scope's market-data quality
   and steps one :class:`LevelMachine` per level (machines are created lazily
   on the first breach; an escalation to a more severe level bypasses the
   cooldown of the lower ones);
3. :func:`persist_transitions` turns transitions into ``rule_states`` rows,
   ``alerts`` rows and ``ALERT_RAISED`` / ``ALERT_CLEARED`` risk events - one
   ALERT_RAISED per episode (02 §7.3 RiskEventKind);
4. :func:`rule_statuses` builds the ``RuleStatus`` list of the snapshot: the
   utilization against the next level (``utilization``) and against the rule's
   hard limit (``hard_utilization`` / ``hard_level``, :func:`limit_utilization`
   with the metric's baseline - the one definition shared with the dashboard
   and ``metric_samples.rule_util``).
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from typing import Any

from futrisk.core.actions import is_effective
from futrisk.core.alerts import ALERT_ACTIVE, ALERT_CLEARED, AlertRecord, EventLog, fmt_value
from futrisk.core.effects import Tx
from futrisk.core.metrics import (
    RATIO_CAP,
    AccountValuation,
    get_metric,
    metric_baseline,
    metric_value,
    money_headroom,
)
from futrisk.core.rules import (
    CompiledRule,
    LevelSpec,
    acct_scope,
    firm_product_scope,
    firm_sector_scope,
    instrument_scope,
    limit_utilization,
    parse_scope,
    product_scope,
    sector_scope,
)
from futrisk.core.state_machine import LevelMachine, Tr
from futrisk.messages.enums import Action, MdQuality, RiskEventKind, RuleSmState
from futrisk.messages.risk import RuleStatus

MachineKey = tuple[str, str, str]  # (rule_id, level, scope_key)


@dataclass(frozen=True, slots=True)
class LevelEval:
    machine: LevelMachine
    transitions: tuple[Tr, ...]
    value: float | None


def scope_targets(
    rule: CompiledRule, account_id: str | None, valuation: AccountValuation, existing: Iterable[str] = ()
) -> list[tuple[str, str | None]]:
    """``(scope_key, key)`` pairs to evaluate for ``rule`` (key = product / sector / instrument or None)."""
    scope = rule.scope
    if account_id is not None and not existing:
        # 常见情形（账户范围、没有未恢复的实例）：直接生成，结果与下面的通用路径相同
        if scope == "account":
            return [(acct_scope(account_id), None)]
        if scope == "account/product":
            items: list[tuple[str, str | None]] = [(product_scope(account_id, p), p) for p in valuation.exposures]
            return sorted(items) if len(items) > 1 else items
        if scope == "account/sector":
            items = [(sector_scope(account_id, s), s) for s in valuation.sectors]
            return sorted(items) if len(items) > 1 else items
    out: dict[str, str | None] = {}
    if scope == "account":
        if account_id is not None:
            out[acct_scope(account_id)] = None
    elif scope == "firm":
        out["firm"] = None
    elif scope in ("account/product", "firm/product"):
        for prod in valuation.exposures:
            out[product_scope(account_id, prod) if account_id else firm_product_scope(prod)] = prod
    elif scope in ("account/sector", "firm/sector"):
        for sec in valuation.sectors:
            out[sector_scope(account_id, sec) if account_id else firm_sector_scope(sec)] = sec
    elif scope == "account/instrument" and account_id is not None:
        for p in valuation.positions:
            out[instrument_scope(account_id, p.instrument_id)] = p.instrument_id
    for sk in existing:
        if sk not in out:
            out[sk] = parse_scope(sk).value
    if len(out) <= 1:
        return list(out.items())
    return sorted(out.items())


def _nonok_scopes(machines: dict[MachineKey, LevelMachine]) -> dict[str, list[str]]:
    """rule_id -> scope keys that have a machine not in OK (index built once per pass)."""
    out: dict[str, list[str]] = {}
    for (rid, _lv, sk), m in machines.items():
        if m.state != RuleSmState.OK:
            out.setdefault(rid, []).append(sk)
    return out


def _quality_ok(valuation: AccountValuation, scope_key: str, key: str | None) -> bool:
    return valuation.quality_for(parse_scope(scope_key).kind, key) == MdQuality.OK


def gross_pct(valuation: AccountValuation) -> float:
    """Gross notional / equity of the owner (the ``min_gross_pct`` gate)."""
    gross = valuation.metrics.gross_notional
    equity = valuation.funds.equity
    if equity > 0:
        return gross / equity
    return 0.0 if gross == 0 else RATIO_CAP


_METRIC_FNS: dict[str, Callable[[Any, str | None], float | None]] = {}


def _metric_fn(name: str) -> Callable[[Any, str | None], float | None]:
    fn = _METRIC_FNS.get(name)
    if fn is None:
        fn = _METRIC_FNS[name] = get_metric(name).fn
    return fn


def rule_value(rule: CompiledRule, valuation: AccountValuation, key: str | None) -> float | None:
    """The rule's metric for one scope; None ("not applicable" = not breached) below ``min_gross_pct``."""
    if rule.min_gross_pct is not None and gross_pct(valuation) < rule.min_gross_pct:
        return None  # 小账本不算集中度风险：视为未越线，状态机按正常路径解除（不是质量冻结）
    return metric_value(rule.metric, valuation, key)


def evaluate_owner(
    rules: Sequence[CompiledRule],
    machines: dict[MachineKey, LevelMachine],
    valuation: AccountValuation,
    *,
    account_id: str | None,
    state_key: int | None,
    now_ns: int,
) -> list[LevelEval]:
    """Step every machine of ``rules`` for one owner; returns the machines that changed.

    Fast path: a rule without any machine of this owner can only create one
    when a level is breached, so for such a rule a scope whose value breaches
    no level (one comparison with the rule's most lenient threshold,
    :meth:`CompiledRule.edge_for`) is skipped - exactly what the full loop would
    do: no machine, no flag, nothing to step. Everything else takes the full
    path; the order of the returned evaluations is unchanged.
    """
    out: list[LevelEval] = []
    nonok = _nonok_scopes(machines) if machines else {}
    with_machines = {k[0] for k in machines} if machines else set()
    gross: float | None = None
    for rule in rules:
        if rule.min_gross_pct is not None:
            if gross is None:
                gross = gross_pct(valuation)
            gated = gross < rule.min_gross_pct
        else:
            gated = False
        fn = _metric_fn(rule.metric)
        if rule.rule_id not in with_machines:
            edge = rule.edge_for(account_id)
            if edge is None or gated:
                continue  # 没有实例、不可能越线（或低于 min_gross_pct = 不适用）
            breached = rule.breached
            if account_id is not None and rule.scope == "account":
                value = fn(valuation, None)
                if value is not None and breached(value, edge):
                    _step_scope(out, rule, machines, valuation, acct_scope(account_id), None, value,
                                account_id=account_id, state_key=state_key, now_ns=now_ns)
                continue
            for scope_key, key in scope_targets(rule, account_id, valuation, ()):
                value = fn(valuation, key)
                if value is not None and breached(value, edge):
                    _step_scope(out, rule, machines, valuation, scope_key, key, value,
                                account_id=account_id, state_key=state_key, now_ns=now_ns)
            continue
        for scope_key, key in scope_targets(rule, account_id, valuation, nonok.get(rule.rule_id, ())):
            value = None if gated else fn(valuation, key)
            _step_scope(out, rule, machines, valuation, scope_key, key, value,
                        account_id=account_id, state_key=state_key, now_ns=now_ns)
    return out


def _step_scope(
    out: list[LevelEval],
    rule: CompiledRule,
    machines: dict[MachineKey, LevelMachine],
    valuation: AccountValuation,
    scope_key: str,
    key: str | None,
    value: float | None,
    *,
    account_id: str | None,
    state_key: int | None,
    now_ns: int,
) -> None:
    """Step the level machines of one rule and scope (created lazily on the first breach)."""
    levels = rule.levels_for(account_id)
    q_ok = _quality_ok(valuation, scope_key, key)
    flags = [value is not None and rule.breached(value, lv.threshold) for lv in levels]
    for i, lv in enumerate(levels):
        mk = (rule.rule_id, lv.name, scope_key)
        m = machines.get(mk)
        if m is None:
            if not flags[i]:
                continue  # 未越线且无状态：不必创建状态机
            m = LevelMachine(rule, lv, scope_key)
            machines[mk] = m
        else:
            m.rule, m.level = rule, lv  # 热加载：阈值立即生效
        escalating = any(flags[i + 1 :])
        trs = m.evaluate(value, quality_ok=q_ok, state_key=state_key, now_ns=now_ns, escalating=escalating)
        if trs:
            out.append(LevelEval(m, tuple(trs), value))


def reevaluate_due(
    machines: dict[MachineKey, LevelMachine], valuation: AccountValuation | None, now_ns: int
) -> list[LevelEval]:
    """Timer pass: re-check machines whose confirm / hold / cooldown timer is due."""
    out: list[LevelEval] = []
    for m in list(machines.values()):
        due = m.due_ns()
        if due is None or due > now_ns:
            continue
        key = parse_scope(m.scope_key).value
        q_ok = valuation is None or _quality_ok(valuation, m.scope_key, key)
        escalating = False
        for other in machines.values():
            if (
                other.rule.rule_id == m.rule.rule_id
                and other.scope_key == m.scope_key
                and other.level.index > m.level.index
                and other.in_breach(m.st.last_value)
            ):
                escalating = True
        trs = m.reevaluate(now_ns, quality_ok=q_ok, escalating=escalating)
        if trs:
            out.append(LevelEval(m, tuple(trs), m.st.last_value))
    return out


def next_due_ns(machines: Iterable[LevelMachine], valuation: AccountValuation | None = None) -> int | None:
    """Earliest timer of ``machines``; quality-gated machines are skipped (they cannot move, 01 §8.14)."""
    best: int | None = None
    for m in machines:
        d = m.due_ns()
        if d is None or (best is not None and d >= best):
            continue
        if valuation is not None and not _quality_ok(valuation, m.scope_key, parse_scope(m.scope_key).value):
            continue
        best = d
    return best


def alert_message(owner: str, m: LevelMachine, value: float | None, *, activated: bool) -> str:
    lv = m.level
    act = {
        Action.ALERT: "告警",
        Action.RESTRICT: "限制下单",
        Action.CIRCUIT_BREAK: "熔断禁开",
        Action.LIQUIDATE: "生成减仓计划",
    }[lv.action]
    metric = m.rule.metric
    if activated:
        latched = "（已锁存）" if m.st.latched else ""
        return (
            f"{owner} {m.rule.rule_id}/{lv.name} 触发：{metric}={fmt_value(metric, value)} {m.rule.op} "
            f"{fmt_value(metric, lv.threshold)}，{act}{latched}"
        )
    return f"{owner} {m.rule.rule_id}/{lv.name} 解除：{metric}={fmt_value(metric, value)}"


def persist_transitions(
    tx: Tx,
    evals: Sequence[LevelEval],
    *,
    events: EventLog,
    alerts: dict[str, AlertRecord],
    trading_day: str,
    account_id: str | None,
    config_hash: str,
    now_ns: int,
) -> list[LevelEval]:
    """Rows and events for machine transitions; returns the evaluations that activated a level."""
    activated: list[LevelEval] = []
    owner = account_id or "firm"
    for ev in evals:
        m = ev.machine
        tx.upsert("rule_states", ("trading_day", "rule_id", "level", "scope_key"), m.to_row(trading_day, config_hash, now_ns))
        if Tr.ACTIVATED in ev.transitions:
            key = m.alert_key(trading_day)
            value = ev.value if ev.value is not None else m.level.threshold
            rec = AlertRecord(
                alert_key=key,
                trading_day=trading_day,
                rule_id=m.rule.rule_id,
                level=m.level.name,
                scope_key=m.scope_key,
                episode=m.st.episode,
                account_id=account_id,
                action=m.level.action,
                severity=m.level.severity,
                metric=m.rule.metric,
                value=value,
                threshold=m.level.threshold,
                latched=m.st.latched,
                status=ALERT_ACTIVE,
                raised_ns=now_ns,
                message=alert_message(owner, m, ev.value, activated=True),
            )
            alerts[key] = rec
            rec.persist_new(tx)
            events.emit(
                tx,
                kind=RiskEventKind.ALERT_RAISED,
                trading_day=trading_day,
                ts_ns=now_ns,
                message=rec.message,
                severity=rec.severity,
                account_id=account_id,
                scope_key=m.scope_key,
                alert_key=key,
                rule_id=m.rule.rule_id,
                level=m.level.name,
                episode=m.st.episode,
                action=m.level.action,
                latched=m.st.latched,
                metric=m.rule.metric,
                value=value,
                threshold=m.level.threshold,
            )
            if m.st.latched:
                tx.mark_critical()
            activated.append(ev)
        if Tr.CLEARED in ev.transitions:
            ep = m.st.episode
            key = m.alert_key(trading_day, ep)
            rec_c = alerts.get(key)
            if rec_c is not None:
                rec_c.status = ALERT_CLEARED
                rec_c.cleared_ns = now_ns
                rec_c.persist_status(tx)
            events.emit(
                tx,
                kind=RiskEventKind.ALERT_CLEARED,
                trading_day=trading_day,
                ts_ns=now_ns,
                message=alert_message(owner, m, ev.value, activated=False),
                severity=m.level.severity,
                account_id=account_id,
                scope_key=m.scope_key,
                alert_key=key,
                rule_id=m.rule.rule_id,
                level=m.level.name,
                episode=ep,
                action=m.level.action,
                metric=m.rule.metric,
                value=ev.value,
                threshold=m.level.threshold,
            )
    return activated


def _next_level(levels: Sequence[LevelSpec], machines: dict[MachineKey, LevelMachine], rule_id: str, scope_key: str) -> LevelSpec | None:
    for lv in levels:
        m = machines.get((rule_id, lv.name, scope_key))
        if m is None or m.state in (RuleSmState.OK, RuleSmState.REARMED):
            return lv
    return None


def rule_statuses(
    rules: Sequence[CompiledRule],
    machines: dict[MachineKey, LevelMachine],
    valuation: AccountValuation,
    *,
    account_id: str | None,
) -> list[RuleStatus]:
    """``RuleStatus`` per rule and scope (02 §7.6)."""
    out: list[RuleStatus] = []
    nonok = _nonok_scopes(machines)
    for rule in rules:
        levels = rule.levels_for(account_id)
        hard = rule.hard_level(account_id)
        baseline = metric_baseline(rule.metric)
        for scope_key, key in scope_targets(rule, account_id, valuation, nonok.get(rule.rule_id, ())):
            q_ok = _quality_ok(valuation, scope_key, key)
            raw = rule_value(rule, valuation, key)
            value = raw if q_ok else None
            active: LevelSpec | None = None
            top_state = RuleSmState.OK
            for lv in levels:
                m = machines.get((rule.rule_id, lv.name, scope_key))
                if m is None:
                    continue
                if is_effective(lv.action, m.state):
                    active = lv
                if m.state != RuleSmState.OK:
                    top_state = m.state
            nxt = _next_level(levels, machines, rule.rule_id, scope_key)
            ref = nxt if nxt is not None else (levels[-1] if levels else None)
            util = rule.utilization(value, ref.threshold) if (value is not None and ref is not None) else None
            headroom = (
                money_headroom(rule.metric, valuation, value, nxt.threshold)
                if (value is not None and nxt is not None and not rule.breached(value, nxt.threshold))
                else None
            )
            out.append(
                RuleStatus(
                    rule_id=rule.rule_id,
                    scope_key=scope_key,
                    metric=rule.metric,
                    value=value,
                    active_level=active.name if active is not None else None,
                    sm_state=top_state,
                    next_level=nxt.name if nxt is not None else None,
                    next_threshold=nxt.threshold if nxt is not None else None,
                    utilization=util,
                    headroom_money=headroom,
                    hard_utilization=limit_utilization(value, hard.threshold, rule.op, baseline) if hard else None,
                    hard_level=hard.name if hard is not None else None,
                )
            )
    return out


__all__ = [
    "LevelEval",
    "MachineKey",
    "alert_message",
    "evaluate_owner",
    "gross_pct",
    "next_due_ns",
    "persist_transitions",
    "reevaluate_due",
    "rule_statuses",
    "rule_value",
    "scope_targets",
]
