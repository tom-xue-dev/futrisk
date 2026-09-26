"""Alerts, risk events and audit records (03 §4.3 ``alerts`` / ``risk_events`` / ``audit_events``; 02 §7.6 RiskEvent).

- one :class:`AlertRecord` per episode, keyed by ``alert_key`` (``ON CONFLICT DO
  NOTHING`` on creation, then status updates ``ACTIVE → CLEARED | RESET |
  ROLLED``; ``acked_*`` set by ``risk.ack_alert`` without touching the state
  machine);
- every :class:`RiskEvent` is written to ``risk_events`` and to the outbox of
  ``risk.event`` in the same transaction; ``event_id`` comes from the injected
  id generator so backtests are reproducible;
- manual interventions append an ``audit_events`` row (actor, role, before /
  after, reason, command id; 01 §8.8).
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from futrisk.common.ids import IdGen
from futrisk.core.effects import Tx
from futrisk.messages import topics
from futrisk.messages.enums import Action, RiskEventKind, Role, Severity
from futrisk.messages.risk import RiskEvent

# AlertStatus (03 §3.3, storage only)
ALERT_ACTIVE = "ACTIVE"
ALERT_CLEARED = "CLEARED"
ALERT_RESET = "RESET"
ALERT_ROLLED = "ROLLED"

# AuditAction (03 §3.3, storage only)
AUDIT_RISK_RESET = "RISK_RESET"
AUDIT_RISK_HALT = "RISK_HALT"
AUDIT_ALERT_ACK = "ALERT_ACK"
AUDIT_KILL_SWITCH_ON = "KILL_SWITCH_ON"
AUDIT_KILL_SWITCH_OFF = "KILL_SWITCH_OFF"
AUDIT_FIRM_RESTRICTIONS = "FIRM_RESTRICTIONS"
AUDIT_CASH_MOVE = "CASH_MOVE"
AUDIT_LIQUIDATION_CONFIRM = "LIQUIDATION_CONFIRM"
AUDIT_LIQUIDATION_CANCEL = "LIQUIDATION_CANCEL"
AUDIT_EOD_FORCE_UNFREEZE = "EOD_FORCE_UNFREEZE"
AUDIT_RECON_RESOLVE = "RECON_RESOLVE"
AUDIT_DAYROLL = "DAYROLL"
AUDIT_DAYROLL_REBASE = "DAYROLL_REBASE"
AUDIT_PROVISIONAL_SOD = "PROVISIONAL_SOD"
AUDIT_CONFIG_COMMIT = "CONFIG_COMMIT"


def _json(value: Any) -> Any:
    if isinstance(value, str):
        return json.loads(value) if value else {}
    return value if value is not None else {}


@dataclass
class AlertRecord:
    """One ``alerts`` row."""

    alert_key: str
    trading_day: str
    rule_id: str
    level: str
    scope_key: str
    episode: int
    account_id: str | None
    action: Action
    severity: Severity
    metric: str
    value: float
    threshold: float
    latched: bool
    status: str
    raised_ns: int
    message: str
    cleared_ns: int | None = None
    reset_ns: int | None = None
    acked_ns: int | None = None
    reset_by: str | None = None
    acked_by: str | None = None
    detail: dict[str, Any] = field(default_factory=dict)

    def row(self) -> dict[str, Any]:
        return {
            "alert_key": self.alert_key,
            "trading_day": self.trading_day,
            "rule_id": self.rule_id,
            "level": self.level,
            "scope_key": self.scope_key,
            "episode": self.episode,
            "account_id": self.account_id,
            "action": self.action.value,
            "severity": self.severity.value,
            "metric": self.metric,
            "value": self.value,
            "threshold": self.threshold,
            "latched": self.latched,
            "status": self.status,
            "raised_ns": self.raised_ns,
            "cleared_ns": self.cleared_ns,
            "reset_ns": self.reset_ns,
            "acked_ns": self.acked_ns,
            "reset_by": self.reset_by,
            "acked_by": self.acked_by,
            "message": self.message,
            "detail": dict(self.detail),
        }

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> AlertRecord:
        return cls(
            alert_key=str(row["alert_key"]),
            trading_day=str(row["trading_day"]),
            rule_id=str(row["rule_id"]),
            level=str(row["level"]),
            scope_key=str(row["scope_key"]),
            episode=int(row["episode"]),
            account_id=row.get("account_id"),
            action=Action(str(row["action"])),
            severity=Severity(str(row["severity"])),
            metric=str(row["metric"]),
            value=float(row["value"]),
            threshold=float(row["threshold"]),
            latched=bool(row["latched"]),
            status=str(row["status"]),
            raised_ns=int(row["raised_ns"]),
            message=str(row.get("message") or ""),
            cleared_ns=row.get("cleared_ns"),
            reset_ns=row.get("reset_ns"),
            acked_ns=row.get("acked_ns"),
            reset_by=row.get("reset_by"),
            acked_by=row.get("acked_by"),
            detail=dict(_json(row.get("detail"))),
        )

    def persist_new(self, tx: Tx) -> None:
        tx.insert_ignore("alerts", ("alert_key",), self.row())

    def persist_status(self, tx: Tx) -> None:
        tx.update(
            "alerts",
            ("alert_key",),
            {
                "alert_key": self.alert_key,
                "status": self.status,
                "cleared_ns": self.cleared_ns,
                "reset_ns": self.reset_ns,
                "acked_ns": self.acked_ns,
                "reset_by": self.reset_by,
                "acked_by": self.acked_by,
            },
        )


def event_row(ev: RiskEvent) -> dict[str, Any]:
    """``risk_events`` row of a RiskEvent (03 §4.3)."""
    return {
        "event_id": ev.event_id,
        "kind": ev.kind.value,
        "trading_day": ev.trading_day,
        "ts_ns": ev.ts_ns,
        "account_id": ev.account_id,
        "scope_key": ev.scope_key,
        "alert_key": ev.alert_key,
        "rule_id": ev.rule_id,
        "level": ev.level,
        "episode": ev.episode,
        "action": ev.action.value if ev.action is not None else None,
        "severity": ev.severity.value,
        "latched": ev.latched,
        "metric": ev.metric,
        "value": ev.value,
        "threshold": ev.threshold,
        "actor": ev.actor,
        "message": ev.message,
        "detail": dict(ev.detail),
    }


def event_from_row(row: dict[str, Any]) -> RiskEvent:
    action = row.get("action")
    return RiskEvent(
        event_id=str(row["event_id"]),
        kind=RiskEventKind(str(row["kind"])),
        trading_day=str(row["trading_day"]),
        ts_ns=int(row["ts_ns"]),
        account_id=row.get("account_id"),
        scope_key=row.get("scope_key"),
        alert_key=row.get("alert_key"),
        rule_id=row.get("rule_id"),
        level=row.get("level"),
        episode=row.get("episode"),
        action=Action(str(action)) if action is not None else None,
        severity=Severity(str(row.get("severity") or "INFO")),
        latched=None if row.get("latched") is None else bool(row.get("latched")),
        metric=row.get("metric"),
        value=row.get("value"),
        threshold=row.get("threshold"),
        actor=row.get("actor"),
        message=str(row.get("message") or ""),
        detail=dict(_json(row.get("detail"))),
    )


class EventLog:
    """Risk events of the current trading day (``risk.query.alerts`` from memory)."""

    def __init__(self, ids: IdGen) -> None:
        self._ids = ids
        self.events: list[RiskEvent] = []

    def emit(
        self,
        tx: Tx,
        *,
        kind: RiskEventKind,
        trading_day: str,
        ts_ns: int,
        message: str,
        severity: Severity = Severity.INFO,
        account_id: str | None = None,
        scope_key: str | None = None,
        alert_key: str | None = None,
        rule_id: str | None = None,
        level: str | None = None,
        episode: int | None = None,
        action: Action | None = None,
        latched: bool | None = None,
        metric: str | None = None,
        value: float | None = None,
        threshold: float | None = None,
        actor: str | None = None,
        detail: dict[str, Any] | None = None,
    ) -> RiskEvent:
        ev = RiskEvent(
            event_id=self._ids.next_id(),
            kind=kind,
            trading_day=trading_day,
            ts_ns=ts_ns,
            account_id=account_id,
            scope_key=scope_key,
            alert_key=alert_key,
            rule_id=rule_id,
            level=level,
            episode=episode,
            action=action,
            severity=severity,
            latched=latched,
            metric=metric,
            value=value,
            threshold=threshold,
            actor=actor,
            message=message,
            detail=detail or {},
        )
        tx.insert_ignore("risk_events", ("event_id",), event_row(ev))
        tx.emit(topics.RISK_EVENT, ev, ts_ns=ts_ns, corr_id=alert_key or ev.event_id, td=trading_day)
        self.events.append(ev)
        return ev

    def of_day(self, trading_day: str, account_id: str | None = None) -> list[RiskEvent]:
        return [
            e for e in self.events if e.trading_day == trading_day and (account_id is None or e.account_id == account_id)
        ]

    def clear_before(self, trading_day: str) -> None:
        self.events = [e for e in self.events if e.trading_day >= trading_day]


def audit(
    tx: Tx,
    *,
    ts_ns: int,
    trading_day: str,
    actor: str,
    action: str,
    target: str,
    role: Role | None = None,
    before: Any = None,
    after: Any = None,
    reason: str | None = None,
    command_id: str | None = None,
    corr_id: str | None = None,
) -> None:
    """Append an ``audit_events`` row (only-append, 03 §4.3)."""
    tx.insert(
        "audit_events",
        {
            "ts_ns": ts_ns,
            "trading_day": trading_day,
            "actor": actor,
            "role": role.value if role is not None else None,
            "action": action,
            "target": target,
            "before": before,
            "after": after,
            "reason": reason,
            "command_id": command_id,
            "corr_id": corr_id,
        },
    )


def fmt_value(metric: str, value: float | None) -> str:
    """Human readable metric value (percent for ratio-like metrics)."""
    if value is None:
        return "n/a"
    if any(t in metric for t in ("pct", "_dd", "risk_degree", "utilization", "limit_stress")):
        return f"{value * 100:.2f}%"
    return f"{value:.4g}"


__all__ = [
    "ALERT_ACTIVE",
    "ALERT_CLEARED",
    "ALERT_RESET",
    "ALERT_ROLLED",
    "AUDIT_ALERT_ACK",
    "AUDIT_CASH_MOVE",
    "AUDIT_CONFIG_COMMIT",
    "AUDIT_DAYROLL",
    "AUDIT_DAYROLL_REBASE",
    "AUDIT_EOD_FORCE_UNFREEZE",
    "AUDIT_FIRM_RESTRICTIONS",
    "AUDIT_KILL_SWITCH_OFF",
    "AUDIT_KILL_SWITCH_ON",
    "AUDIT_LIQUIDATION_CANCEL",
    "AUDIT_LIQUIDATION_CONFIRM",
    "AUDIT_PROVISIONAL_SOD",
    "AUDIT_RECON_RESOLVE",
    "AUDIT_RISK_HALT",
    "AUDIT_RISK_RESET",
    "AlertRecord",
    "EventLog",
    "audit",
    "event_from_row",
    "event_row",
    "fmt_value",
]
