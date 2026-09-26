"""Topic names and reliability classes (02 §4.4, §5).

Syntax: ``domain "." category ["." key]``; keys never contain ``.``. ZeroMQ
filters by prefix, so subscribers must re-check the full topic (``matches``).
"""

from __future__ import annotations

LV = "LV"
FACT = "FACT"

# topic templates -> reliability class
MD_TICK = "md.tick."
MD_BAR_1M = "md.bar.1m."
MD_STATUS = "md.status"
MD_EVENT = "md.event"
TD_EXEC = "td.exec."
TD_STATUS = "td.status"
RISK_SNAPSHOT = "risk.snapshot."
RISK_STATE = "risk.state."
RISK_EVENT = "risk.event"
OMS_ORDER = "oms.order."
RISK_CASH = "risk.cash."
RISK_FIRM = "risk.firm"
RISK_CONTROL_FIRM = "risk.control.firm"
SYS_DAYROLL = "sys.dayroll"
SYS_EOD = "sys.eod"
CLEARING_SETTLED = "clearing.settled"
REPORT_READY = "report.ready"
BACKTEST_STATUS = "backtest.status"
SYS_SERVICE = "sys.service"
SYS_HEARTBEAT = "sys.heartbeat."
SYS_CLOCK = "sys.clock"

_FACT_PREFIXES = (
    MD_EVENT,
    TD_EXEC,
    RISK_STATE,
    RISK_EVENT,
    OMS_ORDER,
    RISK_CASH,
    RISK_CONTROL_FIRM,
    SYS_DAYROLL,
    SYS_EOD,
    CLEARING_SETTLED,
    REPORT_READY,
    SYS_SERVICE,
)


def reliability(topic: str) -> str:
    """Return ``FACT`` or ``LV`` for a concrete topic."""
    for p in _FACT_PREFIXES:
        if p.endswith("."):
            if topic.startswith(p):
                return FACT
        elif topic == p:
            return FACT
    return LV


def is_fact(topic: str) -> bool:
    return reliability(topic) == FACT


def md_tick(instrument_id: str) -> str:
    return f"{MD_TICK}{instrument_id}"


def md_bar_1m(instrument_id: str) -> str:
    return f"{MD_BAR_1M}{instrument_id}"


def td_exec(account_id: str) -> str:
    return f"{TD_EXEC}{account_id}"


def risk_snapshot(account_id: str) -> str:
    return f"{RISK_SNAPSHOT}{account_id}"


def risk_state(account_id: str) -> str:
    return f"{RISK_STATE}{account_id}"


def oms_order(account_id: str) -> str:
    return f"{OMS_ORDER}{account_id}"


def risk_cash(account_id: str) -> str:
    return f"{RISK_CASH}{account_id}"


def sys_heartbeat(instance: str) -> str:
    return f"{SYS_HEARTBEAT}{instance}"


def key_of(topic: str, prefix: str) -> str:
    """Key part of a keyed topic, e.g. ``key_of("td.exec.ACC3", TD_EXEC) == "ACC3"``."""
    if not topic.startswith(prefix):
        raise ValueError(f"{topic!r} does not start with {prefix!r}")
    return topic[len(prefix) :]


def matches(topic: str, subscription: str) -> bool:
    """Exact-match check after ZeroMQ prefix filtering (02 §4.4).

    A subscription ending with ``.`` is a prefix and matches every topic below it
    (``risk.`` matches ``risk.snapshot.ACC1``). Any other subscription is a full
    topic and must match exactly, so ``td.exec.ACC1`` does not receive
    ``td.exec.ACC10``.
    """
    if subscription.endswith("."):
        return topic.startswith(subscription)
    return topic == subscription
