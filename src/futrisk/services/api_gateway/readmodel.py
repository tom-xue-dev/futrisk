"""CQRS read model of api-gateway (01 §5.3.5, 02 §4.1, §4.6, §5).

Fed by Clone-pattern subscriptions (late-joiner snapshot + live updates, 02 §4.6)
to the summary topics of 02 §4.1 and by the heartbeat observer:

=====================  ==============================================================
publisher              topics
=====================  ==============================================================
md-gateway             ``md.tick.``, ``md.status``, ``md.event``
trade-gateway          ``td.exec.``, ``td.status``
risk-engine-k          ``risk.`` (snapshot/state/event/firm/control.firm/cash), ``oms.order.``, ``sys.dayroll``
clearing-worker        ``sys.eod``, ``clearing.settled``, ``report.ready``, ``backtest.status``
supervisor             ``sys.service``
every instance         ``sys.heartbeat.{instance}``
=====================  ==============================================================

The model keeps only the latest state (plus bounded rings of recent events)
in memory and can be rebuilt at any time from ``ctl.snapshot``. Every handler
runs on the event loop and never raises: a payload that cannot be decoded is
logged and counted (a bad message must not stop the dashboard).

Cross-topic rule 2 of 02 §5: the risk state, latch and restrictions of an
account come from whichever of ``risk.state`` / ``risk.snapshot`` has the larger
``state_version``; everything else comes from ``risk.snapshot``.

Liveness: account items carry ``stale`` (the owning shard is not ``UP``) and
``age_ms`` (milliseconds since the owning shard was last heard from), from the
``liveness_of`` hook set by the gateway, so data of a shard that is down is
never shown as live.

EOD runs (``sys.eod``): the latest phase event per trading day is kept
(``eod_phase``; a rewind to an earlier phase of the same version -- late facts
found at the settle step -- is accepted), every phase event is kept in the
``eod_events`` ring (repeated phases after a rewind included; only exact
re-deliveries are dropped), and WARN events (``detail.warning``, repeating the
run's current phase) never count as a phase change: they go to ``eod_warnings``
(change kind ``EOD_WARNING``), as do the ``detail.warnings`` items of a phase
event (``PRE_SETTLE_MISMATCH`` on ``PRICES_FIXED``).

Bounded memory: at most ``MAX_ORDERS_PER_ACCOUNT`` orders per account (oldest
terminal orders go first), only the current trading day's orders are merged
from ``risk.query.orders`` answers, ``MAX_RISK_EVENTS`` risk events and
``MAX_PLANS`` liquidation plans are kept.

Trading day: the shards are the authority. :meth:`ReadModel.shard_trading_day`
is the newest ``trading_day`` of the risk shards' heartbeats (live ones first),
else the newest ``sys.dayroll``. On a roll the risk events and state changes
are rotated (the new day and the day before are kept) and the orders of the
rolled accounts' previous days are dropped; equity curves restart with the
new day's snapshots.

State-change history: every ``AccountStateChanged`` (``risk.state.*``) is kept
in a bounded list next to the risk events; the ``events`` views show both,
each item tagged with ``type`` (``"risk_event"`` / ``"state_change"``), and a
state change carries the rule, level, value and threshold of the alert that
caused it (``cause_alert_key``).

Firm controls (``risk.control.firm``): a MANUAL ``KILL_SWITCH_ON/OFF`` sets the
manual layer; the coordinator's RULE layer arrives as one ``RULE_STATE`` fact
holding the complete rule state (``active`` = rule kill switch,
``restrictions`` = every firm restriction in force); the legacy RULE actions
(``KILL_SWITCH_ON/OFF``, ``SET_RESTRICTIONS``) are still applied. The
effective kill switch is MANUAL or RULE (:meth:`ReadModel.firm_control_state`).

Liquidation plans: the ``risk.event`` of kind ``LIQUIDATION_PLAN`` that creates
a plan carries it in ``detail.plan``; confirm / cancel / expiry at the day roll
emit the same kind with ``detail {plan_id, status, actor}``. Both update
``liquidation_plans`` (newest event wins, a replayed creation never reverts a
decision); ``risk.query.liquidation_plans`` answers are merged as authoritative.

Listeners (the WebSocket hub, the notifier's maintenance hook) receive a
:class:`Change` for every applied message.
"""

from __future__ import annotations

from collections import OrderedDict, deque
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from typing import Any

import msgspec
from loguru import logger

from futrisk.common.clock import Clock
from futrisk.messages.base import Msg, to_builtins
from futrisk.messages.enums import (
    AccountState,
    ControlOrigin,
    FirmAction,
    MdQuality,
    OrderStatus,
    RiskEventKind,
)
from futrisk.messages.envelope import Envelope, open_payload
from futrisk.messages.eod import BacktestStatus, DayRoll, EodPhaseEvent, ReportReady, SettlementCompleted
from futrisk.messages.md import FLAG_WM_RESET, MdEvent, MdStatus, Tick
from futrisk.messages.orders import BrokerOrderUpdate, BrokerStatus, OrderUpdate, Trade
from futrisk.messages.risk import (
    AccountSnapshot,
    AccountStateChanged,
    CashMovement,
    FirmControl,
    FirmSnapshot,
    LiquidationPlan,
    MetricPoint,
    RiskEvent,
)
from futrisk.messages.system import Heartbeat, ServiceStateChanged

# Change kinds delivered to listeners
TICK = "tick"
MD_STATUS = "md_status"
MD_EVENT = "md_event"
SNAPSHOT = "snapshot"
STATE = "state"
RISK_EVENT = "risk_event"
FIRM = "firm"
FIRM_CONTROL = "firm_control"
ORDER = "order"
CASH = "cash"
TRADE = "trade"
BROKER_ORDER = "broker_order"
TD_STATUS = "td_status"
HEARTBEAT = "heartbeat"
SERVICE = "service"
EOD = "eod"
EOD_WARNING = "eod_warning"
SETTLED = "settled"
REPORT = "report"
BACKTEST = "backtest"
DAYROLL = "dayroll"

MAX_RISK_EVENTS = 2000
MAX_STATE_CHANGES = 2000
MAX_MD_EVENTS = 500
MAX_SERVICE_EVENTS = 300
MAX_EOD_EVENTS = 300
MAX_EOD_WARNINGS = 500
MAX_ORDERS_PER_ACCOUNT = 5000
MAX_PLANS = 1000
MAX_EQUITY_POINTS = 6000  # 5 s sampling: ~8 hours
EQUITY_SAMPLE_S = 5.0


@dataclass(frozen=True, slots=True)
class Change:
    """One applied update: ``kind`` (module constants), ``key`` (account/instrument/instance) and the payload."""

    kind: str
    key: str | None
    obj: Any


Listener = Callable[[Change], None]
LivenessOf = Callable[[str], dict[str, Any]]  # account_id -> {"stale": bool, "age_ms": int | None}


@dataclass
class HeartbeatSeen:
    heartbeat: Heartbeat
    recv_mono_ns: int
    recv_ns: int


@dataclass
class ServiceProcState:
    """Process view of one instance from ``sys.service`` (supervisor)."""

    instance: str
    service: str
    state: str
    pid: int | None
    restarts: int
    exit_code: int | None
    ts_ns: int


@dataclass
class ReadModelStats:
    applied: int = 0
    decode_errors: int = 0
    unknown: int = 0
    handler_errors: int = 0
    per_kind: dict[str, int] = field(default_factory=dict)


def worst_hard_utilization(rules: Iterable[Any]) -> tuple[float | None, str | None, str | None]:
    """(utilization, rule_id, level) of the rule closest to its hard limit.

    Uses the core's ``RuleStatus.hard_utilization`` / ``hard_level`` (value / the
    rule's hard-limit threshold) when the shard sends them, else the
    ``utilization`` of the rule's next level (older shards).
    """
    worst: tuple[float | None, str | None, str | None] = (None, None, None)
    for r in rules:
        hard = getattr(r, "hard_utilization", None)
        u = hard if hard is not None else r.utilization
        level = getattr(r, "hard_level", None) if hard is not None else r.next_level
        if u is not None and (worst[0] is None or u > worst[0]):
            worst = (u, r.rule_id, level)
    return worst


class ReadModel:
    """In-memory latest state for REST queries and WebSocket pushes."""

    def __init__(self, clock: Clock) -> None:
        self._clock = clock
        self._listeners: list[Listener] = []
        self.liveness_of: LivenessOf | None = None
        self.stats = ReadModelStats()
        # market data
        self.ticks: dict[str, Tick] = {}
        self.md_status: MdStatus | None = None
        self.md_events: deque[MdEvent] = deque(maxlen=MAX_MD_EVENTS)
        # risk
        self.snapshots: dict[str, AccountSnapshot] = {}
        self.states: dict[str, AccountStateChanged] = {}
        self.risk_events: OrderedDict[str, RiskEvent] = OrderedDict()
        self.state_changes: deque[AccountStateChanged] = deque(maxlen=MAX_STATE_CHANGES)
        self._raises: OrderedDict[str, RiskEvent] = OrderedDict()  # alert_key -> ALERT_RAISED (the "why")
        self.firm: FirmSnapshot | None = None
        self.firm_controls: deque[FirmControl] = deque(maxlen=100)
        self.firm_manual: FirmControl | None = None  # latest MANUAL kill-switch control
        self.firm_rule: dict[str, Any] | None = None  # rule layer: {active, restrictions, ts_ns, ...}
        self.orders: dict[str, dict[str, OrderUpdate]] = {}
        self.order_account: dict[str, str] = {}
        self.plan_account: dict[str, str] = {}
        self.liquidation_plans: OrderedDict[str, dict[str, Any]] = OrderedDict()
        self.cash: deque[CashMovement] = deque(maxlen=500)
        self.dayrolls: deque[DayRoll] = deque(maxlen=50)
        self.shard_td: dict[int, str] = {}
        # trading
        self.td_status: BrokerStatus | None = None
        self.trades: dict[str, deque[Trade]] = {}
        # system
        self.heartbeats: dict[str, HeartbeatSeen] = {}
        self.services: dict[str, ServiceProcState] = {}
        self.service_events: deque[ServiceStateChanged] = deque(maxlen=MAX_SERVICE_EVENTS)
        # clearing
        self.eod_events: deque[Msg] = deque(maxlen=MAX_EOD_EVENTS)
        self.eod_phase: dict[str, EodPhaseEvent] = {}
        self.eod_warnings: deque[dict[str, Any]] = deque(maxlen=MAX_EOD_WARNINGS)
        self._eod_warn_ids: OrderedDict[str, None] = OrderedDict()
        self.settled: dict[str, SettlementCompleted] = {}
        self.reports: dict[str, ReportReady] = {}
        self.backtests: dict[str, BacktestStatus] = {}
        # sampled equity curve per account (02 §9.7 ``equity:{id}``)
        self.equity_points: dict[str, deque[dict[str, Any]]] = {}
        self._equity_td: dict[str, str] = {}

    # ----------------------------------------------------------- listeners
    def add_listener(self, listener: Listener) -> None:
        self._listeners.append(listener)

    def _emit(self, kind: str, key: str | None, obj: Any) -> None:
        self.stats.applied += 1
        self.stats.per_kind[kind] = self.stats.per_kind.get(kind, 0) + 1
        change = Change(kind, key, obj)
        for listener in list(self._listeners):
            try:
                listener(change)
            except Exception:
                logger.exception("readmodel.listener_failed", kind=kind, key=key)

    # --------------------------------------------------------------- input
    def apply(self, env: Envelope) -> None:
        """Apply one envelope of a read-model subscription (snapshot item or live)."""
        try:
            body = open_payload(env)
        except (KeyError, msgspec.DecodeError, msgspec.ValidationError) as exc:
            self.stats.decode_errors += 1
            logger.warning("readmodel.decode_failed", topic=env.topic, type=env.type, src=env.src, error=str(exc))
            return
        try:
            self._dispatch(env, body)
        except Exception:
            self.stats.handler_errors += 1
            logger.exception("readmodel.apply_failed", topic=env.topic, type=env.type, src=env.src)

    def _dispatch(self, env: Envelope, body: Msg) -> None:
        topic = env.topic
        if isinstance(body, Tick):
            self._on_tick(body)
        elif isinstance(body, MdStatus):
            self.md_status = body
            self._emit(MD_STATUS, None, body)
        elif isinstance(body, MdEvent):
            if not any(e.event_id == body.event_id for e in self.md_events):
                self.md_events.append(body)
                self._emit(MD_EVENT, body.instrument_id, body)
        elif isinstance(body, AccountSnapshot):
            self._on_snapshot(body)
        elif isinstance(body, AccountStateChanged):
            self._on_state(body)
        elif isinstance(body, RiskEvent):
            self._on_risk_event(body)
        elif isinstance(body, FirmSnapshot):
            self.firm = body
            self._emit(FIRM, None, body)
        elif isinstance(body, FirmControl):
            self._on_firm_control(body)
        elif isinstance(body, OrderUpdate):
            self._on_order(body)
        elif isinstance(body, CashMovement):
            self.cash.append(body)
            self._emit(CASH, body.account_id, body)
        elif isinstance(body, Trade):
            ring = self.trades.setdefault(body.account_id, deque(maxlen=500))
            ring.append(body)
            self._emit(TRADE, body.account_id, body)
        elif isinstance(body, BrokerOrderUpdate):
            self._emit(BROKER_ORDER, body.account_id, body)
        elif isinstance(body, BrokerStatus):
            self.td_status = body
            self._emit(TD_STATUS, None, body)
        elif isinstance(body, DayRoll):
            self._on_dayroll(body)
        elif isinstance(body, EodPhaseEvent):
            self._on_eod(body)
        elif isinstance(body, SettlementCompleted):
            self.settled[body.trading_day] = body
            self._push_eod(body)
            self._emit(SETTLED, body.trading_day, body)
        elif isinstance(body, ReportReady):
            known = body.report_id in self.reports
            self.reports[body.report_id] = body
            if not known:
                self._push_eod(body)
                self._emit(REPORT, body.report_id, body)
        elif isinstance(body, BacktestStatus):
            self.backtests[body.run_id] = body
            self._emit(BACKTEST, body.run_id, body)
        elif isinstance(body, ServiceStateChanged):
            self._on_service(body)
        elif isinstance(body, Heartbeat):
            self.on_heartbeat(body)
        else:
            self.stats.unknown += 1
            logger.debug("readmodel.unhandled", topic=topic, type=env.type)

    # ------------------------------------------------------------ handlers
    def _on_tick(self, t: Tick) -> None:
        old = self.ticks.get(t.instrument_id)
        if (old is not None and old.trading_day == t.trading_day and t.order_key < old.order_key
                and not t.flags & FLAG_WM_RESET):  # a watermark reset at md-gateway may go backwards
            return
        self.ticks[t.instrument_id] = t
        self._emit(TICK, t.instrument_id, t)

    def _on_snapshot(self, s: AccountSnapshot) -> None:
        old = self.snapshots.get(s.account_id)
        if old is not None and s.as_of_ns < old.as_of_ns and s.state_version <= old.state_version:
            return
        self.snapshots[s.account_id] = s
        self._emit(SNAPSHOT, s.account_id, s)

    def seed_snapshots(self, items: Iterable[AccountSnapshot]) -> int:
        """Apply ``risk.query.accounts`` answers (start-up / shard restart seeding); returns how many applied.

        FACT snapshot items (``risk.state.*``) may be missing from ``ctl.snapshot``
        right after a publisher restart, so the effective state is also seeded from
        the snapshots themselves (``state``, ``state_version``, restrictions).
        """
        n = 0
        for s in items:
            before = self.snapshots.get(s.account_id)
            self._on_snapshot(s)
            if self.snapshots.get(s.account_id) is not before:
                n += 1
        return n

    def _on_state(self, st: AccountStateChanged) -> None:
        old = self.states.get(st.account_id)
        if old is not None and st.state_version < old.state_version:
            self._remember_state(st)  # an older change (history backfill): kept, but it is not the current state
            return
        dup = old is not None and old.state_version == st.state_version and old.ts_ns == st.ts_ns
        self.states[st.account_id] = st
        if not dup and self._remember_state(st):
            self._emit(STATE, st.account_id, st)

    def _remember_state(self, st: AccountStateChanged) -> bool:
        if any(x.account_id == st.account_id and x.state_version == st.state_version and x.ts_ns == st.ts_ns
               for x in self.state_changes):
            return False
        self.state_changes.append(st)
        return True

    def recent_state_changes(self, *, trading_day: str | None = None, account_id: str | None = None,
                             limit: int | None = None) -> list[AccountStateChanged]:
        out = [s for s in self.state_changes
               if (trading_day is None or s.trading_day == trading_day)
               and (account_id is None or s.account_id == account_id)]
        out.sort(key=lambda s: s.ts_ns)
        return out[-limit:] if limit else out

    @staticmethod
    def risk_event_item(ev: RiskEvent) -> dict[str, Any]:
        """A risk event in the ``events`` views: the RiskEvent with ``type="risk_event"``."""
        return {"type": "risk_event", **to_builtins(ev)}

    def state_change_item(self, st: AccountStateChanged) -> dict[str, Any]:
        """A state change in the ``events`` views: the AccountStateChanged with ``type="state_change"`` and the
        rule / level / metric / value / threshold of the alert that caused it (from ``cause_alert_key``)."""
        item: dict[str, Any] = {"type": "state_change", **to_builtins(st),
                                "rule_id": None, "level": None, "metric": None, "value": None, "threshold": None}
        key = st.cause_alert_key
        if key:
            parts = key.split("|")
            if len(parts) >= 5:
                item["rule_id"], item["level"] = parts[1], parts[2]
            raised = self._raises.get(key)
            if raised is not None:
                item.update(rule_id=raised.rule_id or item["rule_id"], level=raised.level or item["level"],
                            metric=raised.metric, value=raised.value, threshold=raised.threshold)
        return item

    def event_items(self, *, trading_day: str | None = None, account_id: str | None = None,
                    limit: int | None = None) -> list[dict[str, Any]]:
        """Risk events and state changes together, oldest first (the WS ``events`` snapshot)."""
        tagged: list[tuple[int, dict[str, Any]]] = [
            (e.ts_ns, self.risk_event_item(e))
            for e in self.recent_risk_events(trading_day=trading_day, account_id=account_id)]
        tagged += [(s.ts_ns, self.state_change_item(s))
                   for s in self.recent_state_changes(trading_day=trading_day, account_id=account_id)]
        tagged.sort(key=lambda x: x[0])
        items = [x for _, x in tagged]
        return items[-limit:] if limit else items

    def _on_risk_event(self, ev: RiskEvent) -> None:
        if ev.event_id in self.risk_events:
            return
        self.risk_events[ev.event_id] = ev
        while len(self.risk_events) > MAX_RISK_EVENTS:
            self.risk_events.popitem(last=False)
        self._note_plan(ev)
        self._note_raise(ev)
        self._emit(RISK_EVENT, ev.account_id, ev)

    def _note_raise(self, ev: RiskEvent) -> None:
        if ev.kind != RiskEventKind.ALERT_RAISED or not ev.alert_key:
            return
        self._raises.pop(ev.alert_key, None)
        self._raises[ev.alert_key] = ev
        while len(self._raises) > MAX_RISK_EVENTS:
            self._raises.popitem(last=False)

    def _note_plan(self, ev: RiskEvent) -> None:
        if ev.kind != RiskEventKind.LIQUIDATION_PLAN:
            return
        detail = ev.detail if isinstance(ev.detail, dict) else {}
        plan = detail.get("plan") if isinstance(detail.get("plan"), dict) else None
        plan_id = (plan or {}).get("plan_id") or detail.get("plan_id")
        if not isinstance(plan_id, str):
            return
        account = ev.account_id or (plan or {}).get("account_id")
        if isinstance(account, str):
            self._plan_owner(plan_id, account)
        entry = self.liquidation_plans.get(plan_id)
        if entry is not None and int(entry.get("updated_ns") or 0) > ev.ts_ns:
            return  # an older event (e.g. the creation replayed after the decision) never reverts it
        if plan is not None:
            entry = {**plan, "updated_ns": ev.ts_ns}
        elif isinstance(detail.get("status"), str):
            entry = dict(entry) if entry is not None else {
                "plan_id": plan_id, "account_id": account, "trading_day": ev.trading_day,
                "trigger_alert_key": ev.alert_key}
            entry["status"] = detail["status"]
            if detail["status"] == "CONFIRMED":
                entry["confirmed_by"] = detail.get("actor") or ev.actor
                entry["confirmed_ns"] = ev.ts_ns
            entry["updated_ns"] = ev.ts_ns
        else:
            return
        self._store_plan(plan_id, entry)

    def _plan_owner(self, plan_id: str, account: str) -> None:
        self.plan_account.pop(plan_id, None)
        self.plan_account[plan_id] = account
        while len(self.plan_account) > MAX_PLANS:
            self.plan_account.pop(next(iter(self.plan_account)))

    def _store_plan(self, plan_id: str, entry: dict[str, Any]) -> None:
        self.liquidation_plans.pop(plan_id, None)
        self.liquidation_plans[plan_id] = entry
        while len(self.liquidation_plans) > MAX_PLANS:
            self.liquidation_plans.popitem(last=False)

    def merge_plans(self, items: Iterable[LiquidationPlan]) -> None:
        """Merge a ``risk.query.liquidation_plans`` answer (the shard's own state is authoritative)."""
        for plan in items:
            self._plan_owner(plan.plan_id, plan.account_id)
            old = self.liquidation_plans.get(plan.plan_id) or {}
            stamp = max(plan.created_ns, plan.confirmed_ns or 0, int(old.get("updated_ns") or 0))
            self._store_plan(plan.plan_id, {**to_builtins(plan), "updated_ns": stamp})

    def plans_of(self, *, trading_day: str | None = None, account_ids: Iterable[str] | None = None,
                 status: str | None = None) -> list[dict[str, Any]]:
        """Plans known to the read model (from events and earlier queries), oldest first."""
        wanted = set(account_ids) if account_ids is not None else None
        out = [dict(p) for p in self.liquidation_plans.values()
               if (trading_day is None or p.get("trading_day") == trading_day)
               and (wanted is None or p.get("account_id") in wanted)
               and (status is None or p.get("status") == status)]
        return sorted(out, key=lambda p: int(p.get("created_ns") or p.get("updated_ns") or 0))

    def merge_risk_events(self, items: Iterable[RiskEvent]) -> int:
        """Merge a ``risk.query.alerts`` answer (history backfill / gap repair; no change events).

        Keeps the newest ``MAX_RISK_EVENTS`` by ``ts_ns``; returns how many were new.
        """
        added = 0
        for ev in items:
            if ev.event_id in self.risk_events:
                continue
            self.risk_events[ev.event_id] = ev
            self._note_plan(ev)
            self._note_raise(ev)
            added += 1
        if len(self.risk_events) > MAX_RISK_EVENTS:
            keep = sorted(self.risk_events.values(), key=lambda e: e.ts_ns)[-MAX_RISK_EVENTS:]
            self.risk_events = OrderedDict((e.event_id, e) for e in keep)
        return added

    def _on_firm_control(self, c: FirmControl) -> None:
        if any(x.ts_ns == c.ts_ns and x.action == c.action and x.origin == c.origin for x in self.firm_controls):
            return
        self.firm_controls.append(c)
        action, origin = str(c.action), str(c.origin)
        if origin == ControlOrigin.MANUAL.value:
            if action in (FirmAction.KILL_SWITCH_ON.value, FirmAction.KILL_SWITCH_OFF.value) and (
                    self.firm_manual is None or c.ts_ns >= self.firm_manual.ts_ns):
                self.firm_manual = c
        else:
            cur: dict[str, Any] = dict(self.firm_rule) if self.firm_rule is not None else {
                "active": False, "restrictions": [], "ts_ns": 0}
            if c.ts_ns >= int(cur["ts_ns"]):
                if action == FirmAction.RULE_STATE.value:  # the complete rule layer in one fact
                    cur["active"] = bool(c.active)
                    cur["restrictions"] = to_builtins(c.restrictions)
                elif action in (FirmAction.KILL_SWITCH_ON.value, FirmAction.KILL_SWITCH_OFF.value):  # legacy
                    cur["active"] = action == FirmAction.KILL_SWITCH_ON.value
                elif action == FirmAction.SET_RESTRICTIONS.value:  # legacy
                    cur["restrictions"] = to_builtins(c.restrictions)
                cur.update(ts_ns=c.ts_ns, reason=c.reason, cause_alert_key=c.cause_alert_key)
                self.firm_rule = cur
        self._emit(FIRM_CONTROL, None, c)

    def firm_control_state(self) -> dict[str, Any] | None:
        """Kill switch = MANUAL or RULE, with both layers (None before any ``risk.control.firm``)."""
        if self.firm_manual is None and self.firm_rule is None:
            return None
        manual = self.firm_manual
        manual_on = manual is not None and str(manual.action) == FirmAction.KILL_SWITCH_ON.value
        rule = self.firm_rule
        return {
            "kill_switch": manual_on or bool(rule and rule.get("active")),
            "manual": None if manual is None else {"active": manual_on, "actor": manual.actor,
                                                   "reason": manual.reason, "ts_ns": manual.ts_ns},
            "rule": None if rule is None else dict(rule),
        }

    def _on_order(self, o: OrderUpdate) -> None:
        book = self.orders.setdefault(o.account_id, {})
        cur = book.get(o.client_order_id)
        if cur is not None and o.updated_ns < cur.updated_ns:
            return
        book[o.client_order_id] = o
        self.order_account[o.client_order_id] = o.account_id
        self._cap_orders(book)
        self._emit(ORDER, o.account_id, o)

    def _cap_orders(self, book: dict[str, OrderUpdate]) -> None:
        """At most ``MAX_ORDERS_PER_ACCOUNT``: the oldest terminal orders go first, then the oldest."""
        excess = len(book) - MAX_ORDERS_PER_ACCOUNT
        if excess <= 0:
            return
        victims = sorted(book.values(), key=lambda x: (not OrderStatus(x.status).is_terminal, x.updated_ns))[:excess]
        for v in victims:
            book.pop(v.client_order_id, None)
            if self.order_account.get(v.client_order_id) == v.account_id:
                self.order_account.pop(v.client_order_id, None)

    def merge_orders(self, account_id: str, items: Iterable[OrderUpdate], *, trading_day: str | None = None) -> int:
        """Merge a ``risk.query.orders`` answer (newest ``updated_ns`` wins, no change events).

        Only orders of ``trading_day`` (default: the account's current trading
        day) enter the live model; history queries of other days are not kept.
        Returns how many orders were merged.
        """
        day = trading_day or self.account_trading_day(account_id)
        book = self.orders.setdefault(account_id, {})
        merged = 0
        for o in items:
            if day is not None and o.trading_day != day:
                continue
            cur = book.get(o.client_order_id)
            if cur is None or o.updated_ns >= cur.updated_ns:
                book[o.client_order_id] = o
                self.order_account[o.client_order_id] = o.account_id
                merged += 1
        self._cap_orders(book)
        return merged

    def account_trading_day(self, account_id: str) -> str | None:
        """The account's current trading day from its latest snapshot (None when unknown)."""
        snap = self.snapshots.get(account_id)
        return snap.trading_day if snap is not None else None

    def _on_dayroll(self, d: DayRoll) -> None:
        if any(x.shard == d.shard and x.to_trading_day == d.to_trading_day and x.mode == d.mode
               and x.sod_version == d.sod_version for x in self.dayrolls):
            return
        self.dayrolls.append(d)
        self.shard_td[d.shard] = d.to_trading_day
        self._rotate_days(d.to_trading_day)
        for acct in d.accounts:
            book = self.orders.get(acct)
            if book:
                for coid in [k for k, o in book.items() if o.trading_day < d.to_trading_day]:
                    book.pop(coid, None)
                    self.order_account.pop(coid, None)
        self._push_eod(d)
        self._emit(DAYROLL, str(d.shard), d)

    def _rotate_days(self, to_td: str) -> None:
        """A roll to ``to_td``: keep the risk events / state changes of ``to_td`` and of the day before only."""
        days = {e.trading_day for e in self.risk_events.values()} | {s.trading_day for s in self.state_changes}
        older = sorted(d for d in days if d < to_td)
        keep_from = older[-1] if older else to_td
        if not any(d < keep_from for d in days):
            return
        self.risk_events = OrderedDict((k, e) for k, e in self.risk_events.items() if e.trading_day >= keep_from)
        kept = [s for s in self.state_changes if s.trading_day >= keep_from]
        self.state_changes = deque(kept, maxlen=MAX_STATE_CHANGES)

    def shard_trading_day(self, alive: Callable[[str], bool] | None = None) -> tuple[str | None, bool]:
        """(the shards' current trading day, whether a live shard reports it).

        The newest ``trading_day`` of the heartbeats of live risk shards; without
        one, the newest known from any shard heartbeat or ``sys.dayroll``.
        """
        rolled = list(self.shard_td.values())  # sys.dayroll may arrive before the next heartbeat
        live: list[str] = []
        known: list[str] = list(rolled)
        for inst, seen in self.heartbeats.items():
            hb = seen.heartbeat
            if hb.service != "risk-engine" or not hb.trading_day:
                continue
            known.append(hb.trading_day)
            if alive is None or alive(inst):
                live.append(hb.trading_day)
        if live:
            return max(live + rolled), True
        return (max(known), False) if known else (None, False)

    def _on_eod(self, e: EodPhaseEvent) -> None:
        detail = e.detail if isinstance(e.detail, dict) else {}
        if isinstance(detail.get("warning"), str):
            # a WARN repeats the run's current phase: it is no phase change (and no second DONE)
            warn_id = str(detail.get("warn_id") or f"{e.trading_day}|{e.version}|{e.ts_ns}")
            context = {k: v for k, v in detail.items() if k not in ("warning", "kind", "warn_id")}
            if self._add_eod_warning(e, warn_id, str(detail.get("kind") or "EOD_WARNING"), detail["warning"],
                                     context):
                self._push_eod(e)
                self._emit(EOD_WARNING, e.trading_day, e)
            return
        cur = self.eod_phase.get(e.trading_day)
        if cur is None or (e.version, e.ts_ns) >= (cur.version, cur.ts_ns):
            self.eod_phase[e.trading_day] = e  # newest wins, a rewind (FROZEN again) included
        if any(isinstance(x, EodPhaseEvent) and x.trading_day == e.trading_day and x.version == e.version
               and x.phase == e.phase and x.ts_ns == e.ts_ns for x in self.eod_events):
            return  # the same event delivered again (snapshot / resync)
        for i, w in enumerate(detail.get("warnings") or []):
            if isinstance(w, dict):
                kind = str(w.get("kind") or "EOD_WARNING")
                self._add_eod_warning(e, f"{e.trading_day}|{e.version}|{e.phase}|{e.ts_ns}|{i}", kind,
                                      str(w.get("message") or kind), {})
        self._push_eod(e)
        self._emit(EOD, e.trading_day, e)

    def _add_eod_warning(self, e: EodPhaseEvent, warn_id: str, kind: str, message: str,
                         context: dict[str, Any]) -> bool:
        if warn_id in self._eod_warn_ids:
            return False
        self._eod_warn_ids[warn_id] = None
        while len(self._eod_warn_ids) > MAX_EOD_WARNINGS * 2:
            self._eod_warn_ids.popitem(last=False)
        self.eod_warnings.append({"trading_day": e.trading_day, "version": e.version, "phase": str(e.phase),
                                  "kind": kind, "message": message, "warn_id": warn_id, "ts_ns": e.ts_ns,
                                  "context": context})
        return True

    def eod_warnings_of(self, trading_day: str, version: int | None = None) -> list[dict[str, Any]]:
        """Warnings of the EOD run(s) of ``trading_day`` seen on ``sys.eod`` (oldest first)."""
        return [w for w in self.eod_warnings
                if w["trading_day"] == trading_day and (version is None or w["version"] == version)]

    def _push_eod(self, obj: Msg) -> None:
        self.eod_events.append(obj)

    def _on_service(self, ev: ServiceStateChanged) -> None:
        if any(x.instance == ev.instance and x.ts_ns == ev.ts_ns and x.to_state == ev.to_state
               for x in self.service_events):
            return
        self.service_events.append(ev)
        self.services[ev.instance] = ServiceProcState(
            instance=ev.instance,
            service=ev.service,
            state=str(ev.to_state),
            pid=ev.pid,
            restarts=ev.restarts,
            exit_code=ev.exit_code,
            ts_ns=ev.ts_ns,
        )
        self._emit(SERVICE, ev.instance, ev)

    def on_heartbeat(self, hb: Heartbeat) -> None:
        """Heartbeat observer (``rt.on_heartbeat``)."""
        self.heartbeats[hb.instance] = HeartbeatSeen(hb, self._clock.monotonic_ns(), self._clock.now_ns())
        self._emit(HEARTBEAT, hb.instance, hb)

    # ------------------------------------------------------------- queries
    def liveness(self, account_id: str) -> dict[str, Any]:
        """``{"stale", "age_ms"}`` of the account's owning shard (not stale when unknown)."""
        if self.liveness_of is None:
            return {"stale": False, "age_ms": None}
        try:
            return self.liveness_of(account_id)
        except Exception:
            logger.exception("readmodel.liveness_failed", account=account_id)
            return {"stale": False, "age_ms": None}

    def effective_state(self, account_id: str) -> dict[str, Any] | None:
        """State, latch and restrictions from the newer of risk.state / risk.snapshot (02 §5 rule 2)."""
        snap = self.snapshots.get(account_id)
        st = self.states.get(account_id)
        if snap is None and st is None:
            return None
        if st is not None and (snap is None or st.state_version > snap.state_version):
            return {
                "state": AccountState(st.to_state),
                "halted_latched": st.halted_latched,
                "restrictions": list(st.restrictions),
                "state_version": st.state_version,
                "source": "state",
            }
        assert snap is not None
        return {
            "state": AccountState(snap.state),
            "halted_latched": snap.halted_latched,
            "restrictions": list(snap.restrictions),
            "state_version": snap.state_version,
            "source": "snapshot",
        }

    def account_snapshot(self, account_id: str) -> dict[str, Any] | None:
        """Latest ``AccountSnapshot`` as builtins with the effective state merged in."""
        snap = self.snapshots.get(account_id)
        if snap is None:
            return None
        d: dict[str, Any] = to_builtins(snap)
        eff = self.effective_state(account_id)
        if eff is not None and eff["source"] == "state":
            d["state"] = str(eff["state"])
            d["halted_latched"] = eff["halted_latched"]
            d["restrictions"] = to_builtins(eff["restrictions"])
            d["state_version"] = eff["state_version"]
        d.update(self.liveness(account_id))
        return d

    def account_summary(self, account_id: str) -> dict[str, Any] | None:
        """WS ``accounts`` item (02 §9.7) plus a few extras the dashboard reads when present."""
        snap = self.snapshots.get(account_id)
        eff = self.effective_state(account_id)
        if snap is None:
            if eff is None:
                return None
            st = self.states[account_id]
            return {
                "account_id": account_id,
                "state": str(eff["state"]),
                "halted_latched": eff["halted_latched"],
                "state_version": eff["state_version"],
                "as_of_ns": st.ts_ns,
                **self.liveness(account_id),
            }
        worst_u, worst_rule, worst_level = worst_hard_utilization(snap.rules)
        assert eff is not None
        return {
            "account_id": snap.account_id,
            "shard": snap.shard,
            "state": str(eff["state"]),
            "halted_latched": eff["halted_latched"],
            "equity": snap.funds.equity,
            "daily_pnl": snap.metrics.daily_pnl,
            "daily_pnl_pct_init": snap.metrics.daily_pnl_pct_init,
            "risk_degree": snap.funds.risk_degree,
            "worst_utilization": worst_u,
            "quality": str(snap.quality),
            "worst_rule": worst_rule,
            "worst_level": worst_level,
            "state_version": eff["state_version"],
            "as_of_ns": snap.as_of_ns,
            "trading_mode": str(snap.trading_mode),
            "trading_day": snap.trading_day,
            "initial_capital": snap.funds.initial_capital,
            "margin": snap.funds.margin,
            "open_orders": snap.open_orders,
            **self.liveness(account_id),
        }

    def account_summaries(self) -> list[dict[str, Any]]:
        out = []
        for acct in sorted(set(self.snapshots) | set(self.states)):
            s = self.account_summary(acct)
            if s is not None:
                out.append(s)
        return out

    def risk_state_rows(self) -> list[dict[str, Any]]:
        """``GET /risk/state``: per account the effective state and complete restriction set."""
        rows = []
        for acct in sorted(set(self.snapshots) | set(self.states)):
            eff = self.effective_state(acct)
            if eff is None:
                continue
            snap = self.snapshots.get(acct)
            st = self.states.get(acct)
            rows.append(
                {
                    "account_id": acct,
                    "shard": snap.shard if snap is not None else None,
                    "trading_day": snap.trading_day if snap is not None else (st.trading_day if st else None),
                    "state": str(eff["state"]),
                    "halted_latched": eff["halted_latched"],
                    "state_version": eff["state_version"],
                    "restrictions": to_builtins(eff["restrictions"]),
                    "trading_mode": str(snap.trading_mode) if snap is not None else None,
                    "last_change": to_builtins(st) if st is not None else None,
                    **self.liveness(acct),
                }
            )
        return rows

    def instrument_quality(self) -> dict[str, dict[str, Any]]:
        if self.md_status is None:
            return {}
        return {i.instrument_id: {"quality": str(i.quality), "limit_lock": str(i.limit_lock)}
                for i in self.md_status.instruments}

    def quote_item(self, t: Tick, quality: dict[str, dict[str, Any]] | None = None) -> dict[str, Any]:
        """WS ``quotes`` item (02 §9.7) from a Tick."""
        q = (quality if quality is not None else self.instrument_quality()).get(t.instrument_id, {})
        change = (t.last - t.pre_settle) / t.pre_settle if t.pre_settle else None
        item: dict[str, Any] = {
            "instrument_id": t.instrument_id,
            "last": t.last,
            "bid1": t.bid1,
            "ask1": t.ask1,
            "volume": t.volume,
            "change_pct": change,
            "source": str(t.source),
            "lag_ms": (t.recv_ns - t.exch_ts_ns) / 1e6,
            "quality": q.get("quality", str(MdQuality.OK)),
            "exchange": str(t.exchange),
            "upper_limit": t.upper_limit,
            "lower_limit": t.lower_limit,
            "pre_settle": t.pre_settle,
        }
        if "limit_lock" in q:
            item["limit_lock"] = q["limit_lock"]
        return item

    def quote_items(self) -> list[dict[str, Any]]:
        quality = self.instrument_quality()
        return [self.quote_item(self.ticks[k], quality) for k in sorted(self.ticks)]

    def recent_risk_events(self, *, trading_day: str | None = None, account_id: str | None = None,
                           limit: int | None = None) -> list[RiskEvent]:
        evs = [e for e in self.risk_events.values()
               if (trading_day is None or e.trading_day == trading_day)
               and (account_id is None or e.account_id == account_id)]
        evs.sort(key=lambda e: e.ts_ns)
        return evs[-limit:] if limit else evs

    def stale_accounts(self) -> list[str]:
        """Accounts in the model whose owning shard is not UP."""
        return [a for a in sorted(set(self.snapshots) | set(self.states)) if self.liveness(a).get("stale")]

    def orders_of(self, account_id: str, trading_day: str | None = None) -> list[OrderUpdate]:
        book = self.orders.get(account_id, {})
        items = [o for o in book.values() if trading_day is None or o.trading_day == trading_day]
        return sorted(items, key=lambda o: o.created_ns)

    # --------------------------------------------------------------- equity
    def sample_equity(self) -> list[tuple[str, dict[str, Any]]]:
        """Take one equity point per account from its latest snapshot (every 5 s, 02 §9.7)."""
        out: list[tuple[str, dict[str, Any]]] = []
        for acct, snap in self.snapshots.items():
            ring = self.equity_points.setdefault(acct, deque(maxlen=MAX_EQUITY_POINTS))
            if self._equity_td.get(acct) != snap.trading_day:
                ring.clear()
                self._equity_td[acct] = snap.trading_day
            eff = self.effective_state(acct)
            point = {
                "ts_ns": snap.as_of_ns,
                "equity": snap.funds.equity,
                "daily_pnl_pct_init": snap.metrics.daily_pnl_pct_init,
                "intraday_dd": snap.metrics.intraday_dd,
                "state": str(eff["state"]) if eff is not None else str(snap.state),
                "nav": snap.metrics.nav,
                "risk_degree": snap.funds.risk_degree,
            }
            if ring and ring[-1]["ts_ns"] >= point["ts_ns"]:
                continue
            ring.append(point)
            out.append((acct, point))
        return out

    def merge_equity(self, account_id: str, trading_day: str, points: Iterable[MetricPoint]) -> list[dict[str, Any]]:
        """Merge ``risk.query.metrics`` history into the sampled curve; returns the full sorted curve."""
        ring = self.equity_points.setdefault(account_id, deque(maxlen=MAX_EQUITY_POINTS))
        if self._equity_td.get(account_id) not in (None, trading_day):
            ring.clear()
        self._equity_td[account_id] = trading_day
        merged = {p["ts_ns"]: p for p in ring}
        for mp in points:
            merged.setdefault(
                mp.ts_ns,
                {
                    "ts_ns": mp.ts_ns,
                    "equity": mp.equity,
                    "daily_pnl_pct_init": mp.daily_pnl_pct_init,
                    "intraday_dd": mp.intraday_dd,
                    "state": str(mp.state),
                    "nav": mp.nav,
                    "risk_degree": mp.risk_degree,
                },
            )
        ordered = [merged[k] for k in sorted(merged)]
        ring.clear()
        ring.extend(ordered[-MAX_EQUITY_POINTS:])
        return list(ring)

    def equity_curve(self, account_id: str) -> list[dict[str, Any]]:
        return list(self.equity_points.get(account_id, ()))

    # -------------------------------------------------------------- system
    def heartbeat_of(self, instance: str) -> Heartbeat | None:
        seen = self.heartbeats.get(instance)
        return seen.heartbeat if seen is not None else None

    def system_items(self, alive: Callable[[str], str] | None = None) -> list[dict[str, Any]]:
        """WS ``system`` state items: ``{instance, state, restarts, queues, rates, latency}`` + extras."""
        out: list[dict[str, Any]] = []
        for inst in sorted(set(self.heartbeats) | set(self.services)):
            seen = self.heartbeats.get(inst)
            proc = self.services.get(inst)
            item: dict[str, Any] = {"instance": inst, "restarts": proc.restarts if proc else 0}
            if seen is not None:
                hb = seen.heartbeat
                item.update(
                    service=hb.service,
                    state=str(hb.state),
                    queues=dict(hb.queues),
                    rates=dict(hb.rates),
                    latency=to_builtins(hb.latency_us),
                    loop_lag_ms=hb.loop_lag_ms,
                    config_version=hb.config_version,
                    config_hash=hb.config_hash,
                    clock_version=hb.clock_version,
                    pid=hb.pid,
                    started_ns=hb.started_ns,
                    trading_mode=str(hb.trading_mode) if hb.trading_mode is not None else None,
                    deps=dict(hb.deps),
                )
            if proc is not None and proc.state in ("EXITED", "RESTARTING", "STOPPED", "FAILED"):
                # the supervisor knows the process is gone: its last heartbeat is stale
                item["state"] = proc.state
            if alive is not None:
                item["liveness"] = alive(inst)
            if "state" not in item and proc is not None:
                item["state"] = proc.state
            out.append(item)
        return out

    def eod_recent(self, limit: int = 100) -> list[Msg]:
        return list(self.eod_events)[-limit:]


__all__ = [
    "BACKTEST",
    "BROKER_ORDER",
    "CASH",
    "DAYROLL",
    "EOD",
    "EOD_WARNING",
    "EQUITY_SAMPLE_S",
    "FIRM",
    "FIRM_CONTROL",
    "HEARTBEAT",
    "MD_EVENT",
    "MD_STATUS",
    "ORDER",
    "REPORT",
    "RISK_EVENT",
    "SERVICE",
    "SETTLED",
    "SNAPSHOT",
    "STATE",
    "TD_STATUS",
    "TICK",
    "TRADE",
    "Change",
    "HeartbeatSeen",
    "ReadModel",
    "worst_hard_utilization",
    "ServiceProcState",
]
