"""Risk payloads: snapshots, state, events, cash, firm controls, commands (02 §6.3, §7.6)."""

from __future__ import annotations

from typing import Any

import msgspec

from futrisk.messages.base import Msg, payload
from futrisk.messages.enums import (
    AccountState,
    Action,
    CashType,
    ControlOrigin,
    Direction,
    Exchange,
    FirmAction,
    LiquidationStatus,
    MdQuality,
    Offset,
    OrderStatus,
    PosDirection,
    ReconBreakKind,
    ReconBreakStatus,
    ReconResolution,
    RestrictionKind,
    RiskEventKind,
    Role,
    RuleSmState,
    Severity,
    TradingMode,
)
from futrisk.messages.orders import OrderUpdate, Trade


class Restriction(msgspec.Struct, kw_only=True, omit_defaults=True, frozen=True):
    kind: RestrictionKind
    scope_key: str
    value: float | None = None
    source: str
    since_ns: int = 0


class AccountFunds(msgspec.Struct, kw_only=True):
    initial_capital: float
    cum_net_deposit: float
    pre_balance: float
    net_deposit: float
    static_equity: float
    close_pnl: float
    position_pnl: float
    fee: float
    equity: float
    margin: float
    frozen_margin: float
    frozen_fee: float
    available: float
    risk_degree: float


class AccountMetricsView(msgspec.Struct, kw_only=True):
    daily_pnl: float
    daily_pnl_pct_init: float
    daily_pnl_pct_sod: float
    floating_pnl_pct_init: float
    perf_equity: float
    nav: float
    intraday_peak_equity: float
    intraday_dd: float
    hwm_equity: float
    hwm_dd: float
    gross_notional: float
    net_notional: float
    leverage: float
    effective_n: float
    limit_stress: float
    orders_today: int
    cancels_today: int


class ExposureView(msgspec.Struct, kw_only=True, omit_defaults=True):
    product: str
    sector: str
    exchange: Exchange
    long_lots: int
    short_lots: int
    net_lots: int
    net_notional: float
    gross_notional: float
    net_pct_equity: float
    cap_pct: float | None = None
    utilization: float | None = None
    # worst MdQuality of the product's held instruments (the quality of product-scope rules); None: not reported
    quality: MdQuality | None = None


class PositionView(msgspec.Struct, kw_only=True, omit_defaults=True):
    account_id: str
    instrument_id: str
    product: str
    exchange: Exchange
    direction: PosDirection
    td_qty: int
    yd_qty: int
    closable_qty: int
    closable_td_qty: int
    closable_yd_qty: int
    avg_open_price: float
    basis_avg_price: float
    last: float | None = None
    position_pnl: float = 0.0
    float_pnl_fifo: float = 0.0
    margin: float = 0.0
    notional: float = 0.0
    pct_equity: float = 0.0
    limit_locked: bool = False


class RuleStatus(msgspec.Struct, kw_only=True, omit_defaults=True):
    rule_id: str
    scope_key: str
    metric: str
    value: float | None = None
    active_level: str | None = None
    sm_state: RuleSmState = RuleSmState.OK
    next_level: str | None = None
    next_threshold: float | None = None
    utilization: float | None = None  # value vs the NEXT level (distance to the next level)
    headroom_money: float | None = None
    # value vs the rule's hard limit (first CIRCUIT_BREAK, else last RESTRICT, else last level; overrides applied):
    # >= / > : value / limit; <= / < : (b - value) / (b - limit) with the metric's baseline b, or limit / value when
    # b = +inf (effective N); clamped at 0; None without a value (or a zero denominator)
    hard_utilization: float | None = None
    hard_level: str | None = None


@payload()
class AccountSnapshot(Msg, kw_only=True):
    account_id: str
    shard: int
    trading_day: str
    version: int
    state_version: int
    as_of_ns: int
    quality: MdQuality
    state: AccountState
    halted_latched: bool = False
    trading_mode: TradingMode = TradingMode.OPEN
    restrictions: list[Restriction] = []
    funds: AccountFunds
    metrics: AccountMetricsView
    exposures: list[ExposureView] = []
    positions: list[PositionView] = []
    rules: list[RuleStatus] = []
    open_orders: int = 0


@payload()
class AccountStateChanged(Msg, kw_only=True):
    account_id: str
    trading_day: str
    from_state: AccountState
    to_state: AccountState
    halted_latched: bool
    state_version: int
    restrictions: list[Restriction]
    cause: str
    cause_alert_key: str | None = None
    actor: str | None = None
    reason: str | None = None
    ts_ns: int


@payload()
class RiskEvent(Msg, kw_only=True):
    event_id: str
    kind: RiskEventKind
    trading_day: str
    ts_ns: int
    account_id: str | None = None
    scope_key: str | None = None
    alert_key: str | None = None
    rule_id: str | None = None
    level: str | None = None
    episode: int | None = None
    action: Action | None = None
    severity: Severity = Severity.INFO
    latched: bool | None = None
    metric: str | None = None
    value: float | None = None
    threshold: float | None = None
    actor: str | None = None
    message: str = ""
    detail: dict[str, Any] = {}


@payload()
class CashMoveRequest(Msg, kw_only=True):
    movement_id: str
    account_id: str
    type: CashType
    amount: str
    reason: str
    actor: str
    role: Role
    requested_ns: int = 0


@payload()
class CashMovement(Msg, kw_only=True):
    movement_id: str
    account_id: str
    trading_day: str
    type: CashType
    amount: str
    actor: str
    reason: str
    ts_ns: int


class CashDigest(msgspec.Struct, kw_only=True):
    account_id: str
    count: int
    amount_sum: str


class ProductNet(msgspec.Struct, kw_only=True):
    product: str
    net_lots: int
    net_notional: float
    gross_notional: float
    accounts: int


class ShardFreshness(msgspec.Struct, kw_only=True):
    shard: int
    as_of_ns: int
    quality: MdQuality


@payload()
class FirmSnapshot(Msg, kw_only=True):
    trading_day: str
    as_of_ns: int
    equity: float
    daily_pnl: float
    daily_pnl_pct_init: float
    margin: float
    risk_degree: float
    gross_notional: float
    net_notional: float
    accounts_by_state: dict[str, int] = {}
    product_net: list[ProductNet] = []
    kill_switch: bool = False
    shards: list[ShardFreshness] = []


@payload()
class FirmControl(Msg, kw_only=True):
    action: FirmAction
    origin: ControlOrigin
    active: bool = False
    restrictions: list[Restriction] = []
    trading_day: str
    actor: str | None = None
    reason: str = ""
    cause_alert_key: str | None = None
    ts_ns: int


class LiquidationStep(msgspec.Struct, kw_only=True, omit_defaults=True):
    instrument_id: str
    direction: Direction
    offset: Offset
    qty: int
    price_rule: str


@payload()
class LiquidationPlan(Msg, kw_only=True):
    plan_id: str
    account_id: str
    trading_day: str
    trigger_alert_key: str
    status: LiquidationStatus
    steps: list[LiquidationStep]
    created_ns: int
    confirmed_by: str | None = None
    confirmed_ns: int | None = None


class MetricPoint(msgspec.Struct, kw_only=True):
    ts_ns: int
    equity: float
    nav: float
    daily_pnl_pct_init: float
    intraday_dd: float
    risk_degree: float
    gross_notional: float
    state: AccountState
    # optional (backward compatible): per-rule utilisation {rule_id/scope_key: util} from metric_samples.rule_util
    # (03 §4.3); the daily report's "盘中峰值" column uses it (01 §5.3.4)
    rule_util: dict[str, float] = {}


@payload()
class MetricSeries(Msg, kw_only=True):
    account_id: str
    trading_day: str
    step_s: int
    points: list[MetricPoint]


@payload()
class RuleSet(Msg, kw_only=True):
    config_version: int
    config_hash: str
    rules: list[dict[str, Any]]


# ---------------------------------------------------------------- commands
@payload()
class ResetRequest(Msg, kw_only=True):
    command_id: str
    account_id: str
    rule_id: str | None = None
    level: str | None = None
    reason: str
    actor: str
    role: Role


@payload()
class ResetAck(Msg, kw_only=True):
    account_state: AccountState
    rearm_threshold: float | None = None


@payload()
class HaltRequest(Msg, kw_only=True):
    command_id: str
    account_id: str
    reason: str
    actor: str
    role: Role


@payload()
class AckAlertRequest(Msg, kw_only=True):
    command_id: str
    alert_key: str
    note: str | None = None
    actor: str
    role: Role


@payload()
class LiquidationConfirm(Msg, kw_only=True):
    command_id: str
    plan_id: str
    decision: str  # "CONFIRM" | "CANCEL"
    actor: str
    role: Role


@payload()
class ReconResolveRequest(Msg, kw_only=True):
    command_id: str
    break_id: str
    resolution: ReconResolution
    note: str
    actor: str
    role: Role


@payload()
class KillSwitchRequest(Msg, kw_only=True):
    command_id: str
    on: bool
    reason: str
    actor: str
    role: Role


@payload()
class QueryAccounts(Msg, kw_only=True):
    account_ids: list[str] | None = None


@payload()
class AccountSnapshotBatch(Msg, kw_only=True):
    items: list[AccountSnapshot]


@payload()
class QueryPositions(Msg, kw_only=True):
    account_id: str | None = None
    product: str | None = None
    instrument_id: str | None = None
    exchange: Exchange | None = None
    sector: str | None = None


@payload()
class PositionBatch(Msg, kw_only=True):
    items: list[PositionView]


@payload()
class QueryOrders(Msg, kw_only=True):
    account_id: str
    trading_day: str
    status: list[OrderStatus] | None = None


@payload()
class OrderBatch(Msg, kw_only=True):
    items: list[OrderUpdate]


@payload()
class QueryFills(Msg, kw_only=True):
    account_id: str
    trading_day: str


@payload()
class FillBatch(Msg, kw_only=True):
    items: list[Trade]


@payload()
class QueryCash(Msg, kw_only=True):
    trading_day: str
    account_id: str | None = None


@payload()
class CashBatch(Msg, kw_only=True):
    items: list[CashMovement]


@payload()
class QueryReconBreaks(Msg, kw_only=True):
    account_id: str | None = None
    status: ReconBreakStatus | None = None


class ReconBreakView(msgspec.Struct, kw_only=True, omit_defaults=True):
    break_id: str
    trading_day: str
    account_id: str
    kind: ReconBreakKind
    status: ReconBreakStatus
    blocking: bool
    resolution: ReconResolution | None = None
    note: str | None = None
    detail: dict[str, Any] = {}
    created_ns: int
    resolved_ns: int | None = None
    resolved_by: str | None = None


@payload()
class ReconBreakBatch(Msg, kw_only=True):
    items: list[ReconBreakView]


@payload()
class QueryAlerts(Msg, kw_only=True):
    trading_day: str
    account_id: str | None = None


@payload()
class RiskEventBatch(Msg, kw_only=True):
    items: list[RiskEvent]


@payload()
class QueryMetrics(Msg, kw_only=True):
    account_id: str
    trading_day: str
    step_s: int = 5


@payload()
class QueryRules(Msg, kw_only=True):
    pass


@payload()
class QueryLiquidationPlans(Msg, kw_only=True):
    """``risk.query.liquidation_plans``: plans of one trading day (None = the current one), optional filters."""

    trading_day: str | None = None
    account_id: str | None = None
    status: LiquidationStatus | None = None


@payload()
class LiquidationPlanBatch(Msg, kw_only=True):
    items: list[LiquidationPlan]
