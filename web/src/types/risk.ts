/**
 * Risk payloads mirrored from `src/futrisk/messages/risk.py` (02 §7.6).
 *
 * `AccountFunds`, `AccountMetricsView`, `ProductNet`, `ShardFreshness`, `MetricPoint`
 * are plain msgspec Structs (no omit_defaults): every field is always present.
 * Other structs omit default-valued fields, which are optional (`?`) here.
 */
import type {
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
  PosDirection,
  ReconBreakKind,
  ReconBreakStatus,
  ReconResolution,
  RestrictionKind,
  RiskEventKind,
  RuleSmState,
  Severity,
  TradingMode,
} from './enums'

export interface Restriction {
  kind: RestrictionKind
  scope_key: string
  value?: number | null
  /** `{rule_id}/{level}` or `SYSTEM:{reason}` */
  source: string
  since_ns?: number
}

/** Intraday funds (float, estimate; clearing is authoritative). */
export interface AccountFunds {
  initial_capital: number
  cum_net_deposit: number
  pre_balance: number
  net_deposit: number
  static_equity: number
  close_pnl: number
  position_pnl: number
  fee: number
  equity: number
  margin: number
  frozen_margin: number
  frozen_fee: number
  available: number
  risk_degree: number
}

export interface AccountMetricsView {
  daily_pnl: number
  /** daily_pnl / initial_capital: the acceptance metric of the daily_loss rule */
  daily_pnl_pct_init: number
  daily_pnl_pct_sod: number
  floating_pnl_pct_init: number
  perf_equity: number
  nav: number
  intraday_peak_equity: number
  intraday_dd: number
  hwm_equity: number
  hwm_dd: number
  gross_notional: number
  net_notional: number
  leverage: number
  effective_n: number
  limit_stress: number
  orders_today: number
  cancels_today: number
}

export interface ExposureView {
  product: string
  sector: string
  exchange: Exchange
  long_lots: number
  short_lots: number
  net_lots: number
  net_notional: number
  gross_notional: number
  net_pct_equity: number
  /** cap = 3% / limit_pct */
  cap_pct?: number | null
  /** net_pct_equity / cap_pct (both unsigned: net_pct_equity = |net_notional| / equity) */
  utilization?: number | null
}

export interface PositionView {
  account_id: string
  instrument_id: string
  product: string
  exchange: Exchange
  direction: PosDirection
  td_qty: number
  yd_qty: number
  closable_qty: number
  closable_td_qty: number
  closable_yd_qty: number
  avg_open_price: number
  basis_avg_price: number
  last?: number | null
  position_pnl?: number
  float_pnl_fifo?: number
  margin?: number
  notional?: number
  pct_equity?: number
  limit_locked?: boolean
}

export interface RuleStatus {
  rule_id: string
  scope_key: string
  metric: string
  value?: number | null
  active_level?: string | null
  sm_state?: RuleSmState
  next_level?: string | null
  next_threshold?: number | null
  /** value relative to the NEXT level (engine formula; drives "who trips first") */
  utilization?: number | null
  headroom_money?: number | null
  /** value relative to the rule's HARD limit (first CIRCUIT_BREAK, else last RESTRICT, else last level); core, when the shard sends it */
  hard_utilization?: number | null
  /** name of that hard-limit level */
  hard_level?: string | null
}

/** `risk.snapshot.{account_id}` payload (LV). */
export interface AccountSnapshot {
  account_id: string
  shard: number
  trading_day: string
  version: number
  state_version: number
  as_of_ns: number
  quality: MdQuality
  state: AccountState
  halted_latched?: boolean
  trading_mode?: TradingMode
  restrictions?: Restriction[]
  funds: AccountFunds
  metrics: AccountMetricsView
  exposures?: ExposureView[]
  positions?: PositionView[]
  rules?: RuleStatus[]
  open_orders?: number
}

/** `risk.state.{account_id}` payload (FACT); also the `risk.halt` reply data. */
export interface AccountStateChanged {
  account_id: string
  trading_day: string
  from_state: AccountState
  to_state: AccountState
  halted_latched: boolean
  state_version: number
  restrictions: Restriction[]
  cause: string
  cause_alert_key?: string | null
  actor?: string | null
  reason?: string | null
  ts_ns: number
}

/** `risk.event` payload (FACT); also `risk.query.alerts` items. */
export interface RiskEvent {
  event_id: string
  kind: RiskEventKind
  trading_day: string
  ts_ns: number
  account_id?: string | null
  scope_key?: string | null
  alert_key?: string | null
  rule_id?: string | null
  level?: string | null
  episode?: number | null
  action?: Action | null
  severity?: Severity
  latched?: boolean | null
  metric?: string | null
  value?: number | null
  threshold?: number | null
  actor?: string | null
  message?: string
  detail?: Record<string, unknown>
}

export interface CashMovement {
  movement_id: string
  account_id: string
  trading_day: string
  type: CashType
  amount: string
  actor: string
  reason: string
  ts_ns: number
}

export interface ProductNet {
  product: string
  net_lots: number
  net_notional: number
  gross_notional: number
  accounts: number
}

export interface ShardFreshness {
  shard: number
  as_of_ns: number
  quality: MdQuality
}

/** `risk.firm` payload (LV, coordinator, 1Hz). */
export interface FirmSnapshot {
  trading_day: string
  as_of_ns: number
  equity: number
  daily_pnl: number
  daily_pnl_pct_init: number
  margin: number
  risk_degree: number
  gross_notional: number
  net_notional: number
  accounts_by_state?: Partial<Record<AccountState, number>>
  product_net?: ProductNet[]
  kill_switch?: boolean
  shards?: ShardFreshness[]
}

export interface FirmControl {
  action: FirmAction
  origin: ControlOrigin
  active?: boolean
  restrictions?: Restriction[]
  trading_day: string
  actor?: string | null
  reason?: string
  cause_alert_key?: string | null
  ts_ns: number
}

export interface LiquidationStep {
  instrument_id: string
  direction: Direction
  offset: Offset
  qty: number
  price_rule: string
}

/** Sent with `risk.event` kind LIQUIDATION_PLAN; also the `risk.liquidation.confirm` reply data. */
export interface LiquidationPlan {
  plan_id: string
  account_id: string
  trading_day: string
  trigger_alert_key: string
  status: LiquidationStatus
  steps: LiquidationStep[]
  created_ns: number
  confirmed_by?: string | null
  confirmed_ns?: number | null
}

export interface MetricPoint {
  ts_ns: number
  equity: number
  nav: number
  daily_pnl_pct_init: number
  intraday_dd: number
  risk_degree: number
  gross_notional: number
  state: AccountState
  /** per-rule utilization at that sample (rule_id -> utilization) */
  rule_util?: Record<string, number>
}

/** `risk.query.metrics` reply data; `GET /accounts/{id}/equity-curve`. */
export interface MetricSeries {
  account_id: string
  trading_day: string
  step_s: number
  points: MetricPoint[]
}

/**
 * One level of a rule definition: `RuleLevel.model_dump(mode="json")` of
 * `futrisk.common.config.risk_rules` (every field present in real answers).
 */
export interface RuleLevelDef {
  name: string
  threshold: number
  action: Action
  params?: Record<string, unknown>
  latch?: boolean
  confirm?: { states?: number; ms?: number }
  rearm?: { extra?: number } | null
}

export type RuleOp = '<=' | '>=' | '<' | '>'

/** RuleDef = `RuleDef.model_dump(mode="json")` (02 §7.6 RuleSet.rules; `risk_rules.yaml` one-to-one). */
export interface RuleDef {
  id: string
  metric: string
  op: RuleOp
  scope?: string
  hysteresis?: number
  cooldown_s?: number
  hold_ms?: number
  pretrade?: boolean
  /** disabled rules are skipped by the gateway's compare view */
  enabled?: boolean
  reset_on_day_roll?: boolean
  /** evaluate only while gross notional / equity >= this; below it the rule counts as not breached */
  min_gross_pct?: number | null
  levels: RuleLevelDef[]
  /** per-account overrides, e.g. `{ACC2: {halt: {threshold: -0.04}}}` */
  overrides?: Record<string, Record<string, Partial<RuleLevelDef>>>
}

/** `risk.query.rules` reply data; `GET /risk/rules`. */
export interface RuleSet {
  config_version: number
  config_hash: string
  rules: RuleDef[]
}

export interface ResetAck {
  account_state: AccountState
  rearm_threshold?: number | null
}

export interface ReconBreakView {
  break_id: string
  trading_day: string
  account_id: string
  kind: ReconBreakKind
  status: ReconBreakStatus
  blocking: boolean
  resolution?: ReconResolution | null
  note?: string | null
  detail?: Record<string, unknown>
  created_ns: number
  resolved_ns?: number | null
  resolved_by?: string | null
}
