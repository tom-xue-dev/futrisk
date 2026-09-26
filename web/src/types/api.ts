/**
 * REST (02 §9.6) and WebSocket (02 §9.7) protocol types, aligned with the implemented
 * api-gateway (`src/futrisk/services/api_gateway`: routes/*.py, readmodel.py, views.py,
 * wshub.py). The mock backend builds its answers from these same types, so a shape
 * change here is checked on both sides by the compiler.
 */
import type {
  AccountState,
  ClockTarget,
  EodPhase,
  LimitLock,
  MdQuality,
  ProcessState,
  Role,
  ServiceState,
  SourceId,
  TradingMode,
} from './enums'
import type { AccountSettlement, BacktestStatus, DayRoll, EodPhaseEvent, ReportReady, SettlementCompleted, SettlementResult } from './eod'
import type { MdEvent, MdStatus } from './md'
import type { OrderUpdate } from './orders'
import type { AccountSnapshot, AccountStateChanged, FirmControl, FirmSnapshot, LiquidationPlan, PositionView, Restriction, RiskEvent } from './risk'
import type { ClockState, Heartbeat, LatencySummary, ServiceStateChanged } from './system'

// ---------------------------------------------------------------- REST envelope

/**
 * Uniform REST body (02 §9.6, `rest.py`); the code is mirrored in the `X-Result-Code`
 * response header. `duplicate: true` marks an idempotent replay (same Idempotency-Key,
 * same request); the same key with a different request answers 409 CONFLICT.
 */
export interface ApiEnvelope<T = unknown> {
  ok: boolean
  code: string
  message?: string | null
  data?: T
  duplicate?: boolean
}

// ---------------------------------------------------------------- WebSocket frames

export type WsMsgType = 'snapshot' | 'upsert' | 'delete' | 'event' | 'heartbeat' | 'resync' | 'error' | 'pong'

/**
 * Server -> client frame (02 §9.7, `wshub.py`). Topic frames `{v, type, topic, seq, ts,
 * mts, td, data}`: `seq` counts per topic and connection from 1; session frames
 * (`heartbeat`, `pong`, `error`) are `{v, type, ts, mts, data}` without topic / seq / td.
 * `ts` is the gateway's wall clock in epoch **milliseconds** (staleness), `mts` the
 * market clock in epoch ms (the demo clock can be offset from wall time).
 */
export interface WsServerMessage<T = unknown> {
  v: number
  type: WsMsgType
  topic?: string
  seq?: number
  ts?: number
  /** market time (epoch ms) */
  mts?: number
  td?: string | null
  data?: T
}

/** Client -> server ops (02 §9.7). The gateway answers `sub` and `resync` with a snapshot. */
export type WsClientOp =
  | { op: 'auth'; key: string }
  | { op: 'sub'; topics: string[] }
  | { op: 'unsub'; topics: string[] }
  | { op: 'resync'; topics: string[] }
  | { op: 'ping'; ts: number }

/** Static WS topics; keyed topics are `positions:{id}`, `orders:{id}`, `equity:{id}`. */
export const WS_TOPICS = ['quotes', 'feeds', 'accounts', 'events', 'compare', 'firm', 'system', 'eod', 'backtests'] as const
export type WsStaticTopic = (typeof WS_TOPICS)[number]

/** Event-class topics (never dropped; a server `resync` means "reload via REST"). */
export function isEventTopic(topic: string): boolean {
  return topic === 'events' || topic === 'eod' || topic.startsWith('orders:') || topic.startsWith('equity:')
}

/** WebSocket close code of a rejected / revoked / throttled authentication (`wshub.CLOSE_UNAUTHORIZED`). */
export const WS_CLOSE_UNAUTHORIZED = 4401
/** Policy close codes: 1008 (session flood), 1009 (message too big), 1013 (slow consumer, try later). */
export const WS_CLOSE_POLICY = 1008
export const WS_CLOSE_TOO_BIG = 1009
export const WS_CLOSE_TRY_AGAIN = 1013
/** Gateway limits of one client op. */
export const WS_MAX_TOPICS_PER_OP = 100
export const WS_MAX_MESSAGE_BYTES = 64 * 1024

// ---------------------------------------------------------------- topic payloads

/** WS `quotes` item (`readmodel.quote_item`); `GET /market/quotes` returns Ticks instead. */
export interface QuoteView {
  instrument_id: string
  last: number
  bid1?: number | null
  ask1?: number | null
  volume?: number
  change_pct?: number | null
  source?: SourceId | null
  lag_ms?: number | null
  quality?: MdQuality
  exchange?: string
  upper_limit?: number | null
  lower_limit?: number | null
  pre_settle?: number | null
  /** only when md.status is known to the gateway */
  limit_lock?: LimitLock
  /** browser receive time (ms), set by the store */
  updated_ms?: number
}

/**
 * Account summary used by the UI: a WS `accounts` item (`readmodel.account_summary` +
 * `name` + owner liveness), a `GET /accounts` snapshot, or a configured account without a
 * snapshot (`missing: true`). Numbers the gateway does not have are null here.
 */
export interface AccountSummary {
  account_id: string
  shard: number | null
  state: AccountState
  halted_latched: boolean
  equity: number | null
  daily_pnl: number | null
  daily_pnl_pct_init: number | null
  risk_degree: number | null
  worst_utilization: number | null
  quality: MdQuality | null
  name?: string
  worst_rule?: string | null
  /** level of the worst rule (hard-limit level with a core `hard_utilization`) */
  worst_level?: string | null
  state_version?: number
  as_of_ns?: number
  trading_mode?: TradingMode
  trading_day?: string
  initial_capital?: number
  margin?: number
  open_orders?: number
  /** owner shard liveness: the shard is down and this is its last known data */
  stale?: boolean
  /** ms since the owning shard was last heard from */
  age_ms?: number | null
  /** configured account without a snapshot in the read model */
  missing?: boolean
}

/** Owner liveness of an account (`readmodel.liveness`): `stale` = the owning shard is not UP. */
export interface OwnerLiveness {
  stale: boolean
  age_ms: number | null
}

/** `GET /accounts/{id}` and the snapshot items of `GET /accounts`: effective state merged, name, owner liveness. */
export type AccountSnapshotView = AccountSnapshot & OwnerLiveness & { name?: string; missing?: false }

/** `GET /accounts` item of a configured account without a snapshot (a known `risk.state` is merged in). */
export interface AccountMissingItem extends OwnerLiveness {
  account_id: string
  name: string
  shard: number
  missing: true
  state?: AccountState
  halted_latched?: boolean
  state_version?: number
}

/** `GET /accounts`: every configured account. */
export type AccountListItem = AccountSnapshotView | AccountMissingItem

/** WS `equity:{account_id}` point (`readmodel.sample_equity`). */
export interface EquityPoint {
  ts_ns: number
  equity: number
  daily_pnl_pct_init: number
  intraday_dd: number
  state?: AccountState
  nav?: number
  risk_degree?: number
}

export type Liveness = 'UP' | 'DOWN' | 'UNKNOWN'

/**
 * WS `system` snapshot item (`WsHub.system_items`): one `kind: "service"` item per
 * instance, plus the market clock as a `kind: "clock"` item (instance `sys.clock`).
 */
export interface SystemTopicItem {
  kind: 'service'
  instance: string
  restarts: number
  service?: string
  /** heartbeat ServiceState, or the supervisor's ProcessState when the process is gone */
  state?: ServiceState | ProcessState
  queues?: Record<string, number>
  rates?: Record<string, number>
  latency?: Record<string, LatencySummary>
  loop_lag_ms?: number
  config_version?: number
  config_hash?: string
  clock_version?: number
  pid?: number
  started_ns?: number
  trading_mode?: TradingMode | null
  deps?: Record<string, string>
  liveness?: Liveness
}

/** Clock item of the WS `system` topic. */
export type ClockTopicItem = ClockState & { kind: 'clock'; instance: 'sys.clock' }

/** WS `system` events: process transitions, kill-switch progress, config 2PC alerts. */
export type SystemServiceEvent = ServiceStateChanged & { kind: 'service' }
export type SystemKillSwitchEvent = KillSwitchRow & { kind: 'kill_switch'; event: 'command' | 'retry' | 'reconcile' | string }
export interface SystemConfigEvent {
  kind: 'config'
  /** e.g. `CONFIG_FAILED` (notified CRITICAL), with `config_version`, `lagging` … */
  event: string
  [key: string]: unknown
}
export type SystemEvent = SystemServiceEvent | SystemKillSwitchEvent | SystemConfigEvent

/** WS `firm` snapshot: the FirmSnapshot plus the kill-switch layers (null before the first one). */
export type FirmTopicSnapshot = FirmSnapshot & { control: FirmControlState | null }

/**
 * Items of the WS `eod` topic. An EodPhaseEvent whose `detail.warning` is set is a WARN
 * (LATE_FACT, OFFICIAL_PRICE_DIFF, PUSH_INCOMPLETE) repeating the run's current phase —
 * not a phase change; PRE_SETTLE_MISMATCH items come in `detail.warnings` of PRICES_FIXED.
 */
export type EodTopicItem = EodPhaseEvent | SettlementCompleted | ReportReady | DayRoll

/** `data` of each WS topic family (snapshot and incremental frames). */
export interface WsTopicPayloads {
  quotes: QuoteView | QuoteView[]
  feeds: MdStatus | MdEvent | null
  accounts: AccountSummary | AccountSummary[]
  positions: PositionView[]
  orders: OrderUpdate | OrderUpdate[]
  equity: EquityPoint | EquityPoint[]
  events: RiskEvent | AccountStateChanged | RiskEvent[]
  compare: CompareView
  firm: FirmTopicSnapshot | FirmControl | null
  system: (SystemTopicItem | ClockTopicItem)[] | SystemEvent
  eod: EodTopicItem | EodTopicItem[]
  /** state topic: snapshot = every run (newest first), upsert per run */
  backtests: BacktestStatus | BacktestStatus[]
}
export type WsTopicFamily = keyof WsTopicPayloads

// ---------------------------------------------------------------- system

/** `GET /system/services` item: supervisor ServiceInfo (when reachable) + latest heartbeat. */
export interface ServiceItem {
  instance: string
  service: string
  /** supervisor ProcessState; absent when the supervisor is unreachable and no sys.service was seen */
  state?: ProcessState
  process_state?: ProcessState
  pid?: number | null
  started_ns?: number | null
  restarts: number
  last_exit_code?: number | null
  heartbeat_state?: ServiceState | null
  last_heartbeat_ns?: number | null
  heartbeat?: Heartbeat
  /** the gateway's peer view: UP / DOWN / UNKNOWN (never seen) */
  liveness?: Liveness
  /** since when the process state holds (supervisor value, else the last sys.service transition, else the start) */
  since_ns: number | null
  /** when the gateway last received a heartbeat of the instance */
  heartbeat_ns: number | null
  /** supervisor extensions */
  env_keys?: string[] | null
  last_error?: string | null
}

export interface SupervisorProbe {
  ok: boolean
  code: string
  message: string
}

/** `GET /system/services` */
export interface SystemServicesResponse {
  items: ServiceItem[]
  events: ServiceStateChanged[]
  supervisor: SupervisorProbe
}

/** Normalized service health row used by the UI (REST item and WS item merged). */
export interface ServiceHealth {
  instance: string
  service: string
  process_state: ProcessState | null
  heartbeat_state: ServiceState | null
  liveness: Liveness | null
  pid: number | null
  restarts: number
  started_ns: number | null
  last_heartbeat_ns: number | null
  last_exit_code: number | null
  loop_lag_ms: number | null
  queues: Record<string, number>
  rates: Record<string, number>
  latency: Record<string, LatencySummary>
  config_version: number | null
  config_hash: string | null
  clock_version: number | null
  trading_mode: TradingMode | null
  deps: Record<string, string>
  heartbeat?: Heartbeat | null
  /** since when `process_state` holds (REST `since_ns`) */
  since_ns: number | null
  /** last heartbeat received by the gateway (REST `heartbeat_ns`) */
  heartbeat_ns: number | null
  /** supervisor's last failure cause */
  last_error: string | null
  /** browser time (ms) of the last update */
  updated_ms: number
}

export interface ShardStatus {
  state: string
  trading_day: string | null
  trading_mode: TradingMode | null
}

export type KillSwitchTargetStatus = 'PENDING' | 'CONFIRMED' | 'FAILED'

/** Confirmation of the desired kill-switch state by one target (`store.KillSwitchTarget`). */
export interface KillSwitchTargetState {
  status: KillSwitchTargetStatus
  code: string | null
  message: string
  attempts: number
  updated_ns: number
}

/** The persisted manual kill-switch decision and its per-target confirmation (`store.KillSwitchRow.to_dict`). */
export interface KillSwitchRow {
  desired_on: boolean
  command_id: string
  reason: string
  actor: string
  role: Role
  requested_ns: number
  updated_ns: number
  targets: Record<string, KillSwitchTargetState>
  pending: string[]
  failed: string[]
  confirmed: string[]
}

/** `/system/status.kill_switch_control`: the row plus whether retries are running (null before any command). */
export type KillSwitchControl = KillSwitchRow & { retrying: boolean }

/** `/system/status.kill_switch_retry` (`Gateway.kill_switch_retry`). */
export interface KillSwitchRetry {
  pending: string[]
  failed: string[]
  desired: { on: boolean; reason: string; actor: string; command_id: string } | null
  updated_ns: number | null
  retrying: boolean
}

/** Kill switch = MANUAL OR RULE (`ReadModel.firm_control_state`; null before any `risk.control.firm`). */
export interface FirmControlState {
  kill_switch: boolean
  manual: { active: boolean; actor: string | null; reason: string; ts_ns: number } | null
  /** the coordinator's rule layer (RULE_STATE): rule kill switch and every firm restriction in force */
  rule: { active: boolean; restrictions: Restriction[]; ts_ns: number; reason?: string; cause_alert_key?: string | null } | null
}

/** `/system/status.auth`: what the last key sync found (`auth.SyncReport.to_dict`) + throttled logins. */
export interface AuthStatus {
  enabled?: string[]
  /** named keys whose `key_env` variable is not set */
  missing_env?: string[]
  /** named keys refused (too short, duplicate plaintext …): name -> why */
  rejected?: Record<string, string>
  /** configured named ADMIN keys that are not enabled */
  admin_unavailable?: string[]
  bootstrap_active?: boolean
  throttled: number
}

/** `/system/status.notifications`: rows per status (+ the notifier's state). */
export interface NotificationsStatus {
  failed: number
  suppressed: number
  pending: number
  counts: Record<string, number>
  channel?: string
  waiting?: number
  stats?: Record<string, unknown>
}

/** `GET /system/status` (read-model summary and what an operator must act on). */
export interface SystemStatus {
  instance: string
  state: string
  /** app.mode: `demo` enables /demo/* */
  mode: string
  trading_day: string
  trading_mode: TradingMode | null
  /** effective firm kill switch (FirmSnapshot, else the control layers); null = unknown */
  kill_switch: boolean | null
  /** absent in captures of gateways before the rule layer was added */
  firm_control?: FirmControlState | null
  kill_switch_retry: KillSwitchRetry
  kill_switch_control: KillSwitchControl | null
  clock: ClockState
  config: { config_version: number; config_hash: string }
  services: { total: number; up: number; down: string[] }
  shards: Record<string, ShardStatus | null>
  readmodel: {
    synced: Record<string, boolean>
    accounts: number
    instruments: number
    applied: number
    decode_errors: number
    accounts_stale: string[]
    repair: { gaps: number; repairs: number; repair_failures: number; resyncs: number } | null
  }
  ws: { clients: number; subscriptions: Record<string, number>; queued: number; queued_bytes: number; overflows: number; evictions: number; revoked: number }
  auth: AuthStatus
  notifications: NotificationsStatus
  service_events: ServiceStateChanged[]
}

/** `GET /health` (public). */
export interface HealthInfo {
  status: string
  instance: string
  state: string
  home_id: string
  pid: number
  epoch: string
  version: string
  trading_day: string
  ts_ns: number
}

/** `GET /market/feeds`: md.status (read model, else md.sources) + md-gateway liveness. */
export type MdFeedsView = MdStatus & OwnerLiveness

/** `GET /auth/me` */
export interface AuthMe {
  key_id: string
  name: string
  role: Role
}

// ---------------------------------------------------------------- risk

/** `GET /risk/events`: `errors` lists shards that did not answer (their events come from the read model only). */
export interface RiskEventsResponse {
  trading_day: string
  items: RiskEvent[]
  errors: Record<string, string>
}

/** `GET /risk/state` row. */
export interface RiskStateRow {
  account_id: string
  shard: number | null
  trading_day: string | null
  state: AccountState
  halted_latched: boolean
  state_version: number
  restrictions: Restriction[]
  trading_mode: TradingMode | null
  last_change: AccountStateChanged | null
  stale: boolean
  age_ms: number | null
}

/** One target of a fan-out command (`router.TargetResult.to_dict`). */
export interface TargetResult<T = unknown> {
  target: string
  ok: boolean
  code: string
  message: string
  duplicate: boolean
  data: T | null
}

/**
 * `POST /risk/firm/kill-switch` data. Always HTTP 200: `ok` only when every target
 * confirmed, otherwise `ok=false` with the worst target code; `retrying` = targets the
 * gateway keeps retrying (retryable answers), `failed` = targets it gave up on.
 */
export interface KillSwitchResult {
  targets: TargetResult<FirmControl>[]
  all_ok: boolean
  control: FirmControl | null
  retrying: string[]
  failed: string[]
}

/**
 * A fan-out command's per-target report (`Gateway.fanout_result`, e.g.
 * `POST /risk/eod/{td}/force-unfreeze`): always HTTP 200, `ok` only when every target
 * confirmed, otherwise `ok=false` with the worst target code.
 */
export interface FanoutResult<T = unknown> {
  targets: TargetResult<T>[]
  all_ok: boolean
}

/** `GET /risk/liquidation-plans` item: a plan of a shard that did not answer comes from the read model, `stale`. */
export type LiquidationPlanView = LiquidationPlan & { stale?: boolean }

/** `GET /risk/liquidation-plans?account=&status=&td=`. */
export interface LiquidationPlansResponse {
  trading_day: string
  items: LiquidationPlanView[]
  errors: Record<string, string>
}

// ---------------------------------------------------------------- positions

export type PositionGroupBy = 'account' | 'product' | 'instrument' | 'exchange' | 'sector'

export interface PositionRow extends PositionView {
  sector: string | null
}

export interface PositionGroup {
  group: string
  positions: number
  accounts: string[]
  instruments: string[]
  long_qty: number
  short_qty: number
  net_qty: number
  td_qty: number
  yd_qty: number
  gross_notional: number
  net_notional: number
  margin: number
  position_pnl: number
  float_pnl_fifo: number
}

/** `GET /positions?account=&product=&instrument=&exchange=&sector=&group_by=` */
export interface PositionsResponse {
  group_by: PositionGroupBy | null
  items: PositionRow[]
  groups: PositionGroup[]
  /** configured accounts without a snapshot in the read model */
  accounts_missing: string[]
  /** accounts whose owner shard is down: their rows are the last known snapshot */
  accounts_stale: string[]
}

// ---------------------------------------------------------------- compare

export type UtilBand = 'ok' | 'watch' | 'warn' | 'breach' | 'na'

/** One heat-map cell: worst scope of one rule for one account (`views.build_matrix`). */
export interface CompareCell {
  account_id: string
  rule_id: string
  scope_key: string
  metric: string
  value: number | null
  /** hard limit of the rule (metric units) */
  limit: number | null
  /** value ÷ limit after normalizing to "bigger is more dangerous" */
  utilization: number | null
  band: UtilBand
  active_level: string | null
}

/** Headroom ranking row (`views.build_ranking`). */
export interface HeadroomItem {
  account_id: string
  rule_id: string
  scope_key: string
  metric: string
  value: number | null
  next_level: string | null
  next_threshold: number | null
  utilization_next: number | null
  headroom: number | null
  headroom_money: number | null
}

export interface ExposureGridCell {
  product: string
  account_id: string
  net_notional: number
  net_lots: number
}

export interface ExposureGrid {
  products: string[]
  accounts: string[]
  cells: ExposureGridCell[]
  firm_net: Record<string, number>
}

/** `GET /risk/compare` body and WS `compare` data (`views.build_compare_view`). */
export interface CompareView {
  trading_day: string
  as_of_ns: number
  accounts: string[]
  rules: string[]
  matrix: CompareCell[]
  ranking: HeadroomItem[]
  exposure: ExposureGrid
  /** owner liveness per account in the view */
  liveness?: Record<string, OwnerLiveness>
  /** accounts whose owner shard is down: their figures are the last ones received */
  accounts_stale?: string[]
}

/** One settled day of `GET /risk/compare/history`. */
export interface CompareHistoryDay {
  trading_day: string
  version?: number
  accounts: AccountSettlement[]
  /** circuit breakers per account (every configured account, zeros included); absent when a shard did not answer */
  breakers?: Record<string, number>
}

/** `GET /risk/compare/history?days=` (1..60): settled days, days without settlement, days that failed. */
export interface CompareHistory {
  days: CompareHistoryDay[]
  missing?: string[]
  errors?: Record<string, string>
}

// ---------------------------------------------------------------- settlement

export interface SettlementDayItem {
  trading_day: string
  version: number
  phase: EodPhase
  /** WARN events of the run (LATE_FACT, OFFICIAL_PRICE_DIFF, PUSH_INCOMPLETE, PRE_SETTLE_MISMATCH) */
  warning_count: number
}

export type EodWarningKind = 'LATE_FACT' | 'OFFICIAL_PRICE_DIFF' | 'PUSH_INCOMPLETE' | 'PRE_SETTLE_MISMATCH'

/**
 * One EOD warning: from `GET /settlement/{td}/status` `detail.warnings` (`{kind, message}`;
 * the read model adds trading_day / version / phase / warn_id / ts_ns / context), or from a
 * WS `eod` WARN event (`detail.warning` = message, `detail.kind`, `detail.warn_id`).
 */
export interface EodWarning {
  kind: EodWarningKind | string
  message: string
  trading_day?: string
  version?: number
  phase?: EodPhase | string
  warn_id?: string
  ts_ns?: number
  context?: Record<string, unknown>
}

/** `GET /settlement/{td}/statements/{account}`: the statement file, or the settlement data when it is missing. */
export interface StatementResponse {
  format: 'text' | 'settlement'
  text?: string
  settlement?: SettlementResult
}

/** `GET /settlement/days?limit=`: days with an EOD phase; `errors` = days clearing-worker could not answer. */
export interface SettlementDaysResponse {
  items: SettlementDayItem[]
  errors: Record<string, string>
}

// ---------------------------------------------------------------- demo

export interface ClockJumpStep {
  step: number
  action: string
  ok: boolean
  detail: unknown
}

/** `POST /demo/clock` data (`clockjump.JumpResult.to_dict`). */
export interface ClockJumpResult {
  clock: ClockState | null
  steps: ClockJumpStep[]
  waited_ms: number
  converged: boolean
  lagging: string[]
}

/** sim-exchange fault (`faults.Fault.to_dict`). */
export interface SimFault {
  fault_id: string
  type: string
  target: string | null
  params: Record<string, unknown>
  duration_s: number | null
  state: string
  created_ns: number
  remaining_s: number | null
  detail: Record<string, unknown>
}

export interface DemoFaultResult {
  fault_id: string
  state: string
  fault: SimFault
}

export interface DemoShockResult {
  shock_id: string
  shock: Record<string, unknown>
}

export interface DemoScenarioResult {
  run_id: string
  name: string
}

/** `GET /demo/scenarios` item (config/scenarios/*.yaml). */
export interface DemoScenarioInfo {
  name: string
  description: string
}

export type ScenarioRunStatus = 'RUNNING' | 'DONE' | 'CANCELED' | 'FAILED'

/** The sim's current / last scenario run (`ScenarioRun.to_dict`); `DELETE /demo/scenario` answers it too. */
export interface ScenarioRunState {
  run_id: string
  name: string
  status: ScenarioRunStatus | string
  speed: number
  step: number
  steps_total: number
  started_ns: number
  error: string | null
  vars: Record<string, unknown>
  faults: string[]
  shocks: string[]
  log: Record<string, unknown>[]
}

/** `GET /demo/state` (sim `GET /control/state`; fields the dashboard reads). */
export interface DemoState {
  epoch?: string
  trading_day?: string
  faults: SimFault[]
  fault_history: SimFault[]
  scenario: ScenarioRunState | null
  paused?: boolean
  market_now_ns?: number
  clock?: { mode: string; offset_ns: number; version: number }
  shocks?: Record<string, unknown>[]
  [key: string]: unknown
}

/** `GET /demo/faults`: active faults and the ones that ended. */
export interface DemoFaultsResponse {
  items: SimFault[]
  history: SimFault[]
}

// ---------------------------------------------------------------- request bodies (pydantic, extra="forbid")

export interface ResetBody {
  reason: string
  rule_id?: string
  level?: string
}

export interface HaltBody {
  reason: string
}

/** `POST /risk/eod/{td}/force-unfreeze` (RISK_MANAGER; only valid while a shard is PROVISIONAL). */
export interface ForceUnfreezeBody {
  reason: string
}

export interface KillSwitchBody {
  on: boolean
  reason: string
}

export interface LiquidationConfirmBody {
  decision: 'CONFIRM' | 'CANCEL'
}

export interface AckAlertBody {
  note?: string
}

export interface DemoFaultBody {
  type: string
  target?: string
  params: Record<string, unknown>
  duration_s?: number
}

export interface DemoShockBody {
  instruments?: string[]
  sector?: string
  pct: number
  duration_s: number
  shape: 'STEP' | 'LINEAR'
}

export interface DemoScenarioBody {
  name: string
  speed?: number
  vars?: Record<string, unknown>
}

/** `POST /demo/clock`: `at_ns` wins over `at` (ISO 8601 market time or epoch ns). */
export interface DemoClockBody {
  target: ClockTarget
  at?: string | number
  at_ns?: number
}

export interface SettleRunBody {
  td: string
  force_new_version: boolean
}

export interface BacktestRunBody {
  config_name: string
  overrides?: Record<string, unknown>
}
