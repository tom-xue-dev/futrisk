/**
 * Type guards and normalizers for REST/WS payloads of the api-gateway.
 *
 * WS frames carry `type` = snapshot/upsert/event but not the payload type name, so
 * payloads that share a topic (e.g. RiskEvent vs AccountStateChanged on `events`) are
 * told apart by their fields. Fields the gateway leaves out (msgspec omit_defaults) are
 * read with their defaults.
 */
import type {
  AccountSnapshot,
  AccountState,
  AccountStateChanged,
  AccountSummary,
  ClockState,
  CompareHistory,
  CompareHistoryDay,
  CompareView,
  DayRoll,
  EodPhaseEvent,
  EodWarning,
  EquityPoint,
  FirmSnapshot,
  Heartbeat,
  LatencySummary,
  LiquidationPlan,
  Liveness,
  MdEvent,
  MdQuality,
  MdStatus,
  ProcessState,
  QuoteView,
  ReportReady,
  Restriction,
  RiskEvent,
  ServiceHealth,
  ServiceState,
  ServiceStateChanged,
  SettlementCompleted,
  SettlementResult,
  SystemTopicItem,
  Tick,
} from '@/types'
import {
  ACCOUNT_STATE_VALUES,
  MD_EVENT_KIND_VALUES,
  RISK_EVENT_KIND_VALUES,
  TICK_FLAG_LIMIT_DOWN_LOCKED,
  TICK_FLAG_LIMIT_UP_LOCKED,
} from '@/types'
import { isNum, toNs } from './format'
import { worstUtilization } from './risk'

export type Obj = Record<string, unknown>

export function isObj(v: unknown): v is Obj {
  return typeof v === 'object' && v !== null && !Array.isArray(v)
}

function str(v: unknown): string | null {
  return typeof v === 'string' ? v : null
}

function num(v: unknown, dflt: number | null = null): number | null {
  return isNum(v) ? v : dflt
}

/** List payloads: a bare array, or an object with `items` / `data` / `results` / `list`. */
export function asItems<T = unknown>(data: unknown): T[] {
  if (Array.isArray(data)) return data as T[]
  if (isObj(data)) {
    for (const k of ['items', 'data', 'results', 'list', 'rows']) {
      const v = data[k]
      if (Array.isArray(v)) return v as T[]
    }
  }
  return []
}

// ---------------------------------------------------------------- guards

export function isAccountSnapshot(x: unknown): x is AccountSnapshot {
  return isObj(x) && typeof x.account_id === 'string' && isObj(x.funds) && isObj(x.metrics)
}

export function isRiskEvent(x: unknown): x is RiskEvent {
  return isObj(x) && typeof x.event_id === 'string' && (RISK_EVENT_KIND_VALUES as readonly string[]).includes(String(x.kind))
}

export function isAccountStateChanged(x: unknown): x is AccountStateChanged {
  return isObj(x) && typeof x.account_id === 'string' && typeof x.to_state === 'string' && isNum(x.state_version)
}

export function isMdStatus(x: unknown): x is MdStatus {
  return isObj(x) && Array.isArray(x.sources) && Array.isArray(x.instruments)
}

export function isMdEvent(x: unknown): x is MdEvent {
  return isObj(x) && typeof x.event_id === 'string' && (MD_EVENT_KIND_VALUES as readonly string[]).includes(String(x.kind))
}

export function isServiceStateChanged(x: unknown): x is ServiceStateChanged {
  return isObj(x) && typeof x.instance === 'string' && typeof x.from_state === 'string' && typeof x.to_state === 'string' && typeof x.reason === 'string'
}

export function isEodPhaseEvent(x: unknown): x is EodPhaseEvent {
  return isObj(x) && typeof x.trading_day === 'string' && typeof x.phase === 'string'
}

export function isSettlementCompleted(x: unknown): x is SettlementCompleted {
  return isObj(x) && typeof x.trading_day === 'string' && typeof x.next_trading_day === 'string' && Array.isArray(x.accounts) && x.phase === undefined
}

export function isReportReady(x: unknown): x is ReportReady {
  return isObj(x) && typeof x.report_id === 'string' && typeof x.type === 'string' && typeof x.path === 'string'
}

export function isDayRoll(x: unknown): x is DayRoll {
  return isObj(x) && typeof x.mode === 'string' && typeof x.from_trading_day === 'string' && typeof x.to_trading_day === 'string'
}

export function isFirmSnapshot(x: unknown): x is FirmSnapshot {
  return isObj(x) && isNum(x.equity) && isNum(x.daily_pnl) && typeof x.trading_day === 'string'
}

function isAccountState(v: unknown): v is AccountState {
  return typeof v === 'string' && (ACCOUNT_STATE_VALUES as readonly string[]).includes(v)
}

function quality(v: unknown): MdQuality {
  return v === 'STALE' || v === 'UNKNOWN' ? v : 'OK'
}

// ---------------------------------------------------------------- accounts

/**
 * Account summary from a WS `accounts` item or a full AccountSnapshot. The gateway sends a
 * reduced item (account_id / state / halted_latched / state_version / as_of_ns) while it
 * knows a `risk.state` but no snapshot yet: the missing numbers are null here, and
 * `mergeAccountSummary` keeps previously known values for them.
 */
export function toAccountSummary(x: unknown): AccountSummary | null {
  if (!isObj(x) || typeof x.account_id !== 'string') return null
  if (x.missing === true && !isAccountState(x.state)) return null // configured, nothing known yet
  if (isAccountSnapshot(x)) {
    const w = worstUtilization(x.rules)
    const s: AccountSummary = {
      account_id: x.account_id,
      shard: x.shard,
      state: x.state,
      halted_latched: Boolean(x.halted_latched),
      equity: x.funds.equity,
      daily_pnl: x.metrics.daily_pnl,
      daily_pnl_pct_init: x.metrics.daily_pnl_pct_init,
      risk_degree: x.funds.risk_degree,
      worst_utilization: w.value,
      worst_rule: w.rule,
      worst_level: w.level,
      quality: quality(x.quality),
      state_version: x.state_version,
      as_of_ns: x.as_of_ns,
      trading_mode: x.trading_mode,
      trading_day: x.trading_day,
      initial_capital: x.funds.initial_capital,
      margin: x.funds.margin,
      open_orders: x.open_orders,
    }
    const name = (x as { name?: unknown }).name
    if (typeof name === 'string') s.name = name
    copyLiveness(x, s)
    return s
  }
  if (!isAccountState(x.state)) return null
  const out: AccountSummary = {
    account_id: x.account_id,
    shard: num(x.shard),
    state: x.state,
    halted_latched: Boolean(x.halted_latched),
    equity: num(x.equity),
    daily_pnl: num(x.daily_pnl),
    daily_pnl_pct_init: num(x.daily_pnl_pct_init),
    risk_degree: num(x.risk_degree),
    worst_utilization: num(x.worst_utilization),
    quality: typeof x.quality === 'string' ? quality(x.quality) : null,
  }
  if (typeof x.name === 'string') out.name = x.name
  if (isNum(x.state_version)) out.state_version = x.state_version
  if (isNum(x.as_of_ns)) out.as_of_ns = x.as_of_ns
  if (typeof x.worst_rule === 'string') out.worst_rule = x.worst_rule
  if (typeof x.worst_level === 'string') out.worst_level = x.worst_level
  if (typeof x.trading_day === 'string') out.trading_day = x.trading_day
  if (isNum(x.initial_capital)) out.initial_capital = x.initial_capital
  if (isNum(x.margin)) out.margin = x.margin
  if (isNum(x.open_orders)) out.open_orders = x.open_orders
  if (typeof x.trading_mode === 'string') out.trading_mode = x.trading_mode as AccountSummary['trading_mode']
  if (x.missing === true) out.missing = true
  copyLiveness(x, out)
  return out
}

/** Owner liveness of an account item (`stale`, `age_ms`) and whether it has a snapshot (`missing`). */
function copyLiveness(x: unknown, out: AccountSummary): void {
  if (!isObj(x)) return
  if (typeof x.stale === 'boolean') out.stale = x.stale
  if (isNum(x.age_ms) || x.age_ms === null) out.age_ms = x.age_ms as number | null
  if (x.missing === false) out.missing = false
}

const SUMMARY_NULLABLE = ['shard', 'equity', 'daily_pnl', 'daily_pnl_pct_init', 'risk_degree', 'quality'] as const

/** Merge a (possibly reduced) summary into the previous one: null fields keep the previous value. */
export function mergeAccountSummary(prev: AccountSummary | undefined, next: AccountSummary): AccountSummary {
  if (!prev) return next
  // fields absent from `next` keep their previous value
  const out: AccountSummary = { ...prev, ...next }
  const rec = out as unknown as Record<string, unknown>
  for (const k of SUMMARY_NULLABLE) {
    if (next[k] === null && prev[k] !== null) rec[k] = prev[k]
  }
  // a reduced item carries no rule information: keep the last known worst rule
  if (next.equity === null && next.worst_utilization === null) {
    out.worst_utilization = prev.worst_utilization
    out.worst_rule = prev.worst_rule
  }
  return out
}

export interface EffectiveState {
  state: AccountState
  halted_latched: boolean
  restrictions: Restriction[]
  state_version: number | null
  source: 'snapshot' | 'summary' | 'change'
}

/**
 * Cross-topic ordering rule (02 §5, rule 2): state, latch and restrictions come from
 * whichever of `risk.snapshot` / `risk.state` has the larger `state_version`.
 * The WS `accounts` summary carries no state_version, so a state change overrides it
 * only when the change is newer than the summary's timestamp.
 */
export function mergeAccountState(
  snapshot: AccountSnapshot | null | undefined,
  summary: AccountSummary | null | undefined,
  change: AccountStateChanged | null | undefined,
): EffectiveState | null {
  let base: EffectiveState | null = null
  let baseTs = -1
  if (snapshot) {
    base = {
      state: snapshot.state,
      halted_latched: Boolean(snapshot.halted_latched),
      restrictions: snapshot.restrictions ?? [],
      state_version: snapshot.state_version,
      source: 'snapshot',
    }
    baseTs = snapshot.as_of_ns
  }
  if (summary && (!base || (isNum(summary.as_of_ns) && summary.as_of_ns > baseTs))) {
    const sv = isNum(summary.state_version) ? summary.state_version : null
    if (!base || sv === null || base.state_version === null || sv >= base.state_version) {
      base = {
        state: summary.state,
        halted_latched: summary.halted_latched,
        restrictions: sv !== null && base && sv === base.state_version ? base.restrictions : (base?.restrictions ?? []),
        state_version: sv ?? base?.state_version ?? null,
        source: 'summary',
      }
      baseTs = summary.as_of_ns ?? baseTs
    }
  }
  if (change) {
    const newer =
      !base ||
      (base.state_version !== null ? change.state_version > base.state_version : change.ts_ns > baseTs)
    if (newer) {
      base = {
        state: change.to_state,
        halted_latched: change.halted_latched,
        restrictions: change.restrictions ?? [],
        state_version: change.state_version,
        source: 'change',
      }
    }
  }
  return base
}

export function normalizeEquityPoint(x: unknown): EquityPoint | null {
  if (!isObj(x) || !isNum(x.ts_ns) || !isNum(x.equity)) return null
  const p: EquityPoint = {
    ts_ns: x.ts_ns,
    equity: x.equity,
    daily_pnl_pct_init: num(x.daily_pnl_pct_init, 0) as number,
    intraday_dd: num(x.intraday_dd, 0) as number,
  }
  if (isAccountState(x.state)) p.state = x.state
  if (isNum(x.nav)) p.nav = x.nav
  if (isNum(x.risk_degree)) p.risk_degree = x.risk_degree
  return p
}

/** Liquidation plan carried by a LIQUIDATION_PLAN risk event (`detail.plan` or the detail itself). */
export function extractPlan(ev: RiskEvent): LiquidationPlan | null {
  const d = ev.detail
  if (!isObj(d)) return null
  const cand = isObj(d.plan) ? d.plan : d
  if (typeof cand.plan_id === 'string' && Array.isArray(cand.steps)) {
    return {
      plan_id: cand.plan_id,
      account_id: str(cand.account_id) ?? ev.account_id ?? '',
      trading_day: str(cand.trading_day) ?? ev.trading_day,
      trigger_alert_key: str(cand.trigger_alert_key) ?? ev.alert_key ?? '',
      status: (str(cand.status) ?? 'PENDING_CONFIRM') as LiquidationPlan['status'],
      steps: cand.steps as LiquidationPlan['steps'],
      created_ns: num(cand.created_ns, ev.ts_ns) as number,
      confirmed_by: str(cand.confirmed_by),
      confirmed_ns: num(cand.confirmed_ns),
    }
  }
  return null
}

export function isLiquidationPlan(x: unknown): x is LiquidationPlan {
  return isObj(x) && typeof x.plan_id === 'string' && Array.isArray(x.steps) && typeof x.status === 'string'
}

// ---------------------------------------------------------------- market

/** Quote view from a WS `quotes` item or a full Tick. */
export function toQuote(x: unknown): QuoteView | null {
  if (!isObj(x) || typeof x.instrument_id !== 'string' || !isNum(x.last)) return null
  if (isNum(x.exch_ts_ns) && isNum(x.recv_ns)) {
    const t = x as unknown as Tick
    const flags = t.flags ?? 0
    const lock = flags & TICK_FLAG_LIMIT_UP_LOCKED ? 'UP' : flags & TICK_FLAG_LIMIT_DOWN_LOCKED ? 'DOWN' : 'NONE'
    return {
      instrument_id: t.instrument_id,
      exchange: t.exchange,
      last: t.last,
      bid1: t.bid1 ?? null,
      ask1: t.ask1 ?? null,
      volume: t.volume,
      change_pct: t.pre_settle ? (t.last - t.pre_settle) / t.pre_settle : null,
      source: t.source,
      lag_ms: (t.recv_ns - t.exch_ts_ns) / 1e6,
      limit_lock: lock,
      upper_limit: t.upper_limit,
      lower_limit: t.lower_limit,
      pre_settle: t.pre_settle,
    }
  }
  const q: QuoteView = {
    instrument_id: x.instrument_id,
    last: x.last,
    bid1: num(x.bid1),
    ask1: num(x.ask1),
    volume: num(x.volume, 0) as number,
    change_pct: num(x.change_pct),
    source: (str(x.source) as QuoteView['source']) ?? null,
    lag_ms: num(x.lag_ms),
  }
  if (typeof x.quality === 'string') q.quality = quality(x.quality)
  if (x.limit_lock === 'UP' || x.limit_lock === 'DOWN' || x.limit_lock === 'NONE') q.limit_lock = x.limit_lock
  if (typeof x.exchange === 'string') q.exchange = x.exchange
  if (isNum(x.upper_limit)) q.upper_limit = x.upper_limit
  if (isNum(x.lower_limit)) q.lower_limit = x.lower_limit
  if (isNum(x.pre_settle)) q.pre_settle = x.pre_settle
  return q
}

// ---------------------------------------------------------------- system

function numRecord(v: unknown): Record<string, number> {
  const out: Record<string, number> = {}
  if (isObj(v)) for (const [k, x] of Object.entries(v)) if (isNum(x)) out[k] = x
  return out
}

function latencyRecord(v: unknown): Record<string, LatencySummary> {
  const out: Record<string, LatencySummary> = {}
  if (isObj(v)) {
    for (const [k, x] of Object.entries(v)) {
      if (isObj(x) && isNum(x.p50) && isNum(x.p99)) out[k] = { p50: x.p50, p99: x.p99, max: num(x.max, x.p99) as number, n: num(x.n, 0) as number }
    }
  }
  return out
}

const PROCESS_STATES = ['STARTING', 'RUNNING', 'EXITED', 'RESTARTING', 'STOPPING', 'STOPPED', 'FAILED']
const PROCESS_ONLY_STATES = ['EXITED', 'RESTARTING', 'STOPPED', 'FAILED']
const SERVICE_STATES = ['STARTING', 'RUNNING', 'DEGRADED', 'STOPPING']
const LIVENESS = ['UP', 'DOWN', 'UNKNOWN']

export function isClockState(x: unknown): x is ClockState {
  return isObj(x) && typeof x.mode === 'string' && isNum(x.offset_ns) && isNum(x.version) && isNum(x.market_now_ns)
}

/** The market-clock item of the WS `system` topic (`kind: "clock"`, instance `sys.clock`). */
export function isClockItem(x: unknown): x is ClockState {
  return isObj(x) && x.kind === 'clock' && isClockState(x)
}

/**
 * Service health from a `GET /system/services` item (`source: 'rest'`) or a WS `system`
 * item (`source: 'ws'`); merges into `prev` when given.
 *
 * - REST: `process_state` (= `state`) is the supervisor's ProcessState when the supervisor
 *   (or a ServiceStateChanged) knows the process; `heartbeat_state` / nested `heartbeat`
 *   are the service's own view. Instances never seen carry only `{instance, service,
 *   restarts, liveness}`.
 * - WS: `state` is the heartbeat ServiceState, or the ProcessState once the supervisor
 *   reports the process EXITED / RESTARTING / STOPPED / FAILED; the heartbeat fields are flat.
 *
 * Both carry the gateway's `liveness` (UP / DOWN / UNKNOWN from heartbeat age).
 */
export interface AlertKeyParts {
  trading_day: string
  rule_id: string
  level: string
  scope_key: string
  episode: number | null
}

/** An alert key `{trading_day}|{rule_id}|{level}|{scope_key}|{episode}` (01 §8.14), or null. */
export function parseAlertKey(key: string | null | undefined): AlertKeyParts | null {
  if (!key) return null
  const p = key.split('|')
  if (p.length < 4) return null
  const ep = p.length > 4 ? Number(p[4]) : NaN
  return { trading_day: p[0] ?? '', rule_id: p[1] ?? '', level: p[2] ?? '', scope_key: p[3] ?? '', episode: Number.isFinite(ep) ? ep : null }
}

const GONE_PROCESS_STATES = ['EXITED', 'RESTARTING', 'STOPPED', 'FAILED']

/**
 * A "gone" process state (EXITED / RESTARTING / STOPPED / FAILED) that is older than the
 * instance's last heartbeat while the gateway sees it UP: the process runs again (e.g.
 * restarted while the supervisor was down). `since_ns` = since when the process state holds,
 * `heartbeat_ns` = last heartbeat received by the gateway (both from GET /system/services).
 */
export function processStateStale(s: Pick<ServiceHealth, 'process_state' | 'liveness' | 'since_ns' | 'heartbeat_ns'>): boolean {
  if (!s.process_state || !GONE_PROCESS_STATES.includes(s.process_state)) return false
  return s.liveness === 'UP' && s.heartbeat_ns !== null && s.since_ns !== null && s.heartbeat_ns > s.since_ns
}

export function toServiceHealth(x: unknown, prev?: ServiceHealth | null, source: 'rest' | 'ws' = 'rest'): ServiceHealth | null {
  if (!isObj(x) || typeof x.instance !== 'string') return null
  const hb: Obj = isObj(x.heartbeat) ? x.heartbeat : source === 'ws' ? x : {}
  const base: ServiceHealth = prev
    ? { ...prev }
    : {
        instance: x.instance,
        service: str(x.service) ?? x.instance.replace(/-\d+$/, ''),
        process_state: null,
        heartbeat_state: null,
        liveness: null,
        pid: null,
        restarts: 0,
        started_ns: null,
        last_heartbeat_ns: null,
        last_exit_code: null,
        loop_lag_ms: null,
        queues: {},
        rates: {},
        latency: {},
        config_version: null,
        config_hash: null,
        clock_version: null,
        trading_mode: null,
        deps: {},
        heartbeat: null,
        since_ns: null,
        heartbeat_ns: null,
        last_error: null,
        updated_ms: Date.now(),
      }
  if (typeof x.service === 'string') base.service = x.service
  const st = str(x.state)
  if (source === 'rest') {
    // a full listing: every field is authoritative, absent means unknown
    const ps = str(x.process_state) ?? st
    base.process_state = ps && PROCESS_STATES.includes(ps) ? (ps as ProcessState) : null
    const hbs = str(x.heartbeat_state) ?? str(hb.state)
    base.heartbeat_state = hbs && SERVICE_STATES.includes(hbs) ? (hbs as ServiceState) : null
    base.last_exit_code = num(x.last_exit_code)
    base.since_ns = num(x.since_ns)
    base.heartbeat_ns = num(x.heartbeat_ns)
    base.last_error = str(x.last_error)
    if (!isNum(x.pid) && !isNum(hb.pid)) base.pid = null
  } else if (st) {
    if (PROCESS_ONLY_STATES.includes(st)) {
      base.process_state = st as ProcessState
      base.heartbeat_state = null
      base.pid = null
    } else if (SERVICE_STATES.includes(st)) {
      base.heartbeat_state = st as ServiceState
      // a fresh heartbeat implies a live process
      if (base.process_state === null || PROCESS_ONLY_STATES.includes(base.process_state)) {
        base.process_state = st === 'STOPPING' ? 'STOPPING' : 'RUNNING'
      }
    }
  }
  const lv = str(x.liveness)
  if (lv && LIVENESS.includes(lv)) base.liveness = lv as Liveness
  if (isNum(x.pid) || isNum(hb.pid)) base.pid = (num(x.pid) ?? num(hb.pid)) as number
  if (isNum(x.restarts)) base.restarts = x.restarts
  if (isNum(x.started_ns) || isNum(hb.started_ns)) base.started_ns = (num(x.started_ns) ?? num(hb.started_ns)) as number
  if (isNum(x.last_heartbeat_ns)) base.last_heartbeat_ns = x.last_heartbeat_ns
  if (x.last_exit_code !== undefined) base.last_exit_code = num(x.last_exit_code)
  if (isNum(hb.loop_lag_ms)) base.loop_lag_ms = hb.loop_lag_ms
  const queues = hb.queues ?? x.queues
  if (isObj(queues)) base.queues = numRecord(queues)
  const rates = hb.rates ?? x.rates
  if (isObj(rates)) base.rates = numRecord(rates)
  const lat = hb.latency_us ?? x.latency ?? x.latency_us
  if (isObj(lat)) base.latency = latencyRecord(lat)
  if (isNum(hb.config_version)) base.config_version = hb.config_version
  if (typeof hb.config_hash === 'string') base.config_hash = hb.config_hash
  if (isNum(hb.clock_version)) base.clock_version = hb.clock_version
  if (typeof hb.trading_mode === 'string') base.trading_mode = hb.trading_mode as ServiceHealth['trading_mode']
  if (isObj(hb.deps)) base.deps = Object.fromEntries(Object.entries(hb.deps).map(([k, v]) => [k, String(v)]))
  if (isObj(x.heartbeat)) base.heartbeat = x.heartbeat as unknown as Heartbeat
  else if (source === 'rest') base.heartbeat = null
  base.updated_ms = Date.now()
  return base
}

export function isSystemTopicItem(x: unknown): x is SystemTopicItem {
  return isObj(x) && typeof x.instance === 'string' && typeof x.state === 'string' && x.from_state === undefined
}

// ---------------------------------------------------------------- compare / history

/** Validate a server `compare` payload; null means "use the client-side builder". */
export function normalizeCompare(x: unknown): CompareView | null {
  if (!isObj(x)) return null
  if (!Array.isArray(x.matrix) || !Array.isArray(x.accounts) || !Array.isArray(x.rules)) return null
  const exposure = isObj(x.exposure) ? x.exposure : null
  if (!exposure || !Array.isArray(exposure.products) || !Array.isArray(exposure.cells)) return null
  const view: CompareView = {
    trading_day: str(x.trading_day) ?? '',
    as_of_ns: num(x.as_of_ns, 0) as number,
    accounts: x.accounts as string[],
    rules: x.rules as string[],
    matrix: x.matrix as CompareView['matrix'],
    ranking: Array.isArray(x.ranking) ? (x.ranking as CompareView['ranking']) : [],
    exposure: {
      products: exposure.products as string[],
      accounts: Array.isArray(exposure.accounts) ? (exposure.accounts as string[]) : (x.accounts as string[]),
      cells: exposure.cells as CompareView['exposure']['cells'],
      firm_net: isObj(exposure.firm_net) ? numRecord(exposure.firm_net) : {},
    },
  }
  if (Array.isArray(x.accounts_stale)) view.accounts_stale = x.accounts_stale.filter((a): a is string => typeof a === 'string')
  if (isObj(x.liveness)) view.liveness = x.liveness as CompareView['liveness']
  return view
}

export interface StatementView {
  text: string | null
  result: SettlementResult | null
}

/** `GET /settlement/{td}/statements/{account}` data: `{format: "text", text}` or `{format: "settlement", settlement}`. */
export function normalizeStatement(data: unknown): StatementView {
  if (!isObj(data)) return { text: null, result: null }
  const settlement = isObj(data.settlement) && Array.isArray(data.settlement.accounts) ? (data.settlement as unknown as SettlementResult) : null
  return { text: data.format === 'text' && typeof data.text === 'string' ? data.text : null, result: settlement }
}

/** An EOD WARN event of the `eod` topic (`detail.warning` set): it repeats the run's phase and is no phase change. */
export function eodWarningOf(ev: EodPhaseEvent): EodWarning | null {
  const d = ev.detail
  if (!isObj(d) || typeof d.warning !== 'string') return null
  const context = Object.fromEntries(Object.entries(d).filter(([k]) => k !== 'warning' && k !== 'kind' && k !== 'warn_id'))
  return {
    kind: str(d.kind) ?? 'EOD_WARNING',
    message: d.warning,
    trading_day: ev.trading_day,
    version: ev.version,
    phase: ev.phase,
    warn_id: str(d.warn_id) ?? undefined,
    ts_ns: ev.ts_ns,
    context,
  }
}

/** `detail.warnings` of an EodPhaseEvent (settle.status: `{kind, message}`; read model: full records; PRICES_FIXED: PRE_SETTLE_MISMATCH items). */
export function eodWarningsIn(ev: EodPhaseEvent): EodWarning[] {
  const d = ev.detail
  if (!isObj(d) || !Array.isArray(d.warnings)) return []
  return d.warnings
    .filter(isObj)
    .map((w) => ({ ...(w as Record<string, unknown>), kind: str(w.kind) ?? 'EOD_WARNING', message: str(w.message) ?? str(w.kind) ?? '', trading_day: str(w.trading_day) ?? ev.trading_day, version: num(w.version) ?? ev.version } as EodWarning))
}

/** `GET /risk/compare/history` body `{days, missing, errors}` (a bare list of days is also accepted). */
export function normalizeHistory(x: unknown): CompareHistory | null {
  const list = isObj(x) && Array.isArray(x.days) ? (x.days as unknown[]) : asItems(x)
  const days: CompareHistoryDay[] = []
  for (const d of list) {
    if (!isObj(d) || typeof d.trading_day !== 'string' || !Array.isArray(d.accounts)) continue
    const day: CompareHistoryDay = { trading_day: d.trading_day, accounts: d.accounts as CompareHistoryDay['accounts'] }
    if (isNum(d.version)) day.version = d.version
    const br = d.breakers ?? d.breaker_counts
    if (isObj(br)) day.breakers = numRecord(br)
    days.push(day)
  }
  if (!days.length && !Array.isArray(list)) return null
  const out: CompareHistory = { days }
  if (isObj(x) && Array.isArray(x.missing)) out.missing = x.missing.filter((d): d is string => typeof d === 'string')
  if (isObj(x) && isObj(x.errors)) out.errors = stringRecord(x.errors)
  return out
}

export function stringRecord(v: unknown): Record<string, string> {
  const out: Record<string, string> = {}
  if (isObj(v)) for (const [k, x] of Object.entries(v)) out[k] = typeof x === 'string' ? x : JSON.stringify(x)
  return out
}

/** A `{source: code}` map of a partial answer as one line: "risk-engine-1=UNAVAILABLE，…". */
export function errorsText(errors: Record<string, string> | null | undefined): string {
  if (!errors) return ''
  return Object.entries(errors)
    .map(([k, v]) => `${k}=${v}`)
    .join('，')
}

/** Epoch ns from a WS frame `ts` of unknown unit. */
export function frameTsNs(ts: unknown): number | null {
  return isNum(ts) ? toNs(ts) : null
}
