/**
 * In-browser simulation of the futrisk backend (mock mode).
 *
 * It reproduces, at demo scale, what the Python services publish: a correlated price
 * engine for the 11 demo instruments with a scripted black-sector sell-off, per-account
 * portfolios valued like risk-engine (MTM vs pre-settle, SHFE big-side margin), the
 * alert state machine of 01 §8.14 (confirm, latch, hysteresis, rearm, episodes), bots
 * whose orders go through a simplified pre-trade pipeline (02 §11.2), Feed A / Feed B
 * arbitration with fault injection, supervisor-style service lifecycles, the demo
 * market clock, the EOD saga and backtest jobs.
 *
 * Scenario: ACC3 (long rb / hc / i, ~1.1x equity after its morning dip-buys) drifts to
 * −2% (warn), −2.5% (restrict), −3% (HALTED pending, then latched after 2 LIVE states /
 * 1s); open orders are then rejected with REJECT_ACCOUNT_HALTED while closes pass. A
 * second leg deepens the drawdown; the "breaker" demo scenario on top takes ACC3 past
 * −5% (derisk -> liquidation plan).
 *
 * Every answer is shaped like the api-gateway's (types/api.ts), and payloads go through
 * `wire()` so that fields at their msgspec default are left out exactly as the gateway
 * does. Killing a risk shard makes its accounts stale / its queries UNAVAILABLE; killing
 * md-gateway or clearing-worker makes their endpoints UNAVAILABLE, like the real system.
 * The gateway's own state is mirrored too: the kill-switch manager (one desired state,
 * per-target PENDING / CONFIRMED / FAILED, retries every 2 s), the firm control layers
 * (MANUAL / RULE), EOD warnings, the sim's fault list / history and scenario runs.
 */
import type {
  AccountFunds,
  AccountListItem,
  AccountMetricsView,
  AccountMissingItem,
  AccountSnapshot,
  AccountSnapshotView,
  AccountState,
  AccountStateChanged,
  AccountSummary,
  AuthMe,
  BacktestStatus,
  ClockJumpResult,
  ClockJumpStep,
  ClockState,
  ClockTopicItem,
  CompareHistory,
  CompareHistoryDay,
  CompareView,
  DemoFaultResult,
  DemoFaultsResponse,
  DemoScenarioInfo,
  DemoScenarioResult,
  DemoShockResult,
  DemoState,
  DayRoll,
  Direction,
  EodPhase,
  EodPhaseEvent,
  ExposureView,
  FirmControl,
  FirmControlState,
  FirmSnapshot,
  FirmTopicSnapshot,
  GapInfo,
  HealthInfo,
  Heartbeat,
  KillSwitchControl,
  KillSwitchResult,
  KillSwitchRetry,
  KillSwitchRow,
  LimitLock,
  LiquidationPlan,
  LiquidationPlansResponse,
  LiquidationPlanView,
  Liveness,
  MdEvent,
  MdEventKind,
  MdFeedsView,
  MdQuality,
  MdStatus,
  MetricPoint,
  MetricSeries,
  Offset,
  OrderUpdate,
  PositionGroup,
  PositionGroupBy,
  PositionRow,
  PositionsResponse,
  PositionView,
  ProcessState,
  ReportReady,
  ResetAck,
  Restriction,
  RiskEvent,
  RiskEventsResponse,
  RiskStateRow,
  RuleLevelDef,
  RuleSet,
  RuleSmState,
  RuleStatus,
  ScenarioRunState,
  ServiceChangeReason,
  ServiceInfo,
  ServiceItem,
  ServiceState,
  ServiceStateChanged,
  SettlementCompleted,
  SettlementDayItem,
  SettlementDaysResponse,
  SettlementResult,
  Severity,
  SimFault,
  SourceId,
  SourceState,
  StatementResponse,
  SystemServicesResponse,
  SystemStatus,
  SystemTopicItem,
  TargetResult,
  Tick,
  Trade,
  TradingMode,
} from '@/types'
import { buildCompareView } from '@/utils/compare'
import { shanghaiParts, shanghaiToNs } from '@/utils/format'
import { levelLabel, ruleLabel } from '@/utils/labels'
import { effectiveLevels, fmtMetric, hardLimitLevel, levelMarks, utilization } from '@/utils/risk'
import {
  ACCOUNTS,
  DEMO_START,
  DEMO_TRADING_DAY,
  EOD_START_MIN,
  INSTRUMENTS,
  INSTRUMENT_MAP,
  NEXT_TRADING_DAYS,
  PRE_CLOSE_MIN,
  RULES,
  SERVICES,
  SESSION_WINDOWS,
  type MockAccountSpec,
  type MockInstrument,
  type MockServiceSpec,
} from './config'
import { generateHistory, renderBacktestHtml, renderRiskDailyHtml, renderStatementCsv, renderStatementTxt, reportPath, reportsForDay } from './history'
import { Rng } from './rng'
import { wire, wireList } from './wire'

export type FrameType = 'snapshot' | 'upsert' | 'event' | 'delete'
export type PublishFn = (topic: string, type: FrameType, data: unknown) => void

export class MockError extends Error {
  readonly code: string
  readonly status: number
  readonly data: unknown
  constructor(code: string, message: string, status = 200, data: unknown = undefined) {
    super(message)
    this.code = code
    this.status = status
    this.data = data
  }
}

/**
 * A handler answer with envelope metadata: a message on an OK answer ("partial: …",
 * "0 latched level(s) reset"), or an ok=false answer that still carries data (kill-switch
 * partial failure, CONFLICT with the current plan). `status` overrides the HTTP status of
 * the code (the kill switch answers 200 on a partial failure).
 */
export class MockResult<T = unknown> {
  constructor(
    readonly data: T,
    readonly message = '',
    readonly ok = true,
    readonly code = 'OK',
    readonly status: number | null = null,
  ) {}
}

/** `readmodel`-style "peer is down" failure of a bus request to `instance`. */
export function peerDown(instance: string): MockError {
  return new MockError('UNAVAILABLE', `${instance}: peer is down (no heartbeat)`, 503)
}

/** The gateway's peer timeout: 3 missed heartbeats of 1 s (services.yaml bus). */
const PEER_TIMEOUT_MS = 3000

const STEP_MS = 500
const NOISE_AMP = 3
/** mean-reversion speed of the price noise (1/s) */
const OU_THETA = 1 / 200
const CONFIG_HASH = '9f2c41d7a0b6e3f58c1d2e4b7a6c9d0e1f2a3b4c5d6e7f8091a2b3c4d5e6f7a8'
const CONFIG_VERSION = 3
const APP_VERSION = '0.1.0'
const GATEWAY_EPOCH = '01a0d6fb-8d75-72c3-bab5-1f952052f4dd'
const RISK_INSTANCES = ['risk-engine-0', 'risk-engine-1']
const HOME_ID = '76380698c9914be2a685abad12d3dfc9'
/** KillSwitchManager: retry interval / attempts per target before FAILED */
const KILL_RETRY_MS = 2000
const KILL_MAX_ATTEMPTS = 30
const SIM_DOWN_MESSAGE = 'sim-exchange control unreachable: All connection attempts failed'

interface MockScenarioStep {
  at_s: number
  action: 'shock' | 'fault' | 'log'
  note?: string
  params?: Record<string, unknown>
}

/** config/scenarios/*.yaml of the demo (GET /demo/scenarios lists name + description). */
const SCENARIOS: { name: string; description: string; steps: MockScenarioStep[] }[] = [
  {
    name: 'all_feeds_outage',
    description: 'Feed A 与 Feed B 同时中断 40 秒：行情质量 STALE（5s）→ UNKNOWN（30s），相关合约禁开，恢复后补数',
    steps: [
      { at_s: 0, action: 'fault', params: { type: 'FEED_DISCONNECT', target: 'feed_a', params: { refuse_reconnect_s: 40 }, duration_s: 40 } },
      { at_s: 0, action: 'fault', params: { type: 'FEED_DISCONNECT', target: 'feed_b', params: { refuse_reconnect_s: 40 }, duration_s: 40 } },
      { at_s: 45, action: 'log', note: '两路行情恢复，质量回到 OK' },
    ],
  },
  {
    name: 'breaker',
    description: '黑色系两段下跌：ACC3 当日亏损越过 −3% 熔断线（开仓被拒、平仓照常），第二段可触发减仓计划',
    steps: [
      { at_s: 0, action: 'shock', params: { sector: 'black', pct: -0.025, duration_s: 60, shape: 'LINEAR' } },
      { at_s: 60, action: 'log', note: '第一段下跌结束' },
      { at_s: 75, action: 'shock', params: { sector: 'black', pct: -0.015, duration_s: 30, shape: 'LINEAR' } },
    ],
  },
  {
    name: 'feed_outage',
    description: 'Feed A 断线 20 秒后恢复并按 seq 补数（缺口回补），期间由 Feed B 提供行情',
    steps: [
      { at_s: 0, action: 'fault', params: { type: 'FEED_DISCONNECT', target: 'feed_a', params: { refuse_reconnect_s: 20 }, duration_s: 20 } },
      { at_s: 25, action: 'log', note: 'Feed A 已恢复' },
    ],
  },
]

interface EodWarningRecord {
  trading_day: string
  version: number
  phase: EodPhase
  kind: string
  message: string
  warn_id: string
  ts_ns: number
  context: Record<string, unknown>
}
/** `Heartbeat.versions`: payload type -> [min, max] protocol version (a few of the real list) */
const PAYLOAD_VERSIONS: Record<string, number[]> = Object.fromEntries(
  ['AccountSnapshot', 'AccountStateChanged', 'ClockState', 'EodPhaseEvent', 'FirmSnapshot', 'Heartbeat', 'MdEvent', 'MdStatus', 'OrderUpdate', 'RiskEvent', 'ServiceStateChanged', 'Tick'].map((n) => [n, [1, 1]]),
)

// ---------------------------------------------------------------- helpers

function roundTick(px: number, tick: number): number {
  const n = Math.round(px / tick)
  return Number((n * tick).toFixed(6))
}

function floorTick(px: number, tick: number): number {
  return Number((Math.floor(px / tick + 1e-9) * tick).toFixed(6))
}

function minuteOfDay(ms: number): number {
  const p = shanghaiParts(ms)
  return p.h * 60 + p.mi + p.s / 60
}

function inSession(spec: MockInstrument, ms: number): boolean {
  const m = minuteOfDay(ms)
  const w = SESSION_WINDOWS[spec.sessions]
  for (const [a, b] of w.day) if (m >= a && m < b) return true
  if (w.night) {
    const [a, b] = w.night
    if (m >= a && m < Math.min(b, 1440)) return true
    if (b > 1440 && m < b - 1440) return true
  }
  return false
}

function smoothstep(x: number): number {
  const t = Math.max(0, Math.min(1, x))
  return t * t * (3 - 2 * t)
}

function crossed(value: number, thr: number, op: string): boolean {
  switch (op) {
    case '<=':
      return value <= thr
    case '<':
      return value < thr
    case '>':
      return value > thr
    default:
      return value >= thr
  }
}

/** Back inside the clear line: threshold × (1 ∓ hysteresis) on the safe side (01 §8.14). */
function cleared(value: number, thr: number, op: string, h: number): boolean {
  if (op === '>=' || op === '>') return value < thr * (1 - h)
  if (thr < 0) return value > thr * (1 - h)
  return value > thr * (1 + h)
}

function sevOf(action: string): Severity {
  switch (action) {
    case 'CIRCUIT_BREAK':
      return 'CRITICAL'
    case 'RESTRICT':
    case 'LIQUIDATE':
      return 'HIGH'
    default:
      return 'WARNING'
  }
}

function stateOfAction(action: string): AccountState {
  switch (action) {
    case 'CIRCUIT_BREAK':
      return 'HALTED'
    case 'RESTRICT':
      return 'RESTRICTED'
    case 'ALERT':
      return 'WARNING'
    default:
      return 'NORMAL'
  }
}

const STATE_RANK: Record<AccountState, number> = { NORMAL: 0, WARNING: 1, RESTRICTED: 2, HALTED: 3 }

// ---------------------------------------------------------------- state types

interface InstState {
  spec: MockInstrument
  exchPx: number
  last: number
  bid: number
  ask: number
  open: number | null
  high: number | null
  low: number | null
  volume: number
  turnover: number
  oi: number
  preSettle: number
  upper: number
  lower: number
  idio: number
  exchNs: number
  recvNs: number
  source: SourceId | null
  lastLiveMs: number
  wasInSession: boolean
  quality: MdQuality
  limitLock: LimitLock
}

interface PosState {
  instrument: string
  direction: 'LONG' | 'SHORT'
  td: number
  yd: number
  tdCost: number
  ydOpenCost: number
  pendingClose: number
}

interface LevelSm {
  state: RuleSmState
  since: number
  confirm: number
  episode: number
  latched: boolean
  rearm: number | null
  cooldownUntil: number
  alertKey: string | null
}

interface Cause {
  cause: string
  alertKey?: string | null
  actor?: string | null
  reason?: string | null
}

interface AcctState {
  spec: MockAccountSpec
  preBalance: number
  netDeposit: number
  cumNetDeposit: number
  closePnl: number
  fee: number
  hwm: number
  peak: number
  positions: Map<string, PosState>
  orders: Map<string, OrderUpdate>
  trades: Trade[]
  ordersToday: number
  cancelsToday: number
  openVol: Record<string, number>
  manualHalt: boolean
  state: AccountState
  latched: boolean
  restrictions: Restriction[]
  restrictKey: string
  stateVersion: number
  lastChange: AccountStateChanged | null
  pendingCause: Cause | null
  sm: Map<string, LevelSm>
  snapshot: AccountSnapshot | null
  version: number
  points: MetricPoint[]
  nextBotMs: number
  botStep: number
  tradingMode: TradingMode
  morningDone: number
}

interface SrcState {
  id: SourceId
  state: SourceState
  sinceNs: number
  connected: boolean
  epoch: string
  exchSeq: number
  lastSeq: number
  lastMsgNs: number | null
  baseLag: number
  extraLag: number
  rejectRate: number
  msgsPerS: number
  votes: number
  probationUntil: number
  dupRatio: number
}

interface Fault {
  fault_id: string
  type: string
  target: string | null
  params: Record<string, unknown>
  untilMs: number | null
  createdMs: number
  durationS: number | null
}

interface Shock {
  id: string
  targets: Set<string>
  pct: number
  startMs: number
  durationS: number
  shape: 'STEP' | 'LINEAR'
}

interface SvcState {
  spec: MockServiceSpec
  processState: ProcessState
  /** browser ms since the process is not RUNNING (liveness DOWN after PEER_TIMEOUT_MS) */
  downSinceMs: number | null
  hbState: ServiceState | null
  pid: number
  startedNs: number
  /** since when `processState` holds (last sys.service transition) */
  sinceNs: number
  restarts: number
  lastHbNs: number | null
  lastExit: number | null
  clockVersion: number
  clockApplyAt: number
  loopLag: number
  queues: Record<string, number>
  rates: Record<string, number>
  latency: Record<string, { p50: number; p99: number; max: number; n: number }>
}

/** Wire rendering of a WS payload by topic family (what `to_builtins` gives the gateway). */
function wireTopic(topic: string, data: unknown): unknown {
  if (data === null || typeof data !== 'object') return data
  if (Array.isArray(data)) return data.map((x) => wireTopic(topic, x))
  const d = data as Record<string, unknown>
  const fam = topic.includes(':') ? topic.slice(0, topic.indexOf(':')) : topic
  switch (fam) {
    case 'events':
      return wire(typeof d.event_id === 'string' ? 'RiskEvent' : 'AccountStateChanged', d)
    case 'orders':
      return wire('OrderUpdate', d)
    case 'positions':
      return wire('PositionView', d)
    case 'firm':
      return wire(typeof d.equity === 'number' ? 'FirmSnapshot' : 'FirmControl', d)
    case 'feeds':
      return wire(Array.isArray(d.sources) ? 'MdStatus' : 'MdEvent', d)
    case 'system':
      return typeof d.from_state === 'string' ? wire('ServiceStateChanged', d) : d
    case 'eod':
      if (typeof d.phase === 'string') return wire('EodPhaseEvent', d)
      if (typeof d.report_id === 'string') return wire('ReportReady', d)
      if (typeof d.from_trading_day === 'string') return wire('DayRoll', d)
      return wire('SettlementCompleted', d)
    default:
      return d
  }
}

// ---------------------------------------------------------------- engine

export class MockEngine {
  private readonly rng = new Rng(20260928)
  private readonly publishFn: PublishFn
  private publishing = false
  private virtualNow: number | null = null
  private offsetMs: number
  private readonly scenarioStartMs: number
  private clockVersion = 3
  tradingDay = DEMO_TRADING_DAY
  private readonly inst = new Map<string, InstState>()
  private readonly accts = new Map<string, AcctState>()
  private readonly sources: SrcState[]
  private readonly faults = new Map<string, Fault>()
  private readonly shocks: Shock[] = []
  private readonly svcs = new Map<string, SvcState>()
  private readonly riskEvents: RiskEvent[] = []
  private readonly pastEvents: Record<string, RiskEvent[]>
  private readonly mdEvents: MdEvent[] = []
  private readonly serviceEvents: ServiceStateChanged[] = []
  private readonly plans = new Map<string, LiquidationPlan>()
  private readonly history: CompareHistoryDay[]
  private readonly eodPhases = new Map<string, EodPhaseEvent>()
  private readonly eodLog: EodPhaseEvent[] = []
  private readonly settledEvents = new Map<string, SettlementCompleted>()
  private readonly reports = new Map<string, ReportReady>()
  private readonly backtests = new Map<string, BacktestStatus>()
  private readonly backtestSummaries = new Map<string, Record<string, unknown>>()
  /** manual kill-switch layer as the shards applied it */
  private killManual = false
  /** the read model's manual layer (last MANUAL FirmControl) */
  private killManualInfo: { actor: string | null; reason: string; ts_ns: number } | null = null
  /** gateway KillSwitchManager: the one persisted desired state and its per-target confirmation */
  private killRow: KillSwitchRow | null = null
  /** the retry task (superseded by a new command) */
  private killRetryTimer: ReturnType<typeof setTimeout> | null = null
  /** the coordinator's rule layer (RULE_STATE); the demo firm rules never trigger */
  private readonly ruleLayerNs = shanghaiToNs(2026, 9, 28, 8, 50, 20)
  /** last FirmSnapshot / md.status received (the read model keeps them while their producer is down) */
  private lastFirm: FirmSnapshot | null = null
  private lastMd: MdStatus | null = null
  private readonly eodWarnings = new Map<string, EodWarningRecord[]>()
  private readonly faultHistory: SimFault[] = []
  private scenarioRun: ScenarioRunState | null = null
  private scenarioTimers: ReturnType<typeof setTimeout>[] = []
  /** app.mode: outside 'demo' the /demo/* endpoints answer 403 */
  appMode: 'demo' | 'live' = 'demo'
  private readonly acked = new Set<string>()
  private readonly manualEpisode = new Map<string, number>()
  private eodRunning = false
  private mdDown = false
  private stepCount = 0
  private lastPointMs = 0
  apiDownUntil = 0
  onApiRestart: (() => void) | null = null
  private timer: ReturnType<typeof setInterval> | null = null

  constructor(publish: PublishFn) {
    this.publishFn = publish
    const startMs = shanghaiToNs(DEMO_START.y, DEMO_START.mo, DEMO_START.d, DEMO_START.h, DEMO_START.mi, DEMO_START.s) / 1e6
    this.scenarioStartMs = startMs
    this.offsetMs = startMs - Date.now()
    const hist = generateHistory()
    this.history = hist.days
    this.pastEvents = hist.events
    for (const spec of INSTRUMENTS) this.inst.set(spec.id, this.newInst(spec, spec.preSettle))
    const openNs = shanghaiToNs(2026, 9, 28, 8, 50, 0)
    this.sources = [
      { id: 'FEED_A', state: 'TRUSTED', sinceNs: openNs, connected: true, epoch: 'e20260928a', exchSeq: 0, lastSeq: 0, lastMsgNs: null, baseLag: 35, extraLag: 0, rejectRate: 0, msgsPerS: 22, votes: 0, probationUntil: 0, dupRatio: 0 },
      { id: 'FEED_B', state: 'TRUSTED', sinceNs: openNs, connected: true, epoch: '', exchSeq: 0, lastSeq: 0, lastMsgNs: null, baseLag: 620, extraLag: 0, rejectRate: 0, msgsPerS: 11, votes: 0, probationUntil: 0, dupRatio: 0 },
    ]
    const lastHist = this.history[this.history.length - 1]
    for (const spec of ACCOUNTS) {
      const histRow = lastHist?.accounts.find((a) => a.account_id === spec.id)
      const hwm = histRow ? Number(histRow.hwm_equity) : spec.hwmEquity
      const a: AcctState = {
        spec,
        preBalance: spec.preBalance,
        netDeposit: 0,
        cumNetDeposit: spec.cumNetDeposit,
        closePnl: 0,
        fee: 0,
        hwm,
        peak: spec.preBalance - spec.cumNetDeposit,
        positions: new Map(),
        orders: new Map(),
        trades: [],
        ordersToday: 0,
        cancelsToday: 0,
        openVol: {},
        manualHalt: false,
        state: 'NORMAL',
        latched: false,
        restrictions: [],
        restrictKey: '[]',
        stateVersion: 40 + this.rng.int(0, 20),
        lastChange: null,
        pendingCause: null,
        sm: new Map(),
        snapshot: null,
        version: 0,
        points: [],
        nextBotMs: 0,
        botStep: 0,
        tradingMode: 'OPEN',
        morningDone: 0,
      }
      for (const l of spec.lots) {
        a.positions.set(`${l.instrument}|${l.direction}`, { instrument: l.instrument, direction: l.direction, td: 0, yd: l.yd, tdCost: 0, ydOpenCost: l.ydOpen * l.yd, pendingClose: 0 })
      }
      this.accts.set(spec.id, a)
    }
    const bootNs = shanghaiToNs(2026, 9, 28, 8, 50, 12)
    for (const spec of SERVICES) {
      const pid = 4000 + this.rng.int(100, 9000)
      const startedNs = bootNs + this.rng.int(0, 20) * 1e9
      this.svcs.set(spec.instance, {
        spec,
        processState: 'RUNNING',
        downSinceMs: null,
        hbState: 'RUNNING',
        pid,
        startedNs,
        sinceNs: startedNs + 2e9,
        restarts: 0,
        lastHbNs: startMs * 1e6,
        lastExit: null,
        clockVersion: this.clockVersion,
        clockApplyAt: 0,
        loopLag: 2,
        queues: { ...spec.queues },
        rates: { ...spec.rates },
        latency: Object.fromEntries(Object.entries(spec.latency).map(([k, [p50, p99]]) => [k, { p50, p99, max: p99 * 1.6, n: 1000 }])),
      })
    }
    this.seedHistory()
    this.simulateMorning(startMs)
    this.seedBacktests()
  }

  // ---------------------------------------------------------------- lifecycle

  start(): void {
    if (this.timer) return
    this.publishing = true
    const now = this.nowMs()
    for (const a of this.accts.values()) a.nextBotMs = now + (a.spec.id === 'ACC3' ? 2500 : this.rng.range(3000, a.spec.botEveryS * 1000))
    this.timer = setInterval(() => {
      try {
        this.step(STEP_MS / 1000, true)
      } catch (e) {
        console.error('[mock] step failed', e)
      }
    }, STEP_MS)
  }

  stop(): void {
    if (this.timer) clearInterval(this.timer)
    this.timer = null
    if (this.killRetryTimer) clearTimeout(this.killRetryTimer)
    this.killRetryTimer = null
    for (const t of this.scenarioTimers) clearTimeout(t)
    this.scenarioTimers = []
  }

  nowMs(): number {
    return this.virtualNow ?? Date.now() + this.offsetMs
  }

  nowNs(): number {
    return this.nowMs() * 1e6
  }

  private publish(topic: string, type: FrameType, data: unknown): void {
    if (this.publishing) this.publishFn(topic, type, wireTopic(topic, data))
  }

  // ---------------------------------------------------------------- history seed

  private seedHistory(): void {
    const t = (h: number, m: number, s = 0) => shanghaiToNs(2026, 9, 28, h, m, s)
    for (const s of this.svcs.values()) {
      this.serviceEvents.push({ instance: s.spec.instance, service: s.spec.service, pid: s.pid, from_state: 'STARTING', to_state: 'RUNNING', restarts: 0, reason: 'STARTUP', trading_day: DEMO_TRADING_DAY, ts_ns: s.startedNs + 2e9 })
    }
    const md = this.svcs.get('md-gateway')
    if (md) {
      md.restarts = 1
      this.serviceEvents.push(
        { instance: 'md-gateway', service: 'md-gateway', pid: md.pid - 7, from_state: 'RUNNING', to_state: 'EXITED', exit_code: 1, restarts: 0, reason: 'EXITED', trading_day: DEMO_TRADING_DAY, ts_ns: t(9, 31, 4) },
        { instance: 'md-gateway', service: 'md-gateway', pid: null, from_state: 'EXITED', to_state: 'RESTARTING', restarts: 0, reason: 'EXITED', trading_day: DEMO_TRADING_DAY, ts_ns: t(9, 31, 5) },
        { instance: 'md-gateway', service: 'md-gateway', pid: md.pid, from_state: 'RESTARTING', to_state: 'RUNNING', restarts: 1, reason: 'STARTUP', trading_day: DEMO_TRADING_DAY, ts_ns: t(9, 31, 8) },
      )
      md.startedNs = t(9, 31, 7)
      md.sinceNs = t(9, 31, 8)
    }
    this.serviceEvents.sort((a, b) => b.ts_ns - a.ts_ns)
    // morning Feed A outage with replay back-fill (01 §6.4)
    const epoch = 'e20260928a'
    const gapId = `feed_a:${epoch}:18842-18901`
    const ev = (kind: MdEventKind, ts: number, extra: Partial<MdEvent>) =>
      this.mdEvents.push({ event_id: this.rng.uuid(ts / 1e6), kind, ts_ns: ts, trading_day: DEMO_TRADING_DAY, detail: {}, ...extra })
    ev('SOURCE_STATE', t(9, 47, 10), { source: 'FEED_A', from_state: 'TRUSTED', to_state: 'EXCLUDED', detail: { reason: 'HEARTBEAT_MISSED', missed: 3 } })
    ev('SOURCE_STATE', t(9, 47, 25), { source: 'FEED_A', from_state: 'EXCLUDED', to_state: 'PROBATION', detail: { reason: 'RECONNECTED' } })
    ev('GAP_OPEN', t(9, 47, 25), { source: 'FEED_A', gap: { gap_id: gapId, source: 'FEED_A', epoch, from_seq: 18842, to_seq: 18901, from_ns: t(9, 47, 7), status: 'OPEN' } })
    ev('GAP_FILLED', t(9, 47, 26), { source: 'FEED_A', gap: { gap_id: gapId, source: 'FEED_A', epoch, from_seq: 18842, to_seq: 18901, from_ns: t(9, 47, 7), to_ns: t(9, 47, 25), status: 'FILLED', filled: 60, duplicates: 0, unrecoverable: 0 } })
    ev('SOURCE_STATE', t(9, 47, 36), { source: 'FEED_A', from_state: 'PROBATION', to_state: 'TRUSTED', detail: { reason: 'CAUGHT_UP', same_volume_matches: 3 } })
    ev('BAD_TICK', t(10, 5, 41), { source: 'FEED_B', instrument_id: 'j2701', detail: { reason: 'MD_QUARANTINE_SPIKE', price: 1692.5, offset_pct: 0.021 } })
    ev('QUARANTINE_DROPPED', t(10, 5, 43), { source: 'FEED_B', instrument_id: 'j2701', detail: { reason: 'NOT_CONFIRMED_2S' } })
    this.mdEvents.sort((a, b) => b.ts_ns - a.ts_ns)
    const sa = this.sources[0]
    if (sa) sa.sinceNs = t(9, 47, 36)
    // reports of past days
    this.history.forEach((d, i) => {
      const created = shanghaiToNs(Number(d.trading_day.slice(0, 4)), Number(d.trading_day.slice(4, 6)), Number(d.trading_day.slice(6, 8)), 15, 34, 10)
      for (const r of reportsForDay(d.trading_day, d.version ?? 1, this.rng, created)) this.reports.set(r.report_id, r)
      const next = this.history[i + 1]?.trading_day ?? DEMO_TRADING_DAY
      this.eodPhases.set(d.trading_day, { trading_day: d.trading_day, version: d.version ?? 1, phase: 'DONE', next_trading_day: next, detail: { reports: 7, accounts: 5 }, ts_ns: created + 3e9 })
    })
    // one past run with an EOD warning (an official price imported after the settlement)
    const warned = this.history[this.history.length - 3]
    if (warned) {
      const ts = shanghaiToNs(Number(warned.trading_day.slice(0, 4)), Number(warned.trading_day.slice(4, 6)), Number(warned.trading_day.slice(6, 8)), 17, 12, 40)
      this.addEodWarning({
        trading_day: warned.trading_day,
        version: warned.version ?? 1,
        phase: 'DONE',
        kind: 'OFFICIAL_PRICE_DIFF',
        message: `rb2701 交易所官方结算价 3452 与结算 v${warned.version ?? 1} 所用价格 3450 不同（+2）：如需按官方价重算请 force_new_version`,
        warn_id: this.rng.uuid(ts / 1e6),
        ts_ns: ts,
        context: { instrument: 'rb2701', official: '3452', used: '3450' },
      })
    }
  }

  private addEodWarning(w: EodWarningRecord): void {
    const key = `${w.trading_day}|${w.version}`
    const list = this.eodWarnings.get(key) ?? []
    list.push(w)
    this.eodWarnings.set(key, list)
  }

  private warningsOf(td: string, version: number): EodWarningRecord[] {
    return this.eodWarnings.get(`${td}|${version}`) ?? []
  }

  /**
   * A WARN of an EOD run (clearing eodwarn.warn_tx): an EodPhaseEvent repeating the run's
   * current phase with `detail.warning` / `kind` / `warn_id` — never a phase change.
   */
  private eodWarn(td: string, version: number, kind: string, message: string, context: Record<string, unknown>): void {
    const cur = this.eodPhases.get(td)
    const phase: EodPhase = cur?.phase ?? 'DONE'
    const ts = this.nowNs()
    const warnId = this.rng.uuid(ts / 1e6)
    this.addEodWarning({ trading_day: td, version, phase, kind, message, warn_id: warnId, ts_ns: ts, context })
    const ev: EodPhaseEvent = { trading_day: td, version, phase, next_trading_day: cur?.next_trading_day ?? null, detail: { warning: message, kind, warn_id: warnId, ...context }, error: null, ts_ns: ts }
    this.eodLog.push(ev)
    if (this.eodLog.length > 100) this.eodLog.shift()
    this.publish('eod', 'event', ev)
  }

  private simulateMorning(startMs: number): void {
    const t0 = shanghaiToNs(2026, 9, 28, 9, 0, 0) / 1e6
    for (let t = t0; t < startMs; t += 5000) {
      this.virtualNow = t
      this.step(5, false)
    }
    this.virtualNow = null
    this.lastPointMs = this.nowMs()
  }

  private seedBacktests(): void {
    const mk = (run_id: string, name: string, state: BacktestStatus['state'], progress: number, daysAgo: number, summary: Record<string, unknown> | null): void => {
      const sub = this.nowNs() - daysAgo * 86400e9
      const b: BacktestStatus = { run_id, name, state, progress, submitted_ns: sub, started_ns: sub + 2e9, finished_ns: state === 'DONE' ? sub + 190e9 : null, summary }
      if (state === 'DONE' && summary) {
        const r = this.newReport(null, 'BACKTEST_HTML', null, run_id, sub + 191e9)
        b.report_id = r.report_id
        this.backtestSummaries.set(r.report_id, summary)
      }
      this.backtests.set(run_id, b)
    }
    mk('bt-0192a1', 'default', 'DONE', 1, 3, {
      baseline: 'no_rules',
      days: 2852,
      no_rules: { total_ret: 0.4344, max_dd: 0.0781, worst_day: -0.0229, latched_days: 0, blocked_opens: 0 },
      block_open: { total_ret: 0.4351, max_dd: 0.0783, worst_day: -0.0224, latched_days: 40, blocked_opens: 11, rule_days: 100, effect_sum: 0.0091, next_effect_sum: -0.0061 },
    })
    mk('bt-0192b7', 'breaker_synthetic', 'DONE', 1, 2, { checks: 16, passed: 16, all_passed: true })
    mk('bt-0192c3', 'default', 'RUNNING', 0.35, 0, null)
  }

  // ---------------------------------------------------------------- step

  private step(dtS: number, live: boolean): void {
    const now = this.nowMs()
    this.stepCount += 1
    this.expireFaults(now)
    this.updateSources(now)
    this.advanceMarket(dtS, now)
    if (!live) this.applyMorningTrades(now)
    for (const a of this.accts.values()) this.revalue(a, now)
    if (live) this.runBots(now)
    const pointDue = now - this.lastPointMs >= 5000 || !live
    if (pointDue) {
      this.lastPointMs = now
      for (const a of this.accts.values()) this.recordPoint(a, live)
    }
    if (live) {
      this.tickBacktests()
      if (this.svcUp('md-gateway')) this.publishQuotes()
      for (const a of this.accts.values()) {
        if (!a.snapshot) continue
        if (this.shardUp(a.spec.shard)) {
          this.publish('accounts', 'upsert', this.summaryOf(a))
          this.publish(`positions:${a.spec.id}`, 'snapshot', a.snapshot.positions ?? [])
        } else if (this.stepCount % 4 === 0) {
          // owner shard down: the gateway re-sends the last item flagged stale (owner liveness)
          this.publish('accounts', 'upsert', this.summaryOf(a))
        }
      }
      if (this.stepCount % 2 === 0) {
        this.tickServices(now)
        if (this.svcUp('risk-engine-0')) {
          this.lastFirm = this.firmSnapshot()
          this.publish('firm', 'snapshot', this.firmTopic())
        }
        if (RISK_INSTANCES.some((i) => this.svcUp(i))) this.publish('compare', 'snapshot', this.compareView())
        if (this.svcUp('md-gateway')) {
          this.lastMd = this.mdStatus()
          this.publish('feeds', 'snapshot', this.lastMd)
        }
        this.publish('system', 'snapshot', this.systemTopicSnapshot())
      }
    }
  }

  // ---------------------------------------------------------------- market

  private newInst(spec: MockInstrument, preSettle: number): InstState {
    const upper = floorTick(preSettle * (1 + spec.limitPct), spec.tick)
    const lower = Number((Math.ceil((preSettle * (1 - spec.limitPct)) / spec.tick - 1e-9) * spec.tick).toFixed(6))
    return {
      spec,
      exchPx: preSettle,
      last: preSettle,
      bid: preSettle - spec.tick,
      ask: preSettle,
      open: null,
      high: null,
      low: null,
      volume: 0,
      turnover: 0,
      oi: spec.openInterest,
      preSettle,
      upper,
      lower,
      idio: 0,
      exchNs: 0,
      recvNs: 0,
      source: null,
      lastLiveMs: 0,
      wasInSession: false,
      quality: 'OK',
      limitLock: 'NONE',
    }
  }

  /** Scripted black-sector drift (log offset) on the demo trading day. */
  private scripted(i: InstState, now: number): number {
    if (i.spec.sector !== 'black' || this.tradingDay !== DEMO_TRADING_DAY) return 0
    const s = (now - this.scenarioStartMs) / 1000
    if (s < 0) {
      const m = minuteOfDay(now)
      return m <= 540 ? 0 : m >= 615 ? -0.008 : -0.008 * ((m - 540) / 75)
    }
    // ACC3 (~1.1x long black incl. the morning dip-buys bought lower) loses ≈ 1.0 × drift:
    // −2% (warn) at ~80s, −2.5% (restrict) at ~97s, −3% (halt, latched after 2 states / 1s) at
    // ~121s, plateau ≈ −3.15%, second leg to ≈ −4.25% (drawdown levels), partial recovery.
    // rb/hc stay above their −5% limit; −5% (derisk -> liquidation plan) needs a further
    // shock, e.g. the "breaker" scenario of the demo controls.
    let d: number
    if (s < 20) d = -0.008
    else if (s < 140) d = -0.008 - 0.0235 * smoothstep((s - 20) / 120)
    else if (s < 220) d = -0.0315 + 0.0005 * Math.sin((s - 140) / 6)
    else if (s < 290) d = -0.0315 - 0.011 * smoothstep((s - 220) / 70)
    else if (s < 360) d = -0.0425 + 0.006 * smoothstep((s - 290) / 70)
    else d = -0.0365 + 0.0015 * Math.sin((s - 360) / 23)
    return Math.log(1 + d)
  }

  private inScript(i: InstState, now: number): boolean {
    return i.spec.sector === 'black' && this.tradingDay === DEMO_TRADING_DAY && now >= this.scenarioStartMs - 60_000 && now - this.scenarioStartMs < 480_000
  }

  private shockOffset(i: InstState, now: number): number {
    let x = 0
    for (const s of this.shocks) {
      if (!s.targets.has(i.spec.id)) continue
      const prog = s.shape === 'STEP' || s.durationS <= 0 ? 1 : Math.max(0, Math.min(1, (now - s.startMs) / (s.durationS * 1000)))
      x += Math.log(1 + s.pct * prog)
    }
    return x
  }

  private liveSource(): SourceId | null {
    if (this.mdDown || !this.svcUp('md-gateway')) return null
    for (const s of this.sources) if (s.state === 'TRUSTED' && s.connected && s.baseLag + s.extraLag <= 3000) return s.id
    return null
  }

  private advanceMarket(dtS: number, now: number): void {
    const simUp = this.svcUp('sim-exchange')
    const live = this.liveSource()
    const src = live ? this.sources.find((s) => s.id === live) : undefined
    const nowNs = now * 1e6
    const sq = Math.sqrt(dtS / (252 * 14400)) * NOISE_AMP
    const zM = this.rng.gauss()
    const zS: Record<string, number> = {}
    let staleCount = 0
    let inSessCount = 0
    for (const i of this.inst.values()) {
      const sess = inSession(i.spec, now)
      if (sess && !i.wasInSession) i.lastLiveMs = now // grace at session open
      i.wasInSession = sess
      if (!sess) continue
      inSessCount += 1
      if (simUp) {
        const z = zS[i.spec.sector] ?? (zS[i.spec.sector] = this.rng.gauss())
        // Ornstein-Uhlenbeck: visible tick-to-tick moves, bounded drift (stationary σ ≈ 0.35%);
        // damped for the black sector while the breaker script runs so the demo timing is stable
        const damp = this.inScript(i, now) ? 0.4 : 1
        i.idio = i.idio * Math.exp(-OU_THETA * dtS) + damp * i.spec.volAnnual * sq * (0.447 * zM + 0.632 * z + 0.632 * this.rng.gauss())
        let px = i.preSettle * Math.exp(i.idio + this.scripted(i, now) + this.shockOffset(i, now))
        px = Math.min(i.upper, Math.max(i.lower, roundTick(px, i.spec.tick)))
        i.exchPx = px
        const q = Math.max(0, Math.round(i.spec.volumePerS * dtS * this.rng.range(0.4, 1.6)))
        i.volume += q
        i.turnover += px * q * i.spec.multiplier
        i.oi = Math.max(0, i.oi + Math.round(this.rng.gauss() * q * 0.15))
        i.limitLock = px >= i.upper ? 'UP' : px <= i.lower ? 'DOWN' : 'NONE'
      }
      if (live && src) {
        i.last = i.exchPx
        const spread = i.spec.tick * (this.rng.chance(0.8) ? 1 : 2)
        i.bid = i.limitLock === 'UP' ? i.last : Number((i.last - (this.rng.chance(0.5) ? 0 : i.spec.tick)).toFixed(6))
        i.ask = i.limitLock === 'DOWN' ? i.last : Number((i.bid + spread).toFixed(6))
        if (i.open === null) i.open = i.last
        i.high = i.high === null ? i.last : Math.max(i.high, i.last)
        i.low = i.low === null ? i.last : Math.min(i.low, i.last)
        i.exchNs = nowNs
        const lag = src.baseLag + src.extraLag + this.rng.range(-15, 25)
        i.recvNs = nowNs + Math.max(1, lag) * 1e6
        i.source = live
        i.lastLiveMs = now
        if (i.quality !== 'OK') this.setQuality(i, 'OK', now)
      } else {
        const age = now - i.lastLiveMs
        if (age > 30_000 && i.quality !== 'UNKNOWN') this.setQuality(i, 'UNKNOWN', now)
        else if (age > 5_000 && i.quality === 'OK') this.setQuality(i, 'STALE', now)
        if (i.quality !== 'OK') staleCount += 1
      }
    }
    // MD_DOWN / MD_RECOVERED once per outage
    if (inSessCount > 0) {
      if (staleCount === inSessCount && !this.mdDownEvent) {
        this.mdDownEvent = true
        this.mdEvent('MD_DOWN', { detail: { instruments: staleCount, reason: live ? 'NO_LIVE' : 'NO_TRUSTED_SOURCE' } })
      } else if (staleCount === 0 && this.mdDownEvent) {
        this.mdDownEvent = false
        this.mdEvent('MD_RECOVERED', { detail: { source: live } })
      }
    }
  }

  private mdDownEvent = false

  private setQuality(i: InstState, q: MdQuality, _now: number): void {
    const from = i.quality
    i.quality = q
    this.mdEvent('QUALITY_CHANGE', { instrument_id: i.spec.id, from_state: from, to_state: q })
  }

  private mdEvent(kind: MdEventKind, extra: Partial<MdEvent>): MdEvent {
    const ts = this.nowNs()
    const ev: MdEvent = { event_id: this.rng.uuid(ts / 1e6), kind, ts_ns: ts, trading_day: this.tradingDay, detail: {}, ...extra }
    this.mdEvents.unshift(ev)
    if (this.mdEvents.length > 600) this.mdEvents.pop()
    this.publish('feeds', 'event', ev)
    return ev
  }

  // ---------------------------------------------------------------- sources & faults

  private activeFault(type: string, target?: string): Fault | undefined {
    for (const f of this.faults.values()) if (f.type === type && (!target || !f.target || f.target === target)) return f
    return undefined
  }

  private expireFaults(now: number): void {
    for (const f of [...this.faults.values()]) {
      if (f.untilMs !== null && now >= f.untilMs) {
        this.faults.delete(f.fault_id)
        this.endFault(f, 'EXPIRED')
      }
    }
  }

  /** A fault that is no longer active goes to the sim's fault history (DONE / EXPIRED / DELETED). */
  private endFault(f: Fault, state: string): void {
    this.faultHistory.push(this.simFault(f, state))
    if (this.faultHistory.length > 50) this.faultHistory.shift()
  }

  private transition(s: SrcState, to: SourceState, detail: Record<string, unknown>): void {
    if (s.state === to) return
    const from = s.state
    s.state = to
    s.sinceNs = this.nowNs()
    this.mdEvent('SOURCE_STATE', { source: s.id, from_state: from, to_state: to, detail })
  }

  private updateSources(now: number): void {
    const simUp = this.svcUp('sim-exchange')
    const inSess = INSTRUMENTS.filter((i) => inSession(i, now)).length
    for (const s of this.sources) {
      const key = s.id === 'FEED_A' ? 'feed_a' : 'feed_b'
      const disc = this.activeFault('FEED_DISCONNECT', key) ?? (simUp ? undefined : ({ type: 'SIM' } as Fault))
      if (simUp && s.id === 'FEED_A') s.exchSeq += inSess
      if (disc) {
        if (s.connected) {
          s.connected = false
          this.transition(s, 'EXCLUDED', { reason: simUp ? 'DISCONNECTED' : 'SIM_DOWN' })
        }
      } else if (!s.connected) {
        s.connected = true
        this.transition(s, 'PROBATION', { reason: 'RECONNECTED' })
        s.probationUntil = now + 10_000
        if (s.id === 'FEED_A' && s.exchSeq > s.lastSeq + 1) this.openAndFillGap(s, s.lastSeq + 1, s.exchSeq, now)
      }
      const mal = this.activeFault('MALFORMED', key)
      s.rejectRate = mal ? Number(mal.params.ratio ?? 0.2) : Math.max(0, s.rejectRate * 0.5 - 0.001)
      if (s.rejectRate > 0.05 && s.state !== 'EXCLUDED') this.transition(s, 'EXCLUDED', { reason: 'REJECT_RATE', reject_rate: s.rejectRate })
      else if (!mal && s.state === 'EXCLUDED' && s.connected && s.rejectRate <= 0.05) {
        this.transition(s, 'PROBATION', { reason: 'REJECT_RATE_RECOVERED' })
        s.probationUntil = now + 10_000
      }
      const lat = this.activeFault('FEED_LATENCY', key)
      s.extraLag = lat ? Number(lat.params.ms ?? 0) : 0
      const dup = this.activeFault('FEED_DUPLICATE', key)
      s.dupRatio = dup ? Number(dup.params.ratio ?? 0.3) : 0
      if (s.state === 'PROBATION' && now >= s.probationUntil && s.connected) {
        const anyTrusted = this.sources.some((x) => x !== s && x.state === 'TRUSTED')
        this.transition(s, 'TRUSTED', anyTrusted ? { reason: 'CAUGHT_UP', same_volume_matches: 3 } : { reason: 'BOOTSTRAP', bootstrap: true })
      }
      if (s.connected && simUp) {
        if (s.id === 'FEED_A') s.lastSeq = s.exchSeq
        s.lastMsgNs = this.nowNs()
        const base = s.id === 'FEED_A' ? inSess * 2 : inSess
        s.msgsPerS = base * (1 + s.dupRatio) * this.rng.range(0.9, 1.1)
      } else s.msgsPerS = 0
    }
  }

  private openAndFillGap(s: SrcState, from: number, to: number, now: number): void {
    const gap: GapInfo = { gap_id: `feed_a:${s.epoch}:${from}-${to}`, source: s.id, epoch: s.epoch, from_seq: from, to_seq: to, from_ns: (now - 1000 * Math.max(1, (to - from) / 22)) * 1e6, status: 'OPEN' }
    this.mdEvent('GAP_OPEN', { source: s.id, gap })
    const done = { ...gap, to_ns: now * 1e6, status: 'FILLED' as const, filled: to - from + 1, duplicates: 0, unrecoverable: 0 }
    setTimeout(() => this.mdEvent('GAP_FILLED', { source: s.id, gap: done }), 900)
  }

  // ---------------------------------------------------------------- accounts

  private pos(a: AcctState, instrument: string, dir: 'LONG' | 'SHORT'): PosState {
    const key = `${instrument}|${dir}`
    let p = a.positions.get(key)
    if (!p) {
      p = { instrument, direction: dir, td: 0, yd: 0, tdCost: 0, ydOpenCost: 0, pendingClose: 0 }
      a.positions.set(key, p)
    }
    return p
  }

  private feeOf(spec: MockInstrument, price: number, qty: number, offset: Offset, closeToday: boolean): number {
    const notional = price * spec.multiplier * qty
    const f = spec.fee
    const mult = closeToday ? f.closeTodayMult : 1
    const base = offset === 'OPEN' ? f.open : f.closeYd
    const fee = f.type === 'rate' ? notional * base * mult : qty * base * mult
    return Math.round(fee * 100) / 100
  }

  /** Apply a fill to positions, close P&L (MTM basis) and fees. */
  private applyFill(a: AcctState, instrument: string, dir: Direction, offset: Offset, qty: number, price: number): void {
    const spec = INSTRUMENT_MAP[instrument]
    const i = this.inst.get(instrument)
    if (!spec || !i) return
    if (offset === 'OPEN') {
      const p = this.pos(a, instrument, dir === 'BUY' ? 'LONG' : 'SHORT')
      p.td += qty
      p.tdCost += price * qty
      a.fee += this.feeOf(spec, price, qty, 'OPEN', false)
      a.openVol[instrument] = (a.openVol[instrument] ?? 0) + qty
      return
    }
    const p = this.pos(a, instrument, dir === 'SELL' ? 'LONG' : 'SHORT')
    const sign = p.direction === 'LONG' ? 1 : -1
    let left = qty
    const useTodayFirst = offset === 'CLOSE_TODAY'
    const takeToday = (n: number) => {
      const avg = p.td > 0 ? p.tdCost / p.td : price
      p.td -= n
      p.tdCost -= avg * n
      a.closePnl += sign * (price - avg) * spec.multiplier * n
      a.fee += this.feeOf(spec, price, n, offset, true)
    }
    const takeYd = (n: number) => {
      const avgOpen = p.yd > 0 ? p.ydOpenCost / p.yd : price
      p.yd -= n
      p.ydOpenCost -= avgOpen * n
      a.closePnl += sign * (price - i.preSettle) * spec.multiplier * n
      a.fee += this.feeOf(spec, price, n, offset, false)
    }
    if (useTodayFirst) {
      const n = Math.min(left, p.td)
      if (n > 0) takeToday(n)
      left -= n
      if (left > 0) takeYd(Math.min(left, p.yd))
    } else {
      const n = Math.min(left, p.yd)
      if (n > 0) takeYd(n)
      left -= n
      if (left > 0) takeToday(Math.min(left, p.td))
    }
    if (Math.abs(p.tdCost) < 1e-6 && p.td === 0) p.tdCost = 0
  }

  private applyMorningTrades(now: number): void {
    for (const a of this.accts.values()) {
      while (a.morningDone < a.spec.morning.length) {
        const mt = a.spec.morning[a.morningDone]
        if (!mt) break
        const [h, m, s] = mt.at.split(':').map(Number) as [number, number, number]
        const at = shanghaiToNs(2026, 9, 28, h, m, s) / 1e6
        if (at > now) break
        const i = this.inst.get(mt.instrument)
        if (!i) break
        const price = mt.direction === 'BUY' ? i.ask : i.bid
        const oid = this.rng.uuid(at)
        const tsNs = at * 1e6
        this.applyFill(a, mt.instrument, mt.direction, mt.offset, mt.qty, price)
        const tr: Trade = { trading_day: this.tradingDay, exchange: i.spec.exchange, trade_id: String(100000 + this.rng.int(0, 899999)), direction: mt.direction, account_id: a.spec.id, instrument_id: mt.instrument, client_order_id: oid, origin: 'INTERNAL', broker_order_ref: `1:${a.spec.shard + 1}:${a.ordersToday + 1}`, offset: mt.offset, price, qty: mt.qty, trade_ts_ns: tsNs + 3e8, broker_seq: a.ordersToday + 1, trade_seq: a.trades.length + 1, received_ns: tsNs + 3.1e8 }
        a.trades.unshift(tr)
        a.orders.set(oid, { client_order_id: oid, account_id: a.spec.id, instrument_id: mt.instrument, exchange: i.spec.exchange, direction: mt.direction, offset: mt.offset, order_type: 'LIMIT', tif: 'GFD', price, qty: mt.qty, status: 'FILLED', filled_qty: mt.qty, avg_fill_price: price, last_fill: { trade_id: tr.trade_id, price, qty: mt.qty, trade_ts_ns: tr.trade_ts_ns }, broker_order_ref: tr.broker_order_ref, source: 'BOT', trading_day: this.tradingDay, created_ns: tsNs, updated_ns: tsNs + 3e8 })
        a.ordersToday += 1
        a.morningDone += 1
      }
    }
    // ACC3's resting open order (canceled by the breaker)
    const a3 = this.accts.get('ACC3')
    const hc = this.inst.get('hc2701')
    const restAt = shanghaiToNs(2026, 9, 28, 10, 11, 30) / 1e6
    if (a3 && hc && now >= restAt && ![...a3.orders.values()].some((o) => o.status === 'ACCEPTED')) {
      const oid = this.rng.uuid(restAt)
      const price = roundTick(hc.preSettle * 0.975, 1)
      a3.orders.set(oid, { client_order_id: oid, account_id: 'ACC3', instrument_id: 'hc2701', exchange: 'SHFE', direction: 'BUY', offset: 'OPEN', order_type: 'LIMIT', tif: 'GFD', price, qty: 5, status: 'ACCEPTED', filled_qty: 0, broker_order_ref: `1:2:${a3.ordersToday + 1}`, source: 'BOT', trading_day: this.tradingDay, created_ns: restAt * 1e6, updated_ns: restAt * 1e6 + 5e7 })
      a3.ordersToday += 1
    }
  }

  private shardUp(shard: number): boolean {
    return this.svcUp(`risk-engine-${shard}`)
  }

  private svcUp(instance: string): boolean {
    const s = this.svcs.get(instance)
    return !s || s.processState === 'RUNNING'
  }

  /** Full valuation + rule evaluation for one account (risk-engine snapshot). */
  private revalue(a: AcctState, now: number): void {
    if (!this.shardUp(a.spec.shard) && a.snapshot) return
    const nowNs = now * 1e6
    const positions: PositionView[] = []
    let positionPnl = 0
    const marginBySide: Record<string, { L: number; S: number; big: boolean }> = {}
    const expo: Record<string, ExposureView> = {}
    let worstQ: MdQuality = 'OK'
    const qRank: Record<MdQuality, number> = { OK: 0, STALE: 1, UNKNOWN: 2 }
    for (const p of a.positions.values()) {
      const qty = p.td + p.yd
      if (qty <= 0) continue
      const i = this.inst.get(p.instrument)
      if (!i) continue
      const spec = i.spec
      const px = i.last
      const sign = p.direction === 'LONG' ? 1 : -1
      const pnl = sign * spec.multiplier * (px * p.td - p.tdCost + (px - i.preSettle) * p.yd)
      const fifo = sign * spec.multiplier * (px * qty - (p.tdCost + p.ydOpenCost))
      const notional = px * spec.multiplier * qty
      const margin = notional * spec.marginRate
      positionPnl += pnl
      const mb = marginBySide[spec.product] ?? (marginBySide[spec.product] = { L: 0, S: 0, big: spec.bigSide })
      if (sign > 0) mb.L += margin
      else mb.S += margin
      if (qRank[i.quality] > qRank[worstQ]) worstQ = i.quality
      const e = expo[spec.product] ?? (expo[spec.product] = { product: spec.product, sector: spec.sector, exchange: spec.exchange, long_lots: 0, short_lots: 0, net_lots: 0, net_notional: 0, gross_notional: 0, net_pct_equity: 0, cap_pct: 0.03 / spec.limitPct, utilization: 0 })
      if (sign > 0) e.long_lots += qty
      else e.short_lots += qty
      e.net_lots += sign * qty
      e.net_notional += sign * notional
      e.gross_notional += notional
      const isShfe = spec.exchange === 'SHFE'
      positions.push({
        account_id: a.spec.id,
        instrument_id: p.instrument,
        product: spec.product,
        exchange: spec.exchange,
        direction: p.direction,
        td_qty: p.td,
        yd_qty: p.yd,
        closable_qty: Math.max(0, qty - p.pendingClose),
        closable_td_qty: isShfe ? p.td : 0,
        closable_yd_qty: isShfe ? p.yd : 0,
        avg_open_price: (p.tdCost + p.ydOpenCost) / qty,
        basis_avg_price: (p.tdCost + i.preSettle * p.yd) / qty,
        last: px,
        position_pnl: pnl,
        float_pnl_fifo: fifo,
        margin,
        notional,
        pct_equity: 0,
        limit_locked: i.limitLock !== 'NONE',
      })
    }
    let margin = 0
    for (const mb of Object.values(marginBySide)) margin += mb.big ? Math.max(mb.L, mb.S) : mb.L + mb.S
    let frozenMargin = 0
    for (const o of a.orders.values()) {
      if (o.offset !== 'OPEN' || !['ROUTING', 'ROUTED', 'ACCEPTED', 'PARTIALLY_FILLED'].includes(o.status)) continue
      const spec = INSTRUMENT_MAP[o.instrument_id]
      if (spec) frozenMargin += (o.price ?? 0) * spec.multiplier * (o.qty - (o.filled_qty ?? 0)) * spec.marginRate
    }
    const staticEq = a.preBalance + a.netDeposit
    const equity = staticEq + a.closePnl + positionPnl - a.fee
    const perf = equity - a.cumNetDeposit
    a.peak = Math.max(a.peak, perf)
    a.hwm = Math.max(a.hwm, perf)
    let gross = 0
    let net = 0
    let absNet = 0
    let stress = 0
    const exposures = Object.values(expo)
    for (const e of exposures) {
      // core/metrics.py: net % of equity is |net| / equity; utilization = net_pct / (3% / limit_pct)
      e.net_pct_equity = equity > 0 ? Math.abs(e.net_notional) / equity : 0
      e.utilization = e.cap_pct ? e.net_pct_equity / e.cap_pct : null
      gross += e.gross_notional
      net += e.net_notional
      absNet += Math.abs(e.net_notional)
      stress += Math.abs(e.net_notional) * (0.03 / (e.cap_pct ?? 0.6))
    }
    // effective N = 1 / HHI of |net notional| weights (effective_n_of); 0 when flat
    let hhi = 0
    for (const e of exposures) if (absNet > 0) hhi += (Math.abs(e.net_notional) / absNet) ** 2
    for (const p of positions) p.pct_equity = equity > 0 ? (p.notional ?? 0) / equity : 0
    const funds: AccountFunds = {
      initial_capital: a.spec.initialCapital,
      cum_net_deposit: a.cumNetDeposit,
      pre_balance: a.preBalance,
      net_deposit: a.netDeposit,
      static_equity: staticEq,
      close_pnl: a.closePnl,
      position_pnl: positionPnl,
      fee: a.fee,
      equity,
      margin,
      frozen_margin: frozenMargin,
      frozen_fee: frozenMargin * 0.001,
      available: equity - margin - frozenMargin - frozenMargin * 0.001,
      risk_degree: equity > 0 ? margin / equity : 0,
    }
    const dailyPnl = equity - staticEq
    const metrics: AccountMetricsView = {
      daily_pnl: dailyPnl,
      daily_pnl_pct_init: dailyPnl / a.spec.initialCapital,
      daily_pnl_pct_sod: staticEq > 0 ? dailyPnl / staticEq : 0,
      floating_pnl_pct_init: positionPnl / a.spec.initialCapital,
      perf_equity: perf,
      nav: perf / a.spec.initialCapital,
      intraday_peak_equity: a.peak,
      intraday_dd: a.peak > 0 ? (a.peak - perf) / a.peak : 0,
      hwm_equity: a.hwm,
      hwm_dd: a.hwm > 0 ? (a.hwm - perf) / a.hwm : 0,
      gross_notional: gross,
      net_notional: net,
      leverage: equity > 0 ? gross / equity : 0,
      effective_n: hhi > 0 ? 1 / hhi : 0,
      limit_stress: equity > 0 ? stress / equity : 0,
      orders_today: a.ordersToday,
      cancels_today: a.cancelsToday,
    }
    const rules = this.evaluateRules(a, funds, metrics, exposures, worstQ, now)
    this.aggregateState(a, now)
    a.version += 1
    a.snapshot = {
      account_id: a.spec.id,
      shard: a.spec.shard,
      trading_day: this.tradingDay,
      version: a.version,
      state_version: a.stateVersion,
      as_of_ns: nowNs,
      quality: worstQ,
      state: a.state,
      halted_latched: a.latched,
      trading_mode: a.tradingMode,
      restrictions: a.restrictions,
      funds,
      metrics,
      exposures: exposures.sort((x, y) => Math.abs(y.net_notional) - Math.abs(x.net_notional)),
      positions: positions.sort((x, y) => x.instrument_id.localeCompare(y.instrument_id)),
      rules,
      open_orders: [...a.orders.values()].filter((o) => ['ROUTING', 'ROUTED', 'ACCEPTED', 'PARTIALLY_FILLED', 'CANCEL_PENDING'].includes(o.status)).length,
    }
  }

  private metricValue(metric: string, f: AccountFunds, m: AccountMetricsView, a: AcctState): number | null {
    if (metric === 'account.compliance_util') {
      const openUtil = Math.max(0, ...Object.entries(a.openVol).map(([id, v]) => (INSTRUMENT_MAP[id]?.exchange === 'CFFEX' ? v / 500 : v / 5000)))
      return Math.max(openUtil, a.cancelsToday / 400)
    }
    const key = metric.replace(/^account\./, '')
    // effective_n_of() is None for a flat book: the rule is not evaluated
    if (key === 'effective_n' && m.effective_n <= 0) return null
    const mv = (m as unknown as Record<string, number>)[key]
    if (typeof mv === 'number') return mv
    const fv = (f as unknown as Record<string, number>)[key]
    return typeof fv === 'number' ? fv : null
  }

  private evaluateRules(a: AcctState, f: AccountFunds, m: AccountMetricsView, exposures: ExposureView[], q: MdQuality, now: number): RuleStatus[] {
    const out: RuleStatus[] = []
    const acct = `acct:${a.spec.id}`
    const frozen = q !== 'OK' || !this.publishingReady(a)
    for (const def of RULES) {
      if (def.enabled === false) continue
      const levels = effectiveLevels(def, a.spec.id)
      const scopes: { key: string; value: number | null }[] = []
      if (def.scope === 'account/product') for (const e of exposures) scopes.push({ key: `${acct}/prod:${e.product}`, value: e.utilization ?? null })
      else if (def.scope === 'account/sector') {
        const sec: Record<string, number> = {}
        for (const e of exposures) sec[e.sector] = (sec[e.sector] ?? 0) + e.net_notional
        for (const [s, v] of Object.entries(sec)) scopes.push({ key: `${acct}/sector:${s}`, value: f.equity > 0 ? Math.abs(v) / f.equity : null })
      } else scopes.push({ key: acct, value: this.metricValue(def.metric, f, m, a) })
      const hard = hardLimitLevel(levels)
      const marks = levelMarks(def, a.spec.id)
      // evaluation.rule_value: below min_gross_pct the rule is "not applicable" = not breached
      const grossPct = f.equity > 0 ? m.gross_notional / f.equity : m.gross_notional === 0 ? 0 : Infinity
      const notApplicable = typeof def.min_gross_pct === 'number' && grossPct < def.min_gross_pct
      if (notApplicable) for (const sc of scopes) sc.value = null
      for (const sc of scopes) {
        let active: RuleLevelDef | null = null
        let smTop: RuleSmState = 'OK'
        for (const lv of levels) {
          const key = `${def.id}|${sc.key}|${lv.name}`
          const sm = a.sm.get(key) ?? { state: 'OK' as RuleSmState, since: 0, confirm: 0, episode: 0, latched: false, rearm: null, cooldownUntil: 0, alertKey: null }
          a.sm.set(key, sm)
          if (sc.value !== null && !frozen) this.stepSm(a, def.id, def.metric, def.op, def.hysteresis ?? 0.2, (def.cooldown_s ?? 60) * 1000, def.hold_ms ?? 3000, lv, sc.key, sm, sc.value, now)
          else if (notApplicable && !frozen) this.clearNotApplicable(a, def.id, def.metric, (def.cooldown_s ?? 60) * 1000, def.hold_ms ?? 3000, lv, sc.key, sm, now)
          if (sm.state === 'ACTIVE' || sm.state === 'PENDING') active = lv
          if (sm.state !== 'OK') smTop = sm.state
        }
        const u = hard ? utilization(sc.value, hard.threshold, def.op, def.metric) : null
        const uVal = frozen ? null : sc.value
        const next = sc.value === null ? null : marks.find((mk) => mk.position > (u ?? 0) + 1e-12)
        out.push({
          rule_id: def.id,
          scope_key: sc.key,
          metric: def.metric,
          value: uVal,
          active_level: active?.name ?? null,
          sm_state: smTop,
          next_level: next?.name ?? null,
          next_threshold: next?.threshold ?? null,
          utilization: frozen ? null : u,
          headroom_money: next && sc.value !== null ? this.headroomMoney(def.metric, sc.value, next.threshold, f, m) : null,
        })
      }
    }
    return out
  }

  private publishingReady(a: AcctState): boolean {
    return a.tradingMode === 'OPEN'
  }

  private headroomMoney(metric: string, value: number, thr: number, f: AccountFunds, m: AccountMetricsView): number | null {
    switch (metric) {
      case 'account.daily_pnl_pct_init':
        return (value - thr) * f.initial_capital
      case 'account.intraday_dd':
        return (thr - value) * m.intraday_peak_equity
      case 'account.hwm_dd':
        return (thr - value) * m.hwm_equity
      case 'account.nav':
        return (value - thr) * f.initial_capital
      case 'account.risk_degree':
        return thr * f.equity - f.margin
      default:
        return null
    }
  }

  private stepSm(a: AcctState, ruleId: string, metric: string, op: string, h: number, cooldownMs: number, holdMs: number, lv: RuleLevelDef, scope: string, sm: LevelSm, value: number, now: number): void {
    const isCrossed = crossed(value, lv.threshold, op)
    const activate = (line: number = lv.threshold, rearmed = false) => {
      sm.state = 'ACTIVE'
      sm.since = now
      sm.episode += 1
      sm.latched = Boolean(lv.latch)
      sm.rearm = null
      sm.alertKey = `${this.tradingDay}|${ruleId}|${lv.name}|${scope}|${sm.episode}`
      const scopeTxt = scope.includes('/prod:') ? `（${scope.split('/prod:')[1]}）` : scope.includes('/sector:') ? `（${scope.split('/sector:')[1]}）` : ''
      const suffix = lv.action === 'CIRCUIT_BREAK' ? '，已熔断禁开' : lv.action === 'RESTRICT' ? '，限制下单' : lv.action === 'LIQUIDATE' ? '，已生成减仓计划' : ''
      this.riskEvent({
        kind: 'ALERT_RAISED',
        account_id: a.spec.id,
        scope_key: scope,
        alert_key: sm.alertKey,
        rule_id: ruleId,
        level: lv.name,
        episode: sm.episode,
        action: lv.action,
        severity: sevOf(lv.action),
        latched: lv.action === 'CIRCUIT_BREAK' ? true : null,
        metric,
        value,
        threshold: line,
        message: `${a.spec.id} ${ruleLabel(ruleId)}${scopeTxt} ${fmtMetric(metric, value)} 越过${rearmed ? `重新布防线（${levelLabel(lv.name)}）` : levelLabel(lv.name) + '线'} ${fmtMetric(metric, line)}${suffix}`,
        detail: rearmed ? { rearmed: true, level_threshold: lv.threshold } : {},
      })
      a.pendingCause = { cause: 'RULE', alertKey: sm.alertKey }
      if (lv.action === 'LIQUIDATE') this.createPlan(a, sm.alertKey, Number(lv.params?.step ?? 0.5), { ruleId, level: lv.name, scope, episode: sm.episode })
    }
    switch (sm.state) {
      case 'OK':
        if (isCrossed && now >= sm.cooldownUntil) {
          if (lv.confirm && ((lv.confirm.states ?? 1) !== 1 || (lv.confirm.ms ?? 0) > 0)) {
            sm.state = 'PENDING'
            sm.since = now
            sm.confirm = 1
            a.pendingCause = { cause: 'RULE' }
          } else activate()
        }
        break
      case 'PENDING':
        if (!isCrossed) {
          sm.state = 'OK'
          a.pendingCause = { cause: 'RULE' }
        } else {
          // state_machine._confirmed: N distinct states (states > 0), or confirm.ms elapsed; states = 0 is time-only
          sm.confirm += 1
          const states = lv.confirm?.states ?? 1
          const ms = lv.confirm?.ms ?? 0
          if ((states > 0 && sm.confirm >= states) || (ms > 0 && now - sm.since >= ms)) activate()
        }
        break
      case 'ACTIVE':
        // a level clears only after it was active for hold_ms and the value is back inside the clear line
        if (!sm.latched && now - sm.since >= holdMs && cleared(value, lv.threshold, op, h)) {
          sm.state = 'OK'
          sm.cooldownUntil = now + cooldownMs
          this.riskEvent({
            kind: 'ALERT_CLEARED',
            account_id: a.spec.id,
            scope_key: scope,
            alert_key: sm.alertKey,
            rule_id: ruleId,
            level: lv.name,
            episode: sm.episode,
            action: lv.action,
            severity: 'INFO',
            metric,
            value,
            threshold: lv.threshold,
            message: `${a.spec.id} ${ruleLabel(ruleId)} 回到解除线内（${fmtMetric(metric, value)}），${levelLabel(lv.name)}解除`,
          })
          a.pendingCause = { cause: 'RULE', alertKey: sm.alertKey }
        }
        break
      case 'REARMED':
        if (sm.rearm !== null && crossed(value, sm.rearm, op)) activate(sm.rearm, true)
        else if (cleared(value, lv.threshold, op, h)) sm.state = 'OK'
        break
    }
  }

  /** A level whose rule is not applicable (min_gross_pct) is not breached: pending drops, active clears after hold. */
  private clearNotApplicable(a: AcctState, ruleId: string, metric: string, cooldownMs: number, holdMs: number, lv: RuleLevelDef, scope: string, sm: LevelSm, now: number): void {
    if (sm.state === 'PENDING') {
      sm.state = 'OK'
      a.pendingCause = { cause: 'RULE' }
    } else if (sm.state === 'ACTIVE' && !sm.latched && now - sm.since >= holdMs) {
      sm.state = 'OK'
      sm.cooldownUntil = now + cooldownMs
      this.riskEvent({ kind: 'ALERT_CLEARED', account_id: a.spec.id, scope_key: scope, alert_key: sm.alertKey, rule_id: ruleId, level: lv.name, episode: sm.episode, action: lv.action, severity: 'INFO', metric, threshold: lv.threshold, message: `${a.spec.id} ${ruleLabel(ruleId)} 不适用（总名义低于门槛），${levelLabel(lv.name)}解除` })
      a.pendingCause = { cause: 'RULE', alertKey: sm.alertKey }
    }
  }

  private aggregateState(a: AcctState, now: number): void {
    let st: AccountState = 'NORMAL'
    let latched = false
    const restr: Restriction[] = []
    let causeKey: string | null = null
    let worstRank = -1
    for (const [key, sm] of a.sm) {
      if (sm.state !== 'ACTIVE' && sm.state !== 'PENDING') continue
      const [ruleId, scope, levelName] = key.split('|') as [string, string, string]
      const def = RULES.find((r) => r.id === ruleId)
      const lv = def ? effectiveLevels(def, a.spec.id).find((l) => l.name === levelName) : undefined
      if (!lv) continue
      if (sm.state === 'PENDING' && lv.action !== 'CIRCUIT_BREAK') continue
      const s = stateOfAction(lv.action)
      if (STATE_RANK[s] > STATE_RANK[st]) st = s
      if (lv.action === 'CIRCUIT_BREAK' && sm.state === 'ACTIVE' && sm.latched) latched = true
      if (STATE_RANK[s] > worstRank && sm.alertKey) {
        worstRank = STATE_RANK[s]
        causeKey = sm.alertKey
      }
      if (sm.state === 'ACTIVE' && lv.action === 'RESTRICT') {
        const src = `${ruleId}/${levelName}`
        const p = lv.params ?? {}
        if (typeof p.max_order_factor === 'number') restr.push({ kind: 'ORDER_SIZE_FACTOR', scope_key: `acct:${a.spec.id}`, value: p.max_order_factor, source: src, since_ns: sm.since * 1e6 || now * 1e6 })
        if (p.no_add_to_losers) restr.push({ kind: 'NO_ADD_TO_LOSERS', scope_key: `acct:${a.spec.id}`, source: src, since_ns: now * 1e6 })
        if (typeof p.exposure_cap_factor === 'number') restr.push({ kind: 'EXPOSURE_CAP_FACTOR', scope_key: `acct:${a.spec.id}`, value: p.exposure_cap_factor, source: src, since_ns: now * 1e6 })
        if (p.block_open) restr.push({ kind: 'BLOCK_OPEN', scope_key: scope, source: src, since_ns: now * 1e6 })
        if (p.block_open_largest_product) {
          const largest = a.snapshot?.exposures?.[0]?.product
          if (largest) restr.push({ kind: 'BLOCK_OPEN', scope_key: `acct:${a.spec.id}/prod:${largest}`, source: src, since_ns: now * 1e6 })
        }
      }
    }
    if (a.manualHalt) {
      st = 'HALTED'
      latched = true
    }
    const rk = JSON.stringify(restr.map((r) => [r.kind, r.scope_key, r.value ?? null, r.source]))
    if (st !== a.state || latched !== a.latched || rk !== a.restrictKey) {
      const from = a.state
      const enteringHalt = st === 'HALTED' && from !== 'HALTED'
      a.state = st
      a.latched = latched
      a.restrictions = restr
      a.restrictKey = rk
      a.stateVersion += 1
      const cause = a.pendingCause ?? { cause: 'RULE' }
      const chg: AccountStateChanged = {
        account_id: a.spec.id,
        trading_day: this.tradingDay,
        from_state: from,
        to_state: st,
        halted_latched: latched,
        state_version: a.stateVersion,
        restrictions: restr,
        cause: cause.cause,
        cause_alert_key: cause.alertKey ?? causeKey,
        actor: cause.actor ?? null,
        reason: cause.reason ?? null,
        ts_ns: now * 1e6,
      }
      a.lastChange = chg
      this.publish('events', 'event', chg)
      if (enteringHalt) this.cancelOpenOrders(a, 'CIRCUIT_BREAK_CANCEL')
    }
    a.pendingCause = null
  }

  private riskEvent(e: Omit<RiskEvent, 'event_id' | 'trading_day' | 'ts_ns'> & { ts_ns?: number }): RiskEvent {
    const ts = e.ts_ns ?? this.nowNs()
    const ev: RiskEvent = { event_id: this.rng.uuid(ts / 1e6), trading_day: this.tradingDay, ts_ns: ts, detail: {}, ...e }
    this.riskEvents.unshift(ev)
    if (this.riskEvents.length > 2000) this.riskEvents.pop()
    this.publish('events', 'event', ev)
    return ev
  }

  private createPlan(a: AcctState, alertKey: string, step: number, src: { ruleId: string; level: string; scope: string; episode: number }): void {
    const steps = [...a.positions.values()]
      .filter((p) => p.td + p.yd > 0)
      .map((p) => {
        const spec = INSTRUMENT_MAP[p.instrument]
        const offset: Offset = spec?.exchange === 'SHFE' ? (p.yd > 0 ? 'CLOSE_YESTERDAY' : 'CLOSE_TODAY') : 'CLOSE'
        return { instrument_id: p.instrument, direction: (p.direction === 'LONG' ? 'SELL' : 'BUY') as Direction, offset, qty: Math.max(1, Math.ceil((p.td + p.yd) * step)), price_rule: '对手价 -2 跳' }
      })
    const plan: LiquidationPlan = { plan_id: this.rng.uuid(this.nowMs()), account_id: a.spec.id, trading_day: this.tradingDay, trigger_alert_key: alertKey, status: 'PENDING_CONFIRM', steps, created_ns: this.nowNs() }
    this.plans.set(plan.plan_id, plan)
    this.riskEvent({
      kind: 'LIQUIDATION_PLAN',
      account_id: a.spec.id,
      scope_key: src.scope,
      alert_key: alertKey,
      rule_id: src.ruleId,
      level: src.level,
      episode: src.episode,
      severity: 'CRITICAL',
      action: 'LIQUIDATE',
      message: `${a.spec.id} 生成减仓计划 ${steps.length} 步，等待风控经理确认`,
      detail: { plan_id: plan.plan_id, plan: wire('LiquidationPlan', plan) },
    })
  }

  private recordPoint(a: AcctState, live: boolean): void {
    const s = a.snapshot
    if (!s) return
    const ruleUtil: Record<string, number> = {}
    for (const r of s.rules ?? []) if (typeof r.utilization === 'number') ruleUtil[`${r.rule_id}/${r.scope_key}`] = Number(r.utilization.toFixed(4))
    const pt: MetricPoint = { ts_ns: s.as_of_ns, equity: s.funds.equity, nav: s.metrics.nav, daily_pnl_pct_init: s.metrics.daily_pnl_pct_init, intraday_dd: s.metrics.intraday_dd, risk_degree: s.funds.risk_degree, gross_notional: s.metrics.gross_notional, state: s.state, rule_util: ruleUtil }
    a.points.push(pt)
    if (a.points.length > 6000) a.points.shift()
    if (live && this.shardUp(a.spec.shard)) this.publish(`equity:${a.spec.id}`, 'upsert', { ts_ns: pt.ts_ns, equity: pt.equity, daily_pnl_pct_init: pt.daily_pnl_pct_init, intraday_dd: pt.intraday_dd, state: pt.state, nav: pt.nav, risk_degree: pt.risk_degree })
  }

  // ---------------------------------------------------------------- orders & bots

  private cancelOpenOrders(a: AcctState, reason: string): void {
    for (const o of a.orders.values()) {
      if (o.offset !== 'OPEN' || !['ROUTING', 'ROUTED', 'ACCEPTED', 'PARTIALLY_FILLED'].includes(o.status)) continue
      o.status = 'CANCELED'
      o.reason = reason
      o.updated_ns = this.nowNs()
      a.cancelsToday += 1
      this.publish(`orders:${a.spec.id}`, 'upsert', { ...o })
    }
  }

  private runBots(now: number): void {
    for (const a of this.accts.values()) {
      if (now < a.nextBotMs) continue
      a.nextBotMs = now + (a.spec.id === 'ACC3' ? 4000 : a.spec.botEveryS * 1000 * this.rng.range(0.8, 1.25))
      if (!this.svcUp('strategy-sim')) continue
      const intent = this.botIntent(a)
      if (intent) this.submit(a, intent.instrument, intent.direction, intent.offset, intent.qty, intent.price)
    }
  }

  private botIntent(a: AcctState): { instrument: string; direction: Direction; offset: Offset; qty: number; price: number } | null {
    const step = a.botStep++
    const px = (id: string, dir: Direction) => {
      const i = this.inst.get(id)
      return i ? (dir === 'BUY' ? i.ask : i.bid) : 0
    }
    const closeOffset = (id: string, dir: 'LONG' | 'SHORT'): Offset => {
      const p = a.positions.get(`${id}|${dir}`)
      if (INSTRUMENT_MAP[id]?.exchange !== 'SHFE') return 'CLOSE'
      return p && p.td > 0 ? 'CLOSE_TODAY' : 'CLOSE_YESTERDAY'
    }
    const cycle = (id: string, dir: 'LONG' | 'SHORT', openFirst: boolean) => {
      const opening = (step % 2 === 0) === openFirst
      const d: Direction = opening ? (dir === 'LONG' ? 'BUY' : 'SELL') : dir === 'LONG' ? 'SELL' : 'BUY'
      return { instrument: id, direction: d, offset: opening ? ('OPEN' as Offset) : closeOffset(id, dir), qty: 1, price: px(id, d) }
    }
    switch (a.spec.id) {
      case 'ACC3': {
        const ids = ['rb2701', 'hc2701', 'i2701']
        const id = ids[Math.floor(step / 2) % ids.length] as string
        return cycle(id, 'LONG', true)
      }
      case 'ACC1': {
        if (step % 9 === 8) {
          const i = this.inst.get('rb2701')
          return i ? { instrument: 'rb2701', direction: 'SELL', offset: 'OPEN', qty: 1, price: roundTick(i.last * 0.94, 1) } : null
        }
        return cycle(step % 4 < 2 ? 'rb2701' : 'hc2701', 'SHORT', true)
      }
      case 'ACC2':
        if (step % 7 === 6) return { instrument: 'm2701', direction: 'SELL', offset: 'OPEN', qty: 1, price: 99999 }
        return cycle(step % 4 < 2 ? 'm2701' : 'cu2611', step % 4 < 2 ? 'SHORT' : 'LONG', true)
      case 'ACC4':
        if (step % 3 === 2) return { instrument: 'IF2612', direction: 'BUY', offset: 'OPEN', qty: 30, price: px('IF2612', 'BUY') }
        return cycle('T2612', 'LONG', true)
      case 'ACC5':
        return cycle(step % 4 < 2 ? 'rb2701' : 'rb2705', step % 4 < 2 ? 'LONG' : 'SHORT', true)
      default:
        return null
    }
  }

  /** Simplified pre-trade pipeline (02 §11.2); closes are exempt from steps 2-quality/kill, 4, 7, 8. */
  private pretrade(a: AcctState, i: InstState, dir: Direction, offset: Offset, qty: number, price: number): { code: string; detail?: Record<string, unknown> } | null {
    const spec = i.spec
    const opening = offset === 'OPEN'
    const maxQty = spec.product === 'IF' || spec.product === 'IC' ? 20 : 500
    if (qty <= 0) return { code: 'REJECT_INVALID_QTY' }
    if (qty > maxQty) return { code: 'REJECT_QTY_EXCEEDS_MAX', detail: { max: maxQty, qty } }
    if (a.tradingMode === 'EOD_FROZEN') return { code: 'REJECT_EOD_IN_PROGRESS' }
    if (!inSession(spec, this.nowMs())) return { code: 'REJECT_SESSION_CLOSED' }
    if (opening && i.quality !== 'OK') return { code: 'REJECT_MD_UNAVAILABLE', detail: { quality: i.quality } }
    if (opening && this.killManual) return { code: 'REJECT_FIRM_KILL_SWITCH' }
    if (!this.svcUp('trade-gateway')) return { code: 'REJECT_GATEWAY_UNAVAILABLE' }
    if (price > i.upper || price < i.lower) return { code: 'REJECT_PRICE_LIMIT', detail: { price, upper: i.upper, lower: i.lower } }
    const dev = Math.abs(price - i.last)
    if (dev > Math.max(50 * spec.tick, 0.03 * i.last)) return { code: 'REJECT_FAT_FINGER', detail: { price, mark: i.last, max_dev: Math.max(50 * spec.tick, 0.03 * i.last) } }
    if (opening) {
      if (a.state === 'HALTED') return { code: 'REJECT_ACCOUNT_HALTED', detail: { halted_latched: a.latched } }
      for (const r of a.restrictions) {
        if (r.kind === 'BLOCK_OPEN' && r.scope_key === `acct:${a.spec.id}`) return { code: 'REJECT_ACCOUNT_BLOCKED', detail: { source: r.source } }
        if (r.kind === 'BLOCK_OPEN' && (r.scope_key.endsWith(`/prod:${spec.product}`) || r.scope_key.endsWith(`/sector:${spec.sector}`))) return { code: 'REJECT_PRODUCT_BLOCKED', detail: { source: r.source, scope_key: r.scope_key } }
        if (r.kind === 'ORDER_SIZE_FACTOR' && typeof r.value === 'number' && qty > Math.floor(maxQty * r.value)) return { code: 'REJECT_ORDER_SIZE_RESTRICTED', detail: { max: Math.floor(maxQty * r.value) } }
        if (r.kind === 'NO_ADD_TO_LOSERS') {
          const posDir = dir === 'BUY' ? 'LONG' : 'SHORT'
          const pv = a.snapshot?.positions?.find((p) => p.product === spec.product && p.direction === posDir)
          if (pv && (pv.position_pnl ?? 0) < 0) return { code: 'REJECT_ADD_TO_LOSER', detail: { product: spec.product, position_pnl: Math.round(pv.position_pnl ?? 0), source: r.source } }
        }
      }
      const need = price * spec.multiplier * qty * spec.marginRate
      const avail = a.snapshot?.funds.available ?? 0
      if (need > avail * 0.95) return { code: 'REJECT_INSUFFICIENT_FUNDS', detail: { required: Math.round(need), available: Math.round(avail) } }
    } else {
      const p = a.positions.get(`${i.spec.id}|${dir === 'SELL' ? 'LONG' : 'SHORT'}`)
      const closable = p ? p.td + p.yd - p.pendingClose : 0
      if (qty > closable) return { code: 'REJECT_CLOSE_EXCEEDS_POSITION', detail: { closable, qty } }
      if (offset === 'CLOSE_TODAY' && p && qty > p.td) return { code: 'REJECT_CLOSE_TODAY_EXCEEDS', detail: { closable_td: p.td } }
      if (offset === 'CLOSE_YESTERDAY' && p && qty > p.yd) return { code: 'REJECT_CLOSE_YD_EXCEEDS', detail: { closable_yd: p.yd } }
    }
    return null
  }

  private submit(a: AcctState, instrument: string, dir: Direction, offset: Offset, qty: number, price: number): void {
    const i = this.inst.get(instrument)
    if (!i) return
    const nowNs = this.nowNs()
    const oid = this.rng.uuid(this.nowMs())
    a.ordersToday += 1
    const base: OrderUpdate = { client_order_id: oid, account_id: a.spec.id, instrument_id: instrument, exchange: i.spec.exchange, direction: dir, offset, order_type: 'LIMIT', tif: 'GFD', price, qty, status: 'ROUTING', filled_qty: 0, source: 'BOT', trading_day: this.tradingDay, created_ns: nowNs, updated_ns: nowNs }
    const rej = this.pretrade(a, i, dir, offset, qty, price)
    if (rej) {
      const o: OrderUpdate = { ...base, status: 'REJECTED', reject_code: rej.code, reject_detail: rej.detail ?? null }
      a.orders.set(oid, o)
      this.publish(`orders:${a.spec.id}`, 'upsert', { ...o })
      return
    }
    a.orders.set(oid, base)
    if (offset !== 'OPEN') {
      const p = a.positions.get(`${instrument}|${dir === 'SELL' ? 'LONG' : 'SHORT'}`)
      if (p) p.pendingClose += qty
    }
    this.publish(`orders:${a.spec.id}`, 'upsert', { ...base })
    const ref = `1:${a.spec.shard + 1}:${a.ordersToday}`
    setTimeout(() => {
      const o = a.orders.get(oid)
      if (!o || o.status !== 'ROUTING') return
      o.status = 'ACCEPTED'
      o.broker_order_ref = ref
      o.updated_ns = this.nowNs()
      this.publish(`orders:${a.spec.id}`, 'upsert', { ...o })
    }, 90)
    setTimeout(() => {
      const o = a.orders.get(oid)
      if (!o || (o.status !== 'ACCEPTED' && o.status !== 'ROUTING')) return
      if (offset !== 'OPEN') {
        const p = a.positions.get(`${instrument}|${dir === 'SELL' ? 'LONG' : 'SHORT'}`)
        if (p) p.pendingClose = Math.max(0, p.pendingClose - qty)
      }
      if (offset === 'OPEN' && a.state === 'HALTED') return // canceled by the breaker meanwhile
      const fillPx = price
      this.applyFill(a, instrument, dir, offset, qty, fillPx)
      const ts = this.nowNs()
      const tr: Trade = { trading_day: this.tradingDay, exchange: i.spec.exchange, trade_id: String(200000 + this.rng.int(0, 799999)), direction: dir, account_id: a.spec.id, instrument_id: instrument, client_order_id: oid, origin: 'INTERNAL', broker_order_ref: ref, offset, price: fillPx, qty, trade_ts_ns: ts, broker_seq: a.ordersToday, trade_seq: a.trades.length + 1, received_ns: ts + 2e6 }
      a.trades.unshift(tr)
      if (a.trades.length > 500) a.trades.pop()
      o.status = 'FILLED'
      o.filled_qty = qty
      o.avg_fill_price = fillPx
      o.last_fill = { trade_id: tr.trade_id, price: fillPx, qty, trade_ts_ns: ts }
      o.updated_ns = ts
      this.publish(`orders:${a.spec.id}`, 'upsert', { ...o })
    }, 380)
  }

  // ---------------------------------------------------------------- services

  private tickServices(now: number): void {
    const nowNs = now * 1e6
    const wall = Date.now()
    const anyExcluded = this.sources.some((s) => s.state !== 'TRUSTED')
    for (const s of this.svcs.values()) {
      if (s.processState === 'RUNNING') {
        s.lastHbNs = nowNs
        s.hbState = s.spec.instance === 'md-gateway' && anyExcluded ? 'DEGRADED' : 'RUNNING'
        if (s.clockVersion < this.clockVersion && wall >= s.clockApplyAt) s.clockVersion = this.clockVersion
        s.loopLag = Math.round(this.rng.range(0, s.spec.instance.startsWith('risk') ? 12 : 30))
        for (const k of Object.keys(s.queues)) s.queues[k] = Math.max(0, Math.round((s.spec.queues[k] ?? 0) + this.rng.gauss() * 2))
        for (const k of Object.keys(s.rates)) s.rates[k] = Math.max(0, Number(((s.spec.rates[k] ?? 0) * this.rng.range(0.8, 1.2)).toFixed(2)))
        if (s.spec.instance === 'md-gateway') {
          const inSess = INSTRUMENTS.filter((i) => inSession(i, now)).length
          s.rates.ticks_in = Number((this.sources.reduce((acc, x) => acc + x.msgsPerS, 0)).toFixed(1))
          s.rates.ticks_live = Number((inSess * 2 * (this.liveSource() ? 1 : 0)).toFixed(1))
          s.rates.rejects = Number((this.sources.reduce((acc, x) => acc + x.rejectRate * x.msgsPerS, 0)).toFixed(2))
        }
        for (const [k, l] of Object.entries(s.latency)) {
          const [p50, p99] = s.spec.latency[k] ?? [l.p50, l.p99]
          l.p50 = Math.round(p50 * this.rng.range(0.85, 1.2))
          l.p99 = Math.round(p99 * this.rng.range(0.8, 1.35))
          l.max = Math.round(l.p99 * this.rng.range(1.2, 2.2))
          l.n += 1000
        }
      }
    }
  }

  /** The gateway's peer view (bus/heartbeat.py): UP while heartbeats arrive, DOWN after 3 missed. */
  private liveness(s: SvcState): Liveness {
    if (s.spec.instance === 'api-gateway') return 'UP'
    if (s.processState === 'RUNNING' || s.processState === 'STARTING' || s.downSinceMs === null) return 'UP'
    return Date.now() - s.downSinceMs >= PEER_TIMEOUT_MS ? 'DOWN' : 'UP'
  }

  /** WS `system` item (readmodel.system_items + kind): heartbeat fields, state = ProcessState once gone. */
  private systemItem(s: SvcState): SystemTopicItem {
    const gone = s.processState === 'EXITED' || s.processState === 'RESTARTING' || s.processState === 'STOPPED' || s.processState === 'FAILED'
    return {
      kind: 'service',
      instance: s.spec.instance,
      restarts: s.restarts,
      service: s.spec.service,
      state: gone ? s.processState : (s.hbState ?? s.processState),
      queues: { ...s.queues },
      rates: { ...s.rates },
      latency: { ...s.latency },
      loop_lag_ms: s.loopLag,
      config_version: CONFIG_VERSION,
      config_hash: CONFIG_HASH,
      clock_version: s.clockVersion,
      pid: s.pid,
      started_ns: s.startedNs,
      trading_mode: s.spec.service === 'risk-engine' ? (this.accts.get(s.spec.instance === 'risk-engine-0' ? 'ACC1' : 'ACC3')?.tradingMode ?? 'OPEN') : null,
      deps: this.depsOf(s),
      liveness: this.liveness(s),
    }
  }

  /** WS `system` snapshot: every service item plus the market clock item. */
  systemTopicSnapshot(): (SystemTopicItem | ClockTopicItem)[] {
    const clock: ClockTopicItem = { ...wire('ClockState', this.clockState()), kind: 'clock', instance: 'sys.clock' }
    return [...[...this.svcs.values()].map((s) => this.systemItem(s)), clock]
  }

  private heartbeatOf(s: SvcState): Heartbeat {
    return {
      instance: s.spec.instance,
      service: s.spec.service,
      state: s.hbState ?? 'STARTING',
      pid: s.pid,
      epoch: `ep-${s.pid}`,
      app_version: APP_VERSION,
      proto: 1,
      config_version: CONFIG_VERSION,
      config_hash: CONFIG_HASH,
      clock_version: s.clockVersion,
      loop_lag_ms: s.loopLag,
      started_ns: s.startedNs,
      trading_day: this.tradingDay,
      trading_mode: s.spec.service === 'risk-engine' ? 'OPEN' : null,
      queues: s.queues,
      rates: s.rates,
      latency_us: s.latency,
      versions: PAYLOAD_VERSIONS,
      deps: this.depsOf(s),
      detail: s.spec.instance === 'api-gateway' ? { http_port: 8000, seeded: [], synced: Object.fromEntries([...this.svcs.values()].filter((x) => !x.spec.external && x !== s).map((x) => [x.spec.instance, x.processState === 'RUNNING'])) } : {},
    }
  }

  /** Peer liveness as a service reports it in its heartbeat (`deps`). */
  private depsOf(s: SvcState): Record<string, string> {
    const peers = s.spec.instance === 'api-gateway'
      ? [...this.svcs.values()].filter((x) => !x.spec.external).map((x) => x.spec.instance)
      : s.spec.service === 'risk-engine'
        ? ['md-gateway', 'trade-gateway', ...(s.spec.instance === 'risk-engine-1' ? ['risk-engine-0'] : [])]
        : []
    return Object.fromEntries(peers.sort().map((p) => [p, this.svcs.get(p) ? this.liveness(this.svcs.get(p) as SvcState) : 'UNKNOWN']))
  }

  private svcEvent(s: SvcState, to: ProcessState, reason: ServiceChangeReason, exitCode: number | null = null, actor: string | null = null): void {
    const ev: ServiceStateChanged = { instance: s.spec.instance, service: s.spec.service, pid: to === 'RUNNING' || to === 'STARTING' ? s.pid : null, from_state: s.processState, to_state: to, exit_code: exitCode, restarts: s.restarts, reason, actor, trading_day: this.tradingDay, ts_ns: this.nowNs() }
    s.processState = to
    s.sinceNs = ev.ts_ns
    if (to === 'RUNNING' || to === 'STARTING') s.downSinceMs = null
    else if (s.downSinceMs === null) s.downSinceMs = Date.now()
    if (to !== 'RUNNING') s.hbState = to === 'STARTING' ? 'STARTING' : null
    this.serviceEvents.unshift(ev)
    if (this.serviceEvents.length > 300) this.serviceEvents.pop()
    this.publish('system', 'event', ev)
  }

  /** Supervisor `svc.ps` entry (ServiceInfo). */
  serviceInfo(s: SvcState): ServiceInfo {
    const running = s.processState === 'RUNNING'
    return wire('ServiceInfo', {
      instance: s.spec.instance,
      service: s.spec.service,
      state: s.processState,
      pid: running || s.processState === 'STARTING' ? s.pid : null,
      started_ns: s.startedNs,
      restarts: s.restarts,
      last_exit_code: s.lastExit,
      heartbeat_state: running ? s.hbState : null,
      last_heartbeat_ns: s.lastHbNs,
    })
  }

  /** `GET /system/services` (routes/system.service_items): ServiceInfo + heartbeat + liveness. */
  listServices(): SystemServicesResponse {
    const items: ServiceItem[] = [...this.svcs.values()]
      .sort((a, b) => a.spec.instance.localeCompare(b.spec.instance))
      .map((s) => {
        const info = this.serviceInfo(s)
        // since_ns: since when the process state holds; heartbeat_ns: the gateway's last heartbeat receipt
        const item: ServiceItem = { ...info, process_state: s.processState, restarts: s.restarts, since_ns: s.sinceNs, heartbeat_ns: s.lastHbNs }
        if (s.lastHbNs !== null) {
          item.heartbeat_state = s.hbState ?? 'STARTING'
          item.last_heartbeat_ns = s.lastHbNs
          item.heartbeat = wire('Heartbeat', this.heartbeatOf(s))
          if (item.pid === undefined) item.pid = s.pid
          if (item.started_ns === undefined) item.started_ns = s.startedNs
        }
        item.liveness = this.liveness(s)
        return item
      })
    const events = wireList('ServiceStateChanged', [...this.serviceEvents].reverse().slice(-100))
    return { items, events, supervisor: { ok: true, code: 'OK', message: '' } }
  }

  private svc(instance: string): SvcState {
    const s = this.svcs.get(instance)
    if (!s) throw new MockError('NOT_FOUND', `unknown instance ${instance}`, 404)
    return s
  }

  killService(instance: string, actor: string): ServiceInfo {
    const s = this.svc(instance)
    if (instance === 'supervisor') throw new MockError('BAD_REQUEST', 'supervisor 是进程守护本身，演示中不可 kill', 400)
    if (s.processState !== 'RUNNING') throw new MockError('CONFLICT', `${instance} is ${s.processState}`, 409)
    s.lastExit = -9
    this.svcEvent(s, 'EXITED', 'KILLED_BY_DEMO', -9, actor)
    if (instance === 'md-gateway') this.mdDown = true
    if (instance === 'api-gateway') {
      this.apiDownUntil = Date.now() + (s.spec.restartDelayS + 2) * 1000
      setTimeout(() => this.onApiRestart?.(), 50)
    }
    setTimeout(() => this.svcEvent(s, 'RESTARTING', 'KILLED_BY_DEMO'), 1000)
    setTimeout(() => {
      s.pid = 4000 + this.rng.int(100, 9000)
      s.startedNs = this.nowNs()
      this.svcEvent(s, 'STARTING', 'STARTUP')
    }, (1 + s.spec.restartDelayS) * 1000)
    setTimeout(() => {
      s.restarts += 1
      s.hbState = 'RUNNING'
      s.clockVersion = this.clockVersion
      this.svcEvent(s, 'RUNNING', 'STARTUP')
      if (instance === 'md-gateway') this.mdDown = false
    }, (2.5 + s.spec.restartDelayS) * 1000)
    return this.serviceInfo(s)
  }

  restartService(instance: string, actor: string): ServiceInfo {
    const s = this.svc(instance)
    if (instance === 'supervisor') throw new MockError('BAD_REQUEST', 'supervisor 不能通过自身重启', 400)
    if (s.processState !== 'RUNNING') throw new MockError('CONFLICT', `${instance} is ${s.processState}`, 409)
    this.svcEvent(s, 'STOPPING', 'MANUAL', null, actor)
    if (instance === 'md-gateway') this.mdDown = true
    setTimeout(() => {
      s.lastExit = 0
      this.svcEvent(s, 'STOPPED', 'MANUAL', 0, actor)
    }, 800)
    setTimeout(() => {
      s.pid = 4000 + this.rng.int(100, 9000)
      s.startedNs = this.nowNs()
      this.svcEvent(s, 'STARTING', 'MANUAL', null, actor)
    }, 1600)
    setTimeout(() => {
      s.restarts += 1
      s.hbState = 'RUNNING'
      this.svcEvent(s, 'RUNNING', 'STARTUP')
      if (instance === 'md-gateway') this.mdDown = false
    }, 3200)
    return this.serviceInfo(s)
  }

  // ---------------------------------------------------------------- clock

  clockState(): ClockState {
    return { mode: 'DEMO', offset_ns: Math.round(this.offsetMs * 1e6), version: this.clockVersion, market_now_ns: this.nowNs(), actor: null, reason: null }
  }

  /** Steps of a clock jump (clockjump.ClockJumpOrchestrator). */
  private jumpResult(steps: ClockJumpStep[], clock: ClockState | null, lagging: string[] = [], waitedMs = 0): ClockJumpResult {
    return { clock: clock ? wire('ClockState', clock) : null, steps, waited_ms: waitedMs, converged: lagging.length === 0, lagging }
  }

  private nextSessionStart(now: number): number {
    const p = shanghaiParts(now)
    const starts = [540, 570, 630, 780, 810, 1260]
    for (let dayOff = 0; dayOff < 5; dayOff++) {
      for (const m of starts) {
        const t = shanghaiToNs(p.y, p.mo, p.d + dayOff, Math.floor(m / 60), m % 60) / 1e6
        if (t - 120_000 > now + 1000) return t
      }
    }
    return now + 3600_000
  }

  /**
   * `POST /demo/clock` (02 §8.8): pause the sim, the supervisor moves the clock, the sim
   * follows, wait for every live instance's clock_version, resume. A backwards jump is
   * refused by the supervisor: the answer is ok=false with the steps done so far.
   */
  clockJump(target: string, at: string | number | undefined, atNs: number | undefined, actor: string): MockResult<ClockJumpResult> {
    const now = this.nowMs()
    const p = shanghaiParts(now)
    const fail = (code: string, message: string): MockResult<ClockJumpResult> => {
      const steps: ClockJumpStep[] = [
        { step: 1, action: 'sim.pause', ok: true, detail: null },
        { step: 2, action: 'svc.clock', ok: false, detail: { code, message } },
        { step: 5, action: 'sim.resume', ok: true, detail: null },
      ]
      return new MockResult(this.jumpResult(steps, null), `svc.clock failed: ${message}`, false, code)
    }
    let targetMs: number
    if (target === 'PRE_CLOSE') {
      targetMs = shanghaiToNs(p.y, p.mo, p.d, Math.floor(PRE_CLOSE_MIN / 60), PRE_CLOSE_MIN % 60) / 1e6
      if (targetMs <= now) return fail('CONFLICT', 'the market clock only moves forward (already past pre_close_at)')
    } else if (target === 'NEXT_OPEN') {
      targetMs = this.nextSessionStart(now) - 120_000
    } else if (target === 'AT') {
      let t: number | null = null
      if (typeof atNs === 'number' && Number.isFinite(atNs)) t = atNs / 1e6
      else if (typeof at === 'number' && Number.isFinite(at)) t = at / 1e6
      else if (typeof at === 'string' && at) {
        // ISO 8601; a naive time is Beijing market time (gateway.parse_market_time)
        const iso = /[zZ]|[+-]\d{2}:?\d{2}$/.test(at) ? at : `${at}+08:00`
        const parsed = Date.parse(iso)
        if (Number.isFinite(parsed)) t = parsed
      }
      if (t === null) throw new MockError('BAD_REQUEST', 'target AT needs at (ISO 8601 market time or epoch ns) or at_ns', 400)
      if (t <= now) return fail('CONFLICT', 'the market clock only moves forward')
      targetMs = t
    } else throw new MockError('BAD_REQUEST', `target: Input should be 'PRE_CLOSE', 'NEXT_OPEN' or 'AT'`, 400)
    const eodAt = shanghaiToNs(Number(this.tradingDay.slice(0, 4)), Number(this.tradingDay.slice(4, 6)), Number(this.tradingDay.slice(6, 8)), Math.floor(EOD_START_MIN / 60), EOD_START_MIN % 60) / 1e6
    const closeAt = eodAt - 15 * 60_000
    const crossesClose = now < closeAt && targetMs >= closeAt
    const crossesEod = targetMs >= eodAt && !this.eodPhases.has(this.tradingDay) && !this.eodRunning
    // pause -> set offset -> wait for clock_version -> resume (02 §8.8)
    this.offsetMs += targetMs - now
    this.clockVersion += 1
    const wall = Date.now()
    for (const s of this.svcs.values()) s.clockApplyAt = wall + this.rng.range(300, 1800)
    for (const i of this.inst.values()) {
      i.lastLiveMs = targetMs
      i.wasInSession = inSession(i.spec, targetMs)
    }
    for (const a of this.accts.values()) a.nextBotMs = targetMs + 2000
    this.lastPointMs = targetMs
    if (crossesClose) for (const a of this.accts.values()) this.cancelOpenOrders(a, 'SESSION_CLOSE')
    const cs: ClockState = { ...this.clockState(), actor, reason: `demo jump ${target}` }
    if (crossesEod) setTimeout(() => this.runEod(this.tradingDay, 1, actor), 600)
    // the heartbeats confirm the new version within ~1.8 s; instances that are down lag
    const lagging = [...this.svcs.values()].filter((s) => s.processState !== 'RUNNING' && !s.spec.external).map((s) => s.spec.instance).sort()
    for (const s of this.svcs.values()) if (s.processState === 'RUNNING') s.clockVersion = this.clockVersion
    const waited = lagging.length ? 5000 : Math.round(this.rng.range(300, 1800))
    const steps: ClockJumpStep[] = [
      { step: 1, action: 'sim.pause', ok: true, detail: null },
      { step: 2, action: 'svc.clock', ok: true, detail: { version: cs.version, offset_ns: cs.offset_ns, market_now_ns: cs.market_now_ns } },
      { step: 3, action: 'sim.clock', ok: true, detail: { ok: true, market_now_ns: cs.market_now_ns } },
      { step: 4, action: 'wait.heartbeats', ok: lagging.length === 0, detail: { lagging, waited_ms: waited } },
      { step: 5, action: 'sim.resume', ok: true, detail: null },
    ]
    return new MockResult(this.jumpResult(steps, cs, lagging, waited))
  }

  // ---------------------------------------------------------------- EOD saga

  runEod(td: string, version: number, _actor: string): { trading_day: string; version: number; phase: EodPhase } {
    this.clearing()
    if (this.eodRunning) throw new MockError('CONFLICT', 'an EOD run is in progress', 409)
    const isCurrent = td === this.tradingDay
    if (!isCurrent && !this.history.some((d) => d.trading_day === td)) throw new MockError('NOT_FOUND', `交易日 ${td} 没有可结算的数据`, 404)
    this.eodRunning = true
    const nextTd = isCurrent ? (NEXT_TRADING_DAYS[NEXT_TRADING_DAYS.indexOf(td) + 1] ?? td) : (NEXT_TRADING_DAYS[0] ?? td)
    const phases: EodPhase[] = ['BEGIN', 'FROZEN', 'TRADES_RECONCILED', 'PRICES_FIXED', 'SETTLED', 'ROLLED', 'REPORTED', 'DONE']
    let idx = 0
    const settlePx: Record<string, number> = {}
    const fire = () => {
      const phase = phases[idx++] as EodPhase
      const detail: Record<string, unknown> = {}
      try {
        if (phase === 'FROZEN' && isCurrent) {
          for (const a of this.accts.values()) {
            a.tradingMode = 'EOD_FROZEN'
            this.cancelOpenOrders(a, 'SESSION_CLOSE')
          }
          detail.accounts = this.accts.size
          detail.open_orders = 0
        } else if (phase === 'TRADES_RECONCILED') {
          detail.trades = [...this.accts.values()].reduce((n, a) => n + a.trades.length, 0)
          detail.patched = 0
        } else if (phase === 'PRICES_FIXED') {
          for (const i of this.inst.values()) settlePx[i.spec.id] = i.spec.exchange === 'SHFE' ? floorTick(i.last, i.spec.tick) : roundTick(i.last, i.spec.tick)
          detail.sources = { VWAP: this.inst.size }
          // md-gateway's previous settlement of j2701 differs from the SOD's (the SOD value is used)
          const j = this.inst.get('j2701')
          if (j && isCurrent) {
            const sod = j.preSettle
            const md = roundTick(sod + j.spec.tick, j.spec.tick)
            detail.pre_settle_mismatch = { j2701: { sod, md } }
            const message = `md-gateway pre_settle differs from the SOD (SOD used): j2701 SOD ${sod} / md ${md}`
            detail.warnings = [{ kind: 'PRE_SETTLE_MISMATCH', message }]
            this.addEodWarning({ trading_day: td, version, phase, kind: 'PRE_SETTLE_MISMATCH', message, warn_id: `${td}|${version}|${phase}|0`, ts_ns: this.nowNs(), context: {} })
          }
        } else if (phase === 'SETTLED') {
          const day = isCurrent ? this.settleCurrentDay(td, version, settlePx) : this.resettlePast(td)
          const completed: SettlementCompleted = {
            trading_day: td,
            version: day.version ?? version,
            next_trading_day: nextTd,
            accounts: day.accounts.map((s) => ({ account_id: s.account_id, close_pnl: s.close_pnl, position_pnl: s.position_pnl, fee: s.fee, balance: s.balance, equity: s.equity, margin: s.margin, available: s.available, risk_degree: s.risk_degree, margin_call: s.margin_call })),
            price_sources: { VWAP: this.inst.size },
            checks: [
              { check: 'EQUITY_IDENTITY', status: 'PASS' },
              { check: 'EXCH_FORMULA', status: 'PASS' },
              { check: 'MTM_VS_FIFO', status: 'PASS' },
              { check: 'FIRM_SUM', status: 'PASS' },
            ],
            ts_ns: this.nowNs(),
          }
          this.settledEvents.set(td, completed)
          this.publish('eod', 'event', completed)
          detail.accounts = day.accounts.length
        } else if (phase === 'ROLLED') {
          const mode = isCurrent ? 'ROLL' : 'REBASE'
          const cleared = isCurrent ? this.dayRoll(nextTd, settlePx) : 0
          for (const shard of [0, 1]) {
            const roll: DayRoll = { shard, mode, from_trading_day: isCurrent ? td : nextTd, to_trading_day: isCurrent ? nextTd : nextTd, sod_version: version, accounts: ACCOUNTS.filter((a) => a.shard === shard).map((a) => a.id), latches_cleared: shard === 1 ? cleared : 0, ts_ns: this.nowNs() }
            this.publish('eod', 'event', roll)
          }
          detail.mode = mode
        } else if (phase === 'REPORTED') {
          const rows = reportsForDay(td, version, this.rng, this.nowNs())
          for (const r of rows) {
            this.reports.set(r.report_id, r)
            this.publish('eod', 'event', r)
          }
          detail.reports = rows.length
        }
      } catch (e) {
        const failed: EodPhaseEvent = { trading_day: td, version, phase: 'FAILED', next_trading_day: nextTd, detail: {}, error: String(e), ts_ns: this.nowNs() }
        this.pushEod(failed)
        this.eodRunning = false
        return
      }
      this.pushEod({ trading_day: td, version, phase, next_trading_day: nextTd, detail, error: null, ts_ns: this.nowNs() })
      if (idx < phases.length) setTimeout(fire, 1100)
      else {
        this.eodRunning = false
        // a trade of ACC2 reaches the clearing copy after the day was settled
        if (isCurrent)
          setTimeout(
            () => this.eodWarn(td, version, 'LATE_FACT', `ACC2 的成交 T-${td}-0417 在 ${td} 结算之后到达：结算 v${version} 不含该笔（需要时用 force_new_version 重算）`, { account_id: 'ACC2', trade_id: `T-${td}-0417` }),
            2500,
          )
      }
    }
    setTimeout(fire, 50)
    return { trading_day: td, version, phase: 'BEGIN' }
  }

  private pushEod(ev: EodPhaseEvent): void {
    this.eodPhases.set(ev.trading_day, ev)
    this.eodLog.push(ev)
    if (this.eodLog.length > 100) this.eodLog.shift()
    this.publish('eod', 'event', ev)
  }

  private settleCurrentDay(td: string, version: number, settlePx: Record<string, number>): CompareHistoryDay {
    const rows = [...this.accts.values()].map((a) => {
      const s = a.snapshot as AccountSnapshot
      const pnlAdj = (s.positions ?? []).reduce((acc, p) => {
        const spec = INSTRUMENT_MAP[p.instrument_id]
        const px = settlePx[p.instrument_id]
        if (!spec || px === undefined || p.last === null || p.last === undefined) return acc
        return acc + (p.direction === 'LONG' ? 1 : -1) * (px - p.last) * spec.multiplier * (p.td_qty + p.yd_qty)
      }, 0)
      const posPnl = s.funds.position_pnl + pnlAdj
      const equity = s.funds.static_equity + s.funds.close_pnl + posPnl - s.funds.fee
      const perf = equity - a.cumNetDeposit
      const prevRow = this.history[this.history.length - 1]?.accounts.find((x) => x.account_id === a.spec.id)
      const prevMdd = prevRow ? Number(prevRow.max_drawdown) : 0
      const hwmNav = Math.max(a.hwm, perf) / a.spec.initialCapital
      const nav = perf / a.spec.initialCapital
      return {
        trading_day: td,
        account_id: a.spec.id,
        version,
        pre_balance: a.preBalance.toFixed(2),
        deposit: a.netDeposit.toFixed(2),
        withdraw: '0.00',
        close_pnl: s.funds.close_pnl.toFixed(2),
        position_pnl: posPnl.toFixed(2),
        fee: s.funds.fee.toFixed(2),
        balance: equity.toFixed(2),
        equity: equity.toFixed(2),
        margin: s.funds.margin.toFixed(2),
        available: (equity - s.funds.margin).toFixed(2),
        risk_degree: (s.funds.margin / equity).toFixed(6),
        margin_call: '0.00',
        exch_formula_pnl: (s.funds.close_pnl + posPnl).toFixed(2),
        fifo_realized_pnl: (s.funds.close_pnl * 1.05).toFixed(2),
        fifo_float_pnl: (posPnl * 0.97).toFixed(2),
        initial_capital: a.spec.initialCapital.toFixed(2),
        cum_net_deposit: a.cumNetDeposit.toFixed(2),
        hwm_equity: Math.max(a.hwm, perf).toFixed(2),
        nav: nav.toFixed(6),
        max_drawdown: Math.max(prevMdd, hwmNav > 0 ? (hwmNav - nav) / hwmNav : 0).toFixed(6),
        checks_passed: true,
      }
    })
    const breakers: Record<string, number> = {}
    for (const e of this.riskEvents) if (e.trading_day === td && e.account_id && ((e.kind === 'ALERT_RAISED' && e.action === 'CIRCUIT_BREAK') || e.kind === 'MANUAL_HALT')) breakers[e.account_id] = (breakers[e.account_id] ?? 0) + 1
    const day: CompareHistoryDay = { trading_day: td, version, accounts: rows, breakers }
    const idx = this.history.findIndex((d) => d.trading_day === td)
    if (idx >= 0) this.history[idx] = day
    else this.history.push(day)
    return day
  }

  private resettlePast(td: string): CompareHistoryDay {
    const idx = this.history.findIndex((d) => d.trading_day === td)
    const d = this.history[idx] as CompareHistoryDay
    const v = (d.version ?? 1) + 1
    const nd: CompareHistoryDay = { ...d, version: v, accounts: d.accounts.map((a) => ({ ...a, version: v })) }
    this.history[idx] = nd
    return nd
  }

  /** Day roll: pre_balance := settled equity, today's lots become yesterday's, latches cleared (reset_on_day_roll). */
  private dayRoll(nextTd: string, settlePx: Record<string, number>): number {
    let latches = 0
    const day = this.history.find((d) => d.trading_day === this.tradingDay)
    this.tradingDay = nextTd
    for (const [id, i] of this.inst) {
      const px = settlePx[id] ?? i.last
      const fresh = this.newInst(i.spec, px)
      fresh.oi = i.oi
      fresh.lastLiveMs = this.nowMs()
      this.inst.set(id, fresh)
    }
    this.shocks.length = 0
    for (const a of this.accts.values()) {
      const row = day?.accounts.find((x) => x.account_id === a.spec.id)
      a.preBalance = row ? Number(row.equity) : (a.snapshot?.funds.equity ?? a.preBalance)
      a.netDeposit = 0
      a.closePnl = 0
      a.fee = 0
      for (const p of a.positions.values()) {
        p.yd += p.td
        p.ydOpenCost += p.tdCost
        p.td = 0
        p.tdCost = 0
        p.pendingClose = 0
      }
      a.hwm = row ? Number(row.hwm_equity) : a.hwm
      a.peak = a.preBalance - a.cumNetDeposit
      a.orders.clear()
      a.trades = []
      a.ordersToday = 0
      a.cancelsToday = 0
      a.openVol = {}
      for (const sm of a.sm.values()) if (sm.latched && sm.state === 'ACTIVE') latches += 1
      if (a.manualHalt) latches += 1
      a.sm.clear()
      a.manualHalt = false
      a.points = []
      a.tradingMode = 'OPEN'
      a.pendingCause = { cause: 'DAYROLL' }
      this.revalue(a, this.nowMs())
    }
    return latches
  }

  // ---------------------------------------------------------------- backtests

  private tickBacktests(): void {
    for (const b of this.backtests.values()) {
      if (b.state === 'QUEUED' && this.nowNs() - b.submitted_ns > 2e9) {
        b.state = 'RUNNING'
        b.started_ns = this.nowNs()
        this.publishBacktest(b)
      } else if (b.state === 'RUNNING') {
        // WS `backtests` (state topic, 1 Hz): progress upserts
        if (this.stepCount % 2 === 0) this.publishBacktest(b)
        b.progress = Math.min(1, (b.progress ?? 0) + (b.run_id === 'bt-0192c3' ? 0.002 : 0.02))
        if ((b.progress ?? 0) >= 1) {
          b.state = 'DONE'
          b.finished_ns = this.nowNs()
          const ret = Number(this.rng.range(-0.02, 0.06).toFixed(4))
          const summary = {
            baseline: 'no_rules',
            days: 244,
            no_rules: { total_ret: ret, max_dd: Number(this.rng.range(0.03, 0.08).toFixed(4)), worst_day: -0.0229, latched_days: 0, blocked_opens: 0 },
            block_open: { total_ret: Number((ret + this.rng.range(-0.002, 0.004)).toFixed(4)), max_dd: Number(this.rng.range(0.03, 0.08).toFixed(4)), worst_day: -0.0224, latched_days: this.rng.int(2, 9), blocked_opens: this.rng.int(0, 5), rule_days: this.rng.int(4, 20), effect_sum: Number(this.rng.range(-0.002, 0.006).toFixed(4)), next_effect_sum: Number(this.rng.range(-0.004, 0.004).toFixed(4)) },
          }
          b.summary = summary
          const r = this.newReport(null, 'BACKTEST_HTML', null, b.run_id, this.nowNs())
          b.report_id = r.report_id
          this.backtestSummaries.set(r.report_id, summary)
          this.publish('eod', 'event', r)
          this.publishBacktest(b)
        }
      }
    }
  }

  private publishBacktest(b: BacktestStatus): void {
    this.publish('backtests', 'upsert', wire('BacktestStatus', { ...b }))
  }

  /** WS `backtests` snapshot: every run, newest first. */
  private backtestsTopic(): BacktestStatus[] {
    return wireList('BacktestStatus', [...this.backtests.values()].sort((a, b) => b.submitted_ns - a.submitted_ns).map((b) => ({ ...b })))
  }

  private newReport(td: string | null, type: ReportReady['type'], account: string | null, runId: string, created: number): ReportReady {
    const r: ReportReady = { report_id: this.rng.uuid(created / 1e6), trading_day: td, type, account_id: account, path: reportPath(td, type, account, runId), sha256: Array.from({ length: 64 }, () => Math.floor(this.rng.next() * 16).toString(16)).join(''), size_bytes: 24_000 + this.rng.int(0, 9000), version: null, created_ns: created }
    this.reports.set(r.report_id, r)
    return r
  }

  /** Settlement, reports and backtests are answered by the clearing-worker. */
  private clearing(): void {
    if (!this.svcUp('clearing-worker')) throw peerDown('clearing-worker')
  }

  runBacktest(configName: string): BacktestStatus {
    if (!configName) throw new MockError('BAD_REQUEST', 'config_name: String should have at least 1 character', 400)
    this.clearing()
    const b: BacktestStatus = { run_id: `bt-${this.rng.uuid(this.nowMs()).slice(0, 8)}`, name: configName, state: 'QUEUED', progress: 0, submitted_ns: this.nowNs() }
    this.backtests.set(b.run_id, b)
    this.publishBacktest(b)
    return wire('BacktestStatus', { ...b })
  }

  listBacktests(): { items: BacktestStatus[] } {
    this.clearing()
    return { items: wireList('BacktestStatus', [...this.backtests.values()].map((b) => ({ ...b }))) }
  }

  getBacktest(id: string): BacktestStatus {
    this.clearing()
    const b = this.backtests.get(id)
    if (!b) throw new MockError('NOT_FOUND', `backtest ${id} not found`, 404)
    return wire('BacktestStatus', { ...b })
  }

  // ---------------------------------------------------------------- commands

  private acct(id: string): AcctState {
    const a = this.accts.get(id)
    if (!a) throw new MockError('NOT_FOUND', `unknown account '${id}'`, 404)
    return a
  }

  /** Commands go to the owning shard: UNAVAILABLE while it is down. */
  private owned(id: string): AcctState {
    const a = this.acct(id)
    if (!this.shardUp(a.spec.shard)) throw peerDown(`risk-engine-${a.spec.shard}`)
    return a
  }

  /**
   * `risk.reset` (engine._cmd_reset): latched ACTIVE levels (highest first) become REARMED
   * with a rearm line, a manual halt is released; OK with "N latched level(s) reset" —
   * N = 0 when nothing was latched.
   */
  reset(id: string, reason: string, ruleId: string | undefined, level: string | undefined, actor: string): MockResult<ResetAck> {
    const a = this.owned(id)
    if (!reason.trim()) throw new MockError('BAD_REQUEST', 'reason must not be empty', 400)
    let rearm: number | null = null
    let n = 0
    const scope = `acct:${id}`
    if (a.manualHalt && (!ruleId || ruleId === 'manual_halt') && (!level || level === 'halt')) {
      a.manualHalt = false
      const ep = this.manualEpisode.get(id) ?? 1
      this.riskEvent({ kind: 'RESET', account_id: id, scope_key: scope, alert_key: `${this.tradingDay}|manual_halt|halt|${scope}|${ep}`, rule_id: 'manual_halt', level: 'halt', episode: ep, action: 'CIRCUIT_BREAK', severity: 'HIGH', actor, message: `${id} manual_halt/halt 已人工重置：${reason}`, detail: { rearm_threshold: null, reason } })
      n += 1
    }
    for (const [key, sm] of a.sm) {
      const [rid, smScope, lvName] = key.split('|') as [string, string, string]
      if (!sm.latched || sm.state !== 'ACTIVE') continue
      if (ruleId && rid !== ruleId) continue
      if (level && lvName !== level) continue
      const def = RULES.find((r) => r.id === rid)
      const lv = def ? effectiveLevels(def, id).find((l) => l.name === lvName) : undefined
      const value = a.snapshot?.rules?.find((r) => r.rule_id === rid && r.scope_key === smScope)?.value ?? null
      const extra = lv?.rearm?.extra ?? 0.01
      let line: number | null = null
      if (lv && value !== null) line = def && (def.op === '<=' || def.op === '<') ? Math.min(lv.threshold, value - extra) : Math.max(lv.threshold, value + extra)
      if (rearm === null) rearm = line
      sm.state = 'REARMED'
      sm.latched = false
      sm.rearm = line
      n += 1
      this.riskEvent({ kind: 'RESET', account_id: id, scope_key: smScope, alert_key: sm.alertKey, rule_id: rid, level: lvName, episode: sm.episode, action: lv?.action ?? null, severity: 'HIGH', actor, message: `${id} ${rid}/${lvName} 已人工重置：${reason}`, detail: { rearm_threshold: line, reason } })
    }
    a.pendingCause = { cause: 'RESET', actor, reason }
    this.aggregateState(a, this.nowMs())
    return new MockResult(wire('ResetAck', { account_state: a.state, rearm_threshold: rearm }), `${n} latched level(s) reset`)
  }

  /** `risk.halt` (engine._cmd_halt): idempotent — an already latched manual halt answers the current state. */
  halt(id: string, reason: string, actor: string): AccountStateChanged {
    const a = this.owned(id)
    if (!reason.trim()) throw new MockError('BAD_REQUEST', 'reason: String should have at least 1 character', 400)
    if (!a.manualHalt) {
      a.manualHalt = true
      const ep = (this.manualEpisode.get(id) ?? 0) + 1
      this.manualEpisode.set(id, ep)
      const scope = `acct:${id}`
      this.riskEvent({ kind: 'MANUAL_HALT', account_id: id, scope_key: scope, alert_key: `${this.tradingDay}|manual_halt|halt|${scope}|${ep}`, rule_id: 'manual_halt', level: 'halt', episode: ep, severity: 'CRITICAL', action: 'CIRCUIT_BREAK', latched: true, actor, message: `${id} 人工熔断：${reason}` })
      a.pendingCause = { cause: 'MANUAL', actor, reason }
      this.aggregateState(a, this.nowMs())
    }
    const cur = a.lastChange
    if (cur && cur.to_state === a.state) return wire('AccountStateChanged', cur)
    return wire('AccountStateChanged', { account_id: id, trading_day: this.tradingDay, from_state: a.state, to_state: a.state, halted_latched: a.latched, state_version: a.stateVersion, restrictions: a.restrictions, cause: 'MANUAL', cause_alert_key: null, actor, reason, ts_ns: this.nowNs() })
  }

  /** FirmControl of a manual kill-switch command as one shard answers it. */
  private firmControl(on: boolean, reason: string, actor: string): FirmControl {
    return wire('FirmControl', { action: on ? 'KILL_SWITCH_ON' : 'KILL_SWITCH_OFF', origin: 'MANUAL', active: on, restrictions: [], trading_day: this.tradingDay, actor, reason, cause_alert_key: null, ts_ns: this.nowNs() })
  }

  /** A shard applying the manual kill switch emits its own KILL_SWITCH_* event (engine._apply_control). */
  private applyKill(on: boolean, reason: string, actor: string): void {
    this.killManual = on
    this.riskEvent({ kind: on ? 'KILL_SWITCH_ON' : 'KILL_SWITCH_OFF', account_id: null, scope_key: 'firm', severity: on ? 'CRITICAL' : 'HIGH', actor, message: `kill switch ${on ? '开启' : '关闭'}（MANUAL）：${reason}；生效=${on}`, detail: { origin: 'MANUAL', effective: on, manual: on, rule: false } })
  }

  /** The MANUAL FirmControl reaches the read model (`risk.control.firm`) and the WS `firm` topic. */
  private setManual(on: boolean, reason: string, actor: string): void {
    const c = this.firmControl(on, reason, actor)
    this.killManualInfo = { actor, reason, ts_ns: c.ts_ns }
    this.publish('firm', 'event', c)
  }

  /**
   * `POST /risk/eod/{td}/force-unfreeze` (engine._cmd_force_unfreeze on every shard): only
   * a PROVISIONAL shard lifts EOD_NOT_SETTLED (others answer PRECONDITION_FAILED); HTTP 200
   * with the per-target results, ok=false with the worst code.
   */
  forceUnfreeze(td: string, reason: string, actor: string): MockResult<{ targets: TargetResult[]; all_ok: boolean }> {
    if (!/^\d{8}$/.test(td)) throw new MockError('BAD_REQUEST', 'td must be YYYYMMDD', 400)
    if (!reason.trim()) throw new MockError('BAD_REQUEST', 'reason: String should have at least 1 character', 400)
    const targets: TargetResult[] = RISK_INSTANCES.map((t) => {
      if (!this.svcUp(t)) return { target: t, ok: false, code: 'UNAVAILABLE', message: `${t}: peer is down (no heartbeat)`, duplicate: false, data: null }
      const shard = Number(t.slice(-1))
      const accts = [...this.accts.values()].filter((a) => a.spec.shard === shard)
      if (!accts.some((a) => a.tradingMode === 'PROVISIONAL')) return { target: t, ok: false, code: 'PRECONDITION_FAILED', message: 'only valid in PROVISIONAL', duplicate: false, data: null }
      for (const a of accts) a.tradingMode = 'OPEN'
      this.riskEvent({ kind: 'EOD_FORCE_UNFREEZE', account_id: null, scope_key: null, severity: 'HIGH', actor, message: `EOD 未完成，${actor} 审计放行临时快照上的开仓：${reason}`, detail: { trading_day: td } })
      return { target: t, ok: true, code: 'OK', message: '', duplicate: false, data: null }
    })
    const failed = targets.filter((t) => !t.ok)
    if (!failed.length) return new MockResult({ targets, all_ok: true }, `${targets.length}/${targets.length} targets confirmed`, true, 'OK', 200)
    const worst = failed.some((t) => t.code === 'PRECONDITION_FAILED') ? 'PRECONDITION_FAILED' : (failed[0]?.code ?? 'UNAVAILABLE')
    return new MockResult({ targets, all_ok: false }, `${targets.length - failed.length}/${targets.length} targets confirmed; failed: ${failed.map((t) => `${t.target}=${t.code}`).join(', ')}`, false, worst, 200)
  }

  /** `ReadModel.firm_control_state`: kill switch = MANUAL OR RULE, with both layers. */
  firmControlState(): FirmControlState {
    const m = this.killManualInfo
    const manual = m ? { active: this.killManual, actor: m.actor, reason: m.reason, ts_ns: m.ts_ns } : null
    const rule = { active: false, restrictions: [], ts_ns: this.ruleLayerNs, reason: '', cause_alert_key: null }
    return { kill_switch: Boolean(manual?.active) || rule.active, manual, rule }
  }

  /** WS `firm` snapshot: the last FirmSnapshot plus the control layers. */
  private firmTopic(): FirmTopicSnapshot | null {
    return this.lastFirm ? { ...this.lastFirm, control: this.firmControlState() } : null
  }

  private summarizeKill(row: KillSwitchRow): KillSwitchRow {
    const names = Object.keys(row.targets)
    const by = (st: string) => names.filter((t) => row.targets[t]?.status === st)
    return { ...row, pending: by('PENDING'), failed: by('FAILED'), confirmed: by('CONFIRMED') }
  }

  private killDict(): KillSwitchRow | null {
    const r = this.killRow
    return r ? { ...r, targets: Object.fromEntries(Object.entries(r.targets).map(([k, v]) => [k, { ...v }])), pending: [...r.pending], failed: [...r.failed], confirmed: [...r.confirmed] } : null
  }

  private publishKillEvent(event: string): void {
    const row = this.killDict()
    if (row) this.publish('system', 'event', { kind: 'kill_switch', event, ...row })
  }

  /**
   * `POST /risk/firm/kill-switch` (KillSwitchManager.command): the new desired state
   * supersedes every pending retry; fan-out to every risk shard and the trade-gateway. A
   * target that is down stays PENDING and is retried every 2 s (FAILED after 30 attempts).
   * Always HTTP 200: ok=false with the worst target code on a partial failure.
   */
  killSwitch(on: boolean, reason: string, actor: string, commandId: string): MockResult<KillSwitchResult> {
    if (!reason.trim()) throw new MockError('BAD_REQUEST', 'reason: String should have at least 1 character', 400)
    const now = this.nowNs()
    const row: KillSwitchRow = { desired_on: on, command_id: commandId, reason, actor, role: 'ADMIN', requested_ns: now, updated_ns: now, targets: {}, pending: [], failed: [], confirmed: [] }
    const targets: TargetResult<FirmControl>[] = [...RISK_INSTANCES, 'trade-gateway'].map((t) => {
      const up = this.svcUp(t)
      const message = up ? '' : `${t}: peer is down (no heartbeat)`
      row.targets[t] = { status: up ? 'CONFIRMED' : 'PENDING', code: up ? 'OK' : 'UNAVAILABLE', message, attempts: 1, updated_ns: now }
      if (!up) return { target: t, ok: false, code: 'UNAVAILABLE', message, duplicate: false, data: null }
      if (t.startsWith('risk-engine')) this.applyKill(on, reason, actor)
      return { target: t, ok: true, code: 'OK', message: '', duplicate: false, data: this.firmControl(on, reason, actor) }
    })
    // a new decision supersedes every pending retry
    if (this.killRetryTimer) clearTimeout(this.killRetryTimer)
    this.killRetryTimer = null
    this.killRow = this.summarizeKill(row)
    if (this.killRow.pending.length) this.killRetryTimer = setTimeout(() => this.retryKillSwitch(), KILL_RETRY_MS)
    if (targets.some((t) => t.ok && t.target.startsWith('risk-engine'))) this.setManual(on, reason, actor)
    this.publishKillEvent('command')
    const failed = targets.filter((t) => !t.ok)
    const control = targets.find((t) => t.ok)?.data ?? null
    const data: KillSwitchResult = { targets, all_ok: failed.length === 0, control, retrying: [...this.killRow.pending], failed: [...this.killRow.failed] }
    if (!failed.length) return new MockResult(data, `${targets.length}/${targets.length} targets confirmed`, true, 'OK', 200)
    const names = failed.map((t) => `${t.target}=${t.code}`).join(', ')
    return new MockResult(data, `${targets.length - failed.length}/${targets.length} targets confirmed; failed: ${names}`, false, failed[0]?.code ?? 'UNAVAILABLE', 200)
  }

  /** KillSwitchManager retry task: every 2 s the PENDING targets get the current desired state. */
  private retryKillSwitch(): void {
    this.killRetryTimer = null
    const row = this.killRow
    if (!row || !row.pending.length) return
    const now = this.nowNs()
    for (const t of row.pending) {
      const st = row.targets[t]
      if (!st) continue
      st.attempts += 1
      st.updated_ns = now
      if (this.svcUp(t)) {
        Object.assign(st, { status: 'CONFIRMED', code: 'OK', message: '' })
        if (t.startsWith('risk-engine')) {
          this.applyKill(row.desired_on, row.reason, row.actor)
          if (!this.killManualInfo || this.killManualInfo.ts_ns < row.requested_ns) this.setManual(row.desired_on, row.reason, row.actor)
        }
      } else if (st.attempts >= KILL_MAX_ATTEMPTS) {
        Object.assign(st, { status: 'FAILED', code: 'UNAVAILABLE', message: `${t}: peer is down (no heartbeat)` })
      }
    }
    row.updated_ns = now
    this.killRow = this.summarizeKill(row)
    this.publishKillEvent('retry')
    if (this.killRow.pending.length) this.killRetryTimer = setTimeout(() => this.retryKillSwitch(), KILL_RETRY_MS)
  }

  /** `/system/status.kill_switch_retry` (Gateway.kill_switch_retry). */
  private killRetryState(): KillSwitchRetry {
    const r = this.killRow
    if (!r) return { pending: [], failed: [], desired: null, updated_ns: null, retrying: false }
    return { pending: [...r.pending], failed: [...r.failed], desired: { on: r.desired_on, reason: r.reason, actor: r.actor, command_id: r.command_id }, updated_ns: r.updated_ns, retrying: this.killRetryTimer !== null }
  }

  /** `/system/status.kill_switch_control` (KillSwitchManager.status). */
  private killControlState(): KillSwitchControl | null {
    const r = this.killDict()
    return r ? { ...r, retrying: this.killRetryTimer !== null } : null
  }

  /** `risk.ack_alert`: acknowledgement only; ALERT_ACKED once (detail {note}). */
  ackAlert(alertKey: string, note: string | undefined, actor: string): void {
    const ev = [...this.riskEvents].reverse().find((e) => e.alert_key === alertKey && (e.kind === 'ALERT_RAISED' || e.kind === 'MANUAL_HALT'))
    if (!ev) throw new MockError('NOT_FOUND', `alert ${alertKey} not found`, 404)
    if (ev.account_id) {
      const a = this.accts.get(ev.account_id)
      if (a && !this.shardUp(a.spec.shard)) throw peerDown(`risk-engine-${a.spec.shard}`)
    }
    if (this.acked.has(alertKey)) return
    this.acked.add(alertKey)
    this.riskEvent({ kind: 'ALERT_ACKED', account_id: ev.account_id ?? null, scope_key: ev.scope_key ?? null, alert_key: alertKey, rule_id: ev.rule_id ?? null, level: ev.level ?? null, episode: ev.episode ?? null, action: ev.action ?? null, severity: 'INFO', actor, message: `${alertKey} 已确认`, detail: { note: note ?? '' } })
  }

  /** `risk.liquidation.confirm`: no event; CONFLICT with the current plan when it is no longer pending. */
  confirmPlan(planId: string, decision: string, actor: string): LiquidationPlan {
    const p = this.plans.get(planId)
    if (!p) throw new MockError('NOT_FOUND', `unknown liquidation plan ${planId}`, 404)
    const a = this.accts.get(p.account_id)
    if (a && !this.shardUp(a.spec.shard)) throw peerDown(`risk-engine-${a.spec.shard}`)
    if (decision !== 'CONFIRM' && decision !== 'CANCEL') throw new MockError('BAD_REQUEST', "decision: Input should be 'CONFIRM' or 'CANCEL'", 400)
    if (p.status !== 'PENDING_CONFIRM') throw new MockError('CONFLICT', `plan is ${p.status}`, 409, wire('LiquidationPlan', p))
    p.status = decision === 'CONFIRM' ? 'CONFIRMED' : 'CANCELED'
    p.confirmed_by = actor
    p.confirmed_ns = this.nowNs()
    // engine._plan_event: LIQUIDATION_PLAN with detail {plan_id, status, actor}
    this.riskEvent({
      kind: 'LIQUIDATION_PLAN',
      account_id: p.account_id,
      scope_key: `acct:${p.account_id}`,
      alert_key: p.trigger_alert_key,
      action: 'LIQUIDATE',
      actor,
      severity: p.status === 'CONFIRMED' ? 'WARNING' : 'INFO',
      message: `${p.account_id} 减仓计划 ${p.plan_id} ${p.status === 'CONFIRMED' ? '已确认' : '已取消'}（${actor}）`,
      detail: { plan_id: p.plan_id, status: p.status, actor },
    })
    return wire('LiquidationPlan', { ...p })
  }

  /**
   * `GET /risk/liquidation-plans?account=&status=&td=`: every shard (or the owner) is
   * asked; the plans of a shard that does not answer come from the read model, `stale`.
   */
  liquidationPlans(account?: string, status?: string, td?: string): MockResult<LiquidationPlansResponse> {
    const statuses = ['PENDING_CONFIRM', 'CONFIRMED', 'CANCELED', 'EXPIRED']
    if (status && !statuses.includes(status)) throw new MockError('BAD_REQUEST', `status must be one of ${statuses.join(', ')}`, 400)
    const targets = account !== undefined ? [`risk-engine-${this.acct(account).spec.shard}`] : RISK_INSTANCES
    const errors: Record<string, string> = {}
    for (const t of targets) if (!this.svcUp(t)) errors[t] = 'UNAVAILABLE'
    const day = td ?? this.tradingDay
    const ownerOf = (p: LiquidationPlan) => `risk-engine-${this.accts.get(p.account_id)?.spec.shard ?? 0}`
    const items: LiquidationPlanView[] = [...this.plans.values()]
      .filter((p) => p.trading_day === day && targets.includes(ownerOf(p)) && (!account || p.account_id === account) && (!status || p.status === status))
      .sort((a, b) => a.created_ns - b.created_ns)
      .map((p) => {
        const v: LiquidationPlanView = wire('LiquidationPlan', { ...p })
        return errors[ownerOf(p)] ? { ...v, stale: true } : v
      })
    const msg = Object.keys(errors).length ? `partial: ${Object.entries(errors).map(([k, v]) => `${k}=${v}`).join(', ')}` : ''
    return new MockResult({ trading_day: day, items, errors }, msg)
  }

  private simUpOrThrow(): void {
    if (!this.svcUp('sim-exchange')) throw new MockError('UNAVAILABLE', SIM_DOWN_MESSAGE, 503)
  }

  /** `GET /demo/state` (sim GET /control/state; the fields the dashboard reads + a few more). */
  demoState(): DemoState {
    this.simUpOrThrow()
    const clock = this.clockState()
    return {
      epoch: this.sources[0]?.epoch ?? '',
      trading_day: this.tradingDay,
      faults: [...this.faults.values()].map((f) => this.simFault(f, 'ACTIVE')),
      fault_history: [...this.faultHistory],
      scenario: this.scenarioRun ? { ...this.scenarioRun, log: [...this.scenarioRun.log] } : null,
      prices: Object.fromEntries([...this.inst.values()].map((i) => [i.spec.id, i.exchPx])),
      paused: false,
      market_now_ns: this.nowNs(),
      clock: { mode: clock.mode, offset_ns: clock.offset_ns, version: clock.version },
      shocks: this.shocks.map((x) => ({ shock_id: x.id, targets: [...x.targets], pct: x.pct, shape: x.shape, duration_s: x.durationS })),
    }
  }

  /** `GET /demo/faults` → `{items, history}`. */
  demoFaults(): DemoFaultsResponse {
    const st = this.demoState()
    return { items: st.faults, history: st.fault_history }
  }

  /** `GET /demo/scenarios` → `[{name, description}]`. */
  demoScenarios(): DemoScenarioInfo[] {
    this.simUpOrThrow()
    return SCENARIOS.map((x) => ({ name: x.name, description: x.description }))
  }

  private simFault(f: Fault, state: string): SimFault {
    return {
      fault_id: f.fault_id,
      type: f.type,
      target: f.target,
      params: f.params,
      duration_s: f.durationS,
      state,
      created_ns: f.createdMs * 1e6,
      remaining_s: state === 'ACTIVE' && f.untilMs !== null ? Math.max(0, (f.untilMs - this.nowMs()) / 1000) : null,
      detail: {},
    }
  }

  /** Sim `POST /control/faults` rules (sim/exchange/faults.py `_validate`). */
  injectFault(type: string, target: string | undefined, params: Record<string, unknown>, durationS: number | undefined): DemoFaultResult {
    this.simUpOrThrow()
    const bad = (m: string) => new MockError('BAD_REQUEST', m, 400)
    const num = (k: string, dflt?: number, lo?: number, hi?: number): number => {
      const v = params[k]
      if (v === undefined || v === null) {
        if (dflt === undefined) throw bad(`params.${k} is required`)
        return dflt
      }
      if (typeof v !== 'number' || !Number.isFinite(v)) throw bad(`params.${k} must be a number`)
      if (lo !== undefined && v < lo) throw bad(`params.${k} must be >= ${lo}`)
      if (hi !== undefined && v > hi) throw bad(`params.${k} must be <= ${hi}`)
      return v
    }
    const tgt = (allowed: (string | null)[], dflt: string | null): string | null => {
      const t = target ?? dflt
      if (!allowed.includes(t)) throw bad(t === null ? 'target is required' : `target must be one of ${allowed.filter(Boolean).join(', ') || 'none'}`)
      return t
    }
    const instrument = (k: string): string => {
      const v = params[k]
      if (typeof v !== 'string' || !v) throw bad(`params.${k} is required`)
      if (!this.inst.has(v)) throw bad(`unknown instrument ${v}`)
      return v
    }
    const now = this.nowMs()
    const id = this.rng.uuid(now)
    let t: string | null = null
    let clean: Record<string, unknown> = {}
    let lifetime: number | null = durationS ?? null
    switch (type) {
      case 'FEED_DISCONNECT': {
        t = tgt(['feed_a', 'feed_b'], null)
        const refuse = num('refuse_reconnect_s', 0, 0)
        const mode = typeof params.mode === 'string' ? params.mode : 'close'
        if (mode !== 'close' && mode !== 'silent') throw bad('params.mode must be close or silent')
        if (mode === 'silent' && t !== 'feed_a') throw bad('mode=silent only applies to feed_a')
        clean = { refuse_reconnect_s: refuse, mode }
        lifetime = durationS ?? (refuse > 0 ? refuse : t === 'feed_a' && mode === 'close' ? 0 : null)
        if (lifetime === 0) lifetime = 1
        break
      }
      case 'FEED_GAP': {
        t = tgt(['feed_a'], 'feed_a')
        const n = Math.round(num('drop_count', undefined, 1))
        clean = { drop_count: n }
        const s = this.sources[0] as SrcState
        this.openAndFillGap(s, s.lastSeq + 1, s.lastSeq + n, now)
        lifetime = durationS ?? 2
        break
      }
      case 'FEED_DUPLICATE':
        t = tgt(['feed_a', 'feed_b'], 'feed_a')
        clean = { ratio: num('ratio', undefined, 0, 1) }
        break
      case 'FEED_REORDER':
        t = tgt(['feed_a', 'feed_b'], 'feed_a')
        clean = { window: Math.round(num('window', undefined, 2)) }
        break
      case 'FEED_LATENCY':
        t = tgt(['feed_a', 'feed_b'], 'feed_a')
        clean = { ms: num('ms', undefined, 0) }
        break
      case 'FEED_FREEZE':
        t = tgt([null, 'feed_a', 'feed_b'], null)
        clean = { instrument: instrument('instrument') }
        break
      case 'BAD_TICK': {
        t = tgt([null, 'feed_a', 'feed_b'], null)
        const inst = instrument('instrument')
        const off = num('offset_pct', undefined, -0.99, 10)
        if (off === 0) throw bad('params.offset_pct must be non-zero (ratio, e.g. 0.05 for +5%)')
        if (params.with_volume !== undefined && typeof params.with_volume !== 'boolean') throw bad('params.with_volume must be a boolean')
        clean = { instrument: inst, offset_pct: off, with_volume: Boolean(params.with_volume) }
        const i = this.inst.get(inst)
        const src: SourceId = t === 'feed_b' ? 'FEED_B' : 'FEED_A'
        this.mdEvent('BAD_TICK', { source: src, instrument_id: inst, detail: { reason: 'MD_QUARANTINE_SPIKE', price: i ? roundTick(i.last * (1 + off), i.spec.tick) : null, offset_pct: off, with_volume: Boolean(params.with_volume) } })
        setTimeout(() => this.mdEvent('QUARANTINE_DROPPED', { source: src, instrument_id: inst, detail: { reason: 'NOT_CONFIRMED_2S' } }), 2000)
        lifetime = durationS ?? 2.5
        break
      }
      case 'MALFORMED': {
        t = tgt(['feed_b'], 'feed_b')
        const mode = typeof params.mode === 'string' ? params.mode : 'body'
        if (mode !== 'body' && mode !== 'record') throw bad('params.mode must be body or record')
        clean = { ratio: num('ratio', undefined, 0, 1), mode }
        break
      }
      case 'SIM_RESTART': {
        tgt([null], null)
        lifetime = 0
        const s = this.sources[0] as SrcState
        s.epoch = `e${this.tradingDay}${String.fromCharCode(98 + this.rng.int(0, 20))}`
        const sim = this.svcs.get('sim-exchange')
        if (sim) {
          this.svcEvent(sim, 'EXITED', 'KILLED_BY_DEMO', 0, 'demo')
          setTimeout(() => this.svcEvent(sim, 'RESTARTING', 'EXITED'), 500)
          setTimeout(() => {
            sim.restarts += 1
            sim.hbState = 'RUNNING'
            this.svcEvent(sim, 'RUNNING', 'STARTUP')
          }, 3000)
        }
        break
      }
      case 'BROKER_FORCE_CLOSE': {
        tgt([null], null)
        const acct = params.account_id
        if (typeof acct !== 'string' || !acct) throw bad('params.account_id is required')
        const inst = instrument('instrument_id')
        const qty = Math.round(num('qty', 0, 0))
        clean = { account_id: acct, instrument_id: inst, qty: qty || null, posi_direction: null }
        lifetime = 0
        break
      }
      case 'BROKER_DISCONNECT': {
        tgt([null], null)
        const secs = num('seconds', undefined, 0)
        if (secs <= 0) throw bad('params.seconds must be > 0')
        clean = { seconds: secs }
        lifetime = durationS ?? secs
        break
      }
      default:
        throw bad(`type: unsupported fault type ${type}`)
    }
    const f: Fault = { fault_id: id, type, target: t, params: clean, untilMs: lifetime === null ? null : now + lifetime * 1000, createdMs: now, durationS: durationS ?? null }
    const state = lifetime === 0 ? 'DONE' : 'ACTIVE'
    if (state === 'ACTIVE') this.faults.set(id, f)
    else this.endFault(f, 'DONE')
    return { fault_id: id, state, fault: this.simFault(f, state) }
  }

  deleteFault(id: string): { ok: boolean } {
    this.simUpOrThrow()
    const f = this.faults.get(id)
    if (!f) throw new MockError('NOT_FOUND', `no active fault ${id}`, 404)
    this.faults.delete(id)
    this.endFault(f, 'DELETED')
    return { ok: true }
  }

  activeFaults(): Fault[] {
    return [...this.faults.values()]
  }

  /** Sim `POST /control/shock` → `{shock_id, shock}`. */
  shock(instruments: string[] | undefined, sector: string | undefined, pct: number, durationS: number, shape: 'STEP' | 'LINEAR'): DemoShockResult {
    this.simUpOrThrow()
    if (!Number.isFinite(pct) || pct === 0 || Math.abs(pct) > 0.2) throw new MockError('BAD_REQUEST', 'pct must be non-zero and within ±0.2', 400)
    const targets = new Set<string>()
    for (const id of instruments ?? []) {
      if (!this.inst.has(id)) throw new MockError('BAD_REQUEST', `unknown instrument ${id}`, 400)
      targets.add(id)
    }
    if (sector) for (const i of INSTRUMENTS) if (i.sector === sector) targets.add(i.id)
    if (!targets.size) throw new MockError('BAD_REQUEST', 'instruments or sector is required', 400)
    const id = this.rng.uuid(this.nowMs())
    this.shocks.push({ id, targets, pct, startMs: this.nowMs(), durationS, shape })
    const steps = shape === 'STEP' || durationS <= 0 ? 1 : Math.max(1, Math.round(durationS * 2))
    return {
      shock_id: id,
      shock: { shock_id: id, targets: [...targets], pct, shape, duration_s: durationS, n_steps: steps, remaining_steps: Object.fromEntries([...targets].map((x) => [x, steps])), done: false, created_ns: this.nowNs() },
    }
  }

  /**
   * Sim `POST /control/scenario` (ScenarioRunner.start) → `{run_id, name}`: one run at a
   * time (CONFLICT while one is RUNNING); `speed` divides the step times and durations.
   */
  scenario(name: string, speed = 1): DemoScenarioResult {
    this.simUpOrThrow()
    if (!(speed > 0)) throw new MockError('BAD_REQUEST', 'speed must be > 0', 400)
    const cur = this.scenarioRun
    if (cur && cur.status === 'RUNNING') throw new MockError('CONFLICT', `scenario '${cur.name}' (${cur.run_id}) is still running`, 409)
    const def = SCENARIOS.find((x) => x.name === name)
    if (!def) throw new MockError('NOT_FOUND', `scenario '${name}' not found in ['config/scenarios']`, 404)
    const run: ScenarioRunState = { run_id: this.rng.uuid(this.nowMs()), name, status: 'RUNNING', speed, step: 0, steps_total: def.steps.length, started_ns: this.nowNs(), error: null, vars: {}, faults: [], shocks: [], log: [] }
    this.scenarioRun = run
    this.scenarioTimers = def.steps.map((st, i) =>
      setTimeout(() => {
        if (this.scenarioRun !== run || run.status !== 'RUNNING') return
        run.step = i + 1
        const entry: Record<string, unknown> = { step: run.step, action: st.action, at_s: st.at_s }
        if (st.note) entry.note = st.note
        try {
          const p = st.params ?? {}
          if (st.action === 'shock') {
            const r = this.shock(undefined, String(p.sector), Number(p.pct), Number(p.duration_s ?? 0) / speed, p.shape === 'STEP' ? 'STEP' : 'LINEAR')
            run.shocks.push(r.shock_id)
            entry.shock_id = r.shock_id
          } else if (st.action === 'fault') {
            const fp = { ...((p.params as Record<string, unknown>) ?? {}) }
            if (typeof fp.refuse_reconnect_s === 'number') fp.refuse_reconnect_s = fp.refuse_reconnect_s / speed
            const r = this.injectFault(String(p.type), typeof p.target === 'string' ? p.target : undefined, fp, typeof p.duration_s === 'number' ? p.duration_s / speed : undefined)
            run.faults.push(r.fault_id)
            entry.fault_id = r.fault_id
          }
        } catch (e) {
          run.status = 'FAILED'
          run.error = e instanceof Error ? e.message : String(e)
        }
        run.log.push(entry)
        if (run.log.length > 20) run.log.shift()
        if (run.status === 'RUNNING' && run.step >= run.steps_total) run.status = 'DONE'
      }, (st.at_s / speed) * 1000 + 20),
    )
    return { run_id: run.run_id, name }
  }

  /** Sim `DELETE /control/scenario`: cancel the running scenario → the run (NOT_FOUND before any). */
  cancelScenario(): ScenarioRunState {
    this.simUpOrThrow()
    const run = this.scenarioRun
    if (!run) throw new MockError('NOT_FOUND', 'no scenario has been started', 404)
    if (run.status === 'RUNNING') {
      for (const t of this.scenarioTimers) clearTimeout(t)
      this.scenarioTimers = []
      run.status = 'CANCELED'
    }
    return { ...run, log: [...run.log] }
  }

  // ---------------------------------------------------------------- views

  /** Owner liveness of an account (Gateway.account_liveness): shard not UP, ms since it was last heard. */
  private liveOf(a: AcctState): { stale: boolean; age_ms: number | null } {
    return this.instanceLive(`risk-engine-${a.spec.shard}`)
  }

  /** `{stale, age_ms}` of an instance (Gateway.instance_liveness). */
  private instanceLive(instance: string): { stale: boolean; age_ms: number | null } {
    const s = this.svcs.get(instance)
    if (!s) return { stale: false, age_ms: null }
    const age = s.lastHbNs !== null ? Math.max(0, Math.round((this.nowNs() - s.lastHbNs) / 1e6)) : null
    return { stale: s.processState !== 'RUNNING', age_ms: age }
  }

  /** WS `accounts` item (readmodel.account_summary + name + owner liveness). */
  summaryOf(a: AcctState): AccountSummary {
    const s = a.snapshot as AccountSnapshot
    let worst: number | null = null
    let rule: string | null = null
    for (const r of s.rules ?? []) if (typeof r.utilization === 'number' && (worst === null || r.utilization > worst)) [worst, rule] = [r.utilization, r.rule_id]
    return {
      account_id: a.spec.id,
      shard: a.spec.shard,
      state: s.state,
      halted_latched: Boolean(s.halted_latched),
      equity: s.funds.equity,
      daily_pnl: s.metrics.daily_pnl,
      daily_pnl_pct_init: s.metrics.daily_pnl_pct_init,
      risk_degree: s.funds.risk_degree,
      worst_utilization: worst,
      quality: s.quality,
      worst_rule: rule,
      state_version: s.state_version,
      as_of_ns: s.as_of_ns,
      trading_mode: s.trading_mode ?? 'OPEN',
      trading_day: s.trading_day,
      initial_capital: s.funds.initial_capital,
      margin: s.funds.margin,
      open_orders: s.open_orders ?? 0,
      name: a.spec.name,
      ...this.liveOf(a),
    }
  }

  private snapshotView(a: AcctState): AccountSnapshotView {
    return { ...wire('AccountSnapshot', a.snapshot as AccountSnapshot), name: a.spec.name, ...this.liveOf(a) }
  }

  /** Every account with a snapshot in the read model (a stale one included). */
  accountSnapshots(): AccountSnapshotView[] {
    return [...this.accts.values()].filter((a) => a.snapshot).map((a) => this.snapshotView(a))
  }

  /** `GET /accounts`: every configured account — its snapshot (`missing: false`) or `{…, missing: true}`. */
  accountList(): AccountListItem[] {
    return [...this.accts.values()]
      .sort((x, y) => x.spec.id.localeCompare(y.spec.id))
      .map((a): AccountListItem => {
        if (a.snapshot) return { ...this.snapshotView(a), missing: false }
        const item: AccountMissingItem = { account_id: a.spec.id, name: a.spec.name, shard: a.spec.shard, missing: true, ...this.liveOf(a) }
        if (a.lastChange) Object.assign(item, { state: a.state, halted_latched: a.latched, state_version: a.stateVersion })
        return item
      })
  }

  /** `GET /accounts/{id}`: read model, else `risk.query.accounts` to the owner. */
  accountSnapshot(id: string): AccountSnapshotView {
    const a = this.acct(id)
    if (a.snapshot) return this.snapshotView(a)
    if (!this.shardUp(a.spec.shard)) throw peerDown(`risk-engine-${a.spec.shard}`)
    throw new MockError('NOT_FOUND', `no snapshot for ${id} yet`, 404)
  }

  positions(id: string): PositionView[] {
    const a = this.acct(id)
    if (a.snapshot) return wireList('PositionView', a.snapshot.positions ?? [])
    throw peerDown(`risk-engine-${a.spec.shard}`)
  }

  /** `GET /accounts/{id}/orders` → `risk.query.orders` (OrderBatch). */
  orders(id: string, td?: string): { items: OrderUpdate[] } {
    const a = this.owned(id)
    const list = td && td !== this.tradingDay ? [] : [...a.orders.values()].sort((x, y) => x.created_ns - y.created_ns)
    return { items: wireList('OrderUpdate', list) }
  }

  /** `GET /accounts/{id}/trades` → `risk.query.fills` (FillBatch). */
  trades(id: string, td?: string): { items: Trade[] } {
    const a = this.owned(id)
    const list = td && td !== this.tradingDay ? [] : [...a.trades].sort((x, y) => x.trade_ts_ns - y.trade_ts_ns)
    return { items: wireList('Trade', list) }
  }

  /** `GET /accounts/{id}/equity-curve` → `risk.query.metrics` (MetricSeries). */
  equityCurve(id: string, td: string | undefined, stepS: number): MetricSeries {
    const a = this.owned(id)
    if (!Number.isInteger(stepS) || stepS < 1 || stepS > 3600) throw new MockError('BAD_REQUEST', 'step_s: Input should be greater than or equal to 1 and less than or equal to 3600', 400)
    const pts = td && td !== this.tradingDay ? [] : a.points
    const out: MetricPoint[] = []
    let lastTs = -Infinity
    for (const p of pts) {
      if (p.ts_ns - lastTs >= stepS * 1e9 - 1) {
        out.push(p)
        lastTs = p.ts_ns
      }
    }
    return { account_id: id, trading_day: td ?? this.tradingDay, step_s: stepS, points: out }
  }

  /** WS `equity:{id}` points (readmodel.sample_equity): no gross_notional / rule_util. */
  private equityPoints(a: AcctState): Record<string, unknown>[] {
    return a.points.map((p) => ({ ts_ns: p.ts_ns, equity: p.equity, daily_pnl_pct_init: p.daily_pnl_pct_init, intraday_dd: p.intraday_dd, state: p.state, nav: p.nav, risk_degree: p.risk_degree }))
  }

  /**
   * `GET /risk/events`: `risk.query.alerts` of every shard (or the owner) merged with the
   * read model. A shard that does not answer is listed in `errors`; for the current day the
   * read model still has what it saw live, for past days that shard's events are missing.
   */
  riskEventsFor(td?: string, account?: string): MockResult<RiskEventsResponse> {
    const day = td ?? this.tradingDay
    if (td !== undefined && !/^\d{8}$/.test(td)) throw new MockError('BAD_REQUEST', "td: String should match pattern '^\\d{8}$'", 400)
    let targets = RISK_INSTANCES
    if (account !== undefined) targets = [`risk-engine-${this.acct(account).spec.shard}`]
    const errors: Record<string, string> = {}
    for (const t of targets) if (!this.svcUp(t)) errors[t] = 'UNAVAILABLE'
    const downShards = new Set(Object.keys(errors).map((t) => Number(t.slice(-1))))
    const shardOf = (e: RiskEvent) => (e.account_id ? (this.accts.get(e.account_id)?.spec.shard ?? 0) : 0)
    let list: RiskEvent[]
    if (day === this.tradingDay) list = this.riskEvents.filter((e) => e.trading_day === day)
    else list = (this.pastEvents[day] ?? this.riskEvents.filter((e) => e.trading_day === day)).filter((e) => !downShards.has(shardOf(e)))
    if (account) list = list.filter((e) => e.account_id === account)
    const items = wireList('RiskEvent', [...list].sort((a, b) => a.ts_ns - b.ts_ns))
    const msg = Object.keys(errors).length ? `partial: ${Object.entries(errors).map(([k, v]) => `${k}=${v}`).join(', ')}` : ''
    return new MockResult({ trading_day: day, items, errors }, msg)
  }

  /** `GET /risk/state[?account=]` rows (readmodel.risk_state_rows + owner liveness). */
  riskStateRows(account?: string): RiskStateRow[] {
    if (account !== undefined) this.acct(account)
    return [...this.accts.values()]
      .filter((a) => (a.snapshot || a.lastChange) && (account === undefined || a.spec.id === account))
      .map((a) => ({
        account_id: a.spec.id,
        shard: a.snapshot ? a.spec.shard : null,
        trading_day: a.snapshot?.trading_day ?? a.lastChange?.trading_day ?? null,
        state: a.snapshot?.state ?? a.state,
        halted_latched: a.snapshot?.halted_latched ?? a.latched,
        state_version: a.snapshot?.state_version ?? a.stateVersion,
        restrictions: wireList('Restriction', a.snapshot?.restrictions ?? a.restrictions),
        trading_mode: a.snapshot?.trading_mode ?? null,
        last_change: a.lastChange ? wire('AccountStateChanged', a.lastChange) : null,
        ...this.liveOf(a),
      }))
  }

  /** `GET /risk/rules`: the coordinator's RuleSet; the local config (with a message) when no shard answers. */
  ruleSet(): MockResult<RuleSet> {
    const rs: RuleSet = { config_version: CONFIG_VERSION, config_hash: CONFIG_HASH, rules: RULES }
    if (RISK_INSTANCES.some((i) => this.svcUp(i))) return new MockResult(rs)
    return new MockResult(rs, 'risk shards unavailable: rules from the local config')
  }

  /** `GET /risk/compare` / WS `compare` (views.build_compare_view with the owner liveness of every account). */
  compareView(): CompareView {
    const v = buildCompareView(this.accountSnapshots(), RULES)
    const liveness = Object.fromEntries(v.accounts.map((id) => [id, this.liveOf(this.acct(id))]))
    return { ...v, liveness, accounts_stale: v.accounts.filter((id) => liveness[id]?.stale).sort() }
  }

  /** `GET /positions` (views.positions_view + accounts_missing / accounts_stale). */
  positionsView(q: { account?: string; product?: string; instrument?: string; exchange?: string; sector?: string; group_by?: string }): PositionsResponse {
    const groups: PositionGroupBy[] = ['account', 'product', 'instrument', 'exchange', 'sector']
    if (q.group_by && !groups.includes(q.group_by as PositionGroupBy)) throw new MockError('BAD_REQUEST', `group_by must be one of ${groups.join(', ')}`, 400)
    const rows: PositionRow[] = []
    for (const a of [...this.accts.values()].sort((x, y) => x.spec.id.localeCompare(y.spec.id))) {
      if (!a.snapshot || (q.account && a.spec.id !== q.account)) continue
      for (const p of a.snapshot.positions ?? []) {
        const sector = INSTRUMENT_MAP[p.instrument_id]?.sector ?? null
        if (q.product && p.product !== q.product) continue
        if (q.instrument && p.instrument_id !== q.instrument) continue
        if (q.exchange && p.exchange !== q.exchange) continue
        if (q.sector && sector !== q.sector) continue
        rows.push({ ...wire('PositionView', p), sector })
      }
    }
    rows.sort((x, y) => x.account_id.localeCompare(y.account_id) || x.instrument_id.localeCompare(y.instrument_id) || x.direction.localeCompare(y.direction))
    const gb = (q.group_by as PositionGroupBy | undefined) ?? null
    const out: PositionGroup[] = []
    if (gb) {
      const field = { account: 'account_id', product: 'product', instrument: 'instrument_id', exchange: 'exchange', sector: 'sector' }[gb] as keyof PositionRow
      const by = new Map<string, PositionGroup & { _a: Set<string>; _i: Set<string> }>()
      for (const r of rows) {
        const k = String(r[field] ?? '')
        let g = by.get(k)
        if (!g) {
          g = { group: k, positions: 0, accounts: [], instruments: [], long_qty: 0, short_qty: 0, net_qty: 0, td_qty: 0, yd_qty: 0, gross_notional: 0, net_notional: 0, margin: 0, position_pnl: 0, float_pnl_fifo: 0, _a: new Set(), _i: new Set() }
          by.set(k, g)
        }
        const qty = r.td_qty + r.yd_qty
        const sign = r.direction === 'LONG' ? 1 : -1
        const notional = Math.abs(r.notional ?? 0)
        g.positions += 1
        g._a.add(r.account_id)
        g._i.add(r.instrument_id)
        if (sign > 0) g.long_qty += qty
        else g.short_qty += qty
        g.net_qty += sign * qty
        g.td_qty += r.td_qty
        g.yd_qty += r.yd_qty
        g.gross_notional += notional
        g.net_notional += sign * notional
        g.margin += r.margin ?? 0
        g.position_pnl += r.position_pnl ?? 0
        g.float_pnl_fifo += r.float_pnl_fifo ?? 0
      }
      for (const k of [...by.keys()].sort()) {
        const { _a, _i, ...g } = by.get(k) as PositionGroup & { _a: Set<string>; _i: Set<string> }
        out.push({ ...g, accounts: [..._a].sort(), instruments: [..._i].sort() })
      }
    }
    const missing = [...this.accts.values()].filter((a) => !a.snapshot).map((a) => a.spec.id).sort()
    const stale = [...this.accts.values()].filter((a) => a.snapshot && !this.shardUp(a.spec.shard)).map((a) => a.spec.id).sort()
    return { group_by: gb, items: rows, groups: out, accounts_missing: missing, accounts_stale: stale }
  }

  firmSnapshot(): FirmSnapshot {
    const snaps = [...this.accts.values()].map((a) => a.snapshot).filter((x): x is AccountSnapshot => x !== null)
    const byState: Record<AccountState, number> = { NORMAL: 0, WARNING: 0, RESTRICTED: 0, HALTED: 0 }
    const prod: Record<string, { product: string; net_lots: number; net_notional: number; gross_notional: number; accounts: number }> = {}
    let equity = 0
    let pnl = 0
    let init = 0
    let margin = 0
    let gross = 0
    let net = 0
    for (const s of snaps) {
      byState[s.state] += 1
      equity += s.funds.equity
      pnl += s.metrics.daily_pnl
      init += s.funds.initial_capital
      margin += s.funds.margin
      gross += s.metrics.gross_notional
      net += s.metrics.net_notional
      for (const e of s.exposures ?? []) {
        const p = prod[e.product] ?? (prod[e.product] = { product: e.product, net_lots: 0, net_notional: 0, gross_notional: 0, accounts: 0 })
        p.net_lots += e.net_lots
        p.net_notional += e.net_notional
        p.gross_notional += e.gross_notional
        p.accounts += 1
      }
    }
    const nowNs = this.nowNs()
    return wire('FirmSnapshot', {
      trading_day: this.tradingDay,
      as_of_ns: nowNs,
      equity,
      daily_pnl: pnl,
      daily_pnl_pct_init: init ? pnl / init : 0,
      margin,
      risk_degree: equity ? margin / equity : 0,
      gross_notional: gross,
      net_notional: net,
      accounts_by_state: byState,
      product_net: Object.values(prod).sort((a, b) => Math.abs(b.net_notional) - Math.abs(a.net_notional)),
      kill_switch: this.killManual,
      shards: [0, 1].map((shard) => ({ shard, as_of_ns: this.shardUp(shard) ? nowNs : nowNs - 5e9, quality: (snaps.filter((s) => s.shard === shard).some((s) => s.quality !== 'OK') ? 'STALE' : 'OK') as MdQuality })),
    })
  }

  quoteOf(i: InstState): Record<string, unknown> {
    return {
      instrument_id: i.spec.id,
      last: i.last,
      bid1: i.bid,
      ask1: i.ask,
      volume: i.volume,
      change_pct: (i.last - i.preSettle) / i.preSettle,
      source: i.source,
      lag_ms: i.recvNs && i.exchNs ? (i.recvNs - i.exchNs) / 1e6 : 0,
      quality: i.quality,
      exchange: i.spec.exchange,
      upper_limit: i.upper,
      lower_limit: i.lower,
      pre_settle: i.preSettle,
      limit_lock: i.limitLock,
    }
  }

  private publishQuotes(): void {
    for (const i of this.inst.values()) if (i.wasInSession && i.source) this.publish('quotes', 'upsert', this.quoteOf(i))
  }

  /** WS `quotes` snapshot: instruments that had a LIVE tick. */
  quotes(): Record<string, unknown>[] {
    return [...this.inst.values()].filter((i) => i.exchNs > 0).map((i) => this.quoteOf(i))
  }

  /** `GET /market/quotes`: the latest LIVE Tick of each instrument (read model). */
  ticks(instruments?: string[]): Tick[] {
    return [...this.inst.values()]
      .filter((i) => i.exchNs > 0 && (!instruments || instruments.includes(i.spec.id)))
      .sort((a, b) => a.spec.id.localeCompare(b.spec.id))
      .map((i) => {
        const p = shanghaiParts(i.exchNs / 1e6)
        return wire('Tick', {
          instrument_id: i.spec.id,
          exchange: i.spec.exchange,
          trading_day: this.tradingDay,
          action_day: `${p.y}${String(p.mo).padStart(2, '0')}${String(p.d).padStart(2, '0')}`,
          exch_ts_ns: i.exchNs,
          last: i.last,
          volume: i.volume,
          turnover: i.turnover,
          open_interest: i.oi,
          bid1: i.bid,
          ask1: i.ask,
          bid_vol1: 10 + this.rng.int(0, 200),
          ask_vol1: 10 + this.rng.int(0, 200),
          open: i.open,
          high: i.high,
          low: i.low,
          upper_limit: i.upper,
          lower_limit: i.lower,
          pre_settle: i.preSettle,
          source: i.source ?? 'FEED_A',
          recv_ns: i.recvNs,
          fingerprint: this.rng.int(1, 2 ** 31),
          flags: (i.limitLock === 'UP' ? 4 : 0) | (i.limitLock === 'DOWN' ? 8 : 0),
        })
      })
  }

  mdStatus(): MdStatus {
    return wire('MdStatus', {
      as_of_ns: this.nowNs(),
      trading_day: this.tradingDay,
      sources: this.sources.map((s) => ({
        source: s.id,
        state: s.state,
        since_ns: s.sinceNs,
        connected: s.connected,
        epoch: s.id === 'FEED_A' ? s.epoch : null,
        last_seq: s.id === 'FEED_A' ? s.lastSeq : null,
        last_msg_ns: s.lastMsgNs,
        lag_ms: s.connected ? s.baseLag + s.extraLag : null,
        reject_rate: Number(s.rejectRate.toFixed(4)),
        msgs_per_s: Number(s.msgsPerS.toFixed(1)),
        integrity_votes: s.votes,
      })),
      instruments: [...this.inst.values()].map((i) => ({ instrument_id: i.spec.id, quality: i.quality, live_source: i.source, wm_volume: i.volume, wm_exch_ts_ns: i.exchNs, last_live_ns: i.lastLiveMs * 1e6, quiet: i.limitLock !== 'NONE', limit_lock: i.limitLock })),
    })
  }

  /** `GET /market/feeds`: the read model's md.status (else md.sources) + md-gateway's `stale` / `age_ms`. */
  feeds(): MdFeedsView {
    if (this.svcUp('md-gateway')) this.lastMd = this.mdStatus()
    if (!this.lastMd) throw peerDown('md-gateway')
    return { ...this.lastMd, ...this.instanceLive('md-gateway') }
  }

  /** `GET /market/events` → md.events (MdEventBatch); md-gateway only. */
  mdEventsFor(td?: string, kind?: string): { items: MdEvent[] } {
    if (!this.svcUp('md-gateway')) throw peerDown('md-gateway')
    const kinds = kind ? kind.split(',').map((k) => k.trim()).filter(Boolean) : null
    const day = td ?? this.tradingDay
    const list = this.mdEvents.filter((e) => e.trading_day === day && (!kinds || kinds.includes(e.kind))).sort((a, b) => a.ts_ns - b.ts_ns)
    return { items: wireList('MdEvent', list) }
  }

  serviceEventsList(): ServiceStateChanged[] {
    return wireList('ServiceStateChanged', [...this.serviceEvents].reverse())
  }

  /** `GET /system/status` (routes/system.system_status). */
  systemStatus(): SystemStatus {
    const instances = [...this.svcs.values()].filter((s) => !s.spec.external)
    const up = instances.filter((s) => this.liveness(s) === 'UP').map((s) => s.spec.instance)
    const down = instances.map((s) => s.spec.instance).filter((i) => !up.includes(i)).sort()
    const shards: SystemStatus['shards'] = {}
    for (const inst of RISK_INSTANCES) {
      const s = this.svcs.get(inst)
      shards[inst] = s && s.processState === 'RUNNING' ? { state: s.hbState ?? 'RUNNING', trading_day: this.tradingDay, trading_mode: this.accts.get(inst === 'risk-engine-0' ? 'ACC1' : 'ACC3')?.tradingMode ?? 'OPEN' } : null
    }
    const modes = new Set([...this.accts.values()].map((a) => a.tradingMode))
    const tradingMode = (['PROVISIONAL', 'EOD_FROZEN', 'OPEN'] as TradingMode[]).find((m) => modes.has(m)) ?? null
    const control = this.firmControlState()
    const counts = { PENDING: 0, SENT: 3 + this.riskEvents.filter((e) => e.severity === 'CRITICAL').length, FAILED: 0, SUPPRESSED: 2 }
    return {
      instance: 'api-gateway',
      state: down.length ? 'DEGRADED' : 'RUNNING',
      mode: this.appMode,
      trading_day: this.tradingDay,
      trading_mode: tradingMode,
      // the read model keeps the last FirmSnapshot (kill_switch = manual OR rule) even while the coordinator is down
      kill_switch: this.lastFirm ? Boolean(this.lastFirm.kill_switch) : control.kill_switch,
      firm_control: control,
      kill_switch_retry: this.killRetryState(),
      kill_switch_control: this.killControlState(),
      clock: wire('ClockState', this.clockState()),
      config: { config_version: CONFIG_VERSION, config_hash: CONFIG_HASH },
      services: { total: instances.length, up: up.length, down },
      shards,
      readmodel: {
        synced: Object.fromEntries(instances.filter((s) => s.spec.instance !== 'api-gateway').map((s) => [s.spec.instance, s.processState === 'RUNNING'])),
        accounts: [...this.accts.values()].filter((a) => a.snapshot).length,
        instruments: [...this.inst.values()].filter((i) => i.exchNs > 0).length,
        applied: this.stepCount * 17,
        decode_errors: 0,
        accounts_stale: [...this.accts.values()].filter((a) => this.liveOf(a).stale).map((a) => a.spec.id).sort(),
        repair: { gaps: 0, repairs: 0, repair_failures: 0, resyncs: 0 },
      },
      ws: { clients: 1, subscriptions: {}, queued: 0, queued_bytes: 0, overflows: 0, evictions: 0, revoked: 0 },
      auth: { enabled: ['mock-admin'], missing_env: [], rejected: {}, admin_unavailable: [], bootstrap_active: false, throttled: 0 },
      notifications: {
        failed: counts.FAILED,
        suppressed: counts.SUPPRESSED,
        pending: counts.PENDING,
        counts,
        channel: 'log',
        waiting: 0,
        stats: { consumed: counts.SENT + counts.SUPPRESSED, created: counts.SENT + counts.SUPPRESSED, duplicates: 0, lifecycle_skipped: 0, suppressed_stale: 0, suppressed_rate: counts.SUPPRESSED, digests: 0, sent: counts.SENT, failed: 0, retries: 0, last_error: null, recent: [] },
      },
      service_events: wireList('ServiceStateChanged', [...this.serviceEvents].reverse().slice(-50)),
    }
  }

  /** Trading days of the window ending with the current day (gateway.recent_trading_days). */
  private recentDays(n: number): string[] {
    const past = this.history.map((d) => d.trading_day).filter((d) => d < this.tradingDay)
    return [...past, this.tradingDay].slice(-n)
  }

  /** `GET /risk/compare/history?days=1..60` → `{days, missing, errors}`. */
  historyDays(n: number): MockResult<CompareHistory> {
    if (!Number.isInteger(n) || n < 1 || n > 60) throw new MockError('BAD_REQUEST', 'days: Input should be greater than or equal to 1 and less than or equal to 60', 400)
    const tds = this.recentDays(n)
    if (!this.svcUp('clearing-worker')) {
      const errors = Object.fromEntries(tds.map((d) => [d, 'UNAVAILABLE']))
      return new MockResult({ days: [], errors } as unknown as CompareHistory, 'settlement history unavailable', false, 'UNAVAILABLE')
    }
    const shardsUp = RISK_INSTANCES.every((i) => this.svcUp(i))
    const days: CompareHistoryDay[] = []
    const missing: string[] = []
    for (const td of tds) {
      const d = this.history.find((x) => x.trading_day === td)
      if (!d) {
        missing.push(td)
        continue
      }
      const item: CompareHistoryDay = { trading_day: d.trading_day, version: d.version ?? 1, accounts: d.accounts }
      if (shardsUp) {
        const counts: Record<string, number> = Object.fromEntries(ACCOUNTS.map((a) => [a.id, 0]))
        for (const [k, v] of Object.entries(d.breakers ?? {})) counts[k] = v
        item.breakers = counts
      }
      days.push(item)
    }
    return new MockResult({ days, missing: missing.sort(), errors: {} })
  }

  eodEvents(): EodPhaseEvent[] {
    return [...this.eodLog]
  }

  /** `GET /settlement/days?limit=1..120` → `{items, errors}` (latest phase per day, newest first). */
  settlementDays(limit: number): MockResult<SettlementDaysResponse> {
    if (!Number.isInteger(limit) || limit < 1 || limit > 120) throw new MockError('BAD_REQUEST', 'limit: Input should be greater than or equal to 1 and less than or equal to 120', 400)
    const tds = this.recentDays(limit)
    const items: SettlementDaysResponse['items'] = []
    const errors: Record<string, string> = {}
    const up = this.svcUp('clearing-worker')
    const item = (ev: EodPhaseEvent): SettlementDayItem => ({ trading_day: ev.trading_day, version: ev.version, phase: ev.phase, warning_count: this.warningsOf(ev.trading_day, ev.version).length })
    for (const td of tds) {
      const ev = this.eodPhases.get(td)
      if (!up) {
        // the read model still knows EOD phases (and warnings) seen live on sys.eod
        if (ev && this.eodLog.some((x) => x.trading_day === td)) items.push(item(ev))
        else errors[td] = 'UNAVAILABLE'
        continue
      }
      if (ev) items.push(item(ev))
    }
    items.sort((a, b) => b.trading_day.localeCompare(a.trading_day))
    const n = Object.keys(errors).length
    return new MockResult({ items, errors }, n ? `clearing-worker unavailable for ${n} day(s)` : '')
  }

  /**
   * `GET /settlement/{td}/status`: settle.status (`detail.warning_count` / `detail.warnings`
   * `[{kind, message}]`, the last 10); while clearing-worker is down the latest phase seen on
   * sys.eod with the read model's warning records.
   */
  settlementStatus(td: string): MockResult<EodPhaseEvent> {
    const ev = this.eodPhases.get(td)
    if (!this.svcUp('clearing-worker')) {
      if (!ev || !this.eodLog.some((x) => x.trading_day === td)) throw peerDown('clearing-worker')
      const warns = this.warningsOf(td, ev.version)
      const data = wire('EodPhaseEvent', { ...ev, detail: { ...(ev.detail ?? {}), warning_count: warns.length, warnings: warns.slice(-10) } })
      return new MockResult(data, 'clearing-worker UNAVAILABLE: latest phase seen on sys.eod')
    }
    if (!ev) throw new MockError('NOT_FOUND', `no EOD run for ${td}`, 404)
    const warns = this.warningsOf(td, ev.version)
    const detail = { ...(ev.detail ?? {}) }
    if (warns.length) {
      detail.warning_count = warns.length
      detail.warnings = warns.slice(-10).map((w) => ({ kind: w.kind, message: w.message }))
    }
    return new MockResult(wire('EodPhaseEvent', { ...ev, detail }))
  }

  settlementResult(td: string, account?: string): SettlementResult {
    this.clearing()
    const d = this.history.find((x) => x.trading_day === td)
    if (!d) throw new MockError('NOT_FOUND', `no settlement for ${td}`, 404)
    return wire('SettlementResult', {
      trading_day: td,
      version: d.version ?? 1,
      accounts: account ? d.accounts.filter((a) => a.account_id === account) : d.accounts,
      prices: INSTRUMENTS.map((i) => ({ trading_day: td, instrument_id: i.id, price: String(i.preSettle), source: 'VWAP', method: i.exchange === 'CFFEX' ? 'VWAP_LAST_HOUR' : 'VWAP_FULL_DAY', rounding: i.exchange === 'SHFE' ? 'FLOOR_TICK' : 'ROUND_TICK', input_quality: 'OK', version: 1 })),
      checks: [
        { check: 'EQUITY_IDENTITY', status: 'PASS' },
        { check: 'EXCH_FORMULA', status: 'PASS' },
        { check: 'MTM_VS_FIFO', status: 'PASS' },
        { check: 'TRADE_DIGEST', status: 'PASS' },
      ],
    } as SettlementResult)
  }

  /**
   * `GET /settlement/{td}/statements/{account}` → `{format, text?, settlement?}` (review
   * fix): the STATEMENT_TXT file when the day has one, else the settlement data with a message.
   */
  statement(td: string, account: string): MockResult<StatementResponse> {
    this.clearing()
    if (!/^\d{8}$/.test(td)) throw new MockError('BAD_REQUEST', 'trading day must be YYYYMMDD', 400)
    const d = this.history.find((x) => x.trading_day === td)
    const hasFile = [...this.reports.values()].some((r) => r.trading_day === td && r.type === 'STATEMENT_TXT' && r.account_id === account)
    if (d && hasFile) return new MockResult({ format: 'text', text: renderStatementTxt(d, account) })
    const result = this.settlementResult(td, account)
    return new MockResult({ format: 'settlement', settlement: result }, 'statement file not available: settlement data instead')
  }

  /** `GET /reports[?td=][&kind=]` (report.list, filtered by type in the gateway). */
  listReports(td?: string, kind?: string): { items: ReportReady[] } {
    const kinds = ['RISK_DAILY_HTML', 'STATEMENT_TXT', 'STATEMENT_CSV', 'SETTLEMENT_JSON', 'BACKTEST_HTML']
    if (kind !== undefined && !kinds.includes(kind)) throw new MockError('BAD_REQUEST', `kind must be one of ${kinds.join(', ')}`, 400)
    this.clearing()
    const list = [...this.reports.values()].filter((r) => (!td || r.trading_day === td) && (!kind || r.type === kind)).sort((a, b) => b.created_ns - a.created_ns)
    return { items: wireList('ReportReady', list) }
  }

  reportFile(id: string): { contentType: string; body: string; filename: string } {
    this.clearing()
    const r = this.reports.get(id)
    if (!r) throw new MockError('NOT_FOUND', `report ${id} not found`, 404)
    const filename = r.path.split('/').pop() ?? 'report'
    if (r.type === 'BACKTEST_HTML') return { contentType: 'text/html; charset=utf-8', body: renderBacktestHtml(r.path, this.backtestSummaries.get(id) ?? {}), filename }
    const d = this.history.find((x) => x.trading_day === r.trading_day)
    if (!d) throw new MockError('NOT_FOUND', `report file ${r.path} not found on the reports volume`, 404)
    switch (r.type) {
      case 'RISK_DAILY_HTML':
        return { contentType: 'text/html; charset=utf-8', body: renderRiskDailyHtml(d, (this.pastEvents[d.trading_day] ?? []).concat(this.riskEvents.filter((e) => e.trading_day === d.trading_day && ((e.kind === 'ALERT_RAISED' && e.action === 'CIRCUIT_BREAK') || e.kind === 'RESET' || e.kind === 'MANUAL_HALT')))), filename }
      case 'STATEMENT_TXT':
        return { contentType: 'text/plain; charset=utf-8', body: renderStatementTxt(d, r.account_id ?? ''), filename }
      case 'STATEMENT_CSV':
        return { contentType: 'text/csv; charset=utf-8', body: renderStatementCsv(d, r.account_id ?? ''), filename }
      default:
        return { contentType: 'application/json', body: JSON.stringify(this.settlementResult(d.trading_day), null, 2), filename }
    }
  }

  plansList(): LiquidationPlan[] {
    return [...this.plans.values()]
  }

  /** `GET /health` (public): identity only — home_id, pid, epoch; no secrets. */
  health(): HealthInfo {
    const down = [...this.svcs.values()].some((s) => !s.spec.external && this.liveness(s) !== 'UP')
    const gw = this.svcs.get('api-gateway')
    return { status: 'ok', instance: 'api-gateway', state: down ? 'DEGRADED' : 'RUNNING', home_id: HOME_ID, pid: gw?.pid ?? 4242, epoch: GATEWAY_EPOCH, version: APP_VERSION, trading_day: this.tradingDay, ts_ns: this.nowNs() }
  }

  hasAccount(id: string): boolean {
    return this.accts.has(id)
  }

  authMe(): AuthMe {
    return { key_id: 'mock-admin', name: 'mock ADMIN key', role: 'ADMIN' }
  }

  /** WS snapshot payload for a topic (on `sub` / `resync`), as wshub.snapshot_data_sync builds it. */
  snapshotFor(topic: string): unknown {
    if (topic === 'quotes') return this.quotes()
    if (topic === 'feeds') {
      if (this.svcUp('md-gateway')) this.lastMd = this.mdStatus()
      return this.lastMd
    }
    if (topic === 'accounts') return [...this.accts.values()].filter((a) => a.snapshot).map((a) => this.summaryOf(a))
    if (topic === 'events') {
      const today = this.riskEvents.filter((e) => e.trading_day === this.tradingDay)
      return wireList('RiskEvent', [...today].sort((a, b) => a.ts_ns - b.ts_ns).slice(-200))
    }
    if (topic === 'compare') return this.compareView()
    if (topic === 'firm') {
      if (this.svcUp('risk-engine-0')) this.lastFirm = this.firmSnapshot()
      return this.firmTopic()
    }
    if (topic === 'system') return this.systemTopicSnapshot()
    if (topic === 'eod') return this.eodEvents().map((e) => wire('EodPhaseEvent', e))
    if (topic === 'backtests') return this.backtestsTopic()
    const [kind, id] = topic.split(':') as [string, string | undefined]
    const a = id ? this.accts.get(id) : undefined
    if (!a) return []
    if (kind === 'positions') return a.snapshot ? wireList('PositionView', a.snapshot.positions ?? []) : []
    if (kind === 'orders') return wireList('OrderUpdate', [...a.orders.values()].sort((x, y) => x.created_ns - y.created_ns))
    if (kind === 'equity') return this.equityPoints(a)
    return []
  }

}
