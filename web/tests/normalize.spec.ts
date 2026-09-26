/** Payload guards / normalizers and the state_version merge rule (02 §5 rule 2). */
import { describe, expect, it } from 'vitest'
import type { AccountSnapshot, AccountStateChanged, AccountSummary, EodPhaseEvent, RiskEvent, ServiceHealth, Tick } from '@/types'
import {
  asItems,
  extractPlan,
  isAccountStateChanged,
  isEodPhaseEvent,
  isMdEvent,
  isMdStatus,
  isReportReady,
  isRiskEvent,
  isServiceStateChanged,
  isSettlementCompleted,
  mergeAccountState,
  normalizeCompare,
  normalizeHistory,
  toAccountSummary,
  toQuote,
  toServiceHealth,
  isClockItem,
  mergeAccountSummary,
  normalizeStatement,
  eodWarningOf,
  eodWarningsIn,
  processStateStale,
} from '@/utils/normalize'

const T = 1_790_000_000_000_000_000

function snap(over: Partial<AccountSnapshot> = {}): AccountSnapshot {
  return {
    account_id: 'ACC3',
    shard: 1,
    trading_day: '20260928',
    version: 7,
    state_version: 10,
    as_of_ns: T,
    quality: 'OK',
    state: 'HALTED',
    halted_latched: false,
    restrictions: [],
    funds: { initial_capital: 1e7, cum_net_deposit: 0, pre_balance: 9.87e6, net_deposit: 0, static_equity: 9.87e6, close_pnl: 0, position_pnl: -3e5, fee: 1200, equity: 9.5688e6, margin: 1.2e6, frozen_margin: 0, frozen_fee: 0, available: 8.3e6, risk_degree: 0.125 },
    metrics: { daily_pnl: -301200, daily_pnl_pct_init: -0.03012, daily_pnl_pct_sod: -0.0305, floating_pnl_pct_init: -0.03, perf_equity: 9.5688e6, nav: 0.95688, intraday_peak_equity: 9.9e6, intraday_dd: 0.0334, hwm_equity: 1.02e7, hwm_dd: 0.062, gross_notional: 1.3e7, net_notional: 1.3e7, leverage: 1.36, effective_n: 3.9, limit_stress: 0.077, orders_today: 12, cancels_today: 1 },
    rules: [
      { rule_id: 'daily_loss', scope_key: 'acct:ACC3', metric: 'account.daily_pnl_pct_init', value: -0.03012, utilization: 1.004, hard_utilization: 1.004, hard_level: 'halt' },
      { rule_id: 'leverage', scope_key: 'acct:ACC3', metric: 'account.leverage', value: 1.36, utilization: 0.34, hard_utilization: 0.34, hard_level: 'block' },
    ],
    open_orders: 0,
    ...over,
  }
}

function change(over: Partial<AccountStateChanged> = {}): AccountStateChanged {
  return { account_id: 'ACC3', trading_day: '20260928', from_state: 'HALTED', to_state: 'HALTED', halted_latched: true, state_version: 11, restrictions: [], cause: 'RULE', ts_ns: T + 1e9, ...over }
}

describe('mergeAccountState', () => {
  it('a state change with a larger state_version wins over the snapshot', () => {
    const eff = mergeAccountState(snap(), null, change())
    expect(eff).toMatchObject({ state: 'HALTED', halted_latched: true, state_version: 11, source: 'change' })
  })

  it('an older state change never overrides a newer snapshot', () => {
    const eff = mergeAccountState(snap({ state_version: 12, state: 'RESTRICTED' }), null, change({ state_version: 11 }))
    expect(eff).toMatchObject({ state: 'RESTRICTED', state_version: 12, source: 'snapshot' })
  })

  it('without state_version on the summary, the newer timestamp wins', () => {
    const sum: AccountSummary = { account_id: 'ACC3', shard: 1, state: 'RESTRICTED', halted_latched: false, equity: 1, daily_pnl: 0, daily_pnl_pct_init: 0, risk_degree: 0, worst_utilization: null, quality: 'OK', as_of_ns: T }
    expect(mergeAccountState(null, sum, change({ ts_ns: T + 5e8 }))?.source).toBe('change')
    expect(mergeAccountState(null, { ...sum, as_of_ns: T + 9e9 }, change({ ts_ns: T + 5e8 }))?.source).toBe('summary')
  })

  it('returns null when nothing is known', () => {
    expect(mergeAccountState(null, null, null)).toBeNull()
  })
})

describe('toAccountSummary', () => {
  it('derives the summary (incl. worst rule) from a full snapshot', () => {
    const s = toAccountSummary(snap())
    expect(s).toMatchObject({ account_id: 'ACC3', state: 'HALTED', halted_latched: false, equity: 9.5688e6, daily_pnl_pct_init: -0.03012, risk_degree: 0.125, worst_utilization: 1.004, worst_rule: 'daily_loss', worst_level: 'halt', state_version: 10, initial_capital: 1e7 })
  })

  it('accepts the WS `accounts` item shape and rejects unknown states', () => {
    const s = toAccountSummary({ account_id: 'ACC1', shard: 0, state: 'NORMAL', halted_latched: false, equity: 10, daily_pnl: 1, daily_pnl_pct_init: 0.001, risk_degree: 0.07, worst_utilization: 0.62, quality: 'OK' })
    expect(s?.worst_utilization).toBe(0.62)
    expect(toAccountSummary({ account_id: 'ACC1', state: 'WEIRD' })).toBeNull()
    expect(toAccountSummary(null)).toBeNull()
  })
})

describe('toQuote', () => {
  it('builds a quote view from a full Tick (change, lag, limit lock from flags)', () => {
    const tick: Tick = { instrument_id: 'rb2701', exchange: 'SHFE', trading_day: '20260928', action_day: '20260928', exch_ts_ns: T, last: 2964, volume: 100, turnover: 1, open_interest: 1, upper_limit: 3276, lower_limit: 2964, pre_settle: 3120, source: 'FEED_B', recv_ns: T + 620e6, flags: 8 }
    const q = toQuote(tick)
    expect(q?.change_pct).toBeCloseTo(-0.05, 10)
    expect(q?.lag_ms).toBeCloseTo(620, 6)
    expect(q?.limit_lock).toBe('DOWN')
    expect(q?.source).toBe('FEED_B')
  })

  it('keeps the WS quote item shape', () => {
    const q = toQuote({ instrument_id: 'IF2612', last: 4426, change_pct: 0.0025, source: 'FEED_A', lag_ms: 50, quality: 'STALE' })
    expect(q).toMatchObject({ instrument_id: 'IF2612', last: 4426, quality: 'STALE', lag_ms: 50 })
    expect(toQuote({ instrument_id: 'x' })).toBeNull()
  })
})

describe('toServiceHealth', () => {
  it('merges a ServiceInfo with its nested heartbeat', () => {
    const h = toServiceHealth({
      instance: 'md-gateway',
      service: 'md-gateway',
      state: 'RUNNING',
      pid: 5135,
      restarts: 1,
      heartbeat_state: 'DEGRADED',
      last_heartbeat_ns: T,
      heartbeat: { instance: 'md-gateway', service: 'md-gateway', state: 'DEGRADED', pid: 5135, epoch: 'e', app_version: '0.1.0', started_ns: T, loop_lag_ms: 5, queues: { normalize: 3 }, rates: { ticks_in: 44 }, latency_us: { normalize: { p50: 32, p99: 140, max: 200, n: 10 } }, config_version: 3, config_hash: 'abc', clock_version: 4 },
    })
    expect(h).toMatchObject({ process_state: 'RUNNING', heartbeat_state: 'DEGRADED', restarts: 1, loop_lag_ms: 5, config_version: 3, clock_version: 4 })
    expect(h?.queues).toEqual({ normalize: 3 })
    expect(h?.latency.normalize?.p99).toBe(140)
  })

  it('reads the WS `system` item: a process-only state means the process is down', () => {
    const prev = toServiceHealth({ kind: 'service', instance: 'risk-engine-1', state: 'RUNNING', restarts: 0, liveness: 'UP' }, null, 'ws')
    expect(prev).toMatchObject({ process_state: 'RUNNING', heartbeat_state: 'RUNNING', service: 'risk-engine', liveness: 'UP' })
    const down = toServiceHealth({ instance: 'risk-engine-1', state: 'EXITED', restarts: 0, pid: 4242, liveness: 'DOWN' }, prev, 'ws')
    expect(down).toMatchObject({ process_state: 'EXITED', heartbeat_state: null, liveness: 'DOWN' })
    const back = toServiceHealth({ instance: 'risk-engine-1', state: 'STARTING', restarts: 1, pid: 4300, liveness: 'UP' }, down, 'ws')
    expect(back).toMatchObject({ process_state: 'RUNNING', heartbeat_state: 'STARTING', restarts: 1, pid: 4300 })
  })

  it('takes a REST item without supervisor data as unknown process state, keeping the liveness', () => {
    // GET /system/services while the supervisor is down: {instance, service, restarts, liveness, since_ns, heartbeat_ns}
    const h = toServiceHealth({ instance: 'clearing-worker', service: 'clearing-worker', restarts: 0, liveness: 'DOWN', since_ns: null, heartbeat_ns: null })
    expect(h).toMatchObject({ process_state: null, heartbeat_state: null, liveness: 'DOWN', pid: null, heartbeat: null, since_ns: null, heartbeat_ns: null })
    const t = toServiceHealth({ instance: 'risk-engine-1', service: 'risk-engine', restarts: 0, liveness: 'UP', heartbeat_state: 'RUNNING', since_ns: T, heartbeat_ns: T + 1e9, last_error: 'boom' })
    expect(t).toMatchObject({ since_ns: T, heartbeat_ns: T + 1e9, last_error: 'boom' })
    // a REST item carrying the supervisor's state: `state` / `process_state` is the ProcessState
    const p = toServiceHealth({ instance: 'md-gateway', service: 'md-gateway', state: 'RESTARTING', process_state: 'RESTARTING', restarts: 2, liveness: 'DOWN' })
    expect(p).toMatchObject({ process_state: 'RESTARTING', liveness: 'DOWN', restarts: 2 })
  })
})

describe('owner liveness and reduced account items', () => {
  it('keeps the numbers of a previous summary when a reduced item (state only) arrives', () => {
    const full = toAccountSummary({ ...snap({ state: 'RESTRICTED', halted_latched: false }), name: '日内逆势加仓', stale: false, age_ms: 300 })
    expect(full).toMatchObject({ name: '日内逆势加仓', stale: false, age_ms: 300, equity: 9.5688e6 })
    const reduced = toAccountSummary({ account_id: 'ACC3', state: 'HALTED', halted_latched: true, state_version: 12, as_of_ns: T + 5e9 })
    expect(reduced).toMatchObject({ equity: null, daily_pnl: null, shard: null, quality: null })
    const merged = mergeAccountSummary(full ?? undefined, reduced as AccountSummary)
    expect(merged).toMatchObject({ state: 'HALTED', halted_latched: true, equity: 9.5688e6, shard: 1, quality: 'OK', name: '日内逆势加仓', worst_rule: 'daily_loss' })
  })

  it('reads the stale flag of an account whose shard is down', () => {
    const s = toAccountSummary({ account_id: 'ACC4', state: 'WARNING', halted_latched: false, equity: 1e7, daily_pnl: 1, daily_pnl_pct_init: 0, risk_degree: 0.2, worst_utilization: 0.8, quality: 'OK', shard: 1, stale: true, age_ms: 12_000 })
    expect(s).toMatchObject({ stale: true, age_ms: 12_000 })
  })
})

describe('landed gateway shapes', () => {
  it('tells the clock item of the WS `system` topic (kind "clock") from service items', () => {
    const clock = { mode: 'DEMO', offset_ns: 5, version: 4, market_now_ns: T }
    expect(isClockItem({ ...clock, kind: 'clock', instance: 'sys.clock' })).toBe(true)
    expect(isClockItem(clock)).toBe(false)
    expect(isClockItem({ kind: 'service', instance: 'md-gateway', state: 'RUNNING', restarts: 0 })).toBe(false)
  })

  it('reads the statement answer {format, text?, settlement?}', () => {
    const settlement = { trading_day: '20260924', version: 1, accounts: [] }
    expect(normalizeStatement({ format: 'text', text: 'abc' })).toEqual({ text: 'abc', result: null })
    expect(normalizeStatement({ format: 'settlement', settlement })).toEqual({ text: null, result: settlement })
    expect(normalizeStatement('plain')).toEqual({ text: null, result: null })
  })

  it('reads the stale accounts and the liveness of a compare view', () => {
    const base = { trading_day: '20260928', as_of_ns: T, accounts: ['ACC1', 'ACC3'], rules: [], matrix: [], ranking: [], exposure: { products: [], accounts: ['ACC1'], cells: [], firm_net: {} } }
    const v = normalizeCompare({ ...base, accounts_stale: ['ACC3'], liveness: { ACC1: { stale: false, age_ms: 40 }, ACC3: { stale: true, age_ms: 9000 } } })
    expect(v?.accounts_stale).toEqual(['ACC3'])
    expect(v?.liveness?.ACC3).toEqual({ stale: true, age_ms: 9000 })
    expect(normalizeCompare(base)?.accounts_stale).toBeUndefined()
  })

  it('skips a configured account without a snapshot or a state; keeps a known state', () => {
    expect(toAccountSummary({ account_id: 'ACC1', name: '黑色系通道突破', shard: 0, missing: true, stale: true, age_ms: null })).toBeNull()
    const s = toAccountSummary({ account_id: 'ACC5', name: '螺纹跨期套利', shard: 1, missing: true, stale: false, age_ms: 30, state: 'WARNING', halted_latched: false, state_version: 3 })
    expect(s).toMatchObject({ account_id: 'ACC5', state: 'WARNING', equity: null, stale: false, age_ms: 30 })
  })

  it('reads EOD warnings: WARN events (detail.warning) and detail.warnings items', () => {
    const warn: EodPhaseEvent = { trading_day: '20260928', version: 1, phase: 'DONE', ts_ns: T, detail: { warning: 'late trade', kind: 'LATE_FACT', warn_id: 'w1', account_id: 'ACC2' } }
    expect(eodWarningOf(warn)).toMatchObject({ kind: 'LATE_FACT', message: 'late trade', warn_id: 'w1', phase: 'DONE', context: { account_id: 'ACC2' } })
    const fixed: EodPhaseEvent = { trading_day: '20260928', version: 1, phase: 'PRICES_FIXED', ts_ns: T, detail: { warnings: [{ kind: 'PRE_SETTLE_MISMATCH', message: 'j2701 SOD 1680 / md 1681' }] } }
    expect(eodWarningOf(fixed)).toBeNull()
    expect(eodWarningsIn(fixed)).toEqual([expect.objectContaining({ kind: 'PRE_SETTLE_MISMATCH', trading_day: '20260928', version: 1 })])
  })

  it('treats a gone process state older than the last heartbeat as stale (since_ns < heartbeat_ns)', () => {
    const base = { process_state: 'EXITED', liveness: 'UP', since_ns: T, heartbeat_ns: T + 5e9 } as Pick<ServiceHealth, 'process_state' | 'liveness' | 'since_ns' | 'heartbeat_ns'>
    expect(processStateStale(base)).toBe(true)
    // exited after its last heartbeat: the state is current
    expect(processStateStale({ ...base, heartbeat_ns: T - 5e8 })).toBe(false)
    expect(processStateStale({ ...base, liveness: 'DOWN' })).toBe(false)
    expect(processStateStale({ ...base, process_state: 'RUNNING' })).toBe(false)
    expect(processStateStale({ ...base, since_ns: null })).toBe(false)
  })
})

describe('guards and list shapes', () => {
  const ev: RiskEvent = { event_id: 'e1', kind: 'LIQUIDATION_PLAN', trading_day: '20260928', ts_ns: T, account_id: 'ACC3', alert_key: 'k', detail: { plan: { plan_id: 'p1', status: 'PENDING_CONFIRM', steps: [{ instrument_id: 'rb2701', direction: 'SELL', offset: 'CLOSE_YESTERDAY', qty: 63, price_rule: '对手价 -2 跳' }], created_ns: T } } }

  it('tells RiskEvent and AccountStateChanged apart on the `events` topic', () => {
    expect(isRiskEvent(ev)).toBe(true)
    expect(isAccountStateChanged(ev)).toBe(false)
    expect(isAccountStateChanged(change())).toBe(true)
    expect(isRiskEvent(change())).toBe(false)
  })

  it('tells the `eod` and `feeds` payloads apart', () => {
    expect(isEodPhaseEvent({ trading_day: '20260928', version: 1, phase: 'SETTLED', ts_ns: T })).toBe(true)
    expect(isSettlementCompleted({ trading_day: '20260928', version: 1, next_trading_day: '20260929', accounts: [], ts_ns: T })).toBe(true)
    expect(isSettlementCompleted({ trading_day: '20260928', phase: 'DONE', next_trading_day: 'x', accounts: [] })).toBe(false)
    expect(isReportReady({ report_id: 'r', type: 'RISK_DAILY_HTML', path: 'reports/x.html' })).toBe(true)
    expect(isMdStatus({ sources: [], instruments: [] })).toBe(true)
    expect(isMdEvent({ event_id: 'm', kind: 'GAP_FILLED', ts_ns: T, trading_day: 'd' })).toBe(true)
    expect(isMdEvent({ event_id: 'm', kind: 'NOPE' })).toBe(false)
    expect(isServiceStateChanged({ instance: 'md-gateway', from_state: 'RUNNING', to_state: 'EXITED', reason: 'KILLED_BY_DEMO' })).toBe(true)
  })

  it('extracts the liquidation plan carried by the event', () => {
    const p = extractPlan(ev)
    expect(p).toMatchObject({ plan_id: 'p1', account_id: 'ACC3', trigger_alert_key: 'k', status: 'PENDING_CONFIRM' })
    expect(p?.steps).toHaveLength(1)
    expect(extractPlan({ ...ev, detail: {} })).toBeNull()
  })

  it('accepts bare lists and {items} wrappers', () => {
    expect(asItems([1, 2])).toEqual([1, 2])
    expect(asItems({ items: [3] })).toEqual([3])
    expect(asItems({ data: [4] })).toEqual([4])
    expect(asItems({ nope: 1 })).toEqual([])
    expect(asItems(null)).toEqual([])
  })

  it('normalizes settlement history in several shapes', () => {
    const day = { trading_day: '20260924', version: 1, accounts: [], breaker_counts: { ACC3: 1 } }
    expect(normalizeHistory({ days: [day] })?.days[0]?.breakers).toEqual({ ACC3: 1 })
    expect(normalizeHistory([day])?.days).toHaveLength(1)
    expect(normalizeHistory({ items: [{ trading_day: 'x' }] })?.days).toHaveLength(0)
  })

  it('rejects compare payloads of an unexpected shape (client-side fallback)', () => {
    expect(normalizeCompare({ matrix: [] })).toBeNull()
    expect(normalizeCompare({ accounts: ['ACC1'], rules: ['daily_loss'], matrix: [], exposure: { products: [], cells: [], firm_net: {} } })?.accounts).toEqual(['ACC1'])
  })
})
