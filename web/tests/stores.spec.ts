/**
 * Store rules on WS frames of the landed gateway: EOD warnings (WARN events and
 * `detail.warnings`, deduplicated, toasted only for new event frames), the firm control
 * layers (MANUAL / RULE_STATE), kill-switch progress events of the `system` topic, the
 * `backtests` state topic, liquidation-plan decision events.
 */
import { createPinia, setActivePinia } from 'pinia'
import { beforeEach, describe, expect, it } from 'vitest'
import { setEodWarningNotifier, useEod } from '@/stores/eod'
import { useEvents } from '@/stores/events'
import { useFirm } from '@/stores/firm'
import { setSystemAlertNotifier, useSystem } from '@/stores/system'
import type { EodWarning, KillSwitchRow, RiskEvent, WsServerMessage } from '@/types'

const T = 1_790_560_800_000_000_000

function frame(topic: string, type: WsServerMessage['type'], data: unknown): WsServerMessage {
  return { v: 1, type, topic, seq: 1, ts: 0, mts: 0, td: '20260928', data } as WsServerMessage
}

beforeEach(() => setActivePinia(createPinia()))

describe('EOD warnings', () => {
  it('records WARN events without a phase change and toasts only new event frames', () => {
    const eod = useEod()
    const toasts: EodWarning[] = []
    setEodWarningNotifier((w) => toasts.push(w))
    const done = { trading_day: '20260928', version: 1, phase: 'DONE', ts_ns: T }
    const warn = { ...done, ts_ns: T + 5, detail: { warning: 'ACC2 late trade', kind: 'LATE_FACT', warn_id: 'w1', account_id: 'ACC2' } }
    const fixed = { trading_day: '20260928', version: 1, phase: 'PRICES_FIXED', ts_ns: T - 5, detail: { warnings: [{ kind: 'PRE_SETTLE_MISMATCH', message: 'j2701 SOD 1680 / md 1681' }] } }
    // snapshot replay: recorded, no toast
    eod.applyFrame(frame('eod', 'snapshot', [fixed, done]))
    expect(toasts).toHaveLength(0)
    expect(eod.warnings['20260928']?.map((w) => w.kind)).toEqual(['PRE_SETTLE_MISMATCH'])
    // live WARN event: toast once, the phase stays DONE
    eod.applyFrame(frame('eod', 'event', warn))
    eod.applyFrame(frame('eod', 'event', warn))
    expect(toasts.map((w) => w.kind)).toEqual(['LATE_FACT'])
    expect(eod.phaseOf('20260928')?.phase).toBe('DONE')
    expect(eod.phases['20260928']?.ts_ns).toBe(T)
    // the status answer lists the same warnings as {kind, message}: no duplicates
    eod.ingest({ ...done, detail: { warning_count: 2, warnings: [{ kind: 'PRE_SETTLE_MISMATCH', message: 'j2701 SOD 1680 / md 1681' }, { kind: 'LATE_FACT', message: 'ACC2 late trade' }] } })
    expect(eod.warnings['20260928']).toHaveLength(2)
    expect(eod.warningCount('20260928')).toBe(2)
    setEodWarningNotifier(null)
  })

  it('takes the days list warning_count when the warnings themselves are not loaded', () => {
    const eod = useEod()
    eod.dayItems['20260922'] = { trading_day: '20260922', version: 1, phase: 'DONE', warning_count: 3 }
    expect(eod.warningCount('20260922')).toBe(3)
  })

  it('replaces the backtest list on a snapshot and upserts single runs', () => {
    const eod = useEod()
    eod.applyBacktestsFrame(frame('backtests', 'snapshot', [{ run_id: 'b', name: 'x', state: 'RUNNING', progress: 0.2, submitted_ns: 2 }, { run_id: 'a', name: 'y', state: 'DONE', progress: 1, submitted_ns: 1 }]))
    eod.applyBacktestsFrame(frame('backtests', 'upsert', { run_id: 'b', name: 'x', state: 'RUNNING', progress: 0.6, submitted_ns: 2 }))
    expect(eod.backtestList.map((b) => [b.run_id, b.progress])).toEqual([
      ['b', 0.6],
      ['a', 1],
    ])
    eod.applyBacktestsFrame(frame('backtests', 'snapshot', []))
    expect(eod.backtestList).toEqual([])
  })
})

describe('firm control layers', () => {
  it('keeps MANUAL and RULE apart: effective = manual OR rule', () => {
    const firm = useFirm()
    firm.applyFrame(frame('firm', 'snapshot', { trading_day: '20260928', as_of_ns: T, equity: 1, daily_pnl: 0, daily_pnl_pct_init: 0, margin: 0, risk_degree: 0, gross_notional: 0, net_notional: 0, accounts_by_state: {}, shards: [], control: null }))
    expect(firm.view.kill_switch).toBe(false)
    firm.applyFrame(frame('firm', 'event', { action: 'RULE_STATE', origin: 'RULE', active: true, restrictions: [{ kind: 'NO_OPEN' }], trading_day: '20260928', reason: 'firm daily loss', ts_ns: T + 1 }))
    expect(firm.view.kill_switch).toBe(true)
    expect(firm.view.control?.rule).toMatchObject({ active: true, reason: 'firm daily loss' })
    firm.applyFrame(frame('firm', 'event', { action: 'KILL_SWITCH_OFF', origin: 'MANUAL', active: false, trading_day: '20260928', actor: 'rm', reason: 'drill over', ts_ns: T + 2 }))
    // the manual layer is off, the rule layer still holds the kill switch
    expect(firm.view.control?.manual).toMatchObject({ active: false, actor: 'rm' })
    expect(firm.view.kill_switch).toBe(true)
    firm.applyFrame(frame('firm', 'event', { action: 'RULE_STATE', origin: 'RULE', active: false, restrictions: [], trading_day: '20260928', ts_ns: T + 3 }))
    expect(firm.view.kill_switch).toBe(false)
  })
})

describe('system events', () => {
  const row: KillSwitchRow = {
    desired_on: true,
    command_id: 'c1',
    reason: 'drill',
    actor: 'rm',
    role: 'RISK_MANAGER',
    requested_ns: T,
    updated_ns: T,
    targets: {
      'risk-engine-0': { status: 'CONFIRMED', code: 'OK', message: '', attempts: 1, updated_ns: T },
      'trade-gateway': { status: 'PENDING', code: 'UNAVAILABLE', message: 'down', attempts: 1, updated_ns: T },
    },
    pending: ['trade-gateway'],
    failed: [],
    confirmed: ['risk-engine-0'],
  }

  it('follows kill-switch progress events and alerts on a target that failed', () => {
    const sys = useSystem()
    const alerts: unknown[] = []
    setSystemAlertNotifier((ev) => alerts.push(ev))
    sys.applyFrame(frame('system', 'event', { kind: 'kill_switch', event: 'command', ...row }))
    expect(sys.killSwitchControl).toMatchObject({ command_id: 'c1', pending: ['trade-gateway'], retrying: true })
    const failed = { ...row, updated_ns: T + 60e9, targets: { ...row.targets, 'trade-gateway': { status: 'FAILED' as const, code: 'UNAVAILABLE', message: 'down', attempts: 30, updated_ns: T + 60e9 } }, pending: [], failed: ['trade-gateway'] }
    sys.applyFrame(frame('system', 'event', { kind: 'kill_switch', event: 'retry', ...failed }))
    sys.applyFrame(frame('system', 'event', { kind: 'kill_switch', event: 'retry', ...failed }))
    expect(alerts).toHaveLength(1)
    expect(sys.killSwitchControl).toMatchObject({ failed: ['trade-gateway'], retrying: false })
    sys.applyFrame(frame('system', 'event', { kind: 'config', event: 'CONFIG_FAILED', config_version: 4, lagging: ['risk-engine-1'] }))
    expect(sys.configEvents[0]).toMatchObject({ event: 'CONFIG_FAILED', config_version: 4 })
    expect(alerts).toHaveLength(2)
    setSystemAlertNotifier(null)
  })

  it('a service event older than the known state is history only', () => {
    const sys = useSystem()
    sys.upsertService({ instance: 'md-gateway', service: 'md-gateway', restarts: 1, process_state: 'RUNNING', state: 'RUNNING', since_ns: T, heartbeat_ns: T + 1e9, liveness: 'UP' }, 'rest')
    sys.ingestEvent({ instance: 'md-gateway', service: 'md-gateway', from_state: 'RUNNING', to_state: 'EXITED', reason: 'EXITED', restarts: 0, trading_day: '20260928', ts_ns: T - 5e9 })
    expect(sys.services['md-gateway']?.process_state).toBe('RUNNING')
    sys.ingestEvent({ instance: 'md-gateway', service: 'md-gateway', from_state: 'RUNNING', to_state: 'EXITED', reason: 'KILLED_BY_DEMO', restarts: 1, trading_day: '20260928', ts_ns: T + 2e9 })
    expect(sys.services['md-gateway']).toMatchObject({ process_state: 'EXITED', since_ns: T + 2e9 })
    expect(sys.events).toHaveLength(2)
  })
})

describe('liquidation plans', () => {
  it('applies a decision event to a known plan and never reverts a decided plan', () => {
    const ev = useEvents()
    const plan = { plan_id: 'p1', account_id: 'ACC3', trading_day: '20260928', trigger_alert_key: 'k', status: 'PENDING_CONFIRM' as const, steps: [], created_ns: T }
    ev.upsertPlan(plan)
    const decision: RiskEvent = { event_id: 'e2', kind: 'LIQUIDATION_PLAN', trading_day: '20260928', ts_ns: T + 9, account_id: 'ACC3', actor: 'rm', detail: { plan_id: 'p1', status: 'CONFIRMED', actor: 'rm' } }
    ev.ingest(decision, true)
    expect(ev.plans.p1).toMatchObject({ status: 'CONFIRMED', confirmed_by: 'rm', confirmed_ns: T + 9 })
    ev.upsertPlan(plan)
    expect(ev.plans.p1?.status).toBe('CONFIRMED')
  })
})
