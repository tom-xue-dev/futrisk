/**
 * Contract tests against answers captured from the real api-gateway, in
 * tests/fixtures/gateway/<path>.json with the raw response head in <path>.h:
 *  - the first captures (only the gateway running: shards, md-gateway, clearing-worker and
 *    supervisor down);
 *  - `live1_*`: the landed gateway with risk-engine-1 up (ACC3 / ACC4), risk-engine-0,
 *    md-gateway, sim-exchange, trade-gateway and supervisor down, after a kill-switch drill.
 *
 * 1. The REST client, the normalizers and the stores read every captured answer (OK,
 *    partial, fan-out HTTP 200 with ok=false, UNAVAILABLE with Retry-After, NOT_FOUND,
 *    `X-Result-Code`).
 * 2. The mock answers the same endpoints with the same shape: every key of the real
 *    answer exists in the mock's (the mock may add fields a degraded capture could not
 *    show, e.g. `state` from the supervisor).
 */
import { readFileSync } from 'node:fs'
import { join } from 'node:path'
import { createPinia, setActivePinia } from 'pinia'
import { afterAll, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest'
import { createEndpoints } from '@/api/endpoints'
import { ApiClient, ApiError, type TransportRequest, type TransportResponse } from '@/api/http'
import type { WsClient } from '@/api/ws'
import { MockEngine } from '@/mock/engine'
import { createRestTransport } from '@/mock/rest'
import { setRuntime } from '@/runtime'
import { useAccounts } from '@/stores/accounts'
import { useEvents } from '@/stores/events'
import { isKillSwitchResult, useFirm } from '@/stores/firm'
import { useMarket } from '@/stores/market'
import { useSystem } from '@/stores/system'
import type {
  AccountListItem,
  KillSwitchResult,
  LiquidationPlansResponse,
  PositionsResponse,
  RiskStateRow,
  SystemServicesResponse,
  SystemStatus,
} from '@/types'
import { isAccountSnapshot, isClockState, normalizeCompare, normalizeHistory, processStateStale, stringRecord, toAccountSummary, toServiceHealth } from '@/utils/normalize'

const DIR = join(__dirname, 'fixtures', 'gateway')

function fixture(name: string): { status: number; headers: Record<string, string>; body: string } {
  const head = readFileSync(join(DIR, `${name}.h`), 'utf8').split(/\r?\n/)
  const status = Number(/HTTP\/[\d.]+ (\d+)/.exec(head[0] ?? '')?.[1] ?? 0)
  const headers: Record<string, string> = {}
  for (const line of head.slice(1)) {
    const i = line.indexOf(':')
    if (i > 0) headers[line.slice(0, i).trim().toLowerCase()] = line.slice(i + 1).trim()
  }
  return { status, headers, body: readFileSync(join(DIR, `${name}.json`), 'utf8') }
}

/** An ApiClient answering every request with one captured fixture. */
function clientFor(name: string): ApiClient {
  const f = fixture(name)
  return new ApiClient({
    getKey: () => 'k',
    transport: async (): Promise<TransportResponse> => ({
      status: f.status,
      headers: { get: (h: string) => f.headers[h.toLowerCase()] ?? null },
      text: async () => f.body,
      blob: async () => new Blob([f.body]),
    }),
  })
}

function envelopeOf(name: string): { ok: boolean; code: string; message: string; data: unknown } {
  return JSON.parse(fixture(name).body) as { ok: boolean; code: string; message: string; data: unknown }
}

function keys(x: unknown): string[] {
  return x && typeof x === 'object' && !Array.isArray(x) ? Object.keys(x).sort() : []
}

function expectKeysCovered(real: unknown, mock: unknown, where: string): void {
  const missing = keys(real).filter((k) => !keys(mock).includes(k))
  expect(missing, `${where}: keys of the real answer missing from the mock`).toEqual([])
}

describe('real gateway answers through the client and normalizers', () => {
  it('every captured envelope carries X-Result-Code = code', () => {
    for (const name of ['health', 'auth_me', 'system_status', 'system_services', 'risk_events', 'risk_compare_history_days_20', 'accounts_ACC3', 'accounts_ACC9', 'settlement_days_limit_5']) {
      const f = fixture(name)
      expect(f.headers['x-result-code'], name).toBe(JSON.parse(f.body).code)
    }
  })

  it('reads a partial OK answer with its message (risk events, settlement days)', async () => {
    const ev = await clientFor('risk_events').getFull<{ trading_day: string; items: unknown[]; errors: Record<string, string> }>('/risk/events')
    expect(ev.message).toMatch(/^partial: /)
    expect(ev.data.errors).toEqual({ 'risk-engine-0': 'UNAVAILABLE', 'risk-engine-1': 'UNAVAILABLE' })
    expect(ev.resultCode).toBe('OK')
    const days = await clientFor('settlement_days_limit_5').getFull<{ items: unknown[]; errors: Record<string, string> }>('/settlement/days')
    expect(Object.values(days.data.errors).every((c) => c === 'UNAVAILABLE')).toBe(true)
  })

  it('turns UNAVAILABLE / NOT_FOUND envelopes into ApiErrors with the gateway message', async () => {
    for (const [name, code, status] of [
      ['accounts_ACC3', 'UNAVAILABLE', 503],
      ['accounts_ACC3_orders', 'UNAVAILABLE', 503],
      ['market_feeds', 'UNAVAILABLE', 503],
      ['reports', 'UNAVAILABLE', 503],
      ['accounts_ACC9', 'NOT_FOUND', 404],
    ] as const) {
      const err = await clientFor(name).get('/x').catch((e: unknown) => e)
      expect(err, name).toBeInstanceOf(ApiError)
      expect((err as ApiError).code).toBe(code)
      expect((err as ApiError).status).toBe(status)
      expect((err as ApiError).message).toBe(envelopeOf(name).message)
    }
  })

  it('keeps the per-day errors of a failed settlement history', async () => {
    const err = (await clientFor('risk_compare_history_days_20').get('/risk/compare/history').catch((e: unknown) => e)) as ApiError
    expect(err.code).toBe('UNAVAILABLE')
    const errors = stringRecord((err.data as { errors?: unknown }).errors)
    expect(Object.keys(errors)).toHaveLength(20)
    expect(normalizeHistory({ days: [], errors })?.errors).toEqual(errors)
  })

  it('reads /system/services items with and without supervisor / heartbeat data', () => {
    const data = envelopeOf('system_services').data as SystemServicesResponse
    expect(data.supervisor.ok).toBe(false)
    const rows = data.items.map((x) => toServiceHealth(x, null, 'rest'))
    const gw = rows.find((r) => r?.instance === 'api-gateway')
    expect(gw).toMatchObject({ liveness: 'UP', heartbeat_state: 'DEGRADED', process_state: null })
    expect(gw?.config_hash).toBeTruthy()
    const shard = rows.find((r) => r?.instance === 'risk-engine-1')
    expect(shard).toMatchObject({ liveness: 'DOWN', process_state: null, heartbeat_state: null, service: 'risk-engine' })
  })

  it('reads /system/status and the empty compare view', () => {
    const st = envelopeOf('system_status').data as Record<string, unknown>
    expect(isClockState(st.clock)).toBe(true)
    expect(st.kill_switch).toBeNull()
    const cmp = normalizeCompare(envelopeOf('risk_compare').data)
    expect(cmp?.accounts).toEqual([])
    expect(cmp?.rules.length).toBe(11)
  })
})

describe('mock answers have the real shapes', () => {
  let engine: MockEngine
  let api: ApiClient

  beforeAll(() => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2026-09-25T02:00:00Z'))
    engine = new MockEngine(() => undefined)
    const transport = createRestTransport(engine)
    api = new ApiClient({ getKey: () => 'k', transport, timeoutMs: 60_000 })
  })

  afterAll(() => {
    engine.stop()
    vi.useRealTimers()
  })

  async function mockFull<T>(path: string): Promise<{ ok: boolean; data: T; message: string }> {
    const p = api.getFull<T>(path)
    await vi.advanceTimersByTimeAsync(200)
    return { ok: true, ...(await p) }
  }

  it('health, auth/me, system/status, system/services', async () => {
    for (const [path, name] of [
      ['/health', 'health'],
      ['/auth/me', 'auth_me'],
      ['/system/status', 'system_status'],
    ] as const) {
      const real = envelopeOf(name).data
      const mock = (await mockFull(path)).data
      expectKeysCovered(real, mock, path)
    }
    const real = envelopeOf('system_services').data as SystemServicesResponse
    const mock = (await mockFull<SystemServicesResponse>('/system/services')).data
    expectKeysCovered(real, mock, '/system/services')
    expectKeysCovered(real.supervisor, mock.supervisor, 'supervisor')
    const realGw = real.items.find((i) => i.instance === 'api-gateway')
    const mockGw = mock.items.find((i) => i.instance === 'api-gateway')
    expectKeysCovered(realGw, mockGw, 'service item')
    expectKeysCovered(realGw?.heartbeat, mockGw?.heartbeat, 'heartbeat')
  })

  it('risk rules, events, compare, positions, settlement days', async () => {
    const rules = envelopeOf('risk_rules').data as { rules: Record<string, unknown>[] }
    const mockRules = (await mockFull<{ rules: Record<string, unknown>[] }>('/risk/rules')).data
    expectKeysCovered(rules, mockRules, '/risk/rules')
    expect(mockRules.rules.map((r) => r.id)).toEqual(rules.rules.map((r) => r.id))
    rules.rules.forEach((r, i) => expectKeysCovered(r, mockRules.rules[i], `rule ${String(r.id)}`))
    for (const [path, name] of [
      ['/risk/events', 'risk_events'],
      ['/risk/compare', 'risk_compare'],
      ['/positions?group_by=product', 'positions_group_by_product'],
      ['/settlement/days?limit=5', 'settlement_days_limit_5'],
    ] as const) {
      expectKeysCovered(envelopeOf(name).data, (await mockFull(path)).data, path)
    }
  })

  it('an unknown account is NOT_FOUND with the gateway message', async () => {
    const p = api.get('/accounts/ACC9').catch((e: unknown) => e)
    await vi.advanceTimersByTimeAsync(200)
    const err = (await p) as ApiError
    expect(err.code).toBe('NOT_FOUND')
    expect(err.message).toBe(envelopeOf('accounts_ACC9').message)
  })

  it('a killed clearing-worker makes history / reports UNAVAILABLE like the capture', async () => {
    engine.killService('clearing-worker', 'test')
    await vi.advanceTimersByTimeAsync(500)
    const p = api.get('/risk/compare/history?days=20').catch((e: unknown) => e)
    const r = api.get('/reports').catch((e: unknown) => e)
    await vi.advanceTimersByTimeAsync(200)
    const err = (await p) as ApiError
    expect(err.code).toBe('UNAVAILABLE')
    expectKeysCovered(envelopeOf('risk_compare_history_days_20').data, err.data, 'history failure data')
    expect(((await r) as ApiError).message).toBe(envelopeOf('reports').message)
  })
})

// ---------------------------------------------------------------- live1: the landed gateway

/** A transport answering each path with its captured fixture (`/api/v1/accounts` -> live1_accounts). */
function fixtureTransport(map: Record<string, string>) {
  return async (req: TransportRequest): Promise<TransportResponse> => {
    const path = new URL(req.url, 'http://x').pathname.replace(/^\/api\/v1/, '')
    const name = map[`${req.method} ${path}`]
    if (!name) throw new Error(`no fixture for ${req.method} ${path}`)
    const f = fixture(name)
    return {
      status: f.status,
      headers: { get: (h: string) => f.headers[h.toLowerCase()] ?? null },
      text: async () => f.body,
      blob: async () => new Blob([f.body]),
    }
  }
}

const LIVE1: Record<string, string> = {
  'GET /accounts': 'live1_accounts',
  'GET /demo/scenarios': 'live1_demo_scenarios',
  'GET /demo/state': 'live1_demo_state',
  'GET /health': 'live1_health',
  'GET /market/feeds': 'live1_market_feeds',
  'GET /positions': 'live1_positions_group_by_product',
  'GET /risk/compare': 'live1_risk_compare',
  'POST /risk/firm/kill-switch': 'live1_risk_firm_kill-switch',
  'GET /risk/liquidation-plans': 'live1_risk_liquidation-plans',
  'GET /risk/state': 'live1_risk_state',
  'GET /system/services': 'live1_system_services',
  'GET /system/status': 'live1_system_status',
}

describe('live1 captures through the client, normalizers and stores', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
    const client = new ApiClient({ getKey: () => 'k', transport: fixtureTransport(LIVE1) })
    setRuntime({ api: createEndpoints(client), ws: null as unknown as WsClient, mock: null, isMock: false })
  })

  afterAll(() => setRuntime(null))

  it('every live1 envelope carries X-Result-Code = code and nosniff', () => {
    for (const name of Object.values(LIVE1)) {
      const f = fixture(name)
      expect(f.headers['x-result-code'], name).toBe(JSON.parse(f.body).code)
      expect(f.headers['x-content-type-options'], name).toBe('nosniff')
    }
  })

  it('GET /accounts lists every configured account; a missing one has no data but a liveness', async () => {
    const acc = useAccounts()
    await acc.loadAll()
    expect(acc.configured).toEqual(['ACC1', 'ACC2', 'ACC3', 'ACC4', 'ACC5'])
    expect(Object.keys(acc.missing).sort()).toEqual(['ACC1', 'ACC2', 'ACC5'])
    expect(acc.withoutData).toEqual(['ACC1', 'ACC2', 'ACC5'])
    expect(Object.keys(acc.snapshots).sort()).toEqual(['ACC3', 'ACC4'])
    expect(acc.staleInfo('ACC3')).toEqual({ stale: false, age_ms: 30 })
    // no snapshot is not "stale data": the missing ACC1 (shard 0 down) is listed as without data
    expect(acc.staleIds).toEqual([])
    const items = envelopeOf('live1_accounts').data as AccountListItem[]
    expect(items.filter((x) => x.missing).map((x) => toAccountSummary(x))).toEqual([null, null, null])
  })

  it('the kill switch fan-out answers HTTP 200 with ok=false, retrying and failed', async () => {
    const out = await useFirm().setKillSwitch(true, 'drill', '0192f3a1-7b2c-7d3e-8f40-9a1b2c3d4e5f')
    expect(out.code).toBe('UNAVAILABLE')
    expect(out.message).toBe('1/3 targets confirmed; failed: risk-engine-0=UNAVAILABLE, trade-gateway=UNAVAILABLE')
    expect(out.result.all_ok).toBe(false)
    expect(out.result.retrying).toEqual(['risk-engine-0', 'trade-gateway'])
    expect(out.result.failed).toEqual([])
    expect(out.result.control?.action).toBe('KILL_SWITCH_ON')
    const err = (await new ApiClient({ getKey: () => 'k', transport: fixtureTransport(LIVE1) }).post('/risk/firm/kill-switch', { on: true, reason: 'drill' }).catch((e: unknown) => e)) as ApiError
    expect(err.status).toBe(200)
    expect(isKillSwitchResult(err.data)).toBe(true)
  })

  it('/system/status: kill-switch retry and control, auth, notifications, read-model staleness', async () => {
    const sys = useSystem()
    await sys.loadStatus()
    const st = sys.status as SystemStatus
    expect(st.kill_switch_retry).toMatchObject({ pending: ['risk-engine-0', 'trade-gateway'], failed: [], desired: { on: true, actor: 'bootstrap' }, retrying: true })
    expect(sys.killSwitchControl).toMatchObject({ desired_on: true, confirmed: ['risk-engine-1'], pending: ['risk-engine-0', 'trade-gateway'], retrying: true })
    expect(sys.killSwitchControl?.targets['risk-engine-0']).toMatchObject({ status: 'PENDING', code: 'UNAVAILABLE', attempts: 1 })
    expect(st.auth).toMatchObject({ bootstrap_active: true, admin_unavailable: [], throttled: 0 })
    expect(st.notifications).toMatchObject({ failed: 0, suppressed: 0, pending: 0 })
    expect(st.readmodel.accounts_stale).toEqual([])
    // no FirmSnapshot and no firm control state: the kill switch is unknown
    expect(useFirm().view.kill_switch).toBeNull()
    expect(useFirm().view.control).toBeNull()
  })

  it('/system/services items carry since_ns / heartbeat_ns (supervisor down)', async () => {
    const sys = useSystem()
    await sys.loadServices()
    expect(sys.supervisor).toMatchObject({ ok: false, code: 'UNAVAILABLE' })
    const shard = sys.services['risk-engine-1']
    expect(shard).toMatchObject({ liveness: 'UP', heartbeat_state: 'RUNNING', process_state: null, since_ns: null })
    expect(typeof shard?.heartbeat_ns).toBe('number')
    expect(sys.services['md-gateway']).toMatchObject({ liveness: 'DOWN', since_ns: null, heartbeat_ns: null })
    expect(Object.values(sys.services).some((x) => processStateStale(x))).toBe(false)
  })

  it('liquidation plans: partial answer with the shards that did not answer', async () => {
    const ev = useEvents()
    await ev.loadPlans()
    expect(ev.planErrors).toEqual({ 'risk-engine-0': 'UNAVAILABLE' })
    expect(ev.plansFor('ACC3').map((p) => [p.plan_id, p.status, p.stale ?? false])).toEqual([['LP-20260928-ACC3-1', 'PENDING_CONFIRM', false]])
  })

  it('503 answers carry Retry-After (md-gateway / sim-exchange down)', async () => {
    const market = useMarket()
    await market.loadFeeds().catch(() => undefined)
    expect(market.feedsError).toMatchObject({ code: 'UNAVAILABLE', message: 'md-gateway: peer is down (no heartbeat)', retryAfterMs: 1000 })
    for (const path of ['/demo/state', '/demo/scenarios']) {
      const e = (await new ApiClient({ getKey: () => 'k', transport: fixtureTransport(LIVE1) }).get(path).catch((x: unknown) => x)) as ApiError
      expect(e.status, path).toBe(503)
      expect(e.retryAfterMs, path).toBe(1000)
      expect(e.message, path).toBe('sim-exchange control unreachable: All connection attempts failed')
    }
  })

  it('compare, positions, risk state and health in the landed shapes', () => {
    const cmp = normalizeCompare(envelopeOf('live1_risk_compare').data)
    expect(cmp?.accounts).toEqual(['ACC3', 'ACC4'])
    expect(cmp?.accounts_stale).toEqual([])
    expect(cmp?.liveness?.ACC3?.stale).toBe(false)
    const pos = envelopeOf('live1_positions_group_by_product').data as PositionsResponse
    expect(pos).toMatchObject({ accounts_missing: ['ACC1', 'ACC2', 'ACC5'], accounts_stale: [] })
    const rows = envelopeOf('live1_risk_state').data as RiskStateRow[]
    expect(rows.map((r) => [r.account_id, r.stale, r.age_ms])).toEqual([
      ['ACC3', false, 62],
      ['ACC4', false, 62],
    ])
    expect(envelopeOf('live1_health').data).toMatchObject({ status: 'ok', home_id: expect.any(String), pid: expect.any(Number) })
    const acc3 = (envelopeOf('live1_accounts').data as AccountListItem[]).find((x) => x.account_id === 'ACC3')
    expect(isAccountSnapshot(acc3)).toBe(true)
  })
})

describe('mock answers have the live1 shapes', () => {
  let engine: MockEngine
  let api: ApiClient

  beforeAll(() => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2026-09-25T02:00:00Z'))
    engine = new MockEngine(() => undefined)
    api = new ApiClient({ getKey: () => 'k', transport: createRestTransport(engine), timeoutMs: 60_000 })
  })

  afterAll(() => {
    engine.stop()
    vi.useRealTimers()
  })

  async function mockGet<T>(path: string): Promise<T> {
    const p = api.get<T>(path)
    await vi.advanceTimersByTimeAsync(200)
    return p
  }

  async function mockError(p: Promise<unknown>): Promise<ApiError> {
    const e = p.catch((x: unknown) => x)
    await vi.advanceTimersByTimeAsync(200)
    const err = await e
    expect(err).toBeInstanceOf(ApiError)
    return err as ApiError
  }

  it('/system/status, /system/services, /health', async () => {
    const real = envelopeOf('live1_system_status').data as SystemStatus
    const mock = await mockGet<SystemStatus>('/system/status')
    expectKeysCovered(real, mock, '/system/status')
    for (const k of ['kill_switch_retry', 'auth', 'notifications', 'readmodel', 'ws'] as const) expectKeysCovered(real[k], mock[k], `status.${k}`)
    const services = envelopeOf('live1_system_services').data as SystemServicesResponse
    const mockServices = await mockGet<SystemServicesResponse>('/system/services')
    for (const inst of ['risk-engine-1', 'md-gateway']) {
      expectKeysCovered(services.items.find((i) => i.instance === inst), mockServices.items.find((i) => i.instance === inst), `service ${inst}`)
    }
    expectKeysCovered(envelopeOf('live1_health').data, await mockGet('/health'), '/health')
  })

  it('/accounts (snapshot and missing items), /risk/state, /risk/compare, /positions', async () => {
    const real = envelopeOf('live1_accounts').data as AccountListItem[]
    // make ACC5 an account without a snapshot for a moment
    const accts = (engine as unknown as { accts: Map<string, { snapshot: unknown }> }).accts
    const acc5 = accts.get('ACC5')
    const saved = acc5?.snapshot
    if (acc5) acc5.snapshot = null
    const mock = await mockGet<AccountListItem[]>('/accounts')
    if (acc5) acc5.snapshot = saved
    expectKeysCovered(real.find((x) => x.account_id === 'ACC3'), mock.find((x) => x.account_id === 'ACC3'), 'account snapshot item')
    expectKeysCovered(real.find((x) => x.account_id === 'ACC5'), mock.find((x) => x.account_id === 'ACC5'), 'missing account item')
    expect(mock.find((x) => x.account_id === 'ACC5')?.missing).toBe(true)
    const rows = envelopeOf('live1_risk_state').data as RiskStateRow[]
    expectKeysCovered(rows[0], (await mockGet<RiskStateRow[]>('/risk/state?account=ACC3'))[0], '/risk/state row')
    expectKeysCovered(envelopeOf('live1_risk_compare').data, await mockGet('/risk/compare'), '/risk/compare')
    expectKeysCovered(envelopeOf('live1_positions_group_by_product').data, await mockGet('/positions?group_by=product'), '/positions')
  })

  it('kill switch: HTTP 200 fan-out with the same data keys', async () => {
    engine.killService('trade-gateway', 'test')
    await vi.advanceTimersByTimeAsync(100)
    const err = await mockError(api.post('/risk/firm/kill-switch', { on: true, reason: 'drill' }))
    const real = envelopeOf('live1_risk_firm_kill-switch').data as KillSwitchResult
    expect(err.status).toBe(200)
    expectKeysCovered(real, err.data, 'kill switch data')
    expectKeysCovered(real.targets[0], (err.data as KillSwitchResult).targets[0], 'kill switch target')
    const st = await mockGet<SystemStatus>('/system/status')
    const realSt = envelopeOf('live1_system_status').data as SystemStatus
    expectKeysCovered(realSt.kill_switch_control, st.kill_switch_control, 'kill_switch_control')
    expectKeysCovered(realSt.kill_switch_control?.targets['risk-engine-0'], st.kill_switch_control?.targets['trade-gateway'], 'kill_switch_control target')
    await vi.advanceTimersByTimeAsync(15_000)
  })

  it('liquidation plans and the 503 answers of md-gateway / sim-exchange', async () => {
    const real = envelopeOf('live1_risk_liquidation-plans').data as LiquidationPlansResponse
    const mock = await mockGet<LiquidationPlansResponse>('/risk/liquidation-plans')
    expectKeysCovered(real, mock, '/risk/liquidation-plans')
    engine.killService('md-gateway', 'test')
    engine.killService('sim-exchange', 'test')
    await vi.advanceTimersByTimeAsync(100)
    for (const [path, name] of [
      ['/market/feeds', 'live1_market_feeds'],
      ['/demo/state', 'live1_demo_state'],
      ['/demo/scenarios', 'live1_demo_scenarios'],
    ] as const) {
      const e = await mockError(api.get(path))
      expect(e.status, path).toBe(503)
      expect(e.retryAfterMs, path).toBe(1000)
      expect(e.message, path).toBe(envelopeOf(name).message)
    }
  })
})
