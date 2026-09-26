/**
 * The mock backend behaves like the api-gateway where the dashboard depends on it:
 * idempotency (replay with `duplicate`, 409 on key reuse, UUID keys, retryable answers not
 * recorded except for record-all routes), the kill-switch manager (HTTP 200 fan-out,
 * per-target PENDING / CONFIRMED, retries), 404 / 405 envelopes, reset with nothing
 * latched, demo endpoints (403 outside demo mode, sim state / faults / scenarios), omitted
 * msgspec defaults, WS `system` items with `kind`, WS errors per `sub` op, owner liveness,
 * every configured account in GET /accounts, liquidation plans of a down shard, EOD warnings.
 */
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest'
import { ApiClient, ApiError } from '@/api/http'
import { MockEngine } from '@/mock/engine'
import { MockHub, MockSocket } from '@/mock/hub'
import { createRestTransport } from '@/mock/rest'
import { wire } from '@/mock/wire'
import type {
  AccountListItem,
  DemoFaultResult,
  DemoFaultsResponse,
  DemoScenarioInfo,
  DemoState,
  KillSwitchResult,
  LiquidationPlansResponse,
  ResetAck,
  ScenarioRunState,
  SettlementDaysResponse,
  SystemStatus,
} from '@/types'

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

/** Resolve a mock request (the transport waits a few ms of fake time). */
async function settle<T>(p: Promise<T>): Promise<T> {
  await vi.advanceTimersByTimeAsync(200)
  return p
}

async function failed(p: Promise<unknown>): Promise<ApiError> {
  const e = await settle(p.catch((x: unknown) => x))
  expect(e).toBeInstanceOf(ApiError)
  return e as ApiError
}

const KEY = '0192f3a1-7b2c-7d3e-8f40-9a1b2c3d4e5f'

describe('idempotency like gateway.mutate', () => {
  it('replays the same request with duplicate: true and rejects another body under the key', async () => {
    const first = await settle(api.postFull<ResetAck>('/risk/accounts/ACC2/reset', { reason: 'test' }, { idempotencyKey: KEY }))
    expect(first.duplicate).toBe(false)
    expect(first.message).toBe('0 latched level(s) reset')
    const again = await settle(api.postFull<ResetAck>('/risk/accounts/ACC2/reset', { reason: 'test' }, { idempotencyKey: KEY }))
    expect(again.duplicate).toBe(true)
    expect(again.data).toEqual(first.data)
    const other = await failed(api.post('/risk/accounts/ACC2/reset', { reason: 'other' }, { idempotencyKey: KEY }))
    expect(other.code).toBe('CONFLICT')
    expect(other.status).toBe(409)
  })

  it('requires a UUID Idempotency-Key', async () => {
    const e = await failed(api.post('/risk/accounts/ACC2/halt', { reason: 'x' }, { idempotencyKey: 'not-a-uuid' }))
    expect(e.code).toBe('BAD_REQUEST')
    expect(e.message).toBe('Idempotency-Key must be a UUID')
  })

  it('does not record retryable answers: a retry with the same key runs again', async () => {
    engine.killService('risk-engine-1', 'test')
    await vi.advanceTimersByTimeAsync(100)
    const key = '0192f3a1-7b2c-7d3e-8f40-000000000001'
    const e = await failed(api.post('/risk/accounts/ACC4/halt', { reason: 'r' }, { idempotencyKey: key }))
    expect(e.code).toBe('UNAVAILABLE')
    expect(e.message).toBe('risk-engine-1: peer is down (no heartbeat)')
    // the account is served stale from the read model meanwhile
    const acc = await settle(api.get<{ stale?: boolean }>('/accounts/ACC4'))
    expect(acc.stale).toBe(true)
    await vi.advanceTimersByTimeAsync(15_000) // supervisor restarts the shard (mock: ~12 s)
    const ok = await settle(api.postFull('/risk/accounts/ACC4/halt', { reason: 'r' }, { idempotencyKey: key }))
    expect(ok.duplicate).toBe(false)
  })
})

describe('kill switch fan-out', () => {
  const key = '0192f3a1-7b2c-7d3e-8f40-000000000002'

  it('answers ok=false with per-target results and HTTP 200 when a target is down', async () => {
    engine.killService('trade-gateway', 'test')
    await vi.advanceTimersByTimeAsync(100)
    const e = await failed(api.post('/risk/firm/kill-switch', { on: true, reason: 'drill' }, { idempotencyKey: key }))
    expect(e.status).toBe(200)
    expect(e.code).toBe('UNAVAILABLE')
    expect(e.message).toBe('2/3 targets confirmed; failed: trade-gateway=UNAVAILABLE')
    const r = e.data as KillSwitchResult
    expect(r.all_ok).toBe(false)
    expect(r.retrying).toEqual(['trade-gateway'])
    expect(r.failed).toEqual([])
    expect(r.targets.map((t) => [t.target, t.ok])).toEqual([
      ['risk-engine-0', true],
      ['risk-engine-1', true],
      ['trade-gateway', false],
    ])
    expect(r.control?.action).toBe('KILL_SWITCH_ON')
    const st = await settle(api.get<SystemStatus>('/system/status'))
    expect(st.kill_switch).toBe(true)
    expect(st.kill_switch_retry).toMatchObject({ pending: ['trade-gateway'], failed: [], desired: { on: true, reason: 'drill', command_id: key }, retrying: true })
    expect(st.kill_switch_control?.targets['trade-gateway']).toMatchObject({ status: 'PENDING', code: 'UNAVAILABLE', attempts: 1 })
    expect(st.kill_switch_control?.confirmed).toEqual(['risk-engine-0', 'risk-engine-1'])
    expect(st.firm_control).toMatchObject({ kill_switch: true, manual: { active: true, reason: 'drill' }, rule: { active: false } })
  })

  it('records every outcome: the same key replays the partial answer', async () => {
    const again = await failed(api.post('/risk/firm/kill-switch', { on: true, reason: 'drill' }, { idempotencyKey: key }))
    expect(again.status).toBe(200)
    expect(again.code).toBe('UNAVAILABLE')
    expect((again.data as KillSwitchResult).retrying).toEqual(['trade-gateway'])
  })

  it('retries the pending target every 2 s until it confirms', async () => {
    await vi.advanceTimersByTimeAsync(15_000) // supervisor restarts trade-gateway, the retry task confirms it
    const st = await settle(api.get<SystemStatus>('/system/status'))
    expect(st.kill_switch_control?.pending).toEqual([])
    expect(st.kill_switch_control?.targets['trade-gateway']?.status).toBe('CONFIRMED')
    expect((st.kill_switch_control?.targets['trade-gateway']?.attempts ?? 0) > 1).toBe(true)
    expect(st.kill_switch_retry.retrying).toBe(false)
    const off = await settle(api.postFull<KillSwitchResult>('/risk/firm/kill-switch', { on: false, reason: 'drill over' }))
    expect(off.data.all_ok).toBe(true)
    expect(off.data.failed).toEqual([])
  })
})

describe('force-unfreeze fan-out', () => {
  it('answers HTTP 200 with ok=false and the worst code when no shard is PROVISIONAL', async () => {
    const e = await failed(api.post('/risk/eod/20260928/force-unfreeze', { reason: 'drill' }))
    expect(e.status).toBe(200)
    expect(e.code).toBe('PRECONDITION_FAILED')
    const r = e.data as { targets: { target: string; ok: boolean; code: string }[]; all_ok: boolean }
    expect(r.all_ok).toBe(false)
    expect(r.targets.map((t) => [t.target, t.code])).toEqual([
      ['risk-engine-0', 'PRECONDITION_FAILED'],
      ['risk-engine-1', 'PRECONDITION_FAILED'],
    ])
  })
})

describe('error envelopes', () => {
  it('answers 404 for an unknown path and 405 for another method of a known path', async () => {
    const nf = await failed(api.get('/nope'))
    expect(nf.status).toBe(404)
    expect(nf.code).toBe('NOT_FOUND')
    expect(nf.message).toBe('unknown API path /api/v1/nope')
    const na = await failed(api.del('/system/status'))
    expect(na.status).toBe(405)
    expect(na.code).toBe('BAD_REQUEST')
    expect(na.message).toBe('method DELETE not allowed; use GET')
  })
})

describe('accounts, plans and EOD warnings', () => {
  it('lists every configured account; the ones without a snapshot are `missing`', async () => {
    const list = await settle(api.get<AccountListItem[]>('/accounts'))
    expect(list.map((x) => x.account_id)).toEqual(['ACC1', 'ACC2', 'ACC3', 'ACC4', 'ACC5'])
    for (const x of list) {
      expect(typeof x.stale).toBe('boolean')
      expect(x.missing).toBe(false)
      expect(typeof x.name).toBe('string')
    }
  })

  it('marks the plans of a shard that does not answer as stale (read model)', async () => {
    const res = await settle(api.getFull<LiquidationPlansResponse>('/risk/liquidation-plans'))
    expect(res.data.errors).toEqual({})
    engine.killService('risk-engine-1', 'test')
    await vi.advanceTimersByTimeAsync(100)
    const down = await settle(api.getFull<LiquidationPlansResponse>('/risk/liquidation-plans', { query: { account: 'ACC3' } }))
    expect(down.data.errors).toEqual({ 'risk-engine-1': 'UNAVAILABLE' })
    expect(down.message).toBe('partial: risk-engine-1=UNAVAILABLE')
    expect(down.data.items.every((p) => p.stale === true)).toBe(true)
    await vi.advanceTimersByTimeAsync(15_000)
  })

  it('reports EOD warnings per day (warning_count, detail.warnings)', async () => {
    const days = await settle(api.getFull<SettlementDaysResponse>('/settlement/days', { query: { limit: 10 } }))
    const warned = days.data.items.filter((d) => d.warning_count > 0)
    expect(warned).toHaveLength(1)
    expect(days.data.items.every((d) => typeof d.warning_count === 'number')).toBe(true)
    const st = await settle(api.get<{ detail: { warning_count?: number; warnings?: { kind: string; message: string }[] } }>(`/settlement/${warned[0]?.trading_day}/status`))
    expect(st.detail.warning_count).toBe(1)
    expect(st.detail.warnings?.[0]).toMatchObject({ kind: 'OFFICIAL_PRICE_DIFF' })
  })
})

describe('demo sim state, faults and scenarios', () => {
  it('lists the scenarios, runs one at a time and stops it', async () => {
    const list = await settle(api.get<DemoScenarioInfo[]>('/demo/scenarios'))
    expect(list.map((x) => x.name)).toEqual(['all_feeds_outage', 'breaker', 'feed_outage'])
    expect(list.every((x) => x.description.length > 0)).toBe(true)
    const started = await settle(api.postFull<{ run_id: string; name: string }>('/demo/scenario', { name: 'feed_outage', speed: 2 }))
    expect(started.data.name).toBe('feed_outage')
    const busy = await failed(api.post('/demo/scenario', { name: 'breaker' }))
    expect(busy.code).toBe('CONFLICT')
    const st = await settle(api.get<DemoState>('/demo/state'))
    expect(st.scenario).toMatchObject({ name: 'feed_outage', status: 'RUNNING', speed: 2, steps_total: 2 })
    expect(st.faults.some((f) => f.type === 'FEED_DISCONNECT')).toBe(true)
    // a second start is refused while one runs (CONFLICT)
    const second = await settle(api.postFull<ScenarioRunState>('/demo/scenario', { name: 'x' }).catch(() => null))
    expect(second).toBeNull()
    const cancel = await settle(api.delFull<ScenarioRunState>('/demo/scenario'))
    expect(cancel.data.status).toBe('CANCELED')
  })

  it('moves deleted faults to the history', async () => {
    const f = await settle(api.postFull<DemoFaultResult>('/demo/faults', { type: 'FEED_LATENCY', target: 'feed_b', params: { ms: 800 } }))
    expect(f.data.state).toBe('ACTIVE')
    await settle(api.delFull(`/demo/faults/${f.data.fault_id}`))
    const faults = await settle(api.get<DemoFaultsResponse>('/demo/faults'))
    expect(faults.items.some((x) => x.fault_id === f.data.fault_id)).toBe(false)
    expect(faults.history.find((x) => x.fault_id === f.data.fault_id)?.state).toBe('DELETED')
  })

  it('records every outcome of a demo mutation, a retryable one included', async () => {
    engine.killService('sim-exchange', 'test')
    await vi.advanceTimersByTimeAsync(100)
    const key = '0192f3a1-7b2c-7d3e-8f40-000000000003'
    const first = await failed(api.post('/demo/shock', { sector: 'black', pct: -0.01 }, { idempotencyKey: key }))
    expect(first.code).toBe('UNAVAILABLE')
    expect(first.retryAfterMs).toBe(1000)
    await vi.advanceTimersByTimeAsync(15_000)
    const replay = await failed(api.post('/demo/shock', { sector: 'black', pct: -0.01 }, { idempotencyKey: key }))
    expect(replay.code).toBe('UNAVAILABLE')
    expect(replay.duplicate).toBe(true)
  })
})

describe('demo endpoints and wire shapes', () => {
  it('answers 403 outside demo mode', async () => {
    engine.appMode = 'live'
    const e = await failed(api.post('/demo/shock', { sector: 'black', pct: -0.01 }))
    expect(e.code).toBe('FORBIDDEN')
    expect(e.message).toBe('demo endpoints are only available in demo mode (app.mode=demo)')
    engine.appMode = 'demo'
  })

  it('omits fields at their msgspec default, recursively', () => {
    const fs = wire('FirmSnapshot', { trading_day: 'd', as_of_ns: 1, equity: 1, daily_pnl: 0, daily_pnl_pct_init: 0, margin: 0, risk_degree: 0, gross_notional: 0, net_notional: 0, accounts_by_state: {}, product_net: [], kill_switch: false, shards: [] })
    expect(Object.keys(fs)).not.toContain('kill_switch')
    expect(Object.keys(fs)).not.toContain('product_net')
    expect(fs.daily_pnl).toBe(0) // required fields stay even when zero
    const ev = wire('MdEvent', { event_id: 'e', kind: 'GAP_FILLED', ts_ns: 1, trading_day: 'd', source: 'FEED_A', gap: { gap_id: 'g', from_ns: 1, status: 'FILLED', filled: 0, duplicates: 0 }, detail: {} })
    expect(ev.gap).toEqual({ gap_id: 'g', from_ns: 1, status: 'FILLED' })
    expect('detail' in ev).toBe(false)
  })

  it('tags the WS system items with kind and appends the sys.clock item', () => {
    const items = engine.snapshotFor('system') as { kind: string; instance: string }[]
    expect(items.every((i) => i.kind === 'service' || i.kind === 'clock')).toBe(true)
    expect(items.at(-1)).toMatchObject({ kind: 'clock', instance: 'sys.clock', mode: 'DEMO' })
  })

  it('carries the control layers in the firm snapshot and lists backtests newest first', () => {
    const firm = engine.snapshotFor('firm') as { control?: unknown } | null
    expect(firm?.control).toMatchObject({ rule: { active: false } })
    const bts = engine.snapshotFor('backtests') as { submitted_ns: number }[]
    expect(bts.length).toBeGreaterThan(0)
    expect(bts.every((b, i) => i === 0 || (bts[i - 1]?.submitted_ns ?? 0) >= b.submitted_ns)).toBe(true)
  })
})

describe('mock WS hub', () => {
  it('answers a sub with snapshots and one error frame listing the unknown topics', async () => {
    const hub = new MockHub()
    hub.setProviders((t) => (t === 'quotes' ? [] : null), () => '20260928', () => Date.now(), (id) => id === 'ACC1')
    const sock = new MockSocket(hub)
    const got: Record<string, unknown>[] = []
    sock.onmessage = (ev) => got.push(JSON.parse(String(ev.data)) as Record<string, unknown>)
    await vi.advanceTimersByTimeAsync(50)
    sock.send(JSON.stringify({ op: 'auth', key: 'k' }))
    sock.send(JSON.stringify({ op: 'sub', topics: ['quotes', 'nope', 'positions:ACCX', 'backtests'] }))
    await vi.advanceTimersByTimeAsync(50)
    const errors = got.filter((m) => m.type === 'error')
    expect(errors).toHaveLength(1)
    expect(errors[0]?.data).toEqual({ code: 'BAD_REQUEST', message: 'unknown topics', topics: ['nope', 'positions:ACCX'] })
    expect(errors[0]).not.toHaveProperty('topic')
    expect(errors[0]).not.toHaveProperty('seq')
    expect(got.filter((m) => m.type === 'snapshot').map((m) => m.topic)).toEqual(['quotes', 'backtests'])
    expect(got.find((m) => m.type === 'snapshot')).toMatchObject({ seq: 1, td: '20260928' })
    hub.dispose()
  })
})
