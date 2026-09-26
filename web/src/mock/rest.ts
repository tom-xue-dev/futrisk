/**
 * Mock api-gateway REST router: the same paths and answers as
 * `src/futrisk/services/api_gateway/routes/*.py`:
 *  - uniform envelope `{ok, code, message, data}` (+ `duplicate: true` on an idempotent
 *    replay) and the `X-Result-Code` + `X-Content-Type-Options: nosniff` headers; code ->
 *    HTTP status as in `rest.http_status` (REJECT_* 200, BUSY/RISK_BUSY 429 and UNAVAILABLE
 *    503 with Retry-After, INTERNAL 500 with a generic message and `data.error_id`, …);
 *  - an unknown path is a 404 envelope ("unknown API path …"), a known path with another
 *    method 405 (code BAD_REQUEST), a body over 1 MiB 413 (code BAD_REQUEST);
 *  - mutations (POST / DELETE) need an `Idempotency-Key` that is a UUID; the same key with
 *    the same request (method, path, body) replays the recorded answer with
 *    `duplicate: true`, with another request it is 409 CONFLICT; TIMEOUT / UNAVAILABLE /
 *    BUSY / RISK_BUSY / INTERNAL answers are not recorded (a retry runs again) — except for
 *    the kill switch and every `/demo/*` mutation, which record every outcome;
 *  - fan-outs (kill switch) always answer HTTP 200;
 *  - `/demo/*` answers 403 FORBIDDEN outside app.mode=demo;
 *  - `GET /reports/{id}` returns the file itself (CSP sandbox, nosniff).
 */
import type { TransportRequest, TransportResponse, Transport } from '@/api/http'
import type { PositionGroupBy } from '@/types'
import { MockError, MockResult, type MockEngine } from './engine'

const STATUS_OF: Record<string, number> = {
  OK: 200,
  BAD_REQUEST: 400,
  UNAUTHORIZED: 401,
  FORBIDDEN: 403,
  NOT_FOUND: 404,
  CONFLICT: 409,
  NOT_OWNER: 409,
  PRECONDITION_FAILED: 409,
  BUSY: 429,
  RISK_BUSY: 429,
  INTERNAL: 500,
  VERSION_UNSUPPORTED: 502,
  UNAVAILABLE: 503,
  TIMEOUT: 504,
}
/** business outcomes: HTTP 200 with ok=false */
const OUTCOME_PREFIXES = ['REJECT_', 'MD_REJECT_', 'MD_QUARANTINE_', 'BROKER_', 'ROUTE_']
const MAX_BODY_BYTES = 1024 * 1024

/** `rest.http_status` */
export function httpStatus(code: string): number {
  if (OUTCOME_PREFIXES.some((p) => code.startsWith(p))) return 200
  return STATUS_OF[code] ?? 500
}

const NOT_RECORDED = new Set(['TIMEOUT', 'UNAVAILABLE', 'BUSY', 'RISK_BUSY', 'INTERNAL'])
const UUID_RE = /^[0-9a-f]{8}-?[0-9a-f]{4}-?[0-9a-f]{4}-?[0-9a-f]{4}-?[0-9a-f]{12}$/i

interface RouteCtx {
  params: string[]
  query: URLSearchParams
  body: Record<string, unknown>
  actor: string
  /** Idempotency-Key of a mutation (the command id), '' for reads */
  key: string
}

type Handler = (ctx: RouteCtx) => unknown

interface Route {
  method: string
  re: RegExp
  handler: Handler
  demo: boolean
  /** every outcome is recorded under the Idempotency-Key (kill switch, /demo mutations) */
  recordAll: boolean
}

interface Envelope {
  ok: boolean
  code: string
  message: string
  data: unknown
  duplicate?: boolean
}

function response(status: number, body: string, contentType = 'application/json', extra: Record<string, string> = {}): TransportResponse {
  const headers: Record<string, string> = { 'content-type': contentType, ...Object.fromEntries(Object.entries(extra).map(([k, v]) => [k.toLowerCase(), v])) }
  return {
    status,
    headers: { get: (name: string) => headers[name.toLowerCase()] ?? null },
    text: async () => body,
    blob: async () => new Blob([body], { type: contentType }),
  }
}

function envelopeResponse(env: Envelope, status = httpStatus(env.code)): TransportResponse {
  const headers: Record<string, string> = { 'X-Result-Code': env.code, 'X-Content-Type-Options': 'nosniff' }
  if (status === 429 || status === 503) headers['Retry-After'] = '1'
  const body: Envelope = { ok: env.ok, code: env.code, message: env.message || '', data: env.data === undefined ? null : env.data }
  if (env.duplicate) body.duplicate = true
  return response(status, JSON.stringify(body), 'application/json', headers)
}

function failure(code: string, message: string, data: unknown = null): Envelope {
  return { ok: false, code, message: message || code, data }
}

function stableJson(v: unknown): string {
  if (v === null || typeof v !== 'object') return JSON.stringify(v)
  if (Array.isArray(v)) return `[${v.map(stableJson).join(',')}]`
  return `{${Object.keys(v as object)
    .sort()
    .map((k) => `${JSON.stringify(k)}:${stableJson((v as Record<string, unknown>)[k])}`)
    .join(',')}}`
}

export function createRestTransport(engine: MockEngine): Transport {
  const idem = new Map<string, { hash: string; env: Envelope; status: number }>()
  const routes: Route[] = []
  const r = (method: string, pattern: string, handler: Handler, opts: { recordAll?: boolean } = {}) => {
    const re = new RegExp(`^${pattern.replace(/\{[a-z_]+\}/g, '([^/]+)')}$`)
    const demo = pattern.startsWith('/demo/')
    routes.push({ method, re, handler, demo, recordAll: opts.recordAll ?? (demo && method !== 'GET') })
  }
  const q = (ctx: RouteCtx, k: string) => ctx.query.get(k) ?? undefined
  const qNum = (ctx: RouteCtx, k: string, dflt: number) => {
    const v = ctx.query.get(k)
    if (v === null) return dflt
    const n = Number(v)
    if (!Number.isFinite(n) || !Number.isInteger(n)) throw new MockError('BAD_REQUEST', `${k}: Input should be a valid integer`, 400)
    return n
  }
  const qTd = (ctx: RouteCtx) => {
    const v = ctx.query.get('td')
    if (v !== null && !/^\d{8}$/.test(v)) throw new MockError('BAD_REQUEST', "td: String should match pattern '^\\d{8}$'", 400)
    return v ?? undefined
  }
  const str = (v: unknown) => (typeof v === 'string' ? v : '')
  const reason = (c: RouteCtx) => {
    const v = str(c.body.reason)
    if (!v) throw new MockError('BAD_REQUEST', 'reason: String should have at least 1 character', 400)
    return v
  }

  // ---- system
  r('GET', '/health', () => engine.health())
  r('GET', '/auth/me', () => engine.authMe())
  r('GET', '/system/services', () => engine.listServices())
  r('GET', '/system/status', () => engine.systemStatus())
  r('POST', '/services/{instance}/restart', (c) => engine.restartService(c.params[0] as string, c.actor))
  r('POST', '/services/{instance}/stop', () => {
    throw new MockError('FORBIDDEN', '模拟模式不支持停止服务（请用重启或 kill）', 403)
  })
  r('POST', '/services/{instance}/start', (c) => {
    throw new MockError('CONFLICT', `${c.params[0]} is RUNNING`, 409)
  })

  // ---- market
  r('GET', '/market/quotes', (c) => engine.ticks(q(c, 'instruments')?.split(',').map((x) => x.trim()).filter(Boolean)))
  r('GET', '/market/feeds', () => engine.feeds())
  r('GET', '/market/events', (c) => engine.mdEventsFor(qTd(c), q(c, 'kind')))

  // ---- accounts & positions
  r('GET', '/accounts', () => engine.accountList())
  r('GET', '/accounts/{id}', (c) => engine.accountSnapshot(c.params[0] as string))
  r('GET', '/accounts/{id}/positions', (c) => engine.positions(c.params[0] as string))
  r('GET', '/accounts/{id}/orders', (c) => engine.orders(c.params[0] as string, qTd(c)))
  r('GET', '/accounts/{id}/trades', (c) => engine.trades(c.params[0] as string, qTd(c)))
  r('GET', '/accounts/{id}/equity-curve', (c) => engine.equityCurve(c.params[0] as string, qTd(c), qNum(c, 'step_s', 5)))
  r('GET', '/positions', (c) =>
    engine.positionsView({ account: q(c, 'account'), product: q(c, 'product'), instrument: q(c, 'instrument'), exchange: q(c, 'exchange'), sector: q(c, 'sector'), group_by: q(c, 'group_by') as PositionGroupBy | undefined }),
  )

  // ---- risk
  r('GET', '/risk/rules', () => engine.ruleSet())
  r('GET', '/risk/state', (c) => engine.riskStateRows(q(c, 'account')))
  r('GET', '/risk/events', (c) => engine.riskEventsFor(qTd(c), q(c, 'account')))
  r('POST', '/risk/events/{alert_key}/ack', (c) => {
    engine.ackAlert(c.params[0] as string, typeof c.body.note === 'string' ? c.body.note : undefined, c.actor)
    return null
  })
  r('GET', '/risk/compare', () => engine.compareView())
  r('GET', '/risk/compare/history', (c) => engine.historyDays(qNum(c, 'days', 20)))
  r('POST', '/risk/accounts/{id}/reset', (c) =>
    engine.reset(c.params[0] as string, reason(c), str(c.body.rule_id) || undefined, str(c.body.level) || undefined, c.actor),
  )
  r('POST', '/risk/accounts/{id}/halt', (c) => engine.halt(c.params[0] as string, reason(c), c.actor))
  r(
    'POST',
    '/risk/firm/kill-switch',
    (c) => {
      if (typeof c.body.on !== 'boolean') throw new MockError('BAD_REQUEST', 'on: Input should be a valid boolean', 400)
      return engine.killSwitch(c.body.on, reason(c), c.actor, c.key)
    },
    // every outcome is recorded: a client retry must never re-assert a decision a newer one replaced
    { recordAll: true },
  )
  r('GET', '/risk/liquidation-plans', (c) => engine.liquidationPlans(q(c, 'account'), q(c, 'status'), qTd(c)))
  r('POST', '/risk/eod/{td}/force-unfreeze', (c) => engine.forceUnfreeze(c.params[0] as string, reason(c), c.actor))
  r('POST', '/risk/liquidation-plans/{plan_id}/confirm', (c) => engine.confirmPlan(c.params[0] as string, str(c.body.decision), c.actor))

  // ---- settlement / reports / backtests
  r('GET', '/settlement/days', (c) => engine.settlementDays(qNum(c, 'limit', 30)))
  r('POST', '/settlement/run', (c) => {
    const td = str(c.body.td)
    if (!/^\d{8}$/.test(td)) throw new MockError('BAD_REQUEST', "td: String should match pattern '^\\d{8}$'", 400)
    const force = Boolean(c.body.force_new_version)
    if (td !== engine.tradingDay && !force) throw new MockError('CONFLICT', `${td} is already settled (force_new_version to re-run)`, 409)
    return engine.runEod(td, 1, c.actor)
  })
  r('GET', '/settlement/{td}/status', (c) => engine.settlementStatus(c.params[0] as string))
  r('GET', '/settlement/{td}/statements/{account}', (c) => engine.statement(c.params[0] as string, c.params[1] as string))
  r('GET', '/settlement/{td}', (c) => {
    const td = c.params[0] as string
    if (!/^\d{8}$/.test(td)) throw new MockError('BAD_REQUEST', 'trading day must be YYYYMMDD', 400)
    return engine.settlementResult(td, q(c, 'account'))
  })
  r('GET', '/reports', (c) => engine.listReports(qTd(c), q(c, 'kind')))
  r('GET', '/backtests', () => engine.listBacktests())
  r('GET', '/backtests/{run_id}', (c) => engine.getBacktest(c.params[0] as string))
  r('POST', '/backtests', (c) => engine.runBacktest(str(c.body.config_name)))

  // ---- demo (ADMIN, app.mode=demo only; mutations record every outcome)
  r('GET', '/demo/state', () => engine.demoState())
  r('GET', '/demo/faults', () => engine.demoFaults())
  r('GET', '/demo/scenarios', () => engine.demoScenarios())
  r('DELETE', '/demo/scenario', () => engine.cancelScenario())
  r('POST', '/demo/faults', (c) =>
    engine.injectFault(str(c.body.type), typeof c.body.target === 'string' ? c.body.target : undefined, (c.body.params as Record<string, unknown>) ?? {}, typeof c.body.duration_s === 'number' ? c.body.duration_s : undefined),
  )
  r('DELETE', '/demo/faults/{fault_id}', (c) => engine.deleteFault(c.params[0] as string))
  r('POST', '/demo/shock', (c) => {
    if (typeof c.body.pct !== 'number') throw new MockError('BAD_REQUEST', 'pct: Field required', 400)
    return engine.shock(Array.isArray(c.body.instruments) ? (c.body.instruments as string[]) : undefined, typeof c.body.sector === 'string' ? c.body.sector : undefined, c.body.pct, Number(c.body.duration_s ?? 0), c.body.shape === 'STEP' ? 'STEP' : 'LINEAR')
  })
  r('POST', '/demo/scenario', (c) => engine.scenario(str(c.body.name), typeof c.body.speed === 'number' ? c.body.speed : 1))
  r('POST', '/demo/clock', (c) =>
    engine.clockJump(str(c.body.target), typeof c.body.at === 'string' || typeof c.body.at === 'number' ? c.body.at : undefined, typeof c.body.at_ns === 'number' ? c.body.at_ns : undefined, c.actor),
  )
  r('POST', '/demo/kill/{instance}', (c) => engine.killService(c.params[0] as string, c.actor))

  const reportFile = createReportFileHandler(engine)

  async function run(route: Route, ctx: RouteCtx): Promise<{ env: Envelope; status: number }> {
    try {
      const out = await route.handler(ctx)
      if (out instanceof MockResult) return { env: { ok: out.ok, code: out.code, message: out.message, data: out.data }, status: out.status ?? httpStatus(out.code) }
      return { env: { ok: true, code: 'OK', message: '', data: out }, status: 200 }
    } catch (e) {
      if (e instanceof MockError) return { env: failure(e.code, e.message, e.data ?? null), status: httpStatus(e.code) }
      // unexpected: a generic message and an error id (the details only go to the log)
      const errorId = Math.random().toString(16).slice(2, 14).padEnd(12, '0')
      console.error(`[mock api-gateway] error ${errorId}`, e)
      return { env: failure('INTERNAL', `internal error (error id ${errorId})`, { error_id: errorId }), status: 500 }
    }
  }

  return async (req: TransportRequest): Promise<TransportResponse> => {
    await new Promise((res) => setTimeout(res, 25 + Math.random() * 60))
    if (req.signal?.aborted) throw new DOMException('aborted', 'AbortError')
    const url = new URL(req.url, 'http://mock.local')
    const path = url.pathname.replace(/^\/api\/v1/, '')
    if (Date.now() < engine.apiDownUntil) throw new TypeError('Failed to fetch (api-gateway restarting)')
    const file = req.method === 'GET' ? /^\/reports\/([^/]+)$/.exec(path) : null
    if (file) return reportFile(decodeURIComponent(file[1] as string))
    const route = routes.find((x) => x.method === req.method && x.re.test(path))
    if (!route) {
      const allowed = [...new Set(routes.filter((x) => x.re.test(path)).map((x) => x.method))]
      if (allowed.length) return envelopeResponse(failure('BAD_REQUEST', `method ${req.method} not allowed; use ${allowed.join(', ')}`), 405)
      return envelopeResponse(failure('NOT_FOUND', `unknown API path ${url.pathname}`))
    }
    if (req.body && new Blob([req.body]).size > MAX_BODY_BYTES) return envelopeResponse(failure('BAD_REQUEST', `request body larger than ${MAX_BODY_BYTES} bytes`), 413)
    if (route.demo && engine.appMode !== 'demo') return envelopeResponse(failure('FORBIDDEN', 'demo endpoints are only available in demo mode (app.mode=demo)'))
    const params = (route.re.exec(path) ?? []).slice(1).map((p) => decodeURIComponent(p))
    let body: Record<string, unknown> = {}
    if (req.body) {
      try {
        body = JSON.parse(req.body) as Record<string, unknown>
      } catch {
        return envelopeResponse(failure('BAD_REQUEST', 'body: invalid JSON'))
      }
    }
    const actor = req.headers['X-API-Key'] ? 'mock-admin' : 'anonymous'
    const mutating = req.method === 'POST' || req.method === 'DELETE'
    const ctx: RouteCtx = { params, query: url.searchParams, body, actor, key: mutating ? (req.headers['Idempotency-Key'] ?? '').trim() : '' }
    if (!mutating) {
      const { env, status } = await run(route, ctx)
      return envelopeResponse(env, status)
    }
    // gateway.mutate: Idempotency-Key (UUID) dedupe on method + path + requester + body
    const key = (req.headers['Idempotency-Key'] ?? '').trim()
    if (!key) return envelopeResponse(failure('BAD_REQUEST', 'the Idempotency-Key header (UUID) is required'))
    if (!UUID_RE.test(key)) return envelopeResponse(failure('BAD_REQUEST', 'Idempotency-Key must be a UUID'))
    const hash = stableJson({ method: req.method, path: url.pathname, requester: actor, body })
    const prev = idem.get(key)
    if (prev) {
      if (prev.hash !== hash) return envelopeResponse(failure('CONFLICT', `Idempotency-Key ${key} was used for a different request`))
      return envelopeResponse({ ...prev.env, duplicate: true }, prev.status)
    }
    const { env, status } = await run(route, ctx)
    if (route.recordAll || !NOT_RECORDED.has(env.code)) idem.set(key, { hash, env, status })
    return envelopeResponse(env, status)
  }
}

/** `GET /reports/{report_id}` returns the file itself, not an envelope. */
export function createReportFileHandler(engine: MockEngine) {
  return (reportId: string): TransportResponse => {
    try {
      const f = engine.reportFile(reportId)
      return response(200, f.body, f.contentType, {
        'Content-Disposition': `inline; filename="${f.filename}"`,
        'Content-Security-Policy': 'sandbox',
        'X-Content-Type-Options': 'nosniff',
        'X-Result-Code': 'OK',
      })
    } catch (e) {
      const code = e instanceof MockError ? e.code : 'INTERNAL'
      return envelopeResponse(failure(code, e instanceof Error ? e.message : String(e)))
    }
  }
}
