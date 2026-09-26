/** REST client: headers (X-API-Key, Idempotency-Key on POST/DELETE), envelope unwrap, error mapping. */
import { describe, expect, it } from 'vitest'
import { ApiClient, ApiError, buildQuery, type TransportRequest, type TransportResponse } from '@/api/http'

function res(status: number, body: unknown, headers: Record<string, string> = {}): TransportResponse {
  const text = typeof body === 'string' ? body : JSON.stringify(body)
  const h = Object.fromEntries(Object.entries(headers).map(([k, v]) => [k.toLowerCase(), v]))
  return { status, headers: { get: (n: string) => h[n.toLowerCase()] ?? null }, text: async () => text, blob: async () => new Blob([text]) }
}

function client(reply: (req: TransportRequest) => TransportResponse | Promise<TransportResponse>, key = 'k-123') {
  const seen: TransportRequest[] = []
  const api = new ApiClient({
    getKey: () => key,
    transport: async (req) => {
      seen.push(req)
      return reply(req)
    },
  })
  return { api, seen }
}

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/

describe('ApiClient', () => {
  it('GET sends X-API-Key, no Idempotency-Key, and unwraps the envelope', async () => {
    const { api, seen } = client(() => res(200, { ok: true, code: 'OK', message: '', data: [{ account_id: 'ACC1' }] }))
    const data = await api.get('/accounts', { query: { td: '20260928', empty: '', none: undefined } })
    expect(data).toEqual([{ account_id: 'ACC1' }])
    expect(seen[0]?.url).toBe('/api/v1/accounts?td=20260928')
    expect(seen[0]?.headers['X-API-Key']).toBe('k-123')
    expect(seen[0]?.headers['Idempotency-Key']).toBeUndefined()
  })

  it('POST / DELETE always carry an Idempotency-Key (UUID) and a JSON body', async () => {
    const { api, seen } = client(() => res(200, { ok: true, code: 'OK', data: { account_state: 'RESTRICTED' } }))
    await api.post('/risk/accounts/ACC3/reset', { reason: 'r' })
    await api.del('/demo/faults/f1')
    expect(seen[0]?.headers['Idempotency-Key']).toMatch(UUID)
    expect(seen[0]?.headers['Content-Type']).toBe('application/json')
    expect(JSON.parse(seen[0]?.body ?? '{}')).toEqual({ reason: 'r' })
    expect(seen[1]?.method).toBe('DELETE')
    expect(seen[1]?.headers['Idempotency-Key']).toMatch(UUID)
    expect(seen[0]?.headers['Idempotency-Key']).not.toBe(seen[1]?.headers['Idempotency-Key'])
  })

  it('reuses a caller-provided Idempotency-Key (retry of the same action)', async () => {
    const { api, seen } = client(() => res(200, { ok: true, code: 'OK', data: null }))
    await api.post('/risk/firm/kill-switch', { on: true, reason: 'x' }, { idempotencyKey: 'same-key' })
    await api.post('/risk/firm/kill-switch', { on: true, reason: 'x' }, { idempotencyKey: 'same-key' })
    expect(seen.map((s) => s.headers['Idempotency-Key'])).toEqual(['same-key', 'same-key'])
  })

  it('getFull / postFull keep the envelope message, the duplicate flag and X-Result-Code', async () => {
    const { api } = client((req) =>
      req.method === 'GET'
        ? res(200, { ok: true, code: 'OK', message: 'partial: risk-engine-1=UNAVAILABLE', data: { items: [], errors: { 'risk-engine-1': 'UNAVAILABLE' } } }, { 'X-Result-Code': 'OK' })
        : res(200, { ok: true, code: 'OK', message: '0 latched level(s) reset', data: { account_state: 'NORMAL' }, duplicate: true }, { 'X-Result-Code': 'OK' }),
    )
    const ev = await api.getFull<{ errors: Record<string, string> }>('/risk/events')
    expect(ev).toMatchObject({ code: 'OK', message: 'partial: risk-engine-1=UNAVAILABLE', duplicate: false, status: 200, resultCode: 'OK' })
    expect(ev.data.errors['risk-engine-1']).toBe('UNAVAILABLE')
    const reset = await api.postFull('/risk/accounts/ACC1/reset', { reason: 'r' })
    expect(reset).toMatchObject({ duplicate: true, message: '0 latched level(s) reset', data: { account_state: 'NORMAL' } })
  })

  it('an ok=false envelope keeps its data (kill-switch partial failure, CONFLICT with the plan)', async () => {
    const data = { targets: [{ target: 'trade-gateway', ok: false, code: 'UNAVAILABLE' }], all_ok: false, control: null, retrying: ['trade-gateway'] }
    const { api } = client(() => res(200, { ok: false, code: 'UNAVAILABLE', message: '2/3 targets confirmed; failed: trade-gateway=UNAVAILABLE', data }, { 'X-Result-Code': 'UNAVAILABLE' }))
    const e = await api.post('/risk/firm/kill-switch', { on: true, reason: 'x' }).catch((x: unknown) => x)
    expect(e).toBeInstanceOf(ApiError)
    expect((e as ApiError).status).toBe(200)
    expect((e as ApiError).data).toEqual(data)
  })

  it('keeps the duplicate flag of a replayed failure, the error id of a 500 and the 405 / 413 statuses', async () => {
    const replay = client(() => res(200, { ok: false, code: 'UNAVAILABLE', message: '1/3 targets confirmed', data: { targets: [], all_ok: false, control: null, retrying: [], failed: [] }, duplicate: true }))
    const e1 = (await replay.api.post('/risk/firm/kill-switch', { on: true, reason: 'x' }).catch((x: unknown) => x)) as ApiError
    expect(e1.duplicate).toBe(true)
    const internal = client(() => res(500, { ok: false, code: 'INTERNAL', message: 'internal error (error id 4f2a9c01b3de)', data: { error_id: '4f2a9c01b3de' } }))
    const e2 = (await internal.api.get('/x').catch((x: unknown) => x)) as ApiError
    expect(e2.errorId).toBe('4f2a9c01b3de')
    expect(e2.duplicate).toBe(false)
    const wrong = client((req) => (req.method === 'DELETE' ? res(405, { ok: false, code: 'BAD_REQUEST', message: 'method DELETE not allowed; use GET', data: null }) : res(413, { ok: false, code: 'BAD_REQUEST', message: 'request body larger than 1048576 bytes', data: null })))
    const e3 = (await wrong.api.del('/system/status').catch((x: unknown) => x)) as ApiError
    expect([e3.code, e3.status]).toEqual(['BAD_REQUEST', 405])
    const e4 = (await wrong.api.post('/trades/import', {}).catch((x: unknown) => x)) as ApiError
    expect([e4.code, e4.status]).toEqual(['BAD_REQUEST', 413])
  })

  it('maps a non-envelope error by X-Result-Code before the HTTP status', async () => {
    const { api } = client(() => res(502, '<html>bad gateway</html>', { 'X-Result-Code': 'UNAVAILABLE' }))
    const e = (await api.get('/x').catch((x: unknown) => x)) as ApiError
    expect(e.code).toBe('UNAVAILABLE')
  })

  it('omits X-API-Key when no key is configured', async () => {
    const { api, seen } = client(() => res(200, { ok: true, code: 'OK', data: 1 }), '')
    await api.get('/health')
    expect(seen[0]?.headers['X-API-Key']).toBeUndefined()
  })

  it('business rejects arrive as HTTP 200 with ok=false and raise their code', async () => {
    const { api } = client(() => res(200, { ok: false, code: 'REJECT_ACCOUNT_HALTED', message: '账户已熔断', data: { client_order_id: 'o1' } }))
    const err = await api.post('/orders', {}).catch((e: unknown) => e)
    expect(err).toBeInstanceOf(ApiError)
    expect((err as ApiError).code).toBe('REJECT_ACCOUNT_HALTED')
    expect((err as ApiError).status).toBe(200)
    expect((err as ApiError).data).toEqual({ client_order_id: 'o1' })
    expect((err as ApiError).display).toBe('REJECT_ACCOUNT_HALTED: 账户已熔断')
  })

  it('maps envelope errors with their HTTP status and Retry-After', async () => {
    const { api } = client(() => res(429, { ok: false, code: 'RISK_BUSY', message: 'queue full' }, { 'Retry-After': '2' }))
    const err = (await api.get('/accounts').catch((e: unknown) => e)) as ApiError
    expect(err.code).toBe('RISK_BUSY')
    expect(err.status).toBe(429)
    expect(err.retryAfterMs).toBe(2000)
  })

  it('maps non-envelope errors (FastAPI validation, 404) to codes', async () => {
    const { api } = client((req) =>
      req.url.includes('bad') ? res(422, { detail: [{ loc: ['body', 'reason'], msg: 'field required' }] }) : res(404, 'Not Found'),
    )
    const e1 = (await api.post('/bad', {}).catch((e: unknown) => e)) as ApiError
    expect(e1.code).toBe('BAD_REQUEST')
    expect(e1.message).toBe('body.reason: field required')
    const e2 = (await api.get('/missing').catch((e: unknown) => e)) as ApiError
    expect(e2.code).toBe('NOT_FOUND')
  })

  it('returns non-envelope success bodies as-is and reports network errors as UNAVAILABLE', async () => {
    const ok = client(() => res(200, [1, 2, 3]))
    expect(await ok.api.get('/x')).toEqual([1, 2, 3])
    const down = client(() => {
      throw new TypeError('Failed to fetch')
    })
    const err = (await down.api.get('/x').catch((e: unknown) => e)) as ApiError
    expect(err.code).toBe('UNAVAILABLE')
  })

  it('downloads files with the key header and surfaces envelope errors', async () => {
    const { api, seen } = client((req) =>
      req.url.endsWith('/r1') ? res(200, '<html>日报</html>', { 'Content-Type': 'text/html; charset=utf-8', 'Content-Disposition': 'inline; filename="risk_daily.html"' }) : res(404, { ok: false, code: 'NOT_FOUND', message: 'no report' }, { 'Content-Type': 'application/json' }),
    )
    const f = await api.getBlob('/reports/r1')
    expect(f.filename).toBe('risk_daily.html')
    expect(f.contentType).toContain('text/html')
    expect(seen[0]?.headers['X-API-Key']).toBe('k-123')
    const err = (await api.getBlob('/reports/r2').catch((e: unknown) => e)) as ApiError
    expect(err.code).toBe('NOT_FOUND')
  })
})

describe('buildQuery', () => {
  it('skips empty values and joins arrays', () => {
    expect(buildQuery({ a: 1, b: '', c: null, d: undefined, e: ['rb2701', 'IF2612'], f: true })).toBe('?a=1&e=rb2701%2CIF2612&f=true')
    expect(buildQuery({})).toBe('')
    expect(buildQuery()).toBe('')
  })
})
