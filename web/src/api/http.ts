/**
 * REST client for the api-gateway (02 §9.6).
 *
 * - Base path `/api/v1`, same origin (the Vite dev server proxies it).
 * - `X-API-Key` on every request; the key comes from the settings dialog.
 * - Mutating requests (POST / DELETE) always carry an `Idempotency-Key` (UUID). Callers
 *   that may retry the same logical action pass their own key so a retry is
 *   deduplicated by the gateway (`processed_commands`, 01 §8.6).
 * - The uniform body `{ok, code, message, data}` is unwrapped; `ok=false` (including
 *   business rejects `REJECT_*` and fan-out partial failures, both HTTP 200) raises an
 *   ApiError carrying the code, the HTTP status, `Retry-After` (429 / 503) and `data`
 *   (fan-out results, the current plan of a CONFLICT, `error_id` of an unexpected 500).
 *   The gateway answers an unknown `/api` path 404, a wrong method 405 and a body over
 *   the size limit 413 — all as envelopes.
 * - The transport is injectable: `fetch` in live mode, the in-browser mock otherwise.
 */
import { uuid } from '@/utils/uuid'

export class ApiError extends Error {
  readonly code: string
  readonly status: number
  readonly retryAfterMs: number | null
  readonly data: unknown
  /** the recorded answer of an earlier request with the same Idempotency-Key (`duplicate: true`) */
  readonly duplicate: boolean

  constructor(code: string, message: string, status: number, retryAfterMs: number | null = null, data: unknown = undefined, duplicate = false) {
    super(message)
    this.name = 'ApiError'
    this.code = code
    this.status = status
    this.retryAfterMs = retryAfterMs
    this.data = data
    this.duplicate = duplicate
  }

  /** "CODE: message" for toasts. */
  get display(): string {
    return this.message && this.message !== this.code ? `${this.code}: ${this.message}` : this.code
  }

  /** Id of an unexpected server error (`500 INTERNAL`, data `{error_id}`): the details are in the gateway log under it. */
  get errorId(): string | null {
    const d = this.data
    return typeof d === 'object' && d !== null && typeof (d as { error_id?: unknown }).error_id === 'string' ? (d as { error_id: string }).error_id : null
  }
}

export interface TransportRequest {
  method: 'GET' | 'POST' | 'DELETE' | 'PUT'
  url: string
  headers: Record<string, string>
  body?: string
  signal?: AbortSignal
}

/** The subset of `Response` the client uses (lets the mock answer without a real fetch). */
export interface TransportResponse {
  status: number
  headers: { get(name: string): string | null }
  text(): Promise<string>
  blob(): Promise<Blob>
}

export type Transport = (req: TransportRequest) => Promise<TransportResponse>

export const fetchTransport: Transport = (req) =>
  fetch(req.url, { method: req.method, headers: req.headers, body: req.body, signal: req.signal, credentials: 'same-origin' })

export type QueryValue = string | number | boolean | null | undefined | string[]

/** A successful answer with its envelope metadata (partial results carry a `message`). */
export interface ApiOk<T> {
  data: T
  code: string
  message: string
  /** idempotent replay of an earlier request with the same Idempotency-Key */
  duplicate: boolean
  status: number
  /** `X-Result-Code` response header (mirrors `code`) */
  resultCode: string | null
}

export interface RequestOptions {
  query?: Record<string, QueryValue>
  body?: unknown
  /** explicit Idempotency-Key (reuse it when retrying the same action) */
  idempotencyKey?: string
  timeoutMs?: number
}

export interface ApiClientOptions {
  baseUrl?: string
  getKey: () => string
  transport?: Transport
  timeoutMs?: number
  /** called on every HTTP 401 (the key is rejected) */
  onUnauthorized?: (message: string) => void
}

/** HTTP status -> error code when the body is not an envelope (02 §9.6 mapping, reversed). */
export const STATUS_TO_CODE: Record<number, string> = {
  400: 'BAD_REQUEST',
  401: 'UNAUTHORIZED',
  403: 'FORBIDDEN',
  404: 'NOT_FOUND',
  405: 'BAD_REQUEST',
  409: 'CONFLICT',
  413: 'BAD_REQUEST',
  422: 'BAD_REQUEST',
  429: 'BUSY',
  500: 'INTERNAL',
  502: 'VERSION_UNSUPPORTED',
  503: 'UNAVAILABLE',
  504: 'TIMEOUT',
}

export function buildQuery(query?: Record<string, QueryValue>): string {
  if (!query) return ''
  const parts: string[] = []
  for (const [k, v] of Object.entries(query)) {
    if (v === undefined || v === null || v === '') continue
    const val = Array.isArray(v) ? v.join(',') : String(v)
    if (val === '') continue
    parts.push(`${encodeURIComponent(k)}=${encodeURIComponent(val)}`)
  }
  return parts.length ? `?${parts.join('&')}` : ''
}

function isEnvelope(x: unknown): x is { ok: boolean; code: string; message?: string | null; data?: unknown; duplicate?: boolean } {
  return typeof x === 'object' && x !== null && typeof (x as { ok?: unknown }).ok === 'boolean' && typeof (x as { code?: unknown }).code === 'string'
}

/** Human message from a FastAPI-style error body (`{detail: str | [{msg, loc}]}`). */
function detailMessage(body: unknown): string | null {
  if (typeof body === 'string') return body.slice(0, 300) || null
  if (typeof body !== 'object' || body === null) return null
  const d = (body as { detail?: unknown; message?: unknown }).detail ?? (body as { message?: unknown }).message
  if (typeof d === 'string') return d
  if (Array.isArray(d)) {
    return d
      .map((e) => {
        if (typeof e === 'object' && e !== null) {
          const loc = Array.isArray((e as { loc?: unknown }).loc) ? ((e as { loc: unknown[] }).loc.join('.') + ': ') : ''
          return loc + String((e as { msg?: unknown }).msg ?? JSON.stringify(e))
        }
        return String(e)
      })
      .join('; ')
  }
  return null
}

function retryAfter(res: TransportResponse): number | null {
  const h = res.headers.get('Retry-After')
  if (!h) return null
  const s = Number(h)
  return Number.isFinite(s) ? s * 1000 : null
}

export class ApiClient {
  readonly baseUrl: string
  private readonly getKey: () => string
  private readonly transport: Transport
  private readonly timeoutMs: number
  private readonly onUnauthorized: ((message: string) => void) | null

  constructor(opts: ApiClientOptions) {
    this.baseUrl = opts.baseUrl ?? '/api/v1'
    this.getKey = opts.getKey
    this.transport = opts.transport ?? fetchTransport
    this.timeoutMs = opts.timeoutMs ?? 10_000
    this.onUnauthorized = opts.onUnauthorized ?? null
  }

  get<T = unknown>(path: string, opts: Omit<RequestOptions, 'body' | 'idempotencyKey'> = {}): Promise<T> {
    return this.request<T>('GET', path, opts)
  }

  /** GET with the envelope metadata (message of partial answers, duplicate flag). */
  getFull<T = unknown>(path: string, opts: Omit<RequestOptions, 'body' | 'idempotencyKey'> = {}): Promise<ApiOk<T>> {
    return this.requestFull<T>('GET', path, opts)
  }

  post<T = unknown>(path: string, body?: unknown, opts: Omit<RequestOptions, 'body'> = {}): Promise<T> {
    return this.request<T>('POST', path, { ...opts, body: body ?? {} })
  }

  postFull<T = unknown>(path: string, body?: unknown, opts: Omit<RequestOptions, 'body'> = {}): Promise<ApiOk<T>> {
    return this.requestFull<T>('POST', path, { ...opts, body: body ?? {} })
  }

  del<T = unknown>(path: string, opts: Omit<RequestOptions, 'body'> = {}): Promise<T> {
    return this.request<T>('DELETE', path, opts)
  }

  delFull<T = unknown>(path: string, opts: Omit<RequestOptions, 'body'> = {}): Promise<ApiOk<T>> {
    return this.requestFull<T>('DELETE', path, opts)
  }

  headers(method: TransportRequest['method'], opts: RequestOptions, accept = 'application/json'): Record<string, string> {
    const h: Record<string, string> = { Accept: accept }
    const key = this.getKey()
    if (key) h['X-API-Key'] = key
    if (opts.body !== undefined) h['Content-Type'] = 'application/json'
    if (method === 'POST' || method === 'DELETE') h['Idempotency-Key'] = opts.idempotencyKey ?? uuid()
    return h
  }

  private async send(method: TransportRequest['method'], path: string, opts: RequestOptions, accept?: string): Promise<TransportResponse> {
    const url = `${this.baseUrl}${path}${buildQuery(opts.query)}`
    const ctrl = typeof AbortController !== 'undefined' ? new AbortController() : null
    const timer = ctrl ? setTimeout(() => ctrl.abort(), opts.timeoutMs ?? this.timeoutMs) : null
    try {
      return await this.transport({
        method,
        url,
        headers: this.headers(method, opts, accept),
        body: opts.body === undefined ? undefined : JSON.stringify(opts.body),
        signal: ctrl?.signal,
      })
    } catch (e) {
      if (ctrl?.signal.aborted) throw new ApiError('TIMEOUT', `请求超时：${method} ${path}`, 0)
      throw new ApiError('UNAVAILABLE', `网络错误：${e instanceof Error ? e.message : String(e)}`, 0)
    } finally {
      if (timer) clearTimeout(timer)
    }
  }

  async request<T = unknown>(method: TransportRequest['method'], path: string, opts: RequestOptions = {}): Promise<T> {
    return (await this.requestFull<T>(method, path, opts)).data
  }

  async requestFull<T = unknown>(method: TransportRequest['method'], path: string, opts: RequestOptions = {}): Promise<ApiOk<T>> {
    const res = await this.send(method, path, opts)
    if (res.status === 401) this.onUnauthorized?.(`${method} ${path}`)
    const text = await res.text()
    const resultCode = res.headers.get('X-Result-Code')
    let body: unknown = undefined
    if (text) {
      try {
        body = JSON.parse(text)
      } catch {
        body = text
      }
    }
    if (isEnvelope(body)) {
      if (body.ok) {
        return { data: body.data as T, code: body.code, message: body.message ?? '', duplicate: body.duplicate === true, status: res.status, resultCode }
      }
      throw new ApiError(body.code, body.message ?? body.code, res.status, retryAfter(res), body.data, body.duplicate === true)
    }
    if (res.status >= 200 && res.status < 300) {
      return { data: body as T, code: resultCode ?? 'OK', message: '', duplicate: false, status: res.status, resultCode }
    }
    const code = resultCode ?? STATUS_TO_CODE[res.status] ?? 'INTERNAL'
    throw new ApiError(code, detailMessage(body) ?? `HTTP ${res.status}`, res.status, retryAfter(res), body)
  }

  /**
   * Download a file endpoint (`GET /reports/{report_id}`) with the API key header. An
   * envelope with `ok=false` is turned into an ApiError.
   */
  async getBlob(path: string): Promise<{ blob: Blob; filename: string | null; contentType: string }> {
    const res = await this.send('GET', path, {}, '*/*')
    const ct = res.headers.get('Content-Type') ?? 'application/octet-stream'
    if (res.status < 200 || res.status >= 300 || ct.includes('application/json')) {
      const text = await res.text()
      let body: unknown = text
      try {
        body = JSON.parse(text)
      } catch {
        // not JSON
      }
      if (isEnvelope(body)) {
        if (!body.ok) throw new ApiError(body.code, body.message ?? body.code, res.status)
        const json = JSON.stringify(body.data ?? body, null, 2)
        return { blob: new Blob([json], { type: 'application/json' }), filename: null, contentType: 'application/json' }
      }
      if (res.status < 200 || res.status >= 300) throw new ApiError(STATUS_TO_CODE[res.status] ?? 'INTERNAL', detailMessage(body) ?? `HTTP ${res.status}`, res.status)
      return { blob: new Blob([text], { type: ct }), filename: null, contentType: ct }
    }
    const cd = res.headers.get('Content-Disposition')
    const m = cd ? /filename\*?=(?:UTF-8'')?"?([^";]+)"?/i.exec(cd) : null
    return { blob: await res.blob(), filename: m?.[1] ? decodeURIComponent(m[1]) : null, contentType: ct }
  }
}
