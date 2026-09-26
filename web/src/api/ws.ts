/**
 * WebSocket client for the api-gateway push protocol (02 §9.7).
 *
 * Protocol handled here (as implemented by the api-gateway's `wshub.py`):
 *  - connect to `/ws` (same origin) and authenticate with the first message
 *    `{"op":"auth","key":…}` (the key never appears in the URL / access logs); a rejected
 *    key gets an `error` frame and close code 4401: the client then stops retrying until
 *    `restart()` (new key). A 4401 whose error frame says `BUSY` is the gateway's auth
 *    throttle ("too many failed authentications, retry in Ns"): retried after that wait;
 *  - `sub` / `unsub` with per-topic reference counting, so several components can share
 *    a topic. The gateway answers every `sub` with a `snapshot` of the topic;
 *  - every topic frame carries a per-topic, per-connection `seq`. For incremental frames
 *    (upsert / delete / event): seq == last+1 -> apply; seq <= last -> duplicate, drop;
 *    seq > last+1 -> gap: apply (it is newer data) and send `{"op":"resync"}` for the
 *    topic, the server answers with a `snapshot` that resets the baseline;
 *  - a server `resync` frame (slow-consumer overflow: the server dropped the topic's
 *    queued frames) resets the baseline, requests a snapshot and asks the app to reload
 *    that topic via REST;
 *  - `error` frames (`{code, message, topics?}`: one per failed op, `topics` = the unknown
 *    topics of a `sub`) are session frames without topic / seq and go to the app;
 *  - on reconnect all baselines are cleared and every topic is re-subscribed; the
 *    snapshots answering those `sub`s are the resync (an explicit `resync` op can be
 *    enabled with `resyncOnReconnect`);
 *  - exponential reconnect backoff with jitter, reset after the first frame of a healthy
 *    connection; an idle watchdog (server heartbeat every 5s) forces a reconnect;
 *  - gateway limits: at most 100 topics per op (ops are split) and 64 KB per message; a
 *    client closed for policy (1008), a too big message (1009) or as too slow (1013)
 *    reconnects with a backoff that is not reset by the first frame until the connection
 *    stayed up for a while;
 *  - resync requests are deduplicated per topic while one is pending (the gateway ignores a
 *    resync while a snapshot of that topic is still queued);
 *  - client ping every `pingIntervalMs`.
 *
 * The class is framework-free; socket factory and timers are injectable for tests.
 */
import type { WsClientOp, WsServerMessage } from '@/types'
import { isEventTopic, WS_CLOSE_POLICY, WS_CLOSE_TOO_BIG, WS_CLOSE_TRY_AGAIN, WS_CLOSE_UNAUTHORIZED, WS_MAX_TOPICS_PER_OP } from '@/types'

export const WS_CONNECTING = 0
export const WS_OPEN = 1

export interface WebSocketLike {
  readonly readyState: number
  send(data: string): void
  close(code?: number, reason?: string): void
  onopen: ((ev: unknown) => void) | null
  onmessage: ((ev: { data: unknown }) => void) | null
  onclose: ((ev: { code?: number; reason?: string }) => void) | null
  onerror: ((ev: unknown) => void) | null
}

export interface TimerApi {
  setTimeout(fn: () => void, ms: number): unknown
  clearTimeout(handle: unknown): void
  setInterval(fn: () => void, ms: number): unknown
  clearInterval(handle: unknown): void
  now(): number
}

/** Late-bound global timers (so fake timers installed after construction still apply). */
export const defaultTimers: TimerApi = {
  setTimeout: (fn, ms) => setTimeout(fn, ms),
  clearTimeout: (h) => clearTimeout(h as ReturnType<typeof setTimeout>),
  setInterval: (fn, ms) => setInterval(fn, ms),
  clearInterval: (h) => clearInterval(h as ReturnType<typeof setInterval>),
  now: () => Date.now(),
}

export interface BackoffOptions {
  initialMs: number
  maxMs: number
  factor: number
  /** relative jitter, e.g. 0.2 = ±20% */
  jitter: number
}

export const DEFAULT_BACKOFF: BackoffOptions = { initialMs: 500, maxMs: 15_000, factor: 2, jitter: 0.2 }

/** Delay before reconnect attempt `attempt` (0-based): min(max, initial·factor^n)·(1 ± jitter). */
export function backoffDelay(attempt: number, opts: BackoffOptions = DEFAULT_BACKOFF, random: () => number = Math.random): number {
  const base = Math.min(opts.maxMs, opts.initialMs * opts.factor ** Math.max(0, attempt))
  const j = opts.jitter > 0 ? (random() * 2 - 1) * opts.jitter : 0
  return Math.max(0, Math.round(base * (1 + j)))
}

export type SeqVerdict = 'apply' | 'duplicate' | 'gap'

/** Last applied seq per topic. */
export class SeqTracker {
  private readonly last = new Map<string, number>()

  /** Verdict for an incremental frame; the first frame after a reset sets the baseline. */
  check(topic: string, seq: number | undefined | null): SeqVerdict {
    if (typeof seq !== 'number' || !Number.isFinite(seq)) return 'apply'
    const prev = this.last.get(topic)
    if (prev === undefined) {
      this.last.set(topic, seq)
      return 'apply'
    }
    if (seq <= prev) return 'duplicate'
    this.last.set(topic, seq)
    return seq === prev + 1 ? 'apply' : 'gap'
  }

  /** Reset a topic; with a seq (snapshot) that seq becomes the baseline. */
  reset(topic: string, seq?: number | null): void {
    if (typeof seq === 'number' && Number.isFinite(seq)) this.last.set(topic, seq)
    else this.last.delete(topic)
  }

  get(topic: string): number | undefined {
    return this.last.get(topic)
  }

  clear(): void {
    this.last.clear()
  }
}

export type WsStatus = 'idle' | 'connecting' | 'open' | 'reconnecting' | 'closed' | 'no-key' | 'unauthorized'
export type ResyncReason = 'server' | 'gap' | 'reconnect'

export interface WsStats {
  received: number
  gaps: number
  duplicates: number
  resyncRequests: number
  serverResyncs: number
  reconnects: number
  lastMessageMs: number | null
  nextRetryMs: number | null
  lastCloseCode: number | null
}

export interface WsClientOptions {
  url: () => string
  getKey: () => string
  createSocket: (url: string) => WebSocketLike
  /** accepted frames (after seq filtering), including `error` frames */
  onMessage: (msg: WsServerMessage) => void
  /** the app should reload the topic via REST (server resync, event-topic gap, reconnect) */
  onResyncRequired?: (topic: string, reason: ResyncReason) => void
  onStatus?: (status: WsStatus, stats: Readonly<WsStats>) => void
  timers?: TimerApi
  random?: () => number
  backoff?: Partial<BackoffOptions>
  pingIntervalMs?: number
  /** force a reconnect when nothing arrives for this long (server heartbeat is 5s) */
  idleTimeoutMs?: number
  /** also send `resync` right after `sub` (only for servers that do not answer `sub` with a snapshot) */
  resyncOnSubscribe?: boolean
  /** also send `resync` for every topic after a reconnect (the gateway's `sub` snapshots already resync) */
  resyncOnReconnect?: boolean
  /** ask the app to reload event topics via REST after a reconnect */
  reloadEventTopicsOnReconnect?: boolean
  /** re-send a resync if its snapshot has not arrived after this long */
  resyncRetryMs?: number
  /** refuse to connect without an API key (live mode) */
  requireKey?: boolean
  /** 'message' (default): first frame `{"op":"auth"}`; 'query': `?token=` in the URL */
  authMode?: 'message' | 'query'
  /** a connection must stay up this long before a policy / slow-client close streak is forgotten */
  policyResetMs?: number
}

/** Split a topic list into ops of at most `WS_MAX_TOPICS_PER_OP` topics. */
export function chunkTopics(topics: string[], size = WS_MAX_TOPICS_PER_OP): string[][] {
  const out: string[][] = []
  for (let i = 0; i < topics.length; i += size) out.push(topics.slice(i, i + size))
  return out
}

export class WsClient {
  readonly seq = new SeqTracker()
  readonly stats: WsStats = {
    received: 0,
    gaps: 0,
    duplicates: 0,
    resyncRequests: 0,
    serverResyncs: 0,
    reconnects: 0,
    lastMessageMs: null,
    nextRetryMs: null,
    lastCloseCode: null,
  }

  private readonly opts: WsClientOptions
  private readonly timers: TimerApi
  private readonly random: () => number
  private readonly backoff: BackoffOptions
  private readonly refs = new Map<string, number>()
  private readonly pendingResync = new Map<string, number>()
  private socket: WebSocketLike | null = null
  private attempt = 0
  private stopped = true
  private everOpened = false
  private healthy = false
  private reconnectTimer: unknown = null
  private pingTimer: unknown = null
  private idleTimer: unknown = null
  private _status: WsStatus = 'idle'
  /** consecutive policy / too-big / slow-client closes (1008 / 1009 / 1013) of short-lived connections */
  private policyStreak = 0
  private openedAtMs = 0
  /** last `error` frame of the current connection (a 4401 after `BUSY` is the auth throttle) */
  private lastError: { code: string; message: string } | null = null

  constructor(opts: WsClientOptions) {
    this.opts = opts
    this.timers = opts.timers ?? defaultTimers
    this.random = opts.random ?? Math.random
    this.backoff = { ...DEFAULT_BACKOFF, ...(opts.backoff ?? {}) }
  }

  get status(): WsStatus {
    return this._status
  }

  /** Currently subscribed topics. */
  get topics(): string[] {
    return [...this.refs.keys()]
  }

  get isOpen(): boolean {
    return this.socket !== null && this.socket.readyState === WS_OPEN
  }

  start(): void {
    if (!this.stopped) return
    this.stopped = false
    this.connect()
  }

  stop(): void {
    this.stopped = true
    this.clearTimer('reconnect')
    this.teardownSocket(1000, 'client stop')
    this.setStatus('closed')
  }

  /** Reconnect immediately (e.g. after the API key changed). */
  restart(): void {
    this.stop()
    this.attempt = 0
    this.policyStreak = 0
    this.everOpened = false
    this.start()
  }

  /** Subscribe topics; returns an idempotent unsubscribe function. */
  subscribe(topics: string[]): () => void {
    const added: string[] = []
    for (const t of topics) {
      const n = (this.refs.get(t) ?? 0) + 1
      this.refs.set(t, n)
      if (n === 1) added.push(t)
    }
    if (added.length && this.isOpen) {
      this.sendTopics('sub', added)
      if (this.opts.resyncOnSubscribe) this.requestResync(added, null)
    }
    let done = false
    return () => {
      if (done) return
      done = true
      const removed: string[] = []
      for (const t of topics) {
        const n = (this.refs.get(t) ?? 0) - 1
        if (n <= 0) {
          this.refs.delete(t)
          this.seq.reset(t)
          this.pendingResync.delete(t)
          removed.push(t)
        } else this.refs.set(t, n)
      }
      if (removed.length && this.isOpen) this.sendTopics('unsub', removed)
    }
  }

  /** Ask the server for fresh snapshots of `topics` (deduplicated while one is pending). */
  requestResync(topics: string[], reason: ResyncReason | null): void {
    const now = this.timers.now()
    const retry = this.opts.resyncRetryMs ?? 5000
    const wanted = topics.filter((t) => {
      if (!this.refs.has(t)) return false
      const since = this.pendingResync.get(t)
      return since === undefined || now - since >= retry
    })
    if (!wanted.length) return
    if (this.sendTopics('resync', wanted)) {
      for (const t of wanted) this.pendingResync.set(t, now)
      this.stats.resyncRequests += 1
    }
    if (reason === 'gap') {
      for (const t of wanted) if (isEventTopic(t)) this.opts.onResyncRequired?.(t, 'gap')
    }
    this.emitStatus()
  }

  /** Handle one raw frame (exposed for tests and the mock transport). */
  handleRaw(data: unknown): void {
    let msg: unknown
    try {
      msg = typeof data === 'string' ? JSON.parse(data) : data
    } catch {
      return
    }
    if (typeof msg !== 'object' || msg === null || typeof (msg as { type?: unknown }).type !== 'string') return
    this.stats.received += 1
    this.stats.lastMessageMs = this.timers.now()
    if (!this.healthy) {
      this.healthy = true
      this.attempt = this.policyStreak
      this.stats.nextRetryMs = null
    }
    this.armIdle()
    this.handle(msg as WsServerMessage)
  }

  // ---------------------------------------------------------------- internals

  private handle(msg: WsServerMessage): void {
    const topic = msg.topic
    switch (msg.type) {
      case 'pong':
      case 'heartbeat':
        return
      case 'error': {
        const d = msg.data as { code?: unknown; message?: unknown } | null
        this.lastError = d && typeof d === 'object' ? { code: String(d.code ?? ''), message: String(d.message ?? '') } : null
        this.opts.onMessage(msg)
        return
      }
      case 'resync': {
        // the server dropped this topic's queued frames: take a fresh snapshot and reload history
        if (!topic) return
        this.seq.reset(topic)
        this.stats.serverResyncs += 1
        // deduplicated while a requested snapshot is pending (no resync spam)
        this.requestResync([topic], null)
        this.opts.onResyncRequired?.(topic, 'server')
        this.emitStatus()
        return
      }
      case 'snapshot': {
        if (topic) {
          if (!this.refs.has(topic)) return
          this.seq.reset(topic, msg.seq)
          this.pendingResync.delete(topic)
        }
        this.opts.onMessage(msg)
        return
      }
      default: {
        if (topic && !this.refs.has(topic)) return
        const verdict = topic ? this.seq.check(topic, msg.seq) : 'apply'
        if (verdict === 'duplicate') {
          this.stats.duplicates += 1
          return
        }
        if (verdict === 'gap' && topic) {
          this.stats.gaps += 1
          this.requestResync([topic], 'gap')
        }
        this.opts.onMessage(msg)
      }
    }
  }

  private connect(): void {
    this.clearTimer('reconnect')
    if (this.stopped) return
    const key = this.opts.getKey()
    if (this.opts.requireKey && !key) {
      this.setStatus('no-key')
      return
    }
    let url = this.opts.url()
    if (this.opts.authMode === 'query' && key) url += `${url.includes('?') ? '&' : '?'}token=${encodeURIComponent(key)}`
    this.setStatus(this.everOpened || this.attempt > 0 ? 'reconnecting' : 'connecting')
    this.healthy = false
    this.lastError = null
    let sock: WebSocketLike
    try {
      sock = this.opts.createSocket(url)
    } catch {
      this.scheduleReconnect()
      return
    }
    this.socket = sock
    sock.onopen = () => this.onOpen(sock)
    sock.onmessage = (ev) => {
      if (this.socket === sock) this.handleRaw(ev.data)
    }
    sock.onerror = () => {
      // an error is always followed by close
    }
    sock.onclose = (ev) => {
      if (this.socket === sock) this.onClose(ev?.code ?? null)
    }
  }

  private onOpen(sock: WebSocketLike): void {
    if (this.socket !== sock) return
    const reconnect = this.everOpened
    this.everOpened = true
    const key = this.opts.getKey()
    if (key && this.opts.authMode !== 'query') this.send({ op: 'auth', key })
    this.openedAtMs = this.timers.now()
    const topics = this.topics
    if (topics.length) {
      this.sendTopics('sub', topics)
      if (this.opts.resyncOnSubscribe || (reconnect && this.opts.resyncOnReconnect)) this.requestResync(topics, null)
      if (reconnect && this.opts.reloadEventTopicsOnReconnect) {
        for (const t of topics) if (isEventTopic(t)) this.opts.onResyncRequired?.(t, 'reconnect')
      }
    }
    this.setStatus('open')
    this.clearTimer('ping')
    const pingMs = this.opts.pingIntervalMs ?? 10_000
    if (pingMs > 0) this.pingTimer = this.timers.setInterval(() => this.send({ op: 'ping', ts: this.timers.now() }), pingMs)
    this.armIdle()
  }

  private onClose(code: number | null): void {
    this.socket = null
    this.stats.lastCloseCode = code
    this.clearTimer('ping')
    this.clearTimer('idle')
    this.seq.clear()
    this.pendingResync.clear()
    if (this.stopped) {
      this.setStatus('closed')
      return
    }
    const lastError = this.lastError
    this.lastError = null
    if (code === WS_CLOSE_UNAUTHORIZED && lastError?.code === 'BUSY') {
      // auth throttled (too many failed authentications from this address): wait, then retry
      const m = /retry in (\d+(?:\.\d+)?)\s*s/.exec(lastError.message)
      this.openedAtMs = 0
      this.scheduleReconnect(m ? Number(m[1]) * 1000 : 30_000)
      return
    }
    if (code === WS_CLOSE_UNAUTHORIZED) {
      // invalid / disabled API key: retrying with the same key cannot succeed
      this.stats.nextRetryMs = null
      this.setStatus('unauthorized')
      return
    }
    const longLived = this.openedAtMs > 0 && this.timers.now() - this.openedAtMs >= (this.opts.policyResetMs ?? 30_000)
    this.openedAtMs = 0
    if (code === WS_CLOSE_POLICY || code === WS_CLOSE_TOO_BIG || code === WS_CLOSE_TRY_AGAIN) {
      // closed by the gateway's limits (too slow / too big): back off, and do not let the
      // first frame of the next short-lived connection reset the backoff
      this.policyStreak = longLived ? 1 : this.policyStreak + 1
      this.attempt = Math.max(this.attempt, this.policyStreak)
    } else if (longLived) {
      this.policyStreak = 0
      this.attempt = 0
    }
    this.scheduleReconnect()
  }

  private scheduleReconnect(minDelayMs = 0): void {
    this.clearTimer('reconnect')
    const delay = Math.max(minDelayMs, backoffDelay(this.attempt, this.backoff, this.random))
    this.attempt += 1
    this.stats.reconnects += 1
    this.stats.nextRetryMs = this.timers.now() + delay
    this.setStatus('reconnecting')
    this.reconnectTimer = this.timers.setTimeout(() => this.connect(), delay)
  }

  private armIdle(): void {
    this.clearTimer('idle')
    const ms = this.opts.idleTimeoutMs ?? 16_000
    if (ms <= 0 || !this.socket) return
    this.idleTimer = this.timers.setTimeout(() => {
      // no frame (not even the 5s server heartbeat) for too long: reconnect
      this.teardownSocket(4000, 'idle timeout')
      this.onClose(4000)
    }, ms)
  }

  private teardownSocket(code: number, reason: string): void {
    const s = this.socket
    this.socket = null
    this.clearTimer('ping')
    this.clearTimer('idle')
    if (!s) return
    s.onopen = null
    s.onmessage = null
    s.onclose = null
    s.onerror = null
    try {
      s.close(code, reason)
    } catch {
      // ignore
    }
  }

  /** Send a topic op split into chunks the gateway accepts; true when every chunk was sent. */
  private sendTopics(op: 'sub' | 'unsub' | 'resync', topics: string[]): boolean {
    let ok = true
    for (const chunk of chunkTopics(topics)) ok = this.send({ op, topics: chunk }) && ok
    return ok
  }

  private send(op: WsClientOp): boolean {
    const s = this.socket
    if (!s || s.readyState !== WS_OPEN) return false
    try {
      s.send(JSON.stringify(op))
      return true
    } catch {
      return false
    }
  }

  private clearTimer(which: 'reconnect' | 'ping' | 'idle'): void {
    if (which === 'reconnect' && this.reconnectTimer !== null) {
      this.timers.clearTimeout(this.reconnectTimer)
      this.reconnectTimer = null
    } else if (which === 'ping' && this.pingTimer !== null) {
      this.timers.clearInterval(this.pingTimer)
      this.pingTimer = null
    } else if (which === 'idle' && this.idleTimer !== null) {
      this.timers.clearTimeout(this.idleTimer)
      this.idleTimer = null
    }
  }

  private setStatus(s: WsStatus): void {
    this._status = s
    this.emitStatus()
  }

  private emitStatus(): void {
    this.opts.onStatus?.(this._status, this.stats)
  }
}
