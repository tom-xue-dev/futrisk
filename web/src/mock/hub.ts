/**
 * Mock WsHub: speaks the gateway's WS protocol (`wshub.py`) to MockSocket clients.
 *  - `auth` / `sub` / `unsub` / `resync` / `ping` ops; `sub` of a new topic and `resync`
 *    answer with a `snapshot`; one `error` frame per `sub` op lists its unknown topics
 *    (`data.topics`); more than 100 topics in one op, or more than 500 subscriptions per
 *    session, is an `error` frame too; a message over 64 KiB closes the session (1009);
 *  - topic frames `{v, type, topic, seq, ts, mts, td, data}`: `seq` per topic and
 *    connection from 1, `ts` wall-clock ms, `mts` market-clock ms (review fix);
 *    topic-less frames `{v, type, ts, mts, data}` (heartbeat every 5 s with data null,
 *    pong, error);
 *  - demo controls: skip a seq number (client sees a gap and resyncs), a server `resync`
 *    frame (slow-client overflow of an event topic: reload via REST), drop every
 *    connection (reconnect with backoff).
 */
import type { WsClientOp, WsServerMessage } from '@/types'
import { isEventTopic, WS_MAX_TOPICS_PER_OP } from '@/types'
import type { WebSocketLike } from '@/api/ws'
import type { FrameType } from './engine'

export type SnapshotProvider = (topic: string) => unknown

interface Conn {
  socket: MockSocket
  authed: boolean
  topics: Set<string>
  seq: Map<string, number>
  skip: Set<string>
}

const STATIC_TOPICS = new Set(['quotes', 'feeds', 'accounts', 'events', 'compare', 'firm', 'system', 'eod', 'backtests'])
const KEYED = ['positions:', 'orders:', 'equity:']
const MAX_TOPICS_PER_CLIENT = 500

export class MockHub {
  private readonly conns = new Set<Conn>()
  private snapshot: SnapshotProvider = () => null
  private tdOf: () => string = () => ''
  private mtsOf: () => number = () => Date.now()
  private hasAccount: (id: string) => boolean = () => true
  private readonly hb: ReturnType<typeof setInterval>

  constructor() {
    this.hb = setInterval(() => {
      for (const c of this.conns) this.send(c, { v: 1, type: 'heartbeat', ts: Date.now(), mts: this.mtsOf(), data: null })
    }, 5000)
  }

  setProviders(snapshot: SnapshotProvider, tradingDay: () => string, marketNowMs: () => number, hasAccount: (id: string) => boolean): void {
    this.snapshot = snapshot
    this.tdOf = tradingDay
    this.mtsOf = marketNowMs
    this.hasAccount = hasAccount
  }

  attach(socket: MockSocket): Conn {
    const c: Conn = { socket, authed: false, topics: new Set(), seq: new Map(), skip: new Set() }
    this.conns.add(c)
    return c
  }

  detach(c: Conn): void {
    this.conns.delete(c)
  }

  private validTopic(t: string): boolean {
    if (STATIC_TOPICS.has(t)) return true
    const p = KEYED.find((k) => t.startsWith(k))
    return p !== undefined && this.hasAccount(t.slice(p.length))
  }

  private nextSeq(c: Conn, topic: string): number {
    let n = (c.seq.get(topic) ?? 0) + 1
    if (c.skip.has(topic)) {
      c.skip.delete(topic)
      n += 1 // simulated loss: the client sees a gap
    }
    c.seq.set(topic, n)
    return n
  }

  private send(c: Conn, msg: WsServerMessage): void {
    c.socket.deliver(JSON.stringify(msg))
  }

  private error(c: Conn, code: string, message: string, extra: Record<string, unknown> = {}): void {
    this.send(c, { v: 1, type: 'error', ts: Date.now(), mts: this.mtsOf(), data: { code, message, ...extra } })
  }

  private sendTopic(c: Conn, type: WsServerMessage['type'], topic: string, data: unknown): void {
    this.send(c, { v: 1, type, topic, seq: this.nextSeq(c, topic), ts: Date.now(), mts: this.mtsOf(), td: this.tdOf(), data })
  }

  handle(c: Conn, raw: string): void {
    if (raw.length > 64 * 1024) {
      c.socket.serverClose(1009, 'message too big')
      return
    }
    let op: WsClientOp
    try {
      op = JSON.parse(raw) as WsClientOp
    } catch {
      this.error(c, 'BAD_REQUEST', 'invalid JSON')
      return
    }
    if ((op.op === 'sub' || op.op === 'unsub' || op.op === 'resync') && (!Array.isArray(op.topics) || op.topics.some((t) => typeof t !== 'string'))) {
      this.error(c, 'BAD_REQUEST', 'topics must be a list of strings')
      return
    }
    if ((op.op === 'sub' || op.op === 'unsub' || op.op === 'resync') && op.topics.length > WS_MAX_TOPICS_PER_OP) {
      this.error(c, 'BAD_REQUEST', `at most ${WS_MAX_TOPICS_PER_OP} topics per op`)
      return
    }
    switch (op.op) {
      case 'auth':
        c.authed = true
        break
      case 'sub': {
        const unknown: string[] = []
        for (const t of op.topics) {
          if (!this.validTopic(t)) {
            unknown.push(t)
            continue
          }
          if (c.topics.has(t)) continue
          if (c.topics.size >= MAX_TOPICS_PER_CLIENT) {
            this.error(c, 'BAD_REQUEST', `at most ${MAX_TOPICS_PER_CLIENT} subscriptions per session`)
            break
          }
          c.topics.add(t)
          this.sendTopic(c, 'snapshot', t, this.snapshot(t))
        }
        // one error frame per op, listing the unknown topics
        if (unknown.length) this.error(c, 'BAD_REQUEST', 'unknown topics', { topics: unknown.slice(0, 20) })
        break
      }
      case 'unsub':
        for (const t of op.topics) {
          c.topics.delete(t)
          c.seq.delete(t)
        }
        break
      case 'resync':
        for (const t of op.topics) if (c.topics.has(t)) this.sendTopic(c, 'snapshot', t, this.snapshot(t))
        break
      case 'ping':
        this.send(c, { v: 1, type: 'pong', ts: Date.now(), mts: this.mtsOf(), data: { ts: op.ts } })
        break
      default:
        this.error(c, 'BAD_REQUEST', `unknown op '${(op as { op?: unknown }).op}'`)
    }
  }

  /** Engine -> clients. */
  publish(topic: string, type: FrameType, data: unknown): void {
    if (!this.conns.size) return
    let payload: string | null = null
    const ts = Date.now()
    const mts = Math.round(this.mtsOf())
    for (const c of this.conns) {
      if (!c.topics.has(topic)) continue
      const seq = this.nextSeq(c, topic)
      if (payload === null) payload = JSON.stringify(data)
      c.socket.deliver(`{"v":1,"type":"${type}","topic":${JSON.stringify(topic)},"seq":${seq},"ts":${ts},"mts":${mts},"td":${JSON.stringify(this.tdOf())},"data":${payload}}`)
    }
  }

  /** Demo: the next frame of `topic` (or of every subscribed topic) skips one seq number. */
  dropNextSeq(topic?: string): void {
    for (const c of this.conns) for (const t of topic ? [topic] : [...c.topics]) if (c.topics.has(t)) c.skip.add(t)
  }

  /** Demo: a slow-client overflow of an event topic — the gateway drops its queue and sends `resync`. */
  serverResync(topic = 'events'): void {
    if (!isEventTopic(topic)) return
    for (const c of this.conns) if (c.topics.has(topic)) this.sendTopic(c, 'resync', topic, null)
  }

  /** Demo: drop every connection (abnormal closure 1006). */
  disconnectAll(): void {
    for (const c of [...this.conns]) c.socket.serverClose(1006, 'mock disconnect')
  }

  dispose(): void {
    clearInterval(this.hb)
  }
}

/** In-memory WebSocket implementing the subset of the browser API the client uses. */
export class MockSocket implements WebSocketLike {
  readyState = 0
  onopen: ((ev: unknown) => void) | null = null
  onmessage: ((ev: { data: unknown }) => void) | null = null
  onclose: ((ev: { code?: number; reason?: string }) => void) | null = null
  onerror: ((ev: unknown) => void) | null = null
  private readonly hub: MockHub
  private conn: Conn | null = null

  constructor(hub: MockHub, refuse = false) {
    this.hub = hub
    setTimeout(() => {
      if (this.readyState !== 0) return
      if (refuse) {
        this.readyState = 3
        this.onerror?.({})
        this.onclose?.({ code: 1006, reason: 'gateway down' })
        return
      }
      this.readyState = 1
      this.conn = this.hub.attach(this)
      this.onopen?.({})
    }, 30)
  }

  send(data: string): void {
    if (this.readyState !== 1 || !this.conn) return
    const c = this.conn
    setTimeout(() => this.hub.handle(c, data), 5)
  }

  deliver(data: string): void {
    if (this.readyState !== 1) return
    setTimeout(() => {
      if (this.readyState === 1) this.onmessage?.({ data })
    }, 0)
  }

  close(code = 1000, reason = ''): void {
    if (this.readyState >= 2) return
    this.readyState = 3
    if (this.conn) this.hub.detach(this.conn)
    this.conn = null
    setTimeout(() => this.onclose?.({ code, reason }), 0)
  }

  serverClose(code: number, reason: string): void {
    this.close(code, reason)
  }
}
