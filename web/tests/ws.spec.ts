/**
 * WebSocket client (02 §9.7, wshub.py): auth frame, subscriptions, per-topic seq
 * tracking, gap -> resync, snapshot baseline, server resync, reconnect with exponential
 * backoff (resubscribe; the gateway's `sub` snapshots are the resync), 4401 rejected key,
 * auth throttle (4401 after BUSY: retry after the wait), gateway limits (100 topics per op,
 * 1008 / 1009 / 1013 closes), error frames, idle watchdog, ping, API-key
 * requirement.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { backoffDelay, chunkTopics, DEFAULT_BACKOFF, SeqTracker, WsClient, type WebSocketLike, type WsClientOptions } from '@/api/ws'
import type { WsClientOp, WsServerMessage } from '@/types'

class FakeSocket implements WebSocketLike {
  readyState = 0
  sent: WsClientOp[] = []
  onopen: ((ev: unknown) => void) | null = null
  onmessage: ((ev: { data: unknown }) => void) | null = null
  onclose: ((ev: { code?: number; reason?: string }) => void) | null = null
  onerror: ((ev: unknown) => void) | null = null
  closedWith: number | null = null

  constructor(readonly url: string) {}

  send(data: string): void {
    this.sent.push(JSON.parse(data) as WsClientOp)
  }

  close(code = 1000): void {
    this.closedWith = code
    this.readyState = 3
    this.onclose?.({ code })
  }

  open(): void {
    this.readyState = 1
    this.onopen?.({})
  }

  recv(msg: Partial<WsServerMessage>): void {
    this.onmessage?.({ data: JSON.stringify({ v: 1, ...msg }) })
  }

  drop(code = 1006): void {
    this.readyState = 3
    this.onclose?.({ code })
  }

  ops(op: WsClientOp['op']): WsClientOp[] {
    return this.sent.filter((s) => s.op === op)
  }
}

interface Harness {
  client: WsClient
  sockets: FakeSocket[]
  messages: WsServerMessage[]
  resyncs: [string, string][]
  last: () => FakeSocket
}

function harness(opts: Partial<WsClientOptions> = {}): Harness {
  const sockets: FakeSocket[] = []
  const messages: WsServerMessage[] = []
  const resyncs: [string, string][] = []
  const client = new WsClient({
    url: () => 'ws://gw.test/ws',
    getKey: () => 'secret-key',
    createSocket: (url) => {
      const s = new FakeSocket(url)
      sockets.push(s)
      return s
    },
    onMessage: (m) => messages.push(m),
    onResyncRequired: (topic, reason) => resyncs.push([topic, reason]),
    backoff: { jitter: 0 },
    pingIntervalMs: 10_000,
    idleTimeoutMs: 16_000,
    ...opts,
  })
  return { client, sockets, messages, resyncs, last: () => sockets[sockets.length - 1] as FakeSocket }
}

beforeEach(() => {
  vi.useFakeTimers()
})

afterEach(() => {
  vi.useRealTimers()
})

describe('SeqTracker', () => {
  it('sets the baseline from the first frame, then applies / drops / flags gaps', () => {
    const t = new SeqTracker()
    expect(t.check('quotes', 7)).toBe('apply')
    expect(t.check('quotes', 8)).toBe('apply')
    expect(t.check('quotes', 8)).toBe('duplicate')
    expect(t.check('quotes', 3)).toBe('duplicate')
    expect(t.check('quotes', 11)).toBe('gap')
    expect(t.get('quotes')).toBe(11)
    expect(t.check('quotes', 12)).toBe('apply')
  })

  it('tracks topics independently and resets to a snapshot seq', () => {
    const t = new SeqTracker()
    t.check('a', 1)
    t.check('b', 100)
    expect(t.check('a', 2)).toBe('apply')
    expect(t.check('b', 101)).toBe('apply')
    t.reset('a', 50)
    expect(t.check('a', 50)).toBe('duplicate')
    expect(t.check('a', 51)).toBe('apply')
    t.reset('b')
    expect(t.check('b', 5)).toBe('apply')
  })

  it('applies frames without a numeric seq', () => {
    const t = new SeqTracker()
    expect(t.check('x', undefined)).toBe('apply')
    expect(t.check('x', null)).toBe('apply')
  })
})

describe('backoffDelay', () => {
  it('doubles from 500ms and caps at the maximum', () => {
    const opts = { ...DEFAULT_BACKOFF, jitter: 0 }
    expect([0, 1, 2, 3, 4, 5, 6, 9].map((n) => backoffDelay(n, opts))).toEqual([500, 1000, 2000, 4000, 8000, 15_000, 15_000, 15_000])
  })

  it('applies bounded jitter', () => {
    const opts = { ...DEFAULT_BACKOFF, jitter: 0.2 }
    expect(backoffDelay(1, opts, () => 0)).toBe(800)
    expect(backoffDelay(1, opts, () => 1)).toBe(1200)
    expect(backoffDelay(1, opts, () => 0.5)).toBe(1000)
  })
})

describe('WsClient', () => {
  it('authenticates with the first frame (key not in the URL) and subscribes pending topics', () => {
    const h = harness()
    h.client.subscribe(['accounts', 'events'])
    h.client.start()
    const s = h.last()
    expect(s.url).toBe('ws://gw.test/ws')
    expect(s.url).not.toContain('secret-key')
    s.open()
    expect(s.sent[0]).toEqual({ op: 'auth', key: 'secret-key' })
    expect(s.sent[1]).toEqual({ op: 'sub', topics: ['accounts', 'events'] })
    expect(s.ops('resync')).toHaveLength(0)
    expect(h.client.status).toBe('open')
  })

  it('subscribing while open sends sub (and resync when resyncOnSubscribe)', () => {
    const h = harness({ resyncOnSubscribe: true })
    h.client.start()
    const s = h.last()
    s.open()
    h.client.subscribe(['positions:ACC3'])
    expect(s.ops('sub')).toContainEqual({ op: 'sub', topics: ['positions:ACC3'] })
    expect(s.ops('resync')).toContainEqual({ op: 'resync', topics: ['positions:ACC3'] })
  })

  it('applies in-order frames and drops duplicates', () => {
    const h = harness()
    h.client.subscribe(['accounts'])
    h.client.start()
    const s = h.last()
    s.open()
    for (const seq of [1, 2, 3]) s.recv({ type: 'upsert', topic: 'accounts', seq, data: { account_id: 'ACC1' } })
    s.recv({ type: 'upsert', topic: 'accounts', seq: 2, data: { account_id: 'ACC1' } })
    expect(h.messages.map((m) => m.seq)).toEqual([1, 2, 3])
    expect(h.client.stats.duplicates).toBe(1)
    expect(h.client.stats.gaps).toBe(0)
  })

  it('on a gap applies the newer frame and requests one resync until the snapshot arrives', () => {
    const h = harness()
    h.client.subscribe(['accounts'])
    h.client.start()
    const s = h.last()
    s.open()
    s.recv({ type: 'upsert', topic: 'accounts', seq: 1 })
    s.recv({ type: 'upsert', topic: 'accounts', seq: 4 })
    expect(h.messages.map((m) => m.seq)).toEqual([1, 4])
    expect(h.client.stats.gaps).toBe(1)
    expect(s.ops('resync')).toEqual([{ op: 'resync', topics: ['accounts'] }])
    // another gap while the resync is pending: counted, not re-requested
    s.recv({ type: 'upsert', topic: 'accounts', seq: 7 })
    expect(h.client.stats.gaps).toBe(2)
    expect(s.ops('resync')).toHaveLength(1)
    // the snapshot resets the baseline
    s.recv({ type: 'snapshot', topic: 'accounts', seq: 10, data: [] })
    s.recv({ type: 'upsert', topic: 'accounts', seq: 11 })
    expect(h.client.stats.gaps).toBe(2)
    expect(h.messages.map((m) => m.seq)).toEqual([1, 4, 7, 10, 11])
    // a later gap may request again
    s.recv({ type: 'upsert', topic: 'accounts', seq: 13 })
    expect(s.ops('resync')).toHaveLength(2)
  })

  it('re-sends a resync when its snapshot does not arrive in time', () => {
    const h = harness({ resyncRetryMs: 5000 })
    h.client.subscribe(['quotes'])
    h.client.start()
    const s = h.last()
    s.open()
    s.recv({ type: 'upsert', topic: 'quotes', seq: 1 })
    s.recv({ type: 'upsert', topic: 'quotes', seq: 3 })
    expect(s.ops('resync')).toHaveLength(1)
    vi.advanceTimersByTime(5000)
    s.recv({ type: 'upsert', topic: 'quotes', seq: 6 })
    expect(s.ops('resync')).toHaveLength(2)
  })

  it('a snapshot sets the baseline: older in-flight frames are duplicates', () => {
    const h = harness()
    h.client.subscribe(['firm'])
    h.client.start()
    const s = h.last()
    s.open()
    s.recv({ type: 'snapshot', topic: 'firm', seq: 50, data: {} })
    s.recv({ type: 'upsert', topic: 'firm', seq: 49 })
    s.recv({ type: 'upsert', topic: 'firm', seq: 51 })
    expect(h.messages.map((m) => m.seq)).toEqual([50, 51])
    expect(h.client.stats.duplicates).toBe(1)
  })

  it('only event topics trigger a REST reload on a gap', () => {
    const h = harness()
    h.client.subscribe(['events', 'accounts', 'orders:ACC3'])
    h.client.start()
    const s = h.last()
    s.open()
    s.recv({ type: 'event', topic: 'events', seq: 1 })
    s.recv({ type: 'event', topic: 'events', seq: 3 })
    s.recv({ type: 'upsert', topic: 'accounts', seq: 1 })
    s.recv({ type: 'upsert', topic: 'accounts', seq: 5 })
    s.recv({ type: 'upsert', topic: 'orders:ACC3', seq: 1 })
    s.recv({ type: 'upsert', topic: 'orders:ACC3', seq: 9 })
    expect(h.resyncs).toEqual([
      ['events', 'gap'],
      ['orders:ACC3', 'gap'],
    ])
  })

  it('a server resync frame resets the baseline and asks for a REST reload', () => {
    const h = harness()
    h.client.subscribe(['events'])
    h.client.start()
    const s = h.last()
    s.open()
    s.recv({ type: 'event', topic: 'events', seq: 1 })
    s.recv({ type: 'resync', topic: 'events', seq: 2, data: null })
    expect(h.resyncs).toEqual([['events', 'server']])
    // a fresh snapshot is requested once (the gateway ignores it while one is queued)
    expect(s.ops('resync')).toEqual([{ op: 'resync', topics: ['events'] }])
    s.recv({ type: 'resync', topic: 'events', seq: 3, data: null })
    expect(s.ops('resync')).toHaveLength(1)
    s.recv({ type: 'event', topic: 'events', seq: 500 })
    expect(h.client.stats.gaps).toBe(0)
    expect(h.client.stats.serverResyncs).toBe(2)
    expect(h.messages.map((m) => m.seq)).toEqual([1, 500])
  })

  it('ignores control frames and frames of topics it is not subscribed to', () => {
    const h = harness()
    h.client.subscribe(['quotes'])
    h.client.start()
    const s = h.last()
    s.open()
    s.recv({ type: 'heartbeat', ts: 1 })
    s.recv({ type: 'pong', ts: 1 })
    s.recv({ type: 'upsert', topic: 'system', seq: 1 })
    s.recv({ type: 'snapshot', topic: 'system', seq: 1 })
    expect(h.messages).toHaveLength(0)
    s.recv({ type: 'error', data: { code: 'UNAUTHORIZED' } })
    expect(h.messages.map((m) => m.type)).toEqual(['error'])
    expect(h.client.stats.received).toBe(5)
  })

  it('releases a topic only when the last reference unsubscribes', () => {
    const h = harness()
    h.client.start()
    const s = h.last()
    s.open()
    const a = h.client.subscribe(['quotes'])
    const b = h.client.subscribe(['quotes'])
    expect(s.ops('sub')).toEqual([{ op: 'sub', topics: ['quotes'] }])
    a()
    a()
    expect(s.ops('unsub')).toHaveLength(0)
    b()
    expect(s.ops('unsub')).toEqual([{ op: 'unsub', topics: ['quotes'] }])
    s.recv({ type: 'upsert', topic: 'quotes', seq: 1 })
    expect(h.messages).toHaveLength(0)
    expect(h.client.topics).toEqual([])
  })

  it('reconnects with exponential backoff, then resubscribes and reloads event topics', () => {
    const h = harness({ reloadEventTopicsOnReconnect: true })
    h.client.subscribe(['accounts', 'events'])
    h.client.start()
    h.last().open()
    h.last().recv({ type: 'upsert', topic: 'accounts', seq: 1 })
    h.last().drop()
    expect(h.client.status).toBe('reconnecting')
    expect(h.sockets).toHaveLength(1)
    vi.advanceTimersByTime(499)
    expect(h.sockets).toHaveLength(1)
    vi.advanceTimersByTime(1)
    expect(h.sockets).toHaveLength(2)
    // fails before any frame: the next delay doubles
    h.last().drop()
    vi.advanceTimersByTime(999)
    expect(h.sockets).toHaveLength(2)
    vi.advanceTimersByTime(1)
    expect(h.sockets).toHaveLength(3)
    h.last().drop()
    vi.advanceTimersByTime(2000)
    expect(h.sockets).toHaveLength(4)
    const s = h.last()
    s.open()
    expect(s.sent[0]).toEqual({ op: 'auth', key: 'secret-key' })
    expect(s.sent[1]).toEqual({ op: 'sub', topics: ['accounts', 'events'] })
    // the gateway answers every sub with a snapshot: no extra resync op
    expect(s.ops('resync')).toHaveLength(0)
    expect(h.resyncs).toContainEqual(['events', 'reconnect'])
    expect(h.resyncs).not.toContainEqual(['accounts', 'reconnect'])
    expect(h.client.stats.reconnects).toBe(3)
    // baselines were cleared: any seq is accepted after the reconnect
    s.recv({ type: 'upsert', topic: 'accounts', seq: 1 })
    expect(h.client.stats.duplicates).toBe(0)
  })

  it('resets the backoff after a healthy connection', () => {
    const h = harness()
    h.client.subscribe(['accounts'])
    h.client.start()
    h.last().drop()
    vi.advanceTimersByTime(500)
    h.last().drop()
    vi.advanceTimersByTime(1000)
    h.last().open()
    h.last().recv({ type: 'heartbeat' })
    h.last().drop()
    vi.advanceTimersByTime(500)
    expect(h.sockets).toHaveLength(4)
  })

  it('forces a reconnect when the connection goes silent (idle watchdog)', () => {
    const h = harness({ idleTimeoutMs: 16_000 })
    h.client.subscribe(['accounts'])
    h.client.start()
    const s = h.last()
    s.open()
    s.recv({ type: 'heartbeat' })
    vi.advanceTimersByTime(15_999)
    expect(s.closedWith).toBeNull()
    vi.advanceTimersByTime(1)
    expect(s.closedWith).toBe(4000)
    expect(h.client.status).toBe('reconnecting')
    vi.advanceTimersByTime(500)
    expect(h.sockets).toHaveLength(2)
  })

  it('pings periodically while open', () => {
    const h = harness({ pingIntervalMs: 10_000, idleTimeoutMs: 0 })
    h.client.start()
    const s = h.last()
    s.open()
    vi.advanceTimersByTime(30_000)
    expect(s.ops('ping')).toHaveLength(3)
  })

  it('does not connect without an API key when one is required', () => {
    const h = harness({ requireKey: true, getKey: () => '' })
    h.client.start()
    expect(h.sockets).toHaveLength(0)
    expect(h.client.status).toBe('no-key')
  })

  it('stop() closes the socket and cancels reconnects', () => {
    const h = harness()
    h.client.start()
    h.last().open()
    h.client.stop()
    expect(h.client.status).toBe('closed')
    vi.advanceTimersByTime(60_000)
    expect(h.sockets).toHaveLength(1)
  })
})

describe('WsClient: gateway limits and rejected keys', () => {
  it('can still send an explicit resync after a reconnect when asked to', () => {
    const h = harness({ resyncOnReconnect: true })
    h.client.subscribe(['accounts'])
    h.client.start()
    h.last().open()
    h.last().drop()
    vi.advanceTimersByTime(500)
    h.last().open()
    expect(h.last().ops('resync')).toEqual([{ op: 'resync', topics: ['accounts'] }])
  })

  it('stops reconnecting after close 4401 (rejected API key) until restart()', () => {
    const h = harness()
    h.client.subscribe(['accounts'])
    h.client.start()
    h.last().open()
    h.last().recv({ type: 'error', data: { code: 'UNAUTHORIZED', message: 'invalid API key' } })
    h.last().drop(4401)
    expect(h.client.status).toBe('unauthorized')
    vi.advanceTimersByTime(60_000)
    expect(h.sockets).toHaveLength(1)
    expect(h.messages[0]?.type).toBe('error')
    h.client.restart()
    expect(h.sockets).toHaveLength(2)
  })

  it('retries after the wait of an auth throttle (4401 after an error frame with code BUSY)', () => {
    const h = harness()
    h.client.subscribe(['accounts'])
    h.client.start()
    h.last().open()
    h.last().recv({ type: 'error', data: { code: 'BUSY', message: 'too many failed authentications, retry in 7s' } })
    h.last().drop(4401)
    expect(h.client.status).toBe('reconnecting')
    vi.advanceTimersByTime(6_999)
    expect(h.sockets).toHaveLength(1)
    vi.advanceTimersByTime(1)
    expect(h.sockets).toHaveLength(2)
    // a later 4401 without the throttle error is a rejected key again
    h.last().open()
    h.last().drop(4401)
    expect(h.client.status).toBe('unauthorized')
  })

  it('passes error frames (one per failed op, unknown topics listed) to the app', () => {
    const h = harness()
    h.client.subscribe(['quotes', 'nope'])
    h.client.start()
    h.last().open()
    h.last().recv({ type: 'error', ts: 1, mts: 1, data: { code: 'BAD_REQUEST', message: 'unknown topics', topics: ['nope'] } })
    expect(h.messages).toEqual([expect.objectContaining({ type: 'error', data: { code: 'BAD_REQUEST', message: 'unknown topics', topics: ['nope'] } })])
    expect(h.client.status).toBe('open')
  })

  it('splits sub / unsub / resync ops into at most 100 topics', () => {
    expect(chunkTopics(Array.from({ length: 230 }, (_, i) => `t${i}`)).map((c) => c.length)).toEqual([100, 100, 30])
    const h = harness()
    const topics = Array.from({ length: 150 }, (_, i) => `positions:ACC${i}`)
    h.client.subscribe(topics)
    h.client.start()
    h.last().open()
    const subs = h.last().ops('sub') as { op: 'sub'; topics: string[] }[]
    expect(subs.map((o) => o.topics.length)).toEqual([100, 50])
    expect(subs.flatMap((o) => o.topics)).toEqual(topics)
  })

  it('backs off after policy / slow-client closes even though the connection sent frames', () => {
    const h = harness()
    h.client.subscribe(['quotes'])
    h.client.start()
    h.last().open()
    h.last().recv({ type: 'heartbeat' })
    h.last().drop(1013)
    // a normal close after a healthy frame would retry after 500 ms; 1013 waits 1 s
    vi.advanceTimersByTime(999)
    expect(h.sockets).toHaveLength(1)
    vi.advanceTimersByTime(1)
    expect(h.sockets).toHaveLength(2)
    h.last().open()
    h.last().recv({ type: 'heartbeat' })
    h.last().drop(1008)
    vi.advanceTimersByTime(1999)
    expect(h.sockets).toHaveLength(2)
    vi.advanceTimersByTime(1)
    expect(h.sockets).toHaveLength(3)
    // a too big message (1009) continues the streak
    h.last().open()
    h.last().recv({ type: 'heartbeat' })
    h.last().drop(1009)
    vi.advanceTimersByTime(3999)
    expect(h.sockets).toHaveLength(3)
    vi.advanceTimersByTime(1)
    expect(h.sockets).toHaveLength(4)
    // a connection that stayed up long enough forgets the streak
    h.last().open()
    h.last().recv({ type: 'heartbeat' })
    for (let i = 0; i < 4; i++) {
      vi.advanceTimersByTime(10_000)
      h.last().recv({ type: 'heartbeat' })
    }
    h.last().drop(1006)
    vi.advanceTimersByTime(500)
    expect(h.sockets).toHaveLength(5)
  })
})
