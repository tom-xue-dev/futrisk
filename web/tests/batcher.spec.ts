/** Throttled store updates: per-key coalescing within a flush window. */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { Batcher, coalesce, coalesceKey } from '@/realtime/batcher'
import type { WsServerMessage } from '@/types'

const up = (topic: string, data: unknown, seq = 0): WsServerMessage => ({ v: 1, type: 'upsert', topic, seq, data })
const snap = (topic: string, data: unknown, seq = 0): WsServerMessage => ({ v: 1, type: 'snapshot', topic, seq, data })
const ev = (topic: string, data: unknown, seq = 0): WsServerMessage => ({ v: 1, type: 'event', topic, seq, data })

describe('coalesceKey', () => {
  it('keys upserts by item id and full snapshots by topic', () => {
    expect(coalesceKey(up('quotes', { instrument_id: 'rb2701' }), 0)).toBe('U|quotes|rb2701')
    expect(coalesceKey(up('accounts', { account_id: 'ACC3' }), 0)).toBe('U|accounts|ACC3')
    expect(coalesceKey(up('orders:ACC3', { client_order_id: 'o-1' }), 0)).toBe('U|orders:ACC3|o-1')
    expect(coalesceKey(snap('positions:ACC3', []), 0)).toBe('S|positions:ACC3')
    expect(coalesceKey(snap('firm', { equity: 1 }), 0)).toBe('S|firm')
    expect(coalesceKey(snap('system', { instance: 'md-gateway' }), 0)).toBe('U|system|md-gateway')
  })

  it('never coalesces events or equity points', () => {
    expect(coalesceKey(ev('events', { event_id: 'e1' }), 3)).toBe('E|3')
    expect(coalesceKey(up('equity:ACC1', { ts_ns: 1, equity: 2 }), 4)).toBe('E|4')
  })
})

describe('coalesce', () => {
  it('keeps the latest upsert per key, in chronological order of the latest version', () => {
    const out = coalesce([
      up('quotes', { instrument_id: 'rb2701', last: 1 }, 1),
      up('quotes', { instrument_id: 'hc2701', last: 5 }, 2),
      ev('events', { event_id: 'e1' }, 1),
      up('quotes', { instrument_id: 'rb2701', last: 2 }, 3),
    ])
    expect(out.map((m) => [m.topic, m.seq])).toEqual([
      ['quotes', 2],
      ['events', 1],
      ['quotes', 3],
    ])
    expect((out[2]?.data as { last: number }).last).toBe(2)
  })

  it('a full snapshot supersedes earlier upserts of its topic only', () => {
    const out = coalesce([
      up('accounts', { account_id: 'ACC1' }, 1),
      up('quotes', { instrument_id: 'rb2701' }, 1),
      ev('events', { event_id: 'e1' }, 1),
      snap('accounts', [{ account_id: 'ACC1' }, { account_id: 'ACC2' }], 5),
      up('accounts', { account_id: 'ACC2' }, 6),
    ])
    expect(out.map((m) => `${m.type}:${m.topic}:${m.seq}`)).toEqual(['upsert:quotes:1', 'event:events:1', 'snapshot:accounts:5', 'upsert:accounts:6'])
  })

  it('keeps every event and equity point', () => {
    const msgs = [ev('events', { event_id: 'a' }, 1), ev('events', { event_id: 'b' }, 2), up('equity:ACC3', { ts_ns: 1 }, 1), up('equity:ACC3', { ts_ns: 2 }, 2)]
    expect(coalesce(msgs)).toHaveLength(4)
  })
})

describe('Batcher', () => {
  beforeEach(() => {
    vi.useFakeTimers()
  })
  afterEach(() => {
    vi.useRealTimers()
  })

  it('flushes coalesced frames at most once per interval', () => {
    const flushed: WsServerMessage[][] = []
    const b = new Batcher((m) => flushed.push(m), () => 200)
    b.push(up('quotes', { instrument_id: 'rb2701', last: 1 }, 1))
    b.push(up('quotes', { instrument_id: 'rb2701', last: 2 }, 2))
    b.push(up('quotes', { instrument_id: 'rb2701', last: 3 }, 3))
    expect(flushed).toHaveLength(0)
    expect(b.pending).toBe(3)
    vi.advanceTimersByTime(199)
    expect(flushed).toHaveLength(0)
    vi.advanceTimersByTime(1)
    expect(flushed).toHaveLength(1)
    expect(flushed[0]?.map((m) => m.seq)).toEqual([3])
    b.push(ev('events', { event_id: 'x' }, 1))
    vi.advanceTimersByTime(200)
    expect(flushed).toHaveLength(2)
  })

  it('flush() delivers immediately and cancels the timer', () => {
    const flushed: WsServerMessage[][] = []
    const b = new Batcher((m) => flushed.push(m), () => 1000)
    b.push(up('accounts', { account_id: 'ACC1' }, 1))
    b.flush()
    expect(flushed).toHaveLength(1)
    vi.advanceTimersByTime(2000)
    expect(flushed).toHaveLength(1)
  })
})
