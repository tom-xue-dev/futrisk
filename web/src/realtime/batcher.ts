/**
 * Throttled store updates.
 *
 * Accepted WS frames are buffered and flushed to the stores at most every `intervalMs`
 * (default 200ms). Within one flush window state frames are coalesced so the stores
 * (and charts) do one update per key instead of one per frame:
 *
 *  - upserts of the same topic + key (instrument_id, account_id, client_order_id,
 *    instance, ...) collapse to the latest one;
 *  - a full `snapshot` of a topic (array data, or an object without an item key)
 *    supersedes earlier upserts / snapshots of that topic in the window;
 *  - event frames (and equity points, which append) are never coalesced and keep their
 *    order.
 */
import type { WsServerMessage } from '@/types'
import { defaultTimers, type TimerApi } from '@/api/ws'

const ITEM_KEYS = ['instrument_id', 'account_id', 'client_order_id', 'instance', 'run_id', 'report_id'] as const

function itemKey(topic: string, data: unknown): string | null {
  if (topic.startsWith('equity:')) return null
  if (typeof data !== 'object' || data === null || Array.isArray(data)) return null
  const d = data as Record<string, unknown>
  for (const k of ITEM_KEYS) {
    const v = d[k]
    if (typeof v === 'string' && v) return v
  }
  return null
}

/** Coalescing key: `S|topic` (full snapshot), `U|topic|key` (keyed state), `E|n` (never coalesced). */
export function coalesceKey(msg: WsServerMessage, index: number): string {
  const topic = msg.topic ?? ''
  if (msg.type === 'snapshot') {
    const k = itemKey(topic, msg.data)
    return k ? `U|${topic}|${k}` : `S|${topic}`
  }
  if (msg.type === 'upsert' || msg.type === 'delete') {
    const k = itemKey(topic, msg.data)
    return k ? `U|${topic}|${k}` : `E|${index}`
  }
  return `E|${index}`
}

/** Coalesce one flush window; the surviving frames keep chronological order of their latest version. */
export function coalesce(msgs: WsServerMessage[]): WsServerMessage[] {
  const out = new Map<string, WsServerMessage>()
  msgs.forEach((m, i) => {
    const key = coalesceKey(m, i)
    if (key.startsWith('S|')) {
      const prefix = `U|${m.topic ?? ''}|`
      for (const k of [...out.keys()]) if (k.startsWith(prefix) || k === key) out.delete(k)
    } else if (out.has(key)) {
      out.delete(key)
    }
    out.set(key, m)
  })
  return [...out.values()]
}

export class Batcher {
  private buf: WsServerMessage[] = []
  private timer: unknown = null
  private readonly flushFn: (msgs: WsServerMessage[]) => void
  private readonly intervalMs: () => number
  private readonly timers: TimerApi

  constructor(flushFn: (msgs: WsServerMessage[]) => void, intervalMs: () => number = () => 200, timers: TimerApi = defaultTimers) {
    this.flushFn = flushFn
    this.intervalMs = intervalMs
    this.timers = timers
  }

  push(msg: WsServerMessage): void {
    this.buf.push(msg)
    if (this.timer === null) this.timer = this.timers.setTimeout(() => this.flush(), Math.max(0, this.intervalMs()))
  }

  flush(): void {
    if (this.timer !== null) {
      this.timers.clearTimeout(this.timer)
      this.timer = null
    }
    if (!this.buf.length) return
    const msgs = coalesce(this.buf)
    this.buf = []
    this.flushFn(msgs)
  }

  get pending(): number {
    return this.buf.length
  }
}
