/**
 * Market data: WS `quotes` (2Hz upserts), WS `feeds` (MdStatus snapshots + MdEvent
 * events), `GET /market/quotes`, `GET /market/feeds`, `GET /market/events`.
 */
import { defineStore } from 'pinia'
import type { InstrumentStatus, MdEvent, MdStatus, QuoteView, SourceStatus, WsServerMessage } from '@/types'
import { rt } from '@/runtime'
import { asItems, isMdEvent, isMdStatus, toQuote } from '@/utils/normalize'
import { toLoadError, type LoadError } from './system'

const MAX_EVENTS = 800
const seenEvents = new Set<string>()

export const useMarket = defineStore('market', {
  state: () => ({
    quotes: {} as Record<string, QuoteView>,
    status: null as MdStatus | null,
    statusAt: 0,
    events: [] as MdEvent[],
    eventsLoaded: false,
    /** last error of GET /market/feeds (md-gateway down and no md.status in the read model) */
    feedsError: null as LoadError | null,
    /** md-gateway liveness of the last GET /market/feeds: `stale` = its last md.status, not live */
    feedsStale: false,
    feedsAgeMs: null as number | null,
    /** last error of GET /market/events (answered by md-gateway only) */
    eventsError: null as LoadError | null,
  }),
  getters: {
    instrumentStatus(state): Record<string, InstrumentStatus> {
      const out: Record<string, InstrumentStatus> = {}
      for (const i of state.status?.instruments ?? []) out[i.instrument_id] = i
      return out
    },
    sources(state): SourceStatus[] {
      return state.status?.sources ?? []
    },
    /** Quotes merged with instrument quality / limit lock from md.status. */
    quoteList(): QuoteView[] {
      const st = this.instrumentStatus
      const ids = new Set([...Object.keys(this.quotes), ...Object.keys(st)])
      return [...ids].sort().map((id) => {
        const q: QuoteView = this.quotes[id] ?? { instrument_id: id, last: Number.NaN }
        const s = st[id]
        return {
          ...q,
          quality: s?.quality ?? q.quality ?? 'OK',
          limit_lock: s?.limit_lock && s.limit_lock !== 'NONE' ? s.limit_lock : (q.limit_lock ?? 'NONE'),
          source: q.source ?? s?.live_source ?? null,
        }
      })
    },
  },
  actions: {
    upsertQuote(x: unknown): void {
      const q = toQuote(x)
      if (!q) return
      const cur = this.quotes[q.instrument_id]
      this.quotes[q.instrument_id] = { ...cur, ...q, updated_ms: Date.now() }
    },

    setStatus(s: MdStatus): void {
      const { stale: _stale, age_ms: _age, ...status } = s as MdStatus & { stale?: boolean; age_ms?: number | null }
      // a newer md.status than the stale one means md-gateway publishes again
      if (this.feedsStale && this.status && status.as_of_ns > this.status.as_of_ns) {
        this.feedsStale = false
        this.feedsAgeMs = 0
      }
      this.status = status
      this.statusAt = Date.now()
    },

    ingestEvent(ev: MdEvent): void {
      if (seenEvents.has(ev.event_id)) return
      seenEvents.add(ev.event_id)
      this.events.push(ev)
      this.events.sort((a, b) => b.ts_ns - a.ts_ns)
      if (this.events.length > MAX_EVENTS) for (const d of this.events.splice(MAX_EVENTS)) seenEvents.delete(d.event_id)
    },

    async loadQuotes(): Promise<void> {
      const data = await rt().api.quotes()
      for (const x of asItems(data)) this.upsertQuote(x)
    },

    async loadFeeds(): Promise<void> {
      try {
        const s = await rt().api.feeds()
        if (isMdStatus(s)) this.setStatus(s)
        this.feedsStale = s.stale === true
        this.feedsAgeMs = s.age_ms ?? null
        this.feedsError = null
      } catch (e) {
        this.feedsError = toLoadError(e)
        throw e
      }
    },

    async loadEvents(td?: string): Promise<void> {
      try {
        const data = await rt().api.marketEvents(td)
        for (const x of asItems(data)) if (isMdEvent(x)) this.ingestEvent(x)
        this.eventsLoaded = true
        this.eventsError = null
      } catch (e) {
        this.eventsError = toLoadError(e)
        throw e
      }
    },

    applyQuotesFrame(msg: WsServerMessage): void {
      const items = Array.isArray(msg.data) ? msg.data : [msg.data]
      for (const x of items) this.upsertQuote(x)
    },

    applyFeedsFrame(msg: WsServerMessage): void {
      const items = Array.isArray(msg.data) ? msg.data : [msg.data]
      for (const x of items) {
        if (isMdStatus(x)) this.setStatus(x)
        else if (isMdEvent(x)) this.ingestEvent(x)
      }
    },
  },
})
