/**
 * Routes coalesced WS frames to the Pinia stores, and reloads topics over REST when the
 * WS client reports a server resync, an event-topic gap or a reconnect (02 §9.7:
 * "超过上限时，服务端对该主题发送 resync，客户端改用 REST 拉取历史").
 */
import type { WsServerMessage } from '@/types'
import type { ResyncReason } from '@/api/ws'
import { useAccounts } from '@/stores/accounts'
import { useCompare } from '@/stores/compare'
import { useConnection } from '@/stores/connection'
import { useEod } from '@/stores/eod'
import { useEvents } from '@/stores/events'
import { useFirm } from '@/stores/firm'
import { useMarket } from '@/stores/market'
import { useSystem } from '@/stores/system'
import { isObj } from '@/utils/normalize'

export function routeFrames(msgs: WsServerMessage[]): void {
  const accounts = useAccounts()
  for (const m of msgs) {
    const t = m.topic ?? ''
    try {
      if (m.type === 'error') {
        // one error frame per op; unknown topics of a `sub` are listed in data.topics
        const d = m.data
        const topics = isObj(d) && Array.isArray(d.topics) ? `：${d.topics.join('，')}` : ''
        useConnection().serverError(isObj(d) ? `${String(d.code ?? '')} ${String(d.message ?? '')}${topics}`.trim() : String(d ?? 'error'))
      } else if (t === 'quotes') useMarket().applyQuotesFrame(m)
      else if (t === 'feeds') useMarket().applyFeedsFrame(m)
      else if (t === 'accounts') accounts.applyAccountsFrame(m)
      else if (t.startsWith('positions:')) accounts.applyPositionsFrame(t.slice('positions:'.length), m)
      else if (t.startsWith('orders:')) accounts.applyOrdersFrame(t.slice('orders:'.length), m)
      else if (t.startsWith('equity:')) accounts.applyEquityFrame(t.slice('equity:'.length), m)
      else if (t === 'events') useEvents().applyFrame(m)
      else if (t === 'compare') useCompare().applyFrame(m)
      else if (t === 'firm') useFirm().applyFrame(m)
      else if (t === 'system') useSystem().applyFrame(m)
      else if (t === 'eod') useEod().applyFrame(m)
      else if (t === 'backtests') useEod().applyBacktestsFrame(m)
    } catch (e) {
      // a malformed frame must never break the stream
      console.warn('[ws] frame dropped', t, e)
    }
  }
}

/** REST reload of a topic after a server resync / event gap / reconnect. */
export function reloadTopic(topic: string, reason: ResyncReason): void {
  const swallow = (p: Promise<unknown>) => p.catch((e: unknown) => useConnection().restError(`${topic}: ${e instanceof Error ? e.message : String(e)}`))
  if (topic === 'events') swallow(useEvents().load())
  else if (topic === 'eod') {
    swallow(useEod().loadReports())
    swallow(useEod().loadDays(60))
  } else if (topic.startsWith('orders:')) swallow(useAccounts().loadOrders(topic.slice('orders:'.length)))
  else if (topic.startsWith('equity:')) swallow(useAccounts().loadEquity(topic.slice('equity:'.length)))
  else if (topic === 'feeds' && reason !== 'gap') swallow(useMarket().loadEvents())
}
