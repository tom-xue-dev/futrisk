/** WebSocket connection status and seq/resync statistics for the header indicator. */
import { defineStore } from 'pinia'
import type { WsStats, WsStatus } from '@/api/ws'

export const useConnection = defineStore('connection', {
  state: () => ({
    status: 'idle' as WsStatus,
    stats: {
      received: 0,
      gaps: 0,
      duplicates: 0,
      resyncRequests: 0,
      serverResyncs: 0,
      reconnects: 0,
      lastMessageMs: null,
      nextRetryMs: null,
      lastCloseCode: null,
    } as WsStats,
    lastError: '' as string,
    restFailures: 0,
    lastRestError: '' as string,
  }),
  actions: {
    update(status: WsStatus, stats: Readonly<WsStats>): void {
      this.status = status
      this.stats = { ...stats }
    },
    serverError(message: string): void {
      this.lastError = message
    },
    restError(message: string): void {
      this.restFailures += 1
      this.lastRestError = message
    },
  },
})
