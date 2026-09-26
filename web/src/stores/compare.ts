/**
 * Multi-account comparison: WS `compare` / `GET /risk/compare` (server view), rule
 * definitions (`GET /risk/rules`, used for hard limits and gauge level marks) and the
 * settlement history for the EOD tab (`GET /risk/compare/history?days=1..60` →
 * `{days, missing, errors}`; each day carries `breakers` only when every risk shard
 * answered, otherwise the breakers of that day are counted from `GET /risk/events?td=`).
 *
 * If the server compare payload is missing or has an unexpected shape, the view is
 * computed client-side from the account snapshots with the same builder the mock uses.
 */
import { defineStore } from 'pinia'
import type { CompareHistory, CompareView, RuleDef, WsServerMessage } from '@/types'
import { ApiError } from '@/api/http'
import { rt } from '@/runtime'
import { buildCompareView } from '@/utils/compare'
import { asItems, isObj, isRiskEvent, normalizeCompare, normalizeHistory, stringRecord } from '@/utils/normalize'
import { useAccounts } from './accounts'
import { toLoadError, type LoadError } from './system'

/** `GET /risk/compare/history` accepts 1..60 days (MAX_HISTORY_DAYS). */
export const HISTORY_DAY_OPTIONS = [20, 40, 60] as const

export interface HistoryError extends LoadError {
  /** per-day codes of the clearing-worker (`data.errors` of the failure) */
  errors: Record<string, string>
}

export const useCompare = defineStore('compare', {
  state: () => ({
    server: null as CompareView | null,
    serverAtMs: 0,
    serverShapeUnknown: false,
    rules: null as RuleDef[] | null,
    rulesVersion: null as { config_version: number; config_hash: string } | null,
    /** envelope message of GET /risk/rules, e.g. "risk shards unavailable: rules from the local config" */
    rulesMessage: '',
    history: null as CompareHistory | null,
    historyError: null as HistoryError | null,
    historyDays: 20 as number,
    historyLoading: false,
    /** breakers per day and account counted from `GET /risk/events?td=` for days whose history lacks them */
    dayBreakers: {} as Record<string, Record<string, number>>,
    /** days whose breaker count is unknown or incomplete (a risk shard did not answer) */
    breakerGaps: [] as string[],
    breakersLoading: false,
  }),
  getters: {
    ruleMap(state): Record<string, RuleDef> {
      const m: Record<string, RuleDef> = {}
      for (const r of state.rules ?? []) m[r.id] = r
      return m
    },
    /** Server view when fresh (< 10s), else computed from the full account snapshots. */
    view(state): CompareView | null {
      if (state.server && Date.now() - state.serverAtMs < 10_000) return state.server
      const snaps = Object.values(useAccounts().snapshots)
      if (snaps.length) return buildCompareView(snaps, state.rules)
      return state.server
    },
    usingFallback(state): boolean {
      return !(state.server && Date.now() - state.serverAtMs < 10_000)
    },
  },
  actions: {
    setServer(x: unknown): void {
      const v = normalizeCompare(x)
      if (v) {
        this.server = v
        this.serverAtMs = Date.now()
        this.serverShapeUnknown = false
      } else if (x !== undefined && x !== null) {
        this.serverShapeUnknown = true
      }
    },

    applyFrame(msg: WsServerMessage): void {
      this.setServer(msg.data)
    },

    async loadCompare(): Promise<void> {
      this.setServer(await rt().api.compare())
    },

    async loadRules(force = false): Promise<void> {
      if (this.rules && !force) return
      const res = await rt().api.rules()
      const rs: unknown = res.data
      const list = isObj(rs) && Array.isArray(rs.rules) ? rs.rules : asItems(rs)
      this.rules = list.filter((r): r is RuleDef => isObj(r) && typeof r.id === 'string' && Array.isArray(r.levels)) as RuleDef[]
      if (isObj(rs) && typeof rs.config_version === 'number') this.rulesVersion = { config_version: rs.config_version, config_hash: String(rs.config_hash ?? '') }
      this.rulesMessage = res.message
    },

    async loadHistory(days: number): Promise<void> {
      const n = Math.max(1, Math.min(60, Math.round(days)))
      this.historyLoading = true
      this.historyDays = n
      this.dayBreakers = {}
      this.breakerGaps = []
      try {
        const res = await rt().api.compareHistory(n)
        this.history = normalizeHistory(res.data) ?? { days: [] }
        this.historyError = null
      } catch (e) {
        this.history = null
        const errors = e instanceof ApiError && isObj(e.data) ? stringRecord(e.data.errors) : {}
        this.historyError = { ...toLoadError(e), errors }
        return
      } finally {
        this.historyLoading = false
      }
      await this.countBreakersFromEvents()
    },

    /**
     * Breakers of the history days that lack `breakers` (a shard did not answer
     * `risk.query.alerts` for that day): ALERT_RAISED with action CIRCUIT_BREAK plus
     * MANUAL_HALT per account, from `GET /risk/events?td=`. A day whose answer still lists
     * shard `errors` (events of that shard came from the read model only) is a gap.
     */
    async countBreakersFromEvents(): Promise<void> {
      const h = this.history
      if (!h) return
      const todo = h.days.filter((d) => !d.breakers).map((d) => d.trading_day)
      if (!todo.length) return
      this.breakersLoading = true
      const counted: Record<string, Record<string, number>> = {}
      const gaps: string[] = []
      const queue = [...todo]
      const worker = async (): Promise<void> => {
        while (queue.length) {
          const td = queue.shift() as string
          try {
            const res = await rt().api.riskEvents(td)
            const counts: Record<string, number> = {}
            for (const e of res.data.items ?? []) {
              if (!isRiskEvent(e) || !e.account_id) continue
              if ((e.kind === 'ALERT_RAISED' && e.action === 'CIRCUIT_BREAK') || e.kind === 'MANUAL_HALT') counts[e.account_id] = (counts[e.account_id] ?? 0) + 1
            }
            counted[td] = counts
            if (Object.keys(res.data.errors ?? {}).length) gaps.push(td)
          } catch {
            gaps.push(td)
          }
        }
      }
      try {
        await Promise.all([worker(), worker(), worker(), worker()])
      } finally {
        this.dayBreakers = counted
        this.breakerGaps = gaps.sort()
        this.breakersLoading = false
      }
    },
  },
})
