/**
 * Accounts: WS `accounts` summaries, full snapshots (`GET /accounts[/id]`), positions
 * (`positions:{id}`), orders (`orders:{id}`), intraday equity points (`equity:{id}`) and
 * the latest `risk.state` change per account (merged by state_version, 02 §5 rule 2).
 *
 * Trading-day boundaries: the intraday series (equity points, orders, trades) belong to one
 * trading day per account (`dayOf`). A snapshot / frame of another day, or a DayRoll, resets
 * them, so yesterday's curve never continues into today's.
 *
 * Staleness: `stale` / `age_ms` of the gateway (owner shard not UP), and at once when the
 * `system` topic reports the owning shard's process gone (EXITED / RESTARTING / …), which is
 * ~3 s before the gateway's heartbeat timeout.
 */
import { defineStore } from 'pinia'
import type {
  AccountMissingItem,
  AccountSnapshot,
  AccountStateChanged,
  AccountSummary,
  EquityPoint,
  OrderUpdate,
  PositionView,
  Trade,
  WsServerMessage,
} from '@/types'
import { rt } from '@/runtime'
import { setAccountName } from '@/utils/labels'
import {
  asItems,
  frameTsNs,
  isAccountSnapshot,
  isObj,
  mergeAccountState,
  mergeAccountSummary,
  normalizeEquityPoint,
  processStateStale,
  toAccountSummary,
  type EffectiveState,
} from '@/utils/normalize'
import { useSystem } from './system'

const GONE_STATES = ['EXITED', 'RESTARTING', 'STOPPED', 'FAILED']

const MAX_ORDERS = 600
const MAX_EQUITY_POINTS = 8000

function insertPoint(list: EquityPoint[], p: EquityPoint): EquityPoint[] {
  const last = list[list.length - 1]
  if (!last || p.ts_ns > last.ts_ns) {
    list.push(p)
  } else if (p.ts_ns === last.ts_ns) {
    list[list.length - 1] = p
  } else {
    const i = list.findIndex((x) => x.ts_ns >= p.ts_ns)
    if (i >= 0 && list[i]?.ts_ns === p.ts_ns) list[i] = p
    else list.splice(i < 0 ? list.length : i, 0, p)
  }
  if (list.length > MAX_EQUITY_POINTS) list.splice(0, list.length - MAX_EQUITY_POINTS)
  return list
}

export const useAccounts = defineStore('accounts', {
  state: () => ({
    summaries: {} as Record<string, AccountSummary>,
    snapshots: {} as Record<string, AccountSnapshot>,
    changes: {} as Record<string, AccountStateChanged>,
    positions: {} as Record<string, PositionView[]>,
    positionsWsAt: {} as Record<string, number>,
    orders: {} as Record<string, Record<string, OrderUpdate>>,
    equity: {} as Record<string, EquityPoint[]>,
    trades: {} as Record<string, Trade[]>,
    loadedAll: false,
    /** every configured account (GET /accounts lists them all) */
    configured: [] as string[],
    /** configured accounts without a snapshot: `{account_id, name, shard, missing, stale, age_ms}` */
    missing: {} as Record<string, AccountMissingItem>,
    /** trading day of each account's intraday series (equity, orders, trades) */
    dayOf: {} as Record<string, string>,
  }),
  getters: {
    ids(state): string[] {
      const s = new Set([...Object.keys(state.summaries), ...Object.keys(state.snapshots), ...state.configured])
      return [...s].sort()
    },
    /**
     * Owner liveness of an account with data: its shard is down and the figures are the last
     * ones received (`stale`). An account without a snapshot is not "stale data" (see `withoutData`).
     */
    staleInfo(state) {
      return (id: string): { stale: boolean; age_ms: number | null } => {
        const s = state.summaries[id]
        const v = state.snapshots[id] as (AccountSnapshot & { stale?: boolean; age_ms?: number | null }) | undefined
        if (!s && !v) return { stale: false, age_ms: null }
        const flagged = s?.stale ?? v?.stale ?? false
        const age = s?.age_ms ?? v?.age_ms ?? null
        if (flagged) return { stale: true, age_ms: age }
        // the owning shard's process is gone (system topic): stale now, before the heartbeat timeout
        const shard = s?.shard ?? v?.shard
        const proc = shard !== null && shard !== undefined ? useSystem().services[`risk-engine-${shard}`] : undefined
        const gone = proc ? proc.process_state !== null && GONE_STATES.includes(proc.process_state) && !processStateStale(proc) : false
        return { stale: gone, age_ms: age }
      }
    },
    staleIds(): string[] {
      return this.ids.filter((id) => this.staleInfo(id).stale)
    },
    /** Configured accounts without a snapshot or a known state (GET /accounts `missing: true`). */
    withoutData(state): string[] {
      return state.configured.filter((id) => !state.summaries[id] && !state.snapshots[id])
    },
    effective(state) {
      return (id: string): EffectiveState | null => mergeAccountState(state.snapshots[id], state.summaries[id], state.changes[id])
    },
    /** Summaries with the effective (state_version-merged) state applied. */
    list(): AccountSummary[] {
      return this.ids
        .map((id) => {
          const base = this.summaries[id] ?? (this.snapshots[id] ? toAccountSummary(this.snapshots[id]) : null)
          if (!base) return null
          const eff = this.effective(id)
          return eff ? { ...base, state: eff.state, halted_latched: eff.halted_latched } : base
        })
        .filter((x): x is AccountSummary => x !== null)
    },
    tradingDay(state): string | null {
      for (const s of Object.values(state.snapshots)) if (s.trading_day) return s.trading_day
      return null
    },
    /** Orders of the account's current trading day (orders of another day are dropped on a roll). */
    orderList(state) {
      return (id: string): OrderUpdate[] => {
        const td = state.dayOf[id]
        return Object.values(state.orders[id] ?? {})
          .filter((o) => !td || !o.trading_day || o.trading_day === td)
          .sort((a, b) => b.updated_ns - a.updated_ns || b.created_ns - a.created_ns)
      }
    },
  },
  actions: {
    /**
     * The account's trading day is `td`: a change drops the intraday series of the previous
     * day (equity points, orders, trades) so they never mix with the new day.
     */
    noteDay(id: string, td: string | null | undefined): void {
      if (!td || !/^\d{8}$/.test(td)) return
      const cur = this.dayOf[id]
      if (cur === td) return
      if (cur && td < cur) return // a late frame of the previous day: ignore
      if (cur) {
        delete this.equity[id]
        delete this.orders[id]
        delete this.trades[id]
      }
      this.dayOf[id] = td
    },

    /** DayRoll (`eod` topic): the listed accounts (all when none) move to `toTd`. */
    rollDay(ids: string[] | null | undefined, toTd: string): void {
      const list = ids && ids.length ? ids : this.ids
      for (const id of list) this.noteDay(id, toTd)
    },

    upsertSnapshot(s: AccountSnapshot): void {
      const prev = this.snapshots[s.account_id]
      if (prev && prev.as_of_ns > s.as_of_ns && prev.version >= s.version) return
      this.noteDay(s.account_id, s.trading_day)
      this.snapshots[s.account_id] = s
      const sum = toAccountSummary(s)
      if (sum) {
        setAccountName(sum.account_id, sum.name)
        const cur = this.summaries[s.account_id]
        if (!cur || !cur.as_of_ns || (sum.as_of_ns ?? 0) >= cur.as_of_ns) this.summaries[s.account_id] = mergeAccountSummary(cur, sum)
      }
      // positions from the snapshot unless the WS positions topic is fresher
      const wsAt = this.positionsWsAt[s.account_id] ?? 0
      if (s.positions && Date.now() - wsAt > 3000) this.positions[s.account_id] = s.positions
    },

    /** `frameMts`: market time (epoch ms) of the WS frame, the item's time when it has none. */
    upsertSummary(x: unknown, frameMts?: number): void {
      const sum = toAccountSummary(x)
      if (!sum) return
      if (sum.as_of_ns === undefined) {
        const ts = frameTsNs(frameMts)
        if (ts !== null) sum.as_of_ns = ts
      }
      setAccountName(sum.account_id, sum.name)
      this.noteDay(sum.account_id, sum.trading_day)
      this.summaries[sum.account_id] = mergeAccountSummary(this.summaries[sum.account_id], sum)
    },

    applyStateChange(chg: AccountStateChanged): void {
      const cur = this.changes[chg.account_id]
      if (!cur || chg.state_version > cur.state_version) this.changes[chg.account_id] = chg
    },

    /** `GET /accounts`: every configured account, its snapshot or a `missing: true` item. */
    async loadAll(): Promise<void> {
      const data = await rt().api.accounts()
      const configured: string[] = []
      const missing: Record<string, AccountMissingItem> = {}
      for (const x of data) {
        configured.push(x.account_id)
        if (x.missing === true) {
          missing[x.account_id] = x
          setAccountName(x.account_id, x.name)
          if (x.state) this.upsertSummary(x) // a risk.state is known without a snapshot
        } else if (isAccountSnapshot(x)) this.upsertSnapshot(x)
      }
      this.configured = configured.sort()
      this.missing = missing
      this.loadedAll = true
    },

    async loadAccount(id: string): Promise<AccountSnapshot | null> {
      const s = await rt().api.account(id)
      if (isAccountSnapshot(s)) {
        this.upsertSnapshot(s)
        return s
      }
      this.upsertSummary(s)
      return null
    },

    async loadPositions(id: string): Promise<void> {
      const data = await rt().api.positions(id)
      this.positions[id] = asItems<PositionView>(data)
    },

    async loadOrders(id: string, td?: string): Promise<void> {
      const data = await rt().api.orders(id, td)
      if (td) this.noteDay(id, td)
      const map: Record<string, OrderUpdate> = {}
      for (const o of asItems<OrderUpdate>(data)) if (o && o.client_order_id) map[o.client_order_id] = o
      // keep WS updates newer than the REST page
      const cur = this.orders[id] ?? {}
      for (const [k, v] of Object.entries(cur)) {
        const r = map[k]
        if (!r || v.updated_ns > r.updated_ns) map[k] = v
      }
      this.orders[id] = map
    },

    async loadTrades(id: string, td?: string): Promise<void> {
      const data = await rt().api.trades(id, td)
      this.trades[id] = asItems<Trade>(data).sort((a, b) => b.trade_ts_ns - a.trade_ts_ns)
    },

    async loadEquity(id: string, td?: string): Promise<void> {
      const series = await rt().api.equityCurve(id, td, 5)
      if (isObj(series) && typeof series.trading_day === 'string') this.noteDay(id, series.trading_day)
      const pts = (isObj(series) ? asItems(series.points) : asItems(series))
        .map(normalizeEquityPoint)
        .filter((p): p is EquityPoint => p !== null)
        .sort((a, b) => a.ts_ns - b.ts_ns)
      // merge live points received meanwhile
      let merged = pts
      for (const p of this.equity[id] ?? []) merged = insertPoint(merged, p)
      this.equity[id] = merged
    },

    // ---------------------------------------------------------------- WS frames

    applyAccountsFrame(msg: WsServerMessage): void {
      if (Array.isArray(msg.data)) {
        for (const x of msg.data) (isAccountSnapshot(x) ? this.upsertSnapshot(x) : this.upsertSummary(x, msg.mts))
      } else if (isAccountSnapshot(msg.data)) this.upsertSnapshot(msg.data)
      else this.upsertSummary(msg.data, msg.mts)
    },

    applyPositionsFrame(id: string, msg: WsServerMessage): void {
      const list = Array.isArray(msg.data) ? msg.data : isObj(msg.data) && Array.isArray(msg.data.positions) ? msg.data.positions : null
      if (!list) return
      this.positions[id] = list as PositionView[]
      this.positionsWsAt[id] = Date.now()
    },

    applyOrdersFrame(id: string, msg: WsServerMessage): void {
      if (msg.type === 'snapshot') this.noteDay(id, msg.td)
      const map = this.orders[id] ?? (this.orders[id] = {})
      const items = Array.isArray(msg.data) ? msg.data : [msg.data]
      for (const o of items) {
        if (!isObj(o) || typeof o.client_order_id !== 'string') continue
        const ou = o as unknown as OrderUpdate
        if (msg.type === 'delete') {
          delete map[ou.client_order_id]
          continue
        }
        const cur = map[ou.client_order_id]
        if (!cur || ou.updated_ns >= cur.updated_ns) map[ou.client_order_id] = ou
      }
      const keys = Object.keys(map)
      if (keys.length > MAX_ORDERS) {
        const drop = Object.values(map)
          .sort((a, b) => a.updated_ns - b.updated_ns)
          .slice(0, keys.length - MAX_ORDERS)
        for (const o of drop) delete map[o.client_order_id]
      }
    },

    applyEquityFrame(id: string, msg: WsServerMessage): void {
      this.noteDay(id, msg.td)
      const raw = Array.isArray(msg.data) ? msg.data : isObj(msg.data) && Array.isArray(msg.data.points) ? msg.data.points : [msg.data]
      const pts = raw.map(normalizeEquityPoint).filter((p): p is EquityPoint => p !== null)
      if (msg.type === 'snapshot' && pts.length > 1) {
        this.equity[id] = pts.sort((a, b) => a.ts_ns - b.ts_ns)
        return
      }
      let list = this.equity[id] ?? []
      for (const p of pts) list = insertPoint(list, p)
      this.equity[id] = list
    },
  },
})
