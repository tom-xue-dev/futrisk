/**
 * Live risk-event stream: `risk.event` (RiskEvent) and `risk.state` (AccountStateChanged)
 * frames of the WS `events` topic, plus history from `GET /risk/events`
 * (`{trading_day, items, errors}`: shards that did not answer are listed in `errors`, their
 * events then come from the gateway's read model only). Liquidation plans: `GET
 * /risk/liquidation-plans` (a plan of a shard that did not answer comes from the read
 * model, marked `stale`), LIQUIDATION_PLAN events (creation: detail `{plan_id, plan}`;
 * decision / expiry: detail `{plan_id, status, actor}`) and confirm replies; a confirm
 * of a plan that is no longer pending answers CONFLICT with the current plan as data.
 *
 * Every item keeps its `trading_day`: the stream shows the current trading day by default
 * (other days on request, with their date), so a day roll starts a clean list.
 */
import { defineStore } from 'pinia'
import type { AccountStateChanged, LiquidationPlan, LiquidationPlanView, LiquidationStatus, RiskEvent, WsServerMessage } from '@/types'
import { SEVERITY_ORDER } from '@/types'
import { ApiError } from '@/api/http'
import { rt } from '@/runtime'
import { extractPlan, isAccountStateChanged, isLiquidationPlan, isObj, isRiskEvent } from '@/utils/normalize'
import { useAccounts } from './accounts'
import { toLoadError, type LoadError } from './system'

export interface StreamItem {
  key: string
  ts_ns: number
  /** trading day of the event / state change */
  trading_day: string
  account_id: string | null
  risk?: RiskEvent
  state?: AccountStateChanged
}

const MAX_ITEMS = 1500
const seen = new Set<string>()

export type LiveEventNotifier = (ev: RiskEvent) => void
let notifier: LiveEventNotifier | null = null

/** Installed by the app shell: toast for live HIGH / CRITICAL events. */
export function setLiveEventNotifier(fn: LiveEventNotifier | null): void {
  notifier = fn
}

export const useEvents = defineStore('events', {
  state: () => ({
    items: [] as StreamItem[],
    plans: {} as Record<string, LiquidationPlanView>,
    /** shards that did not answer the last GET /risk/liquidation-plans (their plans are `stale`) */
    planErrors: {} as Record<string, string>,
    acked: {} as Record<string, boolean>,
    loaded: false,
    /** trading day of the last `GET /risk/events` */
    loadedDay: null as string | null,
    /** shards that did not answer the last firm-wide `GET /risk/events` (`{instance: code}`) */
    loadErrors: {} as Record<string, string>,
    /** the whole firm-wide request failed */
    loadError: null as LoadError | null,
    /** the same for `GET /risk/events?account=` loads, per account */
    accountLoad: {} as Record<string, { errors: Record<string, string>; error: LoadError | null }>,
  }),
  getters: {
    forAccount(state) {
      return (id: string | null | undefined): StreamItem[] => (id ? state.items.filter((i) => i.account_id === id) : state.items)
    },
    riskEvents(state): RiskEvent[] {
      return state.items.filter((i) => i.risk).map((i) => i.risk as RiskEvent)
    },
    /**
     * Why an account is in its state: the alert (ALERT_RAISED / MANUAL_HALT) that drives it —
     * the latest state change's `cause_alert_key` when known, else the newest not-cleared alert
     * of the day with the action of that state (CIRCUIT_BREAK for HALTED, RESTRICT for
     * RESTRICTED, ALERT for WARNING).
     */
    causeOf(state) {
      return (accountId: string, st: string | null | undefined, causeKey?: string | null, td?: string | null): RiskEvent | null => {
        const risks = state.items.filter((i) => i.risk && i.account_id === accountId).map((i) => i.risk as RiskEvent)
        if (causeKey) {
          const hit = risks.find((e) => e.alert_key === causeKey && (e.kind === 'ALERT_RAISED' || e.kind === 'MANUAL_HALT'))
          if (hit) return hit
        }
        const action = st === 'HALTED' ? 'CIRCUIT_BREAK' : st === 'RESTRICTED' ? 'RESTRICT' : st === 'WARNING' ? 'ALERT' : null
        if (!action) return null
        const cleared = new Set(risks.filter((e) => e.kind === 'ALERT_CLEARED' || e.kind === 'RESET').map((e) => e.alert_key))
        return (
          risks.find(
            (e) =>
              (!td || e.trading_day === td) &&
              ((e.kind === 'ALERT_RAISED' && e.action === action) || (action === 'CIRCUIT_BREAK' && e.kind === 'MANUAL_HALT')) &&
              !cleared.has(e.alert_key),
          ) ?? null
        )
      }
    },
    plansFor(state) {
      return (id: string): LiquidationPlanView[] =>
        Object.values(state.plans)
          .filter((p) => p.account_id === id)
          .sort((a, b) => b.created_ns - a.created_ns)
    },
  },
  actions: {
    ingest(x: unknown, live: boolean): void {
      if (isRiskEvent(x)) {
        this.addItem({ key: `e:${x.event_id}`, ts_ns: x.ts_ns, trading_day: x.trading_day, account_id: x.account_id ?? null, risk: x })
        if (x.kind === 'LIQUIDATION_PLAN') {
          const plan = extractPlan(x)
          if (plan) this.upsertPlan(plan)
          else this.applyPlanDecision(x)
        }
        if (x.kind === 'ALERT_ACKED' && x.alert_key) this.acked[x.alert_key] = true
        if (live && notifier && SEVERITY_ORDER[x.severity ?? 'INFO'] >= SEVERITY_ORDER.HIGH) notifier(x)
      } else if (isAccountStateChanged(x)) {
        useAccounts().applyStateChange(x)
        this.addItem({ key: `s:${x.account_id}:${x.state_version}`, ts_ns: x.ts_ns, trading_day: x.trading_day, account_id: x.account_id, state: x })
      }
    },

    /** Insert keeping newest-first order; duplicates (same event / state_version) are ignored. */
    addItem(item: StreamItem): void {
      if (seen.has(item.key)) return
      seen.add(item.key)
      const first = this.items[0]
      if (!first || item.ts_ns >= first.ts_ns) this.items.unshift(item)
      else {
        const i = this.items.findIndex((x) => x.ts_ns < item.ts_ns)
        this.items.splice(i < 0 ? this.items.length : i, 0, item)
      }
      if (this.items.length > MAX_ITEMS) {
        for (const d of this.items.splice(MAX_ITEMS)) seen.delete(d.key)
      }
    },

    upsertPlan(plan: LiquidationPlanView): void {
      const cur = this.plans[plan.plan_id]
      // a decided plan never goes back to pending (an older creation event replayed after the decision)
      if (cur && cur.status !== 'PENDING_CONFIRM' && plan.status === 'PENDING_CONFIRM') return
      const next: LiquidationPlanView = { ...cur, ...plan }
      if (plan.stale === undefined) delete next.stale
      this.plans[plan.plan_id] = next
    },

    /** LIQUIDATION_PLAN event of a decision / expiry: detail `{plan_id, status, actor}` (no plan body). */
    applyPlanDecision(ev: RiskEvent): void {
      const d = ev.detail
      if (!isObj(d) || typeof d.plan_id !== 'string' || typeof d.status !== 'string') return
      const cur = this.plans[d.plan_id]
      if (!cur) return // not known here: the next GET /risk/liquidation-plans brings it
      this.plans[d.plan_id] = {
        ...cur,
        status: d.status as LiquidationStatus,
        confirmed_by: typeof d.actor === 'string' ? d.actor : (cur.confirmed_by ?? null),
        confirmed_ns: d.status === 'EXPIRED' ? (cur.confirmed_ns ?? null) : ev.ts_ns,
      }
    },

    /** `GET /risk/liquidation-plans[?account=]` (today's plans of every shard). */
    async loadPlans(account?: string): Promise<void> {
      const res = await rt().api.liquidationPlans(account ? { account } : {})
      for (const p of res.data.items ?? []) this.upsertPlan({ ...p, stale: p.stale === true ? true : undefined })
      this.planErrors = { ...(res.data.errors ?? {}) }
    },

    applyFrame(msg: WsServerMessage): void {
      const live = msg.type === 'event' || msg.type === 'upsert'
      const items = Array.isArray(msg.data) ? msg.data : [msg.data]
      for (const x of items) this.ingest(x, live)
    },

    async load(td?: string, account?: string): Promise<void> {
      try {
        const res = await rt().api.riskEvents(td, account)
        for (const x of res.data.items ?? []) this.ingest(x, false)
        const errors = { ...(res.data.errors ?? {}) }
        if (account) {
          this.accountLoad[account] = { errors, error: null }
          return
        }
        this.loaded = true
        this.loadedDay = res.data.trading_day ?? null
        this.loadErrors = errors
        this.loadError = null
      } catch (e) {
        // an account's load (e.g. an unknown account: 404) must not mark the firm-wide stream failed
        if (account) this.accountLoad[account] = { errors: {}, error: toLoadError(e) }
        else this.loadError = toLoadError(e)
        throw e
      }
    },

    async ack(alertKey: string, note: string | undefined, key?: string): Promise<void> {
      await rt().api.ackAlert(alertKey, note ? { note } : {}, key)
      this.acked[alertKey] = true
    },

    /**
     * Confirm / cancel a plan. Returns the plan as the risk shard sees it and whether it
     * was already decided (CONFLICT: someone else confirmed / canceled it, or it expired).
     */
    async confirmPlan(planId: string, decision: 'CONFIRM' | 'CANCEL', key?: string): Promise<{ plan: LiquidationPlan | null; conflict: boolean }> {
      try {
        const res = await rt().api.confirmLiquidation(planId, { decision }, key)
        if (isLiquidationPlan(res)) {
          this.upsertPlan(res)
          return { plan: res, conflict: false }
        }
        const cur = this.plans[planId]
        if (cur) this.plans[planId] = { ...cur, status: decision === 'CONFIRM' ? 'CONFIRMED' : 'CANCELED' }
        return { plan: this.plans[planId] ?? null, conflict: false }
      } catch (e) {
        if (e instanceof ApiError && e.code === 'CONFLICT' && isLiquidationPlan(e.data)) {
          this.upsertPlan(e.data)
          return { plan: e.data, conflict: true }
        }
        throw e
      }
    },
  },
})
