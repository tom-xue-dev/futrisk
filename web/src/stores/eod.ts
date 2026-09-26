/**
 * EOD / clearing / reports / backtests: WS `eod` topic (EodPhaseEvent from `sys.eod` —
 * phase changes and WARN events —, SettlementCompleted, ReportReady, DayRoll), WS
 * `backtests` (every run, upsert per run), `GET /settlement/days` (recent trading days with
 * their latest phase and `warning_count`; `errors` lists days the clearing-worker could not
 * answer), `GET /settlement/{td}/status` (`detail.warning_count` / `detail.warnings`),
 * `GET /reports[?td=]`, `GET /backtests`.
 *
 * EOD warnings (LATE_FACT, OFFICIAL_PRICE_DIFF, PUSH_INCOMPLETE, PRE_SETTLE_MISMATCH) never
 * change a run's phase: a WARN event repeats the current phase with `detail.warning`.
 *
 * The day list is the union of `/settlement/days`, live EOD events, report trading days
 * and the current trading day.
 */
import { defineStore } from 'pinia'
import type {
  BacktestStatus,
  DayRoll,
  EodPhase,
  EodPhaseEvent,
  EodWarning,
  ReportReady,
  SettlementCompleted,
  SettlementDayItem,
  SettlementResult,
  WsServerMessage,
} from '@/types'
import { EOD_PHASE_VALUES } from '@/types'
import { ApiError } from '@/api/http'
import { rt } from '@/runtime'
import { asItems, eodWarningOf, eodWarningsIn, isDayRoll, isEodPhaseEvent, isObj, isReportReady, isSettlementCompleted, normalizeStatement } from '@/utils/normalize'
import { useAccounts } from './accounts'
import { toLoadError, type LoadError } from './system'

export type EodWarningNotifier = (w: EodWarning) => void
let warningNotifier: EodWarningNotifier | null = null
let failureNotifier: ((ev: EodPhaseEvent) => void) | null = null

/** Installed by the app shell: a sticky notification for a live FAILED EOD run. */
export function setEodFailureNotifier(fn: ((ev: EodPhaseEvent) => void) | null): void {
  failureNotifier = fn
}

/** Installed by the app shell: toast for live EOD warnings. */
export function setEodWarningNotifier(fn: EodWarningNotifier | null): void {
  warningNotifier = fn
}

/**
 * Content key: live WARN events carry a `warn_id`, the `{kind, message}` items of
 * `settle.status` do not — the same warning must not be listed twice.
 */
function warnKey(w: EodWarning): string {
  return `${w.trading_day}|${w.version}|${w.kind}|${w.message}`
}

/** `GET /settlement/{td}/statements/{account}`: the text file, or the settlement data when the file is missing. */
export interface StatementAnswer {
  text: string | null
  result: SettlementResult | null
  /** envelope message, e.g. "statement file not available: settlement data instead" */
  message: string
}

const PHASE_RANK: Record<string, number> = Object.fromEntries(EOD_PHASE_VALUES.map((p, i) => [p, i]))

function newer(a: EodPhaseEvent, b: EodPhaseEvent | undefined): boolean {
  if (!b) return true
  if (a.version !== b.version) return a.version > b.version
  if (a.ts_ns !== b.ts_ns) return a.ts_ns > b.ts_ns
  return (PHASE_RANK[a.phase] ?? 0) >= (PHASE_RANK[b.phase] ?? 0)
}

export const useEod = defineStore('eod', {
  state: () => ({
    phases: {} as Record<string, EodPhaseEvent>,
    log: [] as EodPhaseEvent[],
    settled: {} as Record<string, SettlementCompleted>,
    results: {} as Record<string, SettlementResult>,
    reports: {} as Record<string, ReportReady>,
    backtests: {} as Record<string, BacktestStatus>,
    rolls: [] as DayRoll[],
    extraDays: [] as string[],
    statusMissing: {} as Record<string, boolean>,
    reportsLoaded: false,
    /** `GET /settlement/days` items by day */
    dayItems: {} as Record<string, SettlementDayItem>,
    /** days whose status the clearing-worker could not give (`{td: code}`) */
    daysErrors: {} as Record<string, string>,
    daysMessage: '',
    daysError: null as LoadError | null,
    reportsError: null as LoadError | null,
    backtestsError: null as LoadError | null,
    /** EOD warnings per trading day (live WARN events + `detail.warnings` of status answers) */
    warnings: {} as Record<string, EodWarning[]>,
  }),
  getters: {
    dayList(state): string[] {
      // days whose status could not be read are only in the notice (listing a whole window of
      // "unknown" rows while the clearing-worker is down would bury the known ones)
      const s = new Set<string>([...Object.keys(state.phases), ...Object.keys(state.settled), ...Object.keys(state.dayItems), ...state.extraDays])
      for (const r of Object.values(state.reports)) if (r.trading_day) s.add(r.trading_day)
      return [...s].filter((d) => /^\d{8}$/.test(d)).sort().reverse()
    },
    /** Latest phase of a day: live / status EodPhaseEvent, else the `/settlement/days` item. */
    phaseOf(state) {
      return (td: string): { phase: EodPhase; version: number; event: EodPhaseEvent | null } | null => {
        const ev = state.phases[td]
        const item = state.dayItems[td]
        if (ev && (!item || ev.version >= item.version)) return { phase: ev.phase, version: ev.version, event: ev }
        if (item) return { phase: item.phase, version: item.version, event: null }
        return null
      }
    },
    reportsFor(state) {
      return (td: string): ReportReady[] =>
        Object.values(state.reports)
          .filter((r) => r.trading_day === td)
          .sort((a, b) => a.type.localeCompare(b.type) || (a.account_id ?? '').localeCompare(b.account_id ?? ''))
    },
    reportList(state): ReportReady[] {
      return Object.values(state.reports).sort((a, b) => b.created_ns - a.created_ns)
    },
    backtestList(state): BacktestStatus[] {
      return Object.values(state.backtests).sort((a, b) => b.submitted_ns - a.submitted_ns)
    },
    /** Phase log of the most recent EOD run (for the live progress stepper). */
    currentRun(state): { td: string; version: number; events: EodPhaseEvent[] } | null {
      const last = state.log[state.log.length - 1]
      if (!last) return null
      const events = state.log.filter((e) => e.trading_day === last.trading_day && e.version === last.version)
      return { td: last.trading_day, version: last.version, events }
    },
  },
  actions: {
    /**
     * `live` = arrived on the WS `eod` topic (feeds the progress stepper); REST loads only
     * update per-day state. `notify` = a new event frame (not a snapshot replay): toast warnings.
     */
    ingest(x: unknown, live = false, notify = false): void {
      if (isEodPhaseEvent(x)) {
        const warn = eodWarningOf(x)
        if (warn) {
          // a WARN repeats the run's current phase: no phase change, no progress step
          if (this.addWarnings(x.trading_day, [warn]).length && notify) warningNotifier?.(warn)
          return
        }
        // e.g. PRE_SETTLE_MISMATCH items of PRICES_FIXED (notified by the gateway too)
        const fresh = this.addWarnings(x.trading_day, eodWarningsIn(x))
        if (notify) for (const w of fresh) warningNotifier?.(w)
      }
      if (isReportReady(x)) this.reports[x.report_id] = x
      else if (isSettlementCompleted(x)) this.settled[x.trading_day] = x
      else if (isDayRoll(x)) {
        this.rolls.unshift(x)
        if (this.rolls.length > 50) this.rolls.pop()
        // the shard's accounts start a new trading day: their intraday series restart
        if (live && x.mode !== 'REBASE') useAccounts().rollDay(x.accounts, x.to_trading_day)
      } else if (isEodPhaseEvent(x)) {
        const known = this.phases[x.trading_day]
        const fresh = !known || known.version !== x.version || known.phase !== x.phase
        if (newer(x, this.phases[x.trading_day])) this.phases[x.trading_day] = x
        if (x.phase === 'FAILED' && notify && fresh) failureNotifier?.(x)
        const dup = this.log.some((e) => e.trading_day === x.trading_day && e.version === x.version && e.phase === x.phase)
        if (live && !dup) {
          this.log.push(x)
          if (this.log.length > 200) this.log.shift()
        }
      }
    },

    /** Add warnings of a day (deduplicated); returns the ones that were new. */
    addWarnings(td: string, list: EodWarning[]): EodWarning[] {
      if (!list.length) return []
      const cur = this.warnings[td] ?? []
      const seenKeys = new Set(cur.map(warnKey))
      const fresh = list.filter((w) => !seenKeys.has(warnKey(w)))
      if (!fresh.length) return []
      this.warnings[td] = [...cur, ...fresh].sort((a, b) => (a.ts_ns ?? 0) - (b.ts_ns ?? 0))
      return fresh
    },

    /** Warning count of a day: the most we know from `/settlement/days`, status answers and events. */
    warningCount(td: string): number {
      const d = this.phases[td]?.detail
      const fromStatus = isObj(d) && typeof d.warning_count === 'number' ? d.warning_count : 0
      return Math.max(this.dayItems[td]?.warning_count ?? 0, fromStatus, this.warnings[td]?.length ?? 0)
    },

    addDays(days: string[]): void {
      const s = new Set(this.extraDays)
      for (const d of days) if (d && !s.has(d)) s.add(d)
      this.extraDays = [...s]
    },

    applyFrame(msg: WsServerMessage): void {
      const items = Array.isArray(msg.data) ? msg.data : [msg.data]
      for (const x of items) this.ingest(x, true, msg.type === 'event')
    },

    async loadDays(limit = 30): Promise<void> {
      try {
        const res = await rt().api.settlementDays(limit)
        for (const it of res.data.items ?? []) if (it && typeof it.trading_day === 'string') this.dayItems[it.trading_day] = it
        this.daysErrors = { ...(res.data.errors ?? {}) }
        this.daysMessage = res.message
        this.daysError = null
      } catch (e) {
        this.daysError = toLoadError(e)
        throw e
      }
    },

    async loadReports(td?: string): Promise<void> {
      try {
        const data = await rt().api.reports(td)
        for (const x of asItems(data)) if (isReportReady(x)) this.reports[x.report_id] = x
        this.reportsLoaded = true
        this.reportsError = null
      } catch (e) {
        this.reportsError = toLoadError(e)
        throw e
      }
    },

    /** `{format, text?, settlement?}`: the statement file's text, or the settlement data (with a message). */
    async loadStatement(td: string, account: string): Promise<StatementAnswer> {
      const res = await rt().api.statement(td, account)
      const v = normalizeStatement(res.data)
      return { text: v.text, result: v.result, message: res.message }
    },

    async loadStatus(td: string): Promise<EodPhaseEvent | null> {
      try {
        const ev = await rt().api.settlementStatus(td)
        if (isEodPhaseEvent(ev)) {
          this.ingest(ev)
          return ev
        }
      } catch (e) {
        if (e instanceof ApiError && e.code === 'NOT_FOUND') this.statusMissing[td] = true
        else throw e
      }
      return null
    },

    async loadResult(td: string): Promise<SettlementResult | null> {
      const r = await rt().api.settlement(td)
      if (isObj(r) && Array.isArray(r.accounts)) {
        this.results[td] = r
        return r
      }
      return null
    },

    /** WS `backtests`: snapshot = every run (newest first), upsert = one run. */
    applyBacktestsFrame(msg: WsServerMessage): void {
      const items = Array.isArray(msg.data) ? msg.data : [msg.data]
      if (msg.type === 'snapshot') this.backtests = {}
      for (const x of items) if (isObj(x) && typeof x.run_id === 'string') this.backtests[x.run_id] = x as unknown as BacktestStatus
      this.backtestsError = null
    },

    async loadBacktests(): Promise<void> {
      try {
        const data = await rt().api.backtests()
        for (const x of asItems<BacktestStatus>(data)) if (isObj(x) && typeof x.run_id === 'string') this.backtests[x.run_id] = x
        this.backtestsError = null
      } catch (e) {
        this.backtestsError = toLoadError(e)
        throw e
      }
    },

    upsertBacktest(b: BacktestStatus): void {
      this.backtests[b.run_id] = b
    },
  },
})
