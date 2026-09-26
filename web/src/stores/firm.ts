/**
 * Firm-level view: WS `firm` topic (snapshots = FirmSnapshot + `control`, the kill-switch
 * layers; events = FirmControl from `risk.control.firm`). When no FirmSnapshot has arrived
 * (coordinator down, or before the first 1 Hz frame) the totals are aggregated from the
 * account summaries and the kill switch comes from `/system/status`.
 *
 * Staleness: the coordinator publishes a FirmSnapshot every second. Without a frame for
 * more than 3 s, or while the coordinator shard is down, the last snapshot is kept but the
 * view is `stale` (the kill switch shows "unknown (stale)"); a shard whose own `as_of_ns`
 * lags the snapshot is listed in `stale_shards` (the totals include its last figures).
 *
 * Kill switch = MANUAL OR RULE: the manual layer is set by `POST /risk/firm/kill-switch`,
 * the rule layer by the coordinator's firm rules (one RULE_STATE fact). The command fans
 * out to every risk shard and the trade-gateway and always answers HTTP 200: `ok=false`
 * with the worst target code on a partial failure, `retrying` = targets the gateway keeps
 * retrying, `failed` = targets it gave up on (alerted CRITICAL).
 */
import { defineStore } from 'pinia'
import type { AccountState, FirmControl, FirmControlState, FirmSnapshot, KillSwitchResult, WsServerMessage } from '@/types'
import { ACCOUNT_STATE_VALUES } from '@/types'
import { ApiError } from '@/api/http'
import { rt } from '@/runtime'
import { isFirmSnapshot, isObj, processStateStale } from '@/utils/normalize'
import { useAccounts } from './accounts'
import { useClock } from './clock'
import { useSystem } from './system'

/** Without a FirmSnapshot for this long the firm totals are stale (the coordinator publishes at 1 Hz). */
export const FIRM_STALE_MS = 3000
/** The shard that runs the firm coordinator (services.yaml `coordinator: true`). */
export const COORDINATOR = 'risk-engine-0'
const GONE_STATES = ['EXITED', 'RESTARTING', 'STOPPED', 'FAILED']

export interface FirmView {
  source: 'firm' | 'accounts'
  trading_day: string | null
  equity: number | null
  daily_pnl: number | null
  daily_pnl_pct_init: number | null
  margin: number | null
  risk_degree: number | null
  gross_notional: number | null
  net_notional: number | null
  accounts_by_state: Record<AccountState, number>
  /** effective kill switch (manual OR rule); null = unknown (no FirmSnapshot, no status) */
  kill_switch: boolean | null
  /** the two layers when known */
  control: FirmControlState | null
  as_of_ns: number | null
  /** accounts without numbers yet (reduced summaries): totals cover the others only */
  partial: number
  /** the FirmSnapshot is not current: no frame for > 3 s, or the coordinator shard is down */
  stale: boolean
  /** browser ms since the last FirmSnapshot frame (null: none yet) */
  stale_ms: number | null
  /** shards whose figures in the snapshot are older than the snapshot itself */
  stale_shards: number[]
}

export type KillStateKind = 'on' | 'off' | 'partial-on' | 'partial-off' | 'unknown' | 'stale'

/** What the kill switch really is: effective flag, its confirmation by every target, freshness. */
export interface KillState {
  kind: KillStateKind
  /** card text, e.g. "已开启 · 全部禁开", "部分生效 2/3（未确认：risk-engine-1）" */
  text: string
  /** header badge text */
  short: string
  confirmed: number
  total: number
  /** targets of the current command that have not confirmed (pending + failed) */
  unconfirmed: string[]
  failed: string[]
  /** last known effective flag (for a stale / unknown state) */
  lastKnown: boolean | null
}

/** Outcome of the last kill-switch command, shown next to the switch. */
export interface KillSwitchOutcome {
  on: boolean
  at_ms: number
  /** envelope code: OK, or the first failing target's code */
  code: string
  message: string
  duplicate: boolean
  result: KillSwitchResult
}

export function isKillSwitchResult(x: unknown): x is KillSwitchResult {
  return isObj(x) && Array.isArray(x.targets) && typeof x.all_ok === 'boolean'
}

/** `retrying` / `failed` default to empty lists (cheap guard for a partial body). */
function killResult(r: KillSwitchResult): KillSwitchResult {
  return { ...r, retrying: Array.isArray(r.retrying) ? r.retrying : [], failed: Array.isArray(r.failed) ? r.failed : [] }
}

function sumOrNull(values: (number | null | undefined)[]): number | null {
  const nums = values.filter((v): v is number => typeof v === 'number' && Number.isFinite(v))
  return nums.length ? nums.reduce((a, b) => a + b, 0) : null
}

export const useFirm = defineStore('firm', {
  state: () => ({
    snapshot: null as FirmSnapshot | null,
    /** kill-switch layers carried by the `firm` snapshot, updated by FirmControl events */
    control: null as FirmControlState | null,
    atMs: 0,
    /** manual kill-switch flag learnt from a FirmControl frame or our own command */
    killSwitchHint: null as boolean | null,
    lastKill: null as KillSwitchOutcome | null,
  }),
  getters: {
    view(state): FirmView {
      const accounts = useAccounts()
      const clock = useClock()
      const counts = Object.fromEntries(ACCOUNT_STATE_VALUES.map((s) => [s, 0])) as Record<AccountState, number>
      for (const a of accounts.list) counts[a.state] += 1
      const sys = useSystem()
      const s = state.snapshot
      const control = state.control ?? sys.status?.firm_control ?? null
      // FirmSnapshot omits `kill_switch` when false (msgspec omit_defaults)
      const kill = s ? (s.kill_switch ?? false) : (state.killSwitchHint ?? control?.kill_switch ?? sys.status?.kill_switch ?? null)
      if (s) {
        const byState = { ...counts }
        for (const k of ACCOUNT_STATE_VALUES) if (typeof s.accounts_by_state?.[k] === 'number') byState[k] = s.accounts_by_state[k] as number
        const coord = sys.services[COORDINATOR]
        const coordDown = coord ? (coord.liveness === 'DOWN' || (coord.process_state !== null && GONE_STATES.includes(coord.process_state))) && !processStateStale(coord) : false
        const ageMs = state.atMs ? Math.max(0, clock.nowMs - state.atMs) : null
        const staleShards = (s.shards ?? []).filter((x) => s.as_of_ns - x.as_of_ns > FIRM_STALE_MS * 1e6).map((x) => x.shard)
        return {
          source: 'firm',
          trading_day: s.trading_day,
          equity: s.equity,
          daily_pnl: s.daily_pnl,
          daily_pnl_pct_init: s.daily_pnl_pct_init,
          margin: s.margin,
          risk_degree: s.risk_degree,
          gross_notional: s.gross_notional,
          net_notional: s.net_notional,
          accounts_by_state: byState,
          kill_switch: kill,
          control,
          as_of_ns: s.as_of_ns,
          partial: 0,
          stale: coordDown || (ageMs !== null && ageMs > FIRM_STALE_MS),
          stale_ms: ageMs,
          stale_shards: staleShards,
        }
      }
      const list = accounts.list
      const withNumbers = list.filter((x) => x.equity !== null)
      const equity = sumOrNull(withNumbers.map((x) => x.equity))
      const pnl = sumOrNull(withNumbers.map((x) => x.daily_pnl))
      const init = sumOrNull(withNumbers.map((x) => x.initial_capital)) ?? 0
      const margins = withNumbers.map((x) => x.margin).filter((m): m is number => typeof m === 'number')
      const margin = margins.length === withNumbers.length && withNumbers.length ? margins.reduce((a, b) => a + b, 0) : null
      return {
        source: 'accounts',
        trading_day: accounts.tradingDay,
        equity,
        daily_pnl: pnl,
        daily_pnl_pct_init: init > 0 && pnl !== null ? pnl / init : null,
        margin,
        risk_degree: margin !== null && equity !== null && equity > 0 ? margin / equity : null,
        gross_notional: null,
        net_notional: null,
        accounts_by_state: counts,
        kill_switch: kill,
        control,
        as_of_ns: null,
        partial: list.length - withNumbers.length,
        stale: false,
        stale_ms: null,
        stale_shards: [],
      }
    },

    /**
     * The kill switch as an operator must read it: "on" only when every target confirmed the
     * current command; "partial" while some are pending / failed; "unknown (stale)" while the
     * firm view is stale (the coordinator's flag cannot be trusted).
     */
    killState(): KillState {
      const v = this.view
      const ctrl = useSystem().killSwitchControl
      const base = { confirmed: 0, total: 0, unconfirmed: [] as string[], failed: [] as string[], lastKnown: v.kill_switch }
      if (v.stale) {
        const last = v.kill_switch === null ? '' : `（最后已知：${v.kill_switch ? '开启' : '关闭'}）`
        return { ...base, kind: 'stale', text: `未知（陈旧）${last}`, short: '全局禁开：未知（陈旧）' }
      }
      if (ctrl && (ctrl.pending.length || ctrl.failed.length)) {
        const total = Object.keys(ctrl.targets).length
        const unconfirmed = [...ctrl.pending, ...ctrl.failed]
        const word = ctrl.desired_on ? '部分生效' : '部分解除'
        return {
          ...base,
          kind: ctrl.desired_on ? 'partial-on' : 'partial-off',
          text: `${word} ${ctrl.confirmed.length}/${total}（未确认：${unconfirmed.join('，')}）`,
          short: `全局禁开：${word} ${ctrl.confirmed.length}/${total}`,
          confirmed: ctrl.confirmed.length,
          total,
          unconfirmed,
          failed: [...ctrl.failed],
        }
      }
      if (v.kill_switch === null) return { ...base, kind: 'unknown', text: '未知（未收到公司层数据）', short: '全局禁开：未知' }
      return v.kill_switch ? { ...base, kind: 'on', text: '已开启 · 全部禁开', short: '全局禁开：开启' } : { ...base, kind: 'off', text: '关闭', short: '全局禁开：关闭' }
    },
  },
  actions: {
    applyFrame(msg: WsServerMessage): void {
      const d = msg.data
      if (isFirmSnapshot(d)) {
        const { control, ...snap } = d as FirmSnapshot & { control?: FirmControlState | null }
        this.snapshot = snap
        this.control = control ?? null
        this.atMs = Date.now()
        this.killSwitchHint = null
      } else if (isObj(d) && typeof d.action === 'string' && typeof d.origin === 'string') {
        this.applyControl(d as unknown as FirmControl)
      }
    },

    /** A FirmControl event: update the layer it belongs to; the next snapshot carries the effective flag. */
    applyControl(c: FirmControl): void {
      const cur: FirmControlState = this.control ?? { kill_switch: false, manual: null, rule: null }
      if (c.origin === 'MANUAL' && (c.action === 'KILL_SWITCH_ON' || c.action === 'KILL_SWITCH_OFF')) {
        const on = c.action === 'KILL_SWITCH_ON'
        cur.manual = { active: on, actor: c.actor ?? null, reason: c.reason ?? '', ts_ns: c.ts_ns }
      } else if (c.origin === 'RULE') {
        const rule = cur.rule ?? { active: false, restrictions: [], ts_ns: 0 }
        if (c.ts_ns >= rule.ts_ns) {
          if (c.action === 'RULE_STATE') {
            rule.active = Boolean(c.active)
            rule.restrictions = c.restrictions ?? []
          } else if (c.action === 'KILL_SWITCH_ON' || c.action === 'KILL_SWITCH_OFF') rule.active = c.action === 'KILL_SWITCH_ON'
          else if (c.action === 'SET_RESTRICTIONS') rule.restrictions = c.restrictions ?? []
          cur.rule = { ...rule, ts_ns: c.ts_ns, reason: c.reason ?? '', cause_alert_key: c.cause_alert_key ?? null }
        }
      }
      cur.kill_switch = Boolean(cur.manual?.active) || Boolean(cur.rule?.active)
      this.control = { ...cur }
      if (this.snapshot) this.snapshot = { ...this.snapshot, kill_switch: cur.kill_switch }
    },

    /**
     * Send the kill switch. A partial failure (some targets confirmed, the others being
     * retried by the gateway) resolves with the outcome instead of throwing; only an
     * answer without per-target results (400 / 401 / 403 / 409 …) throws.
     */
    async setKillSwitch(on: boolean, reason: string, key?: string): Promise<KillSwitchOutcome> {
      let outcome: KillSwitchOutcome
      try {
        const res = await rt().api.killSwitch({ on, reason }, key)
        outcome = { on, at_ms: Date.now(), code: res.code, message: res.message, duplicate: res.duplicate, result: killResult(res.data) }
      } catch (e) {
        if (e instanceof ApiError && isKillSwitchResult(e.data)) {
          outcome = { on, at_ms: Date.now(), code: e.code, message: e.message, duplicate: e.duplicate, result: killResult(e.data) }
        } else {
          throw e
        }
      }
      this.lastKill = outcome
      if (outcome.result.targets.some((t) => t.ok)) {
        // at least one shard applied it. ON is effective at once; after OFF a rule-origin
        // kill may still hold it on: the next FirmSnapshot (1 Hz) has the effective flag.
        this.killSwitchHint = on
        if (this.snapshot && on) this.snapshot = { ...this.snapshot, kill_switch: true }
      }
      return outcome
    },
  },
})
