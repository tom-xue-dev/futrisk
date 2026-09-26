/**
 * Service topology & health.
 *
 * - `GET /system/services` → `{items, events, supervisor}`: supervisor ServiceInfo (when
 *   `svc.ps` answers) merged with the latest heartbeat of every configured instance, plus
 *   the gateway's `liveness` (UP / DOWN / UNKNOWN); `supervisor` is the `svc.ps` probe.
 * - `GET /system/status`: read-model summary and what an operator must act on: app
 *   mode, the effective kill switch with its MANUAL / RULE layers (`firm_control`), the
 *   manual kill switch's desired state and per-target confirmation
 *   (`kill_switch_control`, `kill_switch_retry`), key problems (`auth`), notification
 *   counts, read-model sync / repair.
 * - WS `system`: 1 Hz snapshots of `kind: "service"` items plus the `kind: "clock"` item
 *   (instance `sys.clock`); events with `kind` "service" (ServiceStateChanged),
 *   "kill_switch" (progress of the manual kill switch) and "config" (2PC alerts such as
 *   CONFIG_FAILED).
 * - Market time: every WS frame's `mts` (market epoch ms), else the last ClockState
 *   extrapolated with the browser clock.
 */
import { defineStore } from 'pinia'
import type {
  ClockState,
  KillSwitchControl,
  ServiceHealth,
  ServiceStateChanged,
  SupervisorProbe,
  SystemConfigEvent,
  SystemKillSwitchEvent,
  SystemStatus,
  WsServerMessage,
} from '@/types'
import { ApiError } from '@/api/http'
import { rt } from '@/runtime'
import { isClockItem, isClockState, isObj, isServiceStateChanged, toServiceHealth } from '@/utils/normalize'

const MAX_EVENTS = 400
const MAX_CONFIG_EVENTS = 50
const seen = new Set<string>()

export type SystemAlertNotifier = (ev: SystemConfigEvent | SystemKillSwitchEvent) => void
let alertNotifier: SystemAlertNotifier | null = null

/** Installed by the app shell: toast for config failures and kill-switch targets that failed. */
export function setSystemAlertNotifier(fn: SystemAlertNotifier | null): void {
  alertNotifier = fn
}

export interface LoadError {
  code: string
  message: string
  /** id of an unexpected server error (500 INTERNAL): quote it when reporting */
  errorId?: string | null
  /** `Retry-After` of a 503 / 429 */
  retryAfterMs?: number | null
}

export function toLoadError(e: unknown): LoadError {
  if (e instanceof ApiError) return { code: e.code, message: e.message, errorId: e.errorId, retryAfterMs: e.retryAfterMs }
  return { code: 'UNAVAILABLE', message: e instanceof Error ? e.message : String(e) }
}

export const useSystem = defineStore('system', {
  state: () => ({
    services: {} as Record<string, ServiceHealth>,
    events: [] as ServiceStateChanged[],
    status: null as SystemStatus | null,
    /** browser ms of the last successful GET /system/status */
    statusAtMs: 0,
    statusError: null as LoadError | null,
    servicesError: null as LoadError | null,
    supervisor: null as SupervisorProbe | null,
    clock: null as ClockState | null,
    clockAtMs: 0,
    /** market time (epoch ms) of the last WS frame carrying `mts`, and when it arrived (browser ms) */
    mts: null as number | null,
    mtsAt: 0,
    loaded: false,
    /** latest `kind: "kill_switch"` event of the WS `system` topic (fresher than /system/status between polls) */
    killEvent: null as SystemKillSwitchEvent | null,
    /** `kind: "config"` events (config 2PC alerts), newest first */
    configEvents: [] as SystemConfigEvent[],
  }),
  getters: {
    serviceList(state): ServiceHealth[] {
      return Object.values(state.services).sort((a, b) => a.instance.localeCompare(b.instance))
    },
    /** Most common config hash across heartbeats; services with another hash are flagged. */
    majorityConfigHash(state): string | null {
      const counts = new Map<string, number>()
      for (const s of Object.values(state.services)) if (s.config_hash) counts.set(s.config_hash, (counts.get(s.config_hash) ?? 0) + 1)
      let best: string | null = null
      let n = 0
      for (const [h, c] of counts) if (c > n) [best, n] = [h, c]
      return best
    },
    /** Market time (epoch ns) at browser time `wallMs`: from the frames' `mts`, else the last ClockState. */
    marketNowAt(state) {
      return (wallMs: number = Date.now()): number | null => {
        if (state.mts !== null && wallMs - state.mtsAt < 15_000) return (state.mts + (wallMs - state.mtsAt)) * 1e6
        if (!state.clock) return null
        return state.clock.market_now_ns + (wallMs - state.clockAtMs) * 1e6
      }
    },
    /** Market time "now" (epoch ns); evaluated when read — use `marketNowAt(now)` in reactive views. */
    marketNowNs(): number | null {
      return this.marketNowAt(Date.now())
    },
    /**
     * The manual kill switch's desired state and per-target confirmation: the newer of
     * `/system/status.kill_switch_control` and the last WS `kill_switch` event.
     */
    killSwitchControl(state): KillSwitchControl | null {
      const polled = state.status?.kill_switch_control ?? null
      const ev = state.killEvent
      if (ev && (!polled || ev.command_id !== polled.command_id || ev.updated_ns >= polled.updated_ns)) {
        const pending = ev.pending ?? []
        return { ...ev, retrying: pending.length > 0 || (polled?.command_id === ev.command_id && polled.retrying) }
      }
      return polled
    },
    /** `app.mode` (`demo` enables /demo/*); null until /system/status answered. */
    appMode(state): string | null {
      return state.status?.mode ?? null
    },
    /** Instances the gateway currently sees DOWN (liveness), for banners. */
    downInstances(state): string[] {
      return Object.values(state.services)
        .filter((s) => s.liveness === 'DOWN')
        .map((s) => s.instance)
        .sort()
    },
  },
  actions: {
    upsertService(x: unknown, source: 'rest' | 'ws'): void {
      if (!isObj(x) || typeof x.instance !== 'string') return
      const next = toServiceHealth(x, this.services[x.instance], source)
      if (next) this.services[x.instance] = next
    },

    ingestEvent(ev: ServiceStateChanged): void {
      const key = `${ev.instance}|${ev.ts_ns}|${ev.to_state}`
      if (seen.has(key)) return
      seen.add(key)
      this.events.unshift(ev)
      this.events.sort((a, b) => b.ts_ns - a.ts_ns)
      if (this.events.length > MAX_EVENTS) this.events.splice(MAX_EVENTS)
      const cur = this.services[ev.instance]
      const gone = ['EXITED', 'RESTARTING', 'STOPPED', 'FAILED'].includes(ev.to_state)
      if (cur) {
        // an event older than the state already known (a REST listing / a later event) is history only
        if (cur.since_ns !== null && ev.ts_ns < cur.since_ns) return
        this.services[ev.instance] = {
          ...cur,
          process_state: ev.to_state,
          since_ns: ev.ts_ns,
          heartbeat_state: gone ? null : cur.heartbeat_state,
          restarts: ev.restarts ?? cur.restarts,
          pid: gone ? null : (ev.pid ?? cur.pid),
          last_exit_code: ev.exit_code ?? cur.last_exit_code,
          updated_ms: Date.now(),
        }
      } else {
        this.upsertService(
          { instance: ev.instance, service: ev.service, process_state: ev.to_state, since_ns: ev.ts_ns, restarts: ev.restarts ?? 0, pid: gone ? null : ev.pid, last_exit_code: ev.exit_code ?? null },
          'rest',
        )
      }
    },

    noteMts(mts: number): void {
      const now = Date.now()
      // at most a few updates per second: the view extrapolates in between
      if (this.mts === null || now - this.mtsAt >= 500 || mts < this.mts) {
        this.mts = mts
        this.mtsAt = now
      }
    },

    setClock(c: ClockState): void {
      if (!this.clock || c.version >= this.clock.version || c.market_now_ns >= this.clock.market_now_ns) {
        this.clock = c
        this.clockAtMs = Date.now()
      }
    },

    async loadServices(): Promise<void> {
      try {
        const data = await rt().api.systemServices()
        for (const x of data.items ?? []) this.upsertService(x, 'rest')
        for (const e of data.events ?? []) if (isServiceStateChanged(e)) this.ingestEvent(e)
        this.supervisor = data.supervisor ?? null
        this.servicesError = null
        this.loaded = true
      } catch (e) {
        this.servicesError = toLoadError(e)
        throw e
      }
    },

    async loadStatus(): Promise<void> {
      try {
        const data = await rt().api.systemStatus()
        this.status = data
        this.statusAtMs = Date.now()
        this.statusError = null
        if (isClockState(data.clock)) this.setClock(data.clock)
        for (const e of data.service_events ?? []) if (isServiceStateChanged(e)) this.ingestEvent(e)
      } catch (e) {
        this.statusError = toLoadError(e)
        throw e
      }
    },

    applyFrame(msg: WsServerMessage): void {
      const items = Array.isArray(msg.data) ? msg.data : [msg.data]
      for (const x of items) {
        if (!isObj(x)) continue
        if (x.kind === 'clock') {
          if (isClockItem(x)) this.setClock(x)
        } else if (x.kind === 'kill_switch') {
          const ev = x as unknown as SystemKillSwitchEvent
          const before = this.killEvent
          this.killEvent = ev
          const newlyFailed = (ev.failed ?? []).filter((t) => !(before?.command_id === ev.command_id && before.failed?.includes(t)))
          if (newlyFailed.length && msg.type === 'event') alertNotifier?.(ev)
        } else if (x.kind === 'config') {
          const ev = x as unknown as SystemConfigEvent
          this.configEvents.unshift(ev)
          if (this.configEvents.length > MAX_CONFIG_EVENTS) this.configEvents.pop()
          if (msg.type === 'event') alertNotifier?.(ev)
        } else if (msg.type === 'event' && isServiceStateChanged(x)) this.ingestEvent(x as unknown as ServiceStateChanged)
        else if (x.kind === 'service') this.upsertService(x, 'ws')
      }
    },
  },
})
