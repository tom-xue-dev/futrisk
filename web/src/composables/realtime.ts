/**
 * Component-scoped helpers: WS topic subscriptions tied to the component lifetime,
 * visibility-aware polling, and a one-second ticking clock for "age" displays.
 */
import { computed, onScopeDispose, ref, toValue, watch, type ComputedRef, type MaybeRefOrGetter } from 'vue'
import { hasRuntime, rt } from '@/runtime'
import { useAccounts } from '@/stores/accounts'
import { useAuth } from '@/stores/auth'
import { useFirm } from '@/stores/firm'
import { useSystem } from '@/stores/system'

/**
 * Subscribe to WS topics while the calling component is alive. When the topic list
 * changes, the new topics are subscribed before the old ones are released so shared
 * topics never bounce (reference counted in the WS client).
 */
export function useTopics(topics: MaybeRefOrGetter<string[]>): void {
  let release: (() => void) | null = null
  const stop = watch(
    () => [...new Set(toValue(topics))].sort().join('|'),
    (joined) => {
      const list = joined ? joined.split('|') : []
      const next = list.length && hasRuntime() ? rt().ws.subscribe(list) : null
      release?.()
      release = next
    },
    { immediate: true },
  )
  onScopeDispose(() => {
    stop()
    release?.()
    release = null
  })
}

/** Run `fn` now and every `intervalMs` while the page is visible. */
export function usePolling(fn: () => unknown, intervalMs: MaybeRefOrGetter<number>, immediate = true): { refresh: () => void } {
  let timer: ReturnType<typeof setInterval> | null = null
  const run = () => {
    if (typeof document !== 'undefined' && document.visibilityState === 'hidden') return
    // a rejected key (401): no polling until a new key is saved
    if (useAuth().unauthorized) return
    try {
      const r = fn()
      if (r instanceof Promise) r.catch(() => undefined)
    } catch {
      // polling must never throw into the component
    }
  }
  const stop = watch(
    () => toValue(intervalMs),
    (ms) => {
      if (timer) clearInterval(timer)
      timer = ms > 0 ? setInterval(run, ms) : null
    },
    { immediate: true },
  )
  if (immediate) run()
  onScopeDispose(() => {
    stop()
    if (timer) clearInterval(timer)
  })
  return { refresh: run }
}

/** Reactive `Date.now()` updated every `ms` (for ages / countdowns). */
export function useNow(ms = 1000) {
  const now = ref(Date.now())
  const t = setInterval(() => (now.value = Date.now()), ms)
  onScopeDispose(() => clearInterval(t))
  return now
}

/** The current trading day: the firm's, else the gateway's, else the accounts'. */
export function useTradingDay(): ComputedRef<string | null> {
  const firm = useFirm()
  const system = useSystem()
  const accounts = useAccounts()
  return computed(() => firm.view.trading_day ?? system.status?.trading_day ?? accounts.tradingDay)
}
