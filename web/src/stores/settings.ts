/**
 * User settings.
 *
 * - The API key (sent as `X-API-Key` and in the WS auth frame) is kept in
 *   **sessionStorage** by default: it lives as long as the browser tab and is not written
 *   to disk-backed storage. "Remember this key" (`rememberKey`) opts into localStorage for
 *   a personal workstation. Report content is rendered sandboxed (ReportViewer), so no
 *   report script can run in this origin and read either storage.
 * - Everything else (mock toggle, up/down colors, store throttle, notifications) stays in
 *   localStorage.
 */
import { defineStore } from 'pinia'

const LS_KEY = 'futrisk.settings.v1'
const SS_KEY = 'futrisk.apiKey.v1'

export type ColorScheme = 'red-up' | 'green-up'

export interface SettingsState {
  apiKey: string
  /** keep the API key in localStorage (remember me) instead of sessionStorage */
  rememberKey: boolean
  /** explicit user choice; null = follow VITE_MOCK */
  mockOverride: boolean | null
  colorScheme: ColorScheme
  throttleMs: number
  notify: boolean
}

export function envMock(): boolean {
  const v = String(import.meta.env.VITE_MOCK ?? '').toLowerCase()
  return v === '1' || v === 'true' || v === 'yes'
}

function defaults(): SettingsState {
  return { apiKey: '', rememberKey: false, mockOverride: null, colorScheme: 'red-up', throttleMs: 200, notify: true }
}

function storage(kind: 'local' | 'session'): Storage | null {
  try {
    return (kind === 'local' ? globalThis.localStorage : globalThis.sessionStorage) ?? null
  } catch {
    return null // storage disabled
  }
}

function persist(s: SettingsState): void {
  const ls = storage('local')
  const ss = storage('session')
  try {
    ls?.setItem(
      LS_KEY,
      JSON.stringify({
        ...(s.rememberKey ? { apiKey: s.apiKey } : {}),
        rememberKey: s.rememberKey,
        mockOverride: s.mockOverride,
        colorScheme: s.colorScheme,
        throttleMs: s.throttleMs,
        notify: s.notify,
      }),
    )
    if (s.rememberKey || !s.apiKey) ss?.removeItem(SS_KEY)
    else ss?.setItem(SS_KEY, s.apiKey)
  } catch {
    // storage unavailable (private mode): keep in memory only
  }
}

export function loadSettings(): SettingsState {
  const d = defaults()
  let p: Partial<SettingsState> = {}
  try {
    const raw = storage('local')?.getItem(LS_KEY)
    if (raw) p = JSON.parse(raw) as Partial<SettingsState>
  } catch {
    p = {}
  }
  const rememberKey = p.rememberKey === true
  let sessionKey = ''
  try {
    sessionKey = storage('session')?.getItem(SS_KEY) ?? ''
  } catch {
    sessionKey = ''
  }
  const out: SettingsState = {
    apiKey: sessionKey || (rememberKey && typeof p.apiKey === 'string' ? p.apiKey : ''),
    rememberKey,
    mockOverride: typeof p.mockOverride === 'boolean' ? p.mockOverride : null,
    colorScheme: p.colorScheme === 'green-up' ? 'green-up' : 'red-up',
    throttleMs: typeof p.throttleMs === 'number' && p.throttleMs >= 0 && p.throttleMs <= 5000 ? p.throttleMs : d.throttleMs,
    notify: typeof p.notify === 'boolean' ? p.notify : d.notify,
  }
  // a key stored in localStorage by an earlier version (no rememberKey flag): move it to the session
  if (p.rememberKey === undefined && typeof p.apiKey === 'string' && p.apiKey) {
    out.apiKey = sessionKey || p.apiKey
    persist(out)
  }
  return out
}

export const useSettings = defineStore('settings', {
  state: (): SettingsState => loadSettings(),
  getters: {
    mock: (s): boolean => (s.mockOverride === null ? envMock() : s.mockOverride),
    hasKey: (s): boolean => s.apiKey.trim().length > 0,
  },
  actions: {
    save(patch: Partial<SettingsState>): void {
      Object.assign(this, patch)
      persist(this.$state)
      this.applyColorScheme()
    },
    applyColorScheme(): void {
      if (typeof document !== 'undefined') document.documentElement.dataset.updown = this.colorScheme
    },
  },
})
