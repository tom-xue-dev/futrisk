/** API key storage: sessionStorage by default, localStorage only with "remember this key". */
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { createPinia, setActivePinia } from 'pinia'
import { loadSettings, useSettings } from '@/stores/settings'

class MemStorage {
  private readonly m = new Map<string, string>()
  getItem(k: string): string | null {
    return this.m.has(k) ? (this.m.get(k) as string) : null
  }
  setItem(k: string, v: string): void {
    this.m.set(k, String(v))
  }
  removeItem(k: string): void {
    this.m.delete(k)
  }
}

const g = globalThis as unknown as { localStorage?: MemStorage; sessionStorage?: MemStorage }

beforeEach(() => {
  g.localStorage = new MemStorage()
  g.sessionStorage = new MemStorage()
  setActivePinia(createPinia())
})

afterEach(() => {
  delete g.localStorage
  delete g.sessionStorage
})

describe('settings key storage', () => {
  it('keeps the key in sessionStorage unless remembered', () => {
    const s = useSettings()
    s.save({ apiKey: 'frk_secret', throttleMs: 300 })
    expect(g.sessionStorage?.getItem('futrisk.apiKey.v1')).toBe('frk_secret')
    expect(g.localStorage?.getItem('futrisk.settings.v1')).not.toContain('frk_secret')
    expect(loadSettings()).toMatchObject({ apiKey: 'frk_secret', rememberKey: false, throttleMs: 300 })
    // a new tab: the session is gone, so is the key
    g.sessionStorage = new MemStorage()
    expect(loadSettings().apiKey).toBe('')
  })

  it('stores the key in localStorage with rememberKey and forgets it when unchecked', () => {
    const s = useSettings()
    s.save({ apiKey: 'frk_secret', rememberKey: true })
    expect(g.localStorage?.getItem('futrisk.settings.v1')).toContain('frk_secret')
    expect(g.sessionStorage?.getItem('futrisk.apiKey.v1')).toBeNull()
    g.sessionStorage = new MemStorage()
    expect(loadSettings()).toMatchObject({ apiKey: 'frk_secret', rememberKey: true })
    s.save({ rememberKey: false })
    expect(g.localStorage?.getItem('futrisk.settings.v1')).not.toContain('frk_secret')
    expect(g.sessionStorage?.getItem('futrisk.apiKey.v1')).toBe('frk_secret')
  })

  it('moves a key left in localStorage by an earlier version into the session', () => {
    g.localStorage?.setItem('futrisk.settings.v1', JSON.stringify({ apiKey: 'frk_old', colorScheme: 'green-up' }))
    const loaded = loadSettings()
    expect(loaded).toMatchObject({ apiKey: 'frk_old', rememberKey: false, colorScheme: 'green-up' })
    expect(g.localStorage?.getItem('futrisk.settings.v1')).not.toContain('frk_old')
    expect(g.sessionStorage?.getItem('futrisk.apiKey.v1')).toBe('frk_old')
  })
})
