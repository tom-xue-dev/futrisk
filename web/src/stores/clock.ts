/**
 * A reactive "now" ticking once a second (the app shell drives it), for staleness rules in
 * store getters: `Date.now()` inside a getter is not reactive and would be cached.
 */
import { defineStore } from 'pinia'

export const useClock = defineStore('clock', {
  state: () => ({ nowMs: Date.now() }),
  actions: {
    tick(ms: number = Date.now()): void {
      this.nowMs = ms
    },
  },
})
