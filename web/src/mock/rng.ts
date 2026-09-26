/** Seeded PRNG (mulberry32) so every mock run of the demo is reproducible. */
export class Rng {
  private s: number
  private spare: number | null = null

  constructor(seed: number) {
    this.s = seed >>> 0
  }

  next(): number {
    this.s = (this.s + 0x6d2b79f5) >>> 0
    let t = this.s
    t = Math.imul(t ^ (t >>> 15), t | 1)
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61)
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296
  }

  /** Standard normal (Box-Muller). */
  gauss(): number {
    if (this.spare !== null) {
      const v = this.spare
      this.spare = null
      return v
    }
    let u = 0
    let v = 0
    while (u === 0) u = this.next()
    while (v === 0) v = this.next()
    const r = Math.sqrt(-2 * Math.log(u))
    this.spare = r * Math.sin(2 * Math.PI * v)
    return r * Math.cos(2 * Math.PI * v)
  }

  range(a: number, b: number): number {
    return a + (b - a) * this.next()
  }

  int(a: number, b: number): number {
    return Math.floor(this.range(a, b + 1))
  }

  pick<T>(xs: readonly T[]): T {
    return xs[Math.floor(this.next() * xs.length)] as T
  }

  chance(p: number): boolean {
    return this.next() < p
  }

  /** UUID-shaped id (v7-like: time-ordered prefix) for event_id / report_id. */
  uuid(tsMs: number): string {
    const hex = (n: number, w: number) => Math.floor(n).toString(16).padStart(w, '0').slice(-w)
    const t = hex(tsMs, 12)
    const r = () => hex(this.next() * 0xffff, 4)
    return `${t.slice(0, 8)}-${t.slice(8, 12)}-7${r().slice(1)}-${(8 + this.int(0, 3)).toString(16)}${r().slice(1)}-${r()}${r()}${r()}`
  }
}
