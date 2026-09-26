/**
 * Number, money, percent and time formatting.
 *
 * - Ratios are decimals on the wire (−0.03 = −3%, README §3.4); percent formatting
 *   happens only here.
 * - Money is yuan; compact form uses 万 / 亿 as Chinese trading UIs do.
 * - Times are shown in Asia/Shanghai. Shanghai has no DST (fixed UTC+8), so the
 *   conversion is a constant offset: deterministic and independent of browser locale.
 * - Output never depends on the runtime locale (thousands separators are inserted by
 *   hand), which keeps unit tests stable.
 */

export const DASH = '—'

const SHANGHAI_OFFSET_MS = 8 * 3600 * 1000

/** Finite number type guard. */
export function isNum(v: unknown): v is number {
  return typeof v === 'number' && Number.isFinite(v)
}

/** Parse a number or DecStr ("3118.50"); null for missing/invalid input. */
export function toNum(v: unknown): number | null {
  if (isNum(v)) return v
  if (typeof v === 'string' && v.trim() !== '') {
    const n = Number(v)
    return Number.isFinite(n) ? n : null
  }
  return null
}

function groupThousands(intPart: string): string {
  return intPart.replace(/\B(?=(\d{3})+(?!\d))/g, ',')
}

/** Fixed-point with thousands separators; "-0.00" is normalized to "0.00". */
export function fmtNum(v: unknown, digits = 2): string {
  const n = toNum(v)
  if (n === null) return DASH
  let s = Math.abs(n).toFixed(digits)
  const negative = n < 0 && Number(s) !== 0
  const [ip, fp] = s.split('.')
  s = groupThousands(ip ?? '0') + (fp !== undefined ? `.${fp}` : '')
  return negative ? `-${s}` : s
}

export function fmtInt(v: unknown): string {
  return fmtNum(v, 0)
}

/** Money in yuan with 2 decimals: 12345678.9 -> "12,345,678.90". */
export function fmtMoney(v: unknown, digits = 2): string {
  return fmtNum(v, digits)
}

/** Signed money: "+1,234.00" / "-1,234.00" / "0.00". */
export function fmtSignedMoney(v: unknown, digits = 2): string {
  const n = toNum(v)
  if (n === null) return DASH
  const s = fmtNum(n, digits)
  return n > 0 && Number(Math.abs(n).toFixed(digits)) !== 0 ? `+${s}` : s
}

/**
 * Compact money: |v| >= 1e8 -> "1.23亿", |v| >= 1e4 -> "123.46万", else plain.
 * `sign` prefixes "+" for positive values.
 */
export function fmtMoneyCompact(v: unknown, digits = 2, sign = false): string {
  const n = toNum(v)
  if (n === null) return DASH
  const abs = Math.abs(n)
  let body: string
  if (abs >= 1e8) body = `${(abs / 1e8).toFixed(digits)}亿`
  else if (abs >= 1e4) body = `${(abs / 1e4).toFixed(digits)}万`
  else body = fmtNum(abs, digits)
  const zero = Number(abs.toFixed(digits)) === 0
  if (n < 0 && !zero) return `-${body}`
  if (sign && n > 0 && !zero) return `+${body}`
  return body
}

/** Percent of a decimal ratio: 0.0302 -> "3.02%"; `sign` -> "+3.02%". */
export function fmtPct(v: unknown, digits = 2, sign = false): string {
  const n = toNum(v)
  if (n === null) return DASH
  const pct = n * 100
  const body = Math.abs(pct).toFixed(digits)
  const zero = Number(body) === 0
  if (pct < 0 && !zero) return `-${body}%`
  if (sign && pct > 0 && !zero) return `+${body}%`
  return `${body}%`
}

/** Multiplier: 1.254 -> "1.25×". */
export function fmtTimes(v: unknown, digits = 2): string {
  const n = toNum(v)
  return n === null ? DASH : `${fmtNum(n, digits)}×`
}

/** Decimal places implied by a tick size: 1 -> 0, 0.5 -> 1, 0.02 -> 2, 0.005 -> 3. */
export function decimalsForTick(tick: number | null | undefined): number {
  if (!isNum(tick) || tick <= 0) return 2
  for (let d = 0; d <= 6; d++) {
    const scaled = tick * 10 ** d
    if (Math.abs(scaled - Math.round(scaled)) < 1e-9) return d
  }
  return 6
}

/** Price with decimals from the tick size (defaults to smart precision). */
export function fmtPrice(v: unknown, tick?: number | null): string {
  const n = toNum(v)
  if (n === null) return DASH
  if (tick !== undefined && tick !== null) return fmtNum(n, decimalsForTick(tick))
  const abs = Math.abs(n)
  const digits = abs >= 1000 ? 1 : abs >= 100 ? 2 : 3
  return fmtNum(n, Number.isInteger(n) ? 0 : digits)
}

/** Latency in ms: 35 -> "35 ms", 1234 -> "1.23 s". */
export function fmtLag(ms: unknown): string {
  const n = toNum(ms)
  if (n === null) return DASH
  if (Math.abs(n) >= 1000) return `${(n / 1000).toFixed(2)} s`
  return `${Math.round(n)} ms`
}

/** Microseconds (heartbeat latency summaries): 850 -> "850 µs", 2400 -> "2.40 ms". */
export function fmtMicros(us: unknown): string {
  const n = toNum(us)
  if (n === null) return DASH
  if (Math.abs(n) >= 1000) return `${(n / 1000).toFixed(2)} ms`
  return `${Math.round(n)} µs`
}

/** Duration in ms: "45s", "12m 05s", "3h 07m". */
export function fmtDuration(ms: unknown): string {
  const n = toNum(ms)
  if (n === null || n < 0) return DASH
  const s = Math.floor(n / 1000)
  if (s < 60) return `${s}s`
  const m = Math.floor(s / 60)
  if (m < 60) return `${m}m ${String(s % 60).padStart(2, '0')}s`
  const h = Math.floor(m / 60)
  return `${h}h ${String(m % 60).padStart(2, '0')}m`
}

export function fmtBytes(n: unknown): string {
  const v = toNum(n)
  if (v === null) return DASH
  if (v >= 1024 * 1024) return `${(v / 1024 / 1024).toFixed(1)} MB`
  if (v >= 1024) return `${(v / 1024).toFixed(1)} KB`
  return `${Math.round(v)} B`
}

/**
 * Normalize an epoch timestamp of unknown unit to nanoseconds
 * (seconds, milliseconds, microseconds or nanoseconds are detected by magnitude).
 */
export function toNs(ts: number): number {
  const a = Math.abs(ts)
  if (a >= 1e17) return ts
  if (a >= 1e14) return ts * 1e3
  if (a >= 1e11) return ts * 1e6
  return ts * 1e9
}

export function nsToMs(ns: number): number {
  return ns / 1e6
}

function pad2(n: number): string {
  return String(n).padStart(2, '0')
}

/** Asia/Shanghai calendar fields of an epoch in ms. */
export function shanghaiParts(ms: number): { y: number; mo: number; d: number; h: number; mi: number; s: number; ms: number } {
  const t = new Date(ms + SHANGHAI_OFFSET_MS)
  return {
    y: t.getUTCFullYear(),
    mo: t.getUTCMonth() + 1,
    d: t.getUTCDate(),
    h: t.getUTCHours(),
    mi: t.getUTCMinutes(),
    s: t.getUTCSeconds(),
    ms: t.getUTCMilliseconds(),
  }
}

/** "10:32:15" (or "10:32:15.123") in Asia/Shanghai from epoch ns. */
export function fmtTime(ns: unknown, withMs = false): string {
  const n = toNum(ns)
  if (n === null || n <= 0) return DASH
  const p = shanghaiParts(nsToMs(n))
  const base = `${pad2(p.h)}:${pad2(p.mi)}:${pad2(p.s)}`
  return withMs ? `${base}.${String(p.ms).padStart(3, '0')}` : base
}

/** "2026-09-28 10:32:15" in Asia/Shanghai from epoch ns. */
export function fmtDateTime(ns: unknown): string {
  const n = toNum(ns)
  if (n === null || n <= 0) return DASH
  const p = shanghaiParts(nsToMs(n))
  return `${p.y}-${pad2(p.mo)}-${pad2(p.d)} ${pad2(p.h)}:${pad2(p.mi)}:${pad2(p.s)}`
}

/** "HH:mm" in Asia/Shanghai from epoch ms (chart axes). */
export function fmtClockMs(ms: number, withSeconds = false): string {
  const p = shanghaiParts(ms)
  return withSeconds ? `${pad2(p.h)}:${pad2(p.mi)}:${pad2(p.s)}` : `${pad2(p.h)}:${pad2(p.mi)}`
}

/** Trading day "20260928" -> "2026-09-28". */
export function fmtTd(td: unknown): string {
  if (typeof td !== 'string' || !/^\d{8}$/.test(td)) return typeof td === 'string' && td ? td : DASH
  return `${td.slice(0, 4)}-${td.slice(4, 6)}-${td.slice(6, 8)}`
}

/**
 * Epoch ns of a Shanghai wall-clock time: `shanghaiToNs(2026, 9, 28, 10, 30)`.
 * Used by the mock market clock and the clock-jump dialog.
 */
export function shanghaiToNs(y: number, mo: number, d: number, h = 0, mi = 0, s = 0): number {
  return (Date.UTC(y, mo - 1, d, h, mi, s) - SHANGHAI_OFFSET_MS) * 1e6
}

/**
 * Epoch ns of a Beijing wall-clock string "YYYY-MM-DD HH:mm[:ss]" (or with a "T"),
 * independent of the browser's time zone; null when malformed.
 */
export function parseShanghaiWallTime(text: string | null | undefined): number | null {
  const m = /^\s*(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2})(?::(\d{2}))?\s*$/.exec(text ?? '')
  if (!m) return null
  const [y, mo, d, h, mi, s] = m.slice(1).map((x) => Number(x ?? 0)) as [number, number, number, number, number, number]
  if (mo < 1 || mo > 12 || d < 1 || d > 31 || h > 23 || mi > 59 || s > 59) return null
  return shanghaiToNs(y, mo, d, h, mi, s)
}

/** "YYYY-MM-DD HH:mm:ss" Beijing wall time of an epoch ns (value of the clock-jump picker). */
export function toShanghaiWall(ns: number): string {
  const p = shanghaiParts(nsToMs(ns))
  return `${p.y}-${pad2(p.mo)}-${pad2(p.d)} ${pad2(p.h)}:${pad2(p.mi)}:${pad2(p.s)}`
}

/** ISO-8601 string with +08:00 offset for an epoch ms: "2026-09-28T15:31:00+08:00". */
export function toShanghaiIso(ms: number): string {
  const p = shanghaiParts(ms)
  return `${p.y}-${pad2(p.mo)}-${pad2(p.d)}T${pad2(p.h)}:${pad2(p.mi)}:${pad2(p.s)}+08:00`
}

/** CSS class for a signed value under the up/down color scheme ("up" / "down" / ""). */
export function signClass(v: unknown): string {
  const n = toNum(v)
  if (n === null || n === 0) return ''
  return n > 0 ? 'up' : 'down'
}

/** Shorten long ids (UUIDs, client_order_id) for tables: "0192f3a1…9c2e". */
export function shortId(id: unknown, head = 8, tail = 4): string {
  if (typeof id !== 'string' || !id) return DASH
  if (id.length <= head + tail + 1) return id
  return `${id.slice(0, head)}…${tail > 0 ? id.slice(-tail) : ''}`
}
