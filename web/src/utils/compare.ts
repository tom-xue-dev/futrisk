/**
 * Multi-account comparison math (02 §9.7 `compare` topic content):
 *  - account × rule utilization matrix with color bands,
 *  - headroom ranking (distance to the next level: who trips first),
 *  - product × account signed exposure grid with a firm net column.
 *
 * The same builder produces the mock backend's `compare` payload and the client-side
 * fallback used when the backend's `compare` shape is unavailable, so the math is
 * covered by one set of unit tests.
 */
import type {
  AccountSnapshot,
  CompareCell,
  CompareView,
  ExposureGrid,
  ExposureGridCell,
  HeadroomItem,
  RuleDef,
  RuleStatus,
} from '@/types'
import { isNum } from './format'
import { band, effectiveLevels, hardLimitLevel, hardUtilizationOf, nextLevel, utilization, headroom } from './risk'

function defMap(defs: RuleDef[] | null | undefined): Map<string, RuleDef> {
  const m = new Map<string, RuleDef>()
  for (const d of defs ?? []) m.set(d.id, d)
  return m
}

/** Hard-limit utilization of a rule status (`hardUtilizationOf`: the core's value when present). */
export function cellUtilization(rs: RuleStatus, def: RuleDef | undefined, accountId: string): { u: number | null; limit: number | null } {
  const { u, limit } = hardUtilizationOf(rs, def, accountId)
  return { u, limit }
}

/** Build the account × rule matrix. Rows without a status for a rule yield an 'na' cell. */
export function buildMatrix(snapshots: AccountSnapshot[], defs: RuleDef[] | null | undefined): { accounts: string[]; rules: string[]; matrix: CompareCell[] } {
  const dm = defMap(defs)
  const accounts = snapshots.map((s) => s.account_id).sort()
  const ruleIds: string[] = defs && defs.length ? defs.map((d) => d.id) : []
  if (!ruleIds.length) {
    const seen = new Set<string>()
    for (const s of snapshots) for (const r of s.rules ?? []) if (!seen.has(r.rule_id)) seen.add(r.rule_id)
    ruleIds.push(...seen)
  }
  const matrix: CompareCell[] = []
  for (const s of [...snapshots].sort((a, b) => a.account_id.localeCompare(b.account_id))) {
    for (const ruleId of ruleIds) {
      const def = dm.get(ruleId)
      let worst: { rs: RuleStatus; u: number | null; limit: number | null } | null = null
      for (const rs of s.rules ?? []) {
        if (rs.rule_id !== ruleId) continue
        const { u, limit } = cellUtilization(rs, def, s.account_id)
        if (!worst || (u !== null && (worst.u === null || u > worst.u))) worst = { rs, u, limit }
      }
      if (!worst) {
        matrix.push({
          account_id: s.account_id,
          rule_id: ruleId,
          scope_key: `acct:${s.account_id}`,
          metric: def?.metric ?? '',
          value: null,
          limit: def ? (hardLimitLevel(effectiveLevels(def, s.account_id))?.threshold ?? null) : null,
          utilization: null,
          band: 'na',
          active_level: null,
        })
        continue
      }
      matrix.push({
        account_id: s.account_id,
        rule_id: ruleId,
        scope_key: worst.rs.scope_key,
        metric: worst.rs.metric,
        value: isNum(worst.rs.value) ? worst.rs.value : null,
        limit: worst.limit,
        utilization: worst.u,
        band: band(worst.u),
        active_level: worst.rs.active_level ?? null,
      })
    }
  }
  return { accounts, rules: ruleIds, matrix }
}

/** Headroom ranking: for every account × rule (worst scope) the closeness to its next level. */
export function buildRanking(snapshots: AccountSnapshot[], defs: RuleDef[] | null | undefined): HeadroomItem[] {
  const dm = defMap(defs)
  const items: HeadroomItem[] = []
  for (const s of snapshots) {
    const best = new Map<string, HeadroomItem>()
    for (const rs of s.rules ?? []) {
      const def = dm.get(rs.rule_id)
      const value = isNum(rs.value) ? rs.value : null
      let nextName = rs.next_level ?? null
      let nextThr = isNum(rs.next_threshold) ? rs.next_threshold : null
      if ((nextName === null || nextThr === null) && def) {
        const nx = nextLevel(value, def, s.account_id)
        nextName = nx?.name ?? null
        nextThr = nx?.threshold ?? null
      }
      const op = def?.op ?? (isNum(nextThr) && nextThr < 0 ? '<=' : '>=')
      const uNext = nextThr === null ? null : utilization(value, nextThr, op, def?.metric ?? rs.metric)
      const item: HeadroomItem = {
        account_id: s.account_id,
        rule_id: rs.rule_id,
        scope_key: rs.scope_key,
        metric: rs.metric,
        value,
        next_level: nextName,
        next_threshold: nextThr,
        utilization_next: uNext,
        headroom: headroom(value, nextThr),
        headroom_money: isNum(rs.headroom_money) ? rs.headroom_money : null,
      }
      const cur = best.get(rs.rule_id)
      if (!cur || (uNext !== null && (cur.utilization_next === null || uNext > cur.utilization_next))) best.set(rs.rule_id, item)
    }
    items.push(...best.values())
  }
  return sortRanking(items)
}

/** Closest to the next level first; rows without a next level go last. */
export function sortRanking(items: HeadroomItem[]): HeadroomItem[] {
  return [...items].sort((a, b) => {
    const ua = a.next_level === null ? null : a.utilization_next
    const ub = b.next_level === null ? null : b.utilization_next
    if (ua === null && ub === null) return a.account_id.localeCompare(b.account_id) || a.rule_id.localeCompare(b.rule_id)
    if (ua === null) return 1
    if (ub === null) return -1
    return ub - ua
  })
}

/** Product × account signed net-notional grid; `firm_net` sums each product across accounts. */
export function buildExposureGrid(snapshots: AccountSnapshot[]): ExposureGrid {
  const accounts = snapshots.map((s) => s.account_id).sort()
  const cells: ExposureGridCell[] = []
  const firmNet: Record<string, number> = {}
  for (const s of snapshots) {
    for (const e of s.exposures ?? []) {
      cells.push({ product: e.product, account_id: s.account_id, net_notional: e.net_notional, net_lots: e.net_lots })
      firmNet[e.product] = (firmNet[e.product] ?? 0) + e.net_notional
    }
  }
  const gross: Record<string, number> = {}
  for (const c of cells) gross[c.product] = (gross[c.product] ?? 0) + Math.abs(c.net_notional)
  const products = Object.keys(firmNet).sort((a, b) => (gross[b] ?? 0) - (gross[a] ?? 0) || a.localeCompare(b))
  return { products, accounts, cells, firm_net: firmNet }
}

/** Full compare view from account snapshots (+ rule definitions for hard limits). */
export function buildCompareView(snapshots: AccountSnapshot[], defs: RuleDef[] | null | undefined): CompareView {
  const { accounts, rules, matrix } = buildMatrix(snapshots, defs)
  const asOf = snapshots.reduce((m, s) => Math.max(m, s.as_of_ns || 0), 0)
  return {
    trading_day: snapshots[0]?.trading_day ?? '',
    as_of_ns: asOf,
    accounts,
    rules,
    matrix,
    ranking: buildRanking(snapshots, defs),
    exposure: buildExposureGrid(snapshots),
  }
}

// ---------------------------------------------------------------- colors

function hexToRgb(hex: string): [number, number, number] {
  const h = hex.replace('#', '')
  const n = parseInt(h.length === 3 ? h.split('').map((c) => c + c).join('') : h, 16)
  return [(n >> 16) & 255, (n >> 8) & 255, n & 255]
}

function rgbToHex([r, g, b]: [number, number, number]): string {
  return `#${[r, g, b].map((v) => Math.round(Math.max(0, Math.min(255, v))).toString(16).padStart(2, '0')).join('')}`
}

/** Linear interpolation between two hex colors (t in [0, 1]). */
export function mixHex(a: string, b: string, t: number): string {
  const x = hexToRgb(a)
  const y = hexToRgb(b)
  const k = Math.max(0, Math.min(1, t))
  return rgbToHex([x[0] + (y[0] - x[0]) * k, x[1] + (y[1] - x[1]) * k, x[2] + (y[2] - x[2]) * k])
}

/** Diverging poles: long (net buy) = red, short = blue, neutral gray midpoint. */
export const DIVERGING = { long: '#d03b3b', short: '#2a78d6', mid: '#f0efec' } as const

/**
 * Diverging fill for a signed value: 0 -> gray midpoint, +maxAbs -> red pole,
 * −maxAbs -> blue pole; linear in |value| / maxAbs.
 */
export function divergingColor(value: number, maxAbs: number): string {
  if (!isNum(value) || !isNum(maxAbs) || maxAbs <= 0 || value === 0) return DIVERGING.mid
  const t = Math.min(1, Math.abs(value) / maxAbs)
  return mixHex(DIVERGING.mid, value > 0 ? DIVERGING.long : DIVERGING.short, t)
}

/** WCAG relative luminance of a hex color. */
export function luminance(hex: string): number {
  const [r, g, b] = hexToRgb(hex).map((v) => {
    const c = v / 255
    return c <= 0.03928 ? c / 12.92 : ((c + 0.055) / 1.055) ** 2.4
  }) as [number, number, number]
  return 0.2126 * r + 0.7152 * g + 0.0722 * b
}

/** Ink or white text, whichever contrasts more with the fill. */
export function textOn(fill: string): string {
  const L = luminance(fill)
  const contrastInk = (L + 0.05) / (luminance('#1f2329') + 0.05)
  const contrastWhite = (1.05) / (L + 0.05)
  return contrastWhite > contrastInk ? '#ffffff' : '#1f2329'
}
