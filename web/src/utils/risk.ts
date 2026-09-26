/**
 * Rule utilization math (02 §7.6 RuleStatus, 01 §8.14).
 *
 * Utilization is "value ÷ threshold after normalizing so that bigger = more dangerous":
 *
 *   op '>=' / '>'                      u = value / limit                (risk degree, leverage, drawdown)
 *   op '<=' / '<', finite baseline b   u = (b − value) / (b − limit)    (daily loss b=0, NAV b=1)
 *   op '<=' / '<', infinite baseline   u = limit / value                (effective product count N)
 *
 * u is clamped at 0 (a profitable day is 0% of the loss limit). The heat map divides by
 * the rule's *hard limit* (first CIRCUIT_BREAK level, else the last RESTRICT level,
 * else the last level) so a cell grows monotonically as the account deteriorates; the
 * ranking divides by the *next* level to show who trips first.
 */
import type { RuleDef, RuleLevelDef, RuleOp, RuleStatus, UtilBand } from '@/types'
import { DASH, fmtNum, fmtPct, fmtTimes, isNum } from './format'

/** Neutral point of "lower is worse" metrics; default 0 (losses). */
export const METRIC_BASELINE: Record<string, number> = {
  'account.nav': 1,
  'account.effective_n': Number.POSITIVE_INFINITY,
}

export function isLowerWorse(op: RuleOp | string): boolean {
  return op === '<=' || op === '<'
}

export function baselineOf(metric: string | null | undefined): number {
  if (!metric) return 0
  return METRIC_BASELINE[metric] ?? 0
}

/** Normalized utilization of `value` against `limit` (see module doc). */
export function utilization(
  value: number | null | undefined,
  limit: number | null | undefined,
  op: RuleOp | string,
  metric?: string | null,
): number | null {
  if (!isNum(value) || !isNum(limit)) return null
  let u: number
  if (!isLowerWorse(op)) {
    if (limit === 0) return null
    u = value / limit
  } else {
    const b = baselineOf(metric)
    if (!Number.isFinite(b)) {
      if (value <= 0) return null
      u = limit / value
    } else {
      const denom = b - limit
      if (denom === 0) return null
      u = (b - value) / denom
    }
  }
  if (!Number.isFinite(u)) return null
  return Math.max(0, u)
}

/** Levels of a rule with per-account overrides applied (`overrides: {ACC2: {halt: {threshold}}}`). */
export function effectiveLevels(def: RuleDef, accountId?: string | null): RuleLevelDef[] {
  const ov = accountId ? def.overrides?.[accountId] : undefined
  return def.levels.map((lv) => {
    const patch = ov?.[lv.name]
    return patch ? { ...lv, ...patch, name: lv.name } : lv
  })
}

/** Hard limit level: first CIRCUIT_BREAK, else last RESTRICT, else last level. */
export function hardLimitLevel(levels: RuleLevelDef[]): RuleLevelDef | null {
  if (levels.length === 0) return null
  const cb = levels.find((l) => l.action === 'CIRCUIT_BREAK')
  if (cb) return cb
  const restricts = levels.filter((l) => l.action === 'RESTRICT')
  if (restricts.length) return restricts[restricts.length - 1] ?? null
  return levels[levels.length - 1] ?? null
}

export interface LevelMark {
  name: string
  action: RuleLevelDef['action']
  threshold: number
  /** utilization of the threshold against the hard limit (hard limit = 1) */
  position: number
}

/** Level thresholds positioned on the hard-limit utilization scale (for gauges). */
export function levelMarks(def: RuleDef, accountId?: string | null): LevelMark[] {
  const levels = effectiveLevels(def, accountId)
  const hard = hardLimitLevel(levels)
  if (!hard) return []
  const marks: LevelMark[] = []
  for (const lv of levels) {
    const pos = utilization(lv.threshold, hard.threshold, def.op, def.metric)
    if (pos !== null) marks.push({ name: lv.name, action: lv.action, threshold: lv.threshold, position: pos })
  }
  return marks.sort((a, b) => a.position - b.position)
}

/** Utilization of a value against the rule's hard limit. */
export function hardUtilization(value: number | null | undefined, def: RuleDef, accountId?: string | null): number | null {
  const hard = hardLimitLevel(effectiveLevels(def, accountId))
  if (!hard) return null
  return utilization(value, hard.threshold, def.op, def.metric)
}

/** The next level not yet crossed by `value`, or null when all are crossed. */
export function nextLevel(
  value: number | null | undefined,
  def: RuleDef,
  accountId?: string | null,
): { name: string; threshold: number; action: RuleLevelDef['action'] } | null {
  const marks = levelMarks(def, accountId)
  if (!marks.length) return null
  const u = hardUtilization(value, def, accountId)
  if (u === null) return marks[0] ? { name: marks[0].name, threshold: marks[0].threshold, action: marks[0].action } : null
  const next = marks.find((m) => m.position > u + 1e-12)
  return next ? { name: next.name, threshold: next.threshold, action: next.action } : null
}

/** Band cut points on the utilization scale. */
export const BAND_CUTS = { watch: 0.6, warn: 0.8, breach: 1.0 } as const

export function band(u: number | null | undefined): UtilBand {
  if (!isNum(u)) return 'na'
  if (u >= BAND_CUTS.breach) return 'breach'
  if (u >= BAND_CUTS.warn) return 'warn'
  if (u >= BAND_CUTS.watch) return 'watch'
  return 'ok'
}

export const BAND_LABELS: Record<UtilBand, string> = {
  ok: '正常 <60%',
  watch: '关注 60–80%',
  warn: '接近 80–100%',
  breach: '越限 ≥100%',
  na: '无数据',
}

/** Heat-map cell fills: receding neutral for OK, status tints, critical for breach. */
export const BAND_FILL: Record<UtilBand, string> = {
  ok: '#f0efec',
  watch: '#fde7b0',
  warn: '#f5b58f',
  breach: '#d03b3b',
  na: '#fafaf8',
}

/** Meter/gauge progress colors (accent -> warning -> danger). */
export const BAND_STROKE: Record<UtilBand, string> = {
  ok: '#2a78d6',
  watch: '#eda100',
  warn: '#ec835a',
  breach: '#d03b3b',
  na: '#c3c2b7',
}

/** Distance from value to a threshold in metric units. */
export function headroom(value: number | null | undefined, threshold: number | null | undefined): number | null {
  if (!isNum(value) || !isNum(threshold)) return null
  return Math.abs(threshold - value)
}

/** Worst scope per rule (max utilization; rows without utilization rank last). */
export function worstByRule(rules: RuleStatus[] | undefined, utilOf?: (r: RuleStatus) => number | null): Map<string, RuleStatus> {
  const out = new Map<string, RuleStatus>()
  const getU = utilOf ?? ((r: RuleStatus) => (isNum(r.utilization) ? r.utilization : null))
  for (const r of rules ?? []) {
    const cur = out.get(r.rule_id)
    if (!cur) {
      out.set(r.rule_id, r)
      continue
    }
    const a = getU(r)
    const b = getU(cur)
    if (a !== null && (b === null || a > b)) out.set(r.rule_id, r)
  }
  return out
}

/**
 * Hard-limit utilization of one rule status — the ONE definition shared by the heat map, the
 * overview and the gauges: the core's `hard_utilization` / `hard_level` when the shard sends
 * them, else `value` against the rule's hard-limit level (same formula).
 */
export function hardUtilizationOf(rs: RuleStatus, def: RuleDef | undefined, accountId: string | null): { u: number | null; level: string | null; limit: number | null } {
  const hard = def ? hardLimitLevel(effectiveLevels(def, accountId)) : null
  if (isNum(rs.hard_utilization)) return { u: rs.hard_utilization, level: rs.hard_level ?? hard?.name ?? null, limit: hard?.threshold ?? null }
  if (!def || !hard) return { u: null, level: null, limit: null }
  return { u: utilization(rs.value ?? null, hard.threshold, def.op, def.metric ?? rs.metric), level: hard.name, limit: hard.threshold }
}

export interface WorstRule {
  /** hard-limit utilization (1 = at the hard limit) */
  value: number | null
  rule: string | null
  level: string | null
  scope: string | null
}

/** The account's rule closest to its hard limit (max hard-limit utilization over every rule and scope). */
export function worstHard(rules: RuleStatus[] | undefined, defs: ReadonlyMap<string, RuleDef> | Record<string, RuleDef>, accountId: string | null): WorstRule {
  const get = (id: string): RuleDef | undefined => (defs instanceof Map ? defs.get(id) : (defs as Record<string, RuleDef>)[id])
  let best: WorstRule = { value: null, rule: null, level: null, scope: null }
  for (const r of rules ?? []) {
    const { u, level } = hardUtilizationOf(r, get(r.rule_id), accountId)
    if (u !== null && (best.value === null || u > best.value)) best = { value: u, rule: r.rule_id, level, scope: r.scope_key }
  }
  return best
}

/**
 * Worst rule of the core's rule statuses without rule definitions: only `hard_utilization`
 * counts (the next-level `utilization` is another scale and is never mixed in).
 */
export function worstUtilization(rules: RuleStatus[] | undefined): { value: number | null; rule: string | null; level: string | null } {
  let best: number | null = null
  let rule: string | null = null
  let level: string | null = null
  for (const r of rules ?? []) {
    if (isNum(r.hard_utilization) && (best === null || r.hard_utilization > best)) {
      best = r.hard_utilization
      rule = r.rule_id
      level = r.hard_level ?? null
    }
  }
  return { value: best, rule, level }
}

export type MetricKind = 'pct' | 'nav' | 'count' | 'times' | 'num'

const PCT_METRICS = new Set([
  'account.daily_pnl_pct_init',
  'account.daily_pnl_pct_sod',
  'account.floating_pnl_pct_init',
  'account.intraday_dd',
  'account.hwm_dd',
  'account.risk_degree',
  'account.limit_stress',
  'account.compliance_util',
  'sector.net_pct_equity',
  'product.net_pct_equity',
  'product.utilization',
])

export function metricKind(metric: string | null | undefined): MetricKind {
  if (!metric) return 'num'
  if (PCT_METRICS.has(metric) || metric.endsWith('_pct') || metric.includes('pct_')) return 'pct'
  if (metric === 'account.nav') return 'nav'
  if (metric === 'account.effective_n') return 'count'
  if (metric === 'account.leverage') return 'times'
  return 'num'
}

/** Format a metric value by its kind: -0.0231 -> "-2.31%", 0.9712 -> "0.9712", 2.5 -> "2.50×". */
export function fmtMetric(metric: string | null | undefined, v: number | null | undefined): string {
  if (!isNum(v)) return DASH
  switch (metricKind(metric)) {
    case 'pct':
      return fmtPct(v, 2)
    case 'nav':
      return fmtNum(v, 4)
    case 'count':
      return fmtNum(v, 2)
    case 'times':
      return fmtTimes(v, 2)
    default:
      return fmtNum(v, 4)
  }
}

/** Account state implied by an action (01 §8.14 aggregation). */
export function actionTone(action: string | null | undefined): 'warning' | 'serious' | 'critical' | 'neutral' {
  switch (action) {
    case 'ALERT':
      return 'warning'
    case 'RESTRICT':
      return 'serious'
    case 'CIRCUIT_BREAK':
      return 'critical'
    case 'LIQUIDATE':
      return 'serious'
    default:
      return 'neutral'
  }
}
