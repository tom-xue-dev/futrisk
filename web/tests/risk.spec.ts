/** Rule utilization math (02 §7.6 RuleStatus.utilization; 01 §8.14 thresholds). */
import { describe, expect, it } from 'vitest'
import type { RuleDef, RuleStatus } from '@/types'
import {
  band,
  effectiveLevels,
  fmtMetric,
  hardLimitLevel,
  hardUtilization,
  headroom,
  levelMarks,
  metricKind,
  nextLevel,
  utilization,
  worstByRule,
  worstUtilization,
} from '@/utils/risk'

const dailyLoss: RuleDef = {
  id: 'daily_loss',
  metric: 'account.daily_pnl_pct_init',
  op: '<=',
  levels: [
    { name: 'warn', threshold: -0.02, action: 'ALERT' },
    { name: 'restrict', threshold: -0.025, action: 'RESTRICT' },
    { name: 'halt', threshold: -0.03, action: 'CIRCUIT_BREAK', latch: true },
    { name: 'derisk', threshold: -0.05, action: 'LIQUIDATE' },
  ],
  overrides: { ACC2: { halt: { threshold: -0.04 } } },
}
const hwmDd: RuleDef = {
  id: 'hwm_dd',
  metric: 'account.hwm_dd',
  op: '>=',
  levels: [
    { name: 'warn', threshold: 0.08, action: 'ALERT' },
    { name: 'halve', threshold: 0.12, action: 'RESTRICT' },
    { name: 'block', threshold: 0.15, action: 'RESTRICT' },
  ],
}
const navFloor: RuleDef = {
  id: 'nav_floor',
  metric: 'account.nav',
  op: '<=',
  levels: [
    { name: 'warn', threshold: 0.9, action: 'ALERT' },
    { name: 'derisk', threshold: 0.85, action: 'LIQUIDATE' },
  ],
}
const concentration: RuleDef = {
  id: 'concentration',
  metric: 'account.effective_n',
  op: '<',
  levels: [
    { name: 'warn', threshold: 5, action: 'ALERT' },
    { name: 'restrict', threshold: 3, action: 'RESTRICT' },
  ],
}
const compliance: RuleDef = { id: 'compliance', metric: 'account.compliance_util', op: '>=', levels: [{ name: 'warn', threshold: 0.8, action: 'ALERT' }] }

describe('utilization: value ÷ limit after normalizing to "bigger is more dangerous"', () => {
  it('loss rules (threshold < 0): losses count, profits are 0', () => {
    expect(utilization(-0.021, -0.03, '<=', 'account.daily_pnl_pct_init')).toBeCloseTo(0.7, 10)
    expect(utilization(-0.03, -0.03, '<=', 'account.daily_pnl_pct_init')).toBe(1)
    expect(utilization(-0.033, -0.03, '<=', 'account.daily_pnl_pct_init')).toBeCloseTo(1.1, 10)
    expect(utilization(0.012, -0.03, '<=', 'account.daily_pnl_pct_init')).toBe(0)
  })

  it('"higher is worse" rules divide directly', () => {
    expect(utilization(0.3, 0.5, '>=', 'account.risk_degree')).toBeCloseTo(0.6, 10)
    expect(utilization(4.5, 4, '>=', 'account.leverage')).toBeCloseTo(1.125, 10)
    expect(utilization(1, 0, '>=', 'x')).toBeNull()
  })

  it('NAV floor measures the loss from par (baseline 1)', () => {
    expect(utilization(0.95, 0.85, '<=', 'account.nav')).toBeCloseTo(1 / 3, 10)
    expect(utilization(0.85, 0.85, '<=', 'account.nav')).toBeCloseTo(1, 10)
    expect(utilization(1.03, 0.85, '<=', 'account.nav')).toBe(0)
  })

  it('effective product count uses the inverse (limit ÷ value)', () => {
    expect(utilization(6, 3, '<', 'account.effective_n')).toBeCloseTo(0.5, 10)
    expect(utilization(2, 3, '<', 'account.effective_n')).toBeCloseTo(1.5, 10)
    expect(utilization(0, 3, '<', 'account.effective_n')).toBeNull()
  })

  it('missing inputs yield null', () => {
    expect(utilization(null, -0.03, '<=')).toBeNull()
    expect(utilization(-0.01, undefined, '<=')).toBeNull()
  })
})

describe('levels', () => {
  it('applies per-account overrides', () => {
    expect(effectiveLevels(dailyLoss, 'ACC2').find((l) => l.name === 'halt')?.threshold).toBe(-0.04)
    expect(effectiveLevels(dailyLoss, 'ACC3').find((l) => l.name === 'halt')?.threshold).toBe(-0.03)
    expect(effectiveLevels(dailyLoss).map((l) => l.name)).toEqual(['warn', 'restrict', 'halt', 'derisk'])
  })

  it('hard limit = first CIRCUIT_BREAK, else last RESTRICT, else last level', () => {
    expect(hardLimitLevel(dailyLoss.levels)?.name).toBe('halt')
    expect(hardLimitLevel(hwmDd.levels)?.name).toBe('block')
    expect(hardLimitLevel(navFloor.levels)?.name).toBe('derisk')
    expect(hardLimitLevel(concentration.levels)?.name).toBe('restrict')
    expect(hardLimitLevel(compliance.levels)?.name).toBe('warn')
    expect(hardLimitLevel([])).toBeNull()
  })

  it('positions every level on the hard-limit scale (hard limit = 100%)', () => {
    const pos = levelMarks(dailyLoss).map((m) => [m.name, Number(m.position.toFixed(4))])
    expect(pos).toEqual([
      ['warn', 0.6667],
      ['restrict', 0.8333],
      ['halt', 1],
      ['derisk', 1.6667],
    ])
    // ACC2's breaker at -4% moves the scale
    const acc2 = levelMarks(dailyLoss, 'ACC2').map((m) => [m.name, Number(m.position.toFixed(4))])
    expect(acc2).toEqual([
      ['warn', 0.5],
      ['restrict', 0.625],
      ['halt', 1],
      ['derisk', 1.25],
    ])
    expect(levelMarks(concentration).map((m) => [m.name, Number(m.position.toFixed(2))])).toEqual([
      ['warn', 0.6],
      ['restrict', 1],
    ])
  })

  it('hard utilization and the next level', () => {
    expect(hardUtilization(-0.0225, dailyLoss)).toBeCloseTo(0.75, 10)
    expect(hardUtilization(-0.0225, dailyLoss, 'ACC2')).toBeCloseTo(0.5625, 10)
    expect(nextLevel(0, dailyLoss)?.name).toBe('warn')
    expect(nextLevel(-0.021, dailyLoss)).toEqual({ name: 'restrict', threshold: -0.025, action: 'RESTRICT' })
    expect(nextLevel(-0.031, dailyLoss)?.name).toBe('derisk')
    expect(nextLevel(-0.06, dailyLoss)).toBeNull()
    expect(nextLevel(4, concentration)?.name).toBe('restrict')
    expect(nextLevel(0.1, hwmDd)?.name).toBe('halve')
  })

  it('headroom is the distance to a threshold in metric units', () => {
    expect(headroom(-0.021, -0.025)).toBeCloseTo(0.004, 10)
    expect(headroom(null, -0.025)).toBeNull()
  })
})

describe('bands (heat-map color classes)', () => {
  it('cuts at 60% / 80% / 100%', () => {
    expect(band(0)).toBe('ok')
    expect(band(0.599)).toBe('ok')
    expect(band(0.6)).toBe('watch')
    expect(band(0.799)).toBe('watch')
    expect(band(0.8)).toBe('warn')
    expect(band(0.999)).toBe('warn')
    expect(band(1)).toBe('breach')
    expect(band(2.4)).toBe('breach')
    expect(band(null)).toBe('na')
    expect(band(Number.NaN)).toBe('na')
  })
})

describe('worst scope selection', () => {
  const rs = (rule_id: string, scope_key: string, utilization: number | null): RuleStatus => ({ rule_id, scope_key, metric: 'm', utilization })

  it('keeps the highest utilization per rule and ranks nulls last', () => {
    const m = worstByRule([rs('product_exposure', 'acct:A/prod:rb', 0.4), rs('product_exposure', 'acct:A/prod:i', 0.9), rs('product_exposure', 'acct:A/prod:j', null), rs('leverage', 'acct:A', null)])
    expect(m.get('product_exposure')?.scope_key).toBe('acct:A/prod:i')
    expect(m.get('leverage')?.utilization).toBeNull()
  })

  it('reports the account worst hard-limit utilization (the core value), its rule and level', () => {
    const rh = (rule_id: string, hard: number | null, level: string | null, next: number | null = null): RuleStatus =>
      ({ rule_id, scope_key: 's', metric: 'm', utilization: next, hard_utilization: hard, hard_level: level })
    // the next-level utilization (0.99 on "a") must not decide: the hard limit does
    expect(worstUtilization([rh('a', 0.3, 'block', 0.99), rh('b', 0.95, 'halt'), rh('c', null, null)])).toEqual({ value: 0.95, rule: 'b', level: 'halt' })
    expect(worstUtilization([])).toEqual({ value: null, rule: null, level: null })
  })
})

describe('metric formatting', () => {
  it('formats by metric kind', () => {
    expect(metricKind('account.daily_pnl_pct_init')).toBe('pct')
    expect(fmtMetric('account.daily_pnl_pct_init', -0.0231)).toBe('-2.31%')
    expect(fmtMetric('account.nav', 0.97123)).toBe('0.9712')
    expect(fmtMetric('account.effective_n', 3.8616)).toBe('3.86')
    expect(fmtMetric('account.leverage', 2.5)).toBe('2.50×')
    expect(fmtMetric('product.utilization', 0.6558)).toBe('65.58%')
    expect(fmtMetric('account.nav', null)).toBe('—')
  })
})
