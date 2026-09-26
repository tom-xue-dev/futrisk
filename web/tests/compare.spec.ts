/** Heat map / headroom ranking / exposure grid builders and the diverging color scale. */
import { describe, expect, it } from 'vitest'
import type { AccountSnapshot, ExposureView, RuleDef, RuleStatus } from '@/types'
import { buildCompareView, buildExposureGrid, buildMatrix, buildRanking, divergingColor, DIVERGING, mixHex, textOn } from '@/utils/compare'

const defs: RuleDef[] = [
  {
    id: 'daily_loss',
    metric: 'account.daily_pnl_pct_init',
    op: '<=',
    levels: [
      { name: 'warn', threshold: -0.02, action: 'ALERT' },
      { name: 'restrict', threshold: -0.025, action: 'RESTRICT' },
      { name: 'halt', threshold: -0.03, action: 'CIRCUIT_BREAK' },
    ],
    overrides: { ACC2: { halt: { threshold: -0.04 } } },
  },
  {
    id: 'product_exposure',
    metric: 'product.utilization',
    op: '>=',
    scope: 'account/product',
    levels: [
      { name: 'warn', threshold: 0.7, action: 'ALERT' },
      { name: 'block', threshold: 1.0, action: 'RESTRICT' },
    ],
  },
  { id: 'leverage', metric: 'account.leverage', op: '>=', levels: [{ name: 'warn', threshold: 3, action: 'ALERT' }, { name: 'block', threshold: 4, action: 'RESTRICT' }] },
]

function exposure(product: string, net: number, lots: number, sector = 'black'): ExposureView {
  return { product, sector, exchange: 'SHFE', long_lots: Math.max(0, lots), short_lots: Math.max(0, -lots), net_lots: lots, net_notional: net, gross_notional: Math.abs(net), net_pct_equity: net / 1e7 }
}

function snapshot(account_id: string, rules: RuleStatus[], exposures: ExposureView[]): AccountSnapshot {
  return {
    account_id,
    shard: 0,
    trading_day: '20260928',
    version: 1,
    state_version: 1,
    as_of_ns: 1_790_000_000_000_000_000 + account_id.length,
    quality: 'OK',
    state: 'NORMAL',
    funds: { initial_capital: 1e7, cum_net_deposit: 0, pre_balance: 1e7, net_deposit: 0, static_equity: 1e7, close_pnl: 0, position_pnl: 0, fee: 0, equity: 1e7, margin: 1e6, frozen_margin: 0, frozen_fee: 0, available: 9e6, risk_degree: 0.1 },
    metrics: { daily_pnl: 0, daily_pnl_pct_init: 0, daily_pnl_pct_sod: 0, floating_pnl_pct_init: 0, perf_equity: 1e7, nav: 1, intraday_peak_equity: 1e7, intraday_dd: 0, hwm_equity: 1e7, hwm_dd: 0, gross_notional: 0, net_notional: 0, leverage: 0, effective_n: 0, limit_stress: 0, orders_today: 0, cancels_today: 0 },
    rules,
    exposures,
  }
}

const acc1 = snapshot(
  'ACC1',
  [
    { rule_id: 'daily_loss', scope_key: 'acct:ACC1', metric: 'account.daily_pnl_pct_init', value: -0.021, active_level: 'warn', utilization: 0.84, hard_utilization: 0.7, hard_level: 'halt' },
    { rule_id: 'product_exposure', scope_key: 'acct:ACC1/prod:rb', metric: 'product.utilization', value: 0.5 },
    { rule_id: 'product_exposure', scope_key: 'acct:ACC1/prod:i', metric: 'product.utilization', value: 0.9 },
  ],
  [exposure('rb', -4_000_000, -130), exposure('i', 2_000_000, 25)],
)
const acc2 = snapshot(
  'ACC2',
  [
    { rule_id: 'daily_loss', scope_key: 'acct:ACC2', metric: 'account.daily_pnl_pct_init', value: -0.02 },
    { rule_id: 'leverage', scope_key: 'acct:ACC2', metric: 'account.leverage', value: 2.9, next_level: 'warn', next_threshold: 3, headroom_money: 123 },
  ],
  [exposure('rb', 1_000_000, 30), exposure('IF', 3_000_000, 2, 'equity_index')],
)

describe('buildMatrix', () => {
  const { accounts, rules, matrix } = buildMatrix([acc2, acc1], defs)
  const cell = (a: string, r: string) => matrix.find((c) => c.account_id === a && c.rule_id === r)

  it('orders accounts and rules deterministically', () => {
    expect(accounts).toEqual(['ACC1', 'ACC2'])
    expect(rules).toEqual(['daily_loss', 'product_exposure', 'leverage'])
    expect(matrix).toHaveLength(6)
  })

  it('divides by the hard limit with per-account overrides (not by the backend utilization)', () => {
    expect(cell('ACC1', 'daily_loss')?.utilization).toBeCloseTo(0.7, 10)
    expect(cell('ACC1', 'daily_loss')?.limit).toBe(-0.03)
    expect(cell('ACC1', 'daily_loss')?.band).toBe('watch')
    expect(cell('ACC1', 'daily_loss')?.active_level).toBe('warn')
    // ACC2's breaker is -4%: -2% is only half-way
    expect(cell('ACC2', 'daily_loss')?.utilization).toBeCloseTo(0.5, 10)
    expect(cell('ACC2', 'daily_loss')?.band).toBe('ok')
  })

  it('takes the worst scope of scoped rules', () => {
    expect(cell('ACC1', 'product_exposure')?.scope_key).toBe('acct:ACC1/prod:i')
    expect(cell('ACC1', 'product_exposure')?.utilization).toBeCloseTo(0.9, 10)
    expect(cell('ACC1', 'product_exposure')?.band).toBe('warn')
  })

  it('marks missing rule statuses as no-data', () => {
    expect(cell('ACC1', 'leverage')?.band).toBe('na')
    expect(cell('ACC1', 'leverage')?.value).toBeNull()
    expect(cell('ACC2', 'product_exposure')?.band).toBe('na')
  })

  it('uses the core hard-limit utilization when rule definitions are unknown (never the next-level one)', () => {
    const m = buildMatrix([acc1], null)
    expect(m.rules).toEqual(['daily_loss', 'product_exposure'])
    expect(m.matrix.find((c) => c.rule_id === 'daily_loss')?.utilization).toBe(0.7)
  })
})

describe('buildRanking', () => {
  it('ranks by closeness to the next level and computes missing next levels from the definitions', () => {
    const r = buildRanking([acc1, acc2], defs)
    // leverage 2.9 / 3 = 0.967 > product i 0.9 / 1.0 > ACC1 loss 0.021 / 0.025 = 0.84 > ACC2 loss 0.02 / 0.025 = 0.8
    expect(r.map((x) => `${x.account_id}:${x.rule_id}`)).toEqual(['ACC2:leverage', 'ACC1:product_exposure', 'ACC1:daily_loss', 'ACC2:daily_loss'])
    const lev = r.find((x) => x.rule_id === 'leverage')
    expect(lev?.next_level).toBe('warn')
    expect(lev?.utilization_next).toBeCloseTo(2.9 / 3, 10)
    expect(lev?.headroom).toBeCloseTo(0.1, 10)
    expect(lev?.headroom_money).toBe(123)
    const dl1 = r.find((x) => x.account_id === 'ACC1' && x.rule_id === 'daily_loss')
    expect(dl1?.next_level).toBe('restrict')
    expect(dl1?.utilization_next).toBeCloseTo(0.84, 10)
    const dl2 = r.find((x) => x.account_id === 'ACC2' && x.rule_id === 'daily_loss')
    expect(dl2?.next_level).toBe('restrict')
    expect(dl2?.utilization_next).toBeCloseTo(0.8, 10)
    const pe = r.find((x) => x.rule_id === 'product_exposure')
    expect(pe?.scope_key).toBe('acct:ACC1/prod:i')
    expect(pe?.next_level).toBe('block')
  })
})

describe('buildExposureGrid', () => {
  it('builds a signed product × account grid with a firm net column', () => {
    const g = buildExposureGrid([acc1, acc2])
    expect(g.accounts).toEqual(['ACC1', 'ACC2'])
    expect(g.products).toEqual(['rb', 'IF', 'i'])
    expect(g.firm_net).toEqual({ rb: -3_000_000, i: 2_000_000, IF: 3_000_000 })
    expect(g.cells.find((c) => c.product === 'rb' && c.account_id === 'ACC1')?.net_lots).toBe(-130)
  })

  it('is part of the full compare view', () => {
    const v = buildCompareView([acc1, acc2], defs)
    expect(v.trading_day).toBe('20260928')
    expect(v.exposure.products).toHaveLength(3)
    expect(v.ranking.length).toBeGreaterThan(0)
    expect(v.as_of_ns).toBe(acc2.as_of_ns)
  })
})

describe('colors', () => {
  it('diverges from a gray midpoint to red (long) and blue (short)', () => {
    expect(divergingColor(0, 100)).toBe(DIVERGING.mid)
    expect(divergingColor(100, 100)).toBe(DIVERGING.long)
    expect(divergingColor(-250, 100)).toBe(DIVERGING.short)
    expect(divergingColor(50, 100)).toBe(mixHex(DIVERGING.mid, DIVERGING.long, 0.5))
    expect(divergingColor(5, 0)).toBe(DIVERGING.mid)
  })

  it('interpolates hex colors linearly', () => {
    expect(mixHex('#000000', '#ffffff', 0.5)).toBe('#808080')
    expect(mixHex('#102030', '#102030', 0.3)).toBe('#102030')
  })

  it('picks the more legible text color for a fill', () => {
    expect(textOn('#d03b3b')).toBe('#ffffff')
    expect(textOn('#f0efec')).toBe('#1f2329')
    expect(textOn('#fde7b0')).toBe('#1f2329')
  })
})
