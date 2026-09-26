/** EOD statistics from settlement history: daily return, volatility, Sharpe, max drawdown, breakers. */
import { describe, expect, it } from 'vitest'
import type { AccountSettlement, CompareHistory } from '@/types'
import { annualizedVol, cumulativeReturn, dailyReturn, maxDrawdown, mean, sharpe, stdev, summarizeHistory } from '@/utils/stats'

function row(account_id: string, trading_day: string, over: Partial<AccountSettlement>): AccountSettlement {
  return {
    trading_day,
    account_id,
    version: 1,
    pre_balance: '1000000.00',
    deposit: '0.00',
    withdraw: '0.00',
    close_pnl: '0.00',
    position_pnl: '0.00',
    fee: '0.00',
    balance: '1000000.00',
    equity: '1000000.00',
    margin: '100000.00',
    available: '900000.00',
    risk_degree: '0.1',
    margin_call: '0.00',
    exch_formula_pnl: '0.00',
    fifo_realized_pnl: '0.00',
    fifo_float_pnl: '0.00',
    initial_capital: '1000000.00',
    cum_net_deposit: '0.00',
    hwm_equity: '1000000.00',
    nav: '1.000000',
    max_drawdown: '0.000000',
    checks_passed: true,
    ...over,
  }
}

describe('basic statistics', () => {
  it('mean and sample standard deviation', () => {
    expect(mean([1, 2, 3, 4])).toBe(2.5)
    expect(stdev([1, 2, 3, 4])).toBeCloseTo(1.2909944, 6)
    expect(mean([])).toBeNull()
    expect(stdev([1])).toBeNull()
  })

  it('annualizes volatility with sqrt(252)', () => {
    const r = [0.01, -0.01, 0.02, -0.02]
    expect(annualizedVol(r)).toBeCloseTo((stdev(r) as number) * Math.sqrt(252), 12)
  })

  it('Sharpe = mean / stdev × sqrt(252) with rf = 0', () => {
    const r = [0.01, 0.002, -0.004, 0.006]
    const expected = ((mean(r) as number) / (stdev(r) as number)) * Math.sqrt(252)
    expect(sharpe(r)).toBeCloseTo(expected, 12)
    expect(sharpe([0.01, -0.01, 0.01, -0.01])).toBeCloseTo(0, 12)
    expect(sharpe([0.001, 0.001, 0.001])).toBeNull()
  })

  it('max drawdown is the largest peak-to-trough decline', () => {
    expect(maxDrawdown([1, 1.1, 0.99, 1.05, 0.88, 1.2])).toBeCloseTo(0.2, 12)
    expect(maxDrawdown([1, 1.01, 1.02])).toBe(0)
    expect(maxDrawdown([])).toBeNull()
  })

  it('compounds cumulative returns', () => {
    expect(cumulativeReturn([0.1, -0.1])).toBeCloseTo(-0.01, 12)
    expect(cumulativeReturn([])).toBeNull()
  })
})

describe('dailyReturn', () => {
  it('is P&L over static equity, so deposits are not performance', () => {
    const r = row('ACC2', '20260826', { pre_balance: '1000000.00', deposit: '100000.00', close_pnl: '5000.00', position_pnl: '7000.00', fee: '1000.00' })
    expect(dailyReturn(r)).toBeCloseTo(0.01, 12)
  })

  it('handles withdrawals and missing inputs', () => {
    expect(dailyReturn(row('A', 'd', { pre_balance: '1000000', withdraw: '500000', close_pnl: '5000', position_pnl: '0', fee: '0' }))).toBeCloseTo(0.01, 12)
    expect(dailyReturn(row('A', 'd', { close_pnl: 'x' }))).toBeNull()
    expect(dailyReturn(row('A', 'd', { pre_balance: '0' }))).toBeNull()
  })
})

describe('summarizeHistory', () => {
  const history: CompareHistory = {
    days: [
      { trading_day: '20260924', accounts: [row('ACC1', '20260924', { close_pnl: '-20000', nav: '0.980000' }), row('ACC3', '20260924', { position_pnl: '-31000', nav: '0.969000' })], breakers: { ACC3: 1 } },
      { trading_day: '20260922', accounts: [row('ACC1', '20260922', { close_pnl: '10000', nav: '1.010000' })], breakers: {} },
      { trading_day: '20260923', accounts: [row('ACC1', '20260923', { position_pnl: '5000', nav: '1.015000' }), row('ACC3', '20260923', { position_pnl: '0', nav: '' })] },
    ],
  }

  it('sorts days, aggregates per account and counts breakers', () => {
    const s = summarizeHistory(history)
    expect(s.map((x) => x.account_id)).toEqual(['ACC1', 'ACC3'])
    const a1 = s[0]
    expect(a1?.days).toBe(3)
    expect(a1?.returns.map((r) => r.td)).toEqual(['20260922', '20260923', '20260924'])
    expect(a1?.latest_return).toBeCloseTo(-0.02, 12)
    expect(a1?.latest_td).toBe('20260924')
    expect(a1?.latest_nav).toBeCloseTo(0.98, 12)
    expect(a1?.max_drawdown).toBeCloseTo((1.015 - 0.98) / 1.015, 12)
    expect(a1?.breakers).toBe(0)
    expect(s[1]?.breakers).toBe(1)
  })

  it('falls back to a chained NAV when the settlement NAV is missing', () => {
    const a3 = summarizeHistory(history).find((x) => x.account_id === 'ACC3')
    expect(a3?.navs[0]?.nav).toBeCloseTo(1, 12)
    expect(a3?.navs[1]?.nav).toBeCloseTo(0.969, 12)
  })

  it('counts breakers only over days whose count is known and flags the rest as partial', () => {
    // 20260923 has no `breakers` (a risk shard did not answer the history request)
    const s = summarizeHistory(history)
    expect(s[0]?.breakers).toBe(0)
    expect(s[0]?.breakers_partial).toBe(true)
    // the dashboard counted that day from GET /risk/events?td=
    const full = summarizeHistory(history, { dayBreakers: { '20260923': { ACC1: 2 } } })
    expect(full[0]?.breakers).toBe(2)
    expect(full[1]?.breakers).toBe(1)
    expect(full[0]?.breakers_partial).toBe(false)
  })

  it('marks a counted day with shard errors as partial and reports null without any count', () => {
    const noCounts = summarizeHistory({ days: history.days.map((d) => ({ trading_day: d.trading_day, accounts: d.accounts })) })
    expect(noCounts[0]?.breakers).toBeNull()
    const gap = summarizeHistory(history, { dayBreakers: { '20260923': { ACC1: 2 } }, gaps: ['20260923'] })
    expect(gap[0]?.breakers).toBe(2)
    expect(gap[0]?.breakers_partial).toBe(true)
  })
})
