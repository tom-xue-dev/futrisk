/**
 * Settlement-history statistics for the EOD comparison tab.
 *
 * Daily return = (close_pnl + position_pnl − fee) / (pre_balance + deposit − withdraw),
 * i.e. the day's P&L over the static equity, so deposits and withdrawals are never
 * counted as performance (01 §8.14, 02 §7.6). Volatility and Sharpe are annualized with
 * 252 trading days and a zero risk-free rate. Max drawdown is taken over the NAV path
 * (perf equity / initial capital, `AccountSettlement.nav`).
 */
import type { AccountSettlement, CompareHistory } from '@/types'
import { toNum } from './format'

export const TRADING_DAYS_PER_YEAR = 252

export function mean(xs: number[]): number | null {
  if (!xs.length) return null
  return xs.reduce((a, b) => a + b, 0) / xs.length
}

/** Sample standard deviation (n − 1); null for fewer than 2 points. */
export function stdev(xs: number[]): number | null {
  if (xs.length < 2) return null
  const m = mean(xs) as number
  const v = xs.reduce((a, x) => a + (x - m) ** 2, 0) / (xs.length - 1)
  return Math.sqrt(v)
}

export function annualizedVol(returns: number[], periods = TRADING_DAYS_PER_YEAR): number | null {
  const s = stdev(returns)
  return s === null ? null : s * Math.sqrt(periods)
}

/** Annualized Sharpe ratio with risk-free rate rf per period (default 0). */
export function sharpe(returns: number[], periods = TRADING_DAYS_PER_YEAR, rf = 0): number | null {
  const s = stdev(returns)
  const m = mean(returns)
  if (s === null || m === null || s === 0) return null
  return ((m - rf) / s) * Math.sqrt(periods)
}

/** Largest peak-to-trough decline of a value path, as a positive fraction. */
export function maxDrawdown(path: number[]): number | null {
  if (!path.length) return null
  let peak = path[0] as number
  let mdd = 0
  for (const v of path) {
    if (v > peak) peak = v
    if (peak > 0) mdd = Math.max(mdd, (peak - v) / peak)
  }
  return mdd
}

export function cumulativeReturn(returns: number[]): number | null {
  if (!returns.length) return null
  return returns.reduce((acc, r) => acc * (1 + r), 1) - 1
}

/** P&L over static equity for one settlement row; null when inputs are missing. */
export function dailyReturn(s: AccountSettlement): number | null {
  const close = toNum(s.close_pnl)
  const pos = toNum(s.position_pnl)
  const fee = toNum(s.fee)
  const pre = toNum(s.pre_balance)
  const dep = toNum(s.deposit) ?? 0
  const wd = toNum(s.withdraw) ?? 0
  if (close === null || pos === null || fee === null || pre === null) return null
  const base = pre + dep - wd
  if (base <= 0) return null
  return (close + pos - fee) / base
}

export interface AccountHistoryStats {
  account_id: string
  days: number
  latest_td: string | null
  latest_return: number | null
  mean_return: number | null
  vol_annual: number | null
  sharpe: number | null
  max_drawdown: number | null
  cum_return: number | null
  /** circuit breakers over the days whose count is known; null when no day has a count */
  breakers: number | null
  /** some days of the window have no count or an incomplete one (a risk shard did not answer) */
  breakers_partial: boolean
  latest_nav: number | null
  latest_equity: number | null
  navs: { td: string; nav: number }[]
  returns: { td: string; r: number }[]
}

export interface BreakerInput {
  /** counts of days whose history item lacks `breakers` (td -> account -> n), e.g. from GET /risk/events */
  dayBreakers?: Record<string, Record<string, number>>
  /** days whose count is unknown or incomplete */
  gaps?: string[]
}

/**
 * Per-account statistics over the history window. Breakers per day come from the
 * history item's `breakers` (present when every risk shard answered) or from
 * `dayBreakers`; days with neither, or listed in `gaps`, make the count partial.
 */
export function summarizeHistory(history: CompareHistory, breakerInput: BreakerInput = {}): AccountHistoryStats[] {
  const days = [...history.days].sort((a, b) => a.trading_day.localeCompare(b.trading_day))
  const byAccount = new Map<string, { td: string; s: AccountSettlement }[]>()
  const breakerMap = new Map<string, number>()
  const gaps = new Set(breakerInput.gaps ?? [])
  let known = 0
  let partial = false
  for (const d of days) {
    for (const s of d.accounts) {
      const list = byAccount.get(s.account_id) ?? []
      list.push({ td: d.trading_day, s })
      byAccount.set(s.account_id, list)
    }
    const counts = d.breakers ?? breakerInput.dayBreakers?.[d.trading_day]
    if (counts) {
      known += 1
      for (const [acct, n] of Object.entries(counts)) breakerMap.set(acct, (breakerMap.get(acct) ?? 0) + n)
    }
    if (!counts || (!d.breakers && gaps.has(d.trading_day))) partial = true
  }
  const out: AccountHistoryStats[] = []
  for (const [acct, rows] of [...byAccount.entries()].sort((a, b) => a[0].localeCompare(b[0]))) {
    const returns: { td: string; r: number }[] = []
    const navs: { td: string; nav: number }[] = []
    let chained = 1
    for (const { td, s } of rows) {
      const r = dailyReturn(s)
      if (r !== null) {
        returns.push({ td, r })
        chained *= 1 + r
      }
      const nav = toNum(s.nav)
      navs.push({ td, nav: nav !== null && nav > 0 ? nav : chained })
    }
    const rs = returns.map((x) => x.r)
    const last = rows[rows.length - 1]
    const breakers: number | null = known ? (breakerMap.get(acct) ?? 0) : null
    out.push({
      account_id: acct,
      days: rows.length,
      latest_td: last?.td ?? null,
      latest_return: returns.length ? (returns[returns.length - 1]?.r ?? null) : null,
      mean_return: mean(rs),
      vol_annual: annualizedVol(rs),
      sharpe: sharpe(rs),
      max_drawdown: maxDrawdown(navs.map((n) => n.nav)),
      cum_return: cumulativeReturn(rs),
      breakers,
      breakers_partial: partial,
      latest_nav: navs.length ? (navs[navs.length - 1]?.nav ?? null) : null,
      latest_equity: last ? toNum(last.s.equity) : null,
      navs,
      returns,
    })
  }
  return out
}
