/**
 * Mock settlement history (AccountSettlement rows for the past trading days, ending at
 * today's SOD balances) and report file rendering (risk daily HTML, CTP-style text
 * statement, CSV, settlement JSON, backtest HTML).
 */
import type { AccountSettlement, CompareHistoryDay, ReportReady, RiskEvent } from '@/types'
import { fmtMoney, fmtPct, fmtTd, shanghaiToNs } from '@/utils/format'
import { ACCOUNTS, PAST_TRADING_DAYS, type MockAccountSpec } from './config'
import { Rng } from './rng'

const dec = (v: number, d = 2): string => v.toFixed(d)

function tdNs(td: string, h: number, mi: number, s = 0): number {
  return shanghaiToNs(Number(td.slice(0, 4)), Number(td.slice(4, 6)), Number(td.slice(6, 8)), h, mi, s)
}

export interface HistoryBundle {
  days: CompareHistoryDay[]
  /** breaker events of past days (for `GET /risk/events?td=`) */
  events: Record<string, RiskEvent[]>
}

/** Equity path walked backwards from today's pre_balance: E[t-1] = E[t] / (1 + r[t]) − deposit[t]. */
function backwardPath(spec: MockAccountSpec, r: number[], dep: number[]): number[] {
  const n = r.length
  const eq = new Array<number>(n)
  eq[n - 1] = spec.preBalance
  for (let i = n - 1; i > 0; i--) eq[i - 1] = (eq[i] as number) / (1 + (r[i] as number)) - (dep[i] as number)
  return eq
}

/** Drawdown of the last day's perf equity (equity − cumulative deposits) from the window peak. */
function endDrawdown(spec: MockAccountSpec, eq: number[], dep: number[]): number {
  let cum = spec.cumNetDeposit - dep.reduce((a, b) => a + b, 0)
  let peak = Number.NEGATIVE_INFINITY
  let perf = 0
  for (let i = 0; i < eq.length; i++) {
    cum += dep[i] as number
    perf = (eq[i] as number) - cum
    peak = Math.max(peak, perf)
  }
  return peak > 0 ? (peak - perf) / peak : 0
}

/**
 * Walk each account backwards from today's pre_balance so the last settled equity equals
 * the SOD balance: E[t-1] = E[t] / (1 + r[t]) − deposit[t], with r on static equity.
 */
export function generateHistory(seed = 20260928): HistoryBundle {
  const rng = new Rng(seed)
  const days = PAST_TRADING_DAYS
  const perAccount = new Map<string, AccountSettlement[]>()
  const events: Record<string, RiskEvent[]> = {}
  for (const spec of ACCOUNTS) {
    const n = days.length
    const r: number[] = []
    for (let i = 0; i < n; i++) {
      const td = days[i] as string
      if (spec.history.breakerDays[td]) r.push(-(0.031 + rng.next() * 0.006))
      else r.push(spec.history.mu + spec.history.sigma * rng.gauss())
    }
    const dep: number[] = days.map((td) => (spec.history.depositDay?.td === td ? spec.history.depositDay.amount : 0))
    // shift the (non-breaker) returns so the window's peak sits at the configured HWM:
    // the path then ends with drawdown (hwm − perf) / hwm, as today's SOD expects
    const shifted = (delta: number) => r.map((x, i) => (spec.history.breakerDays[days[i] as string] ? x : x + delta))
    const natural = endDrawdown(spec, backwardPath(spec, r, dep), dep)
    const target = Math.min(natural, spec.history.maxEndDd ?? 0.05)
    let lo = 0
    let hi = 0.003
    if (natural > target) {
      for (let it = 0; it < 50; it++) {
        const mid = (lo + hi) / 2
        if (endDrawdown(spec, backwardPath(spec, shifted(mid), dep), dep) > target) lo = mid
        else hi = mid
      }
    } else hi = 0
    const rr = shifted(hi)
    for (let i = 0; i < n; i++) r[i] = rr[i] as number
    const eq = backwardPath(spec, r, dep)
    const rows: AccountSettlement[] = []
    let cumDep = spec.cumNetDeposit - dep.reduce((a, b) => a + b, 0)
    let hwm = Number.NEGATIVE_INFINITY
    let navPeak = 0
    let maxDd = 0
    const rd = 0.06 + rng.next() * 0.1
    for (let i = 0; i < n; i++) {
      const td = days[i] as string
      const e = eq[i] as number
      const pre = i === 0 ? e / (1 + (r[i] as number)) - (dep[i] as number) : (eq[i - 1] as number)
      cumDep += dep[i] as number
      const pnl = e - pre - (dep[i] as number)
      const fee = 600 + Math.abs(pnl) * 0.015 + rng.next() * 400
      const closePnl = (pnl + fee) * (0.3 + rng.next() * 0.3)
      const posPnl = pnl + fee - closePnl
      const perf = e - cumDep
      hwm = Math.max(hwm, perf)
      const nav = perf / spec.initialCapital
      navPeak = Math.max(navPeak, nav)
      maxDd = Math.max(maxDd, navPeak > 0 ? (navPeak - nav) / navPeak : 0)
      const riskDeg = Math.max(0.02, rd + (rng.next() - 0.5) * 0.04)
      const margin = e * riskDeg
      rows.push({
        trading_day: td,
        account_id: spec.id,
        version: 1,
        pre_balance: dec(pre),
        deposit: dec(dep[i] as number),
        withdraw: '0.00',
        close_pnl: dec(closePnl),
        position_pnl: dec(posPnl),
        fee: dec(fee),
        balance: dec(e),
        equity: dec(e),
        margin: dec(margin),
        available: dec(e - margin),
        risk_degree: dec(riskDeg, 6),
        margin_call: '0.00',
        exch_formula_pnl: dec(closePnl + posPnl),
        fifo_realized_pnl: dec(closePnl * 1.08),
        fifo_float_pnl: dec(posPnl * 0.93),
        initial_capital: dec(spec.initialCapital),
        cum_net_deposit: dec(cumDep),
        hwm_equity: dec(hwm),
        nav: dec(nav, 6),
        max_drawdown: dec(maxDd, 6),
        checks_passed: true,
      })
      if (spec.history.breakerDays[td]) {
        const ts = tdNs(td, 10, 12 + rng.int(0, 40), rng.int(0, 59))
        const v = -(0.0301 + rng.next() * 0.002)
        const list = events[td] ?? (events[td] = [])
        list.push({
          event_id: rng.uuid(ts / 1e6),
          kind: 'ALERT_RAISED',
          trading_day: td,
          ts_ns: ts,
          account_id: spec.id,
          scope_key: `acct:${spec.id}`,
          alert_key: `${td}|daily_loss|halt|acct:${spec.id}|1`,
          rule_id: 'daily_loss',
          level: 'halt',
          episode: 1,
          action: 'CIRCUIT_BREAK',
          severity: 'CRITICAL',
          latched: true,
          metric: 'account.daily_pnl_pct_init',
          value: v,
          threshold: -0.03,
          message: `${spec.id} 当日亏损 ${fmtPct(v)} 越过熔断线 -3.00%，已熔断禁开`,
          detail: {},
        })
      }
    }
    perAccount.set(spec.id, rows)
  }
  const out: CompareHistoryDay[] = days.map((td, i) => {
    const breakers: Record<string, number> = {}
    for (const spec of ACCOUNTS) if (spec.history.breakerDays[td]) breakers[spec.id] = spec.history.breakerDays[td] as number
    return {
      trading_day: td,
      version: 1,
      accounts: ACCOUNTS.map((a) => perAccount.get(a.id)?.[i]).filter((x): x is AccountSettlement => !!x),
      breakers,
    }
  })
  return { days: out, events }
}

// ---------------------------------------------------------------- report files

export function reportPath(td: string | null, type: ReportReady['type'], account?: string | null, runId?: string): string {
  if (type === 'BACKTEST_HTML') return `backtests/${runId ?? 'run'}/report.html`
  const base = `reports/${td}`
  switch (type) {
    case 'RISK_DAILY_HTML':
      return `${base}/risk_daily.html`
    case 'STATEMENT_TXT':
      return `${base}/statement_${account}.txt`
    case 'STATEMENT_CSV':
      return `${base}/statement_${account}.csv`
    default:
      return `${base}/settlement.json`
  }
}

export function reportsForDay(td: string, version: number, rng: Rng, createdNs: number): ReportReady[] {
  const list: ReportReady[] = []
  const mk = (type: ReportReady['type'], account: string | null, size: number): ReportReady => ({
    report_id: rng.uuid(createdNs / 1e6),
    trading_day: td,
    type,
    account_id: account,
    path: reportPath(td, type, account),
    sha256: Array.from({ length: 64 }, () => Math.floor(rng.next() * 16).toString(16)).join(''),
    size_bytes: size,
    version,
    created_ns: createdNs,
  })
  list.push(mk('RISK_DAILY_HTML', null, 180_000 + rng.int(0, 60_000)))
  for (const a of ACCOUNTS) list.push(mk('STATEMENT_TXT', a.id, 4_000 + rng.int(0, 2_000)))
  list.push(mk('SETTLEMENT_JSON', null, 42_000 + rng.int(0, 9_000)))
  return list
}

const esc = (s: string) => s.replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' })[c] as string)

export function renderRiskDailyHtml(day: CompareHistoryDay, events: RiskEvent[]): string {
  const rows = day.accounts
    .map((a) => {
      const pnl = Number(a.close_pnl) + Number(a.position_pnl) - Number(a.fee)
      const spec = ACCOUNTS.find((s) => s.id === a.account_id) as MockAccountSpec | undefined
      return `<tr><td>${esc(a.account_id)}</td><td>${esc(spec?.name ?? '')}</td><td class="n">${fmtMoney(a.pre_balance)}</td><td class="n">${fmtMoney(pnl)}</td><td class="n">${fmtMoney(a.fee)}</td><td class="n">${fmtMoney(a.equity)}</td><td class="n">${fmtMoney(a.margin)}</td><td class="n">${fmtPct(a.risk_degree)}</td><td class="n">${Number(a.nav).toFixed(4)}</td><td class="n">${fmtPct(a.max_drawdown)}</td></tr>`
    })
    .join('')
  const ev = events.length
    ? events.map((e) => `<li>${esc(e.message ?? e.kind)}</li>`).join('')
    : '<li>当日无熔断或重置事件</li>'
  return `<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><title>风控日报 ${fmtTd(day.trading_day)}</title>
<style>body{font-family:system-ui,"Microsoft YaHei",sans-serif;margin:32px;color:#1f2329}h1{font-size:22px}table{border-collapse:collapse;width:100%;font-size:13px}th,td{border-bottom:1px solid #e8e7e1;padding:6px 8px;text-align:left}td.n{text-align:right;font-variant-numeric:tabular-nums}th{background:#f5f6f8}.meta{color:#52514e;font-size:13px}</style></head>
<body><h1>futrisk 风控日报 · ${fmtTd(day.trading_day)}</h1>
<p class="meta">结算版本 v${day.version ?? 1} · 生成方：clearing-worker（模拟数据） · 金额单位：元</p>
<h2>资金表（CTP 口径）</h2><table><thead><tr><th>账户</th><th>策略</th><th>上日结存</th><th>当日盈亏</th><th>手续费</th><th>客户权益</th><th>保证金</th><th>风险度</th><th>单位净值</th><th>最大回撤</th></tr></thead><tbody>${rows}</tbody></table>
<h2>事件与动作</h2><ul>${ev}</ul>
<p class="meta">本报表由仪表盘模拟模式生成，用于演示 GET /reports/{report_id} 的下载流程。</p></body></html>`
}

export function renderStatementTxt(day: CompareHistoryDay, accountId: string): string {
  const a = day.accounts.find((x) => x.account_id === accountId)
  if (!a) return `无 ${accountId} 在 ${day.trading_day} 的结算记录\n`
  const line = (k: string, v: string) => `${k.padEnd(14, ' ')}${v.padStart(20, ' ')}\n`
  return (
    `                     交易结算单（逐日盯市）\n` +
    `账户：${accountId}        交易日：${fmtTd(day.trading_day)}        版本：v${a.version}\n` +
    `-----------------------------------------------------------\n` +
    line('上日结存', fmtMoney(a.pre_balance)) +
    line('出入金', fmtMoney(Number(a.deposit) - Number(a.withdraw))) +
    line('平仓盈亏', fmtMoney(a.close_pnl)) +
    line('持仓盯市盈亏', fmtMoney(a.position_pnl)) +
    line('手续费', fmtMoney(a.fee)) +
    line('期末结存', fmtMoney(a.balance)) +
    line('客户权益', fmtMoney(a.equity)) +
    line('保证金占用', fmtMoney(a.margin)) +
    line('可用资金', fmtMoney(a.available)) +
    line('风险度', fmtPct(a.risk_degree)) +
    line('追加保证金', fmtMoney(a.margin_call)) +
    `-----------------------------------------------------------\n` +
    `校验：资金恒等式 / 交易所公式 / 逐日盯市 = 逐笔对冲：${a.checks_passed ? '全部通过' : '存在差异'}\n`
  )
}

export function renderStatementCsv(day: CompareHistoryDay, accountId: string): string {
  const a = day.accounts.find((x) => x.account_id === accountId)
  if (!a) return 'field,value\n'
  return Object.entries(a)
    .map(([k, v]) => `${k},${String(v)}`)
    .join('\n')
}

export function renderBacktestHtml(name: string, summary: Record<string, unknown>): string {
  const rows = Object.entries(summary)
    .map(([k, v]) => `<tr><td>${esc(k)}</td><td><pre>${esc(JSON.stringify(v, null, 2))}</pre></td></tr>`)
    .join('')
  return `<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><title>回测报告 ${esc(name)}</title><style>body{font-family:system-ui,"Microsoft YaHei",sans-serif;margin:32px}td{border-bottom:1px solid #eee;padding:6px 10px;vertical-align:top}</style></head><body><h1>回测报告：${esc(name)}</h1><table>${rows}</table></body></html>`
}
