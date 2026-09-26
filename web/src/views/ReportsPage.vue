<script setup lang="ts">
/**
 * 报表: settlement days with their latest EOD phase and warning count (GET
 * /settlement/days → `{items, errors}`, WS `eod`, GET /settlement/{td}/status for days
 * outside that list and for the warnings of an expanded day), live EOD progress, EOD
 * warnings (LATE_FACT, OFFICIAL_PRICE_DIFF, PUSH_INCOMPLETE, PRE_SETTLE_MISMATCH), daily
 * risk report / statements (GET /reports, GET /reports/{id} returns the file; GET
 * /settlement/{td}/statements/{account} returns the text, or the settlement data with a
 * message when the file is missing), settlement summary (GET /settlement/{td}), backtest
 * runs with summary metrics (WS `backtests`, GET/POST /backtests). Everything here is
 * answered by the clearing-worker: while it is down the sections say so. A shard trading
 * on a provisional SOD (PROVISIONAL: EOD not settled before the next session) can be
 * released with POST /risk/eod/{td}/force-unfreeze (RISK_MANAGER, fan-out, HTTP 200).
 */
import { computed, reactive, ref, watch } from 'vue'
import JsonBlock from '@/components/JsonBlock.vue'
import ReasonDialog from '@/components/ReasonDialog.vue'
import ReportViewer, { type ReportRef } from '@/components/ReportViewer.vue'
import SourceNotice from '@/components/SourceNotice.vue'
import StatusPill from '@/components/StatusPill.vue'
import { usePolling, useTopics } from '@/composables/realtime'
import { errorText, showError, showOk, showWarn } from '@/composables/feedback'
import { ApiError } from '@/api/http'
import { rt } from '@/runtime'
import { useAccounts } from '@/stores/accounts'
import { needRoleText, useAuth } from '@/stores/auth'
import { useEod } from '@/stores/eod'
import { useFirm } from '@/stores/firm'
import { useSystem } from '@/stores/system'
import type { AccountSettlement, BacktestStatus, EodPhase, EodWarning, FanoutResult, ReportReady, SettlementResult } from '@/types'
import { EOD_PHASE_VALUES } from '@/types'
import { fmtBytes, fmtDateTime, fmtMoney, fmtNum, fmtPct, fmtTd, fmtTime, signClass, toNum } from '@/utils/format'
import { ACCOUNT_NAMES, BACKTEST_STATE_LABELS, EOD_PHASE_LABELS, EOD_WARNING_LABELS, REPORT_TYPE_LABELS, type StatusTone } from '@/utils/labels'
import { uuid } from '@/utils/uuid'

const eod = useEod()
const firm = useFirm()
const accounts = useAccounts()
const auth = useAuth()
const system = useSystem()

// `backtests`: every run as a snapshot, then one upsert per change (progress included)
useTopics(['eod', 'backtests'])

const currentTd = computed(() => firm.view.trading_day ?? accounts.tradingDay)

function loadDays(): void {
  void eod.loadDays(60).catch(() => undefined)
}
function loadReports(): void {
  void eod.loadReports().catch(() => undefined)
}
function loadBacktests(): void {
  void eod.loadBacktests().catch(() => undefined)
}
loadDays()
loadReports()
// the WS `backtests` snapshot brings the list too; the REST answer shows why it is missing
loadBacktests()
// retry what failed (clearing-worker down) every 15 s
usePolling(
  () => {
    if (eod.daysError || Object.keys(eod.daysErrors).length) loadDays()
    if (eod.reportsError) loadReports()
    if (eod.backtestsError) loadBacktests()
    return undefined
  },
  15_000,
  false,
)

watch(currentTd, (td) => td && eod.addDays([td]), { immediate: true })


const PHASE_STEPS: EodPhase[] = EOD_PHASE_VALUES.filter((p) => p !== 'FAILED')

function phaseTone(p: EodPhase | undefined): StatusTone {
  if (!p) return 'neutral'
  if (p === 'DONE') return 'good'
  if (p === 'FAILED') return 'critical'
  return 'warning'
}

const run = computed(() => eod.currentRun)
const runActive = computed(() => {
  const r = run.value
  if (!r) return false
  const last = r.events[r.events.length - 1]
  return !!last && last.phase !== 'DONE' && last.phase !== 'FAILED'
})
const runStep = computed(() => {
  const r = run.value
  if (!r) return 0
  const last = r.events[r.events.length - 1]
  if (!last) return 0
  const i = PHASE_STEPS.indexOf(last.phase)
  return last.phase === 'DONE' ? PHASE_STEPS.length : Math.max(0, i + 1)
})

const showAllDays = ref(false)
const days = computed(() =>
  eod.dayList.slice(0, showAllDays.value ? undefined : 12).map((td) => {
    const ph = eod.phaseOf(td)
    const ev = ph?.event ?? null
    const reports = eod.reportsFor(td)
    return {
      td,
      phase: ph?.phase,
      version: ph?.version,
      next: ev?.next_trading_day,
      ts: ev?.ts_ns,
      error: ev?.error,
      reports,
      daily: reports.find((r) => r.type === 'RISK_DAILY_HTML'),
      missing: eod.statusMissing[td] ?? false,
      unknown: eod.daysErrors[td] ?? null,
      warnings: eod.warningCount(td),
    }
  }),
)

function warningsOf(td: string): EodWarning[] {
  return [...(eod.warnings[td] ?? [])].reverse()
}

function warningContext(w: EodWarning): string {
  const c = w.context ?? {}
  return Object.entries(c)
    .filter(([k]) => k !== 'trading_day' && k !== 'version')
    .map(([k, v]) => `${k}=${typeof v === 'object' ? JSON.stringify(v) : String(v)}`)
    .join(' ')
}

// days outside GET /settlement/days (current trading day, report days): ask their status once
const requested = new Set<string>()
watch(
  () => days.value.map((d) => d.td).join(','),
  () => {
    for (const { td } of days.value) {
      if (requested.has(td) || eod.phaseOf(td) || eod.daysErrors[td]) continue
      requested.add(td)
      void eod.loadStatus(td).catch(() => undefined)
    }
  },
  { immediate: true },
)

function stepTime(p: EodPhase): string {
  const e = run.value?.events.find((x) => x.phase === p)
  return e ? fmtTime(e.ts_ns) : ''
}

// ---------------------------------------------------------------- PROVISIONAL shards (EOD not settled)

/** a shard trades on its provisional SOD (EOD not settled before the next session): closes only */
const provisional = computed(
  () =>
    system.status?.trading_mode === 'PROVISIONAL' ||
    Object.values(system.status?.shards ?? {}).some((s) => s?.trading_mode === 'PROVISIONAL') ||
    accounts.list.some((a) => a.trading_mode === 'PROVISIONAL'),
)
/** the day whose EOD is not settled: the latest earlier day not DONE, else the current day */
const unsettledTd = computed(() => days.value.find((d) => d.td !== currentTd.value && d.phase && d.phase !== 'DONE')?.td ?? currentTd.value ?? '')
const unfreezeDlg = ref(false)

async function forceUnfreeze(reason: string, key: string): Promise<void> {
  const td = unsettledTd.value
  let res: FanoutResult
  let duplicate = false
  try {
    const out = await rt().api.forceUnfreeze(td, { reason }, key)
    res = out.data
    duplicate = out.duplicate
  } catch (e) {
    // fan-out: HTTP 200 with ok=false and the per-target results
    if (e instanceof ApiError && e.status === 200 && e.data && typeof e.data === 'object' && Array.isArray((e.data as FanoutResult).targets)) {
      res = e.data as FanoutResult
      duplicate = e.duplicate
    } else throw e
  }
  const ok = res.targets.filter((t) => t.ok).map((t) => t.target)
  const bad = res.targets.filter((t) => !t.ok).map((t) => `${t.target}=${t.code}`)
  const dup = duplicate ? '（幂等重放）' : ''
  if (res.all_ok) showOk(`已放行 ${fmtTd(td)} 的临时快照开仓：${ok.join('，')}${dup}`)
  else showWarn(`强制放行：${ok.length}/${res.targets.length} 个分片已执行${ok.length ? '（' + ok.join('，') + '）' : ''}；${bad.join('，')}${dup}`)
  void system.loadStatus().catch(() => undefined)
}

// ---------------------------------------------------------------- report files

// report content is shown in a sandboxed viewer (never as a same-origin blob / window)
const viewer = reactive<{ open: boolean; report: ReportRef | null }>({ open: false, report: null })

function openBacktestReport(b: BacktestStatus): void {
  if (b.report_id) {
    viewer.report = { report_id: b.report_id, title: `回测报告 · ${b.name}`, filename: `${b.run_id}.html` }
    viewer.open = true
  }
}

function openReport(r: ReportReady): void {
  const label = REPORT_TYPE_LABELS[r.type as 'RISK_DAILY_HTML'] ?? r.type
  viewer.report = {
    report_id: r.report_id,
    title: `${label}${r.account_id ? ' · ' + r.account_id : ''}${r.trading_day ? ' · ' + fmtTd(r.trading_day) : ''}`,
    filename: r.path.split('/').pop() ?? 'report',
  }
  viewer.open = true
}

const statement = reactive({
  open: false,
  td: '',
  account: '',
  text: '',
  loading: false,
  error: '',
  /** fallback: the settlement data when the text file is not available */
  result: null as SettlementResult | null,
  message: '',
})
const statementRow = computed<AccountSettlement | null>(() => statement.result?.accounts.find((a) => a.account_id === statement.account) ?? statement.result?.accounts[0] ?? null)

async function showStatement(td: string, account: string): Promise<void> {
  Object.assign(statement, { open: true, td, account, text: '', loading: true, error: '', result: null, message: '' })
  try {
    const ans = await eod.loadStatement(td, account)
    statement.text = ans.text ?? ''
    statement.result = ans.result
    statement.message = ans.message
  } catch (e) {
    statement.error = errorText(e)
  } finally {
    statement.loading = false
  }
}

const results = reactive<Record<string, SettlementResult | 'loading' | string>>({})
async function onExpand(row: { td: string }, expanded: unknown): Promise<void> {
  const open = Array.isArray(expanded) ? expanded.some((x) => (x as { td: string }).td === row.td) : Boolean(expanded)
  if (!open) return
  // the warnings themselves come with the day's status (`detail.warnings`, the last 10)
  if (eod.warningCount(row.td) > (eod.warnings[row.td]?.length ?? 0)) void eod.loadStatus(row.td).catch(() => undefined)
  if (results[row.td]) return
  results[row.td] = 'loading'
  try {
    const r = await eod.loadResult(row.td)
    results[row.td] = r ?? '无结算结果'
  } catch (e) {
    results[row.td] = errorText(e)
  }
}

function resultOf(td: string): SettlementResult | null {
  const r = results[td]
  return r && typeof r === 'object' ? r : null
}

// ---------------------------------------------------------------- settlement run

const runDlg = reactive({ open: false, td: '', force: false, loading: false })
function openRun(): void {
  Object.assign(runDlg, { open: true, td: currentTd.value ?? '', force: false, loading: false })
}
async function submitRun(): Promise<void> {
  runDlg.loading = true
  try {
    const res = await rt().api.runSettlement({ td: runDlg.td, force_new_version: runDlg.force }, uuid())
    eod.addDays([runDlg.td])
    const v = res.data?.version ? ` v${res.data.version}` : ''
    showOk(`已发起 ${fmtTd(runDlg.td)}${v} 的结算；阶段进度通过 sys.eod 推送${res.duplicate ? '（幂等重放）' : ''}`)
    runDlg.open = false
  } catch (e) {
    showError(e, '发起结算失败')
  } finally {
    runDlg.loading = false
  }
}

// ---------------------------------------------------------------- backtests

/** the backtest configs shipped in config/backtest/*.yaml */
const BACKTEST_CONFIGS = ['default', 'stress', 'sweep', 'minute', 'breaker_synthetic', 'ticks'] as const
const btDlg = reactive({ open: false, name: 'default', overrides: '', loading: false, error: '' })
async function submitBacktest(): Promise<void> {
  let overrides: Record<string, unknown> | undefined
  if (btDlg.overrides.trim()) {
    try {
      overrides = JSON.parse(btDlg.overrides) as Record<string, unknown>
    } catch {
      btDlg.error = 'overrides 不是合法 JSON'
      return
    }
  }
  btDlg.loading = true
  btDlg.error = ''
  try {
    const res = await rt().api.runBacktest({ config_name: btDlg.name, overrides }, uuid())
    const b = res.data
    if (b && typeof b === 'object' && 'run_id' in b) eod.upsertBacktest(b)
    showOk(`回测已提交${res.duplicate ? '（幂等重放）' : ''}`)
    btDlg.open = false
  } catch (e) {
    btDlg.error = errorText(e)
  } finally {
    btDlg.loading = false
  }
}

function btTone(s: BacktestStatus['state']): StatusTone {
  return s === 'DONE' ? 'good' : s === 'FAILED' ? 'critical' : s === 'RUNNING' ? 'warning' : 'neutral'
}

/** Summary as metric × group table when it has nested groups (e.g. rules / no_rules). */
function summaryTable(summary: Record<string, unknown> | null | undefined): { groups: string[]; rows: { key: string; vals: unknown[] }[] } | null {
  if (!summary) return null
  const groups = Object.keys(summary).filter((k) => typeof summary[k] === 'object' && summary[k] !== null && !Array.isArray(summary[k]))
  if (!groups.length) return null
  const keys = [...new Set(groups.flatMap((g) => Object.keys(summary[g] as Record<string, unknown>)))]
  return { groups, rows: keys.map((k) => ({ key: k, vals: groups.map((g) => (summary[g] as Record<string, unknown>)[k]) })) }
}

const METRIC_LABELS: Record<string, string> = {
  total_return: '区间收益',
  max_drawdown: '最大回撤',
  calmar: 'Calmar',
  triggers: '触发次数',
  avoided_loss: '避免的损失',
  false_positive_rate: '假阳性率',
}
const GROUP_LABELS: Record<string, string> = { rules: '有风控', no_rules: '无风控' }

function metricText(k: string, v: unknown): string {
  const n = toNum(v)
  if (n === null) return v === undefined ? '—' : String(v)
  if (k === 'total_return' || k === 'max_drawdown' || k === 'false_positive_rate') return fmtPct(n, 2)
  if (k === 'avoided_loss') return fmtMoney(n, 0)
  return fmtNum(n, Number.isInteger(n) ? 0 : 2)
}

const reportTypeFilter = ref<string>('')
const reportList = computed(() => (reportTypeFilter.value ? eod.reportList.filter((r) => r.type === reportTypeFilter.value) : eod.reportList).slice(0, 200))
</script>

<template>
  <div class="page">
    <div class="page-head">
      <h1>报表</h1>
      <span class="muted">盘后清算（EOD saga）完成后由 clearing-worker 生成风控日报与结算单</span>
      <span class="spacer" />
      <el-tooltip :disabled="auth.canAdmin" :content="needRoleText('ADMIN')" placement="bottom">
        <span><el-button type="primary" plain :disabled="!auth.canAdmin" @click="openRun">发起结算</el-button></span>
      </el-tooltip>
    </div>

    <SourceNotice v-if="provisional" tone="warn" :message="`有风控分片处于 PROVISIONAL：${fmtTd(unsettledTd)} 的 EOD 未在下一交易时段开始前完成，按临时快照交易、只允许平仓。结算完成后自动恢复；紧急时可审计放行开仓。`">
      <el-tooltip :disabled="auth.canRisk" :content="needRoleText('RISK')" placement="bottom">
        <span><el-button link type="danger" size="small" :disabled="!auth.canRisk" @click="unfreezeDlg = true">强制放行开仓（force-unfreeze）</el-button></span>
      </el-tooltip>
    </SourceNotice>

    <div v-if="run" class="card">
      <div class="card-title">
        EOD 进度 · {{ fmtTd(run.td) }} v{{ run.version }}
        <StatusPill v-if="runActive" tone="warning" label="进行中" />
        <StatusPill v-else :tone="phaseTone(run.events[run.events.length - 1]?.phase)" :label="EOD_PHASE_LABELS[run.events[run.events.length - 1]?.phase ?? 'BEGIN']" />
      </div>
      <el-steps :active="runStep" finish-status="success" align-center>
        <el-step v-for="p in PHASE_STEPS" :key="p" :title="EOD_PHASE_LABELS[p]" :description="stepTime(p)" />
      </el-steps>
      <div v-if="run.events.some((e) => e.phase === 'FAILED')" class="failed">EOD 失败：{{ run.events.find((e) => e.phase === 'FAILED')?.error }}</div>
    </div>

    <div class="card" :class="{ 'section-gap': !!run }">
      <div class="card-title">结算日 <span class="hint">展开查看报表、结算单与结算摘要；阶段来自 GET /settlement/days 与 sys.eod</span></div>
      <SourceNotice v-if="eod.daysError" :error="eod.daysError" subject="结算日列表" :retry="loadDays" />
      <SourceNotice v-else :errors="eod.daysErrors" subject="结算日列表" errors-label="以下交易日的 EOD 状态未取到" compact :message="eod.daysMessage" />
      <el-table :data="days" size="small" row-key="td" empty-text="暂无结算日" @expand-change="onExpand">
        <el-table-column type="expand">
          <template #default="{ row }">
            <div class="expand">
              <div v-if="row.warnings" class="warns">
                <div class="warns-head">
                  EOD 警告 {{ row.warnings }} 条
                  <span class="muted">（不改变 EOD 阶段；需要人工判断，例如是否 force_new_version 重算）</span>
                </div>
                <div v-for="(w, i) in warningsOf(row.td)" :key="i" class="warn">
                  <StatusPill tone="warning" :label="EOD_WARNING_LABELS[w.kind] ?? w.kind" />
                  <span>{{ w.message }}</span>
                  <span class="muted small">
                    <template v-if="w.version"> · v{{ w.version }}</template><template v-if="w.phase"> · {{ EOD_PHASE_LABELS[w.phase as EodPhase] ?? w.phase }}</template><template v-if="w.ts_ns"> · {{ fmtDateTime(w.ts_ns) }}</template><template v-if="warningContext(w)"> · {{ warningContext(w) }}</template>
                  </span>
                </div>
                <div v-if="row.warnings > warningsOf(row.td).length" class="muted small">
                  {{ warningsOf(row.td).length ? `另有 ${row.warnings - warningsOf(row.td).length} 条未显示（结算状态只带最近 10 条）` : '加载警告明细…' }}
                </div>
              </div>
              <div class="links">
                <span class="muted">报表：</span>
                <el-button v-for="r in row.reports" :key="r.report_id" size="small" link type="primary" @click="openReport(r)">
                  {{ REPORT_TYPE_LABELS[r.type as 'RISK_DAILY_HTML'] ?? r.type }}<template v-if="r.account_id"> · {{ r.account_id }}</template>
                </el-button>
                <span v-if="!row.reports.length" class="muted">该日暂无报表</span>
              </div>
              <div class="links">
                <span class="muted">文本结算单：</span>
                <el-button v-for="a in accounts.ids" :key="a" size="small" link @click="showStatement(row.td, a)">{{ a }}</el-button>
              </div>
              <template v-if="results[row.td] === 'loading'"><div class="muted">加载结算结果…</div></template>
              <template v-else-if="resultOf(row.td)">
                <el-table :data="resultOf(row.td)?.accounts ?? []" size="small">
                  <el-table-column label="账户" width="120">
                    <template #default="{ row: a }"><b>{{ a.account_id }}</b> <span class="muted">{{ ACCOUNT_NAMES[a.account_id] ?? '' }}</span></template>
                  </el-table-column>
                  <el-table-column label="上日结存" align="right" min-width="120"><template #default="{ row: a }">{{ fmtMoney(a.pre_balance) }}</template></el-table-column>
                  <el-table-column label="平仓盈亏" align="right" min-width="110"><template #default="{ row: a }"><span :class="signClass(a.close_pnl)">{{ fmtMoney(a.close_pnl) }}</span></template></el-table-column>
                  <el-table-column label="持仓盯市盈亏" align="right" min-width="120"><template #default="{ row: a }"><span :class="signClass(a.position_pnl)">{{ fmtMoney(a.position_pnl) }}</span></template></el-table-column>
                  <el-table-column label="手续费" align="right" min-width="90"><template #default="{ row: a }">{{ fmtMoney(a.fee) }}</template></el-table-column>
                  <el-table-column label="客户权益" align="right" min-width="120"><template #default="{ row: a }">{{ fmtMoney(a.equity) }}</template></el-table-column>
                  <el-table-column label="保证金" align="right" min-width="110"><template #default="{ row: a }">{{ fmtMoney(a.margin) }}</template></el-table-column>
                  <el-table-column label="风险度" align="right" width="80"><template #default="{ row: a }">{{ fmtPct(a.risk_degree, 1) }}</template></el-table-column>
                  <el-table-column label="净值" align="right" width="80"><template #default="{ row: a }">{{ fmtNum(a.nav, 4) }}</template></el-table-column>
                  <el-table-column label="校验" width="80" align="center"><template #default="{ row: a }"><StatusPill :tone="a.checks_passed ? 'good' : 'critical'" :label="a.checks_passed ? '通过' : '差异'" /></template></el-table-column>
                </el-table>
                <div v-if="resultOf(row.td)?.checks?.length" class="checks">
                  <StatusPill v-for="c in resultOf(row.td)?.checks" :key="c.check + (c.account_id ?? '')" :tone="c.status === 'PASS' ? 'good' : c.status === 'WARN' ? 'warning' : 'critical'" :label="`${c.check}${c.account_id ? ' ' + c.account_id : ''}`" />
                </div>
              </template>
              <div v-else-if="typeof results[row.td] === 'string'" class="muted">{{ results[row.td] }}</div>
            </div>
          </template>
        </el-table-column>
        <el-table-column label="交易日" width="120">
          <template #default="{ row }"><b>{{ fmtTd(row.td) }}</b><span v-if="row.td === currentTd" class="muted"> 当前</span></template>
        </el-table-column>
        <el-table-column label="EOD 阶段" width="130">
          <template #default="{ row }">
            <StatusPill v-if="row.phase" :tone="phaseTone(row.phase)" :label="EOD_PHASE_LABELS[row.phase as EodPhase]" />
            <span v-else-if="row.unknown" class="muted" :title="`clearing-worker：${row.unknown}`">状态未知</span>
            <span v-else class="muted">{{ row.missing ? '未发起' : '—' }}</span>
          </template>
        </el-table-column>
        <el-table-column label="版本" width="64" align="right">
          <template #default="{ row }">{{ row.version ? 'v' + row.version : '—' }}</template>
        </el-table-column>
        <el-table-column label="警告" width="72" align="center">
          <template #default="{ row }">
            <el-tooltip v-if="row.warnings" content="EOD 警告（迟到成交、官方价差异、推送未完成、昨结算不一致）：展开查看" placement="top">
              <span class="warn-count">{{ row.warnings }}</span>
            </el-tooltip>
            <span v-else class="muted">—</span>
          </template>
        </el-table-column>
        <el-table-column label="下一交易日" width="110">
          <template #default="{ row }">{{ row.next ? fmtTd(row.next) : '—' }}</template>
        </el-table-column>
        <el-table-column label="阶段时间" width="170">
          <template #default="{ row }">{{ row.ts ? fmtDateTime(row.ts) : '—' }}</template>
        </el-table-column>
        <el-table-column label="报表" min-width="200">
          <template #default="{ row }">
            <el-button v-if="row.daily" size="small" type="primary" link @click.stop="openReport(row.daily)">风控日报</el-button>
            <span class="muted small">{{ row.reports.length }} 份</span>
          </template>
        </el-table-column>
        <el-table-column label="错误" min-width="160">
          <template #default="{ row }"><span v-if="row.error" class="err">{{ row.error }}</span></template>
        </el-table-column>
      </el-table>
      <div v-if="eod.dayList.length > 12" class="more">
        <el-button link type="primary" @click="showAllDays = !showAllDays">{{ showAllDays ? '只看最近 12 个交易日' : `显示全部 ${eod.dayList.length} 个交易日` }}</el-button>
      </div>
    </div>

    <div class="grid cols-2 section-gap">
      <div class="card">
        <div class="card-title">
          报表文件
          <span class="hint">GET /reports/{report_id} 返回文件本身（带 X-API-Key 下载）</span>
          <span class="spacer" />
          <el-select v-model="reportTypeFilter" size="small" clearable placeholder="全部类型" style="width: 150px">
            <el-option v-for="(label, t) in REPORT_TYPE_LABELS" :key="t" :value="t" :label="label" />
          </el-select>
        </div>
        <SourceNotice :error="eod.reportsError" subject="报表列表（report.list）" compact :retry="loadReports" />
        <el-table :data="reportList" size="small" max-height="420" :empty-text="eod.reportsError ? '报表列表暂不可用' : '暂无报表'">
          <el-table-column label="交易日" width="100">
            <template #default="{ row }">{{ row.trading_day ? fmtTd(row.trading_day) : '回测' }}</template>
          </el-table-column>
          <el-table-column label="类型" min-width="120">
            <template #default="{ row }">{{ REPORT_TYPE_LABELS[row.type as 'RISK_DAILY_HTML'] ?? row.type }}<span v-if="row.account_id" class="muted"> · {{ row.account_id }}</span></template>
          </el-table-column>
          <el-table-column label="大小" width="80" align="right">
            <template #default="{ row }">{{ fmtBytes(row.size_bytes) }}</template>
          </el-table-column>
          <el-table-column label="SHA-256" width="110">
            <template #default="{ row }"><span class="mono muted" :title="row.sha256">{{ String(row.sha256 ?? '').slice(0, 10) }}</span></template>
          </el-table-column>
          <el-table-column label="生成时间" width="150">
            <template #default="{ row }">{{ fmtDateTime(row.created_ns) }}</template>
          </el-table-column>
          <el-table-column label="" width="70">
            <template #default="{ row }"><el-button link size="small" type="primary" @click="openReport(row as ReportReady)">打开</el-button></template>
          </el-table-column>
        </el-table>
      </div>

      <div class="card">
        <div class="card-title">
          回测
          <span class="hint">同一 EngineCore 回放历史数据，对照有 / 无风控（01 §8.16）</span>
          <span class="spacer" />
          <el-tooltip :disabled="auth.canAdmin" :content="needRoleText('ADMIN')" placement="bottom">
            <span><el-button size="small" :disabled="!auth.canAdmin" @click="btDlg.open = true">提交回测</el-button></span>
          </el-tooltip>
        </div>
        <SourceNotice :error="eod.backtestsError" subject="回测列表（backtest.list）" compact :retry="loadBacktests" />
        <div v-if="!eod.backtestList.length && !eod.backtestsError" class="empty">暂无回测作业</div>
        <div v-for="b in eod.backtestList" :key="b.run_id" class="bt">
          <div class="bt-head">
            <b>{{ b.name }}</b>
            <span class="mono muted">{{ b.run_id }}</span>
            <StatusPill :tone="btTone(b.state)" :label="BACKTEST_STATE_LABELS[b.state]" />
            <span class="spacer" />
            <el-button v-if="b.report_id" size="small" link type="primary" @click="openBacktestReport(b)">报告</el-button>
          </div>
          <el-progress v-if="b.state === 'RUNNING' || b.state === 'QUEUED'" :percentage="Math.round((b.progress ?? 0) * 100)" :stroke-width="6" />
          <div class="muted small">提交 {{ fmtDateTime(b.submitted_ns) }}<template v-if="b.finished_ns"> · 完成 {{ fmtDateTime(b.finished_ns) }}</template></div>
          <div v-if="b.error" class="err">{{ b.error }}</div>
          <table v-if="summaryTable(b.summary)" class="sum">
            <thead>
              <tr>
                <th>指标</th>
                <th v-for="g in summaryTable(b.summary)?.groups" :key="g">{{ GROUP_LABELS[g] ?? g }}</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="r in summaryTable(b.summary)?.rows" :key="r.key">
                <td>{{ METRIC_LABELS[r.key] ?? r.key }}</td>
                <td v-for="(v, i) in r.vals" :key="i" class="num">{{ metricText(r.key, v) }}</td>
              </tr>
            </tbody>
          </table>
          <JsonBlock v-else-if="b.summary" :value="b.summary" max-height="160px" />
        </div>
      </div>
    </div>

    <ReportViewer v-model="viewer.open" :report="viewer.report" />

    <ReasonDialog
      v-model="unfreezeDlg"
      :title="`EOD 强制放行（${fmtTd(unsettledTd)}）`"
      description="POST /risk/eod/{td}/force-unfreeze：扇出到所有风控分片，只对处于 PROVISIONAL 的分片生效（其余分片答复 PRECONDITION_FAILED），解除 EOD 未结算的开仓限制并写审计。需要 RISK_MANAGER 权限。"
      confirm-text="确认放行"
      danger
      :action="forceUnfreeze"
    />

    <el-dialog v-model="statement.open" :title="`结算单 · ${statement.account} · ${fmtTd(statement.td)}`" width="640px" append-to-body>
      <div v-if="statement.loading" class="muted">加载中…</div>
      <el-alert v-else-if="statement.error" :title="statement.error" type="error" show-icon :closable="false" />
      <template v-else-if="statement.result">
        <SourceNotice :message="`结算单文件暂不可用，以下为结算数据（${statement.message || 'settlement data'}）`" />
        <dl v-if="statementRow" class="kv">
          <dt>上日结存</dt>
          <dd>{{ fmtMoney(statementRow.pre_balance) }}</dd>
          <dt>入金 / 出金</dt>
          <dd>{{ fmtMoney(statementRow.deposit) }} / {{ fmtMoney(statementRow.withdraw) }}</dd>
          <dt>平仓盈亏</dt>
          <dd :class="signClass(statementRow.close_pnl)">{{ fmtMoney(statementRow.close_pnl) }}</dd>
          <dt>持仓盯市盈亏</dt>
          <dd :class="signClass(statementRow.position_pnl)">{{ fmtMoney(statementRow.position_pnl) }}</dd>
          <dt>手续费</dt>
          <dd>{{ fmtMoney(statementRow.fee) }}</dd>
          <dt>客户权益</dt>
          <dd><b>{{ fmtMoney(statementRow.equity) }}</b></dd>
          <dt>保证金 / 风险度</dt>
          <dd>{{ fmtMoney(statementRow.margin) }} / {{ fmtPct(statementRow.risk_degree, 1) }}</dd>
          <dt>单位净值</dt>
          <dd>{{ fmtNum(statementRow.nav, 4) }}</dd>
        </dl>
        <JsonBlock :value="statementRow ?? statement.result" max-height="240px" />
      </template>
      <pre v-else class="stmt">{{ statement.text }}</pre>
    </el-dialog>

    <el-dialog v-model="runDlg.open" title="发起结算（POST /settlement/run）" width="440px" append-to-body>
      <el-form label-width="110px">
        <el-form-item label="交易日">
          <el-input v-model="runDlg.td" placeholder="YYYYMMDD" maxlength="8" />
        </el-form-item>
        <el-form-item label="生成新版本">
          <el-checkbox v-model="runDlg.force">force_new_version（已结算的交易日重算）</el-checkbox>
        </el-form-item>
      </el-form>
      <div class="muted small">需要 ADMIN 权限；阶段 BEGIN → FROZEN → TRADES_RECONCILED → PRICES_FIXED → SETTLED → ROLLED → REPORTED → DONE 实时显示在上方。</div>
      <template #footer>
        <el-button @click="runDlg.open = false">取消</el-button>
        <el-button type="primary" :loading="runDlg.loading" :disabled="!/^\d{8}$/.test(runDlg.td)" @click="submitRun">发起</el-button>
      </template>
    </el-dialog>

    <el-dialog v-model="btDlg.open" title="提交回测（POST /backtests）" width="480px" append-to-body>
      <el-form label-position="top">
        <el-form-item label="回测配置（config/backtest/*.yaml）">
          <el-select v-model="btDlg.name" filterable allow-create default-first-option style="width: 100%">
            <el-option v-for="c in BACKTEST_CONFIGS" :key="c" :label="c" :value="c" />
          </el-select>
        </el-form-item>
        <el-form-item label="参数覆盖（JSON，可空）">
          <el-input v-model="btDlg.overrides" type="textarea" :rows="3" placeholder='{"rules.daily_loss.halt": -0.03}' />
        </el-form-item>
      </el-form>
      <el-alert v-if="btDlg.error" :title="btDlg.error" type="error" show-icon :closable="false" />
      <template #footer>
        <el-button @click="btDlg.open = false">取消</el-button>
        <el-button type="primary" :loading="btDlg.loading" @click="submitBacktest">提交</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<style scoped>
.more {
  text-align: center;
  margin-top: 6px;
}
.expand {
  padding: 4px 16px 12px 48px;
}
.links {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 2px 8px;
  margin-bottom: 6px;
  font-size: 13px;
}
.warns {
  border: 1px solid #f0d9a8;
  background: #fffaf0;
  border-radius: var(--fr-radius);
  padding: 6px 10px;
  margin-bottom: 8px;
  font-size: 13px;
}
.warns-head {
  font-weight: 600;
  margin-bottom: 4px;
}
.warns-head .muted {
  font-weight: 400;
  font-size: 12px;
}
.warn {
  display: flex;
  flex-wrap: wrap;
  gap: 4px 8px;
  align-items: baseline;
  line-height: 1.6;
}
.warn-count {
  display: inline-block;
  min-width: 20px;
  padding: 0 6px;
  border-radius: 10px;
  background: #fdf1d8;
  color: #8a5a00;
  font-weight: 600;
  font-size: 12px;
}
.checks {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
  margin-top: 8px;
}
.failed,
.err {
  color: #b42323;
  font-size: 13px;
}
.failed {
  margin-top: 8px;
}
.small {
  font-size: 12px;
}
.spacer {
  flex: 1;
}
.bt {
  border: 1px solid var(--fr-line);
  border-radius: var(--fr-radius);
  padding: 8px 10px;
  margin-bottom: 8px;
}
.bt-head {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
  margin-bottom: 4px;
}
.sum {
  border-collapse: collapse;
  width: 100%;
  font-size: 12px;
  margin-top: 6px;
}
.sum th,
.sum td {
  border-bottom: 1px solid var(--fr-line);
  padding: 4px 6px;
  text-align: left;
}
.sum th {
  color: var(--fr-ink-2);
  font-weight: 600;
}
.sum td.num {
  text-align: right;
  font-variant-numeric: tabular-nums;
}
.stmt {
  font-family: ui-monospace, 'Cascadia Mono', Consolas, 'Microsoft YaHei Mono', monospace;
  font-size: 12px;
  background: #f7f7f5;
  border: 1px solid var(--fr-line);
  border-radius: 6px;
  padding: 10px 12px;
  white-space: pre;
  overflow: auto;
  max-height: 60vh;
}
</style>
