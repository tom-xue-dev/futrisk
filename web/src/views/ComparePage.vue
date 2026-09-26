<script setup lang="ts">
/**
 * 多账户对比: account × rule utilization heat map (drill-down), headroom ranking,
 * small multiples of intraday daily-P&L % on one y range, product × account signed
 * exposure grid with firm net column (WS `compare` / GET /risk/compare, else computed
 * from the account snapshots), EOD tab from GET /risk/compare/history?days=1..60
 * (`{days, missing, errors}`), cross-account positions from GET /positions.
 */
import { computed, ref, watch } from 'vue'
import { useRouter } from 'vue-router'
import ExposureGridTable from '@/components/ExposureGridTable.vue'
import PositionsExplorer from '@/components/PositionsExplorer.vue'
import SourceNotice from '@/components/SourceNotice.vue'
import StaleBadge from '@/components/StaleBadge.vue'
import StatusPill from '@/components/StatusPill.vue'
import UtilBar from '@/components/UtilBar.vue'
import NavChart from '@/components/charts/NavChart.vue'
import PnlMultiple from '@/components/charts/PnlMultiple.vue'
import UtilHeatmap from '@/components/charts/UtilHeatmap.vue'
import type { ThresholdLine } from '@/components/charts/types'
import { usePolling, useTopics } from '@/composables/realtime'
import { showError } from '@/composables/feedback'
import { useAccounts } from '@/stores/accounts'
import { HISTORY_DAY_OPTIONS, useCompare } from '@/stores/compare'
import { divergingColor, textOn } from '@/utils/compare'
import { fmtMoneyCompact, fmtNum, fmtPct, fmtTd, fmtTime, isNum, signClass } from '@/utils/format'
import { ACCOUNT_NAMES, ACCOUNT_STATE_TONE, ACCOUNT_STATE_LABELS, accountColor, levelLabel, ruleLabel, scopeLabel, type StatusTone } from '@/utils/labels'
import type { HeadroomItem } from '@/types'
import { actionTone, effectiveLevels, fmtMetric } from '@/utils/risk'
import { summarizeHistory } from '@/utils/stats'

const router = useRouter()
const accounts = useAccounts()
const compare = useCompare()

const tab = ref<'live' | 'positions' | 'eod'>('live')
const days = ref<number>(20)

useTopics(() => ['compare', ...accounts.ids.map((id) => `equity:${id}`)])

void compare.loadRules().catch((e: unknown) => showError(e, '加载风控规则失败'))
void compare.loadCompare().catch(() => undefined)
void accounts.loadAll().catch(() => undefined)
watch(
  () => accounts.ids.join(','),
  () => {
    for (const id of accounts.ids) if (!accounts.equity[id]?.length) void accounts.loadEquity(id).catch(() => undefined)
  },
  { immediate: true },
)
// client-side fallback needs fresh full snapshots
usePolling(() => (compare.usingFallback ? accounts.loadAll() : undefined), 3000, false)

const view = computed(() => compare.view)
/** stale accounts: the compare view's `accounts_stale` plus the account items' owner liveness (WS may be newer) */
const staleIds = computed(() => [...new Set([...(view.value?.accounts_stale ?? []), ...accounts.staleIds])].sort())

const ranking = computed(() => (view.value?.ranking ?? []).filter((r) => r.next_level).slice(0, 12))

function dailyLevels(id: string): ThresholdLine[] {
  const def = compare.ruleMap.daily_loss
  if (!def) return [
    { name: 'warn', threshold: -0.02, action: 'ALERT' },
    { name: 'restrict', threshold: -0.025, action: 'RESTRICT' },
    { name: 'halt', threshold: -0.03, action: 'CIRCUIT_BREAK' },
  ]
  return effectiveLevels(def, id).map((l) => ({ name: l.name, threshold: l.threshold, action: l.action }))
}

const multiples = computed(() =>
  accounts.list.map((a) => {
    const pts = accounts.equity[a.account_id] ?? []
    const last = pts[pts.length - 1]
    return { id: a.account_id, state: a.state, latched: a.halted_latched, points: pts, levels: dailyLevels(a.account_id), last: last?.daily_pnl_pct_init ?? a.daily_pnl_pct_init }
  }),
)

/** One y range for every panel (data plus each account's breaker line). */
const yRange = computed(() => {
  let lo = 0
  let hi = 0
  for (const m of multiples.value) {
    for (const p of m.points) {
      lo = Math.min(lo, p.daily_pnl_pct_init * 100)
      hi = Math.max(hi, p.daily_pnl_pct_init * 100)
    }
    for (const l of m.levels) if (l.action === 'CIRCUIT_BREAK') lo = Math.min(lo, l.threshold * 100)
  }
  return { min: Math.floor(lo - 0.5), max: Math.ceil(hi + 0.5) }
})

const xRange = computed(() => {
  let lo = Number.POSITIVE_INFINITY
  let hi = 0
  for (const m of multiples.value) {
    const f = m.points[0]
    const l = m.points[m.points.length - 1]
    if (f) lo = Math.min(lo, f.ts_ns / 1e6)
    if (l) hi = Math.max(hi, l.ts_ns / 1e6)
  }
  return Number.isFinite(lo) ? { min: lo, max: hi } : { min: undefined, max: undefined }
})

/** Tone of the next level's action (alert = warning, restrict = serious, breaker = critical). */
function nextTone(row: HeadroomItem): StatusTone {
  const action = compare.ruleMap[row.rule_id]?.levels.find((l) => l.name === row.next_level)?.action
  return actionTone(action)
}

function onRankRow(row: HeadroomItem): void {
  drill(row.account_id)
}

function drill(accountId: string): void {
  void router.push(`/accounts/${accountId}`)
}

// ---------------------------------------------------------------- EOD tab

function loadHistory(): void {
  void compare.loadHistory(days.value).catch((e: unknown) => showError(e, '加载结算历史失败'))
}

watch(
  () => [tab.value, days.value] as const,
  ([t]) => {
    if (t === 'eod') loadHistory()
  },
)

const stats = computed(() => (compare.history ? summarizeHistory(compare.history, { dayBreakers: compare.dayBreakers, gaps: compare.breakerGaps }) : []))
const historyErrors = computed(() => compare.history?.errors ?? {})
const historyMissing = computed(() => compare.history?.missing ?? [])

const returnGrid = computed(() => {
  const tds = [...new Set(stats.value.flatMap((s) => s.returns.map((r) => r.td)))].sort().reverse().slice(0, 20)
  const maxAbs = stats.value.reduce((m, s) => Math.max(m, ...s.returns.map((r) => Math.abs(r.r))), 0)
  return { tds, maxAbs, map: new Map(stats.value.flatMap((s) => s.returns.map((r) => [`${s.account_id}|${r.td}`, r.r] as const))) }
})

function retStyle(v: number | undefined): Record<string, string> {
  if (v === undefined) return {}
  const bg = divergingColor(v, returnGrid.value.maxAbs || 0.01)
  return { background: bg, color: textOn(bg) }
}
</script>

<template>
  <div class="page">
    <div class="page-head">
      <h1>多账户对比</h1>
      <el-radio-group v-model="tab">
        <el-radio-button value="live">实时对比</el-radio-button>
        <el-radio-button value="positions">持仓汇总</el-radio-button>
        <el-radio-button value="eod">盘后（结算历史）</el-radio-button>
      </el-radio-group>
      <span class="spacer" />
      <span v-if="tab === 'live'" class="muted">
        {{ compare.usingFallback ? '由各账户快照在浏览器端计算（未收到 compare 主题）' : '来自 api-gateway 读模型（compare 主题）' }}
        <template v-if="view?.as_of_ns"> · {{ fmtTime(view.as_of_ns) }}</template>
      </span>
    </div>

    <div v-if="tab === 'positions'" class="card">
      <div class="card-title">跨账户持仓 <span class="hint">GET /positions：来自网关读模型中各账户最新快照；可按账户 / 品种 / 合约 / 交易所 / 板块分组与筛选</span></div>
      <PositionsExplorer :accounts="accounts.ids" :active="tab === 'positions'" />
    </div>

    <template v-else-if="tab === 'live'">
      <SourceNotice v-if="compare.rulesMessage" compact :message="`风控规则：${compare.rulesMessage}`" />
      <SourceNotice v-if="accounts.withoutData.length" compact :missing="accounts.withoutData" missing-label="尚无快照的账户（风控分片未连接或未发布，未参与对比）" />
      <SourceNotice v-if="staleIds.length" compact :missing="staleIds" missing-label="风控分片离线、数据陈旧的账户（按最后一次快照参与对比）" />
      <div class="card">
        <div class="card-title">账户 × 规则使用率 <span class="hint">点击单元格进入该账户</span></div>
        <UtilHeatmap v-if="view && view.matrix.length" :view="view" @drill="drill" />
        <div v-else class="empty">暂无对比数据</div>
      </div>

      <div class="grid cols-2 section-gap">
        <div class="card">
          <div class="card-title">谁最先触发 <span class="hint">按距下一级别的接近程度排序（1 = 已到线）</span></div>
          <el-table :data="ranking" size="small" empty-text="暂无" @row-click="onRankRow">
            <el-table-column label="账户" width="80">
              <template #default="{ row }"><b>{{ row.account_id }}</b></template>
            </el-table-column>
            <el-table-column label="规则 · 范围" min-width="140">
              <template #default="{ row }">{{ ruleLabel(row.rule_id) }}<span v-if="row.scope_key.includes('/')" class="muted"> · {{ scopeLabel(row.scope_key) }}</span></template>
            </el-table-column>
            <el-table-column label="当前值" width="92" align="right">
              <template #default="{ row }">{{ fmtMetric(row.metric, row.value) }}</template>
            </el-table-column>
            <el-table-column label="下一级" width="140">
              <template #default="{ row }">
                <StatusPill :tone="nextTone(row as HeadroomItem)" :label="`${levelLabel(row.next_level)} ${fmtMetric(row.metric, row.next_threshold)}`" />
              </template>
            </el-table-column>
            <el-table-column label="接近程度" min-width="150">
              <template #default="{ row }"><UtilBar :value="row.utilization_next" :max="1.1" /></template>
            </el-table-column>
            <el-table-column label="余量" width="110" align="right">
              <template #default="{ row }">
                <span v-if="isNum(row.headroom_money)">{{ fmtMoneyCompact(row.headroom_money) }}</span>
                <span v-else-if="isNum(row.headroom)" class="muted">{{ fmtMetric(row.metric, row.headroom) }}</span>
                <span v-else class="muted">—</span>
              </template>
            </el-table-column>
          </el-table>
        </div>

        <div class="card">
          <div class="card-title">当日盈亏 % 小多图 <span class="hint">同一纵轴；虚线 = 各账户告警 / 限制 / 熔断线（含按账户覆盖）</span></div>
          <div class="multiples">
            <div v-for="m in multiples" :key="m.id" class="mini" @click="drill(m.id)">
              <div class="mini-head">
                <span class="swatch" :style="{ background: accountColor(m.id) }" />
                <b>{{ m.id }}</b>
                <span class="muted name">{{ ACCOUNT_NAMES[m.id] ?? '' }}</span>
                <StaleBadge v-if="accounts.staleInfo(m.id).stale" :age-ms="accounts.staleInfo(m.id).age_ms" compact />
                <span class="spacer" />
                <StatusPill :tone="ACCOUNT_STATE_TONE[m.state]" :label="ACCOUNT_STATE_LABELS[m.state]" />
                <span class="val" :class="signClass(m.last)">{{ fmtPct(m.last, 2, true) }}</span>
              </div>
              <PnlMultiple :points="m.points" :color="accountColor(m.id)" :y-min="yRange.min" :y-max="yRange.max" :x-min="xRange.min" :x-max="xRange.max" :levels="m.levels" />
            </div>
          </div>
        </div>
      </div>

      <div class="card section-gap">
        <div class="card-title">品种 × 账户带符号敞口 <span class="hint">净名义（元），公司净额 = 各账户之和</span></div>
        <ExposureGridTable v-if="view && view.exposure.products.length" :grid="view.exposure" />
        <div v-else class="empty">暂无敞口</div>
      </div>
    </template>

    <template v-else>
      <div class="card">
        <div class="card-title">
          结算历史统计
          <span class="hint">日收益 = 当日盈亏 ÷ 静态权益（出入金不计入）；年化按 252 天，无风险利率 0；最大回撤按单位净值路径</span>
          <span class="spacer" />
          <el-select v-model="days" size="small" style="width: 120px">
            <el-option v-for="d in HISTORY_DAY_OPTIONS" :key="d" :value="d" :label="`最近 ${d} 天`" />
          </el-select>
        </div>
        <SourceNotice v-if="compare.historyError" :error="compare.historyError" subject="结算历史" :retry="loadHistory">
          <span class="muted">（结算结果由 clearing-worker 提供）</span>
        </SourceNotice>
        <SourceNotice v-else :errors="historyErrors" subject="结算历史" errors-label="以下交易日的结算结果未取到" compact />
        <SourceNotice v-if="historyMissing.length" compact :message="`${historyMissing.length} 个交易日尚无结算结果（未结算或早于系统启用）：${historyMissing.slice(0, 8).join('，')}${historyMissing.length > 8 ? ' …' : ''}`" />
        <SourceNotice
          v-if="compare.breakerGaps.length"
          compact
          :message="`熔断次数不完整：${compare.breakerGaps.length} 个交易日有风控分片未应答（${compare.breakerGaps.slice(0, 6).join('，')}${compare.breakerGaps.length > 6 ? ' …' : ''}），带 * 的次数可能偏少`"
        />
        <el-table v-loading="compare.historyLoading" :data="stats" size="small" :empty-text="compare.historyError ? '结算历史暂不可用' : '暂无结算历史（GET /risk/compare/history）'">
          <el-table-column label="账户" min-width="140">
            <template #default="{ row }"><span class="swatch" :style="{ background: accountColor(row.account_id) }" /> <b>{{ row.account_id }}</b> <span class="muted">{{ ACCOUNT_NAMES[row.account_id] ?? '' }}</span></template>
          </el-table-column>
          <el-table-column label="天数" prop="days" width="60" align="right" />
          <el-table-column label="最新日收益" width="110" align="right">
            <template #default="{ row }"><span :class="signClass(row.latest_return)">{{ fmtPct(row.latest_return, 2, true) }}</span></template>
          </el-table-column>
          <el-table-column label="平均日收益" width="110" align="right">
            <template #default="{ row }"><span :class="signClass(row.mean_return)">{{ fmtPct(row.mean_return, 3, true) }}</span></template>
          </el-table-column>
          <el-table-column label="年化波动率" width="110" align="right">
            <template #default="{ row }">{{ fmtPct(row.vol_annual, 1) }}</template>
          </el-table-column>
          <el-table-column label="夏普比率" width="96" align="right">
            <template #default="{ row }">{{ fmtNum(row.sharpe, 2) }}</template>
          </el-table-column>
          <el-table-column label="最大回撤" width="96" align="right">
            <template #default="{ row }">{{ fmtPct(row.max_drawdown, 2) }}</template>
          </el-table-column>
          <el-table-column label="区间收益" width="100" align="right">
            <template #default="{ row }"><span :class="signClass(row.cum_return)">{{ fmtPct(row.cum_return, 2, true) }}</span></template>
          </el-table-column>
          <el-table-column label="熔断次数" width="90" align="right">
            <template #default="{ row }">
              <span :title="row.breakers_partial ? '部分交易日的熔断次数未知（风控分片未应答），实际可能更多' : ''">
                <b v-if="row.breakers" class="down-strong">{{ row.breakers }}</b><span v-else-if="row.breakers === 0">0</span><span v-else class="muted">—</span><sup v-if="row.breakers_partial && row.breakers !== null" class="muted">*</sup>
              </span>
            </template>
          </el-table-column>
          <el-table-column label="最新净值" width="96" align="right">
            <template #default="{ row }">{{ fmtNum(row.latest_nav, 4) }}</template>
          </el-table-column>
        </el-table>
      </div>

      <div class="grid cols-2 section-gap">
        <div class="card">
          <div class="card-title">单位净值 <span class="hint">业绩权益 ÷ 初始资金</span></div>
          <NavChart v-if="stats.length" :stats="stats" />
          <div v-else class="empty">暂无数据</div>
        </div>
        <div class="card">
          <div class="card-title">逐日收益 <span class="hint">最近 20 个交易日；红 = 盈利，蓝 = 亏损</span></div>
          <div class="ret-wrap">
            <table class="ret">
              <thead>
                <tr>
                  <th>交易日</th>
                  <th v-for="s in stats" :key="s.account_id">{{ s.account_id }}</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="td in returnGrid.tds" :key="td">
                  <td class="td">{{ fmtTd(td) }}</td>
                  <td v-for="s in stats" :key="s.account_id" class="num" :style="retStyle(returnGrid.map.get(`${s.account_id}|${td}`))">{{ fmtPct(returnGrid.map.get(`${s.account_id}|${td}`), 2, true) }}</td>
                </tr>
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </template>
  </div>
</template>

<style scoped>
.multiples {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(260px, 1fr));
  gap: 10px;
}
.mini {
  border: 1px solid var(--fr-line);
  border-radius: var(--fr-radius);
  padding: 6px 8px 0;
  cursor: pointer;
}
.mini:hover {
  border-color: #b8c7de;
}
.mini-head {
  display: flex;
  align-items: center;
  gap: 6px;
  font-size: 13px;
}
.mini-head .name {
  font-size: 12px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
.mini-head .val {
  font-variant-numeric: tabular-nums;
  font-weight: 600;
}
.swatch {
  display: inline-block;
  width: 4px;
  height: 14px;
  border-radius: 2px;
  vertical-align: -2px;
}
.spacer {
  flex: 1;
}
.down-strong {
  color: #b42323;
}
.ret-wrap {
  max-height: 420px;
  overflow: auto;
}
.ret {
  border-collapse: separate;
  border-spacing: 2px;
  width: 100%;
  font-size: 12px;
}
.ret th {
  position: sticky;
  top: 0;
  background: #fff;
  color: var(--fr-ink-2);
  padding: 4px 6px;
  text-align: right;
}
.ret th:first-child,
.ret td.td {
  text-align: left;
  white-space: nowrap;
}
.ret td {
  padding: 4px 6px;
  border-radius: 3px;
}
.ret td.num {
  text-align: right;
  font-variant-numeric: tabular-nums;
}
</style>
