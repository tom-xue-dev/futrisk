<script setup lang="ts">
/**
 * 账户: state badge (HALTED latched / pending), reset + manual halt (reason dialog,
 * Idempotency-Key), intraday equity / P&L / drawdown with event markers, rule gauges,
 * positions, exposures, orders with reject codes, trades, liquidation plans, events.
 *
 * Sources: GET /accounts/{id} (full snapshot, polled 2s: funds / metrics / exposures /
 * rules / restrictions), WS positions:{id}, orders:{id}, equity:{id}, events;
 * GET /accounts/{id}/orders|trades|equity-curve, GET /risk/events?account=.
 */
import { computed, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import EventStream from '@/components/EventStream.vue'
import LiquidationPlans from '@/components/LiquidationPlans.vue'
import OrdersTable from '@/components/OrdersTable.vue'
import PositionsTable from '@/components/PositionsTable.vue'
import ReasonDialog from '@/components/ReasonDialog.vue'
import SourceNotice from '@/components/SourceNotice.vue'
import StaleBadge from '@/components/StaleBadge.vue'
import StateBadge from '@/components/StateBadge.vue'
import StatTile from '@/components/StatTile.vue'
import StatusPill from '@/components/StatusPill.vue'
import UtilBar from '@/components/UtilBar.vue'
import IntradayChart from '@/components/charts/IntradayChart.vue'
import RuleGauge from '@/components/charts/RuleGauge.vue'
import type { ThresholdLine } from '@/components/charts/types'
import { usePolling, useTopics } from '@/composables/realtime'
import { showInfo, showOk } from '@/composables/feedback'
import { rt } from '@/runtime'
import { useAccounts } from '@/stores/accounts'
import { needRoleText, useAuth } from '@/stores/auth'
import { useCompare } from '@/stores/compare'
import { useEvents } from '@/stores/events'
import { toLoadError, type LoadError } from '@/stores/system'
import type { RuleDef, RuleStatus } from '@/types'
import { fmtDateTime, fmtDuration, fmtMoney, fmtMoneyCompact, fmtNum, fmtPct, fmtPrice, fmtSignedMoney, fmtTime, fmtTimes, isNum, signClass } from '@/utils/format'
import {
  ACCOUNT_NAMES,
  ACCOUNT_STATE_TONE,
  DIRECTION_LABELS,
  EXCHANGE_LABELS,
  MD_QUALITY_LABELS,
  MD_QUALITY_TONE,
  OFFSET_LABELS,
  PRODUCT_LABELS,
  RESTRICTION_LABELS,
  SECTOR_LABELS,
  TRADING_MODE_LABELS,
  accountColor,
  levelLabel,
  ruleLabel,
  scopeLabel,
} from '@/utils/labels'
import { effectiveLevels, fmtMetric, hardUtilization, levelMarks, worstByRule } from '@/utils/risk'

const route = useRoute()
const router = useRouter()
const accounts = useAccounts()
const events = useEvents()
const compare = useCompare()
const auth = useAuth()

const id = computed(() => (typeof route.params.id === 'string' && route.params.id ? route.params.id : ''))
const tab = ref('positions')
const windowMin = ref(0)

watch(
  () => [id.value, accounts.ids.length] as const,
  ([v]) => {
    if (!v) void router.replace(`/accounts/${accounts.ids[0] ?? 'ACC1'}`)
  },
  { immediate: true },
)

useTopics(() => (id.value ? [`positions:${id.value}`, `orders:${id.value}`, `equity:${id.value}`] : []))

/** Per-section load errors: most account reads go to the owning risk shard (503 UNAVAILABLE while it is down). */
const errs = ref<{ snapshot: LoadError | null; orders: LoadError | null; equity: LoadError | null; trades: LoadError | null; plans: LoadError | null }>({
  snapshot: null,
  orders: null,
  equity: null,
  trades: null,
  plans: null,
})

async function track(key: keyof typeof errs.value, p: Promise<unknown>): Promise<void> {
  try {
    await p
    errs.value[key] = null
  } catch (e) {
    errs.value[key] = toLoadError(e)
  }
}

function loadAll(v: string): void {
  void track('snapshot', accounts.loadAccount(v))
  void track('orders', accounts.loadOrders(v))
  void track('equity', accounts.loadEquity(v))
  void track('trades', accounts.loadTrades(v))
  void track('plans', events.loadPlans(v))
  void events.load(undefined, v).catch(() => undefined)
}

watch(
  id,
  (v) => {
    if (!v) return
    errs.value = { snapshot: null, orders: null, equity: null, trades: null, plans: null }
    loadAll(v)
  },
  { immediate: true },
)
usePolling(() => (id.value ? track('snapshot', accounts.loadAccount(id.value)) : undefined), 2000, false)
usePolling(() => (id.value && tab.value === 'trades' ? track('trades', accounts.loadTrades(id.value)) : undefined), 5000, false)
// plans also change through LIQUIDATION_PLAN events; the listing brings those of a shard that was down (`stale`)
usePolling(() => (id.value && (tab.value === 'plans' || errs.value.plans || Object.keys(events.planErrors).length) ? track('plans', events.loadPlans(id.value)) : undefined), 10_000, false)
// a section that failed is retried every 10 s (e.g. until the risk shard is back)
usePolling(
  () => {
    const v = id.value
    if (!v) return undefined
    if (errs.value.orders) void track('orders', accounts.loadOrders(v))
    if (errs.value.equity) void track('equity', accounts.loadEquity(v))
    return undefined
  },
  10_000,
  false,
)

/** NOT_FOUND for an account that is not configured (a configured one without a snapshot is `noSnapshotYet`) */
const unknownAccount = computed(
  () => errs.value.snapshot?.code === 'NOT_FOUND' && !accounts.snapshots[id.value] && !(accounts.configured.length > 0 && accounts.configured.includes(id.value)),
)
/** configured, but its shard has not published a snapshot (GET /accounts `missing: true`) */
const noSnapshotYet = computed(() => !accounts.snapshots[id.value] && accounts.missing[id.value] !== undefined)
const noSnapshotText = computed(() => {
  const m = accounts.missing[id.value]
  if (!m) return ''
  const shard = `risk-engine-${m.shard}`
  return m.stale
    ? `账户已配置（${m.name}），所属风控分片 ${shard} 离线且尚未发布过快照：数值暂不可用，分片恢复后自动刷新。`
    : `账户已配置（${m.name}），所属风控分片 ${shard} 尚未发布快照${m.state ? '（当前状态已知）' : ''}：数值暂不可用。`
})
/** a section failing for the same reason as the snapshot (the owning shard is down) is covered by the head notice */
function ownNotice(e: LoadError | null): boolean {
  const s = errs.value.snapshot
  if (!e || unknownAccount.value) return false
  return !(s && !accounts.snapshots[id.value] && e.code === s.code && e.message === s.message)
}
const selectorIds = computed(() => (accounts.ids.includes(id.value) || !id.value || unknownAccount.value ? accounts.ids : [...accounts.ids, id.value].sort()))
function stateTone(aid: string): string {
  const st = accounts.list.find((a) => a.account_id === aid)?.state
  const tone = st ? ACCOUNT_STATE_TONE[st] : 'none'
  return accounts.staleInfo(aid).stale ? `${tone} stale` : tone
}
const staleNow = computed(() => accounts.staleInfo(id.value))

const snap = computed(() => accounts.snapshots[id.value] ?? null)
const summary = computed(() => accounts.summaries[id.value] ?? null)
const eff = computed(() => accounts.effective(id.value))
const positions = computed(() => accounts.positions[id.value] ?? snap.value?.positions ?? [])
const orders = computed(() => accounts.orderList(id.value))
const trades = computed(() => accounts.trades[id.value] ?? [])
const points = computed(() => accounts.equity[id.value] ?? [])
const acctEvents = computed(() => events.riskEvents.filter((e) => e.account_id === id.value))
const plans = computed(() => events.plansFor(id.value))
const pendingPlans = computed(() => plans.value.filter((p) => p.status === 'PENDING_CONFIRM').length)
const color = computed(() => accountColor(id.value || 'ACC1'))

const funds = computed(() => snap.value?.funds ?? null)
const metrics = computed(() => snap.value?.metrics ?? null)
const restrictions = computed(() => eff.value?.restrictions ?? snap.value?.restrictions ?? [])

function levels(def: RuleDef | undefined, fallback: ThresholdLine[]): ThresholdLine[] {
  if (!def) return fallback
  return effectiveLevels(def, id.value).map((l) => ({ name: l.name, threshold: l.threshold, action: l.action }))
}

const pnlLevels = computed(() =>
  levels(compare.ruleMap.daily_loss, [
    { name: 'warn', threshold: -0.02, action: 'ALERT' },
    { name: 'restrict', threshold: -0.025, action: 'RESTRICT' },
    { name: 'halt', threshold: -0.03, action: 'CIRCUIT_BREAK' },
    { name: 'derisk', threshold: -0.05, action: 'LIQUIDATE' },
  ]),
)
const ddLevels = computed(() =>
  levels(compare.ruleMap.intraday_dd, [
    { name: 'warn', threshold: 0.025, action: 'ALERT' },
    { name: 'block', threshold: 0.04, action: 'RESTRICT' },
  ]),
)

interface GaugeModel {
  ruleId: string
  title: string
  scope: string
  valueText: string
  util: number | null
  marks: ReturnType<typeof levelMarks>
  activeLevel: string | null
  smState: RuleStatus['sm_state']
  nextText: string
  headroomText: string
}

const gauges = computed<GaugeModel[]>(() => {
  const rules = snap.value?.rules ?? []
  const defs = compare.rules ?? []
  const ruleIds = defs.length ? defs.map((d) => d.id) : [...new Set(rules.map((r) => r.rule_id))]
  const worst = worstByRule(rules, (r) => {
    const def = compare.ruleMap[r.rule_id]
    return def ? hardUtilization(r.value, def, id.value) : (r.utilization ?? null)
  })
  return ruleIds.map((rid) => {
    const def = compare.ruleMap[rid]
    const rs = worst.get(rid)
    const metric = rs?.metric ?? def?.metric ?? ''
    const util = rs ? (def ? hardUtilization(rs.value, def, id.value) : (rs.utilization ?? null)) : null
    const nextText = rs?.next_level ? `下一级：${levelLabel(rs.next_level)} ${fmtMetric(metric, rs.next_threshold)}` : rs && isNum(rs.value) ? '已越过全部级别' : rs ? '行情质量非 OK，指标暂停' : '无数据'
    let headroomText = ''
    if (rs && isNum(rs.headroom_money)) headroomText = `距下一级还剩 ${fmtMoneyCompact(rs.headroom_money)} 元`
    else if (rs && isNum(rs.value) && isNum(rs.next_threshold)) headroomText = `距下一级 ${fmtMetric(metric, Math.abs(rs.next_threshold - rs.value))}`
    return {
      ruleId: rid,
      title: ruleLabel(rid),
      scope: rs && rs.scope_key.includes('/') ? scopeLabel(rs.scope_key) : '',
      valueText: rs ? fmtMetric(metric, rs.value) : '—',
      util,
      marks: def ? levelMarks(def, id.value) : [],
      activeLevel: rs?.active_level ?? null,
      smState: rs?.sm_state ?? 'OK',
      nextText,
      headroomText,
    }
  })
})

// ---------------------------------------------------------------- actions

const resetOpen = ref(false)
const haltOpen = ref(false)
const resetRule = ref('')

const latchedRules = computed(() => (snap.value?.rules ?? []).filter((r) => r.active_level && compare.ruleMap[r.rule_id]?.levels.find((l) => l.name === r.active_level)?.latch))
const canReset = computed(() => eff.value?.state === 'HALTED' && eff.value.halted_latched)
const canHalt = computed(() => !(eff.value?.state === 'HALTED' && eff.value.halted_latched))

async function doReset(reason: string, key: string): Promise<void> {
  const body = resetRule.value ? { reason, rule_id: resetRule.value.split('/')[0], level: resetRule.value.split('/')[1] } : { reason }
  const res = await rt().api.resetAccount(id.value, body, key)
  const ack = res.data
  // the shard answers OK with "N latched level(s) reset"; N = 0 when nothing was latched
  const n = /^(\d+) latched/.exec(res.message)?.[1]
  const dup = res.duplicate ? '（幂等重放：返回首次请求的结果）' : ''
  if (n === '0') {
    showInfo(`${id.value} 没有已锁存的级别，无需重置（当前状态 ${ack?.account_state ?? '—'}）${dup}`)
  } else {
    const rearm = ack && isNum(ack.rearm_threshold) ? `，重新布防线 ${fmtPct(ack.rearm_threshold)}` : ''
    showOk(`${id.value} 已重置 ${n ?? ''} 个锁存级别：当前状态 ${ack?.account_state ?? '—'}${rearm}${dup}`)
  }
  void accounts.loadAccount(id.value).catch(() => undefined)
}

async function doHalt(reason: string, key: string): Promise<void> {
  const res = await rt().api.haltAccount(id.value, { reason }, key)
  const chg = res.data
  if (chg && typeof chg === 'object' && 'state_version' in chg) accounts.applyStateChange(chg)
  showOk(`${id.value} 已人工熔断（锁存），开仓挂单已撤销${res.duplicate ? '（幂等重放）' : ''}`)
  void accounts.loadAccount(id.value).catch(() => undefined)
}

function selectAccount(v: string | number | boolean | undefined): void {
  if (typeof v === 'string') void router.push(`/accounts/${v}`)
}
</script>

<template>
  <div class="page">
    <div class="page-head">
      <el-radio-group :model-value="id" size="default" @update:model-value="selectAccount">
        <el-radio-button v-for="aid in selectorIds" :key="aid" :value="aid">
          <span class="dot" :class="stateTone(aid)" />{{ aid }}
        </el-radio-button>
      </el-radio-group>
    </div>

    <div class="card head-card">
      <div class="title-row">
        <span class="swatch" :style="{ background: color }" />
        <h1>{{ id }} <span class="name">{{ ACCOUNT_NAMES[id] ?? '' }}</span></h1>
        <StateBadge :state="eff?.state ?? summary?.state" :latched="eff?.halted_latched ?? false" size="lg" />
        <StatusPill v-if="snap?.trading_mode && snap.trading_mode !== 'OPEN'" tone="warning" :label="TRADING_MODE_LABELS[snap.trading_mode]" />
        <StatusPill v-if="snap" :tone="MD_QUALITY_TONE[snap.quality]" :label="`行情 ${MD_QUALITY_LABELS[snap.quality]}`" />
        <StaleBadge v-if="staleNow.stale" :age-ms="staleNow.age_ms" />
        <span class="muted">分片 {{ snap?.shard ?? summary?.shard ?? '—' }} · state_version {{ eff?.state_version ?? '—' }} · 更新 {{ snap ? fmtTime(snap.as_of_ns) : '—' }}</span>
        <span class="spacer" />
        <el-tooltip :content="!auth.canRisk ? needRoleText('RISK') : canReset ? '解除锁存：状态机转为 REARMED，重新布防线 = min(阈值, 当前值 − extra)' : '账户没有已锁存的熔断'" placement="bottom">
          <span><el-button type="primary" :disabled="!canReset || !auth.canRisk" @click="resetOpen = true">重置熔断</el-button></span>
        </el-tooltip>
        <el-tooltip :content="!auth.canRisk ? needRoleText('RISK') : canHalt ? '人工熔断：直接锁存，撤销开仓挂单，只允许减仓' : '账户已处于熔断锁存状态'" placement="bottom">
          <span><el-button type="danger" plain :disabled="!canHalt || !auth.canRisk" @click="haltOpen = true">人工熔断</el-button></span>
        </el-tooltip>
      </div>
      <SourceNotice v-if="staleNow.stale" :message="`账户所属的风控分片 risk-engine-${snap?.shard ?? summary?.shard ?? '?'} 离线：以下为最后一次快照${staleNow.age_ms ? '（' + fmtDuration(staleNow.age_ms) + ' 前）' : ''}，数值与状态不再更新；分片恢复后自动刷新。`" />
      <SourceNotice v-if="unknownAccount" :error="errs.snapshot" subject="账户" />
      <SourceNotice v-else-if="noSnapshotYet" :message="noSnapshotText" :retry="() => loadAll(id)" />
      <SourceNotice v-else-if="errs.snapshot && !snap" :error="errs.snapshot" subject="账户快照" :retry="() => loadAll(id)">
        <span class="muted">（账户数据由所属风控分片提供；分片恢复后自动刷新）</span>
      </SourceNotice>
      <SourceNotice v-else-if="errs.snapshot && snap" compact :message="`快照刷新失败（${errs.snapshot.code}），显示的是 ${fmtTime(snap.as_of_ns)} 的快照`" />
      <div v-if="restrictions.length" class="restr">
        <span class="muted">生效限制：</span>
        <el-tag v-for="(r, i) in restrictions" :key="i" size="small" :type="r.kind === 'BLOCK_OPEN' ? 'danger' : 'warning'" effect="plain">
          {{ RESTRICTION_LABELS[r.kind] ?? r.kind }}<template v-if="r.value != null"> ×{{ r.value }}</template> · {{ scopeLabel(r.scope_key) }} · {{ r.source }}
        </el-tag>
      </div>
      <div v-if="eff?.state === 'HALTED'" class="halt-note" :class="{ latched: eff.halted_latched }">
        {{ eff.halted_latched ? '熔断已锁存：新开仓一律拒绝（REJECT_ACCOUNT_HALTED），平仓照常；只能由风控经理重置或换日解除。' : '已越过熔断线、等待确认：此刻起开仓已被拒绝，挂着的开仓单已撤销；确认前回到阈值内则自动撤销熔断。' }}
      </div>
    </div>

    <div v-if="funds && metrics" class="grid kpis section-gap">
      <StatTile label="动态权益" :value="fmtMoneyCompact(funds.equity)" :sub="`静态 ${fmtMoneyCompact(funds.static_equity)} · 初始 ${fmtMoneyCompact(funds.initial_capital)}`" />
      <StatTile label="当日盈亏" :value="fmtMoneyCompact(metrics.daily_pnl, 2, true)" :value-class="signClass(metrics.daily_pnl)" :sub="`平仓 ${fmtMoneyCompact(funds.close_pnl, 1, true)} · 持仓 ${fmtMoneyCompact(funds.position_pnl, 1, true)} · 费 ${fmtMoneyCompact(funds.fee, 1)}`" />
      <StatTile label="当日盈亏 / 初始资金" :value="fmtPct(metrics.daily_pnl_pct_init, 2, true)" :value-class="signClass(metrics.daily_pnl_pct_init)" :sub="`/ 静态权益 ${fmtPct(metrics.daily_pnl_pct_sod, 2, true)} · 纯浮亏 ${fmtPct(metrics.floating_pnl_pct_init, 2, true)}`" />
      <StatTile label="保证金 / 风险度" :value="fmtPct(funds.risk_degree, 1)" :sub="`保证金 ${fmtMoneyCompact(funds.margin)} · 冻结 ${fmtMoneyCompact(funds.frozen_margin)}`" />
      <StatTile label="可用资金" :value="fmtMoneyCompact(funds.available)" :sub="fmtMoney(funds.available) + ' 元'" />
      <StatTile label="单位净值" :value="fmtNum(metrics.nav, 4)" :sub="`业绩权益 ${fmtMoneyCompact(metrics.perf_equity)}`" />
      <StatTile label="日内回撤 / 高水位回撤" :value="fmtPct(metrics.intraday_dd, 2)" :sub="`高水位回撤 ${fmtPct(metrics.hwm_dd, 2)}`" />
      <StatTile label="杠杆 / 有效品种数" :value="fmtTimes(metrics.leverage)" :sub="`N = ${metrics.effective_n > 0 ? fmtNum(metrics.effective_n, 2) : '—'} · 涨跌停压力 ${fmtPct(metrics.limit_stress, 1)}`" />
      <StatTile label="委托" :value="String(snap?.open_orders ?? 0) + ' 挂单'" :sub="`今日报单 ${metrics.orders_today} · 撤单 ${metrics.cancels_today}`" />
    </div>
    <div v-else-if="!errs.snapshot" class="card section-gap empty">正在加载账户快照（GET /accounts/{{ id }}）…</div>

    <div class="card section-gap">
      <div class="card-title">
        日内权益 · 当日盈亏 · 日内回撤
        <span class="hint">虚线为规则阈值；底色：浅橙 = 限制，浅红 = 熔断；圆点 = 风险事件（悬停查看）</span>
        <span class="spacer" />
        <el-radio-group v-model="windowMin" size="small">
          <el-radio-button :value="0">全天</el-radio-button>
          <el-radio-button :value="30">30 分钟</el-radio-button>
          <el-radio-button :value="5">5 分钟</el-radio-button>
        </el-radio-group>
      </div>
      <SourceNotice v-if="ownNotice(errs.equity) && !points.length" :error="errs.equity" subject="日内曲线" compact />
      <IntradayChart v-if="points.length" :points="points" :events="acctEvents" :color="color" :pnl-levels="pnlLevels" :dd-levels="ddLevels" :window-min="windowMin" />
      <div v-else-if="!ownNotice(errs.equity)" class="empty">{{ errs.equity ? '日内曲线暂不可用（见上方说明）' : `暂无日内数据（GET /accounts/${id}/equity-curve · WS equity:${id}）` }}</div>
    </div>

    <div class="card section-gap">
      <div class="card-title">规则使用率 <span class="hint">每条规则取最差范围；指针 = 当前值 ÷ 硬限额，轨道底色 = 各级别区间</span></div>
      <div v-if="gauges.length" class="gauges">
        <RuleGauge
          v-for="g in gauges"
          :key="g.ruleId"
          :title="g.title"
          :scope="g.scope"
          :value-text="g.valueText"
          :util="g.util"
          :marks="g.marks"
          :active-level="g.activeLevel"
          :sm-state="g.smState"
          :next-text="g.nextText"
          :headroom-text="g.headroomText"
        />
      </div>
      <div v-else class="empty">无规则状态</div>
    </div>

    <div class="card section-gap">
      <el-tabs v-model="tab">
        <el-tab-pane :label="`持仓 ${positions.length}`" name="positions">
          <PositionsTable :rows="positions" />
        </el-tab-pane>
        <el-tab-pane :label="`品种敞口 ${snap?.exposures?.length ?? 0}`" name="exposures">
          <el-table :data="snap?.exposures ?? []" size="small" empty-text="无敞口">
            <el-table-column label="品种" min-width="110">
              <template #default="{ row }"><b>{{ row.product }}</b> <span class="muted">{{ PRODUCT_LABELS[row.product] ?? '' }}</span></template>
            </el-table-column>
            <el-table-column label="板块" width="80">
              <template #default="{ row }">{{ SECTOR_LABELS[row.sector] ?? row.sector }}</template>
            </el-table-column>
            <el-table-column label="交易所" width="80">
              <template #default="{ row }">{{ EXCHANGE_LABELS[row.exchange] ?? row.exchange }}</template>
            </el-table-column>
            <el-table-column label="多 / 空 / 净（手）" width="130" align="right">
              <template #default="{ row }">{{ row.long_lots }} / {{ row.short_lots }} / <b>{{ row.net_lots }}</b></template>
            </el-table-column>
            <el-table-column label="净名义" min-width="120" align="right">
              <template #default="{ row }"><span :class="signClass(row.net_notional)">{{ fmtSignedMoney(row.net_notional, 0) }}</span></template>
            </el-table-column>
            <el-table-column label="总名义" min-width="120" align="right">
              <template #default="{ row }">{{ fmtMoney(row.gross_notional, 0) }}</template>
            </el-table-column>
            <el-table-column label="|净名义| / 权益" width="116" align="right">
              <template #default="{ row }">{{ fmtPct(row.net_pct_equity, 1) }}</template>
            </el-table-column>
            <el-table-column label="上限（3% ÷ 涨跌停）" width="140" align="right">
              <template #default="{ row }">{{ fmtPct(row.cap_pct, 0) }}</template>
            </el-table-column>
            <el-table-column label="上限使用率" min-width="170">
              <template #default="{ row }"><UtilBar :value="row.utilization" /></template>
            </el-table-column>
          </el-table>
        </el-tab-pane>
        <el-tab-pane :label="`委托 ${orders.length}`" name="orders">
          <SourceNotice v-if="ownNotice(errs.orders)" :error="errs.orders" subject="当日委托（risk.query.orders）" compact>
            <span v-if="orders.length" class="muted">（下表为 WS 推送的部分委托）</span>
          </SourceNotice>
          <OrdersTable :rows="orders" />
        </el-tab-pane>
        <el-tab-pane :label="`成交 ${trades.length}`" name="trades">
          <SourceNotice v-if="ownNotice(errs.trades)" :error="errs.trades" subject="当日成交（risk.query.fills）" compact />
          <el-table :data="trades" size="small" empty-text="无成交" max-height="460">
            <el-table-column label="时间" width="90">
              <template #default="{ row }"><span class="mono">{{ fmtTime(row.trade_ts_ns) }}</span></template>
            </el-table-column>
            <el-table-column label="成交号" prop="trade_id" width="100" />
            <el-table-column label="合约" prop="instrument_id" width="90" />
            <el-table-column label="买卖 / 开平" width="110">
              <template #default="{ row }"><span :class="row.direction === 'BUY' ? 'up' : 'down'">{{ DIRECTION_LABELS[row.direction] }}</span> {{ OFFSET_LABELS[row.offset] ?? row.offset }}</template>
            </el-table-column>
            <el-table-column label="价格" width="90" align="right">
              <template #default="{ row }">{{ fmtPrice(row.price) }}</template>
            </el-table-column>
            <el-table-column label="手数" prop="qty" width="70" align="right" />
            <el-table-column label="来源" width="90">
              <template #default="{ row }">{{ row.origin === 'EXTERNAL' ? '外部' : '内部' }}</template>
            </el-table-column>
            <el-table-column label="委托号" min-width="140">
              <template #default="{ row }"><span class="mono muted">{{ row.client_order_id ?? '—' }}</span></template>
            </el-table-column>
          </el-table>
        </el-tab-pane>
        <el-tab-pane name="plans">
          <template #label>
            减仓计划 <el-badge v-if="pendingPlans" :value="pendingPlans" type="danger" class="badge" />
          </template>
          <SourceNotice v-if="ownNotice(errs.plans)" :error="errs.plans" subject="减仓计划" compact />
          <SourceNotice v-else :errors="events.planErrors" subject="减仓计划" errors-label="以下风控分片未应答（其计划为网关缓存，标为陈旧）" compact />
          <LiquidationPlans :plans="plans" />
        </el-tab-pane>
        <el-tab-pane label="风险事件" name="events">
          <EventStream :account-id="id" max-height="480px" :hide-load-notice="unknownAccount" />
        </el-tab-pane>
      </el-tabs>
    </div>

    <ReasonDialog
      v-model="resetOpen"
      :title="`重置 ${id} 熔断`"
      description="POST /risk/accounts/{id}/reset：锁存级别转为 REARMED，重新布防线 = min(阈值, 当前值 − extra)；写审计。需要 RISK_MANAGER 权限。"
      confirm-text="确认重置"
      :action="doReset"
    >
      <el-form label-position="top">
        <el-form-item label="范围（可选）">
          <el-select v-model="resetRule" placeholder="全部已锁存级别" clearable style="width: 100%">
            <el-option v-for="r in latchedRules" :key="r.rule_id + r.active_level" :label="`${ruleLabel(r.rule_id)} · ${levelLabel(r.active_level)}`" :value="`${r.rule_id}/${r.active_level}`" />
          </el-select>
        </el-form-item>
      </el-form>
    </ReasonDialog>
    <ReasonDialog
      v-model="haltOpen"
      :title="`人工熔断 ${id}`"
      description="POST /risk/accounts/{id}/halt：账户直接进入 HALTED 并锁存，撤销开仓挂单，只允许减仓；写审计。需要 RISK_MANAGER 权限。"
      confirm-text="确认熔断"
      danger
      :action="doHalt"
    />
    <div class="muted foot">快照时间 {{ snap ? fmtDateTime(snap.as_of_ns) : '—' }} · 交易日 {{ snap?.trading_day ?? '—' }}</div>
  </div>
</template>

<style scoped>
.head-card .title-row {
  display: flex;
  align-items: center;
  gap: 10px;
  flex-wrap: wrap;
}
.head-card h1 {
  font-size: 20px;
  margin: 0;
}
.head-card .name {
  font-weight: 400;
  font-size: 15px;
  color: var(--fr-ink-2);
  margin-left: 4px;
}
.swatch {
  width: 4px;
  height: 22px;
  border-radius: 2px;
}
.spacer {
  flex: 1;
}
.restr {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
  align-items: center;
  margin-top: 10px;
  font-size: 12px;
}
.halt-note {
  margin-top: 10px;
  padding: 8px 12px;
  border-radius: 6px;
  background: #fff4e5;
  border: 1px solid #f5d29a;
  color: #7a4d00;
  font-size: 13px;
}
.halt-note.latched {
  background: #fdf1f1;
  border-color: #eab0b0;
  color: #8f1f1f;
}
.gauges {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
  gap: 10px;
}
.dot {
  display: inline-block;
  width: 8px;
  height: 8px;
  border-radius: 50%;
  margin-right: 6px;
  vertical-align: 0;
}
.dot.good {
  background: var(--fr-good);
}
.dot.warning {
  background: var(--fr-warning);
}
.dot.serious {
  background: var(--fr-serious);
}
.dot.critical {
  background: var(--fr-critical);
}
.dot.none {
  background: transparent;
  box-shadow: inset 0 0 0 1px #aab2c0;
}
.dot.stale {
  opacity: 0.45;
  outline: 1px dashed #c98a07;
  outline-offset: 1px;
}
.badge {
  margin-left: 4px;
}
.foot {
  margin-top: 10px;
  font-size: 12px;
}
</style>
