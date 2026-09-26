<script setup lang="ts">
/**
 * 行情与系统: quotes (WS `quotes`, GET /market/quotes) with source / lag / quality /
 * limit lock, data-source states + timeline + gap / switch log (WS `feeds`,
 * GET /market/feeds, GET /market/events), service topology & health (WS `system`,
 * GET /system/services, GET /system/status), demo controls (`/demo/*`).
 */
import { computed, ref } from 'vue'
import DemoControls from '@/components/DemoControls.vue'
import SourceNotice from '@/components/SourceNotice.vue'
import StatusPill from '@/components/StatusPill.vue'
import SourceTimeline from '@/components/charts/SourceTimeline.vue'
import TopologyGraph from '@/components/charts/TopologyGraph.vue'
import { useNow, usePolling, useTopics } from '@/composables/realtime'
import { showError, showOk } from '@/composables/feedback'
import { rt } from '@/runtime'
import { useAccounts } from '@/stores/accounts'
import { needRoleText, useAuth } from '@/stores/auth'
import { useMarket } from '@/stores/market'
import { useSystem } from '@/stores/system'
import type { MdEvent, ServiceHealth } from '@/types'
import { MD_EVENT_KIND_VALUES } from '@/types'
import { fmtDateTime, fmtDuration, fmtInt, fmtLag, fmtMicros, fmtNum, fmtPct, fmtPrice, fmtTime, signClass } from '@/utils/format'
import {
  EXCHANGE_LABELS,
  LIMIT_LOCK_LABELS,
  LIVENESS_LABELS,
  LIVENESS_TONE,
  MD_EVENT_LABELS,
  MD_EVENT_TONE,
  MD_QUALITY_LABELS,
  MD_QUALITY_TONE,
  PROCESS_STATE_LABELS,
  PRODUCT_LABELS,
  SERVICE_LABELS,
  SERVICE_REASON_LABELS,
  SERVICE_STATE_LABELS,
  SOURCE_LABELS,
  SOURCE_SHORT,
  SOURCE_STATE_LABELS,
  SOURCE_STATE_TONE,
  productOf,
  type StatusTone,
} from '@/utils/labels'
import { tickOf } from '@/utils/instruments'
import { processStateStale } from '@/utils/normalize'

const market = useMarket()
const system = useSystem()
const accounts = useAccounts()
const auth = useAuth()
const now = useNow(1000)

useTopics(['quotes', 'feeds', 'system'])

// failures are shown inline next to each section (md-gateway / supervisor may be down)
void Promise.allSettled([market.loadQuotes(), market.loadFeeds(), market.loadEvents(), system.loadServices(), system.loadStatus()])
usePolling(() => system.loadServices(), 10_000, false)
usePolling(() => system.loadStatus(), 15_000, false)
// md-gateway liveness (`stale` / `age_ms` of GET /market/feeds) and retries of what failed
usePolling(
  () => {
    if (market.feedsError || market.feedsStale || !market.status) void market.loadFeeds().catch(() => undefined)
    if (market.eventsError) void market.loadEvents().catch(() => undefined)
    return undefined
  },
  10_000,
  false,
)

/**
 * md-gateway is down: md.status (source states, instrument quality) is the last one the
 * gateway received (`stale` of GET /market/feeds; the WS `system` liveness reacts sooner).
 */
const mdDown = computed(() => market.feedsStale || system.services['md-gateway']?.liveness === 'DOWN')
const mdAgeText = computed(() => {
  const age = market.feedsAgeMs
  return age === null || !market.feedsStale ? '' : `，${fmtDuration(age)}前`
})

interface OpsNotice {
  key: string
  text: string
  tone: 'warn' | 'info'
}

/** What an operator must act on besides the services: API keys, notifications, config 2PC alerts (/system/status + WS). */
const opsNotices = computed<OpsNotice[]>(() => {
  const out: OpsNotice[] = []
  const st = system.status
  const a = st?.auth
  if (a) {
    const rejected = a.rejected ?? {}
    const missingEnv = a.missing_env ?? []
    const adminNa = a.admin_unavailable ?? []
    if (adminNa.length) {
      const why = adminNa.map((n) => `${n}（${missingEnv.includes(n) ? '环境变量未设置' : (rejected[n] ?? '未启用')}）`).join('，')
      out.push({ key: 'auth-admin', tone: 'warn', text: `已配置的 ADMIN 密钥未生效：${why}${a.bootstrap_active ? '；引导密钥仍然有效' : '；ADMIN 操作暂不可用'}` })
    } else if (a.bootstrap_active) {
      out.push({ key: 'auth-boot', tone: 'info', text: '正在使用引导 ADMIN 密钥（未配置命名 ADMIN 密钥）：配置 app.yaml api_keys 后自动停用' })
    }
    const otherMissing = missingEnv.filter((n) => !adminNa.includes(n))
    if (otherMissing.length) out.push({ key: 'auth-env', tone: 'warn', text: `以下命名密钥的环境变量未设置：${otherMissing.join('，')}` })
    const otherRejected = Object.entries(rejected).filter(([n]) => !adminNa.includes(n))
    if (otherRejected.length) out.push({ key: 'auth-rej', tone: 'warn', text: `以下密钥被拒绝：${otherRejected.map(([n, w]) => `${n}（${w}）`).join('，')}` })
    if (a.throttled > 0) out.push({ key: 'auth-thr', tone: 'info', text: `鉴权失败过多已限流 ${a.throttled} 次（HTTP 429 / WS 4401）` })
  }
  const n = st?.notifications
  if (n) {
    if (n.failed > 0) out.push({ key: 'notif-failed', tone: 'warn', text: `${n.failed} 条告警通知发送失败，请检查通知通道${n.channel ? `（${n.channel}）` : ''}` })
    if (n.pending > 0) out.push({ key: 'notif-pending', tone: 'info', text: `${n.pending} 条告警通知待发送` })
    if (n.suppressed > 0) out.push({ key: 'notif-supp', tone: 'info', text: `${n.suppressed} 条告警通知已抑制（重复、过期或超出速率）` })
  }
  for (const [i, ev] of system.configEvents.slice(0, 3).entries()) {
    const v = ev.config_version ?? '?'
    const lagging = Array.isArray(ev.lagging) ? (ev.lagging as unknown[]).map(String).join('，') : ''
    const err = typeof ev.error === 'string' ? ev.error : ''
    const text =
      ev.event === 'CONFIG_FAILED'
        ? `配置版本 ${v} 提交后 30s 内未收敛：${lagging || '未知实例'} 未应用（已告警）`
        : ev.event === 'CONFIG_ABORTED'
          ? `配置版本 ${v} 已中止${err ? '：' + err : ''}`
          : `配置事件 ${ev.event}（版本 ${v}）`
    out.push({ key: `cfg-${i}-${ev.event}-${v}`, tone: ev.event === 'CONFIG_FAILED' ? 'warn' : 'info', text })
  }
  return out
})
const servicesSummary = computed(() => {
  const st = system.status
  if (!st) return ''
  return `${st.services.up}/${st.services.total} 个实例在线${st.services.down.length ? '；离线：' + st.services.down.join('，') : ''}`
})

const quotes = computed(() => market.quoteList)
const instruments = computed(() => quotes.value.map((q) => q.instrument_id))

// ---------------------------------------------------------------- feeds
const kindFilter = ref<string>('')
const mdEvents = computed<MdEvent[]>(() => (kindFilter.value ? market.events.filter((e) => e.kind === kindFilter.value) : market.events).slice(0, 300))

const timelineTo = computed(() => {
  void now.value
  const ms = market.status ? market.status.as_of_ns / 1e6 + (Date.now() - market.statusAt) : (system.marketNowAt(now.value) ?? Date.now() * 1e6) / 1e6
  return ms
})
const timelineFrom = computed(() => timelineTo.value - 90 * 60 * 1000)

function eventTone(e: MdEvent): StatusTone {
  if (e.kind === 'SOURCE_STATE' && e.to_state) return SOURCE_STATE_TONE[e.to_state as 'TRUSTED'] ?? 'neutral'
  if (e.kind === 'QUALITY_CHANGE' && e.to_state) return MD_QUALITY_TONE[e.to_state as 'OK'] ?? 'neutral'
  return MD_EVENT_TONE[e.kind] ?? 'neutral'
}

function stateLabel(v: string | null | undefined): string {
  if (!v) return ''
  return SOURCE_STATE_LABELS[v as 'TRUSTED'] ?? MD_QUALITY_LABELS[v as 'OK'] ?? v
}

function detailText(e: MdEvent): string {
  const d = e.detail ?? {}
  return Object.entries(d)
    .map(([k, v]) => `${k}=${typeof v === 'object' ? JSON.stringify(v) : String(v)}`)
    .join(' ')
}

// ---------------------------------------------------------------- services
const services = computed(() => system.serviceList)
const instances = computed(() => services.value.map((s) => s.instance))
const selected = ref<string | null>(null)

/** A "gone" process state older than the instance's last heartbeat (since_ns < heartbeat_ns, liveness UP). */
function staleProcess(s: ServiceHealth): boolean {
  return processStateStale(s)
}

function processTone(s: ServiceHealth): StatusTone {
  if (staleProcess(s)) return s.heartbeat_state === 'DEGRADED' ? 'warning' : 'good'
  const ps = s.process_state
  if (ps === 'EXITED' || ps === 'FAILED' || ps === 'STOPPED') return 'critical'
  if (ps && ps !== 'RUNNING') return 'warning'
  if (!ps) return s.liveness === 'UP' ? 'good' : 'neutral'
  return s.heartbeat_state === 'DEGRADED' ? 'warning' : 'good'
}

/** Process column: the supervisor's view; without it (supervisor down) the gateway's liveness speaks. */
function processLabel(s: ServiceHealth): string {
  if (staleProcess(s)) return '运行（按心跳）'
  if (s.process_state) return PROCESS_STATE_LABELS[s.process_state] ?? s.process_state
  return '—'
}

function livenessTone(s: ServiceHealth): StatusTone {
  if (s.liveness) return LIVENESS_TONE[s.liveness]
  return now.value - s.updated_ms > 15_000 ? 'critical' : 'good'
}

function livenessLabel(s: ServiceHealth): string {
  if (s.liveness) return LIVENESS_LABELS[s.liveness]
  return now.value - s.updated_ms > 15_000 ? '心跳丢失' : '在线'
}

function sum(rec: Record<string, number>): number {
  return Object.values(rec).reduce((a, b) => a + b, 0)
}

function recText(rec: Record<string, number>, digits = 0): string {
  return Object.entries(rec)
    .map(([k, v]) => `${k}: ${fmtNum(v, digits)}`)
    .join('\n')
}

function firstLatency(s: ServiceHealth): string {
  const e = Object.entries(s.latency)[0]
  return e ? `${fmtMicros(e[1].p50)} / ${fmtMicros(e[1].p99)}` : '—'
}

function latencyText(s: ServiceHealth): string {
  return Object.entries(s.latency)
    .map(([k, v]) => `${k}: p50 ${fmtMicros(v.p50)} · p99 ${fmtMicros(v.p99)} · max ${fmtMicros(v.max)}`)
    .join('\n')
}

const clockVersion = computed(() => system.clock?.version ?? null)

async function restart(s: ServiceHealth): Promise<void> {
  try {
    const res = await rt().api.restartService(s.instance)
    showOk(`已请求重启 ${s.instance}${res.duplicate ? '（幂等重放）' : ''}`)
  } catch (e) {
    showError(e, `重启 ${s.instance} 失败`)
  }
}

function rowClass({ row }: { row: ServiceHealth }): string {
  return row.instance === selected.value ? 'row-selected' : ''
}

</script>

<template>
  <div class="page">
    <div class="page-head">
      <h1>行情与系统</h1>
      <span class="muted">行情只发布 LIVE 规范行情；补数（BACKFILL）只进录制与结算，不进风控（01 §8.13）</span>
    </div>

    <div class="card">
      <div class="card-title">行情 <span class="hint">WS quotes 2Hz · 质量 STALE（5s 无可信源）/ UNKNOWN（30s）时相关合约禁开</span></div>
      <el-table :data="quotes" size="small" :empty-text="market.feedsError ? '暂无行情（行情网关未连接）' : '暂无行情'">
        <el-table-column label="合约" min-width="120">
          <template #default="{ row }"><b>{{ row.instrument_id }}</b> <span class="muted">{{ PRODUCT_LABELS[productOf(row.instrument_id)] ?? '' }}</span></template>
        </el-table-column>
        <el-table-column label="交易所" width="76">
          <template #default="{ row }">{{ row.exchange ? (EXCHANGE_LABELS[row.exchange] ?? row.exchange) : '—' }}</template>
        </el-table-column>
        <el-table-column label="最新价" min-width="90" align="right">
          <template #default="{ row }"><b>{{ fmtPrice(row.last, tickOf(row.instrument_id)) }}</b></template>
        </el-table-column>
        <el-table-column label="涨跌幅" width="84" align="right">
          <template #default="{ row }"><span :class="signClass(row.change_pct)">{{ fmtPct(row.change_pct, 2, true) }}</span></template>
        </el-table-column>
        <el-table-column label="买一 / 卖一" min-width="140" align="right">
          <template #default="{ row }">{{ fmtPrice(row.bid1, tickOf(row.instrument_id)) }} / {{ fmtPrice(row.ask1, tickOf(row.instrument_id)) }}</template>
        </el-table-column>
        <el-table-column label="成交量" min-width="96" align="right">
          <template #default="{ row }">{{ fmtInt(row.volume) }}</template>
        </el-table-column>
        <el-table-column label="来源" width="84">
          <template #default="{ row }"><span v-if="row.source" class="src" :class="row.source">{{ SOURCE_SHORT[row.source] ?? row.source }}</span><span v-else class="muted">—</span></template>
        </el-table-column>
        <el-table-column label="延迟" width="84" align="right">
          <template #default="{ row }"><span :class="{ lagging: (row.lag_ms ?? 0) > 1000 }">{{ fmtLag(row.lag_ms) }}</span></template>
        </el-table-column>
        <el-table-column label="质量" width="84" align="center">
          <template #default="{ row }">
            <StatusPill v-if="mdDown" tone="neutral" label="未知" title="行情网关离线：质量未知" />
            <StatusPill v-else :tone="MD_QUALITY_TONE[row.quality as 'OK'] ?? 'neutral'" :label="MD_QUALITY_LABELS[row.quality as 'OK'] ?? row.quality" />
          </template>
        </el-table-column>
        <el-table-column label="封板" width="96" align="center">
          <template #default="{ row }">
            <StatusPill v-if="row.limit_lock && row.limit_lock !== 'NONE'" tone="critical" :label="LIMIT_LOCK_LABELS[row.limit_lock as 'UP']" />
            <span v-else class="muted">—</span>
          </template>
        </el-table-column>
        <el-table-column label="更新" width="72" align="right">
          <template #default="{ row }"><span class="muted">{{ row.updated_ms ? fmtDuration(Math.max(0, now - row.updated_ms)) : '—' }}</span></template>
        </el-table-column>
      </el-table>
    </div>

    <div class="card section-gap">
      <div class="card-title">行情源 <span class="hint">Feed A 推送（带 seq，可续传重放）· Feed B 轮询（无 seq）· 状态 TRUSTED / PROBATION / EXCLUDED</span></div>
      <SourceNotice v-if="!market.status" :error="market.feedsError" subject="数据源状态（md.status / md.sources）" :retry="() => market.loadFeeds().catch(() => undefined)" />
      <SourceNotice v-else-if="mdDown" compact :message="`行情网关离线：数据源状态与合约质量为最后一次收到的 md.status（${fmtTime(market.status.as_of_ns)}${mdAgeText}），不再更新`" />
      <div class="sources">
        <div v-for="s in market.sources" :key="s.source" class="src-card">
          <div class="src-head">
            <b>{{ SOURCE_LABELS[s.source] ?? s.source }}</b>
            <StatusPill :tone="SOURCE_STATE_TONE[s.state]" :label="SOURCE_STATE_LABELS[s.state]" />
            <StatusPill :tone="s.connected ? 'good' : 'critical'" :label="s.connected ? '已连接' : '断开'" />
          </div>
          <dl class="kv">
            <dt>进入当前状态</dt>
            <dd>{{ fmtTime(s.since_ns) }}</dd>
            <template v-if="s.epoch">
              <dt>epoch / last_seq</dt>
              <dd class="mono">{{ s.epoch }} / {{ s.last_seq ?? '—' }}</dd>
            </template>
            <dt>延迟</dt>
            <dd :class="{ lagging: (s.lag_ms ?? 0) > 3000 }">{{ fmtLag(s.lag_ms) }}<span v-if="(s.lag_ms ?? 0) > 3000" class="muted">（> 3s，不计 LIVE）</span></dd>
            <dt>消息速率</dt>
            <dd>{{ fmtNum(s.msgs_per_s, 1) }} /s</dd>
            <dt>拒收率（1 分钟）</dt>
            <dd :class="{ lagging: (s.reject_rate ?? 0) > 0.05 }">{{ fmtPct(s.reject_rate ?? 0, 1) }}</dd>
            <dt>交叉校验投票</dt>
            <dd>{{ s.integrity_votes ?? 0 }}</dd>
          </dl>
        </div>
        <div v-if="!market.sources.length && !market.feedsError" class="empty">暂无数据源状态（GET /market/feeds）</div>
      </div>
      <div class="section-gap">
        <div class="sub-title">源状态时间线 <span class="muted">最近 90 分钟（市场时间）</span></div>
        <SourceTimeline :status="market.status" :events="market.events" :from-ms="timelineFrom" :to-ms="timelineTo" />
      </div>
      <div class="section-gap">
        <div class="sub-title">
          缺口 / 切换 / 质量事件（md.event）
          <el-select v-model="kindFilter" size="small" clearable placeholder="全部类型" style="width: 160px; margin-left: 8px">
            <el-option v-for="k in MD_EVENT_KIND_VALUES" :key="k" :value="k" :label="MD_EVENT_LABELS[k]" />
          </el-select>
        </div>
        <SourceNotice :error="market.eventsError" subject="当日行情事件（md.events，由行情网关提供）" compact>
          <span v-if="market.events.length" class="muted">（下表为 WS 推送的事件）</span>
        </SourceNotice>
        <el-table :data="mdEvents" size="small" max-height="320" empty-text="暂无行情事件">
          <el-table-column label="时间" width="84">
            <template #default="{ row }"><span class="mono">{{ fmtTime(row.ts_ns) }}</span></template>
          </el-table-column>
          <el-table-column label="类型" width="130">
            <template #default="{ row }"><StatusPill :tone="eventTone(row as MdEvent)" :label="MD_EVENT_LABELS[row.kind as 'GAP_OPEN'] ?? row.kind" /></template>
          </el-table-column>
          <el-table-column label="数据源" width="80">
            <template #default="{ row }">{{ row.source ? (SOURCE_SHORT[row.source] ?? row.source) : '全部' }}</template>
          </el-table-column>
          <el-table-column label="合约" prop="instrument_id" width="84" />
          <el-table-column label="迁移" width="130">
            <template #default="{ row }"><span v-if="row.from_state || row.to_state">{{ stateLabel(row.from_state) }} → {{ stateLabel(row.to_state) }}</span></template>
          </el-table-column>
          <el-table-column label="缺口" min-width="260">
            <template #default="{ row }">
              <template v-if="row.gap">
                <span class="mono">{{ row.gap.gap_id }}</span>
                <span class="muted"> · 补齐 {{ row.gap.filled ?? 0 }} · 重复 {{ row.gap.duplicates ?? 0 }} · 不可恢复 {{ row.gap.unrecoverable ?? 0 }}</span>
              </template>
            </template>
          </el-table-column>
          <el-table-column label="详情" min-width="200">
            <template #default="{ row }"><span class="muted mono small">{{ detailText(row as MdEvent) }}</span></template>
          </el-table-column>
        </el-table>
      </div>
    </div>

    <div class="grid cols-2 section-gap">
      <div class="card">
        <div class="card-title">服务拓扑 <span class="hint">颜色 = 进程状态 + 网关看到的在线状态（心跳）；悬停查看指标，点击选中下表行</span></div>
        <TopologyGraph :services="services" :now="now" @select="selected = $event" />
      </div>
      <div class="card">
        <div class="card-title">服务事件（sys.service）</div>
        <el-table :data="system.events" size="small" max-height="380" empty-text="暂无进程事件">
          <el-table-column label="时间" width="84">
            <template #default="{ row }"><span class="mono">{{ fmtTime(row.ts_ns) }}</span></template>
          </el-table-column>
          <el-table-column label="实例" prop="instance" min-width="120" />
          <el-table-column label="迁移" min-width="130">
            <template #default="{ row }">{{ PROCESS_STATE_LABELS[row.from_state as 'RUNNING'] ?? row.from_state }} → {{ PROCESS_STATE_LABELS[row.to_state as 'RUNNING'] ?? row.to_state }}</template>
          </el-table-column>
          <el-table-column label="原因" min-width="110">
            <template #default="{ row }">
              {{ SERVICE_REASON_LABELS[row.reason] ?? row.reason }}
              <div v-if="row.detail" class="muted small" :title="row.detail">{{ row.detail }}</div>
            </template>
          </el-table-column>
          <el-table-column label="退出码" width="70" align="right">
            <template #default="{ row }">{{ row.exit_code ?? '—' }}</template>
          </el-table-column>
          <el-table-column label="重启" width="56" align="right" prop="restarts" />
        </el-table>
      </div>
    </div>

    <div class="card section-gap">
      <div class="card-title">
        服务健康
        <span class="hint">心跳 1Hz（不经过业务主循环）；配置哈希与多数不一致、时钟版本落后时标红（QS-09、02 §8.8）</span>
        <span class="spacer" />
        <span class="muted">{{ servicesSummary }}<template v-if="servicesSummary"> · </template>市场时钟版本 {{ clockVersion ?? '—' }}</span>
      </div>
      <SourceNotice :error="system.servicesError" subject="服务列表" compact />
      <SourceNotice
        v-if="system.supervisor && !system.supervisor.ok"
        compact
        :message="`supervisor 未应答（svc.ps：${system.supervisor.code}${system.supervisor.message ? ' · ' + system.supervisor.message : ''}）：进程列来自 sys.service 事件，在线状态来自网关收到的心跳`"
      />
      <SourceNotice v-for="n in opsNotices" :key="n.key" compact :tone="n.tone" :message="n.text" />
      <el-table :data="services" size="small" :row-class-name="rowClass" empty-text="暂无服务（GET /system/services）">
        <el-table-column label="实例" min-width="150" fixed>
          <template #default="{ row }"><b>{{ row.instance }}</b><div class="muted small">{{ SERVICE_LABELS[row.instance] ?? row.service }}</div></template>
        </el-table-column>
        <el-table-column label="在线" width="92" align="center">
          <template #default="{ row }"><StatusPill :tone="livenessTone(row as ServiceHealth)" :label="livenessLabel(row as ServiceHealth)" /></template>
        </el-table-column>
        <el-table-column label="进程 / 心跳" min-width="150">
          <template #default="{ row }">
            <el-tooltip :disabled="!row.since_ns && !row.heartbeat_ns && !row.last_error" placement="top">
              <template #content>
                <div v-if="row.since_ns">进程状态自 {{ fmtDateTime(row.since_ns) }}</div>
                <div v-if="row.heartbeat_ns">最后心跳 {{ fmtDateTime(row.heartbeat_ns) }}</div>
                <div v-if="staleProcess(row as ServiceHealth)">心跳晚于进程状态：进程已重新运行（supervisor 未报告）</div>
                <div v-if="row.last_error">最近错误：{{ row.last_error }}</div>
              </template>
              <span><StatusPill :tone="processTone(row as ServiceHealth)" :label="processLabel(row as ServiceHealth)" /></span>
            </el-tooltip>
            <span v-if="!row.heartbeat_state" class="muted small"> 无心跳</span>
            <span v-else-if="row.heartbeat_state !== 'RUNNING'" class="small hb-note"> 心跳：{{ SERVICE_STATE_LABELS[row.heartbeat_state as 'RUNNING'] ?? row.heartbeat_state }}</span>
          </template>
        </el-table-column>
        <el-table-column label="PID" width="64" align="right">
          <template #default="{ row }">{{ row.pid ?? '—' }}</template>
        </el-table-column>
        <el-table-column label="重启" width="56" align="right" prop="restarts" />
        <el-table-column label="更新" width="72" align="right">
          <template #default="{ row }"><span class="muted" title="本页最后一次收到该实例数据的时间">{{ fmtDuration(Math.max(0, now - row.updated_ms)) }}</span></template>
        </el-table-column>
        <el-table-column label="主循环滞后" width="88" align="right">
          <template #default="{ row }">{{ row.loop_lag_ms === null ? '—' : row.loop_lag_ms + ' ms' }}</template>
        </el-table-column>
        <el-table-column label="队列深度" width="84" align="right">
          <template #default="{ row }"><el-tooltip :content="recText(row.queues) || '—'" placement="top"><span>{{ fmtInt(sum(row.queues)) }}</span></el-tooltip></template>
        </el-table-column>
        <el-table-column label="消息速率" width="92" align="right">
          <template #default="{ row }"><el-tooltip :content="recText(row.rates, 1) || '—'" placement="top"><span>{{ fmtNum(sum(row.rates), 1) }}/s</span></el-tooltip></template>
        </el-table-column>
        <el-table-column label="延迟 p50 / p99" min-width="140" align="right">
          <template #default="{ row }"><el-tooltip :content="latencyText(row as ServiceHealth) || '—'" placement="top"><span>{{ firstLatency(row as ServiceHealth) }}</span></el-tooltip></template>
        </el-table-column>
        <el-table-column label="配置" width="110">
          <template #default="{ row }">
            <span :class="{ lagging: row.config_hash && system.majorityConfigHash && row.config_hash !== system.majorityConfigHash }" :title="row.config_hash ?? ''">v{{ row.config_version ?? '—' }} · {{ row.config_hash ? row.config_hash.slice(0, 6) : '—' }}</span>
          </template>
        </el-table-column>
        <el-table-column label="时钟版本" width="80" align="right">
          <template #default="{ row }"><span :class="{ lagging: clockVersion !== null && row.clock_version !== null && row.clock_version < clockVersion }">{{ row.clock_version ?? '—' }}</span></template>
        </el-table-column>
        <el-table-column label="操作" width="80" fixed="right">
          <template #default="{ row }">
            <el-popconfirm v-if="row.instance !== 'supervisor' && auth.canAdmin" :title="`重启 ${row.instance}（优雅停机后拉起）？`" @confirm="restart(row as ServiceHealth)">
              <template #reference><el-button link size="small" type="primary">重启</el-button></template>
            </el-popconfirm>
            <el-tooltip v-else-if="row.instance !== 'supervisor'" :content="needRoleText('ADMIN')" placement="left">
              <span><el-button link size="small" disabled>重启</el-button></span>
            </el-tooltip>
          </template>
        </el-table-column>
      </el-table>
    </div>

    <div class="card section-gap">
      <div class="card-title">演示控制</div>
      <DemoControls :instruments="instruments" :instances="instances" :accounts="accounts.ids" />
    </div>
  </div>
</template>

<style scoped>
.src {
  font-size: 12px;
  padding: 1px 6px;
  border-radius: 4px;
  background: #eef3fb;
  color: #1c5cab;
}
.src.FEED_B {
  background: #f3eefb;
  color: #4a3aa7;
}
.hb-note {
  color: #8a5a00;
}
.lagging {
  color: #b42323;
  font-weight: 600;
}
.sources {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
  gap: 10px;
}
.src-card {
  border: 1px solid var(--fr-line);
  border-radius: var(--fr-radius);
  padding: 10px 12px;
}
.src-head {
  display: flex;
  gap: 8px;
  align-items: center;
  margin-bottom: 8px;
}
.sub-title {
  font-weight: 600;
  font-size: 13px;
  margin-bottom: 6px;
  display: flex;
  align-items: center;
}
.sub-title .muted {
  font-weight: 400;
  margin-left: 6px;
  font-size: 12px;
}
.small {
  font-size: 12px;
}
.spacer {
  flex: 1;
}
:deep(.row-selected) {
  --el-table-tr-bg-color: #eef3fb;
}
</style>
