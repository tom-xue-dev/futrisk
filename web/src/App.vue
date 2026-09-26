<script setup lang="ts">
/**
 * App shell: navigation for the five pages, trading day + market clock, the global
 * kill-switch indicator (partial / stale states included), EOD alerts (FAILED run, EOD
 * freeze, provisional trading) with the CLI remedy, WebSocket status and a banner that
 * freezes the page when the connection is down, settings. Subscribes the topics every page
 * needs (firm, accounts, events, eod), polls `/system/status`, and toasts live risk events,
 * EOD warnings / failures and operator alerts (kill-switch targets given up, config not
 * converged).
 */
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { useRoute } from 'vue-router'
import { ElNotification } from 'element-plus'
import zhCn from 'element-plus/es/locale/lang/zh-cn'
import { Setting, Lock, Unlock, Connection } from '@element-plus/icons-vue'
import SettingsDialog from '@/components/SettingsDialog.vue'
import { usePolling, useTopics } from '@/composables/realtime'
import { showError } from '@/composables/feedback'
import { rt } from '@/runtime'
import { useAccounts } from '@/stores/accounts'
import { ROLE_LABELS, useAuth } from '@/stores/auth'
import { useClock } from '@/stores/clock'
import { useCompare } from '@/stores/compare'
import { useConnection } from '@/stores/connection'
import { setEodFailureNotifier, setEodWarningNotifier, useEod } from '@/stores/eod'
import { setLiveEventNotifier, useEvents } from '@/stores/events'
import { useFirm } from '@/stores/firm'
import { useSettings } from '@/stores/settings'
import { setSystemAlertNotifier, useSystem } from '@/stores/system'
import { fmtDateTime, fmtDuration, fmtTd, fmtTime } from '@/utils/format'
import { EOD_PHASE_LABELS, EOD_WARNING_LABELS, RISK_EVENT_LABELS } from '@/utils/labels'

const route = useRoute()
const settings = useSettings()
const conn = useConnection()
const accounts = useAccounts()
const events = useEvents()
const firm = useFirm()
const system = useSystem()
const compare = useCompare()
const auth = useAuth()
const eod = useEod()
const clock = useClock()

const settingsOpen = ref(false)
const now = ref(Date.now())
let timer: ReturnType<typeof setInterval> | null = null

// topics every page needs; `eod` everywhere so EOD warnings / failures are seen on every page.
// `system` (1 Hz service items) only where it is shown: overview, account, market & system.
useTopics(['firm', 'accounts', 'events', 'eod'])

// trading mode (EOD freeze / provisional), kill-switch confirmation: faster while targets are pending
usePolling(() => system.loadStatus(), () => (system.killSwitchControl?.pending.length ? 3_000 : 15_000), false)

const activeMenu = computed(() => {
  const n = route.name
  if (n === 'account') return '/accounts'
  return route.path === '/' ? '/' : `/${route.path.split('/')[1] ?? ''}`
})

const accountLink = computed(() => `/accounts/${(route.name === 'account' && route.params.id) || accounts.ids[0] || 'ACC1'}`)

const tradingDay = computed(() => firm.view.trading_day ?? system.status?.trading_day ?? accounts.tradingDay)

const marketClock = computed(() => {
  void now.value
  const ns = system.marketNowAt(now.value)
  return ns ? fmtDateTime(ns) : null
})

const wsInfo = computed(() => {
  const s = conn.status
  const age = conn.stats.lastMessageMs ? now.value - conn.stats.lastMessageMs : null
  if (s === 'open') return { tone: age !== null && age > 12_000 ? 'warning' : 'good', text: '实时' }
  if (s === 'reconnecting' || s === 'connecting') return { tone: 'warning', text: conn.stats.nextRetryMs ? `重连中 ${fmtDuration(Math.max(0, conn.stats.nextRetryMs - now.value))}` : '连接中' }
  if (s === 'no-key') return { tone: 'critical', text: '未设置 API key' }
  if (s === 'unauthorized') return { tone: 'critical', text: 'API key 无效' }
  return { tone: 'critical', text: '断开' }
})

/** Global kill switch as it really is: on / off / partial (targets not confirmed) / unknown / stale. */
const killBadge = computed(() => {
  const k = firm.killState
  switch (k.kind) {
    case 'on':
      return { cls: 'kill-on', text: k.short, title: '全局禁开已开启：所有账户禁止开仓，平仓照常；全部风控分片与交易网关已确认' }
    case 'partial-on':
    case 'partial-off':
      return { cls: 'kill-partial', text: k.short, title: `${k.text}：未确认的目标仍按原状态处理委托；可在总览页重新下发` }
    case 'stale':
      return { cls: 'kill-unknown', text: k.short, title: `公司层汇总已 ${fmtDuration(firm.view.stale_ms ?? 0)} 未更新（coordinator 分片离线？）${k.text}` }
    case 'unknown':
      return { cls: 'kill-unknown', text: k.short, title: '尚未收到公司层数据：全局禁开状态未知' }
    default:
      return { cls: 'kill-off', text: k.short, title: '全局禁开未开启' }
  }
})
const killLocked = computed(() => firm.killState.kind === 'on' || firm.killState.kind === 'partial-on')

/** EOD problems every page must see: a FAILED run, the EOD freeze, provisional trading. */
const eodAlert = computed(() => {
  // the latest two days only: an old failed run that was never repeated is history, not an alert
  const failedTd = eod.dayList.slice(0, 2).find((d) => eod.phaseOf(d)?.phase === 'FAILED')
  if (failedTd) {
    const ph = eod.phaseOf(failedTd)
    const err = ph?.event?.error
    return {
      cls: 'eod-failed',
      text: `EOD 失败 · ${fmtTd(failedTd)}`,
      title: `${fmtTd(failedTd)} 结算 v${ph?.version ?? '?'} 失败${err ? '：' + err : ''}。处理：排除原因后在「报表」页重新发起结算，或运行 futrisk settle status --td ${failedTd} 查看、futrisk settle run --td ${failedTd} --wait 重跑。`,
    }
  }
  const mode = system.status?.trading_mode
  const td = system.status?.trading_day ?? ''
  if (mode === 'PROVISIONAL') {
    return {
      cls: 'eod-warn',
      text: '临时快照交易',
      title: '上一交易日的 EOD 未在本交易时段开始前完成：各分片按临时快照交易，只允许平仓。完成结算：futrisk settle run --wait（或「报表」页发起结算）；紧急时可在「报表」页审计放行开仓。',
    }
  }
  if (mode === 'EOD_FROZEN') {
    return { cls: 'eod-warn', text: 'EOD 冻结', title: `日终结算进行中：开仓暂停，结算完成后自动恢复。查看进度：futrisk settle status${td ? ' --td ' + td : ''}，或「报表」页。` }
  }
  return null
})

/** WebSocket down for more than 5 s: the page shows the last data, dimmed, with a banner. */
const downSince = ref<number | null>(null)
watch(
  () => conn.status,
  (st) => {
    if (st === 'open') downSince.value = null
    else if (downSince.value === null) downSince.value = Date.now()
  },
  { immediate: true },
)
const connBanner = computed(() => {
  if (conn.status === 'open' || downSince.value === null || now.value - downSince.value < 5_000) return null
  const mts = system.mts
  const at = mts !== null ? fmtTime(mts * 1e6) : null
  const why =
    conn.status === 'unauthorized' ? 'API key 无效' : conn.status === 'no-key' ? '未设置 API key' : conn.stats.nextRetryMs ? `重连中（${fmtDuration(Math.max(0, conn.stats.nextRetryMs - now.value))}）` : '正在重连'
  return `连接中断 · ${at ? `数据停止于 ${at}` : '尚未收到数据'} · ${why}：页面数值不再更新`
})

const roleBadge = computed(() => {
  if (!auth.me) return auth.code && auth.code !== 'OK' ? { text: auth.code === 'UNAUTHORIZED' ? 'key 无效' : '身份未知', title: `GET /auth/me：${auth.code} ${auth.message}` } : null
  return { text: `${auth.me.name || auth.me.key_id} · ${ROLE_LABELS[auth.me.role] ?? auth.me.role}`, title: `API key：${auth.me.key_id}（${auth.me.role}）` }
})

// a rejected key (REST 401 or WS close 4401): ask for another one
watch(
  () => auth.unauthorized || conn.status === 'unauthorized',
  (bad) => {
    if (bad && !rt().isMock) settingsOpen.value = true
  },
)

async function initialLoad(): Promise<void> {
  // live mode: wait for the API key (the settings dialog opens automatically)
  if (!rt().isMock && !settings.hasKey) return
  await auth.load()
  if (auth.unauthorized) return
  const tasks: [string, Promise<unknown>][] = [
    ['账户', accounts.loadAll()],
    ['风险事件', events.load()],
    ['风控规则', compare.loadRules(true)],
    ['系统状态', system.loadStatus()],
    ['结算日', eod.loadDays(10)],
  ]
  const results = await Promise.allSettled(tasks.map((t) => t[1]))
  results.forEach((r, i) => {
    if (r.status === 'rejected') {
      const label = tasks[i]?.[0] ?? ''
      conn.restError(`${label}: ${String(r.reason)}`)
      if (i === 0) showError(r.reason, `加载${label}失败`)
    }
  })
}

onMounted(() => {
  timer = setInterval(() => {
    now.value = Date.now()
    clock.tick(now.value)
    conn.update(rt().ws.status, rt().ws.stats)
  }, 1000)
  setLiveEventNotifier((ev) => {
    if (!settings.notify) return
    ElNotification({
      title: `${RISK_EVENT_LABELS[ev.kind] ?? ev.kind}${ev.account_id ? ' · ' + ev.account_id : ''}`,
      message: ev.message ?? '',
      type: ev.severity === 'CRITICAL' ? 'error' : 'warning',
      duration: ev.severity === 'CRITICAL' ? 8000 : 4500,
      position: 'bottom-right',
    })
  })
  // operator alerts pushed on the WS `system` topic: kill-switch targets given up, config 2PC failures
  setSystemAlertNotifier((ev) => {
    if (ev.kind === 'kill_switch') {
      ElNotification({
        title: '全局禁开未能全部生效',
        message: `${(ev.failed ?? []).join('，')} 未确认全局禁开${ev.desired_on ? '开启' : '解除'}（网关已停止重试并告警）：可在总览页重新下发，或检查该服务`,
        type: 'error',
        duration: 0,
        position: 'bottom-right',
      })
    } else if (ev.event === 'CONFIG_FAILED') {
      const lagging = Array.isArray(ev.lagging) ? (ev.lagging as unknown[]).map(String).join('，') : ''
      ElNotification({ title: '配置未收敛', message: `配置版本 ${String(ev.config_version ?? '?')} 提交后 30s 内未生效：${lagging || '未知实例'}`, type: 'error', duration: 0, position: 'bottom-right' })
    } else {
      ElNotification({ title: `配置事件 ${ev.event}`, message: `配置版本 ${String(ev.config_version ?? '?')}${typeof ev.error === 'string' ? '：' + ev.error : ''}`, type: 'warning', duration: 8000, position: 'bottom-right' })
    }
  })
  // a FAILED EOD run: sticky until closed, with the remedy
  setEodFailureNotifier((ev) => {
    const prev = [...eod.log].reverse().find((e) => e.trading_day === ev.trading_day && e.version === ev.version && e.phase !== 'FAILED')
    ElNotification({
      title: `EOD 失败 · ${fmtTd(ev.trading_day)} v${ev.version}`,
      message: `${prev ? `在「${EOD_PHASE_LABELS[prev.phase]}」之后失败` : '结算失败'}${ev.error ? '：' + ev.error : ''}。排除原因后在「报表」页重新发起结算，或运行 futrisk settle run --td ${ev.trading_day} --wait`,
      type: 'error',
      duration: 0,
      position: 'bottom-right',
    })
  })
  setEodWarningNotifier((w) => {
    if (!settings.notify) return
    ElNotification({
      title: `EOD 警告 · ${EOD_WARNING_LABELS[w.kind] ?? w.kind}${w.trading_day ? ' · ' + fmtTd(w.trading_day) : ''}`,
      message: w.message,
      type: 'warning',
      duration: 8000,
      position: 'bottom-right',
    })
  })
  if (!rt().isMock && !settings.hasKey) settingsOpen.value = true
  void initialLoad()
})

onBeforeUnmount(() => {
  if (timer) clearInterval(timer)
  setLiveEventNotifier(null)
  setSystemAlertNotifier(null)
  setEodWarningNotifier(null)
  setEodFailureNotifier(null)
})
</script>

<template>
  <el-config-provider :locale="zhCn">
  <el-container class="shell">
    <el-header class="header" height="52px">
      <router-link to="/" class="brand">
        <svg width="22" height="22" viewBox="0 0 32 32" aria-hidden="true"><rect width="32" height="32" rx="6" fill="#2b3447" /><path d="M6 22l6-7 5 4 9-11" stroke="#fab219" stroke-width="3" fill="none" stroke-linecap="round" stroke-linejoin="round" /></svg>
        <span>futrisk 风控仪表盘</span>
      </router-link>
      <el-menu mode="horizontal" :default-active="activeMenu" router :ellipsis="false" class="nav" background-color="#1d2433" text-color="#c9cdd6" active-text-color="#ffffff">
        <el-menu-item index="/">总览</el-menu-item>
        <el-menu-item index="/accounts" :route="accountLink">账户</el-menu-item>
        <el-menu-item index="/compare">多账户对比</el-menu-item>
        <el-menu-item index="/market">行情与系统</el-menu-item>
        <el-menu-item index="/reports">报表</el-menu-item>
      </el-menu>
      <div class="spacer" />
      <div class="meta">
        <span v-if="rt().isMock" class="badge mock" title="数据来自浏览器内置的模拟后端（VITE_MOCK / 设置）">模拟数据</span>
        <span v-if="tradingDay" class="item" title="当前交易日">交易日 {{ fmtTd(tradingDay) }}</span>
        <span v-if="marketClock" class="item mono" :title="`市场时钟（Asia/Shanghai），版本 ${system.clock?.version ?? '—'}，模式 ${system.clock?.mode ?? '—'}`">{{ marketClock }}</span>
        <span class="badge" :class="killBadge.cls" :title="killBadge.title">
          <el-icon><Lock v-if="killLocked" /><Unlock v-else /></el-icon>
          {{ killBadge.text }}
        </span>
        <router-link v-if="eodAlert" to="/reports" class="badge" :class="eodAlert.cls" :title="eodAlert.title">{{ eodAlert.text }}</router-link>
        <span v-if="roleBadge" class="badge role" :title="roleBadge.title">{{ roleBadge.text }}</span>
        <span class="badge ws" :class="wsInfo.tone" :title="`WebSocket：收到 ${conn.stats.received} 帧，缺口 ${conn.stats.gaps}，resync ${conn.stats.resyncRequests}，重连 ${conn.stats.reconnects}`" @click="settingsOpen = true">
          <el-icon><Connection /></el-icon>
          {{ wsInfo.text }}
        </span>
        <el-button class="gear" circle text :icon="Setting" title="设置" @click="settingsOpen = true" />
      </div>
    </el-header>
    <el-main class="main" :class="{ frozen: !!connBanner }">
      <div v-if="connBanner" class="conn-banner" role="alert">{{ connBanner }}</div>
      <router-view />
    </el-main>
    <SettingsDialog v-model="settingsOpen" @saved="initialLoad" />
  </el-container>
  </el-config-provider>
</template>

<style scoped>
.shell {
  min-height: 100%;
}
.header {
  display: flex;
  align-items: center;
  gap: 16px;
  background: var(--fr-header);
  color: #fff;
  padding: 0 16px;
  position: sticky;
  top: 0;
  z-index: 20;
}
.brand {
  display: flex;
  align-items: center;
  gap: 8px;
  color: #fff;
  font-weight: 600;
  font-size: 15px;
  white-space: nowrap;
  text-decoration: none;
}
.nav {
  border-bottom: none;
  height: 52px;
}
.nav :deep(.el-menu-item) {
  height: 52px;
  line-height: 52px;
}
.spacer {
  flex: 1;
}
.meta {
  display: flex;
  align-items: center;
  gap: 10px;
  font-size: 12px;
  white-space: nowrap;
}
.item {
  color: #c9cdd6;
}
.badge {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  border-radius: 999px;
  padding: 2px 10px;
  font-size: 12px;
  line-height: 18px;
  border: 1px solid rgba(255, 255, 255, 0.18);
  color: #e6e8ec;
}
.badge.mock {
  background: #3a3322;
  border-color: #6b5a26;
  color: #fad97a;
}
.badge.kill-on {
  background: #d03b3b;
  border-color: #d03b3b;
  color: #fff;
  font-weight: 600;
}
.badge.kill-off {
  color: #c9cdd6;
}
.badge.kill-partial {
  background: #8a5a00;
  border-color: #eda100;
  color: #fff;
  font-weight: 600;
}
.badge.kill-unknown {
  border-style: dashed;
  border-color: #8b93a3;
  color: #aeb4c0;
  font-style: italic;
}
.badge.eod-failed {
  background: #d03b3b;
  border-color: #d03b3b;
  color: #fff;
  font-weight: 600;
  text-decoration: none;
}
.badge.eod-warn {
  background: #8a5a00;
  border-color: #eda100;
  color: #fff;
  text-decoration: none;
}
.badge.role {
  color: #c9cdd6;
}
.badge.ws {
  cursor: pointer;
}
.badge.ws.good {
  border-color: #2f7d3a;
  color: #9be29b;
}
.badge.ws.warning {
  border-color: #8a6a1a;
  color: #fad97a;
}
.badge.ws.critical {
  border-color: #a33;
  color: #ffb3b3;
}
.gear {
  color: #c9cdd6;
}
.main {
  padding: 0;
}
.conn-banner {
  position: sticky;
  top: 52px;
  z-index: 15;
  background: #5b2a2a;
  color: #fff;
  font-size: 13px;
  padding: 6px 20px;
}
/* frozen data must not look live */
.main.frozen :deep(.page) {
  opacity: 0.55;
  filter: grayscale(0.6);
  transition: opacity 0.3s;
}
</style>
