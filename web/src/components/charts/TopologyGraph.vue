<script setup lang="ts">
/**
 * Service topology (01 §5.1 container view) colored by health: process state from the
 * supervisor + heartbeat state reported by the service. Every node also prints its
 * state as text. Click a node to select it.
 */
import { computed } from 'vue'
import VChart from 'vue-echarts'
import type { ServiceHealth } from '@/types'
import { esc, FONT, tooltipBase } from '@/charts'
import { fmtMicros } from '@/utils/format'
import { processStateStale } from '@/utils/normalize'
import { CHART_INK, PROCESS_STATE_LABELS, SERVICE_LABELS, SERVICE_STATE_LABELS, STATUS_COLORS } from '@/utils/labels'

const props = withDefaults(defineProps<{ services: ServiceHealth[]; staleMs?: number; now: number }>(), { staleMs: 5000 })
const emit = defineEmits<{ select: [instance: string] }>()

const LAYOUT: Record<string, [number, number]> = {
  supervisor: [0, 0],
  'sim-exchange': [0, 170],
  'strategy-sim': [0, 330],
  'md-gateway': [200, 70],
  'trade-gateway': [200, 330],
  'risk-engine-0': [400, 150],
  'risk-engine-1': [400, 280],
  'clearing-worker': [600, 70],
  'api-gateway': [600, 250],
  dashboard: [790, 250],
}

const EDGES: [string, string, string][] = [
  ['sim-exchange', 'md-gateway', 'Feed A / B'],
  ['md-gateway', 'risk-engine-0', 'md.tick.*'],
  ['md-gateway', 'risk-engine-1', 'md.tick.*'],
  ['strategy-sim', 'risk-engine-1', 'order.submit'],
  ['strategy-sim', 'risk-engine-0', 'order.submit'],
  ['risk-engine-0', 'trade-gateway', 'route.order'],
  ['risk-engine-1', 'trade-gateway', 'route.order'],
  ['trade-gateway', 'sim-exchange', 'SimBroker'],
  ['risk-engine-1', 'risk-engine-0', 'risk.snapshot.*'],
  ['trade-gateway', 'clearing-worker', 'td.exec.*'],
  ['clearing-worker', 'risk-engine-0', 'eod.*'],
  ['clearing-worker', 'md-gateway', 'md.settle_inputs'],
  ['api-gateway', 'risk-engine-0', 'ctl / risk.*'],
  ['api-gateway', 'risk-engine-1', 'ctl / risk.*'],
  ['api-gateway', 'clearing-worker', 'settle / report'],
  ['api-gateway', 'md-gateway', 'md.*'],
  ['dashboard', 'api-gateway', 'REST + WS'],
]

type Health = 'ok' | 'degraded' | 'down' | 'unknown'

/**
 * Node health: a process the supervisor reports gone wins (unless heartbeats arrived after
 * that state: the process runs again); then the gateway's liveness (UP / DOWN / UNKNOWN from
 * heartbeat age); then the service's own heartbeat state. Without a liveness (older
 * payloads) a stale update counts as a lost heartbeat.
 */
function health(s: ServiceHealth | undefined, now: number, staleMs: number): { h: Health; text: string } {
  if (!s) return { h: 'unknown', text: '未知' }
  const ps = s.process_state
  const gone = ps === 'FAILED' || ps === 'STOPPED' || ps === 'EXITED'
  if (gone && !processStateStale(s)) return { h: 'down', text: PROCESS_STATE_LABELS[ps] }
  if (s.liveness === 'DOWN') return { h: 'down', text: '离线（无心跳）' }
  if (s.liveness === 'UNKNOWN') return { h: 'unknown', text: '未知（未见心跳）' }
  if (ps && ps !== 'RUNNING' && !gone && !processStateStale(s)) return { h: 'degraded', text: PROCESS_STATE_LABELS[ps] }
  if (!s.liveness && now - s.updated_ms > staleMs * 3) return { h: 'down', text: '心跳丢失' }
  if (s.heartbeat_state === 'DEGRADED' || s.heartbeat_state === 'STARTING' || s.heartbeat_state === 'STOPPING') return { h: 'degraded', text: SERVICE_STATE_LABELS[s.heartbeat_state] }
  return { h: 'ok', text: s.heartbeat_state ? SERVICE_STATE_LABELS[s.heartbeat_state] : '运行' }
}

const COLOR: Record<Health, string> = { ok: STATUS_COLORS.good, degraded: STATUS_COLORS.warning, down: STATUS_COLORS.critical, unknown: STATUS_COLORS.neutral }

const option = computed(() => {
  const byId = new Map(props.services.map((s) => [s.instance, s]))
  const ids = Object.keys(LAYOUT)
  const nodes = ids.map((id) => {
    const [x, y] = LAYOUT[id] as [number, number]
    if (id === 'dashboard') return { id, name: '仪表盘', x, y, symbol: 'roundRect', symbolSize: [64, 30], itemStyle: { color: '#ffffff', borderColor: CHART_INK.axis, borderWidth: 1.5 }, label: { show: true, position: 'inside', color: CHART_INK.secondary, fontSize: 11, formatter: '仪表盘' }, svc: null, hs: null }
    const s = byId.get(id)
    const hs = health(s, props.now, props.staleMs)
    return {
      id,
      name: id,
      x,
      y,
      symbolSize: id === 'supervisor' ? 34 : 40,
      itemStyle: { color: COLOR[hs.h], borderColor: '#ffffff', borderWidth: 2 },
      label: { show: true, position: 'bottom', color: CHART_INK.primary, fontSize: 11, lineHeight: 14, formatter: `${id}\n{s|${hs.text}${s && s.restarts ? ' · 重启 ' + s.restarts : ''}}`, rich: { s: { color: CHART_INK.secondary, fontSize: 10 } } },
      svc: s ?? null,
      hs,
    }
  })
  const links = EDGES.map(([a, b, label]) => ({ source: a, target: b, label: { show: false, formatter: label }, value: label }))
  return {
    animation: false,
    textStyle: { fontFamily: FONT },
    tooltip: {
      ...tooltipBase,
      trigger: 'item',
      formatter: (p: { dataType: string; data: { id?: string; svc?: ServiceHealth | null; hs?: { text: string } | null; value?: string; source?: string; target?: string } }) => {
        if (p.dataType === 'edge') return `${esc(p.data.source)} → ${esc(p.data.target)}<br/>${esc(p.data.value)}`
        const s = p.data.svc
        if (!s) return esc(p.data.id === 'dashboard' ? '浏览器仪表盘（本页）' : p.data.id)
        const lat = Object.entries(s.latency).map(([k, v]) => `${esc(k)} p50 ${fmtMicros(v.p50)} / p99 ${fmtMicros(v.p99)}`).join('<br/>')
        const q = Object.entries(s.queues).map(([k, v]) => `${esc(k)}=${v}`).join(' ')
        return `<div style="font-weight:600">${esc(s.instance)} · ${esc(SERVICE_LABELS[s.instance] ?? s.service)}</div><div>状态：${esc(p.data.hs?.text ?? '')}，PID ${s.pid ?? '—'}，重启 ${s.restarts} 次</div><div>主循环滞后：${s.loop_lag_ms ?? '—'} ms</div><div>队列：${q || '—'}</div>${lat ? `<div>${lat}</div>` : ''}`
      },
    },
    series: [
      {
        type: 'graph',
        layout: 'none',
        roam: false,
        top: 20,
        bottom: 40,
        left: 50,
        right: 60,
        edgeSymbol: ['none', 'arrow'],
        edgeSymbolSize: 7,
        lineStyle: { color: CHART_INK.axis, width: 1.2, curveness: 0.12, opacity: 0.9 },
        emphasis: { focus: 'adjacency', lineStyle: { width: 2 } },
        data: nodes,
        links,
      },
    ],
  }
})

function onClick(params: unknown): void {
  const p = params as { dataType?: string; data?: { id?: string } | null }
  if (p.dataType === 'node' && p.data?.id && p.data.id !== 'dashboard') emit('select', p.data.id)
}
</script>

<template>
  <div>
    <v-chart class="chart" style="height: 360px" :option="option" autoresize @click="onClick" />
    <div class="legend">
      <span><i :style="{ background: COLOR.ok }" />运行</span>
      <span><i :style="{ background: COLOR.degraded }" />启动 / 降级 / 重启中</span>
      <span><i :style="{ background: COLOR.down }" />退出 / 心跳丢失</span>
      <span><i :style="{ background: COLOR.unknown }" />未知</span>
    </div>
  </div>
</template>

<style scoped>
.legend {
  display: flex;
  gap: 14px;
  font-size: 12px;
  color: var(--fr-ink-2);
  flex-wrap: wrap;
}
.legend i {
  display: inline-block;
  width: 10px;
  height: 10px;
  border-radius: 50%;
  margin-right: 4px;
  vertical-align: -1px;
}
</style>
