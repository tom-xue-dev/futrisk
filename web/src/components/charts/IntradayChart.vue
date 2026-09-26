<script setup lang="ts">
/**
 * Intraday equity / daily P&L % / intraday drawdown for one account: three panels on a
 * shared time axis (never a dual axis), rule threshold lines, shaded RESTRICTED /
 * HALTED periods and risk-event markers.
 */
import { computed } from 'vue'
import VChart from 'vue-echarts'
import type { EquityPoint, RiskEvent } from '@/types'
import { esc, FONT, timeAxis, tipRow, tooltipBase, valueAxis } from '@/charts'
import { fmtClockMs, fmtMoney, fmtMoneyCompact, fmtPct } from '@/utils/format'
import { ACCOUNT_STATE_LABELS, CHART_INK, RISK_EVENT_LABELS, STATUS_COLORS, levelLabel } from '@/utils/labels'
import type { ThresholdLine } from './types'

const props = withDefaults(
  defineProps<{ points: EquityPoint[]; events: RiskEvent[]; color: string; pnlLevels: ThresholdLine[]; ddLevels: ThresholdLine[]; height?: number; windowMin?: number }>(),
  { height: 520, windowMin: 0 },
)

const LEVEL_COLOR: Record<string, string> = { ALERT: '#c98500', RESTRICT: STATUS_COLORS.serious, CIRCUIT_BREAK: STATUS_COLORS.critical, LIQUIDATE: '#8f1f1f' }
const SEV_COLOR: Record<string, string> = { INFO: '#909399', WARNING: '#eda100', HIGH: STATUS_COLORS.serious, CRITICAL: STATUS_COLORS.critical }

function thresholdMarks(levels: ThresholdLine[], scale: number) {
  return levels.map((l) => ({
    yAxis: l.threshold * scale,
    lineStyle: { color: LEVEL_COLOR[l.action] ?? CHART_INK.axis, type: 'dashed', width: 1 },
    label: { formatter: `${levelLabel(l.name)} ${fmtPct(l.threshold, 1)}`, color: CHART_INK.secondary, fontSize: 11, position: 'end', distance: 6 },
  }))
}

const option = computed(() => {
  const all = props.points
  const lastTs = all[all.length - 1]?.ts_ns ?? 0
  const pts = props.windowMin > 0 ? all.filter((p) => p.ts_ns >= lastTs - props.windowMin * 60e9) : all
  const eq = pts.map((p) => [p.ts_ns / 1e6, p.equity])
  const pnl = pts.map((p) => [p.ts_ns / 1e6, p.daily_pnl_pct_init * 100])
  const dd = pts.map((p) => [p.ts_ns / 1e6, p.intraday_dd * 100])
  // shaded periods of RESTRICTED / HALTED
  const areas: unknown[] = []
  let cur: { state: string; start: number } | null = null
  const close = (end: number) => {
    if (cur) {
      areas.push([{ xAxis: cur.start, itemStyle: { color: cur.state === 'HALTED' ? 'rgba(208,59,59,0.10)' : 'rgba(236,131,90,0.10)' }, label: { show: false } }, { xAxis: end }])
      cur = null
    }
  }
  for (const p of pts) {
    const t = p.ts_ns / 1e6
    const st = p.state
    if (st === 'HALTED' || st === 'RESTRICTED') {
      if (!cur || cur.state !== st) {
        close(t)
        cur = { state: st, start: t }
      }
    } else close(t)
  }
  const lastPt = pts[pts.length - 1]
  if (lastPt) close(lastPt.ts_ns / 1e6)
  // event markers on the P&L panel
  const firstT = pts[0]?.ts_ns ?? 0
  const markers = props.events
    .filter((e) => e.ts_ns >= firstT && ['ALERT_RAISED', 'ALERT_CLEARED', 'RESET', 'MANUAL_HALT', 'LIQUIDATION_PLAN', 'KILL_SWITCH_ON', 'KILL_SWITCH_OFF'].includes(e.kind))
    .map((e) => {
      let y = 0
      for (let i = pts.length - 1; i >= 0; i--) {
        const p = pts[i]
        if (p && p.ts_ns <= e.ts_ns) {
          y = p.daily_pnl_pct_init * 100
          break
        }
      }
      if (e.metric === 'account.daily_pnl_pct_init' && typeof e.value === 'number') y = e.value * 100
      return { value: [e.ts_ns / 1e6, y], ev: e, itemStyle: { color: SEV_COLOR[e.severity ?? 'INFO'] ?? '#909399', borderColor: '#fff', borderWidth: 2 } }
    })
  // P&L panel: always show warn / restrict / halt lines; the derisk line only once P&L gets near it
  const pnlVals = pnl.map((x) => x[1] as number)
  const pnlDataMin = Math.min(0, ...pnlVals)
  const pnlShown = props.pnlLevels.filter((l) => l.action !== 'LIQUIDATE' || pnlDataMin < l.threshold * 100 + 1)
  const pnlMin = Math.floor(Math.min(pnlDataMin, ...pnlShown.map((l) => l.threshold * 100)) * 2 - 0.5) / 2
  const pnlMax = Math.ceil(Math.max(0.5, ...pnlVals) * 2) / 2
  // drawdown panel: the first level always, deeper levels once the drawdown approaches them
  const ddVals = dd.map((x) => x[1] as number)
  const ddDataMax = Math.max(0, ...ddVals)
  const ddShown = props.ddLevels.filter((l, i) => i === 0 || l.threshold * 100 <= ddDataMax * 1.3)
  const ddMax = Math.ceil(Math.max(0.5, ddDataMax, ...ddShown.map((l) => l.threshold * 100)) * 1.1 * 2) / 2

  return {
    animation: false,
    textStyle: { fontFamily: FONT },
    grid: [
      { top: 28, height: '27%', left: 72, right: 96 },
      { top: '40%', height: '34%', left: 72, right: 96 },
      { top: '81%', height: '12%', left: 72, right: 96 },
    ],
    axisPointer: { link: [{ xAxisIndex: 'all' }], lineStyle: { color: CHART_INK.axis } },
    tooltip: {
      ...tooltipBase,
      trigger: 'axis',
      axisPointer: { type: 'line' },
      formatter: (params: { dataIndex: number; seriesName: string }[]) => {
        const i = params.find((p) => p.seriesName !== '事件')?.dataIndex
        const p = i === undefined ? undefined : pts[i]
        if (!p) return ''
        return (
          `<div style="font-weight:600;margin-bottom:4px">${fmtClockMs(p.ts_ns / 1e6, true)}${p.state ? ' · ' + esc(ACCOUNT_STATE_LABELS[p.state]) : ''}</div>` +
          tipRow(props.color, '动态权益', fmtMoney(p.equity)) +
          tipRow(props.color, '当日盈亏 / 初始资金', fmtPct(p.daily_pnl_pct_init)) +
          tipRow(props.color, '日内回撤', fmtPct(p.intraday_dd))
        )
      },
    },
    dataZoom: [{ type: 'inside', xAxisIndex: [0, 1, 2], filterMode: 'none' }],
    xAxis: [timeAxis({ gridIndex: 0, axisLabel: { show: false } }), timeAxis({ gridIndex: 1, axisLabel: { show: false } }), timeAxis({ gridIndex: 2 })],
    yAxis: [
      valueAxis({ gridIndex: 0, name: '动态权益', nameTextStyle: { color: CHART_INK.muted, fontSize: 11, align: 'left' }, axisLabel: { color: CHART_INK.muted, fontSize: 11, formatter: (v: number) => fmtMoneyCompact(v, 0) } }),
      valueAxis({ gridIndex: 1, name: '当日盈亏 %', nameTextStyle: { color: CHART_INK.muted, fontSize: 11, align: 'left' }, min: pnlMin, max: pnlMax, scale: false, axisLabel: { color: CHART_INK.muted, fontSize: 11, formatter: (v: number) => `${v.toFixed(1)}%` } }),
      valueAxis({ gridIndex: 2, name: '日内回撤 %', nameLocation: 'start', nameGap: 8, nameTextStyle: { color: CHART_INK.muted, fontSize: 11, align: 'left' }, min: 0, max: ddMax, scale: false, inverse: true, splitNumber: 2, axisLabel: { color: CHART_INK.muted, fontSize: 11, formatter: (v: number) => `${v.toFixed(1)}%` } }),
    ],
    series: [
      { id: 'equity', name: '动态权益', type: 'line', xAxisIndex: 0, yAxisIndex: 0, data: eq, showSymbol: false, lineStyle: { width: 2, color: props.color }, itemStyle: { color: props.color }, markArea: { silent: true, data: areas } },
      {
        id: 'pnl',
        name: '当日盈亏 %',
        type: 'line',
        xAxisIndex: 1,
        yAxisIndex: 1,
        data: pnl,
        showSymbol: false,
        lineStyle: { width: 2, color: props.color },
        itemStyle: { color: props.color },
        markLine: { symbol: 'none', silent: true, data: [{ yAxis: 0, lineStyle: { color: CHART_INK.axis, type: 'solid', width: 1 }, label: { show: false } }, ...thresholdMarks(pnlShown, 100)] },
        markArea: { silent: true, data: areas },
      },
      {
        id: 'dd',
        name: '日内回撤 %',
        type: 'line',
        xAxisIndex: 2,
        yAxisIndex: 2,
        data: dd,
        showSymbol: false,
        lineStyle: { width: 2, color: props.color },
        itemStyle: { color: props.color },
        areaStyle: { color: props.color, opacity: 0.08 },
        markLine: { symbol: 'none', silent: true, data: thresholdMarks(ddShown, 100) },
      },
      {
        id: 'events',
        name: '事件',
        type: 'scatter',
        xAxisIndex: 1,
        yAxisIndex: 1,
        symbolSize: 11,
        z: 5,
        data: markers,
        tooltip: {
          trigger: 'item',
          formatter: (p: { data: { ev: RiskEvent } }) => {
            const e = p.data.ev
            return `<div style="font-weight:600">${fmtClockMs(e.ts_ns / 1e6, true)} · ${esc(RISK_EVENT_LABELS[e.kind] ?? e.kind)}</div><div style="max-width:320px;white-space:normal">${esc(e.message ?? '')}</div>`
          },
        },
      },
    ],
  }
})
</script>

<template>
  <v-chart class="chart" :style="{ height: height + 'px' }" :option="option" autoresize :update-options="{ notMerge: true }" />
</template>
