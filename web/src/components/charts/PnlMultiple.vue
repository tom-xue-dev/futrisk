<script setup lang="ts">
/**
 * Small multiple: one account's intraday daily-P&L % (of initial capital). All panels
 * share the same y range (set by the parent) so they compare honestly; the account's
 * own alert / restrict / breaker lines are drawn.
 */
import { computed } from 'vue'
import VChart from 'vue-echarts'
import type { EquityPoint } from '@/types'
import { FONT, timeAxis, tipRow, tooltipBase, valueAxis } from '@/charts'
import { fmtClockMs, fmtPct } from '@/utils/format'
import { CHART_INK, STATUS_COLORS } from '@/utils/labels'
import type { ThresholdLine } from './types'

const props = withDefaults(defineProps<{ points: EquityPoint[]; color: string; yMin: number; yMax: number; levels: ThresholdLine[]; xMin?: number; xMax?: number; height?: number }>(), {
  xMin: undefined,
  xMax: undefined,
  height: 150,
})

const LEVEL_COLOR: Record<string, string> = { ALERT: '#c98500', RESTRICT: STATUS_COLORS.serious, CIRCUIT_BREAK: STATUS_COLORS.critical }

const option = computed(() => {
  const data = props.points.map((p) => [p.ts_ns / 1e6, p.daily_pnl_pct_init * 100])
  return {
    animation: false,
    textStyle: { fontFamily: FONT },
    grid: { top: 8, left: 44, right: 22, bottom: 22 },
    tooltip: {
      ...tooltipBase,
      trigger: 'axis',
      formatter: (params: { data: [number, number] }[]) => {
        const d = params[0]?.data
        return d ? `<div style="font-weight:600">${fmtClockMs(d[0], true)}</div>${tipRow(props.color, '当日盈亏 / 初始资金', fmtPct(d[1] / 100))}` : ''
      },
    },
    xAxis: timeAxis({ min: props.xMin, max: props.xMax, splitNumber: 3 }),
    yAxis: valueAxis({ min: props.yMin, max: props.yMax, scale: false, splitNumber: 3, axisLabel: { color: CHART_INK.muted, fontSize: 10, formatter: (v: number) => `${v.toFixed(0)}%` } }),
    series: [
      {
        type: 'line',
        data,
        showSymbol: false,
        lineStyle: { width: 2, color: props.color },
        itemStyle: { color: props.color },
        areaStyle: { color: props.color, opacity: 0.08, origin: 0 },
        markLine: {
          symbol: 'none',
          silent: true,
          label: { show: false },
          data: [
            { yAxis: 0, lineStyle: { color: CHART_INK.axis, type: 'solid', width: 1 } },
            ...props.levels.filter((l) => l.action !== 'LIQUIDATE').map((l) => ({ yAxis: l.threshold * 100, lineStyle: { color: LEVEL_COLOR[l.action] ?? CHART_INK.axis, type: 'dashed', width: 1 } })),
          ],
        },
      },
    ],
  }
})
</script>

<template>
  <v-chart class="chart" :style="{ height: height + 'px' }" :option="option" autoresize :update-options="{ notMerge: true }" />
</template>
