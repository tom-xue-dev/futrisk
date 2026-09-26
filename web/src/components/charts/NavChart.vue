<script setup lang="ts">
/** NAV (perf equity / initial capital) per account over the settlement-history window. */
import { computed } from 'vue'
import VChart from 'vue-echarts'
import type { AccountHistoryStats } from '@/utils/stats'
import { esc, FONT, tipRow, tooltipBase, valueAxis } from '@/charts'
import { fmtTd } from '@/utils/format'
import { accountColor, CHART_INK } from '@/utils/labels'

const props = withDefaults(defineProps<{ stats: AccountHistoryStats[]; height?: number }>(), { height: 280 })

const option = computed(() => {
  const days = [...new Set(props.stats.flatMap((s) => s.navs.map((n) => n.td)))].sort()
  return {
    animation: false,
    textStyle: { fontFamily: FONT },
    grid: { top: 36, left: 56, right: 64, bottom: 28 },
    legend: { top: 4, left: 0, icon: 'roundRect', itemWidth: 14, itemHeight: 3, textStyle: { color: CHART_INK.secondary, fontSize: 12 } },
    tooltip: {
      ...tooltipBase,
      trigger: 'axis',
      formatter: (params: { seriesName: string; color: string; data: number | null; axisValue: string }[]) => {
        const head = `<div style="font-weight:600;margin-bottom:4px">${esc(fmtTd(params[0]?.axisValue))}</div>`
        return head + params.filter((p) => typeof p.data === 'number').map((p) => tipRow(p.color, p.seriesName, (p.data as number).toFixed(4))).join('')
      },
    },
    xAxis: { type: 'category', data: days, boundaryGap: false, axisLine: { lineStyle: { color: CHART_INK.axis } }, axisTick: { show: false }, axisLabel: { color: CHART_INK.muted, fontSize: 11, formatter: (v: string) => `${v.slice(4, 6)}-${v.slice(6, 8)}` } },
    yAxis: valueAxis({ axisLabel: { color: CHART_INK.muted, fontSize: 11, formatter: (v: number) => v.toFixed(2) } }),
    series: props.stats.map((s) => {
      const map = new Map(s.navs.map((n) => [n.td, n.nav]))
      const color = accountColor(s.account_id)
      return {
        name: s.account_id,
        type: 'line',
        data: days.map((d) => map.get(d) ?? null),
        showSymbol: false,
        connectNulls: true,
        lineStyle: { width: 2, color },
        itemStyle: { color },
        endLabel: { show: true, formatter: '{a}', color: CHART_INK.secondary, fontSize: 11 },
      }
    }),
  }
})
</script>

<template>
  <v-chart class="chart" :style="{ height: height + 'px' }" :option="option" autoresize :update-options="{ notMerge: true }" />
</template>
