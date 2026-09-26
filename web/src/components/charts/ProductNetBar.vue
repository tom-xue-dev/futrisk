<script setup lang="ts">
/** Firm net notional by product (diverging: long = red, short = blue), values printed at the bar ends. */
import { computed } from 'vue'
import VChart from 'vue-echarts'
import type { ProductNet } from '@/types'
import { esc, FONT, tooltipBase, valueAxis } from '@/charts'
import { DIVERGING } from '@/utils/compare'
import { fmtMoneyCompact } from '@/utils/format'
import { CHART_INK, PRODUCT_LABELS } from '@/utils/labels'

const props = defineProps<{ items: ProductNet[] }>()

const rows = computed(() => [...props.items].sort((a, b) => Math.abs(b.net_notional) - Math.abs(a.net_notional)).slice(0, 12))
const height = computed(() => 40 + rows.value.length * 26)

const option = computed(() => ({
  animation: false,
  textStyle: { fontFamily: FONT },
  grid: { top: 6, left: 88, right: 70, bottom: 22 },
  tooltip: {
    ...tooltipBase,
    trigger: 'item',
    formatter: (p: { data: { item: ProductNet } }) => {
      const it = p.data.item
      return `<b>${esc(it.product)} ${esc(PRODUCT_LABELS[it.product] ?? '')}</b><br/>净名义 ${esc(fmtMoneyCompact(it.net_notional, 2, true))}<br/>总名义 ${esc(fmtMoneyCompact(it.gross_notional))}<br/>净手数 ${it.net_lots} · ${it.accounts} 个账户`
    },
  },
  xAxis: valueAxis({ scale: false, axisLabel: { color: CHART_INK.muted, fontSize: 10, formatter: (v: number) => fmtMoneyCompact(v, 0) } }),
  yAxis: { type: 'category', inverse: true, data: rows.value.map((r) => `${r.product} ${PRODUCT_LABELS[r.product] ?? ''}`), axisLine: { lineStyle: { color: CHART_INK.axis } }, axisTick: { show: false }, axisLabel: { color: CHART_INK.primary, fontSize: 12 } },
  series: [
    {
      type: 'bar',
      barMaxWidth: 16,
      data: rows.value.map((r) => ({
        value: r.net_notional,
        item: r,
        itemStyle: { color: r.net_notional >= 0 ? DIVERGING.long : DIVERGING.short, borderRadius: r.net_notional >= 0 ? [0, 4, 4, 0] : [4, 0, 0, 4] },
        label: { show: true, position: r.net_notional >= 0 ? 'right' : 'left', color: CHART_INK.secondary, fontSize: 11, formatter: fmtMoneyCompact(r.net_notional, 1, true) },
      })),
    },
  ],
}))
</script>

<template>
  <v-chart class="chart" :style="{ height: height + 'px' }" :option="option" autoresize :update-options="{ notMerge: true }" />
</template>
