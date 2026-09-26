<script setup lang="ts">
/**
 * Account × rule heat map of utilization (value ÷ hard limit). Cell fills are the
 * discrete status bands (neutral < 60%, then warning / serious tints, critical ≥ 100%)
 * and every cell prints its value, so color is never the only channel.
 * Clicking a cell drills down to the account page.
 */
import { computed } from 'vue'
import VChart from 'vue-echarts'
import type { CompareView } from '@/types'
import { esc, FONT, tooltipBase } from '@/charts'
import { textOn } from '@/utils/compare'
import { isNum } from '@/utils/format'
import { BAND_CUTS, BAND_FILL, BAND_LABELS, fmtMetric } from '@/utils/risk'
import { ACCOUNT_NAMES, CHART_INK, levelLabel, ruleLabel, scopeLabel } from '@/utils/labels'

const props = defineProps<{ view: CompareView }>()
const emit = defineEmits<{ drill: [accountId: string, ruleId: string] }>()

const BANDS = ['ok', 'watch', 'warn', 'breach', 'na'] as const
const height = computed(() => 56 + props.view.accounts.length * 44)

const option = computed(() => {
  const accounts = [...props.view.accounts]
  const rules = props.view.rules
  const data = props.view.matrix.map((c) => {
    const x = rules.indexOf(c.rule_id)
    const y = accounts.indexOf(c.account_id)
    const fill = BAND_FILL[c.band]
    return {
      value: [x, y, isNum(c.utilization) ? c.utilization : -1],
      cell: c,
      label: { color: textOn(fill) },
    }
  })
  return {
    animation: false,
    textStyle: { fontFamily: FONT },
    grid: { top: 8, left: 132, right: 12, bottom: 48 },
    // ECharts heat maps require a visualMap; the discrete bands are the legend below
    visualMap: {
      show: false,
      type: 'piecewise',
      dimension: 2,
      pieces: [
        { lt: 0, color: BAND_FILL.na },
        { gte: 0, lt: BAND_CUTS.watch, color: BAND_FILL.ok },
        { gte: BAND_CUTS.watch, lt: BAND_CUTS.warn, color: BAND_FILL.watch },
        { gte: BAND_CUTS.warn, lt: BAND_CUTS.breach, color: BAND_FILL.warn },
        { gte: BAND_CUTS.breach, color: BAND_FILL.breach },
      ],
    },
    tooltip: {
      ...tooltipBase,
      trigger: 'item',
      formatter: (p: { data: { cell: CompareView['matrix'][number] } }) => {
        const c = p.data.cell
        return (
          `<div style="font-weight:600">${esc(c.account_id)} · ${esc(ruleLabel(c.rule_id))}</div>` +
          `<div>范围：${esc(scopeLabel(c.scope_key))}</div>` +
          `<div>当前值：<b>${esc(fmtMetric(c.metric, c.value))}</b></div>` +
          `<div>限额（硬线）：${esc(fmtMetric(c.metric, c.limit))}</div>` +
          `<div>使用率：<b>${isNum(c.utilization) ? Math.round(c.utilization * 100) + '%' : '—'}</b> · ${esc(BAND_LABELS[c.band])}</div>` +
          (c.active_level ? `<div>生效级别：${esc(levelLabel(c.active_level))}</div>` : '') +
          `<div style="color:${CHART_INK.muted};margin-top:4px">点击查看账户详情</div>`
        )
      },
    },
    xAxis: {
      type: 'category',
      data: rules.map((r) => ruleLabel(r)),
      axisLine: { show: false },
      axisTick: { show: false },
      axisLabel: { color: CHART_INK.secondary, fontSize: 12, interval: 0, rotate: rules.length > 8 ? 30 : 0 },
      splitArea: { show: false },
    },
    yAxis: {
      type: 'category',
      data: accounts,
      inverse: true,
      axisLine: { show: false },
      axisTick: { show: false },
      axisLabel: { color: CHART_INK.primary, fontSize: 12, formatter: (v: string) => `${v}  ${ACCOUNT_NAMES[v] ?? ''}` },
    },
    series: [
      {
        type: 'heatmap',
        data,
        itemStyle: { borderColor: '#ffffff', borderWidth: 2, borderRadius: 3 },
        label: {
          show: true,
          fontSize: 12,
          fontWeight: 500,
          formatter: (p: { value: [number, number, number] }) => (p.value[2] < 0 ? '—' : `${Math.round(p.value[2] * 100)}%`),
        },
        emphasis: { itemStyle: { borderColor: '#1f2329', borderWidth: 1.5 } },
      },
    ],
  }
})

function onClick(params: unknown): void {
  const c = (params as { data?: { cell?: { account_id: string; rule_id: string } } | null }).data?.cell
  if (c) emit('drill', c.account_id, c.rule_id)
}
</script>

<template>
  <div>
    <v-chart class="chart" :style="{ height: height + 'px', cursor: 'pointer' }" :option="option" autoresize @click="onClick" />
    <div class="legend">
      <span v-for="b in BANDS" :key="b" class="item"><i :style="{ background: BAND_FILL[b] }" />{{ BAND_LABELS[b] }}</span>
      <span class="muted note">使用率 = 数值 ÷ 硬限额（统一成"越大越危险"后），账户取各范围中最差的一个</span>
    </div>
  </div>
</template>

<style scoped>
.legend {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 4px 14px;
  font-size: 12px;
  color: var(--fr-ink-2);
  margin-top: 2px;
}
.legend i {
  display: inline-block;
  width: 12px;
  height: 12px;
  border-radius: 3px;
  margin-right: 4px;
  vertical-align: -2px;
  border: 1px solid rgba(11, 11, 11, 0.08);
}
.note {
  font-size: 12px;
}
</style>
