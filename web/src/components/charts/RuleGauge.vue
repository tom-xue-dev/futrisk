<script setup lang="ts">
/**
 * One gauge per rule (worst scope): utilization against the rule's hard limit, with the
 * level zones on the track, the next level and the remaining headroom.
 */
import { computed } from 'vue'
import VChart from 'vue-echarts'
import type { RuleSmState } from '@/types'
import { FONT } from '@/charts'
import { isNum } from '@/utils/format'
import { BAND_STROKE, band, type LevelMark } from '@/utils/risk'
import { CHART_INK, SM_STATE_LABELS, levelLabel } from '@/utils/labels'

const props = withDefaults(
  defineProps<{
    title: string
    scope?: string
    valueText: string
    util: number | null
    marks: LevelMark[]
    activeLevel?: string | null
    smState?: RuleSmState
    nextText?: string
    headroomText?: string
  }>(),
  { scope: '', activeLevel: null, smState: 'OK', nextText: '', headroomText: '' },
)

const ZONE: Record<string, string> = { ALERT: '#fde7b0', RESTRICT: '#f5c2a6', CIRCUIT_BREAK: '#f0a8a8', LIQUIDATE: '#e08b8b' }

const maxU = computed(() => {
  const last = props.marks[props.marks.length - 1]?.position ?? 1
  return Math.min(2, Math.max(1.25, last * 1.08, (props.util ?? 0) * 1.05))
})

const option = computed(() => {
  const M = maxU.value
  const segs: [number, string][] = []
  const marks = props.marks
  const first = marks[0]
  segs.push([first ? Math.min(1, first.position / M) : 1, '#e9eef6'])
  marks.forEach((m, i) => {
    const next = marks[i + 1]
    segs.push([next ? Math.min(1, next.position / M) : 1, ZONE[m.action] ?? '#e9eef6'])
  })
  const u = isNum(props.util) ? props.util : 0
  const b = band(props.util)
  return {
    animation: false,
    textStyle: { fontFamily: FONT },
    series: [
      {
        type: 'gauge',
        startAngle: 210,
        endAngle: -30,
        min: 0,
        max: M,
        radius: '96%',
        center: ['50%', '58%'],
        splitNumber: 4,
        axisLine: { lineStyle: { width: 7, color: segs } },
        progress: { show: true, width: 7, roundCap: false, itemStyle: { color: BAND_STROKE[b] } },
        pointer: { show: true, length: '52%', width: 3, itemStyle: { color: CHART_INK.secondary } },
        anchor: { show: true, size: 6, itemStyle: { color: CHART_INK.secondary } },
        axisTick: { show: false },
        splitLine: { show: false },
        axisLabel: { show: false },
        title: { show: false },
        detail: {
          valueAnimation: false,
          offsetCenter: [0, '38%'],
          fontSize: 17,
          fontWeight: 600,
          color: CHART_INK.primary,
          formatter: () => (isNum(props.util) ? `${Math.round(props.util * 100)}%` : '—'),
        },
        data: [{ value: Math.min(u, M) }],
      },
    ],
  }
})

const tone = computed(() => band(props.util))
</script>

<template>
  <div class="gauge" :class="`b-${tone}`">
    <div class="head">
      <span class="title">{{ title }}</span>
      <span v-if="scope" class="scope">{{ scope }}</span>
    </div>
    <v-chart class="chart" style="height: 118px" :option="option" autoresize />
    <div class="val">{{ valueText }}</div>
    <div class="sub">
      <span v-if="activeLevel" class="lvl">生效：{{ levelLabel(activeLevel) }}<template v-if="smState !== 'ACTIVE'"> · {{ SM_STATE_LABELS[smState] }}</template></span>
      <span v-else-if="smState === 'REARMED'" class="lvl">{{ SM_STATE_LABELS.REARMED }}</span>
    </div>
    <div class="sub muted">{{ nextText }}</div>
    <div v-if="headroomText" class="sub muted">{{ headroomText }}</div>
    <div class="legend">
      <span v-for="m in marks" :key="m.name" class="lg"><i :style="{ background: ZONE[m.action] }" />{{ levelLabel(m.name) }} {{ Math.round(m.position * 100) }}%</span>
    </div>
  </div>
</template>

<style scoped>
.gauge {
  border: 1px solid var(--fr-line);
  border-radius: var(--fr-radius);
  padding: 8px 10px 10px;
  background: var(--fr-surface);
  min-width: 0;
}
.gauge.b-breach {
  border-color: #e7a3a3;
  background: #fffafa;
}
.head {
  display: flex;
  align-items: baseline;
  gap: 6px;
}
.title {
  font-weight: 600;
  font-size: 13px;
}
.scope {
  font-size: 12px;
  color: var(--fr-ink-3);
}
.val {
  text-align: center;
  font-size: 13px;
  font-variant-numeric: tabular-nums;
  margin-top: -6px;
}
.sub {
  text-align: center;
  font-size: 12px;
  min-height: 16px;
}
.lvl {
  color: #a8481f;
  font-weight: 500;
}
.legend {
  display: flex;
  flex-wrap: wrap;
  gap: 2px 8px;
  justify-content: center;
  margin-top: 4px;
  font-size: 11px;
  color: var(--fr-ink-3);
}
.lg i {
  display: inline-block;
  width: 8px;
  height: 8px;
  border-radius: 2px;
  margin-right: 3px;
  vertical-align: -1px;
}
</style>
