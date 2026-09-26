<script setup lang="ts">
/**
 * Data-source trust state over time (TRUSTED / PROBATION / EXCLUDED) reconstructed from
 * `md.event` SOURCE_STATE transitions plus the current `md.status`; gap open / filled
 * events are marked on the source's row.
 */
import { computed } from 'vue'
import VChart from 'vue-echarts'
import type { MdEvent, MdStatus, SourceState } from '@/types'
import { esc, FONT, timeAxis, tooltipBase } from '@/charts'
import { fmtClockMs } from '@/utils/format'
import { CHART_INK, MD_EVENT_LABELS, SOURCE_LABELS, SOURCE_STATE_LABELS, STATUS_COLORS } from '@/utils/labels'

const props = withDefaults(defineProps<{ status: MdStatus | null; events: MdEvent[]; fromMs: number; toMs: number }>(), {})

const STATE_COLOR: Record<SourceState, string> = { TRUSTED: STATUS_COLORS.good, PROBATION: STATUS_COLORS.warning, EXCLUDED: STATUS_COLORS.critical }

interface Seg {
  source: string
  state: SourceState
  from: number
  to: number
}

const model = computed(() => {
  const sources = (props.status?.sources.map((s) => s.source as string) ?? ['FEED_A', 'FEED_B']).filter((v, i, a) => a.indexOf(v) === i)
  for (const e of props.events) if (e.kind === 'SOURCE_STATE' && e.source && !sources.includes(e.source)) sources.push(e.source)
  const segs: Seg[] = []
  for (const src of sources) {
    const evs = props.events.filter((e) => e.kind === 'SOURCE_STATE' && e.source === src).sort((a, b) => a.ts_ns - b.ts_ns)
    const cur = props.status?.sources.find((s) => s.source === src)?.state
    let state: SourceState = ((evs[0]?.from_state as SourceState | undefined) ?? cur ?? 'TRUSTED') as SourceState
    let t = props.fromMs
    for (const e of evs) {
      const at = e.ts_ns / 1e6
      if (at > props.fromMs) {
        segs.push({ source: src, state, from: t, to: Math.min(at, props.toMs) })
        t = at
      }
      state = (e.to_state as SourceState) ?? state
    }
    segs.push({ source: src, state: cur ?? state, from: t, to: props.toMs })
  }
  const marks = props.events
    .filter((e) => (e.kind === 'GAP_OPEN' || e.kind === 'GAP_FILLED' || e.kind === 'GAP_UNRECOVERABLE' || e.kind === 'BAD_TICK' || e.kind === 'MD_DOWN') && e.ts_ns / 1e6 >= props.fromMs)
    .map((e) => ({ value: [e.ts_ns / 1e6, Math.max(0, sources.indexOf(e.source ?? 'FEED_A'))], ev: e }))
  return { sources, segs, marks }
})

const option = computed(() => {
  const { sources, segs, marks } = model.value
  return {
    animation: false,
    textStyle: { fontFamily: FONT },
    grid: { top: 10, left: 110, right: 16, bottom: 28 },
    tooltip: {
      ...tooltipBase,
      trigger: 'item',
      formatter: (p: { seriesType: string; data: { seg?: Seg; ev?: MdEvent } }) => {
        if (p.data.seg) {
          const s = p.data.seg
          return `${esc(SOURCE_LABELS[s.source] ?? s.source)}<br/><b>${esc(SOURCE_STATE_LABELS[s.state])}</b> ${fmtClockMs(s.from, true)} – ${fmtClockMs(s.to, true)}`
        }
        const e = p.data.ev as MdEvent
        const g = e.gap
        return `<b>${esc(MD_EVENT_LABELS[e.kind])}</b> ${fmtClockMs(e.ts_ns / 1e6, true)}${g ? `<br/>${esc(g.gap_id)}<br/>补齐 ${g.filled ?? 0}，重复 ${g.duplicates ?? 0}，不可恢复 ${g.unrecoverable ?? 0}` : ''}${e.instrument_id ? '<br/>' + esc(e.instrument_id) : ''}`
      },
    },
    xAxis: timeAxis({ min: props.fromMs, max: props.toMs }),
    yAxis: { type: 'category', data: sources.map((s) => SOURCE_LABELS[s] ?? s), inverse: true, axisLine: { show: false }, axisTick: { show: false }, axisLabel: { color: CHART_INK.primary, fontSize: 12 } },
    series: [
      {
        type: 'custom',
        renderItem: (_params: unknown, api: { value: (i: number) => number; coord: (v: [number, number]) => [number, number]; size: (v: [number, number]) => [number, number] }) => {
          const y = api.value(0)
          const start = api.coord([api.value(1), y])
          const end = api.coord([api.value(2), y])
          const h = api.size([0, 1])[1] * 0.46
          return { type: 'rect', shape: { x: start[0], y: start[1] - h / 2, width: Math.max(1, end[0] - start[0] - 1), height: h, r: 3 }, style: { fill: api.value(3) === 0 ? STATE_COLOR.TRUSTED : api.value(3) === 1 ? STATE_COLOR.PROBATION : STATE_COLOR.EXCLUDED } }
        },
        encode: { x: [1, 2], y: 0 },
        data: segs.map((s) => ({ value: [sources.indexOf(s.source), s.from, s.to, s.state === 'TRUSTED' ? 0 : s.state === 'PROBATION' ? 1 : 2], seg: s })),
      },
      {
        type: 'scatter',
        symbol: 'triangle',
        symbolSize: 9,
        symbolOffset: [0, -14],
        itemStyle: { color: CHART_INK.secondary, borderColor: '#fff', borderWidth: 1 },
        data: marks.map((m) => ({ value: [m.value[0], sources[m.value[1] as number] ? (SOURCE_LABELS[sources[m.value[1] as number] as string] ?? sources[m.value[1] as number]) : ''], ev: m.ev })),
      },
    ],
  }
})
</script>

<template>
  <div>
    <v-chart class="chart" style="height: 150px" :option="option" autoresize :update-options="{ notMerge: true }" />
    <div class="legend">
      <span><i :style="{ background: STATE_COLOR.TRUSTED }" />{{ SOURCE_STATE_LABELS.TRUSTED }}</span>
      <span><i :style="{ background: STATE_COLOR.PROBATION }" />{{ SOURCE_STATE_LABELS.PROBATION }}</span>
      <span><i :style="{ background: STATE_COLOR.EXCLUDED }" />{{ SOURCE_STATE_LABELS.EXCLUDED }}</span>
      <span class="muted">▲ 缺口 / 补数 / 坏点 / 中断事件</span>
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
  border-radius: 2px;
  margin-right: 4px;
  vertical-align: -1px;
}
</style>
