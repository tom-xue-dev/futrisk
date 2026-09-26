<script setup lang="ts">
/** Utilization meter: fill carries severity (accent -> warning -> danger), value printed beside it. */
import { computed } from 'vue'
import { fmtPct, isNum } from '@/utils/format'
import { BAND_LABELS, BAND_STROKE, band } from '@/utils/risk'

const props = withDefaults(defineProps<{ value: number | null | undefined; max?: number; label?: string; width?: string }>(), { max: 1.2, label: '', width: '100%' })

const b = computed(() => band(props.value))
const pct = computed(() => (isNum(props.value) ? Math.min(1, Math.max(0, props.value / props.max)) * 100 : 0))
const limitPos = computed(() => (1 / props.max) * 100)
</script>

<template>
  <div class="util" :style="{ width }" :title="`${label ? label + ' · ' : ''}${fmtPct(value, 1)} · ${BAND_LABELS[b]}`">
    <div class="track">
      <div class="fill" :style="{ width: pct + '%', background: BAND_STROKE[b] }" />
      <div class="limit" :style="{ left: limitPos + '%' }" />
    </div>
    <span class="txt">{{ isNum(value) ? fmtPct(value, 0) : '—' }}</span>
    <span v-if="label" class="lbl">{{ label }}</span>
  </div>
</template>

<style scoped>
.util {
  display: flex;
  align-items: center;
  gap: 8px;
  min-width: 0;
}
.track {
  position: relative;
  flex: 1;
  min-width: 48px;
  height: 6px;
  border-radius: 3px;
  background: #e9eef6;
}
.fill {
  height: 100%;
  border-radius: 3px;
  transition: width 0.3s ease;
}
.limit {
  position: absolute;
  top: -3px;
  width: 1px;
  height: 12px;
  background: #52514e;
  opacity: 0.55;
}
.txt {
  font-variant-numeric: tabular-nums;
  font-size: 12px;
  min-width: 36px;
  text-align: right;
}
.lbl {
  font-size: 12px;
  color: var(--fr-ink-3);
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  max-width: 96px;
}
</style>
