<script setup lang="ts">
/**
 * "Shard down / data stale" badge for an account whose owning risk shard is not alive:
 * the gateway still serves its last snapshot (`stale: true`, `age_ms`), which must not
 * look like live data.
 */
import { computed } from 'vue'
import { Clock } from '@element-plus/icons-vue'
import { fmtDuration } from '@/utils/format'

const props = withDefaults(defineProps<{ ageMs?: number | null; compact?: boolean }>(), { ageMs: null, compact: false })

const text = computed(() => {
  const age = typeof props.ageMs === 'number' && props.ageMs >= 0 ? fmtDuration(props.ageMs) : ''
  if (props.compact) return age ? `陈旧 ${age}` : '陈旧'
  return age ? `分片离线 · 数据陈旧 ${age}` : '分片离线 · 数据陈旧'
})
</script>

<template>
  <span class="stale" :title="`所属风控分片离线：显示的是最后一次快照${ageMs ? '（' + fmtDuration(ageMs) + ' 前）' : ''}，不是实时数据`">
    <el-icon><Clock /></el-icon>{{ text }}
  </span>
</template>

<style scoped>
.stale {
  display: inline-flex;
  align-items: center;
  gap: 3px;
  padding: 0 7px;
  border-radius: 999px;
  border: 1px dashed #c98a07;
  color: #8a5d00;
  background: #fffaf0;
  font-size: 12px;
  line-height: 18px;
  white-space: nowrap;
}
</style>
