<script setup lang="ts">
/** Status pill: status color is always paired with an icon and a text label. */
import { computed } from 'vue'
import { CircleCheckFilled, CircleCloseFilled, InfoFilled, RemoveFilled, WarningFilled } from '@element-plus/icons-vue'
import type { StatusTone } from '@/utils/labels'

const props = withDefaults(defineProps<{ tone: StatusTone; label: string; size?: 'sm' | 'lg'; title?: string }>(), { size: 'sm', title: '' })

const icon = computed(() => {
  switch (props.tone) {
    case 'good':
      return CircleCheckFilled
    case 'warning':
      return WarningFilled
    case 'serious':
      return RemoveFilled
    case 'critical':
      return CircleCloseFilled
    default:
      return InfoFilled
  }
})
</script>

<template>
  <span class="pill" :class="[tone, size === 'lg' ? 'lg' : '']" :title="title || label">
    <el-icon><component :is="icon" /></el-icon>
    <span>{{ label }}</span>
  </span>
</template>
