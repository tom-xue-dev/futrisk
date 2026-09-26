<script setup lang="ts">
/** Account risk state (NORMAL / WARNING / RESTRICTED / HALTED); HALTED shows latched vs pending (01 §8.14). */
import { computed } from 'vue'
import type { AccountState } from '@/types'
import { ACCOUNT_STATE_LABELS, ACCOUNT_STATE_TONE } from '@/utils/labels'
import StatusPill from './StatusPill.vue'

const props = withDefaults(defineProps<{ state: AccountState | null | undefined; latched?: boolean; size?: 'sm' | 'lg' }>(), { latched: false, size: 'sm' })

const label = computed(() => {
  if (!props.state) return '—'
  const base = ACCOUNT_STATE_LABELS[props.state]
  if (props.state !== 'HALTED') return base
  return `${base} · ${props.latched ? '已锁存' : '待确认'}`
})

const title = computed(() => {
  if (props.state !== 'HALTED') return label.value
  return props.latched ? '熔断已确认并锁存：只允许减仓，需风控经理重置或换日解除' : '首次越过熔断线：已禁开并撤销开仓挂单，等待确认（2 个 LIVE 状态或 1s）'
})
</script>

<template>
  <StatusPill v-if="state" :tone="ACCOUNT_STATE_TONE[state]" :label="label" :size="size" :title="title" />
  <StatusPill v-else tone="neutral" label="—" :size="size" />
</template>
