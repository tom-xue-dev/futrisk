<script setup lang="ts">
/** OrderUpdate table (WS `orders:{id}` + REST), reject codes shown verbatim with meaning and evidence. */
import { computed, ref } from 'vue'
import type { OrderStatus, OrderUpdate } from '@/types'
import { fmtPrice, fmtTime, shortId } from '@/utils/format'
import { DIRECTION_LABELS, OFFSET_LABELS, ORDER_SOURCE_LABELS, ORDER_STATUS_LABELS, codeLabel, type StatusTone } from '@/utils/labels'
import StatusPill from './StatusPill.vue'

const props = defineProps<{ rows: OrderUpdate[] }>()

const filter = ref<'ALL' | 'REJECTED' | 'OPEN'>('ALL')
const OPEN_STATES: OrderStatus[] = ['ROUTING', 'ROUTED', 'ACCEPTED', 'PARTIALLY_FILLED', 'CANCEL_PENDING', 'UNKNOWN']

const shown = computed(() => {
  if (filter.value === 'REJECTED') return props.rows.filter((o) => o.status === 'REJECTED' || o.status === 'BROKER_REJECTED')
  if (filter.value === 'OPEN') return props.rows.filter((o) => OPEN_STATES.includes(o.status))
  return props.rows
})

const rejectCount = computed(() => props.rows.filter((o) => o.status === 'REJECTED' || o.status === 'BROKER_REJECTED').length)

function tone(s: OrderStatus): StatusTone {
  switch (s) {
    case 'FILLED':
      return 'good'
    case 'REJECTED':
    case 'BROKER_REJECTED':
      return 'critical'
    case 'PARTIALLY_FILLED':
      return 'warning'
    case 'UNKNOWN':
      return 'serious'
    default:
      return 'neutral'
  }
}

function detailText(o: OrderUpdate): string {
  if (!o.reject_detail) return ''
  return Object.entries(o.reject_detail)
    .map(([k, v]) => `${k}=${typeof v === 'object' ? JSON.stringify(v) : String(v)}`)
    .join('  ')
}
</script>

<template>
  <div>
    <div class="toolbar">
      <el-radio-group v-model="filter" size="small">
        <el-radio-button value="ALL">全部 {{ rows.length }}</el-radio-button>
        <el-radio-button value="REJECTED">拒单 {{ rejectCount }}</el-radio-button>
        <el-radio-button value="OPEN">未终结</el-radio-button>
      </el-radio-group>
    </div>
    <el-table :data="shown" size="small" empty-text="无委托" max-height="460">
      <el-table-column label="时间" width="84">
        <template #default="{ row }"><span class="mono">{{ fmtTime(row.created_ns) }}</span></template>
      </el-table-column>
      <el-table-column label="委托号" width="120">
        <template #default="{ row }"><span class="mono" :title="row.client_order_id">{{ shortId(row.client_order_id) }}</span></template>
      </el-table-column>
      <el-table-column label="合约" prop="instrument_id" width="84" />
      <el-table-column label="买卖 / 开平" width="100">
        <template #default="{ row }">
          <span :class="row.direction === 'BUY' ? 'up' : 'down'">{{ DIRECTION_LABELS[row.direction] }}</span>
          {{ OFFSET_LABELS[row.offset] ?? row.offset }}
        </template>
      </el-table-column>
      <el-table-column label="价格" width="84" align="right">
        <template #default="{ row }">{{ fmtPrice(row.price) }}</template>
      </el-table-column>
      <el-table-column label="数量 / 成交" width="92" align="right">
        <template #default="{ row }">{{ row.qty }} / {{ row.filled_qty ?? 0 }}</template>
      </el-table-column>
      <el-table-column label="状态" width="92">
        <template #default="{ row }"><StatusPill :tone="tone(row.status)" :label="ORDER_STATUS_LABELS[row.status as OrderStatus] ?? row.status" /></template>
      </el-table-column>
      <el-table-column label="拒单码 / 原因" min-width="260">
        <template #default="{ row }">
          <template v-if="row.reject_code">
            <el-tooltip :content="detailText(row as OrderUpdate) || row.reject_code" placement="top" :disabled="!row.reject_detail">
              <span class="code mono">{{ row.reject_code }}</span>
            </el-tooltip>
            <span class="meaning">{{ codeLabel(row.reject_code) }}</span>
          </template>
          <span v-else-if="row.reason" class="muted">{{ row.reason }}</span>
          <span v-else class="muted">—</span>
        </template>
      </el-table-column>
      <el-table-column label="来源" width="72">
        <template #default="{ row }">{{ ORDER_SOURCE_LABELS[row.source] ?? row.source }}</template>
      </el-table-column>
    </el-table>
  </div>
</template>

<style scoped>
.toolbar {
  margin-bottom: 8px;
}
.code {
  color: #b42323;
  background: #fdf1f1;
  border: 1px solid #f1c4c4;
  border-radius: 4px;
  padding: 0 4px;
  margin-right: 6px;
}
.meaning {
  font-size: 12px;
  color: var(--fr-ink-2);
}
</style>
