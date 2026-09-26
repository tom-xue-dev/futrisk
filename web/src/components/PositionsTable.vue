<script setup lang="ts">
/** PositionView table: today / yesterday lots, closable (SHFE split), MTM vs FIFO P&L, margin. */
import type { PositionView } from '@/types'
import { fmtMoney, fmtPct, fmtPrice, fmtSignedMoney, signClass } from '@/utils/format'
import { DIRECTION_LABELS, PRODUCT_LABELS } from '@/utils/labels'
import StatusPill from './StatusPill.vue'

defineProps<{ rows: PositionView[] }>()
</script>

<template>
  <el-table :data="rows" size="small" empty-text="无持仓" :default-sort="{ prop: 'notional', order: 'descending' }">
    <el-table-column label="合约" min-width="110" fixed>
      <template #default="{ row }">
        <b>{{ row.instrument_id }}</b>
        <span class="muted small">{{ PRODUCT_LABELS[row.product] ?? '' }}</span>
      </template>
    </el-table-column>
    <el-table-column label="方向" width="62" align="center">
      <template #default="{ row }"><span :class="row.direction === 'LONG' ? 'up' : 'down'">{{ DIRECTION_LABELS[row.direction] }}</span></template>
    </el-table-column>
    <el-table-column label="今仓" prop="td_qty" width="64" align="right" />
    <el-table-column label="昨仓" prop="yd_qty" width="64" align="right" />
    <el-table-column label="可平" min-width="150" align="right">
      <template #default="{ row }">
        <span>{{ row.closable_qty }}</span>
        <span v-if="row.exchange === 'SHFE' || row.exchange === 'INE'" class="muted small">（今 {{ row.closable_td_qty }} / 昨 {{ row.closable_yd_qty }}）</span>
      </template>
    </el-table-column>
    <el-table-column label="开仓均价" min-width="92" align="right">
      <template #default="{ row }">{{ fmtPrice(row.avg_open_price) }}</template>
    </el-table-column>
    <el-table-column label="持仓均价（盯市）" min-width="112" align="right">
      <template #default="{ row }">{{ fmtPrice(row.basis_avg_price) }}</template>
    </el-table-column>
    <el-table-column label="最新价" min-width="84" align="right">
      <template #default="{ row }">{{ fmtPrice(row.last) }}</template>
    </el-table-column>
    <el-table-column label="盯市盈亏" min-width="110" align="right">
      <template #default="{ row }"><span :class="signClass(row.position_pnl)">{{ fmtSignedMoney(row.position_pnl) }}</span></template>
    </el-table-column>
    <el-table-column label="逐笔浮盈" min-width="110" align="right">
      <template #default="{ row }"><span :class="signClass(row.float_pnl_fifo)">{{ fmtSignedMoney(row.float_pnl_fifo) }}</span></template>
    </el-table-column>
    <el-table-column label="保证金" min-width="108" align="right">
      <template #default="{ row }">{{ fmtMoney(row.margin) }}</template>
    </el-table-column>
    <el-table-column label="名义价值" prop="notional" sortable min-width="118" align="right">
      <template #default="{ row }">{{ fmtMoney(row.notional, 0) }}</template>
    </el-table-column>
    <el-table-column label="占权益" width="76" align="right">
      <template #default="{ row }">{{ fmtPct(row.pct_equity, 1) }}</template>
    </el-table-column>
    <el-table-column label="封板" width="72" align="center">
      <template #default="{ row }">
        <StatusPill v-if="row.limit_locked" tone="critical" label="封板" title="处于涨跌停封板：只能在事前控制，平仓可能无法成交" />
        <span v-else class="muted">—</span>
      </template>
    </el-table-column>
  </el-table>
</template>

<style scoped>
.small {
  font-size: 12px;
  margin-left: 4px;
}
</style>
