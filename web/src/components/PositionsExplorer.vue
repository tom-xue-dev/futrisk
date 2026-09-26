<script setup lang="ts">
/**
 * Cross-account positions: GET /positions?account=&product=&exchange=&sector=&group_by=
 * → `{group_by, items, groups, accounts_missing}`. Rows come from the gateway's read
 * model (latest risk.snapshot of every account); configured accounts without a snapshot
 * are listed in `accounts_missing` and are not in the totals.
 */
import { computed, reactive, ref, watch } from 'vue'
import { usePolling } from '@/composables/realtime'
import { rt } from '@/runtime'
import { toLoadError, type LoadError } from '@/stores/system'
import type { PositionGroupBy, PositionsResponse } from '@/types'
import { fmtMoney, fmtPrice, fmtSignedMoney, signClass } from '@/utils/format'
import { ACCOUNT_NAMES, DIRECTION_LABELS, EXCHANGE_LABELS, PRODUCT_LABELS, SECTOR_LABELS, SECTOR_OPTIONS } from '@/utils/labels'
import SourceNotice from './SourceNotice.vue'

const props = defineProps<{ accounts: string[]; active?: boolean }>()

const f = reactive({ group_by: 'product' as PositionGroupBy | '', account: '', product: '', exchange: '', sector: '' })
const data = ref<PositionsResponse | null>(null)
const error = ref<LoadError | null>(null)
const loading = ref(false)

const GROUP_OPTIONS: { value: PositionGroupBy | ''; label: string }[] = [
  { value: 'product', label: '按品种' },
  { value: 'account', label: '按账户' },
  { value: 'instrument', label: '按合约' },
  { value: 'exchange', label: '按交易所' },
  { value: 'sector', label: '按板块' },
  { value: '', label: '不分组（明细）' },
]

async function load(): Promise<void> {
  loading.value = true
  try {
    data.value = await rt().api.positionsQuery({
      group_by: f.group_by || undefined,
      account: f.account || undefined,
      product: f.product.trim() || undefined,
      exchange: f.exchange || undefined,
      sector: f.sector || undefined,
    })
    error.value = null
  } catch (e) {
    error.value = toLoadError(e)
  } finally {
    loading.value = false
  }
}

watch(
  () => [f.group_by, f.account, f.product, f.exchange, f.sector, props.active] as const,
  () => {
    if (props.active !== false) void load()
  },
  { immediate: true },
)
usePolling(() => (props.active !== false ? load() : undefined), 5000, false)

function groupLabel(g: string): string {
  if (!g) return '—'
  switch (data.value?.group_by) {
    case 'product':
      return PRODUCT_LABELS[g] ? `${g} ${PRODUCT_LABELS[g]}` : g
    case 'account':
      return ACCOUNT_NAMES[g] ? `${g} ${ACCOUNT_NAMES[g]}` : g
    case 'exchange':
      return EXCHANGE_LABELS[g] ?? g
    case 'sector':
      return SECTOR_LABELS[g] ?? g
    default:
      return g
  }
}

const totals = computed(() => {
  const gs = data.value?.groups ?? []
  return {
    gross: gs.reduce((a, g) => a + g.gross_notional, 0),
    net: gs.reduce((a, g) => a + g.net_notional, 0),
    margin: gs.reduce((a, g) => a + g.margin, 0),
    pnl: gs.reduce((a, g) => a + g.position_pnl, 0),
  }
})
</script>

<template>
  <div>
    <el-form inline size="small" class="filters" @submit.prevent>
      <el-form-item label="分组">
        <el-select v-model="f.group_by" style="width: 140px">
          <el-option v-for="o in GROUP_OPTIONS" :key="o.value" :value="o.value" :label="o.label" />
        </el-select>
      </el-form-item>
      <el-form-item label="账户">
        <el-select v-model="f.account" clearable placeholder="全部" style="width: 110px">
          <el-option v-for="a in accounts" :key="a" :value="a" :label="a" />
        </el-select>
      </el-form-item>
      <el-form-item label="品种">
        <el-input v-model="f.product" clearable placeholder="如 rb" style="width: 90px" />
      </el-form-item>
      <el-form-item label="交易所">
        <el-select v-model="f.exchange" clearable placeholder="全部" style="width: 110px">
          <el-option v-for="(label, code) in EXCHANGE_LABELS" :key="code" :value="code" :label="label" />
        </el-select>
      </el-form-item>
      <el-form-item label="板块">
        <el-select v-model="f.sector" clearable placeholder="全部" style="width: 110px">
          <el-option v-for="s in SECTOR_OPTIONS" :key="s.value" :value="s.value" :label="s.label" />
        </el-select>
      </el-form-item>
    </el-form>

    <SourceNotice :error="error" subject="持仓汇总" :retry="load" />
    <SourceNotice v-if="data" :missing="data.accounts_missing" missing-label="以下账户尚无快照（风控分片未连接或未发布），未计入" />
    <SourceNotice v-if="data?.accounts_stale?.length" :missing="data.accounts_stale" missing-label="以下账户的风控分片离线，持仓为最后一次快照（陈旧）" />

    <template v-if="data && data.group_by">
      <el-table v-loading="loading" :data="data.groups" size="small" empty-text="无持仓" max-height="520">
        <el-table-column label="分组" min-width="140">
          <template #default="{ row }"><b>{{ groupLabel(row.group) }}</b></template>
        </el-table-column>
        <el-table-column label="持仓条数" prop="positions" width="80" align="right" />
        <el-table-column label="账户" min-width="120">
          <template #default="{ row }"><span class="muted">{{ row.accounts.join('，') }}</span></template>
        </el-table-column>
        <el-table-column label="多 / 空 / 净（手）" width="140" align="right">
          <template #default="{ row }">{{ row.long_qty }} / {{ row.short_qty }} / <b>{{ row.net_qty }}</b></template>
        </el-table-column>
        <el-table-column label="今 / 昨（手）" width="100" align="right">
          <template #default="{ row }">{{ row.td_qty }} / {{ row.yd_qty }}</template>
        </el-table-column>
        <el-table-column label="总名义" min-width="120" align="right">
          <template #default="{ row }">{{ fmtMoney(row.gross_notional, 0) }}</template>
        </el-table-column>
        <el-table-column label="净名义" min-width="120" align="right">
          <template #default="{ row }"><span :class="signClass(row.net_notional)">{{ fmtSignedMoney(row.net_notional, 0) }}</span></template>
        </el-table-column>
        <el-table-column label="保证金" min-width="110" align="right">
          <template #default="{ row }">{{ fmtMoney(row.margin, 0) }}</template>
        </el-table-column>
        <el-table-column label="盯市盈亏" min-width="110" align="right">
          <template #default="{ row }"><span :class="signClass(row.position_pnl)">{{ fmtSignedMoney(row.position_pnl, 0) }}</span></template>
        </el-table-column>
        <el-table-column label="逐笔浮盈" min-width="110" align="right">
          <template #default="{ row }"><span :class="signClass(row.float_pnl_fifo)">{{ fmtSignedMoney(row.float_pnl_fifo, 0) }}</span></template>
        </el-table-column>
      </el-table>
      <div v-if="data.groups.length" class="muted foot">
        合计：总名义 {{ fmtMoney(totals.gross, 0) }} · 净名义 {{ fmtSignedMoney(totals.net, 0) }} · 保证金 {{ fmtMoney(totals.margin, 0) }} · 盯市盈亏 {{ fmtSignedMoney(totals.pnl, 0) }}
      </div>
    </template>
    <el-table v-else-if="data" v-loading="loading" :data="data.items" size="small" empty-text="无持仓" max-height="520">
      <el-table-column label="账户" prop="account_id" width="80" />
      <el-table-column label="合约" min-width="100">
        <template #default="{ row }"><b>{{ row.instrument_id }}</b></template>
      </el-table-column>
      <el-table-column label="板块" width="80">
        <template #default="{ row }">{{ row.sector ? (SECTOR_LABELS[row.sector] ?? row.sector) : '—' }}</template>
      </el-table-column>
      <el-table-column label="方向" width="62" align="center">
        <template #default="{ row }"><span :class="row.direction === 'LONG' ? 'up' : 'down'">{{ DIRECTION_LABELS[row.direction] }}</span></template>
      </el-table-column>
      <el-table-column label="今 / 昨" width="90" align="right">
        <template #default="{ row }">{{ row.td_qty }} / {{ row.yd_qty }}</template>
      </el-table-column>
      <el-table-column label="持仓均价" width="100" align="right">
        <template #default="{ row }">{{ fmtPrice(row.basis_avg_price) }}</template>
      </el-table-column>
      <el-table-column label="名义价值" min-width="110" align="right">
        <template #default="{ row }">{{ fmtMoney(row.notional, 0) }}</template>
      </el-table-column>
      <el-table-column label="保证金" min-width="100" align="right">
        <template #default="{ row }">{{ fmtMoney(row.margin, 0) }}</template>
      </el-table-column>
      <el-table-column label="盯市盈亏" min-width="100" align="right">
        <template #default="{ row }"><span :class="signClass(row.position_pnl)">{{ fmtSignedMoney(row.position_pnl, 0) }}</span></template>
      </el-table-column>
    </el-table>
  </div>
</template>

<style scoped>
.filters :deep(.el-form-item) {
  margin-bottom: 8px;
}
.foot {
  font-size: 12px;
  margin-top: 6px;
}
</style>
