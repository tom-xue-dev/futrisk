<script setup lang="ts">
/**
 * Product × account signed net-notional grid with a firm net column. Cell fill is
 * diverging (red = net long, blue = net short, gray = flat), scaled per column group
 * (accounts share one scale, the firm column its own); values are always printed.
 */
import { computed } from 'vue'
import type { ExposureGrid } from '@/types'
import { divergingColor, textOn } from '@/utils/compare'
import { fmtMoneyCompact } from '@/utils/format'
import { ACCOUNT_NAMES, PRODUCT_LABELS } from '@/utils/labels'

const props = defineProps<{ grid: ExposureGrid }>()

const cellMap = computed(() => {
  const m = new Map<string, { net: number; lots: number }>()
  for (const c of props.grid.cells) m.set(`${c.product}|${c.account_id}`, { net: c.net_notional, lots: c.net_lots })
  return m
})

const maxAcct = computed(() => props.grid.cells.reduce((m, c) => Math.max(m, Math.abs(c.net_notional)), 0))
const maxFirm = computed(() => Object.values(props.grid.firm_net).reduce((m, v) => Math.max(m, Math.abs(v)), 0))

function style(v: number | undefined, max: number): Record<string, string> {
  if (v === undefined) return {}
  const bg = divergingColor(v, max)
  return { background: bg, color: textOn(bg) }
}
</script>

<template>
  <div class="wrap">
    <table class="xg">
      <thead>
        <tr>
          <th class="prod">品种</th>
          <th v-for="a in grid.accounts" :key="a" :title="ACCOUNT_NAMES[a] ?? a">{{ a }}</th>
          <th class="firm">公司净额</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="p in grid.products" :key="p">
          <td class="prod"><b>{{ p }}</b> <span class="muted">{{ PRODUCT_LABELS[p] ?? '' }}</span></td>
          <td v-for="a in grid.accounts" :key="a" class="num" :style="style(cellMap.get(`${p}|${a}`)?.net, maxAcct)" :title="cellMap.get(`${p}|${a}`) ? `${a} ${p} 净名义 ${fmtMoneyCompact(cellMap.get(`${p}|${a}`)?.net, 2, true)}，净手数 ${cellMap.get(`${p}|${a}`)?.lots}` : `${a} 无 ${p} 敞口`">
            {{ cellMap.get(`${p}|${a}`) ? fmtMoneyCompact(cellMap.get(`${p}|${a}`)?.net, 1, true) : '' }}
          </td>
          <td class="num firm" :style="style(grid.firm_net[p], maxFirm)">{{ fmtMoneyCompact(grid.firm_net[p], 1, true) }}</td>
        </tr>
      </tbody>
    </table>
    <div class="legend">
      <span><i style="background: #d03b3b" />净多</span>
      <span><i style="background: #f0efec" />持平 / 无</span>
      <span><i style="background: #2a78d6" />净空</span>
      <span class="muted">颜色深浅按列组分别缩放（账户列共用一个刻度，公司净额单独刻度）；单位：元</span>
    </div>
  </div>
</template>

<style scoped>
.wrap {
  overflow-x: auto;
}
.xg {
  border-collapse: separate;
  border-spacing: 2px;
  width: 100%;
  font-size: 13px;
}
.xg th {
  font-weight: 600;
  color: var(--fr-ink-2);
  padding: 6px 8px;
  text-align: right;
  white-space: nowrap;
}
.xg th.prod,
.xg td.prod {
  text-align: left;
  white-space: nowrap;
}
.xg td {
  padding: 6px 8px;
  border-radius: 3px;
  background: #fafaf8;
  white-space: nowrap;
}
.xg td.num {
  text-align: right;
  font-variant-numeric: tabular-nums;
}
.xg th.firm,
.xg td.firm {
  border-left: 2px solid var(--fr-line);
  font-weight: 600;
}
.legend {
  display: flex;
  gap: 14px;
  flex-wrap: wrap;
  font-size: 12px;
  color: var(--fr-ink-2);
  margin-top: 6px;
}
.legend i {
  display: inline-block;
  width: 12px;
  height: 12px;
  border-radius: 3px;
  margin-right: 4px;
  vertical-align: -2px;
}
</style>
