<script setup lang="ts">
/**
 * Liquidation plans (LIQUIDATE action): confirm or cancel via
 * POST /risk/liquidation-plans/{plan_id}/confirm {decision}. Confirming only records the
 * decision; closes are then placed manually (01 §8.14, TD-08). A plan marked `stale` comes
 * from the gateway's read model because its shard did not answer GET /risk/liquidation-plans.
 */
import { ref } from 'vue'
import type { LiquidationPlan, LiquidationPlanView } from '@/types'
import { needRoleText, useAuth } from '@/stores/auth'
import { useEvents } from '@/stores/events'
import { showInfo, showOk } from '@/composables/feedback'
import { fmtDateTime, shortId } from '@/utils/format'
import { DIRECTION_LABELS, OFFSET_LABELS, type StatusTone } from '@/utils/labels'
import ReasonDialog from './ReasonDialog.vue'
import StaleBadge from './StaleBadge.vue'
import StatusPill from './StatusPill.vue'

defineProps<{ plans: LiquidationPlanView[] }>()
const events = useEvents()
const auth = useAuth()

const pending = ref<{ plan: LiquidationPlan; decision: 'CONFIRM' | 'CANCEL' } | null>(null)
const open = ref(false)

const STATUS: Record<LiquidationPlan['status'], { tone: StatusTone; label: string }> = {
  PENDING_CONFIRM: { tone: 'warning', label: '待确认' },
  CONFIRMED: { tone: 'good', label: '已确认' },
  CANCELED: { tone: 'neutral', label: '已取消' },
  EXPIRED: { tone: 'neutral', label: '已过期' },
}

function ask(plan: LiquidationPlan, decision: 'CONFIRM' | 'CANCEL'): void {
  pending.value = { plan, decision }
  open.value = true
}

async function run(_reason: string, key: string): Promise<void> {
  if (!pending.value) return
  const { plan, decision } = pending.value
  const res = await events.confirmPlan(plan.plan_id, decision, key)
  if (res.conflict) {
    // CONFLICT with the current plan: someone else decided first, or the plan expired
    const st = res.plan ? STATUS[res.plan.status].label : '已处理'
    const by = res.plan?.confirmed_by ? `（${res.plan.confirmed_by}）` : ''
    showInfo(`该计划当前状态为「${st}」${by}，未再次提交`)
    return
  }
  showOk(decision === 'CONFIRM' ? '减仓计划已确认：请按步骤手工下平仓单（熔断 / 禁开期间平仓照常）' : '减仓计划已取消')
}
</script>

<template>
  <div>
    <div v-if="!plans.length" class="empty">暂无减仓计划（当日亏损 −5%、单位净值 0.85、风险度 70% 等 LIQUIDATE 级别触发时生成）</div>
    <div v-for="p in plans" :key="p.plan_id" class="plan" :class="{ pending: p.status === 'PENDING_CONFIRM' }">
      <div class="plan-head">
        <StatusPill :tone="STATUS[p.status].tone" :label="STATUS[p.status].label" />
        <span class="mono" :title="p.plan_id">{{ shortId(p.plan_id) }}</span>
        <StaleBadge v-if="p.stale" compact title="所属风控分片未应答：计划来自网关缓存，状态可能已变化" />
        <span class="muted">生成于 {{ fmtDateTime(p.created_ns) }}</span>
        <span class="muted mono" :title="p.trigger_alert_key">触发：{{ p.trigger_alert_key }}</span>
        <span class="spacer" />
        <template v-if="p.status === 'PENDING_CONFIRM'">
          <el-tooltip :disabled="auth.canRisk" :content="needRoleText('RISK')" placement="bottom">
            <span>
              <el-button size="small" type="danger" :disabled="!auth.canRisk" @click="ask(p, 'CONFIRM')">确认计划</el-button>
              <el-button size="small" :disabled="!auth.canRisk" @click="ask(p, 'CANCEL')">取消计划</el-button>
            </span>
          </el-tooltip>
        </template>
        <span v-else-if="p.confirmed_by" class="muted">{{ p.confirmed_by }} · {{ fmtDateTime(p.confirmed_ns) }}</span>
      </div>
      <el-table :data="p.steps" size="small">
        <el-table-column label="合约" prop="instrument_id" width="110" />
        <el-table-column label="方向" width="70">
          <template #default="{ row }"><span :class="row.direction === 'BUY' ? 'up' : 'down'">{{ DIRECTION_LABELS[row.direction] }}</span></template>
        </el-table-column>
        <el-table-column label="开平" width="90">
          <template #default="{ row }">{{ OFFSET_LABELS[row.offset] ?? row.offset }}</template>
        </el-table-column>
        <el-table-column label="手数" prop="qty" width="80" align="right" />
        <el-table-column label="建议价格" prop="price_rule" min-width="140" />
      </el-table>
    </div>
    <ReasonDialog
      v-model="open"
      :title="pending?.decision === 'CONFIRM' ? '确认减仓计划' : '取消减仓计划'"
      :description="pending?.decision === 'CONFIRM' ? '确认只记录决定并写审计，不会自动下单；请随后按步骤手工下平仓单。' : '取消后该计划作废（写审计）。'"
      :confirm-text="pending?.decision === 'CONFIRM' ? '确认' : '取消计划'"
      :danger="pending?.decision === 'CONFIRM'"
      :require-reason="false"
      :action="run"
    />
  </div>
</template>

<style scoped>
.plan {
  border: 1px solid var(--fr-line);
  border-radius: var(--fr-radius);
  padding: 10px 12px;
  margin-bottom: 10px;
}
.plan.pending {
  border-color: #f1c98a;
  background: #fffaf0;
}
.plan-head {
  display: flex;
  align-items: center;
  gap: 10px;
  flex-wrap: wrap;
  margin-bottom: 8px;
  font-size: 13px;
}
.spacer {
  flex: 1;
}
</style>
