<script setup lang="ts">
/**
 * Live risk-event stream (WS `events`: RiskEvent + AccountStateChanged), newest first,
 * plus the day's history from GET /risk/events (a shard that did not answer is noted:
 * its events then come from the gateway's read model only). ALERT_RAISED rows can be
 * acknowledged (POST /risk/events/{alert_key}/ack, RISK_MANAGER / ADMIN).
 */
import { computed, ref } from 'vue'
import type { AccountStateChanged, RiskEvent } from '@/types'
import { useAuth } from '@/stores/auth'
import { useEvents, type StreamItem } from '@/stores/events'
import { showError, showOk } from '@/composables/feedback'
import { fmtTime } from '@/utils/format'
import {
  ACCOUNT_STATE_LABELS,
  ACCOUNT_STATE_TONE,
  RESTRICTION_LABELS,
  RISK_EVENT_LABELS,
  SEVERITY_LABELS,
  SEVERITY_TONE,
  STATE_CAUSE_LABELS,
  scopeLabel,
} from '@/utils/labels'
import { uuid } from '@/utils/uuid'
import SourceNotice from './SourceNotice.vue'
import StatusPill from './StatusPill.vue'

const props = withDefaults(defineProps<{ accountId?: string | null; limit?: number; maxHeight?: string; showState?: boolean; compact?: boolean; hideLoadNotice?: boolean }>(), {
  accountId: null,
  limit: 200,
  maxHeight: '420px',
  showState: true,
  compact: false,
  hideLoadNotice: false,
})

const events = useEvents()
const auth = useAuth()
const severityFilter = ref<'ALL' | 'HIGH'>('ALL')
const acking = ref<string | null>(null)

const items = computed<StreamItem[]>(() => {
  let list = events.forAccount(props.accountId)
  if (!props.showState) list = list.filter((i) => i.risk)
  if (severityFilter.value === 'HIGH') list = list.filter((i) => (i.risk ? i.risk.severity === 'HIGH' || i.risk.severity === 'CRITICAL' : i.state?.to_state === 'HALTED'))
  return list.slice(0, props.limit)
})

function stateText(s: AccountStateChanged): string {
  const to = ACCOUNT_STATE_LABELS[s.to_state] + (s.to_state === 'HALTED' ? (s.halted_latched ? '（已锁存）' : '（待确认）') : '')
  const cause = STATE_CAUSE_LABELS[s.cause] ?? s.cause
  const restr = s.restrictions.length ? ' · ' + s.restrictions.map((r) => `${RESTRICTION_LABELS[r.kind] ?? r.kind}${r.value != null ? ' ' + r.value : ''}@${scopeLabel(r.scope_key)}`).join('，') : ''
  const who = s.actor ? ` · ${s.actor}` : ''
  const why = s.reason ? ` · ${s.reason}` : ''
  return `${s.account_id} ${ACCOUNT_STATE_LABELS[s.from_state]} → ${to} · ${cause}${who}${why}${restr}`
}

function canAck(e: RiskEvent): boolean {
  return auth.canRisk && e.kind === 'ALERT_RAISED' && !!e.alert_key && !events.acked[e.alert_key]
}

function reload(): void {
  void events.load(undefined, props.accountId ?? undefined).catch(() => undefined)
}

/** load state of the history this stream shows (firm-wide, or one account's) */
const loadState = computed(() => (props.accountId ? (events.accountLoad[props.accountId] ?? { errors: {}, error: null }) : { errors: events.loadErrors, error: events.loadError }))

async function ack(e: RiskEvent): Promise<void> {
  if (!e.alert_key) return
  acking.value = e.alert_key
  try {
    await events.ack(e.alert_key, undefined, uuid())
    showOk('告警已确认')
  } catch (err) {
    showError(err, '确认告警失败')
  } finally {
    acking.value = null
  }
}
</script>

<template>
  <div class="stream">
    <div v-if="!compact" class="toolbar">
      <el-radio-group v-model="severityFilter" size="small">
        <el-radio-button value="ALL">全部</el-radio-button>
        <el-radio-button value="HIGH">仅高 / 严重</el-radio-button>
      </el-radio-group>
      <span class="muted count">{{ items.length }} 条</span>
    </div>
    <SourceNotice v-if="!hideLoadNotice" :error="loadState.error" :errors="loadState.errors" subject="当日风险事件历史" compact :retry="reload">
      <span v-if="!loadState.error" class="muted">（未应答分片的事件只含网关读模型中的部分）</span>
    </SourceNotice>
    <div class="list" :style="{ maxHeight }">
      <div v-if="!items.length" class="empty">暂无风险事件</div>
      <div v-for="it in items" :key="it.key" class="row" :class="{ crit: it.risk?.severity === 'CRITICAL' || (it.state?.to_state === 'HALTED' && it.state.halted_latched) }">
        <span class="time mono">{{ fmtTime(it.ts_ns) }}</span>
        <template v-if="it.risk">
          <StatusPill :tone="SEVERITY_TONE[it.risk.severity ?? 'INFO']" :label="SEVERITY_LABELS[it.risk.severity ?? 'INFO']" />
          <span class="kind">{{ RISK_EVENT_LABELS[it.risk.kind] ?? it.risk.kind }}</span>
          <router-link v-if="it.risk.account_id" class="acct" :to="`/accounts/${it.risk.account_id}`">{{ it.risk.account_id }}</router-link>
          <span class="msg" :title="it.risk.alert_key ?? ''">{{ it.risk.message || it.risk.kind }}</span>
          <el-button v-if="canAck(it.risk)" link size="small" type="primary" :loading="acking === it.risk.alert_key" @click="ack(it.risk)">确认</el-button>
          <span v-else-if="it.risk.alert_key && events.acked[it.risk.alert_key]" class="muted acked">已确认</span>
        </template>
        <template v-else-if="it.state">
          <StatusPill :tone="ACCOUNT_STATE_TONE[it.state.to_state]" :label="ACCOUNT_STATE_LABELS[it.state.to_state]" />
          <span class="kind">状态变化</span>
          <router-link class="acct" :to="`/accounts/${it.state.account_id}`">{{ it.state.account_id }}</router-link>
          <span class="msg">{{ stateText(it.state) }}</span>
        </template>
      </div>
    </div>
  </div>
</template>

<style scoped>
.toolbar {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-bottom: 8px;
}
.count {
  font-size: 12px;
}
.list {
  overflow-y: auto;
}
.row {
  display: flex;
  align-items: baseline;
  gap: 8px;
  padding: 6px 4px;
  border-bottom: 1px solid var(--fr-line);
  font-size: 13px;
  flex-wrap: wrap;
}
.row.crit {
  background: #fdf1f1;
}
.time {
  color: var(--fr-ink-3);
  flex: none;
}
.kind {
  font-weight: 500;
  flex: none;
}
.acct {
  flex: none;
  font-weight: 500;
}
.msg {
  flex: 1 1 240px;
  min-width: 0;
  color: var(--fr-ink-2);
  word-break: break-word;
}
.acked {
  font-size: 12px;
}
</style>
