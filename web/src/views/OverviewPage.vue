<script setup lang="ts">
/**
 * 总览: firm KPIs (WS `firm` / FirmSnapshot, else aggregated from `accounts`), account
 * state distribution, accounts sorted by worst rule utilization (WS `accounts`), live
 * risk-event stream (WS `events`), kill switch (POST /risk/firm/kill-switch).
 */
import { computed, ref } from 'vue'
import { useRouter } from 'vue-router'
import { Lock, Unlock } from '@element-plus/icons-vue'
import EventStream from '@/components/EventStream.vue'
import ReasonDialog from '@/components/ReasonDialog.vue'
import SourceNotice from '@/components/SourceNotice.vue'
import StaleBadge from '@/components/StaleBadge.vue'
import StateBadge from '@/components/StateBadge.vue'
import StatTile from '@/components/StatTile.vue'
import StatusPill from '@/components/StatusPill.vue'
import UtilBar from '@/components/UtilBar.vue'
import ProductNetBar from '@/components/charts/ProductNetBar.vue'
import { usePolling } from '@/composables/realtime'
import { showOk, showWarn } from '@/composables/feedback'
import { useAccounts } from '@/stores/accounts'
import { needRoleText, useAuth } from '@/stores/auth'
import { useFirm } from '@/stores/firm'
import { useSystem } from '@/stores/system'
import type { AccountState, AccountSummary, ProductNet } from '@/types'
import { ACCOUNT_STATE_VALUES } from '@/types'
import { fmtDateTime, fmtMoney, fmtMoneyCompact, fmtPct, fmtSignedMoney, fmtTime, signClass } from '@/utils/format'
import { ACCOUNT_NAMES, ACCOUNT_STATE_LABELS, ACCOUNT_STATE_TONE, MD_QUALITY_LABELS, MD_QUALITY_TONE, codeLabel, ruleLabel } from '@/utils/labels'

const router = useRouter()
const firm = useFirm()
const accounts = useAccounts()
const auth = useAuth()
const system = useSystem()

const killDialog = ref(false)

// keep full snapshots (worst-rule names, compare fallback) fresh besides the WS summaries
usePolling(() => accounts.loadAll(), 10_000, false)
// kill-switch retry progress and the firm-level fallback flag come from /system/status
usePolling(() => system.loadStatus(), () => (system.killSwitchControl?.retrying || system.killSwitchControl?.pending.length ? 3_000 : 15_000), false)

const view = computed(() => firm.view)

const rows = computed<AccountSummary[]>(() =>
  [...accounts.list].sort((a, b) => {
    const ua = a.worst_utilization
    const ub = b.worst_utilization
    if (ua === null && ub === null) return a.account_id.localeCompare(b.account_id)
    if (ua === null) return 1
    if (ub === null) return -1
    return ub - ua
  }),
)

const productNet = computed<ProductNet[]>(() => {
  const fromFirm = firm.snapshot?.product_net
  if (fromFirm && fromFirm.length) return fromFirm
  const agg: Record<string, ProductNet> = {}
  for (const s of Object.values(accounts.snapshots)) {
    for (const e of s.exposures ?? []) {
      const p = agg[e.product] ?? (agg[e.product] = { product: e.product, net_lots: 0, net_notional: 0, gross_notional: 0, accounts: 0 })
      p.net_lots += e.net_lots
      p.net_notional += e.net_notional
      p.gross_notional += e.gross_notional
      p.accounts += 1
    }
  }
  return Object.values(agg)
})

const riskTone = computed(() => {
  const rd = view.value.risk_degree
  if (rd === null) return ''
  return rd >= 0.5 ? 'down' : ''
})

function rowClass({ row }: { row: AccountSummary }): string {
  return ['row-clickable', row.state === 'HALTED' ? 'row-halted' : '', row.stale ? 'row-stale' : ''].join(' ')
}

function openAccount(row: AccountSummary): void {
  void router.push(`/accounts/${row.account_id}`)
}

async function toggleKill(reason: string, key: string): Promise<void> {
  const on = !view.value.kill_switch
  const out = await firm.setKillSwitch(on, reason, key)
  const r = out.result
  const confirmed = r.targets.filter((t) => t.ok).length
  const dup = out.duplicate ? '（幂等重放）' : ''
  if (r.all_ok) showOk((on ? 'Kill switch 已开启：所有账户禁开，平仓照常' : 'Kill switch 已关闭（若公司级规则仍开启，生效状态保持开启）') + dup)
  else {
    const parts = [`${confirmed}/${r.targets.length} 个目标已确认`]
    if (r.retrying.length) parts.push(`${r.retrying.join('，')} 暂不可达，网关在后台重试`)
    if (r.failed.length) parts.push(`${r.failed.join('，')} 已失败（请人工处理）`)
    showWarn(`Kill switch ${on ? '开启' : '关闭'}：${parts.join('；')}`)
  }
  void system.loadStatus().catch(() => undefined)
}

const killStateText = computed(() => (view.value.kill_switch === null ? '未知（未收到公司层快照）' : view.value.kill_switch ? '已开启 · 全部禁开' : '关闭'))

/** The two layers of the effective kill switch (MANUAL OR RULE). */
const killLayers = computed(() => {
  const c = view.value.control
  if (!c) return null
  return {
    manual: c.manual ? (c.manual.active ? `开启（${c.manual.actor ?? '—'}${c.manual.reason ? '：' + c.manual.reason : ''}）` : '关闭') : '未设置',
    rule: c.rule ? (c.rule.active ? `开启${c.rule.reason ? '（' + c.rule.reason + '）' : ''}` : '关闭') : '—',
    restrictions: c.rule?.restrictions?.length ?? 0,
  }
})

/** Persisted manual kill-switch decision with its per-target confirmation (/system/status + WS system events). */
const killControl = computed(() => system.killSwitchControl)
const showKillPanel = computed(() => {
  const c = killControl.value
  if (!c) return Boolean(firm.lastKill)
  return c.pending.length > 0 || c.failed.length > 0 || Boolean(firm.lastKill)
})
const KILL_TARGET_TEXT: Record<string, string> = { CONFIRMED: '已确认', PENDING: '重试中', FAILED: '已失败' }

const stateOrder: AccountState[] = [...ACCOUNT_STATE_VALUES]

function accountsIn(s: AccountState): AccountSummary[] {
  return accounts.list.filter((a) => a.state === s)
}
</script>

<template>
  <div class="page">
    <div class="page-head">
      <h1>总览</h1>
      <span class="muted">{{ view.source === 'firm' ? '公司层汇总来自 coordinator（risk.firm）' : '公司层汇总由各账户快照计算（未收到 risk.firm）' }}</span>
      <span v-if="view.as_of_ns" class="muted">· 更新于 {{ fmtTime(view.as_of_ns) }}</span>
    </div>

    <div class="grid kpis">
      <StatTile label="公司权益" :value="fmtMoneyCompact(view.equity)" :sub="view.equity === null ? '暂无账户数据' : fmtMoney(view.equity) + ' 元'" emphasis />
      <StatTile label="当日盈亏" :value="fmtMoneyCompact(view.daily_pnl, 2, true)" :value-class="signClass(view.daily_pnl)" :sub="view.daily_pnl === null ? '' : fmtSignedMoney(view.daily_pnl) + ' 元'" emphasis />
      <StatTile label="当日盈亏 / 初始资金" :value="fmtPct(view.daily_pnl_pct_init, 2, true)" :value-class="signClass(view.daily_pnl_pct_init)" sub="验收口径（01 §8.14）" emphasis />
      <StatTile label="保证金占用" :value="fmtMoneyCompact(view.margin)" :sub="view.gross_notional !== null ? `总名义 ${fmtMoneyCompact(view.gross_notional)} · 净名义 ${fmtMoneyCompact(view.net_notional, 2, true)}` : ''" emphasis />
      <StatTile label="风险度（保证金 / 权益）" :value="fmtPct(view.risk_degree, 1)" :value-class="riskTone" sub="阈值 35% / 50% / 70%" emphasis />
      <div class="kill card" :class="{ on: view.kill_switch }">
        <div class="kill-label">
          <el-icon><Lock v-if="view.kill_switch" /><Unlock v-else /></el-icon>
          全局 Kill switch
        </div>
        <div class="kill-state">{{ killStateText }}</div>
        <div v-if="killLayers" class="kill-layers muted">人工：{{ killLayers.manual }} · 规则：{{ killLayers.rule }}<template v-if="killLayers.restrictions"> · 公司级限制 {{ killLayers.restrictions }} 条</template></div>
        <el-tooltip :disabled="auth.canRisk" :content="needRoleText('RISK')" placement="bottom">
          <span>
            <el-button size="small" :type="view.kill_switch ? 'primary' : 'danger'" plain :disabled="!auth.canRisk" @click="killDialog = true">{{ view.kill_switch ? '关闭 kill switch' : '开启 kill switch' }}</el-button>
          </span>
        </el-tooltip>
      </div>
    </div>

    <div v-if="showKillPanel" class="card section-gap kill-result">
      <div class="card-title">
        Kill switch 指令
        <template v-if="killControl">
          ：{{ killControl.desired_on ? '开启' : '关闭' }}
          <span class="hint">{{ killControl.actor }} · {{ fmtDateTime(killControl.requested_ns) }}<template v-if="killControl.reason"> · {{ killControl.reason }}</template> · 扇出到各风控分片与交易网关</span>
        </template>
        <template v-else-if="firm.lastKill">：{{ firm.lastKill.on ? '开启' : '关闭' }}</template>
        <span class="spacer" />
        <el-button v-if="firm.lastKill" link size="small" @click="firm.lastKill = null">收起</el-button>
      </div>
      <template v-if="killControl">
        <SourceNotice v-if="killControl.failed.length" :error="{ code: 'FAILED', message: `${killControl.failed.join('，')} 未能执行本次指令（重试已停止，已告警），请人工处理` }" subject="Kill switch" />
        <SourceNotice v-else-if="killControl.pending.length" compact :message="`${killControl.confirmed.length}/${Object.keys(killControl.targets).length} 个目标已确认；网关每 2s 重试 ${killControl.pending.join('，')}${killControl.retrying ? '' : '（重试任务未运行）'}`" />
        <div class="targets">
          <div v-for="(t, name) in killControl.targets" :key="name" class="target" :class="t.status === 'CONFIRMED' ? 'ok' : t.status === 'FAILED' ? 'failed' : 'bad'">
            <span class="mark">{{ t.status === 'CONFIRMED' ? '✓' : t.status === 'FAILED' ? '✗' : '…' }}</span>
            <b class="mono">{{ name }}</b>
            <span class="muted">{{ KILL_TARGET_TEXT[t.status] ?? t.status }}<template v-if="t.status !== 'CONFIRMED' && t.code"> · {{ t.code }}{{ codeLabel(t.code) ? '（' + codeLabel(t.code) + '）' : '' }}</template> · 第 {{ t.attempts }} 次</span>
          </div>
        </div>
      </template>
      <div v-else-if="firm.lastKill" class="targets">
        <div v-for="t in firm.lastKill.result.targets" :key="t.target" class="target" :class="t.ok ? 'ok' : 'bad'">
          <span class="mark">{{ t.ok ? '✓' : '…' }}</span>
          <b class="mono">{{ t.target }}</b>
          <span v-if="t.ok" class="muted">已确认{{ t.duplicate ? '（重复指令，已去重）' : '' }}</span>
          <span v-else class="muted">{{ t.code }}{{ codeLabel(t.code) ? '（' + codeLabel(t.code) + '）' : '' }}{{ t.message ? ' · ' + t.message : '' }}</span>
        </div>
      </div>
    </div>

    <div class="grid cols-2 section-gap">
      <div class="card">
        <div class="card-title">账户状态分布 <span class="hint">取所有生效级别中最严重的一个（01 §8.14）</span></div>
        <div class="states">
          <div v-for="s in stateOrder" :key="s" class="state-cell" :class="ACCOUNT_STATE_TONE[s]">
            <StatusPill :tone="ACCOUNT_STATE_TONE[s]" :label="ACCOUNT_STATE_LABELS[s]" />
            <div class="state-n">{{ view.accounts_by_state[s] ?? 0 }}</div>
            <div class="state-accts">
              <router-link v-for="a in accountsIn(s)" :key="a.account_id" :to="`/accounts/${a.account_id}`" class="chip">{{ a.account_id }}<template v-if="s === 'HALTED'">{{ a.halted_latched ? ' · 锁存' : ' · 待确认' }}</template></router-link>
            </div>
          </div>
        </div>
      </div>
      <div class="card">
        <div class="card-title">公司品种净敞口 <span class="hint">红 = 净多，蓝 = 净空；跨账户汇总</span></div>
        <ProductNetBar v-if="productNet.length" :items="productNet" />
        <div v-else class="empty">暂无持仓</div>
      </div>
    </div>

    <div class="main-grid section-gap">
      <div class="card">
        <div class="card-title">账户（按最差规则使用率排序） <span class="hint">使用率 = 数值 ÷ 硬限额；点击行查看账户</span></div>
        <SourceNotice v-if="view.partial" compact :message="`${view.partial} 个账户暂只有状态（风控分片尚未发布快照），数值为空且未计入上方汇总`" />
        <SourceNotice v-if="accounts.withoutData.length" compact :missing="accounts.withoutData" missing-label="已配置但尚无快照的账户（所属风控分片未连接或未发布），未列入下表" />
        <SourceNotice v-if="accounts.staleIds.length" compact :message="`${accounts.staleIds.join('，')} 的风控分片离线：显示的是最后一次快照（标“陈旧”），不是实时数据${view.source === 'accounts' ? '；上方汇总含这些陈旧数值' : ''}`" />
        <el-table :data="rows" :row-class-name="rowClass" size="small" empty-text="暂无账户快照（风控分片未连接或尚未发布 risk.snapshot）" @row-click="openAccount">
          <el-table-column label="账户" min-width="128">
            <template #default="{ row }">
              <div class="acct-cell">
                <b>{{ row.account_id }}</b>
                <span class="muted">{{ ACCOUNT_NAMES[row.account_id] ?? '' }}</span>
                <StaleBadge v-if="row.stale" :age-ms="row.age_ms" compact />
              </div>
            </template>
          </el-table-column>
          <el-table-column label="状态" min-width="112">
            <template #default="{ row }"><StateBadge :state="row.state" :latched="row.halted_latched" /></template>
          </el-table-column>
          <el-table-column label="分片" width="50" align="center" prop="shard" />
          <el-table-column label="动态权益" min-width="110" align="right">
            <template #default="{ row }">{{ fmtMoney(row.equity) }}</template>
          </el-table-column>
          <el-table-column label="当日盈亏" min-width="106" align="right">
            <template #default="{ row }"><span :class="signClass(row.daily_pnl)">{{ fmtSignedMoney(row.daily_pnl) }}</span></template>
          </el-table-column>
          <el-table-column label="盈亏 / 初始" width="90" align="right">
            <template #default="{ row }"><span :class="signClass(row.daily_pnl_pct_init)">{{ fmtPct(row.daily_pnl_pct_init, 2, true) }}</span></template>
          </el-table-column>
          <el-table-column label="风险度" width="68" align="right">
            <template #default="{ row }">{{ fmtPct(row.risk_degree, 1) }}</template>
          </el-table-column>
          <el-table-column label="最差规则使用率" min-width="158">
            <template #default="{ row }"><UtilBar :value="row.worst_utilization" :label="row.worst_rule ? ruleLabel(row.worst_rule) : ''" /></template>
          </el-table-column>
          <el-table-column label="行情" width="86" align="center">
            <template #default="{ row }"><StatusPill v-if="row.quality" :tone="MD_QUALITY_TONE[row.quality as 'OK']" :label="MD_QUALITY_LABELS[row.quality as 'OK']" /><span v-else class="muted">—</span></template>
          </el-table-column>
        </el-table>
      </div>
      <div class="card">
        <div class="card-title">实时风险事件 <span class="hint">risk.event + risk.state</span></div>
        <EventStream max-height="520px" />
      </div>
    </div>

    <ReasonDialog
      v-model="killDialog"
      :title="view.kill_switch ? '关闭全局 kill switch' : '开启全局 kill switch'"
      :description="view.kill_switch ? '关闭人工 kill switch。若公司级规则仍开启 kill switch，生效状态保持开启（人工 OR 规则，02 §7.6）。' : '开启后所有账户的开仓单被拒绝（REJECT_FIRM_KILL_SWITCH），平仓照常；api-gateway 扇出到所有风控分片和交易网关。需要 RISK_MANAGER 权限。'"
      :confirm-text="view.kill_switch ? '确认关闭' : '确认开启'"
      :danger="!view.kill_switch"
      :action="toggleKill"
    />
  </div>
</template>

<style scoped>
.kill {
  display: flex;
  flex-direction: column;
  gap: 6px;
  justify-content: space-between;
}
.kill.on {
  border-color: #d03b3b;
  background: #fdf1f1;
}
.kill-label {
  display: flex;
  align-items: center;
  gap: 6px;
  font-size: 12px;
  color: var(--fr-ink-2);
}
.kill-state {
  font-size: 18px;
  font-weight: 600;
}
.kill.on .kill-state {
  color: #b42323;
}
.states {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 10px;
}
.state-cell {
  border: 1px solid var(--fr-line);
  border-radius: var(--fr-radius);
  padding: 10px 12px;
}
.state-cell.critical {
  border-color: #eab0b0;
}
.state-n {
  font-size: 30px;
  font-weight: 600;
  margin-top: 6px;
}
.state-accts {
  display: flex;
  flex-wrap: wrap;
  gap: 4px;
  margin-top: 6px;
  min-height: 22px;
}
.chip {
  font-size: 12px;
  padding: 1px 8px;
  border-radius: 999px;
  background: #f4f5f7;
  border: 1px solid var(--fr-line);
  color: var(--fr-ink);
}
.chip:hover {
  text-decoration: none;
  border-color: #b8c7de;
}
.main-grid {
  display: grid;
  grid-template-columns: minmax(0, 1.75fr) minmax(0, 1fr);
  gap: 14px;
}
@media (max-width: 1280px) {
  .main-grid {
    grid-template-columns: minmax(0, 1fr);
  }
}
.kill-result .spacer {
  flex: 1;
}
.targets {
  display: flex;
  flex-wrap: wrap;
  gap: 8px 18px;
  font-size: 13px;
}
.target {
  display: inline-flex;
  gap: 6px;
  align-items: baseline;
}
.target .mark {
  font-weight: 700;
}
.target.ok .mark {
  color: #1f7a1f;
}
.target.bad .mark {
  color: #9a6700;
}
.target.failed .mark {
  color: #b42323;
}
.kill-layers {
  font-size: 12px;
  line-height: 1.4;
}
.acct-cell {
  display: flex;
  flex-direction: column;
  line-height: 1.3;
}
.acct-cell .muted {
  font-size: 12px;
}
</style>
