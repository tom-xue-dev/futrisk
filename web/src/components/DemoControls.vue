<script setup lang="ts">
/**
 * Demo controls (02 §9.6 `/demo/*`, ADMIN and app.mode=demo only — otherwise the gateway
 * answers 403 FORBIDDEN):
 *  - sim state: GET /demo/state (faults, fault history, current scenario run, pause) polled
 *    while the panel is shown, GET /demo/faults `{items, history}` after a fault change;
 *  - fault injection (POST /demo/faults → `{fault_id, state, fault}`, DELETE /demo/faults/{id});
 *  - price shock (POST /demo/shock → `{shock_id, shock}`);
 *  - scenarios: GET /demo/scenarios `[{name, description}]`, POST /demo/scenario →
 *    `{run_id, name}`, DELETE /demo/scenario (stop the running one → the run);
 *  - kill a service (POST /demo/kill/{instance});
 *  - market-clock jump (POST /demo/clock {target, at?, at_ns?} → ClockJumpResult with the
 *    five orchestration steps, `converged` and the `lagging` instances).
 *
 * Every outcome of a /demo mutation is recorded under its Idempotency-Key (the sim does not
 * deduplicate), so a retry with the same key replays the answer; each click here uses a new key.
 */
import { computed, reactive, ref, watch } from 'vue'
import { useNow, usePolling } from '@/composables/realtime'
import { ApiError } from '@/api/http'
import { rt } from '@/runtime'
import { showError, showInfo, showOk, showWarn } from '@/composables/feedback'
import type { ClockJumpResult, ClockTarget, DemoFaultResult, DemoScenarioInfo, DemoState, ScenarioRunState, SimFault } from '@/types'
import { fmtDateTime, fmtDuration, fmtTime, parseShanghaiWallTime, toShanghaiIso, toShanghaiWall } from '@/utils/format'
import { FAULT_TYPES, SECTOR_OPTIONS, SERVICE_LABELS } from '@/utils/labels'
import { isObj } from '@/utils/normalize'
import { useAuth } from '@/stores/auth'
import { toLoadError, useSystem, type LoadError } from '@/stores/system'
import SourceNotice from './SourceNotice.vue'

const props = defineProps<{ instruments: string[]; instances: string[]; accounts?: string[] }>()
const system = useSystem()
const auth = useAuth()
const now = useNow(1000)

/** 403 answer of a /demo/* call (not in demo mode, or not ADMIN) */
const forbidden = ref('')
const notDemo = computed(() => system.appMode !== null && system.appMode !== 'demo')
const disabled = computed(() => notDemo.value || !auth.canAdmin)

function failed(e: unknown, prefix: string): void {
  if (e instanceof ApiError && e.code === 'FORBIDDEN') {
    forbidden.value = e.message || 'FORBIDDEN'
    return
  }
  showError(e, prefix)
}

function dupNote(dup: boolean): string {
  return dup ? '（幂等重放：返回首次请求的结果）' : ''
}

// ---------------------------------------------------------------- faults
const fault = reactive({ type: 'FEED_DISCONNECT', target: 'feed_a', duration: undefined as number | undefined, params: {} as Record<string, number | boolean | string> })
const faultDef = computed(() => FAULT_TYPES.find((f) => f.value === fault.type) ?? FAULT_TYPES[0])
watch(
  () => fault.type,
  () => {
    const d = faultDef.value
    fault.target = d?.targets[0] ?? ''
    fault.params = Object.fromEntries((d?.params ?? []).map((p) => [p.key, p.default]))
  },
  { immediate: true },
)
// ---------------------------------------------------------------- sim state (GET /demo/state, /demo/faults)
const simState = ref<DemoState | null>(null)
const simError = ref<LoadError | null>(null)
const faults = ref<SimFault[]>([])
const faultHistory = ref<SimFault[]>([])
const scenarioRun = ref<ScenarioRunState | null>(null)

async function refreshState(): Promise<void> {
  if (disabled.value) return
  try {
    const st = await rt().api.demoState()
    simState.value = st
    faults.value = st.faults ?? []
    faultHistory.value = st.fault_history ?? []
    scenarioRun.value = st.scenario ?? null
    simError.value = null
  } catch (e) {
    if (e instanceof ApiError && e.code === 'FORBIDDEN') forbidden.value = e.message || 'FORBIDDEN'
    simError.value = toLoadError(e)
  }
}

async function refreshFaults(): Promise<void> {
  try {
    const r = await rt().api.demoFaults()
    faults.value = r.items ?? []
    faultHistory.value = r.history ?? []
  } catch {
    void refreshState()
  }
}

const scenarioRunning = computed(() => scenarioRun.value?.status === 'RUNNING')
// faster while a scenario runs (its step / log move) or a timed fault counts down
usePolling(refreshState, () => (disabled.value || forbidden.value ? 0 : scenarioRunning.value || faults.value.some((f) => f.remaining_s !== null) ? 2_000 : 5_000), false)

const FAULT_STATE_TEXT: Record<string, string> = { ACTIVE: '生效中', DONE: '已执行', EXPIRED: '已到期', DELETED: '已撤销' }

function faultLabel(f: SimFault): string {
  return FAULT_TYPES.find((t) => t.value === f.type)?.label ?? f.type
}

function faultParams(f: SimFault): string {
  return Object.entries(f.params ?? {})
    .map(([k, v]) => `${k}=${typeof v === 'object' ? JSON.stringify(v) : String(v)}`)
    .join(' ')
}

const historyShown = computed(() => [...faultHistory.value].sort((a, b) => b.created_ns - a.created_ns).slice(0, 10))

function setParam(key: string, v: number | boolean | string): void {
  fault.params[key] = v
}
const busy = ref('')

async function injectFault(): Promise<void> {
  busy.value = 'fault'
  try {
    const params: Record<string, unknown> = { ...fault.params }
    if (fault.type === 'BROKER_FORCE_CLOSE' && !params.qty) delete params.qty
    const res = await rt().api.demoFault({ type: fault.type, target: fault.target || undefined, params, duration_s: fault.duration || undefined })
    const r: DemoFaultResult | null = isObj(res.data) ? res.data : null
    const id = r?.fault_id ?? '—'
    const state = r?.state ?? r?.fault?.state ?? ''
    // one-shot faults (SIM_RESTART, BROKER_FORCE_CLOSE, feed_a close without refusal) are DONE at once
    showOk(`已注入故障 ${id}${state && state !== 'ACTIVE' ? `（${FAULT_STATE_TEXT[state] ?? state}：一次性故障）` : ''}${dupNote(res.duplicate)}`)
  } catch (e) {
    // TIMEOUT / UNAVAILABLE answers are recorded too: check the list below before trying again
    failed(e, '注入故障失败')
  } finally {
    busy.value = ''
    void refreshFaults()
  }
}

const removing = ref('')

async function removeFault(id: string): Promise<void> {
  removing.value = id
  try {
    const res = await rt().api.demoDeleteFault(id)
    showOk(`已撤销故障 ${id}${dupNote(res.duplicate)}`)
  } catch (e) {
    if (e instanceof ApiError && e.code === 'NOT_FOUND') showInfo(`故障 ${id} 已结束（到期或已撤销）`)
    else failed(e, '撤销故障失败')
  } finally {
    removing.value = ''
    void refreshFaults()
  }
}

// ---------------------------------------------------------------- shock
const shock = reactive({ mode: 'sector' as 'sector' | 'instruments', sector: 'black', instruments: [] as string[], pctPct: -4, duration: 30, shape: 'LINEAR' as 'STEP' | 'LINEAR' })

async function doShock(): Promise<void> {
  busy.value = 'shock'
  try {
    const body = shock.mode === 'sector' ? { sector: shock.sector, pct: shock.pctPct / 100, duration_s: shock.duration, shape: shock.shape } : { instruments: shock.instruments, pct: shock.pctPct / 100, duration_s: shock.duration, shape: shock.shape }
    const res = await rt().api.demoShock(body)
    showOk(`已施加价格冲击 ${shock.pctPct}%（${res.data?.shock_id ?? ''}）${dupNote(res.duplicate)}`)
  } catch (e) {
    failed(e, '价格冲击失败')
  } finally {
    busy.value = ''
  }
}

// ---------------------------------------------------------------- scenario
const scenario = reactive({ name: '', speed: 1 })
/** GET /demo/scenarios: config/scenarios/*.yaml of the sim */
const scenarios = ref<DemoScenarioInfo[]>([])
const scenariosError = ref<LoadError | null>(null)

async function loadScenarios(): Promise<void> {
  try {
    const list = await rt().api.demoScenarios()
    scenarios.value = Array.isArray(list) ? list : []
    scenariosError.value = null
    if (!scenario.name || !scenarios.value.some((x) => x.name === scenario.name)) scenario.name = scenarios.value[0]?.name ?? scenario.name
  } catch (e) {
    if (e instanceof ApiError && e.code === 'FORBIDDEN') forbidden.value = e.message || 'FORBIDDEN'
    scenariosError.value = toLoadError(e)
  }
}

const scenarioDesc = computed(() => scenarios.value.find((x) => x.name === scenario.name)?.description ?? '')

async function runScenario(): Promise<void> {
  if (!scenario.name) {
    showError(new Error('请选择剧本'))
    return
  }
  busy.value = 'scenario'
  try {
    const res = await rt().api.demoScenario({ name: scenario.name, speed: scenario.speed })
    showOk(`剧本 ${res.data?.name ?? scenario.name} 已启动（${res.data?.run_id ?? ''}）${dupNote(res.duplicate)}`)
  } catch (e) {
    if (e instanceof ApiError && e.code === 'CONFLICT') showWarn(`${e.message}：先停止当前剧本`)
    else failed(e, '启动剧本失败')
  } finally {
    busy.value = ''
    void refreshState()
  }
}

async function stopScenario(): Promise<void> {
  busy.value = 'scenario-stop'
  try {
    const res = await rt().api.demoCancelScenario()
    scenarioRun.value = res.data
    const st = res.data?.status
    showOk(st === 'CANCELED' ? `剧本 ${res.data.name} 已停止（第 ${res.data.step}/${res.data.steps_total} 步）${dupNote(res.duplicate)}` : `剧本 ${res.data?.name ?? ''} 已结束（${SCENARIO_STATUS_TEXT[st] ?? st}），无需停止`)
  } catch (e) {
    if (e instanceof ApiError && e.code === 'NOT_FOUND') showInfo('虚拟交易所还没有运行过剧本')
    else failed(e, '停止剧本失败')
  } finally {
    busy.value = ''
    void refreshState()
  }
}

const SCENARIO_STATUS_TEXT: Record<string, string> = { RUNNING: '运行中', DONE: '已完成', CANCELED: '已停止', FAILED: '失败' }

function runLogText(e: Record<string, unknown>): string {
  const parts = [`#${String(e.step ?? '')}`, String(e.action ?? '')]
  if (e.note) parts.push(String(e.note))
  if (e.fault_id) parts.push(String(e.fault_id))
  if (e.shock_id) parts.push(String(e.shock_id))
  return parts.join(' · ')
}

// ---------------------------------------------------------------- kill
const killTarget = ref('md-gateway')
const killable = computed(() => props.instances.filter((i) => i !== 'supervisor'))

async function kill(): Promise<void> {
  busy.value = 'kill'
  try {
    const res = await rt().api.demoKill(killTarget.value)
    showOk(`已 kill ${killTarget.value}：supervisor 将按退避策略自动重启${dupNote(res.duplicate)}`)
  } catch (e) {
    failed(e, 'kill 失败')
  } finally {
    busy.value = ''
  }
}

// ---------------------------------------------------------------- clock
/** `at` is a Beijing wall time typed / picked as text, never the browser's local time. */
const clock = reactive({ target: 'PRE_CLOSE' as ClockTarget, at: '' })
const CLOCK_TARGETS: { value: ClockTarget; label: string; hint: string }[] = [
  { value: 'PRE_CLOSE', label: '收盘前', hint: '跳到当日 demo.pre_close_at（默认 14:58）' },
  { value: 'NEXT_OPEN', label: '下一开盘', hint: '跳到下一交易时段开始前 demo.lead_s（默认 120s）' },
  { value: 'AT', label: '指定时刻', hint: '跳到指定北京时间（只能向前）；例如 15:31 触发 EOD' },
]
const atNs = computed(() => parseShanghaiWallTime(clock.at))
const lastJump = ref<{ ok: boolean; code: string; message: string; result: ClockJumpResult } | null>(null)

watch(
  () => clock.target,
  (t) => {
    const mn = system.marketNowAt()
    if (t === 'AT' && !clock.at && mn) clock.at = toShanghaiWall(Math.ceil(mn / 60e9) * 60e9)
  },
)

function isJumpResult(x: unknown): x is ClockJumpResult {
  return isObj(x) && Array.isArray(x.steps) && 'converged' in x
}

async function jump(): Promise<void> {
  if (clock.target === 'AT' && atNs.value === null) {
    showError(new Error('请输入目标北京时间，格式 YYYY-MM-DD HH:mm[:ss]'))
    return
  }
  busy.value = 'clock'
  try {
    const at = atNs.value
    const body = clock.target === 'AT' && at !== null ? { target: clock.target, at: toShanghaiIso(at / 1e6), at_ns: at } : { target: clock.target }
    const res = await rt().api.demoClock(body)
    lastJump.value = { ok: true, code: res.code, message: res.message, result: res.data }
    if (res.data.converged) showOk(`市场时钟已跳转（${CLOCK_TARGETS.find((c) => c.value === clock.target)?.label}）${dupNote(res.duplicate)}`)
    else showWarn(`市场时钟已跳转，但 ${res.data.lagging.join('，')} 在 ${res.data.waited_ms} ms 内未确认新版本`)
    void system.loadStatus().catch(() => undefined)
  } catch (e) {
    if (e instanceof ApiError && isJumpResult(e.data)) lastJump.value = { ok: false, code: e.code, message: e.message, result: e.data }
    failed(e, '时钟跳转失败')
  } finally {
    busy.value = ''
  }
}

const STEP_LABELS: Record<string, string> = {
  'sim.pause': '暂停虚拟交易所',
  'svc.clock': 'supervisor 移动时钟',
  'sim.clock': '虚拟交易所跟随',
  'wait.heartbeats': '等待各服务确认新版本',
  'sim.resume': '恢复虚拟交易所',
}

function stepDetail(d: unknown): string {
  if (d === null || d === undefined) return ''
  if (typeof d === 'string') return d
  if (isObj(d)) {
    if (Array.isArray(d.lagging)) return d.lagging.length ? `未确认：${d.lagging.join('，')}（等待 ${String(d.waited_ms ?? '')} ms）` : `全部确认（${String(d.waited_ms ?? '')} ms）`
    if (typeof d.version === 'number') return `版本 ${d.version} · ${fmtDateTime(d.market_now_ns)}`
    if (typeof d.code === 'string') return `${d.code} ${String(d.message ?? '')}`
  }
  return JSON.stringify(d)
}

const marketNow = computed(() => {
  const ns = system.marketNowAt(now.value)
  return ns ? fmtDateTime(ns) : '—'
})

// load the sim state and the scenario list whenever the controls are usable
watch(
  disabled,
  (d) => {
    if (!d) {
      void refreshState()
      void loadScenarios()
    }
  },
  { immediate: true },
)
</script>

<template>
  <div class="demo">
    <SourceNotice v-if="notDemo" :message="`api-gateway 运行在 app.mode=${system.appMode}：/demo/* 只在 demo 模式开放（其余时候返回 403 FORBIDDEN），演示控制已停用。`" />
    <SourceNotice v-else-if="!auth.canAdmin" :message="`当前 API key 的角色为 ${auth.role}：演示控制需要 ADMIN 角色。`" />
    <SourceNotice v-else-if="forbidden" :error="{ code: 'FORBIDDEN', message: forbidden }" subject="演示控制" />
    <div v-else class="muted note">
      演示接口只在 app.mode=demo 时开放，需要 ADMIN 权限。每次点击使用新的 Idempotency-Key；网关会记录每次结果（含超时 / 不可用），
      超时后请先看下方的当前故障与剧本状态再决定是否重试。
    </div>
    <SourceNotice v-if="!disabled && simError" :error="simError" subject="虚拟交易所状态（GET /demo/state）" :retry="refreshState" compact />

    <fieldset :disabled="disabled" class="fs">
      <section>
        <h4>故障注入 <span class="muted">POST /demo/faults</span></h4>
        <el-form inline size="small" :disabled="disabled" @submit.prevent>
          <el-form-item label="类型">
            <el-select v-model="fault.type" style="width: 170px">
              <el-option v-for="f in FAULT_TYPES" :key="f.value" :value="f.value" :label="f.label" />
            </el-select>
          </el-form-item>
          <el-form-item v-if="faultDef?.targets.length" label="目标">
            <el-select v-model="fault.target" style="width: 110px">
              <el-option v-for="t in faultDef?.targets" :key="t" :value="t" :label="t || '全部行情源'" />
            </el-select>
          </el-form-item>
          <el-form-item v-for="p in faultDef?.params ?? []" :key="p.key" :label="p.label">
            <el-switch v-if="typeof p.default === 'boolean'" :model-value="Boolean(fault.params[p.key])" @update:model-value="(v: string | number | boolean) => setParam(p.key, Boolean(v))" />
            <el-select v-else-if="p.options" :model-value="String(fault.params[p.key] ?? '')" style="width: 100px" @update:model-value="(v: string) => setParam(p.key, v)">
              <el-option v-for="o in p.options" :key="o" :value="o" :label="o" />
            </el-select>
            <el-select v-else-if="p.kind === 'account'" :model-value="String(fault.params[p.key] ?? '')" style="width: 100px" @update:model-value="(v: string) => setParam(p.key, v)">
              <el-option v-for="a in props.accounts ?? []" :key="a" :value="a" :label="a" />
            </el-select>
            <el-select v-else-if="typeof p.default === 'string'" :model-value="String(fault.params[p.key] ?? '')" style="width: 110px" @update:model-value="(v: string) => setParam(p.key, v)">
              <el-option v-for="i in instruments" :key="i" :value="i" :label="i" />
            </el-select>
            <el-input-number
              v-else
              :model-value="Number(fault.params[p.key] ?? 0)"
              :step="p.step ?? 1"
              :min="p.min"
              :max="p.max"
              controls-position="right"
              style="width: 110px"
              @update:model-value="(v: number | undefined) => setParam(p.key, Number(v ?? 0))"
            />
          </el-form-item>
          <el-form-item label="持续（秒，可空）">
            <el-input-number v-model="fault.duration" :min="0" :step="10" controls-position="right" style="width: 100px" />
          </el-form-item>
          <el-form-item>
            <el-button type="warning" :loading="busy === 'fault'" :disabled="disabled" @click="injectFault">注入</el-button>
          </el-form-item>
        </el-form>
        <div class="sub">当前故障 <span class="muted">GET /demo/faults · {{ faults.length }} 个生效</span></div>
        <el-table :data="faults" size="small" max-height="220" :empty-text="simError ? '虚拟交易所状态暂不可用' : '当前没有生效的故障'">
          <el-table-column label="故障" min-width="150">
            <template #default="{ row }"><b>{{ faultLabel(row as SimFault) }}</b><span v-if="row.target" class="muted"> · {{ row.target }}</span></template>
          </el-table-column>
          <el-table-column label="参数" min-width="170">
            <template #default="{ row }"><span class="muted mono small">{{ faultParams(row as SimFault) || '—' }}</span></template>
          </el-table-column>
          <el-table-column label="注入时间" width="90">
            <template #default="{ row }"><span class="mono">{{ fmtTime(row.created_ns) }}</span></template>
          </el-table-column>
          <el-table-column label="剩余" width="80" align="right">
            <template #default="{ row }">{{ row.remaining_s === null ? '直到撤销' : fmtDuration(row.remaining_s * 1000) }}</template>
          </el-table-column>
          <el-table-column label="ID" min-width="120">
            <template #default="{ row }"><span class="mono small">{{ row.fault_id }}</span></template>
          </el-table-column>
          <el-table-column label="" width="70" align="right">
            <template #default="{ row }">
              <el-button link size="small" type="primary" :loading="removing === row.fault_id" :disabled="disabled" @click="removeFault(row.fault_id)">撤销</el-button>
            </template>
          </el-table-column>
        </el-table>
        <details v-if="historyShown.length" class="history">
          <summary class="muted small">最近结束的故障（{{ faultHistory.length }}）</summary>
          <div v-for="f in historyShown" :key="f.fault_id" class="small">
            <span class="mono">{{ fmtTime(f.created_ns) }}</span> {{ faultLabel(f) }}<span v-if="f.target" class="muted"> · {{ f.target }}</span>
            <span class="muted"> · {{ FAULT_STATE_TEXT[f.state] ?? f.state }} · {{ f.fault_id }}</span>
          </div>
        </details>
      </section>

      <section>
        <h4>价格冲击 <span class="muted">POST /demo/shock</span></h4>
        <el-form inline size="small" :disabled="disabled" @submit.prevent>
          <el-form-item label="对象">
            <el-radio-group v-model="shock.mode">
              <el-radio-button value="sector">板块</el-radio-button>
              <el-radio-button value="instruments">合约</el-radio-button>
            </el-radio-group>
          </el-form-item>
          <el-form-item v-if="shock.mode === 'sector'" label="板块">
            <el-select v-model="shock.sector" style="width: 110px">
              <el-option v-for="s in SECTOR_OPTIONS" :key="s.value" :value="s.value" :label="s.label" />
            </el-select>
          </el-form-item>
          <el-form-item v-else label="合约">
            <el-select v-model="shock.instruments" multiple collapse-tags style="width: 200px">
              <el-option v-for="i in instruments" :key="i" :value="i" :label="i" />
            </el-select>
          </el-form-item>
          <el-form-item label="幅度 %">
            <el-input-number v-model="shock.pctPct" :min="-10" :max="10" :step="0.5" controls-position="right" style="width: 100px" />
          </el-form-item>
          <el-form-item label="时长（秒）">
            <el-input-number v-model="shock.duration" :min="0" :step="10" controls-position="right" style="width: 100px" />
          </el-form-item>
          <el-form-item label="形态">
            <el-radio-group v-model="shock.shape">
              <el-radio-button value="LINEAR">线性</el-radio-button>
              <el-radio-button value="STEP">阶跃</el-radio-button>
            </el-radio-group>
          </el-form-item>
          <el-form-item>
            <el-button type="warning" :loading="busy === 'shock'" :disabled="disabled" @click="doShock">施加</el-button>
          </el-form-item>
        </el-form>
      </section>

      <section class="row2">
        <div>
          <h4>场景剧本 <span class="muted">GET /demo/scenarios · POST / DELETE /demo/scenario</span></h4>
          <SourceNotice v-if="scenariosError" :error="scenariosError" subject="剧本列表" :retry="loadScenarios" compact />
          <el-form inline size="small" :disabled="disabled" @submit.prevent>
            <el-form-item>
              <el-select v-model="scenario.name" filterable allow-create default-first-option placeholder="剧本名" style="width: 200px">
                <el-option v-for="x in scenarios" :key="x.name" :value="x.name" :label="x.name" />
              </el-select>
            </el-form-item>
            <el-form-item label="速度">
              <el-input-number v-model="scenario.speed" :min="0.1" :max="20" :step="0.5" controls-position="right" style="width: 90px" />
            </el-form-item>
            <el-form-item>
              <el-button type="warning" :loading="busy === 'scenario'" :disabled="disabled || scenarioRunning" @click="runScenario">执行</el-button>
              <el-button :loading="busy === 'scenario-stop'" :disabled="disabled || !scenarioRunning" @click="stopScenario">停止剧本</el-button>
            </el-form-item>
          </el-form>
          <div v-if="scenarioDesc" class="muted small">{{ scenarioDesc }}</div>
          <div v-if="scenarioRun" class="run small">
            <b>{{ scenarioRun.name }}</b>
            <span :class="['run-st', scenarioRun.status]">{{ SCENARIO_STATUS_TEXT[scenarioRun.status] ?? scenarioRun.status }}</span>
            <span class="muted">第 {{ scenarioRun.step }}/{{ scenarioRun.steps_total }} 步 · ×{{ scenarioRun.speed }} · 开始 {{ fmtTime(scenarioRun.started_ns) }}</span>
            <span v-if="scenarioRun.error" class="run-err"> · {{ scenarioRun.error }}</span>
            <div v-if="scenarioRun.log.length" class="muted mono">{{ runLogText(scenarioRun.log[scenarioRun.log.length - 1] ?? {}) }}</div>
          </div>
        </div>
        <div>
          <h4>Kill 服务 <span class="muted">POST /demo/kill/{instance}</span></h4>
          <el-form inline size="small" :disabled="disabled" @submit.prevent>
            <el-form-item>
              <el-select v-model="killTarget" style="width: 200px">
                <el-option v-for="i in killable" :key="i" :value="i" :label="`${i} · ${SERVICE_LABELS[i] ?? ''}`" />
              </el-select>
            </el-form-item>
            <el-form-item>
              <el-popconfirm :title="`确认 kill ${killTarget}？进程将被直接终止以模拟崩溃`" confirm-button-text="kill" cancel-button-text="取消" @confirm="kill">
                <template #reference>
                  <el-button type="danger" :loading="busy === 'kill'" :disabled="disabled">Kill</el-button>
                </template>
              </el-popconfirm>
            </el-form-item>
          </el-form>
        </div>
      </section>

      <section>
        <h4>市场时钟跳转 <span class="muted">POST /demo/clock · 当前 {{ marketNow }}（北京时间）· 版本 {{ system.clock?.version ?? '—' }}</span></h4>
        <el-form inline size="small" :disabled="disabled" @submit.prevent>
          <el-form-item>
            <el-radio-group v-model="clock.target">
              <el-radio-button v-for="c in CLOCK_TARGETS" :key="c.value" :value="c.value" :title="c.hint">{{ c.label }}</el-radio-button>
            </el-radio-group>
          </el-form-item>
          <el-form-item v-if="clock.target === 'AT'" label="北京时间">
            <el-input v-model="clock.at" placeholder="YYYY-MM-DD HH:mm:ss" style="width: 180px" :class="{ bad: clock.at && atNs === null }" />
          </el-form-item>
          <el-form-item>
            <el-button type="warning" :loading="busy === 'clock'" :disabled="disabled" @click="jump">跳转</el-button>
          </el-form-item>
        </el-form>
        <div class="muted hint">{{ CLOCK_TARGETS.find((c) => c.value === clock.target)?.hint }}。演示 EOD：先「收盘前」看收盘，再「指定时刻」跳到 15:30 之后触发 EOD，最后「下一开盘」。</div>
        <div v-if="lastJump" class="jump">
          <div class="jump-head">
            <b>上次跳转</b>
            <span :class="lastJump.ok ? (lastJump.result.converged ? 'ok' : 'warn') : 'bad'">
              {{ lastJump.ok ? (lastJump.result.converged ? '已完成，各服务已确认新时钟版本' : `已完成，但 ${lastJump.result.lagging.join('，')} 未在 ${lastJump.result.waited_ms} ms 内确认`) : `失败：${lastJump.code} ${lastJump.message}` }}
            </span>
            <span v-if="lastJump.result.clock" class="muted">新时钟 {{ fmtDateTime(lastJump.result.clock.market_now_ns) }} · 版本 {{ lastJump.result.clock.version }}</span>
          </div>
          <ol class="steps">
            <li v-for="st in lastJump.result.steps" :key="st.step" :class="st.ok ? 'ok' : 'bad'">
              {{ st.ok ? '✓' : '✗' }} {{ STEP_LABELS[st.action] ?? st.action }}<span class="muted"> · {{ st.action }}</span>
              <span v-if="stepDetail(st.detail)" class="muted"> · {{ stepDetail(st.detail) }}</span>
            </li>
          </ol>
        </div>
      </section>
    </fieldset>
  </div>
</template>

<style scoped>
.fs {
  border: none;
  margin: 0;
  padding: 0;
  min-width: 0;
}
.note {
  font-size: 12px;
}
.demo section {
  margin-top: 14px;
}
.demo h4 {
  margin: 0 0 8px;
  font-size: 13px;
}
.demo h4 .muted {
  font-weight: 400;
  font-size: 12px;
  margin-left: 6px;
}
.sub {
  font-size: 12.5px;
  font-weight: 600;
  margin: 6px 0 4px;
}
.sub .muted {
  font-weight: 400;
  margin-left: 6px;
}
.history {
  margin-top: 6px;
  line-height: 1.7;
}
.run {
  margin-top: 6px;
  display: flex;
  flex-wrap: wrap;
  gap: 4px 8px;
  align-items: baseline;
}
.run .mono {
  flex-basis: 100%;
}
.run-st {
  font-weight: 600;
}
.run-st.RUNNING {
  color: #1c5cab;
}
.run-st.FAILED,
.run-err {
  color: #a8071a;
}
.small {
  font-size: 12px;
}
.row2 {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 14px;
}
@media (max-width: 1100px) {
  .row2 {
    grid-template-columns: minmax(0, 1fr);
  }
}
.hint {
  font-size: 12px;
}
.bad :deep(.el-input__wrapper) {
  box-shadow: 0 0 0 1px #e6a23c inset;
}
.jump {
  margin-top: 8px;
  font-size: 12.5px;
  border: 1px solid var(--fr-line);
  border-radius: var(--fr-radius);
  padding: 8px 10px;
}
.jump-head {
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
  align-items: baseline;
}
.jump .ok {
  color: #1f7a1f;
}
.jump .warn {
  color: #9a6700;
}
.jump .bad {
  color: #a8071a;
}
.steps {
  margin: 6px 0 0;
  padding-left: 18px;
  line-height: 1.7;
}
</style>
