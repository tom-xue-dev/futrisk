<script setup lang="ts">
/**
 * Settings: API key (localStorage, sent as X-API-Key and in the WS auth frame), mock
 * toggle (reloads the page), up/down colors, store throttle, notifications, WS
 * diagnostics and mock-only protocol demos (seq gap -> resync, drop -> reconnect).
 */
import { computed, ref, watch } from 'vue'
import { useSettings, envMock, type ColorScheme } from '@/stores/settings'
import { useConnection } from '@/stores/connection'
import { ROLE_LABELS, useAuth } from '@/stores/auth'
import { rt } from '@/runtime'
import { fmtDuration } from '@/utils/format'
import { showOk } from '@/composables/feedback'

const props = defineProps<{ modelValue: boolean }>()
const emit = defineEmits<{ 'update:modelValue': [boolean]; saved: [] }>()

const settings = useSettings()
const conn = useConnection()
const auth = useAuth()
const mockAppMode = ref<'demo' | 'live'>('demo')

const apiKey = ref('')
const rememberKey = ref(false)
const mock = ref(false)
const colorScheme = ref<ColorScheme>('red-up')
const throttleMs = ref(200)
const notify = ref(true)

watch(
  () => props.modelValue,
  (open) => {
    if (!open) return
    apiKey.value = settings.apiKey
    rememberKey.value = settings.rememberKey
    mock.value = settings.mock
    colorScheme.value = settings.colorScheme
    throttleMs.value = settings.throttleMs
    notify.value = settings.notify
    mockAppMode.value = rt().mock?.appMode() ?? 'demo'
  },
  { immediate: true },
)

const isMock = computed(() => rt().isMock)
const statusText = computed(() => {
  switch (conn.status) {
    case 'open':
      return '已连接'
    case 'connecting':
      return '连接中'
    case 'reconnecting':
      return '重连中'
    case 'no-key':
      return '未设置 API key'
    case 'unauthorized':
      return 'API key 被拒绝（关闭码 4401），请更换后保存'
    case 'closed':
      return '已断开'
    default:
      return '空闲'
  }
})

function save(): void {
  const mockChanged = mock.value !== settings.mock
  const keyChanged = apiKey.value.trim() !== settings.apiKey
  settings.save({
    apiKey: apiKey.value.trim(),
    rememberKey: rememberKey.value,
    mockOverride: mock.value === envMock() ? null : mock.value,
    colorScheme: colorScheme.value,
    throttleMs: throttleMs.value,
    notify: notify.value,
  })
  emit('update:modelValue', false)
  if (mockChanged) {
    location.reload()
    return
  }
  if (keyChanged) rt().ws.restart()
  emit('saved')
  showOk('设置已保存')
}

function dropSeq(): void {
  rt().mock?.dropNextSeq()
  showOk('已让每个订阅主题的下一帧跳过一个 seq：客户端应检测到缺口并发送 resync')
}

function dropConn(): void {
  rt().mock?.disconnectAll()
  showOk('已断开 WebSocket：客户端将按指数退避重连并重新订阅（服务端对每个 sub 回快照）')
}

function serverResync(): void {
  rt().mock?.serverResync('events')
  showOk('服务端对 events 主题发送了 resync 帧：客户端应重置基线、请求快照并用 REST 补拉当日事件')
}

function setMockAppMode(v: string | number | boolean | undefined): void {
  const mode = v === 'live' ? 'live' : 'demo'
  mockAppMode.value = mode
  rt().mock?.setAppMode(mode)
  showOk(mode === 'demo' ? '模拟后端：app.mode=demo（演示控制可用）' : '模拟后端：app.mode=live（/demo/* 返回 403）')
}
</script>

<template>
  <el-dialog :model-value="modelValue" title="设置" width="560px" append-to-body @update:model-value="emit('update:modelValue', $event)">
    <el-form label-width="120px" label-position="left">
      <el-form-item label="API key">
        <el-input v-model="apiKey" type="password" show-password placeholder="X-API-Key（首次启动见 <home>/secrets/bootstrap_admin.key）" clearable />
        <el-checkbox v-model="rememberKey">在本机记住此 key</el-checkbox>
        <div class="muted hint">
          默认只保存在当前标签页（sessionStorage，关闭即清除）；勾选后保存在本机浏览器（localStorage），共用电脑请勿勾选。
        </div>
        <div class="muted hint">
          <template v-if="auth.me">当前身份：{{ auth.me.name || auth.me.key_id }}（{{ auth.me.key_id }}，{{ ROLE_LABELS[auth.me.role] ?? auth.me.role }} {{ auth.me.role }}）</template>
          <template v-else-if="auth.code && auth.code !== 'OK'">GET /auth/me：{{ auth.code }} {{ auth.message }}</template>
        </div>
      </el-form-item>
      <el-form-item label="模拟数据">
        <el-switch v-model="mock" active-text="模拟模式（浏览器内置后端）" inactive-text="连接 api-gateway" />
        <div class="muted hint">默认值来自 VITE_MOCK（当前构建：{{ envMock() ? '开启' : '关闭' }}）；切换后页面会刷新。</div>
      </el-form-item>
      <el-form-item label="涨跌配色">
        <el-radio-group v-model="colorScheme">
          <el-radio-button value="red-up">红涨绿跌</el-radio-button>
          <el-radio-button value="green-up">绿涨红跌</el-radio-button>
        </el-radio-group>
      </el-form-item>
      <el-form-item label="刷新节流">
        <el-slider v-model="throttleMs" :min="50" :max="2000" :step="50" show-input :show-input-controls="false" style="width: 100%" />
        <div class="muted hint">WS 帧合并后批量写入 store 的最小间隔（毫秒）</div>
      </el-form-item>
      <el-form-item label="告警通知">
        <el-switch v-model="notify" active-text="高 / 严重事件弹出通知" />
      </el-form-item>
    </el-form>

    <el-divider content-position="left">WebSocket 诊断（02 §9.7）</el-divider>
    <dl class="kv">
      <dt>状态</dt>
      <dd>{{ statusText }}<span v-if="conn.stats.lastCloseCode !== null" class="muted">（上次关闭码 {{ conn.stats.lastCloseCode }}）</span></dd>
      <dt>收到帧</dt>
      <dd>{{ conn.stats.received }}</dd>
      <dt>seq 缺口 / 重复</dt>
      <dd>{{ conn.stats.gaps }} / {{ conn.stats.duplicates }}</dd>
      <dt>resync 请求 / 服务端 resync</dt>
      <dd>{{ conn.stats.resyncRequests }} / {{ conn.stats.serverResyncs }}</dd>
      <dt>重连次数</dt>
      <dd>{{ conn.stats.reconnects }}<span v-if="conn.stats.nextRetryMs" class="muted">（{{ fmtDuration(Math.max(0, conn.stats.nextRetryMs - Date.now())) }} 后重试）</span></dd>
      <dt>最后一帧</dt>
      <dd>{{ conn.stats.lastMessageMs ? fmtDuration(Date.now() - conn.stats.lastMessageMs) + ' 前' : '—' }}</dd>
    </dl>
    <div v-if="isMock" class="mock-tools">
      <el-button size="small" @click="dropSeq">模拟 WS 跳号</el-button>
      <el-button size="small" @click="dropConn">模拟 WS 断线</el-button>
      <el-button size="small" @click="serverResync">模拟服务端 resync</el-button>
      <el-button size="small" type="warning" plain @click="rt().mock?.restartScenario()">重新开始演示</el-button>
      <span class="mode">
        模拟 app.mode
        <el-radio-group :model-value="mockAppMode" size="small" @update:model-value="setMockAppMode">
          <el-radio-button value="demo">demo</el-radio-button>
          <el-radio-button value="live">live</el-radio-button>
        </el-radio-group>
      </span>
    </div>

    <template #footer>
      <el-button @click="emit('update:modelValue', false)">取消</el-button>
      <el-button type="primary" @click="save">保存</el-button>
    </template>
  </el-dialog>
</template>

<style scoped>
.hint {
  font-size: 12px;
  line-height: 1.5;
  margin-top: 2px;
}
.mock-tools {
  display: flex;
  gap: 8px;
  margin-top: 12px;
  flex-wrap: wrap;
  align-items: center;
}
.mock-tools .mode {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  font-size: 12px;
  color: var(--fr-ink-2);
}
</style>
