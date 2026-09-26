<script setup lang="ts">
/**
 * Calm inline notice for degraded answers of the api-gateway:
 *  - `error`: the request failed as a whole (e.g. 503 UNAVAILABLE "risk-engine-1: peer is
 *    down (no heartbeat)") — the section shows what it has, with this note;
 *  - `errors`: a partial answer — `{source: code}` of the shards / days that did not
 *    answer (`/risk/events`, `/settlement/days`, `/risk/compare/history`);
 *  - `missing`: configured items without data (e.g. `/positions` `accounts_missing`);
 *  - `message`: the envelope message of an OK answer ("partial: …", "rules from the
 *    local config", "statement file not available: settlement data instead").
 * Neutral / amber styling (never the red of a risk state): these are data-availability
 * notes, not risk events.
 */
import { computed } from 'vue'
import { InfoFilled, WarningFilled } from '@element-plus/icons-vue'
import type { LoadError } from '@/stores/system'
import { codeLabel } from '@/utils/labels'

const props = withDefaults(
  defineProps<{
    error?: LoadError | null
    errors?: Record<string, string> | null
    missing?: string[] | null
    missingLabel?: string
    /** lead-in of the `errors` list */
    errorsLabel?: string
    message?: string | null
    /** what is affected, e.g. "风险事件" */
    subject?: string
    retry?: (() => unknown) | null
    compact?: boolean
    /** force the styling (default: amber for errors, neutral for messages) */
    tone?: 'warn' | 'info' | null
  }>(),
  { error: null, errors: null, missing: null, missingLabel: '暂无数据', errorsLabel: '以下来源未应答', message: '', subject: '', retry: null, compact: false, tone: null },
)

const errorEntries = computed(() => Object.entries(props.errors ?? {}))

const visible = computed(() => Boolean(props.error) || errorEntries.value.length > 0 || (props.missing?.length ?? 0) > 0 || Boolean(props.message))

const toneClass = computed<'warn' | 'info'>(() => props.tone ?? (props.error || errorEntries.value.length ? 'warn' : 'info'))

function codeText(code: string): string {
  const l = codeLabel(code)
  return l ? `${code}（${l}）` : code
}

/** "risk-engine-0, risk-engine-1 = UNAVAILABLE" grouped by code. */
const grouped = computed(() => {
  const by = new Map<string, string[]>()
  for (const [k, v] of errorEntries.value) by.set(v, [...(by.get(v) ?? []), k])
  return [...by.entries()].map(([code, keys]) => ({ code, keys }))
})
</script>

<template>
  <div v-if="visible" class="notice" :class="[toneClass, { compact }]" role="status">
    <el-icon class="ic"><WarningFilled v-if="toneClass === 'warn'" /><InfoFilled v-else /></el-icon>
    <div class="body">
      <template v-if="error">
        <span>{{ subject ? `${subject}暂不可用` : '暂不可用' }}：</span>
        <b class="mono">{{ codeText(error.code) }}</b>
        <span v-if="error.message && error.message !== error.code" class="muted"> · {{ error.message }}</span>
        <span v-if="error.errorId && !error.message.includes(error.errorId)" class="muted"> · 错误编号 <span class="mono">{{ error.errorId }}</span></span>
      </template>
      <template v-else-if="grouped.length">
        <span>{{ subject ? `${subject}为部分结果` : '部分结果' }}，{{ errorsLabel }}：</span>
        <span v-for="g in grouped" :key="g.code" class="grp">
          <span class="mono">{{ g.keys.length > 6 ? `${g.keys.slice(0, 6).join('，')} 等 ${g.keys.length} 项` : g.keys.join('，') }}</span>
          = <b class="mono">{{ codeText(g.code) }}</b>
        </span>
      </template>
      <template v-if="missing && missing.length">
        <span>{{ missingLabel }}：</span><span class="mono">{{ missing.join('，') }}</span>
      </template>
      <span v-if="message && !error" class="muted msg">{{ grouped.length || missing?.length ? ' · ' : '' }}{{ message }}</span>
      <slot />
    </div>
    <el-button v-if="retry" link size="small" class="retry" @click="retry()">重试</el-button>
  </div>
</template>

<style scoped>
.notice {
  display: flex;
  align-items: flex-start;
  gap: 8px;
  padding: 7px 10px;
  border-radius: var(--fr-radius);
  font-size: 12.5px;
  line-height: 1.6;
  margin: 0 0 10px;
  color: var(--fr-ink-2);
}
.notice.compact {
  padding: 4px 8px;
  margin: 0 0 6px;
}
.notice.warn {
  background: #fff8e8;
  border: 1px solid #f3dca8;
}
.notice.info {
  background: #f4f7fb;
  border: 1px solid #dfe6f0;
}
.ic {
  margin-top: 3px;
  flex: none;
}
.warn .ic {
  color: #c98a07;
}
.info .ic {
  color: #7b8aa3;
}
.body {
  flex: 1;
  min-width: 0;
  overflow-wrap: anywhere;
}
.grp + .grp::before {
  content: '；';
}
.retry {
  flex: none;
}
</style>
