<script setup lang="ts">
/**
 * Confirmation dialog for audited manual actions (reset, manual halt, kill switch,
 * liquidation confirm). One Idempotency-Key per dialog session: a retry after a
 * timeout / network error / UNAVAILABLE reuses it so the gateway deduplicates (01 §8.6;
 * those codes are not recorded, so the retry runs again or replays the first answer with
 * `duplicate: true` — the kill switch records every outcome, so its retry always replays
 * the recorded answer while the gateway keeps retrying the targets itself). The gateway
 * fingerprints method + path + key + body, so the key is only reused while the body
 * (reason) is unchanged — the same key with another body would be a 409 CONFLICT. After
 * a business error a fresh key is used.
 */
import { ref, watch } from 'vue'
import { ApiError } from '@/api/http'
import { errorText } from '@/composables/feedback'
import { uuid } from '@/utils/uuid'

const props = withDefaults(
  defineProps<{
    modelValue: boolean
    title: string
    description?: string
    confirmText?: string
    danger?: boolean
    requireReason?: boolean
    reasonLabel?: string
    placeholder?: string
    action: (reason: string, idempotencyKey: string) => Promise<unknown>
  }>(),
  { description: '', confirmText: '确认', danger: false, requireReason: true, reasonLabel: '原因（写入审计）', placeholder: '例如：已核实价格冲击属实，逐步恢复交易' },
)
const emit = defineEmits<{ 'update:modelValue': [boolean]; done: [unknown] }>()

const reason = ref('')
const loading = ref(false)
const error = ref('')
const key = ref(uuid())
/** reason sent under `key` by an attempt that may have reached the gateway (retryable failure) */
const sentReason = ref<string | null>(null)

const RETRYABLE = new Set(['TIMEOUT', 'UNAVAILABLE', 'BUSY', 'RISK_BUSY'])

watch(
  () => props.modelValue,
  (open) => {
    if (open) {
      reason.value = ''
      error.value = ''
      key.value = uuid()
      sentReason.value = null
    }
  },
)

async function submit(): Promise<void> {
  if (props.requireReason && !reason.value.trim()) {
    error.value = '请填写原因：人工干预必须记录原因（01 §8.8 审计）'
    return
  }
  const r = reason.value.trim()
  if (sentReason.value !== null && sentReason.value !== r) {
    key.value = uuid()
    sentReason.value = null
  }
  loading.value = true
  error.value = ''
  try {
    const res = await props.action(r, key.value)
    emit('done', res)
    emit('update:modelValue', false)
  } catch (e) {
    error.value = errorText(e)
    const retryable = e instanceof ApiError && RETRYABLE.has(e.code)
    if (retryable) sentReason.value = r
    else {
      key.value = uuid()
      sentReason.value = null
    }
  } finally {
    loading.value = false
  }
}
</script>

<template>
  <el-dialog :model-value="modelValue" :title="title" width="480px" append-to-body @update:model-value="emit('update:modelValue', $event)">
    <p v-if="description" class="desc">{{ description }}</p>
    <slot />
    <el-form label-position="top" @submit.prevent>
      <el-form-item v-if="requireReason" :label="reasonLabel" required>
        <el-input v-model="reason" type="textarea" :rows="3" :placeholder="placeholder" maxlength="300" show-word-limit />
      </el-form-item>
    </el-form>
    <el-alert v-if="error" :title="error" type="error" show-icon :closable="false" />
    <div class="key muted mono">Idempotency-Key: {{ key }}</div>
    <div v-if="sentReason !== null" class="key muted">重试将沿用同一 Idempotency-Key（网关去重，已执行的请求不会重复执行）；修改原因会改用新 key。</div>
    <template #footer>
      <el-button @click="emit('update:modelValue', false)">取消</el-button>
      <el-button :type="danger ? 'danger' : 'primary'" :loading="loading" @click="submit">{{ confirmText }}</el-button>
    </template>
  </el-dialog>
</template>

<style scoped>
.desc {
  margin: 0 0 12px;
  color: var(--fr-ink-2);
  line-height: 1.6;
}
.key {
  margin-top: 8px;
  font-size: 11px;
}
</style>
