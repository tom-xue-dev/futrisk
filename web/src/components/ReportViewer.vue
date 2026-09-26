<script lang="ts">
export interface ReportRef {
  report_id: string
  title: string
  /** file name suggestion when the answer has no Content-Disposition */
  filename?: string
}
</script>

<script setup lang="ts">
/**
 * Report viewer (GET /reports/{report_id}, fetched with the API key header).
 *
 * Report content is never shown as a same-origin document: an HTML report (risk daily /
 * backtest) is rendered in `<iframe sandbox srcdoc>` with no sandbox tokens — no scripts,
 * an opaque origin, no forms / popups / top navigation — so a script inside a report
 * cannot reach this page, its storage or the API key. Text / CSV / JSON reports are shown
 * as escaped text. "Download" saves the bytes as an attachment (octet-stream, never
 * rendered by the browser).
 */
import { computed, ref, watch } from 'vue'
import { Download } from '@element-plus/icons-vue'
import { rt } from '@/runtime'
import { errorText } from '@/composables/feedback'
import { fmtBytes } from '@/utils/format'

const props = defineProps<{ modelValue: boolean; report: ReportRef | null }>()
const emit = defineEmits<{ 'update:modelValue': [boolean] }>()

const loading = ref(false)
const error = ref('')
const kind = ref<'html' | 'text'>('text')
const content = ref('')
const size = ref(0)
const filename = ref('report')
let bytes: Blob | null = null

const title = computed(() => props.report?.title ?? '报表')

async function load(r: ReportRef): Promise<void> {
  loading.value = true
  error.value = ''
  content.value = ''
  bytes = null
  try {
    const { blob, filename: fn, contentType } = await rt().api.reportFile(r.report_id)
    bytes = blob
    size.value = blob.size
    filename.value = fn ?? r.filename ?? 'report'
    const text = await blob.text()
    kind.value = /html/i.test(contentType) || /\.html?$/i.test(filename.value) ? 'html' : 'text'
    content.value = kind.value === 'text' && /json/i.test(contentType) ? prettyJson(text) : text
  } catch (e) {
    error.value = errorText(e)
  } finally {
    loading.value = false
  }
}

function prettyJson(text: string): string {
  try {
    return JSON.stringify(JSON.parse(text), null, 2)
  } catch {
    return text
  }
}

watch(
  () => [props.modelValue, props.report?.report_id] as const,
  ([open]) => {
    if (open && props.report) void load(props.report)
    if (!open) {
      content.value = ''
      bytes = null
    }
  },
  { immediate: true },
)

function download(): void {
  if (!bytes) return
  // an attachment of opaque bytes: the browser saves it and never renders it in this origin
  const url = URL.createObjectURL(new Blob([bytes], { type: 'application/octet-stream' }))
  const a = document.createElement('a')
  a.href = url
  a.download = filename.value
  a.rel = 'noopener'
  a.click()
  setTimeout(() => URL.revokeObjectURL(url), 30_000)
}
</script>

<template>
  <el-dialog :model-value="modelValue" :title="title" width="min(1100px, 94vw)" top="4vh" append-to-body destroy-on-close @update:model-value="emit('update:modelValue', $event)">
    <div class="bar">
      <span class="muted mono">{{ filename }}<template v-if="size"> · {{ fmtBytes(size) }}</template></span>
      <span class="muted">{{ kind === 'html' ? 'HTML 在沙箱中显示（禁用脚本，独立来源）' : '' }}</span>
      <span class="spacer" />
      <el-button size="small" :icon="Download" :disabled="!content" @click="download">下载</el-button>
    </div>
    <div v-if="loading" class="muted pad">加载中…</div>
    <el-alert v-else-if="error" :title="error" type="error" show-icon :closable="false" />
    <iframe v-else-if="kind === 'html'" class="frame" sandbox="" referrerpolicy="no-referrer" :srcdoc="content" title="report" />
    <pre v-else class="text">{{ content }}</pre>
  </el-dialog>
</template>

<style scoped>
.bar {
  display: flex;
  align-items: center;
  gap: 10px;
  margin-bottom: 8px;
  font-size: 12px;
}
.spacer {
  flex: 1;
}
.pad {
  padding: 24px 0;
}
.frame {
  width: 100%;
  height: 76vh;
  border: 1px solid var(--fr-line);
  border-radius: var(--fr-radius);
  background: #fff;
}
.text {
  max-height: 76vh;
  overflow: auto;
  margin: 0;
  padding: 10px 12px;
  background: #f7f8fa;
  border: 1px solid var(--fr-line);
  border-radius: var(--fr-radius);
  font-size: 12px;
  line-height: 1.5;
  white-space: pre;
}
</style>
