/**
 * Error / success feedback. Backend error codes (02 §11) are always shown verbatim
 * together with their Chinese meaning.
 */
import { ElMessage } from 'element-plus'
import { ApiError } from '@/api/http'
import { codeLabel } from '@/utils/labels'

export function errorText(e: unknown): string {
  if (e instanceof ApiError) {
    const meaning = codeLabel(e.code)
    const msg = e.message && e.message !== e.code ? e.message : ''
    const extra: string[] = []
    if (e.status === 405) extra.push('请求方法不被允许')
    if (e.status === 413) extra.push('请求体过大')
    // unexpected server error: the gateway log has the details under this id
    if (e.errorId && !msg.includes(e.errorId)) extra.push(`错误编号 ${e.errorId}`)
    if ((e.status === 429 || e.status === 503) && e.retryAfterMs) extra.push(`${Math.ceil(e.retryAfterMs / 1000)}s 后可重试`)
    return [e.code, meaning && meaning !== msg ? meaning : '', msg, ...extra].filter(Boolean).join(' · ')
  }
  if (e instanceof Error) return e.message
  return String(e)
}

export function showError(e: unknown, prefix = ''): void {
  ElMessage({ type: 'error', message: prefix ? `${prefix}：${errorText(e)}` : errorText(e), duration: 6000, showClose: true, grouping: true })
}

export function showOk(message: string): void {
  ElMessage({ type: 'success', message, duration: 2500, grouping: true })
}

/** Neutral notice (e.g. a replayed request, nothing to do). */
export function showInfo(message: string): void {
  ElMessage({ type: 'info', message, duration: 4000, grouping: true, showClose: true })
}

/** Partial success that needs attention but no action (e.g. a fan-out target being retried). */
export function showWarn(message: string): void {
  ElMessage({ type: 'warning', message, duration: 7000, grouping: true, showClose: true })
}

/** Await a promise and toast its error; resolves to undefined on failure. */
export async function tryApi<T>(p: Promise<T>, prefix = ''): Promise<T | undefined> {
  try {
    return await p
  } catch (e) {
    showError(e, prefix)
    return undefined
  }
}
