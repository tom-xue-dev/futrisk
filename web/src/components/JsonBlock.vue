<script setup lang="ts">
import { computed } from 'vue'

const props = withDefaults(defineProps<{ value: unknown; maxHeight?: string }>(), { maxHeight: '240px' })
const text = computed(() => {
  try {
    return JSON.stringify(props.value, null, 2)
  } catch {
    return String(props.value)
  }
})
</script>

<template>
  <pre class="json" :style="{ maxHeight }">{{ text }}</pre>
</template>

<style scoped>
.json {
  margin: 0;
  padding: 8px 10px;
  background: #f7f7f5;
  border: 1px solid var(--fr-line);
  border-radius: 6px;
  font-family: ui-monospace, 'Cascadia Mono', Consolas, monospace;
  font-size: 12px;
  overflow: auto;
  white-space: pre-wrap;
  word-break: break-all;
}
</style>
