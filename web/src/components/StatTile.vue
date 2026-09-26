<script setup lang="ts">
/** Stat tile: label · value (proportional figures) · optional sub line. */
withDefaults(defineProps<{ label: string; value: string; sub?: string; valueClass?: string; subClass?: string; emphasis?: boolean }>(), {
  sub: '',
  valueClass: '',
  subClass: '',
  emphasis: false,
})
</script>

<template>
  <div class="stat" :class="{ emphasis }">
    <div class="stat-label">
      <slot name="icon" />
      <span>{{ label }}</span>
    </div>
    <div class="stat-value" :class="valueClass" :title="value">{{ value }}</div>
    <div v-if="sub || $slots.sub" class="stat-sub" :class="subClass" :title="sub">
      <slot name="sub">{{ sub }}</slot>
    </div>
  </div>
</template>

<style scoped>
.stat {
  background: var(--fr-surface);
  border: 1px solid var(--fr-line);
  border-radius: var(--fr-radius);
  padding: 12px 14px;
  min-width: 0;
}
.stat-label {
  display: flex;
  align-items: center;
  gap: 6px;
  font-size: 12px;
  color: var(--fr-ink-2);
}
.stat-value {
  font-size: 22px;
  font-weight: 600;
  margin-top: 4px;
  line-height: 1.2;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.emphasis .stat-value {
  font-size: 28px;
}
.stat-sub {
  font-size: 12px;
  margin-top: 4px;
  color: var(--fr-ink-3);
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
</style>
