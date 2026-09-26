/**
 * ECharts 5 tree-shaken registration (only the chart types and components we use)
 * and shared chart styling helpers.
 */
import { use } from 'echarts/core'
import { CanvasRenderer } from 'echarts/renderers'
import { BarChart, CustomChart, GaugeChart, GraphChart, HeatmapChart, LineChart, ScatterChart } from 'echarts/charts'
import {
  DataZoomComponent,
  GridComponent,
  LegendComponent,
  MarkAreaComponent,
  MarkLineComponent,
  MarkPointComponent,
  TitleComponent,
  TooltipComponent,
  VisualMapComponent,
  AxisPointerComponent,
} from 'echarts/components'
import { CHART_INK } from '@/utils/labels'
import { fmtClockMs } from '@/utils/format'

use([
  CanvasRenderer,
  BarChart,
  CustomChart,
  GaugeChart,
  GraphChart,
  HeatmapChart,
  LineChart,
  ScatterChart,
  DataZoomComponent,
  GridComponent,
  LegendComponent,
  MarkAreaComponent,
  MarkLineComponent,
  MarkPointComponent,
  TitleComponent,
  TooltipComponent,
  VisualMapComponent,
  AxisPointerComponent,
])

export const FONT = 'system-ui, -apple-system, "Segoe UI", "PingFang SC", "Microsoft YaHei", sans-serif'

/** Recessive hairline axis for a time axis in Asia/Shanghai. */
export function timeAxis(extra: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    type: 'time',
    axisLine: { lineStyle: { color: CHART_INK.axis } },
    axisTick: { show: false },
    splitLine: { show: false },
    axisLabel: { color: CHART_INK.muted, fontSize: 11, hideOverlap: true, formatter: (v: number) => fmtClockMs(v) },
    axisPointer: { label: { formatter: (p: { value: number }) => fmtClockMs(p.value, true) } },
    ...extra,
  }
}

export function valueAxis(extra: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    type: 'value',
    scale: true,
    axisLine: { show: false },
    axisTick: { show: false },
    splitLine: { lineStyle: { color: CHART_INK.grid, width: 1 } },
    axisLabel: { color: CHART_INK.muted, fontSize: 11 },
    ...extra,
  }
}

export const tooltipBase = {
  backgroundColor: '#ffffff',
  borderColor: 'rgba(11,11,11,0.10)',
  borderWidth: 1,
  textStyle: { color: CHART_INK.primary, fontSize: 12, fontFamily: FONT },
  extraCssText: 'box-shadow: 0 4px 16px rgba(0,0,0,0.08); border-radius: 6px;',
}

/** Escape text before putting it into an ECharts HTML tooltip (labels are data). */
export function esc(s: unknown): string {
  return String(s ?? '').replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[c] as string)
}

/** Tooltip row with a short line key in the series color (value first, label second). */
export function tipRow(color: string, label: string, value: string): string {
  return `<div style="display:flex;align-items:center;gap:8px;line-height:20px"><span style="display:inline-block;width:12px;height:2px;background:${color}"></span><b style="font-variant-numeric:tabular-nums">${esc(value)}</b><span style="color:${CHART_INK.secondary}">${esc(label)}</span></div>`
}
