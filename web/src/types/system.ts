/**
 * System payloads mirrored from `src/futrisk/messages/system.py` (02 §7.9).
 */
import type {
  ClockMode,
  ClockTarget,
  ProcessState,
  ServiceChangeReason,
  ServiceState,
  TradingMode,
} from './enums'

export interface LatencySummary {
  p50: number
  p99: number
  max: number
  n: number
}

/** `sys.heartbeat.{instance}` payload (LV, 1Hz); also `ctl.status` reply data. */
export interface Heartbeat {
  instance: string
  service: string
  state: ServiceState
  pid: number
  epoch: string
  app_version: string
  proto?: number
  versions?: Record<string, number[]>
  config_version?: number
  config_hash?: string
  clock_version?: number
  loop_lag_ms?: number
  stream?: string | null
  fact_heads?: Record<string, number>
  started_ns: number
  trading_day?: string | null
  trading_mode?: TradingMode | null
  queues?: Record<string, number>
  rates?: Record<string, number>
  latency_us?: Record<string, LatencySummary>
  deps?: Record<string, string>
  detail?: Record<string, unknown>
}

/** `sys.clock` payload (LV); also `svc.clock` reply data. */
export interface ClockState {
  mode: ClockMode
  offset_ns: number
  version: number
  market_now_ns: number
  actor?: string | null
  reason?: string | null
}

export interface ClockJumpRequest {
  command_id: string
  target: ClockTarget
  at_ns?: number | null
  actor?: string
}

/** `sys.service` payload (FACT). */
export interface ServiceStateChanged {
  instance: string
  service: string
  pid?: number | null
  from_state: ProcessState
  to_state: ProcessState
  exit_code?: number | null
  restarts?: number
  reason: ServiceChangeReason
  actor?: string | null
  trading_day?: string | null
  ts_ns: number
  /** supervisor extensions: names (never values) of the instance's environment overrides */
  env_keys?: string[] | null
  /** human-readable cause, e.g. "port 7502 in use by pid 4242 (python.exe)" */
  detail?: string | null
}

/** `svc.*` reply item. */
export interface ServiceInfo {
  instance: string
  service: string
  state: ProcessState
  pid?: number | null
  started_ns?: number | null
  restarts?: number
  last_exit_code?: number | null
  heartbeat_state?: ServiceState | null
  last_heartbeat_ns?: number | null
  /** supervisor extensions: environment override names in effect, last failure cause */
  env_keys?: string[] | null
  last_error?: string | null
}
