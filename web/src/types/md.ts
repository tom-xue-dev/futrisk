/**
 * Market-data payloads mirrored from `src/futrisk/messages/md.py` (02 §7.1).
 *
 * Field names are identical to the msgspec structs. Structs are declared with
 * `omit_defaults=True`, so a field that has a Python default may be absent from the
 * JSON: such fields are optional (`?`) here. `X | None` becomes `X | null`.
 * Integers that carry nanoseconds (`*_ns`) exceed 2^53 and lose sub-microsecond
 * precision in JavaScript, which is irrelevant for display.
 */
import type {
  Exchange,
  GapStatus,
  LimitLock,
  MdEventKind,
  MdQuality,
  SettleInputQuality,
  SettleMethod,
  SourceId,
  SourceState,
} from './enums'

/** `md.tick.{instrument_id}` payload: normalized L1 snapshot (LIVE only). */
export interface Tick {
  instrument_id: string
  exchange: Exchange
  trading_day: string
  action_day: string
  exch_ts_ns: number
  last: number
  volume: number
  turnover: number
  open_interest: number
  bid1?: number | null
  ask1?: number | null
  bid_vol1?: number
  ask_vol1?: number
  open?: number | null
  high?: number | null
  low?: number | null
  upper_limit: number
  lower_limit: number
  pre_settle: number
  pre_close?: number | null
  pre_open_interest?: number | null
  avg_price?: number | null
  source: SourceId
  source_seq?: number | null
  recv_ns: number
  fingerprint?: number
  flags?: number
}

export interface SourceStatus {
  source: SourceId
  state: SourceState
  since_ns: number
  connected: boolean
  epoch?: string | null
  last_seq?: number | null
  last_msg_ns?: number | null
  lag_ms?: number | null
  reject_rate?: number
  msgs_per_s?: number
  integrity_votes?: number
}

export interface InstrumentStatus {
  instrument_id: string
  quality: MdQuality
  live_source?: SourceId | null
  wm_volume?: number
  wm_exch_ts_ns?: number
  last_live_ns?: number | null
  quiet?: boolean
  limit_lock?: LimitLock
}

/** `md.status` payload (LV, 1Hz): source trust states and instrument quality. */
export interface MdStatus {
  as_of_ns: number
  trading_day: string
  sources: SourceStatus[]
  instruments: InstrumentStatus[]
}

export interface GapInfo {
  gap_id: string
  source?: SourceId | null
  epoch?: string | null
  from_seq?: number | null
  to_seq?: number | null
  from_ns: number
  to_ns?: number | null
  status?: GapStatus
  filled?: number
  duplicates?: number
  unrecoverable?: number
}

/** `md.event` payload (FACT). */
export interface MdEvent {
  event_id: string
  kind: MdEventKind
  ts_ns: number
  trading_day: string
  source?: SourceId | null
  instrument_id?: string | null
  from_state?: string | null
  to_state?: string | null
  gap?: GapInfo | null
  detail?: Record<string, unknown>
}

/** `md.settle_inputs` reply item (DecStr money/price fields are strings). */
export interface SettleInput {
  trading_day: string
  instrument_id: string
  exchange: Exchange
  method: SettleMethod
  multiplier: number
  tick_size: string
  day_volume: number
  day_turnover: string
  window_start_ns?: number | null
  window_volume?: number | null
  window_turnover?: string | null
  /** CFFEX hourly-backtrack inputs: hour -> [volume, turnover] as DecStr */
  hourly?: Record<string, string[]> | null
  vwap_raw?: string | null
  last?: string | null
  bid1?: string | null
  ask1?: string | null
  limit_lock?: LimitLock
  pre_settle: string
  quality?: SettleInputQuality
  version?: number
  computed_ns: number
}
