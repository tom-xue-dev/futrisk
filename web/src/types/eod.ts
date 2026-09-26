/**
 * EOD, clearing, report and backtest payloads mirrored from
 * `src/futrisk/messages/eod.py` (02 §7.7-§7.8). Money and prices are DecStr (string).
 */
import type {
  BacktestState,
  CheckStatus,
  DayRollMode,
  Direction,
  EodPhase,
  Exchange,
  FeeType,
  PosDirection,
  ReconCheck,
  ReportType,
  Rounding,
  SettleInputQuality,
  SettleMethod,
  SettlePriceSource,
} from './enums'

/** `sys.eod` payload (FACT); also the `settle.status` reply data. */
export interface EodPhaseEvent {
  trading_day: string
  version: number
  phase: EodPhase
  next_trading_day?: string | null
  detail?: Record<string, unknown>
  error?: string | null
  ts_ns: number
}

/** `sys.dayroll` payload (FACT). */
export interface DayRoll {
  shard: number
  mode: DayRollMode
  from_trading_day: string
  to_trading_day: string
  sod_version: number
  accounts: string[]
  latches_cleared?: number
  ts_ns: number
}

export interface SettlementPriceView {
  trading_day: string
  instrument_id: string
  price: string
  source: SettlePriceSource
  method?: SettleMethod | null
  vwap_raw?: string | null
  rounding?: Rounding
  input_quality?: SettleInputQuality | null
  version?: number
  operator?: string | null
  reason?: string | null
}

export interface CheckResult {
  check: ReconCheck
  status: CheckStatus
  account_id?: string | null
  detail?: Record<string, unknown> | null
}

/** Row of `account_settlements` (03 §4.4); every field present, DecStr money. */
export interface AccountSettlement {
  trading_day: string
  account_id: string
  version: number
  pre_balance: string
  deposit: string
  withdraw: string
  close_pnl: string
  position_pnl: string
  fee: string
  balance: string
  equity: string
  margin: string
  available: string
  risk_degree: string
  margin_call: string
  exch_formula_pnl: string
  fifo_realized_pnl: string
  fifo_float_pnl: string
  initial_capital: string
  cum_net_deposit: string
  hwm_equity: string
  nav: string
  max_drawdown: string
  checks_passed: boolean
}

export interface PositionSettlement {
  trading_day: string
  account_id: string
  instrument_id: string
  direction: PosDirection
  version: number
  product: string
  exchange: Exchange
  sod_qty: number
  open_qty: number
  close_qty: number
  close_today_qty: number
  close_yd_qty: number
  eod_qty: number
  settle_price: string
  pre_settle_price: string
  multiplier: number
  margin_rate: string
  margin_gross: string
  close_pnl: string
  position_pnl: string
  fee: string
  avg_open_price?: string | null
}

export interface MarginGroupView {
  trading_day: string
  account_id: string
  margin_group: string
  version: number
  method: string
  long_margin: string
  short_margin: string
  excluded_margin: string
  charged_margin: string
  detail?: Record<string, unknown>
}

export interface TradeSettlementView {
  trading_day: string
  exchange: Exchange
  trade_id: string
  direction: Direction
  version: number
  account_id: string
  instrument_id: string
  fee: string
  fee_type: FeeType
  fee_rate?: string | null
  fee_per_lot?: string | null
  close_pnl?: string
  matched_today_qty?: number
  matched_yd_qty?: number
}

export interface CloseDetailView {
  trading_day: string
  version: number
  account_id: string
  detail_no: number
  instrument_id: string
  exchange: Exchange
  trade_id: string
  direction: Direction
  close_type: string
  lot_open_td: string
  lot_open_ts_ns: number
  lot_open_price: string
  basis_price: string
  close_price: string
  qty: number
  close_pnl_mtm: string
  close_pnl_fifo: string
}

/** `settle.query` reply data; `GET /settlement/{td}`. */
export interface SettlementResult {
  trading_day: string
  version: number
  accounts: AccountSettlement[]
  positions?: PositionSettlement[]
  margin_groups?: MarginGroupView[]
  trades?: TradeSettlementView[]
  close_details?: CloseDetailView[]
  prices?: SettlementPriceView[]
  checks?: CheckResult[]
}

export interface AccountSettlementSummary {
  account_id: string
  close_pnl: string
  position_pnl: string
  fee: string
  balance: string
  equity: string
  margin: string
  available: string
  risk_degree: string
  margin_call: string
}

/** `clearing.settled` payload (FACT). */
export interface SettlementCompleted {
  trading_day: string
  version: number
  next_trading_day: string
  accounts: AccountSettlementSummary[]
  price_sources?: Partial<Record<SettlePriceSource, number>>
  checks?: CheckResult[]
  ts_ns: number
}

/** `report.ready` payload (FACT); `report.get` / `GET /reports` items. */
export interface ReportReady {
  report_id: string
  trading_day?: string | null
  type: ReportType
  account_id?: string | null
  path: string
  sha256: string
  size_bytes: number
  version?: number | null
  created_ns: number
}

/** `backtest.status` payload (LV); `GET /backtests` items. */
export interface BacktestStatus {
  run_id: string
  name: string
  state: BacktestState
  progress?: number
  submitted_ns: number
  started_ns?: number | null
  finished_ns?: number | null
  report_id?: string | null
  summary?: Record<string, unknown> | null
  error?: string | null
}
