/**
 * Enumerations mirrored from `src/futrisk/messages/enums.py` (02 §7.3).
 *
 * Every enum is a string-literal union with the exact wire values (UPPER_SNAKE).
 * The `*_VALUES` arrays keep a runtime list for selects and validation.
 */

export const EXCHANGE_VALUES = ['SHFE', 'DCE', 'CFFEX', 'INE', 'CZCE', 'GFEX'] as const
export type Exchange = (typeof EXCHANGE_VALUES)[number]

export const SOURCE_ID_VALUES = ['FEED_A', 'FEED_B', 'SINA'] as const
export type SourceId = (typeof SOURCE_ID_VALUES)[number]

export const SOURCE_STATE_VALUES = ['TRUSTED', 'PROBATION', 'EXCLUDED'] as const
export type SourceState = (typeof SOURCE_STATE_VALUES)[number]

export const MD_QUALITY_VALUES = ['OK', 'STALE', 'UNKNOWN'] as const
export type MdQuality = (typeof MD_QUALITY_VALUES)[number]

export const MD_EVENT_KIND_VALUES = [
  'SOURCE_STATE',
  'QUALITY_CHANGE',
  'GAP_OPEN',
  'GAP_FILLED',
  'GAP_UNRECOVERABLE',
  'MD_DOWN',
  'MD_RECOVERED',
  'MD_QUIET',
  'MD_INTEGRITY',
  'MD_FLAPPING',
  'BAD_TICK',
  'QUARANTINE_CONFIRMED',
  'QUARANTINE_DROPPED',
] as const
export type MdEventKind = (typeof MD_EVENT_KIND_VALUES)[number]

export type GapStatus = 'OPEN' | 'FILLED' | 'UNRECOVERABLE'
export type BarQuality = 'OK' | 'GAP_FILLED' | 'GAP_UNFILLED'
export type LimitLock = 'NONE' | 'UP' | 'DOWN'
export type SettleMethod = 'VWAP_FULL_DAY' | 'VWAP_LAST_HOUR'
export type SettleInputQuality = 'OK' | 'APPROX' | 'NO_TRADE'
export type SettlePriceSource =
  | 'MANUAL'
  | 'OFFICIAL'
  | 'VWAP'
  | 'FALLBACK_HOURLY'
  | 'FALLBACK_FULLDAY'
  | 'FALLBACK_MEDIAN'
  | 'FALLBACK_LIMIT'
  | 'FALLBACK_BENCHMARK'
  | 'PRE_SETTLE'
export type Rounding = 'FLOOR_TICK' | 'ROUND_TICK' | 'NONE'

export type Direction = 'BUY' | 'SELL'
export type PosDirection = 'LONG' | 'SHORT'
export type Offset = 'OPEN' | 'CLOSE' | 'CLOSE_TODAY' | 'CLOSE_YESTERDAY' | 'FORCE_CLOSE'
export type OrderType = 'LIMIT' | 'MARKET'
export type TimeInForce = 'GFD' | 'FAK' | 'FOK'
export type OrderSource = 'BOT' | 'MANUAL' | 'API' | 'LIQUIDATION'

export const ORDER_STATUS_VALUES = [
  'ROUTING',
  'ROUTED',
  'ACCEPTED',
  'PARTIALLY_FILLED',
  'FILLED',
  'CANCEL_PENDING',
  'CANCELED',
  'REJECTED',
  'BROKER_REJECTED',
  'UNKNOWN',
] as const
export type OrderStatus = (typeof ORDER_STATUS_VALUES)[number]

export type BrokerOrderStatus =
  | 'RECEIVED'
  | 'SUBMITTED'
  | 'ACCEPTED'
  | 'PARTIALLY_FILLED'
  | 'FILLED'
  | 'CANCEL_PENDING'
  | 'CANCELED'
  | 'REJECTED'
  | 'UNKNOWN'
  | 'EXPIRED'
export type BrokerConnState = 'DISCONNECTED' | 'CONNECTED' | 'LOGGED_IN' | 'RESYNCING'

export const ACCOUNT_STATE_VALUES = ['NORMAL', 'WARNING', 'RESTRICTED', 'HALTED'] as const
export type AccountState = (typeof ACCOUNT_STATE_VALUES)[number]

export type RuleSmState = 'OK' | 'PENDING' | 'ACTIVE' | 'REARMED'
export type Action = 'ALERT' | 'RESTRICT' | 'CIRCUIT_BREAK' | 'LIQUIDATE'

export const SEVERITY_VALUES = ['INFO', 'WARNING', 'HIGH', 'CRITICAL'] as const
export type Severity = (typeof SEVERITY_VALUES)[number]

export type RestrictionKind = 'ORDER_SIZE_FACTOR' | 'NO_ADD_TO_LOSERS' | 'EXPOSURE_CAP_FACTOR' | 'BLOCK_OPEN'

export const RISK_EVENT_KIND_VALUES = [
  'ALERT_RAISED',
  'ALERT_CLEARED',
  'ALERT_ACKED',
  'RESET',
  'MANUAL_HALT',
  'KILL_SWITCH_ON',
  'KILL_SWITCH_OFF',
  'RETRO_BREACH',
  'LIQUIDATION_PLAN',
  'RECON_BREAK',
  'FILL_GAP',
  'EXTERNAL_FILL',
  'EOD_FORCE_UNFREEZE',
] as const
export type RiskEventKind = (typeof RISK_EVENT_KIND_VALUES)[number]

export type CashType = 'DEPOSIT' | 'WITHDRAW'
/** RULE_STATE: the coordinator's complete rule layer in one fact (active = rule kill switch, restrictions = all in force) */
export type FirmAction = 'KILL_SWITCH_ON' | 'KILL_SWITCH_OFF' | 'SET_RESTRICTIONS' | 'RULE_STATE'
export type LiquidationStatus = 'PENDING_CONFIRM' | 'CONFIRMED' | 'CANCELED' | 'EXPIRED'
export type TradingMode = 'OPEN' | 'EOD_FROZEN' | 'PROVISIONAL'
export type TradeOrigin = 'INTERNAL' | 'EXTERNAL'
export type ReconResolution = 'RESYNCED' | 'RESEEDED' | 'ACCEPTED'
export type ReconBreakKind = 'POSITION_MISMATCH' | 'REPLAY_EXPIRED' | 'ORDER_STATE_CONFLICT'
export type ReconBreakStatus = 'OPEN' | 'RESOLVED'
export type TickMode = 'LIVE' | 'BACKFILL' | 'QUARANTINED' | 'REJECTED' | 'DUPLICATE'

export const EOD_PHASE_VALUES = [
  'BEGIN',
  'FROZEN',
  'TRADES_RECONCILED',
  'PRICES_FIXED',
  'SETTLED',
  'ROLLED',
  'REPORTED',
  'DONE',
  'FAILED',
] as const
export type EodPhase = (typeof EOD_PHASE_VALUES)[number]

export type FeeType = 'OPEN' | 'CLOSE_YD' | 'CLOSE_TODAY' | 'INTRADAY_OPEN' | 'INTRADAY_CLOSE'
export type ReconCheck =
  | 'TRADE_DIGEST'
  | 'CASH_DIGEST'
  | 'EQUITY_IDENTITY'
  | 'POSITION_IDENTITY'
  | 'EXCH_FORMULA'
  | 'INTRADAY_VS_SETTLE'
  | 'FIRM_SUM'
  | 'MTM_VS_FIFO'
export type CheckStatus = 'PASS' | 'WARN' | 'FAIL'
export type ReportType = 'RISK_DAILY_HTML' | 'STATEMENT_TXT' | 'STATEMENT_CSV' | 'SETTLEMENT_JSON' | 'BACKTEST_HTML'
export type BacktestState = 'QUEUED' | 'RUNNING' | 'DONE' | 'FAILED'
export type ServiceState = 'STARTING' | 'RUNNING' | 'DEGRADED' | 'STOPPING'
export type ClockMode = 'REAL' | 'DEMO'

export const CLOCK_TARGET_VALUES = ['PRE_CLOSE', 'NEXT_OPEN', 'AT'] as const
export type ClockTarget = (typeof CLOCK_TARGET_VALUES)[number]

export type ProcessState = 'STARTING' | 'RUNNING' | 'EXITED' | 'RESTARTING' | 'STOPPING' | 'STOPPED' | 'FAILED'
export type Role = 'VIEWER' | 'TRADER' | 'RISK_MANAGER' | 'ADMIN'
export type ConfigStatus = 'PREPARING' | 'COMMITTING' | 'COMMITTED' | 'ABORTED' | 'FAILED'
export type ConfigEffect = 'IMMEDIATE' | 'NEXT_TRADING_DAY'
export type DayRollMode = 'ROLL' | 'REBASE' | 'PROVISIONAL' | 'CATCH_UP'
export type ControlOrigin = 'MANUAL' | 'RULE'
export type ServiceChangeReason =
  | 'STARTUP'
  | 'SHUTDOWN'
  | 'EXITED'
  | 'HEARTBEAT_LOST'
  | 'LOOP_HANG'
  | 'KILLED_BY_DEMO'
  | 'MANUAL'
  /** supervisor extension: parked after repeated failures until svc.start / restart */
  | 'GAVE_UP'
  /** supervisor extension: episode closed after a supervisor crash */
  | 'SUPERVISOR_RESTART'

/** Tick.flags bits (02 §7.1). */
export const TICK_FLAG_COARSE_TS = 1 << 0
export const TICK_FLAG_AUCTION = 1 << 1
export const TICK_FLAG_LIMIT_UP_LOCKED = 1 << 2
export const TICK_FLAG_LIMIT_DOWN_LOCKED = 1 << 3

/** Severity ordering of AccountState (01 §8.14 aggregation). */
export const ACCOUNT_STATE_SEVERITY: Record<AccountState, number> = {
  NORMAL: 0,
  WARNING: 1,
  RESTRICTED: 2,
  HALTED: 3,
}

export const SEVERITY_ORDER: Record<Severity, number> = {
  INFO: 0,
  WARNING: 1,
  HIGH: 2,
  CRITICAL: 3,
}
