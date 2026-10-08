"""Enumerations shared by messages, storage and config (02 §7.3).

All values are UPPER_SNAKE strings on the wire. Receivers must not guess the
meaning of unknown values; msgspec raises a ValidationError, which callers log
and count as "cannot handle".
"""

from __future__ import annotations

from enum import StrEnum


class Exchange(StrEnum):
    SHFE = "SHFE"
    DCE = "DCE"
    CFFEX = "CFFEX"
    INE = "INE"
    CZCE = "CZCE"
    GFEX = "GFEX"


class SourceId(StrEnum):
    FEED_A = "FEED_A"
    FEED_B = "FEED_B"
    SINA = "SINA"


class SourceState(StrEnum):
    TRUSTED = "TRUSTED"
    PROBATION = "PROBATION"
    EXCLUDED = "EXCLUDED"


class MdQuality(StrEnum):
    OK = "OK"
    STALE = "STALE"
    UNKNOWN = "UNKNOWN"


class MdEventKind(StrEnum):
    SOURCE_STATE = "SOURCE_STATE"
    QUALITY_CHANGE = "QUALITY_CHANGE"
    GAP_OPEN = "GAP_OPEN"
    GAP_FILLED = "GAP_FILLED"
    GAP_UNRECOVERABLE = "GAP_UNRECOVERABLE"
    MD_DOWN = "MD_DOWN"
    MD_RECOVERED = "MD_RECOVERED"
    MD_QUIET = "MD_QUIET"
    MD_INTEGRITY = "MD_INTEGRITY"
    BAD_TICK = "BAD_TICK"


class GapStatus(StrEnum):
    OPEN = "OPEN"
    FILLED = "FILLED"
    UNRECOVERABLE = "UNRECOVERABLE"


class LimitLock(StrEnum):
    NONE = "NONE"
    UP = "UP"
    DOWN = "DOWN"


class SettleMethod(StrEnum):
    VWAP_FULL_DAY = "VWAP_FULL_DAY"
    VWAP_LAST_HOUR = "VWAP_LAST_HOUR"


class SettleInputQuality(StrEnum):
    OK = "OK"
    APPROX = "APPROX"
    NO_TRADE = "NO_TRADE"


class SettlePriceSource(StrEnum):
    MANUAL = "MANUAL"
    OFFICIAL = "OFFICIAL"
    VWAP = "VWAP"
    FALLBACK_HOURLY = "FALLBACK_HOURLY"
    FALLBACK_FULLDAY = "FALLBACK_FULLDAY"
    FALLBACK_MEDIAN = "FALLBACK_MEDIAN"
    FALLBACK_LIMIT = "FALLBACK_LIMIT"
    FALLBACK_BENCHMARK = "FALLBACK_BENCHMARK"
    PRE_SETTLE = "PRE_SETTLE"


class Rounding(StrEnum):
    FLOOR_TICK = "FLOOR_TICK"
    ROUND_TICK = "ROUND_TICK"
    NONE = "NONE"


class Direction(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class PosDirection(StrEnum):
    LONG = "LONG"
    SHORT = "SHORT"


class Offset(StrEnum):
    OPEN = "OPEN"
    CLOSE = "CLOSE"
    CLOSE_TODAY = "CLOSE_TODAY"
    CLOSE_YESTERDAY = "CLOSE_YESTERDAY"
    FORCE_CLOSE = "FORCE_CLOSE"

    @property
    def is_close(self) -> bool:
        return self is not Offset.OPEN


class OrderType(StrEnum):
    LIMIT = "LIMIT"
    MARKET = "MARKET"


class TimeInForce(StrEnum):
    GFD = "GFD"
    FAK = "FAK"
    FOK = "FOK"


class OrderSource(StrEnum):
    BOT = "BOT"
    MANUAL = "MANUAL"
    API = "API"
    LIQUIDATION = "LIQUIDATION"


class OrderStatus(StrEnum):
    ROUTING = "ROUTING"
    ROUTED = "ROUTED"
    ACCEPTED = "ACCEPTED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCEL_PENDING = "CANCEL_PENDING"
    CANCELED = "CANCELED"
    REJECTED = "REJECTED"
    BROKER_REJECTED = "BROKER_REJECTED"
    UNKNOWN = "UNKNOWN"

    @property
    def is_terminal(self) -> bool:
        return self in (OrderStatus.REJECTED, OrderStatus.FILLED, OrderStatus.CANCELED, OrderStatus.BROKER_REJECTED)


class BrokerOrderStatus(StrEnum):
    RECEIVED = "RECEIVED"
    SUBMITTED = "SUBMITTED"
    ACCEPTED = "ACCEPTED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCEL_PENDING = "CANCEL_PENDING"
    CANCELED = "CANCELED"
    REJECTED = "REJECTED"
    UNKNOWN = "UNKNOWN"
    EXPIRED = "EXPIRED"

    @property
    def is_terminal(self) -> bool:
        return self in (
            BrokerOrderStatus.FILLED,
            BrokerOrderStatus.CANCELED,
            BrokerOrderStatus.REJECTED,
            BrokerOrderStatus.EXPIRED,
        )


class BrokerConnState(StrEnum):
    DISCONNECTED = "DISCONNECTED"
    CONNECTED = "CONNECTED"
    LOGGED_IN = "LOGGED_IN"
    RESYNCING = "RESYNCING"


class AccountState(StrEnum):
    NORMAL = "NORMAL"
    WARNING = "WARNING"
    RESTRICTED = "RESTRICTED"
    HALTED = "HALTED"

    @property
    def severity(self) -> int:
        return _ACCOUNT_STATE_ORDER[self]


_ACCOUNT_STATE_ORDER = {
    AccountState.NORMAL: 0,
    AccountState.WARNING: 1,
    AccountState.RESTRICTED: 2,
    AccountState.HALTED: 3,
}


class RuleSmState(StrEnum):
    OK = "OK"
    PENDING = "PENDING"
    ACTIVE = "ACTIVE"
    REARMED = "REARMED"


class Action(StrEnum):
    ALERT = "ALERT"
    RESTRICT = "RESTRICT"
    CIRCUIT_BREAK = "CIRCUIT_BREAK"
    LIQUIDATE = "LIQUIDATE"


class Severity(StrEnum):
    INFO = "INFO"
    WARNING = "WARNING"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class RestrictionKind(StrEnum):
    ORDER_SIZE_FACTOR = "ORDER_SIZE_FACTOR"
    NO_ADD_TO_LOSERS = "NO_ADD_TO_LOSERS"
    EXPOSURE_CAP_FACTOR = "EXPOSURE_CAP_FACTOR"
    BLOCK_OPEN = "BLOCK_OPEN"


class RiskEventKind(StrEnum):
    ALERT_RAISED = "ALERT_RAISED"
    ALERT_CLEARED = "ALERT_CLEARED"
    ALERT_ACKED = "ALERT_ACKED"
    RESET = "RESET"
    MANUAL_HALT = "MANUAL_HALT"
    KILL_SWITCH_ON = "KILL_SWITCH_ON"
    KILL_SWITCH_OFF = "KILL_SWITCH_OFF"
    RETRO_BREACH = "RETRO_BREACH"
    LIQUIDATION_PLAN = "LIQUIDATION_PLAN"
    RECON_BREAK = "RECON_BREAK"
    FILL_GAP = "FILL_GAP"
    EXTERNAL_FILL = "EXTERNAL_FILL"
    EOD_FORCE_UNFREEZE = "EOD_FORCE_UNFREEZE"


class CashType(StrEnum):
    DEPOSIT = "DEPOSIT"
    WITHDRAW = "WITHDRAW"


class FirmAction(StrEnum):
    KILL_SWITCH_ON = "KILL_SWITCH_ON"  # origin MANUAL
    KILL_SWITCH_OFF = "KILL_SWITCH_OFF"  # origin MANUAL
    SET_RESTRICTIONS = "SET_RESTRICTIONS"  # legacy; the coordinator emits RULE_STATE instead
    # origin RULE: the complete rule-layer state in one fact (active = rule kill switch,
    # restrictions = every firm restriction in force), so the newest fact alone restores it
    # (ctl.snapshot fallback of risk.control.firm keeps only the newest message)
    RULE_STATE = "RULE_STATE"


class LiquidationStatus(StrEnum):
    PENDING_CONFIRM = "PENDING_CONFIRM"
    CONFIRMED = "CONFIRMED"
    CANCELED = "CANCELED"
    EXPIRED = "EXPIRED"


class TradingMode(StrEnum):
    OPEN = "OPEN"
    EOD_FROZEN = "EOD_FROZEN"
    PROVISIONAL = "PROVISIONAL"


class TradeOrigin(StrEnum):
    INTERNAL = "INTERNAL"
    EXTERNAL = "EXTERNAL"


class ReconResolution(StrEnum):
    RESYNCED = "RESYNCED"
    RESEEDED = "RESEEDED"
    ACCEPTED = "ACCEPTED"


class ReconBreakKind(StrEnum):
    POSITION_MISMATCH = "POSITION_MISMATCH"
    REPLAY_EXPIRED = "REPLAY_EXPIRED"
    ORDER_STATE_CONFLICT = "ORDER_STATE_CONFLICT"


class ReconBreakStatus(StrEnum):
    OPEN = "OPEN"
    RESOLVED = "RESOLVED"


class TickMode(StrEnum):
    LIVE = "LIVE"
    BACKFILL = "BACKFILL"
    REJECTED = "REJECTED"
    DUPLICATE = "DUPLICATE"


class EodPhase(StrEnum):
    BEGIN = "BEGIN"
    FROZEN = "FROZEN"
    TRADES_RECONCILED = "TRADES_RECONCILED"
    PRICES_FIXED = "PRICES_FIXED"
    SETTLED = "SETTLED"
    ROLLED = "ROLLED"
    REPORTED = "REPORTED"
    DONE = "DONE"
    FAILED = "FAILED"


class FeeType(StrEnum):
    OPEN = "OPEN"
    CLOSE_YD = "CLOSE_YD"
    CLOSE_TODAY = "CLOSE_TODAY"
    INTRADAY_OPEN = "INTRADAY_OPEN"
    INTRADAY_CLOSE = "INTRADAY_CLOSE"


class ReconCheck(StrEnum):
    TRADE_DIGEST = "TRADE_DIGEST"
    CASH_DIGEST = "CASH_DIGEST"
    EQUITY_IDENTITY = "EQUITY_IDENTITY"
    POSITION_IDENTITY = "POSITION_IDENTITY"
    EXCH_FORMULA = "EXCH_FORMULA"
    INTRADAY_VS_SETTLE = "INTRADAY_VS_SETTLE"
    FIRM_SUM = "FIRM_SUM"
    MTM_VS_FIFO = "MTM_VS_FIFO"


class CheckStatus(StrEnum):
    PASS = "PASS"
    WARN = "WARN"
    FAIL = "FAIL"


class ReportType(StrEnum):
    RISK_DAILY_HTML = "RISK_DAILY_HTML"
    STATEMENT_TXT = "STATEMENT_TXT"
    STATEMENT_CSV = "STATEMENT_CSV"
    SETTLEMENT_JSON = "SETTLEMENT_JSON"
    BACKTEST_HTML = "BACKTEST_HTML"


class BacktestState(StrEnum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    DONE = "DONE"
    FAILED = "FAILED"


class ServiceState(StrEnum):
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    DEGRADED = "DEGRADED"
    STOPPING = "STOPPING"


class ClockMode(StrEnum):
    REAL = "REAL"
    DEMO = "DEMO"


class ClockTarget(StrEnum):
    PRE_CLOSE = "PRE_CLOSE"
    NEXT_OPEN = "NEXT_OPEN"
    AT = "AT"


class ProcessState(StrEnum):
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    EXITED = "EXITED"
    RESTARTING = "RESTARTING"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"
    FAILED = "FAILED"


class Role(StrEnum):
    VIEWER = "VIEWER"
    TRADER = "TRADER"
    RISK_MANAGER = "RISK_MANAGER"
    ADMIN = "ADMIN"


class ConfigStatus(StrEnum):
    PREPARING = "PREPARING"
    COMMITTING = "COMMITTING"
    COMMITTED = "COMMITTED"
    ABORTED = "ABORTED"
    FAILED = "FAILED"


class ConfigEffect(StrEnum):
    IMMEDIATE = "IMMEDIATE"
    NEXT_TRADING_DAY = "NEXT_TRADING_DAY"


class DayRollMode(StrEnum):
    ROLL = "ROLL"
    REBASE = "REBASE"
    PROVISIONAL = "PROVISIONAL"
    # a shard already PROVISIONAL on a later day records the official SOD of a skipped day
    # (catch-up after missed EODs); its current day is rebased only when that day's SOD arrives
    CATCH_UP = "CATCH_UP"


class FactStartPolicy(StrEnum):
    FROM_BEGINNING = "FROM_BEGINNING"
    FROM_NOW = "FROM_NOW"


class ControlOrigin(StrEnum):
    MANUAL = "MANUAL"
    RULE = "RULE"


class ServiceChangeReason(StrEnum):
    STARTUP = "STARTUP"
    SHUTDOWN = "SHUTDOWN"
    EXITED = "EXITED"
    HEARTBEAT_LOST = "HEARTBEAT_LOST"
    LOOP_HANG = "LOOP_HANG"
    KILLED_BY_DEMO = "KILLED_BY_DEMO"
    MANUAL = "MANUAL"
    GAVE_UP = "GAVE_UP"  # supervisor extension: parked after repeated failures until svc.start/restart
    SUPERVISOR_RESTART = "SUPERVISOR_RESTART"  # supervisor extension: episode closed after a supervisor crash
