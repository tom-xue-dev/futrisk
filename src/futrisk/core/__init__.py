"""Pure domain core: instruments, fees, lots, P&L, margin, settlement prices and clearing (01 §5.4, §8.15).

No I/O, no system clock, deterministic, ``Decimal``-exact for clearing. The
clearing-worker and the backtester call :func:`settle_day` directly; the risk
engine builds on :class:`PositionBook`, :class:`DayFeeBook`, the margin
functions and :class:`IntradayLedger`.

Module map:

- :mod:`.instruments` - :class:`InstrumentRegistry` (schedules, limits, big-side groups);
- :mod:`.lots` - :class:`PositionBook` / :class:`Position` / :class:`Lot`;
- :mod:`.fees` - close-today classification and per-trade fees;
- :mod:`.pnl` - mark-to-market, FIFO and exchange-formula P&L;
- :mod:`.margin` - gross and big-side margin, frozen margin;
- :mod:`.settle_price` - settlement price priority and fallback chains;
- :mod:`.settlement` - :func:`settle_day` and next-day SOD;
- :mod:`.ledger` - :class:`IntradayLedger` (AccountFunds for the risk engine).
"""

from futrisk.core.fees import (
    DayFeeBook,
    DayFeeClassifier,
    FeeLegs,
    FillFee,
    FloatFeeRates,
    TradeFee,
    compute_fee,
)
from futrisk.core.instruments import (
    InstrumentInfo,
    InstrumentRegistry,
    MarginMethod,
    ScheduleLookupError,
    UnknownInstrumentError,
)
from futrisk.core.ledger import IntradayLedger, LedgerFill, RestingOrder
from futrisk.core.lots import (
    Closable,
    CloseSlice,
    CloseType,
    FillResult,
    Lot,
    Position,
    PositionBook,
    PositionError,
    normalize_offset,
    pos_direction,
)
from futrisk.core.margin import (
    AccountMargin,
    MarginEntry,
    MarginGroup,
    frozen_margin,
    group_charge,
    intraday_margin,
    settlement_margin,
)
from futrisk.core.pnl import exchange_formula_pnl
from futrisk.core.settle_price import (
    BenchmarkMove,
    HourlyBucket,
    ManualPrice,
    build_settle_input,
    determine_settlement_price,
    trading_hour_buckets,
    trading_hour_windows,
)
from futrisk.core.settlement import (
    AccountResult,
    DaySettlement,
    PositionResult,
    SettlementError,
    TradeResult,
    seed_sod,
    settle_day,
)

__all__ = [
    "AccountMargin",
    "AccountResult",
    "BenchmarkMove",
    "Closable",
    "CloseSlice",
    "CloseType",
    "DayFeeBook",
    "DayFeeClassifier",
    "DaySettlement",
    "FeeLegs",
    "FillFee",
    "FillResult",
    "FloatFeeRates",
    "HourlyBucket",
    "InstrumentInfo",
    "InstrumentRegistry",
    "IntradayLedger",
    "LedgerFill",
    "Lot",
    "ManualPrice",
    "MarginEntry",
    "MarginGroup",
    "MarginMethod",
    "Position",
    "PositionBook",
    "PositionError",
    "PositionResult",
    "RestingOrder",
    "ScheduleLookupError",
    "SettlementError",
    "TradeFee",
    "TradeResult",
    "UnknownInstrumentError",
    "build_settle_input",
    "compute_fee",
    "determine_settlement_price",
    "exchange_formula_pnl",
    "frozen_margin",
    "group_charge",
    "intraday_margin",
    "normalize_offset",
    "pos_direction",
    "seed_sod",
    "settle_day",
    "settlement_margin",
    "trading_hour_buckets",
    "trading_hour_windows",
]
