"""EOD, clearing, report and backtest payloads (02 §6.5, §7.7-§7.8).

Money and prices that enter clearing are ``DecStr`` (``str``) for exactness.
"""

from __future__ import annotations

from typing import Any

import msgspec

from futrisk.messages.base import Msg, payload
from futrisk.messages.enums import (
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
    Role,
    Rounding,
    SettleInputQuality,
    SettleMethod,
    SettlePriceSource,
)
from futrisk.messages.orders import AccountDigest
from futrisk.messages.risk import CashDigest


@payload()
class EodPhaseEvent(Msg, kw_only=True):
    trading_day: str
    version: int
    phase: EodPhase
    next_trading_day: str | None = None
    detail: dict[str, Any] = {}
    error: str | None = None
    ts_ns: int


@payload()
class EodFreezeRequest(Msg, kw_only=True):
    trading_day: str
    version: int


@payload()
class EodFreezeAck(Msg, kw_only=True):
    trading_day: str
    shard: int
    accounts: list[AccountDigest]
    cash: list[CashDigest]
    open_orders: int = 0
    frozen_ns: int


@payload()
class EodRolloverRequest(Msg, kw_only=True):
    trading_day: str
    next_trading_day: str
    sod_version: int


@payload()
class DayRoll(Msg, kw_only=True):
    shard: int
    mode: DayRollMode
    from_trading_day: str
    to_trading_day: str
    sod_version: int
    accounts: list[str]
    latches_cleared: int = 0
    ts_ns: int


@payload()
class ForceUnfreezeRequest(Msg, kw_only=True):
    command_id: str
    trading_day: str
    reason: str
    actor: str
    role: Role


@payload()
class SettleRunRequest(Msg, kw_only=True):
    command_id: str
    trading_day: str
    force_new_version: bool = False
    actor: str = "system"


@payload()
class SettleRunAck(Msg, kw_only=True):
    trading_day: str
    version: int
    phase: EodPhase


class OfficialPrice(msgspec.Struct, kw_only=True):
    instrument_id: str
    price: str


@payload()
class OfficialPriceImport(Msg, kw_only=True):
    command_id: str
    trading_day: str
    prices: list[OfficialPrice]
    source: str
    actor: str
    role: Role


@payload()
class ImportAck(Msg, kw_only=True):
    inserted: int
    changed: int


class SodLot(msgspec.Struct, kw_only=True, omit_defaults=True):
    instrument_id: str
    direction: PosDirection
    lot_no: int
    open_td: str
    open_ts_ns: int
    open_price: str
    qty: int
    basis_price: str


class SodAccount(msgspec.Struct, kw_only=True, omit_defaults=True):
    account_id: str
    pre_balance: str
    initial_capital: str
    cum_net_deposit: str = "0"
    hwm_equity: str
    lots: list[SodLot] = []


class SettlementRef(msgspec.Struct, kw_only=True):
    trading_day: str
    version: int


@payload()
class SodSnapshot(Msg, kw_only=True):
    trading_day: str
    version: int
    from_settlement: SettlementRef | None = None
    accounts: list[SodAccount]
    pre_settle: dict[str, str] = {}


@payload()
class SeedSodRequest(Msg, kw_only=True):
    command_id: str
    trading_day: str
    accounts: list[SodAccount]
    pre_settle: dict[str, str] = {}
    reason: str
    actor: str
    role: Role


@payload()
class SodRequest(Msg, kw_only=True):
    trading_day: str
    account_ids: list[str] = []
    version: int | None = None


@payload()
class PriceOverride(Msg, kw_only=True):
    command_id: str
    trading_day: str
    instrument_id: str
    price: str
    reason: str
    actor: str
    role: Role


@payload()
class SettlementPriceView(Msg, kw_only=True):
    trading_day: str
    instrument_id: str
    price: str
    source: SettlePriceSource
    method: SettleMethod | None = None
    vwap_raw: str | None = None
    rounding: Rounding = Rounding.NONE
    input_quality: SettleInputQuality | None = None
    version: int = 1
    operator: str | None = None
    reason: str | None = None


@payload()
class SettleQuery(Msg, kw_only=True):
    trading_day: str
    account_id: str | None = None
    version: int | None = None


class CheckResult(msgspec.Struct, kw_only=True, omit_defaults=True):
    check: ReconCheck
    status: CheckStatus
    account_id: str | None = None
    detail: dict[str, Any] | None = None


class AccountSettlement(msgspec.Struct, kw_only=True):
    trading_day: str
    account_id: str
    version: int
    pre_balance: str
    deposit: str
    withdraw: str
    close_pnl: str
    position_pnl: str
    fee: str
    balance: str
    equity: str
    margin: str
    available: str
    risk_degree: str
    margin_call: str
    exch_formula_pnl: str
    fifo_realized_pnl: str
    fifo_float_pnl: str
    initial_capital: str
    cum_net_deposit: str
    hwm_equity: str
    nav: str
    max_drawdown: str
    checks_passed: bool


class PositionSettlement(msgspec.Struct, kw_only=True, omit_defaults=True):
    trading_day: str
    account_id: str
    instrument_id: str
    direction: PosDirection
    version: int
    product: str
    exchange: Exchange
    sod_qty: int
    open_qty: int
    close_qty: int
    close_today_qty: int
    close_yd_qty: int
    eod_qty: int
    settle_price: str
    pre_settle_price: str
    multiplier: int
    margin_rate: str
    margin_gross: str
    close_pnl: str
    position_pnl: str
    fee: str
    avg_open_price: str | None = None


class MarginGroupView(msgspec.Struct, kw_only=True):
    trading_day: str
    account_id: str
    margin_group: str
    version: int
    method: str  # "GROSS" | "BIG_SIDE"
    long_margin: str
    short_margin: str
    excluded_margin: str
    charged_margin: str
    detail: dict[str, Any] = {}


class TradeSettlementView(msgspec.Struct, kw_only=True, omit_defaults=True):
    trading_day: str
    exchange: Exchange
    trade_id: str
    direction: Direction
    version: int
    account_id: str
    instrument_id: str
    fee: str
    fee_type: FeeType
    fee_rate: str | None = None
    fee_per_lot: str | None = None
    close_pnl: str = "0"
    matched_today_qty: int = 0
    matched_yd_qty: int = 0


class CloseDetailView(msgspec.Struct, kw_only=True):
    trading_day: str
    version: int
    account_id: str
    detail_no: int
    instrument_id: str
    exchange: Exchange
    trade_id: str
    direction: Direction
    close_type: str  # "TODAY" | "YD"
    lot_open_td: str
    lot_open_ts_ns: int
    lot_open_price: str
    basis_price: str
    close_price: str
    qty: int
    close_pnl_mtm: str
    close_pnl_fifo: str


@payload()
class SettlementResult(Msg, kw_only=True):
    trading_day: str
    version: int
    accounts: list[AccountSettlement]
    positions: list[PositionSettlement] = []
    margin_groups: list[MarginGroupView] = []
    trades: list[TradeSettlementView] = []
    close_details: list[CloseDetailView] = []
    prices: list[SettlementPriceView] = []
    checks: list[CheckResult] = []


class AccountSettlementSummary(msgspec.Struct, kw_only=True):
    account_id: str
    close_pnl: str
    position_pnl: str
    fee: str
    balance: str
    equity: str
    margin: str
    available: str
    risk_degree: str
    margin_call: str


@payload()
class SettlementCompleted(Msg, kw_only=True):
    trading_day: str
    version: int
    next_trading_day: str
    accounts: list[AccountSettlementSummary]
    price_sources: dict[str, int] = {}
    checks: list[CheckResult] = []
    ts_ns: int


@payload()
class EodStatusRequest(Msg, kw_only=True):
    trading_day: str


# ---------------------------------------------------------------- reports
@payload()
class ReportReady(Msg, kw_only=True):
    report_id: str
    trading_day: str | None = None
    type: ReportType
    account_id: str | None = None
    path: str
    sha256: str
    size_bytes: int
    version: int | None = None
    created_ns: int


@payload()
class ReportListRequest(Msg, kw_only=True):
    trading_day: str | None = None


@payload()
class ReportList(Msg, kw_only=True):
    items: list[ReportReady]


@payload()
class ReportGetRequest(Msg, kw_only=True):
    report_id: str


# ---------------------------------------------------------------- backtests
@payload()
class BacktestRunRequest(Msg, kw_only=True):
    command_id: str
    config_name: str
    overrides: dict[str, Any] | None = None
    actor: str = "system"


@payload()
class BacktestStatus(Msg, kw_only=True):
    run_id: str
    name: str
    state: BacktestState
    progress: float = 0.0
    submitted_ns: int
    started_ns: int | None = None
    finished_ns: int | None = None
    report_id: str | None = None
    summary: dict[str, Any] | None = None
    error: str | None = None


@payload()
class BacktestStatusRequest(Msg, kw_only=True):
    run_id: str


@payload()
class BacktestListRequest(Msg, kw_only=True):
    pass


@payload()
class BacktestList(Msg, kw_only=True):
    items: list[BacktestStatus]
