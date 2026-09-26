"""EngineCore: the single-writer brain of one risk-engine shard (01 §4 一致性域不拆, §5.3.1, ADR-004; 02 §8.1, §10).

The core is pure and deterministic: it never performs I/O, never reads the
system clock (time comes from the injected :class:`Clock`, ids from the
injected :class:`IdGen`) and answers every input with an ordered
``list[Effect]`` (:mod:`futrisk.core.effects`). The risk-engine shell and the
backtester drive the *same* object; ``ENGINE.md`` next to this file documents
the method list, the Effect execution contract and the table mapping.

Inputs (02 §8.1 main loop):

- market data: :meth:`apply_mark` (LIVE ticks, conflated per instrument),
  :meth:`apply_md_status`, :meth:`set_md_down`;
- execution: :meth:`apply_exec` (``Trade`` / ``BrokerOrderUpdate`` of
  ``td.exec.{account}``), :meth:`on_route_ack`, :meth:`on_cancel_result`,
  :meth:`on_resolve_result`, :meth:`on_route_timeout`;
- commands: :meth:`handle` (``order.*``, ``risk.*``, ``cash.move``,
  ``firm.kill_switch``, ``eod.*``, ``risk.query.*``);
- sub-request results: :meth:`on_request_result`;
- loop hooks: :meth:`evaluate` / :meth:`evaluate_dirty`, :meth:`on_timer`;
- firm: :meth:`apply_firm_control`, :meth:`ingest_snapshot` (coordinator);
- lifecycle and recovery: :meth:`set_lifecycle`, :meth:`set_system_restriction`,
  :meth:`set_gateway_status`, :meth:`restore`, :meth:`post_restore`,
  :meth:`load_sod`, :meth:`roll_day`, :meth:`freeze`.
"""

from __future__ import annotations

import json
import math
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, field, replace
from decimal import Decimal
from typing import Any

import msgspec

from futrisk.common.calendar import TradingCalendar
from futrisk.common.clock import Clock, bj_to_ns, parse_td, td_str
from futrisk.common.config.accounts import AccountsConfig
from futrisk.common.config.risk_rules import RiskRulesConfig, RuleDef, RuleLevel
from futrisk.common.decimals import D
from futrisk.common.ids import IdGen
from futrisk.core._num import money_str, price_str
from futrisk.core.actions import (
    ActionInput,
    LiquidationCandidate,
    action_outcome,
    is_effective,
    liquidation_steps,
)
from futrisk.core.alerts import (
    ALERT_ACTIVE,
    ALERT_CLEARED,
    ALERT_RESET,
    ALERT_ROLLED,
    AUDIT_ALERT_ACK,
    AUDIT_CASH_MOVE,
    AUDIT_CONFIG_COMMIT,
    AUDIT_DAYROLL,
    AUDIT_DAYROLL_REBASE,
    AUDIT_EOD_FORCE_UNFREEZE,
    AUDIT_KILL_SWITCH_OFF,
    AUDIT_KILL_SWITCH_ON,
    AUDIT_LIQUIDATION_CANCEL,
    AUDIT_LIQUIDATION_CONFIRM,
    AUDIT_PROVISIONAL_SOD,
    AUDIT_RECON_RESOLVE,
    AUDIT_RISK_HALT,
    AUDIT_RISK_RESET,
    AlertRecord,
    EventLog,
    audit,
    event_from_row,
)
from futrisk.core.effects import (
    CommandCtx,
    Effect,
    Persist,
    Publish,
    ReplyTo,
    Tx,
    order_request_hash,
    request_hash,
)
from futrisk.core.eod import (
    EOD_FORCE_UNFROZEN,
    EOD_FROZEN,
    EOD_OPEN,
    EOD_PROVISIONAL,
    EOD_ROLLED,
    PROVISIONAL_SOD_VERSION,
    EodStatus,
    account_digests,
    build_provisional_sod,
    cash_digests,
    cash_from_row,
    cash_trading_day,
    provisional_prices,
    trade_from_fill_row,
)
from futrisk.core.evaluation import (
    LevelEval,
    evaluate_owner,
    next_due_ns,
    persist_transitions,
    reevaluate_due,
    rule_statuses,
)
from futrisk.core.firm import FirmAggregator, FirmState, firm_control_row, msgspec_restriction
from futrisk.core.instruments import InstrumentRegistry
from futrisk.core.ledger import IntradayLedger, RestingOrder
from futrisk.core.lots import PositionError, pos_direction
from futrisk.core.metrics import AccountValuation, StaticCache, StaticInfo, build_valuation
from futrisk.core.orders import (
    CANCELLABLE,
    R_CANCEL_REFUSED,
    R_CANCEL_RETRY,
    R_CIRCUIT_BREAK_CANCEL,
    R_FILL_AFTER_TERMINAL,
    R_MANUAL_CANCEL,
    R_RESTART,
    R_ROUTE_TIMEOUT,
    R_SESSION_CLOSE,
    TERMINAL,
    WORKING_BROKER_STATES,
    OrderBook,
    OrderRecord,
    broker_status_target,
    emit_order,
    persist_order,
    set_status,
    status_after_fill,
)
from futrisk.core.pretrade import check_order
from futrisk.core.restrictions import (
    SYSTEM_EOD_NOT_SETTLED,
    SYSTEM_MD_UNKNOWN,
    SYSTEM_REASONS,
    SYSTEM_RECON_BREAK,
    SYSTEM_STARTING,
    StateContribution,
    aggregate,
    merge_since,
    same_restrictions,
    system_restriction,
)
from futrisk.core.risk_account import Lifecycle, RiskAccount, TokenBucket
from futrisk.core.rules import (
    CompiledRule,
    LevelSpec,
    RuleBook,
    acct_scope,
    alert_key,
    compile_rule,
    parse_scope,
)
from futrisk.core.state_machine import LevelMachine, state_from_row
from futrisk.messages import codes, topics
from futrisk.messages.base import Msg, decode, encode
from futrisk.messages.enums import (
    AccountState,
    Action,
    BrokerOrderStatus,
    CashType,
    ControlOrigin,
    DayRollMode,
    FirmAction,
    LiquidationStatus,
    MdQuality,
    Offset,
    OrderSource,
    OrderStatus,
    OrderType,
    PosDirection,
    ReconBreakKind,
    ReconBreakStatus,
    ReconResolution,
    RiskEventKind,
    Role,
    RuleSmState,
    Severity,
    TimeInForce,
    TradeOrigin,
    TradingMode,
)
from futrisk.messages.envelope import Reply, err_reply, ok_reply
from futrisk.messages.eod import (
    DayRoll,
    EodFreezeAck,
    EodFreezeRequest,
    EodRolloverRequest,
    ForceUnfreezeRequest,
    SodAccount,
    SodLot,
    SodRequest,
    SodSnapshot,
)
from futrisk.messages.md import (
    FLAG_LIMIT_DOWN_LOCKED,
    FLAG_LIMIT_UP_LOCKED,
    FLAG_WM_RESET,
    MdStatus,
    SettleInputsBatch,
    SettleInputsRequest,
    Tick,
)
from futrisk.messages.orders import (
    BrokerOrderBatch,
    BrokerOrderUpdate,
    BrokerPositionBatch,
    CancelAllAck,
    CancelAllRequest,
    CancelRequest,
    FillRef,
    OrderAck,
    OrderQuery,
    OrderRequest,
    PositionQuery,
    RouteAck,
    RouteOrder,
    Trade,
    TradeBatch,
    TradeQuery,
)
from futrisk.messages.risk import (
    AccountSnapshot,
    AccountSnapshotBatch,
    AccountStateChanged,
    AckAlertRequest,
    CashBatch,
    CashMovement,
    CashMoveRequest,
    FillBatch,
    FirmControl,
    FirmSnapshot,
    HaltRequest,
    KillSwitchRequest,
    LiquidationConfirm,
    LiquidationPlan,
    LiquidationPlanBatch,
    OrderBatch,
    PositionBatch,
    QueryAccounts,
    QueryAlerts,
    QueryCash,
    QueryFills,
    QueryLiquidationPlans,
    QueryMetrics,
    QueryOrders,
    QueryPositions,
    QueryReconBreaks,
    QueryRules,
    ReconBreakBatch,
    ReconBreakView,
    ReconResolveRequest,
    ResetAck,
    ResetRequest,
    Restriction,
    RiskEventBatch,
    RuleSet,
)

NS_PER_MS = 1_000_000
NS_PER_S = 1_000_000_000
RESOLVE_RETRY_NS = 5 * NS_PER_S  # route.resolve failed / still UNKNOWN: ask again
RESOLVE_INFLIGHT_TIMEOUT_NS = 30 * NS_PER_S  # route.resolve without any answer: send it again
CANCEL_TTL_NS = 5 * NS_PER_S  # CANCEL_PENDING still open after this: route.resolve, re-send (02 §8.3)
CANCEL_TTL_MAX_NS = 60 * NS_PER_S  # exponential backoff cap of the CANCEL_PENDING checks
CANCEL_ESCALATE_ATTEMPTS = 5  # route.cancel sends before an ORDER_STATE_CONFLICT break is opened
SNAPSHOT_KEEPALIVE_NS = NS_PER_S  # every account snapshot at least once a second (coordinator freshness)
_STATE_RANK = {Action.CIRCUIT_BREAK: 3, Action.RESTRICT: 2, Action.ALERT: 1, Action.LIQUIDATE: 0}
MANUAL_RULE_ID = "manual_halt"  # 人工熔断以一个锁存的伪规则状态机表示（rule_states / alerts 同一套持久化）
MANUAL_LEVEL = "halt"
CLEARING = "clearing-worker"
TRADE_GATEWAY = "trade-gateway"
MD_GATEWAY = "md-gateway"
API_GATEWAY = "api-gateway"
RISK_MANAGER_ROLES = frozenset({Role.RISK_MANAGER, Role.ADMIN})

_MANUAL_RULE: CompiledRule = compile_rule(
    RuleDef(
        id=MANUAL_RULE_ID,
        metric="manual",
        op="<=",
        levels=[RuleLevel(name=MANUAL_LEVEL, threshold=0.0, action=Action.CIRCUIT_BREAK, latch=True)],
    )
)


@dataclass
class _Pending:
    """An outstanding :class:`~futrisk.core.effects.Request` (02 §8.1 待办表)."""

    kind: str
    tokens: list[Any] = field(default_factory=list)  # reply tokens waiting for the result
    payload: Msg | None = None
    data: dict[str, Any] = field(default_factory=dict)


class _QualityView(Mapping[str, MdQuality]):
    """Read-only ``instrument -> MdQuality`` view of the engine (md-down aware) for :func:`build_valuation`."""

    __slots__ = ("_eng",)

    def __init__(self, eng: EngineCore) -> None:
        self._eng = eng

    def __getitem__(self, instrument_id: str) -> MdQuality:
        return self._eng.quality_of(instrument_id)

    def __iter__(self) -> Iterator[str]:
        return iter(self._eng._quality)

    def __len__(self) -> int:
        return len(self._eng._quality)


MAX_ORDER_QTY = 2**31 - 1  # orders.qty is an INTEGER with CHECK(qty > 0) (03 §4.3)


def _storable(req: OrderRequest) -> bool:
    """The order fits the ``orders`` row (03 §4.3): 0 < qty <= 2³¹−1, price None or a positive finite number."""
    if not 0 < req.qty <= MAX_ORDER_QTY:
        return False
    return req.price is None or (math.isfinite(req.price) and req.price > 0)


def _qmoney(value: str) -> Decimal | None:
    """Strict positive money amount with at most two decimals (03 §4.3 cash_movements.amount)."""
    try:
        d = Decimal(value)
    except Exception:
        return None
    if not d.is_finite() or d <= 0 or d != d.quantize(Decimal("0.01")):
        return None
    return d


class EngineCore:
    """Pure, deterministic core of one risk-engine shard (see the module docstring and ENGINE.md)."""

    def __init__(
        self,
        *,
        shard: int,
        registry: InstrumentRegistry,
        rules: RiskRulesConfig,
        accounts: AccountsConfig,
        clock: Clock,
        ids: IdGen,
        calendar: TradingCalendar | None = None,
        instance: str | None = None,
        coordinator: bool = False,
        config_hash: str = "",
        config_version: int = 0,
        coordinator_instance: str = "risk-engine-0",
        auto_provisional: bool = True,
        query_trades_on_roll: bool | None = None,
    ) -> None:
        self.shard = shard
        self.instance = instance or f"risk-engine-{shard}"
        self.coordinator_instance = coordinator_instance
        self.auto_provisional = auto_provisional  # False: the caller (backtest) always rolls explicitly
        # ROLL / PROVISIONAL ask td.query.trades for the new day (fills buffered in memory before the roll)
        self.query_trades_on_roll = auto_provisional if query_trades_on_roll is None else query_trades_on_roll
        self._registry = registry
        self._calendar = calendar if calendar is not None else registry.calendar
        self.clock = clock
        self.ids = ids
        self.accounts_cfg = accounts
        self.rulebook = RuleBook(rules, config_hash=config_hash, config_version=config_version)
        self.owned: tuple[str, ...] = tuple(accounts.accounts_of_shard(shard))
        self._accounts: dict[str, RiskAccount] = {}
        self._orders = OrderBook()
        self.events = EventLog(ids)
        self.alerts: dict[str, AlertRecord] = {}
        self.plans: dict[str, LiquidationPlan] = {}
        self.breaks: dict[str, ReconBreakView] = {}
        self._processed: dict[str, tuple[str, Reply]] = {}
        self.firm = FirmState()
        self.aggregator: FirmAggregator | None = (
            FirmAggregator(registry, expected_shards=accounts.shards()) if coordinator else None
        )
        self._ticks: dict[str, Tick] = {}
        self._day_marks: dict[str, float] = {}  # last mark of every instrument on the current trading day
        self._rule_due_all: int | None = None  # cached next_rule_due_ns() (valid while _rule_due_all_ok)
        self._rule_due_all_ok = False
        self._rule_dues: dict[str, int] = {}  # account -> earliest rule timer (only accounts that have one)
        self._rule_due_stale: set[str] = set()  # accounts whose entry is recomputed at the next call
        self._static = StaticCache(registry, "")
        self._qview = _QualityView(self)
        self._sessions: dict[tuple[str, ...], list[tuple[int, int]]] = {}  # 当前交易日的交易时段（含集合竞价）
        # 01 §6.1 by_instrument 索引：合约 -> 持仓或有在途委托的账户（行情只更新并标脏这些账户）
        self._by_instrument: dict[str, set[str]] = {}
        self._acct_insts: dict[str, set[str]] = {}
        self._quality: dict[str, MdQuality] = {}  # effective quality per instrument (before the md-down override)
        self._status_q: dict[str, MdQuality] = {}  # latest md.status quality per instrument: the authority
        self._live_seen: set[str] = set()  # instruments that received a LIVE tick (md.tick stream)
        self._md_down = False
        self._sod: SodSnapshot | None = None
        self._trading_day = ""
        self._mode = TradingMode.OPEN
        self._eod: dict[str, EodStatus] = {}
        self._lifecycle = Lifecycle.LOADING
        self._gateway_up = True
        self._pending: dict[str, _Pending] = {}
        self._prev_day: tuple[str, dict[str, list[Trade]], dict[str, list[CashMovement]]] | None = None
        self._next_day_cash: list[CashMovement] = []
        self._future_trades: list[Trade] = []
        self._last_dayroll: DayRoll | None = None
        self._dayrolls: dict[tuple[str, int], DayRoll] = {}  # (to_trading_day, sod_version) -> reply of eod.rollover
        self._provisional_pending = False
        self._prov_trigger: tuple[str, int | None] | None = None  # (trading day, first session start of the next day)
        self._next_snapshot_ns: int | None = None
        self._next_sample_ns: int | None = None
        self._next_recon_ns: int | None = None
        self._next_firm_ns: int | None = None

    # ================================================================ host view
    # (PretradeHost protocol, 02 §11.2)
    @property
    def registry(self) -> InstrumentRegistry:
        return self._registry

    @property
    def calendar(self) -> TradingCalendar | None:
        return self._calendar

    @property
    def trading_day(self) -> str:
        return self._trading_day

    @property
    def lifecycle(self) -> Lifecycle:
        return self._lifecycle

    @property
    def trading_mode(self) -> TradingMode:
        return self._mode

    @property
    def gateway_up(self) -> bool:
        return self._gateway_up

    @property
    def pretrade_cfg(self) -> Any:
        return self.rulebook.config.pretrade

    @property
    def orders(self) -> OrderBook:
        return self._orders

    @property
    def sod_version(self) -> int | None:
        return self._sod.version if self._sod is not None else None

    @property
    def provisional(self) -> bool:
        return self._sod is not None and self._sod.version == PROVISIONAL_SOD_VERSION

    def account(self, account_id: str) -> RiskAccount:
        return self._accounts[account_id]

    def account_ids(self) -> list[str]:
        return sorted(self._accounts)

    def quality_of(self, instrument_id: str) -> MdQuality:
        if self._md_down:
            return MdQuality.STALE if self._quality.get(instrument_id) != MdQuality.UNKNOWN else MdQuality.UNKNOWN
        return self._quality.get(instrument_id, MdQuality.UNKNOWN)

    def kill_switch_effective(self) -> bool:
        return self.firm.kill_switch

    def _pre_settle(self, instrument_id: str) -> Decimal | None:
        if self._sod is not None and instrument_id in self._sod.pre_settle:
            return D(self._sod.pre_settle[instrument_id])
        return None

    def mark_of(self, instrument_id: str) -> float | None:
        t = self._ticks.get(instrument_id)
        if t is not None and t.trading_day == self._trading_day:
            return self._tick_mark(t)
        ps = self._pre_settle(instrument_id)
        return float(ps) if ps is not None else None

    def limits_of(self, instrument_id: str) -> tuple[float, float] | None:
        t = self._ticks.get(instrument_id)
        if t is not None and t.trading_day == self._trading_day and t.upper_limit > 0 and t.lower_limit > 0:
            return (t.upper_limit, t.lower_limit)
        ps = self._pre_settle(instrument_id)
        if ps is None or not self._registry.has(instrument_id):
            return None
        up, lo = self._registry.limit_prices(instrument_id, ps, self._trading_day or None)
        return (float(up), float(lo))

    def session_open(self, instrument_id: str, now_ns: int) -> bool:
        """True inside a session window (auction or continuous) of the engine's trading day (01 §8.2).

        Windows come from the trading calendar and are cached per product
        session template set for the current trading day.
        """
        cal = self._calendar
        if cal is None:
            return True
        key = tuple(self._registry.product_spec(instrument_id).sessions)
        wins = self._sessions.get(key)
        if wins is None:
            wins = [(w.start_ns, w.end_ns) for w in cal.windows(key, parse_td(self._trading_day))]
            self._sessions[key] = wins
        return any(a <= now_ns < b for a, b in wins)

    def pretrade_rules(self) -> Sequence[CompiledRule]:
        return self.rulebook.pretrade_rules()

    def static_info(self, instrument_id: str) -> StaticInfo:
        """Product, sector, exchange, multiplier and date-effective limit pct (cached per trading day)."""
        return self._static.get(instrument_id)

    def valuation(self, account: RiskAccount) -> AccountValuation:
        if account.valuation is None:
            account.valuation = self._value(account)
            self._rules_touched(account)  # the quality gating of its timers now uses this valuation
        return account.valuation

    @staticmethod
    def _tick_mark(t: Tick) -> float:
        # 02 §7.6：封板时按涨跌停价估值
        if t.flags & FLAG_LIMIT_UP_LOCKED and t.upper_limit > 0:
            return t.upper_limit
        if t.flags & FLAG_LIMIT_DOWN_LOCKED and t.lower_limit > 0:
            return t.lower_limit
        return t.last

    def _now(self) -> int:
        return self.clock.now_ns()

    # ============================================================ SOD install
    def _new_account(self, sa: SodAccount, sod: SodSnapshot) -> RiskAccount:
        ledger = IntradayLedger(self._registry, sod.trading_day, sa, {k: D(v) for k, v in sod.pre_settle.items()})
        cfg = self.rulebook.config.pretrade
        ra = RiskAccount(account_id=sa.account_id, ledger=ledger, bucket=TokenBucket(cfg.throttle_rate_per_s, cfg.throttle_burst))
        ra.intraday_peak = float(D(sa.pre_balance) - D(sa.cum_net_deposit))
        for inst, t in self._ticks.items():
            if t.trading_day == sod.trading_day:
                ledger.set_mark(inst, self._tick_mark(t))
        return ra

    def _stage_accounts(self, sod: SodSnapshot) -> dict[str, RiskAccount]:
        """Build the new ledgers of ``sod`` without touching the engine (validation before a roll)."""
        staged: dict[str, RiskAccount] = {}
        for sa in sod.accounts:
            if sa.account_id not in self.owned:
                continue
            if sa.account_id in staged:
                raise ValueError(f"SOD {sod.trading_day} v{sod.version}: account {sa.account_id} listed twice")
            staged[sa.account_id] = self._new_account(sa, sod)
        missing = sorted(set(self._accounts) - set(staged))
        if missing:
            raise ValueError(f"SOD {sod.trading_day} v{sod.version} misses accounts {missing} of shard {self.shard}")
        return staged

    def _install_sod(self, sod: SodSnapshot, *, keep_state: bool, staged: dict[str, RiskAccount] | None = None) -> None:
        """Replace the SOD (and the ledgers). ``keep_state`` keeps machines / account states (REBASE)."""
        old = self._accounts
        new: dict[str, RiskAccount] = {}
        if sod.trading_day != self._trading_day:
            self._static = StaticCache(self._registry, sod.trading_day)
            self._day_marks = {i: self._tick_mark(t) for i, t in self._ticks.items() if t.trading_day == sod.trading_day}
        if staged is None:
            staged = self._stage_accounts(sod)
        for sa in sod.accounts:
            ra_staged = staged.get(sa.account_id)
            if ra_staged is None:
                continue
            ra = ra_staged
            prev = old.get(sa.account_id)
            if prev is not None:
                ra.bucket = prev.bucket
                ra.state_version = prev.state_version
                ra.state = prev.state
                ra.halted_latched = prev.halted_latched
                ra.state_since_ns = prev.state_since_ns
                ra.cause = prev.cause
                ra.cause_alert_key = prev.cause_alert_key
                ra.restrictions = list(prev.restrictions)
                ra.system = dict(prev.system)
                ra.snapshot_version = prev.snapshot_version
                if keep_state:
                    ra.machines = prev.machines
                    ra.intraday_peak = max(ra.intraday_peak, prev.intraday_peak)
                    ra.orders_today = prev.orders_today
                    ra.cancels_today = prev.cancels_today
                    ra.cancels_by_inst = dict(prev.cancels_by_inst)
                    ra.large_cancels_by_inst = dict(prev.large_cancels_by_inst)
                    ra.market_seq = prev.market_seq
            new[sa.account_id] = ra
        self._accounts = new
        self._rules_touched()
        self._sod = sod
        if sod.trading_day != self._trading_day:
            self._sessions = {}
            self._prov_trigger = None
        self._trading_day = sod.trading_day
        self._eod.setdefault(sod.trading_day, EodStatus(sod.trading_day))
        self._by_instrument = {}
        self._acct_insts = {}
        for ra in new.values():
            self._reindex(ra)

    def _sod_rows(self, tx: Tx, sod: SodSnapshot, now_ns: int) -> None:
        """``sod_accounts`` / ``sod_lots`` / ``sod_prices`` replaced as a whole (03 §7 换日或重新定基)."""
        td = sod.trading_day
        prov = sod.version == PROVISIONAL_SOD_VERSION
        for table in ("sod_lots", "sod_accounts", "sod_prices"):
            tx.delete(table, ("trading_day",), {"trading_day": td})
        for sa in sod.accounts:
            if sa.account_id not in self.owned:
                continue
            tx.insert(
                "sod_accounts",
                {
                    "trading_day": td,
                    "account_id": sa.account_id,
                    "sod_version": sod.version,
                    "pre_balance": sa.pre_balance,
                    "initial_capital": sa.initial_capital,
                    "cum_net_deposit": sa.cum_net_deposit,
                    "hwm_equity": sa.hwm_equity,
                    "provisional": prov,
                    "loaded_ns": now_ns,
                },
            )
            for lot in sa.lots:
                tx.insert(
                    "sod_lots",
                    {
                        "trading_day": td,
                        "account_id": sa.account_id,
                        "instrument_id": lot.instrument_id,
                        "direction": lot.direction.value,
                        "lot_no": lot.lot_no,
                        "sod_version": sod.version,
                        "open_td": lot.open_td,
                        "open_ts_ns": lot.open_ts_ns,
                        "open_price": lot.open_price,
                        "qty": lot.qty,
                        "basis_price": lot.basis_price,
                    },
                )
        for inst, px in sorted(sod.pre_settle.items()):
            tx.insert("sod_prices", {"trading_day": td, "instrument_id": inst, "pre_settle": px, "sod_version": sod.version})

    def load_sod(self, sod: SodSnapshot) -> list[Effect]:
        """Initial load (建账推送 / first start): install the SOD and persist it.

        The lifecycle is left as the caller set it: while LOADING nothing is
        evaluated (the shell moves to WARMUP after its fact catch-up, the
        backtest straight to RUNNING).
        """
        now = self._now()
        tx = Tx(label="load_sod")
        self._install_sod(sod, keep_state=False)
        self._sod_rows(tx, sod, now)
        tx.upsert("eod_states", ("trading_day",), self._eod[sod.trading_day].row())
        tx.mark_critical()
        for ra in self._accounts.values():
            if self._lifecycle == Lifecycle.WARMUP:
                ra.system[SYSTEM_STARTING] = system_restriction(ra.account_id, SYSTEM_STARTING, now)
            self._refresh_state(ra, tx, now, cause="SYSTEM")
        return tx.effects()

    # ============================================================ market data
    def apply_mark(self, tick: Tick, *, live: bool = True) -> None:
        """Apply one tick (02 §8.1 step 5); marks holders dirty. Older or equal ``order_key`` is ignored,
        except for a live tick flagged ``FLAG_WM_RESET`` (md-gateway rolled its watermark back).

        ``live=True`` (the ``md.tick`` stream, LIVE-only at md-gateway): an
        instrument without an ``md.status`` entry becomes OK, and the tick is a
        new LIVE market state (``confirm.states``) for the accounts holding it.
        ``md.status`` is the authority: a live tick never raises the quality
        above the latest status of its instrument. ``live=False`` (e.g. a tick
        from a ``ctl.snapshot`` of md-gateway's LV cache, possibly old): only
        the mark / price moves; quality and the LIVE state key do not.
        """
        inst = tick.instrument_id
        prev = self._ticks.get(inst)
        if prev is not None and tick.order_key <= prev.order_key and not (live and tick.flags & FLAG_WM_RESET):
            return  # a LIVE tick flagged WM_RESET follows md-gateway's watermark roll-back: apply it anyway
        self._ticks[inst] = tick
        if live and inst not in self._live_seen:
            self._live_seen.add(inst)
            if self._requalify(inst):
                self._quality_changed(inst)
        if tick.trading_day != self._trading_day:
            return  # 其他交易日的行情（如换日前的夜盘）换日后再用；当日最后价保存在 _day_marks
        mark = self._tick_mark(tick)
        self._day_marks[inst] = mark
        for a in self._by_instrument.get(inst, ()):
            ra = self._accounts.get(a)
            if ra is not None:
                ra.ledger.set_mark(inst, mark)  # 增量估值：只重估该合约
                if live and inst in ra.held_insts:
                    ra.touch_market()  # 持仓合约的新 LIVE 行情才是新的市场状态（confirm.states）
                else:
                    ra.touch()

    def _requalify(self, instrument_id: str) -> bool:
        """Effective quality = the latest md.status (authority), else OK after a LIVE tick, else UNKNOWN."""
        st = self._status_q.get(instrument_id)
        if st is None:
            st = MdQuality.OK if instrument_id in self._live_seen else MdQuality.UNKNOWN
        if self._quality.get(instrument_id) == st:
            return False
        self._quality[instrument_id] = st
        return True

    def _quality_changed(self, instrument_id: str) -> None:
        for a in self._by_instrument.get(instrument_id, ()):
            ra = self._accounts.get(a)
            if ra is not None:
                ra.dirty = True
                ra.snapshot_dirty = True
                self._rules_touched(ra)

    @staticmethod
    def _holds(ra: RiskAccount, instrument_id: str) -> bool:
        book = ra.ledger.book
        for d in (PosDirection.LONG, PosDirection.SHORT):
            pos = book.find(ra.account_id, instrument_id, d)
            if pos is not None and pos.qty > 0:
                return True
        return False

    def _index_add(self, ra: RiskAccount, instrument_id: str) -> None:
        self._by_instrument.setdefault(instrument_id, set()).add(ra.account_id)
        self._acct_insts.setdefault(ra.account_id, set()).add(instrument_id)
        mark = self._day_marks.get(instrument_id)
        if mark is not None:
            ra.ledger.set_mark(instrument_id, mark)  # 新纳入的合约立即按当日最新行情估值

    def _index_remove(self, ra: RiskAccount, instrument_id: str) -> None:
        holders = self._by_instrument.get(instrument_id)
        if holders is not None:
            holders.discard(ra.account_id)
            if not holders:
                del self._by_instrument[instrument_id]
        self._acct_insts.get(ra.account_id, set()).discard(instrument_id)

    def _reindex(self, ra: RiskAccount, instrument_id: str | None = None) -> None:
        """Maintain the by_instrument index of one account (positions + working orders).

        With ``instrument_id`` only that instrument is re-checked (incremental,
        after a fill or an order change); without it the whole account is
        rebuilt (SOD install).
        """
        a = ra.account_id
        cur = self._acct_insts.setdefault(a, set())
        if instrument_id is not None:
            held = self._holds(ra, instrument_id)
            if held:
                ra.held_insts.add(instrument_id)
            else:
                ra.held_insts.discard(instrument_id)
            needed = held or bool(self._orders.resting_on(a, instrument_id))
            if needed and instrument_id not in cur:
                self._index_add(ra, instrument_id)
            elif not needed and instrument_id in cur:
                self._index_remove(ra, instrument_id)
            return
        ra.held_insts = {i for i, _d, _q in ra.ledger.held()}
        new = ra.held_insts | {o.instrument_id for o in self._orders.resting(a)}
        for inst in sorted(new - cur):
            self._index_add(ra, inst)
        for inst in sorted(cur - new):
            self._index_remove(ra, inst)

    def holders(self, instrument_id: str) -> list[str]:
        """Accounts affected by a tick of ``instrument_id`` (the by_instrument index)."""
        return sorted(self._by_instrument.get(instrument_id, ()))

    def apply_md_status(self, status: MdStatus) -> None:
        """Apply ``md.status``: per-instrument quality (01 §8.13); accounts holding changed instruments become dirty.

        The md-gateway heartbeat state (:meth:`set_md_down`) is independent: a
        status message never clears it (only ``set_md_down(False)`` does).
        """
        for s in status.instruments:
            self._status_q[s.instrument_id] = s.quality
            if self._requalify(s.instrument_id):
                self._quality_changed(s.instrument_id)

    def set_md_down(self, down: bool = True) -> None:
        """md-gateway heartbeat lost / back (02 §4.8): every instrument counts as STALE while down."""
        if self._md_down == down:
            return
        self._md_down = down
        self._rules_touched()
        for ra in self._accounts.values():
            ra.dirty = True
            ra.snapshot_dirty = True

    # ============================================================ evaluation
    def _value(self, ra: RiskAccount) -> AccountValuation:
        val = build_valuation(
            ra.ledger,
            self._registry,
            quality=self._qview if self._md_down else self._quality,  # 行情心跳丢失时统一视为 STALE
            intraday_peak=ra.intraday_peak,
            orders_today=ra.orders_today,
            cancels_today=ra.cancels_today,
            static=self._static,
            compliance_util=ra.compliance_util(self._registry, self.clock.monotonic_ns()),  # bucket: pre-trade clock
            quality_tolerance=self.rulebook.config.system.quality_stale_tolerance,
        )
        if val.quality == MdQuality.OK:
            # 日内峰值只在行情可信时推进（01 §8.14 质量门控）
            ra.intraday_peak = max(ra.intraday_peak, val.metrics.perf_equity)
        return val

    def dirty_accounts(self) -> list[str]:
        return sorted(a for a, ra in self._accounts.items() if ra.dirty)

    def evaluate(self, account_id: str) -> list[Effect]:
        """Evaluate one account now: metrics → rules → state machines → actions (02 §8.1 step 6).

        No-op while LOADING (the SOD alone, before the fact catch-up, is not the account's state).
        """
        ra = self._accounts.get(account_id)
        if ra is None or self._lifecycle == Lifecycle.LOADING:
            return []
        tx = Tx(label="evaluate")
        self._evaluate(ra, tx, self._now())
        return tx.effects()

    def evaluate_dirty(self) -> list[Effect]:
        """Evaluate every dirty account once (one transaction for the whole loop iteration); no-op while LOADING."""
        if self._lifecycle == Lifecycle.LOADING:
            return []
        tx = Tx(label="evaluate")
        now = self._now()
        for a in self.dirty_accounts():
            self._evaluate(self._accounts[a], tx, now)
        return tx.effects()

    def evaluate_all(self) -> list[Effect]:
        """Full evaluation of every account (e.g. before RUNNING after a restart, 01 §6.5)."""
        for ra in self._accounts.values():
            ra.dirty = True
        return self.evaluate_dirty()

    def _evaluate(self, ra: RiskAccount, tx: Tx, now: int) -> None:
        val = self._value(ra)
        ra.valuation = val
        self._rules_touched(ra)
        ra.dirty = False
        ra.snapshot_dirty = True
        # 01 §8.5：账户超过 50% 的敞口为 UNKNOWN 时，整个账户禁开
        share = val.unknown_exposure_share()
        had_unknown = SYSTEM_MD_UNKNOWN in ra.system
        if share > self.rulebook.config.system.md_unknown_account_block_pct:
            ra.system.setdefault(SYSTEM_MD_UNKNOWN, system_restriction(ra.account_id, SYSTEM_MD_UNKNOWN, now))
        else:
            ra.system.pop(SYSTEM_MD_UNKNOWN, None)
        evals = evaluate_owner(
            self.rulebook.account_rules(), ra.machines, val, account_id=ra.account_id, state_key=ra.market_seq, now_ns=now
        )
        sys_changed = had_unknown != (SYSTEM_MD_UNKNOWN in ra.system)
        if not evals and not sys_changed and val.largest_product() == ra.refresh_largest:
            return  # 没有迁移、限制的输入未变：账户状态不会变化，跳过重算
        self._after_transitions(ra, evals, tx, now, cause="SYSTEM" if sys_changed and not evals else "RULE")

    def _after_transitions(
        self, ra: RiskAccount, evals: Sequence[LevelEval], tx: Tx, now: int, *, cause: str = "RULE"
    ) -> None:
        self._rules_touched(ra)
        activated = persist_transitions(
            tx,
            evals,
            events=self.events,
            alerts=self.alerts,
            trading_day=self._trading_day,
            account_id=ra.account_id,
            config_hash=self.rulebook.config_hash,
            now_ns=now,
        )
        for ev in activated:
            if ev.machine.level.action == Action.LIQUIDATE:
                self._create_plan(ra, ev.machine, tx, now)
        cause_key = self._cause_key(ra) if evals else None
        self._refresh_state(ra, tx, now, cause=cause, cause_alert_key=cause_key)
        for ev in evals:
            m = ev.machine
            if m.state == RuleSmState.OK and m.st.cooldown_until_ns is None and m.st.episode == 0:
                ra.machines.pop(m.key, None)  # 确认前撤销且从未生效：无需保留

    def _cause_key(self, ra: RiskAccount) -> str | None:
        """Alert key of the most severe effective level (the one that sets the account state).

        Ranked by the action's account state (CIRCUIT_BREAK > RESTRICT > ALERT >
        LIQUIDATE), then the level index; None while that level is a circuit
        break still PENDING (no alert yet).
        """
        best: LevelMachine | None = None
        best_rank: tuple[int, int, int, tuple[str, str, str]] | None = None
        for m in ra.machines.values():
            if not is_effective(m.level.action, m.state):
                continue
            rank = (_STATE_RANK[m.level.action], m.level.index, m.st.active_since_ns or 0, m.key)
            if best_rank is None or rank > best_rank:
                best, best_rank = m, rank
        if best is None or best.state != RuleSmState.ACTIVE or best.st.episode <= 0:
            return None
        return best.alert_key(self._trading_day)

    # ------------------------------------------------------- account state
    def _contributions(self, ra: RiskAccount, now: int) -> tuple[list[StateContribution], list[Restriction]]:
        contribs: list[StateContribution] = []
        restr: list[Restriction] = []
        largest = ra.valuation.largest_product() if ra.valuation is not None else None
        ra.refresh_largest = largest
        for m in sorted(ra.machines.values(), key=lambda x: x.key):
            out = action_outcome(
                ActionInput(
                    rule_id=m.rule.rule_id,
                    level=m.level,
                    scope_key=m.scope_key,
                    account_id=ra.account_id,
                    sm_state=m.state,
                    latched=m.st.latched,
                    largest_product=largest,
                    since_ns=now,
                )
            )
            if out.contribution is not None:
                contribs.append(out.contribution)
            restr.extend(out.restrictions)
        for reason in SYSTEM_REASONS:
            r = ra.system.get(reason)
            if r is not None:
                contribs.append(StateContribution(AccountState.RESTRICTED))
                restr.append(r)
        if self.firm.restrictions:
            contribs.append(StateContribution(AccountState.RESTRICTED))
            restr.extend(self.firm.restrictions)
        if self.firm.kill_switch:
            contribs.append(StateContribution(AccountState.RESTRICTED))
        return contribs, restr

    def _refresh_state(
        self,
        ra: RiskAccount,
        tx: Tx,
        now: int,
        *,
        cause: str,
        cause_alert_key: str | None = None,
        actor: str | None = None,
        reason: str | None = None,
    ) -> AccountStateChanged | None:
        """Recompute state + restrictions; persist and publish ``risk.state`` on change (01 §8.14 账户状态的聚合)."""
        contribs, restr = self._contributions(ra, now)
        state, latched = aggregate(contribs)
        new_restr = merge_since(restr, ra.restrictions)
        if state == ra.state and latched == ra.halted_latched and same_restrictions(new_restr, ra.restrictions):
            return None
        prev_state, prev_latched = ra.state, ra.halted_latched
        if state != ra.state:
            ra.state_since_ns = now
        ra.state = state
        ra.halted_latched = latched
        ra.restrictions = new_restr
        ra.state_version += 1
        ra.cause = cause
        ra.cause_alert_key = cause_alert_key
        ra.snapshot_dirty = True
        msg = AccountStateChanged(
            account_id=ra.account_id,
            trading_day=self._trading_day,
            from_state=prev_state,
            to_state=state,
            halted_latched=latched,
            state_version=ra.state_version,
            restrictions=list(new_restr),
            cause=cause,
            cause_alert_key=cause_alert_key,
            actor=actor,
            reason=reason,
            ts_ns=now,
        )
        tx.upsert("account_states", ("account_id",), self._account_state_row(ra, now))
        tx.emit(topics.risk_state(ra.account_id), msg, ts_ns=now, corr_id=cause_alert_key, td=self._trading_day)
        if state == AccountState.HALTED or prev_state == AccountState.HALTED or latched != prev_latched:
            tx.mark_critical()
        if state == AccountState.HALTED and prev_state != AccountState.HALTED:
            # 01 §8.14 / 02 §8.3：进入 HALTED（含待确认阶段）立即撤销所有挂着的开仓单，包括 ROUTING 和 UNKNOWN
            self._cancel_open_orders(ra, tx, now, reason=R_CIRCUIT_BREAK_CANCEL)
        return msg

    def _account_state_row(self, ra: RiskAccount, now: int) -> dict[str, Any]:
        return {
            "account_id": ra.account_id,
            "trading_day": self._trading_day,
            "state": ra.state.value,
            "halted_latched": ra.halted_latched,
            "restrictions": [msgspec_restriction(r) for r in ra.restrictions],
            "cause": ra.cause,
            "cause_alert_key": ra.cause_alert_key,
            "state_version": ra.state_version,
            "since_ns": ra.state_since_ns,
            "updated_ns": now,
        }

    def _current_state_message(self, ra: RiskAccount, prev: AccountState, now: int, *, cause: str) -> AccountStateChanged:
        return AccountStateChanged(
            account_id=ra.account_id,
            trading_day=self._trading_day,
            from_state=prev,
            to_state=ra.state,
            halted_latched=ra.halted_latched,
            state_version=ra.state_version,
            restrictions=list(ra.restrictions),
            cause=cause,
            cause_alert_key=ra.cause_alert_key,
            ts_ns=now,
        )

    def _cancel_open_orders(self, ra: RiskAccount, tx: Tx, now: int, *, reason: str) -> int:
        n = 0
        for o in self._orders.resting(ra.account_id):
            if not o.is_open or o.status not in CANCELLABLE:
                continue
            self._send_cancel(ra, o, tx, now, command_id=self.ids.next_id(), reason=reason)
            n += 1
        return n

    def _send_cancel(self, ra: RiskAccount, o: OrderRecord, tx: Tx, now: int, *, command_id: str, reason: str) -> None:
        """A new cancel intent: CANCEL_PENDING + ``route.cancel`` (02 §8.3); counts as one cancel of the day."""
        o.status_before_cancel = o.status
        o.cancel_command_id = command_id
        o.cancel_sent_ns = now
        o.cancel_attempts += 1
        o.cancel_error = False
        o.cancel_due_ns = None
        o.resolve_next_ns = None
        if set_status(tx, o, OrderStatus.CANCEL_PENDING, now_ns=now, reason=reason):
            self._count_cancel(ra, o)
        tx.cancel(o.client_order_id, command_id, reason)

    def _count_cancel(self, ra: RiskAccount, o: OrderRecord) -> None:
        """Compliance counters: every entry into CANCEL_PENDING is one cancel (restore counts order_events the same way)."""
        ra.cancels_today += 1
        ra.cancels_by_inst[o.instrument_id] = ra.cancels_by_inst.get(o.instrument_id, 0) + 1
        large = self._registry.rules(o.instrument_id).large_cancel
        if large is not None and o.qty >= large.min_qty:
            ra.large_cancels_by_inst[o.instrument_id] = ra.large_cancels_by_inst.get(o.instrument_id, 0) + 1
        ra.counters_changed()  # the cached account.compliance_util is stale
        ra.dirty = True  # re-evaluate: the cancel counts may cross the compliance warning
        ra.snapshot_dirty = True

    def _resend_cancel(self, o: OrderRecord, tx: Tx, now: int, *, reason: str, command_id: str | None = None) -> None:
        """Send ``route.cancel`` again for a CANCEL_PENDING order with a NEW command id (not a new cancel)."""
        cid = command_id or self.ids.next_id()
        o.cancel_command_id = cid
        o.cancel_sent_ns = now
        o.cancel_attempts += 1
        o.cancel_error = False
        o.resolve_next_ns = None
        self._order_note(tx, o, now, reason=reason, detail={"attempt": o.cancel_attempts, "command_id": cid})
        tx.cancel(o.client_order_id, cid, reason)

    @staticmethod
    def _order_note(tx: Tx, rec: OrderRecord, now: int, *, reason: str, detail: dict[str, Any]) -> None:
        """Persist order bookkeeping without a status change (``orders`` upsert + an ``order_events`` note)."""
        rec.updated_ns = now
        persist_order(tx, rec)
        tx.insert("order_events", {"client_order_id": rec.client_order_id, "ts_ns": now, "from_status": rec.status.value,
                                   "to_status": rec.status.value, "reason": reason, "detail": detail})

    @staticmethod
    def _cancel_ttl(attempts: int) -> int:
        """TTL of a pending cancel after ``attempts`` sends: 5 s, 10 s, 20 s, 40 s, then 60 s."""
        return min(CANCEL_TTL_MAX_NS, CANCEL_TTL_NS << min(max(0, attempts - 1), 4))

    def _cancel_check_ns(self, o: OrderRecord) -> int:
        """When the CANCEL_PENDING order ``o`` is checked next (``route.resolve``)."""
        if o.resolve_inflight:
            return (o.resolve_sent_ns or 0) + RESOLVE_INFLIGHT_TIMEOUT_NS
        due = 0 if o.cancel_error else (o.cancel_sent_ns or 0) + self._cancel_ttl(o.cancel_attempts)
        if o.resolve_next_ns is not None:
            due = max(due, o.resolve_next_ns)
        return due

    @staticmethod
    def _unknown_check_ns(o: OrderRecord) -> int:
        """When the UNKNOWN order ``o`` is resolved next."""
        if o.resolve_inflight:
            return (o.resolve_sent_ns or 0) + RESOLVE_INFLIGHT_TIMEOUT_NS
        return o.resolve_next_ns if o.resolve_next_ns is not None else 0

    def _halt_guard(self, ra: RiskAccount | None, o: OrderRecord, tx: Tx, now: int, *, due: bool = False) -> None:
        """01 §8.14 / 02 §8.3: an OPEN order working again while the account is HALTED is cancelled.

        Applies after a refused cancel (revert), a rebuilt order and a resolve
        or route ack that finds the order working. The first re-cancel after a
        refusal is immediate; repeated refusals back off like the cancel TTL
        (``cancel_due_ns``, driven by :meth:`on_timer`). Kill switch and
        BLOCK_OPEN restrictions reject new opens only (02 §11.2), so they never
        cancel resting orders and are not guarded here.
        """
        if ra is None or not o.is_open or o.status not in CANCELLABLE or ra.state != AccountState.HALTED:
            o.cancel_due_ns = None
            return
        if not due and o.cancel_attempts >= 2 and o.cancel_sent_ns is not None:
            at = o.cancel_sent_ns + self._cancel_ttl(o.cancel_attempts - 1)
            if now < at:
                o.cancel_due_ns = at
                return
        self._send_cancel(ra, o, tx, now, command_id=self.ids.next_id(), reason=R_CIRCUIT_BREAK_CANCEL)

    def _cancel_retry(self, ra: RiskAccount | None, o: OrderRecord, tx: Tx, now: int) -> None:
        """A CANCEL_PENDING order is still working at the broker: re-send with a new id, escalate after N sends."""
        if ra is not None and o.cancel_attempts >= CANCEL_ESCALATE_ATTEMPTS:
            self._escalate_cancel(ra, o, tx, now)
        self._resend_cancel(o, tx, now, reason=R_CANCEL_RETRY)

    def _escalate_cancel(self, ra: RiskAccount, o: OrderRecord, tx: Tx, now: int) -> None:
        """ORDER_STATE_CONFLICT recon break + risk.event for a cancel stuck after N sends (once per order)."""
        if o.cancel_escalated:
            return
        o.cancel_escalated = True
        for b in self.breaks.values():
            if (b.status == ReconBreakStatus.OPEN and b.kind == ReconBreakKind.ORDER_STATE_CONFLICT
                    and b.detail.get("client_order_id") == o.client_order_id and b.detail.get("reason") == "CANCEL_STUCK"):
                return
        halted_open = o.is_open and ra.state == AccountState.HALTED
        self._open_break(ra, ReconBreakKind.ORDER_STATE_CONFLICT, False,
                         {"reason": "CANCEL_STUCK", "client_order_id": o.client_order_id, "attempts": o.cancel_attempts,
                          "is_open": o.is_open, "account_halted": ra.state == AccountState.HALTED},
                         tx, now, severity=Severity.HIGH if halted_open else Severity.WARNING)

    # ------------------------------------------------------- liquidation
    def _create_plan(self, ra: RiskAccount, m: LevelMachine, tx: Tx, now: int) -> None:
        """LIQUIDATE: build a plan to confirm or cancel; never executed automatically (01 TD-08)."""
        sk = parse_scope(m.scope_key)
        cands: list[LiquidationCandidate] = []
        for pos in ra.ledger.book.positions(ra.account_id):
            if pos.qty <= 0:
                continue
            info = self._registry.get(pos.instrument_id)
            if sk.kind == "prod" and info.product != sk.value:
                continue
            if sk.kind == "sector" and info.sector != sk.value:
                continue
            if sk.kind == "inst" and pos.instrument_id != sk.value:
                continue
            c = pos.closable()
            cands.append(
                LiquidationCandidate(
                    instrument_id=pos.instrument_id,
                    direction=pos.direction,
                    closable_today=c.today,
                    closable_yd=c.yd,
                    position_pnl=ra.ledger.position_pnl_of(pos.key),
                    explicit_close_today=info.rules.explicit_close_today,
                )
            )
        params = m.level.params
        steps = liquidation_steps(cands, step=float(params.get("step", 0.5)), price_ticks=int(params.get("price_ticks", 2)))
        key = m.alert_key(self._trading_day)
        plan = LiquidationPlan(
            plan_id=self.ids.next_id(),
            account_id=ra.account_id,
            trading_day=self._trading_day,
            trigger_alert_key=key,
            status=LiquidationStatus.PENDING_CONFIRM,
            steps=steps,
            created_ns=now,
        )
        self.plans[plan.plan_id] = plan
        tx.upsert("liquidation_plans", ("plan_id",), self._plan_row(plan))
        self.events.emit(
            tx,
            kind=RiskEventKind.LIQUIDATION_PLAN,
            trading_day=self._trading_day,
            ts_ns=now,
            message=f"{ra.account_id} 生成减仓计划 {len(steps)} 步，等待风控经理确认",
            severity=Severity.CRITICAL,
            account_id=ra.account_id,
            scope_key=m.scope_key,
            alert_key=key,
            rule_id=m.rule.rule_id,
            level=m.level.name,
            episode=m.st.episode,
            action=Action.LIQUIDATE,
            detail={"plan_id": plan.plan_id, "plan": msgspec.to_builtins(plan)},
        )
        tx.mark_critical()

    def _plan_event(self, tx: Tx, plan: LiquidationPlan, now: int, *, actor: str | None) -> None:
        """``risk.event`` (LIQUIDATION_PLAN) for a decided / expired plan: detail {plan_id, status, actor}."""
        text = {LiquidationStatus.CONFIRMED: "已确认", LiquidationStatus.CANCELED: "已取消",
                LiquidationStatus.EXPIRED: "已过期（换日）"}.get(plan.status, plan.status.value)
        self.events.emit(
            tx,
            kind=RiskEventKind.LIQUIDATION_PLAN,
            trading_day=plan.trading_day,
            ts_ns=now,
            message=f"{plan.account_id} 减仓计划 {plan.plan_id} {text}" + (f"（{actor}）" if actor else ""),
            severity=Severity.WARNING if plan.status == LiquidationStatus.CONFIRMED else Severity.INFO,
            account_id=plan.account_id,
            scope_key=acct_scope(plan.account_id),
            alert_key=plan.trigger_alert_key,
            action=Action.LIQUIDATE,
            actor=actor,
            detail={"plan_id": plan.plan_id, "status": plan.status.value, "actor": actor},
        )

    @staticmethod
    def _plan_row(plan: LiquidationPlan) -> dict[str, Any]:
        done = plan.status in (LiquidationStatus.CONFIRMED, LiquidationStatus.CANCELED, LiquidationStatus.EXPIRED)
        return {
            "plan_id": plan.plan_id,
            "account_id": plan.account_id,
            "trading_day": plan.trading_day,
            "trigger_alert_key": plan.trigger_alert_key,
            "status": plan.status.value,
            "steps": msgspec.to_builtins(plan.steps),
            "created_ns": plan.created_ns,
            "confirmed_by": plan.confirmed_by,
            "confirmed_ns": plan.confirmed_ns,
            "finished_ns": plan.confirmed_ns if done else None,
        }

    # ------------------------------------------------------------ snapshots
    def snapshot(self, account_id: str) -> AccountSnapshot:
        """Full ``AccountSnapshot`` of one account (02 §7.6), without bumping its version."""
        return self._snapshot(self._accounts[account_id], self._now())

    def _snapshot(self, ra: RiskAccount, now: int) -> AccountSnapshot:
        val = self.valuation(ra)
        locked = [i for i, t in self._ticks.items() if t.flags & (FLAG_LIMIT_UP_LOCKED | FLAG_LIMIT_DOWN_LOCKED)]
        funds = ra.ledger.funds()
        return AccountSnapshot(
            account_id=ra.account_id,
            shard=self.shard,
            trading_day=self._trading_day,
            version=ra.snapshot_version,
            state_version=ra.state_version,
            as_of_ns=now,
            quality=val.quality,
            state=ra.state,
            halted_latched=ra.halted_latched,
            trading_mode=self._mode,
            restrictions=list(ra.restrictions),
            funds=funds,
            metrics=val.metrics,
            exposures=[val.exposures[p] for p in sorted(val.exposures)],
            positions=ra.ledger.position_views(equity=funds.equity, limit_locked=locked),
            rules=rule_statuses(self.rulebook.account_rules(), ra.machines, val, account_id=ra.account_id),
            open_orders=len(self._orders.resting(ra.account_id)),
        )

    def _publish_snapshots(self, tx: Tx, now: int, *, force: bool = False) -> None:
        """Changed accounts, plus every account at least once per second (keep-alive for the coordinator)."""
        for a in sorted(self._accounts):
            ra = self._accounts[a]
            stale = ra.last_snapshot_ns is None or now - ra.last_snapshot_ns >= SNAPSHOT_KEEPALIVE_NS
            if not (ra.snapshot_dirty or force or stale):
                continue
            ra.snapshot_version += 1
            snap = self._snapshot(ra, now)
            ra.snapshot_dirty = False
            ra.last_snapshot_ns = now
            tx.publish(topics.risk_snapshot(a), snap)
            if self.aggregator is not None:
                self.aggregator.ingest(snap)

    def _metric_rows(self, tx: Tx, now: int) -> None:
        """``metric_samples`` rows, one per account (03 §4.3; every ``metric_sample_s``)."""
        for a in sorted(self._accounts):
            ra = self._accounts[a]
            val = self.valuation(ra)
            util: dict[str, float] = {}
            for rs in rule_statuses(self.rulebook.account_rules(), ra.machines, val, account_id=a):
                if rs.hard_utilization is not None:  # against the hard limit (the dashboard's 使用率)
                    util[f"{rs.rule_id}/{rs.scope_key}"] = rs.hard_utilization
            m, f = val.metrics, val.funds
            tx.insert_ignore(
                "metric_samples",
                ("account_id", "ts_ns"),
                {
                    "account_id": a,
                    "ts_ns": now,
                    "trading_day": self._trading_day,
                    "equity": f.equity,
                    "perf_equity": m.perf_equity,
                    "daily_pnl": m.daily_pnl,
                    "gross_notional": m.gross_notional,
                    "net_notional": m.net_notional,
                    "rule_util": util,
                    "nav": m.nav,
                    "daily_pnl_pct_init": m.daily_pnl_pct_init,
                    "intraday_dd": m.intraday_dd,
                    "hwm_dd": m.hwm_dd,
                    "risk_degree": f.risk_degree,
                    "leverage": m.leverage,
                    "state": ra.state.value,
                    "quality": val.quality.value,
                },
            )

    # ================================================================ timers
    def next_timer_ns(self) -> int | None:
        """Earliest time :meth:`on_timer` has work to do (for the shell's poll timeout).

        Only timers that can make progress count: quality-gated rule timers, a
        provisional trigger already waiting for ``md.settle_inputs`` and order
        checks while the trade-gateway is down (re-driven when it comes back)
        are left out, so the result is never a past time with nothing to do.
        """
        cands: list[int] = []
        rule_due = self.next_rule_due_ns()
        if rule_due is not None:
            cands.append(rule_due)
        up = self._gateway_up
        for o in self._orders.resting():
            st = o.status
            if st == OrderStatus.ROUTING:
                if o.expire_ns:
                    cands.append(o.expire_ns)
            elif not up:
                continue
            elif st == OrderStatus.UNKNOWN:
                cands.append(self._unknown_check_ns(o))
            elif st == OrderStatus.CANCEL_PENDING:
                cands.append(self._cancel_check_ns(o))
            elif o.cancel_due_ns is not None:
                cands.append(o.cancel_due_ns)
        if self._sod is not None and self._lifecycle != Lifecycle.LOADING:
            # 快照 / 采样 / 对账（及 coordinator 的公司层汇总）定时器：还没运行过的视为立即到期；设为 0 = 关闭
            now = self._now()
            sysc = self.rulebook.config.system
            loop: list[int | None] = []
            if sysc.snapshot_hz > 0:
                loop.append(self._next_snapshot_ns)
            if sysc.metric_sample_s > 0:
                loop.append(self._next_sample_ns)
            if sysc.recon_interval_s > 0:
                loop.append(self._next_recon_ns)
            if self.aggregator is not None:
                loop.append(self._next_firm_ns)
            cands.extend(now if t is None else t for t in loop)
        if not self._provisional_pending:
            start = self._provisional_trigger_ns()
            if start is not None:
                cands.append(start)
        return min(cands) if cands else None

    def next_rule_due_ns(self) -> int | None:
        """Earliest confirm / hold / cooldown timer of the accounts' rule machines (quality-gated ones excluded).

        With the periodic timers off (``snapshot_hz`` / ``metric_sample_s`` /
        ``recon_interval_s`` = 0) :meth:`next_timer_ns` is exactly this plus
        the order timers and the provisional trigger, so a backtest can drive
        time with :meth:`next_timer_ns` alone.
        """
        if self._rule_due_all_ok:
            return self._rule_due_all
        dues = self._rule_dues
        for a in self._rule_due_stale:
            ra = self._accounts.get(a)
            d = next_due_ns(ra.machines.values(), ra.valuation) if ra is not None and ra.machines else None
            if d is None:
                dues.pop(a, None)
            else:
                dues[a] = d
        self._rule_due_stale.clear()
        best = min(dues.values()) if dues else None
        self._rule_due_all = best
        self._rule_due_all_ok = True
        return best

    def _rules_touched(self, ra: RiskAccount | None = None) -> None:
        """The machines (or the quality gating) of ``ra`` - or of every account - may have new timers.

        Cache of :meth:`next_rule_due_ns`: only the accounts touched since the
        last call are rescanned (an evaluation, a transition, a quality change,
        a command on their machines); ``ra`` None rescans every account.
        """
        self._rule_due_all_ok = False
        if ra is not None:
            self._rule_due_stale.add(ra.account_id)
        else:
            self._rule_dues.clear()
            self._rule_due_stale = set(self._accounts)

    def on_timer(self, now_ns: int | None = None) -> list[Effect]:
        """Timers (02 §8.1 step 7): provisional trigger, route expiry / resolve retries, confirm / hold /
        cooldown timers, snapshots (``snapshot_hz``), metric samples (``metric_sample_s``), position recon
        (``recon_interval_s``) and, on the coordinator, firm aggregation.

        A periodic timer whose setting is 0 (or negative) is OFF: nothing is
        published / sampled / requested and :meth:`next_timer_ns` ignores it.
        """
        now = self._now() if now_ns is None else now_ns
        tx = Tx(label="timer")
        if self._lifecycle == Lifecycle.LOADING or self._sod is None:
            return []
        self._check_provisional(tx, now)
        self._route_timers(tx, now)
        for a in self.dirty_accounts():
            self._evaluate(self._accounts[a], tx, now)
        for ra in self._accounts.values():
            evals = reevaluate_due(ra.machines, ra.valuation, now)
            if evals:
                self._after_transitions(ra, evals, tx, now)
        sysc = self.rulebook.config.system
        if sysc.snapshot_hz > 0 and (self._next_snapshot_ns is None or now >= self._next_snapshot_ns):
            self._publish_snapshots(tx, now)
            self._next_snapshot_ns = now + int(NS_PER_S / sysc.snapshot_hz)
        if sysc.metric_sample_s > 0 and (self._next_sample_ns is None or now >= self._next_sample_ns):
            self._metric_rows(tx, now)
            self._next_sample_ns = now + int(sysc.metric_sample_s * NS_PER_S)
        if sysc.recon_interval_s > 0 and (self._next_recon_ns is None or now >= self._next_recon_ns):
            if self._next_recon_ns is not None and self._lifecycle in (Lifecycle.WARMUP, Lifecycle.RUNNING):
                self._request_positions(tx, now)
            self._next_recon_ns = now + int(sysc.recon_interval_s * NS_PER_S)
        if self.aggregator is not None and (self._next_firm_ns is None or now >= self._next_firm_ns):
            self._firm_tick(tx, now)
            self._next_firm_ns = now + NS_PER_S
        return tx.effects()

    def _route_timers(self, tx: Tx, now: int) -> None:
        """Route expiry, UNKNOWN resolves, CANCEL_PENDING TTL checks and scheduled halt re-cancels (02 §8.3)."""
        for o in self._orders.resting():
            st = o.status
            if st == OrderStatus.ROUTING:
                if o.expire_ns and now >= o.expire_ns:
                    # 02 §8.3：过了 expire_ns 仍无应答 → UNKNOWN，改用 route.resolve 确认
                    set_status(tx, o, OrderStatus.UNKNOWN, now_ns=now, reason=R_ROUTE_TIMEOUT)
                    if self._gateway_up:
                        self._send_resolve(o, tx, now)
                continue
            if not self._gateway_up:
                continue  # trade-gateway 不可用：恢复时由 set_gateway_status(True) 统一重新驱动
            if st == OrderStatus.UNKNOWN:
                if now >= self._unknown_check_ns(o):
                    self._send_resolve(o, tx, now)
            elif st == OrderStatus.CANCEL_PENDING:
                if now >= self._cancel_check_ns(o):
                    self._send_resolve(o, tx, now)  # 撤单超时：先确认柜台状态，仍在工作再换新 command_id 重发
            elif o.cancel_due_ns is not None and now >= o.cancel_due_ns:
                o.cancel_due_ns = None
                self._halt_guard(self._accounts.get(o.account_id), o, tx, now, due=True)

    def _send_resolve(self, o: OrderRecord, tx: Tx, now: int) -> None:
        o.resolve_inflight = True
        o.resolve_sent_ns = now
        tx.resolve(o.client_order_id, o.trading_day)

    # ============================================================ lifecycle
    def set_lifecycle(self, lifecycle: Lifecycle) -> list[Effect]:
        """LOADING / WARMUP (BLOCK_OPEN ``SYSTEM:STARTING``, closes allowed) / RUNNING / STOPPING (02 §8.6)."""
        now = self._now()
        tx = Tx(label="lifecycle")
        self._lifecycle = lifecycle
        for ra in self._accounts.values():
            if lifecycle == Lifecycle.WARMUP:
                ra.system.setdefault(SYSTEM_STARTING, system_restriction(ra.account_id, SYSTEM_STARTING, now))
            elif lifecycle == Lifecycle.RUNNING:
                ra.system.pop(SYSTEM_STARTING, None)
            self._refresh_state(ra, tx, now, cause="SYSTEM")
        return tx.effects()

    def set_system_restriction(self, account_id: str | None, reason: str, on: bool) -> list[Effect]:
        """Set / clear ``SYSTEM:{reason}`` BLOCK_OPEN on one account (None = all), e.g. FILL_GAP (02 §8.2)."""
        if reason not in SYSTEM_REASONS:
            raise ValueError(f"unknown system restriction {reason!r}")
        now = self._now()
        tx = Tx(label="system_restriction")
        targets = [self._accounts[account_id]] if account_id is not None else list(self._accounts.values())
        for ra in targets:
            if on:
                ra.system.setdefault(reason, system_restriction(ra.account_id, reason, now))
            else:
                ra.system.pop(reason, None)
            self._refresh_state(ra, tx, now, cause="SYSTEM")
            if on and reason == "FILL_GAP":
                self.events.emit(
                    tx,
                    kind=RiskEventKind.FILL_GAP,
                    trading_day=self._trading_day,
                    ts_ns=now,
                    message=f"{ra.account_id} 成交流缺口，禁开直至补齐",
                    severity=Severity.HIGH,
                    account_id=ra.account_id,
                    scope_key=acct_scope(ra.account_id),
                )
        return tx.effects()

    def clear_system_restriction(self, account_id: str | None, reason: str) -> list[Effect]:
        """Lift ``SYSTEM:{reason}`` (e.g. FILL_GAP after the shell's fact catch-up, also one restored from
        ``account_states``) on one account or all (None)."""
        return self.set_system_restriction(account_id, reason, False)

    def open_recon_break(self, account_id: str, kind: ReconBreakKind, detail: Mapping[str, Any], *,
                         blocking: bool = True) -> list[Effect]:
        """Open a recon break from the shell (e.g. ``REPLAY_EXPIRED``: a fact gap outside the replay window,
        02 §8.2); blocking breaks add BLOCK_OPEN ``SYSTEM:RECON_BREAK``. An identical open break is not repeated."""
        ra = self._accounts.get(account_id)
        if ra is None:
            raise KeyError(f"account {account_id} is not loaded on shard {self.shard}")
        d = dict(detail)
        for b in self.breaks.values():
            if (b.account_id == account_id and b.status == ReconBreakStatus.OPEN and b.kind == kind
                    and b.blocking == blocking and dict(b.detail) == d):
                return []
        tx = Tx(label="recon_break")
        self._open_break(ra, kind, blocking, d, tx, self._now())
        return tx.effects()

    def set_gateway_status(self, up: bool) -> list[Effect]:
        """trade-gateway up / down (02 §4.8).

        Down: new orders are rejected, in-flight ROUTING orders become UNKNOWN
        and order checks pause. Up again: every UNKNOWN and CANCEL_PENDING
        order is re-driven at once (``route.resolve`` first; a pending cancel
        still working is then re-sent) and due halt re-cancels go out.
        """
        now = self._now()
        tx = Tx(label="gateway")
        was = self._gateway_up
        self._gateway_up = up
        if was and not up:
            for o in self._orders.resting():
                if o.status == OrderStatus.ROUTING:
                    set_status(tx, o, OrderStatus.UNKNOWN, now_ns=now, reason="GATEWAY_DOWN")
                    o.resolve_next_ns = now
        elif up and not was:
            for o in self._orders.resting():
                if o.status in (OrderStatus.UNKNOWN, OrderStatus.CANCEL_PENDING):
                    o.resolve_next_ns = None
                    o.cancel_error = o.cancel_error or o.status == OrderStatus.CANCEL_PENDING
                    self._send_resolve(o, tx, now)
                elif o.cancel_due_ns is not None:
                    o.cancel_due_ns = None
                    self._halt_guard(self._accounts.get(o.account_id), o, tx, now, due=True)
        return tx.effects()

    # ============================================================ commands
    def handle(self, payload: Msg, ctx: CommandCtx) -> list[Effect]:
        """Handle one command addressed to this shard (02 §6.3); the reply is a :class:`ReplyTo` effect."""
        tx = Tx(label=ctx.command)
        spec = self._commands().get(ctx.command)
        if spec is None:
            tx.reply(ctx.token, err_reply(codes.BAD_REQUEST, f"unknown command {ctx.command}"))
            return tx.effects()
        typ, fn = spec
        if not isinstance(payload, typ):
            tx.reply(ctx.token, err_reply(codes.BAD_REQUEST, f"{ctx.command} expects {typ.__name__}"))
            return tx.effects()
        fn(payload, ctx, tx)
        return tx.effects()

    def _commands(self) -> dict[str, tuple[type[Msg], Any]]:
        return {
            "order.submit": (OrderRequest, self._cmd_order_submit),
            "order.cancel": (CancelRequest, self._cmd_order_cancel),
            "order.cancel_all": (CancelAllRequest, self._cmd_cancel_all),
            "risk.reset": (ResetRequest, self._cmd_reset),
            "risk.halt": (HaltRequest, self._cmd_halt),
            "risk.ack_alert": (AckAlertRequest, self._cmd_ack_alert),
            "risk.liquidation.confirm": (LiquidationConfirm, self._cmd_liquidation_confirm),
            "risk.recon.resolve": (ReconResolveRequest, self._cmd_recon_resolve),
            "cash.move": (CashMoveRequest, self._cmd_cash_move),
            "firm.kill_switch": (KillSwitchRequest, self._cmd_kill_switch),
            "eod.freeze": (EodFreezeRequest, self._cmd_eod_freeze),
            "eod.rollover": (EodRolloverRequest, self._cmd_eod_rollover),
            "eod.force_unfreeze": (ForceUnfreezeRequest, self._cmd_force_unfreeze),
            "risk.query.accounts": (QueryAccounts, self._q_accounts),
            "risk.query.positions": (QueryPositions, self._q_positions),
            "risk.query.orders": (QueryOrders, self._q_orders),
            "risk.query.fills": (QueryFills, self._q_fills),
            "risk.query.cash": (QueryCash, self._q_cash),
            "risk.query.recon_breaks": (QueryReconBreaks, self._q_recon_breaks),
            "risk.query.alerts": (QueryAlerts, self._q_alerts),
            "risk.query.metrics": (QueryMetrics, self._q_metrics),
            "risk.query.rules": (QueryRules, self._q_rules),
            "risk.query.liquidation_plans": (QueryLiquidationPlans, self._q_liquidation_plans),
        }

    # ------------------------------------------------------- idempotency
    def _dedupe(self, key: str, payload: Msg, ctx: CommandCtx, tx: Tx) -> bool:
        """Duplicate command → first reply with ``duplicate=true``; same key other content → CONFLICT (02 §4.7)."""
        hit = self._processed.get(key)
        if hit is None:
            return False
        if hit[0] == request_hash(payload):
            tx.reply(ctx.token, msgspec.structs.replace(hit[1], duplicate=True))
        else:
            tx.reply(ctx.token, err_reply(codes.CONFLICT, f"idempotency key {key} reused with different content"))
        return True

    def _record(self, key: str, payload: Msg, ctx: CommandCtx, tx: Tx, reply: Reply) -> None:
        """Remember and persist the first result (``processed_commands``, same transaction)."""
        h = request_hash(payload)
        self._processed[key] = (h, reply)
        tx.insert_ignore(
            "processed_commands",
            ("command_id",),
            {
                "command_id": key,
                "command": ctx.command,
                "requester": ctx.requester,
                "request_hash": h,
                "result_code": reply.code,
                "reply": encode(reply),
                "trading_day": self._trading_day,
                "received_ns": ctx.received_ns or self._now(),
            },
        )
        tx.reply(ctx.token, reply)

    def _owned_account(self, account_id: str) -> tuple[RiskAccount | None, Reply | None]:
        ra = self._accounts.get(account_id)
        if ra is not None:
            return ra, None
        if account_id in self.accounts_cfg.accounts and account_id not in self.owned:
            return None, err_reply(codes.NOT_OWNER, f"{account_id} belongs to shard {self.accounts_cfg.shard_of(account_id)}")
        if account_id in self.owned:
            return None, err_reply(codes.UNAVAILABLE, f"{account_id} not loaded yet (no SOD)")
        return None, err_reply(codes.NOT_FOUND, f"unknown account {account_id}")

    # --------------------------------------------------------- order.submit
    def _cmd_order_submit(self, req: OrderRequest, ctx: CommandCtx, tx: Tx) -> None:
        now = self._now()
        t0 = self.clock.monotonic_ns()
        if self._lifecycle == Lifecycle.LOADING:
            # 恢复完成之前：不评估、不写库，一律 REJECT_SYSTEM_NOT_READY（与外壳一致）
            code = codes.REJECT_SYSTEM_NOT_READY
            tx.reply(ctx.token, err_reply(code, "risk engine loading",
                                          data=self._reject_ack(req, code, {"lifecycle": self._lifecycle.value})))
            return
        h = order_request_hash(req)
        existing = self._orders.get(req.client_order_id)
        if existing is None and self._orders.seen(req.client_order_id):
            # 03 §4.0：client_order_id 全局唯一。已换日清理的旧委托仍占用这个 id（orders 表中已有该行）
            code = codes.REJECT_DUPLICATE_ORDER_ID
            tx.reply(ctx.token, err_reply(code, "client_order_id already used on an earlier trading day",
                                          data=self._reject_ack(req, code, {"reason": "reused from an earlier day"})))
            return
        if existing is not None:
            if existing.request_hash == h:
                ack = existing.ack()
                if ack.accepted:
                    r = ok_reply(ack, duplicate=True)
                else:
                    r = msgspec.structs.replace(err_reply(existing.reject_code or codes.CONFLICT, data=ack), duplicate=True)
            else:
                r = err_reply(codes.REJECT_DUPLICATE_ORDER_ID, "client_order_id reused with different content",
                              data=self._reject_ack(req, codes.REJECT_DUPLICATE_ORDER_ID, None))
            tx.reply(ctx.token, r)
            return
        ra = self._accounts.get(req.account_id)
        if ra is None:
            if req.account_id in self.accounts_cfg.accounts and req.account_id not in self.owned:
                tx.reply(ctx.token, err_reply(codes.NOT_OWNER, f"{req.account_id} is not owned by shard {self.shard}"))
            elif req.account_id in self.owned:
                code = codes.REJECT_SYSTEM_NOT_READY
                tx.reply(ctx.token, err_reply(code, "SOD not loaded", data=self._reject_ack(req, code, None)))
            else:
                code = codes.REJECT_UNKNOWN_ACCOUNT
                tx.reply(ctx.token, err_reply(code, data=self._reject_ack(req, code, None)))
            return
        if self._lifecycle == Lifecycle.STOPPING:
            code = codes.REJECT_SYSTEM_NOT_READY
            tx.reply(ctx.token, err_reply(code, "risk engine stopping",
                                          data=self._reject_ack(req, code, {"lifecycle": self._lifecycle.value})))
            return
        if ra.dirty:
            self._evaluate(ra, tx, now)  # 02 §6.2：账户为 dirty 时先评估规则，再跑事前检查
        res = check_order(self, ra, req, now_ns=now, mono_ns=t0)
        us = max(0, (self.clock.monotonic_ns() - t0) // 1000)
        if res.code == codes.REJECT_UNKNOWN_INSTRUMENT or not _storable(req):
            # 合约不在主数据中，或数量 / 价格无法按 03 §4.3 落库（qty > 0 且在 INTEGER 范围内、价格为正的有限数）：
            # 只应答，不写 orders（重复提交会得到同样的拒单）
            code = res.code or codes.REJECT_INVALID_ORDER
            tx.reply(ctx.token, err_reply(code, data=self._reject_ack(req, code, res.detail, us)))
            return
        info = self._registry.get(req.instrument_id)
        rec = OrderRecord(
            client_order_id=req.client_order_id,
            account_id=req.account_id,
            instrument_id=req.instrument_id,
            exchange=info.exchange,
            direction=req.direction,
            offset=res.offset,
            order_type=req.order_type,
            tif=req.tif,
            price=req.price,
            qty=req.qty,
            trading_day=self._trading_day,
            source=req.source,
            status=OrderStatus.ROUTING if res.ok else OrderStatus.REJECTED,
            created_ns=now,
            updated_ns=now,
            request_hash=h,
            config_hash=self.rulebook.config_hash,
            strategy_tag=req.strategy_tag,
            actor=req.actor,
            risk_check_us=us,
        )
        self._orders.add(rec)
        ra.orders_today += 1
        ra.snapshot_dirty = True
        ra.dirty = True  # the order-rate part of compliance_util moved: re-evaluate (80 % warning before the reject)
        if not res.ok:
            rec.reject_code = res.code
            rec.reject_detail = res.detail
            emit_order(tx, rec, from_status=None, now_ns=now, reason=res.code)
            tx.reply(ctx.token, err_reply(res.code or codes.INTERNAL, data=rec.ack()))
            return
        rec.risk_passed_ns = now
        rec.expire_ns = now + int(self.rulebook.config.pretrade.route_ttl_ms) * NS_PER_MS
        rec.frozen_margin, rec.frozen_fee = res.frozen_margin, res.frozen_fee
        if not rec.is_open:
            pos = ra.ledger.book.find(ra.account_id, rec.instrument_id, pos_direction(rec.direction, rec.offset))
            if pos is not None:
                pos.freeze_close(rec.offset, rec.qty)  # 在途平仓占用可平量（02 §7.6 closable_qty）
                rec.close_reserved = rec.qty
        self._sync_order(ra, rec)
        emit_order(tx, rec, from_status=None, now_ns=now)
        tx.route(
            RouteOrder(
                client_order_id=rec.client_order_id,
                account_id=rec.account_id,
                instrument_id=rec.instrument_id,
                exchange=rec.exchange,
                direction=rec.direction,
                offset=rec.offset,
                order_type=rec.order_type,
                tif=rec.tif,
                price=rec.price,
                qty=rec.qty,
                trading_day=rec.trading_day,
                risk_passed_ns=rec.risk_passed_ns,
                expire_ns=rec.expire_ns,
                src_instance=self.instance,
            )
        )
        tx.reply(ctx.token, ok_reply(rec.ack()))

    def _reject_ack(self, req: OrderRequest, code: str, detail: dict[str, Any] | None, us: int = 0) -> OrderAck:
        return OrderAck(
            client_order_id=req.client_order_id,
            accepted=False,
            status=OrderStatus.REJECTED,
            reject_code=code,
            reject_detail=detail,
            risk_check_us=us,
            trading_day=self._trading_day,
        )

    def _sync_order(self, ra: RiskAccount, rec: OrderRecord) -> None:
        """Keep the ledger's registered resting orders in line with the order book (02 §8.3 冻结保证金).

        Orders hold frozen margin / fee while ROUTING, ROUTED, UNKNOWN, ACCEPTED,
        PARTIALLY_FILLED or CANCEL_PENDING (unfilled remainder) and release them
        on terminal states.
        """
        if rec.holds_frozen and rec.trading_day == ra.trading_day:
            price = rec.price if rec.price is not None else (self.mark_of(rec.instrument_id) or 0.0)
            ra.ledger.add_order(rec.client_order_id,
                                RestingOrder(rec.instrument_id, rec.direction, rec.offset, rec.remaining, price))
        else:
            ra.ledger.remove_order(rec.client_order_id)
        self._reindex(ra, rec.instrument_id)
        self._funds_changed(ra)

    def _register_orders(self, ra: RiskAccount) -> None:
        """(Re)register every working order of the account on a fresh ledger (roll / rebase / restore)."""
        for o in self._orders.resting(ra.account_id):
            self._sync_order(ra, o)
        self._funds_changed(ra)

    def _funds_changed(self, ra: RiskAccount) -> None:
        if ra.valuation is not None:
            ra.valuation = replace(ra.valuation, funds=ra.ledger.funds())
        ra.snapshot_dirty = True

    def _release_close(self, ra: RiskAccount, rec: OrderRecord) -> None:
        if rec.close_reserved <= 0:
            return
        pos = ra.ledger.book.find(ra.account_id, rec.instrument_id, pos_direction(rec.direction, rec.offset))
        if pos is not None:
            pos.unfreeze_close(rec.offset, rec.close_reserved)
        rec.close_reserved = 0

    def _terminal(self, ra: RiskAccount, rec: OrderRecord) -> None:
        """Release what a terminal order held (close reservation, frozen amounts)."""
        if rec.is_terminal:
            self._release_close(ra, rec)
        self._sync_order(ra, rec)

    # --------------------------------------------------------- order.cancel
    def _cmd_order_cancel(self, req: CancelRequest, ctx: CommandCtx, tx: Tx) -> None:
        if self._dedupe(req.command_id, req, ctx, tx):
            return
        now = self._now()
        rec = self._orders.get(req.client_order_id)
        reply: Reply
        if rec is None or rec.account_id != req.account_id:
            reply = err_reply(codes.NOT_FOUND, f"order {req.client_order_id} not found")
        elif rec.is_terminal:
            reply = err_reply(codes.CONFLICT, f"order is {rec.status.value}", data=rec.to_update())
        elif rec.status == OrderStatus.CANCEL_PENDING:
            stale = (rec.cancel_sent_ns is None or rec.cancel_error
                     or now - rec.cancel_sent_ns >= self._cancel_ttl(rec.cancel_attempts))
            if stale:
                # 撤单已过 TTL 或上次应答出错：用本命令的 command_id 重发（不算新的撤单次数）
                self._resend_cancel(rec, tx, now, reason=req.reason or R_MANUAL_CANCEL, command_id=req.command_id)
                reply = ok_reply(rec.to_update(), message="cancel re-sent")
            else:
                reply = ok_reply(rec.to_update(), message="cancel already pending")
        else:
            ra = self._accounts[rec.account_id]
            rules = self._registry.rules(rec.instrument_id)
            n = ra.cancels_by_inst.get(rec.instrument_id, 0)
            large = rules.large_cancel
            if rules.cancel_limit and n >= rules.cancel_limit:
                # 02 §11.2 第 5 步：撤单次数达到上限（适用于 order.cancel）
                reply = err_reply(codes.REJECT_CANCEL_LIMIT, f"{n} cancels today >= {rules.cancel_limit}", data=rec.to_update())
            elif large is not None and rec.qty >= large.min_qty and ra.large_cancels_by_inst.get(rec.instrument_id, 0) >= large.count:
                reply = err_reply(codes.REJECT_LARGE_CANCEL_LIMIT, f"large cancels >= {large.count}", data=rec.to_update())
            else:
                self._send_cancel(ra, rec, tx, now, command_id=req.command_id, reason=req.reason or R_MANUAL_CANCEL)
                ra.snapshot_dirty = True
                reply = ok_reply(rec.to_update())
        self._record(req.command_id, req, ctx, tx, reply)

    def _cmd_cancel_all(self, req: CancelAllRequest, ctx: CommandCtx, tx: Tx) -> None:
        if self._dedupe(req.command_id, req, ctx, tx):
            return
        now = self._now()
        ra, err = self._owned_account(req.account_id)
        if ra is None:
            self._record(req.command_id, req, ctx, tx, err or err_reply(codes.NOT_FOUND))
            return
        count = 0
        for o in self._orders.resting(ra.account_id):
            if o.status not in CANCELLABLE:
                continue
            if req.scope == "OPEN_ONLY" and not o.is_open:
                continue
            if req.product is not None and self._registry.product(o.instrument_id) != req.product:
                continue
            self._send_cancel(ra, o, tx, now, command_id=self.ids.next_id(), reason=req.reason or R_MANUAL_CANCEL)
            count += 1
        self._record(req.command_id, req, ctx, tx, ok_reply(CancelAllAck(count=count)))

    # ============================================================ execution
    def apply_exec(self, msg: Trade | BrokerOrderUpdate) -> list[Effect]:
        """Apply one ``td.exec.{account}`` message in stream order (02 §5 跨主题顺序 1, §8.3).

        The shell adds its ``fact_cursors`` row to the returned transaction (or
        writes it alone when the list has no :class:`Persist`, e.g. duplicates).
        """
        tx = Tx(label="exec")
        now = self._now()
        if isinstance(msg, Trade):
            self._apply_trade(msg, tx, now)
        else:
            self._apply_order_update(msg, tx, now)
        return tx.effects()

    def _book_trade(self, ra: RiskAccount, t: Trade, rec: OrderRecord | None) -> Any:
        """In-memory application of a fill (ledger, counters, today's list). None when the book refuses it."""
        if rec is not None and not rec.is_open and rec.close_reserved > 0:
            take = min(rec.close_reserved, t.qty)
            pos = ra.ledger.book.find(ra.account_id, rec.instrument_id, pos_direction(rec.direction, rec.offset))
            if pos is not None:
                pos.unfreeze_close(rec.offset, take)
            rec.close_reserved -= take
        try:
            lf = ra.ledger.apply_fill(t, lenient=True)  # 成交是事实：SHFE 平今/平昨不足时按实际 lot 记录
        except (PositionError, ValueError):
            return None
        ra.trades.append(t)
        ra.trade_keys.add(t.trade_key)
        self._reindex(ra, t.instrument_id)
        if t.offset == Offset.OPEN:
            ra.open_volume[t.instrument_id] = ra.open_volume.get(t.instrument_id, 0) + t.qty
        other = ra.trade_ids_by_dir.get(f"{t.exchange}|{t.trade_id}")
        if other is not None and other != t.direction.value:
            ra.self_trades += 1  # 自成交：同一账户买卖双方共用一个 trade_id（02 §7.5）
        ra.counters_changed()  # open volume, self trades, positions (限仓): the cached compliance_util is stale
        ra.trade_ids_by_dir[f"{t.exchange}|{t.trade_id}"] = t.direction.value
        return lf

    def _fill_row(self, t: Trade, lf: Any, now: int, *, link_order: bool) -> dict[str, Any]:
        return {
            "trading_day": t.trading_day,
            "exchange": t.exchange.value,
            "trade_id": t.trade_id,
            "direction": t.direction.value,
            "account_id": t.account_id,
            "instrument_id": t.instrument_id,
            "client_order_id": t.client_order_id if link_order else None,
            "origin": t.origin.value,
            "broker_order_ref": t.broker_order_ref,
            "offset": t.offset.value,
            "price": price_str(D(t.price)),
            "qty": t.qty,
            "trade_ts_ns": t.trade_ts_ns,
            "trade_seq": t.trade_seq,
            "broker_seq": t.broker_seq,
            "received_ns": t.received_ns,
            "close_today_qty": lf.close_today_qty,
            "close_yd_qty": lf.close_yd_qty,
            "fee_est": money_str(lf.fee_est),
            "applied_ns": now,
        }

    def _apply_trade(self, t: Trade, tx: Tx, now: int) -> None:
        ra = self._accounts.get(t.account_id)
        if t.trading_day != self._trading_day:
            if self._trading_day and t.trading_day > self._trading_day:
                self._future_trades.append(t)  # 下一交易日（夜盘）的成交：换日后应用
                return
            if ra is not None and t.trade_key not in ra.trade_keys and not self._trade_break_open(ra, t.trade_key):
                self._open_break(ra, ReconBreakKind.POSITION_MISMATCH, True,
                                 {"reason": "LATE_TRADE", "trade_key": t.trade_key, "trade": msgspec.to_builtins(t)}, tx, now)
            return
        if ra is None or t.trade_key in ra.trade_keys:
            return  # 未知账户或重复成交（按 trade_key 去重）
        rec = self._orders.get(t.client_order_id) if t.client_order_id else None
        if rec is not None and rec.account_id != t.account_id:
            rec = None
        lf = self._book_trade(ra, t, rec)
        if lf is None:
            # 平仓超过持仓：账本无法应用，失效即安全 → 阻塞开仓的对账异常，EOD 核对会发现差异；同一成交只记一条
            if not self._trade_break_open(ra, t.trade_key):
                self._open_break(ra, ReconBreakKind.POSITION_MISMATCH, True,
                                 {"reason": "CLOSE_EXCEEDS_BOOK", "trade_key": t.trade_key, "trade": msgspec.to_builtins(t)},
                                 tx, now)
            return
        tx.insert_ignore("fills", ("trading_day", "exchange", "trade_id", "direction"),
                         self._fill_row(t, lf, now, link_order=rec is not None))
        ra.last_fill_ns = now
        if rec is not None:
            self._order_fill(ra, rec, t, tx, now)
        else:
            self.events.emit(
                tx,
                kind=RiskEventKind.EXTERNAL_FILL,
                trading_day=self._trading_day,
                ts_ns=now,
                message=f"{t.account_id} 外部成交 {t.instrument_id} {t.direction.value} {t.offset.value} {t.qty}@{t.price}",
                severity=Severity.HIGH if t.offset == Offset.FORCE_CLOSE else Severity.WARNING,
                account_id=t.account_id,
                scope_key=acct_scope(t.account_id),
                detail={"trade_key": t.trade_key, "origin": t.origin.value, "offset": t.offset.value,
                        "client_order_id": t.client_order_id},
            )
            if t.origin == TradeOrigin.INTERNAL and t.client_order_id:
                self._request_order_rebuild(t, tx)
        if rec is not None:
            self._sync_order(ra, rec)
        else:
            self._funds_changed(ra)
        ra.touch()

    def _order_fill(self, ra: RiskAccount, rec: OrderRecord, t: Trade, tx: Tx, now: int) -> None:
        was_terminal = rec.is_terminal
        prev = rec.status
        before = rec.remaining
        rec.filled_qty += t.qty
        rec.fill_value += D(t.price) * t.qty
        rec.last_fill = FillRef(trade_id=t.trade_id, price=t.price, qty=t.qty, trade_ts_ns=t.trade_ts_ns)
        if rec.broker_order_ref is None:
            rec.broker_order_ref = t.broker_order_ref
        if before > 0 and not was_terminal:
            share = rec.remaining / before  # 按成交前的未成交量折算，避免多次部分成交重复打折
            rec.frozen_margin *= share
            rec.frozen_fee *= share
        target = status_after_fill(rec)
        if was_terminal:
            # 02 §8.3：终结状态再收到成交 → 照常应用，按累计成交量改正状态，并记不阻塞开仓的对账异常
            self._open_break(ra, ReconBreakKind.ORDER_STATE_CONFLICT, False,
                             {"client_order_id": rec.client_order_id, "status": prev.value, "trade_key": t.trade_key}, tx, now)
            if not set_status(tx, rec, target, now_ns=now, reason=R_FILL_AFTER_TERMINAL):
                emit_order(tx, rec, from_status=prev, now_ns=now, reason=R_FILL_AFTER_TERMINAL)
        elif not set_status(tx, rec, target, now_ns=now):
            emit_order(tx, rec, from_status=prev, now_ns=now)
        if rec.is_terminal:
            self._release_close(ra, rec)

    def _apply_order_update(self, u: BrokerOrderUpdate, tx: Tx, now: int) -> None:
        rec = self._orders.get(u.client_order_id) if u.client_order_id else None
        if rec is None:
            return  # 外部委托或本地没有记录的委托：成交到达时再处理（02 §8.3）
        ra = self._accounts.get(rec.account_id)
        if ra is None:
            return
        target = broker_status_target(rec, u.status, u.broker_code)
        ref_changed = bool(u.broker_order_ref) and rec.broker_order_ref != u.broker_order_ref
        if ref_changed:
            rec.broker_order_ref = u.broker_order_ref
        if target is not None and target != rec.status:
            prev = rec.status
            if target == OrderStatus.BROKER_REJECTED:
                rec.reject_code = u.broker_code or ("ROUTE_LOST" if u.status == BrokerOrderStatus.EXPIRED else "BROKER_REJECTED")
                rec.reject_detail = {"broker_msg": u.broker_msg} if u.broker_msg else None
            if prev == OrderStatus.CANCEL_PENDING and target not in TERMINAL:
                # trade-gateway 重述了柜台最后状态（broker_code 为拒绝原因，broker_seq=0）：撤单被拒，委托仍在工作
                rec.cancel_error = False
                rec.resolve_next_ns = None
                set_status(tx, rec, target, now_ns=now, reason=u.broker_code or R_CANCEL_REFUSED,
                           detail={"cancel_refused": True, "broker_code": u.broker_code, "broker_msg": u.broker_msg,
                                   "broker_status": u.status.value})
            else:
                set_status(tx, rec, target, now_ns=now, reason=u.broker_code or u.status.value)
            if target == OrderStatus.CANCEL_PENDING:
                # 柜台报告的撤单（例如从其他终端撤单）：同样计入撤单次数，并启动撤单超时
                if rec.cancel_sent_ns is None:
                    rec.cancel_sent_ns = now
                self._count_cancel(ra, rec)
            self._terminal(ra, rec)
            self._halt_guard(ra, rec, tx, now)
        elif ref_changed:
            rec.updated_ns = now
            persist_order(tx, rec)

    # ------------------------------------------------------ routing results
    def on_route_ack(self, client_order_id: str, reply: Reply) -> list[Effect]:
        """Result of ``route.order`` (RouteAck or error Reply, 02 §6.4)."""
        tx = Tx(label="route_ack")
        now = self._now()
        rec = self._orders.get(client_order_id)
        if rec is None or rec.is_terminal:
            return []
        ra = self._accounts.get(rec.account_id)
        ack = reply.open_as(RouteAck) if reply.data is not None else None
        if reply.ok and (ack is None or ack.status != BrokerOrderStatus.EXPIRED):
            if ack is not None and ack.broker_order_ref:
                rec.broker_order_ref = ack.broker_order_ref
            if rec.status in (OrderStatus.ROUTING, OrderStatus.UNKNOWN):
                set_status(tx, rec, OrderStatus.ROUTED, now_ns=now)
                self._halt_guard(ra, rec, tx, now)
            return tx.effects()
        if reply.code in codes.RETRYABLE:
            return []  # 传输层失败：外壳在 expire_ns 之前重试；过期后由定时器转 UNKNOWN
        code = reply.code if reply.code != codes.OK else codes.ROUTE_EXPIRED
        if rec.status == OrderStatus.CANCEL_PENDING:
            set_status(tx, rec, OrderStatus.CANCELED, now_ns=now, reason=code)
        else:
            rec.reject_code = code
            set_status(tx, rec, OrderStatus.BROKER_REJECTED, now_ns=now, reason=code)
        if ra is not None:
            self._terminal(ra, rec)
        return tx.effects()

    def on_route_timeout(self, client_order_id: str) -> list[Effect]:
        """The shell gave up on ``route.order``: past ``expire_ns`` the order becomes UNKNOWN at once."""
        tx = Tx(label="route_timeout")
        now = self._now()
        rec = self._orders.get(client_order_id)
        if rec is None or rec.status != OrderStatus.ROUTING or now < rec.expire_ns:
            return []
        set_status(tx, rec, OrderStatus.UNKNOWN, now_ns=now, reason=R_ROUTE_TIMEOUT)
        self._send_resolve(rec, tx, now)
        return tx.effects()

    @staticmethod
    def _route_ack(reply: Reply) -> RouteAck | None:
        if reply.data is None:
            return None
        try:
            ack: RouteAck = reply.open_as(RouteAck)
        except (msgspec.ValidationError, msgspec.DecodeError, TypeError, ValueError):
            return None
        return ack

    def on_cancel_result(self, client_order_id: str, reply: Reply) -> list[Effect]:
        """Result of ``route.cancel`` (02 §6.4).

        The trade-gateway accepts a cancel durably (``RouteAck(CANCEL_PENDING)``)
        and reports the outcome on ``td.exec``, so an OK reply changes nothing.
        A tombstone (``EXPIRED`` / ``ROUTE_LOST``: never received) → CANCELED; a
        route rejected at the gateway (``REJECTED``) → BROKER_REJECTED. Transport
        failures are retried by the shell with the same command id; any other
        error keeps CANCEL_PENDING and makes the next timer check the order at
        once (``route.resolve``, then a re-send with a new command id).
        """
        tx = Tx(label="cancel_result")
        now = self._now()
        rec = self._orders.get(client_order_id)
        if rec is None or rec.status != OrderStatus.CANCEL_PENDING:
            return []
        if reply.code in codes.RETRYABLE:
            return []  # 传输失败：外壳用同一个 command_id 重试
        ra = self._accounts.get(rec.account_id)
        ack = self._route_ack(reply)
        st = ack.status if ack is not None else None
        if st == BrokerOrderStatus.EXPIRED or reply.code == codes.ROUTE_LOST:
            code = reply.code if reply.code and reply.code != codes.OK else codes.ROUTE_LOST
            set_status(tx, rec, OrderStatus.CANCELED, now_ns=now, reason=code)  # 从没到过柜台（墓碑）
        elif st == BrokerOrderStatus.REJECTED:
            rec.reject_code = reply.code if not reply.ok else "BROKER_REJECTED"
            set_status(tx, rec, OrderStatus.BROKER_REJECTED, now_ns=now, reason=rec.reject_code)
        elif reply.ok:
            rec.cancel_error = False
            return []  # 撤单已受理（持久化在 trade-gateway）：结果以 td.exec 为准，超时后 route.resolve 兜底
        else:
            rec.cancel_error = True
            return []
        if ra is not None:
            self._terminal(ra, rec)
        return tx.effects()

    def on_resolve_result(self, client_order_id: str, reply: Reply) -> list[Effect]:
        """Result of ``route.resolve`` for an UNKNOWN or CANCEL_PENDING order (02 §8.3).

        Tombstone (``EXPIRED`` / ``ROUTE_LOST``) → BROKER_REJECTED, or CANCELED
        when a cancel was pending; ``CANCELED`` → CANCELED (FILLED when fully
        filled); ``REJECTED`` → BROKER_REJECTED. A working order: UNKNOWN →
        ROUTED / ACCEPTED / PARTIALLY_FILLED (halt guard applies); a pending
        cancel is re-sent with a new command id (``CANCEL_STUCK`` break after
        5 sends). ``FILLED`` waits for the trades of ``td.exec``. Failures and
        ``UNKNOWN`` ask again after 5 s. For an order already expired at the
        roll (SESSION_CLOSE) a broker that still reports it working opens a
        non-blocking ORDER_STATE_CONFLICT break.
        """
        tx = Tx(label="resolve_result")
        now = self._now()
        rec = self._orders.get(client_order_id)
        if rec is None:
            return []
        rec.resolve_inflight = False
        rec.resolve_sent_ns = None
        ra = self._accounts.get(rec.account_id)
        ack = self._route_ack(reply)
        st = ack.status if ack is not None else None
        if rec.is_terminal:
            if rec.session_expired:
                rec.session_expired = False
                if ra is not None and st in WORKING_BROKER_STATES | {BrokerOrderStatus.CANCEL_PENDING}:
                    self._open_break(ra, ReconBreakKind.ORDER_STATE_CONFLICT, False,
                                     {"reason": "SESSION_CLOSE_STILL_WORKING", "client_order_id": rec.client_order_id,
                                      "trading_day": rec.trading_day, "broker_status": st.value if st else None}, tx, now)
            return tx.effects()
        lost = st == BrokerOrderStatus.EXPIRED or reply.code == codes.ROUTE_LOST
        if lost:
            # 从没收到过（或到达时已过期）：trade-gateway 写墓碑并返回 EXPIRED
            code = reply.code if reply.code and reply.code != codes.OK else codes.ROUTE_LOST
            if rec.status == OrderStatus.CANCEL_PENDING:
                set_status(tx, rec, OrderStatus.CANCELED, now_ns=now, reason=code)
            else:
                rec.reject_code = code
                set_status(tx, rec, OrderStatus.BROKER_REJECTED, now_ns=now, reason=code)
        elif st == BrokerOrderStatus.REJECTED:
            rec.reject_code = reply.code if not reply.ok else "BROKER_REJECTED"
            set_status(tx, rec, OrderStatus.BROKER_REJECTED, now_ns=now, reason="RESOLVED_REJECTED")
        elif reply.ok and ack is not None and st is not None and st != BrokerOrderStatus.UNKNOWN:
            if ack.broker_order_ref:
                rec.broker_order_ref = ack.broker_order_ref
            rec.resolve_next_ns = None
            if st == BrokerOrderStatus.CANCELED:
                done = OrderStatus.FILLED if rec.filled_qty >= rec.qty else OrderStatus.CANCELED
                set_status(tx, rec, done, now_ns=now, reason="RESOLVED_CANCELED")
            elif st == BrokerOrderStatus.FILLED:
                if rec.filled_qty >= rec.qty:
                    set_status(tx, rec, OrderStatus.FILLED, now_ns=now, reason="RESOLVED_FILLED")
                else:
                    # 柜台已全部成交：成交以 td.exec 为准，稍后再确认
                    if rec.status == OrderStatus.UNKNOWN:
                        set_status(tx, rec, OrderStatus.PARTIALLY_FILLED if rec.filled_qty else OrderStatus.ACCEPTED,
                                   now_ns=now, reason="RESOLVED")
                    else:
                        rec.resolve_next_ns = now + RESOLVE_RETRY_NS
            elif rec.status == OrderStatus.CANCEL_PENDING:
                due = rec.cancel_sent_ns is None or now >= rec.cancel_sent_ns + self._cancel_ttl(rec.cancel_attempts)
                if rec.cancel_error or due:
                    self._cancel_retry(ra, rec, tx, now)  # 超时仍在工作：换新的 command_id 重发撤单
                # 否则：这次确认发出之后才发出的撤单还在 TTL 之内，由它自己的超时检查跟进
            elif rec.status == OrderStatus.UNKNOWN:
                if st == BrokerOrderStatus.CANCEL_PENDING:
                    # 柜台侧已有撤单在途（例如重启前发出的撤单）：恢复撤单状态，由撤单超时继续跟进
                    rec.status_before_cancel = OrderStatus.UNKNOWN
                    if rec.cancel_sent_ns is None:
                        rec.cancel_sent_ns = now
                    if set_status(tx, rec, OrderStatus.CANCEL_PENDING, now_ns=now, reason="RESOLVED") and ra is not None:
                        self._count_cancel(ra, rec)
                else:
                    if st in (BrokerOrderStatus.RECEIVED, BrokerOrderStatus.SUBMITTED) and not rec.filled_qty:
                        target = OrderStatus.ROUTED
                    else:
                        target = OrderStatus.PARTIALLY_FILLED if rec.filled_qty else OrderStatus.ACCEPTED
                    set_status(tx, rec, target, now_ns=now, reason="RESOLVED")
                    self._halt_guard(ra, rec, tx, now)
        else:
            rec.resolve_next_ns = now + RESOLVE_RETRY_NS  # 失败或仍未知：稍后重试
        if ra is not None and rec.is_terminal:
            self._terminal(ra, rec)
        return tx.effects()

    # ------------------------------------------------------- sub-requests
    def _new_request(self, tx: Tx, kind: str, target: str, command: str, payload: Msg, *,
                     timeout_ms: int = 2000, reply_token: Any = None, **data: Any) -> str:
        token = f"{kind}:{self.ids.next_id()}"
        p = _Pending(kind=kind, payload=payload, data=dict(data))
        if reply_token is not None:
            p.tokens.append(reply_token)
        self._pending[token] = p
        tx.request(token, target, command, payload, timeout_ms)
        return token

    def pending_requests(self) -> dict[str, str]:
        """Outstanding sub-requests: ``{token: kind}``."""
        return {k: v.kind for k, v in self._pending.items()}

    def on_request_result(self, token: str, reply: Reply) -> list[Effect]:
        """Result (or TIMEOUT / UNAVAILABLE error) of a :class:`Request` effect."""
        p = self._pending.pop(token, None)
        if p is None:
            return []
        tx = Tx(label=f"result:{p.kind}")
        now = self._now()
        if p.kind == "rollover":
            self._on_rollover_result(p, reply, tx, now)
        elif p.kind == "provisional":
            self._on_provisional_inputs(p, reply, tx, now)
        elif p.kind == "recon_positions":
            if reply.ok and reply.data is not None:
                self._on_positions(reply.open_as(BrokerPositionBatch), tx, now)
        elif p.kind == "recon_trades":
            self._on_trade_batch(p, reply, tx, now)
        elif p.kind == "order_rebuild":
            self._on_order_rebuild(p, reply, tx, now)
        return tx.effects()

    def _request_order_rebuild(self, t: Trade, tx: Tx) -> None:
        self._new_request(
            tx, "order_rebuild", TRADE_GATEWAY, "td.query.orders",
            OrderQuery(trading_day=t.trading_day, account_id=t.account_id, client_order_ids=[t.client_order_id or ""]),
            trade=t,
        )

    def _on_order_rebuild(self, p: _Pending, reply: Reply, tx: Tx, now: int) -> None:
        """02 §8.3 本地没有记录的委托：按 td.query.orders 补建委托记录."""
        t: Trade = p.data["trade"]
        if not reply.ok or reply.data is None or t.client_order_id is None or t.client_order_id in self._orders:
            return
        batch = reply.open_as(BrokerOrderBatch)
        ra = self._accounts.get(t.account_id)
        if ra is None:
            return
        for u in batch.items:
            if u.client_order_id != t.client_order_id:
                continue
            info = self._registry.get(t.instrument_id)
            fills = [x for x in ra.trades if x.client_order_id == t.client_order_id]
            rec = OrderRecord(
                client_order_id=t.client_order_id,
                account_id=t.account_id,
                instrument_id=t.instrument_id,
                exchange=info.exchange,
                direction=t.direction,
                offset=t.offset,
                order_type=OrderType.LIMIT,
                tif=TimeInForce.GFD,
                price=t.price,
                qty=max(u.filled_qty + u.remaining_qty, sum(x.qty for x in fills)),
                trading_day=t.trading_day,
                source=OrderSource.API,
                status=OrderStatus.ACCEPTED,
                created_ns=now,
                updated_ns=now,
                request_hash="",
                config_hash=self.rulebook.config_hash,
                filled_qty=sum(x.qty for x in fills),
                fill_value=sum((D(x.price) * x.qty for x in fills), Decimal(0)),
                broker_order_ref=u.broker_order_ref,
            )
            rec.status = OrderStatus.ROUTED  # 柜台已受理；再按柜台状态与累计成交量确定当前状态
            target = broker_status_target(rec, u.status)
            if rec.filled_qty >= rec.qty:
                rec.status = OrderStatus.FILLED
            elif target is not None:
                rec.status = target
            else:
                rec.status = OrderStatus.PARTIALLY_FILLED if rec.filled_qty else OrderStatus.ACCEPTED
            self._orders.add(rec)
            emit_order(tx, rec, from_status=None, now_ns=now, reason="REBUILT_FROM_BROKER",
                       detail={"broker_status": u.status.value, "broker_order_ref": u.broker_order_ref})
            # 02 §8.3：补建委托记录并写审计（对账处理的一种，沿用 RECON_RESOLVE 审计动作）
            audit(tx, ts_ns=now, trading_day=self._trading_day, actor="system", action=AUDIT_RECON_RESOLVE,
                  target=f"order:{rec.client_order_id}",
                  after={"kind": "ORDER_REBUILD", "account_id": rec.account_id, "instrument_id": rec.instrument_id,
                         "status": rec.status.value, "qty": rec.qty, "filled_qty": rec.filled_qty,
                         "broker_status": u.status.value, "trade_key": t.trade_key},
                  reason="order rebuilt from td.query.orders (fill for an order unknown locally)",
                  corr_id=rec.client_order_id)
            tx.mark_critical()
            self._sync_order(ra, rec)
            self._halt_guard(ra, rec, tx, now)
            return

    # --------------------------------------------------- position recon (02 §8.7)
    def request_position_recon(self) -> list[Effect]:
        """Ask the trade-gateway for broker positions now (warm-up recon, 02 §8.6)."""
        tx = Tx(label="recon")
        self._request_positions(tx, self._now())
        return tx.effects()

    def _request_positions(self, tx: Tx, now: int) -> None:
        if any(p.kind == "recon_positions" for p in self._pending.values()):
            return
        self._new_request(tx, "recon_positions", TRADE_GATEWAY, "td.query.positions",
                          PositionQuery(trading_day=self._trading_day))

    def _on_positions(self, batch: BrokerPositionBatch, tx: Tx, now: int) -> None:
        quiet = self.rulebook.config.system.recon_quiet_s * NS_PER_S
        broker: dict[str, dict[tuple[str, PosDirection], tuple[int, int]]] = {}
        for it in batch.items:
            broker.setdefault(it.account_id, {})[(it.instrument_id, it.direction)] = (it.td_qty, it.yd_qty)
        for a in sorted(self._accounts):
            ra = self._accounts[a]
            ours = {(p.instrument_id, p.direction): (p.td_qty, p.yd_qty) for p in ra.ledger.book.positions(a) if p.qty > 0}
            theirs = broker.get(a, {})
            busy = {o.instrument_id for o in self._orders.resting(a)}
            recent = ra.last_fill_ns and now - ra.last_fill_ns < quiet
            diffs: list[dict[str, Any]] = []
            for key in sorted(set(ours) | set(theirs), key=lambda k: (k[0], k[1].value)):
                if key[0] in busy or recent:
                    continue  # 有在途委托或最近 5s 内有成交：暂不比较
                o, b = ours.get(key, (0, 0)), theirs.get(key, (0, 0))
                if o != b:
                    diffs.append({"instrument_id": key[0], "direction": key[1].value, "ours": list(o), "broker": list(b)})
            open_mismatch = [x for x in self.breaks.values() if x.account_id == a and x.status == ReconBreakStatus.OPEN
                             and x.kind == ReconBreakKind.POSITION_MISMATCH]
            if diffs:
                ra.recon_mismatch_streak += 1
                if ra.recon_mismatch_streak >= 2 and not open_mismatch:
                    # 连续两次不一致 → recon_breaks + BLOCK_OPEN（SYSTEM:RECON_BREAK）+ 自动补拉成交
                    self._open_break(ra, ReconBreakKind.POSITION_MISMATCH, True, {"diffs": diffs}, tx, now)
                    self._new_request(tx, "recon_trades", TRADE_GATEWAY, "td.query.trades",
                                      TradeQuery(trading_day=self._trading_day, account_id=a, limit=1000), account_id=a)
            elif not recent and not busy:
                ra.recon_mismatch_streak = 0
                for brk in open_mismatch:
                    self._resolve_break(brk, ReconResolution.RESYNCED, "auto: positions match after resync", "system", tx, now)
                self._unblock_recon(ra, tx, now)

    def _on_trade_batch(self, p: _Pending, reply: Reply, tx: Tx, now: int) -> None:
        if not reply.ok or reply.data is None:
            return
        batch = reply.open_as(TradeBatch)
        for t in sorted(batch.trades, key=lambda x: x.trade_seq):
            self._apply_trade(t, tx, now)
        if not batch.complete:
            self._new_request(tx, "recon_trades", TRADE_GATEWAY, "td.query.trades",
                              TradeQuery(trading_day=self._trading_day, account_id=p.data.get("account_id"),
                                         from_trade_seq=batch.next_trade_seq, limit=1000), **p.data)

    def _break_row(self, b: ReconBreakView) -> dict[str, Any]:
        return {
            "break_id": b.break_id,
            "trading_day": b.trading_day,
            "account_id": b.account_id,
            "kind": b.kind.value,
            "status": b.status.value,
            "blocking": b.blocking,
            "resolution": b.resolution.value if b.resolution is not None else None,
            "note": b.note,
            "detail": dict(b.detail),
            "created_ns": b.created_ns,
            "resolved_ns": b.resolved_ns,
            "resolved_by": b.resolved_by,
        }

    def _trade_break_open(self, ra: RiskAccount, trade_key: str) -> bool:
        """An OPEN break already records this trade (re-deliveries of an unbookable / late trade)."""
        return any(b.account_id == ra.account_id and b.status == ReconBreakStatus.OPEN
                   and b.detail.get("trade_key") == trade_key for b in self.breaks.values())

    def _open_break(self, ra: RiskAccount, kind: ReconBreakKind, blocking: bool, detail: dict[str, Any], tx: Tx, now: int,
                    *, severity: Severity | None = None) -> ReconBreakView:
        brk = ReconBreakView(
            break_id=self.ids.next_id(),
            trading_day=self._trading_day,
            account_id=ra.account_id,
            kind=kind,
            status=ReconBreakStatus.OPEN,
            blocking=blocking,
            detail=detail,
            created_ns=now,
        )
        self.breaks[brk.break_id] = brk
        tx.upsert("recon_breaks", ("break_id",), self._break_row(brk))
        self.events.emit(
            tx,
            kind=RiskEventKind.RECON_BREAK,
            trading_day=self._trading_day,
            ts_ns=now,
            message=f"{ra.account_id} 对账异常 {kind.value}{'，禁开' if blocking else '（不阻塞开仓）'}",
            severity=severity if severity is not None else (Severity.HIGH if blocking else Severity.WARNING),
            account_id=ra.account_id,
            scope_key=acct_scope(ra.account_id),
            detail={"break_id": brk.break_id, "blocking": blocking, "kind": kind.value, **detail},
        )
        tx.mark_critical()
        if blocking:
            ra.system.setdefault(SYSTEM_RECON_BREAK, system_restriction(ra.account_id, SYSTEM_RECON_BREAK, now))
            self._refresh_state(ra, tx, now, cause="SYSTEM")
        return brk

    def _resolve_break(self, brk: ReconBreakView, resolution: ReconResolution, note: str, actor: str, tx: Tx, now: int) -> ReconBreakView:
        new = msgspec.structs.replace(brk, status=ReconBreakStatus.RESOLVED, resolution=resolution, note=note,
                                      resolved_ns=now, resolved_by=actor)
        self.breaks[brk.break_id] = new
        tx.upsert("recon_breaks", ("break_id",), self._break_row(new))
        tx.mark_critical()
        return new

    def _unblock_recon(self, ra: RiskAccount, tx: Tx, now: int) -> None:
        still = any(b.account_id == ra.account_id and b.status == ReconBreakStatus.OPEN and b.blocking
                    for b in self.breaks.values())
        if not still and SYSTEM_RECON_BREAK in ra.system:
            del ra.system[SYSTEM_RECON_BREAK]
            self._refresh_state(ra, tx, now, cause="SYSTEM")

    # ============================================================ risk commands
    def _role_error(self, role: Role, allowed: frozenset[Role]) -> Reply | None:
        if role not in allowed:
            return err_reply(codes.FORBIDDEN, f"role {role!s} not allowed")  # 02 §4.11：下游再校验一次 role
        return None

    def _cmd_reset(self, req: ResetRequest, ctx: CommandCtx, tx: Tx) -> None:
        """``risk.reset``: latched levels → REARMED with rearm line (01 §8.14), manual halt released."""
        if self._dedupe(req.command_id, req, ctx, tx):
            return
        err = self._role_error(req.role, RISK_MANAGER_ROLES)
        if err is None and not req.reason.strip():
            err = err_reply(codes.BAD_REQUEST, "reason must not be empty")
        ra, err2 = self._owned_account(req.account_id)
        if err is not None or ra is None:
            self._record(req.command_id, req, ctx, tx, err or err2 or err_reply(codes.NOT_FOUND))
            return
        now = self._now()
        td = self._trading_day
        prev = ra.state
        rearm: float | None = None
        n = 0
        self._rules_touched(ra)
        orphans: list[tuple[str, str, str]] = []
        for m in sorted(ra.machines.values(), key=lambda x: (-x.level.index, x.key)):
            if req.rule_id is not None and m.rule.rule_id != req.rule_id:
                continue
            if req.level is not None and m.level.name != req.level:
                continue
            if m.state != RuleSmState.ACTIVE or not m.st.latched:
                continue
            key = m.alert_key(td)
            if m.rule.rule_id == MANUAL_RULE_ID:
                ep = m.st.episode
                m.st = type(m.st)()
                m.st.episode = ep
            elif self._is_orphan(m, ra.account_id):
                # 规则已被热加载删除、只因锁存而保留的实例：重置后直接回到 OK 并移除
                m.reset(now)
                m.st.state, m.st.rearm_threshold, m.st.latched = RuleSmState.OK, None, False
                orphans.append(m.key)
            else:
                m.reset(now)
                if rearm is None:
                    rearm = m.st.rearm_threshold
            rec = self.alerts.get(key)
            if rec is not None:
                rec.status = ALERT_RESET
                rec.reset_ns = now
                rec.reset_by = req.actor
                rec.persist_status(tx)
            tx.upsert("rule_states", ("trading_day", "rule_id", "level", "scope_key"), m.to_row(td, self.rulebook.config_hash, now))
            self.events.emit(
                tx,
                kind=RiskEventKind.RESET,
                trading_day=td,
                ts_ns=now,
                message=f"{ra.account_id} {m.rule.rule_id}/{m.level.name} 已人工重置：{req.reason}",
                severity=Severity.HIGH,
                account_id=ra.account_id,
                scope_key=m.scope_key,
                alert_key=key,
                rule_id=m.rule.rule_id,
                level=m.level.name,
                episode=m.st.episode,
                action=m.level.action,
                actor=req.actor,
                detail={"rearm_threshold": m.st.rearm_threshold, "reason": req.reason},
            )
            n += 1
        for k in orphans:
            ra.machines.pop(k, None)
        self._refresh_state(ra, tx, now, cause="RESET", actor=req.actor, reason=req.reason)
        audit(tx, ts_ns=now, trading_day=td, actor=req.actor, role=req.role, action=AUDIT_RISK_RESET,
              target=acct_scope(ra.account_id), before={"state": prev.value}, after={"state": ra.state.value, "reset": n},
              reason=req.reason, command_id=req.command_id)
        tx.mark_critical()
        reply = ok_reply(ResetAck(account_state=ra.state, rearm_threshold=rearm), message=f"{n} latched level(s) reset")
        self._record(req.command_id, req, ctx, tx, reply)

    def _cmd_halt(self, req: HaltRequest, ctx: CommandCtx, tx: Tx) -> None:
        """``risk.halt``: manual circuit break, latched at once (02 §6.3)."""
        if self._dedupe(req.command_id, req, ctx, tx):
            return
        err = self._role_error(req.role, RISK_MANAGER_ROLES)
        ra, err2 = self._owned_account(req.account_id)
        if err is not None or ra is None:
            self._record(req.command_id, req, ctx, tx, err or err2 or err_reply(codes.NOT_FOUND))
            return
        now = self._now()
        td = self._trading_day
        prev = ra.state
        scope = acct_scope(ra.account_id)
        mk = (MANUAL_RULE_ID, MANUAL_LEVEL, scope)
        self._rules_touched(ra)
        m = ra.machines.get(mk)
        if m is None:
            m = LevelMachine(_MANUAL_RULE, _MANUAL_RULE.base_levels[0], scope)
            ra.machines[mk] = m
        key: str | None = m.alert_key(td) if m.st.episode else None
        if not (m.state == RuleSmState.ACTIVE and m.st.latched):
            m.st.state = RuleSmState.ACTIVE
            m.st.episode += 1
            m.st.latched = True
            m.st.active_since_ns = now
            m.st.last_value = 0.0
            key = m.alert_key(td)
            rec = AlertRecord(
                alert_key=key, trading_day=td, rule_id=MANUAL_RULE_ID, level=MANUAL_LEVEL, scope_key=scope,
                episode=m.st.episode, account_id=ra.account_id, action=Action.CIRCUIT_BREAK, severity=Severity.CRITICAL,
                metric="manual", value=0.0, threshold=0.0, latched=True, status=ALERT_ACTIVE, raised_ns=now,
                message=f"{ra.account_id} 人工熔断：{req.reason}", detail={"actor": req.actor, "reason": req.reason},
            )
            self.alerts[key] = rec
            rec.persist_new(tx)
            tx.upsert("rule_states", ("trading_day", "rule_id", "level", "scope_key"), m.to_row(td, self.rulebook.config_hash, now))
            self.events.emit(
                tx, kind=RiskEventKind.MANUAL_HALT, trading_day=td, ts_ns=now, message=rec.message,
                severity=Severity.CRITICAL, account_id=ra.account_id, scope_key=scope, alert_key=key,
                rule_id=MANUAL_RULE_ID, level=MANUAL_LEVEL, episode=m.st.episode, action=Action.CIRCUIT_BREAK,
                latched=True, actor=req.actor,
            )
        msg = self._refresh_state(ra, tx, now, cause="MANUAL", cause_alert_key=key, actor=req.actor, reason=req.reason)
        audit(tx, ts_ns=now, trading_day=td, actor=req.actor, role=req.role, action=AUDIT_RISK_HALT,
              target=scope, before={"state": prev.value}, after={"state": ra.state.value}, reason=req.reason,
              command_id=req.command_id, corr_id=key)
        tx.mark_critical()
        reply = ok_reply(msg or self._current_state_message(ra, prev, now, cause="MANUAL"))
        self._record(req.command_id, req, ctx, tx, reply)

    def _cmd_ack_alert(self, req: AckAlertRequest, ctx: CommandCtx, tx: Tx) -> None:
        """``risk.ack_alert``: acknowledgement flag only, the state machine is untouched (02 §6.3)."""
        if self._dedupe(req.command_id, req, ctx, tx):
            return
        err = self._role_error(req.role, RISK_MANAGER_ROLES)
        rec = self.alerts.get(req.alert_key)
        if err is None and rec is None:
            err = err_reply(codes.NOT_FOUND, f"alert {req.alert_key} not found")
        if err is not None or rec is None:
            self._record(req.command_id, req, ctx, tx, err or err_reply(codes.NOT_FOUND))
            return
        now = self._now()
        if rec.acked_ns is None:
            rec.acked_ns = now
            rec.acked_by = req.actor
            rec.persist_status(tx)
            self.events.emit(
                tx, kind=RiskEventKind.ALERT_ACKED, trading_day=rec.trading_day, ts_ns=now,
                message=f"{rec.alert_key} 已确认", severity=Severity.INFO, account_id=rec.account_id,
                scope_key=rec.scope_key, alert_key=rec.alert_key, rule_id=rec.rule_id, level=rec.level,
                episode=rec.episode, action=rec.action, actor=req.actor, detail={"note": req.note or ""},
            )
            audit(tx, ts_ns=now, trading_day=self._trading_day, actor=req.actor, role=req.role, action=AUDIT_ALERT_ACK,
                  target=rec.scope_key, reason=req.note, command_id=req.command_id, corr_id=rec.alert_key)
            tx.mark_critical()  # 03 DB-03：人工命令逐次落盘
        self._record(req.command_id, req, ctx, tx, ok_reply())

    def _cmd_liquidation_confirm(self, req: LiquidationConfirm, ctx: CommandCtx, tx: Tx) -> None:
        """Record the decision on a plan; never places orders (01 TD-08)."""
        if self._dedupe(req.command_id, req, ctx, tx):
            return
        err = self._role_error(req.role, RISK_MANAGER_ROLES)
        plan = self.plans.get(req.plan_id)
        if err is None and plan is None:
            err = err_reply(codes.NOT_FOUND, f"plan {req.plan_id} not found")
        if err is None and req.decision not in ("CONFIRM", "CANCEL"):
            err = err_reply(codes.BAD_REQUEST, "decision must be CONFIRM or CANCEL")
        if err is None and plan is not None and plan.status != LiquidationStatus.PENDING_CONFIRM:
            err = err_reply(codes.CONFLICT, f"plan is {plan.status.value}", data=plan)
        if err is not None or plan is None:
            self._record(req.command_id, req, ctx, tx, err or err_reply(codes.NOT_FOUND))
            return
        now = self._now()
        status = LiquidationStatus.CONFIRMED if req.decision == "CONFIRM" else LiquidationStatus.CANCELED
        new = msgspec.structs.replace(plan, status=status, confirmed_by=req.actor, confirmed_ns=now)
        self.plans[plan.plan_id] = new
        tx.upsert("liquidation_plans", ("plan_id",), self._plan_row(new))
        self._plan_event(tx, new, now, actor=req.actor)
        audit(tx, ts_ns=now, trading_day=self._trading_day, actor=req.actor, role=req.role,
              action=AUDIT_LIQUIDATION_CONFIRM if status == LiquidationStatus.CONFIRMED else AUDIT_LIQUIDATION_CANCEL,
              target=acct_scope(plan.account_id), before={"status": plan.status.value}, after={"status": status.value},
              command_id=req.command_id, corr_id=plan.plan_id)
        tx.mark_critical()
        self._record(req.command_id, req, ctx, tx, ok_reply(new))

    def _cmd_recon_resolve(self, req: ReconResolveRequest, ctx: CommandCtx, tx: Tx) -> None:
        if self._dedupe(req.command_id, req, ctx, tx):
            return
        err = self._role_error(req.role, RISK_MANAGER_ROLES)
        brk = self.breaks.get(req.break_id)
        if err is None and brk is None:
            err = err_reply(codes.NOT_FOUND, f"recon break {req.break_id} not found")
        if err is None and brk is not None and brk.status == ReconBreakStatus.RESOLVED:
            err = err_reply(codes.CONFLICT, "already resolved")
        if err is not None or brk is None:
            self._record(req.command_id, req, ctx, tx, err or err_reply(codes.NOT_FOUND))
            return
        now = self._now()
        self._resolve_break(brk, req.resolution, req.note, req.actor, tx, now)
        audit(tx, ts_ns=now, trading_day=self._trading_day, actor=req.actor, role=req.role, action=AUDIT_RECON_RESOLVE,
              target=acct_scope(brk.account_id), before={"status": brk.status.value},
              after={"status": ReconBreakStatus.RESOLVED.value, "resolution": req.resolution.value}, reason=req.note,
              command_id=req.command_id, corr_id=brk.break_id)
        ra = self._accounts.get(brk.account_id)
        if ra is not None:
            self._unblock_recon(ra, tx, now)
        self._record(req.command_id, req, ctx, tx, ok_reply())

    def _cmd_cash_move(self, req: CashMoveRequest, ctx: CommandCtx, tx: Tx) -> None:
        """``cash.move`` with trading-day attribution by the EOD state (02 §6.3, 01 §8.15)."""
        if self._dedupe(req.movement_id, req, ctx, tx):
            return
        err = self._role_error(req.role, frozenset({Role.ADMIN}))
        amount = _qmoney(req.amount)
        if err is None and amount is None:
            err = err_reply(codes.BAD_REQUEST, "amount must be a positive DecStr with at most two decimals")
        ra, err2 = self._owned_account(req.account_id)
        if err is None and ra is None:
            err = err2
        now = self._now()
        td = self._trading_day
        cash_td = td
        if err is None:
            try:
                cash_td = cash_trading_day(self._mode, td, self._next_trading_day())
            except ValueError:
                err = err_reply(codes.PRECONDITION_FAILED, "next trading day unknown")
        if err is None and ra is not None and amount is not None and req.type == CashType.WITHDRAW:
            # 冻结后记入下一交易日的出金同样按当前可用资金校验，并扣除已登记、尚未入账的出金
            pending = sum((D(c.amount) for c in self._next_day_cash
                           if c.account_id == ra.account_id and c.type == CashType.WITHDRAW), Decimal(0))
            available = ra.ledger.funds().available - float(pending)
            if float(amount) > available:
                err = err_reply(codes.PRECONDITION_FAILED,
                                f"withdrawal exceeds available funds ({available:.2f} after pending withdrawals {pending})")
        if err is not None or ra is None or amount is None:
            self._record(req.movement_id, req, ctx, tx, err or err_reply(codes.BAD_REQUEST))
            return
        cm = CashMovement(movement_id=req.movement_id, account_id=ra.account_id, trading_day=cash_td, type=req.type,
                          amount=money_str(amount), actor=req.actor, reason=req.reason, ts_ns=now)
        if cash_td == td:
            ra.ledger.apply_cash(cm)
            ra.cash.append(cm)
            ra.movement_ids.add(cm.movement_id)
            ra.touch()
        else:
            self._next_day_cash.append(cm)  # 冻结之后、换日之前：记入下一交易日
        tx.insert_ignore("cash_movements", ("movement_id",), {
            "movement_id": cm.movement_id, "account_id": cm.account_id, "trading_day": cm.trading_day,
            "type": cm.type.value, "amount": cm.amount, "actor": cm.actor, "reason": cm.reason, "ts_ns": now,
        })
        tx.emit(topics.risk_cash(ra.account_id), cm, ts_ns=now, corr_id=cm.movement_id, td=cash_td)
        audit(tx, ts_ns=now, trading_day=td, actor=req.actor, role=req.role, action=AUDIT_CASH_MOVE,
              target=acct_scope(ra.account_id), after=msgspec.to_builtins(cm), reason=req.reason, command_id=req.movement_id)
        tx.mark_critical()
        self._record(req.movement_id, req, ctx, tx, ok_reply(cm))

    # ================================================================== firm
    def _cmd_kill_switch(self, req: KillSwitchRequest, ctx: CommandCtx, tx: Tx) -> None:
        """Manual kill switch: stored per origin, effective = manual OR rule (02 §7.6 FirmControl)."""
        if self._dedupe(req.command_id, req, ctx, tx):
            return
        err = self._role_error(req.role, RISK_MANAGER_ROLES)
        if err is not None:
            self._record(req.command_id, req, ctx, tx, err)
            return
        now = self._now()
        before = self.firm.kill_switch
        control = FirmControl(
            action=FirmAction.KILL_SWITCH_ON if req.on else FirmAction.KILL_SWITCH_OFF,
            origin=ControlOrigin.MANUAL,
            active=req.on,
            restrictions=[],  # a manual kill switch carries no restrictions
            trading_day=self._trading_day,
            actor=req.actor,
            reason=req.reason,
            ts_ns=now,
        )
        self._apply_control(control, tx, now, src_instance=API_GATEWAY)
        audit(tx, ts_ns=now, trading_day=self._trading_day, actor=req.actor, role=req.role,
              action=AUDIT_KILL_SWITCH_ON if req.on else AUDIT_KILL_SWITCH_OFF, target="firm",
              before={"effective": before}, after={"effective": self.firm.kill_switch, "manual": self.firm.manual_kill,
                                                   "rule": self.firm.rule_kill},
              reason=req.reason, command_id=req.command_id)
        self._record(req.command_id, req, ctx, tx, ok_reply(control))

    def apply_firm_control(self, control: FirmControl) -> list[Effect]:
        """Apply ``risk.control.firm`` from the coordinator (the shell adds the ``fact_cursors`` row).

        ``RULE_STATE`` replaces the whole RULE layer (rule kill switch + firm rule
        restrictions), so the newest fact alone - e.g. the ``ctl.snapshot``
        fallback - restores it; the legacy actions are still accepted.
        """
        tx = Tx(label="firm_control")
        src = self.coordinator_instance if control.origin == ControlOrigin.RULE else API_GATEWAY
        self._apply_control(control, tx, self._now(), src_instance=src)
        return tx.effects()

    def _apply_control(self, control: FirmControl, tx: Tx, now: int, *, src_instance: str) -> None:
        ch = self.firm.apply(control)
        tx.insert("firm_controls", firm_control_row(control, src_instance))
        tx.mark_critical()
        flag_changed = ch.manual_changed if control.origin == ControlOrigin.MANUAL else ch.rule_changed
        is_kill = flag_changed  # 只在标志真正变化时发事件
        if is_kill:
            on = self.firm.manual_kill if control.origin == ControlOrigin.MANUAL else self.firm.rule_kill
            self.events.emit(
                tx,
                kind=RiskEventKind.KILL_SWITCH_ON if on else RiskEventKind.KILL_SWITCH_OFF,
                trading_day=self._trading_day,
                ts_ns=now,
                message=f"kill switch {'开启' if on else '关闭'}（{control.origin.value}）：{control.reason}；生效={self.firm.kill_switch}",
                severity=Severity.CRITICAL if on else Severity.HIGH,
                scope_key="firm",
                alert_key=control.cause_alert_key,
                actor=control.actor,
                detail={"origin": control.origin.value, "effective": self.firm.kill_switch,
                        "manual": self.firm.manual_kill, "rule": self.firm.rule_kill},
            )
        cause = "KILL_SWITCH" if is_kill else "RULE"
        for a in sorted(self._accounts):
            self._refresh_state(self._accounts[a], tx, now, cause=cause, cause_alert_key=control.cause_alert_key,
                                actor=control.actor, reason=control.reason)

    def ingest_snapshot(self, snapshot: AccountSnapshot) -> None:
        """Coordinator: another shard's ``risk.snapshot.*`` (own snapshots are ingested when published)."""
        if self.aggregator is not None:
            self.aggregator.ingest(snapshot)

    def note_shard_alive(self, shard: int, ts_ns: int | None = None) -> None:
        """Coordinator: a heartbeat of ``shard`` (freshness of its accounts between snapshots, H1)."""
        if self.aggregator is not None:
            self.aggregator.note_shard_alive(shard, self._now() if ts_ns is None else ts_ns)

    def firm_snapshot(self) -> FirmSnapshot:
        """Current ``FirmSnapshot`` (coordinator only)."""
        if self.aggregator is None:
            raise RuntimeError("not the coordinator")
        now = self._now()
        self.aggregator.build_valuation(self._trading_day, now)
        return self.aggregator.firm_snapshot(self._trading_day, now, kill_switch=self.firm.kill_switch)

    def _firm_tick(self, tx: Tx, now: int) -> None:
        """Coordinator 1 Hz: aggregate, evaluate firm rules, publish controls and ``risk.firm``."""
        agg = self.aggregator
        if agg is None:
            return
        td = self._trading_day
        val = agg.build_valuation(td, now)
        evals = evaluate_owner(self.rulebook.firm_rules(), agg.machines, val, account_id=None,
                               state_key=agg.state_seq, now_ns=now)
        persist_transitions(tx, evals, events=self.events, alerts=self.alerts, trading_day=td, account_id=None,
                            config_hash=self.rulebook.config_hash, now_ns=now)
        kill, cause, restr = agg.desired_controls(td, now)
        if kill != self.firm.rule_kill or not same_restrictions(restr, self.firm.restrictions):
            # 规则层的完整状态作为一条事实发布（kill switch + 全部公司级限制），最新一条即可恢复
            control = FirmControl(
                action=FirmAction.RULE_STATE, origin=ControlOrigin.RULE, active=kill,
                restrictions=merge_since(restr, self.firm.restrictions), trading_day=td, reason="firm rules",
                cause_alert_key=cause if kill else None, ts_ns=now,
            )
            tx.emit(topics.RISK_CONTROL_FIRM, control, ts_ns=now, corr_id=control.cause_alert_key, td=td)
            self._apply_control(control, tx, now, src_instance=self.instance)
        tx.publish(topics.RISK_FIRM, agg.firm_snapshot(td, now, kill_switch=self.firm.kill_switch))

    # =================================================================== EOD
    def _next_trading_day(self) -> str | None:
        cal = self._calendar
        if cal is None or not self._trading_day:
            return None
        try:
            return td_str(cal.next_trading_day(parse_td(self._trading_day)))
        except LookupError:
            return None

    def _provisional_trigger_ns(self) -> int | None:
        """Start of the next trading day's first session (02 §8.4 临时日初快照的触发)."""
        cal = self._calendar
        if not self.auto_provisional or cal is None or self._sod is None or self._lifecycle == Lifecycle.LOADING:
            return None
        cached = self._prov_trigger
        if cached is not None and cached[0] == self._trading_day:
            return cached[1]
        nxt = self._next_trading_day()
        value: int | None = None
        if nxt is not None:
            templates = sorted({s for spec in self._registry.products.products.values() for s in spec.sessions})
            start = cal.first_open(templates, parse_td(nxt))
            value = bj_to_ns(start) if start is not None else None
        self._prov_trigger = (self._trading_day, value)
        return value

    def _check_provisional(self, tx: Tx, now: int) -> None:
        if self._provisional_pending:
            return
        start = self._provisional_trigger_ns()
        if start is None or now < start:
            return
        td = self._trading_day
        nxt = self._next_trading_day()
        st = self._eod.setdefault(td, EodStatus(td))
        if st.status == EOD_OPEN:
            st.status = EOD_FROZEN  # 连 eod.freeze 都没收到：先自行冻结 T
            st.frozen_ns = now
        self._mode = TradingMode.EOD_FROZEN
        tx.upsert("eod_states", ("trading_day",), st.row())
        tx.mark_critical()
        self._provisional_pending = True
        held = sorted({i for ra in self._accounts.values() for i, _d, _q in ra.ledger.held()}
                      | {t.instrument_id for ra in self._accounts.values() for t in ra.trades})
        self._new_request(tx, "provisional", MD_GATEWAY, "md.settle_inputs",
                          SettleInputsRequest(trading_day=td, instruments=held), timeout_ms=30_000,
                          trading_day=td, next_trading_day=nxt)

    def _on_provisional_inputs(self, p: _Pending, reply: Reply, tx: Tx, now: int) -> None:
        self._provisional_pending = False
        if p.data.get("trading_day") != self._trading_day or self._sod is None:
            return  # 正式换日已经先到
        nxt = p.data.get("next_trading_day")
        if not isinstance(nxt, str):
            return
        inputs = reply.open_as(SettleInputsBatch).items if (reply.ok and reply.data is not None) else []
        sod = self._build_provisional(nxt, inputs)
        self._roll(sod, DayRollMode.PROVISIONAL, tx, now)

    def _build_provisional(self, next_td: str, inputs: Sequence[Any],
                           overrides: Mapping[str, float] | None = None) -> SodSnapshot:
        """Own SOD for ``next_td`` (02 §8.4 构建临时快照).

        Prices: VWAP from ``md.settle_inputs`` > ``overrides`` (enter_provisional) >
        the day's last mark (``_day_marks``: ticks of the current trading day only,
        a next-day tick never replaces T's close) > previous settlement.
        """
        assert self._sod is not None
        trades = [t for a in sorted(self._accounts) for t in self._accounts[a].trades]
        cash = [c for a in sorted(self._accounts) for c in self._accounts[a].cash]
        insts = {i for ra in self._accounts.values() for i, _d, _q in ra.ledger.held()} | {t.instrument_id for t in trades}
        insts |= {lot.instrument_id for sa in self._sod.accounts for lot in sa.lots}
        marks: dict[str, float] = {}
        for i in insts:
            m = self._day_marks.get(i)
            if m is None:
                ps = self._pre_settle(i)
                m = float(ps) if ps is not None else None
            if m is not None:
                marks[i] = m
        for t in trades:
            marks.setdefault(t.instrument_id, t.price)
        for i, px in (overrides or {}).items():
            marks[i] = float(px)
        pre = {k: D(v) for k, v in self._sod.pre_settle.items()}
        for i in insts:
            tk = self._ticks.get(i)
            if i not in pre and tk is not None and tk.pre_settle > 0:
                pre[i] = D(tk.pre_settle)
        prices = provisional_prices(self._registry, insts, settle_inputs=inputs, marks=marks, pre_settle=pre)
        for i in insts:
            pre.setdefault(i, prices[i])  # 无昨结算的新合约：以临时结算价兜底（只用于展示）
        return build_provisional_sod(registry=self._registry, sod=self._sod, trades=trades, cash=cash, prices=prices,
                                     next_trading_day=next_td, pre_settle=pre)

    def enter_provisional(self, prices: Mapping[str, float] | None = None) -> list[Effect]:
        """Force the PROVISIONAL transition now (tests / backtests).

        ``prices`` (instrument -> price) are the provisional settlement prices
        of those instruments; the others use the day's last marks.
        """
        nxt = self._next_trading_day()
        if nxt is None:
            raise RuntimeError("next trading day unknown (no calendar)")
        tx = Tx(label="provisional")
        now = self._now()
        st = self._eod.setdefault(self._trading_day, EodStatus(self._trading_day))
        if st.status == EOD_OPEN:
            st.status, st.frozen_ns = EOD_FROZEN, now
        self._roll(self._build_provisional(nxt, [], prices), DayRollMode.PROVISIONAL, tx, now)
        return tx.effects()

    def freeze(self, trading_day: str, version: int = 0) -> EodFreezeAck:
        """Backtest helper: ``eod.freeze`` without persistence (the shell uses :meth:`handle`)."""
        tx = Tx(label="eod.freeze")
        res = self._freeze(trading_day, version, tx, self._now())
        if isinstance(res, Reply):
            raise RuntimeError(f"freeze refused: {res.code} {res.message}")
        return res

    def freeze_digest_source(self, trading_day: str) -> str | None:
        """Who answers ``eod.freeze{X}`` (02 §6.3, §8.4; catch-up after missed EODs).

        - ``"MEMORY"``: the core (:meth:`handle`): X is the current trading day
          (normal freeze), or - while PROVISIONAL - the day the provisional SOD
          was built from (digests only, the trading mode is unchanged);
        - ``"STORE"``: the shell, digests only from risk.db with
          :func:`futrisk.core.eod.digest_only_ack` over the ``fills`` /
          ``cash_movements`` rows of X: X is an older day this shard ran (it
          has an ``eod_states`` row), settled already (re-settlement) or not
          (missed EODs). Nothing is frozen and the current state is unchanged;
        - None: refused with PRECONDITION_FAILED (a later day, or a day this
          shard never ran).
        """
        td = self._trading_day
        if not td:
            return None
        if trading_day == td:
            return "MEMORY"
        if self._mode == TradingMode.PROVISIONAL and self._prev_day is not None and trading_day == self._prev_day[0]:
            return "MEMORY"
        if trading_day < td and trading_day in self._eod:
            return "STORE"
        return None

    def _freeze(self, trading_day: str, version: int, tx: Tx, now: int) -> EodFreezeAck | Reply:
        source = self.freeze_digest_source(trading_day)
        if source is None:
            return err_reply(codes.PRECONDITION_FAILED,
                             f"cannot freeze {trading_day}: current trading day {self._trading_day or '-'} "
                             f"({self._mode.value}); not a day of shard {self.shard}")
        if source == "STORE":
            return err_reply(codes.NOT_FOUND,
                             f"{trading_day} is an older day: its digests come from risk.db "
                             "(freeze_digest_source == STORE, digest_only_ack), nothing is frozen")
        if trading_day != self._trading_day and self._prev_day is not None:
            # 02 §6.3：已自行冻结并进入 PROVISIONAL 时，只返回摘要，不改变交易模式
            _td, trades, cash = self._prev_day
            accounts = sorted(set(self._accounts) | set(trades) | set(cash))
            st = self._eod.setdefault(trading_day, EodStatus(trading_day, status=EOD_PROVISIONAL))
            open_orders = 0
        else:  # OPEN / EOD_FROZEN, or PROVISIONAL for its own (new) day
            trades = {a: list(ra.trades) for a, ra in self._accounts.items()}
            cash = {a: list(ra.cash) for a, ra in self._accounts.items()}
            accounts = sorted(self._accounts)
            st = self._eod.setdefault(trading_day, EodStatus(trading_day))
            if st.status == EOD_OPEN:
                st.status = EOD_FROZEN
            if st.frozen_ns is None:
                st.frozen_ns = now
            self._mode = TradingMode.EOD_FROZEN
            for ra in self._accounts.values():
                ra.snapshot_dirty = True
            open_orders = sum(1 for o in self._orders.resting() if o.trading_day == trading_day)
        ack = EodFreezeAck(
            trading_day=trading_day,
            shard=self.shard,
            accounts=account_digests(trades, accounts),
            cash=cash_digests(cash, accounts),
            open_orders=open_orders,
            frozen_ns=st.frozen_ns or now,
        )
        st.freeze_version = version
        st.digest = {"accounts": msgspec.to_builtins(ack.accounts), "cash": msgspec.to_builtins(ack.cash)}
        tx.upsert("eod_states", ("trading_day",), st.row())
        tx.mark_critical()
        return ack

    def _cmd_eod_freeze(self, req: EodFreezeRequest, ctx: CommandCtx, tx: Tx) -> None:
        res = self._freeze(req.trading_day, req.version, tx, self._now())
        tx.reply(ctx.token, res if isinstance(res, Reply) else ok_reply(res))

    # ------------------------------------------------------- eod.rollover
    def _prov_source(self, day: str) -> EodStatus | None:
        """The day whose provisional SOD is ``day`` (None when ``day`` runs on an official SOD)."""
        for st in self._eod.values():
            if (st.next_trading_day == day and st.trading_day < day and st.sod_version == PROVISIONAL_SOD_VERSION
                    and st.status in (EOD_PROVISIONAL, EOD_FORCE_UNFROZEN)):
                return st
        return None

    def last_official_day(self) -> str | None:
        """Trading day of the newest official (clearing) SOD in the chain leading to the current day."""
        if self._sod is None:
            return None
        day = self._trading_day
        if not self.provisional:
            return day
        for _ in range(len(self._eod) + 1):
            src = self._prov_source(day)
            if src is None:
                return day
            day = src.trading_day
        return day

    def _roll_in_effect(self, dr: DayRoll) -> bool:
        """True while the roll ``dr`` answered is still the applied state (duplicate eod.rollover)."""
        if self._sod is None:
            return False
        if dr.to_trading_day == self._trading_day:
            return self._sod.version == dr.sod_version
        if dr.to_trading_day < self._trading_day:
            st = self._eod.get(dr.from_trading_day)
            return (st is not None and st.status == EOD_ROLLED and st.next_trading_day == dr.to_trading_day
                    and st.sod_version == dr.sod_version)
        return False

    def _rollover_error(self, req: EodRolloverRequest) -> Reply | None:
        """Preconditions of ``eod.rollover{X -> Y, V}`` (02 §8.4; contiguity of the catch-up)."""
        cur = self._sod
        if cur is None:
            return None  # 建账推送：直接加载
        x, y, v = req.trading_day, req.next_trading_day, req.sod_version
        td = self._trading_day
        if y <= x:
            return err_reply(codes.PRECONDITION_FAILED, f"next_trading_day {y} must follow trading_day {x}")
        if y > td and x != td:
            return err_reply(codes.PRECONDITION_FAILED, f"rollover {x}->{y}: current trading day is {td}")
        if y == td and cur.version > v and not self.provisional:
            return err_reply(codes.PRECONDITION_FAILED, f"sod version {v} of {y} is older than the applied {cur.version}")
        st = self._eod.get(x)
        resettle = st is not None and st.status == EOD_ROLLED and st.next_trading_day == y
        last = self.last_official_day()
        if x != last and not resettle:
            return err_reply(codes.PRECONDITION_FAILED,
                             f"rollover {x}->{y} is not contiguous: the last official SOD of shard {self.shard} is for "
                             f"{last}; settle {last} first")
        if y < td and resettle and st is not None and st.sod_version is not None and st.sod_version > v:
            return err_reply(codes.PRECONDITION_FAILED, f"sod version {v} of {y} is older than the recorded {st.sod_version}")
        return None

    def _cmd_eod_rollover(self, req: EodRolloverRequest, ctx: CommandCtx, tx: Tx) -> None:
        """``eod.rollover``: fetch the named SOD version asynchronously, then roll / rebase / catch up / load
        (02 §6.3, §8.4).

        ``Y`` later than the current day → ROLL; ``Y`` the current day → REBASE
        (newer version, or the official SOD of a provisional day); ``Y`` older
        than the current day (the shard went PROVISIONAL over missed EODs) →
        CATCH_UP: the official SOD of ``Y`` is stored, the current state is
        unchanged. ``X`` must be the day of the last official SOD (or a
        re-settlement of an already settled ``X -> Y``). Repeats of the same
        ``(Y, V)`` get the first ``DayRoll`` with ``duplicate=true``.
        """
        key = (req.next_trading_day, req.sod_version)
        for p in self._pending.values():
            if p.kind == "rollover" and p.data.get("key") == key:
                p.tokens.append(ctx.token)  # 重复推送：等同一个结果
                return
        dr = self._dayrolls.get(key)
        if dr is not None and self._roll_in_effect(dr):
            tx.reply(ctx.token, ok_reply(dr, duplicate=True))
            return
        cur = self._sod
        if cur is not None and self._trading_day == req.next_trading_day and cur.version == req.sod_version:
            tx.reply(ctx.token, ok_reply(self._current_dayroll(req.trading_day), duplicate=True))
            return
        err = self._rollover_error(req)
        if err is not None:
            tx.reply(ctx.token, err)
            return
        self._new_request(
            tx, "rollover", CLEARING, "settle.sod_snapshot",
            SodRequest(trading_day=req.next_trading_day, account_ids=list(self.owned), version=req.sod_version),
            timeout_ms=30_000, reply_token=ctx.token, key=key, request=req,
        )

    def _current_dayroll(self, from_td: str) -> DayRoll:
        """A DayRoll describing the SOD in force (answer of a duplicate whose original reply is unknown)."""
        assert self._sod is not None
        st = self._eod.get(from_td)
        ts = st.rolled_ns if st is not None and st.rolled_ns else 0
        return DayRoll(shard=self.shard, mode=DayRollMode.ROLL, from_trading_day=from_td, to_trading_day=self._trading_day,
                       sod_version=self._sod.version, accounts=sorted(self._accounts), ts_ns=ts)

    def _on_rollover_result(self, p: _Pending, reply: Reply, tx: Tx, now: int) -> None:
        req: EodRolloverRequest = p.data["request"]
        if not reply.ok or reply.data is None:
            for tok in p.tokens:
                tx.reply(tok, err_reply(reply.code if not reply.ok else codes.INTERNAL, reply.message or "sod_snapshot failed"))
            return
        sod = reply.open_as(SodSnapshot)
        if sod.trading_day != req.next_trading_day or sod.version != req.sod_version:
            for tok in p.tokens:
                tx.reply(tok, err_reply(codes.PRECONDITION_FAILED, "SOD snapshot does not match the rollover request"))
            return
        done = self._dayrolls.get((req.next_trading_day, req.sod_version))
        if done is not None and self._roll_in_effect(done):
            for tok in p.tokens:
                tx.reply(tok, ok_reply(done, duplicate=True))
            return
        err = self._rollover_error(req)
        if err is not None:
            for tok in p.tokens:
                tx.reply(tok, err)
            return
        try:
            if self._sod is None:
                for e in self.load_sod(sod):
                    self._absorb(tx, e)
                dr = DayRoll(shard=self.shard, mode=DayRollMode.ROLL, from_trading_day=req.trading_day,
                             to_trading_day=sod.trading_day, sod_version=sod.version, accounts=sorted(self._accounts),
                             ts_ns=now)
                tx.emit(topics.SYS_DAYROLL, dr, ts_ns=now, td=sod.trading_day)
                self._last_dayroll = dr
                self._dayrolls[(dr.to_trading_day, dr.sod_version)] = dr
            elif sod.trading_day < self._trading_day:
                dr = self._catch_up(req, sod, tx, now)
            else:
                mode = DayRollMode.REBASE if self._trading_day == sod.trading_day else DayRollMode.ROLL
                dr = self._roll(sod, mode, tx, now, settled_td=req.trading_day)
        except ValueError as exc:  # SOD rejected by the validation (nothing was changed)
            for tok in p.tokens:
                tx.reply(tok, err_reply(codes.PRECONDITION_FAILED, f"SOD {sod.trading_day} v{sod.version} rejected: {exc}"))
            return
        for tok in p.tokens:
            tx.reply(tok, ok_reply(dr))

    def _catch_up(self, req: EodRolloverRequest, sod: SodSnapshot, tx: Tx, now: int) -> DayRoll:
        """CATCH_UP: store the official SOD of an older day ``Y``; the current (provisional) day is unchanged.

        ``eod_states[X]`` becomes ROLLED (``next_trading_day = Y``, ``sod_version = V``)
        and the ``sod_*`` rows of ``Y`` are replaced by the official version; the
        later rollover whose ``Y`` is the current day rebases it (02 §8.4).
        """
        self._stage_accounts(sod)  # 校验（每个账户都在、账本可以建立）；不改变当前状态
        x, y = req.trading_day, sod.trading_day
        self._sod_rows(tx, sod, now)
        st = self._eod.setdefault(x, EodStatus(x))
        st.status, st.next_trading_day, st.sod_version, st.rolled_ns = EOD_ROLLED, y, sod.version, now
        tx.upsert("eod_states", ("trading_day",), st.row())
        audit(tx, ts_ns=now, trading_day=self._trading_day, actor="system", action=AUDIT_DAYROLL, target=f"shard:{self.shard}",
              before={"trading_day": x}, after={"mode": DayRollMode.CATCH_UP.value, "trading_day": y,
                                                 "sod_version": sod.version, "current_trading_day": self._trading_day})
        dr = DayRoll(shard=self.shard, mode=DayRollMode.CATCH_UP, from_trading_day=x, to_trading_day=y,
                     sod_version=sod.version, accounts=sorted(self._accounts), ts_ns=now)
        tx.emit(topics.SYS_DAYROLL, dr, ts_ns=now, td=self._trading_day)
        tx.mark_critical()
        self._dayrolls[(y, sod.version)] = dr
        return dr

    @staticmethod
    def _absorb(tx: Tx, e: Effect) -> None:
        if isinstance(e, Persist):
            tx.rows.extend(e.rows)
            tx.outbox.extend(e.outbox)
            tx.critical = tx.critical or e.critical
        elif isinstance(e, Publish):
            tx.publishes.append(e)
        elif isinstance(e, ReplyTo):
            tx.replies.append(e)
        else:
            tx.actions.append(e)

    def roll_day(self, sod: SodSnapshot, mode: DayRollMode | None = None) -> list[Effect]:
        """Install ``sod``: ROLL (new day), REBASE (same day, newer version) or PROVISIONAL (own snapshot).

        The backtester calls this once per day with the SOD from :func:`settle_day`.
        """
        tx = Tx(label="dayroll")
        now = self._now()
        if self._sod is None:
            return self.load_sod(sod)
        if mode is None:
            mode = DayRollMode.REBASE if sod.trading_day == self._trading_day else DayRollMode.ROLL
        self._roll(sod, mode, tx, now)
        return tx.effects()

    def _roll(self, sod: SodSnapshot, mode: DayRollMode, tx: Tx, now: int, *, settled_td: str | None = None) -> DayRoll:
        """ROLL (next day), PROVISIONAL (own SOD of the next day) or REBASE (same day, newer / official SOD).

        The SOD is validated before anything changes (every owned account
        present, ledgers buildable): a ValueError leaves the engine untouched.
        ROLL / PROVISIONAL also expire the previous days' orders (CANCELED,
        SESSION_CLOSE, confirmed with ``route.resolve``), roll the rule state,
        and ask ``td.query.trades`` for fills of the new day that were only
        buffered in memory (``query_trades_on_roll``). ROLL and a REBASE from a
        provisional SOD compare the official SOD lots with the engine's
        end-of-day lots (01 §6.6): a difference opens a non-blocking
        POSITION_MISMATCH break (``reason = SOD_VS_EOD``).
        """
        from_td = self._trading_day
        to_td = sod.trading_day
        if mode == DayRollMode.REBASE:
            if to_td != from_td:
                raise ValueError(f"REBASE needs the current trading day {from_td}, got {to_td}")
        elif to_td <= from_td:
            raise ValueError(f"{mode.value} from {from_td} needs a later trading day, got {to_td}")
        staged = self._stage_accounts(sod)  # 先校验再修改：校验失败时引擎状态不变
        was_prov = self.provisional
        eod_lots = self._eod_lots(from_provisional=was_prov) if (
            mode == DayRollMode.ROLL or (mode == DayRollMode.REBASE and was_prov)) else None
        settled_day = settled_td or from_td
        latches = 0
        if mode in (DayRollMode.ROLL, DayRollMode.PROVISIONAL):
            self._expire_old_orders(to_td, tx, now)
            # 01 §8.14 换日：所有实例归零，episode 从头计数；锁存按 reset_on_day_roll 解除；告警改为 ROLLED
            carried: dict[str, dict[tuple[str, str, str], LevelMachine]] = {}
            for a, ra in self._accounts.items():
                keep: dict[tuple[str, str, str], LevelMachine] = {}
                for k, m in ra.machines.items():
                    if m.day_roll(now):
                        latches += 1
                    if m.state != RuleSmState.OK:
                        keep[k] = m
                carried[a] = keep
            if self.aggregator is not None:
                for m in self.aggregator.machines.values():
                    m.day_roll(now)
                self.aggregator.machines = {k: m for k, m in self.aggregator.machines.items() if m.state != RuleSmState.OK}
                self.aggregator.forget_day(to_td)
            for rec in sorted(self.alerts.values(), key=lambda r: r.alert_key):
                if rec.status == ALERT_ACTIVE:
                    rec.status = ALERT_ROLLED
                    rec.persist_status(tx)
            for pid in sorted(self.plans):
                plan = self.plans[pid]
                if plan.status == LiquidationStatus.PENDING_CONFIRM:
                    self.plans[pid] = msgspec.structs.replace(plan, status=LiquidationStatus.EXPIRED)
                    tx.upsert("liquidation_plans", ("plan_id",), self._plan_row(self.plans[pid]))
                    self._plan_event(tx, self.plans[pid], now, actor=None)
            self._prev_day = (
                from_td,
                {a: list(ra.trades) for a, ra in self._accounts.items()},
                {a: list(ra.cash) for a, ra in self._accounts.items()},
            )
            self._install_sod(sod, keep_state=False, staged=staged)
            for a, ra in self._accounts.items():
                ra.machines = carried.get(a, {})
                for m in ra.machines.values():
                    self._raise_carried(ra, m, tx, now, from_td)
            self.alerts = {k: v for k, v in self.alerts.items() if v.trading_day == to_td}
            self.events.clear_before(to_td)
            self._orders.prune(to_td)
            if mode == DayRollMode.ROLL:
                self._mode = TradingMode.OPEN
                st = self._eod.setdefault(from_td, EodStatus(from_td))
                st.status, st.next_trading_day, st.sod_version, st.rolled_ns = EOD_ROLLED, to_td, sod.version, now
            else:
                self._mode = TradingMode.PROVISIONAL
                st = self._eod.setdefault(from_td, EodStatus(from_td))
                st.status, st.next_trading_day, st.sod_version, st.provisional_ns = EOD_PROVISIONAL, to_td, sod.version, now
            tx.upsert("eod_states", ("trading_day",), st.row())
            pending_cash = [c for c in self._next_day_cash if c.trading_day == to_td]
            self._next_day_cash = [c for c in self._next_day_cash if c.trading_day > to_td]
            for c in pending_cash:
                ra_c = self._accounts.get(c.account_id)
                if ra_c is not None and c.movement_id not in ra_c.movement_ids:
                    ra_c.ledger.apply_cash(c)
                    ra_c.cash.append(c)
                    ra_c.movement_ids.add(c.movement_id)
            audit(tx, ts_ns=now, trading_day=to_td, actor="system",
                  action=AUDIT_DAYROLL if mode == DayRollMode.ROLL else AUDIT_PROVISIONAL_SOD, target=f"shard:{self.shard}",
                  before={"trading_day": from_td}, after={"trading_day": to_td, "sod_version": sod.version,
                                                           "latches_cleared": latches})
        else:  # REBASE：替换日初快照并按顺序重放当日成交；风控状态机与 episode 保持不变
            trades = {a: list(ra.trades) for a, ra in self._accounts.items()}
            cash = {a: list(ra.cash) for a, ra in self._accounts.items()}
            self._install_sod(sod, keep_state=True, staged=staged)
            for a, ra in self._accounts.items():
                for t in trades.get(a, []):
                    if self._book_trade(ra, t, None) is None and not self._trade_break_open(ra, t.trade_key):
                        self._open_break(ra, ReconBreakKind.POSITION_MISMATCH, True,
                                         {"reason": "REBASE_REPLAY_FAILED", "trade_key": t.trade_key,
                                          "trade": msgspec.to_builtins(t)}, tx, now)
                for c in cash.get(a, []):
                    ra.ledger.apply_cash(c)
                    ra.cash.append(c)
                    ra.movement_ids.add(c.movement_id)
            if was_prov:
                self._mode = TradingMode.OPEN
                if settled_td is not None:
                    src = settled_td
                elif sod.from_settlement is not None:
                    src = sod.from_settlement.trading_day
                else:
                    src = self._prev_day[0] if self._prev_day else from_td
                settled_day = src
                st = self._eod.setdefault(src, EodStatus(src))
                st.status, st.next_trading_day, st.sod_version, st.rolled_ns = EOD_ROLLED, to_td, sod.version, now
                tx.upsert("eod_states", ("trading_day",), st.row())
            audit(tx, ts_ns=now, trading_day=to_td, actor="system", action=AUDIT_DAYROLL_REBASE, target=f"shard:{self.shard}",
                  after={"trading_day": to_td, "sod_version": sod.version, "from_provisional": was_prov})
        for a, ra in self._accounts.items():
            if mode == DayRollMode.PROVISIONAL:
                ra.system[SYSTEM_EOD_NOT_SETTLED] = system_restriction(a, SYSTEM_EOD_NOT_SETTLED, now)
            else:
                ra.system.pop(SYSTEM_EOD_NOT_SETTLED, None)
            self._rereserve_closes(ra)
        future = [t for t in self._future_trades if t.trading_day == to_td]
        self._future_trades = [t for t in self._future_trades if t.trading_day > to_td]
        for t in sorted(future, key=lambda x: x.trade_seq):
            self._apply_trade(t, tx, now)
        self._sod_rows(tx, sod, now)
        tx.upsert("eod_states", ("trading_day",), self._eod.setdefault(to_td, EodStatus(to_td)).row())
        for a in sorted(self._accounts):
            ra = self._accounts[a]
            self._register_orders(ra)
            ra.valuation = self._value(ra)
            ra.dirty = True
            self._refresh_state(ra, tx, now, cause="DAYROLL")
        if eod_lots is not None:
            self._sod_vs_eod(eod_lots, sod, settled_day, tx, now)
        if self.query_trades_on_roll and mode in (DayRollMode.ROLL, DayRollMode.PROVISIONAL):
            # 换日前只缓存在内存里的下一交易日成交（夜盘）：向 trade-gateway 补拉，按 trade_key 去重
            for a in sorted(self._accounts):
                self._new_request(tx, "recon_trades", TRADE_GATEWAY, "td.query.trades",
                                  TradeQuery(trading_day=to_td, account_id=a, limit=1000), account_id=a)
        self._rules_touched()
        dr = DayRoll(shard=self.shard, mode=mode, from_trading_day=from_td, to_trading_day=to_td, sod_version=sod.version,
                     accounts=sorted(self._accounts), latches_cleared=latches, ts_ns=now)
        tx.emit(topics.SYS_DAYROLL, dr, ts_ns=now, td=to_td)
        tx.mark_critical()
        self._last_dayroll = dr
        self._dayrolls[(to_td, sod.version)] = dr
        return dr

    def _expire_old_orders(self, to_td: str, tx: Tx, now: int) -> None:
        """M6 / 02 §8.3: GFD / FAK / FOK orders of a closed day are CANCELED (SESSION_CLOSE) and confirmed.

        Close reservations and frozen amounts are released; ``route.resolve``
        asks the trade-gateway for the final state (a broker still reporting
        the order working opens a non-blocking break). A late fill is still a
        fact and is applied by the usual rules.
        """
        for o in self._orders.resting():
            if o.trading_day >= to_td or o.tif not in (TimeInForce.GFD, TimeInForce.FAK, TimeInForce.FOK):
                continue
            ra = self._accounts.get(o.account_id)
            set_status(tx, o, OrderStatus.CANCELED, now_ns=now, reason=R_SESSION_CLOSE,
                       detail={"trading_day": o.trading_day, "next_trading_day": to_td})
            if ra is not None:
                self._release_close(ra, o)
                ra.ledger.remove_order(o.client_order_id)
            o.session_expired = True
            self._send_resolve(o, tx, now)

    def _eod_lots(self, *, from_provisional: bool) -> dict[str, dict[tuple[str, str], int]]:
        """End-of-day lots per account: the provisional SOD in force (built from the engine's own EOD) or the book."""
        out: dict[str, dict[tuple[str, str], int]] = {}
        if from_provisional and self._sod is not None:
            for sa in self._sod.accounts:
                d = out.setdefault(sa.account_id, {})
                for lot in sa.lots:
                    k = (lot.instrument_id, lot.direction.value)
                    d[k] = d.get(k, 0) + lot.qty
            return out
        for a, ra in self._accounts.items():
            d = out.setdefault(a, {})
            for pos in ra.ledger.book.positions(a):
                if pos.qty > 0:
                    d[(pos.instrument_id, pos.direction.value)] = pos.qty
        return out

    def _sod_vs_eod(self, eod_lots: dict[str, dict[tuple[str, str], int]], sod: SodSnapshot, settled_day: str,
                    tx: Tx, now: int) -> None:
        """01 §6.6 EOD position cross-check: official SOD lots vs the engine's end-of-day lots (WARN break)."""
        for sa in sorted(sod.accounts, key=lambda x: x.account_id):
            ra = self._accounts.get(sa.account_id)
            if ra is None:
                continue
            theirs: dict[tuple[str, str], int] = {}
            for lot in sa.lots:
                k = (lot.instrument_id, lot.direction.value)
                theirs[k] = theirs.get(k, 0) + lot.qty
            ours = eod_lots.get(sa.account_id, {})
            diffs: list[dict[str, Any]] = []
            for k in sorted(set(ours) | set(theirs)):
                if ours.get(k, 0) == theirs.get(k, 0):
                    continue
                contract = self._registry.get(k[0]).contract if self._registry.has(k[0]) else None
                if contract is not None and contract.last_trade_day <= settled_day and k not in theirs:
                    continue  # 到期交割的合约不会出现在下一交易日的日初快照中
                diffs.append({"instrument_id": k[0], "direction": k[1], "engine": ours.get(k, 0), "sod": theirs.get(k, 0)})
            if diffs:
                self._open_break(ra, ReconBreakKind.POSITION_MISMATCH, False,
                                 {"reason": "SOD_VS_EOD", "settled_day": settled_day, "sod_version": sod.version,
                                  "diffs": diffs}, tx, now)

    def _raise_carried(self, ra: RiskAccount, m: LevelMachine, tx: Tx, now: int, from_td: str) -> None:
        """reset_on_day_roll=false: a latch carried into the new day is a new alert (episode 1)."""
        td = self._trading_day
        key = m.alert_key(td)
        rec = AlertRecord(
            alert_key=key, trading_day=td, rule_id=m.rule.rule_id, level=m.level.name, scope_key=m.scope_key,
            episode=m.st.episode, account_id=ra.account_id, action=m.level.action, severity=m.level.severity,
            metric=m.rule.metric, value=m.st.last_value if m.st.last_value is not None else m.level.threshold,
            threshold=m.level.threshold, latched=True, status=ALERT_ACTIVE, raised_ns=now,
            message=f"{ra.account_id} {m.rule.rule_id}/{m.level.name} 锁存延续到 {td}", detail={"carried_from": from_td},
        )
        self.alerts[key] = rec
        rec.persist_new(tx)
        tx.upsert("rule_states", ("trading_day", "rule_id", "level", "scope_key"), m.to_row(td, self.rulebook.config_hash, now))
        self.events.emit(tx, kind=RiskEventKind.ALERT_RAISED, trading_day=td, ts_ns=now, message=rec.message,
                         severity=rec.severity, account_id=ra.account_id, scope_key=m.scope_key, alert_key=key,
                         rule_id=m.rule.rule_id, level=m.level.name, episode=m.st.episode, action=m.level.action,
                         latched=True, metric=m.rule.metric, value=rec.value, threshold=rec.threshold,
                         detail={"carried_from": from_td})

    def _rereserve_closes(self, ra: RiskAccount) -> None:
        for o in self._orders.resting(ra.account_id):
            if o.is_open or o.remaining <= 0:
                continue
            o.close_reserved = 0
            pos = ra.ledger.book.find(ra.account_id, o.instrument_id, pos_direction(o.direction, o.offset))
            if pos is None:
                continue
            try:
                pos.freeze_close(o.offset, o.remaining)
                o.close_reserved = o.remaining
            except PositionError:
                o.close_reserved = 0

    def _cmd_force_unfreeze(self, req: ForceUnfreezeRequest, ctx: CommandCtx, tx: Tx) -> None:
        """``eod.force_unfreeze``: lift SYSTEM:EOD_NOT_SETTLED in PROVISIONAL, audited (02 §6.3)."""
        if self._dedupe(req.command_id, req, ctx, tx):
            return
        err = self._role_error(req.role, RISK_MANAGER_ROLES)
        if err is None and self._mode != TradingMode.PROVISIONAL:
            err = err_reply(codes.PRECONDITION_FAILED, "only valid in PROVISIONAL")
        if err is not None:
            self._record(req.command_id, req, ctx, tx, err)
            return
        now = self._now()
        src = self._prev_day[0] if self._prev_day is not None else req.trading_day
        st = self._eod.setdefault(src, EodStatus(src))
        st.status, st.forced_by, st.forced_reason = EOD_FORCE_UNFROZEN, req.actor, req.reason
        tx.upsert("eod_states", ("trading_day",), st.row())
        for a in sorted(self._accounts):
            ra = self._accounts[a]
            ra.system.pop(SYSTEM_EOD_NOT_SETTLED, None)
            self._refresh_state(ra, tx, now, cause="MANUAL", actor=req.actor, reason=req.reason)
        self.events.emit(tx, kind=RiskEventKind.EOD_FORCE_UNFREEZE, trading_day=self._trading_day, ts_ns=now,
                         message=f"EOD 未完成，{req.actor} 审计放行临时快照上的开仓：{req.reason}", severity=Severity.HIGH,
                         actor=req.actor, detail={"trading_day": src})
        audit(tx, ts_ns=now, trading_day=self._trading_day, actor=req.actor, role=req.role, action=AUDIT_EOD_FORCE_UNFREEZE,
              target=f"shard:{self.shard}", reason=req.reason, command_id=req.command_id)
        tx.mark_critical()
        self._record(req.command_id, req, ctx, tx, ok_reply())

    # ================================================================ queries
    def _q_accounts(self, q: QueryAccounts, ctx: CommandCtx, tx: Tx) -> None:
        ids = q.account_ids if q.account_ids else sorted(self._accounts)
        now = self._now()
        items = [self._snapshot(self._accounts[a], now) for a in ids if a in self._accounts]
        tx.reply(ctx.token, ok_reply(AccountSnapshotBatch(items=items)))

    def _q_positions(self, q: QueryPositions, ctx: CommandCtx, tx: Tx) -> None:
        out = []
        for a in sorted(self._accounts):
            if q.account_id is not None and a != q.account_id:
                continue
            ra = self._accounts[a]
            for pv in ra.ledger.position_views():
                info = self._registry.get(pv.instrument_id)
                if q.product is not None and pv.product != q.product:
                    continue
                if q.instrument_id is not None and pv.instrument_id != q.instrument_id:
                    continue
                if q.exchange is not None and pv.exchange != q.exchange:
                    continue
                if q.sector is not None and info.sector != q.sector:
                    continue
                out.append(pv)
        tx.reply(ctx.token, ok_reply(PositionBatch(items=out)))

    def _history(self, ctx: CommandCtx, tx: Tx, what: str) -> None:
        tx.reply(ctx.token, err_reply(codes.NOT_FOUND, f"{what}: not in memory, answered by the shell from risk.db"))

    def _q_orders(self, q: QueryOrders, ctx: CommandCtx, tx: Tx) -> None:
        if q.trading_day != self._trading_day:
            self._history(ctx, tx, "orders of another trading day")
            return
        items = [o.to_update() for o in self._orders.of_account(q.account_id, q.trading_day)
                 if q.status is None or o.status in q.status]
        tx.reply(ctx.token, ok_reply(OrderBatch(items=items)))

    def _q_fills(self, q: QueryFills, ctx: CommandCtx, tx: Tx) -> None:
        ra = self._accounts.get(q.account_id)
        if ra is None or q.trading_day != self._trading_day:
            self._history(ctx, tx, "fills of another trading day")
            return
        tx.reply(ctx.token, ok_reply(FillBatch(items=list(ra.trades))))

    def _q_cash(self, q: QueryCash, ctx: CommandCtx, tx: Tx) -> None:
        pool: list[CashMovement] = [c for ra in self._accounts.values() for c in ra.cash] + list(self._next_day_cash)
        if self._prev_day is not None:
            pool += [c for cs in self._prev_day[2].values() for c in cs]
        items = [c for c in pool if c.trading_day == q.trading_day and (q.account_id is None or c.account_id == q.account_id)]
        if not items and q.trading_day not in (self._trading_day, self._next_trading_day()):
            self._history(ctx, tx, "cash of another trading day")
            return
        tx.reply(ctx.token, ok_reply(CashBatch(items=sorted(items, key=lambda c: (c.ts_ns, c.movement_id)))))

    def _q_recon_breaks(self, q: QueryReconBreaks, ctx: CommandCtx, tx: Tx) -> None:
        items = [b for _, b in sorted(self.breaks.items())
                 if (q.account_id is None or b.account_id == q.account_id) and (q.status is None or b.status == q.status)]
        tx.reply(ctx.token, ok_reply(ReconBreakBatch(items=items)))

    def _q_alerts(self, q: QueryAlerts, ctx: CommandCtx, tx: Tx) -> None:
        if q.trading_day != self._trading_day:
            self._history(ctx, tx, "risk events of another trading day")
            return
        tx.reply(ctx.token, ok_reply(RiskEventBatch(items=self.events.of_day(q.trading_day, q.account_id))))

    def _q_metrics(self, q: QueryMetrics, ctx: CommandCtx, tx: Tx) -> None:
        self._history(ctx, tx, "metric series (metric_samples)")

    def _q_liquidation_plans(self, q: QueryLiquidationPlans, ctx: CommandCtx, tx: Tx) -> None:
        """Liquidation plans of the current trading day (optional account / status filters)."""
        td = q.trading_day or self._trading_day
        if td != self._trading_day:
            self._history(ctx, tx, "liquidation plans of another trading day (liquidation_plans)")
            return
        items = [p for p in sorted(self.plans.values(), key=lambda x: (x.created_ns, x.plan_id))
                 if p.trading_day == td and (q.account_id is None or p.account_id == q.account_id)
                 and (q.status is None or p.status == q.status)]
        tx.reply(ctx.token, ok_reply(LiquidationPlanBatch(items=items)))

    def _q_rules(self, q: QueryRules, ctx: CommandCtx, tx: Tx) -> None:
        rs = RuleSet(config_version=self.rulebook.config_version, config_hash=self.rulebook.config_hash,
                     rules=self.rulebook.rule_dicts())
        tx.reply(ctx.token, ok_reply(rs))

    def update_rules(self, rules: RiskRulesConfig, *, config_hash: str = "", config_version: int = 0) -> list[Effect]:
        """Hot reload of ``risk_rules`` (IMMEDIATE, 02 §7.10): thresholds apply at the next evaluation.

        Machines of removed / disabled rules or levels are dropped (their
        restriction lifts; an ACTIVE alert is closed as CLEARED), except a
        latched ACTIVE level: it keeps its old compiled rule - the HALTED state
        and its alert stay - until ``risk.reset`` or the day roll, and an
        ``audit_events`` row (CONFIG_COMMIT) records it. Account states are
        refreshed at once. **The shell must execute the returned effects.**
        """
        tx = Tx(label="config.rules")
        now = self._now()
        self.rulebook = RuleBook(rules, config_hash=config_hash, config_version=config_version)
        for a in sorted(self._accounts):
            ra = self._accounts[a]
            self._reload_machines(ra.machines, a, tx, now)
            ra.bucket.rate_per_s = rules.pretrade.throttle_rate_per_s
            ra.bucket.burst = rules.pretrade.throttle_burst
            ra.dirty = True
            self._refresh_state(ra, tx, now, cause="RULE", reason=f"risk_rules v{config_version}")
        if self.aggregator is not None:
            self._reload_machines(self.aggregator.machines, None, tx, now)
        self._rules_touched()
        return tx.effects()

    def _is_orphan(self, m: LevelMachine, account_id: str | None) -> bool:
        """The machine's rule / level is no longer in the rule book (kept only because it is latched)."""
        if m.rule.rule_id == MANUAL_RULE_ID:
            return False
        rule = self.rulebook.get(m.rule.rule_id)
        return rule is None or rule.level(account_id, m.level.name) is None

    def _reload_machines(self, machines: dict[tuple[str, str, str], LevelMachine], account_id: str | None,
                         tx: Tx, now: int) -> None:
        td = self._trading_day
        for k in sorted(machines):
            m = machines[k]
            if m.rule.rule_id == MANUAL_RULE_ID:
                continue
            rule = self.rulebook.get(m.rule.rule_id)
            lv = rule.level(account_id, m.level.name) if rule is not None else None
            if rule is not None and lv is not None:
                m.rule, m.level = rule, lv
                continue
            key = m.alert_key(td)
            if m.state == RuleSmState.ACTIVE and m.st.latched:
                # M5：锁存的熔断不因配置变更而静默解除，保留旧规则直到 risk.reset 或换日
                audit(tx, ts_ns=now, trading_day=td, actor="system", action=AUDIT_CONFIG_COMMIT, target=m.scope_key,
                      before={"rule_id": m.rule.rule_id, "level": m.level.name, "state": m.state.value, "latched": True,
                              "episode": m.st.episode},
                      after={"kept": True, "until": "risk.reset or the day roll",
                             "config_version": self.rulebook.config_version},
                      reason=f"rule {m.rule.rule_id}/{m.level.name} removed or disabled while latched: the latch is kept",
                      corr_id=key)
                tx.mark_critical()
                continue
            del machines[k]
            rec = self.alerts.get(key) if m.state == RuleSmState.ACTIVE and m.st.episode > 0 else None
            if rec is not None and rec.status == ALERT_ACTIVE:
                rec.status = ALERT_CLEARED
                rec.cleared_ns = now
                rec.persist_status(tx)
                self.events.emit(tx, kind=RiskEventKind.ALERT_CLEARED, trading_day=td, ts_ns=now,
                                 message=f"{account_id or 'firm'} {m.rule.rule_id}/{m.level.name} 解除：规则已被配置删除",
                                 severity=m.level.severity, account_id=account_id, scope_key=m.scope_key, alert_key=key,
                                 rule_id=m.rule.rule_id, level=m.level.name, episode=m.st.episode, action=m.level.action,
                                 detail={"reason": "rule removed by config", "config_version": self.rulebook.config_version})

    def _orphan_rule(self, row: Mapping[str, Any], trading_day: str) -> tuple[CompiledRule, LevelSpec]:
        """Compiled stand-in for a latched ``rule_states`` row whose rule is gone from the config (M5).

        Built from the matching ``alerts`` row (action, metric, threshold); it is
        never evaluated, only kept (HALTED stays) until ``risk.reset`` or the day roll.
        """
        rule_id, level, scope_key = str(row["rule_id"]), str(row["level"]), str(row["scope_key"])
        rec = self.alerts.get(alert_key(trading_day, rule_id, level, scope_key, int(row.get("episode") or 0)))
        action = rec.action if rec is not None else Action.CIRCUIT_BREAK
        if action not in (Action.CIRCUIT_BREAK, Action.LIQUIDATE):
            action = Action.CIRCUIT_BREAK
        threshold = rec.threshold if rec is not None else 0.0
        rule = compile_rule(RuleDef.model_validate({
            "id": rule_id, "metric": rec.metric if rec is not None else "orphan", "op": "<=",
            "levels": [{"name": level, "threshold": threshold, "action": action.value, "latch": True,
                        "confirm": {"states": 1, "ms": 0}}],
        }))
        return rule, rule.base_levels[0]

    # ================================================================ restore
    def restore(
        self,
        tables: Mapping[str, Sequence[Mapping[str, Any]]],
        *,
        lifecycle: Lifecycle = Lifecycle.WARMUP,
        intraday_peaks: Mapping[str, float] | None = None,
        dayrolls: Sequence[DayRoll] = (),
    ) -> None:
        """Rebuild the in-memory state from risk-{shard}.db rows (01 §6.5; 03 §4.3 重启恢复用到的表).

        ``tables`` maps table name -> rows (column dicts as persisted by the
        Persist effects; JSON columns may be decoded objects or JSON text).
        Needed: ``sod_accounts`` / ``sod_lots`` / ``sod_prices`` of the latest
        trading day ``td``; ``eod_states`` (recent rows: modes, catch-up chain,
        duplicate rollovers); ``orders`` of ``td`` plus every non-terminal
        order (incl. the ``cancel_*`` columns); ``order_events`` of the orders
        of ``td`` (compliance counters); ``fills`` and ``cash_movements`` of
        ``td`` - and, when ``td`` runs on a provisional SOD (version -1), also
        those of the day it was built from (``eod_states.next_trading_day ==
        td``), for its ``eod.freeze`` digests (plus cash of later days);
        ``rule_states`` / ``alerts`` / ``risk_events`` of ``td``;
        ``account_states``; ``firm_controls`` (all, by ``control_id``);
        ``liquidation_plans``; ``recon_breaks`` (``td`` and every OPEN one);
        ``processed_commands``; ``metric_samples`` of ``td`` (intraday peak,
        rows with ``quality != OK`` are ignored) or ``intraday_peaks``.
        ``dayrolls`` are the ``sys.dayroll`` facts of the outbox (exact replies
        for re-pushed rollovers; otherwise they are rebuilt from ``eod_states``).
        No effects are produced: call :meth:`post_restore` next.
        """

        def rows(name: str) -> list[dict[str, Any]]:
            return [dict(r) for r in tables.get(name, ())]

        sod_acc = rows("sod_accounts")
        if not sod_acc:
            raise ValueError("restore needs sod_accounts rows")
        td = max(str(r["trading_day"]) for r in sod_acc)
        lots_by_acct: dict[str, list[SodLot]] = {}
        for r in rows("sod_lots"):
            if r["trading_day"] != td:
                continue
            lots_by_acct.setdefault(str(r["account_id"]), []).append(
                SodLot(instrument_id=r["instrument_id"], direction=PosDirection(r["direction"]), lot_no=int(r["lot_no"]),
                       open_td=r["open_td"], open_ts_ns=int(r["open_ts_ns"]), open_price=str(r["open_price"]),
                       qty=int(r["qty"]), basis_price=str(r["basis_price"])))
        version = 0
        accounts: list[SodAccount] = []
        for r in sorted((r for r in sod_acc if r["trading_day"] == td), key=lambda x: str(x["account_id"])):
            version = int(r["sod_version"])
            accounts.append(SodAccount(account_id=r["account_id"], pre_balance=str(r["pre_balance"]),
                                       initial_capital=str(r["initial_capital"]), cum_net_deposit=str(r["cum_net_deposit"]),
                                       hwm_equity=str(r["hwm_equity"]), lots=lots_by_acct.get(str(r["account_id"]), [])))
        prices = {str(r["instrument_id"]): str(r["pre_settle"]) for r in rows("sod_prices") if r["trading_day"] == td}
        sod = SodSnapshot(trading_day=td, version=version, accounts=accounts, pre_settle=prices)
        self._install_sod(sod, keep_state=False)
        for r in rows("eod_states"):
            st = EodStatus.from_row(r)
            self._eod[st.trading_day] = st
        # 交易模式
        self._mode = TradingMode.OPEN
        prev_prov = sorted((s for s in self._eod.values()
                            if s.next_trading_day == td and s.trading_day < td
                            and s.status in (EOD_PROVISIONAL, EOD_FORCE_UNFROZEN)), key=lambda s: s.trading_day)
        src_day = prev_prov[-1].trading_day if (version == PROVISIONAL_SOD_VERSION and prev_prov) else None
        if version == PROVISIONAL_SOD_VERSION:
            self._mode = TradingMode.PROVISIONAL
        elif self._eod.get(td, EodStatus(td)).status == EOD_FROZEN:
            self._mode = TradingMode.EOD_FROZEN
        # 委托
        for r in rows("orders"):
            rec = OrderRecord.from_row(r)
            if (rec.trading_day == td or rec.status not in TERMINAL) and rec.client_order_id not in self._orders:
                self._orders.add(rec)
        # 合规计数（03 §4.3：由当日委托与委托事件推导；成交相关的计数在重放成交时恢复）
        today = {o.client_order_id: o for o in self._orders.all() if o.trading_day == td}
        for o in today.values():
            ra_o = self._accounts.get(o.account_id)
            if ra_o is not None and o.request_hash:  # 补建的委托（request_hash 为空）不是本系统的报单
                ra_o.orders_today += 1
        events = sorted(rows("order_events"), key=lambda x: (int(x.get("ts_ns") or 0), int(x.get("event_id") or 0)))
        for r in events:
            if r.get("to_status") != OrderStatus.CANCEL_PENDING.value or r.get("from_status") == OrderStatus.CANCEL_PENDING.value:
                continue
            rec_e = today.get(str(r["client_order_id"]))
            ra_e = self._accounts.get(rec_e.account_id) if rec_e is not None else None
            if rec_e is not None and ra_e is not None:
                self._count_cancel(ra_e, rec_e)
        # 成交（按 fill_seq 重放）；临时快照所依据的交易日的成交留作 eod.freeze 摘要
        prev_trades: dict[str, list[Trade]] = {}
        prev_cash: dict[str, list[CashMovement]] = {}
        fills = sorted(rows("fills"), key=lambda x: int(x.get("fill_seq") or 0))
        for r in fills:
            day = str(r["trading_day"])
            if day != td and day != src_day:
                continue
            t = trade_from_fill_row(r)
            if day == src_day:
                prev_trades.setdefault(t.account_id, []).append(t)
                continue
            ra = self._accounts.get(t.account_id)
            if ra is not None and t.trade_key not in ra.trade_keys:
                self._book_trade(ra, t, None)
                ra.last_fill_ns = max(ra.last_fill_ns, int(r.get("applied_ns") or 0))
        # 出入金
        for r in rows("cash_movements"):
            cm = cash_from_row(r)
            ra = self._accounts.get(cm.account_id)
            if ra is None:
                continue
            if cm.trading_day == td and cm.movement_id not in ra.movement_ids:
                ra.ledger.apply_cash(cm)
                ra.cash.append(cm)
                ra.movement_ids.add(cm.movement_id)
            elif cm.trading_day > td:
                self._next_day_cash.append(cm)
            elif cm.trading_day == src_day:
                prev_cash.setdefault(cm.account_id, []).append(cm)
        for r in rows("alerts"):
            if r["trading_day"] == td:
                rec_a = AlertRecord.from_row(r)
                self.alerts[rec_a.alert_key] = rec_a
        # 状态机（规则已删除但仍锁存的实例按告警行重建，M5）
        for r in rows("rule_states"):
            if r["trading_day"] != td:
                continue
            sk = parse_scope(str(r["scope_key"]))
            st_row = state_from_row(r)
            rule = _MANUAL_RULE if r["rule_id"] == MANUAL_RULE_ID else self.rulebook.get(str(r["rule_id"]))
            level = rule.level(sk.account_id, str(r["level"])) if rule is not None else None
            if rule is None or level is None:
                if not (st_row.state == RuleSmState.ACTIVE and st_row.latched):
                    continue
                rule, level = self._orphan_rule(r, td)
            m = LevelMachine(rule, level, str(r["scope_key"]), st_row)
            if sk.is_firm:
                if self.aggregator is not None:
                    self.aggregator.machines[m.key] = m
            elif sk.account_id in self._accounts:
                self._accounts[sk.account_id].machines[m.key] = m
        # 账户状态（含 state_version 与锁存）
        for r in rows("account_states"):
            ra = self._accounts.get(str(r["account_id"]))
            if ra is None:
                continue
            ra.state = AccountState(str(r["state"]))
            ra.halted_latched = bool(r["halted_latched"])
            ra.state_version = int(r["state_version"])
            ra.state_since_ns = int(r.get("since_ns") or 0)
            ra.cause = str(r.get("cause") or "SYSTEM")
            ra.cause_alert_key = r.get("cause_alert_key")
            restr = r.get("restrictions") or []
            if isinstance(restr, str):
                restr = json.loads(restr)
            ra.restrictions = [msgspec.convert(x, Restriction) for x in restr]
            for x in ra.restrictions:
                if x.source == "SYSTEM:FILL_GAP":
                    ra.system["FILL_GAP"] = x
        # 公司级控制（按 control_id 顺序重放）
        for r in sorted(rows("firm_controls"), key=lambda x: int(x.get("control_id") or 0)):
            rs = r.get("restrictions") or []
            if isinstance(rs, str):
                rs = json.loads(rs)
            self.firm.apply(FirmControl(
                action=FirmAction(str(r["action"])), origin=ControlOrigin(str(r["origin"])), active=bool(r["active"]),
                restrictions=[msgspec.convert(x, Restriction) for x in rs], trading_day=str(r["trading_day"]),
                actor=r.get("actor"), reason=str(r.get("reason") or ""), cause_alert_key=r.get("cause_alert_key"),
                ts_ns=int(r["ts_ns"])))
        for r in rows("liquidation_plans"):
            steps = r.get("steps") or []
            if isinstance(steps, str):
                steps = json.loads(steps)
            plan = msgspec.convert({
                "plan_id": r["plan_id"], "account_id": r["account_id"], "trading_day": r["trading_day"],
                "trigger_alert_key": r["trigger_alert_key"], "status": r["status"], "steps": steps,
                "created_ns": r["created_ns"], "confirmed_by": r.get("confirmed_by"), "confirmed_ns": r.get("confirmed_ns"),
            }, LiquidationPlan)
            self.plans[plan.plan_id] = plan
        for r in rows("recon_breaks"):
            detail = r.get("detail") or {}
            if isinstance(detail, str):
                detail = json.loads(detail)
            brk = msgspec.convert({**{k: v for k, v in r.items() if v is not None}, "detail": detail,
                                   "blocking": bool(r["blocking"])}, ReconBreakView)
            self.breaks[brk.break_id] = brk
            ra = self._accounts.get(brk.account_id)
            if ra is not None and brk.status == ReconBreakStatus.OPEN and brk.blocking:
                ra.system[SYSTEM_RECON_BREAK] = system_restriction(ra.account_id, SYSTEM_RECON_BREAK, brk.created_ns)
        for r in rows("risk_events"):
            if r["trading_day"] == td:
                self.events.events.append(event_from_row(r))
        for r in rows("processed_commands"):
            raw = r["reply"]
            self._processed[str(r["command_id"])] = (str(r["request_hash"]),
                                                     raw if isinstance(raw, Reply) else decode(bytes(raw), Reply))
        peaks: dict[str, float] = {}
        for r in rows("metric_samples"):
            if r["trading_day"] == td and str(r.get("quality") or MdQuality.OK.value) == MdQuality.OK.value:
                a = str(r["account_id"])  # 峰值只按行情可信时的样本恢复
                peaks[a] = max(peaks.get(a, float("-inf")), float(r["perf_equity"]))
        for a, pk in (intraday_peaks or {}).items():
            peaks[a] = max(peaks.get(a, float("-inf")), pk)
        for a, pk in peaks.items():
            if a in self._accounts:
                self._accounts[a].intraday_peak = max(self._accounts[a].intraday_peak, pk)
        if self._mode == TradingMode.PROVISIONAL:
            forced = any(s.status == EOD_FORCE_UNFROZEN for s in prev_prov)
            if src_day is not None:
                self._prev_day = (src_day, prev_trades, prev_cash)
            for ra in self._accounts.values():
                if not forced:
                    ra.system[SYSTEM_EOD_NOT_SETTLED] = system_restriction(ra.account_id, SYSTEM_EOD_NOT_SETTLED, 0)
        self._next_day_cash.sort(key=lambda c: (c.ts_ns, c.movement_id))
        # 已应答过的换日（重复推送的 eod.rollover 按原结果应答）
        self._dayrolls = {}
        for st in sorted(self._eod.values(), key=lambda s: s.trading_day):
            if st.status != EOD_ROLLED or not st.next_trading_day or st.sod_version is None:
                continue
            if st.provisional_ns is not None:
                mode = DayRollMode.REBASE if st.next_trading_day == td else DayRollMode.CATCH_UP
            else:
                mode = DayRollMode.ROLL
            self._dayrolls[(st.next_trading_day, st.sod_version)] = DayRoll(
                shard=self.shard, mode=mode, from_trading_day=st.trading_day, to_trading_day=st.next_trading_day,
                sod_version=st.sod_version, accounts=sorted(self._accounts), ts_ns=st.rolled_ns or 0)
        for dr in dayrolls:
            self._dayrolls[(dr.to_trading_day, dr.sod_version)] = dr
        self._last_dayroll = self._dayrolls.get((td, version))
        for ra in self._accounts.values():
            self._rereserve_closes(ra)
            self._register_orders(ra)
            ra.dirty = True
        self._rules_touched()
        self._lifecycle = lifecycle

    def post_restore(self) -> list[Effect]:
        """Effects after :meth:`restore` (02 §8.3 进程重启, 01 §6.5).

        Orders of closed days are expired (SESSION_CLOSE); ROUTING → UNKNOWN;
        every working OPEN order of a HALTED account is cancelled (halt guard);
        every UNKNOWN and CANCEL_PENDING order is checked with ``route.resolve``
        (a pending cancel that is still working is then re-sent: the crash may
        have happened before it went out). Warm-up restriction ``SYSTEM:STARTING``;
        account states recomputed (no duplicate alerts).
        """
        tx = Tx(label="post_restore")
        now = self._now()
        self._expire_old_orders(self._trading_day, tx, now)
        for o in self._orders.resting():
            if o.status == OrderStatus.ROUTING:
                set_status(tx, o, OrderStatus.UNKNOWN, now_ns=now, reason=R_RESTART)
            if o.status == OrderStatus.CANCEL_PENDING:
                o.cancel_error = True  # 不知道撤单是否在崩溃前发出：确认后仍在工作就重发
        self._rules_touched()  # new valuations: the quality gating of the rule timers may change
        for a in sorted(self._accounts):
            ra = self._accounts[a]
            ra.valuation = self._value(ra)
            if self._lifecycle == Lifecycle.WARMUP:
                ra.system.setdefault(SYSTEM_STARTING, system_restriction(a, SYSTEM_STARTING, now))
            self._refresh_state(ra, tx, now, cause="SYSTEM")
            if ra.state == AccountState.HALTED:
                for o in self._orders.resting(a):
                    self._halt_guard(ra, o, tx, now)
        for o in self._orders.resting():
            if o.status == OrderStatus.UNKNOWN or (o.status == OrderStatus.CANCEL_PENDING and o.cancel_error):
                self._send_resolve(o, tx, now)
        return tx.effects()
