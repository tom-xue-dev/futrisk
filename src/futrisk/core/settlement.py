"""Daily mark-to-market settlement: a pure library (01 §8.15, §6.6 step 5, §8.16; 02 §7.7-§7.8; 03 §4.4).

:func:`settle_day` turns one trading day's inputs into the full settlement:

    SOD snapshot + trades + cash movements + settlement prices
        -> account / position / margin-group / trade / close-detail / price rows,
           reconciliation checks and the next day's SOD snapshot.

The clearing-worker persists the result in one transaction; the backtester
calls the same function day by day (01 §8.16). There is no I/O, no clock and no
randomness: identical inputs give identical outputs, which makes re-running a
``(trading_day, version)`` idempotent (03 §4.4 版本规则). Every settlement-price
row is stamped with the settlement ``version``, so a ``force_new_version`` rerun
never collides on the ``settlement_prices`` key.

Formulas (01 §8.15, 03 §4.4 column descriptions):

- close P&L: today lot (close − open), yesterday lot (close − pre_settle);
  position P&L: today lot (settle − open), yesterday lot (settle − pre_settle);
- fee: per trade, rounded ROUND_HALF_UP to 0.01 (:mod:`futrisk.core.fees`);
- balance = pre_balance + deposit − withdraw + close_pnl + position_pnl − fee;
  equity = balance; available = equity − margin; risk_degree = margin / equity;
  margin_call = max(0, margin − equity);
- margin: settlement prices × multiplier × qty × company rate with big side
  (:mod:`futrisk.core.margin`);
- performance equity = equity − cum_net_deposit (02 §7.6); nav = perf equity /
  initial capital; hwm_equity = max(previous hwm, perf equity);
  ``max_drawdown`` (03 column name) = the **current** drawdown from that high
  water mark, (hwm − perf equity) / hwm — not a historical maximum.

Checks (02 §7.3 ``ReconCheck``), each comparing independently derived numbers:

- ``EQUITY_IDENTITY``: balance − static equity == exchange-formula P&L − fees
  (the formula comes from trades and prices, not from lots); Σ trade rows'
  close P&L == Σ close-detail rows == account close P&L; Σ trade rows' fees ==
  account fee == Σ position fees; Σ margin-group rows == account margin;
- ``POSITION_IDENTITY``: sod + open − close == eod == remaining lots == next-day
  SOD lots, close-today + close-yesterday == close, slices cover each close;
- ``EXCH_FORMULA``: offset-independent exchange formula == close + position P&L
  per contract;
- ``MTM_VS_FIFO``: close + position == FIFO realized + FIFO float(EOD) − FIFO
  float(SOD);
- ``FIRM_SUM``: Σ account equity / margin / fee against the flat row lists and
  the formula, Σ position eod_qty against all next-day SOD lots.

``TRADE_DIGEST``, ``CASH_DIGEST`` and ``INTRADAY_VS_SETTLE`` need other services'
data and belong to the clearing-worker.

Inputs that cannot be settled raise :class:`SettlementError` (EOD must fail
rather than publish wrong numbers, 02 §8.4 step 5): unknown account or
instrument, a trade or cash movement of another day, duplicate trade keys or
movement ids, cash not exact to the cent, a close exceeding the position, a
settlement price ≤ 0, a held or traded contract without a ``SettlementPriceView``
or without a previous settlement price.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation, localcontext
from typing import Any

import msgspec

from futrisk.common.decimals import D, q_money
from futrisk.core._num import CLEARING_CTX, PRICE_QUANTUM, money_str, price_str, rate_str, ratio
from futrisk.core.fees import DayFeeBook, FeeLegs, TradeFee
from futrisk.core.instruments import InstrumentRegistry, UnknownInstrumentError
from futrisk.core.lots import CloseSlice, CloseType, FillResult, PositionBook, PositionError, PosKey
from futrisk.core.margin import AccountMargin, settlement_margin
from futrisk.core.pnl import exchange_formula_pnl, fifo_close_pnl, fifo_float_pnl, mtm_close_pnl, position_pnl
from futrisk.messages.enums import (
    CashType,
    CheckStatus,
    Exchange,
    FeeType,
    PosDirection,
    ReconCheck,
)
from futrisk.messages.eod import (
    AccountSettlement,
    AccountSettlementSummary,
    CheckResult,
    CloseDetailView,
    MarginGroupView,
    PositionSettlement,
    SettlementCompleted,
    SettlementPriceView,
    SettlementRef,
    SettlementResult,
    SodAccount,
    SodLot,
    SodSnapshot,
    TradeSettlementView,
)
from futrisk.messages.md import SettleInput
from futrisk.messages.orders import Trade
from futrisk.messages.risk import CashMovement

ZERO = Decimal(0)
CENT = Decimal("0.01")


class SettlementError(Exception):
    """The day's inputs are inconsistent and cannot be settled."""


# ---------------------------------------------------------------- results
@dataclass(frozen=True, slots=True)
class TradeResult:
    """One trade after booking, classification and fee computation."""

    trade: Trade
    price: Decimal
    fill: FillResult
    fee: TradeFee
    close_pnl: Decimal  # mark-to-market, exact
    close_pnl_fifo: Decimal  # per-trade, exact

    @property
    def key(self) -> PosKey:
        return self.fill.key


@dataclass(frozen=True, slots=True)
class PositionResult:
    """One ``position_settlements`` row with exact values."""

    account_id: str
    instrument_id: str
    direction: PosDirection
    product: str
    exchange: Exchange
    sod_qty: int
    open_qty: int
    close_qty: int
    close_today_qty: int
    close_yd_qty: int
    eod_qty: int
    lot_qty: int  # Σ remaining lots, cross-checked against eod_qty
    settle_price: Decimal
    pre_settle_price: Decimal
    multiplier: int
    margin_rate: Decimal
    margin_gross: Decimal
    close_pnl: Decimal
    position_pnl: Decimal
    fee: Decimal
    avg_open_price: Decimal | None
    fifo_realized: Decimal
    fifo_float: Decimal
    fifo_float_sod: Decimal

    @property
    def key(self) -> PosKey:
        return (self.account_id, self.instrument_id, self.direction)


@dataclass(frozen=True, slots=True)
class AccountResult:
    """One ``account_settlements`` row with exact values (money quantized to 0.01).

    ``max_drawdown`` keeps the 03 §4.4 column name but means the **current**
    drawdown of performance equity from its high water mark,
    ``(hwm_equity − perf_equity) / hwm_equity`` (see :attr:`hwm_drawdown`); the
    SOD carries no running maximum.
    """

    account_id: str
    pre_balance: Decimal
    deposit: Decimal
    withdraw: Decimal
    close_pnl: Decimal
    position_pnl: Decimal
    fee: Decimal
    balance: Decimal
    equity: Decimal
    margin: Decimal
    available: Decimal
    risk_degree: Decimal
    margin_call: Decimal
    exch_formula_pnl: Decimal
    fifo_realized_pnl: Decimal
    fifo_float_pnl: Decimal
    initial_capital: Decimal
    cum_net_deposit: Decimal
    perf_equity: Decimal
    hwm_equity: Decimal
    nav: Decimal
    max_drawdown: Decimal
    checks_passed: bool

    @property
    def hwm_drawdown(self) -> Decimal:
        """Current drawdown from the high water mark (the value stored as ``max_drawdown``)."""
        return self.max_drawdown

    @property
    def net_deposit(self) -> Decimal:
        return self.deposit - self.withdraw

    @property
    def static_equity(self) -> Decimal:
        """静态权益 = 上日结存 + 当日净入金."""
        return self.pre_balance + self.net_deposit

    @property
    def day_pnl(self) -> Decimal:
        """当日盈亏 = 权益 − 静态权益 (close + position − fee)."""
        return self.equity - self.static_equity


@dataclass(frozen=True)
class DaySettlement:
    """Everything :func:`settle_day` produces for one ``(trading_day, version)``."""

    trading_day: str
    version: int
    next_trading_day: str
    result: SettlementResult
    next_sod: SodSnapshot
    accounts: dict[str, AccountResult]
    positions: list[PositionResult]
    trades: list[TradeResult]
    margins: dict[str, AccountMargin[Decimal]]
    checks: list[CheckResult] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        """No check failed (02 §8.4 step 5: WARN is allowed, FAIL blocks the roll)."""
        return all(c.status != CheckStatus.FAIL for c in self.checks)

    def failed_checks(self) -> list[CheckResult]:
        return [c for c in self.checks if c.status == CheckStatus.FAIL]

    def summaries(self) -> list[AccountSettlementSummary]:
        return [
            AccountSettlementSummary(
                account_id=a.account_id,
                close_pnl=a.close_pnl,
                position_pnl=a.position_pnl,
                fee=a.fee,
                balance=a.balance,
                equity=a.equity,
                margin=a.margin,
                available=a.available,
                risk_degree=a.risk_degree,
                margin_call=a.margin_call,
            )
            for a in self.result.accounts
        ]

    def completed(self, ts_ns: int) -> SettlementCompleted:
        """``clearing.settled`` payload (02 §7.8); ``ts_ns`` is injected, never read from a clock."""
        sources = Counter(p.source.value for p in self.result.prices)
        return SettlementCompleted(
            trading_day=self.trading_day,
            version=self.version,
            next_trading_day=self.next_trading_day,
            accounts=self.summaries(),
            price_sources=dict(sorted(sources.items())),
            checks=list(self.checks),
            ts_ns=ts_ns,
        )


# ---------------------------------------------------------------- helpers
@dataclass(slots=True)
class _Rec:
    trade: Trade
    price: Decimal
    fill: FillResult
    legs: FeeLegs
    close_mtm: Decimal
    close_fifo: Decimal


@dataclass(slots=True)
class _Acct:
    """Account numbers before the checks are known."""

    account_id: str
    pre_balance: Decimal
    deposit: Decimal
    withdraw: Decimal
    close_pnl: Decimal
    position_pnl: Decimal
    fee: Decimal
    balance: Decimal
    margin: Decimal
    initial_capital: Decimal
    cum_net_deposit: Decimal
    perf_equity: Decimal
    hwm_equity: Decimal
    formula_total: Decimal
    formula_detail: list[dict[str, Any]]
    formula_ok: bool
    fifo_realized: Decimal
    fifo_float: Decimal


def _check(
    check: ReconCheck, ok: bool, account_id: str | None, detail: dict[str, Any], *, warn: bool = False
) -> CheckResult:
    status = CheckStatus.PASS if ok else (CheckStatus.WARN if warn else CheckStatus.FAIL)
    return CheckResult(check=check, status=status, account_id=account_id, detail=detail)


def _diff_detail(expected: Decimal, actual: Decimal, **extra: Any) -> dict[str, Any]:
    return {"expected": money_str(expected), "actual": money_str(actual), "diff": money_str(actual - expected), **extra}


def _dsum(values: Iterable[str]) -> Decimal:
    """Σ of DecStr row values (checks read the emitted rows back, independently of the aggregates)."""
    return sum((D(v) for v in values), ZERO)


def seed_sod(
    trading_day: str,
    capital: Mapping[str, Decimal | str | int],
    *,
    version: int = 0,
    pre_settle: Mapping[str, Decimal | str] | None = None,
) -> SodSnapshot:
    """First-day SOD (``settle.seed_sod``): flat accounts, pre_balance = hwm = initial capital."""
    accounts = [
        SodAccount(
            account_id=acct,
            pre_balance=money_str(D(cap)),
            initial_capital=money_str(D(cap)),
            cum_net_deposit="0.00",
            hwm_equity=money_str(D(cap)),
            lots=[],
        )
        for acct, cap in sorted(capital.items())
    ]
    prices = {k: price_str(D(v)) for k, v in sorted((pre_settle or {}).items())}
    return SodSnapshot(trading_day=trading_day, version=version, accounts=accounts, pre_settle=prices)


# ---------------------------------------------------------------- main
def settle_day(
    *,
    trading_day: str,
    version: int,
    sod: SodSnapshot,
    trades: Iterable[Trade],
    cash_movements: Iterable[CashMovement],
    settle_prices: Mapping[str, Decimal | str | SettlementPriceView],
    registry: InstrumentRegistry,
    next_trading_day: str,
    sod_version: int | None = None,
    pre_settle: Mapping[str, Decimal] | None = None,
    settle_inputs: Mapping[str, SettleInput] | None = None,
    require_price_views: bool = True,
) -> DaySettlement:
    """Settle one trading day for every account in ``sod`` (see the module docstring).

    - ``trades``: the day's trades of all accounts; they are applied by
      ``(trade_ts_ns, trade_seq)`` with a stable sort, so trades with equal keys
      keep the order given — pass them in arrival order;
    - ``settle_prices``: per instrument. Every held or traded contract needs a
      ``SettlementPriceView`` (it becomes the ``settlement_prices`` row, re-stamped
      with ``version``); plain prices are accepted for other contracts and only
      feed the next SOD's ``pre_settle``. ``require_price_views=False`` accepts
      plain prices everywhere (no price row is produced for them) — only for
      computations whose rows are never persisted, e.g. the risk engine's
      provisional SOD (02 §8.4);
    - previous settlement prices come from ``sod.pre_settle``, then
      ``pre_settle``, then ``settle_inputs[inst].pre_settle``; a held or traded
      contract without one is an error (never silently today's price);
    - ``sod_version``: version of the generated next-day snapshot (default ``version``).
    """
    with localcontext(CLEARING_CTX):
        return _settle(
            trading_day=trading_day,
            version=version,
            sod=sod,
            trades=list(trades),
            cash=list(cash_movements),
            settle_prices=settle_prices,
            registry=registry,
            next_td=next_trading_day,
            sod_version=version if sod_version is None else sod_version,
            extra_pre=pre_settle or {},
            settle_inputs=settle_inputs or {},
            require_views=require_price_views,
        )


def _settle(
    *,
    trading_day: str,
    version: int,
    sod: SodSnapshot,
    trades: list[Trade],
    cash: list[CashMovement],
    settle_prices: Mapping[str, Decimal | str | SettlementPriceView],
    registry: InstrumentRegistry,
    next_td: str,
    sod_version: int,
    extra_pre: Mapping[str, Decimal],
    settle_inputs: Mapping[str, SettleInput],
    require_views: bool = True,
) -> DaySettlement:
    # ---------------------------------------------------------- 0. validation
    if sod.trading_day != trading_day:
        raise SettlementError(f"SOD snapshot is for {sod.trading_day}, not {trading_day}")
    if next_td <= trading_day:
        raise SettlementError(f"next trading day {next_td} must be after {trading_day}")
    sod_accounts: dict[str, SodAccount] = {}
    for a in sod.accounts:
        if a.account_id in sod_accounts:
            raise SettlementError(f"account {a.account_id} appears twice in the SOD snapshot")
        sod_accounts[a.account_id] = a
    seen_keys: set[str] = set()
    for t in trades:
        if t.trading_day != trading_day:
            raise SettlementError(f"trade {t.trade_key} belongs to {t.trading_day}, not {trading_day}")
        if t.account_id not in sod_accounts:
            raise SettlementError(f"trade {t.trade_key}: account {t.account_id} has no SOD")
        if t.qty <= 0:
            raise SettlementError(f"trade {t.trade_key}: qty must be positive")
        if t.trade_key in seen_keys:
            raise SettlementError(f"duplicate trade key {t.trade_key}")
        seen_keys.add(t.trade_key)
        try:
            exch = registry.exchange(t.instrument_id)
        except UnknownInstrumentError:
            raise SettlementError(f"trade {t.trade_key}: unknown instrument {t.instrument_id}") from None
        if exch != t.exchange:
            raise SettlementError(f"trade {t.trade_key}: {t.instrument_id} trades on {exch}, not {t.exchange}")
    seen_moves: set[str] = set()
    for mv in cash:
        if mv.trading_day != trading_day:
            raise SettlementError(f"cash movement {mv.movement_id} belongs to {mv.trading_day}, not {trading_day}")
        if mv.account_id not in sod_accounts:
            raise SettlementError(f"cash movement {mv.movement_id}: account {mv.account_id} has no SOD")
        if mv.movement_id in seen_moves:
            raise SettlementError(f"duplicate cash movement id {mv.movement_id}")
        seen_moves.add(mv.movement_id)
        try:
            amount = D(mv.amount)
            exact = amount == amount.quantize(CENT)
        except (ValueError, InvalidOperation):
            raise SettlementError(f"cash movement {mv.movement_id}: bad amount {mv.amount!r}") from None
        if amount <= 0:
            raise SettlementError(f"cash movement {mv.movement_id}: amount must be positive")
        if not exact:
            raise SettlementError(f"cash movement {mv.movement_id}: amount {mv.amount} is not exact to the cent")
    prices: dict[str, Decimal] = {}
    views: dict[str, SettlementPriceView] = {}
    for inst, value in sorted(settle_prices.items()):
        if isinstance(value, SettlementPriceView):
            if value.trading_day != trading_day or value.instrument_id != inst:
                raise SettlementError(f"settlement price view for {value.instrument_id}/{value.trading_day} filed under {inst}")
            # settlement_prices 主键含结算版本：按本次结算版本重新盖章，重跑新版本不会撞键
            views[inst] = msgspec.structs.replace(value, version=version)
            px = D(value.price)
        else:
            px = D(value)
        if px <= 0:
            raise SettlementError(f"settlement price of {inst} must be positive, got {px}")
        prices[inst] = px

    # ---------------------------------------------------------- 1. SOD book
    book = PositionBook(registry, trading_day)
    try:
        book.load_sod_accounts(sod.accounts)
    except (ValueError, UnknownInstrumentError) as exc:
        raise SettlementError(f"bad SOD snapshot: {exc}") from exc
    sod_qty: dict[PosKey, int] = {}
    fifo_sod: dict[PosKey, Decimal] = {}
    for pos in book.positions():
        mult = registry.multiplier(pos.instrument_id)
        sod_qty[pos.key] = pos.qty
        # SOD 浮盈（逐笔口径，按昨结算估值）：MTM_VS_FIFO 恒等式的期初项
        fifo_sod[pos.key] = sum(
            ((lot.basis_price - lot.open_price) * lot.qty for lot in pos.lots), ZERO
        ) * mult * pos.sign

    # ------------------------------------------- 2. trades, chronologically
    fee_book = DayFeeBook(registry, trading_day)
    recs: list[_Rec] = []
    # 稳定排序：时间戳与 trade_seq 都相同的成交保持调用方给出的（到达）顺序
    for t in sorted(trades, key=lambda x: (x.trade_ts_ns, x.trade_seq)):
        price = D(t.price)
        try:
            fill = book.apply_trade(t)
        except PositionError as exc:
            raise SettlementError(f"trade {t.trade_key} cannot be applied: {exc}") from exc
        mult = registry.multiplier(t.instrument_id)
        side = fill.pos_direction
        if fill.is_open:
            legs = fee_book.on_fill(fill.key, is_open=True, qty=t.qty, price=price).legs
            close_mtm = close_fifo = ZERO
        else:
            legs = fee_book.on_fill(
                fill.key, is_open=False, qty=t.qty, price=price, today_lot_qty=fill.close_today_qty
            ).legs
            close_mtm = sum((mtm_close_pnl(side, s, price, mult) for s in fill.slices), ZERO)
            close_fifo = sum((fifo_close_pnl(side, s, price, mult) for s in fill.slices), ZERO)
        recs.append(_Rec(t, price, fill, legs, close_mtm, close_fifo))

    # 3. 手续费：全部成交分类完毕后逐笔精确计算（DCE 开仓腿可能被后续平仓改为日内）
    trade_results = [
        TradeResult(
            trade=r.trade,
            price=r.price,
            fill=r.fill,
            fee=fee_book.final_fee(r.trade.instrument_id, r.legs),
            close_pnl=r.close_mtm,
            close_pnl_fifo=r.close_fifo,
        )
        for r in recs
    ]

    # ------------------------------------ 4. settlement and previous prices
    held_yd = {k[1] for k, q in sod_qty.items() if q > 0}
    touched: set[str] = held_yd | {t.instrument_id for t in trades}
    missing = sorted(touched - set(prices))
    if missing:
        raise SettlementError(f"missing settlement price for {', '.join(missing)}")
    bare = sorted(touched - set(views)) if require_views else []
    if bare:
        raise SettlementError(f"settlement price of held/traded {', '.join(bare)} must be a SettlementPriceView")
    pre_map: dict[str, Decimal] = {}
    for inst in sorted(touched):
        if inst in sod.pre_settle:
            pre_map[inst] = D(sod.pre_settle[inst])
        elif inst in extra_pre:
            pre_map[inst] = D(extra_pre[inst])
        elif inst in settle_inputs:
            pre_map[inst] = D(settle_inputs[inst].pre_settle)
        else:
            what = "held yesterday position" if inst in held_yd else "new contract"
            raise SettlementError(f"no previous settlement price for {inst} ({what}); pass pre_settle or settle_inputs")

    # ------------------------------------------------ 5. margin per account
    margins: dict[str, AccountMargin[Decimal]] = {}
    for acct in sorted(sod_accounts):
        held = [(p.instrument_id, p.direction, p.qty) for p in book.positions(acct) if p.qty > 0]
        margins[acct] = settlement_margin(held, prices, registry, trading_day)

    # --------------------------------------------------- 6. position rows
    gross = {acct: m.gross_by_position() for acct, m in margins.items()}
    by_key: dict[PosKey, list[TradeResult]] = {}
    for tr in trade_results:
        by_key.setdefault(tr.key, []).append(tr)
    keys = sorted(set(sod_qty) | set(by_key), key=lambda k: (k[0], k[1], k[2].value))
    positions: list[PositionResult] = []
    for key in keys:
        acct, inst, side = key
        info = registry.get(inst)
        cur = book.find(*key)
        lots = list(cur.lots) if cur is not None else []
        rows = by_key.get(key, [])
        opens = [r for r in rows if r.fill.is_open]
        closes = [r for r in rows if not r.fill.is_open]
        s_px = prices[inst]
        eod = sum(lot.qty for lot in lots)
        avg_open = cur.avg_open_price() if cur is not None and eod else None
        positions.append(
            PositionResult(
                account_id=acct,
                instrument_id=inst,
                direction=side,
                product=info.product,
                exchange=info.exchange,
                sod_qty=sod_qty.get(key, 0),
                open_qty=sum(r.trade.qty for r in opens),
                close_qty=sum(r.trade.qty for r in closes),
                # 03 §4.4：平今 / 平昨手数按交易所的计费规则拆分
                close_today_qty=sum(r.fee.qty_of(FeeType.CLOSE_TODAY, FeeType.INTRADAY_CLOSE) for r in closes),
                close_yd_qty=sum(r.fee.qty_of(FeeType.CLOSE_YD) for r in closes),
                eod_qty=sod_qty.get(key, 0) + sum(r.trade.qty for r in opens) - sum(r.trade.qty for r in closes),
                lot_qty=eod,
                settle_price=s_px,
                pre_settle_price=pre_map[inst],
                multiplier=info.multiplier,
                margin_rate=registry.margin_rate(inst, trading_day),
                margin_gross=gross[acct].get((inst, side), ZERO),
                close_pnl=sum((r.close_pnl for r in closes), ZERO),
                position_pnl=position_pnl(side, lots, s_px, info.multiplier),
                fee=sum((r.fee.fee for r in rows), ZERO),
                avg_open_price=avg_open.quantize(PRICE_QUANTUM) if avg_open is not None else None,
                fifo_realized=sum((r.close_pnl_fifo for r in closes), ZERO),
                fifo_float=fifo_float_pnl(side, lots, s_px, info.multiplier),
                fifo_float_sod=fifo_sod.get(key, ZERO),
            )
        )

    # ------------------------------------------------- 7. account numbers
    cash_by_acct: dict[str, tuple[Decimal, Decimal]] = {}
    for mv in cash:
        dep, wd = cash_by_acct.get(mv.account_id, (ZERO, ZERO))
        amt = D(mv.amount)
        if mv.type == CashType.DEPOSIT:
            dep += amt
        else:
            wd += amt
        cash_by_acct[mv.account_id] = (dep, wd)

    calcs: dict[str, _Acct] = {}
    for acct in sorted(sod_accounts):
        sa = sod_accounts[acct]
        prow = [p for p in positions if p.account_id == acct]
        trows = [r for r in trade_results if r.trade.account_id == acct]
        dep, wd = cash_by_acct.get(acct, (ZERO, ZERO))
        pre_balance = q_money(D(sa.pre_balance))
        close_total = sum((q_money(p.close_pnl) for p in prow), ZERO)
        pos_total = sum((q_money(p.position_pnl) for p in prow), ZERO)
        fee_total = sum((r.fee.fee for r in trows), ZERO)
        balance = pre_balance + dep - wd + close_total + pos_total - fee_total
        cum_net = q_money(D(sa.cum_net_deposit)) + dep - wd
        perf = balance - cum_net
        # EXCH_FORMULA（01 §8.15 第二道校验）：按账户 × 合约，与开平标志无关
        formula_total = ZERO
        formula_detail: list[dict[str, Any]] = []
        formula_ok = True
        for inst in sorted({p.instrument_id for p in prow}):
            fills = [(r.trade.direction, r.price, r.trade.qty) for r in trows if r.trade.instrument_id == inst]
            f_val = exchange_formula_pnl(
                fills,
                settle=prices[inst],
                pre_settle=pre_map[inst],
                yd_long=sod_qty.get((acct, inst, PosDirection.LONG), 0),
                yd_short=sod_qty.get((acct, inst, PosDirection.SHORT), 0),
                multiplier=registry.multiplier(inst),
            )
            mtm = sum((p.close_pnl + p.position_pnl for p in prow if p.instrument_id == inst), ZERO)
            formula_total += f_val
            if f_val != mtm:
                formula_ok = False
            formula_detail.append(
                {"instrument_id": inst, "formula": money_str(f_val), "mtm": money_str(mtm), "diff": money_str(mtm - f_val)}
            )
        calcs[acct] = _Acct(
            account_id=acct,
            pre_balance=pre_balance,
            deposit=dep,
            withdraw=wd,
            close_pnl=close_total,
            position_pnl=pos_total,
            fee=fee_total,
            balance=balance,
            margin=margins[acct].total,
            initial_capital=q_money(D(sa.initial_capital)),
            cum_net_deposit=cum_net,
            perf_equity=perf,
            hwm_equity=max(q_money(D(sa.hwm_equity)), perf),
            formula_total=formula_total,
            formula_detail=formula_detail,
            formula_ok=formula_ok,
            fifo_realized=sum((q_money(p.fifo_realized) for p in prow), ZERO),
            fifo_float=sum((q_money(p.fifo_float) for p in prow), ZERO),
        )

    # ------------------------------------------------------- 8. row views
    position_rows = [_position_view(p, trading_day, version) for p in positions]
    group_rows = [
        MarginGroupView(
            trading_day=trading_day,
            account_id=acct,
            margin_group=g.margin_group,
            version=version,
            method=g.method.value,
            long_margin=money_str(g.long_margin),
            short_margin=money_str(g.short_margin),
            excluded_margin=money_str(g.excluded_margin),
            charged_margin=money_str(g.charged_margin),
            detail=g.detail(),
        )
        for acct in sorted(margins)
        for g in margins[acct].groups
    ]
    trade_rows: list[TradeSettlementView] = []
    detail_rows: list[CloseDetailView] = []
    detail_no: dict[str, int] = {}
    for r in trade_results:
        t = r.trade
        trade_rows.append(
            TradeSettlementView(
                trading_day=trading_day,
                exchange=t.exchange,
                trade_id=t.trade_id,
                direction=t.direction,
                version=version,
                account_id=t.account_id,
                instrument_id=t.instrument_id,
                fee=money_str(r.fee.fee),
                fee_type=r.fee.fee_type,
                fee_rate=price_str(r.fee.fee_rate) if r.fee.fee_rate is not None else None,
                fee_per_lot=money_str(r.fee.fee_per_lot) if r.fee.fee_per_lot is not None else None,
                close_pnl=money_str(r.close_pnl),
                matched_today_qty=r.fill.close_today_qty,
                matched_yd_qty=r.fill.close_yd_qty,
            )
        )
        mult = registry.multiplier(t.instrument_id)
        for s in r.fill.slices:
            n = detail_no.get(t.account_id, 0) + 1
            detail_no[t.account_id] = n
            detail_rows.append(_close_detail_view(t, s, r, n, mult, trading_day, version))

    # --------------------------------------------------- 9. next-day SOD
    try:
        book.roll(next_td, prices)
    except KeyError as exc:  # pragma: no cover - guarded by step 4
        raise SettlementError(str(exc)) from exc
    sod_lots = {acct: book.sod_lots(acct) for acct in sorted(calcs)}
    next_sod = SodSnapshot(
        trading_day=next_td,
        version=sod_version,
        from_settlement=SettlementRef(trading_day=trading_day, version=version),
        accounts=[
            SodAccount(
                account_id=acct,
                pre_balance=money_str(calc.balance),
                initial_capital=money_str(calc.initial_capital),
                cum_net_deposit=money_str(calc.cum_net_deposit),
                hwm_equity=money_str(calc.hwm_equity),
                lots=sod_lots[acct],
            )
            for acct, calc in sorted(calcs.items())
        ],
        pre_settle={inst: price_str(px) for inst, px in sorted(prices.items())},
    )

    # ------------------------------------------------------ 10. checks
    checks: list[CheckResult] = []
    accounts: dict[str, AccountResult] = {}
    account_rows: list[AccountSettlement] = []
    for acct, calc in sorted(calcs.items()):
        acct_checks = _account_checks(
            calc,
            [p for p in positions if p.account_id == acct],
            [r for r in trade_results if r.trade.account_id == acct],
            [x for x in trade_rows if x.account_id == acct],
            [x for x in detail_rows if x.account_id == acct],
            [x for x in group_rows if x.account_id == acct],
            sod_lots[acct],
        )
        checks.extend(acct_checks)
        equity = calc.balance
        res = AccountResult(
            account_id=acct,
            pre_balance=calc.pre_balance,
            deposit=calc.deposit,
            withdraw=calc.withdraw,
            close_pnl=calc.close_pnl,
            position_pnl=calc.position_pnl,
            fee=calc.fee,
            balance=calc.balance,
            equity=equity,
            margin=calc.margin,
            available=equity - calc.margin,
            risk_degree=ratio(calc.margin, equity),
            margin_call=max(ZERO, calc.margin - equity),
            exch_formula_pnl=q_money(calc.formula_total),
            fifo_realized_pnl=calc.fifo_realized,
            fifo_float_pnl=calc.fifo_float,
            initial_capital=calc.initial_capital,
            cum_net_deposit=calc.cum_net_deposit,
            perf_equity=calc.perf_equity,
            hwm_equity=calc.hwm_equity,
            nav=ratio(calc.perf_equity, calc.initial_capital),
            max_drawdown=ratio(calc.hwm_equity - calc.perf_equity, calc.hwm_equity) if calc.hwm_equity > 0 else ratio(ZERO, Decimal(1)),
            checks_passed=all(x.status != CheckStatus.FAIL for x in acct_checks),
        )
        accounts[acct] = res
        account_rows.append(_account_view(res, trading_day, version))
    checks.append(_firm_check(calcs, positions, trade_rows, group_rows, next_sod))

    result = SettlementResult(
        trading_day=trading_day,
        version=version,
        accounts=account_rows,
        positions=position_rows,
        margin_groups=group_rows,
        trades=trade_rows,
        close_details=detail_rows,
        prices=[views[k] for k in sorted(views)],
        checks=list(checks),
    )
    return DaySettlement(
        trading_day=trading_day,
        version=version,
        next_trading_day=next_td,
        result=result,
        next_sod=next_sod,
        accounts=accounts,
        positions=positions,
        trades=trade_results,
        margins=margins,
        checks=checks,
    )


# ---------------------------------------------------------------- checks
def _account_checks(
    c: _Acct,
    prow: list[PositionResult],
    trows: list[TradeResult],
    trade_views: list[TradeSettlementView],
    detail_views: list[CloseDetailView],
    group_views: list[MarginGroupView],
    next_lots: list[SodLot],
) -> list[CheckResult]:
    out: list[CheckResult] = []
    static = c.pre_balance + c.deposit - c.withdraw
    # EQUITY_IDENTITY：结存恒等式；当日盈亏 = 交易所公式 − 手续费；逐行明细与汇总独立相加一致
    trade_close = _dsum(x.close_pnl for x in trade_views)
    detail_close = _dsum(x.close_pnl_mtm for x in detail_views)
    trade_fee = _dsum(x.fee for x in trade_views)
    pos_fee = sum((p.fee for p in prow), ZERO)
    group_margin = _dsum(x.charged_margin for x in group_views)
    day_pnl = c.balance - static
    formula_day = c.formula_total - c.fee
    eq_ok = (
        c.balance == static + c.close_pnl + c.position_pnl - c.fee
        and day_pnl == formula_day
        and trade_close == detail_close == c.close_pnl
        and trade_fee == pos_fee == c.fee
        and group_margin == c.margin
    )
    out.append(
        _check(
            ReconCheck.EQUITY_IDENTITY,
            eq_ok,
            c.account_id,
            _diff_detail(
                formula_day,
                day_pnl,
                close_trades=money_str(trade_close),
                close_details=money_str(detail_close),
                close_account=money_str(c.close_pnl),
                fee_trades=money_str(trade_fee),
                fee_positions=money_str(pos_fee),
                margin_groups=money_str(group_margin),
                margin_account=money_str(c.margin),
            ),
        )
    )
    # POSITION_IDENTITY：期初 + 开 − 平 = 期末 = 剩余 lot = 下一日 SOD；平今 + 平昨 = 平仓
    lots_next: dict[tuple[str, PosDirection], int] = {}
    for lot in next_lots:
        k = (lot.instrument_id, lot.direction)
        lots_next[k] = lots_next.get(k, 0) + lot.qty
    bad: list[dict[str, Any]] = []
    for p in prow:
        nxt = lots_next.pop((p.instrument_id, p.direction), 0)
        if not (
            p.eod_qty == p.lot_qty == p.sod_qty + p.open_qty - p.close_qty == nxt
            and p.close_today_qty + p.close_yd_qty == p.close_qty
            and p.eod_qty >= 0
        ):
            bad.append(
                {
                    "instrument_id": p.instrument_id,
                    "direction": p.direction.value,
                    "sod": p.sod_qty,
                    "open": p.open_qty,
                    "close": p.close_qty,
                    "close_today": p.close_today_qty,
                    "close_yd": p.close_yd_qty,
                    "eod": p.eod_qty,
                    "lots": p.lot_qty,
                    "next_sod": nxt,
                }
            )
    for (inst, direction), qty in sorted(lots_next.items(), key=lambda kv: (kv[0][0], kv[0][1].value)):
        bad.append({"instrument_id": inst, "direction": direction.value, "next_sod": qty, "eod": 0})
    for r in trows:
        if not r.fill.is_open and sum(s.qty for s in r.fill.slices) != r.trade.qty:
            bad.append({"trade_key": r.trade.trade_key, "matched": sum(s.qty for s in r.fill.slices)})
    out.append(_check(ReconCheck.POSITION_IDENTITY, not bad, c.account_id, {"positions": len(prow), "violations": bad}))
    mtm_total = sum((p.close_pnl + p.position_pnl for p in prow), ZERO)
    out.append(
        _check(
            ReconCheck.EXCH_FORMULA,
            c.formula_ok,
            c.account_id,
            {**_diff_detail(c.formula_total, mtm_total), "instruments": c.formula_detail},
        )
    )
    fifo_side = sum((p.fifo_realized + p.fifo_float - p.fifo_float_sod for p in prow), ZERO)
    out.append(
        _check(
            ReconCheck.MTM_VS_FIFO,
            fifo_side == mtm_total,
            c.account_id,
            _diff_detail(
                fifo_side,
                mtm_total,
                fifo_realized=money_str(sum((p.fifo_realized for p in prow), ZERO)),
                fifo_float=money_str(sum((p.fifo_float for p in prow), ZERO)),
                fifo_float_sod=money_str(sum((p.fifo_float_sod for p in prow), ZERO)),
            ),
        )
    )
    return out


def _firm_check(
    calcs: Mapping[str, _Acct],
    positions: list[PositionResult],
    trade_views: list[TradeSettlementView],
    group_views: list[MarginGroupView],
    next_sod: SodSnapshot,
) -> CheckResult:
    """FIRM_SUM：各账户之和与全部明细行、交易所公式、下一日 SOD 独立汇总的公司层数字一致."""
    rows = list(calcs.values())
    equity = sum((a.balance for a in rows), ZERO)
    expected_equity = (
        sum((a.pre_balance + a.deposit - a.withdraw for a in rows), ZERO)
        + sum((a.formula_total for a in rows), ZERO)
        - _dsum(x.fee for x in trade_views)
    )
    margin = sum((a.margin for a in rows), ZERO)
    group_margin = _dsum(x.charged_margin for x in group_views)
    fee = sum((a.fee for a in rows), ZERO)
    eod_qty = sum(p.eod_qty for p in positions)
    sod_qty = sum(lot.qty for acc in next_sod.accounts for lot in acc.lots)
    ok = equity == expected_equity and margin == group_margin and fee == _dsum(x.fee for x in trade_views) and eod_qty == sod_qty
    return _check(
        ReconCheck.FIRM_SUM,
        ok,
        None,
        _diff_detail(
            expected_equity,
            equity,
            accounts=len(rows),
            margin=money_str(margin),
            margin_groups=money_str(group_margin),
            fee=money_str(fee),
            eod_qty=eod_qty,
            next_sod_qty=sod_qty,
        ),
    )


# ---------------------------------------------------------------- views
def _account_view(a: AccountResult, trading_day: str, version: int) -> AccountSettlement:
    return AccountSettlement(
        trading_day=trading_day,
        account_id=a.account_id,
        version=version,
        pre_balance=money_str(a.pre_balance),
        deposit=money_str(a.deposit),
        withdraw=money_str(a.withdraw),
        close_pnl=money_str(a.close_pnl),
        position_pnl=money_str(a.position_pnl),
        fee=money_str(a.fee),
        balance=money_str(a.balance),
        equity=money_str(a.equity),
        margin=money_str(a.margin),
        available=money_str(a.available),
        risk_degree=rate_str(a.risk_degree),
        margin_call=money_str(a.margin_call),
        exch_formula_pnl=money_str(a.exch_formula_pnl),
        fifo_realized_pnl=money_str(a.fifo_realized_pnl),
        fifo_float_pnl=money_str(a.fifo_float_pnl),
        initial_capital=money_str(a.initial_capital),
        cum_net_deposit=money_str(a.cum_net_deposit),
        hwm_equity=money_str(a.hwm_equity),
        nav=rate_str(a.nav),
        max_drawdown=rate_str(a.max_drawdown),
        checks_passed=a.checks_passed,
    )


def _position_view(p: PositionResult, trading_day: str, version: int) -> PositionSettlement:
    return PositionSettlement(
        trading_day=trading_day,
        account_id=p.account_id,
        instrument_id=p.instrument_id,
        direction=p.direction,
        version=version,
        product=p.product,
        exchange=p.exchange,
        sod_qty=p.sod_qty,
        open_qty=p.open_qty,
        close_qty=p.close_qty,
        close_today_qty=p.close_today_qty,
        close_yd_qty=p.close_yd_qty,
        eod_qty=p.eod_qty,
        settle_price=price_str(p.settle_price),
        pre_settle_price=price_str(p.pre_settle_price),
        multiplier=p.multiplier,
        margin_rate=price_str(p.margin_rate),
        margin_gross=money_str(p.margin_gross),
        close_pnl=money_str(p.close_pnl),
        position_pnl=money_str(p.position_pnl),
        fee=money_str(p.fee),
        avg_open_price=price_str(p.avg_open_price) if p.avg_open_price is not None else None,
    )


def _close_detail_view(
    t: Trade, s: CloseSlice, r: TradeResult, n: int, mult: int, trading_day: str, version: int
) -> CloseDetailView:
    side = r.fill.pos_direction
    return CloseDetailView(
        trading_day=trading_day,
        version=version,
        account_id=t.account_id,
        detail_no=n,
        instrument_id=t.instrument_id,
        exchange=t.exchange,
        trade_id=t.trade_id,
        direction=t.direction,
        close_type=s.close_type.value,
        lot_open_td=s.lot_open_td,
        lot_open_ts_ns=s.lot_open_ts_ns,
        lot_open_price=price_str(s.lot_open_price),
        basis_price=price_str(s.lot_open_price if s.close_type == CloseType.TODAY else s.basis_price),
        close_price=price_str(r.price),
        qty=s.qty,
        close_pnl_mtm=money_str(mtm_close_pnl(side, s, r.price, mult)),
        close_pnl_fifo=money_str(fifo_close_pnl(side, s, r.price, mult)),
    )


__all__ = [
    "AccountResult",
    "DaySettlement",
    "PositionResult",
    "SettlementError",
    "TradeResult",
    "seed_sod",
    "settle_day",
]
