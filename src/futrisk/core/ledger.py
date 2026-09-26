"""Intraday account ledger for the risk engine (01 §8.1 Ledger, §8.12, §8.14; 02 §7.6 AccountFunds / PositionView).

:class:`IntradayLedger` maintains one account's funds for one trading day from
the SOD snapshot, fills, cash movements, marks and resting orders, with the
02 §7.6 ``AccountFunds`` definitions:

    static_equity = pre_balance + net_deposit
    equity        = static_equity + close_pnl + position_pnl − fee
    available     = equity − margin − frozen_margin − frozen_fee
    risk_degree   = margin / equity

It reuses the clearing primitives (position book, fee classification, big-side
margin rule), so the intraday numbers converge to the EOD settlement when the
marks equal the settlement prices. A new trading day starts a new ledger from
the new SOD; :meth:`refresh_rates` re-reads cached rates after a config reload.

Hot path (01 §8.12, pre-trade budget ~50 µs): everything a tick or a pre-trade
check touches is cached as float per instrument and per margin group:

- per instrument: rate, multiplier, fee rates, long/short quantity and Σ basis
  × qty / Σ open × qty, the P&L slope and intercept (``pnl = slope × mark −
  intercept``) and the gross long/short margin at the mark;
- per margin group: long / short / excluded sums, the charged margin
  (:func:`futrisk.core.margin.group_charge`) and the frozen margin of the
  registered resting orders.

:meth:`set_mark` revalues only the ticked instrument and its group (no
``Decimal``, no registry lookups); :meth:`funds` sums the cached per-instrument
and per-group values; :meth:`order_cost` prices one more resting order against
the cached group sums (big-side aware). Fills rebuild the touched instrument's
aggregates from its lots (rare compared with ticks).

Float vs Decimal (01 §8.3, ADR-009): cash, close P&L of fills and fees stay
``Decimal`` (they reconcile with the EOD settlement exactly, except the DCE
intraday fee estimate's rounding) and are mirrored as floats for the hot path;
marks, position P&L, margin, frozen amounts and ratios are float64.

Resting orders: either register them with :meth:`add_order` / :meth:`update_order`
/ :meth:`remove_order` (frozen amounts then follow marks incrementally), or
compute them externally with :meth:`frozen_for_orders` and :meth:`set_frozen`.
Reported frozen amounts are the sum of both mechanisms; use one.

The ledger is pure: no clock, no I/O; the engine feeds events in order. Fills
and cash of another trading day are rejected; a repeated trade key or movement
id is ignored (idempotent replay).
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, replace
from decimal import Decimal

from futrisk.common.decimals import D
from futrisk.core.fees import DayFeeBook, FillFee, FloatFeeRates
from futrisk.core.instruments import InstrumentRegistry, MarginMethod
from futrisk.core.lots import FillResult, Position, PositionBook, PosKey, pos_direction
from futrisk.core.margin import AccountMargin, ContractMargin, MarginGroup, group_charge
from futrisk.core.pnl import mtm_close_pnl
from futrisk.messages.enums import CashType, Direction, Offset, PosDirection
from futrisk.messages.eod import SodAccount
from futrisk.messages.orders import Trade
from futrisk.messages.risk import AccountFunds, CashMovement, PositionView

RISK_DEGREE_CAP = 9999.99999999


@dataclass(frozen=True, slots=True)
class LedgerFill:
    """Effect of one fill on the ledger (``fills`` row values, 03 §4.3).

    ``duplicate`` is True when the trade key was already applied; the ledger was
    not changed and the original effect is returned.
    """

    fill: FillResult
    fee: FillFee
    close_pnl: Decimal  # mark-to-market close P&L, exact
    duplicate: bool = False

    @property
    def fee_est(self) -> Decimal:
        return self.fee.estimate

    @property
    def close_today_qty(self) -> int:
        return self.fill.close_today_qty

    @property
    def close_yd_qty(self) -> int:
        return self.fill.close_yd_qty


@dataclass(frozen=True, slots=True)
class RestingOrder:
    """Unfilled remainder of a working order, for frozen margin / fee and pre-trade cost."""

    instrument_id: str
    direction: Direction
    offset: Offset
    qty: int
    price: float


class _Group:
    """Cached sums of one margin group (``margin_groups.margin_group``)."""

    __slots__ = (
        "charged",
        "excl_sum",
        "frozen",
        "group_id",
        "long_sum",
        "members",
        "method",
        "o_excl",
        "o_long",
        "o_short",
        "orders",
        "short_sum",
    )

    def __init__(self, group_id: str, method: MarginMethod) -> None:
        self.group_id = group_id
        self.method = method
        self.members: list[_Inst] = []
        self.long_sum = 0.0
        self.short_sum = 0.0
        self.excl_sum = 0.0
        self.charged = 0.0
        self.orders: dict[str, tuple[int, float]] = {}  # order_id -> (bucket 0 long / 1 short / 2 excluded, margin)
        self.o_long = 0.0
        self.o_short = 0.0
        self.o_excl = 0.0
        self.frozen = 0.0


class _Inst:
    """Cached valuation state of one instrument in this account."""

    __slots__ = (
        "excluded",
        "explicit_mark",
        "fee_rates",
        "fee_rule",
        "group",
        "instrument_id",
        "intercept",
        "long_basis",
        "long_margin",
        "long_open",
        "long_qty",
        "mark",
        "mult",
        "pnl",
        "rate",
        "short_basis",
        "short_margin",
        "short_open",
        "short_qty",
        "slope",
    )

    def __init__(
        self,
        instrument_id: str,
        mult: float,
        rate: float,
        group: _Group,
        excluded: bool,
        fee_rates: FloatFeeRates,
        fee_rule: str,
    ) -> None:
        self.instrument_id = instrument_id
        self.mult = mult
        self.rate = rate
        self.group = group
        self.excluded = excluded
        self.fee_rates = fee_rates
        self.fee_rule = fee_rule
        self.mark = 0.0
        self.explicit_mark = False
        self.long_qty = 0
        self.short_qty = 0
        self.long_basis = 0.0
        self.short_basis = 0.0
        self.long_open = 0.0
        self.short_open = 0.0
        self.slope = 0.0
        self.intercept = 0.0
        self.pnl = 0.0
        self.long_margin = 0.0
        self.short_margin = 0.0


class IntradayLedger:
    """One account's intraday position book and funds (see the module docstring)."""

    def __init__(
        self,
        registry: InstrumentRegistry,
        trading_day: str,
        sod_account: SodAccount,
        pre_settle: Mapping[str, Decimal | str] | None = None,
    ) -> None:
        self.registry = registry
        self.trading_day = trading_day
        self.account_id = sod_account.account_id
        self.initial_capital = D(sod_account.initial_capital)
        self.pre_balance = D(sod_account.pre_balance)
        self.cum_net_deposit_sod = D(sod_account.cum_net_deposit)
        self.hwm_equity_sod = D(sod_account.hwm_equity)
        self.pre_settle: dict[str, Decimal] = {k: D(v) for k, v in (pre_settle or {}).items()}
        self.book = PositionBook(registry, trading_day)
        self.book.load_sod_account(sod_account)
        self.fees = DayFeeBook(registry, trading_day)
        self.net_deposit = Decimal(0)
        self.close_pnl = Decimal(0)
        self.fee = Decimal(0)
        # 外部计算后经 set_frozen 设置的冻结金额（与登记挂单的冻结金额相加后报告）
        self.frozen_margin = 0.0
        self.frozen_fee = 0.0
        self._static_f = float(self.pre_balance)
        self._net_dep_f = 0.0
        self._close_f = 0.0
        self._fee_f = 0.0
        self._marks: dict[str, float] = {}
        self._last_fill_px: dict[str, float] = {}
        self._inst: dict[str, _Inst] = {}
        self._inst_list: list[_Inst] = []  # sorted by instrument id
        self._groups: dict[str, _Group] = {}
        self._group_list: list[_Group] = []
        self._orders: dict[str, tuple[RestingOrder, _Inst, float]] = {}  # id -> (order, state, fee)
        self._orders_fee = 0.0
        self._fills: dict[str, LedgerFill] = {}
        self._cash_ids: set[str] = set()
        for pos in self.book.positions(self.account_id):
            s = self._state(pos.instrument_id)
            self._refresh_side(s, pos.direction)
        for s in self._inst_list:
            self._init_mark(s)
            self._revalue(s)

    # ------------------------------------------------------------- state
    def _state(self, instrument_id: str) -> _Inst:
        s = self._inst.get(instrument_id)
        if s is not None:
            return s
        info = self.registry.get(instrument_id)
        g = self._groups.get(info.margin_group)
        if g is None:
            g = _Group(info.margin_group, info.margin_method)
            self._groups[info.margin_group] = g
            self._group_list.append(g)
            self._group_list.sort(key=lambda x: x.group_id)
        s = _Inst(
            instrument_id,
            float(info.multiplier),
            float(self.registry.margin_rate(instrument_id, self.trading_day)),
            g,
            info.margin_method == MarginMethod.BIG_SIDE
            and self.registry.big_side_excluded(instrument_id, self.trading_day, intraday=True),
            FloatFeeRates.from_schedule(self.fees.schedule(instrument_id), info.multiplier),
            info.rules.close_today_fee_rule,
        )
        g.members.append(s)
        g.members.sort(key=lambda x: x.instrument_id)
        self._inst[instrument_id] = s
        self._inst_list.append(s)
        self._inst_list.sort(key=lambda x: x.instrument_id)
        self._init_mark(s)
        return s

    def _init_mark(self, s: _Inst) -> None:
        """Mark before the first tick: explicit mark > pre-settlement > lot basis > last fill price."""
        inst = s.instrument_id
        if inst in self._marks:
            s.mark, s.explicit_mark = self._marks[inst], True
            return
        s.explicit_mark = False
        if inst in self.pre_settle:
            s.mark = float(self.pre_settle[inst])
        elif s.long_qty + s.short_qty > 0:
            s.mark = (s.long_basis + s.short_basis) / (s.long_qty + s.short_qty)
        elif inst in self._last_fill_px:
            s.mark = self._last_fill_px[inst]

    def _refresh_side(self, s: _Inst, side: PosDirection) -> None:
        pos = self.book.find(self.account_id, s.instrument_id, side)
        qty = pos.qty if pos is not None else 0
        basis = float(pos.basis_notional) if pos is not None and qty else 0.0
        opened = float(pos.open_notional) if pos is not None and qty else 0.0
        if side == PosDirection.LONG:
            s.long_qty, s.long_basis, s.long_open = qty, basis, opened
        else:
            s.short_qty, s.short_basis, s.short_open = qty, basis, opened
        s.slope = s.mult * (s.long_qty - s.short_qty)
        s.intercept = s.mult * (s.long_basis - s.short_basis)

    def _revalue(self, s: _Inst) -> None:
        m = s.mark
        s.pnl = s.slope * m - s.intercept
        # 与 margin._compute 相同的乘法顺序：价格 × 乘数 × 手数 × 保证金率
        s.long_margin = m * s.mult * s.long_qty * s.rate
        s.short_margin = m * s.mult * s.short_qty * s.rate
        self._regroup(s.group)

    @staticmethod
    def _regroup(g: _Group) -> None:
        long_sum = short_sum = excl = 0.0
        for s in g.members:
            if s.excluded:
                excl += s.long_margin
                excl += s.short_margin
            else:
                long_sum += s.long_margin
                short_sum += s.short_margin
        g.long_sum, g.short_sum, g.excl_sum = long_sum, short_sum, excl
        g.charged = group_charge(g.method, long_sum, short_sum, excl)
        if g.orders:
            g.frozen = group_charge(g.method, long_sum + g.o_long, short_sum + g.o_short, excl + g.o_excl) - g.charged
        else:
            g.frozen = 0.0

    # ------------------------------------------------------------- events
    def apply_fill(self, trade: Trade, *, lenient: bool = False) -> LedgerFill:
        """Book a fill (chronological order); raises ``PositionError`` if a close exceeds the position.

        A fill of another account or trading day raises ``ValueError``; a trade
        key already applied returns the original effect with ``duplicate=True``.
        """
        if trade.account_id != self.account_id:
            raise ValueError(f"fill of {trade.account_id} applied to ledger {self.account_id}")
        if trade.trading_day != self.trading_day:
            raise ValueError(f"fill {trade.trade_key} of {trade.trading_day} applied to ledger of {self.trading_day}")
        seen = self._fills.get(trade.trade_key)
        if seen is not None:
            return replace(seen, duplicate=True)
        price = D(trade.price)
        fill = self.book.apply_trade(trade, lenient=lenient)
        if fill.is_open:
            fee = self.fees.on_fill(fill.key, is_open=True, qty=trade.qty, price=price)
            close = Decimal(0)
        else:
            fee = self.fees.on_fill(
                fill.key, is_open=False, qty=trade.qty, price=price, today_lot_qty=fill.close_today_qty
            )
            mult = self.registry.multiplier(trade.instrument_id)
            close = sum((mtm_close_pnl(fill.pos_direction, sl, price, mult) for sl in fill.slices), Decimal(0))
        self.close_pnl += close
        self.fee += fee.estimate
        self._close_f = float(self.close_pnl)
        self._fee_f = float(self.fee)
        self._last_fill_px[trade.instrument_id] = float(trade.price)
        s = self._state(trade.instrument_id)
        self._refresh_side(s, fill.pos_direction)
        if not s.explicit_mark:
            self._init_mark(s)
        self._revalue(s)
        result = LedgerFill(fill=fill, fee=fee, close_pnl=close)
        self._fills[trade.trade_key] = result
        return result

    def apply_cash(self, movement: CashMovement) -> bool:
        """Apply a cash movement of this trading day; returns False for a repeated ``movement_id``."""
        if movement.account_id != self.account_id:
            raise ValueError(f"cash of {movement.account_id} applied to ledger {self.account_id}")
        if movement.trading_day != self.trading_day:
            raise ValueError(
                f"cash movement {movement.movement_id} of {movement.trading_day} applied to ledger of {self.trading_day}"
            )
        if movement.movement_id in self._cash_ids:
            return False
        self._cash_ids.add(movement.movement_id)
        amt = D(movement.amount)
        self.net_deposit += amt if movement.type == CashType.DEPOSIT else -amt
        self._net_dep_f = float(self.net_deposit)
        self._static_f = float(self.pre_balance + self.net_deposit)
        return True

    def set_mark(self, instrument_id: str, price: float) -> None:
        """Latest valuation price (last price; the limit price when locked, 02 §7.6). O(group size)."""
        self._marks[instrument_id] = price
        s = self._inst.get(instrument_id)
        if s is not None:
            s.mark = price
            s.explicit_mark = True
            self._revalue(s)

    def set_frozen(self, *, margin: float, fee: float) -> None:
        """Frozen amounts computed outside the ledger (added to those of registered orders)."""
        self.frozen_margin = margin
        self.frozen_fee = fee

    def refresh_rates(self, registry: InstrumentRegistry | None = None) -> None:
        """Re-read cached margin / fee rates and exclusions (after a config reload), then revalue."""
        if registry is not None:
            self.registry = registry
            self.book.set_registry(registry)
            self.fees.set_registry(registry)
        for s in self._inst_list:
            info = self.registry.get(s.instrument_id)
            s.rate = float(self.registry.margin_rate(s.instrument_id, self.trading_day))
            s.excluded = info.margin_method == MarginMethod.BIG_SIDE and self.registry.big_side_excluded(
                s.instrument_id, self.trading_day, intraday=True
            )
            s.fee_rates = FloatFeeRates.from_schedule(self.fees.schedule(s.instrument_id), info.multiplier)
            s.fee_rule = info.rules.close_today_fee_rule
        for oid, (order, _s, _fee) in list(self._orders.items()):
            self.add_order(oid, order)
        for s in self._inst_list:
            self._revalue(s)

    # ------------------------------------------------------ resting orders
    def _order_bucket(self, s: _Inst, side: PosDirection) -> int:
        if s.excluded:
            return 2
        return 0 if side == PosDirection.LONG else 1

    def _close_fee(self, s: _Inst, side: PosDirection, order: RestingOrder) -> float:
        key = (self.account_id, s.instrument_id, side)
        pos = self.book.find(*key)
        today = pos.preview_close(order.offset, order.qty)[0] if pos is not None else 0
        legs = self.fees.classifier.preview_close(
            key, order.qty, s.fee_rule, today_lot_qty=today, intraday=s.fee_rates.intraday is not None
        )
        return s.fee_rates.fee(legs.items(), order.price)

    def order_cost(self, order: RestingOrder) -> tuple[float, float]:
        """``(margin_delta, fee)`` of one more resting order, for the pre-trade funds check.

        The margin delta is the increase of the group's charged margin (big-side
        aware, valued at the order price) on top of the positions **and** the
        registered resting orders; a small-side SHFE order costs 0 until it would
        flip the big side. Closing orders return a margin delta of 0 (closing
        releases margin; 02 §11.2 exempts them from the funds check) and their
        estimated fee. Fees are rounded per trade like the settlement.
        """
        if order.qty <= 0:
            return 0.0, 0.0
        side = pos_direction(order.direction, order.offset)
        s = self._state(order.instrument_id)
        if order.offset != Offset.OPEN:
            return 0.0, self._close_fee(s, side, order)
        g = s.group
        m = order.price * s.mult * order.qty * s.rate
        long_sum = g.long_sum + g.o_long
        short_sum = g.short_sum + g.o_short
        excl = g.excl_sum + g.o_excl
        before = group_charge(g.method, long_sum, short_sum, excl)
        bucket = self._order_bucket(s, side)
        if bucket == 2:
            after = group_charge(g.method, long_sum, short_sum, excl + m)
        elif bucket == 0:
            after = group_charge(g.method, long_sum + m, short_sum, excl)
        else:
            after = group_charge(g.method, long_sum, short_sum + m, excl)
        return after - before, s.fee_rates.open_fee(order.price, order.qty)

    def add_order(self, order_id: str, order: RestingOrder) -> None:
        """Register (or replace) a resting order; its frozen margin and fee are maintained incrementally."""
        if order_id in self._orders:
            self.remove_order(order_id)
        if order.qty <= 0:
            return
        side = pos_direction(order.direction, order.offset)
        s = self._state(order.instrument_id)
        if order.offset == Offset.OPEN:
            fee = s.fee_rates.open_fee(order.price, order.qty)
            s.group.orders[order_id] = (self._order_bucket(s, side), order.price * s.mult * order.qty * s.rate)
            self._resum_orders(s.group)
        else:
            fee = self._close_fee(s, side, order)
        self._orders[order_id] = (order, s, fee)
        self._orders_fee = sum(f for _, _, f in self._orders.values())

    def update_order(self, order_id: str, qty: int) -> None:
        """Change the unfilled quantity of a registered order (partial fill); 0 removes it."""
        entry = self._orders.get(order_id)
        if entry is None:
            return
        if qty <= 0:
            self.remove_order(order_id)
            return
        self.add_order(order_id, replace(entry[0], qty=qty))

    def remove_order(self, order_id: str) -> bool:
        """Unregister a resting order (filled, cancelled or rejected); False if unknown."""
        entry = self._orders.pop(order_id, None)
        if entry is None:
            return False
        g = entry[1].group
        if g.orders.pop(order_id, None) is not None:
            self._resum_orders(g)
        self._orders_fee = sum(f for _, _, f in self._orders.values())
        return True

    def _resum_orders(self, g: _Group) -> None:
        sums = [0.0, 0.0, 0.0]
        for bucket, m in g.orders.values():
            sums[bucket] += m
        g.o_long, g.o_short, g.o_excl = sums
        self._regroup(g)

    @property
    def orders_frozen_margin(self) -> float:
        return sum(g.frozen for g in self._group_list)

    @property
    def orders_frozen_fee(self) -> float:
        return self._orders_fee

    def frozen_for_orders(self, orders: Iterable[RestingOrder]) -> tuple[float, float]:
        """``(frozen_margin, frozen_fee)`` of an explicit list of resting orders (not registered).

        Open orders freeze the incremental big-side margin on top of the
        positions (see :func:`futrisk.core.margin.frozen_margin`) and their open
        fee; close orders freeze only their fee (closing releases margin).
        """
        add: dict[str, list[float]] = {}
        fee = 0.0
        for o in orders:
            if o.qty <= 0:
                continue
            side = pos_direction(o.direction, o.offset)
            s = self._state(o.instrument_id)
            if o.offset == Offset.OPEN:
                sums = add.setdefault(s.group.group_id, [0.0, 0.0, 0.0])
                sums[self._order_bucket(s, side)] += o.price * s.mult * o.qty * s.rate
                fee += s.fee_rates.open_fee(o.price, o.qty)
            else:
                fee += self._close_fee(s, side, o)
        frozen = 0.0
        for gid, (a, b, c) in add.items():
            g = self._groups[gid]
            frozen += group_charge(g.method, g.long_sum + a, g.short_sum + b, g.excl_sum + c) - g.charged
        return max(0.0, frozen), fee

    # ---------------------------------------------------------- valuation
    def mark(self, instrument_id: str) -> float:
        """Mark used for valuation: latest mark > pre-settlement > lot basis > last fill price."""
        s = self._inst.get(instrument_id)
        if s is not None and (s.explicit_mark or s.long_qty or s.short_qty or instrument_id in self.pre_settle):
            return s.mark
        if instrument_id in self._marks:
            return self._marks[instrument_id]
        if instrument_id in self.pre_settle:
            return float(self.pre_settle[instrument_id])
        if instrument_id in self._last_fill_px:
            return self._last_fill_px[instrument_id]
        raise KeyError(f"no mark for {instrument_id}")

    def instruments(self) -> list[str]:
        return sorted(s.instrument_id for s in self._inst_list if s.long_qty or s.short_qty)

    def position_pnl_of(self, key: PosKey) -> float:
        s = self._inst.get(key[1])
        if s is None:
            return 0.0
        if key[2] == PosDirection.LONG:
            return s.mult * (s.mark * s.long_qty - s.long_basis) if s.long_qty else 0.0
        return -s.mult * (s.mark * s.short_qty - s.short_basis) if s.short_qty else 0.0

    def fifo_float_of(self, key: PosKey) -> float:
        s = self._inst.get(key[1])
        if s is None:
            return 0.0
        if key[2] == PosDirection.LONG:
            return s.mult * (s.mark * s.long_qty - s.long_open) if s.long_qty else 0.0
        return -s.mult * (s.mark * s.short_qty - s.short_open) if s.short_qty else 0.0

    def position_pnl(self) -> float:
        """持仓盯市盈亏 at the current marks."""
        total = 0.0
        for s in self._inst_list:
            total += s.pnl
        return total

    def held(self) -> list[tuple[str, PosDirection, int]]:
        """Non-flat positions as ``(instrument_id, direction, qty)``, sorted."""
        out: list[tuple[str, PosDirection, int]] = []
        for s in self._inst_list:
            if s.long_qty:
                out.append((s.instrument_id, PosDirection.LONG, s.long_qty))
            if s.short_qty:
                out.append((s.instrument_id, PosDirection.SHORT, s.short_qty))
        return out

    def margin(self) -> AccountMargin[float]:
        """Intraday margin with big side at the current marks, from the cached group sums."""
        groups: list[MarginGroup[float]] = []
        total = 0.0
        for g in self._group_list:
            rows: list[ContractMargin[float]] = []
            for s in g.members:
                if s.long_qty:
                    rows.append(ContractMargin(s.instrument_id, PosDirection.LONG, s.long_qty, s.mark, s.rate, s.long_margin, s.excluded))
                if s.short_qty:
                    rows.append(
                        ContractMargin(s.instrument_id, PosDirection.SHORT, s.short_qty, s.mark, s.rate, s.short_margin, s.excluded)
                    )
            if not rows:
                continue
            groups.append(MarginGroup(g.group_id, g.method, g.long_sum, g.short_sum, g.excl_sum, g.charged, tuple(rows)))
            total += g.charged
        return AccountMargin(tuple(groups), total)

    def funds(self) -> AccountFunds:
        """02 §7.6 ``AccountFunds`` (float, estimate; clearing is authoritative). O(instruments + groups)."""
        pnl = 0.0
        for s in self._inst_list:
            pnl += s.pnl
        margin = 0.0
        orders_frozen = 0.0
        for g in self._group_list:
            margin += g.charged
            orders_frozen += g.frozen
        frozen_margin = self.frozen_margin + orders_frozen
        frozen_fee = self.frozen_fee + self._orders_fee
        static = self._static_f
        equity = static + self._close_f + pnl - self._fee_f
        if equity > 0:
            risk = min(margin / equity, RISK_DEGREE_CAP)
        else:
            risk = RISK_DEGREE_CAP if margin > 0 else 0.0
        return AccountFunds(
            initial_capital=float(self.initial_capital),
            cum_net_deposit=float(self.cum_net_deposit_sod) + self._net_dep_f,
            pre_balance=float(self.pre_balance),
            net_deposit=self._net_dep_f,
            static_equity=static,
            close_pnl=self._close_f,
            position_pnl=pnl,
            fee=self._fee_f,
            equity=equity,
            margin=margin,
            frozen_margin=frozen_margin,
            frozen_fee=frozen_fee,
            available=equity - margin - frozen_margin - frozen_fee,
            risk_degree=risk,
        )

    def position_views(
        self, *, equity: float | None = None, limit_locked: Iterable[str] = ()
    ) -> list[PositionView]:
        """02 §7.6 ``PositionView`` rows for every non-flat position (cached values, one funds() at most)."""
        locked = set(limit_locked)
        eq = self.funds().equity if equity is None else equity
        out: list[PositionView] = []
        for s in self._inst_list:
            for direction in (PosDirection.LONG, PosDirection.SHORT):
                long_side = direction == PosDirection.LONG
                qty = s.long_qty if long_side else s.short_qty
                if qty == 0:
                    continue
                pos = self.book.find(self.account_id, s.instrument_id, direction)
                if pos is None:  # pragma: no cover - aggregates are rebuilt from the book
                    continue
                out.append(self._view(s, pos, long_side, qty, eq, s.instrument_id in locked))
        return out

    def _view(self, s: _Inst, pos: Position, long_side: bool, qty: int, eq: float, locked: bool) -> PositionView:
        basis = s.long_basis if long_side else s.short_basis
        opened = s.long_open if long_side else s.short_open
        sign = 1 if long_side else -1
        info = self.registry.get(s.instrument_id)
        td, yd = pos.split()
        closable = pos.closable()
        notional = s.mark * s.mult * qty
        return PositionView(
            account_id=self.account_id,
            instrument_id=s.instrument_id,
            product=info.product,
            exchange=info.exchange,
            direction=pos.direction,
            td_qty=td,
            yd_qty=yd,
            closable_qty=closable.total,
            closable_td_qty=closable.today,
            closable_yd_qty=closable.yd,
            avg_open_price=opened / qty,
            basis_avg_price=basis / qty,
            last=s.mark,
            position_pnl=sign * s.mult * (s.mark * qty - basis),
            float_pnl_fifo=sign * s.mult * (s.mark * qty - opened),
            margin=s.long_margin if long_side else s.short_margin,
            notional=notional,
            pct_equity=notional / eq if eq > 0 else 0.0,
            limit_locked=locked,
        )


__all__ = ["IntradayLedger", "LedgerFill", "RestingOrder"]
