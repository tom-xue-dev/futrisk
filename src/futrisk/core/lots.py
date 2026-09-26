"""Lot-based position book (01 §8.1 domain model, §5.3.1 持仓模型, §8.15 开平与计费).

A position is keyed by ``(account_id, instrument_id, PosDirection)`` and holds
:class:`Lot` objects in FIFO order: open trading day, then open timestamp, then
lot number (so a seeded yesterday lot with a late timestamp still comes before
today's lots). Positions are only ever changed by fills (event sourcing, 01 §8.1).

Mark-to-market basis: a today lot's basis is its open price; at the day roll
every lot becomes a yesterday lot whose basis is that day's settlement price
(03 §4.4 ``sod_lots.basis_price``). "Today" means ``lot.open_td`` equals the
book's current trading day, so a night-session fill (which belongs to the next
trading day, 01 §8.2) is a today lot of that day.

Close rules come from :class:`ExchangeRules`:

- ``explicit_close_today`` (SHFE/INE): ``CLOSE_TODAY`` consumes only today lots;
  ``CLOSE_YESTERDAY`` and plain ``CLOSE`` (CTP semantics) only yesterday lots;
- otherwise any lot may be consumed, in ``close_style`` order: ``FIFO`` (oldest
  first, so yesterday lots first) or ``TODAY_FIRST``;
- ``FORCE_CLOSE`` (broker liquidation, only seen on fills) follows ``CLOSE``;
  because it is a fact the counter has already executed, on SHFE it spills into
  today lots when the yesterday lots are not enough instead of being rejected
  (the slices record the real lot types, so fees follow the lots consumed).

Validation failures map to the pre-trade reject codes of 02 §11.2 step 6.
"""

from __future__ import annotations

import bisect
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum

from futrisk.common.config.products import ExchangeRules
from futrisk.common.decimals import D
from futrisk.core._num import div, price_str
from futrisk.core.instruments import InstrumentRegistry
from futrisk.messages import codes
from futrisk.messages.enums import Direction, Offset, PosDirection
from futrisk.messages.eod import SodAccount, SodLot
from futrisk.messages.orders import Trade


class CloseType(StrEnum):
    """``close_details.close_type`` (storage-only enum, 03 §3.3)."""

    TODAY = "TODAY"
    YD = "YD"


class CloseBucket(StrEnum):
    """Which lots an offset may consume."""

    TODAY = "TODAY"
    YD = "YD"
    ANY = "ANY"


class PositionError(Exception):
    """A close cannot be applied; ``code`` is the 02 §11.2 reject code."""

    def __init__(self, code: str, message: str, *, requested: int = 0, available: int = 0) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code
        self.requested = requested
        self.available = available


@dataclass(slots=True)
class Lot:
    """One opening fill (or its unclosed remainder)."""

    lot_no: int
    open_td: str
    open_ts_ns: int
    open_price: Decimal
    qty: int
    basis_price: Decimal

    @property
    def sort_key(self) -> tuple[str, int, int]:
        """FIFO order: open trading day, open timestamp, lot number."""
        return (self.open_td, self.open_ts_ns, self.lot_no)


@dataclass(frozen=True, slots=True)
class CloseSlice:
    """The part of one lot consumed by a close (one ``close_details`` row)."""

    lot_no: int
    close_type: CloseType
    lot_open_td: str
    lot_open_ts_ns: int
    lot_open_price: Decimal
    basis_price: Decimal
    qty: int


@dataclass(frozen=True, slots=True)
class Closable:
    """Closable quantities after deducting pending (frozen) close orders."""

    total: int
    today: int
    yd: int


PosKey = tuple[str, str, PosDirection]


def pos_direction(direction: Direction, offset: Offset) -> PosDirection:
    """Position side touched by a fill: BUY-OPEN/SELL-CLOSE -> LONG, SELL-OPEN/BUY-CLOSE -> SHORT."""
    if offset == Offset.OPEN:
        return PosDirection.LONG if direction == Direction.BUY else PosDirection.SHORT
    return PosDirection.SHORT if direction == Direction.BUY else PosDirection.LONG


def direction_sign(direction: PosDirection) -> int:
    return 1 if direction == PosDirection.LONG else -1


def normalize_offset(rules: ExchangeRules, offset: Offset) -> Offset:
    """Offset as routed to the exchange (02 §7.3 ``Offset``, §7.4 ``RouteOrder``).

    SHFE/INE: plain ``CLOSE`` becomes ``CLOSE_YESTERDAY`` (CTP semantics); other
    exchanges fold ``CLOSE_TODAY``/``CLOSE_YESTERDAY`` into ``CLOSE``.
    ``FORCE_CLOSE`` is kept (it only appears on broker fills).
    """
    if offset == Offset.OPEN or offset == Offset.FORCE_CLOSE:
        return offset
    if rules.explicit_close_today:
        return Offset.CLOSE_YESTERDAY if offset == Offset.CLOSE else offset
    return Offset.CLOSE


def _bucket_for(explicit_close_today: bool, offset: Offset) -> CloseBucket:
    if offset == Offset.OPEN:
        raise ValueError("OPEN is not a closing offset")
    if explicit_close_today:
        # 01 §8.15：SHFE/INE 必须显式指定平今或平昨；CLOSE（及按 CLOSE 处理的 FORCE_CLOSE）= 平昨
        return CloseBucket.TODAY if offset == Offset.CLOSE_TODAY else CloseBucket.YD
    return CloseBucket.ANY


def close_bucket(rules: ExchangeRules, offset: Offset) -> CloseBucket:
    """Lots a closing offset may consume under the exchange rules."""
    return _bucket_for(rules.explicit_close_today, offset)


class Position:
    """Lots of one ``(account, instrument, direction)`` plus frozen close quantities.

    Running aggregates (total and today quantity, Σ open and basis notional)
    are kept in step with every mutation, so the quantities read by the
    pre-trade path and the ledger are O(1) whatever the number of lots. The
    notional sums are exact Decimal sums, identical (value and exponent) to
    summing the lots: they are added on open / load and recomputed after a
    close or a roll.
    """

    __slots__ = (
        "_basis_n",
        "_open_n",
        "_qty",
        "_td",
        "_trading_day",
        "account_id",
        "close_style",
        "direction",
        "explicit_close_today",
        "frozen_any",
        "frozen_today",
        "frozen_yd",
        "instrument_id",
        "lots",
        "next_lot_no",
    )

    def __init__(
        self,
        account_id: str,
        instrument_id: str,
        direction: PosDirection,
        trading_day: str,
        *,
        close_style: str = "FIFO",
        explicit_close_today: bool = False,
    ) -> None:
        if close_style not in ("FIFO", "TODAY_FIRST"):
            raise ValueError(f"unknown close_style {close_style!r}")
        self.account_id = account_id
        self.instrument_id = instrument_id
        self.direction = direction
        self.close_style = close_style
        self.explicit_close_today = explicit_close_today
        self.lots: list[Lot] = []
        self._qty = 0
        self._td = 0
        self._open_n = Decimal(0)
        self._basis_n = Decimal(0)
        self.trading_day = trading_day
        self.next_lot_no = 1
        self.frozen_today = 0
        self.frozen_yd = 0
        self.frozen_any = 0

    @classmethod
    def for_rules(
        cls, account_id: str, instrument_id: str, direction: PosDirection, trading_day: str, rules: ExchangeRules
    ) -> Position:
        return cls(
            account_id,
            instrument_id,
            direction,
            trading_day,
            close_style=rules.close_style,
            explicit_close_today=rules.explicit_close_today,
        )

    def __repr__(self) -> str:
        return (
            f"Position({self.account_id}, {self.instrument_id}, {self.direction}, "
            f"td={self.td_qty}, yd={self.yd_qty}, lots={len(self.lots)})"
        )

    # ------------------------------------------------------------ quantities
    @property
    def key(self) -> PosKey:
        return (self.account_id, self.instrument_id, self.direction)

    @property
    def sign(self) -> int:
        return direction_sign(self.direction)

    @property
    def trading_day(self) -> str:
        return self._trading_day

    @trading_day.setter
    def trading_day(self, value: str) -> None:
        self._trading_day = value
        self._td = sum(lot.qty for lot in self.lots if lot.open_td == value)  # 今仓按新的交易日重新归类

    @property
    def qty(self) -> int:
        return self._qty

    @property
    def td_qty(self) -> int:
        return self._td

    @property
    def yd_qty(self) -> int:
        return self._qty - self._td

    def is_today(self, lot: Lot) -> bool:
        return lot.open_td == self._trading_day

    def split(self) -> tuple[int, int]:
        """``(td_qty, yd_qty)``."""
        return self._td, self._qty - self._td

    @property
    def open_notional(self) -> Decimal:
        """Σ open_price × qty (per-trade / FIFO cost)."""
        return self._open_n

    @property
    def basis_notional(self) -> Decimal:
        """Σ basis_price × qty (mark-to-market cost)."""
        return self._basis_n

    def _add(self, lot: Lot) -> None:
        self._qty += lot.qty
        if lot.open_td == self._trading_day:
            self._td += lot.qty
        self._open_n += lot.open_price * lot.qty
        self._basis_n += lot.basis_price * lot.qty

    def _recount(self) -> None:
        """Recompute every aggregate from the lots (after a close or a roll)."""
        day = self._trading_day
        self._qty = sum(lot.qty for lot in self.lots)
        self._td = sum(lot.qty for lot in self.lots if lot.open_td == day)
        self._open_n = sum((lot.open_price * lot.qty for lot in self.lots), Decimal(0))
        self._basis_n = sum((lot.basis_price * lot.qty for lot in self.lots), Decimal(0))

    def avg_open_price(self) -> Decimal | None:
        """开仓均价（逐笔口径）; None when flat."""
        q = self.qty
        return div(self.open_notional, Decimal(q)) if q else None

    def basis_avg_price(self) -> Decimal | None:
        """持仓均价（盯市口径：今仓按开仓价，昨仓按昨结算价）; None when flat."""
        q = self.qty
        return div(self.basis_notional, Decimal(q)) if q else None

    # -------------------------------------------------------------- mutation
    def _insert(self, lot: Lot) -> None:
        lots = self.lots
        key = lot.sort_key
        if not lots or key >= lots[-1].sort_key:
            lots.append(lot)  # 常见情形：新开的今仓排在最后（与 bisect_right 结果相同）
        else:
            keys = [x.sort_key for x in lots]
            lots.insert(bisect.bisect_right(keys, key), lot)
        self._add(lot)

    def load_lot(self, lot: Lot) -> None:
        """Restore a lot (e.g. from a SOD snapshot); keeps FIFO order and lot numbering."""
        if lot.qty <= 0:
            raise ValueError(f"lot qty must be positive, got {lot.qty}")
        self._insert(lot)
        self.next_lot_no = max(self.next_lot_no, lot.lot_no + 1)

    def open(self, qty: int, price: Decimal, ts_ns: int, *, open_td: str | None = None) -> Lot:
        """Add a today lot; its basis is the open price."""
        if qty <= 0:
            raise ValueError(f"open qty must be positive, got {qty}")
        lot = Lot(
            lot_no=self.next_lot_no,
            open_td=open_td or self.trading_day,
            open_ts_ns=ts_ns,
            open_price=price,
            qty=qty,
            basis_price=price,
        )
        self.next_lot_no += 1
        self._insert(lot)
        return lot

    def _bucket(self, offset: Offset) -> CloseBucket:
        return _bucket_for(self.explicit_close_today, offset)

    def _consumption_order(self, bucket: CloseBucket, lenient: bool) -> list[Lot]:
        today = [lot for lot in self.lots if self.is_today(lot)]
        yd = [lot for lot in self.lots if not self.is_today(lot)]
        if bucket == CloseBucket.TODAY:
            return today + yd if lenient else today
        if bucket == CloseBucket.YD:
            return yd + today if lenient else yd
        if self.close_style == "TODAY_FIRST":
            return today + yd
        return list(self.lots)  # FIFO：按开仓时间先开先平（昨仓天然在前）

    @staticmethod
    def _exceeds_code(bucket: CloseBucket) -> str:
        if bucket == CloseBucket.TODAY:
            return codes.REJECT_CLOSE_TODAY_EXCEEDS
        if bucket == CloseBucket.YD:
            return codes.REJECT_CLOSE_YD_EXCEEDS
        return codes.REJECT_CLOSE_EXCEEDS_POSITION

    def preview_close(self, offset: Offset, qty: int) -> tuple[int, int]:
        """``(today, yesterday)`` lots a close of ``qty`` would consume now, without changing the book.

        Capped at what is held; used for fee estimates of resting close orders
        (``LOT_MATCH`` classifies by the lots consumed).
        """
        lenient = offset == Offset.FORCE_CLOSE
        today = yd = 0
        remaining = max(qty, 0)
        for lot in self._consumption_order(self._bucket(offset), lenient):
            if remaining == 0:
                break
            take = min(lot.qty, remaining)
            if self.is_today(lot):
                today += take
            else:
                yd += take
            remaining -= take
        return today, yd

    def close(self, offset: Offset, qty: int, *, lenient: bool = False) -> list[CloseSlice]:
        """Consume lots for a closing fill and return the matched slices.

        Checks against the lots actually held (a fill of a pending order uses up
        its own frozen quantity: call :meth:`unfreeze_close` for the filled part).
        ``lenient=True`` lets an explicit today/yesterday close spill into the
        other bucket, for broker facts that must be applied anyway; the slices
        then record the real lot types. ``FORCE_CLOSE`` is always lenient.
        """
        if qty <= 0:
            raise ValueError(f"close qty must be positive, got {qty}")
        lenient = lenient or offset == Offset.FORCE_CLOSE
        bucket = self._bucket(offset)
        order = self._consumption_order(bucket, lenient)
        available = sum(lot.qty for lot in order)
        if qty > available:
            code = codes.REJECT_CLOSE_EXCEEDS_POSITION if lenient else self._exceeds_code(bucket)
            raise PositionError(
                code,
                f"{self.account_id} {self.instrument_id} {self.direction}: close {qty} ({offset}) "
                f"but only {available} closable",
                requested=qty,
                available=available,
            )
        slices: list[CloseSlice] = []
        remaining = qty
        for lot in order:
            if remaining == 0:
                break
            take = min(lot.qty, remaining)
            slices.append(
                CloseSlice(
                    lot_no=lot.lot_no,
                    close_type=CloseType.TODAY if self.is_today(lot) else CloseType.YD,
                    lot_open_td=lot.open_td,
                    lot_open_ts_ns=lot.open_ts_ns,
                    lot_open_price=lot.open_price,
                    basis_price=lot.basis_price,
                    qty=take,
                )
            )
            lot.qty -= take
            remaining -= take
        self.lots = [lot for lot in self.lots if lot.qty > 0]
        self._recount()
        return slices

    # ------------------------------------------------------ pending closes
    def closable(self) -> Closable:
        """Closable quantities net of pending close orders (PositionView.closable_*)."""
        td, yd = self.split()
        c_td = max(0, td - self.frozen_today)
        c_yd = max(0, yd - self.frozen_yd)
        fa = self.frozen_any
        if fa:
            # 非显式平今的交易所：在途平仓按 close_style 的消耗顺序分摊到今/昨仓
            if self.close_style == "TODAY_FIRST":
                take = min(c_td, fa)
                c_td -= take
                c_yd = max(0, c_yd - (fa - take))
            else:
                take = min(c_yd, fa)
                c_yd -= take
                c_td = max(0, c_td - (fa - take))
        return Closable(total=c_td + c_yd, today=c_td, yd=c_yd)

    def check_close(self, offset: Offset, qty: int) -> str | None:
        """Pre-trade position check (02 §11.2 step 6); None when the close is allowed."""
        if qty <= 0:
            return codes.REJECT_INVALID_QTY
        c = self.closable()
        bucket = self._bucket(offset)
        if bucket == CloseBucket.TODAY:
            return codes.REJECT_CLOSE_TODAY_EXCEEDS if qty > c.today else None
        if bucket == CloseBucket.YD:
            return codes.REJECT_CLOSE_YD_EXCEEDS if qty > c.yd else None
        return codes.REJECT_CLOSE_EXCEEDS_POSITION if qty > c.total else None

    def freeze_close(self, offset: Offset, qty: int) -> None:
        """Reserve closable quantity for a resting close order (raises if not closable)."""
        code = self.check_close(offset, qty)
        if code is not None:
            c = self.closable()
            raise PositionError(code, f"cannot freeze close {qty} ({offset})", requested=qty, available=c.total)
        bucket = self._bucket(offset)
        if bucket == CloseBucket.TODAY:
            self.frozen_today += qty
        elif bucket == CloseBucket.YD:
            self.frozen_yd += qty
        else:
            self.frozen_any += qty

    def unfreeze_close(self, offset: Offset, qty: int) -> None:
        """Release a reservation (fill, cancel or reject of the close order)."""
        bucket = self._bucket(offset)
        if bucket == CloseBucket.TODAY:
            self.frozen_today = max(0, self.frozen_today - qty)
        elif bucket == CloseBucket.YD:
            self.frozen_yd = max(0, self.frozen_yd - qty)
        else:
            self.frozen_any = max(0, self.frozen_any - qty)

    # -------------------------------------------------------------- day roll
    def roll(self, next_trading_day: str, settle_price: Decimal) -> None:
        """Day roll: every lot becomes a yesterday lot with basis = settlement price."""
        for lot in self.lots:
            lot.basis_price = settle_price
        self._trading_day = next_trading_day
        self._recount()
        self.frozen_today = self.frozen_yd = self.frozen_any = 0


@dataclass(frozen=True, slots=True)
class FillResult:
    """Outcome of applying one fill to the book."""

    key: PosKey
    is_open: bool
    lot: Lot | None
    slices: tuple[CloseSlice, ...]

    @property
    def pos_direction(self) -> PosDirection:
        return self.key[2]

    @property
    def close_today_qty(self) -> int:
        """Today lots consumed (``fills.close_today_qty``)."""
        return sum(s.qty for s in self.slices if s.close_type == CloseType.TODAY)

    @property
    def close_yd_qty(self) -> int:
        """Yesterday lots consumed (``fills.close_yd_qty``)."""
        return sum(s.qty for s in self.slices if s.close_type == CloseType.YD)


def lot_from_sod(lot: SodLot) -> Lot:
    return Lot(
        lot_no=lot.lot_no,
        open_td=lot.open_td,
        open_ts_ns=lot.open_ts_ns,
        open_price=D(lot.open_price),
        qty=lot.qty,
        basis_price=D(lot.basis_price),
    )


def lot_to_sod(instrument_id: str, direction: PosDirection, lot: Lot) -> SodLot:
    return SodLot(
        instrument_id=instrument_id,
        direction=direction,
        lot_no=lot.lot_no,
        open_td=lot.open_td,
        open_ts_ns=lot.open_ts_ns,
        open_price=price_str(lot.open_price),
        qty=lot.qty,
        basis_price=price_str(lot.basis_price),
    )


class PositionBook:
    """All positions of a set of accounts for one trading day."""

    def __init__(self, registry: InstrumentRegistry, trading_day: str) -> None:
        self._registry = registry
        self._trading_day = trading_day
        self._positions: dict[PosKey, Position] = {}

    @property
    def trading_day(self) -> str:
        return self._trading_day

    @property
    def registry(self) -> InstrumentRegistry:
        return self._registry

    def set_registry(self, registry: InstrumentRegistry) -> None:
        """Use a reloaded registry for positions created from now on (existing lots are kept)."""
        self._registry = registry

    # ------------------------------------------------------------- access
    def find(self, account_id: str, instrument_id: str, direction: PosDirection) -> Position | None:
        return self._positions.get((account_id, instrument_id, direction))

    def position(self, account_id: str, instrument_id: str, direction: PosDirection) -> Position:
        """Get or create the position (instrument must be known to the registry)."""
        key = (account_id, instrument_id, direction)
        pos = self._positions.get(key)
        if pos is None:
            rules = self._registry.rules(instrument_id)
            pos = Position.for_rules(account_id, instrument_id, direction, self._trading_day, rules)
            self._positions[key] = pos
        return pos

    def positions(self, account_id: str | None = None) -> list[Position]:
        """Positions sorted by key (including flat ones touched today)."""
        items = sorted(self._positions.items(), key=lambda kv: (kv[0][0], kv[0][1], kv[0][2].value))
        return [p for k, p in items if account_id is None or k[0] == account_id]

    def accounts(self) -> list[str]:
        return sorted({k[0] for k in self._positions})

    # ------------------------------------------------------------- loading
    def load_sod_account(self, account: SodAccount) -> None:
        """Load an account's SOD lots (all are yesterday lots of this trading day)."""
        for sl in account.lots:
            if sl.open_td >= self._trading_day:
                raise ValueError(
                    f"SOD lot {account.account_id} {sl.instrument_id} #{sl.lot_no} opened on {sl.open_td}, "
                    f"not before trading day {self._trading_day}"
                )
            self.position(account.account_id, sl.instrument_id, sl.direction).load_lot(lot_from_sod(sl))

    def load_sod_accounts(self, accounts: Iterable[SodAccount]) -> None:
        for a in accounts:
            self.load_sod_account(a)

    # --------------------------------------------------------------- fills
    def apply_fill(
        self,
        *,
        account_id: str,
        instrument_id: str,
        direction: Direction,
        offset: Offset,
        price: Decimal,
        qty: int,
        ts_ns: int,
        lenient: bool = False,
    ) -> FillResult:
        """Apply one fill (FORCE_CLOSE is handled as CLOSE)."""
        side = pos_direction(direction, offset)
        pos = self.position(account_id, instrument_id, side)
        if offset == Offset.OPEN:
            lot = pos.open(qty, price, ts_ns)
            return FillResult(key=pos.key, is_open=True, lot=lot, slices=())
        slices = pos.close(offset, qty, lenient=lenient)
        return FillResult(key=pos.key, is_open=False, lot=None, slices=tuple(slices))

    def apply_trade(self, trade: Trade, *, lenient: bool = False) -> FillResult:
        """Apply a ``Trade`` (float price converted with ``Decimal(str(x))``, 02 §4.3)."""
        return self.apply_fill(
            account_id=trade.account_id,
            instrument_id=trade.instrument_id,
            direction=trade.direction,
            offset=trade.offset,
            price=D(trade.price),
            qty=trade.qty,
            ts_ns=trade.trade_ts_ns,
            lenient=lenient,
        )

    def check_close(
        self, account_id: str, instrument_id: str, direction: Direction, offset: Offset, qty: int
    ) -> str | None:
        """Pre-trade check for a closing order given as the order's own direction."""
        side = pos_direction(direction, offset)
        pos = self.find(account_id, instrument_id, side)
        if pos is None:
            if qty <= 0:
                return codes.REJECT_INVALID_QTY
            bucket = close_bucket(self._registry.rules(instrument_id), offset)
            return Position._exceeds_code(bucket)
        return pos.check_close(offset, qty)

    # ------------------------------------------------------------ day roll
    def roll(self, next_trading_day: str, settle_prices: Mapping[str, Decimal]) -> None:
        """Roll every position to ``next_trading_day`` (basis = settlement price) and drop flat ones."""
        missing = sorted({p.instrument_id for p in self._positions.values() if p.lots} - set(settle_prices))
        if missing:
            raise KeyError(f"missing settlement price for {', '.join(missing)}")
        for key in list(self._positions):
            pos = self._positions[key]
            if not pos.lots:
                del self._positions[key]
                continue
            pos.roll(next_trading_day, settle_prices[pos.instrument_id])
        self._trading_day = next_trading_day

    def sod_lots(self, account_id: str) -> list[SodLot]:
        """The account's lots as ``SodLot`` rows (after :meth:`roll`)."""
        out: list[SodLot] = []
        for pos in self.positions(account_id):
            out.extend(lot_to_sod(pos.instrument_id, pos.direction, lot) for lot in pos.lots)
        return out


__all__ = [
    "Closable",
    "CloseBucket",
    "CloseSlice",
    "CloseType",
    "FillResult",
    "Lot",
    "PosKey",
    "Position",
    "PositionBook",
    "PositionError",
    "close_bucket",
    "direction_sign",
    "lot_from_sod",
    "lot_to_sod",
    "normalize_offset",
    "pos_direction",
]
