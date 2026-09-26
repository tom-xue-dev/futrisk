"""Client-side position tracker built from order fills (01 §8.1, §8.15; 02 §7.3 ``Offset``, §7.4).

The risk-engine's lot book is the authority; strategies keep a light copy to decide
*how* to close. Per ``(account, instrument, side)`` the tracker keeps today's and
yesterday's lots (``td``/``yd``) and the average open price:

- a fill with ``OPEN`` adds to ``td`` of the side it opens (BUY -> LONG, SELL -> SHORT);
- a closing fill (BUY closes SHORT, SELL closes LONG) consumes lots according to the
  (normalised) offset: ``CLOSE_TODAY`` -> ``td``; ``CLOSE_YESTERDAY`` -> ``yd``; plain
  ``CLOSE`` -> ``yd`` on SHFE/INE (CTP semantics), otherwise the exchange ``close_style``
  (``FIFO``: yesterday first; ``TODAY_FIRST``: today first); ``FORCE_CLOSE`` -> yesterday
  first. A shortfall spills into the other bucket and is recorded as *drift*
  (``drift_accounts``) so the owner can resync from ``risk.query.positions``;
- when the trading day advances (first tick or fill of a new day; night sessions belong
  to the next trading day, 01 §8.2) every ``td`` lot becomes ``yd``.

:meth:`PositionTracker.close_plan` turns "close N lots" into orders: on SHFE
(``explicit_close_today``) one ``CLOSE_YESTERDAY`` and/or one ``CLOSE_TODAY`` slice,
elsewhere a single ``CLOSE`` - net of closes already working (02 §11.2 step 6).
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from loguru import logger

from futrisk.client.pricing import ContractTable
from futrisk.messages.enums import Direction, Offset, PosDirection
from futrisk.messages.risk import PositionView


def side_opened(direction: Direction) -> PosDirection:
    """Position side an OPEN fill adds to."""
    return PosDirection.LONG if direction == Direction.BUY else PosDirection.SHORT


def side_closed(direction: Direction) -> PosDirection:
    """Position side a closing order reduces (BUY closes SHORT, SELL closes LONG)."""
    return PosDirection.SHORT if direction == Direction.BUY else PosDirection.LONG


def close_direction(side: PosDirection) -> Direction:
    """Order direction that closes ``side``."""
    return Direction.SELL if side == PosDirection.LONG else Direction.BUY


def open_direction(side: PosDirection) -> Direction:
    return Direction.BUY if side == PosDirection.LONG else Direction.SELL


@dataclass
class Leg:
    """Lots of one ``(account, instrument, side)``."""

    td: int = 0
    yd: int = 0
    avg_price: float = 0.0

    @property
    def total(self) -> int:
        return self.td + self.yd

    def add(self, qty: int, price: float) -> None:
        total = self.total
        if price > 0:
            self.avg_price = (self.avg_price * total + price * qty) / (total + qty) if total + qty > 0 else 0.0
        self.td += qty

    def take(self, qty: int, order: tuple[str, ...]) -> int:
        """Remove ``qty`` lots from the buckets in ``order``; returns the shortfall (drift)."""
        left = qty
        for bucket in order:
            have = getattr(self, bucket)
            n = min(have, left)
            setattr(self, bucket, have - n)
            left -= n
            if left == 0:
                break
        if self.total == 0:
            self.avg_price = 0.0
        return left


@dataclass(frozen=True, slots=True)
class PendingCloses:
    """Remaining quantity of working close orders on one side, by lot bucket."""

    td: int = 0
    yd: int = 0
    any: int = 0

    @property
    def total(self) -> int:
        return self.td + self.yd + self.any


class PositionTracker:
    """Today/yesterday lots per account, instrument and side (see module docstring)."""

    def __init__(self, contracts: ContractTable, trading_day: str | None = None) -> None:
        self._contracts = contracts
        self._legs: dict[tuple[str, str, PosDirection], Leg] = {}
        self._trading_day = trading_day
        self.drift_accounts: set[str] = set()

    @property
    def trading_day(self) -> str | None:
        return self._trading_day

    # ------------------------------------------------------------ updates
    def roll_to(self, trading_day: str | None) -> bool:
        """Advance to ``trading_day``; today's lots become yesterday's. Returns True on a roll."""
        if not trading_day:
            return False
        if self._trading_day is None:
            self._trading_day = trading_day
            return False
        if trading_day <= self._trading_day:
            return False
        for leg in self._legs.values():
            leg.yd += leg.td
            leg.td = 0
        logger.info("positions.day_roll", from_td=self._trading_day, to_td=trading_day)
        self._trading_day = trading_day
        return True

    def apply_fill(
        self,
        account_id: str,
        instrument_id: str,
        direction: Direction,
        offset: Offset,
        qty: int,
        price: float = 0.0,
        *,
        trading_day: str | None = None,
    ) -> None:
        """Apply one fill (``qty`` > 0) with its normalised offset."""
        if qty <= 0:
            return
        self.roll_to(trading_day)
        if offset == Offset.OPEN:
            self._leg(account_id, instrument_id, side_opened(direction)).add(qty, price)
            return
        leg = self._leg(account_id, instrument_id, side_closed(direction))
        shortfall = leg.take(qty, self._bucket_order(instrument_id, offset))
        if shortfall:
            self.drift_accounts.add(account_id)
            logger.warning(
                "positions.drift",
                account_id=account_id,
                instrument_id=instrument_id,
                offset=str(offset),
                qty=qty,
                shortfall=shortfall,
            )

    def _bucket_order(self, instrument_id: str, offset: Offset) -> tuple[str, ...]:
        if offset == Offset.CLOSE_TODAY:
            return ("td", "yd")
        if offset in (Offset.CLOSE_YESTERDAY, Offset.FORCE_CLOSE):
            return ("yd", "td")
        info = self._contracts.get(instrument_id) if self._contracts.has(instrument_id) else None
        if info is None or info.explicit_close_today or info.close_style != "TODAY_FIRST":
            return ("yd", "td")  # SHFE CLOSE = 平昨 (CTP); FIFO: yesterday lots are older
        return ("td", "yd")  # 01 §8.15：CFFEX 先平当日新开仓

    def seed(self, account_id: str, views: Iterable[PositionView]) -> None:
        """Replace the account's positions with an authoritative snapshot (``risk.query.positions``)."""
        for key in [k for k in self._legs if k[0] == account_id]:
            del self._legs[key]
        for v in views:
            if v.account_id != account_id:
                continue
            leg = self._leg(account_id, v.instrument_id, v.direction)
            leg.td, leg.yd = max(0, v.td_qty), max(0, v.yd_qty)
            leg.avg_price = v.avg_open_price if leg.total > 0 else 0.0
        self.drift_accounts.discard(account_id)

    def set_leg(self, account_id: str, instrument_id: str, side: PosDirection, *, td: int = 0, yd: int = 0,
                avg_price: float = 0.0) -> None:
        """Set one leg directly (tests, manual corrections)."""
        leg = self._leg(account_id, instrument_id, side)
        leg.td, leg.yd, leg.avg_price = td, yd, avg_price

    # ------------------------------------------------------------ queries
    def _leg(self, account_id: str, instrument_id: str, side: PosDirection) -> Leg:
        key = (account_id, instrument_id, side)
        leg = self._legs.get(key)
        if leg is None:
            leg = Leg()
            self._legs[key] = leg
        return leg

    def leg(self, account_id: str, instrument_id: str, side: PosDirection) -> Leg:
        """A copy of the leg (all zeros when unknown)."""
        leg = self._legs.get((account_id, instrument_id, side))
        return Leg(leg.td, leg.yd, leg.avg_price) if leg is not None else Leg()

    def qty(self, account_id: str, instrument_id: str, side: PosDirection) -> int:
        leg = self._legs.get((account_id, instrument_id, side))
        return leg.total if leg is not None else 0

    def net(self, account_id: str, instrument_id: str) -> int:
        """Long lots minus short lots."""
        return self.qty(account_id, instrument_id, PosDirection.LONG) - self.qty(
            account_id, instrument_id, PosDirection.SHORT
        )

    def instruments(self, account_id: str) -> list[str]:
        return sorted({k[1] for k, leg in self._legs.items() if k[0] == account_id and leg.total > 0})

    def snapshot(self, account_id: str) -> dict[str, dict[str, tuple[int, int]]]:
        """``{instrument: {"LONG": (td, yd), "SHORT": (td, yd)}}`` of non-empty legs."""
        out: dict[str, dict[str, tuple[int, int]]] = {}
        for (acct, inst, side), leg in sorted(self._legs.items()):
            if acct == account_id and leg.total > 0:
                out.setdefault(inst, {})[str(side)] = (leg.td, leg.yd)
        return out

    def closable(self, account_id: str, instrument_id: str, side: PosDirection,
                 pending: PendingCloses | None = None) -> tuple[int, int, int]:
        """``(total, td, yd)`` still closable after the working close orders in ``pending``."""
        leg = self._legs.get((account_id, instrument_id, side)) or Leg()
        p = pending or PendingCloses()
        explicit = self._contracts.has(instrument_id) and self._contracts.get(instrument_id).explicit_close_today
        if explicit:
            td = max(0, leg.td - p.td)
            yd = max(0, leg.yd - p.yd - p.any)  # SHFE CLOSE = 平昨
            return td + yd, td, yd
        total = max(0, leg.total - p.total)
        return total, min(total, leg.td), min(total, leg.yd)

    def close_plan(
        self,
        account_id: str,
        instrument_id: str,
        side: PosDirection,
        qty: int,
        *,
        pending: PendingCloses | None = None,
        prefer_today: bool = False,
    ) -> list[tuple[Offset, int]]:
        """Orders closing up to ``qty`` lots of ``side``: ``[(offset, lots), ...]`` (may be empty).

        SHFE/INE: yesterday lots first (``CLOSE_YESTERDAY``) then today's (``CLOSE_TODAY``),
        or the reverse with ``prefer_today``; other exchanges: one ``CLOSE``.
        """
        if qty <= 0:
            return []
        total, td, yd = self.closable(account_id, instrument_id, side, pending)
        if total <= 0:
            return []
        explicit = self._contracts.has(instrument_id) and self._contracts.get(instrument_id).explicit_close_today
        if not explicit:
            return [(Offset.CLOSE, min(qty, total))]
        order = ((Offset.CLOSE_TODAY, td), (Offset.CLOSE_YESTERDAY, yd))
        if not prefer_today:
            order = (order[1], order[0])
        plan: list[tuple[Offset, int]] = []
        left = qty
        for offset, avail in order:
            n = min(left, avail)
            if n > 0:
                plan.append((offset, n))
                left -= n
        return plan


__all__ = [
    "Leg",
    "PendingCloses",
    "PositionTracker",
    "close_direction",
    "open_direction",
    "side_closed",
    "side_opened",
]
