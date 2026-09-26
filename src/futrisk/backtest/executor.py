"""Target positions -> orders through the engine's pre-trade path (``handle(order.submit)``).

The executor of one account turns the strategy's target net lots per contract
into marketable limit orders at the current price point:

- only contracts with a market state at the current timestamp are traded (a
  product in another session waits for its next state);
- contracts that already have a working order of this account are skipped
  (a locked-limit order waits for the market; nothing is double-submitted);
- a reduction closes first (SHFE: close-yesterday lots, then close-today
  lots, as ``CLOSE_YESTERDAY`` / ``CLOSE_TODAY``; other exchanges ``CLOSE``),
  and only the remainder opens the other side;
- orders are split at the product's ``max_order_qty``;
- reject handling: a ``REJECT_ORDER_SIZE_RESTRICTED`` is retried once at the
  allowed size; after a ``REJECT_ACCOUNT_HALTED`` the account stops sending
  opening orders while the engine reports it HALTED (the suppressed attempts
  are counted), every other reject ends this contract's rebalance for the
  current state. Every submission and its result is logged.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any

from futrisk.backtest.paths import TickGrid
from futrisk.core.instruments import InstrumentRegistry
from futrisk.messages import codes
from futrisk.messages.enums import AccountState, Direction, Offset, OrderSource, OrderType, PosDirection
from futrisk.messages.envelope import Reply
from futrisk.messages.orders import OrderAck, OrderRequest


@dataclass(frozen=True, slots=True)
class PosInfo:
    """One contract of an account as the engine's book has it."""

    long_qty: int = 0
    short_qty: int = 0
    long_td: int = 0  # closable today lots (net of working closes)
    long_yd: int = 0
    short_td: int = 0
    short_yd: int = 0

    @property
    def net(self) -> int:
        return self.long_qty - self.short_qty


@dataclass
class ExecStats:
    submitted: int = 0
    accepted: int = 0
    rejected: int = 0
    suppressed_opens: int = 0  # opening attempts not sent while HALTED after a REJECT_ACCOUNT_HALTED
    rejects: dict[str, int] = field(default_factory=dict)


SubmitFn = Callable[[OrderRequest, str], Reply]


class Executor:
    """Executor of one account (see the module docstring)."""

    def __init__(
        self,
        account_id: str,
        registry: InstrumentRegistry,
        submit: SubmitFn,
        *,
        id_prefix: str,
    ) -> None:
        self.account_id = account_id
        self.registry = registry
        self._submit = submit
        self._prefix = id_prefix
        self._n = 0
        self.stats = ExecStats()
        self.halt_gate = False  # set by REJECT_ACCOUNT_HALTED, cleared when the account leaves HALTED
        self._grids: dict[str, TickGrid] = {}

    def _coid(self) -> str:
        self._n += 1
        return f"{self._prefix}-{self._n:08d}"

    def order_price(self, instrument_id: str, direction: Direction, price: float) -> float:
        """The limit price on today's tick grid, on the marketable side of ``price`` (history may be off-grid)."""
        g = self._grids.get(instrument_id)
        if g is None:
            g = TickGrid(self.registry.tick_size(instrument_id))
            self._grids[instrument_id] = g
        return g.ceil(price) if direction == Direction.BUY else g.floor(price)

    def max_qty(self, instrument_id: str) -> int:
        spec = self.registry.product_spec(instrument_id)
        return int(spec.max_order_qty.get(OrderType.LIMIT.value, 1000))

    # -------------------------------------------------------------- orders
    def send(
        self,
        instrument_id: str,
        direction: Direction,
        offset: Offset,
        qty: int,
        price: float,
        *,
        purpose: str,
        source: OrderSource = OrderSource.BOT,
    ) -> tuple[int, str | None]:
        """Submit one order (split at ``max_order_qty``); returns (lots accepted, last reject code)."""
        left = qty
        accepted = 0
        code: str | None = None
        cap = self.max_qty(instrument_id)
        price = self.order_price(instrument_id, direction, price)
        retried = False
        while left > 0:
            q = min(left, cap)
            req = OrderRequest(
                client_order_id=self._coid(),
                account_id=self.account_id,
                instrument_id=instrument_id,
                direction=direction,
                offset=offset,
                order_type=OrderType.LIMIT,
                price=price,
                qty=q,
                source=source,
                strategy_tag=purpose,
            )
            self.stats.submitted += 1
            reply = self._submit(req, purpose)
            if reply.ok:
                self.stats.accepted += 1
                accepted += q
                left -= q
                continue
            code = reply.code
            self.stats.rejected += 1
            self.stats.rejects[code] = self.stats.rejects.get(code, 0) + 1
            if code == codes.REJECT_ACCOUNT_HALTED:
                self.halt_gate = True
                break
            if code == codes.REJECT_ORDER_SIZE_RESTRICTED and not retried:
                detail = _detail(reply)
                allowed = int(detail.get("max", 0)) if detail else 0
                if allowed > 0:
                    cap = allowed
                    retried = True
                    continue
            break
        return accepted, code

    def rebalance(
        self,
        targets: Mapping[str, int],
        net: Mapping[str, int],
        pos_info: Callable[[str], PosInfo],
        prices: Mapping[str, float],
        live: set[str],
        working: set[str],
        state: AccountState,
        *,
        purpose: str = "rebalance",
    ) -> None:
        """Move every live contract towards its target (contracts of the targets' products not listed -> 0).

        ``net`` is the engine's net position per contract; ``pos_info`` gives the
        closable split of one contract (only read when a contract must move).
        """
        if self.halt_gate and state != AccountState.HALTED:
            self.halt_gate = False
        want = dict(targets)
        products = {self.registry.product(i) for i in want}
        for inst, n in net.items():
            if inst not in want and n != 0 and self.registry.product(inst) in products:
                want[inst] = 0  # an old main after a roll, or a contract the strategy no longer wants
        for inst in sorted(want):
            if inst not in live or inst in working:
                continue
            price = prices.get(inst)
            if price is None:
                continue
            delta = want[inst] - net.get(inst, 0)
            if delta == 0:
                continue
            pos = pos_info(inst)
            if delta < 0:
                self._move(inst, Direction.SELL, -delta, pos.long_yd, pos.long_td, price, purpose)
            else:
                self._move(inst, Direction.BUY, delta, pos.short_yd, pos.short_td, price, purpose)

    def _move(self, inst: str, direction: Direction, qty: int, close_yd: int, close_td: int, price: float, purpose: str) -> None:
        left = qty
        explicit = self.registry.explicit_close_today(inst)
        if explicit:
            for off, avail in ((Offset.CLOSE_YESTERDAY, close_yd), (Offset.CLOSE_TODAY, close_td)):
                q = min(left, avail)
                if q > 0:
                    done, code = self.send(inst, direction, off, q, price, purpose=purpose)
                    left -= done
                    if code is not None and done < q:
                        return
        else:
            q = min(left, close_yd + close_td)
            if q > 0:
                done, code = self.send(inst, direction, Offset.CLOSE, q, price, purpose=purpose)
                left -= done
                if code is not None and done < q:
                    return
        if left <= 0:
            return
        if self.halt_gate:
            self.stats.suppressed_opens += 1
            return
        self.send(inst, direction, Offset.OPEN, left, price, purpose=purpose)


def _detail(reply: Reply) -> dict[str, Any] | None:
    try:
        ack = reply.open_as(OrderAck)
    except Exception:
        return None
    detail: dict[str, Any] | None = ack.reject_detail
    return detail


def pos_info(positions: Any, account_id: str, instrument_id: str) -> PosInfo:
    """Closable split of one contract from an engine ``PositionBook``."""
    d: dict[str, int] = {}
    for direction in (PosDirection.LONG, PosDirection.SHORT):
        pos = positions.find(account_id, instrument_id, direction)
        if pos is None or pos.qty <= 0:
            continue
        c = pos.closable()
        if direction == PosDirection.LONG:
            d.update(long_qty=pos.qty, long_td=c.today, long_yd=c.yd)
        else:
            d.update(short_qty=pos.qty, short_td=c.today, short_yd=c.yd)
    return PosInfo(**d)


__all__ = ["ExecStats", "Executor", "PosInfo", "pos_info"]
