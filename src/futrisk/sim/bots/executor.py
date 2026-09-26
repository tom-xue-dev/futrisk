"""Target positions -> orders for one account (01 §6.2 R2; 02 §4.7, §7.4, §8.3, §11.2).

:meth:`Executor.decide` is called for one instrument at a time (on its ticks) and
returns at most one :class:`SubmitAction` or :class:`CancelAction`:

1. one RPC in flight per instrument; ``ALL``-kind gate blocks hold everything back;
2. an own working order is left alone while it is young and still wanted; it is
   cancelled after ``order_ttl_s`` of market time, or at once when the target moved
   against it; an **in-doubt** order (no answer after the client retries) is re-sent
   with the *same* ``client_order_id`` and finally given up (02 §4.7, 01 §6.5);
3. otherwise ``delta = target - net``: the opposite side is **closed first** (on SHFE
   split into ``CLOSE_YESTERDAY``/``CLOSE_TODAY`` by the position tracker, net of working
   closes), then new lots are **opened** - only if the gates allow opening (probe size
   after a block expired, reduced size under ``ORDER_SIZE_FACTOR``);
4. sizes are sliced (``slice_notional``, ``max_slice_lots``, ``max_order_qty``) and scaled
   by a published ``ORDER_SIZE_FACTOR`` (visibly smaller opening orders while the account is
   RESTRICTED), small rebalances are skipped (``deadband_frac``), and a token bucket keeps
   the order rate at a few per minute. Prices: closes cross the spread by 1-2 ticks; opens
   cross or, with probability ``passive_prob``, join the book (the account's seeded RNG);
5. while the account is HALTED, closes go on as usual and one 1-lot opening **halt probe**
   in the direction of the target goes out every ``halt_probe_s`` (01 §6.3: the opening
   orders are rejected with ``REJECT_ACCOUNT_HALTED`` - visible, not a storm; after a reset
   the first accepted probe ends the halt).

Results come back through :meth:`on_submit_result`, :meth:`on_cancel_result` and
:meth:`on_order_event`, which feed the :class:`~futrisk.sim.bots.gates.Gates`.
"""

from __future__ import annotations

import math
import random
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass

from loguru import logger

from futrisk.client.orders import ClientState, OrderBook, TrackedOrder
from futrisk.client.positions import PositionTracker, side_closed
from futrisk.client.pricing import ContractInfo, ContractTable, aggressive_price, passive_price
from futrisk.messages import codes
from futrisk.messages.enums import Direction, Offset, OrderSource, OrderStatus
from futrisk.messages.envelope import Reply
from futrisk.messages.md import Tick
from futrisk.messages.orders import OrderAck, OrderRequest, OrderUpdate
from futrisk.sim.bots.config import ExecutionConfig
from futrisk.sim.bots.gates import Gates
from futrisk.sim.bots.indicators import NS_PER_S

NewOrder = Callable[..., OrderRequest]


@dataclass(frozen=True, slots=True)
class SubmitAction:
    request: OrderRequest
    kind: str  # open | close | probe | resubmit
    why: str = ""

    @property
    def instrument_id(self) -> str:
        return self.request.instrument_id


@dataclass(frozen=True, slots=True)
class CancelAction:
    account_id: str
    client_order_id: str
    instrument_id: str
    why: str = ""


Action = SubmitAction | CancelAction


class TokenBucket:
    """Order-rate limiter on market time: ``per_min`` refill rate, ``burst`` capacity."""

    def __init__(self, per_min: float, burst: int) -> None:
        self.rate_per_ns = max(0.0, per_min) / (60 * NS_PER_S)
        self.burst = max(1, burst)
        self.tokens = float(self.burst)
        self._last: int | None = None

    def _refill(self, now_ns: int) -> None:
        if self._last is not None and now_ns > self._last:
            self.tokens = min(float(self.burst), self.tokens + (now_ns - self._last) * self.rate_per_ns)
        if self._last is None or now_ns > self._last:
            self._last = now_ns

    def available(self, now_ns: int) -> bool:
        self._refill(now_ns)
        return self.tokens >= 1.0

    def take(self, now_ns: int) -> bool:
        self._refill(now_ns)
        if self.tokens >= 1.0:
            self.tokens -= 1.0
            return True
        return False


def _sign(x: int) -> int:
    return (x > 0) - (x < 0)


class Executor:
    """Order management of one account (see module docstring)."""

    def __init__(
        self,
        account_id: str,
        *,
        contracts: ContractTable,
        positions: PositionTracker,
        orders: OrderBook,
        gates: Gates,
        cfg: ExecutionConfig,
        new_order: NewOrder,
        rng: random.Random | None = None,
        strategy_tag: str | None = None,
        source: OrderSource = OrderSource.BOT,
    ) -> None:
        self.account_id = account_id
        self.contracts = contracts
        self.positions = positions
        self.orders = orders
        self.gates = gates
        self.cfg = cfg
        self.new_order = new_order
        self.rng = rng or random.Random(0)
        self.strategy_tag = strategy_tag
        self.source = source
        self.limiter = TokenBucket(cfg.max_orders_per_min, cfg.burst)
        self.inflight: dict[str, str] = {}
        self.placed_ns: dict[str, int] = {}
        self.cancel_ns: dict[str, int] = {}
        self.refresh_wanted = False
        self.next_halt_probe_ns = 0
        self.stats: Counter[str] = Counter()

    # ------------------------------------------------------------ decisions
    def decide(self, instrument_id: str, target: int | None, tick: Tick, now_ns: int) -> Action | None:
        """At most one action for ``instrument_id`` towards ``target`` (``None``: hold)."""
        if instrument_id in self.inflight:
            return None
        info = self.contracts.get(instrument_id)
        net = self.positions.net(self.account_id, instrument_id)
        delta = None if target is None else target - net
        gave_up = False
        for o in self.orders.working(self.account_id, instrument_id):
            if not o.own:
                continue  # never touch orders placed by others (e.g. manual orders)
            act = self._manage_working(o, delta, now_ns)
            if act is not None or o.working:
                return act
            gave_up = True  # an in-doubt order was just given up: decide afresh on the next tick
        if gave_up or target is None or delta is None:
            return None
        if not self.gates.all_allowed(instrument_id, now_ns):
            return None
        small = target != 0 and _sign(target) == _sign(net) and abs(delta) < max(1.0, self.cfg.deadband_frac * abs(target))
        if delta == 0 or small:
            return self._halt_probe(instrument_id, target, tick, now_ns, info)
        direction = Direction.BUY if delta > 0 else Direction.SELL
        close_side = side_closed(direction)
        pending = self.orders.pending_closes(self.account_id, instrument_id, close_side)
        closable = self.positions.closable(self.account_id, instrument_id, close_side, pending)[0]
        price_ref = tick.last if tick.last > 0 else (tick.bid1 or tick.ask1 or 0.0)
        cap = self._slice_cap(info, price_ref)
        if closable > 0:
            if not self.gates.close_allowed(instrument_id, now_ns):
                return None
            plan = self.positions.close_plan(
                self.account_id, instrument_id, close_side, min(abs(delta), closable, cap), pending=pending
            )
            if not plan:
                return None
            offset, qty = plan[0]
            kind, why = "close", f"target {target} vs net {net}"
            passive = False
        else:
            want = abs(delta) - pending.total  # closes still working (e.g. a manual close) count first
            if want <= 0:
                return None
            if self.gates.halted:
                return self._halt_probe(instrument_id, target, tick, now_ns, info)
            state = self.gates.open_state(info.product, instrument_id, now_ns, info.sector)
            if not state.allowed:
                return None
            if (direction == Direction.BUY and tick.ask1 is None) or (direction == Direction.SELL and tick.bid1 is None):
                return None  # limit-locked against us: nothing to open into
            qty = min(want, cap, self.gates.size_limit(cap, now_ns))
            kind, why = "open", f"target {target} vs net {net}"
            factor = self.gates.size_factor(info.product)
            if factor < 1.0:  # RESTRICTED: the published order-size factor scales every opening order
                qty = max(1, math.floor(qty * factor))
                why += f", ORDER_SIZE_FACTOR {factor:g}"
            if state.probe:
                qty = min(qty, max(1, self.cfg.probe_qty))
                kind = "probe"
            offset = Offset.OPEN
            passive = self.cfg.passive_prob > 0 and self.rng.random() < self.cfg.passive_prob
        if qty <= 0 or not self.limiter.take(now_ns):
            return None
        price = self._price(tick, direction, info, passive=passive)
        if price is None:
            return None
        return self._submit(instrument_id, direction, offset, qty, price, now_ns, kind=kind, why=why, passive=passive)

    def _halt_probe(self, instrument_id: str, target: int, tick: Tick, now_ns: int, info: ContractInfo) -> Action | None:
        """While HALTED: one 1-lot opening order per ``halt_probe_s`` in the target's direction."""
        if not self.gates.halted or self.cfg.halt_probe_s <= 0 or target == 0 or now_ns < self.next_halt_probe_ns:
            return None
        direction = Direction.BUY if target > 0 else Direction.SELL
        if (direction == Direction.BUY and tick.ask1 is None) or (direction == Direction.SELL and tick.bid1 is None):
            return None
        if not self.limiter.take(now_ns):
            return None
        price = self._price(tick, direction, info, passive=False)
        if price is None:
            return None
        self.next_halt_probe_ns = now_ns + int(self.cfg.halt_probe_s * NS_PER_S)
        self.stats["halt_probe"] += 1
        return self._submit(instrument_id, direction, Offset.OPEN, max(1, self.cfg.probe_qty), price, now_ns,
                            kind="halt_probe", why="account HALTED: opening probe (REJECT_ACCOUNT_HALTED expected)",
                            passive=False)

    def _submit(self, instrument_id: str, direction: Direction, offset: Offset, qty: int, price: float, now_ns: int,
                *, kind: str, why: str, passive: bool) -> SubmitAction:
        req = self.new_order(
            self.account_id,
            instrument_id,
            direction,
            offset,
            qty,
            price,
            strategy_tag=self.strategy_tag,
            source=self.source,
        )
        self.placed_ns[req.client_order_id] = now_ns
        self.inflight[instrument_id] = "submit"
        self.stats["submitted"] += 1
        logger.bind(corr_id=req.client_order_id).info(
            "bot.order",
            account_id=self.account_id,
            instrument_id=instrument_id,
            direction=str(direction),
            offset=str(offset),
            qty=qty,
            price=price,
            kind=kind,
            passive=passive,
            why=why,
        )
        return SubmitAction(req, kind, why)

    def _manage_working(self, o: TrackedOrder, delta: int | None, now_ns: int) -> Action | None:
        cid = o.client_order_id
        placed = self.placed_ns.setdefault(cid, now_ns)
        age_ns = now_ns - placed
        if o.state == ClientState.IN_DOUBT:
            if age_ns >= self.cfg.in_doubt_giveup_s * NS_PER_S:
                self.orders.mark_lost(cid)
                self.stats["lost"] += 1
                logger.bind(corr_id=cid).warning("bot.order_lost", account_id=self.account_id, instrument_id=o.instrument_id)
                return None
            if o.request is not None and age_ns >= self.cfg.in_doubt_resubmit_s * NS_PER_S * max(1, o.submit_attempts):
                self.inflight[o.instrument_id] = "submit"
                self.stats["resubmitted"] += 1
                logger.bind(corr_id=cid).info("bot.order_resubmit", account_id=self.account_id,
                                              instrument_id=o.instrument_id, attempts=o.submit_attempts)
                return SubmitAction(o.request, "resubmit", "in doubt: same client_order_id")
            return None
        if o.state == ClientState.PENDING or o.status is None:
            return None
        if o.cancel_requested:
            sent = self.cancel_ns.get(cid)
            if sent is not None and now_ns - sent >= self.cfg.cancel_retry_s * NS_PER_S:
                o.cancel_requested = False  # outcome unknown: allow another cancel below
            else:
                return None
        if not o.cancellable or not self.gates.cancel_allowed(o.instrument_id, now_ns):
            return None
        why = ""
        if delta is not None:
            wanted = (o.direction == Direction.BUY and delta > 0) or (o.direction == Direction.SELL and delta < 0)
            if not wanted:
                why = f"target moved (delta {delta})"
            elif o.remaining > abs(delta):
                why = f"oversized ({o.remaining} > {abs(delta)})"
        if not why and age_ns >= self.cfg.order_ttl_s * NS_PER_S:
            why = f"ttl {self.cfg.order_ttl_s:g}s"
        if not why:
            return None
        self.inflight[o.instrument_id] = "cancel"
        self.cancel_ns[cid] = now_ns
        self.stats["cancel_sent"] += 1
        logger.bind(corr_id=cid).info("bot.cancel", account_id=self.account_id, instrument_id=o.instrument_id,
                                      remaining=o.remaining, why=why)
        return CancelAction(self.account_id, cid, o.instrument_id, why)

    def _slice_cap(self, info: ContractInfo, price: float) -> int:
        per = info.notional(price) if price > 0 else 0.0
        by_notional = math.floor(self.cfg.slice_notional / per) if per > 0 else self.cfg.max_slice_lots
        return max(1, min(self.cfg.max_slice_lots, info.max_limit_qty, by_notional))

    def _price(self, tick: Tick, direction: Direction, info: ContractInfo, *, passive: bool) -> float | None:
        if passive:
            return passive_price(tick, direction, info.tick_size)
        choices = self.cfg.aggression_ticks or [1]
        return aggressive_price(tick, direction, self.rng.choice(choices), info.tick_size)

    # ------------------------------------------------------------ results
    def on_submit_result(self, req: OrderRequest, reply: Reply, ack: OrderAck | None, now_ns: int) -> None:
        self.inflight.pop(req.instrument_id, None)
        product = self.contracts.get(req.instrument_id).product
        is_open = req.offset == Offset.OPEN
        cid = req.client_order_id
        if reply.ok and (ack is None or ack.accepted):
            self.stats["accepted"] += 1
            self.gates.on_accept(product=product, instrument_id=req.instrument_id, is_open=is_open, now_ns=now_ns)
            logger.bind(corr_id=cid).info("bot.order_accepted", account_id=self.account_id,
                                          instrument_id=req.instrument_id, duplicate=reply.duplicate)
            return
        if reply.code in codes.RETRYABLE:
            self.stats["in_doubt"] += 1
            self.gates.on_reject(reply.code, product=product, instrument_id=req.instrument_id, is_open=is_open,
                                 now_ns=now_ns)
            logger.bind(corr_id=cid).warning("bot.order_in_doubt", account_id=self.account_id,
                                             instrument_id=req.instrument_id, code=reply.code)
            return
        code = ack.reject_code if ack is not None and ack.reject_code else reply.code
        self.stats[f"reject:{code}"] += 1
        self.stats["rejected"] += 1
        if code == codes.REJECT_ACCOUNT_HALTED and self.cfg.halt_probe_s > 0:
            self.next_halt_probe_ns = max(self.next_halt_probe_ns, now_ns + int(self.cfg.halt_probe_s * NS_PER_S))
        self.placed_ns.pop(cid, None)
        self.gates.on_reject(
            code,
            product=product,
            instrument_id=req.instrument_id,
            is_open=is_open,
            now_ns=now_ns,
            qty=req.qty,
            detail=ack.reject_detail if ack is not None else None,
        )
        logger.bind(corr_id=cid).warning(
            "bot.order_rejected",
            account_id=self.account_id,
            instrument_id=req.instrument_id,
            offset=str(req.offset),
            qty=req.qty,
            code=code,
            detail=ack.reject_detail if ack is not None else None,
        )

    def on_cancel_result(self, action: CancelAction, reply: Reply, update: OrderUpdate | None, now_ns: int) -> None:
        self.inflight.pop(action.instrument_id, None)
        o = self.orders.get(action.client_order_id)
        if reply.ok:
            self.stats["cancel_ok"] += 1
            return
        if reply.code == codes.CONFLICT:
            self.refresh_wanted = True  # already terminal on the server; the update may be pending
            return
        if reply.code in codes.RETRYABLE:
            return
        product = self.contracts.get(action.instrument_id).product
        self.gates.on_reject(reply.code, product=product, instrument_id=action.instrument_id,
                             is_open=o.is_open if o is not None else False, now_ns=now_ns)

    def on_order_event(self, order: TrackedOrder, update: OrderUpdate, fill_qty: int, now_ns: int) -> None:
        """Order update for this account (own or foreign)."""
        if update.status == OrderStatus.CANCELED and (update.reason or "") == "CIRCUIT_BREAK_CANCEL":
            self.gates.mark_halted(now_ns)
        if fill_qty > 0:
            self.stats["filled_lots"] += fill_qty
        if order.own and update.status == OrderStatus.BROKER_REJECTED:
            code = update.reject_code or update.reason or "BROKER_REJECTED"
            self.stats[f"broker_reject:{code}"] += 1
            product = self.contracts.get(update.instrument_id).product
            self.gates.on_reject(code, product=product, instrument_id=update.instrument_id, is_open=order.is_open,
                                 now_ns=now_ns, qty=order.qty, detail=update.reject_detail)
        if order.terminal:
            self.placed_ns.pop(order.client_order_id, None)
            self.cancel_ns.pop(order.client_order_id, None)

    def working_orders(self) -> list[TrackedOrder]:
        return [o for o in self.orders.working(self.account_id) if o.own]

    @property
    def idle(self) -> bool:
        """No own working order and no RPC in flight (safe moment for a position resync)."""
        return not self.inflight and not self.working_orders()


__all__ = ["Action", "CancelAction", "Executor", "SubmitAction", "TokenBucket"]
