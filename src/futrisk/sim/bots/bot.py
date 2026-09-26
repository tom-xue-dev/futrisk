"""One account's bot: strategy + executor + gates over a shared client (01 §7.1, §6.2 R2).

``AccountBot.on_tick`` updates the strategy with the tick and, when trading is enabled,
asks the executor for at most one action on that instrument. ``on_risk_update`` adopts the
account's published risk state (``risk.state`` / ``risk.snapshot``: HALTED, restrictions,
equity) into the gates and the strategy context. Everything here is synchronous and
side-effect free apart from state updates; the host performs the returned actions through
:class:`~futrisk.client.FutriskClient`.
"""

from __future__ import annotations

from typing import Any

from loguru import logger

from futrisk.client.client import OrderEvent
from futrisk.client.riskview import RiskUpdate
from futrisk.messages.md import Tick
from futrisk.messages.risk import AccountSnapshot
from futrisk.sim.bots.executor import Action, Executor
from futrisk.sim.bots.gates import Gates
from futrisk.sim.bots.strategies import Strategy, StrategyContext


class AccountBot:
    """Strategy, executor and context of one account."""

    def __init__(self, account_id: str, *, strategy: Strategy, executor: Executor, ctx: StrategyContext) -> None:
        self.account_id = account_id
        self.strategy = strategy
        self.executor = executor
        self.ctx = ctx
        self.trading = False
        self._day: str | None = None
        self.errors = 0
        self.last_targets: dict[str, int] = {}
        self.drift = 0  # consecutive risk.snapshot positions differing from the tracker

    @property
    def gates(self) -> Gates:
        return self.ctx.gates

    @property
    def instruments(self) -> tuple[str, ...]:
        return self.strategy.instruments

    def on_tick(self, tick: Tick) -> list[Action]:
        """Feed one tick; returns the actions to perform (empty while trading is disabled)."""
        self.ctx.update_tick(tick)
        if tick.trading_day and tick.trading_day != self._day:
            first = self._day is None
            self._day = tick.trading_day
            if not first:
                logger.info("bot.day_roll", account_id=self.account_id, trading_day=tick.trading_day)
                self.gates.on_day_roll()
                self.strategy.on_day_roll(tick.trading_day, self.ctx)
        try:
            self.strategy.on_tick(tick, self.ctx)
            if not self.trading:
                return []
            self.last_targets = self.strategy.targets(self.ctx)
            target = self.last_targets.get(tick.instrument_id)
            act = self.executor.decide(tick.instrument_id, target, tick, self.ctx.now_ns)
        except Exception:
            self.errors += 1
            logger.exception("bot.tick_failed", account_id=self.account_id, instrument_id=tick.instrument_id)
            return []
        return [act] if act is not None else []

    def on_order_event(self, event: OrderEvent) -> None:
        try:
            fill_qty = event.fill.qty if event.fill is not None else 0
            self.executor.on_order_event(event.order, event.update, fill_qty, self.ctx.now_ns)
            if event.fill is not None:
                logger.bind(corr_id=event.order.client_order_id).info(
                    "bot.fill",
                    account_id=self.account_id,
                    instrument_id=event.update.instrument_id,
                    direction=str(event.update.direction),
                    offset=str(event.update.offset),
                    qty=event.fill.qty,
                    price=event.fill.price,
                    own=event.order.own,
                    net=self.ctx.net(event.update.instrument_id),
                )
        except Exception:
            self.errors += 1
            logger.exception("bot.order_event_failed", account_id=self.account_id)

    def on_risk_update(self, update: RiskUpdate) -> None:
        """Published state and restrictions -> gates (HALTED without an opening attempt); equity -> context."""
        try:
            view = update.view
            if view.known:
                now = self.ctx.now_ns or update.ts_ns
                self.gates.apply_risk(view.state, view.halted_latched, view.restrictions, now, view.state_version)
            equity = view.equity
            if equity is not None and equity > 0:
                self.ctx.equity = equity
        except Exception:
            self.errors += 1
            logger.exception("bot.risk_update_failed", account_id=self.account_id)

    def positions_match(self, snap: AccountSnapshot) -> bool:
        """True when the tracker holds the same lots (today/yesterday, per side) as the engine snapshot."""
        theirs: dict[str, dict[str, tuple[int, int]]] = {}
        for v in snap.positions:
            if v.account_id == self.account_id and (v.td_qty or v.yd_qty):
                theirs.setdefault(v.instrument_id, {})[str(v.direction)] = (v.td_qty, v.yd_qty)
        return self.ctx.positions.snapshot(self.account_id) == theirs

    def status(self) -> dict[str, Any]:
        """Summary for the periodic status log (no state change: targets are the last computed ones)."""
        return {
            "account_id": self.account_id,
            "strategy": self.strategy.name,
            "trading": self.trading,
            "equity": round(self.ctx.equity),
            "halted": self.gates.halted,
            "positions": self.ctx.positions.snapshot(self.account_id),
            "targets": dict(self.last_targets),
            "working": len(self.executor.working_orders()),
            "blocks": self.gates.blocks(self.ctx.now_ns),
            "stats": dict(self.executor.stats),
            "signal": self.strategy.describe(),
            "errors": self.errors,
        }


__all__ = ["AccountBot"]
