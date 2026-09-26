"""The four rule actions and their registry (01 §8.14 四种动作; 02 §10 ``@register_action``).

An action handler maps an *effective* level (ACTIVE, or PENDING for circuit
breaks) to its contribution to the account state and its restrictions:

- ``ALERT`` → WARNING only;
- ``RESTRICT`` → RESTRICTED + restrictions (:mod:`futrisk.core.restrictions`),
  never latched, removed when the level clears;
- ``CIRCUIT_BREAK`` → HALTED as soon as the level is PENDING
  (``halted_latched=false``), latched once confirmed (01 §8.14, ADR-007). The
  engine cancels every resting open order when the account enters HALTED;
- ``LIQUIDATE`` → no state change; on activation the engine builds a
  :class:`LiquidationPlan` (:func:`liquidation_steps`) that a risk manager
  confirms or cancels. Plans are never executed automatically (01 TD-08).

One-shot side effects (cancels, plans) are driven by transitions in the engine;
handlers here are pure functions of the level state.
"""

from __future__ import annotations

import math
from collections.abc import Callable, Iterable
from dataclasses import dataclass

from futrisk.core.restrictions import StateContribution, level_restrictions
from futrisk.core.rules import LevelSpec
from futrisk.messages.enums import AccountState, Action, Direction, Offset, PosDirection, RuleSmState
from futrisk.messages.risk import LiquidationStep, Restriction


@dataclass(frozen=True, slots=True)
class ActionInput:
    rule_id: str
    level: LevelSpec
    scope_key: str
    account_id: str | None
    sm_state: RuleSmState
    latched: bool
    largest_product: str | None
    since_ns: int


@dataclass(frozen=True, slots=True)
class ActionOutcome:
    contribution: StateContribution | None = None
    restrictions: tuple[Restriction, ...] = ()


ActionFn = Callable[[ActionInput], ActionOutcome]
_ACTIONS: dict[Action, ActionFn] = {}


def register_action(action: Action) -> Callable[[ActionFn], ActionFn]:
    """Decorator registering the handler of an :class:`Action`."""

    def deco(fn: ActionFn) -> ActionFn:
        _ACTIONS[action] = fn
        return fn

    return deco


def is_effective(action: Action, sm_state: RuleSmState) -> bool:
    """A level acts while ACTIVE; a circuit break already while PENDING (01 §8.14)."""
    if sm_state == RuleSmState.ACTIVE:
        return True
    return action == Action.CIRCUIT_BREAK and sm_state == RuleSmState.PENDING


def action_outcome(inp: ActionInput) -> ActionOutcome:
    if not is_effective(inp.level.action, inp.sm_state):
        return ActionOutcome()
    return _ACTIONS[inp.level.action](inp)


@register_action(Action.ALERT)
def _alert(inp: ActionInput) -> ActionOutcome:
    return ActionOutcome(StateContribution(AccountState.WARNING))


@register_action(Action.RESTRICT)
def _restrict(inp: ActionInput) -> ActionOutcome:
    rs = level_restrictions(
        inp.rule_id, inp.level, inp.scope_key, inp.account_id, largest_product=inp.largest_product, since_ns=inp.since_ns
    )
    return ActionOutcome(StateContribution(AccountState.RESTRICTED), tuple(rs))


@register_action(Action.CIRCUIT_BREAK)
def _circuit_break(inp: ActionInput) -> ActionOutcome:
    # PENDING：立即 HALTED（未锁存）；确认后锁存
    latched = inp.sm_state == RuleSmState.ACTIVE and inp.latched
    return ActionOutcome(StateContribution(AccountState.HALTED, latched=latched))


@register_action(Action.LIQUIDATE)
def _liquidate(inp: ActionInput) -> ActionOutcome:
    return ActionOutcome()  # 01 §8.14：LIQUIDATE 不单独改变账户状态


@dataclass(frozen=True, slots=True)
class LiquidationCandidate:
    """A position as seen by the liquidation planner."""

    instrument_id: str
    direction: PosDirection
    closable_today: int
    closable_yd: int
    position_pnl: float
    explicit_close_today: bool


def price_rule(direction: Direction, ticks: int) -> str:
    """Suggested price "对手价 ± N 跳" (02 §7.6 LiquidationPlan.steps.price_rule)."""
    return f"对手价-{ticks}跳" if direction == Direction.SELL else f"对手价+{ticks}跳"


def liquidation_steps(candidates: Iterable[LiquidationCandidate], *, step: float, price_ticks: int = 2) -> list[LiquidationStep]:
    """Reduce every position by ``step`` (ceil, at least one lot), losers first.

    SHFE/INE positions are split into CLOSE_YESTERDAY then CLOSE_TODAY steps
    (explicit close-today exchanges); other exchanges use CLOSE.
    """
    frac = min(max(step, 0.0), 1.0)
    out: list[LiquidationStep] = []
    for c in sorted(candidates, key=lambda x: (x.position_pnl, x.instrument_id, x.direction.value)):
        total = c.closable_today + c.closable_yd
        if total <= 0 or frac <= 0:
            continue
        qty = min(total, max(1, math.ceil(total * frac - 1e-9)))
        side = Direction.SELL if c.direction == PosDirection.LONG else Direction.BUY
        rule = price_rule(side, price_ticks)
        if c.explicit_close_today:
            yd = min(qty, c.closable_yd)
            td = qty - yd
            if yd:
                out.append(LiquidationStep(instrument_id=c.instrument_id, direction=side,
                                           offset=Offset.CLOSE_YESTERDAY, qty=yd, price_rule=rule))
            if td:
                out.append(LiquidationStep(instrument_id=c.instrument_id, direction=side,
                                           offset=Offset.CLOSE_TODAY, qty=td, price_rule=rule))
        else:
            out.append(LiquidationStep(instrument_id=c.instrument_id, direction=side, offset=Offset.CLOSE, qty=qty,
                                       price_rule=rule))
    return out


__all__ = [
    "ActionInput",
    "ActionOutcome",
    "LiquidationCandidate",
    "action_outcome",
    "is_effective",
    "liquidation_steps",
    "price_rule",
    "register_action",
]
