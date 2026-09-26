"""Alert state machine: one instance per (rule, level, scope_key) (01 §8.14, ADR-007; 03 §4.3 ``rule_states``).

States (02 §7.3 ``RuleSmState``)::

    OK ──breach──> PENDING ──confirm──> ACTIVE ──(not latched) beyond clear line for hold_ms──> OK
       <─back inside before confirm─┘        └─(latched) risk.reset──> REARMED
    REARMED ──breach of the rearm line──> PENDING ──confirm──> ACTIVE (new episode)
    REARMED ──beyond the clear line for hold_ms──> OK;   any state ──day roll──> OK

Rules implemented exactly as 01 §8.14:

- **confirm**: ``confirm.states`` counts *distinct* LIVE market states (the
  engine passes a per-account state key that only changes on a new LIVE tick
  of a held instrument; fills and cash do not count), or ``confirm.ms``
  elapsed in PENDING - whichever comes first; ``states=1`` confirms
  immediately (no PENDING transition is reported); ``states=0`` is
  time-only confirmation (only ``confirm.ms`` confirms; compile rejects
  ``states=0`` with ``ms=0``);
- **clear line**: threshold × (1 ∓ hysteresis) on the safe side; leaving ACTIVE
  needs ``hold_ms`` continuously beyond it; latched levels never auto-clear;
- **cooldown** (ALERT levels only): after ACTIVE → OK the same level cannot
  trigger for ``cooldown_s`` unless a more severe level of the same rule and
  scope is breached at the same time (escalation is never delayed). RESTRICT,
  CIRCUIT_BREAK and LIQUIDATE levels re-activate on every re-breach (a
  cooldown would switch the restriction off: fail-open);
- **REARMED**: rearm line = min(threshold, value at reset − extra),
  direction-aware; a re-breach goes through the same confirmation (so one bad
  tick cannot re-latch the account) and becomes a new episode;
- **quality gating**: while the metric's market-data quality is not OK the
  machine does not move at all, timers included; a running hold timer is reset
  (it must not span an untrusted period);
- **episode**: +1 on every entry into ACTIVE; the alert key is
  ``{td}|{rule_id}|{level}|{scope_key}|{episode}`` (02 §7.3);
- **day roll**: every instance returns to OK with episode 0; latches are
  released when ``reset_on_day_roll`` (default); otherwise a latched level is
  carried into the new day as episode 1.

The machine is pure: it only mutates its own :class:`SmState` and reports
:class:`Tr` transitions; the engine turns them into events, rows and actions.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from futrisk.core.rules import CompiledRule, LevelSpec, alert_key
from futrisk.messages.enums import Action, RuleSmState

NS_PER_MS = 1_000_000


class Tr(StrEnum):
    """Transition kinds reported by :class:`LevelMachine`."""

    PENDING = "PENDING"  # breach seen, waiting for confirmation (reported only when not immediate)
    PROGRESS = "PROGRESS"  # persisted field changed without a state change (confirm count, clear timer)
    REVOKED = "REVOKED"  # PENDING -> OK / REARMED: back inside before confirmation
    ACTIVATED = "ACTIVATED"  # -> ACTIVE, new episode (ALERT_RAISED)
    CLEARED = "CLEARED"  # ACTIVE -> OK (ALERT_CLEARED)
    REARM_CLEARED = "REARM_CLEARED"  # REARMED -> OK
    RESET = "RESET"  # latched ACTIVE -> REARMED (risk.reset)
    ROLLED = "ROLLED"  # day roll


@dataclass(slots=True)
class SmState:
    """Persisted state of one machine (columns of ``rule_states``)."""

    state: RuleSmState = RuleSmState.OK
    episode: int = 0
    latched: bool = False
    confirm_count: int = 0
    pending_since_ns: int | None = None
    active_since_ns: int | None = None
    clear_since_ns: int | None = None
    cooldown_until_ns: int | None = None
    rearm_threshold: float | None = None
    last_value: float | None = None
    last_state_key: int | None = field(default=None)  # not persisted: None after a restore

    def persisted(self) -> tuple[Any, ...]:
        """Fields stored in ``rule_states`` except ``last_value`` (used to detect changes)."""
        return (
            self.state,
            self.episode,
            self.latched,
            self.confirm_count,
            self.pending_since_ns,
            self.active_since_ns,
            self.clear_since_ns,
            self.cooldown_until_ns,
            self.rearm_threshold,
        )


class LevelMachine:
    """State machine of one ``(rule, level, scope_key)``."""

    __slots__ = ("level", "rule", "scope_key", "st")

    def __init__(self, rule: CompiledRule, level: LevelSpec, scope_key: str, state: SmState | None = None) -> None:
        self.rule = rule
        self.level = level
        self.scope_key = scope_key
        self.st = state if state is not None else SmState()

    def __repr__(self) -> str:
        return f"LevelMachine({self.rule.rule_id}/{self.level.name} {self.scope_key} {self.st.state} ep={self.st.episode})"

    # ------------------------------------------------------------ helpers
    @property
    def state(self) -> RuleSmState:
        return self.st.state

    @property
    def key(self) -> tuple[str, str, str]:
        return (self.rule.rule_id, self.level.name, self.scope_key)

    def alert_key(self, trading_day: str, episode: int | None = None) -> str:
        return alert_key(trading_day, self.rule.rule_id, self.level.name, self.scope_key, self.st.episode if episode is None else episode)

    def breach_line(self) -> float:
        """Threshold in force: the rearm line while REARMED (or PENDING from REARMED)."""
        st = self.st
        if st.rearm_threshold is not None and st.state in (RuleSmState.REARMED, RuleSmState.PENDING):
            return st.rearm_threshold
        return self.level.threshold

    def in_breach(self, value: float | None) -> bool:
        return value is not None and self.rule.breached(value, self.breach_line())

    def is_effective(self) -> bool:
        """True when the level's action is in force (ACTIVE; PENDING for circuit breaks, see actions)."""
        return self.st.state == RuleSmState.ACTIVE

    def _confirmed(self, now_ns: int) -> bool:
        st, lv = self.st, self.level
        if lv.confirm_states > 0 and st.confirm_count >= lv.confirm_states:
            return True  # confirm.states = 0: the distinct-state path is disabled (time-only)
        return (
            lv.confirm_ms > 0
            and st.pending_since_ns is not None
            and now_ns - st.pending_since_ns >= lv.confirm_ms * NS_PER_MS
        )

    def _activate(self, now_ns: int) -> None:
        st = self.st
        st.state = RuleSmState.ACTIVE
        st.episode += 1
        st.latched = self.level.latch
        st.active_since_ns = now_ns
        st.pending_since_ns = None
        st.confirm_count = 0
        st.clear_since_ns = None
        st.rearm_threshold = None

    def _to_ok(self) -> None:
        st = self.st
        st.state = RuleSmState.OK
        st.latched = False
        st.confirm_count = 0
        st.pending_since_ns = None
        st.active_since_ns = None
        st.clear_since_ns = None
        st.rearm_threshold = None

    # --------------------------------------------------------------- step
    def evaluate(
        self,
        value: float | None,
        *,
        quality_ok: bool,
        state_key: int | None,
        now_ns: int,
        escalating: bool = False,
    ) -> list[Tr]:
        """Feed one evaluation; returns the transitions (empty when nothing persisted changed).

        ``value`` None means "not applicable" (e.g. effective N of a flat
        account): never a breach, always beyond the clear line. ``state_key``
        identifies the account's LIVE market state; None means "no new state"
        (timer re-checks).
        """
        if not quality_ok:
            # 01 §8.14 质量门控：行情质量不是 OK 时，状态机保持不动，既不升级也不降级。
            # 解除的保持计时（hold_ms）不能跨越行情不可信的时段：计时清零，恢复后重新开始。
            if self.st.clear_since_ns is not None:
                self.st.clear_since_ns = None
                return [Tr.PROGRESS]
            return []
        st = self.st
        before = st.persisted()
        st.last_value = value
        out: list[Tr] = []
        state = st.state
        if state == RuleSmState.OK:
            if self.in_breach(value):
                cooling = (
                    self.level.action == Action.ALERT
                    and st.cooldown_until_ns is not None
                    and now_ns < st.cooldown_until_ns
                )
                if cooling and not escalating:
                    # 01 §8.14 冷却：回到 OK 后 cooldown_s 之内同一告警级别不再触发（升级不受限）。
                    # 冷却只作用于 ALERT：RESTRICT / CIRCUIT_BREAK / LIQUIDATE 再次越线照常生效，
                    # 否则冷却期内限制会失效（fail-open）。
                    return []
                st.state = RuleSmState.PENDING
                st.pending_since_ns = now_ns
                st.confirm_count = 1
                st.last_state_key = state_key
                st.cooldown_until_ns = None
                if self._confirmed(now_ns):
                    self._activate(now_ns)
                    out.append(Tr.ACTIVATED)
                else:
                    out.append(Tr.PENDING)
        elif state == RuleSmState.PENDING:
            if not self.in_breach(value):
                back_to_rearmed = st.rearm_threshold is not None
                st.pending_since_ns = None
                st.confirm_count = 0
                st.last_state_key = None
                st.state = RuleSmState.REARMED if back_to_rearmed else RuleSmState.OK
                out.append(Tr.REVOKED)
            else:
                if state_key is None:
                    pass  # timer re-check: no new market state
                elif st.last_state_key is None:
                    st.last_state_key = state_key  # first state seen after a restore or a timer trigger
                elif state_key != st.last_state_key:
                    # 01 §8.14：confirm.states 计数的是不同的 LIVE 市场状态（states=0 只按时间确认，不计数）
                    if self.level.confirm_states > 0:
                        st.confirm_count += 1
                    st.last_state_key = state_key
                if self._confirmed(now_ns):
                    self._activate(now_ns)
                    out.append(Tr.ACTIVATED)
        elif state == RuleSmState.ACTIVE:
            if not st.latched:
                out.extend(self._clear_progress(value, now_ns, cleared=Tr.CLEARED))
        else:  # REARMED
            if self.in_breach(value):
                st.state = RuleSmState.PENDING
                st.pending_since_ns = now_ns
                st.confirm_count = 1
                st.last_state_key = state_key
                st.clear_since_ns = None
                if self._confirmed(now_ns):
                    self._activate(now_ns)
                    out.append(Tr.ACTIVATED)
                else:
                    out.append(Tr.PENDING)
            else:
                out.extend(self._clear_progress(value, now_ns, cleared=Tr.REARM_CLEARED))
        if not out and st.persisted() != before:
            out.append(Tr.PROGRESS)
        return out

    def _clear_progress(self, value: float | None, now_ns: int, *, cleared: Tr) -> list[Tr]:
        st = self.st
        beyond = value is None or self.rule.beyond_clear(value, self.level.threshold)
        if not beyond:
            st.clear_since_ns = None
            return []
        if st.clear_since_ns is None:
            st.clear_since_ns = now_ns
        if now_ns - st.clear_since_ns >= self.rule.hold_ms * NS_PER_MS:
            self._to_ok()
            if cleared == Tr.CLEARED and self.level.action == Action.ALERT and self.rule.cooldown_ms > 0:
                st.cooldown_until_ns = now_ns + self.rule.cooldown_ms * NS_PER_MS
            return [cleared]
        return []

    def reevaluate(self, now_ns: int, *, quality_ok: bool, escalating: bool = False) -> list[Tr]:
        """Timer re-check with the last value and no new market state (confirm.ms, hold_ms, cooldown end)."""
        return self.evaluate(self.st.last_value, quality_ok=quality_ok, state_key=None, now_ns=now_ns, escalating=escalating)

    def due_ns(self) -> int | None:
        """Next time a timer could change this machine without a new market state."""
        st = self.st
        cands: list[int] = []
        if st.state == RuleSmState.PENDING and self.level.confirm_ms > 0 and st.pending_since_ns is not None:
            cands.append(st.pending_since_ns + self.level.confirm_ms * NS_PER_MS)
        if st.state in (RuleSmState.ACTIVE, RuleSmState.REARMED) and st.clear_since_ns is not None:
            cands.append(st.clear_since_ns + self.rule.hold_ms * NS_PER_MS)
        if (
            st.state == RuleSmState.OK
            and self.level.action == Action.ALERT
            and st.cooldown_until_ns is not None
            and self.in_breach(st.last_value)
        ):
            cands.append(st.cooldown_until_ns)
        return min(cands) if cands else None

    # ------------------------------------------------------ manual / roll
    def reset(self, now_ns: int) -> bool:
        """``risk.reset``: a latched ACTIVE level becomes REARMED; returns False when not latched."""
        st = self.st
        if st.state != RuleSmState.ACTIVE or not st.latched:
            return False
        value = st.last_value if st.last_value is not None else self.level.threshold
        st.state = RuleSmState.REARMED
        st.latched = False
        st.rearm_threshold = self.rule.rearm_line(self.level.threshold, value, self.level.rearm_extra)
        st.clear_since_ns = None
        st.pending_since_ns = None
        st.confirm_count = 0
        st.last_state_key = None
        return True

    def day_roll(self, now_ns: int) -> bool:
        """Reset for a new trading day; returns True when a latch was released."""
        st = self.st
        was_latched = st.state == RuleSmState.ACTIVE and st.latched
        carry = was_latched and not self.rule.reset_on_day_roll
        self.st = SmState()
        if carry:
            # reset_on_day_roll=false：锁存延续到新交易日，作为新交易日的第 1 个 episode
            self.st.state = RuleSmState.ACTIVE
            self.st.latched = True
            self.st.episode = 1
            self.st.active_since_ns = now_ns
            return False
        return was_latched

    # --------------------------------------------------------- persistence
    def to_row(self, trading_day: str, config_hash: str, now_ns: int) -> dict[str, Any]:
        """``rule_states`` row (03 §4.3)."""
        st = self.st
        return {
            "trading_day": trading_day,
            "rule_id": self.rule.rule_id,
            "level": self.level.name,
            "scope_key": self.scope_key,
            "state": st.state.value,
            "episode": st.episode,
            "latched": st.latched,
            "confirm_count": st.confirm_count,
            "pending_since_ns": st.pending_since_ns,
            "active_since_ns": st.active_since_ns,
            "clear_since_ns": st.clear_since_ns,
            "cooldown_until_ns": st.cooldown_until_ns,
            "rearm_threshold": st.rearm_threshold,
            "last_value": st.last_value,
            "config_hash": config_hash,
            "updated_ns": now_ns,
        }


def state_from_row(row: dict[str, Any]) -> SmState:
    """Rebuild :class:`SmState` from a ``rule_states`` row."""

    def opt_int(name: str) -> int | None:
        v = row.get(name)
        return None if v is None else int(v)

    def opt_float(name: str) -> float | None:
        v = row.get(name)
        return None if v is None else float(v)

    return SmState(
        state=RuleSmState(str(row["state"])),
        episode=int(row.get("episode") or 0),
        latched=bool(row.get("latched")),
        confirm_count=int(row.get("confirm_count") or 0),
        pending_since_ns=opt_int("pending_since_ns"),
        active_since_ns=opt_int("active_since_ns"),
        clear_since_ns=opt_int("clear_since_ns"),
        cooldown_until_ns=opt_int("cooldown_until_ns"),
        rearm_threshold=opt_float("rearm_threshold"),
        last_value=opt_float("last_value"),
        last_state_key=None,
    )


__all__ = ["LevelMachine", "SmState", "Tr", "state_from_row"]
