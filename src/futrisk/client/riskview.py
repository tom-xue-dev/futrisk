"""An account's risk state as seen by a strategy: ``risk.state.{acct}`` + ``risk.snapshot.{acct}`` (02 §5, §7.6).

Both topics come from the shard owning the account:

- ``risk.state.{account}`` (FACT, :class:`AccountStateChanged`) on every change of the
  account state, the ``halted_latched`` flag or the restriction set;
- ``risk.snapshot.{account}`` (LV, :class:`AccountSnapshot`, <= 2 Hz) with funds (equity),
  positions, exposures, state and restrictions.

02 §5 rule 2: the state and the restrictions are taken from whichever of the two carries
the larger ``state_version`` (persisted by the engine, so it keeps growing across
restarts); funds and positions come from the latest snapshot (``as_of_ns``, the engine's
market clock, which never goes back). Merging is idempotent and order-independent, so a
start-up ``ctl.snapshot`` (Clone, 02 §4.6) and live messages can arrive in any order.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from futrisk.messages.enums import AccountState
from futrisk.messages.risk import AccountSnapshot, AccountStateChanged, Restriction


@dataclass
class RiskView:
    """Latest known risk state of one account (see the module docstring)."""

    account_id: str
    state: AccountState = AccountState.NORMAL
    halted_latched: bool = False
    restrictions: list[Restriction] = field(default_factory=list)
    state_version: int = -1
    trading_day: str | None = None
    snapshot: AccountSnapshot | None = None
    as_of_ns: int = -1
    known: bool = False

    @property
    def halted(self) -> bool:
        return self.state == AccountState.HALTED

    @property
    def equity(self) -> float | None:
        if self.snapshot is None:
            return None
        return self.snapshot.funds.equity

    def _set_state(self, state: AccountState, latched: bool, restrictions: list[Restriction], version: int) -> bool:
        changed = (state, latched, restrictions) != (self.state, self.halted_latched, self.restrictions)
        self.state, self.halted_latched, self.restrictions = state, latched, list(restrictions)
        self.state_version = version
        self.known = True
        return changed

    def apply_state(self, msg: AccountStateChanged) -> bool:
        """Apply a ``risk.state`` message; returns True when state/latch/restrictions changed."""
        if msg.account_id != self.account_id or msg.state_version < self.state_version:
            return False
        self.trading_day = msg.trading_day or self.trading_day
        return self._set_state(msg.to_state, msg.halted_latched, msg.restrictions, msg.state_version)

    def apply_snapshot(self, snap: AccountSnapshot) -> tuple[bool, bool]:
        """Apply a ``risk.snapshot``; returns ``(snapshot_taken, state_changed)``."""
        if snap.account_id != self.account_id or snap.as_of_ns < self.as_of_ns:
            return False, False
        self.snapshot = snap
        self.as_of_ns = snap.as_of_ns
        self.trading_day = snap.trading_day or self.trading_day
        changed = False
        if snap.state_version >= self.state_version:
            changed = self._set_state(snap.state, snap.halted_latched, snap.restrictions, snap.state_version)
        return True, changed


@dataclass(frozen=True, slots=True)
class RiskUpdate:
    """One applied risk message.

    ``source`` is ``state``, ``snapshot``, ``clone`` (start-up ``ctl.snapshot``) or ``query``
    (``risk.query.accounts``); ``ts_ns`` is the engine time of the message (``ts_ns`` of the
    state change, ``as_of_ns`` of a snapshot).
    """

    account_id: str
    view: RiskView
    source: str
    state_changed: bool
    snapshot: AccountSnapshot | None = None
    ts_ns: int = 0


__all__ = ["RiskUpdate", "RiskView"]
