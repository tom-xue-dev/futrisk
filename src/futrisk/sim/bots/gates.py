"""Reject handling for the bots: which orders to hold back, where, and for how long (02 §11).

Every reject code maps to a :class:`GateRule`: the **scope** it blocks (account,
product or instrument), the **kind** of orders it blocks (opening only, all, closing
only, cancels) and a back-off class. A repeated reject doubles the back-off up to its
maximum; time is market time (tick timestamps), so the behaviour is deterministic
given the tick stream.

Opening-only blocks follow the pre-trade pipeline (02 §11.2): halted/blocked accounts,
the firm kill switch, stale market data, product blocks, add-to-loser, exposure caps,
what-if and funds only stop *opening* orders - closing orders are exempt from those
steps, so the bots keep managing (and reducing) their positions.

When an account- or product-level opening block expires, the next opening order is a
*probe* (``probe_qty`` lots). An accepted opening order clears the opening blocks of
its account/product/instrument; a rejected probe re-arms the block with a longer
back-off. ``REJECT_ACCOUNT_HALTED`` (or a ``CIRCUIT_BREAK_CANCEL`` of an own order)
also sets :attr:`Gates.halted`, which strategies may use to de-risk. The day roll
clears day-scoped blocks and the halt (latches are released at the day roll by
default, 01 §8.14).

The account's published risk state (``risk.state`` / ``risk.snapshot``, :meth:`Gates.apply_risk`)
is the primary source: HALTED is known without an opening attempt, ``BLOCK_OPEN``
restrictions hold opening orders back for as long as they exist (account, product,
sector or instrument scope; firm scopes too), ``ORDER_SIZE_FACTOR`` scales opening sizes
(:meth:`Gates.size_factor`) and ``EXPOSURE_CAP_FACTOR`` the product caps. ``NO_ADD_TO_LOSERS``
is left to the engine (02 §11.2 step 4 decides what a loser is): the rejects it produces
arm a product back-off. While HALTED, opening orders go out only as the executor's periodic
1-lot probes (``halt_probe_s``), so the halt stays visible without a storm.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from loguru import logger

from futrisk.messages import codes
from futrisk.messages.enums import AccountState, RestrictionKind
from futrisk.messages.risk import Restriction
from futrisk.sim.bots.config import GatesConfig
from futrisk.sim.bots.indicators import NS_PER_S


class Scope(StrEnum):
    ACCOUNT = "ACCOUNT"
    PRODUCT = "PRODUCT"
    INSTRUMENT = "INSTRUMENT"


class Kind(StrEnum):
    OPEN = "OPEN"  # opening orders only
    ALL = "ALL"  # every order
    CLOSE = "CLOSE"  # closing orders only
    CANCEL = "CANCEL"  # order.cancel


@dataclass(frozen=True, slots=True)
class GateRule:
    scope: Scope
    kind: Kind
    backoff: str  # halt | block | transport | short | long | day
    halted: bool = False
    resync: bool = False
    size: bool = False


_R = GateRule
# 02 §11.1/§11.2/§11.3 -> what to hold back
RULES: dict[str, GateRule] = {
    # step 1: static validation - a bug or a config mismatch; stop trying for a long time
    codes.REJECT_UNKNOWN_ACCOUNT: _R(Scope.ACCOUNT, Kind.ALL, "long"),
    codes.REJECT_UNKNOWN_INSTRUMENT: _R(Scope.INSTRUMENT, Kind.ALL, "long"),
    codes.REJECT_NOT_TRADABLE: _R(Scope.INSTRUMENT, Kind.ALL, "long"),
    codes.REJECT_INVALID_ORDER: _R(Scope.INSTRUMENT, Kind.ALL, "long"),
    codes.REJECT_INVALID_QTY: _R(Scope.INSTRUMENT, Kind.ALL, "short", size=True),
    codes.REJECT_QTY_EXCEEDS_MAX: _R(Scope.INSTRUMENT, Kind.ALL, "short", size=True),
    codes.REJECT_PRICE_OFF_TICK: _R(Scope.INSTRUMENT, Kind.ALL, "long"),
    codes.REJECT_DUPLICATE_ORDER_ID: _R(Scope.INSTRUMENT, Kind.ALL, "short"),
    # step 2: system state
    codes.REJECT_SYSTEM_NOT_READY: _R(Scope.ACCOUNT, Kind.ALL, "transport"),
    codes.REJECT_EOD_IN_PROGRESS: _R(Scope.ACCOUNT, Kind.ALL, "block"),
    codes.REJECT_SESSION_CLOSED: _R(Scope.INSTRUMENT, Kind.ALL, "block"),
    codes.REJECT_MD_UNAVAILABLE: _R(Scope.INSTRUMENT, Kind.OPEN, "block"),
    codes.REJECT_FIRM_KILL_SWITCH: _R(Scope.ACCOUNT, Kind.OPEN, "halt"),
    codes.REJECT_GATEWAY_UNAVAILABLE: _R(Scope.ACCOUNT, Kind.ALL, "transport"),
    # step 3: price
    codes.REJECT_PRICE_LIMIT: _R(Scope.INSTRUMENT, Kind.ALL, "short"),
    codes.REJECT_FAT_FINGER: _R(Scope.INSTRUMENT, Kind.ALL, "short"),
    # step 4: account and restrictions (opening orders only)
    codes.REJECT_ACCOUNT_HALTED: _R(Scope.ACCOUNT, Kind.OPEN, "halt", halted=True),
    codes.REJECT_ACCOUNT_BLOCKED: _R(Scope.ACCOUNT, Kind.OPEN, "block"),
    codes.REJECT_PRODUCT_BLOCKED: _R(Scope.PRODUCT, Kind.OPEN, "block"),
    codes.REJECT_ORDER_SIZE_RESTRICTED: _R(Scope.INSTRUMENT, Kind.OPEN, "short", size=True),
    codes.REJECT_ADD_TO_LOSER: _R(Scope.PRODUCT, Kind.OPEN, "block"),
    codes.REJECT_EXPOSURE_CAP: _R(Scope.PRODUCT, Kind.OPEN, "block"),
    # step 5: throttling / compliance
    codes.REJECT_THROTTLED: _R(Scope.ACCOUNT, Kind.ALL, "short"),
    codes.REJECT_OPEN_VOLUME_LIMIT: _R(Scope.INSTRUMENT, Kind.OPEN, "day"),
    codes.REJECT_CANCEL_LIMIT: _R(Scope.INSTRUMENT, Kind.CANCEL, "day"),
    codes.REJECT_LARGE_CANCEL_LIMIT: _R(Scope.INSTRUMENT, Kind.CANCEL, "day"),
    # step 6: positions (our copy drifted: resync)
    codes.REJECT_CLOSE_EXCEEDS_POSITION: _R(Scope.INSTRUMENT, Kind.CLOSE, "short", resync=True),
    codes.REJECT_CLOSE_TODAY_EXCEEDS: _R(Scope.INSTRUMENT, Kind.CLOSE, "short", resync=True),
    codes.REJECT_CLOSE_YD_EXCEEDS: _R(Scope.INSTRUMENT, Kind.CLOSE, "short", resync=True),
    codes.REJECT_POSITION_LIMIT: _R(Scope.INSTRUMENT, Kind.OPEN, "long"),
    # steps 7-9
    codes.REJECT_INSUFFICIENT_FUNDS: _R(Scope.ACCOUNT, Kind.OPEN, "block"),
    codes.REJECT_WHATIF_BREACH: _R(Scope.ACCOUNT, Kind.OPEN, "block"),
    codes.REJECT_SELF_TRADE: _R(Scope.INSTRUMENT, Kind.ALL, "short"),
    # generic Reply codes (02 §11.1)
    codes.TIMEOUT: _R(Scope.ACCOUNT, Kind.ALL, "transport"),
    codes.UNAVAILABLE: _R(Scope.ACCOUNT, Kind.ALL, "transport"),
    codes.BUSY: _R(Scope.ACCOUNT, Kind.ALL, "transport"),
    codes.RISK_BUSY: _R(Scope.ACCOUNT, Kind.ALL, "transport"),
    codes.NOT_OWNER: _R(Scope.ACCOUNT, Kind.ALL, "long"),
    codes.FORBIDDEN: _R(Scope.ACCOUNT, Kind.ALL, "long"),
    codes.BAD_REQUEST: _R(Scope.INSTRUMENT, Kind.ALL, "block"),
    codes.VERSION_UNSUPPORTED: _R(Scope.ACCOUNT, Kind.ALL, "long"),
    codes.INTERNAL: _R(Scope.ACCOUNT, Kind.ALL, "block"),
    # broker / route (BROKER_REJECTED order updates, 02 §11.3)
    codes.ROUTE_KILL_SWITCH: _R(Scope.ACCOUNT, Kind.OPEN, "halt"),
    codes.ROUTE_EXPIRED: _R(Scope.INSTRUMENT, Kind.ALL, "short"),
    codes.ROUTE_LOST: _R(Scope.INSTRUMENT, Kind.ALL, "short"),
    codes.BROKER_DISCONNECTED: _R(Scope.ACCOUNT, Kind.ALL, "transport"),
    codes.BROKER_REJECT_CLOSE_TODAY: _R(Scope.INSTRUMENT, Kind.CLOSE, "short", resync=True),
    codes.BROKER_REJECT_CLOSE_YD: _R(Scope.INSTRUMENT, Kind.CLOSE, "short", resync=True),
    codes.BROKER_REJECT_FUNDS: _R(Scope.ACCOUNT, Kind.OPEN, "block"),
    codes.BROKER_REJECT_PRICE: _R(Scope.INSTRUMENT, Kind.ALL, "short"),
}
DEFAULT_RULE = GateRule(Scope.INSTRUMENT, Kind.ALL, "short")
#: fixed back-off classes (seconds): (base, max)
_FIXED = {"short": (2.0, 30.0), "long": (300.0, 3600.0), "day": (3600.0, 3600.0)}
#: reasons that block by structure (not by P&L): ACC3 may move that budget elsewhere
STRUCTURAL_BLOCKS = frozenset({codes.REJECT_PRODUCT_BLOCKED, codes.REJECT_EXPOSURE_CAP, codes.REJECT_POSITION_LIMIT})
_SIZE_KEYS = ("max_qty", "max", "allowed", "limit", "max_order_qty", "allowed_qty")


def rule_for(code: str) -> GateRule:
    if code in RULES:
        return RULES[code]
    if code.startswith("BROKER_"):
        return GateRule(Scope.INSTRUMENT, Kind.ALL, "short")
    return DEFAULT_RULE


@dataclass
class Block:
    scope: Scope
    key: str
    kind: Kind
    code: str
    since_ns: int
    until_ns: int
    strikes: int = 1


@dataclass(frozen=True, slots=True)
class OpenState:
    allowed: bool
    probe: bool = False
    reason: str | None = None


class Gates:
    """Order gating state of one account (see module docstring)."""

    def __init__(self, account_id: str, cfg: GatesConfig | None = None) -> None:
        self.account_id = account_id
        self.cfg = cfg or GatesConfig()
        self._blocks: dict[tuple[Scope, str, Kind], Block] = {}
        self.halted = False
        self.halted_latched = False
        self.halted_since_ns: int | None = None
        self.risk_version = -1  # state_version of the last applied published state
        self.halt_version = -1  # published state_version known when the halt was learned
        self.size_cap: int | None = None
        self._size_since_ns = 0
        self.resync_wanted = False
        self.rejects: dict[str, int] = {}
        # published restrictions (risk.state / risk.snapshot), replaced as a whole on every update
        self.restrictions: tuple[Restriction, ...] = ()
        self._r_account: str | None = None  # source of an account-wide BLOCK_OPEN
        self._r_products: dict[str, str] = {}
        self._r_sectors: dict[str, str] = {}
        self._r_instruments: dict[str, str] = {}
        self._r_size: dict[str | None, float] = {}  # product (None: account-wide) -> ORDER_SIZE_FACTOR
        self._r_cap: dict[str | None, float] = {}  # product (None: account-wide) -> EXPOSURE_CAP_FACTOR
        self.no_add_to_losers = False

    # ------------------------------------------------------------ recording
    def _backoff_ns(self, cls: str, strikes: int) -> int:
        if cls == "halt":
            base, top = self.cfg.halt_backoff_s, self.cfg.halt_backoff_max_s
        elif cls == "block":
            base, top = self.cfg.block_backoff_s, self.cfg.block_backoff_max_s
        elif cls == "transport":
            base, top = self.cfg.transport_backoff_s, self.cfg.transport_backoff_max_s
        else:
            base, top = _FIXED.get(cls, _FIXED["short"])
        return int(min(base * (2 ** max(0, strikes - 1)), top) * NS_PER_S)

    def on_reject(
        self,
        code: str,
        *,
        product: str,
        instrument_id: str,
        is_open: bool,
        now_ns: int,
        qty: int = 0,
        detail: dict[str, Any] | None = None,
    ) -> GateRule:
        """Record a reject of an order (pre-trade, transport or broker) and arm the block."""
        rule = rule_for(code)
        self.rejects[code] = self.rejects.get(code, 0) + 1
        if rule.scope == Scope.ACCOUNT:
            key = ""
        elif rule.scope == Scope.PRODUCT:
            key = product
        else:
            key = instrument_id
        bkey = (rule.scope, key, rule.kind)
        blk = self._blocks.get(bkey)
        strikes = blk.strikes + 1 if blk is not None else 1
        until = now_ns + self._backoff_ns(rule.backoff, strikes)
        self._blocks[bkey] = Block(rule.scope, key, rule.kind, code, now_ns if blk is None else blk.since_ns, until,
                                   strikes)
        if rule.halted:
            self.mark_halted(now_ns)
        if rule.resync:
            self.resync_wanted = True
        if rule.size:
            self._restrict_size(qty, detail, now_ns)
        logger.bind(corr_id=self.account_id).warning(
            "bot.gate",
            account_id=self.account_id,
            code=code,
            scope=str(rule.scope),
            key=key,
            kind=str(rule.kind),
            strikes=strikes,
            backoff_s=round((until - now_ns) / NS_PER_S, 1),
            opening=is_open,
        )
        return rule

    def _restrict_size(self, qty: int, detail: dict[str, Any] | None, now_ns: int) -> None:
        cap: int | None = None
        for k in _SIZE_KEYS:
            v = (detail or {}).get(k)
            if isinstance(v, (int, float)) and not isinstance(v, bool) and v >= 1:
                cap = int(v)
                break
        if cap is None:
            cap = max(1, qty // 2) if qty > 1 else 1
        self.size_cap = cap if self.size_cap is None else min(self.size_cap, cap)
        self._size_since_ns = now_ns

    def on_accept(self, *, product: str, instrument_id: str, is_open: bool, now_ns: int) -> None:
        """An order passed the pre-trade checks: clear the blocks it disproves."""
        for bkey in list(self._blocks):
            scope, key, kind = bkey
            hit = (
                scope == Scope.ACCOUNT
                or (scope == Scope.PRODUCT and key == product)
                or (scope == Scope.INSTRUMENT and key == instrument_id)
            )
            if not hit:
                continue
            if kind == Kind.ALL or (kind == Kind.OPEN and is_open) or (kind == Kind.CLOSE and not is_open):
                del self._blocks[bkey]
        if is_open and self.halted:
            logger.info("bot.halt_cleared", account_id=self.account_id)
            self.halted = False
            self.halted_since_ns = None

    def mark_halted(self, now_ns: int) -> None:
        if not self.halted:
            self.halted = True
            self.halted_since_ns = now_ns
            self.halt_version = self.risk_version
            logger.warning("bot.halted", account_id=self.account_id)
        bkey = (Scope.ACCOUNT, "", Kind.OPEN)
        if bkey not in self._blocks:
            self._blocks[bkey] = Block(Scope.ACCOUNT, "", Kind.OPEN, codes.REJECT_ACCOUNT_HALTED, now_ns,
                                       now_ns + self._backoff_ns("halt", 1))

    def apply_risk(self, state: AccountState, latched: bool, restrictions: Sequence[Restriction], now_ns: int,
                   version: int | None = None) -> None:
        """Adopt the account's published state and restrictions (02 §7.6 ``AccountStateChanged``).

        ``version`` is the message's ``state_version``: a halt learned from a reject (after
        the last published state) is only lifted by a *newer* published state, so a snapshot
        published before the halt cannot clear it.
        """
        v = self.risk_version + 1 if version is None else version
        if v < self.risk_version:
            return
        self.risk_version = v
        if state == AccountState.HALTED:
            self.mark_halted(now_ns)
            self.halt_version = max(self.halt_version, v)
            self.halted_latched = latched
        elif self.halted and v > self.halt_version:
            self.clear_halted()
        self.restrictions = tuple(restrictions)
        self._r_account = None
        self._r_products, self._r_sectors, self._r_instruments = {}, {}, {}
        self._r_size, self._r_cap = {}, {}
        self.no_add_to_losers = False
        own = f"acct:{self.account_id}"
        for r in restrictions:
            owner, _, rest = r.scope_key.partition("/")
            if owner not in (own, "firm"):
                continue
            kind, _, value = rest.partition(":")
            product = value if kind == "prod" else None
            if r.kind == RestrictionKind.BLOCK_OPEN:
                if not kind:
                    self._r_account = r.source
                elif kind == "prod":
                    self._r_products[value] = r.source
                elif kind == "sector":
                    self._r_sectors[value] = r.source
                elif kind == "inst":
                    self._r_instruments[value] = r.source
            elif r.kind == RestrictionKind.ORDER_SIZE_FACTOR and r.value is not None and kind in ("", "prod"):
                self._r_size[product] = min(self._r_size.get(product, 1.0), r.value)
            elif r.kind == RestrictionKind.EXPOSURE_CAP_FACTOR and r.value is not None and kind in ("", "prod"):
                self._r_cap[product] = min(self._r_cap.get(product, 1.0), r.value)
            elif r.kind == RestrictionKind.NO_ADD_TO_LOSERS:
                self.no_add_to_losers = True

    def size_factor(self, product: str) -> float:
        """ORDER_SIZE_FACTOR in force for opening orders of ``product`` (1.0 without restriction)."""
        return min(self._r_size.get(None, 1.0), self._r_size.get(product, 1.0))

    def exposure_cap_factor(self, product: str) -> float:
        """EXPOSURE_CAP_FACTOR in force for ``product`` (1.0 without restriction)."""
        return min(self._r_cap.get(None, 1.0), self._r_cap.get(product, 1.0))

    def restriction_block(self, product: str, instrument_id: str, sector: str | None = None) -> str | None:
        """Reject code a published ``BLOCK_OPEN`` restriction would produce for an opening order."""
        if self._r_account is not None:
            return codes.REJECT_ACCOUNT_BLOCKED
        if product in self._r_products or instrument_id in self._r_instruments:
            return codes.REJECT_PRODUCT_BLOCKED
        if sector is not None and sector in self._r_sectors:
            return codes.REJECT_PRODUCT_BLOCKED
        return None

    def clear_halted(self) -> None:
        """The account is known not to be HALTED any more (e.g. risk.reset seen in a snapshot)."""
        if self.halted:
            logger.info("bot.halt_cleared", account_id=self.account_id)
        self.halted = False
        self.halted_latched = False
        self.halted_since_ns = None
        for bkey in [k for k, b in self._blocks.items() if b.code == codes.REJECT_ACCOUNT_HALTED]:
            del self._blocks[bkey]

    def on_day_roll(self) -> None:
        """New trading day: latches are released (01 §8.14 reset_on_day_roll); forget day blocks."""
        self.clear_halted()
        for bkey in [k for k, b in self._blocks.items() if rule_for(b.code).backoff in ("day", "halt", "block")]:
            del self._blocks[bkey]
        self.size_cap = None

    # ------------------------------------------------------------ queries
    def _active(self, scope: Scope, key: str, kind: Kind, now_ns: int) -> Block | None:
        b = self._blocks.get((scope, key, kind))
        return b if b is not None and now_ns < b.until_ns else None

    def _expired(self, scope: Scope, key: str, kind: Kind, now_ns: int) -> Block | None:
        b = self._blocks.get((scope, key, kind))
        return b if b is not None and now_ns >= b.until_ns else None

    def all_allowed(self, instrument_id: str, now_ns: int) -> bool:
        """No ALL-kind block on the account or the instrument."""
        return (
            self._active(Scope.ACCOUNT, "", Kind.ALL, now_ns) is None
            and self._active(Scope.INSTRUMENT, instrument_id, Kind.ALL, now_ns) is None
        )

    def close_allowed(self, instrument_id: str, now_ns: int) -> bool:
        return self.all_allowed(instrument_id, now_ns) and self._active(
            Scope.INSTRUMENT, instrument_id, Kind.CLOSE, now_ns
        ) is None

    def cancel_allowed(self, instrument_id: str, now_ns: int) -> bool:
        return self._active(Scope.INSTRUMENT, instrument_id, Kind.CANCEL, now_ns) is None

    def open_state(self, product: str, instrument_id: str, now_ns: int, sector: str | None = None) -> OpenState:
        """May an opening order go out now, and must it be a probe?

        Never while HALTED (the executor sends its own periodic halt probes) or while a
        published ``BLOCK_OPEN`` restriction covers the order.
        """
        if not self.all_allowed(instrument_id, now_ns):
            return OpenState(False, reason="ALL_BLOCKED")
        if self.halted:
            return OpenState(False, reason=codes.REJECT_ACCOUNT_HALTED)
        blocked = self.restriction_block(product, instrument_id, sector)
        if blocked is not None:
            return OpenState(False, reason=blocked)
        for scope, key in ((Scope.ACCOUNT, ""), (Scope.PRODUCT, product), (Scope.INSTRUMENT, instrument_id)):
            b = self._active(scope, key, Kind.OPEN, now_ns)
            if b is not None:
                return OpenState(False, reason=b.code)
        probe = any(
            self._expired(scope, key, Kind.OPEN, now_ns) is not None
            for scope, key in ((Scope.ACCOUNT, ""), (Scope.PRODUCT, product))
        )
        return OpenState(True, probe=probe)

    def product_block(self, product: str) -> Block | None:
        """Opening block on ``product`` (active, or expired and not yet disproved by an accept)."""
        return self._blocks.get((Scope.PRODUCT, product, Kind.OPEN))

    def size_limit(self, default_max: int, now_ns: int) -> int:
        """Max lots per opening order (ORDER_SIZE_FACTOR restriction learned from rejects)."""
        if self.size_cap is None:
            return default_max
        if now_ns - self._size_since_ns >= int(self.cfg.size_restore_s * NS_PER_S):
            self.size_cap = None
            return default_max
        return max(1, min(default_max, self.size_cap))

    def blocks(self, now_ns: int) -> list[dict[str, Any]]:
        """Active and probe-pending blocks and published restrictions (for status logs)."""
        published = [
            {"scope": "RESTRICTION", "key": r.scope_key, "kind": str(r.kind), "code": r.source, "value": r.value}
            for r in self.restrictions
        ]
        return published + [
            {
                "scope": str(b.scope),
                "key": b.key,
                "kind": str(b.kind),
                "code": b.code,
                "left_s": round(max(0, b.until_ns - now_ns) / NS_PER_S, 1),
                "strikes": b.strikes,
            }
            for b in self._blocks.values()
        ]


__all__ = ["RULES", "STRUCTURAL_BLOCKS", "Block", "GateRule", "Gates", "Kind", "OpenState", "Scope", "rule_for"]
