"""Restrictions and account-state aggregation (01 §8.14 四种动作 / 账户状态的聚合; 02 §7.6 Restriction).

- RESTRICT params map to :class:`Restriction` entries on the rule's scope:
  ``max_order_factor`` → ORDER_SIZE_FACTOR, ``no_add_to_losers`` →
  NO_ADD_TO_LOSERS, ``exposure_cap_factor`` → EXPOSURE_CAP_FACTOR,
  ``block_open`` → BLOCK_OPEN on the rule's scope, ``block_open_largest_product``
  → BLOCK_OPEN on ``acct:{A}/prod:{largest}`` (recomputed at every evaluation).
  They are never latched and disappear with the level (auto-clear on recovery);
- system restrictions ``SYSTEM:{FILL_GAP|MD_UNKNOWN|RECON_BREAK|EOD_NOT_SETTLED|STARTING}``
  are BLOCK_OPEN on the account scope;
- firm restrictions (``firm``, ``firm/prod:…``, ``firm/sector:…``) come from
  ``risk.control.firm`` and apply to every account of every shard;
- account state = most severe of: ALERT → WARNING; RESTRICT, SYSTEM:* and firm
  restrictions, kill switch → RESTRICTED; CIRCUIT_BREAK (PENDING or ACTIVE) and
  manual halt → HALTED; LIQUIDATE does not change the state.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from futrisk.core.rules import LevelSpec, acct_scope, firm_product_scope, parse_scope, product_scope
from futrisk.messages.enums import AccountState, RestrictionKind
from futrisk.messages.risk import Restriction

SYSTEM_FILL_GAP = "FILL_GAP"
SYSTEM_MD_UNKNOWN = "MD_UNKNOWN"
SYSTEM_RECON_BREAK = "RECON_BREAK"
SYSTEM_EOD_NOT_SETTLED = "EOD_NOT_SETTLED"
SYSTEM_STARTING = "STARTING"
SYSTEM_REASONS = (SYSTEM_FILL_GAP, SYSTEM_MD_UNKNOWN, SYSTEM_RECON_BREAK, SYSTEM_EOD_NOT_SETTLED, SYSTEM_STARTING)


def system_source(reason: str) -> str:
    if reason not in SYSTEM_REASONS:
        raise ValueError(f"unknown system restriction reason {reason!r}")
    return f"SYSTEM:{reason}"


def system_restriction(account_id: str, reason: str, since_ns: int) -> Restriction:
    return Restriction(
        kind=RestrictionKind.BLOCK_OPEN, scope_key=acct_scope(account_id), source=system_source(reason), since_ns=since_ns
    )


def level_source(rule_id: str, level: str) -> str:
    return f"{rule_id}/{level}"


def level_restrictions(
    rule_id: str,
    level: LevelSpec,
    scope_key: str,
    account_id: str | None,
    *,
    largest_product: str | None,
    since_ns: int,
) -> list[Restriction]:
    """Restrictions produced by an active RESTRICT level (01 §8.14); ``account_id`` None = firm rule."""
    p: Mapping[str, Any] = level.params
    src = level_source(rule_id, level.name)
    out: list[Restriction] = []
    if p.get("max_order_factor") is not None:
        out.append(Restriction(kind=RestrictionKind.ORDER_SIZE_FACTOR, scope_key=scope_key,
                               value=float(p["max_order_factor"]), source=src, since_ns=since_ns))
    if p.get("no_add_to_losers"):
        out.append(Restriction(kind=RestrictionKind.NO_ADD_TO_LOSERS, scope_key=scope_key, source=src, since_ns=since_ns))
    if p.get("exposure_cap_factor") is not None:
        out.append(Restriction(kind=RestrictionKind.EXPOSURE_CAP_FACTOR, scope_key=scope_key,
                               value=float(p["exposure_cap_factor"]), source=src, since_ns=since_ns))
    if p.get("block_open"):
        out.append(Restriction(kind=RestrictionKind.BLOCK_OPEN, scope_key=scope_key, source=src, since_ns=since_ns))
    if p.get("block_open_largest_product") and largest_product is not None:
        # 01 §8.14：有效品种数 < 3 时禁止在最大品种上加仓
        largest = product_scope(account_id, largest_product) if account_id else firm_product_scope(largest_product)
        out.append(Restriction(kind=RestrictionKind.BLOCK_OPEN, scope_key=largest, source=src, since_ns=since_ns))
    return out


RestrictionId = tuple[RestrictionKind, str, float | None, str]


def restriction_id(r: Restriction) -> RestrictionId:
    """Identity of a restriction ignoring ``since_ns`` (change detection)."""
    return (r.kind, r.scope_key, r.value, r.source)


def merge_since(new: Iterable[Restriction], old: Iterable[Restriction]) -> list[Restriction]:
    """Keep the original ``since_ns`` of restrictions that stay in force; sorted for stable output."""
    since = {restriction_id(r): r.since_ns for r in old}
    out: list[Restriction] = []
    seen: set[RestrictionId] = set()
    for r in new:
        rid = restriction_id(r)
        if rid in seen:
            continue
        seen.add(rid)
        if rid in since and since[rid] != r.since_ns:
            r = Restriction(kind=r.kind, scope_key=r.scope_key, value=r.value, source=r.source, since_ns=since[rid])
        out.append(r)
    out.sort(key=lambda x: (x.source, x.kind.value, x.scope_key, x.value if x.value is not None else -1.0))
    return out


def same_restrictions(a: Iterable[Restriction], b: Iterable[Restriction]) -> bool:
    return sorted(map(restriction_id, a), key=repr) == sorted(map(restriction_id, b), key=repr)


@dataclass(frozen=True, slots=True)
class OrderScope:
    """What a restriction is matched against: the order's account and contract."""

    account_id: str
    instrument_id: str
    product: str
    sector: str


def applies(r: Restriction, o: OrderScope) -> bool:
    """True when ``r``'s scope covers the order (firm scopes cover every account)."""
    sk = parse_scope(r.scope_key)
    if not sk.is_firm and sk.account_id != o.account_id:
        return False
    if sk.kind is None:
        return True
    if sk.kind == "prod":
        return sk.value == o.product
    if sk.kind == "sector":
        return sk.value == o.sector
    if sk.kind == "inst":
        return sk.value == o.instrument_id
    return False


def is_account_wide(r: Restriction) -> bool:
    """Account (or firm) scope without a product / sector / instrument part."""
    return parse_scope(r.scope_key).kind is None


def matching(restrictions: Iterable[Restriction], o: OrderScope, kind: RestrictionKind) -> list[Restriction]:
    return [r for r in restrictions if r.kind == kind and applies(r, o)]


@dataclass(frozen=True, slots=True)
class StateContribution:
    """What one source contributes to the account state."""

    state: AccountState
    latched: bool = False


def aggregate(contributions: Iterable[StateContribution]) -> tuple[AccountState, bool]:
    """Most severe state; ``halted_latched`` when any HALTED contribution is latched."""
    state = AccountState.NORMAL
    latched = False
    for c in contributions:
        if c.state.severity > state.severity:
            state = c.state
        if c.state == AccountState.HALTED and c.latched:
            latched = True
    return state, (latched if state == AccountState.HALTED else False)


__all__ = [
    "SYSTEM_EOD_NOT_SETTLED",
    "SYSTEM_FILL_GAP",
    "SYSTEM_MD_UNKNOWN",
    "SYSTEM_REASONS",
    "SYSTEM_RECON_BREAK",
    "SYSTEM_STARTING",
    "OrderScope",
    "StateContribution",
    "aggregate",
    "applies",
    "is_account_wide",
    "level_restrictions",
    "level_source",
    "matching",
    "merge_since",
    "restriction_id",
    "same_restrictions",
    "system_restriction",
    "system_source",
]
