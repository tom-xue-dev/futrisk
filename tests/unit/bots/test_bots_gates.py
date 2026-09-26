"""Reject handling of the bots (02 §11.2): what is held back, where, how long; probes and recovery."""

from __future__ import annotations

from futrisk.messages import codes
from futrisk.messages.enums import AccountState, RestrictionKind
from futrisk.messages.risk import Restriction
from futrisk.sim.bots.config import GatesConfig
from futrisk.sim.bots.gates import Gates, Kind, Scope, rule_for

S = 1_000_000_000
T = 1_790_559_300 * S


def rej(g: Gates, code: str, *, product: str = "rb", instrument: str = "rb2701", is_open: bool = True,
        now: int = T, qty: int = 0, detail: dict | None = None) -> None:
    g.on_reject(code, product=product, instrument_id=instrument, is_open=is_open, now_ns=now, qty=qty, detail=detail)


def test_halted_blocks_opening_only_until_an_open_is_accepted() -> None:
    g = Gates("ACC3", GatesConfig(halt_backoff_s=30, halt_backoff_max_s=120))
    rej(g, codes.REJECT_ACCOUNT_HALTED)
    assert g.halted and g.halted_since_ns == T
    st = g.open_state("hc", "hc2701", T + 1 * S)
    assert not st.allowed and st.reason == codes.REJECT_ACCOUNT_HALTED  # every product of the account
    assert g.close_allowed("rb2701", T + 1 * S) and g.all_allowed("rb2701", T + 1 * S)  # closes continue
    # no back-off probing while HALTED: the executor sends its own 1-lot halt probes (halt_probe_s)
    assert not g.open_state("rb", "rb2701", T + 300 * S).allowed
    g.on_accept(product="rb", instrument_id="rb2701", is_open=False, now_ns=T + 301 * S)  # a close proves nothing
    assert g.halted
    g.on_accept(product="rb", instrument_id="rb2701", is_open=True, now_ns=T + 302 * S)  # a probe got through
    st = g.open_state("rb", "rb2701", T + 302 * S)
    assert st.allowed and not st.probe and not g.halted


def test_published_state_drives_the_halt_without_an_opening_attempt() -> None:
    g = Gates("ACC3")
    g.apply_risk(AccountState.WARNING, False, [], T, version=4)
    assert not g.halted and g.open_state("rb", "rb2701", T).allowed
    g.apply_risk(AccountState.HALTED, False, [], T + S, version=5)  # PENDING: halted, not latched yet
    assert g.halted and not g.halted_latched and g.halted_since_ns == T + S
    g.apply_risk(AccountState.HALTED, True, [], T + 2 * S, version=6)
    assert g.halted_latched and g.halted_since_ns == T + S  # the halt started at the first message
    g.apply_risk(AccountState.WARNING, False, [], T + 3 * S, version=5)  # stale (older state_version): ignored
    assert g.halted
    g.apply_risk(AccountState.RESTRICTED, False, [], T + 4 * S, version=7)  # risk.reset -> REARMED etc.
    assert not g.halted and g.open_state("rb", "rb2701", T + 4 * S).allowed


def test_a_halt_learned_from_a_reject_survives_an_older_snapshot() -> None:
    g = Gates("ACC3")
    g.apply_risk(AccountState.RESTRICTED, False, [], T, version=8)
    rej(g, codes.REJECT_ACCOUNT_HALTED, now=T + S)  # the engine halted after state_version 8
    g.apply_risk(AccountState.RESTRICTED, False, [], T + 2 * S, version=8)  # a snapshot published before the halt
    assert g.halted
    g.apply_risk(AccountState.HALTED, True, [], T + 3 * S, version=9)
    g.apply_risk(AccountState.NORMAL, False, [], T + 4 * S, version=10)  # released (e.g. risk.reset)
    assert not g.halted


def test_published_restrictions() -> None:
    g = Gates("ACC3")
    rs = [
        Restriction(kind=RestrictionKind.ORDER_SIZE_FACTOR, scope_key="acct:ACC3", value=0.5, source="daily_loss/restrict"),
        Restriction(kind=RestrictionKind.NO_ADD_TO_LOSERS, scope_key="acct:ACC3", source="daily_loss/restrict"),
        Restriction(kind=RestrictionKind.BLOCK_OPEN, scope_key="acct:ACC3/prod:rb", source="product_exposure/block"),
        Restriction(kind=RestrictionKind.BLOCK_OPEN, scope_key="acct:ACC3/sector:agri", source="sector_exposure/block"),
        Restriction(kind=RestrictionKind.EXPOSURE_CAP_FACTOR, scope_key="acct:ACC3", value=0.5, source="hwm_dd/halve"),
        Restriction(kind=RestrictionKind.BLOCK_OPEN, scope_key="acct:ACC5", source="SYSTEM:FILL_GAP"),  # another account
        Restriction(kind=RestrictionKind.BLOCK_OPEN, scope_key="firm/prod:cu", source="firm/prod:cu"),
    ]
    g.apply_risk(AccountState.RESTRICTED, False, rs, T, version=3)
    assert g.size_factor("hc") == 0.5 and g.exposure_cap_factor("i") == 0.5 and g.no_add_to_losers
    assert g.open_state("rb", "rb2701", T).reason == codes.REJECT_PRODUCT_BLOCKED
    assert g.open_state("m", "m2701", T, sector="agri").reason == codes.REJECT_PRODUCT_BLOCKED
    assert g.open_state("cu", "cu2611", T).reason == codes.REJECT_PRODUCT_BLOCKED  # firm scope applies
    assert g.open_state("hc", "hc2701", T).allowed  # NO_ADD_TO_LOSERS is the engine's call (02 §11.2 step 4)
    assert g.close_allowed("rb2701", T)
    g.apply_risk(AccountState.RESTRICTED, False,
                 [Restriction(kind=RestrictionKind.BLOCK_OPEN, scope_key="acct:ACC3", source="SYSTEM:STARTING")],
                 T + S, version=4)
    assert g.open_state("hc", "hc2701", T + S).reason == codes.REJECT_ACCOUNT_BLOCKED and g.size_factor("hc") == 1.0
    g.apply_risk(AccountState.NORMAL, False, [], T + 2 * S, version=5)
    assert g.open_state("rb", "rb2701", T + 2 * S).allowed and not g.no_add_to_losers
    assert g.blocks(T) == [] or all(b["scope"] != "RESTRICTION" for b in g.blocks(T))


def test_product_and_instrument_scopes() -> None:
    g = Gates("ACC3")
    rej(g, codes.REJECT_ADD_TO_LOSER)  # NO_ADD_TO_LOSERS on rb
    assert not g.open_state("rb", "rb2701", T).allowed
    assert g.open_state("hc", "hc2701", T).allowed  # other products still open
    assert g.product_block("rb") is not None and g.product_block("rb").code == codes.REJECT_ADD_TO_LOSER
    rej(g, codes.REJECT_MD_UNAVAILABLE, product="i", instrument="i2701")
    assert not g.open_state("i", "i2701", T).allowed and g.close_allowed("i2701", T)
    rej(g, codes.REJECT_SESSION_CLOSED, product="hc", instrument="hc2701")
    assert not g.close_allowed("hc2701", T) and not g.all_allowed("hc2701", T)


def test_size_restriction_learned_and_restored() -> None:
    g = Gates("ACC3", GatesConfig(size_restore_s=120))
    rej(g, codes.REJECT_ORDER_SIZE_RESTRICTED, qty=40, detail={"max": 12})
    assert g.size_limit(50, T) == 12
    rej(g, codes.REJECT_ORDER_SIZE_RESTRICTED, qty=10, now=T + 5 * S)  # no detail: halve the rejected size
    assert g.size_limit(50, T + 5 * S) == 5
    assert g.size_limit(50, T + 200 * S) == 50


def test_transport_close_and_cancel_rules() -> None:
    g = Gates("ACC1", GatesConfig(transport_backoff_s=3))
    rej(g, codes.TIMEOUT, is_open=False)
    assert not g.all_allowed("rb2701", T + 2 * S) and g.all_allowed("rb2701", T + 3 * S)
    rej(g, codes.REJECT_CLOSE_TODAY_EXCEEDS, is_open=False)
    assert g.resync_wanted and not g.close_allowed("rb2701", T + 1 * S)
    assert g.open_state("rb", "rb2701", T + 4 * S).allowed
    rej(g, codes.REJECT_CANCEL_LIMIT)
    assert not g.cancel_allowed("rb2701", T + 60 * S) and g.cancel_allowed("hc2701", T)
    g.on_day_roll()
    assert g.cancel_allowed("rb2701", T + 60 * S)


def test_rule_table() -> None:
    assert rule_for(codes.REJECT_FIRM_KILL_SWITCH).kind == Kind.OPEN
    assert rule_for(codes.REJECT_PRODUCT_BLOCKED).scope == Scope.PRODUCT
    assert rule_for(codes.REJECT_WHATIF_BREACH).scope == Scope.ACCOUNT
    assert rule_for("BROKER_REJECT_SOMETHING").scope == Scope.INSTRUMENT
    assert rule_for("NEVER_SEEN").kind == Kind.ALL
    g = Gates("ACC2")
    rej(g, "NEVER_SEEN")
    assert not g.all_allowed("rb2701", T) and g.all_allowed("cu2611", T)
    assert g.rejects == {"NEVER_SEEN": 1} and g.blocks(T)[0]["code"] == "NEVER_SEEN"


def test_day_roll_releases_halt() -> None:
    g = Gates("ACC3")
    g.mark_halted(T)
    assert not g.open_state("rb", "rb2701", T).allowed
    g.on_day_roll()
    assert not g.halted and g.open_state("rb", "rb2701", T).allowed
