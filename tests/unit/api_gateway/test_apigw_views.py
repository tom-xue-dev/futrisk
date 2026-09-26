"""Read-model merge rules (02 §5 rule 2) and the comparison / aggregation math (02 §9.6-§9.7)."""

from __future__ import annotations

import pytest
from apigw_support import MONDAY_10AM, FixedClock, position, rule_status, snapshot

from futrisk.common.config import FullConfig
from futrisk.common.config.risk_rules import RuleDef
from futrisk.messages.enums import AccountState, Exchange, PosDirection, RestrictionKind
from futrisk.messages.envelope import make_envelope
from futrisk.messages.risk import AccountStateChanged, Restriction
from futrisk.services.api_gateway import readmodel as rm
from futrisk.services.api_gateway.views import (
    aggregate_positions,
    band,
    build_matrix,
    effective_levels,
    hard_limit_level,
    next_level,
    position_rows,
    utilization,
)


def rules(cfg: FullConfig) -> dict[str, RuleDef]:
    return {r.id: r for r in cfg.risk_rules.rules}


class TestUtilization:
    def test_directions_and_baselines(self) -> None:
        assert utilization(0.35, 0.5, ">=") == pytest.approx(0.7)
        assert utilization(-0.015, -0.03, "<=", "account.daily_pnl_pct_init") == pytest.approx(0.5)
        assert utilization(0.02, -0.03, "<=") == 0.0  # a profitable day uses nothing
        assert utilization(0.95, 0.85, "<=", "account.nav") == pytest.approx(1 / 3)
        assert utilization(6.0, 3.0, "<=", "account.effective_n") == pytest.approx(0.5)
        assert utilization(None, 1.0, ">=") is None and utilization(1.0, 0.0, ">=") is None
        assert [band(u) for u in (None, 0.1, 0.6, 0.85, 1.0)] == ["na", "ok", "watch", "warn", "breach"]

    def test_levels_with_overrides(self, default_config: FullConfig) -> None:
        daily = rules(default_config)["daily_loss"]
        patched = daily.model_copy(update={"overrides": {"ACC2": {"halt": {"threshold": -0.04}}}})
        hard = hard_limit_level(effective_levels(patched, "ACC2"))
        assert hard is not None and hard["name"] == "halt" and hard["threshold"] == -0.04
        assert hard_limit_level(effective_levels(patched, "ACC1"))["threshold"] == -0.03  # type: ignore[index]
        assert next_level(-0.021, patched, "ACC1")["name"] == "restrict"  # type: ignore[index]
        assert next_level(-0.06, patched, "ACC1") is None  # every level crossed
        nav = rules(default_config)["nav_floor"]
        assert hard_limit_level(effective_levels(nav, None))["name"] == "derisk"  # type: ignore[index]

    def test_matrix_uses_hard_limit(self, default_config: FullConfig) -> None:
        snaps = [snapshot("ACC1", rules=[rule_status("risk_degree", "account.risk_degree", 0.35, "acct:ACC1"),
                                          rule_status("daily_loss", "account.daily_pnl_pct_init", -0.0,
                                                      "acct:ACC1")])]
        m = build_matrix(snaps, default_config.risk_rules.rules)
        cells = {c["rule_id"]: c for c in m["matrix"]}
        assert cells["risk_degree"]["limit"] == 0.5 and cells["risk_degree"]["utilization"] == pytest.approx(0.7)
        assert cells["risk_degree"]["band"] == "watch"
        assert cells["hwm_dd"]["band"] == "na" and cells["hwm_dd"]["value"] is None  # no status for that rule
        assert m["rules"] == [r.id for r in default_config.risk_rules.rules]


class TestPositions:
    def test_rows_and_groups(self, default_config: FullConfig) -> None:
        snaps = [
            snapshot("ACC1", positions=[position("ACC1", "rb2701", "rb", PosDirection.LONG, 3, 90_000.0),
                                        position("ACC1", "cu2611", "cu", PosDirection.SHORT, 1, 390_000.0)]),
            snapshot("ACC2", positions=[position("ACC2", "IF2612", "IF", PosDirection.LONG, 2, 2_600_000.0,
                                                 Exchange.CFFEX)]),
        ]
        rows = position_rows(snaps, default_config.products)
        assert {r["sector"] for r in rows} == {"black", "nonferrous", default_config.products.products["IF"].sector}
        by_sector = {g["group"]: g for g in aggregate_positions(rows, "sector")}
        assert by_sector["nonferrous"]["net_notional"] == -390_000.0 and by_sector["black"]["long_qty"] == 3
        by_account = {g["group"]: g for g in aggregate_positions(rows, "account")}
        assert by_account["ACC1"]["positions"] == 2 and by_account["ACC1"]["instruments"] == ["cu2611", "rb2701"]
        with pytest.raises(ValueError):
            aggregate_positions(rows, "colour")


class TestReadModel:
    def test_state_version_decides(self) -> None:
        model = rm.ReadModel(FixedClock())
        changes: list[rm.Change] = []
        model.add_listener(changes.append)
        model.seed_snapshots([snapshot("ACC3", state=AccountState.NORMAL, state_version=4)])
        halted = AccountStateChanged(
            account_id="ACC3", trading_day="20260928", from_state=AccountState.NORMAL, to_state=AccountState.HALTED,
            halted_latched=True, state_version=5, cause="RULE", ts_ns=MONDAY_10AM + 1,
            restrictions=[Restriction(kind=RestrictionKind.BLOCK_OPEN, scope_key="acct:ACC3", source="daily_loss/halt")],
        )
        model.apply(make_envelope("risk.state.ACC3", halted, src="risk-engine-1", epoch="e", ts_ns=1, seq=1))
        eff = model.effective_state("ACC3")
        assert eff is not None and eff["state"] == AccountState.HALTED and eff["source"] == "state"
        summary = model.account_summary("ACC3")
        assert summary is not None and summary["state"] == "HALTED" and summary["halted_latched"] is True
        assert model.account_snapshot("ACC3")["restrictions"][0]["kind"] == "BLOCK_OPEN"  # type: ignore[index]
        # a later snapshot with a larger state_version wins again
        model.seed_snapshots([snapshot("ACC3", state=AccountState.WARNING, state_version=6,
                                       as_of_ns=MONDAY_10AM + 2)])
        assert model.effective_state("ACC3")["state"] == AccountState.WARNING  # type: ignore[index]
        # stale state changes are ignored
        model.apply(make_envelope("risk.state.ACC3", halted, src="risk-engine-1", epoch="e", ts_ns=1, seq=2))
        assert [c.kind for c in changes] == [rm.SNAPSHOT, rm.STATE, rm.SNAPSHOT]

    def test_bad_payload_is_counted_not_raised(self) -> None:
        model = rm.ReadModel(FixedClock())
        env = make_envelope("risk.snapshot.ACC3", snapshot("ACC3"), src="risk-engine-1", epoch="e", ts_ns=1, seq=1)
        from msgspec import structs

        model.apply(structs.replace(env, payload=b"\xc1garbage"))
        model.apply(structs.replace(env, type="NoSuchType"))
        assert model.stats.decode_errors == 2 and not model.snapshots

    def test_equity_sampling(self) -> None:
        model = rm.ReadModel(FixedClock())
        model.seed_snapshots([snapshot("ACC3", equity=9_900_000.0)])
        first = model.sample_equity()
        assert [(a, p["equity"]) for a, p in first] == [("ACC3", 9_900_000.0)]
        assert model.sample_equity() == []  # no new snapshot, no new point
