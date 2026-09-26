"""``account.compliance_util`` early warning (01 §8.14 合规维度: the ``compliance`` rule warns at 80 %).

Besides open lots / cancels / large cancels / self trades, the utilization now includes the position limit (限仓:
position of the larger side / ``position_limit`` per instrument) and the order-rate throttle (the share of the
token bucket in use: 100 % = the next order is REJECT_THROTTLED), both of which pre-trade only hard-rejected.
"""

from __future__ import annotations

import pytest
from enginetest_support import PRE, B, Harness, O, config, registry, rules_with, sod_snapshot

from futrisk.core.metrics import ComplianceCounters, compliance_utilization
from futrisk.core.risk_account import TokenBucket
from futrisk.messages.enums import PosDirection, RiskEventKind, RuleSmState, TradeOrigin
from futrisk.messages.eod import SodLot

S_NS = 1_000_000_000


def _compliance_only() -> Harness:
    return Harness(rules_with([r for r in config().risk_rules.rules if r.id == "compliance"]))


def _warn_state(h: Harness, account: str = "ACC3") -> RuleSmState:
    m = h.eng.account(account).machines.get(("compliance", "warn", f"acct:{account}"))
    return m.state if m is not None else RuleSmState.OK


class TestUtilization:
    def test_position_and_order_rate_parts(self) -> None:
        reg = registry()
        assert compliance_utilization(reg, ComplianceCounters(positions={"IH2612": 960})) == pytest.approx(0.8)  # 限仓 1200
        assert compliance_utilization(reg, ComplianceCounters(positions={"rb2701": 10_000})) == 0.0  # no position limit
        assert compliance_utilization(reg, ComplianceCounters(order_rate=0.85)) == pytest.approx(0.85)
        both = ComplianceCounters(open_volume={"IF2612": 100}, positions={"IF2612": 4500}, order_rate=0.5)
        assert compliance_utilization(reg, both) == pytest.approx(0.9)  # max(100/500, 4500/5000, 0.5)

    def test_token_bucket_utilization(self) -> None:
        b = TokenBucket(rate_per_s=20.0, burst=40)
        assert b.utilization(0) == 0.0  # never used
        for _ in range(32):
            assert b.take(0)
        assert b.utilization(0) == pytest.approx(0.8)
        assert b.utilization(S_NS // 2) == pytest.approx((40 - 18) / 40)  # 10 tokens back after 0.5 s
        assert b.utilization(2 * S_NS) == 0.0
        assert b.tokens == pytest.approx(8.0)  # reading never takes or refills
        for _ in range(8):
            assert b.take(0)
        assert b.utilization(0) == 1.0 and not b.take(0)  # empty: REJECT_THROTTLED


class TestEngine:
    def test_crossing_80_pct_of_the_position_limit_raises_the_compliance_warn(self) -> None:
        lots = {"ACC3": [SodLot(instrument_id="IH2612", direction=PosDirection.LONG, lot_no=1, open_td="20260925",
                                open_ts_ns=1, open_price="2950", qty=950, basis_price="2950")]}
        h = Harness(rules_with([r for r in config().risk_rules.rules if r.id == "compliance"]),
                    sod=sod_snapshot(lots=lots, pre_settle={**PRE, "IH2612": 2950.0}))
        h.ticks({"IH2612": 2950.0})
        ra = h.eng.account("ACC3")
        assert h.eng.valuation(ra).compliance_util == pytest.approx(950 / 1200)  # 79 %
        assert _warn_state(h) == RuleSmState.OK
        t = h.trade("ACC3", "IH2612", B, O, 2950.0, 10, origin=TradeOrigin.EXTERNAL)
        h.run(h.eng.apply_exec(t))
        h.evaluate()
        assert h.eng.valuation(ra).compliance_util == pytest.approx(960 / 1200)  # 80 %: position 960 / 限仓 1200
        assert _warn_state(h) == RuleSmState.ACTIVE
        raised = [e for e in h.events(RiskEventKind.ALERT_RAISED) if e.rule_id == "compliance"]
        assert len(raised) == 1 and raised[0].level == "warn"

    def test_the_order_rate_warns_before_the_throttle_rejects(self) -> None:
        h = _compliance_only()
        h.ticks({"rb2701": 3100.0})
        for n in range(31):  # 31 / 40 of the burst: 77.5 %
            rep, _ = h.submit("ACC3", "rb2701", B, O, 1, 3100.0, coid=f"q{n}")
            assert rep.ok
        h.evaluate()
        assert _warn_state(h) == RuleSmState.OK
        rep, _ = h.submit("ACC3", "rb2701", B, O, 1, 3100.0, coid="q31")
        assert rep.ok
        assert h.eng.dirty_accounts() == ["ACC3"]  # the order itself asks for an evaluation
        h.evaluate()
        assert h.eng.valuation(h.eng.account("ACC3")).compliance_util == pytest.approx(0.8)
        assert _warn_state(h) == RuleSmState.ACTIVE  # 80 %: warned while 8 tokens are left
        h.advance(2000)  # the bucket refills; the next evaluation sees 0 %
        h.tick("rb2701", 3101.0)
        assert h.eng.valuation(h.eng.account("ACC3")).compliance_util == 0.0
