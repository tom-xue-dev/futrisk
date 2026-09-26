"""End-to-end EOD saga tests of the clearing-worker against stub services over the real bus (02 §8.4)."""

from __future__ import annotations

import asyncio
import json
import sqlite3
from collections.abc import Sequence
from concurrent.futures import ProcessPoolExecutor
from decimal import Decimal
from pathlib import Path

import msgspec
import pytest
from clearing_support import (
    NEXT,
    PREV,
    D,
    Harness,
    eventually,
    sample_alerts,
    sample_md_events,
    sample_metrics,
    sample_orders,
    sample_svc_events,
)

from futrisk.common.config import FullConfig
from futrisk.common.decimals import D as Dec
from futrisk.common.ids import uuid7
from futrisk.core import DaySettlement, determine_settlement_price, settle_day
from futrisk.messages import codes, decode_envelope, open_payload
from futrisk.messages.common import ReplayBatch, ReplayRequest
from futrisk.messages.enums import CheckStatus, ReconCheck, ReportType
from futrisk.messages.eod import (
    EodPhaseEvent,
    ReportList,
    ReportListRequest,
    SettlementResult,
    SettleQuery,
    SettleRunAck,
    SodRequest,
    SodSnapshot,
)
from futrisk.services.clearing.reports import SECTION_TITLES, STATEMENT_HEADINGS
from futrisk.services.clearing.reports.statement import m2

PHASES = ["BEGIN", "FROZEN", "TRADES_RECONCILED", "PRICES_FIXED", "SETTLED", "ROLLED", "REPORTED", "DONE"]


def expected_settlement(h: Harness, sod: SodSnapshot, version: int = 1) -> DaySettlement:
    """The same day settled directly with the core (the service must persist exactly this)."""
    scn = h.scn
    inputs = h.md.inputs
    views = {
        inst: determine_settlement_price(inp, scn.registry, version=version) for inst, inp in sorted(inputs.items())
    }
    return settle_day(
        trading_day=D,
        version=version,
        sod=sod,
        trades=sorted(scn.trades, key=lambda t: t.trade_seq),
        cash_movements=scn.cash,
        settle_prices=views,
        registry=scn.registry,
        next_trading_day=NEXT,
        sod_version=1,
        pre_settle={inst: Dec(inp.pre_settle) for inst, inp in inputs.items()},
    )


async def sys_eod_phases(h: Harness) -> list[EodPhaseEvent]:
    reply = await h.api.to("clearing-worker", "ctl.replay", ReplayRequest(topic="sys.eod", from_seq=1))
    batch = reply.open_as(ReplayBatch)
    return [open_payload(decode_envelope(b)) for b in batch.envelopes]  # type: ignore[misc]


def db_rows(h: Harness, sql: str) -> list[tuple[object, ...]]:
    con = sqlite3.connect(h.tmp / "cw" / "db" / "clearing.db")
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


def _by(items: Sequence[object], *keys: str) -> list[object]:
    return sorted(items, key=lambda x: tuple(str(getattr(x, k)) for k in keys))


class TestHappyPath:
    async def test_seed_settle_push_report(self, tmp_path: Path, bus_config: FullConfig, pool: ProcessPoolExecutor) -> None:
        h = Harness(bus_config, tmp_path, executor=pool)
        try:
            await h.start_stubs()
            h.re1.alerts = sample_alerts()
            h.re1.orders = sample_orders()
            h.re1.metrics = {"ACC3": sample_metrics()}
            h.md.events = sample_md_events()
            h.sup.events = sample_svc_events()
            await h.start_clearing()

            # --- first-day seed (version 0) is pushed to both shards with trading_day = previous trading day
            seeded = (await h.seed()).open_as(SodSnapshot)
            assert seeded.trading_day == D and seeded.version == 0
            await eventually(lambda: all(s.snapshots for s in h.shards))
            for s in h.shards:
                assert s.rollovers[0].trading_day == PREV and s.rollovers[0].next_trading_day == D
                assert s.rollovers[0].sod_version == 0
                assert {a.account_id for a in s.snapshots[0].accounts} == set(s.accounts)
            # pushed_ns is written only after BOTH shards' DayRoll replies arrived (the stubs fetch first, reply after)
            await eventually(lambda: db_rows(h, f"SELECT pushed_ns FROM sod_versions WHERE trading_day='{D}'")[0][0])
            pushed = await h.api.call("settle.sod_snapshot", SodRequest(trading_day=D, account_ids=[]))

            # --- the day: trades copied from td.exec, cash from risk.cash
            await h.load_day()
            await h.wait_copies()
            h.re1.rollover_gate = asyncio.Event()  # risk-engine-1 confirms late

            reply = await h.api.settle_run()
            assert reply.ok, reply
            ack = reply.open_as(SettleRunAck)
            assert (ack.trading_day, ack.version) == (D, 1)

            # pushed_ns only after ALL shards confirmed: risk-engine-0 is rolled, risk-engine-1 still waits
            await eventually(lambda: any(r.next_trading_day == NEXT for r in h.re0.rollovers) and h.re0.rolled.get(NEXT) == 1)
            await eventually(lambda: any(r.next_trading_day == NEXT for r in h.re1.rollovers))
            st = await h.status()
            assert st.phase.value == "SETTLED"
            not_pushed = await h.api.call("settle.sod_snapshot", SodRequest(trading_day=NEXT, account_ids=[]))
            assert not_pushed.code == codes.NOT_FOUND  # no pushed version of NEXT yet
            assert db_rows(h, f"SELECT pushed_ns FROM sod_versions WHERE trading_day='{NEXT}'") == [(None,)]
            h.re1.rollover_gate.set()
            done = await h.wait_phase("DONE")
            assert done.error is None

            # --- sys.eod: every phase once, in order
            phases = [e.phase.value for e in await sys_eod_phases(h)]
            assert phases == ["BEGIN", *PHASES[1:]][: len(phases)] and phases == PHASES

            # --- persisted settlement == settle_day output
            assert pushed.ok
            sod = pushed.open_as(SodSnapshot)
            exp = expected_settlement(h, sod)
            got = (await h.api.call("settle.query", SettleQuery(trading_day=D))).open_as(SettlementResult)
            assert got.version == 1
            assert got.accounts == exp.result.accounts
            assert got.positions == exp.result.positions
            assert _by(got.margin_groups, "account_id", "margin_group") == _by(exp.result.margin_groups, "account_id", "margin_group")
            assert _by(got.trades, "account_id", "trade_id") == _by(exp.result.trades, "account_id", "trade_id")
            assert _by(got.close_details, "account_id", "detail_no") == _by(exp.result.close_details, "account_id", "detail_no")
            assert got.prices == exp.result.prices
            settle_checks = sorted((c.check.value, c.status.value, c.account_id or "") for c in exp.checks)
            got_checks = sorted(
                (c.check.value, c.status.value, c.account_id or "")
                for c in got.checks
                if c.check not in (ReconCheck.TRADE_DIGEST, ReconCheck.CASH_DIGEST)
            )
            assert got_checks == settle_checks
            digest = [c for c in got.checks if c.check in (ReconCheck.TRADE_DIGEST, ReconCheck.CASH_DIGEST)]
            assert digest and all(c.status == CheckStatus.PASS for c in digest)

            # --- next-day SOD: persisted, pushed to both shards, fetched by version
            nxt = (await h.api.call("settle.sod_snapshot", SodRequest(trading_day=NEXT, account_ids=[]))).open_as(SodSnapshot)
            assert nxt.version == 1 and nxt.from_settlement is not None and nxt.from_settlement.version == 1
            assert nxt.pre_settle == exp.next_sod.pre_settle
            assert _by(nxt.accounts, "account_id") == _by(exp.next_sod.accounts, "account_id")
            for s in h.shards:
                last = s.snapshots[-1]
                assert (last.trading_day, last.version) == (NEXT, 1)
                assert s.rollovers[-1].trading_day == D and s.rollovers[-1].sod_version == 1
            pushed_ns = db_rows(h, f"SELECT pushed_ns FROM sod_versions WHERE trading_day='{NEXT}' AND version=1")[0][0]
            assert pushed_ns is not None
            assert db_rows(h, f"SELECT phase, triggered_by FROM eod_runs WHERE trading_day='{D}'") == [("DONE", "ops")]

            # --- reports: files, index, report.ready
            reports = (await h.api.call("report.list", ReportListRequest(trading_day=D))).open_as(ReportList)
            kinds = [r.type for r in reports.items]
            assert kinds.count(ReportType.RISK_DAILY_HTML) == 1 and kinds.count(ReportType.SETTLEMENT_JSON) == 1
            assert kinds.count(ReportType.STATEMENT_TXT) == 5 and kinds.count(ReportType.STATEMENT_CSV) == 5
            rdir = tmp_path / "cw" / "reports" / D
            html = (rdir / "risk_daily.html").read_text(encoding="utf-8")
            for title in SECTION_TITLES:
                assert title in html, title
            assert "ACC3 10:32:15 于 −3.02% 熔断，拒绝 7 笔开仓" in html
            assert "data:image/png;base64," in html
            txt = (rdir / "statement_ACC1.txt").read_text(encoding="utf-8")
            for heading in STATEMENT_HEADINGS:
                assert heading in txt, heading
            acc1 = next(a for a in exp.result.accounts if a.account_id == "ACC1")
            assert m2(acc1.equity) in txt and m2(acc1.pre_balance) in txt and m2(acc1.close_pnl) in txt
            assert "T0001" in txt and "rb2701" in txt
            assert (rdir / "statement_ACC1.csv").read_bytes().startswith(b"\xef\xbb\xbf")
            doc = json.loads((rdir / "settlement.json").read_text(encoding="utf-8"))
            assert doc["version"] == 1 and len(doc["accounts"]) == 5
            ready = await h.api.to("clearing-worker", "ctl.replay", ReplayRequest(topic="report.ready", from_seq=1))
            assert len(ready.open_as(ReplayBatch).envelopes) == len(reports.items) == 12

            # --- daily maintenance after DONE: backup of clearing.db
            backup = tmp_path / "cw" / "backup" / D / "clearing.db"
            await eventually(lambda: backup.exists(), timeout=10)
            con = sqlite3.connect(backup)
            try:
                assert con.execute("SELECT count(*) FROM account_settlements").fetchone()[0] == 5
            finally:
                con.close()
        finally:
            await h.close()


class TestAccountingDetail:
    async def test_settled_equity_matches_hand_numbers(self, tmp_path: Path, bus_config: FullConfig, pool: ProcessPoolExecutor) -> None:
        """Spot-check one account against hand-computed numbers (SHFE FLOOR_TICK settlement)."""
        h = Harness(bus_config, tmp_path, executor=pool)
        try:
            await h.start_stubs()
            await h.start_clearing()
            await h.seed()
            await h.load_day()
            await h.wait_copies()
            assert (await h.api.settle_run()).ok
            await h.wait_phase("DONE")
            got = (await h.api.call("settle.query", SettleQuery(trading_day=D, account_id="ACC1"))).open_as(SettlementResult)
            acc = got.accounts[0]
            # rb2701 VWAP = (3117*400 + 3119*300 + 3121*300) / 1000 = 3118.8 -> FLOOR_TICK 3118
            price = next(p for p in got.prices if p.instrument_id == "rb2701")
            assert price.price == "3118" and price.source.value == "VWAP"
            # close yd 4 @3120 vs pre 3110: +400; position: 6 yd (3118-3110)*10*6 = 480, 2 td (3118-3125)*10*2 = -140
            assert Decimal(acc.close_pnl) == Decimal("400.00")
            assert Decimal(acc.position_pnl) == Decimal("340.00")
            fee = Decimal(acc.fee)
            assert Decimal(acc.equity) == Decimal("10000000.00") + Decimal("740.00") - fee
        finally:
            await h.close()


@pytest.mark.parametrize("dummy", [0])
def test_msgspec_roundtrip_of_result(dummy: int) -> None:
    """SettlementResult survives the JSON bundle used by the report worker."""
    res = SettlementResult(trading_day=D, version=1, accounts=[])
    assert msgspec.convert(msgspec.to_builtins(res), SettlementResult) == res
    assert uuid7() != uuid7()
