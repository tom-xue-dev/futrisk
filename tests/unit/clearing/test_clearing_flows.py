"""clearing-worker flows over the real bus: reconciliation repair and failure, restart/resume, SOD versions,
re-push to a returning shard, price priority and re-settlement, idempotency, scheduler, backtest jobs."""

from __future__ import annotations

import asyncio
import dataclasses
import json
import sqlite3
from concurrent.futures import ProcessPoolExecutor
from decimal import Decimal
from pathlib import Path
from typing import Any

import msgspec
from clearing_support import CW, FAST, NEXT, PREV, Api, D, Harness, StrictShard, eventually

from futrisk.common.config import FullConfig
from futrisk.common.ids import uuid7
from futrisk.messages import codes, decode_envelope, open_payload
from futrisk.messages.common import ReplayBatch, ReplayRequest
from futrisk.messages.enums import BacktestState, DayRollMode, ReportType, Role, SettlePriceSource
from futrisk.messages.eod import (
    BacktestList,
    BacktestListRequest,
    BacktestRunRequest,
    BacktestStatus,
    BacktestStatusRequest,
    ImportAck,
    OfficialPrice,
    OfficialPriceImport,
    PriceOverride,
    ReportGetRequest,
    ReportReady,
    SettlementPriceView,
    SettlementResult,
    SettleQuery,
    SettleRunAck,
    SettleRunRequest,
    SodRequest,
    SodSnapshot,
)

PHASES = ["BEGIN", "FROZEN", "TRADES_RECONCILED", "PRICES_FIXED", "SETTLED", "ROLLED", "REPORTED", "DONE"]


def rows(h: Harness, sql: str) -> list[tuple[Any, ...]]:
    con = sqlite3.connect(h.tmp / "cw" / "db" / "clearing.db")
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


def step_log(h: Harness, td: str = D, version: int = 1) -> dict[str, Any]:
    raw = rows(h, f"SELECT step_log FROM eod_runs WHERE trading_day='{td}' AND version={version}")[0][0]
    data: dict[str, Any] = json.loads(raw)
    return data


async def eod_events(h: Harness) -> list[Any]:
    reply = await h.api.to(CW, "ctl.replay", ReplayRequest(topic="sys.eod", from_seq=1))
    return [open_payload(decode_envelope(b)) for b in reply.open_as(ReplayBatch).envelopes]


async def ready_day(h: Harness, *, publish_all: bool = True) -> None:
    await h.start_stubs()
    await h.start_clearing()
    await h.seed()
    await eventually(lambda: all(s.rolled.get(D) == 0 for s in h.shards))
    if publish_all:
        await h.load_day()
        await h.wait_copies()


def sod_req(td: str, version: int | None = None) -> SodRequest:
    return SodRequest(trading_day=td, account_ids=[], version=version)


class TestReconciliation:
    async def test_missing_copy_pulled_and_lagging_shard_refrozen(
        self, tmp_path: Path, bus_config: FullConfig, pool: ProcessPoolExecutor
    ) -> None:
        h = Harness(bus_config, tmp_path, executor=pool, render_reports=False)
        try:
            await ready_day(h, publish_all=False)
            hidden = h.scn.trades[2]  # ACC3: never reaches the local copy through td.exec
            lagging = h.scn.trades[3]  # ACC4: risk-engine-1 applies it only after the first freeze
            for t in h.scn.trades:
                await h.tg.add_trade(t, publish=t is not hidden)
                h.shard_of(t.account_id).fills.append(t)
            for c in h.scn.cash:
                await h.shard_of(c.account_id).add_cash(c)
            h.re1.missing_keys = {lagging.trade_key}
            h.re1.stale_acks = 1
            await eventually(lambda: h.svc.copies.trades_applied >= len(h.scn.trades) - 1 and h.svc.copies.cash_applied >= len(h.scn.cash))
            assert (await h.api.settle_run()).ok
            await h.wait_phase("DONE")
            log = step_log(h)["TRADES_RECONCILED"]
            assert log["pulled_trades"] == 1 and log["refreezes"] == 1 and log["trades"] == len(h.scn.trades)
            assert len(h.re0.freeze_calls) == len(h.re1.freeze_calls) == 2
            assert any(q.account_id == "ACC3" for q in h.tg.query_calls)
            digest = rows(h, f"SELECT \"check\", status FROM recon_checks WHERE trading_day='{D}' AND \"check\"='TRADE_DIGEST'")
            assert digest and {s for _, s in digest} == {"PASS"}
        finally:
            await h.close()

    async def test_persistent_mismatch_fails_with_detail(
        self, tmp_path: Path, bus_config: FullConfig, pool: ProcessPoolExecutor
    ) -> None:
        h = Harness(bus_config, tmp_path, executor=pool, render_reports=False)
        try:
            await ready_day(h)
            h.re1.missing_keys = {h.scn.trades[3].trade_key}  # ACC4's fill is never in risk-engine-1's ack
            h.re1.stale_acks = None
            assert (await h.api.settle_run()).ok
            st = await h.wait_phase("FAILED")
            assert "digests still differ" in (st.error or "") and "ACC4" in (st.error or "")
            assert len(h.re1.freeze_calls) == 1 + FAST.recon_refreezes
            fail = rows(h, f"SELECT account_id, detail FROM recon_checks WHERE trading_day='{D}' AND status='FAIL'")
            assert [a for a, _ in fail] == ["ACC4"]
            detail = json.loads(fail[0][1])
            assert detail["local"]["count"] == detail["trade_gateway"]["count"] == 1 and detail["risk"]["count"] == 0
            assert rows(h, "SELECT count(*) FROM account_settlements") == [(0,)]
            events = await eod_events(h)
            assert [e.phase.value for e in events] == ["BEGIN", "FROZEN", "FAILED"]
            assert events[-1].error == st.error
            diffs = events[-1].detail["differences"]  # 02 §8.4 step 3: the FAILED detail lists the differences
            assert [(d["check"], d["account_id"]) for d in diffs] == [("TRADE_DIGEST", "ACC4")]
            # a re-run after the operator fixed the shard gets a new version
            h.re1.stale_acks = 0
            ack = (await h.api.settle_run()).open_as(SettleRunAck)
            assert ack.version == 2
            await h.wait_phase("DONE")
        finally:
            await h.close()


class TestRestart:
    async def test_restart_mid_saga_resumes_from_last_step(
        self, tmp_path: Path, bus_config: FullConfig, pool: ProcessPoolExecutor
    ) -> None:
        h = Harness(bus_config, tmp_path, executor=pool, render_reports=False)
        try:
            await ready_day(h)
            h.md.gate = asyncio.Event()  # step 4 hangs on md.settle_inputs
            assert (await h.api.settle_run()).ok
            await eventually(lambda: h.md.calls >= 1)
            assert (await h.status()).phase.value == "TRADES_RECONCILED"
            freezes = (len(h.re0.freeze_calls), len(h.re1.freeze_calls))
            await h.stop_clearing()  # crash-like stop in the middle of the saga
            assert rows(h, f"SELECT phase FROM eod_runs WHERE trading_day='{D}'") == [("TRADES_RECONCILED",)]
            h.md.gate.set()
            h.md.gate = None
            await h.start_clearing()
            await h.wait_phase("DONE")
            assert (len(h.re0.freeze_calls), len(h.re1.freeze_calls)) == freezes  # no re-freeze after resume
            assert h.md.calls == 2
            assert [e.phase.value for e in await eod_events(h)] == PHASES
            assert all(s.rolled.get(NEXT) == 1 for s in h.shards)
        finally:
            await h.close()


class TestSodVersions:
    async def test_snapshot_version_semantics_and_seed_correction(
        self, tmp_path: Path, bus_config: FullConfig, pool: ProcessPoolExecutor
    ) -> None:
        h = Harness(bus_config, tmp_path, executor=pool, render_reports=False)
        try:
            await h.start_stubs()
            await h.start_clearing()
            assert (await h.api.call("settle.sod_snapshot", sod_req(D))).code == codes.NOT_FOUND
            assert (await h.api.call("settle.sod_snapshot", sod_req(D, 0))).code == codes.NOT_FOUND
            for s in h.shards:
                s.rollover_gate = asyncio.Event()
            await h.seed()
            await eventually(lambda: all(s.rollovers for s in h.shards))
            v0 = await h.api.call("settle.sod_snapshot", sod_req(D, 0))
            assert v0.ok and v0.open_as(SodSnapshot).version == 0  # by version: even before the push completed
            assert (await h.api.call("settle.sod_snapshot", sod_req(D))).code == codes.NOT_FOUND  # nothing pushed yet
            for s in h.shards:
                s.rollover_gate.set()
            await eventually(lambda: all(s.rolled.get(D) == 0 for s in h.shards))
            await eventually(lambda: rows(h, f"SELECT pushed_ns FROM sod_versions WHERE trading_day='{D}'")[0][0] is not None)
            latest = (await h.api.call("settle.sod_snapshot", sod_req(D))).open_as(SodSnapshot)
            assert latest.version == 0
            part = await h.api.call("settle.sod_snapshot", SodRequest(trading_day=D, account_ids=["ACC3"]))
            assert [a.account_id for a in part.open_as(SodSnapshot).accounts] == ["ACC3"]

            # correction of ACC1 only: a new SEED version, other accounts carried over; shards rebase
            for s in h.shards:
                s.rollover_gate = asyncio.Event()
            req = h.scn.seed_request()
            acc1 = next(a for a in req.accounts if a.account_id == "ACC1")
            fixed = msgspec.structs.replace(acc1, pre_balance="10000500.00")
            req2 = msgspec.structs.replace(req, accounts=[fixed], command_id=uuid7(), reason="fix ACC1")
            v1 = await h.api.call("settle.seed_sod", req2)
            assert v1.ok and v1.open_as(SodSnapshot).version == 1
            assert (await h.api.call("settle.sod_snapshot", sod_req(D))).open_as(SodSnapshot).version == 0
            snap1 = (await h.api.call("settle.sod_snapshot", sod_req(D, 1))).open_as(SodSnapshot)
            assert len(snap1.accounts) == 5 and next(a for a in snap1.accounts if a.account_id == "ACC1").pre_balance == "10000500.00"
            for s in h.shards:
                s.rollover_gate.set()
            await eventually(lambda: all(s.rolled.get(D) == 1 for s in h.shards))
            await eventually(lambda: rows(h, f"SELECT pushed_ns FROM sod_versions WHERE trading_day='{D}' AND version=1")[0][0] is not None)
            assert (await h.api.call("settle.sod_snapshot", sod_req(D))).open_as(SodSnapshot).version == 1
            assert all(s.rollovers[-1].trading_day == PREV and s.rollovers[-1].sod_version == 1 for s in h.shards)

            # guards: older day, role, unknown account
            prev_req = h.scn.seed_request(trading_day=PREV)
            flat = [msgspec.structs.replace(a, lots=[]) for a in prev_req.accounts]
            older = await h.api.call("settle.seed_sod", msgspec.structs.replace(prev_req, accounts=flat))
            assert older.code == codes.CONFLICT  # a later day's snapshot exists
            lots_late = await h.api.call("settle.seed_sod", prev_req)
            assert lots_late.code == codes.BAD_REQUEST  # lots must be opened before the seeded day
            bad_role = msgspec.structs.replace(h.scn.seed_request(), role=Role.TRADER)
            assert (await h.api.call("settle.seed_sod", bad_role)).code == codes.FORBIDDEN
            unknown = h.scn.seed_request()
            ghost = msgspec.structs.replace(unknown.accounts[0], account_id="ACC9")
            assert (await h.api.call("settle.seed_sod", msgspec.structs.replace(unknown, accounts=[ghost]))).code == codes.BAD_REQUEST
            other = await h.api.call("settle.seed_sod", h.scn.seed_request(), timeout_ms=3000)
            assert other.ok  # a fresh command id is a new correction (v2)
            dup_key = h.scn.seed_request("fixed-key")
            first = await h.api.call("settle.seed_sod", dup_key)
            again = await h.api.call("settle.seed_sod", dup_key)
            assert first.ok and again.duplicate and again.open_as(SodSnapshot).version == first.open_as(SodSnapshot).version
            # only api-gateway may seed (02 §4.11 whitelist)
            bot = Api(bus_config, identity="strategy-sim")
            try:
                denied = await bot.call("settle.seed_sod", h.scn.seed_request(), retries=0)
                assert denied.code == codes.FORBIDDEN
                assert (await bot.call("settle.sod_snapshot", sod_req(D))).ok  # queries are open
            finally:
                await bot.close()
        finally:
            await h.close()


class TestRepush:
    async def test_repush_to_shard_that_comes_back(self, tmp_path: Path, bus_config: FullConfig, pool: ProcessPoolExecutor) -> None:
        settings = dataclasses.replace(FAST, repush_interval_s=60.0)  # only the heartbeat triggers the re-push
        h = Harness(bus_config, tmp_path, executor=pool, settings=settings, render_reports=False)
        new: StrictShard | None = None
        try:
            await ready_day(h)
            h.re1.rollover_gate = asyncio.Event()
            assert (await h.api.settle_run()).ok
            await eventually(lambda: any(r.next_trading_day == NEXT for r in h.re1.rollovers) and h.re0.rolled.get(NEXT) == 1)
            assert (await h.status()).phase.value == "SETTLED"
            await h.re1.stop()  # the shard dies before confirming
            await eventually(lambda: h.svc.rt.peers.state("risk-engine-1") == "DOWN", timeout=5)
            assert rows(h, f"SELECT pushed_ns FROM sod_versions WHERE trading_day='{NEXT}'") == [(None,)]
            new = StrictShard(bus_config, tmp_path / "re1b", "risk-engine-1")
            await new.start(with_db=True)
            await h.wait_phase("DONE", timeout=20)
            assert any(r.next_trading_day == NEXT and r.sod_version == 1 for r in new.rollovers)
            assert new.snapshots[-1].trading_day == NEXT and new.snapshots[-1].version == 1
            assert {a.account_id for a in new.snapshots[-1].accounts} == set(new.accounts)
            assert rows(h, f"SELECT pushed_ns IS NOT NULL FROM sod_versions WHERE trading_day='{NEXT}'") == [(1,)]
            assert step_log(h)["ROLLED"]["shards"] == ["risk-engine-0", "risk-engine-1"]
        finally:
            if new is not None:
                await new.stop()
            await h.close()


class TestPrices:
    async def test_override_official_priority_and_resettlement(
        self, tmp_path: Path, bus_config: FullConfig, pool: ProcessPoolExecutor
    ) -> None:
        h = Harness(bus_config, tmp_path, executor=pool, render_reports=False)
        try:
            await ready_day(h)
            imp = OfficialPriceImport(
                command_id=uuid7(),
                trading_day=D,
                prices=[OfficialPrice(instrument_id="rb2701", price="3119"), OfficialPrice(instrument_id="rb2705", price="3151")],
                source="shfe_file",
                actor="admin",
                role=Role.ADMIN,
            )
            r = await h.api.call("settle.import_official", imp)
            assert r.ok and r.open_as(ImportAck) == ImportAck(inserted=2, changed=0)
            ov = PriceOverride(command_id=uuid7(), trading_day=D, instrument_id="rb2701", price="3121", reason="exchange notice",
                               actor="rm", role=Role.RISK_MANAGER)
            r = await h.api.call("settle.override_price", ov)
            view = r.open_as(SettlementPriceView)
            assert r.ok and view.source == SettlePriceSource.MANUAL and view.version == 1 and "next run" in (r.message or "")
            off_tick = msgspec.structs.replace(ov, command_id=uuid7(), price="3121.5")
            assert (await h.api.call("settle.override_price", off_tick)).code == codes.BAD_REQUEST
            trader = PriceOverride(command_id=uuid7(), trading_day=D, instrument_id="rb2701", price="3121", reason="x",
                                   actor="t", role=Role.TRADER)
            assert (await h.api.call("settle.override_price", trader)).code == codes.FORBIDDEN

            assert (await h.api.settle_run()).ok
            await h.wait_phase("DONE")
            v1 = (await h.api.call("settle.query", SettleQuery(trading_day=D))).open_as(SettlementResult)
            src = {p.instrument_id: (p.source.value, p.price) for p in v1.prices}
            assert src["rb2701"] == ("MANUAL", "3121")  # manual > official > VWAP
            assert src["rb2705"] == ("OFFICIAL", "3151")
            assert src["IF2612"] == ("VWAP", "4448.2")
            assert src["cu2611"] == ("FALLBACK_MEDIAN", "78500")
            manual = next(p for p in v1.prices if p.instrument_id == "rb2701")
            assert manual.operator == "rm" and manual.reason == "exchange notice"

            # after PRICES_FIXED: an override needs force_new_version; an official price that differs WARNs
            late = PriceOverride(command_id=uuid7(), trading_day=D, instrument_id="rb2701", price="3122", reason="late fix",
                                 actor="rm", role=Role.RISK_MANAGER)
            r = await h.api.call("settle.override_price", late)
            assert r.ok and "force_new_version" in (r.message or "") and r.open_as(SettlementPriceView).version == 2
            imp2 = OfficialPriceImport(command_id=uuid7(), trading_day=D, prices=[OfficialPrice(instrument_id="rb2705", price="3149")],
                                       source="shfe_file", actor="admin", role=Role.ADMIN)
            r = await h.api.call("settle.import_official", imp2)
            assert r.ok and r.open_as(ImportAck).changed == 1 and (r.message or "").startswith("WARN")
            assert (await h.api.call("settle.import_official", imp2)).duplicate

            # force_new_version: version 2 settles with the new prices and is pushed again (shards rebase)
            ack = (await h.api.settle_run(force=True)).open_as(SettleRunAck)
            assert ack.version == 2
            await h.wait_phase("DONE")
            v2 = (await h.api.call("settle.query", SettleQuery(trading_day=D))).open_as(SettlementResult)
            src2 = {p.instrument_id: (p.source.value, p.price) for p in v2.prices}
            assert v2.version == 2 and src2["rb2701"] == ("MANUAL", "3122") and src2["rb2705"] == ("OFFICIAL", "3149")
            old = (await h.api.call("settle.query", SettleQuery(trading_day=D, version=1))).open_as(SettlementResult)
            assert {p.instrument_id: p.price for p in old.prices}["rb2701"] == "3121"
            for s in h.shards:
                assert s.rolled.get(NEXT) == 2 and s.rollovers[-1].sod_version == 2
            nxt = (await h.api.call("settle.sod_snapshot", sod_req(NEXT))).open_as(SodSnapshot)
            assert nxt.version == 2 and nxt.pre_settle["rb2701"] == "3122"
            assert Decimal(next(a.equity for a in v2.accounts if a.account_id == "ACC1")) != Decimal(
                next(a.equity for a in v1.accounts if a.account_id == "ACC1")
            )
        finally:
            await h.close()


class TestIdempotency:
    async def test_settle_run_idempotency_and_preconditions(
        self, tmp_path: Path, bus_config: FullConfig, pool: ProcessPoolExecutor
    ) -> None:
        h = Harness(bus_config, tmp_path, executor=pool, render_reports=False)
        try:
            await ready_day(h)
            early = await h.api.settle_run(PREV)
            assert early.code == codes.PRECONDITION_FAILED  # no SOD for the previous day
            assert (await h.api.settle_run("20260926")).code == codes.BAD_REQUEST  # Saturday
            h.md.gate = asyncio.Event()
            cmd = uuid7()
            r1 = await h.api.settle_run(command_id=cmd)
            r2 = await h.api.settle_run(command_id=cmd)
            assert r1.ok and not r1.duplicate and r2.duplicate
            assert r2.open_as(SettleRunAck) == r1.open_as(SettleRunAck) == SettleRunAck(trading_day=D, version=1, phase=r1.open_as(SettleRunAck).phase)
            conflict = await h.api.call(
                "settle.run", SettleRunRequest(command_id=cmd, trading_day=D, force_new_version=True, actor="ops")
            )
            assert conflict.code == codes.CONFLICT  # same key, other content
            busy = await h.api.settle_run(force=True)
            assert busy.code == codes.CONFLICT and "in progress" in (busy.message or "")
            again = await h.api.settle_run()
            assert again.ok and again.open_as(SettleRunAck).version == 1  # continues the run in progress
            h.md.gate.set()
            await h.wait_phase("DONE")
            done = await h.api.settle_run()
            assert done.ok and done.open_as(SettleRunAck).version == 1 and "already settled" in (done.message or "")
            assert rows(h, f"SELECT count(*) FROM eod_runs WHERE trading_day='{D}'") == [(1,)]
            assert (await h.api.settle_run(PREV)).code == codes.CONFLICT  # a later day is settled
            bot = Api(bus_config, identity="strategy-sim")
            try:
                assert (await bot.call("settle.run", SettleRunRequest(command_id=uuid7(), trading_day=D), retries=0)).code == codes.FORBIDDEN
            finally:
                await bot.close()
        finally:
            await h.close()


class TestScheduler:
    async def test_scheduler_starts_eod_at_eod_start_at(self, tmp_path: Path, bus_config: FullConfig, pool: ProcessPoolExecutor) -> None:
        settings = dataclasses.replace(FAST, scheduler_enabled=True, scheduler_interval_s=0.1)
        h = Harness(bus_config, tmp_path, executor=pool, settings=settings, clock_at="2026-09-28 15:29:57",
                    render_reports=False)
        try:
            await ready_day(h)
            st = await h.wait_phase("DONE", timeout=20)
            assert st.version == 1
            run = rows(h, f"SELECT triggered_by, started_ns FROM eod_runs WHERE trading_day='{D}'")
            assert run[0][0] == "scheduler"
            from clearing_support import ts

            assert run[0][1] >= ts("2026-09-28 15:30:00")
            assert h.svc.scheduler.last_started == D
            await asyncio.sleep(0.3)
            assert await h.svc.scheduler.tick() is None  # NEXT is not due yet; D already has a run
        finally:
            await h.close()


class TestBacktests:
    async def test_backtest_job_runs_and_registers_report(self, tmp_path: Path, bus_config: FullConfig, pool: ProcessPoolExecutor) -> None:
        settings = dataclasses.replace(FAST, backtest_runner="fake_backtest:run_job")
        h = Harness(bus_config, tmp_path, executor=pool, settings=settings)
        try:
            await h.start_clearing()
            req = BacktestRunRequest(command_id=uuid7(), config_name="breaker_2016", overrides={"seed": 11}, actor="ops")
            r = await h.api.call("backtest.run", req)
            assert r.ok and r.open_as(BacktestStatus).state == BacktestState.QUEUED
            assert (await h.api.call("backtest.run", req)).duplicate
            run_id = r.open_as(BacktestStatus).run_id

            async def finished() -> bool:
                st = (await h.api.call("backtest.status", BacktestStatusRequest(run_id=run_id))).open_as(BacktestStatus)
                return bool(st.state in (BacktestState.DONE, BacktestState.FAILED))

            await eventually(finished, timeout=20)
            st = (await h.api.call("backtest.status", BacktestStatusRequest(run_id=run_id))).open_as(BacktestStatus)
            assert st.state == BacktestState.DONE and st.progress == 1.0 and st.error is None
            assert st.summary == {"rules": {"max_drawdown": "0.051"}, "no_rules": {"max_drawdown": "0.083"}}
            rep = (await h.api.call("report.get", ReportGetRequest(report_id=st.report_id or ""))).open_as(ReportReady)
            assert rep.type == ReportType.BACKTEST_HTML and rep.trading_day is None and rep.version is None
            assert rep.path == f"backtests/{run_id}/report.html" and (tmp_path / "cw" / rep.path).exists()
            row = rows(h, f"SELECT seed, git_sha, data_hash, report_id FROM backtest_runs WHERE run_id='{run_id}'")[0]
            assert row == (11, "deadbeef", "b" * 64, st.report_id)
            listed = (await h.api.call("backtest.list", BacktestListRequest())).open_as(BacktestList)
            assert [i.run_id for i in listed.items] == [run_id]
        finally:
            await h.close()

    async def test_missing_backtest_engine_fails_clearly(self, tmp_path: Path, bus_config: FullConfig, pool: ProcessPoolExecutor) -> None:
        settings = dataclasses.replace(FAST, backtest_runner="futrisk_no_such_pkg.runner:run_job")
        h = Harness(bus_config, tmp_path, executor=pool, settings=settings)
        try:
            await h.start_clearing()
            r = await h.api.call("backtest.run", BacktestRunRequest(command_id=uuid7(), config_name="x", actor="ops"))
            run_id = r.open_as(BacktestStatus).run_id

            async def failed() -> bool:
                st = (await h.api.call("backtest.status", BacktestStatusRequest(run_id=run_id))).open_as(BacktestStatus)
                return bool(st.state == BacktestState.FAILED)

            await eventually(failed, timeout=10)
            st = (await h.api.call("backtest.status", BacktestStatusRequest(run_id=run_id))).open_as(BacktestStatus)
            assert "backtest engine not available" in (st.error or "")
            assert (await h.api.call("backtest.status", BacktestStatusRequest(run_id="nope"))).code == codes.NOT_FOUND
        finally:
            await h.close()

    async def test_runner_error_marks_job_failed(self, tmp_path: Path, bus_config: FullConfig, pool: ProcessPoolExecutor) -> None:
        settings = dataclasses.replace(FAST, backtest_runner="fake_backtest:failing_job")
        h = Harness(bus_config, tmp_path, executor=pool, settings=settings)
        try:
            await h.start_clearing()
            r = await h.api.call("backtest.run", BacktestRunRequest(command_id=uuid7(), config_name="nope", actor="ops"))
            run_id = r.open_as(BacktestStatus).run_id

            async def failed() -> bool:
                st = (await h.api.call("backtest.status", BacktestStatusRequest(run_id=run_id))).open_as(BacktestStatus)
                return bool(st.state == BacktestState.FAILED)

            await eventually(failed, timeout=15)
            st = (await h.api.call("backtest.status", BacktestStatusRequest(run_id=run_id))).open_as(BacktestStatus)
            assert "unknown backtest config nope" in (st.error or "")
        finally:
            await h.close()


def test_dayroll_modes_known() -> None:
    assert {m.value for m in DayRollMode} >= {"ROLL", "REBASE"}


class TestEntryPoint:
    async def test_serve_starts_and_stops_on_ctl_shutdown(self, tmp_path: Path, bus_config: FullConfig) -> None:
        from clearing_support import market_clock_at

        from futrisk.messages.common import Ping, Pong, ShutdownRequest
        from futrisk.services.clearing.main import run, serve

        assert callable(run)
        task = asyncio.create_task(
            serve(home=tmp_path / "cw", config=bus_config, market_clock=market_clock_at("2026-09-28 16:00:00"),
                  setup_logs=False, settings=FAST)
        )
        cli = Api(bus_config, identity="cli")
        sup = Api(bus_config, identity="supervisor")
        try:
            async def running() -> bool:
                r = await cli.client.request(CW, "ctl.ping", Ping(), timeout_ms=500, retries=0)
                return bool(r.ok and r.open_as(Pong).state == "RUNNING")

            await eventually(running, timeout=15)
            assert (await sup.client.request(CW, "ctl.shutdown", ShutdownRequest(reason="test"), timeout_ms=3000)).ok
            assert await asyncio.wait_for(task, 20) == 0
            con = sqlite3.connect(tmp_path / "cw" / "db" / "clearing.db")
            try:
                assert con.execute("SELECT clean_shutdown FROM db_meta").fetchone()[0] == 1
            finally:
                con.close()
        finally:
            if not task.done():
                task.cancel()
            await cli.close()
            await sup.close()


class TestRecoveryByOperator:
    async def test_md_down_fails_then_manual_prices_rerun(
        self, tmp_path: Path, bus_config: FullConfig, pool: ProcessPoolExecutor
    ) -> None:
        h = Harness(bus_config, tmp_path, executor=pool, render_reports=False)
        try:
            await ready_day(h)
            h.md.fail = True  # md-gateway cannot compute settlement inputs
            assert (await h.api.settle_run()).ok
            st = await h.wait_phase("FAILED")
            assert "md-gateway unavailable" in (st.error or "") and "settle.override_price" in (st.error or "")
            for inst, px in (("rb2701", "3117"), ("rb2705", "3150"), ("IF2612", "4450.2")):
                ov = PriceOverride(command_id=uuid7(), trading_day=D, instrument_id=inst, price=px, reason="md down",
                                   actor="rm", role=Role.RISK_MANAGER)
                r = await h.api.call("settle.override_price", ov)
                assert r.ok and r.open_as(SettlementPriceView).version == 2  # applies to the re-run
            ack = (await h.api.settle_run()).open_as(SettleRunAck)
            assert ack.version == 2
            await h.wait_phase("DONE")
            res = (await h.api.call("settle.query", SettleQuery(trading_day=D))).open_as(SettlementResult)
            assert {p.instrument_id: (p.source.value, p.price) for p in res.prices} == {
                "rb2701": ("MANUAL", "3117"),
                "rb2705": ("MANUAL", "3150"),
                "IF2612": ("MANUAL", "4450.2"),
                "cu2611": ("PRE_SETTLE", "78500"),  # M-2: every SOD pre_settle contract is priced (carried forward)
            }
            nxt = (await h.api.call("settle.sod_snapshot", sod_req(NEXT))).open_as(SodSnapshot)
            assert nxt.pre_settle == {"IF2612": "4450.2", "cu2611": "78500", "rb2701": "3117", "rb2705": "3150"}
        finally:
            await h.close()

    async def test_failed_reports_are_regenerated_by_settle_run(
        self, tmp_path: Path, bus_config: FullConfig, pool: ProcessPoolExecutor
    ) -> None:
        h = Harness(bus_config, tmp_path, executor=pool)
        try:
            await ready_day(h)
            real = h.svc.saga._reports
            calls: list[tuple[str, int]] = []

            async def broken(td: str, version: int) -> Any:
                calls.append((td, version))
                raise OSError("disk full")

            h.svc.saga._reports = broken
            assert (await h.api.settle_run()).ok
            await h.wait_phase("DONE")  # report failures are only a WARN (02 §8.4 step 7)
            assert step_log(h)["REPORTED"]["errors"] == ["OSError: disk full"] and calls == [(D, 1)]
            assert not (tmp_path / "cw" / "reports" / D / "risk_daily.html").exists()
            h.svc.saga._reports = real
            r = await h.api.settle_run()
            assert r.ok and r.open_as(SettleRunAck).version == 1
            await eventually(lambda: (tmp_path / "cw" / "reports" / D / "risk_daily.html").exists(), timeout=15)
        finally:
            await h.close()
