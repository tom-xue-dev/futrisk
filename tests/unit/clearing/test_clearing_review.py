"""Review follow-ups of the clearing-worker over the real bus (02 §8.4; 03 §7): catch-up after missed EODs,
pinned SOD and digests, late facts, price inputs, config versions, crash points, reports, repairs, the pool."""

from __future__ import annotations

import asyncio
import dataclasses
import json
import sqlite3
import time
from collections.abc import Callable
from concurrent.futures import Executor, Future, ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from decimal import Decimal
from pathlib import Path
from typing import Any

import msgspec
import pytest
from clearing_support import CW, FAST, NEXT, PREV, D, Harness, eventually

from futrisk.common.config import FullConfig
from futrisk.common.config.products import MarginSchedule
from futrisk.common.ids import uuid7
from futrisk.core import InstrumentRegistry
from futrisk.messages import codes, decode_envelope, open_payload
from futrisk.messages.common import ReplayBatch, ReplayRequest
from futrisk.messages.enums import BacktestState, ConfigEffect, Direction, Offset, Role
from futrisk.messages.eod import (
    BacktestRunRequest,
    BacktestStatus,
    BacktestStatusRequest,
    OfficialPrice,
    OfficialPriceImport,
    PriceOverride,
    SettlementPriceView,
    SettlementResult,
    SettleQuery,
    SettleRunAck,
    SodRequest,
    SodSnapshot,
)
from futrisk.services.clearing import jobs
from futrisk.services.clearing.reports.gather import ReportService

AFTER = "20260930"
PHASES = ["BEGIN", "FROZEN", "TRADES_RECONCILED", "PRICES_FIXED", "SETTLED", "ROLLED", "REPORTED", "DONE"]


def rows(h: Harness, sql: str, *args: Any) -> list[tuple[Any, ...]]:
    con = sqlite3.connect(h.tmp / "cw" / "db" / "clearing.db")
    try:
        return con.execute(sql, args).fetchall()
    finally:
        con.close()


def step_log(h: Harness, td: str = D, version: int = 1) -> dict[str, Any]:
    raw = rows(h, "SELECT step_log FROM eod_runs WHERE trading_day=? AND version=?", td, version)[0][0]
    data: dict[str, Any] = json.loads(raw)
    return data


async def events(h: Harness, topic: str = "sys.eod") -> list[Any]:
    reply = await h.api.to(CW, "ctl.replay", ReplayRequest(topic=topic, from_seq=1))
    return [open_payload(decode_envelope(b)) for b in reply.open_as(ReplayBatch).envelopes]


def phases_of(evs: list[Any], td: str = D, version: int = 1) -> list[str]:
    return [e.phase.value for e in evs if e.trading_day == td and e.version == version and not e.detail.get("warning")]


def warnings_of(evs: list[Any], kind: str) -> list[Any]:
    return [e for e in evs if e.detail.get("kind") == kind and e.detail.get("warning")]


async def ready_day(h: Harness, *, publish_all: bool = True) -> None:
    await h.start_stubs()
    await h.start_clearing()
    await h.seed()
    await eventually(lambda: all(s.rolled.get(D) == 0 for s in h.shards))
    if publish_all:
        await h.load_day()
        await h.wait_copies()


def override(inst: str, price: str, td: str = D) -> PriceOverride:
    return PriceOverride(command_id=uuid7(), trading_day=td, instrument_id=inst, price=price, reason="review",
                         actor="rm", role=Role.RISK_MANAGER)


async def query(h: Harness, td: str = D, account: str | None = None, version: int | None = None) -> SettlementResult:
    reply = await h.api.call("settle.query", SettleQuery(trading_day=td, account_id=account, version=version))
    assert reply.ok, reply
    res: SettlementResult = reply.open_as(SettlementResult)
    return res


# ------------------------------------------------------------------ H-3 catch-up
class TestCatchUp:
    async def test_two_missed_eods_settle_in_order_catch_up_then_rebase(
        self, tmp_path: Path, bus_config: FullConfig, pool: ProcessPoolExecutor
    ) -> None:
        settings = dataclasses.replace(FAST, scheduler_interval_s=0.1)
        h = Harness(bus_config, tmp_path, executor=pool, settings=settings, clock_at="2026-09-30 10:00:00",
                    render_reports=False)
        try:
            await ready_day(h)
            late_day = h.scn.trade("ACC2", "rb2701", Direction.BUY, Offset.OPEN, 3119.0, 1, "2026-09-29 10:00:00", td=NEXT)
            await h.tg.add_trade(late_day)
            h.shard_of("ACC2").fills.append(late_day)
            await eventually(lambda: h.svc.copies.trades_applied >= len(h.scn.trades) + 1)
            for s in h.shards:  # no EOD on 09-28 nor 09-29: the shards rolled PROVISIONALly to 09-29, then 09-30
                s.provisional_roll(NEXT)
                s.provisional_roll(AFTER)
            h.svc.scheduler.start()  # due day at 09-30 10:00 is 09-29: both skipped days, oldest first
            await h.wait_phase("DONE", td=NEXT, timeout=40)
            assert rows(h, "SELECT trading_day, version, phase FROM eod_runs ORDER BY 1, 2") == [
                (D, 1, "DONE"), (NEXT, 1, "DONE")]
            for s in h.shards:
                assert not s.violations, s.violations
                assert s.modes == [(PREV, D, 0, "ROLL"), (D, NEXT, 1, "CATCH_UP"), (NEXT, AFTER, 1, "REBASE")]
                assert (s.current_day, s.mode, s.unsettled) == (AFTER, "OPEN", [])
                assert [f.trading_day for f in s.freeze_calls] == [D, NEXT]  # digest-only freezes of skipped days
            # never BEGIN a day before the previous one is ROLLED
            assert step_log(h, NEXT)["BEGIN"]["ts_ns"] >= step_log(h, D)["ROLLED"]["ts_ns"]
            nxt = await query(h, NEXT, "ACC2")
            assert [t.trade_id for t in nxt.trades] == [late_day.trade_id]
            prices = {p.instrument_id: (p.source.value, p.price) for p in (await query(h, NEXT)).prices}
            assert prices["rb2701"] == ("PRE_SETTLE", "3118") and set(prices) == {"IF2612", "cu2611", "rb2701", "rb2705"}
            after = (await h.api.call("settle.sod_snapshot", SodRequest(trading_day=AFTER))).open_as(SodSnapshot)
            assert after.version == 1 and set(after.pre_settle) == {"IF2612", "cu2611", "rb2701", "rb2705"}
        finally:
            await h.close()


# ------------------------------------------------------------ H-1 / L9 / M-6 seeds
class TestSeedGuards:
    async def test_seed_during_run_conflicts_and_the_run_keeps_its_sod(
        self, tmp_path: Path, bus_config: FullConfig, pool: ProcessPoolExecutor
    ) -> None:
        h = Harness(bus_config, tmp_path, executor=pool, render_reports=False)
        try:
            await ready_day(h)
            h.md.gate = asyncio.Event()
            assert (await h.api.settle_run()).ok
            await eventually(lambda: h.md.calls >= 1)
            req = h.scn.seed_request()
            acc1 = next(a for a in req.accounts if a.account_id == "ACC1")
            fix = msgspec.structs.replace(req, accounts=[msgspec.structs.replace(acc1, pre_balance="10000500.00")],
                                          command_id=uuid7(), reason="fix ACC1")
            r = await h.api.call("settle.seed_sod", fix)
            assert r.code == codes.CONFLICT and "EOD run in progress" in (r.message or "")
            h.md.gate.set()
            await h.wait_phase("DONE")
            log = step_log(h)
            assert log["BEGIN"]["sod_version"] == 0 and log["PRICES_FIXED"]["sod_version"] == 0
            assert (await query(h, account="ACC1")).accounts[0].pre_balance == "10000000.00"
            assert all(s.rolled.get(D) == 0 for s in h.shards)  # nothing rebased the day during its EOD
        finally:
            await h.close()

    async def test_inconsistent_basis_and_skipping_an_unsettled_day_are_refused(
        self, tmp_path: Path, bus_config: FullConfig
    ) -> None:
        h = Harness(bus_config, tmp_path)
        try:
            await h.start_stubs()
            await h.start_clearing()
            req = h.scn.seed_request()
            accts = [
                msgspec.structs.replace(a, lots=[msgspec.structs.replace(a.lots[0], basis_price="3105")])
                if a.account_id == "ACC1" else a
                for a in req.accounts
            ]
            bad = await h.api.call("settle.seed_sod", msgspec.structs.replace(req, accounts=accts, command_id=uuid7()))
            assert bad.code == codes.BAD_REQUEST and "basis" in (bad.message or "") and "ACC1 rb2701" in (bad.message or "")
            assert (await h.api.call("settle.seed_sod", req)).ok
            # a pre_settle correction that contradicts the carried-over lots (ACC1 and ACC5 hold rb2701 at 3110)
            flat = next(a for a in req.accounts if a.account_id == "ACC2")
            moved = msgspec.structs.replace(req, command_id=uuid7(), accounts=[flat], pre_settle={"rb2701": "3111"})
            r = await h.api.call("settle.seed_sod", moved)
            assert r.code == codes.BAD_REQUEST and "ACC5 rb2701" in (r.message or "")
            # L9: seeding a later day while the newest SOD day is not settled would skip it
            later = h.scn.seed_request(trading_day=NEXT)
            r = await h.api.call("settle.seed_sod", later)
            assert r.code == codes.CONFLICT and "not settled" in (r.message or "")
        finally:
            await h.close()


# ------------------------------------------------------------------ H-2 late facts
class TestLateFacts:
    async def test_fact_after_reconcile_goes_back_to_frozen(
        self, tmp_path: Path, bus_config: FullConfig, pool: ProcessPoolExecutor
    ) -> None:
        h = Harness(bus_config, tmp_path, executor=pool, render_reports=False)
        try:
            await ready_day(h)
            h.md.gate = asyncio.Event()
            assert (await h.api.settle_run()).ok
            await eventually(lambda: h.md.calls >= 1)  # TRADES_RECONCILED pinned the digests, step 4 waits
            late = h.scn.trade("ACC2", "rb2701", Direction.BUY, Offset.OPEN, 3119.0, 1, "2026-09-28 14:59:00")
            await h.tg.add_trade(late)  # e.g. td.import_trades after the digest
            h.shard_of("ACC2").fills.append(late)
            await eventually(lambda: h.svc.copies.trades_applied >= len(h.scn.trades) + 1)
            h.md.gate.set()
            await h.wait_phase("DONE")
            assert phases_of(await events(h)) == [
                "BEGIN", "FROZEN", "TRADES_RECONCILED", "PRICES_FIXED",
                "FROZEN", "TRADES_RECONCILED", "PRICES_FIXED", "SETTLED", "ROLLED", "REPORTED", "DONE",
            ]
            log = step_log(h)
            assert log["FROZEN"]["rewind"]["accounts"] == ["ACC2"] and log["FROZEN"]["refreezes_used"] == 1
            assert log["TRADES_RECONCILED"]["refreezes_used"] == 1
            assert [t.trade_id for t in (await query(h, account="ACC2")).trades] == [late.trade_id]
        finally:
            await h.close()

    async def test_new_contract_after_reconcile_fails_with_the_remedy_then_settles_with_a_manual_price(
        self, tmp_path: Path, bus_config: FullConfig, pool: ProcessPoolExecutor
    ) -> None:
        h = Harness(bus_config, tmp_path, executor=pool, render_reports=False)
        try:
            await ready_day(h)
            h.md.gate = asyncio.Event()
            assert (await h.api.settle_run()).ok
            await eventually(lambda: h.md.calls >= 1)
            late = h.scn.trade("ACC2", "hc2701", Direction.BUY, Offset.OPEN, 3300.0, 1, "2026-09-28 14:59:00")
            await h.tg.add_trade(late)
            h.shard_of("ACC2").fills.append(late)
            await eventually(lambda: h.svc.copies.trades_applied >= len(h.scn.trades) + 1)
            h.md.gate.set()
            st = await h.wait_phase("FAILED")
            assert "no settlement price for hc2701" in (st.error or "") and "settle.override_price" in (st.error or "")
            assert (await h.api.call("settle.override_price", override("hc2701", "3301"))).ok
            assert (await h.api.settle_run()).open_as(SettleRunAck).version == 2
            await h.wait_phase("DONE")
            res = await query(h, account="ACC2")
            hc = next(p for p in res.prices if p.instrument_id == "hc2701")
            assert (hc.source.value, hc.price) == ("MANUAL", "3301")
            pos = next(p for p in res.positions if p.instrument_id == "hc2701")
            assert pos.pre_settle_price == "3301"  # M-3: the manual price stands in as its previous settlement price
            assert step_log(h, version=2)["PRICES_FIXED"]["pre_settle_fallback"] == {"hc2701": "MANUAL"}
        finally:
            await h.close()

    async def test_fact_after_done_is_flagged_and_resettled_without_freezing(
        self, tmp_path: Path, bus_config: FullConfig, pool: ProcessPoolExecutor
    ) -> None:
        h = Harness(bus_config, tmp_path, executor=pool, render_reports=False)
        try:
            await ready_day(h)
            assert (await h.api.settle_run()).ok
            await h.wait_phase("DONE")
            first = h.scn.trade("ACC2", "rb2701", Direction.BUY, Offset.OPEN, 3119.0, 1, "2026-09-28 14:59:00")
            second = h.scn.trade("ACC2", "rb2701", Direction.BUY, Offset.OPEN, 3120.0, 1, "2026-09-28 14:59:30")
            for t in (first, second):
                await h.tg.add_trade(t)  # broker resync after the EOD
            await eventually(lambda: h.svc.copies.trades_applied >= len(h.scn.trades) + 2)
            await h.svc.db.run_write(lambda c: None)
            warns = warnings_of(await events(h), "LATE_FACT")
            assert len(warns) == 1  # one WARN per (run, account); the second fact only raises the counters
            w = warns[0]
            assert (w.trading_day, w.version, w.phase.value) == (D, 1, "DONE") and w.detail["account_id"] == "ACC2"
            assert "force_new_version" in w.detail["warning"] and w.detail["warn_id"]
            entry = step_log(h)["WARNINGS"][0]
            assert entry["kind"] == "LATE_FACT" and entry["trades"] == 2
            st = await h.status()
            assert st.phase.value == "DONE" and st.detail["warning_count"] == 1
            assert st.detail["warnings"][0]["kind"] == "LATE_FACT"
            # the operator re-settles: the shards trade 09-29 already, so they are not frozen again (REBASE at the end)
            freezes = [len(s.freeze_calls) for s in h.shards]
            assert (await h.api.settle_run(force=True)).open_as(SettleRunAck).version == 2
            st = await h.wait_phase("DONE")
            assert st.version == 2 and [len(s.freeze_calls) for s in h.shards] == freezes
            assert step_log(h, version=2)["BEGIN"]["resettle_of"] == 1
            assert sorted(t.trade_id for t in (await query(h, account="ACC2")).trades) == sorted(
                [first.trade_id, second.trade_id])
            digest = rows(h, "SELECT status FROM recon_checks WHERE trading_day=? AND version=2 AND account_id='ACC2' "
                             "AND \"check\"='TRADE_DIGEST'", D)
            assert digest == [("WARN",)]  # agrees with trade-gateway, differs from v1
            for s in h.shards:
                assert s.rolled[NEXT] == 2 and s.modes[-1][3] == "REBASE" and not s.violations
        finally:
            await h.close()


# --------------------------------------------------------------- M-1..M-4 prices
class TestPriceInputs:
    async def test_new_contract_with_md_down(self, tmp_path: Path, bus_config: FullConfig, pool: ProcessPoolExecutor) -> None:
        h = Harness(bus_config, tmp_path, executor=pool, render_reports=False)
        try:
            await ready_day(h, publish_all=False)
            new = h.scn.trade("ACC2", "hc2701", Direction.BUY, Offset.OPEN, 3300.0, 1, "2026-09-28 10:05:00")
            h.scn.trades.append(new)  # first traded today: no previous settlement price in the SOD
            await h.load_day()
            await h.wait_copies()
            h.md.fail = True
            assert (await h.api.settle_run()).ok
            st = await h.wait_phase("FAILED")
            assert "hc2701" in (st.error or "") and "pre_settle" in (st.error or "")
            for inst, px in (("rb2701", "3117"), ("rb2705", "3150"), ("IF2612", "4450.2"), ("hc2701", "3301")):
                assert (await h.api.call("settle.override_price", override(inst, px))).ok
            assert (await h.api.settle_run()).ok
            await h.wait_phase("DONE")
            nxt = (await h.api.call("settle.sod_snapshot", SodRequest(trading_day=NEXT))).open_as(SodSnapshot)
            assert nxt.pre_settle["hc2701"] == "3301" and nxt.pre_settle["cu2611"] == "78500"
            assert step_log(h, version=2)["PRICES_FIXED"]["pre_settle_fallback"] == {"hc2701": "MANUAL"}
        finally:
            await h.close()

    async def test_override_and_import_during_step4_apply_to_the_run(
        self, tmp_path: Path, bus_config: FullConfig, pool: ProcessPoolExecutor
    ) -> None:
        h = Harness(bus_config, tmp_path, executor=pool, render_reports=False)
        try:
            await ready_day(h)
            h.md.inputs["cu2611"] = msgspec.structs.replace(h.md.inputs["cu2611"], pre_settle="78510")  # SOD: 78500
            h.md.gate = asyncio.Event()
            assert (await h.api.settle_run()).ok
            await eventually(lambda: h.md.calls >= 1)  # md.settle_inputs is pending OUTSIDE the price lock
            t0 = time.perf_counter()
            r = await h.api.call("settle.override_price", override("rb2701", "3121"), timeout_ms=3_000, retries=0)
            assert r.ok and "run in progress (v1)" in (r.message or "") and r.open_as(SettlementPriceView).version == 1
            imp = OfficialPriceImport(command_id=uuid7(), trading_day=D, prices=[OfficialPrice(instrument_id="rb2705", price="3151")],
                                      source="shfe_file", actor="admin", role=Role.ADMIN)
            r = await h.api.call("settle.import_official", imp, timeout_ms=3_000, retries=0)
            assert r.ok and "run in progress (v1)" in (r.message or "")
            assert time.perf_counter() - t0 < 2.0
            h.md.gate.set()
            await h.wait_phase("DONE")
            src = {p.instrument_id: (p.source.value, p.price) for p in (await query(h)).prices}
            assert src["rb2701"] == ("MANUAL", "3121") and src["rb2705"] == ("OFFICIAL", "3151")
            assert src["cu2611"] == ("FALLBACK_MEDIAN", "78500")  # MEDIAN(78400, 78700, SOD pre 78500), not md's 78510
            pf = step_log(h)["PRICES_FIXED"]
            assert pf["pre_settle_mismatch"] == {"cu2611": {"sod": "78500", "md": "78510"}}
            assert pf["warnings"][0]["kind"] == "PRE_SETTLE_MISMATCH"
            assert [w["kind"] for w in step_log(h)["WARNINGS"]] == ["PRE_SETTLE_MISMATCH"]
            # after the settlement an official price that differs WARNs: reply, log and a sys.eod WARN event
            imp2 = OfficialPriceImport(command_id=uuid7(), trading_day=D, prices=[OfficialPrice(instrument_id="rb2705", price="3149")],
                                       source="shfe_file", actor="admin", role=Role.ADMIN)
            r = await h.api.call("settle.import_official", imp2)
            assert r.ok and (r.message or "").startswith("WARN") and "(v2)" in (r.message or "")
            w = warnings_of(await events(h), "OFFICIAL_PRICE_DIFF")
            assert len(w) == 1 and w[0].detail["instruments"] == {"rb2705": {"used": "3151", "official": "3149"}}
        finally:
            await h.close()


# ------------------------------------------------------------ M-5 config versions
class TestConfigVersions:
    async def test_the_day_settles_with_the_config_in_force_on_that_day(
        self, tmp_path: Path, bus_config: FullConfig, pool: ProcessPoolExecutor
    ) -> None:
        h = Harness(bus_config, tmp_path, executor=pool, render_reports=False)
        try:
            await ready_day(h)
            rt = h.svc.rt
            cfg = rt.config
            v0 = cfg.config_version
            rb = cfg.products.products["rb"]
            products = cfg.products.model_copy(update={"products": {
                **cfg.products.products, "rb": rb.model_copy(update={"margin": [MarginSchedule(exchange_rate=Decimal("0.20"))]})}})
            new = dataclasses.replace(cfg, products=products, config_version=v0 + 1, config_hash="e" * 64,
                                      files={**cfg.files, "products.yaml": "e" * 64})
            # api-gateway stamps NEXT_TRADING_DAY sections with the day they apply from (active.json), then commits
            assert rt.layout is not None
            rt.layout.active_json.parent.mkdir(parents=True, exist_ok=True)
            rt.layout.active_json.write_text(json.dumps({"config_version": v0 + 1, "path": "config/versions/x",
                                                         "effective": {"products": {"effect": "NEXT_TRADING_DAY",
                                                                                    "trading_day": NEXT}}}), encoding="utf-8")
            rt._apply_config(new, {"products": ConfigEffect.NEXT_TRADING_DAY})
            await eventually(lambda: len(rows(h, "SELECT config_version FROM config_history")) == 2)
            assert rows(h, "SELECT config_version, settle_from FROM config_history ORDER BY 1") == [(v0, None), (v0 + 1, NEXT)]
            h.md.gate = asyncio.Event()
            assert (await h.api.settle_run()).ok
            await eventually(lambda: h.md.calls >= 1)
            rt._config = dataclasses.replace(new, config_version=v0 + 2, config_hash="f" * 64)  # a commit during the run
            h.md.gate.set()
            await h.wait_phase("DONE")
            log = step_log(h)
            assert log["BEGIN"]["config_version"] == v0 and log["SETTLED"]["config_version"] == v0
            assert rows(h, "SELECT config_hash FROM eod_runs WHERE trading_day=?", D) == [(cfg.config_hash,)]
            expected = InstrumentRegistry(cfg.products, cfg.calendar).margin_rate("rb2701", D)
            acc1 = await query(h, account="ACC1")
            rates = {Decimal(p.margin_rate) for p in acc1.positions if p.instrument_id == "rb2701"}
            assert rates == {expected} and expected != Decimal("0.20")
        finally:
            await h.close()


# ----------------------------------------------------------------- crash points
class TestCrashPoints:
    async def test_crash_at_every_phase_resumes_without_duplicates(
        self, tmp_path: Path, bus_config: FullConfig, pool: ProcessPoolExecutor, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        h = Harness(bus_config, tmp_path, executor=pool)

        def phase() -> list[tuple[Any, ...]]:
            return rows(h, "SELECT phase FROM eod_runs WHERE trading_day=?", D)

        try:
            await ready_day(h, publish_all=False)
            hidden = h.scn.trades[2]  # ACC3: never copied through td.exec, step 3 pulls it
            for t in h.scn.trades:
                await h.tg.add_trade(t, publish=t is not hidden)
                h.shard_of(t.account_id).fills.append(t)
            for c in h.scn.cash:
                await h.shard_of(c.account_id).add_cash(c)
            await eventually(lambda: h.svc.copies.trades_applied >= len(h.scn.trades) - 1 and h.svc.copies.cash_applied >= 2)

            # 1) after BEGIN: the shards hold eod.freeze
            gate = asyncio.Event()
            for s in h.shards:
                s.freeze_gate = gate
            assert (await h.api.settle_run()).ok
            await eventually(lambda: all(s.freeze_calls for s in h.shards))
            await h.stop_clearing()
            assert phase() == [("BEGIN",)]
            for s in h.shards:
                s.freeze_gate = None
            gate.set()
            # 2) after FROZEN: trade-gateway holds td.digest
            h.tg.digest_gate = asyncio.Event()
            await h.start_clearing()
            await eventually(lambda: h.tg.digest_calls >= 1 and phase() == [("FROZEN",)])
            await h.stop_clearing()
            assert phase() == [("FROZEN",)]
            h.tg.digest_gate.set()
            h.tg.digest_gate = None
            # 3) mid-reconcile after a pull: the re-freeze that follows the pull is held
            gate = asyncio.Event()
            for s in h.shards:
                s.freeze_gate, s.freeze_gate_skip = gate, 3
            await h.start_clearing()
            await eventually(lambda: any(q.account_id == "ACC3" for q in h.tg.query_calls) and all(len(s.freeze_calls) >= 4 for s in h.shards))
            await h.stop_clearing()
            assert phase() == [("FROZEN",)] and rows(h, "SELECT count(*) FROM trades") == [(len(h.scn.trades),)]
            for s in h.shards:
                s.freeze_gate = None
            gate.set()
            # 4) after SETTLED: the shards hold eod.rollover
            gate = asyncio.Event()
            for s in h.shards:
                s.rollover_gate = gate
            await h.start_clearing()
            await eventually(lambda: phase() == [("SETTLED",)] and all(s.rollovers[-1].next_trading_day == NEXT for s in h.shards))
            await h.stop_clearing()
            assert phase() == [("SETTLED",)] and rows(h, "SELECT version, count(*) FROM account_settlements GROUP BY 1") == [(1, 5)]
            for s in h.shards:
                s.rollover_gate = None
            held = gate  # released once the next instance runs: those replies go nowhere
            # 5) ROLLED, reports partly registered
            orig = ReportService.register
            hold = asyncio.Event()
            seen = {"n": 0}

            async def slow_register(self: ReportService, *args: Any, **kwargs: Any) -> Any:
                seen["n"] += 1
                if seen["n"] > 3:
                    await hold.wait()
                return await orig(self, *args, **kwargs)

            monkeypatch.setattr(ReportService, "register", slow_register)
            await h.start_clearing()
            held.set()
            await eventually(lambda: phase() == [("ROLLED",)] and seen["n"] > 3, timeout=20)
            await h.stop_clearing()
            assert phase() == [("ROLLED",)] and rows(h, "SELECT count(*) FROM reports") == [(3,)]
            monkeypatch.setattr(ReportService, "register", orig)
            await h.start_clearing()
            await h.wait_phase("DONE")

            assert phases_of(await events(h)) == PHASES  # every phase exactly once across five restarts
            assert len(await events(h, "clearing.settled")) == 1
            assert rows(h, "SELECT count(*), count(DISTINCT path) FROM reports WHERE trading_day=?", D) == [(12, 12)]
            assert rows(h, "SELECT count(*) FROM trades") == [(len(h.scn.trades),)]
            assert rows(h, "SELECT version, pushed_ns IS NOT NULL FROM sod_versions WHERE trading_day=?", NEXT) == [(1, 1)]
            for s in h.shards:
                assert s.rolled[NEXT] == 1 and not s.violations
        finally:
            await h.close()

    async def test_restart_between_settled_and_rollover(
        self, tmp_path: Path, bus_config: FullConfig, pool: ProcessPoolExecutor
    ) -> None:
        """The reviewer's probe: the worker dies after SETTLED before any shard confirmed."""
        h = Harness(bus_config, tmp_path, executor=pool, render_reports=False)
        try:
            await ready_day(h)
            gate = asyncio.Event()
            for s in h.shards:
                s.rollover_gate = gate
            assert (await h.api.settle_run()).ok
            await h.wait_phase("SETTLED")
            await h.stop_clearing()
            for s in h.shards:
                s.rollover_gate = None
            await h.start_clearing()
            gate.set()  # the rollovers held for the dead instance complete now (duplicates for the shards)
            await h.wait_phase("DONE", timeout=30)
            assert phases_of(await events(h)) == PHASES
            assert len(await events(h, "clearing.settled")) == 1
            assert rows(h, "SELECT version, count(*) FROM account_settlements GROUP BY 1") == [(1, 5)]
            assert rows(h, "SELECT version, source, pushed_ns IS NOT NULL FROM sod_versions WHERE trading_day=?", NEXT) == [
                (1, "EOD", 1)]
            assert all(s.rolled[NEXT] == 1 and not s.violations for s in h.shards)
        finally:
            await h.close()


# ---------------------------------------------------------------------- reports
class TestReports:
    async def test_regeneration_runs_once_clears_errors_and_registers_idempotently(
        self, tmp_path: Path, bus_config: FullConfig, pool: ProcessPoolExecutor
    ) -> None:
        h = Harness(bus_config, tmp_path, executor=pool)
        try:
            await ready_day(h)
            real = h.svc.saga._reports

            async def broken(td: str, version: int) -> Any:
                raise OSError("disk full")

            h.svc.saga._reports = broken
            assert (await h.api.settle_run()).ok
            await h.wait_phase("DONE")
            h.svc.saga._reports = real
            calls: list[tuple[str, int]] = []
            orig = h.svc.reports.generate

            async def counting(td: str, version: int) -> Any:
                calls.append((td, version))
                await asyncio.sleep(0.3)
                return await orig(td, version)

            h.svc.reports.generate = counting
            r1, r2 = await asyncio.gather(h.api.settle_run(), h.api.settle_run())
            assert r1.ok and r2.ok
            await eventually(lambda: "errors_cleared" in step_log(h)["REPORTED"], timeout=20)
            assert calls == [(D, 1)] and "errors" not in step_log(h)["REPORTED"]
            assert (await h.api.settle_run()).ok
            await asyncio.sleep(0.3)
            assert calls == [(D, 1)]  # errors cleared: no further regeneration
            ids = rows(h, "SELECT report_id FROM reports WHERE trading_day=? ORDER BY path", D)
            n_ready = len(await events(h, "report.ready"))
            await orig(D, 1)  # generating the same version again keeps one row per file
            assert rows(h, "SELECT report_id FROM reports WHERE trading_day=? ORDER BY path", D) == ids and len(ids) == 12
            assert len(await events(h, "report.ready")) <= n_ready + 12
            assert not list((tmp_path / "cw" / "reports" / D).glob("*.tmp"))
        finally:
            await h.close()

    async def test_reports_render_with_peers_down(self, tmp_path: Path, bus_config: FullConfig, pool: ProcessPoolExecutor) -> None:
        h = Harness(bus_config, tmp_path, executor=pool)
        try:
            await ready_day(h)
            assert (await h.api.settle_run()).ok
            await h.wait_phase("DONE")
            for stub in (h.md, h.sup, h.re0, h.re1):
                await stub.stop()
            t0 = time.perf_counter()
            out = await h.svc.reports.generate(D, 1)
            assert len(out) == 12 and time.perf_counter() - t0 < 10
            html = (tmp_path / "cw" / "reports" / D / "risk_daily.html").read_text(encoding="utf-8")
            for note in ("行情事件不可用", "服务事件不可用", "风险事件不可用", "可用率（估算）", "FALLBACK_BENCHMARK"):
                assert note in html, note
        finally:
            await h.close()

    async def test_full_day_under_a_chinese_home(self, tmp_path: Path, bus_config: FullConfig, pool: ProcessPoolExecutor) -> None:
        home = tmp_path / "清算 数据目录"
        home.mkdir()
        h = Harness(bus_config, home, executor=pool)
        try:
            await ready_day(h)
            assert (await h.api.settle_run()).ok
            await h.wait_phase("DONE")
            rdir = home / "cw" / "reports" / D
            assert len([p for p in rdir.iterdir() if not p.name.endswith(".tmp")]) == 12
            await eventually(lambda: (home / "cw" / "backup" / D / "clearing.db").exists(), timeout=10)
        finally:
            await h.close()


# --------------------------------------------------------------- copies, upkeep
class TestUpkeep:
    async def test_copy_repairs_pull_trades_and_cash(self, tmp_path: Path, bus_config: FullConfig) -> None:
        h = Harness(bus_config, tmp_path)
        try:
            await h.start_stubs()
            await h.start_clearing()
            await h.seed()
            trade, cash = h.scn.trades[0], h.scn.cash[0]
            await h.tg.add_trade(trade, publish=False)  # published before the purged replay window
            shard = h.shard_of(cash.account_id)
            shard.cash.append(cash)
            done = asyncio.Event()
            h.svc.copies._on_expired(("trade-gateway", f"td.exec.{trade.account_id}"), 1, 9, done.set)
            await asyncio.wait_for(done.wait(), 10)
            assert rows(h, "SELECT trade_id FROM trades") == [(trade.trade_id,)]
            done2 = asyncio.Event()
            h.svc.copies._on_stream_change((shard.instance, f"risk.cash.{cash.account_id}"), "old", "new", 1, done2.set)
            await asyncio.wait_for(done2.wait(), 10)
            assert rows(h, "SELECT movement_id, src_instance FROM cash_movements") == [(cash.movement_id, shard.instance)]
            assert h.svc.copies.repaired_rows == 2
        finally:
            await h.close()

    async def test_maintenance_during_a_run(self, tmp_path: Path, bus_config: FullConfig, pool: ProcessPoolExecutor) -> None:
        h = Harness(bus_config, tmp_path, executor=pool, render_reports=False)
        try:
            await ready_day(h)
            assert (await h.api.settle_run()).ok
            await h.wait_phase("DONE")
            h.md.gate = asyncio.Event()
            assert (await h.api.settle_run(force=True)).open_as(SettleRunAck).version == 2
            await eventually(lambda: h.md.calls >= 2)  # v2 waits in step 4
            rep = await h.svc.maintenance.run(D)
            assert not rep.errors and rep.backup is not None
            con = sqlite3.connect(rep.backup)
            try:
                assert con.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
                assert con.execute("SELECT phase FROM eod_runs WHERE version=2").fetchall() == [("TRADES_RECONCILED",)]
            finally:
                con.close()
            h.md.gate.set()
            st = await h.wait_phase("DONE")
            assert st.version == 2
        finally:
            await h.close()

    async def test_push_that_does_not_complete_warns(self, tmp_path: Path, bus_config: FullConfig, pool: ProcessPoolExecutor) -> None:
        h = Harness(bus_config, tmp_path, executor=pool, render_reports=False)
        try:
            await ready_day(h)
            h.re1.fail_rollovers = 1 + FAST.rollover_retries  # every attempt of the first push fails
            assert (await h.api.settle_run()).ok
            await h.wait_phase("DONE")  # re-pushed every repush_interval_s until confirmed
            w = warnings_of(await events(h), "PUSH_INCOMPLETE")
            assert len(w) == 1 and w[0].phase.value in ("SETTLED", "ROLLED") and w[0].detail["shards"] == ["risk-engine-1"]
            log = step_log(h)
            assert log["ROLLED"]["shards"] == ["risk-engine-0", "risk-engine-1"]
            assert [x["kind"] for x in log["WARNINGS"]] == ["PUSH_INCOMPLETE"]  # kept through ROLLED .. DONE
        finally:
            await h.close()


# ------------------------------------------------------------------ L7 the pool
class _BreakSettle(Executor):
    """The first ``breaks`` settle jobs fail as if their worker process died."""

    def __init__(self, inner: Executor, breaks: int) -> None:
        self.inner = inner
        self.breaks = breaks

    def submit(self, fn: Callable[..., Any], /, *args: Any, **kwargs: Any) -> Future[Any]:
        if fn is jobs.settle_job and self.breaks > 0:
            self.breaks -= 1
            fut: Future[Any] = Future()
            fut.set_exception(BrokenProcessPool("a worker died"))
            return fut
        return self.inner.submit(fn, *args, **kwargs)


class TestPool:
    async def test_settlement_is_retried_once_when_a_worker_died(
        self, tmp_path: Path, bus_config: FullConfig, pool: ProcessPoolExecutor
    ) -> None:
        ex = _BreakSettle(pool, 1)
        h = Harness(bus_config, tmp_path, executor=ex, render_reports=False)
        try:
            await ready_day(h)
            assert (await h.api.settle_run()).ok
            await h.wait_phase("DONE")
            ex.breaks = 2
            assert (await h.api.settle_run(force=True)).ok
            st = await h.wait_phase("FAILED")
            assert st.version == 2 and "died twice" in (st.error or "")
        finally:
            await h.close()


# ------------------------------------------------------------------ L6 backtests
class TestBacktestContract:
    async def test_seed_type_and_report_outside_out_dir(self, tmp_path: Path, bus_config: FullConfig, pool: ProcessPoolExecutor) -> None:
        settings = dataclasses.replace(FAST, backtest_runner="fake_backtest:escaping_job")
        h = Harness(bus_config, tmp_path, executor=pool, settings=settings)
        try:
            await h.start_clearing()
            for bad in ("7", True, 1.5):
                r = await h.api.call("backtest.run", BacktestRunRequest(command_id=uuid7(), config_name="x",
                                                                        overrides={"seed": bad}, actor="ops"))
                assert r.code == codes.BAD_REQUEST, bad
            r = await h.api.call("backtest.run", BacktestRunRequest(command_id=uuid7(), config_name="x",
                                                                    overrides={"seed": 3}, actor="ops"))
            run_id = r.open_as(BacktestStatus).run_id

            async def finished() -> bool:
                st = (await h.api.call("backtest.status", BacktestStatusRequest(run_id=run_id))).open_as(BacktestStatus)
                return st.state in (BacktestState.DONE, BacktestState.FAILED)

            await eventually(finished, timeout=20)
            st = (await h.api.call("backtest.status", BacktestStatusRequest(run_id=run_id))).open_as(BacktestStatus)
            assert st.state == BacktestState.DONE and st.report_id is None and st.summary is None
            assert rows(h, "SELECT seed, git_sha FROM backtest_runs WHERE run_id=?", run_id) == [(3, "cafe")]
            assert rows(h, "SELECT count(*) FROM reports") == [(0,)]
        finally:
            await h.close()
