"""Unit tests of the clearing-worker building blocks (no bus): schema, store round trip, digests, prices,
scheduler, pushes, report rendering, statements and maintenance."""

from __future__ import annotations

import asyncio
import json
import sqlite3
from decimal import Decimal
from pathlib import Path
from typing import Any

import msgspec
import pytest
from clearing_support import (
    NEXT,
    D,
    Scenario,
    sample_alerts,
    sample_md_events,
    sample_metrics,
    sample_orders,
    ts,
)
from sqlalchemy import inspect as sa_inspect

from futrisk.common.config import FullConfig
from futrisk.core import DaySettlement, determine_settlement_price, settle_day
from futrisk.db import Database, Outbox, ProcessedCommands
from futrisk.messages import codes
from futrisk.messages.enums import (
    AccountState,
    CheckStatus,
    DayRollMode,
    MdQuality,
    PosDirection,
    ReconCheck,
    SettlePriceSource,
)
from futrisk.messages.envelope import Reply, err_reply, ok_reply
from futrisk.messages.eod import DayRoll, EodFreezeAck, SodSnapshot
from futrisk.messages.orders import TradeDigest, compute_account_digest
from futrisk.messages.risk import AccountFunds, AccountMetricsView, AccountSnapshot, PositionView
from futrisk.services.clearing import digests, schema, store
from futrisk.services.clearing.maintenance import Maintenance, prune_backups, vacuum_into, wal_checkpoint
from futrisk.services.clearing.prices import decide_prices, price_changes
from futrisk.services.clearing.pushes import PushManager, PushOutcome
from futrisk.services.clearing.reports import SECTION_TITLES, STATEMENT_HEADINGS
from futrisk.services.clearing.reports.model import ReportBundle, key_sentences, utilization
from futrisk.services.clearing.reports.render import render_all, render_daily_html
from futrisk.services.clearing.reports.statement import render_statement_txt, width
from futrisk.services.clearing.saga import intraday_checks
from futrisk.services.clearing.scheduler import due_trading_day
from futrisk.services.clearing.settings import ClearingSettings
from futrisk.services.clearing.store import OverrideRow


# ------------------------------------------------------------------ fixtures
def settle_scenario(cfg: FullConfig) -> tuple[Scenario, SodSnapshot, DaySettlement]:
    scn = Scenario(cfg)
    req = scn.seed_request()
    sod = SodSnapshot(trading_day=D, version=0, accounts=req.accounts, pre_settle=req.pre_settle)
    inputs = scn.settle_inputs()
    views = {i: determine_settlement_price(inp, scn.registry, version=1) for i, inp in inputs.items()}
    ds = settle_day(
        trading_day=D,
        version=1,
        sod=sod,
        trades=scn.trades,
        cash_movements=scn.cash,
        settle_prices=views,
        registry=scn.registry,
        next_trading_day=NEXT,
        sod_version=1,
    )
    return scn, sod, ds


def open_db(tmp: Path) -> Database:
    db = Database(tmp / "clearing.db", schema.METADATA, synchronous="FULL", migrations=schema.MIGRATIONS)
    db.open()
    return db


def bundle_of(cfg: FullConfig, scn: Scenario, sod: SodSnapshot, ds: DaySettlement, **extra: Any) -> dict[str, Any]:
    b: dict[str, Any] = {
        "trading_day": D,
        "version": 1,
        "next_trading_day": NEXT,
        "generated_ns": ts("2026-09-28 15:40:00"),
        "config_hash": cfg.config_hash,
        "config_version": 3,
        "triggered_by": "scheduler",
        "phases": {"BEGIN": ts("2026-09-28 15:30:00"), "SETTLED": ts("2026-09-28 15:30:05")},
        "night_from": None,
        "open_ns": ts("2026-09-28 08:55:00"),
        "close_ns": ts("2026-09-28 15:15:00"),
        "settlement": msgspec.to_builtins(ds.result),
        "trades": msgspec.to_builtins(scn.trades),
        "sod": msgspec.to_builtins(sod),
        "next_sod": msgspec.to_builtins(ds.next_sod),
        "accounts": {a: {"name": s.name, "shard": s.shard} for a, s in cfg.accounts.accounts.items()},
        "instruments": {
            i.instrument_id: {
                "product": i.product,
                "name": cfg.products.products[i.product].name,
                "sector": cfg.products.products[i.product].sector,
                "exchange": cfg.products.products[i.product].exchange.value,
                "multiplier": cfg.products.products[i.product].multiplier,
                "limit_pct": str(cfg.products.products[i.product].limit_pct),
            }
            for i in cfg.products.instruments
        },
        "rules": [r.model_dump(mode="json") for r in cfg.risk_rules.rules],
        "history": {"ACC1": [{"trading_day": "20260924", "nav": "1.0", "max_drawdown": "0"},
                             {"trading_day": D, "nav": "1.0001", "max_drawdown": "0"}]},
        "alerts": {"items": [], "error": None},
        "orders": {"items": [], "error": None},
        "metrics": {"series": {}, "error": None},
        "md_events": {"items": [], "error": None},
        "svc_history": {"items": [], "error": None},
    }
    b.update(extra)
    return b


# ---------------------------------------------------------------------- schema
class TestSchema:
    def test_frozen_ddl_matches_metadata(self) -> None:
        assert schema.schema_ddl() == schema.V1_SQL
        assert schema.schema_ddl(2) == schema.V2_SQL  # v2: config_history

    def test_tables_and_keys_follow_03_4_4(self, tmp_path: Path) -> None:
        db = open_db(tmp_path)
        try:
            con = sqlite3.connect(db.path)
            tables = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            expected = {
                "trades", "cash_movements", "eod_runs", "official_prices", "price_overrides", "settlement_prices",
                "account_settlements", "position_settlements", "margin_groups", "trade_settlements", "close_details",
                "sod_versions", "sod_accounts", "sod_lots", "sod_prices", "recon_checks", "reports", "backtest_runs",
                "schema_version", "db_meta", "outbox", "outbox_topics", "processed_commands", "fact_cursors",
                "config_history",
            }
            assert expected <= tables
            pk = {r[1] for r in con.execute("PRAGMA table_info(position_settlements)") if r[5]}
            assert pk == {"trading_day", "account_id", "instrument_id", "direction", "version"}
            cols = [r[1] for r in con.execute("PRAGMA table_info(account_settlements)")]
            for c in ("exch_formula_pnl", "fifo_realized_pnl", "fifo_float_pnl", "hwm_equity", "nav", "max_drawdown", "checks_passed"):
                assert c in cols
            assert "pushed_ns" in [r[1] for r in con.execute("PRAGMA table_info(sod_versions)")]
            idx = {r[1] for r in con.execute("SELECT * FROM sqlite_master WHERE type='index'")}
            assert {"ix_trades__td_acct", "ix_recon_checks__run", "ix_reports__td_type", "ix_price_overrides__td_inst"} <= idx
            with pytest.raises(sqlite3.IntegrityError):  # eod_qty = sod + open - close
                con.execute(
                    "INSERT INTO position_settlements VALUES ('20260928','A','rb2701','LONG',1,'rb','SHFE',1,1,0,0,0,5,"
                    "'1','1',10,'0.1','0','0','0','0',NULL)"
                )
            con.close()
            assert db.info.schema_version == 2 and sa_inspect is not None
        finally:
            db.close()

    def test_store_round_trip_of_a_settlement(self, tmp_path: Path, default_config: FullConfig) -> None:
        scn, sod, ds = settle_scenario(default_config)
        db = open_db(tmp_path)
        try:
            def tx(c: Any) -> None:
                store.save_sod(c, sod, source=schema.SodSource.SEED, created_ns=1, reason="seed", actor="t")
                store.save_settlement(c, ds.result, created_ns=2)
                store.insert_checks(c, D, 1, ds.checks, created_ns=2)
                store.save_sod(c, ds.next_sod, source=schema.SodSource.EOD, created_ns=2)

            db.write(tx)
            got = db.read(lambda c: store.load_settlement(c, D, 1))
            assert got is not None
            assert got.accounts == ds.result.accounts and got.positions == ds.result.positions
            assert sorted(got.trades, key=lambda t: t.trade_id) == sorted(ds.result.trades, key=lambda t: t.trade_id)
            assert got.prices == ds.result.prices and len(got.checks) == len(ds.checks)
            nxt = db.read(lambda c: store.load_sod(c, NEXT, 1))
            assert nxt == msgspec.structs.replace(ds.next_sod, accounts=sorted(ds.next_sod.accounts, key=lambda a: a.account_id))
            db.write(lambda c: store.save_settlement(c, ds.result, created_ns=3))  # re-running a version is idempotent
            assert db.read(lambda c: store.load_settlement(c, D, 1)) == got
            one = db.read(lambda c: store.load_settlement(c, D, 1, account_id="ACC1"))
            assert one is not None and [a.account_id for a in one.accounts] == ["ACC1"]
            assert {p.account_id for p in one.positions} == {"ACC1"}
        finally:
            db.close()


# --------------------------------------------------------------------- digests
class TestDigests:
    def test_three_way_and_cash(self, default_config: FullConfig) -> None:
        scn = Scenario(default_config)
        trades = scn.trades
        local = digests.local_trade_digests(trades)
        td = TradeDigest(trading_day=D, accounts=list(local.values()), total_count=len(trades), generated_ns=0)
        acks = {
            "risk-engine-0": EodFreezeAck(trading_day=D, shard=0, accounts=[local["ACC1"]], cash=[], frozen_ns=0),
            "risk-engine-1": EodFreezeAck(
                trading_day=D,
                shard=1,
                accounts=[local["ACC3"], local["ACC4"], local["ACC5"]],
                cash=list(digests.cash_digests(scn.cash).values()),
                open_orders=2,
                frozen_ns=0,
            ),
        }
        ok = digests.compare(accounts=["ACC2"], local_trades=local, td_digest=td, acks=acks, local_cash=digests.cash_digests(scn.cash))
        assert ok.ok and {c.account_id for c in ok.checks} >= {"ACC1", "ACC2", "ACC3"}
        assert digests.open_order_checks(acks)[0].status == CheckStatus.WARN
        # local copy misses ACC3's trade; risk lags on ACC4; cash amount formatting differs but equals
        partial = digests.local_trade_digests([t for t in trades if t.account_id != "ACC3"])
        lag = dict(acks)
        lag["risk-engine-1"] = msgspec.structs.replace(
            acks["risk-engine-1"],
            accounts=[local["ACC3"], compute_account_digest("ACC4", []), local["ACC5"]],
            cash=[msgspec.structs.replace(acks["risk-engine-1"].cash[0], amount_sum="70000")],  # net of +100000 / -30000
        )
        bad = digests.compare(accounts=[], local_trades=partial, td_digest=td, acks=lag, local_cash=digests.cash_digests(scn.cash))
        assert bad.trade_mismatch == {"ACC3", "ACC4"} and bad.local_missing == {"ACC3"} and not bad.cash_mismatch
        cash_bad = digests.compare(accounts=[], local_trades=local, td_digest=td, acks=acks, local_cash={})
        assert cash_bad.cash_mismatch == {"ACC3"}
        fail = next(c for c in cash_bad.checks if c.check == ReconCheck.CASH_DIGEST and c.account_id == "ACC3")
        fd = fail.detail or {}
        assert fail.status == CheckStatus.FAIL and fd["expected"] == "70000.00" and fd["actual"] == "0.00"


# ---------------------------------------------------------------------- prices
class TestPrices:
    def test_priority_and_fallbacks(self, default_config: FullConfig) -> None:
        scn = Scenario(default_config)
        inputs = scn.settle_inputs()
        ov = {"rb2701": OverrideRow("o1", D, "rb2701", "3121", "notice", "rm", 1)}
        dec = decide_prices(
            registry=scn.registry,
            trading_day=D,
            version=3,
            needed={"rb2701", "rb2705", "IF2612", "hc2701", "cu2611"},
            inputs={k: v for k, v in inputs.items() if k != "rb2705"},  # md has no input for rb2705
            overrides=ov,
            official={"IF2612": "4450.0"},
            sod_pre={"rb2705": "3140", "hc2701": "3300"},
        )
        src = {k: (v.source, v.price, v.version) for k, v in dec.views.items()}
        assert src["rb2701"] == (SettlePriceSource.MANUAL, "3121", 3)
        assert src["IF2612"][0] == SettlePriceSource.OFFICIAL and src["IF2612"][1] == "4450"
        assert src["rb2705"] == (SettlePriceSource.PRE_SETTLE, "3140", 3)  # missing input: exchange fallback chain
        assert src["hc2701"][0] == SettlePriceSource.PRE_SETTLE
        assert src["cu2611"][0] == SettlePriceSource.FALLBACK_MEDIAN and not dec.missing
        assert dec.views["rb2701"].operator == "rm" and dec.views["rb2701"].reason == "notice"
        missing = decide_prices(registry=scn.registry, trading_day=D, version=1, needed={"j2701"}, inputs={},
                                overrides={}, official={}, sod_pre={})
        assert missing.missing == ["j2701"]
        unknown = decide_prices(registry=scn.registry, trading_day=D, version=1, needed=set(), inputs={},
                                overrides={}, official={"zz9999": "1"}, sod_pre={})
        assert "zz9999" in unknown.skipped and not unknown.views
        assert price_changes({"a": "3118.0", "b": "5"}, {"a": "3118", "b": "6"}) == {"b": ("6", "5")}


# ------------------------------------------------------------------- scheduler
class TestScheduler:
    def test_due_trading_day(self, default_config: FullConfig) -> None:
        assert due_trading_day(default_config, ts("2026-09-28 15:29:59")) == "20260924"
        assert due_trading_day(default_config, ts("2026-09-28 15:30:00")) == D
        assert due_trading_day(default_config, ts("2026-09-28 21:30:00")) == D
        assert due_trading_day(default_config, ts("2026-09-26 12:00:00")) == "20260924"  # Saturday
        assert due_trading_day(default_config, ts("2026-10-03 10:00:00")) == "20260930"  # National Day holiday


# ----------------------------------------------------------------------- pushes
class _FakeRt:
    def __init__(self) -> None:
        self.replies: dict[str, list[Reply]] = {}
        self.calls: list[tuple[str, int]] = []
        self.days: list[tuple[str, str]] = []
        self.gates: dict[str, asyncio.Event] = {}
        from futrisk.common.clock import MarketClock

        self.market_clock = MarketClock()
        self.fatals: list[BaseException] = []

    async def request(self, target: str, command: str, payload: Any, **_: Any) -> Reply:
        self.calls.append((target, payload.sod_version))
        self.days.append((target, payload.next_trading_day))
        gate = self.gates.get(target)
        if gate is not None:
            await gate.wait()
        queue = self.replies.get(target) or []
        if queue:
            return queue.pop(0)
        return ok_reply(DayRoll(shard=0, mode=DayRollMode.ROLL, from_trading_day=payload.trading_day,
                                to_trading_day=payload.next_trading_day, sod_version=payload.sod_version, accounts=[],
                                ts_ns=0))

    def fatal(self, exc: BaseException, where: str) -> None:
        self.fatals.append(exc)


def status(pm: PushManager) -> dict[str, Any]:
    st = pm.status()
    assert st is not None
    return st


class TestPushManager:
    async def test_all_confirm_then_finalize_supersede_and_repush(self) -> None:
        rt = _FakeRt()
        pm = PushManager(rt, ["s0", "s1"], ClearingSettings(repush_interval_s=3600))  # type: ignore[arg-type]
        finalized: list[tuple[int, list[str]]] = []
        stuck: list[list[str]] = []

        async def fin(ns: int, shards: list[str]) -> None:
            finalized.append((ns, shards))

        rt.replies["s1"] = [err_reply(codes.UNAVAILABLE, "down")]
        task = asyncio.create_task(
            pm.push(D, NEXT, 1, source="EOD", finalize=fin, on_stuck=lambda t: stuck.append(t.pending))
        )
        await asyncio.sleep(0.05)
        assert not task.done() and status(pm)["pending"] == ["s1"] and not finalized
        assert stuck == [["s1"]]  # L3: the first failure after the retries is reported once
        pm.on_peer_change("s1", "DOWN")
        pm.on_peer_change("s1", "UP")  # heartbeat back: re-push
        assert await asyncio.wait_for(task, 2) == PushOutcome.PUSHED
        assert finalized and finalized[0][1] == ["s0", "s1"] and len(finalized) == 1
        assert rt.calls.count(("s1", 1)) == 2 and stuck == [["s1"]]
        # a newer version of the same day supersedes a pending one: its waiters finish when the newer is pushed
        rt.gates["s0"] = asyncio.Event()
        old = asyncio.create_task(pm.push(D, NEXT, 2, source="EOD", finalize=fin))
        await asyncio.sleep(0.05)
        new = asyncio.create_task(pm.push(D, NEXT, 3, source="SEED", finalize=fin))
        await asyncio.sleep(0.05)
        older = asyncio.create_task(pm.push(D, NEXT, 2, source="EOD", finalize=fin))
        await asyncio.sleep(0.05)
        assert not old.done() and not older.done()  # NEXT is not pushed yet
        rt.gates["s0"].set()
        assert await asyncio.wait_for(new, 2) == PushOutcome.PUSHED
        assert await asyncio.wait_for(old, 2) == PushOutcome.SUPERSEDED
        assert await asyncio.wait_for(older, 2) == PushOutcome.SUPERSEDED
        # PROVISIONAL does not confirm; CATCH_UP does (a shard on a later day records a skipped day's SOD)
        rt.replies["s0"] = [ok_reply(DayRoll(shard=0, mode=DayRollMode.PROVISIONAL, from_trading_day=D, to_trading_day=NEXT,
                                             sod_version=4, accounts=[], ts_ns=0))]
        rt.replies["s1"] = [ok_reply(DayRoll(shard=1, mode=DayRollMode.CATCH_UP, from_trading_day=D, to_trading_day=NEXT,
                                             sod_version=4, accounts=[], ts_ns=0))]
        t4 = asyncio.create_task(pm.push(D, NEXT, 4, source="SEED", finalize=fin))
        await asyncio.sleep(0.05)
        assert "s0" in status(pm)["errors"] and status(pm)["modes"] == {"s1": "CATCH_UP"}
        pm.on_peer_change("s0", "UP")
        assert await asyncio.wait_for(t4, 2) == PushOutcome.PUSHED
        await pm.stop()
        assert not rt.fatals

    async def test_days_are_pushed_in_order_never_superseded_across_days(self) -> None:
        rt = _FakeRt()
        pm = PushManager(rt, ["s0"], ClearingSettings(repush_interval_s=3600))  # type: ignore[arg-type]
        done: list[str] = []

        def fin(day: str) -> Any:
            async def f(ns: int, shards: list[str]) -> None:
                done.append(day)

            return f

        rt.gates["s0"] = asyncio.Event()
        first = asyncio.create_task(pm.push(D, NEXT, 1, source="EOD", finalize=fin(NEXT)))
        await asyncio.sleep(0.05)
        later = asyncio.create_task(pm.push(NEXT, "20260930", 1, source="EOD", finalize=fin("20260930")))
        await asyncio.sleep(0.05)
        assert rt.days == [("s0", NEXT)] and status(pm)["queued"] == ["20260930:v1"]  # the later day waits
        rt.gates["s0"].set()
        assert await asyncio.wait_for(first, 2) == PushOutcome.PUSHED  # not superseded by the later day
        assert await asyncio.wait_for(later, 2) == PushOutcome.PUSHED
        assert done == [NEXT, "20260930"] and rt.days == [("s0", NEXT), ("s0", "20260930")]
        # an earlier day queued while a later one is in flight: the later push is paused, then resumed
        rt.gates["s0"] = asyncio.Event()
        rt.days.clear()
        later = asyncio.create_task(pm.push("20260930", "20261008", 1, source="EOD", finalize=fin("20261008")))
        await asyncio.sleep(0.05)
        earlier = asyncio.create_task(pm.push(NEXT, "20260930", 2, source="SEED", finalize=fin("20260930")))
        await asyncio.sleep(0.05)
        assert status(pm)["next_trading_day"] == "20260930" and status(pm)["queued"] == ["20261008:v1"]
        rt.gates["s0"].set()
        assert await asyncio.wait_for(earlier, 2) == PushOutcome.PUSHED
        assert await asyncio.wait_for(later, 2) == PushOutcome.PUSHED
        assert rt.days == [("s0", "20261008"), ("s0", "20260930"), ("s0", "20261008")]
        assert done[-2:] == ["20260930", "20261008"]
        await pm.stop()


# ---------------------------------------------------------------------- reports
class TestReports:
    def test_html_sections_notes_and_sentences(self, default_config: FullConfig) -> None:
        scn, sod, ds = settle_scenario(default_config)
        b = bundle_of(
            default_config,
            scn,
            sod,
            ds,
            alerts={"items": msgspec.to_builtins(sample_alerts()), "error": None},
            orders={"items": msgspec.to_builtins(sample_orders(3)), "error": None},
            metrics={"series": {"ACC3": msgspec.to_builtins(sample_metrics())}, "error": None},
            md_events={"items": msgspec.to_builtins(sample_md_events()), "error": None},
            svc_history={"items": [], "error": "supervisor svc.history: UNAVAILABLE"},
        )
        html = render_daily_html(ReportBundle.from_dict(b), with_charts=False)
        for title in SECTION_TITLES:
            assert f">{SECTION_TITLES.index(title) + 1} {title}<" in html
        assert "ACC3 10:32:15 于 −3.02% 熔断，拒绝 3 笔开仓" in html
        assert "服务事件不可用：supervisor svc.history: UNAVAILABLE" in html
        assert "无夜盘" in html and "FALLBACK_MEDIAN" in html
        rb = ReportBundle.from_dict(bundle_of(default_config, scn, sod, ds))
        from futrisk.services.clearing.reports import model

        funds, _ = model.funds_rows(rb)
        sentences = key_sentences(rb, funds, model.exposures(rb))
        assert any("当日盈亏最高" in s for s in sentences)

    def test_render_all_files(self, tmp_path: Path, default_config: FullConfig) -> None:
        scn, sod, ds = settle_scenario(default_config)
        files = render_all(bundle_of(default_config, scn, sod, ds), str(tmp_path))
        kinds = sorted({f["type"] for f in files})
        assert kinds == ["RISK_DAILY_HTML", "SETTLEMENT_JSON", "STATEMENT_CSV", "STATEMENT_TXT"]
        for f in files:
            p = tmp_path / f["path"]
            assert p.exists() and p.stat().st_size == f["size_bytes"] and f["path"].startswith(f"reports/{D}/")
        doc = json.loads((tmp_path / "reports" / D / "settlement.json").read_text(encoding="utf-8"))
        assert doc["trading_day"] == D and "close_details" in doc and "matched_today_qty" in doc["trades"][0]
        csv_bytes = (tmp_path / "reports" / D / "statement_ACC5.csv").read_bytes()
        assert csv_bytes.startswith(b"\xef\xbb\xbf") and "持仓汇总".encode() in csv_bytes

    def test_statement_text(self, default_config: FullConfig) -> None:
        scn, sod, ds = settle_scenario(default_config)
        text = render_statement_txt(ReportBundle.from_dict(bundle_of(default_config, scn, sod, ds)), "ACC5")
        for h in STATEMENT_HEADINGS:
            assert h in text
        tables: list[list[str]] = []
        cur: list[str] = []
        for ln in [*text.splitlines(), ""]:
            if ln.startswith(("|", "+")):
                cur.append(ln)
            elif cur:
                tables.append(cur)
                cur = []
        assert len(tables) == 4  # 成交记录, 平仓明细, 持仓明细, 持仓汇总
        for t in tables:  # CJK-aware alignment: every line of a table has the same display width
            assert len({width(ln) for ln in t}) == 1, t
        assert "rb2705" in text and "BIG_SIDE" in text  # spread: single-side margin

    def test_hard_level_and_metric_baseline(self) -> None:
        from futrisk.services.clearing.reports.model import hard_level_of

        daily_loss = {"levels": [{"name": "warn", "threshold": -0.02, "action": "ALERT"},
                                 {"name": "restrict", "threshold": -0.025, "action": "RESTRICT"},
                                 {"name": "halt", "threshold": -0.03, "action": "CIRCUIT_BREAK"},
                                 {"name": "derisk", "threshold": -0.05, "action": "LIQUIDATE"}],
                      "overrides": {"ACC2": {"halt": {"threshold": -0.04}}}}
        assert hard_level_of(daily_loss, "ACC1")["name"] == "halt"  # the circuit break, not the last level
        assert hard_level_of(daily_loss, "ACC2")["threshold"] == -0.04  # overrides applied
        nav = {"levels": [{"name": "warn", "threshold": 0.9, "action": "ALERT"},
                          {"name": "derisk", "threshold": 0.85, "action": "LIQUIDATE"}]}
        assert hard_level_of(nav, "ACC1")["name"] == "derisk"  # no CIRCUIT_BREAK / RESTRICT: the last level
        # NAV is measured from 1: (1 - 0.95) / (1 - 0.85) = 1/3, the dashboard's number (not 0.85 / 0.95)
        u = utilization(Decimal("0.95"), 0.85, "<=", "account.nav")
        assert u is not None and abs(u - Decimal(1) / Decimal(3)) < Decimal("1e-9")

    def test_utilization(self) -> None:
        assert utilization(Decimal("-0.03"), -0.02, "<=") == Decimal("1.5")
        assert utilization(Decimal("0.01"), -0.02, "<=") == Decimal(0)
        assert utilization(Decimal("0.9"), 0.9, "<=") == Decimal(1)
        assert utilization(Decimal("0.2"), 0.4, ">=") == Decimal("0.5")
        assert utilization(None, 1.0, ">=") is None


# ------------------------------------------------------------- intraday check
def test_intraday_vs_settle(default_config: FullConfig) -> None:
    scn, sod, ds = settle_scenario(default_config)
    acc1 = ds.accounts["ACC1"]
    zero_metrics = AccountMetricsView(
        daily_pnl=0, daily_pnl_pct_init=0, daily_pnl_pct_sod=0, floating_pnl_pct_init=0, perf_equity=0, nav=1,
        intraday_peak_equity=0, intraday_dd=0, hwm_equity=0, hwm_dd=0, gross_notional=0, net_notional=0,
        leverage=0, effective_n=0, limit_stress=0, orders_today=0, cancels_today=0,
    )

    def snap(equity: float, last: float) -> AccountSnapshot:
        funds = AccountFunds(initial_capital=1e7, cum_net_deposit=0, pre_balance=1e7, net_deposit=0, static_equity=1e7,
                             close_pnl=0, position_pnl=0, fee=float(acc1.fee), equity=equity, margin=0, frozen_margin=0,
                             frozen_fee=0, available=0, risk_degree=0)
        pos = PositionView(account_id="ACC1", instrument_id="rb2701", product="rb", exchange=scn.registry.exchange("rb2701"),
                           direction=PosDirection.LONG, td_qty=2, yd_qty=6, closable_qty=8, closable_td_qty=2, closable_yd_qty=6,
                           avg_open_price=3106.25, basis_avg_price=3112.0, last=last)
        return AccountSnapshot(account_id="ACC1", shard=0, trading_day=D, version=1, state_version=1, as_of_ns=0,
                               quality=MdQuality.OK, state=AccountState.NORMAL, funds=funds, metrics=zero_metrics,
                               positions=[pos])

    eq = float(acc1.equity)
    # last 3120 vs settle 3118: 8 lots x 10 x 2 = 160 explained
    good = intraday_checks([snap(eq + 160.0, 3120.0)], ds, scn.registry)
    assert good[0].status == CheckStatus.PASS and good[0].check == ReconCheck.INTRADAY_VS_SETTLE
    bad = intraday_checks([snap(eq + 5_000.0, 3120.0)], ds, scn.registry)
    assert bad[0].status == CheckStatus.WARN and (bad[0].detail or {})["explained"] == "160.00"


# ------------------------------------------------------------------ maintenance
class TestMaintenance:
    def test_vacuum_checkpoint_prune(self, tmp_path: Path) -> None:
        src = tmp_path / "x.db"
        con = sqlite3.connect(src)
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("CREATE TABLE t (a INTEGER)")
        con.execute("INSERT INTO t VALUES (1)")
        con.commit()
        con.close()
        assert wal_checkpoint(src)[0] == 0
        backup = tmp_path / "backup"
        for day in ("20260901", "20260902", "20260903"):
            (backup / day).mkdir(parents=True)
            vacuum_into(src, backup / day / "clearing.db")
        (backup / "20260901" / "md.db").write_bytes(b"other service")
        target = vacuum_into(src, backup / "20260903" / "clearing.db")  # same day again: overwritten
        assert sqlite3.connect(target).execute("SELECT a FROM t").fetchall() == [(1,)]
        assert prune_backups(backup, keep=1) == ["20260901", "20260902"]
        assert (backup / "20260901" / "md.db").exists() and not (backup / "20260902").exists()
        assert (backup / "20260903" / "clearing.db").exists()

    async def test_run_purges(self, tmp_path: Path, default_config: FullConfig) -> None:
        db = open_db(tmp_path / "db")
        try:
            outbox = Outbox(db, instance="clearing-worker")
            processed = ProcessedCommands(db)
            old = ts("2026-09-10 16:00:00")

            def tx(c: Any) -> None:
                for i, td in enumerate(("20260922", "20260924", D)):
                    processed.record(c, command_id=f"c{i}", command="settle.run", requester="api-gateway",
                                     request_hash="0" * 64, reply=ok_reply(), trading_day=td, received_ns=old)
                outbox.append(c, topic="sys.eod", type="EodPhaseEvent", v=1, payload=b"\x80", td=D, epoch="e", created_ns=old)

            db.write(tx)
            db.write(lambda c: outbox.mark_published(c, [1], old))
            m = Maintenance(db, outbox, ClearingSettings(purge_batch=1), backup_dir=tmp_path / "backup",
                            calendar=default_config.calendar, wall_ns=lambda: ts("2026-09-28 16:00:00"))
            rep = await m.run(D)
            assert not rep.errors and rep.purged_commands == 1 and rep.purged_outbox == 1
            assert (tmp_path / "backup" / D / "clearing.db").exists()
            left = db.read(lambda c: [r[0] for r in c.exec_driver_sql("SELECT command_id FROM processed_commands")])
            assert sorted(left) == ["c1", "c2"]  # the previous and the current trading day are kept
            assert outbox.last_seq("sys.eod") == 1  # sequence numbers never go back
        finally:
            db.close()


def test_err_reply_codes() -> None:
    assert err_reply(codes.NOT_FOUND).code == "NOT_FOUND"


def test_limit_peaks_from_rule_util(default_config: FullConfig) -> None:
    """``MetricPoint.rule_util`` (optional field) feeds the "盘中峰值" column; breaches come from ALERT_RAISED."""
    from futrisk.services.clearing.reports import model

    scn, sod, ds = settle_scenario(default_config)
    pts = [msgspec.structs.replace(p, rule_util={"daily_loss/acct:ACC3": 0.4 + i / 10}) for i, p in enumerate(sample_metrics())]
    b = ReportBundle.from_dict(
        bundle_of(
            default_config,
            scn,
            sod,
            ds,
            metrics={"series": {"ACC3": msgspec.to_builtins(pts)}, "error": None},
            alerts={"items": msgspec.to_builtins(sample_alerts()), "error": None},
        )
    )
    funds, _ = model.funds_rows(b)
    rows = {(r.account_id, r.rule_id): r for r in model.limit_rows(b, funds, model.exposures(b))}
    loss = rows[("ACC3", "daily_loss")]
    assert loss.peak_util == Decimal("1.5") and loss.breaches == 2 and loss.peak_value == Decimal("-0.033")
    assert ("ACC1", "compliance") not in rows  # nothing to evaluate: not listed
    assert rows[("ACC1", "risk_degree")].eod_value == Decimal(next(a.risk_degree for a in ds.result.accounts if a.account_id == "ACC1"))


def test_cash_digest_signed_net_matches_engine_ack() -> None:
    """``amount_sum`` = deposits − withdrawals, the same as EngineCore's EodFreezeAck (futrisk.core.eod)."""
    from futrisk.core.eod import account_digests
    from futrisk.core.eod import cash_digests as engine_cash_digests
    from futrisk.messages.enums import CashType
    from futrisk.messages.risk import CashDigest, CashMovement

    dep = CashMovement(movement_id="m1", account_id="ACC3", trading_day=D, type=CashType.DEPOSIT, amount="100000",
                       actor="ops", reason="in", ts_ns=1)
    wd = CashMovement(movement_id="m2", account_id="ACC3", trading_day=D, type=CashType.WITHDRAW, amount="30000.00",
                      actor="ops", reason="out", ts_ns=2)
    local = digests.cash_digests([dep, wd])
    assert local == {"ACC3": CashDigest(account_id="ACC3", count=2, amount_sum="70000.00")}
    accounts = ["ACC3", "ACC4", "ACC5"]
    engine = engine_cash_digests({"ACC3": [dep, wd]}, accounts)  # also lists the flat accounts (count 0)
    ack = EodFreezeAck(trading_day=D, shard=1, accounts=account_digests({}, accounts), cash=engine, frozen_ns=0)
    empty = TradeDigest(trading_day=D, accounts=[], total_count=0, generated_ns=0)
    ok = digests.compare(accounts=accounts, local_trades={}, td_digest=empty, acks={"risk-engine-1": ack}, local_cash=local)
    assert ok.ok
    acc3 = next(c for c in ok.checks if c.check == ReconCheck.CASH_DIGEST and c.account_id == "ACC3")
    d3 = acc3.detail or {}
    assert acc3.status == CheckStatus.PASS and d3["expected"] == d3["actual"] == "70000.00"
    # the shard recorded the withdrawal as a deposit: same count and gross amount, but the net breaks the digest
    wrong = msgspec.structs.replace(wd, type=CashType.DEPOSIT)
    bad_ack = msgspec.structs.replace(ack, cash=engine_cash_digests({"ACC3": [dep, wrong]}, accounts))
    bad = digests.compare(accounts=accounts, local_trades={}, td_digest=empty, acks={"risk-engine-1": bad_ack}, local_cash=local)
    assert bad.cash_mismatch == {"ACC3"}
    fail = next(c for c in bad.checks if c.check == ReconCheck.CASH_DIGEST and c.account_id == "ACC3")
    df = fail.detail or {}
    assert fail.status == CheckStatus.FAIL and df["expected"] == "130000.00" and df["actual"] == "70000.00"


def test_progress_file_and_replace_tolerate_open_readers(tmp_path: Path) -> None:
    """Windows refuses to rename over a file another handle has open: progress must not fail the job."""
    import threading

    from futrisk.services.clearing.fsutil import replace_file
    from futrisk.services.clearing.jobs import FileProgress

    target = tmp_path / "progress.json"
    prog = FileProgress(str(target))
    prog(0.1)
    with open(target, encoding="utf-8"):  # the service's poller is reading it
        prog(0.2)  # never raises (best-effort)
    prog(0.3)
    assert json.loads(target.read_text(encoding="utf-8"))["progress"] == 0.3
    dst, src = tmp_path / "report.html", tmp_path / "report.html.tmp"
    dst.write_text("old", encoding="utf-8")
    src.write_text("new", encoding="utf-8")
    reader = open(dst, encoding="utf-8")  # noqa: SIM115 - closed by the timer while replace_file retries
    threading.Timer(0.1, reader.close).start()
    replace_file(src, dst)  # retried until the reader let go
    assert dst.read_text(encoding="utf-8") == "new"
