"""Unit checks of the review fixes without the bus: price inputs (SOD pre_settle first, carry forward, fallback
pre_settle), digest pinning, idempotent report registration, config history, backtest result contract and the
frozen-build entry points (freeze_support, template as package data)."""

from __future__ import annotations

import asyncio
import dataclasses
import json
from decimal import Decimal
from pathlib import Path
from typing import Any

import msgspec
import pytest
from clearing_support import NEXT, D, Scenario, ts

from futrisk.common.config import FullConfig
from futrisk.core import build_settle_input, settle_day
from futrisk.db import Database
from futrisk.messages.enums import Direction, Offset, PosDirection, ReportType, SettlePriceSource
from futrisk.messages.eod import ReportReady, SodAccount, SodLot, SodSnapshot
from futrisk.services.clearing import digests, schema, store
from futrisk.services.clearing.backtests import report_inside, result_fields
from futrisk.services.clearing.confighist import ConfigHistory, settle_hash, stamped_from
from futrisk.services.clearing.prices import decide_prices, fallback_pre_settle
from futrisk.services.clearing.store import ConfigRow


def open_db(tmp: Path) -> Database:
    db = Database(tmp / "clearing.db", schema.METADATA, synchronous="FULL", migrations=schema.MIGRATIONS)
    db.open()
    return db


# ---------------------------------------------------------------- M-1 .. M-3
def test_sod_pre_settle_wins_over_md_and_every_sod_contract_is_carried_forward(default_config: FullConfig) -> None:
    """md reports 3100 as rb2701's previous settlement, the SOD (and the lots) say 3110: no trade, no move -> 0 P&L."""
    scn = Scenario(default_config)
    reg = scn.registry
    sod = SodSnapshot(
        trading_day=D,
        version=1,
        accounts=[SodAccount(account_id="ACC1", pre_balance="10000000.00", initial_capital="10000000.00",
                             cum_net_deposit="0.00", hwm_equity="10000000.00",
                             lots=[SodLot(instrument_id="rb2701", direction=PosDirection.LONG, lot_no=1, open_td="20260924",
                                          open_ts_ns=0, open_price="3100", qty=10, basis_price="3110")])],
        pre_settle={"rb2701": "3110", "IF2612": "4462.2", "cu2611": "78500", "rb2705": "3140"},
    )
    inputs = {"rb2701": build_settle_input(reg, "rb2701", D, [], pre_settle="3100", bid1="3090", ask1="3130")}
    dec = decide_prices(registry=reg, trading_day=D, version=1, needed={"rb2701"}, inputs=inputs, overrides={},
                        official={}, sod_pre=sod.pre_settle)
    rb = dec.views["rb2701"]
    # MEDIAN(bid 3090, ask 3130, pre 3110 from the SOD) = 3110; with md's 3100 it would have been 3100
    assert (rb.source, rb.price) == (SettlePriceSource.FALLBACK_MEDIAN, "3110")
    assert dec.pre_mismatch == {"rb2701": {"sod": "3110", "md": "3100"}}
    assert set(dec.views) == {"rb2701", "IF2612", "cu2611", "rb2705"}  # every SOD pre_settle contract is priced
    assert {dec.views[i].source for i in ("IF2612", "cu2611", "rb2705")} == {SettlePriceSource.PRE_SETTLE}
    ds = settle_day(trading_day=D, version=1, sod=sod, trades=[], cash_movements=[], settle_prices=dec.views,
                    registry=reg, next_trading_day=NEXT, sod_version=1,
                    pre_settle={k: Decimal(v.pre_settle) for k, v in inputs.items()})
    assert Decimal(ds.result.accounts[0].position_pnl) == 0
    assert set(ds.next_sod.pre_settle) == {"rb2701", "IF2612", "cu2611", "rb2705"}


def test_fallback_pre_settle_for_new_contracts(default_config: FullConfig) -> None:
    scn = Scenario(default_config)
    dec = decide_prices(registry=scn.registry, trading_day=D, version=2, needed={"hc2701", "j2701", "rb2701"}, inputs={},
                        overrides={"hc2701": store.OverrideRow("o", D, "hc2701", "3301", "md down", "rm", 1)},
                        official={"j2701": "2100"}, sod_pre={"rb2701": "3110"})
    assert not dec.missing
    fb = fallback_pre_settle(touched={"hc2701", "j2701", "rb2701"}, sod_pre={"rb2701": "3110"}, inputs={},
                             earlier_inputs={"j2701": "2080"}, views=dec.views)
    # an earlier run's md input wins over today's official price; manual stands in when nothing else exists
    assert fb == {"hc2701": {"price": "3301", "source": "MANUAL"},
                  "j2701": {"price": "2080", "source": "md_input_of_earlier_run"}}


# ----------------------------------------------------------------------- H-2
def test_pinned_digests_detect_changed_accounts(default_config: FullConfig) -> None:
    scn = Scenario(default_config)
    pinned = json.loads(json.dumps(digests.pin(scn.trades, scn.cash)))  # as stored in the step log
    assert digests.changed_accounts(pinned, scn.trades, scn.cash) == []
    extra = scn.trade("ACC2", "rb2701", Direction.BUY, Offset.OPEN, 3119.0, 1, "2026-09-28 14:59:00")
    assert digests.changed_accounts(pinned, [*scn.trades, extra], scn.cash) == ["ACC2"]
    assert digests.changed_accounts(pinned, scn.trades, scn.cash[:1]) == ["ACC3"]
    same = [msgspec.structs.replace(c, amount=c.amount.rstrip("0").rstrip(".")) for c in scn.cash]  # "100000" == "100000.00"
    assert digests.changed_accounts(pinned, scn.trades, same) == []


# ----------------------------------------------------------------------- L2
def test_report_registration_is_idempotent(tmp_path: Path) -> None:
    db = open_db(tmp_path)
    try:
        def rep(sha: str, rid: str) -> ReportReady:
            return ReportReady(report_id=rid, trading_day=D, type=ReportType.STATEMENT_TXT, account_id="ACC1",
                               path=f"reports/{D}/statement_ACC1.txt", sha256=sha, size_bytes=10, version=1, created_ns=1)

        first, new = db.write(lambda c: store.upsert_report(c, rep("a" * 64, "r1")))
        again, changed = db.write(lambda c: store.upsert_report(c, rep("a" * 64, "r2")))
        assert new and not changed and again.report_id == "r1"
        moved, changed = db.write(lambda c: store.upsert_report(c, rep("b" * 64, "r3")))
        assert changed and moved.report_id == "r1" and moved.sha256 == "b" * 64
        other = msgspec.structs.replace(rep("a" * 64, "r4"), version=2)  # another settlement version: its own row
        assert db.write(lambda c: store.upsert_report(c, other))[1]
        rows = db.read(lambda c: store.list_reports(c, D))
        assert sorted((r.report_id, r.version, r.sha256[0]) for r in rows) == [("r1", 1, "b"), ("r4", 2, "a")]
    finally:
        db.close()


# ----------------------------------------------------------------------- M-5
class _Clock:
    def now_ns(self) -> int:
        return int(ts("2026-09-28 14:00:00"))


class _Layout:
    def __init__(self, home: Path) -> None:
        self.home = home
        self.versions_dir = home / "config" / "versions"
        self.active_json = home / "config" / "active.json"


class _Rt:
    def __init__(self, cfg: FullConfig, layout: _Layout | None = None) -> None:
        self.config = cfg
        self.layout = layout
        self.market_clock = _Clock()


def test_config_in_force_per_trading_day(tmp_path: Path, default_config: FullConfig) -> None:
    cfg = default_config
    db = open_db(tmp_path)
    try:
        hist = ConfigHistory(_Rt(cfg), db)  # type: ignore[arg-type]
        now = ts("2026-09-28 14:00:00")
        v1 = dataclasses.replace(cfg, config_version=1)
        v2 = dataclasses.replace(cfg, config_version=2, files={**cfg.files, "products.yaml": "e" * 64})  # products changed
        v3 = dataclasses.replace(v2, config_version=3, files={**v2.files, "risk_rules.yaml": "f" * 64})  # rules only
        assert settle_hash(v1) != settle_hash(v2) == settle_hash(v3)
        db.write(lambda c: hist.record_tx(c, v1, now, committed_now=False))
        db.write(lambda c: hist.record_tx(c, v2, now, committed_now=True))  # a commit now: from the next trading day
        db.write(lambda c: hist.record_tx(c, v3, now, committed_now=True))
        rows = db.read(store.config_rows)
        assert [(r.config_version, r.settle_from) for r in rows] == [(1, None), (2, NEXT), (3, NEXT)]
        assert db.read(lambda c: hist.for_day_tx(c, D))[0] == 1  # the day of the commit keeps its products
        assert db.read(lambda c: hist.for_day_tx(c, NEXT))[0] == 3
        db.write(lambda c: hist.record_tx(c, v2, now, committed_now=True))  # idempotent by version
        assert len(db.read(store.config_rows)) == 3
        active = {"config_version": 4, "effective": {"products": {"effect": "NEXT_TRADING_DAY", "trading_day": "20261009"},
                                                     "risk_rules": {"effect": "IMMEDIATE"}}}
        assert stamped_from(active, 4) == "20261009" and stamped_from(active, 3) is None
        loaded, note = asyncio.run(hist.config(cfg.config_version))
        assert loaded is cfg and note is None  # the version this process runs with is served from memory
        missing, note = asyncio.run(hist.config(99))
        assert missing is cfg and note is not None and "99" in note
        assert isinstance(rows[0], ConfigRow)
    finally:
        db.close()


def test_config_version_is_loaded_from_the_versions_directory(tmp_path: Path, default_config: FullConfig) -> None:
    """A restarted worker settles a run with the version pinned at BEGIN, read with the common loader."""
    import shutil

    from futrisk.common.paths import bundled_config_dir

    layout = _Layout(tmp_path / "home")
    shutil.copytree(bundled_config_dir(), layout.versions_dir / "5",
                    ignore=shutil.ignore_patterns("versions", "active.json"))
    db = open_db(tmp_path)
    try:
        hist = ConfigHistory(_Rt(default_config, layout), db)  # type: ignore[arg-type]
        cfg5, note = asyncio.run(hist.config(5))
        assert note is None and cfg5.config_version == 5 and cfg5 is not default_config
        assert cfg5.products.products.keys() == default_config.products.products.keys()
        assert asyncio.run(hist.config(5))[0] is cfg5  # cached
        assert hist.registry(cfg5) is hist.registry(cfg5)
    finally:
        db.close()


# ----------------------------------------------------------------------- L6
def test_backtest_result_contract(tmp_path: Path) -> None:
    fields = result_fields({"summary": {"x": float("nan")}, "config_hash": "c" * 64, "data_hash": "text", "seed": 5,
                            "git_sha": "abc", "report_path": "r.html"})
    assert fields["summary"] == {"x": None} and fields["config_hash"] == "c" * 64 and len(fields["data_hash"]) == 64
    assert (fields["seed"], fields["git_sha"], fields["report_path"]) == (5, "abc", "r.html")
    bad = result_fields({"summary": [1], "seed": True, "git_sha": "x" * 65, "report_path": 3, "config_hash": 7})
    assert bad == {}
    assert result_fields(None) == {}
    out = tmp_path / "bt" / "run1"
    out.mkdir(parents=True)
    assert report_inside(out, None) == (out / "report.html").resolve()
    assert report_inside(out, "sub/r.html") == (out / "sub" / "r.html").resolve()
    assert report_inside(out, "../other/r.html") is None
    assert report_inside(out, str(tmp_path / "evil.html")) is None


# ----------------------------------------------------------------------- H-4
def test_main_calls_freeze_support_before_anything_else(monkeypatch: pytest.MonkeyPatch) -> None:
    import multiprocessing

    import futrisk.main as entry

    calls: list[str] = []
    monkeypatch.setattr(multiprocessing, "freeze_support", lambda: calls.append("freeze_support"))
    monkeypatch.setattr(entry, "_load_subapps", lambda: calls.append("subapps"))
    monkeypatch.setattr(entry, "app", lambda: calls.append("app"))
    monkeypatch.setenv("PYTHONUTF8", "1")
    entry.main()
    assert calls == ["freeze_support", "subapps", "app"]


def test_report_template_is_package_data() -> None:
    from futrisk.services.clearing.reports import render

    source = render.template_source()
    assert "<html" in source.lower() and "{{ titles[0] }}" in source
    assert render.check_template() == len(source)
    with pytest.raises(FileNotFoundError):
        render.check_template("missing.html.j2")
    assert not hasattr(render, "TEMPLATES_DIR")  # never located through __file__


async def test_service_start_fails_fast_without_template(
    tmp_path: Path, bus_config: FullConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    from clearing_support import CW, market_clock_at

    from futrisk.runtime import AsyncServiceRuntime
    from futrisk.services.clearing import service as svc_mod

    def missing() -> int:
        raise FileNotFoundError("risk_daily.html.j2 not packaged")

    monkeypatch.setattr(svc_mod, "check_template", missing)
    rt = AsyncServiceRuntime(CW, config=bus_config, home=tmp_path / "cw", market_clock=market_clock_at("2026-09-28 16:00:00"),
                             setup_logs=False)
    await rt.start()
    try:
        with pytest.raises(FileNotFoundError):
            await svc_mod.ClearingService(rt).start()
        assert not (tmp_path / "cw" / "db" / "clearing.db").exists()  # nothing was opened
    finally:
        await rt.stop()


def test_account_ids_are_safe_file_names() -> None:
    from futrisk.services.clearing.reports.render import safe_component

    assert safe_component("ACC1") == "ACC1"
    for bad in ("../x", "..", "a/b", "a\\b", "C:evil", "账户1", ""):
        name = safe_component(bad)
        assert name not in (".", "..") and "/" not in name and "\\" not in name and ":" not in name
        assert name.rsplit("-", 1)[-1].isalnum() and len(name) <= 73
    assert safe_component("a/b") != safe_component("a_b")  # replaced characters never collide with a real id


# ------------------------------------------------------------------ CFFEX benchmark (结算细则第四十三条)
def _last_hour(reg: Any, inst: str, trades: list[tuple[float, int]], pre: str) -> Any:
    t0 = ts("2026-09-28 14:10:00")
    tape = [(t0 + i * 60_000_000_000, px, qty) for i, (px, qty) in enumerate(trades)]
    return build_settle_input(reg, inst, D, tape, pre_settle=pre)


def test_cffex_benchmark_is_the_traded_contract_nearest_to_delivery(default_config: FullConfig) -> None:
    """Near expiry the next month has the volume, but the benchmark stays the front month that traded."""
    reg = Scenario(default_config).registry
    sod_pre = {"IF2610": "4400", "IF2611": "4430", "IF2612": "4462.2", "IF2703": "4480"}
    inputs = {
        "IF2610": _last_hour(reg, "IF2610", [(4410.0, 5)], "4400"),  # expiring front month: 5 lots, +10 points
        "IF2611": _last_hour(reg, "IF2611", [(4460.0, 300)], "4430"),  # next month: most volume, +30 points
        "IF2612": build_settle_input(reg, "IF2612", D, [], pre_settle="4462.2"),  # no trade today
        "IH2610": _last_hour(reg, "IH2610", [(2700.0, 50)], "2600"),  # another product never counts
    }
    dec = decide_prices(registry=reg, trading_day=D, version=1, needed={"IF2612", "IF2703"}, inputs=inputs,
                        overrides={}, official={}, sod_pre=sod_pre)
    for inst in ("IF2612", "IF2703"):
        assert dec.views[inst].source == SettlePriceSource.FALLBACK_BENCHMARK, inst
    # the POINT move of the front month (+10), added to each contract's own previous settlement price
    assert dec.views["IF2612"].price == "4472.2"  # not 4492.2 (IF2611's +30) nor 4472.4 (a +0.227 % move)
    assert dec.views["IF2703"].price == "4490"


def test_cffex_benchmark_skipped_without_a_traded_contract(default_config: FullConfig) -> None:
    reg = Scenario(default_config).registry
    inputs = {
        "IF2611": build_settle_input(reg, "IF2611", D, [], pre_settle="4430"),  # quiet, priced only officially
        "IF2612": build_settle_input(reg, "IF2612", D, [], pre_settle="4462.2"),
        "IH2610": _last_hour(reg, "IH2610", [(2700.0, 50)], "2600"),
    }
    dec = decide_prices(registry=reg, trading_day=D, version=1, needed={"IF2612"}, inputs=inputs, overrides={},
                        official={"IF2611": "4440"}, sod_pre={"IF2611": "4430", "IF2612": "4462.2"})
    assert dec.views["IF2611"].source == SettlePriceSource.OFFICIAL  # priced, but it did not trade: no benchmark
    assert (dec.views["IF2612"].source, dec.views["IF2612"].price) == (SettlePriceSource.PRE_SETTLE, "4462.2")


# ------------------------------------------------------------------ drawdown labels and history maximum
def test_max_drawdown_since_the_first_settled_day(tmp_path: Path, default_config: FullConfig) -> None:
    from futrisk.core import determine_settlement_price
    from futrisk.messages.enums import EodPhase
    from futrisk.messages.eod import SettlementResult
    from futrisk.services.clearing.store import EodRun

    scn = Scenario(default_config)
    req = scn.seed_request()
    sod = SodSnapshot(trading_day=D, version=0, accounts=req.accounts, pre_settle=req.pre_settle)
    views = {i: determine_settlement_price(inp, scn.registry, version=1) for i, inp in scn.settle_inputs().items()}
    ds = settle_day(trading_day=D, version=1, sod=sod, trades=scn.trades, cash_movements=scn.cash, settle_prices=views,
                    registry=scn.registry, next_trading_day=NEXT, sod_version=1)
    db = open_db(tmp_path)
    try:
        def day(td: str, version: int, dd: str, phase: EodPhase = EodPhase.DONE) -> Any:
            accts = [msgspec.structs.replace(a, trading_day=td, version=version,
                                             max_drawdown=dd if a.account_id == "ACC1" else "0")
                     for a in ds.result.accounts]

            def tx(c: Any) -> None:
                store.insert_run(c, EodRun(trading_day=td, version=version, phase=phase, next_trading_day=NEXT,
                                           triggered_by="t", config_hash="0" * 64))
                store.save_settlement(c, SettlementResult(trading_day=td, version=version, accounts=accts), created_ns=1)

            db.write(tx)

        day("20260923", 1, "0.05")
        day("20260924", 1, "0.08")  # replaced by version 2 of the same day
        day("20260924", 2, "0.02")
        day(D, 1, "0.03")
        day(NEXT, 1, "0.09")  # after the reported day
        got = db.read(lambda c: store.max_drawdowns(c, D))
        assert got["ACC1"] == "0.05" and got["ACC2"] == "0"
    finally:
        db.close()


def test_drawdown_labels_match_the_values(default_config: FullConfig) -> None:
    """``max_drawdown`` holds the CURRENT drawdown from the high-water mark: labels say so."""
    from futrisk.core import determine_settlement_price
    from futrisk.services.clearing.reports.model import ReportBundle, funds_rows
    from futrisk.services.clearing.reports.render import render_daily_html
    from futrisk.services.clearing.reports.statement import render_statement_csv, render_statement_txt

    scn = Scenario(default_config)
    req = scn.seed_request()
    sod = SodSnapshot(trading_day=D, version=0, accounts=req.accounts, pre_settle=req.pre_settle)
    views = {i: determine_settlement_price(inp, scn.registry, version=1) for i, inp in scn.settle_inputs().items()}
    ds = settle_day(trading_day=D, version=1, sod=sod, trades=scn.trades, cash_movements=scn.cash, settle_prices=views,
                    registry=scn.registry, next_trading_day=NEXT, sod_version=1)
    bundle = {
        "trading_day": D, "version": 1, "next_trading_day": NEXT, "generated_ns": 1, "config_hash": "0" * 64,
        "config_version": 1, "triggered_by": "t", "settlement": msgspec.to_builtins(ds.result),
        "trades": msgspec.to_builtins(scn.trades), "sod": msgspec.to_builtins(sod),
        "accounts": {a: {"name": s.name} for a, s in default_config.accounts.accounts.items()},
        "max_dd": {"ACC1": "0.0412"},
    }
    b = ReportBundle.from_dict(bundle)
    funds, total = funds_rows(b)
    acc1 = next(f for f in funds if f.account_id == "ACC1")
    assert acc1.peak_drawdown == Decimal("0.0412") and total.peak_drawdown is None
    assert next(f for f in funds if f.account_id == "ACC2").peak_drawdown is None  # no history: shown as "—"
    txt, csv_text = render_statement_txt(b, "ACC1"), render_statement_csv(b, "ACC1")
    assert "距高水位回撤" in txt and "最大回撤" not in txt
    assert "当前回撤（距高水位）" in csv_text and "最大回撤" not in csv_text
    html = render_daily_html(b, with_charts=False)
    assert "距高水位回撤</th><th>历史最大回撤</th>" in html and "<th>最大回撤</th>" not in html
    assert "4.12%" in html
