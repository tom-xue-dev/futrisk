"""``futrisk settle ... | report ...`` against the fake api-gateway (02 §9.6, §6.5)."""

from __future__ import annotations

import webbrowser
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import url2pathname

import pytest
from clitools_support import Cli, is_uuid

HEALTH = {"status": "ok", "trading_day": "20260928", "ts_ns": 1_790_000_000_000_000_000}
ACCOUNT = {"trading_day": "20260925", "account_id": "ACC1", "version": 2, "pre_balance": "10000000.00",
           "deposit": "0.00", "withdraw": "0.00", "close_pnl": "-12345.67", "position_pnl": "2500.10",
           "fee": "123.40", "balance": "9990031.03", "equity": "9990031.03", "margin": "1200000.00",
           "available": "8790031.03", "risk_degree": "0.1201", "margin_call": "0.00", "exch_formula_pnl": "0.00",
           "fifo_realized_pnl": "0.00", "fifo_float_pnl": "0.00", "initial_capital": "10000000.00",
           "cum_net_deposit": "0.00", "hwm_equity": "10000000.00", "nav": "0.999003", "max_drawdown": "0.000997",
           "checks_passed": True}


def test_settle_run_defaults_to_the_current_trading_day(cli: Cli) -> None:
    cli.api.on("GET", "/health", HEALTH)
    cli.api.on("POST", "/settlement/run", {"trading_day": "20260928", "version": 1, "phase": "BEGIN"})
    r = cli.invoke("settle", "run")
    assert r.exit_code == 0, r.output
    health = cli.api.last("GET", "/health")
    assert "x-api-key" not in health.headers  # public
    run = cli.api.last("POST", "/settlement/run")
    assert run.body == {"td": "20260928", "force_new_version": False} and is_uuid(run.idem)
    assert "settlement of 20260928: version 1, phase BEGIN" in r.output


def test_settle_run_wait_polls_until_done(cli: Cli) -> None:
    cli.api.on("POST", "/settlement/run", {"trading_day": "20260928", "version": 2, "phase": "BEGIN"})
    # the first answer is still the end of version 1: not the end of the run just started
    for version, phase in ((1, "DONE"), (2, "FROZEN"), (2, "SETTLED"), (2, "REPORTED"), (2, "DONE")):
        cli.api.on("GET", "/settlement/20260928/status",
                   {"trading_day": "20260928", "version": version, "phase": phase, "next_trading_day": "20260929",
                    "detail": {"accounts": 5}, "ts_ns": 1_790_000_000_000_000_000})
    r = cli.invoke("settle", "run", "--td", "2026-09-28", "--force-new-version", "--wait")
    assert r.exit_code == 0, r.output
    assert cli.api.last("POST", "/settlement/run").body == {"td": "20260928", "force_new_version": True}
    for line in ("v1 DONE", "v2 FROZEN", "v2 SETTLED", "v2 REPORTED", "v2 DONE"):
        assert line in r.output
    assert len(cli.api.calls("GET", "/settlement/20260928/status")) == 5
    assert cli.sleeps == [1.0, 1.0, 1.0, 1.0]
    assert "done" in r.output and "20260929" in r.output


def test_settle_run_wait_failure_exits_1(cli: Cli) -> None:
    cli.api.on("POST", "/settlement/run", {"trading_day": "20260928", "version": 1, "phase": "BEGIN"})
    cli.api.on("GET", "/settlement/20260928/status",
               {"trading_day": "20260928", "version": 1, "phase": "FAILED", "error": "prices missing: cu2611",
                "ts_ns": 1_790_000_000_000_000_000})
    r = cli.invoke("settle", "run", "--td", "20260928", "--wait")
    assert r.exit_code == 1
    assert "prices missing: cu2611" in r.output and "FAILED" in r.output


def test_settle_status_show_and_days(cli: Cli) -> None:
    days = {"items": [{"trading_day": "20260928", "version": 1, "phase": "FROZEN"},
                      {"trading_day": "20260925", "version": 2, "phase": "DONE"}], "errors": {"20260924": "TIMEOUT"}}
    cli.api.on("GET", "/settlement/days", days)
    cli.api.on("GET", "/settlement/20260925",
               {"trading_day": "20260925", "version": 2, "accounts": [ACCOUNT],
                "positions": [{"account_id": "ACC1", "instrument_id": "rb2701", "direction": "LONG", "sod_qty": 1,
                               "open_qty": 2, "close_qty": 1, "eod_qty": 2, "settle_price": "3110",
                               "pre_settle_price": "3120", "margin_gross": "31100.00", "close_pnl": "-100.00",
                               "position_pnl": "-200.00", "fee": "3.11"}],
                "prices": [{"trading_day": "20260925", "instrument_id": "rb2701", "price": "3110",
                            "source": "VWAP", "method": "VWAP_FULL_DAY", "input_quality": "OK"}],
                "checks": [{"check": "EQUITY_IDENTITY", "status": "PASS"},
                           {"check": "MTM_VS_FIFO", "status": "WARN", "account_id": "ACC1", "detail": {"diff": "0.01"}}]})
    r = cli.invoke("settle", "show", "--account", "ACC1")
    assert r.exit_code == 0, r.output
    assert cli.api.last("GET", "/settlement/20260925").params == {"account": "ACC1"}  # newest settled day
    out = r.output
    for text in ("10000000.00", "-12345.67", "9990031.03", "0.1201", "0.999003", "31100.00", "VWAP_FULL_DAY",
                 "1/2 passed", "MTM_VS_FIFO", "diff=0.01"):
        assert text in out, text

    cli.api.on("GET", "/settlement/20260928/status", {"trading_day": "20260928", "version": 1, "phase": "FROZEN",
                                                      "detail": {"shards": 2}, "ts_ns": 1_790_000_000_000_000_000})
    r = cli.invoke("settle", "status", "--td", "20260928")
    assert r.exit_code == 0 and "FROZEN" in r.output and "shards=2" in r.output

    r = cli.invoke("settle", "days", "--limit", "5")
    assert r.exit_code == 0, r.output
    assert cli.api.last("GET", "/settlement/days").params == {"limit": "5"}
    assert "20260925" in r.output and "DONE" in r.output and "20260924=TIMEOUT" in r.output


def test_settle_show_without_a_settled_day(cli: Cli) -> None:
    cli.api.on("GET", "/settlement/days", {"items": [{"trading_day": "20260928", "version": 1, "phase": "BEGIN"}]})
    r = cli.invoke("settle", "show")
    assert r.exit_code == 1 and "NOT_FOUND" in r.output and "pass --td" in r.output


REPORTS = [
    {"report_id": "r-1", "trading_day": "20260928", "type": "RISK_DAILY_HTML", "account_id": None,
     "path": "reports/20260928/risk_daily.html", "sha256": "aa", "size_bytes": 100, "version": 1,
     "created_ns": 1_790_000_000_000_000_000},
    {"report_id": "r-2", "trading_day": "20260928", "type": "RISK_DAILY_HTML", "account_id": None,
     "path": "reports/20260928/risk_daily.html", "sha256": "bb", "size_bytes": 120, "version": 2,
     "created_ns": 1_790_000_100_000_000_000},
    {"report_id": "r-3", "trading_day": "20260928", "type": "STATEMENT_TXT", "account_id": "ACC1",
     "path": "reports/20260928/statement_ACC1.txt", "sha256": "cc", "size_bytes": 50, "version": 2,
     "created_ns": 1_790_000_100_000_000_000},
    {"report_id": "r-4", "trading_day": "20260928", "type": "STATEMENT_CSV", "account_id": "ACC1",
     "path": "reports/20260928/statement_ACC1.csv", "sha256": "dd", "size_bytes": 60, "version": 2,
     "created_ns": 1_790_000_100_000_000_000},
]


def test_report_list_with_kind_filter(cli: Cli) -> None:
    cli.api.on("GET", "/reports", {"items": REPORTS})  # this fake ignores ?kind= (an older gateway)
    r = cli.invoke("report", "list", "--td", "20260928", "--kind", "statement")
    assert r.exit_code == 0, r.output
    assert cli.api.last("GET", "/reports").params == {"td": "20260928"}  # two kinds: filtered here only
    assert "STATEMENT_TXT" in r.output and "STATEMENT_CSV" in r.output and "RISK_DAILY_HTML" not in r.output
    r, doc = cli.json("report", "list", "--kind", "statement-txt")
    assert cli.api.last("GET", "/reports").params == {"kind": "STATEMENT_TXT"}  # one kind: the server filters
    assert [x["report_id"] for x in doc["data"]["items"]] == ["r-3"]
    cli.api.on("GET", "/reports", {"items": REPORTS[2:3]})  # a gateway honouring ?kind=
    r, doc = cli.json("report", "list", "--kind", "STATEMENT_TXT")
    assert [x["report_id"] for x in doc["data"]["items"]] == ["r-3"] and doc["code"] == "OK"
    cli.api.on("GET", "/reports", None, code="BAD_REQUEST", message="kind must be one of RISK_DAILY_HTML, ...")
    r = cli.invoke("report", "list", "--kind", "weekly")
    assert cli.api.last("GET", "/reports").params == {"kind": "WEEKLY"}  # unknown here: the server judges
    assert r.exit_code == 1 and "kind must be one of" in r.output


@pytest.fixture
def opened(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    urls: list[str] = []

    def fake_open(url: str) -> bool:
        urls.append(url)
        return True

    monkeypatch.setattr(webbrowser, "open", fake_open)
    return urls


def test_report_daily_opens_the_local_file(cli: Cli, opened: list[str]) -> None:
    local = cli.home / "reports" / "20260928" / "risk_daily.html"
    local.parent.mkdir(parents=True)
    local.write_text("<html>daily</html>", encoding="utf-8")
    cli.api.on("GET", "/reports", {"items": REPORTS})
    r = cli.invoke("report", "daily", "--open")
    assert r.exit_code == 0, r.output
    assert cli.api.last("GET", "/reports").params == {"kind": "RISK_DAILY_HTML"}
    assert "http://127.0.0.1:18080/api/v1/reports/r-2" in r.output  # newest version
    assert str(local) in r.output
    assert opened == [local.resolve().as_uri()]
    assert cli.api.calls("GET", "/reports/r-2") == []  # no download needed


def test_report_daily_downloads_when_the_file_is_not_local(cli: Cli, opened: list[str]) -> None:
    cli.api.on("GET", "/reports", {"items": REPORTS})
    cli.api.on("GET", "/reports/r-2", raw="<html>风控日报</html>".encode(), headers={"content-type": "text/html"})
    r = cli.invoke("report", "daily", "--td", "20260928", "--open")
    assert r.exit_code == 0, r.output
    assert "not on this machine" in r.output
    assert len(opened) == 1
    path = Path(url2pathname(urlparse(opened[0]).path))
    assert path.read_text(encoding="utf-8") == "<html>风控日报</html>"

    r, doc = cli.json("report", "daily")
    assert doc["report_id"] == "r-2" and doc["url"].endswith("/api/v1/reports/r-2") and doc["local_exists"] is False


def test_report_daily_missing(cli: Cli) -> None:
    cli.api.on("GET", "/reports", {"items": REPORTS[2:]})
    r = cli.invoke("report", "daily", "--td", "20260928")
    assert r.exit_code == 1 and "NOT_FOUND" in r.output and "no daily risk report for 20260928" in r.output


def test_report_statement_prints_the_text_as_is(cli: Cli) -> None:
    text = "客户号 ACC1            交易日 20260928\n权益 9990031.03   可用资金 8790031.03\n"
    cli.api.on("GET", "/settlement/20260928/statements/ACC1", {"format": "text", "text": text})
    r = cli.invoke("report", "statement", "ACC1", "--td", "20260928")
    assert r.exit_code == 0, r.output
    assert r.stdout == text  # byte for byte, no extra newline
    cli.api.on("GET", "/settlement/20260928/statements/ACC1", text)  # an older gateway: the text itself
    assert cli.invoke("report", "statement", "ACC1", "--td", "20260928").stdout == text


def test_report_statement_falls_back_to_settlement_data(cli: Cli) -> None:
    cli.api.on("GET", "/settlement/days", {"items": [{"trading_day": "20260925", "version": 2, "phase": "REPORTED"}]})
    cli.api.on("GET", "/settlement/20260925/statements/ACC1",
               {"format": "settlement", "settlement": {"trading_day": "20260925", "version": 2, "accounts": [ACCOUNT]}},
               message="statement file not available: settlement data instead")
    r = cli.invoke("report", "statement", "ACC1")
    assert r.exit_code == 0, r.output
    assert "statement file not available" in r.output and "9990031.03" in r.output
