"""``futrisk settle ... | report ...``: end-of-day settlement and reports (02 §9.6, §6.5; 01 §5.2).

=========================================  ======================================================
command                                    REST call (api-gateway ``/api/v1``)
=========================================  ======================================================
``settle run [--td] [--wait]``             ``POST /settlement/run {td, force_new_version}`` (+ status polls)
``settle status [--td]``                   ``GET /settlement/{td}/status``
``settle show [--td] [--account]``         ``GET /settlement/{td}?account=&version=``
``settle days``                            ``GET /settlement/days?limit=``
``report list [--td] [--kind]``            ``GET /reports?td=&kind=`` (a prefix of several kinds: filtered here)
``report daily [--td] [--open]``           ``GET /reports?td=&kind=RISK_DAILY_HTML`` -> the newest; file / URL
``report statement <account> [--td]``      ``GET /settlement/{td}/statements/{account}`` (``{format: text|settlement}``)
=========================================  ======================================================

Without ``--td``: ``settle run|status`` use the current trading day (``GET /health``), ``settle
show`` and ``report statement`` the newest settled day (``GET /settlement/days``), ``report
daily`` the newest daily report. Settlement money is ``DecStr`` and printed exactly as given.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import typer

from futrisk.cli.toolkit import (
    ApiOpt,
    CliError,
    HomeOpt,
    JsonOpt,
    Session,
    TdOpt,
    as_dict,
    bj_time,
    dicts,
    fmt,
    kv,
    session,
    state_style,
    td_option,
)

settle_app = typer.Typer(help="End-of-day settlement: run, status, results (ADMIN runs it)", no_args_is_help=True)
report_app = typer.Typer(help="Daily risk report and account statements", no_args_is_help=True)

#: EOD phases at or after which a settlement result exists (02 §8.4)
SETTLED_PHASES = ("SETTLED", "ROLLED", "REPORTED", "DONE")
FINAL_PHASES = ("DONE", "FAILED")
POLL_S = 1.0
#: 02 §7.8 ``ReportType`` (``GET /reports?kind=`` takes exactly one of them)
REPORT_KINDS = ("RISK_DAILY_HTML", "STATEMENT_TXT", "STATEMENT_CSV", "SETTLEMENT_JSON", "BACKTEST_HTML")
DAILY_KIND = "RISK_DAILY_HTML"


def _seg(value: str) -> str:
    from futrisk.cli.apiclient import seg

    return seg(value)


def _int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def latest_settled_td(s: Session, limit: int = 30) -> str:
    """Newest trading day whose EOD reached ``SETTLED`` (``GET /settlement/days``)."""
    reply = s.api.get("/settlement/days", params={"limit": limit})
    items = reply.data.get("items") if isinstance(reply.data, dict) else None
    for item in items or []:
        if isinstance(item, dict) and item.get("phase") in SETTLED_PHASES and item.get("trading_day"):
            return str(item["trading_day"])
    raise CliError("NOT_FOUND", f"no settled trading day in the last {limit} trading days; pass --td")


# ====================================================================== settle
def _status_fields(s: Session, ev: dict[str, Any]) -> None:
    phase = ev.get("phase")
    pairs: list[tuple[str, Any]] = [
        ("trading day", ev.get("trading_day")),
        ("version", ev.get("version")),
        ("phase", (phase, state_style(phase) or ("green" if phase in SETTLED_PHASES else ""))),
        ("next trading day", ev.get("next_trading_day")),
        ("updated", bj_time(ev.get("ts_ns"))),
    ]
    if ev.get("error"):
        pairs.append(("error", (ev.get("error"), "red")))
    detail = ev.get("detail")
    if isinstance(detail, dict) and detail:
        pairs.append(("detail", kv(detail)))
    s.out.fields(pairs)


@settle_app.command("run")
def settle_run(
    td: str | None = TdOpt,
    force_new_version: bool = typer.Option(False, "--force-new-version", help="Settle again as a new version"),
    wait: bool = typer.Option(False, "--wait", "-w", help="Poll the status until DONE or FAILED"),
    timeout: float = typer.Option(300.0, "--timeout", help="Seconds to wait with --wait"),
    home: str | None = HomeOpt,
    api: str | None = ApiOpt,
    as_json: bool = JsonOpt,
) -> None:
    """Start (or continue) the EOD settlement saga of a trading day (POST /settlement/run)."""
    day = td_option(td)
    with session(home, api, as_json) as s:
        from futrisk.cli import apiclient

        day = day or s.trading_day()
        reply = s.api.post("/settlement/run", {"td": day, "force_new_version": force_new_version})
        ack = as_dict(reply.data)
        if not wait:
            if s.out.json:
                s.out.raw(reply.raw)
                return
            dup = " (duplicate)" if reply.duplicate else ""
            msg = f"; {reply.message}" if reply.message else ""
            s.out.ok(f"settlement of {day}: version {fmt(ack.get('version'))}, phase {fmt(ack.get('phase'))}{msg}{dup}")
            s.out.dim(f"follow it with `futrisk settle status --td {day}`")
            return
        if not s.out.json:
            s.out.line(f"settlement of {day}: version {fmt(ack.get('version'))}, phase {fmt(ack.get('phase'))}; waiting...")
        deadline = time.monotonic() + max(0.0, timeout)
        version = _int(ack.get("version"))
        last: str | None = None
        while True:
            st = s.api.get(f"/settlement/{day}/status", check=False)
            ev = as_dict(st.data) if st.ok else {}
            phase = ev.get("phase")
            if phase and phase != last and not s.out.json:
                s.out.line(f"  {bj_time(ev.get('ts_ns'), date=False)}  v{fmt(ev.get('version'))} {phase}",
                           state_style(phase) or None)
            last = phase or last
            if phase in FINAL_PHASES and _int(ev.get("version")) >= version:  # not an older version's end
                break
            if not st.ok and st.code not in ("NOT_FOUND", "TIMEOUT", "UNAVAILABLE", "BUSY"):
                raise st.error()
            if time.monotonic() >= deadline:
                raise CliError("TIMEOUT", f"settlement of {day} still in {last or 'no phase'} after {timeout:g}s",
                               hint=f"`futrisk settle status --td {day}`")
            apiclient.SLEEP(POLL_S)
        if s.out.json:
            s.out.raw(st.raw)
            if phase == "FAILED":
                raise typer.Exit(1)
            return
        _status_fields(s, ev)
        if phase == "FAILED":
            raise CliError("INTERNAL", f"settlement of {day} FAILED: {fmt(ev.get('error'))}")
        s.out.ok(f"settlement of {day} done (`futrisk report daily --td {day}`)")


@settle_app.command("status")
def settle_status(
    td: str | None = TdOpt,
    home: str | None = HomeOpt,
    api: str | None = ApiOpt,
    as_json: bool = JsonOpt,
) -> None:
    """Latest EOD phase of a trading day (GET /settlement/{td}/status)."""
    day = td_option(td)
    with session(home, api, as_json) as s:
        day = day or s.trading_day()
        reply = s.api.get(f"/settlement/{day}/status")
        if s.out.json:
            s.out.raw(reply.raw)
            return
        _status_fields(s, as_dict(reply.data))


def _accounts_view(s: Session, accounts: list[dict[str, Any]]) -> None:
    if len(accounts) == 1:
        a = accounts[0]
        keys = ["pre_balance", "deposit", "withdraw", "close_pnl", "position_pnl", "fee", "balance", "equity", "margin",
                "available", "risk_degree", "margin_call", "nav", "max_drawdown", "hwm_equity", "initial_capital",
                "cum_net_deposit", "fifo_realized_pnl", "fifo_float_pnl", "exch_formula_pnl", "checks_passed"]
        s.out.line(f"account {a.get('account_id')} (settlement v{fmt(a.get('version'))} of {a.get('trading_day')})",
                   "bold")
        s.out.fields([(k.replace("_", " "), a.get(k)) for k in keys if k in a])
        return
    s.out.table(
        ["ACCOUNT", "PRE BALANCE", "DEPOSIT", "WITHDRAW", "CLOSE P&L", "POSITION P&L", "FEE", "EQUITY", "MARGIN",
         "AVAILABLE", "RISK DEGREE", "MARGIN CALL", "NAV", "CHECKS"],
        [
            [a.get("account_id"), a.get("pre_balance"), a.get("deposit"), a.get("withdraw"), a.get("close_pnl"),
             a.get("position_pnl"), a.get("fee"), a.get("equity"), a.get("margin"), a.get("available"),
             a.get("risk_degree"), a.get("margin_call"), a.get("nav"),
             ("ok", "green") if a.get("checks_passed") else ("FAILED", "red")]
            for a in accounts
        ],
        title="accounts",
    )


@settle_app.command("show")
def settle_show(
    td: str | None = TdOpt,
    account: str | None = typer.Option(None, "--account", "-a", help="Only this account"),
    version: int | None = typer.Option(None, "--version", help="Settlement version (default: the effective one)"),
    home: str | None = HomeOpt,
    api: str | None = ApiOpt,
    as_json: bool = JsonOpt,
) -> None:
    """Settlement result: accounts, positions, prices, checks (GET /settlement/{td})."""
    day = td_option(td)
    with session(home, api, as_json) as s:
        day = day or latest_settled_td(s)
        reply = s.api.get(f"/settlement/{day}", params={"account": account, "version": version})
        if s.out.json:
            s.out.raw(reply.raw)
            return
        res = as_dict(reply.data)
        s.out.line(f"settlement of {fmt(res.get('trading_day'))}, version {fmt(res.get('version'))}", "bold")
        accounts = dicts(res.get("accounts"))
        _accounts_view(s, accounts)
        positions = dicts(res.get("positions"))
        if positions:
            s.out.table(
                ["ACCOUNT", "INSTRUMENT", "DIR", "SOD", "OPEN", "CLOSE", "EOD", "SETTLE", "PRE SETTLE", "MARGIN",
                 "CLOSE P&L", "POSITION P&L", "FEE"],
                [
                    [p.get("account_id"), p.get("instrument_id"), p.get("direction"), p.get("sod_qty"), p.get("open_qty"),
                     p.get("close_qty"), p.get("eod_qty"), p.get("settle_price"), p.get("pre_settle_price"),
                     p.get("margin_gross"), p.get("close_pnl"), p.get("position_pnl"), p.get("fee")]
                    for p in positions
                ],
                title="positions",
            )
        prices = dicts(res.get("prices"))
        if prices:
            s.out.table(
                ["INSTRUMENT", "SETTLE PRICE", "SOURCE", "METHOD", "INPUT", "OPERATOR", "REASON"],
                [
                    [p.get("instrument_id"), p.get("price"), p.get("source"), p.get("method"), p.get("input_quality"),
                     p.get("operator"), p.get("reason")]
                    for p in prices
                ],
                title="settlement prices",
            )
        checks = dicts(res.get("checks"))
        bad = [c for c in checks if c.get("status") != "PASS"]
        if checks:
            style = "green" if not bad else ("red" if any(c.get("status") == "FAIL" for c in bad) else "yellow")
            s.out.line(f"checks: {len(checks) - len(bad)}/{len(checks)} passed", style)
        if bad:
            s.out.table(
                ["CHECK", "STATUS", "ACCOUNT", "DETAIL"],
                [[c.get("check"), (c.get("status"), state_style(c.get("status"))), c.get("account_id"),
                  kv(c.get("detail") if isinstance(c.get("detail"), dict) else None)] for c in bad],
            )


@settle_app.command("days")
def settle_days(
    limit: int = typer.Option(20, "--limit", "-n", min=1, max=120, help="Number of recent trading days"),
    home: str | None = HomeOpt,
    api: str | None = ApiOpt,
    as_json: bool = JsonOpt,
) -> None:
    """Recent trading days with their latest EOD phase (GET /settlement/days)."""
    with session(home, api, as_json) as s:
        reply = s.api.get("/settlement/days", params={"limit": limit})
        if s.out.json:
            s.out.raw(reply.raw)
            return
        data = as_dict(reply.data)
        items = dicts(data.get("items"))
        s.out.table(
            ["TRADING DAY", "VERSION", "PHASE"],
            [[i.get("trading_day"), i.get("version"), (i.get("phase"), state_style(i.get("phase")))] for i in items],
            title=f"settlement days ({len(items)} with an EOD run)",
        )
        errors = data.get("errors")
        if isinstance(errors, dict) and errors:
            s.out.warn(f"clearing-worker did not answer for: {kv(errors)}")


# ====================================================================== reports
def report_kind(kind: str | None) -> tuple[str | None, str | None]:
    """``--kind`` -> ``(?kind= for the server, prefix filtered here)``.

    A type or a prefix of exactly one known type is filtered by the server; a prefix of several
    (``statement`` = TXT + CSV) only here; an unknown text is left to the server to judge. The
    prefix is filtered here in every case (an older api-gateway ignores ``?kind=``).
    """
    if kind is None or not kind.strip():
        return None, None
    prefix = kind.strip().upper().replace("-", "_")
    known = [t for t in REPORT_KINDS if t.startswith(prefix)]
    if len(known) == 1:
        return known[0], prefix
    return (None if known else prefix), prefix


def _kind_match(item: dict[str, Any], prefix: str | None) -> bool:
    return prefix is None or str(item.get("type", "")).upper().startswith(prefix)


@report_app.command("list")
def report_list(
    td: str | None = TdOpt,
    kind: str | None = typer.Option(
        None, "--kind", "-k", help="Report type or prefix: risk_daily, statement, statement_txt, settlement, backtest"
    ),
    home: str | None = HomeOpt,
    api: str | None = ApiOpt,
    as_json: bool = JsonOpt,
) -> None:
    """Generated reports (GET /reports?td=&kind=)."""
    day = td_option(td)
    server_kind, prefix = report_kind(kind)
    with session(home, api, as_json) as s:
        reply = s.api.get("/reports", params={"td": day, "kind": server_kind})
        answered = dicts(as_dict(reply.data).get("items"))
        items = [r for r in answered if _kind_match(r, prefix)]
        if s.out.json:
            if len(items) == len(answered):
                s.out.raw(reply.raw)
            else:  # several kinds, or an older gateway without ?kind=: the same envelope, filtered here
                s.out.obj({"ok": True, "code": reply.code, "message": reply.message, "data": {"items": items}})
            return
        s.out.table(
            ["CREATED", "TD", "TYPE", "ACCOUNT", "VERSION", "BYTES", "PATH", "REPORT ID"],
            [
                [bj_time(r.get("created_ns")), r.get("trading_day"), r.get("type"), r.get("account_id"),
                 r.get("version"), r.get("size_bytes"), r.get("path"), r.get("report_id")]
                for r in items
            ],
            title=f"reports ({len(items)})",
        )


def _local_report(s: Session, rel: str) -> Path | None:
    """The report file under ``<home>`` (None when the path leaves the home)."""
    home = s.layout.home.resolve()
    path = (home / rel).resolve()
    return path if path.is_relative_to(home) else None


def _download(s: Session, report: dict[str, Any]) -> Path:
    import tempfile

    resp = s.api.download(f"/reports/{_seg(str(report.get('report_id')))}")
    name = Path(str(report.get("path") or "report.html")).name
    stem = f"{report.get('trading_day') or 'na'}_v{_int(report.get('version'))}_{name}"
    target = Path(tempfile.gettempdir()) / "futrisk-reports" / stem
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(resp.content)
    return target


@report_app.command("daily")
def report_daily(
    td: str | None = TdOpt,
    open_it: bool = typer.Option(False, "--open", help="Open the report in the browser"),
    home: str | None = HomeOpt,
    api: str | None = ApiOpt,
    as_json: bool = JsonOpt,
) -> None:
    """The daily risk report (RISK_DAILY_HTML) of a day or the newest one: path, URL, --open."""
    day = td_option(td)
    with session(home, api, as_json) as s:
        reply = s.api.get("/reports", params={"td": day, "kind": DAILY_KIND})
        cands = [r for r in dicts(as_dict(reply.data).get("items")) if _kind_match(r, DAILY_KIND)]
        if not cands:
            which = f"for {day}" if day else "yet"
            raise CliError("NOT_FOUND", f"no daily risk report {which}",
                           hint="it is written at the end of the EOD settlement (`futrisk settle status`)")
        best = max(cands, key=lambda r: (str(r.get("trading_day") or ""), _int(r.get("version")),
                                         _int(r.get("created_ns"))))
        url = s.api.url(f"/reports/{_seg(str(best.get('report_id')))}")
        local = _local_report(s, str(best.get("path") or ""))
        exists = local is not None and local.is_file()
        if s.out.json:
            s.out.obj({**best, "url": url, "local_path": str(local) if local else None, "local_exists": exists})
        else:
            s.out.fields([
                ("trading day", best.get("trading_day")),
                ("version", best.get("version")),
                ("created", bj_time(best.get("created_ns"))),
                ("bytes", best.get("size_bytes")),
                ("sha256", best.get("sha256")),
                ("path", (f"{local}" if exists else f"{local or best.get('path')} (not on this machine)",
                          "" if exists else "yellow")),
                ("url", f"{url} (needs the X-API-Key header)"),
            ])
        if open_it:
            import webbrowser

            target = local if exists and local is not None else _download(s, best)
            opened = False
            try:
                opened = webbrowser.open(target.as_uri())
            except Exception:
                opened = False
            if not s.out.json:
                if opened:
                    s.out.ok(f"opened {target}")
                else:
                    s.out.warn(f"could not start a browser; open {target}")


@report_app.command("statement")
def report_statement(
    account: str = typer.Argument(..., help="Account id"),
    td: str | None = TdOpt,
    home: str | None = HomeOpt,
    api: str | None = ApiOpt,
    as_json: bool = JsonOpt,
) -> None:
    """The text statement of an account (GET /settlement/{td}/statements/{account}), printed as is.

    The answer is ``{format: "text", text}`` (the statement file) or, when the file is not
    available, ``{format: "settlement", settlement}`` (shown as the account's settlement).
    """
    day = td_option(td)
    with session(home, api, as_json) as s:
        day = day or latest_settled_td(s)
        reply = s.api.get(f"/settlement/{day}/statements/{_seg(account)}")
        if s.out.json:
            s.out.raw(reply.raw)
            return
        data = reply.data
        doc = as_dict(data)
        if doc.get("format") == "text" and isinstance(doc.get("text"), str):
            s.out.text(doc["text"])
            return
        if isinstance(data, str):  # an older api-gateway: the text itself
            s.out.text(data)
            return
        s.out.warn(reply.message or f"no statement file for {account} on {day}: settlement data instead")
        res = as_dict(doc.get("settlement")) if doc.get("format") == "settlement" else doc
        accounts = [a for a in dicts(res.get("accounts")) if a.get("account_id") == account]
        if not accounts:
            raise CliError("NOT_FOUND", f"no settlement of {account} on {day}")
        _accounts_view(s, accounts)


__all__ = ["latest_settled_td", "report_app", "settle_app"]
