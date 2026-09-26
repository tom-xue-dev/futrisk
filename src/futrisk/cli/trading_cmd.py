"""``futrisk positions | accounts | order ... | risk ...``: books, orders and manual risk actions (02 §9.6).

=====================================  ==========================================================
command                                REST call (api-gateway ``/api/v1``)
=====================================  ==========================================================
``positions [filters] [--group-by]``   ``GET /positions?account=&product=&instrument=&exchange=&sector=&group_by=``
``accounts``                           ``GET /accounts`` (every configured account; ``missing``, ``stale``)
``order place``                        ``POST /orders`` (``client_order_id`` = ``Idempotency-Key`` = new UUID)
``order cancel <client_order_id>``     ``DELETE /orders/{id}?account=&reason=``
``order cancel-all --account``         ``POST /orders/cancel-all {account_id, scope, product?, reason}``
``order list --account [--active]``    ``GET /accounts/{id}/orders?td=&status=``
``risk state [--account]``             ``GET /risk/state?account=``
``risk events [--td] [--account]``     ``GET /risk/events?td=&account=``
``risk reset|halt <account>``          ``POST /risk/accounts/{id}/reset|halt {reason, ...}``
``risk ack <alert_key>``               ``POST /risk/events/{alert_key}/ack {note}``
``risk kill-switch on|off``            ``POST /risk/firm/kill-switch {on, reason}`` (HTTP 200, per target)
=====================================  ==========================================================

Snapshot money (equity, notional, P&L) is float in the API (02 §7.6) and printed as given;
ratios (P&L %, risk degree, % of equity) are shown as percentages.
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any, Literal

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
    pct,
    session,
    state_style,
    td_option,
    to_decimal,
)

order_app = typer.Typer(help="Place, cancel and list orders (TRADER)", no_args_is_help=True)
risk_app = typer.Typer(help="Risk state, events and manual risk actions (RISK_MANAGER)", no_args_is_help=True)

OFFSETS = {
    "open": "OPEN",
    "close": "CLOSE",
    "close-today": "CLOSE_TODAY",
    "close-yesterday": "CLOSE_YESTERDAY",
}
#: order statuses that can still change (``--active``); 02 §7.4
ACTIVE_STATUSES = ("ROUTING", "ROUTED", "ACCEPTED", "PARTIALLY_FILLED", "CANCEL_PENDING", "UNKNOWN")


def _seg(value: str) -> str:
    from futrisk.cli.apiclient import seg

    return seg(value)


def _require_ok(s: Session, reply: Any) -> None:
    """``--json``: print the envelope; exit 1 when it is not ok."""
    s.out.raw(reply.raw)
    if not reply.ok:
        raise typer.Exit(1)


# ====================================================================== positions
def positions(
    account: str | None = typer.Option(None, "--account", "-a", help="Only this account"),
    product: str | None = typer.Option(None, "--product", "-p", help="Only this product, e.g. rb"),
    instrument: str | None = typer.Option(None, "--instrument", "-i", help="Only this instrument, e.g. rb2701"),
    exchange: str | None = typer.Option(None, "--exchange", "-e", help="Only this exchange, e.g. SHFE"),
    sector: str | None = typer.Option(None, "--sector", "-s", help="Only this sector, e.g. black"),
    group_by: Literal["account", "product", "instrument", "exchange", "sector"] | None = typer.Option(
        None, "--group-by", "-g", case_sensitive=False, help="Aggregate by account|product|instrument|exchange|sector"
    ),
    home: str | None = HomeOpt,
    api: str | None = ApiOpt,
    as_json: bool = JsonOpt,
) -> None:
    """Positions across all accounts and shards (GET /positions), optionally grouped."""
    with session(home, api, as_json) as s:
        params = {
            "account": account,
            "product": product,
            "instrument": instrument,
            "exchange": exchange.upper() if exchange else None,
            "sector": sector,
            "group_by": group_by.lower() if group_by else None,
        }
        reply = s.api.get("/positions", params=params)
        if s.out.json:
            s.out.raw(reply.raw)
            return
        data = as_dict(reply.data)
        items = dicts(data.get("items"))
        groups = dicts(data.get("groups"))
        if group_by:
            s.out.table(
                ["GROUP", "POSITIONS", "LONG", "SHORT", "NET", "GROSS NOTIONAL", "NET NOTIONAL", "MARGIN", "POS P&L",
                 "ACCOUNTS", "INSTRUMENTS"],
                [
                    [g.get("group"), g.get("positions"), g.get("long_qty"), g.get("short_qty"), g.get("net_qty"),
                     g.get("gross_notional"), g.get("net_notional"), g.get("margin"), g.get("position_pnl"),
                     ",".join(map(str, g.get("accounts") or [])), ",".join(map(str, g.get("instruments") or []))]
                    for g in groups
                ],
                title=f"positions by {group_by.lower()} ({len(items)} position rows)",
            )
        else:
            s.out.table(
                ["ACCOUNT", "INSTRUMENT", "DIR", "TD", "YD", "CLOSABLE", "AVG OPEN", "LAST", "POS P&L", "MARGIN",
                 "NOTIONAL", "% EQUITY", "LOCK"],
                [
                    [p.get("account_id"), p.get("instrument_id"), (p.get("direction"), _dir_style(p.get("direction"))),
                     p.get("td_qty"), p.get("yd_qty"), p.get("closable_qty"), p.get("avg_open_price"), p.get("last"),
                     p.get("position_pnl"), p.get("margin"), p.get("notional"), pct(p.get("pct_equity")),
                     ("LOCKED", "red") if p.get("limit_locked") else ""]
                    for p in items
                ],
                title=f"positions ({len(items)})",
            )
        missing = data.get("accounts_missing") or []
        if missing:
            s.out.warn(f"accounts without a snapshot yet (not included): {', '.join(map(str, missing))}")
        stale = data.get("accounts_stale") or []
        if stale:
            s.out.warn(f"stale snapshots (owning shard not updating): {', '.join(map(str, stale))}")


def _dir_style(direction: Any) -> str:
    return {"LONG": "green", "SHORT": "red", "BUY": "green", "SELL": "red"}.get(str(direction), "")


# ====================================================================== accounts
def accounts(home: str | None = HomeOpt, api: str | None = ApiOpt, as_json: bool = JsonOpt) -> None:
    """Accounts overview: state, equity, daily P&L %, risk degree, snapshot age (GET /accounts)."""
    with session(home, api, as_json) as s:
        reply = s.api.get("/accounts")
        if s.out.json:
            s.out.raw(reply.raw)
            return
        items = dicts(reply.data)
        rows = []
        for a in items:
            funds = as_dict(a.get("funds"))
            metrics = as_dict(a.get("metrics"))
            state = str(a.get("state") or "")
            if a.get("halted_latched") and state != "HALTED":
                state += " (latched)"
            if a.get("trading_mode") not in (None, "OPEN"):
                state += f" {a.get('trading_mode')}"
            if a.get("missing"):  # listed from accounts.yaml, no snapshot yet (state from risk.state if any)
                state = f"{state} (no snapshot)".strip()
            as_of: Any = bj_time(a.get("as_of_ns"), date=False)
            if a.get("stale"):  # the owning shard stopped updating
                prefix = "" if as_of == "-" else f"{as_of} "
                as_of = (f"{prefix}stale {_age(a.get('age_ms'))}", "bold red")
            rows.append(
                [a.get("account_id"), a.get("name"), a.get("shard"), (state or "-", state_style(a.get("state"))),
                 funds.get("equity"), metrics.get("daily_pnl"), pct(metrics.get("daily_pnl_pct_init")),
                 pct(funds.get("risk_degree")), funds.get("margin"), funds.get("available"), a.get("open_orders"),
                 (a.get("quality"), state_style(a.get("quality"))), as_of]
            )
        days = sorted({str(a.get("trading_day")) for a in items if a.get("trading_day")})
        s.out.table(
            ["ACCOUNT", "NAME", "SHARD", "STATE", "EQUITY", "DAILY P&L", "P&L %", "RISK DEG", "MARGIN", "AVAILABLE",
             "ORDERS", "QUALITY", "AS OF"],
            rows,
            title=f"accounts ({len(rows)})" + (f", trading day {', '.join(days)}" if days else ""),
        )
        listed = {str(a.get("account_id")) for a in items}
        missing = sorted({str(a.get("account_id")) for a in items if a.get("missing")}
                         | (set(s.cfg.accounts.accounts) - listed))  # the second part: an older gateway
        if missing:
            s.out.warn(f"accounts without a snapshot yet: {', '.join(missing)}")


def _age(ms: Any) -> str:
    """``age_ms`` as ``12.3s`` / ``4m05s`` / ``2h07m``."""
    d = to_decimal(ms)
    if d is None or not d.is_finite():
        return "?"
    s = float(d) / 1000
    if s < 60:
        return f"{s:.1f}s"
    if s < 3600:
        return f"{int(s // 60)}m{int(s % 60):02d}s"
    return f"{int(s // 3600)}h{int(s % 3600 // 60):02d}m"


# ====================================================================== orders
def _price(value: str | None) -> float | None:
    if value is None:
        return None
    try:
        d = Decimal(value.strip())
    except InvalidOperation:
        raise typer.BadParameter(f"not a number: {value!r}", param_hint="--price") from None
    if not d.is_finite() or d <= 0:
        raise typer.BadParameter("must be a positive price", param_hint="--price")
    return float(d)


@order_app.command("place")
def order_place(
    account: str = typer.Option(..., "--account", "-a", help="Account id, e.g. ACC1"),
    instrument: str = typer.Option(..., "--instrument", "-i", help="Instrument id, e.g. rb2701"),
    side: Literal["buy", "sell"] = typer.Option(..., "--side", case_sensitive=False, help="buy | sell"),
    offset: Literal["open", "close", "close-today", "close-yesterday"] = typer.Option(
        ..., "--offset", case_sensitive=False, help="open | close | close-today | close-yesterday"
    ),
    qty: int = typer.Option(..., "--qty", "-q", min=1, help="Lots"),
    price: str | None = typer.Option(None, "--price", help="Limit price (required for limit orders)"),
    order_type: Literal["limit", "market"] = typer.Option(
        "limit", "--type", case_sensitive=False, help="limit | market"
    ),
    tif: Literal["gfd", "fak", "fok"] | None = typer.Option(
        None, "--tif", case_sensitive=False, help="Time in force (default: GFD)"
    ),
    home: str | None = HomeOpt,
    api: str | None = ApiOpt,
    as_json: bool = JsonOpt,
) -> None:
    """Submit one order (POST /orders); a rejected order prints its reject code and exits 1."""
    kind = order_type.lower()
    if kind == "limit" and price is None:
        raise typer.BadParameter("a limit order needs --price", param_hint="--price")
    if kind == "market" and price is not None:
        raise typer.BadParameter("a market order takes no --price", param_hint="--price")
    px = _price(price)
    with session(home, api, as_json) as s:
        from futrisk.cli.apiclient import new_idempotency_key

        coid = new_idempotency_key()
        body: dict[str, Any] = {
            "client_order_id": coid,
            "account_id": account,
            "instrument_id": instrument,
            "direction": side.upper(),
            "offset": OFFSETS[offset.lower()],
            "order_type": kind.upper(),
            "qty": qty,
        }
        if px is not None:
            body["price"] = px
        if tif:
            body["tif"] = tif.upper()
        reply = s.api.post("/orders", body, idempotency_key=coid, check=False)
        if s.out.json:
            _require_ok(s, reply)
            return
        ack = as_dict(reply.data)
        if reply.ok:
            dup = " (duplicate: already submitted)" if reply.duplicate else ""
            s.out.ok(f"order {coid} accepted{dup}")
            s.out.fields([
                ("status", ack.get("status")),
                ("trading day", ack.get("trading_day")),
                ("risk check", f"{fmt(ack.get('risk_check_us'))} us"),
            ])
            return
        if reply.status != 200:  # transport / auth / validation error; a rejected order is ok=false + HTTP 200
            raise reply.error()
        s.out.line(f"order {coid} REJECTED: {reply.code}", "bold red")
        reason = reply.message if reply.message and reply.message != reply.code else None
        detail = ack.get("reject_detail")
        pairs: list[tuple[str, Any]] = [("reject code", (ack.get("reject_code") or reply.code, "red"))]
        if reason:
            pairs.append(("reason", reason))
        if isinstance(detail, dict) and detail:
            pairs.append(("detail", kv(detail)))
        if ack.get("trading_day"):
            pairs.append(("trading day", ack.get("trading_day")))
        s.out.fields(pairs)
        raise typer.Exit(1)


@order_app.command("cancel")
def order_cancel(
    client_order_id: str = typer.Argument(..., help="client_order_id of the order"),
    account: str | None = typer.Option(None, "--account", "-a", help="Owning account (speeds up the lookup)"),
    reason: str | None = typer.Option(None, "--reason", help="Cancel reason"),
    home: str | None = HomeOpt,
    api: str | None = ApiOpt,
    as_json: bool = JsonOpt,
) -> None:
    """Cancel one order (DELETE /orders/{client_order_id})."""
    with session(home, api, as_json) as s:
        reply = s.api.delete(f"/orders/{_seg(client_order_id)}", params={"account": account, "reason": reason},
                             check=False)
        if s.out.json:
            _require_ok(s, reply)
            return
        if not reply.ok:
            raise reply.error()
        upd = as_dict(reply.data)
        note = f" ({reply.message})" if reply.message else ""
        s.out.ok(f"cancel of {client_order_id} requested: status {fmt(upd.get('status'))}{note}")


@order_app.command("cancel-all")
def order_cancel_all(
    account: str = typer.Option(..., "--account", "-a", help="Account id"),
    open_only: bool = typer.Option(False, "--open-only", help="Only opening orders (keep closing orders)"),
    product: str | None = typer.Option(None, "--product", "-p", help="Only orders of this product"),
    reason: str = typer.Option("cli cancel-all", "--reason", help="Cancel reason"),
    home: str | None = HomeOpt,
    api: str | None = ApiOpt,
    as_json: bool = JsonOpt,
) -> None:
    """Cancel every resting order of an account (POST /orders/cancel-all)."""
    with session(home, api, as_json) as s:
        body = {"account_id": account, "scope": "OPEN_ONLY" if open_only else "ALL", "reason": reason}
        if product:
            body["product"] = product
        reply = s.api.post("/orders/cancel-all", body)
        if s.out.json:
            s.out.raw(reply.raw)
            return
        count = reply.data.get("count") if isinstance(reply.data, dict) else None
        dup = " (duplicate)" if reply.duplicate else ""
        s.out.ok(f"cancel-all {account}: {fmt(count)} order(s) cancel-requested{dup}")


@order_app.command("list")
def order_list(
    account: str = typer.Option(..., "--account", "-a", help="Account id"),
    active: bool = typer.Option(False, "--active", help="Only orders that can still change"),
    td: str | None = TdOpt,
    home: str | None = HomeOpt,
    api: str | None = ApiOpt,
    as_json: bool = JsonOpt,
) -> None:
    """Order flow of one account (GET /accounts/{id}/orders)."""
    day = td_option(td)
    with session(home, api, as_json) as s:
        params = {"td": day, "status": ",".join(ACTIVE_STATUSES) if active else None}
        reply = s.api.get(f"/accounts/{_seg(account)}/orders", params=params)
        if s.out.json:
            s.out.raw(reply.raw)
            return
        items = reply.data.get("items") if isinstance(reply.data, dict) else reply.data
        orders = sorted((o for o in items or [] if isinstance(o, dict)), key=lambda o: int(o.get("created_ns") or 0))
        s.out.table(
            ["TIME", "CLIENT ORDER ID", "INSTRUMENT", "SIDE", "OFFSET", "TYPE", "PRICE", "QTY", "FILLED", "AVG PX",
             "STATUS", "REJECT", "SOURCE"],
            [
                [bj_time(o.get("created_ns"), date=False), o.get("client_order_id"), o.get("instrument_id"),
                 (o.get("direction"), _dir_style(o.get("direction"))), o.get("offset"), o.get("order_type"),
                 o.get("price"), o.get("qty"), o.get("filled_qty"), o.get("avg_fill_price"),
                 (o.get("status"), state_style(o.get("status"))), o.get("reject_code"), o.get("source")]
                for o in orders
            ],
            title=f"orders of {account}{' (active)' if active else ''}: {len(orders)}",
        )


# ====================================================================== risk
def _restrictions(items: Any) -> str:
    out = []
    for r in items or []:
        if isinstance(r, dict):
            value = f"={fmt(r.get('value'))}" if r.get("value") is not None else ""
            out.append(f"{r.get('kind')} {r.get('scope_key')}{value}")
    return "; ".join(out) or "-"


@risk_app.command("state")
def risk_state(
    account: str | None = typer.Option(None, "--account", "-a", help="Only this account"),
    home: str | None = HomeOpt,
    api: str | None = ApiOpt,
    as_json: bool = JsonOpt,
) -> None:
    """Account states and active restrictions (GET /risk/state?account=)."""
    with session(home, api, as_json) as s:
        reply = s.api.get("/risk/state", params={"account": account})
        answered = dicts(reply.data)
        rows = answered if account is None else [r for r in answered if r.get("account_id") == account]
        if account is not None and not rows:
            raise CliError("NOT_FOUND", f"no risk state for account {account} yet")
        if s.out.json:
            if len(rows) == len(answered):
                s.out.raw(reply.raw)
            else:  # an older gateway ignored ?account=: the same envelope, filtered here
                s.out.obj({"ok": True, "code": reply.code, "message": reply.message, "data": rows})
            return
        table = []
        for r in rows:
            last = r.get("last_change") if isinstance(r.get("last_change"), dict) else None
            change = "-"
            if last:
                by = f" by {last.get('actor')}" if last.get("actor") else ""
                change = (f"{last.get('from_state')}->{last.get('to_state')} {last.get('cause')}"
                          f" {bj_time(last.get('ts_ns'), date=False)}{by}")
            table.append(
                [r.get("account_id"), r.get("shard"), r.get("trading_day"), (r.get("state"), state_style(r.get("state"))),
                 r.get("halted_latched"), r.get("trading_mode"), _restrictions(r.get("restrictions")), change]
            )
        s.out.table(["ACCOUNT", "SHARD", "TD", "STATE", "LATCHED", "MODE", "RESTRICTIONS", "LAST CHANGE"], table,
                    title="risk state")


@risk_app.command("events")
def risk_events(
    td: str | None = TdOpt,
    account: str | None = typer.Option(None, "--account", "-a", help="Only this account"),
    limit: int = typer.Option(50, "--limit", "-n", min=1, help="Show the newest N events"),
    home: str | None = HomeOpt,
    api: str | None = ApiOpt,
    as_json: bool = JsonOpt,
) -> None:
    """Risk events of a trading day (GET /risk/events); the ALERT KEY column feeds `risk ack`."""
    day = td_option(td)
    with session(home, api, as_json) as s:
        reply = s.api.get("/risk/events", params={"td": day, "account": account})
        if s.out.json:
            s.out.raw(reply.raw)
            return
        data = as_dict(reply.data)
        items = dicts(data.get("items"))
        shown = items[-limit:]
        rows = []
        for e in shown:
            rule = "/".join(str(x) for x in (e.get("rule_id"), e.get("level")) if x) or None
            rows.append(
                [bj_time(e.get("ts_ns"), date=False), e.get("kind"), (e.get("severity"), state_style(e.get("severity"))),
                 e.get("account_id"), rule, e.get("action"), e.get("value"), e.get("threshold"), e.get("message"),
                 e.get("alert_key")]
            )
        more = f", newest {len(shown)} shown" if len(shown) < len(items) else ""
        s.out.table(
            ["TIME", "KIND", "SEVERITY", "ACCOUNT", "RULE/LEVEL", "ACTION", "VALUE", "THRESHOLD", "MESSAGE",
             "ALERT KEY"],
            rows,
            title=f"risk events of {data.get('trading_day') or day or '-'}: {len(items)}{more}",
        )
        errors = data.get("errors")
        if isinstance(errors, dict) and errors:
            s.out.warn(f"partial result: {kv(errors)}")


@risk_app.command("reset")
def risk_reset(
    account: str = typer.Argument(..., help="Account id"),
    reason: str = typer.Option(..., "--reason", "-r", help="Why (audited)"),
    rule_id: str | None = typer.Option(None, "--rule", help="Only this rule (default: every latched level)"),
    level: str | None = typer.Option(None, "--level", help="Only this level of --rule"),
    home: str | None = HomeOpt,
    api: str | None = ApiOpt,
    as_json: bool = JsonOpt,
) -> None:
    """Reset latched circuit breakers of an account (POST /risk/accounts/{id}/reset)."""
    with session(home, api, as_json) as s:
        body: dict[str, Any] = {"reason": reason}
        if rule_id:
            body["rule_id"] = rule_id
        if level:
            body["level"] = level
        reply = s.api.post(f"/risk/accounts/{_seg(account)}/reset", body)
        if s.out.json:
            s.out.raw(reply.raw)
            return
        ack = as_dict(reply.data)
        extra = f", re-arm threshold {fmt(ack.get('rearm_threshold'))}" if ack.get("rearm_threshold") is not None else ""
        msg = f" ({reply.message})" if reply.message else ""
        s.out.ok(f"reset {account}: state {fmt(ack.get('account_state'))}{extra}{msg}")


@risk_app.command("halt")
def risk_halt(
    account: str = typer.Argument(..., help="Account id"),
    reason: str = typer.Option(..., "--reason", "-r", help="Why (audited)"),
    home: str | None = HomeOpt,
    api: str | None = ApiOpt,
    as_json: bool = JsonOpt,
) -> None:
    """Manual circuit breaker: halt an account (POST /risk/accounts/{id}/halt)."""
    with session(home, api, as_json) as s:
        reply = s.api.post(f"/risk/accounts/{_seg(account)}/halt", {"reason": reason})
        if s.out.json:
            s.out.raw(reply.raw)
            return
        ch = as_dict(reply.data)
        latched = " (latched)" if ch.get("halted_latched") else ""
        s.out.ok(f"halt {account}: {fmt(ch.get('from_state'))} -> {fmt(ch.get('to_state'))}{latched}")


@risk_app.command("ack")
def risk_ack(
    alert_key: str = typer.Argument(..., help="Alert key from `futrisk risk events`, e.g. 20260928|daily_loss|halt|acct:ACC3|1"),
    note: str | None = typer.Option(None, "--note", help="Acknowledgement note"),
    home: str | None = HomeOpt,
    api: str | None = ApiOpt,
    as_json: bool = JsonOpt,
) -> None:
    """Acknowledge an alert (POST /risk/events/{alert_key}/ack); the rule state is untouched."""
    with session(home, api, as_json) as s:
        reply = s.api.post(f"/risk/events/{_seg(alert_key)}/ack", {"note": note} if note else {})
        if s.out.json:
            s.out.raw(reply.raw)
            return
        dup = " (duplicate)" if reply.duplicate else ""
        s.out.ok(f"acknowledged {alert_key}{dup}")


@risk_app.command("kill-switch")
def risk_kill_switch(
    switch: Literal["on", "off"] = typer.Argument(..., case_sensitive=False, help="on | off"),
    reason: str = typer.Option(..., "--reason", "-r", help="Why (audited)"),
    home: str | None = HomeOpt,
    api: str | None = ApiOpt,
    as_json: bool = JsonOpt,
) -> None:
    """Firm-wide kill switch to every risk shard and trade-gateway (POST /risk/firm/kill-switch).

    The answer is always HTTP 200 with a result per target; ``ok`` only when every target
    confirmed (exit 1 otherwise). Targets that did not confirm are retried by api-gateway.
    """
    on = switch.lower() == "on"
    with session(home, api, as_json) as s:
        reply = s.api.post("/risk/firm/kill-switch", {"on": on, "reason": reason}, check=False)
        if s.out.json:
            _require_ok(s, reply)
            return
        data = as_dict(reply.data)
        targets = dicts(data.get("targets"))
        if not targets and not reply.ok:
            raise reply.error()
        retrying = [str(t) for t in data.get("retrying") or []]
        failed = [str(t) for t in data.get("failed") or []]
        if targets:
            rows = []
            for t in targets:
                name = str(t.get("target"))
                if t.get("ok"):
                    result: tuple[str, str] = ("ok", "green")
                elif name in retrying and name not in failed:
                    result = ("retrying", "yellow")
                else:
                    result = ("FAILED", "bold red")
                rows.append([name, result, t.get("code"), t.get("message")])
            dup = " (duplicate: the recorded answer)" if reply.duplicate else ""
            s.out.table(["TARGET", "RESULT", "CODE", "MESSAGE"], rows, title=f"kill switch {'ON' if on else 'OFF'}{dup}")
        if retrying:
            s.out.warn(f"api-gateway keeps retrying: {', '.join(retrying)}")
        if failed:
            s.out.line(f"failed, not retried (non-retryable answer or retries used up): {', '.join(failed)}",
                       "bold red")
        if not reply.ok:
            raise CliError(reply.code, reply.message or reply.code, status=reply.status)
        s.out.ok(f"kill switch {'ON' if on else 'OFF'}: {reply.message or 'all targets confirmed'}")


__all__ = ["accounts", "order_app", "positions", "risk_app"]
