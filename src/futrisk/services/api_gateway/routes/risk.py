"""Risk routes: rules, state, events, comparison, manual risk actions (02 §9.6, §6.3; 01 §6.3).

Manual commands carry ``actor`` (API key id) and ``role`` so the risk shard can
re-check the role (02 §4.11). ``firm.kill_switch`` goes to every risk shard and
trade-gateway through the kill-switch manager (one persisted desired state;
targets that answer a retryable code are retried in the background, a newer
command supersedes older retries; see :mod:`..killswitch`),
``eod.force_unfreeze`` to every shard. Both answer HTTP 200 with the per-target
results: ``ok`` only when every target confirmed, ``code`` the worst target code.
"""

from __future__ import annotations

import asyncio
from typing import Any

from fastapi import APIRouter, Depends, Query, Request
from starlette.responses import Response

from futrisk.messages import codes
from futrisk.messages.base import to_builtins
from futrisk.messages.enums import Action, LiquidationStatus, ReconBreakStatus, RiskEventKind
from futrisk.messages.eod import ForceUnfreezeRequest, SettlementResult, SettleQuery
from futrisk.messages.risk import (
    AckAlertRequest,
    HaltRequest,
    KillSwitchRequest,
    LiquidationConfirm,
    LiquidationPlanBatch,
    QueryAlerts,
    QueryLiquidationPlans,
    QueryReconBreaks,
    QueryRules,
    ReconBreakBatch,
    ReconResolveRequest,
    ResetRequest,
    RiskEvent,
    RiskEventBatch,
    RuleSet,
)
from futrisk.services.api_gateway.auth import Perm, Principal
from futrisk.services.api_gateway.gateway import Gateway
from futrisk.services.api_gateway.models import (
    AckBody,
    ForceUnfreezeBody,
    HaltBody,
    KillSwitchBody,
    LiquidationConfirmBody,
    ReconResolveBody,
    ResetBody,
)
from futrisk.services.api_gateway.rest import ApiError, ApiResult
from futrisk.services.api_gateway.router import CLEARING
from futrisk.services.api_gateway.routes.accounts import check_account
from futrisk.services.api_gateway.routes.deps import Envelope, require
from futrisk.services.api_gateway.views import build_compare_view

MAX_HISTORY_DAYS = 60


def account_of_alert_key(alert_key: str) -> str | None:
    """Account of ``{td}|{rule_id}|{level}|{scope_key}|{episode}`` (None for firm scopes)."""
    parts = alert_key.split("|")
    if len(parts) < 5:
        return None
    scope = parts[3]
    if scope.startswith("acct:"):
        return scope[len("acct:"):].split("/", 1)[0]
    return None


def local_ruleset(gw: Gateway) -> dict[str, Any]:
    cfg = gw.config()
    return {
        "config_version": cfg.config_version,
        "config_hash": cfg.config_hash,
        "rules": [r.model_dump(mode="json") for r in cfg.risk_rules.rules],
    }


async def find_plan_owner(gw: Gateway, plan_id: str) -> str | None:
    acct = gw.readmodel.plan_account.get(plan_id)
    if acct is not None:
        return acct
    td = gw.trading_day()
    results = await gw.router.fan_out(gw.router.risk_instances(), "risk.query.alerts", QueryAlerts(trading_day=td),
                                      timeout_ms=2000, retries=0)
    for res in results:
        if not res.ok:
            continue
        batch = res.reply.open_as(RiskEventBatch)
        for ev in batch.items if batch else []:
            if ev.kind != RiskEventKind.LIQUIDATION_PLAN:
                continue
            plan = ev.detail.get("plan") if isinstance(ev.detail.get("plan"), dict) else ev.detail
            if isinstance(plan, dict) and plan.get("plan_id") == plan_id:
                return ev.account_id or (plan.get("account_id") if isinstance(plan.get("account_id"), str) else None)
    return None


async def count_breakers(gw: Gateway, trading_day: str) -> dict[str, int] | None:
    """Circuit breakers per account on ``trading_day`` (ALERT_RAISED with CIRCUIT_BREAK + MANUAL_HALT).

    None when a shard could not be asked (the dashboard then counts from ``/risk/events``).
    """
    results = await gw.router.fan_out(gw.router.risk_instances(), "risk.query.alerts",
                                      QueryAlerts(trading_day=trading_day), timeout_ms=2000, retries=0)
    counts: dict[str, int] = dict.fromkeys(gw.router.accounts(), 0)
    for res in results:
        if not res.ok:
            return None
        batch = res.reply.open_as(RiskEventBatch)
        for ev in batch.items if batch else []:
            if ev.account_id is None:
                continue
            breaker = (ev.kind == RiskEventKind.ALERT_RAISED and ev.action == Action.CIRCUIT_BREAK) or (
                ev.kind == RiskEventKind.MANUAL_HALT
            )
            if breaker:
                counts[ev.account_id] = counts.get(ev.account_id, 0) + 1
    return counts


def build(gw: Gateway) -> APIRouter:
    r = APIRouter(tags=["risk"])
    read = Depends(require(gw, Perm.READ))
    risk = Depends(require(gw, Perm.RISK))

    # ---------------------------------------------------------------- reads
    @r.get("/risk/rules", response_model=Envelope, summary="Effective rules (risk.query.rules)")
    async def rules(p: Principal = read) -> Response:
        targets = [gw.router.coordinator()] + [t for t in gw.router.risk_instances() if t != gw.router.coordinator()]
        for t in targets:
            reply = await gw.router.request(t, "risk.query.rules", QueryRules(), timeout_ms=1500, retries=0)
            if reply.ok and reply.open_as(RuleSet) is not None:
                return ApiResult.from_reply(reply).response()
        return ApiResult.success(local_ruleset(gw), "risk shards unavailable: rules from the local config").response()

    @r.get("/risk/state", response_model=Envelope, summary="Account states and restrictions (read model)")
    async def state(account: str | None = None, p: Principal = read) -> Response:
        rows = gw.readmodel.risk_state_rows()
        if account is not None:
            check_account(gw, account)
            rows = [x for x in rows if x["account_id"] == account]
        return ApiResult.success(rows).response()

    @r.get("/risk/events", response_model=Envelope,
           summary="Risk events of a day (risk.query.alerts + read model) and the state changes seen (read model)")
    async def events(td: str | None = Query(None, pattern=r"^\d{8}$"), account: str | None = None,
                     p: Principal = read) -> Response:
        day = td or gw.trading_day()
        if account is not None:
            check_account(gw, account)
            targets = [gw.router.owner_of(account)]
        else:
            targets = gw.router.risk_instances()
        results = await gw.router.fan_out(targets, "risk.query.alerts", QueryAlerts(trading_day=day, account_id=account),
                                          timeout_ms=2000, retries=0)
        merged: dict[str, RiskEvent] = {}
        errors: dict[str, str] = {}
        for res in results:
            if not res.ok:
                errors[res.target] = res.reply.code
                continue
            batch = res.reply.open_as(RiskEventBatch)
            for ev in batch.items if batch else []:
                merged[ev.event_id] = ev
        rm = gw.readmodel
        for ev in rm.recent_risk_events(trading_day=day, account_id=account):
            merged.setdefault(ev.event_id, ev)
        items = sorted(merged.values(), key=lambda e: e.ts_ns)
        changes = rm.recent_state_changes(trading_day=day, account_id=account)
        data = {
            "trading_day": day,
            "items": [rm.risk_event_item(e) for e in items],
            "state_changes": [rm.state_change_item(s) for s in changes],
            "errors": errors,
        }
        msg = f"partial: {', '.join(f'{k}={v}' for k, v in errors.items())}" if errors else ""
        return ApiResult.success(data, msg).response()

    @r.get("/risk/compare", response_model=Envelope, summary="Multi-account comparison (read model)")
    async def compare(p: Principal = read) -> Response:
        rm = gw.readmodel
        snaps = [rm.snapshots[a] for a in sorted(rm.snapshots)]
        view = build_compare_view(snaps, gw.config().risk_rules.rules, rm.liveness_of)
        return ApiResult.success(view).response()

    @r.get("/risk/compare/history", response_model=Envelope,
           summary="Settlement history (settle.query per day) with circuit-breaker counts")
    async def compare_history(days: int = Query(20, ge=1, le=MAX_HISTORY_DAYS), p: Principal = read) -> Response:
        tds = gw.recent_trading_days(days)
        sem = asyncio.Semaphore(6)

        async def one(day: str) -> tuple[str, Any, dict[str, int] | None]:
            async with sem:
                reply = await gw.router.request(CLEARING, "settle.query", SettleQuery(trading_day=day),
                                                timeout_ms=5000, retries=0)
                breakers = await count_breakers(gw, day) if reply.ok else None
                return day, reply, breakers

        out: list[dict[str, Any]] = []
        missing: list[str] = []
        errors: dict[str, str] = {}
        for day, reply, breakers in await asyncio.gather(*(one(x) for x in tds)):
            if reply.ok:
                res = reply.open_as(SettlementResult)
                if res is not None:
                    item: dict[str, Any] = {"trading_day": res.trading_day, "version": res.version,
                                            "accounts": to_builtins(res.accounts)}
                    if breakers is not None:
                        item["breakers"] = breakers
                    out.append(item)
                    continue
            if reply.code == codes.NOT_FOUND:
                missing.append(day)
            else:
                errors[day] = reply.code
        out.sort(key=lambda x: x["trading_day"])
        if errors and not out and len(errors) == len(tds):
            first = next(iter(errors.values()))
            return ApiResult.failure(first, "settlement history unavailable", {"errors": errors}).response()
        return ApiResult.success({"days": out, "missing": sorted(missing), "errors": errors}).response()

    @r.get("/risk/liquidation-plans", response_model=Envelope,
           summary="Liquidation plans of every shard (risk.query.liquidation_plans), ?account= &status= &td=")
    async def liquidation_plans(account: str | None = None, status: str | None = None,
                                td: str | None = Query(None, pattern=r"^\d{8}$"), p: Principal = read) -> Response:
        st: LiquidationStatus | None = None
        if status:
            try:
                st = LiquidationStatus(status)
            except ValueError as exc:
                raise ApiError(codes.BAD_REQUEST, f"status must be one of {', '.join(LiquidationStatus)}") from exc
        if account is not None:
            check_account(gw, account)
            targets = [gw.router.owner_of(account)]
        else:
            targets = gw.router.risk_instances()
        req = QueryLiquidationPlans(trading_day=td, account_id=account, status=st)
        results = await gw.router.fan_out(targets, "risk.query.liquidation_plans", req, timeout_ms=2000, retries=0)
        items: dict[str, Any] = {}
        errors: dict[str, str] = {}
        rm = gw.readmodel
        for res in results:
            batch = res.reply.open_as(LiquidationPlanBatch) if res.ok else None
            if batch is None:
                errors[res.target] = res.reply.code if not res.ok else codes.INTERNAL
                continue
            rm.merge_plans(batch.items)  # confirm routes by plan id; events keep them current
            for plan in batch.items:
                items[plan.plan_id] = to_builtins(plan)
        # a shard that did not answer: the plans the read model knows for its accounts, marked stale
        for target in errors:
            for plan in rm.plans_of(trading_day=td or gw.trading_day(),
                                    account_ids=[account] if account else gw.router.accounts_of(target),
                                    status=st.value if st is not None else None):
                plan.pop("updated_ns", None)
                items.setdefault(plan["plan_id"], {**plan, "stale": True})
        ordered = sorted(items.values(), key=lambda x: int(x.get("created_ns") or 0))
        data = {"trading_day": td or gw.trading_day(), "items": ordered, "errors": errors}
        msg = f"partial: {', '.join(f'{k}={v}' for k, v in errors.items())}" if errors else ""
        return ApiResult.success(data, msg).response()

    @r.get("/risk/recon-breaks", response_model=Envelope, summary="Reconciliation breaks of every shard")
    async def recon_breaks(account: str | None = None, status: str | None = None, p: Principal = read) -> Response:
        st = None
        if status:
            try:
                st = ReconBreakStatus(status)
            except ValueError as exc:
                raise ApiError(codes.BAD_REQUEST, f"unknown status {status!r}") from exc
        if account is not None:
            check_account(gw, account)
            targets = [gw.router.owner_of(account)]
        else:
            targets = gw.router.risk_instances()
        results = await gw.router.fan_out(targets, "risk.query.recon_breaks",
                                          QueryReconBreaks(account_id=account, status=st))
        items: list[Any] = []
        for res in results:
            if res.ok:
                batch = res.reply.open_as(ReconBreakBatch)
                items.extend(batch.items if batch else [])
        items.sort(key=lambda b: b.created_ns)
        ok, code, message = gw.router.summarize(results)
        data = {"items": to_builtins(items), "targets": [x.to_dict() | {"data": None} for x in results]}
        if not ok and not any(x.ok for x in results):
            return ApiResult.failure(code, message, data).response()
        return ApiResult.success(data, "" if ok else message).response()

    # -------------------------------------------------------------- actions
    @r.post("/risk/events/{alert_key:path}/ack", response_model=Envelope, summary="risk.ack_alert (RISK_MANAGER)")
    async def ack(alert_key: str, body: AckBody, request: Request, p: Principal = risk) -> Response:
        acct = account_of_alert_key(alert_key)

        async def run(cid: str) -> ApiResult:
            req = AckAlertRequest(command_id=cid, alert_key=alert_key, note=body.note, actor=p.key_id, role=p.role)
            if acct is not None and gw.router.has_account(acct):
                return ApiResult.from_reply(await gw.router.to_account(acct, "risk.ack_alert", req))
            res = await gw.router.first_owner([gw.router.coordinator()] + [
                t for t in gw.router.risk_instances() if t != gw.router.coordinator()], "risk.ack_alert", req)
            return ApiResult.from_reply(res.reply)

        return await gw.mutate(request, p, command="risk.ack_alert", target=alert_key,
                               body=body.model_dump(mode="json"), run=run)

    @r.post("/risk/accounts/{account_id}/reset", response_model=Envelope, summary="risk.reset (RISK_MANAGER)")
    async def reset(account_id: str, body: ResetBody, request: Request, p: Principal = risk) -> Response:
        check_account(gw, account_id)

        async def run(cid: str) -> ApiResult:
            req = ResetRequest(command_id=cid, account_id=account_id, rule_id=body.rule_id, level=body.level,
                               reason=body.reason, actor=p.key_id, role=p.role)
            return ApiResult.from_reply(await gw.router.to_account(account_id, "risk.reset", req))

        return await gw.mutate(request, p, command="risk.reset", target=account_id,
                               body=body.model_dump(mode="json"), run=run)

    @r.post("/risk/accounts/{account_id}/halt", response_model=Envelope, summary="risk.halt (RISK_MANAGER)")
    async def halt(account_id: str, body: HaltBody, request: Request, p: Principal = risk) -> Response:
        check_account(gw, account_id)

        async def run(cid: str) -> ApiResult:
            req = HaltRequest(command_id=cid, account_id=account_id, reason=body.reason, actor=p.key_id, role=p.role)
            return ApiResult.from_reply(await gw.router.to_account(account_id, "risk.halt", req))

        return await gw.mutate(request, p, command="risk.halt", target=account_id,
                               body=body.model_dump(mode="json"), run=run)

    @r.post("/risk/firm/kill-switch", response_model=Envelope,
            summary="firm.kill_switch to every shard and trade-gateway (RISK_MANAGER); HTTP 200 with per-target "
                    "results")
    async def kill_switch(body: KillSwitchBody, request: Request, p: Principal = risk) -> Response:
        async def run(cid: str) -> ApiResult:
            req = KillSwitchRequest(command_id=cid, on=body.on, reason=body.reason, actor=p.key_id, role=p.role)
            results = await gw.killswitch.command(req)
            control = next((x.to_dict()["data"] for x in results if x.ok), None)
            return gw.fanout_result(results, {"control": control, "retrying": gw.killswitch.pending(),
                                              "failed": gw.killswitch.failed()})

        # every outcome is recorded: a client retry must never re-assert a decision a newer one replaced
        return await gw.mutate(request, p, command="firm.kill_switch", target="firm",
                               body=body.model_dump(mode="json"), run=run, record_all=True)

    @r.post("/risk/recon-breaks/{break_id}/resolve", response_model=Envelope,
            summary="risk.recon.resolve (RISK_MANAGER)")
    async def resolve(break_id: str, body: ReconResolveBody, request: Request, p: Principal = risk) -> Response:
        async def run(cid: str) -> ApiResult:
            results = await gw.router.fan_out(gw.router.risk_instances(), "risk.query.recon_breaks",
                                              QueryReconBreaks(), timeout_ms=2000, retries=0)
            owner: str | None = None
            for res in results:
                batch = res.reply.open_as(ReconBreakBatch) if res.ok else None
                if batch is not None and any(b.break_id == break_id for b in batch.items):
                    owner = res.target
                    break
            if owner is None:
                unavailable = [x.target for x in results if not x.ok]
                if unavailable:
                    return ApiResult.failure(codes.UNAVAILABLE,
                                             f"break {break_id} not found; shards unavailable: {', '.join(unavailable)}")
                return ApiResult.failure(codes.NOT_FOUND, f"unknown reconciliation break {break_id}")
            req = ReconResolveRequest(command_id=cid, break_id=break_id, resolution=body.resolution, note=body.note,
                                      actor=p.key_id, role=p.role)
            return ApiResult.from_reply(await gw.router.request(owner, "risk.recon.resolve", req))

        return await gw.mutate(request, p, command="risk.recon.resolve", target=break_id,
                               body=body.model_dump(mode="json"), run=run)

    @r.post("/risk/liquidation-plans/{plan_id}/confirm", response_model=Envelope,
            summary="risk.liquidation.confirm (RISK_MANAGER)")
    async def confirm_plan(plan_id: str, body: LiquidationConfirmBody, request: Request,
                           p: Principal = risk) -> Response:
        async def run(cid: str) -> ApiResult:
            acct = await find_plan_owner(gw, plan_id)
            req = LiquidationConfirm(command_id=cid, plan_id=plan_id, decision=body.decision, actor=p.key_id,
                                     role=p.role)
            if acct is not None and gw.router.has_account(acct):
                return ApiResult.from_reply(await gw.router.to_account(acct, "risk.liquidation.confirm", req))
            return ApiResult.failure(codes.NOT_FOUND, f"unknown liquidation plan {plan_id}")

        return await gw.mutate(request, p, command="risk.liquidation.confirm", target=plan_id,
                               body=body.model_dump(mode="json"), run=run)

    @r.post("/risk/eod/{td}/force-unfreeze", response_model=Envelope,
            summary="eod.force_unfreeze to every shard (RISK_MANAGER)")
    async def force_unfreeze(td: str, body: ForceUnfreezeBody, request: Request, p: Principal = risk) -> Response:
        if len(td) != 8 or not td.isdigit():
            raise ApiError(codes.BAD_REQUEST, "td must be YYYYMMDD")

        async def run(cid: str) -> ApiResult:
            req = ForceUnfreezeRequest(command_id=cid, trading_day=td, reason=body.reason, actor=p.key_id,
                                       role=p.role)
            results = await gw.router.fan_out(gw.router.risk_instances(), "eod.force_unfreeze", req)
            return gw.fanout_result(results)

        return await gw.mutate(request, p, command="eod.force_unfreeze", target=td,
                               body=body.model_dump(mode="json"), run=run)

    return r


__all__ = ["account_of_alert_key", "build", "count_breakers", "find_plan_owner", "local_ruleset"]
