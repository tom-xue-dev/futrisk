"""Account, position and order routes (02 §9.6).

Reads come from the read model (``risk.snapshot.*``) or from the owning shard
(``risk.query.*``); writes are routed to the shard owning the account
(``accounts.yaml``), carrying ``actor`` = API key id and ``role`` = its role so
that the shard can re-check the role (02 §4.11).
"""

from __future__ import annotations

import asyncio
from typing import Any

from fastapi import APIRouter, Depends, Query, Request
from starlette.responses import Response

from futrisk.messages import codes
from futrisk.messages.base import to_builtins
from futrisk.messages.enums import OrderSource, OrderStatus
from futrisk.messages.orders import CancelAllRequest, CancelRequest, OrderAck, OrderRequest
from futrisk.messages.risk import (
    AccountSnapshotBatch,
    CashMoveRequest,
    OrderBatch,
    PositionBatch,
    QueryAccounts,
    QueryFills,
    QueryMetrics,
    QueryOrders,
    QueryPositions,
)
from futrisk.services.api_gateway.auth import Perm, Principal
from futrisk.services.api_gateway.gateway import Gateway
from futrisk.services.api_gateway.models import CancelAllBody, CancelBody, CashBody, OrderBody
from futrisk.services.api_gateway.rest import ApiError, ApiResult
from futrisk.services.api_gateway.router import UnknownAccount
from futrisk.services.api_gateway.routes.deps import Envelope, require
from futrisk.services.api_gateway.views import GROUP_BY, positions_view

TD = Query(None, pattern=r"^\d{8}$", description="trading day YYYYMMDD (default: current)")


def check_account(gw: Gateway, account_id: str) -> None:
    if not gw.router.has_account(account_id):
        raise ApiError(codes.NOT_FOUND, f"unknown account {account_id!r}")


def with_name(gw: Gateway, item: dict[str, Any]) -> dict[str, Any]:
    """Add the account's display name from accounts.yaml (``name``)."""
    spec = gw.config().accounts.accounts.get(str(item.get("account_id")))
    if spec is not None:
        item["name"] = spec.name
    return item


async def find_order_owner(gw: Gateway, client_order_id: str, account: str | None) -> str | None:
    """Account of an order: read model, then ``?account=``, then ``risk.query.orders`` of every account."""
    acct = gw.readmodel.order_account.get(client_order_id)
    if acct is not None:
        return acct
    if account is not None:
        return account if gw.router.has_account(account) else None
    accounts = gw.router.accounts()

    async def query(a: str) -> None:
        td = gw.account_td(a)
        reply = await gw.router.to_account(
            a, "risk.query.orders", QueryOrders(account_id=a, trading_day=td), timeout_ms=2000, retries=0
        )
        if reply.ok:
            batch = reply.open_as(OrderBatch)
            if batch is not None:
                gw.readmodel.merge_orders(a, batch.items, trading_day=td)

    await asyncio.gather(*(query(a) for a in accounts))
    return gw.readmodel.order_account.get(client_order_id)


def build(gw: Gateway) -> APIRouter:
    r = APIRouter(tags=["accounts"])
    read = Depends(require(gw, Perm.READ))
    trade = Depends(require(gw, Perm.TRADE))
    admin = Depends(require(gw, Perm.ADMIN))

    # ------------------------------------------------------------ accounts
    @r.get("/accounts", response_model=Envelope,
           summary="Every configured account: id, name, shard (accounts.yaml) + its latest AccountSnapshot")
    async def accounts(p: Principal = read) -> Response:
        rm = gw.readmodel
        specs = gw.config().accounts.accounts
        items: list[dict[str, Any]] = []
        for a in sorted(set(specs) | set(rm.snapshots)):
            snap = rm.account_snapshot(a)
            if snap is not None:
                items.append(with_name(gw, snap) | {"missing": False})
                continue
            spec = specs[a]
            item: dict[str, Any] = {"account_id": a, "name": spec.name, "shard": spec.shard, "missing": True}
            eff = rm.effective_state(a)  # a risk.state may be known without a snapshot
            if eff is not None:
                item.update(state=str(eff["state"]), halted_latched=eff["halted_latched"],
                            state_version=eff["state_version"])
            items.append(item | rm.liveness(a))
        return ApiResult.success(items).response()

    @r.get("/accounts/{account_id}", response_model=Envelope, summary="AccountSnapshot (read model)")
    async def account(account_id: str, p: Principal = read) -> Response:
        check_account(gw, account_id)
        snap = gw.readmodel.account_snapshot(account_id)
        if snap is not None:
            return ApiResult.success(with_name(gw, snap)).response()
        reply = await gw.router.to_account(account_id, "risk.query.accounts", QueryAccounts(account_ids=[account_id]))
        if not reply.ok:
            return ApiResult.from_reply(reply).response()
        batch = reply.open_as(AccountSnapshotBatch)
        items = [s for s in (batch.items if batch else []) if s.account_id == account_id]
        if not items:
            raise ApiError(codes.NOT_FOUND, f"no snapshot for {account_id} yet")
        return ApiResult.success(with_name(gw, to_builtins(items[0]))).response()

    @r.get("/accounts/{account_id}/positions", response_model=Envelope, summary="Positions of one account")
    async def positions_of(account_id: str, p: Principal = read) -> Response:
        check_account(gw, account_id)
        snap = gw.readmodel.snapshots.get(account_id)
        if snap is not None:
            return ApiResult.success(to_builtins(snap.positions)).response()
        # no snapshot in the read model yet: ask the owning shard
        reply = await gw.router.to_account(account_id, "risk.query.positions", QueryPositions(account_id=account_id))
        if not reply.ok:
            return ApiResult.from_reply(reply).response()
        batch = reply.open_as(PositionBatch)
        return ApiResult.success(to_builtins(batch.items if batch else [])).response()

    @r.get("/accounts/{account_id}/orders", response_model=Envelope, summary="Order flow (risk.query.orders)")
    async def orders(account_id: str, td: str | None = TD, status: str | None = None, p: Principal = read) -> Response:
        check_account(gw, account_id)
        st = None
        if status:
            try:
                st = [OrderStatus(s.strip()) for s in status.split(",") if s.strip()]
            except ValueError as exc:
                raise ApiError(codes.BAD_REQUEST, f"unknown status in {status!r}") from exc
        current = gw.account_td(account_id)
        req = QueryOrders(account_id=account_id, trading_day=td or current, status=st)
        reply = await gw.router.to_account(account_id, "risk.query.orders", req)
        if reply.ok and req.trading_day == current:  # history of other days stays out of the live model
            batch = reply.open_as(OrderBatch)
            if batch is not None:
                gw.readmodel.merge_orders(account_id, batch.items, trading_day=current)
        return ApiResult.from_reply(reply).response()

    @r.get("/accounts/{account_id}/trades", response_model=Envelope, summary="Applied fills (risk.query.fills)")
    async def trades(account_id: str, td: str | None = TD, p: Principal = read) -> Response:
        check_account(gw, account_id)
        req = QueryFills(account_id=account_id, trading_day=td or gw.account_td(account_id))
        return ApiResult.from_reply(await gw.router.to_account(account_id, "risk.query.fills", req)).response()

    @r.get("/accounts/{account_id}/equity-curve", response_model=Envelope, summary="Metric series (risk.query.metrics)")
    async def equity_curve(account_id: str, td: str | None = TD, step_s: int = Query(5, ge=1, le=3600),
                           p: Principal = read) -> Response:
        check_account(gw, account_id)
        req = QueryMetrics(account_id=account_id, trading_day=td or gw.account_td(account_id), step_s=step_s)
        return ApiResult.from_reply(await gw.router.to_account(account_id, "risk.query.metrics", req)).response()

    @r.post("/accounts/{account_id}/cash", response_model=Envelope, summary="Deposit / withdraw (cash.move, ADMIN)")
    async def cash(account_id: str, body: CashBody, request: Request, p: Principal = admin) -> Response:
        check_account(gw, account_id)

        async def run(movement_id: str) -> ApiResult:
            req = CashMoveRequest(
                movement_id=movement_id,
                account_id=account_id,
                type=body.type,
                amount=body.amount,
                reason=body.reason,
                actor=p.key_id,
                role=p.role,
                requested_ns=gw.clock.now_ns(),
            )
            return ApiResult.from_reply(await gw.router.to_account(account_id, "cash.move", req))

        return await gw.mutate(request, p, command="cash.move", target=account_id,
                               body=body.model_dump(mode="json"), run=run)

    # ----------------------------------------------------------- positions
    @r.get("/positions", response_model=Envelope, summary="Positions across shards with optional group_by")
    async def positions(
        account: str | None = None,
        product: str | None = None,
        instrument: str | None = None,
        exchange: str | None = None,
        sector: str | None = None,
        group_by: str | None = Query(None, description=" | ".join(GROUP_BY)),
        p: Principal = read,
    ) -> Response:
        if group_by is not None and group_by not in GROUP_BY:
            raise ApiError(codes.BAD_REQUEST, f"group_by must be one of {', '.join(GROUP_BY)}")
        rm = gw.readmodel
        snaps = [rm.snapshots[a] for a in sorted(rm.snapshots)]
        data = positions_view(
            snaps,
            gw.config().products,
            group_by=group_by,
            account=account,
            product=product,
            instrument=instrument,
            exchange=exchange,
            sector=sector,
        )
        configured = set(gw.router.accounts())
        data["accounts_missing"] = sorted(configured - set(rm.snapshots))
        data["accounts_stale"] = [a for a in sorted(rm.snapshots) if rm.liveness(a).get("stale")]
        return ApiResult.success(data).response()

    # -------------------------------------------------------------- orders
    @r.post("/orders", response_model=Envelope, summary="order.submit (TRADER; client_order_id is the idempotency key)")
    async def submit(body: OrderBody, request: Request, p: Principal = trade) -> Response:
        gw.set_audit(request, command="order.submit", command_id=body.client_order_id, target=body.account_id)
        req = OrderRequest(
            client_order_id=body.client_order_id,
            account_id=body.account_id,
            instrument_id=body.instrument_id,
            direction=body.direction,
            offset=body.offset,
            order_type=body.order_type,
            tif=body.tif,
            price=body.price,
            qty=body.qty,
            source=body.source or OrderSource.MANUAL,
            strategy_tag=body.strategy_tag,
            actor=p.key_id,
            sent_ns=gw.clock.now_ns(),
        )
        try:
            reply = await gw.router.to_account(body.account_id, "order.submit", req)
        except UnknownAccount:
            ack = OrderAck(
                client_order_id=body.client_order_id,
                accepted=False,
                status=OrderStatus.REJECTED,
                reject_code=codes.REJECT_UNKNOWN_ACCOUNT,
                trading_day=gw.trading_day(),
            )
            res = ApiResult.failure(codes.REJECT_UNKNOWN_ACCOUNT, f"unknown account {body.account_id}", to_builtins(ack))
        else:
            res = ApiResult.from_reply(reply)
        request.state.result_code = res.code
        return res.response()

    @r.post("/orders/cancel-all", response_model=Envelope, summary="order.cancel_all (TRADER)")
    async def cancel_all(body: CancelAllBody, request: Request, p: Principal = trade) -> Response:
        check_account(gw, body.account_id)

        async def run(cid: str) -> ApiResult:
            req = CancelAllRequest(command_id=cid, account_id=body.account_id, scope=body.scope, product=body.product,
                                   reason=body.reason, actor=p.key_id)
            return ApiResult.from_reply(await gw.router.to_account(body.account_id, "order.cancel_all", req))

        return await gw.mutate(request, p, command="order.cancel_all", target=body.account_id,
                               body=body.model_dump(mode="json"), run=run)

    @r.delete("/orders/{client_order_id}", response_model=Envelope, summary="order.cancel (TRADER)")
    async def cancel(client_order_id: str, request: Request,
                     account: str | None = Query(None, description="owning account (optional)"),
                     reason: str | None = None, p: Principal = trade) -> Response:
        body: dict[str, Any] = {"account": account, "reason": reason}
        raw = await request.body()
        if raw:
            try:
                extra = CancelBody.model_validate_json(raw)
                reason = reason or extra.reason
                body["reason"] = reason
            except ValueError as exc:
                raise ApiError(codes.BAD_REQUEST, f"bad body: {exc}") from exc

        async def run(cid: str) -> ApiResult:
            owner = await find_order_owner(gw, client_order_id, account)
            if owner is None:
                return ApiResult.failure(codes.NOT_FOUND, f"unknown order {client_order_id} (pass ?account=)")
            req = CancelRequest(command_id=cid, client_order_id=client_order_id, account_id=owner, reason=reason)
            return ApiResult.from_reply(await gw.router.to_account(owner, "order.cancel", req))

        return await gw.mutate(request, p, command="order.cancel", target=client_order_id, body=body, run=run)

    return r


__all__ = ["build", "check_account", "find_order_owner", "with_name"]
