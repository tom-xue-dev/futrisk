"""System routes: health, service overview/status and service control (02 §9.6).

``GET /system/status`` also reports what an operator must act on:
``kill_switch_control`` (the desired manual kill-switch state with its
``pending`` / ``failed`` targets), ``auth`` (named keys missing or rejected,
bootstrap key active), ``notifications`` (row counts per status, incl.
``SUPPRESSED``) and ``readmodel.repair`` (FACT gaps / resyncs).
"""

from __future__ import annotations

import os
from typing import Any

from fastapi import APIRouter, Depends, Request
from starlette.responses import Response

from futrisk import __version__
from futrisk.messages.base import to_builtins
from futrisk.messages.system import PsRequest, ServiceList, SvcRequest
from futrisk.services.api_gateway.auth import Perm, Principal
from futrisk.services.api_gateway.gateway import Gateway
from futrisk.services.api_gateway.rest import ApiResult
from futrisk.services.api_gateway.router import SUPERVISOR
from futrisk.services.api_gateway.routes.deps import Envelope, require
from futrisk.services.api_gateway.schema import NotificationStatus


def service_items(gw: Gateway, infos: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    """ServiceInfo (supervisor, when reachable) merged with the latest heartbeat of every instance."""
    rm = gw.readmodel
    names = set(infos) | set(rm.heartbeats) | set(rm.services) | set(gw.config().services.instances)
    items = []
    for inst in sorted(names):
        item: dict[str, Any] = {"instance": inst}
        info = infos.get(inst)
        proc = rm.services.get(inst)
        seen = rm.heartbeats.get(inst)
        if info is not None:
            item.update(info)
            item["process_state"] = info.get("state")
        elif proc is not None:
            item.update(service=proc.service, state=proc.state, pid=proc.pid, restarts=proc.restarts,
                        last_exit_code=proc.exit_code, process_state=proc.state)
        # since when the process state holds: the supervisor's own value when it sends one, else the last
        # sys.service transition, else the process start
        since = info.get("since_ns") if info is not None else None
        if since is None and proc is not None:
            since = proc.ts_ns
        if since is None and info is not None:
            since = info.get("started_ns")
        item["since_ns"] = since
        item["heartbeat_ns"] = seen.recv_ns if seen is not None else None
        if seen is not None:
            hb = seen.heartbeat
            item.setdefault("service", hb.service)
            item["heartbeat_state"] = str(hb.state)
            item["last_heartbeat_ns"] = seen.recv_ns
            item["heartbeat"] = to_builtins(hb)
            item.setdefault("pid", hb.pid)
            item.setdefault("started_ns", hb.started_ns)
        item.setdefault("service", inst.rsplit("-", 1)[0] if inst.rsplit("-", 1)[-1].isdigit() else inst)
        item.setdefault("restarts", 0)
        if gw.liveness is not None:
            item["liveness"] = gw.liveness(inst)
        items.append(item)
    return items


def build(gw: Gateway) -> APIRouter:
    r = APIRouter(tags=["system"])
    read = Depends(require(gw, Perm.READ))
    admin = Depends(require(gw, Perm.ADMIN))

    @r.get("/health", response_model=Envelope,
           summary="Local liveness and identity (public): home_id, pid, epoch -- no secrets")
    async def health() -> Response:
        return ApiResult.success(
            {
                "status": "ok",
                "instance": gw.instance,
                "state": gw.state(),
                "home_id": gw.home_id,
                "pid": os.getpid(),
                "epoch": gw.epoch,
                "version": __version__,
                "trading_day": gw.trading_day(),
                "ts_ns": gw.clock.now_ns(),
            }
        ).response()

    @r.get("/auth/me", response_model=Envelope, summary="The caller's key id, name and role")
    async def me(p: Principal = read) -> Response:
        return ApiResult.success({"key_id": p.key_id, "name": p.name, "role": p.role.value}).response()

    @r.get("/system/services", response_model=Envelope, summary="Read model (heartbeats, sys.service) + svc.ps")
    async def system_services(p: Principal = read) -> Response:
        reply = await gw.router.request(SUPERVISOR, "svc.ps", PsRequest(), timeout_ms=1000, retries=0)
        infos: dict[str, dict[str, Any]] = {}
        if reply.ok:
            lst = reply.open_as(ServiceList)
            if lst is not None:
                infos = {i.instance: to_builtins(i) for i in lst.items}
        data = {
            "items": service_items(gw, infos),
            "events": to_builtins(list(gw.readmodel.service_events)[-100:]),
            "supervisor": {"ok": reply.ok, "code": reply.code, "message": reply.message or ""},
        }
        return ApiResult.success(data).response()

    @r.get("/system/status", response_model=Envelope, summary="Read-model summary")
    async def system_status(p: Principal = read) -> Response:
        rm = gw.readmodel
        cfg = gw.config()
        shards: dict[str, Any] = {}
        modes: set[str] = set()
        for inst in gw.router.risk_instances():
            hb = rm.heartbeat_of(inst)
            if hb is not None:
                shards[inst] = {
                    "state": str(hb.state),
                    "trading_day": hb.trading_day,
                    "trading_mode": str(hb.trading_mode) if hb.trading_mode is not None else None,
                }
                if hb.trading_mode is not None:
                    modes.add(str(hb.trading_mode))
            else:
                shards[inst] = None
        up = [i for i in cfg.services.instances if gw.liveness is None or gw.liveness(i) == "UP"]
        trading_mode = None
        for m in ("PROVISIONAL", "EOD_FROZEN", "OPEN"):
            if m in modes:
                trading_mode = m
                break
        try:
            notif_counts = await gw.store.count_notifications_by_status()
        except Exception:  # pragma: no cover - database closed during shutdown
            notif_counts = {}
        report = gw.keys.last_report
        control = gw.firm_control_state()  # MANUAL (api-gateway's desired state) + RULE (RULE_STATE)
        data = {
            "instance": gw.instance,
            "state": gw.state(),
            "mode": cfg.app.mode,
            "trading_day": gw.trading_day(),
            "trading_mode": trading_mode,
            "kill_switch": (bool(rm.firm.kill_switch) if rm.firm is not None
                            else (control["kill_switch"] if control is not None else None)),
            "firm_control": control,
            "kill_switch_retry": gw.kill_switch_retry(),
            "kill_switch_control": gw.killswitch.status(),
            "clock": gw.clock_state(),
            "config": {"config_version": cfg.config_version, "config_hash": cfg.config_hash},
            "services": {"total": len(cfg.services.instances), "up": len(up),
                         "down": sorted(set(cfg.services.instances) - set(up))},
            "shards": shards,
            "readmodel": {
                "synced": gw.synced() if gw.synced is not None else {},
                "accounts": len(rm.snapshots),
                "instruments": len(rm.ticks),
                "applied": rm.stats.applied,
                "decode_errors": rm.stats.decode_errors,
                "accounts_stale": rm.stale_accounts(),
                "repair": gw.repair.stats() if gw.repair is not None else None,
            },
            "ws": gw.hub.stats(),
            "auth": {**(report.to_dict() if report is not None else {}),
                     "throttled": gw.auth_throttle.rejected},
            "notifications": {
                "failed": notif_counts.get(NotificationStatus.FAILED.value, 0),
                "suppressed": notif_counts.get(NotificationStatus.SUPPRESSED.value, 0),
                "pending": notif_counts.get(NotificationStatus.PENDING.value, 0),
                "counts": notif_counts,
                **(gw.notifier.status() if gw.notifier is not None else {}),
            },
            "service_events": to_builtins(list(rm.service_events)[-50:]),
        }
        return ApiResult.success(data).response()

    def control(action: str) -> Any:
        async def handler(instance: str, request: Request, p: Principal = admin) -> Response:
            command = f"svc.{action}"

            async def run(cid: str) -> ApiResult:
                reply = await gw.router.request(
                    SUPERVISOR, command, SvcRequest(command_id=cid, instance=instance, actor=p.key_id)
                )
                return ApiResult.from_reply(reply)

            return await gw.mutate(request, p, command=command, target=instance, body={"instance": instance}, run=run)

        handler.__name__ = f"service_{action}"
        return handler

    for action in ("start", "stop", "restart"):
        r.add_api_route(
            f"/services/{{instance}}/{action}",
            control(action),
            methods=["POST"],
            response_model=Envelope,
            summary=f"svc.{action} (ADMIN)",
        )
    return r


__all__ = ["build", "service_items"]
