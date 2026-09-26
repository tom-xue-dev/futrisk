"""Demo routes (ADMIN, demo mode only): sim-exchange control proxy, clock jump, service kill (02 §9.5, §9.6, §8.8).

Every sim-exchange control call goes through these routes, so it carries an
API key and lands in ``api_audit`` (the sim control API itself has no
authentication and listens on 127.0.0.1 only):

==========================  =================================  ==========================
REST                        sim control API                    notes
==========================  =================================  ==========================
GET ``/demo/state``         GET ``/control/state``             faults, scenario, clock, pause
GET ``/demo/faults``        GET ``/control/state``             ``{items, history}`` view
POST ``/demo/faults``       POST ``/control/faults``
DELETE ``/demo/faults/{id}``  DELETE ``/control/faults/{id}``
POST ``/demo/shock``        POST ``/control/shock``
GET ``/demo/scenarios``     GET ``/control/scenarios``
POST ``/demo/scenario``     POST ``/control/scenario``
DELETE ``/demo/scenario``   DELETE ``/control/scenario``       cancel the running scenario
POST ``/demo/clock``        pause / ``svc.clock`` / clock / resume  02 §8.8 (:mod:`..clockjump`)
POST ``/demo/kill/{inst}``  (``svc.kill`` to the supervisor)
==========================  =================================  ==========================

The sim does not deduplicate, so every outcome of a mutating ``/demo`` call is
recorded under its ``Idempotency-Key`` -- including ``TIMEOUT`` / ``UNAVAILABLE``:
a client retry gets the recorded answer instead of injecting twice (use a new
key to try again).
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Request
from starlette.responses import Response

from futrisk.messages.system import SvcRequest
from futrisk.services.api_gateway.auth import Perm, Principal
from futrisk.services.api_gateway.gateway import Gateway, parse_market_time
from futrisk.services.api_gateway.models import DemoClockBody, DemoFaultBody, DemoScenarioBody, DemoShockBody
from futrisk.services.api_gateway.ports import SimControlError
from futrisk.services.api_gateway.rest import ApiResult
from futrisk.services.api_gateway.router import SUPERVISOR
from futrisk.services.api_gateway.routes.deps import Envelope, demo_only, require


async def sim_call(gw: Gateway, method: str, path: str, body: Any = None) -> ApiResult:
    try:
        return ApiResult.success(await gw.sim.call(method, path, body))
    except SimControlError as exc:
        return ApiResult.failure(exc.code, exc.message, exc.body if isinstance(exc.body, dict) else None)


def build(gw: Gateway) -> APIRouter:
    r = APIRouter(tags=["demo"])
    admin = Depends(require(gw, Perm.ADMIN))
    demo = Depends(demo_only(gw))

    # ------------------------------------------------------------------ reads
    @r.get("/demo/state", response_model=Envelope, summary="Sim-exchange state (sim GET /control/state)")
    async def state(p: Principal = admin, _: None = demo) -> Response:
        return (await sim_call(gw, "GET", "/state")).response()

    @r.get("/demo/faults", response_model=Envelope,
           summary="Active faults and fault history (from sim GET /control/state)")
    async def faults(p: Principal = admin, _: None = demo) -> Response:
        res = await sim_call(gw, "GET", "/state")
        if res.ok and isinstance(res.data, dict):
            res.data = {"items": res.data.get("faults") or [], "history": res.data.get("fault_history") or []}
        return res.response()

    @r.get("/demo/scenarios", response_model=Envelope, summary="Available scenarios (sim GET /control/scenarios)")
    async def scenarios(p: Principal = admin, _: None = demo) -> Response:
        return (await sim_call(gw, "GET", "/scenarios")).response()

    # -------------------------------------------------------------- mutations
    @r.post("/demo/faults", response_model=Envelope, summary="Inject a fault (sim POST /control/faults)")
    async def add_fault(body: DemoFaultBody, request: Request, p: Principal = admin, _: None = demo) -> Response:
        payload = body.model_dump(mode="json", exclude_none=True)
        return await gw.mutate(request, p, command="sim.faults", target=body.type, body=payload,
                               run=lambda cid: sim_call(gw, "POST", "/faults", payload), record_all=True)

    @r.delete("/demo/faults/{fault_id}", response_model=Envelope, summary="Remove a fault (sim DELETE)")
    async def delete_fault(fault_id: str, request: Request, p: Principal = admin, _: None = demo) -> Response:
        return await gw.mutate(request, p, command="sim.faults.delete", target=fault_id, body={},
                               run=lambda cid: sim_call(gw, "DELETE", f"/faults/{fault_id}"), record_all=True)

    @r.post("/demo/shock", response_model=Envelope, summary="Price shock (sim POST /control/shock)")
    async def shock(body: DemoShockBody, request: Request, p: Principal = admin, _: None = demo) -> Response:
        payload = body.model_dump(mode="json", exclude_none=True)
        return await gw.mutate(request, p, command="sim.shock", target=body.sector or ",".join(body.instruments or []),
                               body=payload, run=lambda cid: sim_call(gw, "POST", "/shock", payload),
                               record_all=True)

    @r.post("/demo/scenario", response_model=Envelope, summary="Run a scenario (sim POST /control/scenario)")
    async def scenario(body: DemoScenarioBody, request: Request, p: Principal = admin, _: None = demo) -> Response:
        payload = body.model_dump(mode="json", exclude_none=True)
        return await gw.mutate(request, p, command="sim.scenario", target=body.name, body=payload,
                               run=lambda cid: sim_call(gw, "POST", "/scenario", payload), record_all=True)

    @r.delete("/demo/scenario", response_model=Envelope,
              summary="Cancel the running scenario (sim DELETE /control/scenario)")
    async def cancel_scenario(request: Request, p: Principal = admin, _: None = demo) -> Response:
        return await gw.mutate(request, p, command="sim.scenario.cancel", target="scenario", body={},
                               run=lambda cid: sim_call(gw, "DELETE", "/scenario"), record_all=True)

    @r.post("/demo/clock", response_model=Envelope, summary="Market clock jump (02 §8.8 orchestration)")
    async def clock(body: DemoClockBody, request: Request, p: Principal = admin, _: None = demo) -> Response:
        at_ns = body.at_ns if body.at_ns is not None else parse_market_time(body.at)

        async def run(cid: str) -> ApiResult:
            res = await gw.clockjump.jump(command_id=cid, target=body.target, at_ns=at_ns, actor=p.key_id)
            return ApiResult(ok=res.ok, code=res.code, message=res.message, data=res.to_dict())

        return await gw.mutate(request, p, command="svc.clock", target=str(body.target),
                               body=body.model_dump(mode="json"), run=run, record_all=True)

    @r.post("/demo/kill/{instance}", response_model=Envelope, summary="svc.kill: simulate a crash")
    async def kill(instance: str, request: Request, p: Principal = admin, _: None = demo) -> Response:
        async def run(cid: str) -> ApiResult:
            reply = await gw.router.request(SUPERVISOR, "svc.kill",
                                            SvcRequest(command_id=cid, instance=instance, actor=p.key_id))
            return ApiResult.from_reply(reply)

        return await gw.mutate(request, p, command="svc.kill", target=instance, body={"instance": instance}, run=run,
                               record_all=True)

    return r


__all__ = ["build", "sim_call"]
