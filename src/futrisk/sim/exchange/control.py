"""Control API of the virtual exchange (02 §9.5), served only when ``app.mode=demo``.

``http://{host}:{port}/control`` (127.0.0.1 only). Answers are plain JSON documents as
listed in 02 §9.5; errors are ``{"ok": false, "code", "message"}`` with HTTP 400 / 404 /
409 (``code`` from 02 §11.1).

| Method and path | Body | Answer |
|---|---|---|
| ``GET /control/state`` | - | ``{epoch, trading_day, faults, scenario, prices, ...}`` |
| ``POST /control/faults`` | ``{type, target?, params, duration_s?}`` | ``{fault_id, ...}`` |
| ``DELETE /control/faults/{fault_id}`` | - | ``{ok}`` |
| ``POST /control/shock`` | ``{instruments?, sector?, pct, duration_s, shape}`` | ``{shock_id, ...}`` |
| ``GET /control/scenarios`` | - | ``[{name, description}]`` |
| ``POST /control/scenario`` | ``{name, speed?, vars?}`` | ``{run_id}`` |
| ``DELETE /control/scenario`` | - | the cancelled run (extension) |
| ``POST /control/pause`` | - | ``{ok}`` |
| ``POST /control/clock`` | ``{offset_ns, version}`` (paused only) | ``{ok, market_now_ns}`` |
| ``POST /control/resume`` | - | ``{ok}`` |
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field
from starlette.exceptions import HTTPException as StarletteHTTPException

from futrisk.sim.exchange.errors import SimError, not_found
from futrisk.sim.exchange.servers import UvicornRunner

if TYPE_CHECKING:
    from futrisk.sim.exchange.app import SimExchange


class FaultBody(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: str
    target: str | None = None
    params: dict[str, Any] = Field(default_factory=dict)
    duration_s: float | None = Field(default=None, ge=0)


class ShockBody(BaseModel):
    model_config = ConfigDict(extra="forbid")

    instruments: list[str] | None = None
    sector: str | None = None
    pct: float = Field(description="ratio: -0.04 means -4% (README §3.4)")
    duration_s: float = Field(default=0.0, ge=0)
    shape: Literal["STEP", "LINEAR"] = "STEP"


class ScenarioBody(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    speed: float = Field(default=1.0, gt=0)
    vars: dict[str, Any] = Field(default_factory=dict)


class ClockBody(BaseModel):
    model_config = ConfigDict(extra="forbid")

    offset_ns: int
    version: int = Field(ge=0)


def create_control_app(ex: SimExchange) -> FastAPI:
    app = FastAPI(
        title="futrisk sim-exchange control API",
        version="1",
        docs_url="/control/docs",
        openapi_url="/control/openapi.json",
        redoc_url=None,
    )

    @app.exception_handler(SimError)
    async def _sim_error(_request: Request, exc: SimError) -> JSONResponse:
        return JSONResponse(exc.to_dict(), status_code=exc.status)

    @app.exception_handler(RequestValidationError)
    async def _validation_error(_request: Request, exc: RequestValidationError) -> JSONResponse:
        detail = "; ".join(f"{'.'.join(str(p) for p in e['loc'])}: {e['msg']}" for e in exc.errors())
        return JSONResponse({"ok": False, "code": "BAD_REQUEST", "message": detail}, status_code=400)

    @app.exception_handler(StarletteHTTPException)
    async def _http_error(_request: Request, exc: StarletteHTTPException) -> JSONResponse:
        code = {404: "NOT_FOUND", 405: "BAD_REQUEST"}.get(exc.status_code, "INTERNAL")
        return JSONResponse({"ok": False, "code": code, "message": str(exc.detail)}, status_code=exc.status_code)

    @app.get("/control/state")
    async def state() -> dict[str, Any]:
        return ex.state()

    @app.post("/control/faults")
    async def add_fault(body: FaultBody) -> dict[str, Any]:
        fault = await ex.faults.add(body.type, body.target, body.params, body.duration_s,
                                    created_ns=ex.clock.now_ns())
        return {"fault_id": fault.fault_id, "state": fault.state, "fault": fault.to_dict()}

    @app.delete("/control/faults/{fault_id}")
    async def delete_fault(fault_id: str) -> dict[str, Any]:
        if not await ex.faults.remove(fault_id):
            raise not_found(f"no active fault {fault_id}")
        return {"ok": True}

    @app.post("/control/shock")
    async def shock(body: ShockBody) -> dict[str, Any]:
        sh = ex.add_shock(pct=body.pct, instruments=body.instruments, sector=body.sector,
                          duration_s=body.duration_s, shape=body.shape)
        return {"shock_id": sh.shock_id, "shock": sh.to_dict()}

    @app.get("/control/scenarios")
    async def scenarios() -> list[dict[str, str]]:
        return ex.scenarios.list()

    @app.post("/control/scenario")
    async def run_scenario(body: ScenarioBody) -> dict[str, Any]:
        run = ex.scenarios.start(body.name, body.speed, body.vars)
        return {"run_id": run.run_id, "name": run.name}

    @app.delete("/control/scenario")
    async def cancel_scenario() -> dict[str, Any]:
        run = await ex.scenarios.cancel()
        if run is None:
            raise not_found("no scenario has been started")
        return run.to_dict()

    @app.post("/control/pause")
    async def pause() -> dict[str, Any]:
        await ex.pause()
        return {"ok": True}

    @app.post("/control/clock")
    async def clock(body: ClockBody) -> dict[str, Any]:
        market_now = ex.set_clock(body.offset_ns, body.version)
        return {"ok": True, "market_now_ns": market_now}

    @app.post("/control/resume")
    async def resume() -> dict[str, Any]:
        ex.resume()
        return {"ok": True}

    return app


class ControlServer:
    def __init__(self, ex: SimExchange, host: str, port: int) -> None:
        self.app = create_control_app(ex)
        self._runner = UvicornRunner(self.app, host, port, name="sim-control")

    @property
    def url(self) -> str:
        return f"http://{self._runner.host}:{self._runner.port}/control"

    @property
    def port(self) -> int:
        return self._runner.port

    async def start(self) -> None:
        await self._runner.start()

    async def stop(self) -> None:
        await self._runner.stop()
