"""Injected interfaces of api-gateway: bus requests and the sim-exchange control API (02 §10).

The FastAPI app, the command router, the 2PC coordinator, the clock-jump
orchestrator and the WebSocket hub only talk to other processes through these
small protocols, so unit tests inject fakes and never need real services.

* :class:`BusFacade` - request/reply over the bus (02 §4.7 retry policy);
  :class:`RuntimeBus` implements it on :class:`~futrisk.runtime.AsyncServiceRuntime`.
* :class:`SimControl` - the sim-exchange control API (02 §9.5);
  :class:`HttpSimControl` implements it with ``httpx``.
"""

from __future__ import annotations

import asyncio
from typing import Any, Protocol

import httpx
from loguru import logger

from futrisk.bus.timeouts import MAX_RETRIES, default_timeout_ms
from futrisk.common.config import FullConfig
from futrisk.messages import codes
from futrisk.messages.base import Msg
from futrisk.messages.envelope import Reply
from futrisk.runtime import AsyncServiceRuntime


class BusFacade(Protocol):
    """Request/reply to another internal instance."""

    async def request(
        self,
        target: str,
        command: str,
        payload: Msg,
        *,
        timeout_ms: int | None = None,
        retries: int | None = None,
        deadline_ns: int | None = None,
    ) -> Reply:
        """Send ``command`` to ``target``; never raises for transport errors (codes instead)."""
        ...


class RuntimeBus:
    """:class:`BusFacade` backed by the service runtime's DEALER clients.

    Two guards keep HTTP handlers responsive when a peer is gone (e.g. a demo
    ``svc.kill``): a target whose heartbeat is ``DOWN`` answers ``UNAVAILABLE``
    at once (02 §4.1: a DEALER must not queue for an absent peer), and every
    request is bounded by its per-attempt timeout x attempts + backoff slack
    (a request that would outlive that budget answers ``TIMEOUT``).
    """

    SLACK_S = 1.5

    def __init__(self, rt: AsyncServiceRuntime, *, fast_fail_down: bool = True) -> None:
        self._rt = rt
        self._fast_fail = fast_fail_down

    async def request(
        self,
        target: str,
        command: str,
        payload: Msg,
        *,
        timeout_ms: int | None = None,
        retries: int | None = None,
        deadline_ns: int | None = None,
    ) -> Reply:
        if self._fast_fail and target != self._rt.instance and self._rt.peers.state(target) == "DOWN":
            return Reply(ok=False, code=codes.UNAVAILABLE, message=f"{target}: peer is down (no heartbeat)")
        per_try_s = (timeout_ms if timeout_ms is not None else default_timeout_ms(command)) / 1000
        attempts = 1 + (MAX_RETRIES if retries is None else max(0, retries))
        budget_s = per_try_s * attempts + self.SLACK_S
        try:
            return await asyncio.wait_for(
                self._rt.request(target, command, payload, timeout_ms=timeout_ms, retries=retries,
                                 deadline_ns=deadline_ns),
                budget_s,
            )
        except TimeoutError:
            logger.warning("bus.request_budget_exceeded", target=target, command=command, budget_s=budget_s)
            return Reply(ok=False, code=codes.TIMEOUT, message=f"{target} {command}: no reply within {budget_s:.1f}s")


class SimControlError(Exception):
    """A failed sim-exchange control call, carrying a 02 §11.1 code."""

    def __init__(self, code: str, message: str, status: int | None = None, body: Any = None) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.status = status
        self.body = body


class SimControl(Protocol):
    """The sim-exchange control API (02 §9.5); every method returns the decoded JSON answer."""

    async def call(self, method: str, path: str, body: Any = None) -> Any:
        """``method`` ``/control{path}``; raises :class:`SimControlError` on failure."""
        ...


_STATUS_CODES = {400: codes.BAD_REQUEST, 404: codes.NOT_FOUND, 409: codes.CONFLICT, 503: codes.UNAVAILABLE}


class HttpSimControl:
    """HTTP client of ``http://{sim.control.host}:{sim.control.port}/control``."""

    def __init__(self, base_url: str, *, timeout_s: float = 5.0, client: httpx.AsyncClient | None = None) -> None:
        self.base_url = base_url.rstrip("/")
        self._client = client or httpx.AsyncClient(timeout=timeout_s, trust_env=False)

    @classmethod
    def from_config(cls, cfg: FullConfig, **kw: Any) -> HttpSimControl:
        c = cfg.sim.control
        return cls(f"http://{c.host}:{c.port}/control", **kw)

    async def call(self, method: str, path: str, body: Any = None) -> Any:
        url = f"{self.base_url}{path}"
        try:
            resp = await self._client.request(method, url, json=body if body is not None else None)
        except httpx.TimeoutException as exc:
            raise SimControlError(codes.TIMEOUT, f"sim-exchange control {method} {path}: timeout") from exc
        except httpx.HTTPError as exc:
            raise SimControlError(codes.UNAVAILABLE, f"sim-exchange control unreachable: {exc}") from exc
        try:
            data = resp.json()
        except ValueError:
            data = resp.text
        if resp.status_code >= 400:
            code = _STATUS_CODES.get(resp.status_code, codes.INTERNAL)
            message = f"sim-exchange answered HTTP {resp.status_code}"
            if isinstance(data, dict):
                code = str(data.get("code") or code)
                message = str(data.get("message") or message)
            logger.warning("sim_control.error", method=method, path=path, status=resp.status_code, code=code)
            raise SimControlError(code, message, resp.status_code, data)
        return data

    async def aclose(self) -> None:
        await self._client.aclose()


__all__ = ["BusFacade", "HttpSimControl", "RuntimeBus", "SimControl", "SimControlError"]
