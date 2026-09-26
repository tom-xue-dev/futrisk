"""Market-clock jump orchestration for ``POST /demo/clock`` (02 §8.8, 01 §8.2, ADR-017).

========  ================================  ==============================================
step      from -> to                        action
========  ================================  ==============================================
1         api-gateway -> sim-exchange       ``POST /control/pause`` (no quotes, no matching)
2         api-gateway -> supervisor         ``svc.clock`` -> persists the offset, version+1,
                                            publishes ``sys.clock``; answers ``ClockState``
3         api-gateway -> sim-exchange       ``POST /control/clock {offset_ns, version}``
                                            (idempotent: retried up to ``sim_attempts`` times)
4         api-gateway                       wait until every live internal instance reports
                                            ``clock_version >= version`` in its heartbeat,
                                            at most 5 s; on timeout warn and continue
5         api-gateway -> sim-exchange       ``POST /control/resume``
========  ================================  ==============================================

Failure rules -- the sim must never run on an offset the rest of the system
does not use:

* step 1 fails: nothing changed, the error is returned;
* step 2 fails: the market clock this process follows (``sys.clock``) is read
  back; a ``TIMEOUT`` is ambiguous (the supervisor may have applied the jump
  and lost the answer), so the check waits up to ``confirm_s`` (``sys.clock``
  is re-sent every 5 s). When the version advanced, the jump did happen: the
  new offset is pushed to the sim (step 3) and the jump continues. When it did
  not, the sim is still in sync and is resumed; the error is returned;
* step 3 fails after its retries: the sim is **kept PAUSED** and an explicit
  error is returned (resuming it would run the exchange on the old offset);
  retry ``POST /demo/clock`` (or resume through the sim control API once it
  is fixed).
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from loguru import logger

from futrisk.common.clock import Clock
from futrisk.messages import codes
from futrisk.messages.base import to_builtins
from futrisk.messages.enums import ClockMode, ClockTarget
from futrisk.messages.system import ClockJumpRequest, ClockState
from futrisk.services.api_gateway.ports import BusFacade, SimControl, SimControlError
from futrisk.services.api_gateway.router import SUPERVISOR

ClockVersions = Callable[[], dict[str, int]]
CurrentClock = Callable[[], tuple[int, int, str]]  # (version, offset_ns, mode) of the followed sys.clock


@dataclass
class JumpResult:
    """Outcome of one clock jump (REST ``data``)."""

    ok: bool = True
    code: str = codes.OK
    message: str = ""
    clock: ClockState | None = None
    steps: list[dict[str, Any]] = field(default_factory=list)
    waited_ms: int = 0
    lagging: list[str] = field(default_factory=list)
    sim_paused: bool = False  # the sim was left PAUSED on purpose (not in sync with the market clock)

    def to_dict(self) -> dict[str, Any]:
        return {
            "clock": to_builtins(self.clock) if self.clock is not None else None,
            "steps": self.steps,
            "waited_ms": self.waited_ms,
            "converged": not self.lagging,
            "lagging": self.lagging,
            "sim_paused": self.sim_paused,
        }


def _clock_of(clock: Clock) -> tuple[int, int, str]:
    return (int(getattr(clock, "version", 0)), int(getattr(clock, "offset_ns", 0)),
            str(getattr(clock, "mode", "REAL")))


class ClockJumpOrchestrator:
    """Runs the five steps of 02 §8.8; one jump at a time."""

    def __init__(
        self,
        *,
        bus: BusFacade,
        sim: SimControl,
        clock_versions: ClockVersions,
        clock: Clock,
        current_clock: CurrentClock | None = None,
        wait_timeout_s: float = 5.0,
        poll_s: float = 0.05,
        confirm_s: float = 6.0,
        sim_attempts: int = 3,
        sim_backoff_s: float = 0.3,
        sleep: Callable[[float], Awaitable[Any]] = asyncio.sleep,
    ) -> None:
        self._bus = bus
        self._sim = sim
        self._versions = clock_versions
        self._clock = clock
        self._current = current_clock or (lambda: _clock_of(clock))
        self._wait_s = wait_timeout_s
        self._poll_s = poll_s
        self._confirm_s = confirm_s
        self._sim_attempts = max(1, sim_attempts)
        self._sim_backoff_s = sim_backoff_s
        self._sleep = sleep
        self._lock = asyncio.Lock()

    async def jump(self, *, command_id: str, target: ClockTarget, at_ns: int | None, actor: str) -> JumpResult:
        async with self._lock:
            res = await self._jump(command_id, target, at_ns, actor)
        logger.info("clock_jump.done", ok=res.ok, code=res.code, target=str(target), actor=actor,
                    version=res.clock.version if res.clock else None, sim_paused=res.sim_paused)
        return res

    @staticmethod
    def _step(res: JumpResult, n: int, action: str, ok: bool, detail: Any = None) -> None:
        res.steps.append({"step": n, "action": action, "ok": ok, "detail": detail})

    async def _jump(self, command_id: str, target: ClockTarget, at_ns: int | None, actor: str) -> JumpResult:
        res = JumpResult()
        before = self._current()
        # 1) pause the sim
        try:
            await self._sim.call("POST", "/pause")
        except SimControlError as exc:
            self._step(res, 1, "sim.pause", False, exc.message)
            res.ok, res.code, res.message = False, exc.code, f"sim-exchange pause failed: {exc.message}"
            return res
        self._step(res, 1, "sim.pause", True)
        in_sync = True  # the paused sim runs on the offset the rest of the system uses
        try:
            # 2) the supervisor moves the clock (persist, version+1, publish sys.clock)
            req = ClockJumpRequest(command_id=command_id, target=target, at_ns=at_ns, actor=actor)
            reply = await self._bus.request(SUPERVISOR, "svc.clock", req)
            state = reply.open_as(ClockState) if reply.ok and reply.data is not None else None
            if state is None:
                code = reply.code if not reply.ok else codes.INTERNAL
                self._step(res, 2, "svc.clock", False, {"code": code, "message": reply.message})
                state = await self._observed(before, ambiguous=code == codes.TIMEOUT, actor=actor)
                if state is None:
                    res.ok, res.code = False, code
                    res.message = f"svc.clock failed: {reply.message or code}; the market clock did not change"
                    await self._resume(res)
                    return res
                self._step(res, 2, "sys.clock.observed", True, {"version": state.version,
                                                                 "offset_ns": state.offset_ns})
                res.message = (f"svc.clock answered {code}, but sys.clock advanced to version {state.version}: "
                               "the jump was applied")
                logger.warning("clock_jump.recovered", code=code, version=state.version)
            else:
                self._step(res, 2, "svc.clock", True, {"version": state.version, "offset_ns": state.offset_ns,
                                                       "market_now_ns": state.market_now_ns})
            in_sync = False
            res.clock = state
            # 3) the sim follows the new offset (idempotent: retried)
            failure = await self._sync_sim(res, state)
            if failure is not None:
                res.ok, res.code = False, failure.code
                res.sim_paused = True
                res.message = (f"sim-exchange could not adopt clock version {state.version} ({failure.message}); "
                               "it stays PAUSED so it never runs on the old offset: retry POST /demo/clock")
                self._step(res, 5, "sim.resume", False, "skipped: the sim is not in sync, kept PAUSED")
                logger.error("clock_jump.sim_kept_paused", version=state.version, code=failure.code)
                return res
            in_sync = True
            # 4) wait for every live instance to apply the new clock version (max 5 s)
            start = self._clock.monotonic_ns()
            deadline = start + int(self._wait_s * 1e9)
            while True:
                lagging = sorted(i for i, v in self._versions().items() if v < state.version)
                if not lagging or self._clock.monotonic_ns() >= deadline:
                    break
                await self._sleep(self._poll_s)
            res.waited_ms = (self._clock.monotonic_ns() - start) // 1_000_000
            res.lagging = lagging
            if lagging:
                logger.warning("clock_jump.lagging", version=state.version, lagging=lagging)
            self._step(res, 4, "wait.heartbeats", not lagging, {"lagging": lagging, "waited_ms": res.waited_ms})
            # 5) resume
            await self._resume(res)
            return res
        except BaseException:
            if in_sync:  # unexpected error or cancellation: never leave a consistent sim frozen
                await asyncio.shield(self._resume(res))
            raise

    async def _observed(self, before: tuple[int, int, str], *, ambiguous: bool, actor: str) -> ClockState | None:
        """The followed ``sys.clock`` when it advanced past ``before`` (waits ``confirm_s`` when ambiguous)."""
        deadline = self._clock.monotonic_ns() + int((self._confirm_s if ambiguous else 0) * 1e9)
        while True:
            version, offset_ns, mode = self._current()
            if version > before[0]:
                return ClockState(mode=ClockMode(mode), offset_ns=offset_ns, version=version,
                                  market_now_ns=self._clock.now_ns(), actor=actor)
            if self._clock.monotonic_ns() >= deadline:
                return None
            await self._sleep(self._poll_s)

    async def _sync_sim(self, res: JumpResult, state: ClockState) -> SimControlError | None:
        body = {"offset_ns": state.offset_ns, "version": state.version}
        failure: SimControlError | None = None
        for attempt in range(1, self._sim_attempts + 1):
            try:
                ans = await self._sim.call("POST", "/clock", body)
            except SimControlError as exc:
                failure = exc
                self._step(res, 3, "sim.clock", False, {"attempt": attempt, "code": exc.code, "message": exc.message})
                if attempt < self._sim_attempts:
                    await self._sleep(self._sim_backoff_s * attempt)
                continue
            self._step(res, 3, "sim.clock", True, ans)
            return None
        return failure

    async def _resume(self, res: JumpResult) -> None:
        try:
            await self._sim.call("POST", "/resume")
        except SimControlError as exc:
            self._step(res, 5, "sim.resume", False, exc.message)
            res.sim_paused = True
            if res.ok:
                res.ok, res.code = False, exc.code
                res.message = f"sim-exchange resume failed: {exc.message}"
            return
        self._step(res, 5, "sim.resume", True)


__all__ = ["ClockJumpOrchestrator", "JumpResult"]
