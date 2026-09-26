"""Scenario scripts: ``config/scenarios/<name>.yaml`` run by ``POST /control/scenario`` (02 §9.5).

File format::

    name: breaker
    description: one paragraph shown by GET /control/scenarios
    vars:                      # defaults, overridable per run: {"vars": {"leg1_pct": -0.02}}
      sector: black
      leg1_pct: -0.015
    steps:                     # executed in at_s order, relative to the run start
      - at_s: 0
        action: shock          # shock | fault | clear_fault | clear_faults | pause | resume | log
        params: {sector: "${sector}", pct: "${leg1_pct}", duration_s: 60, shape: LINEAR}
      - at_s: 30
        action: fault
        ref: a_down            # name to clear it later with {action: clear_fault, params: {ref: a_down}}
        params: {type: FEED_DISCONNECT, target: feed_a, params: {refuse_reconnect_s: 20}}

``"${name}"`` as a whole value is replaced by the variable (keeping its type); inside a
longer string it is substituted as text. ``speed`` divides ``at_s``, the ``duration_s`` of
shocks and faults and the duration-type fault parameters (``refuse_reconnect_s``,
``seconds``); latencies, ratios and counts are not scaled.
"""

from __future__ import annotations

import asyncio
import re
import time
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

import yaml
from loguru import logger
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from futrisk.common.ids import uuid7
from futrisk.sim.exchange.errors import SimError, bad_request, conflict, not_found

if TYPE_CHECKING:
    from futrisk.sim.exchange.app import SimExchange

_NAME_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
_VAR_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")
#: fault parameters that are durations in seconds and therefore scale with ``speed``
_SCALED_FAULT_PARAMS = ("refuse_reconnect_s", "seconds")

Action = Literal["shock", "fault", "clear_fault", "clear_faults", "pause", "resume", "log"]


class ScenarioStep(BaseModel):
    model_config = ConfigDict(extra="forbid")

    at_s: float = Field(0.0, ge=0)
    action: Action
    params: dict[str, Any] = Field(default_factory=dict)
    ref: str | None = None
    note: str | None = None


class ScenarioFile(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    description: str = ""
    vars: dict[str, Any] = Field(default_factory=dict)
    steps: list[ScenarioStep]


def substitute(value: Any, variables: dict[str, Any]) -> Any:
    """Replace ``${var}`` references recursively (see the module docstring)."""
    if isinstance(value, str):
        m = _VAR_RE.fullmatch(value)
        if m:
            if m.group(1) not in variables:
                raise bad_request(f"undefined scenario variable {m.group(1)!r}")
            return variables[m.group(1)]

        def repl(mm: re.Match[str]) -> str:
            if mm.group(1) not in variables:
                raise bad_request(f"undefined scenario variable {mm.group(1)!r}")
            return str(variables[mm.group(1)])

        return _VAR_RE.sub(repl, value)
    if isinstance(value, dict):
        return {k: substitute(v, variables) for k, v in value.items()}
    if isinstance(value, list):
        return [substitute(v, variables) for v in value]
    return value


def _dirs(directories: Path | Sequence[Path]) -> list[Path]:
    return [directories] if isinstance(directories, Path) else list(directories)


def load_scenario(directories: Path | Sequence[Path], name: str) -> ScenarioFile:
    """Load ``<name>.yaml`` from the first directory that has it."""
    if not _NAME_RE.match(name):
        raise bad_request(f"invalid scenario name {name!r}")
    dirs = _dirs(directories)
    path = next((d / f"{name}.yaml" for d in dirs if (d / f"{name}.yaml").is_file()), None)
    if path is None:
        raise not_found(f"scenario {name!r} not found in {[str(d) for d in dirs]}")
    try:
        with open(path, encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        return ScenarioFile.model_validate(data)
    except (yaml.YAMLError, ValidationError) as exc:
        raise bad_request(f"scenario {name!r} is invalid: {exc}") from exc


def list_scenarios(directories: Path | Sequence[Path]) -> list[dict[str, str]]:
    """``[{name, description}]`` over the directories (the first one wins for a name)."""
    out: list[dict[str, str]] = []
    seen: set[str] = set()
    paths = [p for d in _dirs(directories) if d.is_dir() for p in sorted(d.glob("*.yaml"))]
    for path in paths:
        if path.stem in seen:
            continue
        seen.add(path.stem)
        try:
            with open(path, encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
            desc = str(data.get("description", "")).strip()
        except (OSError, yaml.YAMLError):
            desc = "(unreadable)"
        out.append({"name": path.stem, "description": desc})
    return out


@dataclass
class ScenarioRun:
    run_id: str
    name: str
    speed: float
    variables: dict[str, Any]
    steps_total: int
    started_ns: int
    status: str = "RUNNING"  # RUNNING | DONE | FAILED | CANCELED
    step: int = 0
    error: str | None = None
    refs: dict[str, str] = field(default_factory=dict)
    faults: list[str] = field(default_factory=list)
    shocks: list[str] = field(default_factory=list)
    log: list[dict[str, Any]] = field(default_factory=list)
    task: asyncio.Task[None] | None = field(default=None, repr=False)

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "name": self.name,
            "status": self.status,
            "speed": self.speed,
            "step": self.step,
            "steps_total": self.steps_total,
            "started_ns": self.started_ns,
            "error": self.error,
            "vars": self.variables,
            "faults": list(self.faults),
            "shocks": list(self.shocks),
            "log": self.log[-20:],
        }


class ScenarioRunner:
    """Runs one scenario at a time against a :class:`SimExchange`."""

    def __init__(self, ex: SimExchange, directories: Path | Sequence[Path]) -> None:
        self._ex = ex
        self.directories = _dirs(directories)
        self.current: ScenarioRun | None = None

    def list(self) -> list[dict[str, str]]:
        return list_scenarios(self.directories)

    def start(self, name: str, speed: float = 1.0, variables: dict[str, Any] | None = None) -> ScenarioRun:
        if not speed > 0:
            raise bad_request("speed must be > 0")
        if self.current is not None and self.current.status == "RUNNING":
            raise conflict(f"scenario {self.current.name!r} ({self.current.run_id}) is still running")
        scen = load_scenario(self.directories, name)
        merged = {**scen.vars, **(variables or {})}
        run = ScenarioRun(
            run_id=uuid7(),
            name=scen.name,
            speed=float(speed),
            variables=merged,
            steps_total=len(scen.steps),
            started_ns=self._ex.clock.now_ns(),
        )
        run.task = asyncio.create_task(self._run(run, scen), name=f"scenario-{name}")
        self.current = run
        logger.info("scenario {} started (run_id={}, speed={})", name, run.run_id, speed)
        return run

    async def cancel(self) -> ScenarioRun | None:
        run = self.current
        if run is None or run.task is None or run.task.done():
            return run
        run.task.cancel()
        await asyncio.gather(run.task, return_exceptions=True)
        return run

    def state(self) -> dict[str, Any] | None:
        return self.current.to_dict() if self.current is not None else None

    async def _run(self, run: ScenarioRun, scen: ScenarioFile) -> None:
        t0 = time.monotonic()
        steps = sorted(enumerate(scen.steps), key=lambda p: (p[1].at_s, p[0]))
        try:
            for idx, (_, step) in enumerate(steps):
                delay = step.at_s / run.speed - (time.monotonic() - t0)
                if delay > 0:
                    await asyncio.sleep(delay)
                run.step = idx + 1
                await self._execute(run, step)
            run.status = "DONE"
            logger.info("scenario {} done (run_id={})", run.name, run.run_id)
        except asyncio.CancelledError:
            run.status = "CANCELED"
            raise
        except SimError as exc:
            run.status, run.error = "FAILED", exc.message
            logger.error("scenario {} failed at step {}: {}", run.name, run.step, exc.message)
        except Exception as exc:  # a bug in a step must not kill the exchange
            run.status, run.error = "FAILED", repr(exc)
            logger.exception("scenario {} failed at step {}", run.name, run.step)

    async def _execute(self, run: ScenarioRun, step: ScenarioStep) -> None:
        params = substitute(step.params, run.variables)
        entry: dict[str, Any] = {"step": run.step, "action": step.action, "at_s": step.at_s}
        if step.note:
            entry["note"] = substitute(step.note, run.variables)
        ex = self._ex
        if step.action == "shock":
            duration = float(params.get("duration_s", 0.0)) / run.speed
            shock = ex.add_shock(
                pct=params.get("pct"),
                instruments=params.get("instruments"),
                sector=params.get("sector"),
                duration_s=duration,
                shape=params.get("shape", "STEP"),
            )
            run.shocks.append(shock.shock_id)
            entry["shock_id"] = shock.shock_id
        elif step.action == "fault":
            duration = params.get("duration_s")
            fparams = dict(params.get("params") or {})
            for key in _SCALED_FAULT_PARAMS:  # durations follow the run speed; latencies/ratios do not
                if isinstance(fparams.get(key), int | float) and not isinstance(fparams[key], bool):
                    fparams[key] = float(fparams[key]) / run.speed
            fault = await ex.faults.add(
                str(params.get("type", "")),
                params.get("target"),
                fparams,
                None if duration is None else float(duration) / run.speed,
                created_ns=ex.clock.now_ns(),
            )
            run.faults.append(fault.fault_id)
            if step.ref:
                run.refs[step.ref] = fault.fault_id
            entry["fault_id"] = fault.fault_id
        elif step.action == "clear_fault":
            ref = str(params.get("ref", ""))
            if ref not in run.refs:
                raise bad_request(f"clear_fault: unknown ref {ref!r}")
            await ex.faults.remove(run.refs[ref])
        elif step.action == "clear_faults":
            for fid in run.faults:
                await ex.faults.remove(fid)
        elif step.action == "pause":
            await ex.pause()
        elif step.action == "resume":
            ex.resume()
        # "log" only records the note
        run.log.append(entry)
        logger.info("scenario {} step {}/{}: {} {}", run.name, run.step, run.steps_total, step.action,
                    entry.get("note", ""))
