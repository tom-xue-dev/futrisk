"""Measurement hooks inside a risk-engine process started by the benches (``probe re``; never in production).

The benches run the real service through the production entry point (``futrisk svc risk-engine``);
before that entry point runs, :func:`install` wraps a few methods of :class:`RiskEngineService` and
:class:`EngineCore` **in that child process only** to take ``perf_counter_ns`` timestamps (one
system-wide timeline on the machine, so they can be compared with the publisher's):

* start-up: ``start`` (runtime, risk.db, core), ``_on_clock_ready`` (restore from risk.db: load +
  ``core.restore`` + ``post_restore``) and every ``_set_phase`` (CLOCK / SOD / CATCHUP / WARMUP / RUNNING);
* tick -> decision (``ticks=True``): live ``md.tick`` envelopes whose ``corr_id`` is ``"m:{publish_ns}"``
  are remembered with their receipt time (``_on_md``: drained from the SUB socket by the loop), become
  *covered* when their slot is applied
  (``_apply_marks``; conflated ticks are covered by the newer tick of the same instrument), are
  *decided* when the next ``evaluate_dirty`` returns and *executed* when the effects of that evaluation
  have been executed (``execute``: persist + publishes) - the same end point as the heartbeat's
  ``latency_us.tick_eval``, which starts at the loop's drain instead of at the publisher;
* every garbage collection of the process (generation, start, duration: ``gc.callbacks``);
* ``profile="running"``: cProfile from RUNNING until the service stops; ``profile="startup"``: from
  ``RiskEngineService.start`` until RUNNING (top functions in the output).

The results are written as JSON to ``out`` when ``RiskEngineService.run`` returns.
"""

from __future__ import annotations

import cProfile
import gc
import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from futrisk.bench.core import now_ns, profile_lines


@dataclass
class _State:
    out: Path
    ticks: bool
    t_probe_start_ns: int
    t_installed_ns: int = 0
    marks: dict[str, int] = field(default_factory=dict)
    phases: list[tuple[str, int]] = field(default_factory=list)
    pending: list[tuple[int, int]] = field(default_factory=list)  # (published, received)
    covered: list[tuple[int, int]] = field(default_factory=list)
    decided: list[tuple[int, int, int]] = field(default_factory=list)
    samples: list[tuple[int, int, int, int]] = field(default_factory=list)  # (published, received, decided, executed)
    evaluations: int = 0
    profiler: cProfile.Profile | None = None
    profile_mode: str | None = None
    profile_lines: list[str] = field(default_factory=list)
    gc_events: list[tuple[int, int, int]] = field(default_factory=list)  # (generation, start ns, duration ns)
    gc_t: int = 0


_STATE: _State | None = None


def install(out: Path, *, ticks: bool = False, profile: str | None = None, t_probe_start_ns: int = 0) -> None:
    """Wrap the risk-engine methods (call once, before the service is constructed)."""
    global _STATE
    from futrisk.core.engine import EngineCore
    from futrisk.services.risk_engine.service import RiskEngineService

    st = _State(out=out, ticks=ticks, t_probe_start_ns=t_probe_start_ns)
    st.profiler = cProfile.Profile() if profile else None
    st.profile_mode = profile
    _STATE = st

    orig_start = RiskEngineService.start
    orig_clock_ready = RiskEngineService._on_clock_ready
    orig_set_phase = RiskEngineService._set_phase
    orig_run = RiskEngineService.run

    def start(self: Any) -> None:
        st.marks.setdefault("start_begin", now_ns())
        if st.profiler is not None and st.profile_mode == "startup":
            st.profiler.enable()
        orig_start(self)
        st.marks.setdefault("start_end", now_ns())

    def on_clock_ready(self: Any) -> None:
        st.marks["restore_begin"] = now_ns()
        orig_clock_ready(self)
        st.marks["restore_end"] = now_ns()

    def set_phase(self: Any, phase: Any) -> None:
        name = str(getattr(phase, "value", phase))
        st.phases.append((name, now_ns()))
        if st.profiler is not None and name == "RUNNING" and st.profile_mode == "startup":
            st.profiler.disable()
        orig_set_phase(self, phase)
        if st.profiler is not None and name == "RUNNING" and st.profile_mode == "running":
            st.profiler.enable()

    def run(self: Any, **kw: Any) -> int:
        code = -1
        try:
            code = int(orig_run(self, **kw))
            return code
        finally:
            _dump(st, code)

    RiskEngineService.start = start  # type: ignore[method-assign]
    RiskEngineService._on_clock_ready = on_clock_ready  # type: ignore[method-assign]
    RiskEngineService._set_phase = set_phase  # type: ignore[method-assign]
    RiskEngineService.run = run  # type: ignore[method-assign]

    if ticks:
        orig_on_md = RiskEngineService._on_md
        orig_apply = RiskEngineService._apply_marks
        orig_execute = RiskEngineService.execute
        orig_eval = EngineCore.evaluate_dirty

        def on_md(self: Any, env: Any) -> None:
            if self._md_live:
                c = env.corr_id
                if c is not None and c.startswith("m:"):
                    st.pending.append((int(c[2:]), now_ns()))
            orig_on_md(self, env)

        def apply_marks(self: Any) -> int:
            n = int(orig_apply(self))
            if n and st.pending:
                st.covered.extend(st.pending)
                st.pending.clear()
            return n

        def evaluate_dirty(self: Any) -> Any:
            effects = orig_eval(self)
            if st.covered:
                t = now_ns()
                st.decided.extend((tp, tr, t) for tp, tr in st.covered)
                st.covered.clear()
                st.evaluations += 1
            return effects

        def execute(self: Any, effects: Any, *, cursor: Any = None) -> None:
            orig_execute(self, effects, cursor=cursor)
            if st.decided:
                t = now_ns()
                st.samples.extend((tp, tr, td, t) for tp, tr, td in st.decided)
                st.decided.clear()

        RiskEngineService._on_md = on_md  # type: ignore[method-assign]
        RiskEngineService._apply_marks = apply_marks  # type: ignore[method-assign]
        RiskEngineService.execute = execute  # type: ignore[method-assign]
        EngineCore.evaluate_dirty = evaluate_dirty  # type: ignore[method-assign]

    def gc_cb(phase: str, info: dict[str, Any]) -> None:
        if phase == "start":
            st.gc_t = now_ns()
        elif st.gc_t and len(st.gc_events) < 200_000:
            st.gc_events.append((int(info.get("generation", 0)), st.gc_t, now_ns() - st.gc_t))

    gc.callbacks.append(gc_cb)
    st.t_installed_ns = now_ns()


def _dump(st: _State, code: int) -> None:
    if st.profiler is not None:
        st.profiler.disable()  # no-op when already stopped at RUNNING ("startup")
        try:
            st.profile_lines = profile_lines(st.profiler, top=30)
        except (TypeError, ValueError):  # nothing was profiled (the service never got that far)
            st.profile_lines = []
    doc = {
        "pid": os.getpid(),
        "exit_code": code,
        "t_probe_start_ns": st.t_probe_start_ns,
        "t_installed_ns": st.t_installed_ns,
        "marks": st.marks,
        "phases": st.phases,
        "ticks": {"samples": st.samples, "evaluations": st.evaluations, "pending_left": len(st.pending),
                  "covered_left": len(st.covered)} if st.ticks else None,
        "profile": st.profile_lines,
        "gc_events": st.gc_events,
    }
    tmp = st.out.with_suffix(".tmp")
    tmp.write_text(json.dumps(doc), encoding="utf-8")
    os.replace(tmp, st.out)


__all__ = ["install"]
