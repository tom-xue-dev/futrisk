"""Result model, statistics and the stopwatch shared by every benchmark (01 §8.12, plan §10.4).

* :func:`now_ns` is ``time.perf_counter_ns`` - the only clock used for measuring. On Windows it is
  ``QueryPerformanceCounter``: sub-microsecond resolution and **one system-wide timeline**, so
  timestamps taken in different processes on the same machine can be subtracted (the multi-process
  benches rely on this and check it at start-up). ``time.time()`` has ~15.6 ms resolution there.
* :class:`Metric` is either a distribution (latency samples, summarised as n / p50 / p90 / p99 / max /
  mean) or a scalar (throughput, duration). Percentiles use the nearest-rank method on the sorted
  samples: ``pXX = sorted[ceil(XX/100 * n) - 1]``.
* :class:`Target` is one design target (e.g. ``p99 <= 3 ms``); a metric passes when every target holds.
  A bench's verdict is the verdict of its primary metric(s); other metrics are informative.
"""

from __future__ import annotations

import math
import statistics
import time
from collections.abc import Iterable, Sequence
from dataclasses import asdict, dataclass, field
from typing import Any, Literal

#: stopwatch of the benchmarks (never business time: services use their injected clocks)
now_ns = time.perf_counter_ns  # noqa: TID251 - benchmark stopwatch, system-wide QPC timeline on Windows

Verdict = Literal["PASS", "MISS", "ERROR", "N/A"]

_TO_US = {"ns": 1e-3, "us": 1.0, "ms": 1e3, "s": 1e6}


def percentile(sorted_values: Sequence[float], q: float) -> float:
    """Nearest-rank percentile (``q`` in 0..1) of already sorted values."""
    n = len(sorted_values)
    if n == 0:
        raise ValueError("no samples")
    k = max(1, math.ceil(q * n))
    return float(sorted_values[min(n, k) - 1])


@dataclass(frozen=True)
class Target:
    """One design target on a metric statistic: ``stat op value unit`` (``p99 <= 3 ms``, ``rate >= 100000 msg/s``)."""

    stat: str  # p50 | p90 | p99 | max | mean | value
    op: Literal["<=", "<", ">=", ">"]
    value: float
    unit: str  # latency: ns | us | ms | s ; scalars: the metric's unit

    def text(self) -> str:
        sym = {"<=": "≤", "<": "<", ">=": "≥", ">": ">"}[self.op]
        v = f"{self.value:,.0f}" if self.value >= 1000 else f"{self.value:g}"
        name = "" if self.stat == "value" else f"{self.stat} "
        unit = "µs" if self.unit == "us" else self.unit
        return f"{name}{sym} {v} {unit}"

    def holds(self, metric: Metric) -> bool | None:
        got = metric.stat(self.stat)
        if got is None:
            return None
        if metric.kind == "latency":
            got = got / _TO_US[self.unit]  # latency statistics are stored in µs
        return {"<=": got <= self.value, "<": got < self.value, ">=": got >= self.value, ">": got > self.value}[self.op]


@dataclass
class Metric:
    """A latency distribution (µs) or a scalar value (``kind`` rate / duration / count / ratio)."""

    name: str
    kind: Literal["latency", "rate", "duration", "count", "ratio"]
    unit: str  # latency: "us"; rate: "msg/s", "ticks/s", ...; duration: "s"
    n: int = 0
    p50: float | None = None
    p90: float | None = None
    p99: float | None = None
    max: float | None = None
    mean: float | None = None
    min: float | None = None
    value: float | None = None
    targets: list[Target] = field(default_factory=list)
    primary: bool = False
    note: str = ""
    verdict: Verdict | None = None

    def stat(self, name: str) -> float | None:
        if name == "value":
            return self.value
        got = getattr(self, name, None)
        return float(got) if isinstance(got, int | float) else None

    def judge(self) -> Verdict | None:
        if not self.targets:
            self.verdict = None
            return None
        checks = [t.holds(self) for t in self.targets]
        if any(c is None for c in checks):
            self.verdict = "N/A"
        else:
            self.verdict = "PASS" if all(checks) else "MISS"
        return self.verdict

    def target_text(self) -> str:
        return ", ".join(t.text() for t in self.targets)


def latency(name: str, samples_us: Iterable[float], *, targets: Sequence[Target] = (), primary: bool = False,
            note: str = "") -> Metric:
    """Summarise latency samples given in microseconds."""
    data = sorted(float(x) for x in samples_us)
    m = Metric(name=name, kind="latency", unit="us", n=len(data), targets=list(targets), primary=primary, note=note)
    if data:
        m.p50 = round(percentile(data, 0.50), 2)
        m.p90 = round(percentile(data, 0.90), 2)
        m.p99 = round(percentile(data, 0.99), 2)
        m.max = round(data[-1], 2)
        m.min = round(data[0], 2)
        m.mean = round(statistics.fmean(data), 2)
    m.judge()
    return m


def latency_ns(name: str, samples_ns: Iterable[int], **kw: Any) -> Metric:
    return latency(name, (x / 1000.0 for x in samples_ns), **kw)


def from_summary(name: str, summary: Any, *, note: str = "") -> Metric:
    """A latency metric from a service's ``LatencySummary`` (µs; p50 / p99 / max over its sliding window)."""
    m = Metric(name=name, kind="latency", unit="us", note=note)
    if summary is not None:
        m.p50, m.p99, m.max, m.n = float(summary.p50), float(summary.p99), float(summary.max), int(summary.n)
    return m


def scalar(name: str, value: float | None, unit: str, *, kind: Literal["rate", "duration", "count", "ratio"] = "rate",
           n: int = 0, targets: Sequence[Target] = (), primary: bool = False, note: str = "") -> Metric:
    m = Metric(name=name, kind=kind, unit=unit, n=n, value=None if value is None else round(float(value), 4),
               targets=list(targets), primary=primary, note=note)
    m.judge()
    return m


def durations(name: str, samples_s: Iterable[float], *, targets: Sequence[Target] = (), primary: bool = False,
              note: str = "") -> Metric:
    """A (small) distribution of durations in seconds (e.g. repeated restarts)."""
    data = sorted(float(x) for x in samples_s)
    m = Metric(name=name, kind="duration", unit="s", n=len(data), targets=list(targets), primary=primary, note=note)
    if data:
        m.p50 = round(percentile(data, 0.50), 4)
        m.p90 = round(percentile(data, 0.90), 4)
        m.p99 = round(percentile(data, 0.99), 4)
        m.max = round(data[-1], 4)
        m.min = round(data[0], 4)
        m.mean = round(statistics.fmean(data), 4)
        m.value = m.p50
    m.judge()
    return m


SizeMode = Literal["tiny", "quick", "full"]


@dataclass
class BenchContext:
    """Options shared by every bench: size mode, profiling, where temporary files go, progress output.

    ``mode``: ``full`` (the documented numbers), ``quick`` (a few seconds per bench) or ``tiny`` (smoke tests:
    the plumbing only, numbers meaningless). ``overrides`` replace single sizes (``{"n": 5000}``);
    ``heartbeat_ms`` overrides the bus heartbeat of the multi-process world (production default 1000 ms).
    """

    mode: SizeMode = "full"
    profile: bool = False
    keep: bool = False
    tmp_root: str | None = None
    overrides: dict[str, Any] = field(default_factory=dict)
    heartbeat_ms: int | None = None
    echo: Any = None  # Callable[[str], None]

    @property
    def quick(self) -> bool:
        return self.mode != "full"

    def size(self, key: str, *, full: Any, quick: Any, tiny: Any = None) -> Any:
        if key in self.overrides:
            return self.overrides[key]
        if self.mode == "full":
            return full
        if self.mode == "tiny" and tiny is not None:
            return tiny
        return quick

    def say(self, text: str) -> None:
        if self.echo is not None:
            self.echo(text)


@dataclass
class BenchResult:
    """Outcome of one benchmark."""

    name: str
    title: str
    target: str  # the design target in words (01 §8.12 / plan §10.4)
    quick: bool
    params: dict[str, Any] = field(default_factory=dict)
    metrics: list[Metric] = field(default_factory=list)
    verdict: Verdict = "N/A"
    wall_s: float = 0.0
    load: dict[str, Any] = field(default_factory=dict)
    notes: list[str] = field(default_factory=list)
    extra: dict[str, Any] = field(default_factory=dict)
    profile: list[str] = field(default_factory=list)
    error: str | None = None

    def add(self, metric: Metric) -> Metric:
        self.metrics.append(metric)
        return metric

    def decide(self) -> Verdict:
        if self.error is not None:
            self.verdict = "ERROR"
            return self.verdict
        primaries = [m for m in self.metrics if m.primary and m.targets]
        verdicts = [m.judge() for m in primaries]
        if not verdicts or any(v in (None, "N/A") for v in verdicts):
            self.verdict = "N/A"
        else:
            self.verdict = "PASS" if all(v == "PASS" for v in verdicts) else "MISS"
        return self.verdict

    def primary(self) -> Metric | None:
        for m in self.metrics:
            if m.primary:
                return m
        return None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class GcMonitor:
    """Garbage-collector pauses while active (``gc.callbacks``): collections per generation, total / max pause.

    CPython's cyclic GC stops every thread of the process; its pauses are one source of latency tails
    (01 §11 RK-01).
    """

    def __init__(self) -> None:
        self._t = 0
        self.counts = [0, 0, 0]
        self.total_ns = [0, 0, 0]
        self.max_ns = [0, 0, 0]

    def _cb(self, phase: str, info: dict[str, Any]) -> None:
        if phase == "start":
            self._t = now_ns()
            return
        gen = int(info.get("generation", 0))
        if 0 <= gen <= 2 and self._t:
            dt = now_ns() - self._t
            self.counts[gen] += 1
            self.total_ns[gen] += dt
            self.max_ns[gen] = max(self.max_ns[gen], dt)

    def __enter__(self) -> GcMonitor:
        import gc

        gc.callbacks.append(self._cb)
        return self

    def __exit__(self, *exc: object) -> None:
        import gc

        if self._cb in gc.callbacks:
            gc.callbacks.remove(self._cb)

    def summary(self) -> dict[str, Any]:
        import gc

        return {
            "enabled": gc.isenabled(),
            "collections": {f"gen{g}": self.counts[g] for g in range(3)},
            "pause_total_ms": round(sum(self.total_ns) / 1e6, 2),
            "pause_max_ms": {f"gen{g}": round(self.max_ns[g] / 1e6, 2) for g in range(3)},
        }


def gc_window(events: Iterable[Sequence[int]], t0_ns: int, t1_ns: int) -> dict[str, Any]:
    """GC pauses of another process (``(generation, start_ns, duration_ns)``) that started in ``[t0, t1]``."""
    counts = [0, 0, 0]
    total = [0, 0, 0]
    worst = [0, 0, 0]
    for gen, start, dur in events:
        if t0_ns <= start <= t1_ns and 0 <= gen <= 2:
            counts[gen] += 1
            total[gen] += dur
            worst[gen] = max(worst[gen], dur)
    span = max(1, t1_ns - t0_ns)
    return {
        "collections": {f"gen{g}": counts[g] for g in range(3)},
        "pause_total_ms": round(sum(total) / 1e6, 2),
        "pause_max_ms": {f"gen{g}": round(worst[g] / 1e6, 2) for g in range(3)},
        "paused_pct_of_window": round(100.0 * sum(total) / span, 2),
    }


def profile_lines(prof: Any, *, top: int = 25) -> list[str]:
    """Top functions of a ``cProfile.Profile`` by own time, then by cumulative time (pstats text lines)."""
    import io
    import pstats

    out: list[str] = ["(CPython 3.12 cProfile records every thread of the process; cumulative times include time "
                      "spent waiting for the GIL or I/O)"]
    for key, n in (("tottime", top), ("cumulative", max(10, top // 2))):
        buf = io.StringIO()
        pstats.Stats(prof, stream=buf).sort_stats(key).print_stats(n)
        lines = [ln.rstrip() for ln in buf.getvalue().splitlines() if ln.strip()]
        # keep the table (from the header line on), drop pstats' preamble
        start = next((i for i, ln in enumerate(lines) if ln.lstrip().startswith("ncalls")), 0)
        out += [f"--- by {key} ---", *lines[start:]]
    return out


def fmt_us(v: float | None) -> str:
    """A latency in µs, printed with a readable unit."""
    if v is None:
        return "-"
    if v >= 1e6:
        return f"{v / 1e6:.2f} s"
    if v >= 1e3:
        return f"{v / 1e3:.2f} ms"
    return f"{v:.1f} µs"


def fmt_value(m: Metric, v: float | None) -> str:
    if v is None:
        return "-"
    if m.kind == "latency":
        return fmt_us(v)
    if m.kind == "duration":
        return f"{v:.3f} s"
    if m.kind == "rate":
        return f"{v:,.0f} {m.unit}"
    if m.kind == "ratio":
        return f"{v:.3f}"
    return f"{v:,.0f}"


__all__ = [
    "BenchContext",
    "BenchResult",
    "GcMonitor",
    "Metric",
    "SizeMode",
    "Target",
    "Verdict",
    "durations",
    "fmt_us",
    "fmt_value",
    "from_summary",
    "gc_window",
    "latency",
    "latency_ns",
    "now_ns",
    "percentile",
    "profile_lines",
    "scalar",
]
