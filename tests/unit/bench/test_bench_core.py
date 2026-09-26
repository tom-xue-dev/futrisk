"""Statistics, targets, verdicts and the report rendering of the benchmark suite."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from futrisk.bench.core import BenchResult, Target, durations, latency, latency_ns, percentile, scalar
from futrisk.bench.runner import Report, render_markdown, resolve_names, write_json


def test_nearest_rank_percentiles() -> None:
    data = sorted(float(x) for x in range(1, 101))  # 1..100
    assert percentile(data, 0.50) == 50.0
    assert percentile(data, 0.90) == 90.0
    assert percentile(data, 0.99) == 99.0
    assert percentile(data, 1.0) == 100.0
    assert percentile([7.0], 0.99) == 7.0
    with pytest.raises(ValueError):
        percentile([], 0.5)


def test_latency_metric_and_targets_in_other_units() -> None:
    m = latency("x", [100.0] * 98 + [2500.0, 4000.0], targets=[Target("p99", "<=", 3, "ms")], primary=True)
    assert (m.n, m.p50, m.p99, m.max) == (100, 100.0, 2500.0, 4000.0)
    assert m.verdict == "PASS"  # p99 = 2.5 ms
    m2 = latency_ns("y", [1_000_000, 5_000_000], targets=[Target("p99", "<=", 3, "ms")])
    assert m2.p99 == 5000.0 and m2.verdict == "MISS"
    both = latency("z", [40.0] * 99 + [300.0], targets=[Target("p50", "<=", 50, "us"), Target("p99", "<=", 250, "us")])
    assert both.verdict == "PASS"  # p99 of 100 samples is the 99th value (40)
    assert "≤ 50 µs" in both.target_text()


def test_scalar_duration_and_bench_verdicts() -> None:
    rate = scalar("rate", 120_000, "msg/s", targets=[Target("value", ">=", 100_000, "msg/s")], primary=True)
    assert rate.verdict == "PASS"
    d = durations("start", [1.5, 2.0, 3.2], targets=[Target("max", "<", 3, "s")], primary=True)
    assert d.verdict == "MISS" and d.value == 2.0
    res = BenchResult(name="b", title="t", target="x", quick=True, metrics=[rate, scalar("info", 1, "n", kind="count")])
    assert res.decide() == "PASS"
    res.metrics.append(d)
    assert res.decide() == "MISS"
    res.error = "boom"
    assert res.decide() == "ERROR"
    assert BenchResult(name="c", title="t", target="x", quick=True).decide() == "N/A"


def test_resolve_names() -> None:
    assert resolve_names(None)[0] == "bus" and len(resolve_names(["all"])) == 7
    assert resolve_names(["order-rtt", "bus", "bus"]) == ["order_rtt", "bus"]
    with pytest.raises(ValueError):
        resolve_names(["nope"])


def test_report_json_and_markdown(tmp_path: Path) -> None:
    res = BenchResult(name="bus", title="Bus", target="≥ 100,000 msg/s", quick=True)
    res.add(scalar("delivered", 90_000, "msg/s", n=1000, targets=[Target("value", ">=", 100_000, "msg/s")], primary=True))
    res.add(latency("one-way", [100.0, 200.0, 300.0]))
    res.decide()
    report = Report(started_at="2026-09-25T00:00:00+08:00", finished_at="2026-09-25T00:01:00+08:00", mode="quick",
                    machine={"cpu": "test cpu", "cores_physical": 8, "cores_logical": 16}, baseline_load={"system_pct": 3.0},
                    benches=[res])
    path = write_json(report, tmp_path / "bench" / "r.json")
    doc = json.loads(path.read_text(encoding="utf-8"))
    assert doc["benches"][0]["verdict"] == "MISS" and doc["benches"][0]["metrics"][0]["value"] == 90_000
    md = render_markdown(report)
    assert "| bus | **delivered** | 1,000 |" in md and "**MISS**" in md and "test cpu" in md
