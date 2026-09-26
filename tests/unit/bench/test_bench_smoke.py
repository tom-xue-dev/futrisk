"""Every bench end to end with tiny sizes: the result structure only (no performance assertion).

The multi-process benches start real child processes (fakes, the risk-engine through the production entry
point, a bus subscriber) on free loopback ports in ``tmp_path``; each test checks that no child it started is
left running.
"""

from __future__ import annotations

import json
from pathlib import Path

import psutil
import pytest
from typer.testing import CliRunner

from futrisk.bench import NAMES
from futrisk.bench.core import BenchContext, BenchResult
from futrisk.bench.runner import BENCHES, run_one

MULTI_PROCESS = {"bus", "tick2decision", "order_rtt", "recovery"}


def _children() -> set[int]:
    return {p.pid for p in psutil.Process().children(recursive=True)}


def _check(res: BenchResult, name: str) -> None:
    assert res.error is None, f"{name} failed: {res.error}\n{res.extra.get('traceback', '')}"
    assert res.name == name and res.title and res.target
    assert res.verdict in ("PASS", "MISS", "N/A")
    primary = res.primary()
    assert primary is not None and primary.targets, "every bench is judged on a primary metric with a target"
    assert primary.verdict in ("PASS", "MISS")
    assert primary.n > 0 or primary.value is not None
    for m in res.metrics:
        if m.kind == "latency" and m.n:
            assert m.p50 is not None and m.max is not None and m.p50 <= m.max
    assert res.load.get("samples", 0) >= 1
    json.dumps(res.to_dict(), default=str)  # serialisable


def test_registry_is_complete() -> None:
    assert set(NAMES) == set(BENCHES)


@pytest.mark.timeout(300)
@pytest.mark.parametrize("name", ["pretrade", "replay", "ingest"])
def test_in_process_bench_tiny(name: str, tmp_path: Path) -> None:
    res = run_one(name, BenchContext(mode="tiny", tmp_root=str(tmp_path)))
    _check(res, name)


@pytest.mark.slow
@pytest.mark.timeout(600)
@pytest.mark.parametrize("name", sorted(MULTI_PROCESS))
def test_multi_process_bench_tiny(name: str, tmp_path: Path) -> None:
    before = _children()
    res = run_one(name, BenchContext(mode="tiny", tmp_root=str(tmp_path), heartbeat_ms=100))
    _check(res, name)
    assert _children() - before == set(), "a child process of the bench is still running"
    assert not any(tmp_path.iterdir()), "temporary homes are removed after the bench"


@pytest.mark.timeout(300)
def test_cli_json(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from futrisk.cli import bench_cmd

    monkeypatch.setenv("FUTRISK_HOME", str(tmp_path / "home"))
    md = tmp_path / "out.md"
    result = CliRunner().invoke(bench_cmd.app, ["pretrade", "--tiny", "--json", "--out", str(md),
                                                "--set", "pretrade.n=50"])
    assert result.exit_code == 0, result.output
    doc = json.loads(result.stdout[result.stdout.index("{"):])
    assert doc["mode"] == "tiny" and doc["benches"][0]["name"] == "pretrade"
    assert doc["benches"][0]["params"]["calls_per_metric"] == 50
    assert Path(doc["files"]["json"]).is_file() and Path(doc["files"]["json"]).parent == tmp_path / "home" / "bench"
    assert "| pretrade |" in md.read_text(encoding="utf-8")
    assert doc["machine"]["cpu"] and doc["machine"]["cores_logical"] >= 1
