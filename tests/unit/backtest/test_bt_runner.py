"""``run_job`` contract (the clearing-worker entry point), incl. a real spawn process pool, and the CLI smoke test."""

from __future__ import annotations

import json
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import yaml
from typer.testing import CliRunner

from futrisk.backtest.config import resolve_config_path
from futrisk.backtest.runner import run_job
from futrisk.services.clearing.jobs import FileProgress, backtest_job


def small_config(tmp_path: Path) -> Path:
    """The acceptance scenario only, two arms: a sub-second job."""
    raw = yaml.safe_load(resolve_config_path("breaker_synthetic").read_text(encoding="utf-8"))
    raw["name"] = "tiny"
    raw["scenarios"] = raw["scenarios"][:1]
    raw["arms"] = [a for a in raw["arms"] if a["name"] in ("no_rules", "block_open")]
    p = tmp_path / "tiny.yaml"
    p.write_text(yaml.safe_dump(raw, allow_unicode=True), encoding="utf-8")
    return p


def test_run_job_contract(tmp_path: Path) -> None:
    calls: list[tuple[float, str | None]] = []
    out = tmp_path / "out"
    res = run_job(str(small_config(tmp_path)), {"workers": 1}, str(out), lambda f, m=None: calls.append((f, m)))
    for key in ("summary", "config_hash", "data_hash", "seed", "git_sha", "report_path"):
        assert key in res, key
    assert isinstance(res["summary"], dict) and res["summary"]["all_passed"] is True
    assert len(res["config_hash"]) == 64 and len(res["data_hash"]) == 64
    assert res["git_sha"] == "nogit" or len(res["git_sha"]) == 40
    assert res["seed"] == 20260928 and res["report_path"] == "report.html"
    for name in ("report.html", "metrics.csv", "events.parquet", "manifest.json"):
        assert (out / name).exists(), name
    fracs = [f for f, _m in calls]
    assert fracs[0] == 0.0 and fracs[-1] == 1.0
    assert all(b >= a for a, b in zip(fracs, fracs[1:], strict=False)) and len(set(fracs)) > 3
    m = json.loads((out / "manifest.json").read_text(encoding="utf-8"))
    assert m["events_hash"] == res["events_hash"] and m["seed"] == 20260928 and m["versions"]["python"]


def test_run_job_in_a_spawn_process_pool_with_its_own_pool(tmp_path: Path) -> None:
    # the clearing-worker's spawn worker runs the job, whose scheduler starts a second (nested) spawn pool
    cfg = small_config(tmp_path)
    out = tmp_path / "pool"
    with ProcessPoolExecutor(max_workers=1, mp_context=multiprocessing.get_context("spawn")) as pool:
        fut = pool.submit(backtest_job, "futrisk.backtest.runner:run_job", str(cfg), {"workers": 2}, str(out))
        res = fut.result(timeout=240)
    assert res["summary"]["all_passed"] is True and (out / "report.html").exists()
    progress = json.loads((out / "progress.json").read_text(encoding="utf-8"))
    assert progress["progress"] == 1.0
    one = tmp_path / "inline"
    res1 = run_job(str(cfg), {"workers": 1}, str(one), lambda f, m=None: None)
    assert res1["events_hash"] == res["events_hash"]  # the number of workers does not change the result


def test_file_progress_is_picklable_callback(tmp_path: Path) -> None:
    fp = FileProgress(str(tmp_path / "p.json"))
    fp(0.5, "half")
    assert json.loads((tmp_path / "p.json").read_text(encoding="utf-8"))["progress"] == 0.5


def test_cli_run_breaker_synthetic(tmp_path: Path) -> None:
    from futrisk.main import _load_subapps, app

    _load_subapps()
    out = tmp_path / "cli"
    r = CliRunner().invoke(app, ["backtest", "run", "breaker_synthetic", "--out", str(out)])
    assert r.exit_code == 0, r.output
    assert "16/16" in r.output
    assert (out / "report.html").exists() and (out / "events.parquet").exists()
    r2 = CliRunner().invoke(app, ["backtest", "show", str(out)])
    assert r2.exit_code == 0 and "events_hash" in r2.output
    r3 = CliRunner().invoke(app, ["backtest", "list"])
    assert r3.exit_code == 0 and "breaker_synthetic" in r3.output
