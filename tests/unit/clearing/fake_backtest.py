"""A stand-in for ``futrisk.backtest.runner.run_job`` (imported by the pool workers in the tests)."""

from __future__ import annotations

import time
from collections.abc import Callable
from pathlib import Path
from typing import Any


def run_job(config_name: str, overrides: dict[str, Any] | None, out_dir: str, progress: Callable[..., None]) -> dict[str, Any]:
    for i in range(1, 5):
        progress(i / 4, f"step {i}")
        time.sleep(0.05)
    Path(out_dir, "report.html").write_text(
        f"<html><body><h1>backtest {config_name}</h1></body></html>", encoding="utf-8"
    )
    return {
        "summary": {"rules": {"max_drawdown": "0.051"}, "no_rules": {"max_drawdown": "0.083"}},
        "config_hash": "a" * 64,
        "data_hash": "b" * 64,
        "seed": int((overrides or {}).get("seed", 7)),
        "git_sha": "deadbeef",
    }


def failing_job(config_name: str, overrides: dict[str, Any] | None, out_dir: str, progress: Callable[..., None]) -> dict[str, Any]:
    raise ValueError(f"unknown backtest config {config_name}")


def slow_job(config_name: str, overrides: dict[str, Any] | None, out_dir: str, progress: Callable[..., None]) -> dict[str, Any]:
    """Runs for a minute unless its worker is terminated (shutdown tests)."""
    for i in range(1200):
        progress(i / 1200, "working")
        time.sleep(0.05)
    return {}


def escaping_job(config_name: str, overrides: dict[str, Any] | None, out_dir: str, progress: Callable[..., None]) -> dict[str, Any]:
    """A result that breaks the contract: report outside ``out_dir``, wrong types for seed and summary."""
    outside = Path(out_dir).parent / "outside.html"
    outside.write_text("<html>not mine</html>", encoding="utf-8")
    return {"report_path": str(outside), "seed": "7", "summary": [1, 2], "git_sha": "cafe"}
