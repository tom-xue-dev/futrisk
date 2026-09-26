"""Process-pool entry points of the clearing-worker (01 §8.12: 进程池（结算、报表、回测）).

Every function here is a top-level, import-safe callable with picklable
arguments, as required by the ``spawn`` start method used on Windows. Heavy
imports (matplotlib, Jinja2, the backtester) happen inside the functions so
that a worker only loads what its job needs.
"""

from __future__ import annotations

import importlib
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

from futrisk.common.calendar import TradingCalendar
from futrisk.common.config.products import ProductsConfig
from futrisk.core import DaySettlement, InstrumentRegistry, settle_day
from futrisk.services.clearing.fsutil import replace_file


def settle_job(products: ProductsConfig, calendar: TradingCalendar, params: dict[str, Any]) -> DaySettlement:
    """Run :func:`futrisk.core.settle_day` (pure, Decimal-exact) in a worker process."""
    registry = InstrumentRegistry(products, calendar)
    return settle_day(registry=registry, **params)


def render_reports_job(bundle: dict[str, Any], home: str) -> list[dict[str, Any]]:
    """Render the day's report files under ``<home>/reports/<td>/`` (see :mod:`.reports.render`)."""
    from futrisk.services.clearing.reports.render import render_all

    return render_all(bundle, home)


class BacktestUnavailable(RuntimeError):
    """The backtest engine module cannot be imported."""


class FileProgress:
    """Picklable progress callback: writes ``{"progress": x, "message": m}`` atomically to a JSON file.

    The clearing-worker polls the file and publishes ``backtest.status`` (LV).
    Progress is best-effort: a write that keeps failing (the service holds the
    file open for reading on Windows) is skipped, never failing the job.
    """

    def __init__(self, path: str) -> None:
        self.path = path

    def __call__(self, fraction: float, message: str | None = None) -> None:
        value = min(1.0, max(0.0, float(fraction)))
        p = Path(self.path)
        tmp = p.with_suffix(".tmp")
        try:
            p.parent.mkdir(parents=True, exist_ok=True)
            tmp.write_text(json.dumps({"progress": value, "message": message}, ensure_ascii=False), encoding="utf-8")
            replace_file(tmp, p, attempts=8)
        except OSError:
            pass  # the next progress call writes again


def resolve_runner(spec: str) -> Callable[..., Any]:
    """``"package.module:function"`` -> callable; :class:`BacktestUnavailable` when it cannot be imported."""
    module, _, name = spec.partition(":")
    try:
        mod = importlib.import_module(module)
    except ImportError as exc:
        raise BacktestUnavailable(f"backtest engine not available: cannot import {module} ({exc})") from exc
    fn = getattr(mod, name or "run_job", None)
    if fn is None or not callable(fn):
        raise BacktestUnavailable(f"backtest engine not available: {module} has no callable {name or 'run_job'}")
    return fn  # type: ignore[no-any-return]


def backtest_job(runner_spec: str, config_name: str, overrides: dict[str, Any] | None, out_dir: str) -> dict[str, Any]:
    """Run one backtest: ``run_job(config_name, overrides, out_dir, progress) -> dict`` (01 §8.16)."""
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    runner = resolve_runner(runner_spec)
    progress = FileProgress(str(Path(out_dir) / "progress.json"))
    result = runner(config_name, overrides, out_dir, progress)
    if result is None:
        return {}
    if not isinstance(result, dict):
        raise TypeError(f"backtest runner returned {type(result).__name__}, expected dict")
    return result


__all__ = [
    "BacktestUnavailable",
    "FileProgress",
    "backtest_job",
    "render_reports_job",
    "resolve_runner",
    "settle_job",
]
