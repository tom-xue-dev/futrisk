"""Tunables of the clearing-worker (timeouts and retries follow 02 §4.7 and §8.4).

Production uses the defaults; tests shorten the timers.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ClearingSettings:
    """Timeouts, retries and background intervals of the clearing-worker.

    Defaults: ``eod.*`` 30 s per attempt, at most 3 retries (02 §4.7, §8.4 steps 2 and 6);
    at most 3 re-freezes when digests disagree (step 3); re-push to unconfirmed shards
    when their heartbeat returns and every ``repush_interval_s`` as a safety net.
    """

    freeze_timeout_ms: int = 30_000
    freeze_retries: int = 3
    rollover_timeout_ms: int = 30_000
    rollover_retries: int = 3
    repush_interval_s: float = 30.0
    recon_refreezes: int = 3
    query_timeout_ms: int = 10_000
    query_retries: int = 3
    md_timeout_ms: int = 30_000
    md_retries: int = 3
    report_fetch_timeout_ms: int = 5_000
    report_fetch_retries: int = 0
    metrics_step_s: int = 60
    history_days: int = 60
    scheduler_enabled: bool = True
    scheduler_interval_s: float = 5.0
    pool_workers: int = 3
    pool_stop_grace_s: float = 1.0
    backtest_concurrency: int = 1
    backtest_runner: str = "futrisk.backtest.runner:run_job"
    progress_poll_s: float = 0.5
    maintenance_enabled: bool = True
    backup_keep_days: int = 20
    outbox_retention_days: int = 7
    purge_batch: int = 2_000


__all__ = ["ClearingSettings"]
