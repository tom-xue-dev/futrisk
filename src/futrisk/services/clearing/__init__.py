"""clearing-worker: EOD saga, settlement persistence, SOD versions, reports, backtest jobs (01 §5.3.4, §6.6).

Module map:

- :mod:`.schema` - ``clearing.db`` tables (03 §4.4) and the frozen v1 migration;
- :mod:`.store` - typed reads/writes of those tables (row <-> payload);
- :mod:`.copies` - local copies of ``td.exec.*`` trades and ``risk.cash.*`` movements;
- :mod:`.digests` - three-way trade digest and cash digest reconciliation (02 §8.4 step 3);
- :mod:`.prices` - settlement price priority: manual > official > md inputs > fallback chain;
- :mod:`.pushes` - ``eod.rollover`` pushes, ``pushed_ns`` and re-push (补推) to returning shards;
- :mod:`.saga` - the EOD saga (02 §8.4) persisted in ``eod_runs``, resumable after restarts;
- :mod:`.eodwarn` - WARN events of runs (``sys.eod`` ``detail.warning``: late facts, official price
  differences, incomplete pushes, pre_settle mismatches);
- :mod:`.confighist` - the configuration in force per trading day (``config_history``);
- :mod:`.scheduler` - starts the saga at ``app.eod_start_at`` market time;
- :mod:`.commands` - ``settle.*``, ``report.*`` and ``backtest.*`` command handlers (02 §6.5);
- :mod:`.reports` - daily risk report, statements and settlement JSON (01 §5.3.4, 03 §5.3);
- :mod:`.backtests` - backtest jobs in the process pool (01 §8.16);
- :mod:`.maintenance` - checkpoint, backup and purges after ``sys.eod`` DONE (03 §8);
- :mod:`.jobs` - spawn-safe process-pool entry points;
- :mod:`.service` - :class:`ClearingService` wiring everything to the runtime;
- :mod:`.main` - ``futrisk svc clearing-worker`` entry point ``run()``.
"""

from futrisk.services.clearing.settings import ClearingSettings

__all__ = ["ClearingSettings"]
