"""Backtests that verify the effectiveness of the risk rules (01 §8.16, plan §7.1).

The backtest runs the **same** :class:`~futrisk.core.engine.EngineCore` and the
same clearing library (:func:`futrisk.core.settle_day`) as production, on an
:class:`~futrisk.bus.inproc.InprocBus` with a deterministic in-process broker;
no service is started.

Modules:

- :mod:`.config` - ``config/backtest/*.yaml`` models, overrides, config hash;
- :mod:`.data` - layer A/B loaders (``data/hist``, hashed when read), historical contract universe;
- :mod:`.paths` - deterministic intraday paths, trading timeline, bounding classes, hit rate;
- :mod:`.pathmodel` - the stochastic path ensemble calibrated on the minute bars;
- :mod:`.sources` - day plans of layers A (daily), B (minute), C (recorded ticks), D (scenarios);
- :mod:`.broker` - the in-process trade-gateway and its fill model;
- :mod:`.strategies` / :mod:`.executor` - the five demo account styles on bars, targets → orders;
- :mod:`.arms` - rule sets of the experiment arms (no_rules, alert_only, block_open, halt_only, halve,
  flatten, placebo);
- :mod:`.replay` - the day loop (SOD roll → replay → settlement prices → settle_day);
- :mod:`.experiment` - groups × path realizations × arms × accounts on a spawn pool (stages, copies);
- :mod:`.metrics` / :mod:`.analysis` - statistics, bootstraps, TP/FP, effects per mark, ensemble
  aggregation, sensitivity, controls, leave-one-out, sweep optimum / plateau;
- :mod:`.report` - self-contained ``report.html``; :mod:`.manifest` - reproducibility hashes;
- :mod:`.runner` - calibration, outputs and ``run_job`` (the clearing-worker entry point);
- :mod:`.bench` - replay throughput.

Imports stay light here: the clearing-worker imports :mod:`.runner` inside a spawn worker.
"""
