"""Backtests that verify the effectiveness of the risk rules (01 §8.16).

The backtest runs the **same** :class:`~futrisk.core.engine.EngineCore` and the
same clearing library (:func:`futrisk.core.settle_day`) as production, on an
:class:`~futrisk.bus.inproc.InprocBus` with a deterministic in-process broker;
no service is started. Two kinds of run:

- ``arms`` (layer A): daily bars since 2015 replayed as a synthesized intraday
  path, a dual moving-average strategy per account, and the same days replayed
  once without rules (``no_rules``) and once with the production daily-loss
  rule (``block_open``); the difference is the rule's effect;
- ``scenario`` (layer D): synthetic scenarios with known answers and exact
  checks (``breaker_synthetic``: the 3 % acceptance scenario).

Modules:

- :mod:`.config` - ``config/backtest/*.yaml`` models, overrides, config hash;
- :mod:`.data` - layer A loader (``data/hist``, hashed when read), historical contract universe;
- :mod:`.paths` - the deterministic intraday path of a daily bar, trading timeline;
- :mod:`.sources` - day plans of layers A (daily) and D (scenarios);
- :mod:`.broker` - the in-process trade-gateway and its fill model;
- :mod:`.strategies` / :mod:`.executor` - the dual moving-average strategy, targets → orders;
- :mod:`.arms` - the rule set of each arm (``none`` / ``block_open``);
- :mod:`.replay` - the day loop (SOD roll → replay → settlement prices → settle_day);
- :mod:`.analysis` - per-arm statistics, the trigger days, the scenario checks;
- :mod:`.report` - self-contained ``report.html``; :mod:`.manifest` - reproducibility hashes;
- :mod:`.runner` - outputs and ``run_job`` (the clearing-worker entry point).

Imports stay light here: the clearing-worker imports :mod:`.runner` inside a spawn worker.
"""
