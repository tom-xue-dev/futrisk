"""risk-engine service shell: sockets, SQLite, recovery, EOD and coordinator duties around the EngineCore.

* :mod:`.service` - :class:`RiskEngineService` (runtime wiring, the 02 §8.1 loop, effect execution,
  start-up phases, EOD, coordinator, config reload, maintenance trigger);
* :mod:`.schema` - risk-{shard}.db tables (03 §4.3);
* :mod:`.store` - Persist rows -> SQL, restore loading, history queries, retention;
* :mod:`.maintenance` - 03 §8 maintenance after ``sys.eod`` DONE;
* :mod:`.stats` - latency / rate statistics for the heartbeat;
* :mod:`.main` - ``futrisk svc risk-engine --shard N``.
"""
