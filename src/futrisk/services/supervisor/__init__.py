"""supervisor: process manager, health monitor, restart policy and market clock (01 §5.3.6).

Components (01 §5.3.6, 02 §4.8, §6.6, §8.8; 03 §4.6):

* :mod:`~futrisk.services.supervisor.process` - ``ProcessManager``: spawns every service
  process (``futrisk svc <service> --home H --parent-pid P``) in its own process group,
  console output (ANSI-free, size-rotated) to ``<home>/logs/<instance>.console.log``;
  port pre-check, per-instance environment allow-list, graceful and hard termination,
  descendants reaped;
* :mod:`~futrisk.services.supervisor.supervisor` - ``Supervisor``: per-instance state
  machine (``ProcessState``), startup in ``supervisor.startup_order`` waiting only for
  liveness (heartbeats of the child's own process tree), ``HealthMonitor`` rules (exit,
  heartbeat loss, loop hang, sim-exchange ``/v1/health``), exponential-backoff restarts and
  give-up, reverse-order shutdown, ``svc.*`` commands (``ServiceList.home_id``);
* :mod:`~futrisk.services.supervisor.policy` - restart backoff;
* :mod:`~futrisk.services.supervisor.events` - ``service_events`` + ``sys.service`` (FACT);
* :mod:`~futrisk.services.supervisor.schema` / ``maintenance`` - supervisor.db (migration 2:
  ``service_events`` v2) and its daily checkpoint / backup / purge;
* :mod:`~futrisk.services.supervisor.clock_keeper` - ``ClockKeeper``: the market clock
  (``clock_state``, ``sys.clock``, ``svc.clock``);
* :mod:`~futrisk.services.supervisor.main` - ``run()`` entry point (``futrisk up`` /
  ``futrisk svc supervisor``);
* :mod:`~futrisk.services.supervisor.launch_sim` - ``run()`` for ``futrisk svc sim-exchange``.

The package ``__init__`` stays import-light on purpose: ``launch_sim`` is imported by
every sim-exchange process.
"""
