"""``strategy-sim``: the strategy bots of the demo accounts (01 §3.1, §7.1).

One process runs the bots of all five accounts over one :class:`futrisk.client.FutriskClient`:

- :mod:`~futrisk.sim.bots.strategies` - pure, tick-driven strategy logic per account;
- :mod:`~futrisk.sim.bots.executor` - targets -> orders (slicing, SHFE close offsets,
  rate limit, cancels, in-doubt resubmission);
- :mod:`~futrisk.sim.bots.gates` - back-off after rejects (02 §11.2);
- :mod:`~futrisk.sim.bots.main` - ``run()`` entry point (``futrisk svc strategy-sim``);
- ``bots.yaml`` - documented default configuration.

Kept import-light on purpose: the service entry imports ``futrisk.sim.bots.main``.
"""
