"""Simulated external components (01 §5.4 ``src/futrisk/sim/``).

- :mod:`futrisk.sim.exchange`: the virtual exchange ``sim-exchange`` (price engine,
  Feed A / Feed B market data, SimBroker trading counter, control API, scenarios);
- ``futrisk.sim.bots``: the strategy bots (``strategy-sim``).

This package intentionally imports nothing so that importing one component does not
pull in the heavy dependencies of the other.
"""
