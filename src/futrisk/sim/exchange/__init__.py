"""sim-exchange: the virtual exchange external component (01 §5.1, §8.13; 02 §9.1-§9.5).

Entry points:

- :func:`run_sim_exchange` - run until a stop event is set (used by ``futrisk svc``);
- :class:`SimExchange` - the application object (``async with`` in tests);
- ``python -m futrisk.sim.exchange --home PATH --clock-offset-ns N`` for manual runs.

See ``src/futrisk/sim/README.md`` for the JSON protocol quick reference.
"""

from futrisk.sim.exchange.app import SimExchange, run_sim_exchange
from futrisk.sim.exchange.engine import InstrumentState, PriceEngine, Quote

__all__ = ["InstrumentState", "PriceEngine", "Quote", "SimExchange", "run_sim_exchange"]
