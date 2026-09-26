"""Service runtime framework (02 §4.8, §8.1, §8.5, §8.6).

* :class:`AsyncServiceRuntime` - asyncio services (md-gateway, clearing-worker,
  api-gateway, supervisor).
* :class:`SyncServiceRuntime` - sync poller services (risk-engine, trade-gateway).
* :func:`run_async` - run an asyncio service on a selector loop (Windows).
* :mod:`futrisk.runtime.testing` - free ports / fast bus timings for tests.
"""

from futrisk.runtime.async_runtime import AsyncServiceRuntime, AsyncSubscription
from futrisk.runtime.base import API_GATEWAY, SUPERVISOR, RuntimeBase
from futrisk.runtime.clock_sync import ClockFollower
from futrisk.runtime.config2pc import ConfigParticipant, compute_effects
from futrisk.runtime.loop import ensure_selector_loop, run_async, selector_loop_factory
from futrisk.runtime.sync_runtime import SyncServiceRuntime, SyncSubscription

__all__ = [
    "API_GATEWAY",
    "SUPERVISOR",
    "AsyncServiceRuntime",
    "AsyncSubscription",
    "ClockFollower",
    "ConfigParticipant",
    "RuntimeBase",
    "SyncServiceRuntime",
    "SyncSubscription",
    "compute_effects",
    "ensure_selector_loop",
    "run_async",
    "selector_loop_factory",
]
