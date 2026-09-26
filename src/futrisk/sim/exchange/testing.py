"""Helpers for running sim-exchange inside tests (this package's and other services').

Example (md-gateway / trade-gateway integration tests)::

    from futrisk.sim.exchange import SimExchange
    from futrisk.sim.exchange.testing import MARKET_OPEN, make_test_config, offset_for

    cfg = make_test_config(load_default_config())      # ephemeral ports, 50 ms ticks
    async with SimExchange(cfg, HomeLayout(tmp_path), offset_for(MARKET_OPEN)) as ex:
        ex.feed_a_url, ex.feed_b_url, ex.broker_endpoint, ex.control_url

``MARKET_OPEN`` (Monday 2026-09-28 09:35 Beijing) has every simulated product in a
continuous session, so tests do not depend on the real date.
"""

from __future__ import annotations

import dataclasses
from datetime import datetime
from typing import Any

from futrisk.common.clock import BJ, SystemClock, bj_to_ns
from futrisk.common.config import FullConfig

#: a Monday morning when every simulated product is in a continuous session
MARKET_OPEN = datetime(2026, 9, 28, 9, 35, tzinfo=BJ)


def offset_for(dt: datetime) -> int:
    """Market-clock offset (ns) that makes market time equal ``dt`` now."""
    return bj_to_ns(dt) - SystemClock().now_ns()


def make_test_config(
    base: FullConfig,
    *,
    tick_ms: int = 50,
    hb_ms: int = 200,
    ring_size: int = 100_000,
    ack_ms: int = 0,
    fill_ms: int = 5,
    mode: str = "demo",
    seed: int | None = None,
) -> FullConfig:
    """``base`` with fast ticks and ephemeral ports for every sim-exchange listener.

    The actual endpoints are available after ``start()`` as ``feed_a_url``,
    ``feed_b_url``, ``broker_endpoint`` and ``control_url``.
    """
    s = base.sim
    pe_update: dict[str, Any] = {"tick_interval_ms": tick_ms}
    if seed is not None:
        pe_update["seed"] = seed
    sim = s.model_copy(update={
        "price_engine": s.price_engine.model_copy(update=pe_update),
        "feed_a": s.feed_a.model_copy(update={"port": 0, "heartbeat_ms": hb_ms, "ring_size": ring_size}),
        "feed_b": s.feed_b.model_copy(update={"port": 0}),
        "broker": s.broker.model_copy(update={
            "endpoint": "tcp://127.0.0.1:*", "ack_delay_ms": ack_ms, "fill_delay_ms": fill_ms,
        }),
        "control": s.control.model_copy(update={"port": 0}),
    })
    app = base.app.model_copy(update={"mode": mode})
    return dataclasses.replace(base, sim=sim, app=app)

