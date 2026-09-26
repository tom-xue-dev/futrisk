"""``sim.yaml``: virtual exchange (price engine, feeds, broker) parameters."""

from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class SimInstrument(_Strict):
    pre_settle: Decimal
    vol_annual: float = 0.20
    jump_intensity_per_day: float = 0.5
    jump_sigma: float = 0.01
    volume_per_s: float = 5.0
    open_interest: int = 100_000


class SimPriceEngine(_Strict):
    seed: int = 20260928
    tick_interval_ms: int = 500
    sector_correlation: float = 0.6
    market_correlation: float = 0.2
    instruments: dict[str, SimInstrument] = Field(default_factory=dict)


class SimFeedA(_Strict):
    host: str = "127.0.0.1"
    port: int = 7001
    ring_size: int = 100_000
    heartbeat_ms: int = 1000


class SimFeedB(_Strict):
    host: str = "127.0.0.1"
    port: int = 7002


class SimBroker(_Strict):
    endpoint: str = "tcp://127.0.0.1:7003"
    front_id: int = 1
    ack_delay_ms: int = 5
    fill_delay_ms: int = 20


class SimControl(_Strict):
    host: str = "127.0.0.1"
    port: int = 7004


class SimConfig(_Strict):
    price_engine: SimPriceEngine = SimPriceEngine()
    feed_a: SimFeedA = SimFeedA()
    feed_b: SimFeedB = SimFeedB()
    broker: SimBroker = SimBroker()
    control: SimControl = SimControl()
