"""System payloads: heartbeat, market clock, supervisor, config 2PC (02 §6.6-§6.7, §7.9-§7.10)."""

from __future__ import annotations

from typing import Any

import msgspec

from futrisk.messages.base import Msg, payload
from futrisk.messages.enums import (
    ClockMode,
    ClockTarget,
    ConfigEffect,
    ConfigStatus,
    ProcessState,
    ServiceChangeReason,
    ServiceState,
    TradingMode,
)


class LatencySummary(msgspec.Struct, kw_only=True):
    p50: float
    p99: float
    max: float
    n: int


@payload()
class Heartbeat(Msg, kw_only=True):
    instance: str
    service: str
    state: ServiceState
    pid: int
    epoch: str
    app_version: str
    proto: int = 1
    versions: dict[str, list[int]] = {}
    config_version: int = 0
    config_hash: str = ""
    clock_version: int = 0
    loop_lag_ms: int = 0
    stream: str | None = None
    fact_heads: dict[str, int] = {}
    started_ns: int
    trading_day: str | None = None
    trading_mode: TradingMode | None = None
    queues: dict[str, int] = {}
    rates: dict[str, float] = {}
    latency_us: dict[str, LatencySummary] = {}
    deps: dict[str, str] = {}
    detail: dict[str, Any] = {}


@payload()
class ClockState(Msg, kw_only=True):
    mode: ClockMode
    offset_ns: int
    version: int
    market_now_ns: int
    actor: str | None = None
    reason: str | None = None


@payload()
class ClockJumpRequest(Msg, kw_only=True):
    command_id: str
    target: ClockTarget
    at_ns: int | None = None
    actor: str = "system"


@payload()
class ServiceStateChanged(Msg, kw_only=True):
    instance: str
    service: str
    pid: int | None = None
    from_state: ProcessState
    to_state: ProcessState
    exit_code: int | None = None
    restarts: int = 0
    reason: ServiceChangeReason
    actor: str | None = None
    trading_day: str | None = None
    ts_ns: int
    # optional (supervisor extensions): names (never values) of the instance's environment overrides,
    # and a human-readable cause, e.g. "port 7502 in use by pid 4242 (python.exe)"
    env_keys: list[str] | None = None
    detail: str | None = None


@payload()
class ServiceInfo(Msg, kw_only=True):
    instance: str
    service: str
    state: ProcessState
    pid: int | None = None
    started_ns: int | None = None
    restarts: int = 0
    last_exit_code: int | None = None
    heartbeat_state: ServiceState | None = None
    last_heartbeat_ns: int | None = None
    # optional (supervisor extensions): environment override names in effect, last failure cause
    env_keys: list[str] | None = None
    last_error: str | None = None


@payload()
class PsRequest(Msg, kw_only=True):
    pass


@payload()
class ServiceList(Msg, kw_only=True):
    items: list[ServiceInfo]
    home_id: str | None = None  # the supervisor's HomeLayout.home_id() (cross-home guard)


@payload()
class SvcRequest(Msg, kw_only=True):
    command_id: str
    instance: str
    actor: str = "cli"
    # optional (supervisor extension): environment overrides of this instance (per-instance allow-list,
    # e.g. strategy-sim FUTRISK_BOTS_MODE), kept for its later restarts; a null value unsets a name;
    # accepted by svc.start / svc.restart of a single instance
    env: dict[str, str | None] | None = None


@payload()
class SvcHistoryRequest(Msg, kw_only=True):
    trading_day: str | None = None
    since_ns: int | None = None


@payload()
class ServiceEventBatch(Msg, kw_only=True):
    items: list[ServiceStateChanged]


# ---------------------------------------------------------------- config 2PC
@payload()
class ConfigPrepare(Msg, kw_only=True):
    config_version: int
    config_hash: str
    path: str
    files: dict[str, str]
    requested_by: str


@payload()
class ConfigPrepared(Msg, kw_only=True):
    instance: str
    config_version: int
    ok: bool
    errors: list[str] = []
    effects: dict[str, ConfigEffect] = {}
    computed_hash: str = ""


@payload()
class ConfigCommit(Msg, kw_only=True):
    config_version: int


@payload()
class ConfigAbort(Msg, kw_only=True):
    config_version: int
    reason: str = ""


@payload()
class DecisionQuery(Msg, kw_only=True):
    config_version: int


@payload()
class ConfigDecision(Msg, kw_only=True):
    config_version: int
    status: ConfigStatus
