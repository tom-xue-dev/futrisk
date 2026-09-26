"""``services.yaml``: endpoints, bus parameters, supervision policy (02 §3)."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class GcConfig(_Strict):
    """Garbage-collector policy of the service processes (01 §11 RK-01; ``futrisk.runtime.gcpolicy``)."""

    enabled: bool = True
    threshold: tuple[int, int, int] = (10_000, 20, 100)  # gc.set_threshold at start
    freeze_on_running: bool = True  # gc.collect() + gc.freeze() once, when the service reports RUNNING


class BusConfig(_Strict):
    heartbeat_ms: int = 1000
    peer_timeout_missed: int = 3
    hang_threshold_ms: int = 30_000
    pub_hwm: int = 10_000
    md_pub_hwm: int = 100_000
    router_hwm: int = 1000
    replay_window_days: int = 7
    gc: GcConfig = GcConfig()


class InstanceConfig(_Strict):
    pub: str | None = None
    router: str | None = None
    http: str | None = None
    shard: int | None = None
    coordinator: bool = False


class RestartPolicy(_Strict):
    backoff_initial_ms: int = 1000
    backoff_max_ms: int = 30_000
    stable_reset_s: int = 60


class SupervisorConfig(_Strict):
    startup_order: list[str] = Field(
        default_factory=lambda: [
            "sim-exchange",
            "md-gateway",
            "trade-gateway",
            "clearing-worker",
            "risk-engine-0",
            "risk-engine-1",
            "api-gateway",
            "strategy-sim",
        ]
    )
    external: list[str] = Field(default_factory=lambda: ["sim-exchange", "strategy-sim"])
    restart: RestartPolicy = RestartPolicy()
    liveness_timeout_s: float = 20.0
    shutdown_grace_s: float = 10.0


class ServicesConfig(_Strict):
    profile: str = "demo"
    bus: BusConfig = BusConfig()
    instances: dict[str, InstanceConfig]
    supervisor: SupervisorConfig = SupervisorConfig()

    def instance(self, name: str) -> InstanceConfig:
        try:
            return self.instances[name]
        except KeyError:
            raise KeyError(f"unknown instance {name!r} in services.yaml") from None

    def risk_instances(self) -> list[str]:
        return sorted(n for n in self.instances if n.startswith("risk-engine-"))

    def coordinator(self) -> str:
        for n, c in self.instances.items():
            if c.coordinator:
                return n
        return "risk-engine-0"

    def shard_instance(self, shard: int) -> str:
        return f"risk-engine-{shard}"
