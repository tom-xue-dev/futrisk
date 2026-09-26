"""``app.yaml``: run mode, demo clock, EOD schedule, logging, API keys."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class DemoConfig(_Strict):
    start_at: str | None = None  # ISO local time (Beijing); None = next trading day 08:58
    pre_close_at: str = "14:58"
    lead_s: int = 120
    auto_seed: bool = True


class LoggingConfig(_Strict):
    level: str = "INFO"
    retention_days: int = 30
    rotation_mb: int = 100


class ApiKeyConfig(_Strict):
    name: str
    role: Literal["VIEWER", "TRADER", "RISK_MANAGER", "ADMIN"]
    key_env: str  # environment variable holding the plaintext key


class NotifyConfig(_Strict):
    webhook_url_env: str | None = None
    channel: Literal["wecom", "dingtalk", "feishu", "log"] = "log"
    min_severity: Literal["INFO", "WARNING", "HIGH", "CRITICAL"] = "HIGH"  # risk.event severities below are not sent


class AppConfig(_Strict):
    mode: Literal["demo", "prod"] = "demo"
    demo: DemoConfig = DemoConfig()
    eod_start_at: str = "15:30"
    logging: LoggingConfig = LoggingConfig()
    api_keys: list[ApiKeyConfig] = Field(default_factory=list)
    notify: NotifyConfig = NotifyConfig()
