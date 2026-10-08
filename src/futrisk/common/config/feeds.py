"""``feeds.yaml``: market data sources and arbitration parameters (01 §8.13)."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class FeedAConfig(_Strict):
    enabled: bool = True
    url: str = "ws://127.0.0.1:7001/feed"
    priority: int = 1
    reconnect_backoff_ms: list[int] = Field(default_factory=lambda: [200, 500, 1000, 2000, 5000])


class FeedBConfig(_Strict):
    enabled: bool = True
    url: str = "http://127.0.0.1:7002/v1/quotes"
    poll_ms: int = 1000
    timeout_ms: int = 800  # read timeout of one poll (connect timeout: 1 s)
    priority: int = 2


class SinaConfig(_Strict):
    enabled: bool = False
    url: str = "https://hq.sinajs.cn/list="
    referer: str = "https://finance.sina.com.cn"
    poll_ms: int = 1500
    timeout_ms: int = 1200
    priority: int = 3
    trust_env: bool = False  # honor HTTP(S)_PROXY env vars (02 §9.3)
    symbols: dict[str, str] = Field(default_factory=dict)  # instrument_id -> sina code


class ArbitrationConfig(_Strict):
    exclude_missed_heartbeats: int = 3
    exclude_failed_polls: int = 3
    exclude_reject_rate: float = 0.05
    exclude_integrity_votes: int = 2
    probation_healthy_s: float = 10.0
    min_dwell_s: float = 5.0
    reorder_buffer_max: int = 32
    reorder_buffer_ms: int = 100
    stale_s: float = 5.0
    unknown_s: float = 30.0
    open_grace_s: float = 30.0
    live_max_latency_ms: int = 3000
    watermark_flush_s: float = 1.0


class FeedsConfig(_Strict):
    feed_a: FeedAConfig = FeedAConfig()
    feed_b: FeedBConfig = FeedBConfig()
    sina: SinaConfig = SinaConfig()
    arbitration: ArbitrationConfig = ArbitrationConfig()
