"""Tunable parameters of md-gateway (01 §8.13 arbitration table, 03 §5.1, 02 §9.1-§9.2).

The values of the 01 §8.13 table live in ``feeds.yaml`` (:class:`ArbitrationConfig`);
:class:`MdParams` adds the implementation constants the documents leave open, each with a
short rationale. Tests shorten the timers by building their own :class:`MdParams`.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from futrisk.common.clock import NS_PER_MS, NS_PER_S
from futrisk.common.config.feeds import ArbitrationConfig, FeedsConfig


@dataclass(frozen=True)
class MdParams:
    """Arbitration parameters (``feeds.yaml``) plus md-gateway implementation constants."""

    arb: ArbitrationConfig = field(default_factory=ArbitrationConfig)
    #: a state stamped up to this long after a session window end is still "in session" (closing snapshots)
    close_grace_s: float = 60.0
    #: polled records / snapshots of contracts outside their session *now* are skipped (02 §9.2)
    poll_skip_grace_s: float = 5.0
    #: states stamped further in the future than this are rejected by the normalizer (MD_REJECT_FUTURE_TS)
    future_tolerance_s: float = 120.0
    #: two-sided LIVE latency: a state stamped more than this after its receive time is rejected (clock skew)
    future_skew_s: float = 1.0
    #: per-instrument fingerprint memory for DUPLICATE labelling (≈ a trading day at 2 states/s)
    fingerprint_cap: int = 50_000
    #: per-instrument (volume -> price per source) memory for the equal-volume cross-check
    volume_history: int = 1024
    #: Feed A in-connection replay: timeout per attempt and attempts before reconnecting
    replay_timeout_s: float = 5.0
    replay_max_attempts: int = 3
    #: tail gap = heartbeat ``head_seq`` beyond what was received on this many heartbeats in a row
    tail_gap_heartbeats: int = 2
    #: reject-rate window (01 §8.13 "最近 1 分钟") and minimum sample before it can exclude a source
    reject_window_s: float = 60.0
    reject_min_samples: int = 20
    #: window in which ``exclude_integrity_votes`` votes exclude a source
    vote_window_s: float = 300.0
    #: md.status period (02 §5: 1 Hz)
    status_interval_s: float = 1.0
    #: raw rows are handed to the recorder thread at this period
    record_handoff_s: float = 0.2
    #: raw shards: roll every 5 s or 10k rows (03 §5.1)
    raw_roll_s: float = 5.0
    raw_roll_rows: int = 10_000
    #: rows buffered on the event loop beyond this are dropped (counted, logged; cursors stop advancing)
    recorder_max_pending_rows: int = 500_000
    #: rate limits of informational events (at most one per key and interval)
    quiet_event_interval_s: float = 60.0
    bad_tick_event_interval_s: float = 10.0
    integrity_event_interval_s: float = 10.0
    rate_limiter_max_keys: int = 4096
    #: after start-up, quality degradations are not announced (MD_DOWN) for this long; ``None`` = ``arb.open_grace_s``
    startup_grace_s: float | None = None
    #: canonical tick files are (re)built this long after a trading day's last close
    canonical_build_delay_s: float = 120.0
    #: Feed B HTTP connect timeout (the read timeout is ``feeds.feed_b.timeout_ms``)
    feed_b_connect_timeout_s: float = 1.0
    #: Feed A WebSocket incoming queue bound (frames); beyond it TCP back-pressure applies
    ws_max_queue: int = 4096

    @classmethod
    def from_feeds(cls, feeds: FeedsConfig, **overrides: object) -> MdParams:
        return cls(arb=feeds.arbitration, **overrides)  # type: ignore[arg-type]

    @property
    def startup_grace_ns(self) -> int:
        s = self.arb.open_grace_s if self.startup_grace_s is None else self.startup_grace_s
        return int(s * NS_PER_S)

    @property
    def stale_ns(self) -> int:
        return int(self.arb.stale_s * NS_PER_S)

    @property
    def unknown_ns(self) -> int:
        return int(self.arb.unknown_s * NS_PER_S)

    @property
    def open_grace_ns(self) -> int:
        return int(self.arb.open_grace_s * NS_PER_S)

    @property
    def live_max_latency_ns(self) -> int:
        return int(self.arb.live_max_latency_ms) * NS_PER_MS

    @property
    def future_skew_ns(self) -> int:
        return int(self.future_skew_s * NS_PER_S)

    @property
    def reorder_ns(self) -> int:
        return int(self.arb.reorder_buffer_ms) * NS_PER_MS

    @property
    def dwell_ns(self) -> int:
        return int(self.arb.min_dwell_s * NS_PER_S)

    @property
    def probation_ns(self) -> int:
        return int(self.arb.probation_healthy_s * NS_PER_S)

    @property
    def close_grace_ns(self) -> int:
        return int(self.close_grace_s * NS_PER_S)

    @property
    def poll_skip_grace_ns(self) -> int:
        return int(self.poll_skip_grace_s * NS_PER_S)

    @property
    def watermark_flush_ns(self) -> int:
        return max(int(self.arb.watermark_flush_s * NS_PER_S), 10 * NS_PER_MS)


__all__ = ["MdParams"]
