"""Tunable parameters of md-gateway (01 §8.13 arbitration table, 03 §5.1, 02 §9.1-§9.2).

Every value of the 01 §8.13 table lives in ``feeds.yaml`` (:class:`ArbitrationConfig`);
:class:`MdParams` adds the implementation constants that the documents leave open
(grace periods, cache sizes, plausibility bounds, rate limits), each with a short
rationale. Tests shorten the timers by building their own :class:`MdParams`.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from futrisk.common.clock import NS_PER_MS, NS_PER_S
from futrisk.common.config.feeds import ArbitrationConfig, FeedsConfig


@dataclass(frozen=True)
class MdParams:
    """Arbitration parameters (``feeds.yaml``) plus md-gateway implementation constants."""

    arb: ArbitrationConfig = field(default_factory=ArbitrationConfig)
    #: a state stamped up to this long after a session window end is still "in session"
    #: (closing snapshots such as DCE's late 15:00 broadcast, 01 §8.2)
    close_grace_s: float = 60.0
    #: polled records / snapshots of contracts outside their session *now* are skipped (02 §9.2);
    #: this grace keeps the closing snapshot of a window
    poll_skip_grace_s: float = 5.0
    #: states stamped further in the future than this are rejected by the normalizer (MD_REJECT_FUTURE_TS)
    future_tolerance_s: float = 120.0
    #: two-sided LIVE latency (C1): a state stamped more than this after its receive time is
    #: quarantined (clock skew allowance); it never becomes LIVE nor a predecessor
    future_skew_s: float = 1.0
    #: per-instrument fingerprint memory for DUPLICATE labelling (≈ a trading day at 2 states/s)
    fingerprint_cap: int = 50_000
    #: per-instrument (volume -> price) memory for the equal-volume cross-check
    volume_history: int = 1024
    #: per (source, instrument) accepted states kept for ordering checks of replays
    source_history: int = 256
    #: recent LIVE keys per instrument (watermark roll-back targets)
    live_history: int = 64
    #: Feed A in-connection replay: timeout per attempt and attempts before reconnecting
    replay_timeout_s: float = 5.0
    replay_max_attempts: int = 3
    #: tail gap = heartbeat ``head_seq`` beyond what was received on this many heartbeats in a row
    tail_gap_heartbeats: int = 2
    #: reject rate window (01 §8.13 "最近 1 分钟") and minimum sample before it can exclude a source;
    #: the rate is computed without the source's worst instrument (one bad contract never excludes a source)
    reject_window_s: float = 60.0
    reject_min_samples: int = 20
    #: window in which ``exclude_integrity_votes`` votes exclude a source
    vote_window_s: float = 300.0
    #: a 1-minute bar is published this long after its minute ends (LIVE latency budget)
    bar_close_grace_s: float = 3.0
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
    #: plausible volume increment (C1): ``min + factor · rate · max(Δt, 1 s) + oi_fraction · reference OI``;
    #: rate and OI come from accepted states (previous state of the source, else the instrument
    #: reference, else ``pre_open_interest``), never from the record being checked
    volume_jump_min: int = 1_000
    volume_jump_factor: float = 20.0
    volume_jump_oi_fraction: float = 0.03
    #: robust σ (H3): per-second variance samples are winsorized at (k·σ)²
    sigma_winsor_k: float = 3.0
    #: the spike threshold never exceeds this share of the limit band (upper - lower)
    spike_band_cap: float = 0.25
    #: dropped spike levels are remembered this long per (source, instrument) (M3 sparse contracts)
    spike_memory_s: float = 30.0
    #: watermark sanity (C1): trusted states newer than the watermark by ``stale_s`` but below it for
    #: this long roll the watermark back (``None`` = ``arb.stale_s``)
    wm_sanity_s: float | None = None
    #: after start-up, quality degradations are not announced (MD_DOWN) for this long;
    #: ``None`` = ``arb.open_grace_s`` (sources need to connect and bootstrap, 01 §8.13)
    startup_grace_s: float | None = None
    #: canonical tick files are (re)built this long after a trading day's last close
    canonical_build_delay_s: float = 120.0
    #: Feed B HTTP timeouts (02 §9.2: connect 1 s, read 1.5 s)
    feed_b_connect_timeout_s: float = 1.0
    feed_b_read_timeout_s: float = 1.5
    #: Feed A WebSocket incoming queue bound (frames); beyond it TCP back-pressure applies
    ws_max_queue: int = 4096
    #: md.ticks flushes the raw buffers at most this often (paging must not create a shard per page)
    ticks_flush_interval_s: float = 1.0

    # ------------------------------------------------------------------ helpers
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
    def spike_confirm_ns(self) -> int:
        return int(self.arb.spike_confirm_ms) * NS_PER_MS

    @property
    def spike_memory_ns(self) -> int:
        return int(self.spike_memory_s * NS_PER_S)

    @property
    def dwell_ns(self) -> int:
        return int(self.arb.min_dwell_s * NS_PER_S)

    @property
    def probation_ns(self) -> int:
        return int(self.arb.probation_healthy_s * NS_PER_S)

    @property
    def flapping_window_ns(self) -> int:
        return int(self.arb.flapping_window_s * NS_PER_S)

    @property
    def close_grace_ns(self) -> int:
        return int(self.close_grace_s * NS_PER_S)

    @property
    def poll_skip_grace_ns(self) -> int:
        return int(self.poll_skip_grace_s * NS_PER_S)

    @property
    def wm_sanity_ns(self) -> int:
        s = self.arb.stale_s if self.wm_sanity_s is None else self.wm_sanity_s
        return int(s * NS_PER_S)

    @property
    def watermark_flush_ns(self) -> int:
        return max(int(self.arb.watermark_flush_s * NS_PER_S), 10 * NS_PER_MS)


__all__ = ["MdParams"]
