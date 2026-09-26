"""Restart policy: exponential backoff 1s -> 30s, reset after a stable run (01 §5.3.6, 02 §4.8)."""

from __future__ import annotations

from futrisk.common.config.services import RestartPolicy
from futrisk.services.supervisor.policy import RestartBackoff, backoff_delay_ms


def test_backoff_doubles_up_to_the_cap() -> None:
    policy = RestartPolicy()  # 1000 ms -> 30000 ms, reset after 60 s
    assert [backoff_delay_ms(policy, n) for n in range(8)] == [1000, 2000, 4000, 8000, 16000, 30000, 30000, 30000]
    assert backoff_delay_ms(policy, 500) == 30000
    assert backoff_delay_ms(policy, -3) == 1000


def test_counter_resets_only_after_a_stable_run() -> None:
    b = RestartBackoff(RestartPolicy(backoff_initial_ms=100, backoff_max_ms=1000, stable_reset_s=60))
    assert [b.next_delay_ms() for _ in range(5)] == [100, 200, 400, 800, 1000]
    assert b.failures == 5
    assert b.stable_for(59.9) is False and b.failures == 5
    assert b.stable_for(60.0) is True and b.failures == 0
    assert b.stable_for(120.0) is False  # nothing to reset
    assert b.next_delay_ms() == 100
