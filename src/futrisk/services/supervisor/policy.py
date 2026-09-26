"""Restart policy: exponential backoff reset after a stable run (01 §5.3.6, 02 §4.8).

``services.yaml`` ``supervisor.restart``: ``backoff_initial_ms`` (1 s) doubles on every
consecutive failure up to ``backoff_max_ms`` (30 s); after ``stable_reset_s`` (60 s) of
uninterrupted RUNNING the failure count goes back to 0, so the next crash is restarted
after ``backoff_initial_ms`` again.
"""

from __future__ import annotations

from dataclasses import dataclass

from futrisk.common.config.services import RestartPolicy


def backoff_delay_ms(policy: RestartPolicy, failures: int) -> int:
    """Delay before the restart that follows ``failures`` earlier consecutive failures.

    ``failures = 0`` gives ``backoff_initial_ms``; each further failure doubles it, capped
    at ``backoff_max_ms`` (1 s -> 2 s -> 4 s ... 30 s with the defaults).
    """
    n = max(0, int(failures))
    initial = max(0, policy.backoff_initial_ms)
    cap = max(initial, policy.backoff_max_ms)
    if n >= 32:
        return cap
    return int(min(cap, initial * (1 << n)))


@dataclass
class RestartBackoff:
    """Consecutive-failure counter of one instance."""

    policy: RestartPolicy
    failures: int = 0

    def next_delay_ms(self) -> int:
        """Delay of the next restart; counts one more consecutive failure."""
        delay = backoff_delay_ms(self.policy, self.failures)
        self.failures += 1
        return delay

    def stable_for(self, running_s: float) -> bool:
        """Reset the counter after ``stable_reset_s`` of running; True when it was reset."""
        if self.failures and running_s >= self.policy.stable_reset_s:
            self.failures = 0
            return True
        return False

    def reset(self) -> None:
        self.failures = 0


__all__ = ["RestartBackoff", "backoff_delay_ms"]
