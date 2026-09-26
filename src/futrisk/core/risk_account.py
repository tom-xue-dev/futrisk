"""Per-account runtime state of the risk engine (01 §5.3.1 Portfolio / 增量估值; 02 §7.6).

A :class:`RiskAccount` bundles everything the single-writer engine keeps for
one account: the intraday ledger (lots + funds), the alert state machines, the
effective restrictions and account state (with the persisted ``state_version``),
system restrictions, the manual-halt latch, compliance counters, the pre-trade
token bucket, today's applied fills and cash (for digests, queries and REBASE)
and the dirty / snapshot bookkeeping of the main loop (02 §8.1).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum

from futrisk.core.instruments import InstrumentRegistry
from futrisk.core.ledger import IntradayLedger
from futrisk.core.metrics import AccountValuation, ComplianceCounters, compliance_utilization
from futrisk.core.state_machine import LevelMachine
from futrisk.messages.enums import AccountState
from futrisk.messages.orders import Trade
from futrisk.messages.risk import CashMovement, Restriction


class Lifecycle(StrEnum):
    """Engine lifecycle (02 §8.6 risk-engine 就绪条件, 01 §6.8 停机)."""

    LOADING = "LOADING"  # SOD / fills not rebuilt yet: every order REJECT_SYSTEM_NOT_READY
    WARMUP = "WARMUP"  # BLOCK_OPEN (SYSTEM:STARTING) on every account, closes allowed
    RUNNING = "RUNNING"
    STOPPING = "STOPPING"  # ctl.shutdown: every order REJECT_SYSTEM_NOT_READY


@dataclass(slots=True)
class TokenBucket:
    """Order-rate throttle per account (02 §11.2 step 5 ``REJECT_THROTTLED``)."""

    rate_per_s: float
    burst: int
    tokens: float = -1.0
    last_ns: int | None = None

    def take(self, now_ns: int) -> bool:
        if self.tokens < 0:
            self.tokens = float(self.burst)
        if self.last_ns is not None and now_ns > self.last_ns:
            self.tokens = min(float(self.burst), self.tokens + (now_ns - self.last_ns) * self.rate_per_s / 1e9)
        self.last_ns = now_ns if self.last_ns is None else max(self.last_ns, now_ns)
        if self.tokens < 1.0:
            return False
        self.tokens -= 1.0
        return True

    def utilization(self, now_ns: int) -> float:
        """Share of the burst in use at ``now_ns`` (refill included, nothing taken).

        0 = full bucket (or never used); 1 = empty: the next order is
        REJECT_THROTTLED (from 1 - 1/burst on). The order-rate part of
        ``account.compliance_util``: the bucket measures the rate over its own
        window (burst / rate seconds), so 100 % means "throttled now" - a steady
        flow at exactly ``rate_per_s`` keeps the bucket full.
        """
        if self.tokens < 0 or self.burst <= 0:
            return 0.0
        tokens = self.tokens
        if self.last_ns is not None and now_ns > self.last_ns:
            tokens = min(float(self.burst), tokens + (now_ns - self.last_ns) * self.rate_per_s / 1e9)
        return max(0.0, (self.burst - tokens) / self.burst)


@dataclass
class RiskAccount:
    """All runtime state of one account."""

    account_id: str
    ledger: IntradayLedger
    bucket: TokenBucket
    machines: dict[tuple[str, str, str], LevelMachine] = field(default_factory=dict)
    # 账户状态（02 §7.6 AccountStateChanged；state_version 持久化在 account_states）
    state: AccountState = AccountState.NORMAL
    halted_latched: bool = False
    state_version: int = 0
    state_since_ns: int = 0
    cause: str = "SYSTEM"
    cause_alert_key: str | None = None
    restrictions: list[Restriction] = field(default_factory=list)
    system: dict[str, Restriction] = field(default_factory=dict)  # reason -> restriction
    manual_halt: bool = False
    manual_halt_ns: int | None = None
    # 估值与主循环
    intraday_peak: float = float("-inf")
    valuation: AccountValuation | None = None
    dirty: bool = True
    # 01 §8.14 confirm.states：只有 LIVE 行情（持仓合约的新 tick）才算新的市场状态；成交、出入金只标脏
    market_seq: int = 0
    refresh_largest: str | None = None  # largest product used by the last account-state refresh
    held_insts: set[str] = field(default_factory=set)  # instruments with a non-flat position (by_instrument index)
    snapshot_dirty: bool = True
    snapshot_version: int = 0
    last_snapshot_ns: int | None = None
    # 当日事实（摘要、查询、重新定基）
    trades: list[Trade] = field(default_factory=list)
    trade_keys: set[str] = field(default_factory=set)
    cash: list[CashMovement] = field(default_factory=list)
    movement_ids: set[str] = field(default_factory=set)
    # 合规计数
    orders_today: int = 0
    cancels_today: int = 0
    open_volume: dict[str, int] = field(default_factory=dict)
    cancels_by_inst: dict[str, int] = field(default_factory=dict)
    large_cancels_by_inst: dict[str, int] = field(default_factory=dict)
    self_trades: int = 0
    trade_ids_by_dir: dict[str, str] = field(default_factory=dict)  # trade_id -> direction (self-trade detection)
    # 柜台持仓对账（02 §8.7）
    recon_mismatch_streak: int = 0
    last_fill_ns: int = 0
    # cache (memory): compliance utilization of the counters, valid while compliance_version is unchanged
    compliance_version: int = 0
    compliance_cache: tuple[int, float] | None = None

    @property
    def trading_day(self) -> str:
        return self.ledger.trading_day

    def position_lots(self) -> dict[str, int]:
        """Instrument -> position of its larger side (lots), for the position-limit share (限仓 is per side)."""
        out: dict[str, int] = {}
        for inst, _direction, qty in self.ledger.held():
            if qty > out.get(inst, 0):
                out[inst] = qty
        return out

    def compliance(self, now_ns: int | None = None) -> ComplianceCounters:
        """Inputs of ``account.compliance_util``; the order-rate part only with ``now_ns`` (the bucket's clock)."""
        return ComplianceCounters(
            open_volume=dict(self.open_volume),
            cancels=dict(self.cancels_by_inst),
            large_cancels=dict(self.large_cancels_by_inst),
            self_trades=self.self_trades,
            positions=self.position_lots(),
            order_rate=self.bucket.utilization(now_ns) if now_ns is not None else 0.0,
        )

    def compliance_util(self, registry: InstrumentRegistry, now_ns: int | None = None) -> float:
        """``account.compliance_util`` = :func:`compliance_utilization` of :meth:`compliance` (``now_ns``).

        The counter and position part is cached until :meth:`counters_changed`;
        the order-rate part (time dependent) is added at every call.
        """
        c = self.compliance_cache
        if c is None or c[0] != self.compliance_version:
            c = (self.compliance_version, compliance_utilization(registry, self.compliance()))
            self.compliance_cache = c
        if now_ns is None:
            return c[1]
        return max(c[1], self.bucket.utilization(now_ns))

    def counters_changed(self) -> None:
        """A compliance input changed (open volume, cancels, self trades, positions): the cached part is stale."""
        self.compliance_version += 1

    def reset_day_counters(self) -> None:
        self.orders_today = 0
        self.cancels_today = 0
        self.open_volume.clear()
        self.cancels_by_inst.clear()
        self.large_cancels_by_inst.clear()
        self.self_trades = 0
        self.counters_changed()
        self.trade_ids_by_dir.clear()
        self.trades = []
        self.trade_keys = set()
        self.cash = []
        self.movement_ids = set()
        self.recon_mismatch_streak = 0

    def touch(self) -> None:
        """A new fact (fill, cash movement) reached this account: re-evaluate, same market state."""
        self.dirty = True
        self.snapshot_dirty = True

    def touch_market(self) -> None:
        """A new LIVE market state (tick of a held / working instrument): the confirm.states key moves."""
        self.market_seq += 1
        self.dirty = True
        self.snapshot_dirty = True


__all__ = ["Lifecycle", "RiskAccount", "TokenBucket"]
