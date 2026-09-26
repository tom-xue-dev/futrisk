"""Firm-level controls and the coordinator's aggregation (01 §5.3.1 coordinator; 02 §7.6 FirmSnapshot / FirmControl; 03 §4.3 firm_controls).

**Every shard** keeps a :class:`FirmState`: the kill switch flags of the two
origins (``MANUAL`` from api-gateway, ``RULE`` from the coordinator) stored
separately, effective = manual OR rule - one origin can never switch off the
other's kill switch - and the current firm restrictions (``firm``,
``firm/prod:…``, ``firm/sector:…``) that apply to every account.

The coordinator publishes the whole RULE layer as **one** fact,
``FirmControl(action=RULE_STATE, origin=RULE, active=<rule kill switch>,
restrictions=<every firm rule restriction in force>)``: a consumer replaces its
RULE layer with it, so the newest fact alone restores the state (the
``ctl.snapshot`` fallback of ``risk.control.firm`` keeps only the newest
message). MANUAL controls stay ``KILL_SWITCH_ON`` / ``KILL_SWITCH_OFF`` (no
restrictions). The legacy RULE actions ``KILL_SWITCH_ON/OFF`` (rule flag only)
and ``SET_RESTRICTIONS`` (restrictions only) are still applied.

**The coordinator** (shard 0) additionally runs a :class:`FirmAggregator`: it
ingests every shard's ``AccountSnapshot``, builds the firm valuation (sums of
funds, product nets across accounts, worst quality, shard freshness) and the
``FirmSnapshot`` (``risk.firm``), and evaluates the firm-scope rules with the
same state machines as accounts. Effective firm circuit breaks switch the RULE
kill switch on; effective firm RESTRICT levels become ``SET_RESTRICTIONS``;
both are published as ``FirmControl`` on ``risk.control.firm``.

Freshness is judged **per shard**: a shard is fresh while its newest snapshot
or heartbeat (:meth:`FirmAggregator.note_shard_alive`) is younger than
``stale_ns``; every shard re-publishes each account at least once per second
(keep-alive), so a quiet account is not stale. Only accounts with exposure can
downgrade the firm quality (a flat account carries no market risk). Expected
shards that never reported are listed as STALE in ``FirmSnapshot.shards``.
An account counts with the worst quality of its held instruments (the worst
``ExposureView.quality``), not with ``AccountSnapshot.quality``, which applies
the account-level materiality tolerance (``system.quality_stale_tolerance``):
the firm scopes keep their definition (worst quality of the exposed accounts).

The firm intraday peak and high-water mark are tracked from the firm's own
performance equity (not the sum of the accounts' peaks, which overstates the
drawdown): the peak starts at the firm's SOD performance equity and moves only
on trusted marks; the HWM is the running maximum across days, kept in memory
only (after a restart it restarts from the current day). Latched firm-level
machines have no reset command: they are released by the day roll only.
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

from futrisk.core.actions import ActionInput, action_outcome
from futrisk.core.instruments import InstrumentRegistry
from futrisk.core.metrics import (
    LIMIT_LOSS_BUDGET,
    METRIC_BASELINES,
    AccountValuation,
    effective_n_of,
    register_metric,
    sector_exposures,
    worst_quality,
)
from futrisk.core.restrictions import merge_since, same_restrictions
from futrisk.core.state_machine import LevelMachine
from futrisk.messages.enums import (
    AccountState,
    Action,
    ControlOrigin,
    Exchange,
    FirmAction,
    MdQuality,
    RuleSmState,
)
from futrisk.messages.risk import (
    AccountFunds,
    AccountMetricsView,
    AccountSnapshot,
    ExposureView,
    FirmControl,
    FirmSnapshot,
    ProductNet,
    Restriction,
    ShardFreshness,
)

RATIO_CAP = 9999.0
FIRM_OWNER = "firm"


def _ratio(num: float, den: float) -> float:
    if den > 0:
        return num / den
    if num == 0:
        return 0.0
    return RATIO_CAP if num > 0 else -RATIO_CAP


@dataclass(frozen=True, slots=True)
class FirmChange:
    """What one applied :class:`FirmControl` changed."""

    kill_changed: bool  # the effective kill switch (manual OR rule)
    restrictions_changed: bool
    manual_changed: bool
    rule_changed: bool


def firm_restrictions(restrictions: list[Restriction]) -> list[Restriction]:
    """Only firm-scoped restrictions (``firm``, ``firm/prod:…``, ``firm/sector:…``) apply to every account."""
    return [r for r in restrictions if r.scope_key == "firm" or r.scope_key.startswith("firm/")]


@dataclass
class FirmState:
    """Kill switch flags per origin and firm restrictions, held by every shard."""

    manual_kill: bool = False
    rule_kill: bool = False
    restrictions: list[Restriction] = field(default_factory=list)
    manual_reason: str = ""
    rule_cause_alert_key: str | None = None

    @property
    def kill_switch(self) -> bool:
        """生效状态 = 人工标志 OR 规则标志 (02 §7.6 FirmControl)."""
        return self.manual_kill or self.rule_kill

    def apply(self, control: FirmControl) -> FirmChange:
        """Apply one control (see the module docstring for the actions); reports what changed.

        ``RULE_STATE`` replaces the whole RULE layer: ``active`` is the rule
        kill switch and ``restrictions`` the complete set of firm rule
        restrictions (an empty list lifts them all).
        """
        before_kill, before_manual, before_rule = self.kill_switch, self.manual_kill, self.rule_kill
        restr_changed = False
        act = control.action
        if act == FirmAction.RULE_STATE:
            self.rule_kill = bool(control.active)
            self.rule_cause_alert_key = control.cause_alert_key if control.active else None
            restr_changed = self._set_restrictions(control.restrictions)
        elif act in (FirmAction.KILL_SWITCH_ON, FirmAction.KILL_SWITCH_OFF):
            flag = act == FirmAction.KILL_SWITCH_ON
            if control.origin == ControlOrigin.MANUAL:
                self.manual_kill = flag
                self.manual_reason = control.reason
            else:  # legacy RULE kill action
                self.rule_kill = flag
                self.rule_cause_alert_key = control.cause_alert_key if flag else None
        else:  # legacy SET_RESTRICTIONS
            restr_changed = self._set_restrictions(control.restrictions)
        return FirmChange(
            kill_changed=before_kill != self.kill_switch,
            restrictions_changed=restr_changed,
            manual_changed=before_manual != self.manual_kill,
            rule_changed=before_rule != self.rule_kill,
        )

    def _set_restrictions(self, restrictions: list[Restriction]) -> bool:
        new = firm_restrictions(list(restrictions))
        changed = not same_restrictions(new, self.restrictions)
        self.restrictions = merge_since(new, self.restrictions)
        return changed


def firm_control_row(control: FirmControl, src_instance: str) -> dict[str, Any]:
    """``firm_controls`` row (03 §4.3): ``active`` = the kill flag for KILL_SWITCH_ON/OFF and RULE_STATE
    (the rule kill switch), 0 for the legacy SET_RESTRICTIONS; ``restrictions`` as carried by the control."""
    carries_flag = control.action in (FirmAction.KILL_SWITCH_ON, FirmAction.KILL_SWITCH_OFF, FirmAction.RULE_STATE)
    return {
        "action": control.action.value,
        "origin": control.origin.value,
        "active": bool(control.active) if carries_flag else False,
        "restrictions": [msgspec_restriction(r) for r in control.restrictions],
        "trading_day": control.trading_day,
        "actor": control.actor,
        "reason": control.reason,
        "cause_alert_key": control.cause_alert_key,
        "src_instance": src_instance,
        "ts_ns": control.ts_ns,
    }


def msgspec_restriction(r: Restriction) -> dict[str, Any]:
    out: dict[str, Any] = {"kind": r.kind.value, "scope_key": r.scope_key, "source": r.source, "since_ns": r.since_ns}
    if r.value is not None:
        out["value"] = r.value
    return out


# ------------------------------------------------------------------ firm metrics
def _alias(field_name: str, source: str) -> Any:
    def fn(v: Any, key: str | None) -> float | None:
        obj = v.metrics if source == "metrics" else v.funds
        return float(getattr(obj, field_name))

    return fn


for _f in AccountMetricsView.__struct_fields__:
    if _f != "effective_n":
        register_metric(f"firm.{_f}", scope="firm", baseline=METRIC_BASELINES.get(_f, 0.0))(_alias(_f, "metrics"))
for _f in AccountFunds.__struct_fields__:
    register_metric(f"firm.{_f}", scope="firm")(_alias(_f, "funds"))


@register_metric("firm.effective_n", scope="firm", baseline=math.inf)
def _firm_effective_n(v: Any, key: str | None) -> float | None:
    return effective_n_of(v.exposures.values())


# ------------------------------------------------------------------ aggregator
@dataclass
class _SnapEntry:
    snapshot: AccountSnapshot
    shard: int


def _has_exposure(s: AccountSnapshot) -> bool:
    return any(ev.long_lots or ev.short_lots for ev in s.exposures)


def _held_quality(s: AccountSnapshot) -> MdQuality:
    """Worst quality of the account's held instruments: the worst per-product quality of the snapshot.

    ``AccountSnapshot.quality`` is the account-scope quality with the
    materiality tolerance applied; a snapshot without per-product qualities
    (older producer) falls back to it.
    """
    qs = [ev.quality for ev in s.exposures]
    if not qs:
        return s.quality
    worst = MdQuality.OK
    for q in qs:
        if q is None:
            return s.quality
        worst = worst_quality([worst, q])
    return worst


class FirmAggregator:
    """Coordinator-side aggregation of shard snapshots and firm rule machines."""

    def __init__(
        self, registry: InstrumentRegistry, *, stale_ns: int = 5_000_000_000, expected_shards: Iterable[int] = ()
    ) -> None:
        self.registry = registry
        self.stale_ns = stale_ns
        self.expected_shards: set[int] = set(expected_shards)
        self._snaps: dict[str, _SnapEntry] = {}
        self._shard_seen: dict[int, int] = {}  # shard -> newest snapshot as_of_ns / heartbeat ts
        self.machines: dict[tuple[str, str, str], LevelMachine] = {}
        self.valuation: AccountValuation | None = None
        self.state_seq = 0
        self._peak_day: str | None = None
        self._peak: float | None = None  # firm intraday peak of performance equity (trusted marks only)
        self._hwm: float | None = None  # firm high-water mark (memory only)
        self._lp: dict[tuple[str, str], float] = {}  # (product, trading day) -> limit pct

    def ingest(self, snapshot: AccountSnapshot) -> bool:
        """Keep the newest snapshot per account (by ``as_of_ns`` then ``version``); the shard is alive."""
        self.note_shard_alive(snapshot.shard, snapshot.as_of_ns)
        cur = self._snaps.get(snapshot.account_id)
        if cur is not None and (cur.snapshot.as_of_ns, cur.snapshot.version) >= (snapshot.as_of_ns, snapshot.version):
            return False
        self._snaps[snapshot.account_id] = _SnapEntry(snapshot, snapshot.shard)
        self.state_seq += 1
        return True

    def note_shard_alive(self, shard: int, ts_ns: int) -> None:
        """A snapshot or heartbeat of ``shard`` at ``ts_ns`` (its freshness)."""
        if ts_ns > self._shard_seen.get(shard, -1):
            self._shard_seen[shard] = ts_ns

    def shard_stale(self, shard: int, now_ns: int) -> bool:
        seen = self._shard_seen.get(shard)
        return seen is None or now_ns - seen > self.stale_ns

    def _limit_pct(self, product: str, trading_day: str) -> float:
        key = (product, trading_day)
        lp = self._lp.get(key)
        if lp is None:
            spec = self.registry.products.products.get(product)
            lp = float(spec.limit_pct_on(trading_day)) if spec is not None else 0.0
            self._lp[key] = lp
        return lp

    def snapshots(self) -> list[AccountSnapshot]:
        return [e.snapshot for _, e in sorted(self._snaps.items())]

    def forget_day(self, trading_day: str) -> None:
        """Drop snapshots of other trading days (after the day roll)."""
        self._snaps = {k: e for k, e in self._snaps.items() if e.snapshot.trading_day == trading_day}

    def _quality(self, s: AccountSnapshot, now_ns: int) -> MdQuality:
        if not _has_exposure(s):
            return MdQuality.OK  # 无敞口的账户没有市场风险，不拉低公司层质量
        q = _held_quality(s)  # 所持合约中最差的质量（账户层的重要性阈值不传到公司层）
        if self.shard_stale(s.shard, now_ns):
            return worst_quality([q, MdQuality.STALE])
        return q

    def build_valuation(self, trading_day: str, now_ns: int) -> AccountValuation:
        snaps = [s for s in self.snapshots() if s.trading_day == trading_day]
        f_sum: dict[str, float] = {k: 0.0 for k in AccountFunds.__struct_fields__}
        for s in snaps:
            for k in f_sum:
                f_sum[k] += float(getattr(s.funds, k))
        equity = f_sum["equity"]
        f_sum["risk_degree"] = _ratio(f_sum["margin"], equity)
        funds = AccountFunds(**f_sum)
        prod: dict[str, dict[str, Any]] = {}
        prod_q: dict[str, MdQuality] = {}
        sect_q: dict[str, MdQuality] = {}
        qualities: list[MdQuality] = []
        for s in snaps:
            q = self._quality(s, now_ns)
            qualities.append(q)
            for ev in s.exposures:
                d = prod.setdefault(ev.product, {"sector": ev.sector, "exchange": ev.exchange, "long": 0, "short": 0,
                                                  "net_lots": 0, "net": 0.0, "gross": 0.0})
                d["long"] += ev.long_lots
                d["short"] += ev.short_lots
                d["net_lots"] += ev.net_lots
                d["net"] += ev.net_notional
                d["gross"] += ev.gross_notional
                prod_q[ev.product] = worst_quality([prod_q.get(ev.product, MdQuality.OK), q])
                sect_q[ev.sector] = worst_quality([sect_q.get(ev.sector, MdQuality.OK), q])
        exposures: dict[str, ExposureView] = {}
        limit_pct: dict[str, float] = {}
        product_sector: dict[str, str] = {}
        for p in sorted(prod):
            d = prod[p]
            lp = self._limit_pct(p, trading_day)
            cap = LIMIT_LOSS_BUDGET / lp if lp > 0 else None
            npct = _ratio(abs(d["net"]), equity)
            exposures[p] = ExposureView(
                product=p, sector=d["sector"], exchange=Exchange(d["exchange"]), long_lots=d["long"], short_lots=d["short"],
                net_lots=d["net_lots"], net_notional=d["net"], gross_notional=d["gross"], net_pct_equity=npct,
                cap_pct=cap, utilization=npct / cap if cap else None,
            )
            limit_pct[p] = lp
            product_sector[p] = d["sector"]
        daily = equity - funds.static_equity
        perf = equity - funds.cum_net_deposit
        quality = worst_quality(qualities)
        # 公司层峰值 / 高水位按公司自身的业绩权益跟踪（账户峰值之和会高估回撤）
        if self._peak_day != trading_day:
            self._peak_day = trading_day
            self._peak = None
        sod_perf = funds.static_equity - funds.cum_net_deposit  # = Σ(pre_balance − cum_net_deposit_sod)
        peak = max(self._peak if self._peak is not None else sod_perf, sod_perf)
        if quality == MdQuality.OK:
            peak = max(peak, perf)
            self._peak = peak
        hwm = max(self._hwm if self._hwm is not None else peak, peak)
        self._hwm = hwm
        gross = sum(ev.gross_notional for ev in exposures.values())
        net = sum(ev.net_notional for ev in exposures.values())
        eff = effective_n_of(exposures.values())
        metrics = AccountMetricsView(
            daily_pnl=daily,
            daily_pnl_pct_init=_ratio(daily, funds.initial_capital),
            daily_pnl_pct_sod=_ratio(daily, funds.static_equity),
            floating_pnl_pct_init=_ratio(funds.position_pnl, funds.initial_capital),
            perf_equity=perf,
            nav=_ratio(perf, funds.initial_capital),
            intraday_peak_equity=peak,
            intraday_dd=_ratio(peak - perf, peak) if peak > 0 else 0.0,
            hwm_equity=hwm,
            hwm_dd=_ratio(hwm - perf, hwm) if hwm > 0 else 0.0,
            gross_notional=gross,
            net_notional=net,
            leverage=_ratio(gross, equity),
            effective_n=eff if eff is not None else 0.0,
            limit_stress=_ratio(sum(abs(ev.net_notional) * limit_pct[p] for p, ev in exposures.items()), equity),
            orders_today=sum(s.metrics.orders_today for s in snaps),
            cancels_today=sum(s.metrics.cancels_today for s in snaps),
        )
        val = AccountValuation(
            account_id=FIRM_OWNER,
            funds=funds,
            metrics=metrics,
            exposures=exposures,
            sectors=sector_exposures(exposures, equity),
            positions=(),
            quality=quality,
            product_quality=prod_q,
            sector_quality=sect_q,
            instrument_quality={},
            compliance_util=0.0,
            hwm_sod=hwm,
            limit_pct=limit_pct,
            product_sector=product_sector,
        )
        self.valuation = val
        return val

    def firm_snapshot(self, trading_day: str, now_ns: int, *, kill_switch: bool) -> FirmSnapshot:
        val = self.valuation if self.valuation is not None else self.build_valuation(trading_day, now_ns)
        snaps = [s for s in self.snapshots() if s.trading_day == trading_day]
        by_state: dict[str, int] = {st.value: 0 for st in AccountState}
        for s in snaps:
            by_state[s.state.value] += 1
        accounts_per_product: dict[str, int] = {}
        for s in snaps:
            for ev in s.exposures:
                if ev.long_lots or ev.short_lots:
                    accounts_per_product[ev.product] = accounts_per_product.get(ev.product, 0) + 1
        shards: dict[int, tuple[int, MdQuality]] = {}
        for sh in sorted(self.expected_shards | set(self._shard_seen)):
            seen = self._shard_seen.get(sh)
            # 从没报过或已过期的分片一律列为 STALE
            shards[sh] = (seen or 0, MdQuality.STALE if self.shard_stale(sh, now_ns) else MdQuality.OK)
        for s in snaps:
            as_of, q = shards.get(s.shard, (0, MdQuality.OK))
            shards[s.shard] = (as_of, worst_quality([q, self._quality(s, now_ns)]))
        return FirmSnapshot(
            trading_day=trading_day,
            as_of_ns=now_ns,
            equity=val.funds.equity,
            daily_pnl=val.metrics.daily_pnl,
            daily_pnl_pct_init=val.metrics.daily_pnl_pct_init,
            margin=val.funds.margin,
            risk_degree=val.funds.risk_degree,
            gross_notional=val.metrics.gross_notional,
            net_notional=val.metrics.net_notional,
            accounts_by_state=by_state,
            product_net=[
                ProductNet(product=p, net_lots=ev.net_lots, net_notional=ev.net_notional, gross_notional=ev.gross_notional,
                           accounts=accounts_per_product.get(p, 0))
                for p, ev in sorted(val.exposures.items())
            ],
            kill_switch=kill_switch,
            shards=[ShardFreshness(shard=k, as_of_ns=v[0], quality=v[1]) for k, v in sorted(shards.items())],
        )

    def desired_controls(self, trading_day: str, now_ns: int) -> tuple[bool, str | None, list[Restriction]]:
        """``(rule kill switch, cause alert key, firm restrictions)`` from the effective firm levels."""
        kill = False
        cause: str | None = None
        restrictions: list[Restriction] = []
        largest = self.valuation.largest_product() if self.valuation is not None else None
        for m in sorted(self.machines.values(), key=lambda x: x.key):
            out = action_outcome(
                ActionInput(rule_id=m.rule.rule_id, level=m.level, scope_key=m.scope_key, account_id=None,
                            sm_state=m.state, latched=m.st.latched, largest_product=largest, since_ns=now_ns)
            )
            if m.level.action == Action.CIRCUIT_BREAK and out.contribution is not None:
                kill = True
                if cause is None and m.st.episode > 0 and m.state == RuleSmState.ACTIVE:
                    cause = m.alert_key(trading_day)
            restrictions.extend(out.restrictions)
        return kill, cause, restrictions


def controls_state(state: FirmState) -> Mapping[str, Any]:
    """Compact view for audits / queries."""
    return {
        "manual_kill": state.manual_kill,
        "rule_kill": state.rule_kill,
        "kill_switch": state.kill_switch,
        "restrictions": [msgspec_restriction(r) for r in state.restrictions],
    }


__all__ = [
    "FIRM_OWNER",
    "FirmAggregator",
    "FirmChange",
    "FirmState",
    "controls_state",
    "firm_control_row",
    "firm_restrictions",
    "msgspec_restriction",
]
