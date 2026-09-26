"""ROUTER caller whitelist (02 §4.11).

Commands listed here are accepted only from the given DEALER identities
(identity = caller instance id, or ``cli``); other callers receive
``FORBIDDEN``. Commands not listed are open to every caller. ``*`` in a
command pattern matches any suffix (``ctl.config.*``); identity patterns use
shell wildcards (``risk-engine-*``). :data:`INTERNAL` stands for every
internal instance configured in ``services.yaml``.

=================================================================  ===========================
command                                                            allowed callers
=================================================================  ===========================
route.order, route.cancel, route.resolve                           risk-engine-*
eod.freeze, eod.rollover                                           clearing-worker
eod.force_unfreeze, firm.kill_switch, risk.reset, risk.halt,       api-gateway
risk.ack_alert, risk.liquidation.confirm, risk.recon.resolve,
cash.move, settle.override_price, settle.import_official,
settle.seed_sod, td.import_trades
order.submit, order.cancel, order.cancel_all                       strategy-sim, api-gateway
md.source.set, settle.run, backtest.run                            api-gateway
config.decision                                                    all internal instances
svc.start, svc.stop, svc.restart, svc.kill                         api-gateway, cli
svc.clock                                                          api-gateway
ctl.shutdown                                                       supervisor, cli
ctl.config.*                                                       api-gateway
=================================================================  ===========================
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from fnmatch import fnmatchcase

INTERNAL = "@internal"

WHITELIST: Mapping[str, tuple[str, ...]] = {
    "route.order": ("risk-engine-*",),
    "route.cancel": ("risk-engine-*",),
    "route.resolve": ("risk-engine-*",),
    "eod.freeze": ("clearing-worker",),
    "eod.rollover": ("clearing-worker",),
    "eod.force_unfreeze": ("api-gateway",),
    "firm.kill_switch": ("api-gateway",),
    "risk.reset": ("api-gateway",),
    "risk.halt": ("api-gateway",),
    "risk.ack_alert": ("api-gateway",),
    "risk.liquidation.confirm": ("api-gateway",),
    "risk.recon.resolve": ("api-gateway",),
    "cash.move": ("api-gateway",),
    "settle.override_price": ("api-gateway",),
    "settle.import_official": ("api-gateway",),
    "settle.seed_sod": ("api-gateway",),
    "td.import_trades": ("api-gateway",),
    "order.submit": ("strategy-sim", "api-gateway"),
    "order.cancel": ("strategy-sim", "api-gateway"),
    "order.cancel_all": ("strategy-sim", "api-gateway"),
    "md.source.set": ("api-gateway",),
    "settle.run": ("api-gateway",),
    "backtest.run": ("api-gateway",),
    "config.decision": (INTERNAL,),
    "svc.start": ("api-gateway", "cli"),
    "svc.stop": ("api-gateway", "cli"),
    "svc.restart": ("api-gateway", "cli"),
    "svc.kill": ("api-gateway", "cli"),
    "svc.clock": ("api-gateway",),
    "ctl.shutdown": ("supervisor", "cli"),
    "ctl.config.*": ("api-gateway",),
}


class Whitelist:
    """Caller check for a ROUTER; ``internal`` is the set of configured instances."""

    def __init__(
        self,
        internal: Iterable[str] = (),
        rules: Mapping[str, tuple[str, ...]] | None = None,
    ) -> None:
        self._internal = frozenset(internal)
        self._exact: dict[str, tuple[str, ...]] = {}
        self._prefix: list[tuple[str, tuple[str, ...]]] = []
        for pattern, callers in (rules if rules is not None else WHITELIST).items():
            if pattern.endswith(".*"):
                self._prefix.append((pattern[:-1], callers))
            else:
                self._exact[pattern] = callers

    def rule_for(self, command: str) -> tuple[str, ...] | None:
        """Allowed caller patterns for ``command`` or ``None`` when unrestricted."""
        rule = self._exact.get(command)
        if rule is not None:
            return rule
        for prefix, callers in self._prefix:
            if command.startswith(prefix):
                return callers
        return None

    def allowed(self, command: str, identity: str) -> bool:
        rule = self.rule_for(command)
        if rule is None:
            return True
        for pattern in rule:
            if pattern == INTERNAL:
                if identity in self._internal:
                    return True
            elif fnmatchcase(identity, pattern):
                return True
        return False


__all__ = ["INTERNAL", "WHITELIST", "Whitelist"]
