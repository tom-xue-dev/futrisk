"""Account -> risk-engine shard routing (02 §3 rules, §4.1 command matrix).

The shard owning an account is the ``shard`` field of ``accounts.yaml``; its
instance is ``risk-engine-{shard}`` and the endpoints come from ``services.yaml``.
A command sent to the wrong shard is answered with ``NOT_OWNER``, so the client
always resolves the owner before sending.
"""

from __future__ import annotations

from collections.abc import Iterable

from futrisk.common.config.accounts import AccountsConfig
from futrisk.common.config.services import ServicesConfig


class UnknownAccountError(KeyError):
    """The account is not configured in ``accounts.yaml`` (or its shard has no endpoint)."""


class ShardRouter:
    """Resolves the owning ``risk-engine-{k}`` instance of every account."""

    def __init__(self, accounts: AccountsConfig, services: ServicesConfig) -> None:
        self._accounts = accounts
        self._services = services

    def accounts(self) -> list[str]:
        return sorted(self._accounts.accounts)

    def has(self, account_id: str) -> bool:
        return account_id in self._accounts.accounts

    def shard_of(self, account_id: str) -> int:
        spec = self._accounts.accounts.get(account_id)
        if spec is None:
            raise UnknownAccountError(f"unknown account {account_id!r}")
        return spec.shard

    def instance_of(self, account_id: str) -> str:
        """``risk-engine-{shard}`` of the account; raises :class:`UnknownAccountError`."""
        instance = self._services.shard_instance(self.shard_of(account_id))
        if instance not in self._services.instances:
            raise UnknownAccountError(f"account {account_id!r}: shard instance {instance!r} not in services.yaml")
        return instance

    def accounts_of(self, instance: str) -> list[str]:
        return sorted(a for a in self._accounts.accounts if self._services.shard_instance(self.shard_of(a)) == instance)

    def instances(self, accounts: Iterable[str] | None = None) -> list[str]:
        """Distinct shard instances owning ``accounts`` (default: every configured account)."""
        wanted = self.accounts() if accounts is None else list(accounts)
        return sorted({self.instance_of(a) for a in wanted})


__all__ = ["ShardRouter", "UnknownAccountError"]
