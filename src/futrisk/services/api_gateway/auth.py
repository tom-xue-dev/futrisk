"""API keys, bootstrap key lifecycle and the RBAC permission matrix (01 §8.8, 02 §9.6).

* Keys are presented in the ``X-API-Key`` header (REST) or with the WebSocket
  ``auth`` op / ``?token=`` (02 §9.7). Only ``sha256(key)`` is stored (``api_keys``).
* **Named keys** come from ``app.yaml`` ``api_keys: [{name, role, key_env}]``;
  the plaintext is read from the environment variable ``key_env``. ``key_id`` =
  ``name`` (it is the actor written to audits and forwarded commands). A named
  key removed from ``app.yaml`` or whose variable is unset is disabled; so is a
  key shorter than 16 characters, and every key whose plaintext is also used by
  another named key (a duplicate would make the role ambiguous). A named ADMIN
  key that cannot be enabled is logged at ERROR and shown in ``/system/status``;
  the bootstrap key is re-armed as recovery.
* **Bootstrap key**: while no enabled named ADMIN key exists, an ADMIN key
  ``key_id="bootstrap"`` is generated and written to
  ``<home>/secrets/bootstrap_admin.key`` (one line, readable by the current user
  only: the ``secrets/`` directory and the file are restricted *before* the key
  is written; failures are logged at WARNING); the log only names the file.
  It survives restarts while the file matches; once a named ADMIN key is
  configured it is disabled and the file removed.
* **Failed authentications** are throttled per client address
  (:class:`AuthThrottle`, a token bucket): past the burst the client gets
  ``429 BUSY`` with ``Retry-After`` without its key being checked.
* **Permission matrix** (02 §9.6): every GET needs a valid key (any role);
  orders need TRADER; reset/halt/kill switch/ack/liquidation/recon/price
  override/EOD force-unfreeze need RISK_MANAGER; cash, settlement run, official
  prices, SOD seed, trade import, backtests, config reload, data sources,
  service control and demo endpoints need ADMIN. ADMIN may do everything;
  TRADER and RISK_MANAGER do not include each other.
"""

from __future__ import annotations

import hashlib
import os
import secrets
import subprocess
import sys
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path

from loguru import logger

from futrisk.common.config.app import AppConfig
from futrisk.common.paths import HomeLayout
from futrisk.messages.enums import Role
from futrisk.services.api_gateway.store import ApiStore, KeyRecord

BOOTSTRAP_KEY_ID = "bootstrap"
BOOTSTRAP_KEY_NAME = "bootstrap ADMIN key"
BOOTSTRAP_FILE = "bootstrap_admin.key"
KEY_PREFIX = "frk_"
MIN_KEY_LENGTH = 16


class Perm(StrEnum):
    """Permission classes of the 02 §9.6 matrix."""

    READ = "READ"  # every GET query
    TRADE = "TRADE"  # place / cancel orders
    RISK = "RISK"  # reset, halt, kill switch, ack, liquidation, recon, price override, force-unfreeze
    ADMIN = "ADMIN"  # cash, settlement, imports, SOD seed, backtests, config, sources, services, demo


ROLE_PERMS: Mapping[Role, frozenset[Perm]] = {
    Role.VIEWER: frozenset({Perm.READ}),
    Role.TRADER: frozenset({Perm.READ, Perm.TRADE}),
    Role.RISK_MANAGER: frozenset({Perm.READ, Perm.RISK}),
    Role.ADMIN: frozenset({Perm.READ, Perm.TRADE, Perm.RISK, Perm.ADMIN}),
}


def allowed(role: Role, perm: Perm) -> bool:
    """True when ``role`` holds ``perm`` (02 §9.6)."""
    return perm in ROLE_PERMS.get(role, frozenset())


def hash_key(key: str) -> str:
    """SHA-256 (lowercase hex) of a presented API key."""
    return hashlib.sha256(key.strip().encode("utf-8")).hexdigest()


def generate_key() -> str:
    """A new random API key (~256 bits)."""
    return KEY_PREFIX + secrets.token_urlsafe(32)


def bootstrap_key_path(layout: HomeLayout) -> Path:
    """``<home>/secrets/bootstrap_admin.key``."""
    return layout.secrets_dir / BOOTSTRAP_FILE


def read_bootstrap_key(layout: HomeLayout) -> str | None:
    """The bootstrap ADMIN key written by api-gateway (for ``futrisk demo`` and the CLI), or None."""
    path = bootstrap_key_path(layout)
    try:
        text = path.read_text(encoding="utf-8").strip()
    except OSError:
        return None
    return text or None


def restrict_to_owner(path: Path, *, directory: bool = False) -> bool:
    """Only the current user: POSIX 0600 / 0700; Windows: an ACL for the user alone (icacls).

    Returns False (and logs a WARNING) when the permissions could not be set.
    """
    ok = True
    try:
        os.chmod(path, 0o700 if directory else 0o600)
    except OSError as exc:
        ok = False
        logger.warning("secrets.chmod_failed", path=str(path), error=str(exc))
    if sys.platform != "win32":
        return ok
    user = os.environ.get("USERNAME")
    if not user:
        logger.warning("secrets.acl_failed", path=str(path), error="USERNAME is not set")
        return False
    grant = f"{user}:(OI)(CI)F" if directory else f"{user}:(R,W)"
    try:
        res = subprocess.run(
            ["icacls", str(path), "/inheritance:r", "/grant:r", grant],
            check=False,
            capture_output=True,
            timeout=10,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
    except (OSError, subprocess.SubprocessError) as exc:
        logger.warning("secrets.acl_failed", path=str(path), error=str(exc))
        return False
    if res.returncode != 0:
        logger.warning("secrets.acl_failed", path=str(path), returncode=res.returncode)
        return False
    return ok


def write_secret_file(path: Path, content: str, *, restrict: bool = True) -> None:
    """Write a secret atomically: create the file empty, restrict it, write it, then rename (L6).

    The ``secrets/`` directory is restricted as well, so the secret is never
    readable by others, not even for a moment.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    if restrict:
        restrict_to_owner(path.parent, directory=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.unlink(missing_ok=True)
    fd = os.open(str(tmp), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    os.close(fd)
    if restrict:
        restrict_to_owner(tmp)
    with open(tmp, "w", encoding="utf-8", newline="\n") as f:
        f.write(content + "\n")
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


class AuthThrottle:
    """Token bucket of failed authentications per client address (review L5).

    Every failure takes one token; tokens come back at ``per_s`` up to
    ``burst``. While a client has less than one token, :meth:`blocked` returns
    the seconds to wait and the request is answered ``429`` without its key
    being checked (no oracle for guessing). Successful requests cost nothing.
    """

    def __init__(self, *, burst: float = 30.0, per_s: float = 2.0, max_clients: int = 10_000,
                 clock: Callable[[], float] = time.monotonic) -> None:
        self.burst = burst
        self.per_s = per_s
        self._max = max_clients
        self._clock = clock
        self._buckets: dict[str, tuple[float, float]] = {}
        self.rejected = 0

    def _tokens(self, client: str, now: float) -> float:
        tokens, last = self._buckets.get(client, (self.burst, now))
        return min(self.burst, tokens + (now - last) * self.per_s)

    def blocked(self, client: str) -> float:
        """Seconds until ``client`` may try again (0 when it is not throttled)."""
        if client not in self._buckets:
            return 0.0
        tokens = self._tokens(client, self._clock())
        if tokens >= 1.0:
            return 0.0
        self.rejected += 1
        return (1.0 - tokens) / self.per_s

    def failure(self, client: str) -> None:
        """Count one failed authentication of ``client``."""
        now = self._clock()
        tokens = self._tokens(client, now) - 1.0
        if client not in self._buckets and len(self._buckets) >= self._max:
            self._buckets.clear()  # bounded memory: a flood of addresses only resets the buckets
        self._buckets[client] = (tokens, now)

    def reset(self) -> None:
        self._buckets.clear()


@dataclass(frozen=True)
class Principal:
    """The authenticated caller of one request / WebSocket session."""

    key_id: str
    name: str
    role: Role

    def can(self, perm: Perm) -> bool:
        return allowed(self.role, perm)


@dataclass
class SyncReport:
    """What :meth:`KeyManager.sync` did (for logs and tests)."""

    enabled: list[str] = field(default_factory=list)
    disabled: list[str] = field(default_factory=list)
    missing_env: list[str] = field(default_factory=list)
    rejected: dict[str, str] = field(default_factory=dict)  # name -> why (too short, duplicate plaintext)
    admin_unavailable: list[str] = field(default_factory=list)  # configured named ADMIN keys not enabled
    bootstrap_created: bool = False
    bootstrap_active: bool = False
    bootstrap_disabled: bool = False

    def to_dict(self) -> dict[str, object]:
        return {
            "enabled": list(self.enabled),
            "missing_env": list(self.missing_env),
            "rejected": dict(self.rejected),
            "admin_unavailable": list(self.admin_unavailable),
            "bootstrap_active": self.bootstrap_active,
        }


class KeyManager:
    """Keeps ``api_keys`` in line with ``app.yaml`` and verifies presented keys.

    ``sync`` runs at start-up and after every committed config version.
    ``authenticate`` is a pure in-memory lookup (safe on the event loop).
    """

    def __init__(
        self,
        store: ApiStore,
        layout: HomeLayout,
        *,
        env: Mapping[str, str] | None = None,
        wall_ns: Callable[[], int] | None = None,
        restrict_permissions: bool = True,
    ) -> None:
        self._store = store
        self._layout = layout
        self._env = env if env is not None else os.environ
        self._wall_ns = wall_ns or time.time_ns
        self._restrict = restrict_permissions
        self._by_hash: dict[str, KeyRecord] = {}
        self.last_report: SyncReport | None = None

    @property
    def bootstrap_path(self) -> Path:
        return bootstrap_key_path(self._layout)

    async def reload(self) -> None:
        """Refresh the in-memory key cache from api.db."""
        rows = await self._store.load_keys()
        self._by_hash = {r.key_hash: r for r in rows}

    async def sync(self, app: AppConfig) -> SyncReport:
        """Apply ``app.api_keys`` and the bootstrap-key rule, then refresh the cache."""
        report = SyncReport()
        now = self._wall_ns()
        existing = {r.key_id: r for r in await self._store.load_keys()}
        configured: set[str] = set()
        candidates: list[tuple[str, str, Role]] = []  # (key_id, plaintext, role) past the local checks
        roles: dict[str, Role] = {}
        for spec in app.api_keys:
            key_id = spec.name
            if key_id == BOOTSTRAP_KEY_ID:
                logger.error("api_keys.reserved_name", name=key_id)
                continue
            if key_id in configured:
                logger.error("api_keys.duplicate_name", name=key_id)
                continue
            configured.add(key_id)
            roles[key_id] = Role(spec.role)
            plaintext = (self._env.get(spec.key_env) or "").strip()
            if not plaintext:
                report.missing_env.append(key_id)
                log = logger.error if roles[key_id] == Role.ADMIN else logger.warning
                log("api_keys.env_missing", name=key_id, key_env=spec.key_env, role=spec.role)
                continue
            if len(plaintext) < MIN_KEY_LENGTH:
                report.rejected[key_id] = f"shorter than {MIN_KEY_LENGTH} characters"
                logger.error("api_keys.too_short", name=key_id, key_env=spec.key_env, min_length=MIN_KEY_LENGTH)
                continue
            candidates.append((key_id, plaintext, roles[key_id]))
        by_hash: dict[str, list[str]] = {}
        for key_id, plaintext, _ in candidates:
            by_hash.setdefault(hash_key(plaintext), []).append(key_id)
        for names in by_hash.values():
            if len(names) > 1:
                for n in names:
                    report.rejected[n] = "same key as " + ", ".join(x for x in names if x != n)
                logger.error("api_keys.duplicate_plaintext", names=sorted(names))
        for key_id, plaintext, role in candidates:
            if key_id in report.rejected:
                continue
            old = existing.get(key_id)
            rec = KeyRecord(
                key_id=key_id,
                key_hash=hash_key(plaintext),
                name=key_id,
                role=role,
                created_ns=old.created_ns if old is not None else now,
                expires_ns=old.expires_ns if old is not None else None,
                disabled=False,
            )
            if old is None or (old.key_hash, old.role, old.disabled) != (rec.key_hash, rec.role, rec.disabled):
                await self._store.upsert_key(rec)
            report.enabled.append(key_id)
        for key_id, old in existing.items():
            if key_id == BOOTSTRAP_KEY_ID or key_id in report.enabled or old.disabled:
                continue
            await self._store.set_key_disabled(key_id, True)
            report.disabled.append(key_id)
            logger.info("api_keys.disabled", key_id=key_id,
                        reason="not configured" if key_id not in configured else "not usable")
        report.admin_unavailable = sorted(k for k, r in roles.items() if r == Role.ADMIN and k not in report.enabled)
        if report.admin_unavailable:
            logger.error("api_keys.admin_key_unavailable", names=report.admin_unavailable,
                         missing_env=[k for k in report.admin_unavailable if k in report.missing_env],
                         rejected={k: v for k, v in report.rejected.items() if k in report.admin_unavailable})
        named_admin = any(roles[k] == Role.ADMIN for k in report.enabled)
        await self._sync_bootstrap(existing.get(BOOTSTRAP_KEY_ID), named_admin, now, report)
        await self.reload()
        self.last_report = report
        logger.info(
            "api_keys.synced",
            enabled=report.enabled,
            disabled=report.disabled,
            missing_env=report.missing_env,
            rejected=sorted(report.rejected),
            bootstrap_active=report.bootstrap_active,
        )
        return report

    async def _sync_bootstrap(self, row: KeyRecord | None, named_admin: bool, now: int, report: SyncReport) -> None:
        path = self.bootstrap_path
        if named_admin:
            if row is not None and not row.disabled:
                await self._store.set_key_disabled(BOOTSTRAP_KEY_ID, True)
                report.bootstrap_disabled = True
                logger.info("api_keys.bootstrap_disabled", reason="a named ADMIN key is configured")
            if path.exists():
                path.unlink(missing_ok=True)
            return
        report.bootstrap_active = True
        current = read_bootstrap_key(self._layout)
        if row is not None and not row.disabled and current is not None and hash_key(current) == row.key_hash:
            logger.info("api_keys.bootstrap_active", file=str(path))
            return
        key = generate_key()
        write_secret_file(path, key, restrict=self._restrict)
        await self._store.upsert_key(
            KeyRecord(
                key_id=BOOTSTRAP_KEY_ID,
                key_hash=hash_key(key),
                name=BOOTSTRAP_KEY_NAME,
                role=Role.ADMIN,
                created_ns=now,
                expires_ns=None,
                disabled=False,
            )
        )
        report.bootstrap_created = True
        # 01 §8.8: never log the key itself, only where it is.
        logger.warning("api_keys.bootstrap_created", file=str(path))

    def authenticate(self, presented: str | None) -> Principal | None:
        """Principal of a presented key, or None (unknown, disabled or expired)."""
        if not presented:
            return None
        return self.authenticate_hash(hash_key(presented))

    def authenticate_hash(self, key_hash: str) -> Principal | None:
        """Principal of a key by its SHA-256 (WebSocket sessions re-check their key this way)."""
        rec = self._by_hash.get(key_hash)
        if rec is None or rec.disabled:
            return None
        if rec.expires_ns is not None and self._wall_ns() >= rec.expires_ns:
            return None
        return Principal(key_id=rec.key_id, name=rec.name, role=rec.role)

    def keys(self) -> list[KeyRecord]:
        """Cached key rows (hashes only)."""
        return sorted(self._by_hash.values(), key=lambda r: r.key_id)


__all__ = [
    "BOOTSTRAP_FILE",
    "BOOTSTRAP_KEY_ID",
    "MIN_KEY_LENGTH",
    "ROLE_PERMS",
    "AuthThrottle",
    "KeyManager",
    "Perm",
    "Principal",
    "SyncReport",
    "allowed",
    "bootstrap_key_path",
    "generate_key",
    "hash_key",
    "read_bootstrap_key",
    "restrict_to_owner",
    "write_secret_file",
]
