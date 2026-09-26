"""Config hot-reload participant: two-phase commit (02 §6.1, §7.10, §8.5; 01 §6.7).

* ``ctl.config.prepare`` loads and validates the immutable version directory
  (``config/versions/{N}/``) with :func:`load_config_dir`, recomputes the
  config hash, checks restart-only changes (endpoints in ``services.yaml`` /
  ``feeds.yaml``) and service-specific validators, then answers
  ``ConfigPrepared{ok, errors, effects, computed_hash}``. A yes vote is
  persisted under ``<home>/run/config-2pc/{instance}.json``.
* ``ctl.config.commit`` swaps the in-memory config and calls the service
  callback with the per-section effects (``IMMEDIATE`` / ``NEXT_TRADING_DAY``);
  ``ctl.config.abort`` discards the staged version. Both are idempotent.
* A participant that voted yes never aborts on its own: when no decision
  arrived within 60 s, or after a restart in the prepared state, the runtime
  asks the coordinator with ``config.decision``; ``COMMITTING``/``COMMITTED``/
  ``FAILED`` -> commit, ``ABORTED`` -> abort, ``PREPARING`` -> ask again later.
"""

from __future__ import annotations

import json
import os
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path

from loguru import logger

from futrisk.bus.rpc_common import Handler, RequestContext
from futrisk.common.clock import Clock, SystemClock
from futrisk.common.config import ConfigError, FullConfig, load_config_dir, read_active
from futrisk.common.paths import HomeLayout
from futrisk.messages import codes
from futrisk.messages.base import Msg
from futrisk.messages.enums import ConfigEffect, ConfigStatus
from futrisk.messages.envelope import Reply, err_reply, ok_reply
from futrisk.messages.system import ConfigAbort, ConfigCommit, ConfigDecision, ConfigPrepare, ConfigPrepared

Validator = Callable[[FullConfig, FullConfig], list[str]]
CommitCallback = Callable[[FullConfig, dict[str, ConfigEffect]], None]

_FILE_SECTIONS = {
    "app.yaml": "app",
    "services.yaml": "services",
    "products.yaml": "products",
    "accounts.yaml": "accounts",
    "risk_rules.yaml": "risk_rules",
    "feeds.yaml": "feeds",
    "sim.yaml": "sim",
}
# 02 §7.10: risk thresholds and account risk profiles apply immediately, product
# parameters (margin, fees, limits) and the calendar from the next trading day.
SECTION_EFFECTS: dict[str, ConfigEffect] = {
    "risk_rules": ConfigEffect.IMMEDIATE,
    "accounts": ConfigEffect.IMMEDIATE,
    "products": ConfigEffect.NEXT_TRADING_DAY,
    "calendar": ConfigEffect.NEXT_TRADING_DAY,
}
_COMMIT_STATUSES = frozenset({ConfigStatus.COMMITTING, ConfigStatus.COMMITTED, ConfigStatus.FAILED})


def section_of(path: str) -> str:
    """Config section of a file path relative to the config root."""
    if path in _FILE_SECTIONS:
        return _FILE_SECTIONS[path]
    return path.split("/", 1)[0] if "/" in path else path


def compute_effects(current: FullConfig, new: FullConfig) -> tuple[dict[str, ConfigEffect], list[str]]:
    """Per-section effect of switching ``current`` -> ``new`` and restart-only errors."""
    changed: set[str] = set()
    for path in set(current.files) | set(new.files):
        if current.files.get(path) != new.files.get(path):
            changed.add(section_of(path))
    effects = {sec: SECTION_EFFECTS.get(sec, ConfigEffect.IMMEDIATE) for sec in sorted(changed)}
    errors: list[str] = []
    if "services" in changed and (
        current.services.instances != new.services.instances
        or current.services.bus != new.services.bus
        or current.services.profile != new.services.profile
    ):
        errors.append("services.yaml: endpoint/bus changes require a service restart")
    if "feeds" in changed:

        def urls(cfg: FullConfig) -> tuple[str, str, str]:
            return (cfg.feeds.feed_a.url, cfg.feeds.feed_b.url, cfg.feeds.sina.url)

        if urls(current) != urls(new):
            errors.append("feeds.yaml: data source endpoint changes require a service restart")
    return effects, errors


@dataclass
class PreparedConfig:
    version: int
    path: Path
    config: FullConfig
    effects: dict[str, ConfigEffect]
    prepared_mono_ns: int
    next_query_mono_ns: int


class ConfigParticipant:
    """2PC participant state machine; not thread-safe (call from one thread/loop)."""

    def __init__(
        self,
        *,
        instance: str,
        layout: HomeLayout | None,
        get_config: Callable[[], FullConfig],
        apply_config: CommitCallback,
        validators: Sequence[Validator] = (),
        clock: Clock | None = None,
        decision_timeout_s: float = 60.0,
        decision_retry_s: float = 10.0,
    ) -> None:
        self.instance = instance
        self._layout = layout
        self._get_config = get_config
        self._apply_config = apply_config
        self._validators = list(validators)
        self._clock = clock or SystemClock()
        self._timeout_ns = int(decision_timeout_s * 1e9)
        self._retry_ns = int(decision_retry_s * 1e9)
        self._prepared: PreparedConfig | None = None

    # ------------------------------------------------------------ props
    @property
    def prepared(self) -> PreparedConfig | None:
        return self._prepared

    def add_validator(self, validator: Validator) -> None:
        self._validators.append(validator)

    def _state_file(self) -> Path | None:
        if self._layout is None:
            return None
        return self._layout.home / "run" / "config-2pc" / f"{self.instance}.json"

    def _resolve(self, path: str) -> Path:
        p = Path(path)
        if not p.is_absolute() and self._layout is not None:
            p = self._layout.home / p
        return p

    # -------------------------------------------------------- 2PC steps
    def prepare(self, req: ConfigPrepare) -> ConfigPrepared:
        """Validate and stage in one call (``validate`` + ``stage``)."""
        return self.stage(req, *self.validate(req))

    def validate(self, req: ConfigPrepare) -> tuple[ConfigPrepared, FullConfig | None]:
        """Load and check the version directory; no state change, safe on a worker thread."""
        current = self._get_config()
        if req.config_version < current.config_version:
            res = ConfigPrepared(
                instance=self.instance,
                config_version=req.config_version,
                ok=False,
                errors=[f"stale version {req.config_version} < active {current.config_version}"],
            )
            return res, None
        path = self._resolve(req.path)
        try:
            new = load_config_dir(path, req.config_version)
        except (ConfigError, OSError) as exc:
            res = ConfigPrepared(instance=self.instance, config_version=req.config_version, ok=False, errors=[str(exc)])
            return res, None
        errors: list[str] = []
        if new.config_hash != req.config_hash:
            errors.append(f"config hash mismatch: computed {new.config_hash}, requested {req.config_hash}")
        if req.files and dict(req.files) != new.files:
            errors.append("file list or file hashes differ from the request")
        effects, restart_errors = compute_effects(current, new)
        errors.extend(restart_errors)
        for validator in list(self._validators):
            try:
                errors.extend(validator(current, new))
            except Exception as exc:  # a broken validator must not crash the service
                logger.exception("config.validator_failed")
                errors.append(f"validator error: {exc}")
        res = ConfigPrepared(
            instance=self.instance,
            config_version=req.config_version,
            ok=not errors,
            errors=errors,
            effects=effects,
            computed_hash=new.config_hash,
        )
        return res, new

    def stage(self, req: ConfigPrepare, result: ConfigPrepared, new: FullConfig | None) -> ConfigPrepared:
        """Record a yes vote (and persist it); call on the thread that owns the participant."""
        current = self._get_config()
        if result.ok and new is not None and req.config_version != current.config_version:
            if self._prepared is not None and self._prepared.version != req.config_version:
                logger.warning("config.prepare_replaces", old=self._prepared.version, new=req.config_version)
            now = self._clock.monotonic_ns()
            self._prepared = PreparedConfig(
                version=req.config_version,
                path=self._resolve(req.path),
                config=new,
                effects=dict(result.effects),
                prepared_mono_ns=now,
                next_query_mono_ns=now + self._timeout_ns,
            )
            self._persist(req.config_version, req.path)
        logger.info(
            "config.prepare",
            version=req.config_version,
            ok=result.ok,
            errors=result.errors,
            effects={k: str(v) for k, v in result.effects.items()},
        )
        return result

    def commit(self, req: ConfigCommit) -> Reply:
        p = self._prepared
        if p is not None and p.version == req.config_version:
            return self._do_commit(p.config, p.effects)
        current = self._get_config()
        if req.config_version == current.config_version:
            return ok_reply(message="already active")
        # not prepared in this process (e.g. restarted): active.json already points to it
        cfg = self._load_from_active(req.config_version)
        if cfg is None:
            return err_reply(codes.CONFLICT, f"config version {req.config_version} was not prepared")
        effects, _ = compute_effects(current, cfg)
        return self._do_commit(cfg, effects)

    def abort(self, req: ConfigAbort) -> Reply:
        p = self._prepared
        if p is not None and p.version == req.config_version:
            logger.info("config.abort", version=req.config_version, reason=req.reason)
            self._clear()
        return ok_reply()

    def _do_commit(self, cfg: FullConfig, effects: dict[str, ConfigEffect]) -> Reply:
        try:
            self._apply_config(cfg, effects)
        except Exception as exc:
            logger.exception("config.commit_failed", version=cfg.config_version)
            return err_reply(codes.INTERNAL, f"commit failed: {exc}")
        logger.info("config.commit", version=cfg.config_version, hash=cfg.config_hash)
        self._clear()
        return ok_reply()

    def _load_from_active(self, version: int) -> FullConfig | None:
        if self._layout is None:
            return None
        active = read_active(self._layout)
        if active is None or int(active.get("config_version", -1)) != version:
            return None
        try:
            return load_config_dir(self._resolve(str(active["path"])), version)
        except (ConfigError, OSError):
            logger.exception("config.load_active_failed", version=version)
            return None

    # ----------------------------------------------------- decision query
    def recover(self) -> int | None:
        """After a restart: reload a persisted yes vote; returns the version to query."""
        f = self._state_file()
        if f is None or not f.exists():
            return None
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
            version = int(data["config_version"])
            path_s = str(data["path"])
        except (OSError, ValueError, KeyError):
            logger.exception("config.recover_failed", file=str(f))
            f.unlink(missing_ok=True)
            return None
        if version <= self._get_config().config_version:
            f.unlink(missing_ok=True)
            return None
        path = self._resolve(path_s)
        try:
            cfg = load_config_dir(path, version)
        except (ConfigError, OSError):
            logger.exception("config.recover_load_failed", version=version)
            f.unlink(missing_ok=True)
            return None
        effects, _ = compute_effects(self._get_config(), cfg)
        now = self._clock.monotonic_ns()
        self._prepared = PreparedConfig(version, path, cfg, effects, now, now)
        logger.warning("config.recovered_prepared", version=version)
        return version

    def due_decision(self, now_ns: int | None = None) -> int | None:
        """Version whose decision must be queried now (``None`` if nothing is due)."""
        p = self._prepared
        if p is None:
            return None
        now = self._clock.monotonic_ns() if now_ns is None else now_ns
        return p.version if now >= p.next_query_mono_ns else None

    def mark_queried(self, now_ns: int | None = None) -> None:
        p = self._prepared
        if p is not None:
            now = self._clock.monotonic_ns() if now_ns is None else now_ns
            p.next_query_mono_ns = now + self._retry_ns

    def next_timer_ns(self) -> int | None:
        return self._prepared.next_query_mono_ns if self._prepared is not None else None

    def on_decision(self, decision: ConfigDecision) -> None:
        p = self._prepared
        if p is None or decision.config_version != p.version:
            return
        status = ConfigStatus(str(decision.status))
        logger.info("config.decision", version=p.version, status=status.value)
        if status in _COMMIT_STATUSES:
            self._do_commit(p.config, p.effects)
        elif status == ConfigStatus.ABORTED:
            self._clear()
        # PREPARING: keep waiting; the next query is already scheduled

    def on_decision_reply(self, reply: Reply) -> None:
        if not reply.ok:
            logger.warning("config.decision_query_failed", code=reply.code, message=reply.message)
            return
        decision = reply.open_as(ConfigDecision)
        if decision is not None:
            self.on_decision(decision)

    # ------------------------------------------------------ persistence
    def _persist(self, version: int, path: str) -> None:
        f = self._state_file()
        if f is None:
            return
        f.parent.mkdir(parents=True, exist_ok=True)
        tmp = f.with_suffix(".tmp")
        tmp.write_text(json.dumps({"config_version": version, "path": path}), encoding="utf-8")
        os.replace(tmp, f)

    def _clear(self) -> None:
        self._prepared = None
        f = self._state_file()
        if f is not None:
            f.unlink(missing_ok=True)

    # ---------------------------------------------------------- handlers
    def handlers(self) -> dict[str, tuple[Handler, type[Msg]]]:
        def prepare(command: str, payload: Msg, ctx: RequestContext) -> Reply:
            assert isinstance(payload, ConfigPrepare)
            return ok_reply(self.prepare(payload))

        def commit(command: str, payload: Msg, ctx: RequestContext) -> Reply:
            assert isinstance(payload, ConfigCommit)
            return self.commit(payload)

        def abort(command: str, payload: Msg, ctx: RequestContext) -> Reply:
            assert isinstance(payload, ConfigAbort)
            return self.abort(payload)

        return {
            "ctl.config.prepare": (prepare, ConfigPrepare),
            "ctl.config.commit": (commit, ConfigCommit),
            "ctl.config.abort": (abort, ConfigAbort),
        }


__all__ = [
    "SECTION_EFFECTS",
    "CommitCallback",
    "ConfigParticipant",
    "PreparedConfig",
    "Validator",
    "compute_effects",
    "section_of",
]
