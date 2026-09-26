"""api.db schema/migrations, API keys (weak / duplicate keys), bootstrap key lifecycle and secret-file order,
the failed-authentication throttle and the permission matrix (01 §8.8, 02 §9.6, 03 §4.5)."""

from __future__ import annotations

import asyncio
import dataclasses
import sqlite3
from pathlib import Path

import pytest
from apigw_support import open_db
from loguru import logger
from sqlalchemy import MetaData

from futrisk.common.config import FullConfig
from futrisk.common.config.app import ApiKeyConfig
from futrisk.common.paths import HomeLayout
from futrisk.db import Database, render_ddl
from futrisk.db.migrations import split_sql
from futrisk.messages.enums import Role
from futrisk.services.api_gateway import auth as auth_mod
from futrisk.services.api_gateway.auth import (
    BOOTSTRAP_KEY_ID,
    ROLE_PERMS,
    AuthThrottle,
    KeyManager,
    Perm,
    allowed,
    bootstrap_key_path,
    hash_key,
    read_bootstrap_key,
    write_secret_file,
)
from futrisk.services.api_gateway.schema import METADATA, MIGRATIONS, V1_SQL, V2_SQL
from futrisk.services.api_gateway.store import ApiStore


class TestSchema:
    def test_frozen_sql_matches_metadata(self) -> None:
        md = MetaData()
        for t in METADATA.sorted_tables:
            if t.name != "schema_version":
                t.to_metadata(md)
        # the frozen migrations together create exactly the tables of METADATA
        assert sorted(split_sql(render_ddl(md))) == sorted(split_sql(V1_SQL) + split_sql(V2_SQL))

    async def test_v1_database_upgrades_to_v2(self, tmp_path: Path) -> None:
        layout = HomeLayout(tmp_path)
        md1 = MetaData()
        for t in METADATA.sorted_tables:
            if t.name != "kill_switch":
                t.to_metadata(md1)
        v1 = Database(layout.db_path("api"), md1, name="api", synchronous="FULL", migrations=MIGRATIONS[:1])
        await asyncio.to_thread(v1.open)
        await asyncio.to_thread(v1.close)
        db = await open_db(layout)  # all migrations: v2 is applied on top
        await asyncio.to_thread(db.close)
        con = sqlite3.connect(layout.db_path("api"))
        try:
            versions = [r[0] for r in con.execute("SELECT version FROM schema_version ORDER BY version")]
            names = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        finally:
            con.close()
        assert versions == [1, 2] and "kill_switch" in names

    async def test_migration_creates_tables(self, tmp_path: Path) -> None:
        layout = HomeLayout(tmp_path)
        db = await open_db(layout)
        await asyncio.to_thread(db.close)
        con = sqlite3.connect(layout.db_path("api"))
        try:
            names = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            version = con.execute("SELECT max(version) FROM schema_version").fetchone()[0]
            sync = con.execute("PRAGMA journal_mode").fetchone()[0]
        finally:
            con.close()
        assert {"api_keys", "api_audit", "notifications", "config_versions", "processed_commands",
                "fact_cursors", "db_meta", "schema_version", "kill_switch"} <= names
        assert "outbox" not in names  # api.db publishes no FACTs (03 §4.0)
        assert version == 2 and sync == "wal"
        # reopening validates the recorded checksum
        db2 = await open_db(layout)
        await asyncio.to_thread(db2.close)


class TestPermissionMatrix:
    @pytest.mark.parametrize(
        ("role", "perms"),
        [
            (Role.VIEWER, {Perm.READ}),
            (Role.TRADER, {Perm.READ, Perm.TRADE}),
            (Role.RISK_MANAGER, {Perm.READ, Perm.RISK}),
            (Role.ADMIN, {Perm.READ, Perm.TRADE, Perm.RISK, Perm.ADMIN}),
        ],
    )
    def test_matrix(self, role: Role, perms: set[Perm]) -> None:
        assert ROLE_PERMS[role] == perms
        for perm in Perm:
            assert allowed(role, perm) == (perm in perms)


RO = "ro-secret-key-0123456789"
OPS = "ops-secret-key-0123456789"
SECRET = "trader-secret-0123456789"


def _cfg(default_config: FullConfig, specs: list[ApiKeyConfig]) -> FullConfig:
    return dataclasses.replace(default_config, app=default_config.app.model_copy(update={"api_keys": specs}))


class TestBootstrapKey:
    async def test_lifecycle(self, tmp_path: Path, default_config: FullConfig) -> None:
        layout = HomeLayout(tmp_path)
        layout.ensure()
        db = await open_db(layout)
        store = ApiStore(db)
        records: list[tuple[str, dict[str, object]]] = []
        sink = logger.add(lambda m: records.append((str(m), dict(m.record["extra"]))), level="DEBUG")
        try:
            # first start without any key -> bootstrap ADMIN key in <home>/secrets
            km = KeyManager(store, layout, env={}, restrict_permissions=False)
            rep = await km.sync(default_config.app)
            assert rep.bootstrap_created and rep.bootstrap_active
            key = read_bootstrap_key(layout)
            assert key is not None and bootstrap_key_path(layout).parent == layout.secrets_dir
            p = km.authenticate(key)
            assert p is not None and p.role == Role.ADMIN and p.key_id == BOOTSTRAP_KEY_ID
            assert km.authenticate("wrong") is None and km.authenticate(None) is None
            rows = await store.load_keys()
            assert [r.key_hash for r in rows] == [hash_key(key)]  # only the hash is stored
            flat = [msg + " " + " ".join(str(v) for v in extra.values()) for msg, extra in records]
            assert not any(key in line for line in flat)  # never logged (01 §8.8)
            assert any(extra.get("file") == str(bootstrap_key_path(layout)) for _, extra in records)

            # restart: same key stays valid
            km2 = KeyManager(store, layout, env={}, restrict_permissions=False)
            rep2 = await km2.sync(default_config.app)
            assert not rep2.bootstrap_created and km2.authenticate(key) is not None

            # file lost -> a new key is generated, the old one stops working
            bootstrap_key_path(layout).unlink()
            km3 = KeyManager(store, layout, env={}, restrict_permissions=False)
            assert (await km3.sync(default_config.app)).bootstrap_created
            new_key = read_bootstrap_key(layout)
            assert new_key != key and km3.authenticate(key) is None and km3.authenticate(new_key) is not None

            # a VIEWER key alone does not retire the bootstrap key
            viewer = _cfg(default_config, [ApiKeyConfig(name="ro", role="VIEWER", key_env="RO_KEY")])
            km4 = KeyManager(store, layout, env={"RO_KEY": RO}, restrict_permissions=False)
            rep4 = await km4.sync(viewer.app)
            assert rep4.bootstrap_active and km4.authenticate(new_key) is not None
            assert km4.authenticate(RO).role == Role.VIEWER  # type: ignore[union-attr]

            # a named ADMIN key disables the bootstrap key and removes its file
            ops = _cfg(default_config, [ApiKeyConfig(name="ops", role="ADMIN", key_env="OPS_KEY")])
            km5 = KeyManager(store, layout, env={"OPS_KEY": OPS}, restrict_permissions=False)
            rep5 = await km5.sync(ops.app)
            assert rep5.bootstrap_disabled and not rep5.bootstrap_active
            assert km5.authenticate(new_key) is None and not bootstrap_key_path(layout).exists()
            ops_p = km5.authenticate(OPS)
            assert ops_p is not None and ops_p.key_id == "ops" and ops_p.role == Role.ADMIN
            assert "ro" in rep5.disabled and km5.authenticate(RO) is None  # dropped from app.yaml

            # the variable disappears -> the named key is disabled and the bootstrap key comes back
            km6 = KeyManager(store, layout, env={}, restrict_permissions=False)
            rep6 = await km6.sync(ops.app)
            assert rep6.missing_env == ["ops"] and km6.authenticate(OPS) is None
            assert rep6.admin_unavailable == ["ops"]  # shown in /system/status, logged at ERROR
            assert rep6.bootstrap_created and km6.authenticate(read_bootstrap_key(layout)) is not None
        finally:
            logger.remove(sink)
            await asyncio.to_thread(db.close)

    async def test_expired_key_rejected(self, tmp_path: Path, default_config: FullConfig) -> None:
        layout = HomeLayout(tmp_path)
        db = await open_db(layout)
        store = ApiStore(db)
        try:
            now = [1_000]
            km = KeyManager(store, layout, env={"K": SECRET}, restrict_permissions=False, wall_ns=lambda: now[0])
            cfg = _cfg(default_config, [ApiKeyConfig(name="t", role="TRADER", key_env="K")])
            await km.sync(cfg.app)
            rec = next(r for r in await store.load_keys() if r.key_id == "t")
            await store.upsert_key(dataclasses.replace(rec, expires_ns=2_000))
            await km.reload()
            assert km.authenticate(SECRET) is not None
            now[0] = 2_000
            assert km.authenticate(SECRET) is None
        finally:
            await asyncio.to_thread(db.close)


class TestMaintenance:
    async def test_purge_and_backup(self, tmp_path: Path) -> None:
        from futrisk.messages.envelope import ok_reply
        from futrisk.services.api_gateway.store import DAY_NS, AuditRecord

        layout = HomeLayout(tmp_path)
        db = await open_db(layout)
        store = ApiStore(db)
        try:
            now = 400 * DAY_NS
            for td, key in (("20260924", "old"), ("20260928", "kept")):
                assert await store.record_processed(command_id=key, command="risk.reset", requester="rm",
                                                    request_hash="h", reply=ok_reply(), trading_day=td, received_ns=1)
            assert not await store.record_processed(command_id="kept", command="x", requester="rm", request_hash="h",
                                                    reply=ok_reply(), trading_day="20260928", received_ns=1)
            for ts in (now - 181 * DAY_NS, now - DAY_NS):
                store.submit_audit(AuditRecord(ts_ns=ts, key_id=None, role=None, method="GET", path="/x",
                                               http_status=200, result_code="OK", latency_us=1, client_ip=""))
            purged = await store.purge(keep_processed_from_td="20260928", now_ns=now)
            assert purged == {"processed_commands": 1, "api_audit": 1, "notifications": 0}
            assert await store.lookup_processed("old") is None and await store.lookup_processed("kept") is not None
            dest = await store.backup(layout.backup_dir / "20260928" / "api.db")
            con = sqlite3.connect(dest)
            try:
                assert con.execute("SELECT count(*) FROM processed_commands").fetchone()[0] == 1
            finally:
                con.close()
        finally:
            await asyncio.to_thread(db.close)


class TestKeyHygiene:
    async def test_weak_and_duplicate_keys_are_rejected(self, tmp_path: Path, default_config: FullConfig) -> None:
        layout = HomeLayout(tmp_path)
        layout.ensure()
        db = await open_db(layout)
        store = ApiStore(db)
        records: list[tuple[str, str, dict[str, object]]] = []
        sink = logger.add(lambda m: records.append((m.record["level"].name, str(m), dict(m.record["extra"]))),
                          level="DEBUG")
        try:
            specs = [ApiKeyConfig(name="ops", role="ADMIN", key_env="OPS"),
                     ApiKeyConfig(name="ro", role="VIEWER", key_env="RO"),
                     ApiKeyConfig(name="short", role="TRADER", key_env="SHORT"),
                     ApiKeyConfig(name="rm", role="RISK_MANAGER", key_env="RM")]
            env = {"OPS": "same-key-for-two-names", "RO": "same-key-for-two-names", "SHORT": "tiny-key",
                   "RM": "risk-manager-key-0123"}
            km = KeyManager(store, layout, env=env, restrict_permissions=False)
            rep = await km.sync(_cfg(default_config, specs).app)
            # a duplicate plaintext makes the role ambiguous: both keys are refused (p21)
            assert set(rep.rejected) == {"ops", "ro", "short"} and "same key as ro" in rep.rejected["ops"]
            assert rep.enabled == ["rm"] and km.authenticate("same-key-for-two-names") is None
            assert km.authenticate("tiny-key") is None and km.authenticate("risk-manager-key-0123") is not None
            # no usable named ADMIN key: the bootstrap key is re-armed as recovery and the problem is an ERROR
            assert rep.admin_unavailable == ["ops"] and rep.bootstrap_active and read_bootstrap_key(layout)
            errors = [(msg, extra) for level, msg, extra in records if level == "ERROR"]
            assert any("api_keys.duplicate_plaintext" in m for m, _ in errors)
            assert any("api_keys.too_short" in m for m, _ in errors)
            assert any("api_keys.admin_key_unavailable" in m for m, _ in errors)
            flat = " ".join(m + " " + " ".join(str(v) for v in e.values()) for _, m, e in records)
            assert "same-key-for-two-names" not in flat and "tiny-key" not in flat  # plaintexts never logged
            # fixing the environment enables the keys again and retires the bootstrap key
            env2 = {**env, "RO": "viewer-key-0123456789", "SHORT": "trader-key-0123456789"}
            rep2 = await KeyManager(store, layout, env=env2, restrict_permissions=False).sync(
                _cfg(default_config, specs).app)
            assert sorted(rep2.enabled) == ["ops", "rm", "ro", "short"] and not rep2.bootstrap_active
            assert not bootstrap_key_path(layout).exists()
        finally:
            logger.remove(sink)
            await asyncio.to_thread(db.close)


class TestSecretFile:
    def test_created_restricted_then_written(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        seen: list[tuple[str, bool, int]] = []

        def fake_restrict(path: Path, *, directory: bool = False) -> bool:
            seen.append((path.name, directory, 0 if directory else path.stat().st_size))
            return True

        monkeypatch.setattr(auth_mod, "restrict_to_owner", fake_restrict)
        target = tmp_path / "secrets" / "bootstrap_admin.key"
        write_secret_file(target, "frk_secret")
        # the directory first, then the EMPTY file, and only then the key is written (L6)
        assert seen == [("secrets", True, 0), ("bootstrap_admin.key.tmp", False, 0)]
        assert target.read_text(encoding="utf-8") == "frk_secret\n"
        assert not target.with_suffix(".key.tmp").exists()

    def test_acl_failure_is_a_warning(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        import subprocess

        records: list[tuple[str, str]] = []
        sink = logger.add(lambda m: records.append((m.record["level"].name, str(m))), level="DEBUG")

        def failing_run(*args: object, **kwargs: object) -> subprocess.CompletedProcess[bytes]:
            return subprocess.CompletedProcess(args=[], returncode=5, stdout=b"", stderr=b"denied")

        monkeypatch.setattr(auth_mod.subprocess, "run", failing_run)
        monkeypatch.setattr(auth_mod.sys, "platform", "win32")
        monkeypatch.setenv("USERNAME", "someone")
        f = tmp_path / "x.key"
        f.write_text("x", encoding="utf-8")
        try:
            assert auth_mod.restrict_to_owner(f) is False
        finally:
            logger.remove(sink)
        assert any(level == "WARNING" and "secrets.acl_failed" in msg for level, msg in records)


class TestThrottle:
    def test_token_bucket(self) -> None:
        now = [100.0]
        t = AuthThrottle(burst=3, per_s=1.0, clock=lambda: now[0])
        assert t.blocked("1.2.3.4") == 0
        for _ in range(3):
            t.failure("1.2.3.4")
        assert t.blocked("1.2.3.4") == pytest.approx(1.0)  # out of tokens: wait for one
        assert t.blocked("5.6.7.8") == 0  # per client address
        now[0] += 1.0
        assert t.blocked("1.2.3.4") == 0
