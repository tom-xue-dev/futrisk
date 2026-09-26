"""Config 2PC coordinator (02 §8.5, 01 §6.7): happy path, abort/timeout paths, local validation,
restart recovery, FAILED after the heartbeat check, decision queries."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest
from apigw_support import Harness, err, hdr, make_gateway, new_key, with_keys

from futrisk.common.config import FullConfig, ensure_home_config, read_active, snapshot_working_config
from futrisk.messages import codes
from futrisk.messages.enums import ConfigEffect, ConfigStatus, Role
from futrisk.messages.envelope import Reply, ok_reply
from futrisk.messages.system import ConfigAbort, ConfigCommit, ConfigPrepare, ConfigPrepared
from futrisk.services.api_gateway.store import ConfigVersionRow

INSTANCES = ["api-gateway", "clearing-worker", "md-gateway", "risk-engine-0", "risk-engine-1", "supervisor",
             "trade-gateway"]


class Participants:
    """Fake participants: vote, commit (their heartbeat then reports the new hash), abort."""

    def __init__(self, h: Harness) -> None:
        self.h = h
        self.hb: dict[str, tuple[int, str]] = {}
        self.log: list[tuple[Any, ...]] = []
        self.vote: dict[str, Reply] = {}
        self.converge = True
        self.hashes: dict[int, str] = {}

    def heartbeat(self, instance: str) -> tuple[int, str] | None:
        return self.hb.get(instance)

    def prepare(self, target: str, req: ConfigPrepare) -> Reply:
        self.hashes[req.config_version] = req.config_hash
        self.log.append(("prepare", target, req.config_version))
        if target in self.vote:
            return self.vote[target]
        return ok_reply(ConfigPrepared(instance=target, config_version=req.config_version, ok=True,
                                       effects={"risk_rules": ConfigEffect.IMMEDIATE}, computed_hash=req.config_hash))

    async def commit(self, target: str, req: ConfigCommit) -> Reply:
        row = await self.h.gw.store.get_config_version(req.config_version)
        active = read_active(self.h.layout)
        assert row is not None and active is not None
        self.log.append(("commit", target, req.config_version, row.status, active["config_version"]))
        if self.converge:
            self.hb[target] = (req.config_version, self.hashes.get(req.config_version, row.config_hash))
        return ok_reply()

    async def abort(self, target: str, req: ConfigAbort) -> Reply:
        row = await self.h.gw.store.get_config_version(req.config_version)
        assert row is not None
        self.log.append(("abort", target, req.config_version, row.status))
        return ok_reply()

    def install(self) -> None:
        self.h.bus.on("ctl.config.prepare", self.prepare)
        self.h.bus.on("ctl.config.commit", self.commit)
        self.h.bus.on("ctl.config.abort", self.abort)

    def of(self, kind: str) -> list[tuple[Any, ...]]:
        return [x for x in self.log if x[0] == kind]


async def setup(tmp_path: Path, cfg: FullConfig, **kw: Any) -> tuple[Harness, Participants]:
    holder: dict[str, Participants] = {}
    cfg2, env = with_keys(cfg)
    h = await make_gateway(tmp_path, cfg2, env=env, heartbeat_config=lambda i: holder["p"].heartbeat(i), **kw)
    ensure_home_config(h.layout)  # working dir + versions/1 + active.json
    p = Participants(h)
    p.install()
    holder["p"] = p
    return h, p


def edit_rules(h: Harness, old: str = "threshold: -0.030", new: str = "threshold: -0.031") -> None:
    f = h.layout.config_dir / "risk_rules.yaml"
    f.write_text(f.read_text(encoding="utf-8").replace(old, new, 1), encoding="utf-8")


async def reload(h: Harness) -> Any:
    async with h.client() as c:
        return await c.post("/api/v1/config/reload", headers=hdr(Role.ADMIN, idem=new_key()))


class TestReload:
    async def test_happy_path(self, tmp_path: Path, default_config: FullConfig) -> None:
        h, p = await setup(tmp_path, default_config)
        try:
            edit_rules(h)
            r = await reload(h)
            body = r.json()
            assert r.status_code == 200 and body["ok"], body
            assert body["data"]["config_version"] == 2 and body["data"]["status"] == "COMMITTED"
            await h.gw.coordinator.wait_idle()
            # every instance was asked, including api-gateway itself
            assert sorted(x[1] for x in p.of("prepare")) == INSTANCES
            # the COMMITTING decision and active.json were persisted before any commit went out
            assert {(x[3], x[4]) for x in p.of("commit")} == {(ConfigStatus.COMMITTING, 2)}
            row = await h.gw.store.get_config_version(2)
            assert row is not None and row.status == ConfigStatus.COMMITTED and row.finished_ns is not None
            assert all(a["ok"] and a["committed"] for a in row.acks.values()) and set(row.acks) == set(INSTANCES)
            assert row.effective == {"risk_rules": {"effect": "IMMEDIATE"}}
            assert (h.layout.versions_dir / "2" / "risk_rules.yaml").read_text(encoding="utf-8").count("-0.031") == 1
            active = read_active(h.layout)
            assert active is not None and active["config_version"] == 2 and active["path"] == "config/versions/2"
            versions = await h.gw.store.list_config_versions()
            assert [v.config_version for v in versions] == [2]
            assert not p.of("abort")
            # the decision is answered from the persisted row
            dec = await h.gw.coordinator.decision(2)
            assert dec is not None and dec.status == ConfigStatus.COMMITTED
            assert await h.gw.coordinator.decision(99) is None
        finally:
            await h.close()

    async def test_next_trading_day_effect_records_the_day(self, tmp_path: Path, default_config: FullConfig) -> None:
        h, p = await setup(tmp_path, default_config)
        try:
            def prepare(target: str, req: ConfigPrepare) -> Reply:
                p.hashes[req.config_version] = req.config_hash
                return ok_reply(ConfigPrepared(instance=target, config_version=req.config_version, ok=True,
                                               effects={"products": ConfigEffect.NEXT_TRADING_DAY},
                                               computed_hash=req.config_hash))

            h.bus.on("ctl.config.prepare", prepare)
            r = await reload(h)
            assert r.status_code == 200
            active = read_active(h.layout)
            assert active is not None
            assert active["effective"] == {"products": {"effect": "NEXT_TRADING_DAY", "trading_day": "20260929"}}
        finally:
            await h.close()

    async def test_rejected_vote_aborts(self, tmp_path: Path, default_config: FullConfig) -> None:
        h, p = await setup(tmp_path, default_config)
        try:
            p.vote["risk-engine-1"] = ok_reply(ConfigPrepared(instance="risk-engine-1", config_version=2, ok=False,
                                                              errors=["rule daily_loss: bad level order"]))
            r = await reload(h)
            body = r.json()
            assert r.status_code == 400 and body["code"] == codes.BAD_REQUEST
            assert body["data"]["status"] == "ABORTED" and "bad level order" in body["data"]["error"]
            # ABORTED was persisted before the aborts went out, to every instance asked to prepare
            assert sorted(x[1] for x in p.of("abort")) == INSTANCES
            assert {x[3] for x in p.of("abort")} == {ConfigStatus.ABORTED}
            assert not p.of("commit")
            active = read_active(h.layout)
            assert active is not None and active["config_version"] == 1
            dec = await h.gw.coordinator.decision(2)
            assert dec is not None and dec.status == ConfigStatus.ABORTED
        finally:
            await h.close()

    async def test_timeout_aborts(self, tmp_path: Path, default_config: FullConfig) -> None:
        h, p = await setup(tmp_path, default_config)
        try:
            p.vote["md-gateway"] = err(codes.TIMEOUT, "no answer within 5000ms")
            r = await reload(h)
            assert r.status_code == 504 and r.json()["data"]["status"] == "ABORTED"
            assert len(p.of("abort")) == len(INSTANCES)
            prep = [c for c in h.bus.of("ctl.config.prepare")]
            assert all(c.kw["timeout_ms"] == 5000 for c in prep)
        finally:
            await h.close()

    async def test_invalid_working_config_is_rejected_locally(self, tmp_path: Path, default_config: FullConfig) -> None:
        h, p = await setup(tmp_path, default_config)
        try:
            (h.layout.config_dir / "risk_rules.yaml").write_text("rules: [{id: broken}]\n", encoding="utf-8")
            r = await reload(h)
            body = r.json()
            assert r.status_code == 400 and body["data"]["status"] == "ABORTED"
            assert "invalid configuration" in body["data"]["error"]
            assert not p.of("prepare") and not p.of("abort")
        finally:
            await h.close()

    async def test_failed_when_heartbeats_do_not_converge(self, tmp_path: Path, default_config: FullConfig) -> None:
        h, p = await setup(tmp_path, default_config, verify_timeout_s=0.4)
        try:
            p.converge = False
            r = await reload(h)
            assert r.status_code == 200 and r.json()["data"]["status"] == "COMMITTED"
            await asyncio.wait_for(h.gw.coordinator.wait_idle(), 5)
            row = await h.gw.store.get_config_version(2)
            assert row is not None and row.status == ConfigStatus.FAILED and "risk-engine-0" in (row.error or "")
            # FAILED still means "commit" for participants that ask (02 §8.5 step 5)
            dec = await h.gw.coordinator.decision(2)
            assert dec is not None and dec.status == ConfigStatus.FAILED
            assert len(p.of("commit")) > len(INSTANCES)  # commits were resent to the laggards
        finally:
            await h.close()


class TestRecovery:
    async def test_restart_recovery(self, tmp_path: Path, default_config: FullConfig) -> None:
        h, p = await setup(tmp_path, default_config)
        try:
            store = h.gw.store
            n2, path2 = snapshot_working_config(h.layout)
            edit_rules(h)
            n3, path3 = snapshot_working_config(h.layout)
            for n, path, status in ((n2, path2, ConfigStatus.PREPARING), (n3, path3, ConfigStatus.COMMITTING)):
                await store.insert_config_version(ConfigVersionRow(
                    config_version=n, config_hash=f"hash{n}", path=path.relative_to(h.layout.home).as_posix(),
                    files={}, effective={"risk_rules": {"effect": "IMMEDIATE"}}, status=status,
                    requested_by="admin", requested_ns=1))
            handled = await h.gw.coordinator.recover()
            assert handled == [2, 3]
            await asyncio.wait_for(h.gw.coordinator.wait_idle(), 5)
            row2 = await store.get_config_version(2)
            row3 = await store.get_config_version(3)
            assert row2 is not None and row2.status == ConfigStatus.ABORTED and "restarted" in (row2.error or "")
            assert {(x[2], x[3]) for x in p.of("abort")} == {(2, ConfigStatus.ABORTED)}
            assert sorted(x[1] for x in p.of("commit") if x[2] == 3) == INSTANCES
            active = read_active(h.layout)
            assert active is not None and active["config_version"] == 3
            assert row3 is not None and row3.status == ConfigStatus.COMMITTED
        finally:
            await h.close()


@pytest.mark.parametrize("status", [ConfigStatus.COMMITTING, ConfigStatus.COMMITTED, ConfigStatus.ABORTED])
async def test_decision_reflects_persisted_status(tmp_path: Path, default_config: FullConfig,
                                                  status: ConfigStatus) -> None:
    h, _ = await setup(tmp_path, default_config)
    try:
        await h.gw.store.insert_config_version(ConfigVersionRow(
            config_version=7, config_hash="h", path="config/versions/7", files={}, effective={}, status=status,
            requested_by="admin", requested_ns=1))
        dec = await h.gw.coordinator.decision(7)
        assert dec is not None and dec.status == status and dec.config_version == 7
    finally:
        await h.close()


class TestSupersedeAndCrash:
    async def test_second_reload_during_the_verify(self, tmp_path: Path, default_config: FullConfig) -> None:
        h, p = await setup(tmp_path, default_config, verify_timeout_s=1.5)
        try:
            events: list[dict[str, Any]] = []
            h.gw.hub.system_event = events.append  # type: ignore[method-assign]
            p.converge = False  # v2: every commit is acknowledged but no heartbeat reports it yet
            edit_rules(h)
            assert (await reload(h)).json()["data"]["config_version"] == 2
            await asyncio.sleep(0.3)  # v2's heartbeat check is running (and resending commits)
            p.converge = True
            edit_rules(h, "threshold: -0.031", "threshold: -0.032")
            r3 = await reload(h)  # v3 while v2 is still being verified
            assert r3.status_code == 200 and r3.json()["data"]["config_version"] == 3
            n_commits = len(p.of("commit"))
            await asyncio.wait_for(h.gw.coordinator.wait_idle(), 5)
            row2 = await h.gw.store.get_config_version(2)
            row3 = await h.gw.store.get_config_version(3)
            # v2 was superseded, not FAILED; no alert, no commit of v2 after v3 went out
            assert row2 is not None and row2.status == ConfigStatus.COMMITTED and row2.error is None
            assert row3 is not None and row3.status == ConfigStatus.COMMITTED
            assert not [x for x in p.of("commit")[n_commits:] if x[2] == 2]
            assert not [e for e in events if e.get("event") == "CONFIG_FAILED"]
            active = read_active(h.layout)
            assert active is not None and active["config_version"] == 3
        finally:
            await h.close()

    async def test_crash_after_committing_before_active_json(self, tmp_path: Path,
                                                             default_config: FullConfig) -> None:
        h, p = await setup(tmp_path, default_config)
        try:
            edit_rules(h)
            n, path = snapshot_working_config(h.layout)
            # the decision COMMITTING was persisted, then the process died before active.json was rewritten
            await h.gw.store.insert_config_version(ConfigVersionRow(
                config_version=n, config_hash="h2", path=path.relative_to(h.layout.home).as_posix(), files={},
                effective={"risk_rules": {"effect": "IMMEDIATE"}}, status=ConfigStatus.COMMITTING,
                requested_by="admin", requested_ns=1))
            active = read_active(h.layout)
            assert active is not None and active["config_version"] == 1
            assert await h.gw.coordinator.recover() == [n]
            await asyncio.wait_for(h.gw.coordinator.wait_idle(), 5)
            active = read_active(h.layout)
            assert active is not None and active["config_version"] == n  # re-pointed on recovery
            assert active["effective"] == {"risk_rules": {"effect": "IMMEDIATE"}}
            assert sorted(x[1] for x in p.of("commit") if x[2] == n) == INSTANCES
            row = await h.gw.store.get_config_version(n)
            assert row is not None and row.status == ConfigStatus.COMMITTED
        finally:
            await h.close()

    async def test_older_committing_rows_are_not_recommitted(self, tmp_path: Path,
                                                             default_config: FullConfig) -> None:
        h, p = await setup(tmp_path, default_config)
        try:
            for n, status in ((2, ConfigStatus.COMMITTING), (3, ConfigStatus.COMMITTED)):
                await h.gw.store.insert_config_version(ConfigVersionRow(
                    config_version=n, config_hash=f"h{n}", path=f"config/versions/{n}", files={}, effective={},
                    status=status, requested_by="admin", requested_ns=1))
            assert await h.gw.coordinator.recover() == [2]
            await asyncio.wait_for(h.gw.coordinator.wait_idle(), 5)
            assert not p.of("commit")  # never roll the participants back to v2
            row2 = await h.gw.store.get_config_version(2)
            assert row2 is not None and row2.status == ConfigStatus.COMMITTED
        finally:
            await h.close()

    async def test_failed_config_is_published_and_notified(self, tmp_path: Path,
                                                           default_config: FullConfig) -> None:
        h, p = await setup(tmp_path, default_config, verify_timeout_s=0.3)
        try:
            events: list[dict[str, Any]] = []
            h.gw.hub.system_event = events.append  # type: ignore[method-assign]
            p.converge = False
            await reload(h)
            await asyncio.wait_for(h.gw.coordinator.wait_idle(), 5)
            failed = [e for e in events if e.get("event") == "CONFIG_FAILED"]
            assert failed and failed[0]["kind"] == "config" and failed[0]["config_version"] == 2
            assert h.gw.notifier is not None
            await h.gw.notifier.drain()
            n = await h.gw.store.get_notification("CONFIG|2|FAILED", "log")
            assert n is not None and n.severity.value == "CRITICAL" and n.kind == "CONFIG_FAILED"
            assert "risk-engine-0" in n.payload["lagging"]
        finally:
            await h.close()
