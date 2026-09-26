"""Config 2PC participant (02 §8.5): prepare validation and effects, commit/abort, decision queries."""

from __future__ import annotations

from pathlib import Path

import pytest

from futrisk.common.clock import SimClock
from futrisk.common.config import (
    FullConfig,
    ensure_home_config,
    file_hashes,
    load_active_config,
    load_config_dir,
    snapshot_working_config,
    write_active,
)
from futrisk.common.paths import HomeLayout
from futrisk.messages import codes
from futrisk.messages.enums import ConfigEffect, ConfigStatus
from futrisk.messages.envelope import ok_reply
from futrisk.messages.system import ConfigAbort, ConfigCommit, ConfigDecision, ConfigPrepare
from futrisk.runtime import ConfigParticipant

S = 1_000_000_000


class Node:
    """A participant with its own 'active' config, as a service would hold it."""

    def __init__(self, layout: HomeLayout, clock: SimClock, instance: str = "risk-engine-0") -> None:
        self.layout = layout
        self.config: FullConfig = load_active_config(layout)
        self.commits: list[tuple[int, dict[str, ConfigEffect]]] = []
        self.p = ConfigParticipant(
            instance=instance,
            layout=layout,
            get_config=lambda: self.config,
            apply_config=self._apply,
            clock=clock,
        )

    def _apply(self, cfg: FullConfig, effects: dict[str, ConfigEffect]) -> None:
        self.config = cfg
        self.commits.append((cfg.config_version, effects))


@pytest.fixture
def layout(tmp_path: Path) -> HomeLayout:
    lay = HomeLayout(tmp_path / "home")
    ensure_home_config(lay)
    return lay


def new_version(layout: HomeLayout, edit: dict[str, tuple[str, str]]) -> ConfigPrepare:
    """Edit the working config (``file: (old, new)``), snapshot it and build the prepare request."""
    for name, (old, new) in edit.items():
        f = layout.config_dir / name
        text = f.read_text(encoding="utf-8")
        assert old in text, f"{old!r} not in {name}"
        f.write_text(text.replace(old, new, 1), encoding="utf-8")
    n, path = snapshot_working_config(layout)
    cfg = load_config_dir(path, n)
    return ConfigPrepare(
        config_version=n,
        config_hash=cfg.config_hash,
        path=path.relative_to(layout.home).as_posix(),
        files=file_hashes(path),
        requested_by="api-gateway",
    )


def threshold_edit() -> dict[str, tuple[str, str]]:
    return {"risk_rules.yaml": ("threshold: -0.030", "threshold: -0.031")}


class TestPrepare:
    def test_ok_with_effects_and_hash(self, layout: HomeLayout) -> None:
        node = Node(layout, SimClock(0))
        req = new_version(layout, threshold_edit())
        res = node.p.prepare(req)
        assert res.ok and res.errors == [] and res.computed_hash == req.config_hash
        assert res.effects == {"risk_rules": ConfigEffect.IMMEDIATE} and res.instance == "risk-engine-0"
        assert node.p.prepared is not None and node.p.prepared.version == req.config_version
        assert (layout.home / "run" / "config-2pc" / "risk-engine-0.json").exists()

    def test_products_next_trading_day(self, layout: HomeLayout) -> None:
        node = Node(layout, SimClock(0))
        text = (layout.config_dir / "products.yaml").read_text(encoding="utf-8")
        (layout.config_dir / "products.yaml").write_text(text + "\n# comment only change\n", encoding="utf-8")
        n, path = snapshot_working_config(layout)
        cfg = load_config_dir(path, n)
        res = node.p.prepare(ConfigPrepare(config_version=n, config_hash=cfg.config_hash, path=str(path), files={}, requested_by="x"))
        assert res.ok and res.effects == {"products": ConfigEffect.NEXT_TRADING_DAY}

    def test_hash_mismatch_rejected(self, layout: HomeLayout) -> None:
        node = Node(layout, SimClock(0))
        req = new_version(layout, threshold_edit())
        bad = ConfigPrepare(config_version=req.config_version, config_hash="0" * 64, path=req.path, files={}, requested_by="x")
        res = node.p.prepare(bad)
        assert not res.ok and "hash mismatch" in res.errors[0] and node.p.prepared is None

    def test_invalid_config_rejected(self, layout: HomeLayout) -> None:
        node = Node(layout, SimClock(0))
        f = layout.config_dir / "accounts.yaml"
        f.write_text(f.read_text(encoding="utf-8").replace("accounts:", "accounts:\n  BAD: {oops: 1}", 1), encoding="utf-8")
        n, path = snapshot_working_config(layout)
        req = ConfigPrepare(config_version=n, config_hash="0" * 64, path=str(path), files={}, requested_by="x")
        res = node.p.prepare(req)
        assert not res.ok and "accounts.yaml" in res.errors[0] and node.p.prepared is None

    def test_endpoint_change_requires_restart(self, layout: HomeLayout) -> None:
        node = Node(layout, SimClock(0))
        req = new_version(layout, {"services.yaml": ("tcp://127.0.0.1:7101", "tcp://127.0.0.1:9101")})
        res = node.p.prepare(req)
        assert not res.ok and any("restart" in e for e in res.errors)

    def test_service_validator_and_stale_version(self, layout: HomeLayout) -> None:
        node = Node(layout, SimClock(0))
        node.p.add_validator(lambda cur, new: ["rules not supported by this shard"])
        res = node.p.prepare(new_version(layout, threshold_edit()))
        assert not res.ok and res.errors == ["rules not supported by this shard"]
        stale = ConfigPrepare(config_version=0, config_hash="x", path="config/versions/1", files={}, requested_by="x")
        assert not node.p.prepare(stale).ok


class TestDecision:
    def test_commit_and_abort(self, layout: HomeLayout) -> None:
        node = Node(layout, SimClock(0))
        req = new_version(layout, threshold_edit())
        node.p.prepare(req)
        assert node.p.commit(ConfigCommit(config_version=req.config_version)).ok
        assert node.config.config_version == req.config_version and node.commits[0][1] == {"risk_rules": ConfigEffect.IMMEDIATE}
        assert node.p.prepared is None and not (layout.home / "run" / "config-2pc" / "risk-engine-0.json").exists()
        assert node.p.commit(ConfigCommit(config_version=req.config_version)).ok  # idempotent
        req3 = new_version(layout, {"risk_rules.yaml": ("threshold: -0.031", "threshold: -0.032")})
        node.p.prepare(req3)
        assert node.p.abort(ConfigAbort(config_version=req3.config_version, reason="peer failed")).ok
        assert node.p.prepared is None and node.config.config_version == req.config_version
        assert node.p.abort(ConfigAbort(config_version=99)).ok
        assert node.p.commit(ConfigCommit(config_version=99)).code == codes.CONFLICT

    def test_no_decision_within_60s_queries_coordinator(self, layout: HomeLayout) -> None:
        clock = SimClock(0)
        node = Node(layout, clock)
        req = new_version(layout, threshold_edit())
        node.p.prepare(req)
        assert node.p.due_decision() is None
        clock.advance(61 * S)
        assert node.p.due_decision() == req.config_version
        node.p.mark_queried()
        assert node.p.due_decision() is None
        node.p.on_decision_reply(ok_reply(ConfigDecision(config_version=req.config_version, status=ConfigStatus.PREPARING)))
        assert node.p.prepared is not None  # wait and ask again later
        clock.advance(11 * S)
        assert node.p.due_decision() == req.config_version
        node.p.on_decision(ConfigDecision(config_version=req.config_version, status=ConfigStatus.FAILED))
        assert node.config.config_version == req.config_version  # FAILED means "decided to commit"

    def test_restart_in_prepared_state_recovers_and_aborts(self, layout: HomeLayout) -> None:
        clock = SimClock(0)
        node = Node(layout, clock)
        req = new_version(layout, threshold_edit())
        node.p.prepare(req)
        restarted = Node(layout, clock)  # active.json still points to version 1
        assert restarted.p.recover() == req.config_version
        assert restarted.p.due_decision() == req.config_version  # query at once after a restart
        restarted.p.on_decision(ConfigDecision(config_version=req.config_version, status=ConfigStatus.ABORTED))
        assert restarted.p.prepared is None and restarted.config.config_version == 1
        assert Node(layout, clock).p.recover() is None

    def test_restart_then_committing_decision(self, layout: HomeLayout) -> None:
        clock = SimClock(0)
        node = Node(layout, clock)
        req = new_version(layout, threshold_edit())
        node.p.prepare(req)
        restarted = Node(layout, clock)
        restarted.p.recover()
        restarted.p.on_decision(ConfigDecision(config_version=req.config_version, status=ConfigStatus.COMMITTING))
        assert restarted.config.config_version == req.config_version

    def test_commit_without_prepare_uses_active_json(self, layout: HomeLayout) -> None:
        node = Node(layout, SimClock(0))
        req = new_version(layout, threshold_edit())
        write_active(layout, req.config_version, layout.home / req.path)  # coordinator persisted the decision
        assert node.p.commit(ConfigCommit(config_version=req.config_version)).ok
        assert node.config.config_version == req.config_version
