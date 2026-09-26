"""``futrisk config commit``: offline commit of ``<home>/config`` as a new active version."""

from __future__ import annotations

import json
from pathlib import Path

import yaml
from typer.testing import CliRunner

from futrisk.cli.config_cmd import app
from futrisk.common.config import ensure_home_config
from futrisk.common.paths import HomeLayout
from futrisk.runtime.testing import free_tcp_port


def _home(tmp_path: Path) -> HomeLayout:
    layout = HomeLayout(tmp_path / "home")
    ensure_home_config(layout)
    # free supervisor ports so that "is the system running?" is answered "no" deterministically
    path = layout.config_dir / "services.yaml"
    svc = yaml.safe_load(path.read_text(encoding="utf-8"))
    svc["instances"]["supervisor"]["pub"] = f"tcp://127.0.0.1:{free_tcp_port()}"
    svc["instances"]["supervisor"]["router"] = f"tcp://127.0.0.1:{free_tcp_port()}"
    path.write_text(yaml.safe_dump(svc, sort_keys=False, allow_unicode=True), encoding="utf-8")
    return layout


def test_commit_snapshots_the_working_config_as_the_next_version(tmp_path: Path) -> None:
    layout = _home(tmp_path)
    before = json.loads(layout.active_json.read_text(encoding="utf-8"))["config_version"]
    rules = layout.config_dir / "risk_rules.yaml"
    rules.write_text(rules.read_text(encoding="utf-8").replace("cooldown_s: 60", "cooldown_s: 90", 1), encoding="utf-8")
    result = CliRunner().invoke(app, ["commit", "--home", str(layout.home)])
    assert result.exit_code == 0, result.output
    active = json.loads(layout.active_json.read_text(encoding="utf-8"))
    assert active["config_version"] == before + 1
    committed = layout.home / active["path"] / "risk_rules.yaml"
    assert "cooldown_s: 90" in committed.read_text(encoding="utf-8")


def test_an_invalid_working_config_is_not_committed(tmp_path: Path) -> None:
    layout = _home(tmp_path)
    before = layout.active_json.read_text(encoding="utf-8")
    (layout.config_dir / "accounts.yaml").write_text("accounts: {'A.B': {name: x, shard: 0, initial_capital: 1}}\n",
                                                     encoding="utf-8")
    result = CliRunner().invoke(app, ["commit", "--home", str(layout.home)])
    assert result.exit_code == 1 and "INVALID" in result.output
    assert layout.active_json.read_text(encoding="utf-8") == before
