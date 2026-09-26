"""Frozen exe (PyInstaller): data next to the exe, the exe's code identity instead of the source hash / git."""

from __future__ import annotations

import hashlib
import sys
from pathlib import Path

import pytest

from futrisk.backtest.config import BacktestConfig, app_path
from futrisk.backtest.manifest import code_hash, git_sha


@pytest.fixture
def frozen(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    exe = tmp_path / "futrisk.exe"
    exe.write_bytes(b"MZ fake exe")
    internal = tmp_path / "_internal"
    internal.mkdir()
    (internal / "base_library.zip").write_bytes(b"PK fake stdlib")
    monkeypatch.setattr(sys, "frozen", True, raising=False)
    monkeypatch.setattr(sys, "executable", str(exe))
    monkeypatch.setattr(sys, "_MEIPASS", str(internal), raising=False)
    return tmp_path.resolve()


def test_data_dir_and_relative_paths_are_next_to_the_exe(frozen: Path) -> None:
    assert BacktestConfig(name="x").data_dir() == frozen / "data" / "hist"
    assert BacktestConfig(name="x", data={"dir": "mydata/hist"}).data_dir() == frozen / "mydata" / "hist"
    assert app_path("cfg") == frozen / "cfg"
    absolute = frozen / "elsewhere"
    assert app_path(str(absolute)) == absolute


def test_code_identity_replaces_the_source_hash_and_git(frozen: Path) -> None:
    want = hashlib.sha256(b"MZ fake exe" + b"PK fake stdlib").hexdigest()
    assert code_hash() == (want, {"exe": want})
    assert git_sha() == want
    from futrisk.services.clearing.backtests import git_sha as hosted_git_sha

    assert hosted_git_sha() == want and len(want) == 64  # backtest_runs.git_sha holds <= 64 chars


def test_development_keeps_the_source_hash() -> None:
    total, per = code_hash()
    assert len(total) == 64 and {"backtest", "core"} <= set(per)
    assert BacktestConfig(name="x").data_dir().parts[-2:] == ("data", "hist")
