"""``futrisk doctor``: environment checks against temporary homes (free / occupied ports, bad config)."""

from __future__ import annotations

import json
import socket
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest
import yaml
from clitools_support import make_home, tools_app
from typer.testing import CliRunner

from futrisk.cli import apiclient, doctor_cmd
from futrisk.common.config import load_default_config
from futrisk.common.paths import HomeLayout
from futrisk.runtime.testing import free_tcp_port

W32TM_OK = "Leap Indicator: 0(no warning)\nSource: time.windows.com,0x9\nLast Successful Sync Time: 2026/9/25 10:00:00\n"


@pytest.fixture(autouse=True)
def _w32tm(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(doctor_cmd, "run_w32tm", lambda: (0, W32TM_OK))


@pytest.fixture(autouse=True)
def _no_network(monkeypatch: pytest.MonkeyPatch) -> None:
    """The /health question never leaves the test (a real gateway may run on this machine)."""

    def refuse(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("no network in tests", request=request)

    monkeypatch.setattr(apiclient, "TRANSPORT", httpx.MockTransport(refuse))


def serve_health(monkeypatch: pytest.MonkeyPatch, data: dict[str, Any]) -> list[str]:
    """Answer GET /api/v1/health with ``data`` (mutable between runs); returns the requested URLs."""
    urls: list[str] = []

    def health(request: httpx.Request) -> httpx.Response:
        urls.append(str(request.url))
        return httpx.Response(200, json={"ok": True, "code": "OK", "message": "", "data": dict(data)})

    monkeypatch.setattr(apiclient, "TRANSPORT", httpx.MockTransport(health))
    return urls


@pytest.fixture
def listener() -> Iterator[int]:
    """A port held by this (non-futrisk) test process."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    sock.listen(1)
    yield sock.getsockname()[1]
    sock.close()


def free_home(root: Path, overrides: dict[str, dict[str, str]] | None = None, *,
              api_port: int | None = None) -> tuple[Path, int]:
    """A home whose every endpoint (bus, api http, sim) is on a fresh free port; returns (home, api port)."""
    services: dict[str, dict[str, Any]] = {}
    for name, inst in load_default_config().services.instances.items():
        services[name] = {k: f"tcp://127.0.0.1:{free_tcp_port()}" for k in ("pub", "router") if getattr(inst, k)}
    for name, update in (overrides or {}).items():
        services.setdefault(name, {}).update(update)
    api_port = api_port or free_tcp_port()
    sim = {"feed_a": {"port": free_tcp_port()}, "feed_b": {"port": free_tcp_port()},
           "broker": {"endpoint": f"tcp://127.0.0.1:{free_tcp_port()}"}, "control": {"port": free_tcp_port()}}
    home = make_home(root, api_url=f"http://127.0.0.1:{api_port}", services=services, sim=sim)
    return home, api_port


def run(*args: str) -> tuple[Any, dict[str, Any]]:
    r = CliRunner().invoke(tools_app(), ["doctor", *args, "--json"], catch_exceptions=False)
    doc = json.loads(r.stdout)
    return r, doc


def by_check(doc: dict[str, Any]) -> dict[str, dict[str, str]]:
    return {c["check"]: c for c in doc["checks"]}


def test_doctor_passes_on_a_clean_home(tmp_path: Path) -> None:
    home, api_port = free_home(tmp_path / "home")
    other_api = free_tcp_port()
    r, doc = run("--home", str(home), "--api", f"http://127.0.0.1:{other_api}")
    assert r.exit_code == 0, r.stdout
    checks = by_check(doc)
    assert doc["ok"] is True
    for name in ("python", "home", "non-ASCII path", "tz Asia/Shanghai", "clock sync", "pyzmq", "sqlite", "config",
                 "config active", "ports md-gateway", "ports api-gateway", "ports sim-exchange", "dashboard"):
        assert checks[name]["status"] == "PASS", checks[name]
    assert "time.windows.com" in checks["clock sync"]["detail"]
    assert checks["system"]["detail"].startswith("down: all 19 local endpoints are free")
    assert f"http 127.0.0.1:{other_api} free" in checks["ports api-gateway"]["detail"]  # --api wins
    assert "中文" in checks["non-ASCII path"]["detail"]
    assert checks["app mode"]["detail"].startswith("app.mode=demo")
    assert checks["api-gateway home"]["status"] == "INFO" and "no api-gateway listening" in checks["api-gateway home"]["detail"]
    assert checks["api key"]["status"] == "INFO" and "present" in checks["api key"]["detail"]
    assert not list(home.glob(".doctor-*")) and not list((home / "db").glob(".doctor-*"))  # probes cleaned up

    table = CliRunner().invoke(tools_app(), ["doctor", "--home", str(home)], catch_exceptions=False)
    assert table.exit_code == 0 and "futrisk doctor" in table.stdout and "0 failed" in table.stdout


def test_doctor_fails_when_another_program_holds_a_port(tmp_path: Path, listener: int,
                                                        monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(doctor_cmd, "is_futrisk_cmdline", lambda name, cmdline: False)
    home, _ = free_home(tmp_path / "home", {"md-gateway": {"pub": f"tcp://127.0.0.1:{listener}"}})
    r, doc = run("--home", str(home))
    assert r.exit_code == 1
    row = by_check(doc)["ports md-gateway"]
    assert row["status"] == "FAIL" and f"127.0.0.1:{listener} in use by" in row["detail"] and "not futrisk" in row["detail"]
    assert by_check(doc)["ports trade-gateway"]["status"] == "PASS"
    assert "held by other programs" in by_check(doc)["system"]["detail"]

    table = CliRunner().invoke(tools_app(), ["doctor", "--home", str(home)], catch_exceptions=False)
    assert table.exit_code == 1 and "FAIL" in table.stdout and "1 failed" in table.stdout


def test_doctor_reports_ports_held_by_futrisk(tmp_path: Path, listener: int, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(doctor_cmd, "is_futrisk_cmdline", lambda name, cmdline: True)
    home, _ = free_home(tmp_path / "home", {"md-gateway": {"router": f"tcp://127.0.0.1:{listener}"}})
    r, doc = run("--home", str(home))
    assert r.exit_code == 0, r.stdout
    row = by_check(doc)["ports md-gateway"]
    assert row["status"] == "PASS" and f"127.0.0.1:{listener} held by futrisk" in row["detail"]
    assert by_check(doc)["system"]["detail"].startswith("1 of 19 endpoints held by futrisk, 18 free")


def test_doctor_fails_on_bad_config_and_duplicate_endpoints(tmp_path: Path) -> None:
    dup = f"tcp://127.0.0.1:{free_tcp_port()}"
    home, _ = free_home(tmp_path / "home", {"md-gateway": {"pub": dup}, "trade-gateway": {"pub": dup}})
    r, doc = run("--home", str(home))
    assert r.exit_code == 1
    assert "configured twice (also md-gateway pub)" in by_check(doc)["ports trade-gateway"]["detail"]

    working = home / "config" / "risk_rules.yaml"
    working.write_text(yaml.safe_dump({"rules": "not a list"}), encoding="utf-8")
    r, doc = run("--home", str(home))
    checks = by_check(doc)
    assert r.exit_code == 1
    assert checks["config"]["status"] == "FAIL" and "risk_rules.yaml" in checks["config"]["detail"]
    assert checks["config active"]["status"] == "PASS"  # the committed version still loads


def test_doctor_does_not_create_a_missing_home(tmp_path: Path) -> None:
    home = tmp_path / "nowhere" / "home"
    r, doc = run("--home", str(home))
    checks = by_check(doc)
    assert checks["home"]["status"] == "INFO" and "does not exist yet" in checks["home"]["detail"]
    assert checks["config"]["status"] == "INFO" and "bundled defaults valid" in checks["config"]["detail"]
    assert "home not created yet" in checks["non-ASCII path"]["detail"]
    assert checks["logs"]["status"] == "INFO"
    assert not home.exists() and not (tmp_path / "nowhere").exists()


def test_clock_sync_outcomes() -> None:
    fail = doctor_cmd.check_clock_sync(lambda: (-2147023834, "发生下列错误: 服务尚未启动。 (0x80070426)"))
    assert fail.status == "WARN" and "0x80070426" in fail.detail and "服务尚未启动" in fail.detail
    cmos = doctor_cmd.check_clock_sync(lambda: (0, "Source: Local CMOS Clock\n"))
    assert cmos.status == "WARN" and "not synchronised" in cmos.detail
    zh = doctor_cmd.check_clock_sync(lambda: (0, "跃点指示符: 0(未发出警告)\n源: time.windows.com,0x9\n上次成功同步时间: 2026/9/25 10:00:00\n"))
    assert zh.status == "PASS" and "time.windows.com" in zh.detail and "2026/9/25" in zh.detail
    assert doctor_cmd.check_clock_sync(lambda: None).status == "INFO"


def test_futrisk_process_detection() -> None:
    fut = doctor_cmd.is_futrisk_cmdline
    assert fut("python.exe", ["python.exe", "-m", "futrisk.main", "svc", "api-gateway", "--home", "x"])
    assert fut("futrisk.exe", ["C:\\futrisk\\futrisk.exe", "up", "--headless"])
    assert fut("python.exe", ["python", "D:\\CTA\\.venv\\Scripts\\futrisk.exe", "svc", "md-gateway"])
    assert fut("python.exe", ["python", "-m", "futrisk.sim.exchange", "--home", "x"])
    assert not fut("python.exe", ["python", "-m", "pytest", "tests/unit/cli"])
    assert not fut("nginx.exe", ["nginx.exe", "-c", "C:\\futrisk-home\\nginx.conf"])
    svc = doctor_cmd.service_of
    assert svc(["python", "-m", "futrisk.main", "svc", "risk-engine", "--shard", "1"]) == "risk-engine-1"
    assert svc(["futrisk.exe", "svc", "md-gateway", "--instance", "md-gateway"]) == "md-gateway"
    assert svc(["futrisk.exe", "up", "--headless"]) == "supervisor"


def test_doctor_checks_whose_home_the_gateway_serves(tmp_path: Path, listener: int,
                                                    monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(doctor_cmd, "is_futrisk_cmdline", lambda name, cmdline: True)
    home, _ = free_home(tmp_path / "home", api_port=listener)  # "the api-gateway" listens
    ours = HomeLayout(home).home_id()
    health = {"status": "ok", "home_id": ours, "pid": 777}
    urls = serve_health(monkeypatch, health)
    r, doc = run("--home", str(home))
    row = by_check(doc)["api-gateway home"]
    assert r.exit_code == 0, r.stdout
    assert row["status"] == "PASS" and f"serves this home (home_id {ours})" in row["detail"] and "pid 777" in row["detail"]
    assert urls == [f"http://127.0.0.1:{listener}/api/v1/health"]

    health["home_id"] = "f" * 32
    r, doc = run("--home", str(home))
    row = by_check(doc)["api-gateway home"]
    assert r.exit_code == 1
    assert row["status"] == "FAIL" and f"serves another home (home_id {'f' * 32}; this home is {ours})" in row["detail"]

    del health["home_id"]
    r, doc = run("--home", str(home))
    row = by_check(doc)["api-gateway home"]
    assert r.exit_code == 0 and row["status"] == "WARN" and "reports no home_id" in row["detail"]


def test_doctor_on_a_never_started_home_with_a_gateway_up(tmp_path: Path, listener: int,
                                                         monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(doctor_cmd, "is_futrisk_cmdline", lambda name, cmdline: True)
    home, _ = free_home(tmp_path / "home", api_port=listener)
    serve_health(monkeypatch, {"status": "ok", "home_id": "a" * 32})
    r, doc = run("--home", str(home))
    row = by_check(doc)["api-gateway home"]
    assert r.exit_code == 1 and row["status"] == "FAIL" and "never started (no home_id)" in row["detail"]
    assert not HomeLayout(home).home_id_file.exists()  # the doctor does not create the id


def test_doctor_flags_ports_held_by_futrisk_of_another_home(tmp_path: Path, listener: int,
                                                           monkeypatch: pytest.MonkeyPatch) -> None:
    other = tmp_path / "other-home"
    argv = ["python", "-m", "futrisk.main", "svc", "md-gateway", "--home", str(other)]
    monkeypatch.setattr(doctor_cmd, "process_info", lambda pid: ("python.exe", argv))
    home, _ = free_home(tmp_path / "home", {"md-gateway": {"pub": f"tcp://127.0.0.1:{listener}"}})
    r, doc = run("--home", str(home))
    row = by_check(doc)["ports md-gateway"]
    assert r.exit_code == 1
    assert row["status"] == "FAIL" and "held by futrisk md-gateway (pid" in row["detail"]
    assert f"of another home ({other})" in row["detail"]

    argv[-1] = str(home)  # the same home (spelled differently is fine)
    r, doc = run("--home", str(home))
    row = by_check(doc)["ports md-gateway"]
    assert row["status"] == "PASS" and "held by futrisk md-gateway" in row["detail"]
