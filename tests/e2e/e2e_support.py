"""Helpers for the multi-process end-to-end tests (``tests/e2e``).

The tests run the real system the way an operator does - ``futrisk demo``, ``futrisk kill``,
``futrisk down`` as subprocesses - against a throw-away home whose every port is a fresh
loopback port, so they never collide with another futrisk on the machine. Everything is
observed through the public surfaces only: the REST API (``/api/v1``), the CLI and the
md-gateway PUB socket.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TypeVar

import httpx
import yaml

from futrisk.common.config import ensure_home_config
from futrisk.common.paths import HomeLayout, bundled_config_dir
from futrisk.runtime.testing import free_tcp_port

T = TypeVar("T")

REPO = Path(__file__).resolve().parents[2]


def run_home(keep: int = 3) -> Path:
    """A fresh ``<repo>/var/e2e/run-<timestamp>/home`` (git-ignored); only the newest ``keep`` runs are kept."""
    root = REPO / "var" / "e2e"
    root.mkdir(parents=True, exist_ok=True)
    runs = sorted(p for p in root.iterdir() if p.is_dir() and p.name.startswith("run-"))
    for old in runs[: max(0, len(runs) - (keep - 1))]:
        shutil.rmtree(old, ignore_errors=True)
    return root / time.strftime("run-%Y%m%d-%H%M%S") / "home"


def make_home(home: Path, *, start_at: str | None = None) -> dict[str, Any]:
    """Copy the bundled config into ``home`` and give every endpoint a free loopback port."""
    used: set[int] = set()

    def port() -> int:
        while True:
            p = free_tcp_port()
            if p not in used and not 7000 <= p <= 8100:  # keep clear of the default ports
                used.add(p)
                return p

    if home.exists():
        shutil.rmtree(home)
    cfg_dir = home / "config"
    shutil.copytree(bundled_config_dir(), cfg_dir)

    def edit(name: str, fn: Callable[[dict[str, Any]], None]) -> dict[str, Any]:
        path = cfg_dir / name
        doc: dict[str, Any] = yaml.safe_load(path.read_text(encoding="utf-8"))
        fn(doc)
        path.write_text(yaml.safe_dump(doc, sort_keys=False, allow_unicode=True), encoding="utf-8")
        return doc

    def services(doc: dict[str, Any]) -> None:
        for inst in doc["instances"].values():
            inst["pub"] = f"tcp://127.0.0.1:{port()}"
            inst["router"] = f"tcp://127.0.0.1:{port()}"
            if "http" in inst:
                inst["http"] = f"http://127.0.0.1:{port()}"

    svc = edit("services.yaml", services)
    fa, fb, br, ct = port(), port(), port(), port()

    def sim(doc: dict[str, Any]) -> None:
        doc["feed_a"]["port"] = fa
        doc["feed_b"]["port"] = fb
        doc["broker"]["endpoint"] = f"tcp://127.0.0.1:{br}"
        doc["control"]["port"] = ct

    def feeds(doc: dict[str, Any]) -> None:
        doc["feed_a"]["url"] = f"ws://127.0.0.1:{fa}/feed"
        doc["feed_b"]["url"] = f"http://127.0.0.1:{fb}/v1/quotes"

    edit("sim.yaml", sim)
    edit("feeds.yaml", feeds)
    if start_at:
        edit("app.yaml", lambda doc: doc["demo"].__setitem__("start_at", start_at))
    ensure_home_config(HomeLayout(home))
    return svc


def cli(home: Path, *args: str, timeout: float = 120.0) -> subprocess.CompletedProcess[str]:
    """Run ``futrisk <args> --home <home>`` as an operator would."""
    env = dict(os.environ, PYTHONUTF8="1", PYTHONIOENCODING="utf-8")
    return subprocess.run(
        [sys.executable, "-m", "futrisk.main", *args, "--home", str(home)],
        capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=timeout, env=env, cwd=REPO,
    )


def wait_until(pred: Callable[[], T | None], timeout: float, *, interval: float = 1.0, what: str = "condition") -> T:
    """Poll ``pred`` until it returns a truthy value (returned); AssertionError after ``timeout`` s."""
    deadline = time.monotonic() + timeout
    last_exc: Exception | None = None
    while True:
        try:
            value = pred()
            if value:
                return value
        except Exception as exc:  # noqa: BLE001 - transient API errors while services (re)start
            last_exc = exc
        if time.monotonic() > deadline:
            raise AssertionError(f"timed out after {timeout:.0f}s waiting for {what}" + (f" (last error: {last_exc!r})" if last_exc else ""))
        time.sleep(interval)


@dataclass
class Api:
    """REST client of the api-gateway of one home (bootstrap ADMIN key)."""

    base: str
    key: str

    @classmethod
    def for_home(cls, home: Path, svc: dict[str, Any]) -> Api:
        key = (home / "secrets" / "bootstrap_admin.key").read_text(encoding="utf-8").strip()
        return cls(svc["instances"]["api-gateway"]["http"] + "/api/v1", key)

    def _client(self) -> httpx.Client:
        return httpx.Client(trust_env=False, timeout=30.0, headers={"X-API-Key": self.key})

    def get(self, path: str, **params: Any) -> dict[str, Any]:
        with self._client() as c:
            doc: dict[str, Any] = c.get(self.base + path, params={k: v for k, v in params.items() if v is not None}).json()
        return doc

    def post(self, path: str, body: dict[str, Any], *, key: str | None = None) -> dict[str, Any]:
        with self._client() as c:
            doc: dict[str, Any] = c.post(self.base + path, json=body,
                                         headers={"Idempotency-Key": key or str(uuid.uuid4())}).json()
        return doc

    def data(self, path: str, **params: Any) -> Any:
        doc = self.get(path, **params)
        assert doc.get("ok"), f"GET {path}: {doc.get('code')} {doc.get('message')}"
        return doc.get("data")

    # ---------------------------------------------------------------- domain helpers
    def account(self, account_id: str) -> dict[str, Any]:
        d: dict[str, Any] = self.data(f"/accounts/{account_id}")
        return d

    def active_levels(self, account_id: str) -> set[str]:
        """``{"daily_loss/halt", ...}`` of the rule levels currently ACTIVE on the account."""
        return {f"{r['rule_id']}/{r['active_level']}" for r in self.account(account_id).get("rules") or []
                if r.get("active_level") and r.get("sm_state") == "ACTIVE"}

    def risk_events(self, account_id: str) -> list[dict[str, Any]]:
        d = self.data("/risk/events", account=account_id)
        items: list[dict[str, Any]] = d["items"] if isinstance(d, dict) else d
        return items

    def orders(self, account_id: str) -> list[dict[str, Any]]:
        d = self.data(f"/accounts/{account_id}/orders")
        items: list[dict[str, Any]] = d["items"] if isinstance(d, dict) else d
        return items

    def place(self, account_id: str, instrument_id: str, direction: str, offset: str, qty: int) -> dict[str, Any]:
        coid = str(uuid.uuid4())
        return self.post("/orders", {"client_order_id": coid, "account_id": account_id, "instrument_id": instrument_id,
                                     "direction": direction, "offset": offset, "order_type": "MARKET", "qty": qty},
                         key=coid)
