"""One-shot demo seeding for docker compose: the "books" step of ``futrisk demo``.

compose runs this as the ``seed`` service once api-gateway is healthy. It seeds the
first-day start-of-day books from ``accounts.yaml`` through the REST API (``POST
/api/v1/settlement/seed-sod``, the same request and idempotency key as ``futrisk demo``)
unless books already exist; the risk-engine shards load that snapshot and finish their
start-up, and the bots trade from the market open on. Re-running it is harmless.

The trading day comes from the **market** clock, as in ``futrisk demo``: the supervisor's
``sys.clock`` (demo mode: the first start puts the market at ``app.demo.start_at``, by
default the next trading day's 08:58), or the wall clock when no supervisor is configured.

Environment: ``FUTRISK_API`` (default ``http://api-gateway:8000``), ``FUTRISK_API_KEY`` or
``FUTRISK_API_KEY_FILE`` (api-gateway's ``<home>/secrets/bootstrap_admin.key``, mounted
read-only), ``FUTRISK_SEED_TIMEOUT_S`` (default 180). Exit code 0 when the books exist.
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

import httpx

from futrisk.cli.ops_cmd import OpsContext, SupervisorClient, load_context, seed_sod_once
from futrisk.common.clock import ns_to_bj

SUPERVISOR = "supervisor"


def api_key(deadline: float) -> str | None:
    key = os.environ.get("FUTRISK_API_KEY", "").strip()
    path = Path(os.environ.get("FUTRISK_API_KEY_FILE", "/var/lib/futrisk-api/secrets/bootstrap_admin.key"))
    while not key and time.monotonic() < deadline:  # api-gateway writes it at its first start
        try:
            key = path.read_text(encoding="utf-8").strip()
        except OSError:
            time.sleep(1.0)
    return key or None


def market_now_ns(ctx: OpsContext, deadline: float) -> int | None:
    """Market time now: wall clock + the supervisor's ``sys.clock`` offset (``None``: no answer in time)."""
    if SUPERVISOR not in ctx.cfg.services.instances:
        return time.time_ns()
    with SupervisorClient(ctx.cfg) as sc:
        while True:
            clk = sc.clock(timeout_ms=2_000)
            if clk is not None:
                return time.time_ns() + int(clk.offset_ns)
            if time.monotonic() >= deadline:
                return None
            time.sleep(1.0)


def main() -> int:
    api = os.environ.get("FUTRISK_API", "http://api-gateway:8000").rstrip("/")
    deadline = time.monotonic() + float(os.environ.get("FUTRISK_SEED_TIMEOUT_S", "180"))
    ctx = load_context(None)  # FUTRISK_HOME: the shared config volume holds the committed version
    key = api_key(deadline)
    if key is None:
        print("seed: no API key (FUTRISK_API_KEY / FUTRISK_API_KEY_FILE)", file=sys.stderr)
        return 1
    now = market_now_ns(ctx, deadline)
    if now is None:
        print("seed: the supervisor did not answer with the market clock (sys.clock)", file=sys.stderr)
        return 1
    print(f"seed: market time {ns_to_bj(now):%Y-%m-%d %H:%M:%S} (Beijing)")
    with httpx.Client(trust_env=False, timeout=35.0) as client:
        ok, message = seed_sod_once(client, api, key, ctx, now, deadline)
    print(f"seed: {message}", file=sys.stdout if ok else sys.stderr)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
