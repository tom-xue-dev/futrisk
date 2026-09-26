"""``futrisk svc md-gateway`` entry point: runtime + service, exits when the parent process dies (01 §5.3.6)."""

from __future__ import annotations

import asyncio
import dataclasses
import subprocess
import sys
from pathlib import Path

import pytest

from futrisk.cli.svc_cmd import resolve
from futrisk.common.clock import MarketClock
from futrisk.common.config import FullConfig
from futrisk.runtime.testing import free_tcp_port
from futrisk.services.md_gateway import main as md_main

from .mdg_support import md_config

pytestmark = pytest.mark.timeout(60)


def test_svc_command_resolves_to_run() -> None:
    assert resolve("md-gateway") is md_main.run


async def test_exits_when_the_parent_process_dies(default_config: FullConfig, tmp_path: Path) -> None:
    cfg = md_config(default_config)
    port = free_tcp_port()  # nothing listens: the sources keep reconnecting
    feeds = cfg.feeds.model_copy(
        update={
            "feed_a": cfg.feeds.feed_a.model_copy(update={"url": f"ws://127.0.0.1:{port}/feed"}),
            "feed_b": cfg.feeds.feed_b.model_copy(update={"url": f"http://127.0.0.1:{port}/v1/quotes"}),
        }
    )
    cfg = dataclasses.replace(cfg, feeds=feeds)
    parent = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(1.5)"])
    try:
        code = await asyncio.wait_for(
            md_main.main_async(
                parent_pid=parent.pid,
                home=str(tmp_path),
                runtime_kwargs={"config": cfg, "market_clock": MarketClock(), "setup_logs": False},
                signals=False,
                parent_poll_s=0.2,
            ),
            30,
        )
    finally:
        parent.wait(10)
    assert code == 0
    assert (tmp_path / "db" / "md.db").exists()
