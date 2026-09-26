"""Runtime GC policy (bench follow-up, 01 §11 RK-01): thresholds at start, collect + freeze once before the
first RUNNING heartbeat, pause statistics in the heartbeat ``detail["gc"]``, ``services.yaml`` + ``FUTRISK_GC``."""

from __future__ import annotations

import gc
from collections.abc import Iterator
from pathlib import Path

import pytest

from futrisk.common.clock import MarketClock
from futrisk.common.config import FullConfig
from futrisk.common.config.services import GcConfig
from futrisk.messages.enums import ServiceState
from futrisk.runtime import AsyncServiceRuntime, gcpolicy
from futrisk.runtime.gcpolicy import DEFAULT_THRESHOLD, GcSettings


@pytest.fixture
def restore_gc() -> Iterator[None]:
    threshold = gc.get_threshold()
    frozen = gcpolicy._frozen
    yield
    gc.set_threshold(*threshold)
    gcpolicy._frozen = frozen


class TestSettings:
    def test_config_and_environment(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("FUTRISK_GC", raising=False)
        assert GcSettings.from_config(GcConfig()) == GcSettings(True, DEFAULT_THRESHOLD, True)
        assert GcSettings.from_config(GcConfig(enabled=False, threshold=(1000, 5, 5))) == GcSettings(False, (1000, 5, 5), True)
        monkeypatch.setenv("FUTRISK_GC", "off")
        assert not GcSettings.from_config(GcConfig()).enabled
        monkeypatch.setenv("FUTRISK_GC", "20000,10,50")
        assert GcSettings.from_config(GcConfig()) == GcSettings(True, (20000, 10, 50), True)
        monkeypatch.setenv("FUTRISK_GC", "nofreeze")
        assert GcSettings.from_config(GcConfig()) == GcSettings(True, DEFAULT_THRESHOLD, False)
        monkeypatch.setenv("FUTRISK_GC", "700,10,10,nofreeze")
        assert GcSettings.from_config(GcConfig()) == GcSettings(True, (700, 10, 10), False)
        monkeypatch.setenv("FUTRISK_GC", "garbage,1")
        assert GcSettings.from_config(GcConfig()) == GcSettings(True, DEFAULT_THRESHOLD, True)

    def test_default_config_has_the_section(self, default_config: FullConfig) -> None:
        assert default_config.services.bus.gc == GcConfig()
        assert default_config.services.bus.gc.threshold == DEFAULT_THRESHOLD


class TestPolicy:
    def test_thresholds_and_statistics(self, restore_gc: None) -> None:
        gcpolicy.apply_at_start(GcSettings(threshold=(12_345, 7, 9)))
        assert gc.get_threshold() == (12_345, 7, 9)
        before = gcpolicy.stats_snapshot()
        assert before is not None
        gc.collect(0)
        after = gcpolicy.stats_snapshot()
        assert after is not None and after["collections"][0] > before["collections"][0]
        assert after["last_generation"] == 0 and after["last_pause_ms"] >= 0 and after["threshold"] == [12_345, 7, 9]
        gcpolicy.apply_at_start(GcSettings(enabled=False, threshold=(1, 1, 1)))
        assert gc.get_threshold() == (12_345, 7, 9)  # disabled: thresholds untouched

    def test_freeze_once_per_process(self, restore_gc: None, monkeypatch: pytest.MonkeyPatch) -> None:
        calls: list[str] = []
        monkeypatch.setattr(gcpolicy.gc, "collect", lambda *a: calls.append("collect") or 0)
        monkeypatch.setattr(gcpolicy.gc, "freeze", lambda: calls.append("freeze"))
        gcpolicy._frozen = False
        assert not gcpolicy.freeze_once(GcSettings(freeze_on_running=False))
        assert not gcpolicy.freeze_once(GcSettings(enabled=False))
        assert gcpolicy.freeze_once(GcSettings())
        assert not gcpolicy.freeze_once(GcSettings())
        assert calls == ["collect", "freeze"]


class TestRuntime:
    async def test_freeze_before_the_first_running_heartbeat(
        self, tmp_path: Path, bus_config: FullConfig, restore_gc: None, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("FUTRISK_GC", raising=False)
        seen: list[str] = []
        async with AsyncServiceRuntime("clearing-worker", config=bus_config, home=tmp_path, market_clock=MarketClock(),
                                       setup_logs=False) as rt:
            assert gc.get_threshold() == DEFAULT_THRESHOLD
            monkeypatch.setattr(gcpolicy.gc, "collect", lambda *a: seen.append(f"collect:{rt.state.value}") or 0)
            monkeypatch.setattr(gcpolicy.gc, "freeze", lambda: seen.append(f"freeze:{rt.state.value}"))
            gcpolicy._frozen = False
            rt.set_state(ServiceState.RUNNING)
            rt.set_state(ServiceState.DEGRADED)
            rt.set_state(ServiceState.RUNNING)
            assert seen == ["collect:STARTING", "freeze:STARTING"]  # once, before RUNNING is announced
            detail = rt.build_heartbeat().detail["gc"]
            assert set(detail) >= {"collections", "last_pause_ms", "max_pause_ms", "frozen", "threshold"}
            assert detail["threshold"] == list(DEFAULT_THRESHOLD)
