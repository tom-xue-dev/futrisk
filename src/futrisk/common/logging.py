"""Structured logging (01 §8.7).

- JSON Lines, one file per service instance: ``<home>/logs/{instance}_{YYYYMMDD}.log``;
- fields: ts, level, instance, trading_day, corr_id, event, detail;
- daily rotation, extra split above ``rotation_mb``; old files compressed and
  kept for ``retention_days`` (03 §8);
- ``enqueue=True`` so logging never blocks the business loop.

Usage::

    from loguru import logger
    setup_logging("risk-engine-1", layout.logs_dir)
    logger.bind(corr_id=order_id, trading_day=td).info("order.accepted", qty=3)
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from loguru import logger

_RESERVED = {"instance", "trading_day", "corr_id"}
#: log times are Beijing time whatever the OS time zone (this machine runs UTC+9); fixed offset
#: because China has no DST and loguru's time formatting needs a fixed-offset tzinfo
LOG_TZ = timezone(timedelta(hours=8), "CST")
_configured_instance: str | None = None


def _json_patcher(instance: str):  # type: ignore[no-untyped-def]
    def patch(record: dict[str, Any]) -> None:
        record["time"] = record["time"].astimezone(LOG_TZ)
        extra = record["extra"]
        detail = {k: v for k, v in extra.items() if k not in _RESERVED and k != "json"}
        doc = {
            "ts": record["time"].isoformat(timespec="microseconds"),
            "level": record["level"].name,
            "instance": extra.get("instance", instance),
            "trading_day": extra.get("trading_day"),
            "corr_id": extra.get("corr_id"),
            "event": record["message"],
            "module": record["name"],
        }
        if detail:
            doc["detail"] = detail
        if record["exception"] is not None:
            doc["exception"] = str(record["exception"].value)
        extra["json"] = json.dumps(doc, ensure_ascii=False, default=str)

    return patch


class _Rotator:
    """Rotate at Beijing midnight or when the file exceeds ``max_bytes``."""

    def __init__(self, max_bytes: int) -> None:
        self._max = max_bytes
        self._day = datetime.now(LOG_TZ).date()

    def __call__(self, message: Any, file: Any) -> bool:
        today = datetime.now(LOG_TZ).date()
        if today != self._day:
            self._day = today
            return True
        file.seek(0, 2)
        return bool(file.tell() + len(message) > self._max)


def setup_logging(
    instance: str,
    logs_dir: Path | None,
    level: str = "INFO",
    retention_days: int = 30,
    rotation_mb: int = 100,
    console: bool = True,
) -> None:
    """Configure loguru for a service instance (idempotent per process)."""
    global _configured_instance
    logger.remove()
    logger.configure(extra={"instance": instance}, patcher=_json_patcher(instance))
    if console:
        logger.add(
            sys.stderr,
            level=level,
            format="<green>{time:HH:mm:ss.SSS}</green> <level>{level: <7}</level> "
            "<cyan>{extra[instance]}</cyan> {message}",
            enqueue=False,
        )
    if logs_dir is not None:
        logs_dir.mkdir(parents=True, exist_ok=True)
        logger.add(
            str(logs_dir / f"{instance}_{{time:YYYYMMDD}}.log"),
            level=level,
            format="{extra[json]}",
            rotation=_Rotator(rotation_mb * 1024 * 1024),
            retention=f"{retention_days} days",
            compression="zip",
            encoding="utf-8",
            enqueue=True,
        )
    _configured_instance = instance


def configured_instance() -> str | None:
    return _configured_instance
