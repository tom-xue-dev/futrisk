"""Atomic file replacement that tolerates Windows sharing violations.

On Windows ``os.replace`` fails with ``PermissionError`` (WinError 5/32) while
another process or thread has the target open, e.g. the service polling a
backtest's ``progress.json`` or api-gateway serving a report being
regenerated. The window is short, so the rename is retried briefly.
"""

from __future__ import annotations

import os
import time
from pathlib import Path


def replace_file(src: str | Path, dst: str | Path, *, attempts: int = 40, delay_s: float = 0.025) -> None:
    """``os.replace(src, dst)``, retried while the target is locked by a reader."""
    for i in range(max(1, attempts)):
        try:
            os.replace(src, dst)
            return
        except PermissionError:
            if i == attempts - 1:
                raise
            time.sleep(delay_s)


__all__ = ["replace_file"]
