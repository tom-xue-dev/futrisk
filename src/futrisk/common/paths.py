"""Runtime directory layout (01 §7.1).

``<home>`` defaults to ``<exe dir>/var`` for the frozen exe (falling back to
``%LOCALAPPDATA%/futrisk`` when not writable) and to ``<repo>/var`` in
development. ``--home`` or ``FUTRISK_HOME`` override it.
"""

from __future__ import annotations

import os
import sys
import uuid
from dataclasses import dataclass
from pathlib import Path


def is_frozen() -> bool:
    return bool(getattr(sys, "frozen", False))


def repo_root() -> Path:
    # src/futrisk/common/paths.py -> parents[3] is the repository root
    return Path(__file__).resolve().parents[3]


def app_dir() -> Path:
    """Folder of the installed application: the exe's folder when frozen, else the repository root.

    Files shipped next to the application (e.g. ``data/hist`` for backtests) are found here;
    unlike :func:`repo_root` this does not depend on PyInstaller's ``_internal`` layout.
    """
    if is_frozen():
        return Path(sys.executable).resolve().parent
    return repo_root()


def code_identity() -> str:
    """Identity of the running code for reproducibility manifests (backtests).

    Frozen: SHA-256 of the executable's bundle archive (every compiled module is in it);
    development: ``""`` - callers hash the source tree / use git.
    """
    if not is_frozen():
        return ""
    import hashlib

    h = hashlib.sha256()
    exe = Path(sys.executable).resolve()
    base = Path(getattr(sys, "_MEIPASS", exe.parent))
    for p in (exe, base / "base_library.zip"):
        if p.is_file():
            with open(p, "rb") as f:
                for chunk in iter(lambda: f.read(1 << 20), b""):
                    h.update(chunk)
    return h.hexdigest()


def bundled_config_dir() -> Path:
    """Default configuration shipped with the package (copied to <home>/config on first start)."""
    if is_frozen():
        return Path(getattr(sys, "_MEIPASS", Path(sys.executable).parent)) / "config"
    return repo_root() / "config"


def _writable(path: Path) -> bool:
    try:
        path.mkdir(parents=True, exist_ok=True)
        probe = path / ".write_probe"
        probe.write_text("ok", encoding="utf-8")
        probe.unlink()
        return True
    except OSError:
        return False


def resolve_home(explicit: str | os.PathLike[str] | None = None) -> Path:
    if explicit:
        return Path(explicit).resolve()
    env = os.environ.get("FUTRISK_HOME")
    if env:
        return Path(env).resolve()
    if is_frozen():
        candidate = Path(sys.executable).parent / "var"
        if _writable(candidate):
            return candidate
        return Path(os.environ.get("LOCALAPPDATA", str(Path.home()))) / "futrisk"
    return repo_root() / "var"


@dataclass(frozen=True)
class HomeLayout:
    home: Path

    @property
    def config_dir(self) -> Path:
        return self.home / "config"

    @property
    def versions_dir(self) -> Path:
        return self.home / "config" / "versions"

    @property
    def active_json(self) -> Path:
        return self.home / "config" / "active.json"

    @property
    def db_dir(self) -> Path:
        return self.home / "db"

    @property
    def backup_dir(self) -> Path:
        return self.home / "backup"

    @property
    def ticks_raw_dir(self) -> Path:
        return self.home / "ticks" / "raw"

    @property
    def ticks_canonical_dir(self) -> Path:
        return self.home / "ticks" / "canonical"

    @property
    def ticks_wire_dir(self) -> Path:
        """Raw Sina HTTP responses archived for re-parsing (``ticks/wire/<td>/sina``)."""
        return self.home / "ticks" / "wire"

    @property
    def reports_dir(self) -> Path:
        return self.home / "reports"

    @property
    def backtests_dir(self) -> Path:
        return self.home / "backtests"

    @property
    def logs_dir(self) -> Path:
        return self.home / "logs"

    @property
    def secrets_dir(self) -> Path:
        return self.home / "secrets"

    def db_path(self, name: str) -> Path:
        """Path of a service database, e.g. ``db_path("risk-1")`` -> ``<home>/db/risk-1.db``."""
        return self.db_dir / f"{name}.db"

    @property
    def home_id_file(self) -> Path:
        return self.home / "home_id"

    def home_id(self) -> str:
        """Stable id of this home (created on first use).

        Processes of one home report it (``Pong.home_id``, ``ServiceList.home_id``, api-gateway
        ``/health``) so that the CLI and the supervisor never act on another home's processes that
        happen to own the same ports.

        ``FUTRISK_HOME_ID`` overrides it: a deployment whose services run with separate home
        volumes (docker compose, one container per service) sets the same id everywhere.
        """
        env = os.environ.get("FUTRISK_HOME_ID", "").strip()
        if env:
            return env
        f = self.home_id_file
        try:
            value = f.read_text(encoding="utf-8").strip()
            if value:
                return value
        except FileNotFoundError:
            pass
        self.home.mkdir(parents=True, exist_ok=True)
        value = uuid.uuid4().hex
        tmp = f.with_suffix(".tmp")
        tmp.write_text(value, encoding="utf-8")
        try:
            os.replace(tmp, f)
        except OSError:  # a concurrent first start won the race: use its id
            tmp.unlink(missing_ok=True)
        return f.read_text(encoding="utf-8").strip()

    def ensure(self) -> HomeLayout:
        for p in (
            self.config_dir,
            self.versions_dir,
            self.db_dir,
            self.backup_dir,
            self.ticks_raw_dir,
            self.ticks_canonical_dir,
            self.reports_dir,
            self.backtests_dir,
            self.logs_dir,
            self.secrets_dir,
        ):
            p.mkdir(parents=True, exist_ok=True)
        return self
