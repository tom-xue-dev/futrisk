"""Reproducibility manifest of a backtest run (01 §8.16: config hash, data hash, seed, git SHA).

``data_hash`` is the SHA-256 over ``"{relative path}:{sha256}"`` lines of every
data file the run read, sorted by path. The per-file hashes come from
``data/hist/manifest.json`` (written by the fetch script) and are verified
against the files on disk; a file whose content no longer matches its manifest
entry is hashed from disk and listed under ``data_manifest_mismatch``.
"""

from __future__ import annotations

import hashlib
import json
import platform
import subprocess
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from futrisk.common.paths import code_identity, repo_root


def git_sha(root: Path | None = None) -> str:
    """Current commit, or ``nogit`` without a repository / without commits (same rule as the clearing-worker).

    Frozen exe: there is no repository next to it; the code is identified by
    :func:`futrisk.common.paths.code_identity` (SHA-256 of the executable bundle) instead.
    """
    ident = code_identity()
    if ident:
        return ident
    try:
        out = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=str(root or repo_root()),
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return "nogit"
    sha = out.stdout.strip()
    return sha if out.returncode == 0 and len(sha) == 40 else "nogit"


def versions() -> dict[str, str]:
    import matplotlib
    import msgspec
    import numpy
    import pandas
    import pyarrow
    import pydantic

    from futrisk import __version__

    return {
        "python": sys.version.split()[0],
        "platform": platform.platform(),
        "futrisk": __version__,
        "numpy": numpy.__version__,
        "pandas": pandas.__version__,
        "pyarrow": pyarrow.__version__,
        "matplotlib": matplotlib.__version__,
        "msgspec": msgspec.__version__,
        "pydantic": pydantic.__version__,
    }


def data_hash(files: Mapping[str, str], data_dir: Path | None = None) -> tuple[str, dict[str, Any]]:
    """``(hash, info)`` for ``{relative path: sha256 of the file on disk}``; cross-checked with data/hist/manifest.json."""
    info: dict[str, Any] = {"files": dict(sorted(files.items())), "manifest_checked": 0, "data_manifest_mismatch": []}
    man: dict[str, Any] = {}
    if data_dir is not None and (data_dir / "manifest.json").exists():
        try:
            man = json.loads((data_dir / "manifest.json").read_text(encoding="utf-8")).get("files", {})
        except (OSError, ValueError):
            man = {}
    for rel, sha in files.items():
        entry = man.get(rel)
        if isinstance(entry, dict) and entry.get("sha256"):
            info["manifest_checked"] += 1
            if entry["sha256"] != sha:
                info["data_manifest_mismatch"].append(rel)
    text = "\n".join(f"{k}:{files[k]}" for k in sorted(files))
    return hashlib.sha256(text.encode("utf-8")).hexdigest(), info


def code_hash(root: Path | None = None) -> tuple[str, dict[str, str]]:
    """SHA-256 over every ``src/futrisk/**/*.py`` (``"{path}:{sha256}"`` lines) and the per-package hashes.

    Without a git commit (``git_sha = nogit``) this ties a run to the exact engine, clearing and backtest code.
    Frozen exe: the sources are compiled into the bundle, so the hash is
    :func:`futrisk.common.paths.code_identity` (per package: ``{"exe": identity}``).
    """
    ident = code_identity()
    if ident:
        return ident, {"exe": ident}
    base = (root or repo_root()) / "src" / "futrisk"
    files = {p.relative_to(base).as_posix(): file_sha256(p) for p in sorted(base.rglob("*.py")) if "__pycache__" not in p.parts}
    packages: dict[str, list[str]] = {}
    for rel, sha in files.items():
        packages.setdefault(rel.split("/", 1)[0] if "/" in rel else ".", []).append(f"{rel}:{sha}")
    nl = "\n"
    per = {k: hashlib.sha256(nl.join(v).encode("utf-8")).hexdigest() for k, v in sorted(packages.items())}
    total = hashlib.sha256(nl.join(f"{k}:{v}" for k, v in files.items()).encode("utf-8")).hexdigest()
    return total, per


def file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def json_sha256(obj: Any) -> str:
    text = json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


__all__ = ["code_hash", "data_hash", "file_sha256", "git_sha", "json_sha256", "versions"]
