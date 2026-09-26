# -*- mode: python ; coding: utf-8 -*-
"""PyInstaller spec of ``futrisk.exe``: one-folder console build (01 §7.1 D1).

One executable runs every service as a sub-command (``futrisk svc md-gateway``, ...);
``futrisk up`` (the supervisor) re-launches ``futrisk.exe svc <name>`` for each child.

Build from the repository root with the Poetry venv (or run ``scripts\\build_exe.ps1``,
which also builds the dashboard, runs smoke tests and writes the zip + SHA-256)::

    .venv\\Scripts\\python.exe -m PyInstaller packaging\\futrisk.spec --noconfirm --clean

Output ``dist\\futrisk\\``::

    futrisk.exe                         bootloader + PYZ (every compiled module) + version resource;
                                        runs Python in-process (one-folder), so Ctrl-Break and
                                        psutil see the real pid
    _internal\\                          sys._MEIPASS: Python runtime, extension modules, DLLs, data
        futrisk\\web_static\\dist\\        dashboard, served by api-gateway at /
        futrisk\\services\\clearing\\templates\\   Jinja2 report templates
        futrisk\\sim\\bots\\bots.yaml        strategy-sim defaults
        config\\                         bundled default config (paths.bundled_config_dir()),
                                        copied to <home>\\config at the first start
    data\\hist\\                          historical data for backtests (see DATA_HIST below)
    var\\                                the default <home>, created at the first start (not shipped)
"""

import re
import shutil
import sys
from pathlib import Path

from PyInstaller.utils.hooks import collect_submodules, copy_metadata

ROOT = Path(SPECPATH).resolve().parent  # SPECPATH (set by PyInstaller) = packaging/
PKG = ROOT / "src" / "futrisk"
ENTRY = ROOT / "packaging" / "futrisk_entry.py"

# single source of the version: src/futrisk/__init__.py (also pyproject.toml's version)
VERSION = re.search(r'__version__\s*=\s*"([^"]+)"', (PKG / "__init__.py").read_text(encoding="utf-8")).group(1)

WEB_DIST = PKG / "web_static" / "dist"
if not (WEB_DIST / "index.html").is_file():
    raise SystemExit(
        f"{WEB_DIST / 'index.html'} is missing: build the dashboard first (cd web; npm ci; npm run build) "
        "or run scripts\\build_exe.ps1"
    )

# ------------------------------------------------------------------ data files
# Every non-.py file the package reads at run time. Destinations mirror the package layout
# because the code finds them relative to its own module (__file__), which PyInstaller
# points into _internal\futrisk\...
datas = [
    # api_gateway.app.static_dir() = Path(futrisk.__file__).parent / "web_static/dist"
    (str(WEB_DIST), "futrisk/web_static/dist"),
    # clearing.reports.render reads importlib.resources.files("futrisk.services.clearing")
    # / "templates/risk_daily.html.j2"; frozen, that is _internal/futrisk/services/clearing.
    # clearing-worker refuses to start without it (checked by the `up` smoke test).
    (str(PKG / "services" / "clearing" / "templates"), "futrisk/services/clearing/templates"),
    # sim.bots.config.PACKAGED_CONFIG = Path(__file__).with_name("bots.yaml")
    (str(PKG / "sim" / "bots" / "bots.yaml"), "futrisk/sim/bots"),
    # the whole default config tree (yaml files, calendar/, scenarios/, backtest/):
    # paths.bundled_config_dir() is sys._MEIPASS/config when frozen
    (str(ROOT / "config"), "config"),
]
# `futrisk doctor` reports the tz database version with importlib.metadata.version("tzdata")
datas += copy_metadata("tzdata")

# ------------------------------------------------------------------ hidden imports
hiddenimports = [
    # main.SUBAPPS / main.TOPLEVEL and cli.svc_cmd.SERVICES import the CLI modules and the
    # services by name (importlib), and the rule registries (@register_metric/@register_action)
    # fill up as modules are imported: PyInstaller cannot see any of it, so take the package.
    *collect_submodules("futrisk"),
    # zoneinfo has no system tz database on Windows and falls back to the tzdata package,
    # which it loads through importlib.resources; naming it here makes the tzdata hook
    # bundle the zoneinfo files (Asia/Shanghai is used for every market time).
    "tzdata",
    # uvicorn.Config.load() imports loop / http / ws / lifespan implementations from strings
    # (loop="auto", http="auto" -> httptools or h11, ws="websockets-sansio", lifespan="off",
    # uvicorn.logging). The contrib hook already collects uvicorn; kept explicit on purpose.
    *collect_submodules("uvicorn"),
    # websockets exposes its API through lazy imports (websockets/__init__.py); Feed A of
    # sim-exchange and md-gateway's Feed A client use websockets.asyncio.
    *collect_submodules("websockets"),
    # report charts use matplotlib's Agg backend by name (matplotlib.use("Agg"))
    "matplotlib.backends.backend_agg",
]

excludes = [
    # dev dependency group: test and lint tooling is never imported by the application
    "pytest", "_pytest", "pytest_asyncio", "pytest_cov", "pytest_timeout", "hypothesis",
    "coverage", "mypy", "mypyc", "ruff",
    # dev-time data tools (scripts/fetch_hist.py) live in the separate .venv-data
    "akshare", "polars",
    # GUI toolkits and notebooks matplotlib/pandas could drag in: only Agg is used
    "tkinter", "_tkinter", "PyQt5", "PyQt6", "PySide2", "PySide6", "wx", "gi",
    "IPython", "ipykernel", "jupyter_client", "notebook",
    # build tooling (setuptools arrives as a dependency of PyInstaller itself)
    "setuptools", "pkg_resources", "_distutils_hack", "distutils", "pip",
]

a = Analysis(
    [str(ENTRY)],
    pathex=[str(ROOT / "src")],  # import futrisk from the source tree, not an installed copy
    binaries=[],  # libzmq (pyzmq.libs), arrow and numpy DLLs come from the library hooks
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=excludes,
    noarchive=False,
    optimize=0,  # keep docstrings and asserts: typer builds --help from docstrings
)
# Library development files nothing reads at run time: C/C++ headers, MSVC import libraries
# and Cython sources, collected mostly by the pyarrow hook (~13 MB).
DEV_FILES = (".h", ".hpp", ".lib", ".pyx", ".pxd", ".pxi", ".cc", ".cpp")
a.datas = [entry for entry in a.datas if not entry[0].lower().endswith(DEV_FILES)]
pyz = PYZ(a.pure)

# ------------------------------------------------------------------ version resource
version_info = None
if sys.platform == "win32":
    from PyInstaller.utils.win32.versioninfo import (
        FixedFileInfo, StringFileInfo, StringStruct, StringTable, VarFileInfo, VarStruct, VSVersionInfo,
    )

    nums = tuple((list(map(int, re.findall(r"\d+", VERSION)[:4])) + [0, 0, 0, 0])[:4])
    version_info = VSVersionInfo(
        ffi=FixedFileInfo(filevers=nums, prodvers=nums),
        kids=[
            StringFileInfo([
                StringTable("040904B0", [  # U.S. English, Unicode
                    StringStruct("CompanyName", "futrisk"),
                    StringStruct("FileDescription", "futrisk - multi-product futures risk control and clearing"),
                    StringStruct("FileVersion", VERSION),
                    StringStruct("InternalName", "futrisk"),
                    StringStruct("OriginalFilename", "futrisk.exe"),
                    StringStruct("ProductName", "futrisk"),
                    StringStruct("ProductVersion", VERSION),
                ]),
            ]),
            VarFileInfo([VarStruct("Translation", [0x0409, 1200])]),
        ],
    )

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,  # one-folder: binaries and data go to COLLECT
    name="futrisk",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,
    console=True,  # CLI + service processes: stdout/stderr, Ctrl-C, CTRL_BREAK from the supervisor
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    version=version_info,
    contents_directory="_internal",  # the PyInstaller 6 default (sys._MEIPASS)
)
coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=False,
    upx_exclude=[],
    name="futrisk",
)

# ------------------------------------------------------------------ data/hist (after COLLECT)
# Backtests read historical data from BacktestConfig.data_dir() = app_dir() / "data/hist", and
# futrisk.common.paths.app_dir() is the folder of futrisk.exe when frozen: the data ships next
# to the exe, where it can be refreshed without a rebuild. Runs after COLLECT has written
# dist\futrisk.
DATA_HIST = ROOT / "data" / "hist"
if DATA_HIST.is_dir():
    shutil.copytree(DATA_HIST, Path(coll.name) / "data" / "hist", dirs_exist_ok=True)
else:
    print(f"WARNING: {DATA_HIST} not found; the layer A backtest needs data\\hist next to futrisk.exe")
