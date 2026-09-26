"""The benchmark book: accounts, instruments, SOD positions and the runtime home of the multi-process benches.

The scenario of plan §10.4 is "50 accounts × 20 instruments": the default ``products.yaml`` lists 13
contracts, so seven more months of configured products are added (``EXTRA_INSTRUMENTS``). Every account
(``B01``..``B50``, shard 1, 10 M CNY) holds ``holdings`` instruments from SOD - by default 4, like the demo
accounts (1-6 products each), which gives every instrument ``accounts × holdings / instruments`` = 10
holders; ``holdings = instruments`` is the full matrix (every account holds every instrument). Positions
are small (≈ 2 % of equity per position, long and short alternating) so that no rule is breached and the
accounts stay NORMAL while prices move by a few ticks.

The market clock is the demo clock at :data:`MARKET_OPEN` (Monday 2026-09-28 09:35 Beijing): every
product is in its continuous session and the trading day is :data:`TD`.

Ports: every endpoint of the world gets a free loopback port picked at random in
:data:`PORT_RANGE` - below the OS's dynamic (ephemeral) range, so an outgoing connection of any process
(or a ZeroMQ reconnect that connects to itself) can never occupy the port of a service that restarts,
and far from the default ports of a running installation (7101-7602, 8000).
"""

from __future__ import annotations

import dataclasses
import random
import shutil
import socket
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml

from futrisk.common.clock import BJ
from futrisk.common.config import FullConfig, ensure_home_config, load_active_config, load_default_config
from futrisk.common.decimals import D
from futrisk.common.paths import HomeLayout, bundled_config_dir
from futrisk.messages.enums import PosDirection
from futrisk.messages.eod import SodAccount, SodLot, SodSnapshot

#: listening ports of the bench worlds: below the dynamic port range (Windows 49152-65535, Linux 32768-60999)
PORT_RANGE = (20000, 32000)
_rng = random.Random()

TD = "20260928"
PREV_TD = "20260924"  # 2026-09-25 is the Mid-Autumn holiday
MARKET_OPEN = datetime(2026, 9, 28, 9, 35, tzinfo=BJ)
SHARD = 1
RE_INSTANCE = f"risk-engine-{SHARD}"
CAPITAL = "10000000"

#: the 20 instruments (pre-settlement price), products interleaved so that consecutive holdings differ
INSTRUMENTS: dict[str, float] = {
    "rb2701": 3100.0, "hc2701": 3300.0, "cu2611": 80000.0, "au2612": 850.0, "i2701": 800.0,
    "j2701": 1800.0, "m2701": 3000.0, "IF2612": 4500.0, "IH2612": 2800.0, "IC2612": 6000.0,
    "T2612": 108.0, "rb2705": 3120.0, "hc2705": 3320.0, "cu2612": 80200.0, "au2702": 852.0,
    "i2705": 790.0, "j2705": 1790.0, "m2705": 3050.0, "IM2612": 6500.0, "rb2610": 3080.0,
}
#: contracts added to products.yaml for the bench: (instrument, product, last trade day, delivery month)
EXTRA_INSTRUMENTS: tuple[tuple[str, str, str, str], ...] = (
    ("hc2705", "hc", "20270517", "202705"),
    ("cu2612", "cu", "20261215", "202612"),
    ("au2702", "au", "20270222", "202702"),
    ("i2705", "i", "20270517", "202705"),
    ("j2705", "j", "20270517", "202705"),
    ("m2705", "m", "20270517", "202705"),
    ("IM2612", "IM", "20261218", "202612"),
)
#: SOD lots per position by product (≈ 1-2 % of 10 M equity; one lot for the large contracts)
LOT_QTY: dict[str, int] = {"rb": 6, "hc": 6, "cu": 1, "au": 1, "i": 2, "j": 1, "m": 6, "IF": 1, "IH": 1, "IC": 1,
                           "IM": 1, "T": 1}


@dataclass(frozen=True)
class WorldSpec:
    """Size of the benchmark book and bus timings of the multi-process world."""

    accounts: int = 50
    holdings: int = 4
    instruments: int = 20
    heartbeat_ms: int = 1000  # production default (services.yaml)
    peer_timeout_missed: int = 3
    md_idle_ms: int = 200  # fake md-gateway: one tick per instrument every md_idle_ms outside bursts
    tg_auto_fill: bool = True  # fake trade-gateway: fill every accepted order right after the RouteAck
    tg_fill_parts: int = 1  # ... in this many fills (qty split as evenly as possible)
    throttle_per_s: float = 1_000_000.0  # pre-trade token bucket: never the bottleneck of a benchmark

    def __post_init__(self) -> None:
        if not 1 <= self.instruments <= len(INSTRUMENTS):
            raise ValueError(f"instruments must be 1..{len(INSTRUMENTS)}")
        if not 1 <= self.holdings <= self.instruments:
            raise ValueError("holdings must be 1..instruments")
        if not 1 <= self.accounts <= 99:
            raise ValueError("accounts must be 1..99")

    def to_args(self) -> dict[str, Any]:
        return dataclasses.asdict(self)


def instrument_ids(spec: WorldSpec) -> list[str]:
    return list(INSTRUMENTS)[: spec.instruments]


def account_ids(spec: WorldSpec) -> list[str]:
    return [f"B{n:02d}" for n in range(1, spec.accounts + 1)]


def product_of(instrument_id: str) -> str:
    i = 0
    while i < len(instrument_id) and instrument_id[i].isalpha():
        i += 1
    return instrument_id[:i]


def book(spec: WorldSpec) -> dict[str, list[tuple[str, PosDirection, int]]]:
    """SOD positions: account -> [(instrument, direction, qty)] (``holdings`` consecutive instruments, rotating)."""
    insts = instrument_ids(spec)
    out: dict[str, list[tuple[str, PosDirection, int]]] = {}
    for n, acct in enumerate(account_ids(spec)):
        rows = []
        for j in range(spec.holdings):
            inst = insts[(n * spec.holdings + j) % len(insts)]
            d = PosDirection.LONG if (n + j) % 2 == 0 else PosDirection.SHORT
            rows.append((inst, d, LOT_QTY.get(product_of(inst), 1)))
        out[acct] = rows
    return out


def sod_snapshot(spec: WorldSpec, *, trading_day: str = TD, version: int = 1) -> SodSnapshot:
    accounts = []
    for acct, rows in book(spec).items():
        lots = [SodLot(instrument_id=inst, direction=d, lot_no=k + 1, open_td=PREV_TD, open_ts_ns=k + 1,
                       open_price=f"{INSTRUMENTS[inst]:g}", qty=qty, basis_price=f"{INSTRUMENTS[inst]:g}")
                for k, (inst, d, qty) in enumerate(rows)]
        cap = f"{D(CAPITAL):.2f}"
        accounts.append(SodAccount(account_id=acct, pre_balance=cap, initial_capital=cap, cum_net_deposit="0.00",
                                   hwm_equity=cap, lots=lots))
    return SodSnapshot(trading_day=trading_day, version=version, accounts=accounts,
                       pre_settle={i: f"{INSTRUMENTS[i]:g}" for i in instrument_ids(spec)})


def yd_positions(spec: WorldSpec) -> dict[tuple[str, str, PosDirection], int]:
    out: dict[tuple[str, str, PosDirection], int] = {}
    for acct, rows in book(spec).items():
        for inst, d, qty in rows:
            out[(acct, inst, d)] = out.get((acct, inst, d), 0) + qty
    return out


def free_port(taken: set[int] | None = None) -> int:
    """A free loopback TCP port in :data:`PORT_RANGE` (never an ephemeral port; see the module docstring)."""
    lo, hi = PORT_RANGE
    for _ in range(1000):
        port = _rng.randint(lo, hi)
        if taken is not None and port in taken:
            continue
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            if hasattr(socket, "SO_EXCLUSIVEADDRUSE"):  # Windows: fail if anybody holds the port in any way
                s.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
            try:
                s.bind(("127.0.0.1", port))
            except OSError:
                continue
        if taken is not None:
            taken.add(port)
        return port
    raise RuntimeError(f"no free port in {lo}-{hi}")


def with_ports(config: FullConfig, *, heartbeat_ms: int | None = None, peer_timeout_missed: int | None = None
               ) -> FullConfig:
    """``config`` with fresh :func:`free_port` endpoints for every instance that has a PUB / ROUTER and the given bus
    timings (like :func:`futrisk.runtime.testing.config_with_free_ports`, but outside the ephemeral range)."""
    services = config.services
    taken: set[int] = set()
    instances = {}
    for name, inst in services.instances.items():
        update: dict[str, str] = {}
        if inst.pub is not None:
            update["pub"] = f"tcp://127.0.0.1:{free_port(taken)}"
        if inst.router is not None:
            update["router"] = f"tcp://127.0.0.1:{free_port(taken)}"
        instances[name] = inst.model_copy(update=update)
    bus_update: dict[str, int] = {}
    if heartbeat_ms is not None:
        bus_update["heartbeat_ms"] = heartbeat_ms
    if peer_timeout_missed is not None:
        bus_update["peer_timeout_missed"] = peer_timeout_missed
    bus = services.bus.model_copy(update=bus_update) if bus_update else services.bus
    return dataclasses.replace(config, services=services.model_copy(update={"instances": instances, "bus": bus}))


def write_home(home: Path, spec: WorldSpec, *, base: FullConfig | None = None) -> FullConfig:
    """``<home>/config``: the bundled configuration with the bench book, free loopback ports and the bus timings of
    ``spec``; returns the committed (``active.json``) configuration every process of the world loads."""
    base = base or load_default_config()
    cfg_dir = home / "config"
    if cfg_dir.exists():
        shutil.rmtree(cfg_dir)
    shutil.copytree(bundled_config_dir(), cfg_dir)
    ports = with_ports(base, heartbeat_ms=spec.heartbeat_ms, peer_timeout_missed=spec.peer_timeout_missed)
    _dump(cfg_dir / "services.yaml", ports.services.model_dump(mode="json"))
    accounts = {a: {"name": f"bench {a}", "shard": SHARD, "initial_capital": CAPITAL,
                    "products": sorted({product_of(i) for i, _d, _q in rows})}
                for a, rows in book(spec).items()}
    _dump(cfg_dir / "accounts.yaml", {"accounts": accounts})
    products = yaml.safe_load((cfg_dir / "products.yaml").read_text(encoding="utf-8"))
    have = {i["instrument_id"] for i in products["instruments"]}
    for inst, prod, last, month in EXTRA_INSTRUMENTS:
        if inst not in have:
            products["instruments"].append({"instrument_id": inst, "product": prod, "last_trade_day": last,
                                            "delivery_month": month})
    _dump(cfg_dir / "products.yaml", products)
    rr = base.risk_rules.model_dump(mode="json")
    rr["pretrade"].update(throttle_rate_per_s=spec.throttle_per_s, throttle_burst=int(spec.throttle_per_s))
    _dump(cfg_dir / "risk_rules.yaml", rr)
    layout = HomeLayout(home)
    ensure_home_config(layout)
    return load_active_config(layout)


def _dump(path: Path, data: Any) -> None:
    path.write_text(yaml.safe_dump(data, allow_unicode=True, sort_keys=False), encoding="utf-8")


__all__ = [
    "CAPITAL",
    "EXTRA_INSTRUMENTS",
    "INSTRUMENTS",
    "MARKET_OPEN",
    "PORT_RANGE",
    "PREV_TD",
    "RE_INSTANCE",
    "SHARD",
    "TD",
    "WorldSpec",
    "account_ids",
    "book",
    "free_port",
    "instrument_ids",
    "product_of",
    "sod_snapshot",
    "with_ports",
    "write_home",
    "yd_positions",
]
