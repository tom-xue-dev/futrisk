"""Acceptance run of the whole system (task.md 验收场景 + 行情容错 + 盘后清算), 8 real processes.

One system is started for the module with ``futrisk demo --scenario breaker --headless`` in a
throw-away home and exercised in order:

1. QS-03 breaker: ACC3 (10M) crosses the daily-loss levels -2% ALERT -> -2.5% RESTRICT -> -3%
   CIRCUIT_BREAK (latched); every alert is raised exactly once;
2. while HALTED an opening order is rejected REJECT_ACCOUNT_HALTED, a closing order fills;
3. ``futrisk kill risk-engine-1``: the supervisor restarts it, ACC3 is still HALTED, no alert
   is raised twice (alert_key idempotency) and opens are still rejected;
4. QS-05 feed outage: Feed A is cut for 20 s; Feed B carries the LIVE stream, Feed A resumes
   through probation, the gap is replayed as BACKFILL (GAP_FILLED, 0 duplicates) and md.tick
   never goes backwards or repeats a market state;
5. EOD: the market clock jumps to 15:29, the saga settles 20260928 on its own at 15:30, every
   reconciliation check passes and the daily report + statements are written; the shards roll
   to 20260929.

Run: ``.venv/Scripts/python -m pytest -m e2e tests/e2e -v`` (about 8-10 minutes).
"""

from __future__ import annotations

import collections
import threading
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
import zmq

from futrisk.messages.base import decode
from futrisk.messages.envelope import decode_envelope
from futrisk.messages.md import Tick

from .e2e_support import Api, cli, make_home, run_home, wait_until

pytestmark = [pytest.mark.e2e, pytest.mark.slow]

TD = "20260928"
START_AT = "2026-09-28T08:59:40"  # market time of a fresh demo: the day session opens 20 s later
BREAKER_LEVELS = ("daily_loss/warn", "daily_loss/restrict", "daily_loss/halt")


class System:
    def __init__(self, home: Path, svc: dict[str, Any]) -> None:
        self.home = home
        self.svc = svc
        self.api = Api.for_home(home, svc)
        self.notes: dict[str, Any] = {}


@pytest.fixture(scope="module")
def system() -> Iterator[System]:
    home = run_home()  # kept under var/e2e/ for post-mortems (logs, DBs, reports)
    print(f"e2e home: {home}")
    svc = make_home(home)
    run = cli(home, "demo", "--scenario", "breaker", "--headless", "--timeout", "240", "--start-at", START_AT,
              timeout=600)
    try:
        assert run.returncode == 0, f"futrisk demo failed:\n{run.stdout}\n{run.stderr}"
        assert "scenario breaker started" in run.stdout, run.stdout
        yield System(home, svc)
    finally:
        down = cli(home, "down", timeout=180)
        assert "stopped" in down.stdout or "nothing to stop" in down.stdout, down.stdout + down.stderr


def _alert_keys(events: list[dict[str, Any]]) -> list[str]:
    return [e["alert_key"] for e in events if e.get("kind") == "ALERT_RAISED" and e.get("alert_key")]


def test_breaker_levels_raise_once_each(system: System) -> None:
    api = system.api
    wait_until(lambda: "daily_loss/halt" in api.active_levels("ACC3"), 600, interval=2, what="ACC3 halt ACTIVE")
    acct = api.account("ACC3")
    assert acct["state"] == "HALTED"
    pnl = acct["metrics"]["daily_pnl_pct_init"]
    assert pnl <= -0.03, pnl
    events = api.risk_events("ACC3")
    keys = _alert_keys(events)
    assert len(keys) == len(set(keys)), f"an alert was raised twice: {keys}"
    raised = {"/".join(k.split("|")[1:3]) for k in keys}
    assert set(BREAKER_LEVELS) <= raised, raised
    # the daily-loss levels were crossed in order
    order = [("/".join(e["alert_key"].split("|")[1:3]), e["ts_ns"]) for e in events if e.get("kind") == "ALERT_RAISED"]
    ts = {lvl: t for lvl, t in order}
    assert ts["daily_loss/warn"] <= ts["daily_loss/restrict"] <= ts["daily_loss/halt"]
    system.notes["alert_keys"] = sorted(keys)


def test_opens_rejected_closes_fill_while_halted(system: System) -> None:
    api = system.api
    rej = api.place("ACC3", "rb2701", "BUY", "OPEN", 1)
    assert not rej.get("ok") and rej.get("code") == "REJECT_ACCOUNT_HALTED", rej
    held = next(p for p in api.data("/positions", account="ACC3")["items"] if p["instrument_id"] == "rb2701")
    assert (held.get("td_qty") or 0) + (held.get("yd_qty") or 0) > 0
    ok = api.place("ACC3", "rb2701", "SELL", "CLOSE_TODAY", 1)
    assert ok.get("ok"), ok
    coid = ok["data"]["client_order_id"]  # OrderAck
    wait_until(lambda: any(o["client_order_id"] == coid and o["status"] == "FILLED" for o in api.orders("ACC3")), 30,
               what="the closing order to fill")
    by_code = collections.Counter(o.get("reject_code") for o in api.orders("ACC3") if o.get("offset") == "OPEN")
    assert by_code["REJECT_ACCOUNT_HALTED"] >= 1


def test_halt_survives_risk_engine_kill(system: System) -> None:
    api = system.api
    before = _alert_keys(api.risk_events("ACC3"))
    killed = cli(system.home, "kill", "risk-engine-1")
    assert killed.returncode == 0, killed.stdout + killed.stderr

    def restarted() -> bool:
        items = api.data("/system/services")["items"]
        it = next(i for i in items if i["instance"] == "risk-engine-1")
        return it.get("restarts", 0) >= 1 and it.get("state") == "RUNNING" and it.get("heartbeat_state") == "RUNNING"

    wait_until(restarted, 90, interval=2, what="risk-engine-1 restarted and RUNNING")
    wait_until(lambda: "daily_loss/halt" in api.active_levels("ACC3"), 60, interval=1, what="ACC3 halt restored")
    assert api.account("ACC3")["state"] == "HALTED"
    rej = api.place("ACC3", "hc2701", "BUY", "OPEN", 1)
    assert rej.get("code") == "REJECT_ACCOUNT_HALTED", rej
    after = _alert_keys(api.risk_events("ACC3"))
    assert len(after) == len(set(after)), f"duplicate alerts after the restart: {after}"
    assert set(before) <= set(after)
    assert not [k for k in after if "|daily_loss|" in k and not k.endswith("|1")], after  # still episode 1


class TickTap:
    """Collects md.tick envelopes from md-gateway's PUB socket on a thread."""

    def __init__(self, endpoint: str) -> None:
        self.ticks: list[tuple[float, Tick]] = []
        self._stop = threading.Event()
        self._ctx = zmq.Context()
        self._sock = self._ctx.socket(zmq.SUB)
        self._sock.setsockopt(zmq.RCVHWM, 0)
        self._sock.setsockopt(zmq.SUBSCRIBE, b"md.tick.")
        self._sock.setsockopt(zmq.RCVTIMEO, 200)
        self._sock.connect(endpoint)
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                _topic, data = self._sock.recv_multipart()
            except zmq.Again:
                continue
            env = decode_envelope(data)
            if env.type == "Tick":
                self.ticks.append((time.monotonic(), decode(env.payload, Tick)))

    def close(self) -> None:
        self._stop.set()
        self._thread.join(2)
        self._sock.close(0)
        self._ctx.term()


def test_feed_a_outage_fails_over_and_backfills_without_duplicates(system: System) -> None:
    api = system.api
    tap = TickTap(system.svc["instances"]["md-gateway"]["pub"])
    try:
        time.sleep(3)

        def start() -> dict[str, Any] | None:
            # the breaker scenario keeps running for ~330 s after its start (the halt comes earlier)
            r = api.post("/demo/scenario", {"name": "feed_outage"})
            if r.get("code") == "CONFLICT":
                return None
            assert r.get("ok"), r
            return r

        wait_until(start, 420, interval=5, what="the breaker scenario to end and feed_outage to start")
        t0 = time.monotonic()
        time.sleep(75)  # 20 s outage + resume/probation + the seq gap / duplicate / reorder faults
    finally:
        tap.close()
    ticks = tap.ticks
    assert ticks, "no md.tick received"
    # no LIVE state published twice or out of order, per instrument
    last: dict[str, tuple[str, int, int]] = {}
    for _, t in ticks:
        k = t.order_key
        prev = last.get(t.instrument_id)
        assert prev is None or k > prev, f"{t.instrument_id}: order_key {k} after {prev}"
        last[t.instrument_id] = k
    # Feed B kept the LIVE stream going during Feed A's outage (rb2701 ticks every 0.5 s in the sim)
    rb = [ts for ts, t in ticks if t.instrument_id == "rb2701" and t0 <= ts <= t0 + 20]
    assert rb and max(b - a for a, b in zip([t0, *rb], [*rb, t0 + 20], strict=True)) < 4.0
    events = api.data("/market/events")
    items = events["items"] if isinstance(events, dict) else events
    feed_a = [(e.get("from_state"), e.get("to_state")) for e in items
              if e.get("kind") == "SOURCE_STATE" and e.get("source") == "FEED_A"]
    assert ("TRUSTED", "EXCLUDED") in feed_a and ("PROBATION", "TRUSTED") in feed_a, feed_a
    filled = [e["gap"] for e in items if e.get("kind") == "GAP_FILLED" and e.get("gap")]
    assert filled and any(g.get("filled", 0) > 0 for g in filled), filled
    assert all(g.get("duplicates", 0) == 0 for g in filled), filled


def test_eod_settles_reconciles_and_reports(system: System) -> None:
    api = system.api
    jumped = api.post("/demo/clock", {"target": "AT", "at": "2026-09-28T15:29:00+08:00"})
    assert jumped.get("ok"), jumped

    def done() -> dict[str, Any] | None:
        st = api.get(f"/settlement/{TD}/status")
        data: dict[str, Any] | None = st.get("data") if st.get("ok") else None
        if data and data.get("phase") == "FAILED":
            raise AssertionError(f"EOD FAILED: {data}")
        return data if data and data.get("phase") == "DONE" else None

    wait_until(done, 180, interval=2, what="the EOD saga of 20260928 to finish")
    settled = api.data(f"/settlement/{TD}")
    accounts = settled["accounts"]
    assert {a["account_id"] for a in accounts} == {"ACC1", "ACC2", "ACC3", "ACC4", "ACC5"}
    assert all(a["checks_passed"] for a in accounts), [(a["account_id"], a["checks_passed"]) for a in accounts]
    assert all(c["status"] in ("PASS", "WARN") for c in settled["checks"]), [c for c in settled["checks"] if c["status"] not in ("PASS", "WARN")]
    acc3 = next(a for a in accounts if a["account_id"] == "ACC3")
    # the exchange's own P&L formula equals close + position P&L (01 §8.15 second check)
    assert abs(float(acc3["exch_formula_pnl"]) - float(acc3["close_pnl"]) - float(acc3["position_pnl"])) < 0.01
    reports = api.data("/reports", td=TD)
    kinds = collections.Counter(r.get("type") for r in (reports["items"] if isinstance(reports, dict) else reports))
    assert kinds.get("RISK_DAILY_HTML", 0) >= 1, kinds
    html = (system.home / "reports" / TD / "risk_daily.html").read_text(encoding="utf-8")
    assert "熔断" in html and "ACC3" in html
    wait_until(lambda: api.account("ACC3").get("trading_day") == "20260929", 60, what="the shards to roll to 20260929")
