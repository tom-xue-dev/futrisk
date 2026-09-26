"""clearing-worker commands (02 §6.5, §4.7, §4.11).

State-changing commands are idempotent by ``command_id`` through
:meth:`AsyncServiceRuntime.idempotent`: the duplicate check, the business
writes and the ``processed_commands`` record run in ONE writer job, and a
concurrent duplicate waits for the first. A duplicate returns the first reply
with ``duplicate=true``; the same key with other content gets ``CONFLICT``.
They are refused with ``UNAVAILABLE`` until the service is ready (market clock
known, runs recovered); queries are served from the start (risk-engine shards
fetch ``settle.sod_snapshot`` early).

============================  ==========================  ================================================
command                       caller / role               effect
============================  ==========================  ================================================
``settle.run``                api-gateway                 start/continue the saga; ``force_new_version``
``settle.import_official``    api-gateway, ADMIN          official prices; says which version they apply
                                                          to; WARN when a settled price differs
``settle.seed_sod``           api-gateway, ADMIN          new SEED snapshot version, pushed to all shards
``settle.override_price``     api-gateway, RISK_MANAGER+  manual price (applies before PRICES_FIXED)
``settle.query``              any                         ``SettlementResult`` of a version
``settle.sod_snapshot``       any (risk-engine)           snapshot by version, or latest pushed
``settle.status``             any                         latest ``EodPhaseEvent`` of a day (+ warnings)
``report.list`` / ``.get``    any                         ``reports`` index
``backtest.*``                api-gateway (run)           backtest jobs (:mod:`.backtests`)
============================  ==========================  ================================================

``settle.seed_sod`` guards (02 §6.5, §8.4):

* ``CONFLICT`` while the day has an EOD run that is neither FAILED nor DONE
  (the run pinned its SOD version at BEGIN; correct afterwards and re-settle);
* ``CONFLICT`` for a day before the newest SOD day, and for a day after the
  newest SOD day while that day is not settled (it would be skipped) unless
  the request carries ``force_skip: true`` (not a field of 02 ``SeedSodRequest``
  yet: read when present);
* ``BAD_REQUEST`` when a lot's ``basis_price`` differs from the snapshot's
  ``pre_settle`` of its contract (yesterday's lots are valued at the previous
  settlement price), checked on the merged snapshot (carried-over accounts too).
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from loguru import logger
from sqlalchemy import Connection

from futrisk.bus.rpc_common import RequestContext
from futrisk.common.clock import parse_td, td_str
from futrisk.common.decimals import D, dec_str, dec_str_strip, q_money
from futrisk.messages import codes
from futrisk.messages.base import Msg
from futrisk.messages.enums import EodPhase, Role, Rounding, SettlePriceSource
from futrisk.messages.envelope import Reply, err_reply, ok_reply
from futrisk.messages.eod import (
    BacktestList,
    BacktestListRequest,
    BacktestRunRequest,
    BacktestStatusRequest,
    EodPhaseEvent,
    EodStatusRequest,
    ImportAck,
    OfficialPriceImport,
    PriceOverride,
    ReportGetRequest,
    ReportList,
    ReportListRequest,
    SeedSodRequest,
    SettlementPriceView,
    SettleQuery,
    SettleRunAck,
    SettleRunRequest,
    SodAccount,
    SodLot,
    SodRequest,
    SodSnapshot,
)
from futrisk.services.clearing import eodwarn, store
from futrisk.services.clearing.prices import price_changes
from futrisk.services.clearing.schema import SodSource
from futrisk.services.clearing.store import EodRun, phase_index

if TYPE_CHECKING:
    from futrisk.services.clearing.service import ClearingService

_BULKY_STEP_KEYS = frozenset(
    {"prices", "input_pre_settle", "skipped", "digests", "pre_settle_fallback_prices", "config_hash"}
)
_RISK_ROLES = frozenset({Role.RISK_MANAGER, Role.ADMIN})


class BadRequest(ValueError):
    """Invalid request content (``BAD_REQUEST``)."""


def target_version(conn: Connection, trading_day: str) -> tuple[int, str]:
    """Settlement version a manual/official price applies to, and the sentence saying so (02 §6.5)."""
    latest = store.latest_run(conn, trading_day)
    next_version = store.max_run_version(conn, trading_day) + 1
    if latest is None or latest.phase == EodPhase.FAILED:
        return next_version, f"applies to the next run of {trading_day} (v{next_version})"
    if latest.in_progress and phase_index(latest.phase) < phase_index(EodPhase.PRICES_FIXED):
        return latest.version, f"applies to the run in progress (v{latest.version})"
    return next_version, (
        f"{trading_day} v{latest.version} is past PRICES_FIXED ({latest.phase.value}); "
        f"re-settle with settle.run force_new_version to use it (v{next_version})"
    )


class ClearingCommands:
    """Handlers of the clearing-worker commands, bound to a :class:`ClearingService`."""

    def __init__(self, svc: ClearingService) -> None:
        self._svc = svc

    def register(self) -> None:
        rt = self._svc.rt
        handlers: dict[str, tuple[Callable[..., Any], type[Msg]]] = {
            "settle.run": (self.settle_run, SettleRunRequest),
            "settle.import_official": (self.import_official, OfficialPriceImport),
            "settle.seed_sod": (self.seed_sod, SeedSodRequest),
            "settle.override_price": (self.override_price, PriceOverride),
            "settle.query": (self.settle_query, SettleQuery),
            "settle.sod_snapshot": (self.sod_snapshot, SodRequest),
            "settle.status": (self.settle_status, EodStatusRequest),
            "report.list": (self.report_list, ReportListRequest),
            "report.get": (self.report_get, ReportGetRequest),
            "backtest.run": (self.backtest_run, BacktestRunRequest),
            "backtest.status": (self.backtest_status, BacktestStatusRequest),
            "backtest.list": (self.backtest_list, BacktestListRequest),
        }
        for command, (fn, ptype) in handlers.items():
            rt.add_handler(command, fn, payload_type=ptype)

    # ------------------------------------------------------------- helpers
    @property
    def _now(self) -> int:
        return self._svc.rt.market_clock.now_ns()

    def _valid_day(self, td: str) -> bool:
        try:
            return len(td) == 8 and self._svc.rt.config.calendar.is_trading_day(parse_td(td))
        except (ValueError, IndexError):
            return False

    def _record_day(self, td: str) -> str:
        """``processed_commands.trading_day`` (the request's day when valid, else the current one)."""
        return td if len(td) == 8 and td.isdigit() else self._svc.current_trading_day()

    def _not_ready(self) -> Reply | None:
        if self._svc.ready.is_set():
            return None
        return err_reply(codes.UNAVAILABLE, "clearing-worker is starting (waiting for the market clock); retry later")

    async def _once(self, cmd: str, req: Any, ctx: RequestContext, trading_day: str, handler: Callable[[Connection], Reply]) -> Reply:
        """``rt.idempotent`` for this service's database (L10)."""
        svc = self._svc
        return await svc.rt.idempotent(
            svc.db,
            cmd,
            ctx.requester,
            req.command_id,
            req,
            handler,
            trading_day=self._record_day(trading_day),
            received_ns=ctx.received_ns,
        )

    # ---------------------------------------------------------- settle.run
    async def settle_run(self, cmd: str, req: SettleRunRequest, ctx: RequestContext) -> Reply:
        busy = self._not_ready()
        if busy is not None:
            return busy
        saga = self._svc.saga
        now = self._now
        created: list[EodRun] = []

        def handler(conn: Connection) -> Reply:
            decision = saga.decide_run_tx(
                conn,
                req.trading_day,
                force_new=req.force_new_version,
                triggered_by=req.actor or ctx.requester,
                now_ns=now,
            )
            if not decision.ok or decision.run is None:
                return err_reply(decision.code, decision.message)
            run = decision.run
            created.append(run)
            ack = SettleRunAck(trading_day=run.trading_day, version=run.version, phase=run.phase)
            return ok_reply(ack, message=decision.message)

        reply = await self._once(cmd, req, ctx, req.trading_day, handler)
        for run in created:
            saga.spawn(run)
            logger.info("settle.run", trading_day=run.trading_day, version=run.version, phase=run.phase.value)
            if run.phase == EodPhase.DONE and run.log(EodPhase.REPORTED).get("errors"):
                # 02 §8.4 第 7 步：报表生成失败只记 WARN，可以稍后补生成（同一版本同时只补生成一次）
                self._svc.regenerate_reports(run.trading_day, run.version)
        return reply

    # ------------------------------------------------ settle.import_official
    async def import_official(self, cmd: str, req: OfficialPriceImport, ctx: RequestContext) -> Reply:
        busy = self._not_ready()
        if busy is not None:
            return busy
        if req.role != Role.ADMIN:
            return err_reply(codes.FORBIDDEN, "settle.import_official requires ADMIN")
        if not self._valid_day(req.trading_day):
            return err_reply(codes.BAD_REQUEST, f"{req.trading_day} is not a trading day")
        registry = self._svc.registry()
        prices: list[tuple[str, str]] = []
        seen: set[str] = set()
        try:
            if not req.prices:
                raise BadRequest("prices must not be empty")
            for p in req.prices:
                if p.instrument_id in seen:
                    raise BadRequest(f"{p.instrument_id} appears twice")
                seen.add(p.instrument_id)
                if not registry.has(p.instrument_id):
                    raise BadRequest(f"unknown instrument {p.instrument_id}")
                px = D(p.price)
                if not px.is_finite() or px <= 0:
                    raise BadRequest(f"{p.instrument_id}: price must be positive")
                prices.append((p.instrument_id, dec_str_strip(px)))
        except (BadRequest, ValueError, ArithmeticError) as exc:
            return err_reply(codes.BAD_REQUEST, str(exc))
        now = self._now
        td = req.trading_day
        rt = self._svc.rt
        warnings: list[str] = []

        def handler(conn: Connection) -> Reply:
            _version, applies = target_version(conn, td)
            effective = store.effective_version(conn, td)
            imported = dict(prices)
            if effective is not None:
                used = {v.instrument_id: v.price for v in store.load_prices(conn, td, effective)}
                changes = price_changes(imported, used)
                for inst, (old, new) in changes.items():
                    warnings.append(f"{inst}: settled v{effective} used {old}, official {new}")
                run = store.get_run(conn, td, effective)
                if changes and run is not None:
                    eodwarn.warn_tx(
                        conn,
                        rt,
                        run,
                        eodwarn.OFFICIAL_PRICE_DIFF,
                        "official prices differ from the settlement in use - " + "; ".join(warnings)
                        + " (re-settle with settle.run force_new_version)",
                        now_ns=now,
                        extra={
                            "import_id": req.command_id,
                            "instruments": {inst: {"used": old, "official": new} for inst, (old, new) in changes.items()},
                        },
                    )
            else:
                changes = price_changes(imported, store.latest_official(conn, td))
            store.insert_official(
                conn,
                trading_day=td,
                prices=prices,
                import_id=req.command_id,
                source=req.source,
                actor=req.actor,
                imported_ns=now,
            )
            message = applies
            if warnings:
                message = (
                    "WARN: official prices differ from the settlement in use - "
                    + "; ".join(warnings)
                    + f" (re-settle with settle.run force_new_version); the import {applies}"
                )
            return ok_reply(ImportAck(inserted=len(prices), changed=len(changes)), message=message)

        async with self._svc.saga.price_lock(td):  # M-4: same lock as EOD step 4's read-decide-commit
            reply = await self._once(cmd, req, ctx, td, handler)
        if warnings and not reply.duplicate:
            logger.warning("settle.official_price_differs", trading_day=td, details=warnings)
        return reply

    # ----------------------------------------------------- settle.seed_sod
    def _validate_seed(self, req: SeedSodRequest) -> tuple[list[SodAccount], dict[str, str]]:
        cfg = self._svc.rt.config
        registry = self._svc.registry()
        if not req.accounts:
            raise BadRequest("accounts must not be empty")
        if not req.reason.strip():
            raise BadRequest("reason must not be empty")
        accounts: list[SodAccount] = []
        seen: set[str] = set()
        prices = {inst: dec_str_strip(D(px)) for inst, px in req.pre_settle.items()}
        for inst, px in prices.items():
            if not registry.has(inst):
                raise BadRequest(f"pre_settle: unknown instrument {inst}")
            if D(px) <= 0:
                raise BadRequest(f"pre_settle {inst}: price must be positive")
        for a in req.accounts:
            if a.account_id in seen:
                raise BadRequest(f"account {a.account_id} appears twice")
            seen.add(a.account_id)
            if a.account_id not in cfg.accounts.accounts:
                raise BadRequest(f"unknown account {a.account_id} (accounts.yaml)")
            lots: list[SodLot] = []
            keys: set[tuple[str, str, int]] = set()
            for lot in a.lots:
                if not registry.has(lot.instrument_id):
                    raise BadRequest(f"{a.account_id}: unknown instrument {lot.instrument_id}")
                if lot.qty <= 0:
                    raise BadRequest(f"{a.account_id} {lot.instrument_id}: qty must be positive")
                if len(lot.open_td) != 8 or lot.open_td >= req.trading_day:
                    raise BadRequest(f"{a.account_id} {lot.instrument_id}: open_td must be before {req.trading_day}")
                key = (lot.instrument_id, lot.direction.value, lot.lot_no)
                if key in keys:
                    raise BadRequest(f"{a.account_id}: duplicate lot {key}")
                keys.add(key)
                open_px, basis = D(lot.open_price), D(lot.basis_price)
                if open_px <= 0 or basis <= 0:
                    raise BadRequest(f"{a.account_id} {lot.instrument_id}: prices must be positive")
                lots.append(
                    SodLot(
                        instrument_id=lot.instrument_id,
                        direction=lot.direction,
                        lot_no=lot.lot_no,
                        open_td=lot.open_td,
                        open_ts_ns=lot.open_ts_ns,
                        open_price=dec_str_strip(open_px),
                        qty=lot.qty,
                        basis_price=dec_str_strip(basis),
                    )
                )
                prices.setdefault(lot.instrument_id, dec_str_strip(basis))
            accounts.append(
                SodAccount(
                    account_id=a.account_id,
                    pre_balance=dec_str(q_money(D(a.pre_balance))),
                    initial_capital=dec_str(q_money(D(a.initial_capital))),
                    cum_net_deposit=dec_str(q_money(D(a.cum_net_deposit))),
                    hwm_equity=dec_str(q_money(D(a.hwm_equity))),
                    lots=lots,
                )
            )
        return accounts, prices

    async def seed_sod(self, cmd: str, req: SeedSodRequest, ctx: RequestContext) -> Reply:
        busy = self._not_ready()
        if busy is not None:
            return busy
        if req.role != Role.ADMIN:
            return err_reply(codes.FORBIDDEN, "settle.seed_sod requires ADMIN")
        if not self._valid_day(req.trading_day):
            return err_reply(codes.BAD_REQUEST, f"{req.trading_day} is not a trading day")
        try:
            accounts, prices = self._validate_seed(req)
        except (BadRequest, ValueError, ArithmeticError) as exc:
            return err_reply(codes.BAD_REQUEST, str(exc))
        now = self._now
        td = req.trading_day
        force_skip = bool(getattr(req, "force_skip", False))
        seeded: list[SodSnapshot] = []

        def handler(conn: Connection) -> Reply:
            run = store.latest_run(conn, td)
            if run is not None and run.phase not in (EodPhase.FAILED, EodPhase.DONE):
                return err_reply(
                    codes.CONFLICT,
                    f"EOD run in progress: {td} v{run.version} ({run.phase.value}) uses SOD "
                    f"v{run.log(EodPhase.BEGIN).get('sod_version')}; correct the SOD after it finished "
                    "and re-settle with settle.run force_new_version",
                )
            latest_day = store.latest_sod_day(conn)
            if latest_day is not None and latest_day > td:
                return err_reply(codes.CONFLICT, f"a SOD snapshot for the later trading day {latest_day} exists")
            if latest_day is not None and td > latest_day and store.effective_version(conn, latest_day) is None:
                if not force_skip:
                    return err_reply(
                        codes.CONFLICT,
                        f"the newest SOD day {latest_day} is not settled; seeding the later day {td} would skip it "
                        f"(settle {latest_day} first)",
                    )
                logger.warning("settle.seed_sod_skips_days", newest=latest_day, seeded=td)
            merged = {a.account_id: a for a in accounts}
            pre = dict(prices)
            current = store.latest_sod_version(conn, td)
            if current is not None:  # 更正：未给出的账户沿用当前版本
                prev = store.load_sod(conn, td, current.version)
                if prev is not None:
                    for a in prev.accounts:
                        merged.setdefault(a.account_id, a)
                    for inst, px in prev.pre_settle.items():
                        pre.setdefault(inst, px)
            bad = [
                f"{a.account_id} {lot.instrument_id} lot {lot.lot_no}: basis {lot.basis_price} != pre_settle "
                f"{pre.get(lot.instrument_id)}"
                for a in merged.values()
                for lot in a.lots
                if lot.instrument_id not in pre or D(lot.basis_price) != D(pre[lot.instrument_id])
            ]
            if bad:  # M-6：昨仓按昨结算价计价，basis_price 必须等于快照的 pre_settle
                return err_reply(
                    codes.BAD_REQUEST,
                    "lot basis_price must equal the snapshot's pre_settle of its contract: " + "; ".join(bad[:10]),
                )
            version = store.next_sod_version(conn, td, seed=True)
            snap = SodSnapshot(
                trading_day=td,
                version=version,
                from_settlement=None,
                accounts=[merged[k] for k in sorted(merged)],
                pre_settle=dict(sorted(pre.items())),
            )
            store.save_sod(conn, snap, source=SodSource.SEED, created_ns=now, reason=req.reason, actor=req.actor)
            seeded.append(snap)
            return ok_reply(snap, message=f"seeded {td} v{version}")

        reply = await self._once(cmd, req, ctx, td, handler)
        for snap in seeded:
            logger.info("settle.seed_sod", trading_day=td, version=snap.version, accounts=len(snap.accounts))
            self._svc.push_seed(td, snap.version)
        return reply

    # ------------------------------------------------ settle.override_price
    async def override_price(self, cmd: str, req: PriceOverride, ctx: RequestContext) -> Reply:
        busy = self._not_ready()
        if busy is not None:
            return busy
        if req.role not in _RISK_ROLES:
            return err_reply(codes.FORBIDDEN, "settle.override_price requires RISK_MANAGER or ADMIN")
        if not self._valid_day(req.trading_day):
            return err_reply(codes.BAD_REQUEST, f"{req.trading_day} is not a trading day")
        registry = self._svc.registry()
        if not registry.has(req.instrument_id):
            return err_reply(codes.BAD_REQUEST, f"unknown instrument {req.instrument_id}")
        if not req.reason.strip():
            return err_reply(codes.BAD_REQUEST, "reason must not be empty")
        try:
            price = D(req.price)
            if not price.is_finite() or price <= 0:
                raise BadRequest("price must be positive")
            if not registry.on_tick(req.instrument_id, price):
                raise BadRequest(f"{req.price} is not on the tick grid of {req.instrument_id}")
        except (BadRequest, ValueError, ArithmeticError) as exc:
            return err_reply(codes.BAD_REQUEST, str(exc))
        now = self._now
        td = req.trading_day

        def handler(conn: Connection) -> Reply:
            target, message = target_version(conn, td)
            store.insert_override(
                conn,
                override_id=req.command_id,
                trading_day=td,
                instrument_id=req.instrument_id,
                price=dec_str_strip(price),
                reason=req.reason,
                actor=req.actor,
                created_ns=now,
            )
            view = SettlementPriceView(
                trading_day=td,
                instrument_id=req.instrument_id,
                price=dec_str_strip(price),
                source=SettlePriceSource.MANUAL,
                method=registry.settle_method(req.instrument_id),
                rounding=Rounding.NONE,
                version=target,
                operator=req.actor,
                reason=req.reason,
            )
            return ok_reply(view, message=message)

        async with self._svc.saga.price_lock(td):
            return await self._once(cmd, req, ctx, td, handler)

    # ------------------------------------------------------------- queries
    async def settle_query(self, cmd: str, req: SettleQuery, ctx: RequestContext) -> Reply:
        def q(c: Connection) -> Any:
            version = req.version if req.version is not None else store.effective_version(c, req.trading_day)
            if version is None:
                return None
            return store.load_settlement(c, req.trading_day, version, account_id=req.account_id)

        result = await self._svc.db.run_read(q)
        if result is None:
            return err_reply(codes.NOT_FOUND, f"no settlement for {req.trading_day}")
        return ok_reply(result)

    async def sod_snapshot(self, cmd: str, req: SodRequest, ctx: RequestContext) -> Reply:
        """02 §6.5: a given version (pushed or not); no version -> the latest pushed one; else NOT_FOUND."""

        def q(c: Connection) -> SodSnapshot | None:
            if req.version is not None:
                return store.load_sod(c, req.trading_day, req.version, req.account_ids)
            row = store.latest_pushed_version(c, req.trading_day)
            if row is None:
                return None
            return store.load_sod(c, req.trading_day, row.version, req.account_ids)

        snap = await self._svc.db.run_read(q)
        if snap is None:
            which = f"v{req.version}" if req.version is not None else "pushed"
            return err_reply(codes.NOT_FOUND, f"no {which} SOD snapshot for {req.trading_day}")
        return ok_reply(snap)

    async def settle_status(self, cmd: str, req: EodStatusRequest, ctx: RequestContext) -> Reply:
        run = await self._svc.db.run_read(lambda c: store.latest_run(c, req.trading_day))
        if run is None:
            return err_reply(codes.NOT_FOUND, f"no EOD run for {req.trading_day}")
        detail = {k: v for k, v in run.log(run.phase).items() if k not in _BULKY_STEP_KEYS}
        warns = [w for w in run.step_log.get(store.WARNINGS_KEY) or [] if isinstance(w, dict)]
        if warns:
            detail["warning_count"] = len(warns)
            detail["warnings"] = [{"kind": w.get("kind"), "message": w.get("message")} for w in warns[-10:]]
        return ok_reply(
            EodPhaseEvent(
                trading_day=run.trading_day,
                version=run.version,
                phase=run.phase,
                next_trading_day=run.next_trading_day,
                detail=detail,
                error=run.error,
                ts_ns=run.updated_ns,
            )
        )

    async def report_list(self, cmd: str, req: ReportListRequest, ctx: RequestContext) -> Reply:
        items = await self._svc.db.run_read(lambda c: store.list_reports(c, req.trading_day))
        return ok_reply(ReportList(items=items))

    async def report_get(self, cmd: str, req: ReportGetRequest, ctx: RequestContext) -> Reply:
        rep = await self._svc.db.run_read(lambda c: store.get_report(c, req.report_id))
        if rep is None:
            return err_reply(codes.NOT_FOUND, f"no report {req.report_id}")
        return ok_reply(rep)

    # ------------------------------------------------------------ backtests
    async def backtest_run(self, cmd: str, req: BacktestRunRequest, ctx: RequestContext) -> Reply:
        busy = self._not_ready()
        if busy is not None:
            return busy
        return await self._svc.backtests.submit(req, requester=ctx.requester, received_ns=ctx.received_ns)

    async def backtest_status(self, cmd: str, req: BacktestStatusRequest, ctx: RequestContext) -> Reply:
        st = await self._svc.db.run_read(lambda c: store.get_backtest(c, req.run_id))
        if st is None:
            return err_reply(codes.NOT_FOUND, f"no backtest {req.run_id}")
        return ok_reply(st)

    async def backtest_list(self, cmd: str, req: BacktestListRequest, ctx: RequestContext) -> Reply:
        items = await self._svc.db.run_read(store.list_backtests)
        return ok_reply(BacktestList(items=items))


def prev_trading_day(svc: ClearingService, td: str) -> str:
    """Previous trading day of ``td`` (the ``eod.rollover`` ``trading_day`` of a seed push)."""
    try:
        return td_str(svc.rt.config.calendar.prev_trading_day(parse_td(td)))
    except LookupError:
        return td


__all__ = ["BadRequest", "ClearingCommands", "prev_trading_day", "target_version"]
