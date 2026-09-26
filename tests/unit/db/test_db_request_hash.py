"""request_hash strips the time fields at the top level only (api-gateway review follow-up).

A ``/trades/import`` retried with the same Idempotency-Key but trades that differ only in their
timestamps must answer CONFLICT, not replay the first result; a retry with a fresh ``sent_ns`` /
``requested_ns`` on the request itself is still a duplicate."""

from __future__ import annotations

from dbtest_support import T0, DbFactory
from sqlalchemy import Connection

from futrisk.db import ProcessedCommands, request_hash
from futrisk.messages import codes, encode
from futrisk.messages.enums import CashType, Direction, Exchange, Offset, Role
from futrisk.messages.envelope import Reply, ok_reply
from futrisk.messages.orders import Trade, TradeImport
from futrisk.messages.risk import CashMoveRequest


def _trade(trade_id: str, *, received_ns: int = 0, trade_ts_ns: int = 1_000) -> Trade:
    return Trade(
        trading_day="20260928", exchange=Exchange.SHFE, trade_id=trade_id, direction=Direction.BUY,
        account_id="ACC1", instrument_id="rb2701", broker_order_ref="ref", offset=Offset.OPEN,
        price=3100.0, qty=1, trade_ts_ns=trade_ts_ns, received_ns=received_ns,
    )


def _import(*trades: Trade) -> TradeImport:
    return TradeImport(command_id="k1", trading_day="20260928", trades=list(trades), source="csv", actor="ops",
                       role=Role.ADMIN)


def _fingerprint(trades: list[dict[str, object]], requester: str = "admin-key") -> dict[str, object]:
    """What api-gateway hashes for an idempotent REST mutation."""
    return {"method": "POST", "path": "/api/v1/trades/import", "requester": requester,
            "body": {"trading_day": "20260928", "trades": trades, "source": "import"}}


class TestTopLevelOnly:
    def test_top_level_time_fields_are_ignored(self) -> None:
        a = CashMoveRequest(movement_id="m1", account_id="ACC1", type=CashType.DEPOSIT, amount="10.00", reason="r",
                            actor="ops", role=Role.ADMIN, requested_ns=1)
        b = CashMoveRequest(movement_id="m1", account_id="ACC1", type=CashType.DEPOSIT, amount="10.00", reason="r",
                            actor="ops", role=Role.ADMIN, requested_ns=2)
        assert request_hash(a) == request_hash(b) == request_hash(encode(b))
        assert request_hash({"x": 1, "sent_ns": 1, "received_ns": 2}) == request_hash({"x": 1, "ts_ns": 3})

    def test_nested_time_fields_count(self) -> None:
        # REST fingerprint: the body (and its trades) is nested under "body"
        assert request_hash(_fingerprint([{"trade_id": "t1", "ts_ns": 1}])) != request_hash(
            _fingerprint([{"trade_id": "t1", "ts_ns": 2}])
        )
        assert request_hash(_fingerprint([{"trade_id": "t1", "ts_ns": 1}])) == request_hash(
            _fingerprint([{"ts_ns": 1, "trade_id": "t1"}])
        )
        # bus payload: TradeImport.trades[].received_ns
        assert request_hash(_import(_trade("t1", received_ns=5))) != request_hash(_import(_trade("t1", received_ns=6)))
        assert request_hash(_import(_trade("t1"))) == request_hash(encode(_import(_trade("t1"))))

    def test_import_with_other_trade_times_is_a_conflict(self, db_factory: DbFactory) -> None:
        db = db_factory()
        db.open()
        pc = ProcessedCommands(db)
        calls: list[int] = []

        def handler(conn: Connection) -> Reply:
            calls.append(1)
            return ok_reply(message="imported")

        def run(req: TradeImport) -> Reply:
            return db.write(
                lambda c: pc.run_once(
                    c, command_id=req.command_id, command="td.import_trades", requester="api-gateway", payload=req,
                    trading_day="20260928", received_ns=T0, handler=handler,
                )
            )

        first = run(_import(_trade("t1", received_ns=5, trade_ts_ns=100)))
        assert first.ok and not first.duplicate
        again = run(_import(_trade("t1", received_ns=5, trade_ts_ns=100)))
        assert again.duplicate and again.message == "imported"
        other_ts = run(_import(_trade("t1", received_ns=6, trade_ts_ns=100)))
        assert other_ts.code == codes.CONFLICT
        other_trade_ts = run(_import(_trade("t1", received_ns=5, trade_ts_ns=101)))
        assert other_trade_ts.code == codes.CONFLICT
        assert calls == [1]
