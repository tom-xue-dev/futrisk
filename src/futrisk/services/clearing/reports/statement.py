"""CTP-style account statements: text and CSV (03 §5.3 ``STATEMENT_TXT`` / ``STATEMENT_CSV``).

Sections (the headings of a CTP 结算单): 资金状况, 成交记录, 平仓明细, 持仓明细,
持仓汇总. The text statement aligns columns by display width (CJK characters
count double); the CSV carries the same sections as structured blocks and is
written UTF-8 with BOM so that spreadsheet programs open it correctly.
"""

from __future__ import annotations

import csv
import io
import unicodedata
from collections.abc import Sequence
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

from futrisk.common.decimals import D, dec_str_strip
from futrisk.services.clearing.reports.model import ReportBundle, generated_label
from futrisk.services.clearing.reports.sections import STATEMENT_HEADINGS

_DIR_CN = {"BUY": "买", "SELL": "卖", "LONG": "买", "SHORT": "卖"}
_OFFSET_CN = {"OPEN": "开", "CLOSE": "平", "CLOSE_TODAY": "平今", "CLOSE_YESTERDAY": "平昨", "FORCE_CLOSE": "强平"}
_CLOSE_TYPE_CN = {"TODAY": "平今", "YD": "平昨"}


def m2(x: Any) -> str:
    """Money with thousands separators and two decimals (ASCII minus, statements are plain text)."""
    d = D(x).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return f"{d:,.2f}"


def plain2(x: Any) -> str:
    return f"{D(x).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP):.2f}"


def rate_pct(x: Any) -> str:
    return f"{(D(x) * 100).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP):.2f}%"


def width(text: str) -> int:
    return sum(2 if unicodedata.east_asian_width(ch) in ("W", "F") else 1 for ch in text)


def pad(text: str, n: int, *, right: bool = False) -> str:
    fill = " " * max(0, n - width(text))
    return fill + text if right else text + fill


def text_table(headers: Sequence[str], rows: Sequence[Sequence[str]], numeric: Sequence[bool]) -> list[str]:
    widths = [width(h) for h in headers]
    for r in rows:
        for i, cell in enumerate(r):
            widths[i] = max(widths[i], width(cell))
    sep = "+" + "+".join("-" * (w + 2) for w in widths) + "+"
    out = [sep, "| " + " | ".join(pad(h, widths[i]) for i, h in enumerate(headers)) + " |", sep]
    for r in rows:
        out.append("| " + " | ".join(pad(c, widths[i], right=numeric[i]) for i, c in enumerate(r)) + " |")
    out.append(sep)
    if not rows:
        out.insert(-1, "| " + pad("（无）", sum(widths) + 3 * (len(widths) - 1)) + " |")
    return out


# ------------------------------------------------------------------- sections
def _account(b: ReportBundle, acct: str) -> Any:
    for a in b.settlement.accounts:
        if a.account_id == acct:
            return a
    raise KeyError(acct)


def funds_section(b: ReportBundle, acct: str) -> list[tuple[str, str]]:
    a = _account(b, acct)
    net = D(a.deposit) - D(a.withdraw)
    return [
        ("上日结存", a.pre_balance),
        ("入金", a.deposit),
        ("出金", a.withdraw),
        ("当日存取合计", str(net)),
        ("平仓盈亏", a.close_pnl),
        ("持仓盯市盈亏", a.position_pnl),
        ("手续费", a.fee),
        ("当日结存", a.balance),
        ("客户权益", a.equity),
        ("保证金占用", a.margin),
        ("可用资金", a.available),
        ("风险度", a.risk_degree),
        ("追加保证金", a.margin_call),
        ("初始资金", a.initial_capital),
        ("累计净入金", a.cum_net_deposit),
        ("单位净值", a.nav),
        ("当前回撤（距高水位）", a.max_drawdown),  # max_drawdown 存的是当前距高水位的回撤
    ]


def trade_rows(b: ReportBundle, acct: str) -> list[dict[str, Any]]:
    """成交记录: ``trade_settlements`` (fee, close P&L) joined with the trade copy (price, qty, offset, time)."""
    by_key = {(t.exchange.value, t.trade_id, t.direction.value): t for t in b.trades if t.account_id == acct}
    keyed = []
    for ts in (x for x in b.settlement.trades if x.account_id == acct):
        t = by_key.get((ts.exchange.value, ts.trade_id, ts.direction.value))
        keyed.append((t.trade_ts_ns if t is not None else 0, ts.trade_id, ts, t))
    rows = []
    for _, _, ts, t in sorted(keyed, key=lambda k: (k[0], k[1])):
        price = D(t.price) if t is not None else None
        qty = t.qty if t is not None else ts.matched_today_qty + ts.matched_yd_qty
        mult = int((b.instruments.get(ts.instrument_id) or {}).get("multiplier") or 1)
        rows.append(
            {
                "trading_day": ts.trading_day,
                "exchange": ts.exchange.value,
                "product": b.product_of(ts.instrument_id),
                "instrument_id": ts.instrument_id,
                "direction": _DIR_CN.get(ts.direction.value, ts.direction.value),
                "hedge": "投",
                "price": dec_str_strip(D(t.price)) if t is not None else "",
                "qty": qty,
                "turnover": plain2(price * qty * mult) if price is not None else "",
                "offset": _OFFSET_CN.get(t.offset.value, t.offset.value) if t is not None else "",
                "fee": ts.fee,
                "close_pnl": ts.close_pnl,
                "trade_id": ts.trade_id,
                "time": _hms(t.trade_ts_ns) if t is not None else "",
            }
        )
    return rows


def close_rows(b: ReportBundle, acct: str) -> list[dict[str, Any]]:
    rows = []
    for c in sorted((x for x in b.settlement.close_details if x.account_id == acct), key=lambda x: x.detail_no):
        rows.append(
            {
                "trading_day": c.trading_day,
                "exchange": c.exchange.value,
                "product": b.product_of(c.instrument_id),
                "instrument_id": c.instrument_id,
                "open_td": c.lot_open_td,
                "direction": _DIR_CN.get(c.direction.value, c.direction.value),
                "qty": c.qty,
                "open_price": c.lot_open_price,
                "basis_price": c.basis_price,
                "close_price": c.close_price,
                "close_pnl": c.close_pnl_mtm,
                "close_pnl_fifo": c.close_pnl_fifo,
                "type": _CLOSE_TYPE_CN.get(c.close_type, c.close_type),
            }
        )
    return rows


def holding_rows(b: ReportBundle, acct: str) -> list[dict[str, Any]]:
    """Per lot (from the next-day SOD, basis restored to the MTM basis of the settled day)."""
    positions = {(p.instrument_id, p.direction.value): p for p in b.settlement.positions if p.account_id == acct}
    lots = []
    if b.next_sod is not None:
        for a in b.next_sod.accounts:
            if a.account_id == acct:
                lots = list(a.lots)
    rows = []
    for lot in sorted(lots, key=lambda x: (x.instrument_id, x.direction.value, x.lot_no)):
        pos = positions.get((lot.instrument_id, lot.direction.value))
        settle = D(lot.basis_price)
        mult = pos.multiplier if pos is not None else int((b.instruments.get(lot.instrument_id) or {}).get("multiplier") or 1)
        pre = D(pos.pre_settle_price) if pos is not None else settle
        basis = D(lot.open_price) if lot.open_td == b.trading_day else pre
        sign = 1 if lot.direction.value == "LONG" else -1
        rate = D(pos.margin_rate) if pos is not None else Decimal(0)
        rows.append(
            {
                "exchange": pos.exchange.value if pos is not None else "",
                "product": b.product_of(lot.instrument_id),
                "instrument_id": lot.instrument_id,
                "open_td": lot.open_td,
                "hedge": "投",
                "direction": _DIR_CN.get(lot.direction.value, lot.direction.value),
                "qty": lot.qty,
                "open_price": lot.open_price,
                "pre_settle": str(pre),
                "settle": str(settle),
                "float_pnl": plain2((settle - D(lot.open_price)) * lot.qty * mult * sign),
                "mtm_pnl": plain2((settle - basis) * lot.qty * mult * sign),
                "margin": plain2(settle * lot.qty * mult * rate),
            }
        )
    return rows


def summary_rows(b: ReportBundle, acct: str) -> list[dict[str, Any]]:
    by_inst: dict[str, dict[str, Any]] = {}
    for p in b.settlement.positions:
        if p.account_id != acct or p.eod_qty == 0:
            continue
        r = by_inst.setdefault(
            p.instrument_id,
            {
                "product": p.product,
                "instrument_id": p.instrument_id,
                "long_qty": 0,
                "long_avg": "",
                "short_qty": 0,
                "short_avg": "",
                "pre_settle": p.pre_settle_price,
                "settle": p.settle_price,
                "position_pnl": Decimal(0),
                "margin": Decimal(0),
            },
        )
        if p.direction.value == "LONG":
            r["long_qty"] = p.eod_qty
            r["long_avg"] = p.avg_open_price or ""
        else:
            r["short_qty"] = p.eod_qty
            r["short_avg"] = p.avg_open_price or ""
        r["position_pnl"] += D(p.position_pnl)
        r["margin"] += D(p.margin_gross)
    return [dict(v, position_pnl=plain2(v["position_pnl"]), margin=plain2(v["margin"]), hedge="投") for _, v in sorted(by_inst.items())]


def _hms(ns: int) -> str:
    from futrisk.common.clock import ns_to_bj

    return ns_to_bj(ns).strftime("%H:%M:%S")


# ------------------------------------------------------------------- renderers
TRADE_COLS = (
    ("成交日期", "trading_day", False),
    ("交易所", "exchange", False),
    ("品种", "product", False),
    ("合约", "instrument_id", False),
    ("买/卖", "direction", False),
    ("投/保", "hedge", False),
    ("成交价", "price", True),
    ("手数", "qty", True),
    ("成交额", "turnover", True),
    ("开平", "offset", False),
    ("手续费", "fee", True),
    ("平仓盈亏", "close_pnl", True),
    ("成交时间", "time", False),
    ("成交编号", "trade_id", False),
)
CLOSE_COLS = (
    ("平仓日期", "trading_day", False),
    ("交易所", "exchange", False),
    ("品种", "product", False),
    ("合约", "instrument_id", False),
    ("开仓日期", "open_td", False),
    ("买/卖", "direction", False),
    ("手数", "qty", True),
    ("开仓价", "open_price", True),
    ("盯市基准", "basis_price", True),
    ("成交价", "close_price", True),
    ("平仓盈亏", "close_pnl", True),
    ("逐笔平仓盈亏", "close_pnl_fifo", True),
    ("类型", "type", False),
)
HOLDING_COLS = (
    ("交易所", "exchange", False),
    ("品种", "product", False),
    ("合约", "instrument_id", False),
    ("开仓日期", "open_td", False),
    ("投/保", "hedge", False),
    ("买/卖", "direction", False),
    ("持仓量", "qty", True),
    ("开仓价", "open_price", True),
    ("昨结算", "pre_settle", True),
    ("结算价", "settle", True),
    ("浮动盈亏", "float_pnl", True),
    ("盯市盈亏", "mtm_pnl", True),
    ("保证金", "margin", True),
)
SUMMARY_COLS = (
    ("品种", "product", False),
    ("合约", "instrument_id", False),
    ("买持", "long_qty", True),
    ("买均价", "long_avg", True),
    ("卖持", "short_qty", True),
    ("卖均价", "short_avg", True),
    ("昨结算", "pre_settle", True),
    ("今结算", "settle", True),
    ("持仓盯市盈亏", "position_pnl", True),
    ("保证金占用", "margin", True),
    ("投/保", "hedge", False),
)


def _table(cols: Sequence[tuple[str, str, bool]], rows: Sequence[dict[str, Any]]) -> list[str]:
    return text_table(
        [c[0] for c in cols],
        [[str(r.get(c[1], "")) for c in cols] for r in rows],
        [c[2] for c in cols],
    )


def render_statement_txt(b: ReportBundle, acct: str) -> str:
    a = _account(b, acct)
    name = b.account_name(acct)
    lines: list[str] = []
    rule = "=" * 96
    lines.append(pad("", 38) + "结  算  单")
    lines.append(pad("", 34) + "Settlement Statement")
    lines.append(rule)
    lines.append(f"客户号 Client ID：{acct}    客户名称 Name：{name}")
    lines.append(f"交易日 Date：{b.trading_day}    结算版本 Version：v{b.version}    币种：人民币 CNY")
    lines.append(f"生成时间：{generated_label(b)}    配置哈希：{b.config_hash[:12]}")
    lines.append(rule)
    # 资金状况（两列排布，与 CTP 结算单一致）
    lines.append(pad("", 40) + STATEMENT_HEADINGS[0] + "  Account Summary")
    lines.append("-" * 96)
    pairs = [
        ("上日结存", m2(a.pre_balance), "客户权益", m2(a.equity)),
        ("当日存取合计", m2(D(a.deposit) - D(a.withdraw)), "保证金占用", m2(a.margin)),
        ("平仓盈亏", m2(a.close_pnl), "可用资金", m2(a.available)),
        ("持仓盯市盈亏", m2(a.position_pnl), "风险度", rate_pct(a.risk_degree)),
        ("手续费", m2(a.fee), "追加保证金", m2(a.margin_call)),
        ("当日结存", m2(a.balance), "单位净值", str(D(a.nav).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP))),
        ("入金", m2(a.deposit), "出金", m2(a.withdraw)),
        ("初始资金", m2(a.initial_capital), "距高水位回撤", rate_pct(a.max_drawdown)),
    ]
    for l1, v1, l2, v2 in pairs:
        lines.append(pad(l1 + "：", 16) + pad(v1, 22, right=True) + "    " + pad(l2 + "：", 16) + pad(v2, 22, right=True))
    sections = (
        (STATEMENT_HEADINGS[1], "Transaction Record", TRADE_COLS, trade_rows(b, acct)),
        (STATEMENT_HEADINGS[2], "Position Closed", CLOSE_COLS, close_rows(b, acct)),
        (STATEMENT_HEADINGS[3], "Positions Detail", HOLDING_COLS, holding_rows(b, acct)),
        (STATEMENT_HEADINGS[4], "Positions", SUMMARY_COLS, summary_rows(b, acct)),
    )
    for title, en, cols, rows in sections:
        lines.append("")
        lines.append(pad("", 40) + title + "  " + en)
        lines.extend(_table(cols, rows))
        if title == STATEMENT_HEADINGS[4] and rows:
            groups = [g for g in b.settlement.margin_groups if g.account_id == acct]
            if groups:
                lines.append(
                    "保证金按单向大边收取：" + "；".join(f"{g.margin_group} {g.method} {m2(g.charged_margin)}" for g in groups)
                )
    lines.append("")
    lines.append("注：盯市盈亏按结算价逐日盯市；平今以开仓价、平昨以昨结算价为基准（01 §8.15）。")
    return "\n".join(lines) + "\n"


def render_statement_csv(b: ReportBundle, acct: str) -> str:
    """Same sections as the text statement, as CSV blocks (caller writes it with ``utf-8-sig``)."""
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(["section", STATEMENT_HEADINGS[0]])
    w.writerow(["account_id", acct])
    w.writerow(["trading_day", b.trading_day])
    w.writerow(["version", b.version])
    w.writerow(["项目", "金额"])
    for label, value in funds_section(b, acct):
        w.writerow([label, value])
    for title, cols, rows in (
        (STATEMENT_HEADINGS[1], TRADE_COLS, trade_rows(b, acct)),
        (STATEMENT_HEADINGS[2], CLOSE_COLS, close_rows(b, acct)),
        (STATEMENT_HEADINGS[3], HOLDING_COLS, holding_rows(b, acct)),
        (STATEMENT_HEADINGS[4], SUMMARY_COLS, summary_rows(b, acct)),
    ):
        w.writerow([])
        w.writerow(["section", title])
        w.writerow([c[0] for c in cols])
        for r in rows:
            w.writerow([r.get(c[1], "") for c in cols])
    return buf.getvalue()


__all__ = [
    "close_rows",
    "funds_section",
    "holding_rows",
    "m2",
    "render_statement_csv",
    "render_statement_txt",
    "summary_rows",
    "text_table",
    "trade_rows",
    "width",
]
