"""Self-contained ``report.html`` of a backtest run (inline CSS, the chart as a base64 PNG)."""

from __future__ import annotations

import base64
import html
import io
from collections.abc import Mapping, Sequence
from typing import Any

from futrisk.backtest.analysis import TOTAL, by_account
from futrisk.backtest.config import BacktestConfig
from futrisk.backtest.replay import ArmResult

MAX_TRIGGER_ROWS = 300

CSS = """
body{font-family:-apple-system,"Segoe UI","Microsoft YaHei",sans-serif;margin:24px auto;max-width:1180px;padding:0 16px;
color:#1f2328;background:#fff;line-height:1.5}
h1{font-size:22px;margin:0 0 4px}h2{font-size:17px;margin:28px 0 8px;border-bottom:1px solid #d0d7de;padding-bottom:4px}
p.lead{color:#57606a;margin:0 0 12px}table{border-collapse:collapse;margin:6px 0 12px;font-size:13px}
th,td{border:1px solid #d0d7de;padding:3px 8px;text-align:right;white-space:nowrap}th{background:#f6f8fa}
td.l,th.l{text-align:left}.ok{color:#1a7f37;font-weight:600}.bad{color:#cf222e;font-weight:600}
.box{background:#f6f8fa;border:1px solid #d0d7de;border-radius:6px;padding:10px 14px;margin:8px 0}
img{max-width:100%}.small{font-size:12px;color:#57606a}
"""


def _e(x: Any) -> str:
    return html.escape("" if x is None else str(x))


def _pct(x: Any, digits: int = 2) -> str:
    return "—" if x is None else f"{float(x) * 100:.{digits}f}%"


def _pp(x: Any, digits: int = 2) -> str:
    return "—" if x is None else f"{float(x) * 100:+.{digits}f}pp"


def _table(headers: Sequence[str], rows: Sequence[Sequence[Any]], left: int = 1) -> str:
    head = "".join(f'<th class="l">{_e(h)}</th>' if i < left else f"<th>{_e(h)}</th>" for i, h in enumerate(headers))
    body = "".join(
        "<tr>" + "".join(f'<td class="l">{c}</td>' if i < left else f"<td>{c}</td>" for i, c in enumerate(r)) + "</tr>"
        for r in rows
    )
    return f"<table><tr>{head}</tr>{body}</table>"


def equity_chart(results: Mapping[str, ArmResult]) -> str | None:
    """Cumulative P&L of the whole book (% of the initial capital) per arm, as a data URI."""
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except Exception:  # pragma: no cover - matplotlib is a dependency; the report still renders without it
        return None
    fig, ax = plt.subplots(figsize=(10.5, 3.6), dpi=100)
    for name, r in results.items():
        rows = by_account(r).get(TOTAL, [])
        cap = sum(r.capital.values()) or 1.0
        cum, ys = 0.0, []
        for row in rows:
            cum += float(row["day_pnl"])
            ys.append(cum / cap * 100)
        ax.plot(range(len(ys)), ys, label=name, linewidth=1.1)
        latched = [i for i, row in enumerate(rows) if row["halt_latched"]]
        if latched:
            ax.scatter(latched, [ys[i] for i in latched], s=10, zorder=3)
    days = [row["trading_day"] for row in by_account(next(iter(results.values()))).get(TOTAL, [])]
    if days:
        step = max(1, len(days) // 8)
        ticks = list(range(0, len(days), step))
        ax.set_xticks(ticks)
        ax.set_xticklabels([f"{days[i][:4]}-{days[i][4:6]}" for i in ticks], fontsize=8)
    ax.set_ylabel("cumulative P&L, % of capital", fontsize=9)
    ax.grid(alpha=0.3)
    ax.legend(fontsize=8, loc="upper left")
    fig.tight_layout()
    buf = io.BytesIO()
    fig.savefig(buf, format="png")
    plt.close(fig)
    return "data:image/png;base64," + base64.b64encode(buf.getvalue()).decode("ascii")


def _meta(cfg: BacktestConfig, manifest: Mapping[str, Any]) -> str:
    rows = [
        ("配置", f"{_e(cfg.name)}（{_e(cfg.kind)}，{_e(cfg.layer)} 层）"),
        ("方案", "、".join(f"{_e(a.name)}（{_e(a.tier)}）" for a in cfg.arms)),
        ("熔断阈值", _pct(cfg.rules.threshold)),
        ("配置哈希", _e(str(manifest.get("config_hash", ""))[:16])),
        ("数据哈希", _e(str(manifest.get("data_hash", ""))[:16])),
        ("代码", f"git {_e(manifest.get('git_sha'))} · code {_e(str(manifest.get('code_hash', ''))[:16])}"),
        ("运行", f"{_e(manifest.get('started_at'))} → {_e(manifest.get('finished_at'))}，{_e(manifest.get('wall_s'))} 秒"),
    ]
    return _table(["项", "内容"], [(k, v) for k, v in rows], left=2)


def _arms_body(summary: Mapping[str, Any], results: Mapping[str, ArmResult]) -> str:
    base = summary["baseline"]
    per = summary["period"]
    parts = [f'<p class="lead">回放 {_e(per["start"])} ~ {_e(per["end"])}，共 {_e(per["days"])} 个交易日；'
             f"基线方案为 <b>{_e(base)}</b>（不启用规则），其他方案与它逐日配对比较。</p>"]
    for name, t in summary["triggers"].items():
        s = t["summary"]
        tot = summary["deltas"].get(name, {}).get(TOTAL, {})
        parts.append(
            f'<div class="box"><b>{_e(name)}</b> 相对 {_e(base)}：总收益 {_pp(tot.get("total_ret"))}，'
            f'最大回撤 {_pp(tot.get("max_dd"))}，最差单日 {_pp(tot.get("worst_day"))}。规则在 {s["days"]} 个账户日生效'
            f'（熔断锁存 {s["latched"]} 次），当日效果合计 {_pp(s["effect_sum"])}：{s["effect_positive"]} 天少亏、'
            f'{s["effect_negative"]} 天多亏；次日效果合计 {_pp(s["next_effect_sum"])}。</div>')
    chart = equity_chart(results)
    if chart:
        parts.append(f'<h2>累计盈亏（全部账户）</h2><img src="{chart}" alt="cumulative P&amp;L"><p class="small">'
                     "圆点是熔断锁存的交易日。</p>")
    parts.append("<h2>各方案统计</h2>")
    rows: list[Sequence[Any]] = []
    for name, a in summary["arms"].items():
        for acct, st in a["stats"].items():
            rows.append((f"{_e(name)}", _e(acct), _pct(st["total_ret"]), _pct(st["max_dd"]),
                         f'{_pct(st["worst_day"])}（{_e(st["worst_day_date"])}）', st["warn_days"], st["restrict_days"],
                         st["halt_days"], st["latched_days"], st["blocked_opens"], st["blocked_adds"], st["orders"]))
    parts.append(_table(["方案", "账户", "收益", "最大回撤", "最差单日", "告警天数", "限制天数", "熔断天数", "锁存天数",
                         "被拒开仓", "被拒加仓", "委托数"], rows, left=2))
    parts.append(f"<h2>规则效果（相对 {_e(base)}）</h2>")
    rows = [(_e(name), _e(acct), _pp(d["total_ret"]), _pp(d["max_dd"]), _pp(d["worst_day"]))
            for name, per_acct in summary["deltas"].items() for acct, d in per_acct.items()]
    parts.append(_table(["方案", "账户", "收益差", "最大回撤差", "最差单日差"], rows, left=2))
    for name, t in summary["triggers"].items():
        days = t["days"]
        parts.append(f"<h2>{_e(name)} 生效日明细（{len(days)} 个账户日）</h2>")
        rows = [(_e(r["trading_day"]), _e(r["account_id"]), _e(r["level"]), "是" if r["latched"] else "",
                 _pct(r["value"]), _pct(r["base_pnl"]), _pct(r["arm_pnl"]), _pp(r["effect"]), _pp(r["next_effect"]),
                 r["blocked_opens"], r["blocked_adds"]) for r in days[:MAX_TRIGGER_ROWS]]
        parts.append(_table(["交易日", "账户", "级别", "锁存", "触发值", "基线当日", "方案当日", "当日效果", "次日效果",
                             "被拒开仓", "被拒加仓"], rows, left=3))
        if len(days) > MAX_TRIGGER_ROWS:
            parts.append(f'<p class="small">只列出前 {MAX_TRIGGER_ROWS} 行，完整明细见 daily.csv。</p>')
    c = summary["checks"]
    ok = c["digest_mismatches"] == 0 and c["settle_failed_checks"] == 0
    parts.append("<h2>一致性检查</h2>")
    parts.append(f'<p class="{"ok" if ok else "bad"}">EOD 成交摘要不一致 {c["digest_mismatches"]} 次，'
                 f'结算对账检查失败 {c["settle_failed_checks"]} 次。</p>')
    return "".join(parts)


def _scenario_body(summary: Mapping[str, Any]) -> str:
    h = summary["headline"]
    ok = bool(h.get("all_passed"))
    parts = [f'<p class="{"ok" if ok else "bad"}">检查 {h.get("passed")}/{h.get("checks")} 项通过。</p>',
             "<h2>检查结果</h2>"]
    rows = [(_e(c["scenario"]), _e(c["arm"]), _e(c["check"]), '<span class="ok">通过</span>' if c["ok"]
             else '<span class="bad">失败</span>', _e(c["detail"])) for c in summary["checks"]]
    parts.append(_table(["场景", "方案", "检查", "结果", "详情"], rows, left=5))
    for sc in summary["scenarios"]:
        parts.append(f"<h2>{_e(sc['name'])}</h2><p class=\"lead\">{_e(sc['description'])}</p>")
        for arm, a in sc["arms"].items():
            ev = [(_e(e["step"]), _e(e["ts"]), _e(e["kind"]), _e(e["level"]), "是" if e["latched"] else "",
                   _pct(e["value"])) for e in a["events"]]
            parts.append(f"<p><b>{_e(arm)}</b>（{_e(a['tier'])}）：风险事件 {len(ev)} 条</p>")
            if ev:
                parts.append(_table(["步骤", "时间", "事件", "级别", "锁存", "指标值"], ev, left=4))
            orders = [(_e(o["label"]), _e(o["step"]), _e(o["instrument_id"]), _e(f"{o['direction']} {o['offset']}"),
                       o["qty"], _e(o["code"] or "受理"), _e(",".join(o["statuses"]))) for o in a["scripted"]]
            if orders:
                parts.append(_table(["委托", "步骤", "合约", "方向", "手数", "结果", "状态"], orders, left=4))
    return "".join(parts)


def render_report(cfg: BacktestConfig, summary: Mapping[str, Any], results: Mapping[Any, ArmResult],
                  manifest: Mapping[str, Any]) -> str:
    """The whole page. ``results``: ``{arm: result}`` (arms) or ``{(scenario, arm): result}`` (scenario)."""
    title = f"回测报告：{cfg.name}"
    if summary.get("kind") == "arms":
        body = _arms_body(summary, {str(k): v for k, v in results.items()})
        note = ("A 层只有日线，日内路径按“开盘价靠近哪个极值就先走哪个”合成，结论是这个路径模型下的结论；"
                "双均线策略只是占位，用来让委托经过同一个风控内核（10 文档）。")
    else:
        body = _scenario_body(summary)
        note = "D 层合成剧本：价格路径和委托都是给定的，每项检查都有确定的正确答案（10 文档）。"
    return (f'<!DOCTYPE html><html lang="zh-CN"><head><meta charset="utf-8"><title>{_e(title)}</title>'
            f"<style>{CSS}</style></head><body><h1>{_e(title)}</h1><p class=\"lead\">{_e(cfg.description)}</p>"
            f"{_meta(cfg, manifest)}{body}<h2>说明</h2><p class=\"small\">{_e(note)}</p></body></html>")


__all__ = ["equity_chart", "render_report"]
