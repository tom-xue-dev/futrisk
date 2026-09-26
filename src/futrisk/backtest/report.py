"""Self-contained HTML report of a backtest run (inline CSS, matplotlib charts as base64 PNG, Agg backend).

Sections depend on the run kind:

- ``arms``: the conclusions (generated from the numbers), per-arm statistics,
  paired effects with their intervals and multiplicity-adjusted significance,
  the path-model sensitivity table, the controls (restrict vs circuit break,
  placebo, the ladder), charts, effects per mark (close / settle / next open),
  true/false positives, bounding classes, leave-one-out, the path model's
  calibration and hit rate, roll handling, performance and the disclosures;
- ``stress``: one table and one intraday chart per window;
- ``sweep``: threshold × tier heatmaps in- and out-of-sample, the optimum's
  location and the plateau test per objective, rank stability;
- ``scenario``: the checks of every layer D scenario and its P&L path.

Every sentence of the conclusions is produced from the summary's numbers; an
ensemble's numbers are medians over the path realizations and conditional on
the calibrated path model.
"""

from __future__ import annotations

import base64
import html
import io
import math
from collections.abc import Mapping, Sequence
from typing import Any

from futrisk.backtest.config import BacktestConfig
from futrisk.backtest.metrics import STATIC, TOTAL, daily_matrix, fmt_money, fmt_pct, fmt_pp
from futrisk.backtest.replay import ArmResult

# reference palette (fixed slot order; dataviz skill references/palette.md)
SLOTS = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#4a3aa7", "#e34948"]
INK = "#0b0b0b"
INK2 = "#52514e"
MUTED = "#898781"
GRID = "#e1e0d9"
SURFACE = "#fcfcfb"
ACTING = ("block_open", "halt_only", "halve", "flatten")
TIER_ZH = {"none": "无规则", "alert_only": "仅告警", "block_open": "限制+熔断（生产）", "halt_only": "仅熔断",
           "halve": "熔断+减半", "flatten": "熔断+清仓", "placebo": "安慰剂", "production": "全部规则"}
BASIS_ZH = {"close": "收盘价", "settle": "结算价", "open": "次日开盘"}

CSS = """
:root{--surface:#fcfcfb;--page:#f9f9f7;--ink:#0b0b0b;--ink2:#52514e;--muted:#898781;--grid:#e1e0d9;--rule:#c3c2b7;
--good:#006300;--bad:#d03b3b;--accent:#2a78d6;--card:#ffffff}
@media (prefers-color-scheme: dark){:root:not([data-theme="light"]){--surface:#1a1a19;--page:#0d0d0d;--ink:#ffffff;
--ink2:#c3c2b7;--muted:#898781;--grid:#2c2c2a;--rule:#383835;--good:#0ca30c;--bad:#e66767;--accent:#3987e5;--card:#1a1a19}}
:root[data-theme="dark"]{--surface:#1a1a19;--page:#0d0d0d;--ink:#ffffff;--ink2:#c3c2b7;--muted:#898781;--grid:#2c2c2a;
--rule:#383835;--good:#0ca30c;--bad:#e66767;--accent:#3987e5;--card:#1a1a19}
*{box-sizing:border-box}
body{margin:0;background:var(--page);color:var(--ink);font:14px/1.55 system-ui,-apple-system,"Segoe UI","Microsoft YaHei",sans-serif}
main{max-width:1180px;margin:0 auto;padding:24px 16px 64px}
h1{font-size:24px;margin:0 0 4px}
h2{font-size:18px;margin:36px 0 10px;padding-top:8px;border-top:1px solid var(--rule)}
h3{font-size:15px;margin:22px 0 8px}
p,li{color:var(--ink2);max-width:96ch}
.sub{color:var(--muted);font-size:13px}
.card{background:var(--card);border:1px solid var(--grid);border-radius:8px;padding:12px 14px;margin:10px 0}
.kv{display:grid;grid-template-columns:max-content 1fr;gap:2px 14px;font-size:13px}
.kv b{color:var(--ink2);font-weight:600}
.scroll{overflow-x:auto;max-width:100%}
table{border-collapse:collapse;font-size:12.5px;margin:6px 0 12px;font-variant-numeric:tabular-nums}
th,td{border-bottom:1px solid var(--grid);padding:4px 8px;text-align:right;white-space:nowrap}
th{color:var(--ink2);font-weight:600;background:var(--surface);position:sticky;top:0}
td.l,th.l{text-align:left}
tr.total td{font-weight:600}
.pos{color:var(--good)}.neg{color:var(--bad)}
.ok{color:var(--good);font-weight:600}.fail{color:var(--bad);font-weight:600}
img.chart{max-width:100%;height:auto;border:1px solid var(--grid);border-radius:6px;background:#fcfcfb;margin:6px 0}
.note{font-size:12.5px;color:var(--muted)}
.lead{font-size:14.5px;color:var(--ink)}
code{font-size:12px}
"""


# ------------------------------------------------------------------ helpers
def esc(x: Any) -> str:
    return html.escape("" if x is None else str(x))


def _num(x: Any, fmt: str = "{:,.2f}") -> str:
    if x is None or (isinstance(x, float) and not math.isfinite(x)):
        return "—"
    try:
        return fmt.format(x)
    except (TypeError, ValueError):
        return esc(x)


def _signed_cls(x: Any) -> str:
    if isinstance(x, int | float) and math.isfinite(x) and x != 0:
        return "pos" if x > 0 else "neg"
    return ""


def _med(x: Any) -> Any:
    """A number, or the median of an ensemble quantile dict."""
    if isinstance(x, dict):
        return x.get("median", x.get("delta"))
    return x


def table(headers: Sequence[str], rows: Sequence[Sequence[Any]], *, left: int = 1, classes: Sequence[str] | None = None) -> str:
    out = ['<div class="scroll"><table><thead><tr>']
    for i, h in enumerate(headers):
        out.append(f'<th class="{"l" if i < left else ""}">{esc(h)}</th>')
    out.append("</tr></thead><tbody>")
    for k, r in enumerate(rows):
        cls = f' class="{classes[k]}"' if classes and classes[k] else ""
        out.append(f"<tr{cls}>")
        for i, c in enumerate(r):
            if isinstance(c, tuple):
                txt, css = c
                out.append(f'<td class="{"l " if i < left else ""}{css}">{txt}</td>')
            else:
                out.append(f'<td class="{"l" if i < left else ""}">{c if isinstance(c, str) else esc(c)}</td>')
        out.append("</tr>")
    out.append("</tbody></table></div>")
    return "".join(out)


def _mpl() -> Any:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib import font_manager, rcParams

    names = {f.name for f in font_manager.fontManager.ttflist}
    for cand in ("Microsoft YaHei", "SimHei", "Noto Sans CJK SC", "Source Han Sans SC", "WenQuanYi Zen Hei"):
        if cand in names:
            rcParams["font.sans-serif"] = [cand, *rcParams.get("font.sans-serif", [])]
            break
    rcParams["axes.unicode_minus"] = False
    rcParams["axes.edgecolor"] = GRID
    rcParams["axes.labelcolor"] = INK2
    rcParams["xtick.color"] = MUTED
    rcParams["ytick.color"] = MUTED
    rcParams["axes.grid"] = True
    rcParams["grid.color"] = GRID
    rcParams["grid.linewidth"] = 0.6
    rcParams["axes.facecolor"] = SURFACE
    rcParams["figure.facecolor"] = SURFACE
    rcParams["axes.spines.top"] = False
    rcParams["axes.spines.right"] = False
    rcParams["font.size"] = 9
    return plt


def _png(fig: Any) -> str:
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=110, bbox_inches="tight")
    import matplotlib.pyplot as plt

    plt.close(fig)
    return '<img class="chart" alt="chart" src="data:image/png;base64,' + base64.b64encode(buf.getvalue()).decode("ascii") + '">'


def arm_colors(names: Sequence[str]) -> dict[str, str]:
    return {n: SLOTS[i % len(SLOTS)] for i, n in enumerate(names)}


def _dates(days: Sequence[str]) -> list[Any]:
    from datetime import date

    return [date(int(d[:4]), int(d[4:6]), int(d[6:])) for d in days]


def _pp(d: Mapping[str, Any] | None, key: str = "delta") -> str:
    if not d:
        return "—"
    return fmt_pp(d.get(key))


def _range(d: Mapping[str, Any] | None, lo: str, hi: str) -> str:
    if not d or d.get(lo) is None or d.get(hi) is None:
        return "—"
    return f"[{fmt_pp(d.get(lo))}, {fmt_pp(d.get(hi))}]"


# ------------------------------------------------------------------ charts
def equity_chart(results: Sequence[ArmResult], account: str, title: str, sets: Mapping[str, Sequence[str]]) -> str:
    plt = _mpl()
    import numpy as np

    names = [r.arm for r in results]
    colors = arm_colors(names)
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10.5, 5.2), sharex=True, gridspec_kw={"height_ratios": [2.2, 1]})
    for r in results:
        days, pnl = daily_matrix(r.daily, r.accounts, sets)
        if account not in pnl or not days:
            continue
        members = sets.get(account, [account])
        cap = sum(r.capital.get(a, 0.0) for a in members)
        if cap <= 0:
            continue
        nav = 1.0 + np.cumsum(pnl[account]) / cap
        peak = np.maximum.accumulate(np.concatenate([[1.0], nav]))[1:]
        dd = (nav - peak) / peak
        x = _dates(days)
        ls = "--" if r.tier == "alert_only" else ":" if r.tier == "placebo" else "-"
        ax1.plot(x, nav, color=colors[r.arm], lw=1.4, ls=ls, label=r.arm)
        ax2.plot(x, dd * 100, color=colors[r.arm], lw=1.1, ls=ls)
    ax1.set_ylabel("净值 (初始资金=1)")
    ax2.set_ylabel("回撤 %")
    ax1.set_title(title, fontsize=10, color=INK, loc="left")
    ax1.legend(fontsize=8, ncol=min(6, len(results)), frameon=False, loc="upper left")
    fig.tight_layout()
    return _png(fig)


def band_chart(curves: Mapping[str, Any], account: str, title: str) -> str:
    """Cumulative Δ return vs the baseline: median over the path realizations with the 5-95 % band."""
    plt = _mpl()
    days = curves.get("days") or []
    if not days:
        return ""
    x = _dates(days)
    names = [n for n in curves if n != "days" and account in curves[n]]
    colors = arm_colors(names)
    fig, ax = plt.subplots(figsize=(10.5, 3.6))
    for n in names:
        c = curves[n][account]
        ax.fill_between(x, c["p5"] * 100, c["p95"] * 100, color=colors[n], alpha=0.14, lw=0)
        ax.plot(x, c["p50"] * 100, color=colors[n], lw=1.4, label=n)
    ax.axhline(0, color=MUTED, lw=0.8)
    ax.set_ylabel("累计 Δ收益（百分点）")
    ax.set_title(title, fontsize=10, color=INK, loc="left")
    ax.legend(fontsize=8, ncol=6, frameon=False, loc="upper left")
    fig.tight_layout()
    return _png(fig)


def intraday_chart(series: Mapping[str, Sequence[tuple[int, float]]], thresholds: Mapping[str, float], title: str) -> str:
    """Daily P&L (% of initial capital) through the day per arm, with the rule levels."""
    plt = _mpl()
    from futrisk.common.clock import ns_to_bj

    colors = arm_colors(list(series))
    fig, ax = plt.subplots(figsize=(10.5, 3.6))
    for name, pts in series.items():
        if not pts:
            continue
        x = list(range(len(pts)))
        ax.plot(x, [p * 100 for _t, p in pts], color=colors[name], lw=1.4, label=name, marker="o", ms=2.0)
    for lv, th in thresholds.items():
        ax.axhline(th * 100, color=MUTED, lw=0.8)
        ax.text(0, th * 100, f" {lv} {th * 100:.1f}%", color=INK2, fontsize=7, va="bottom")
    any_pts = next((p for p in series.values() if p), None)
    if any_pts:
        n = len(any_pts)
        ticks = sorted({0, n // 4, n // 2, 3 * n // 4, n - 1})
        ax.set_xticks(ticks)
        ax.set_xticklabels([ns_to_bj(any_pts[i][0]).strftime("%m-%d %H:%M") for i in ticks], fontsize=7)
    ax.set_ylabel("当日盈亏 / 初始资金 %")
    ax.set_xlabel("市场状态（按时间顺序；休市时段不占宽度）", fontsize=8, color=MUTED)
    ax.set_title(title, fontsize=10, color=INK, loc="left")
    ax.legend(fontsize=8, ncol=5, frameon=False, loc="best")
    fig.tight_layout()
    return _png(fig)


def heatmap(values: Sequence[Sequence[float | None]], rows: Sequence[str], cols: Sequence[str], title: str, fmt: str) -> str:
    plt = _mpl()
    import numpy as np
    from matplotlib.colors import LinearSegmentedColormap

    arr = np.array([[np.nan if v is None else v for v in r] for r in values], dtype=float)
    fin = arr[np.isfinite(arr)]
    lim = float(np.max(np.abs(fin))) if fin.size else 1.0
    lim = lim or 1.0
    cmap = LinearSegmentedColormap.from_list("div", ["#e34948", "#f0efec", "#2a78d6"])
    fig, ax = plt.subplots(figsize=(1.2 + 0.95 * len(cols), 0.9 + 0.5 * len(rows)))
    ax.imshow(arr, cmap=cmap, vmin=-lim, vmax=lim, aspect="auto")
    ax.set_xticks(range(len(cols)))
    ax.set_xticklabels(cols)
    ax.set_yticks(range(len(rows)))
    ax.set_yticklabels(rows)
    ax.grid(False)
    for i in range(arr.shape[0]):
        for j in range(arr.shape[1]):
            v = arr[i, j]
            ax.text(j, i, "—" if not np.isfinite(v) else fmt.format(v), ha="center", va="center", fontsize=8, color=INK)
    ax.set_title(title, fontsize=10, color=INK, loc="left")
    fig.tight_layout()
    return _png(fig)


# ------------------------------------------------------------------ sections
def header(cfg: BacktestConfig, manifest: Mapping[str, Any], summary: Mapping[str, Any]) -> str:
    perf = summary.get("performance", {})
    model = summary.get("model") or {}
    mk = model.get("kind")
    if mk == "ensemble":
        model_txt = (f"路径集合：{model.get('paths')} 条随机日内路径（按 B 层分钟线校准，见“路径模型”一节），"
                     "所有结论以该模型为条件")
    elif mk == "deterministic":
        model_txt = f"确定性日内路径（规则 {esc(model.get('rule'))}，同时段合约同步到达极值）"
    else:
        model_txt = "—"
    rows = [
        ("配置", f"{esc(cfg.name)} ({esc(cfg.kind)}, 层 {esc(cfg.layer)})"),
        ("区间", f"{esc(cfg.start or '数据起点')} – {esc(cfg.end or '数据终点')}"),
        ("日内路径", model_txt),
        ("config_hash", f"<code>{esc(manifest.get('config_hash'))}</code>"),
        ("data_hash", f"<code>{esc(manifest.get('data_hash'))}</code> ({len(manifest.get('data', {}).get('files', {}))} 个文件，读取时计算)"),
        ("code_hash", f"<code>{esc(str(manifest.get('code_hash'))[:16])}</code>"
                      + (f"（运行期间有改动：{esc(', '.join(manifest.get('code_changed_during_run') or []))}）"
                         if manifest.get("code_changed_during_run") else "")),
        ("seed / git", f"{esc(manifest.get('seed'))} / {esc(manifest.get('git_sha'))}"),
        ("生成", f"{esc(manifest.get('started_at'))} → {esc(manifest.get('finished_at'))}，耗时 {perf.get('wall_s', 0) or 0:.1f}s"),
    ]
    kv = "".join(f"<b>{k}</b><span>{v}</span>" for k, v in rows)
    desc = f"<p>{esc(cfg.description)}</p>" if cfg.description else ""
    return f'<h1>回测报告：{esc(cfg.name)}</h1><div class="sub">futrisk 风控规则有效性回测（01 §8.16）</div>{desc}<div class="card kv">{kv}</div>'


def _tier(summary: Mapping[str, Any], name: str) -> str | None:
    return (summary.get("arms", {}).get(name) or {}).get("tier")


def _arm_by_tier(summary: Mapping[str, Any], tier: str) -> str | None:
    for name, a in summary.get("arms", {}).items():
        if a.get("tier") == tier:
            return str(name)
    return None


def _focus_accounts(summary: Mapping[str, Any], cfg: BacktestConfig) -> list[str]:
    sets = summary.get("sets") or {}
    out = [TOTAL] + ([STATIC] if STATIC in sets else [])
    ladder = [a for a, r in sorted(cfg.accounts.items()) if r.role == "book" and r.strategy == "counter_trend_add"]
    controls = [a for a, r in sorted(cfg.accounts.items()) if r.role == "control"]
    return out + ladder + controls


def _acct_zh(a: str, cfg: BacktestConfig) -> str:
    if a == TOTAL:
        return "TOTAL（全部账户）"
    if a == STATIC:
        return "静态持仓账户合计"
    r = cfg.accounts.get(a)
    if r is not None and r.title:
        return f"{a}（{r.title}）"
    if r is not None and r.strategy == "counter_trend_add" and r.role == "book":
        return f"{a}（逆势加仓）"
    if r is not None and r.role == "control":
        return f"{a}（对照）"
    return a


def conclusions(summary: Mapping[str, Any], cfg: BacktestConfig) -> str:
    """Conclusion sentences generated from the summary's numbers."""
    deltas = summary.get("deltas") or {}
    if not deltas:
        return ""
    ens = (summary.get("model") or {}).get("kind") == "ensemble"
    paths = int(summary.get("paths") or 0)
    lines: list[str] = []
    cond = f"（以路径模型为条件：{paths} 条校准路径的中位数；方括号先为路径 5–95% 区间，再为路径×日期自助 95% 区间）" if ens else \
        "（单条确定性路径；方括号为日期块自助 95% 区间）"
    focus = _focus_accounts(summary, cfg)
    for tier in ACTING:
        name = _arm_by_tier(summary, tier)
        if name is None or name not in deltas:
            continue
        parts = []
        for a in focus:
            d = (deltas[name].get(a) or {}).get("total_ret")
            if not d:
                continue
            rng = (f"{_range(d, 'p5', 'p95')} {_range(d, 'lo', 'hi')}" if ens else _range(d, "lo", "hi"))
            sig = d.get("significant")
            sig_txt = "" if sig is None else ("，多重比较校正后显著" if sig else "，校正后不显著")
            parts.append(f"{esc(_acct_zh(a, cfg))} {fmt_pp(d.get('delta'))} {rng}{sig_txt}")
        if parts:
            lines.append(f"<li><b>{esc(name)}（{TIER_ZH.get(tier, tier)}）</b> Δ总收益：" + "；".join(parts) + "。</li>")
    comp = summary.get("comparisons") or {}
    dec = comp.get("decomposition") or {}
    ladder_first = [a for a in focus if a not in (TOTAL, STATIC) and cfg.accounts.get(a) is not None
                    and cfg.accounts[a].role == "book"] + [TOTAL]
    for a in ladder_first:
        v = dec.get(a)
        if not v:
            continue
        blk, ho, rp = _med(v.get("block_open")), _med(v.get("halt_only")), _med(v.get("restrict_part"))
        if blk is None:
            continue
        share = f"（限制级别占 {rp / blk * 100:.0f}%）" if blk and abs(blk) > 1e-9 and rp is not None else ""
        lines.append(f"<li><b>拆分</b>（{esc(_acct_zh(a, cfg))}）：限制+熔断 {fmt_pp(blk)} = 仅熔断 {fmt_pp(ho)} + "
                     f"限制级别（单笔减半、亏损品种禁止加仓）{fmt_pp(rp)}{share}。</li>")
    ladder_acct = next((a for a in focus if a not in (TOTAL, STATIC) and cfg.accounts.get(a) is not None
                        and cfg.accounts[a].role == "book"), None)
    for pname, p in (comp.get("placebo") or {}).items():
        accts = p.get("accounts") or {}
        for a in [x for x in (ladder_acct, TOTAL) if x]:
            v = accts.get(a)
            if not v:
                continue
            tv = v.get("timing_value")
            tvm = _med(tv)
            if tvm is None:
                continue
            band = f"[{fmt_pp(tv.get('p5'))}, {fmt_pp(tv.get('p95'))}]" if isinstance(tv, dict) else ""
            verdict = ""
            if isinstance(tv, dict) and tv.get("n", 0) > 1 and tv.get("p5") is not None and tv.get("p95") is not None:
                verdict = ("：各条路径上触发时机都优于随机时点" if tv["p5"] > 0 else "：各条路径上触发时机都不如随机时点"
                           if tv["p95"] < 0 else "：在路径之间与随机时点区分不开")
            else:
                verdict = "：触发时机优于随机时点" if tvm > 0 else "：触发时机不优于随机时点"
            lines.append(f"<li><b>安慰剂 {esc(pname)}</b>（{esc(_acct_zh(a, cfg))}；同样的动作放到未触发的随机交易日，次数与日内时点匹配）："
                         f"安慰剂 {fmt_pp(_med(v.get('placebo')))}，真实触发 {fmt_pp(_med(v.get('trigger')))}，"
                         f"时机价值 = 真实 − 安慰剂 {fmt_pp(tvm)} {band}{verdict}。</li>")
    blk_name = _arm_by_tier(summary, "block_open")
    for b, v in (comp.get("ladder") or {}).items():
        lc = _med(v.get("ladder_contribution"))
        if lc is None:
            continue
        txt = f"<li><b>加仓阶梯</b>：基线中 {esc(b)} 的加仓部分（{esc(b)} − 无加仓对照 {esc(v.get('control'))}）贡献 {fmt_pp(lc)}"
        dd = deltas.get(blk_name or "", {})
        xb = ((dd.get(b) or {}).get("total_ret") or {}).get("delta")
        xc = ((dd.get(v.get("control")) or {}).get("total_ret") or {}).get("delta")
        if xb is not None and xc is not None:
            txt += (f"；限制+熔断对 {esc(b)} 的效果 {fmt_pp(xb)}，对无加仓对照 {fmt_pp(xc)}，"
                    f"两者之差 {fmt_pp(xb - xc)} 是拦下加仓的部分")
            if abs(xb) > 1e-9:
                txt += f"（占 {esc(b)} 效果的 {(xb - xc) / xb * 100:.0f}%）"
        lines.append(txt + "。加仓阶梯在合成路径上的盈亏取决于日内路径的振荡（见第 4 节）。</li>")
    sens = summary.get("sensitivity") or {}
    groups = sens.get("groups") or {}
    main_g = groups.get("main") or {}

    def same_window(key: str) -> bool:
        g = groups.get(key) or {}
        st = (g.get("meta") or {}).get("set") or {}
        return (g.get("layer") == main_g.get("layer") and g.get("start") == main_g.get("start")
                and g.get("end") == main_g.get("end") and "data.minute_window" not in st)

    rows = [r for r in sens.get("rows", []) if r.get("tier") == "block_open" and same_window(str(r.get("group")))]
    if len(rows) >= 2 and ladder_acct:
        vals = [(r.get("title"), r.get(f"d_ret|{ladder_acct}")) for r in rows if r.get(f"d_ret|{ladder_acct}") is not None]
        if len(vals) >= 2:
            lo = min(vals, key=lambda x: x[1])
            hi = max(vals, key=lambda x: x[1])
            lines.append(f"<li><b>路径敏感性</b>（同一区间）：限制+熔断对 {esc(ladder_acct)} 的效果随日内路径模型从 {fmt_pp(lo[1])}"
                         f"（{esc(lo[0])}）到 {fmt_pp(hi[1])}（{esc(hi[0])}）。</li>")
    bw = [r for r in sens.get("rows", []) if r.get("tier") == "block_open" and not same_window(str(r.get("group")))]
    if bw and ladder_acct:
        parts = [f"{esc(r.get('title'))}：加仓贡献 {fmt_pp(r.get(f'ladder|{ladder_acct}'))}，限制+熔断对 {esc(ladder_acct)} "
                 f"{fmt_pp(r.get(f'd_ret|{ladder_acct}'))}" for r in bw]
        lines.append("<li><b>分钟线窗口对照</b>（B 层真实分钟路径 vs 同窗口的 A 层合成路径）：" + "；".join(parts) + "。</li>")
    tp = summary.get("tp_fp") or {}
    if tp.get("close"):
        c0, c1 = (tp["close"].get("0") or {}), (tp["close"].get("1") or {})
        s0 = (tp.get("settle") or {}).get("0") or {}
        lines.append(f"<li><b>触发的持续性</b>：halt 触发当日收盘时亏损仍 ≥ 阈值的比例 {fmt_pct(_med(c0.get('tp_rate')), 1)}"
                     f"（按结算价计 {fmt_pct(_med(s0.get('tp_rate')), 1)}：结算价是全天成交量加权价，下跌日高于收盘价）；"
                     f"至次日收盘 {fmt_pct(_med(c1.get('tp_rate')), 1)}。开盘跳空触发占 {fmt_pct(_med(tp.get('gap_share')), 1)}"
                     "（合成路径没有夜盘与日盘之间的跳空，这是下限）。</li>")
    return ("<h2>1 结论</h2>" + f"<p class=\"lead\">{cond}</p><ul>" + "".join(lines) + "</ul>" +
            "<p class=\"note\">这些数字全部由本次回测的汇总自动生成。“显著”指按账户做 Holm 校正后的自助 p 值 &lt; 0.05；"
            "路径集合的结果依赖于校准所用的近几个月分钟线及其对 2015 年以来的外推。</p>")


def summary_section(summary: Mapping[str, Any], cfg: BacktestConfig) -> str:
    arms = summary.get("arms", {})
    if not arms:
        return ""
    ens = (summary.get("model") or {}).get("kind") == "ensemble"
    heads = ["方案", "档位", "账户", "总收益", "CAGR", "最大回撤", "Calmar", "CVaR95(日)", "年化波动", "熔断锁存日",
             "开盘跳空触发", "被拒开仓", "被拒加仓", "手续费"]
    rows = []
    classes = []
    sets = summary.get("sets") or {}
    for name, a in arms.items():
        st = a["stats"]
        accts = [k for k in st if k not in sets] + [k for k in (TOTAL, STATIC) if k in st]
        for acct in accts:
            s = st[acct]
            rows.append([name, TIER_ZH.get(a.get("tier") or "", a.get("tier") or ""), acct,
                         (fmt_pct(s.get("total_ret")), _signed_cls(s.get("total_ret"))), fmt_pct(s.get("cagr")),
                         fmt_pct(s.get("max_dd")), _num(s.get("calmar")) if (s.get("cagr") or 0) > 0 else "—",
                         fmt_pct(s.get("cvar95")), fmt_pct(s.get("vol_a")),
                         _num(s.get("latched_days"), "{:,.0f}"), _num(s.get("halt_at_open"), "{:,.0f}"),
                         _num(s.get("blocked_opens"), "{:,.0f}"), _num(s.get("blocked_adds"), "{:,.0f}"), fmt_money(s.get("fees"))])
            classes.append("total" if acct in (TOTAL, STATIC) else "")
    note = ("收益与回撤按初始资金计（不复利）。Calmar = CAGR ÷ 最大回撤，仅在 CAGR > 0 时列出。TOTAL 只含账面账户；"
            "对照账户（如去掉加仓的 ACC3）单独列出、不计入 TOTAL。")
    if ens:
        note += " 路径集合：表中每个数是各条路径结果的中位数。"
    return "<h2>2 各方案汇总</h2>" + table(heads, rows, left=3, classes=classes) + f'<p class="note">{note}</p>'


def ci_section(summary: Mapping[str, Any], cfg: BacktestConfig) -> str:
    deltas = summary.get("deltas", {})
    if not deltas:
        return ""
    ens = (summary.get("model") or {}).get("kind") == "ensemble"
    if ens:
        heads = ["方案", "账户", "Δ总收益", "路径5–95%", "路径×日期95%", "校正p", "Δ最大回撤", "路径5–95%", "路径×日期95%",
                 "ΔCVaR95", "路径×日期95%"]
    else:
        heads = ["方案", "账户", "Δ总收益", "95% CI", "校正p", "Δ最大回撤", "95% CI", "ΔCVaR95", "95% CI", "ΔCalmar", "95% CI"]
    rows = []
    for arm, per in deltas.items():
        sets = summary.get("sets") or {}
        order = [k for k in per if k not in sets] + [k for k in (TOTAL, STATIC) if k in per]
        for acct in order:
            d = per[acct]
            tr, dd, cv, cm = d.get("total_ret", {}), d.get("max_dd", {}), d.get("cvar95", {}), d.get("calmar", {})
            padj: Any = _num(tr.get("p_adj"), "{:.3f}")
            if tr.get("significant"):
                padj = (padj + " *", "pos" if (tr.get("delta") or 0) > 0 else "neg")
            if ens:
                rows.append([arm, acct, (fmt_pp(tr.get("delta")), _signed_cls(tr.get("delta"))), _range(tr, "p5", "p95"),
                             _range(tr, "lo", "hi"), padj, fmt_pp(dd.get("delta")), _range(dd, "p5", "p95"),
                             _range(dd, "lo", "hi"), fmt_pp(cv.get("delta")), _range(cv, "lo", "hi")])
            else:
                rows.append([arm, acct, (fmt_pp(tr.get("delta")), _signed_cls(tr.get("delta"))), _range(tr, "lo", "hi"), padj,
                             fmt_pp(dd.get("delta")), _range(dd, "lo", "hi"), fmt_pp(cv.get("delta")), _range(cv, "lo", "hi"),
                             _num(cm.get("delta")), f"[{_num(cm.get('lo'))}, {_num(cm.get('hi'))}]"])
    base = summary.get("baseline")
    note = (f"差值 = 方案 − 基线（{esc(base)}；alert_only 与 no_rules 的交易完全相同）。日期块自助：同一组交易日配对重抽样"
            f"（循环块长 {cfg.analysis.bootstrap_block} 个交易日）。校正 p = 按账户与账户组做 {cfg.analysis.multiplicity.upper()} "
            "校正后的双侧自助 p 值，* 表示 &lt; 0.05。最大回撤与 CVaR 为正数表示损失，Δ为负表示改善。")
    if ens:
        note += (" 路径集合：Δ为各路径效果的中位数；路径5–95% 只反映路径模型的不确定性；路径×日期95% 每次抽一条路径再对其交易日"
                 f"做块抽样（{cfg.ensemble.bootstrap} 次），同时包含两种不确定性。")
    return "<h2>3 配对差值与区间</h2>" + table(heads, rows, left=2) + f'<p class="note">{note}</p>'


def sensitivity_section(summary: Mapping[str, Any], cfg: BacktestConfig) -> str:
    sens = summary.get("sensitivity") or {}
    rows_in = sens.get("rows") or []
    if not rows_in:
        return ""
    focus = [a for a in _focus_accounts(summary, cfg)]
    heads = ["路径/数据", "条数", "方案"] + [f"Δ收益 {a}" for a in focus] + ["ΔMDD TOTAL", "熔断锁存日"]
    ladder_keys = sorted({k for r in rows_in for k in r if k.startswith("ladder|")})
    heads += [f"加仓贡献 {k.split('|', 1)[1]}" for k in ladder_keys]
    body = []
    for r in rows_in:
        cells: list[Any] = [esc(r.get("title")), _num(r.get("paths"), "{:,.0f}") if r.get("paths") else "确定", esc(r.get("arm"))]
        for a in focus:
            v = r.get(f"d_ret|{a}")
            if r.get("paths") and r.get(f"d_ret_p5|{a}") is not None:
                cells.append((f"{fmt_pp(v)} [{fmt_pp(r.get(f'd_ret_p5|{a}'))}, {fmt_pp(r.get(f'd_ret_p95|{a}'))}]", _signed_cls(v)))
            else:
                cells.append((fmt_pp(v), _signed_cls(v)))
        cells.append(fmt_pp(r.get(f"d_mdd|{TOTAL}")))
        cells.append(_num(r.get("latched_days"), "{:,.0f}"))
        for k in ladder_keys:
            cells.append(fmt_pp(r.get(k)))
        body.append(cells)
    groups = sens.get("groups") or {}
    desc = []
    for g in groups.values():
        m = g.get("meta") or {}
        sets_txt = ", ".join(f"{k}={v}" for k, v in (m.get("set") or {}).items())
        desc.append(f"<li><b>{esc(g.get('title'))}</b>：层 {esc(g.get('layer'))}，{esc(g.get('start') or '起点')}–"
                    f"{esc(g.get('end') or '终点')}，" + (f"{g.get('paths')} 条路径" if g.get("paths") else "确定性路径")
                    + (f"；覆盖：{esc(sets_txt)}" if sets_txt else "") + "</li>")
    return ("<h2>4 路径敏感性</h2><p>同一组数据与策略，只改变日内路径的假设（极值先后、振荡、合约是否同步），"
            "以及在分钟线（B 层）覆盖的窗口上与真实分钟路径对照。集合行显示中位数 [路径 5%, 95%]。</p>"
            + table(heads, body, left=3) + "<ul>" + "".join(desc) + "</ul>"
            + '<p class="note">加仓贡献 = 无规则时加仓账户与其无加仓对照账户的总收益之差（阶梯本身赚或亏多少）。B 层窗口较短、触发很少，'
              "它主要检验路径模型能否复现加仓阶梯在真实分钟路径上的盈亏。</p>")


def comparisons_section(summary: Mapping[str, Any], cfg: BacktestConfig) -> str:
    comp = summary.get("comparisons") or {}
    if not comp:
        return ""
    out = ["<h2>5 对照：限制与熔断的拆分、安慰剂、加仓阶梯</h2>"]
    dec = comp.get("decomposition") or {}
    rows: list[list[Any]]
    if dec:
        rows = [[esc(_acct_zh(a, cfg)), fmt_pp(_med(v.get("block_open"))), fmt_pp(_med(v.get("halt_only"))),
                 fmt_pp(_med(v.get("restrict_part")))] for a, v in dec.items()]
        out.append("<h3>限制+熔断 = 仅熔断 + 限制级别</h3>" + table(["账户", "限制+熔断 Δ收益", "仅熔断 Δ收益", "限制级别部分"], rows) +
                   '<p class="note">限制级别（−2.5%）：单笔委托减半、亏损品种禁止加仓（REJECT_ADD_TO_LOSER）；熔断（−3%）：禁止开仓'
                   "（REJECT_ACCOUNT_HALTED）。生产规则 block_open 两者都有。</p>")
    rej = []
    for name, a in (summary.get("arms") or {}).items():
        for acct, codes in (a.get("rejects_by_code") or {}).items():
            if codes:
                rej.append([esc(name), esc(acct)] + [_num(codes.get(c), "{:,.0f}") for c in
                                                     ("REJECT_ADD_TO_LOSER", "REJECT_ACCOUNT_HALTED", "REJECT_ORDER_SIZE_RESTRICTED")])
    if rej:
        out.append(table(["方案", "账户", "禁止加仓", "熔断禁开", "单笔超限"], rej, left=2))
    for pname, p in (comp.get("placebo") or {}).items():
        rows = []
        for a, v in (p.get("accounts") or {}).items():
            tv = v.get("timing_value")
            band = f" [{fmt_pp(tv.get('p5'))}, {fmt_pp(tv.get('p95'))}]" if isinstance(tv, dict) else ""
            rows.append([esc(_acct_zh(a, cfg)), fmt_pp(_med(v.get("trigger"))), fmt_pp(_med(v.get("placebo"))),
                         (fmt_pp(_med(tv)) + band, _signed_cls(_med(tv)))])
        out.append(f"<h3>安慰剂 {esc(pname)}（模仿 {esc(p.get('of'))}）</h3>" +
                   table(["账户", "真实触发 Δ收益", "安慰剂 Δ收益", "时机价值"], rows) +
                   '<p class="note">安慰剂：不运行规则，在该账户没有触发熔断的随机交易日（次数等于真实方案的锁存日数），'
                   "按真实锁存的日内时点执行同样的动作（经引擎人工熔断 + 同样比例的减仓）。时机价值 = 真实 − 安慰剂："
                   "若接近 0，效果来自“少持仓”而不是“在亏损时停下”。</p>")
    lad = comp.get("ladder") or {}
    if lad:
        rows = [[esc(b), esc(v.get("control")), fmt_pct(_med(v.get("book_ret"))), fmt_pct(_med(v.get("control_ret"))),
                 (fmt_pp(_med(v.get("ladder_contribution"))), _signed_cls(_med(v.get("ladder_contribution"))))]
                for b, v in lad.items()]
        out.append("<h3>加仓阶梯的贡献（基线）</h3>" + table(["账户", "对照（无加仓）", "账户总收益", "对照总收益", "加仓贡献"], rows))
    return "".join(out)


def avoided_section(summary: Mapping[str, Any], cfg: BacktestConfig) -> str:
    am = summary.get("avoided_missed", {})
    if not am:
        return ""
    primary, ph = cfg.analysis.primary_basis, cfg.analysis.primary_horizon
    rows = []
    for arm, per in am.items():
        for basis in ("close", "settle", "open"):
            b = per.get(basis) or {}
            for h, v in b.items():
                mark = f"{BASIS_ZH[basis]}" + (" ★" if basis == primary and str(ph) == h else "")
                rows.append([arm, mark, f"d..d+{h}", _num(_med(v.get("episodes")), "{:,.0f}"), fmt_money(_med(v.get("avoided"))),
                             fmt_money(_med(v.get("missed"))), (fmt_money(_med(v.get("net"))), _signed_cls(_med(v.get("net")))),
                             f"{_num(_med(v.get('wins')), '{:,.0f}')}/{_num(_med(v.get('losses')), '{:,.0f}')}"])
    pl = summary.get("position_level", {})
    extra = ""
    if pl:
        extra = "<h3>被强制减仓头寸的价值</h3>" + table(
            ["方案", "减仓动作数", "至当日收盘", "至结算价", "至次日开盘"],
            [[arm, _num(_med(v.get("actions")), "{:,.0f}"),
              (fmt_money(_med(v.get("value_close"))), _signed_cls(_med(v.get("value_close")))),
              (fmt_money(_med(v.get("value_settle"))), _signed_cls(_med(v.get("value_settle")))),
              (fmt_money(_med(v.get("value_next_open"))), _signed_cls(_med(v.get("value_next_open"))))]
             for arm, v in pl.items() if _med(v.get("actions"))])
    return ("<h2>7 规则起作用日的效果（按计价口径）</h2>" +
            table(["方案", "计价", "期限", "起作用账户日", "正值合计", "负值合计", "净额", "正/负次数"], rows, left=3) + extra +
            '<p class="note">起作用账户日 = 当天触发了 restrict 或 halt 的账户日；效果 = 该方案 − 基线从 d 日日初到 d+h 日计价点的盈亏'
            "（元），截至该账户下一次起作用。计价点：收盘价（主口径，★ 为主要期限）；结算价（SHFE/DCE 为全天成交量加权价，下跌收盘日"
            "高于收盘价，所以按结算价的“次日效果”里其实还包含当天尾盘的下跌，不是隔夜延续）；次日开盘（隔夜跳空后的首个价格）。"
            "正值 = 规则避免了损失，负值 = 被拦下/减掉的头寸之后赚钱了。被减仓头寸价值 = 平掉的手数 ×（计价价 − 平仓价），按方向取号。</p>")


def tpfp_section(summary: Mapping[str, Any]) -> str:
    tp = summary.get("tp_fp")
    if not tp:
        return ""
    rows = []
    for basis in ("close", "settle", "open"):
        per = tp.get(basis) or {}
        for h, v in per.items():
            rows.append([BASIS_ZH[basis], f"d..d+{h}", _num(_med(v.get("tp")), "{:,.0f}"), _num(_med(v.get("fp")), "{:,.0f}"),
                         fmt_pct(_med(v.get("tp_rate")), 1), _num(_med(v.get("deepened")), "{:,.0f}"),
                         fmt_pct(_med(v.get("deepened_rate")), 1)])
    return ("<h2>8 真阳 / 假阳</h2>" + f"<p>alert_only 方案 halt 级别触发 {_num(_med(tp.get('triggers')), '{:,.0f}')} 次"
            f"（换月日 {_num(_med(tp.get('on_roll_days')), '{:,.0f}')} 次" +
            ("，按配置不计入比率" if _med(tp.get("excluded_roll_days")) else "") +
            f"；依赖估算结算价的交易日 {_num(_med(tp.get('on_estimated_settle_days')), '{:,.0f}')} 次）；开盘跳空触发 "
            f"{_num(_med(tp.get('gap_triggers')), '{:,.0f}')} 次（{fmt_pct(_med(tp.get('gap_share')), 1)}，下限：合成路径在夜盘与日盘之间没有跳空）。</p>" +
            table(["计价", "期限", "真阳 TP", "假阳 FP", "TP 率", "亏损加深", "加深率"], rows, left=2) +
            '<p class="note">TP：从触发日日初到 d+h 日计价点的累计收益仍 ≤ −阈值（亏损持续越过阈值）；FP：已回到阈值以内。'
            "按收盘价与按结算价的差别就是结算价与收盘价之差：下跌日结算价（全天成交量加权）高于收盘价。</p>")


def bounding_section(summary: Mapping[str, Any]) -> str:
    b = summary.get("bounding", {})
    if not b:
        return ""
    rows = []
    for arm, per in b.items():
        for lv, v in per.items():
            iv = v.get("interval")
            if isinstance(iv, list) and iv and isinstance(iv[0], dict):
                iv_txt = f"[{_num(_med(iv[0]), '{:,.0f}')}, {_num(_med(iv[1]), '{:,.0f}')}]"
            elif isinstance(iv, list):
                iv_txt = f"[{iv[0]}, {iv[1]}]"
            else:
                iv_txt = "—"
            rows.append([arm, lv, _num(_med(v.get("certain")), "{:,.0f}"), _num(_med(v.get("uncertain")), "{:,.0f}"),
                         _num(_med(v.get("impossible")), "{:,.0f}"), iv_txt, _num(_med(v.get("synth_triggers")), "{:,.0f}"),
                         _num(_med(v.get("certain_triggered")), "{:,.0f}"), _num(_med(v.get("uncertain_triggered")), "{:,.0f}"),
                         _num(_med(v.get("strategy_driven")), "{:,.0f}"), _num(_med(v.get("mixed_session_days")), "{:,.0f}")])
    ei = summary.get("effect_interval", {})
    rows2 = []
    for arm, v in ei.items():
        items = v.items() if isinstance(v, dict) and all(isinstance(x, dict) and "low" in x for x in v.values()) else [("", v)]
        for key, x in items:
            if not isinstance(x, dict) or "low" not in x:
                continue
            rows2.append([arm, esc(key), fmt_money(_med(x.get("certain_part"))), fmt_money(_med(x.get("uncertain_part"))),
                          f"[{fmt_money(_med(x.get('low')))}, {fmt_money(_med(x.get('high')))}]"])
    return ("<h2>9 日线路径的界定分类</h2>" + table(["方案", "级别", "必然触发", "不确定", "静态不可能", "静态持仓触发日数区间",
                                                "回放触发日数", "必然且触发", "不确定且触发", "因日内交易触发（另计）", "混合时段账户日"], rows) +
            (table(["方案", "计价_期限", "必然日的差额", "其余触发日的差额", "效果区间 [下限, 上限]"], rows2, left=2) if rows2 else "") +
            '<p class="note">按开盘后的静态持仓计算（账户日）：悲观界 = 所有合约同时处于不利极值；乐观界 = 任何符合日线的路径都至少跌到的'
            "水平。乐观界仍越线 = 必然触发，悲观界不越线 = 静态持仓不可能触发，其余不确定。静态持仓触发日数区间 = [必然, 必然 + 不确定]。"
            "“因日内交易触发” = 静态持仓不可能越线、但回放中触发了（越线来自账户自己的日内交易，例如逆势加仓），单独计数，"
            "它同样依赖路径模型。开盘时刻不同的持仓（夜盘合约与只有日盘的股指）没有共同的开盘状态，按各合约独立开盘计算。"
            "分类只说明静态持仓能否越线，与路径顺序无关的只是触发次数的区间，规则效果的大小仍取决于越线后的路径。"
            "效果区间：必然日的差额全计，其余触发日的差额按 0 到实测值计（下限加上其中的负值，上限加上正值）。</p>")


def loo_section(summary: Mapping[str, Any], cfg: BacktestConfig) -> str:
    loo = summary.get("leave_one_out") or {}
    if not loo:
        return ""
    rows = []
    focus = _focus_accounts(summary, cfg)
    for arm, per in loo.items():
        for a in focus:
            v = per.get(a)
            if not v or not v.get("drops"):
                continue
            full = (v.get("full") or {}).get("d_total_ret")
            flips = v.get("sign_flips") or []
            rows.append([arm, esc(a), fmt_pp(full), fmt_pp(v.get("min_d_total_ret")), esc(v.get("min_drop")),
                         fmt_pp(v.get("max_d_total_ret")), ("是：" + esc(", ".join(flips))) if flips else "否"])
    if not rows:
        return ""
    ens = (summary.get("model") or {}).get("kind") == "ensemble"
    return ("<h2>10 稳健性：逐年 / 逐窗口剔除</h2>" + table(["方案", "账户", "全区间 Δ收益", "剔除后最小", "剔除的是", "剔除后最大",
                                                        "符号翻转"], rows, left=2) +
            '<p class="note">每次去掉一个自然年或一个压力窗口的全部交易日重新计算 Δ总收益（' +
            ("路径集合：各路径逐日差额的平均" if ens else "单条路径") + "）。效果集中在少数日子时，剔除一年就会明显改变结论。</p>")


def hit_section(summary: Mapping[str, Any]) -> str:
    hr = summary.get("hit_rate")
    if not hr:
        return ""
    rows = []
    for p, per in sorted(hr.get("by_product", {}).items()):
        v = per.get("60")
        if v:
            rows.append([p, _num(v["days"], "{:,.0f}"), _num(v["determined"], "{:,.0f}"), _num(v["same_bar"], "{:,.0f}"),
                         fmt_pct(v["hit_rate"], 1)])
    tot = [[f"{k}m", _num(v["days"], "{:,.0f}"), _num(v["determined"], "{:,.0f}"), fmt_pct(v["hit_rate"], 1)]
           for k, v in hr.get("by_period", {}).items()]
    return ("<h3>“开盘价靠近哪个极值就先走哪个”的命中率（B 层分钟线）</h3>" + table(["周期", "交易日", "可判定", "命中率"], tot) +
            "<p>分品种（仅 60 分钟线；5/15 分钟线每个品种只有几十天）：</p>" +
            table(["品种", "交易日", "可判定", "高低点同一根K线", "命中率"], rows) +
            '<p class="note">可判定 = 当日最高点与最低点落在不同的分钟K线上；命中 = 先出现的极值与规则预测一致。约 80% 的命中率意味着'
            "约五分之一的交易日两个极值的先后与规则相反：确定性路径把这部分日子的顺序排错了，路径集合按这个概率随机抽取。</p>")


def model_section(summary: Mapping[str, Any]) -> str:
    model = summary.get("model") or {}
    cal = model.get("calibration")
    parts = ["<h2>11 日内路径模型</h2>"]
    if not cal:
        parts.append(f"<p>确定性路径：规则 {esc(model.get('rule'))}，腿内线性插值，同一时段模板的合约在同样的时点（交易时间的 1/3、2/3）"
                     "到达极值（悲观的同步假设）。</p>")
    else:
        cp = cal.get("calibration_period")
        rows = []
        for p, v in sorted((cal.get("products") or {}).items()):
            eff = v.get(f"efficiency_{cp}m") or {}
            chk = v.get("checks") or {}
            c60 = chk.get("60m") or {}
            c5 = chk.get("5m") or {}
            rows.append([p, esc(v.get("sector")), fmt_pct(v.get("hit_rate"), 1), _num(v.get("order_days"), "{:,.0f}"),
                         fmt_pct(v.get("p_near_first"), 1), esc(v.get("timing")), _num(v.get("sigma"), "{:.2f}"),
                         f"{_num(eff.get('empirical'), '{:.2f}')} / {_num(eff.get('model'), '{:.2f}')} / {_num(eff.get('linear'), '{:.2f}')}",
                         f"{_num(c60.get('empirical'), '{:.2f}')} / {_num(c60.get('model'), '{:.2f}')}",
                         f"{_num(c5.get('empirical'), '{:.2f}')} / {_num(c5.get('model'), '{:.2f}')}"])
        parts.append(f"<p>{model.get('paths')} 条随机路径。每条路径每个合约日：极值先后按下表概率抽取；两个极值的时刻取自分钟线的经验分布；"
                     "锚点之间是直线加布朗桥振荡（σ 以当日振幅为单位），截断在 [最低, 最高] 内；同一板块的品种通过高斯 copula 相关"
                     "（ρ 见下），不同板块独立；同一品种的合约（近月/远月）共享抽样。开盘、最高、最低、收盘与日线完全一致。</p>")
        parts.append(table(["品种", "板块", "60m命中率", "可判定日", "近端先到概率 p", "时点分布来源", "σ",
                            f"{cp}m 效率 实测/模型/直线", "60m 效率 实测/模型", "5m 效率 实测/模型"], rows, left=2))
        srows = [[s, ", ".join(v.get("products") or []), _num(v.get("rank_correlation"), "{:.2f}"), _num(v.get("rho"), "{:.2f}")]
                 for s, v in sorted((cal.get("sectors") or {}).items())]
        parts.append(table(["板块", "品种", "极值时点秩相关", "copula ρ"], srows))
        parts.append('<p class="note">效率 = Σ|相邻K线收盘价之差| ÷ 当日振幅（开盘价作为第一个收盘价）：衡量日内振荡。σ 使模型在校准周期上的平均效率'
                     "等于实测值；其它周期的对比是检验。“直线”为没有振荡的确定性路径的效率。校准数据只有最近几个月的新浪分钟线，"
                     "被外推到 2015 年以来的全部交易日。</p>")
    parts.append(hit_section(summary))
    return "".join(parts)


def roll_section(summary: Mapping[str, Any]) -> str:
    rg = summary.get("roll_gaps")
    if not rg:
        return ""
    est = rg.get("estimated_rolls") or {}
    rows = [[p, _num(rg["rolls"].get(p), "{:,.0f}"), _num(est.get(p, 0), "{:,.0f}"), f"{rg['gap_mean_abs_pct'].get(p, 0):.2f}%",
             f"{rg['gap_max_abs_pct'].get(p, 0):.2f}%"] for p in sorted(rg["rolls"])]
    return ("<h2>12 换月处理</h2><p>回测持有真实合约：换月日在开盘状态平掉旧主力、开新主力，旧合约按其自身昨结算计盈亏、"
            "新合约按开仓价计，所以引擎的当日盈亏里没有换月跳空。策略信号用按换月比例回调的连续序列：比例 = 新合约昨结算 ÷ 旧合约昨结算；"
            "DCE 没有新合约的昨结算，用新合约前一日的（估算）结算价，2024-07-17 以后只有主力合约的数据，用新合约的开盘价（均单独计数）。"
            f"若改用未调整的连续序列，基线中 {rg['roll_account_days']} 个持仓换月的账户日里会多出 {rg['naive_false_triggers']} 次假熔断、"
            f"漏掉 {rg['naive_missed_triggers']} 次（按结算口径的阈值判断）。</p>" +
            table(["品种", "换月次数", "其中估算", "平均|换月价差|", "最大|换月价差|"], rows))


def perf_section(summary: Mapping[str, Any]) -> str:
    p = summary.get("performance")
    if not p:
        return ""
    kv = [
        ("回放 tick 数 / 市场状态数", f"{_num(p.get('ticks'), '{:,.0f}')} / {_num(p.get('states'), '{:,.0f}')}"),
        ("策略委托数（经事前检查）", _num(p.get("orders"), "{:,.0f}")),
        ("回放吞吐（ticks/s；states/s）", f"{_num(p.get('ticks_per_s_replay'), '{:,.0f}')}；{_num(p.get('states_per_s_replay'), '{:,.0f}')}"),
        ("端到端吞吐（含每日结算，ticks/s）", _num(p.get("ticks_per_s_end_to_end"), "{:,.0f}")),
        ("回放 / 策略与委托 / 结算 / 数据准备（CPU 秒）", f"{p.get('replay_s', 0) or 0:.1f} / {p.get('strategy_s', 0) or 0:.1f} / "
                                              f"{p.get('settle_s', 0) or 0:.1f} / {p.get('plan_s', 0) or 0:.1f}"),
        ("回放的账户日 / 复制的账户运行", f"{_num(p.get('replayed_account_days'), '{:,.0f}')} / {_num(p.get('copied_account_runs'), '{:,.0f}')}"),
        ("每账户日结算耗时", f"{p.get('settle_ms_per_account_day') or 0:.1f} ms"),
    ]
    return ("<h2>13 性能</h2><div class=\"card kv\">" + "".join(f"<b>{k}</b><span>{v}</span>" for k, v in kv) + "</div>" +
            '<p class="note">复制的账户运行：某账户在 alert_only 中从未触发 restrict/halt 时，规则在任何动作档位都不会生效，'
            "其结果与基线逐日相同，直接复制而不重放（检查见测试）。</p>")


def disclosures(cfg: BacktestConfig, summary: Mapping[str, Any] | None = None) -> str:
    model = (summary or {}).get("model") or {}
    ens = model.get("kind") == "ensemble"
    items = [
        "只启用了日亏规则（daily_loss）；其它生产规则（回撤、集中度等）不在各方案里，以便单独度量这一条规则。",
        "block_open（生产规则）= 限制级别（−2.5%：单笔减半、亏损品种禁止加仓）+ 熔断（−3%：禁止开仓，平仓允许），"
        "halt_only 只保留熔断；两者之差是限制级别的贡献。halve / flatten 是回测自己的动作（生产环境只生成减仓计划，TD-08），"
        "在熔断锁存时按生产减仓规划器下单。",
        "ACC3 的“熔断后减仓 10%”（halt_trim）在各方案中关闭，以免与方案的动作混在一起。",
        ("日内路径为随机路径集合（见第 11 节）：结论以路径模型为条件；同一板块品种的极值时点按校准的相关性部分同步。"
         if ens else
         "日内路径为确定性合成路径：同一时段的合约在同样时点到达极值（同步），这是偏悲观的假设；随机路径集合放宽了它。"),
        ("确定性合成路径在腿内单调，熔断确认（2 个不同的行情状态或 1 秒）总会成功，不会出现“待确认后撤销”；"
         "随机路径有振荡，可以出现撤销。" if not ens else
         "随机路径有振荡，熔断待确认后可能撤销（引擎的 2 个行情状态 / 1 秒确认规则照常执行）。"),
        "仓位按初始资金定规模（不复利）：某个方案干预之后，下一次调仓各方案又回到相同的持仓，配对差额只反映规则本身；"
        "收益、回撤都按初始资金计。",
        "历史合约按 2026 年 products.yaml 的乘数、最小变动价位、手续费、保证金率与交易时段计算（历史费率、保证金调整、"
        "2015 年股指 09:15 开盘等均未建模）；历史价格不在今天的价位网格上时，委托价按可成交方向取整，成交价仍为历史价格。",
        "涨跌停：状态价格等于当日极值且该极值达到配置涨跌停、或是 ≥3.5% 的开盘/收盘极值时视为封板，封板方向不成交。",
        "成交：新委托在当前价格点全部成交（无滑点、无部分成交）；不可成交的限价单挂着，价格穿越后按限价成交；收盘撤单。",
        "DCE 主力以外合约与无结算价的交易日用 (最高+最低)/2 估算结算价；DCE 换月的回调比例用估算的新合约昨结算（第 12 节）。",
        "B 层（新浪分钟线）成交量不可靠，结算价仍取当日官方结算价；C 层（自录 tick）按 01 §8.15 从回放数据计算结算价。",
        "开盘跳空触发占比是下限：合成路径在夜盘与日盘之间没有跳空。",
        "回测不启动任何服务：同一个 EngineCore（auto_provisional=False，周期定时器关闭）+ InprocBus + 进程内券商 + 同一个 settle_day。",
    ]
    return "<h2>14 披露与简化</h2><ul>" + "".join(f"<li>{esc(x)}</li>" for x in items) + "</ul>"


def stress_rows_table(rows: Sequence[Mapping[str, Any]]) -> str:
    body = []
    for r in rows:
        band = ""
        if r.get("ret_p5") is not None and r.get("ret_p95") is not None:
            band = f" [{fmt_pct(r['ret_p5'])}, {fmt_pct(r['ret_p95'])}]"
        body.append([esc(r["title"]), r["arm"], r["account_id"], (fmt_pct(r.get("ret")) + band, _signed_cls(r.get("ret"))),
                     fmt_pct(r.get("max_dd")), fmt_pct(r.get("worst_day")), fmt_pct(r.get("worst_intraday")),
                     _num(r.get("halts"), "{:,.1f}" if r.get("paths") else "{:,.0f}"), _num(r.get("days"), "{:,.0f}")])
    return table(["窗口", "方案", "账户", "窗口收益", "窗口内最大回撤", "最差单日", "最差盘中", "熔断", "交易日"], body, left=3)


# ------------------------------------------------------------------ kinds
def render_arms(cfg: BacktestConfig, summary: Mapping[str, Any], results: Sequence[ArmResult], extra: Mapping[str, Any]) -> str:
    sets = summary.get("sets") or {TOTAL: list(results[0].accounts) if results else []}
    out = [conclusions(summary, cfg), summary_section(summary, cfg), ci_section(summary, cfg),
           sensitivity_section(summary, cfg), comparisons_section(summary, cfg)]
    ens = (summary.get("model") or {}).get("kind") == "ensemble"
    charts = []
    curves = extra.get("curves")
    if curves:
        for a in _focus_accounts(summary, cfg):
            charts.append(band_chart(curves, a, f"{_acct_zh(a, cfg)}：相对基线的累计 Δ收益（中位数与 5–95% 路径带）"))
    shown = [r for r in results if r.tier != "alert_only"] or list(results)
    for acct in [a for a in _focus_accounts(summary, cfg) if a in sets or (results and a in results[0].accounts)]:
        charts.append(equity_chart(shown, acct, f"{_acct_zh(acct, cfg)} 净值与回撤" + ("（路径 0）" if ens else ""), sets))
    out.append("<h2>6 净值与回撤</h2>" + "".join(charts))
    out.append(avoided_section(summary, cfg))
    out.append(tpfp_section(summary))
    out.append(bounding_section(summary))
    out.append(loo_section(summary, cfg))
    if summary.get("stress"):
        rows = [r for r in summary["stress"] if r["account_id"] in (TOTAL, STATIC, "ACC3")]
        out.append("<h3>压力窗口（从全区间回测中截取）</h3>" + stress_rows_table(rows))
    out.append(focus_charts(results, cfg))
    out.append(model_section(summary))
    out.append(roll_section(summary))
    out.append(perf_section(summary))
    out.append(disclosures(cfg, summary))
    checks = summary.get("checks") or {}
    if checks:
        out.append(f"<p class=\"note\">一致性检查：alert_only 与 no_rules 逐日盈亏完全相同 = {esc(checks.get('alert_only_equals_no_rules'))}"
                   f"（最大差 {esc(checks.get('max_abs_diff'))}）。</p>")
    return "".join(x for x in out if x)


def focus_charts(results: Sequence[ArmResult], cfg: BacktestConfig) -> str:
    days = sorted(cfg.analysis.focus_days)
    if cfg.stress is not None:
        days = sorted(set(days) | {w.focus_day for w in cfg.stress.windows if w.focus_day})
    ladder = cfg.rules.ladder
    out = []
    for d in days:
        for acct in (results[0].accounts if results else []):
            series = {r.arm: r.intraday.get(f"{d}|{acct}", []) for r in results if r.tier not in ("alert_only", "placebo")}
            if not any(series.values()):
                continue
            mins = [min(p for _t, p in s) for s in series.values() if s]
            if not mins or min(mins) > -cfg.rules.threshold * ladder.get("warn", 0.67):
                continue
            ths = {lv: -cfg.rules.threshold * m for lv, m in ladder.items() if lv in ("warn", "restrict", "halt")}
            out.append(intraday_chart(series, ths, f"{d} {acct} 盘中当日盈亏路径"))
    return "".join(out)


def render_stress(cfg: BacktestConfig, summary: Mapping[str, Any], results: Sequence[ArmResult]) -> str:
    out = ["<h2>压力窗口</h2>"]
    model = summary.get("model") or {}
    if model.get("kind") == "ensemble":
        out.append(f"<p>每个窗口 {model.get('paths')} 条随机日内路径：表中为中位数，方括号为路径 5–95%。盘中曲线来自路径 0。</p>")
    skipped = summary.get("skipped") or []
    if skipped:
        out.append("<p>未覆盖：" + "；".join(f"{esc(s.get('id'))}（{esc(s.get('reason'))}）" for s in skipped) + "</p>")
    ladder = cfg.rules.ladder
    by_group: dict[str, list[ArmResult]] = {}
    for r in results:
        by_group.setdefault(str(r.stats.get("group") or ""), []).append(r)
    for w in summary.get("windows", []):
        rows = [r for r in summary.get("table", []) if r["window"] == w["id"]]
        out.append(f"<h3>{esc(w['title'])}（{esc(w['start'])}–{esc(w['end'])}）</h3>")
        if w.get("note"):
            out.append(f"<p>{esc(w['note'])}</p>")
        out.append(stress_rows_table(rows))
        fd = w.get("focus_day")
        res = by_group.get(f"win:{w['id']}", [])
        if fd and res:
            for acct in res[0].accounts:
                series = {r.arm: r.intraday.get(f"{fd}|{acct}", []) for r in res if r.tier not in ("alert_only", "placebo")}
                if any(series.values()):
                    ths = {lv: -cfg.rules.threshold * m for lv, m in ladder.items() if lv in ("warn", "restrict", "halt")}
                    out.append(intraday_chart(series, ths, f"{fd} {acct} 盘中当日盈亏"))
            focus = [f for f in summary.get("focus", []) if f["window"] == w["id"]]
            if focus:
                out.append(table(["方案", "账户", "当日收益(结算)", "当日收益(收盘)", "盘中最低", "首个状态", "收盘", "熔断", "锁存",
                                  "被拒开仓", "界定分类"],
                                 [[f["arm"], f["account_id"], (fmt_pct(f.get("ret")), _signed_cls(f.get("ret"))),
                                   fmt_pct(f.get("close_ret")), fmt_pct(f.get("intraday_min_pct")), fmt_pct(f.get("open_pct")),
                                   fmt_pct(f.get("close_pct")), "是" if f.get("halted") else "", "是" if f.get("halt_latched") else "",
                                   _num(f.get("blocked_opens"), "{:,.0f}"), esc(f.get("class_halt"))] for f in focus], left=2))
    out.append(disclosures(cfg, summary))
    return "".join(out)


def render_sweep(cfg: BacktestConfig, summary: Mapping[str, Any]) -> str:
    model = summary.get("model") or {}
    out = ["<h2>阈值 × 动作档位扫描</h2>",
           "<p>每个格子是一次独立回测（同一数据与种子），效果相对同期基线。" +
           (f"每个格子 {model.get('paths')} 条随机日内路径，格内为中位数。" if model.get("kind") == "ensemble" else "") +
           "Calmar 只在基线与方案的 CAGR 都为正时给出差值（负收益的 Calmar 没有意义）。</p>"]
    ths = summary.get("thresholds", [])
    tiers = [t for t in summary.get("tiers", []) if t != "alert_only"]
    views = [("d_total_ret", 1.0, 100.0, "Δ总收益（百分点）", "{:+.1f}"), ("d_max_dd", -1.0, 100.0, "−Δ最大回撤（百分点）", "{:+.2f}"),
             ("d_calmar", 1.0, 1.0, "ΔCalmar（CAGR>0 时）", "{:+.2f}")]
    for per, g in summary.get("grid", {}).items():
        cells = g.get("cells", {})
        for acct in (cfg.sweep.focus_accounts if cfg.sweep else [TOTAL]):
            for key, sign, scale, title, fmt in views:
                vals = []
                for tier in tiers:
                    row: list[float | None] = []
                    for t in ths:
                        c = cells.get(f"{tier}|{t}", {}).get(acct)
                        v = c.get(key) if c else None
                        row.append(None if v is None else sign * scale * float(v))
                    vals.append(row)
                if any(v is not None for r in vals for v in r):
                    out.append(heatmap(vals, tiers, [f"{t * 100:g}%" for t in ths], f"{per} {acct}：{title}", fmt))
    an = summary.get("analysis", {})
    rows = []
    for per, items in an.items():
        for k, v in items.items():
            if "skipped" in v:
                rows.append([per, esc(k), "—", "—", "—", "—", esc(v["skipped"])])
                continue
            rows.append([per, esc(k), f"{float(v['best_threshold']) * 100:g}%" if v.get("best_threshold") is not None else "—",
                         "是" if v.get("edge_optimum") else "否", esc(v.get("monotone")),
                         {True: "是", False: "否", None: "—"}[v.get("plateau")], esc(v.get("reason"))])
    out.append("<h3>最优阈值的位置与 3% 是否处在平台区</h3>" +
               table(["区间", "档位|账户|目标", "最优阈值", "在网格边缘", "单调", "3% 在平台", "判定"], rows, left=2))
    st = summary.get("stability", {})
    if st:
        out.append(table(["档位|账户|目标", "样本内外排序相关 (Spearman)", "样本内最优", "样本外最优"],
                         [[esc(k), _num(v.get("spearman")), f"{(v.get('best_in') or 0) * 100:g}%", f"{(v.get('best_out') or 0) * 100:g}%"]
                          for k, v in st.items()], left=1))
    out.append('<p class="note">平台判定：3% 与相邻阈值之间的局部斜率不超过整个网格平均斜率（全距 ÷ 阈值跨度）的一半，且 3% 的目标值与最优值'
               "之差不超过全距的四分之一；一条直线（处处斜率相同）因此不算平台。最优阈值在网格边缘时，网格没有把最优值包住，"
               "只能说“更紧/更松的阈值更好”，不能说 3% 最优。Spearman 使用平均秩处理并列，输入为常数时无定义（—）。</p>")
    out.append(disclosures(cfg, summary))
    return "".join(out)


def render_scenario(cfg: BacktestConfig, summary: Mapping[str, Any], results: Sequence[ArmResult]) -> str:
    out = []
    checks = summary.get("checks", [])
    passed = sum(1 for c in checks if c["ok"])
    out.append(f"<h2>合成剧本（D 层）：{passed}/{len(checks)} 项检查通过</h2>")
    ladder = cfg.rules.ladder
    for sc in summary.get("scenarios", []):
        out.append(f"<h3>{esc(sc['name'])}</h3><p>{esc(sc.get('description'))}</p>")
        rows = [[esc(c["arm"]), esc(_check_text(c["check"])), ("通过", "ok") if c["ok"] else ("失败", "fail"), esc(c["detail"])]
                for c in checks if c["scenario"] == sc["name"]]
        if rows:
            out.append(table(["方案", "检查", "结果", "实际"], rows, left=2))
        for arm, a in sc["arms"].items():
            evs = [e for e in a["events"] if e["kind"] in ("ALERT_RAISED", "ALERT_CLEARED", "LIQUIDATION_PLAN", "MANUAL_HALT")]
            sts = a["states"]
            body = [[esc(e["step"]), esc(e["ts"])[11:19], e["kind"], esc(e["level"]), esc(e["action"]),
                     "是" if e["latched"] else "", fmt_pct(e["value"])] for e in evs]
            body += [[esc(s["step"]), esc(s["ts"])[11:19], "STATE", f"{s['from_state']}→{s['to_state']}", esc(s["restrictions"]),
                      "是" if s["halted_latched"] else "", ""] for s in sts]
            body.sort(key=lambda r: r[1])
            out.append(f"<h3>{esc(sc['name'])} / {esc(arm)} 事件时间线</h3>")
            out.append(table(["步骤", "时间", "类型", "级别/状态", "动作/限制", "锁存", "当日盈亏"], body, left=1))
            sc_rows = [[esc(o["label"]), esc(o["step"]), o["instrument_id"], o["direction"], o["offset"], o["qty"],
                        esc(o["code"] or "OK"), esc(",".join(o["statuses"]))] for o in a["scripted"]]
            if sc_rows:
                out.append(table(["脚本委托", "步骤", "合约", "方向", "开平", "手数", "结果", "委托状态"], sc_rows, left=2))
        series = {}
        for arm, a in sc["arms"].items():
            for _k, pts in a["intraday"].items():
                series[arm] = [(int(t), float(p)) for t, p in pts]
        if series:
            ths = {lv: -cfg.rules.threshold * m for lv, m in ladder.items() if lv in ("warn", "restrict", "halt")}
            out.append(intraday_chart(series, ths, f"{sc['name']}：当日盈亏路径"))
    return "".join(out)


def _check_text(c: Mapping[str, Any]) -> str:
    t = c.get("type")
    if t == "event":
        return f"{c.get('rule', 'daily_loss')}/{c['level']} 恰好 {c.get('n', 1)} 条 ALERT_RAISED，于 {c['step']}" + (
            f"，锁存={c['latched']}" if "latched" in c else "")
    if t == "no_event":
        return f"没有 {c.get('rule', 'daily_loss')}/{c['level']} 的 ALERT_RAISED"
    if t == "state":
        return f"状态首次变为 {c['to']}" + (f"（锁存={c['latched']}）" if "latched" in c else "") + f" 于 {c['step']}"
    if t == "order":
        return f"脚本委托 {c['label']} → {c['result']}"
    if t == "final_state":
        return f"最终状态 {c['state']}" + (f"，锁存={c['latched']}" if "latched" in c else "")
    if t == "count":
        return f"{c.get('kind', 'ALERT_RAISED')} {c.get('rule') or ''}/{c.get('level') or ''} 共 {c['n']} 条"
    return str(c)


def render_report(cfg: BacktestConfig, summary: Mapping[str, Any], results: Sequence[ArmResult], groups: Sequence[Any],
                  manifest: Mapping[str, Any], *, extra: Mapping[str, Any] | None = None) -> str:
    body = [header(cfg, manifest, summary)]
    kind = summary.get("kind")
    if kind == "arms":
        body.append(render_arms(cfg, summary, results, extra or {}))
    elif kind == "stress":
        body.append(render_stress(cfg, summary, results))
    elif kind == "sweep":
        body.append(render_sweep(cfg, summary))
    elif kind == "scenario":
        body.append(render_scenario(cfg, summary, results))
    notes = summary.get("notes") or []
    if notes:
        body.append("<h2>数据说明</h2><ul>" + "".join(f"<li>{esc(n)}</li>" for n in notes[:40]) + "</ul>")
    return ("<!doctype html><html lang=\"zh-CN\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" "
            "content=\"width=device-width,initial-scale=1\"><title>回测报告</title><style>" + CSS + "</style></head><body><main>" +
            "".join(body) + "</main></body></html>")


__all__ = ["render_report"]
