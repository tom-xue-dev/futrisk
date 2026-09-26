"""Report charts rendered with matplotlib's Agg backend through the object API (01 §5.3.4 图表).

No ``pyplot`` global state is used, so rendering is safe in worker processes
and threads alike. Every chart is returned as a ``data:image/png;base64,...``
URI embedded in the self-contained HTML (03 §5.3).
"""

from __future__ import annotations

import base64
import io
import math
from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any

from futrisk.common.clock import BJ

_PALETTE = ("#2a6fb0", "#d0642c", "#3c9a5f", "#8e5ab5", "#b8912f", "#c0405a", "#4a9aa6", "#6b6b6b")


def _figure(width: float, height: float) -> Any:
    import matplotlib

    matplotlib.use("Agg", force=False)
    from matplotlib.backends.backend_agg import FigureCanvasAgg
    from matplotlib.figure import Figure

    fig = Figure(figsize=(width, height), dpi=110)
    FigureCanvasAgg(fig)
    _cjk_font()
    return fig


_FONT_READY = False


def _cjk_font() -> None:
    """Prefer a CJK-capable font when one is installed (titles contain Chinese)."""
    global _FONT_READY
    if _FONT_READY:
        return
    from matplotlib import font_manager, rcParams

    names = {f.name for f in font_manager.fontManager.ttflist}
    for candidate in ("Microsoft YaHei", "SimHei", "Noto Sans CJK SC", "Source Han Sans SC", "WenQuanYi Zen Hei"):
        if candidate in names:
            rcParams["font.sans-serif"] = [candidate, *rcParams.get("font.sans-serif", [])]
            break
    rcParams["axes.unicode_minus"] = False
    _FONT_READY = True


def _to_uri(fig: Any) -> str:
    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    return "data:image/png;base64," + base64.b64encode(buf.getvalue()).decode("ascii")


def intraday_small_multiples(
    series: Mapping[str, Sequence[tuple[int, float]]], *, threshold: float | None = None, title: str = ""
) -> str | None:
    """One small panel per account: intraday P&L / initial capital (%) with the halt line."""
    accounts = [a for a, pts in sorted(series.items()) if pts]
    if not accounts:
        return None
    cols = min(3, len(accounts))
    rows = math.ceil(len(accounts) / cols)
    fig = _figure(3.4 * cols, 2.3 * rows)
    for i, acct in enumerate(accounts):
        ax = fig.add_subplot(rows, cols, i + 1)
        pts = series[acct]
        xs = [datetime.fromtimestamp(t / 1e9, tz=BJ) for t, _ in pts]
        ys = [v * 100 for _, v in pts]
        color = _PALETTE[i % len(_PALETTE)]
        ax.plot(xs, ys, color=color, linewidth=1.2)
        ax.fill_between(xs, ys, 0, color=color, alpha=0.12)
        ax.axhline(0, color="#999999", linewidth=0.6)
        if threshold is not None:
            ax.axhline(threshold * 100, color="#c0405a", linewidth=0.8, linestyle="--")
        ax.set_title(acct, fontsize=9)
        ax.tick_params(labelsize=7)
        ax.yaxis.set_major_formatter(_pct_formatter())
        for label in ax.get_xticklabels():
            label.set_rotation(0)
        ax.xaxis.set_major_formatter(_time_formatter())
        ax.grid(True, linewidth=0.3, alpha=0.5)
    if title:
        fig.suptitle(title, fontsize=10)
    fig.tight_layout()
    return _to_uri(fig)


def nav_drawdown(history: Mapping[str, Sequence[tuple[str, float, float]]], *, title: str = "") -> str | None:
    """60-day NAV lines per account (top) and drawdown from the high-water mark (bottom)."""
    accounts = [a for a, pts in sorted(history.items()) if pts]
    if not accounts:
        return None
    fig = _figure(7.6, 4.6)
    ax1 = fig.add_subplot(2, 1, 1)
    ax2 = fig.add_subplot(2, 1, 2, sharex=ax1)
    days = sorted({d for a in accounts for d, _, _ in history[a]})
    index = {d: i for i, d in enumerate(days)}
    for i, acct in enumerate(accounts):
        pts = history[acct]
        xs = [index[d] for d, _, _ in pts]
        color = _PALETTE[i % len(_PALETTE)]
        ax1.plot(xs, [n for _, n, _ in pts], color=color, linewidth=1.3, marker="o", markersize=2.5, label=acct)
        ax2.plot(xs, [-dd * 100 for _, _, dd in pts], color=color, linewidth=1.1, marker="o", markersize=2)
    ax1.set_ylabel("NAV", fontsize=8)
    ax2.set_ylabel("距高水位回撤", fontsize=8)
    ax1.ticklabel_format(axis="y", useOffset=False, style="plain")
    ax1.yaxis.set_major_formatter(_fixed_formatter(4))
    ax2.yaxis.set_major_formatter(_pct_formatter())
    lo = min((-dd * 100 for a in accounts for _, _, dd in history[a]), default=0.0)
    ax2.set_ylim(min(lo * 1.15, -0.5), 0.05)
    navs = [n for a in accounts for _, n, _ in history[a]]
    if navs and max(navs) - min(navs) < 0.005:  # tiny range: keep the axis readable
        mid = (max(navs) + min(navs)) / 2
        ax1.set_ylim(mid - 0.005, mid + 0.005)
    if len(days) == 1:
        ax1.set_xlim(-1, 1)
    ax1.legend(fontsize=7, ncol=min(5, len(accounts)), loc="upper center", bbox_to_anchor=(0.5, 1.22), frameon=False)
    ax1.grid(True, linewidth=0.3, alpha=0.5)
    ax2.grid(True, linewidth=0.3, alpha=0.5)
    ticks = list(range(0, len(days), max(1, len(days) // 8)))
    ax2.set_xticks(ticks)
    ax2.set_xticklabels([f"{days[t][4:6]}-{days[t][6:]}" for t in ticks], fontsize=7)
    ax1.tick_params(labelsize=7)
    ax2.tick_params(labelsize=7)
    if title:
        fig.suptitle(title, fontsize=10)
    fig.tight_layout()
    return _to_uri(fig)


def _pct_formatter() -> Any:
    from matplotlib.ticker import FuncFormatter

    return FuncFormatter(lambda v, _pos: f"{v:.1f}%")


def _fixed_formatter(digits: int) -> Any:
    from matplotlib.ticker import FuncFormatter

    return FuncFormatter(lambda v, _pos: f"{v:.{digits}f}")


def _time_formatter() -> Any:
    from matplotlib.dates import DateFormatter

    return DateFormatter("%H:%M", tz=BJ)  # type: ignore[no-untyped-call]


__all__ = ["intraday_small_multiples", "nav_drawdown"]
