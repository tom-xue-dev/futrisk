"""Render every report file of one settlement (process-pool side, 03 §5.3).

=====================  ===============================================  ==============================
type                   path (relative to ``<home>``)                    content
=====================  ===============================================  ==============================
``RISK_DAILY_HTML``    ``reports/{td}/risk_daily.html``                 self-contained, printable HTML
``STATEMENT_TXT``      ``reports/{td}/statement_{account}.txt``         CTP-style text statement
``STATEMENT_CSV``      ``reports/{td}/statement_{account}.csv``         structured statement, UTF-8 BOM
``SETTLEMENT_JSON``    ``reports/{td}/settlement.json``                 the full ``SettlementResult``
=====================  ===============================================  ==============================

Files are written atomically (a uniquely named temporary file + rename,
:func:`.fsutil.replace_file`); a new settlement version overwrites the same
names (03 §5.3). The function returns one descriptor per file with its SHA-256
and size; the service registers them.

``{account}`` is the account id made safe for a file name
(:func:`safe_component`: only ``[A-Za-z0-9_.-]``, never ``.``/``..``, and a
short hash suffix whenever characters had to be replaced, so two ids never
share a file). The service does this itself even though ``accounts.yaml``
validates the ids, so a path never leaves ``reports/{td}/``.

The Jinja2 template is package data read with :mod:`importlib.resources`
(``futrisk.services.clearing/templates/risk_daily.html.j2``), never located
through ``__file__``, so a wheel, a zip import or a frozen build finds it;
:func:`check_template` lets the service fail fast at start-up when it is missing.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import uuid
from decimal import Decimal
from importlib import resources
from pathlib import Path
from typing import Any

import msgspec

from futrisk.messages.enums import ReportType
from futrisk.services.clearing.fsutil import replace_file
from futrisk.services.clearing.reports import charts, model
from futrisk.services.clearing.reports.model import ReportBundle
from futrisk.services.clearing.reports.sections import SECTION_TITLES
from futrisk.services.clearing.reports.statement import render_statement_csv, render_statement_txt

TEMPLATE_PACKAGE = "futrisk.services.clearing"
TEMPLATE_NAME = "risk_daily.html.j2"


def template_source(name: str = TEMPLATE_NAME) -> str:
    """Source of a report template, read as package data (``importlib.resources``)."""
    return (resources.files(TEMPLATE_PACKAGE) / "templates" / name).read_text(encoding="utf-8")


def check_template(name: str = TEMPLATE_NAME) -> int:
    """Raise when the template cannot be read (called at service start); returns its length."""
    text = template_source(name)
    if not text.strip():
        raise FileNotFoundError(f"report template {name} is empty")
    return len(text)


_UNSAFE = re.compile(r"[^A-Za-z0-9_.-]")


def safe_component(text: str) -> str:
    """``text`` usable as one file-name component (see the module docstring)."""
    cleaned = _UNSAFE.sub("_", text).strip(".")[:64]
    if cleaned == text and cleaned:
        return cleaned
    suffix = hashlib.sha256(text.encode("utf-8")).hexdigest()[:8]
    return f"{cleaned or 'account'}-{suffix}"


def _write(path: Path, data: bytes) -> dict[str, Any]:
    path.parent.mkdir(parents=True, exist_ok=True)
    # 临时文件名唯一：同一文件的两次生成不会互相覆盖临时文件
    tmp = path.with_name(f"{path.name}.{os.getpid()}.{uuid.uuid4().hex[:8]}.tmp")
    try:
        tmp.write_bytes(data)
        replace_file(tmp, path)  # api-gateway may be serving the previous version
    finally:
        if tmp.exists():
            tmp.unlink()
    return {"sha256": hashlib.sha256(data).hexdigest(), "size_bytes": len(data)}


def _halt_threshold(b: ReportBundle) -> float | None:
    for rule in b.rules:
        if rule.get("metric") == "account.daily_pnl_pct_init":
            for level in rule.get("levels") or []:
                if level.get("action") == "CIRCUIT_BREAK":
                    return float(level.get("threshold"))
    return None


def build_daily_context(b: ReportBundle, *, with_charts: bool = True) -> dict[str, Any]:
    """Everything the ``risk_daily.html.j2`` template needs (numbers already formatted)."""
    funds, total = model.funds_rows(b)
    exps = model.exposures(b)
    products, pnl_prod, prod_totals = model.pnl_by_product(b)
    sectors, pnl_sector, sector_totals = model.pnl_by_sector(b)
    grid_products, grid_rows = model.exposure_grid(b, exps)
    limits = model.limit_rows(b, funds, exps)
    dq = model.data_quality(b)
    system = model.system_rows(b)
    stress = model.stress_rows(b)
    checks = b.settlement.checks
    check_counts: dict[str, int] = {}
    for c in checks:
        check_counts[c.status.value] = check_counts.get(c.status.value, 0) + 1
    initial_total = total.initial_capital
    intraday: dict[str, list[tuple[int, float]]] = {
        acct: [(p.ts_ns, float(p.daily_pnl_pct_init)) for p in pts] for acct, pts in b.metrics.items()
    }
    history = {
        acct: [(h["trading_day"], float(Decimal(h["nav"])), float(Decimal(h["max_drawdown"]))) for h in rows]
        for acct, rows in b.history.items()
    }
    chart_intraday = chart_nav = None
    if with_charts:
        chart_intraday = charts.intraday_small_multiples(intraday, threshold=_halt_threshold(b))
        chart_nav = charts.nav_drawdown(history)
    return {
        "b": b,
        "m": model,
        "titles": SECTION_TITLES,
        "header": {
            "trading_day": model.td_label(b.trading_day),
            "session": model.night_label(b),
            "version": b.version,
            "next_trading_day": model.td_label(b.next_trading_day) if b.next_trading_day else "—",
            "config_version": b.config_version,
            "config_hash": b.config_hash,
            "generated": model.generated_label(b),
            "triggered_by": b.triggered_by,
            "phases": {k: model.ymd_hms(v) for k, v in sorted(b.phases.items(), key=lambda kv: kv[1])},
        },
        "prices": model.price_rows(b),
        "price_sources": model.price_source_counts(b),
        "funds": funds,
        "total": total,
        "day_pnl_pct": model.ratio(total.day_pnl, initial_total) if initial_total else None,
        "sentences": model.key_sentences(b, funds, exps),
        "check_counts": dict(sorted(check_counts.items())),
        "checks": [c for c in checks if c.status.value != "PASS"],
        "products": products,
        "pnl_prod": pnl_prod,
        "prod_totals": prod_totals,
        "sectors": sectors,
        "pnl_sector": pnl_sector,
        "sector_totals": sector_totals,
        "exposures": exps,
        "grid_products": grid_products,
        "grid_rows": grid_rows,
        "margin_groups": b.settlement.margin_groups,
        "limits": limits,
        "events": model.events_sorted(b),
        "rejected": model.rejected_orders(b),
        "dq": dq,
        "system": system,
        "svc_events": sorted(b.svc_events, key=lambda e: e.ts_ns)[-30:],
        "stress": stress,
        "chart_intraday": chart_intraday,
        "chart_nav": chart_nav,
    }


def render_daily_html(b: ReportBundle, *, with_charts: bool = True) -> str:
    import jinja2

    env = jinja2.Environment(
        loader=jinja2.FunctionLoader(template_source),
        autoescape=jinja2.select_autoescape(["html", "j2"]),
        trim_blocks=True,
        lstrip_blocks=True,
    )
    template = env.get_template(TEMPLATE_NAME)
    return template.render(**build_daily_context(b, with_charts=with_charts))


def render_all(bundle: dict[str, Any], home: str) -> list[dict[str, Any]]:
    """Write all report files of the bundle's settlement; returns descriptors (type, account, path, hash, size)."""
    b = ReportBundle.from_dict(bundle)
    root = Path(home)
    rel_dir = Path("reports") / b.trading_day
    out: list[dict[str, Any]] = []

    def emit(kind: ReportType, name: str, data: bytes, account_id: str | None = None) -> None:
        rel = (rel_dir / name).as_posix()
        meta = _write(root / rel, data)
        out.append({"type": kind.value, "account_id": account_id, "path": rel, **meta})

    emit(ReportType.RISK_DAILY_HTML, "risk_daily.html", render_daily_html(b).encode("utf-8"))
    for acct in b.account_ids():
        name = safe_component(acct)
        emit(ReportType.STATEMENT_TXT, f"statement_{name}.txt", render_statement_txt(b, acct).encode("utf-8"), acct)
        emit(
            ReportType.STATEMENT_CSV,
            f"statement_{name}.csv",
            render_statement_csv(b, acct).encode("utf-8-sig"),
            acct,
        )
    doc = full_builtins(b.settlement)  # 03 §5.3：同 SettlementResult（含默认值字段，便于外部系统对接）
    emit(ReportType.SETTLEMENT_JSON, "settlement.json", json.dumps(doc, ensure_ascii=False, indent=2).encode("utf-8"))
    return out


def full_builtins(obj: Any) -> Any:
    """Like ``msgspec.to_builtins`` but keeps fields equal to their defaults (``omit_defaults`` structs)."""
    if isinstance(obj, msgspec.Struct):
        return {f: full_builtins(getattr(obj, f)) for f in obj.__struct_fields__}
    if isinstance(obj, dict):
        return {str(k): full_builtins(v) for k, v in obj.items()}
    if isinstance(obj, list | tuple):
        return [full_builtins(v) for v in obj]
    return msgspec.to_builtins(obj)


__all__ = [
    "TEMPLATE_NAME",
    "TEMPLATE_PACKAGE",
    "build_daily_context",
    "check_template",
    "render_all",
    "render_daily_html",
    "safe_component",
    "template_source",
]
