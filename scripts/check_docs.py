"""Mechanical cross-consistency checks for docs/README.md and every numbered doc.

Run from the repo root with the project venv: `.venv/Scripts/python scripts/check_docs.py`
(the table check imports the service schemas). Exit code 0 means no problems.
Checks: section cross-references resolve; every topic/command used is defined in
02 §5/§6; payload types referenced in 02 are defined; tables referenced in the docs
exist in 03; every table and column of the code schemas is documented in 03 with the
same nullability; every UPPER_SNAKE token is a declared enum value or code; every
`FUTRISK_*` variable is declared in 02 §3.1 or 11 §13 and read by the code; ports in
01 and 02 agree; no leftover authoring markers.
"""
from __future__ import annotations

import importlib
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DOCS = ROOT / "docs"
FILES = {"00": DOCS / "README.md"}
FILES.update({p.name[:2]: p for p in sorted(DOCS.glob("[0-9][0-9]-*.md"))})
text = {k: p.read_text(encoding="utf-8") for k, p in FILES.items()}
problems: list[str] = []


def section_ids(doc: str) -> set[str]:
    ids = set()
    for m in re.finditer(r"^#{2,5}\s+(?:附录\s*)?([0-9]+(?:\.[0-9]+)*)[.\s]", text[doc], re.M):
        ids.add(m.group(1).rstrip("."))
    return ids


sections = {k: section_ids(k) for k in FILES}

# ---- 1. cross references like "02 §4.7" or "(03 §4.0)" or bare "§8.2" (same doc)
for doc, body in text.items():
    for lineno, line in enumerate(body.splitlines(), 1):
        for cell in line.split("|"):
            for seg in re.split(r"[；;]", cell):
                carry = None  # a doc prefix carries over "、§x" continuations inside one segment
                for m in re.finditer(r"(README\s*|本文\s*|([0-9]{2})\s*)?§\s*([0-9]+(?:\.[0-9]+)*)", seg):
                    if m.group(1) and m.group(1).strip().startswith("README"):
                        carry = "00"
                    elif m.group(1) and m.group(1).strip().startswith("本文"):
                        carry = doc
                    elif m.group(2):
                        carry = m.group(2)
                    target = carry or doc
                    sec = m.group(3).rstrip(".")
                    if target not in sections:
                        problems.append(f"[xref] {doc}:{lineno} -> doc {target} does not exist")
                    elif sec not in sections[target]:
                        problems.append(f"[xref] {doc}:{lineno} -> {target} §{sec} not found")


def section_body(doc: str, num: str) -> str:
    body = text[doc]
    m = re.search(rf"^(#{{2,5}})\s+{re.escape(num)}[.\s].*$", body, re.M)
    if not m:
        return ""
    level = len(m.group(1))
    rest = body[m.end():]
    n = re.search(rf"^#{{2,{level}}}\s", rest, re.M)
    return rest[: n.start()] if n else rest


# ---- 2. canonical topics (02 §5) and commands (02 §6)
sec5 = section_body("02", "5")
topics = set(re.findall(r"^\|\s*`([a-z][a-z0-9_.{}]+)`", sec5, re.M))
sec6 = section_body("02", "6")
commands = set()
for m in re.finditer(r"^\|\s*((?:`[a-z][a-z0-9_.]+`(?:\s*/\s*)?)+)\s*\|", sec6, re.M):
    commands.update(re.findall(r"`([a-z][a-z0-9_.]+)`", m.group(1)))
print(f"topics={len(topics)} commands={len(commands)}")


def norm_topic(t: str) -> str:
    return re.sub(r"\{[a-z_]+\}", "{}", t)


topic_norm = {norm_topic(t) for t in topics}
topic_prefixes = {t.split("{")[0] for t in topics}
cmd_prefixes = {c.rsplit(".", 1)[0] + "." for c in commands}

# identifiers that look like topics/commands used anywhere
ident_re = re.compile(r"`((?:md|td|risk|oms|clearing|report|backtest|sys|ctl|order|route|settle|eod|cash|firm|svc)\.[a-z0-9_.{}*]+)`")
# old plan names (02 §13.1) and database file names are not topics/commands
known_extra = {"sys.config.reload", "eod.ack", "sys.shutdown", "td.order.{acct}", "td.trade.{acct}", "md.db", "td.db", "clearing.db", "api.db", "supervisor.db"}
for doc, body in text.items():
    for m in ident_re.finditer(body):
        ident = m.group(1).rstrip(".")
        if ident in known_extra or re.search(r"\.(html|json|csv|parquet|md|yaml|txt|py|log)$", ident):
            continue  # file names such as report.html are not topics
        n = norm_topic(ident).replace(".*", "")
        ok = (
            ident in commands
            or n in topic_norm
            or any(t.startswith(n) for t in topic_norm)  # prefix subscription like `md.tick.`
            or ident.endswith(".*") and any(c.startswith(ident[:-1]) for c in commands | topics)
            or re.fullmatch(r"[a-z]+\.\*", ident) is not None
            or any(n == p.rstrip(".") for p in topic_prefixes | cmd_prefixes)
            or re.sub(r"\.[A-Za-z0-9_]+$", ".{}", ident) in topic_norm  # concrete key, e.g. md.tick.rb2701
        )
        if not ok:
            line = body.count("\n", 0, m.start()) + 1
            problems.append(f"[name] {doc}:{line} unknown topic/command `{ident}`")

# ---- 3. payload types defined vs referenced (02)
body02 = text["02"]
defined = set(re.findall(r"\*\*`([A-Z][A-Za-z0-9]+)`\*\*", body02))
defined |= set(re.findall(r"`([A-Z][A-Za-z0-9]+)\{", body02))
defined |= set(re.findall(r"^\|\s*`([A-Z][A-Za-z0-9]+)`\s*\|", body02, re.M))
defined |= set(re.findall(r"^`([A-Z][A-Za-z0-9]+)`（", body02, re.M))
defined |= set(re.findall(r"^- `([A-Z][A-Za-z0-9]+)`", body02, re.M))
defined |= set(re.findall(r"`([A-Z][A-Za-z0-9]+)` 的字段", body02))
enum_names = set(re.findall(r"^\|\s*`([A-Z][A-Za-z]+)`\s*\|\s*`", section_body("02", "7.3"), re.M))
defined |= enum_names
builtins = {"FeedATick", "RtnOrder", "RtnTrade", "BrokerPosition", "CashDigest",  # external message types defined in 02 §9 tables
            "Struct", "Protocol", "Decimal", "DecStr", "Effect", "EngineCore", "Bus", "Clock", "MarketDataSource",
            "BrokerAdapter", "SettlementPriceProvider", "Notifier", "RuleDef", "CheckResult", "AccountDigest",
            "SnapshotItem", "LatencySummary", "FillRef", "AccountSettlementSummary", "SodAccount", "SodLot",
            "AccountSettlement", "PositionSettlement", "SettlementPriceView", "SourceStatus", "InstrumentStatus",
            "GapInfo", "AccountFunds", "AccountMetricsView", "ExposureView", "PositionView", "RuleStatus",
            "Restriction", "Envelope", "Reply"}
refs = set()
for sec in ("5", "6"):
    for m in re.finditer(r"`([A-Z][A-Za-z0-9]+)`", section_body("02", sec)):
        refs.add(m.group(1))
for m in re.finditer(r"list\[([A-Z][A-Za-z0-9]+)\]", body02):
    refs.add(m.group(1))
undefined = sorted(r for r in refs if r not in defined and r not in builtins
                   and not re.fullmatch(r"[A-Z_]+", r))
for u in undefined:
    problems.append(f"[type] 02 references type `{u}` but no definition found")

# ---- 4. tables defined (03) vs referenced in 01/02
body03 = text["03"]
tables = set()
for m in re.finditer(r"^####\s+(.+)$", body03, re.M):
    tables.update(re.findall(r"`([a-z_0-9]+)`", m.group(1)))
print(f"tables={len(tables)}")
table_ref_re = re.compile(r"`([a-z_]+)`\s*(?:表(?!示)|（03)")
for doc in (d for d in text if d != "03"):
    for m in table_ref_re.finditer(text[doc]):
        t = m.group(1)
        if t not in tables:
            line = text[doc].count("\n", 0, m.start()) + 1
            problems.append(f"[table] {doc}:{line} references table `{t}` not in 03")
for m in re.finditer(r"(?:risk|td|md|clearing|api|supervisor)\.`([a-z_]+)`", body03):
    if m.group(1) not in tables:
        line = body03.count("\n", 0, m.start()) + 1
        problems.append(f"[table] 03:{line} db.`{m.group(1)}` not a table")

# ---- 5. enum values: every UPPER token used somewhere must be declared somewhere
declared_upper = set()
for doc in ("02", "03"):
    for line in text[doc].splitlines():
        if line.startswith("|") and ("枚举" in line or re.search(r"\|\s*`[A-Z][A-Za-z]+`\s*\|\s*`[A-Z_]+`", line)):
            declared_upper.update(re.findall(r"`([A-Z][A-Z0-9_]+)`", line))
for sec in ("7.3", "11.1", "11.2", "11.3"):
    declared_upper.update(re.findall(r"`([A-Z][A-Z0-9_]+)`", section_body("02", sec)))
declared_upper.update(re.findall(r"`([A-Z][A-Z0-9_]+)`", section_body("03", "3.3")))
if "11" in text:  # 11 is generated from the config models: literal values in its type column are declared there
    for line in text["11"].splitlines():
        cells = re.split(r"(?<!\\)\|", line)  # a type like `A` \| `B` keeps its escaped pipes
        if line.startswith("| `") and len(cells) > 3:
            declared_upper.update(re.findall(r"`([A-Z][A-Z0-9_]+)`", cells[2]))
allow = {"OK", "MUST", "SHOULD", "MAY", "LV", "FACT", "CMD", "DBL_MAX", "PRAGMA", "NOT", "NULL", "FULL", "NORMAL",
         "WAL", "ON", "IMMEDIATE", "BEGIN", "DONE", "UP", "DOWN", "CLOSE", "OPEN", "SYSTEM", "RULE", "MANUAL",
         "RESET", "DAYROLL", "KILL_SWITCH", "HEARTBEAT_LOST", "EXITED", "KILLED_BY_DEMO", "SHUTDOWN", "STARTUP",
         "CERTAIN", "IMPOSSIBLE", "AMBIGUOUS", "INDEX", "TREASURY", "GFD", "IOC", "AV", "CV", "ANY", "LIMIT",
         "DELETE", "PART_TRADED", "ALL_TRADED", "EPOCH_CHANGED", "SEQ_EVICTED", "FEED_DISCONNECT", "FEED_GAP",
         "FEED_DUPLICATE", "FEED_REORDER", "FEED_LATENCY", "FEED_FREEZE", "BAD_TICK", "MALFORMED",
         "SIM_RESTART", "BROKER_DISCONNECT", "STEP", "LINEAR", "EXCLUDE", "INCLUDE", "ALL", "OPEN_ONLY",
         "CONFIRM", "CANCEL", "COARSE_TS", "AUCTION", "LIMIT_UP_LOCKED", "LIMIT_DOWN_LOCKED",
         "CIRCUIT_BREAK_CANCEL", "ROUTE_TIMEOUT", "SESSION_CLOSE", "EOD_FAILED", "SERVICE_FAILED",
         "FILL_GAP", "MD_UNKNOWN", "RECON_BREAK", "EOD_NOT_SETTLED", "STARTING", "GROSS", "EOD", "SVC",
         "RETRO", "UUIDv7", "CTP", "API", "SHA", "UTC", "JSON", "HTTP", "REST", "WS", "GET", "POST", "PUT",
         "HALT_OPEN", "RISK_BUSY", "BUSY",
         # formats, SQL words, ZMQ options and the DB logical type names of 03 §3.1
         "YYYYMMDD", "HHMMSS", "LINGER", "CHECK", "UUID", "INT", "DEC_PRICE", "DEC_MONEY", "DEC_AMOUNT",
         "DEC_RATE", "REAL", "BOOL", "BLOB", "HASH", "ENUM", "TEXT", "BROKER_FORCE_CLOSE", "ACKED",  # ACKED: old state name kept in 02 §13.1 rename table
         # log levels, ruff rule sets, CLI placeholders, the backtest's firm-total pseudo account, code constant names
         "TRACE", "DEBUG", "ERROR", "SIM", "TID", "TID251", "TYPE", "PATH", "TOTAL", "MIGRATIONS",
         "ALTER", "WARNINGS"}  # SQL keyword; the eod_runs.step_log key of 03 §4.4
ID_LIKE = re.compile(r"^(ACC\d+|[A-Z]{1,2}\d{4}|CFF_RE_\w*)$")  # account ids, contract codes, sina prefixes
for doc, body in text.items():
    for m in re.finditer(r"`([A-Z][A-Z0-9_]{2,})`", body):
        tok = m.group(1)
        if tok not in declared_upper and tok not in allow and not ID_LIKE.match(tok) and not tok.startswith("FUTRISK_"):
            line = body.count("\n", 0, m.start()) + 1
            problems.append(f"[enum] {doc}:{line} `{tok}` not declared as enum value / code")

# ---- 6. ports consistency 01 vs 02
ports01 = set(re.findall(r":(7\d{3}|8000)\b", text["01"]))
ports02 = set(re.findall(r":(7\d{3}|8000)\b", text["02"]))
if ports01 != ports02:
    problems.append(f"[ports] 01-only={sorted(ports01 - ports02)} 02-only={sorted(ports02 - ports01)}")

# ---- 7. leftover markers
for doc, body in text.items():
    for marker in ("APPEND-HERE", "TODO-FILL"):
        if marker in body:
            problems.append(f"[marker] {doc} still contains {marker}")

# ---- 8. environment variables: declared in 02 §3.1 or 11 §13, and read by the code
env_declared: set[str] = set()
for doc, sec in (("02", "3.1"), ("11", "13")):
    if doc in text:
        env_declared.update(re.findall(r"`(FUTRISK_[A-Z0-9_]+)`", section_body(doc, sec)))
# names a deployment chooses itself (app.yaml key_env / webhook_url_env), e.g. in 11's examples
configured_env = set(re.findall(r"(?:key_env|webhook_url_env):\s*(FUTRISK_[A-Z0-9_]+)", "\n".join(text.values())))
SKIP = {"check_docs.py", "gen_config_reference.py"}  # tools that only print the names
code_text = "\n".join(
    f.read_text(encoding="utf-8", errors="ignore")
    for d in ("src", "scripts", "deploy", "tests")
    for f in (ROOT / d).rglob("*")
    if f.suffix in {".py", ".yml", ".yaml", ".ps1"} and "__pycache__" not in f.parts and f.name not in SKIP
)
for doc, body in text.items():
    for m in re.finditer(r"\b(FUTRISK_[A-Z0-9_]*[A-Z0-9])\b", body):
        if m.group(1) not in env_declared | configured_env:
            line = body.count("\n", 0, m.start()) + 1
            problems.append(f"[env] {doc}:{line} `{m.group(1)}` not declared in 02 §3.1 or 11 §13")
for var in sorted(env_declared):
    if not re.search(rf"\b{var}\b", code_text):
        problems.append(f"[env] `{var}` is declared but no code reads it")

# ---- 9. against the code: tables and columns (03 §4), enums (02 §7.3), payload fields (02 §6/§7), codes (02 §11)
sys.path.insert(0, str(ROOT / "src"))
try:
    import enum
    import inspect

    import msgspec

    from futrisk.messages import codes as code_consts
except ImportError as exc:  # run with the project venv
    problems.append(f"[code] cannot import the project ({exc}); run with .venv/Scripts/python")
    code_consts = None

if code_consts is not None:
    # 9.1 tables: every code table documented in its 03 §4.x section, with the same columns and nullability
    SCHEMAS = {"4.0": "risk_engine", "4.1": "md_gateway", "4.2": "trade_gateway", "4.3": "risk_engine",
               "4.4": "clearing", "4.5": "api_gateway", "4.6": "supervisor"}
    common = set(re.findall(r"^####\s+`([a-z_0-9]+)`", section_body("03", "4.0"), re.M))
    compared = 0
    for sec, svc in SCHEMAS.items():
        meta = importlib.import_module(f"futrisk.services.{svc}.schema").METADATA
        code_tables = {n: t for n, t in meta.tables.items() if (n in common) == (sec == "4.0")}
        documented: set[str] = set()
        for part in re.split(r"^####\s+", section_body("03", sec), flags=re.M)[1:]:
            head, _, rest = part.partition("\n")
            in_head = re.findall(r"`([a-z_0-9]+)`", head)
            names = [n for n in in_head if n in code_tables] or in_head[:1]
            documented.update(names)
            rows = [r for r in rest.splitlines() if r.startswith("| `")]
            if not rows or not all(n in code_tables for n in names):
                continue  # e.g. "columns as in §4.3"
            cols: dict[str, set[bool]] = {}
            for n in names:
                for c in code_tables[n].columns:
                    cols.setdefault(c.name, set()).add(bool(c.nullable))
            doc_cols: dict[str, str] = {}
            for r in rows:
                cells = [c.strip() for c in r.strip().strip("|").split("|")]
                cnames = re.findall(r"`([a-z_0-9]+)`", cells[0])
                nulls = [x.strip() for x in cells[2].split("/")] if len(cells) > 2 else []
                for i, c in enumerate(cnames):
                    doc_cols[c] = nulls[i] if len(nulls) == len(cnames) else (nulls[0] if len(nulls) == 1 else "")
            label = "/".join(names)
            problems.extend(f"[column] 03 §{sec} `{label}` misses column `{c}`" for c in sorted(set(cols) - set(doc_cols)))
            problems.extend(f"[column] 03 §{sec} `{label}` documents `{c}`, not in the code"
                            for c in sorted(set(doc_cols) - set(cols)))
            for c, v in doc_cols.items():
                if c in cols and len(cols[c]) == 1 and v in ("是", "否"):
                    compared += 1
                    if (v == "是") != next(iter(cols[c])):
                        problems.append(f"[column] 03 §{sec} `{label}`.`{c}` nullable: doc {v}, "
                                        f"code {'是' if next(iter(cols[c])) else '否'}")
        problems.extend(f"[table] 03 §{sec} misses code table `{n}`" for n in sorted(set(code_tables) - documented))
        problems.extend(f"[table] 03 §{sec} documents `{n}`, not in the code" for n in sorted(documented - set(code_tables)))
    print(f"columns compared={compared}")

    # 9.2 futrisk.messages: enums (02 §7.3 or 03 §3.3) and Struct fields with required-ness (02 §6, §7)
    code_structs: dict[str, type] = {}
    code_enums: dict[str, type] = {}
    for mod in ("common", "enums", "envelope", "eod", "md", "orders", "risk", "system"):
        module = importlib.import_module(f"futrisk.messages.{mod}")
        for n, o in vars(module).items():
            if inspect.isclass(o) and o.__module__ == module.__name__:
                if issubclass(o, msgspec.Struct):
                    code_structs[n] = o
                elif issubclass(o, enum.Enum):
                    code_enums[n] = o
    enum_rows: dict[str, set[str]] = {}
    for doc, sec in (("02", "7.3"), ("03", "3.3")):
        for m in re.finditer(r"^\|\s*`([A-Z][A-Za-z]+)`\s*\|([^|]*)\|", section_body(doc, sec), re.M):
            enum_rows.setdefault(m.group(1), set()).update(re.findall(r"`([A-Za-z0-9_]+)`", m.group(2)))
    for n, e in sorted(code_enums.items()):
        values = {str(x.value) for x in e}
        if n not in enum_rows:
            problems.append(f"[enum-code] `{n}` ({len(values)} values) not in 02 §7.3 or 03 §3.3")
        elif values != enum_rows[n]:
            problems.append(f"[enum-code] `{n}`: code-only {sorted(values - enum_rows[n])}, "
                            f"doc-only {sorted(enum_rows[n] - values)}")

    def inline_fields(parts: list[str]) -> dict[str, bool]:
        """[`f: T`, `g: T?`, `h: T = v`] -> {field: required}."""
        out = {}
        for part in parts:
            f = re.match(r"\s*(\w+)\s*:\s*(.+?)\s*(=.*)?$", part)
            if f:
                out[f.group(1)] = not f.group(2).endswith("?") and f.group(3) is None
        return out

    def split_top(spec: str) -> list[str]:
        """Split at commas outside brackets: `a: dict[str, int], b: int` -> two parts."""
        parts, depth, cur = [], 0, ""
        for ch in spec:
            depth += (ch in "[({") - (ch in "])}")
            if ch == "," and depth == 0:
                parts.append(cur)
                cur = ""
            else:
                cur += ch
        return [*parts, cur]

    doc_fields: dict[str, dict[str, bool]] = {}
    for m in re.finditer(r"`([A-Z][A-Za-z0-9]+)\{([^`]*)\}`", body02):  # inline Name{f: T, ...} (§6)
        doc_fields.setdefault(m.group(1), inline_fields(split_top(m.group(2))))
    for m in re.finditer(r"^\|\s*`([A-Z][A-Za-z0-9]+)`\s*\|([^|]*`\w+:[^|]*)\|", body02, re.M):  # | `Name` | `f: T`；… |
        doc_fields.setdefault(m.group(1), inline_fields(re.findall(r"`([^`]+)`", m.group(2))))
    lines02 = body02.splitlines()
    for i, line in enumerate(lines02):  # **`Name`** or `Name`（…）： followed by a "| 字段 |" table (§4, §7)
        m = re.match(r"^(?:\*\*)?`([A-Z][A-Za-z0-9]+)`", line)
        if not m:
            continue
        j = i + 1
        while j < len(lines02) and not lines02[j].startswith(("|", "#", "**`", "`")):
            j += 1
        fields: dict[str, bool] = {}
        for row in lines02[j + 2:] if j < len(lines02) and lines02[j].startswith("| 字段") else []:
            if not row.startswith("|"):
                break
            cells = [c.strip() for c in row.strip().strip("|").split("|")]
            for f in re.findall(r"`(\w+)`", cells[0]):
                fields[f] = len(cells) > 2 and cells[2].startswith("是")
        if fields:
            doc_fields.setdefault(m.group(1), fields)
    fields_compared = 0
    for n, st in sorted(code_structs.items()):
        if n == "Msg":
            continue
        names = st.__struct_fields__
        defaults = dict(zip(names[len(names) - len(st.__struct_defaults__):], st.__struct_defaults__, strict=True))
        # a keyword-only field without a default is stored as NODEFAULT: still required
        required = {f: f not in defaults or defaults[f] is msgspec.NODEFAULT for f in names}
        doc_f = doc_fields.get(n)
        if doc_f is None:
            problems.append(f"[field] `{n}` has no field definition in 02")
            continue
        fields_compared += len(doc_f)
        if set(doc_f) != set(names):
            problems.append(f"[field] `{n}`: code-only {sorted(set(names) - set(doc_f))}, "
                            f"doc-only {sorted(set(doc_f) - set(names))}")
        wrong = sorted(f for f in set(doc_f) & set(names) if doc_f[f] != required[f])
        if wrong:
            problems.append(f"[field] `{n}`: required-ness differs for {wrong} "
                            f"(required in code: {[f for f in wrong if required[f]]})")
    print(f"structs={len(code_structs) - 1} fields compared={fields_compared} enums={len(code_enums)}")

    # 9.3 codes: the constants of futrisk.messages.codes and the codes listed in 02 §11
    doc_codes: set[str] = set()
    for sec in ("11.1", "11.2", "11.3"):
        for row in section_body("02", sec).splitlines():
            cells = row.split("|")
            if row.startswith("|") and len(cells) > 3:  # the code column (11.2 has the pipeline step first)
                doc_codes.update(re.findall(r"`([A-Z][A-Z0-9_]+)`", cells[2 if sec == "11.2" else 1]))
    consts = {v for k, v in vars(code_consts).items() if k.isupper() and isinstance(v, str)}
    src_text = "\n".join(f.read_text(encoding="utf-8") for f in (ROOT / "src").rglob("*.py"))
    problems.extend(f"[code] `{c}` (futrisk.messages.codes) not listed in 02 §11" for c in sorted(consts - doc_codes))
    problems.extend(f"[code] 02 §11 lists `{c}`, which no source file produces"
                    for c in sorted(doc_codes - consts) if not re.search(rf"[\"']{c}[\"']", src_text))

print(f"\n{len(problems)} problem(s)")
for p in problems:
    print(" -", p)
sys.exit(1 if problems else 0)
