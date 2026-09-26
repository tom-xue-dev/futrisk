"""Shared plumbing of the operator/tools CLI (01 §7.1; plan §9 "CLI").

``futrisk positions | accounts | order | risk | settle | report | fault | shock | scenario | clock``
talk to api-gateway's REST API (02 §9.6), the sim-exchange controls included (``/demo/*``,
authenticated and audited); only an older api-gateway without a ``/demo`` read route makes
``fault list|clear --all`` and ``scenario list|cancel`` fall back to the sim-exchange control API
(02 §9.5, localhost, demo only). ``futrisk doctor`` checks the local environment.

* ``--home`` picks the runtime home (committed config, API key); ``--api`` overrides the
  api-gateway base URL (default: ``instances.api-gateway.http`` of ``services.yaml``); ``--json``
  prints the raw JSON answer instead of tables;
* the API key is ``FUTRISK_API_KEY`` or ``<home>/secrets/bootstrap_admin.key`` (as ``futrisk demo``);
  the bootstrap key is only sent to an api-gateway whose ``/health`` ``home_id`` is this home's;
* a failed call prints ``CODE: message`` (the API's code) and exits with code 1;
* numbers are printed as the API wrote them: JSON floats are parsed as :class:`~decimal.Decimal`
  and decimal strings are passed through, so money is never re-formatted through binary floats
  (only ratios are shown as percentages);
* times are Beijing market time (``Asia/Shanghai``), whatever the machine's time zone.

Import-light on purpose: :mod:`futrisk.main` imports the command modules on every invocation
(each ``futrisk svc`` child included), so httpx, msgspec and rich load only when a command runs.
"""

from __future__ import annotations

import contextlib
import os
import re
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any

import typer

from futrisk.cli.ops_cmd import (
    API_KEY_ENV,
    API_PREFIX,
    BOOTSTRAP_KEY_FILE,
    OpsContext,
    load_context,
    read_api_key,
)

if TYPE_CHECKING:
    from rich.console import Console

    from futrisk.cli.apiclient import ApiClient, SimControlClient
    from futrisk.common.config import FullConfig
    from futrisk.common.paths import HomeLayout

# 02 §11.1 codes used by the CLI itself
BAD_REQUEST = "BAD_REQUEST"
UNAUTHORIZED = "UNAUTHORIZED"
FORBIDDEN = "FORBIDDEN"
NOT_FOUND = "NOT_FOUND"
CONFLICT = "CONFLICT"
UNAVAILABLE = "UNAVAILABLE"
TIMEOUT = "TIMEOUT"

DEMO_HINT = "demo control needs app.mode=demo (config/app.yaml of the running home)"

HomeOpt = typer.Option(None, "--home", help="Runtime home directory (default: FUTRISK_HOME or the default home)")
ApiOpt = typer.Option(
    None, "--api", help="api-gateway base URL (default: the api-gateway http endpoint of services.yaml)"
)
JsonOpt = typer.Option(False, "--json", help="Print the raw JSON answer instead of tables")
TdOpt = typer.Option(None, "--td", help="Trading day YYYYMMDD (or YYYY-MM-DD)")

_NUMERIC = re.compile(r"^[-+]?(\d[\d,]*(\.\d+)?|\.\d+)([eE][-+]?\d+)?%?$")
_STATE_STYLE = {
    "NORMAL": "green",
    "WARNING": "yellow",
    "RESTRICTED": "dark_orange",
    "HALTED": "bold red",
    "OK": "green",
    "STALE": "yellow",
    "UNKNOWN": "dim",
    "ACTIVE": "yellow",
    "DONE": "green",
    "FAILED": "bold red",
    "REJECTED": "red",
    "BROKER_REJECTED": "red",
    "FILLED": "green",
    "CANCELED": "dim",
    "PASS": "green",
    "WARN": "yellow",
    "FAIL": "bold red",
    "INFO": "cyan",
    "CRITICAL": "bold red",
    "HIGH": "red",
}


class CliError(Exception):
    """A failed operation: printed as ``CODE: message`` and the command exits with code 1.

    ``raw`` is the API's envelope (printed as is with ``--json``); ``hint`` is an extra line.
    """

    def __init__(
        self,
        code: str,
        message: str,
        *,
        status: int | None = None,
        raw: bytes | None = None,
        data: Any = None,
        hint: str | None = None,
    ) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message
        self.status = status
        self.raw = raw
        self.data = data
        self.hint = hint


# ====================================================================== formatting
def as_dict(value: Any) -> dict[str, Any]:
    """``value`` when it is a JSON object, else ``{}`` (tolerant rendering of API data)."""
    return value if isinstance(value, dict) else {}


def dicts(value: Any) -> list[dict[str, Any]]:
    """The JSON objects of a JSON array (``[]`` for anything else)."""
    return [v for v in value if isinstance(v, dict)] if isinstance(value, list) else []


def fmt(value: Any) -> str:
    """A value as the API gave it (``Decimal`` keeps the JSON text); ``-`` for null."""
    if value is None or value == "":
        return "-"
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, dict):
        return kv(value)
    if isinstance(value, list):
        return ", ".join(fmt(v) for v in value) or "-"
    return str(value)


def to_decimal(value: Any) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def pct(value: Any, places: int = 2) -> str:
    """A ratio as a percentage (``-0.0302`` -> ``-3.02%``), computed exactly on the decimal text."""
    d = to_decimal(value)
    if d is None or not d.is_finite():
        return "-"
    q = (d * 100).quantize(Decimal(1).scaleb(-places), rounding=ROUND_HALF_UP)
    return f"{q}%"


def kv(mapping: Mapping[str, Any] | None) -> str:
    """``k=v k2=v2`` (compact, for params and details)."""
    if not mapping:
        return "-"
    return " ".join(f"{k}={fmt(v) if not isinstance(v, dict | list) else _compact(v)}" for k, v in mapping.items())


def _compact(value: Any) -> str:
    if isinstance(value, dict):
        return "{" + ",".join(f"{k}:{_compact(v)}" for k, v in value.items()) + "}"
    if isinstance(value, list):
        return "[" + ",".join(_compact(v) for v in value) + "]"
    return fmt(value)


def bj_time(ns: Any, *, date: bool = True) -> str:
    """Epoch ns as Beijing time ``YYYY-MM-DD HH:MM:SS`` (or ``HH:MM:SS``)."""
    if ns is None or isinstance(ns, bool):
        return "-"
    try:
        value = int(ns)
    except (TypeError, ValueError):
        return fmt(ns)
    if value <= 0:
        return "-"
    from futrisk.common.clock import ns_to_bj

    return ns_to_bj(value).strftime("%Y-%m-%d %H:%M:%S" if date else "%H:%M:%S")


def span(ns: Any) -> str:
    """Signed duration of ``ns`` nanoseconds, e.g. ``+2d 05:10:00``."""
    try:
        value = int(ns)
    except (TypeError, ValueError):
        return "-"
    sign = "-" if value < 0 else "+"
    s = abs(value) // 1_000_000_000
    d, rem = divmod(s, 86_400)
    h, rem = divmod(rem, 3600)
    m, sec = divmod(rem, 60)
    return f"{sign}{d}d {h:02d}:{m:02d}:{sec:02d}" if d else f"{sign}{h:02d}:{m:02d}:{sec:02d}"


def td_option(value: str | None, param: str = "--td") -> str | None:
    """``YYYYMMDD`` / ``YYYY-MM-DD`` -> ``YYYYMMDD``; ``BadParameter`` otherwise."""
    if value is None or value == "":
        return None
    text = value.strip().replace("-", "")
    if not re.fullmatch(r"\d{8}", text):
        raise typer.BadParameter(f"expected YYYYMMDD, got {value!r}", param_hint=param)
    return text


def parse_value(text: str) -> Any:
    """A ``k=v`` value: JSON (numbers, true/false/null, quoted strings, lists) or a plain string."""
    import json

    t = text.strip()
    if t.lower() in ("true", "false"):
        return t.lower() == "true"
    try:
        return json.loads(t)
    except ValueError:
        return t


def parse_kv(items: Sequence[str] | None, param: str) -> dict[str, Any]:
    """``["ratio=0.2", "instrument=rb2701"]`` -> ``{"ratio": 0.2, "instrument": "rb2701"}``."""
    out: dict[str, Any] = {}
    for item in items or []:
        key, sep, value = item.partition("=")
        key = key.strip()
        if not sep or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
            raise typer.BadParameter(f"expected KEY=VALUE, got {item!r}", param_hint=param)
        out[key] = parse_value(value)
    return out


def state_style(value: Any) -> str:
    return _STATE_STYLE.get(str(value), "")


# ====================================================================== JSON
_codec: dict[str, Any] = {}


def _msgspec_codec() -> tuple[Any, Any]:
    if not _codec:
        import msgspec

        _codec["dec"] = msgspec.json.Decoder(float_hook=Decimal)
        _codec["enc"] = msgspec.json.Encoder(decimal_format="number")
    return _codec["dec"], _codec["enc"]


def decode_json(raw: bytes) -> Any:
    """JSON with floats as ``Decimal`` (the exact text the API wrote); ``ValueError`` when invalid."""
    import msgspec

    try:
        return _msgspec_codec()[0].decode(raw)
    except msgspec.DecodeError as exc:
        raise ValueError(f"invalid JSON: {exc}") from exc


def dump_json(obj: Any) -> str:
    """Pretty JSON of decoded data (``Decimal`` back as JSON numbers, strings unchanged)."""
    import msgspec

    encoded: bytes = _msgspec_codec()[1].encode(obj)
    return msgspec.json.format(encoded, indent=2).decode("utf-8")


def pretty_json(raw: bytes) -> str:
    """Pretty-print a JSON body without decoding its values (numbers stay as written)."""
    import msgspec

    try:
        return msgspec.json.format(raw, indent=2).decode("utf-8")
    except (msgspec.DecodeError, ValueError):
        return raw.decode("utf-8", "replace")


# ====================================================================== output
Cell = Any  # str | Decimal | int | None | tuple[text, style]


class Out:
    """Output of one command: rich tables / lines on stdout, errors on stderr, JSON with ``--json``."""

    def __init__(self, as_json: bool = False) -> None:
        self.json = as_json
        self._out: Console | None = None
        self._err: Console | None = None

    def console(self, err: bool = False) -> Console:
        from rich.console import Console

        if err:
            if self._err is None:
                self._err = Console(stderr=True, highlight=False, soft_wrap=True)
            return self._err
        if self._out is None:
            self._out = Console(highlight=False, soft_wrap=True)
        return self._out

    # ---------------------------------------------------------------- lines
    def line(self, text: str = "", style: str | None = None, *, err: bool = False) -> None:
        from rich.text import Text

        self.console(err).print(Text(text, style=style or ""))

    def ok(self, text: str) -> None:
        self.line(text, "green")

    def warn(self, text: str) -> None:
        self.line(text, "yellow")

    def dim(self, text: str) -> None:
        self.line(text, "dim")

    def fields(self, pairs: Sequence[tuple[str, Cell]]) -> None:
        """Aligned ``label  value`` lines (values may be ``(text, style)``)."""
        from rich.text import Text

        width = max((len(k) for k, _ in pairs), default=0)
        for label, value in pairs:
            line = Text(f"{label:<{width}}  ")
            line.append_text(self._cell(value))
            self.console().print(line)

    def text(self, text: str) -> None:
        """Plain text exactly as given (statements)."""
        typer.echo(text, nl=not text.endswith("\n"))

    # ---------------------------------------------------------------- tables
    @staticmethod
    def _cell(value: Cell) -> Any:
        from rich.text import Text

        if isinstance(value, tuple) and len(value) == 2:
            return Text(fmt(value[0]), style=value[1] or "")
        if value is None or value == "":
            return Text("-", style="dim")
        return Text(fmt(value))

    def table(
        self,
        columns: Sequence[str],
        rows: Sequence[Sequence[Cell]],
        *,
        title: str | None = None,
        caption: str | None = None,
    ) -> None:
        """A table; columns whose cells all look numeric are right-aligned and never wrapped."""
        from rich import box
        from rich.table import Table

        table = Table(
            title=title,
            title_justify="left",
            caption=caption,
            caption_justify="left",
            header_style="bold",
            box=box.SIMPLE_HEAD,
            pad_edge=False,
        )
        cells = [[self._cell(v) for v in row] for row in rows]
        for i, name in enumerate(columns):
            values = [c[i].plain for c in cells if i < len(c) and c[i].plain not in ("-", "")]
            if bool(values) and all(_NUMERIC.match(v) for v in values):
                table.add_column(name, justify="right", no_wrap=True)
            else:
                table.add_column(name, justify="left", overflow="fold")
        for row in cells:
            table.add_row(*row)
        self.console().print(table)

    # ------------------------------------------------------------------ JSON
    def raw(self, raw: bytes) -> None:
        """The API's JSON body, pretty-printed without touching any value."""
        typer.echo(pretty_json(raw))

    def obj(self, obj: Any) -> None:
        typer.echo(dump_json(obj))

    # ---------------------------------------------------------------- errors
    def error(self, exc: CliError) -> None:
        if self.json:
            if exc.raw:
                self.raw(exc.raw)
            else:
                self.obj({"ok": False, "code": exc.code, "message": exc.message, "data": exc.data})
            return
        where = f" (HTTP {exc.status})" if exc.status and exc.status >= 400 else ""  # REJECT_* come with 200
        self.line(f"error: {exc.code}{where}: {exc.message}", "bold red", err=True)
        if exc.hint:
            self.line(f"hint: {exc.hint}", "yellow", err=True)


# ====================================================================== session
def resolve_api_url(ctx: OpsContext, api: str | None) -> str:
    """``--api`` or the api-gateway http endpoint of ``services.yaml`` (without ``/api/v1``)."""
    url = (api or ctx.api_url or "").strip().rstrip("/")
    if url.endswith(API_PREFIX):
        url = url[: -len(API_PREFIX)].rstrip("/")
    if not url:
        raise CliError(BAD_REQUEST, "services.yaml has no http endpoint for api-gateway; pass --api URL")
    if "://" not in url:
        url = f"http://{url}"
    return url


@dataclass
class Session:
    """What a tools command needs: the home's config, the API client and the output."""

    ctx: OpsContext
    api: ApiClient
    out: Out
    api_url: str
    _health: dict[str, Any] | None = None

    @property
    def layout(self) -> HomeLayout:
        return self.ctx.layout

    @property
    def cfg(self) -> FullConfig:
        return self.ctx.cfg

    def sim(self) -> SimControlClient:
        """Client of the sim-exchange control API (02 §9.5): demo mode only, localhost."""
        from futrisk.cli.apiclient import SimControlClient

        if self.cfg.app.mode != "demo":
            raise CliError(
                FORBIDDEN,
                "demo endpoints are only available in demo mode (app.mode=demo)",
                hint=f"app.mode is {self.cfg.app.mode!r} in {self.layout.config_dir}",
            )
        c = self.cfg.sim.control
        return SimControlClient(f"http://{c.host}:{c.port}/control")

    def health(self) -> dict[str, Any]:
        """The public ``GET /health`` data (fetched once per command)."""
        if self._health is None:
            self._health = as_dict(self.api.get("/health", auth=False).data)
        return self._health

    def trading_day(self) -> str:
        """The current trading day by market time (``/health``)."""
        td = self.health().get("trading_day")
        if not isinstance(td, str) or not td:
            raise CliError(UNAVAILABLE, "GET /health returned no trading_day")
        return td

    def market_now_ns(self) -> int:
        """Market time (``/health``)."""
        ts = self.health().get("ts_ns")
        if not isinstance(ts, int):
            raise CliError(UNAVAILABLE, "GET /health returned no ts_ns")
        return ts

    def check_home(self) -> None:
        """Refuse an api-gateway that serves another home (``/health`` ``home_id``): this home's
        bootstrap key is never sent there. A ``/health`` without ``home_id`` (older gateway) passes."""
        theirs = self.health().get("home_id")
        ours = self.layout.home_id()
        if isinstance(theirs, str) and theirs and theirs != ours:
            raise CliError(
                CONFLICT,
                f"the api-gateway at {self.api_url} serves another home (home_id {theirs}; {self.layout.home} "
                f"is {ours}): this home's API key is not sent there",
                hint="use the --home of that system, or give this home other ports in services.yaml",
            )


def missing_key_message(layout: HomeLayout) -> str:
    return (
        f"no API key: set {API_KEY_ENV} or start the system once "
        f"(api-gateway writes {layout.secrets_dir / BOOTSTRAP_KEY_FILE})"
    )


@contextlib.contextmanager
def session(home: str | None, api: str | None, as_json: bool, *, forbidden_hint: str | None = None) -> Iterator[Session]:
    """Load ``<home>``, open the API client; a :class:`CliError` is printed and ends with exit code 1.

    When the key is this home's bootstrap key file, the gateway must serve this home
    (:meth:`Session.check_home`); an explicit ``FUTRISK_API_KEY`` skips the check (e.g. ``--api`` of a
    docker deployment). ``forbidden_hint`` is added to ``FORBIDDEN`` answers about demo mode.
    """
    out = Out(as_json)
    try:
        ctx = load_context(home)
        from futrisk.cli.apiclient import ApiClient

        url = resolve_api_url(ctx, api)
        key = read_api_key(ctx.layout)
        with ApiClient(url, key, missing_key=missing_key_message(ctx.layout)) as client:
            s = Session(ctx, client, out, url)
            if key is not None and not os.environ.get(API_KEY_ENV, "").strip():
                s.check_home()
            yield s
    except CliError as exc:
        if exc.hint is None and exc.code == "BUSY" and "authentication" in exc.message.lower():
            exc.hint = f"check the API key ({API_KEY_ENV} or {BOOTSTRAP_KEY_FILE}); failed keys are throttled"
        if forbidden_hint and exc.code == FORBIDDEN and exc.hint is None and "demo" in exc.message.lower():
            exc.hint = forbidden_hint
        out.error(exc)
        raise typer.Exit(1) from None


__all__ = [
    "ApiOpt",
    "CliError",
    "HomeOpt",
    "JsonOpt",
    "Out",
    "Session",
    "TdOpt",
    "bj_time",
    "fmt",
    "kv",
    "parse_kv",
    "parse_value",
    "pct",
    "resolve_api_url",
    "session",
    "span",
    "state_style",
    "td_option",
]
