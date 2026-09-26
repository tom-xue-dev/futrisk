"""Market-data adapters shared by md-gateway and the recording tools (01 §8.13, 02 §7.1, §9.3, 03 §5.1).

Modules:

- :mod:`~futrisk.marketdata.fingerprint`: cross-source ``Tick.fingerprint`` (02 §7.1);
- :mod:`~futrisk.marketdata.normalize`: instrument metadata, limit derivation, lock flags;
- :mod:`~futrisk.marketdata.sina`: ``hq.sinajs.cn`` protocol, parser and ``Tick`` normalizer;
- :mod:`~futrisk.marketdata.poller`: async poller and the ``SinaSource`` market-data source (02 §10);
- :mod:`~futrisk.marketdata.sessions`: session-window gate for polling;
- :mod:`~futrisk.marketdata.tickschema`: Arrow schemas of tick / wire shards (03 §5.1);
- :mod:`~futrisk.marketdata.recorder`: classifier, crash-safe shard writer, readers, rebuild;
- :mod:`~futrisk.marketdata.lifecycle`: keep-awake, single-instance lock, console-close flush, stop signals.
"""

from futrisk.marketdata.fingerprint import (
    FINGERPRINT_FIELDS,
    compute_fingerprint,
    tick_fingerprint,
    with_fingerprint,
)
from futrisk.marketdata.lifecycle import ConsoleCloseGuard, InstanceLock, LockHeldError, keep_awake
from futrisk.marketdata.normalize import (
    InstrumentMeta,
    canonical_instrument_id,
    derive_limits,
    instrument_meta,
)
from futrisk.marketdata.poller import (
    SINA_CAPABILITIES,
    SinaBatch,
    SinaPoll,
    SinaPoller,
    SinaSource,
    SourceCapabilities,
    make_sina_client,
)
from futrisk.marketdata.recorder import (
    ParquetShardWriter,
    ShardWriteError,
    SinaRecorder,
    TickClassifier,
    list_shards,
    list_trading_days,
    read_raw_ticks,
    read_wire,
    rebuild_ticks_from_wire,
)
from futrisk.marketdata.sessions import SessionGate
from futrisk.marketdata.sina import (
    SinaFormatError,
    SinaNormalizer,
    SinaQuote,
    SinaResult,
    parse_quote,
    resolve_sina_instruments,
    sina_code_for,
)
from futrisk.marketdata.tickschema import TICK_SCHEMA, WIRE_SCHEMA

__all__ = [
    "FINGERPRINT_FIELDS",
    "SINA_CAPABILITIES",
    "TICK_SCHEMA",
    "WIRE_SCHEMA",
    "ConsoleCloseGuard",
    "InstanceLock",
    "InstrumentMeta",
    "LockHeldError",
    "ParquetShardWriter",
    "SessionGate",
    "ShardWriteError",
    "SinaBatch",
    "SinaFormatError",
    "SinaNormalizer",
    "SinaPoll",
    "SinaPoller",
    "SinaQuote",
    "SinaRecorder",
    "SinaResult",
    "SinaSource",
    "SourceCapabilities",
    "TickClassifier",
    "canonical_instrument_id",
    "compute_fingerprint",
    "derive_limits",
    "instrument_meta",
    "keep_awake",
    "list_shards",
    "list_trading_days",
    "make_sina_client",
    "parse_quote",
    "read_raw_ticks",
    "read_wire",
    "rebuild_ticks_from_wire",
    "resolve_sina_instruments",
    "sina_code_for",
    "tick_fingerprint",
    "with_fingerprint",
]
