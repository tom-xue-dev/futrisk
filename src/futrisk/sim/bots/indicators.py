"""Small, deterministic indicators for the strategy bots (pure functions of the tick stream).

Time always comes from ticks (``exch_ts_ns``, market time), never from the wall clock,
so a replayed tick sequence produces the same bars and signals.
"""

from __future__ import annotations

import math
from collections import deque
from collections.abc import Iterable
from dataclasses import dataclass

NS_PER_S = 1_000_000_000


@dataclass
class OhlcBar:
    start_ns: int
    open: float
    high: float
    low: float
    close: float
    ticks: int = 1

    def update(self, price: float) -> None:
        self.high = max(self.high, price)
        self.low = min(self.low, price)
        self.close = price
        self.ticks += 1


class BarBuilder:
    """Fixed-length time bars aligned to multiples of ``bar_s`` of market time.

    :meth:`update` returns the *completed* bar when a tick opens a new bar (a bar is
    complete only once a later tick proves its interval is over), else ``None``.
    """

    def __init__(self, bar_s: float) -> None:
        if bar_s <= 0:
            raise ValueError("bar_s must be > 0")
        self.bar_ns = int(bar_s * NS_PER_S)
        self.current: OhlcBar | None = None

    def update(self, ts_ns: int, price: float) -> OhlcBar | None:
        start = ts_ns - ts_ns % self.bar_ns
        cur = self.current
        if cur is None:
            self.current = OhlcBar(start, price, price, price, price)
            return None
        if start > cur.start_ns:
            self.current = OhlcBar(start, price, price, price, price)
            return cur
        cur.update(price)  # same bar (or a late tick of an older bar: folded in)
        return None


class SMA:
    """Simple moving average over the last ``n`` values."""

    def __init__(self, n: int) -> None:
        if n <= 0:
            raise ValueError("n must be > 0")
        self.n = n
        self._q: deque[float] = deque(maxlen=n)
        self._sum = 0.0

    def add(self, x: float) -> float | None:
        if len(self._q) == self.n:
            self._sum -= self._q[0]
        self._q.append(x)
        self._sum += x
        return self.value

    @property
    def ready(self) -> bool:
        return len(self._q) == self.n

    @property
    def value(self) -> float | None:
        return self._sum / len(self._q) if self.ready else None

    def __len__(self) -> int:
        return len(self._q)


class Channel:
    """Donchian channel over completed bars: highest high / lowest low of the last ``n``."""

    def __init__(self, n: int) -> None:
        if n <= 0:
            raise ValueError("n must be > 0")
        self.n = n
        self._highs: deque[float] = deque(maxlen=n)
        self._lows: deque[float] = deque(maxlen=n)

    def add(self, bar: OhlcBar) -> None:
        self._highs.append(bar.high)
        self._lows.append(bar.low)

    def __len__(self) -> int:
        return len(self._highs)

    @property
    def ready(self) -> bool:
        return len(self._highs) == self.n

    def upper(self, last: int | None = None) -> float:
        """Highest high of the last ``last`` bars (default: all ``n``)."""
        return max(_tail(self._highs, last))

    def lower(self, last: int | None = None) -> float:
        return min(_tail(self._lows, last))


def _tail(q: deque[float], last: int | None) -> Iterable[float]:
    if last is None or last >= len(q):
        return q
    return list(q)[-last:]


class RollingStats:
    """Mean and (population) standard deviation over the last ``n`` samples."""

    def __init__(self, n: int) -> None:
        if n <= 1:
            raise ValueError("n must be > 1")
        self.n = n
        self._q: deque[float] = deque(maxlen=n)

    def add(self, x: float) -> None:
        self._q.append(x)

    def __len__(self) -> int:
        return len(self._q)

    @property
    def mean(self) -> float:
        return sum(self._q) / len(self._q) if self._q else 0.0

    @property
    def std(self) -> float:
        k = len(self._q)
        if k < 2:
            return 0.0
        m = self.mean
        return math.sqrt(sum((x - m) ** 2 for x in self._q) / k)

    def zscore(self, x: float) -> float | None:
        sd = self.std
        if sd <= 1e-12:
            return None
        return (x - self.mean) / sd


__all__ = ["NS_PER_S", "SMA", "BarBuilder", "Channel", "OhlcBar", "RollingStats"]
