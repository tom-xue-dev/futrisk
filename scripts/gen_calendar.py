"""Generate ``config/calendar/trading_days.csv`` (weekdays minus exchange holidays).

2025 and 2026 follow the State Council holiday schedules (2026: Mid-Autumn
09-25, National Day 10-01..10-07). 2027 is an estimate and must be replaced by
the official schedule when it is published.

Usage::

    python scripts/gen_calendar.py
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path


def _range(a: str, b: str) -> list[date]:
    d0, d1 = date.fromisoformat(a), date.fromisoformat(b)
    out = []
    while d0 <= d1:
        out.append(d0)
        d0 += timedelta(days=1)
    return out


HOLIDAYS: list[date] = [
    # 2025 (official)
    *_range("2025-01-01", "2025-01-01"),
    *_range("2025-01-28", "2025-02-04"),
    *_range("2025-04-04", "2025-04-06"),
    *_range("2025-05-01", "2025-05-05"),
    *_range("2025-05-31", "2025-06-02"),
    *_range("2025-10-01", "2025-10-08"),
    # 2026 (official)
    *_range("2026-01-01", "2026-01-03"),
    *_range("2026-02-15", "2026-02-23"),
    *_range("2026-04-04", "2026-04-06"),
    *_range("2026-05-01", "2026-05-05"),
    *_range("2026-06-19", "2026-06-21"),
    *_range("2026-09-25", "2026-09-27"),
    *_range("2026-10-01", "2026-10-07"),
    # 2027 (ESTIMATED)
    *_range("2027-01-01", "2027-01-03"),
    *_range("2027-02-05", "2027-02-13"),
    *_range("2027-04-05", "2027-04-05"),
    *_range("2027-05-01", "2027-05-05"),
    *_range("2027-06-09", "2027-06-09"),
    *_range("2027-09-15", "2027-09-15"),
    *_range("2027-10-01", "2027-10-07"),
]


def trading_days(start: date, end: date) -> list[date]:
    hs = set(HOLIDAYS)
    out = []
    d = start
    while d <= end:
        if d.weekday() < 5 and d not in hs:
            out.append(d)
        d += timedelta(days=1)
    return out


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    path = root / "config" / "calendar" / "trading_days.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    days = trading_days(date(2025, 1, 1), date(2027, 12, 31))
    lines = [
        "# China futures trading days (SHFE/DCE/CFFEX). 2025-2026 official; 2027 estimated.",
        "trading_day",
    ]
    lines += [d.strftime("%Y%m%d") for d in days]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"wrote {len(days)} trading days to {path}")


if __name__ == "__main__":
    main()
