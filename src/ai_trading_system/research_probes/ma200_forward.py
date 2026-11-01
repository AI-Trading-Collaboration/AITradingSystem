"""QQQ 200-day moving-average pullback signal's forward record (TRADING-2571-01).

Authority: owner_decision:TRADING-2571:2026-11-01:ma200_forward_record.
Use Yahoo adjusted QQQ daily bars. A touch has the previous close above its
200-day average and today's low at or below today's average. Three closes at or
above the touch-day average confirm entry at the third close. Count only signals
whose average exceeds its value 20 sessions earlier. Stop at -8%, filling at the
lower of the open and stop price; hold for 60 sessions and compare with entry on
any window day. Resume three days after failed touches, ignore touches within
20 sessions of entry, and apply the slope filter after scanning.
For learning only, not trading instructions: read one explicitly named price
file and write only this record and summary, offline, without daily or broker access.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Annotated, Any, NamedTuple

import pandas as pd
import typer

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.core.provenance import git_provenance
from ai_trading_system.research_bench.ledger import chain_digest
from ai_trading_system.trading_calendar import is_us_equity_trading_day

RUN_ID = "TRADING-2571-01"
SCHEMA = "ma200_forward.v1"
OWNER_DECISION = "owner_decision:TRADING-2571:2026-11-01:ma200_forward_record"
TICKER = "QQQ"
EASTERN = "America/New_York"
# Frozen pilot rules and descriptive criteria: OWNER_DECISION, requirement sections 1-3.
# Never tune against forward outcomes; the pilot ends with the window's settlements.
WINDOW_START = date(2026, 11, 2)
WINDOW_END = date(2027, 10, 29)
SMA_DAYS = 200
SLOPE_LAG = 20
CONFIRM_DAYS = 3
COOLDOWN = 20
HOLD_DAYS = 60
STOP = 0.08
LINE_UP = 0.10
LINE_MEAN = 0.03
BACKTEST_START = date(2000, 1, 3)
BACKTEST_END = date(2026, 10, 30)
GAP_CHECK_FROM = date(2025, 10, 1)
# Frozen replay/revision precision from the requirement, not performance thresholds.
REVISION_TOLERANCE = 1e-5
# Yahoo scales old adjusted prices down after dividends; a row's [RESCALE_FLOOR, 1] is no revision.
RESCALE_FLOOR = 0.9
REPRODUCE_TOLERANCE = 5e-4
# Numerical identity tolerance: avoid binary-rounding differences at exact boundaries.
TOLERANCE = 1e-9
BACKTEST: dict[str, Any] = {
    "touches": 84,
    "signals": 21,
    "failed": 63,
    "counted": 13,
    "entries": (
        "2004-10-21",
        "2007-08-20",
        "2010-05-10",
        "2010-06-15",
        "2010-08-02",
        "2010-09-09",
        "2011-10-24",
        "2012-11-01",
        "2014-10-17",
        "2016-07-07",
        "2019-02-20",
        "2020-04-14",
        "2024-08-07",
    ),
    "up60": 0.8461538461538461,
    "mean60": 0.08367354908251776,
    "mean60_stop": 0.06397331931128644,
    "worst60": -0.013577064042907816,
    "base_days": 6489,
    "base_up60": 0.68777931884728,
    "base_mean60": 0.029483887039537075,
    "base_mean60_stop": 0.023918070521552356,
}
EVENTS_FILE = "events.jsonl"
SUMMARY_FILE = "summary.md"
DEFAULT_DIR = PROJECT_ROOT / "research" / "_forward" / "ma200"
KINDS = ("touch", "settle", "checked")
COMMON_KEYS = frozenset(
    {
        "schema_version",
        "seq",
        "kind",
        "decided_on",
        "prev_sha256",
        "code_commit",
        "price_file",
        "price_sha256",
        "price_last_date",
        "through",
    }
)
TOUCH_FIELDS = (
    "touch_date",
    "prev_date",
    "prev_close",
    "prev_sma",
    "low",
    "sma",
    "sma_lag",
    "filter",
    "closes",
    "outcome",
    "entry_date",
    "entry_price",
    "counted",
)
SETTLE_FIELDS = (
    "touch_date",
    "entry_date",
    "entry_price",
    "exit_date",
    "exit_price",
    "r60",
    "stopped",
    "stop_date",
    "stop_open",
    "stop_low",
    "stop_fill",
    "r60_stop",
    "counted",
)
CHECKED_FIELDS = ("touches", "signals", "counted", "settled", "open")
FIELDS = {"touch": TOUCH_FIELDS, "settle": SETTLE_FIELDS, "checked": CHECKED_FIELDS}
PRICE_FIELDS = {
    "touch": ("prev_close", "prev_sma", "low", "sma", "sma_lag", "closes", "entry_price"),
    "settle": ("entry_price", "exit_price", "stop_open", "stop_low", "stop_fill"),
    "checked": (),
}
RETURN_FIELDS = ("r60", "r60_stop")
ANCHORS = {"touch": "sma", "settle": "entry_price"}
NOTE_TEXT = "只用于学习检验，不是交易指令。"
HOLDS_TEXT = "合格：60 日上涨比例高 10 个百分点以上，带止损的 60 日均值高 3 个百分点以上"
NOT_HOLDS_TEXT = "不合格：两条线没有同时过"
NO_SIGNAL_TEXT = "窗口内没有信号，不判"
IN_PROGRESS_TEXT = "进行中：只描述，不判"
PREFIX = "ma200_forward: "

app = typer.Typer(help="QQQ 200 日线回踩信号的前向记录：只用于学习检验。")


class Ma200Error(ValueError):
    """A price file or record cannot be used; do not write anything."""


@dataclass(frozen=True)
class Bars:
    dates: tuple[date, ...]
    open: tuple[float, ...]
    low: tuple[float, ...]
    close: tuple[float, ...]
    file: str
    sha256: str
    skipped: int

    def __len__(self) -> int:
        return len(self.dates)


def _number(value: Any) -> bool:
    if isinstance(value, bool) or not isinstance(value, (float, int)):
        return False
    try:
        return math.isfinite(value)
    except OverflowError:
        return False


def parse_chart(payload: Mapping[str, Any], *, file: str, sha256: str) -> Bars:
    try:
        result = payload["chart"]["result"][0]
        meta = result["meta"]
        stamps = result["timestamp"]
        quote = result["indicators"]["quote"][0]
        columns = [quote[key] for key in ("open", "low", "close")]
        columns.append(result["indicators"]["adjclose"][0]["adjclose"])
        if not isinstance(meta, dict) or not isinstance(stamps, list):
            raise ValueError("invalid metadata or timestamps")
        if any(not isinstance(column, list) for column in columns):
            raise ValueError("invalid price arrays")
    except (KeyError, IndexError, TypeError, ValueError) as exc:
        raise Ma200Error(f"Yahoo chart structure: {exc}") from exc
    if meta.get("symbol") != TICKER:
        raise Ma200Error("Yahoo symbol is not QQQ")
    if meta.get("dataGranularity", "1d") != "1d":
        raise Ma200Error("Yahoo bars must be daily")
    if any(len(column) != len(stamps) for column in columns):
        raise Ma200Error("Yahoo column length does not match timestamps")
    try:
        dates = pd.to_datetime(stamps, unit="s", utc=True).tz_convert(EASTERN).date
    except (ValueError, TypeError, OverflowError) as exc:
        raise Ma200Error(f"Yahoo timestamps: {exc}") from exc
    kept_dates: list[date] = []
    opens: list[float] = []
    lows: list[float] = []
    closes: list[float] = []
    skipped = 0
    for day, raw_open, raw_low, raw_close, adjusted in zip(dates, *columns, strict=True):
        if not all(_number(v) and v > 0 for v in (raw_open, raw_low, raw_close, adjusted)):
            skipped += 1
            continue
        if day < BACKTEST_START:
            continue
        factor = adjusted / raw_close
        adjusted_open, adjusted_low = raw_open * factor, raw_low * factor
        if not all(_number(v) and v > 0 for v in (adjusted_open, adjusted_low)):
            raise Ma200Error("Yahoo adjustment gives invalid prices")
        kept_dates.append(day)
        opens.append(float(adjusted_open))
        lows.append(float(adjusted_low))
        closes.append(float(adjusted))
    if not kept_dates or kept_dates[0] != BACKTEST_START:
        raise Ma200Error(f"Yahoo first date is not {BACKTEST_START}")
    if any(a >= b for a, b in zip(kept_dates, kept_dates[1:], strict=False)):
        raise Ma200Error("Yahoo dates must be strictly increasing")
    if len(kept_dates) < SMA_DAYS + SLOPE_LAG:
        raise Ma200Error(f"Yahoo needs at least {SMA_DAYS + SLOPE_LAG} rows")
    return Bars(tuple(kept_dates), tuple(opens), tuple(lows), tuple(closes), file, sha256, skipped)


def load_chart(path: Path) -> Bars:
    try:
        raw = path.read_bytes()
        payload = json.loads(raw)
        if not isinstance(payload, dict):
            raise ValueError("Yahoo JSON must be an object")
        return parse_chart(payload, file=path.name, sha256=hashlib.sha256(raw).hexdigest())
    except (OSError, ValueError, UnicodeError) as exc:
        raise Ma200Error(f"{path}: {exc}") from exc


def cut(bars: Bars, through: date) -> Bars:
    count = sum(day <= through for day in bars.dates)
    return Bars(
        bars.dates[:count],
        bars.open[:count],
        bars.low[:count],
        bars.close[:count],
        bars.file,
        bars.sha256,
        bars.skipped,
    )


def moving_average(close: Sequence[float]) -> tuple[float | None, ...]:
    return tuple(
        None if i < SMA_DAYS - 1 else math.fsum(close[i - SMA_DAYS + 1 : i + 1]) / SMA_DAYS
        for i in range(len(close))
    )


class Touch(NamedTuple):
    position: int
    outcome: str
    entry: int | None
    filter: bool
    counted: bool


def scan(bars: Bars) -> list[Touch]:
    averages = moving_average(bars.close)
    found: list[Touch] = []
    last_entry = None
    t = SMA_DAYS
    while t < len(bars):
        previous, sma = averages[t - 1], averages[t]
        if (
            previous is None
            or sma is None
            or bars.close[t - 1] <= previous
            or bars.low[t] > sma
            or (last_entry is not None and t - last_entry < COOLDOWN)
        ):
            t += 1
            continue
        lag = averages[t - SLOPE_LAG]
        passes = lag is not None and sma > lag
        entry = t + CONFIRM_DAYS - 1
        if entry >= len(bars):
            found.append(Touch(t, "pending", None, passes, False))
            break
        if all(value >= sma for value in bars.close[t : entry + 1]):
            found.append(Touch(t, "signal", entry, passes, passes))
            last_entry = entry
        else:
            found.append(Touch(t, "failed", None, passes, False))
        t += CONFIRM_DAYS
    return found


class Outcome(NamedTuple):
    exit: int
    r60: float
    stopped: bool
    stop: int | None
    fill: float | None
    r60_stop: float


def _stop(bars: Bars, entry: int, end: int) -> tuple[int | None, float | None]:
    line = bars.close[entry] * (1 - STOP)
    for i in range(entry + 1, end + 1):
        if bars.low[i] <= line:
            return i, min(bars.open[i], line)
    return None, None


def settle(bars: Bars, entry: int) -> Outcome | None:
    exit_position = entry + HOLD_DAYS
    if exit_position >= len(bars):
        return None
    r60 = bars.close[exit_position] / bars.close[entry] - 1
    stop, fill = _stop(bars, entry, exit_position)
    return Outcome(
        exit_position,
        r60,
        stop is not None,
        stop,
        fill,
        r60 if fill is None else fill / bars.close[entry] - 1,
    )


def holding(bars: Bars, entry: int) -> dict[str, Any]:
    stop, fill = _stop(bars, entry, len(bars) - 1)
    return {
        "days": len(bars) - 1 - entry,
        "last_return": bars.close[-1] / bars.close[entry] - 1,
        "stop_date": None if stop is None else bars.dates[stop].isoformat(),
        "stop_return": None if fill is None else fill / bars.close[entry] - 1,
    }


def base_rows(bars: Bars, start: date, end: date) -> list[tuple[date, float, float]]:
    rows = []
    for entry, day in enumerate(bars.dates):
        if start <= day <= end:
            outcome = settle(bars, entry)
            if outcome is not None:
                rows.append((day, outcome.r60, outcome.r60_stop))
    return rows


def describe(r60: Sequence[float], r60_stop: Sequence[float]) -> dict[str, Any]:
    n = len(r60)
    return {
        "n": n,
        "up60": sum(value > 0 for value in r60) / n if n else None,
        "mean60": math.fsum(r60) / n if n else None,
        "mean60_stop": math.fsum(r60_stop) / n if n else None,
        "worst60": min(r60) if n else None,
    }


def reproduce(bars: Bars) -> dict[str, Any]:
    bars = cut(bars, BACKTEST_END)
    touches = scan(bars)
    counted = [t for t in touches if t.counted]
    outcomes = [settle(bars, t.entry) for t in counted if t.entry is not None]
    stats = describe(
        [o.r60 for o in outcomes if o is not None],
        [o.r60_stop for o in outcomes if o is not None],
    )
    base = base_rows(bars, bars.dates[SMA_DAYS - 1], BACKTEST_END)
    base_stats = describe([r for _, r, _ in base], [r for _, _, r in base])
    values = {
        "touches": len(touches),
        "signals": sum(t.outcome == "signal" for t in touches),
        "failed": sum(t.outcome == "failed" for t in touches),
        "counted": len(counted),
        "entries": tuple(bars.dates[t.entry].isoformat() for t in counted if t.entry is not None),
        **{key: stats[key] for key in ("up60", "mean60", "mean60_stop", "worst60")},
        "base_days": base_stats["n"],
        **{f"base_{key}": base_stats[key] for key in ("up60", "mean60", "mean60_stop")},
    }
    problems = []
    for key, expected in BACKTEST.items():
        value = values[key]
        matches = (
            _number(value) and abs(value - expected) <= REPRODUCE_TOLERANCE
            if isinstance(expected, float)
            else value == expected
        )
        if not matches:
            problems.append(f"{key} {value} is not {expected}")
    return {"values": values, "problems": problems, "ok": not problems}


def build_events(bars: Bars, through: date) -> list[dict[str, Any]]:
    bars = cut(bars, through)
    averages = moving_average(bars.close)
    events = []
    for touch in scan(bars):
        t = touch.position
        if not WINDOW_START <= bars.dates[t] <= WINDOW_END or touch.outcome == "pending":
            continue
        entry = touch.entry
        touch_date = bars.dates[t].isoformat()
        events.append(
            {
                "kind": "touch",
                "decided_on": bars.dates[t + CONFIRM_DAYS - 1].isoformat(),
                "touch_date": touch_date,
                "prev_date": bars.dates[t - 1].isoformat(),
                "prev_close": bars.close[t - 1],
                "prev_sma": averages[t - 1],
                "low": bars.low[t],
                "sma": averages[t],
                "sma_lag": averages[t - SLOPE_LAG],
                "filter": touch.filter,
                "closes": list(bars.close[t : t + CONFIRM_DAYS]),
                "outcome": touch.outcome,
                "entry_date": None if entry is None else bars.dates[entry].isoformat(),
                "entry_price": None if entry is None else bars.close[entry],
                "counted": touch.counted,
            }
        )
        if entry is None:
            continue
        outcome = settle(bars, entry)
        if outcome is None:
            continue
        stop = outcome.stop
        events.append(
            {
                "kind": "settle",
                "decided_on": bars.dates[outcome.exit].isoformat(),
                "touch_date": touch_date,
                "entry_date": bars.dates[entry].isoformat(),
                "entry_price": bars.close[entry],
                "exit_date": bars.dates[outcome.exit].isoformat(),
                "exit_price": bars.close[outcome.exit],
                "r60": outcome.r60,
                "stopped": outcome.stopped,
                "stop_date": None if stop is None else bars.dates[stop].isoformat(),
                "stop_open": None if stop is None else bars.open[stop],
                "stop_low": None if stop is None else bars.low[stop],
                "stop_fill": outcome.fill,
                "r60_stop": outcome.r60_stop,
                "counted": touch.counted,
            }
        )
    return sorted(events, key=lambda e: (e["decided_on"], e["touch_date"], e["kind"] != "touch"))


def pending(bars: Bars, through: date) -> list[str]:
    bars = cut(bars, through)
    return [
        bars.dates[t.position].isoformat()
        for t in scan(bars)
        if t.outcome == "pending" and WINDOW_START <= bars.dates[t.position] <= WINDOW_END
    ]


def trading_gaps(bars: Bars, start: date, end: date) -> list[str]:
    present = set(bars.dates)
    problems = [
        f"{day} is not a US trading day"
        for day in bars.dates
        if start <= day <= end and not is_us_equity_trading_day(day)
    ]
    day = start
    while day <= end:
        if day not in present and is_us_equity_trading_day(day):
            problems.append(f"no bar for trading day {day}")
        day += timedelta(days=1)
    return problems


def _row_bytes(row: Mapping[str, Any]) -> bytes:
    return json.dumps(
        row,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _lines(directory: Path) -> list[bytes]:
    try:
        return [
            line
            for line in (directory / EVENTS_FILE).read_bytes().replace(b"\r\n", b"\n").split(b"\n")
            if line.strip()
        ]
    except FileNotFoundError:
        return []
    except OSError as exc:
        raise Ma200Error(f"{EVENTS_FILE}: {exc}") from exc


def _equal(left: Any, right: Any) -> bool:
    if isinstance(left, bool) or isinstance(right, bool):
        return left is right
    if isinstance(left, (float, int)) or isinstance(right, (float, int)):
        return (
            _number(left)
            and _number(right)
            and math.isclose(left, right, rel_tol=REVISION_TOLERANCE, abs_tol=1e-9)
        )
    if isinstance(left, list) and isinstance(right, list):
        return len(left) == len(right) and all(
            _equal(a, b) for a, b in zip(left, right, strict=True)
        )
    return bool(left == right)


def rescale_factor(new: Any, old: Any) -> float | None:
    if _number(new) and _number(old) and new > 0 and old > 0:
        factor = new / old
        if RESCALE_FLOOR <= factor <= 1 + REVISION_TOLERANCE:
            return float(factor)
    return None


def rescale(recorded: Mapping[str, Any], recomputed: Mapping[str, Any]) -> float:
    anchor = ANCHORS.get(recorded["kind"])
    if anchor is not None and recomputed.get("kind") == recorded["kind"]:
        factor = rescale_factor(recomputed.get(anchor), recorded.get(anchor))
        if factor is not None:
            return factor
    return 1.0


def differences(recorded: Mapping[str, Any], recomputed: Mapping[str, Any]) -> list[str]:
    factor = rescale(recorded, recomputed)
    kind = recorded["kind"]

    def scaled(value: Any) -> Any:
        if isinstance(value, list):
            return [item * factor if _number(item) else item for item in value]
        return value * factor if _number(value) else value

    def return_equal(old: Any, new: Any) -> bool:
        return bool(_number(old) and _number(new) and abs(old - new) <= REVISION_TOLERANCE)

    changed = []
    for key in ("kind", "decided_on", *FIELDS[kind]):
        old, new = recorded.get(key), recomputed.get(key)
        if key in PRICE_FIELDS[kind]:
            matches = _equal(scaled(old), new)
        elif key in RETURN_FIELDS:
            matches = return_equal(old, new)
        else:
            matches = _equal(old, new)
        if not matches:
            changed.append(key)
    return changed


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise Ma200Error(message)


def _iso(value: Any) -> date:
    if not isinstance(value, str):
        raise Ma200Error("date is not an ISO string")
    result = date.fromisoformat(value)
    _require(result.isoformat() == value, "date is not YYYY-MM-DD")
    return result


def _positive_fields(row: Mapping[str, Any], fields: Sequence[str]) -> None:
    for field in fields:
        _require(_number(row[field]) and row[field] > 0, f"{field} is not finite and positive")


def _touch_problem(row: Mapping[str, Any]) -> None:
    touch_date = _iso(row["touch_date"])
    _require(WINDOW_START <= touch_date <= WINDOW_END, "touch_date is outside the window")
    _positive_fields(row, ("prev_close", "prev_sma", "low", "sma"))
    _require(row["prev_close"] > row["prev_sma"], "prev_close is not above prev_sma")
    _require(row["low"] <= row["sma"], "low is not at or below sma")
    lag = row["sma_lag"]
    _require(lag is None or (_number(lag) and lag > 0), "sma_lag is invalid")
    _require(
        row["filter"] is (lag is not None and row["sma"] > lag),
        "filter does not match sma > sma_lag",
    )
    closes = row["closes"]
    _require(
        isinstance(closes, list)
        and len(closes) == CONFIRM_DAYS
        and all(_number(v) and v > 0 for v in closes),
        "closes must be three positive prices",
    )
    signal = all(v >= row["sma"] for v in closes)
    _require(
        row["outcome"] == ("signal" if signal else "failed"),
        "outcome does not match the three closes",
    )
    _require(row["counted"] is (signal and row["filter"]), "counted is not signal and filter")
    _require(
        _iso(row["prev_date"]) < touch_date < _iso(row["decided_on"]), "touch dates are not ordered"
    )
    if signal:
        _require(row["entry_date"] == row["decided_on"], "entry_date is not decided_on")
        _require(_equal(row["entry_price"], closes[-1]), "entry_price is not the third close")
    else:
        _require(
            row["entry_date"] is None and row["entry_price"] is None, "failed touch has an entry"
        )


def _settle_problem(
    row: Mapping[str, Any],
    touches: Mapping[str, Mapping[str, Any]],
    settled: set[str],
) -> None:
    touch = touches.get(row["touch_date"])
    _require(
        touch is not None and touch["outcome"] == "signal", "settle has no earlier signal touch"
    )
    assert touch is not None
    for field in ("entry_date", "entry_price", "counted"):
        matches = (
            rescale_factor(row[field], touch[field]) is not None
            if field == "entry_price"
            else _equal(row[field], touch[field])
        )
        _require(matches, f"{field} does not match its touch")
    _require(row["touch_date"] not in settled, "signal is settled twice")
    _positive_fields(row, ("entry_price", "exit_price"))
    _require(row["exit_date"] == row["decided_on"], "exit_date is not decided_on")
    _require(_iso(row["entry_date"]) < _iso(row["exit_date"]), "exit is not after entry")
    _require(
        _equal(row["r60"], row["exit_price"] / row["entry_price"] - 1),
        "r60 is not exit_price / entry_price - 1",
    )
    _require(type(row["stopped"]) is bool, "stopped is not bool")
    if row["stopped"]:
        _positive_fields(row, ("stop_low", "stop_open", "stop_fill"))
        line = row["entry_price"] * (1 - STOP)
        _require(row["stop_low"] <= line, "stop_low is above the stop price")
        _require(
            _equal(row["stop_fill"], min(row["stop_open"], line)),
            "stop_fill is not min(stop_open, entry_price * 0.92)",
        )
        _require(
            _iso(row["entry_date"]) < _iso(row["stop_date"]) <= _iso(row["exit_date"]),
            "stop_date is outside the holding period",
        )
        _require(
            _equal(row["r60_stop"], row["stop_fill"] / row["entry_price"] - 1),
            "r60_stop is not stop_fill / entry_price - 1",
        )
    else:
        _require(
            all(row[k] is None for k in ("stop_date", "stop_open", "stop_low", "stop_fill")),
            "stop fields are not None without a stop",
        )
        _require(_equal(row["r60_stop"], row["r60"]), "r60_stop is not r60")


def _counts(rows: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    touches = [r for r in rows if r["kind"] == "touch"]
    signals = [r for r in touches if r["outcome"] == "signal"]
    settled = sum(r["kind"] == "settle" for r in rows)
    return {
        "touches": len(touches),
        "signals": len(signals),
        "counted": sum(r["counted"] is True for r in signals),
        "settled": settled,
        "open": len(signals) - settled,
    }


def verify(directory: Path) -> list[str]:
    previous = None
    previous_day: date | None = None
    checked: date | None = None
    rows: list[dict[str, Any]] = []
    touches: dict[str, Mapping[str, Any]] = {}
    settled: set[str] = set()
    for n, raw in enumerate(_lines(directory), 1):
        prefix = f"{EVENTS_FILE} line {n}: "
        try:
            try:
                row = json.loads(raw)
            except (ValueError, UnicodeError) as exc:
                raise Ma200Error("not JSON") from exc
            _require(isinstance(row, dict), "kind is missing")
            kind = row.get("kind")
            _require(isinstance(kind, str) and kind in KINDS, "kind is invalid")
            _require(row.keys() == COMMON_KEYS | set(FIELDS[kind]), "keys do not match")
            _require(row["schema_version"] == SCHEMA, "schema_version does not match")
            _require(type(row["seq"]) is int and row["seq"] == n, f"seq is not {n}")
            _require(row["prev_sha256"] == previous, "prev_sha256 does not match")
            day = _iso(row["decided_on"])
            _require(previous_day is None or day >= previous_day, "decided_on goes backwards")
            through = _iso(row["through"])
            _require(
                day <= through <= _iso(row["price_last_date"]), "invalid through or price_last_date"
            )
            _require(
                isinstance(row["code_commit"], str) and len(row["code_commit"]) == 40,
                "code_commit is not a 40-character string",
            )
            _require(
                isinstance(row["price_file"], str) and isinstance(row["price_sha256"], str),
                "invalid price file provenance",
            )
            if kind == "checked":
                _require(day == through, "checked decided_on is not through")
                _require(checked is None or through > checked, "checked through does not advance")
                for field, value in _counts(rows).items():
                    _require(
                        type(row[field]) is int and row[field] == value, f"{field} is not {value}"
                    )
                checked = through
            else:
                _require(
                    checked is None or day > checked, "decided_on is not after checked through"
                )
                if kind == "touch":
                    _touch_problem(row)
                    _require(row["touch_date"] not in touches, "duplicate touch_date")
                    touches[row["touch_date"]] = row
                else:
                    _settle_problem(row, touches, settled)
                    settled.add(row["touch_date"])
            rows.append(row)
            previous_day = day
            previous = chain_digest(raw)
        except (ValueError, TypeError, KeyError, OverflowError) as exc:
            return [prefix + str(exc)]
    if rows and rows[-1]["kind"] != "checked":
        return [f"{EVENTS_FILE}: the last row is not a checked row"]
    return []


def read_events(directory: Path) -> list[dict[str, Any]]:
    problems = verify(directory)
    if problems:
        raise Ma200Error(f"the record is broken: {problems[0]}")
    return [json.loads(raw) for raw in _lines(directory)]


def last_through(rows: Sequence[Mapping[str, Any]]) -> date | None:
    return next((_iso(row["through"]) for row in reversed(rows) if row["kind"] == "checked"), None)


def revisions(rows: Sequence[Mapping[str, Any]], bars: Bars) -> list[str]:
    through = last_through(rows)
    if through is None:
        return []
    recorded = [r for r in rows if r["kind"] != "checked"]
    recomputed = build_events(bars, through)
    problems = []
    if len(recorded) != len(recomputed):
        problems.append(
            f"{len(recorded)} recorded events but the price file gives {len(recomputed)}"
        )
    for row, event in zip(recorded, recomputed, strict=False):
        changed = differences(row, event)
        if changed:
            problems.append(f"line {row['seq']} differs in {', '.join(changed)}")
    return problems


def append(
    directory: Path,
    bars: Bars,
    through: date,
    *,
    provenance: Mapping[str, Any],
) -> list[dict[str, Any]]:
    _require(provenance.get("code_modified") is False, "code_modified is not False")
    commit = provenance.get("commit")
    _require(isinstance(commit, str) and len(commit) == 40, "commit is not a 40-character string")
    _require(through >= WINDOW_START, "through is before the window")
    _require(through in bars.dates, f"no price for {through}")
    existing = read_events(directory)
    previous_through = last_through(existing)
    _require(
        previous_through is None or through > previous_through,
        f"already checked through {previous_through}",
    )
    gaps = trading_gaps(bars, GAP_CHECK_FROM, through)
    if gaps:
        raise Ma200Error(gaps[0])
    replay = reproduce(bars)
    if not replay["ok"]:
        raise Ma200Error(f"backtest: {replay['problems'][0]}")
    changed = revisions(existing, bars)
    if changed:
        raise Ma200Error(changed[0])
    contents = [
        event
        for event in build_events(bars, through)
        if previous_through is None or _iso(event["decided_on"]) > previous_through
    ]
    contents.append(
        {
            "kind": "checked",
            "decided_on": through.isoformat(),
            **_counts([*existing, *contents]),
        }
    )
    raw = _lines(directory)
    previous = chain_digest(raw[-1]) if raw else None
    rows = []
    encoded = []
    for seq, content in enumerate(contents, len(existing) + 1):
        row = {
            **content,
            "schema_version": SCHEMA,
            "seq": seq,
            "prev_sha256": previous,
            "code_commit": commit,
            "price_file": bars.file,
            "price_sha256": bars.sha256,
            "price_last_date": bars.dates[-1].isoformat(),
            "through": through.isoformat(),
        }
        data = _row_bytes(row)
        previous = chain_digest(data)
        rows.append(row)
        encoded.append(data + b"\n")
    path = directory / EVENTS_FILE
    try:
        needs_separator = path.exists() and path.stat().st_size > 0
        if needs_separator:
            with path.open("rb") as source:
                source.seek(-1, 2)
                needs_separator = source.read(1) != b"\n"
        directory.mkdir(parents=True, exist_ok=True)
        with path.open("ab") as sink:
            sink.write((b"\n" if needs_separator else b"") + b"".join(encoded))
    except OSError as exc:
        raise Ma200Error(f"{path}: {exc}") from exc
    return rows


def check(directory: Path, bars: Bars | None = None) -> list[str]:
    problems = verify(directory)
    if not problems and bars is not None:
        problems.extend(f"backtest: {p}" for p in reproduce(bars)["problems"])
        problems.extend(revisions(read_events(directory), bars))
    return problems


def judge(
    signals: Mapping[str, Any],
    base: Mapping[str, Any] | None,
    complete: bool,
) -> dict[str, Any]:
    diff_up = diff_mean = None
    if signals["n"] and base is not None and base["n"]:
        diff_up = signals["up60"] - base["up60"]
        diff_mean = signals["mean60_stop"] - base["mean60"]
    line_up = diff_up is not None and diff_up >= LINE_UP - TOLERANCE
    line_mean = diff_mean is not None and diff_mean >= LINE_MEAN - TOLERANCE
    if not complete:
        state, text = "in_progress", IN_PROGRESS_TEXT
    elif not signals["n"]:
        state, text = "no_signal", NO_SIGNAL_TEXT
    elif line_up and line_mean:
        state, text = "holds", HOLDS_TEXT
    else:
        state, text = "not_holds", NOT_HOLDS_TEXT
    return {
        "signals": signals,
        "base": base,
        "diff_up": diff_up,
        "diff_mean": diff_mean,
        "line_up": line_up,
        "line_mean": line_mean,
        "complete": complete,
        "state": state,
        "text": text,
    }


def criteria(rows: Sequence[Mapping[str, Any]], bars: Bars | None) -> dict[str, Any]:
    settled = [r for r in rows if r["kind"] == "settle" and r["counted"]]
    signals = describe([r["r60"] for r in settled], [r["r60_stop"] for r in settled])
    through = last_through(rows)
    if bars is None or through is None:
        return judge(signals, None, False)
    bars = cut(bars, through)
    base = base_rows(bars, WINDOW_START, WINDOW_END)
    stats = describe([r for _, r, _ in base], [r for _, _, r in base])
    window_days = sum(
        WINDOW_START <= day <= WINDOW_END and is_us_equity_trading_day(day) for day in bars.dates
    )
    counts = _counts(rows)
    complete = bool(
        bars.dates
        and bars.dates[-1] >= WINDOW_END
        and stats["n"] == window_days
        and counts["open"] == 0
        and not pending(bars, through)
    )
    return judge(signals, stats, complete)


def _pct(value: float | None) -> str:
    return "—" if value is None else f"{value * 100:+.2f}%"


def _stats_text(stats: Mapping[str, Any]) -> str:
    up = "—" if stats["up60"] is None else f"{stats['up60']:.1%}"
    return (
        f"{stats['n']} 次，60 日上涨 {up}，均值 {_pct(stats['mean60'])}，"
        f"带止损 {_pct(stats['mean60_stop'])}"
    )


def render_summary(rows: Sequence[Mapping[str, Any]], bars: Bars | None = None) -> str:
    through = last_through(rows)
    lines = [
        "# QQQ 200 日线回踩信号的前向记录",
        "",
        f"- {RUN_ID}；依据 `{OWNER_DECISION}`。{NOTE_TEXT}",
        f"- 窗口：{WINDOW_START} 至 {WINDOW_END}（触线日）；实际请求截至：{through or '还没有'}。",
        f"- 检查到：{through or '还没有'}",
        "- 数据质量：追加前检查价格结构、交易日缺口、固定回测复现和已记事件修订。",
    ]
    if bars is not None:
        lines.append(
            f"- 价格文件：{bars.file}（{bars.sha256[:12]}…）；"
            f"文件范围：{bars.dates[0]} 至 {bars.dates[-1]}。"
        )
        if through is not None:
            bars = cut(bars, through)
    lines += [
        "",
        "## 信号",
        "",
        "| 触线日 | 买入日 | 买入价 | 计入 | 状态 | 60 日 | 带止损 |",
        "|---|---|---|---|---|---|---|",
    ]
    signals = [r for r in rows if r["kind"] == "touch" and r["outcome"] == "signal"]
    settlements = {r["touch_date"]: r for r in rows if r["kind"] == "settle"}
    for signal in signals:
        outcome = settlements.get(signal["touch_date"])
        r60 = r60_stop = None
        if outcome is not None:
            status = "已结算"
            if outcome["stopped"]:
                status += f"，{outcome['stop_date']} 止损"
            r60, r60_stop = outcome["r60"], outcome["r60_stop"]
        elif bars is not None and _iso(signal["entry_date"]) in bars.dates:
            state = holding(bars, bars.dates.index(_iso(signal["entry_date"])))
            status = f"持有第 {state['days']} 天，至今 {_pct(state['last_return'])}"
            if state["stop_date"] is not None:
                status += f"，{state['stop_date']} 止损 {_pct(state['stop_return'])}"
        else:
            status = "持有中（没给价格文件）"
        lines.append(
            f"| {signal['touch_date']} | {signal['entry_date']} | {signal['entry_price']:.2f} | "
            f"{'是' if signal['counted'] else '否'} | {status} | {_pct(r60)} | {_pct(r60_stop)} |"
        )
    if not signals:
        lines.append("还没有信号。")
    failed = [r["touch_date"] for r in rows if r["kind"] == "touch" and r["outcome"] == "failed"]
    result = criteria(rows, bars)
    lines += [
        "",
        "## 没站稳的触线",
        "",
        "、".join(failed) + "。" if failed else "没有。",
        "",
        "## 合格标准（Baker 事先定）",
        "",
        f"判定：{result['text']}",
        "信号：" + _stats_text(result["signals"]),
    ]
    base = result["base"]
    lines.append(
        "同期任意一天："
        + ("没给价格文件" if base is None else _stats_text(base) + "（带止损只参考）")
    )
    for key, title in (
        ("diff_up", "上涨比例差（线 +10.00）"),
        ("diff_mean", "信号带止损均值减任意一天不带止损均值（线 +3.00）"),
    ):
        value = result[key]
        lines.append(f"- {title}：" + ("—" if value is None else f"{value * 100:+.2f} 个百分点"))
    lines += [
        "",
        "## 回测对照（2000-01 至 2026-10，固定）",
        "",
        f"计入 {BACKTEST['counted']} 次，60 日上涨 {BACKTEST['up60']:.1%}，"
        f"均值 {_pct(BACKTEST['mean60'])}，带止损 {_pct(BACKTEST['mean60_stop'])}，"
        f"最差 {_pct(BACKTEST['worst60'])}。",
        f"任意一天 {BACKTEST['base_days']} 天，60 日上涨 {BACKTEST['base_up60']:.1%}，"
        f"均值 {_pct(BACKTEST['base_mean60'])}，带止损 {_pct(BACKTEST['base_mean60_stop'])}。",
        "信号很少，合格标准只当描述看；以上固定历史对照不是新的研究默认窗口。",
        "",
        "production_effect=none；broker_action=none。",
        "",
    ]
    return "\n".join(lines)


def write_summary(directory: Path, bars: Bars | None = None) -> Path:
    rows = read_events(directory)
    text = render_summary(rows, bars)
    path = directory / SUMMARY_FILE
    try:
        directory.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8", newline="\n")
    except OSError as exc:
        raise Ma200Error(f"{path}: {exc}") from exc
    return path


def event_text(row: Mapping[str, Any]) -> str:
    if row["kind"] == "touch":
        if row["outcome"] == "failed":
            return f"{row['touch_date']} 触线，没站稳"
        counted = "计入" if row["counted"] else "不计入：200 日线不高于 20 个交易日前"
        return (
            f"{row['touch_date']} 触线，站稳，{row['entry_date']} 收盘 "
            f"{row['entry_price']:.2f} 买入（{counted}）"
        )
    if row["kind"] == "settle":
        text = (
            f"{row['entry_date']} 买入的信号 {row['exit_date']} 满 60 日："
            f"不带止损 {_pct(row['r60'])}，带止损 {_pct(row['r60_stop'])}"
        )
        return text + (f"，{row['stop_date']} 止损" if row["stopped"] else "")
    return (
        f"检查到 {row['through']}：窗口内触线 {row['touches']} 次，信号 {row['signals']} 次"
        f"（计入 {row['counted']}），已结算 {row['settled']}，持有中 {row['open']}"
    )


def _date(value: str) -> date:
    try:
        return _iso(value)
    except ValueError:
        raise typer.BadParameter("日期必须是 YYYY-MM-DD") from None


def _echo(text: str) -> None:
    typer.echo(PREFIX + text)


@app.command("reproduce")
def reproduce_command(
    prices: Annotated[Path, typer.Option(help="Yahoo QQQ 日线 JSON 文件。")],
) -> None:
    try:
        result = reproduce(load_chart(prices))
        v = result["values"]
        up = "—" if v["up60"] is None else f"{v['up60']:.1%}"
        _echo(
            f"回测复现：触线 {v['touches']}，信号 {v['signals']}，计入 {v['counted']}，"
            f"60 日上涨 {up}，均值 {_pct(v['mean60'])}"
        )
        for problem in result["problems"]:
            _echo(problem)
        if not result["ok"]:
            raise typer.Exit(code=1)
    except Ma200Error as exc:
        _echo(str(exc))
        raise typer.Exit(code=1) from None


@app.command("append")
def append_command(
    prices: Annotated[Path, typer.Option(help="Yahoo QQQ 日线 JSON 文件。")],
    through: Annotated[str, typer.Option(help="检查截至 YYYY-MM-DD。")],
    directory: Annotated[Path | None, typer.Option("--dir", help="记录目录。")] = None,
) -> None:
    day = _date(through)
    try:
        bars = load_chart(prices)
        rows = append(
            DEFAULT_DIR if directory is None else directory,
            bars,
            day,
            provenance=git_provenance(PROJECT_ROOT),
        )
    except Ma200Error as exc:
        _echo("拒绝追加：" + str(exc))
        raise typer.Exit(code=1) from None
    for row in rows:
        _echo(event_text(row))
    for touch in pending(bars, day):
        _echo(f"{touch} 触线，站没站稳要等之后两天的收盘")


@app.command("summary")
def summary_command(
    prices: Annotated[Path | None, typer.Option(help="可选 Yahoo QQQ 日线 JSON 文件。")] = None,
    directory: Annotated[Path | None, typer.Option("--dir", help="记录目录。")] = None,
) -> None:
    try:
        path = write_summary(
            DEFAULT_DIR if directory is None else directory,
            None if prices is None else load_chart(prices),
        )
        _echo(f"汇总已写 {path}")
    except Ma200Error as exc:
        _echo(str(exc))
        raise typer.Exit(code=1) from None


@app.command("check")
def check_command(
    prices: Annotated[Path | None, typer.Option(help="可选 Yahoo QQQ 日线 JSON 文件。")] = None,
    directory: Annotated[Path | None, typer.Option("--dir", help="记录目录。")] = None,
) -> None:
    directory = DEFAULT_DIR if directory is None else directory
    try:
        problems = check(directory, None if prices is None else load_chart(prices))
        for problem in problems:
            _echo(problem)
        if problems:
            raise typer.Exit(code=1)
        rows = read_events(directory)
        _echo(f"记录完好，{len(rows)} 行，检查到 {last_through(rows) or '还没有'}")
    except Ma200Error as exc:
        _echo(str(exc))
        raise typer.Exit(code=1) from None


def main() -> None:
    app()


if __name__ == "__main__":
    main()
