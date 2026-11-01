"""The 200-day pullback rule on closes, counted on both rulers (TRADING-2565-104).

Owner decision owner_decision:TRADING-2565:2026-11-01:research_loop_p7 and
period-7 charter section 5.2, step 0, variant 0 authorize counts only. The rule
frozen on 2026-10-31 replaces the low touching the average with the close
falling to it; everything else stays fixed. Development windows are QQQ
1999–2010 and US market 1929–2010, cut at 2010-12-31. No investment returns,
stop losses, or US-market entry dates are printed. This is neither a probe nor
an experiment, makes no accounting entries, and touches no daily report or broker.
"""

from __future__ import annotations

import json
import math
from collections.abc import Mapping, Sequence
from datetime import date
from pathlib import Path
from typing import Annotated, Any, NamedTuple

import pandas as pd
import typer

from ai_trading_system.research_bench import trial as tr
from ai_trading_system.research_bench.data import BenchData
from ai_trading_system.research_bench.settings import LoadedSettings
from ai_trading_system.research_data import qqq_long_history as qlh
from ai_trading_system.research_probes import policy_table as pt

RUN_ID = "TRADING-2565-104"
OWNER_DECISION = "owner_decision:TRADING-2565:2026-11-01:research_loop_p7"
VARIANT = "v0_close"
MARKETS: tuple[tr.Market, ...] = ("qqq", "us_market")
MARKET_TEXT = pt.MARKET_TEXT
# Frozen print-only baseline under the named decision and charter section 5.2,
# step 0; exit at the first-line review, with any research opening separately signed.
SMA_DAYS = 200
SLOPE_LAG = 20
CONFIRM_DAYS = 3
COOLDOWN = 20
HOLD_DAYS = 60
FULL_RANGE = {"qqq": "1999_2010", "us_market": "1929_2010"}
ERAS = (
    ("1929_1961", date(1929, 1, 1), date(1961, 12, 31)),
    ("1962_1989", date(1962, 1, 1), date(1989, 12, 31)),
    ("1990_2010", date(1990, 1, 1), date(2010, 12, 31)),
)
# Already-read original-rule dates, not outcomes of this close-only variant.
ORIGINAL_ENTRIES = (
    "2004-10-21",
    "2007-08-20",
    "2010-05-10",
    "2010-06-15",
    "2010-08-02",
    "2010-09-09",
)
CHECK_JSON = "entry_timing_counts.json"
app = typer.Typer(help="200 日线回踩收盘价版：只数次数，不印收益，不进账本。")


class Closes(NamedTuple):
    dates: tuple[date, ...]
    close: tuple[float, ...]
    missing: int


class Touch(NamedTuple):
    position: int
    outcome: str
    entry: int | None
    filter: bool
    counted: bool


def closes(returns: pd.Series, end: date) -> Closes:
    dates: list[date] = []
    values: list[float] = []
    missing = 0
    level = 1.0
    for timestamp, raw in zip(
        pd.DatetimeIndex(returns.index), returns.to_numpy(dtype=float), strict=True
    ):
        day = timestamp.date()
        if day > end:
            continue
        value = float(raw)
        if not math.isfinite(value):
            if values:
                missing += 1
            continue
        level *= 1.0 + value
        dates.append(day)
        values.append(level)
    return Closes(tuple(dates), tuple(values), missing)


def moving_average(close: Sequence[float]) -> tuple[float | None, ...]:
    return tuple(
        None if p < SMA_DAYS - 1 else math.fsum(close[p - SMA_DAYS + 1 : p + 1]) / SMA_DAYS
        for p in range(len(close))
    )


def scan(close: Sequence[float], start: int = 0) -> list[Touch]:
    averages = moving_average(close)
    touches: list[Touch] = []
    last_entry: int | None = None
    t = max(start, SMA_DAYS)
    while t < len(close):
        previous, current = averages[t - 1], averages[t]
        if (
            previous is None
            or current is None
            or not close[t - 1] > previous
            or not close[t] <= current
            or (last_entry is not None and t - last_entry < COOLDOWN)
        ):
            t += 1
            continue
        lagged = averages[t - SLOPE_LAG]
        passes = lagged is not None and current > lagged
        entry = t + CONFIRM_DAYS
        if entry >= len(close):
            touches.append(Touch(t, "pending", None, passes, False))
            break
        if all(close[p] >= current for p in range(t + 1, entry + 1)):
            touches.append(Touch(t, "signal", entry, passes, passes))
            last_entry = entry
        else:
            touches.append(Touch(t, "failed", None, passes, False))
        t += CONFIRM_DAYS + 1
    return touches


def held(length: int, entries: Sequence[int]) -> tuple[bool, ...]:
    mask = [False] * length
    for entry in entries:
        for p in range(entry + 1, min(entry + HOLD_DAYS + 1, length)):
            mask[p] = True
    return tuple(mask)


def count_range(
    series: Closes,
    touches: Sequence[Touch],
    mask: Sequence[bool],
    name: str,
    start: date,
    end: date,
) -> dict[str, Any]:
    sessions = sum(start <= day <= end for day in series.dates)
    inside = [t for t in touches if start <= series.dates[t.position] <= end]
    hold_days = sum(h and start <= day <= end for day, h in zip(series.dates, mask, strict=True))
    return {
        "range": name,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "sessions": sessions,
        "touches": len(inside),
        "signals": sum(t.outcome == "signal" for t in inside),
        "failed": sum(t.outcome == "failed" for t in inside),
        "pending": sum(t.outcome == "pending" for t in inside),
        "counted": sum(t.counted for t in inside),
        "hold_days": hold_days,
        "exposure": hold_days / sessions if sessions else None,
        "open_at_end": sum(
            t.counted and t.entry is not None and t.entry + HOLD_DAYS > len(series.dates) - 1
            for t in inside
        ),
    }


def check_inputs(
    datasets: Mapping[str, BenchData], loaded: LoadedSettings, gate: tr.TrialGate
) -> dict[str, Any]:
    markets: dict[str, Any] = {}
    for market in MARKETS:
        window = pt.market_window(loaded, gate, market)
        if window.end is None:
            raise ValueError("开发窗口必须有结束日期。")
        series = closes(datasets[market].returns["qqq"], window.end)
        first_in = next(
            (p for p, day in enumerate(series.dates) if day >= window.start), len(series.dates)
        )
        touches = scan(series.close, first_in)
        entries = [t.entry for t in touches if t.counted and t.entry is not None]
        mask = held(len(series.dates), entries)
        first_scan = max(first_in, SMA_DAYS)
        ranges = [(FULL_RANGE[market], window.start, window.end)]
        if market == "us_market":
            ranges.extend(
                (name, max(start, window.start), min(end, window.end)) for name, start, end in ERAS
            )
        row: dict[str, Any] = {
            "window_id": window.window_id,
            "start": window.start.isoformat(),
            "end": window.end.isoformat(),
            "first_close_date": series.dates[0].isoformat() if series.dates else None,
            "first_scan_date": series.dates[first_scan].isoformat()
            if first_scan < len(series.dates)
            else None,
            "warmed_at_start": first_in < len(series.dates)
            and first_in >= SMA_DAYS + SLOPE_LAG - 1,
            "missing_days": series.missing,
            "ranges": [
                count_range(series, touches, mask, name, start, end) for name, start, end in ranges
            ],
        }
        if market == "qqq":
            row["entries"] = [series.dates[entry].isoformat() for entry in entries]
            row["original_rule_entries_2000_2010"] = list(ORIGINAL_ENTRIES)
        markets[market] = row
    return {
        "run_id": RUN_ID,
        "owner_decision": OWNER_DECISION,
        "variant": VARIANT,
        "rule": {
            "sma_days": SMA_DAYS,
            "slope_lag": SLOPE_LAG,
            "confirm_days": CONFIRM_DAYS,
            "cooldown": COOLDOWN,
            "hold_days": HOLD_DAYS,
        },
        "markets": markets,
        "ok": all(
            row["missing_days"] == 0 and row["first_scan_date"] is not None
            for row in markets.values()
        )
        and markets["us_market"]["warmed_at_start"],
    }


def summary_line(check: Mapping[str, Any]) -> str:
    markets = check["markets"]
    parts = []
    for market in MARKETS:
        row = markets[market]["ranges"][0]
        parts.append(
            f"{MARKET_TEXT[market]} 触线 {row['touches']}、信号 {row['signals']}、"
            f"计入 {row['counted']}、持有 {row['hold_days']} 天"
        )
    eras = "，".join(
        f"{row['range'].replace('_', '–')} 计入 {row['counted']}"
        for row in markets["us_market"]["ranges"][1:]
    )
    return (
        f"entry_timing_counts: {'；'.join(parts)}（{eras}）；"
        f"输入检查 {'PASS' if check['ok'] else 'FAIL'}"
    )


def load_data(
    snapshot_root: Path,
) -> tuple[dict[str, BenchData], tr.TrialGate, LoadedSettings, Path]:
    return pt.load_data(snapshot_root)


def write_check(check: Mapping[str, Any], out: Path) -> None:
    out.mkdir(parents=True, exist_ok=True)
    out.joinpath(CHECK_JSON).write_text(
        json.dumps(check, indent=2, ensure_ascii=False, allow_nan=False) + "\n", encoding="utf-8"
    )


@app.callback()
def main() -> None:
    """只数开发窗口的回踩次数，不印收益。"""


@app.command("check")
def check_command(
    out: Annotated[Path, typer.Option(help="次数结果目录，已有结果拒绝覆盖。")],
    root: Annotated[Path, typer.Option(help="长历史快照根目录。")] = qlh.DEFAULT_SNAPSHOT_ROOT,
) -> None:
    if out.joinpath(CHECK_JSON).exists():
        typer.echo("entry_timing_counts: 已有结果，拒绝覆盖。")
        raise typer.Exit(code=1)
    try:
        datasets, gate, loaded, _ = load_data(root)
    except (OSError, ValueError, KeyError, tr.TrialError) as exc:
        lines = str(exc).splitlines()
        message = lines[0] if lines else ""
        typer.echo(f"entry_timing_counts: 数据载入失败：{message}")
        raise typer.Exit(code=1) from None
    check = check_inputs(datasets, loaded, gate)
    write_check(check, out)
    typer.echo(summary_line(check))
    if not check["ok"]:
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
