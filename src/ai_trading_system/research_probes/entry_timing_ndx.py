"""The 200-day pullback rule on closes, on the Nasdaq-100 price index 1986–1999.

TRADING-2565-107, owner_decision:TRADING-2565:2026-11-02:ndx_price_1986_1999:
Baker asked on 2026-11-02 to see QQQ's tracked index before QQQ began trading.
The registered ndx_price_1986_1999 window uses the long-history snapshot's FRED
Nasdaq-100 price index, without dividends, from 1986-01-02 through 1999-03-09.
Only v0_close is printed: counts as in TRADING-2565-104, then 20/60-session
returns with and without an 8% closing stop beside any day of the entry year,
as in TRADING-2565-105. Print only: no verdict or accounting entries, no gate
or ruler changes, and no daily-report or broker action.
"""

from __future__ import annotations

import json
import math
from collections.abc import Mapping, Sequence
from datetime import date, timedelta
from pathlib import Path
from typing import Annotated, Any

import typer

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.core.provenance import git_provenance
from ai_trading_system.paths import join_root
from ai_trading_system.research_bench.settings import LoadedSettings, load_settings
from ai_trading_system.research_data import qqq_long_history as qlh
from ai_trading_system.research_probes import entry_timing_counts as ec
from ai_trading_system.research_probes import entry_timing_returns as er
from ai_trading_system.yaml_loader import safe_load_yaml_path

RUN_ID = "TRADING-2565-107"
OWNER_DECISION = "owner_decision:TRADING-2565:2026-11-02:ndx_price_1986_1999"
VARIANT = ec.VARIANT
WINDOW_ID = "ndx_price_1986_1999"
WINDOW_ROLE = "owner_print_only"
START = date(1986, 1, 2)
END = date(1999, 3, 9)
SNAPSHOT_ID = "qlh-20261013-0b0df53cb1a2"
COLUMN = "ndx_price"
RANGE = "1986_1999"
TITLE = "纳斯达克 100 价格指数 1986–1999"
# Data-quality bound from the snapshot spec's suspicious_index_daily_return_abs.
MAX_MOVE = 0.25
# The last close may precede the window end by at most four calendar days.
END_SLACK_DAYS = 4
RESULT_JSON = "entry_timing_ndx.json"
RESULT_MD = "entry_timing_ndx.md"
RULE_TEXT = (
    "规则：v0_close（收盘价触线、3 天确认、20 天冷却、均线高于 20 天前才计入），"
    "入场在确认第 3 天的收盘；h 天收益和 8% 收盘止损同 TRADING-2565-105。"
    "当年任一天 = 入场那一年里每个交易日都当入场日、同样算法的平均，"
    "h 天后超出 1999-03-09 的日子不算。"
    "数据：纳斯达克 100 价格指数（QQQ 跟踪的指数，FRED，不含分红），"
    "1986-01-02..1999-03-09；和前一个收盘同值的日子当作休市沿用前值，去掉。"
    "只印，不判。"
)
app = typer.Typer(help="200 日线回踩收盘价版：只印纳斯达克 100 指数的次数和收益，不判。")


def registered_window(loaded: LoadedSettings) -> dict[str, Any]:
    raw = safe_load_yaml_path(join_root(loaded.root, loaded.settings.window_registry))
    windows = raw.get("windows") if isinstance(raw, Mapping) else None
    entry = windows.get(WINDOW_ID) if isinstance(windows, Mapping) else None
    if not isinstance(entry, Mapping):
        raise ValueError(f"窗口登记表里没有 {WINDOW_ID}。")
    found = (str(entry.get("start")), str(entry.get("end")), entry.get("role"))
    if found != (START.isoformat(), END.isoformat(), WINDOW_ROLE):
        raise ValueError(f"窗口 {WINDOW_ID} 的登记和模块不一致：{found}")
    return {
        "window_id": WINDOW_ID,
        "role": WINDOW_ROLE,
        "start": START.isoformat(),
        "end": END.isoformat(),
    }


def prices(levels: Sequence[tuple[date, float]]) -> tuple[ec.Closes, tuple[date, ...]]:
    dates: list[date] = []
    values: list[float] = []
    flat: list[date] = []
    missing = 0
    for day, value in levels:
        if not START <= day <= END:
            continue
        if not math.isfinite(value):
            if values:
                missing += 1
            continue
        if values and value == values[-1]:
            flat.append(day)
            continue
        dates.append(day)
        values.append(value)
    return ec.Closes(tuple(dates), tuple(values), missing), tuple(flat)


def largest_move(series: ec.Closes) -> dict[str, Any] | None:
    if len(series.close) < 2:
        return None
    moves = [series.close[p] / series.close[p - 1] - 1.0 for p in range(1, len(series.close))]
    position = max(range(len(moves)), key=lambda p: abs(moves[p]))
    return {"date": series.dates[position + 1].isoformat(), "return": moves[position]}


def check_inputs(
    series: ec.Closes, flat: Sequence[date], window: Mapping[str, Any]
) -> dict[str, Any]:
    touches = ec.scan(series.close, 0)
    entries = [t.entry for t in touches if t.counted and t.entry is not None]
    mask = ec.held(len(series.dates), entries)
    move = largest_move(series)
    row: dict[str, Any] = {
        **dict(window),
        "first_close_date": series.dates[0].isoformat() if series.dates else None,
        "last_close_date": series.dates[-1].isoformat() if series.dates else None,
        "first_scan_date": series.dates[ec.SMA_DAYS].isoformat()
        if len(series.dates) > ec.SMA_DAYS
        else None,
        "missing_days": series.missing,
        "flat_days": len(flat),
        "flat_dates": [day.isoformat() for day in flat],
        "largest_move": move,
        "ranges": [ec.count_range(series, touches, mask, RANGE, START, END)],
        "entries": [series.dates[entry].isoformat() for entry in entries],
    }
    ok = (
        series.missing == 0
        and bool(series.dates)
        and series.dates[0] == START
        and series.dates[-1] >= END - timedelta(days=END_SLACK_DAYS)
        and row["first_scan_date"] is not None
        and move is not None
        and abs(move["return"]) <= MAX_MOVE
    )
    return {
        "run_id": RUN_ID,
        "owner_decision": OWNER_DECISION,
        "variant": VARIANT,
        "rule": {
            "sma_days": ec.SMA_DAYS,
            "slope_lag": ec.SLOPE_LAG,
            "confirm_days": ec.CONFIRM_DAYS,
            "cooldown": ec.COOLDOWN,
            "hold_days": ec.HOLD_DAYS,
        },
        "ndx": row,
        "ok": ok,
    }


def ndx_returns(series: ec.Closes, start: date, end: date) -> dict[str, Any]:
    first = next((p for p, day in enumerate(series.dates) if day >= start), len(series.dates))
    touches = ec.scan(series.close, first)
    outcomes = {
        h: [None if d < first else er.outcome(series.close, d, h) for d in range(len(series.close))]
        for h in er.HORIZONS
    }
    years: dict[tuple[int, int], dict[str, Any]] = {}
    signals: list[dict[str, Any]] = []
    for touch in touches:
        if not touch.counted or touch.entry is None:
            continue
        touch_day, entry_day = series.dates[touch.position], series.dates[touch.entry]
        row: dict[str, Any] = {
            "touch": touch_day.isoformat(),
            "entry": entry_day.isoformat(),
            "year": entry_day.year,
            "range": RANGE,
        }
        for h in er.HORIZONS:
            one = outcomes[h][touch.entry]
            if one is None:
                row[f"h{h}"] = None
                continue
            key = (h, entry_day.year)
            if key not in years:
                years[key] = er.baseline(
                    series.dates,
                    outcomes[h],
                    max(start, date(entry_day.year, 1, 1)),
                    min(end, date(entry_day.year, 12, 31)),
                )
            year = years[key]
            row[f"h{h}"] = {
                "plain": one.plain,
                "stop": one.stop,
                "stop_day": one.stop_day,
                "year_n": year["n"],
                "year_plain": year["plain_mean"],
                "year_stop": year["stop_mean"],
            }
        signals.append(row)
    inside = [s for s in signals if start <= date.fromisoformat(s["touch"]) <= end]
    summary: dict[str, Any] = {
        "range": RANGE,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "signals": len(inside),
    }
    for h in er.HORIZONS:
        summary[f"h{h}"] = er._range_summary(
            inside, h, er.baseline(series.dates, outcomes[h], start, end)
        )
    return {"signals": signals, "ranges": [summary]}


def run_probe(
    levels: Sequence[tuple[date, float]],
    source: Mapping[str, Any],
    window: Mapping[str, Any],
    *,
    provenance: Mapping[str, Any],
) -> dict[str, Any]:
    series, flat = prices(levels)
    check = check_inputs(series, flat, window)
    result: dict[str, Any] = {
        "run_id": RUN_ID,
        "owner_decision": OWNER_DECISION,
        "variant": VARIANT,
        "rule": {**check["rule"], "horizons": list(er.HORIZONS), "stop": er.STOP},
        "provenance": dict(provenance),
        "source": dict(source),
        "check": check,
        "returns": None,
        "complete": False,
    }
    if not check["ok"]:
        return result
    result["returns"] = ndx_returns(series, START, END)
    result["complete"] = True
    return result


def render_markdown(result: Mapping[str, Any]) -> str:
    row = result["check"]["ndx"]
    c = row["ranges"][0]
    move = row["largest_move"]
    flat = "、".join(row["flat_dates"]) or "无"
    lines = [
        f"# 200 日线回踩收盘价版：{TITLE}（{RUN_ID}）",
        "",
        RULE_TEXT,
        "",
        f"数据：快照 `{result['source']['snapshot_id']}` 的 `{COLUMN}`，"
        f"{row['first_close_date']}..{row['last_close_date']}，{c['sessions']} 个收盘；"
        f"去掉的同值日 {row['flat_days']} 天（{flat}）；"
        f"最大单日变动 {er.pct(move['return'])}（{move['date']}）。",
        "",
        "## 次数",
        "",
        "| 范围 | 交易日 | 触线 | 信号 | 失败 | 待定 | 计入 | 持有天数 | 平均敞口 "
        "| 到期末不满 60 天 |",
        "|" + "---|" * 10,
        er._line(
            [RANGE.replace("_", "–")]
            + [
                str(c[key])
                for key in (
                    "sessions",
                    "touches",
                    "signals",
                    "failed",
                    "pending",
                    "counted",
                    "hold_days",
                )
            ]
            + [er.share(c["exposure"]), str(c["open_at_end"])]
        ),
        "",
        "## 收益",
        "",
        "| 触线日 | 入场日 | 20 天 | 20 天止损 | 当年 20 天 | 当年 20 天止损 "
        "| 60 天 | 60 天止损 | 当年 60 天 | 当年 60 天止损 |",
        "|" + "---|" * 10,
    ]
    found = result["returns"]
    for signal in found["signals"]:
        cells = [signal["touch"], signal["entry"]]
        for h in er.HORIZONS:
            cell = signal[f"h{h}"]
            if cell is None:
                cells += ["—"] * 4
                continue
            stop = er.pct(cell["stop"])
            if cell["stop_day"] is not None:
                stop += f"（第 {cell['stop_day']} 天止损）"
            cells += [
                er.pct(cell["plain"]),
                stop,
                er.pct(cell["year_plain"]),
                er.pct(cell["year_stop"]),
            ]
        lines.append(er._line(cells))
    lines += [
        "",
        "| 范围 | 天数 | 信号 | 均值 | 止损均值 | 当年均值 | 当年止损均值 | 上涨 | 止损上涨 "
        "| 赢当年 | 止损赢当年 | 止损触发 | 任一天均值 | 任一天止损均值 | 任一天上涨比例 "
        "| 任一天止损上涨比例 |",
        "|" + "---|" * 16,
    ]
    for span in found["ranges"]:
        for h in er.HORIZONS:
            values = span[f"h{h}"]
            k = values["settled"]
            cells = [span["range"].replace("_", "–"), str(h), str(k)]
            cells += [
                er.pct(values[key])
                for key in ("plain_mean", "stop_mean", "year_plain_mean", "year_stop_mean")
            ]
            cells += [
                f"{values[key]}/{k}"
                for key in (
                    "plain_up",
                    "stop_up",
                    "plain_beats_year",
                    "stop_beats_year",
                    "stopped",
                )
            ]
            cells += [er.pct(values["any_day_plain_mean"]), er.pct(values["any_day_stop_mean"])]
            cells += [
                er.share(values["any_day_plain_up_share"]),
                er.share(values["any_day_stop_up_share"]),
            ]
            lines.append(er._line(cells))
    return "\n".join(lines) + "\n"


def summary_line(result: Mapping[str, Any], out: Path) -> str:
    c = result["check"]["ndx"]["ranges"][0]
    r = result["returns"]["ranges"][0]
    parts = []
    for h in reversed(er.HORIZONS):
        values = r[f"h{h}"]
        parts.append(
            f"{h} 天均值 {er.pct(values['plain_mean'])}（止损 {er.pct(values['stop_mean'])}），"
            f"当年任一天 {er.pct(values['year_plain_mean'])}"
            f"（止损 {er.pct(values['year_stop_mean'])}）"
        )
    return (
        f"entry_timing_ndx: {TITLE} 触线 {c['touches']}、信号 {c['signals']}、"
        f"计入 {c['counted']}；{'；'.join(parts)}；结果目录 {out}"
    )


def load_data(
    snapshot_root: Path,
) -> tuple[list[tuple[date, float]], dict[str, Any], dict[str, Any]]:
    import pandas as pd

    window = registered_window(load_settings())
    daily, report = qlh.load_daily(qlh.resolve_snapshot_dir(snapshot_root, SNAPSHOT_ID))
    if report.get("snapshot_id") != SNAPSHOT_ID:
        raise ValueError(f"快照不是 {SNAPSHOT_ID}。")
    column = daily[COLUMN]
    levels = [
        (timestamp.date(), float(value))
        for timestamp, value in zip(
            pd.DatetimeIndex(column.index), column.to_numpy(dtype=float), strict=True
        )
        if timestamp.date() <= END
    ]
    source = {
        "snapshot_id": SNAPSHOT_ID,
        "column": COLUMN,
        "check_status": report.get("status"),
        "series_csv_sha256": report.get("series_csv_sha256"),
        "check_warnings": sorted(
            {
                str(issue.get("code"))
                for issue in report.get("issues", [])
                if isinstance(issue, dict) and issue.get("severity") == "WARN"
            }
        ),
    }
    return levels, source, window


def write_result(result: Mapping[str, Any], out: Path) -> None:
    out.mkdir(parents=True, exist_ok=True)
    out.joinpath(RESULT_JSON).write_text(
        json.dumps(result, indent=2, ensure_ascii=False, allow_nan=False) + "\n", encoding="utf-8"
    )
    if result["complete"]:
        out.joinpath(RESULT_MD).write_text(render_markdown(result), encoding="utf-8")


@app.callback()
def main() -> None:
    """只印登记窗口的指数次数和收益，不判、不进账本。"""


@app.command("run")
def run_command(
    out: Annotated[Path, typer.Option(help="结果目录，已有结果拒绝覆盖。")],
    root: Annotated[Path, typer.Option(help="长历史快照根目录。")] = qlh.DEFAULT_SNAPSHOT_ROOT,
) -> None:
    if any(out.joinpath(name).exists() for name in (RESULT_JSON, RESULT_MD)):
        typer.echo("entry_timing_ndx: 已有结果，拒绝覆盖。")
        raise typer.Exit(code=1)
    try:
        levels, source, window = load_data(root)
    except (OSError, ValueError, KeyError) as exc:
        lines = str(exc).splitlines()
        message = lines[0] if lines else ""
        typer.echo(f"entry_timing_ndx: 数据载入失败：{message}")
        raise typer.Exit(code=1) from None
    result = run_probe(levels, source, window, provenance=git_provenance(PROJECT_ROOT))
    write_result(result, out)
    if not result["complete"]:
        typer.echo("entry_timing_ndx: 输入检查 FAIL，没有算收益")
        raise typer.Exit(code=1)
    typer.echo(summary_line(result, out))


if __name__ == "__main__":
    app()
