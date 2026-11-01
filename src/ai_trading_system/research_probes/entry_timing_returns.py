"""Returns after the 200-day pullback signal on closes, beside any day of the same year.

TRADING-2565-105, owner_decision:TRADING-2565:2026-11-01:entry_timing_v0_returns:
Baker asked on 2026-11-01 for v0_close's counted signals' 20/60-session returns
and the entry year's any-day averages, with and without an 8% closing stop,
for his own review. Only development windows (QQQ 1999–2010, US market
1929–2010) are used, cut at 2010-12-31. Print only: no verdict or accounting
entries, no gate or ruler changes, and no daily-report or broker action.
"""

from __future__ import annotations

import json
import math
from collections.abc import Mapping, Sequence
from datetime import date
from pathlib import Path
from typing import Annotated, Any, NamedTuple

import typer

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.core.provenance import git_provenance
from ai_trading_system.research_bench import trial as tr
from ai_trading_system.research_bench.data import BenchData
from ai_trading_system.research_bench.settings import LoadedSettings
from ai_trading_system.research_data import qqq_long_history
from ai_trading_system.research_probes import entry_timing_counts as ec

RUN_ID = "TRADING-2565-105"
OWNER_DECISION = "owner_decision:TRADING-2565:2026-11-01:entry_timing_v0_returns"
VARIANT = ec.VARIANT
# Temporary print-only baseline under OWNER_DECISION; exit at Baker's review.
# These are requested comparisons, not acceptance or promotion thresholds.
HORIZONS = (20, 60)
STOP = 0.08
RESULT_JSON = "entry_timing_returns.json"
RESULT_MD = "entry_timing_returns.md"
RULE_TEXT = (
    "规则：v0_close（收盘价触线、3 天确认、20 天冷却、均线高于 20 天前才计入），"
    "入场在确认第 3 天的收盘。h 天收益 = 第 h 个收盘 ÷ 入场收盘 − 1；"
    "8% 止损 = 持有期内第一个收盘 ≤ 入场 × 0.92 就按那天的收盘出场。"
    "当年任一天 = 入场那一年里每个交易日都当入场日、同样算法的平均，"
    "h 天后超出 2010-12-31 的日子不算。只用开发窗口，数据切到 2010-12-31；"
    "1952 年以前有周六交易日，没有调整。只印，不判。"
)
app = typer.Typer(help="200 日线回踩收盘价版：只印收益对比，不判，不进账本。")


class Outcome(NamedTuple):
    plain: float
    stop: float
    stop_day: int | None


def outcome(close: Sequence[float], entry: int, horizon: int) -> Outcome | None:
    if entry < 0 or entry + horizon >= len(close):
        return None
    base = close[entry]
    plain = close[entry + horizon] / base - 1.0
    for p in range(entry + 1, entry + horizon + 1):
        if close[p] / base <= 1.0 - STOP:
            return Outcome(plain, close[p] / base - 1.0, p - entry)
    return Outcome(plain, plain, None)


def baseline(
    dates: Sequence[date], outcomes: Sequence[Outcome | None], start: date, end: date
) -> dict[str, Any]:
    kept = [
        value
        for day, value in zip(dates, outcomes, strict=True)
        if start <= day <= end and value is not None
    ]
    n = len(kept)
    return {
        "n": n,
        "plain_mean": math.fsum(o.plain for o in kept) / n if n else None,
        "stop_mean": math.fsum(o.stop for o in kept) / n if n else None,
        "plain_up_share": sum(o.plain > 0.0 for o in kept) / n if n else None,
        "stop_up_share": sum(o.stop > 0.0 for o in kept) / n if n else None,
    }


def _range_summary(
    signals: Sequence[Mapping[str, Any]],
    horizon: int,
    every: Mapping[str, Any],
) -> dict[str, Any]:
    cells = [s[f"h{horizon}"] for s in signals if s[f"h{horizon}"] is not None]
    k = len(cells)
    return {
        "settled": k,
        "plain_mean": math.fsum(c["plain"] for c in cells) / k if k else None,
        "stop_mean": math.fsum(c["stop"] for c in cells) / k if k else None,
        "year_plain_mean": math.fsum(c["year_plain"] for c in cells) / k if k else None,
        "year_stop_mean": math.fsum(c["year_stop"] for c in cells) / k if k else None,
        "plain_up": sum(c["plain"] > 0.0 for c in cells),
        "stop_up": sum(c["stop"] > 0.0 for c in cells),
        "plain_beats_year": sum(c["plain"] > c["year_plain"] for c in cells),
        "stop_beats_year": sum(c["stop"] > c["year_stop"] for c in cells),
        "stopped": sum(c["stop_day"] is not None for c in cells),
        "any_day_n": every["n"],
        "any_day_plain_mean": every["plain_mean"],
        "any_day_stop_mean": every["stop_mean"],
        "any_day_plain_up_share": every["plain_up_share"],
        "any_day_stop_up_share": every["stop_up_share"],
    }


def market_returns(series: ec.Closes, market: str, start: date, end: date) -> dict[str, Any]:
    first = next((p for p, day in enumerate(series.dates) if day >= start), len(series.dates))
    touches = ec.scan(series.close, first)
    outcomes = {
        h: [None if d < first else outcome(series.close, d, h) for d in range(len(series.close))]
        for h in HORIZONS
    }
    eras = [(name, max(a, start), min(b, end)) for name, a, b in ec.ERAS]
    years: dict[tuple[int, int], dict[str, Any]] = {}
    signals: list[dict[str, Any]] = []
    for touch in touches:
        if not touch.counted or touch.entry is None:
            continue
        touch_day, entry_day = series.dates[touch.position], series.dates[touch.entry]
        name = ec.FULL_RANGE[market]
        if market == "us_market":
            name = next((label for label, a, b in eras if a <= touch_day <= b), "")
            if not name:
                raise ValueError(f"触线日 {touch_day} 不在美股大盘的年代段内。")
        row: dict[str, Any] = {
            "touch": touch_day.isoformat(),
            "entry": entry_day.isoformat(),
            "year": entry_day.year,
            "range": name,
        }
        for h in HORIZONS:
            one = outcomes[h][touch.entry]
            if one is None:
                row[f"h{h}"] = None
                continue
            key = (h, entry_day.year)
            if key not in years:
                years[key] = baseline(
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
    spans = [(ec.FULL_RANGE[market], start, end)]
    if market == "us_market":
        spans.extend(eras)
    ranges: list[dict[str, Any]] = []
    for name, a, b in spans:
        inside = [s for s in signals if a <= date.fromisoformat(s["touch"]) <= b]
        summary: dict[str, Any] = {
            "range": name,
            "start": a.isoformat(),
            "end": b.isoformat(),
            "signals": len(inside),
        }
        for h in HORIZONS:
            summary[f"h{h}"] = _range_summary(inside, h, baseline(series.dates, outcomes[h], a, b))
        ranges.append(summary)
    return {"signals": signals, "ranges": ranges}


def run_probe(
    datasets: Mapping[str, BenchData],
    loaded: LoadedSettings,
    gate: tr.TrialGate,
    *,
    provenance: Mapping[str, Any],
) -> dict[str, Any]:
    check = ec.check_inputs(datasets, loaded, gate)
    result: dict[str, Any] = {
        "run_id": RUN_ID,
        "owner_decision": OWNER_DECISION,
        "variant": VARIANT,
        "rule": {**check["rule"], "horizons": list(HORIZONS), "stop": STOP},
        "provenance": dict(provenance),
        "check": check,
        "markets": {},
        "complete": False,
    }
    if not check["ok"]:
        return result
    for market in ec.MARKETS:
        row = check["markets"][market]
        start, end = date.fromisoformat(row["start"]), date.fromisoformat(row["end"])
        series = ec.closes(datasets[market].returns["qqq"], end)
        result["markets"][market] = {
            "window_id": row["window_id"],
            "start": row["start"],
            "end": row["end"],
            **market_returns(series, market, start, end),
        }
    result["complete"] = True
    return result


def pct(value: float | None) -> str:
    return "—" if value is None else f"{100.0 * value:+.2f}%"


def share(value: float | None) -> str:
    return "—" if value is None else f"{100.0 * value:.1f}%"


def _line(cells: Sequence[str]) -> str:
    return "| " + " | ".join(cells) + " |"


def render_markdown(result: Mapping[str, Any]) -> str:
    lines = [
        "# 200 日线回踩收盘价版：信号后 20/60 天收益对当年任一天入场（TRADING-2565-105）",
        "",
        RULE_TEXT,
    ]
    for market in ec.MARKETS:
        found = result["markets"][market]
        lines += [
            "",
            f"## {ec.MARKET_TEXT[market]}",
            "",
            "| 触线日 | 入场日 | 20 天 | 20 天止损 | 当年 20 天 | 当年 20 天止损 "
            "| 60 天 | 60 天止损 | 当年 60 天 | 当年 60 天止损 |",
            "|" + "---|" * 10,
        ]
        for signal in found["signals"]:
            cells = [signal["touch"], signal["entry"]]
            for h in HORIZONS:
                cell = signal[f"h{h}"]
                if cell is None:
                    cells += ["—"] * 4
                    continue
                stop = pct(cell["stop"])
                if cell["stop_day"] is not None:
                    stop += f"（第 {cell['stop_day']} 天止损）"
                cells += [pct(cell["plain"]), stop, pct(cell["year_plain"]), pct(cell["year_stop"])]
            lines.append(_line(cells))
        lines += [
            "",
            "| 范围 | 天数 | 信号 | 均值 | 止损均值 | 当年均值 | 当年止损均值 | 上涨 | 止损上涨 "
            "| 赢当年 | 止损赢当年 | 止损触发 | 任一天均值 | 任一天止损均值 | 任一天上涨比例 "
            "| 任一天止损上涨比例 |",
            "|" + "---|" * 16,
        ]
        for row in found["ranges"]:
            for h in HORIZONS:
                values = row[f"h{h}"]
                k = values["settled"]
                cells = [row["range"].replace("_", "–"), str(h), str(k)]
                cells += [
                    pct(values[key])
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
                cells += [pct(values["any_day_plain_mean"]), pct(values["any_day_stop_mean"])]
                cells += [
                    share(values["any_day_plain_up_share"]),
                    share(values["any_day_stop_up_share"]),
                ]
                lines.append(_line(cells))
    return "\n".join(lines) + "\n"


def summary_line(result: Mapping[str, Any], out: Path) -> str:
    parts = []
    for market in ec.MARKETS:
        row = result["markets"][market]["ranges"][0]
        values = row["h60"]
        parts.append(
            f"{ec.MARKET_TEXT[market]} 信号 {row['signals']}，60 天均值 "
            f"{pct(values['plain_mean'])}（止损 {pct(values['stop_mean'])}），当年任一天 "
            f"{pct(values['year_plain_mean'])}（止损 {pct(values['year_stop_mean'])}）"
        )
    return f"entry_timing_returns: {'；'.join(parts)}；结果目录 {out}"


def load_data(
    snapshot_root: Path,
) -> tuple[dict[str, BenchData], tr.TrialGate, LoadedSettings, Path]:
    return ec.load_data(snapshot_root)


def write_result(result: Mapping[str, Any], out: Path) -> None:
    out.mkdir(parents=True, exist_ok=True)
    out.joinpath(RESULT_JSON).write_text(
        json.dumps(result, indent=2, ensure_ascii=False, allow_nan=False) + "\n", encoding="utf-8"
    )
    if result["complete"]:
        out.joinpath(RESULT_MD).write_text(render_markdown(result), encoding="utf-8")


@app.callback()
def main() -> None:
    """只印开发窗口的收益对比，不判、不进账本。"""


@app.command("run")
def run_command(
    out: Annotated[Path, typer.Option(help="收益结果目录，已有结果拒绝覆盖。")],
    root: Annotated[
        Path, typer.Option(help="长历史快照根目录。")
    ] = qqq_long_history.DEFAULT_SNAPSHOT_ROOT,
) -> None:
    if any(out.joinpath(name).exists() for name in (RESULT_JSON, RESULT_MD)):
        typer.echo("entry_timing_returns: 已有结果，拒绝覆盖。")
        raise typer.Exit(code=1)
    try:
        datasets, gate, loaded, _ = load_data(root)
    except (OSError, ValueError, KeyError, tr.TrialError) as exc:
        lines = str(exc).splitlines()
        message = lines[0] if lines else ""
        typer.echo(f"entry_timing_returns: 数据载入失败：{message}")
        raise typer.Exit(code=1) from None
    result = run_probe(datasets, loaded, gate, provenance=git_provenance(PROJECT_ROOT))
    write_result(result, out)
    if not result["complete"]:
        typer.echo("entry_timing_returns: 输入检查 FAIL，没有算收益")
        raise typer.Exit(code=1)
    typer.echo(summary_line(result, out))


if __name__ == "__main__":
    app()
