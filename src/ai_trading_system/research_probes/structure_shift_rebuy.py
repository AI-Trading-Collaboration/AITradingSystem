"""The structure-shift strategy's buy-back above the 200-day line (TRADING-2565-124).

Under owner_decision:TRADING-2565:2026-11-04:structure_shift_rebuy_above_line,
reprint TRADING-2565-122 on QQQ 1999–2010 and the registered
ndx_price_1986_1999 and qqq_after_2010_owner_print windows, checking every
printed total return. Append two rows to its below-line X2 control: buy back
at the first close above the line after selling, or at the second of two
consecutive such closes. Read only QQQ and its own index. Print only: no
verdicts, ledger entries, gate or ruler changes, daily reports or broker actions.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import date
from pathlib import Path
from typing import Annotated, Any

import typer

from ai_trading_system.research_bench import trial as tr
from ai_trading_system.research_bench.data import BenchData
from ai_trading_system.research_bench.settings import LoadedSettings
from ai_trading_system.research_data import qqq_long_history as qlh
from ai_trading_system.research_probes import entry_exit_combo as cb
from ai_trading_system.research_probes import entry_structure_shift as es
from ai_trading_system.research_probes import entry_timing_counts as ec
from ai_trading_system.research_probes import entry_timing_ndx as en
from ai_trading_system.research_probes import exit_rules_counts as xc
from ai_trading_system.research_probes import exit_rules_returns as xr
from ai_trading_system.research_probes import structure_shift_periods as sp

RUN_ID = "TRADING-2565-124"
OWNER_DECISION = "owner_decision:TRADING-2565:2026-11-04:structure_shift_rebuy_above_line"
SPANS = sp.SPANS
BASE = "x2_s5_under"
# Consecutive closes strictly above the 200-day line, all after the sale close.
BACK = {"x2_s5_under_back1": 1, "x2_s5_under_back2": 2}
SHOWN = ("x2_s5", BASE, *BACK, "x2_s5_line", "x2_a", "hold")
TRIPS_SHOWN = (BASE, *BACK)
# Printed by TRADING-2565-122 (structure_shift_periods.json), for reproduction only.
PERIODS_TOTALS: dict[str, dict[str, float]] = {
    "dev": {
        "x2_s5": 0.761935,
        "x3_s5": 0.732322,
        "x2x3_s5": 0.358581,
        "x2_s2": -0.157292,
        "x3_s2": -0.018409,
        "x2x3_s2": -0.383552,
        "x2_a": -0.087305,
        "x3_a": 0.217754,
        "x2x3_a": -0.038947,
        "hold": -0.370499,
        "x2_s5_under": 0.985413,
        "x3_s5_under": 1.092173,
        "x2x3_s5_under": 1.011555,
        "x2_s5_line": 0.676461,
    },
    "ndx": {
        "x2_s5": 6.897791,
        "x3_s5": 2.534258,
        "x2x3_s5": 2.732365,
        "x2_s2": 8.340913,
        "x3_s2": 4.065815,
        "x2x3_s2": 3.609067,
        "x2_a": 1.197806,
        "x3_a": 0.705183,
        "x2x3_a": 0.812665,
        "hold": 13.360477,
        "x2_s5_under": 6.206591,
        "x3_s5_under": 4.565942,
        "x2x3_s5_under": 4.565942,
        "x2_s5_line": 7.250423,
    },
    "qqq": {
        "x2_s5": 1.598219,
        "x3_s5": 0.282725,
        "x2x3_s5": 0.337662,
        "x2_s2": 1.685064,
        "x3_s2": 0.393602,
        "x2x3_s2": 0.450635,
        "x2_a": 0.52942,
        "x3_a": -0.113774,
        "x2x3_a": -0.113774,
        "hold": 14.43185,
        "x2_s5_under": 4.842585,
        "x3_s5_under": 4.55145,
        "x2x3_s5_under": 4.581502,
        "x2_s5_line": 2.3364,
    },
}
PERIODS_FIRST_DAY = {"dev": "1999-12-23", "ndx": "1986-10-17", "qqq": "2011-01-03"}
PERIODS_SESSIONS = {"dev": 2771, "ndx": 3116, "qqq": 3966}
STRATEGY_TEXT = {
    **sp.STRATEGY_TEXT,
    "x2_s5_under_back1": "X2 回落 8%（只在 200 日线下算），A 加结构转变买回，"
    "卖出后收盘站上 200 日线就买回（左右各 5 天）",
    "x2_s5_under_back2": "X2 回落 8%（只在 200 日线下算），A 加结构转变买回，"
    "卖出后连续两个收盘站上 200 日线才买回（左右各 5 天）",
}
HOW_TEXT = {**sp.HOW_TEXT, "back": "收盘站上 200 日线"}
CHECK_JSON = "structure_shift_rebuy_check.json"
RESULT_JSON = "structure_shift_rebuy.json"
RESULT_MD = "structure_shift_rebuy.md"
RULE_TEXT = (
    "只印不判，不检验、不下结论，不是交易建议。三段都已看过 TRADING-2565-122 的结果；"
    "这次补的是设计上卖出以后没有回场路的缺口，沿用原有 200 日线，没有可调的数。"
    "先重算核对 TRADING-2565-122 的三段，满仓起点、交易日数和十四行"
    "总收益都等于它印的才印新行。结果只作描述，真检验只有前瞻记录。"
)
NOTE = (
    "在只于 200 日线下算的 X2 行上加两行：卖出后第一个收盘严格高于当天的 200 日线就买回，"
    "或连续两个收盘严格高于当天的线、在第二个收盘买回。从卖出的下一个收盘起计数，"
    "缺线或没有严格高于就重新计数。X2 和 L 止损卖出以后都用。和 v0_close 入场、"
    "结构转变入场谁先到用谁，同一个收盘依次算 v0_close、结构转变、站上 200 日线。"
    "这种买回不加 L 止损；别的规则不变。离 200 日线 = 出场响那个收盘 ÷ 当天的 200 日线 − 1。"
)
app = typer.Typer(help="结构转变策略补「站上 200 日线就买回」：只印不判。")


def back_day(
    close: Sequence[float], line: Sequence[float | None], v: int, confirm: int
) -> int | None:
    """Find the close completing the required above-line run after the sale."""
    run = 0
    for p in range(v + 1, len(close)):
        at = line[p]
        run = run + 1 if at is not None and close[p] > at else 0
        if run >= confirm:
            return p
    return None


def walk_back(
    data: xc.Prepared,
    entries: Sequence[int],
    moves: Sequence[es.Shift],
    s0: int,
    line: Sequence[float | None],
    confirm: int,
) -> cb.Route:
    """Keep the original below-line exit path, adding the above-line buy-back."""
    close = data.close
    n = len(close)
    held = [False] * n
    buys, sells = [s0], []
    trips: list[dict[str, Any]] = []
    b = s0
    floor: float | None = None
    while b < n - 1:
        fired = sp.exit_under("x2", data, line, b)
        if floor is not None:
            under = next((d for d in range(b + 1, n) if close[d] < floor), None)
            if under is not None and (fired is None or under < fired[0]):
                fired = (under, "L")
        if fired is None or fired[0] + 1 > n - 1:
            break
        d, reason = fired
        v = d + 1
        held[b + 1 : v + 1] = [True] * (v - b)
        sells.append(v)
        entry = next((e for e in entries if e > v), None)
        move = next((m for m in moves if m.entry > v), None)
        reach = back_day(close, line, v, confirm)
        r: int | None = None
        if entry is not None and (move is None or entry <= move.entry):
            r, how, floor = entry, "entry", None
        elif move is not None:
            r, how, floor = move.entry, "shift", close[move.low]
        else:
            how = "none"
        if reach is not None and (r is None or reach < r):
            r, how, floor = reach, "back", None
        last = r if r is not None else n - 1
        trips.append(
            {
                "decision": data.dates[d].isoformat(),
                "reason": reason,
                "sell": data.dates[v].isoformat(),
                "buy": data.dates[r].isoformat() if r is not None else None,
                "how": how,
                "days_out": last - v,
                "change": close[last] / close[v] - 1.0,
            }
        )
        if r is None:
            return cb.Route(tuple(held), tuple(buys), tuple(sells), tuple(trips))
        buys.append(r)
        b = r
    held[b + 1 :] = [True] * (n - 1 - b)
    return cb.Route(tuple(held), tuple(buys), tuple(sells), tuple(trips))


def span_result(series: ec.Closes, first_in: int) -> dict[str, Any]:
    """Preserve the prior tables and append the two authorized buy-back paths."""
    block = sp.span_result(series, first_in)
    dates, close = series.dates, series.close
    entries = sp.v0_entries(close, first_in)
    s0 = max(first_in, ec.SMA_DAYS)
    prepared = xc.prepare(dates, close)
    line = ec.moving_average(close)
    moves = es.shifts(close, line, 5, s0)
    positions = {day.isoformat(): p for p, day in enumerate(dates)}
    span = block["span"]
    for strategy, confirm in BACK.items():
        path = walk_back(prepared, entries, moves, s0, line, confirm)
        values = cb.equity(close, path, s0)
        for trip in path.trips:
            d = positions[trip["decision"]]
            at = line[d]
            if at is None:
                raise ValueError("出场响的收盘缺 200 日线。")
            trip["vs_line"] = close[d] / at - 1.0
        span["strategies"][strategy] = {
            **cb.metrics(dates, values, path, s0),
            "by_shift": sum(t["how"] == "shift" for t in path.trips),
            "low_stops": sum(t["reason"] == "L" for t in path.trips),
            "trips": list(path.trips),
            "by_line": 0,
        }
        annual = cb.year_returns(dates, values, s0)
        for year in span["years"]:
            year[strategy] = annual[year["year"]]
    for row in span["strategies"].values():
        row["by_back"] = sum(t["how"] == "back" for t in row["trips"])
    return block


def periods_check(name: str, block: Mapping[str, Any]) -> dict[str, Any]:
    """Compare all prior totals, the full-position start, and session count."""
    span = block["span"]
    first, sessions = span["first_day"], span["sessions"]
    totals = {s: round(span["strategies"][s]["total"], 6) for s in sp.STRATEGIES}
    return {
        "first_day": first,
        "sessions": sessions,
        "totals": totals,
        "ok": first == PERIODS_FIRST_DAY[name]
        and sessions == PERIODS_SESSIONS[name]
        and totals == PERIODS_TOTALS[name],
    }


def rule_block() -> dict[str, Any]:
    """Retain the prior rules and identify only the new buy-back conditions."""
    return {
        **sp.rule_block(),
        "back": {
            "strategies": list(BACK),
            "base": BASE,
            "confirm_closes": dict(BACK),
            "buy_back": (
                "close_ending_the_first_run_of_confirm_closes_strictly_above_sma200_after_the_sale_close"
            ),
            "after": ["X2", "L"],
            "order": ["entry", "shift", "back"],
            "low_stop": "none_after_back_buy_back",
        },
    }


def check_table(
    data: BenchData,
    loaded: LoadedSettings,
    gate: tr.TrialGate,
    window: Mapping[str, Any],
    levels: Sequence[tuple[date, float]],
    source: Mapping[str, Any],
    ndx_window: Mapping[str, Any],
) -> dict[str, Any]:
    """Reuse the two input checks without computing strategy outcomes."""
    q = sp.qqq_inputs(data, loaded, gate, window)
    ndx = sp.ndx_inputs(levels, ndx_window)
    return {
        "run_id": RUN_ID,
        "owner_decision": OWNER_DECISION,
        "source": {"qqq": {"snapshot_id": data.source.get("snapshot_id")}, "ndx": dict(source)},
        "rule": rule_block(),
        "check": {"qqq": q.check, "ndx": ndx.check},
        "ok": q.check["ok"] and ndx.check["ok"],
    }


def run_table(
    data: BenchData,
    loaded: LoadedSettings,
    gate: tr.TrialGate,
    window: Mapping[str, Any],
    levels: Sequence[tuple[date, float]],
    source: Mapping[str, Any],
    ndx_window: Mapping[str, Any],
) -> dict[str, Any]:
    """Expose spans only after both inputs and every reproduction check pass."""
    q = sp.qqq_inputs(data, loaded, gate, window)
    ndx = sp.ndx_inputs(levels, ndx_window)
    result: dict[str, Any] = {
        "run_id": RUN_ID,
        "owner_decision": OWNER_DECISION,
        "source": {"qqq": {"snapshot_id": data.source.get("snapshot_id")}, "ndx": dict(source)},
        "rule": rule_block(),
        "check": {"qqq": q.check, "ndx": ndx.check, "dev": None, "periods": None},
        "complete": False,
    }
    if not q.check["ok"] or not ndx.check["ok"]:
        return result
    spans = {
        "dev": span_result(q.dev, q.dev_first_in),
        "ndx": span_result(ndx.series, 0),
        "qqq": span_result(q.full, q.first_in),
    }
    result["check"]["dev"] = sp.dev_check(spans["dev"])
    periods = {name: periods_check(name, spans[name]) for name in SPANS}
    result["check"]["periods"] = periods
    if not result["check"]["dev"]["ok"] or not all(c["ok"] for c in periods.values()):
        return result
    result["spans"] = spans
    result["complete"] = True
    return result


def _line(cells: Sequence[str]) -> str:
    return "| " + " | ".join(cells) + " |"


def render_span(name: str, block: Mapping[str, Any]) -> list[str]:
    """Render the selected strategies, annual returns, and buy-back details."""
    span = block["span"]
    lines = [
        f"## {sp.SPAN_TEXT[name]}",
        "",
        f"窗口 `{sp.SPAN_WINDOW[name]}`；满仓起点 {span['first_day']}，到 {span['last_day']}，"
        f"共 {span['sessions']} 个交易日。v0_close 入场 {len(span['entries'])} 次。",
        "",
    ]
    headers = [
        "策略",
        "总收益",
        "年化",
        "最大回撤",
        "回撤高点 → 低点",
        "最差 5% 的 20 日收益",
        "在场时间",
        "在场时年化",
        "卖出次数",
        "靠入场 / 结构转变 / 200 日线涨到卖价 / 站上 200 日线 / 没买回",
        "跌回 L 下卖出",
        "买回更便宜",
        "平均买回差价",
        "平均空仓天数",
    ]
    lines += ["### 策略", "", _line(headers), "|" + "---|" * len(headers)]
    for strategy in SHOWN:
        row = span["strategies"][strategy]
        lines.append(
            _line(
                [
                    STRATEGY_TEXT[strategy],
                    xr.pct(row["total"]),
                    xr.pct(row["cagr"]),
                    xr.pct(row["max_drawdown"]),
                    f"{row['drawdown_peak']} → {row['drawdown_trough']}",
                    xr.pct(row["worst20"]),
                    cb.share_text(row["in_market"]),
                    xr.pct(row["in_market_cagr"]),
                    str(row["sells"]),
                    f"{row['by_entry']} / {row['by_shift']} / {row['by_line']} / "
                    f"{row['by_back']} / {row['not_back']}",
                    str(row["low_stops"]),
                    str(row["cheaper"]),
                    xr.pct(row["change_mean"]),
                    cb.days_text(row["days_out_mean"]),
                ]
            )
        )
    year_headers = ["年", *(STRATEGY_TEXT[s] for s in SHOWN)]
    lines += [
        "",
        "每年（从上一年最后一个收盘算，第一年从满仓起点算）：",
        "",
        _line(year_headers),
        "|" + "---|" * len(year_headers),
    ]
    for row in span["years"]:
        lines.append(_line([str(row["year"]), *(xr.pct(row[s]) for s in SHOWN)]))
    trip_headers = [
        "策略",
        "出场响",
        "原因",
        "离 200 日线",
        "卖出",
        "买回",
        "怎么买回",
        "空仓天数",
        "买回差价",
    ]
    lines += [
        "",
        "线下才卖及两种新买回的每次卖出和买回（原因 L = 收盘跌回 L 下；"
        "没买回的差价算到最后一个收盘）：",
        "",
        _line(trip_headers),
        "|" + "---|" * len(trip_headers),
    ]
    for strategy in TRIPS_SHOWN:
        for trip in span["strategies"][strategy]["trips"]:
            lines.append(
                _line(
                    [
                        STRATEGY_TEXT[strategy],
                        trip["decision"],
                        trip["reason"],
                        xr.pct(trip["vs_line"]),
                        trip["sell"],
                        trip["buy"] if trip["buy"] is not None else "—",
                        HOW_TEXT[trip["how"]],
                        str(trip["days_out"]),
                        xr.pct(trip["change"]),
                    ]
                )
            )
    return lines


def render_markdown(result: Mapping[str, Any]) -> str:
    """Print the three spans without interpretation or verdicts."""
    lines = [
        "# 结构转变策略补「站上 200 日线就买回」（TRADING-2565-124）",
        "",
        RULE_TEXT,
        "",
        NOTE,
        "",
    ]
    for name in SPANS:
        lines += render_span(name, result["spans"][name]) + [""]
    return "\n".join(lines).rstrip() + "\n"


def summary_line(result: Mapping[str, Any], out: Path) -> str:
    """Summarize the selected totals with the two nondevelopment spans first."""
    parts = []
    for name in ("ndx", "qqq", "dev"):
        rows = result["spans"][name]["span"]["strategies"]
        shown = "、".join(f"{s} {xr.pct(rows[s]['total'])}" for s in (*TRIPS_SHOWN, "hold"))
        parts.append(f"{name} 总收益 {shown}")
    return f"structure_shift_rebuy: {'；'.join(parts)}；结果目录 {out}"


def check_line(check: Mapping[str, Any], out: Path) -> str:
    """Report both input statuses and their coverage."""
    q, n = check["check"]["qqq"], check["check"]["ndx"]
    return (
        f"structure_shift_rebuy check: ndx {'ok' if n['ok'] else 'FAIL'}，"
        f"{n['inputs']['closes']} 个收盘；"
        f"qqq {'ok' if q['ok'] else 'FAIL'}，2011-01-03..{q['last_close_date']}，"
        f"{q['sessions']} 个交易日，缺 {q['missing_days']} 天；结果目录 {out}"
    )


def write_check(check: Mapping[str, Any], out: Path) -> None:
    """Write only the finite UTF-8 input-check artifact."""
    text = json.dumps(check, indent=2, ensure_ascii=False, allow_nan=False) + "\n"
    out.mkdir(parents=True, exist_ok=True)
    out.joinpath(CHECK_JSON).write_text(text, encoding="utf-8")


def write_result(result: Mapping[str, Any], out: Path) -> None:
    """Retain failed checks as JSON, rendering tables only on completion."""
    text = json.dumps(result, indent=2, ensure_ascii=False, allow_nan=False) + "\n"
    out.mkdir(parents=True, exist_ok=True)
    out.joinpath(RESULT_JSON).write_text(text, encoding="utf-8")
    if result["complete"]:
        out.joinpath(RESULT_MD).write_text(render_markdown(result), encoding="utf-8")


@app.callback()
def main() -> None:
    """Print the authorized above-line buy-back controls."""


@app.command("check")
def check_command(
    out: Annotated[Path, typer.Option(help="输入检查目录，已有检查拒绝覆盖。")],
    root: Annotated[Path, typer.Option(help="长历史快照根目录。")] = qlh.DEFAULT_SNAPSHOT_ROOT,
) -> None:
    if out.joinpath(CHECK_JSON).exists():
        typer.echo("structure_shift_rebuy: 已有检查结果，拒绝覆盖。")
        raise typer.Exit(code=1)
    try:
        data, gate, loaded, window = sp.load_qqq(root)
        levels, source, ndx_window = en.load_data(root)
        check = check_table(data, loaded, gate, window, levels, source, ndx_window)
    except (OSError, ValueError, KeyError, tr.TrialError) as exc:
        lines = str(exc).splitlines()
        message = lines[0] if lines else ""
        typer.echo(f"structure_shift_rebuy: 数据载入失败：{message}")
        raise typer.Exit(code=1) from None
    write_check(check, out)
    typer.echo(check_line(check, out))
    if not check["ok"]:
        raise typer.Exit(code=1)


@app.command("run")
def run_command(
    out: Annotated[Path, typer.Option(help="策略结果目录，已有结果拒绝覆盖。")],
    root: Annotated[Path, typer.Option(help="长历史快照根目录。")] = qlh.DEFAULT_SNAPSHOT_ROOT,
) -> None:
    if any(out.joinpath(name).exists() for name in (RESULT_JSON, RESULT_MD)):
        typer.echo("structure_shift_rebuy: 已有结果，拒绝覆盖。")
        raise typer.Exit(code=1)
    try:
        data, gate, loaded, window = sp.load_qqq(root)
        levels, source, ndx_window = en.load_data(root)
        result = run_table(data, loaded, gate, window, levels, source, ndx_window)
    except (OSError, ValueError, KeyError, tr.TrialError) as exc:
        lines = str(exc).splitlines()
        message = lines[0] if lines else ""
        typer.echo(f"structure_shift_rebuy: 数据载入失败：{message}")
        raise typer.Exit(code=1) from None
    write_result(result, out)
    if not result["complete"]:
        stage = "输入检查" if result["check"]["dev"] is None else "TRADING-2565-122 重算核对"
        typer.echo(f"structure_shift_rebuy: {stage} FAIL，没有印新行")
        raise typer.Exit(code=1)
    typer.echo(summary_line(result, out))


if __name__ == "__main__":
    app()
