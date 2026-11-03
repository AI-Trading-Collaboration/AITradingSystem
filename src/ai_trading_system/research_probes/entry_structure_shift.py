"""A structure-shift entry after QQQ breaks below its 200-day line (TRADING-2565-120).

Under owner_decision:TRADING-2565:2026-11-03:entry_structure_shift, Baker
asked for an entry after a break that keeps falling: wait for a lower swing
low L below the line, then buy the first close above the latest swing high H
before L. A close below L triggers a sale at the next close. Strict swings
need k closes on each side and become known only k closes later. Baker chose
k = 5; print k = 2 beside it. Print 20/60-close returns and any-day controls,
then add this buy-back to TRADING-2565-118 version A. Use only QQQ's 1999–2010
development window, cut at 2010-12-31. Print only: no verdicts or accounting
entries, no gate or ruler changes, and no daily report or broker action.
"""

from __future__ import annotations

import json
import math
from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import date
from pathlib import Path
from typing import Annotated, Any, NamedTuple

import typer

from ai_trading_system.research_bench import trial as tr
from ai_trading_system.research_bench.data import BenchData
from ai_trading_system.research_bench.settings import LoadedSettings
from ai_trading_system.research_data import qqq_long_history as qlh
from ai_trading_system.research_probes import entry_exit_combo as cb
from ai_trading_system.research_probes import entry_timing_counts as ec
from ai_trading_system.research_probes import exit_rules_after as xa
from ai_trading_system.research_probes import exit_rules_counts as xc
from ai_trading_system.research_probes import exit_rules_returns as xr

RUN_ID = "TRADING-2565-120"
OWNER_DECISION = "owner_decision:TRADING-2565:2026-11-03:entry_structure_shift"
# Frozen print-only pilot under OWNER_DECISION; exit at Baker's review of this table.
# These settings describe development-window paths, not acceptance or promotion gates.
SWINGS = (5, 2)
HORIZONS = (20, 60)
PLAIN = tuple(f"{x}_a" for x in cb.EXITS)
SHIFTED = tuple(f"{x}_s{k}" for k in SWINGS for x in cb.EXITS)
STRATEGIES = (*SHIFTED, *PLAIN, "hold")
STRATEGY_TEXT = {
    **{
        f"{x}_s{k}": f"{cb.EXIT_TEXT[x]}，A 加结构转变买回（左右各 {k} 天）"
        for k in SWINGS
        for x in cb.EXITS
    },
    **{s: cb.STRATEGY_TEXT[s] for s in PLAIN},
    "hold": cb.STRATEGY_TEXT["hold"],
}
HOW_TEXT = {**cb.HOW_TEXT, "shift": "结构转变"}
SPAN_TEXT = cb.SPAN_TEXT
RESULT_JSON = "entry_structure_shift.json"
RESULT_MD = "entry_structure_shift.md"
RULE_TEXT = (
    "只印不判，不检验、不下结论，不是交易建议：QQQ 1999–2010 开发窗口"
    "（切到 2010-12-31）已看过结果。前一个收盘在 200 日线上或线上，这个收盘在线下"
    "算跌破，每次新的跌破重新开始找。摆动低点（高点）是严格低于（高于）它左右各 k 个收盘"
    "的收盘，要到 k 个收盘以后才知道，只用当时已知的收盘。"
    "跌破后，在 200 日线下、比当前的低点 L 更低的摆动低点就成为新的 L，"
    "H 是 L 之前最近的一个摆动高点；第一个收盘高于 H 的收盘，就在这个收盘入场。"
    "入场后清空 L 和 H，再入场要有比上次 L 更低的摆动低点；新的跌破则重来。"
    "收盘低于 L 的收盘，下一个收盘卖。风险 = 入场收盘 ÷ L 收盘 − 1。"
    "h 天收益 = 第 h 个收盘 ÷ 入场收盘 − 1；带止损收益看入场后"
    "第 1 到 h − 1 个收盘里第一次低于 L，按下一个收盘算，没低于就用第 h 个收盘。"
    "当年任一天平均是从满仓起点起、和入场同一年的每个收盘之后 h 个收盘的收益平均；"
    "整段任一天平均和上涨占比用满仓起点到窗口末所有算得出 h 天收益的收盘。"
    "后面没有 h 个收盘的入场不算，任一天对照也不算。"
    "k = 5 是 Baker 选的，k = 2 只作对照。"
)
NOTE = cb.NOTE + (
    "这张表只有 A 版：卖出后在 v0_close 入场和结构转变入场里先到的那个收盘买回满仓，"
    "同一个收盘算 v0_close 入场。靠结构转变买回的那段另加止损："
    "收盘低于那次的 L，下一个收盘卖，和出场同一个收盘响算出场。"
)
app = typer.Typer(help="QQQ 跌破 200 日线后的结构转变入场：只印不判。")


class Shift(NamedTuple):
    """The break, swing low, preceding swing high, and entry close positions."""

    cross: int
    low: int
    high: int
    entry: int


class Outcome(NamedTuple):
    """Fixed-horizon and next-close-stop returns, with the optional sale offset."""

    plain: float
    stop: float
    stop_day: int | None


def is_low(close: Sequence[float], s: int, k: int) -> bool:
    """Require a strictly lower close than each of the k neighbors on either side."""
    if s - k < 0 or s + k >= len(close):
        return False
    return all(close[s] < close[p] for p in range(s - k, s + k + 1) if p != s)


def is_high(close: Sequence[float], s: int, k: int) -> bool:
    """Require a strictly higher close than each of the k neighbors on either side."""
    if s - k < 0 or s + k >= len(close):
        return False
    return all(close[s] > close[p] for p in range(s - k, s + k + 1) if p != s)


def last_high(close: Sequence[float], before: int, k: int) -> int | None:
    """Find the latest strict swing high before the low being confirmed."""
    return next((s for s in range(before - 1, k - 1, -1) if is_high(close, s, k)), None)


def shifts(close: Sequence[float], line: Sequence[float | None], k: int, start: int) -> list[Shift]:
    """Walk closes in order, using swings only once their right side is known."""
    if k < 1:
        raise ValueError(f"k must be at least 1, got {k}")
    found: list[Shift] = []
    cross: int | None = None
    low: int | None = None
    high: int | None = None
    bar = math.inf
    for p in range(max(start, 1), len(close)):
        previous, current = line[p - 1], line[p]
        if (
            previous is not None
            and current is not None
            and close[p - 1] >= previous
            and close[p] < current
        ):
            cross, low, high, bar = p, None, None, math.inf
        if cross is None:
            continue
        s = p - k
        if s >= cross:
            here = line[s]
            if here is not None and close[s] < here and close[s] < bar and is_low(close, s, k):
                low = s
                high = last_high(close, s, k)
                bar = close[s]
        if low is not None and high is not None and close[p] > close[high]:
            found.append(Shift(cross, low, high, p))
            low = high = None
    return found


def outcome(close: Sequence[float], move: Shift, horizon: int) -> Outcome | None:
    """Measure a settled horizon, optionally selling one close after falling below L."""
    e = move.entry
    if e + horizon >= len(close):
        return None
    plain = close[e + horizon] / close[e] - 1.0
    for p in range(e + 1, e + horizon):
        if close[p] < close[move.low]:
            return Outcome(plain, close[p + 1] / close[e] - 1.0, p + 1 - e)
    return Outcome(plain, plain, None)


def any_day(
    dates: Sequence[date], close: Sequence[float], s0: int, horizon: int
) -> list[tuple[date, float]]:
    """List every evaluable start close and its fixed-horizon return."""
    return [
        (dates[d], close[d + horizon] / close[d] - 1.0) for d in range(s0, len(close) - horizon)
    ]


def signal_rows(
    dates: Sequence[date],
    close: Sequence[float],
    line: Sequence[float | None],
    moves: Sequence[Shift],
    s0: int,
) -> list[dict[str, Any]]:
    """Describe each entry and compare settled outcomes with its calendar-year control."""
    year_means: dict[int, dict[int, float]] = {}
    for h in HORIZONS:
        years: dict[int, list[float]] = defaultdict(list)
        for day, value in any_day(dates, close, s0, h):
            years[day.year].append(value)
        year_means[h] = {y: math.fsum(values) / len(values) for y, values in years.items()}
    rows: list[dict[str, Any]] = []
    for move in moves:
        low_line, entry_line = line[move.low], line[move.entry]
        if low_line is None or entry_line is None:
            raise ValueError("结构转变入场缺 200 日线。")
        row: dict[str, Any] = {
            "cross": dates[move.cross].isoformat(),
            "low": dates[move.low].isoformat(),
            "low_below_line": close[move.low] / low_line - 1.0,
            "high": dates[move.high].isoformat(),
            "entry": dates[move.entry].isoformat(),
            "entry_vs_line": close[move.entry] / entry_line - 1.0,
            "risk": close[move.entry] / close[move.low] - 1.0,
        }
        for h in HORIZONS:
            cell = outcome(close, move, h)
            row[f"h{h}"] = (
                {
                    "plain": cell.plain,
                    "stop": cell.stop,
                    "stop_day": cell.stop_day,
                    "year_mean": year_means[h][dates[move.entry].year],
                }
                if cell is not None
                else None
            )
        rows.append(row)
    return rows


def horizon_summary(
    rows: Sequence[Mapping[str, Any]], horizon: int, every: Sequence[tuple[date, float]]
) -> dict[str, Any]:
    """Summarize settled entries and all evaluable closes without statistical tests."""
    cells = [row[f"h{horizon}"] for row in rows if row[f"h{horizon}"] is not None]
    return {
        "signals": len(rows),
        "settled": len(cells),
        "plain_mean": xa.mean([c["plain"] for c in cells]),
        "stop_mean": xa.mean([c["stop"] for c in cells]),
        "year_mean": xa.mean([c["year_mean"] for c in cells]),
        "plain_up": sum(c["plain"] > 0 for c in cells),
        "stop_up": sum(c["stop"] > 0 for c in cells),
        "plain_beats_year": sum(c["plain"] > c["year_mean"] for c in cells),
        "stop_beats_year": sum(c["stop"] > c["year_mean"] for c in cells),
        "stopped": sum(c["stop_day"] is not None for c in cells),
        "any_day_n": len(every),
        "any_day_mean": xa.mean([value for _, value in every]),
        "any_day_up_share": sum(value > 0 for _, value in every) / len(every) if every else None,
    }


def simulate(
    strategy: str,
    data: xc.Prepared,
    entries: Sequence[int],
    moves: Sequence[Shift],
    s0: int,
) -> cb.Route:
    """Add shift buy-backs and their low stop to A, delegating unchanged paths."""
    if strategy in (*PLAIN, "hold"):
        return cb.simulate(strategy, data, entries, s0)
    if strategy not in SHIFTED:
        raise ValueError(f"unknown strategy {strategy}")
    rule = strategy.rpartition("_s")[0]
    close = data.close
    n = len(close)
    held = [False] * n
    buys, sells = [s0], []
    trips: list[dict[str, Any]] = []
    b = s0
    floor: float | None = None
    while b < n - 1:
        fired = cb.exit_day(rule, data, b)
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
        r: int | None = None
        if entry is not None and (move is None or entry <= move.entry):
            r, how, floor = entry, "entry", None
        elif move is not None:
            r, how, floor = move.entry, "shift", close[move.low]
        else:
            how = "none"
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


def span_table(
    series: ec.Closes,
    data: xc.Prepared,
    entries: Sequence[int],
    moves: Mapping[int, Sequence[Shift]],
    s0: int,
) -> dict[str, Any]:
    """Describe all strategies with the original costs, equity, and summary measures."""
    strategies: dict[str, Any] = {}
    annual: dict[str, dict[int, float]] = {}
    for strategy in STRATEGIES:
        k = int(strategy.rpartition("_s")[2]) if strategy in SHIFTED else SWINGS[0]
        path = simulate(strategy, data, entries, moves[k], s0)
        values = cb.equity(data.close, path, s0)
        strategies[strategy] = {
            **cb.metrics(series.dates, values, path, s0),
            "by_shift": sum(t["how"] == "shift" for t in path.trips),
            "low_stops": sum(t["reason"] == "L" for t in path.trips),
            "trips": list(path.trips),
        }
        annual[strategy] = cb.year_returns(series.dates, values, s0)
    return {
        "first_day": series.dates[s0].isoformat(),
        "last_day": series.dates[-1].isoformat(),
        "sessions": len(series.dates) - 1 - s0,
        "entries": [series.dates[e].isoformat() for e in entries if e >= s0],
        "strategies": strategies,
        "years": [
            {"year": year, **{s: annual[s][year] for s in STRATEGIES}} for year in annual["hold"]
        ],
    }


def run_table(data: BenchData, loaded: LoadedSettings, gate: tr.TrialGate) -> dict[str, Any]:
    """Check the original QQQ input and entry dates before calculating signals or paths."""
    series, prepared, _, _, check = xa.qqq_inputs(data, loaded, gate)
    dates = series.dates
    n = len(dates)
    start = date.fromisoformat(check["start"])
    first_in = next((p for p, day in enumerate(dates) if day >= start), n)
    entries = [
        t.entry for t in ec.scan(series.close, first_in) if t.counted and t.entry is not None
    ]
    s0 = max(first_in, ec.SMA_DAYS)
    entry_dates = tuple(dates[e].isoformat() for e in entries)
    entry_check = {
        "variant": ec.VARIANT,
        "first_scan_date": dates[s0].isoformat() if s0 < n else None,
        "entries": list(entry_dates),
        "expected": list(cb.V0_ENTRIES),
        "ok": entry_dates == cb.V0_ENTRIES and s0 + cb.TAIL_DAYS < n,
    }
    result: dict[str, Any] = {
        "run_id": RUN_ID,
        "owner_decision": OWNER_DECISION,
        "source": {"snapshot_id": data.source.get("snapshot_id")},
        "rule": {
            "swings": list(SWINGS),
            "swing": "close_strictly_beyond_k_closes_each_side_known_k_closes_later",
            "break": "close_below_sma200_after_close_at_or_above",
            "low": "swing_low_below_sma200_since_break_lower_than_current_low_and_last_entry_low",
            "high": "latest_swing_high_before_low",
            "shift_entry": "first_close_above_high_close",
            "shift_stop": "close_below_low_close_sell_next_close",
            "horizons": list(HORIZONS),
            "long_sma_days": ec.SMA_DAYS,
            "strategies": list(STRATEGIES),
            "entry": ec.VARIANT,
            "start": "full",
            "trail": cb.TRAIL,
            "stop": cb.STOP,
            "sma_days": xc.SMA_DAYS,
            "sell": "next_close",
            "buy_back_a": "entry_close",
            "buy_back_shift": "first_of_entry_close_or_shift_entry_close_tie_is_entry",
            "flat_return": 0.0,
            "cost": cb.COST,
            "tail_days": cb.TAIL_DAYS,
            "tail": cb.TAIL,
            "sessions_per_year": cb.SESSIONS_PER_YEAR,
        },
        "check": {"qqq": check, "entries": entry_check},
        "complete": False,
    }
    if not check["ok"] or not entry_check["ok"]:
        return result
    line = ec.moving_average(series.close)
    moves = {k: shifts(series.close, line, k, s0) for k in SWINGS}
    signals: dict[str, Any] = {}
    for k in SWINGS:
        rows = signal_rows(dates, series.close, line, moves[k], s0)
        signals[f"k{k}"] = {
            "rows": rows,
            "summary": {
                f"h{h}": horizon_summary(rows, h, any_day(dates, series.close, s0, h))
                for h in HORIZONS
            },
        }
    result["signals"] = signals
    result["span"] = span_table(series, prepared, entries, moves, s0)
    result["complete"] = True
    return result


def _line(cells: Sequence[str]) -> str:
    return "| " + " | ".join(cells) + " |"


def stop_text(cell: Mapping[str, Any] | None) -> str:
    """Show the stopped return and, when applicable, its sale offset."""
    if cell is None:
        return "—"
    value = xr.pct(cell["stop"])
    return value if cell["stop_day"] is None else f"{value}（第 {cell['stop_day']} 天卖）"


def render_markdown(result: Mapping[str, Any]) -> str:
    """Print signal outcomes and the A-only strategy comparison in Chinese."""
    lines = ["# QQQ 跌破 200 日线后的结构转变入场（TRADING-2565-120）", "", RULE_TEXT, ""]
    signal_headers = [
        "跌破",
        "低点 L",
        "L 在 200 日线下",
        "摆动高点 H",
        "入场",
        "入场离 200 日线",
        "风险（离 L）",
        "20 天",
        "20 天带止损",
        "当年任一天 20 天",
        "60 天",
        "60 天带止损",
        "当年任一天 60 天",
    ]
    summary_headers = [
        "天数",
        "入场",
        "算得出",
        "平均",
        "带止损平均",
        "当年任一天平均",
        "涨",
        "带止损涨",
        "高于当年任一天",
        "带止损高于当年任一天",
        "止损",
        "整段任一天平均",
        "整段任一天上涨占比",
    ]
    for k in SWINGS:
        block = result["signals"][f"k{k}"]
        lines += [
            f"## 左右各 {k} 天：入场 {len(block['rows'])} 次",
            "",
            _line(signal_headers),
            "|" + "---|" * len(signal_headers),
        ]
        for row in block["rows"]:
            cells = [
                row["cross"],
                row["low"],
                xr.pct(row["low_below_line"]),
                row["high"],
                row["entry"],
                xr.pct(row["entry_vs_line"]),
                xr.pct(row["risk"]),
            ]
            for h in HORIZONS:
                cell = row[f"h{h}"]
                cells += (
                    [xr.pct(cell["plain"]), stop_text(cell), xr.pct(cell["year_mean"])]
                    if cell is not None
                    else ["—", "—", "—"]
                )
            lines.append(_line(cells))
        lines += ["", _line(summary_headers), "|" + "---|" * len(summary_headers)]
        for h in HORIZONS:
            summary = block["summary"][f"h{h}"]
            lines.append(
                _line(
                    [
                        f"{h} 天",
                        str(summary["signals"]),
                        str(summary["settled"]),
                        xr.pct(summary["plain_mean"]),
                        xr.pct(summary["stop_mean"]),
                        xr.pct(summary["year_mean"]),
                        str(summary["plain_up"]),
                        str(summary["stop_up"]),
                        str(summary["plain_beats_year"]),
                        str(summary["stop_beats_year"]),
                        str(summary["stopped"]),
                        xr.pct(summary["any_day_mean"]),
                        cb.share_text(summary["any_day_up_share"]),
                    ]
                )
            )
        lines.append("")
    span = result["span"]
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
        "靠入场 / 结构转变 / 没买回",
        "跌回 L 下卖出",
        "买回更便宜",
        "平均买回差价",
        "平均空仓天数",
    ]
    lines += [
        f"## 策略：{SPAN_TEXT}",
        "",
        NOTE,
        "",
        f"满仓起点 {span['first_day']}，到 {span['last_day']}，共 {span['sessions']} 个交易日。"
        f"v0_close 入场 {len(span['entries'])} 次：{'、'.join(span['entries'])}。",
        "",
        _line(headers),
        "|" + "---|" * len(headers),
    ]
    for strategy in STRATEGIES:
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
                    f"{row['by_entry']} / {row['by_shift']} / {row['not_back']}",
                    str(row["low_stops"]),
                    str(row["cheaper"]),
                    xr.pct(row["change_mean"]),
                    cb.days_text(row["days_out_mean"]),
                ]
            )
        )
    shown = (*(f"{x}_s5" for x in cb.EXITS), *PLAIN, "hold")
    year_headers = ["年", *(STRATEGY_TEXT[s] for s in shown)]
    lines += [
        "",
        "每年（从上一年最后一个收盘算，第一年从满仓起点算；左右各 2 天的每年数见 JSON）：",
        "",
        _line(year_headers),
        "|" + "---|" * len(year_headers),
    ]
    for row in span["years"]:
        lines.append(_line([str(row["year"]), *(xr.pct(row[s]) for s in shown)]))
    trip_headers = ["策略", "出场响", "原因", "卖出", "买回", "怎么买回", "空仓天数", "买回差价"]
    lines += [
        "",
        "加结构转变买回的每次卖出和买回（原因 L = 收盘跌回 L 下；"
        "没买回的差价算到最后一个收盘；只等入场的见 TRADING-2565-118）：",
        "",
        _line(trip_headers),
        "|" + "---|" * len(trip_headers),
    ]
    for strategy in SHIFTED:
        for trip in span["strategies"][strategy]["trips"]:
            lines.append(
                _line(
                    [
                        STRATEGY_TEXT[strategy],
                        trip["decision"],
                        trip["reason"],
                        trip["sell"],
                        trip["buy"] if trip["buy"] is not None else "—",
                        HOW_TEXT[trip["how"]],
                        str(trip["days_out"]),
                        xr.pct(trip["change"]),
                    ]
                )
            )
    return "\n".join(lines).rstrip() + "\n"


def summary_line(result: Mapping[str, Any], out: Path) -> str:
    rows = result["span"]["strategies"]
    counts = "、".join(
        f"左右各 {k} 天 {len(result['signals'][f'k{k}']['rows'])} 次" for k in SWINGS
    )
    parts = "、".join(
        f"{STRATEGY_TEXT[s]} 年化 {xr.pct(rows[s]['cagr'])} 回撤 {xr.pct(rows[s]['max_drawdown'])}"
        for s in (*(f"{x}_s5" for x in cb.EXITS), "hold")
    )
    return f"entry_structure_shift: {SPAN_TEXT} 结构转变入场 {counts}；{parts}；结果目录 {out}"


def write_result(result: Mapping[str, Any], out: Path) -> None:
    text = json.dumps(result, indent=2, ensure_ascii=False, allow_nan=False) + "\n"
    out.mkdir(parents=True, exist_ok=True)
    out.joinpath(RESULT_JSON).write_text(text, encoding="utf-8")
    if result["complete"]:
        out.joinpath(RESULT_MD).write_text(render_markdown(result), encoding="utf-8")


@app.callback()
def main() -> None:
    """Print the structure-shift entries without investment conclusions."""


@app.command("run")
def run_command(
    out: Annotated[Path, typer.Option(help="结构转变入场结果目录，已有结果拒绝覆盖。")],
    root: Annotated[Path, typer.Option(help="长历史快照根目录。")] = qlh.DEFAULT_SNAPSHOT_ROOT,
) -> None:
    if any(out.joinpath(name).exists() for name in (RESULT_JSON, RESULT_MD)):
        typer.echo("entry_structure_shift: 已有结果，拒绝覆盖。")
        raise typer.Exit(code=1)
    try:
        data, gate, loaded = xc.load_data(root)
        result = run_table(data, loaded, gate)
    except (OSError, ValueError, KeyError, tr.TrialError) as exc:
        lines = str(exc).splitlines()
        message = lines[0] if lines else ""
        typer.echo(f"entry_structure_shift: 数据载入失败：{message}")
        raise typer.Exit(code=1) from None
    write_result(result, out)
    if not result["complete"]:
        typer.echo("entry_structure_shift: 输入检查 FAIL，没有算入场")
        raise typer.Exit(code=1)
    typer.echo(summary_line(result, out))


if __name__ == "__main__":
    app()
