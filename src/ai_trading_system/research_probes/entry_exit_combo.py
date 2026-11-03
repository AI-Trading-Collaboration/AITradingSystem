"""A first QQQ strategy from our entry and exit signals (TRADING-2565-118).

Under owner_decision:TRADING-2565:2026-11-03:entry_exit_combo, Baker asked
for the closing-price 200-day pullback entry and the exit family together,
beside buy and hold. Start fully invested at the first evaluable entry close;
sell at the close after an exit signal. A waits for an entry; B also buys at
the first close recovering the selling close. Use only QQQ's 1999–2010
development window, cut at 2010-12-31. Print only: no verdicts or accounting
entries, no gate or ruler changes, and no daily report or broker action.
"""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import date
from pathlib import Path
from typing import Annotated, Any, NamedTuple

import typer

from ai_trading_system.research_bench import trial as tr
from ai_trading_system.research_bench.data import BenchData
from ai_trading_system.research_bench.settings import LoadedSettings
from ai_trading_system.research_data import qqq_long_history as qlh
from ai_trading_system.research_probes import entry_timing_counts as ec
from ai_trading_system.research_probes import exit_rules_after as xa
from ai_trading_system.research_probes import exit_rules_counts as xc
from ai_trading_system.research_probes import exit_rules_returns as xr

RUN_ID = "TRADING-2565-118"
OWNER_DECISION = "owner_decision:TRADING-2565:2026-11-03:entry_exit_combo"
EXITS = ("x2", "x3", "x2x3")
VERSIONS = ("a", "b")
STRATEGIES = (*(f"{x}_{v}" for x in EXITS for v in VERSIONS), "hold")
# Frozen print-only pilot under OWNER_DECISION; exit at Baker's review of this table.
# These numbers describe one development-window path, not acceptance or promotion gates.
TRAIL = xc.STOP
STOP = xc.STOP
COST = 0.0005
TAIL_DAYS = 20
TAIL = 0.05
YEAR_DAYS = 365.25
SESSIONS_PER_YEAR = 252
# Closing-price entry dates already printed by -104, used only for the input check.
V0_ENTRIES = (
    "2000-05-15",
    "2000-08-09",
    "2004-03-26",
    "2004-05-05",
    "2010-05-25",
    "2010-09-10",
)
EXIT_TEXT = {"x2": "X2 回落 8%", "x3": "X3 跌破 50 日线", "x2x3": "X2 或 X3 先响"}
VERSION_TEXT = {"a": "A 只等入场买回", "b": "B 回到卖出价也买回"}
STRATEGY_TEXT = {
    **{f"{x}_{v}": f"{EXIT_TEXT[x]}，{VERSION_TEXT[v]}" for x in EXITS for v in VERSIONS},
    "hold": "买入持有",
}
HOW_TEXT = {"entry": "入场信号", "recovery": "回到卖出价", "none": "没买回"}
SPAN_TEXT = xa.SPAN_TEXT["qqq"]
RESULT_JSON = "entry_exit_combo.json"
RESULT_MD = "entry_exit_combo.md"
NOTE = (
    "只印不判，不检验、不下结论，不是交易建议：QQQ 1999–2010 开发窗口"
    "（QQQ 实际价格，切到 2010-12-31），对入场和出场都是看过结果的历史。"
    "从第一个能判入场的收盘起满仓，买入持有也从同一个收盘起。"
    "入场用 v0_close 收盘价版计入的信号，和 10-31 冻结的原规则（用最低价碰线）不一样，"
    "挑的日子也不同。X2 从买进以来的最高收盘回落 8%；"
    "X3 连续两个收盘严格低于 50 日线，买进后先有一个收盘在 50 日线上或线上方才开始算，"
    "在那以前收盘比买入价跌 8% 就卖；X2 或 X3 谁先响谁算，同一天算 X2。"
    "出场响后下一个收盘卖，最后一个收盘才响的不卖。"
    "A 版卖出后只等入场信号，在信号收盘买回满仓；"
    "B 版另外在收盘回到卖出那天的收盘价或更高时，在那个收盘买回满仓，"
    "和入场同一天算入场。空仓按 0 收益算，不算利息；每次买卖扣 0.05%。"
    "年化按日历天数；最差 5% 的 20 日收益是所有相隔 20 个交易日的净值变化的"
    "5% 分位（取最近的名次）；在场时年化是期末净值按在场天数、每年 252 个交易日折算，"
    "包含全部买卖成本。买回差价是买回收盘 ÷ 卖出收盘 − 1，负数是买回更便宜；"
    "到窗口末还空仓的算到最后一个收盘，平均买回差价只算买回了的，平均空仓天数算全部卖出。"
)
app = typer.Typer(help="QQQ 入场加出场的第一版策略：只印不判。")


class Route(NamedTuple):
    """Holdings between closes, transaction closes, and each sale's subsequent path."""

    held: tuple[bool, ...]
    buys: tuple[int, ...]
    sells: tuple[int, ...]
    trips: tuple[dict[str, Any], ...]


def exit_day(rule: str, data: xc.Prepared, b: int) -> tuple[int, str] | None:
    """Find the first exit decision strictly after the buying close."""
    if rule not in EXITS:
        raise ValueError(f"unknown exit {rule}")
    close, sma = data.close, data.sma
    high = close[b]
    first = sma[b]
    armed = first is not None and close[b] >= first
    for d in range(b + 1, len(close)):
        high = max(high, close[d])
        current, previous = sma[d], sma[d - 1]
        if current is not None and close[d] >= current:
            armed = True
        if rule in ("x2", "x2x3") and close[d] / high <= 1.0 - TRAIL:
            return d, "X2"
        if rule in ("x3", "x2x3"):
            if (
                armed
                and current is not None
                and previous is not None
                and close[d] < current
                and close[d - 1] < previous
            ):
                return d, "X3"
            if not armed and close[d] / close[b] <= 1.0 - STOP:
                return d, "stop"
    return None


def buy_back(
    version: str, data: xc.Prepared, entries: Sequence[int], v: int
) -> tuple[int, str] | None:
    """Choose the next entry or, for B, the first recovery of the selling close."""
    if version not in VERSIONS:
        raise ValueError(f"unknown version {version}")
    entry = next((e for e in entries if e > v), None)
    recovery = (
        next((r for r in range(v + 1, len(data.close)) if data.close[r] >= data.close[v]), None)
        if version == "b"
        else None
    )
    if entry is not None and (recovery is None or entry <= recovery):
        return entry, "entry"
    if recovery is not None:
        return recovery, "recovery"
    return None


def simulate(strategy: str, data: xc.Prepared, entries: Sequence[int], s0: int) -> Route:
    """Build a close-to-close path, executing each exit one close after its decision."""
    if strategy not in STRATEGIES:
        raise ValueError(f"unknown strategy {strategy}")
    n = len(data.close)
    held = [False] * n
    buys, sells = [s0], []
    trips: list[dict[str, Any]] = []
    b = s0
    if strategy != "hold":
        rule, _, version = strategy.partition("_")
        while b < n - 1:
            fired = exit_day(rule, data, b)
            if fired is None or fired[0] + 1 > n - 1:
                break
            d, reason = fired
            v = d + 1
            held[b + 1 : v + 1] = [True] * (v - b)
            sells.append(v)
            back = buy_back(version, data, entries, v)
            r, how = back if back is not None else (None, "none")
            last = r if r is not None else n - 1
            trips.append(
                {
                    "decision": data.dates[d].isoformat(),
                    "reason": reason,
                    "sell": data.dates[v].isoformat(),
                    "buy": data.dates[r].isoformat() if r is not None else None,
                    "how": how,
                    "days_out": last - v,
                    "change": data.close[last] / data.close[v] - 1.0,
                }
            )
            if r is None:
                return Route(tuple(held), tuple(buys), tuple(sells), tuple(trips))
            buys.append(r)
            b = r
    held[b + 1 :] = [True] * (n - 1 - b)
    return Route(tuple(held), tuple(buys), tuple(sells), tuple(trips))


def equity(close: Sequence[float], path: Route, s0: int) -> list[float]:
    """Apply held returns and a multiplicative cost at every transaction close."""
    trades = Counter((*path.buys, *path.sells))
    values = [(1.0 - COST) ** trades[s0]]
    for p in range(s0 + 1, len(close)):
        change = close[p] / close[p - 1] if path.held[p] else 1.0
        values.append(values[-1] * change * (1.0 - COST) ** trades[p])
    return values


def metrics(dates: Sequence[date], values: Sequence[float], path: Route, s0: int) -> dict[str, Any]:
    """Describe one path without acceptance rules or significance tests."""
    n = len(dates)
    years = (dates[n - 1] - dates[s0]).days / YEAR_DAYS
    peak = values[0]
    peak_day = drawdown_peak = drawdown_trough = dates[s0]
    worst = 0.0
    for i, value in enumerate(values):
        if value > peak:
            peak, peak_day = value, dates[s0 + i]
        drawdown = value / peak - 1.0
        if drawdown < worst:
            worst, drawdown_peak, drawdown_trough = drawdown, peak_day, dates[s0 + i]
    tails = [values[i] / values[i - TAIL_DAYS] - 1.0 for i in range(TAIL_DAYS, len(values))]
    held_days = sum(path.held[s0 + 1 :])
    sessions = n - 1 - s0
    back = [float(t["change"]) for t in path.trips if t["how"] != "none"]
    return {
        "total": values[-1] - 1.0,
        "cagr": values[-1] ** (1.0 / years) - 1.0 if years > 0 else None,
        "max_drawdown": worst,
        "drawdown_peak": drawdown_peak.isoformat(),
        "drawdown_trough": drawdown_trough.isoformat(),
        "worst20": xr.rank(tails, TAIL) if tails else None,
        "in_market": held_days / sessions if sessions > 0 else None,
        "in_market_cagr": values[-1] ** (SESSIONS_PER_YEAR / held_days) - 1.0
        if held_days
        else None,
        "sells": len(path.sells),
        "by_entry": sum(t["how"] == "entry" for t in path.trips),
        "by_recovery": sum(t["how"] == "recovery" for t in path.trips),
        "not_back": sum(t["how"] == "none" for t in path.trips),
        "cheaper": sum(change < 0 for change in back),
        "change_mean": xa.mean(back),
        "days_out_mean": xa.mean([float(t["days_out"]) for t in path.trips]),
    }


def year_returns(dates: Sequence[date], values: Sequence[float], s0: int) -> dict[int, float]:
    """Measure each calendar year from the previous year end, initially from principal."""
    base = 1.0
    result: dict[int, float] = {}
    for p in range(s0, len(dates)):
        if p == len(dates) - 1 or dates[p + 1].year != dates[p].year:
            value = values[p - s0]
            result[dates[p].year] = value / base - 1.0
            base = value
    return result


def span_table(
    series: ec.Closes, data: xc.Prepared, entries: Sequence[int], s0: int
) -> dict[str, Any]:
    strategies: dict[str, Any] = {}
    annual: dict[str, dict[int, float]] = {}
    for strategy in STRATEGIES:
        path = simulate(strategy, data, entries, s0)
        values = equity(data.close, path, s0)
        strategies[strategy] = {
            **metrics(series.dates, values, path, s0),
            "trips": list(path.trips),
        }
        annual[strategy] = year_returns(series.dates, values, s0)
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
        "expected": list(V0_ENTRIES),
        "ok": entry_dates == V0_ENTRIES and s0 + TAIL_DAYS < n,
    }
    result: dict[str, Any] = {
        "run_id": RUN_ID,
        "owner_decision": OWNER_DECISION,
        "source": {"snapshot_id": data.source.get("snapshot_id")},
        "rule": {
            "strategies": list(STRATEGIES),
            "entry": ec.VARIANT,
            "start": "full",
            "trail": TRAIL,
            "stop": STOP,
            "sma_days": xc.SMA_DAYS,
            "sell": "next_close",
            "buy_back_a": "entry_close",
            "buy_back_b": "entry_close_or_first_close_at_or_above_sell_close",
            "flat_return": 0.0,
            "cost": COST,
            "tail_days": TAIL_DAYS,
            "tail": TAIL,
            "sessions_per_year": SESSIONS_PER_YEAR,
        },
        "check": {"qqq": check, "entries": entry_check},
        "complete": False,
    }
    if not check["ok"] or not entry_check["ok"]:
        return result
    result["span"] = span_table(series, prepared, entries, s0)
    result["complete"] = True
    return result


def share_text(value: float | None) -> str:
    return "—" if value is None else f"{100 * value:.0f}%"


def days_text(value: float | None) -> str:
    return "—" if value is None else f"{value:.0f}"


def _line(cells: Sequence[str]) -> str:
    return "| " + " | ".join(cells) + " |"


def render_markdown(result: Mapping[str, Any]) -> str:
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
        "靠入场 / 回到卖出价 / 没买回",
        "买回更便宜",
        "平均买回差价",
        "平均空仓天数",
    ]
    lines = [
        "# QQQ 入场加出场：第一版策略（TRADING-2565-118）",
        "",
        NOTE,
        "",
        f"## {SPAN_TEXT}",
        "",
        f"满仓起点 {span['first_day']}，到 {span['last_day']}，共 {span['sessions']} 个交易日。"
        f"入场信号 {len(span['entries'])} 次：{'、'.join(span['entries'])}。",
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
                    share_text(row["in_market"]),
                    xr.pct(row["in_market_cagr"]),
                    str(row["sells"]),
                    f"{row['by_entry']} / {row['by_recovery']} / {row['not_back']}",
                    str(row["cheaper"]),
                    xr.pct(row["change_mean"]),
                    days_text(row["days_out_mean"]),
                ]
            )
        )
    year_headers = ["年", *(STRATEGY_TEXT[s] for s in STRATEGIES)]
    lines += [
        "",
        "每年（从上一年最后一个收盘算，第一年从满仓起点算）：",
        "",
        _line(year_headers),
        "|" + "---|" * len(year_headers),
    ]
    for row in span["years"]:
        lines.append(_line([str(row["year"]), *(xr.pct(row[s]) for s in STRATEGIES)]))
    trip_headers = ["策略", "出场响", "原因", "卖出", "买回", "怎么买回", "空仓天数", "买回差价"]
    lines += [
        "",
        "每次卖出和买回（没买回的差价算到最后一个收盘）：",
        "",
        _line(trip_headers),
        "|" + "---|" * len(trip_headers),
    ]
    for strategy in STRATEGIES:
        if strategy == "hold":
            continue
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
    span = result["span"]
    rows = span["strategies"]
    parts = "、".join(
        f"{STRATEGY_TEXT[s]} 年化 {xr.pct(rows[s]['cagr'])} 回撤 {xr.pct(rows[s]['max_drawdown'])}"
        for s in STRATEGIES
    )
    return (
        f"entry_exit_combo: {SPAN_TEXT} 满仓起步，入场 {len(span['entries'])} 次；{parts}；"
        f"结果目录 {out}"
    )


def write_result(result: Mapping[str, Any], out: Path) -> None:
    text = json.dumps(result, indent=2, ensure_ascii=False, allow_nan=False) + "\n"
    out.mkdir(parents=True, exist_ok=True)
    out.joinpath(RESULT_JSON).write_text(text, encoding="utf-8")
    if result["complete"]:
        out.joinpath(RESULT_MD).write_text(render_markdown(result), encoding="utf-8")


@app.callback()
def main() -> None:
    """Print the first entry-and-exit strategy without investment conclusions."""


@app.command("run")
def run_command(
    out: Annotated[Path, typer.Option(help="策略结果目录，已有结果拒绝覆盖。")],
    root: Annotated[Path, typer.Option(help="长历史快照根目录。")] = qlh.DEFAULT_SNAPSHOT_ROOT,
) -> None:
    if any(out.joinpath(name).exists() for name in (RESULT_JSON, RESULT_MD)):
        typer.echo("entry_exit_combo: 已有结果，拒绝覆盖。")
        raise typer.Exit(code=1)
    try:
        data, gate, loaded = xc.load_data(root)
        result = run_table(data, loaded, gate)
    except (OSError, ValueError, KeyError, tr.TrialError) as exc:
        lines = str(exc).splitlines()
        message = lines[0] if lines else ""
        typer.echo(f"entry_exit_combo: 数据载入失败：{message}")
        raise typer.Exit(code=1) from None
    write_result(result, out)
    if not result["complete"]:
        typer.echo("entry_exit_combo: 输入检查 FAIL，没有算策略")
        raise typer.Exit(code=1)
    typer.echo(summary_line(result, out))


if __name__ == "__main__":
    app()
