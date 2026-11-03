"""The first QQQ strategy with a next-close confirmation (TRADING-2565-119).

Under owner_decision:TRADING-2565:2026-11-03:entry_exit_confirm, Baker asked
that an exit sell only at a lower next close, and B's price recovery buy only
at a higher next close; otherwise that trigger lapses. The entry already
waits for three closes and stays unchanged. Print the TRADING-2565-118 paths
beside these paths. Use only QQQ's 1999–2010 development window, cut at
2010-12-31. Print only: no verdicts or accounting entries, no gate or ruler
changes, and no daily report or broker action.
"""

from __future__ import annotations

import json
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

RUN_ID = "TRADING-2565-119"
OWNER_DECISION = "owner_decision:TRADING-2565:2026-11-03:entry_exit_confirm"
PLAIN = cb.STRATEGIES[:-1]
CONFIRMED = tuple(f"{s}_c" for s in PLAIN)
STRATEGIES = (*CONFIRMED, *cb.STRATEGIES)
STRATEGY_TEXT = {
    **{f"{s}_c": f"{cb.STRATEGY_TEXT[s]}，第二天确认" for s in PLAIN},
    **{s: f"{cb.STRATEGY_TEXT[s]}，不确认" for s in PLAIN},
    "hold": cb.STRATEGY_TEXT["hold"],
}
SPAN_TEXT = cb.SPAN_TEXT
RESULT_JSON = "entry_exit_confirm.json"
RESULT_MD = "entry_exit_confirm.md"
NOTE = cb.NOTE + (
    "确认版出场条件在某个收盘成立，要看下一个收盘，"
    "比这个收盘低才在下一个收盘卖，不然这次作废、接着拿，之后条件再成立再看；"
    "作废不重置买进以来的最高收盘和 X3 是否已经站上 50 日线。"
    "B 版收盘回到卖出价或更高以后，也要看下一个收盘，"
    "比那个收盘高才在下一个收盘买回，不然接着等；持平都算作废。"
    "入场信号本身已经要三个收盘确认，不再加；A 版买回、B 版靠入场买回都不变，"
    "入场和确认买回同一个收盘算入场。"
    "最后一个收盘才成立的看不到下一个收盘，不执行，也不算作废。"
    "「不确认」的六行就是 TRADING-2565-118 的规则，同一份数据重算。"
)
app = typer.Typer(help="QQQ 入场加出场，出场和回补要第二天收盘确认：只印不判。")


class Step(NamedTuple):
    """An execution close, its trigger, its reason, and preceding lapsed triggers."""

    at: int | None
    trigger: int | None
    how: str
    lapsed: int


def triggers(rule: str, data: xc.Prepared, b: int) -> list[tuple[int, str]]:
    """List every exit trigger after buying, without resetting the holding state."""
    if rule not in cb.EXITS:
        raise ValueError(f"unknown exit {rule}")
    close, sma = data.close, data.sma
    high = close[b]
    first = sma[b]
    armed = first is not None and close[b] >= first
    found: list[tuple[int, str]] = []
    for d in range(b + 1, len(close)):
        high = max(high, close[d])
        current, previous = sma[d], sma[d - 1]
        if current is not None and close[d] >= current:
            armed = True
        if rule in ("x2", "x2x3") and close[d] / high <= 1.0 - cb.TRAIL:
            found.append((d, "X2"))
        elif rule in ("x3", "x2x3"):
            if (
                armed
                and current is not None
                and previous is not None
                and close[d] < current
                and close[d - 1] < previous
            ):
                found.append((d, "X3"))
            elif not armed and close[d] / close[b] <= 1.0 - cb.STOP:
                found.append((d, "stop"))
    return found


def confirmed_exit(rule: str, data: xc.Prepared, b: int) -> Step:
    """Sell at the first next close strictly below its exit-trigger close."""
    lapsed = 0
    for d, reason in triggers(rule, data, b):
        if d + 1 >= len(data.close):
            break
        if data.close[d + 1] < data.close[d]:
            return Step(d + 1, d, reason, lapsed)
        lapsed += 1
    return Step(None, None, "", lapsed)


def confirmed_buy_back(version: str, data: xc.Prepared, entries: Sequence[int], v: int) -> Step:
    """Prefer entry closes; B may also buy a recovery confirmed by a higher close."""
    if version not in cb.VERSIONS:
        raise ValueError(f"unknown version {version}")
    entry = next((e for e in entries if e > v), None)
    lapsed = 0
    if version == "b":
        close = data.close
        for r in range(v + 1, len(close) - 1):
            if close[r] < close[v]:
                continue
            if entry is not None and entry <= r + 1:
                break
            if close[r + 1] > close[r]:
                return Step(r + 1, r, "recovery", lapsed)
            lapsed += 1
    if entry is not None:
        return Step(entry, entry, "entry", lapsed)
    return Step(None, None, "none", lapsed)


def simulate(
    strategy: str, data: xc.Prepared, entries: Sequence[int], s0: int
) -> tuple[cb.Route, int | None, int | None]:
    """Build confirmed holdings or delegate an unchanged path to the first version."""
    if strategy in cb.STRATEGIES:
        return cb.simulate(strategy, data, entries, s0), None, None
    if strategy not in CONFIRMED:
        raise ValueError(f"unknown strategy {strategy}")
    rule, _, version = strategy.removesuffix("_c").partition("_")
    n = len(data.close)
    held = [False] * n
    buys, sells = [s0], []
    trips: list[dict[str, Any]] = []
    b = s0
    exit_lapsed = buy_lapsed = 0
    while b < n - 1:
        out = confirmed_exit(rule, data, b)
        exit_lapsed += out.lapsed
        if out.at is None:
            break
        v = out.at
        # Every executed exit has a preceding trigger close.
        assert out.trigger is not None
        held[b + 1 : v + 1] = [True] * (v - b)
        sells.append(v)
        back = confirmed_buy_back(version, data, entries, v)
        buy_lapsed += back.lapsed
        r = back.at
        last = r if r is not None else n - 1
        trips.append(
            {
                "decision": data.dates[out.trigger].isoformat(),
                "reason": out.how,
                "sell": data.dates[v].isoformat(),
                "buy": data.dates[r].isoformat() if r is not None else None,
                "how": back.how,
                "days_out": last - v,
                "change": data.close[last] / data.close[v] - 1.0,
                "exit_lapsed": out.lapsed,
                "buy_lapsed": back.lapsed,
            }
        )
        if r is None:
            return (
                cb.Route(tuple(held), tuple(buys), tuple(sells), tuple(trips)),
                exit_lapsed,
                buy_lapsed,
            )
        buys.append(r)
        b = r
    held[b + 1 :] = [True] * (n - 1 - b)
    return cb.Route(tuple(held), tuple(buys), tuple(sells), tuple(trips)), exit_lapsed, buy_lapsed


def span_table(
    series: ec.Closes, data: xc.Prepared, entries: Sequence[int], s0: int
) -> dict[str, Any]:
    """Describe all paths using the first version's equity and summary calculations."""
    strategies: dict[str, Any] = {}
    annual: dict[str, dict[int, float]] = {}
    for strategy in STRATEGIES:
        path, exit_lapsed, buy_lapsed = simulate(strategy, data, entries, s0)
        values = cb.equity(data.close, path, s0)
        strategies[strategy] = {
            **cb.metrics(series.dates, values, path, s0),
            "exit_lapsed": exit_lapsed,
            "buy_lapsed": buy_lapsed,
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
    """Check the same QQQ input and entry dates before calculating any path."""
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
            "strategies": list(STRATEGIES),
            "entry": ec.VARIANT,
            "start": "full",
            "trail": cb.TRAIL,
            "stop": cb.STOP,
            "sma_days": xc.SMA_DAYS,
            "sell": "next_close",
            "sell_confirmed": "next_close_if_lower_than_trigger_close",
            "buy_back_a": "entry_close",
            "buy_back_b": "entry_close_or_first_close_at_or_above_sell_close",
            "buy_back_b_confirmed": "entry_close_or_next_close_if_higher_than_recovery_close",
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
    result["span"] = span_table(series, prepared, entries, s0)
    result["complete"] = True
    return result


def _line(cells: Sequence[str]) -> str:
    return "| " + " | ".join(cells) + " |"


def lapsed_text(row: Mapping[str, Any], strategy: str) -> str:
    """Show lapse counts only where next-close confirmation applies."""
    if row["exit_lapsed"] is None:
        return "—"
    back = "—" if strategy.endswith("_a_c") else str(row["buy_lapsed"])
    return f"{row['exit_lapsed']} / {back}"


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
        "第二天作废：出场 / 回补",
    ]
    lines = [
        "# QQQ 入场加出场：出场和回补要第二天收盘确认（TRADING-2565-119）",
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
                    cb.share_text(row["in_market"]),
                    xr.pct(row["in_market_cagr"]),
                    str(row["sells"]),
                    f"{row['by_entry']} / {row['by_recovery']} / {row['not_back']}",
                    str(row["cheaper"]),
                    xr.pct(row["change_mean"]),
                    cb.days_text(row["days_out_mean"]),
                    lapsed_text(row, strategy),
                ]
            )
        )
    shown = (*CONFIRMED, "hold")
    year_headers = ["年", *(STRATEGY_TEXT[s] for s in shown)]
    lines += [
        "",
        "每年（从上一年最后一个收盘算，第一年从满仓起点算；不确认的六行每年数见 JSON）：",
        "",
        _line(year_headers),
        "|" + "---|" * len(year_headers),
    ]
    for row in span["years"]:
        lines.append(_line([str(row["year"]), *(xr.pct(row[s]) for s in shown)]))
    trip_headers = [
        "策略",
        "出场响",
        "原因",
        "卖出",
        "买回",
        "怎么买回",
        "空仓天数",
        "买回差价",
        "之前作废：出场 / 回补",
    ]
    lines += [
        "",
        "第二天确认的每次卖出和买回（没买回的差价算到最后一个收盘；不确认的见 TRADING-2565-118）：",
        "",
        _line(trip_headers),
        "|" + "---|" * len(trip_headers),
    ]
    for strategy in CONFIRMED:
        for trip in span["strategies"][strategy]["trips"]:
            lines.append(
                _line(
                    [
                        STRATEGY_TEXT[strategy],
                        trip["decision"],
                        trip["reason"],
                        trip["sell"],
                        trip["buy"] if trip["buy"] is not None else "—",
                        cb.HOW_TEXT[trip["how"]],
                        str(trip["days_out"]),
                        xr.pct(trip["change"]),
                        lapsed_text(trip, strategy),
                    ]
                )
            )
    return "\n".join(lines).rstrip() + "\n"


def summary_line(result: Mapping[str, Any], out: Path) -> str:
    span = result["span"]
    rows = span["strategies"]
    parts = "、".join(
        f"{STRATEGY_TEXT[s]} 年化 {xr.pct(rows[s]['cagr'])} 回撤 {xr.pct(rows[s]['max_drawdown'])}"
        for s in (*CONFIRMED, "hold")
    )
    return (
        f"entry_exit_confirm: {SPAN_TEXT} 满仓起步，入场 {len(span['entries'])} 次；{parts}；"
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
    """Print the next-close confirmation paths without investment conclusions."""


@app.command("run")
def run_command(
    out: Annotated[Path, typer.Option(help="策略结果目录，已有结果拒绝覆盖。")],
    root: Annotated[Path, typer.Option(help="长历史快照根目录。")] = qlh.DEFAULT_SNAPSHOT_ROOT,
) -> None:
    if any(out.joinpath(name).exists() for name in (RESULT_JSON, RESULT_MD)):
        typer.echo("entry_exit_confirm: 已有结果，拒绝覆盖。")
        raise typer.Exit(code=1)
    try:
        data, gate, loaded = xc.load_data(root)
        result = run_table(data, loaded, gate)
    except (OSError, ValueError, KeyError, tr.TrialError) as exc:
        lines = str(exc).splitlines()
        message = lines[0] if lines else ""
        typer.echo(f"entry_exit_confirm: 数据载入失败：{message}")
        raise typer.Exit(code=1) from None
    write_result(result, out)
    if not result["complete"]:
        typer.echo("entry_exit_confirm: 输入检查 FAIL，没有算策略")
        raise typer.Exit(code=1)
    typer.echo(summary_line(result, out))


if __name__ == "__main__":
    app()
