"""Price list of the seven QQQ exit rules against same-length holds (TRADING-2565-109).

Owner decision owner_decision:TRADING-2565:2026-11-02:exit_family_price_list:
step zero missed the first line, and Baker chose a print-only price list. Use
QQQ's 1999–2010 development window, cut at 2010-12-31, with every close an
entry. Put each rule's trades beside holds of lengths frozen at step zero.
Print yearly differences and left tails, without tests or conclusions. Load
only the QQQ ruler. This is neither a probe nor an experiment, makes no
accounting entries, and touches no daily report or broker.
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
from ai_trading_system.research_probes import entry_timing_counts as ec
from ai_trading_system.research_probes import exit_rules_counts as xc
from ai_trading_system.research_probes import policy_table as pt

RUN_ID = "TRADING-2565-109"
OWNER_DECISION = "owner_decision:TRADING-2565:2026-11-02:exit_family_price_list"
MARKET = xc.MARKET
RULES = xc.RULES
# Frozen print-only pilot under OWNER_DECISION; exit at Baker's price-list review.
# These replicate step zero and describe trades, not acceptance or promotion.
H_MATCH = {"X0": 44, "X1": 69, "X2": 42, "X3": 18, "X4": 89, "X5": 51, "X6": 42}
STEP0_TRADES = 2602
STEP0_FIRST = "2000-03-06"
STEP0_LAST = "2010-07-12"
STEP0_K = 4.421121529829901
R60 = 60
TAIL = 0.05
LOSS_LINES = (0.08, 0.15)
AFTER_DAYS = 20
YEAR_DAYS = 252
GATE_TAIL = 0.10
GATE_COST = 0.01
RESULT_JSON = "exit_rules_returns.json"
RESULT_MD = "exit_rules_returns.md"
NOTE = (
    "只印不判，不检验、不下结论：QQQ 1999–2010 开发窗口切到 2010-12-31，"
    "每天收盘都入场，和拿同样天数 H 比，H 由第 0 步写死。"
    "窗口只有两段大熊，第一道线没过；收益是按笔算术，"
    "per_trade_gate_size_like 只是按笔的门尺寸标签，门是组合几何的，不是过门。"
    "每年差是平均每笔差 × 252 ÷ H，背后每天入场的仓位不是 1 倍。"
)
app = typer.Typer(help="QQQ 七条出场规则价目表：只印收益对比，不判，不进账本。")


class Trade(NamedTuple):
    entry: int
    sell: int
    value: float
    deepest: float


def trade(close: Sequence[float], entry: int, sell: int) -> Trade:
    base = close[entry]
    return Trade(entry, sell, close[sell] / base - 1.0, min(close[entry : sell + 1]) / base - 1.0)


def tail_mean(values: Sequence[float], share: float = TAIL) -> float:
    kept = sorted(values)[: max(math.ceil(share * len(values)), 1)]
    return math.fsum(kept) / len(kept)


def rank(values: Sequence[float], share: float) -> float:
    return sorted(values)[max(math.ceil(share * len(values)) - 1, 0)]


def side(trades: Sequence[Trade]) -> dict[str, int | float]:
    values = [t.value for t in trades]
    deepest = [t.deepest for t in trades]
    n = len(trades)
    return {
        "trades": n,
        "mean": math.fsum(values) / n,
        "median": rank(values, 0.5),
        "worst": min(values),
        "p05": rank(values, TAIL),
        "cvar05": tail_mean(values),
        "loss08": sum(v <= -LOSS_LINES[0] for v in values),
        "loss15": sum(v <= -LOSS_LINES[1] for v in values),
        "deepest_mean": math.fsum(deepest) / n,
        "deepest_p05": rank(deepest, TAIL),
    }


def gain(rule: float, control: float) -> float | None:
    return (rule - control) / abs(control) if control != 0 else None


def compare(
    rule_side: Mapping[str, float], control_side: Mapping[str, float], hold: int
) -> dict[str, float | bool | None]:
    difference = rule_side["mean"] - control_side["mean"]
    per_year = difference * YEAR_DAYS / hold
    tail_gain = gain(rule_side["cvar05"], control_side["cvar05"])
    return {
        "mean_difference": difference,
        "per_year": per_year,
        "cvar05_gain": tail_gain,
        "p05_gain": gain(rule_side["p05"], control_side["p05"]),
        "per_trade_gate_size_like": (
            tail_gain is not None and tail_gain >= GATE_TAIL and per_year >= -GATE_COST
        ),
    }


def after_up(close: Sequence[float], days: Sequence[int]) -> dict[str, int | float | None]:
    kept = [d for d in days if d + AFTER_DAYS < len(close)]
    n = len(kept)
    up = sum(close[d + AFTER_DAYS] > close[d] for d in kept)
    return {"n": n, "up": up, "share": up / n if n else None}


def exposure(trades: Sequence[Trade], hold: int, start: int, end: int) -> dict[str, int | float]:
    # Changes apply on entry + 1 and sell + 1: entry < day <= sell.
    changes: dict[int, int] = defaultdict(int)
    for one in trades:
        changes[one.entry + 1] += 1
        changes[one.sell + 1] -= 1
    opened = sum(t.entry < start <= t.sell for t in trades)
    ratios = [opened / hold]
    for day in range(start + 1, end + 1):
        opened += changes.get(day, 0)
        ratios.append(opened / hold)
    return {
        "days": len(ratios),
        "mean": math.fsum(ratios) / len(ratios),
        "p05": rank(ratios, TAIL),
        "p95": rank(ratios, 1.0 - TAIL),
        "max": max(ratios),
    }


def by_year(
    dates: Sequence[date], rule_trades: Sequence[Trade], control_trades: Sequence[Trade]
) -> list[dict[str, int | float]]:
    years: dict[int, list[float]] = defaultdict(list)
    for rule, control in zip(rule_trades, control_trades, strict=True):
        years[dates[rule.entry].year].append(rule.value - control.value)
    return [
        {
            "year": year,
            "trades": len(values),
            "mean_difference": math.fsum(values) / len(values),
            "worst_difference": min(values),
        }
        for year, values in sorted(years.items())
    ]


def check_step0(
    series: ec.Closes,
    entries: Sequence[int],
    k: float | None,
    exits: Mapping[str, Mapping[int, xc.Exit]],
) -> dict[str, Any]:
    first = series.dates[entries[0]].isoformat() if entries else None
    last = series.dates[entries[-1]].isoformat() if entries else None
    holds = {rule: xc.rule_counts(exits[rule])["h_match"] for rule in RULES}
    return {
        "missing_days": series.missing,
        "trades": len(entries),
        "first": first,
        "last": last,
        "k": k,
        "h_match": holds,
        "ok": (
            series.missing == 0
            and len(entries) == STEP0_TRADES
            and first == STEP0_FIRST
            and last == STEP0_LAST
            and k is not None
            and math.isclose(k, STEP0_K, rel_tol=1e-12, abs_tol=0.0)
            and holds == H_MATCH
        ),
    }


def run_table(data: BenchData, loaded: LoadedSettings, gate: tr.TrialGate) -> dict[str, Any]:
    window = pt.market_window(loaded, gate, MARKET)
    if window.end is None:
        raise ValueError("开发窗口必须有结束日期。")
    series = ec.closes(data.returns["qqq"], window.end)
    close = series.close
    first_in = next(
        (p for p, day in enumerate(series.dates) if day >= window.start), len(series.dates)
    )
    prepared = xc.prepare(series.dates, close)
    entries = xc.entry_range(prepared, first_in)
    k = xc.scale(prepared, entries)
    exits = {
        rule: {e: xc.exit_for(rule, prepared, e, k if k is not None else math.nan) for e in entries}
        for rule in RULES
    }
    check = check_step0(series, entries, k, exits)
    result: dict[str, Any] = {
        "run_id": RUN_ID,
        "owner_decision": OWNER_DECISION,
        "market": MARKET,
        "window": {
            "window_id": window.window_id,
            "start": window.start.isoformat(),
            "end": window.end.isoformat(),
        },
        "rule": {
            "h_match": dict(H_MATCH),
            "r60": R60,
            "tail": TAIL,
            "loss_lines": list(LOSS_LINES),
            "after_days": AFTER_DAYS,
            "year_days": YEAR_DAYS,
            "gate_tail": GATE_TAIL,
            "gate_cost": GATE_COST,
            "sell": "next_close",
        },
        "check": check,
        "complete": False,
    }
    if not check["ok"]:
        return result
    start, end = entries[0] + xc.LONG_HOLD, entries[-1] + 1
    rules: dict[str, Any] = {}
    for rule in RULES:
        hold = H_MATCH[rule]
        made = [trade(close, e, exits[rule][e].sell) for e in entries]
        control = [trade(close, e, e + hold) for e in entries]
        mine, theirs = side(made), side(control)
        rules[rule] = {
            "hold": hold,
            "rule": mine,
            "control": theirs,
            "compare": compare(mine, theirs, hold),
            "exposure": exposure(made, hold, start, end),
            "false_exit": after_up(
                close, [exits[rule][e].sell for e in entries if exits[rule][e].reason == "signal"]
            ),
            "years": by_year(series.dates, made, control),
        }
    entry_set_b: list[dict[str, Any]] = []
    for touch in ec.scan(close, first_in):
        if not touch.counted or touch.entry is None or touch.entry not in entries:
            continue
        e = touch.entry
        entry_set_b.append(
            {
                "entry": series.dates[e].isoformat(),
                "r60": trade(close, e, e + R60).value,
                "rules": {
                    rule: {
                        "sell": series.dates[exits[rule][e].sell].isoformat(),
                        "value": trade(close, e, exits[rule][e].sell).value,
                        "control": trade(close, e, e + H_MATCH[rule]).value,
                    }
                    for rule in RULES
                },
            }
        )
    result["rules"] = rules
    result["r60"] = side([trade(close, e, e + R60) for e in entries])
    result["after_up_any_day"] = after_up(close, list(entries))
    result["entry_set_b"] = entry_set_b
    result["complete"] = True
    return result


def pct(value: float | None) -> str:
    return "—" if value is None else f"{100.0 * value:+.2f}%"


def points(value: float | None) -> str:
    return "—" if value is None else f"{100.0 * value:+.2f}"


def share(value: float | None) -> str:
    return "—" if value is None else f"{100.0 * value:.1f}%"


def _line(cells: Sequence[str]) -> str:
    return "| " + " | ".join(cells) + " |"


def render_markdown(result: Mapping[str, Any]) -> str:
    headers = [
        "规则",
        "H",
        "每年差（点）",
        "每笔平均差（点）",
        "CVaR5 规则",
        "CVaR5 对照",
        "CVaR5 改善",
        "5% 分位 规则",
        "5% 分位 对照",
        "最差一笔 规则",
        "最差一笔 对照",
        "亏过 15% 规则",
        "亏过 15% 对照",
        "最深浮亏 5% 规则",
        "最深浮亏 5% 对照",
        "卖后 20 天更高",
        "仓位 5%/95%/最大（倍）",
        "按笔的门尺寸标签",
    ]
    lines = [
        "# QQQ 出场规则价目表（TRADING-2565-109）",
        "",
        NOTE,
        "",
        "## 价目表（每条规则对拿 H 天）",
        "",
        _line(headers),
        "|" + "---|" * len(headers),
    ]
    rules = result["rules"]
    for rule in RULES:
        row = rules[rule]
        mine, theirs, diff = row["rule"], row["control"], row["compare"]
        after, position = row["false_exit"], row["exposure"]
        lines.append(
            _line(
                [
                    rule,
                    str(row["hold"]),
                    points(diff["per_year"]),
                    points(diff["mean_difference"]),
                    pct(mine["cvar05"]),
                    pct(theirs["cvar05"]),
                    share(diff["cvar05_gain"]),
                    pct(mine["p05"]),
                    pct(theirs["p05"]),
                    pct(mine["worst"]),
                    pct(theirs["worst"]),
                    str(mine["loss15"]),
                    str(theirs["loss15"]),
                    pct(mine["deepest_p05"]),
                    pct(theirs["deepest_p05"]),
                    f"{share(after['share'])}（{after['up']}/{after['n']}）",
                    "/".join(f"{position[key]:.2f}" for key in ("p05", "p95", "max")),
                    "是" if diff["per_trade_gate_size_like"] else "—",
                ]
            )
        )
    any_day, full = result["after_up_any_day"], result["r60"]
    lines += [
        "",
        f"任一天的底数：20 天后更高 {share(any_day['share'])}（{any_day['up']}/{any_day['n']}）。",
        f"拿满 60 天：{full['trades']} 笔，平均 {pct(full['mean'])}，"
        f"最差 {pct(full['worst'])}，5% 分位 {pct(full['p05'])}，"
        f"CVaR5 {pct(full['cvar05'])}。",
        "",
        "## 按入场年：每笔平均差（点）",
        "",
        _line(["入场年", *RULES]),
        "|" + "---|" * (1 + len(RULES)),
    ]
    years = {
        rule: {row["year"]: row["mean_difference"] for row in rules[rule]["years"]}
        for rule in RULES
    }
    for year in sorted(years[RULES[0]]):
        lines.append(_line([str(year), *(points(years[rule][year]) for rule in RULES)]))
    lines += [
        "",
        "## 200 日线回踩收盘版的入场（入场集 B）：收益（括号里是对照）",
        "",
        _line(["入场日", "拿满 60 天", *RULES]),
        "|" + "---|" * (2 + len(RULES)),
    ]
    for entry in result["entry_set_b"]:
        cells = [entry["entry"], pct(entry["r60"])]
        for rule in RULES:
            one = entry["rules"][rule]
            cells.append(f"{pct(one['value'])}（{pct(one['control'])}）")
        lines.append(_line(cells))
    return "\n".join(lines) + "\n"


def summary_line(result: Mapping[str, Any], out: Path) -> str:
    rules = result["rules"]
    yearly = "、".join(f"{rule} {points(rules[rule]['compare']['per_year'])}" for rule in RULES)
    tails = "、".join(f"{rule} {share(rules[rule]['compare']['cvar05_gain'])}" for rule in RULES)
    return (
        f"exit_rules_returns: {xc.MARKET_TEXT} {result['check']['trades']} 笔；"
        f"每年差（点）{yearly}；CVaR5 改善 {tails}；结果目录 {out}"
    )


def write_result(result: Mapping[str, Any], out: Path) -> None:
    text = json.dumps(result, indent=2, ensure_ascii=False, allow_nan=False) + "\n"
    out.mkdir(parents=True, exist_ok=True)
    out.joinpath(RESULT_JSON).write_text(text, encoding="utf-8")
    if result["complete"]:
        out.joinpath(RESULT_MD).write_text(render_markdown(result), encoding="utf-8")


@app.callback()
def main() -> None:
    """Print the QQQ exit-rule price list without investment conclusions."""


@app.command("run")
def run_command(
    out: Annotated[Path, typer.Option(help="价目表结果目录，已有结果拒绝覆盖。")],
    root: Annotated[Path, typer.Option(help="长历史快照根目录。")] = qlh.DEFAULT_SNAPSHOT_ROOT,
) -> None:
    if any(out.joinpath(name).exists() for name in (RESULT_JSON, RESULT_MD)):
        typer.echo("exit_rules_returns: 已有结果，拒绝覆盖。")
        raise typer.Exit(code=1)
    try:
        data, gate, loaded = xc.load_data(root)
        result = run_table(data, loaded, gate)
    except (OSError, ValueError, KeyError, tr.TrialError) as exc:
        lines = str(exc).splitlines()
        message = lines[0] if lines else ""
        typer.echo(f"exit_rules_returns: 数据载入失败：{message}")
        raise typer.Exit(code=1) from None
    write_result(result, out)
    if not result["complete"]:
        typer.echo("exit_rules_returns: 输入检查 FAIL，没有算收益")
        raise typer.Exit(code=1)
    typer.echo(summary_line(result, out))


if __name__ == "__main__":
    app()
