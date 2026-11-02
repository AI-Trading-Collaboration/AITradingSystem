"""The QQQ exit family's price list on the Nasdaq-100 price index 1986–1999.

TRADING-2565-111, owner_decision:TRADING-2565:2026-11-02:exit_family_ndx_1986_1999:
after the QQQ 1999–2010 price list (TRADING-2565-109), Baker chose to print
that same table on QQQ's tracked index before QQQ began trading. Read the
registered ndx_price_1986_1999 window through entry_timing_ndx. Every close
after 52 weeks of history is an entry. Keep the seven rules and stop factor
k frozen from QQQ; recompute matched holds here using only sell dates.
Print only: no verdict or accounting entries, no gate or ruler changes,
and no daily-report or broker action.
"""

from __future__ import annotations

import json
import math
from collections.abc import Mapping, Sequence
from datetime import date
from pathlib import Path
from typing import Annotated, Any

import typer

from ai_trading_system.research_data import qqq_long_history as qlh
from ai_trading_system.research_probes import entry_timing_counts as ec
from ai_trading_system.research_probes import entry_timing_ndx as en
from ai_trading_system.research_probes import exit_rules_counts as xc
from ai_trading_system.research_probes import exit_rules_returns as xr

RUN_ID = "TRADING-2565-111"
OWNER_DECISION = "owner_decision:TRADING-2565:2026-11-02:exit_family_ndx_1986_1999"
RULES = xc.RULES
K = xr.STEP0_K
# Frozen at step zero on QQQ 2000–2010, not recomputed on this span.
QQQ_H = dict(xr.H_MATCH)
# Frozen print-only pilot under OWNER_DECISION; exit at Baker's price-list review.
# These reproduce the TRADING-2565-107 data check and -109 reference readings.
NDX_CLOSES = 3317
NDX_LAST = "1999-03-09"
NDX_FLAT_DATES = (
    "1986-07-31",
    "1986-12-29",
    "1987-02-06",
    "1987-08-14",
    "1988-06-13",
    "1988-10-19",
    "1989-03-01",
    "1989-05-02",
    "1989-08-01",
    "1989-09-25",
    "1990-12-13",
    "1992-03-24",
    "1995-03-22",
    "1997-08-15",
    "1998-07-03",
    "1999-01-20",
)
NDX_MOVE_DATE = "1987-10-19"
NDX_MOVE = -0.1508
MIN_TRADES = 2000
QQQ_PER_YEAR = {
    "X0": 0.0630,
    "X1": 0.0359,
    "X2": 0.0201,
    "X3": 0.0191,
    "X4": -0.0509,
    "X5": 0.0553,
    "X6": 0.0144,
}
QQQ_CVAR05_GAIN = {
    "X0": 0.5543,
    "X1": 0.3423,
    "X2": 0.3576,
    "X3": 0.4900,
    "X4": -0.1117,
    "X5": 0.4653,
    "X6": 0.1687,
}
RESULT_JSON = "exit_rules_ndx.json"
RESULT_MD = "exit_rules_ndx.md"
NOTE = (
    "只印不判，不检验、不下结论：数据是纳斯达克 100 价格指数（不含分红），"
    "1986-01-02..1999-03-09，有 52 周历史以后每天收盘入场。"
    "七条规则和止损系数 k 照 QQQ 2000–2010 写死，H 在这一段上只用卖出日重算，"
    "QQQ 的 H 印在旁边。这一段以涨为主，大跌在 1987-10、1990、1998。"
    "收益是按笔算术，per_trade_gate_size_like 标签是按笔的，门是组合几何的，不是过门。"
    "每年差是平均每笔差 × 252 ÷ H，是在场的时间点换来的；每天入场的仓位不是 1 倍。"
)
app = typer.Typer(help="QQQ 出场规则在纳斯达克 100 价格指数上的价目表：只印不判。")


def check_inputs(
    series: ec.Closes,
    flat: Sequence[date],
    window: Mapping[str, Any],
    entries: Sequence[int],
) -> dict[str, Any]:
    move = en.largest_move(series)
    first = series.dates[0].isoformat() if series.dates else None
    last = series.dates[-1].isoformat() if series.dates else None
    flat_dates = [day.isoformat() for day in flat]
    return {
        **dict(window),
        "missing_days": series.missing,
        "closes": len(series.close),
        "flat_days": len(flat),
        "flat_dates": flat_dates,
        "first_close_date": first,
        "last_close_date": last,
        "largest_move": move,
        "trades": len(entries),
        "first_entry": series.dates[entries[0]].isoformat() if entries else None,
        "last_entry": series.dates[entries[-1]].isoformat() if entries else None,
        "ok": (
            series.missing == 0
            and first == en.START.isoformat()
            and last == NDX_LAST
            and len(series.close) == NDX_CLOSES
            and tuple(flat_dates) == NDX_FLAT_DATES
            and move is not None
            and move["date"] == NDX_MOVE_DATE
            and round(move["return"], 4) == NDX_MOVE
            and len(entries) >= MIN_TRADES
        ),
    }


def scale(prepared: xc.Prepared, entries: Sequence[int]) -> dict[str, float]:
    # Use volatility only, never trade returns.
    sigmas = [s for e in entries if (s := prepared.sigma[e]) is not None]
    mean = math.fsum(sigmas) / len(sigmas)
    distances = [K * s for s in sigmas]
    return {
        "sigma20_mean": mean,
        "x0_stop_in_sigma20": xc.STOP / mean,
        "qqq_x0_stop_in_sigma20": K,
        "x1_x2_distance_mean": math.fsum(distances) / len(distances),
        "x1_x2_distance_p10": xr.rank(distances, 0.10),
        "x1_x2_distance_p90": xr.rank(distances, 0.90),
    }


def run_table(
    levels: Sequence[tuple[date, float]],
    source: Mapping[str, Any],
    window: Mapping[str, Any],
) -> dict[str, Any]:
    series, flat = en.prices(levels)
    close = series.close
    prepared = xc.prepare(series.dates, close)
    entries = xc.entry_range(prepared, 0)
    check = check_inputs(series, flat, window, entries)
    result: dict[str, Any] = {
        "run_id": RUN_ID,
        "owner_decision": OWNER_DECISION,
        "source": dict(source),
        "rule": {
            "k": K,
            "qqq_h": dict(QQQ_H),
            "r60": xr.R60,
            "tail": xr.TAIL,
            "loss_lines": list(xr.LOSS_LINES),
            "after_days": xr.AFTER_DAYS,
            "year_days": xr.YEAR_DAYS,
            "gate_tail": xr.GATE_TAIL,
            "gate_cost": xr.GATE_COST,
            "sell": "next_close",
        },
        "check": check,
        "complete": False,
    }
    if not check["ok"]:
        return result
    result["scale"] = scale(prepared, entries)
    exits = {rule: {e: xc.exit_for(rule, prepared, e, K) for e in entries} for rule in RULES}
    start, end = entries[0] + xc.LONG_HOLD, entries[-1] + 1
    rules: dict[str, Any] = {}
    for rule in RULES:
        counts = xc.rule_counts(exits[rule])
        hold = int(counts["h_match"] or 0)
        made = [xr.trade(close, e, exits[rule][e].sell) for e in entries]
        control = [xr.trade(close, e, e + hold) for e in entries]
        mine, theirs = xr.side(made), xr.side(control)
        position = xr.exposure(made, hold, start, end)
        rules[rule] = {
            "hold": hold,
            "qqq_hold": QQQ_H[rule],
            "counts": counts,
            "rule": mine,
            "control": theirs,
            "compare": xr.compare(mine, theirs, hold),
            "exposure": position,
            # Arithmetic drift attributable to mean exposure deviating from one.
            "drift_from_exposure": (position["mean"] - 1.0) * theirs["mean"] * xr.YEAR_DAYS / hold,
            "false_exit": xr.after_up(
                close, [exits[rule][e].sell for e in entries if exits[rule][e].reason == "signal"]
            ),
            "years": xr.by_year(series.dates, made, control),
            "qqq": {"per_year": QQQ_PER_YEAR[rule], "cvar05_gain": QQQ_CVAR05_GAIN[rule]},
        }
    entry_set_b: list[dict[str, Any]] = []
    for touch in ec.scan(close, 0):
        if not touch.counted or touch.entry is None or touch.entry not in entries:
            continue
        e = touch.entry
        entry_set_b.append(
            {
                "entry": series.dates[e].isoformat(),
                "r60": xr.trade(close, e, e + xr.R60).value,
                "rules": {
                    rule: {
                        "sell": series.dates[exits[rule][e].sell].isoformat(),
                        "value": xr.trade(close, e, exits[rule][e].sell).value,
                        "control": xr.trade(close, e, e + rules[rule]["hold"]).value,
                    }
                    for rule in RULES
                },
            }
        )
    result["rules"] = rules
    result["r60"] = xr.side([xr.trade(close, e, e + xr.R60) for e in entries])
    result["after_up_any_day"] = xr.after_up(close, list(entries))
    result["entry_set_b"] = entry_set_b
    result["complete"] = True
    return result


def _line(cells: Sequence[str]) -> str:
    return "| " + " | ".join(cells) + " |"


def render_markdown(result: Mapping[str, Any]) -> str:
    check, width = result["check"], result["scale"]
    move = check["largest_move"]
    lines = [
        f"# QQQ 出场规则价目表：{en.TITLE}（{RUN_ID}）",
        "",
        NOTE,
        "",
        f"数据：快照 `{result['source']['snapshot_id']}` 的 `{result['source']['column']}`，"
        f"{check['first_close_date']}..{check['last_close_date']}，{check['closes']} 个收盘；"
        f"同值日 {check['flat_days']} 天；最大单日变动 {xr.pct(move['return'])}（{move['date']}）；"
        f"入场 {check['trades']} 笔，{check['first_entry']}..{check['last_entry']}；k = {K}。",
        f"止损宽度：20 日波动平均 {xr.share(width['sigma20_mean'])}；"
        f"X0 的 8% 约是 {width['x0_stop_in_sigma20']:.2f} 个 20 日波动"
        f"（QQQ 2000–2010 上 {K:.2f} 个）；"
        f"X1、X2 距离平均 {xr.share(width['x1_x2_distance_mean'])}，"
        f"10%/90% 分位 {xr.share(width['x1_x2_distance_p10'])}"
        f"/{xr.share(width['x1_x2_distance_p90'])}。",
        "",
        "## 次数",
        "",
        _line(
            [
                "规则",
                "笔数",
                "信号出场",
                "到期",
                "平均持有",
                "中位",
                "10%",
                "90%",
                "H（这一段）",
                "H（QQQ）",
            ]
        ),
        "|" + "---|" * 10,
    ]
    rules = result["rules"]
    for rule in RULES:
        row = rules[rule]
        counts = row["counts"]
        lines.append(
            _line(
                [
                    rule,
                    str(counts["trades"]),
                    str(counts["signal"]),
                    str(counts["time"]),
                    f"{counts['hold_mean']:.1f}",
                    str(counts["hold_median"]),
                    str(counts["hold_p10"]),
                    str(counts["hold_p90"]),
                    str(row["hold"]),
                    str(row["qqq_hold"]),
                ]
            )
        )
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
        "平均仓位（倍）",
        "仓位偏 1 的漂移（点）",
        "仓位 5%/95%/最大（倍）",
        "按笔的门尺寸标签",
        "QQQ 2000–2010 每年差（点）",
        "QQQ 2000–2010 CVaR5 改善",
    ]
    lines += ["", "## 价目表（每条规则对拿 H 天）", "", _line(headers), "|" + "---|" * len(headers)]
    for rule in RULES:
        row = rules[rule]
        mine, theirs, diff = row["rule"], row["control"], row["compare"]
        after, position = row["false_exit"], row["exposure"]
        lines.append(
            _line(
                [
                    rule,
                    str(row["hold"]),
                    xr.points(diff["per_year"]),
                    xr.points(diff["mean_difference"]),
                    xr.pct(mine["cvar05"]),
                    xr.pct(theirs["cvar05"]),
                    xr.share(diff["cvar05_gain"]),
                    xr.pct(mine["p05"]),
                    xr.pct(theirs["p05"]),
                    xr.pct(mine["worst"]),
                    xr.pct(theirs["worst"]),
                    str(mine["loss15"]),
                    str(theirs["loss15"]),
                    xr.pct(mine["deepest_p05"]),
                    xr.pct(theirs["deepest_p05"]),
                    f"{xr.share(after['share'])}（{after['up']}/{after['n']}）",
                    f"{position['mean']:.4f}",
                    xr.points(row["drift_from_exposure"]),
                    "/".join(f"{position[key]:.2f}" for key in ("p05", "p95", "max")),
                    "是" if diff["per_trade_gate_size_like"] else "—",
                    xr.points(row["qqq"]["per_year"]),
                    xr.share(row["qqq"]["cvar05_gain"]),
                ]
            )
        )
    any_day, full = result["after_up_any_day"], result["r60"]
    lines += [
        "",
        f"任一天的底数：20 天后更高 {xr.share(any_day['share'])}"
        f"（{any_day['up']}/{any_day['n']}）。",
        f"拿满 60 天：{full['trades']} 笔，平均 {xr.pct(full['mean'])}，"
        f"最差 {xr.pct(full['worst'])}，5% 分位 {xr.pct(full['p05'])}，"
        f"CVaR5 {xr.pct(full['cvar05'])}。",
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
        lines.append(_line([str(year), *(xr.points(years[rule][year]) for rule in RULES)]))
    lines += [
        "",
        "## 200 日线回踩收盘版的入场（入场集 B）：收益（括号里是对照）",
        "",
        _line(["入场日", "拿满 60 天", *RULES]),
        "|" + "---|" * (2 + len(RULES)),
    ]
    for entry in result["entry_set_b"]:
        cells = [entry["entry"], xr.pct(entry["r60"])]
        for rule in RULES:
            one = entry["rules"][rule]
            cells.append(f"{xr.pct(one['value'])}（{xr.pct(one['control'])}）")
        lines.append(_line(cells))
    return "\n".join(lines) + "\n"


def summary_line(result: Mapping[str, Any], out: Path) -> str:
    rules = result["rules"]
    yearly = "、".join(f"{rule} {xr.points(rules[rule]['compare']['per_year'])}" for rule in RULES)
    tails = "、".join(f"{rule} {xr.share(rules[rule]['compare']['cvar05_gain'])}" for rule in RULES)
    return (
        f"exit_rules_ndx: {en.TITLE} {result['check']['trades']} 笔；"
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
    """Print the exit family's price list on QQQ's tracked price index, without verdicts."""


@app.command("run")
def run_command(
    out: Annotated[Path, typer.Option(help="价目表结果目录，已有结果拒绝覆盖。")],
    root: Annotated[Path, typer.Option(help="长历史快照根目录。")] = qlh.DEFAULT_SNAPSHOT_ROOT,
) -> None:
    if any(out.joinpath(name).exists() for name in (RESULT_JSON, RESULT_MD)):
        typer.echo("exit_rules_ndx: 已有结果，拒绝覆盖。")
        raise typer.Exit(code=1)
    try:
        levels, source, window = en.load_data(root)
        result = run_table(levels, source, window)
    except (OSError, ValueError, KeyError) as exc:
        lines = str(exc).splitlines()
        message = lines[0] if lines else ""
        typer.echo(f"exit_rules_ndx: 数据载入失败：{message}")
        raise typer.Exit(code=1) from None
    write_result(result, out)
    if not result["complete"]:
        typer.echo("exit_rules_ndx: 输入检查 FAIL，没有算收益")
        raise typer.Exit(code=1)
    typer.echo(summary_line(result, out))


if __name__ == "__main__":
    app()
