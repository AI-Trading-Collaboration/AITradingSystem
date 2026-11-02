"""The QQQ exit family read decline by decline (TRADING-2565-117).

Under owner_decision:TRADING-2565:2026-11-02:exit_family_big_declines,
Baker chose to read each big decline after the 20-day fall tables (-113/-115).
His entries are infrequent and mostly during high volatility, so exits should
find positions before big falls for someone holding through the peak. A
retrospective 15% closing-price zigzag describes declines on both spans;
four entry-price-independent rules use only closes known on the decision day.
Print distance from the peak, the fraction avoided and false alarms per year.
Print only: no verdicts or accounting entries, no gate or ruler changes,
and no daily report or broker action.
"""

from __future__ import annotations

import json
import math
from bisect import bisect_left, bisect_right
from collections.abc import Mapping, Sequence
from datetime import date
from pathlib import Path
from typing import Annotated, Any

import typer

from ai_trading_system.research_bench import trial as tr
from ai_trading_system.research_bench.data import BenchData
from ai_trading_system.research_bench.settings import LoadedSettings
from ai_trading_system.research_data import qqq_long_history as qlh
from ai_trading_system.research_probes import entry_timing_counts as ec
from ai_trading_system.research_probes import entry_timing_ndx as en
from ai_trading_system.research_probes import exit_rules_after as xa
from ai_trading_system.research_probes import exit_rules_counts as xc
from ai_trading_system.research_probes import exit_rules_ndx as xn
from ai_trading_system.research_probes import exit_rules_returns as xr

RUN_ID = "TRADING-2565-117"
OWNER_DECISION = "owner_decision:TRADING-2565:2026-11-02:exit_family_big_declines"
RULES = ("X2", "X3", "X4", "X5")
NOT_APPLICABLE = ("X0", "X1", "X6")
SPANS = xa.SPANS
# Frozen print-only pilot under OWNER_DECISION; exit at Baker's review of this table.
# These numbers describe declines and alarm locations, not acceptance or promotion gates.
SWING_LINE = 0.15
PRE_PEAK = 20
EARLY = 1.0 / 3.0
TRAIL = xc.STOP
YEAR_DAYS = 365.25
SPAN_TEXT = xa.SPAN_TEXT
RULE_TEXT = {
    "X2": "移动止损（回落 8%）",
    "X3": "跌破 50 日线",
    "X4": "周急跌破位",
    "X5": "第二腿",
}
RESULT_JSON = "exit_rules_declines.json"
RESULT_MD = "exit_rules_declines.md"
NOTE = (
    "只印不判，不检验、不下结论：QQQ 1999–2010 开发窗口切到 2010-12-31；"
    "纳斯达克 100 价格指数 1986–1999，不含分红。两段对出场族都是看过结果的历史。"
    "大跌是事后按收盘 15% 的之字形划的，最后一段没被反向确认的不算；"
    "只用来对照，不进任何规则。看的是一直拿过高点的人："
    "X2 从重新开始以来的最高收盘回落 8%，是第 0 步 X2 宽度的平均，"
    "波动大时买进的人宽度会更宽；X3 连续两个收盘在 50 日均线下；"
    "X4 周急跌破位；X5 第二腿，不设 120 天期限。四条都只用当时已知的收盘，"
    "X2、X5 每次响了以后从第二天重新开始。X0、X1、X6 按买入价算"
    "（或只在买进后第 20 天判一次），不算。"
    "每次大跌取高点之后、低点当天为止第一次响的日子，下一个收盘卖；"
    "高点时规则还算不出来的记缺历史，不进汇总。"
    "躲掉几成是（卖出收盘 − 低点收盘）÷（高点收盘 − 低点收盘），"
    "低点当天才响或没响算 0；「早」是已跌不到整段的三分之一（按对数），含等号。"
    "警报按簇取第一天（隔不到 20 个交易日的并成一簇），"
    "只分第一个和最后一个拐点之间的警报；在大跌里、在高点前 20 天里"
    "（截到上一个低点之后）以外的算虚惊，按日历天数折成每年几个。"
    "买回是入场信号的事，这张表不算错过的反弹。"
)
app = typer.Typer(help="QQQ 出场族按大跌逐次看：只印不判。")


def pivots(close: Sequence[float]) -> list[tuple[int, str]]:
    """Confirm alternating extremes only after a reversal of the frozen width."""
    found: list[tuple[int, str]] = []
    high = low = 0
    direction: str | None = None
    for t in range(1, len(close)):
        if direction == "up":
            if close[t] > close[high]:
                high = t
            elif close[t] / close[high] <= 1.0 - SWING_LINE:
                found.append((high, "peak"))
                direction, low = "down", t
        elif direction == "down":
            if close[t] < close[low]:
                low = t
            elif close[t] / close[low] >= 1.0 + SWING_LINE:
                found.append((low, "trough"))
                direction, high = "up", t
        else:
            if close[t] > close[high]:
                high = t
            if close[t] < close[low]:
                low = t
            if close[t] / close[high] <= 1.0 - SWING_LINE:
                found.append((high, "peak"))
                direction, low = "down", t
            elif close[t] / close[low] >= 1.0 + SWING_LINE:
                found.append((low, "trough"))
                direction, high = "up", t
    return found


def declines(found: Sequence[tuple[int, str]]) -> list[tuple[int, int]]:
    return [
        (p, q)
        for (p, first), (q, second) in zip(found, found[1:], strict=False)
        if first == "peak" and second == "trough"
    ]


def previous_low(found: Sequence[tuple[int, str]], p: int) -> int | None:
    return next((i for i, kind in reversed(found) if i < p and kind == "trough"), None)


def window_start(p: int, q0: int | None) -> int:
    return max(p - PRE_PEAK, 0 if q0 is None else q0 + 1, 0)


def first_signal(rule: str, data: xc.Prepared, e: int) -> int | None:
    """Run X2 or X5 without a holding limit, using the step-zero state machine."""
    if rule not in ("X2", "X5"):
        raise ValueError(f"unknown rule {rule}")
    close = data.close
    high = close[e]
    break_day: int | None = None
    break_high = high
    low: float | None = None
    for d in range(e + 1, len(close)):
        high = max(high, close[d])
        if rule == "X2":
            if close[d] / high <= 1.0 - TRAIL:
                return d
        elif rule == "X5":
            if break_day is None:
                average = data.sma[d]
                if average is not None and close[d] < average:
                    break_day, break_high = d, high
                continue
            if close[d] >= break_high:
                break_day, low = None, None
                continue
            if low is None:
                s = d - xc.SWING
                if s >= break_day + 1 and all(
                    close[s] > close[p] for p in range(s - xc.SWING, s + xc.SWING + 1) if p != s
                ):
                    low = min(close[break_day : s + 1])
            if low is not None and close[d] < low:
                return d
    return None


def signal_days(rule: str, data: xc.Prepared) -> list[int]:
    n = len(data.close)
    if rule == "X3":
        return [
            d
            for d in range(1, n)
            if (current := data.sma[d]) is not None
            and (previous := data.sma[d - 1]) is not None
            and data.close[d] < current
            and data.close[d - 1] < previous
        ]
    if rule == "X4":
        return [d for d, flag in enumerate(data.drop) if flag]
    if rule not in ("X2", "X5"):
        raise ValueError(f"unknown rule {rule}")
    days: list[int] = []
    e = 0
    while e < n - 1:
        d = first_signal(rule, data, e)
        if d is None:
            break
        days.append(d)
        e = d + 1
    return days


def ready(rule: str, data: xc.Prepared) -> int:
    n = len(data.close)
    if rule == "X2":
        return 0
    if rule in ("X3", "X5"):
        return next((p for p, value in enumerate(data.sma) if value is not None), n)
    if rule == "X4":
        ends = [p for p in range(n - 1) if data.week[p + 1] > data.week[p]]
        return ends[xc.WEEKS + 1] if len(ends) > xc.WEEKS + 1 else n
    raise ValueError(f"unknown rule {rule}")


def episode(
    close: Sequence[float],
    dates: Sequence[date],
    p: int,
    q: int,
    q0: int | None,
    days: Sequence[int],
    start: int,
) -> dict[str, Any]:
    if p < start:
        return {"status": "no_history"}
    after_peak = bisect_right(days, p)
    pre_peak = bisect_left(days, window_start(p, q0)) < after_peak
    if after_peak >= len(days) or days[after_peak] > q:
        return {"status": "silent", "avoided": 0.0, "pre_peak": pre_peak}
    d = days[after_peak]
    v = d + 1
    return {
        "status": "fired",
        "date": dates[d].isoformat(),
        "days_after_peak": d - p,
        "fall_at": close[d] / close[p] - 1.0,
        "fall_before": close[d - 1] / close[p] - 1.0,
        "done": math.log(close[d] / close[p]) / math.log(close[q] / close[p]) + 0.0,
        "after_sell": close[q] / close[v] - 1.0 if v <= q else 0.0,
        "avoided": (close[v] - close[q]) / (close[p] - close[q]) if v <= q else 0.0,
        "pre_peak": pre_peak,
    }


def summary(
    rows: Sequence[Mapping[str, Any]], reference: Sequence[Mapping[str, Any]] | None
) -> dict[str, int | float | None]:
    kept = [row for row in rows if row["status"] != "no_history"]
    fired = [row for row in kept if row["status"] == "fired"]
    early = [row for row in fired if row["done"] <= EARLY]
    falls = [float(row["fall_at"]) for row in fired]
    days = [float(row["days_after_peak"]) for row in fired]
    before_x2 = (
        None
        if reference is None
        else sum(
            row["status"] == "fired"
            and other["status"] == "fired"
            and row["days_after_peak"] < other["days_after_peak"]
            for row, other in zip(rows, reference, strict=True)
        )
    )
    return {
        "n": len(kept),
        "no_history": len(rows) - len(kept),
        "fired": len(fired),
        "early": len(early),
        "early_quiet": sum(not row["pre_peak"] for row in early),
        "before_x2": before_x2,
        "pre_peak": sum(bool(row["pre_peak"]) for row in kept),
        "fall_at_mean": xa.mean(falls),
        "fall_at_median": xr.rank(falls, 0.5) if falls else None,
        "done_mean": xa.mean([float(row["done"]) for row in fired]),
        "days_median": xr.rank(days, 0.5) if days else None,
        "avoided_mean": math.fsum(row["avoided"] for row in kept) / len(kept) if kept else None,
    }


def pair(rows: Sequence[Mapping[str, Mapping[str, Any]]]) -> dict[str, int]:
    both = [row for row in rows if row["X3"]["status"] == row["X5"]["status"] == "fired"]
    return {
        "both": len(both),
        "x5_not_earlier": sum(
            row["X5"]["days_after_peak"] >= row["X3"]["days_after_peak"] for row in both
        ),
    }


def alarm_kind(o: int, spans: Sequence[tuple[int, int, int | None]]) -> str:
    if any(p < o <= q for p, q, _ in spans):
        return "decline"
    if any(window_start(p, q0) <= o <= p for p, _, q0 in spans):
        return "pre_peak"
    return "false"


def alarms(
    days: Sequence[int],
    dates: Sequence[date],
    found: Sequence[tuple[int, str]],
    spans: Sequence[tuple[int, int, int | None]],
) -> dict[str, Any]:
    onsets = xa.first_days(days)
    if not found:
        return {
            "rows": [],
            "outside": len(onsets),
            "years": None,
            "decline": 0,
            "pre_peak": 0,
            "false": 0,
            "per_year": None,
            "false_per_year": None,
        }
    lo, hi = found[0][0], found[-1][0]
    rows = [
        {"date": dates[o].isoformat(), "kind": alarm_kind(o, spans)}
        for o in onsets
        if lo <= o <= hi
    ]
    years = (dates[hi] - dates[lo]).days / YEAR_DAYS
    counts = {
        kind: sum(row["kind"] == kind for row in rows) for kind in ("decline", "pre_peak", "false")
    }
    return {
        "rows": rows,
        "outside": len(onsets) - len(rows),
        "years": years,
        **counts,
        "per_year": len(rows) / years if years > 0 else None,
        "false_per_year": counts["false"] / years if years > 0 else None,
    }


def span_table(series: ec.Closes, data: xc.Prepared) -> dict[str, Any]:
    close, dates = series.close, series.dates
    n = len(close)
    found = pivots(close)
    spans = [(p, q, previous_low(found, p)) for p, q in declines(found)]
    days = {rule: signal_days(rule, data) for rule in RULES}
    starts = {rule: ready(rule, data) for rule in RULES}
    episodes: list[dict[str, Any]] = [
        {
            "peak": dates[p].isoformat(),
            "trough": dates[q].isoformat(),
            "truncated": p == 0,
            "depth": close[q] / close[p] - 1.0,
            "days": q - p,
            "rise_before": close[p] / close[q0] - 1.0 if q0 is not None else None,
            "rise_days": p - q0 if q0 is not None else None,
            "rules": {
                rule: episode(close, dates, p, q, q0, days[rule], starts[rule]) for rule in RULES
            },
        }
        for p, q, q0 in spans
    ]
    rows = {rule: [one["rules"][rule] for one in episodes] for rule in RULES}
    return {
        "first_day": dates[0].isoformat(),
        "last_day": dates[-1].isoformat(),
        "pivots": [
            {"date": dates[i].isoformat(), "kind": kind, "close": close[i]} for i, kind in found
        ],
        "episode_count": len(episodes),
        "episodes": episodes,
        "rules": {
            rule: {
                "signals": len(days[rule]),
                "ready": dates[starts[rule]].isoformat() if starts[rule] < n else None,
                **summary(rows[rule], None if rule == "X2" else rows["X2"]),
                "alarms": alarms(days[rule], dates, found, spans),
            }
            for rule in RULES
        },
        "x5_vs_x3": pair([one["rules"] for one in episodes]),
    }


def run_table(
    data: BenchData,
    loaded: LoadedSettings,
    gate: tr.TrialGate,
    levels: Sequence[tuple[date, float]],
    source: Mapping[str, Any],
    window: Mapping[str, Any],
) -> dict[str, Any]:
    q_series, q_prepared, _, _, q_check = xa.qqq_inputs(data, loaded, gate)
    n_series, flat = en.prices(levels)
    n_prepared = xc.prepare(n_series.dates, n_series.close)
    n_entries = xc.entry_range(n_prepared, 0)
    n_check = xn.check_inputs(n_series, flat, window, n_entries)
    result: dict[str, Any] = {
        "run_id": RUN_ID,
        "owner_decision": OWNER_DECISION,
        "source": {"qqq": {"snapshot_id": data.source.get("snapshot_id")}, "ndx": dict(source)},
        "rule": {
            "rules": list(RULES),
            "not_applicable": list(NOT_APPLICABLE),
            "swing_line": SWING_LINE,
            "trail": TRAIL,
            "pre_peak": PRE_PEAK,
            "early": EARLY,
            "cluster_gap": xc.CLUSTER_GAP,
            "sell": "next_close",
        },
        "check": {"qqq": q_check, "ndx": n_check},
        "complete": False,
    }
    if not q_check["ok"] or not n_check["ok"]:
        return result
    result["spans"] = {
        "qqq": span_table(q_series, q_prepared),
        "ndx": span_table(n_series, n_prepared),
    }
    result["complete"] = True
    return result


def share_text(value: float | None) -> str:
    return "—" if value is None else f"{100 * value:.0f}%"


def rate_text(value: float | None) -> str:
    return "—" if value is None else f"{value:.1f}"


def _line(cells: Sequence[str]) -> str:
    return "| " + " | ".join(cells) + " |"


def cell(row: Mapping[str, Any]) -> str:
    if row["status"] == "no_history":
        return "缺历史"
    text = (
        "没响"
        if row["status"] == "silent"
        else f"{xr.pct(row['fall_at'])} / {share_text(row['avoided'])}"
    )
    return text + ("＊" if row["pre_peak"] else "")


def render_markdown(result: Mapping[str, Any]) -> str:
    lines = ["# QQQ 出场族按大跌逐次看（TRADING-2565-117）", "", NOTE, ""]
    headers = [
        "规则",
        "是什么",
        "大跌次数",
        "响了",
        "早",
        "早而且高点前没响",
        "比 X2 先响",
        "平均响时已跌",
        "中位响时已跌",
        "平均已跌占整段",
        "离高点天数中位",
        "平均躲掉几成",
        "高点前 20 天响过",
        "警报每年",
        "虚惊每年",
        "在大跌里 / 高点前 / 虚惊",
    ]
    episode_headers = ["高点", "低点", "跌幅", "天数", "之前涨了", "上涨段天数", *RULES]
    for span in SPANS:
        one = result["spans"][span]
        lines += [
            f"## {SPAN_TEXT[span]}",
            "",
            f"收盘 {one['first_day']}..{one['last_day']}，"
            f"跌过 15% 的大跌 {one['episode_count']} 次。",
            "",
            _line(headers),
            "|" + "---|" * len(headers),
        ]
        for rule in RULES:
            row = one["rules"][rule]
            alarm = row["alarms"]
            lines.append(
                _line(
                    [
                        rule,
                        RULE_TEXT[rule],
                        str(row["n"]),
                        str(row["fired"]),
                        str(row["early"]),
                        str(row["early_quiet"]),
                        "—" if row["before_x2"] is None else str(row["before_x2"]),
                        xr.pct(row["fall_at_mean"]),
                        xr.pct(row["fall_at_median"]),
                        share_text(row["done_mean"]),
                        "—" if row["days_median"] is None else f"{row['days_median']:.0f}",
                        share_text(row["avoided_mean"]),
                        str(row["pre_peak"]),
                        rate_text(alarm["per_year"]),
                        rate_text(alarm["false_per_year"]),
                        f"{alarm['decline']} / {alarm['pre_peak']} / {alarm['false']}",
                    ]
                )
            )
        paired = one["x5_vs_x3"]
        lines += [
            "",
            f"X3、X5 都响的 {paired['both']} 次里，X5 不比 X3 早的 {paired['x5_not_earlier']} 次。",
            "",
            "每次大跌：每条规则的格写「响时已跌 / 躲掉几成」；"
            "＊ 是高点前 20 天里（截到上一个低点之后）响过。",
            "",
            _line(episode_headers),
            "|" + "---|" * len(episode_headers),
        ]
        for row in one["episodes"]:
            lines.append(
                _line(
                    [
                        row["peak"] + ("（开头截断）" if row["truncated"] else ""),
                        row["trough"],
                        xr.pct(row["depth"]),
                        str(row["days"]),
                        xr.pct(row["rise_before"]),
                        "—" if row["rise_days"] is None else str(row["rise_days"]),
                        *(cell(row["rules"][rule]) for rule in RULES),
                    ]
                )
            )
        outside = "、".join(f"{rule} {one['rules'][rule]['alarms']['outside']}" for rule in RULES)
        lines += ["", f"第一个和最后一个拐点以外、没分类的警报：{outside}。", ""]
    return "\n".join(lines).rstrip() + "\n"


def summary_line(result: Mapping[str, Any], out: Path) -> str:
    spans: list[str] = []
    for span in SPANS:
        one = result["spans"][span]
        rules = "、".join(
            f"{rule} 躲掉 {share_text(one['rules'][rule]['avoided_mean'])}、"
            f"虚惊每年 {rate_text(one['rules'][rule]['alarms']['false_per_year'])}"
            for rule in RULES
        )
        spans.append(f"{SPAN_TEXT[span]} 大跌 {one['episode_count']} 次，{rules}")
    return "exit_rules_declines: 按大跌逐次看：" + "；".join(spans) + f"；结果目录 {out}"


def write_result(result: Mapping[str, Any], out: Path) -> None:
    text = json.dumps(result, indent=2, ensure_ascii=False, allow_nan=False) + "\n"
    out.mkdir(parents=True, exist_ok=True)
    out.joinpath(RESULT_JSON).write_text(text, encoding="utf-8")
    if result["complete"]:
        out.joinpath(RESULT_MD).write_text(render_markdown(result), encoding="utf-8")


@app.callback()
def main() -> None:
    """Print the exit family decline table without investment conclusions."""


@app.command("run")
def run_command(
    out: Annotated[Path, typer.Option(help="逐次大跌结果目录，已有结果拒绝覆盖。")],
    root: Annotated[Path, typer.Option(help="长历史快照根目录。")] = qlh.DEFAULT_SNAPSHOT_ROOT,
) -> None:
    if any(out.joinpath(name).exists() for name in (RESULT_JSON, RESULT_MD)):
        typer.echo("exit_rules_declines: 已有结果，拒绝覆盖。")
        raise typer.Exit(code=1)
    try:
        data, gate, loaded = xc.load_data(root)
        levels, source, window = en.load_data(root)
        result = run_table(data, loaded, gate, levels, source, window)
    except (OSError, ValueError, KeyError, tr.TrialError) as exc:
        lines = str(exc).splitlines()
        message = lines[0] if lines else ""
        typer.echo(f"exit_rules_declines: 数据载入失败：{message}")
        raise typer.Exit(code=1) from None
    write_result(result, out)
    if not result["complete"]:
        typer.echo("exit_rules_declines: 输入检查 FAIL，没有划大跌")
        raise typer.Exit(code=1)
    typer.echo(summary_line(result, out))


if __name__ == "__main__":
    app()
