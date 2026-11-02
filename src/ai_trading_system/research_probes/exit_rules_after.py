"""How far the price falls in the 20 closes after each QQQ exit signal (TRADING-2565-113).

Under owner_decision:TRADING-2565:2026-11-02:exit_family_after_signal, Baker
asked about the next 20 days after reviewing the two price lists (-109, -111).
Exit signals govern exits; buying back belongs to entry signals. Keep step
zero's entries, seven rules and stop factor on both spans. Select signal
exits at the decision close and print subsequent falls and drawdowns beside
every day on that span and every day in the same volatility band.
TRADING-2565-115 adds both sides' mean volatility and a volatility-scaled same-band reference.
Print only:
no verdicts or accounting entries, no gate or ruler changes, and no daily
report or broker action.
"""

from __future__ import annotations

import json
import math
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
from ai_trading_system.research_probes import exit_rules_counts as xc
from ai_trading_system.research_probes import exit_rules_ndx as xn
from ai_trading_system.research_probes import exit_rules_returns as xr
from ai_trading_system.research_probes import policy_table as pt

RUN_ID = "TRADING-2565-113"
OWNER_DECISION = "owner_decision:TRADING-2565:2026-11-02:exit_family_after_signal"
RULES = xc.RULES
SPANS = ("qqq", "ndx")
VIEWS = ("trades", "days", "clusters")
MATCHED = ("fall_mean", "fall_share10", "scaled_fall_mean", "sigma_mean", "r20_mean")
# Frozen print-only pilot under OWNER_DECISION; exit at Baker's review of this table.
# These describe the post-signal paths, not acceptance or promotion gates.
AFTER = 20
TAIL = xr.TAIL
FALL_LINES = (0.05, 0.10)
BANDS = 5
SPAN_TEXT = {"qqq": "QQQ 1999–2010（开发窗口，切到 2010-12-31）", "ndx": en.TITLE}
RESULT_JSON = "exit_rules_after.json"
RESULT_MD = "exit_rules_after.md"
NOTE = (
    "只印不判，不检验、不下结论：QQQ 1999–2010 开发窗口切到 2010-12-31；"
    "纳斯达克 100 价格指数 1986–1999，不含分红。入场、七条规则和 k 都照第 0 步写死。"
    "只看规则判出场的笔，信号日是判断日（下一个收盘才卖）。"
    "最大跌幅是之后 20 个收盘里最低收盘相对信号日收盘的跌幅，没跌算 0；"
    "最大回撤是信号日起这 21 个收盘里从高点到之后低点的最大跌幅。"
    "三种看法：每笔、信号日去重、簇（隔不到 20 个交易日的并成一簇，每簇只取第一天）。"
    "对照是同一段里每一天，和同 20 日波动档（五档）的每一天；"
    "档内的波动不一定对齐，所以另印信号日和同档的平均 σ20，"
    "和按 σ 缩放的同档对照（每个信号日的 σ20 乘它那一档每天「最大跌幅 ÷ σ20」的平均）。"
    "「20 天后涨跌」「20 天后更高」按笔的版本，价目表已经印过（从卖出日算，差一天）。"
    "买回是入场信号的事，这张表不算错过的反弹。"
)
app = typer.Typer(help="QQQ 出场信号之后 20 天的最大跌幅：只印不判。")


def after(close: Sequence[float], t: int) -> dict[str, float]:
    path = close[t : t + AFTER + 1]
    peak = path[0]
    drawdown = 0.0
    for value in path:
        peak = max(peak, value)
        drawdown = min(drawdown, value / peak - 1.0)
    return {
        "fall": min(min(path[1:]) / close[t] - 1.0, 0.0),
        "drawdown": drawdown,
        "next": close[t + 1] / close[t] - 1.0,
        "r20": close[t + AFTER] / close[t] - 1.0,
    }


def stats(rows: Sequence[Mapping[str, float]]) -> dict[str, int | float | None]:
    keys = (
        "n",
        "fall_mean",
        "fall_median",
        "fall_p05",
        "fall_cvar05",
        "fall_share05",
        "fall_share10",
        "drawdown_mean",
        "drawdown_p05",
        "next_mean",
        "r20_mean",
        "r20_up",
    )
    n = len(rows)
    if not n:
        return {key: 0 if key == "n" else None for key in keys}
    falls = [row["fall"] for row in rows]
    drawdowns = [row["drawdown"] for row in rows]
    return {
        "n": n,
        "fall_mean": math.fsum(falls) / n,
        "fall_median": xr.rank(falls, 0.5),
        "fall_p05": xr.rank(falls, TAIL),
        "fall_cvar05": xr.tail_mean(falls, TAIL),
        "fall_share05": sum(f <= -FALL_LINES[0] for f in falls) / n,
        "fall_share10": sum(f <= -FALL_LINES[1] for f in falls) / n,
        "drawdown_mean": math.fsum(drawdowns) / n,
        "drawdown_p05": xr.rank(drawdowns, TAIL),
        "next_mean": math.fsum(row["next"] for row in rows) / n,
        "r20_mean": math.fsum(row["r20"] for row in rows) / n,
        "r20_up": sum(row["r20"] > 0 for row in rows) / n,
    }


def first_days(days: Sequence[int]) -> list[int]:
    ordered = sorted(set(days))
    return [d for i, d in enumerate(ordered) if i == 0 or d - ordered[i - 1] >= xc.CLUSTER_GAP]


def band_edges(sigmas: Sequence[float]) -> list[float]:
    return [xr.rank(sigmas, i / BANDS) for i in range(1, BANDS)]


def band(sigma: float, edges: Sequence[float]) -> int:
    return sum(sigma > edge for edge in edges)


def mean(values: Sequence[float]) -> float | None:
    return math.fsum(values) / len(values) if values else None


def span_table(
    series: ec.Closes,
    prepared: xc.Prepared,
    entries: Sequence[int],
    exits: Mapping[str, Mapping[int, xc.Exit]],
) -> dict[str, Any]:
    close = series.close
    last = len(close) - AFTER - 1
    sigmas = {t: s for t in range(entries[0], last + 1) if (s := prepared.sigma[t]) is not None}
    base_days = list(sigmas)
    rows = {t: after(close, t) for t in base_days}
    edges = band_edges(list(sigmas.values()))
    band_of = {t: band(s, edges) for t, s in sigmas.items()}

    def describe(days: Sequence[int]) -> dict[str, int | float | None]:
        return {
            **stats([rows[t] for t in days]),
            "sigma_mean": mean([sigmas[t] for t in days]),
            "fall_per_sigma_mean": mean([rows[t]["fall"] / sigmas[t] for t in days]),
        }

    bands = [describe([t for t in base_days if band_of[t] == b]) for b in range(BANDS)]

    def matched(days: Sequence[int]) -> dict[str, float | None]:
        if not days:
            return dict.fromkeys(MATCHED)
        result: dict[str, float | None] = {}
        for key in MATCHED:
            values: list[float] = []
            for d in days:
                reference_key = "fall_per_sigma_mean" if key == "scaled_fall_mean" else key
                value = bands[band_of[d]][reference_key]
                # A chosen day belongs to its own nonempty reference band.
                assert value is not None
                if key == "scaled_fall_mean":
                    # Scale the band's mean fall per sigma by this signal day's own sigma.
                    value *= sigmas[d]
                values.append(value)
            result[key] = mean(values)
        return result

    rules: dict[str, Any] = {}
    for rule in RULES:
        signalled = [
            d
            for e in entries
            if (x := exits[rule][e]).reason == "signal" and (d := x.decision) is not None
        ]
        kept = [d for d in signalled if d in rows]
        days = sorted(set(kept))
        firsts = first_days(days)
        views = {
            view: {
                **stats([rows[d] for d in chosen]),
                "sigma_mean": mean([sigmas[d] for d in chosen]),
                "matched": matched(chosen),
            }
            for view, chosen in zip(VIEWS, (kept, days, firsts), strict=True)
        }
        rules[rule] = {
            "signals": len(signalled),
            "beyond_end": len(signalled) - len(kept),
            "views": views,
            "clusters": [
                {
                    "date": series.dates[d].isoformat(),
                    "band": band_of[d],
                    "sigma": sigmas[d],
                    **rows[d],
                }
                for d in firsts
            ],
        }
    return {
        "first_day": series.dates[base_days[0]].isoformat(),
        "last_day": series.dates[base_days[-1]].isoformat(),
        "any_day": describe(base_days),
        "band_edges": edges,
        "bands": bands,
        "rules": rules,
    }


def qqq_inputs(
    data: BenchData, loaded: LoadedSettings, gate: tr.TrialGate
) -> tuple[ec.Closes, xc.Prepared, Sequence[int], dict[str, dict[int, xc.Exit]], dict[str, Any]]:
    window = pt.market_window(loaded, gate, xc.MARKET)
    if window.end is None:
        raise ValueError("开发窗口必须有结束日期。")
    series = ec.closes(data.returns["qqq"], window.end)
    first_in = next(
        (p for p, day in enumerate(series.dates) if day >= window.start), len(series.dates)
    )
    prepared = xc.prepare(series.dates, series.close)
    entries = xc.entry_range(prepared, first_in)
    k = xc.scale(prepared, entries)
    exits = {
        rule: {e: xc.exit_for(rule, prepared, e, k if k is not None else math.nan) for e in entries}
        for rule in RULES
    }
    check = {
        "window_id": window.window_id,
        "start": window.start.isoformat(),
        "end": window.end.isoformat(),
        **xr.check_step0(series, entries, k, exits),
    }
    return series, prepared, entries, exits, check


def run_table(
    data: BenchData,
    loaded: LoadedSettings,
    gate: tr.TrialGate,
    levels: Sequence[tuple[date, float]],
    source: Mapping[str, Any],
    window: Mapping[str, Any],
) -> dict[str, Any]:
    series, prepared, entries, exits, check = qqq_inputs(data, loaded, gate)
    n_series, flat = en.prices(levels)
    n_prepared = xc.prepare(n_series.dates, n_series.close)
    n_entries = xc.entry_range(n_prepared, 0)
    n_check = xn.check_inputs(n_series, flat, window, n_entries)
    result: dict[str, Any] = {
        "run_id": RUN_ID,
        "owner_decision": OWNER_DECISION,
        "source": {"qqq": {"snapshot_id": data.source.get("snapshot_id")}, "ndx": dict(source)},
        "rule": {
            "k": xr.STEP0_K,
            "after": AFTER,
            "tail": TAIL,
            "fall_lines": list(FALL_LINES),
            "bands": BANDS,
            "cluster_gap": xc.CLUSTER_GAP,
            "signal_day": "decision",
            "sell": "next_close",
        },
        "check": {"qqq": check, "ndx": n_check},
        "complete": False,
    }
    if not check["ok"] or not n_check["ok"]:
        return result
    n_exits = {
        rule: {e: xc.exit_for(rule, n_prepared, e, xr.STEP0_K) for e in n_entries} for rule in RULES
    }
    result["spans"] = {
        "qqq": span_table(series, prepared, entries, exits),
        "ndx": span_table(n_series, n_prepared, n_entries, n_exits),
    }
    result["complete"] = True
    return result


def _line(cells: Sequence[str]) -> str:
    return "| " + " | ".join(cells) + " |"


def sigma_text(value: float | None) -> str:
    return "—" if value is None else f"{100.0 * value:.2f}%"


def ratio_text(value: float | None) -> str:
    return "—" if value is None else f"{value:+.2f}"


def render_markdown(result: Mapping[str, Any]) -> str:
    lines = ["# QQQ 出场信号之后 20 天的最大跌幅（TRADING-2565-113）", "", NOTE, ""]
    headers = [
        "规则",
        "看法",
        "个数",
        "平均最大跌幅",
        "任一天",
        "同波动档",
        "同档按 σ 缩放",
        "信号日 σ20",
        "同档 σ20",
        "中位",
        "最差 5%",
        "CVaR5",
        "跌过 5%",
        "跌过 10%",
        "跌过 10% 同波动档",
        "平均最大回撤",
        "次日涨跌",
        "20 天后涨跌",
        "同档 20 天后涨跌",
        "20 天后更高",
    ]
    for span in SPANS:
        one = result["spans"][span]
        base = one["any_day"]
        lines += [
            f"## {SPAN_TEXT[span]}",
            "",
            f"任一天：{one['first_day']}..{one['last_day']}，{base['n']} 天，"
            f"平均最大跌幅 {xr.pct(base['fall_mean'])}，中位 {xr.pct(base['fall_median'])}，"
            f"最差 5% {xr.pct(base['fall_p05'])}，跌过 5% {xr.share(base['fall_share05'])}，"
            f"跌过 10% {xr.share(base['fall_share10'])}，"
            f"平均最大回撤 {xr.pct(base['drawdown_mean'])}，"
            f"20 天后更高 {xr.share(base['r20_up'])}，平均 σ20 {sigma_text(base['sigma_mean'])}，"
            f"最大跌幅 ÷ σ20 平均 {ratio_text(base['fall_per_sigma_mean'])}。",
            "20 日波动五档（从低到高）的平均最大跌幅："
            + "、".join(xr.pct(b["fall_mean"]) for b in one["bands"])
            + "；平均 σ20："
            + "、".join(sigma_text(b["sigma_mean"]) for b in one["bands"])
            + "；最大跌幅 ÷ σ20 平均："
            + "、".join(ratio_text(b["fall_per_sigma_mean"]) for b in one["bands"])
            + "。",
            "",
            _line(headers),
            "|" + "---|" * len(headers),
        ]
        for rule in RULES:
            for view, word in zip(VIEWS, ("每笔", "信号日", "簇"), strict=True):
                row = one["rules"][rule]["views"][view]
                lines.append(
                    _line(
                        [
                            rule,
                            word,
                            str(row["n"]),
                            xr.pct(row["fall_mean"]),
                            xr.pct(base["fall_mean"]),
                            xr.pct(row["matched"]["fall_mean"]),
                            xr.pct(row["matched"]["scaled_fall_mean"]),
                            sigma_text(row["sigma_mean"]),
                            sigma_text(row["matched"]["sigma_mean"]),
                            xr.pct(row["fall_median"]),
                            xr.pct(row["fall_p05"]),
                            xr.pct(row["fall_cvar05"]),
                            xr.share(row["fall_share05"]),
                            xr.share(row["fall_share10"]),
                            xr.share(row["matched"]["fall_share10"]),
                            xr.pct(row["drawdown_mean"]),
                            xr.pct(row["next_mean"]),
                            xr.pct(row["r20_mean"]),
                            xr.pct(row["matched"]["r20_mean"]),
                            xr.share(row["r20_up"]),
                        ]
                    )
                )
        counts = "、".join(
            f"{rule} {one['rules'][rule]['signals']}（超出 {one['rules'][rule]['beyond_end']}）"
            for rule in RULES
        )
        lines += ["", f"信号数（含 20 天后超出数据末尾、没算的）：{counts}。", ""]
    return "\n".join(lines).rstrip() + "\n"


def summary_line(result: Mapping[str, Any], out: Path) -> str:
    spans: list[str] = []
    for span in SPANS:
        one = result["spans"][span]
        clusters = "、".join(
            f"{rule} {xr.pct(one['rules'][rule]['views']['clusters']['fall_mean'])}"
            for rule in RULES
        )
        spans.append(
            f"{SPAN_TEXT[span]} 任一天 {xr.pct(one['any_day']['fall_mean'])}，按簇 {clusters}"
        )
    return "exit_rules_after: 信号后 20 天平均最大跌幅：" + "；".join(spans) + f"；结果目录 {out}"


def write_result(result: Mapping[str, Any], out: Path) -> None:
    text = json.dumps(result, indent=2, ensure_ascii=False, allow_nan=False) + "\n"
    out.mkdir(parents=True, exist_ok=True)
    out.joinpath(RESULT_JSON).write_text(text, encoding="utf-8")
    if result["complete"]:
        out.joinpath(RESULT_MD).write_text(render_markdown(result), encoding="utf-8")


@app.callback()
def main() -> None:
    """Print post-signal falls without investment conclusions."""


@app.command("run")
def run_command(
    out: Annotated[Path, typer.Option(help="跌幅表结果目录，已有结果拒绝覆盖。")],
    root: Annotated[Path, typer.Option(help="长历史快照根目录。")] = qlh.DEFAULT_SNAPSHOT_ROOT,
) -> None:
    if any(out.joinpath(name).exists() for name in (RESULT_JSON, RESULT_MD)):
        typer.echo("exit_rules_after: 已有结果，拒绝覆盖。")
        raise typer.Exit(code=1)
    try:
        data, gate, loaded = xc.load_data(root)
        levels, source, window = en.load_data(root)
        result = run_table(data, loaded, gate, levels, source, window)
    except (OSError, ValueError, KeyError, tr.TrialError) as exc:
        lines = str(exc).splitlines()
        message = lines[0] if lines else ""
        typer.echo(f"exit_rules_after: 数据载入失败：{message}")
        raise typer.Exit(code=1) from None
    write_result(result, out)
    if not result["complete"]:
        typer.echo("exit_rules_after: 输入检查 FAIL，没有算跌幅")
        raise typer.Exit(code=1)
    typer.echo(summary_line(result, out))


if __name__ == "__main__":
    app()
