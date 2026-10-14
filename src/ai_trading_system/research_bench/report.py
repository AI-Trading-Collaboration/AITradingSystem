"""The first screen of a question's report (TRADING-2565 S3c): what a reader must see before any
return number.

For every chosen run: the trial count against the cap and across all questions, the deflated
Sharpe ratio, the risk-first comparison with the matched constant-exposure benchmark together with
its bootstrap interval, worst leave-one-out values and placebo rank, the cost and delay reruns, and
the preregistered rules. Built from the run records only, so it shows what was recorded and nothing
else.
"""

from __future__ import annotations

import math
from pathlib import Path
from typing import Any

from ai_trading_system.research_bench.ledger import ledger_summary, run_records
from ai_trading_system.research_bench.metrics import cvar_field
from ai_trading_system.research_bench.prereg import PREREG_FILE, load_prereg
from ai_trading_system.research_bench.settings import BenchSettings

# Shown as numbers, not percentages.
_PLAIN_FIELDS = frozenset({"sharpe_vs_cash"})
# Fields where a lower value is better: their worst leave-one-out value is the maximum.
_LOWER_IS_BETTER = frozenset({"annual_volatility"})


def table_fields(settings: BenchSettings) -> tuple[str, ...]:
    """Risk first: drawdown, the left tail, compounding after a bad stretch, then return."""

    tails = sorted(settings.cvar_levels, reverse=True)
    return (
        "max_drawdown",
        *(f"{cvar_field(level)}_relative_improvement" for level in tails),
        "rolling_geometric_return_low",
        "geometric_annual_return",
        "sharpe_vs_cash",
        "annual_volatility",
    )


def chosen_runs(records: list[dict[str, Any]], run_ids: list[str]) -> list[dict[str, Any]]:
    """The named runs, or else the latest successful run of each strategy in each window role."""

    ok = [record for record in records if record.get("status") == "ok"]
    if run_ids:
        found = {record["run_id"]: record for record in ok}
        missing = [run_id for run_id in run_ids if run_id not in found]
        if missing:
            raise KeyError(f"no successful run {', '.join(missing)}")
        return [found[run_id] for run_id in run_ids]
    latest: dict[tuple[str, str], dict[str, Any]] = {}
    for record in ok:
        latest[(record["strategy"]["id"], record["window"]["role"])] = record
    return sorted(latest.values(), key=lambda record: int(record["seq"]))


def question_report(
    root_dir: Path, question_id: str, settings: BenchSettings, run_ids: list[str] | None = None
) -> list[str]:
    summary = ledger_summary(root_dir)
    item = summary["questions"].get(question_id)
    if item is None:
        raise KeyError(f"no question {question_id}")
    directory = root_dir.joinpath(question_id)
    prereg = load_prereg(directory.joinpath(PREREG_FILE))
    reads = ", ".join(f"{read_id} {role}" for read_id, role in item["reads"]) or "-"
    lines = [
        f"{question_id}: {prereg.question.strip().splitlines()[0]}",
        f"snapshot {prereg.data.snapshot_id}; qqq {prereg.data.qqq_series};"
        f" owner decision {prereg.owner_decision or '-'}",
        f"trials {item['trials']}/{item['trial_cap']} here, {summary['global_trials']} over all"
        f" questions; reads {reads}; chain {'ok' if not item['problems'] else 'BROKEN'}",
    ]
    if item["problems"]:
        return [*lines, *(f"  problem: {problem}" for problem in item["problems"])]
    lines_by_run = {run["run_id"]: run for run in item["runs"]}
    records = run_records(directory)
    for record in chosen_runs(records, run_ids or []):
        lines.extend(_run_section(record, lines_by_run[record["run_id"]], summary, settings))
    if len(lines) == 3:
        lines.append("no successful run yet")
    return lines


def _run_section(
    record: dict[str, Any], line: dict[str, Any], summary: dict[str, Any], settings: BenchSettings
) -> list[str]:
    result = record["result"]
    strategy = result["strategy"]
    window = record["window"]
    read = f" (read {record['window_read_id']})" if record.get("window_read_id") else ""
    trial = (record.get("trial") or {}).get("number")
    floor = record.get("sample_floor_met")
    out = [
        "",
        f"{record['run_id']} {record['strategy']['id']} {window['role']}{read}"
        f" {strategy['first_return_date']}..{strategy['last_return_date']},"
        f" {strategy['return_days']} days{'' if trial is None else f', trial {trial}'}"
        f"{'' if floor is None else ('; sample floor met' if floor else '; sample floor NOT met')}",
        f"  deflated Sharpe {_num(line['deflated_sharpe'])} against"
        f" {summary['global_trials']} trials; Sharpe {_num(line['sharpe_annual'])};"
        f" average exposure {strategy['average_exposure']:.2f}",
    ]
    comparison = result["comparison"]
    falsification = result.get("falsification")
    if not falsification:
        out.append("  vs matched constant exposure (no falsification; run with --falsify):")
        out.extend(
            f"    {field:<36}{_fmt(field, comparison.get(field)):>9}"
            for field in table_fields(settings)
        )
    else:
        confidence = falsification["settings"]["confidence"]
        out.append(
            f"  vs matched constant exposure{'':<8}{'diff':>9}{'CI low':>9}{'CI high':>9}"
            f"{'year worst':>11}{'episode worst':>14}{'placebo':>9}"
        )
        for field in table_fields(settings):
            worst = "max" if field in _LOWER_IS_BETTER else "min"
            out.append(
                f"    {field:<36}{_fmt(field, comparison.get(field)):>9}"
                f"{_fmt(field, falsification['ci_low'].get(field)):>9}"
                f"{_fmt(field, falsification['ci_high'].get(field)):>9}"
                f"{_fmt(field, falsification[f'leave_year_{worst}'].get(field)):>11}"
                f"{_fmt(field, falsification[f'leave_episode_{worst}'].get(field)):>14}"
                f"{_num(falsification['placebo_rank'].get(field), 2):>9}"
            )
        out.append(
            f"  CI {confidence:g} stationary bootstrap; CI, leave-out and placebo annualize by"
            " trading days"
        )
        out.append("  " + _reruns(falsification, "cost_", "{} bps"))
        out.append("  " + _reruns(falsification, "delay_", "+{}d"))
    rules = record.get("rules")
    if rules:
        held = sum(1 for rule in rules if rule["holds"])
        out.append(f"  rules {held}/{len(rules)} hold:")
        out.extend(
            f"    {rule['metric']} {rule['op']} {rule['value']:g}: observed"
            f" {_num(rule['observed'])} {'holds' if rule['holds'] else 'FAILS'}"
            for rule in rules
        )
    out.extend(_events_section(result.get("events")))
    return out


def _events_section(events: dict[str, Any] | None) -> list[str]:
    if events is None:
        return ["  减仓事件：无（S4c 之前的运行）"]
    lines = [
        "  减仓事件（S4c；规则见 config/research/research_screen.yaml）：",
        f"    事件数 {events['count']}；每年事件数 {events['per_year']:.2f}；"
        f"短事件数 {events['short_count']}",
    ]
    for name, label in (("protective", "保护性减仓"), ("false_alarm", "误报"), ("neutral", "中性")):
        outcome = events["outcomes"][name]
        lines.append(
            f"    {label}（{name}）：{outcome['count']} 次；QQQ 累计收益"
            f" {_fmt('qqq_return', outcome['qqq_return'])}"
        )
    for name, episode in events["episodes"].items():
        exit_lag = episode["exit_lag_days"]
        reentry_lag = episode["reentry_lag_days"]
        lines.append(
            f"    {name}：{episode['days']} 个交易日；减仓日占比"
            f" {episode['derisked_share']:.1%}；漏掉的跌幅"
            f" {_fmt('qqq_return', episode['missed_loss'])}；"
            f"退出晚 {'无减仓' if exit_lag is None else str(exit_lag) + ' 天'}；"
            f"回补晚 {'截至数据结束未回补' if reentry_lag is None else str(reentry_lag) + ' 天'}"
        )
    return lines


def _reruns(falsification: dict[str, Any], prefix: str, unit: str) -> str:
    parts = []
    for key, value in falsification.items():
        if not key.startswith(prefix) or not isinstance(value, dict):
            continue
        number = key.removeprefix(prefix).replace("p", ".")
        parts.append(
            (
                float(number),
                f"{unit.format(number)}: maxDD {_fmt('max_drawdown', value.get('max_drawdown'))}"
                f" geo/yr {_fmt('geometric_annual_return', value.get('geometric_annual_return'))}",
            )
        )
    text = "; ".join(part for _, part in sorted(parts)) or "-"
    return f"{prefix.rstrip('_')} reruns vs matched: {text}"


def _fmt(field: str, value: Any) -> str:
    if not isinstance(value, int | float) or not math.isfinite(value):
        return "-"
    if field in _PLAIN_FIELDS:
        return f"{value:+.2f}"
    return f"{value * 100:+.1f}%"


def _num(value: Any, digits: int = 3) -> str:
    if not isinstance(value, int | float) or not math.isfinite(value):
        return "-"
    return f"{value:.{digits}f}"
