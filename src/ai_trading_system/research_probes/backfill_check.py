"""Backfill probe: closed questions' fixed rules on US market development data, 1929–2010.

Measures the live trial gate, comparisons and derisking episodes for six preregistered
configurations; only rv21_weekly overrides its source's rebalance schedule. Uses the
bench's simulation, falsification and rule evaluation unchanged. T0004's US primary
record is a read-only reference, not rerun. This is a market proxy, not QQQ evidence.
Does not read validation/consistency windows, write research ledgers, count as a trial,
reopen families, change failure tags, or authorize production or broker actions.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Any

import pandas as pd
import typer

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.core.provenance import git_provenance
from ai_trading_system.research_bench import ledger as lg
from ai_trading_system.research_bench import trial as tr
from ai_trading_system.research_bench.checks import strategy_targets
from ai_trading_system.research_bench.data import BenchData
from ai_trading_system.research_bench.prereg import StrategyEntry, evaluate_rules, load_prereg
from ai_trading_system.research_bench.run import BenchCheckError, RunSpec, run_strategy
from ai_trading_system.research_bench.settings import LoadedSettings, load_settings, resolve_window
from ai_trading_system.research_bench.signal_strategy import signal_exposure
from ai_trading_system.research_data import qqq_long_history as qlh
from ai_trading_system.research_probes import gate_power as gp

RUN_ID = "TRADING-2567-33"
OWNER_DECISION = "owner_decision:TRADING-2567:2026-10-20:backfill_check"
DATA_TRIAL = "T0004"
MARKET: tr.Market = "us_market"
RESERVE = "cash"
JOURNAL_FILE = "backfill_rows.jsonl"
RESULT_JSON = "backfill_result.json"
RESULT_MD = "backfill_result.md"

app = typer.Typer(help="回填检验：只读美股大盘开发窗口，不写研究账本。")


@dataclass(frozen=True)
class Config:
    id: str
    question: str
    strategy: str
    decides: bool
    schedule: str | None = None


CONFIGS = (
    Config("fast_slow", "QQQ-Q3", "fast_slow", True),
    Config("trend4", "QQQ-Q2", "trend4", True),
    Config("slow_only", "QQQ-Q3", "slow_only", False),
    Config("rv21", "QQQ-Q4", "rv21", True),
    Config("fast_slow_weekly", "QQQ-Q3", "fast_slow_weekly", False),
    Config("rv21_weekly", "QQQ-Q4", "rv21", False, "week_end"),
)


def source_entry(root_dir: Path, config: Config) -> StrategyEntry:
    entry = load_prereg(root_dir / config.question / "prereg.yaml").strategy(config.strategy)
    if entry.function != tr.STRATEGY_FUNCTION:
        raise ValueError(f"{config.id}: {entry.function} is not signal_exposure")
    if entry.delay_days != 0 or entry.leveraged_benchmarks:
        raise ValueError(f"{config.id}: delay_days must be 0 and leveraged_benchmarks false")
    if config.schedule is not None:
        rebalance = type(entry.rebalance).model_validate(
            {**entry.rebalance.model_dump(), "schedule": config.schedule}
        )
        entry = entry.model_copy(update={"rebalance": rebalance})
    return entry


def run_spec(config: Config, entry: StrategyEntry, gate: tr.TrialGate) -> RunSpec:
    return RunSpec(
        question_type=gate.markets[MARKET].question_type_for(RESERVE),
        strategy_id=config.id,
        params=entry.params,
        window_role="development",
        rebalance=entry.rebalance.to_rebalance(),
        delay_days=0,
        leveraged_benchmarks=False,
        reserve=RESERVE,
    )


def _error(exc: Exception) -> str:
    return f"{type(exc).__name__}: {exc}".splitlines()[0]


def check_targets(
    data: BenchData,
    loaded: LoadedSettings,
    gate: tr.TrialGate,
    root_dir: Path,
    configs: Sequence[Config] = CONFIGS,
) -> list[dict[str, Any]]:
    window = resolve_window(loaded, gate.markets[MARKET].question_type_for(RESERVE), "development")
    rows: list[dict[str, Any]] = []
    for config in configs:
        try:
            entry = source_entry(root_dir, config)
            targets = strategy_targets(signal_exposure, data.inputs, entry.params)
            dates = pd.DatetimeIndex(targets.index)
            inside = targets.loc[
                (dates >= pd.Timestamp(window.start))
                & (True if window.end is None else dates <= pd.Timestamp(window.end))
            ]
            valid = targets.dropna()
            missing = int(inside.isna().sum())
            rows.append(
                {
                    "config": config.id,
                    "first_valid": None
                    if valid.empty
                    else pd.Timestamp(valid.index[0]).date().isoformat(),
                    "window_start": window.start.isoformat(),
                    "window_days": len(inside),
                    "missing_days": missing,
                    "ok": len(inside) > 0 and missing == 0,
                }
            )
        except Exception as exc:
            rows.append({"config": config.id, "ok": False, "error": _error(exc)})
    return rows


def _events(value: Mapping[str, Any] | None) -> dict[str, Any]:
    events = value or {}
    outcomes = events.get("outcomes") or {}
    return {
        "count": events.get("count"),
        "outcomes": {
            key: (outcomes.get(key) or {}).get("count")
            for key in ("protective", "false_alarm", "neutral")
        },
        "episodes": events.get("episodes", {}),
    }


def _gate_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return {
        "gate": list(rows),
        "holds": sum(bool(row["holds"]) for row in rows),
        "rules": len(rows),
        "gate_holds": all(row["holds"] for row in rows),
    }


def evaluate(
    config: Config, data: BenchData, loaded: LoadedSettings, gate: tr.TrialGate, root_dir: Path
) -> dict[str, Any]:
    row: dict[str, Any] = {
        "config": config.id,
        "question": config.question,
        "strategy": config.strategy,
        "decides": config.decides,
        "rebalance": None,
    }
    try:
        entry = source_entry(root_dir, config)
        row["rebalance"] = entry.rebalance.model_dump()
        market = gate.markets[MARKET]
        outcome = run_strategy(
            data,
            signal_exposure,
            run_spec(config, entry, gate),
            loaded,
            falsification_confidence=gate.confidence,
            episodes=market.event_episodes,
        ).to_dict()
        rules = evaluate_rules(market.rules, {**(outcome.get("falsification") or {}), **outcome})
        row.update(
            status="ok",
            window=outcome["window"],
            **_gate_summary(rules),
            comparison=outcome["comparison"],
            diagnostics=outcome.get("diagnostics"),
            events=_events(outcome.get("events")),
        )
    except BenchCheckError as exc:
        row.update(status="check_failed", problems=exc.problems)
    except Exception as exc:
        row.update(status="error", error=_error(exc))
    return dict(gp._clean(row))


def reference_row(
    records: Sequence[Mapping[str, Any]], trial_id: str, primary: str
) -> dict[str, Any] | None:
    for record in reversed(records):
        if (
            record.get("trial_id") == trial_id
            and record.get("config") == primary
            and record.get("market") == MARKET
            and record.get("status") == "ok"
        ):
            outcome = record.get("result") or {}
            row = {
                "config": f"{trial_id} {primary}",
                "run_id": record["run_id"],
                **_gate_summary(record.get("gate") or []),
                "comparison": outcome.get("comparison"),
                "events": _events(outcome.get("events")),
            }
            return dict(gp._clean(row))
    return None


def run_probe(
    data: BenchData,
    loaded: LoadedSettings,
    gate: tr.TrialGate,
    root_dir: Path,
    *,
    configs: Sequence[Config] = CONFIGS,
    records: Sequence[Mapping[str, Any]] = (),
    primary: str = "",
    journal: Path | None = None,
    progress: Callable[[str, dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    window = resolve_window(loaded, gate.markets[MARKET].question_type_for(RESERVE), "development")
    head = gp._clean(
        {
            "run_id": RUN_ID,
            "owner_decision": OWNER_DECISION,
            "gate_sha256": lg.chain_digest((root_dir / tr.TRIALS_DIR / "prereg.yaml").read_bytes()),
            "confidence": gate.confidence,
            "market": MARKET,
            "window": {
                "window_id": window.window_id,
                "start": window.start.isoformat(),
                "end": gp._iso(window.end),
            },
            "data_trial": DATA_TRIAL,
            "source": dict(data.source),
            "settings_sha256": loaded.sha256,
            "configs": [config.id for config in configs],
            "provenance": git_provenance(root_dir),
        }
    )
    rows: list[dict[str, Any]] = []
    sink = None if journal is None else journal.open("x", encoding="utf-8")

    def append(value: dict[str, Any]) -> None:
        if sink is not None:
            sink.write(json.dumps(value, ensure_ascii=False, allow_nan=False) + "\n")
            sink.flush()

    try:
        append(head)
        for config in configs:
            row = evaluate(config, data, loaded, gate, root_dir)
            rows.append(row)
            append(row)
            if progress is not None:
                progress(config.id, row)
    finally:
        if sink is not None:
            sink.close()
    return {
        **head,
        "rows": rows,
        "reference": reference_row(records, DATA_TRIAL, primary) if primary else None,
        "complete": all(row["status"] == "ok" for row in rows),
    }


def render_markdown(result: Mapping[str, Any]) -> str:
    window = result["window"]
    lines = [
        f"# 回填检验（{RUN_ID}）",
        "",
        f"- 研究窗口：{window['window_id']}；请求范围 {window['start']}..{window['end']}。",
        f"- 门 sha256：{result['gate_sha256']}；置信度：{gp._pct(result['confidence'])}。",
        f"- 设置 sha256：{result['settings_sha256']}；数据来源：{result['source']}。",
        "- 数据由试跑数据核查路径载入；美股大盘是代理，不是 QQQ，不读取保留窗口。",
        "- 只量回填表现，不改家族标签，不算试验，不授权生产或 broker。",
        "",
        "| 配置 | 来源 | 判 | 左尾 | 左尾区间下限 | 回撤（点） | 几何（点/年） | "
        "25 bps 几何 | 成立 | 全过 | 事件 / 保护 / 误报 |",
        "|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    rows = list(result["rows"])
    if result.get("reference") is not None:
        rows.append(result["reference"])
    metrics = (
        "comparison.cvar_5pct_relative_improvement",
        "ci_low.cvar_5pct_relative_improvement",
        "comparison.max_drawdown",
        "comparison.geometric_annual_return",
        "cost_25.geometric_annual_return",
    )
    for row in rows:
        reference = "run_id" in row
        source = (
            f"试跑记录 {row['run_id']}" if reference else f"{row['question']} {row['strategy']}"
        )
        role = "参照" if reference else ("判" if row["decides"] else "只读")
        prefix = f"| {row['config']} | {source} | {role} |"
        if row.get("status", "ok") != "ok":
            reason = row.get("error") or "；".join(row.get("problems", []))
            reason = str(reason).replace("|", "\\|").replace("\n", " ")
            lines.append(f"{prefix} {reason} | - | - | - | - | - | - | - |")
            continue
        observed = {rule["metric"]: rule.get("observed") for rule in row["gate"]}
        values = " | ".join(gp._pct(observed.get(metric)) for metric in metrics)
        events = row["events"]
        outcomes = events["outcomes"]
        counts = " / ".join(
            str(value) if value is not None else "-"
            for value in (events["count"], outcomes["protective"], outcomes["false_alarm"])
        )
        lines.append(
            f"{prefix} {values} | {row['holds']}/{row['rules']} | "
            f"{'是' if row['gate_holds'] else '否'} | {counts} |"
        )
    episodes = (
        "crash_1929",
        "flash_crash_1962",
        "crash_1987",
        "ltcm_1998",
        "dotcom_bear",
        "global_financial_crisis",
    )
    lines += [
        "",
        "## 快跌段",
        "",
        "每格为减仓天数占比（退出延迟天数）；缺值为 -。",
        "",
        "| 配置 | " + " | ".join(episodes) + " |",
        "|---|" + "---|" * len(episodes),
    ]
    for row in rows:
        details = (row.get("events") or {}).get("episodes") or {}
        cells = []
        for episode in episodes:
            detail = details.get(episode) or {}
            lag = detail.get("exit_lag_days")
            cells.append(
                f"{gp._pct(detail.get('derisked_share'))}（{lag if lag is not None else '-'}）"
            )
        lines.append(f"| {row['config']} | " + " | ".join(cells) + " |")
    lines += ["", "## 实际评估范围", ""]
    for row in result["rows"]:
        evaluated = row.get("window")
        if evaluated:
            lines.append(f"- {row['config']}：{evaluated['start']}..{evaluated['end']}。")
    return "\n".join([*lines, ""])


def write_result(result: Mapping[str, Any], out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    out_dir.joinpath(RESULT_JSON).write_text(
        json.dumps(gp._clean(dict(result)), indent=2, ensure_ascii=False, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    out_dir.joinpath(RESULT_MD).write_text(render_markdown(result), encoding="utf-8")


def load_data(snapshot_root: Path) -> tuple[BenchData, tr.TrialGate, LoadedSettings, Path, str]:
    loaded = load_settings()
    root_dir = lg.research_root(loaded)
    gate, plan = tr.load_trial(root_dir, DATA_TRIAL)
    problems = tr.gate_problems(gate, loaded)
    if problems:
        raise ValueError("; ".join(problems))
    data = tr.market_data(
        MARKET, gate, plan, snapshot_root=snapshot_root, project_root=PROJECT_ROOT
    )
    if (
        data.source.get("market") != MARKET
        or data.source.get("snapshot_id") != plan.data.signal_snapshot_id
    ):
        raise ValueError(f"data are not the us_market of {plan.data.signal_snapshot_id}")
    return data, gate, loaded, root_dir, plan.primary


def _load(root: Path) -> tuple[BenchData, tr.TrialGate, LoadedSettings, Path, str]:
    try:
        return load_data(root)
    except (OSError, ValueError, KeyError, tr.TrialError) as exc:
        typer.echo(f"backfill_check: 数据载入失败：{exc}")
        raise typer.Exit(code=1) from None


def _print_checks(rows: Sequence[Mapping[str, Any]]) -> bool:
    for row in rows:
        if "error" in row:
            typer.echo(f"backfill_check: {row['config']} 算不出：{row['error']}")
        else:
            typer.echo(
                f"backfill_check: {row['config']} 首个有值日 {row['first_valid']}，"
                f"窗口起点 {row['window_start']}，窗口内 {row['window_days']} 天，"
                f"缺 {row['missing_days']} 天，{'可以' if row['ok'] else '不行'}"
            )
    return all(row["ok"] for row in rows)


@app.callback()
def main() -> None:
    """只读开发窗口的回填检验。"""


@app.command("check")
def check_command(
    root: Annotated[Path, typer.Option(help="长历史快照根目录。")] = qlh.DEFAULT_SNAPSHOT_ROOT,
) -> None:
    data, gate, loaded, root_dir, _ = _load(root)
    if not _print_checks(check_targets(data, loaded, gate, root_dir)):
        raise typer.Exit(code=1)


@app.command("run")
def run_command(
    out: Annotated[Path, typer.Option(help="结果目录，保存逐行记录、JSON 和中文报告。")],
    root: Annotated[Path, typer.Option(help="长历史快照根目录。")] = qlh.DEFAULT_SNAPSHOT_ROOT,
) -> None:
    if any(out.joinpath(name).exists() for name in (JOURNAL_FILE, RESULT_JSON)):
        typer.echo("backfill_check: 已有结果，拒绝覆盖。")
        raise typer.Exit(code=1)
    data, gate, loaded, root_dir, primary = _load(root)
    if not _print_checks(check_targets(data, loaded, gate, root_dir)):
        typer.echo("backfill_check: 开跑前检查没过，停下。")
        raise typer.Exit(code=1)

    def progress(config_id: str, row: dict[str, Any]) -> None:
        typer.echo(f"backfill_check: {config_id} {row['status']}")

    out.mkdir(parents=True, exist_ok=True)
    result = run_probe(
        data,
        loaded,
        gate,
        root_dir,
        records=tr.trial_records(root_dir),
        primary=primary,
        journal=out / JOURNAL_FILE,
        progress=progress,
    )
    write_result(result, out)
    typer.echo(f"backfill_check: {'完成' if result['complete'] else '有配置出错'}；结果目录 {out}")


if __name__ == "__main__":
    app()
