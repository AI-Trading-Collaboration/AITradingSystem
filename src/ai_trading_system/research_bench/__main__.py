"""Command line for the research bench. Output is summaries only; no rows of data are printed.

python -m ai_trading_system.research_bench benchmarks [--snapshot ID] [--qqq-series proxy]
python -m ai_trading_system.research_bench run QUESTION --strategy ID [--set KEY=VALUE ...]
python -m ai_trading_system.research_bench validate QUESTION
python -m ai_trading_system.research_bench consistency QUESTION
python -m ai_trading_system.research_bench ledger [QUESTION]

Every run is recorded under research/<QUESTION>/ (see ``research_bench.ledger``).
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Annotated, Any

import typer

from ai_trading_system.research_bench import ledger as lg
from ai_trading_system.research_bench.data import BenchData, QqqSeries, from_snapshot
from ai_trading_system.research_bench.metrics import cvar_field
from ai_trading_system.research_bench.run import BenchCheckError, WindowPermit, benchmark_summaries
from ai_trading_system.research_bench.settings import LoadedSettings, WindowRole, load_settings
from ai_trading_system.research_data import qqq_long_history as qlh

app = typer.Typer(
    help="研究评估台（TRADING-2565、TRADING-2567 S3）：输出只有汇总。", no_args_is_help=True
)


@app.callback()
def main() -> None:
    """研究评估台。"""


@app.command("benchmarks")
def benchmarks_command(
    snapshot: Annotated[str, typer.Option(help="长历史快照 ID，默认 LATEST。")] = "",
    root: Annotated[Path, typer.Option(help="快照根目录。")] = qlh.DEFAULT_SNAPSHOT_ROOT,
    qqq_series: Annotated[
        str, typer.Option(help="QQQ 用哪条序列：proxy（总收益指数减费率）或 actual（真实 QQQ）。")
    ] = "proxy",
) -> None:
    """在开发窗口上跑固定基准（买入持有 QQQ、现金、固定 2 倍和 3 倍），核对数据和评估台。"""

    if qqq_series not in ("proxy", "actual"):
        typer.echo("benchmarks: --qqq-series must be proxy or actual")
        raise typer.Exit(code=1)
    series: QqqSeries = "proxy" if qqq_series == "proxy" else "actual"
    loaded = load_settings()
    data = from_snapshot(qlh.resolve_snapshot_dir(root, snapshot or None), qqq_series=series)
    started = time.perf_counter()
    window, summaries = benchmark_summaries(data, loaded)
    seconds = time.perf_counter() - started
    typer.echo(
        f"snapshot {data.source['snapshot_id']} check {data.source['check_status']}"
        f" warnings {','.join(data.source['check_warnings']) or '-'}; qqq {series}"
    )
    typer.echo(
        f"window {window.window_id} ({window.role}) {window.start}..{window.end or 'latest'};"
        f" cost {loaded.cost_bps:g} bps; {len(summaries)} simulations in {seconds:.2f}s"
    )
    tail = cvar_field(max(loaded.settings.cvar_levels))
    typer.echo(
        f"{'benchmark':<18}{'geo/yr':>9}{'vol':>8}{'maxDD':>9}{tail:>11}"
        f"{'worstM':>9}{'roll3y p10':>12}"
    )
    for name, row in summaries.items():
        typer.echo(
            f"{name:<18}{_pct(row['geometric_annual_return']):>9}{_pct(row['annual_volatility']):>8}"
            f"{_pct(row['max_drawdown']):>9}{_pct(row[tail], 2):>11}{_pct(row['worst_month']):>9}"
            f"{_pct(row['rolling_geometric_return_low']):>12}"
        )


QuestionArgument = Annotated[str, typer.Argument(help="题号，例如 QQQ-Q1（research/<题号>/）。")]
SnapshotRootOption = Annotated[Path, typer.Option("--root", help="快照根目录。")]


@app.command("run")
def run_command(
    question: QuestionArgument,
    strategy: Annotated[str, typer.Option(help="预注册里的策略 id。")],
    set_: Annotated[
        list[str] | None,
        typer.Option("--set", help="开发窗口改参数：KEY=VALUE（VALUE 按 JSON 读），可多次。"),
    ] = None,
    root: SnapshotRootOption = qlh.DEFAULT_SNAPSHOT_ROOT,
) -> None:
    """在开发窗口上跑一个预注册策略，并登记这次运行（失败的也登记）。"""

    loaded = load_settings()
    item = _question(loaded, question)
    params = dict(_parse_setting(text) for text in set_ or [])
    data = _data(item, root)
    _run_one(item, data, item.prereg.strategy(strategy).id, "development", params, None, loaded)


@app.command("validate")
def validate_command(
    question: QuestionArgument, root: SnapshotRootOption = qlh.DEFAULT_SNAPSHOT_ROOT
) -> None:
    """读验证窗口（每题一次）：预注册和策略模块要和 main 上的一致，owner decision 要在 main 上。"""

    _read_window(question, "validation", root)


@app.command("consistency")
def consistency_command(
    question: QuestionArgument, root: SnapshotRootOption = qlh.DEFAULT_SNAPSHOT_ROOT
) -> None:
    """读一致性窗口：只能在验证窗口读过之后。"""

    _read_window(question, "consistency_check", root)


@app.command("ledger")
def ledger_command(
    question: Annotated[str, typer.Argument(help="只看这一题；不给就看全部。")] = "",
) -> None:
    """试验台账：每题和全局的试验数、哈希链检查、每次运行的折算夏普。"""

    loaded = load_settings()
    summary = lg.ledger_summary(lg.research_root(loaded))
    variance = summary["trial_sharpe_variance"]
    typer.echo(
        f"global development trials {summary['global_trials']};"
        f" trial Sharpe variance (daily) {'-' if variance is None else f'{variance:.3g}'}"
    )
    for name, item in summary["questions"].items():
        if question and name != question:
            continue
        reads = ", ".join(f"{read_id} {role}" for read_id, role in item["reads"]) or "-"
        typer.echo(
            f"{name}: trials {item['trials']}/{item['trial_cap']}; runs {len(item['runs'])};"
            f" reads {reads}; chain {'ok' if not item['problems'] else 'BROKEN'}"
        )
        for problem in item["problems"]:
            typer.echo(f"  problem: {problem}")
        for run in item["runs"]:
            typer.echo(
                f"  {run['run_id']} {run['strategy']} {run['role']} {run['status']}"
                f" trial {run['trial'] or '-'} sharpe {_num(run['sharpe_annual'])}"
                f" deflated {_num(run['deflated_sharpe'])}"
            )
    if question and question not in summary["questions"]:
        typer.echo(f"ledger: no question {question}")
        raise typer.Exit(code=1)


def _read_window(question: str, role: WindowRole, root: Path) -> None:
    loaded = load_settings()
    item = _question(loaded, question)
    data = _data(item, root)
    try:
        permit = lg.register_read(item, role, loaded)
    except lg.LedgerError as exc:
        typer.echo(f"{role}: {exc}")
        raise typer.Exit(code=1) from None
    typer.echo(f"read {permit.read_id} registered ({role}): {', '.join(permit.strategies)}")
    failed = False
    for strategy_id in permit.strategies:
        failed |= not _run_one(item, data, strategy_id, role, None, permit, loaded)
    if failed:
        raise typer.Exit(code=1)


def _run_one(
    item: lg.Question,
    data: BenchData,
    strategy_id: str,
    role: WindowRole,
    params: dict[str, Any] | None,
    permit: WindowPermit | None,
    loaded: LoadedSettings,
) -> bool:
    try:
        recorded = lg.run_and_record(
            item, data, strategy_id, role=role, params=params, permit=permit, loaded=loaded
        )
    except lg.TrialCapError as exc:
        typer.echo(f"{strategy_id}: refused, nothing ran: {exc}")
        return False
    except BenchCheckError as exc:
        typer.echo(f"{strategy_id}: checks failed (recorded): {'; '.join(exc.problems)}")
        return False
    except lg.LedgerError as exc:
        typer.echo(f"{strategy_id}: {exc}")
        return False
    record = recorded.record
    summary = record["result"]["strategy"]
    comparison = record["result"]["comparison"]
    trial = record.get("trial") or {}
    typer.echo(
        f"{record['run_id']} {strategy_id} {role}"
        f" trial {trial.get('number', '-')}{' (repeat)' if trial and not trial['new'] else ''}:"
        f" geo/yr {_pct(summary['geometric_annual_return'])}"
        f" maxDD {_pct(summary['max_drawdown'])}"
        f" cvar5 {_pct(summary['cvar_5pct'], 2)}"
        f" avg exposure {summary['average_exposure']:.2f};"
        f" vs matched: maxDD {_pct(comparison['max_drawdown'])}"
        f" cvar5 rel {_pct(comparison['cvar_5pct_relative_improvement'])}"
        f" geo/yr {_pct(comparison['geometric_annual_return'])}"
    )
    for rule in record.get("rules") or []:
        typer.echo(
            f"  rule {rule['metric']} {rule['op']} {rule['value']:g}:"
            f" observed {_num(rule['observed'])} {'holds' if rule['holds'] else 'FAILS'}"
        )
    if role != "development" and not record["sample_floor_met"]:
        typer.echo("  sample floor not met")
    return True


def _question(loaded: LoadedSettings, question: str) -> lg.Question:
    directory = lg.research_root(loaded).joinpath(question)
    try:
        return lg.load_question(directory)
    except (OSError, ValueError) as exc:
        typer.echo(f"{question}: {str(exc).splitlines()[0]}")
        raise typer.Exit(code=1) from None


def _data(item: lg.Question, root: Path) -> BenchData:
    wanted = item.prereg.data
    return from_snapshot(
        qlh.resolve_snapshot_dir(root, wanted.snapshot_id), qqq_series=wanted.qqq_series
    )


def _parse_setting(text: str) -> tuple[str, Any]:
    key, sep, raw = text.partition("=")
    if not sep or not key:
        typer.echo(f"--set expects KEY=VALUE, got {text!r}")
        raise typer.Exit(code=1)
    try:
        return key, json.loads(raw)
    except ValueError:
        return key, raw


def _num(value: Any) -> str:
    return "-" if value is None else f"{float(value):.3f}"


def _pct(value: Any, digits: int = 1) -> str:
    if value is None:
        return "-"
    return f"{float(value) * 100:.{digits}f}%"


if __name__ == "__main__":
    app()
