"""Command line for the research bench. Output is summaries only; no rows of data are printed.

python -m ai_trading_system.research_bench benchmarks [--snapshot ID] [--qqq-series proxy]
python -m ai_trading_system.research_bench run QUESTION --strategy ID [--set KEY=VALUE ...]
    [--falsify]
python -m ai_trading_system.research_bench validate QUESTION
python -m ai_trading_system.research_bench consistency QUESTION
python -m ai_trading_system.research_bench ledger [QUESTION]
python -m ai_trading_system.research_bench report QUESTION [--run RUN_ID ...]
python -m ai_trading_system.research_bench screen SIGNAL_ID --signal-snapshot ID
    [--bench-snapshot ID] [--root PATH]
python -m ai_trading_system.research_bench screen-ledger
python -m ai_trading_system.research_bench trial-run TRIAL --config ID --market qqq|us_market
    [--root PATH]
python -m ai_trading_system.research_bench trial-ledger
python -m ai_trading_system.research_bench trial-report TRIAL
python -m ai_trading_system.research_bench trial-segments TRIAL [--check] [--root PATH]

Screens are recorded in research/_screens/ledger/; only development data is evaluated.
Every run is recorded under research/<QUESTION>/ (see ``research_bench.ledger``).
Trial runs are recorded in research/_trials/runs/ (see ``research_bench.trial``).
"""

from __future__ import annotations

import json
import re
import time
from pathlib import Path
from typing import Annotated, Any

import typer

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.core.provenance import git_provenance
from ai_trading_system.research_bench import ledger as lg
from ai_trading_system.research_bench import trial as tr
from ai_trading_system.research_bench.data import (
    BenchData,
    QqqSeries,
    from_snapshot,
    with_signal_sources,
)
from ai_trading_system.research_bench.metrics import cvar_field
from ai_trading_system.research_bench.report import question_report
from ai_trading_system.research_bench.run import BenchCheckError, WindowPermit, benchmark_summaries
from ai_trading_system.research_bench.screen import (
    ScreenCheckError,
    ScreenMarket,
    bh_adjust,
    load_signal,
    screen_signal,
    screen_verdict,
)
from ai_trading_system.research_bench.screen_ledger import (
    ScreenLedgerError,
    append_screen_record,
    check_screen_repeat,
    family_key,
    screen_ledger_summary,
    screen_records,
    verify_screen_ledger,
)
from ai_trading_system.research_bench.screen_settings import load_screen_settings
from ai_trading_system.research_bench.settings import LoadedSettings, WindowRole, load_settings
from ai_trading_system.research_data import qqq_long_history as qlh
from ai_trading_system.research_data import signal_sources

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
    falsify: Annotated[
        bool, typer.Option("--falsify", help="同时跑标准证伪（验证和一致性窗口总是跑）。")
    ] = False,
    root: SnapshotRootOption = qlh.DEFAULT_SNAPSHOT_ROOT,
) -> None:
    """在开发窗口上跑一个预注册策略，并登记这次运行（失败的也登记）。"""

    loaded = load_settings()
    item = _question(loaded, question)
    params = dict(_parse_setting(text) for text in set_ or [])
    data = _data(item, root)
    strategy_id = item.prereg.strategy(strategy).id
    if not _run_one(item, data, strategy_id, "development", params, None, loaded, falsify=falsify):
        raise typer.Exit(code=1)


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


@app.command("report")
def report_command(
    question: QuestionArgument,
    run: Annotated[
        list[str] | None,
        typer.Option("--run", help="只看这些运行号；不给就看每个策略在每个窗口的最新一次。"),
    ] = None,
) -> None:
    """报告第一屏：试验数、折算夏普、和同敞口基准的对照及其区间、剔除、随机择时、成本和推迟、规则。"""

    loaded = load_settings()
    try:
        lines = question_report(lg.research_root(loaded), question, loaded.settings, run or [])
    except (KeyError, OSError, ValueError, lg.LedgerError) as exc:
        typer.echo(f"report: {str(exc).strip(chr(39))}")
        raise typer.Exit(code=1) from None
    for line in lines:
        typer.echo(line)


@app.command("screen")
def screen_command(
    signal_id: Annotated[str, typer.Argument(help="信号模块 id。")],
    signal_snapshot: Annotated[str, typer.Option(help="已检查的信号来源快照 ID。")],
    bench_snapshot: Annotated[str, typer.Option(help="长历史快照 ID，默认 LATEST。")] = "",
    root: SnapshotRootOption = qlh.DEFAULT_SNAPSHOT_ROOT,
) -> None:
    """只在开发窗口筛一个信号，检查通过才加入全局家族；不构成投资批准。"""
    try:
        loaded_settings = load_screen_settings()
        settings = loaded_settings.settings.screen
        loaded = load_signal(signal_id)
        records = screen_records(PROJECT_ROOT)
        bench_directory = qlh.resolve_snapshot_dir(root, bench_snapshot or None)
        body: dict[str, Any] = {
            "signal_id": signal_id,
            "module_sha256": loaded.module_sha256,
            "spec_sha256": loaded.spec_sha256,
            "params": loaded.spec.params,
            "data": {
                "signal_snapshot_id": signal_snapshot,
                "bench_snapshot_id": bench_directory.name,
            },
            "settings_sha256": loaded_settings.sha256,
        }
        check_screen_repeat(records, body)
        snapshot = signal_sources.load_snapshot(PROJECT_ROOT, signal_snapshot)
        bench = from_snapshot(bench_directory, qqq_series=settings.qqq_series)
        markets = {"qqq": ScreenMarket("qqq", bench.inputs["ndx_return"], settings.qqq_eval_start)}
        factors = snapshot.frames["ff_factors_daily"]
        markets["us_market"] = ScreenMarket(
            "us_market", factors["Mkt-RF"] + factors["RF"], settings.long_eval_start
        )
        industries = snapshot.frames["ff_49_industries_daily"]
        for name in industries.columns:
            market_id = "industry_" + re.sub(r"[^a-z0-9]", "_", str(name).lower())
            if market_id in markets:
                raise ScreenCheckError([f"duplicate industry id {market_id}"])
            markets[market_id] = ScreenMarket(market_id, industries[name], settings.long_eval_start)
        lags = {spec.source_id: spec.known_lag_trading_days for spec in snapshot.specs}
        results = screen_signal(loaded, markets, snapshot.frames, lags, settings)
        latest = {family_key(record): record for record in records}
        # Reruns on different data replace this member's latest result, not its family identity.
        latest[family_key(body)] = {"primary_p_value": results["primary_p_value"]}
        tested = {
            key: value["primary_p_value"]
            for key, value in latest.items()
            if value["primary_p_value"] is not None
        }
        adjusted = dict(zip(tested, bh_adjust(list(tested.values())), strict=True)).get(
            family_key(body)
        )
        verdict = screen_verdict(
            **results["verdict_inputs"], primary_adjusted_p=adjusted, settings=settings
        )
        provenance = git_provenance(PROJECT_ROOT)
        record = append_screen_record(
            PROJECT_ROOT,
            {
                **body,
                "markets": sorted(results["markets"]),
                "results": results,
                "code": {"commit": provenance["commit"], "modified": provenance["code_modified"]},
                "primary_p_value": results["primary_p_value"],
                "verdict": verdict,
                "family_size": len(latest),
            },
        )
    except ScreenCheckError as exc:
        typer.echo("筛选检查未通过，不记账：" + "; ".join(exc.problems))
        raise typer.Exit(code=1) from None
    except (OSError, ValueError, KeyError) as exc:
        typer.echo(f"筛选拒绝，不记账：{exc}")
        raise typer.Exit(code=1) from None
    typer.echo(
        f"记录 {record['screen_id']}；信号 {signal_id}；"
        f"数据 {signal_snapshot} / {bench_directory.name}"
    )
    for market_id in dict.fromkeys(("qqq", settings.primary_market)):
        if market_id not in results["markets"]:
            continue
        row = results["markets"][market_id]["horizons"][str(settings.primary_horizon)]
        typer.echo(
            f"  {market_id} h={settings.primary_horizon}：天数 {row['days']}；"
            f"n_eff {_num(row['n_eff'])}；mde {_num(row['mde'])}；ic {_num(row['ic'])}；"
            f"区间 [{_num(row['ic_low'])}, {_num(row['ic_high'])}]；"
            f"p_value {_num(row['p_value'])}"
        )
    typer.echo(
        f"复现广度 {_num(results['breadth'])}；方向一致 "
        f"{results['direction_matches']}/{results['direction_tested']}；"
        f"基准最大相关 {_num(results['baseline_max_abs_corr'])}；"
        f"十年剔除稳定 {results['verdict_inputs']['leave_out_stable']}"
    )
    typer.echo(
        f"家族大小 {len(latest)}；调整后 p {_num(adjusted)}；"
        f"裁决 {_verdict_text(verdict)} ({verdict})"
    )


@app.command("screen-ledger")
def screen_ledger_command() -> None:
    """只读全局筛选账，以当前家族重算 BH 和裁决；不读数据快照。"""
    problems = verify_screen_ledger(PROJECT_ROOT)
    typer.echo("筛选账链状态：" + ("BROKEN" if problems else "PASS"))
    if problems:
        for problem in problems:
            typer.echo(f"  问题：{problem}")
        raise typer.Exit(code=1)
    try:
        summary = screen_ledger_summary(PROJECT_ROOT, load_screen_settings().settings.screen)
    except ScreenLedgerError as exc:
        typer.echo(f"筛选账拒绝：{exc}")
        raise typer.Exit(code=1) from None
    typer.echo(f"家族大小 {summary['family_size']}；参与 BH {summary['tested']}")
    for member in summary["members"]:
        typer.echo(
            f"  {member['signal_id']} #{member['number']}；"
            f"p {_num(member['primary_p_value'])}；调整后 p {_num(member['adjusted_p'])}；"
            f"记录时 {_verdict_text(member['verdict_at_record'])}；"
            f"当前 {_verdict_text(member['verdict'])}"
        )


@app.command("trial-run")
def trial_run_command(
    trial_id: Annotated[str, typer.Argument(help="试跑编号，例如 T0001。")],
    config: Annotated[str, typer.Option(help="试跑计划的配置 id。")],
    market: Annotated[str, typer.Option(help="市场：qqq 或 us_market。")],
    root: SnapshotRootOption = qlh.DEFAULT_SNAPSHOT_ROOT,
) -> None:
    """在开发窗口试跑一个配置和市场，登记结果及门的判定。"""
    try:
        loaded = load_settings()
        root_dir = lg.research_root(loaded)
        gate, plan = tr.load_trial(root_dir, trial_id)
        if market not in tr.MARKETS:
            raise tr.TrialError(f"market must be one of {list(tr.MARKETS)}")
        data = tr.market_data(market, gate, plan, snapshot_root=root, project_root=PROJECT_ROOT)
        record = tr.run_trial(root_dir, trial_id, config, market, data, loaded=loaded)
    except tr.TrialError as exc:
        typer.echo(f"试跑拒绝，不记账：{exc}")
        raise typer.Exit(code=1) from None
    except (OSError, ValueError, KeyError) as exc:
        typer.echo(f"试跑拒绝，不记账：{str(exc).splitlines()[0]}")
        raise typer.Exit(code=1) from None
    text = f"记录 {record['run_id']}；{trial_id} {config} {market}；状态 {record['status']}"
    if record["status"] == "ok":
        rows = record["gate"]
        text += f"；门 {sum(row['holds'] for row in rows)}/{len(rows)} 条成立"
        failed = [row["metric"] for row in rows if not row["holds"]]
        if failed:
            text += f"；没过：{', '.join(failed)}"
    elif record["status"] == "check_failed":
        text += f"；问题：{'; '.join(record['problems'])}"
    elif record["status"] == "error":
        text += f"；错误：{record['error']}"
    typer.echo(text)
    if record["status"] != "ok":
        raise typer.Exit(code=1)


@app.command("trial-ledger")
def trial_ledger_command() -> None:
    """检查试跑账链，列出主配置判定和包含试跑配置的全局试验数。"""
    loaded = load_settings()
    summary = tr.trial_ledger_summary(lg.research_root(loaded))
    typer.echo("试跑账链状态：" + ("BROKEN" if summary["problems"] else "PASS"))
    for problem in summary["problems"]:
        typer.echo(f"  问题：{problem}")
    typer.echo(
        f"全局总试验数 {summary['global_trials']}（各题开发试验 {summary['question_trials']}，"
        f"试跑配置 {summary['trial_configs']}）"
    )
    for trial_id, item in summary["trials"].items():
        typer.echo(
            f"{trial_id}：配置 {item['configs']}/{item['max_configs']}；"
            f"运行 {len(item['runs'])} 条；主配置 {item['primary']}："
            f"{tr.VERDICT_NAMES[item['verdict']]}"
        )
        for run in item["runs"]:
            typer.echo(
                f"  {run['run_id']} {run['config']} {run['market']} {run['status']} "
                f"门 {run['rules_held']}/{run['rules']}"
            )
    if summary["problems"]:
        raise typer.Exit(code=1)


@app.command("trial-report")
def trial_report_command(
    trial_id: Annotated[str, typer.Argument(help="试跑编号，例如 T0001。")],
) -> None:
    """从已登记的汇总生成试跑候选包的第一页，不读取数据快照。"""
    try:
        loaded = load_settings()
        lines = tr.trial_report(lg.research_root(loaded), trial_id, loaded)
    except (tr.TrialError, OSError, ValueError, KeyError) as exc:
        typer.echo(f"试跑报告拒绝：{exc}")
        raise typer.Exit(code=1) from None
    for line in lines:
        typer.echo(line)


@app.command("trial-segments")
def trial_segments_command(
    trial_id: Annotated[str, typer.Argument(help="数据用哪份试跑计划的快照，例如 T0007。")],
    check: Annotated[
        bool, typer.Option("--check", help="和门里的段清单核对；不一致或没有清单时退出码 1。")
    ] = False,
    root: SnapshotRootOption = qlh.DEFAULT_SNAPSHOT_ROOT,
) -> None:
    """按写死的规则在各市场的开发窗口重算慢熊、快跌和长牛段，只打印段的起止、月数和深度。"""
    try:
        loaded = load_settings()
        root_dir = lg.research_root(loaded)
        gate, plan = tr.load_trial(root_dir, trial_id)
        datasets = {
            market: tr.market_data(
                market, gate, plan, snapshot_root=root, project_root=PROJECT_ROOT
            )
            for market in tr.SEGMENT_MARKETS
        }
        rows = tr.segment_check(gate, plan, datasets, loaded)
    except (tr.TrialError, OSError, ValueError, KeyError) as exc:
        typer.echo(f"段清单拒绝：{str(exc).splitlines()[0]}")
        raise typer.Exit(code=1) from None
    typer.echo(f"试跑段清单（只读，不进判定）：数据为 {trial_id} 的快照")
    kinds = {"slow_bear": "慢熊", "fast_drop": "快跌", "long_bull": "长牛"}
    consistent = True
    for row in rows:
        counts = {kind: sum(s["kind"] == kind for s in row["segments"]) for kind in kinds}
        typer.echo(
            f"{tr.MARKET_NAMES[row['market']]} {row['window_id']} {row['start']}..{row['end']}："
            f"慢熊 {counts['slow_bear']} 段，快跌 {counts['fast_drop']} 段，"
            f"长牛 {counts['long_bull']} 段"
        )
        for s in row["segments"]:
            typer.echo(
                f"  {s['id']} {kinds[s['kind']]} {s['start']}..{s['end']} "
                f"{s['months']} 个月 {s['change']:+.1%}"
            )
        if not row["listed"]:
            status = "没有"
            consistent = False
        elif row["problems"]:
            status = "不一致：" + "; ".join(row["problems"])
            consistent = False
        else:
            status = "一致"
        typer.echo(f"  门里的清单：{status}")
    typer.echo("核对：" + ("一致" if consistent else "不一致"))
    if check and not consistent:
        raise typer.Exit(code=1)


def _verdict_text(verdict: str | None) -> str:
    return {"candidate": "候选", "eliminated": "淘汰", "inconclusive": "证据不足"}.get(
        verdict or "", "无裁决输入"
    )


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
    *,
    falsify: bool = False,
) -> bool:
    try:
        recorded = lg.run_and_record(
            item,
            data,
            strategy_id,
            role=role,
            params=params,
            permit=permit,
            loaded=loaded,
            falsify=falsify,
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
    falsification = record["result"].get("falsification")
    if falsification:
        low, high = falsification["ci_low"], falsification["ci_high"]
        typer.echo(
            f"  falsification: maxDD CI {_pct(low['max_drawdown'])}..{_pct(high['max_drawdown'])}"
            f" worst year {_pct(falsification['leave_year_min']['max_drawdown'])}"
            f" placebo {_num(falsification['placebo_rank']['max_drawdown'])};"
            f" geo/yr CI {_pct(low['geometric_annual_return'])}"
            f"..{_pct(high['geometric_annual_return'])}"
            f" placebo {_num(falsification['placebo_rank']['geometric_annual_return'])}"
            f" (report {item.question_id} for the rest)"
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
    data = from_snapshot(
        qlh.resolve_snapshot_dir(root, wanted.snapshot_id), qqq_series=wanted.qqq_series
    )
    if wanted.signal_snapshot_id is None:
        return data
    snapshot = signal_sources.load_snapshot(PROJECT_ROOT, wanted.signal_snapshot_id)
    lags = {spec.source_id: spec.known_lag_trading_days for spec in snapshot.specs}
    return with_signal_sources(data, snapshot.frames, lags, snapshot_id=wanted.signal_snapshot_id)


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
