from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from ai_trading_system.config import (
    DEFAULT_INDUSTRY_CHAIN_CONFIG_PATH,
    DEFAULT_RISK_EVENTS_CONFIG_PATH,
    DEFAULT_SCENARIO_LIBRARY_CONFIG_PATH,
    DEFAULT_WATCHLIST_CONFIG_PATH,
    PROJECT_ROOT,
    load_industry_chain,
    load_risk_events,
    load_watchlist,
)
from ai_trading_system.scenario_library import (
    default_scenario_library_report_path,
    load_scenario_library,
    lookup_scenario,
    render_scenario_lookup,
    validate_scenario_library,
    write_scenario_library_report,
)

console = Console()
scenarios_app = typer.Typer(help="AI 产业链情景压力测试库。", no_args_is_help=True)


@scenarios_app.command("validate")
def validate_scenarios_command(
    input_path: Annotated[
        Path,
        typer.Option(help="scenario library YAML 路径。"),
    ] = DEFAULT_SCENARIO_LIBRARY_CONFIG_PATH,
    industry_chain_path: Annotated[
        Path,
        typer.Option(help="产业链配置文件路径，用于校验 affected_nodes。"),
    ] = DEFAULT_INDUSTRY_CHAIN_CONFIG_PATH,
    watchlist_path: Annotated[
        Path,
        typer.Option(help="观察池配置文件路径，用于校验 affected_tickers。"),
    ] = DEFAULT_WATCHLIST_CONFIG_PATH,
    risk_events_path: Annotated[
        Path,
        typer.Option(help="风险事件配置路径，用于校验 linked_risk_event_ids。"),
    ] = DEFAULT_RISK_EVENTS_CONFIG_PATH,
    as_of: Annotated[
        str | None,
        typer.Option(help="校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 情景库校验报告输出路径。"),
    ] = None,
) -> None:
    """校验 AI 产业链情景压力测试库。"""
    validation_date = _parse_date(as_of) if as_of else date.today()
    report = validate_scenario_library(
        load_scenario_library(input_path),
        as_of=validation_date,
        industry_chain=load_industry_chain(industry_chain_path),
        watchlist=load_watchlist(watchlist_path),
        risk_events=load_risk_events(risk_events_path),
    )
    report_path = output_path or default_scenario_library_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        validation_date,
    )
    write_scenario_library_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]情景库状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    console.print(f"情景数：{report.scenario_count}；Active：{report.active_count}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")
    if not report.passed:
        raise typer.Exit(code=1)


@scenarios_app.command("lookup")
def lookup_scenario_command(
    scenario_id: Annotated[
        str,
        typer.Option("--id", help="scenario id。"),
    ],
    input_path: Annotated[
        Path,
        typer.Option(help="scenario library YAML 路径。"),
    ] = DEFAULT_SCENARIO_LIBRARY_CONFIG_PATH,
) -> None:
    """按 scenario_id 反查情景定义。"""
    try:
        scenario = lookup_scenario(input_path, scenario_id)
    except FileNotFoundError as exc:
        raise typer.BadParameter(f"scenario library 不存在：{input_path}") from exc
    except (KeyError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    console.print(render_scenario_lookup(scenario))


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter("日期必须是 YYYY-MM-DD") from exc
