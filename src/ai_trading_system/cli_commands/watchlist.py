from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

from ai_trading_system.config import (
    DEFAULT_WATCHLIST_CONFIG_PATH,
    PROJECT_ROOT,
    load_universe,
    load_watchlist,
)
from ai_trading_system.watchlist import (
    default_watchlist_report_path,
    validate_watchlist_config,
    write_watchlist_validation_report,
)
from ai_trading_system.watchlist_lifecycle import (
    DEFAULT_WATCHLIST_LIFECYCLE_PATH,
    default_watchlist_lifecycle_report_path,
    load_watchlist_lifecycle,
    validate_watchlist_lifecycle,
    write_watchlist_lifecycle_report,
)

console = Console()
watchlist_app = typer.Typer(help="观察池和能力圈管理。", no_args_is_help=True)


@watchlist_app.command("list")
def list_watchlist(
    config_path: Annotated[
        Path,
        typer.Option(help="观察池配置文件路径。"),
    ] = DEFAULT_WATCHLIST_CONFIG_PATH,
    active_only: Annotated[
        bool,
        typer.Option("--active-only/--all", help="只显示活跃标的，或显示全部配置标的。"),
    ] = True,
) -> None:
    """列出观察池和能力圈配置。"""
    watchlist = load_watchlist(config_path)
    items = [item for item in watchlist.items if item.active or not active_only]

    table = Table(title="观察池与能力圈")
    table.add_column("Ticker")
    table.add_column("公司")
    table.add_column("类型")
    table.add_column("阶段")
    table.add_column("能力圈")
    table.add_column("风险")
    table.add_column("Thesis")
    table.add_column("产业链节点")

    for item in sorted(items, key=lambda value: value.ticker):
        table.add_row(
            item.ticker,
            item.company_name,
            item.instrument_type,
            _decision_stage_label(item.decision_stage),
            f"{item.competence_score:.0f}",
            _risk_level_label(item.default_risk_level),
            "需要" if item.thesis_required else "不需要",
            ", ".join(item.ai_chain_nodes),
        )

    console.print(table)


@watchlist_app.command("validate")
def validate_watchlist(
    config_path: Annotated[
        Path,
        typer.Option(help="观察池配置文件路径。"),
    ] = DEFAULT_WATCHLIST_CONFIG_PATH,
    as_of: Annotated[
        str | None,
        typer.Option(help="校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 观察池校验报告输出路径。"),
    ] = None,
) -> None:
    """校验观察池覆盖、能力圈和 thesis 约束。"""
    universe = load_universe()
    watchlist = load_watchlist(config_path)
    validation_date = _parse_date(as_of) if as_of else date.today()
    report_path = output_path or default_watchlist_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        validation_date,
    )

    report = validate_watchlist_config(
        watchlist=watchlist,
        universe=universe,
        as_of=validation_date,
    )
    write_watchlist_validation_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]观察池校验状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    console.print(f"活跃标的数：{report.active_count}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")

    if not report.passed:
        raise typer.Exit(code=1)


@watchlist_app.command("validate-lifecycle")
def validate_watchlist_lifecycle_command(
    input_path: Annotated[
        Path,
        typer.Option(help="观察池 lifecycle YAML 配置路径。"),
    ] = DEFAULT_WATCHLIST_LIFECYCLE_PATH,
    watchlist_path: Annotated[
        Path,
        typer.Option(help="当前观察池配置路径，用于一致性校验。"),
    ] = DEFAULT_WATCHLIST_CONFIG_PATH,
    as_of: Annotated[
        str | None,
        typer.Option(help="校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown lifecycle 校验报告输出路径。"),
    ] = None,
) -> None:
    """校验观察池 point-in-time 生命周期。"""
    universe = load_universe()
    watchlist = load_watchlist(watchlist_path)
    validation_date = _parse_date(as_of) if as_of else date.today()
    report_path = output_path or default_watchlist_lifecycle_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        validation_date,
    )
    report = validate_watchlist_lifecycle(
        lifecycle=load_watchlist_lifecycle(input_path),
        input_path=input_path,
        watchlist=watchlist,
        universe=universe,
        as_of=validation_date,
    )
    write_watchlist_lifecycle_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]观察池 lifecycle 校验状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    console.print(f"生命周期记录数：{report.entry_count}；当前活跃：{report.active_entry_count}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")

    if not report.passed:
        raise typer.Exit(code=1)


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter("日期必须是 YYYY-MM-DD") from exc


def _risk_level_label(level: str) -> str:
    return {
        "low": "低",
        "medium": "中",
        "high": "高",
        "critical": "极高",
    }.get(level, level)


def _decision_stage_label(stage: str) -> str:
    return {
        "watch_only": "仅观察",
        "active_trade": "主动交易",
    }.get(stage, stage)
