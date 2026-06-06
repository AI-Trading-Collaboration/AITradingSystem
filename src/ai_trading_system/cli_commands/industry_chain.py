from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

from ai_trading_system.config import (
    DEFAULT_INDUSTRY_CHAIN_CONFIG_PATH,
    DEFAULT_WATCHLIST_CONFIG_PATH,
    PROJECT_ROOT,
    load_industry_chain,
    load_watchlist,
)
from ai_trading_system.industry_chain import (
    default_industry_chain_report_path,
    validate_industry_chain_config,
    write_industry_chain_validation_report,
)

console = Console()
industry_chain_app = typer.Typer(help="产业链节点和因果图管理。", no_args_is_help=True)


@industry_chain_app.command("list")
def list_industry_chain(
    config_path: Annotated[
        Path,
        typer.Option(help="产业链配置文件路径。"),
    ] = DEFAULT_INDUSTRY_CHAIN_CONFIG_PATH,
) -> None:
    """列出产业链节点和因果关系。"""
    industry_chain = load_industry_chain(config_path)

    table = Table(title="产业链因果图")
    table.add_column("节点")
    table.add_column("名称")
    table.add_column("父节点")
    table.add_column("周期")
    table.add_column("现金流")
    table.add_column("情绪")
    table.add_column("相关标的")

    for node in sorted(industry_chain.nodes, key=lambda value: value.node_id):
        table.add_row(
            node.node_id,
            node.name,
            ", ".join(node.parent_node_ids) or "无",
            _horizon_label(node.impact_horizon),
            _relevance_label(node.cash_flow_relevance),
            _relevance_label(node.sentiment_relevance),
            ", ".join(node.related_tickers),
        )

    console.print(table)


@industry_chain_app.command("validate")
def validate_industry_chain(
    config_path: Annotated[
        Path,
        typer.Option(help="产业链配置文件路径。"),
    ] = DEFAULT_INDUSTRY_CHAIN_CONFIG_PATH,
    watchlist_path: Annotated[
        Path,
        typer.Option(help="观察池配置文件路径，用于校验节点引用。"),
    ] = DEFAULT_WATCHLIST_CONFIG_PATH,
    as_of: Annotated[
        str | None,
        typer.Option(help="校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 产业链校验报告输出路径。"),
    ] = None,
) -> None:
    """校验产业链节点、父子关系和观察池引用。"""
    industry_chain = load_industry_chain(config_path)
    watchlist = load_watchlist(watchlist_path)
    validation_date = _parse_date(as_of) if as_of else date.today()
    report_path = output_path or default_industry_chain_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        validation_date,
    )

    report = validate_industry_chain_config(
        industry_chain=industry_chain,
        watchlist=watchlist,
        as_of=validation_date,
    )
    write_industry_chain_validation_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]产业链校验状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    console.print(f"节点数：{len(report.nodes)}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")

    if not report.passed:
        raise typer.Exit(code=1)


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter("日期必须是 YYYY-MM-DD") from exc


def _horizon_label(value: str) -> str:
    return {
        "short": "短期",
        "medium": "中期",
        "long": "长期",
    }.get(value, value)


def _relevance_label(value: str) -> str:
    return {
        "low": "低",
        "medium": "中",
        "high": "高",
    }.get(value, value)
