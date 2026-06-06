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
from ai_trading_system.thesis import (
    build_thesis_review_report,
    default_thesis_review_report_path,
    default_thesis_validation_report_path,
    load_trade_thesis_store,
    validate_trade_thesis_store,
    write_thesis_review_report,
    write_thesis_validation_report,
)

console = Console()
thesis_app = typer.Typer(help="交易 thesis 和假设验证管理。", no_args_is_help=True)


@thesis_app.command("list")
def list_theses(
    input_path: Annotated[
        Path,
        typer.Option(help="交易 thesis YAML 文件或目录路径。"),
    ] = PROJECT_ROOT
    / "data"
    / "external"
    / "trade_theses",
) -> None:
    """列出本地交易 thesis。"""
    store = load_trade_thesis_store(input_path)

    table = Table(title="交易 Thesis")
    table.add_column("Thesis")
    table.add_column("Ticker")
    table.add_column("方向")
    table.add_column("状态")
    table.add_column("创建日期")
    table.add_column("复核频率")
    table.add_column("文件")

    for loaded in sorted(store.loaded, key=lambda item: item.thesis.thesis_id):
        thesis = loaded.thesis
        table.add_row(
            thesis.thesis_id,
            thesis.ticker,
            _thesis_direction_label(thesis.direction),
            _thesis_status_label(thesis.status),
            thesis.created_at.isoformat(),
            _thesis_review_frequency_label(thesis.review_frequency),
            str(loaded.path),
        )

    console.print(table)
    if not store.loaded:
        console.print("未发现可读取的交易 thesis。")
    if store.load_errors:
        console.print(
            f"[red]存在 {len(store.load_errors)} 个加载错误，请运行 validate 查看。[/red]"
        )


@thesis_app.command("validate")
def validate_theses(
    input_path: Annotated[
        Path,
        typer.Option(help="交易 thesis YAML 文件或目录路径。"),
    ] = PROJECT_ROOT
    / "data"
    / "external"
    / "trade_theses",
    watchlist_path: Annotated[
        Path,
        typer.Option(help="观察池配置文件路径。"),
    ] = DEFAULT_WATCHLIST_CONFIG_PATH,
    industry_chain_path: Annotated[
        Path,
        typer.Option(help="产业链配置文件路径。"),
    ] = DEFAULT_INDUSTRY_CHAIN_CONFIG_PATH,
    as_of: Annotated[
        str | None,
        typer.Option(help="校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown thesis 校验报告输出路径。"),
    ] = None,
) -> None:
    """校验交易 thesis 的结构、引用和复核约束。"""
    validation_date = _parse_date(as_of) if as_of else date.today()
    report_path = output_path or default_thesis_validation_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        validation_date,
    )
    store = load_trade_thesis_store(input_path)
    report = validate_trade_thesis_store(
        store=store,
        watchlist=load_watchlist(watchlist_path),
        industry_chain=load_industry_chain(industry_chain_path),
        as_of=validation_date,
    )
    write_thesis_validation_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]交易 thesis 校验状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    console.print(f"Thesis 数量：{report.thesis_count}；活跃：{report.active_count}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")

    if not report.passed:
        raise typer.Exit(code=1)


@thesis_app.command("review")
def review_theses(
    input_path: Annotated[
        Path,
        typer.Option(help="交易 thesis YAML 文件或目录路径。"),
    ] = PROJECT_ROOT
    / "data"
    / "external"
    / "trade_theses",
    watchlist_path: Annotated[
        Path,
        typer.Option(help="观察池配置文件路径。"),
    ] = DEFAULT_WATCHLIST_CONFIG_PATH,
    industry_chain_path: Annotated[
        Path,
        typer.Option(help="产业链配置文件路径。"),
    ] = DEFAULT_INDUSTRY_CHAIN_CONFIG_PATH,
    as_of: Annotated[
        str | None,
        typer.Option(help="复核日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown thesis 复核报告输出路径。"),
    ] = None,
) -> None:
    """复核交易 thesis 的验证指标、证伪条件和风险事件状态。"""
    review_date = _parse_date(as_of) if as_of else date.today()
    report_path = output_path or default_thesis_review_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        review_date,
    )
    store = load_trade_thesis_store(input_path)
    validation_report = validate_trade_thesis_store(
        store=store,
        watchlist=load_watchlist(watchlist_path),
        industry_chain=load_industry_chain(industry_chain_path),
        as_of=review_date,
    )
    review_report = build_thesis_review_report(validation_report)
    write_thesis_review_report(review_report, report_path)

    status_style = (
        "green"
        if review_report.status == "PASS"
        else ("yellow" if validation_report.passed else "red")
    )
    console.print(f"[{status_style}]交易 thesis 复核状态：{review_report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    console.print(
        f"Thesis 数量：{validation_report.thesis_count}；" f"活跃：{validation_report.active_count}"
    )
    console.print(
        f"校验错误数：{validation_report.error_count}；"
        f"校验警告数：{validation_report.warning_count}"
    )

    if not validation_report.passed:
        raise typer.Exit(code=1)


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter("日期必须是 YYYY-MM-DD") from exc


def _thesis_direction_label(value: str) -> str:
    return {
        "long": "做多",
        "short": "做空",
        "hedge": "对冲",
        "watch": "观察",
    }.get(value, value)


def _thesis_status_label(value: str) -> str:
    return {
        "draft": "草稿",
        "active": "活跃",
        "warning": "警告",
        "challenged": "受挑战",
        "paused": "暂停",
        "closed": "已关闭",
        "invalidated": "已证伪",
    }.get(value, value)


def _thesis_review_frequency_label(value: str) -> str:
    return {
        "daily": "每日",
        "weekly": "每周",
        "monthly": "每月",
        "quarterly": "每季",
        "event_driven": "事件驱动",
    }.get(value, value)
