from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from ai_trading_system.catalyst_calendar import (
    default_catalyst_calendar_report_path,
    load_catalyst_calendar,
    lookup_catalyst,
    render_catalyst_lookup,
    validate_catalyst_calendar,
    write_catalyst_calendar_report,
)
from ai_trading_system.config import (
    DEFAULT_CATALYST_CALENDAR_CONFIG_PATH,
    DEFAULT_INDUSTRY_CHAIN_CONFIG_PATH,
    DEFAULT_RISK_EVENTS_CONFIG_PATH,
    DEFAULT_WATCHLIST_CONFIG_PATH,
    PROJECT_ROOT,
    load_industry_chain,
    load_risk_events,
    load_watchlist,
)

console = Console()
catalysts_app = typer.Typer(help="未来催化剂日历和事件前复核。", no_args_is_help=True)


@catalysts_app.command("validate")
def validate_catalysts_command(
    input_path: Annotated[
        Path,
        typer.Option(help="catalyst calendar YAML 路径。"),
    ] = DEFAULT_CATALYST_CALENDAR_CONFIG_PATH,
    industry_chain_path: Annotated[
        Path,
        typer.Option(help="产业链配置文件路径，用于校验 related_nodes。"),
    ] = DEFAULT_INDUSTRY_CHAIN_CONFIG_PATH,
    watchlist_path: Annotated[
        Path,
        typer.Option(help="观察池配置文件路径，用于校验 related_tickers。"),
    ] = DEFAULT_WATCHLIST_CONFIG_PATH,
    risk_events_path: Annotated[
        Path,
        typer.Option(help="风险事件配置路径，用于校验 linked_risk_event_ids。"),
    ] = DEFAULT_RISK_EVENTS_CONFIG_PATH,
    as_of: Annotated[
        str | None,
        typer.Option(help="校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    windows: Annotated[
        str,
        typer.Option(help="逗号分隔的 upcoming catalyst 窗口，单位为自然日。"),
    ] = "5,20,60",
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 催化剂日历报告输出路径。"),
    ] = None,
) -> None:
    """校验未来催化剂日历并输出 5/20/60 天窗口。"""
    validation_date = _parse_date(as_of) if as_of else date.today()
    window_values = tuple(_parse_positive_int_csv(windows, "催化剂窗口"))
    report = validate_catalyst_calendar(
        load_catalyst_calendar(input_path),
        as_of=validation_date,
        industry_chain=load_industry_chain(industry_chain_path),
        watchlist=load_watchlist(watchlist_path),
        risk_events=load_risk_events(risk_events_path),
        windows=window_values,
    )
    report_path = output_path or default_catalyst_calendar_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        validation_date,
    )
    write_catalyst_calendar_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]催化剂日历状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    console.print(
        f"事件数：{report.event_count}；" f"未来 {max(report.windows)} 天：{report.upcoming_count}"
    )
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")
    if not report.passed:
        raise typer.Exit(code=1)


@catalysts_app.command("upcoming")
def upcoming_catalysts_command(
    input_path: Annotated[
        Path,
        typer.Option(help="catalyst calendar YAML 路径。"),
    ] = DEFAULT_CATALYST_CALENDAR_CONFIG_PATH,
    as_of: Annotated[
        str | None,
        typer.Option(help="评估日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    windows: Annotated[
        str,
        typer.Option(help="逗号分隔的 upcoming catalyst 窗口，单位为自然日。"),
    ] = "5,20,60",
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown upcoming catalyst 报告输出路径。"),
    ] = None,
) -> None:
    """输出 upcoming catalyst 分桶报告。"""
    validation_date = _parse_date(as_of) if as_of else date.today()
    window_values = tuple(_parse_positive_int_csv(windows, "催化剂窗口"))
    report = validate_catalyst_calendar(
        load_catalyst_calendar(input_path),
        as_of=validation_date,
        industry_chain=load_industry_chain(),
        watchlist=load_watchlist(),
        risk_events=load_risk_events(),
        windows=window_values,
    )
    report_path = output_path or default_catalyst_calendar_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        validation_date,
    )
    write_catalyst_calendar_report(report, report_path)
    console.print(f"Upcoming catalyst 报告：{report_path}")
    console.print(f"未来 {max(report.windows)} 天事件数：{report.upcoming_count}")
    if not report.passed:
        console.print("[red]催化剂日历校验失败，upcoming 报告仅供排查。[/red]")
        raise typer.Exit(code=1)


@catalysts_app.command("lookup")
def lookup_catalyst_command(
    catalyst_id: Annotated[
        str,
        typer.Option("--id", help="catalyst id。"),
    ],
    input_path: Annotated[
        Path,
        typer.Option(help="catalyst calendar YAML 路径。"),
    ] = DEFAULT_CATALYST_CALENDAR_CONFIG_PATH,
) -> None:
    """按 catalyst_id 反查催化剂事件。"""
    try:
        event = lookup_catalyst(input_path, catalyst_id)
    except FileNotFoundError as exc:
        raise typer.BadParameter(f"catalyst calendar 不存在：{input_path}") from exc
    except (KeyError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    console.print(render_catalyst_lookup(event))


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter("日期必须是 YYYY-MM-DD") from exc


def _parse_csv_items(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def _parse_positive_int_csv(value: str, label: str) -> list[int]:
    items = _parse_csv_items(value)
    if not items:
        raise typer.BadParameter(f"{label}不能为空。")
    parsed: list[int] = []
    for item in items:
        try:
            integer = int(item)
        except ValueError as exc:
            raise typer.BadParameter(f"{label}必须是逗号分隔的正整数。") from exc
        if integer <= 0:
            raise typer.BadParameter(f"{label}必须是正整数。")
        parsed.append(integer)
    return parsed
