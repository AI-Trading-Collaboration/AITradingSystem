from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from ai_trading_system.config import DEFAULT_EXECUTION_POLICY_CONFIG_PATH, PROJECT_ROOT
from ai_trading_system.execution_policy import (
    default_execution_policy_report_path,
    load_execution_policy,
    lookup_execution_action,
    render_execution_action_lookup,
    validate_execution_policy,
    write_execution_policy_report,
)

console = Console()
execution_app = typer.Typer(help="Advisory execution policy 和执行纪律。", no_args_is_help=True)


@execution_app.command("validate")
def validate_execution_policy_command(
    input_path: Annotated[
        Path,
        typer.Option(help="execution policy YAML 路径。"),
    ] = DEFAULT_EXECUTION_POLICY_CONFIG_PATH,
    as_of: Annotated[
        str | None,
        typer.Option(help="校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown execution policy 校验报告输出路径。"),
    ] = None,
) -> None:
    """校验 advisory execution policy 和固定动作词表。"""
    validation_date = _parse_date(as_of) if as_of else date.today()
    report = validate_execution_policy(
        load_execution_policy(input_path),
        as_of=validation_date,
    )
    report_path = output_path or default_execution_policy_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        validation_date,
    )
    write_execution_policy_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]执行政策状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    console.print(f"动作数：{report.action_count}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")
    if not report.passed:
        raise typer.Exit(code=1)


@execution_app.command("lookup")
def lookup_execution_action_command(
    action_id: Annotated[
        str,
        typer.Option("--id", help="execution action id。"),
    ],
    input_path: Annotated[
        Path,
        typer.Option(help="execution policy YAML 路径。"),
    ] = DEFAULT_EXECUTION_POLICY_CONFIG_PATH,
) -> None:
    """按 execution action id 反查动作定义。"""
    try:
        action = lookup_execution_action(input_path, action_id)
    except FileNotFoundError as exc:
        raise typer.BadParameter(f"execution policy 不存在：{input_path}") from exc
    except (KeyError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    console.print(render_execution_action_lookup(action))


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter("日期必须是 YYYY-MM-DD") from exc
