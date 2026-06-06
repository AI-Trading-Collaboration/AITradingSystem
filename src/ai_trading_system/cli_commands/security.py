from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.secret_hygiene import (
    default_secret_scan_report_path,
    scan_secrets,
    write_secret_scan_report,
)

console = Console()
security_app = typer.Typer(help="密钥卫生和供应商权限治理。", no_args_is_help=True)


@security_app.command("scan-secrets")
def security_scan_secrets_command(
    as_of: Annotated[
        str | None,
        typer.Option(help="扫描日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    scan_paths: Annotated[
        str,
        typer.Option(
            help="逗号分隔的扫描入口；默认扫描 config、docs、outputs 和 download manifest。",
        ),
    ] = "config,docs,outputs,data/raw/download_manifest.csv",
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown secret hygiene 扫描报告输出路径。"),
    ] = None,
) -> None:
    """扫描可提交或报告文件中的疑似 secret literal。"""
    scan_date = _parse_date(as_of) if as_of else date.today()
    selected_paths = tuple(Path(item) for item in _parse_csv_items(scan_paths))
    report_path = output_path or default_secret_scan_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        scan_date,
    )
    report = scan_secrets(paths=selected_paths, as_of=scan_date)
    write_secret_scan_report(report, report_path)

    style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{style}]Secret hygiene：{report.status}[/{style}]")
    console.print(f"报告：{report_path}")
    console.print(f"扫描文件数：{report.scanned_file_count}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")
    if not report.passed:
        raise typer.Exit(code=1)


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter("日期必须是 YYYY-MM-DD") from exc


def _parse_csv_items(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]
