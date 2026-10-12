from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from ai_trading_system import secret_hygiene
from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.secret_hygiene import (
    SecretScanMode,
    default_secret_scan_report_path,
    scan_secrets,
    write_secret_scan_report,
)

console = Console()
security_app = typer.Typer(help="密钥卫生和供应商权限治理。", no_args_is_help=True)
DEFAULT_SECRET_SCAN_PATHS = "config,docs,outputs,data/raw/download_manifest.csv"


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
    ] = DEFAULT_SECRET_SCAN_PATHS,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown secret hygiene 扫描报告输出路径。"),
    ] = None,
    cache_path: Annotated[
        Path | None,
        typer.Option(
            help=(
                "增量扫描缓存路径。不指定时：默认扫描入口使用 "
                "data/cache/secret_hygiene/scan_cache.json；自定义扫描入口不使用缓存，全量扫描。"
            ),
        ),
    ] = None,
    full_scan: Annotated[
        bool,
        typer.Option("--full-scan", help="忽略增量缓存，全量重扫并重建缓存。"),
    ] = False,
) -> None:
    """扫描可提交或报告文件中的疑似 secret literal。"""
    scan_date = _parse_date(as_of) if as_of else date.today()
    selected_paths = tuple(Path(item) for item in _parse_csv_items(scan_paths))
    report_path = output_path or default_secret_scan_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        scan_date,
    )
    effective_cache_path = cache_path
    if effective_cache_path is None and scan_paths == DEFAULT_SECRET_SCAN_PATHS:
        effective_cache_path = secret_hygiene.DEFAULT_SECRET_SCAN_CACHE_PATH
    report = scan_secrets(
        paths=selected_paths,
        as_of=scan_date,
        cache_path=effective_cache_path,
        force_full_scan=full_scan,
    )
    write_secret_scan_report(report, report_path)

    style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{style}]Secret hygiene：{report.status}[/{style}]")
    console.print(f"报告：{report_path}")
    console.print(f"扫描文件数：{report.scanned_file_count}")
    mode_text = "增量" if report.scan_mode == SecretScanMode.INCREMENTAL else "全量"
    console.print(
        f"扫描方式：{mode_text}；本次读取：{report.read_file_count}；"
        f"沿用上次结果：{report.reused_file_count}"
    )
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
