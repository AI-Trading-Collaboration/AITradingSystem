from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.docs_freshness import (
    default_docs_freshness_paths,
    validate_docs_freshness,
    write_docs_freshness_report,
)

console = Console()
docs_app = typer.Typer(help="项目文档治理和新鲜度检查。", no_args_is_help=True)


@docs_app.command("validate-freshness")
def validate_docs_freshness_command(
    paths: Annotated[
        list[Path] | None,
        typer.Option(
            "--path",
            help="要检查的新鲜度文档路径；不传则检查关键 docs 和 requirements。",
        ),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="可选 Markdown 文档新鲜度报告输出路径。"),
    ] = None,
) -> None:
    """检查关键项目文档的 `最后更新` 是否落后于内部状态记录。"""
    checked_paths = tuple(paths or default_docs_freshness_paths(PROJECT_ROOT))
    report = validate_docs_freshness(checked_paths)
    if output_path is not None:
        write_docs_freshness_report(report, output_path)

    style = "green" if report.passed else "red"
    console.print(f"[{style}]文档新鲜度：{report.status}[/{style}]")
    console.print(f"检查文档数：{len(report.records)}")
    console.print(f"问题数：{len(report.issues)}")
    if output_path is not None:
        console.print(f"报告：{output_path}")
    for issue in report.issues[:10]:
        console.print(f"{issue.path}: {issue.code}: {issue.message}")
    if report.issues:
        raise typer.Exit(code=1)
