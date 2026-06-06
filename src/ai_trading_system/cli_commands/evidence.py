from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.market_evidence import (
    default_market_evidence_report_path,
    import_market_evidence_csv,
    load_market_evidence_store,
    validate_market_evidence_store,
    write_market_evidence_import_report,
    write_market_evidence_validation_report,
    write_market_evidence_yaml,
)

DEFAULT_MARKET_EVIDENCE_PATH = PROJECT_ROOT / "data" / "external" / "market_evidence"

console = Console()
evidence_app = typer.Typer(help="新市场信息 evidence 账本。", no_args_is_help=True)


@evidence_app.command("validate")
def validate_market_evidence(
    input_path: Annotated[
        Path,
        typer.Option(help="market_evidence YAML 文件或目录路径。"),
    ] = DEFAULT_MARKET_EVIDENCE_PATH,
    as_of: Annotated[
        str | None,
        typer.Option(help="校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown evidence 校验报告输出路径。"),
    ] = None,
) -> None:
    """校验新市场信息 evidence 账本。"""
    validation_date = _parse_date(as_of) if as_of else date.today()
    report_path = output_path or default_market_evidence_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        validation_date,
    )
    report = validate_market_evidence_store(
        load_market_evidence_store(input_path),
        as_of=validation_date,
    )
    write_market_evidence_validation_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]Market evidence 状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    console.print(f"证据数：{report.evidence_count}；待复核：{report.pending_review_count}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")
    if not report.passed:
        raise typer.Exit(code=1)


@evidence_app.command("import-csv")
def import_market_evidence_command(
    input_path: Annotated[
        Path,
        typer.Option(help="人工复核或 LLM 分类后的 market_evidence CSV 路径。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option(help="写入 market_evidence YAML 的目录。"),
    ] = DEFAULT_MARKET_EVIDENCE_PATH,
    report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 导入报告输出路径。"),
    ] = None,
) -> None:
    """从 CSV 导入 market_evidence YAML。"""
    import_report = import_market_evidence_csv(input_path)
    import_report_output = report_path or (
        PROJECT_ROOT / "outputs" / "reports" / f"market_evidence_import_{date.today()}.md"
    )
    write_market_evidence_import_report(import_report, import_report_output)
    if not import_report.passed:
        console.print("[red]Market evidence CSV 导入失败，未写入 YAML。[/red]")
        console.print(f"导入报告：{import_report_output}")
        raise typer.Exit(code=1)

    written_paths = write_market_evidence_yaml(import_report.evidence, output_dir)
    console.print("[green]Market evidence 已导入。[/green]")
    console.print(f"导入报告：{import_report_output}")
    console.print(f"写入证据数：{len(written_paths)}")
    console.print(f"输出目录：{output_dir}")


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter("日期必须是 YYYY-MM-DD") from exc
