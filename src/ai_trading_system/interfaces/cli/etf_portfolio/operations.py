from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Annotated, cast

import typer

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.etf_portfolio.operations import (
    DEFAULT_ETF_OPERATIONS_SCHEDULE_CONFIG_PATH,
    OperationsGraphCadence,
    build_operations_health_report,
    build_operations_scheduler_dry_run,
    build_operations_validation_report,
    write_operations_health_report,
    write_operations_scheduler_dry_run,
    write_operations_validation_report,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import ops_app

DEFAULT_ETF_OPERATIONS_DRY_RUN_DIR = PROJECT_ROOT / "outputs" / "dry_runs" / "etf_operations"


DEFAULT_ETF_OPERATIONS_REPORT_DIR = PROJECT_ROOT / "reports" / "etf_portfolio" / "operations"


DEFAULT_ETF_OPERATIONS_VALIDATION_DIR = DEFAULT_ETF_OPERATIONS_REPORT_DIR / "validation"


def parse_operations_graph_cadence(value: str) -> OperationsGraphCadence:
    if value not in {"daily", "weekly", "biweekly", "monthly"}:
        raise typer.BadParameter("--cadence must be one of: daily, weekly, biweekly, monthly")
    return cast(OperationsGraphCadence, value)


@ops_app.command("dry-run")
def ops_dry_run_command(
    cadence: Annotated[
        str,
        typer.Option("--cadence", help="Operations cadence: daily/weekly/biweekly/monthly。"),
    ],
    as_of: Annotated[
        str,
        typer.Option("--as-of", "--date", help="评估日期 YYYY-MM-DD。"),
    ],
    root_path: Annotated[
        Path,
        typer.Option("--root-path", help="扫描既有 artifacts 的根目录。"),
    ] = PROJECT_ROOT,
    output_path: Annotated[
        Path | None,
        typer.Option("--output-path", help="dry-run JSON 输出路径。"),
    ] = None,
    include_optional: Annotated[
        bool,
        typer.Option(
            "--include-optional/--skip-optional",
            help="是否把 optional steps 纳入计划。",
        ),
    ] = True,
    no_write: Annotated[
        bool,
        typer.Option("--no-write", help="只打印 dry-run 摘要，不写 JSON artifact。"),
    ] = False,
) -> None:
    """只规划 ETF operations cadence，不执行命令、不写 production state。"""
    requested_cadence = parse_operations_graph_cadence(cadence)
    report = build_operations_scheduler_dry_run(
        cadence=requested_cadence,
        as_of=as_of,
        root_path=root_path,
        include_optional=include_optional,
    )
    output = output_path or (
        DEFAULT_ETF_OPERATIONS_DRY_RUN_DIR
        / f"operations_dry_run_{report.cadence}_{report.as_of_date.isoformat()}.json"
    )
    if not no_write:
        write_operations_scheduler_dry_run(report, output)
        typer.echo(f"ETF operations dry-run JSON：{output}")
    else:
        typer.echo("ETF operations dry-run JSON：not_written")
    typer.echo(f"dry_run_id={report.dry_run_id}")
    typer.echo(f"cadence={report.cadence}")
    typer.echo(f"as_of_date={report.as_of_date.isoformat()}")
    typer.echo(f"status={report.status}")
    typer.echo(f"planned_step_count={len(report.planned_steps)}")
    typer.echo(f"blocking_failure_count={len(report.blocking_failures)}")
    typer.echo(f"warning_count={len(report.warnings)}")
    typer.echo("dry_run_only=true")
    typer.echo("commands_executed=false")
    typer.echo("production_state_mutated=false")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@ops_app.command("report")
def ops_report_command(
    cadence: Annotated[
        str,
        typer.Option("--cadence", help="Operations cadence: daily/weekly/biweekly/monthly。"),
    ],
    as_of: Annotated[
        str,
        typer.Option("--as-of", "--date", help="评估日期 YYYY-MM-DD。"),
    ],
    root_path: Annotated[
        Path,
        typer.Option("--root-path", help="扫描既有 artifacts 的根目录。"),
    ] = PROJECT_ROOT,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="operations report 输出目录。"),
    ] = DEFAULT_ETF_OPERATIONS_REPORT_DIR,
    json_path: Annotated[
        Path | None,
        typer.Option("--json-path", help="显式 JSON 输出路径。"),
    ] = None,
    markdown_path: Annotated[
        Path | None,
        typer.Option("--markdown-path", help="显式 Markdown 输出路径。"),
    ] = None,
    include_optional: Annotated[
        bool,
        typer.Option(
            "--include-optional/--skip-optional",
            help="是否把 optional steps 纳入 report plan。",
        ),
    ] = True,
) -> None:
    """生成 ETF operations health JSON / Markdown report；不执行计划命令。"""
    requested_cadence = parse_operations_graph_cadence(cadence)
    report = build_operations_health_report(
        cadence=requested_cadence,
        as_of=as_of,
        root_path=root_path,
        include_optional=include_optional,
    )
    cadence_dir = output_dir / report.cadence
    json_output = json_path or (
        cadence_dir / f"operations_health_{report.as_of_date.isoformat()}.json"
    )
    markdown_output = markdown_path or (
        cadence_dir / f"operations_health_{report.as_of_date.isoformat()}.md"
    )
    paths = write_operations_health_report(
        report,
        json_path=json_output,
        markdown_path=markdown_output,
    )
    typer.echo(f"ETF operations health JSON：{paths['json']}")
    typer.echo(f"ETF operations health Markdown：{paths['markdown']}")
    typer.echo(f"report_id={report.report_id}")
    typer.echo(f"cadence={report.cadence}")
    typer.echo(f"as_of_date={report.as_of_date.isoformat()}")
    typer.echo(f"status={report.status}")
    typer.echo(f"planned_step_count={len(report.pipeline_schedule)}")
    typer.echo(f"blocking_failure_count={len(report.failures)}")
    typer.echo(f"warning_count={len(report.warnings)}")
    typer.echo("commands_executed=false")
    typer.echo("production_state_mutated=false")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@ops_app.command("validate")
def ops_validate_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="评估日期 YYYY-MM-DD；省略时使用今天。"),
    ] = None,
    root_path: Annotated[
        Path,
        typer.Option("--root-path", help="扫描既有 artifacts 的根目录。"),
    ] = PROJECT_ROOT,
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="operations schedule config path。"),
    ] = DEFAULT_ETF_OPERATIONS_SCHEDULE_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="operations validation 输出目录。"),
    ] = DEFAULT_ETF_OPERATIONS_VALIDATION_DIR,
    json_path: Annotated[
        Path | None,
        typer.Option("--json-path", help="显式 JSON 输出路径。"),
    ] = None,
    markdown_path: Annotated[
        Path | None,
        typer.Option("--markdown-path", help="显式 Markdown 输出路径。"),
    ] = None,
) -> None:
    """校验 ETF operations workflow 完整性和安全边界；失败时 exit 1。"""
    requested_as_of = as_of or date.today().isoformat()
    report = build_operations_validation_report(
        as_of=requested_as_of,
        root_path=root_path,
        config_path=config_path,
    )
    json_output = json_path or (
        output_dir / f"operations_validation_{report.as_of_date.isoformat()}.json"
    )
    markdown_output = markdown_path or (
        output_dir / f"operations_validation_{report.as_of_date.isoformat()}.md"
    )
    paths = write_operations_validation_report(
        report,
        json_path=json_output,
        markdown_path=markdown_output,
    )
    typer.echo(f"ETF operations validation JSON：{paths['json']}")
    typer.echo(f"ETF operations validation Markdown：{paths['markdown']}")
    typer.echo(f"report_id={report.report_id}")
    typer.echo(f"as_of_date={report.as_of_date.isoformat()}")
    typer.echo(f"status={report.status}")
    typer.echo(f"failed_check_count={report.failed_check_count}")
    typer.echo(f"warning_check_count={report.warning_check_count}")
    typer.echo("commands_executed=false")
    typer.echo("production_state_mutated=false")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")
    if report.status != "PASS":
        raise typer.Exit(code=1)


__all__ = [
    "DEFAULT_ETF_OPERATIONS_DRY_RUN_DIR",
    "DEFAULT_ETF_OPERATIONS_REPORT_DIR",
    "DEFAULT_ETF_OPERATIONS_SCHEDULE_CONFIG_PATH",
    "DEFAULT_ETF_OPERATIONS_VALIDATION_DIR",
    "ops_dry_run_command",
    "ops_report_command",
    "ops_validate_command",
    "parse_operations_graph_cadence",
]
