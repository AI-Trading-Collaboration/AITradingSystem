from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio.parameter_review import (
    DEFAULT_PARAMETER_REVIEW_AGGREGATION_DIR,
    DEFAULT_PARAMETER_REVIEW_REVIEW_DIR,
    DEFAULT_PARAMETER_REVIEW_VALIDATION_DIR,
    build_parameter_review_aggregation,
    build_parameter_review_report,
    build_parameter_review_validation_report,
    write_parameter_review_aggregation,
    write_parameter_review_report,
    write_parameter_review_validation_report,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import parameter_review_app
from ai_trading_system.interfaces.cli.etf_portfolio.weekly_review import weekly_review_date
from ai_trading_system.reports.report_index import DEFAULT_REPORT_REGISTRY_PATH


def run_parameter_review_report(
    *,
    as_of: str | None,
    latest: bool,
    report_index_path: Path | None,
    report_registry_path: Path,
    output_dir: Path,
) -> None:
    run_date = weekly_review_date(as_of=as_of, latest=latest)
    payload = build_parameter_review_report(
        as_of=run_date,
        report_index_path=report_index_path,
        report_registry_path=report_registry_path,
    )
    json_path = output_dir / f"parameter_review_{run_date.isoformat()}.json"
    md_path = output_dir / f"parameter_review_{run_date.isoformat()}.md"
    write_parameter_review_report(payload, json_path=json_path, markdown_path=md_path)
    typer.echo(f"ETF parameter review report：{md_path}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"candidate_count={payload['summary']['candidate_count']}")
    typer.echo(
        f"eligible_for_manual_review_count={payload['summary']['eligible_for_manual_review_count']}"
    )
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@parameter_review_app.command("aggregate")
def parameter_review_aggregate_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="parameter review aggregation 日期 YYYY-MM-DD。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="使用当前日期扫描 latest artifacts。"),
    ] = False,
    report_index_path: Annotated[
        Path | None,
        typer.Option(help="既有 report_index JSON；不传时只读扫描 report registry。"),
    ] = None,
    report_registry_path: Annotated[
        Path,
        typer.Option(help="report registry config path。"),
    ] = DEFAULT_REPORT_REGISTRY_PATH,
    output_dir: Annotated[
        Path,
        typer.Option(help="parameter review evidence 输出目录。"),
    ] = DEFAULT_PARAMETER_REVIEW_AGGREGATION_DIR,
) -> None:
    """聚合 TRADING-070 ETF parameter review forward evidence。"""
    run_date = weekly_review_date(as_of=as_of, latest=latest)
    payload = build_parameter_review_aggregation(
        as_of=run_date,
        report_index_path=report_index_path,
        report_registry_path=report_registry_path,
    )
    json_path = output_dir / f"parameter_review_evidence_{run_date.isoformat()}.json"
    md_path = output_dir / f"parameter_review_evidence_{run_date.isoformat()}.md"
    write_parameter_review_aggregation(payload, json_path=json_path, markdown_path=md_path)
    typer.echo(f"ETF parameter review evidence：{md_path}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"candidate_count={payload['candidate_count']}")
    typer.echo(f"evidence_record_count={payload['evidence_record_count']}")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@parameter_review_app.command("report")
def parameter_review_report_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="parameter review report 日期 YYYY-MM-DD。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="使用当前日期扫描 latest artifacts。"),
    ] = False,
    report_index_path: Annotated[
        Path | None,
        typer.Option(help="既有 report_index JSON；不传时只读扫描 report registry。"),
    ] = None,
    report_registry_path: Annotated[
        Path,
        typer.Option(help="report registry config path。"),
    ] = DEFAULT_REPORT_REGISTRY_PATH,
    output_dir: Annotated[
        Path,
        typer.Option(help="parameter review report 输出目录。"),
    ] = DEFAULT_PARAMETER_REVIEW_REVIEW_DIR,
) -> None:
    """生成 TRADING-070 ETF parameter review report。"""
    run_parameter_review_report(
        as_of=as_of,
        latest=latest,
        report_index_path=report_index_path,
        report_registry_path=report_registry_path,
        output_dir=output_dir,
    )


@parameter_review_app.command("run")
def parameter_review_run_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="parameter review report 日期 YYYY-MM-DD。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="使用当前日期扫描 latest artifacts。"),
    ] = False,
    report_index_path: Annotated[
        Path | None,
        typer.Option(help="既有 report_index JSON；不传时只读扫描 report registry。"),
    ] = None,
    report_registry_path: Annotated[
        Path,
        typer.Option(help="report registry config path。"),
    ] = DEFAULT_REPORT_REGISTRY_PATH,
    output_dir: Annotated[
        Path,
        typer.Option(help="parameter review report 输出目录。"),
    ] = DEFAULT_PARAMETER_REVIEW_REVIEW_DIR,
) -> None:
    """运行 TRADING-070 ETF parameter review report workflow。"""
    run_parameter_review_report(
        as_of=as_of,
        latest=latest,
        report_index_path=report_index_path,
        report_registry_path=report_registry_path,
        output_dir=output_dir,
    )


@parameter_review_app.command("validate")
def parameter_review_validate_command(
    report_registry_path: Annotated[
        Path,
        typer.Option(help="report registry config path。"),
    ] = DEFAULT_REPORT_REGISTRY_PATH,
    output_dir: Annotated[
        Path,
        typer.Option(help="parameter review validation 输出目录。"),
    ] = DEFAULT_PARAMETER_REVIEW_VALIDATION_DIR,
) -> None:
    """生成 TRADING-070 parameter review validation gate；失败时 fail closed。"""
    generated = datetime.now(UTC)
    payload = build_parameter_review_validation_report(
        report_registry_path=report_registry_path,
        generated_at=generated,
    )
    stem = f"parameter_review_validation_{generated.date().isoformat()}"
    json_path = output_dir / f"{stem}.json"
    md_path = output_dir / f"{stem}.md"
    write_parameter_review_validation_report(
        payload,
        json_path=json_path,
        markdown_path=md_path,
    )
    typer.echo(f"ETF parameter review validation gate：{md_path}")
    typer.echo(f"status={payload['status']}")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


__all__ = [
    "parameter_review_aggregate_command",
    "parameter_review_report_command",
    "parameter_review_run_command",
    "parameter_review_validate_command",
    "run_parameter_review_report",
]
