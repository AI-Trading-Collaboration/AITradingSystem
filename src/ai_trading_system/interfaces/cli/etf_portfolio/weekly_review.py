from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio.models import DEFAULT_ETF_TARGET_PATH
from ai_trading_system.etf_portfolio.weekly_review import (
    DEFAULT_ETF_WEEKLY_REVIEW_AGGREGATION_DIR,
    DEFAULT_ETF_WEEKLY_REVIEW_DIR,
    DEFAULT_ETF_WEEKLY_REVIEW_VALIDATION_DIR,
    build_weekly_review_aggregation,
    build_weekly_review_report,
    build_weekly_review_validation_report,
    write_weekly_review_aggregation,
    write_weekly_review_report,
    write_weekly_review_validation_report,
)
from ai_trading_system.interfaces.cli.etf_portfolio.common import parse_date
from ai_trading_system.interfaces.cli.etf_portfolio.registration import weekly_review_app
from ai_trading_system.reports.report_index import DEFAULT_REPORT_REGISTRY_PATH


def weekly_review_date(*, as_of: str | None, latest: bool) -> date:
    if latest and as_of is not None:
        raise typer.BadParameter("--latest and --as-of cannot be combined")
    return date.today() if latest or as_of is None else parse_date(as_of)


def run_weekly_review_generate(
    *,
    as_of: str | None,
    latest: bool,
    report_index_path: Path | None,
    report_registry_path: Path,
    target_weights_path: Path,
    required_report: list[str] | None,
    output_dir: Path,
    aggregation_dir: Path,
) -> None:
    run_date = weekly_review_date(as_of=as_of, latest=latest)
    generated = datetime.now(UTC)
    aggregation = build_weekly_review_aggregation(
        as_of=run_date,
        report_index_path=report_index_path,
        report_registry_path=report_registry_path,
        target_weights_path=target_weights_path,
        required_report_ids=required_report,
        generated_at=generated,
    )
    aggregation_path = aggregation_dir / f"weekly_review_aggregation_{run_date.isoformat()}.json"
    write_weekly_review_aggregation(aggregation, aggregation_path)
    if aggregation["aggregation_status"] == "FAIL":
        typer.echo(f"ETF weekly review aggregation blocked：{aggregation_path}")
        typer.echo(f"aggregation_status={aggregation['aggregation_status']}")
        raise typer.Exit(code=1)
    payload = build_weekly_review_report(
        as_of=run_date,
        aggregation_payload=aggregation,
        generated_at=generated,
    )
    json_path = output_dir / f"weekly_review_{run_date.isoformat()}.json"
    md_path = output_dir / f"weekly_review_{run_date.isoformat()}.md"
    write_weekly_review_report(payload, json_path=json_path, markdown_path=md_path)
    typer.echo(f"ETF weekly review：{md_path}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"aggregation={aggregation_path}")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@weekly_review_app.command("aggregate")
def weekly_review_aggregate_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="weekly review aggregation 日期 YYYY-MM-DD。"),
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
    target_weights_path: Annotated[
        Path,
        typer.Option(help="ETF target weights CSV。"),
    ] = DEFAULT_ETF_TARGET_PATH,
    required_report: Annotated[
        list[str] | None,
        typer.Option("--require-report", help="配置为必需的 report_id，可重复。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option(help="weekly review aggregation 输出目录。"),
    ] = DEFAULT_ETF_WEEKLY_REVIEW_AGGREGATION_DIR,
) -> None:
    """只读聚合 ETF weekly review 所需 latest artifacts。"""
    run_date = weekly_review_date(as_of=as_of, latest=latest)
    payload = build_weekly_review_aggregation(
        as_of=run_date,
        report_index_path=report_index_path,
        report_registry_path=report_registry_path,
        target_weights_path=target_weights_path,
        required_report_ids=required_report,
    )
    json_path = output_dir / f"weekly_review_aggregation_{run_date.isoformat()}.json"
    write_weekly_review_aggregation(payload, json_path)
    typer.echo(f"ETF weekly review aggregation：{json_path}")
    typer.echo(f"aggregation_status={payload['aggregation_status']}")
    typer.echo(f"loaded_sections={len(payload['loaded_sections'])}")
    typer.echo(f"missing_sections={len(payload['missing_sections'])}")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")
    if payload["aggregation_status"] == "FAIL":
        raise typer.Exit(code=1)


@weekly_review_app.command("generate")
def weekly_review_generate_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="weekly review 日期 YYYY-MM-DD。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="使用当前日期生成 weekly review。"),
    ] = False,
    report_index_path: Annotated[
        Path | None,
        typer.Option(help="既有 report_index JSON；不传时只读扫描 report registry。"),
    ] = None,
    report_registry_path: Annotated[
        Path,
        typer.Option(help="report registry config path。"),
    ] = DEFAULT_REPORT_REGISTRY_PATH,
    target_weights_path: Annotated[
        Path,
        typer.Option(help="ETF target weights CSV。"),
    ] = DEFAULT_ETF_TARGET_PATH,
    required_report: Annotated[
        list[str] | None,
        typer.Option("--require-report", help="配置为必需的 report_id，可重复。"),
    ] = None,
    output_dir: Annotated[Path, typer.Option(help="weekly review 输出目录。")] = (
        DEFAULT_ETF_WEEKLY_REVIEW_DIR
    ),
    aggregation_dir: Annotated[
        Path,
        typer.Option(help="aggregation artifact 输出目录。"),
    ] = DEFAULT_ETF_WEEKLY_REVIEW_AGGREGATION_DIR,
) -> None:
    """生成 TRADING-068 ETF weekly portfolio review JSON/Markdown。"""
    run_weekly_review_generate(
        as_of=as_of,
        latest=latest,
        report_index_path=report_index_path,
        report_registry_path=report_registry_path,
        target_weights_path=target_weights_path,
        required_report=required_report,
        output_dir=output_dir,
        aggregation_dir=aggregation_dir,
    )


@weekly_review_app.command("run")
def weekly_review_run_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="weekly review 日期 YYYY-MM-DD。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="使用当前日期生成 weekly review。"),
    ] = False,
    report_index_path: Annotated[
        Path | None,
        typer.Option(help="既有 report_index JSON；不传时只读扫描 report registry。"),
    ] = None,
    report_registry_path: Annotated[
        Path,
        typer.Option(help="report registry config path。"),
    ] = DEFAULT_REPORT_REGISTRY_PATH,
    target_weights_path: Annotated[
        Path,
        typer.Option(help="ETF target weights CSV。"),
    ] = DEFAULT_ETF_TARGET_PATH,
    required_report: Annotated[
        list[str] | None,
        typer.Option("--require-report", help="配置为必需的 report_id，可重复。"),
    ] = None,
    output_dir: Annotated[Path, typer.Option(help="weekly review 输出目录。")] = (
        DEFAULT_ETF_WEEKLY_REVIEW_DIR
    ),
    aggregation_dir: Annotated[
        Path,
        typer.Option(help="aggregation artifact 输出目录。"),
    ] = DEFAULT_ETF_WEEKLY_REVIEW_AGGREGATION_DIR,
) -> None:
    """`generate` 的别名；只读生成 weekly review package。"""
    run_weekly_review_generate(
        as_of=as_of,
        latest=latest,
        report_index_path=report_index_path,
        report_registry_path=report_registry_path,
        target_weights_path=target_weights_path,
        required_report=required_report,
        output_dir=output_dir,
        aggregation_dir=aggregation_dir,
    )


@weekly_review_app.command("validate")
def weekly_review_validate_command(
    report_registry_path: Annotated[
        Path,
        typer.Option(help="report registry config path。"),
    ] = DEFAULT_REPORT_REGISTRY_PATH,
    output_dir: Annotated[
        Path,
        typer.Option(help="weekly review validation 输出目录。"),
    ] = DEFAULT_ETF_WEEKLY_REVIEW_VALIDATION_DIR,
) -> None:
    """生成 TRADING-068 weekly review validation gate；失败时 fail closed。"""
    generated = datetime.now(UTC)
    payload = build_weekly_review_validation_report(
        report_registry_path=report_registry_path,
        generated_at=generated,
    )
    stem = f"weekly_review_validation_{generated.date().isoformat()}"
    json_path = output_dir / f"{stem}.json"
    md_path = output_dir / f"{stem}.md"
    write_weekly_review_validation_report(payload, json_path=json_path, markdown_path=md_path)
    typer.echo(f"ETF weekly review validation gate：{md_path}")
    typer.echo(f"status={payload['status']}")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


__all__ = [
    "run_weekly_review_generate",
    "weekly_review_aggregate_command",
    "weekly_review_date",
    "weekly_review_generate_command",
    "weekly_review_run_command",
    "weekly_review_validate_command",
]
