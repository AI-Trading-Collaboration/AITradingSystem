from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio.dynamic_v3_paper_tracking import (
    DEFAULT_ADVISORY_OUTCOME_DIR,
    DEFAULT_PAPER_PORTFOLIO_DIR,
    DEFAULT_SHADOW_AGING_DIR,
    DEFAULT_WEEKLY_ADVISORY_REVIEW_DIR,
    run_weekly_advisory_review,
    validate_weekly_advisory_review_artifact,
    weekly_advisory_review_report_payload,
)
from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    DEFAULT_OWNER_REVIEW_JOURNAL_DIR,
    DEFAULT_POSITION_ADVISORY_DAILY_DIR,
    DEFAULT_SHADOW_MONITOR_RUN_DIR,
)
from ai_trading_system.interfaces.cli.etf_portfolio.common import parse_date
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_rescue_app,
    dynamic_v3_weekly_advisory_review_app,
)


@dynamic_v3_weekly_advisory_review_app.command("run")
def dynamic_v3_weekly_advisory_review_run_command(
    week_ending: Annotated[str, typer.Option("--week-ending", help="week ending date。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="weekly advisory review artifact root。"),
    ] = DEFAULT_WEEKLY_ADVISORY_REVIEW_DIR,
    shadow_monitor_run_dir: Annotated[
        Path,
        typer.Option("--shadow-monitor-run-dir", help="shadow monitor run artifact root。"),
    ] = DEFAULT_SHADOW_MONITOR_RUN_DIR,
    daily_advisory_dir: Annotated[
        Path,
        typer.Option("--daily-advisory-dir", help="daily advisory artifact root。"),
    ] = DEFAULT_POSITION_ADVISORY_DAILY_DIR,
    owner_review_dir: Annotated[
        Path,
        typer.Option("--owner-review-dir", help="owner review journal root。"),
    ] = DEFAULT_OWNER_REVIEW_JOURNAL_DIR,
    paper_portfolio_dir: Annotated[
        Path,
        typer.Option("--paper-portfolio-dir", help="paper portfolio artifact root。"),
    ] = DEFAULT_PAPER_PORTFOLIO_DIR,
    advisory_outcome_dir: Annotated[
        Path,
        typer.Option("--advisory-outcome-dir", help="advisory outcome artifact root。"),
    ] = DEFAULT_ADVISORY_OUTCOME_DIR,
    shadow_aging_dir: Annotated[
        Path,
        typer.Option("--shadow-aging-dir", help="shadow aging artifact root。"),
    ] = DEFAULT_SHADOW_AGING_DIR,
) -> None:
    """生成 TRADING-140 weekly advisory review。"""
    result = run_weekly_advisory_review(
        week_ending=parse_date(week_ending),
        output_dir=output_dir,
        shadow_monitor_run_dir=shadow_monitor_run_dir,
        daily_advisory_dir=daily_advisory_dir,
        owner_review_dir=owner_review_dir,
        paper_portfolio_dir=paper_portfolio_dir,
        advisory_outcome_dir=advisory_outcome_dir,
        shadow_aging_dir=shadow_aging_dir,
    )
    manifest = result["manifest"]
    typer.echo(f"weekly_review_id={result['weekly_review_id']}")
    typer.echo(f"weekly_review_dir={result['weekly_review_dir']}")
    typer.echo(f"weekly_recommendation={manifest['weekly_recommendation']}")
    typer.echo(f"paper_portfolio_status={manifest['paper_portfolio_status']}")
    typer.echo("broker_action_taken=false")


@dynamic_v3_weekly_advisory_review_app.command("report")
def dynamic_v3_weekly_advisory_review_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest weekly advisory review。"),
    ] = False,
    weekly_review_id: Annotated[
        str | None,
        typer.Option("--weekly-review-id", help="weekly review id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="weekly advisory review artifact root。"),
    ] = DEFAULT_WEEKLY_ADVISORY_REVIEW_DIR,
) -> None:
    """展示 TRADING-140 weekly advisory review 摘要。"""
    payload = weekly_advisory_review_report_payload(
        weekly_review_id=weekly_review_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"weekly_review_id={payload['weekly_review_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"weekly_recommendation={payload['weekly_recommendation']}")
    typer.echo(f"paper_portfolio_status={payload['paper_portfolio_status']}")
    typer.echo(f"report_path={payload['weekly_review_report_path']}")
    typer.echo("broker_action_taken=false")


@dynamic_v3_rescue_app.command("validate-weekly-advisory-review")
def dynamic_v3_validate_weekly_advisory_review_command(
    weekly_review_id: Annotated[
        str,
        typer.Option("--weekly-review-id", help="weekly review id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="weekly advisory review artifact root。"),
    ] = DEFAULT_WEEKLY_ADVISORY_REVIEW_DIR,
) -> None:
    """校验 TRADING-140 weekly advisory review artifact。"""
    payload = validate_weekly_advisory_review_artifact(
        weekly_review_id=weekly_review_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_taken=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


__all__ = [
    "dynamic_v3_validate_weekly_advisory_review_command",
    "dynamic_v3_weekly_advisory_review_report_command",
    "dynamic_v3_weekly_advisory_review_run_command",
]
