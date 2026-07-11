from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio.dynamic_v3_real_snapshot import (
    DEFAULT_WEEKLY_REAL_SNAPSHOT_REVIEW_DIR,
    run_weekly_real_snapshot_review,
    validate_weekly_real_snapshot_review,
    weekly_real_snapshot_review_report_payload,
)
from ai_trading_system.interfaces.cli.etf_portfolio.common import mapping_obj, parse_date
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_rescue_app,
    dynamic_v3_weekly_real_snapshot_review_app,
)


@dynamic_v3_weekly_real_snapshot_review_app.command("run")
def dynamic_v3_weekly_real_snapshot_review_run_command(
    week_ending: Annotated[
        str,
        typer.Option("--week-ending", help="week ending date YYYY-MM-DD。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="weekly real snapshot review artifact root。"),
    ] = DEFAULT_WEEKLY_REAL_SNAPSHOT_REVIEW_DIR,
) -> None:
    """生成 TRADING-208 weekly real snapshot advisory review。"""
    result = run_weekly_real_snapshot_review(
        week_ending=parse_date(week_ending),
        output_dir=output_dir,
    )
    summary = result["weekly_real_snapshot_summary"]
    typer.echo(f"weekly_real_review_id={result['weekly_real_review_id']}")
    typer.echo(f"snapshot_status={summary['snapshot_status']}")
    typer.echo(f"owner_decision={summary['owner_decision']}")
    typer.echo(f"next_action={summary['next_action']}")
    typer.echo("broker_action_taken=false")
    typer.echo("order_ticket_generated=false")


@dynamic_v3_weekly_real_snapshot_review_app.command("report")
def dynamic_v3_weekly_real_snapshot_review_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest weekly real snapshot review。"),
    ] = False,
    weekly_real_review_id: Annotated[
        str | None,
        typer.Option("--weekly-real-review-id", help="weekly real snapshot review id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="weekly real snapshot review artifact root。"),
    ] = DEFAULT_WEEKLY_REAL_SNAPSHOT_REVIEW_DIR,
) -> None:
    """展示 TRADING-208 weekly real snapshot advisory review 摘要。"""
    payload = weekly_real_snapshot_review_report_payload(
        weekly_real_review_id=weekly_real_review_id,
        latest=latest,
        output_dir=output_dir,
    )
    summary = mapping_obj(payload.get("weekly_real_snapshot_summary"))
    typer.echo(f"weekly_real_review_id={payload['weekly_real_review_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"owner_decision={summary.get('owner_decision')}")
    typer.echo(f"next_action={summary.get('next_action')}")
    typer.echo(f"report_path={payload['weekly_real_snapshot_review_report_path']}")


@dynamic_v3_rescue_app.command("validate-weekly-real-snapshot-review")
def dynamic_v3_validate_weekly_real_snapshot_review_command(
    weekly_real_review_id: Annotated[
        str,
        typer.Option("--weekly-real-review-id", help="weekly real snapshot review id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="weekly real snapshot review artifact root。"),
    ] = DEFAULT_WEEKLY_REAL_SNAPSHOT_REVIEW_DIR,
) -> None:
    """校验 TRADING-208 weekly real snapshot advisory review artifact。"""
    payload = validate_weekly_real_snapshot_review(
        weekly_real_review_id=weekly_real_review_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_taken=false")
    typer.echo("order_ticket_generated=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)
