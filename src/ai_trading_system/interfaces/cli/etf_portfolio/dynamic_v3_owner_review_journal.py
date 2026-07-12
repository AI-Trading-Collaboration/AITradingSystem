from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    DEFAULT_OWNER_REVIEW_JOURNAL_DIR,
    DEFAULT_POSITION_ADVISORY_DAILY_DIR,
    create_owner_review,
    owner_review_report_payload,
    owner_review_summary,
    record_owner_review_decision,
    validate_owner_review_artifact,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_owner_review_app,
    dynamic_v3_rescue_app,
)


@dynamic_v3_owner_review_app.command("create")
def dynamic_v3_owner_review_create_command(
    daily_advisory_id: Annotated[
        str,
        typer.Option("--daily-advisory-id", help="daily advisory id。"),
    ],
    daily_advisory_dir: Annotated[
        Path,
        typer.Option("--daily-advisory-dir", help="daily advisory artifact root。"),
    ] = DEFAULT_POSITION_ADVISORY_DAILY_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="owner review journal root。"),
    ] = DEFAULT_OWNER_REVIEW_JOURNAL_DIR,
) -> None:
    """创建 TRADING-135 owner review record。"""
    result = create_owner_review(
        daily_advisory_id=daily_advisory_id,
        daily_advisory_dir=daily_advisory_dir,
        output_dir=output_dir,
    )
    review = result["review"]
    typer.echo(f"review_id={result['review_id']}")
    typer.echo(f"owner_decision={review['owner_decision']}")
    typer.echo(f"journal_dir={result['journal_dir']}")
    typer.echo("broker_action_taken=false")


@dynamic_v3_owner_review_app.command("list")
def dynamic_v3_owner_review_list_command(
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="owner review journal root。"),
    ] = DEFAULT_OWNER_REVIEW_JOURNAL_DIR,
) -> None:
    """列出 TRADING-135 owner review journal 摘要。"""
    payload = owner_review_summary(output_dir=output_dir)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"review_count={payload['review_count']}")
    typer.echo(f"pending_owner_review_count={payload['pending_owner_review_count']}")
    typer.echo(f"latest_review_id={payload['latest_review_id']}")
    typer.echo("broker_action_taken=false")


@dynamic_v3_owner_review_app.command("report")
def dynamic_v3_owner_review_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest owner review。"),
    ] = False,
    review_id: Annotated[str | None, typer.Option("--review-id", help="owner review id。")] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="owner review journal root。"),
    ] = DEFAULT_OWNER_REVIEW_JOURNAL_DIR,
) -> None:
    """展示 TRADING-135 owner review report。"""
    payload = owner_review_report_payload(
        latest=latest,
        review_id=review_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"latest_review_id={payload['latest_review_id']}")
    typer.echo(f"latest_owner_decision={payload['latest_owner_decision']}")
    typer.echo(f"pending_owner_review_count={payload['pending_owner_review_count']}")
    typer.echo(f"report_path={payload['owner_review_report_path']}")
    typer.echo("broker_action_taken=false")


@dynamic_v3_owner_review_app.command("record-decision")
def dynamic_v3_owner_review_record_decision_command(
    review_id: Annotated[str, typer.Option("--review-id", help="owner review id。")],
    decision: Annotated[str, typer.Option("--decision", help="owner decision。")],
    manual_notes: Annotated[
        str,
        typer.Option("--manual-notes", help="manual owner notes。"),
    ] = "",
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="owner review journal root。"),
    ] = DEFAULT_OWNER_REVIEW_JOURNAL_DIR,
    daily_advisory_dir: Annotated[
        Path,
        typer.Option("--daily-advisory-dir", help="daily advisory artifact root。"),
    ] = DEFAULT_POSITION_ADVISORY_DAILY_DIR,
) -> None:
    """记录 TRADING-135 owner decision。"""
    result = record_owner_review_decision(
        review_id=review_id,
        decision=decision,
        manual_notes=manual_notes,
        output_dir=output_dir,
        daily_advisory_dir=daily_advisory_dir,
    )
    review = result["review"]
    typer.echo(f"review_id={result['review_id']}")
    typer.echo(f"owner_decision={review['owner_decision']}")
    typer.echo(f"broker_action_taken={review['broker_action_taken']}")


@dynamic_v3_rescue_app.command("validate-owner-review")
def dynamic_v3_validate_owner_review_command(
    review_id: Annotated[str, typer.Option("--review-id", help="owner review id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="owner review journal root。"),
    ] = DEFAULT_OWNER_REVIEW_JOURNAL_DIR,
) -> None:
    """校验 TRADING-135 owner review record。"""
    payload = validate_owner_review_artifact(review_id=review_id, output_dir=output_dir)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_taken=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)
