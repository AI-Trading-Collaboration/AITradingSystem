from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio.dynamic_v3_real_snapshot import (
    DEFAULT_REAL_EXECUTION_OWNER_REVIEW_DIR,
    create_real_execution_owner_review,
    real_execution_owner_review_report_payload,
    record_real_execution_owner_decision,
    validate_real_execution_owner_review,
)
from ai_trading_system.interfaces.cli.etf_portfolio.common import mapping_obj
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_real_execution_owner_review_app,
    dynamic_v3_rescue_app,
)


@dynamic_v3_real_execution_owner_review_app.command("create")
def dynamic_v3_real_execution_owner_review_create_command(
    dry_run_id: Annotated[str, typer.Option("--dry-run-id", help="dry run id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="real execution owner review artifact root。"),
    ] = DEFAULT_REAL_EXECUTION_OWNER_REVIEW_DIR,
) -> None:
    """从 TRADING-205 dry run 创建 TRADING-206 owner review。"""
    result = create_real_execution_owner_review(dry_run_id=dry_run_id, output_dir=output_dir)
    manifest = result["manifest"]
    typer.echo(f"review_id={result['review_id']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(f"recommended_action={manifest['recommended_action']}")
    typer.echo(f"owner_decision={manifest['owner_decision']}")
    typer.echo("broker_action_taken=false")


@dynamic_v3_real_execution_owner_review_app.command("record")
def dynamic_v3_real_execution_owner_review_record_command(
    review_id: Annotated[str, typer.Option("--review-id", help="owner review id。")],
    decision: Annotated[str, typer.Option("--decision", help="owner decision。")],
    owner_notes: Annotated[
        str,
        typer.Option("--owner-notes", help="owner notes; do not include sensitive account data。"),
    ] = "",
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="real execution owner review artifact root。"),
    ] = DEFAULT_REAL_EXECUTION_OWNER_REVIEW_DIR,
) -> None:
    """记录 TRADING-206 owner decision。"""
    result = record_real_execution_owner_decision(
        review_id=review_id,
        decision=decision,
        owner_notes=owner_notes,
        output_dir=output_dir,
    )
    payload = result["decision"]
    typer.echo(f"review_id={review_id}")
    typer.echo(f"owner_decision={payload['owner_decision']}")
    typer.echo("broker_action_taken=false")
    typer.echo("order_ticket_generated=false")


@dynamic_v3_real_execution_owner_review_app.command("report")
def dynamic_v3_real_execution_owner_review_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest owner execution review。"),
    ] = False,
    review_id: Annotated[str | None, typer.Option("--review-id", help="owner review id。")] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="real execution owner review artifact root。"),
    ] = DEFAULT_REAL_EXECUTION_OWNER_REVIEW_DIR,
) -> None:
    """展示 TRADING-206 owner review 摘要。"""
    payload = real_execution_owner_review_report_payload(
        review_id=review_id,
        latest=latest,
        output_dir=output_dir,
    )
    decision = mapping_obj(payload.get("owner_execution_decision"))
    typer.echo(f"review_id={payload['review_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"owner_decision={decision.get('owner_decision')}")
    typer.echo(f"recommended_action={decision.get('recommended_action')}")
    typer.echo(f"report_path={payload['real_execution_owner_review_report_path']}")


@dynamic_v3_rescue_app.command("validate-real-execution-owner-review")
def dynamic_v3_validate_real_execution_owner_review_command(
    review_id: Annotated[str, typer.Option("--review-id", help="owner review id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="real execution owner review artifact root。"),
    ] = DEFAULT_REAL_EXECUTION_OWNER_REVIEW_DIR,
) -> None:
    """校验 TRADING-206 owner review artifact。"""
    payload = validate_real_execution_owner_review(review_id=review_id, output_dir=output_dir)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_taken=false")
    typer.echo("order_ticket_generated=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)
