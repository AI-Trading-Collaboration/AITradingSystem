from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio.dynamic_v3_outcome_accumulation import (
    DEFAULT_OUTCOME_DUE_DIR,
    DEFAULT_OUTCOME_UPDATE_REVIEW_DIR,
    outcome_update_review_report_payload,
    run_outcome_update_review,
    validate_outcome_update_review_artifact,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_outcome_update_review_app,
    dynamic_v3_rescue_app,
)


@dynamic_v3_outcome_update_review_app.command("run")
def dynamic_v3_outcome_update_review_run_command(
    due_id: Annotated[str, typer.Option("--due-id", help="outcome due id。")],
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="outcome update review artifact root。")
    ] = DEFAULT_OUTCOME_UPDATE_REVIEW_DIR,
    outcome_due_dir: Annotated[
        Path, typer.Option("--outcome-due-dir", help="outcome due artifact root。")
    ] = DEFAULT_OUTCOME_DUE_DIR,
) -> None:
    """生成 validated update-ready 人工复核包。"""
    result = run_outcome_update_review(
        due_id=due_id, output_dir=output_dir, outcome_due_dir=outcome_due_dir
    )
    safety = result["update_safety_checks"]
    typer.echo(f"update_review_id={result['update_review_id']}")
    typer.echo(f"review_dir={result['review_dir']}")
    typer.echo(f"status={result['manifest']['status']}")
    typer.echo(f"ready_to_update_count={safety['ready_to_update_count']}")
    typer.echo(f"blocked_count={safety['blocked_count']}")
    typer.echo(f"future_data_used_in_decision={safety['future_data_used_in_decision']}")
    typer.echo("production_effect=none")
    typer.echo("broker_action_taken=false")


@dynamic_v3_outcome_update_review_app.command("report")
def dynamic_v3_outcome_update_review_report_command(
    latest: Annotated[
        bool, typer.Option("--latest/--no-latest", help="读取 latest outcome update review。")
    ] = False,
    review_id: Annotated[str | None, typer.Option("--review-id", help="review id。")]=None,
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="outcome update review artifact root。")
    ] = DEFAULT_OUTCOME_UPDATE_REVIEW_DIR,
) -> None:
    """展示 update-ready review 摘要。"""
    payload = outcome_update_review_report_payload(
        review_id=review_id, latest=latest, output_dir=output_dir
    )
    safety = payload["update_safety_checks"]
    typer.echo(f"update_review_id={payload['update_review_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"ready_to_update_count={safety['ready_to_update_count']}")
    typer.echo(f"blocked_count={safety['blocked_count']}")
    typer.echo(f"future_data_used_in_decision={safety['future_data_used_in_decision']}")
    typer.echo(f"report_path={payload['outcome_update_review_report_path']}")
    typer.echo("production_effect=none")


@dynamic_v3_rescue_app.command("validate-outcome-update-review")
def dynamic_v3_validate_outcome_update_review_command(
    review_id: Annotated[str, typer.Option("--review-id", help="review id。")],
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="outcome update review artifact root。")
    ] = DEFAULT_OUTCOME_UPDATE_REVIEW_DIR,
) -> None:
    """校验 TRADING-156 outcome update review artifact。"""
    payload = validate_outcome_update_review_artifact(
        review_id=review_id, output_dir=output_dir
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_effect=none")
    typer.echo("broker_action_taken=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


__all__ = [
    "dynamic_v3_outcome_update_review_run_command",
    "dynamic_v3_outcome_update_review_report_command",
    "dynamic_v3_validate_outcome_update_review_command",
]
