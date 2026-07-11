from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    DEFAULT_EXECUTION_GUARDRAILS_DIR,
    DEFAULT_MANUAL_EXECUTION_REVIEW_DIR,
    DEFAULT_MANUAL_PORTFOLIO_SNAPSHOT_DIR,
    DEFAULT_PORTFOLIO_EXPOSURE_DIR,
    DEFAULT_POSITION_DRIFT_DIR,
    build_manual_execution_review_pack,
    manual_execution_review_report_payload,
    validate_manual_execution_review_artifact,
)
from ai_trading_system.interfaces.cli.etf_portfolio.common import mapping_obj
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_manual_execution_review_app,
    dynamic_v3_rescue_app,
)


@dynamic_v3_manual_execution_review_app.command("pack")
def dynamic_v3_manual_execution_review_pack_command(
    snapshot_id: Annotated[str, typer.Option("--snapshot-id", help="manual snapshot id。")],
    exposure_id: Annotated[str, typer.Option("--exposure-id", help="portfolio exposure id。")],
    drift_id: Annotated[str, typer.Option("--drift-id", help="position drift id。")],
    guardrail_id: Annotated[str, typer.Option("--guardrail-id", help="guardrail id。")],
    snapshot_dir: Annotated[
        Path,
        typer.Option("--snapshot-dir", help="manual portfolio snapshot artifact root。"),
    ] = DEFAULT_MANUAL_PORTFOLIO_SNAPSHOT_DIR,
    exposure_dir: Annotated[
        Path,
        typer.Option("--exposure-dir", help="portfolio exposure artifact root。"),
    ] = DEFAULT_PORTFOLIO_EXPOSURE_DIR,
    drift_dir: Annotated[
        Path,
        typer.Option("--drift-dir", help="position drift artifact root。"),
    ] = DEFAULT_POSITION_DRIFT_DIR,
    guardrail_dir: Annotated[
        Path,
        typer.Option("--guardrail-dir", help="execution guardrails artifact root。"),
    ] = DEFAULT_EXECUTION_GUARDRAILS_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="manual execution review artifact root。"),
    ] = DEFAULT_MANUAL_EXECUTION_REVIEW_DIR,
) -> None:
    """生成 TRADING-203 manual execution review pack。"""
    result = build_manual_execution_review_pack(
        snapshot_id=snapshot_id,
        exposure_id=exposure_id,
        drift_id=drift_id,
        guardrail_id=guardrail_id,
        snapshot_dir=snapshot_dir,
        exposure_dir=exposure_dir,
        drift_dir=drift_dir,
        guardrail_dir=guardrail_dir,
        output_dir=output_dir,
    )
    decision = result["manual_execution_decision"]
    typer.echo(f"manual_review_id={result['manual_review_id']}")
    typer.echo(f"recommended_action={decision['recommended_action']}")
    typer.echo(f"order_ticket_generated={decision['order_ticket_generated']}")
    typer.echo(f"owner_approval_required={decision['owner_approval_required']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_manual_execution_review_app.command("report")
def dynamic_v3_manual_execution_review_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest manual execution review。"),
    ] = False,
    review_id: Annotated[
        str | None,
        typer.Option("--review-id", "--manual-review-id", help="manual review id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="manual execution review artifact root。"),
    ] = DEFAULT_MANUAL_EXECUTION_REVIEW_DIR,
) -> None:
    """展示 TRADING-203 manual execution review 摘要。"""
    payload = manual_execution_review_report_payload(
        review_id=review_id,
        latest=latest,
        output_dir=output_dir,
    )
    decision = mapping_obj(payload.get("manual_execution_decision"))
    typer.echo(f"manual_review_id={payload['manual_review_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"recommended_action={decision.get('recommended_action')}")
    typer.echo(f"order_ticket_generated={decision.get('order_ticket_generated')}")
    typer.echo(f"owner_approval_required={decision.get('owner_approval_required')}")
    typer.echo(f"report_path={payload['manual_execution_review_report_path']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_rescue_app.command("validate-manual-execution-review")
def dynamic_v3_validate_manual_execution_review_command(
    review_id: Annotated[
        str,
        typer.Option("--review-id", "--manual-review-id", help="manual review id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="manual execution review artifact root。"),
    ] = DEFAULT_MANUAL_EXECUTION_REVIEW_DIR,
) -> None:
    """校验 TRADING-203 manual execution review artifact。"""
    payload = validate_manual_execution_review_artifact(review_id=review_id, output_dir=output_dir)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("order_ticket_generated=false")
    typer.echo("broker_action_allowed=false")
    typer.echo("owner_approval_required=true")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)
