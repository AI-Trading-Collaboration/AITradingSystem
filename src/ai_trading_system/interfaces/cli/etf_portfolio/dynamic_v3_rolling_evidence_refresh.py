from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio.dynamic_v3_outcome_accumulation import (
    DEFAULT_OUTCOME_UPDATE_DIR,
    DEFAULT_ROLLING_EVIDENCE_REFRESH_DIR,
    rolling_evidence_refresh_report_payload,
    run_rolling_evidence_refresh,
    validate_rolling_evidence_refresh_artifact,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_rescue_app,
    dynamic_v3_rolling_evidence_refresh_app,
)


@dynamic_v3_rolling_evidence_refresh_app.command("run")
def dynamic_v3_rolling_evidence_refresh_run_command(
    outcome_update_id: Annotated[
        str,
        typer.Option("--outcome-update-id", help="outcome update id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="rolling evidence refresh artifact root。"),
    ] = DEFAULT_ROLLING_EVIDENCE_REFRESH_DIR,
    outcome_update_dir: Annotated[
        Path,
        typer.Option("--outcome-update-dir", help="outcome update artifact root。"),
    ] = DEFAULT_OUTCOME_UPDATE_DIR,
) -> None:
    """刷新 outcome update 下游 evidence artifacts。"""
    result = run_rolling_evidence_refresh(
        outcome_update_id=outcome_update_id,
        output_dir=output_dir,
        outcome_update_dir=outcome_update_dir,
    )
    refreshed = result["refreshed_artifacts"]
    delta = result["evidence_delta_summary"]
    typer.echo(f"refresh_id={result['refresh_id']}")
    typer.echo(f"refresh_dir={result['refresh_dir']}")
    typer.echo(f"transaction_status={result['transaction']['status']}")
    typer.echo(f"outcome_dashboard_id={refreshed['outcome_dashboard_id']}")
    typer.echo(f"limited_vs_notrade_id={refreshed['limited_vs_notrade_id']}")
    typer.echo(f"consensus_risk_id={refreshed['consensus_risk_id']}")
    typer.echo(f"weekly_advisory_review_id={refreshed['weekly_advisory_review_id']}")
    typer.echo(f"material_change={delta['material_change']}")
    typer.echo("production_effect=none")
    typer.echo("broker_action_taken=false")


@dynamic_v3_rolling_evidence_refresh_app.command("report")
def dynamic_v3_rolling_evidence_refresh_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest rolling evidence refresh。"),
    ] = False,
    refresh_id: Annotated[str | None, typer.Option("--refresh-id", help="refresh id。")] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="rolling evidence refresh artifact root。"),
    ] = DEFAULT_ROLLING_EVIDENCE_REFRESH_DIR,
) -> None:
    """展示 rolling evidence refresh 摘要。"""
    payload = rolling_evidence_refresh_report_payload(
        refresh_id=refresh_id,
        latest=latest,
        output_dir=output_dir,
    )
    delta = payload["evidence_delta_summary"]
    after = delta["after"]
    typer.echo(f"refresh_id={payload['refresh_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"transaction_status={payload['transaction']['status']}")
    typer.echo(f"forward_available={after['forward_available']}")
    typer.echo(f"forward_pending={after['forward_pending']}")
    typer.echo(f"limited_vs_notrade_confidence={after['limited_vs_notrade_confidence']}")
    typer.echo(f"consensus_target_risk={after['consensus_target_risk']}")
    typer.echo(f"material_change={delta['material_change']}")
    typer.echo(f"report_path={payload['rolling_evidence_refresh_report_path']}")
    typer.echo("production_effect=none")


@dynamic_v3_rescue_app.command("validate-rolling-evidence-refresh")
def dynamic_v3_validate_rolling_evidence_refresh_command(
    refresh_id: Annotated[str, typer.Option("--refresh-id", help="refresh id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="rolling evidence refresh artifact root。"),
    ] = DEFAULT_ROLLING_EVIDENCE_REFRESH_DIR,
) -> None:
    """校验 TRADING-158 rolling evidence refresh artifact。"""
    payload = validate_rolling_evidence_refresh_artifact(
        refresh_id=refresh_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_effect=none")
    typer.echo("broker_action_taken=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


__all__ = [
    "dynamic_v3_rolling_evidence_refresh_run_command",
    "dynamic_v3_rolling_evidence_refresh_report_command",
    "dynamic_v3_validate_rolling_evidence_refresh_command",
]
