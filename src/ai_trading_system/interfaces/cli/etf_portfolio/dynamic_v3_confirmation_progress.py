from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio.dynamic_v3_confirmation_cycle import (
    DEFAULT_CONFIRMATION_PROGRESS_DIR,
    DEFAULT_CONFIRMATION_REGISTRY_DIR,
    confirmation_progress_report_payload,
    update_confirmation_progress,
    validate_confirmation_progress_artifact,
)
from ai_trading_system.etf_portfolio.dynamic_v3_outcome_accumulation import (
    DEFAULT_CONSENSUS_RISK_DIR,
    DEFAULT_LIMITED_VS_NOTRADE_DIR,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_confirmation_progress_app,
    dynamic_v3_rescue_app,
)


@dynamic_v3_confirmation_progress_app.command("update")
def dynamic_v3_confirmation_progress_update_command(
    registry_id: Annotated[
        str,
        typer.Option("--registry-id", "--registry_id", help="registry id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="confirmation progress artifact root。"),
    ] = DEFAULT_CONFIRMATION_PROGRESS_DIR,
    registry_dir: Annotated[
        Path,
        typer.Option("--registry-dir", help="confirmation registry artifact root。"),
    ] = DEFAULT_CONFIRMATION_REGISTRY_DIR,
    limited_vs_notrade_dir: Annotated[
        Path,
        typer.Option("--limited-vs-notrade-dir", help="limited-vs-notrade artifact root。"),
    ] = DEFAULT_LIMITED_VS_NOTRADE_DIR,
    consensus_risk_dir: Annotated[
        Path,
        typer.Option("--consensus-risk-dir", help="consensus-risk artifact root。"),
    ] = DEFAULT_CONSENSUS_RISK_DIR,
) -> None:
    """更新 TRADING-175 confirmation target progress。"""
    result = update_confirmation_progress(
        registry_id=registry_id,
        registry_dir=registry_dir,
        output_dir=output_dir,
        limited_vs_notrade_dir=limited_vs_notrade_dir,
        consensus_risk_dir=consensus_risk_dir,
    )
    summary = result["target_progress_summary"]
    typer.echo(f"progress_id={result['progress_id']}")
    typer.echo(f"progress_dir={result['progress_dir']}")
    typer.echo(f"status={result['manifest']['status']}")
    typer.echo(f"ready_for_evaluation_count={summary['ready_for_evaluation_count']}")
    typer.echo(f"insufficient_events_count={summary['insufficient_events_count']}")
    typer.echo(f"summary_recommendation={summary['summary_recommendation']}")
    typer.echo("auto_apply=false")
    typer.echo("production_effect=none")


@dynamic_v3_confirmation_progress_app.command("report")
def dynamic_v3_confirmation_progress_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest progress。"),
    ] = False,
    progress_id: Annotated[
        str | None,
        typer.Option("--progress-id", "--progress_id", help="progress id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="confirmation progress artifact root。"),
    ] = DEFAULT_CONFIRMATION_PROGRESS_DIR,
) -> None:
    """展示 TRADING-175 confirmation progress 摘要。"""
    payload = confirmation_progress_report_payload(
        progress_id=progress_id,
        latest=latest,
        output_dir=output_dir,
    )
    summary = payload["target_progress_summary"]
    typer.echo(f"progress_id={payload['progress_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"ready_for_evaluation_count={summary['ready_for_evaluation_count']}")
    typer.echo(f"insufficient_events_count={summary['insufficient_events_count']}")
    typer.echo(f"report_path={payload['confirmation_progress_report_path']}")
    typer.echo("production_effect=none")


@dynamic_v3_rescue_app.command("validate-confirmation-progress")
def dynamic_v3_validate_confirmation_progress_command(
    progress_id: Annotated[
        str,
        typer.Option("--progress-id", "--progress_id", help="progress id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="confirmation progress artifact root。"),
    ] = DEFAULT_CONFIRMATION_PROGRESS_DIR,
) -> None:
    """校验 TRADING-175 confirmation progress artifact。"""
    payload = validate_confirmation_progress_artifact(
        progress_id=progress_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_effect=none")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


__all__ = [
    "dynamic_v3_confirmation_progress_report_command",
    "dynamic_v3_confirmation_progress_update_command",
    "dynamic_v3_validate_confirmation_progress_command",
]
