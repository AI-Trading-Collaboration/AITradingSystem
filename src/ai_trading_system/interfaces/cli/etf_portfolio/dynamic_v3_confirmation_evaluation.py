from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio.dynamic_v3_confirmation_cycle import (
    DEFAULT_CONFIRMATION_EVALUATION_DIR,
    DEFAULT_CONFIRMATION_PROGRESS_DIR,
    confirmation_evaluation_report_payload,
    run_confirmation_evaluation,
    validate_confirmation_evaluation_artifact,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_confirmation_evaluate_app,
    dynamic_v3_rescue_app,
)


@dynamic_v3_confirmation_evaluate_app.command("run")
def dynamic_v3_confirmation_evaluate_run_command(
    progress_id: Annotated[
        str,
        typer.Option("--progress-id", "--progress_id", help="progress id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="confirmation evaluation artifact root。"),
    ] = DEFAULT_CONFIRMATION_EVALUATION_DIR,
    progress_dir: Annotated[
        Path,
        typer.Option("--progress-dir", help="confirmation progress artifact root。"),
    ] = DEFAULT_CONFIRMATION_PROGRESS_DIR,
) -> None:
    """运行 TRADING-176 success/failure condition evaluation。"""
    result = run_confirmation_evaluation(
        progress_id=progress_id,
        progress_dir=progress_dir,
        output_dir=output_dir,
    )
    summary = result["confirmation_evaluation_summary"]
    typer.echo(f"evaluation_id={result['evaluation_id']}")
    typer.echo(f"evaluation_dir={result['evaluation_dir']}")
    typer.echo(f"status={result['manifest']['status']}")
    typer.echo(f"success_count={summary['success_count']}")
    typer.echo(f"failure_count={summary['failure_count']}")
    typer.echo(f"not_ready_count={summary['not_ready_count']}")
    typer.echo("auto_apply=false")
    typer.echo("production_effect=none")


@dynamic_v3_confirmation_evaluate_app.command("report")
def dynamic_v3_confirmation_evaluate_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest evaluation。"),
    ] = False,
    evaluation_id: Annotated[
        str | None,
        typer.Option("--evaluation-id", "--evaluation_id", help="evaluation id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="confirmation evaluation artifact root。"),
    ] = DEFAULT_CONFIRMATION_EVALUATION_DIR,
) -> None:
    """展示 TRADING-176 confirmation evaluation 摘要。"""
    payload = confirmation_evaluation_report_payload(
        evaluation_id=evaluation_id,
        latest=latest,
        output_dir=output_dir,
    )
    summary = payload["confirmation_evaluation_summary"]
    typer.echo(f"evaluation_id={payload['evaluation_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"success_count={summary['success_count']}")
    typer.echo(f"failure_count={summary['failure_count']}")
    typer.echo(f"not_ready_count={summary['not_ready_count']}")
    typer.echo(f"report_path={payload['confirmation_evaluation_report_path']}")
    typer.echo("production_effect=none")


@dynamic_v3_rescue_app.command("validate-confirmation-evaluate")
def dynamic_v3_validate_confirmation_evaluate_command(
    evaluation_id: Annotated[
        str,
        typer.Option("--evaluation-id", "--evaluation_id", help="evaluation id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="confirmation evaluation artifact root。"),
    ] = DEFAULT_CONFIRMATION_EVALUATION_DIR,
) -> None:
    """校验 TRADING-176 confirmation evaluation artifact。"""
    payload = validate_confirmation_evaluation_artifact(
        evaluation_id=evaluation_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_effect=none")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


__all__ = [
    "dynamic_v3_confirmation_evaluate_report_command",
    "dynamic_v3_confirmation_evaluate_run_command",
    "dynamic_v3_validate_confirmation_evaluate_command",
]
