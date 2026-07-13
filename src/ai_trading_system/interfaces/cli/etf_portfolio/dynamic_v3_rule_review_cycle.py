from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio.dynamic_v3_confirmation_cycle import (
    DEFAULT_CONFIRMATION_EVALUATION_DIR,
    DEFAULT_CONFIRMATION_PROGRESS_DIR,
    DEFAULT_CONFIRMATION_REGISTRY_DIR,
    DEFAULT_RULE_REVIEW_CYCLE_DIR,
    rule_review_cycle_report_payload,
    run_rule_review_cycle,
    validate_rule_review_cycle_artifact,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_rescue_app,
    dynamic_v3_rule_review_cycle_app,
)


@dynamic_v3_rule_review_cycle_app.command("run")
def dynamic_v3_rule_review_cycle_run_command(
    registry_id: Annotated[
        str,
        typer.Option("--registry-id", "--registry_id", help="registry id。"),
    ],
    progress_id: Annotated[
        str,
        typer.Option("--progress-id", "--progress_id", help="progress id。"),
    ],
    evaluation_id: Annotated[
        str,
        typer.Option("--evaluation-id", "--evaluation_id", help="evaluation id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="rule review cycle artifact root。"),
    ] = DEFAULT_RULE_REVIEW_CYCLE_DIR,
    registry_dir: Annotated[
        Path,
        typer.Option("--registry-dir", help="confirmation registry artifact root。"),
    ] = DEFAULT_CONFIRMATION_REGISTRY_DIR,
    progress_dir: Annotated[
        Path,
        typer.Option("--progress-dir", help="confirmation progress artifact root。"),
    ] = DEFAULT_CONFIRMATION_PROGRESS_DIR,
    evaluation_dir: Annotated[
        Path,
        typer.Option("--evaluation-dir", help="confirmation evaluation artifact root。"),
    ] = DEFAULT_CONFIRMATION_EVALUATION_DIR,
) -> None:
    """生成 TRADING-177 rule review cycle report。"""
    result = run_rule_review_cycle(
        registry_id=registry_id,
        progress_id=progress_id,
        evaluation_id=evaluation_id,
        registry_dir=registry_dir,
        progress_dir=progress_dir,
        evaluation_dir=evaluation_dir,
        output_dir=output_dir,
    )
    manifest = result["manifest"]
    typer.echo(f"cycle_id={result['cycle_id']}")
    typer.echo(f"cycle_dir={result['cycle_dir']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(f"cycle_recommendation={manifest['cycle_recommendation']}")
    typer.echo(f"targets_requiring_owner_action={manifest['targets_requiring_owner_action']}")
    typer.echo("policy_change_allowed=false")
    typer.echo("auto_apply=false")
    typer.echo("production_effect=none")


@dynamic_v3_rule_review_cycle_app.command("report")
def dynamic_v3_rule_review_cycle_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest rule review cycle。"),
    ] = False,
    cycle_id: Annotated[
        str | None,
        typer.Option("--cycle-id", "--cycle_id", help="cycle id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="rule review cycle artifact root。"),
    ] = DEFAULT_RULE_REVIEW_CYCLE_DIR,
) -> None:
    """展示 TRADING-177 rule review cycle 摘要。"""
    payload = rule_review_cycle_report_payload(
        cycle_id=cycle_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"cycle_id={payload['cycle_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"cycle_recommendation={payload['cycle_recommendation']}")
    typer.echo(f"targets_requiring_owner_action={payload['targets_requiring_owner_action']}")
    typer.echo(f"report_path={payload['rule_review_cycle_report_path']}")
    typer.echo("policy_change_allowed=false")
    typer.echo("production_effect=none")


@dynamic_v3_rescue_app.command("validate-rule-review-cycle")
def dynamic_v3_validate_rule_review_cycle_command(
    cycle_id: Annotated[
        str,
        typer.Option("--cycle-id", "--cycle_id", help="cycle id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="rule review cycle artifact root。"),
    ] = DEFAULT_RULE_REVIEW_CYCLE_DIR,
) -> None:
    """校验 TRADING-177 rule review cycle artifact。"""
    payload = validate_rule_review_cycle_artifact(cycle_id=cycle_id, output_dir=output_dir)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("policy_change_allowed=false")
    typer.echo("production_effect=none")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


__all__ = [
    "dynamic_v3_rule_review_cycle_report_command",
    "dynamic_v3_rule_review_cycle_run_command",
    "dynamic_v3_validate_rule_review_cycle_command",
]
