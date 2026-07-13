from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio.dynamic_v3_backtest_simulation import (
    DEFAULT_ADVISORY_PROPOSAL_REVIEW_DIR,
    DEFAULT_BACKTEST_SIM_FORWARD_BRIDGE_DIR,
    DEFAULT_FORWARD_CONFIRMATION_PLAN_DIR,
    forward_confirmation_plan_report_payload,
    run_forward_confirmation_plan,
    validate_forward_confirmation_plan_artifact,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_forward_confirmation_plan_app,
    dynamic_v3_rescue_app,
)


@dynamic_v3_forward_confirmation_plan_app.command("run")
def dynamic_v3_forward_confirmation_plan_run_command(
    proposal_review_id: Annotated[
        str,
        typer.Option("--proposal-review-id", "--proposal_review_id", help="proposal review id。"),
    ],
    bridge_id: Annotated[str, typer.Option("--bridge-id", help="simulation bridge id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="forward confirmation plan artifact root。"),
    ] = DEFAULT_FORWARD_CONFIRMATION_PLAN_DIR,
    proposal_review_dir: Annotated[
        Path,
        typer.Option("--proposal-review-dir", help="advisory proposal review root。"),
    ] = DEFAULT_ADVISORY_PROPOSAL_REVIEW_DIR,
    bridge_dir: Annotated[
        Path,
        typer.Option("--bridge-dir", help="backtest simulation bridge root。"),
    ] = DEFAULT_BACKTEST_SIM_FORWARD_BRIDGE_DIR,
) -> None:
    """生成 TRADING-173 forward confirmation plan。"""
    result = run_forward_confirmation_plan(
        proposal_review_id=proposal_review_id,
        bridge_id=bridge_id,
        output_dir=output_dir,
        proposal_review_dir=proposal_review_dir,
        bridge_dir=bridge_dir,
    )
    manifest = result["manifest"]
    targets = result["confirmation_targets"]
    typer.echo(f"confirmation_plan_id={result['confirmation_plan_id']}")
    typer.echo(f"confirmation_plan_dir={result['confirmation_plan_dir']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(f"target_count={len(targets['targets'])}")
    typer.echo("auto_apply=false")
    typer.echo("production_effect=none")


@dynamic_v3_forward_confirmation_plan_app.command("report")
def dynamic_v3_forward_confirmation_plan_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest forward confirmation plan。"),
    ] = False,
    confirmation_plan_id: Annotated[
        str | None,
        typer.Option(
            "--confirmation-plan-id",
            "--confirmation_plan_id",
            help="confirmation plan id。",
        ),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="forward confirmation plan artifact root。"),
    ] = DEFAULT_FORWARD_CONFIRMATION_PLAN_DIR,
) -> None:
    """展示 TRADING-173 forward confirmation plan 摘要。"""
    payload = forward_confirmation_plan_report_payload(
        confirmation_plan_id=confirmation_plan_id,
        latest=latest,
        output_dir=output_dir,
    )
    targets = payload["confirmation_targets"]
    typer.echo(f"confirmation_plan_id={payload['confirmation_plan_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"target_count={len(targets['targets'])}")
    typer.echo(f"report_path={payload['forward_confirmation_plan_report_path']}")
    typer.echo("production_effect=none")


@dynamic_v3_rescue_app.command("validate-forward-confirmation-plan")
def dynamic_v3_validate_forward_confirmation_plan_command(
    confirmation_plan_id: Annotated[
        str,
        typer.Option(
            "--confirmation-plan-id",
            "--confirmation_plan_id",
            help="confirmation plan id。",
        ),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="forward confirmation plan artifact root。"),
    ] = DEFAULT_FORWARD_CONFIRMATION_PLAN_DIR,
) -> None:
    """校验 TRADING-173 forward confirmation plan artifact。"""
    payload = validate_forward_confirmation_plan_artifact(
        confirmation_plan_id=confirmation_plan_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_effect=none")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


__all__ = [
    "dynamic_v3_forward_confirmation_plan_report_command",
    "dynamic_v3_forward_confirmation_plan_run_command",
    "dynamic_v3_validate_forward_confirmation_plan_command",
]
