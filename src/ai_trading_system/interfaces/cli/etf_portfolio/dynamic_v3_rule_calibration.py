from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio.dynamic_v3_historical_replay import (
    DEFAULT_RULE_CALIBRATION_DIR,
    DEFAULT_VARIANT_COMPARISON_DIR,
    rule_calibration_report_payload,
    run_rule_calibration,
    validate_rule_calibration_artifact,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_rescue_app,
    dynamic_v3_rule_calibration_app,
)


@dynamic_v3_rule_calibration_app.command("run")
def dynamic_v3_rule_calibration_run_command(
    comparison_id: Annotated[str, typer.Option("--comparison-id", help="comparison id。")],
    comparison_dir: Annotated[
        Path,
        typer.Option("--comparison-dir", help="variant comparison artifact root。"),
    ] = DEFAULT_VARIANT_COMPARISON_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="rule calibration artifact root。"),
    ] = DEFAULT_RULE_CALIBRATION_DIR,
) -> None:
    """运行 TRADING-149 historical replay rule calibration proposal。"""
    result = run_rule_calibration(
        comparison_id=comparison_id,
        comparison_dir=comparison_dir,
        output_dir=output_dir,
    )
    proposals = result["proposed_policy_adjustments"]["proposals"]
    first = proposals[0] if proposals else {}
    typer.echo(f"calibration_id={result['calibration_id']}")
    typer.echo(f"calibration_dir={result['calibration_dir']}")
    typer.echo(f"status={result['manifest']['status']}")
    typer.echo(f"proposal={first.get('change_type', 'MISSING')}")
    typer.echo("auto_apply=false")
    typer.echo("owner_approval_required=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_rule_calibration_app.command("report")
def dynamic_v3_rule_calibration_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest rule calibration。"),
    ] = False,
    calibration_id: Annotated[
        str | None,
        typer.Option("--calibration-id", help="calibration id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="rule calibration artifact root。"),
    ] = DEFAULT_RULE_CALIBRATION_DIR,
) -> None:
    """展示 rule calibration 摘要。"""
    payload = rule_calibration_report_payload(
        calibration_id=calibration_id,
        latest=latest,
        output_dir=output_dir,
    )
    proposals = payload["proposed_policy_adjustments"]["proposals"]
    first = proposals[0] if proposals else {}
    typer.echo(f"calibration_id={payload['calibration_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"proposal={first.get('change_type', 'MISSING')}")
    typer.echo(f"report_path={payload['rule_calibration_report_path']}")
    typer.echo("auto_apply=false")
    typer.echo("owner_approval_required=true")
    typer.echo("production_effect=none")


@dynamic_v3_rescue_app.command("validate-rule-calibration")
def dynamic_v3_validate_rule_calibration_command(
    calibration_id: Annotated[str, typer.Option("--calibration-id", help="calibration id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="rule calibration artifact root。"),
    ] = DEFAULT_RULE_CALIBRATION_DIR,
) -> None:
    """校验 TRADING-149 rule calibration artifact。"""
    payload = validate_rule_calibration_artifact(
        calibration_id=calibration_id, output_dir=output_dir
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("auto_apply=false")
    typer.echo("production_effect=none")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


__all__ = [
    "dynamic_v3_rule_calibration_run_command",
    "dynamic_v3_rule_calibration_report_command",
    "dynamic_v3_validate_rule_calibration_command",
]
