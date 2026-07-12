from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio.dynamic_v3_historical_replay import (
    DEFAULT_REPLAY_DIAGNOSIS_DIR,
    DEFAULT_REPLAY_FORWARD_BRIDGE_DIR,
    DEFAULT_RULE_CALIBRATION_DIR,
    DEFAULT_VARIANT_COMPARISON_DIR,
    replay_forward_bridge_report_payload,
    run_replay_forward_bridge,
    validate_replay_forward_bridge_artifact,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_replay_forward_bridge_app,
    dynamic_v3_rescue_app,
)


@dynamic_v3_replay_forward_bridge_app.command("run")
def dynamic_v3_replay_forward_bridge_run_command(
    diagnosis_id: Annotated[str, typer.Option("--diagnosis-id", help="diagnosis id。")],
    comparison_id: Annotated[str, typer.Option("--comparison-id", help="comparison id。")],
    calibration_id: Annotated[str, typer.Option("--calibration-id", help="calibration id。")],
    diagnosis_dir: Annotated[
        Path,
        typer.Option("--diagnosis-dir", help="replay diagnosis artifact root。"),
    ] = DEFAULT_REPLAY_DIAGNOSIS_DIR,
    comparison_dir: Annotated[
        Path,
        typer.Option("--comparison-dir", help="variant comparison artifact root。"),
    ] = DEFAULT_VARIANT_COMPARISON_DIR,
    calibration_dir: Annotated[
        Path,
        typer.Option("--calibration-dir", help="rule calibration artifact root。"),
    ] = DEFAULT_RULE_CALIBRATION_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="replay forward bridge artifact root。"),
    ] = DEFAULT_REPLAY_FORWARD_BRIDGE_DIR,
) -> None:
    """运行 TRADING-150 replay-to-forward tracking bridge。"""
    result = run_replay_forward_bridge(
        diagnosis_id=diagnosis_id,
        comparison_id=comparison_id,
        calibration_id=calibration_id,
        diagnosis_dir=diagnosis_dir,
        comparison_dir=comparison_dir,
        calibration_dir=calibration_dir,
        output_dir=output_dir,
    )
    focus = result["forward_tracking_focus"]
    typer.echo(f"bridge_id={result['bridge_id']}")
    typer.echo(f"bridge_dir={result['bridge_dir']}")
    typer.echo(f"status={result['manifest']['status']}")
    typer.echo(f"best_variant={result['manifest']['best_variant']}")
    typer.echo(f"forward_tracking_status={focus['forward_tracking_status']}")
    typer.echo(f"next_action={result['manifest']['next_action']}")
    typer.echo("production_effect=none")
    typer.echo("broker_action_taken=false")


@dynamic_v3_replay_forward_bridge_app.command("report")
def dynamic_v3_replay_forward_bridge_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest replay forward bridge。"),
    ] = False,
    bridge_id: Annotated[str | None, typer.Option("--bridge-id", help="bridge id。")] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="replay forward bridge artifact root。"),
    ] = DEFAULT_REPLAY_FORWARD_BRIDGE_DIR,
) -> None:
    """展示 replay-to-forward bridge 摘要。"""
    payload = replay_forward_bridge_report_payload(
        bridge_id=bridge_id,
        latest=latest,
        output_dir=output_dir,
    )
    focus = payload["forward_tracking_focus"]
    typer.echo(f"bridge_id={payload['bridge_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"best_variant={payload['best_variant']}")
    typer.echo(f"forward_tracking_status={focus['forward_tracking_status']}")
    typer.echo(f"next_action={payload['next_action']}")
    typer.echo(f"report_path={payload['replay_forward_bridge_report_path']}")
    typer.echo("production_effect=none")
    typer.echo("broker_action_taken=false")


@dynamic_v3_rescue_app.command("validate-replay-forward-bridge")
def dynamic_v3_validate_replay_forward_bridge_command(
    bridge_id: Annotated[str, typer.Option("--bridge-id", help="bridge id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="replay forward bridge artifact root。"),
    ] = DEFAULT_REPLAY_FORWARD_BRIDGE_DIR,
) -> None:
    """校验 TRADING-150 replay-to-forward bridge artifact。"""
    payload = validate_replay_forward_bridge_artifact(bridge_id=bridge_id, output_dir=output_dir)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_effect=none")
    typer.echo("broker_action_taken=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


__all__ = [
    "dynamic_v3_replay_forward_bridge_run_command",
    "dynamic_v3_replay_forward_bridge_report_command",
    "dynamic_v3_validate_replay_forward_bridge_command",
]
