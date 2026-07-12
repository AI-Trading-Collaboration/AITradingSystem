from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio.dynamic_v3_backtest_simulation import (
    DEFAULT_BACKTEST_SIM_CALIBRATION_DIR,
    DEFAULT_BACKTEST_SIM_FORWARD_BRIDGE_DIR,
    backtest_sim_forward_bridge_report_payload,
    run_backtest_sim_forward_bridge,
    validate_backtest_sim_forward_bridge_artifact,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_backtest_sim_app,
    dynamic_v3_rescue_app,
)


@dynamic_v3_backtest_sim_app.command("forward-bridge")
def dynamic_v3_backtest_sim_forward_bridge_command(
    calibration_pack_id: Annotated[
        str,
        typer.Option("--calibration-pack-id", help="calibration pack id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="backtest simulation bridge artifact root。"),
    ] = DEFAULT_BACKTEST_SIM_FORWARD_BRIDGE_DIR,
    calibration_dir: Annotated[
        Path,
        typer.Option("--calibration-dir", help="backtest simulation calibration root。"),
    ] = DEFAULT_BACKTEST_SIM_CALIBRATION_DIR,
) -> None:
    """生成 TRADING-168 simulation-to-forward bridge。"""
    result = run_backtest_sim_forward_bridge(
        calibration_pack_id=calibration_pack_id,
        output_dir=output_dir,
        calibration_dir=calibration_dir,
    )
    manifest = result["manifest"]
    targets = result["forward_confirmation_targets"]
    typer.echo(f"bridge_id={result['bridge_id']}")
    typer.echo(f"bridge_dir={result['bridge_dir']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(f"next_action={manifest['next_action']}")
    typer.echo(f"target_count={len(targets['targets'])}")
    typer.echo("broker_action_allowed=false")
    typer.echo("production_effect=none")


@dynamic_v3_backtest_sim_app.command("forward-bridge-report")
def dynamic_v3_backtest_sim_forward_bridge_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest backtest sim bridge。"),
    ] = False,
    bridge_id: Annotated[str | None, typer.Option("--bridge-id", help="bridge id。")] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="backtest simulation bridge artifact root。"),
    ] = DEFAULT_BACKTEST_SIM_FORWARD_BRIDGE_DIR,
) -> None:
    """展示 TRADING-168 forward bridge 摘要。"""
    payload = backtest_sim_forward_bridge_report_payload(
        bridge_id=bridge_id,
        latest=latest,
        output_dir=output_dir,
    )
    targets = payload["forward_confirmation_targets"]
    typer.echo(f"bridge_id={payload['bridge_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"next_action={payload['next_action']}")
    typer.echo(f"target_count={len(targets['targets'])}")
    typer.echo(f"report_path={payload['sim_forward_bridge_report_path']}")
    typer.echo("production_effect=none")


@dynamic_v3_rescue_app.command("validate-backtest-sim-forward-bridge")
def dynamic_v3_validate_backtest_sim_forward_bridge_command(
    bridge_id: Annotated[str, typer.Option("--bridge-id", help="bridge id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="backtest simulation bridge artifact root。"),
    ] = DEFAULT_BACKTEST_SIM_FORWARD_BRIDGE_DIR,
) -> None:
    """校验 TRADING-168 backtest simulation forward bridge artifact。"""
    payload = validate_backtest_sim_forward_bridge_artifact(
        bridge_id=bridge_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_taken=false")
    typer.echo("production_effect=none")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


__all__ = [
    "dynamic_v3_backtest_sim_forward_bridge_command",
    "dynamic_v3_backtest_sim_forward_bridge_report_command",
    "dynamic_v3_validate_backtest_sim_forward_bridge_command",
]
