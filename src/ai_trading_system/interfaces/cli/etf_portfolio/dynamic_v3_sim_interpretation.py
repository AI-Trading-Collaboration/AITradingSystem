from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio.dynamic_v3_backtest_simulation import (
    DEFAULT_BACKTEST_SIM_CALIBRATION_DIR,
    DEFAULT_BACKTEST_SIM_FORWARD_BRIDGE_DIR,
    DEFAULT_BACKTEST_SIM_OUTCOME_DIR,
    DEFAULT_SIM_INTERPRETATION_DIR,
    run_sim_interpretation,
    sim_interpretation_report_payload,
    validate_sim_interpretation_artifact,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_rescue_app,
    dynamic_v3_sim_interpretation_app,
)


@dynamic_v3_sim_interpretation_app.command("run")
def dynamic_v3_sim_interpretation_run_command(
    outcome_id: Annotated[str, typer.Option("--outcome-id", help="simulation outcome id。")],
    calibration_id: Annotated[
        str,
        typer.Option("--calibration-id", help="simulation calibration pack id。"),
    ],
    bridge_id: Annotated[str, typer.Option("--bridge-id", help="simulation bridge id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="simulation interpretation artifact root。"),
    ] = DEFAULT_SIM_INTERPRETATION_DIR,
    outcome_dir: Annotated[
        Path,
        typer.Option("--outcome-dir", help="backtest simulation outcome artifact root。"),
    ] = DEFAULT_BACKTEST_SIM_OUTCOME_DIR,
    calibration_dir: Annotated[
        Path,
        typer.Option("--calibration-dir", help="backtest simulation calibration root。"),
    ] = DEFAULT_BACKTEST_SIM_CALIBRATION_DIR,
    bridge_dir: Annotated[
        Path,
        typer.Option("--bridge-dir", help="backtest simulation bridge root。"),
    ] = DEFAULT_BACKTEST_SIM_FORWARD_BRIDGE_DIR,
) -> None:
    """生成 TRADING-169 simulation interpretation pack。"""
    result = run_sim_interpretation(
        outcome_id=outcome_id,
        calibration_id=calibration_id,
        bridge_id=bridge_id,
        output_dir=output_dir,
        outcome_dir=outcome_dir,
        calibration_dir=calibration_dir,
        bridge_dir=bridge_dir,
    )
    manifest = result["manifest"]
    typer.echo(f"interpretation_id={result['interpretation_id']}")
    typer.echo(f"interpretation_dir={result['interpretation_dir']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(f"source_best_variant={manifest['source_best_variant']}")
    typer.echo("report_label=BACKTEST_SIMULATION_NOT_PIT")
    typer.echo("production_effect=none")


@dynamic_v3_sim_interpretation_app.command("report")
def dynamic_v3_sim_interpretation_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest simulation interpretation。"),
    ] = False,
    interpretation_id: Annotated[
        str | None,
        typer.Option("--interpretation-id", help="interpretation id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="simulation interpretation artifact root。"),
    ] = DEFAULT_SIM_INTERPRETATION_DIR,
) -> None:
    """展示 TRADING-169 interpretation 摘要。"""
    payload = sim_interpretation_report_payload(
        interpretation_id=interpretation_id,
        latest=latest,
        output_dir=output_dir,
    )
    matrix = payload["variant_interpretation_matrix"]
    typer.echo(f"interpretation_id={payload['interpretation_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"variant_count={len(matrix['variants'])}")
    typer.echo(f"report_path={payload['sim_interpretation_report_path']}")
    typer.echo("report_label=BACKTEST_SIMULATION_NOT_PIT")
    typer.echo("production_effect=none")


@dynamic_v3_rescue_app.command("validate-sim-interpretation")
def dynamic_v3_validate_sim_interpretation_command(
    interpretation_id: Annotated[
        str,
        typer.Option("--interpretation-id", help="interpretation id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="simulation interpretation artifact root。"),
    ] = DEFAULT_SIM_INTERPRETATION_DIR,
) -> None:
    """校验 TRADING-169 simulation interpretation artifact。"""
    payload = validate_sim_interpretation_artifact(
        interpretation_id=interpretation_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("report_label=BACKTEST_SIMULATION_NOT_PIT")
    typer.echo("production_effect=none")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


__all__ = [
    "dynamic_v3_sim_interpretation_report_command",
    "dynamic_v3_sim_interpretation_run_command",
    "dynamic_v3_validate_sim_interpretation_command",
]
