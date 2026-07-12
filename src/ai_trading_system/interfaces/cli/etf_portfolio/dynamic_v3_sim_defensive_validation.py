from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio.dynamic_v3_backtest_simulation import (
    DEFAULT_BACKTEST_SIM_OUTCOME_DIR,
    DEFAULT_SIM_DEFENSIVE_VALIDATION_DIR,
    run_sim_defensive_validation,
    sim_defensive_validation_report_payload,
    validate_sim_defensive_validation_artifact,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_rescue_app,
    dynamic_v3_sim_defensive_validation_app,
)


@dynamic_v3_sim_defensive_validation_app.command("run")
def dynamic_v3_sim_defensive_validation_run_command(
    outcome_id: Annotated[str, typer.Option("--outcome-id", help="simulation outcome id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="simulation defensive validation artifact root。"),
    ] = DEFAULT_SIM_DEFENSIVE_VALIDATION_DIR,
    outcome_dir: Annotated[
        Path,
        typer.Option("--outcome-dir", help="backtest simulation outcome artifact root。"),
    ] = DEFAULT_BACKTEST_SIM_OUTCOME_DIR,
) -> None:
    """生成 TRADING-171 defensive validation。"""
    result = run_sim_defensive_validation(
        outcome_id=outcome_id,
        output_dir=output_dir,
        outcome_dir=outcome_dir,
    )
    summary = result["defensive_validation_summary"]
    typer.echo(f"defensive_validation_id={result['defensive_validation_id']}")
    typer.echo(f"defensive_validation_dir={result['defensive_validation_dir']}")
    typer.echo(f"status={result['manifest']['status']}")
    typer.echo(
        f"defensive_limited_adjustment_status="
        f"{summary['defensive_limited_adjustment_status']}"
    )
    typer.echo("report_label=BACKTEST_SIMULATION_NOT_PIT")
    typer.echo("production_effect=none")


@dynamic_v3_sim_defensive_validation_app.command("report")
def dynamic_v3_sim_defensive_validation_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest defensive validation。"),
    ] = False,
    defensive_validation_id: Annotated[
        str | None,
        typer.Option(
            "--defensive-validation-id",
            "--defensive_validation_id",
            help="defensive validation id。",
        ),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="simulation defensive validation artifact root。"),
    ] = DEFAULT_SIM_DEFENSIVE_VALIDATION_DIR,
) -> None:
    """展示 TRADING-171 defensive validation 摘要。"""
    payload = sim_defensive_validation_report_payload(
        defensive_validation_id=defensive_validation_id,
        latest=latest,
        output_dir=output_dir,
    )
    summary = payload["defensive_validation_summary"]
    typer.echo(f"defensive_validation_id={payload['defensive_validation_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(
        f"defensive_limited_adjustment_status="
        f"{summary['defensive_limited_adjustment_status']}"
    )
    typer.echo(f"report_path={payload['defensive_validation_report_path']}")
    typer.echo("production_effect=none")


@dynamic_v3_rescue_app.command("validate-sim-defensive-validation")
def dynamic_v3_validate_sim_defensive_validation_command(
    defensive_validation_id: Annotated[
        str,
        typer.Option(
            "--defensive-validation-id",
            "--defensive_validation_id",
            help="defensive validation id。",
        ),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="simulation defensive validation artifact root。"),
    ] = DEFAULT_SIM_DEFENSIVE_VALIDATION_DIR,
) -> None:
    """校验 TRADING-171 defensive validation artifact。"""
    payload = validate_sim_defensive_validation_artifact(
        defensive_validation_id=defensive_validation_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_effect=none")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


__all__ = [
    "dynamic_v3_sim_defensive_validation_report_command",
    "dynamic_v3_sim_defensive_validation_run_command",
    "dynamic_v3_validate_sim_defensive_validation_command",
]
