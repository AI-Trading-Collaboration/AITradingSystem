from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio.dynamic_v3_backtest_simulation import (
    DEFAULT_BACKTEST_SIM_EVENT_DIR,
    DEFAULT_BACKTEST_SIM_OUTCOME_DIR,
    DEFAULT_BACKTEST_SIM_SENSITIVITY_DIR,
    DEFAULT_BACKTEST_SIM_VARIANT_DIR,
    backtest_sim_sensitivity_report_payload,
    run_backtest_sim_sensitivity,
    validate_backtest_sim_sensitivity_artifact,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_backtest_sim_app,
    dynamic_v3_rescue_app,
)


@dynamic_v3_backtest_sim_app.command("sensitivity-run")
def dynamic_v3_backtest_sim_sensitivity_run_command(
    sim_outcome_id: Annotated[str, typer.Option("--sim-outcome-id")],
    output_dir: Annotated[
        Path, typer.Option("--output-dir")
    ] = DEFAULT_BACKTEST_SIM_SENSITIVITY_DIR,
    outcome_dir: Annotated[Path, typer.Option("--outcome-dir")] = DEFAULT_BACKTEST_SIM_OUTCOME_DIR,
    variant_dir: Annotated[Path, typer.Option("--variant-dir")] = DEFAULT_BACKTEST_SIM_VARIANT_DIR,
    event_dir: Annotated[Path, typer.Option("--event-dir")] = DEFAULT_BACKTEST_SIM_EVENT_DIR,
) -> None:
    result = run_backtest_sim_sensitivity(
        sim_outcome_id=sim_outcome_id,
        output_dir=output_dir,
        outcome_dir=outcome_dir,
        variant_dir=variant_dir,
        event_dir=event_dir,
    )
    manifest = result["manifest"]
    warnings = result["overfit_warning_summary"]
    typer.echo(f"sensitivity_id={result['sensitivity_id']}")
    typer.echo(f"sensitivity_dir={result['sensitivity_dir']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(f"simulation_overfit_status={warnings['simulation_overfit_status']}")
    typer.echo(f"strong_calibration_allowed={warnings['strong_calibration_allowed']}")
    typer.echo("broker_action_taken=false")
    typer.echo("production_effect=none")


@dynamic_v3_backtest_sim_app.command("sensitivity-report")
def dynamic_v3_backtest_sim_sensitivity_report_command(
    latest: Annotated[bool, typer.Option("--latest/--no-latest")] = False,
    sensitivity_id: Annotated[str | None, typer.Option("--sensitivity-id")] = None,
    output_dir: Annotated[
        Path, typer.Option("--output-dir")
    ] = DEFAULT_BACKTEST_SIM_SENSITIVITY_DIR,
) -> None:
    payload = backtest_sim_sensitivity_report_payload(
        sensitivity_id=sensitivity_id,
        latest=latest,
        output_dir=output_dir,
    )
    warnings = payload["overfit_warning_summary"]
    typer.echo(f"sensitivity_id={payload['sensitivity_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"simulation_overfit_status={warnings['simulation_overfit_status']}")
    typer.echo(f"strong_calibration_allowed={warnings['strong_calibration_allowed']}")
    typer.echo(f"report_path={payload['backtest_sim_sensitivity_report_path']}")
    typer.echo("production_effect=none")


@dynamic_v3_rescue_app.command("validate-backtest-sim-sensitivity")
def dynamic_v3_validate_backtest_sim_sensitivity_command(
    sensitivity_id: Annotated[str, typer.Option("--sensitivity-id")],
    output_dir: Annotated[
        Path, typer.Option("--output-dir")
    ] = DEFAULT_BACKTEST_SIM_SENSITIVITY_DIR,
) -> None:
    payload = validate_backtest_sim_sensitivity_artifact(
        sensitivity_id=sensitivity_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_taken=false")
    typer.echo("production_effect=none")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


__all__ = [
    "dynamic_v3_backtest_sim_sensitivity_run_command",
    "dynamic_v3_backtest_sim_sensitivity_report_command",
    "dynamic_v3_validate_backtest_sim_sensitivity_command",
]
