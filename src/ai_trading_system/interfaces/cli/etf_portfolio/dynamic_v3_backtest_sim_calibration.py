from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio.dynamic_v3_backtest_simulation import (
    DEFAULT_BACKTEST_SIM_CALIBRATION_DIR,
    DEFAULT_BACKTEST_SIM_OUTCOME_DIR,
    DEFAULT_BACKTEST_SIM_PAPER_DIR,
    DEFAULT_BACKTEST_SIM_REGIME_DIR,
    DEFAULT_BACKTEST_SIM_SENSITIVITY_DIR,
    backtest_sim_calibration_report_payload,
    run_backtest_sim_calibration_pack,
    validate_backtest_sim_calibration_artifact,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_backtest_sim_app,
    dynamic_v3_rescue_app,
)


@dynamic_v3_backtest_sim_app.command("calibration-pack")
def dynamic_v3_backtest_sim_calibration_pack_command(
    sim_outcome_id: Annotated[str, typer.Option("--sim-outcome-id")],
    sim_paper_id: Annotated[str, typer.Option("--sim-paper-id")],
    regime_review_id: Annotated[str, typer.Option("--regime-review-id")],
    sensitivity_id: Annotated[str, typer.Option("--sensitivity-id")],
    output_dir: Annotated[
        Path, typer.Option("--output-dir")
    ] = DEFAULT_BACKTEST_SIM_CALIBRATION_DIR,
    outcome_dir: Annotated[Path, typer.Option("--outcome-dir")] = DEFAULT_BACKTEST_SIM_OUTCOME_DIR,
    paper_dir: Annotated[Path, typer.Option("--paper-dir")] = DEFAULT_BACKTEST_SIM_PAPER_DIR,
    regime_dir: Annotated[Path, typer.Option("--regime-dir")] = DEFAULT_BACKTEST_SIM_REGIME_DIR,
    sensitivity_dir: Annotated[
        Path, typer.Option("--sensitivity-dir")
    ] = DEFAULT_BACKTEST_SIM_SENSITIVITY_DIR,
) -> None:
    result = run_backtest_sim_calibration_pack(
        sim_outcome_id=sim_outcome_id,
        sim_paper_id=sim_paper_id,
        regime_review_id=regime_review_id,
        sensitivity_id=sensitivity_id,
        output_dir=output_dir,
        outcome_dir=outcome_dir,
        paper_dir=paper_dir,
        regime_dir=regime_dir,
        sensitivity_dir=sensitivity_dir,
    )
    manifest = result["manifest"]
    evidence = result["simulation_evidence_summary"]
    typer.echo(f"calibration_pack_id={result['calibration_pack_id']}")
    typer.echo(f"calibration_dir={result['calibration_pack_dir']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(f"calibration_readiness={evidence['calibration_readiness']}")
    typer.echo(f"auto_apply={manifest['auto_apply']}")
    typer.echo("broker_action_allowed=false")
    typer.echo("production_effect=none")


@dynamic_v3_backtest_sim_app.command("calibration-report")
def dynamic_v3_backtest_sim_calibration_report_command(
    latest: Annotated[bool, typer.Option("--latest/--no-latest")] = False,
    calibration_pack_id: Annotated[str | None, typer.Option("--calibration-pack-id")] = None,
    output_dir: Annotated[
        Path, typer.Option("--output-dir")
    ] = DEFAULT_BACKTEST_SIM_CALIBRATION_DIR,
) -> None:
    payload = backtest_sim_calibration_report_payload(
        calibration_pack_id=calibration_pack_id,
        latest=latest,
        output_dir=output_dir,
    )
    evidence = payload["simulation_evidence_summary"]
    typer.echo(f"calibration_pack_id={payload['calibration_pack_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"calibration_readiness={evidence['calibration_readiness']}")
    typer.echo(f"auto_apply={payload['auto_apply']}")
    typer.echo(f"report_path={payload['backtest_sim_calibration_report_path']}")
    typer.echo("production_effect=none")


@dynamic_v3_rescue_app.command("validate-backtest-sim-calibration")
def dynamic_v3_validate_backtest_sim_calibration_command(
    calibration_pack_id: Annotated[str, typer.Option("--calibration-pack-id")],
    output_dir: Annotated[
        Path, typer.Option("--output-dir")
    ] = DEFAULT_BACKTEST_SIM_CALIBRATION_DIR,
) -> None:
    payload = validate_backtest_sim_calibration_artifact(
        calibration_pack_id=calibration_pack_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_taken=false")
    typer.echo("production_effect=none")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


__all__ = [
    "dynamic_v3_backtest_sim_calibration_pack_command",
    "dynamic_v3_backtest_sim_calibration_report_command",
    "dynamic_v3_validate_backtest_sim_calibration_command",
]
