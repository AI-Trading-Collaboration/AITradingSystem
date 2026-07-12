from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio.dynamic_v3_backtest_simulation import (
    DEFAULT_BACKTEST_SIM_OUTCOME_DIR,
    DEFAULT_SIM_RISK_RETURN_DIR,
    run_sim_risk_return,
    sim_risk_return_report_payload,
    validate_sim_risk_return_artifact,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_rescue_app,
    dynamic_v3_sim_risk_return_app,
)


@dynamic_v3_sim_risk_return_app.command("run")
def dynamic_v3_sim_risk_return_run_command(
    outcome_id: Annotated[str, typer.Option("--outcome-id", help="simulation outcome id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="simulation risk-return artifact root。"),
    ] = DEFAULT_SIM_RISK_RETURN_DIR,
    outcome_dir: Annotated[
        Path,
        typer.Option("--outcome-dir", help="backtest simulation outcome artifact root。"),
    ] = DEFAULT_BACKTEST_SIM_OUTCOME_DIR,
) -> None:
    """生成 TRADING-170 simulation risk-return review。"""
    result = run_sim_risk_return(
        outcome_id=outcome_id,
        output_dir=output_dir,
        outcome_dir=outcome_dir,
    )
    manifest = result["manifest"]
    typer.echo(f"risk_return_id={result['risk_return_id']}")
    typer.echo(f"risk_return_dir={result['risk_return_dir']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(f"row_count={len(result['active_variant_tradeoff_table'])}")
    typer.echo("report_label=BACKTEST_SIMULATION_NOT_PIT")
    typer.echo("production_effect=none")


@dynamic_v3_sim_risk_return_app.command("report")
def dynamic_v3_sim_risk_return_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest simulation risk-return。"),
    ] = False,
    risk_return_id: Annotated[
        str | None,
        typer.Option("--risk-return-id", "--risk_return_id", help="risk-return id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="simulation risk-return artifact root。"),
    ] = DEFAULT_SIM_RISK_RETURN_DIR,
) -> None:
    """展示 TRADING-170 risk-return 摘要。"""
    payload = sim_risk_return_report_payload(
        risk_return_id=risk_return_id,
        latest=latest,
        output_dir=output_dir,
    )
    summary = payload["risk_adjusted_summary"]
    typer.echo(f"risk_return_id={payload['risk_return_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"summary_count={len(summary['summary'])}")
    typer.echo(f"report_path={payload['risk_return_report_path']}")
    typer.echo("production_effect=none")


@dynamic_v3_rescue_app.command("validate-sim-risk-return")
def dynamic_v3_validate_sim_risk_return_command(
    risk_return_id: Annotated[
        str,
        typer.Option("--risk-return-id", "--risk_return_id", help="risk-return id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="simulation risk-return artifact root。"),
    ] = DEFAULT_SIM_RISK_RETURN_DIR,
) -> None:
    """校验 TRADING-170 simulation risk-return artifact。"""
    payload = validate_sim_risk_return_artifact(
        risk_return_id=risk_return_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_effect=none")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


__all__ = [
    "dynamic_v3_sim_risk_return_report_command",
    "dynamic_v3_sim_risk_return_run_command",
    "dynamic_v3_validate_sim_risk_return_command",
]
