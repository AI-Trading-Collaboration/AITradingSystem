from __future__ import annotations

from pathlib import Path
from typing import Annotated, Any

import typer

from ai_trading_system.etf_portfolio.dynamic_v3_backtest_simulation import (
    DEFAULT_BACKTEST_SIM_OUTCOME_DIR,
    DEFAULT_BACKTEST_SIM_REGIME_DIR,
    backtest_sim_regime_report_payload,
    run_backtest_sim_regime_review,
    validate_backtest_sim_regime_artifact,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_backtest_sim_app,
    dynamic_v3_rescue_app,
)


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


@dynamic_v3_backtest_sim_app.command("regime-review")
def dynamic_v3_backtest_sim_regime_review_command(
    sim_outcome_id: Annotated[
        str, typer.Option("--sim-outcome-id", help="simulation outcome id。")
    ],
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="regime artifact root。")
    ] = DEFAULT_BACKTEST_SIM_REGIME_DIR,
    outcome_dir: Annotated[
        Path, typer.Option("--outcome-dir", help="outcome artifact root。")
    ] = DEFAULT_BACKTEST_SIM_OUTCOME_DIR,
) -> None:
    result = run_backtest_sim_regime_review(
        sim_outcome_id=sim_outcome_id, output_dir=output_dir, outcome_dir=outcome_dir
    )
    manifest = result["manifest"]
    typer.echo(f"regime_review_id={result['regime_review_id']}")
    typer.echo(f"regime_dir={result['regime_review_dir']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(
        f"regime_count={len(_mapping(result['regime_window_inventory'].get('regime_counts')))}"
    )
    typer.echo("broker_action_taken=false")
    typer.echo("production_effect=none")


@dynamic_v3_backtest_sim_app.command("regime-report")
def dynamic_v3_backtest_sim_regime_report_command(
    latest: Annotated[bool, typer.Option("--latest/--no-latest")] = False,
    regime_review_id: Annotated[str | None, typer.Option("--regime-review-id")] = None,
    output_dir: Annotated[Path, typer.Option("--output-dir")] = DEFAULT_BACKTEST_SIM_REGIME_DIR,
) -> None:
    payload = backtest_sim_regime_report_payload(
        regime_review_id=regime_review_id, latest=latest, output_dir=output_dir
    )
    typer.echo(f"regime_review_id={payload['regime_review_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(
        f"regime_count={len(_mapping(payload['regime_window_inventory'].get('regime_counts')))}"
    )
    typer.echo(f"report_path={payload['backtest_sim_regime_report_path']}")
    typer.echo("production_effect=none")


@dynamic_v3_rescue_app.command("validate-backtest-sim-regime")
def dynamic_v3_validate_backtest_sim_regime_command(
    regime_review_id: Annotated[str, typer.Option("--regime-review-id")],
    output_dir: Annotated[Path, typer.Option("--output-dir")] = DEFAULT_BACKTEST_SIM_REGIME_DIR,
) -> None:
    payload = validate_backtest_sim_regime_artifact(
        regime_review_id=regime_review_id, output_dir=output_dir
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_taken=false")
    typer.echo("production_effect=none")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


__all__ = [
    "dynamic_v3_backtest_sim_regime_review_command",
    "dynamic_v3_backtest_sim_regime_report_command",
    "dynamic_v3_validate_backtest_sim_regime_command",
]
