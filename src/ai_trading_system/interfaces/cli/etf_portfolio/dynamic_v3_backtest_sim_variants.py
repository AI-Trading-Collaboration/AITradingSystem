from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio.dynamic_v3_backtest_simulation import (
    DEFAULT_BACKTEST_SIM_EVENT_DIR,
    DEFAULT_BACKTEST_SIM_VARIANT_DIR,
    backtest_sim_variant_report_payload,
    generate_backtest_sim_variants,
    validate_backtest_sim_variants_artifact,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_backtest_sim_app,
    dynamic_v3_rescue_app,
)


@dynamic_v3_backtest_sim_app.command("variants-generate")
def dynamic_v3_backtest_sim_variants_generate_command(
    event_set_id: Annotated[str, typer.Option("--event-set-id", help="event set id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="backtest simulation variant artifact root。"),
    ] = DEFAULT_BACKTEST_SIM_VARIANT_DIR,
    event_dir: Annotated[
        Path,
        typer.Option("--event-dir", help="backtest simulation event artifact root。"),
    ] = DEFAULT_BACKTEST_SIM_EVENT_DIR,
) -> None:
    """生成 TRADING-162 simulated advisory variants。"""
    result = generate_backtest_sim_variants(
        event_set_id=event_set_id,
        output_dir=output_dir,
        event_dir=event_dir,
    )
    manifest = result["manifest"]
    typer.echo(f"variant_set_id={result['variant_set_id']}")
    typer.echo(f"variant_dir={result['variant_set_dir']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(f"variant_row_count={len(result['variant_rows'])}")
    typer.echo(f"ready_count={manifest['ready_count']}")
    typer.echo(f"skipped_count={manifest['skipped_count']}")
    typer.echo("broker_action_taken=false")
    typer.echo("production_effect=none")


@dynamic_v3_backtest_sim_app.command("variants-report")
def dynamic_v3_backtest_sim_variants_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest backtest sim variants。"),
    ] = False,
    variant_set_id: Annotated[
        str | None,
        typer.Option("--variant-set-id", help="variant set id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="backtest simulation variant artifact root。"),
    ] = DEFAULT_BACKTEST_SIM_VARIANT_DIR,
) -> None:
    """展示 TRADING-162 variant 摘要。"""
    payload = backtest_sim_variant_report_payload(
        variant_set_id=variant_set_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"variant_set_id={payload['variant_set_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"variant_row_count={len(payload['simulated_variant_weights'])}")
    typer.echo(f"ready_count={payload['ready_count']}")
    typer.echo(f"report_path={payload['variant_generation_report_path']}")
    typer.echo("production_effect=none")


@dynamic_v3_rescue_app.command("validate-backtest-sim-variants")
def dynamic_v3_validate_backtest_sim_variants_command(
    variant_set_id: Annotated[
        str,
        typer.Option("--variant-set-id", help="variant set id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="backtest simulation variant artifact root。"),
    ] = DEFAULT_BACKTEST_SIM_VARIANT_DIR,
) -> None:
    """校验 TRADING-162 backtest simulation variants artifact。"""
    payload = validate_backtest_sim_variants_artifact(
        variant_set_id=variant_set_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_taken=false")
    typer.echo("production_effect=none")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


__all__ = [
    "dynamic_v3_backtest_sim_variants_generate_command",
    "dynamic_v3_backtest_sim_variants_report_command",
    "dynamic_v3_validate_backtest_sim_variants_command",
]
