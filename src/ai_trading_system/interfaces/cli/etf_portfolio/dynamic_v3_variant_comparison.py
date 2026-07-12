from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio.dynamic_v3_historical_replay import (
    DEFAULT_BACKFILL_REPAIR_DIR,
    DEFAULT_BACKFILLED_OUTCOME_DIR,
    DEFAULT_VARIANT_COMPARISON_DIR,
    run_variant_comparison,
    validate_variant_comparison_artifact,
    variant_comparison_report_payload,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_rescue_app,
    dynamic_v3_variant_comparison_app,
)


@dynamic_v3_variant_comparison_app.command("run")
def dynamic_v3_variant_comparison_run_command(
    backfill_id: Annotated[str, typer.Option("--backfill-id", help="backfill id。")],
    repair_id: Annotated[
        str | None, typer.Option("--repair-id", help="optional repair id。")
    ] = None,
    backfill_dir: Annotated[
        Path,
        typer.Option("--backfill-dir", help="backfilled outcome artifact root。"),
    ] = DEFAULT_BACKFILLED_OUTCOME_DIR,
    repair_dir: Annotated[
        Path,
        typer.Option("--repair-dir", help="backfill repair artifact root。"),
    ] = DEFAULT_BACKFILL_REPAIR_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="variant comparison artifact root。"),
    ] = DEFAULT_VARIANT_COMPARISON_DIR,
) -> None:
    """运行 TRADING-148 replay variant performance comparison。"""
    result = run_variant_comparison(
        backfill_id=backfill_id,
        repair_id=repair_id,
        backfill_dir=backfill_dir,
        repair_dir=repair_dir,
        output_dir=output_dir,
    )
    rank = result["variant_rank_summary"]
    typer.echo(f"comparison_id={result['comparison_id']}")
    typer.echo(f"comparison_dir={result['comparison_dir']}")
    typer.echo(f"status={result['manifest']['status']}")
    typer.echo(f"best_variant={rank['best_variant']}")
    typer.echo(f"recommendation_confidence={rank['recommendation_confidence']}")
    typer.echo("production_effect=none")
    typer.echo("broker_action_taken=false")


@dynamic_v3_variant_comparison_app.command("report")
def dynamic_v3_variant_comparison_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest variant comparison。"),
    ] = False,
    comparison_id: Annotated[
        str | None,
        typer.Option("--comparison-id", help="comparison id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="variant comparison artifact root。"),
    ] = DEFAULT_VARIANT_COMPARISON_DIR,
) -> None:
    """展示 variant comparison 摘要。"""
    payload = variant_comparison_report_payload(
        comparison_id=comparison_id,
        latest=latest,
        output_dir=output_dir,
    )
    rank = payload["variant_rank_summary"]
    typer.echo(f"comparison_id={payload['comparison_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"best_variant={rank['best_variant']}")
    typer.echo(f"recommendation_confidence={rank['recommendation_confidence']}")
    typer.echo(f"report_path={payload['variant_comparison_report_path']}")
    typer.echo("production_effect=none")
    typer.echo("broker_action_taken=false")


@dynamic_v3_rescue_app.command("validate-variant-comparison")
def dynamic_v3_validate_variant_comparison_command(
    comparison_id: Annotated[str, typer.Option("--comparison-id", help="comparison id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="variant comparison artifact root。"),
    ] = DEFAULT_VARIANT_COMPARISON_DIR,
) -> None:
    """校验 TRADING-148 variant comparison artifact。"""
    payload = validate_variant_comparison_artifact(
        comparison_id=comparison_id, output_dir=output_dir
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_effect=none")
    typer.echo("broker_action_taken=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


__all__ = [
    "dynamic_v3_variant_comparison_run_command",
    "dynamic_v3_variant_comparison_report_command",
    "dynamic_v3_validate_variant_comparison_command",
]
