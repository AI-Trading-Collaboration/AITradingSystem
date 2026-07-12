from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio.dynamic_v3_historical_replay import (
    DEFAULT_BACKFILLED_OUTCOME_DIR,
    DEFAULT_HISTORICAL_REPLAY_DIR,
    backfill_outcome_report_payload,
    run_backfill_outcome,
    validate_backfill_outcome_artifact,
)
from ai_trading_system.etf_portfolio.dynamic_v3_paper_tracking import (
    DEFAULT_PAPER_PORTFOLIO_CONFIG_PATH,
    DEFAULT_RATES_CACHE_PATH,
)
from ai_trading_system.etf_portfolio.models import DEFAULT_ETF_PRICE_PATH
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_backfill_outcome_app,
    dynamic_v3_rescue_app,
)


@dynamic_v3_backfill_outcome_app.command("run")
def dynamic_v3_backfill_outcome_run_command(
    replay_id: Annotated[str, typer.Option("--replay-id", help="replay id。")],
    replay_dir: Annotated[
        Path,
        typer.Option("--replay-dir", help="historical replay artifact root。"),
    ] = DEFAULT_HISTORICAL_REPLAY_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="backfilled outcome artifact root。"),
    ] = DEFAULT_BACKFILLED_OUTCOME_DIR,
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="cached ETF price path。"),
    ] = DEFAULT_ETF_PRICE_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="cached rates path。"),
    ] = DEFAULT_RATES_CACHE_PATH,
    config_path: Annotated[
        Path,
        typer.Option("--config", "--config-path", help="paper portfolio config。"),
    ] = DEFAULT_PAPER_PORTFOLIO_CONFIG_PATH,
) -> None:
    """运行 TRADING-143 backfilled outcome evaluation。"""
    result = run_backfill_outcome(
        replay_id=replay_id,
        replay_dir=replay_dir,
        output_dir=output_dir,
        prices_path=prices_path,
        rates_path=rates_path,
        config_path=config_path,
    )
    manifest = result["manifest"]
    typer.echo(f"backfill_id={result['backfill_id']}")
    typer.echo(f"backfill_dir={result['backfill_dir']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(f"available_count={manifest['available_count']}")
    typer.echo(f"pending_count={manifest['pending_count']}")
    typer.echo(f"insufficient_data_count={manifest['insufficient_data_count']}")
    typer.echo(f"best_variant={manifest['best_variant']}")
    typer.echo(f"data_quality_status={manifest['data_quality_status']}")
    typer.echo("broker_action_taken=false")


@dynamic_v3_backfill_outcome_app.command("report")
def dynamic_v3_backfill_outcome_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest backfilled outcome。"),
    ] = False,
    backfill_id: Annotated[
        str | None,
        typer.Option("--backfill-id", help="backfill id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="backfilled outcome artifact root。"),
    ] = DEFAULT_BACKFILLED_OUTCOME_DIR,
) -> None:
    """展示 TRADING-143 backfilled outcome 摘要。"""
    payload = backfill_outcome_report_payload(
        backfill_id=backfill_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"backfill_id={payload['backfill_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"available_count={payload['available_count']}")
    typer.echo(f"pending_count={payload['pending_count']}")
    typer.echo(f"insufficient_data_count={payload['insufficient_data_count']}")
    typer.echo(f"best_variant={payload['best_variant']}")
    typer.echo(f"report_path={payload['backfill_outcome_report_path']}")
    typer.echo("broker_action_taken=false")


@dynamic_v3_rescue_app.command("validate-backfill-outcome")
def dynamic_v3_validate_backfill_outcome_command(
    backfill_id: Annotated[str, typer.Option("--backfill-id", help="backfill id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="backfilled outcome artifact root。"),
    ] = DEFAULT_BACKFILLED_OUTCOME_DIR,
) -> None:
    """校验 TRADING-143 backfilled outcome artifact。"""
    payload = validate_backfill_outcome_artifact(
        backfill_id=backfill_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_taken=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


__all__ = [
    "dynamic_v3_backfill_outcome_run_command",
    "dynamic_v3_backfill_outcome_report_command",
    "dynamic_v3_validate_backfill_outcome_command",
]
