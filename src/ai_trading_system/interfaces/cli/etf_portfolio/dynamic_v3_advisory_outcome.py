from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio.dynamic_v3_paper_tracking import (
    DEFAULT_ADVISORY_OUTCOME_DIR,
    DEFAULT_PAPER_PORTFOLIO_CONFIG_PATH,
    DEFAULT_PAPER_PORTFOLIO_DIR,
    DEFAULT_RATES_CACHE_PATH,
    advisory_outcome_report_payload,
    track_advisory_outcome,
    update_advisory_outcome,
    validate_advisory_outcome_artifact,
)
from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    DEFAULT_POSITION_ADVISORY_DAILY_DIR,
)
from ai_trading_system.etf_portfolio.models import DEFAULT_ETF_PRICE_PATH
from ai_trading_system.interfaces.cli.etf_portfolio.common import parse_date
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_advisory_outcome_app,
    dynamic_v3_rescue_app,
)


@dynamic_v3_advisory_outcome_app.command("track")
def dynamic_v3_advisory_outcome_track_command(
    daily_advisory_id: Annotated[
        str,
        typer.Option("--daily-advisory-id", help="daily advisory id。"),
    ],
    config_path: Annotated[
        Path,
        typer.Option("--config", "--config-path", help="paper portfolio config。"),
    ] = DEFAULT_PAPER_PORTFOLIO_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="advisory outcome artifact root。"),
    ] = DEFAULT_ADVISORY_OUTCOME_DIR,
    daily_advisory_dir: Annotated[
        Path,
        typer.Option("--daily-advisory-dir", help="daily advisory artifact root。"),
    ] = DEFAULT_POSITION_ADVISORY_DAILY_DIR,
    paper_portfolio_dir: Annotated[
        Path,
        typer.Option("--paper-portfolio-dir", help="paper portfolio artifact root。"),
    ] = DEFAULT_PAPER_PORTFOLIO_DIR,
) -> None:
    """创建 TRADING-137 advisory outcome tracker。"""
    result = track_advisory_outcome(
        daily_advisory_id=daily_advisory_id,
        config_path=config_path,
        output_dir=output_dir,
        daily_advisory_dir=daily_advisory_dir,
        paper_portfolio_dir=paper_portfolio_dir,
    )
    manifest = result["manifest"]
    typer.echo(f"outcome_id={result['outcome_id']}")
    typer.echo(f"outcome_dir={result['outcome_dir']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(f"tracked_windows={','.join(str(item) for item in manifest['tracked_windows'])}")
    typer.echo("broker_action_taken=false")


@dynamic_v3_advisory_outcome_app.command("update")
def dynamic_v3_advisory_outcome_update_command(
    as_of: Annotated[str, typer.Option("--as-of", help="update as-of date。")],
    outcome_id: Annotated[
        str | None,
        typer.Option("--outcome-id", help="optional outcome id；default latest。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="advisory outcome artifact root。"),
    ] = DEFAULT_ADVISORY_OUTCOME_DIR,
    paper_portfolio_dir: Annotated[
        Path,
        typer.Option("--paper-portfolio-dir", help="paper portfolio artifact root。"),
    ] = DEFAULT_PAPER_PORTFOLIO_DIR,
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="cached price path。"),
    ] = DEFAULT_ETF_PRICE_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="cached rates path。"),
    ] = DEFAULT_RATES_CACHE_PATH,
) -> None:
    """更新 TRADING-137 advisory outcome windows。"""
    result = update_advisory_outcome(
        as_of=parse_date(as_of),
        outcome_id=outcome_id,
        output_dir=output_dir,
        paper_portfolio_dir=paper_portfolio_dir,
        prices_path=prices_path,
        rates_path=rates_path,
    )
    manifest = result["manifest"]
    typer.echo(f"outcome_id={result['outcome_id']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(f"data_quality_status={manifest['data_quality_status']}")
    typer.echo("broker_action_taken=false")


@dynamic_v3_advisory_outcome_app.command("report")
def dynamic_v3_advisory_outcome_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest advisory outcome。"),
    ] = False,
    outcome_id: Annotated[str | None, typer.Option("--outcome-id", help="outcome id。")]
    = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="advisory outcome artifact root。"),
    ] = DEFAULT_ADVISORY_OUTCOME_DIR,
) -> None:
    """展示 TRADING-137 advisory outcome 摘要。"""
    payload = advisory_outcome_report_payload(
        outcome_id=outcome_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"outcome_id={payload['outcome_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"data_quality_status={payload['data_quality_status']}")
    typer.echo(f"report_path={payload['advisory_outcome_report_path']}")
    typer.echo("broker_action_taken=false")


@dynamic_v3_rescue_app.command("validate-advisory-outcome")
def dynamic_v3_validate_advisory_outcome_command(
    outcome_id: Annotated[str, typer.Option("--outcome-id", help="outcome id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="advisory outcome artifact root。"),
    ] = DEFAULT_ADVISORY_OUTCOME_DIR,
) -> None:
    """校验 TRADING-137 advisory outcome artifact。"""
    payload = validate_advisory_outcome_artifact(outcome_id=outcome_id, output_dir=output_dir)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_taken=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


__all__ = [
    "dynamic_v3_advisory_outcome_report_command",
    "dynamic_v3_advisory_outcome_track_command",
    "dynamic_v3_advisory_outcome_update_command",
    "dynamic_v3_validate_advisory_outcome_command",
]
