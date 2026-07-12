from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio.dynamic_v3_historical_replay import (
    DEFAULT_REPLAY_INVENTORY_DIR,
    build_replay_inventory,
    replay_inventory_report_payload,
    validate_replay_inventory_artifact,
)
from ai_trading_system.etf_portfolio.dynamic_v3_paper_tracking import (
    DEFAULT_PAPER_PORTFOLIO_CONFIG_PATH,
    DEFAULT_PAPER_PORTFOLIO_DIR,
)
from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    DEFAULT_CONSENSUS_DRIFT_DIR,
    DEFAULT_OWNER_REVIEW_JOURNAL_DIR,
    DEFAULT_POSITION_ADVISORY_DAILY_DIR,
    DEFAULT_SHADOW_MONITOR_RUN_DIR,
)
from ai_trading_system.etf_portfolio.models import DEFAULT_ETF_PRICE_PATH
from ai_trading_system.interfaces.cli.etf_portfolio.common import parse_date
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_replay_inventory_app,
    dynamic_v3_rescue_app,
)


@dynamic_v3_replay_inventory_app.command("build")
def dynamic_v3_replay_inventory_build_command(
    start: Annotated[str, typer.Option("--start", help="replay inventory start date。")],
    end: Annotated[str, typer.Option("--end", help="replay inventory end date。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="replay inventory artifact root。"),
    ] = DEFAULT_REPLAY_INVENTORY_DIR,
    daily_advisory_dir: Annotated[
        Path,
        typer.Option("--daily-advisory-dir", help="daily advisory artifact root。"),
    ] = DEFAULT_POSITION_ADVISORY_DAILY_DIR,
    shadow_monitor_run_dir: Annotated[
        Path,
        typer.Option("--shadow-monitor-run-dir", help="shadow monitor run artifact root。"),
    ] = DEFAULT_SHADOW_MONITOR_RUN_DIR,
    consensus_drift_dir: Annotated[
        Path,
        typer.Option("--consensus-drift-dir", help="consensus drift artifact root。"),
    ] = DEFAULT_CONSENSUS_DRIFT_DIR,
    owner_review_dir: Annotated[
        Path,
        typer.Option("--owner-review-dir", help="owner review journal root。"),
    ] = DEFAULT_OWNER_REVIEW_JOURNAL_DIR,
    paper_portfolio_dir: Annotated[
        Path,
        typer.Option("--paper-portfolio-dir", help="paper portfolio artifact root。"),
    ] = DEFAULT_PAPER_PORTFOLIO_DIR,
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="cached ETF price path。"),
    ] = DEFAULT_ETF_PRICE_PATH,
    config_path: Annotated[
        Path,
        typer.Option("--config", "--config-path", help="paper portfolio config。"),
    ] = DEFAULT_PAPER_PORTFOLIO_CONFIG_PATH,
) -> None:
    """构建 TRADING-141 historical replay inventory 和 PIT safety audit。"""
    result = build_replay_inventory(
        start=parse_date(start),
        end=parse_date(end),
        output_dir=output_dir,
        daily_advisory_dir=daily_advisory_dir,
        shadow_monitor_run_dir=shadow_monitor_run_dir,
        consensus_drift_dir=consensus_drift_dir,
        owner_review_dir=owner_review_dir,
        paper_portfolio_dir=paper_portfolio_dir,
        prices_path=prices_path,
        config_path=config_path,
    )
    manifest = result["manifest"]
    typer.echo(f"inventory_id={result['inventory_id']}")
    typer.echo(f"inventory_dir={result['inventory_dir']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(f"total_replay_events={manifest['total_replay_events']}")
    typer.echo(f"pit_safe_count={manifest['pit_safe_count']}")
    typer.echo(f"pit_warning_count={manifest['pit_warning_count']}")
    typer.echo(f"pit_unsafe_count={manifest['pit_unsafe_count']}")
    typer.echo("broker_action_taken=false")


@dynamic_v3_replay_inventory_app.command("report")
def dynamic_v3_replay_inventory_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest replay inventory。"),
    ] = False,
    inventory_id: Annotated[
        str | None,
        typer.Option("--inventory-id", help="replay inventory id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="replay inventory artifact root。"),
    ] = DEFAULT_REPLAY_INVENTORY_DIR,
) -> None:
    """展示 TRADING-141 replay inventory 摘要。"""
    payload = replay_inventory_report_payload(
        inventory_id=inventory_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"inventory_id={payload['inventory_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"total_replay_events={payload['total_replay_events']}")
    typer.echo(f"pit_safe_count={payload['pit_safe_count']}")
    typer.echo(f"pit_warning_count={payload['pit_warning_count']}")
    typer.echo(f"pit_unsafe_count={payload['pit_unsafe_count']}")
    typer.echo(f"report_path={payload['replay_inventory_report_path']}")
    typer.echo("broker_action_taken=false")


@dynamic_v3_rescue_app.command("validate-replay-inventory")
def dynamic_v3_validate_replay_inventory_command(
    inventory_id: Annotated[str, typer.Option("--inventory-id", help="inventory id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="replay inventory artifact root。"),
    ] = DEFAULT_REPLAY_INVENTORY_DIR,
) -> None:
    """校验 TRADING-141 replay inventory artifact。"""
    payload = validate_replay_inventory_artifact(
        inventory_id=inventory_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_taken=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


__all__ = [
    "dynamic_v3_replay_inventory_build_command",
    "dynamic_v3_replay_inventory_report_command",
    "dynamic_v3_validate_replay_inventory_command",
]
