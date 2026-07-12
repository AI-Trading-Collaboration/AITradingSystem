from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio.dynamic_v3_historical_replay import (
    DEFAULT_BACKFILL_REPAIR_DIR,
    DEFAULT_BACKFILLED_OUTCOME_DIR,
    DEFAULT_HISTORICAL_REPLAY_DIR,
    DEFAULT_REPLAY_DIAGNOSIS_DIR,
    backfill_repair_report_payload,
    run_backfill_repair,
    validate_backfill_repair_artifact,
)
from ai_trading_system.etf_portfolio.dynamic_v3_paper_tracking import (
    DEFAULT_RATES_CACHE_PATH,
)
from ai_trading_system.etf_portfolio.models import DEFAULT_ETF_PRICE_PATH
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_backfill_repair_app,
    dynamic_v3_rescue_app,
)


@dynamic_v3_backfill_repair_app.command("run")
def dynamic_v3_backfill_repair_run_command(
    backfill_id: Annotated[str, typer.Option("--backfill-id", help="backfill id。")],
    diagnosis_id: Annotated[str, typer.Option("--diagnosis-id", help="diagnosis id。")],
    backfill_dir: Annotated[
        Path,
        typer.Option("--backfill-dir", help="backfilled outcome artifact root。"),
    ] = DEFAULT_BACKFILLED_OUTCOME_DIR,
    diagnosis_dir: Annotated[
        Path,
        typer.Option("--diagnosis-dir", help="replay diagnosis artifact root。"),
    ] = DEFAULT_REPLAY_DIAGNOSIS_DIR,
    replay_dir: Annotated[
        Path,
        typer.Option("--replay-dir", help="historical replay artifact root。"),
    ] = DEFAULT_HISTORICAL_REPLAY_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="backfill repair artifact root。"),
    ] = DEFAULT_BACKFILL_REPAIR_DIR,
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="cached ETF price path。"),
    ] = DEFAULT_ETF_PRICE_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="cached rates path。"),
    ] = DEFAULT_RATES_CACHE_PATH,
) -> None:
    """运行 TRADING-147 backfilled outcome availability repair。"""
    result = run_backfill_repair(
        backfill_id=backfill_id,
        diagnosis_id=diagnosis_id,
        backfill_dir=backfill_dir,
        diagnosis_dir=diagnosis_dir,
        replay_dir=replay_dir,
        output_dir=output_dir,
        prices_path=prices_path,
        rates_path=rates_path,
    )
    delta = result["backfill_availability_delta"]
    typer.echo(f"repair_id={result['repair_id']}")
    typer.echo(f"repair_dir={result['repair_dir']}")
    typer.echo(f"status={result['manifest']['status']}")
    typer.echo(f"repaired_count={delta['repaired_count']}")
    typer.echo(f"still_pending_count={delta['still_pending_count']}")
    typer.echo(f"still_insufficient_count={delta['still_insufficient_count']}")
    typer.echo("future_data_used_in_decision=false")
    typer.echo("production_effect=none")
    typer.echo("broker_action_taken=false")


@dynamic_v3_backfill_repair_app.command("report")
def dynamic_v3_backfill_repair_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest backfill repair。"),
    ] = False,
    repair_id: Annotated[str | None, typer.Option("--repair-id", help="repair id。")] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="backfill repair artifact root。"),
    ] = DEFAULT_BACKFILL_REPAIR_DIR,
) -> None:
    """展示 backfill repair 摘要。"""
    payload = backfill_repair_report_payload(
        repair_id=repair_id, latest=latest, output_dir=output_dir
    )
    delta = payload["backfill_availability_delta"]
    typer.echo(f"repair_id={payload['repair_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"repaired_count={delta['repaired_count']}")
    typer.echo(f"still_pending_count={delta['still_pending_count']}")
    typer.echo(f"still_insufficient_count={delta['still_insufficient_count']}")
    typer.echo(f"report_path={payload['backfill_repair_report_path']}")
    typer.echo("future_data_used_in_decision=false")
    typer.echo("production_effect=none")


@dynamic_v3_rescue_app.command("validate-backfill-repair")
def dynamic_v3_validate_backfill_repair_command(
    repair_id: Annotated[str, typer.Option("--repair-id", help="repair id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="backfill repair artifact root。"),
    ] = DEFAULT_BACKFILL_REPAIR_DIR,
) -> None:
    """校验 TRADING-147 backfill repair artifact。"""
    payload = validate_backfill_repair_artifact(repair_id=repair_id, output_dir=output_dir)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("future_data_used_in_decision=false")
    typer.echo("production_effect=none")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


__all__ = [
    "dynamic_v3_backfill_repair_run_command",
    "dynamic_v3_backfill_repair_report_command",
    "dynamic_v3_validate_backfill_repair_command",
]
