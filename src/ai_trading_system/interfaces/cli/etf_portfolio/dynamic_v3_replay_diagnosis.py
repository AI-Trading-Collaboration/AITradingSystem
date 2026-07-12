from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio.dynamic_v3_historical_replay import (
    DEFAULT_BACKFILLED_OUTCOME_DIR,
    DEFAULT_HISTORICAL_PAPER_SIM_DIR,
    DEFAULT_HISTORICAL_REPLAY_DIR,
    DEFAULT_REPLAY_DIAGNOSIS_DIR,
    DEFAULT_REPLAY_INVENTORY_DIR,
    DEFAULT_REPLAY_PERFORMANCE_REVIEW_DIR,
    replay_diagnosis_report_payload,
    run_replay_diagnosis,
    validate_replay_diagnosis_artifact,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_replay_diagnosis_app,
    dynamic_v3_rescue_app,
)


@dynamic_v3_replay_diagnosis_app.command("run")
def dynamic_v3_replay_diagnosis_run_command(
    inventory_id: Annotated[str, typer.Option("--inventory-id", help="inventory id。")],
    replay_id: Annotated[str, typer.Option("--replay-id", help="replay id。")],
    backfill_id: Annotated[str, typer.Option("--backfill-id", help="backfill id。")],
    sim_id: Annotated[str, typer.Option("--sim-id", help="historical paper sim id。")],
    review_id: Annotated[
        str,
        typer.Option("--review-id", help="replay performance review id。"),
    ],
    inventory_dir: Annotated[
        Path,
        typer.Option("--inventory-dir", help="replay inventory artifact root。"),
    ] = DEFAULT_REPLAY_INVENTORY_DIR,
    replay_dir: Annotated[
        Path,
        typer.Option("--replay-dir", help="historical replay artifact root。"),
    ] = DEFAULT_HISTORICAL_REPLAY_DIR,
    backfill_dir: Annotated[
        Path,
        typer.Option("--backfill-dir", help="backfilled outcome artifact root。"),
    ] = DEFAULT_BACKFILLED_OUTCOME_DIR,
    sim_dir: Annotated[
        Path,
        typer.Option("--sim-dir", help="historical paper sim artifact root。"),
    ] = DEFAULT_HISTORICAL_PAPER_SIM_DIR,
    review_dir: Annotated[
        Path,
        typer.Option("--review-dir", help="replay performance review artifact root。"),
    ] = DEFAULT_REPLAY_PERFORMANCE_REVIEW_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="replay diagnosis artifact root。"),
    ] = DEFAULT_REPLAY_DIAGNOSIS_DIR,
) -> None:
    """运行 TRADING-146_to_150 replay coverage diagnosis。"""
    result = run_replay_diagnosis(
        inventory_id=inventory_id,
        replay_id=replay_id,
        backfill_id=backfill_id,
        sim_id=sim_id,
        review_id=review_id,
        inventory_dir=inventory_dir,
        replay_dir=replay_dir,
        backfill_dir=backfill_dir,
        sim_dir=sim_dir,
        review_dir=review_dir,
        output_dir=output_dir,
    )
    coverage = result["coverage_breakdown"]
    pending = result["pending_reason_summary"]["pending_reasons"]
    top_reason = pending[0]["reason"] if pending else "MISSING"
    typer.echo(f"diagnosis_id={result['diagnosis_id']}")
    typer.echo(f"diagnosis_dir={result['diagnosis_dir']}")
    typer.echo(f"status={result['manifest']['status']}")
    typer.echo(f"pit_safe_count={coverage['inventory']['pit_safe']}")
    typer.echo(f"pit_warning_count={coverage['inventory']['pit_warning']}")
    typer.echo(f"pit_unsafe_count={coverage['inventory']['pit_unsafe']}")
    typer.echo(f"available_windows={coverage['backfill']['available_windows']}")
    typer.echo(f"pending_windows={coverage['backfill']['pending_windows']}")
    typer.echo(f"insufficient_data_windows={coverage['backfill']['insufficient_data_windows']}")
    typer.echo(f"top_pending_reason={top_reason}")
    typer.echo("production_effect=none")
    typer.echo("broker_action_taken=false")


@dynamic_v3_replay_diagnosis_app.command("report")
def dynamic_v3_replay_diagnosis_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest replay diagnosis。"),
    ] = False,
    diagnosis_id: Annotated[
        str | None,
        typer.Option("--diagnosis-id", help="diagnosis id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="replay diagnosis artifact root。"),
    ] = DEFAULT_REPLAY_DIAGNOSIS_DIR,
) -> None:
    """展示 replay diagnosis 摘要。"""
    payload = replay_diagnosis_report_payload(
        diagnosis_id=diagnosis_id,
        latest=latest,
        output_dir=output_dir,
    )
    coverage = payload["replay_coverage_breakdown"]
    pending = payload["replay_pending_reason_summary"]["pending_reasons"]
    typer.echo(f"diagnosis_id={payload['diagnosis_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"pit_safe_count={coverage['inventory']['pit_safe']}")
    typer.echo(f"pit_warning_count={coverage['inventory']['pit_warning']}")
    typer.echo(f"pit_unsafe_count={coverage['inventory']['pit_unsafe']}")
    typer.echo(f"available_windows={coverage['backfill']['available_windows']}")
    typer.echo(f"pending_windows={coverage['backfill']['pending_windows']}")
    typer.echo(f"top_pending_reason={pending[0]['reason'] if pending else 'MISSING'}")
    typer.echo(f"report_path={payload['replay_diagnosis_report_path']}")
    typer.echo("production_effect=none")
    typer.echo("broker_action_taken=false")


@dynamic_v3_rescue_app.command("validate-replay-diagnosis")
def dynamic_v3_validate_replay_diagnosis_command(
    diagnosis_id: Annotated[str, typer.Option("--diagnosis-id", help="diagnosis id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="replay diagnosis artifact root。"),
    ] = DEFAULT_REPLAY_DIAGNOSIS_DIR,
) -> None:
    """校验 TRADING-146_to_150 replay diagnosis artifact。"""
    payload = validate_replay_diagnosis_artifact(
        diagnosis_id=diagnosis_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_effect=none")
    typer.echo("broker_action_taken=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


__all__ = [
    "dynamic_v3_replay_diagnosis_run_command",
    "dynamic_v3_replay_diagnosis_report_command",
    "dynamic_v3_validate_replay_diagnosis_command",
]
