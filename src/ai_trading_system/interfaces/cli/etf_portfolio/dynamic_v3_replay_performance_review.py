from __future__ import annotations

from pathlib import Path
from typing import Annotated, Any

import typer

from ai_trading_system.etf_portfolio.dynamic_v3_historical_replay import (
    DEFAULT_BACKFILLED_OUTCOME_DIR,
    DEFAULT_HISTORICAL_PAPER_SIM_DIR,
    DEFAULT_REPLAY_PERFORMANCE_REVIEW_DIR,
    replay_performance_review_report_payload,
    run_replay_performance_review,
    validate_replay_performance_review_artifact,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_replay_performance_review_app,
    dynamic_v3_rescue_app,
)


@dynamic_v3_replay_performance_review_app.command("run")
def dynamic_v3_replay_performance_review_run_command(
    backfill_id: Annotated[str, typer.Option("--backfill-id", help="backfill id。")],
    sim_id: Annotated[str, typer.Option("--sim-id", help="historical paper sim id。")],
    backfill_dir: Annotated[
        Path,
        typer.Option("--backfill-dir", help="backfilled outcome artifact root。"),
    ] = DEFAULT_BACKFILLED_OUTCOME_DIR,
    sim_dir: Annotated[
        Path,
        typer.Option("--sim-dir", help="historical paper sim artifact root。"),
    ] = DEFAULT_HISTORICAL_PAPER_SIM_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="replay performance review artifact root。"),
    ] = DEFAULT_REPLAY_PERFORMANCE_REVIEW_DIR,
) -> None:
    """运行 TRADING-145 replay performance review。"""
    result = run_replay_performance_review(
        backfill_id=backfill_id,
        sim_id=sim_id,
        backfill_dir=backfill_dir,
        sim_dir=sim_dir,
        output_dir=output_dir,
    )
    manifest = result["manifest"]
    rec = result["calibration_recommendations"]["recommendations"][0]
    typer.echo(f"review_id={result['review_id']}")
    typer.echo(f"review_dir={result['review_dir']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(f"best_variant={manifest['best_variant']}")
    typer.echo(f"calibration_recommendation={rec['type']}")
    typer.echo(f"requires_owner_approval={rec['requires_owner_approval']}")
    typer.echo("production_effect=none")
    typer.echo("broker_action_taken=false")


@dynamic_v3_replay_performance_review_app.command("report")
def dynamic_v3_replay_performance_review_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest replay performance review。"),
    ] = False,
    review_id: Annotated[str | None, typer.Option("--review-id", help="review id。")] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="replay performance review artifact root。"),
    ] = DEFAULT_REPLAY_PERFORMANCE_REVIEW_DIR,
) -> None:
    """展示 TRADING-145 replay performance review 摘要。"""
    payload = replay_performance_review_report_payload(
        review_id=review_id,
        latest=latest,
        output_dir=output_dir,
    )
    recommendations = payload.get("calibration_recommendations")
    recs = recommendations.get("recommendations", []) if isinstance(recommendations, dict) else []
    first: dict[str, Any] = recs[0] if recs and isinstance(recs[0], dict) else {}
    typer.echo(f"review_id={payload['review_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"best_variant={payload['best_variant']}")
    typer.echo(f"available_outcome_count={payload['available_outcome_count']}")
    typer.echo(f"calibration_recommendation={first.get('type', 'MISSING')}")
    typer.echo(f"report_path={payload['replay_performance_review_path']}")
    typer.echo("production_effect=none")
    typer.echo("broker_action_taken=false")


@dynamic_v3_rescue_app.command("validate-replay-performance-review")
def dynamic_v3_validate_replay_performance_review_command(
    review_id: Annotated[str, typer.Option("--review-id", help="review id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="replay performance review artifact root。"),
    ] = DEFAULT_REPLAY_PERFORMANCE_REVIEW_DIR,
) -> None:
    """校验 TRADING-145 replay performance review artifact。"""
    payload = validate_replay_performance_review_artifact(
        review_id=review_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_effect=none")
    typer.echo("broker_action_taken=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


__all__ = [
    "dynamic_v3_replay_performance_review_run_command",
    "dynamic_v3_replay_performance_review_report_command",
    "dynamic_v3_validate_replay_performance_review_command",
]
