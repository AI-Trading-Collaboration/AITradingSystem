from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Annotated, Any

import typer

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target
from ai_trading_system.etf_portfolio import (
    dynamic_v3_weight_search_targeted as weight_batch_search,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_near_miss_ab_comparison_app,
    dynamic_v3_rescue_app,
    dynamic_v3_targeted_search_v3_app,
    dynamic_v3_targeted_v3_backfill_app,
)


def _mapping_obj(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


@dynamic_v3_targeted_search_v3_app.command("build")
def dynamic_v3_targeted_search_v3_build_command(
    coverage_gap_id: Annotated[str, typer.Option("--coverage-gap-id", help="coverage gap id。")],
    coverage_gap_dir: Annotated[
        Path,
        typer.Option("--coverage-gap-dir", help="search coverage gap root。"),
    ] = weight_batch_search.DEFAULT_SEARCH_COVERAGE_GAP_DIR,
    near_miss_dir: Annotated[
        Path,
        typer.Option("--near-miss-dir", help="near-miss artifact root。"),
    ] = weight_batch_search.DEFAULT_NEAR_MISS_CANDIDATES_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="targeted search v3 matrix root。"),
    ] = weight_batch_search.DEFAULT_TARGETED_SEARCH_V3_DIR,
) -> None:
    result = weight_batch_search.build_targeted_search_v3(
        coverage_gap_id=coverage_gap_id,
        coverage_gap_dir=coverage_gap_dir,
        near_miss_dir=near_miss_dir,
        output_dir=output_dir,
    )
    typer.echo(f"v3_matrix_id={result['v3_matrix_id']}")
    typer.echo(f"status={result['manifest']['status']}")
    typer.echo(f"variant_count={result['manifest']['variant_count']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_targeted_search_v3_app.command("report")
def dynamic_v3_targeted_search_v3_report_command(
    latest: Annotated[
        bool, typer.Option("--latest/--no-latest", help="读取 latest v3 matrix。")
    ] = False,
    v3_matrix_id: Annotated[
        str | None, typer.Option("--v3-matrix-id", help="v3 matrix id。")
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="targeted search v3 matrix root。"),
    ] = weight_batch_search.DEFAULT_TARGETED_SEARCH_V3_DIR,
) -> None:
    payload = weight_batch_search.targeted_search_v3_report_payload(
        v3_matrix_id=v3_matrix_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"v3_matrix_id={payload['v3_matrix_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"variant_count={payload['variant_count']}")
    typer.echo(f"report_path={payload['targeted_search_v3_report_path']}")


@dynamic_v3_rescue_app.command("validate-targeted-search-v3")
def dynamic_v3_validate_targeted_search_v3_command(
    v3_matrix_id: Annotated[str, typer.Option("--v3-matrix-id", help="v3 matrix id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="targeted search v3 matrix root。"),
    ] = weight_batch_search.DEFAULT_TARGETED_SEARCH_V3_DIR,
) -> None:
    payload = weight_batch_search.validate_targeted_search_v3_artifact(
        v3_matrix_id=v3_matrix_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_targeted_v3_backfill_app.command("run")
def dynamic_v3_targeted_v3_backfill_run_command(
    v3_matrix_id: Annotated[str, typer.Option("--v3-matrix-id", help="v3 matrix id。")],
    v3_matrix_dir: Annotated[
        Path,
        typer.Option("--v3-matrix-dir", help="targeted search v3 matrix root。"),
    ] = weight_batch_search.DEFAULT_TARGETED_SEARCH_V3_DIR,
    baseline_backfill_dir: Annotated[
        Path,
        typer.Option("--baseline-backfill-dir", help="paper shadow backfill root。"),
    ] = system_target.DEFAULT_PAPER_SHADOW_BACKFILL_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="targeted v3 backfill root。"),
    ] = weight_batch_search.DEFAULT_TARGETED_V3_BACKFILL_DIR,
    price_cache_path: Annotated[
        Path | None,
        typer.Option("--price-cache-path", help="price cache override。"),
    ] = None,
    rates_cache_path: Annotated[
        Path,
        typer.Option("--rates-cache-path", help="rates cache path。"),
    ] = system_target.DEFAULT_RATES_CACHE_PATH,
) -> None:
    result = weight_batch_search.run_targeted_v3_backfill(
        v3_matrix_id=v3_matrix_id,
        v3_matrix_dir=v3_matrix_dir,
        baseline_backfill_dir=baseline_backfill_dir,
        output_dir=output_dir,
        price_cache_path=price_cache_path,
        rates_cache_path=rates_cache_path,
    )
    manifest = result["manifest"]
    typer.echo(f"v3_backfill_id={result['v3_backfill_id']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(f"data_quality_status={manifest['data_quality_status']}")
    typer.echo(f"variants_completed={manifest['variants_completed']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_targeted_v3_backfill_app.command("resume")
def dynamic_v3_targeted_v3_backfill_resume_command(
    v3_backfill_id: Annotated[str, typer.Option("--v3-backfill-id", help="v3 backfill id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="targeted v3 backfill root。"),
    ] = weight_batch_search.DEFAULT_TARGETED_V3_BACKFILL_DIR,
) -> None:
    result = weight_batch_search.resume_targeted_v3_backfill(
        v3_backfill_id=v3_backfill_id,
        output_dir=output_dir,
    )
    progress = _mapping_obj(result["progress"])
    typer.echo(f"v3_backfill_id={result['v3_backfill_id']}")
    typer.echo(f"resume_status={result['resume_status']}")
    typer.echo(f"variants_completed={progress.get('variants_completed')}")


@dynamic_v3_targeted_v3_backfill_app.command("report")
def dynamic_v3_targeted_v3_backfill_report_command(
    latest: Annotated[
        bool, typer.Option("--latest/--no-latest", help="读取 latest v3 backfill。")
    ] = False,
    v3_backfill_id: Annotated[
        str | None, typer.Option("--v3-backfill-id", help="v3 backfill id。")
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="targeted v3 backfill root。"),
    ] = weight_batch_search.DEFAULT_TARGETED_V3_BACKFILL_DIR,
) -> None:
    payload = weight_batch_search.targeted_v3_backfill_report_payload(
        v3_backfill_id=v3_backfill_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"v3_backfill_id={payload['v3_backfill_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"data_quality_status={payload['data_quality_status']}")
    typer.echo(f"report_path={payload['targeted_v3_backfill_report_path']}")


@dynamic_v3_rescue_app.command("validate-targeted-v3-backfill")
def dynamic_v3_validate_targeted_v3_backfill_command(
    v3_backfill_id: Annotated[str, typer.Option("--v3-backfill-id", help="v3 backfill id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="targeted v3 backfill root。"),
    ] = weight_batch_search.DEFAULT_TARGETED_V3_BACKFILL_DIR,
) -> None:
    payload = weight_batch_search.validate_targeted_v3_backfill_artifact(
        v3_backfill_id=v3_backfill_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_near_miss_ab_comparison_app.command("run")
def dynamic_v3_near_miss_ab_comparison_run_command(
    v3_backfill_id: Annotated[str, typer.Option("--v3-backfill-id", help="v3 backfill id。")],
    near_miss_id: Annotated[str, typer.Option("--near-miss-id", help="near-miss id。")],
    v3_backfill_dir: Annotated[
        Path,
        typer.Option("--v3-backfill-dir", help="targeted v3 backfill root。"),
    ] = weight_batch_search.DEFAULT_TARGETED_V3_BACKFILL_DIR,
    v3_matrix_dir: Annotated[
        Path,
        typer.Option("--v3-matrix-dir", help="targeted search v3 matrix root。"),
    ] = weight_batch_search.DEFAULT_TARGETED_SEARCH_V3_DIR,
    near_miss_dir: Annotated[
        Path,
        typer.Option("--near-miss-dir", help="near-miss artifact root。"),
    ] = weight_batch_search.DEFAULT_NEAR_MISS_CANDIDATES_DIR,
    scorecard_dir: Annotated[
        Path,
        typer.Option("--scorecard-dir", help="weight scorecard artifact root。"),
    ] = weight_batch_search.DEFAULT_WEIGHT_SCORECARD_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="A/B comparison root。"),
    ] = weight_batch_search.DEFAULT_NEAR_MISS_AB_COMPARISON_DIR,
) -> None:
    result = weight_batch_search.run_near_miss_ab_comparison(
        v3_backfill_id=v3_backfill_id,
        near_miss_id=near_miss_id,
        v3_backfill_dir=v3_backfill_dir,
        v3_matrix_dir=v3_matrix_dir,
        near_miss_dir=near_miss_dir,
        scorecard_dir=scorecard_dir,
        output_dir=output_dir,
    )
    summary = result["ab_winner_summary"]
    typer.echo(f"ab_id={result['ab_id']}")
    typer.echo(f"status={result['manifest']['status']}")
    typer.echo(f"best_v3_variant={summary['best_v3_variant']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_near_miss_ab_comparison_app.command("report")
def dynamic_v3_near_miss_ab_comparison_report_command(
    latest: Annotated[bool, typer.Option("--latest/--no-latest", help="读取 latest A/B。")] = False,
    ab_id: Annotated[str | None, typer.Option("--ab-id", help="A/B id。")] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="A/B comparison root。"),
    ] = weight_batch_search.DEFAULT_NEAR_MISS_AB_COMPARISON_DIR,
) -> None:
    payload = weight_batch_search.near_miss_ab_comparison_report_payload(
        ab_id=ab_id,
        latest=latest,
        output_dir=output_dir,
    )
    summary = _mapping_obj(payload.get("ab_winner_summary"))
    typer.echo(f"ab_id={payload['ab_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"best_v3_variant={summary.get('best_v3_variant')}")
    typer.echo(f"report_path={payload['near_miss_ab_comparison_report_path']}")


@dynamic_v3_rescue_app.command("validate-near-miss-ab-comparison")
def dynamic_v3_validate_near_miss_ab_comparison_command(
    ab_id: Annotated[str, typer.Option("--ab-id", help="A/B id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="A/B comparison root。"),
    ] = weight_batch_search.DEFAULT_NEAR_MISS_AB_COMPARISON_DIR,
) -> None:
    payload = weight_batch_search.validate_near_miss_ab_comparison_artifact(
        ab_id=ab_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)
