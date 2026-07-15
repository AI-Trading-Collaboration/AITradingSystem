from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Annotated, Any

import typer

from ai_trading_system.etf_portfolio import (
    dynamic_v3_weight_search_diagnostics as weight_batch_search,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_cash_buffer_attribution_app,
    dynamic_v3_near_miss_candidates_app,
    dynamic_v3_no_promotion_review_app,
    dynamic_v3_rescue_app,
    dynamic_v3_search_coverage_gap_app,
)


def _mapping_obj(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _texts(value: Any) -> list[str]:
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [str(item) for item in value]
    return []


@dynamic_v3_no_promotion_review_app.command("run")
def dynamic_v3_no_promotion_review_run_command(
    scorecard_id: Annotated[str, typer.Option("--scorecard-id", help="source scorecard id。")],
    scorecard_dir: Annotated[
        Path,
        typer.Option("--scorecard-dir", help="weight scorecard artifact root。"),
    ] = weight_batch_search.DEFAULT_WEIGHT_SCORECARD_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="no-promotion review artifact root。"),
    ] = weight_batch_search.DEFAULT_NO_PROMOTION_REVIEW_DIR,
) -> None:
    result = weight_batch_search.run_no_promotion_review(
        scorecard_id=scorecard_id,
        scorecard_dir=scorecard_dir,
        output_dir=output_dir,
    )
    summary = result["no_promotion_reason_summary"]
    typer.echo(f"review_id={result['review_id']}")
    typer.echo(f"status={result['manifest']['status']}")
    typer.echo(f"promoted_candidate_count={summary['promoted_candidate_count']}")
    typer.echo(f"gate_assessment={summary['gate_assessment']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_no_promotion_review_app.command("report")
def dynamic_v3_no_promotion_review_report_command(
    latest: Annotated[
        bool, typer.Option("--latest/--no-latest", help="读取 latest review。")
    ] = False,
    review_id: Annotated[str | None, typer.Option("--review-id", help="review id。")] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="no-promotion review artifact root。"),
    ] = weight_batch_search.DEFAULT_NO_PROMOTION_REVIEW_DIR,
) -> None:
    payload = weight_batch_search.no_promotion_review_report_payload(
        review_id=review_id,
        latest=latest,
        output_dir=output_dir,
    )
    summary = _mapping_obj(payload.get("no_promotion_reason_summary"))
    typer.echo(f"review_id={payload['review_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"gate_assessment={summary.get('gate_assessment')}")
    typer.echo(f"report_path={payload['no_promotion_review_report_path']}")


@dynamic_v3_rescue_app.command("validate-no-promotion-review")
def dynamic_v3_validate_no_promotion_review_command(
    review_id: Annotated[str, typer.Option("--review-id", help="review id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="no-promotion review artifact root。"),
    ] = weight_batch_search.DEFAULT_NO_PROMOTION_REVIEW_DIR,
) -> None:
    payload = weight_batch_search.validate_no_promotion_review_artifact(
        review_id=review_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_near_miss_candidates_app.command("extract")
def dynamic_v3_near_miss_candidates_extract_command(
    scorecard_id: Annotated[str, typer.Option("--scorecard-id", help="source scorecard id。")],
    no_promotion_review_id: Annotated[
        str,
        typer.Option("--no-promotion-review-id", help="no-promotion review id。"),
    ],
    scorecard_dir: Annotated[
        Path,
        typer.Option("--scorecard-dir", help="weight scorecard artifact root。"),
    ] = weight_batch_search.DEFAULT_WEIGHT_SCORECARD_DIR,
    review_dir: Annotated[
        Path,
        typer.Option("--review-dir", help="no-promotion review artifact root。"),
    ] = weight_batch_search.DEFAULT_NO_PROMOTION_REVIEW_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="near-miss artifact root。"),
    ] = weight_batch_search.DEFAULT_NEAR_MISS_CANDIDATES_DIR,
) -> None:
    result = weight_batch_search.extract_near_miss_candidates(
        scorecard_id=scorecard_id,
        no_promotion_review_id=no_promotion_review_id,
        scorecard_dir=scorecard_dir,
        review_dir=review_dir,
        output_dir=output_dir,
    )
    typer.echo(f"near_miss_id={result['near_miss_id']}")
    typer.echo(f"status={result['manifest']['status']}")
    typer.echo(f"candidate_count={result['manifest']['candidate_count']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_near_miss_candidates_app.command("report")
def dynamic_v3_near_miss_candidates_report_command(
    latest: Annotated[
        bool, typer.Option("--latest/--no-latest", help="读取 latest near-miss。")
    ] = False,
    near_miss_id: Annotated[
        str | None, typer.Option("--near-miss-id", help="near-miss id。")
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="near-miss artifact root。"),
    ] = weight_batch_search.DEFAULT_NEAR_MISS_CANDIDATES_DIR,
) -> None:
    payload = weight_batch_search.near_miss_candidates_report_payload(
        near_miss_id=near_miss_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"near_miss_id={payload['near_miss_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"candidate_count={payload['candidate_count']}")
    typer.echo(f"report_path={payload['near_miss_report_path']}")


@dynamic_v3_rescue_app.command("validate-near-miss-candidates")
def dynamic_v3_validate_near_miss_candidates_command(
    near_miss_id: Annotated[str, typer.Option("--near-miss-id", help="near-miss id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="near-miss artifact root。"),
    ] = weight_batch_search.DEFAULT_NEAR_MISS_CANDIDATES_DIR,
) -> None:
    payload = weight_batch_search.validate_near_miss_candidates_artifact(
        near_miss_id=near_miss_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_cash_buffer_attribution_app.command("run")
def dynamic_v3_cash_buffer_attribution_run_command(
    variant_id: Annotated[str, typer.Option("--variant-id", help="cash buffer variant id。")],
    scorecard_id: Annotated[str, typer.Option("--scorecard-id", help="source scorecard id。")],
    near_miss_id: Annotated[str, typer.Option("--near-miss-id", help="near-miss id。")],
    scorecard_dir: Annotated[
        Path,
        typer.Option("--scorecard-dir", help="weight scorecard artifact root。"),
    ] = weight_batch_search.DEFAULT_WEIGHT_SCORECARD_DIR,
    near_miss_dir: Annotated[
        Path,
        typer.Option("--near-miss-dir", help="near-miss artifact root。"),
    ] = weight_batch_search.DEFAULT_NEAR_MISS_CANDIDATES_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="cash buffer attribution root。"),
    ] = weight_batch_search.DEFAULT_CASH_BUFFER_ATTRIBUTION_DIR,
) -> None:
    result = weight_batch_search.run_cash_buffer_attribution(
        variant_id=variant_id,
        scorecard_id=scorecard_id,
        near_miss_id=near_miss_id,
        scorecard_dir=scorecard_dir,
        near_miss_dir=near_miss_dir,
        output_dir=output_dir,
    )
    failure = result["cash_buffer_failure_reason"]
    typer.echo(f"attribution_id={result['attribution_id']}")
    typer.echo(f"status={result['manifest']['status']}")
    typer.echo(f"primary_failure_reason={failure['primary_failure_reason']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_cash_buffer_attribution_app.command("report")
def dynamic_v3_cash_buffer_attribution_report_command(
    latest: Annotated[
        bool, typer.Option("--latest/--no-latest", help="读取 latest attribution。")
    ] = False,
    attribution_id: Annotated[
        str | None,
        typer.Option("--attribution-id", help="cash buffer attribution id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="cash buffer attribution root。"),
    ] = weight_batch_search.DEFAULT_CASH_BUFFER_ATTRIBUTION_DIR,
) -> None:
    payload = weight_batch_search.cash_buffer_attribution_report_payload(
        attribution_id=attribution_id,
        latest=latest,
        output_dir=output_dir,
    )
    failure = _mapping_obj(payload.get("cash_buffer_failure_reason"))
    typer.echo(f"attribution_id={payload['attribution_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"primary_failure_reason={failure.get('primary_failure_reason')}")
    typer.echo(f"report_path={payload['cash_buffer_attribution_report_path']}")


@dynamic_v3_rescue_app.command("validate-cash-buffer-attribution")
def dynamic_v3_validate_cash_buffer_attribution_command(
    attribution_id: Annotated[str, typer.Option("--attribution-id", help="attribution id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="cash buffer attribution root。"),
    ] = weight_batch_search.DEFAULT_CASH_BUFFER_ATTRIBUTION_DIR,
) -> None:
    payload = weight_batch_search.validate_cash_buffer_attribution_artifact(
        attribution_id=attribution_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_search_coverage_gap_app.command("run")
def dynamic_v3_search_coverage_gap_run_command(
    search_space_id: Annotated[str, typer.Option("--search-space-id", help="search space id。")],
    near_miss_id: Annotated[str, typer.Option("--near-miss-id", help="near-miss id。")],
    cash_buffer_attribution_id: Annotated[
        str,
        typer.Option("--cash-buffer-attribution-id", help="cash buffer attribution id。"),
    ],
    search_space_dir: Annotated[
        Path,
        typer.Option("--search-space-dir", help="weight search space artifact root。"),
    ] = weight_batch_search.DEFAULT_WEIGHT_SEARCH_SPACE_DIR,
    near_miss_dir: Annotated[
        Path,
        typer.Option("--near-miss-dir", help="near-miss artifact root。"),
    ] = weight_batch_search.DEFAULT_NEAR_MISS_CANDIDATES_DIR,
    attribution_dir: Annotated[
        Path,
        typer.Option("--attribution-dir", help="cash buffer attribution root。"),
    ] = weight_batch_search.DEFAULT_CASH_BUFFER_ATTRIBUTION_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="search coverage gap root。"),
    ] = weight_batch_search.DEFAULT_SEARCH_COVERAGE_GAP_DIR,
) -> None:
    result = weight_batch_search.run_search_coverage_gap(
        search_space_id=search_space_id,
        near_miss_id=near_miss_id,
        cash_buffer_attribution_id=cash_buffer_attribution_id,
        search_space_dir=search_space_dir,
        near_miss_dir=near_miss_dir,
        attribution_dir=attribution_dir,
        output_dir=output_dir,
    )
    typer.echo(f"coverage_gap_id={result['coverage_gap_id']}")
    typer.echo(f"status={result['manifest']['status']}")
    typer.echo(
        "recommended_focus="
        + ",".join(_texts(result["targeted_v3_recommendations"].get("recommended_focus")))
    )
    typer.echo("broker_action_allowed=false")


@dynamic_v3_search_coverage_gap_app.command("report")
def dynamic_v3_search_coverage_gap_report_command(
    latest: Annotated[
        bool, typer.Option("--latest/--no-latest", help="读取 latest coverage gap。")
    ] = False,
    coverage_gap_id: Annotated[
        str | None, typer.Option("--coverage-gap-id", help="coverage gap id。")
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="search coverage gap root。"),
    ] = weight_batch_search.DEFAULT_SEARCH_COVERAGE_GAP_DIR,
) -> None:
    payload = weight_batch_search.search_coverage_gap_report_payload(
        coverage_gap_id=coverage_gap_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"coverage_gap_id={payload['coverage_gap_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"report_path={payload['search_coverage_gap_report_path']}")


@dynamic_v3_rescue_app.command("validate-search-coverage-gap")
def dynamic_v3_validate_search_coverage_gap_command(
    coverage_gap_id: Annotated[str, typer.Option("--coverage-gap-id", help="coverage gap id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="search coverage gap root。"),
    ] = weight_batch_search.DEFAULT_SEARCH_COVERAGE_GAP_DIR,
) -> None:
    payload = weight_batch_search.validate_search_coverage_gap_artifact(
        coverage_gap_id=coverage_gap_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)
