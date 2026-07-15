from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target
from ai_trading_system.etf_portfolio import (
    dynamic_v3_weight_search_evaluation as weight_batch_search,
)
from ai_trading_system.interfaces.cli.etf_portfolio.common import mapping_obj as _mapping_obj
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_rescue_app,
    dynamic_v3_weight_adaptive_branch_app,
    dynamic_v3_weight_expanded_search_app,
    dynamic_v3_weight_robustness_review_app,
    dynamic_v3_weight_scorecard_app,
)


@dynamic_v3_weight_scorecard_app.command("run")
def dynamic_v3_weight_scorecard_run_command(
    backfill_id: Annotated[str, typer.Option("--backfill-id", help="batch backfill id。")],
    backfill_dir: Annotated[
        Path,
        typer.Option("--backfill-dir", help="batch backfill artifact root。"),
    ] = weight_batch_search.DEFAULT_WEIGHT_BATCH_BACKFILL_DIR,
    matrix_dir: Annotated[
        Path,
        typer.Option("--matrix-dir", help="Batch-2 matrix artifact root。"),
    ] = weight_batch_search.DEFAULT_WEIGHT_EXPERIMENT_BATCH2_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="weight scorecard artifact root。"),
    ] = weight_batch_search.DEFAULT_WEIGHT_SCORECARD_DIR,
) -> None:
    result = weight_batch_search.run_weight_scorecard(
        backfill_id=backfill_id,
        backfill_dir=backfill_dir,
        matrix_dir=matrix_dir,
        output_dir=output_dir,
    )
    manifest = result["manifest"]
    typer.echo(f"scorecard_id={result['scorecard_id']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(f"top_return_candidate={manifest['top_return_candidate']}")
    typer.echo(f"top_drawdown_candidate={manifest['top_drawdown_candidate']}")
    typer.echo(f"pareto_count={len(result['pareto_frontier']['candidates'])}")


@dynamic_v3_weight_scorecard_app.command("report")
def dynamic_v3_weight_scorecard_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest scorecard。"),
    ] = False,
    scorecard_id: Annotated[
        str | None,
        typer.Option("--scorecard-id", help="scorecard id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="weight scorecard artifact root。"),
    ] = weight_batch_search.DEFAULT_WEIGHT_SCORECARD_DIR,
) -> None:
    payload = weight_batch_search.weight_scorecard_report_payload(
        scorecard_id=scorecard_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"scorecard_id={payload['scorecard_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"top_return_candidate={payload['top_return_candidate']}")
    typer.echo(f"report_path={payload['weight_scorecard_report_path']}")


@dynamic_v3_rescue_app.command("validate-weight-scorecard")
def dynamic_v3_validate_weight_scorecard_command(
    scorecard_id: Annotated[str, typer.Option("--scorecard-id", help="scorecard id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="weight scorecard artifact root。"),
    ] = weight_batch_search.DEFAULT_WEIGHT_SCORECARD_DIR,
) -> None:
    payload = weight_batch_search.validate_weight_scorecard_artifact(
        scorecard_id=scorecard_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_weight_robustness_review_app.command("run")
def dynamic_v3_weight_robustness_review_run_command(
    scorecard_id: Annotated[str, typer.Option("--scorecard-id", help="scorecard id。")],
    scorecard_dir: Annotated[
        Path,
        typer.Option("--scorecard-dir", help="weight scorecard artifact root。"),
    ] = weight_batch_search.DEFAULT_WEIGHT_SCORECARD_DIR,
    backfill_dir: Annotated[
        Path,
        typer.Option("--backfill-dir", help="batch backfill artifact root。"),
    ] = weight_batch_search.DEFAULT_WEIGHT_BATCH_BACKFILL_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="robustness review artifact root。"),
    ] = weight_batch_search.DEFAULT_WEIGHT_ROBUSTNESS_REVIEW_DIR,
) -> None:
    result = weight_batch_search.run_weight_robustness_review(
        scorecard_id=scorecard_id,
        scorecard_dir=scorecard_dir,
        backfill_dir=backfill_dir,
        output_dir=output_dir,
    )
    summary = result["robustness_summary"]
    typer.echo(f"robustness_id={result['robustness_id']}")
    typer.echo(f"status={result['manifest']['status']}")
    typer.echo(f"robust_candidates={','.join(summary['robust_candidates'])}")


@dynamic_v3_weight_robustness_review_app.command("report")
def dynamic_v3_weight_robustness_review_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest robustness review。"),
    ] = False,
    robustness_id: Annotated[
        str | None,
        typer.Option("--robustness-id", help="robustness id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="robustness review artifact root。"),
    ] = weight_batch_search.DEFAULT_WEIGHT_ROBUSTNESS_REVIEW_DIR,
) -> None:
    payload = weight_batch_search.weight_robustness_review_report_payload(
        robustness_id=robustness_id,
        latest=latest,
        output_dir=output_dir,
    )
    summary = _mapping_obj(payload.get("robustness_summary"))
    typer.echo(f"robustness_id={payload['robustness_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"robust_candidates={','.join(summary.get('robust_candidates', []))}")
    typer.echo(f"report_path={payload['weight_robustness_review_report_path']}")


@dynamic_v3_rescue_app.command("validate-weight-robustness-review")
def dynamic_v3_validate_weight_robustness_review_command(
    robustness_id: Annotated[
        str,
        typer.Option("--robustness-id", help="robustness id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="robustness review artifact root。"),
    ] = weight_batch_search.DEFAULT_WEIGHT_ROBUSTNESS_REVIEW_DIR,
) -> None:
    payload = weight_batch_search.validate_weight_robustness_review_artifact(
        robustness_id=robustness_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_weight_adaptive_branch_app.command("run")
def dynamic_v3_weight_adaptive_branch_run_command(
    scorecard_id: Annotated[str, typer.Option("--scorecard-id", help="scorecard id。")],
    robustness_id: Annotated[
        str,
        typer.Option("--robustness-id", help="robustness id。"),
    ],
    scorecard_dir: Annotated[
        Path,
        typer.Option("--scorecard-dir", help="weight scorecard artifact root。"),
    ] = weight_batch_search.DEFAULT_WEIGHT_SCORECARD_DIR,
    robustness_dir: Annotated[
        Path,
        typer.Option("--robustness-dir", help="robustness review artifact root。"),
    ] = weight_batch_search.DEFAULT_WEIGHT_ROBUSTNESS_REVIEW_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="adaptive branch artifact root。"),
    ] = weight_batch_search.DEFAULT_WEIGHT_ADAPTIVE_BRANCH_DIR,
) -> None:
    result = weight_batch_search.run_weight_adaptive_branch(
        scorecard_id=scorecard_id,
        robustness_id=robustness_id,
        scorecard_dir=scorecard_dir,
        robustness_dir=robustness_dir,
        output_dir=output_dir,
    )
    decision = result["branch_decision"]
    typer.echo(f"branch_id={result['branch_id']}")
    typer.echo(f"branch_decision={decision['branch_decision']}")
    typer.echo(f"next_command={decision['next_command']}")


@dynamic_v3_weight_adaptive_branch_app.command("report")
def dynamic_v3_weight_adaptive_branch_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest adaptive branch。"),
    ] = False,
    branch_id: Annotated[str | None, typer.Option("--branch-id", help="branch id。")] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="adaptive branch artifact root。"),
    ] = weight_batch_search.DEFAULT_WEIGHT_ADAPTIVE_BRANCH_DIR,
) -> None:
    payload = weight_batch_search.weight_adaptive_branch_report_payload(
        branch_id=branch_id,
        latest=latest,
        output_dir=output_dir,
    )
    decision = _mapping_obj(payload.get("branch_decision_payload"))
    typer.echo(f"branch_id={payload['branch_id']}")
    typer.echo(f"branch_decision={decision.get('branch_decision')}")
    typer.echo(f"report_path={payload['weight_adaptive_branch_report_path']}")


@dynamic_v3_rescue_app.command("validate-weight-adaptive-branch")
def dynamic_v3_validate_weight_adaptive_branch_command(
    branch_id: Annotated[str, typer.Option("--branch-id", help="branch id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="adaptive branch artifact root。"),
    ] = weight_batch_search.DEFAULT_WEIGHT_ADAPTIVE_BRANCH_DIR,
) -> None:
    payload = weight_batch_search.validate_weight_adaptive_branch_artifact(
        branch_id=branch_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_weight_expanded_search_app.command("build")
def dynamic_v3_weight_expanded_search_build_command(
    branch_id: Annotated[str, typer.Option("--branch-id", help="branch id。")],
    branch_dir: Annotated[
        Path,
        typer.Option("--branch-dir", help="adaptive branch artifact root。"),
    ] = weight_batch_search.DEFAULT_WEIGHT_ADAPTIVE_BRANCH_DIR,
    search_space_dir: Annotated[
        Path,
        typer.Option("--search-space-dir", help="weight search space artifact root。"),
    ] = weight_batch_search.DEFAULT_WEIGHT_SEARCH_SPACE_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="expanded matrix artifact root。"),
    ] = weight_batch_search.DEFAULT_WEIGHT_EXPANDED_SEARCH_DIR,
) -> None:
    result = weight_batch_search.build_weight_expanded_search(
        branch_id=branch_id,
        branch_dir=branch_dir,
        search_space_dir=search_space_dir,
        output_dir=output_dir,
    )
    typer.echo(f"expanded_matrix_id={result['batch2_matrix_id']}")
    typer.echo(f"variant_count={result['manifest']['variant_count']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_weight_expanded_search_app.command("run")
def dynamic_v3_weight_expanded_search_run_command(
    expanded_matrix_id: Annotated[
        str,
        typer.Option("--expanded-matrix-id", help="expanded matrix id。"),
    ],
    expanded_matrix_dir: Annotated[
        Path,
        typer.Option("--expanded-matrix-dir", help="expanded matrix artifact root。"),
    ] = weight_batch_search.DEFAULT_WEIGHT_EXPANDED_SEARCH_DIR,
    baseline_backfill_dir: Annotated[
        Path,
        typer.Option("--baseline-backfill-dir", help="paper shadow backfill root。"),
    ] = system_target.DEFAULT_PAPER_SHADOW_BACKFILL_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="weight batch backfill artifact root。"),
    ] = weight_batch_search.DEFAULT_WEIGHT_BATCH_BACKFILL_DIR,
    price_cache_path: Annotated[
        Path | None,
        typer.Option("--price-cache-path", help="price cache override。"),
    ] = None,
) -> None:
    result = weight_batch_search.run_weight_expanded_search(
        expanded_matrix_id=expanded_matrix_id,
        expanded_matrix_dir=expanded_matrix_dir,
        baseline_backfill_dir=baseline_backfill_dir,
        output_dir=output_dir,
        price_cache_path=price_cache_path,
    )
    typer.echo(f"batch_backfill_id={result['batch_backfill_id']}")
    typer.echo(f"status={result['manifest']['status']}")
