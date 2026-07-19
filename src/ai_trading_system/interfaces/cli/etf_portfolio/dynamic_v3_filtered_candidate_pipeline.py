from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio import (
    dynamic_v3_filtered_candidate_pipeline as filtered_candidate_pipeline,
)
from ai_trading_system.interfaces.cli.etf_portfolio.common import (
    mapping_obj as _mapping_obj,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_filtered_candidate_backfill_app,
    dynamic_v3_filtered_candidate_promotion_review_app,
    dynamic_v3_filtered_vs_original_comparison_app,
    dynamic_v3_owner_signal_roadmap_app,
    dynamic_v3_rescue_app,
    dynamic_v3_signal_gate_experiment_app,
)


@dynamic_v3_filtered_candidate_backfill_app.command("run")
def dynamic_v3_filtered_candidate_backfill_run_command(
    filter_design_id: Annotated[
        str,
        typer.Option("--filter-design-id", help="filter design id。"),
    ],
    filter_design_dir: Annotated[
        Path,
        typer.Option("--filter-design-dir", help="candidate quality filter design root。"),
    ] = filtered_candidate_pipeline.DEFAULT_CANDIDATE_QUALITY_FILTER_DESIGN_DIR,
    ledger_dir: Annotated[
        Path,
        typer.Option("--ledger-dir", help="candidate signal ledger artifact root。"),
    ] = filtered_candidate_pipeline.DEFAULT_CANDIDATE_SIGNAL_LEDGER_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="filtered candidate backfill artifact root。"),
    ] = filtered_candidate_pipeline.DEFAULT_FILTERED_CANDIDATE_BACKFILL_DIR,
) -> None:
    result = filtered_candidate_pipeline.run_filtered_candidate_backfill(
        filter_design_id=filter_design_id,
        filter_design_dir=filter_design_dir,
        ledger_dir=ledger_dir,
        output_dir=output_dir,
    )
    typer.echo(f"filtered_backfill_id={result['filtered_backfill_id']}")
    typer.echo(f"status={result['manifest']['status']}")
    typer.echo(f"data_quality_status={result['manifest']['data_quality_status']}")
    typer.echo(f"variant_count={len(result['filtered_variant_specs'])}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_filtered_candidate_backfill_app.command("report")
def dynamic_v3_filtered_candidate_backfill_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest。"),
    ] = False,
    filtered_backfill_id: Annotated[
        str | None,
        typer.Option("--filtered-backfill-id", help="filtered backfill id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="filtered candidate backfill artifact root。"),
    ] = filtered_candidate_pipeline.DEFAULT_FILTERED_CANDIDATE_BACKFILL_DIR,
) -> None:
    payload = filtered_candidate_pipeline.filtered_candidate_backfill_report_payload(
        filtered_backfill_id=filtered_backfill_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"filtered_backfill_id={payload['filtered_backfill_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"data_quality_status={payload['data_quality_status']}")
    typer.echo(f"report_path={payload['filtered_candidate_backfill_report_path']}")


@dynamic_v3_rescue_app.command("validate-filtered-candidate-backfill")
def dynamic_v3_validate_filtered_candidate_backfill_command(
    filtered_backfill_id: Annotated[
        str,
        typer.Option("--filtered-backfill-id", help="filtered backfill id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="filtered candidate backfill artifact root。"),
    ] = filtered_candidate_pipeline.DEFAULT_FILTERED_CANDIDATE_BACKFILL_DIR,
) -> None:
    payload = filtered_candidate_pipeline.validate_filtered_candidate_backfill_artifact(
        filtered_backfill_id=filtered_backfill_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_filtered_vs_original_comparison_app.command("run")
def dynamic_v3_filtered_vs_original_comparison_run_command(
    filtered_backfill_id: Annotated[
        str,
        typer.Option("--filtered-backfill-id", help="filtered backfill id。"),
    ],
    filtered_backfill_dir: Annotated[
        Path,
        typer.Option("--filtered-backfill-dir", help="filtered candidate backfill root。"),
    ] = filtered_candidate_pipeline.DEFAULT_FILTERED_CANDIDATE_BACKFILL_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="filtered-vs-original comparison artifact root。"),
    ] = filtered_candidate_pipeline.DEFAULT_FILTERED_VS_ORIGINAL_COMPARISON_DIR,
) -> None:
    result = filtered_candidate_pipeline.run_filtered_vs_original_comparison(
        filtered_backfill_id=filtered_backfill_id,
        filtered_backfill_dir=filtered_backfill_dir,
        output_dir=output_dir,
    )
    summary = _mapping_obj(result.get("filtered_improvement_summary"))
    typer.echo(f"comparison_id={result['comparison_id']}")
    typer.echo(f"status={result['manifest']['status']}")
    typer.echo(f"best_filtered_variant={summary.get('best_filtered_variant')}")
    typer.echo(f"recommendation={summary.get('recommendation')}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_filtered_vs_original_comparison_app.command("report")
def dynamic_v3_filtered_vs_original_comparison_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest。"),
    ] = False,
    comparison_id: Annotated[
        str | None,
        typer.Option("--comparison-id", help="filtered-vs-original comparison id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="filtered-vs-original comparison artifact root。"),
    ] = filtered_candidate_pipeline.DEFAULT_FILTERED_VS_ORIGINAL_COMPARISON_DIR,
) -> None:
    payload = filtered_candidate_pipeline.filtered_vs_original_comparison_report_payload(
        comparison_id=comparison_id,
        latest=latest,
        output_dir=output_dir,
    )
    summary = _mapping_obj(payload.get("filtered_improvement_summary"))
    typer.echo(f"comparison_id={payload['comparison_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"best_filtered_variant={summary.get('best_filtered_variant')}")
    typer.echo(f"report_path={payload['filtered_vs_original_comparison_report_path']}")


@dynamic_v3_rescue_app.command("validate-filtered-vs-original-comparison")
def dynamic_v3_validate_filtered_vs_original_comparison_command(
    comparison_id: Annotated[
        str,
        typer.Option("--comparison-id", help="comparison id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="filtered-vs-original comparison artifact root。"),
    ] = filtered_candidate_pipeline.DEFAULT_FILTERED_VS_ORIGINAL_COMPARISON_DIR,
) -> None:
    payload = filtered_candidate_pipeline.validate_filtered_vs_original_comparison_artifact(
        comparison_id=comparison_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_signal_gate_experiment_app.command("run")
def dynamic_v3_signal_gate_experiment_run_command(
    filter_design_id: Annotated[
        str,
        typer.Option("--filter-design-id", help="filter design id。"),
    ],
    filter_design_dir: Annotated[
        Path,
        typer.Option("--filter-design-dir", help="candidate quality filter design root。"),
    ] = filtered_candidate_pipeline.DEFAULT_CANDIDATE_QUALITY_FILTER_DESIGN_DIR,
    ledger_dir: Annotated[
        Path,
        typer.Option("--ledger-dir", help="candidate signal ledger artifact root。"),
    ] = filtered_candidate_pipeline.DEFAULT_CANDIDATE_SIGNAL_LEDGER_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="signal gate experiment artifact root。"),
    ] = filtered_candidate_pipeline.DEFAULT_SIGNAL_GATE_EXPERIMENT_DIR,
) -> None:
    result = filtered_candidate_pipeline.run_signal_gate_experiment(
        filter_design_id=filter_design_id,
        filter_design_dir=filter_design_dir,
        ledger_dir=ledger_dir,
        output_dir=output_dir,
    )
    summary = _mapping_obj(result.get("signal_gate_experiment_summary"))
    typer.echo(f"signal_gate_experiment_id={result['signal_gate_experiment_id']}")
    typer.echo(f"status={result['manifest']['status']}")
    typer.echo(f"recommended_next_action={summary.get('recommended_next_action')}")
    typer.echo(f"formalization_ready={summary.get('formalization_ready')}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_signal_gate_experiment_app.command("report")
def dynamic_v3_signal_gate_experiment_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest。"),
    ] = False,
    signal_gate_experiment_id: Annotated[
        str | None,
        typer.Option("--signal-gate-experiment-id", help="signal gate experiment id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="signal gate experiment artifact root。"),
    ] = filtered_candidate_pipeline.DEFAULT_SIGNAL_GATE_EXPERIMENT_DIR,
) -> None:
    payload = filtered_candidate_pipeline.signal_gate_experiment_report_payload(
        signal_gate_experiment_id=signal_gate_experiment_id,
        latest=latest,
        output_dir=output_dir,
    )
    summary = _mapping_obj(payload.get("signal_gate_experiment_summary"))
    typer.echo(f"signal_gate_experiment_id={payload['signal_gate_experiment_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"recommended_next_action={summary.get('recommended_next_action')}")
    typer.echo(f"report_path={payload['signal_gate_experiment_report_path']}")


@dynamic_v3_rescue_app.command("validate-signal-gate-experiment")
def dynamic_v3_validate_signal_gate_experiment_command(
    signal_gate_experiment_id: Annotated[
        str,
        typer.Option("--signal-gate-experiment-id", help="signal gate experiment id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="signal gate experiment artifact root。"),
    ] = filtered_candidate_pipeline.DEFAULT_SIGNAL_GATE_EXPERIMENT_DIR,
) -> None:
    payload = filtered_candidate_pipeline.validate_signal_gate_experiment_artifact(
        signal_gate_experiment_id=signal_gate_experiment_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_filtered_candidate_promotion_review_app.command("run")
def dynamic_v3_filtered_candidate_promotion_review_run_command(
    comparison_id: Annotated[
        str,
        typer.Option("--comparison-id", help="comparison id。"),
    ],
    signal_gate_experiment_id: Annotated[
        str,
        typer.Option("--signal-gate-experiment-id", help="signal gate experiment id。"),
    ],
    comparison_dir: Annotated[
        Path,
        typer.Option("--comparison-dir", help="filtered-vs-original comparison root。"),
    ] = filtered_candidate_pipeline.DEFAULT_FILTERED_VS_ORIGINAL_COMPARISON_DIR,
    experiment_dir: Annotated[
        Path,
        typer.Option("--experiment-dir", help="signal gate experiment root。"),
    ] = filtered_candidate_pipeline.DEFAULT_SIGNAL_GATE_EXPERIMENT_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="filtered candidate promotion review root。"),
    ] = filtered_candidate_pipeline.DEFAULT_FILTERED_CANDIDATE_PROMOTION_REVIEW_DIR,
) -> None:
    result = filtered_candidate_pipeline.run_filtered_candidate_promotion_review(
        comparison_id=comparison_id,
        signal_gate_experiment_id=signal_gate_experiment_id,
        comparison_dir=comparison_dir,
        experiment_dir=experiment_dir,
        output_dir=output_dir,
    )
    decision = _mapping_obj(result.get("filtered_promotion_decision"))
    typer.echo(f"filtered_review_id={result['filtered_review_id']}")
    typer.echo(f"status={result['manifest']['status']}")
    typer.echo(f"decision={decision.get('decision')}")
    typer.echo(f"confidence={decision.get('confidence')}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_filtered_candidate_promotion_review_app.command("report")
def dynamic_v3_filtered_candidate_promotion_review_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest。"),
    ] = False,
    filtered_review_id: Annotated[
        str | None,
        typer.Option("--filtered-review-id", help="filtered candidate promotion review id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="filtered candidate promotion review root。"),
    ] = filtered_candidate_pipeline.DEFAULT_FILTERED_CANDIDATE_PROMOTION_REVIEW_DIR,
) -> None:
    payload = filtered_candidate_pipeline.filtered_candidate_promotion_review_report_payload(
        filtered_review_id=filtered_review_id,
        latest=latest,
        output_dir=output_dir,
    )
    decision = _mapping_obj(payload.get("filtered_promotion_decision"))
    typer.echo(f"filtered_review_id={payload['filtered_review_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"decision={decision.get('decision')}")
    typer.echo(f"report_path={payload['filtered_candidate_promotion_review_report_path']}")


@dynamic_v3_rescue_app.command("validate-filtered-candidate-promotion-review")
def dynamic_v3_validate_filtered_candidate_promotion_review_command(
    filtered_review_id: Annotated[
        str,
        typer.Option("--filtered-review-id", help="filtered review id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="filtered candidate promotion review root。"),
    ] = filtered_candidate_pipeline.DEFAULT_FILTERED_CANDIDATE_PROMOTION_REVIEW_DIR,
) -> None:
    payload = filtered_candidate_pipeline.validate_filtered_candidate_promotion_review_artifact(
        filtered_review_id=filtered_review_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_owner_signal_roadmap_app.command("build")
def dynamic_v3_owner_signal_roadmap_build_command(
    filtered_review_id: Annotated[
        str,
        typer.Option("--filtered-review-id", help="filtered review id。"),
    ],
    review_dir: Annotated[
        Path,
        typer.Option("--review-dir", help="filtered candidate promotion review root。"),
    ] = filtered_candidate_pipeline.DEFAULT_FILTERED_CANDIDATE_PROMOTION_REVIEW_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="owner signal roadmap root。"),
    ] = filtered_candidate_pipeline.DEFAULT_OWNER_SIGNAL_ROADMAP_DIR,
) -> None:
    result = filtered_candidate_pipeline.build_owner_signal_roadmap(
        filtered_review_id=filtered_review_id,
        review_dir=review_dir,
        output_dir=output_dir,
    )
    summary = _mapping_obj(result.get("owner_signal_roadmap_summary"))
    typer.echo(f"owner_signal_roadmap_id={result['owner_signal_roadmap_id']}")
    typer.echo(f"status={result['manifest']['status']}")
    typer.echo(f"recommended_owner_action={summary.get('recommended_owner_action')}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_owner_signal_roadmap_app.command("report")
def dynamic_v3_owner_signal_roadmap_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest。"),
    ] = False,
    owner_signal_roadmap_id: Annotated[
        str | None,
        typer.Option("--owner-signal-roadmap-id", help="owner signal roadmap id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="owner signal roadmap root。"),
    ] = filtered_candidate_pipeline.DEFAULT_OWNER_SIGNAL_ROADMAP_DIR,
) -> None:
    payload = filtered_candidate_pipeline.owner_signal_roadmap_report_payload(
        owner_signal_roadmap_id=owner_signal_roadmap_id,
        latest=latest,
        output_dir=output_dir,
    )
    summary = _mapping_obj(payload.get("owner_signal_roadmap_summary"))
    typer.echo(f"owner_signal_roadmap_id={payload['owner_signal_roadmap_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"recommended_owner_action={summary.get('recommended_owner_action')}")
    typer.echo(f"report_path={payload['owner_signal_roadmap_report_path']}")


@dynamic_v3_rescue_app.command("validate-owner-signal-roadmap")
def dynamic_v3_validate_owner_signal_roadmap_command(
    owner_signal_roadmap_id: Annotated[
        str,
        typer.Option("--owner-signal-roadmap-id", help="owner signal roadmap id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="owner signal roadmap root。"),
    ] = filtered_candidate_pipeline.DEFAULT_OWNER_SIGNAL_ROADMAP_DIR,
) -> None:
    payload = filtered_candidate_pipeline.validate_owner_signal_roadmap_artifact(
        owner_signal_roadmap_id=owner_signal_roadmap_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)
