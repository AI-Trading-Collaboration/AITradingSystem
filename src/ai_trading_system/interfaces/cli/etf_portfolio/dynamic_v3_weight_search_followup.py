from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Annotated, Any

import typer

from ai_trading_system.etf_portfolio import dynamic_v3_weight_search_followup as followup
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_candidate_promotion_v2_app,
    dynamic_v3_next_formal_or_search_plan_app,
    dynamic_v3_promotion_threshold_sensitivity_app,
    dynamic_v3_rescue_app,
)


def _mapping_obj(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


@dynamic_v3_promotion_threshold_sensitivity_app.command("run")
def dynamic_v3_promotion_threshold_sensitivity_run_command(
    v3_backfill_id: Annotated[
        str,
        typer.Option("--v3-backfill-id", help="targeted v3 backfill id。"),
    ],
    ab_id: Annotated[str, typer.Option("--ab-id", help="A/B id。")],
    v3_backfill_dir: Annotated[
        Path,
        typer.Option("--v3-backfill-dir", help="targeted v3 backfill root。"),
    ] = followup.DEFAULT_TARGETED_V3_BACKFILL_DIR,
    v3_matrix_dir: Annotated[
        Path,
        typer.Option("--v3-matrix-dir", help="targeted search v3 matrix root。"),
    ] = followup.DEFAULT_TARGETED_SEARCH_V3_DIR,
    ab_dir: Annotated[
        Path,
        typer.Option("--ab-dir", help="A/B comparison root。"),
    ] = followup.DEFAULT_NEAR_MISS_AB_COMPARISON_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="threshold sensitivity root。"),
    ] = followup.DEFAULT_PROMOTION_THRESHOLD_SENSITIVITY_DIR,
) -> None:
    result = followup.run_promotion_threshold_sensitivity(
        v3_backfill_id=v3_backfill_id,
        ab_id=ab_id,
        v3_backfill_dir=v3_backfill_dir,
        v3_matrix_dir=v3_matrix_dir,
        ab_dir=ab_dir,
        output_dir=output_dir,
    )
    typer.echo(f"sensitivity_id={result['sensitivity_id']}")
    typer.echo(f"status={result['manifest']['status']}")
    typer.echo(f"scenario_count={len(result['threshold_scenarios'])}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_promotion_threshold_sensitivity_app.command("report")
def dynamic_v3_promotion_threshold_sensitivity_report_command(
    latest: Annotated[
        bool, typer.Option("--latest/--no-latest", help="读取 latest sensitivity。")
    ] = False,
    sensitivity_id: Annotated[
        str | None,
        typer.Option("--sensitivity-id", help="threshold sensitivity id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="threshold sensitivity root。"),
    ] = followup.DEFAULT_PROMOTION_THRESHOLD_SENSITIVITY_DIR,
) -> None:
    payload = followup.promotion_threshold_sensitivity_report_payload(
        sensitivity_id=sensitivity_id,
        latest=latest,
        output_dir=output_dir,
    )
    impact = _mapping_obj(payload.get("threshold_candidate_impact"))
    typer.echo(f"sensitivity_id={payload['sensitivity_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"relaxed_only_count={impact.get('relaxed_only_count')}")
    typer.echo(f"report_path={payload['threshold_sensitivity_report_path']}")


@dynamic_v3_rescue_app.command("validate-promotion-threshold-sensitivity")
def dynamic_v3_validate_promotion_threshold_sensitivity_command(
    sensitivity_id: Annotated[
        str,
        typer.Option("--sensitivity-id", help="threshold sensitivity id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="threshold sensitivity root。"),
    ] = followup.DEFAULT_PROMOTION_THRESHOLD_SENSITIVITY_DIR,
) -> None:
    payload = followup.validate_promotion_threshold_sensitivity_artifact(
        sensitivity_id=sensitivity_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_candidate_promotion_v2_app.command("run")
def dynamic_v3_candidate_promotion_v2_run_command(
    v3_backfill_id: Annotated[
        str,
        typer.Option("--v3-backfill-id", help="targeted v3 backfill id。"),
    ],
    ab_id: Annotated[str, typer.Option("--ab-id", help="A/B id。")],
    sensitivity_id: Annotated[
        str,
        typer.Option("--sensitivity-id", help="threshold sensitivity id。"),
    ],
    v3_backfill_dir: Annotated[
        Path,
        typer.Option("--v3-backfill-dir", help="targeted v3 backfill root。"),
    ] = followup.DEFAULT_TARGETED_V3_BACKFILL_DIR,
    v3_matrix_dir: Annotated[
        Path,
        typer.Option("--v3-matrix-dir", help="targeted search v3 matrix root。"),
    ] = followup.DEFAULT_TARGETED_SEARCH_V3_DIR,
    ab_dir: Annotated[
        Path,
        typer.Option("--ab-dir", help="A/B comparison root。"),
    ] = followup.DEFAULT_NEAR_MISS_AB_COMPARISON_DIR,
    sensitivity_dir: Annotated[
        Path,
        typer.Option("--sensitivity-dir", help="threshold sensitivity root。"),
    ] = followup.DEFAULT_PROMOTION_THRESHOLD_SENSITIVITY_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="candidate promotion v2 root。"),
    ] = followup.DEFAULT_CANDIDATE_PROMOTION_V2_DIR,
) -> None:
    result = followup.run_candidate_promotion_v2(
        v3_backfill_id=v3_backfill_id,
        ab_id=ab_id,
        sensitivity_id=sensitivity_id,
        v3_backfill_dir=v3_backfill_dir,
        v3_matrix_dir=v3_matrix_dir,
        ab_dir=ab_dir,
        sensitivity_dir=sensitivity_dir,
        output_dir=output_dir,
    )
    decision = _mapping_obj(result.get("promotion_v2_decision"))
    typer.echo(f"promotion_v2_id={result['promotion_v2_id']}")
    typer.echo(f"status={result['manifest']['status']}")
    typer.echo(f"decision={decision.get('decision')}")
    typer.echo(f"promoted_count={decision.get('promoted_count')}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_candidate_promotion_v2_app.command("report")
def dynamic_v3_candidate_promotion_v2_report_command(
    latest: Annotated[
        bool, typer.Option("--latest/--no-latest", help="读取 latest promotion v2。")
    ] = False,
    promotion_v2_id: Annotated[
        str | None,
        typer.Option("--promotion-v2-id", help="candidate promotion v2 id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="candidate promotion v2 root。"),
    ] = followup.DEFAULT_CANDIDATE_PROMOTION_V2_DIR,
) -> None:
    payload = followup.candidate_promotion_v2_report_payload(
        promotion_v2_id=promotion_v2_id,
        latest=latest,
        output_dir=output_dir,
    )
    decision = _mapping_obj(payload.get("promotion_v2_decision"))
    typer.echo(f"promotion_v2_id={payload['promotion_v2_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"decision={decision.get('decision')}")
    typer.echo(f"report_path={payload['candidate_promotion_v2_report_path']}")


@dynamic_v3_rescue_app.command("validate-candidate-promotion-v2")
def dynamic_v3_validate_candidate_promotion_v2_command(
    promotion_v2_id: Annotated[
        str,
        typer.Option("--promotion-v2-id", help="candidate promotion v2 id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="candidate promotion v2 root。"),
    ] = followup.DEFAULT_CANDIDATE_PROMOTION_V2_DIR,
) -> None:
    payload = followup.validate_candidate_promotion_v2_artifact(
        promotion_v2_id=promotion_v2_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_next_formal_or_search_plan_app.command("run")
def dynamic_v3_next_formal_or_search_plan_run_command(
    promotion_v2_id: Annotated[
        str,
        typer.Option("--promotion-v2-id", help="candidate promotion v2 id。"),
    ],
    promotion_v2_dir: Annotated[
        Path,
        typer.Option("--promotion-v2-dir", help="candidate promotion v2 root。"),
    ] = followup.DEFAULT_CANDIDATE_PROMOTION_V2_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="next formal/search plan root。"),
    ] = followup.DEFAULT_NEXT_FORMAL_OR_SEARCH_PLAN_DIR,
) -> None:
    result = followup.run_next_formal_or_search_plan(
        promotion_v2_id=promotion_v2_id,
        promotion_v2_dir=promotion_v2_dir,
        output_dir=output_dir,
    )
    decision = _mapping_obj(result.get("next_plan_decision"))
    typer.echo(f"plan_id={result['plan_id']}")
    typer.echo(f"status={result['manifest']['status']}")
    typer.echo(f"decision={decision.get('decision')}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_next_formal_or_search_plan_app.command("report")
def dynamic_v3_next_formal_or_search_plan_report_command(
    latest: Annotated[
        bool, typer.Option("--latest/--no-latest", help="读取 latest next plan。")
    ] = False,
    plan_id: Annotated[str | None, typer.Option("--plan-id", help="next plan id。")] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="next formal/search plan root。"),
    ] = followup.DEFAULT_NEXT_FORMAL_OR_SEARCH_PLAN_DIR,
) -> None:
    payload = followup.next_formal_or_search_plan_report_payload(
        plan_id=plan_id,
        latest=latest,
        output_dir=output_dir,
    )
    decision = _mapping_obj(payload.get("next_plan_decision"))
    typer.echo(f"plan_id={payload['plan_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"decision={decision.get('decision')}")
    typer.echo(f"report_path={payload['next_formal_or_search_plan_report_path']}")


@dynamic_v3_rescue_app.command("validate-next-formal-or-search-plan")
def dynamic_v3_validate_next_formal_or_search_plan_command(
    plan_id: Annotated[str, typer.Option("--plan-id", help="next plan id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="next formal/search plan root。"),
    ] = followup.DEFAULT_NEXT_FORMAL_OR_SEARCH_PLAN_DIR,
) -> None:
    payload = followup.validate_next_formal_or_search_plan_artifact(
        plan_id=plan_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)
