from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio import (
    dynamic_v3_weight_search_decision as weight_batch_search,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_formal_method_auto_plan_app,
    dynamic_v3_owner_research_decision_pack_app,
    dynamic_v3_rescue_app,
    dynamic_v3_weight_candidate_cluster_app,
    dynamic_v3_weight_method_promotion_gate_app,
    dynamic_v3_weight_search_dashboard_app,
    dynamic_v3_weight_top_candidate_interpretation_app,
)


@dynamic_v3_weight_candidate_cluster_app.command("run")
def dynamic_v3_weight_candidate_cluster_run_command(
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
        typer.Option("--output-dir", help="candidate cluster artifact root。"),
    ] = weight_batch_search.DEFAULT_WEIGHT_CANDIDATE_CLUSTER_DIR,
) -> None:
    result = weight_batch_search.run_weight_candidate_cluster(
        scorecard_id=scorecard_id,
        robustness_id=robustness_id,
        scorecard_dir=scorecard_dir,
        robustness_dir=robustness_dir,
        output_dir=output_dir,
    )
    typer.echo(f"cluster_id={result['cluster_id']}")
    typer.echo(f"cluster_count={result['manifest']['cluster_count']}")


@dynamic_v3_weight_candidate_cluster_app.command("report")
def dynamic_v3_weight_candidate_cluster_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest candidate cluster。"),
    ] = False,
    cluster_id: Annotated[str | None, typer.Option("--cluster-id", help="cluster id。")] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="candidate cluster artifact root。"),
    ] = weight_batch_search.DEFAULT_WEIGHT_CANDIDATE_CLUSTER_DIR,
) -> None:
    payload = weight_batch_search.weight_candidate_cluster_report_payload(
        cluster_id=cluster_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"cluster_id={payload['cluster_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"report_path={payload['candidate_cluster_report_path']}")


@dynamic_v3_rescue_app.command("validate-weight-candidate-cluster")
def dynamic_v3_validate_weight_candidate_cluster_command(
    cluster_id: Annotated[str, typer.Option("--cluster-id", help="cluster id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="candidate cluster artifact root。"),
    ] = weight_batch_search.DEFAULT_WEIGHT_CANDIDATE_CLUSTER_DIR,
) -> None:
    payload = weight_batch_search.validate_weight_candidate_cluster_artifact(
        cluster_id=cluster_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_weight_top_candidate_interpretation_app.command("run")
def dynamic_v3_weight_top_candidate_interpretation_run_command(
    cluster_id: Annotated[str, typer.Option("--cluster-id", help="cluster id。")],
    cluster_dir: Annotated[
        Path,
        typer.Option("--cluster-dir", help="candidate cluster artifact root。"),
    ] = weight_batch_search.DEFAULT_WEIGHT_CANDIDATE_CLUSTER_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="top candidate interpretation root。"),
    ] = weight_batch_search.DEFAULT_WEIGHT_TOP_CANDIDATE_INTERPRETATION_DIR,
) -> None:
    result = weight_batch_search.run_weight_top_candidate_interpretation(
        cluster_id=cluster_id,
        cluster_dir=cluster_dir,
        output_dir=output_dir,
    )
    typer.echo(f"interpretation_id={result['interpretation_id']}")
    typer.echo(f"recommended_variant={result['manifest']['recommended_variant']}")


@dynamic_v3_weight_top_candidate_interpretation_app.command("report")
def dynamic_v3_weight_top_candidate_interpretation_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest interpretation。"),
    ] = False,
    interpretation_id: Annotated[
        str | None,
        typer.Option("--interpretation-id", help="interpretation id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="top candidate interpretation root。"),
    ] = weight_batch_search.DEFAULT_WEIGHT_TOP_CANDIDATE_INTERPRETATION_DIR,
) -> None:
    payload = weight_batch_search.weight_top_candidate_interpretation_report_payload(
        interpretation_id=interpretation_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"interpretation_id={payload['interpretation_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"recommended_variant={payload['recommended_variant']}")


@dynamic_v3_rescue_app.command("validate-weight-top-candidate-interpretation")
def dynamic_v3_validate_weight_top_candidate_interpretation_command(
    interpretation_id: Annotated[
        str,
        typer.Option("--interpretation-id", help="interpretation id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="top candidate interpretation root。"),
    ] = weight_batch_search.DEFAULT_WEIGHT_TOP_CANDIDATE_INTERPRETATION_DIR,
) -> None:
    payload = weight_batch_search.validate_weight_top_candidate_interpretation_artifact(
        interpretation_id=interpretation_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_weight_method_promotion_gate_app.command("run")
def dynamic_v3_weight_method_promotion_gate_run_command(
    interpretation_id: Annotated[
        str,
        typer.Option("--interpretation-id", help="interpretation id。"),
    ],
    interpretation_dir: Annotated[
        Path,
        typer.Option("--interpretation-dir", help="top candidate interpretation root。"),
    ] = weight_batch_search.DEFAULT_WEIGHT_TOP_CANDIDATE_INTERPRETATION_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="promotion gate artifact root。"),
    ] = weight_batch_search.DEFAULT_WEIGHT_METHOD_PROMOTION_GATE_DIR,
) -> None:
    result = weight_batch_search.run_weight_method_promotion_gate(
        interpretation_id=interpretation_id,
        interpretation_dir=interpretation_dir,
        output_dir=output_dir,
    )
    typer.echo(f"promotion_gate_id={result['promotion_gate_id']}")
    typer.echo(f"promoted_candidate_count={result['manifest']['promoted_candidate_count']}")


@dynamic_v3_weight_method_promotion_gate_app.command("report")
def dynamic_v3_weight_method_promotion_gate_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest promotion gate。"),
    ] = False,
    promotion_gate_id: Annotated[
        str | None,
        typer.Option("--promotion-gate-id", help="promotion gate id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="promotion gate artifact root。"),
    ] = weight_batch_search.DEFAULT_WEIGHT_METHOD_PROMOTION_GATE_DIR,
) -> None:
    payload = weight_batch_search.weight_method_promotion_gate_report_payload(
        promotion_gate_id=promotion_gate_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"promotion_gate_id={payload['promotion_gate_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"promoted_candidate_count={payload['promoted_candidate_count']}")


@dynamic_v3_rescue_app.command("validate-weight-method-promotion-gate")
def dynamic_v3_validate_weight_method_promotion_gate_command(
    promotion_gate_id: Annotated[
        str,
        typer.Option("--promotion-gate-id", help="promotion gate id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="promotion gate artifact root。"),
    ] = weight_batch_search.DEFAULT_WEIGHT_METHOD_PROMOTION_GATE_DIR,
) -> None:
    payload = weight_batch_search.validate_weight_method_promotion_gate_artifact(
        promotion_gate_id=promotion_gate_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_formal_method_auto_plan_app.command("run")
def dynamic_v3_formal_method_auto_plan_run_command(
    promotion_gate_id: Annotated[
        str,
        typer.Option("--promotion-gate-id", help="promotion gate id。"),
    ],
    promotion_gate_dir: Annotated[
        Path,
        typer.Option("--promotion-gate-dir", help="promotion gate artifact root。"),
    ] = weight_batch_search.DEFAULT_WEIGHT_METHOD_PROMOTION_GATE_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="formal method auto-plan root。"),
    ] = weight_batch_search.DEFAULT_FORMAL_METHOD_AUTO_PLAN_DIR,
) -> None:
    result = weight_batch_search.run_formal_method_auto_plan(
        promotion_gate_id=promotion_gate_id,
        promotion_gate_dir=promotion_gate_dir,
        output_dir=output_dir,
    )
    typer.echo(f"plan_id={result['plan_id']}")
    typer.echo(f"status={result['manifest']['status']}")
    typer.echo(f"implemented={result['manifest']['implemented']}")


@dynamic_v3_formal_method_auto_plan_app.command("report")
def dynamic_v3_formal_method_auto_plan_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest formal method plan。"),
    ] = False,
    plan_id: Annotated[str | None, typer.Option("--plan-id", help="plan id。")] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="formal method auto-plan root。"),
    ] = weight_batch_search.DEFAULT_FORMAL_METHOD_AUTO_PLAN_DIR,
) -> None:
    payload = weight_batch_search.formal_method_auto_plan_report_payload(
        plan_id=plan_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"plan_id={payload['plan_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"implemented={payload['implemented']}")


@dynamic_v3_rescue_app.command("validate-formal-method-auto-plan")
def dynamic_v3_validate_formal_method_auto_plan_command(
    plan_id: Annotated[str, typer.Option("--plan-id", help="plan id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="formal method auto-plan root。"),
    ] = weight_batch_search.DEFAULT_FORMAL_METHOD_AUTO_PLAN_DIR,
) -> None:
    payload = weight_batch_search.validate_formal_method_auto_plan_artifact(
        plan_id=plan_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_weight_search_dashboard_app.command("build")
def dynamic_v3_weight_search_dashboard_build_command(
    scorecard_id: Annotated[str, typer.Option("--scorecard-id", help="scorecard id。")],
    branch_id: Annotated[str, typer.Option("--branch-id", help="branch id。")],
    promotion_gate_id: Annotated[
        str | None,
        typer.Option("--promotion-gate-id", help="promotion gate id。"),
    ] = None,
    scorecard_dir: Annotated[
        Path,
        typer.Option("--scorecard-dir", help="weight scorecard artifact root。"),
    ] = weight_batch_search.DEFAULT_WEIGHT_SCORECARD_DIR,
    branch_dir: Annotated[
        Path,
        typer.Option("--branch-dir", help="adaptive branch artifact root。"),
    ] = weight_batch_search.DEFAULT_WEIGHT_ADAPTIVE_BRANCH_DIR,
    promotion_gate_dir: Annotated[
        Path,
        typer.Option("--promotion-gate-dir", help="promotion gate artifact root。"),
    ] = weight_batch_search.DEFAULT_WEIGHT_METHOD_PROMOTION_GATE_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="weight search dashboard root。"),
    ] = weight_batch_search.DEFAULT_WEIGHT_SEARCH_DASHBOARD_DIR,
) -> None:
    result = weight_batch_search.build_weight_search_dashboard(
        scorecard_id=scorecard_id,
        branch_id=branch_id,
        promotion_gate_id=promotion_gate_id,
        scorecard_dir=scorecard_dir,
        branch_dir=branch_dir,
        promotion_gate_dir=promotion_gate_dir,
        output_dir=output_dir,
    )
    typer.echo(f"dashboard_id={result['dashboard_id']}")
    typer.echo(f"variants_total={result['search_summary']['variants_total']}")
    typer.echo(f"branch_decision={result['next_actions']['branch_decision']}")


@dynamic_v3_weight_search_dashboard_app.command("report")
def dynamic_v3_weight_search_dashboard_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest dashboard。"),
    ] = False,
    dashboard_id: Annotated[
        str | None,
        typer.Option("--dashboard-id", help="dashboard id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="weight search dashboard root。"),
    ] = weight_batch_search.DEFAULT_WEIGHT_SEARCH_DASHBOARD_DIR,
) -> None:
    payload = weight_batch_search.weight_search_dashboard_report_payload(
        dashboard_id=dashboard_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"dashboard_id={payload['dashboard_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"top_candidate={payload['top_candidates']['top_overall_candidate']}")


@dynamic_v3_rescue_app.command("validate-weight-search-dashboard")
def dynamic_v3_validate_weight_search_dashboard_command(
    dashboard_id: Annotated[
        str,
        typer.Option("--dashboard-id", help="dashboard id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="weight search dashboard root。"),
    ] = weight_batch_search.DEFAULT_WEIGHT_SEARCH_DASHBOARD_DIR,
) -> None:
    payload = weight_batch_search.validate_weight_search_dashboard_artifact(
        dashboard_id=dashboard_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_owner_research_decision_pack_app.command("build")
def dynamic_v3_owner_research_decision_pack_build_command(
    dashboard_id: Annotated[
        str,
        typer.Option("--dashboard-id", help="dashboard id。"),
    ],
    dashboard_dir: Annotated[
        Path,
        typer.Option("--dashboard-dir", help="weight search dashboard root。"),
    ] = weight_batch_search.DEFAULT_WEIGHT_SEARCH_DASHBOARD_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="owner decision pack root。"),
    ] = weight_batch_search.DEFAULT_OWNER_RESEARCH_DECISION_PACK_DIR,
) -> None:
    result = weight_batch_search.build_owner_research_decision_pack(
        dashboard_id=dashboard_id,
        dashboard_dir=dashboard_dir,
        output_dir=output_dir,
    )
    typer.echo(f"owner_pack_id={result['owner_pack_id']}")
    typer.echo(f"recommended_decision={result['manifest']['recommended_owner_decision']}")


@dynamic_v3_owner_research_decision_pack_app.command("report")
def dynamic_v3_owner_research_decision_pack_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest owner pack。"),
    ] = False,
    owner_pack_id: Annotated[
        str | None,
        typer.Option("--owner-pack-id", help="owner decision pack id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="owner decision pack root。"),
    ] = weight_batch_search.DEFAULT_OWNER_RESEARCH_DECISION_PACK_DIR,
) -> None:
    payload = weight_batch_search.owner_research_decision_pack_report_payload(
        owner_pack_id=owner_pack_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"owner_pack_id={payload['owner_pack_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"recommended_decision={payload['recommended_owner_decision']}")


@dynamic_v3_rescue_app.command("validate-owner-research-decision-pack")
def dynamic_v3_validate_owner_research_decision_pack_command(
    owner_pack_id: Annotated[
        str,
        typer.Option("--owner-pack-id", help="owner decision pack id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="owner decision pack root。"),
    ] = weight_batch_search.DEFAULT_OWNER_RESEARCH_DECISION_PACK_DIR,
) -> None:
    payload = weight_batch_search.validate_owner_research_decision_pack_artifact(
        owner_pack_id=owner_pack_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)
