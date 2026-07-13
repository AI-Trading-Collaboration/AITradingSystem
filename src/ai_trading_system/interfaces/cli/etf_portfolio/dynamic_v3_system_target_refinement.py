from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Annotated, Any

import typer

from ai_trading_system.etf_portfolio import dynamic_v3_system_target_refinement as system_target
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_alternative_method_review_app,
    dynamic_v3_data_warning_repair_plan_app,
    dynamic_v3_limited_instability_app,
    dynamic_v3_limited_risk_attribution_app,
    dynamic_v3_refined_method_proposal_app,
    dynamic_v3_rescue_app,
)


def _mapping_obj(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _records_obj(value: Any) -> list[dict[str, Any]]:
    return [dict(item) for item in value] if isinstance(value, list) else []


@dynamic_v3_limited_instability_app.command("run")
def dynamic_v3_limited_instability_run_command(
    backfill_id: Annotated[str, typer.Option("--backfill-id", help="backfill id。")],
    consistency_id: Annotated[str, typer.Option("--consistency-id", help="consistency id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="limited instability artifact root。"),
    ] = system_target.DEFAULT_LIMITED_INSTABILITY_DIR,
) -> None:
    """运行 TRADING-224 limited_adjustment rolling instability diagnosis。"""
    result = system_target.run_limited_instability_diagnosis(
        backfill_id=backfill_id,
        consistency_id=consistency_id,
        output_dir=output_dir,
    )
    summary = result["instability_reason_summary"]
    typer.echo(f"instability_id={result['instability_id']}")
    typer.echo(f"instability_dir={result['instability_dir']}")
    typer.echo(f"unstable_window_count={summary['unstable_window_count']}")
    typer.echo(f"dominant_failure_regime={summary['dominant_failure_regime']}")
    typer.echo(f"recommendation={summary['recommendation']}")
    typer.echo("not_official_target_weights=true")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_limited_instability_app.command("report")
def dynamic_v3_limited_instability_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest limited instability。"),
    ] = False,
    instability_id: Annotated[
        str | None,
        typer.Option("--instability-id", help="instability id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="limited instability artifact root。"),
    ] = system_target.DEFAULT_LIMITED_INSTABILITY_DIR,
) -> None:
    """展示 TRADING-224 limited instability diagnosis 摘要。"""
    payload = system_target.limited_instability_report_payload(
        instability_id=instability_id,
        latest=latest,
        output_dir=output_dir,
    )
    summary = _mapping_obj(payload.get("instability_reason_summary"))
    typer.echo(f"instability_id={payload['instability_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"unstable_window_count={summary.get('unstable_window_count')}")
    typer.echo(f"dominant_failure_regime={summary.get('dominant_failure_regime')}")
    typer.echo(f"recommendation={summary.get('recommendation')}")
    typer.echo(f"report_path={payload['limited_instability_report_path']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_rescue_app.command("validate-limited-instability")
def dynamic_v3_validate_limited_instability_command(
    instability_id: Annotated[
        str,
        typer.Option("--instability-id", help="instability id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="limited instability artifact root。"),
    ] = system_target.DEFAULT_LIMITED_INSTABILITY_DIR,
) -> None:
    """校验 TRADING-224 limited instability artifact。"""
    payload = system_target.validate_limited_instability_artifact(
        instability_id=instability_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("not_official_target_weights=true")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_limited_risk_attribution_app.command("run")
def dynamic_v3_limited_risk_attribution_run_command(
    backfill_id: Annotated[str, typer.Option("--backfill-id", help="backfill id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="limited risk attribution artifact root。"),
    ] = system_target.DEFAULT_LIMITED_RISK_ATTRIBUTION_DIR,
) -> None:
    """运行 TRADING-225 return-improves-risk-worsens attribution。"""
    result = system_target.run_limited_risk_attribution(
        backfill_id=backfill_id,
        output_dir=output_dir,
    )
    exposure = result["exposure_shift_attribution"]
    returns = result["return_contribution_by_symbol"]
    drawdown = result["drawdown_contribution_by_symbol"]
    typer.echo(f"risk_attribution_id={result['risk_attribution_id']}")
    typer.echo(f"risk_attribution_dir={result['risk_attribution_dir']}")
    typer.echo(
        "top_return_contributors="
        + ",".join(str(item) for item in returns["top_positive_contributors"])
    )
    typer.echo(
        "top_drawdown_contributors="
        + ",".join(str(item) for item in drawdown["top_drawdown_contributors"])
    )
    typer.echo(f"risk_worsening_source={exposure['risk_worsening_source']}")
    typer.echo("not_official_target_weights=true")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_limited_risk_attribution_app.command("report")
def dynamic_v3_limited_risk_attribution_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest limited risk attribution。"),
    ] = False,
    risk_attribution_id: Annotated[
        str | None,
        typer.Option("--risk-attribution-id", "--attribution-id", help="risk attribution id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="limited risk attribution artifact root。"),
    ] = system_target.DEFAULT_LIMITED_RISK_ATTRIBUTION_DIR,
) -> None:
    """展示 TRADING-225 limited risk attribution 摘要。"""
    payload = system_target.limited_risk_attribution_report_payload(
        risk_attribution_id=risk_attribution_id,
        latest=latest,
        output_dir=output_dir,
    )
    exposure = _mapping_obj(payload.get("exposure_shift_attribution"))
    returns = _mapping_obj(payload.get("return_contribution_by_symbol"))
    drawdown = _mapping_obj(payload.get("drawdown_contribution_by_symbol"))
    typer.echo(f"risk_attribution_id={payload['risk_attribution_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(
        "top_return_contributors="
        + ",".join(str(item) for item in returns.get("top_positive_contributors", []))
    )
    typer.echo(
        "top_drawdown_contributors="
        + ",".join(str(item) for item in drawdown.get("top_drawdown_contributors", []))
    )
    typer.echo(f"risk_worsening_source={exposure.get('risk_worsening_source')}")
    typer.echo(f"report_path={payload['limited_risk_attribution_report_path']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_rescue_app.command("validate-limited-risk-attribution")
def dynamic_v3_validate_limited_risk_attribution_command(
    risk_attribution_id: Annotated[
        str,
        typer.Option("--attribution-id", "--risk-attribution-id", help="risk attribution id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="limited risk attribution artifact root。"),
    ] = system_target.DEFAULT_LIMITED_RISK_ATTRIBUTION_DIR,
) -> None:
    """校验 TRADING-225 limited risk attribution artifact。"""
    payload = system_target.validate_limited_risk_attribution_artifact(
        risk_attribution_id=risk_attribution_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("not_official_target_weights=true")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_data_warning_repair_plan_app.command("run")
def dynamic_v3_data_warning_repair_plan_run_command(
    impact_id: Annotated[str, typer.Option("--impact-id", help="data warning impact id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="data warning repair plan artifact root。"),
    ] = system_target.DEFAULT_DATA_WARNING_REPAIR_PLAN_DIR,
) -> None:
    """运行 TRADING-226 data warning blocking review / repair plan。"""
    result = system_target.run_data_warning_repair_plan(
        impact_id=impact_id,
        output_dir=output_dir,
    )
    matrix = result["warning_blocking_matrix"]
    typer.echo(f"repair_plan_id={result['repair_plan_id']}")
    typer.echo(f"repair_plan_dir={result['repair_plan_dir']}")
    typer.echo(f"overall_data_warning_status={matrix['overall_data_warning_status']}")
    typer.echo(f"hardening_allowed_after_repair={matrix['hardening_allowed_after_repair']}")
    typer.echo("auto_repair_executed=false")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_data_warning_repair_plan_app.command("report")
def dynamic_v3_data_warning_repair_plan_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest data warning repair plan。"),
    ] = False,
    repair_plan_id: Annotated[
        str | None,
        typer.Option("--repair-plan-id", help="repair plan id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="data warning repair plan artifact root。"),
    ] = system_target.DEFAULT_DATA_WARNING_REPAIR_PLAN_DIR,
) -> None:
    """展示 TRADING-226 data warning repair plan 摘要。"""
    payload = system_target.data_warning_repair_plan_report_payload(
        repair_plan_id=repair_plan_id,
        latest=latest,
        output_dir=output_dir,
    )
    matrix = _mapping_obj(payload.get("warning_blocking_matrix"))
    actions = _records_obj(payload.get("warning_repair_actions"))
    typer.echo(f"repair_plan_id={payload['repair_plan_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo("warning_ids=" + ",".join(str(row.get("warning_id")) for row in actions))
    typer.echo(f"overall_data_warning_status={matrix.get('overall_data_warning_status')}")
    typer.echo(f"hardening_allowed_after_repair={matrix.get('hardening_allowed_after_repair')}")
    typer.echo(f"report_path={payload['data_warning_repair_plan_report_path']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_rescue_app.command("validate-data-warning-repair-plan")
def dynamic_v3_validate_data_warning_repair_plan_command(
    repair_plan_id: Annotated[
        str,
        typer.Option("--repair-plan-id", help="repair plan id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="data warning repair plan artifact root。"),
    ] = system_target.DEFAULT_DATA_WARNING_REPAIR_PLAN_DIR,
) -> None:
    """校验 TRADING-226 data warning repair plan artifact。"""
    payload = system_target.validate_data_warning_repair_plan_artifact(
        repair_plan_id=repair_plan_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("not_official_target_weights=true")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_alternative_method_review_app.command("run")
def dynamic_v3_alternative_method_review_run_command(
    backfill_id: Annotated[str, typer.Option("--backfill-id", help="backfill id。")],
    risk_attribution_id: Annotated[
        str,
        typer.Option("--risk-attribution-id", help="risk attribution id。"),
    ],
    instability_id: Annotated[str, typer.Option("--instability-id", help="instability id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="alternative method review artifact root。"),
    ] = system_target.DEFAULT_ALTERNATIVE_METHOD_REVIEW_DIR,
) -> None:
    """运行 TRADING-227 alternative method candidate review。"""
    result = system_target.run_alternative_method_review(
        backfill_id=backfill_id,
        risk_attribution_id=risk_attribution_id,
        instability_id=instability_id,
        output_dir=output_dir,
    )
    scorecard = result["alternative_method_scorecard"]
    typer.echo(f"alt_review_id={result['alt_review_id']}")
    typer.echo(f"alt_review_dir={result['alt_review_dir']}")
    typer.echo(f"recommended_alternative={scorecard['recommended_alternative']}")
    typer.echo("auto_apply=false")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_alternative_method_review_app.command("report")
def dynamic_v3_alternative_method_review_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest alternative method review。"),
    ] = False,
    alt_review_id: Annotated[
        str | None,
        typer.Option("--alt-review-id", help="alternative review id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="alternative method review artifact root。"),
    ] = system_target.DEFAULT_ALTERNATIVE_METHOD_REVIEW_DIR,
) -> None:
    """展示 TRADING-227 alternative method review 摘要。"""
    payload = system_target.alternative_method_review_report_payload(
        alt_review_id=alt_review_id,
        latest=latest,
        output_dir=output_dir,
    )
    scorecard = _mapping_obj(payload.get("alternative_method_scorecard"))
    candidates = _mapping_obj(payload.get("alternative_method_candidates"))
    methods = [str(row.get("method")) for row in _records_obj(candidates.get("candidates"))]
    typer.echo(f"alt_review_id={payload['alt_review_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"recommended_alternative={scorecard.get('recommended_alternative')}")
    typer.echo("proposed_methods=" + ",".join(methods))
    typer.echo(f"report_path={payload['alternative_method_review_report_path']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_rescue_app.command("validate-alternative-method-review")
def dynamic_v3_validate_alternative_method_review_command(
    alt_review_id: Annotated[str, typer.Option("--alt-review-id", help="alternative review id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="alternative method review artifact root。"),
    ] = system_target.DEFAULT_ALTERNATIVE_METHOD_REVIEW_DIR,
) -> None:
    """校验 TRADING-227 alternative method review artifact。"""
    payload = system_target.validate_alternative_method_review_artifact(
        alt_review_id=alt_review_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("not_official_target_weights=true")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_refined_method_proposal_app.command("run")
def dynamic_v3_refined_method_proposal_run_command(
    instability_id: Annotated[str, typer.Option("--instability-id", help="instability id。")],
    risk_attribution_id: Annotated[
        str,
        typer.Option("--risk-attribution-id", help="risk attribution id。"),
    ],
    repair_plan_id: Annotated[
        str,
        typer.Option("--repair-plan-id", help="repair plan id。"),
    ],
    alt_review_id: Annotated[str, typer.Option("--alt-review-id", help="alternative review id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="refined method proposal artifact root。"),
    ] = system_target.DEFAULT_REFINED_METHOD_PROPOSAL_DIR,
) -> None:
    """运行 TRADING-228 refined research method proposal pack。"""
    result = system_target.run_refined_method_proposal(
        instability_id=instability_id,
        risk_attribution_id=risk_attribution_id,
        repair_plan_id=repair_plan_id,
        alt_review_id=alt_review_id,
        output_dir=output_dir,
    )
    decision = result["refined_method_decision"]
    methods = [
        str(row.get("method"))
        for row in _records_obj(result["proposed_next_methods"].get("methods"))
    ]
    typer.echo(f"proposal_id={result['proposal_id']}")
    typer.echo(f"proposal_dir={result['proposal_dir']}")
    typer.echo(f"recommended_next_step={decision['recommended_next_step']}")
    typer.echo("proposed_next_methods=" + ",".join(methods))
    typer.echo(f"confidence={decision['confidence']}")
    typer.echo("auto_apply=false")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_refined_method_proposal_app.command("report")
def dynamic_v3_refined_method_proposal_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest refined method proposal。"),
    ] = False,
    proposal_id: Annotated[
        str | None,
        typer.Option("--proposal-id", help="proposal id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="refined method proposal artifact root。"),
    ] = system_target.DEFAULT_REFINED_METHOD_PROPOSAL_DIR,
) -> None:
    """展示 TRADING-228 refined method proposal 摘要。"""
    payload = system_target.refined_method_proposal_report_payload(
        proposal_id=proposal_id,
        latest=latest,
        output_dir=output_dir,
    )
    decision = _mapping_obj(payload.get("refined_method_decision"))
    methods_payload = _mapping_obj(payload.get("proposed_next_methods"))
    methods = [str(row.get("method")) for row in _records_obj(methods_payload.get("methods"))]
    typer.echo(f"proposal_id={payload['proposal_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"recommended_next_step={decision.get('recommended_next_step')}")
    typer.echo("proposed_next_methods=" + ",".join(methods))
    typer.echo(f"confidence={decision.get('confidence')}")
    typer.echo(f"report_path={payload['refined_method_proposal_report_path']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_rescue_app.command("validate-refined-method-proposal")
def dynamic_v3_validate_refined_method_proposal_command(
    proposal_id: Annotated[str, typer.Option("--proposal-id", help="proposal id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="refined method proposal artifact root。"),
    ] = system_target.DEFAULT_REFINED_METHOD_PROPOSAL_DIR,
) -> None:
    """校验 TRADING-228 refined method proposal artifact。"""
    payload = system_target.validate_refined_method_proposal_artifact(
        proposal_id=proposal_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("not_official_target_weights=true")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)
