from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Annotated, Any

import typer

from ai_trading_system.etf_portfolio import (
    dynamic_v3_filtered_candidate_readiness as filtered_readiness,
)
from ai_trading_system.etf_portfolio import dynamic_v3_paper_shadow_health as paper_shadow_health
from ai_trading_system.etf_portfolio import (
    dynamic_v3_paper_shadow_outcome_attribution as paper_shadow_outcome_attribution,
)
from ai_trading_system.etf_portfolio import dynamic_v3_paper_shadow_weekly as paper_shadow_weekly
from ai_trading_system.etf_portfolio import (
    dynamic_v3_shadow_decision_comparison as shadow_decision_comparison,
)
from ai_trading_system.etf_portfolio import dynamic_v3_stress_scenarios as stress_scenarios
from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    DEFAULT_CANDIDATE_CLUSTER_DIR,
    DEFAULT_POSITION_ADVISORY_DIR,
    DEFAULT_POSITION_REVIEW_DIR,
    DEFAULT_SHADOW_SHORTLIST_DIR,
    DEFAULT_SHORTLIST_DIR,
    build_position_review_pack,
    position_review_report_payload,
    validate_position_review_artifact,
)
from ai_trading_system.interfaces.cli.etf_portfolio.common import mapping_obj as _mapping_obj
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_paper_shadow_outcome_attribution_app,
    dynamic_v3_position_review_app,
    dynamic_v3_rescue_app,
    dynamic_v3_shadow_decision_comparison_app,
    dynamic_v3_stress_scenario_library_app,
)


def _texts(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value] if value else []
    if isinstance(value, list | tuple | set):
        return [str(item) for item in value if item not in (None, "")]
    return [str(value)] if value != "" else []


def _echo_paper_shadow_outcome_attribution_summary(
    *,
    manifest: Mapping[str, Any],
    report: Mapping[str, Any],
    validation: Mapping[str, Any],
) -> None:
    typer.echo(f"attribution_id={report.get('attribution_id') or manifest.get('attribution_id')}")
    typer.echo(f"candidate={report.get('candidate') or manifest.get('candidate')}")
    typer.echo(
        "paper_shadow_outcome_attribution_status="
        f"{report.get('paper_shadow_outcome_attribution_status')}"
    )
    typer.echo(f"weekly_review_id={report.get('weekly_review_id')}")
    typer.echo(f"weekly_decision={report.get('weekly_decision')}")
    typer.echo(f"dominant_driver={report.get('dominant_driver')}")
    typer.echo(f"dominant_confidence={report.get('dominant_confidence')}")
    typer.echo(f"active_driver_count={report.get('active_driver_count')}")
    typer.echo(f"unknown_driver_count={report.get('unknown_driver_count')}")
    typer.echo(f"blocking_reasons={','.join(_texts(report.get('blocking_reasons')))}")
    typer.echo(f"warnings={','.join(_texts(report.get('warnings')))}")
    typer.echo(f"next_required_action={report.get('next_required_action')}")
    typer.echo(f"validation_status={validation.get('status', 'NOT_RUN')}")
    typer.echo(f"report_path={manifest.get('paper_shadow_outcome_attribution_markdown_path')}")
    typer.echo("paper_shadow_outcome_attribution_only=true")
    typer.echo("read_only_attribution=true")
    typer.echo("weekly_decision_mutated=false")
    typer.echo("data_downloaded_by_attribution=false")
    typer.echo("pipelines_executed_by_attribution=false")
    typer.echo("not_official_target_weights=true")
    typer.echo("broker_action_allowed=false")
    typer.echo("paper_account_state_mutated=false")
    typer.echo("production_effect=none")


@dynamic_v3_paper_shadow_outcome_attribution_app.command("run")
def dynamic_v3_paper_shadow_outcome_attribution_run_command(
    as_of: Annotated[
        str | None,
        typer.Option(
            "--as-of",
            "--date",
            help=(
                "paper-shadow outcome attribution as-of date YYYY-MM-DD；"
                "省略时使用 weekly end 或当前 UTC 日期。"
            ),
        ),
    ] = None,
    weekly_review_id: Annotated[
        str | None,
        typer.Option(
            "--weekly-review-id",
            "--paper-shadow-weekly-review-id",
            help="paper-shadow weekly review id；缺省读取 latest。",
        ),
    ] = None,
    weekly_review_dir: Annotated[
        Path,
        typer.Option("--weekly-review-dir", help="paper-shadow weekly review artifact root。"),
    ] = paper_shadow_weekly.DEFAULT_PAPER_SHADOW_WEEKLY_REVIEW_DIR,
    paper_shadow_health_id: Annotated[
        str | None,
        typer.Option(
            "--paper-shadow-health-id",
            "--health-id",
            help="paper-shadow health id；缺省读取 latest。",
        ),
    ] = None,
    paper_shadow_health_report_path: Annotated[
        Path | None,
        typer.Option(
            "--paper-shadow-health-report-path",
            help="显式 paper-shadow health report JSON。",
        ),
    ] = None,
    paper_shadow_health_dir: Annotated[
        Path,
        typer.Option("--paper-shadow-health-dir", help="paper-shadow health artifact root。"),
    ] = paper_shadow_health.DEFAULT_PAPER_SHADOW_HEALTH_DIR,
    config_path: Annotated[
        Path,
        typer.Option("--config", "--config-path", help="outcome attribution policy YAML。"),
    ] = paper_shadow_outcome_attribution.DEFAULT_OUTCOME_ATTRIBUTION_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="paper-shadow outcome attribution artifact root。"),
    ] = paper_shadow_outcome_attribution.DEFAULT_OUTCOME_ATTRIBUTION_DIR,
) -> None:
    result = paper_shadow_outcome_attribution.run_paper_shadow_outcome_attribution(
        as_of=None if as_of is None else _parse_dynamic_v3_outcome_date(as_of, "--as-of"),
        weekly_review_id=weekly_review_id,
        weekly_review_dir=weekly_review_dir,
        paper_shadow_health_id=paper_shadow_health_id,
        paper_shadow_health_report_path=paper_shadow_health_report_path,
        paper_shadow_health_dir=paper_shadow_health_dir,
        config_path=config_path,
        output_dir=output_dir,
    )
    _echo_paper_shadow_outcome_attribution_summary(
        manifest=_mapping_obj(result.get("manifest")),
        report=_mapping_obj(result.get("paper_shadow_outcome_attribution_report")),
        validation=_mapping_obj(result.get("paper_shadow_outcome_attribution_validation")),
    )


@dynamic_v3_paper_shadow_outcome_attribution_app.command("report")
def dynamic_v3_paper_shadow_outcome_attribution_report_command(
    latest: Annotated[bool, typer.Option("--latest/--no-latest", help="读取 latest。")] = False,
    attribution_id: Annotated[
        str | None,
        typer.Option("--attribution-id", help="paper-shadow outcome attribution artifact id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="paper-shadow outcome attribution artifact root。"),
    ] = paper_shadow_outcome_attribution.DEFAULT_OUTCOME_ATTRIBUTION_DIR,
) -> None:
    if not latest and not attribution_id:
        raise typer.BadParameter("--attribution-id or --latest is required")
    payload = paper_shadow_outcome_attribution.paper_shadow_outcome_attribution_report_payload(
        attribution_id=attribution_id,
        latest=latest,
        output_dir=output_dir,
    )
    _echo_paper_shadow_outcome_attribution_summary(
        manifest=_mapping_obj(payload),
        report=_mapping_obj(payload.get("paper_shadow_outcome_attribution_report")),
        validation=_mapping_obj(payload.get("paper_shadow_outcome_attribution_validation")),
    )


@dynamic_v3_rescue_app.command("validate-paper-shadow-outcome-attribution")
def dynamic_v3_validate_paper_shadow_outcome_attribution_command(
    attribution_id: Annotated[
        str | None,
        typer.Option("--attribution-id", help="paper-shadow outcome attribution artifact id。"),
    ] = None,
    latest: Annotated[bool, typer.Option("--latest/--no-latest", help="读取 latest。")] = False,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="paper-shadow outcome attribution artifact root。"),
    ] = paper_shadow_outcome_attribution.DEFAULT_OUTCOME_ATTRIBUTION_DIR,
) -> None:
    resolved_id = attribution_id
    if latest:
        payload = paper_shadow_outcome_attribution.paper_shadow_outcome_attribution_report_payload(
            latest=True,
            output_dir=output_dir,
        )
        resolved_id = str(payload.get("attribution_id") or "")
    if not resolved_id:
        raise typer.BadParameter("--attribution-id or --latest is required")
    _echo_validation_payload(
        paper_shadow_outcome_attribution.validate_paper_shadow_outcome_attribution_artifact(
            attribution_id=resolved_id,
            output_dir=output_dir,
        )
    )


def _echo_shadow_decision_comparison_summary(
    *,
    manifest: Mapping[str, Any],
    report: Mapping[str, Any],
    validation: Mapping[str, Any],
) -> None:
    previous_state = _mapping_obj(report.get("previous_state"))
    current_state = _mapping_obj(report.get("current_state"))
    typer.echo(f"comparison_id={report.get('comparison_id') or manifest.get('comparison_id')}")
    typer.echo(f"candidate={report.get('candidate') or manifest.get('candidate')}")
    typer.echo(
        f"shadow_decision_comparison_status={report.get('shadow_decision_comparison_status')}"
    )
    typer.echo(f"current_readiness_id={report.get('current_readiness_id')}")
    typer.echo(f"previous_readiness_id={report.get('previous_readiness_id')}")
    typer.echo(f"decision_changed={report.get('decision_changed')}")
    typer.echo(f"change_classification={report.get('change_classification')}")
    typer.echo(f"change_reason={report.get('change_reason')}")
    typer.echo(
        f"previous_state={previous_state.get('readiness_status')}:"
        f"safe={previous_state.get('safe_to_continue_shadow')}:"
        f"weekly={previous_state.get('weekly_decision')}:"
        f"signal={previous_state.get('signal_input_completeness')}"
    )
    typer.echo(
        f"current_state={current_state.get('readiness_status')}:"
        f"safe={current_state.get('safe_to_continue_shadow')}:"
        f"weekly={current_state.get('weekly_decision')}:"
        f"signal={current_state.get('signal_input_completeness')}"
    )
    typer.echo(f"recommended_owner_action={report.get('recommended_owner_action')}")
    typer.echo(f"validation_status={validation.get('status', 'NOT_RUN')}")
    typer.echo(f"report_path={manifest.get('shadow_decision_comparison_markdown_path')}")
    typer.echo("shadow_decision_comparison_only=true")
    typer.echo("read_only_comparison=true")
    typer.echo("decision_mutated=false")
    typer.echo("data_downloaded_by_comparison=false")
    typer.echo("pipelines_executed_by_comparison=false")
    typer.echo("not_official_target_weights=true")
    typer.echo("broker_action_allowed=false")
    typer.echo("paper_account_state_mutated=false")
    typer.echo("production_effect=none")


@dynamic_v3_shadow_decision_comparison_app.command("run")
def dynamic_v3_shadow_decision_comparison_run_command(
    as_of: Annotated[
        str | None,
        typer.Option(
            "--as-of",
            "--date",
            help=(
                "shadow decision comparison as-of date YYYY-MM-DD；"
                "省略时使用 current readiness as_of 或当前 UTC 日期。"
            ),
        ),
    ] = None,
    current_readiness_id: Annotated[
        str | None,
        typer.Option(
            "--current-readiness-id",
            "--current-shadow-continuation-readiness-id",
            help="current shadow continuation readiness id；缺省读取 latest。",
        ),
    ] = None,
    previous_readiness_id: Annotated[
        str | None,
        typer.Option(
            "--previous-readiness-id",
            "--previous-shadow-continuation-readiness-id",
            help=(
                "previous shadow continuation readiness id；"
                "缺省从 artifact root 解析 latest prior。"
            ),
        ),
    ] = None,
    readiness_dir: Annotated[
        Path,
        typer.Option("--readiness-dir", help="shadow continuation readiness artifact root。"),
    ] = filtered_readiness.DEFAULT_SHADOW_CONTINUATION_READINESS_DIR,
    weekly_review_dir: Annotated[
        Path,
        typer.Option("--weekly-review-dir", help="paper-shadow weekly review artifact root。"),
    ] = paper_shadow_weekly.DEFAULT_PAPER_SHADOW_WEEKLY_REVIEW_DIR,
    paper_shadow_health_dir: Annotated[
        Path,
        typer.Option("--paper-shadow-health-dir", help="paper-shadow health artifact root。"),
    ] = paper_shadow_health.DEFAULT_PAPER_SHADOW_HEALTH_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="shadow decision comparison artifact root。"),
    ] = shadow_decision_comparison.DEFAULT_SHADOW_DECISION_COMPARISON_DIR,
) -> None:
    result = shadow_decision_comparison.run_shadow_decision_comparison(
        as_of=None if as_of is None else _parse_dynamic_v3_outcome_date(as_of, "--as-of"),
        current_readiness_id=current_readiness_id,
        previous_readiness_id=previous_readiness_id,
        readiness_dir=readiness_dir,
        weekly_review_dir=weekly_review_dir,
        paper_shadow_health_dir=paper_shadow_health_dir,
        output_dir=output_dir,
    )
    _echo_shadow_decision_comparison_summary(
        manifest=_mapping_obj(result.get("manifest")),
        report=_mapping_obj(result.get("shadow_decision_comparison_report")),
        validation=_mapping_obj(result.get("shadow_decision_comparison_validation")),
    )


@dynamic_v3_shadow_decision_comparison_app.command("report")
def dynamic_v3_shadow_decision_comparison_report_command(
    latest: Annotated[bool, typer.Option("--latest/--no-latest", help="读取 latest。")] = False,
    comparison_id: Annotated[
        str | None,
        typer.Option("--comparison-id", help="shadow decision comparison artifact id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="shadow decision comparison artifact root。"),
    ] = shadow_decision_comparison.DEFAULT_SHADOW_DECISION_COMPARISON_DIR,
) -> None:
    if not latest and not comparison_id:
        raise typer.BadParameter("--comparison-id or --latest is required")
    payload = shadow_decision_comparison.shadow_decision_comparison_report_payload(
        comparison_id=comparison_id,
        latest=latest,
        output_dir=output_dir,
    )
    _echo_shadow_decision_comparison_summary(
        manifest=_mapping_obj(payload),
        report=_mapping_obj(payload.get("shadow_decision_comparison_report")),
        validation=_mapping_obj(payload.get("shadow_decision_comparison_validation")),
    )


@dynamic_v3_rescue_app.command("validate-shadow-decision-comparison")
def dynamic_v3_validate_shadow_decision_comparison_command(
    comparison_id: Annotated[
        str | None,
        typer.Option("--comparison-id", help="shadow decision comparison artifact id。"),
    ] = None,
    latest: Annotated[bool, typer.Option("--latest/--no-latest", help="读取 latest。")] = False,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="shadow decision comparison artifact root。"),
    ] = shadow_decision_comparison.DEFAULT_SHADOW_DECISION_COMPARISON_DIR,
) -> None:
    resolved_id = comparison_id
    if latest:
        payload = shadow_decision_comparison.shadow_decision_comparison_report_payload(
            latest=True,
            output_dir=output_dir,
        )
        resolved_id = str(payload.get("comparison_id") or "")
    if not resolved_id:
        raise typer.BadParameter("--comparison-id or --latest is required")
    _echo_validation_payload(
        shadow_decision_comparison.validate_shadow_decision_comparison_artifact(
            comparison_id=resolved_id,
            output_dir=output_dir,
        )
    )


@dynamic_v3_stress_scenario_library_app.command("report")
def dynamic_v3_stress_scenario_library_report_command(
    latest: Annotated[bool, typer.Option("--latest/--no-latest", help="读取 latest。")] = False,
    library_run_id: Annotated[
        str | None,
        typer.Option("--library-run-id", help="stress scenario library run id。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", "--config-path", help="stress scenario library YAML。"),
    ] = stress_scenarios.DEFAULT_STRESS_SCENARIO_LIBRARY_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="stress scenario library artifact root。"),
    ] = stress_scenarios.DEFAULT_STRESS_SCENARIO_LIBRARY_DIR,
) -> None:
    if latest or library_run_id:
        payload = stress_scenarios.stress_scenario_library_report_payload(
            library_run_id=library_run_id,
            latest=latest,
            output_dir=output_dir,
        )
        manifest = _mapping_obj(payload)
        library = _mapping_obj(payload.get("stress_scenario_library"))
        validation = _mapping_obj(payload.get("stress_scenario_validation"))
    else:
        result = stress_scenarios.build_stress_scenario_library(
            config_path=config_path,
            output_dir=output_dir,
        )
        manifest = _mapping_obj(result.get("manifest"))
        library = _mapping_obj(result.get("stress_scenario_library"))
        validation = _mapping_obj(result.get("stress_scenario_validation"))
    typer.echo(f"library_run_id={manifest.get('library_run_id')}")
    typer.echo(f"stress_scenario_library_id={library.get('stress_scenario_library_id')}")
    typer.echo(f"scenario_count={library.get('scenario_count')}")
    typer.echo(f"required_scenarios_present={library.get('required_scenarios_present')}")
    typer.echo(f"candidate_validation_use={library.get('candidate_validation_use')}")
    typer.echo(f"next_validation_action={library.get('next_validation_action')}")
    typer.echo(f"validation_status={validation.get('status', 'NOT_RUN')}")
    typer.echo(f"report_path={manifest.get('stress_scenario_report_path')}")
    typer.echo("stress_scenario_library_only=true")
    typer.echo("data_downloaded_by_library=false")
    typer.echo("pipelines_executed_by_library=false")
    typer.echo("broker_action_allowed=false")
    typer.echo("production_effect=none")


@dynamic_v3_rescue_app.command("validate-stress-scenario-library")
def dynamic_v3_validate_stress_scenario_library_command(
    library_run_id: Annotated[
        str | None,
        typer.Option("--library-run-id", help="stress scenario library run id。"),
    ] = None,
    latest: Annotated[bool, typer.Option("--latest/--no-latest", help="读取 latest。")] = False,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="stress scenario library artifact root。"),
    ] = stress_scenarios.DEFAULT_STRESS_SCENARIO_LIBRARY_DIR,
) -> None:
    resolved_id = library_run_id
    if latest:
        payload = stress_scenarios.stress_scenario_library_report_payload(
            latest=True,
            output_dir=output_dir,
        )
        resolved_id = str(payload.get("library_run_id") or "")
    if not resolved_id:
        raise typer.BadParameter("--library-run-id or --latest is required")
    _echo_validation_payload(
        stress_scenarios.validate_stress_scenario_library_artifact(
            library_run_id=resolved_id,
            output_dir=output_dir,
        )
    )


def _parse_dynamic_v3_outcome_date(value: str, option_name: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter(f"{option_name} must use YYYY-MM-DD") from exc


def _echo_validation_payload(payload: Mapping[str, Any]) -> None:
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_allowed=false")
    typer.echo("production_effect=none")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_position_review_app.command("pack")
def dynamic_v3_position_review_pack_command(
    shortlist_id: Annotated[str, typer.Option("--shortlist-id", help="shortlist id。")],
    cluster_id: Annotated[str, typer.Option("--cluster-id", help="cluster id。")],
    shadow_shortlist_id: Annotated[
        str,
        typer.Option("--shadow-shortlist-id", help="shadow shortlist id。"),
    ],
    advisory_id: Annotated[str, typer.Option("--advisory-id", help="advisory id。")],
    shortlist_dir: Annotated[
        Path,
        typer.Option("--shortlist-dir", help="shortlist artifact root。"),
    ] = DEFAULT_SHORTLIST_DIR,
    cluster_dir: Annotated[
        Path,
        typer.Option("--cluster-dir", help="candidate cluster artifact root。"),
    ] = DEFAULT_CANDIDATE_CLUSTER_DIR,
    shadow_shortlist_dir: Annotated[
        Path,
        typer.Option("--shadow-shortlist-dir", help="shadow shortlist artifact root。"),
    ] = DEFAULT_SHADOW_SHORTLIST_DIR,
    advisory_dir: Annotated[
        Path,
        typer.Option("--advisory-dir", help="position advisory artifact root。"),
    ] = DEFAULT_POSITION_ADVISORY_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="position review artifact root。"),
    ] = DEFAULT_POSITION_REVIEW_DIR,
) -> None:
    """生成 TRADING-130 position review pack。"""
    result = build_position_review_pack(
        shortlist_id=shortlist_id,
        cluster_id=cluster_id,
        shadow_shortlist_id=shadow_shortlist_id,
        advisory_id=advisory_id,
        shortlist_dir=shortlist_dir,
        cluster_dir=cluster_dir,
        shadow_shortlist_dir=shadow_shortlist_dir,
        advisory_dir=advisory_dir,
        output_dir=output_dir,
    )
    decision = result["go_no_go_decision"]
    typer.echo(f"review_id={result['review_id']}")
    typer.echo(f"review_dir={result['review_dir']}")
    typer.echo(f"shadow_observation_readiness={decision['shadow_observation_readiness']}")
    typer.echo(f"position_advisory_readiness={decision['position_advisory_readiness']}")
    typer.echo(f"production_readiness={decision['production_readiness']}")
    typer.echo(f"recommended_next_action={decision['recommended_next_action']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_position_review_app.command("report")
def dynamic_v3_position_review_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest position review pointer。"),
    ] = False,
    review_id: Annotated[str | None, typer.Option("--review-id", help="review id。")] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="position review artifact root。"),
    ] = DEFAULT_POSITION_REVIEW_DIR,
) -> None:
    """展示 TRADING-130 position review 摘要。"""
    payload = position_review_report_payload(
        review_id=review_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"review_id={payload['review_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"production_readiness={payload['production_readiness']}")
    typer.echo(f"recommended_next_action={payload['recommended_next_action']}")
    typer.echo(f"report_path={payload['position_review_report_path']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_rescue_app.command("validate-position-review")
def dynamic_v3_validate_position_review_command(
    review_id: Annotated[str, typer.Option("--review-id", help="review id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="position review artifact root。"),
    ] = DEFAULT_POSITION_REVIEW_DIR,
) -> None:
    """校验 TRADING-130 position review artifact。"""
    payload = validate_position_review_artifact(review_id=review_id, output_dir=output_dir)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)
