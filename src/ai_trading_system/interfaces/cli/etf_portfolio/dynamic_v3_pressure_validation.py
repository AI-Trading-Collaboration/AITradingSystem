from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio.dynamic_v3_pressure_validation import (
    DEFAULT_DEFENSIVE_PRESSURE_COMPARE_DIR,
    DEFAULT_DEFENSIVE_RULE_REVIEW_DIR,
    DEFAULT_PRESSURE_OUTCOME_BACKFILL_DIR,
    DEFAULT_PRESSURE_REGIME_TAG_DIR,
    DEFAULT_PRESSURE_TAG_DIAGNOSIS_DIR,
    DEFAULT_WEEKLY_OPS_DECISION_UPDATE_DIR,
    defensive_pressure_compare_report_payload,
    defensive_rule_review_report_payload,
    pressure_outcome_backfill_report_payload,
    pressure_tag_diagnosis_report_payload,
    run_defensive_pressure_compare,
    run_defensive_rule_review,
    run_pressure_outcome_backfill,
    run_pressure_tag_diagnosis,
    run_weekly_ops_decision_update,
    validate_defensive_pressure_compare_artifact,
    validate_defensive_rule_review_artifact,
    validate_pressure_outcome_backfill_artifact,
    validate_pressure_tag_diagnosis_artifact,
    validate_weekly_ops_decision_update_artifact,
    weekly_ops_decision_update_report_payload,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_defensive_pressure_compare_app,
    dynamic_v3_defensive_rule_review_app,
    dynamic_v3_pressure_outcome_backfill_app,
    dynamic_v3_pressure_tag_diagnosis_app,
    dynamic_v3_rescue_app,
    dynamic_v3_weekly_ops_decision_update_app,
)


def _parse_dynamic_v3_outcome_date(value: str, option_name: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter(f"{option_name} must use YYYY-MM-DD") from exc


@dynamic_v3_pressure_tag_diagnosis_app.command("run")
def dynamic_v3_pressure_tag_diagnosis_run_command(
    tag_id: Annotated[str, typer.Option("--tag-id", "--tag_id", help="source pressure tag id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="pressure tag diagnosis artifact root。"),
    ] = DEFAULT_PRESSURE_TAG_DIAGNOSIS_DIR,
    pressure_tag_dir: Annotated[
        Path,
        typer.Option("--pressure-tag-dir", help="pressure regime tag artifact root。"),
    ] = DEFAULT_PRESSURE_REGIME_TAG_DIR,
) -> None:
    """运行 TRADING-184 pressure tag threshold/mapping diagnosis。"""
    result = run_pressure_tag_diagnosis(
        tag_id=tag_id,
        output_dir=output_dir,
        pressure_tag_dir=pressure_tag_dir,
    )
    summary = result["diagnosis_summary"]
    mapping = result["outcome_mapping_diagnostics"]
    typer.echo(f"diagnosis_id={result['diagnosis_id']}")
    typer.echo(f"status={result['manifest']['status']}")
    typer.echo(f"primary_reason={summary['primary_reason']}")
    typer.echo(f"pressure_relevant_outcomes={mapping['pressure_relevant_outcomes']}")
    typer.echo(
        "backtest_simulation_pressure_outcomes_available="
        f"{mapping['backtest_simulation_pressure_outcomes_available']}"
    )
    typer.echo("policy_change_allowed=false")
    typer.echo("production_effect=none")


@dynamic_v3_pressure_tag_diagnosis_app.command("report")
def dynamic_v3_pressure_tag_diagnosis_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest pressure tag diagnosis。"),
    ] = False,
    diagnosis_id: Annotated[
        str | None,
        typer.Option("--diagnosis-id", "--diagnosis_id", help="diagnosis id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="pressure tag diagnosis artifact root。"),
    ] = DEFAULT_PRESSURE_TAG_DIAGNOSIS_DIR,
) -> None:
    """展示 TRADING-184 pressure tag diagnosis 摘要。"""
    payload = pressure_tag_diagnosis_report_payload(
        diagnosis_id=diagnosis_id,
        latest=latest,
        output_dir=output_dir,
    )
    summary = payload["diagnosis_summary"]
    mapping = payload["outcome_mapping_diagnostics"]
    typer.echo(f"diagnosis_id={payload['diagnosis_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"primary_reason={summary['primary_reason']}")
    typer.echo(f"pressure_relevant_outcomes={mapping['pressure_relevant_outcomes']}")
    typer.echo(f"report_path={payload['pressure_tag_diagnosis_report_path']}")
    typer.echo("production_effect=none")


@dynamic_v3_rescue_app.command("validate-pressure-tag-diagnosis")
def dynamic_v3_validate_pressure_tag_diagnosis_command(
    diagnosis_id: Annotated[
        str,
        typer.Option("--diagnosis-id", "--diagnosis_id", help="pressure tag diagnosis id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="pressure tag diagnosis artifact root。"),
    ] = DEFAULT_PRESSURE_TAG_DIAGNOSIS_DIR,
) -> None:
    """校验 TRADING-184 pressure tag diagnosis artifact。"""
    payload = validate_pressure_tag_diagnosis_artifact(
        diagnosis_id=diagnosis_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("policy_change_allowed=false")
    typer.echo("production_effect=none")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_pressure_outcome_backfill_app.command("run")
def dynamic_v3_pressure_outcome_backfill_run_command(
    start: Annotated[str, typer.Option("--start", help="start date YYYY-MM-DD。")],
    end: Annotated[str, typer.Option("--end", help="end date YYYY-MM-DD。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="pressure outcome backfill artifact root。"),
    ] = DEFAULT_PRESSURE_OUTCOME_BACKFILL_DIR,
) -> None:
    """运行 TRADING-185 pressure outcome backfill。"""
    result = run_pressure_outcome_backfill(
        start=_parse_dynamic_v3_outcome_date(start, "--start"),
        end=_parse_dynamic_v3_outcome_date(end, "--end"),
        output_dir=output_dir,
    )
    summary = result["pressure_source_summary"]
    by_source = summary["by_source_mode"]
    typer.echo(f"pressure_backfill_id={result['pressure_backfill_id']}")
    typer.echo(f"status={result['manifest']['status']}")
    typer.echo(f"total_pressure_outcomes={summary['total_pressure_outcomes']}")
    typer.echo(f"FORWARD_OUTCOME={by_source['FORWARD_OUTCOME']}")
    typer.echo(f"HISTORICAL_REPLAY={by_source['HISTORICAL_REPLAY']}")
    typer.echo(f"BACKTEST_SIMULATION={by_source['BACKTEST_SIMULATION']}")
    typer.echo(
        f"defensive_validation_relevant_count={summary['defensive_validation_relevant_count']}"
    )
    typer.echo("production_effect=none")


@dynamic_v3_pressure_outcome_backfill_app.command("report")
def dynamic_v3_pressure_outcome_backfill_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest pressure outcome backfill。"),
    ] = False,
    backfill_id: Annotated[
        str | None,
        typer.Option("--backfill-id", "--pressure-backfill-id", help="pressure backfill id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="pressure outcome backfill artifact root。"),
    ] = DEFAULT_PRESSURE_OUTCOME_BACKFILL_DIR,
) -> None:
    """展示 TRADING-185 pressure outcome backfill 摘要。"""
    payload = pressure_outcome_backfill_report_payload(
        backfill_id=backfill_id,
        latest=latest,
        output_dir=output_dir,
    )
    summary = payload["pressure_source_summary"]
    typer.echo(f"pressure_backfill_id={payload['pressure_backfill_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"total_pressure_outcomes={summary['total_pressure_outcomes']}")
    typer.echo(
        f"defensive_validation_relevant_count={summary['defensive_validation_relevant_count']}"
    )
    typer.echo(f"report_path={payload['pressure_backfill_report_path']}")
    typer.echo("production_effect=none")


@dynamic_v3_rescue_app.command("validate-pressure-outcome-backfill")
def dynamic_v3_validate_pressure_outcome_backfill_command(
    backfill_id: Annotated[
        str,
        typer.Option(
            "--backfill-id",
            "--pressure-backfill-id",
            help="pressure outcome backfill id。",
        ),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="pressure outcome backfill artifact root。"),
    ] = DEFAULT_PRESSURE_OUTCOME_BACKFILL_DIR,
) -> None:
    """校验 TRADING-185 pressure outcome backfill artifact。"""
    payload = validate_pressure_outcome_backfill_artifact(
        backfill_id=backfill_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_allowed=false")
    typer.echo("production_effect=none")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_defensive_pressure_compare_app.command("run")
def dynamic_v3_defensive_pressure_compare_run_command(
    pressure_backfill_id: Annotated[
        str,
        typer.Option("--pressure-backfill-id", "--backfill-id", help="pressure backfill id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="defensive pressure compare artifact root。"),
    ] = DEFAULT_DEFENSIVE_PRESSURE_COMPARE_DIR,
    backfill_dir: Annotated[
        Path,
        typer.Option("--backfill-dir", help="pressure outcome backfill artifact root。"),
    ] = DEFAULT_PRESSURE_OUTCOME_BACKFILL_DIR,
) -> None:
    """运行 TRADING-186 defensive pressure-window comparison。"""
    result = run_defensive_pressure_compare(
        pressure_backfill_id=pressure_backfill_id,
        backfill_dir=backfill_dir,
        output_dir=output_dir,
    )
    summary = result["defensive_pressure_summary"]
    typer.echo(f"comparison_id={result['comparison_id']}")
    typer.echo(f"status={result['manifest']['status']}")
    typer.echo(f"defensive_status={summary['defensive_status']}")
    typer.echo(f"can_support_rule_approval={summary['can_support_rule_approval']}")
    typer.echo("production_effect=none")


@dynamic_v3_defensive_pressure_compare_app.command("report")
def dynamic_v3_defensive_pressure_compare_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest defensive pressure compare。"),
    ] = False,
    comparison_id: Annotated[
        str | None,
        typer.Option("--comparison-id", "--comparison_id", help="comparison id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="defensive pressure compare artifact root。"),
    ] = DEFAULT_DEFENSIVE_PRESSURE_COMPARE_DIR,
) -> None:
    """展示 TRADING-186 defensive pressure compare 摘要。"""
    payload = defensive_pressure_compare_report_payload(
        comparison_id=comparison_id,
        latest=latest,
        output_dir=output_dir,
    )
    summary = payload["defensive_pressure_summary"]
    typer.echo(f"comparison_id={payload['comparison_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"defensive_status={summary['defensive_status']}")
    typer.echo(f"can_support_rule_approval={summary['can_support_rule_approval']}")
    typer.echo(f"report_path={payload['defensive_pressure_compare_report_path']}")
    typer.echo("production_effect=none")


@dynamic_v3_rescue_app.command("validate-defensive-pressure-compare")
def dynamic_v3_validate_defensive_pressure_compare_command(
    comparison_id: Annotated[
        str,
        typer.Option("--comparison-id", "--comparison_id", help="defensive comparison id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="defensive pressure compare artifact root。"),
    ] = DEFAULT_DEFENSIVE_PRESSURE_COMPARE_DIR,
) -> None:
    """校验 TRADING-186 defensive pressure compare artifact。"""
    payload = validate_defensive_pressure_compare_artifact(
        comparison_id=comparison_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_allowed=false")
    typer.echo("production_effect=none")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_defensive_rule_review_app.command("run")
def dynamic_v3_defensive_rule_review_run_command(
    comparison_id: Annotated[
        str,
        typer.Option("--comparison-id", "--comparison_id", help="defensive comparison id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="defensive rule review artifact root。"),
    ] = DEFAULT_DEFENSIVE_RULE_REVIEW_DIR,
    comparison_dir: Annotated[
        Path,
        typer.Option("--comparison-dir", help="defensive pressure compare artifact root。"),
    ] = DEFAULT_DEFENSIVE_PRESSURE_COMPARE_DIR,
) -> None:
    """运行 TRADING-187 defensive rule status review。"""
    result = run_defensive_rule_review(
        comparison_id=comparison_id,
        comparison_dir=comparison_dir,
        output_dir=output_dir,
    )
    matrix = result["defensive_rule_decision_matrix"]
    typer.echo(f"review_id={result['review_id']}")
    typer.echo(f"status={result['manifest']['status']}")
    typer.echo(f"recommended_status={matrix['recommended_status']}")
    typer.echo(f"rule_approval_allowed={matrix['rule_approval_allowed']}")
    typer.echo("auto_apply=false")
    typer.echo("production_effect=none")


@dynamic_v3_defensive_rule_review_app.command("report")
def dynamic_v3_defensive_rule_review_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest defensive rule review。"),
    ] = False,
    review_id: Annotated[
        str | None,
        typer.Option("--review-id", "--review_id", help="defensive rule review id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="defensive rule review artifact root。"),
    ] = DEFAULT_DEFENSIVE_RULE_REVIEW_DIR,
) -> None:
    """展示 TRADING-187 defensive rule review 摘要。"""
    payload = defensive_rule_review_report_payload(
        review_id=review_id,
        latest=latest,
        output_dir=output_dir,
    )
    matrix = payload["defensive_rule_decision_matrix"]
    typer.echo(f"review_id={payload['review_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"recommended_status={matrix['recommended_status']}")
    typer.echo(f"rule_approval_allowed={matrix['rule_approval_allowed']}")
    typer.echo(f"report_path={payload['defensive_rule_review_report_path']}")
    typer.echo("production_effect=none")


@dynamic_v3_rescue_app.command("validate-defensive-rule-review")
def dynamic_v3_validate_defensive_rule_review_command(
    review_id: Annotated[
        str,
        typer.Option("--review-id", "--review_id", help="defensive rule review id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="defensive rule review artifact root。"),
    ] = DEFAULT_DEFENSIVE_RULE_REVIEW_DIR,
) -> None:
    """校验 TRADING-187 defensive rule review artifact。"""
    payload = validate_defensive_rule_review_artifact(
        review_id=review_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("auto_apply=false")
    typer.echo("production_effect=none")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_weekly_ops_decision_update_app.command("run")
def dynamic_v3_weekly_ops_decision_update_run_command(
    weekly_cycle_id: Annotated[
        str,
        typer.Option("--weekly-cycle-id", "--weekly_cycle_id", help="weekly cycle id。"),
    ],
    pressure_backfill_id: Annotated[
        str,
        typer.Option("--pressure-backfill-id", "--backfill-id", help="pressure backfill id。"),
    ],
    defensive_review_id: Annotated[
        str,
        typer.Option("--defensive-review-id", "--review-id", help="defensive review id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="weekly ops decision update artifact root。"),
    ] = DEFAULT_WEEKLY_OPS_DECISION_UPDATE_DIR,
) -> None:
    """运行 TRADING-188 weekly operations decision update。"""
    result = run_weekly_ops_decision_update(
        weekly_cycle_id=weekly_cycle_id,
        pressure_backfill_id=pressure_backfill_id,
        defensive_review_id=defensive_review_id,
        output_dir=output_dir,
    )
    matrix = result["updated_weekly_decision_matrix"]
    typer.echo(f"decision_update_id={result['decision_update_id']}")
    typer.echo(f"status={result['manifest']['status']}")
    typer.echo(f"weekly_recommendation={matrix['weekly_recommendation']}")
    typer.echo(f"policy_change_allowed={matrix['policy_change_allowed']}")
    typer.echo(f"broker_action_allowed={matrix['broker_action_allowed']}")
    typer.echo("production_effect=none")


@dynamic_v3_weekly_ops_decision_update_app.command("report")
def dynamic_v3_weekly_ops_decision_update_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest weekly ops decision update。"),
    ] = False,
    decision_update_id: Annotated[
        str | None,
        typer.Option(
            "--decision-update-id",
            "--decision_update_id",
            help="weekly ops decision update id。",
        ),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="weekly ops decision update artifact root。"),
    ] = DEFAULT_WEEKLY_OPS_DECISION_UPDATE_DIR,
) -> None:
    """展示 TRADING-188 weekly ops decision update 摘要。"""
    payload = weekly_ops_decision_update_report_payload(
        decision_update_id=decision_update_id,
        latest=latest,
        output_dir=output_dir,
    )
    matrix = payload["updated_weekly_decision_matrix"]
    typer.echo(f"decision_update_id={payload['decision_update_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"weekly_recommendation={matrix['weekly_recommendation']}")
    typer.echo(f"policy_change_allowed={matrix['policy_change_allowed']}")
    typer.echo(f"broker_action_allowed={matrix['broker_action_allowed']}")
    typer.echo(f"report_path={payload['weekly_ops_decision_update_report_path']}")
    typer.echo("production_effect=none")


@dynamic_v3_rescue_app.command("validate-weekly-ops-decision-update")
def dynamic_v3_validate_weekly_ops_decision_update_command(
    decision_update_id: Annotated[
        str,
        typer.Option(
            "--decision-update-id",
            "--decision_update_id",
            help="weekly ops decision update id。",
        ),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="weekly ops decision update artifact root。"),
    ] = DEFAULT_WEEKLY_OPS_DECISION_UPDATE_DIR,
) -> None:
    """校验 TRADING-188 weekly ops decision update artifact。"""
    payload = validate_weekly_ops_decision_update_artifact(
        decision_update_id=decision_update_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("policy_change_allowed=false")
    typer.echo("production_effect=none")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)
