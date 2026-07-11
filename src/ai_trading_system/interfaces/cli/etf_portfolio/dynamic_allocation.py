from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio.dynamic_allocation import (
    DEFAULT_DYNAMIC_ALLOCATION_DECISION_DIR,
    DEFAULT_DYNAMIC_ALLOCATION_POLICY_CONFIG_PATH,
    DEFAULT_DYNAMIC_ALLOCATION_REGISTRY_DIR,
    DEFAULT_DYNAMIC_ALLOCATION_REPORT_DIR,
    DEFAULT_DYNAMIC_ALLOCATION_VALIDATION_DIR,
    DynamicAllocationError,
    build_dynamic_allocation_decision_record,
    build_dynamic_allocation_policy_registry,
    build_dynamic_allocation_report,
    build_dynamic_allocation_validation_report,
    latest_dynamic_allocation_report_path,
    load_dynamic_allocation_policy_config,
    write_dynamic_allocation_decision_record,
    write_dynamic_allocation_policy_registry,
    write_dynamic_allocation_report,
    write_dynamic_allocation_validation_report,
)
from ai_trading_system.interfaces.cli.etf_portfolio.common import (
    load_optional_json_payload,
    mapping_obj,
    parse_date,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import dynamic_allocation_app
from ai_trading_system.interfaces.cli.etf_portfolio.trend_calibration import (
    latest_trend_calibration_report_path,
)


def json_float_mapping_option(value: str | None, *, option_name: str) -> dict[str, float]:
    if value in (None, ""):
        return {}
    try:
        payload = json.loads(value)
    except json.JSONDecodeError as exc:
        raise typer.BadParameter(f"{option_name} must be valid JSON") from exc
    if not isinstance(payload, dict):
        raise typer.BadParameter(f"{option_name} must be a JSON object")
    result: dict[str, float] = {}
    for key, item in payload.items():
        try:
            result[str(key)] = float(item)
        except (TypeError, ValueError) as exc:
            raise typer.BadParameter(f"{option_name} value for {key!r} must be numeric") from exc
    return result


@dynamic_allocation_app.command("decide")
def dynamic_allocation_decide_command(
    decision_date: Annotated[
        str | None,
        typer.Option("--date", help="Dynamic allocation decision date YYYY-MM-DD。"),
    ] = None,
    score_profile: Annotated[
        str,
        typer.Option("--score-profile", help="Score profile from policy sample_score_profiles。"),
    ] = "neutral",
    scores_json: Annotated[
        str | None,
        typer.Option("--scores-json", help="Explicit score mapping JSON，覆盖 score-profile。"),
    ] = None,
    previous_weights_json: Annotated[
        str | None,
        typer.Option("--previous-weights-json", help="Previous weights JSON，默认 base weights。"),
    ] = None,
    previous_scores_json: Annotated[
        str | None,
        typer.Option("--previous-scores-json", help="Previous score mapping JSON。"),
    ] = None,
    days_since_last_rebalance: Annotated[
        int | None,
        typer.Option("--days-since-last-rebalance", help="Holding-period gate sample input。"),
    ] = None,
    confirmed_regime_days: Annotated[
        int | None,
        typer.Option("--confirmed-regime-days", help="Regime confirmation gate sample input。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="dynamic allocation policy config。"),
    ] = DEFAULT_DYNAMIC_ALLOCATION_POLICY_CONFIG_PATH,
    trend_report_path: Annotated[
        Path | None,
        typer.Option("--trend-report-path", help="Explicit TRADING-083 trend report JSON。"),
    ] = None,
    latest_trend_report: Annotated[
        bool,
        typer.Option(
            "--latest-trend-report/--no-latest-trend-report",
            help="没有显式 trend report 时读取 latest TRADING-083 report。",
        ),
    ] = True,
    decision_output_dir: Annotated[
        Path,
        typer.Option("--decision-output-dir", help="dynamic decision 输出目录。"),
    ] = DEFAULT_DYNAMIC_ALLOCATION_DECISION_DIR,
    report_output_dir: Annotated[
        Path,
        typer.Option("--report-output-dir", help="dynamic allocation report 输出目录。"),
    ] = DEFAULT_DYNAMIC_ALLOCATION_REPORT_DIR,
    registry_output_dir: Annotated[
        Path,
        typer.Option("--registry-output-dir", help="dynamic allocation registry 输出目录。"),
    ] = DEFAULT_DYNAMIC_ALLOCATION_REGISTRY_DIR,
) -> None:
    """生成 TRADING-084 candidate-only dynamic allocation decision。"""
    try:
        policy = load_dynamic_allocation_policy_config(config_path)
        scores = (
            json_float_mapping_option(scores_json, option_name="--scores-json")
            if scores_json
            else dict(policy.sample_score_profiles[score_profile])
        )
        previous_weights = (
            json_float_mapping_option(
                previous_weights_json,
                option_name="--previous-weights-json",
            )
            if previous_weights_json
            else None
        )
        previous_scores = (
            json_float_mapping_option(previous_scores_json, option_name="--previous-scores-json")
            if previous_scores_json
            else policy.sample_score_profiles.get("neutral")
        )
    except KeyError as exc:
        raise typer.BadParameter(f"unknown score profile: {score_profile}") from exc
    except DynamicAllocationError as exc:
        raise typer.BadParameter(str(exc)) from exc
    resolved_date = parse_date(decision_date) if decision_date else date.today()
    source_trend_report = trend_report_path
    if source_trend_report is None and latest_trend_report:
        source_trend_report = latest_trend_calibration_report_path()
    trend_payload = load_optional_json_payload(source_trend_report)
    trend_summary = mapping_obj(trend_payload.get("summary"))
    trend_coverage = mapping_obj(trend_payload.get("dataset_coverage"))
    data_quality_status = str(
        trend_summary.get(
            "data_quality_status",
            trend_coverage.get("data_quality_status", "UNKNOWN"),
        )
    )
    try:
        decision = build_dynamic_allocation_decision_record(
            policy=policy,
            decision_date=resolved_date,
            input_scores=scores,
            previous_weights=previous_weights,
            previous_scores=previous_scores,
            days_since_last_rebalance=days_since_last_rebalance,
            confirmed_regime_days=confirmed_regime_days,
            source_trend_report=str(source_trend_report or ""),
            data_quality_status=data_quality_status,
        )
        report = build_dynamic_allocation_report(
            policy=policy,
            decision_records=[decision],
            source_trend_report=str(source_trend_report or ""),
        )
        registry = build_dynamic_allocation_policy_registry(
            policy,
            latest_report_path=str(source_trend_report or ""),
        )
    except DynamicAllocationError as exc:
        raise typer.BadParameter(str(exc)) from exc
    decision_paths = write_dynamic_allocation_decision_record(
        decision,
        output_dir=decision_output_dir,
    )
    report_paths = write_dynamic_allocation_report(report, output_dir=report_output_dir)
    registry_paths = write_dynamic_allocation_policy_registry(
        registry,
        output_dir=registry_output_dir,
    )
    summary = report["summary"]
    typer.echo(f"ETF dynamic allocation decision JSON：{decision_paths['json']}")
    typer.echo(f"ETF dynamic allocation report JSON：{report_paths['json']}")
    typer.echo(f"ETF dynamic allocation policy registry JSON：{registry_paths['json']}")
    typer.echo(f"status={report['status']}")
    typer.echo(f"selected_regime={summary['selected_regime']}")
    typer.echo(f"rebalance_decision={summary['rebalance_decision']}")
    typer.echo(f"candidate_target_weights={json.dumps(summary['candidate_target_weights'])}")
    typer.echo("commands_executed=false")
    typer.echo("production_state_mutated=false")
    typer.echo("baseline_config_mutated=false")
    typer.echo("official_target_weights_mutated=false")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@dynamic_allocation_app.command("report")
def dynamic_allocation_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取最新 dynamic allocation report。"),
    ] = True,
    report_path: Annotated[
        Path | None,
        typer.Option("--report-path", help="显式 report JSON path。"),
    ] = None,
    report_dir: Annotated[
        Path,
        typer.Option("--report-dir", help="report artifact directory。"),
    ] = DEFAULT_DYNAMIC_ALLOCATION_REPORT_DIR,
) -> None:
    """只读展示 latest TRADING-084 dynamic allocation report 摘要。"""
    resolved = report_path
    if resolved is None and latest:
        resolved = latest_dynamic_allocation_report_path(report_dir)
    payload = load_optional_json_payload(resolved)
    if not payload:
        raise typer.BadParameter("dynamic allocation report not found")
    summary = payload.get("summary")
    if not isinstance(summary, dict):
        raise typer.BadParameter(f"invalid dynamic allocation report: {resolved}")
    typer.echo(f"dynamic_allocation_report={resolved}")
    typer.echo(f"status={payload.get('status')}")
    typer.echo(f"selected_regime={summary.get('selected_regime')}")
    typer.echo(f"rebalance_decision={summary.get('rebalance_decision')}")
    typer.echo(f"candidate_target_weights={json.dumps(summary.get('candidate_target_weights'))}")
    typer.echo(f"data_quality_status={summary.get('data_quality_status')}")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("official_target_weights_mutated=false")


@dynamic_allocation_app.command("validate")
def dynamic_allocation_validate_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="dynamic allocation policy config。"),
    ] = DEFAULT_DYNAMIC_ALLOCATION_POLICY_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="validation report 输出目录。"),
    ] = DEFAULT_DYNAMIC_ALLOCATION_VALIDATION_DIR,
) -> None:
    """校验 TRADING-084 dynamic allocation workflow 和 safety boundary。"""
    payload = build_dynamic_allocation_validation_report(policy_config_path=config_path)
    paths = write_dynamic_allocation_validation_report(payload, output_dir=output_dir)
    typer.echo(f"ETF dynamic allocation validation JSON：{paths['json']}")
    typer.echo(f"ETF dynamic allocation validation Markdown：{paths['markdown']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("commands_executed=false")
    typer.echo("production_state_mutated=false")
    typer.echo("baseline_config_mutated=false")
    typer.echo("official_target_weights_mutated=false")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)
