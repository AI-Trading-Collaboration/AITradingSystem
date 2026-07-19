from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Annotated, Any

import typer

from ai_trading_system.etf_portfolio import (
    dynamic_v3_filtered_candidate_readiness as filtered_readiness,
)
from ai_trading_system.etf_portfolio import (
    dynamic_v3_normal_paper_shadow_resumption_gate as normal_shadow_resumption_gate,
)
from ai_trading_system.etf_portfolio import dynamic_v3_paper_shadow_daily as paper_shadow_daily
from ai_trading_system.etf_portfolio import dynamic_v3_paper_shadow_drift as paper_shadow_drift
from ai_trading_system.etf_portfolio import dynamic_v3_paper_shadow_health as paper_shadow_health
from ai_trading_system.etf_portfolio import dynamic_v3_paper_shadow_weekly as paper_shadow_weekly
from ai_trading_system.etf_portfolio import (
    dynamic_v3_readiness_health_recovery as readiness_health_recovery,
)
from ai_trading_system.etf_portfolio import (
    dynamic_v3_signal_input_completeness as signal_input_completeness,
)
from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target
from ai_trading_system.interfaces.cli.etf_portfolio.common import mapping_obj as _mapping_obj
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_evidence_staleness_monitor_app,
    dynamic_v3_normal_paper_shadow_resumption_gate_app,
    dynamic_v3_paper_shadow_health_app,
    dynamic_v3_readiness_health_recovery_app,
    dynamic_v3_rescue_app,
    dynamic_v3_shadow_continuation_readiness_app,
)


def _texts(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value] if value else []
    if isinstance(value, list | tuple | set):
        return [str(item) for item in value if item not in (None, "")]
    return [str(value)] if value != "" else []


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


@dynamic_v3_evidence_staleness_monitor_app.command("run")
def dynamic_v3_evidence_staleness_monitor_run_command(
    as_of: Annotated[
        str | None,
        typer.Option(
            "--as-of",
            "--date",
            help="freshness as-of date YYYY-MM-DD；省略时使用当前 UTC 日期。",
        ),
    ] = None,
    candidate: Annotated[
        str,
        typer.Option("--candidate", help="filtered candidate id。"),
    ] = filtered_readiness.TOP_FILTERED_CANDIDATE,
    evidence_id: Annotated[
        str | None,
        typer.Option("--evidence-id", help="filtered candidate evidence id；缺省读取 latest。"),
    ] = None,
    stress_backfill_id: Annotated[
        str | None,
        typer.Option("--stress-backfill-id", help="stress backfill id；缺省读取 latest。"),
    ] = None,
    ab_review_id: Annotated[
        str | None,
        typer.Option("--ab-review-id", help="filtered candidate A/B review id；缺省读取 latest。"),
    ] = None,
    owner_review_id: Annotated[
        str | None,
        typer.Option("--owner-review-id", help="owner filtered review id；缺省读取 latest。"),
    ] = None,
    paper_shadow_daily_id: Annotated[
        str | None,
        typer.Option(
            "--paper-shadow-daily-id",
            "--daily-observation-id",
            help="paper-shadow daily observation id；缺省读取 latest。",
        ),
    ] = None,
    paper_shadow_drift_monitor_id: Annotated[
        str | None,
        typer.Option(
            "--paper-shadow-drift-monitor-id",
            "--drift-monitor-id",
            help="paper-shadow drift monitor id；缺省读取 latest。",
        ),
    ] = None,
    paper_shadow_weekly_review_id: Annotated[
        str | None,
        typer.Option(
            "--paper-shadow-weekly-review-id",
            "--weekly-review-id",
            help="paper-shadow weekly review id；缺省读取 latest。",
        ),
    ] = None,
    signal_input_completeness_id: Annotated[
        str | None,
        typer.Option(
            "--signal-input-completeness-id",
            "--signal-input-monitor-id",
            help="signal input completeness monitor id；缺省读取 latest。",
        ),
    ] = None,
    signal_input_completeness_report_path: Annotated[
        Path | None,
        typer.Option(
            "--signal-input-completeness-report-path",
            help="显式 signal input completeness report JSON。",
        ),
    ] = None,
    policy_path: Annotated[
        Path,
        typer.Option("--policy-path", help="evidence staleness policy YAML。"),
    ] = filtered_readiness.DEFAULT_EVIDENCE_STALENESS_POLICY_PATH,
    price_cache_path: Annotated[
        Path,
        typer.Option("--price-cache-path", help="standardized price cache CSV。"),
    ] = system_target.DEFAULT_PRICE_CACHE_PATH,
    market_panel_dir: Annotated[
        Path,
        typer.Option("--market-panel-dir", help="market panel report directory。"),
    ] = filtered_readiness.DEFAULT_MARKET_PANEL_REPORT_DIR,
    fallback_policy_report_path: Annotated[
        Path | None,
        typer.Option("--fallback-policy-report-path", help="显式 fallback policy report JSON。"),
    ] = None,
    fallback_policy_output_dir: Annotated[
        Path,
        typer.Option("--fallback-policy-output-dir", help="fallback policy artifact root。"),
    ] = filtered_readiness.DEFAULT_DATA_SOURCE_FALLBACK_DIR,
    cache_catalog_report_path: Annotated[
        Path | None,
        typer.Option("--cache-catalog-report-path", help="显式 cache catalog report JSON。"),
    ] = None,
    cache_catalog_output_dir: Annotated[
        Path,
        typer.Option("--cache-catalog-output-dir", help="cache catalog artifact root。"),
    ] = filtered_readiness.DEFAULT_CACHE_CATALOG_DIR,
    paper_shadow_daily_dir: Annotated[
        Path,
        typer.Option("--paper-shadow-daily-dir", help="paper-shadow daily artifact root。"),
    ] = filtered_readiness.DEFAULT_PAPER_SHADOW_DAILY_DIR,
    paper_shadow_drift_monitor_dir: Annotated[
        Path,
        typer.Option(
            "--paper-shadow-drift-monitor-dir",
            help="paper-shadow drift monitor artifact root。",
        ),
    ] = filtered_readiness.DEFAULT_PAPER_SHADOW_DRIFT_MONITOR_DIR,
    paper_shadow_weekly_review_dir: Annotated[
        Path,
        typer.Option(
            "--paper-shadow-weekly-review-dir",
            help="paper-shadow weekly review artifact root。",
        ),
    ] = filtered_readiness.DEFAULT_PAPER_SHADOW_WEEKLY_REVIEW_DIR,
    signal_input_completeness_dir: Annotated[
        Path,
        typer.Option(
            "--signal-input-completeness-dir",
            help="signal input completeness artifact root。",
        ),
    ] = signal_input_completeness.DEFAULT_SIGNAL_INPUT_COMPLETENESS_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="evidence staleness monitor artifact root。"),
    ] = filtered_readiness.DEFAULT_EVIDENCE_STALENESS_MONITOR_DIR,
) -> None:
    result = filtered_readiness.run_evidence_staleness_monitor(
        as_of=None if as_of is None else _parse_dynamic_v3_outcome_date(as_of, "--as-of"),
        candidate=candidate,
        evidence_id=evidence_id,
        stress_backfill_id=stress_backfill_id,
        ab_review_id=ab_review_id,
        owner_review_id=owner_review_id,
        paper_shadow_daily_id=paper_shadow_daily_id,
        paper_shadow_drift_monitor_id=paper_shadow_drift_monitor_id,
        paper_shadow_weekly_review_id=paper_shadow_weekly_review_id,
        signal_input_completeness_id=signal_input_completeness_id,
        signal_input_completeness_report_path=signal_input_completeness_report_path,
        policy_path=policy_path,
        price_cache_path=price_cache_path,
        market_panel_dir=market_panel_dir,
        fallback_policy_report_path=fallback_policy_report_path,
        fallback_policy_output_dir=fallback_policy_output_dir,
        cache_catalog_report_path=cache_catalog_report_path,
        cache_catalog_output_dir=cache_catalog_output_dir,
        paper_shadow_daily_dir=paper_shadow_daily_dir,
        paper_shadow_drift_monitor_dir=paper_shadow_drift_monitor_dir,
        paper_shadow_weekly_review_dir=paper_shadow_weekly_review_dir,
        signal_input_completeness_dir=signal_input_completeness_dir,
        output_dir=output_dir,
    )
    report = _mapping_obj(result.get("evidence_staleness_report"))
    validation = _mapping_obj(result.get("evidence_staleness_validation"))
    typer.echo(f"monitor_id={result['monitor_id']}")
    typer.echo(f"evidence_freshness_status={report.get('evidence_freshness_status')}")
    typer.echo(f"stale_artifacts={','.join(_texts(report.get('stale_artifacts')))}")
    typer.echo(f"blocking_artifacts={','.join(_texts(report.get('blocking_artifacts')))}")
    typer.echo(f"missing_artifacts={','.join(_texts(report.get('missing_artifacts')))}")
    typer.echo(f"requested_as_of={report.get('requested_as_of')}")
    typer.echo(f"freshness_reference_date={report.get('freshness_reference_date')}")
    typer.echo(f"latest_complete_market_date={report.get('latest_complete_market_date')}")
    typer.echo(f"market_calendar_status={report.get('market_calendar_status')}")
    typer.echo(f"fallback_status={report.get('fallback_status')}")
    typer.echo(f"fallback_used_count={report.get('fallback_used_count')}")
    typer.echo(f"fallback_blocking_data_types={report.get('fallback_blocking_data_types')}")
    typer.echo(f"cache_integrity_status={report.get('cache_integrity_status')}")
    typer.echo(
        f"cache_blocking_entry_ids={','.join(_texts(report.get('cache_blocking_entry_ids')))}"
    )
    typer.echo(f"cache_checksum_mismatch_count={report.get('cache_checksum_mismatch_count')}")
    typer.echo(f"signal_input_status={report.get('signal_input_status')}")
    typer.echo(f"signal_input_blocking_count={report.get('signal_input_blocking_count')}")
    typer.echo(f"signal_input_warning_count={report.get('signal_input_warning_count')}")
    typer.echo(f"coverage_status={report.get('coverage_status')}")
    typer.echo(
        "weekly_review_coverage_classification="
        f"{report.get('weekly_review_coverage_classification')}"
    )
    typer.echo(
        "weekly_review_coverage_safe_for_continuation="
        f"{report.get('weekly_review_coverage_safe_for_continuation')}"
    )
    typer.echo(f"next_refresh_action={report.get('next_refresh_action')}")
    typer.echo(f"safe_to_continue_shadow={report.get('safe_to_continue_shadow')}")
    typer.echo(f"safety_boundary_status={report.get('safety_boundary_status')}")
    typer.echo(f"validation_status={validation.get('status')}")
    typer.echo("evidence_staleness_monitor_only=true")
    typer.echo("data_downloaded_by_monitor=false")
    typer.echo("pipelines_executed_by_monitor=false")
    typer.echo("broker_action_allowed=false")
    typer.echo("production_effect=none")


@dynamic_v3_evidence_staleness_monitor_app.command("report")
def dynamic_v3_evidence_staleness_monitor_report_command(
    latest: Annotated[bool, typer.Option("--latest/--no-latest", help="读取 latest。")] = False,
    monitor_id: Annotated[
        str | None,
        typer.Option("--monitor-id", help="monitor id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="evidence staleness monitor artifact root。"),
    ] = filtered_readiness.DEFAULT_EVIDENCE_STALENESS_MONITOR_DIR,
) -> None:
    payload = filtered_readiness.evidence_staleness_monitor_report_payload(
        monitor_id=monitor_id,
        latest=latest,
        output_dir=output_dir,
    )
    report = _mapping_obj(payload.get("evidence_staleness_report"))
    typer.echo(f"monitor_id={payload['monitor_id']}")
    typer.echo(f"evidence_freshness_status={report.get('evidence_freshness_status')}")
    typer.echo(f"stale_artifacts={','.join(_texts(report.get('stale_artifacts')))}")
    typer.echo(f"blocking_artifacts={','.join(_texts(report.get('blocking_artifacts')))}")
    typer.echo(f"missing_artifacts={','.join(_texts(report.get('missing_artifacts')))}")
    typer.echo(f"requested_as_of={report.get('requested_as_of')}")
    typer.echo(f"freshness_reference_date={report.get('freshness_reference_date')}")
    typer.echo(f"latest_complete_market_date={report.get('latest_complete_market_date')}")
    typer.echo(f"market_calendar_status={report.get('market_calendar_status')}")
    typer.echo(f"fallback_status={report.get('fallback_status')}")
    typer.echo(f"fallback_used_count={report.get('fallback_used_count')}")
    typer.echo(f"fallback_blocking_data_types={report.get('fallback_blocking_data_types')}")
    typer.echo(f"cache_integrity_status={report.get('cache_integrity_status')}")
    typer.echo(
        f"cache_blocking_entry_ids={','.join(_texts(report.get('cache_blocking_entry_ids')))}"
    )
    typer.echo(f"cache_checksum_mismatch_count={report.get('cache_checksum_mismatch_count')}")
    typer.echo(f"signal_input_status={report.get('signal_input_status')}")
    typer.echo(f"signal_input_blocking_count={report.get('signal_input_blocking_count')}")
    typer.echo(f"signal_input_warning_count={report.get('signal_input_warning_count')}")
    typer.echo(f"coverage_status={report.get('coverage_status')}")
    typer.echo(
        "weekly_review_coverage_classification="
        f"{report.get('weekly_review_coverage_classification')}"
    )
    typer.echo(
        "weekly_review_coverage_safe_for_continuation="
        f"{report.get('weekly_review_coverage_safe_for_continuation')}"
    )
    typer.echo(f"next_refresh_action={report.get('next_refresh_action')}")
    typer.echo(f"safe_to_continue_shadow={report.get('safe_to_continue_shadow')}")
    typer.echo(f"safety_boundary_status={report.get('safety_boundary_status')}")
    typer.echo(f"report_path={payload['evidence_staleness_markdown_path']}")
    typer.echo("production_effect=none")


@dynamic_v3_rescue_app.command("validate-evidence-staleness-monitor")
def dynamic_v3_validate_evidence_staleness_monitor_command(
    monitor_id: Annotated[str, typer.Option("--monitor-id", help="monitor id。")],
    policy_path: Annotated[
        Path,
        typer.Option("--policy-path", help="evidence staleness policy YAML。"),
    ] = filtered_readiness.DEFAULT_EVIDENCE_STALENESS_POLICY_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="evidence staleness monitor artifact root。"),
    ] = filtered_readiness.DEFAULT_EVIDENCE_STALENESS_MONITOR_DIR,
) -> None:
    _echo_validation_payload(
        filtered_readiness.validate_evidence_staleness_monitor_artifact(
            monitor_id=monitor_id,
            output_dir=output_dir,
            policy_path=policy_path,
        )
    )


@dynamic_v3_shadow_continuation_readiness_app.command("run")
def dynamic_v3_shadow_continuation_readiness_run_command(
    as_of: Annotated[
        str | None,
        typer.Option(
            "--as-of",
            "--date",
            help="readiness as-of date YYYY-MM-DD；省略时使用当前 UTC 日期。",
        ),
    ] = None,
    candidate: Annotated[
        str,
        typer.Option("--candidate", help="filtered candidate id。"),
    ] = filtered_readiness.TOP_FILTERED_CANDIDATE,
    paper_shadow_daily_id: Annotated[
        str | None,
        typer.Option(
            "--paper-shadow-daily-id",
            "--daily-observation-id",
            help="paper-shadow daily observation id；缺省读取 latest。",
        ),
    ] = None,
    paper_shadow_drift_monitor_id: Annotated[
        str | None,
        typer.Option(
            "--paper-shadow-drift-monitor-id",
            "--drift-monitor-id",
            help="paper-shadow drift monitor id；缺省读取 latest。",
        ),
    ] = None,
    paper_shadow_weekly_review_id: Annotated[
        str | None,
        typer.Option(
            "--paper-shadow-weekly-review-id",
            "--weekly-review-id",
            help="paper-shadow weekly review id；缺省读取 latest。",
        ),
    ] = None,
    evidence_staleness_monitor_id: Annotated[
        str | None,
        typer.Option(
            "--evidence-staleness-monitor-id",
            "--staleness-monitor-id",
            help="evidence staleness monitor id；缺省读取 latest。",
        ),
    ] = None,
    signal_input_completeness_id: Annotated[
        str | None,
        typer.Option(
            "--signal-input-completeness-id",
            "--signal-input-monitor-id",
            help="signal input completeness monitor id；缺省读取 latest。",
        ),
    ] = None,
    signal_input_completeness_report_path: Annotated[
        Path | None,
        typer.Option(
            "--signal-input-completeness-report-path",
            help="显式 signal input completeness report JSON。",
        ),
    ] = None,
    data_quality_report_path: Annotated[
        Path | None,
        typer.Option("--data-quality-report-path", help="data quality report path。"),
    ] = None,
    data_quality_report_dir: Annotated[
        Path,
        typer.Option("--data-quality-report-dir", help="data quality report directory。"),
    ] = filtered_readiness.DEFAULT_MARKET_PANEL_REPORT_DIR,
    fallback_policy_report_path: Annotated[
        Path | None,
        typer.Option("--fallback-policy-report-path", help="显式 fallback policy report JSON。"),
    ] = None,
    fallback_policy_output_dir: Annotated[
        Path,
        typer.Option("--fallback-policy-output-dir", help="fallback policy artifact root。"),
    ] = filtered_readiness.DEFAULT_DATA_SOURCE_FALLBACK_DIR,
    cache_catalog_report_path: Annotated[
        Path | None,
        typer.Option("--cache-catalog-report-path", help="显式 cache catalog report JSON。"),
    ] = None,
    cache_catalog_output_dir: Annotated[
        Path,
        typer.Option("--cache-catalog-output-dir", help="cache catalog artifact root。"),
    ] = filtered_readiness.DEFAULT_CACHE_CATALOG_DIR,
    paper_shadow_daily_dir: Annotated[
        Path,
        typer.Option("--paper-shadow-daily-dir", help="paper-shadow daily artifact root。"),
    ] = filtered_readiness.DEFAULT_PAPER_SHADOW_DAILY_DIR,
    paper_shadow_drift_monitor_dir: Annotated[
        Path,
        typer.Option(
            "--paper-shadow-drift-monitor-dir",
            help="paper-shadow drift monitor artifact root。",
        ),
    ] = filtered_readiness.DEFAULT_PAPER_SHADOW_DRIFT_MONITOR_DIR,
    paper_shadow_weekly_review_dir: Annotated[
        Path,
        typer.Option(
            "--paper-shadow-weekly-review-dir",
            help="paper-shadow weekly review artifact root。",
        ),
    ] = filtered_readiness.DEFAULT_PAPER_SHADOW_WEEKLY_REVIEW_DIR,
    evidence_staleness_monitor_dir: Annotated[
        Path,
        typer.Option(
            "--evidence-staleness-monitor-dir",
            help="evidence staleness monitor artifact root。",
        ),
    ] = filtered_readiness.DEFAULT_EVIDENCE_STALENESS_MONITOR_DIR,
    signal_input_completeness_dir: Annotated[
        Path,
        typer.Option(
            "--signal-input-completeness-dir",
            help="signal input completeness artifact root。",
        ),
    ] = signal_input_completeness.DEFAULT_SIGNAL_INPUT_COMPLETENESS_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="shadow continuation readiness artifact root。"),
    ] = filtered_readiness.DEFAULT_SHADOW_CONTINUATION_READINESS_DIR,
) -> None:
    result = filtered_readiness.run_shadow_continuation_readiness_report(
        as_of=None if as_of is None else _parse_dynamic_v3_outcome_date(as_of, "--as-of"),
        candidate=candidate,
        paper_shadow_daily_id=paper_shadow_daily_id,
        paper_shadow_drift_monitor_id=paper_shadow_drift_monitor_id,
        paper_shadow_weekly_review_id=paper_shadow_weekly_review_id,
        evidence_staleness_monitor_id=evidence_staleness_monitor_id,
        signal_input_completeness_id=signal_input_completeness_id,
        signal_input_completeness_report_path=signal_input_completeness_report_path,
        data_quality_report_path=data_quality_report_path,
        data_quality_report_dir=data_quality_report_dir,
        fallback_policy_report_path=fallback_policy_report_path,
        fallback_policy_output_dir=fallback_policy_output_dir,
        cache_catalog_report_path=cache_catalog_report_path,
        cache_catalog_output_dir=cache_catalog_output_dir,
        paper_shadow_daily_dir=paper_shadow_daily_dir,
        paper_shadow_drift_monitor_dir=paper_shadow_drift_monitor_dir,
        paper_shadow_weekly_review_dir=paper_shadow_weekly_review_dir,
        evidence_staleness_monitor_dir=evidence_staleness_monitor_dir,
        signal_input_completeness_dir=signal_input_completeness_dir,
        output_dir=output_dir,
    )
    report = _mapping_obj(result.get("shadow_continuation_readiness_report"))
    validation = _mapping_obj(result.get("shadow_continuation_readiness_validation"))
    typer.echo(f"readiness_id={result['readiness_id']}")
    typer.echo(f"shadow_continuation_readiness={report.get('shadow_continuation_readiness')}")
    typer.echo(f"safe_to_continue_shadow={report.get('safe_to_continue_shadow')}")
    typer.echo(f"missing_artifacts={','.join(_texts(report.get('missing_artifacts')))}")
    typer.echo(f"blocking_artifacts={','.join(_texts(report.get('blocking_artifacts')))}")
    typer.echo(f"stale_artifacts={','.join(_texts(report.get('stale_artifacts')))}")
    typer.echo(f"coverage_status={report.get('coverage_status')}")
    typer.echo(f"fallback_status={report.get('fallback_status')}")
    typer.echo(f"fallback_used_count={report.get('fallback_used_count')}")
    typer.echo(f"fallback_blocking_data_types={report.get('fallback_blocking_data_types')}")
    typer.echo(f"cache_integrity_status={report.get('cache_integrity_status')}")
    typer.echo(
        f"cache_blocking_entry_ids={','.join(_texts(report.get('cache_blocking_entry_ids')))}"
    )
    typer.echo(f"cache_checksum_mismatch_count={report.get('cache_checksum_mismatch_count')}")
    typer.echo(f"signal_input_status={report.get('signal_input_status')}")
    typer.echo(f"signal_input_blocking_count={report.get('signal_input_blocking_count')}")
    typer.echo(f"signal_input_warning_count={report.get('signal_input_warning_count')}")
    typer.echo(f"manual_review_required={report.get('manual_review_required')}")
    typer.echo(f"next_required_action={report.get('next_required_action')}")
    typer.echo(f"data_validation_status={report.get('data_validation_status')}")
    typer.echo(f"safety_boundary_status={report.get('safety_boundary_status')}")
    typer.echo(f"validation_status={validation.get('status')}")
    typer.echo("shadow_continuation_readiness_only=true")
    typer.echo("advisory_only=true")
    typer.echo("not_official_target_weights=true")
    typer.echo("broker_action_allowed=false")
    typer.echo("production_effect=none")


@dynamic_v3_shadow_continuation_readiness_app.command("report")
def dynamic_v3_shadow_continuation_readiness_report_command(
    latest: Annotated[bool, typer.Option("--latest/--no-latest", help="读取 latest。")] = False,
    readiness_id: Annotated[
        str | None,
        typer.Option("--readiness-id", help="shadow continuation readiness id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="shadow continuation readiness artifact root。"),
    ] = filtered_readiness.DEFAULT_SHADOW_CONTINUATION_READINESS_DIR,
) -> None:
    if not latest and not readiness_id:
        raise typer.BadParameter("--readiness-id or --latest is required")
    payload = filtered_readiness.shadow_continuation_readiness_report_payload(
        readiness_id=readiness_id,
        latest=latest,
        output_dir=output_dir,
    )
    report = _mapping_obj(payload.get("shadow_continuation_readiness_report"))
    validation = _mapping_obj(payload.get("shadow_continuation_readiness_validation"))
    typer.echo(f"readiness_id={payload['readiness_id']}")
    typer.echo(f"shadow_continuation_readiness={report.get('shadow_continuation_readiness')}")
    typer.echo(f"safe_to_continue_shadow={report.get('safe_to_continue_shadow')}")
    typer.echo(f"missing_artifacts={','.join(_texts(report.get('missing_artifacts')))}")
    typer.echo(f"blocking_artifacts={','.join(_texts(report.get('blocking_artifacts')))}")
    typer.echo(f"stale_artifacts={','.join(_texts(report.get('stale_artifacts')))}")
    typer.echo(f"coverage_status={report.get('coverage_status')}")
    typer.echo(f"fallback_status={report.get('fallback_status')}")
    typer.echo(f"fallback_used_count={report.get('fallback_used_count')}")
    typer.echo(f"fallback_blocking_data_types={report.get('fallback_blocking_data_types')}")
    typer.echo(f"cache_integrity_status={report.get('cache_integrity_status')}")
    typer.echo(
        f"cache_blocking_entry_ids={','.join(_texts(report.get('cache_blocking_entry_ids')))}"
    )
    typer.echo(f"cache_checksum_mismatch_count={report.get('cache_checksum_mismatch_count')}")
    typer.echo(f"signal_input_status={report.get('signal_input_status')}")
    typer.echo(f"signal_input_blocking_count={report.get('signal_input_blocking_count')}")
    typer.echo(f"signal_input_warning_count={report.get('signal_input_warning_count')}")
    typer.echo(f"manual_review_required={report.get('manual_review_required')}")
    typer.echo(f"next_required_action={report.get('next_required_action')}")
    typer.echo(f"data_validation_status={report.get('data_validation_status')}")
    typer.echo(f"safety_boundary_status={report.get('safety_boundary_status')}")
    typer.echo(f"validation_status={validation.get('status', 'NOT_RUN')}")
    typer.echo(f"report_path={payload['shadow_continuation_readiness_markdown_path']}")
    typer.echo("production_effect=none")


@dynamic_v3_rescue_app.command("validate-shadow-continuation-readiness")
def dynamic_v3_validate_shadow_continuation_readiness_command(
    readiness_id: Annotated[
        str | None,
        typer.Option("--readiness-id", help="shadow continuation readiness id。"),
    ] = None,
    latest: Annotated[bool, typer.Option("--latest/--no-latest", help="读取 latest。")] = False,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="shadow continuation readiness artifact root。"),
    ] = filtered_readiness.DEFAULT_SHADOW_CONTINUATION_READINESS_DIR,
) -> None:
    resolved_id = readiness_id
    if latest:
        payload = filtered_readiness.shadow_continuation_readiness_report_payload(
            latest=True,
            output_dir=output_dir,
        )
        resolved_id = str(payload.get("readiness_id") or "")
    if not resolved_id:
        raise typer.BadParameter("--readiness-id or --latest is required")
    _echo_validation_payload(
        filtered_readiness.validate_shadow_continuation_readiness_artifact(
            readiness_id=resolved_id,
            output_dir=output_dir,
        )
    )


def _echo_paper_shadow_health_summary(
    *,
    manifest: Mapping[str, Any],
    report: Mapping[str, Any],
    validation: Mapping[str, Any],
) -> None:
    typer.echo(f"health_id={report.get('health_id') or manifest.get('health_id')}")
    typer.echo(f"paper_shadow_health_status={report.get('paper_shadow_health_status')}")
    typer.echo(f"safe_to_continue_shadow={report.get('safe_to_continue_shadow')}")
    typer.echo(f"data_freshness_status={report.get('data_freshness_status')}")
    typer.echo(f"signal_input_status={report.get('signal_input_status')}")
    typer.echo(f"fallback_status={report.get('fallback_status')}")
    typer.echo(f"cache_integrity_status={report.get('cache_integrity_status')}")
    typer.echo(f"weekly_review_coverage_status={report.get('weekly_review_coverage_status')}")
    typer.echo(f"drift_status={report.get('drift_status')}")
    typer.echo(f"readiness_status={report.get('readiness_status')}")
    typer.echo(f"data_refresh_audit_status={report.get('data_refresh_audit_status')}")
    typer.echo(f"blocking_reasons={','.join(_texts(report.get('blocking_reasons')))}")
    typer.echo(f"warnings={','.join(_texts(report.get('warnings')))}")
    typer.echo(f"next_required_action={report.get('next_required_action')}")
    typer.echo(f"validation_status={validation.get('status', 'NOT_RUN')}")
    typer.echo(f"report_path={manifest.get('paper_shadow_health_markdown_path')}")
    typer.echo("paper_shadow_health_check_only=true")
    typer.echo("read_only_health_aggregation=true")
    typer.echo("data_downloaded_by_health_check=false")
    typer.echo("pipelines_executed_by_health_check=false")
    typer.echo("not_official_target_weights=true")
    typer.echo("broker_action_allowed=false")
    typer.echo("paper_account_state_mutated=false")
    typer.echo("production_effect=none")


@dynamic_v3_paper_shadow_health_app.command("run")
def dynamic_v3_paper_shadow_health_run_command(
    as_of: Annotated[
        str | None,
        typer.Option(
            "--as-of",
            "--date",
            help="paper-shadow health as-of date YYYY-MM-DD；省略时使用当前 UTC 日期。",
        ),
    ] = None,
    price_cache_path: Annotated[
        Path,
        typer.Option("--price-cache-path", help="standardized price cache CSV。"),
    ] = system_target.DEFAULT_PRICE_CACHE_PATH,
    market_panel_dir: Annotated[
        Path,
        typer.Option("--market-panel-dir", help="market panel report directory。"),
    ] = paper_shadow_health.DEFAULT_MARKET_PANEL_REPORT_DIR,
    signal_input_completeness_id: Annotated[
        str | None,
        typer.Option(
            "--signal-input-completeness-id",
            "--signal-input-monitor-id",
            help="signal input completeness monitor id；缺省读取 latest。",
        ),
    ] = None,
    signal_input_completeness_report_path: Annotated[
        Path | None,
        typer.Option(
            "--signal-input-completeness-report-path",
            help="显式 signal input completeness report JSON。",
        ),
    ] = None,
    paper_shadow_daily_id: Annotated[
        str | None,
        typer.Option(
            "--paper-shadow-daily-id",
            "--daily-observation-id",
            help="paper-shadow daily observation id；缺省读取 latest。",
        ),
    ] = None,
    paper_shadow_drift_monitor_id: Annotated[
        str | None,
        typer.Option(
            "--paper-shadow-drift-monitor-id",
            "--drift-monitor-id",
            help="paper-shadow drift monitor id；缺省读取 latest。",
        ),
    ] = None,
    paper_shadow_weekly_review_id: Annotated[
        str | None,
        typer.Option(
            "--paper-shadow-weekly-review-id",
            "--weekly-review-id",
            help="paper-shadow weekly review id；缺省读取 latest。",
        ),
    ] = None,
    evidence_staleness_monitor_id: Annotated[
        str | None,
        typer.Option(
            "--evidence-staleness-monitor-id",
            "--staleness-monitor-id",
            help="evidence staleness monitor id；缺省读取 latest。",
        ),
    ] = None,
    shadow_continuation_readiness_id: Annotated[
        str | None,
        typer.Option(
            "--shadow-continuation-readiness-id",
            "--readiness-id",
            help="shadow continuation readiness id；缺省读取 latest。",
        ),
    ] = None,
    fallback_policy_report_path: Annotated[
        Path | None,
        typer.Option("--fallback-policy-report-path", help="显式 fallback policy report JSON。"),
    ] = None,
    cache_catalog_report_path: Annotated[
        Path | None,
        typer.Option("--cache-catalog-report-path", help="显式 cache catalog report JSON。"),
    ] = None,
    data_refresh_audit_id: Annotated[
        str | None,
        typer.Option("--data-refresh-audit-id", help="data refresh audit id；缺省读取 latest。"),
    ] = None,
    signal_input_completeness_dir: Annotated[
        Path,
        typer.Option(
            "--signal-input-completeness-dir",
            help="signal input completeness artifact root。",
        ),
    ] = signal_input_completeness.DEFAULT_SIGNAL_INPUT_COMPLETENESS_DIR,
    paper_shadow_daily_dir: Annotated[
        Path,
        typer.Option("--paper-shadow-daily-dir", help="paper-shadow daily artifact root。"),
    ] = paper_shadow_daily.DEFAULT_PAPER_SHADOW_DAILY_DIR,
    paper_shadow_drift_monitor_dir: Annotated[
        Path,
        typer.Option(
            "--paper-shadow-drift-monitor-dir",
            help="paper-shadow drift monitor artifact root。",
        ),
    ] = paper_shadow_drift.DEFAULT_PAPER_SHADOW_DRIFT_MONITOR_DIR,
    paper_shadow_weekly_review_dir: Annotated[
        Path,
        typer.Option(
            "--paper-shadow-weekly-review-dir",
            help="paper-shadow weekly review artifact root。",
        ),
    ] = paper_shadow_weekly.DEFAULT_PAPER_SHADOW_WEEKLY_REVIEW_DIR,
    evidence_staleness_monitor_dir: Annotated[
        Path,
        typer.Option(
            "--evidence-staleness-monitor-dir",
            help="evidence staleness monitor artifact root。",
        ),
    ] = filtered_readiness.DEFAULT_EVIDENCE_STALENESS_MONITOR_DIR,
    shadow_continuation_readiness_dir: Annotated[
        Path,
        typer.Option(
            "--shadow-continuation-readiness-dir",
            help="shadow continuation readiness artifact root。",
        ),
    ] = filtered_readiness.DEFAULT_SHADOW_CONTINUATION_READINESS_DIR,
    fallback_policy_output_dir: Annotated[
        Path,
        typer.Option("--fallback-policy-output-dir", help="fallback policy artifact root。"),
    ] = paper_shadow_health.DEFAULT_DATA_SOURCE_FALLBACK_DIR,
    cache_catalog_output_dir: Annotated[
        Path,
        typer.Option("--cache-catalog-output-dir", help="cache catalog artifact root。"),
    ] = paper_shadow_health.DEFAULT_CACHE_CATALOG_DIR,
    data_refresh_audit_dir: Annotated[
        Path,
        typer.Option("--data-refresh-audit-dir", help="data refresh audit artifact root。"),
    ] = paper_shadow_health.DEFAULT_DATA_REFRESH_AUDIT_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="paper-shadow health artifact root。"),
    ] = paper_shadow_health.DEFAULT_PAPER_SHADOW_HEALTH_DIR,
) -> None:
    result = paper_shadow_health.run_paper_shadow_health_report(
        as_of=None if as_of is None else _parse_dynamic_v3_outcome_date(as_of, "--as-of"),
        price_cache_path=price_cache_path,
        market_panel_dir=market_panel_dir,
        signal_input_completeness_id=signal_input_completeness_id,
        signal_input_completeness_report_path=signal_input_completeness_report_path,
        paper_shadow_daily_id=paper_shadow_daily_id,
        paper_shadow_drift_monitor_id=paper_shadow_drift_monitor_id,
        paper_shadow_weekly_review_id=paper_shadow_weekly_review_id,
        evidence_staleness_monitor_id=evidence_staleness_monitor_id,
        shadow_continuation_readiness_id=shadow_continuation_readiness_id,
        fallback_policy_report_path=fallback_policy_report_path,
        cache_catalog_report_path=cache_catalog_report_path,
        data_refresh_audit_id=data_refresh_audit_id,
        signal_input_completeness_dir=signal_input_completeness_dir,
        paper_shadow_daily_dir=paper_shadow_daily_dir,
        paper_shadow_drift_monitor_dir=paper_shadow_drift_monitor_dir,
        paper_shadow_weekly_review_dir=paper_shadow_weekly_review_dir,
        evidence_staleness_monitor_dir=evidence_staleness_monitor_dir,
        shadow_continuation_readiness_dir=shadow_continuation_readiness_dir,
        fallback_policy_output_dir=fallback_policy_output_dir,
        cache_catalog_output_dir=cache_catalog_output_dir,
        data_refresh_audit_dir=data_refresh_audit_dir,
        output_dir=output_dir,
    )
    _echo_paper_shadow_health_summary(
        manifest=_mapping_obj(result.get("manifest")),
        report=_mapping_obj(result.get("paper_shadow_health_report")),
        validation=_mapping_obj(result.get("paper_shadow_health_validation")),
    )


@dynamic_v3_paper_shadow_health_app.command("report")
def dynamic_v3_paper_shadow_health_report_command(
    latest: Annotated[bool, typer.Option("--latest/--no-latest", help="读取 latest。")] = False,
    health_id: Annotated[
        str | None,
        typer.Option("--health-id", help="paper-shadow health artifact id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="paper-shadow health artifact root。"),
    ] = paper_shadow_health.DEFAULT_PAPER_SHADOW_HEALTH_DIR,
) -> None:
    if not latest and not health_id:
        raise typer.BadParameter("--health-id or --latest is required")
    payload = paper_shadow_health.paper_shadow_health_report_payload(
        health_id=health_id,
        latest=latest,
        output_dir=output_dir,
    )
    _echo_paper_shadow_health_summary(
        manifest=_mapping_obj(payload),
        report=_mapping_obj(payload.get("paper_shadow_health_report")),
        validation=_mapping_obj(payload.get("paper_shadow_health_validation")),
    )


@dynamic_v3_rescue_app.command("validate-paper-shadow-health")
def dynamic_v3_validate_paper_shadow_health_command(
    health_id: Annotated[
        str | None,
        typer.Option("--health-id", help="paper-shadow health artifact id。"),
    ] = None,
    latest: Annotated[bool, typer.Option("--latest/--no-latest", help="读取 latest。")] = False,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="paper-shadow health artifact root。"),
    ] = paper_shadow_health.DEFAULT_PAPER_SHADOW_HEALTH_DIR,
) -> None:
    resolved_id = health_id
    if latest:
        payload = paper_shadow_health.paper_shadow_health_report_payload(
            latest=True,
            output_dir=output_dir,
        )
        resolved_id = str(payload.get("health_id") or "")
    if not resolved_id:
        raise typer.BadParameter("--health-id or --latest is required")
    _echo_validation_payload(
        paper_shadow_health.validate_paper_shadow_health_artifact(
            health_id=resolved_id,
            output_dir=output_dir,
        )
    )


def _echo_readiness_health_recovery_summary(
    *,
    manifest: Mapping[str, Any],
    report: Mapping[str, Any],
    validation: Mapping[str, Any],
) -> None:
    statuses = _mapping_obj(report.get("source_statuses"))
    typer.echo(f"recovery_id={report.get('recovery_id') or manifest.get('recovery_id')}")
    typer.echo(f"readiness_health_recovery_status={report.get('readiness_health_recovery_status')}")
    typer.echo(f"normal_paper_shadow_may_resume={report.get('normal_paper_shadow_may_resume')}")
    typer.echo(f"hard_stop_triggered={report.get('hard_stop_triggered')}")
    typer.echo(f"manual_review_required={report.get('manual_review_required')}")
    typer.echo(f"signal_input_status={statuses.get('signal_input_status')}")
    typer.echo(f"evidence_freshness_status={statuses.get('evidence_freshness_status')}")
    typer.echo(f"shadow_continuation_readiness={statuses.get('shadow_continuation_readiness')}")
    typer.echo(f"paper_shadow_health_status={statuses.get('paper_shadow_health_status')}")
    typer.echo(f"evidence_staleness_monitor_id={report.get('evidence_staleness_monitor_id')}")
    typer.echo(f"shadow_continuation_readiness_id={report.get('shadow_continuation_readiness_id')}")
    typer.echo(f"paper_shadow_health_id={report.get('paper_shadow_health_id')}")
    typer.echo(f"blocking_reasons={','.join(_texts(report.get('blocking_reasons')))}")
    typer.echo(f"warning_reasons={','.join(_texts(report.get('warning_reasons')))}")
    typer.echo(f"next_required_action={report.get('next_required_action')}")
    typer.echo(f"validation_status={validation.get('status', 'NOT_RUN')}")
    typer.echo(f"report_path={manifest.get('readiness_health_recovery_markdown_path')}")
    typer.echo("readiness_health_recovery_chain_only=true")
    typer.echo("normal_paper_shadow_observation_gate_only=true")
    typer.echo("promotion_board_allowed=false")
    typer.echo("extended_shadow_allowed=false")
    typer.echo("not_official_target_weights=true")
    typer.echo("broker_action_allowed=false")
    typer.echo("production_effect=none")


@dynamic_v3_readiness_health_recovery_app.command("run")
def dynamic_v3_readiness_health_recovery_run_command(
    as_of: Annotated[
        str | None,
        typer.Option(
            "--as-of",
            "--date",
            help="readiness/health recovery as-of date YYYY-MM-DD；省略时使用当前 UTC 日期。",
        ),
    ] = None,
    candidate: Annotated[
        str,
        typer.Option("--candidate", help="filtered candidate id。"),
    ] = filtered_readiness.TOP_FILTERED_CANDIDATE,
    signal_input_completeness_id: Annotated[
        str | None,
        typer.Option(
            "--signal-input-completeness-id",
            "--signal-input-monitor-id",
            help="restored signal input completeness monitor id；缺省读取 latest。",
        ),
    ] = None,
    signal_input_completeness_report_path: Annotated[
        Path | None,
        typer.Option(
            "--signal-input-completeness-report-path",
            help="显式 signal input completeness report JSON。",
        ),
    ] = None,
    paper_shadow_daily_id: Annotated[
        str | None,
        typer.Option(
            "--paper-shadow-daily-id",
            "--daily-observation-id",
            help="paper-shadow daily observation id；缺省读取 latest。",
        ),
    ] = None,
    paper_shadow_drift_monitor_id: Annotated[
        str | None,
        typer.Option(
            "--paper-shadow-drift-monitor-id",
            "--drift-monitor-id",
            help="paper-shadow drift monitor id；缺省读取 latest。",
        ),
    ] = None,
    paper_shadow_weekly_review_id: Annotated[
        str | None,
        typer.Option(
            "--paper-shadow-weekly-review-id",
            "--weekly-review-id",
            help="paper-shadow weekly review id；缺省读取 latest。",
        ),
    ] = None,
    data_quality_report_path: Annotated[
        Path | None,
        typer.Option("--data-quality-report-path", help="data quality report path。"),
    ] = None,
    fallback_policy_report_path: Annotated[
        Path | None,
        typer.Option("--fallback-policy-report-path", help="显式 fallback policy report JSON。"),
    ] = None,
    cache_catalog_report_path: Annotated[
        Path | None,
        typer.Option("--cache-catalog-report-path", help="显式 cache catalog report JSON。"),
    ] = None,
    data_refresh_audit_id: Annotated[
        str | None,
        typer.Option("--data-refresh-audit-id", help="data refresh audit id；缺省读取 latest。"),
    ] = None,
    price_cache_path: Annotated[
        Path,
        typer.Option("--price-cache-path", help="standardized price cache CSV。"),
    ] = system_target.DEFAULT_PRICE_CACHE_PATH,
    market_panel_dir: Annotated[
        Path,
        typer.Option("--market-panel-dir", help="market panel report directory。"),
    ] = filtered_readiness.DEFAULT_MARKET_PANEL_REPORT_DIR,
    evidence_staleness_monitor_dir: Annotated[
        Path,
        typer.Option(
            "--evidence-staleness-monitor-dir",
            help="evidence staleness monitor artifact root。",
        ),
    ] = filtered_readiness.DEFAULT_EVIDENCE_STALENESS_MONITOR_DIR,
    shadow_continuation_readiness_dir: Annotated[
        Path,
        typer.Option(
            "--shadow-continuation-readiness-dir",
            help="shadow continuation readiness artifact root。",
        ),
    ] = filtered_readiness.DEFAULT_SHADOW_CONTINUATION_READINESS_DIR,
    paper_shadow_health_dir: Annotated[
        Path,
        typer.Option("--paper-shadow-health-dir", help="paper-shadow health artifact root。"),
    ] = paper_shadow_health.DEFAULT_PAPER_SHADOW_HEALTH_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="readiness/health recovery artifact root。"),
    ] = readiness_health_recovery.DEFAULT_READINESS_HEALTH_RECOVERY_DIR,
) -> None:
    result = readiness_health_recovery.run_readiness_health_recovery_chain(
        as_of=None if as_of is None else _parse_dynamic_v3_outcome_date(as_of, "--as-of"),
        candidate=candidate,
        signal_input_completeness_id=signal_input_completeness_id,
        signal_input_completeness_report_path=signal_input_completeness_report_path,
        paper_shadow_daily_id=paper_shadow_daily_id,
        paper_shadow_drift_monitor_id=paper_shadow_drift_monitor_id,
        paper_shadow_weekly_review_id=paper_shadow_weekly_review_id,
        data_quality_report_path=data_quality_report_path,
        fallback_policy_report_path=fallback_policy_report_path,
        cache_catalog_report_path=cache_catalog_report_path,
        data_refresh_audit_id=data_refresh_audit_id,
        price_cache_path=price_cache_path,
        market_panel_dir=market_panel_dir,
        evidence_staleness_monitor_dir=evidence_staleness_monitor_dir,
        shadow_continuation_readiness_dir=shadow_continuation_readiness_dir,
        paper_shadow_health_dir=paper_shadow_health_dir,
        output_dir=output_dir,
    )
    _echo_readiness_health_recovery_summary(
        manifest=_mapping_obj(result.get("manifest")),
        report=_mapping_obj(result.get("readiness_health_recovery_report")),
        validation=_mapping_obj(result.get("readiness_health_recovery_validation")),
    )


@dynamic_v3_readiness_health_recovery_app.command("report")
def dynamic_v3_readiness_health_recovery_report_command(
    latest: Annotated[bool, typer.Option("--latest/--no-latest", help="读取 latest。")] = False,
    recovery_id: Annotated[
        str | None,
        typer.Option("--recovery-id", help="readiness/health recovery id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="readiness/health recovery artifact root。"),
    ] = readiness_health_recovery.DEFAULT_READINESS_HEALTH_RECOVERY_DIR,
) -> None:
    if not latest and not recovery_id:
        raise typer.BadParameter("--recovery-id or --latest is required")
    payload = readiness_health_recovery.readiness_health_recovery_report_payload(
        recovery_id=recovery_id,
        latest=latest,
        output_dir=output_dir,
    )
    _echo_readiness_health_recovery_summary(
        manifest=_mapping_obj(payload),
        report=_mapping_obj(payload.get("readiness_health_recovery_report")),
        validation=_mapping_obj(payload.get("readiness_health_recovery_validation")),
    )


@dynamic_v3_rescue_app.command("validate-readiness-health-recovery")
def dynamic_v3_validate_readiness_health_recovery_command(
    recovery_id: Annotated[
        str | None,
        typer.Option("--recovery-id", help="readiness/health recovery id。"),
    ] = None,
    latest: Annotated[bool, typer.Option("--latest/--no-latest", help="读取 latest。")] = False,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="readiness/health recovery artifact root。"),
    ] = readiness_health_recovery.DEFAULT_READINESS_HEALTH_RECOVERY_DIR,
) -> None:
    resolved_id = recovery_id
    if latest:
        payload = readiness_health_recovery.readiness_health_recovery_report_payload(
            latest=True,
            output_dir=output_dir,
        )
        resolved_id = str(payload.get("recovery_id") or "")
    if not resolved_id:
        raise typer.BadParameter("--recovery-id or --latest is required")
    _echo_validation_payload(
        readiness_health_recovery.validate_readiness_health_recovery_artifact(
            recovery_id=resolved_id,
            output_dir=output_dir,
        )
    )


def _echo_normal_paper_shadow_resumption_gate_summary(
    *,
    manifest: Mapping[str, Any],
    report: Mapping[str, Any],
    validation: Mapping[str, Any],
) -> None:
    typer.echo(f"gate_id={report.get('gate_id') or manifest.get('gate_id')}")
    typer.echo(
        "normal_paper_shadow_resumption_gate_status="
        f"{report.get('normal_paper_shadow_resumption_gate_status')}"
    )
    typer.echo(f"normal_paper_shadow_may_resume={report.get('normal_paper_shadow_may_resume')}")
    typer.echo(f"owner_action={report.get('owner_action')}")
    typer.echo(f"manual_owner_review_completed={report.get('manual_owner_review_completed')}")
    typer.echo(f"readiness_health_recovery_id={report.get('readiness_health_recovery_id')}")
    typer.echo(f"readiness_health_recovery_status={report.get('readiness_health_recovery_status')}")
    typer.echo(f"blocking_reasons={','.join(_texts(report.get('blocking_reasons')))}")
    typer.echo(f"warning_reasons={','.join(_texts(report.get('warning_reasons')))}")
    typer.echo(f"next_required_action={report.get('next_required_action')}")
    typer.echo(f"validation_status={validation.get('status', 'NOT_RUN')}")
    typer.echo(f"report_path={manifest.get('normal_paper_shadow_resumption_gate_markdown_path')}")
    typer.echo("normal_paper_shadow_resumption_gate_only=true")
    typer.echo("normal_paper_shadow_observation_only=true")
    typer.echo("promotion_board_allowed=false")
    typer.echo("extended_shadow_allowed=false")
    typer.echo("live_trading_allowed=false")
    typer.echo("not_official_target_weights=true")
    typer.echo("broker_action_allowed=false")
    typer.echo("production_effect=none")


@dynamic_v3_normal_paper_shadow_resumption_gate_app.command("run")
def dynamic_v3_normal_paper_shadow_resumption_gate_run_command(
    as_of: Annotated[
        str | None,
        typer.Option(
            "--as-of",
            "--date",
            help=(
                "normal paper-shadow resumption gate as-of date YYYY-MM-DD；"
                "省略时使用当前 UTC 日期。"
            ),
        ),
    ] = None,
    candidate: Annotated[
        str,
        typer.Option("--candidate", help="filtered candidate id。"),
    ] = filtered_readiness.TOP_FILTERED_CANDIDATE,
    readiness_health_recovery_id: Annotated[
        str | None,
        typer.Option(
            "--readiness-health-recovery-id",
            "--recovery-id",
            help="readiness/health recovery id；缺省读取 latest。",
        ),
    ] = None,
    owner_action: Annotated[
        str | None,
        typer.Option(
            "--owner-action",
            help=(
                "显式 owner action；只接受 hold 或 continue_normal_shadow 作为 "
                "safe non-promotion action。"
            ),
        ),
    ] = None,
    manual_owner_review_completed: Annotated[
        bool,
        typer.Option(
            "--manual-owner-review-completed/--manual-owner-review-not-completed",
            help="显式 owner action 是否来自已完成人工复核；恢复 normal shadow 必须为 true。",
        ),
    ] = False,
    owner_decision_report_path: Annotated[
        Path | None,
        typer.Option(
            "--owner-decision-report-path",
            help="显式 owner decision audit report JSON。",
        ),
    ] = None,
    owner_decision_reports_dir: Annotated[
        Path,
        typer.Option("--owner-decision-reports-dir", help="owner decision audit reports dir。"),
    ] = normal_shadow_resumption_gate.DEFAULT_OWNER_DECISION_REPORTS_DIR,
    owner_decision_log_path: Annotated[
        Path,
        typer.Option("--owner-decision-log-path", help="append-only owner decision JSONL。"),
    ] = normal_shadow_resumption_gate.owner_log.DEFAULT_OWNER_DECISION_AUDIT_LOG_PATH,
    readiness_health_recovery_dir: Annotated[
        Path,
        typer.Option("--readiness-health-recovery-dir", help="readiness/health recovery root。"),
    ] = readiness_health_recovery.DEFAULT_READINESS_HEALTH_RECOVERY_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="normal paper-shadow resumption gate root。"),
    ] = normal_shadow_resumption_gate.DEFAULT_NORMAL_PAPER_SHADOW_RESUMPTION_GATE_DIR,
) -> None:
    result = normal_shadow_resumption_gate.run_normal_paper_shadow_resumption_gate(
        as_of=None if as_of is None else _parse_dynamic_v3_outcome_date(as_of, "--as-of"),
        candidate=candidate,
        readiness_health_recovery_id=readiness_health_recovery_id,
        readiness_health_recovery_dir=readiness_health_recovery_dir,
        owner_action=owner_action,
        manual_owner_review_completed=manual_owner_review_completed,
        owner_decision_report_path=owner_decision_report_path,
        owner_decision_reports_dir=owner_decision_reports_dir,
        owner_decision_log_path=owner_decision_log_path,
        output_dir=output_dir,
    )
    _echo_normal_paper_shadow_resumption_gate_summary(
        manifest=_mapping_obj(result.get("manifest")),
        report=_mapping_obj(result.get("normal_paper_shadow_resumption_gate_report")),
        validation=_mapping_obj(result.get("normal_paper_shadow_resumption_gate_validation")),
    )


@dynamic_v3_normal_paper_shadow_resumption_gate_app.command("report")
def dynamic_v3_normal_paper_shadow_resumption_gate_report_command(
    latest: Annotated[bool, typer.Option("--latest/--no-latest", help="读取 latest。")] = False,
    gate_id: Annotated[
        str | None,
        typer.Option("--gate-id", help="normal paper-shadow resumption gate id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="normal paper-shadow resumption gate root。"),
    ] = normal_shadow_resumption_gate.DEFAULT_NORMAL_PAPER_SHADOW_RESUMPTION_GATE_DIR,
) -> None:
    if not latest and not gate_id:
        raise typer.BadParameter("--gate-id or --latest is required")
    payload = normal_shadow_resumption_gate.normal_paper_shadow_resumption_gate_report_payload(
        gate_id=gate_id,
        latest=latest,
        output_dir=output_dir,
    )
    _echo_normal_paper_shadow_resumption_gate_summary(
        manifest=_mapping_obj(payload),
        report=_mapping_obj(payload.get("normal_paper_shadow_resumption_gate_report")),
        validation=_mapping_obj(payload.get("normal_paper_shadow_resumption_gate_validation")),
    )


@dynamic_v3_rescue_app.command("validate-normal-paper-shadow-resumption-gate")
def dynamic_v3_validate_normal_paper_shadow_resumption_gate_command(
    gate_id: Annotated[
        str | None,
        typer.Option("--gate-id", help="normal paper-shadow resumption gate id。"),
    ] = None,
    latest: Annotated[bool, typer.Option("--latest/--no-latest", help="读取 latest。")] = False,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="normal paper-shadow resumption gate root。"),
    ] = normal_shadow_resumption_gate.DEFAULT_NORMAL_PAPER_SHADOW_RESUMPTION_GATE_DIR,
) -> None:
    resolved_id = gate_id
    if latest:
        payload = normal_shadow_resumption_gate.normal_paper_shadow_resumption_gate_report_payload(
            latest=True,
            output_dir=output_dir,
        )
        resolved_id = str(payload.get("gate_id") or "")
    if not resolved_id:
        raise typer.BadParameter("--gate-id or --latest is required")
    _echo_validation_payload(
        normal_shadow_resumption_gate.validate_normal_paper_shadow_resumption_gate_artifact(
            gate_id=resolved_id,
            output_dir=output_dir,
        )
    )
