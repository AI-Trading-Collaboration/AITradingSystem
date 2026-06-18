from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from datetime import date
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from ai_trading_system.alerts import (
    default_alert_report_path,
)
from ai_trading_system.cli_commands.data_artifacts import (
    _resolve_market_data_freshness_path,
    _resolve_market_data_refresh_path,
)
from ai_trading_system.cli_commands.parameter_artifacts import (
    _resolve_shadow_backtest_summary_path,
    _resolve_weight_stability_path,
    _resolve_weight_stability_readiness_path,
    _resolve_weight_tuning_failure_path,
    _resolve_weight_tuning_path,
)
from ai_trading_system.cli_commands.portfolio_artifacts import (
    _resolve_portfolio_candidate_review_decision_path,
    _resolve_portfolio_candidate_tracking_path,
    _resolve_portfolio_candidates_path,
    _resolve_portfolio_sensitivity_path,
    _resolve_portfolio_tracking_review_path,
    _resolve_portfolio_turnover_attribution_path,
)
from ai_trading_system.config import (
    PROJECT_ROOT,
    configured_price_tickers,
    configured_rate_series,
    load_data_quality,
    load_universe,
)
from ai_trading_system.daily_task_dashboard import (
    build_daily_task_dashboard_report,
    default_daily_decision_summary_path,
    default_daily_task_dashboard_json_path,
    default_daily_task_dashboard_path,
    write_daily_decision_summary_json,
    write_daily_task_dashboard,
    write_daily_task_dashboard_json,
)
from ai_trading_system.data.quality import (
    default_quality_report_path,
    validate_data_cache,
    write_data_quality_report,
)
from ai_trading_system.decision_learning_queue import (
    DEFAULT_DECISION_LEARNING_QUEUE_PATH,
)
from ai_trading_system.decision_outcomes import (
    DEFAULT_DECISION_OUTCOMES_PATH,
)
from ai_trading_system.decision_snapshots import (
    DEFAULT_DECISION_SNAPSHOT_DIR,
    default_decision_snapshot_path,
)
from ai_trading_system.documentation_contract import default_documentation_contract_json_path
from ai_trading_system.evidence_dashboard import (
    build_evidence_dashboard_report,
    default_evidence_dashboard_json_path,
    default_evidence_dashboard_path,
    write_evidence_dashboard,
    write_evidence_dashboard_json,
)
from ai_trading_system.feedback_loop_review import (
    default_feedback_loop_review_report_path,
)
from ai_trading_system.market_feedback_optimization import (
    default_market_feedback_optimization_report_path,
)
from ai_trading_system.ops_daily import (
    default_daily_ops_run_metadata_path,
    default_daily_ops_run_report_path,
)
from ai_trading_system.order_intent_candidates import (
    default_order_intent_candidates_path,
    write_order_intent_candidates_json,
)
from ai_trading_system.periodic_investment_review import (
    DEFAULT_PERIODIC_INVESTMENT_REVIEW_REPORT_DIR,
    DEFAULT_SCORES_DAILY_PATH,
    build_periodic_investment_review_report,
    default_periodic_investment_review_report_path,
    write_periodic_investment_review_report,
)
from ai_trading_system.prediction_ledger import (
    DEFAULT_PREDICTION_OUTCOMES_PATH,
)
from ai_trading_system.report_traceability import (
    default_report_trace_bundle_path,
)
from ai_trading_system.reports import decision_stage_review as decision_stage_reports
from ai_trading_system.reports import (
    exact_blocker_warning_inventory as exact_inventory_reports,
)
from ai_trading_system.reports import executable_research_binding as executable_binding_reports
from ai_trading_system.reports import (
    executable_research_evidence_repair as evidence_repair_reports,
)
from ai_trading_system.reports import next_research_cycle as next_research_reports
from ai_trading_system.reports import (
    normal_paper_shadow_observation_clock as normal_observation_clock_reports,
)
from ai_trading_system.reports import post_recovery_governance_pack as post_recovery_reports
from ai_trading_system.reports import recovery_triage as recovery_triage_reports
from ai_trading_system.reports import (
    remaining_blocker_resolution_ledger as blocker_ledger_reports,
)
from ai_trading_system.reports import report_index_warning_cleanup as warning_cleanup_reports
from ai_trading_system.reports import return_to_research_reset as return_research_reports
from ai_trading_system.reports.artifact_lineage import (
    build_artifact_lineage_payload,
    default_artifact_lineage_json_path,
    default_artifact_lineage_markdown_path,
    default_artifact_lineage_validation_json_path,
    default_artifact_lineage_validation_markdown_path,
    validate_artifact_lineage_payload,
    write_artifact_lineage_json,
    write_artifact_lineage_markdown,
    write_artifact_lineage_validation_json,
    write_artifact_lineage_validation_markdown,
)
from ai_trading_system.reports.calculation_explainers import (
    DEFAULT_METRIC_EXPLAINERS_CONFIG_PATH,
    build_calculation_explainers_payload,
    default_calculation_explainers_path,
    write_calculation_explainers_json,
)
from ai_trading_system.reports.candidate_rejection_postmortem import (
    build_candidate_rejection_postmortem_payload,
    default_candidate_rejection_postmortem_json_path,
    default_candidate_rejection_postmortem_markdown_path,
    default_candidate_rejection_postmortem_validation_json_path,
    default_candidate_rejection_postmortem_validation_markdown_path,
    latest_candidate_rejection_postmortem_json_path,
    validate_candidate_rejection_postmortem_payload,
    write_candidate_rejection_postmortem_json,
    write_candidate_rejection_postmortem_markdown,
    write_candidate_rejection_postmortem_validation_json,
    write_candidate_rejection_postmortem_validation_markdown,
)
from ai_trading_system.reports.decision_snapshot_lifecycle_policy import (
    SNAPSHOT_AVAILABLE,
    build_decision_snapshot_lifecycle_policy_payload,
    default_decision_snapshot_lifecycle_policy_json_path,
    default_decision_snapshot_lifecycle_policy_markdown_path,
    default_decision_snapshot_lifecycle_policy_validation_json_path,
    default_decision_snapshot_lifecycle_policy_validation_markdown_path,
    latest_decision_snapshot_lifecycle_policy_json_path,
    validate_decision_snapshot_lifecycle_policy_payload,
    write_decision_snapshot_lifecycle_policy_json,
    write_decision_snapshot_lifecycle_policy_markdown,
    write_decision_snapshot_lifecycle_policy_validation_json,
    write_decision_snapshot_lifecycle_policy_validation_markdown,
)
from ai_trading_system.reports.extended_shadow_observation_clock import (
    build_extended_shadow_observation_clock_payload,
    default_extended_shadow_observation_clock_json_path,
    default_extended_shadow_observation_clock_markdown_path,
    default_extended_shadow_observation_clock_validation_json_path,
    default_extended_shadow_observation_clock_validation_markdown_path,
    latest_extended_shadow_observation_clock_json_path,
    validate_extended_shadow_observation_clock_payload,
    write_extended_shadow_observation_clock_json,
    write_extended_shadow_observation_clock_markdown,
    write_extended_shadow_observation_clock_validation_json,
    write_extended_shadow_observation_clock_validation_markdown,
)
from ai_trading_system.reports.extended_shadow_protocol import (
    build_extended_shadow_protocol_payload,
    default_extended_shadow_protocol_json_path,
    default_extended_shadow_protocol_markdown_path,
    default_extended_shadow_protocol_validation_json_path,
    default_extended_shadow_protocol_validation_markdown_path,
    latest_extended_shadow_protocol_json_path,
    validate_extended_shadow_protocol_payload,
    write_extended_shadow_protocol_json,
    write_extended_shadow_protocol_markdown,
    write_extended_shadow_protocol_validation_json,
    write_extended_shadow_protocol_validation_markdown,
)
from ai_trading_system.reports.market_panel import (
    build_market_panel_payload,
    default_market_panel_json_path,
    default_market_panel_report_path,
    write_market_panel_json,
    write_market_panel_report,
)
from ai_trading_system.reports.owner_decision_audit_log import (
    DEFAULT_OWNER_DECISION_AUDIT_LOG_PATH,
    OwnerDecisionAuditLogError,
    append_owner_decision_record,
    build_owner_decision_audit_log_payload,
    default_owner_decision_audit_log_json_path,
    default_owner_decision_audit_log_markdown_path,
    default_owner_decision_audit_log_validation_json_path,
    default_owner_decision_audit_log_validation_markdown_path,
    latest_owner_decision_audit_log_json_path,
    validate_owner_decision_audit_log_payload,
    write_owner_decision_audit_log_json,
    write_owner_decision_audit_log_markdown,
    write_owner_decision_audit_log_validation_json,
    write_owner_decision_audit_log_validation_markdown,
)
from ai_trading_system.reports.owner_review_template_v2 import (
    build_owner_review_template_v2_payload,
    default_owner_review_template_v2_json_path,
    default_owner_review_template_v2_markdown_path,
    default_owner_review_template_v2_validation_json_path,
    default_owner_review_template_v2_validation_markdown_path,
    latest_owner_review_template_v2_json_path,
    validate_owner_review_template_v2_payload,
    write_owner_review_template_v2_json,
    write_owner_review_template_v2_markdown,
    write_owner_review_template_v2_validation_json,
    write_owner_review_template_v2_validation_markdown,
)
from ai_trading_system.reports.paper_shadow_promotion_board import (
    build_paper_shadow_promotion_board_payload,
    default_paper_shadow_promotion_board_json_path,
    default_paper_shadow_promotion_board_markdown_path,
    default_paper_shadow_promotion_board_validation_json_path,
    default_paper_shadow_promotion_board_validation_markdown_path,
    latest_paper_shadow_promotion_board_json_path,
    validate_paper_shadow_promotion_board_payload,
    write_paper_shadow_promotion_board_json,
    write_paper_shadow_promotion_board_markdown,
    write_paper_shadow_promotion_board_validation_json,
    write_paper_shadow_promotion_board_validation_markdown,
)
from ai_trading_system.reports.production_boundary_static_scan import (
    DEFAULT_ALLOWLIST_PATH,
    build_production_boundary_static_scan_payload,
    default_production_boundary_static_scan_json_path,
    default_production_boundary_static_scan_markdown_path,
    default_production_boundary_static_scan_validation_json_path,
    default_production_boundary_static_scan_validation_markdown_path,
    latest_production_boundary_static_scan_json_path,
    validate_production_boundary_static_scan_payload,
    write_production_boundary_static_scan_json,
    write_production_boundary_static_scan_markdown,
    write_production_boundary_static_scan_validation_json,
    write_production_boundary_static_scan_validation_markdown,
)
from ai_trading_system.reports.reader_brief import (
    build_reader_brief_payload,
    build_reader_brief_quality_payload,
    default_reader_brief_html_path,
    default_reader_brief_json_path,
    default_reader_brief_quality_json_path,
    default_reader_brief_quality_markdown_path,
    write_reader_brief_html,
    write_reader_brief_json,
    write_reader_brief_quality_json,
    write_reader_brief_quality_markdown,
)
from ai_trading_system.reports.reader_brief_consistency import (
    build_reader_brief_consistency_payload,
    default_reader_brief_consistency_json_path,
    default_reader_brief_consistency_markdown_path,
    default_reader_brief_consistency_validation_json_path,
    default_reader_brief_consistency_validation_markdown_path,
    latest_reader_brief_consistency_json_path,
    validate_reader_brief_consistency_payload,
    write_reader_brief_consistency_json,
    write_reader_brief_consistency_markdown,
    write_reader_brief_consistency_validation_json,
    write_reader_brief_consistency_validation_markdown,
)
from ai_trading_system.reports.recovery_evidence_pack import (
    build_recovery_evidence_pack_payload,
    default_recovery_evidence_pack_json_path,
    default_recovery_evidence_pack_markdown_path,
    default_recovery_evidence_pack_validation_json_path,
    default_recovery_evidence_pack_validation_markdown_path,
    latest_recovery_evidence_pack_json_path,
    validate_recovery_evidence_pack_payload,
    write_recovery_evidence_pack_json,
    write_recovery_evidence_pack_markdown,
    write_recovery_evidence_pack_validation_json,
    write_recovery_evidence_pack_validation_markdown,
)
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_INDEX_WAIVER_PATH,
    DEFAULT_REPORT_REGISTRY_PATH,
    build_report_index_payload,
    default_report_index_html_path,
    default_report_index_json_path,
    write_report_index_html,
    write_report_index_json,
)
from ai_trading_system.reports.report_quality_gate import (
    build_report_quality_gate_payload,
    default_report_quality_gate_json_path,
    default_report_quality_gate_markdown_path,
    write_report_quality_gate_json,
    write_report_quality_gate_markdown,
)
from ai_trading_system.reports.research_governance_end_to_end_pack import (
    build_research_governance_end_to_end_pack_payload,
    default_research_governance_end_to_end_pack_json_path,
    default_research_governance_end_to_end_pack_markdown_path,
    default_research_governance_end_to_end_pack_validation_json_path,
    default_research_governance_end_to_end_pack_validation_markdown_path,
    latest_research_governance_end_to_end_pack_json_path,
    validate_research_governance_end_to_end_pack_payload,
    write_research_governance_end_to_end_pack_json,
    write_research_governance_end_to_end_pack_markdown,
    write_research_governance_end_to_end_pack_validation_json,
    write_research_governance_end_to_end_pack_validation_markdown,
)
from ai_trading_system.reports.research_governance_recovery_pack import (
    build_research_governance_recovery_pack_payload,
    default_research_governance_recovery_pack_json_path,
    default_research_governance_recovery_pack_markdown_path,
    default_research_governance_recovery_pack_validation_json_path,
    default_research_governance_recovery_pack_validation_markdown_path,
    latest_research_governance_recovery_pack_json_path,
    validate_research_governance_recovery_pack_payload,
    write_research_governance_recovery_pack_json,
    write_research_governance_recovery_pack_markdown,
    write_research_governance_recovery_pack_validation_json,
    write_research_governance_recovery_pack_validation_markdown,
)
from ai_trading_system.reports.research_governance_summary import (
    build_research_governance_summary_payload,
    default_research_governance_summary_json_path,
    default_research_governance_summary_report_path,
    write_research_governance_summary_json,
    write_research_governance_summary_report,
)
from ai_trading_system.reports.research_monthly_review_pack import (
    build_research_monthly_review_pack_payload,
    default_research_monthly_review_pack_json_path,
    default_research_monthly_review_pack_markdown_path,
    default_research_monthly_review_pack_validation_json_path,
    default_research_monthly_review_pack_validation_markdown_path,
    latest_research_monthly_review_pack_json_path,
    validate_research_monthly_review_pack_payload,
    write_research_monthly_review_pack_json,
    write_research_monthly_review_pack_markdown,
    write_research_monthly_review_pack_validation_json,
    write_research_monthly_review_pack_validation_markdown,
)
from ai_trading_system.reports.research_roadmap_dashboard import (
    build_research_roadmap_dashboard_payload,
    default_research_roadmap_dashboard_json_path,
    default_research_roadmap_dashboard_markdown_path,
    default_research_roadmap_dashboard_validation_json_path,
    default_research_roadmap_dashboard_validation_markdown_path,
    latest_research_roadmap_dashboard_json_path,
    validate_research_roadmap_dashboard_payload,
    write_research_roadmap_dashboard_json,
    write_research_roadmap_dashboard_markdown,
    write_research_roadmap_dashboard_validation_json,
    write_research_roadmap_dashboard_validation_markdown,
)
from ai_trading_system.reports.research_safety_boundary import (
    build_research_safety_boundary_payload,
    default_research_safety_boundary_json_path,
    default_research_safety_boundary_markdown_path,
    default_research_safety_boundary_validation_json_path,
    default_research_safety_boundary_validation_markdown_path,
    latest_research_safety_boundary_json_path,
    validate_research_safety_boundary_payload,
    write_research_safety_boundary_json,
    write_research_safety_boundary_markdown,
    write_research_safety_boundary_validation_json,
    write_research_safety_boundary_validation_markdown,
)
from ai_trading_system.reports.score_change_attribution import (
    build_score_change_attribution_payload,
    default_score_change_attribution_json_path,
    default_score_change_attribution_report_path,
    write_score_change_attribution_json,
    write_score_change_attribution_report,
)
from ai_trading_system.reports.task_register_consistency import (
    build_task_register_consistency_payload,
    default_task_register_consistency_json_path,
    default_task_register_consistency_markdown_path,
    default_task_register_consistency_validation_json_path,
    default_task_register_consistency_validation_markdown_path,
    latest_task_register_consistency_json_path,
    validate_task_register_consistency_payload,
    write_task_register_consistency_json,
    write_task_register_consistency_markdown,
    write_task_register_consistency_validation_json,
    write_task_register_consistency_validation_markdown,
)
from ai_trading_system.reports.waiver_inventory import (
    build_waiver_inventory_payload,
    default_waiver_inventory_json_path,
    default_waiver_inventory_markdown_path,
    default_waiver_inventory_validation_json_path,
    default_waiver_inventory_validation_markdown_path,
    latest_waiver_inventory_json_path,
    validate_waiver_inventory_payload,
    write_waiver_inventory_json,
    write_waiver_inventory_markdown,
    write_waiver_inventory_validation_json,
    write_waiver_inventory_validation_markdown,
)
from ai_trading_system.rule_experiments import (
    DEFAULT_RULE_EXPERIMENT_LEDGER_PATH,
)
from ai_trading_system.scoring.daily import (
    default_daily_score_report_path,
)
from ai_trading_system.trading_engine.market_data_freshness import (
    load_market_data_freshness_payload,
    market_data_freshness_payload_date,
    validate_market_data_freshness_payload,
    write_market_data_freshness_report_alias,
)
from ai_trading_system.trading_engine.market_data_refresh import (
    load_market_data_refresh_payload,
    market_data_refresh_payload_date,
    validate_market_data_refresh_payload,
    write_market_data_refresh_report_alias,
)
from ai_trading_system.trading_engine.parameters.weight_stability import (
    load_weight_stability_payload,
    validate_weight_stability_payload,
    weight_stability_payload_date,
    write_weight_stability_report_alias,
)
from ai_trading_system.trading_engine.parameters.weight_stability_readiness import (
    load_weight_stability_readiness_payload,
    validate_weight_stability_readiness_payload,
    weight_stability_readiness_payload_date,
    write_weight_stability_readiness_report_alias,
)
from ai_trading_system.trading_engine.parameters.weight_tuning import (
    load_weight_tuning_payload,
    validate_weight_tuning_payload,
    weight_tuning_payload_date,
    write_weight_tuning_report_alias,
)
from ai_trading_system.trading_engine.parameters.weight_tuning_failure import (
    load_weight_tuning_failure_payload,
    validate_weight_tuning_failure_payload,
    weight_tuning_failure_payload_date,
    write_weight_tuning_failure_report_alias,
)
from ai_trading_system.trading_engine.portfolio_candidate_review import (
    load_portfolio_candidate_review_payload,
    portfolio_candidate_review_payload_date,
    validate_portfolio_candidate_review_decision_payload,
    write_portfolio_candidate_review_report_alias,
)
from ai_trading_system.trading_engine.portfolio_candidate_tracking import (
    load_portfolio_candidate_tracking_payload,
    portfolio_candidate_tracking_payload_date,
    validate_portfolio_candidate_tracking_payload,
    write_portfolio_candidate_tracking_report_alias,
)
from ai_trading_system.trading_engine.portfolio_candidates import (
    load_portfolio_candidates_payload,
    portfolio_candidates_payload_date,
    validate_portfolio_candidates_payload,
    write_portfolio_candidates_report_alias,
)
from ai_trading_system.trading_engine.portfolio_sensitivity import (
    load_portfolio_sensitivity_payload,
    portfolio_sensitivity_payload_date,
    validate_portfolio_sensitivity_payload,
    write_portfolio_sensitivity_report_alias,
)
from ai_trading_system.trading_engine.portfolio_tracking_review import (
    load_portfolio_tracking_review_payload,
    portfolio_tracking_review_payload_date,
    validate_portfolio_tracking_review_payload,
    write_portfolio_tracking_review_report_alias,
)
from ai_trading_system.trading_engine.portfolio_turnover_attribution import (
    load_portfolio_turnover_attribution_payload,
    portfolio_turnover_attribution_payload_date,
    validate_portfolio_turnover_attribution_payload,
    write_portfolio_turnover_attribution_report_alias,
)
from ai_trading_system.trading_engine.reports.parameter_promotion_report import (
    default_parameter_promotion_json_path,
    load_parameter_promotion_payload,
    write_parameter_promotion_report_alias,
)
from ai_trading_system.trading_engine.reports.shadow_backtest_report import (
    load_shadow_backtest_payload,
    validate_shadow_backtest_payload,
    write_shadow_backtest_report_alias,
)
from ai_trading_system.trading_engine.signal_ablation import (
    default_signal_ablation_json_path,
    default_signal_ablation_root,
    latest_signal_ablation_path,
    load_signal_ablation_payload,
    signal_ablation_payload_date,
    validate_signal_ablation_payload,
    write_signal_ablation_report_alias,
)
from ai_trading_system.trading_engine.signal_calibration import (
    default_signal_calibration_json_path,
    default_signal_calibration_root,
    latest_signal_calibration_path,
    load_signal_calibration_payload,
    signal_calibration_payload_date,
    validate_signal_calibration_payload,
    write_signal_calibration_report_alias,
)
from ai_trading_system.trading_engine.signal_snapshots import (
    default_signal_snapshot_json_path,
    default_signal_snapshot_root,
    latest_signal_snapshot_path,
    load_signal_snapshot_payload,
    signal_snapshot_summary,
    validate_signal_snapshot_payload,
    write_signal_snapshot_report_alias,
)

reports_app = typer.Typer(help="投资报告和周期复盘。", no_args_is_help=True)
task_register_consistency_app = typer.Typer(
    help="Task register consistency governance reports.",
    no_args_is_help=True,
)
reports_app.add_typer(task_register_consistency_app, name="task-register-consistency")
owner_decision_audit_log_app = typer.Typer(
    help="Append-only owner decision audit log governance reports.",
    no_args_is_help=True,
)
reports_app.add_typer(owner_decision_audit_log_app, name="owner-decision-audit-log")
console = Console()


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter("日期必须使用 YYYY-MM-DD 格式。") from exc


def _resolve_signal_snapshot_path(*, latest: bool, as_of: str | None) -> Path:
    root = default_signal_snapshot_root()
    if latest or as_of is None:
        latest_path = latest_signal_snapshot_path(root)
        if latest_path is None:
            raise typer.BadParameter(f"未找到 signal snapshot artifact：{root}")
        return latest_path
    return default_signal_snapshot_json_path(root, _parse_date(as_of))


def _resolve_signal_ablation_path(*, latest: bool, as_of: str | None) -> Path:
    root = default_signal_ablation_root()
    if latest or as_of is None:
        latest_path = latest_signal_ablation_path(root)
        if latest_path is None:
            raise typer.BadParameter(f"未找到 signal ablation artifact：{root}")
        return latest_path
    return default_signal_ablation_json_path(root, _parse_date(as_of))


def _resolve_signal_calibration_path(*, latest: bool, as_of: str | None) -> Path:
    root = default_signal_calibration_root()
    if latest or as_of is None:
        latest_path = latest_signal_calibration_path(root)
        if latest_path is None:
            raise typer.BadParameter(f"未找到 signal calibration artifact：{root}")
        return latest_path
    return default_signal_calibration_json_path(root, _parse_date(as_of))


def _resolve_parameter_promotion_path(*, latest: bool, as_of: str | None) -> Path:
    root = PROJECT_ROOT / "artifacts" / "parameter_promotion"
    if latest or as_of is None:
        candidates = sorted(root.glob("*/parameter_promotion_decision.json"))
        if not candidates:
            raise typer.BadParameter(f"未找到 parameter promotion artifact：{root}")
        return max(candidates, key=lambda path: path.stat().st_mtime)
    return default_parameter_promotion_json_path(root, _parse_date(as_of))


def _shadow_backtest_payload_date(payload: dict[str, object], source_path: Path) -> date:
    metadata = payload.get("metadata")
    if isinstance(metadata, dict):
        run_id = str(metadata.get("run_id") or "")
        raw_date = run_id.removeprefix("shadow-backtest-")
        try:
            return date.fromisoformat(raw_date)
        except ValueError:
            pass
    try:
        return date.fromisoformat(source_path.parent.name)
    except ValueError as exc:
        message = f"无法从 shadow backtest artifact 推断日期：{source_path}"
        raise typer.BadParameter(message) from exc


def _signal_snapshot_payload_date(payload: dict[str, object], source_path: Path) -> date:
    metadata = payload.get("metadata")
    if isinstance(metadata, dict):
        raw_date = str(metadata.get("as_of") or "")
        try:
            return date.fromisoformat(raw_date)
        except ValueError:
            snapshot_id = str(metadata.get("snapshot_id") or "")
            raw_date = snapshot_id.removeprefix("signal-snapshot-")
            try:
                return date.fromisoformat(raw_date)
            except ValueError:
                pass
    try:
        return date.fromisoformat(source_path.parent.name)
    except ValueError as exc:
        message = f"无法从 signal snapshot artifact 推断日期：{source_path}"
        raise typer.BadParameter(message) from exc


def _parameter_promotion_payload_date(payload: dict[str, object], source_path: Path) -> date:
    metadata = payload.get("metadata")
    if isinstance(metadata, dict):
        run_id = str(metadata.get("run_id") or "")
        raw_date = run_id.removeprefix("shadow-backtest-")
        try:
            return date.fromisoformat(raw_date)
        except ValueError:
            pass
    try:
        return date.fromisoformat(source_path.parent.name)
    except ValueError as exc:
        message = f"无法从 parameter promotion artifact 推断日期：{source_path}"
        raise typer.BadParameter(message) from exc


def _latest_decision_snapshot_path(snapshot_dir: Path) -> Path:
    candidates: list[tuple[date, Path]] = []
    for path in snapshot_dir.glob("decision_snapshot_*.json"):
        if not path.is_file():
            continue
        try:
            candidates.append((_decision_snapshot_date(path), path))
        except typer.BadParameter:
            continue
    if not candidates:
        raise typer.BadParameter(f"未找到可用 decision_snapshot：{snapshot_dir}")
    return max(candidates, key=lambda item: (item[0], item[1].name))[1]


def _decision_snapshot_date(path: Path) -> date:
    raw_date = path.stem.removeprefix("decision_snapshot_")
    try:
        return date.fromisoformat(raw_date)
    except ValueError:
        pass
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise typer.BadParameter(f"无法读取 decision_snapshot 日期：{path}") from exc
    if isinstance(payload, dict):
        signal_date = payload.get("signal_date") or payload.get("as_of")
        if signal_date:
            return _parse_date(str(signal_date))
    raise typer.BadParameter(f"decision_snapshot 文件名或内容缺少 YYYY-MM-DD 日期：{path}")


def _read_optional_json_object(path: Path | None) -> dict[str, object]:
    if path is None or not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _download_manifest_path(prices_path: Path) -> Path:
    return prices_path.parent / "download_manifest.csv"


def _marketstack_prices_path(prices_path: Path) -> Path:
    return prices_path.parent / "prices_marketstack_daily.csv"


def _requires_marketstack_prices(prices_path: Path) -> bool:
    default_prices_path = PROJECT_ROOT / "data" / "raw" / "prices_daily.csv"
    try:
        return prices_path.resolve() == default_prices_path.resolve()
    except OSError:
        return prices_path == default_prices_path


@reports_app.command("investment-review")
def investment_periodic_review_command(
    period: Annotated[
        str,
        typer.Option(help="复盘周期：weekly 或 monthly。"),
    ] = "weekly",
    as_of: Annotated[
        str | None,
        typer.Option(help="复盘截止日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    since: Annotated[
        str | None,
        typer.Option(help="复盘起始日期，格式为 YYYY-MM-DD；不传时按周期默认。"),
    ] = None,
    scores_path: Annotated[
        Path,
        typer.Option(help="scores_daily.csv 路径。"),
    ] = DEFAULT_SCORES_DAILY_PATH,
    decision_snapshot_path: Annotated[
        Path,
        typer.Option(help="decision snapshot 文件或目录。"),
    ] = DEFAULT_DECISION_SNAPSHOT_DIR,
    outcomes_path: Annotated[
        Path,
        typer.Option(help="decision_outcomes.csv 路径。"),
    ] = DEFAULT_DECISION_OUTCOMES_PATH,
    prediction_outcomes_path: Annotated[
        Path,
        typer.Option(help="prediction_outcomes.csv 路径，用于 production vs challenger 复盘。"),
    ] = DEFAULT_PREDICTION_OUTCOMES_PATH,
    learning_queue_path: Annotated[
        Path,
        typer.Option(help="decision learning queue JSON 路径。"),
    ] = DEFAULT_DECISION_LEARNING_QUEUE_PATH,
    rule_experiment_path: Annotated[
        Path,
        typer.Option(help="rule experiment ledger JSON 路径。"),
    ] = DEFAULT_RULE_EXPERIMENT_LEDGER_PATH,
    market_regime_id: Annotated[
        str,
        typer.Option(help="报告声明使用的市场阶段 id。"),
    ] = "ai_after_chatgpt",
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 周报/月报复盘报告输出路径。"),
    ] = None,
) -> None:
    """生成周报/月报投资复盘报告。"""
    if period not in {"weekly", "monthly"}:
        raise typer.BadParameter("period 必须是 weekly 或 monthly")
    review_date = _parse_date(as_of) if as_of else date.today()
    since_date = _parse_date(since) if since else None
    report = build_periodic_investment_review_report(
        period=period,  # type: ignore[arg-type]
        as_of=review_date,
        since=since_date,
        market_regime_id=market_regime_id,
        scores_path=scores_path,
        decision_snapshot_path=decision_snapshot_path,
        outcomes_path=outcomes_path,
        prediction_outcomes_path=prediction_outcomes_path,
        learning_queue_path=learning_queue_path,
        rule_experiment_path=rule_experiment_path,
    )
    report_path = output_path or default_periodic_investment_review_report_path(
        DEFAULT_PERIODIC_INVESTMENT_REVIEW_REPORT_DIR,
        period,  # type: ignore[arg-type]
        review_date,
    )
    write_periodic_investment_review_report(report, report_path)
    style = "green" if report.status == "PASS" else "yellow"
    console.print(f"[{style}]投资复盘状态：{report.status}[/{style}]")
    console.print(f"报告：{report_path}")
    console.print(
        f"区间：{report.since.isoformat()} 至 {report.as_of.isoformat()}；"
        f"样本：{len(report.score_rows)}"
    )


@reports_app.command("calculation-explainers")
def calculation_explainers_command(
    as_of: Annotated[
        str | None,
        typer.Option(help="计算解释日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    decision_snapshot_path: Annotated[
        Path | None,
        typer.Option(help="decision snapshot JSON 路径；不传时按 as_of 使用默认路径。"),
    ] = None,
    registry_path: Annotated[
        Path,
        typer.Option(help="metric explainer registry YAML 路径。"),
    ] = DEFAULT_METRIC_EXPLAINERS_CONFIG_PATH,
    scores_daily_path: Annotated[
        Path | None,
        typer.Option(help="scores_daily.csv 路径；不传时使用默认处理后评分缓存。"),
    ] = DEFAULT_SCORES_DAILY_PATH,
    output_path: Annotated[
        Path | None,
        typer.Option(help="calculation explainers JSON 输出路径。"),
    ] = None,
) -> None:
    """生成只读计算解释 JSON。"""
    report_date = _parse_date(as_of) if as_of else date.today()
    snapshot_path = decision_snapshot_path or default_decision_snapshot_path(
        DEFAULT_DECISION_SNAPSHOT_DIR,
        report_date,
    )
    resolved_output_path = output_path or default_calculation_explainers_path(
        PROJECT_ROOT / "outputs" / "reports",
        report_date,
    )
    try:
        payload = build_calculation_explainers_payload(
            as_of=report_date,
            decision_snapshot_path=snapshot_path,
            registry_path=registry_path,
            scores_daily_path=scores_daily_path,
        )
    except FileNotFoundError as exc:
        raise typer.BadParameter(str(exc)) from exc
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    json_path = write_calculation_explainers_json(payload, resolved_output_path)
    style = "green" if payload["status"] == "PASS" else "yellow"
    console.print(f"[{style}]计算解释：{payload['status']}[/{style}]")
    console.print(f"Calculation explainers JSON：{json_path}")
    console.print(
        f"metrics：{len(payload['metrics'])}；"
        f"warnings：{len(payload['warnings'])}；"
        f"production_effect={payload['production_effect']}"
    )


@reports_app.command("shadow-parameter-backtest")
def shadow_parameter_backtest_report_command(
    latest: Annotated[
        bool,
        typer.Option(help="读取最新正式 shadow backtest artifact。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="报告日期，格式为 YYYY-MM-DD。"),
    ] = None,
    source_path: Annotated[
        Path | None,
        typer.Option(help="显式 shadow_backtest_summary.json 路径。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 alias 输出目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
) -> None:
    """从正式 artifact 生成 shadow parameter backtest 报告 alias。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    json_source = source_path or _resolve_shadow_backtest_summary_path(
        latest=latest,
        as_of=as_of,
    )
    payload = load_shadow_backtest_payload(json_source)
    issues = validate_shadow_backtest_payload(payload)
    if issues:
        raise typer.BadParameter("shadow backtest JSON 校验失败：" + "; ".join(issues))
    report_date = _shadow_backtest_payload_date(payload, json_source)
    json_path, markdown_path = write_shadow_backtest_report_alias(
        payload,
        reports_dir,
        report_date,
    )
    console.print("[green]Shadow parameter backtest report：OK[/green]")
    console.print(f"JSON：{json_path}")
    console.print(f"Markdown：{markdown_path}")
    console.print("production_effect=none；不修改 production 参数")


@reports_app.command("parameter-promotion")
def parameter_promotion_report_command(
    latest: Annotated[
        bool,
        typer.Option(help="读取最新正式 parameter promotion artifact。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="报告日期，格式为 YYYY-MM-DD。"),
    ] = None,
    source_path: Annotated[
        Path | None,
        typer.Option(help="显式 parameter_promotion_decision.json 路径。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 alias 输出目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
) -> None:
    """从正式 artifact 生成 parameter promotion decision 报告 alias。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    json_source = source_path or _resolve_parameter_promotion_path(
        latest=latest,
        as_of=as_of,
    )
    payload = load_parameter_promotion_payload(json_source)
    if not payload:
        raise typer.BadParameter(f"无法读取 parameter promotion JSON：{json_source}")
    report_date = _parameter_promotion_payload_date(payload, json_source)
    json_path, markdown_path = write_parameter_promotion_report_alias(
        payload,
        reports_dir,
        report_date,
    )
    decision = payload.get("promotion_decision", {})
    status = decision.get("status", "UNKNOWN") if isinstance(decision, dict) else "UNKNOWN"
    console.print("[green]Parameter promotion report：OK[/green]")
    console.print(f"promotion_status：{status}")
    console.print(f"JSON：{json_path}")
    console.print(f"Markdown：{markdown_path}")
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")


@reports_app.command("signal-snapshot")
def signal_snapshot_report_command(
    latest: Annotated[
        bool,
        typer.Option(help="读取最新正式 signal snapshot artifact。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="报告日期，格式为 YYYY-MM-DD。"),
    ] = None,
    source_path: Annotated[
        Path | None,
        typer.Option(help="显式 signal_snapshot.json 路径。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 alias 输出目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
) -> None:
    """从正式 artifact 生成 signal snapshot 报告 alias。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    json_source = source_path or _resolve_signal_snapshot_path(latest=latest, as_of=as_of)
    payload = load_signal_snapshot_payload(json_source)
    issues = validate_signal_snapshot_payload(payload)
    if issues:
        raise typer.BadParameter("signal snapshot JSON 校验失败：" + "; ".join(issues))
    report_date = _signal_snapshot_payload_date(payload, json_source)
    json_path, markdown_path = write_signal_snapshot_report_alias(payload, reports_dir, report_date)
    summary = signal_snapshot_summary(payload)
    console.print("[green]Signal snapshot report：OK[/green]")
    console.print(f"status：{summary.get('status', 'UNKNOWN')}")
    console.print(f"JSON：{json_path}")
    console.print(f"Markdown：{markdown_path}")
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")


@reports_app.command("signal-ablation")
def signal_ablation_report_command(
    latest: Annotated[
        bool,
        typer.Option(help="读取最新正式 signal ablation artifact。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="报告日期，格式为 YYYY-MM-DD。"),
    ] = None,
    source_path: Annotated[
        Path | None,
        typer.Option(help="显式 signal_ablation_summary.json 路径。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 alias 输出目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
) -> None:
    """从正式 artifact 生成 signal ablation 报告 alias。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    json_source = source_path or _resolve_signal_ablation_path(latest=latest, as_of=as_of)
    payload = load_signal_ablation_payload(json_source)
    issues = validate_signal_ablation_payload(payload)
    if issues:
        raise typer.BadParameter("signal ablation JSON 校验失败：" + "; ".join(issues))
    try:
        report_date = signal_ablation_payload_date(payload, json_source)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    json_path, markdown_path = write_signal_ablation_report_alias(
        payload,
        reports_dir,
        report_date,
    )
    summary = payload.get("summary", {}) if isinstance(payload, dict) else {}
    console.print("[green]Signal ablation report：OK[/green]")
    if isinstance(summary, dict):
        console.print(
            f"positive_signals={len(summary.get('positive_signals', []))}；"
            f"negative_signals={len(summary.get('negative_signals', []))}；"
            f"promotion_credit_signals={len(summary.get('promotion_credit_signals', []))}"
        )
        reason = summary.get("no_promotion_credit_reason")
        if reason:
            console.print(f"no_promotion_credit_reason={reason}")
    console.print(f"JSON：{json_path}")
    console.print(f"Markdown：{markdown_path}")
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")


@reports_app.command("signal-calibration")
def signal_calibration_report_command(
    latest: Annotated[
        bool,
        typer.Option(help="读取最新正式 signal calibration artifact。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="报告日期，格式为 YYYY-MM-DD。"),
    ] = None,
    source_path: Annotated[
        Path | None,
        typer.Option(help="显式 signal_calibration_summary.json 路径。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 alias 输出目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
) -> None:
    """从正式 artifact 生成 signal calibration 报告 alias。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    json_source = source_path or _resolve_signal_calibration_path(latest=latest, as_of=as_of)
    payload = load_signal_calibration_payload(json_source)
    issues = validate_signal_calibration_payload(payload)
    if issues:
        raise typer.BadParameter("signal calibration JSON 校验失败：" + "; ".join(issues))
    try:
        report_date = signal_calibration_payload_date(payload, json_source)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    json_path, markdown_path = write_signal_calibration_report_alias(
        payload,
        reports_dir,
        report_date,
    )
    ranking = payload.get("ranking", {}) if isinstance(payload, dict) else {}
    console.print("[green]Signal calibration report：OK[/green]")
    if isinstance(ranking, dict):
        console.print(f"best_profile={ranking.get('best_profile', 'UNKNOWN')}")
        reason = ranking.get("reason")
        if reason:
            console.print(f"reason={reason}")
    console.print(f"JSON：{json_path}")
    console.print(f"Markdown：{markdown_path}")
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")


@reports_app.command("portfolio-sensitivity")
def portfolio_sensitivity_report_command(
    latest: Annotated[
        bool,
        typer.Option(help="读取最新正式 portfolio sensitivity artifact。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="报告日期，格式为 YYYY-MM-DD。"),
    ] = None,
    source_path: Annotated[
        Path | None,
        typer.Option(help="显式 portfolio_sensitivity_summary.json 路径。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 alias 输出目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
) -> None:
    """从正式 artifact 生成 portfolio sensitivity 报告 alias。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    json_source = source_path or _resolve_portfolio_sensitivity_path(
        latest=latest,
        as_of=as_of,
    )
    payload = load_portfolio_sensitivity_payload(json_source)
    issues = validate_portfolio_sensitivity_payload(payload)
    if issues:
        raise typer.BadParameter("portfolio sensitivity JSON 校验失败：" + "; ".join(issues))
    try:
        report_date = portfolio_sensitivity_payload_date(payload, json_source)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    json_path, markdown_path = write_portfolio_sensitivity_report_alias(
        payload,
        reports_dir,
        report_date,
    )
    ranking = payload.get("ranking", {}) if isinstance(payload, dict) else {}
    diagnosis = payload.get("diagnosis", {}) if isinstance(payload, dict) else {}
    console.print("[green]Portfolio sensitivity report：OK[/green]")
    if isinstance(ranking, dict):
        console.print(f"best_profile={ranking.get('best_profile', 'UNKNOWN')}")
    if isinstance(diagnosis, dict):
        console.print(f"primary_bottleneck={diagnosis.get('primary_bottleneck', 'UNKNOWN')}")
    console.print(f"JSON：{json_path}")
    console.print(f"Markdown：{markdown_path}")
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")


@reports_app.command("portfolio-candidates")
def portfolio_candidates_report_command(
    latest: Annotated[
        bool,
        typer.Option(help="读取最新正式 portfolio candidates artifact。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="报告日期，格式为 YYYY-MM-DD。"),
    ] = None,
    source_path: Annotated[
        Path | None,
        typer.Option(help="显式 portfolio_candidates_summary.json 路径。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 alias 输出目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
) -> None:
    """从正式 artifact 生成 portfolio candidates 报告 alias。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    json_source = source_path or _resolve_portfolio_candidates_path(
        latest=latest,
        as_of=as_of,
    )
    payload = load_portfolio_candidates_payload(json_source)
    issues = validate_portfolio_candidates_payload(payload)
    if issues:
        raise typer.BadParameter("portfolio candidates JSON 校验失败：" + "; ".join(issues))
    try:
        report_date = portfolio_candidates_payload_date(payload, json_source)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    json_path, markdown_path = write_portfolio_candidates_report_alias(
        payload,
        reports_dir,
        report_date,
    )
    ranking = payload.get("ranking", {}) if isinstance(payload, dict) else {}
    console.print("[green]Portfolio candidates report：OK[/green]")
    if isinstance(ranking, dict):
        console.print(f"best_profile={ranking.get('best_profile', 'UNKNOWN')}")
        reason = ranking.get("reason")
        if reason:
            console.print(f"reason={reason}")
    console.print(f"JSON：{json_path}")
    console.print(f"Markdown：{markdown_path}")
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")


@reports_app.command("portfolio-turnover-attribution")
def portfolio_turnover_attribution_report_command(
    latest: Annotated[
        bool,
        typer.Option(help="读取最新正式 portfolio turnover attribution artifact。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="报告日期，格式为 YYYY-MM-DD。"),
    ] = None,
    source_path: Annotated[
        Path | None,
        typer.Option(help="显式 portfolio_turnover_attribution_summary.json 路径。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 alias 输出目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
) -> None:
    """从正式 artifact 生成 portfolio turnover attribution 报告 alias。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    json_source = source_path or _resolve_portfolio_turnover_attribution_path(
        latest=latest,
        as_of=as_of,
    )
    payload = load_portfolio_turnover_attribution_payload(json_source)
    issues = validate_portfolio_turnover_attribution_payload(payload)
    if issues:
        raise typer.BadParameter(
            "portfolio turnover attribution JSON 校验失败：" + "; ".join(issues)
        )
    try:
        report_date = portfolio_turnover_attribution_payload_date(payload, json_source)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    json_path, markdown_path = write_portfolio_turnover_attribution_report_alias(
        payload,
        reports_dir,
        report_date,
    )
    root_cause = payload.get("root_cause", {}) if isinstance(payload, dict) else {}
    candidate_summary = (
        payload.get("candidate_turnover_summary", {}) if isinstance(payload, dict) else {}
    )
    console.print("[green]Portfolio turnover attribution report：OK[/green]")
    if isinstance(root_cause, dict):
        console.print(f"root_cause_category={root_cause.get('category', 'mixed')}")
    if isinstance(candidate_summary, dict):
        console.print(f"failed_by_turnover={candidate_summary.get('total_failed_by_turnover', 0)}")
    console.print(f"JSON：{json_path}")
    console.print(f"Markdown：{markdown_path}")
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")


@reports_app.command("portfolio-candidate-review")
def portfolio_candidate_review_report_command(
    latest: Annotated[
        bool,
        typer.Option(help="读取最新正式 portfolio candidate review decision。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="报告日期，格式为 YYYY-MM-DD。"),
    ] = None,
    source_path: Annotated[
        Path | None,
        typer.Option(help="显式 portfolio_candidate_review_decision.json 路径。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 alias 输出目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
) -> None:
    """从正式 artifact 生成 portfolio candidate review 报告 alias。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    decision_source = source_path or _resolve_portfolio_candidate_review_decision_path(
        latest=latest,
        as_of=as_of,
    )
    decision_payload = load_portfolio_candidate_review_payload(decision_source)
    issues = validate_portfolio_candidate_review_decision_payload(decision_payload)
    if issues:
        raise typer.BadParameter("portfolio candidate review JSON 校验失败：" + "; ".join(issues))
    package_source = decision_source.parent / "portfolio_candidate_review_package.json"
    package_payload = load_portfolio_candidate_review_payload(package_source)
    if not package_payload:
        raise typer.BadParameter(f"未找到 review package artifact：{package_source}")
    try:
        report_date = portfolio_candidate_review_payload_date(
            decision_payload,
            decision_source,
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    json_path, markdown_path = write_portfolio_candidate_review_report_alias(
        package_payload,
        decision_payload,
        reports_dir,
        report_date,
    )
    decision = decision_payload.get("decision", {}) if isinstance(decision_payload, dict) else {}
    candidate = decision_payload.get("candidate", {}) if isinstance(decision_payload, dict) else {}
    console.print("[green]Portfolio candidate review report：OK[/green]")
    if isinstance(decision, dict):
        console.print(f"status={decision.get('status', 'UNKNOWN')}")
        console.print(f"allowed_next_step={decision.get('allowed_next_step', 'UNKNOWN')}")
    if isinstance(candidate, dict):
        console.print(f"candidate_profile={candidate.get('profile_name', 'UNKNOWN')}")
    console.print(f"JSON：{json_path}")
    console.print(f"Markdown：{markdown_path}")
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")


@reports_app.command("portfolio-candidate-tracking")
def portfolio_candidate_tracking_report_command(
    latest: Annotated[
        bool,
        typer.Option(help="读取最新正式 portfolio candidate tracking summary。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="报告日期，格式为 YYYY-MM-DD。"),
    ] = None,
    source_path: Annotated[
        Path | None,
        typer.Option(help="显式 portfolio_candidate_tracking_summary.json 路径。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 alias 输出目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
) -> None:
    """从正式 artifact 生成 portfolio candidate tracking 报告 alias。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    json_source = source_path or _resolve_portfolio_candidate_tracking_path(
        latest=latest,
        as_of=as_of,
    )
    payload = load_portfolio_candidate_tracking_payload(json_source)
    issues = validate_portfolio_candidate_tracking_payload(payload)
    if issues:
        raise typer.BadParameter("portfolio candidate tracking JSON 校验失败：" + "; ".join(issues))
    try:
        report_date = portfolio_candidate_tracking_payload_date(payload, json_source)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    json_path, markdown_path = write_portfolio_candidate_tracking_report_alias(
        payload,
        reports_dir,
        report_date,
    )
    candidate = payload.get("candidate", {}) if isinstance(payload, dict) else {}
    console.print("[green]Portfolio candidate tracking report：OK[/green]")
    if isinstance(candidate, dict):
        console.print(f"candidate_profile={candidate.get('profile_name', 'UNKNOWN')}")
        console.print(f"tracking_status={candidate.get('tracking_status', 'UNKNOWN')}")
    console.print(f"JSON：{json_path}")
    console.print(f"Markdown：{markdown_path}")
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")


@reports_app.command("portfolio-tracking-review")
def portfolio_tracking_review_report_command(
    latest: Annotated[
        bool,
        typer.Option(help="读取最新正式 portfolio tracking review summary。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="报告日期，格式为 YYYY-MM-DD。"),
    ] = None,
    source_path: Annotated[
        Path | None,
        typer.Option(help="显式 portfolio_tracking_review_summary.json 路径。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 alias 输出目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
) -> None:
    """从正式 artifact 生成 portfolio tracking review 报告 alias。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    json_source = source_path or _resolve_portfolio_tracking_review_path(
        latest=latest,
        as_of=as_of,
    )
    payload = load_portfolio_tracking_review_payload(json_source)
    issues = validate_portfolio_tracking_review_payload(payload)
    if issues:
        raise typer.BadParameter("portfolio tracking review JSON 校验失败：" + "; ".join(issues))
    try:
        report_date = portfolio_tracking_review_payload_date(payload, json_source)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    json_path, markdown_path = write_portfolio_tracking_review_report_alias(
        payload,
        reports_dir,
        report_date,
    )
    recommendation = payload.get("recommendation", {}) if isinstance(payload, dict) else {}
    candidate = payload.get("candidate", {}) if isinstance(payload, dict) else {}
    console.print("[green]Portfolio tracking review report：OK[/green]")
    if isinstance(candidate, dict):
        console.print(f"candidate_profile={candidate.get('profile_name', 'UNKNOWN')}")
        console.print(f"tracking_days={candidate.get('tracking_days', 0)}")
    if isinstance(recommendation, dict):
        console.print(f"recommendation={recommendation.get('status', 'UNKNOWN')}")
    console.print(f"JSON：{json_path}")
    console.print(f"Markdown：{markdown_path}")
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")


@reports_app.command("weight-tuning")
def weight_tuning_report_command(
    latest: Annotated[
        bool,
        typer.Option(help="读取最新正式 weight tuning summary。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="报告日期，格式为 YYYY-MM-DD。"),
    ] = None,
    source_path: Annotated[
        Path | None,
        typer.Option(help="显式 weight_tuning_summary.json 路径。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 alias 输出目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
) -> None:
    """从正式 artifact 生成 weight tuning 报告 alias。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    json_source = source_path or _resolve_weight_tuning_path(
        latest=latest,
        as_of=as_of,
    )
    payload = load_weight_tuning_payload(json_source)
    issues = validate_weight_tuning_payload(payload)
    if issues:
        raise typer.BadParameter("weight tuning JSON 校验失败：" + "; ".join(issues))
    try:
        report_date = weight_tuning_payload_date(payload, json_source)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    json_path, markdown_path = write_weight_tuning_report_alias(
        payload,
        reports_dir,
        report_date,
    )
    recommended = payload.get("recommended_candidate", {}) if isinstance(payload, dict) else {}
    search = payload.get("search", {}) if isinstance(payload, dict) else {}
    console.print("[green]Weight tuning report：OK[/green]")
    if isinstance(recommended, dict):
        console.print(f"weight_candidate_status={recommended.get('status', 'UNKNOWN')}")
    if isinstance(search, dict):
        console.print(f"candidates_evaluated={search.get('candidates_evaluated', 0)}")
    console.print(f"JSON：{json_path}")
    console.print(f"Markdown：{markdown_path}")
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")


@reports_app.command("weight-stability")
def weight_stability_report_command(
    latest: Annotated[
        bool,
        typer.Option(help="读取最新正式 weight stability summary。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="报告日期，格式为 YYYY-MM-DD。"),
    ] = None,
    source_path: Annotated[
        Path | None,
        typer.Option(help="显式 weight_stability_summary.json 路径。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 alias 输出目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
) -> None:
    """从正式 artifact 生成 weight stability 报告 alias。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    json_source = source_path or _resolve_weight_stability_path(
        latest=latest,
        as_of=as_of,
    )
    payload = load_weight_stability_payload(json_source)
    issues = validate_weight_stability_payload(payload)
    if issues:
        raise typer.BadParameter("weight stability JSON 校验失败：" + "; ".join(issues))
    try:
        report_date = weight_stability_payload_date(payload, json_source)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    json_path, markdown_path = write_weight_stability_report_alias(
        payload,
        reports_dir,
        report_date,
    )
    recommended = payload.get("recommended_candidate", {}) if isinstance(payload, dict) else {}
    search = payload.get("search_summary", {}) if isinstance(payload, dict) else {}
    console.print("[green]Weight stability report：OK[/green]")
    if isinstance(recommended, dict):
        console.print(f"stable_candidate_status={recommended.get('status', 'UNKNOWN')}")
    if isinstance(search, dict):
        console.print(f"candidates_backtested={search.get('candidates_backtested', 0)}")
    console.print(f"JSON：{json_path}")
    console.print(f"Markdown：{markdown_path}")
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")


@reports_app.command("weight-stability-readiness")
def weight_stability_readiness_report_command(
    latest: Annotated[
        bool,
        typer.Option(help="读取最新正式 weight stability readiness summary。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="报告日期，格式为 YYYY-MM-DD。"),
    ] = None,
    source_path: Annotated[
        Path | None,
        typer.Option(help="显式 weight_stability_readiness_summary.json 路径。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 alias 输出目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
) -> None:
    """从正式 artifact 生成 weight stability readiness 报告 alias。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    json_source = source_path or _resolve_weight_stability_readiness_path(
        latest=latest,
        as_of=as_of,
    )
    payload = load_weight_stability_readiness_payload(json_source)
    issues = validate_weight_stability_readiness_payload(payload)
    if issues:
        raise typer.BadParameter("weight stability readiness JSON 校验失败：" + "; ".join(issues))
    try:
        report_date = weight_stability_readiness_payload_date(payload, json_source)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    json_path, markdown_path = write_weight_stability_readiness_report_alias(
        payload,
        reports_dir,
        report_date,
    )
    eligibility = payload.get("stable_tuning_eligibility", {})
    console.print("[green]Weight stability readiness report：OK[/green]")
    if isinstance(eligibility, dict):
        console.print(
            f"readiness_status={eligibility.get('status', 'UNKNOWN')}；"
            f"can_run={str(eligibility.get('can_run', False)).lower()}"
        )
    console.print(f"JSON：{json_path}")
    console.print(f"Markdown：{markdown_path}")
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")


@reports_app.command("weight-tuning-failure")
def weight_tuning_failure_report_command(
    latest: Annotated[
        bool,
        typer.Option(help="读取最新正式 weight tuning failure attribution。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="报告日期，格式为 YYYY-MM-DD。"),
    ] = None,
    source_path: Annotated[
        Path | None,
        typer.Option(help="显式 weight_tuning_failure_summary.json 路径。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 alias 输出目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
) -> None:
    """从正式 artifact 生成 weight tuning failure attribution 报告 alias。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    json_source = source_path or _resolve_weight_tuning_failure_path(
        latest=latest,
        as_of=as_of,
    )
    payload = load_weight_tuning_failure_payload(json_source)
    issues = validate_weight_tuning_failure_payload(payload)
    if issues:
        raise typer.BadParameter("weight tuning failure JSON 校验失败：" + "; ".join(issues))
    try:
        report_date = weight_tuning_failure_payload_date(payload, json_source)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    json_path, markdown_path = write_weight_tuning_failure_report_alias(
        payload,
        reports_dir,
        report_date,
    )
    root_cause = payload.get("root_cause", {}) if isinstance(payload, dict) else {}
    rejection = payload.get("candidate_rejection_summary", {}) if isinstance(payload, dict) else {}
    console.print("[green]Weight tuning failure report：OK[/green]")
    if isinstance(root_cause, dict):
        console.print(f"root_cause_category={root_cause.get('category', 'mixed')}")
    if isinstance(rejection, dict):
        console.print(f"total_candidates={rejection.get('total_candidates', 0)}")
    console.print(f"JSON：{json_path}")
    console.print(f"Markdown：{markdown_path}")
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")


@reports_app.command("data-freshness")
def market_data_freshness_report_command(
    latest: Annotated[
        bool,
        typer.Option(help="读取最新正式 market data freshness summary。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="报告日期，格式为 YYYY-MM-DD。"),
    ] = None,
    source_path: Annotated[
        Path | None,
        typer.Option(help="显式 market_data_freshness_summary.json 路径。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 alias 输出目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
) -> None:
    """从正式 artifact 生成 market data freshness 报告 alias。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    json_source = source_path or _resolve_market_data_freshness_path(
        latest=latest,
        as_of=as_of,
    )
    payload = load_market_data_freshness_payload(json_source)
    issues = validate_market_data_freshness_payload(payload)
    if issues:
        raise typer.BadParameter("market data freshness JSON 校验失败：" + "; ".join(issues))
    try:
        report_date = market_data_freshness_payload_date(payload, json_source)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    json_path, markdown_path = write_market_data_freshness_report_alias(
        payload,
        reports_dir,
        report_date,
    )
    freshness = payload.get("freshness", {}) if isinstance(payload, dict) else {}
    readiness = payload.get("tracking_readiness", {}) if isinstance(payload, dict) else {}
    console.print("[green]Market data freshness report：OK[/green]")
    if isinstance(freshness, dict):
        console.print(f"freshness_status={freshness.get('status', 'UNKNOWN')}")
    if isinstance(readiness, dict):
        console.print(f"tracking_readiness={readiness.get('readiness', 'UNKNOWN')}")
    console.print(f"JSON：{json_path}")
    console.print(f"Markdown：{markdown_path}")
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")


@reports_app.command("data-refresh")
def market_data_refresh_report_command(
    latest: Annotated[
        bool,
        typer.Option(help="读取最新正式 market data refresh summary。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="报告日期，格式为 YYYY-MM-DD。"),
    ] = None,
    source_path: Annotated[
        Path | None,
        typer.Option(help="显式 market_data_refresh_summary.json 路径。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 alias 输出目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
) -> None:
    """从正式 artifact 生成 market data refresh 报告 alias。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    json_source = source_path or _resolve_market_data_refresh_path(
        latest=latest,
        as_of=as_of,
    )
    payload = load_market_data_refresh_payload(json_source)
    issues = validate_market_data_refresh_payload(payload)
    if issues:
        raise typer.BadParameter("market data refresh JSON 校验失败：" + "; ".join(issues))
    try:
        report_date = market_data_refresh_payload_date(payload, json_source)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    json_path, markdown_path = write_market_data_refresh_report_alias(
        payload,
        reports_dir,
        report_date,
    )
    metadata = payload.get("metadata", {}) if isinstance(payload, dict) else {}
    after = payload.get("after", {}) if isinstance(payload, dict) else {}
    console.print("[green]Market data refresh report：OK[/green]")
    if isinstance(metadata, dict):
        console.print(f"refresh_status={metadata.get('status', 'UNKNOWN')}")
    if isinstance(after, dict):
        console.print(f"freshness_status={after.get('freshness_status', 'UNKNOWN')}")
        console.print(f"tracking_status={after.get('candidate_tracking_status', 'UNKNOWN')}")
    console.print(f"JSON：{json_path}")
    console.print(f"Markdown：{markdown_path}")
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")


@reports_app.command("reader-brief")
def reader_brief_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Reader Brief 日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option(help="使用默认 decision snapshot 目录中的最新 signal-date snapshot。"),
    ] = False,
    reports_dir: Annotated[
        Path,
        typer.Option(help="同日报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    decision_snapshot_path: Annotated[
        Path | None,
        typer.Option(help="decision snapshot JSON 路径；不传时按 as_of 使用默认路径。"),
    ] = None,
    calculation_explainers_path: Annotated[
        Path | None,
        typer.Option(help="calculation explainers JSON 路径；不传时按 as_of 使用默认报告路径。"),
    ] = None,
    daily_decision_summary_path: Annotated[
        Path | None,
        typer.Option(help="daily_decision_summary JSON 路径；不传时按 as_of 使用默认报告路径。"),
    ] = None,
    evidence_dashboard_json_path: Annotated[
        Path | None,
        typer.Option(help="evidence dashboard JSON 路径；不传时按 as_of 使用默认报告路径。"),
    ] = None,
    daily_task_dashboard_json_path: Annotated[
        Path | None,
        typer.Option(help="daily task dashboard JSON 路径；不传时按 as_of 使用默认报告路径。"),
    ] = None,
    daily_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 日报路径；不传时按 as_of 使用默认报告路径。"),
    ] = None,
    trace_bundle_path: Annotated[
        Path | None,
        typer.Option(help="日报 trace bundle JSON 路径；不传时按日报路径推导。"),
    ] = None,
    score_change_attribution_path: Annotated[
        Path | None,
        typer.Option(help="score_change_attribution JSON 路径；不传时按 as_of 使用默认报告路径。"),
    ] = None,
    market_panel_path: Annotated[
        Path | None,
        typer.Option(help="market_panel JSON 路径；不传时按 as_of 使用默认报告路径。"),
    ] = None,
    research_governance_summary_path: Annotated[
        Path | None,
        typer.Option(
            help="research_governance_summary JSON 路径；不传时按 as_of 使用默认报告路径。"
        ),
    ] = None,
    report_index_path: Annotated[
        Path | None,
        typer.Option(help="report_index JSON 路径；不传时按 as_of 使用默认报告路径。"),
    ] = None,
    documentation_contract_path: Annotated[
        Path | None,
        typer.Option(help="documentation_contract JSON 路径；不传时按 as_of 使用默认报告路径。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Reader Brief HTML 输出路径。"),
    ] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Reader Brief JSON 输出路径。"),
    ] = None,
) -> None:
    """生成只读统一读者入口 HTML/JSON。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --as-of/--date 同时使用")
    if latest:
        snapshot_path = decision_snapshot_path or _latest_decision_snapshot_path(
            DEFAULT_DECISION_SNAPSHOT_DIR
        )
        report_date = _decision_snapshot_date(snapshot_path)
    else:
        report_date = _parse_date(as_of) if as_of else date.today()
        snapshot_path = decision_snapshot_path or default_decision_snapshot_path(
            DEFAULT_DECISION_SNAPSHOT_DIR,
            report_date,
        )
    calc_path = calculation_explainers_path or default_calculation_explainers_path(
        reports_dir,
        report_date,
    )
    decision_summary_path = daily_decision_summary_path or default_daily_decision_summary_path(
        reports_dir,
        report_date,
    )
    dashboard_json_path = evidence_dashboard_json_path or default_evidence_dashboard_json_path(
        reports_dir,
        report_date,
    )
    task_dashboard_json_path = (
        daily_task_dashboard_json_path
        or default_daily_task_dashboard_json_path(
            reports_dir,
            report_date,
        )
    )
    daily_path = daily_report_path or default_daily_score_report_path(reports_dir, report_date)
    trace_path = trace_bundle_path or default_report_trace_bundle_path(daily_path)
    score_change_path = score_change_attribution_path or default_score_change_attribution_json_path(
        reports_dir, report_date
    )
    market_panel_json_path = market_panel_path or default_market_panel_json_path(
        reports_dir,
        report_date,
    )
    research_governance_path = (
        research_governance_summary_path
        or default_research_governance_summary_json_path(reports_dir, report_date)
    )
    index_path = report_index_path or default_report_index_json_path(reports_dir, report_date)
    docs_contract_path = documentation_contract_path or default_documentation_contract_json_path(
        reports_dir,
        report_date,
    )
    html_output = output_path or default_reader_brief_html_path(reports_dir, report_date)
    json_output = json_output_path or default_reader_brief_json_path(reports_dir, report_date)
    try:
        payload = build_reader_brief_payload(
            as_of=report_date,
            reports_dir=reports_dir,
            decision_snapshot_path=snapshot_path,
            calculation_explainers_path=calc_path,
            daily_decision_summary_path=decision_summary_path,
            evidence_dashboard_json_path=dashboard_json_path,
            daily_task_dashboard_json_path=task_dashboard_json_path,
            daily_report_path=daily_path,
            trace_bundle_path=trace_path,
            score_change_attribution_path=score_change_path,
            market_panel_path=market_panel_json_path,
            research_governance_summary_path=research_governance_path,
            report_index_path=index_path,
            documentation_contract_path=docs_contract_path,
        )
    except FileNotFoundError as exc:
        raise typer.BadParameter(str(exc)) from exc
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    html_path = write_reader_brief_html(payload, html_output)
    json_path = write_reader_brief_json(payload, json_output)
    style = "green" if payload["status"] == "PASS" else "yellow"
    console.print(f"[{style}]Reader Brief：{payload['status']}[/{style}]")
    console.print(f"Reader Brief HTML：{html_path}")
    console.print(f"Reader Brief JSON：{json_path}")
    console.print(
        f"warnings：{len(payload['warnings'])}；"
        f"production_effect={payload['production_effect']}；"
        "不生成交易指令"
    )


@reports_app.command("validate-reader-brief")
def validate_reader_brief_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Reader Brief 质量校验日期，格式为 YYYY-MM-DD。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option(
            help="使用默认 decision snapshot 目录中的最新 signal-date，并校验对应 Reader Brief。"
        ),
    ] = False,
    reports_dir: Annotated[
        Path,
        typer.Option(help="Reader Brief artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    reader_brief_json_path: Annotated[
        Path | None,
        typer.Option(help="Reader Brief JSON 路径；不传时按日期使用默认路径。"),
    ] = None,
    reader_brief_html_path: Annotated[
        Path | None,
        typer.Option(help="Reader Brief HTML 路径；不传时按日期使用默认路径。"),
    ] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Reader Brief quality JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Reader Brief quality Markdown 输出路径。"),
    ] = None,
) -> None:
    """校验既有 Reader Brief，并生成只读 quality report。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --as-of/--date 同时使用")
    if latest:
        snapshot_path = _latest_decision_snapshot_path(DEFAULT_DECISION_SNAPSHOT_DIR)
        report_date = _decision_snapshot_date(snapshot_path)
    else:
        report_date = _parse_date(as_of) if as_of else date.today()
    source_json = reader_brief_json_path or default_reader_brief_json_path(
        reports_dir,
        report_date,
    )
    source_html = reader_brief_html_path or default_reader_brief_html_path(
        reports_dir,
        report_date,
    )
    if not source_json.exists():
        raise typer.BadParameter(f"Reader Brief JSON not found: {source_json}")
    raw = json.loads(source_json.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise typer.BadParameter(f"Reader Brief JSON must be an object: {source_json}")
    payload = build_reader_brief_quality_payload(
        reader_brief_payload=raw,
        reader_brief_json_path=source_json,
        reader_brief_html_path=source_html,
    )
    quality_json = json_output_path or default_reader_brief_quality_json_path(
        reports_dir,
        report_date,
    )
    quality_md = markdown_output_path or default_reader_brief_quality_markdown_path(
        reports_dir,
        report_date,
    )
    json_path = write_reader_brief_quality_json(payload, quality_json)
    md_path = write_reader_brief_quality_markdown(payload, quality_md)
    style = "green" if payload["status"] == "OK" else "yellow"
    console.print(f"[{style}]Reader Brief quality：{payload['status']}[/{style}]")
    console.print(f"Reader Brief quality JSON：{json_path}")
    console.print(f"Reader Brief quality Markdown：{md_path}")
    console.print(
        f"checks：{payload['summary']['check_count']}；"
        f"failed：{payload['summary']['failed_check_count']}；"
        f"production_effect={payload['production_effect']}；只读校验"
    )


@reports_app.command("reader-brief-consistency")
def reader_brief_consistency_command(
    as_of: Annotated[
        str | None,
        typer.Option(
            "--as-of",
            "--date",
            help="Reader Brief consistency 日期，格式为 YYYY-MM-DD。",
        ),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option(help="使用默认 decision snapshot 目录中的最新 signal-date。"),
    ] = False,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    report_index_path: Annotated[
        Path | None,
        typer.Option(help="Report index JSON 路径；不传时按日期使用默认路径。"),
    ] = None,
    project_root: Annotated[
        Path,
        typer.Option(help="用于解析相对 artifact path 的项目根目录。"),
    ] = PROJECT_ROOT,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Reader Brief consistency JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Reader Brief consistency Markdown 输出路径。"),
    ] = None,
) -> None:
    """生成 Reader Brief section consistency pack；只读扫描现有 report artifacts。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --as-of/--date 同时使用")
    if latest:
        report_date = _decision_snapshot_date(
            _latest_decision_snapshot_path(DEFAULT_DECISION_SNAPSHOT_DIR)
        )
    else:
        report_date = _parse_date(as_of) if as_of else date.today()
    source_index = report_index_path or default_report_index_json_path(reports_dir, report_date)
    try:
        raw_index = json.loads(source_index.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise typer.BadParameter(f"report index JSON not found: {source_index}") from exc
    except json.JSONDecodeError as exc:
        raise typer.BadParameter(f"report index JSON cannot be parsed: {source_index}") from exc
    if not isinstance(raw_index, dict):
        raise typer.BadParameter(f"report index JSON must be an object: {source_index}")
    payload = build_reader_brief_consistency_payload(
        as_of=report_date,
        report_index_payload=raw_index,
        report_index_path=source_index,
        project_root=project_root,
    )
    consistency_json = json_output_path or default_reader_brief_consistency_json_path(
        reports_dir,
        report_date,
    )
    consistency_md = markdown_output_path or default_reader_brief_consistency_markdown_path(
        reports_dir,
        report_date,
    )
    json_path = write_reader_brief_consistency_json(payload, consistency_json)
    md_path = write_reader_brief_consistency_markdown(payload, consistency_md)
    style = "green" if payload["consistency_status"] == "PASS" else "yellow"
    if payload["consistency_status"] == "FAIL":
        style = "red"
    summary = payload["summary"]
    console.print(f"[{style}]Reader Brief consistency：{payload['consistency_status']}[/{style}]")
    console.print(f"Reader Brief consistency JSON：{json_path}")
    console.print(f"Reader Brief consistency Markdown：{md_path}")
    console.print(
        f"reports：{summary['checked_report_count']}；"
        f"missing_sections：{summary['missing_section_count']}；"
        f"unclear_decisions：{summary['unclear_decision_count']}；"
        f"production_effect={payload['production_effect']}；只读一致性检查"
    )
    if payload["consistency_status"] == "FAIL":
        raise typer.Exit(code=1)


@reports_app.command("validate-reader-brief-consistency")
def validate_reader_brief_consistency_command(
    latest: Annotated[
        bool,
        typer.Option(help="校验 reports_dir 中最新 Reader Brief consistency JSON。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Reader Brief consistency validation 日期。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    source_json_path: Annotated[
        Path | None,
        typer.Option(help="Reader Brief consistency JSON 路径；优先级高于 --latest/--as-of。"),
    ] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Reader Brief consistency validation JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Reader Brief consistency validation Markdown 输出路径。"),
    ] = None,
) -> None:
    """校验 Reader Brief consistency pack；daily Reader Brief core section 缺失时 fail closed。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --as-of/--date 同时使用")
    if source_json_path is not None:
        source_path = source_json_path
    elif latest:
        latest_path = latest_reader_brief_consistency_json_path(reports_dir)
        if latest_path is None:
            raise typer.BadParameter(f"未找到 Reader Brief consistency JSON：{reports_dir}")
        source_path = latest_path
    else:
        report_date = _parse_date(as_of) if as_of else date.today()
        source_path = default_reader_brief_consistency_json_path(reports_dir, report_date)
    if not source_path.exists():
        raise typer.BadParameter(f"Reader Brief consistency JSON not found: {source_path}")
    try:
        raw_payload = json.loads(source_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise typer.BadParameter(
            f"Reader Brief consistency JSON cannot be parsed: {source_path}"
        ) from exc
    if not isinstance(raw_payload, dict):
        raise typer.BadParameter(f"Reader Brief consistency JSON must be an object: {source_path}")
    payload = validate_reader_brief_consistency_payload(raw_payload)
    source_artifacts = dict(payload.get("input_artifacts", {}))
    source_artifacts["reader_brief_consistency_pack"] = str(source_path)
    payload["input_artifacts"] = source_artifacts
    report_date = _parse_date(str(payload.get("as_of") or date.today().isoformat()))
    validation_json = json_output_path or default_reader_brief_consistency_validation_json_path(
        reports_dir,
        report_date,
    )
    validation_md = (
        markdown_output_path
        or default_reader_brief_consistency_validation_markdown_path(reports_dir, report_date)
    )
    json_path = write_reader_brief_consistency_validation_json(payload, validation_json)
    md_path = write_reader_brief_consistency_validation_markdown(payload, validation_md)
    style = "green" if payload["validation_status"] == "PASS" else "yellow"
    if payload["validation_status"] == "FAIL":
        style = "red"
    summary = payload["summary"]
    console.print(
        f"[{style}]Reader Brief consistency validation：{payload['validation_status']}[/{style}]"
    )
    console.print(f"Reader Brief consistency validation JSON：{json_path}")
    console.print(f"Reader Brief consistency validation Markdown：{md_path}")
    console.print(
        f"checks：{summary['check_count']}；"
        f"failed：{summary['failed_check_count']}；"
        f"warnings：{summary['warning_check_count']}；"
        f"production_effect={payload['production_effect']}；只读校验"
    )
    if payload["validation_status"] == "FAIL":
        raise typer.Exit(code=1)


@reports_app.command("production-boundary-static-scan")
def production_boundary_static_scan_command(
    as_of: Annotated[
        str | None,
        typer.Option(
            "--as-of",
            "--date",
            help="Production boundary static scan 日期，格式为 YYYY-MM-DD。",
        ),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    project_root: Annotated[
        Path,
        typer.Option(help="用于解析默认 scan roots 的项目根目录。"),
    ] = PROJECT_ROOT,
    allowlist_path: Annotated[
        Path,
        typer.Option(help="Production boundary static scan allowlist YAML。"),
    ] = DEFAULT_ALLOWLIST_PATH,
    scan_root: Annotated[
        list[Path] | None,
        typer.Option("--scan-root", help="可重复指定扫描根目录；不传时扫描默认 roots。"),
    ] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Production boundary static scan JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Production boundary static scan Markdown 输出路径。"),
    ] = None,
) -> None:
    """生成 production boundary static scan；只读扫描 source/config/docs。"""
    report_date = _parse_date(as_of) if as_of else date.today()
    payload = build_production_boundary_static_scan_payload(
        as_of=report_date,
        project_root=project_root,
        scan_roots=scan_root,
        allowlist_path=allowlist_path,
    )
    scan_json = json_output_path or default_production_boundary_static_scan_json_path(
        reports_dir,
        report_date,
    )
    scan_md = markdown_output_path or default_production_boundary_static_scan_markdown_path(
        reports_dir,
        report_date,
    )
    json_path = write_production_boundary_static_scan_json(payload, scan_json)
    md_path = write_production_boundary_static_scan_markdown(payload, scan_md)
    status = payload["scan_status"]
    style = "green" if status == "OK" else "yellow"
    if status == "BLOCKING":
        style = "red"
    summary = payload["summary"]
    console.print(f"[{style}]Production boundary static scan：{status}[/{style}]")
    console.print(f"Production boundary static scan JSON：{json_path}")
    console.print(f"Production boundary static scan Markdown：{md_path}")
    console.print(
        f"files：{summary['scanned_file_count']}；"
        f"findings：{summary['finding_count']}；"
        f"blocking：{summary['blocking_finding_count']}；"
        f"warnings：{summary['warning_finding_count']}；"
        f"allowed：{summary['allowed_match_count']}；"
        f"production_effect={payload['production_effect']}；只读 static scan"
    )
    if status == "BLOCKING":
        raise typer.Exit(code=1)


@reports_app.command("validate-production-boundary-static-scan")
def validate_production_boundary_static_scan_command(
    latest: Annotated[
        bool,
        typer.Option(help="校验 reports_dir 中最新 production boundary static scan JSON。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Production boundary static scan validation 日期。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    source_json_path: Annotated[
        Path | None,
        typer.Option(
            help="Production boundary static scan JSON 路径；优先级高于 --latest/--as-of。"
        ),
    ] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Production boundary static scan validation JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Production boundary static scan validation Markdown 输出路径。"),
    ] = None,
) -> None:
    """校验 production boundary static scan；blocking finding 时 fail closed。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --as-of/--date 同时使用")
    if source_json_path is not None:
        source_path = source_json_path
    elif latest:
        latest_path = latest_production_boundary_static_scan_json_path(reports_dir)
        if latest_path is None:
            raise typer.BadParameter(f"未找到 production boundary static scan JSON：{reports_dir}")
        source_path = latest_path
    else:
        report_date = _parse_date(as_of) if as_of else date.today()
        source_path = default_production_boundary_static_scan_json_path(reports_dir, report_date)
    if not source_path.exists():
        raise typer.BadParameter(f"Production boundary static scan JSON not found: {source_path}")
    try:
        raw_payload = json.loads(source_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise typer.BadParameter(
            f"Production boundary static scan JSON cannot be parsed: {source_path}"
        ) from exc
    if not isinstance(raw_payload, dict):
        raise typer.BadParameter(
            f"Production boundary static scan JSON must be an object: {source_path}"
        )
    payload = validate_production_boundary_static_scan_payload(raw_payload)
    source_artifacts = dict(payload.get("input_artifacts", {}))
    source_artifacts["production_boundary_static_scan"] = str(source_path)
    payload["input_artifacts"] = source_artifacts
    report_date = _parse_date(str(payload.get("as_of") or date.today().isoformat()))
    validation_json = (
        json_output_path
        or default_production_boundary_static_scan_validation_json_path(reports_dir, report_date)
    )
    validation_md = (
        markdown_output_path
        or default_production_boundary_static_scan_validation_markdown_path(
            reports_dir,
            report_date,
        )
    )
    json_path = write_production_boundary_static_scan_validation_json(payload, validation_json)
    md_path = write_production_boundary_static_scan_validation_markdown(payload, validation_md)
    status = payload["validation_status"]
    style = "green" if status == "OK" else "yellow"
    if status == "BLOCKING":
        style = "red"
    summary = payload["summary"]
    console.print(f"[{style}]Production boundary static scan validation：{status}[/{style}]")
    console.print(f"Production boundary static scan validation JSON：{json_path}")
    console.print(f"Production boundary static scan validation Markdown：{md_path}")
    console.print(
        f"checks：{summary['check_count']}；"
        f"failed：{summary['failed_check_count']}；"
        f"warnings：{summary['warning_check_count']}；"
        f"production_effect={payload['production_effect']}；只读校验"
    )
    if status == "BLOCKING":
        raise typer.Exit(code=1)


@reports_app.command("owner-review-template-v2")
def owner_review_template_v2_command(
    as_of: Annotated[
        str | None,
        typer.Option(
            "--as-of",
            "--date",
            help="Owner review template v2 日期，格式为 YYYY-MM-DD。",
        ),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Owner review template v2 JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Owner review template v2 Markdown 输出路径。"),
    ] = None,
) -> None:
    """生成 owner review template v2；只读输出 manual-review contract。"""
    report_date = _parse_date(as_of) if as_of else date.today()
    payload = build_owner_review_template_v2_payload(as_of=report_date)
    template_json = json_output_path or default_owner_review_template_v2_json_path(
        reports_dir,
        report_date,
    )
    template_md = markdown_output_path or default_owner_review_template_v2_markdown_path(
        reports_dir,
        report_date,
    )
    json_path = write_owner_review_template_v2_json(payload, template_json)
    md_path = write_owner_review_template_v2_markdown(payload, template_md)
    summary = payload["summary"]
    console.print(f"[green]Owner review template v2：{payload['template_status']}[/green]")
    console.print(f"Owner review template v2 JSON：{json_path}")
    console.print(f"Owner review template v2 Markdown：{md_path}")
    console.print(
        f"fields：{summary['required_field_count']}；"
        f"owner_actions：{summary['owner_action_count']}；"
        f"production_effect={payload['production_effect']}；manual-review-only"
    )


@reports_app.command("validate-owner-review-template-v2")
def validate_owner_review_template_v2_command(
    latest: Annotated[
        bool,
        typer.Option(help="校验 reports_dir 中最新 owner review template v2 JSON。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Owner review template v2 validation 日期。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    source_json_path: Annotated[
        Path | None,
        typer.Option(help="Owner review template v2 JSON 路径；优先级高于 --latest/--as-of。"),
    ] = None,
    review_json_path: Annotated[
        Path | None,
        typer.Option(help="可选：已填写 owner review JSON；提供时按 v2 contract 校验。"),
    ] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Owner review template v2 validation JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Owner review template v2 validation Markdown 输出路径。"),
    ] = None,
) -> None:
    """校验 owner review template v2；可选校验 filled review JSON。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --as-of/--date 同时使用")
    if source_json_path is not None:
        source_path = source_json_path
    elif latest:
        latest_path = latest_owner_review_template_v2_json_path(reports_dir)
        if latest_path is None:
            raise typer.BadParameter(f"未找到 owner review template v2 JSON：{reports_dir}")
        source_path = latest_path
    else:
        report_date = _parse_date(as_of) if as_of else date.today()
        source_path = default_owner_review_template_v2_json_path(reports_dir, report_date)
    if not source_path.exists():
        raise typer.BadParameter(f"Owner review template v2 JSON not found: {source_path}")
    try:
        raw_payload = json.loads(source_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise typer.BadParameter(
            f"Owner review template v2 JSON cannot be parsed: {source_path}"
        ) from exc
    if not isinstance(raw_payload, dict):
        raise typer.BadParameter(f"Owner review template v2 JSON must be an object: {source_path}")

    review_payload = None
    if review_json_path is not None:
        if not review_json_path.exists():
            raise typer.BadParameter(f"Filled owner review JSON not found: {review_json_path}")
        try:
            review_payload = json.loads(review_json_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise typer.BadParameter(
                f"Filled owner review JSON cannot be parsed: {review_json_path}"
            ) from exc
        if not isinstance(review_payload, dict):
            raise typer.BadParameter(
                f"Filled owner review JSON must be an object: {review_json_path}"
            )

    payload = validate_owner_review_template_v2_payload(
        raw_payload,
        review_record=review_payload,
        review_record_path=review_json_path,
    )
    source_artifacts = dict(payload.get("input_artifacts", {}))
    source_artifacts["owner_review_template_v2"] = str(source_path)
    if review_json_path is not None:
        source_artifacts["filled_owner_review"] = str(review_json_path)
    payload["input_artifacts"] = source_artifacts
    report_date = _parse_date(str(payload.get("as_of") or date.today().isoformat()))
    validation_json = (
        json_output_path
        or default_owner_review_template_v2_validation_json_path(reports_dir, report_date)
    )
    validation_md = (
        markdown_output_path
        or default_owner_review_template_v2_validation_markdown_path(
            reports_dir,
            report_date,
        )
    )
    json_path = write_owner_review_template_v2_validation_json(payload, validation_json)
    md_path = write_owner_review_template_v2_validation_markdown(payload, validation_md)
    status = payload["validation_status"]
    style = "green" if status == "PASS" else "red"
    summary = payload["summary"]
    console.print(f"[{style}]Owner review template v2 validation：{status}[/{style}]")
    console.print(f"Owner review template v2 validation JSON：{json_path}")
    console.print(f"Owner review template v2 validation Markdown：{md_path}")
    console.print(
        f"checks：{summary['check_count']}；"
        f"failed：{summary['failed_check_count']}；"
        f"review_record_provided：{summary['review_record_provided']}；"
        f"production_effect={payload['production_effect']}；只读校验"
    )
    if status == "FAIL":
        raise typer.Exit(code=1)


@owner_decision_audit_log_app.command("append")
def owner_decision_audit_log_append_command(
    decision_json_path: Annotated[
        Path,
        typer.Option(help="已填写 owner decision 或 owner review JSON 路径。"),
    ],
    log_path: Annotated[
        Path,
        typer.Option(help="Append-only owner decision audit JSONL 路径。"),
    ] = DEFAULT_OWNER_DECISION_AUDIT_LOG_PATH,
) -> None:
    """向 owner decision audit log 追加一条治理记录；不重写既有 JSONL。"""
    if not decision_json_path.exists():
        raise typer.BadParameter(f"Owner decision JSON not found: {decision_json_path}")
    try:
        raw_record = json.loads(decision_json_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise typer.BadParameter(
            f"Owner decision JSON cannot be parsed: {decision_json_path}"
        ) from exc
    if not isinstance(raw_record, dict):
        raise typer.BadParameter(f"Owner decision JSON must be an object: {decision_json_path}")
    try:
        record = append_owner_decision_record(
            raw_record,
            log_path=log_path,
            source_record_path=decision_json_path,
        )
    except OwnerDecisionAuditLogError as exc:
        raise typer.BadParameter(str(exc)) from exc

    console.print("[green]Owner decision audit log append：PASS[/green]")
    console.print(f"Owner decision audit log JSONL：{log_path}")
    console.print(f"decision_id：{record['decision_id']}")
    console.print(
        f"candidate_id：{record['candidate_id']}；"
        f"owner_action：{record['owner_action']}；"
        f"safety_status：{record['safety_status']}；"
        f"production_effect={record['production_effect']}；append-only governance log"
    )


@owner_decision_audit_log_app.command("report")
def owner_decision_audit_log_report_command(
    as_of: Annotated[
        str | None,
        typer.Option(
            "--as-of",
            "--date",
            help="Owner decision audit log report 日期，格式为 YYYY-MM-DD。",
        ),
    ] = None,
    log_path: Annotated[
        Path,
        typer.Option(help="Append-only owner decision audit JSONL 路径。"),
    ] = DEFAULT_OWNER_DECISION_AUDIT_LOG_PATH,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Owner decision audit log JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Owner decision audit log Markdown 输出路径。"),
    ] = None,
) -> None:
    """生成 owner decision audit log report；只读读取 append-only JSONL。"""
    report_date = _parse_date(as_of) if as_of else date.today()
    payload = build_owner_decision_audit_log_payload(
        as_of=report_date,
        log_path=log_path,
    )
    report_json = json_output_path or default_owner_decision_audit_log_json_path(
        reports_dir,
        report_date,
    )
    report_md = markdown_output_path or default_owner_decision_audit_log_markdown_path(
        reports_dir,
        report_date,
    )
    json_path = write_owner_decision_audit_log_json(payload, report_json)
    md_path = write_owner_decision_audit_log_markdown(payload, report_md)
    status = payload["audit_log_status"]
    style = "green" if status == "AUDIT_LOG_PASS" else "yellow"
    if status == "AUDIT_LOG_BLOCKED":
        style = "red"
    summary = payload["summary"]
    console.print(f"[{style}]Owner decision audit log：{status}[/{style}]")
    console.print(f"Owner decision audit log JSON：{json_path}")
    console.print(f"Owner decision audit log Markdown：{md_path}")
    console.print(
        f"records：{summary['included_record_count']}；"
        f"latest_decision_id：{summary['latest_decision_id']}；"
        f"monthly_input：{summary['monthly_review_pack_input']}；"
        f"promotion_input：{summary['promotion_board_input']}；"
        f"production_effect={payload['production_effect']}；只读 report"
    )


@owner_decision_audit_log_app.command("validate")
def validate_owner_decision_audit_log_command(
    latest: Annotated[
        bool,
        typer.Option(help="校验 reports_dir 中最新 owner decision audit log report JSON。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Owner decision audit log validation 日期。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    source_json_path: Annotated[
        Path | None,
        typer.Option(
            help="Owner decision audit log report JSON 路径；优先级高于 --latest/--as-of。",
        ),
    ] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Owner decision audit log validation JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Owner decision audit log validation Markdown 输出路径。"),
    ] = None,
) -> None:
    """校验 owner decision audit log report 和 append-only governance boundary。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --as-of/--date 同时使用")
    if source_json_path is not None:
        source_path = source_json_path
    elif latest:
        latest_path = latest_owner_decision_audit_log_json_path(reports_dir)
        if latest_path is None:
            raise typer.BadParameter(f"未找到 owner decision audit log JSON：{reports_dir}")
        source_path = latest_path
    else:
        report_date = _parse_date(as_of) if as_of else date.today()
        source_path = default_owner_decision_audit_log_json_path(reports_dir, report_date)
    if not source_path.exists():
        raise typer.BadParameter(f"Owner decision audit log JSON not found: {source_path}")
    try:
        raw_payload = json.loads(source_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise typer.BadParameter(
            f"Owner decision audit log JSON cannot be parsed: {source_path}"
        ) from exc
    if not isinstance(raw_payload, dict):
        raise typer.BadParameter(f"Owner decision audit log JSON must be an object: {source_path}")

    payload = validate_owner_decision_audit_log_payload(raw_payload)
    source_artifacts = dict(payload.get("input_artifacts", {}))
    source_artifacts["owner_decision_audit_log_report"] = str(source_path)
    payload["input_artifacts"] = source_artifacts
    report_date = _parse_date(str(payload.get("as_of") or date.today().isoformat()))
    validation_json = (
        json_output_path
        or default_owner_decision_audit_log_validation_json_path(reports_dir, report_date)
    )
    validation_md = (
        markdown_output_path
        or default_owner_decision_audit_log_validation_markdown_path(
            reports_dir,
            report_date,
        )
    )
    json_path = write_owner_decision_audit_log_validation_json(payload, validation_json)
    md_path = write_owner_decision_audit_log_validation_markdown(payload, validation_md)
    status = payload["validation_status"]
    style = "green" if status == "PASS" else "red"
    summary = payload["summary"]
    console.print(f"[{style}]Owner decision audit log validation：{status}[/{style}]")
    console.print(f"Owner decision audit log validation JSON：{json_path}")
    console.print(f"Owner decision audit log validation Markdown：{md_path}")
    console.print(
        f"checks：{summary['check_count']}；"
        f"failed：{summary['failed_check_count']}；"
        f"source_records：{summary['source_record_count']}；"
        f"production_effect={payload['production_effect']}；只读校验"
    )
    if status == "FAIL":
        raise typer.Exit(code=1)


@reports_app.command("research-monthly-review-pack")
def research_monthly_review_pack_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Research monthly review pack 日期。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option(help="使用 reports_dir 中最新 report_index JSON。"),
    ] = False,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    report_index_path: Annotated[
        Path | None,
        typer.Option(help="Report index JSON 路径；不传时按日期使用默认路径。"),
    ] = None,
    project_root: Annotated[
        Path,
        typer.Option(help="用于解析相对 artifact path 的项目根目录。"),
    ] = PROJECT_ROOT,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Research monthly review pack JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Research monthly review pack Markdown 输出路径。"),
    ] = None,
) -> None:
    """生成 monthly research governance review pack；只读聚合既有 report artifacts。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --as-of/--date 同时使用")
    if latest and report_index_path is None:
        latest_index = max(
            reports_dir.glob("report_index_????-??-??.json"),
            default=None,
            key=lambda path: path.name,
        )
        if latest_index is None:
            raise typer.BadParameter(f"未找到 report index JSON：{reports_dir}")
        source_index = latest_index
    else:
        report_date = _parse_date(as_of) if as_of else date.today()
        source_index = report_index_path or default_report_index_json_path(
            reports_dir,
            report_date,
        )
    try:
        raw_index = json.loads(source_index.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise typer.BadParameter(f"report index JSON not found: {source_index}") from exc
    except json.JSONDecodeError as exc:
        raise typer.BadParameter(f"report index JSON cannot be parsed: {source_index}") from exc
    if not isinstance(raw_index, dict):
        raise typer.BadParameter(f"report index JSON must be an object: {source_index}")
    report_date = _parse_date(
        as_of or str(raw_index.get("as_of") or date.today().isoformat())
    )
    payload = build_research_monthly_review_pack_payload(
        as_of=report_date,
        report_index_payload=raw_index,
        report_index_path=source_index,
        project_root=project_root,
    )
    report_json = json_output_path or default_research_monthly_review_pack_json_path(
        reports_dir,
        report_date,
    )
    report_md = markdown_output_path or default_research_monthly_review_pack_markdown_path(
        reports_dir,
        report_date,
    )
    json_path = write_research_monthly_review_pack_json(payload, report_json)
    md_path = write_research_monthly_review_pack_markdown(payload, report_md)
    status = payload["monthly_review_status"]
    style = "green" if status == "MONTHLY_REVIEW_READY" else "yellow"
    if status == "MONTHLY_REVIEW_BLOCKED":
        style = "red"
    summary = payload["summary"]
    console.print(f"[{style}]Research monthly review pack：{status}[/{style}]")
    console.print(f"Research monthly review pack JSON：{json_path}")
    console.print(f"Research monthly review pack Markdown：{md_path}")
    console.print(
        f"active_candidates：{summary['active_candidate_count']}；"
        f"needs_evidence：{summary['needs_evidence_candidate_count']}；"
        f"blockers：{summary['major_blocker_count']}；"
        f"warnings：{summary['major_warning_count']}；"
        f"safety：{summary['safety_audit_status']}；"
        f"data_governance：{summary['data_governance_status']}；"
        f"production_effect={payload['production_effect']}；只读 monthly governance pack"
    )


@reports_app.command("validate-research-monthly-review-pack")
def validate_research_monthly_review_pack_command(
    latest: Annotated[
        bool,
        typer.Option(help="校验 reports_dir 中最新 research monthly review pack JSON。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Research monthly review pack validation 日期。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    source_json_path: Annotated[
        Path | None,
        typer.Option(
            help="Research monthly review pack JSON 路径；优先级高于 --latest/--as-of。"
        ),
    ] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Research monthly review pack validation JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Research monthly review pack validation Markdown 输出路径。"),
    ] = None,
) -> None:
    """校验 monthly research review pack；schema/source/safety drift 时 fail closed。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --as-of/--date 同时使用")
    if source_json_path is not None:
        source_path = source_json_path
    elif latest:
        latest_path = latest_research_monthly_review_pack_json_path(reports_dir)
        if latest_path is None:
            raise typer.BadParameter(
                f"未找到 research monthly review pack JSON：{reports_dir}"
            )
        source_path = latest_path
    else:
        report_date = _parse_date(as_of) if as_of else date.today()
        source_path = default_research_monthly_review_pack_json_path(
            reports_dir,
            report_date,
        )
    if not source_path.exists():
        raise typer.BadParameter(f"Research monthly review pack JSON not found: {source_path}")
    try:
        raw_payload = json.loads(source_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise typer.BadParameter(
            f"Research monthly review pack JSON cannot be parsed: {source_path}"
        ) from exc
    if not isinstance(raw_payload, dict):
        raise typer.BadParameter(
            f"Research monthly review pack JSON must be an object: {source_path}"
        )
    payload = validate_research_monthly_review_pack_payload(raw_payload)
    source_artifacts = dict(payload.get("input_artifacts", {}))
    source_artifacts["research_monthly_review_pack"] = str(source_path)
    payload["input_artifacts"] = source_artifacts
    report_date = _parse_date(str(payload.get("as_of") or date.today().isoformat()))
    validation_json = json_output_path or default_research_monthly_review_pack_validation_json_path(
        reports_dir,
        report_date,
    )
    validation_md = (
        markdown_output_path
        or default_research_monthly_review_pack_validation_markdown_path(
            reports_dir,
            report_date,
        )
    )
    json_path = write_research_monthly_review_pack_validation_json(payload, validation_json)
    md_path = write_research_monthly_review_pack_validation_markdown(payload, validation_md)
    status = payload["validation_status"]
    style = "green" if status == "PASS" else "yellow"
    if status == "FAIL":
        style = "red"
    summary = payload["summary"]
    console.print(f"[{style}]Research monthly review pack validation：{status}[/{style}]")
    console.print(f"Research monthly review pack validation JSON：{json_path}")
    console.print(f"Research monthly review pack validation Markdown：{md_path}")
    console.print(
        f"checks：{summary['check_count']}；"
        f"failed：{summary['failed_check_count']}；"
        f"source_blockers：{summary['source_major_blocker_count']}；"
        f"source_warnings：{summary['source_major_warning_count']}；"
        f"production_effect={payload['production_effect']}；只读校验"
    )
    if status == "FAIL":
        raise typer.Exit(code=1)


@reports_app.command("paper-shadow-promotion-board")
def paper_shadow_promotion_board_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Paper-shadow promotion board 日期。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option(help="使用 reports_dir 中最新 report_index JSON。"),
    ] = False,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    report_index_path: Annotated[
        Path | None,
        typer.Option(help="Report index JSON 路径；不传时按日期使用默认路径。"),
    ] = None,
    project_root: Annotated[
        Path,
        typer.Option(help="用于解析相对 artifact path 的项目根目录。"),
    ] = PROJECT_ROOT,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Paper-shadow promotion board JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Paper-shadow promotion board Markdown 输出路径。"),
    ] = None,
) -> None:
    """生成 paper-shadow-only promotion board；不推进 live trading。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --as-of/--date 同时使用")
    if latest and report_index_path is None:
        latest_index = max(
            reports_dir.glob("report_index_????-??-??.json"),
            default=None,
            key=lambda path: path.name,
        )
        if latest_index is None:
            raise typer.BadParameter(f"未找到 report index JSON：{reports_dir}")
        source_index = latest_index
    else:
        report_date = _parse_date(as_of) if as_of else date.today()
        source_index = report_index_path or default_report_index_json_path(
            reports_dir,
            report_date,
        )
    try:
        raw_index = json.loads(source_index.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise typer.BadParameter(f"report index JSON not found: {source_index}") from exc
    except json.JSONDecodeError as exc:
        raise typer.BadParameter(f"report index JSON cannot be parsed: {source_index}") from exc
    if not isinstance(raw_index, dict):
        raise typer.BadParameter(f"report index JSON must be an object: {source_index}")
    report_date = _parse_date(
        as_of or str(raw_index.get("as_of") or date.today().isoformat())
    )
    payload = build_paper_shadow_promotion_board_payload(
        as_of=report_date,
        report_index_payload=raw_index,
        report_index_path=source_index,
        project_root=project_root,
    )
    board_json = json_output_path or default_paper_shadow_promotion_board_json_path(
        reports_dir,
        report_date,
    )
    board_md = markdown_output_path or default_paper_shadow_promotion_board_markdown_path(
        reports_dir,
        report_date,
    )
    json_path = write_paper_shadow_promotion_board_json(payload, board_json)
    md_path = write_paper_shadow_promotion_board_markdown(payload, board_md)
    decision = payload["board_decision"]
    style = "green" if decision in {"EXTEND_SHADOW", "CONTINUE_NORMAL_SHADOW"} else "yellow"
    if decision in {"RETURN_TO_RESEARCH", "REJECT"}:
        style = "red"
    summary = payload["summary"]
    console.print(f"[{style}]Paper-shadow promotion board：{decision}[/{style}]")
    console.print(f"Paper-shadow promotion board JSON：{json_path}")
    console.print(f"Paper-shadow promotion board Markdown：{md_path}")
    console.print(
        f"candidate：{summary['candidate_id']}；"
        f"checks：{summary['evidence_check_count']}；"
        f"blocked：{summary['blocked_evidence_count']}；"
        f"warnings：{summary['warning_evidence_count']}；"
        f"safety：{summary['safety_status']}；"
        f"readiness：{summary['readiness_status']}；"
        f"production_effect={payload['production_effect']}；paper-shadow only"
    )


@reports_app.command("validate-paper-shadow-promotion-board")
def validate_paper_shadow_promotion_board_command(
    latest: Annotated[
        bool,
        typer.Option(help="校验 reports_dir 中最新 paper-shadow promotion board JSON。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Paper-shadow promotion board validation 日期。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    source_json_path: Annotated[
        Path | None,
        typer.Option(help="Paper-shadow promotion board JSON 路径；优先于 --latest/--as-of。"),
    ] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Paper-shadow promotion board validation JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Paper-shadow promotion board validation Markdown 输出路径。"),
    ] = None,
) -> None:
    """校验 paper-shadow promotion board；结构或安全边界漂移时 fail closed。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --as-of/--date 同时使用")
    if source_json_path is not None:
        source_path = source_json_path
    elif latest:
        latest_path = latest_paper_shadow_promotion_board_json_path(reports_dir)
        if latest_path is None:
            raise typer.BadParameter(f"未找到 paper-shadow promotion board JSON：{reports_dir}")
        source_path = latest_path
    else:
        report_date = _parse_date(as_of) if as_of else date.today()
        source_path = default_paper_shadow_promotion_board_json_path(
            reports_dir,
            report_date,
        )
    if not source_path.exists():
        raise typer.BadParameter(f"Paper-shadow promotion board JSON not found: {source_path}")
    try:
        raw_payload = json.loads(source_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise typer.BadParameter(
            f"Paper-shadow promotion board JSON cannot be parsed: {source_path}"
        ) from exc
    if not isinstance(raw_payload, dict):
        raise typer.BadParameter(
            f"Paper-shadow promotion board JSON must be an object: {source_path}"
        )
    payload = validate_paper_shadow_promotion_board_payload(raw_payload)
    source_artifacts = dict(payload.get("input_artifacts", {}))
    source_artifacts["paper_shadow_promotion_board"] = str(source_path)
    payload["input_artifacts"] = source_artifacts
    report_date = _parse_date(str(payload.get("as_of") or date.today().isoformat()))
    validation_json = json_output_path or default_paper_shadow_promotion_board_validation_json_path(
        reports_dir,
        report_date,
    )
    validation_md = (
        markdown_output_path
        or default_paper_shadow_promotion_board_validation_markdown_path(
            reports_dir,
            report_date,
        )
    )
    json_path = write_paper_shadow_promotion_board_validation_json(payload, validation_json)
    md_path = write_paper_shadow_promotion_board_validation_markdown(payload, validation_md)
    status = payload["validation_status"]
    style = "green" if status == "PASS" else "yellow"
    if status == "FAIL":
        style = "red"
    summary = payload["summary"]
    console.print(f"[{style}]Paper-shadow promotion board validation：{status}[/{style}]")
    console.print(f"Paper-shadow promotion board validation JSON：{json_path}")
    console.print(f"Paper-shadow promotion board validation Markdown：{md_path}")
    console.print(
        f"checks：{summary['check_count']}；"
        f"failed：{summary['failed_check_count']}；"
        f"source_blockers：{summary['source_blocker_count']}；"
        f"source_warnings：{summary['source_warning_count']}；"
        f"production_effect={payload['production_effect']}；只读校验"
    )
    if status == "FAIL":
        raise typer.Exit(code=1)


@reports_app.command("candidate-rejection-postmortem-template")
def candidate_rejection_postmortem_template_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Candidate rejection postmortem 日期。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option(help="使用 reports_dir 中最新 report_index JSON。"),
    ] = False,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    report_index_path: Annotated[
        Path | None,
        typer.Option(help="Report index JSON 路径；不传时按日期使用默认路径。"),
    ] = None,
    postmortem_json_path: Annotated[
        Path | None,
        typer.Option(help="可选已填写 rejection postmortem JSON；不传时只生成空模板。"),
    ] = None,
    project_root: Annotated[
        Path,
        typer.Option(help="用于解析相对 artifact path 的项目根目录。"),
    ] = PROJECT_ROOT,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Candidate rejection postmortem template JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Candidate rejection postmortem template Markdown 输出路径。"),
    ] = None,
) -> None:
    """生成 candidate rejection postmortem template；不拒绝候选或修改状态。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --as-of/--date 同时使用")
    if latest and report_index_path is None:
        latest_index = max(
            reports_dir.glob("report_index_????-??-??.json"),
            default=None,
            key=lambda path: path.name,
        )
        if latest_index is None:
            raise typer.BadParameter(f"未找到 report index JSON：{reports_dir}")
        source_index = latest_index
    else:
        report_date = _parse_date(as_of) if as_of else date.today()
        source_index = report_index_path or default_report_index_json_path(
            reports_dir,
            report_date,
        )
    try:
        raw_index = json.loads(source_index.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise typer.BadParameter(f"report index JSON not found: {source_index}") from exc
    except json.JSONDecodeError as exc:
        raise typer.BadParameter(f"report index JSON cannot be parsed: {source_index}") from exc
    if not isinstance(raw_index, dict):
        raise typer.BadParameter(f"report index JSON must be an object: {source_index}")
    report_date = _parse_date(
        as_of or str(raw_index.get("as_of") or date.today().isoformat())
    )
    payload = build_candidate_rejection_postmortem_payload(
        as_of=report_date,
        report_index_payload=raw_index,
        report_index_path=source_index,
        postmortem_json_path=postmortem_json_path,
        project_root=project_root,
    )
    report_json = json_output_path or default_candidate_rejection_postmortem_json_path(
        reports_dir,
        report_date,
    )
    report_md = markdown_output_path or default_candidate_rejection_postmortem_markdown_path(
        reports_dir,
        report_date,
    )
    json_path = write_candidate_rejection_postmortem_json(payload, report_json)
    md_path = write_candidate_rejection_postmortem_markdown(payload, report_md)
    status = payload["template_status"]
    style = "green" if status == "TEMPLATE_READY" else "red"
    summary = payload["summary"]
    console.print(f"[{style}]Candidate rejection postmortem template：{status}[/{style}]")
    console.print(f"Candidate rejection postmortem template JSON：{json_path}")
    console.print(f"Candidate rejection postmortem template Markdown：{md_path}")
    console.print(
        f"candidate：{summary['candidate_id']}；"
        f"filled_status：{summary['filled_postmortem_status']}；"
        f"sections：{summary['required_section_count']}；"
        f"failed_gates：{summary['failed_evidence_gate_count']}；"
        f"stress_failures：{summary['failed_stress_scenario_count']}；"
        f"production_effect={payload['production_effect']}；只读模板"
    )


@reports_app.command("validate-candidate-rejection-postmortem-template")
def validate_candidate_rejection_postmortem_template_command(
    latest: Annotated[
        bool,
        typer.Option(help="校验 reports_dir 中最新 candidate rejection postmortem template JSON。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Candidate rejection postmortem validation 日期。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    source_json_path: Annotated[
        Path | None,
        typer.Option(help="Template JSON 路径；优先于 --latest/--as-of。"),
    ] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Candidate rejection postmortem validation JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Candidate rejection postmortem validation Markdown 输出路径。"),
    ] = None,
) -> None:
    """校验 candidate rejection postmortem template 和可选 filled record。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --as-of/--date 同时使用")
    if source_json_path is not None:
        source_path = source_json_path
    elif latest:
        latest_path = latest_candidate_rejection_postmortem_json_path(reports_dir)
        if latest_path is None:
            raise typer.BadParameter(f"未找到 candidate rejection postmortem JSON：{reports_dir}")
        source_path = latest_path
    else:
        report_date = _parse_date(as_of) if as_of else date.today()
        source_path = default_candidate_rejection_postmortem_json_path(
            reports_dir,
            report_date,
        )
    if not source_path.exists():
        raise typer.BadParameter(f"Candidate rejection postmortem JSON not found: {source_path}")
    try:
        raw_payload = json.loads(source_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise typer.BadParameter(
            f"Candidate rejection postmortem JSON cannot be parsed: {source_path}"
        ) from exc
    if not isinstance(raw_payload, dict):
        raise typer.BadParameter(
            f"Candidate rejection postmortem JSON must be an object: {source_path}"
        )
    payload = validate_candidate_rejection_postmortem_payload(raw_payload)
    source_artifacts = dict(payload.get("input_artifacts", {}))
    source_artifacts["candidate_rejection_postmortem_template"] = str(source_path)
    payload["input_artifacts"] = source_artifacts
    report_date = _parse_date(str(payload.get("as_of") or date.today().isoformat()))
    validation_json = (
        json_output_path
        or default_candidate_rejection_postmortem_validation_json_path(
            reports_dir,
            report_date,
        )
    )
    validation_md = (
        markdown_output_path
        or default_candidate_rejection_postmortem_validation_markdown_path(
            reports_dir,
            report_date,
        )
    )
    json_path = write_candidate_rejection_postmortem_validation_json(payload, validation_json)
    md_path = write_candidate_rejection_postmortem_validation_markdown(payload, validation_md)
    status = payload["validation_status"]
    style = "green" if status == "PASS" else "red"
    summary = payload["summary"]
    console.print(f"[{style}]Candidate rejection postmortem validation：{status}[/{style}]")
    console.print(f"Candidate rejection postmortem validation JSON：{json_path}")
    console.print(f"Candidate rejection postmortem validation Markdown：{md_path}")
    console.print(
        f"checks：{summary['check_count']}；"
        f"failed：{summary['failed_check_count']}；"
        f"filled_status：{summary['filled_postmortem_status']}；"
        f"production_effect={payload['production_effect']}；只读校验"
    )
    if status == "FAIL":
        raise typer.Exit(code=1)


@reports_app.command("decision-snapshot-lifecycle-policy")
def decision_snapshot_lifecycle_policy_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Decision snapshot lifecycle policy 日期。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option(help="使用 reports_dir 中最新 report_index JSON 的 as_of。"),
    ] = False,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    snapshot_dir: Annotated[
        Path,
        typer.Option(help="canonical decision snapshot 目录。"),
    ] = DEFAULT_DECISION_SNAPSHOT_DIR,
    decision_snapshot_path: Annotated[
        Path | None,
        typer.Option(help="显式 decision snapshot JSON 路径；不传时按 as_of 使用默认路径。"),
    ] = None,
    report_index_path: Annotated[
        Path | None,
        typer.Option(
            help="Report index JSON 路径；不传时按日期使用默认路径，缺失时只记录 MISSING。"
        ),
    ] = None,
    allow_latest_context: Annotated[
        bool,
        typer.Option(help="缺失当日 snapshot 时，允许显式标记为 latest-context non-blocking。"),
    ] = False,
    today: Annotated[
        str | None,
        typer.Option(help="测试/审计用 today override，格式 YYYY-MM-DD。"),
    ] = None,
    project_root: Annotated[
        Path,
        typer.Option(help="用于解析相对 artifact path 的项目根目录。"),
    ] = PROJECT_ROOT,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Decision snapshot lifecycle policy JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Decision snapshot lifecycle policy Markdown 输出路径。"),
    ] = None,
) -> None:
    """生成 decision snapshot lifecycle policy；不补造缺失 snapshot。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --as-of/--date 同时使用")
    source_index: Path | None = report_index_path
    if latest and source_index is None:
        source_index = max(
            reports_dir.glob("report_index_????-??-??.json"),
            default=None,
            key=lambda path: path.name,
        )
        if source_index is None:
            raise typer.BadParameter(f"未找到 report index JSON：{reports_dir}")
    if latest and source_index is not None:
        raw_index = _read_optional_json_object(source_index)
        report_date = _parse_date(str(raw_index.get("as_of") or date.today().isoformat()))
    else:
        report_date = _parse_date(as_of) if as_of else date.today()
        source_index = report_index_path or default_report_index_json_path(
            reports_dir,
            report_date,
        )
        raw_index = _read_optional_json_object(source_index)
    payload = build_decision_snapshot_lifecycle_policy_payload(
        as_of=report_date,
        decision_snapshot_path=decision_snapshot_path,
        snapshot_dir=snapshot_dir,
        report_index_payload=raw_index,
        report_index_path=source_index if source_index and source_index.exists() else None,
        allow_latest_context=allow_latest_context,
        today=_parse_date(today) if today else None,
        project_root=project_root,
    )
    report_json = json_output_path or default_decision_snapshot_lifecycle_policy_json_path(
        reports_dir,
        report_date,
    )
    report_md = markdown_output_path or default_decision_snapshot_lifecycle_policy_markdown_path(
        reports_dir,
        report_date,
    )
    json_path = write_decision_snapshot_lifecycle_policy_json(payload, report_json)
    md_path = write_decision_snapshot_lifecycle_policy_markdown(payload, report_md)
    status = payload["snapshot_lifecycle_status"]
    style = "green" if status == SNAPSHOT_AVAILABLE else "yellow"
    if status == "SNAPSHOT_MISSING_BLOCKING":
        style = "red"
    summary = payload["summary"]
    console.print(f"[{style}]Decision snapshot lifecycle policy：{status}[/{style}]")
    console.print(f"Decision snapshot lifecycle policy JSON：{json_path}")
    console.print(f"Decision snapshot lifecycle policy Markdown：{md_path}")
    console.print(
        f"target：{summary['target_as_of']}；"
        f"snapshot_exists：{summary['snapshot_exists']}；"
        f"latest：{summary['latest_available_snapshot_date']}；"
        f"blocking_impact：{summary['blocking_impact']}；"
        f"production_effect={payload['production_effect']}；read-only/no fabrication"
    )


@reports_app.command("validate-decision-snapshot-lifecycle-policy")
def validate_decision_snapshot_lifecycle_policy_command(
    latest: Annotated[
        bool,
        typer.Option(help="校验 reports_dir 中最新 decision snapshot lifecycle policy JSON。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Decision snapshot lifecycle validation 日期。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    source_json_path: Annotated[
        Path | None,
        typer.Option(
            help="Decision snapshot lifecycle policy JSON 路径；优先于 --latest/--as-of。"
        ),
    ] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Decision snapshot lifecycle validation JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Decision snapshot lifecycle validation Markdown 输出路径。"),
    ] = None,
) -> None:
    """校验 decision snapshot lifecycle policy；不补造缺失 snapshot。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --as-of/--date 同时使用")
    if source_json_path is not None:
        source_path = source_json_path
    elif latest:
        latest_path = latest_decision_snapshot_lifecycle_policy_json_path(reports_dir)
        if latest_path is None:
            raise typer.BadParameter(
                f"未找到 decision snapshot lifecycle policy JSON：{reports_dir}"
            )
        source_path = latest_path
    else:
        report_date = _parse_date(as_of) if as_of else date.today()
        source_path = default_decision_snapshot_lifecycle_policy_json_path(
            reports_dir,
            report_date,
        )
    if not source_path.exists():
        raise typer.BadParameter(
            f"Decision snapshot lifecycle policy JSON not found: {source_path}"
        )
    try:
        raw_payload = json.loads(source_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise typer.BadParameter(
            f"Decision snapshot lifecycle policy JSON cannot be parsed: {source_path}"
        ) from exc
    if not isinstance(raw_payload, dict):
        raise typer.BadParameter(
            f"Decision snapshot lifecycle policy JSON must be an object: {source_path}"
        )
    payload = validate_decision_snapshot_lifecycle_policy_payload(raw_payload)
    source_artifacts = dict(payload.get("input_artifacts", {}))
    source_artifacts["decision_snapshot_lifecycle_policy"] = str(source_path)
    payload["input_artifacts"] = source_artifacts
    report_date = _parse_date(str(payload.get("as_of") or date.today().isoformat()))
    validation_json = (
        json_output_path
        or default_decision_snapshot_lifecycle_policy_validation_json_path(
            reports_dir,
            report_date,
        )
    )
    validation_md = (
        markdown_output_path
        or default_decision_snapshot_lifecycle_policy_validation_markdown_path(
            reports_dir,
            report_date,
        )
    )
    json_path = write_decision_snapshot_lifecycle_policy_validation_json(
        payload,
        validation_json,
    )
    md_path = write_decision_snapshot_lifecycle_policy_validation_markdown(
        payload,
        validation_md,
    )
    status = payload["validation_status"]
    style = "green" if status == "PASS" else "yellow"
    if status == "FAIL":
        style = "red"
    summary = payload["summary"]
    console.print(f"[{style}]Decision snapshot lifecycle policy validation：{status}[/{style}]")
    console.print(f"Decision snapshot lifecycle policy validation JSON：{json_path}")
    console.print(f"Decision snapshot lifecycle policy validation Markdown：{md_path}")
    console.print(
        f"checks：{summary['check_count']}；"
        f"failed：{summary['failed_check_count']}；"
        f"source_status：{summary['source_snapshot_lifecycle_status']}；"
        f"snapshot_exists：{summary['snapshot_exists']}；"
        f"production_effect={payload['production_effect']}；只读校验"
    )
    if status == "FAIL":
        raise typer.Exit(code=1)


@reports_app.command("extended-shadow-observation-clock")
def extended_shadow_observation_clock_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Extended shadow observation clock 日期。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option(help="使用 reports_dir 中最新 report_index JSON。"),
    ] = False,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    report_index_path: Annotated[
        Path | None,
        typer.Option(help="Report index JSON 路径；不传时按日期使用默认路径。"),
    ] = None,
    project_root: Annotated[
        Path,
        typer.Option(help="用于解析相对 artifact path 的项目根目录。"),
    ] = PROJECT_ROOT,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Extended shadow observation clock JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Extended shadow observation clock Markdown 输出路径。"),
    ] = None,
) -> None:
    """生成 extended-shadow observation clock；不补造 observation days。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --as-of/--date 同时使用")
    if latest and report_index_path is None:
        latest_index = max(
            reports_dir.glob("report_index_????-??-??.json"),
            default=None,
            key=lambda path: path.name,
        )
        if latest_index is None:
            raise typer.BadParameter(f"未找到 report index JSON：{reports_dir}")
        source_index = latest_index
    else:
        report_date = _parse_date(as_of) if as_of else date.today()
        source_index = report_index_path or default_report_index_json_path(
            reports_dir,
            report_date,
        )
    try:
        raw_index = json.loads(source_index.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise typer.BadParameter(f"report index JSON not found: {source_index}") from exc
    except json.JSONDecodeError as exc:
        raise typer.BadParameter(f"report index JSON cannot be parsed: {source_index}") from exc
    if not isinstance(raw_index, dict):
        raise typer.BadParameter(f"report index JSON must be an object: {source_index}")
    report_date = _parse_date(
        as_of or str(raw_index.get("as_of") or date.today().isoformat())
    )
    payload = build_extended_shadow_observation_clock_payload(
        as_of=report_date,
        report_index_payload=raw_index,
        report_index_path=source_index,
        project_root=project_root,
    )
    report_json = json_output_path or default_extended_shadow_observation_clock_json_path(
        reports_dir,
        report_date,
    )
    report_md = markdown_output_path or default_extended_shadow_observation_clock_markdown_path(
        reports_dir,
        report_date,
    )
    json_path = write_extended_shadow_observation_clock_json(payload, report_json)
    md_path = write_extended_shadow_observation_clock_markdown(payload, report_md)
    status = payload["observation_clock_status"]
    style = "green" if status == "OBSERVATION_PERIOD_MET" else "yellow"
    summary = payload["summary"]
    console.print(f"[{style}]Extended shadow observation clock：{status}[/{style}]")
    console.print(f"Extended shadow observation clock JSON：{json_path}")
    console.print(f"Extended shadow observation clock Markdown：{md_path}")
    console.print(
        f"candidate：{summary['candidate_id']}；"
        f"current：{summary['current_count']}；"
        f"required：{summary['required_count']}；"
        f"missing：{summary['missing_day_count']}；"
        f"invalid：{summary['invalid_day_count']}；"
        f"production_effect={payload['production_effect']}；paper-shadow only"
    )


@reports_app.command("validate-extended-shadow-observation-clock")
def validate_extended_shadow_observation_clock_command(
    latest: Annotated[
        bool,
        typer.Option(help="校验 reports_dir 中最新 extended shadow observation clock JSON。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option(
            "--as-of",
            "--date",
            help="Extended shadow observation clock validation 日期。",
        ),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    source_json_path: Annotated[
        Path | None,
        typer.Option(help="Extended shadow observation clock JSON 路径；优先于 --latest/--as-of。"),
    ] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Extended shadow observation clock validation JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Extended shadow observation clock validation Markdown 输出路径。"),
    ] = None,
) -> None:
    """校验 extended-shadow observation clock；缺 observation days 时保持 fail closed。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --as-of/--date 同时使用")
    if source_json_path is not None:
        source_path = source_json_path
    elif latest:
        latest_path = latest_extended_shadow_observation_clock_json_path(reports_dir)
        if latest_path is None:
            raise typer.BadParameter(
                f"未找到 extended shadow observation clock JSON：{reports_dir}"
            )
        source_path = latest_path
    else:
        report_date = _parse_date(as_of) if as_of else date.today()
        source_path = default_extended_shadow_observation_clock_json_path(
            reports_dir,
            report_date,
        )
    if not source_path.exists():
        raise typer.BadParameter(
            f"Extended shadow observation clock JSON not found: {source_path}"
        )
    try:
        raw_payload = json.loads(source_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise typer.BadParameter(
            f"Extended shadow observation clock JSON cannot be parsed: {source_path}"
        ) from exc
    if not isinstance(raw_payload, dict):
        raise typer.BadParameter(
            f"Extended shadow observation clock JSON must be an object: {source_path}"
        )
    payload = validate_extended_shadow_observation_clock_payload(raw_payload)
    source_artifacts = dict(payload.get("input_artifacts", {}))
    source_artifacts["extended_shadow_observation_clock"] = str(source_path)
    payload["input_artifacts"] = source_artifacts
    report_date = _parse_date(str(payload.get("as_of") or date.today().isoformat()))
    validation_json = (
        json_output_path
        or default_extended_shadow_observation_clock_validation_json_path(
            reports_dir,
            report_date,
        )
    )
    validation_md = (
        markdown_output_path
        or default_extended_shadow_observation_clock_validation_markdown_path(
            reports_dir,
            report_date,
        )
    )
    json_path = write_extended_shadow_observation_clock_validation_json(
        payload,
        validation_json,
    )
    md_path = write_extended_shadow_observation_clock_validation_markdown(
        payload,
        validation_md,
    )
    status = payload["validation_status"]
    style = "green" if status == "PASS" else "yellow"
    if status == "FAIL":
        style = "red"
    summary = payload["summary"]
    console.print(f"[{style}]Extended shadow observation clock validation：{status}[/{style}]")
    console.print(f"Extended shadow observation clock validation JSON：{json_path}")
    console.print(f"Extended shadow observation clock validation Markdown：{md_path}")
    console.print(
        f"checks：{summary['check_count']}；"
        f"failed：{summary['failed_check_count']}；"
        f"current：{summary['current_count']}；"
        f"required：{summary['required_count']}；"
        f"production_effect={payload['production_effect']}；只读校验"
    )
    if status == "FAIL":
        raise typer.Exit(code=1)


@reports_app.command("extended-shadow-protocol")
def extended_shadow_protocol_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Extended shadow protocol 日期。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option(help="使用 reports_dir 中最新 report_index JSON。"),
    ] = False,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    report_index_path: Annotated[
        Path | None,
        typer.Option(help="Report index JSON 路径；不传时按日期使用默认路径。"),
    ] = None,
    project_root: Annotated[
        Path,
        typer.Option(help="用于解析相对 artifact path 的项目根目录。"),
    ] = PROJECT_ROOT,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Extended shadow protocol JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Extended shadow protocol Markdown 输出路径。"),
    ] = None,
) -> None:
    """生成 extended shadow protocol eligibility report；不推进 live trading。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --as-of/--date 同时使用")
    if latest and report_index_path is None:
        latest_index = max(
            reports_dir.glob("report_index_????-??-??.json"),
            default=None,
            key=lambda path: path.name,
        )
        if latest_index is None:
            raise typer.BadParameter(f"未找到 report index JSON：{reports_dir}")
        source_index = latest_index
    else:
        report_date = _parse_date(as_of) if as_of else date.today()
        source_index = report_index_path or default_report_index_json_path(
            reports_dir,
            report_date,
        )
    try:
        raw_index = json.loads(source_index.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise typer.BadParameter(f"report index JSON not found: {source_index}") from exc
    except json.JSONDecodeError as exc:
        raise typer.BadParameter(f"report index JSON cannot be parsed: {source_index}") from exc
    if not isinstance(raw_index, dict):
        raise typer.BadParameter(f"report index JSON must be an object: {source_index}")
    report_date = _parse_date(
        as_of or str(raw_index.get("as_of") or date.today().isoformat())
    )
    payload = build_extended_shadow_protocol_payload(
        as_of=report_date,
        report_index_payload=raw_index,
        report_index_path=source_index,
        project_root=project_root,
    )
    report_json = json_output_path or default_extended_shadow_protocol_json_path(
        reports_dir,
        report_date,
    )
    report_md = markdown_output_path or default_extended_shadow_protocol_markdown_path(
        reports_dir,
        report_date,
    )
    json_path = write_extended_shadow_protocol_json(payload, report_json)
    md_path = write_extended_shadow_protocol_markdown(payload, report_md)
    status = payload["eligibility_status"]
    style = "green" if status == "EXTENDED_SHADOW_ELIGIBLE" else "yellow"
    summary = payload["summary"]
    console.print(f"[{style}]Extended shadow protocol：{status}[/{style}]")
    console.print(f"Extended shadow protocol JSON：{json_path}")
    console.print(f"Extended shadow protocol Markdown：{md_path}")
    console.print(
        f"candidate：{summary['candidate_id']}；"
        f"observed_days：{summary['observed_trading_days']}；"
        f"minimum_days：{summary['minimum_observation_trading_days']}；"
        f"blocked：{summary['blocked_check_count']}；"
        f"warnings：{summary['warning_check_count']}；"
        f"safety：{summary['safety_status']}；"
        f"production_effect={payload['production_effect']}；paper-shadow only"
    )


@reports_app.command("validate-extended-shadow-protocol")
def validate_extended_shadow_protocol_command(
    latest: Annotated[
        bool,
        typer.Option(help="校验 reports_dir 中最新 extended shadow protocol JSON。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Extended shadow protocol validation 日期。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    source_json_path: Annotated[
        Path | None,
        typer.Option(help="Extended shadow protocol JSON 路径；优先于 --latest/--as-of。"),
    ] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Extended shadow protocol validation JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Extended shadow protocol validation Markdown 输出路径。"),
    ] = None,
) -> None:
    """校验 extended shadow protocol；结构或安全边界漂移时 fail closed。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --as-of/--date 同时使用")
    if source_json_path is not None:
        source_path = source_json_path
    elif latest:
        latest_path = latest_extended_shadow_protocol_json_path(reports_dir)
        if latest_path is None:
            raise typer.BadParameter(f"未找到 extended shadow protocol JSON：{reports_dir}")
        source_path = latest_path
    else:
        report_date = _parse_date(as_of) if as_of else date.today()
        source_path = default_extended_shadow_protocol_json_path(
            reports_dir,
            report_date,
        )
    if not source_path.exists():
        raise typer.BadParameter(f"Extended shadow protocol JSON not found: {source_path}")
    try:
        raw_payload = json.loads(source_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise typer.BadParameter(
            f"Extended shadow protocol JSON cannot be parsed: {source_path}"
        ) from exc
    if not isinstance(raw_payload, dict):
        raise typer.BadParameter(f"Extended shadow protocol JSON must be an object: {source_path}")
    payload = validate_extended_shadow_protocol_payload(raw_payload)
    source_artifacts = dict(payload.get("input_artifacts", {}))
    source_artifacts["extended_shadow_protocol"] = str(source_path)
    payload["input_artifacts"] = source_artifacts
    report_date = _parse_date(str(payload.get("as_of") or date.today().isoformat()))
    validation_json = json_output_path or default_extended_shadow_protocol_validation_json_path(
        reports_dir,
        report_date,
    )
    validation_md = (
        markdown_output_path
        or default_extended_shadow_protocol_validation_markdown_path(
            reports_dir,
            report_date,
        )
    )
    json_path = write_extended_shadow_protocol_validation_json(payload, validation_json)
    md_path = write_extended_shadow_protocol_validation_markdown(payload, validation_md)
    status = payload["validation_status"]
    style = "green" if status == "PASS" else "yellow"
    if status == "FAIL":
        style = "red"
    summary = payload["summary"]
    console.print(f"[{style}]Extended shadow protocol validation：{status}[/{style}]")
    console.print(f"Extended shadow protocol validation JSON：{json_path}")
    console.print(f"Extended shadow protocol validation Markdown：{md_path}")
    console.print(
        f"checks：{summary['check_count']}；"
        f"failed：{summary['failed_check_count']}；"
        f"source_blockers：{summary['source_blocker_count']}；"
        f"source_warnings：{summary['source_warning_count']}；"
        f"production_effect={payload['production_effect']}；只读校验"
    )
    if status == "FAIL":
        raise typer.Exit(code=1)


@reports_app.command("research-roadmap-dashboard")
def research_roadmap_dashboard_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Research roadmap dashboard 日期。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option(help="使用 reports_dir 中最新 report_index JSON。"),
    ] = False,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    report_index_path: Annotated[
        Path | None,
        typer.Option(help="Report index JSON 路径；不传时按日期使用默认路径。"),
    ] = None,
    task_register_path: Annotated[
        Path | None,
        typer.Option(help="docs/task_register.md 路径；不传时使用项目默认路径。"),
    ] = None,
    completed_register_path: Annotated[
        Path | None,
        typer.Option(help="docs/task_register_completed.md 路径；不传时使用项目默认路径。"),
    ] = None,
    project_root: Annotated[
        Path,
        typer.Option(help="用于解析 task register 和 artifact path 的项目根目录。"),
    ] = PROJECT_ROOT,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Research roadmap dashboard JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Research roadmap dashboard Markdown 输出路径。"),
    ] = None,
) -> None:
    """生成只读 research roadmap dashboard；不修改 task/candidate/paper-shadow 状态。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --as-of/--date 同时使用")
    if latest and report_index_path is None:
        latest_index = max(
            reports_dir.glob("report_index_????-??-??.json"),
            default=None,
            key=lambda path: path.name,
        )
        if latest_index is None:
            raise typer.BadParameter(f"未找到 report index JSON：{reports_dir}")
        source_index = latest_index
    else:
        report_date = _parse_date(as_of) if as_of else date.today()
        source_index = report_index_path or default_report_index_json_path(
            reports_dir,
            report_date,
        )
    try:
        raw_index = json.loads(source_index.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise typer.BadParameter(f"report index JSON not found: {source_index}") from exc
    except json.JSONDecodeError as exc:
        raise typer.BadParameter(f"report index JSON cannot be parsed: {source_index}") from exc
    if not isinstance(raw_index, dict):
        raise typer.BadParameter(f"report index JSON must be an object: {source_index}")
    report_date = _parse_date(
        as_of or str(raw_index.get("as_of") or date.today().isoformat())
    )
    payload = build_research_roadmap_dashboard_payload(
        as_of=report_date,
        report_index_payload=raw_index,
        report_index_path=source_index,
        task_register_path=task_register_path
        or project_root
        / "docs"
        / "task_register.md",
        completed_register_path=completed_register_path
        or project_root
        / "docs"
        / "task_register_completed.md",
        project_root=project_root,
    )
    report_json = json_output_path or default_research_roadmap_dashboard_json_path(
        reports_dir,
        report_date,
    )
    report_md = markdown_output_path or default_research_roadmap_dashboard_markdown_path(
        reports_dir,
        report_date,
    )
    json_path = write_research_roadmap_dashboard_json(payload, report_json)
    md_path = write_research_roadmap_dashboard_markdown(payload, report_md)
    status = payload["dashboard_status"]
    style = "green" if status == "ROADMAP_HEALTHY" else "yellow"
    if status == "ROADMAP_BLOCKED":
        style = "red"
    summary = payload["summary"]
    console.print(f"[{style}]Research roadmap dashboard：{status}[/{style}]")
    console.print(f"Research roadmap dashboard JSON：{json_path}")
    console.print(f"Research roadmap dashboard Markdown：{md_path}")
    console.print(
        f"active_tasks：{summary['active_task_count']}；"
        f"completed_tasks：{summary['completed_task_count']}；"
        f"blockers：{summary['open_blocker_count']}；"
        f"stale_artifacts：{summary['stale_artifact_count']}；"
        f"active_candidates：{summary['active_candidate_count']}；"
        f"paper_shadow：{summary['paper_shadow_status']}；"
        f"safety：{summary['safety_status']}；"
        f"production_effect={payload['production_effect']}；只读 roadmap dashboard"
    )


@reports_app.command("validate-research-roadmap-dashboard")
def validate_research_roadmap_dashboard_command(
    latest: Annotated[
        bool,
        typer.Option(help="校验 reports_dir 中最新 research roadmap dashboard JSON。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Research roadmap dashboard validation 日期。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    source_json_path: Annotated[
        Path | None,
        typer.Option(help="Research roadmap dashboard JSON 路径；优先于 --latest/--as-of。"),
    ] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Research roadmap dashboard validation JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Research roadmap dashboard validation Markdown 输出路径。"),
    ] = None,
) -> None:
    """校验 research roadmap dashboard；结构或只读边界漂移时 fail closed。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --as-of/--date 同时使用")
    if source_json_path is not None:
        source_path = source_json_path
    elif latest:
        latest_path = latest_research_roadmap_dashboard_json_path(reports_dir)
        if latest_path is None:
            raise typer.BadParameter(f"未找到 research roadmap dashboard JSON：{reports_dir}")
        source_path = latest_path
    else:
        report_date = _parse_date(as_of) if as_of else date.today()
        source_path = default_research_roadmap_dashboard_json_path(
            reports_dir,
            report_date,
        )
    if not source_path.exists():
        raise typer.BadParameter(f"Research roadmap dashboard JSON not found: {source_path}")
    try:
        raw_payload = json.loads(source_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise typer.BadParameter(
            f"Research roadmap dashboard JSON cannot be parsed: {source_path}"
        ) from exc
    if not isinstance(raw_payload, dict):
        raise typer.BadParameter(
            f"Research roadmap dashboard JSON must be an object: {source_path}"
        )
    payload = validate_research_roadmap_dashboard_payload(raw_payload)
    source_artifacts = dict(payload.get("input_artifacts", {}))
    source_artifacts["research_roadmap_dashboard"] = str(source_path)
    payload["input_artifacts"] = source_artifacts
    report_date = _parse_date(str(payload.get("as_of") or date.today().isoformat()))
    validation_json = json_output_path or default_research_roadmap_dashboard_validation_json_path(
        reports_dir,
        report_date,
    )
    validation_md = (
        markdown_output_path
        or default_research_roadmap_dashboard_validation_markdown_path(
            reports_dir,
            report_date,
        )
    )
    json_path = write_research_roadmap_dashboard_validation_json(payload, validation_json)
    md_path = write_research_roadmap_dashboard_validation_markdown(payload, validation_md)
    status = payload["validation_status"]
    style = "green" if status == "PASS" else "yellow"
    if status == "FAIL":
        style = "red"
    summary = payload["summary"]
    console.print(f"[{style}]Research roadmap dashboard validation：{status}[/{style}]")
    console.print(f"Research roadmap dashboard validation JSON：{json_path}")
    console.print(f"Research roadmap dashboard validation Markdown：{md_path}")
    console.print(
        f"checks：{summary['check_count']}；"
        f"failed：{summary['failed_check_count']}；"
        f"dashboard_blockers：{summary['source_blocker_count']}；"
        f"dashboard_warnings：{summary['source_warning_count']}；"
        f"production_effect={payload['production_effect']}；只读校验"
    )
    if status == "FAIL":
        raise typer.Exit(code=1)


@reports_app.command("research-governance-end-to-end-pack")
def research_governance_end_to_end_pack_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Research governance end-to-end pack 日期。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option(help="使用 reports_dir 中最新 report_index JSON。"),
    ] = False,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    report_index_path: Annotated[
        Path | None,
        typer.Option(help="Report index JSON 路径；不传时按日期使用默认路径。"),
    ] = None,
    project_root: Annotated[
        Path,
        typer.Option(help="用于解析相对 artifact path 的项目根目录。"),
    ] = PROJECT_ROOT,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Research governance end-to-end pack JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Research governance end-to-end pack Markdown 输出路径。"),
    ] = None,
) -> None:
    """生成只读 research governance end-to-end pack；不运行上游或修改状态。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --as-of/--date 同时使用")
    if latest and report_index_path is None:
        latest_index = max(
            reports_dir.glob("report_index_????-??-??.json"),
            default=None,
            key=lambda path: path.name,
        )
        if latest_index is None:
            raise typer.BadParameter(f"未找到 report index JSON：{reports_dir}")
        source_index = latest_index
    else:
        report_date = _parse_date(as_of) if as_of else date.today()
        source_index = report_index_path or default_report_index_json_path(
            reports_dir,
            report_date,
        )
    try:
        raw_index = json.loads(source_index.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise typer.BadParameter(f"report index JSON not found: {source_index}") from exc
    except json.JSONDecodeError as exc:
        raise typer.BadParameter(f"report index JSON cannot be parsed: {source_index}") from exc
    if not isinstance(raw_index, dict):
        raise typer.BadParameter(f"report index JSON must be an object: {source_index}")
    report_date = _parse_date(
        as_of or str(raw_index.get("as_of") or date.today().isoformat())
    )
    payload = build_research_governance_end_to_end_pack_payload(
        as_of=report_date,
        report_index_payload=raw_index,
        report_index_path=source_index,
        project_root=project_root,
    )
    report_json = json_output_path or default_research_governance_end_to_end_pack_json_path(
        reports_dir,
        report_date,
    )
    report_md = markdown_output_path or default_research_governance_end_to_end_pack_markdown_path(
        reports_dir,
        report_date,
    )
    json_path = write_research_governance_end_to_end_pack_json(payload, report_json)
    md_path = write_research_governance_end_to_end_pack_markdown(payload, report_md)
    status = payload["overall_governance_status"]
    style = "green" if status == "GOVERNANCE_HEALTHY" else "yellow"
    if status == "GOVERNANCE_BLOCKED":
        style = "red"
    summary = payload["summary"]
    console.print(f"[{style}]Research governance end-to-end pack：{status}[/{style}]")
    console.print(f"Research governance end-to-end pack JSON：{json_path}")
    console.print(f"Research governance end-to-end pack Markdown：{md_path}")
    console.print(
        f"sources：{summary['source_report_count']}；"
        f"available：{summary['available_source_count']}；"
        f"blockers：{summary['blocking_item_count']}；"
        f"warnings：{summary['warning_item_count']}；"
        f"manual_review：{summary['manual_review_item_count']}；"
        f"top_blocker：{summary['top_blocker']}；"
        f"production_effect={payload['production_effect']}；只读 end-to-end pack"
    )


@reports_app.command("validate-research-governance-end-to-end-pack")
def validate_research_governance_end_to_end_pack_command(
    latest: Annotated[
        bool,
        typer.Option(help="校验 reports_dir 中最新 research governance pack JSON。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Research governance pack validation 日期。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    source_json_path: Annotated[
        Path | None,
        typer.Option(help="Research governance pack JSON 路径；优先于 --latest/--as-of。"),
    ] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Research governance pack validation JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Research governance pack validation Markdown 输出路径。"),
    ] = None,
) -> None:
    """校验 research governance end-to-end pack；缺 source 或安全漂移时 fail closed。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --as-of/--date 同时使用")
    if source_json_path is not None:
        source_path = source_json_path
    elif latest:
        latest_path = latest_research_governance_end_to_end_pack_json_path(reports_dir)
        if latest_path is None:
            raise typer.BadParameter(f"未找到 research governance pack JSON：{reports_dir}")
        source_path = latest_path
    else:
        report_date = _parse_date(as_of) if as_of else date.today()
        source_path = default_research_governance_end_to_end_pack_json_path(
            reports_dir,
            report_date,
        )
    if not source_path.exists():
        raise typer.BadParameter(f"Research governance pack JSON not found: {source_path}")
    try:
        raw_payload = json.loads(source_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise typer.BadParameter(
            f"Research governance pack JSON cannot be parsed: {source_path}"
        ) from exc
    if not isinstance(raw_payload, dict):
        raise typer.BadParameter(
            f"Research governance pack JSON must be an object: {source_path}"
        )
    payload = validate_research_governance_end_to_end_pack_payload(raw_payload)
    source_artifacts = dict(payload.get("input_artifacts", {}))
    source_artifacts["research_governance_end_to_end_pack"] = str(source_path)
    payload["input_artifacts"] = source_artifacts
    report_date = _parse_date(str(payload.get("as_of") or date.today().isoformat()))
    validation_json = (
        json_output_path
        or default_research_governance_end_to_end_pack_validation_json_path(
            reports_dir,
            report_date,
        )
    )
    validation_md = (
        markdown_output_path
        or default_research_governance_end_to_end_pack_validation_markdown_path(
            reports_dir,
            report_date,
        )
    )
    json_path = write_research_governance_end_to_end_pack_validation_json(
        payload,
        validation_json,
    )
    md_path = write_research_governance_end_to_end_pack_validation_markdown(
        payload,
        validation_md,
    )
    status = payload["validation_status"]
    style = "green" if status == "PASS" else "yellow"
    if status == "FAIL":
        style = "red"
    summary = payload["summary"]
    console.print(f"[{style}]Research governance pack validation：{status}[/{style}]")
    console.print(f"Research governance pack validation JSON：{json_path}")
    console.print(f"Research governance pack validation Markdown：{md_path}")
    console.print(
        f"checks：{summary['check_count']}；"
        f"failed：{summary['failed_check_count']}；"
        f"source_blockers：{summary['source_blocker_count']}；"
        f"source_warnings：{summary['source_warning_count']}；"
        f"production_effect={payload['production_effect']}；只读校验"
    )
    if status == "FAIL":
        raise typer.Exit(code=1)


@reports_app.command("research-governance-recovery-pack")
def research_governance_recovery_pack_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Research governance recovery pack 日期。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option(help="使用 reports_dir 中最新 report_index JSON。"),
    ] = False,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    report_index_path: Annotated[
        Path | None,
        typer.Option(help="Report index JSON 路径；不传时按日期使用默认路径。"),
    ] = None,
    project_root: Annotated[
        Path,
        typer.Option(help="用于解析相对 artifact path 的项目根目录。"),
    ] = PROJECT_ROOT,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Research governance recovery pack JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Research governance recovery pack Markdown 输出路径。"),
    ] = None,
) -> None:
    """生成只读 research governance recovery pack；不运行上游或修改状态。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --as-of/--date 同时使用")
    if latest and report_index_path is None:
        latest_index = max(
            reports_dir.glob("report_index_????-??-??.json"),
            default=None,
            key=lambda path: path.name,
        )
        if latest_index is None:
            raise typer.BadParameter(f"未找到 report index JSON：{reports_dir}")
        source_index = latest_index
    else:
        report_date = _parse_date(as_of) if as_of else date.today()
        source_index = report_index_path or default_report_index_json_path(
            reports_dir,
            report_date,
        )
    try:
        raw_index = json.loads(source_index.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise typer.BadParameter(f"report index JSON not found: {source_index}") from exc
    except json.JSONDecodeError as exc:
        raise typer.BadParameter(f"report index JSON cannot be parsed: {source_index}") from exc
    if not isinstance(raw_index, dict):
        raise typer.BadParameter(f"report index JSON must be an object: {source_index}")
    report_date = _parse_date(
        as_of or str(raw_index.get("as_of") or date.today().isoformat())
    )
    payload = build_research_governance_recovery_pack_payload(
        as_of=report_date,
        report_index_payload=raw_index,
        report_index_path=source_index,
        project_root=project_root,
    )
    report_json = json_output_path or default_research_governance_recovery_pack_json_path(
        reports_dir,
        report_date,
    )
    report_md = (
        markdown_output_path
        or default_research_governance_recovery_pack_markdown_path(
            reports_dir,
            report_date,
        )
    )
    json_path = write_research_governance_recovery_pack_json(payload, report_json)
    md_path = write_research_governance_recovery_pack_markdown(payload, report_md)
    status = payload["recovery_governance_status"]
    style = "green" if status == "RECOVERY_GOVERNANCE_HEALTHY" else "yellow"
    if status == "RECOVERY_GOVERNANCE_BLOCKED":
        style = "red"
    summary = payload["summary"]
    console.print(f"[{style}]Research governance recovery pack：{status}[/{style}]")
    console.print(f"Research governance recovery pack JSON：{json_path}")
    console.print(f"Research governance recovery pack Markdown：{md_path}")
    console.print(
        f"sources：{summary['source_report_count']}；"
        f"available：{summary['available_source_count']}；"
        f"blockers：{summary['remaining_blocker_count']}；"
        f"warnings：{summary['remaining_warning_count']}；"
        f"normal_shadow：{summary['normal_paper_shadow_may_resume']}；"
        f"extended_forbidden：{summary['extended_shadow_remains_forbidden']}；"
        f"live_forbidden：{summary['live_trading_remains_forbidden']}；"
        f"production_effect={payload['production_effect']}；只读 recovery governance"
    )


@reports_app.command("validate-research-governance-recovery-pack")
def validate_research_governance_recovery_pack_command(
    latest: Annotated[
        bool,
        typer.Option(help="校验 reports_dir 中最新 research governance recovery pack JSON。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Research governance recovery validation 日期。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    source_json_path: Annotated[
        Path | None,
        typer.Option(
            help="Research governance recovery pack JSON 路径；优先于 --latest/--as-of。"
        ),
    ] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Research governance recovery validation JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Research governance recovery validation Markdown 输出路径。"),
    ] = None,
) -> None:
    """校验 research governance recovery pack；缺 source 或安全漂移时 fail closed。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --as-of/--date 同时使用")
    if source_json_path is not None:
        source_path = source_json_path
    elif latest:
        latest_path = latest_research_governance_recovery_pack_json_path(reports_dir)
        if latest_path is None:
            raise typer.BadParameter(
                f"未找到 research governance recovery pack JSON：{reports_dir}"
            )
        source_path = latest_path
    else:
        report_date = _parse_date(as_of) if as_of else date.today()
        source_path = default_research_governance_recovery_pack_json_path(
            reports_dir,
            report_date,
        )
    if not source_path.exists():
        raise typer.BadParameter(
            f"Research governance recovery pack JSON not found: {source_path}"
        )
    try:
        raw_payload = json.loads(source_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise typer.BadParameter(
            f"Research governance recovery pack JSON cannot be parsed: {source_path}"
        ) from exc
    if not isinstance(raw_payload, dict):
        raise typer.BadParameter(
            f"Research governance recovery pack JSON must be an object: {source_path}"
        )
    payload = validate_research_governance_recovery_pack_payload(raw_payload)
    source_artifacts = dict(payload.get("input_artifacts", {}))
    source_artifacts["research_governance_recovery_pack"] = str(source_path)
    payload["input_artifacts"] = source_artifacts
    report_date = _parse_date(str(payload.get("as_of") or date.today().isoformat()))
    validation_json = (
        json_output_path
        or default_research_governance_recovery_pack_validation_json_path(
            reports_dir,
            report_date,
        )
    )
    validation_md = (
        markdown_output_path
        or default_research_governance_recovery_pack_validation_markdown_path(
            reports_dir,
            report_date,
        )
    )
    json_path = write_research_governance_recovery_pack_validation_json(
        payload,
        validation_json,
    )
    md_path = write_research_governance_recovery_pack_validation_markdown(
        payload,
        validation_md,
    )
    status = payload["validation_status"]
    style = "green" if status == "PASS" else "yellow"
    if status == "FAIL":
        style = "red"
    summary = payload["summary"]
    console.print(f"[{style}]Research governance recovery validation：{status}[/{style}]")
    console.print(f"Research governance recovery validation JSON：{json_path}")
    console.print(f"Research governance recovery validation Markdown：{md_path}")
    console.print(
        f"checks：{summary['check_count']}；"
        f"failed：{summary['failed_check_count']}；"
        f"remaining_blockers：{summary['remaining_blocker_count']}；"
        f"remaining_warnings：{summary['remaining_warning_count']}；"
        f"live_forbidden：{summary['live_trading_remains_forbidden']}；"
        f"production_effect={payload['production_effect']}；只读校验"
    )
    if status == "FAIL":
        raise typer.Exit(code=1)


def _read_json_mapping_for_report_cli(path: Path, label: str) -> dict[str, object]:
    try:
        raw_payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise typer.BadParameter(f"{label} JSON not found: {path}") from exc
    except json.JSONDecodeError as exc:
        raise typer.BadParameter(f"{label} JSON cannot be parsed: {path}") from exc
    if not isinstance(raw_payload, dict):
        raise typer.BadParameter(f"{label} JSON must be an object: {path}")
    return raw_payload


def _latest_report_json_path(
    reports_dir: Path,
    latest_fn: Callable[[Path], Path | None],
    label: str,
) -> Path:
    latest_path = latest_fn(reports_dir)
    if latest_path is None:
        raise typer.BadParameter(f"未找到 {label} JSON：{reports_dir}")
    return latest_path


@reports_app.command("recovery-blocker-triage")
def recovery_blocker_triage_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Recovery blocker triage 日期。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option(help="使用 reports_dir 中最新 research governance recovery pack JSON。"),
    ] = False,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    recovery_pack_path: Annotated[
        Path | None,
        typer.Option(help="Research governance recovery pack JSON 路径。"),
    ] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Recovery blocker triage JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Recovery blocker triage Markdown 输出路径。"),
    ] = None,
) -> None:
    """生成 TRADING-401 recovery blocker triage；只读，不解决 blocker。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --as-of/--date 同时使用")
    if recovery_pack_path is not None:
        source_path = recovery_pack_path
    elif latest:
        source_path = _latest_report_json_path(
            reports_dir,
            latest_research_governance_recovery_pack_json_path,
            "research governance recovery pack",
        )
    else:
        report_date = _parse_date(as_of) if as_of else date.today()
        source_path = default_research_governance_recovery_pack_json_path(
            reports_dir,
            report_date,
        )
    source_payload = _read_json_mapping_for_report_cli(
        source_path,
        "Research governance recovery pack",
    )
    report_date = _parse_date(str(source_payload.get("as_of") or date.today().isoformat()))
    payload = recovery_triage_reports.build_recovery_blocker_triage_payload(
        as_of=report_date,
        recovery_pack_payload=source_payload,
        recovery_pack_path=source_path,
    )
    report_json = (
        json_output_path
        or recovery_triage_reports.default_recovery_blocker_triage_json_path(
            reports_dir,
            report_date,
        )
    )
    report_md = (
        markdown_output_path
        or recovery_triage_reports.default_recovery_blocker_triage_markdown_path(
            reports_dir,
            report_date,
        )
    )
    json_path = recovery_triage_reports.write_recovery_blocker_triage_json(
        payload,
        report_json,
    )
    md_path = recovery_triage_reports.write_recovery_blocker_triage_markdown(
        payload,
        report_md,
    )
    summary = payload["summary"]
    console.print("[yellow]Recovery blocker triage：RECOVERY_BLOCKERS_PRESENT[/yellow]")
    console.print(f"Recovery blocker triage JSON：{json_path}")
    console.print(f"Recovery blocker triage Markdown：{md_path}")
    console.print(
        f"blockers：{summary['recovery_blocker_count']}；"
        f"normal_shadow：{summary['normal_paper_shadow_may_resume']}；"
        f"extended_forbidden：{summary['extended_shadow_remains_forbidden']}；"
        f"live_forbidden：{summary['live_trading_remains_forbidden']}；"
        f"production_effect={payload['production_effect']}；只读 triage"
    )


@reports_app.command("validate-recovery-blocker-triage")
def validate_recovery_blocker_triage_command(
    latest: Annotated[
        bool,
        typer.Option(help="校验 reports_dir 中最新 recovery blocker triage JSON。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Recovery blocker triage validation 日期。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    source_json_path: Annotated[
        Path | None,
        typer.Option(help="Recovery blocker triage JSON 路径；优先于 --latest/--as-of。"),
    ] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Recovery blocker triage validation JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Recovery blocker triage validation Markdown 输出路径。"),
    ] = None,
) -> None:
    """校验 TRADING-401 recovery blocker triage。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --as-of/--date 同时使用")
    if source_json_path is not None:
        source_path = source_json_path
    elif latest:
        source_path = _latest_report_json_path(
            reports_dir,
            recovery_triage_reports.latest_recovery_blocker_triage_json_path,
            "recovery blocker triage",
        )
    else:
        report_date = _parse_date(as_of) if as_of else date.today()
        source_path = recovery_triage_reports.default_recovery_blocker_triage_json_path(
            reports_dir,
            report_date,
        )
    raw_payload = _read_json_mapping_for_report_cli(source_path, "Recovery blocker triage")
    payload = recovery_triage_reports.validate_recovery_blocker_triage_payload(raw_payload)
    payload["input_artifacts"] = {
        **dict(payload.get("input_artifacts", {})),
        "recovery_blocker_triage": str(source_path),
    }
    report_date = _parse_date(str(payload.get("as_of") or date.today().isoformat()))
    validation_json = (
        json_output_path
        or recovery_triage_reports.default_recovery_blocker_triage_validation_json_path(
            reports_dir,
            report_date,
        )
    )
    validation_md = (
        markdown_output_path
        or recovery_triage_reports.default_recovery_blocker_triage_validation_markdown_path(
            reports_dir,
            report_date,
        )
    )
    json_path = recovery_triage_reports.write_recovery_blocker_triage_validation_json(
        payload,
        validation_json,
    )
    md_path = recovery_triage_reports.write_recovery_blocker_triage_validation_markdown(
        payload,
        validation_md,
    )
    _print_recovery_triage_validation_result("Recovery blocker triage", payload, json_path, md_path)


@reports_app.command("report-index-warning-triage")
def report_index_warning_triage_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Report index warning triage 日期。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option(help="使用 reports_dir 中最新 report index JSON。"),
    ] = False,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    report_index_path: Annotated[
        Path | None,
        typer.Option(help="Report index JSON 路径。"),
    ] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Report index warning triage JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Report index warning triage Markdown 输出路径。"),
    ] = None,
) -> None:
    """生成 TRADING-402 report-index unwaived warning triage；不应用 waiver。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --as-of/--date 同时使用")
    if report_index_path is not None:
        source_path = report_index_path
    elif latest:
        source_path = max(
            reports_dir.glob("report_index_????-??-??.json"),
            default=None,
            key=lambda path: path.name,
        )
        if source_path is None:
            raise typer.BadParameter(f"未找到 report index JSON：{reports_dir}")
    else:
        report_date = _parse_date(as_of) if as_of else date.today()
        source_path = default_report_index_json_path(reports_dir, report_date)
    raw_index = _read_json_mapping_for_report_cli(source_path, "Report index")
    report_date = _parse_date(str(raw_index.get("as_of") or date.today().isoformat()))
    payload = recovery_triage_reports.build_report_index_warning_triage_payload(
        as_of=report_date,
        report_index_payload=raw_index,
        report_index_path=source_path,
    )
    report_json = (
        json_output_path
        or recovery_triage_reports.default_report_index_warning_triage_json_path(
            reports_dir,
            report_date,
        )
    )
    report_md = (
        markdown_output_path
        or recovery_triage_reports.default_report_index_warning_triage_markdown_path(
            reports_dir,
            report_date,
        )
    )
    json_path = recovery_triage_reports.write_report_index_warning_triage_json(
        payload,
        report_json,
    )
    md_path = recovery_triage_reports.write_report_index_warning_triage_markdown(
        payload,
        report_md,
    )
    summary = payload["summary"]
    console.print(f"[yellow]Report index warning triage：{payload['triage_status']}[/yellow]")
    console.print(f"Report index warning triage JSON：{json_path}")
    console.print(f"Report index warning triage Markdown：{md_path}")
    console.print(
        f"unwaived：{summary['unwaived_warning_count']}；"
        f"true_blockers：{summary['true_blocker_count']}；"
        f"silent_waivers：{summary['silent_waiver_count']}；"
        f"production_effect={payload['production_effect']}；只读 triage"
    )


@reports_app.command("validate-report-index-warning-triage")
def validate_report_index_warning_triage_command(
    latest: Annotated[
        bool,
        typer.Option(help="校验 reports_dir 中最新 report index warning triage JSON。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Report index warning triage validation 日期。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    source_json_path: Annotated[
        Path | None,
        typer.Option(help="Report index warning triage JSON 路径。"),
    ] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Report index warning triage validation JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Report index warning triage validation Markdown 输出路径。"),
    ] = None,
) -> None:
    """校验 TRADING-402 report-index warning triage。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --as-of/--date 同时使用")
    if source_json_path is not None:
        source_path = source_json_path
    elif latest:
        source_path = _latest_report_json_path(
            reports_dir,
            recovery_triage_reports.latest_report_index_warning_triage_json_path,
            "report index warning triage",
        )
    else:
        report_date = _parse_date(as_of) if as_of else date.today()
        source_path = recovery_triage_reports.default_report_index_warning_triage_json_path(
            reports_dir,
            report_date,
        )
    raw_payload = _read_json_mapping_for_report_cli(
        source_path,
        "Report index warning triage",
    )
    payload = recovery_triage_reports.validate_report_index_warning_triage_payload(raw_payload)
    payload["input_artifacts"] = {
        **dict(payload.get("input_artifacts", {})),
        "report_index_warning_triage": str(source_path),
    }
    report_date = _parse_date(str(payload.get("as_of") or date.today().isoformat()))
    validation_json = (
        json_output_path
        or recovery_triage_reports.default_report_index_warning_triage_validation_json_path(
            reports_dir,
            report_date,
        )
    )
    validation_md = (
        markdown_output_path
        or recovery_triage_reports.default_report_index_warning_triage_validation_markdown_path(
            reports_dir,
            report_date,
        )
    )
    json_path = recovery_triage_reports.write_report_index_warning_triage_validation_json(
        payload,
        validation_json,
    )
    md_path = recovery_triage_reports.write_report_index_warning_triage_validation_markdown(
        payload,
        validation_md,
    )
    _print_recovery_triage_validation_result(
        "Report index warning triage",
        payload,
        json_path,
        md_path,
    )


@reports_app.command("recovery-pack-source-depth-audit")
def recovery_pack_source_depth_audit_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Recovery source depth audit 日期。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option(help="使用 reports_dir 中最新 research governance recovery pack JSON。"),
    ] = False,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    recovery_pack_path: Annotated[
        Path | None,
        typer.Option(help="Research governance recovery pack JSON 路径。"),
    ] = None,
    report_index_path: Annotated[
        Path | None,
        typer.Option(help="Report index JSON 路径；可覆盖 pack input_artifacts。"),
    ] = None,
    project_root: Annotated[
        Path,
        typer.Option(help="用于解析相对 artifact path 的项目根目录。"),
    ] = PROJECT_ROOT,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Recovery source depth audit JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Recovery source depth audit Markdown 输出路径。"),
    ] = None,
) -> None:
    """生成 TRADING-404 recovery pack source depth audit。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --as-of/--date 同时使用")
    if recovery_pack_path is not None:
        source_path = recovery_pack_path
    elif latest:
        source_path = _latest_report_json_path(
            reports_dir,
            latest_research_governance_recovery_pack_json_path,
            "research governance recovery pack",
        )
    else:
        report_date = _parse_date(as_of) if as_of else date.today()
        source_path = default_research_governance_recovery_pack_json_path(
            reports_dir,
            report_date,
        )
    source_payload = _read_json_mapping_for_report_cli(
        source_path,
        "Research governance recovery pack",
    )
    report_date = _parse_date(str(source_payload.get("as_of") or date.today().isoformat()))
    report_index_payload = (
        _read_json_mapping_for_report_cli(report_index_path, "Report index")
        if report_index_path is not None
        else None
    )
    payload = recovery_triage_reports.build_recovery_pack_source_depth_audit_payload(
        as_of=report_date,
        recovery_pack_payload=source_payload,
        recovery_pack_path=source_path,
        report_index_payload=report_index_payload,
        report_index_path=report_index_path,
        project_root=project_root,
    )
    report_json = (
        json_output_path
        or recovery_triage_reports.default_recovery_pack_source_depth_audit_json_path(
            reports_dir,
            report_date,
        )
    )
    report_md = (
        markdown_output_path
        or recovery_triage_reports.default_recovery_pack_source_depth_audit_markdown_path(
            reports_dir,
            report_date,
        )
    )
    json_path = recovery_triage_reports.write_recovery_pack_source_depth_audit_json(
        payload,
        report_json,
    )
    md_path = recovery_triage_reports.write_recovery_pack_source_depth_audit_markdown(
        payload,
        report_md,
    )
    summary = payload["summary"]
    style = (
        "red"
        if payload["source_depth_audit_status"] == "RECOVERY_SOURCE_BLOCKED"
        else "yellow"
    )
    console.print(
        f"[{style}]Recovery source depth audit："
        f"{payload['source_depth_audit_status']}[/{style}]"
    )
    console.print(f"Recovery source depth audit JSON：{json_path}")
    console.print(f"Recovery source depth audit Markdown：{md_path}")
    console.print(
        f"source_availability：{summary['source_availability']}；"
        f"unhealthy：{summary['unhealthy_source_count']}；"
        f"blocked：{summary['blocked_source_count']}；"
        f"production_effect={payload['production_effect']}；只读 audit"
    )


@reports_app.command("validate-recovery-pack-source-depth-audit")
def validate_recovery_pack_source_depth_audit_command(
    latest: Annotated[
        bool,
        typer.Option(help="校验 reports_dir 中最新 recovery source depth audit JSON。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Recovery source depth validation 日期。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    source_json_path: Annotated[
        Path | None,
        typer.Option(help="Recovery source depth audit JSON 路径。"),
    ] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Recovery source depth validation JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Recovery source depth validation Markdown 输出路径。"),
    ] = None,
) -> None:
    """校验 TRADING-404 recovery source depth audit。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --as-of/--date 同时使用")
    if source_json_path is not None:
        source_path = source_json_path
    elif latest:
        source_path = _latest_report_json_path(
            reports_dir,
            recovery_triage_reports.latest_recovery_pack_source_depth_audit_json_path,
            "recovery source depth audit",
        )
    else:
        report_date = _parse_date(as_of) if as_of else date.today()
        source_path = recovery_triage_reports.default_recovery_pack_source_depth_audit_json_path(
            reports_dir,
            report_date,
        )
    raw_payload = _read_json_mapping_for_report_cli(source_path, "Recovery source depth audit")
    payload = recovery_triage_reports.validate_recovery_pack_source_depth_audit_payload(
        raw_payload
    )
    payload["input_artifacts"] = {
        **dict(payload.get("input_artifacts", {})),
        "recovery_pack_source_depth_audit": str(source_path),
    }
    report_date = _parse_date(str(payload.get("as_of") or date.today().isoformat()))
    validation_json = (
        json_output_path
        or recovery_triage_reports.default_recovery_pack_source_depth_audit_validation_json_path(
            reports_dir,
            report_date,
        )
    )
    validation_md = markdown_output_path
    if validation_md is None:
        rt = recovery_triage_reports
        default_source_depth_validation_md = (
            rt.default_recovery_pack_source_depth_audit_validation_markdown_path
        )
        validation_md = default_source_depth_validation_md(reports_dir, report_date)
    json_path = recovery_triage_reports.write_recovery_pack_source_depth_audit_validation_json(
        payload,
        validation_json,
    )
    md_path = recovery_triage_reports.write_recovery_pack_source_depth_audit_validation_markdown(
        payload,
        validation_md,
    )
    _print_recovery_triage_validation_result(
        "Recovery source depth audit",
        payload,
        json_path,
        md_path,
    )


@reports_app.command("recovery-owner-action-map")
def recovery_owner_action_map_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Recovery owner action map 日期。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    recovery_pack_path: Annotated[
        Path | None,
        typer.Option(help="Research governance recovery pack JSON 路径。"),
    ] = None,
    blocker_triage_path: Annotated[
        Path | None,
        typer.Option(help="Recovery blocker triage JSON 路径。"),
    ] = None,
    warning_triage_path: Annotated[
        Path | None,
        typer.Option(help="Report index warning triage JSON 路径。"),
    ] = None,
    source_depth_audit_path: Annotated[
        Path | None,
        typer.Option(help="Recovery source depth audit JSON 路径。"),
    ] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Recovery owner action map JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Recovery owner action map Markdown 输出路径。"),
    ] = None,
) -> None:
    """生成 TRADING-405 recovery owner action map。"""
    report_date = _parse_date(as_of) if as_of else date.today()
    recovery_path = recovery_pack_path or default_research_governance_recovery_pack_json_path(
        reports_dir,
        report_date,
    )
    blocker_path = (
        blocker_triage_path
        or recovery_triage_reports.default_recovery_blocker_triage_json_path(
            reports_dir,
            report_date,
        )
    )
    warning_path = (
        warning_triage_path
        or recovery_triage_reports.default_report_index_warning_triage_json_path(
            reports_dir,
            report_date,
        )
    )
    source_audit_path = (
        source_depth_audit_path
        or recovery_triage_reports.default_recovery_pack_source_depth_audit_json_path(
            reports_dir,
            report_date,
        )
    )
    recovery_payload = _read_json_mapping_for_report_cli(
        recovery_path,
        "Research governance recovery pack",
    )
    blocker_payload = _read_json_mapping_for_report_cli(blocker_path, "Recovery blocker triage")
    warning_payload = _read_json_mapping_for_report_cli(warning_path, "Report index warning triage")
    source_audit_payload = _read_json_mapping_for_report_cli(
        source_audit_path,
        "Recovery source depth audit",
    )
    report_date = _parse_date(str(recovery_payload.get("as_of") or report_date.isoformat()))
    payload = recovery_triage_reports.build_recovery_owner_action_map_payload(
        as_of=report_date,
        recovery_pack_payload=recovery_payload,
        blocker_triage_payload=blocker_payload,
        report_index_warning_triage_payload=warning_payload,
        source_depth_audit_payload=source_audit_payload,
        input_artifacts={
            "research_governance_recovery_pack": str(recovery_path),
            "recovery_blocker_triage": str(blocker_path),
            "report_index_warning_triage": str(warning_path),
            "recovery_pack_source_depth_audit": str(source_audit_path),
        },
    )
    report_json = (
        json_output_path
        or recovery_triage_reports.default_recovery_owner_action_map_json_path(
            reports_dir,
            report_date,
        )
    )
    report_md = (
        markdown_output_path
        or recovery_triage_reports.default_recovery_owner_action_map_markdown_path(
            reports_dir,
            report_date,
        )
    )
    json_path = recovery_triage_reports.write_recovery_owner_action_map_json(
        payload,
        report_json,
    )
    md_path = recovery_triage_reports.write_recovery_owner_action_map_markdown(
        payload,
        report_md,
    )
    summary = payload["summary"]
    console.print(
        f"[yellow]Recovery owner action map："
        f"{payload['owner_action_map_status']}[/yellow]"
    )
    console.print(f"Recovery owner action map JSON：{json_path}")
    console.print(f"Recovery owner action map Markdown：{md_path}")
    console.print(
        f"open_actions：{summary['open_action_count']}；"
        f"next_owner_action：{summary['next_owner_action']}；"
        f"live_forbidden：{summary['live_trading_forbidden']}；"
        f"production_effect={payload['production_effect']}；只读 checklist"
    )


@reports_app.command("validate-recovery-owner-action-map")
def validate_recovery_owner_action_map_command(
    latest: Annotated[
        bool,
        typer.Option(help="校验 reports_dir 中最新 recovery owner action map JSON。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Recovery owner action map validation 日期。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    source_json_path: Annotated[
        Path | None,
        typer.Option(help="Recovery owner action map JSON 路径。"),
    ] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Recovery owner action map validation JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Recovery owner action map validation Markdown 输出路径。"),
    ] = None,
) -> None:
    """校验 TRADING-405 recovery owner action map。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --as-of/--date 同时使用")
    if source_json_path is not None:
        source_path = source_json_path
    elif latest:
        source_path = _latest_report_json_path(
            reports_dir,
            recovery_triage_reports.latest_recovery_owner_action_map_json_path,
            "recovery owner action map",
        )
    else:
        report_date = _parse_date(as_of) if as_of else date.today()
        source_path = recovery_triage_reports.default_recovery_owner_action_map_json_path(
            reports_dir,
            report_date,
        )
    raw_payload = _read_json_mapping_for_report_cli(source_path, "Recovery owner action map")
    payload = recovery_triage_reports.validate_recovery_owner_action_map_payload(raw_payload)
    payload["input_artifacts"] = {
        **dict(payload.get("input_artifacts", {})),
        "recovery_owner_action_map": str(source_path),
    }
    report_date = _parse_date(str(payload.get("as_of") or date.today().isoformat()))
    validation_json = (
        json_output_path
        or recovery_triage_reports.default_recovery_owner_action_map_validation_json_path(
            reports_dir,
            report_date,
        )
    )
    validation_md = (
        markdown_output_path
        or recovery_triage_reports.default_recovery_owner_action_map_validation_markdown_path(
            reports_dir,
            report_date,
        )
    )
    json_path = recovery_triage_reports.write_recovery_owner_action_map_validation_json(
        payload,
        validation_json,
    )
    md_path = recovery_triage_reports.write_recovery_owner_action_map_validation_markdown(
        payload,
        validation_md,
    )
    _print_recovery_triage_validation_result(
        "Recovery owner action map",
        payload,
        json_path,
        md_path,
    )


@reports_app.command("exact-blocker-warning-inventory")
def exact_blocker_warning_inventory_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Exact blocker/warning inventory 日期。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    blocker_triage_path: Annotated[
        Path | None,
        typer.Option(help="Recovery blocker triage JSON 路径。"),
    ] = None,
    post_recovery_pack_path: Annotated[
        Path | None,
        typer.Option(help="Post-recovery governance pack JSON 路径。"),
    ] = None,
    source_depth_audit_path: Annotated[
        Path | None,
        typer.Option(help="Recovery pack source depth audit JSON 路径。"),
    ] = None,
    report_index_warning_triage_path: Annotated[
        Path | None,
        typer.Option(help="Report index warning triage JSON 路径。"),
    ] = None,
    normal_observation_clock_path: Annotated[
        Path | None,
        typer.Option(help="Normal paper-shadow observation clock JSON 路径。"),
    ] = None,
    owner_action_map_path: Annotated[
        Path | None,
        typer.Option(help="Recovery owner action map JSON 路径。"),
    ] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Exact blocker/warning inventory JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Exact blocker/warning inventory Markdown 输出路径。"),
    ] = None,
) -> None:
    """TRADING-421：生成逐条 blocker / warning inventory。"""
    report_date = _parse_date(as_of) if as_of else date.today()
    payload = exact_inventory_reports.build_exact_blocker_warning_inventory_payload(
        as_of=report_date,
        blocker_triage_path=blocker_triage_path
        or recovery_triage_reports.default_recovery_blocker_triage_json_path(
            reports_dir,
            report_date,
        ),
        post_recovery_pack_path=post_recovery_pack_path
        or post_recovery_reports.default_post_recovery_governance_pack_json_path(
            reports_dir,
            report_date,
        ),
        source_depth_audit_path=source_depth_audit_path
        or recovery_triage_reports.default_recovery_pack_source_depth_audit_json_path(
            reports_dir,
            report_date,
        ),
        report_index_warning_triage_path=report_index_warning_triage_path
        or recovery_triage_reports.default_report_index_warning_triage_json_path(
            reports_dir,
            report_date,
        ),
        normal_observation_clock_path=normal_observation_clock_path
        or normal_observation_clock_reports.default_normal_paper_shadow_observation_clock_json_path(
            reports_dir,
            report_date,
        ),
        owner_action_map_path=owner_action_map_path
        or recovery_triage_reports.default_recovery_owner_action_map_json_path(
            reports_dir,
            report_date,
        ),
        reports_dir=reports_dir,
    )
    report_date = _parse_date(str(payload.get("as_of") or report_date.isoformat()))
    report_json = (
        json_output_path
        or exact_inventory_reports.default_exact_blocker_warning_inventory_json_path(
            reports_dir,
            report_date,
        )
    )
    report_md = (
        markdown_output_path
        or exact_inventory_reports.default_exact_blocker_warning_inventory_markdown_path(
            reports_dir,
            report_date,
        )
    )
    json_path = exact_inventory_reports.write_exact_blocker_warning_inventory_json(
        payload,
        report_json,
    )
    md_path = exact_inventory_reports.write_exact_blocker_warning_inventory_markdown(
        payload,
        report_md,
    )
    summary = payload["summary"]
    console.print(f"[yellow]Exact blocker/warning inventory：{payload['status']}[/yellow]")
    console.print(f"Exact blocker/warning inventory JSON：{json_path}")
    console.print(f"Exact blocker/warning inventory Markdown：{md_path}")
    console.print(
        f"blockers：{summary['blocker_count']}；"
        f"warnings：{summary['warning_count']}；"
        f"report_index_warnings：{summary['report_index_warning_count']}；"
        f"normal_shadow：{summary['normal_paper_shadow_may_resume']}；"
        f"extended_forbidden：{summary['extended_shadow_remains_forbidden']}；"
        f"live_forbidden：{summary['live_trading_remains_forbidden']}；"
        f"production_effect={payload['production_effect']}；只读 inventory"
    )


@reports_app.command("validate-exact-blocker-warning-inventory")
def validate_exact_blocker_warning_inventory_command(
    latest: Annotated[
        bool,
        typer.Option(help="校验 reports_dir 中最新 exact blocker/warning inventory JSON。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Exact blocker/warning inventory validation 日期。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    source_json_path: Annotated[
        Path | None,
        typer.Option(help="Exact blocker/warning inventory JSON 路径。"),
    ] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Exact blocker/warning inventory validation JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Exact blocker/warning inventory validation Markdown 输出路径。"),
    ] = None,
) -> None:
    """校验 TRADING-421 exact blocker/warning inventory。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --as-of/--date 同时使用")
    if source_json_path is not None:
        source_path = source_json_path
    elif latest:
        source_path = _latest_report_json_path(
            reports_dir,
            exact_inventory_reports.latest_exact_blocker_warning_inventory_json_path,
            "exact blocker/warning inventory",
        )
    else:
        report_date = _parse_date(as_of) if as_of else date.today()
        source_path = exact_inventory_reports.default_exact_blocker_warning_inventory_json_path(
            reports_dir,
            report_date,
        )
    raw_payload = _read_json_mapping_for_report_cli(source_path, "Exact blocker/warning inventory")
    payload = exact_inventory_reports.validate_exact_blocker_warning_inventory_payload(
        raw_payload,
    )
    payload["input_artifacts"] = {
        **dict(payload.get("input_artifacts", {})),
        "exact_blocker_warning_inventory": str(source_path),
    }
    report_date = _parse_date(str(payload.get("as_of") or date.today().isoformat()))
    validation_json = (
        json_output_path
        or exact_inventory_reports.default_exact_blocker_warning_inventory_validation_json_path(
            reports_dir,
            report_date,
        )
    )
    validation_md = (
        markdown_output_path
        or exact_inventory_reports.default_exact_blocker_warning_inventory_validation_markdown_path(
            reports_dir,
            report_date,
        )
    )
    json_path = exact_inventory_reports.write_exact_blocker_warning_inventory_validation_json(
        payload,
        validation_json,
    )
    md_path = exact_inventory_reports.write_exact_blocker_warning_inventory_validation_markdown(
        payload,
        validation_md,
    )
    _print_recovery_triage_validation_result(
        "Exact blocker/warning inventory",
        payload,
        json_path,
        md_path,
    )


@reports_app.command("remaining-blocker-resolution-ledger")
def remaining_blocker_resolution_ledger_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Remaining blocker resolution ledger 日期。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    recovery_pack_path: Annotated[
        Path | None,
        typer.Option(help="Research governance recovery pack JSON 路径。"),
    ] = None,
    blocker_triage_path: Annotated[
        Path | None,
        typer.Option(help="Recovery blocker triage JSON 路径。"),
    ] = None,
    warning_triage_path: Annotated[
        Path | None,
        typer.Option(help="Report index warning triage JSON 路径。"),
    ] = None,
    source_depth_audit_path: Annotated[
        Path | None,
        typer.Option(help="Recovery source depth audit JSON 路径。"),
    ] = None,
    owner_action_map_path: Annotated[
        Path | None,
        typer.Option(help="Recovery owner action map JSON 路径。"),
    ] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Remaining blocker resolution ledger JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Remaining blocker resolution ledger Markdown 输出路径。"),
    ] = None,
) -> None:
    """TRADING-408：生成只读 remaining blocker/warning resolution ledger。"""
    report_date = _parse_date(as_of) if as_of else date.today()
    payload = blocker_ledger_reports.build_remaining_blocker_resolution_ledger_payload(
        as_of=report_date,
        recovery_pack_path=recovery_pack_path
        or default_research_governance_recovery_pack_json_path(reports_dir, report_date),
        blocker_triage_path=blocker_triage_path
        or recovery_triage_reports.default_recovery_blocker_triage_json_path(
            reports_dir,
            report_date,
        ),
        warning_triage_path=warning_triage_path
        or recovery_triage_reports.default_report_index_warning_triage_json_path(
            reports_dir,
            report_date,
        ),
        source_depth_audit_path=source_depth_audit_path
        or recovery_triage_reports.default_recovery_pack_source_depth_audit_json_path(
            reports_dir,
            report_date,
        ),
        owner_action_map_path=owner_action_map_path
        or recovery_triage_reports.default_recovery_owner_action_map_json_path(
            reports_dir,
            report_date,
        ),
        reports_dir=reports_dir,
    )
    report_date = _parse_date(str(payload.get("as_of") or report_date.isoformat()))
    report_json = (
        json_output_path
        or blocker_ledger_reports.default_remaining_blocker_resolution_ledger_json_path(
            reports_dir,
            report_date,
        )
    )
    report_md = (
        markdown_output_path
        or blocker_ledger_reports.default_remaining_blocker_resolution_ledger_markdown_path(
            reports_dir,
            report_date,
        )
    )
    json_path = blocker_ledger_reports.write_remaining_blocker_resolution_ledger_json(
        payload,
        report_json,
    )
    md_path = blocker_ledger_reports.write_remaining_blocker_resolution_ledger_markdown(
        payload,
        report_md,
    )
    summary = payload["summary"]
    ledger_status = payload["ledger_status"]
    console.print(f"[yellow]Remaining blocker resolution ledger：{ledger_status}[/yellow]")
    console.print(f"Remaining blocker resolution ledger JSON：{json_path}")
    console.print(f"Remaining blocker resolution ledger Markdown：{md_path}")
    console.print(
        f"blockers：{summary['blocker_count']}；"
        f"warnings：{summary['warning_count']}；"
        f"normal_shadow：{summary['normal_paper_shadow_may_resume']}；"
        f"extended_forbidden：{summary['extended_shadow_remains_forbidden']}；"
        f"live_forbidden：{summary['live_trading_remains_forbidden']}；"
        f"production_effect={payload['production_effect']}；只读 ledger"
    )


@reports_app.command("validate-remaining-blocker-resolution-ledger")
def validate_remaining_blocker_resolution_ledger_command(
    latest: Annotated[
        bool,
        typer.Option(help="校验 reports_dir 中最新 remaining blocker resolution ledger JSON。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option(
            "--as-of",
            "--date",
            help="Remaining blocker resolution ledger validation 日期。",
        ),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    source_json_path: Annotated[
        Path | None,
        typer.Option(help="Remaining blocker resolution ledger JSON 路径。"),
    ] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Remaining blocker resolution ledger validation JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Remaining blocker resolution ledger validation Markdown 输出路径。"),
    ] = None,
) -> None:
    """校验 TRADING-408 remaining blocker resolution ledger。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --as-of/--date 同时使用")
    if source_json_path is not None:
        source_path = source_json_path
    elif latest:
        source_path = _latest_report_json_path(
            reports_dir,
            blocker_ledger_reports.latest_remaining_blocker_resolution_ledger_json_path,
            "remaining blocker resolution ledger",
        )
    else:
        report_date = _parse_date(as_of) if as_of else date.today()
        source_path = (
            blocker_ledger_reports.default_remaining_blocker_resolution_ledger_json_path(
                reports_dir,
                report_date,
            )
        )
    raw_payload = _read_json_mapping_for_report_cli(
        source_path,
        "Remaining blocker resolution ledger",
    )
    payload = blocker_ledger_reports.validate_remaining_blocker_resolution_ledger_payload(
        raw_payload
    )
    payload["input_artifacts"] = {
        **dict(payload.get("input_artifacts", {})),
        "remaining_blocker_resolution_ledger": str(source_path),
    }
    report_date = _parse_date(str(payload.get("as_of") or date.today().isoformat()))
    validation_json = (
        json_output_path
        or blocker_ledger_reports.default_remaining_blocker_resolution_ledger_validation_json_path(
            reports_dir,
            report_date,
        )
    )
    default_ledger_validation_md = (
        blocker_ledger_reports.default_remaining_blocker_resolution_ledger_validation_markdown_path
    )
    validation_md = markdown_output_path or default_ledger_validation_md(
        reports_dir,
        report_date,
    )
    json_path = blocker_ledger_reports.write_remaining_blocker_resolution_ledger_validation_json(
        payload,
        validation_json,
    )
    md_path = (
        blocker_ledger_reports.write_remaining_blocker_resolution_ledger_validation_markdown(
            payload,
            validation_md,
        )
    )
    _print_recovery_triage_validation_result(
        "Remaining blocker resolution ledger",
        payload,
        json_path,
        md_path,
    )


@reports_app.command("report-index-warning-cleanup")
def report_index_warning_cleanup_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Report index warning cleanup 日期。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    warning_triage_path: Annotated[
        Path | None,
        typer.Option(help="Report index warning triage JSON 路径。"),
    ] = None,
    report_index_path: Annotated[
        Path | None,
        typer.Option(help="Report index JSON 路径。"),
    ] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Report index warning cleanup JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Report index warning cleanup Markdown 输出路径。"),
    ] = None,
) -> None:
    """TRADING-414：只读清理 report-index warnings，不应用 silent waiver。"""
    report_date = _parse_date(as_of) if as_of else date.today()
    payload = warning_cleanup_reports.build_report_index_warning_cleanup_payload(
        as_of=report_date,
        report_index_warning_triage_path=warning_triage_path
        or recovery_triage_reports.default_report_index_warning_triage_json_path(
            reports_dir,
            report_date,
        ),
        report_index_path=report_index_path or default_report_index_json_path(
            reports_dir,
            report_date,
        ),
        reports_dir=reports_dir,
    )
    report_date = _parse_date(str(payload.get("as_of") or report_date.isoformat()))
    report_json = (
        json_output_path
        or warning_cleanup_reports.default_report_index_warning_cleanup_json_path(
            reports_dir,
            report_date,
        )
    )
    report_md = (
        markdown_output_path
        or warning_cleanup_reports.default_report_index_warning_cleanup_markdown_path(
            reports_dir,
            report_date,
        )
    )
    json_path = warning_cleanup_reports.write_report_index_warning_cleanup_json(
        payload,
        report_json,
    )
    md_path = warning_cleanup_reports.write_report_index_warning_cleanup_markdown(
        payload,
        report_md,
    )
    summary = payload["summary"]
    style = "green" if payload["cleanup_status"] == "REPORT_INDEX_WARNINGS_CLEARED" else "yellow"
    console.print(f"[{style}]Report index warning cleanup：{payload['cleanup_status']}[/{style}]")
    console.print(f"Report index warning cleanup JSON：{json_path}")
    console.print(f"Report index warning cleanup Markdown：{md_path}")
    console.print(
        f"remaining_unwaived：{summary['remaining_unwaived_count']}；"
        f"fixed：{summary['fixed_warning_count']}；"
        f"silent_waivers：{summary['silent_waiver_count']}；"
        f"production_effect={payload['production_effect']}；只读 cleanup"
    )


@reports_app.command("validate-report-index-warning-cleanup")
def validate_report_index_warning_cleanup_command(
    latest: Annotated[
        bool,
        typer.Option(help="校验 reports_dir 中最新 report index warning cleanup JSON。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Report index warning cleanup validation 日期。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    source_json_path: Annotated[
        Path | None,
        typer.Option(help="Report index warning cleanup JSON 路径。"),
    ] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Report index warning cleanup validation JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Report index warning cleanup validation Markdown 输出路径。"),
    ] = None,
) -> None:
    """校验 TRADING-414 report-index warning cleanup。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --as-of/--date 同时使用")
    if source_json_path is not None:
        source_path = source_json_path
    elif latest:
        source_path = _latest_report_json_path(
            reports_dir,
            warning_cleanup_reports.latest_report_index_warning_cleanup_json_path,
            "report index warning cleanup",
        )
    else:
        report_date = _parse_date(as_of) if as_of else date.today()
        source_path = warning_cleanup_reports.default_report_index_warning_cleanup_json_path(
            reports_dir,
            report_date,
        )
    raw_payload = _read_json_mapping_for_report_cli(source_path, "Report index warning cleanup")
    payload = warning_cleanup_reports.validate_report_index_warning_cleanup_payload(raw_payload)
    payload["input_artifacts"] = {
        **dict(payload.get("input_artifacts", {})),
        "report_index_warning_cleanup": str(source_path),
    }
    report_date = _parse_date(str(payload.get("as_of") or date.today().isoformat()))
    validation_json = (
        json_output_path
        or warning_cleanup_reports.default_report_index_warning_cleanup_validation_json_path(
            reports_dir,
            report_date,
        )
    )
    validation_md = (
        markdown_output_path
        or warning_cleanup_reports.default_report_index_warning_cleanup_validation_markdown_path(
            reports_dir,
            report_date,
        )
    )
    json_path = warning_cleanup_reports.write_report_index_warning_cleanup_validation_json(
        payload,
        validation_json,
    )
    md_path = warning_cleanup_reports.write_report_index_warning_cleanup_validation_markdown(
        payload,
        validation_md,
    )
    _print_recovery_triage_validation_result(
        "Report index warning cleanup",
        payload,
        json_path,
        md_path,
    )


@reports_app.command("normal-paper-shadow-observation-clock")
def normal_paper_shadow_observation_clock_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Normal paper-shadow observation clock 日期。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option(help="使用 reports_dir 中最新 report_index JSON。"),
    ] = False,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    report_index_path: Annotated[
        Path | None,
        typer.Option(help="Report index JSON 路径。"),
    ] = None,
    project_root: Annotated[
        Path,
        typer.Option(help="用于解析相对 artifact path 的项目根目录。"),
    ] = PROJECT_ROOT,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Normal paper-shadow observation clock JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Normal paper-shadow observation clock Markdown 输出路径。"),
    ] = None,
) -> None:
    """TRADING-418：生成 normal paper-shadow observation clock bootstrap。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --as-of/--date 同时使用")
    if latest and report_index_path is None:
        source_index = max(
            reports_dir.glob("report_index_????-??-??.json"),
            default=None,
            key=lambda path: path.name,
        )
        if source_index is None:
            raise typer.BadParameter(f"未找到 report index JSON：{reports_dir}")
    else:
        report_date = _parse_date(as_of) if as_of else date.today()
        source_index = report_index_path or default_report_index_json_path(
            reports_dir,
            report_date,
        )
    raw_index = _read_json_mapping_for_report_cli(source_index, "Report index")
    report_date = _parse_date(str(raw_index.get("as_of") or date.today().isoformat()))
    payload = normal_observation_clock_reports.build_normal_paper_shadow_observation_clock_payload(
        as_of=report_date,
        report_index_payload=raw_index,
        report_index_path=source_index,
        project_root=project_root,
    )
    report_json = (
        json_output_path
        or normal_observation_clock_reports.default_normal_paper_shadow_observation_clock_json_path(
            reports_dir,
            report_date,
        )
    )
    default_normal_clock_md = (
        normal_observation_clock_reports.default_normal_paper_shadow_observation_clock_markdown_path
    )
    report_md = markdown_output_path or default_normal_clock_md(
        reports_dir,
        report_date,
    )
    json_path = normal_observation_clock_reports.write_normal_paper_shadow_observation_clock_json(
        payload,
        report_json,
    )
    md_path = normal_observation_clock_reports.write_normal_paper_shadow_observation_clock_markdown(
        payload,
        report_md,
    )
    status = payload["normal_observation_clock_status"]
    style = "green" if status == "OBSERVATION_PERIOD_MET" else "yellow"
    summary = payload["summary"]
    console.print(f"[{style}]Normal paper-shadow observation clock：{status}[/{style}]")
    console.print(f"Normal paper-shadow observation clock JSON：{json_path}")
    console.print(f"Normal paper-shadow observation clock Markdown：{md_path}")
    console.print(
        f"normal_shadow：{summary['normal_paper_shadow_may_resume']}；"
        f"current：{summary['current_count']}；"
        f"required：{summary['required_count']}；"
        f"extended_forbidden：{summary['extended_shadow_remains_forbidden']}；"
        f"live_forbidden：{summary['live_trading_remains_forbidden']}；"
        f"production_effect={payload['production_effect']}；只读 clock"
    )


@reports_app.command("validate-normal-paper-shadow-observation-clock")
def validate_normal_paper_shadow_observation_clock_command(
    latest: Annotated[
        bool,
        typer.Option(help="校验 reports_dir 中最新 normal paper-shadow observation clock JSON。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Normal observation clock validation 日期。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    source_json_path: Annotated[
        Path | None,
        typer.Option(help="Normal paper-shadow observation clock JSON 路径。"),
    ] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Normal observation clock validation JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Normal observation clock validation Markdown 输出路径。"),
    ] = None,
) -> None:
    """校验 TRADING-418 normal paper-shadow observation clock。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --as-of/--date 同时使用")
    if source_json_path is not None:
        source_path = source_json_path
    elif latest:
        source_path = _latest_report_json_path(
            reports_dir,
            normal_observation_clock_reports.latest_normal_paper_shadow_observation_clock_json_path,
            "normal paper-shadow observation clock",
        )
    else:
        report_date = _parse_date(as_of) if as_of else date.today()
        source_path = (
            normal_observation_clock_reports.default_normal_paper_shadow_observation_clock_json_path(
                reports_dir,
                report_date,
            )
        )
    raw_payload = _read_json_mapping_for_report_cli(
        source_path,
        "Normal paper-shadow observation clock",
    )
    payload = (
        normal_observation_clock_reports.validate_normal_paper_shadow_observation_clock_payload(
            raw_payload
        )
    )
    payload["input_artifacts"] = {
        **dict(payload.get("input_artifacts", {})),
        "normal_paper_shadow_observation_clock": str(source_path),
    }
    report_date = _parse_date(str(payload.get("as_of") or date.today().isoformat()))
    default_normal_clock_validation_json = (
        normal_observation_clock_reports.default_normal_paper_shadow_observation_clock_validation_json_path
    )
    validation_json = json_output_path or default_normal_clock_validation_json(
        reports_dir,
        report_date,
    )
    default_normal_clock_validation_md = (
        normal_observation_clock_reports.default_normal_paper_shadow_observation_clock_validation_markdown_path
    )
    validation_md = markdown_output_path or default_normal_clock_validation_md(
        reports_dir,
        report_date,
    )
    json_path = (
        normal_observation_clock_reports.write_normal_paper_shadow_observation_clock_validation_json(
            payload,
            validation_json,
        )
    )
    md_path = (
        normal_observation_clock_reports.write_normal_paper_shadow_observation_clock_validation_markdown(
            payload,
            validation_md,
        )
    )
    _print_recovery_triage_validation_result(
        "Normal paper-shadow observation clock",
        payload,
        json_path,
        md_path,
    )


@reports_app.command("post-recovery-governance-pack")
def post_recovery_governance_pack_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Post-recovery governance pack 日期。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    recovery_pack_path: Annotated[
        Path | None,
        typer.Option(help="Research governance recovery pack JSON 路径。"),
    ] = None,
    blocker_ledger_path: Annotated[
        Path | None,
        typer.Option(help="Remaining blocker resolution ledger JSON 路径。"),
    ] = None,
    warning_cleanup_path: Annotated[
        Path | None,
        typer.Option(help="Report index warning cleanup JSON 路径。"),
    ] = None,
    normal_observation_clock_path: Annotated[
        Path | None,
        typer.Option(help="Normal paper-shadow observation clock JSON 路径。"),
    ] = None,
    owner_decision_audit_log_path: Annotated[
        Path | None,
        typer.Option(help="Owner decision audit log report JSON 路径。"),
    ] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Post-recovery governance pack JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Post-recovery governance pack Markdown 输出路径。"),
    ] = None,
) -> None:
    """TRADING-419：生成 final post-recovery governance pack。"""
    report_date = _parse_date(as_of) if as_of else date.today()
    payload = post_recovery_reports.build_post_recovery_governance_pack_payload(
        as_of=report_date,
        recovery_pack_path=recovery_pack_path
        or default_research_governance_recovery_pack_json_path(reports_dir, report_date),
        blocker_ledger_path=blocker_ledger_path
        or blocker_ledger_reports.default_remaining_blocker_resolution_ledger_json_path(
            reports_dir,
            report_date,
        ),
        warning_cleanup_path=warning_cleanup_path
        or warning_cleanup_reports.default_report_index_warning_cleanup_json_path(
            reports_dir,
            report_date,
        ),
        normal_observation_clock_path=normal_observation_clock_path
        or normal_observation_clock_reports.default_normal_paper_shadow_observation_clock_json_path(
            reports_dir,
            report_date,
        ),
        owner_decision_audit_log_path=owner_decision_audit_log_path,
        reports_dir=reports_dir,
    )
    report_date = _parse_date(str(payload.get("as_of") or report_date.isoformat()))
    report_json = (
        json_output_path
        or post_recovery_reports.default_post_recovery_governance_pack_json_path(
            reports_dir,
            report_date,
        )
    )
    report_md = (
        markdown_output_path
        or post_recovery_reports.default_post_recovery_governance_pack_markdown_path(
            reports_dir,
            report_date,
        )
    )
    json_path = post_recovery_reports.write_post_recovery_governance_pack_json(
        payload,
        report_json,
    )
    md_path = post_recovery_reports.write_post_recovery_governance_pack_markdown(
        payload,
        report_md,
    )
    summary = payload["summary"]
    style = "red" if payload["post_recovery_status"] == "POST_RECOVERY_BLOCKED" else "yellow"
    if payload["post_recovery_status"] == "POST_RECOVERY_HEALTHY":
        style = "green"
    post_status = payload["post_recovery_status"]
    console.print(f"[{style}]Post-recovery governance pack：{post_status}[/{style}]")
    console.print(f"Post-recovery governance pack JSON：{json_path}")
    console.print(f"Post-recovery governance pack Markdown：{md_path}")
    console.print(
        f"blockers：{summary['remaining_blocker_count']}；"
        f"warnings：{summary['remaining_warning_count']}；"
        f"normal_shadow：{summary['normal_paper_shadow_may_resume']}；"
        f"extended_forbidden：{summary['extended_shadow_remains_forbidden']}；"
        f"live_forbidden：{summary['live_trading_remains_forbidden']}；"
        f"next_owner_action：{summary['next_owner_action']}；"
        f"production_effect={payload['production_effect']}；只读 governance"
    )


@reports_app.command("validate-post-recovery-governance-pack")
def validate_post_recovery_governance_pack_command(
    latest: Annotated[
        bool,
        typer.Option(help="校验 reports_dir 中最新 post-recovery governance pack JSON。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Post-recovery governance validation 日期。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    source_json_path: Annotated[
        Path | None,
        typer.Option(help="Post-recovery governance pack JSON 路径。"),
    ] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Post-recovery governance validation JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Post-recovery governance validation Markdown 输出路径。"),
    ] = None,
) -> None:
    """校验 TRADING-419 post-recovery governance pack。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --as-of/--date 同时使用")
    if source_json_path is not None:
        source_path = source_json_path
    elif latest:
        source_path = _latest_report_json_path(
            reports_dir,
            post_recovery_reports.latest_post_recovery_governance_pack_json_path,
            "post-recovery governance pack",
        )
    else:
        report_date = _parse_date(as_of) if as_of else date.today()
        source_path = post_recovery_reports.default_post_recovery_governance_pack_json_path(
            reports_dir,
            report_date,
        )
    raw_payload = _read_json_mapping_for_report_cli(source_path, "Post-recovery governance pack")
    payload = post_recovery_reports.validate_post_recovery_governance_pack_payload(raw_payload)
    payload["input_artifacts"] = {
        **dict(payload.get("input_artifacts", {})),
        "post_recovery_governance_pack": str(source_path),
    }
    report_date = _parse_date(str(payload.get("as_of") or date.today().isoformat()))
    validation_json = (
        json_output_path
        or post_recovery_reports.default_post_recovery_governance_pack_validation_json_path(
            reports_dir,
            report_date,
        )
    )
    validation_md = (
        markdown_output_path
        or post_recovery_reports.default_post_recovery_governance_pack_validation_markdown_path(
            reports_dir,
            report_date,
        )
    )
    json_path = post_recovery_reports.write_post_recovery_governance_pack_validation_json(
        payload,
        validation_json,
    )
    md_path = post_recovery_reports.write_post_recovery_governance_pack_validation_markdown(
        payload,
        validation_md,
    )
    _print_recovery_triage_validation_result(
        "Post-recovery governance pack",
        payload,
        json_path,
        md_path,
    )


def _write_decision_stage_report(
    payload: Mapping[str, object],
    *,
    reports_dir: Path,
    report_date: date,
    json_output_path: Path | None = None,
    markdown_output_path: Path | None = None,
) -> tuple[Path, Path]:
    report_type = str(payload.get("report_type") or "")
    report_json = json_output_path or decision_stage_reports.default_decision_stage_json_path(
        report_type,
        reports_dir,
        report_date,
    )
    report_md = (
        markdown_output_path
        or decision_stage_reports.default_decision_stage_markdown_path(
            report_type,
            reports_dir,
            report_date,
        )
    )
    json_path = decision_stage_reports.write_decision_stage_json(payload, report_json)
    md_path = decision_stage_reports.write_decision_stage_markdown(payload, report_md)
    return json_path, md_path


def _decision_stage_source_path(
    *,
    reports_dir: Path,
    report_date: date,
    report_type: str,
    latest: bool,
    source_json_path: Path | None,
    label: str,
) -> Path:
    if source_json_path is not None:
        return source_json_path
    if latest:
        source_path = decision_stage_reports.latest_decision_stage_json_path(
            report_type,
            reports_dir,
        )
        if source_path is None:
            raise typer.BadParameter(f"未找到 {label} JSON：{reports_dir}")
        return source_path
    return decision_stage_reports.default_decision_stage_json_path(
        report_type,
        reports_dir,
        report_date,
    )


@reports_app.command("decision-stage-review")
def decision_stage_review_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Decision-stage governance review 日期。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    report_index_path: Annotated[
        Path | None,
        typer.Option(help="Report index JSON 路径；不传时按日期使用默认路径。"),
    ] = None,
    recovery_pack_path: Annotated[
        Path | None,
        typer.Option(help="Research governance recovery pack JSON 路径。"),
    ] = None,
    report_quality_gate_path: Annotated[
        Path | None,
        typer.Option(help="Report quality gate JSON 路径。"),
    ] = None,
    project_root: Annotated[
        Path,
        typer.Option(help="用于解析相对 artifact path 的项目根目录。"),
    ] = PROJECT_ROOT,
    owner_decision_log_path: Annotated[
        Path,
        typer.Option(help="Owner decision audit JSONL 路径；dry-run 只读。"),
    ] = DEFAULT_OWNER_DECISION_AUDIT_LOG_PATH,
    dry_run_decision_option: Annotated[
        str,
        typer.Option(help="Owner decision dry-run 选项；不会 append。"),
    ] = "keep_hold",
) -> None:
    """TRADING-429~438：生成 decision-stage 只读治理审查包。"""
    report_date = _parse_date(as_of) if as_of else date.today()
    payloads = decision_stage_reports.build_decision_stage_review_payloads(
        as_of=report_date,
        reports_dir=reports_dir,
        report_index_path=report_index_path,
        recovery_pack_path=recovery_pack_path,
        report_quality_gate_path=report_quality_gate_path,
        project_root=project_root,
        owner_decision_log_path=owner_decision_log_path,
        dry_run_decision_option=dry_run_decision_option,
    )
    written: list[tuple[str, Path, Path]] = []
    for report_type, payload in payloads.items():
        json_path, md_path = _write_decision_stage_report(
            payload,
            reports_dir=reports_dir,
            report_date=report_date,
        )
        written.append((report_type, json_path, md_path))
    snapshot = payloads[decision_stage_reports.GOVERNANCE_SNAPSHOT_REPORT_TYPE]
    summary = snapshot["summary"]
    console.print(f"[red]Decision-stage governance：{snapshot['status']}[/red]")
    for report_type, json_path, md_path in written:
        console.print(f"{report_type} JSON：{json_path}")
        console.print(f"{report_type} Markdown：{md_path}")
    console.print(
        f"blockers：{summary['blocker_count']}；"
        f"warnings：{summary['warning_count']}；"
        f"recommended_owner_action：{summary['recommended_owner_action']}；"
        f"normal_shadow：{summary['normal_shadow_may_resume']}；"
        f"extended_forbidden：{summary['extended_shadow_remains_forbidden']}；"
        f"live_forbidden：{summary['live_trading_remains_forbidden']}；"
        f"dry_run_written：False；production_effect={snapshot['production_effect']}"
    )


@reports_app.command("eight-blocker-decision-review")
def eight_blocker_decision_review_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Eight-blocker review 日期。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option(help="使用 reports_dir 中最新 research governance recovery pack JSON。"),
    ] = False,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    recovery_pack_path: Annotated[
        Path | None,
        typer.Option(help="Research governance recovery pack JSON 路径。"),
    ] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Eight-blocker review JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Eight-blocker review Markdown 输出路径。"),
    ] = None,
) -> None:
    """生成 exact-eight blocker decision review；不修改 blocker 逻辑。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --as-of/--date 同时使用")
    report_date = _parse_date(as_of) if as_of else date.today()
    if recovery_pack_path is None and latest:
        recovery_pack_path = _latest_report_json_path(
            reports_dir,
            latest_research_governance_recovery_pack_json_path,
            "research governance recovery pack",
        )
    recovery_pack_path = recovery_pack_path or default_research_governance_recovery_pack_json_path(
        reports_dir,
        report_date,
    )
    recovery_pack_payload = _read_json_mapping_for_report_cli(
        recovery_pack_path,
        "Research governance recovery pack",
    )
    payload = decision_stage_reports.build_eight_blocker_decision_review_payload(
        as_of=report_date,
        recovery_pack_payload=recovery_pack_payload,
        recovery_pack_path=recovery_pack_path,
    )
    json_path, md_path = _write_decision_stage_report(
        payload,
        reports_dir=reports_dir,
        report_date=report_date,
        json_output_path=json_output_path,
        markdown_output_path=markdown_output_path,
    )
    summary = payload["summary"]
    console.print(f"[red]Eight-blocker decision review：{payload['status']}[/red]")
    console.print(f"Eight-blocker review JSON：{json_path}")
    console.print(f"Eight-blocker review Markdown：{md_path}")
    console.print(
        f"exact_blockers：{summary['remaining_blocker_count']}；"
        f"candidate_return_blockers：{summary['candidate_return_blocker_count']}；"
        f"owner_judgment_blockers：{summary['owner_judgment_blocker_count']}；"
        f"production_effect={payload['production_effect']}；只读 review"
    )


@reports_app.command("validate-eight-blocker-decision-review")
def validate_eight_blocker_decision_review_command(
    latest: Annotated[
        bool,
        typer.Option(help="校验 reports_dir 中最新 eight-blocker decision review JSON。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Eight-blocker validation 日期。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    source_json_path: Annotated[
        Path | None,
        typer.Option(help="Eight-blocker decision review JSON 路径。"),
    ] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Eight-blocker validation JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Eight-blocker validation Markdown 输出路径。"),
    ] = None,
) -> None:
    """校验 exact-eight blocker decision review。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --as-of/--date 同时使用")
    report_date = _parse_date(as_of) if as_of else date.today()
    source_path = _decision_stage_source_path(
        reports_dir=reports_dir,
        report_date=report_date,
        report_type=decision_stage_reports.EIGHT_BLOCKER_REPORT_TYPE,
        latest=latest,
        source_json_path=source_json_path,
        label="eight-blocker decision review",
    )
    source_payload = _read_json_mapping_for_report_cli(
        source_path,
        "Eight-blocker decision review",
    )
    payload = decision_stage_reports.validate_eight_blocker_decision_review_payload(
        source_payload
    )
    payload["input_artifacts"] = {
        **dict(payload.get("input_artifacts", {})),
        "eight_blocker_decision_review": str(source_path),
    }
    report_date = _parse_date(str(payload.get("as_of") or report_date.isoformat()))
    json_path, md_path = _write_decision_stage_report(
        payload,
        reports_dir=reports_dir,
        report_date=report_date,
        json_output_path=json_output_path,
        markdown_output_path=markdown_output_path,
    )
    status = payload["validation_status"]
    style = "green" if status == "PASS" else "red"
    summary = payload["summary"]
    console.print(f"[{style}]Eight-blocker validation：{status}[/{style}]")
    console.print(f"Eight-blocker validation JSON：{json_path}")
    console.print(f"Eight-blocker validation Markdown：{md_path}")
    console.print(
        f"checks：{summary['check_count']}；"
        f"failed：{summary['failed_check_count']}；"
        f"exact_blockers：{summary['exact_blocker_count']}；"
        f"production_effect={payload['production_effect']}；只读校验"
    )
    if status == "FAIL":
        raise typer.Exit(code=1)


@reports_app.command("owner-decision-dry-run")
def owner_decision_dry_run_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Owner decision dry-run 日期。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    decision_option: Annotated[
        str,
        typer.Option(help="Dry-run owner decision option；不会 append。"),
    ] = "keep_hold",
    owner_options_path: Annotated[
        Path | None,
        typer.Option(help="Owner decision options packet JSON 路径。"),
    ] = None,
    report_index_path: Annotated[
        Path | None,
        typer.Option(help="缺少 owner options packet 时用于临时构建的 report_index JSON。"),
    ] = None,
    recovery_pack_path: Annotated[
        Path | None,
        typer.Option(help="缺少 owner options packet 时用于临时构建的 recovery pack JSON。"),
    ] = None,
    report_quality_gate_path: Annotated[
        Path | None,
        typer.Option(help="缺少 owner options packet 时用于临时构建的 quality gate JSON。"),
    ] = None,
    project_root: Annotated[
        Path,
        typer.Option(help="用于解析相对 artifact path 的项目根目录。"),
    ] = PROJECT_ROOT,
    owner_decision_log_path: Annotated[
        Path,
        typer.Option(help="Owner decision audit JSONL 路径；dry-run 只读。"),
    ] = DEFAULT_OWNER_DECISION_AUDIT_LOG_PATH,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Owner decision dry-run JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Owner decision dry-run Markdown 输出路径。"),
    ] = None,
) -> None:
    """Dry-run owner decision audit record；不 append 审计日志。"""
    report_date = _parse_date(as_of) if as_of else date.today()
    if owner_options_path is not None and owner_options_path.exists():
        owner_options_payload = _read_json_mapping_for_report_cli(
            owner_options_path,
            "Owner decision options packet",
        )
    else:
        default_options_path = decision_stage_reports.default_decision_stage_json_path(
            decision_stage_reports.OWNER_OPTIONS_REPORT_TYPE,
            reports_dir,
            report_date,
        )
        if default_options_path.exists():
            owner_options_payload = _read_json_mapping_for_report_cli(
                default_options_path,
                "Owner decision options packet",
            )
        else:
            payloads = decision_stage_reports.build_decision_stage_review_payloads(
                as_of=report_date,
                reports_dir=reports_dir,
                report_index_path=report_index_path,
                recovery_pack_path=recovery_pack_path,
                report_quality_gate_path=report_quality_gate_path,
                project_root=project_root,
                owner_decision_log_path=owner_decision_log_path,
                dry_run_decision_option=decision_option,
            )
            owner_options_payload = payloads[decision_stage_reports.OWNER_OPTIONS_REPORT_TYPE]
    payload = decision_stage_reports.build_owner_decision_dry_run_payload(
        as_of=report_date,
        decision_option=decision_option,
        owner_options_payload=owner_options_payload,
        log_path=owner_decision_log_path,
    )
    json_path, md_path = _write_decision_stage_report(
        payload,
        reports_dir=reports_dir,
        report_date=report_date,
        json_output_path=json_output_path,
        markdown_output_path=markdown_output_path,
    )
    summary = payload["summary"]
    style = "green" if summary["dry_run_status"] == "OWNER_DECISION_DRY_RUN_VALID" else "yellow"
    if summary["dry_run_status"] == "OWNER_DECISION_DRY_RUN_BLOCKED":
        style = "red"
    console.print(f"[{style}]Owner decision dry-run：{summary['dry_run_status']}[/{style}]")
    console.print(f"Owner decision dry-run JSON：{json_path}")
    console.print(f"Owner decision dry-run Markdown：{md_path}")
    console.print(
        f"decision_option：{summary['decision_option']}；"
        f"record_validation：{summary['record_validation_status']}；"
        f"would_append：{summary['would_append']}；"
        f"real_entry_written：{summary['real_entry_written']}；"
        f"production_effect={payload['production_effect']}；只读 dry-run"
    )
    if summary["dry_run_status"] == "OWNER_DECISION_DRY_RUN_BLOCKED":
        raise typer.Exit(code=1)


def _return_to_research_source_path(
    *,
    reports_dir: Path,
    report_date: date,
    report_type: str,
    latest: bool,
    source_json_path: Path | None,
    label: str,
) -> Path:
    if source_json_path is not None:
        return source_json_path
    if latest:
        source_path = return_research_reports.latest_return_to_research_json_path(
            report_type,
            reports_dir,
        )
        if source_path is None:
            raise typer.BadParameter(f"未找到 {label} JSON：{reports_dir}")
        return source_path
    return return_research_reports.default_return_to_research_json_path(
        report_type,
        reports_dir,
        report_date,
    )


def _write_return_to_research_report(
    payload: Mapping[str, object],
    *,
    reports_dir: Path,
    report_date: date,
    json_output_path: Path | None = None,
    markdown_output_path: Path | None = None,
) -> tuple[Path, Path]:
    report_type = str(payload.get("report_type"))
    json_path = json_output_path or return_research_reports.default_return_to_research_json_path(
        report_type,
        reports_dir,
        report_date,
    )
    md_path = (
        markdown_output_path
        or return_research_reports.default_return_to_research_markdown_path(
            report_type,
            reports_dir,
            report_date,
        )
    )
    return (
        return_research_reports.write_return_to_research_json(payload, json_path),
        return_research_reports.write_return_to_research_markdown(payload, md_path),
    )


def _write_next_research_cycle_report(
    payload: Mapping[str, object],
    *,
    reports_dir: Path,
    report_date: date,
    json_output_path: Path | None = None,
    markdown_output_path: Path | None = None,
) -> tuple[Path, Path]:
    report_type = str(payload.get("report_type"))
    json_path = json_output_path or next_research_reports.default_next_research_cycle_json_path(
        report_type,
        reports_dir,
        report_date,
    )
    md_path = (
        markdown_output_path
        or next_research_reports.default_next_research_cycle_markdown_path(
            report_type,
            reports_dir,
            report_date,
        )
    )
    return (
        next_research_reports.write_next_research_cycle_json(payload, json_path),
        next_research_reports.write_next_research_cycle_markdown(payload, md_path),
    )


def _write_executable_binding_report(
    payload: Mapping[str, object],
    *,
    reports_dir: Path,
    report_date: date,
    json_output_path: Path | None = None,
    markdown_output_path: Path | None = None,
) -> tuple[Path, Path]:
    report_type = str(payload.get("report_type"))
    json_path = json_output_path or executable_binding_reports.default_executable_binding_json_path(
        report_type,
        reports_dir,
        report_date,
    )
    md_path = (
        markdown_output_path
        or executable_binding_reports.default_executable_binding_markdown_path(
            report_type,
            reports_dir,
            report_date,
        )
    )
    return (
        executable_binding_reports.write_executable_binding_json(payload, json_path),
        executable_binding_reports.write_executable_binding_markdown(payload, md_path),
    )


def _write_evidence_repair_report(
    payload: Mapping[str, object],
    *,
    reports_dir: Path,
    report_date: date,
    json_output_path: Path | None = None,
    markdown_output_path: Path | None = None,
) -> tuple[Path, Path]:
    report_type = str(payload.get("report_type"))
    json_path = json_output_path or evidence_repair_reports.default_evidence_repair_json_path(
        report_type,
        reports_dir,
        report_date,
    )
    md_path = (
        markdown_output_path
        or evidence_repair_reports.default_evidence_repair_markdown_path(
            report_type,
            reports_dir,
            report_date,
        )
    )
    return (
        evidence_repair_reports.write_evidence_repair_json(payload, json_path),
        evidence_repair_reports.write_evidence_repair_markdown(payload, md_path),
    )


def _next_research_cycle_source_path(
    *,
    reports_dir: Path,
    report_date: date,
    report_type: str,
    latest: bool,
    source_json_path: Path | None,
    label: str,
) -> Path:
    if source_json_path is not None:
        return source_json_path
    if latest:
        latest_path = next_research_reports.latest_next_research_cycle_json_path(
            report_type,
            reports_dir,
        )
        if latest_path is None:
            raise typer.BadParameter(f"找不到 latest {label} JSON")
        return latest_path
    return next_research_reports.default_next_research_cycle_json_path(
        report_type,
        reports_dir,
        report_date,
    )


def _executable_binding_source_path(
    *,
    reports_dir: Path,
    report_date: date,
    report_type: str,
    latest: bool,
    source_json_path: Path | None,
    label: str,
) -> Path:
    if source_json_path is not None:
        return source_json_path
    if latest:
        latest_path = executable_binding_reports.latest_executable_binding_json_path(
            report_type,
            reports_dir,
        )
        if latest_path is None:
            raise typer.BadParameter(f"找不到 latest {label} JSON")
        return latest_path
    return executable_binding_reports.default_executable_binding_json_path(
        report_type,
        reports_dir,
        report_date,
    )


def _evidence_repair_source_path(
    *,
    reports_dir: Path,
    report_date: date,
    report_type: str,
    latest: bool,
    source_json_path: Path | None,
    label: str,
) -> Path:
    if source_json_path is not None:
        return source_json_path
    if latest:
        latest_path = evidence_repair_reports.latest_evidence_repair_json_path(
            report_type,
            reports_dir,
        )
        if latest_path is None:
            raise typer.BadParameter(f"找不到 latest {label} JSON")
        return latest_path
    return evidence_repair_reports.default_evidence_repair_json_path(
        report_type,
        reports_dir,
        report_date,
    )


def _run_next_research_data_quality_gate(
    *,
    report_date: date,
    reports_dir: Path,
    prices_path: Path,
    rates_path: Path,
    data_quality_output_path: Path | None,
    full_universe: bool,
) -> dict[str, object]:
    universe = load_universe()
    quality_report_path = data_quality_output_path or default_quality_report_path(
        reports_dir,
        report_date,
    )
    quality_report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=configured_price_tickers(
            universe,
            include_full_ai_chain=full_universe,
        ),
        expected_rate_series=configured_rate_series(universe),
        quality_config=load_data_quality(),
        as_of=report_date,
        manifest_path=_download_manifest_path(prices_path),
        secondary_prices_path=_marketstack_prices_path(prices_path),
        require_secondary_prices=_requires_marketstack_prices(prices_path),
    )
    write_data_quality_report(quality_report, quality_report_path)
    return {
        "status": quality_report.status,
        "passed": quality_report.passed,
        "error_count": quality_report.error_count,
        "warning_count": quality_report.warning_count,
        "report_path": str(quality_report_path),
    }


def _write_next_research_validation(
    source_payload: Mapping[str, object],
    *,
    expected_report_type: str,
    reports_dir: Path,
    json_output_path: Path | None = None,
    markdown_output_path: Path | None = None,
) -> tuple[dict[str, object], Path, Path]:
    payload = next_research_reports.validate_next_research_cycle_payload(
        source_payload,
        expected_report_type=expected_report_type,
    )
    report_date = _parse_date(str(payload.get("as_of")))
    json_path, md_path = _write_next_research_cycle_report(
        payload,
        reports_dir=reports_dir,
        report_date=report_date,
        json_output_path=json_output_path,
        markdown_output_path=markdown_output_path,
    )
    return payload, json_path, md_path


def _write_executable_binding_validation(
    source_payload: Mapping[str, object],
    *,
    expected_report_type: str,
    reports_dir: Path,
    json_output_path: Path | None = None,
    markdown_output_path: Path | None = None,
) -> tuple[dict[str, object], Path, Path]:
    payload = executable_binding_reports.validate_executable_binding_payload(
        source_payload,
        expected_report_type=expected_report_type,
    )
    report_date = _parse_date(str(payload.get("as_of")))
    json_path, md_path = _write_executable_binding_report(
        payload,
        reports_dir=reports_dir,
        report_date=report_date,
        json_output_path=json_output_path,
        markdown_output_path=markdown_output_path,
    )
    return payload, json_path, md_path


def _write_evidence_repair_validation(
    source_payload: Mapping[str, object],
    *,
    reports_dir: Path,
    json_output_path: Path | None = None,
    markdown_output_path: Path | None = None,
) -> tuple[dict[str, object], Path, Path]:
    payload = (
        evidence_repair_reports.validate_executable_research_evidence_gap_ledger_payload(
            source_payload
        )
    )
    report_date = _parse_date(str(payload.get("as_of")))
    json_path, md_path = _write_evidence_repair_report(
        payload,
        reports_dir=reports_dir,
        report_date=report_date,
        json_output_path=json_output_path,
        markdown_output_path=markdown_output_path,
    )
    return payload, json_path, md_path


def _load_next_research_source_payload(
    *,
    report_type: str,
    report_date: date,
    reports_dir: Path,
    latest: bool = False,
    source_json_path: Path | None = None,
    label: str,
) -> tuple[Path, dict[str, object]]:
    source_path = _next_research_cycle_source_path(
        reports_dir=reports_dir,
        report_date=report_date,
        report_type=report_type,
        latest=latest,
        source_json_path=source_json_path,
        label=label,
    )
    return source_path, _read_json_mapping_for_report_cli(source_path, label)


def _load_executable_binding_source_payload(
    *,
    report_type: str,
    report_date: date,
    reports_dir: Path,
    latest: bool = False,
    source_json_path: Path | None = None,
    label: str,
) -> tuple[Path, dict[str, object]]:
    source_path = _executable_binding_source_path(
        reports_dir=reports_dir,
        report_date=report_date,
        report_type=report_type,
        latest=latest,
        source_json_path=source_json_path,
        label=label,
    )
    return source_path, _read_json_mapping_for_report_cli(source_path, label)


def _load_evidence_repair_source_payload(
    *,
    report_type: str,
    report_date: date,
    reports_dir: Path,
    latest: bool = False,
    source_json_path: Path | None = None,
    label: str,
) -> tuple[Path, dict[str, object]]:
    source_path = _evidence_repair_source_path(
        reports_dir=reports_dir,
        report_date=report_date,
        report_type=report_type,
        latest=latest,
        source_json_path=source_json_path,
        label=label,
    )
    return source_path, _read_json_mapping_for_report_cli(source_path, label)


@reports_app.command("return-to-research-reset")
def return_to_research_reset_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Return-to-research reset 日期。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    decision_source_dir: Annotated[
        Path,
        typer.Option(help="Owner decision source JSON 目录。"),
    ] = PROJECT_ROOT
    / "docs"
    / "decisions",
    owner_decision_log_path: Annotated[
        Path,
        typer.Option(help="Owner decision audit JSONL 路径。"),
    ] = DEFAULT_OWNER_DECISION_AUDIT_LOG_PATH,
    append_owner_decision: Annotated[
        bool,
        typer.Option(
            help=(
                "追加 TRADING-439 return_to_research owner decision；若同 decision_id "
                "已存在则复用。"
            ),
        ),
    ] = True,
) -> None:
    """TRADING-439~448：记录 return_to_research 决策并生成研究重置包。"""
    report_date = _parse_date(as_of) if as_of else date.today()
    try:
        payloads = return_research_reports.build_return_to_research_reset_payloads(
            as_of=report_date,
            reports_dir=reports_dir,
            decision_source_dir=decision_source_dir,
            owner_decision_log_path=owner_decision_log_path,
            append_owner_decision=append_owner_decision,
        )
    except (FileNotFoundError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc

    written: list[tuple[str, Path, Path]] = []
    for report_type in return_research_reports.RESET_REPORT_TYPES:
        payload = payloads[report_type]
        json_path, md_path = _write_return_to_research_report(
            payload,
            reports_dir=reports_dir,
            report_date=report_date,
        )
        written.append((report_type, json_path, md_path))

    snapshot = payloads[return_research_reports.GOVERNANCE_SNAPSHOT_REPORT_TYPE]
    snapshot_json = next(
        path
        for report_type, path, _ in written
        if report_type == return_research_reports.GOVERNANCE_SNAPSHOT_REPORT_TYPE
    )
    validation = return_research_reports.validate_return_to_research_governance_snapshot_payload(
        snapshot
    )
    validation["input_artifacts"] = {
        **dict(validation.get("input_artifacts", {})),
        "return_to_research_governance_snapshot": str(snapshot_json),
    }
    validation_json, validation_md = _write_return_to_research_report(
        validation,
        reports_dir=reports_dir,
        report_date=report_date,
    )
    summary = snapshot["summary"]
    style = "green" if snapshot["status"] == "RETURN_TO_RESEARCH_COMPLETE" else "yellow"
    if validation["status"] == "FAIL":
        style = "red"
    console.print(f"[{style}]Return-to-research reset：{snapshot['status']}[/{style}]")
    for report_type, json_path, md_path in written:
        console.print(f"{report_type} JSON：{json_path}")
        console.print(f"{report_type} Markdown：{md_path}")
    console.print(f"return_to_research_governance_snapshot_validation JSON：{validation_json}")
    console.print(f"return_to_research_governance_snapshot_validation Markdown：{validation_md}")
    console.print(
        f"owner_decision：{summary['owner_decision_id']}；"
        f"candidate_status：{summary['candidate_status']}；"
        f"normal_shadow_active：{summary['normal_paper_shadow_active']}；"
        f"extended_allowed：{summary['extended_shadow_allowed']}；"
        f"live_allowed：{summary['live_trading_allowed']}；"
        f"candidate_rejected：{summary['candidate_rejected']}；"
        f"validation：{validation['status']}；"
        f"production_effect={snapshot['production_effect']}"
    )
    if validation["status"] == "FAIL":
        raise typer.Exit(code=1)


@reports_app.command("validate-return-to-research-governance-snapshot")
def validate_return_to_research_governance_snapshot_command(
    latest: Annotated[
        bool,
        typer.Option(help="校验 reports_dir 中最新 return-to-research governance snapshot。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Return-to-research validation 日期。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    source_json_path: Annotated[
        Path | None,
        typer.Option(help="Return-to-research governance snapshot JSON 路径。"),
    ] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Validation JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Validation Markdown 输出路径。"),
    ] = None,
) -> None:
    """校验 return-to-research final governance snapshot。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --as-of/--date 同时使用")
    report_date = _parse_date(as_of) if as_of else date.today()
    source_path = _return_to_research_source_path(
        reports_dir=reports_dir,
        report_date=report_date,
        report_type=return_research_reports.GOVERNANCE_SNAPSHOT_REPORT_TYPE,
        latest=latest,
        source_json_path=source_json_path,
        label="return-to-research governance snapshot",
    )
    source_payload = _read_json_mapping_for_report_cli(
        source_path,
        "Return-to-research governance snapshot",
    )
    payload = return_research_reports.validate_return_to_research_governance_snapshot_payload(
        source_payload
    )
    payload["input_artifacts"] = {
        **dict(payload.get("input_artifacts", {})),
        "return_to_research_governance_snapshot": str(source_path),
    }
    report_date = _parse_date(str(payload.get("as_of") or report_date.isoformat()))
    json_path, md_path = _write_return_to_research_report(
        payload,
        reports_dir=reports_dir,
        report_date=report_date,
        json_output_path=json_output_path,
        markdown_output_path=markdown_output_path,
    )
    status = payload["status"]
    style = "green" if status == "PASS" else "red"
    summary = payload["summary"]
    console.print(f"[{style}]Return-to-research snapshot validation：{status}[/{style}]")
    console.print(f"Validation JSON：{json_path}")
    console.print(f"Validation Markdown：{md_path}")
    console.print(
        f"checks：{summary['check_count']}；"
        f"failed：{summary['failed_check_count']}；"
        f"candidate_status：{summary['candidate_status']}；"
        f"production_effect={payload['production_effect']}；只读校验"
    )
    if status == "FAIL":
        raise typer.Exit(code=1)


@reports_app.command("next-research-cycle-intake")
def next_research_cycle_intake_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Next research cycle intake 日期。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Intake JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Intake Markdown 输出路径。"),
    ] = None,
) -> None:
    """TRADING-449：生成 next research cycle intake pack。"""
    report_date = _parse_date(as_of) if as_of else date.today()
    try:
        payload = next_research_reports.build_next_research_cycle_intake_payload(
            as_of=report_date,
            reports_dir=reports_dir,
        )
    except (FileNotFoundError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    json_path, md_path = _write_next_research_cycle_report(
        payload,
        reports_dir=reports_dir,
        report_date=report_date,
        json_output_path=json_output_path,
        markdown_output_path=markdown_output_path,
    )
    console.print(f"[green]Next research cycle intake：{payload['status']}[/green]")
    console.print(f"JSON：{json_path}")
    console.print(f"Markdown：{md_path}")


@reports_app.command("next-candidate-spec-freeze")
def next_candidate_spec_freeze_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Frozen next candidate spec 日期。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Frozen spec JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Frozen spec Markdown 输出路径。"),
    ] = None,
) -> None:
    """TRADING-450：冻结 research-only next candidate spec。"""
    report_date = _parse_date(as_of) if as_of else date.today()
    try:
        payload = next_research_reports.build_next_candidate_spec_frozen_payload(
            as_of=report_date,
            reports_dir=reports_dir,
        )
    except (FileNotFoundError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    json_path, md_path = _write_next_research_cycle_report(
        payload,
        reports_dir=reports_dir,
        report_date=report_date,
        json_output_path=json_output_path,
        markdown_output_path=markdown_output_path,
    )
    summary = payload["summary"]
    console.print(f"[green]Next candidate frozen spec：{payload['status']}[/green]")
    console.print(f"candidate_id：{summary['candidate_id']}")
    console.print(f"JSON：{json_path}")
    console.print(f"Markdown：{md_path}")


@reports_app.command("next-candidate-backfill")
def next_candidate_backfill_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Next candidate backfill 日期。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    prices_path: Annotated[
        Path,
        typer.Option(help="标准化日线价格 CSV 路径。"),
    ] = PROJECT_ROOT
    / "data"
    / "raw"
    / "prices_daily.csv",
    rates_path: Annotated[
        Path,
        typer.Option(help="标准化 FRED 宏观序列 CSV 路径。"),
    ] = PROJECT_ROOT
    / "data"
    / "raw"
    / "rates_daily.csv",
    data_quality_output_path: Annotated[
        Path | None,
        typer.Option(help="数据质量 Markdown 输出路径；不传时按日期使用默认报告路径。"),
    ] = None,
    full_universe: Annotated[
        bool,
        typer.Option("--full-universe", help="按完整 AI 产业链标的运行 validate-data。"),
    ] = False,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Backfill JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Backfill Markdown 输出路径。"),
    ] = None,
) -> None:
    """TRADING-451：运行 research-only next candidate backfill gate。"""
    report_date = _parse_date(as_of) if as_of else date.today()
    data_quality_gate = _run_next_research_data_quality_gate(
        report_date=report_date,
        reports_dir=reports_dir,
        prices_path=prices_path,
        rates_path=rates_path,
        data_quality_output_path=data_quality_output_path,
        full_universe=full_universe,
    )
    console.print(
        f"数据质量状态：{data_quality_gate['status']}；"
        f"报告：{data_quality_gate['report_path']}"
    )
    if data_quality_gate["passed"] is not True:
        raise typer.Exit(code=1)
    try:
        payload = next_research_reports.build_next_candidate_backfill_payload(
            as_of=report_date,
            reports_dir=reports_dir,
            data_quality_gate=data_quality_gate,
            prices_path=prices_path,
        )
    except (FileNotFoundError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    json_path, md_path = _write_next_research_cycle_report(
        payload,
        reports_dir=reports_dir,
        report_date=report_date,
        json_output_path=json_output_path,
        markdown_output_path=markdown_output_path,
    )
    console.print(f"[yellow]Next candidate backfill：{payload['status']}[/yellow]")
    console.print(f"JSON：{json_path}")
    console.print(f"Markdown：{md_path}")


@reports_app.command("next-candidate-stress-review")
def next_candidate_stress_review_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Next candidate stress review 日期。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    project_root: Annotated[
        Path,
        typer.Option(help="项目根目录，用于读取 stress/casebook artifacts。"),
    ] = PROJECT_ROOT,
) -> None:
    """TRADING-452：生成 next candidate stress review。"""
    report_date = _parse_date(as_of) if as_of else date.today()
    try:
        payload = next_research_reports.build_next_candidate_stress_review_payload(
            as_of=report_date,
            reports_dir=reports_dir,
            project_root=project_root,
        )
    except (FileNotFoundError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    json_path, md_path = _write_next_research_cycle_report(
        payload,
        reports_dir=reports_dir,
        report_date=report_date,
    )
    style = "green" if payload["status"] in {"STRONG", "MIXED"} else "yellow"
    console.print(f"[{style}]Next candidate stress review：{payload['status']}[/{style}]")
    console.print(f"JSON：{json_path}")
    console.print(f"Markdown：{md_path}")


@reports_app.command("next-candidate-cost-benchmark-review")
def next_candidate_cost_benchmark_review_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Next candidate cost/benchmark 日期。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    project_root: Annotated[
        Path,
        typer.Option(help="项目根目录，用于读取 cost/benchmark artifacts。"),
    ] = PROJECT_ROOT,
) -> None:
    """TRADING-453：生成 next candidate cost/benchmark review。"""
    report_date = _parse_date(as_of) if as_of else date.today()
    _, backfill = _load_next_research_source_payload(
        report_type=next_research_reports.BACKFILL_REPORT_TYPE,
        report_date=report_date,
        reports_dir=reports_dir,
        label="next candidate backfill",
    )
    payload = next_research_reports.build_next_candidate_cost_benchmark_review_payload(
        as_of=report_date,
        project_root=project_root,
        backfill_payload=backfill,
    )
    json_path, md_path = _write_next_research_cycle_report(
        payload,
        reports_dir=reports_dir,
        report_date=report_date,
    )
    console.print(f"[yellow]Next candidate cost/benchmark：{payload['status']}[/yellow]")
    console.print(f"JSON：{json_path}")
    console.print(f"Markdown：{md_path}")


@reports_app.command("next-candidate-vs-returned-candidate-comparison")
def next_candidate_vs_returned_comparison_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Next candidate comparison 日期。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
) -> None:
    """TRADING-454：比较 next candidate 与 returned candidate。"""
    report_date = _parse_date(as_of) if as_of else date.today()
    _, backfill = _load_next_research_source_payload(
        report_type=next_research_reports.BACKFILL_REPORT_TYPE,
        report_date=report_date,
        reports_dir=reports_dir,
        label="next candidate backfill",
    )
    _, stress = _load_next_research_source_payload(
        report_type=next_research_reports.STRESS_REVIEW_REPORT_TYPE,
        report_date=report_date,
        reports_dir=reports_dir,
        label="next candidate stress review",
    )
    _, cost_benchmark = _load_next_research_source_payload(
        report_type=next_research_reports.COST_BENCHMARK_REVIEW_REPORT_TYPE,
        report_date=report_date,
        reports_dir=reports_dir,
        label="next candidate cost benchmark review",
    )
    payload = next_research_reports.build_next_candidate_vs_returned_comparison_payload(
        as_of=report_date,
        reports_dir=reports_dir,
        backfill_payload=backfill,
        stress_review_payload=stress,
        cost_benchmark_payload=cost_benchmark,
    )
    json_path, md_path = _write_next_research_cycle_report(
        payload,
        reports_dir=reports_dir,
        report_date=report_date,
    )
    console.print(f"[yellow]Next candidate comparison：{payload['status']}[/yellow]")
    console.print(f"JSON：{json_path}")
    console.print(f"Markdown：{md_path}")


@reports_app.command("next-candidate-signal-robustness-review")
def next_candidate_signal_robustness_review_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Next candidate signal robustness 日期。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    project_root: Annotated[
        Path,
        typer.Option(help="项目根目录，用于读取 signal completeness artifacts。"),
    ] = PROJECT_ROOT,
) -> None:
    """TRADING-455：生成 signal robustness review。"""
    report_date = _parse_date(as_of) if as_of else date.today()
    _, frozen = _load_next_research_source_payload(
        report_type=next_research_reports.FROZEN_SPEC_REPORT_TYPE,
        report_date=report_date,
        reports_dir=reports_dir,
        label="next candidate frozen spec",
    )
    _, backfill = _load_next_research_source_payload(
        report_type=next_research_reports.BACKFILL_REPORT_TYPE,
        report_date=report_date,
        reports_dir=reports_dir,
        label="next candidate backfill",
    )
    payload = next_research_reports.build_next_candidate_signal_robustness_review_payload(
        as_of=report_date,
        reports_dir=reports_dir,
        project_root=project_root,
        frozen_spec_payload=frozen,
        backfill_payload=backfill,
    )
    json_path, md_path = _write_next_research_cycle_report(
        payload,
        reports_dir=reports_dir,
        report_date=report_date,
    )
    console.print(f"[yellow]Next candidate signal robustness：{payload['status']}[/yellow]")
    console.print(f"JSON：{json_path}")
    console.print(f"Markdown：{md_path}")


@reports_app.command("next-candidate-overfit-window-sensitivity")
def next_candidate_window_sensitivity_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Next candidate window sensitivity 日期。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
) -> None:
    """TRADING-456：生成 overfit/window sensitivity review。"""
    report_date = _parse_date(as_of) if as_of else date.today()
    _, frozen = _load_next_research_source_payload(
        report_type=next_research_reports.FROZEN_SPEC_REPORT_TYPE,
        report_date=report_date,
        reports_dir=reports_dir,
        label="next candidate frozen spec",
    )
    _, backfill = _load_next_research_source_payload(
        report_type=next_research_reports.BACKFILL_REPORT_TYPE,
        report_date=report_date,
        reports_dir=reports_dir,
        label="next candidate backfill",
    )
    payload = next_research_reports.build_next_candidate_window_sensitivity_payload(
        as_of=report_date,
        frozen_spec_payload=frozen,
        backfill_payload=backfill,
    )
    json_path, md_path = _write_next_research_cycle_report(
        payload,
        reports_dir=reports_dir,
        report_date=report_date,
    )
    console.print(f"[yellow]Next candidate window sensitivity：{payload['status']}[/yellow]")
    console.print(f"JSON：{json_path}")
    console.print(f"Markdown：{md_path}")


@reports_app.command("next-candidate-research-gate")
def next_candidate_research_gate_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Next candidate research gate 日期。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
) -> None:
    """TRADING-457：生成 next candidate research gate。"""
    report_date = _parse_date(as_of) if as_of else date.today()
    _, frozen = _load_next_research_source_payload(
        report_type=next_research_reports.FROZEN_SPEC_REPORT_TYPE,
        report_date=report_date,
        reports_dir=reports_dir,
        label="next candidate frozen spec",
    )
    _, backfill = _load_next_research_source_payload(
        report_type=next_research_reports.BACKFILL_REPORT_TYPE,
        report_date=report_date,
        reports_dir=reports_dir,
        label="next candidate backfill",
    )
    _, safety_audit = _load_executable_binding_source_payload(
        report_type=executable_binding_reports.SAFETY_AUDIT_REPORT_TYPE,
        report_date=report_date,
        reports_dir=reports_dir,
        label="executable binding safety audit",
    )
    _, stress = _load_next_research_source_payload(
        report_type=next_research_reports.STRESS_REVIEW_REPORT_TYPE,
        report_date=report_date,
        reports_dir=reports_dir,
        label="next candidate stress review",
    )
    _, cost_benchmark = _load_next_research_source_payload(
        report_type=next_research_reports.COST_BENCHMARK_REVIEW_REPORT_TYPE,
        report_date=report_date,
        reports_dir=reports_dir,
        label="next candidate cost benchmark review",
    )
    _, comparison = _load_next_research_source_payload(
        report_type=next_research_reports.VS_RETURNED_REPORT_TYPE,
        report_date=report_date,
        reports_dir=reports_dir,
        label="next candidate comparison",
    )
    _, signal = _load_next_research_source_payload(
        report_type=next_research_reports.SIGNAL_ROBUSTNESS_REPORT_TYPE,
        report_date=report_date,
        reports_dir=reports_dir,
        label="next candidate signal robustness",
    )
    _, window = _load_next_research_source_payload(
        report_type=next_research_reports.WINDOW_SENSITIVITY_REPORT_TYPE,
        report_date=report_date,
        reports_dir=reports_dir,
        label="next candidate window sensitivity",
    )
    payload = next_research_reports.build_next_candidate_research_gate_payload(
        as_of=report_date,
        frozen_spec_payload=frozen,
        safety_audit_payload=safety_audit,
        backfill_payload=backfill,
        stress_review_payload=stress,
        cost_benchmark_payload=cost_benchmark,
        comparison_payload=comparison,
        signal_robustness_payload=signal,
        window_sensitivity_payload=window,
    )
    json_path, md_path = _write_next_research_cycle_report(
        payload,
        reports_dir=reports_dir,
        report_date=report_date,
    )
    console.print(f"[yellow]Next candidate research gate：{payload['status']}[/yellow]")
    console.print(f"JSON：{json_path}")
    console.print(f"Markdown：{md_path}")


@reports_app.command("next-candidate-owner-research-review-packet")
def next_candidate_owner_research_review_packet_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Owner research review packet 日期。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
) -> None:
    """TRADING-458：生成 owner research review packet。"""
    report_date = _parse_date(as_of) if as_of else date.today()
    _, gate = _load_next_research_source_payload(
        report_type=next_research_reports.RESEARCH_GATE_REPORT_TYPE,
        report_date=report_date,
        reports_dir=reports_dir,
        label="next candidate research gate",
    )
    payload = next_research_reports.build_next_candidate_owner_research_review_packet_payload(
        as_of=report_date,
        research_gate_payload=gate,
    )
    json_path, md_path = _write_next_research_cycle_report(
        payload,
        reports_dir=reports_dir,
        report_date=report_date,
    )
    console.print(f"[green]Owner research review packet：{payload['status']}[/green]")
    console.print(f"JSON：{json_path}")
    console.print(f"Markdown：{md_path}")


@reports_app.command("next-candidate-research-cycle-snapshot")
def next_candidate_research_cycle_snapshot_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Next research cycle snapshot 日期。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
) -> None:
    """TRADING-459：生成 next research cycle final snapshot。"""
    report_date = _parse_date(as_of) if as_of else date.today()
    payloads: dict[str, dict[str, object]] = {}
    for report_type in next_research_reports.NEXT_RESEARCH_CYCLE_REPORT_TYPES[:-1]:
        _, payload = _load_next_research_source_payload(
            report_type=report_type,
            report_date=report_date,
            reports_dir=reports_dir,
            label=report_type,
        )
        payloads[report_type] = payload
    executable_payloads: dict[str, dict[str, object]] = {}
    for report_type in (
        executable_binding_reports.CONTRACT_REPORT_TYPE,
        executable_binding_reports.SIGNAL_BINDING_REPORT_TYPE,
        executable_binding_reports.WEIGHT_BINDING_REPORT_TYPE,
        executable_binding_reports.SAFETY_AUDIT_REPORT_TYPE,
    ):
        _, payload = _load_executable_binding_source_payload(
            report_type=report_type,
            report_date=report_date,
            reports_dir=reports_dir,
            label=report_type,
        )
        executable_payloads[report_type] = payload
    payload = next_research_reports.build_next_research_cycle_snapshot_payload(
        as_of=report_date,
        intake_payload=payloads[next_research_reports.INTAKE_REPORT_TYPE],
        frozen_spec_payload=payloads[next_research_reports.FROZEN_SPEC_REPORT_TYPE],
        executable_contract_payload=executable_payloads[
            executable_binding_reports.CONTRACT_REPORT_TYPE
        ],
        signal_binding_payload=executable_payloads[
            executable_binding_reports.SIGNAL_BINDING_REPORT_TYPE
        ],
        weight_binding_payload=executable_payloads[
            executable_binding_reports.WEIGHT_BINDING_REPORT_TYPE
        ],
        safety_audit_payload=executable_payloads[
            executable_binding_reports.SAFETY_AUDIT_REPORT_TYPE
        ],
        backfill_payload=payloads[next_research_reports.BACKFILL_REPORT_TYPE],
        stress_review_payload=payloads[next_research_reports.STRESS_REVIEW_REPORT_TYPE],
        cost_benchmark_payload=payloads[
            next_research_reports.COST_BENCHMARK_REVIEW_REPORT_TYPE
        ],
        comparison_payload=payloads[next_research_reports.VS_RETURNED_REPORT_TYPE],
        signal_robustness_payload=payloads[
            next_research_reports.SIGNAL_ROBUSTNESS_REPORT_TYPE
        ],
        window_sensitivity_payload=payloads[
            next_research_reports.WINDOW_SENSITIVITY_REPORT_TYPE
        ],
        research_gate_payload=payloads[next_research_reports.RESEARCH_GATE_REPORT_TYPE],
        owner_packet_payload=payloads[
            next_research_reports.OWNER_REVIEW_PACKET_REPORT_TYPE
        ],
    )
    json_path, md_path = _write_next_research_cycle_report(
        payload,
        reports_dir=reports_dir,
        report_date=report_date,
    )
    console.print(f"[yellow]Next research cycle snapshot：{payload['status']}[/yellow]")
    console.print(f"JSON：{json_path}")
    console.print(f"Markdown：{md_path}")


@reports_app.command("executable-research-evidence-gap-ledger")
def executable_research_evidence_gap_ledger_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Evidence gap ledger 日期。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Evidence gap ledger JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Evidence gap ledger Markdown 输出路径。"),
    ] = None,
) -> None:
    """TRADING-471：生成 executable research evidence gap ledger。"""
    report_date = _parse_date(as_of) if as_of else date.today()
    try:
        payload = (
            evidence_repair_reports.build_executable_research_evidence_gap_ledger_payload(
                as_of=report_date,
                reports_dir=reports_dir,
            )
        )
    except (FileNotFoundError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    json_path, md_path = _write_evidence_repair_report(
        payload,
        reports_dir=reports_dir,
        report_date=report_date,
        json_output_path=json_output_path,
        markdown_output_path=markdown_output_path,
    )
    summary = payload["summary"]
    console.print(f"[yellow]Evidence gap ledger：{payload['status']}[/yellow]")
    console.print(f"gap_count：{summary['gap_count']}")
    console.print(f"blocking_gap_count：{summary['blocking_gap_count']}")
    console.print(f"candidate_redesign_gap_count：{summary['candidate_redesign_gap_count']}")
    console.print(f"JSON：{json_path}")
    console.print(f"Markdown：{md_path}")


@reports_app.command("next-candidate-executable-binding-contract")
def next_candidate_executable_binding_contract_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Executable binding contract 日期。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Contract JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Contract Markdown 输出路径。"),
    ] = None,
) -> None:
    """TRADING-460：定义 frozen next candidate executable binding contract。"""
    report_date = _parse_date(as_of) if as_of else date.today()
    try:
        payload = (
            executable_binding_reports.build_next_candidate_executable_binding_contract_payload(
                as_of=report_date,
                reports_dir=reports_dir,
            )
        )
    except (FileNotFoundError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    json_path, md_path = _write_executable_binding_report(
        payload,
        reports_dir=reports_dir,
        report_date=report_date,
        json_output_path=json_output_path,
        markdown_output_path=markdown_output_path,
    )
    summary = payload["summary"]
    console.print(f"[green]Executable binding contract：{payload['status']}[/green]")
    console.print(f"candidate_id：{summary['candidate_id']}")
    console.print(f"binding_version：{summary['binding_version']}")
    console.print(f"JSON：{json_path}")
    console.print(f"Markdown：{md_path}")


@reports_app.command("next-candidate-signal-binding")
def next_candidate_signal_binding_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Next candidate signal binding 日期。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    project_root: Annotated[
        Path,
        typer.Option(help="用于解析相对 signal/feature input path 的项目根目录。"),
    ] = PROJECT_ROOT,
    signal_input_policy_path: Annotated[
        Path,
        typer.Option(help="Signal input completeness policy 路径。"),
    ] = executable_binding_reports.DEFAULT_SIGNAL_INPUT_POLICY_PATH,
    signal_binding_policy_path: Annotated[
        Path,
        typer.Option(help="Signal binding governance policy 路径。"),
    ] = executable_binding_reports.DEFAULT_SIGNAL_BINDING_POLICY_PATH,
    prices_path: Annotated[
        Path,
        typer.Option(help="标准化日线价格 CSV 路径。"),
    ] = PROJECT_ROOT
    / "data"
    / "raw"
    / "prices_daily.csv",
    rates_path: Annotated[
        Path,
        typer.Option(help="标准化 FRED 宏观序列 CSV 路径。"),
    ] = PROJECT_ROOT
    / "data"
    / "raw"
    / "rates_daily.csv",
    data_quality_output_path: Annotated[
        Path | None,
        typer.Option(help="数据质量 Markdown 输出路径；不传时按日期使用默认报告路径。"),
    ] = None,
    full_universe: Annotated[
        bool,
        typer.Option("--full-universe", help="按完整 AI 产业链标的运行 validate-data。"),
    ] = False,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Signal binding JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Signal binding Markdown 输出路径。"),
    ] = None,
) -> None:
    """TRADING-461：生成 research-only next candidate signal binding。"""
    report_date = _parse_date(as_of) if as_of else date.today()
    data_quality_gate = _run_next_research_data_quality_gate(
        report_date=report_date,
        reports_dir=reports_dir,
        prices_path=prices_path,
        rates_path=rates_path,
        data_quality_output_path=data_quality_output_path,
        full_universe=full_universe,
    )
    console.print(
        f"数据质量状态：{data_quality_gate['status']}；"
        f"报告：{data_quality_gate['report_path']}"
    )
    if data_quality_gate["passed"] is not True:
        raise typer.Exit(code=1)
    try:
        payload = executable_binding_reports.build_next_candidate_signal_binding_payload(
            as_of=report_date,
            reports_dir=reports_dir,
            project_root=project_root,
            signal_input_policy_path=signal_input_policy_path,
            signal_binding_policy_path=signal_binding_policy_path,
            data_quality_gate=data_quality_gate,
        )
    except (FileNotFoundError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    json_path, md_path = _write_executable_binding_report(
        payload,
        reports_dir=reports_dir,
        report_date=report_date,
        json_output_path=json_output_path,
        markdown_output_path=markdown_output_path,
    )
    summary = payload["summary"]
    style = (
        "red"
        if payload["status"] == executable_binding_reports.SIGNAL_BINDING_BLOCKED
        else "yellow"
    )
    console.print(f"[{style}]Next candidate signal binding：{payload['status']}[/{style}]")
    console.print(f"candidate_id：{summary['candidate_id']}")
    console.print(f"latest_signal_date：{summary['latest_signal_date']}")
    console.print(f"signal_rows：{summary['signal_row_count']}")
    console.print(f"JSON：{json_path}")
    console.print(f"Markdown：{md_path}")


@reports_app.command("next-candidate-research-weight-binding")
def next_candidate_research_weight_binding_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Next candidate research weight binding 日期。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    weight_binding_policy_path: Annotated[
        Path,
        typer.Option(help="Research weight binding governance policy 路径。"),
    ] = executable_binding_reports.DEFAULT_WEIGHT_BINDING_POLICY_PATH,
    prices_path: Annotated[
        Path,
        typer.Option(help="标准化日线价格 CSV 路径。"),
    ] = PROJECT_ROOT
    / "data"
    / "raw"
    / "prices_daily.csv",
    rates_path: Annotated[
        Path,
        typer.Option(help="标准化 FRED 宏观序列 CSV 路径。"),
    ] = PROJECT_ROOT
    / "data"
    / "raw"
    / "rates_daily.csv",
    data_quality_output_path: Annotated[
        Path | None,
        typer.Option(help="数据质量 Markdown 输出路径；不传时按日期使用默认报告路径。"),
    ] = None,
    full_universe: Annotated[
        bool,
        typer.Option("--full-universe", help="按完整 AI 产业链标的运行 validate-data。"),
    ] = False,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Research weight binding JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Research weight binding Markdown 输出路径。"),
    ] = None,
) -> None:
    """TRADING-462：生成 research-only hypothetical weight binding。"""
    report_date = _parse_date(as_of) if as_of else date.today()
    data_quality_gate = _run_next_research_data_quality_gate(
        report_date=report_date,
        reports_dir=reports_dir,
        prices_path=prices_path,
        rates_path=rates_path,
        data_quality_output_path=data_quality_output_path,
        full_universe=full_universe,
    )
    console.print(
        f"数据质量状态：{data_quality_gate['status']}；"
        f"报告：{data_quality_gate['report_path']}"
    )
    if data_quality_gate["passed"] is not True:
        raise typer.Exit(code=1)
    try:
        payload = (
            executable_binding_reports.build_next_candidate_research_weight_binding_payload(
                as_of=report_date,
                reports_dir=reports_dir,
                weight_binding_policy_path=weight_binding_policy_path,
                data_quality_gate=data_quality_gate,
            )
        )
    except (FileNotFoundError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    json_path, md_path = _write_executable_binding_report(
        payload,
        reports_dir=reports_dir,
        report_date=report_date,
        json_output_path=json_output_path,
        markdown_output_path=markdown_output_path,
    )
    summary = payload["summary"]
    style = (
        "red"
        if payload["status"] == executable_binding_reports.WEIGHT_BINDING_BLOCKED
        else "yellow"
    )
    console.print(
        f"[{style}]Next candidate research weight binding："
        f"{payload['status']}[/{style}]"
    )
    console.print(f"candidate_id：{summary['candidate_id']}")
    console.print(f"latest_signal_date：{summary['latest_signal_date']}")
    console.print(f"turnover_proxy：{summary['turnover_proxy']}")
    console.print(f"JSON：{json_path}")
    console.print(f"Markdown：{md_path}")


@reports_app.command("executable-binding-safety-audit")
def executable_binding_safety_audit_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Executable binding safety audit 日期。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    project_root: Annotated[
        Path,
        typer.Option(help="用于解析相对 scan path 的项目根目录。"),
    ] = PROJECT_ROOT,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Safety audit JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Safety audit Markdown 输出路径。"),
    ] = None,
) -> None:
    """TRADING-463：审计 executable binding safety boundary。"""
    report_date = _parse_date(as_of) if as_of else date.today()
    try:
        payload = executable_binding_reports.build_executable_binding_safety_audit_payload(
            as_of=report_date,
            reports_dir=reports_dir,
            project_root=project_root,
        )
    except (FileNotFoundError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    json_path, md_path = _write_executable_binding_report(
        payload,
        reports_dir=reports_dir,
        report_date=report_date,
        json_output_path=json_output_path,
        markdown_output_path=markdown_output_path,
    )
    summary = payload["summary"]
    style = (
        "red"
        if payload["status"] == executable_binding_reports.SAFETY_BLOCKED
        else "yellow"
        if payload["status"] == executable_binding_reports.SAFETY_WARNING
        else "green"
    )
    console.print(f"[{style}]Executable binding safety audit：{payload['status']}[/{style}]")
    console.print(f"candidate_id：{summary['candidate_id']}")
    console.print(f"artifact_failures：{summary['failed_artifact_check_count']}")
    console.print(f"static_blockers：{summary['blocking_static_finding_count']}")
    console.print(f"JSON：{json_path}")
    console.print(f"Markdown：{md_path}")


def _validate_next_research_cycle_command(
    *,
    expected_report_type: str,
    latest: bool,
    as_of: str | None,
    reports_dir: Path,
    source_json_path: Path | None,
    json_output_path: Path | None,
    markdown_output_path: Path | None,
) -> None:
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --as-of/--date 同时使用")
    report_date = _parse_date(as_of) if as_of else date.today()
    source_path, source_payload = _load_next_research_source_payload(
        report_type=expected_report_type,
        report_date=report_date,
        reports_dir=reports_dir,
        latest=latest,
        source_json_path=source_json_path,
        label=expected_report_type,
    )
    payload, json_path, md_path = _write_next_research_validation(
        source_payload,
        expected_report_type=expected_report_type,
        reports_dir=reports_dir,
        json_output_path=json_output_path,
        markdown_output_path=markdown_output_path,
    )
    status = payload["status"]
    style = "green" if status == "PASS" else "red"
    summary = payload["summary"]
    console.print(f"[{style}]{expected_report_type} validation：{status}[/{style}]")
    console.print(f"Source JSON：{source_path}")
    console.print(f"Validation JSON：{json_path}")
    console.print(f"Validation Markdown：{md_path}")
    console.print(
        f"checks：{summary['check_count']}；"
        f"failed：{summary['failed_check_count']}；"
        f"production_effect={payload['production_effect']}"
    )
    if status == "FAIL":
        raise typer.Exit(code=1)


def _validate_executable_binding_command(
    *,
    expected_report_type: str,
    latest: bool,
    as_of: str | None,
    reports_dir: Path,
    source_json_path: Path | None,
    json_output_path: Path | None,
    markdown_output_path: Path | None,
) -> None:
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --as-of/--date 同时使用")
    report_date = _parse_date(as_of) if as_of else date.today()
    source_path, source_payload = _load_executable_binding_source_payload(
        report_type=expected_report_type,
        report_date=report_date,
        reports_dir=reports_dir,
        latest=latest,
        source_json_path=source_json_path,
        label=expected_report_type,
    )
    payload, json_path, md_path = _write_executable_binding_validation(
        source_payload,
        expected_report_type=expected_report_type,
        reports_dir=reports_dir,
        json_output_path=json_output_path,
        markdown_output_path=markdown_output_path,
    )
    status = payload["status"]
    style = "green" if status == "PASS" else "red"
    summary = payload["summary"]
    console.print(f"[{style}]{expected_report_type} validation：{status}[/{style}]")
    console.print(f"Source JSON：{source_path}")
    console.print(f"Validation JSON：{json_path}")
    console.print(f"Validation Markdown：{md_path}")
    console.print(
        f"checks：{summary['check_count']}；"
        f"failed：{summary['failed_check_count']}；"
        f"production_effect={payload['production_effect']}"
    )
    if status == "FAIL":
        raise typer.Exit(code=1)


@reports_app.command("validate-next-research-cycle-intake")
def validate_next_research_cycle_intake_command(
    latest: Annotated[bool, typer.Option(help="校验 latest intake artifact。")] = False,
    as_of: Annotated[str | None, typer.Option("--as-of", "--date")] = None,
    reports_dir: Annotated[Path, typer.Option(help="报告 artifact 所在目录。")] = PROJECT_ROOT
    / "outputs"
    / "reports",
    source_json_path: Annotated[Path | None, typer.Option(help="Source JSON 路径。")] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Validation JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Validation Markdown 输出路径。"),
    ] = None,
) -> None:
    _validate_next_research_cycle_command(
        expected_report_type=next_research_reports.INTAKE_REPORT_TYPE,
        latest=latest,
        as_of=as_of,
        reports_dir=reports_dir,
        source_json_path=source_json_path,
        json_output_path=json_output_path,
        markdown_output_path=markdown_output_path,
    )


@reports_app.command("validate-next-candidate-spec-frozen")
def validate_next_candidate_spec_frozen_command(
    latest: Annotated[bool, typer.Option(help="校验 latest frozen spec artifact。")] = False,
    as_of: Annotated[str | None, typer.Option("--as-of", "--date")] = None,
    reports_dir: Annotated[Path, typer.Option(help="报告 artifact 所在目录。")] = PROJECT_ROOT
    / "outputs"
    / "reports",
    source_json_path: Annotated[Path | None, typer.Option(help="Source JSON 路径。")] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Validation JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Validation Markdown 输出路径。"),
    ] = None,
) -> None:
    _validate_next_research_cycle_command(
        expected_report_type=next_research_reports.FROZEN_SPEC_REPORT_TYPE,
        latest=latest,
        as_of=as_of,
        reports_dir=reports_dir,
        source_json_path=source_json_path,
        json_output_path=json_output_path,
        markdown_output_path=markdown_output_path,
    )


@reports_app.command("validate-next-candidate-backfill")
def validate_next_candidate_backfill_command(
    latest: Annotated[bool, typer.Option(help="校验 latest backfill artifact。")] = False,
    as_of: Annotated[str | None, typer.Option("--as-of", "--date")] = None,
    reports_dir: Annotated[Path, typer.Option(help="报告 artifact 所在目录。")] = PROJECT_ROOT
    / "outputs"
    / "reports",
    source_json_path: Annotated[Path | None, typer.Option(help="Source JSON 路径。")] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Validation JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Validation Markdown 输出路径。"),
    ] = None,
) -> None:
    _validate_next_research_cycle_command(
        expected_report_type=next_research_reports.BACKFILL_REPORT_TYPE,
        latest=latest,
        as_of=as_of,
        reports_dir=reports_dir,
        source_json_path=source_json_path,
        json_output_path=json_output_path,
        markdown_output_path=markdown_output_path,
    )


@reports_app.command("validate-next-candidate-stress-review")
def validate_next_candidate_stress_review_command(
    latest: Annotated[bool, typer.Option(help="校验 latest stress review artifact。")] = False,
    as_of: Annotated[str | None, typer.Option("--as-of", "--date")] = None,
    reports_dir: Annotated[Path, typer.Option(help="报告 artifact 所在目录。")] = PROJECT_ROOT
    / "outputs"
    / "reports",
    source_json_path: Annotated[Path | None, typer.Option(help="Source JSON 路径。")] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Validation JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Validation Markdown 输出路径。"),
    ] = None,
) -> None:
    _validate_next_research_cycle_command(
        expected_report_type=next_research_reports.STRESS_REVIEW_REPORT_TYPE,
        latest=latest,
        as_of=as_of,
        reports_dir=reports_dir,
        source_json_path=source_json_path,
        json_output_path=json_output_path,
        markdown_output_path=markdown_output_path,
    )


@reports_app.command("validate-next-candidate-cost-benchmark-review")
def validate_next_candidate_cost_benchmark_review_command(
    latest: Annotated[bool, typer.Option(help="校验 latest cost/benchmark artifact。")] = False,
    as_of: Annotated[str | None, typer.Option("--as-of", "--date")] = None,
    reports_dir: Annotated[Path, typer.Option(help="报告 artifact 所在目录。")] = PROJECT_ROOT
    / "outputs"
    / "reports",
    source_json_path: Annotated[Path | None, typer.Option(help="Source JSON 路径。")] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Validation JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Validation Markdown 输出路径。"),
    ] = None,
) -> None:
    _validate_next_research_cycle_command(
        expected_report_type=next_research_reports.COST_BENCHMARK_REVIEW_REPORT_TYPE,
        latest=latest,
        as_of=as_of,
        reports_dir=reports_dir,
        source_json_path=source_json_path,
        json_output_path=json_output_path,
        markdown_output_path=markdown_output_path,
    )


@reports_app.command("validate-next-candidate-vs-returned-candidate-comparison")
def validate_next_candidate_vs_returned_comparison_command(
    latest: Annotated[bool, typer.Option(help="校验 latest comparison artifact。")] = False,
    as_of: Annotated[str | None, typer.Option("--as-of", "--date")] = None,
    reports_dir: Annotated[Path, typer.Option(help="报告 artifact 所在目录。")] = PROJECT_ROOT
    / "outputs"
    / "reports",
    source_json_path: Annotated[Path | None, typer.Option(help="Source JSON 路径。")] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Validation JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Validation Markdown 输出路径。"),
    ] = None,
) -> None:
    _validate_next_research_cycle_command(
        expected_report_type=next_research_reports.VS_RETURNED_REPORT_TYPE,
        latest=latest,
        as_of=as_of,
        reports_dir=reports_dir,
        source_json_path=source_json_path,
        json_output_path=json_output_path,
        markdown_output_path=markdown_output_path,
    )


@reports_app.command("validate-next-candidate-signal-robustness-review")
def validate_next_candidate_signal_robustness_review_command(
    latest: Annotated[bool, typer.Option(help="校验 latest signal robustness artifact。")] = False,
    as_of: Annotated[str | None, typer.Option("--as-of", "--date")] = None,
    reports_dir: Annotated[Path, typer.Option(help="报告 artifact 所在目录。")] = PROJECT_ROOT
    / "outputs"
    / "reports",
    source_json_path: Annotated[Path | None, typer.Option(help="Source JSON 路径。")] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Validation JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Validation Markdown 输出路径。"),
    ] = None,
) -> None:
    _validate_next_research_cycle_command(
        expected_report_type=next_research_reports.SIGNAL_ROBUSTNESS_REPORT_TYPE,
        latest=latest,
        as_of=as_of,
        reports_dir=reports_dir,
        source_json_path=source_json_path,
        json_output_path=json_output_path,
        markdown_output_path=markdown_output_path,
    )


@reports_app.command("validate-next-candidate-overfit-window-sensitivity")
def validate_next_candidate_window_sensitivity_command(
    latest: Annotated[bool, typer.Option(help="校验 latest window sensitivity artifact。")] = False,
    as_of: Annotated[str | None, typer.Option("--as-of", "--date")] = None,
    reports_dir: Annotated[Path, typer.Option(help="报告 artifact 所在目录。")] = PROJECT_ROOT
    / "outputs"
    / "reports",
    source_json_path: Annotated[Path | None, typer.Option(help="Source JSON 路径。")] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Validation JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Validation Markdown 输出路径。"),
    ] = None,
) -> None:
    _validate_next_research_cycle_command(
        expected_report_type=next_research_reports.WINDOW_SENSITIVITY_REPORT_TYPE,
        latest=latest,
        as_of=as_of,
        reports_dir=reports_dir,
        source_json_path=source_json_path,
        json_output_path=json_output_path,
        markdown_output_path=markdown_output_path,
    )


@reports_app.command("validate-next-candidate-research-gate")
def validate_next_candidate_research_gate_command(
    latest: Annotated[bool, typer.Option(help="校验 latest research gate artifact。")] = False,
    as_of: Annotated[str | None, typer.Option("--as-of", "--date")] = None,
    reports_dir: Annotated[Path, typer.Option(help="报告 artifact 所在目录。")] = PROJECT_ROOT
    / "outputs"
    / "reports",
    source_json_path: Annotated[Path | None, typer.Option(help="Source JSON 路径。")] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Validation JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Validation Markdown 输出路径。"),
    ] = None,
) -> None:
    _validate_next_research_cycle_command(
        expected_report_type=next_research_reports.RESEARCH_GATE_REPORT_TYPE,
        latest=latest,
        as_of=as_of,
        reports_dir=reports_dir,
        source_json_path=source_json_path,
        json_output_path=json_output_path,
        markdown_output_path=markdown_output_path,
    )


@reports_app.command("validate-next-candidate-owner-research-review-packet")
def validate_next_candidate_owner_research_review_packet_command(
    latest: Annotated[bool, typer.Option(help="校验 latest owner packet artifact。")] = False,
    as_of: Annotated[str | None, typer.Option("--as-of", "--date")] = None,
    reports_dir: Annotated[Path, typer.Option(help="报告 artifact 所在目录。")] = PROJECT_ROOT
    / "outputs"
    / "reports",
    source_json_path: Annotated[Path | None, typer.Option(help="Source JSON 路径。")] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Validation JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Validation Markdown 输出路径。"),
    ] = None,
) -> None:
    _validate_next_research_cycle_command(
        expected_report_type=next_research_reports.OWNER_REVIEW_PACKET_REPORT_TYPE,
        latest=latest,
        as_of=as_of,
        reports_dir=reports_dir,
        source_json_path=source_json_path,
        json_output_path=json_output_path,
        markdown_output_path=markdown_output_path,
    )


@reports_app.command("validate-next-candidate-research-cycle-snapshot")
def validate_next_candidate_research_cycle_snapshot_command(
    latest: Annotated[bool, typer.Option(help="校验 latest research cycle snapshot。")] = False,
    as_of: Annotated[str | None, typer.Option("--as-of", "--date")] = None,
    reports_dir: Annotated[Path, typer.Option(help="报告 artifact 所在目录。")] = PROJECT_ROOT
    / "outputs"
    / "reports",
    source_json_path: Annotated[Path | None, typer.Option(help="Source JSON 路径。")] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Validation JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Validation Markdown 输出路径。"),
    ] = None,
) -> None:
    _validate_next_research_cycle_command(
        expected_report_type=next_research_reports.CYCLE_SNAPSHOT_REPORT_TYPE,
        latest=latest,
        as_of=as_of,
        reports_dir=reports_dir,
        source_json_path=source_json_path,
        json_output_path=json_output_path,
        markdown_output_path=markdown_output_path,
    )


@reports_app.command("validate-executable-research-evidence-gap-ledger")
def validate_executable_research_evidence_gap_ledger_command(
    latest: Annotated[
        bool,
        typer.Option(help="校验 latest executable research evidence gap ledger。"),
    ] = False,
    as_of: Annotated[str | None, typer.Option("--as-of", "--date")] = None,
    reports_dir: Annotated[Path, typer.Option(help="报告 artifact 所在目录。")] = PROJECT_ROOT
    / "outputs"
    / "reports",
    source_json_path: Annotated[Path | None, typer.Option(help="Source JSON 路径。")] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Validation JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Validation Markdown 输出路径。"),
    ] = None,
) -> None:
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --as-of/--date 同时使用")
    report_date = _parse_date(as_of) if as_of else date.today()
    source_path, source_payload = _load_evidence_repair_source_payload(
        report_type=evidence_repair_reports.EVIDENCE_GAP_LEDGER_REPORT_TYPE,
        report_date=report_date,
        reports_dir=reports_dir,
        latest=latest,
        source_json_path=source_json_path,
        label="executable research evidence gap ledger",
    )
    payload, json_path, md_path = _write_evidence_repair_validation(
        source_payload,
        reports_dir=reports_dir,
        json_output_path=json_output_path,
        markdown_output_path=markdown_output_path,
    )
    status = payload["status"]
    style = "green" if status == "PASS" else "red"
    summary = payload["summary"]
    console.print(f"[{style}]Evidence gap ledger validation：{status}[/{style}]")
    console.print(f"Source JSON：{source_path}")
    console.print(f"Validation JSON：{json_path}")
    console.print(f"Validation Markdown：{md_path}")
    console.print(
        f"checks：{summary['check_count']}；"
        f"failed：{summary['failed_check_count']}；"
        f"production_effect={payload['production_effect']}"
    )
    if status == "FAIL":
        raise typer.Exit(code=1)


@reports_app.command("validate-next-candidate-executable-binding-contract")
def validate_next_candidate_executable_binding_contract_command(
    latest: Annotated[
        bool,
        typer.Option(help="校验 latest executable binding contract artifact。"),
    ] = False,
    as_of: Annotated[str | None, typer.Option("--as-of", "--date")] = None,
    reports_dir: Annotated[Path, typer.Option(help="报告 artifact 所在目录。")] = PROJECT_ROOT
    / "outputs"
    / "reports",
    source_json_path: Annotated[Path | None, typer.Option(help="Source JSON 路径。")] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Validation JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Validation Markdown 输出路径。"),
    ] = None,
) -> None:
    _validate_executable_binding_command(
        expected_report_type=executable_binding_reports.CONTRACT_REPORT_TYPE,
        latest=latest,
        as_of=as_of,
        reports_dir=reports_dir,
        source_json_path=source_json_path,
        json_output_path=json_output_path,
        markdown_output_path=markdown_output_path,
    )


@reports_app.command("validate-next-candidate-signal-binding")
def validate_next_candidate_signal_binding_command(
    latest: Annotated[
        bool,
        typer.Option(help="校验 latest next candidate signal binding artifact。"),
    ] = False,
    as_of: Annotated[str | None, typer.Option("--as-of", "--date")] = None,
    reports_dir: Annotated[Path, typer.Option(help="报告 artifact 所在目录。")] = PROJECT_ROOT
    / "outputs"
    / "reports",
    source_json_path: Annotated[Path | None, typer.Option(help="Source JSON 路径。")] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Validation JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Validation Markdown 输出路径。"),
    ] = None,
) -> None:
    _validate_executable_binding_command(
        expected_report_type=executable_binding_reports.SIGNAL_BINDING_REPORT_TYPE,
        latest=latest,
        as_of=as_of,
        reports_dir=reports_dir,
        source_json_path=source_json_path,
        json_output_path=json_output_path,
        markdown_output_path=markdown_output_path,
    )


@reports_app.command("validate-next-candidate-research-weight-binding")
def validate_next_candidate_research_weight_binding_command(
    latest: Annotated[
        bool,
        typer.Option(help="校验 latest next candidate research weight binding artifact。"),
    ] = False,
    as_of: Annotated[str | None, typer.Option("--as-of", "--date")] = None,
    reports_dir: Annotated[Path, typer.Option(help="报告 artifact 所在目录。")] = PROJECT_ROOT
    / "outputs"
    / "reports",
    source_json_path: Annotated[Path | None, typer.Option(help="Source JSON 路径。")] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Validation JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Validation Markdown 输出路径。"),
    ] = None,
) -> None:
    _validate_executable_binding_command(
        expected_report_type=executable_binding_reports.WEIGHT_BINDING_REPORT_TYPE,
        latest=latest,
        as_of=as_of,
        reports_dir=reports_dir,
        source_json_path=source_json_path,
        json_output_path=json_output_path,
        markdown_output_path=markdown_output_path,
    )


@reports_app.command("validate-executable-binding-safety-audit")
def validate_executable_binding_safety_audit_command(
    latest: Annotated[
        bool,
        typer.Option(help="校验 latest executable binding safety audit artifact。"),
    ] = False,
    as_of: Annotated[str | None, typer.Option("--as-of", "--date")] = None,
    reports_dir: Annotated[Path, typer.Option(help="报告 artifact 所在目录。")] = PROJECT_ROOT
    / "outputs"
    / "reports",
    source_json_path: Annotated[Path | None, typer.Option(help="Source JSON 路径。")] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Validation JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Validation Markdown 输出路径。"),
    ] = None,
) -> None:
    _validate_executable_binding_command(
        expected_report_type=executable_binding_reports.SAFETY_AUDIT_REPORT_TYPE,
        latest=latest,
        as_of=as_of,
        reports_dir=reports_dir,
        source_json_path=source_json_path,
        json_output_path=json_output_path,
        markdown_output_path=markdown_output_path,
    )


@reports_app.command("recovery-governance-rerun-after-triage")
def recovery_governance_rerun_after_triage_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Recovery governance rerun 日期。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    report_index_path: Annotated[
        Path | None,
        typer.Option(help="Report index JSON 路径。"),
    ] = None,
    project_root: Annotated[
        Path,
        typer.Option(help="用于解析相对 artifact path 的项目根目录。"),
    ] = PROJECT_ROOT,
    blocker_triage_path: Annotated[
        Path | None,
        typer.Option(help="Recovery blocker triage JSON 路径。"),
    ] = None,
    warning_triage_path: Annotated[
        Path | None,
        typer.Option(help="Report index warning triage JSON 路径。"),
    ] = None,
    source_depth_audit_path: Annotated[
        Path | None,
        typer.Option(help="Recovery source depth audit JSON 路径。"),
    ] = None,
    owner_action_map_path: Annotated[
        Path | None,
        typer.Option(help="Recovery owner action map JSON 路径。"),
    ] = None,
) -> None:
    """TRADING-407：triage 后只读 rerun canonical recovery governance pack。"""
    report_date = _parse_date(as_of) if as_of else date.today()
    source_index = report_index_path or default_report_index_json_path(reports_dir, report_date)
    raw_index = _read_json_mapping_for_report_cli(source_index, "Report index")
    report_date = _parse_date(str(raw_index.get("as_of") or report_date.isoformat()))
    blocker_path = (
        blocker_triage_path
        or recovery_triage_reports.default_recovery_blocker_triage_json_path(
            reports_dir,
            report_date,
        )
    )
    warning_path = (
        warning_triage_path
        or recovery_triage_reports.default_report_index_warning_triage_json_path(
            reports_dir,
            report_date,
        )
    )
    source_audit_path = (
        source_depth_audit_path
        or recovery_triage_reports.default_recovery_pack_source_depth_audit_json_path(
            reports_dir,
            report_date,
        )
    )
    action_map_path = (
        owner_action_map_path
        or recovery_triage_reports.default_recovery_owner_action_map_json_path(
            reports_dir,
            report_date,
        )
    )
    blocker_payload = _read_json_mapping_for_report_cli(blocker_path, "Recovery blocker triage")
    warning_payload = _read_json_mapping_for_report_cli(warning_path, "Report index warning triage")
    source_audit_payload = _read_json_mapping_for_report_cli(
        source_audit_path,
        "Recovery source depth audit",
    )
    action_map_payload = _read_json_mapping_for_report_cli(
        action_map_path,
        "Recovery owner action map",
    )
    recovery_payload = build_research_governance_recovery_pack_payload(
        as_of=report_date,
        report_index_payload=raw_index,
        report_index_path=source_index,
        project_root=project_root,
    )
    triage_context = recovery_triage_reports.build_recovery_governance_rerun_triage_context(
        blocker_triage_payload=blocker_payload,
        report_index_warning_triage_payload=warning_payload,
        source_depth_audit_payload=source_audit_payload,
        owner_action_map_payload=action_map_payload,
        input_artifacts={
            "recovery_blocker_triage": str(blocker_path),
            "report_index_warning_triage": str(warning_path),
            "recovery_pack_source_depth_audit": str(source_audit_path),
            "recovery_owner_action_map": str(action_map_path),
        },
    )
    recovery_payload = recovery_triage_reports.attach_triage_context_to_recovery_pack(
        recovery_payload,
        triage_context,
    )
    report_json = default_research_governance_recovery_pack_json_path(
        reports_dir,
        report_date,
    )
    report_md = default_research_governance_recovery_pack_markdown_path(
        reports_dir,
        report_date,
    )
    json_path = write_research_governance_recovery_pack_json(recovery_payload, report_json)
    md_path = write_research_governance_recovery_pack_markdown(recovery_payload, report_md)
    validation_payload = validate_research_governance_recovery_pack_payload(recovery_payload)
    validation_payload["input_artifacts"] = {
        **dict(validation_payload.get("input_artifacts", {})),
        "research_governance_recovery_pack": str(json_path),
        **triage_context["input_artifacts"],
    }
    validation_payload["triage_context"] = triage_context["summary"]
    validation_json = default_research_governance_recovery_pack_validation_json_path(
        reports_dir,
        report_date,
    )
    validation_md = default_research_governance_recovery_pack_validation_markdown_path(
        reports_dir,
        report_date,
    )
    validation_json_path = write_research_governance_recovery_pack_validation_json(
        validation_payload,
        validation_json,
    )
    validation_md_path = write_research_governance_recovery_pack_validation_markdown(
        validation_payload,
        validation_md,
    )
    summary = recovery_payload["summary"]
    status = recovery_payload["recovery_governance_status"]
    style = "red" if status == "RECOVERY_GOVERNANCE_BLOCKED" else "yellow"
    console.print(f"[{style}]Recovery governance rerun after triage：{status}[/{style}]")
    console.print(f"Research governance recovery pack JSON：{json_path}")
    console.print(f"Research governance recovery pack Markdown：{md_path}")
    console.print(f"Research governance recovery validation JSON：{validation_json_path}")
    console.print(f"Research governance recovery validation Markdown：{validation_md_path}")
    console.print(
        f"remaining_blockers：{summary['remaining_blocker_count']}；"
        f"remaining_warnings：{summary['remaining_warning_count']}；"
        f"normal_shadow：{summary['normal_paper_shadow_may_resume']}；"
        f"extended_forbidden：{summary['extended_shadow_remains_forbidden']}；"
        f"live_forbidden：{summary['live_trading_remains_forbidden']}；"
        f"production_effect={recovery_payload['production_effect']}；只读 rerun"
    )


def _print_recovery_triage_validation_result(
    label: str,
    payload: Mapping[str, object],
    json_path: Path,
    md_path: Path,
) -> None:
    status = str(payload["validation_status"])
    style = "green" if status == "PASS" else "yellow"
    if status == "FAIL":
        style = "red"
    summary = payload["summary"]
    console.print(f"[{style}]{label} validation：{status}[/{style}]")
    console.print(f"{label} validation JSON：{json_path}")
    console.print(f"{label} validation Markdown：{md_path}")
    console.print(
        f"checks：{summary['check_count']}；"
        f"failed：{summary['failed_check_count']}；"
        f"warnings：{summary['warning_check_count']}；"
        f"production_effect={payload['production_effect']}；只读校验"
    )
    if status == "FAIL":
        raise typer.Exit(code=1)


@reports_app.command("recovery-evidence-pack")
def recovery_evidence_pack_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Recovery evidence pack 日期。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option(help="使用 reports_dir 中最新 report_index JSON。"),
    ] = False,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    report_index_path: Annotated[
        Path | None,
        typer.Option(help="Report index JSON 路径；不传时按日期使用默认路径。"),
    ] = None,
    project_root: Annotated[
        Path,
        typer.Option(help="用于解析相对 artifact path 的项目根目录。"),
    ] = PROJECT_ROOT,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Recovery evidence pack JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Recovery evidence pack Markdown 输出路径。"),
    ] = None,
) -> None:
    """生成只读 recovery evidence pack；不运行上游或修改状态。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --as-of/--date 同时使用")
    if latest and report_index_path is None:
        latest_index = max(
            reports_dir.glob("report_index_????-??-??.json"),
            default=None,
            key=lambda path: path.name,
        )
        if latest_index is None:
            raise typer.BadParameter(f"未找到 report index JSON：{reports_dir}")
        source_index = latest_index
    else:
        report_date = _parse_date(as_of) if as_of else date.today()
        source_index = report_index_path or default_report_index_json_path(
            reports_dir,
            report_date,
        )
    try:
        raw_index = json.loads(source_index.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise typer.BadParameter(f"report index JSON not found: {source_index}") from exc
    except json.JSONDecodeError as exc:
        raise typer.BadParameter(f"report index JSON cannot be parsed: {source_index}") from exc
    if not isinstance(raw_index, dict):
        raise typer.BadParameter(f"report index JSON must be an object: {source_index}")
    report_date = _parse_date(
        as_of or str(raw_index.get("as_of") or date.today().isoformat())
    )
    payload = build_recovery_evidence_pack_payload(
        as_of=report_date,
        report_index_payload=raw_index,
        report_index_path=source_index,
        project_root=project_root,
    )
    report_json = json_output_path or default_recovery_evidence_pack_json_path(
        reports_dir,
        report_date,
    )
    report_md = markdown_output_path or default_recovery_evidence_pack_markdown_path(
        reports_dir,
        report_date,
    )
    json_path = write_recovery_evidence_pack_json(payload, report_json)
    md_path = write_recovery_evidence_pack_markdown(payload, report_md)
    status = payload["recovery_evidence_status"]
    style = "green" if status == "RECOVERY_EVIDENCE_COMPLETE" else "yellow"
    if status == "RECOVERY_EVIDENCE_BLOCKED":
        style = "red"
    summary = payload["summary"]
    console.print(f"[{style}]Recovery evidence pack：{status}[/{style}]")
    console.print(f"Recovery evidence pack JSON：{json_path}")
    console.print(f"Recovery evidence pack Markdown：{md_path}")
    console.print(
        f"sources：{summary['source_report_count']}；"
        f"available：{summary['available_source_count']}；"
        f"remaining_blockers：{summary['remaining_recovery_blocker_count']}；"
        f"warnings：{summary['warning_item_count']}；"
        f"next_action：{summary['next_action']}；"
        f"production_effect={payload['production_effect']}；只读 recovery evidence"
    )


@reports_app.command("validate-recovery-evidence-pack")
def validate_recovery_evidence_pack_command(
    latest: Annotated[
        bool,
        typer.Option(help="校验 reports_dir 中最新 recovery evidence pack JSON。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Recovery evidence pack validation 日期。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    source_json_path: Annotated[
        Path | None,
        typer.Option(help="Recovery evidence pack JSON 路径；优先于 --latest/--as-of。"),
    ] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Recovery evidence pack validation JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Recovery evidence pack validation Markdown 输出路径。"),
    ] = None,
) -> None:
    """校验 recovery evidence pack；缺 source 或安全漂移时 fail closed。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --as-of/--date 同时使用")
    if source_json_path is not None:
        source_path = source_json_path
    elif latest:
        latest_path = latest_recovery_evidence_pack_json_path(reports_dir)
        if latest_path is None:
            raise typer.BadParameter(f"未找到 recovery evidence pack JSON：{reports_dir}")
        source_path = latest_path
    else:
        report_date = _parse_date(as_of) if as_of else date.today()
        source_path = default_recovery_evidence_pack_json_path(reports_dir, report_date)
    if not source_path.exists():
        raise typer.BadParameter(f"Recovery evidence pack JSON not found: {source_path}")
    try:
        raw_payload = json.loads(source_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise typer.BadParameter(
            f"Recovery evidence pack JSON cannot be parsed: {source_path}"
        ) from exc
    if not isinstance(raw_payload, dict):
        raise typer.BadParameter(
            f"Recovery evidence pack JSON must be an object: {source_path}"
        )
    payload = validate_recovery_evidence_pack_payload(raw_payload)
    source_artifacts = dict(payload.get("input_artifacts", {}))
    source_artifacts["recovery_evidence_pack"] = str(source_path)
    payload["input_artifacts"] = source_artifacts
    report_date = _parse_date(str(payload.get("as_of") or date.today().isoformat()))
    validation_json = json_output_path or default_recovery_evidence_pack_validation_json_path(
        reports_dir,
        report_date,
    )
    validation_md = (
        markdown_output_path
        or default_recovery_evidence_pack_validation_markdown_path(
            reports_dir,
            report_date,
        )
    )
    json_path = write_recovery_evidence_pack_validation_json(payload, validation_json)
    md_path = write_recovery_evidence_pack_validation_markdown(payload, validation_md)
    status = payload["validation_status"]
    style = "green" if status == "PASS" else "yellow"
    if status == "FAIL":
        style = "red"
    summary = payload["summary"]
    console.print(f"[{style}]Recovery evidence pack validation：{status}[/{style}]")
    console.print(f"Recovery evidence pack validation JSON：{json_path}")
    console.print(f"Recovery evidence pack validation Markdown：{md_path}")
    console.print(
        f"checks：{summary['check_count']}；"
        f"failed：{summary['failed_check_count']}；"
        f"remaining_blockers：{summary['remaining_recovery_blocker_count']}；"
        f"warnings：{summary['warning_item_count']}；"
        f"production_effect={payload['production_effect']}；只读校验"
    )
    if status == "FAIL":
        raise typer.Exit(code=1)


@reports_app.command("research-safety-boundary-audit")
def research_safety_boundary_audit_command(
    as_of: Annotated[
        str | None,
        typer.Option(
            "--as-of",
            "--date",
            help="Research safety boundary audit 日期，格式为 YYYY-MM-DD。",
        ),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option(help="使用默认 decision snapshot 目录中的最新 signal-date。"),
    ] = False,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    report_index_path: Annotated[
        Path | None,
        typer.Option(help="Report index JSON 路径；不传时按日期使用默认路径。"),
    ] = None,
    task_register_path: Annotated[
        Path | None,
        typer.Option(help="active task register 路径。"),
    ] = None,
    completed_task_register_path: Annotated[
        Path | None,
        typer.Option(help="completed task register 路径。"),
    ] = None,
    project_root: Annotated[
        Path,
        typer.Option(help="用于解析相对 artifact path 的项目根目录。"),
    ] = PROJECT_ROOT,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Research safety boundary audit JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Research safety boundary audit Markdown 输出路径。"),
    ] = None,
) -> None:
    """生成 research safety boundary audit；只读扫描 task registers 和 report artifacts。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --as-of/--date 同时使用")
    if latest:
        report_date = _decision_snapshot_date(
            _latest_decision_snapshot_path(DEFAULT_DECISION_SNAPSHOT_DIR)
        )
    else:
        report_date = _parse_date(as_of) if as_of else date.today()
    source_index = report_index_path or default_report_index_json_path(reports_dir, report_date)
    try:
        raw_index = json.loads(source_index.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise typer.BadParameter(f"report index JSON not found: {source_index}") from exc
    except json.JSONDecodeError as exc:
        raise typer.BadParameter(f"report index JSON cannot be parsed: {source_index}") from exc
    if not isinstance(raw_index, dict):
        raise typer.BadParameter(f"report index JSON must be an object: {source_index}")
    payload = build_research_safety_boundary_payload(
        as_of=report_date,
        report_index_payload=raw_index,
        report_index_path=source_index,
        task_register_path=task_register_path,
        completed_task_register_path=completed_task_register_path,
        project_root=project_root,
    )
    audit_json = json_output_path or default_research_safety_boundary_json_path(
        reports_dir,
        report_date,
    )
    audit_md = markdown_output_path or default_research_safety_boundary_markdown_path(
        reports_dir,
        report_date,
    )
    json_path = write_research_safety_boundary_json(payload, audit_json)
    md_path = write_research_safety_boundary_markdown(payload, audit_md)
    status = payload["safety_status"]
    style = "green" if status == "SAFETY_PASS" else "yellow"
    if status == "SAFETY_BLOCKED":
        style = "red"
    summary = payload["summary"]
    console.print(f"[{style}]Research safety boundary audit：{status}[/{style}]")
    console.print(f"Research safety boundary audit JSON：{json_path}")
    console.print(f"Research safety boundary audit Markdown：{md_path}")
    console.print(
        f"tasks：{summary['task_check_count']}；"
        f"artifacts：{summary['artifact_check_count']}；"
        f"unsafe_signals：{summary['unsafe_signal_count']}；"
        f"missing_metadata：{summary['missing_metadata_count']}；"
        f"production_effect={payload['production_effect']}；只读 safety audit"
    )
    if status == "SAFETY_BLOCKED":
        raise typer.Exit(code=1)


@reports_app.command("validate-research-safety-boundary")
def validate_research_safety_boundary_command(
    latest: Annotated[
        bool,
        typer.Option(help="校验 reports_dir 中最新 research safety boundary audit JSON。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Research safety boundary validation 日期。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    source_json_path: Annotated[
        Path | None,
        typer.Option(
            help="Research safety boundary audit JSON 路径；优先级高于 --latest/--as-of。"
        ),
    ] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Research safety boundary validation JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Research safety boundary validation Markdown 输出路径。"),
    ] = None,
) -> None:
    """校验 research safety boundary audit；unsafe positive signal 时 fail closed。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --as-of/--date 同时使用")
    if source_json_path is not None:
        source_path = source_json_path
    elif latest:
        latest_path = latest_research_safety_boundary_json_path(reports_dir)
        if latest_path is None:
            raise typer.BadParameter(f"未找到 research safety boundary audit JSON：{reports_dir}")
        source_path = latest_path
    else:
        report_date = _parse_date(as_of) if as_of else date.today()
        source_path = default_research_safety_boundary_json_path(reports_dir, report_date)
    if not source_path.exists():
        raise typer.BadParameter(f"Research safety boundary audit JSON not found: {source_path}")
    try:
        raw_payload = json.loads(source_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise typer.BadParameter(
            f"Research safety boundary audit JSON cannot be parsed: {source_path}"
        ) from exc
    if not isinstance(raw_payload, dict):
        raise typer.BadParameter(
            f"Research safety boundary audit JSON must be an object: {source_path}"
        )
    payload = validate_research_safety_boundary_payload(raw_payload)
    source_artifacts = dict(payload.get("input_artifacts", {}))
    source_artifacts["research_safety_boundary_audit"] = str(source_path)
    payload["input_artifacts"] = source_artifacts
    report_date = _parse_date(str(payload.get("as_of") or date.today().isoformat()))
    validation_json = json_output_path or default_research_safety_boundary_validation_json_path(
        reports_dir,
        report_date,
    )
    validation_md = (
        markdown_output_path
        or default_research_safety_boundary_validation_markdown_path(reports_dir, report_date)
    )
    json_path = write_research_safety_boundary_validation_json(payload, validation_json)
    md_path = write_research_safety_boundary_validation_markdown(payload, validation_md)
    status = payload["validation_status"]
    style = "green" if status == "SAFETY_PASS" else "yellow"
    if status == "SAFETY_BLOCKED":
        style = "red"
    summary = payload["summary"]
    console.print(f"[{style}]Research safety boundary validation：{status}[/{style}]")
    console.print(f"Research safety boundary validation JSON：{json_path}")
    console.print(f"Research safety boundary validation Markdown：{md_path}")
    console.print(
        f"checks：{summary['check_count']}；"
        f"failed：{summary['failed_check_count']}；"
        f"warnings：{summary['warning_check_count']}；"
        f"production_effect={payload['production_effect']}；只读校验"
    )
    if status == "SAFETY_BLOCKED":
        raise typer.Exit(code=1)


@reports_app.command("artifact-lineage")
def artifact_lineage_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Artifact lineage 日期，格式为 YYYY-MM-DD。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option(help="使用默认 decision snapshot 目录中的最新 signal-date。"),
    ] = False,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    project_root: Annotated[
        Path,
        typer.Option(help="用于解析 artifact 相对路径的项目根目录。"),
    ] = PROJECT_ROOT,
    report_index_path: Annotated[
        Path | None,
        typer.Option(help="可选 report_index JSON 路径；不传时只读扫描 report registry。"),
    ] = None,
    registry_path: Annotated[
        Path,
        typer.Option(help="report_registry.yaml 路径。"),
    ] = DEFAULT_REPORT_REGISTRY_PATH,
    waiver_path: Annotated[
        Path,
        typer.Option(help="report index visibility waiver YAML 路径。"),
    ] = DEFAULT_REPORT_INDEX_WAIVER_PATH,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Artifact lineage JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Artifact lineage Markdown 输出路径。"),
    ] = None,
) -> None:
    """生成 candidate research chain artifact lineage graph；只读扫描既有 artifact。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --as-of/--date 同时使用")
    if latest:
        snapshot_path = _latest_decision_snapshot_path(DEFAULT_DECISION_SNAPSHOT_DIR)
        report_date = _decision_snapshot_date(snapshot_path)
    else:
        report_date = _parse_date(as_of) if as_of else date.today()
    report_index_payload: dict[str, object] | None = None
    if report_index_path is not None:
        if not report_index_path.exists():
            raise typer.BadParameter(f"report_index JSON not found: {report_index_path}")
        try:
            raw_index = json.loads(report_index_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise typer.BadParameter(
                f"report_index JSON cannot be parsed: {report_index_path}"
            ) from exc
        if not isinstance(raw_index, dict):
            raise typer.BadParameter(f"report_index JSON must be an object: {report_index_path}")
        report_index_payload = raw_index
    try:
        payload = build_artifact_lineage_payload(
            as_of=report_date,
            project_root=project_root,
            report_index_payload=report_index_payload,
            report_index_path=report_index_path,
            registry_path=registry_path,
            waiver_path=waiver_path,
        )
    except FileNotFoundError as exc:
        raise typer.BadParameter(str(exc)) from exc
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    lineage_json = json_output_path or default_artifact_lineage_json_path(
        reports_dir,
        report_date,
    )
    lineage_md = markdown_output_path or default_artifact_lineage_markdown_path(
        reports_dir,
        report_date,
    )
    json_path = write_artifact_lineage_json(payload, lineage_json)
    md_path = write_artifact_lineage_markdown(payload, lineage_md)
    style = "green" if payload["lineage_status"] == "PASS" else "yellow"
    if payload["lineage_status"] == "FAIL":
        style = "red"
    summary = payload["summary"]
    console.print(f"[{style}]Artifact lineage：{payload['lineage_status']}[/{style}]")
    console.print(f"Artifact lineage JSON：{json_path}")
    console.print(f"Artifact lineage Markdown：{md_path}")
    console.print(
        f"families：{summary['available_required_family_count']}/"
        f"{summary['required_family_count']}；"
        f"edges：{summary['passing_required_edge_count']}/"
        f"{summary['required_edge_count']}；"
        f"blocking：{summary['blocking_issue_count']}；"
        f"warnings：{summary['warning_issue_count']}；"
        f"production_effect={payload['production_effect']}；只读 lineage"
    )


@reports_app.command("validate-artifact-lineage")
def validate_artifact_lineage_command(
    as_of: Annotated[
        str | None,
        typer.Option(
            "--as-of",
            "--date",
            help="Artifact lineage validation 日期，格式为 YYYY-MM-DD。",
        ),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option(help="使用默认 decision snapshot 目录中的最新 signal-date。"),
    ] = False,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    artifact_lineage_json_path: Annotated[
        Path | None,
        typer.Option(help="Artifact lineage JSON 路径；不传时按日期使用默认路径。"),
    ] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Artifact lineage validation JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Artifact lineage validation Markdown 输出路径。"),
    ] = None,
) -> None:
    """校验 artifact lineage graph 的 family / edge / production safety 覆盖。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --as-of/--date 同时使用")
    if latest:
        snapshot_path = _latest_decision_snapshot_path(DEFAULT_DECISION_SNAPSHOT_DIR)
        report_date = _decision_snapshot_date(snapshot_path)
    else:
        report_date = _parse_date(as_of) if as_of else date.today()
    lineage_path = artifact_lineage_json_path or default_artifact_lineage_json_path(
        reports_dir,
        report_date,
    )
    if not lineage_path.exists():
        raise typer.BadParameter(f"artifact lineage JSON not found: {lineage_path}")
    try:
        raw_payload = json.loads(lineage_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise typer.BadParameter(
            f"artifact lineage JSON cannot be parsed: {lineage_path}"
        ) from exc
    if not isinstance(raw_payload, dict):
        raise typer.BadParameter(f"artifact lineage JSON must be an object: {lineage_path}")
    payload = validate_artifact_lineage_payload(raw_payload)
    raw_source_artifacts = payload.get("source_artifacts")
    source_artifacts = (
        dict(raw_source_artifacts) if isinstance(raw_source_artifacts, Mapping) else {}
    )
    source_artifacts["artifact_lineage_graph"] = str(lineage_path)
    payload["source_artifacts"] = source_artifacts
    payload["input_artifacts"] = source_artifacts
    validation_json = json_output_path or default_artifact_lineage_validation_json_path(
        reports_dir,
        report_date,
    )
    validation_md = markdown_output_path or default_artifact_lineage_validation_markdown_path(
        reports_dir,
        report_date,
    )
    json_path = write_artifact_lineage_validation_json(payload, validation_json)
    md_path = write_artifact_lineage_validation_markdown(payload, validation_md)
    style = "green" if payload["validation_status"] == "PASS" else "yellow"
    if payload["validation_status"] == "FAIL":
        style = "red"
    summary = payload["summary"]
    console.print(f"[{style}]Artifact lineage validation：{payload['validation_status']}[/{style}]")
    console.print(f"Artifact lineage validation JSON：{json_path}")
    console.print(f"Artifact lineage validation Markdown：{md_path}")
    console.print(
        f"checks：{summary['check_count']}；"
        f"failed：{summary['failed_check_count']}；"
        f"blocking：{summary['blocking_issue_count']}；"
        f"warnings：{summary['warning_issue_count']}；"
        f"production_effect={payload['production_effect']}；只读校验"
    )
    if payload["validation_status"] == "FAIL":
        raise typer.Exit(code=1)


@task_register_consistency_app.command("run")
def task_register_consistency_run_command(
    as_of: Annotated[
        str | None,
        typer.Option(
            "--as-of",
            "--date",
            help="Task register consistency 日期，格式为 YYYY-MM-DD。",
        ),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    project_root: Annotated[
        Path,
        typer.Option(help="用于解析 task register、docs link 和 artifact catalog 的项目根目录。"),
    ] = PROJECT_ROOT,
    task_register_path: Annotated[
        Path | None,
        typer.Option(help="docs/task_register.md 路径；不传时使用项目默认路径。"),
    ] = None,
    completed_register_path: Annotated[
        Path | None,
        typer.Option(help="docs/task_register_completed.md 路径；不传时使用项目默认路径。"),
    ] = None,
    registry_path: Annotated[
        Path,
        typer.Option(help="report_registry.yaml 路径。"),
    ] = DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Annotated[
        Path | None,
        typer.Option(help="docs/artifact_catalog.md 路径；不传时使用项目默认路径。"),
    ] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Task register consistency JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Task register consistency Markdown 输出路径。"),
    ] = None,
) -> None:
    """生成 task register consistency report；只读扫描治理文档。"""
    report_date = _parse_date(as_of) if as_of else date.today()
    payload = build_task_register_consistency_payload(
        as_of=report_date,
        project_root=project_root,
        task_register_path=task_register_path,
        completed_register_path=completed_register_path,
        report_registry_path=registry_path,
        artifact_catalog_path=artifact_catalog_path,
    )
    json_output = json_output_path or default_task_register_consistency_json_path(
        reports_dir,
        report_date,
    )
    markdown_output = markdown_output_path or default_task_register_consistency_markdown_path(
        reports_dir,
        report_date,
    )
    json_path = write_task_register_consistency_json(payload, json_output)
    markdown_path = write_task_register_consistency_markdown(payload, markdown_output)
    style = "green" if payload["consistency_status"] == "PASS" else "yellow"
    if payload["consistency_status"] == "FAIL":
        style = "red"
    summary = payload["summary"]
    console.print(
        f"[{style}]Task register consistency：{payload['consistency_status']}[/{style}]"
    )
    console.print(f"Task register consistency JSON：{json_path}")
    console.print(f"Task register consistency Markdown：{markdown_path}")
    console.print(
        f"active：{summary['active_task_count']}；"
        f"completed：{summary['completed_task_count']}；"
        f"checks：{summary['check_count']}；"
        f"failed：{summary['failed_check_count']}；"
        f"production_effect={payload['production_effect']}；只读治理检查"
    )


@task_register_consistency_app.command("report")
def task_register_consistency_report_command(
    latest: Annotated[
        bool,
        typer.Option(help="读取 reports_dir 中最新 task_register_consistency JSON。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option(
            "--as-of",
            "--date",
            help="Task register consistency 日期，格式为 YYYY-MM-DD。",
        ),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    source_json_path: Annotated[
        Path | None,
        typer.Option(help="Task register consistency JSON 路径；优先级高于 --latest/--as-of。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Task register consistency Markdown 输出路径。"),
    ] = None,
) -> None:
    """从现有 JSON 重渲染 task register consistency Markdown。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --as-of/--date 同时使用")
    if source_json_path is not None:
        source_path = source_json_path
    elif latest:
        latest_path = latest_task_register_consistency_json_path(reports_dir)
        if latest_path is None:
            raise typer.BadParameter(f"未找到 task_register_consistency JSON：{reports_dir}")
        source_path = latest_path
    else:
        report_date = _parse_date(as_of) if as_of else date.today()
        source_path = default_task_register_consistency_json_path(reports_dir, report_date)
    if not source_path.exists():
        raise typer.BadParameter(f"task register consistency JSON not found: {source_path}")
    try:
        payload = json.loads(source_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise typer.BadParameter(
            f"task register consistency JSON cannot be parsed: {source_path}"
        ) from exc
    if not isinstance(payload, dict):
        raise typer.BadParameter(f"task register consistency JSON must be an object: {source_path}")
    report_date = _parse_date(str(payload.get("as_of") or date.today().isoformat()))
    markdown_output = markdown_output_path or default_task_register_consistency_markdown_path(
        reports_dir,
        report_date,
    )
    markdown_path = write_task_register_consistency_markdown(payload, markdown_output)
    status = payload.get("consistency_status", "UNKNOWN")
    console.print(f"[green]Task register consistency report：{status}[/green]")
    console.print(f"Source JSON：{source_path}")
    console.print(f"Markdown：{markdown_path}")


@task_register_consistency_app.command("validate")
def task_register_consistency_validate_command(
    latest: Annotated[
        bool,
        typer.Option(help="校验 reports_dir 中最新 task_register_consistency JSON。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Task register consistency validation 日期。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    source_json_path: Annotated[
        Path | None,
        typer.Option(help="Task register consistency JSON 路径；优先级高于 --latest/--as-of。"),
    ] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Task register consistency validation JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Task register consistency validation Markdown 输出路径。"),
    ] = None,
) -> None:
    """校验 task register consistency report，并在 blocker 存在时 fail closed。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --as-of/--date 同时使用")
    if source_json_path is not None:
        source_path = source_json_path
    elif latest:
        latest_path = latest_task_register_consistency_json_path(reports_dir)
        if latest_path is None:
            raise typer.BadParameter(f"未找到 task_register_consistency JSON：{reports_dir}")
        source_path = latest_path
    else:
        report_date = _parse_date(as_of) if as_of else date.today()
        source_path = default_task_register_consistency_json_path(reports_dir, report_date)
    if not source_path.exists():
        raise typer.BadParameter(f"task register consistency JSON not found: {source_path}")
    try:
        raw_payload = json.loads(source_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise typer.BadParameter(
            f"task register consistency JSON cannot be parsed: {source_path}"
        ) from exc
    if not isinstance(raw_payload, dict):
        raise typer.BadParameter(f"task register consistency JSON must be an object: {source_path}")
    payload = validate_task_register_consistency_payload(raw_payload)
    source_artifacts = dict(payload.get("source_artifacts", {}))
    source_artifacts["task_register_consistency"] = str(source_path)
    payload["source_artifacts"] = source_artifacts
    payload["input_artifacts"] = source_artifacts
    report_date = _parse_date(str(payload.get("as_of") or date.today().isoformat()))
    validation_json = json_output_path or default_task_register_consistency_validation_json_path(
        reports_dir,
        report_date,
    )
    validation_md = (
        markdown_output_path
        or default_task_register_consistency_validation_markdown_path(reports_dir, report_date)
    )
    json_path = write_task_register_consistency_validation_json(payload, validation_json)
    markdown_path = write_task_register_consistency_validation_markdown(payload, validation_md)
    style = "green" if payload["validation_status"] == "PASS" else "yellow"
    if payload["validation_status"] == "FAIL":
        style = "red"
    summary = payload["summary"]
    console.print(
        f"[{style}]Task register consistency validation："
        f"{payload['validation_status']}[/{style}]"
    )
    console.print(f"Task register consistency validation JSON：{json_path}")
    console.print(f"Task register consistency validation Markdown：{markdown_path}")
    console.print(
        f"checks：{summary['check_count']}；"
        f"failed：{summary['failed_check_count']}；"
        f"warnings：{summary['warning_check_count']}；"
        f"production_effect={payload['production_effect']}；只读校验"
    )
    if payload["validation_status"] == "FAIL":
        raise typer.Exit(code=1)


@reports_app.command("quality-gate")
def report_quality_gate_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Report quality gate 日期，格式为 YYYY-MM-DD。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option(
            help="使用默认 decision snapshot 目录中的最新 signal-date，并校验对应报告集合。"
        ),
    ] = False,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    project_root: Annotated[
        Path,
        typer.Option(help="用于解析 report index 中相对 artifact 路径的项目根目录。"),
    ] = PROJECT_ROOT,
    report_index_path: Annotated[
        Path | None,
        typer.Option(help="report_index JSON 路径；不传时按日期使用默认路径。"),
    ] = None,
    reader_brief_json_path: Annotated[
        Path | None,
        typer.Option(help="Reader Brief JSON 路径；不传时按日期使用默认路径。"),
    ] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Report quality gate JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Report quality gate Markdown 输出路径。"),
    ] = None,
) -> None:
    """校验 report / Reader Brief 的基础可读性 section，并生成只读 quality gate report。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --as-of/--date 同时使用")
    if latest:
        snapshot_path = _latest_decision_snapshot_path(DEFAULT_DECISION_SNAPSHOT_DIR)
        report_date = _decision_snapshot_date(snapshot_path)
    else:
        report_date = _parse_date(as_of) if as_of else date.today()
    index_path = report_index_path or default_report_index_json_path(reports_dir, report_date)
    brief_json_path = reader_brief_json_path or default_reader_brief_json_path(
        reports_dir,
        report_date,
    )
    report_index_payload: dict[str, object]
    if index_path.exists():
        try:
            raw_index = json.loads(index_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise typer.BadParameter(f"report_index JSON cannot be parsed: {index_path}") from exc
        if not isinstance(raw_index, dict):
            raise typer.BadParameter(f"report_index JSON must be an object: {index_path}")
        report_index_payload = raw_index
    else:
        report_index_payload = {
            "schema_version": 1,
            "report_type": "report_index",
            "as_of": report_date.isoformat(),
            "status": "MISSING",
            "production_effect": "none",
            "reports": [],
            "summary": {"report_count": 0},
        }
    reader_brief_payload: dict[str, object] | None = None
    if brief_json_path.exists():
        try:
            raw_brief = json.loads(brief_json_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise typer.BadParameter(
                f"Reader Brief JSON cannot be parsed: {brief_json_path}"
            ) from exc
        if not isinstance(raw_brief, dict):
            raise typer.BadParameter(f"Reader Brief JSON must be an object: {brief_json_path}")
        reader_brief_payload = raw_brief
    payload = build_report_quality_gate_payload(
        as_of=report_date,
        report_index_payload=report_index_payload,
        report_index_path=index_path,
        reader_brief_payload=reader_brief_payload,
        reader_brief_json_path=brief_json_path,
        project_root=project_root,
    )
    quality_json = json_output_path or default_report_quality_gate_json_path(
        reports_dir,
        report_date,
    )
    quality_md = markdown_output_path or default_report_quality_gate_markdown_path(
        reports_dir,
        report_date,
    )
    json_path = write_report_quality_gate_json(payload, quality_json)
    md_path = write_report_quality_gate_markdown(payload, quality_md)
    style = "green" if payload["report_quality_status"] == "PASS" else "yellow"
    if payload["report_quality_status"] == "FAIL":
        style = "red"
    summary = payload["summary"]
    console.print(f"[{style}]Report quality gate：{payload['report_quality_status']}[/{style}]")
    console.print(f"Report quality gate JSON：{json_path}")
    console.print(f"Report quality gate Markdown：{md_path}")
    console.print(
        f"checked_reports：{summary['checked_report_count']}；"
        f"missing_sections：{summary['missing_section_count']}；"
        f"blocking：{summary['blocking_quality_issue_count']}；"
        f"warnings：{summary['warning_quality_issue_count']}；"
        f"production_effect={payload['production_effect']}；只读校验"
    )


@reports_app.command("score-change-attribution")
def score_change_attribution_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="变化归因日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option(help="使用默认 decision snapshot 目录中的最新 signal-date snapshot。"),
    ] = False,
    decision_snapshot_path: Annotated[
        Path | None,
        typer.Option(help="当前 decision snapshot JSON 路径；不传时按 as_of 使用默认路径。"),
    ] = None,
    previous_decision_snapshot_path: Annotated[
        Path | None,
        typer.Option(help="上一条 decision snapshot JSON 路径；不传时从 snapshot 目录自动发现。"),
    ] = None,
    snapshot_dir: Annotated[
        Path,
        typer.Option(help="用于自动发现上一条 decision snapshot 的目录。"),
    ] = DEFAULT_DECISION_SNAPSHOT_DIR,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Score change attribution Markdown 输出路径。"),
    ] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Score change attribution JSON 输出路径。"),
    ] = None,
) -> None:
    """生成只读 score change attribution 报告。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --as-of/--date 同时使用")
    if latest:
        snapshot_path = decision_snapshot_path or _latest_decision_snapshot_path(snapshot_dir)
        report_date = _decision_snapshot_date(snapshot_path)
    else:
        report_date = _parse_date(as_of) if as_of else date.today()
        snapshot_path = decision_snapshot_path or default_decision_snapshot_path(
            snapshot_dir,
            report_date,
        )
    reports_dir = PROJECT_ROOT / "outputs" / "reports"
    markdown_output = output_path or default_score_change_attribution_report_path(
        reports_dir,
        report_date,
    )
    json_output = json_output_path or default_score_change_attribution_json_path(
        reports_dir,
        report_date,
    )
    try:
        payload = build_score_change_attribution_payload(
            as_of=report_date,
            decision_snapshot_path=snapshot_path,
            previous_decision_snapshot_path=previous_decision_snapshot_path,
            snapshot_dir=snapshot_dir,
        )
    except FileNotFoundError as exc:
        raise typer.BadParameter(str(exc)) from exc
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    report_path = write_score_change_attribution_report(payload, markdown_output)
    json_path = write_score_change_attribution_json(payload, json_output)
    style = "green" if payload["status"] == "PASS" else "yellow"
    console.print(f"[{style}]Score change attribution：{payload['status']}[/{style}]")
    console.print(f"Score change attribution report：{report_path}")
    console.print(f"Score change attribution JSON：{json_path}")
    console.print(
        f"warnings：{len(payload['warnings'])}；"
        f"production_effect={payload['production_effect']}；"
        "不重算 score"
    )


@reports_app.command("market-panel")
def market_panel_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Market panel 日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option(help="使用默认 decision snapshot 目录中的最新 signal-date。"),
    ] = False,
    prices_path: Annotated[
        Path,
        typer.Option(help="标准化日线价格 CSV 路径。"),
    ] = PROJECT_ROOT
    / "data"
    / "raw"
    / "prices_daily.csv",
    rates_path: Annotated[
        Path,
        typer.Option(help="标准化 FRED 宏观序列 CSV 路径。"),
    ] = PROJECT_ROOT
    / "data"
    / "raw"
    / "rates_daily.csv",
    output_path: Annotated[
        Path | None,
        typer.Option(help="Market panel Markdown 输出路径。"),
    ] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Market panel JSON 输出路径。"),
    ] = None,
    data_quality_output_path: Annotated[
        Path | None,
        typer.Option(help="数据质量 Markdown 输出路径；不传时按日期使用默认报告路径。"),
    ] = None,
) -> None:
    """生成只读 market price panel 报告。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --as-of/--date 同时使用")
    if latest:
        report_date = _decision_snapshot_date(
            _latest_decision_snapshot_path(DEFAULT_DECISION_SNAPSHOT_DIR)
        )
    else:
        report_date = _parse_date(as_of) if as_of else date.today()
    reports_dir = PROJECT_ROOT / "outputs" / "reports"
    markdown_output = output_path or default_market_panel_report_path(reports_dir, report_date)
    json_output = json_output_path or default_market_panel_json_path(reports_dir, report_date)
    quality_report_path = data_quality_output_path or default_quality_report_path(
        reports_dir,
        report_date,
    )
    quality_config = load_data_quality()
    quality_report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=["SPY", "QQQ", "SMH", "SOXX", "^VIX"],
        expected_rate_series=["DGS10"],
        quality_config=quality_config,
        as_of=report_date,
        manifest_path=_download_manifest_path(prices_path),
        secondary_prices_path=_marketstack_prices_path(prices_path),
        require_secondary_prices=_requires_marketstack_prices(prices_path),
    )
    write_data_quality_report(quality_report, quality_report_path)
    if not quality_report.passed:
        payload = build_market_panel_payload(
            as_of=report_date,
            prices_path=prices_path,
            rates_path=rates_path,
            data_quality_status=quality_report.status,
            data_quality_report_path=quality_report_path,
            read_cached_data=False,
        )
        report_path = write_market_panel_report(payload, markdown_output)
        json_path = write_market_panel_json(payload, json_output)
        console.print("[red]Market panel 数据质量状态：FAIL[/red]")
        console.print(f"数据质量报告：{quality_report_path}")
        console.print(f"Market panel report：{report_path}")
        console.print(f"Market panel JSON：{json_path}")
        raise typer.Exit(code=1)
    payload = build_market_panel_payload(
        as_of=report_date,
        prices_path=prices_path,
        rates_path=rates_path,
        data_quality_status=quality_report.status,
        data_quality_report_path=quality_report_path,
    )
    report_path = write_market_panel_report(payload, markdown_output)
    json_path = write_market_panel_json(payload, json_output)
    style = "green" if payload["status"] == "PASS" else "yellow"
    console.print(f"[{style}]Market panel：{payload['status']}[/{style}]")
    console.print(f"Market panel report：{report_path}")
    console.print(f"Market panel JSON：{json_path}")
    console.print(
        f"available：{payload['summary']['available_proxy_count']}/"
        f"{payload['summary']['proxy_count']}；"
        f"production_effect={payload['production_effect']}；只读市场面板"
    )


@reports_app.command("research-governance-summary")
def research_governance_summary_command(
    as_of: Annotated[
        str | None,
        typer.Option(
            "--as-of",
            "--date",
            help="Research governance summary 日期，格式为 YYYY-MM-DD，默认今天。",
        ),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option(help="使用默认 decision snapshot 目录中的最新 signal-date。"),
    ] = False,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Research governance summary Markdown 输出路径。"),
    ] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Research governance summary JSON 输出路径。"),
    ] = None,
    project_root: Annotated[
        Path,
        typer.Option(help="用于发现 research / shadow / governance artifacts 的项目根目录。"),
    ] = PROJECT_ROOT,
) -> None:
    """生成只读 research governance summary。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --as-of/--date 同时使用")
    if latest:
        report_date = _decision_snapshot_date(
            _latest_decision_snapshot_path(DEFAULT_DECISION_SNAPSHOT_DIR)
        )
    else:
        report_date = _parse_date(as_of) if as_of else date.today()
    reports_dir = project_root / "outputs" / "reports"
    markdown_output = output_path or default_research_governance_summary_report_path(
        reports_dir,
        report_date,
    )
    json_output = json_output_path or default_research_governance_summary_json_path(
        reports_dir,
        report_date,
    )
    payload = build_research_governance_summary_payload(
        as_of=report_date,
        project_root=project_root,
    )
    report_path = write_research_governance_summary_report(payload, markdown_output)
    json_path = write_research_governance_summary_json(payload, json_output)
    style = "green" if payload["governance_status"] == "OK" else "yellow"
    console.print(f"[{style}]Research governance summary：{payload['governance_status']}[/{style}]")
    console.print(f"Research governance summary report：{report_path}")
    console.print(f"Research governance summary JSON：{json_path}")
    console.print(
        f"cards：{payload['summary']['card_count']}；"
        f"manual_review：{len(payload['manual_review_queue'])}；"
        f"promotion_status={payload['promotion_status']}；"
        f"production_effect={payload['production_effect']}；"
        "只读汇总"
    )


@reports_app.command("index")
def report_index_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Report index 日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option(help="使用默认 decision snapshot 目录中的最新 signal-date。"),
    ] = False,
    registry_path: Annotated[
        Path,
        typer.Option(help="report_registry.yaml 路径。"),
    ] = DEFAULT_REPORT_REGISTRY_PATH,
    waiver_path: Annotated[
        Path,
        typer.Option(help="report index visibility waiver YAML 路径。"),
    ] = DEFAULT_REPORT_INDEX_WAIVER_PATH,
    project_root: Annotated[
        Path,
        typer.Option(help="用于扫描 report artifacts 的项目根目录。"),
    ] = PROJECT_ROOT,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Report index HTML 输出路径。"),
    ] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Report index JSON 输出路径。"),
    ] = None,
) -> None:
    """生成只读报告 registry / cadence index。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --as-of/--date 同时使用")
    if latest:
        report_date = _decision_snapshot_date(
            _latest_decision_snapshot_path(DEFAULT_DECISION_SNAPSHOT_DIR)
        )
    else:
        report_date = _parse_date(as_of) if as_of else date.today()
    reports_dir = project_root / "outputs" / "reports"
    html_output = output_path or default_report_index_html_path(reports_dir, report_date)
    json_output = json_output_path or default_report_index_json_path(reports_dir, report_date)
    try:
        payload = build_report_index_payload(
            as_of=report_date,
            project_root=project_root,
            registry_path=registry_path,
            waiver_path=waiver_path,
        )
    except FileNotFoundError as exc:
        raise typer.BadParameter(str(exc)) from exc
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    html_path = write_report_index_html(payload, html_output)
    json_path = write_report_index_json(payload, json_output)
    style = "green" if payload["status"] == "PASS" else "yellow"
    console.print(f"[{style}]Report index：{payload['status']}[/{style}]")
    console.print(f"Report index HTML：{html_path}")
    console.print(f"Report index JSON：{json_path}")
    console.print(
        f"reports：{payload['summary']['report_count']}；"
        f"missing：{payload['summary']['missing_count']}；"
        f"stale：{payload['summary']['stale_count']}；"
        f"waived：{payload['summary']['explicit_waiver_count']}；"
        f"expired_waivers：{payload['summary']['expired_waiver_count']}；"
        f"unwaived：{payload['summary']['unwaived_warning_count']}；"
        f"production_effect={payload['production_effect']}；"
        "只读扫描"
    )


@reports_app.command("waiver-inventory")
def waiver_inventory_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Waiver inventory 日期，格式为 YYYY-MM-DD。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    registry_path: Annotated[
        Path,
        typer.Option(help="report_registry.yaml 路径。"),
    ] = DEFAULT_REPORT_REGISTRY_PATH,
    waiver_path: Annotated[
        Path,
        typer.Option(help="report index visibility waiver YAML 路径。"),
    ] = DEFAULT_REPORT_INDEX_WAIVER_PATH,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Waiver inventory JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Waiver inventory Markdown 输出路径。"),
    ] = None,
) -> None:
    """生成 report index waiver inventory；只读扫描 waiver policy。"""
    report_date = _parse_date(as_of) if as_of else date.today()
    try:
        payload = build_waiver_inventory_payload(
            as_of=report_date,
            waiver_path=waiver_path,
            registry_path=registry_path,
        )
    except (FileNotFoundError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    inventory_json = json_output_path or default_waiver_inventory_json_path(
        reports_dir,
        report_date,
    )
    inventory_md = markdown_output_path or default_waiver_inventory_markdown_path(
        reports_dir,
        report_date,
    )
    json_path = write_waiver_inventory_json(payload, inventory_json)
    md_path = write_waiver_inventory_markdown(payload, inventory_md)
    style = "green" if payload["inventory_status"] == "PASS" else "yellow"
    if payload["inventory_status"] == "FAIL":
        style = "red"
    summary = payload["summary"]
    console.print(f"[{style}]Waiver inventory：{payload['inventory_status']}[/{style}]")
    console.print(f"Waiver inventory JSON：{json_path}")
    console.print(f"Waiver inventory Markdown：{md_path}")
    console.print(
        f"waivers：{summary['expanded_waiver_count']}；"
        f"active：{summary['active_waiver_count']}；"
        f"expired：{summary['expired_waiver_count']}；"
        f"expiring_soon：{summary['expiring_soon_waiver_count']}；"
        f"production_effect={payload['production_effect']}；只读治理检查"
    )
    if payload["inventory_status"] == "FAIL":
        raise typer.Exit(code=1)


@reports_app.command("validate-waiver-inventory")
def validate_waiver_inventory_command(
    latest: Annotated[
        bool,
        typer.Option(help="校验 reports_dir 中最新 waiver inventory JSON。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Waiver inventory validation 日期。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    source_json_path: Annotated[
        Path | None,
        typer.Option(help="Waiver inventory JSON 路径；优先级高于 --latest/--as-of。"),
    ] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Waiver inventory validation JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Waiver inventory validation Markdown 输出路径。"),
    ] = None,
) -> None:
    """校验 waiver inventory，并在 expired waiver 存在时 fail closed。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --as-of/--date 同时使用")
    if source_json_path is not None:
        source_path = source_json_path
    elif latest:
        latest_path = latest_waiver_inventory_json_path(reports_dir)
        if latest_path is None:
            raise typer.BadParameter(f"未找到 waiver inventory JSON：{reports_dir}")
        source_path = latest_path
    else:
        report_date = _parse_date(as_of) if as_of else date.today()
        source_path = default_waiver_inventory_json_path(reports_dir, report_date)
    if not source_path.exists():
        raise typer.BadParameter(f"waiver inventory JSON not found: {source_path}")
    try:
        raw_payload = json.loads(source_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise typer.BadParameter(f"waiver inventory JSON cannot be parsed: {source_path}") from exc
    if not isinstance(raw_payload, dict):
        raise typer.BadParameter(f"waiver inventory JSON must be an object: {source_path}")
    payload = validate_waiver_inventory_payload(raw_payload)
    source_artifacts = dict(payload.get("input_artifacts", {}))
    source_artifacts["waiver_inventory"] = str(source_path)
    payload["input_artifacts"] = source_artifacts
    report_date = _parse_date(str(payload.get("as_of") or date.today().isoformat()))
    validation_json = json_output_path or default_waiver_inventory_validation_json_path(
        reports_dir,
        report_date,
    )
    validation_md = markdown_output_path or default_waiver_inventory_validation_markdown_path(
        reports_dir,
        report_date,
    )
    json_path = write_waiver_inventory_validation_json(payload, validation_json)
    md_path = write_waiver_inventory_validation_markdown(payload, validation_md)
    style = "green" if payload["validation_status"] == "PASS" else "yellow"
    if payload["validation_status"] == "FAIL":
        style = "red"
    summary = payload["summary"]
    console.print(
        f"[{style}]Waiver inventory validation：{payload['validation_status']}[/{style}]"
    )
    console.print(f"Waiver inventory validation JSON：{json_path}")
    console.print(f"Waiver inventory validation Markdown：{md_path}")
    console.print(
        f"checks：{summary['check_count']}；"
        f"failed：{summary['failed_check_count']}；"
        f"warnings：{summary['warning_check_count']}；"
        f"production_effect={payload['production_effect']}；只读校验"
    )
    if payload["validation_status"] == "FAIL":
        raise typer.Exit(code=1)


@reports_app.command("dashboard")
def evidence_dashboard_command(
    as_of: Annotated[
        str | None,
        typer.Option(help="Dashboard 评估日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    daily_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 日报路径；不传时按 as_of 使用默认日报路径。"),
    ] = None,
    trace_bundle_path: Annotated[
        Path | None,
        typer.Option(help="日报 evidence bundle JSON 路径；不传时按日报路径推导。"),
    ] = None,
    decision_snapshot_path: Annotated[
        Path | None,
        typer.Option(help="decision snapshot JSON 路径；不传时按 as_of 使用默认路径。"),
    ] = None,
    belief_state_path: Annotated[
        Path | None,
        typer.Option(help="belief_state JSON 路径；不传时从 decision snapshot 读取。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="HTML dashboard 输出路径。"),
    ] = None,
    alerts_report_path: Annotated[
        Path | None,
        typer.Option(help="alerts Markdown 路径；不传时按 as_of 使用默认告警报告路径。"),
    ] = None,
    scores_daily_path: Annotated[
        Path | None,
        typer.Option(help="scores_daily.csv 路径；不传时使用默认处理后评分缓存。"),
    ] = None,
    market_feedback_report_path: Annotated[
        Path | None,
        typer.Option(
            help=(
                "market_feedback_optimization Markdown 路径；不传且使用默认日报路径时，"
                "若同日默认报告存在则接入。"
            ),
        ),
    ] = None,
    feedback_loop_review_path: Annotated[
        Path | None,
        typer.Option(
            help=(
                "feedback_loop_review Markdown 路径；不传且使用默认日报路径时，"
                "若同日默认报告存在则接入。"
            ),
        ),
    ] = None,
    investment_review_path: Annotated[
        Path | None,
        typer.Option(
            help=(
                "investment weekly/monthly review Markdown 路径；不传且使用默认日报路径时，"
                "若同日 weekly 默认报告存在则接入。"
            ),
        ),
    ] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Dashboard JSON payload 输出路径；不传时写入默认同名 JSON。"),
    ] = None,
) -> None:
    """生成只读 evidence-first HTML dashboard。"""
    dashboard_date = _parse_date(as_of) if as_of else date.today()
    daily_path = daily_report_path or default_daily_score_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        dashboard_date,
    )
    trace_path = trace_bundle_path or default_report_trace_bundle_path(daily_path)
    snapshot_path = decision_snapshot_path or default_decision_snapshot_path(
        DEFAULT_DECISION_SNAPSHOT_DIR,
        dashboard_date,
    )
    dashboard_output = output_path or default_evidence_dashboard_path(
        PROJECT_ROOT / "outputs" / "reports",
        dashboard_date,
    )
    alert_path = alerts_report_path or default_alert_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        dashboard_date,
    )
    scores_path = scores_daily_path or DEFAULT_SCORES_DAILY_PATH
    market_feedback_path = market_feedback_report_path
    loop_review_path = feedback_loop_review_path
    periodic_review_path = investment_review_path
    if daily_report_path is None:
        default_market_feedback_path = default_market_feedback_optimization_report_path(
            PROJECT_ROOT / "outputs" / "reports",
            dashboard_date,
        )
        default_loop_review_path = default_feedback_loop_review_report_path(
            PROJECT_ROOT / "outputs" / "reports",
            dashboard_date,
        )
        default_periodic_review_path = default_periodic_investment_review_report_path(
            DEFAULT_PERIODIC_INVESTMENT_REVIEW_REPORT_DIR,
            "weekly",
            dashboard_date,
        )
        market_feedback_path = market_feedback_path or (
            default_market_feedback_path if default_market_feedback_path.exists() else None
        )
        loop_review_path = loop_review_path or (
            default_loop_review_path if default_loop_review_path.exists() else None
        )
        periodic_review_path = periodic_review_path or (
            default_periodic_review_path if default_periodic_review_path.exists() else None
        )
    dashboard_json_output = json_output_path or (
        default_evidence_dashboard_json_path(PROJECT_ROOT / "outputs" / "reports", dashboard_date)
        if output_path is None
        else dashboard_output.with_suffix(".json")
    )
    try:
        report = build_evidence_dashboard_report(
            as_of=dashboard_date,
            daily_report_path=daily_path,
            trace_bundle_path=trace_path,
            decision_snapshot_path=snapshot_path,
            belief_state_path=belief_state_path,
            alerts_report_path=alert_path,
            scores_daily_path=scores_path,
            market_feedback_report_path=market_feedback_path,
            feedback_loop_review_path=loop_review_path,
            investment_review_path=periodic_review_path,
        )
    except FileNotFoundError as exc:
        raise typer.BadParameter(str(exc)) from exc
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc

    dashboard_path = write_evidence_dashboard(report, dashboard_output)
    dashboard_json_path = write_evidence_dashboard_json(report, dashboard_json_output)
    style = "green" if report.status == "PASS" else "yellow"
    claim_count = len(report.trace_bundle.get("claims", []))
    dataset_count = len(report.trace_bundle.get("dataset_refs", []))
    console.print(f"[{style}]Evidence dashboard：{report.status}[/{style}]")
    console.print(f"Dashboard：{dashboard_path}")
    console.print(f"Dashboard JSON：{dashboard_json_path}")
    console.print(
        f"核心 claim：{claim_count}；"
        f"输入 dataset：{dataset_count}；"
        f"警告：{len(report.warnings)}"
    )


@reports_app.command("daily-tasks")
def daily_task_dashboard_command(
    as_of: Annotated[
        str | None,
        typer.Option(help="每日任务展示日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    metadata_path: Annotated[
        Path | None,
        typer.Option(help="daily_ops_run_metadata JSON 路径；不传时按 as_of 使用默认路径。"),
    ] = None,
    run_report_path: Annotated[
        Path | None,
        typer.Option(help="daily_ops_run Markdown 路径；不传时按 as_of 使用默认路径。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="同日子任务报告所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    output_path: Annotated[
        Path | None,
        typer.Option(help="每日任务 HTML dashboard 输出路径。"),
    ] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="每日任务 JSON payload 输出路径。"),
    ] = None,
    decision_summary_output_path: Annotated[
        Path | None,
        typer.Option(help="每日决策总线 JSON 输出路径。"),
    ] = None,
    order_intent_candidates_output_path: Annotated[
        Path | None,
        typer.Option(help="Order intent candidate JSON 输出路径。"),
    ] = None,
    paper_trading_trend_days: Annotated[
        int,
        typer.Option(help="Paper Trading Trend 读取窗口；可选 7、14 或 30。"),
    ] = 7,
) -> None:
    """生成 daily-run 子任务总控展示页。"""
    dashboard_date = _parse_date(as_of) if as_of else date.today()
    resolved_metadata_path = metadata_path or default_daily_ops_run_metadata_path(
        reports_dir,
        dashboard_date,
    )
    resolved_run_report_path = run_report_path or default_daily_ops_run_report_path(
        reports_dir,
        dashboard_date,
    )
    dashboard_output = output_path or default_daily_task_dashboard_path(
        reports_dir,
        dashboard_date,
    )
    dashboard_json_output = json_output_path or (
        default_daily_task_dashboard_json_path(reports_dir, dashboard_date)
        if output_path is None
        else dashboard_output.with_suffix(".json")
    )
    decision_summary_output = decision_summary_output_path or default_daily_decision_summary_path(
        reports_dir,
        dashboard_date,
    )
    order_intent_candidates_output = (
        order_intent_candidates_output_path
        or default_order_intent_candidates_path(reports_dir, dashboard_date)
    )
    try:
        report = build_daily_task_dashboard_report(
            as_of=dashboard_date,
            metadata_path=resolved_metadata_path,
            run_report_path=resolved_run_report_path if resolved_run_report_path.exists() else None,
            reports_dir=reports_dir,
            paper_trading_trend_days=paper_trading_trend_days,
        )
    except FileNotFoundError as exc:
        raise typer.BadParameter(str(exc)) from exc
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc

    html_path = write_daily_task_dashboard(report, dashboard_output)
    json_path = write_daily_task_dashboard_json(report, dashboard_json_output)
    decision_summary_path = write_daily_decision_summary_json(
        report,
        decision_summary_output,
    )
    order_intent_candidates_path = write_order_intent_candidates_json(
        as_of=dashboard_date,
        daily_decision_summary_path=decision_summary_path,
        output_path=order_intent_candidates_output,
        project_root=report.project_root,
    )
    style = (
        "green"
        if report.status == "PASS"
        else "yellow" if report.status == "PASS_WITH_SKIPS" else "red"
    )
    console.print(f"[{style}]每日任务展示：{report.status}[/{style}]")
    console.print(f"Dashboard：{html_path}")
    console.print(f"Dashboard JSON：{json_path}")
    console.print(f"Daily decision summary JSON：{decision_summary_path}")
    console.print(f"Order intent candidates JSON：{order_intent_candidates_path}")
    console.print(
        f"步骤：{len(report.tasks)}；失败：{report.failed_count}；"
        f"跳过：{report.skipped_count}；风险/限制：{report.risk_count}"
    )
