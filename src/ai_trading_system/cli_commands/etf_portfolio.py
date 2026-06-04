from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Annotated, cast

import pandas as pd
import typer

from ai_trading_system.config import (
    PROJECT_ROOT,
    configured_price_tickers,
    configured_rate_series,
    load_data_quality,
    load_universe,
)
from ai_trading_system.data.quality import (
    default_quality_report_path,
    validate_data_cache,
)
from ai_trading_system.data.quality import (
    write_data_quality_report as write_cache_data_quality_report,
)
from ai_trading_system.etf_portfolio.ai_attribution import (
    DEFAULT_AI_ATTRIBUTION_DATASET_DIR,
    DEFAULT_AI_ATTRIBUTION_REVIEW_DIR,
    DEFAULT_AI_ATTRIBUTION_VALIDATION_DIR,
    build_ai_attribution_dataset,
    build_ai_attribution_report,
    build_ai_attribution_validation_report,
    load_ai_confirmation_report_payloads,
    write_ai_attribution_dataset,
    write_ai_attribution_report,
    write_ai_attribution_validation_report,
)
from ai_trading_system.etf_portfolio.ai_confirmation import (
    DEFAULT_AI_CONFIRMATION_FEATURE_DIR,
    DEFAULT_AI_CONFIRMATION_OVERLAY_DIR,
    DEFAULT_AI_CONFIRMATION_POLICY_CONFIG_PATH,
    DEFAULT_AI_CONFIRMATION_STANDALONE_REPORT_DIR,
    DEFAULT_AI_CONFIRMATION_UNIVERSE_CONFIG_PATH,
    DEFAULT_AI_CONFIRMATION_VALIDATION_DIR,
    ai_confirmation_price_group_ids,
    all_enabled_price_tickers,
    build_ai_confirmation_breadth_features,
    build_ai_confirmation_report,
    build_ai_confirmation_shadow_overlay_experiment,
    build_ai_confirmation_validation_report,
    latest_ai_confirmation_report_path,
    load_ai_confirmation_base_weights,
    load_ai_confirmation_events,
    load_ai_confirmation_policy_config,
    load_ai_confirmation_universe_config,
    validate_ai_confirmation_data_availability,
    write_ai_confirmation_breadth_features,
    write_ai_confirmation_report,
    write_ai_confirmation_shadow_overlay,
    write_ai_confirmation_validation_report,
)
from ai_trading_system.etf_portfolio.allocation import (
    allocate_portfolio,
    latest_weights_from_file,
    load_allocation,
    write_allocation,
)
from ai_trading_system.etf_portfolio.backtest import run_portfolio_backtest, write_backtest_run
from ai_trading_system.etf_portfolio.baseline_review import (
    DEFAULT_BASELINE_REVIEW_DECISION_DIR,
    DEFAULT_BASELINE_REVIEW_OUTCOME_DIR,
    DEFAULT_BASELINE_REVIEW_PACKAGE_DIR,
    DEFAULT_BASELINE_REVIEW_POLICY_CONFIG_PATH,
    DEFAULT_BASELINE_REVIEW_PROPOSAL_DIR,
    DEFAULT_BASELINE_REVIEW_REPORT_DIR,
    DEFAULT_BASELINE_REVIEW_VALIDATION_DIR,
    BaselineReviewError,
    build_baseline_change_proposal_draft,
    build_baseline_review_eligibility,
    build_baseline_review_evidence_matrix,
    build_baseline_review_package,
    build_baseline_review_validation_report,
    build_candidate_review_outcome,
    build_owner_review_decision,
    link_baseline_review_decision_to_journal,
    write_baseline_change_proposal_draft,
    write_baseline_review_outcome,
    write_baseline_review_package,
    write_baseline_review_validation_report,
    write_owner_review_decision,
)
from ai_trading_system.etf_portfolio.credibility import (
    DEFAULT_ETF_CREDIBILITY_DIR,
    build_credibility_gate,
    write_credibility_gate,
)
from ai_trading_system.etf_portfolio.data import (
    ingest_yfinance_prices,
    latest_price_date,
    load_standard_prices,
    read_price_frame,
    standardize_price_frame,
    validate_price_data,
    write_quality_report,
)
from ai_trading_system.etf_portfolio.data_quality import (
    DEFAULT_ETF_DATA_QUALITY_POLICY_CONFIG_PATH,
    DEFAULT_ETF_DATA_QUALITY_REPORT_DIR,
    DEFAULT_ETF_DATA_QUALITY_VALIDATION_DIR,
    build_data_quality_report,
    build_data_quality_validation_report,
    check_price_freshness,
    load_data_quality_policy_config,
    write_data_quality_report,
    write_data_quality_validation_report,
)
from ai_trading_system.etf_portfolio.decision_journal import (
    DEFAULT_DECISION_JOURNAL_PATH,
    DEFAULT_DECISION_JOURNAL_PROPOSAL_DIR,
    DEFAULT_DECISION_JOURNAL_REPORT_DIR,
    DEFAULT_DECISION_JOURNAL_VALIDATION_DIR,
    DecisionJournalError,
    add_decision_entry,
    build_candidate_state_update_proposals,
    build_decision_entry_from_weekly_review,
    build_decision_journal_analytics,
    build_decision_journal_report,
    build_decision_journal_validation_report,
    decision_entries,
    load_decision_journal,
    remove_decision_entry,
    update_decision_entry,
    write_decision_journal,
    write_decision_journal_analytics,
    write_decision_journal_report,
    write_decision_journal_validation_report,
    write_decision_state_update_proposals,
)
from ai_trading_system.etf_portfolio.experiments import (
    DEFAULT_ETF_EXPERIMENT_RUN_DIR,
    DEFAULT_ETF_EXPERIMENT_WEEKLY_REVIEW_DIR,
    DEFAULT_ETF_SHADOW_CANDIDATE_REGISTRY_PATH,
    apply_ranking_policy_to_comparison_report,
    build_candidate_selection_report,
    build_experiment_comparison_report,
    build_experiment_validation_report,
    build_weekly_experiment_review,
    enroll_shadow_candidates,
    find_latest_experiment_run_dir,
    load_experiment_pack_registry,
    load_experiment_registry,
    run_experiment_batch,
    write_candidate_selection_report,
    write_experiment_comparison_report,
    write_experiment_validation_report,
    write_weekly_experiment_review_report,
)
from ai_trading_system.etf_portfolio.features import (
    build_feature_store,
    latest_feature_date,
    load_feature_store,
    write_feature_store,
)
from ai_trading_system.etf_portfolio.forward import (
    DEFAULT_ETF_FORWARD_CONFIG_PATH,
    DEFAULT_ETF_FORWARD_DECISION_LEDGER_PATH,
    DEFAULT_ETF_FORWARD_REPORT_DIR,
    run_forward_dashboard,
    run_forward_update,
    run_forward_validation,
    run_forward_watchlist,
    run_forward_weekly_review,
)
from ai_trading_system.etf_portfolio.governance import (
    DEFAULT_ETF_PARAMETER_GOVERNANCE_CONFIG_PATH,
    evaluate_parameter_candidate,
    load_parameter_governance_policy,
    read_parameter_candidate,
    write_parameter_governance_summary,
)
from ai_trading_system.etf_portfolio.models import (
    DEFAULT_ETF_BACKTEST_CONFIG_PATH,
    DEFAULT_ETF_BACKTEST_DIR,
    DEFAULT_ETF_FEATURE_PATH,
    DEFAULT_ETF_LEDGER_PATH,
    DEFAULT_ETF_P2_MANIFEST_PATH,
    DEFAULT_ETF_PRICE_PATH,
    DEFAULT_ETF_REGIME_PATH,
    DEFAULT_ETF_REPORT_DIR,
    DEFAULT_ETF_SIGNAL_PATH,
    DEFAULT_ETF_STRATEGY_CONFIG_PATH,
    DEFAULT_ETF_TARGET_PATH,
    ETFAllocationRecord,
    load_etf_config_bundle,
)
from ai_trading_system.etf_portfolio.operations import (
    DEFAULT_ETF_OPERATIONS_SCHEDULE_CONFIG_PATH,
    OperationsGraphCadence,
    build_operations_health_report,
    build_operations_scheduler_dry_run,
    build_operations_validation_report,
    write_operations_health_report,
    write_operations_scheduler_dry_run,
    write_operations_validation_report,
)
from ai_trading_system.etf_portfolio.p1 import (
    append_experiment_registry,
    append_experiment_run,
    build_confirmation_scores,
    build_experiment_comparison,
    build_governance_status,
    build_portfolio_attribution,
    build_relative_strength_table,
    evaluate_event_risk,
    evaluate_satellite_candidates,
    write_frame_and_report,
)
from ai_trading_system.etf_portfolio.p2 import (
    build_advanced_risk_report,
    build_edgar_text_topic_audit,
    build_ensemble_candidates,
    build_holdings_lookthrough_report,
    build_live_interface_preflight,
    build_ml_ranking_candidates,
    build_news_theme_tracking_report,
    build_source_contract_report,
    build_walk_forward_readiness_report,
    build_weight_optimizer_candidates,
    derive_edgar_text_events_from_timeline,
    derive_options_iv_skew_from_vix,
    fetch_edgar_text_documents_from_timeline,
    import_p2_source,
    normalize_etf_holdings_source,
    normalize_news_theme_source,
    normalize_options_risk_source,
    p2_metadata,
)
from ai_trading_system.etf_portfolio.parameter_review import (
    DEFAULT_PARAMETER_REVIEW_AGGREGATION_DIR,
    DEFAULT_PARAMETER_REVIEW_REVIEW_DIR,
    DEFAULT_PARAMETER_REVIEW_VALIDATION_DIR,
    build_parameter_review_aggregation,
    build_parameter_review_report,
    build_parameter_review_validation_report,
    write_parameter_review_aggregation,
    write_parameter_review_report,
    write_parameter_review_validation_report,
)
from ai_trading_system.etf_portfolio.regime import (
    generate_regime_for_date,
    load_regimes,
    select_regime_for_date,
    write_regime,
)
from ai_trading_system.etf_portfolio.reporting import render_daily_brief, write_daily_brief
from ai_trading_system.etf_portfolio.satellite import (
    DEFAULT_SATELLITE_EXPERIMENT_DIR,
    DEFAULT_SATELLITE_FEATURE_DIR,
    DEFAULT_SATELLITE_POLICY_CONFIG_PATH,
    DEFAULT_SATELLITE_STANDALONE_REPORT_DIR,
    DEFAULT_SATELLITE_UNIVERSE_CONFIG_PATH,
    DEFAULT_SATELLITE_VALIDATION_DIR,
    build_satellite_policy_validation_report,
    build_satellite_relative_strength_features,
    build_satellite_replacement_report,
    build_satellite_shadow_portfolio_experiment,
    latest_satellite_report_path,
    load_satellite_policy_config,
    load_satellite_universe_config,
    satellite_price_symbols,
    validate_satellite_data_availability,
    write_satellite_features,
    write_satellite_policy_validation_report,
    write_satellite_replacement_report,
    write_satellite_shadow_experiment,
)
from ai_trading_system.etf_portfolio.satellite_attribution import (
    DEFAULT_SATELLITE_ATTRIBUTION_DATASET_DIR,
    DEFAULT_SATELLITE_ATTRIBUTION_REVIEW_DIR,
    DEFAULT_SATELLITE_ATTRIBUTION_VALIDATION_DIR,
    build_satellite_attribution_dataset,
    build_satellite_attribution_report,
    build_satellite_attribution_validation_report,
    load_ai_confirmation_report_payloads_for_satellite,
    load_satellite_replacement_report_payloads,
    write_satellite_attribution_dataset,
    write_satellite_attribution_report,
    write_satellite_attribution_validation_report,
)
from ai_trading_system.etf_portfolio.shadow_ready_review import (
    DEFAULT_SHADOW_READY_REVIEW_APPROVAL_DIR,
    DEFAULT_SHADOW_READY_REVIEW_ENROLLMENT_DIR,
    DEFAULT_SHADOW_READY_REVIEW_PACKAGE_DIR,
    DEFAULT_SHADOW_READY_REVIEW_POLICY_CONFIG_PATH,
    DEFAULT_SHADOW_READY_REVIEW_VALIDATION_DIR,
    ShadowReadyReviewError,
    aggregate_shadow_ready_review_candidates,
    build_near_shadow_review_summary,
    build_shadow_candidate_approved_enrollment,
    build_shadow_candidate_owner_approval,
    build_shadow_candidate_review_package,
    build_shadow_candidate_review_validation_report,
    load_shadow_ready_review_policy_config,
    load_shadow_review_diagnostics_artifacts,
    rank_shadow_ready_review_candidates,
    write_shadow_candidate_approved_enrollment,
    write_shadow_candidate_owner_approval,
    write_shadow_candidate_review_package,
    write_shadow_candidate_review_validation_report,
)
from ai_trading_system.etf_portfolio.signals import (
    generate_signals_for_date,
    load_signals,
    select_signals_for_date,
    signals_to_frame,
    write_signals,
)
from ai_trading_system.etf_portfolio.simulation import (
    evaluate_simulation_ledger,
    record_simulation_snapshot,
    summarize_simulation_for_brief,
    write_simulation_report,
)
from ai_trading_system.etf_portfolio.stability import (
    build_allocation_stability_diagnostics,
    write_allocation_stability_diagnostics,
)
from ai_trading_system.etf_portfolio.strategy_evidence_dashboard import (
    DEFAULT_STRATEGY_EVIDENCE_AGGREGATION_DIR,
    DEFAULT_STRATEGY_EVIDENCE_CONFIG_PATH,
    DEFAULT_STRATEGY_EVIDENCE_REPORT_DIR,
    DEFAULT_STRATEGY_EVIDENCE_VALIDATION_DIR,
    build_strategy_evidence_aggregation,
    build_strategy_evidence_dashboard,
    build_strategy_evidence_validation_report,
    write_strategy_evidence_dashboard_report,
    write_strategy_evidence_validation_report,
)
from ai_trading_system.etf_portfolio.trend_calibration import (
    DEFAULT_TREND_CALIBRATION_DATASET_DIR,
    DEFAULT_TREND_CALIBRATION_POLICY_CONFIG_PATH,
    DEFAULT_TREND_CALIBRATION_REGISTRY_DIR,
    DEFAULT_TREND_CALIBRATION_REPORT_DIR,
    DEFAULT_TREND_CALIBRATION_VALIDATION_DIR,
    TrendCalibrationError,
    build_trend_calibration_report,
    build_trend_calibration_validation_report,
    build_trend_signal_dataset,
    latest_trend_calibration_report_path,
    load_trend_calibration_policy_config,
    write_trend_calibration_report,
    write_trend_calibration_validation_report,
    write_trend_signal_config_registry,
    write_trend_signal_dataset,
)
from ai_trading_system.etf_portfolio.weekly_review import (
    DEFAULT_ETF_WEEKLY_REVIEW_AGGREGATION_DIR,
    DEFAULT_ETF_WEEKLY_REVIEW_DIR,
    DEFAULT_ETF_WEEKLY_REVIEW_VALIDATION_DIR,
    build_weekly_review_aggregation,
    build_weekly_review_report,
    build_weekly_review_validation_report,
    write_weekly_review_aggregation,
    write_weekly_review_report,
    write_weekly_review_validation_report,
)
from ai_trading_system.etf_portfolio.weight_calibration import (
    DEFAULT_CANDIDATE_WEIGHT_REGISTRY_PATH,
    DEFAULT_ETF_WEIGHT_CALIBRATION_DATA_DIR,
    DEFAULT_ETF_WEIGHT_CALIBRATION_REPORT_DIR,
    DEFAULT_ETF_WEIGHT_SEARCH_CONFIG_PATH,
    DEFAULT_WEIGHT_CALIBRATION_PRESET_CONFIG_PATH,
    DEFAULT_WEIGHT_CALIBRATION_VALIDATION_DIR,
    DEFAULT_WEIGHT_CANDIDATE_COMPARISON_DIR,
    DEFAULT_WEIGHT_DUAL_TRACK_REPORT_DIR,
    DEFAULT_WEIGHT_FORWARD_ENROLLMENT_PATH,
    DEFAULT_WEIGHT_FORWARD_EVIDENCE_DIR,
    DEFAULT_WEIGHT_INITIAL_RECOMMENDATION_DIR,
    DEFAULT_WEIGHT_OVERFIT_DIAGNOSTICS_DIR,
    DEFAULT_WEIGHT_OVERFIT_EXPLANATION_DIR,
    DEFAULT_WEIGHT_PROPOSAL_DIR,
    DEFAULT_WEIGHT_REGIME_ROBUSTNESS_DIR,
    DEFAULT_WEIGHT_SEARCH_DIAGNOSTICS_DIR,
    DEFAULT_WEIGHT_TOP_CANDIDATE_EXPORT_DIR,
    WEIGHT_ROBUST_SEARCH_PACK_IDS,
    WEIGHT_SEARCH_DIAGNOSTICS_DEFAULT_PRESETS,
    build_backtest_forward_evidence_aggregation,
    build_candidate_weight_proposals,
    build_dual_track_weight_calibration_report,
    build_dual_track_weight_calibration_validation_report,
    build_historical_weight_calibration_usability_validation_report,
    build_historical_weight_search_diagnostics_report,
    build_weight_candidate_comparison_table,
    build_weight_initial_recommendation_report,
    build_weight_overfit_diagnostics,
    build_weight_overfit_explanations,
    build_weight_regime_robustness_heatmap,
    build_weight_top_candidate_export,
    enroll_candidate_weights_forward,
    enroll_top_weight_candidates_forward,
    find_latest_weight_search_run_dir,
    load_candidate_weight_registry,
    load_weight_calibration_preset,
    load_weight_calibration_preset_registry,
    load_weight_forward_enrollments,
    load_weight_search_definition,
    load_weight_search_registry,
    read_weight_search_run_payload,
    register_candidate_weight_sets,
    resolve_weight_calibration_preset,
    run_historical_weight_search,
    write_backtest_forward_evidence_aggregation,
    write_candidate_weight_proposals,
    write_dual_track_weight_calibration_report,
    write_dual_track_weight_calibration_validation_report,
    write_historical_weight_calibration_usability_validation_report,
    write_historical_weight_search_diagnostics_report,
    write_weight_candidate_comparison_table,
    write_weight_initial_recommendation_report,
    write_weight_overfit_diagnostics,
    write_weight_overfit_explanations,
    write_weight_regime_robustness_heatmap,
    write_weight_search_run,
    write_weight_top_candidate_export,
)
from ai_trading_system.etf_portfolio.weight_calibration_cache import (
    DEFAULT_WEIGHT_CALIBRATION_CACHE_POLICY_CONFIG_PATH,
    DEFAULT_WEIGHT_CALIBRATION_CACHE_VALIDATION_DIR,
    DEFAULT_WEIGHT_CALIBRATION_PERFORMANCE_REPORT_DIR,
    build_weight_calibration_cache_parallel_validation_report,
    load_weight_calibration_cache_policy_config,
    write_weight_calibration_cache_parallel_validation_report,
    write_weight_calibration_performance_report,
)
from ai_trading_system.etf_portfolio.weight_calibration_profiling import (
    DEFAULT_WEIGHT_CALIBRATION_PROFILING_POLICY_CONFIG_PATH,
    DEFAULT_WEIGHT_CALIBRATION_PROFILING_REPORT_DIR,
    DEFAULT_WEIGHT_CALIBRATION_PROFILING_VALIDATION_DIR,
    build_weight_calibration_profiling_report,
    build_weight_calibration_profiling_validation_report,
    load_weight_calibration_profiling_policy_config,
    normalize_weight_calibration_profile_mode,
    profiling_mode_settings,
    run_with_optional_cprofile,
    write_cprofile_artifacts,
    write_weight_calibration_candidate_hotspot_table,
    write_weight_calibration_profiling_report,
    write_weight_calibration_profiling_validation_report,
)
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)

etf_app = typer.Typer(help="ETF 主仓组合配置、信号、回测和模拟舱。", no_args_is_help=True)
data_app = typer.Typer(help="ETF price ingest and validation。", no_args_is_help=True)
features_app = typer.Typer(help="ETF feature store。", no_args_is_help=True)
signals_app = typer.Typer(help="ETF signal engine。", no_args_is_help=True)
regime_app = typer.Typer(help="ETF market regime engine。", no_args_is_help=True)
portfolio_app = typer.Typer(help="ETF portfolio allocation。", no_args_is_help=True)
backtest_app = typer.Typer(help="ETF portfolio backtest。", no_args_is_help=True)
simulation_app = typer.Typer(help="ETF simulation ledger。", no_args_is_help=True)
report_app = typer.Typer(help="ETF portfolio reports。", no_args_is_help=True)
run_app = typer.Typer(help="ETF portfolio full workflow。", no_args_is_help=True)
relative_strength_app = typer.Typer(help="ETF P1 relative strength。", no_args_is_help=True)
confirmation_app = typer.Typer(help="ETF P1 confirmation scores。", no_args_is_help=True)
satellite_app = typer.Typer(help="ETF P1 satellite candidates。", no_args_is_help=True)
satellite_attribution_app = typer.Typer(
    help="ETF satellite replacement forward attribution review。",
    no_args_is_help=True,
)
attribution_app = typer.Typer(help="ETF P1 attribution。", no_args_is_help=True)
experiments_app = typer.Typer(help="ETF P1 experiment registry。", no_args_is_help=True)
forward_app = typer.Typer(help="ETF forward shadow simulation review。", no_args_is_help=True)
ai_confirmation_app = typer.Typer(
    help="ETF AI confirmation overlay calibration。",
    no_args_is_help=True,
)
ai_attribution_app = typer.Typer(
    help="ETF AI confirmation forward attribution review。",
    no_args_is_help=True,
)
weekly_review_app = typer.Typer(help="ETF weekly portfolio review package。", no_args_is_help=True)
decision_journal_app = typer.Typer(
    help="ETF portfolio decision journal and human review notes。",
    no_args_is_help=True,
)
parameter_review_app = typer.Typer(
    help="ETF allocation parameter review from forward evidence。",
    no_args_is_help=True,
)
weight_calibration_app = typer.Typer(
    help="ETF dual-track weight calibration。",
    no_args_is_help=True,
)
ops_app = typer.Typer(help="ETF operations workflow planning。", no_args_is_help=True)
data_quality_app = typer.Typer(
    help="ETF data quality and staleness governance。",
    no_args_is_help=True,
)
evidence_dashboard_app = typer.Typer(
    help="ETF strategy evidence dashboard。",
    no_args_is_help=True,
)
baseline_review_app = typer.Typer(
    help="ETF baseline candidate review playbook。",
    no_args_is_help=True,
)
shadow_review_app = typer.Typer(
    help="ETF shadow-ready candidate review and enrollment playbook。",
    no_args_is_help=True,
)
trend_calibration_app = typer.Typer(
    help="ETF trend signal weight calibration workflow。",
    no_args_is_help=True,
)
governance_app = typer.Typer(help="ETF P1 weight governance。", no_args_is_help=True)
events_app = typer.Typer(help="ETF P1 event risk flags。", no_args_is_help=True)
p2_app = typer.Typer(help="ETF P2 observe-only contracts。", no_args_is_help=True)
credibility_app = typer.Typer(help="ETF credibility validation gate。", no_args_is_help=True)

etf_app.add_typer(data_app, name="data")
etf_app.add_typer(features_app, name="features")
etf_app.add_typer(signals_app, name="signals")
etf_app.add_typer(regime_app, name="regime")
etf_app.add_typer(portfolio_app, name="portfolio")
etf_app.add_typer(backtest_app, name="backtest")
etf_app.add_typer(simulation_app, name="simulation")
etf_app.add_typer(report_app, name="report")
etf_app.add_typer(run_app, name="run")
etf_app.add_typer(relative_strength_app, name="relative-strength")
etf_app.add_typer(confirmation_app, name="confirmation")
etf_app.add_typer(satellite_app, name="satellite")
etf_app.add_typer(satellite_attribution_app, name="satellite-attribution")
etf_app.add_typer(attribution_app, name="attribution")
etf_app.add_typer(experiments_app, name="experiments")
etf_app.add_typer(forward_app, name="forward")
etf_app.add_typer(ai_confirmation_app, name="ai-confirmation")
etf_app.add_typer(ai_attribution_app, name="ai-attribution")
etf_app.add_typer(weekly_review_app, name="weekly-review")
etf_app.add_typer(decision_journal_app, name="decision-journal")
etf_app.add_typer(parameter_review_app, name="parameter-review")
etf_app.add_typer(weight_calibration_app, name="weight-calibration")
etf_app.add_typer(ops_app, name="ops")
etf_app.add_typer(data_quality_app, name="data-quality")
etf_app.add_typer(evidence_dashboard_app, name="evidence-dashboard")
etf_app.add_typer(baseline_review_app, name="baseline-review")
etf_app.add_typer(shadow_review_app, name="shadow-review")
etf_app.add_typer(trend_calibration_app, name="trend-calibration")
etf_app.add_typer(governance_app, name="governance")
etf_app.add_typer(events_app, name="events")
etf_app.add_typer(p2_app, name="p2")
etf_app.add_typer(credibility_app, name="credibility")

DEFAULT_ETF_OPERATIONS_DRY_RUN_DIR = (
    PROJECT_ROOT / "outputs" / "dry_runs" / "etf_operations"
)
DEFAULT_ETF_OPERATIONS_REPORT_DIR = (
    PROJECT_ROOT / "reports" / "etf_portfolio" / "operations"
)
DEFAULT_ETF_OPERATIONS_VALIDATION_DIR = DEFAULT_ETF_OPERATIONS_REPORT_DIR / "validation"


@evidence_dashboard_app.command("aggregate")
def evidence_dashboard_aggregate_command(
    as_of: Annotated[
        str,
        typer.Option("--as-of", "--date", help="评估日期 YYYY-MM-DD。"),
    ],
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="evidence dashboard source registry。"),
    ] = DEFAULT_STRATEGY_EVIDENCE_CONFIG_PATH,
    report_index_path: Annotated[
        Path | None,
        typer.Option("--report-index-path", help="可选 report index JSON。"),
    ] = None,
    report_registry_path: Annotated[
        Path,
        typer.Option("--report-registry-path", help="report registry YAML。"),
    ] = DEFAULT_REPORT_REGISTRY_PATH,
    root_path: Annotated[
        Path,
        typer.Option("--root-path", help="扫描既有 artifacts 的根目录。"),
    ] = PROJECT_ROOT,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="aggregation JSON 输出目录。"),
    ] = DEFAULT_STRATEGY_EVIDENCE_AGGREGATION_DIR,
    json_path: Annotated[
        Path | None,
        typer.Option("--json-path", help="显式 JSON 输出路径。"),
    ] = None,
) -> None:
    """聚合既有 ETF strategy evidence sources，不运行上游、不补造结论。"""
    run_date = _parse_date(as_of)
    payload = build_strategy_evidence_aggregation(
        as_of=run_date,
        config_path=config_path,
        report_index_path=report_index_path,
        report_registry_path=report_registry_path,
        root_path=root_path,
    )
    output = json_path or output_dir / f"strategy_evidence_aggregation_{run_date.isoformat()}.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    typer.echo(f"ETF strategy evidence aggregation JSON：{output}")
    typer.echo(f"aggregation_status={payload['aggregation_status']}")
    typer.echo(f"loaded_sources={len(payload['loaded_sources'])}")
    typer.echo(f"missing_sources={len(payload['missing_sources'])}")
    typer.echo(f"stale_sources={len(payload['stale_sources'])}")
    typer.echo(f"blocked_sources={len(payload['blocked_sources'])}")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@evidence_dashboard_app.command("report")
def evidence_dashboard_report_command(
    as_of: Annotated[
        str,
        typer.Option("--as-of", "--date", help="评估日期 YYYY-MM-DD。"),
    ],
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="evidence dashboard source registry。"),
    ] = DEFAULT_STRATEGY_EVIDENCE_CONFIG_PATH,
    report_index_path: Annotated[
        Path | None,
        typer.Option("--report-index-path", help="可选 report index JSON。"),
    ] = None,
    report_registry_path: Annotated[
        Path,
        typer.Option("--report-registry-path", help="report registry YAML。"),
    ] = DEFAULT_REPORT_REGISTRY_PATH,
    root_path: Annotated[
        Path,
        typer.Option("--root-path", help="扫描既有 artifacts 的根目录。"),
    ] = PROJECT_ROOT,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="dashboard report 输出目录。"),
    ] = DEFAULT_STRATEGY_EVIDENCE_REPORT_DIR,
    json_path: Annotated[
        Path | None,
        typer.Option("--json-path", help="显式 JSON 输出路径。"),
    ] = None,
    markdown_path: Annotated[
        Path | None,
        typer.Option("--markdown-path", help="显式 Markdown 输出路径。"),
    ] = None,
) -> None:
    """生成 ETF Strategy Evidence Dashboard JSON / Markdown。"""
    run_date = _parse_date(as_of)
    dashboard = build_strategy_evidence_dashboard(
        as_of=run_date,
        config_path=config_path,
        report_index_path=report_index_path,
        report_registry_path=report_registry_path,
        root_path=root_path,
    )
    paths = write_strategy_evidence_dashboard_report(
        dashboard,
        json_path=json_path
        or output_dir / f"strategy_evidence_dashboard_{run_date.isoformat()}.json",
        markdown_path=markdown_path
        or output_dir / f"strategy_evidence_dashboard_{run_date.isoformat()}.md",
    )
    typer.echo(f"ETF strategy evidence dashboard JSON：{paths['json']}")
    typer.echo(f"ETF strategy evidence dashboard Markdown：{paths['markdown']}")
    typer.echo(f"overall_status={dashboard.overall_status}")
    typer.echo(f"evidence_card_count={len(dashboard.evidence_cards)}")
    typer.echo(f"candidate_ranking_count={len(dashboard.candidate_rankings)}")
    typer.echo(f"conflict_count={len(dashboard.conflicts)}")
    typer.echo(f"manual_review_priority_count={len(dashboard.manual_review_priorities)}")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@evidence_dashboard_app.command("validate")
def evidence_dashboard_validate_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="评估日期 YYYY-MM-DD；省略时使用今天。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="evidence dashboard source registry。"),
    ] = DEFAULT_STRATEGY_EVIDENCE_CONFIG_PATH,
    report_registry_path: Annotated[
        Path,
        typer.Option("--report-registry-path", help="report registry YAML。"),
    ] = DEFAULT_REPORT_REGISTRY_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="validation report 输出目录。"),
    ] = DEFAULT_STRATEGY_EVIDENCE_VALIDATION_DIR,
    json_path: Annotated[
        Path | None,
        typer.Option("--json-path", help="显式 JSON 输出路径。"),
    ] = None,
    markdown_path: Annotated[
        Path | None,
        typer.Option("--markdown-path", help="显式 Markdown 输出路径。"),
    ] = None,
) -> None:
    """校验 TRADING-076 evidence dashboard workflow 和 safety boundary。"""
    run_date = date.today() if as_of is None else _parse_date(as_of)
    payload = build_strategy_evidence_validation_report(
        as_of=run_date,
        config_path=config_path,
        report_registry_path=report_registry_path,
    )
    paths = write_strategy_evidence_validation_report(
        payload,
        json_path=json_path
        or output_dir / f"strategy_evidence_validation_{run_date.isoformat()}.json",
        markdown_path=markdown_path
        or output_dir / f"strategy_evidence_validation_{run_date.isoformat()}.md",
    )
    typer.echo(f"ETF strategy evidence validation JSON：{paths['json']}")
    typer.echo(f"ETF strategy evidence validation Markdown：{paths['markdown']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@baseline_review_app.command("eligibility")
def baseline_review_eligibility_command(
    candidate: Annotated[str, typer.Option("--candidate", help="Baseline review candidate ID。")],
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="评估日期 YYYY-MM-DD；省略时使用今天。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="baseline review policy config。"),
    ] = DEFAULT_BASELINE_REVIEW_POLICY_CONFIG_PATH,
    report_index_path: Annotated[
        Path | None,
        typer.Option("--report-index-path", help="可选 report index JSON。"),
    ] = None,
    report_registry_path: Annotated[
        Path,
        typer.Option("--report-registry-path", help="report registry YAML。"),
    ] = DEFAULT_REPORT_REGISTRY_PATH,
    root_path: Annotated[
        Path,
        typer.Option("--root-path", help="扫描既有 artifacts 的根目录。"),
    ] = PROJECT_ROOT,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="eligibility JSON 输出目录。"),
    ] = DEFAULT_BASELINE_REVIEW_REPORT_DIR / "eligibility",
    json_path: Annotated[
        Path | None,
        typer.Option("--json-path", help="显式 JSON 输出路径。"),
    ] = None,
) -> None:
    """评估 ETF baseline review candidate 是否可进入 owner manual review。"""
    run_date = date.today() if as_of is None else _parse_date(as_of)
    try:
        payload = build_baseline_review_eligibility(
            candidate_id=candidate,
            as_of=run_date,
            config_path=config_path,
            report_index_path=report_index_path,
            report_registry_path=report_registry_path,
            root_path=root_path,
        )
    except BaselineReviewError as exc:
        raise typer.BadParameter(str(exc)) from exc
    output = json_path or output_dir / f"baseline_review_eligibility_{run_date.isoformat()}.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    typer.echo(f"ETF baseline review eligibility JSON：{output}")
    typer.echo(f"candidate_id={payload['candidate_id']}")
    typer.echo(f"eligibility_status={payload['eligibility_status']}")
    typer.echo(f"blocker_count={len(payload['blockers'])}")
    typer.echo(f"warning_count={len(payload['warnings'])}")
    typer.echo("commands_executed=false")
    typer.echo("production_state_mutated=false")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")
    if payload["eligibility_status"] in {"blocked", "rejected_by_policy"}:
        raise typer.Exit(code=1)


@baseline_review_app.command("matrix")
def baseline_review_matrix_command(
    candidate: Annotated[str, typer.Option("--candidate", help="Baseline review candidate ID。")],
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="评估日期 YYYY-MM-DD；省略时使用今天。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="baseline review policy config。"),
    ] = DEFAULT_BASELINE_REVIEW_POLICY_CONFIG_PATH,
    report_index_path: Annotated[
        Path | None,
        typer.Option("--report-index-path", help="可选 report index JSON。"),
    ] = None,
    report_registry_path: Annotated[
        Path,
        typer.Option("--report-registry-path", help="report registry YAML。"),
    ] = DEFAULT_REPORT_REGISTRY_PATH,
    root_path: Annotated[
        Path,
        typer.Option("--root-path", help="扫描既有 artifacts 的根目录。"),
    ] = PROJECT_ROOT,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="evidence matrix JSON 输出目录。"),
    ] = DEFAULT_BASELINE_REVIEW_REPORT_DIR / "matrix",
    json_path: Annotated[
        Path | None,
        typer.Option("--json-path", help="显式 JSON 输出路径。"),
    ] = None,
) -> None:
    """生成 ETF baseline review evidence requirement matrix。"""
    run_date = date.today() if as_of is None else _parse_date(as_of)
    try:
        matrix = build_baseline_review_evidence_matrix(
            candidate_id=candidate,
            as_of=run_date,
            config_path=config_path,
            report_index_path=report_index_path,
            report_registry_path=report_registry_path,
            root_path=root_path,
        )
    except BaselineReviewError as exc:
        raise typer.BadParameter(str(exc)) from exc
    payload = matrix.model_dump(mode="json")
    output = json_path or output_dir / f"baseline_review_matrix_{run_date.isoformat()}.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    typer.echo(f"ETF baseline review matrix JSON：{output}")
    typer.echo(f"candidate_id={matrix.candidate_id}")
    typer.echo(f"row_count={len(matrix.rows)}")
    typer.echo(f"blocking_row_count={len([row for row in matrix.rows if row.blocking])}")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@baseline_review_app.command("package")
def baseline_review_package_command(
    candidate: Annotated[str, typer.Option("--candidate", help="Baseline review candidate ID。")],
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="评估日期 YYYY-MM-DD；省略时使用今天。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="baseline review policy config。"),
    ] = DEFAULT_BASELINE_REVIEW_POLICY_CONFIG_PATH,
    report_index_path: Annotated[
        Path | None,
        typer.Option("--report-index-path", help="可选 report index JSON。"),
    ] = None,
    report_registry_path: Annotated[
        Path,
        typer.Option("--report-registry-path", help="report registry YAML。"),
    ] = DEFAULT_REPORT_REGISTRY_PATH,
    root_path: Annotated[
        Path,
        typer.Option("--root-path", help="扫描既有 artifacts 的根目录。"),
    ] = PROJECT_ROOT,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="review package 输出目录。"),
    ] = DEFAULT_BASELINE_REVIEW_PACKAGE_DIR,
    json_path: Annotated[
        Path | None,
        typer.Option("--json-path", help="显式 JSON 输出路径。"),
    ] = None,
    markdown_path: Annotated[
        Path | None,
        typer.Option("--markdown-path", help="显式 Markdown 输出路径。"),
    ] = None,
) -> None:
    """生成 ETF baseline candidate human review package。"""
    run_date = date.today() if as_of is None else _parse_date(as_of)
    try:
        payload = build_baseline_review_package(
            candidate_id=candidate,
            as_of=run_date,
            config_path=config_path,
            report_index_path=report_index_path,
            report_registry_path=report_registry_path,
            root_path=root_path,
        )
    except BaselineReviewError as exc:
        raise typer.BadParameter(str(exc)) from exc
    paths = write_baseline_review_package(
        payload,
        json_path=json_path or output_dir / f"baseline_review_package_{run_date.isoformat()}.json",
        markdown_path=markdown_path
        or output_dir / f"baseline_review_package_{run_date.isoformat()}.md",
    )
    typer.echo(f"ETF baseline review package JSON：{paths['json']}")
    typer.echo(f"ETF baseline review package Markdown：{paths['markdown']}")
    typer.echo(f"candidate_id={payload['candidate_id']}")
    typer.echo(f"eligibility_status={payload['eligibility']['eligibility_status']}")
    typer.echo(f"blocker_count={len(payload['blockers'])}")
    typer.echo("commands_executed=false")
    typer.echo("production_state_mutated=false")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@baseline_review_app.command("capture-decision")
def baseline_review_capture_decision_command(
    review_package_path: Annotated[
        Path,
        typer.Option("--review-package-path", help="Baseline review package JSON。"),
    ],
    owner_decision: Annotated[str, typer.Option("--owner-decision", help="Owner decision。")],
    rationale: Annotated[str, typer.Option("--rationale", help="Owner rationale。")],
    confidence: Annotated[float, typer.Option("--confidence", help="0.0-1.0 confidence。")],
    condition: Annotated[
        list[str] | None,
        typer.Option("--condition", help="Decision condition; can be repeated。"),
    ] = None,
    follow_up_task: Annotated[
        list[str] | None,
        typer.Option("--follow-up-task", help="Follow-up task; can be repeated。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="baseline review policy config。"),
    ] = DEFAULT_BASELINE_REVIEW_POLICY_CONFIG_PATH,
    journal_path: Annotated[
        Path,
        typer.Option("--journal-path", help="ETF decision journal path。"),
    ] = DEFAULT_DECISION_JOURNAL_PATH,
    link_journal: Annotated[
        bool,
        typer.Option("--link-journal/--skip-journal-link", help="是否写入 decision journal。"),
    ] = True,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="decision capture 输出目录。"),
    ] = DEFAULT_BASELINE_REVIEW_DECISION_DIR,
    json_path: Annotated[
        Path | None,
        typer.Option("--json-path", help="显式 JSON 输出路径。"),
    ] = None,
    markdown_path: Annotated[
        Path | None,
        typer.Option("--markdown-path", help="显式 Markdown 输出路径。"),
    ] = None,
) -> None:
    """捕获 owner baseline review decision，并可链接到 decision journal。"""
    try:
        package = _load_optional_json_payload(review_package_path)
        decision = build_owner_review_decision(
            review_package=package,
            owner_decision=owner_decision,
            rationale=rationale,
            confidence=confidence,
            conditions=condition,
            follow_up_tasks=follow_up_task,
            config_path=config_path,
        )
        if link_journal:
            decision = link_baseline_review_decision_to_journal(
                decision,
                review_package_path=review_package_path,
                journal_path=journal_path,
            )
    except BaselineReviewError as exc:
        raise typer.BadParameter(str(exc)) from exc
    stem = _artifact_stem(decision["decision_id"])
    paths = write_owner_review_decision(
        decision,
        json_path=json_path or output_dir / f"{stem}.json",
        markdown_path=markdown_path or output_dir / f"{stem}.md",
    )
    typer.echo(f"ETF baseline review decision JSON：{paths['json']}")
    typer.echo(f"ETF baseline review decision Markdown：{paths['markdown']}")
    typer.echo(f"decision_id={decision['decision_id']}")
    typer.echo(f"owner_decision={decision['owner_decision']}")
    typer.echo(f"decision_journal_status={decision['decision_journal_linkage']['status']}")
    typer.echo("commands_executed=false")
    typer.echo("production_state_mutated=false")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@baseline_review_app.command("proposal-draft")
def baseline_review_proposal_draft_command(
    review_package_path: Annotated[
        Path,
        typer.Option("--review-package-path", help="Baseline review package JSON。"),
    ],
    decision_path: Annotated[
        Path,
        typer.Option("--decision-path", help="Owner decision JSON。"),
    ],
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="baseline review policy config。"),
    ] = DEFAULT_BASELINE_REVIEW_POLICY_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="proposal draft 输出目录。"),
    ] = DEFAULT_BASELINE_REVIEW_PROPOSAL_DIR,
    json_path: Annotated[
        Path | None,
        typer.Option("--json-path", help="显式 JSON 输出路径。"),
    ] = None,
    markdown_path: Annotated[
        Path | None,
        typer.Option("--markdown-path", help="显式 Markdown 输出路径。"),
    ] = None,
) -> None:
    """仅在 owner approve 后生成 baseline change proposal draft；不应用变更。"""
    try:
        package = _load_optional_json_payload(review_package_path)
        decision = _load_optional_json_payload(decision_path)
        payload = build_baseline_change_proposal_draft(
            review_package=package,
            owner_decision=decision,
            config_path=config_path,
        )
    except BaselineReviewError as exc:
        raise typer.BadParameter(str(exc)) from exc
    stem = _artifact_stem(payload["proposal_id"])
    paths = write_baseline_change_proposal_draft(
        payload,
        json_path=json_path or output_dir / f"{stem}.json",
        markdown_path=markdown_path or output_dir / f"{stem}.md",
    )
    typer.echo(f"ETF baseline change proposal draft JSON：{paths['json']}")
    typer.echo(f"ETF baseline change proposal draft Markdown：{paths['markdown']}")
    typer.echo(f"proposal_id={payload['proposal_id']}")
    typer.echo("proposal_is_draft_only=true")
    typer.echo("baseline_config_mutated=false")
    typer.echo("target_weights_mutated=false")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@baseline_review_app.command("outcome")
def baseline_review_outcome_command(
    candidate: Annotated[str, typer.Option("--candidate", help="Baseline review candidate ID。")],
    decision_path: Annotated[
        Path | None,
        typer.Option("--decision-path", help="Optional owner decision JSON。"),
    ] = None,
    proposal_path: Annotated[
        Path | None,
        typer.Option("--proposal-path", help="Optional proposal draft JSON。"),
    ] = None,
    previous_outcome_path: Annotated[
        Path | None,
        typer.Option("--previous-outcome-path", help="Optional prior outcome JSON。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="baseline review policy config。"),
    ] = DEFAULT_BASELINE_REVIEW_POLICY_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="review outcome 输出目录。"),
    ] = DEFAULT_BASELINE_REVIEW_OUTCOME_DIR,
    json_path: Annotated[
        Path | None,
        typer.Option("--json-path", help="显式 JSON 输出路径。"),
    ] = None,
    markdown_path: Annotated[
        Path | None,
        typer.Option("--markdown-path", help="显式 Markdown 输出路径。"),
    ] = None,
) -> None:
    """记录 baseline review outcome；不修改 production state。"""
    try:
        decision = _load_optional_json_payload(decision_path) if decision_path else None
        proposal = _load_optional_json_payload(proposal_path) if proposal_path else None
        previous = (
            _load_optional_json_payload(previous_outcome_path)
            if previous_outcome_path
            else None
        )
        payload = build_candidate_review_outcome(
            candidate_id=candidate,
            decision=decision,
            proposal=proposal,
            previous_outcome=previous,
            config_path=config_path,
        )
    except BaselineReviewError as exc:
        raise typer.BadParameter(str(exc)) from exc
    stem = _artifact_stem(f"baseline-review-outcome-{candidate}")
    paths = write_baseline_review_outcome(
        payload,
        json_path=json_path or output_dir / f"{stem}.json",
        markdown_path=markdown_path or output_dir / f"{stem}.md",
    )
    typer.echo(f"ETF baseline review outcome JSON：{paths['json']}")
    typer.echo(f"ETF baseline review outcome Markdown：{paths['markdown']}")
    typer.echo(f"candidate_id={payload['candidate_id']}")
    typer.echo(f"latest_review_status={payload['latest_review_status']}")
    typer.echo("commands_executed=false")
    typer.echo("production_state_mutated=false")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@baseline_review_app.command("validate")
def baseline_review_validate_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="评估日期 YYYY-MM-DD；省略时使用今天。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="baseline review policy config。"),
    ] = DEFAULT_BASELINE_REVIEW_POLICY_CONFIG_PATH,
    report_registry_path: Annotated[
        Path,
        typer.Option("--report-registry-path", help="report registry YAML。"),
    ] = DEFAULT_REPORT_REGISTRY_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="validation report 输出目录。"),
    ] = DEFAULT_BASELINE_REVIEW_VALIDATION_DIR,
    json_path: Annotated[
        Path | None,
        typer.Option("--json-path", help="显式 JSON 输出路径。"),
    ] = None,
    markdown_path: Annotated[
        Path | None,
        typer.Option("--markdown-path", help="显式 Markdown 输出路径。"),
    ] = None,
) -> None:
    """校验 TRADING-077 baseline review playbook 完整性和安全边界。"""
    run_date = date.today() if as_of is None else _parse_date(as_of)
    payload = build_baseline_review_validation_report(
        as_of=run_date,
        config_path=config_path,
        report_registry_path=report_registry_path,
    )
    paths = write_baseline_review_validation_report(
        payload,
        json_path=json_path
        or output_dir / f"baseline_review_validation_{run_date.isoformat()}.json",
        markdown_path=markdown_path
        or output_dir / f"baseline_review_validation_{run_date.isoformat()}.md",
    )
    typer.echo(f"ETF baseline review validation JSON：{paths['json']}")
    typer.echo(f"ETF baseline review validation Markdown：{paths['markdown']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo(f"warning_check_count={payload['warning_check_count']}")
    typer.echo("commands_executed=false")
    typer.echo("production_state_mutated=false")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@shadow_review_app.command("package")
def shadow_review_package_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取最新 diagnostics artifacts。"),
    ] = True,
    diagnostics_json_path: Annotated[
        Path | None,
        typer.Option("--diagnostics-json-path", help="historical diagnostics JSON。"),
    ] = None,
    stable_shapes_csv_path: Annotated[
        Path | None,
        typer.Option("--stable-shapes-csv-path", help="stable shapes CSV。"),
    ] = None,
    near_shadow_csv_path: Annotated[
        Path | None,
        typer.Option("--near-shadow-csv-path", help="near-shadow CSV。"),
    ] = None,
    diagnostics_dir: Annotated[
        Path,
        typer.Option("--diagnostics-dir", help="diagnostics artifact directory。"),
    ] = DEFAULT_WEIGHT_SEARCH_DIAGNOSTICS_DIR,
    top: Annotated[int, typer.Option("--top", help="review package top N candidates。")] = 3,
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="shadow-ready review policy config。"),
    ] = DEFAULT_SHADOW_READY_REVIEW_POLICY_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="review package 输出目录。"),
    ] = DEFAULT_SHADOW_READY_REVIEW_PACKAGE_DIR,
    json_path: Annotated[
        Path | None,
        typer.Option("--json-path", help="显式 JSON 输出路径。"),
    ] = None,
    markdown_path: Annotated[
        Path | None,
        typer.Option("--markdown-path", help="显式 Markdown 输出路径。"),
    ] = None,
) -> None:
    """生成 TRADING-082 shadow-ready candidate owner review package。"""
    try:
        policy = load_shadow_ready_review_policy_config(config_path)
        artifacts = load_shadow_review_diagnostics_artifacts(
            diagnostics_json_path=diagnostics_json_path,
            stable_shapes_csv_path=stable_shapes_csv_path,
            near_shadow_csv_path=near_shadow_csv_path,
            diagnostics_dir=diagnostics_dir,
            latest=latest,
        )
        aggregation = aggregate_shadow_ready_review_candidates(artifacts, policy=policy)
        ranking = rank_shadow_ready_review_candidates(aggregation, policy=policy)
        near_shadow = build_near_shadow_review_summary(artifacts)
        payload = build_shadow_candidate_review_package(
            artifacts=artifacts,
            aggregation=aggregation,
            ranking=ranking,
            near_shadow_summary=near_shadow,
            policy=policy,
            top=top,
        )
    except ShadowReadyReviewError as exc:
        raise typer.BadParameter(str(exc)) from exc
    stem = _artifact_stem(str(payload["review_package_id"]))
    paths = write_shadow_candidate_review_package(
        payload,
        json_path=json_path or output_dir / f"{stem}.json",
        markdown_path=markdown_path or output_dir / f"{stem}.md",
    )
    summary = payload["review_summary"]
    typer.echo(f"ETF shadow candidate review package JSON：{paths['json']}")
    typer.echo(f"ETF shadow candidate review package Markdown：{paths['markdown']}")
    typer.echo(f"review_package_id={payload['review_package_id']}")
    typer.echo(f"top_candidate={summary['top_candidate']}")
    typer.echo(f"pending_review_count={summary['pending_review_count']}")
    typer.echo(f"blocked_count={summary['blocked_count']}")
    typer.echo("commands_executed=false")
    typer.echo("production_state_mutated=false")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")
    if payload["diagnostics_source_summary"]["artifact_status"] == "FAIL":
        raise typer.Exit(code=1)


@shadow_review_app.command("approve")
def shadow_review_approve_command(
    review_package_path: Annotated[
        Path,
        typer.Option("--review-package-path", "--package", help="review package JSON。"),
    ],
    shape_id: Annotated[str, typer.Option("--shape", "--shape-id", help="shape id。")],
    owner_decision: Annotated[str, typer.Option("--owner-decision", help="owner decision。")],
    rationale: Annotated[str, typer.Option("--rationale", help="owner rationale。")],
    confidence: Annotated[float, typer.Option("--confidence", help="0.0-1.0 confidence。")],
    selected_weight_set_id: Annotated[
        str | None,
        typer.Option("--selected-weight-set-id", "--weight-set", help="selected weight set id。"),
    ] = None,
    condition: Annotated[
        list[str] | None,
        typer.Option("--condition", help="approval condition; can be repeated。"),
    ] = None,
    decision_journal_link: Annotated[
        str | None,
        typer.Option("--decision-journal-link", help="decision journal link/id。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="shadow-ready review policy config。"),
    ] = DEFAULT_SHADOW_READY_REVIEW_POLICY_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="approval 输出目录。"),
    ] = DEFAULT_SHADOW_READY_REVIEW_APPROVAL_DIR,
    json_path: Annotated[
        Path | None,
        typer.Option("--json-path", help="显式 JSON 输出路径。"),
    ] = None,
    markdown_path: Annotated[
        Path | None,
        typer.Option("--markdown-path", help="显式 Markdown 输出路径。"),
    ] = None,
) -> None:
    """捕获 owner shadow-review approval；不 enroll、不修改 production。"""
    try:
        package = _load_optional_json_payload(review_package_path)
        payload = build_shadow_candidate_owner_approval(
            review_package=package,
            shape_id=shape_id,
            selected_weight_set_id=selected_weight_set_id,
            owner_decision=owner_decision,
            rationale=rationale,
            confidence=confidence,
            conditions=condition,
            decision_journal_link=decision_journal_link,
            policy=load_shadow_ready_review_policy_config(config_path),
        )
    except ShadowReadyReviewError as exc:
        raise typer.BadParameter(str(exc)) from exc
    stem = _artifact_stem(str(payload["approval_id"]))
    paths = write_shadow_candidate_owner_approval(
        payload,
        json_path=json_path or output_dir / f"{stem}.json",
        markdown_path=markdown_path or output_dir / f"{stem}.md",
    )
    typer.echo(f"ETF shadow candidate owner approval JSON：{paths['json']}")
    typer.echo(f"ETF shadow candidate owner approval Markdown：{paths['markdown']}")
    typer.echo(f"approval_id={payload['approval_id']}")
    typer.echo(f"owner_decision={payload['owner_decision']}")
    typer.echo(f"shape_id={payload['shape_id']}")
    typer.echo("commands_executed=false")
    typer.echo("production_state_mutated=false")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@shadow_review_app.command("enroll-approved")
def shadow_review_enroll_approved_command(
    approval_path: Annotated[
        Path,
        typer.Option("--approval-path", "--approval", help="owner approval JSON。"),
    ],
    review_package_path: Annotated[
        Path,
        typer.Option("--review-package-path", "--package", help="review package JSON。"),
    ],
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="shadow-ready review policy config。"),
    ] = DEFAULT_SHADOW_READY_REVIEW_POLICY_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="approved enrollment 输出目录。"),
    ] = DEFAULT_SHADOW_READY_REVIEW_ENROLLMENT_DIR,
    json_path: Annotated[
        Path | None,
        typer.Option("--json-path", help="显式 JSON 输出路径。"),
    ] = None,
    markdown_path: Annotated[
        Path | None,
        typer.Option("--markdown-path", help="显式 Markdown 输出路径。"),
    ] = None,
) -> None:
    """只对 owner-approved candidates 建立 forward shadow tracking enrollment。"""
    try:
        approval = _load_optional_json_payload(approval_path)
        package = _load_optional_json_payload(review_package_path)
        payload = build_shadow_candidate_approved_enrollment(
            approval=approval,
            review_package=package,
            policy=load_shadow_ready_review_policy_config(config_path),
        )
    except ShadowReadyReviewError as exc:
        raise typer.BadParameter(str(exc)) from exc
    stem = _artifact_stem(str(payload["enrollment_id"]))
    paths = write_shadow_candidate_approved_enrollment(
        payload,
        json_path=json_path or output_dir / f"{stem}.json",
        markdown_path=markdown_path or output_dir / f"{stem}.md",
    )
    typer.echo(f"ETF shadow candidate enrollment JSON：{paths['json']}")
    typer.echo(f"ETF shadow candidate enrollment Markdown：{paths['markdown']}")
    typer.echo(f"enrollment_id={payload['enrollment_id']}")
    typer.echo(f"shadow_candidate_id={payload['shadow_candidate_id']}")
    typer.echo(f"forward_tracking_status={payload['forward_tracking_status']}")
    typer.echo("commands_executed=false")
    typer.echo("production_state_mutated=false")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@shadow_review_app.command("validate")
def shadow_review_validate_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="shadow-ready review policy config。"),
    ] = DEFAULT_SHADOW_READY_REVIEW_POLICY_CONFIG_PATH,
    report_registry_path: Annotated[
        Path,
        typer.Option("--report-registry-path", help="report registry YAML。"),
    ] = DEFAULT_REPORT_REGISTRY_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="validation report 输出目录。"),
    ] = DEFAULT_SHADOW_READY_REVIEW_VALIDATION_DIR,
    json_path: Annotated[
        Path | None,
        typer.Option("--json-path", help="显式 JSON 输出路径。"),
    ] = None,
    markdown_path: Annotated[
        Path | None,
        typer.Option("--markdown-path", help="显式 Markdown 输出路径。"),
    ] = None,
) -> None:
    """校验 TRADING-082 shadow candidate review/enrollment workflow 和 safety boundary。"""
    payload = build_shadow_candidate_review_validation_report(
        config_path=config_path,
        report_registry_path=report_registry_path,
    )
    stem = _artifact_stem(str(payload["validation_id"]))
    paths = write_shadow_candidate_review_validation_report(
        payload,
        json_path=json_path or output_dir / f"{stem}.json",
        markdown_path=markdown_path or output_dir / f"{stem}.md",
    )
    typer.echo(f"ETF shadow candidate review validation JSON：{paths['json']}")
    typer.echo(f"ETF shadow candidate review validation Markdown：{paths['markdown']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("commands_executed=false")
    typer.echo("production_state_mutated=false")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@trend_calibration_app.command("run")
def trend_calibration_run_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="标准化 ETF daily price cache。"),
    ] = DEFAULT_ETF_PRICE_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="标准化 FRED rates cache for validate-data gate。"),
    ] = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv",
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="数据质量门禁日期，默认 today。"),
    ] = None,
    start: Annotated[
        str | None,
        typer.Option("--start", help="trend calibration requested start date。"),
    ] = None,
    end: Annotated[
        str | None,
        typer.Option("--end", help="trend calibration requested end date。"),
    ] = None,
    top: Annotated[int, typer.Option("--top", help="Top trend configs to retain。")] = 5,
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="trend calibration policy config。"),
    ] = DEFAULT_TREND_CALIBRATION_POLICY_CONFIG_PATH,
    data_quality_output_path: Annotated[
        Path | None,
        typer.Option("--data-quality-output-path", help="validate-data markdown output path。"),
    ] = None,
    dataset_output_dir: Annotated[
        Path,
        typer.Option("--dataset-output-dir", help="trend signal dataset 输出目录。"),
    ] = DEFAULT_TREND_CALIBRATION_DATASET_DIR,
    report_output_dir: Annotated[
        Path,
        typer.Option("--report-output-dir", help="trend calibration report 输出目录。"),
    ] = DEFAULT_TREND_CALIBRATION_REPORT_DIR,
    registry_output_dir: Annotated[
        Path,
        typer.Option("--registry-output-dir", help="trend signal config registry 输出目录。"),
    ] = DEFAULT_TREND_CALIBRATION_REGISTRY_DIR,
) -> None:
    """运行 TRADING-083 trend signal weight calibration；不输出 target weights。"""
    validation_date = _parse_date(as_of) if as_of else date.today()
    quality_output = data_quality_output_path or default_quality_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        validation_date,
    )
    universe = load_universe()
    data_quality_report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=configured_price_tickers(universe),
        expected_rate_series=configured_rate_series(universe),
        quality_config=load_data_quality(),
        as_of=validation_date,
        manifest_path=_download_manifest_path(prices_path),
        secondary_prices_path=_marketstack_prices_path(prices_path),
        require_secondary_prices=_requires_marketstack_prices(prices_path),
    )
    write_cache_data_quality_report(data_quality_report, quality_output)
    typer.echo(f"validate_data_status={data_quality_report.status}")
    typer.echo(f"validate_data_report={quality_output}")
    if not data_quality_report.passed:
        raise typer.Exit(code=1)
    try:
        policy = load_trend_calibration_policy_config(config_path)
        etf_config = load_etf_config_bundle()
        prices, etf_quality = load_standard_prices(
            prices_path,
            etf_config.assets,
            etf_config.strategy,
        )
        if not etf_quality.passed:
            raise TrendCalibrationError(
                f"ETF price validation failed before trend calibration: {etf_quality.status}"
            )
        start_date = _parse_date(start) if start else None
        end_date = _parse_date(end) if end else None
        features = build_feature_store(
            prices,
            assets=etf_config.assets,
            strategy=etf_config.strategy,
            start=None,
            end=end_date,
        )
        dataset = build_trend_signal_dataset(
            features=features,
            prices=prices,
            strategy=etf_config.strategy,
            policy=policy,
            start=start_date,
            end=end_date,
            data_quality_status=data_quality_report.status,
            data_quality_report=str(quality_output),
            price_source_path=str(prices_path),
        )
        report = build_trend_calibration_report(
            dataset=dataset,
            policy=policy,
            top=top,
        )
    except TrendCalibrationError as exc:
        raise typer.BadParameter(str(exc)) from exc
    dataset_paths = write_trend_signal_dataset(dataset, output_dir=dataset_output_dir)
    report_paths = write_trend_calibration_report(report, output_dir=report_output_dir)
    registry_paths = write_trend_signal_config_registry(
        report["trend_signal_config_registry"],
        output_dir=registry_output_dir,
    )
    summary = report["summary"]
    typer.echo(f"ETF trend signal dataset JSON：{dataset_paths['json']}")
    typer.echo(f"ETF trend calibration report JSON：{report_paths['json']}")
    typer.echo(f"ETF trend signal config registry JSON：{registry_paths['json']}")
    typer.echo(f"status={report['status']}")
    typer.echo(f"top_config={summary['top_config']}")
    typer.echo(f"top_quality_score={summary['top_quality_score']}")
    typer.echo("evaluation_only=true")
    typer.echo("commands_executed=false")
    typer.echo("production_state_mutated=false")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@trend_calibration_app.command("report")
def trend_calibration_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取最新 trend calibration report。"),
    ] = True,
    report_path: Annotated[
        Path | None,
        typer.Option("--report-path", help="显式 report JSON path。"),
    ] = None,
    report_dir: Annotated[
        Path,
        typer.Option("--report-dir", help="report artifact directory。"),
    ] = DEFAULT_TREND_CALIBRATION_REPORT_DIR,
) -> None:
    """只读展示 latest TRADING-083 trend calibration report 摘要。"""
    resolved = report_path
    if resolved is None and latest:
        resolved = latest_trend_calibration_report_path(report_dir)
    if resolved is None:
        raise typer.BadParameter("trend calibration report not found")
    payload = _load_optional_json_payload(resolved)
    summary = payload.get("summary")
    if not isinstance(summary, Mapping):
        raise typer.BadParameter(f"invalid trend calibration report: {resolved}")
    typer.echo(f"trend_calibration_report={resolved}")
    typer.echo(f"status={payload.get('status')}")
    typer.echo(f"top_config={summary.get('top_config')}")
    typer.echo(f"evidence_status={summary.get('evidence_status')}")
    typer.echo(f"redundancy_risk={summary.get('redundancy_risk')}")
    typer.echo(f"regime_stability={summary.get('regime_stability')}")
    typer.echo("evaluation_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")


@trend_calibration_app.command("validate")
def trend_calibration_validate_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="trend calibration policy config。"),
    ] = DEFAULT_TREND_CALIBRATION_POLICY_CONFIG_PATH,
    report_registry_path: Annotated[
        Path,
        typer.Option("--report-registry-path", help="report registry YAML。"),
    ] = DEFAULT_REPORT_REGISTRY_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="validation report 输出目录。"),
    ] = DEFAULT_TREND_CALIBRATION_VALIDATION_DIR,
) -> None:
    """校验 TRADING-083 trend calibration workflow 和 safety boundary。"""
    payload = build_trend_calibration_validation_report(
        config_path=config_path,
        report_registry_path=report_registry_path,
    )
    paths = write_trend_calibration_validation_report(payload, output_dir=output_dir)
    typer.echo(f"ETF trend calibration validation JSON：{paths['json']}")
    typer.echo(f"ETF trend calibration validation Markdown：{paths['markdown']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("evaluation_only=true")
    typer.echo("commands_executed=false")
    typer.echo("production_state_mutated=false")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@etf_app.command("validate-config")
def validate_config_command() -> None:
    """校验 ETF P0 配置。"""
    config = load_etf_config_bundle()
    typer.echo("ETF 配置校验通过。")
    typer.echo(f"model_version={config.strategy.model.version}")
    typer.echo(f"config_hash={config.config_hash}")
    typer.echo(f"assets={', '.join(config.assets.assets)}")


@ops_app.command("dry-run")
def ops_dry_run_command(
    cadence: Annotated[
        str,
        typer.Option("--cadence", help="Operations cadence: daily/weekly/biweekly/monthly。"),
    ],
    as_of: Annotated[
        str,
        typer.Option("--as-of", "--date", help="评估日期 YYYY-MM-DD。"),
    ],
    root_path: Annotated[
        Path,
        typer.Option("--root-path", help="扫描既有 artifacts 的根目录。"),
    ] = PROJECT_ROOT,
    output_path: Annotated[
        Path | None,
        typer.Option("--output-path", help="dry-run JSON 输出路径。"),
    ] = None,
    include_optional: Annotated[
        bool,
        typer.Option(
            "--include-optional/--skip-optional",
            help="是否把 optional steps 纳入计划。",
        ),
    ] = True,
    no_write: Annotated[
        bool,
        typer.Option("--no-write", help="只打印 dry-run 摘要，不写 JSON artifact。"),
    ] = False,
) -> None:
    """只规划 ETF operations cadence，不执行命令、不写 production state。"""
    requested_cadence = _parse_operations_graph_cadence(cadence)
    report = build_operations_scheduler_dry_run(
        cadence=requested_cadence,
        as_of=as_of,
        root_path=root_path,
        include_optional=include_optional,
    )
    output = output_path or (
        DEFAULT_ETF_OPERATIONS_DRY_RUN_DIR
        / f"operations_dry_run_{report.cadence}_{report.as_of_date.isoformat()}.json"
    )
    if not no_write:
        write_operations_scheduler_dry_run(report, output)
        typer.echo(f"ETF operations dry-run JSON：{output}")
    else:
        typer.echo("ETF operations dry-run JSON：not_written")
    typer.echo(f"dry_run_id={report.dry_run_id}")
    typer.echo(f"cadence={report.cadence}")
    typer.echo(f"as_of_date={report.as_of_date.isoformat()}")
    typer.echo(f"status={report.status}")
    typer.echo(f"planned_step_count={len(report.planned_steps)}")
    typer.echo(f"blocking_failure_count={len(report.blocking_failures)}")
    typer.echo(f"warning_count={len(report.warnings)}")
    typer.echo("dry_run_only=true")
    typer.echo("commands_executed=false")
    typer.echo("production_state_mutated=false")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@ops_app.command("report")
def ops_report_command(
    cadence: Annotated[
        str,
        typer.Option("--cadence", help="Operations cadence: daily/weekly/biweekly/monthly。"),
    ],
    as_of: Annotated[
        str,
        typer.Option("--as-of", "--date", help="评估日期 YYYY-MM-DD。"),
    ],
    root_path: Annotated[
        Path,
        typer.Option("--root-path", help="扫描既有 artifacts 的根目录。"),
    ] = PROJECT_ROOT,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="operations report 输出目录。"),
    ] = DEFAULT_ETF_OPERATIONS_REPORT_DIR,
    json_path: Annotated[
        Path | None,
        typer.Option("--json-path", help="显式 JSON 输出路径。"),
    ] = None,
    markdown_path: Annotated[
        Path | None,
        typer.Option("--markdown-path", help="显式 Markdown 输出路径。"),
    ] = None,
    include_optional: Annotated[
        bool,
        typer.Option(
            "--include-optional/--skip-optional",
            help="是否把 optional steps 纳入 report plan。",
        ),
    ] = True,
) -> None:
    """生成 ETF operations health JSON / Markdown report；不执行计划命令。"""
    requested_cadence = _parse_operations_graph_cadence(cadence)
    report = build_operations_health_report(
        cadence=requested_cadence,
        as_of=as_of,
        root_path=root_path,
        include_optional=include_optional,
    )
    cadence_dir = output_dir / report.cadence
    json_output = json_path or (
        cadence_dir / f"operations_health_{report.as_of_date.isoformat()}.json"
    )
    markdown_output = markdown_path or (
        cadence_dir / f"operations_health_{report.as_of_date.isoformat()}.md"
    )
    paths = write_operations_health_report(
        report,
        json_path=json_output,
        markdown_path=markdown_output,
    )
    typer.echo(f"ETF operations health JSON：{paths['json']}")
    typer.echo(f"ETF operations health Markdown：{paths['markdown']}")
    typer.echo(f"report_id={report.report_id}")
    typer.echo(f"cadence={report.cadence}")
    typer.echo(f"as_of_date={report.as_of_date.isoformat()}")
    typer.echo(f"status={report.status}")
    typer.echo(f"planned_step_count={len(report.pipeline_schedule)}")
    typer.echo(f"blocking_failure_count={len(report.failures)}")
    typer.echo(f"warning_count={len(report.warnings)}")
    typer.echo("commands_executed=false")
    typer.echo("production_state_mutated=false")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@ops_app.command("validate")
def ops_validate_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="评估日期 YYYY-MM-DD；省略时使用今天。"),
    ] = None,
    root_path: Annotated[
        Path,
        typer.Option("--root-path", help="扫描既有 artifacts 的根目录。"),
    ] = PROJECT_ROOT,
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="operations schedule config path。"),
    ] = DEFAULT_ETF_OPERATIONS_SCHEDULE_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="operations validation 输出目录。"),
    ] = DEFAULT_ETF_OPERATIONS_VALIDATION_DIR,
    json_path: Annotated[
        Path | None,
        typer.Option("--json-path", help="显式 JSON 输出路径。"),
    ] = None,
    markdown_path: Annotated[
        Path | None,
        typer.Option("--markdown-path", help="显式 Markdown 输出路径。"),
    ] = None,
) -> None:
    """校验 ETF operations workflow 完整性和安全边界；失败时 exit 1。"""
    requested_as_of = as_of or date.today().isoformat()
    report = build_operations_validation_report(
        as_of=requested_as_of,
        root_path=root_path,
        config_path=config_path,
    )
    json_output = json_path or (
        output_dir / f"operations_validation_{report.as_of_date.isoformat()}.json"
    )
    markdown_output = markdown_path or (
        output_dir / f"operations_validation_{report.as_of_date.isoformat()}.md"
    )
    paths = write_operations_validation_report(
        report,
        json_path=json_output,
        markdown_path=markdown_output,
    )
    typer.echo(f"ETF operations validation JSON：{paths['json']}")
    typer.echo(f"ETF operations validation Markdown：{paths['markdown']}")
    typer.echo(f"report_id={report.report_id}")
    typer.echo(f"as_of_date={report.as_of_date.isoformat()}")
    typer.echo(f"status={report.status}")
    typer.echo(f"failed_check_count={report.failed_check_count}")
    typer.echo(f"warning_check_count={report.warning_check_count}")
    typer.echo("commands_executed=false")
    typer.echo("production_state_mutated=false")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")
    if report.status != "PASS":
        raise typer.Exit(code=1)


@data_quality_app.command("price-freshness")
def data_quality_price_freshness_command(
    prices_path: Annotated[Path, typer.Option(help="ETF 标准价格 CSV/Parquet 路径。")] = (
        DEFAULT_ETF_PRICE_PATH
    ),
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="评估日期 YYYY-MM-DD；省略时使用 latest。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="data quality policy config path。"),
    ] = DEFAULT_ETF_DATA_QUALITY_POLICY_CONFIG_PATH,
) -> None:
    """检查 ETF required / optional price freshness；不写 production state。"""
    config = load_etf_config_bundle()
    policy = load_data_quality_policy_config(config_path)
    run_date = _resolve_date(as_of, prices_path=prices_path)
    raw = read_price_frame(prices_path)
    prices, _ = standardize_price_frame(
        raw,
        assets=config.assets,
        source_name=str(prices_path),
        extra_symbols=set(policy.data_quality.price_freshness.optional_assets),
    )
    payload = check_price_freshness(prices, policy=policy, as_of=run_date)
    summary = payload["summary"]
    typer.echo(f"ETF data quality price freshness as_of={run_date.isoformat()}")
    typer.echo(f"record_count={summary['record_count']}")
    typer.echo(f"blocking_count={summary['blocking_count']}")
    typer.echo(f"warning_count={summary['warning_count']}")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")
    if summary["blocking_count"]:
        raise typer.Exit(code=1)


@data_quality_app.command("report")
def data_quality_report_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="评估日期 YYYY-MM-DD；省略时使用 latest。"),
    ] = None,
    prices_path: Annotated[Path, typer.Option(help="ETF 标准价格 CSV/Parquet 路径。")] = (
        DEFAULT_ETF_PRICE_PATH
    ),
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="data quality policy config path。"),
    ] = DEFAULT_ETF_DATA_QUALITY_POLICY_CONFIG_PATH,
    report_registry_path: Annotated[
        Path,
        typer.Option("--report-registry-path", help="report registry YAML 路径。"),
    ] = DEFAULT_REPORT_REGISTRY_PATH,
    root_path: Annotated[
        Path,
        typer.Option("--root-path", help="扫描既有 artifacts 的根目录。"),
    ] = PROJECT_ROOT,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="data quality governance report 输出目录。"),
    ] = DEFAULT_ETF_DATA_QUALITY_REPORT_DIR,
    json_path: Annotated[
        Path | None,
        typer.Option("--json-path", help="显式 JSON 输出路径。"),
    ] = None,
    markdown_path: Annotated[
        Path | None,
        typer.Option("--markdown-path", help="显式 Markdown 输出路径。"),
    ] = None,
) -> None:
    """生成 TRADING-075 data quality governance report；只读扫描现有缓存和 artifacts。"""
    run_date = _resolve_date(as_of, prices_path=prices_path)
    payload = build_data_quality_report(
        as_of=run_date,
        prices_path=prices_path,
        policy_config_path=config_path,
        report_registry_path=report_registry_path,
        root_path=root_path,
    )
    json_output = json_path or output_dir / f"data_quality_report_{run_date.isoformat()}.json"
    markdown_output = markdown_path or output_dir / f"data_quality_report_{run_date.isoformat()}.md"
    paths = write_data_quality_report(
        payload,
        json_path=json_output,
        markdown_path=markdown_output,
    )
    typer.echo(f"ETF data quality governance JSON：{paths['json']}")
    typer.echo(f"ETF data quality governance Markdown：{paths['markdown']}")
    typer.echo(f"report_id={payload['report_id']}")
    typer.echo(f"as_of_date={payload['as_of_date']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"blocking_failure_count={len(payload['blocking_failures'])}")
    typer.echo(f"warning_count={len(payload['warnings'])}")
    typer.echo("commands_executed=false")
    typer.echo("production_state_mutated=false")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")
    if payload["status"] == "BLOCKED":
        raise typer.Exit(code=1)


@data_quality_app.command("validate")
def data_quality_validate_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="评估日期 YYYY-MM-DD；省略时使用今天。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="data quality policy config path。"),
    ] = DEFAULT_ETF_DATA_QUALITY_POLICY_CONFIG_PATH,
    report_registry_path: Annotated[
        Path,
        typer.Option("--report-registry-path", help="report registry YAML 路径。"),
    ] = DEFAULT_REPORT_REGISTRY_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="data quality validation 输出目录。"),
    ] = DEFAULT_ETF_DATA_QUALITY_VALIDATION_DIR,
    json_path: Annotated[
        Path | None,
        typer.Option("--json-path", help="显式 JSON 输出路径。"),
    ] = None,
    markdown_path: Annotated[
        Path | None,
        typer.Option("--markdown-path", help="显式 Markdown 输出路径。"),
    ] = None,
) -> None:
    """校验 TRADING-075 workflow 完整性和安全边界；失败时 exit 1。"""
    requested_as_of = as_of or date.today().isoformat()
    payload = build_data_quality_validation_report(
        as_of=requested_as_of,
        policy_config_path=config_path,
        report_registry_path=report_registry_path,
    )
    run_date = _parse_date(requested_as_of)
    json_output = json_path or output_dir / f"data_quality_validation_{run_date.isoformat()}.json"
    markdown_output = (
        markdown_path or output_dir / f"data_quality_validation_{run_date.isoformat()}.md"
    )
    paths = write_data_quality_validation_report(
        payload,
        json_path=json_output,
        markdown_path=markdown_output,
    )
    typer.echo(f"ETF data quality validation JSON：{paths['json']}")
    typer.echo(f"ETF data quality validation Markdown：{paths['markdown']}")
    typer.echo(f"report_id={payload['report_id']}")
    typer.echo(f"as_of_date={payload['as_of_date']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo(f"warning_check_count={payload['warning_check_count']}")
    typer.echo("commands_executed=false")
    typer.echo("production_state_mutated=false")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@data_app.command("ingest")
def data_ingest_command(
    symbols: Annotated[list[str] | None, typer.Option("--symbols", help="ETF symbols。")] = None,
    start: Annotated[str, typer.Option(help="开始日期 YYYY-MM-DD。")] = "2015-01-01",
    end: Annotated[str | None, typer.Option(help="结束日期 YYYY-MM-DD，默认今天。")] = None,
    output_path: Annotated[Path, typer.Option(help="输出价格缓存路径。")] = (
        PROJECT_ROOT / "data" / "etf_portfolio" / "prices.csv"
    ),
) -> None:
    """从 yfinance 下载 ETF P0 价格并写入标准缓存。"""
    config = load_etf_config_bundle()
    selected_symbols = symbols or list(config.assets.tradeable_symbols)
    output = ingest_yfinance_prices(
        symbols=selected_symbols,
        start=_parse_date(start),
        end=_parse_date(end) if end else date.today(),
        output_path=output_path,
    )
    typer.echo(f"ETF 价格缓存已写入：{output}")


@data_app.command("validate")
def data_validate_command(
    prices_path: Annotated[Path, typer.Option(help="ETF 标准价格 CSV/Parquet 路径。")] = (
        DEFAULT_ETF_PRICE_PATH
    ),
    as_of: Annotated[str | None, typer.Option("--date", "--as-of", help="评估日期。")] = None,
    output_path: Annotated[Path | None, typer.Option(help="质量报告输出路径。")] = None,
) -> None:
    """校验 ETF price cache。"""
    config = load_etf_config_bundle()
    run_date = _resolve_date(as_of, prices_path=prices_path)
    raw = read_price_frame(prices_path)
    prices, metadata_issues = standardize_price_frame(
        raw,
        assets=config.assets,
        source_name=str(prices_path),
    )
    report = validate_price_data(
        prices,
        assets=config.assets,
        strategy=config.strategy,
        as_of=run_date,
        extra_issues=metadata_issues,
    )
    report_path = output_path or DEFAULT_ETF_REPORT_DIR / f"data_quality_{run_date.isoformat()}.md"
    write_quality_report(report, report_path)
    typer.echo(f"ETF 数据质量状态：{report.status}")
    typer.echo(f"报告：{report_path}")
    if not report.passed:
        raise typer.Exit(code=1)


@features_app.command("build")
def features_build_command(
    prices_path: Annotated[Path, typer.Option(help="ETF 标准价格 CSV/Parquet 路径。")] = (
        DEFAULT_ETF_PRICE_PATH
    ),
    output_path: Annotated[Path, typer.Option(help="Feature store 输出路径。")] = (
        DEFAULT_ETF_FEATURE_PATH
    ),
    start: Annotated[str | None, typer.Option(help="特征构建开始日期。")] = None,
    end: Annotated[str | None, typer.Option(help="特征构建结束日期。")] = None,
    include_satellites: Annotated[
        bool,
        typer.Option("--include-satellites", help="包含 P1 satellite stock optional features。"),
    ] = False,
) -> None:
    """构建 ETF feature store；先执行同一数据质量门禁。"""
    config = load_etf_config_bundle()
    prices, report = load_standard_prices(
        prices_path,
        config.assets,
        config.strategy,
        extra_symbols=_satellite_symbols(config) if include_satellites else None,
    )
    if not report.passed:
        typer.echo(f"ETF 数据质量状态：{report.status}，已停止 feature build。")
        raise typer.Exit(code=1)
    features = build_feature_store(
        prices,
        assets=config.assets,
        strategy=config.strategy,
        start=_parse_date(start) if start else None,
        end=_resolve_date(end, prices=prices) if end else None,
    )
    write_feature_store(features, output_path)
    typer.echo(f"ETF feature store 已写入：{output_path}（{len(features)} 行）")
    typer.echo(f"data_quality_status={report.status}")


@relative_strength_app.command("report")
def relative_strength_report_command(
    features_path: Annotated[Path, typer.Option(help="Feature store 路径。")] = (
        DEFAULT_ETF_FEATURE_PATH
    ),
    prices_path: Annotated[Path, typer.Option(help="价格缓存路径，用于质量门禁。")] = (
        DEFAULT_ETF_PRICE_PATH
    ),
    date_option: Annotated[str | None, typer.Option("--date", help="日期或 latest。")] = None,
    output_dir: Annotated[Path, typer.Option(help="输出目录。")] = DEFAULT_ETF_REPORT_DIR / "p1",
) -> None:
    """生成 P1 relative strength table/report。"""
    config = load_etf_config_bundle()
    quality_metadata = _p1_quality_metadata(prices_path, config, include_satellites=True)
    features = load_feature_store(features_path)
    run_date = _resolve_feature_date(date_option, features)
    table = build_relative_strength_table(features, config=config, run_date=run_date)
    csv_path = output_dir / f"{run_date.isoformat()}_relative_strength.csv"
    md_path = output_dir / f"{run_date.isoformat()}_relative_strength.md"
    write_frame_and_report(
        table,
        csv_path,
        md_path,
        "ETF Relative Strength Report",
        metadata=quality_metadata,
    )
    typer.echo(f"ETF relative strength report：{md_path}")


@confirmation_app.command("report")
def confirmation_report_command(
    features_path: Annotated[Path, typer.Option(help="Feature store 路径。")] = (
        DEFAULT_ETF_FEATURE_PATH
    ),
    prices_path: Annotated[Path, typer.Option(help="价格缓存路径，用于质量门禁。")] = (
        DEFAULT_ETF_PRICE_PATH
    ),
    date_option: Annotated[str | None, typer.Option("--date", help="日期或 latest。")] = None,
    output_dir: Annotated[Path, typer.Option(help="输出目录。")] = DEFAULT_ETF_REPORT_DIR / "p1",
) -> None:
    """生成 P1 AI / semiconductor confirmation scores。"""
    config = load_etf_config_bundle()
    if config.p1 is None:
        raise typer.BadParameter("缺少 ETF P1 config")
    quality_metadata = _p1_quality_metadata(prices_path, config, include_satellites=True)
    features = load_feature_store(features_path)
    run_date = _resolve_feature_date(date_option, features)
    rs_table = build_relative_strength_table(features, config=config, run_date=run_date)
    scores = build_confirmation_scores(rs_table, p1_config=config.p1, run_date=run_date)
    csv_path = output_dir / f"{run_date.isoformat()}_confirmation_scores.csv"
    md_path = output_dir / f"{run_date.isoformat()}_confirmation_scores.md"
    write_frame_and_report(
        scores,
        csv_path,
        md_path,
        "ETF Confirmation Scores",
        metadata=quality_metadata,
    )
    typer.echo(f"ETF confirmation scores：{md_path}")


@ai_confirmation_app.command("features")
def ai_confirmation_features_command(
    prices_path: Annotated[Path, typer.Option(help="ETF / AI 标准价格 CSV/Parquet 路径。")] = (
        DEFAULT_ETF_PRICE_PATH
    ),
    date_option: Annotated[str | None, typer.Option("--date", help="日期或 latest。")] = None,
    output_dir: Annotated[Path, typer.Option(help="输出目录。")] = (
        DEFAULT_AI_CONFIRMATION_FEATURE_DIR
    ),
    universe_path: Annotated[Path, typer.Option(help="AI confirmation universe config。")] = (
        DEFAULT_AI_CONFIRMATION_UNIVERSE_CONFIG_PATH
    ),
) -> None:
    """生成 TRADING-066B AI / semiconductor breadth features。"""
    config = load_etf_config_bundle()
    ai_config = load_ai_confirmation_universe_config(universe_path)
    extra_symbols = set(all_enabled_price_tickers(ai_config)) - set(config.assets.assets)
    prices, quality_report = load_standard_prices(
        prices_path,
        config.assets,
        config.strategy,
        extra_symbols=extra_symbols,
    )
    if not quality_report.passed:
        typer.echo(f"ETF 数据质量状态：{quality_report.status}，已停止 AI confirmation features。")
        raise typer.Exit(code=1)
    run_date = _resolve_date(date_option, prices=prices)
    availability = validate_ai_confirmation_data_availability(
        ai_config,
        _available_price_symbols(prices, run_date),
        group_ids=ai_confirmation_price_group_ids(ai_config),
    )
    if availability["status"] == "FAIL":
        typer.echo("AI confirmation 数据覆盖状态：FAIL，已停止 feature build。")
        for error in availability["errors"]:
            typer.echo(f"- {error}")
        raise typer.Exit(code=1)
    report_metadata = _quality_metadata(quality_report)
    records = build_ai_confirmation_breadth_features(
        prices,
        config=ai_config,
        run_date=run_date,
    )
    paths = write_ai_confirmation_breadth_features(
        records,
        output_dir=output_dir,
        run_date=run_date,
        data_quality_status=quality_report.status,
        data_quality_report=str(report_metadata["data_quality_report"]).strip("`"),
    )
    typer.echo(f"AI confirmation features JSON：{paths['json']}")
    typer.echo(f"AI confirmation features CSV：{paths['csv']}")
    typer.echo(f"data_quality_status={quality_report.status}")
    typer.echo(f"ai_data_availability_status={availability['status']}")


@ai_confirmation_app.command("report")
def ai_confirmation_report_command(
    prices_path: Annotated[Path, typer.Option(help="ETF / AI 标准价格 CSV/Parquet 路径。")] = (
        DEFAULT_ETF_PRICE_PATH
    ),
    date_option: Annotated[str | None, typer.Option("--date", help="日期或 latest。")] = None,
    output_dir: Annotated[Path, typer.Option(help="AI confirmation report 输出目录。")] = (
        DEFAULT_AI_CONFIRMATION_STANDALONE_REPORT_DIR
    ),
    universe_path: Annotated[Path, typer.Option(help="AI confirmation universe config。")] = (
        DEFAULT_AI_CONFIRMATION_UNIVERSE_CONFIG_PATH
    ),
    policy_path: Annotated[Path, typer.Option(help="AI confirmation scoring policy config。")] = (
        DEFAULT_AI_CONFIRMATION_POLICY_CONFIG_PATH
    ),
    events_path: Annotated[
        Path | None,
        typer.Option(help="可选 AI event calendar JSON/CSV。缺失时按 no active events 处理。"),
    ] = None,
) -> None:
    """生成 TRADING-066G standalone AI confirmation JSON/Markdown report。"""
    config = load_etf_config_bundle()
    ai_config = load_ai_confirmation_universe_config(universe_path)
    policy_config = load_ai_confirmation_policy_config(policy_path)
    extra_symbols = set(all_enabled_price_tickers(ai_config)) - set(config.assets.assets)
    prices, quality_report = load_standard_prices(
        prices_path,
        config.assets,
        config.strategy,
        extra_symbols=extra_symbols,
    )
    if not quality_report.passed:
        typer.echo(f"ETF 数据质量状态：{quality_report.status}，已停止 AI confirmation report。")
        raise typer.Exit(code=1)
    run_date = _resolve_date(date_option, prices=prices)
    availability = validate_ai_confirmation_data_availability(
        ai_config,
        _available_price_symbols(prices, run_date),
        group_ids=ai_confirmation_price_group_ids(ai_config),
    )
    if availability["status"] == "FAIL":
        typer.echo("AI confirmation 数据覆盖状态：FAIL，已停止 report build。")
        for error in availability["errors"]:
            typer.echo(f"- {error}")
        raise typer.Exit(code=1)
    report_metadata = _quality_metadata(quality_report)
    payload = build_ai_confirmation_report(
        prices=prices,
        events=load_ai_confirmation_events(events_path),
        universe_config=ai_config,
        policy_config=policy_config,
        run_date=run_date,
        data_quality_status=quality_report.status,
        data_quality_report=str(report_metadata["data_quality_report"]).strip("`"),
        market_regime=config.backtest.backtest.regime,
        requested_date_range=_price_requested_date_range(prices, run_date),
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    stem = f"ai_confirmation_report_{run_date.isoformat()}"
    json_path = output_dir / f"{stem}.json"
    markdown_path = output_dir / f"{stem}.md"
    write_ai_confirmation_report(payload, json_path=json_path, markdown_path=markdown_path)
    typer.echo(f"AI confirmation report JSON：{json_path}")
    typer.echo(f"AI confirmation report Markdown：{markdown_path}")
    typer.echo(f"AIConfirmationScore={payload['AIConfirmationScore']['score_value']}")
    typer.echo(f"score_band={payload['AIConfirmationScore']['score_band']}")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")


@ai_confirmation_app.command("overlay")
def ai_confirmation_overlay_command(
    candidate_id: Annotated[
        str,
        typer.Option("--candidate", help="Base candidate id for the shadow overlay output."),
    ],
    base_weights_path: Annotated[
        Path,
        typer.Option(help="JSON/YAML/CSV base candidate weights; read-only input."),
    ],
    date_option: Annotated[str | None, typer.Option("--date", help="日期或 latest。")] = None,
    ai_confirmation_report_path: Annotated[
        Path | None,
        typer.Option(help="AI confirmation report JSON；缺省读取 latest report。"),
    ] = None,
    report_dir: Annotated[Path, typer.Option(help="AI confirmation report 查找目录。")] = (
        DEFAULT_AI_CONFIRMATION_STANDALONE_REPORT_DIR
    ),
    output_dir: Annotated[Path, typer.Option(help="shadow overlay 输出目录。")] = (
        DEFAULT_AI_CONFIRMATION_OVERLAY_DIR
    ),
    policy_path: Annotated[Path, typer.Option(help="AI confirmation scoring policy config。")] = (
        DEFAULT_AI_CONFIRMATION_POLICY_CONFIG_PATH
    ),
) -> None:
    """生成 TRADING-066H candidate-only shadow overlay；不写 production weights。"""
    run_date = _parse_date(date_option) if date_option and date_option != "latest" else date.today()
    report_path = ai_confirmation_report_path or latest_ai_confirmation_report_path(
        report_dir,
        as_of=run_date if date_option else None,
    )
    if report_path is None:
        typer.echo("AI confirmation report not found; run report before overlay.")
        raise typer.Exit(code=1)
    report_payload = json.loads(report_path.read_text(encoding="utf-8"))
    if isinstance(report_payload, dict) and report_payload.get("date"):
        run_date = date.fromisoformat(str(report_payload["date"]))
    overlay = build_ai_confirmation_shadow_overlay_experiment(
        base_weights=load_ai_confirmation_base_weights(base_weights_path),
        ai_confirmation_payload=report_payload,
        policy_config=load_ai_confirmation_policy_config(policy_path),
        run_date=run_date,
        base_candidate_id=candidate_id,
    )
    stem = f"ai_confirmation_overlay_{run_date.isoformat()}_{candidate_id}"
    json_path = output_dir / f"{stem}.json"
    markdown_path = output_dir / f"{stem}.md"
    write_ai_confirmation_shadow_overlay(overlay, json_path=json_path, markdown_path=markdown_path)
    typer.echo(f"AI confirmation shadow overlay JSON：{json_path}")
    typer.echo(f"AI confirmation shadow overlay Markdown：{markdown_path}")
    typer.echo(f"AIConfirmationScore={overlay['AIConfirmationScore']}")
    typer.echo(f"overlay_direction={overlay['overlay_adjustment']['direction']}")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")


@ai_confirmation_app.command("validate")
def ai_confirmation_validate_command(
    output_dir: Annotated[Path, typer.Option(help="validation 输出目录。")] = (
        DEFAULT_AI_CONFIRMATION_VALIDATION_DIR
    ),
    universe_path: Annotated[Path, typer.Option(help="AI confirmation universe config。")] = (
        DEFAULT_AI_CONFIRMATION_UNIVERSE_CONFIG_PATH
    ),
    policy_path: Annotated[Path, typer.Option(help="AI confirmation scoring policy config。")] = (
        DEFAULT_AI_CONFIRMATION_POLICY_CONFIG_PATH
    ),
    report_registry_path: Annotated[Path, typer.Option(help="report registry config。")] = (
        DEFAULT_REPORT_REGISTRY_PATH
    ),
) -> None:
    """生成 TRADING-066J final AI confirmation validation gate。"""
    from ai_trading_system.reports.reader_brief import build_reader_brief_payload

    generated = datetime.now(tz=UTC)
    payload = build_ai_confirmation_validation_report(
        universe_config=load_ai_confirmation_universe_config(universe_path),
        policy_config=load_ai_confirmation_policy_config(policy_path),
        report_registry=load_report_registry(report_registry_path),
        reader_brief_available=callable(build_reader_brief_payload),
        generated_at=generated.isoformat(),
    )
    stem = f"ai_confirmation_validation_{generated.date().isoformat()}"
    json_path = output_dir / f"{stem}.json"
    markdown_path = output_dir / f"{stem}.md"
    write_ai_confirmation_validation_report(
        payload,
        json_path=json_path,
        markdown_path=markdown_path,
    )
    typer.echo(f"AI confirmation validation JSON：{json_path}")
    typer.echo(f"AI confirmation validation Markdown：{markdown_path}")
    typer.echo(f"status={payload['status']}")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@ai_attribution_app.command("build")
def ai_attribution_build_command(
    prices_path: Annotated[Path, typer.Option(help="ETF 标准价格 CSV/Parquet 路径。")] = (
        DEFAULT_ETF_PRICE_PATH
    ),
    as_of: Annotated[str | None, typer.Option("--as-of", help="评估日期或 latest。")] = None,
    start: Annotated[
        str,
        typer.Option(help="attribution 起始日期，默认 AI regime start。"),
    ] = "2022-12-01",
    ai_confirmation_report_dir: Annotated[
        Path,
        typer.Option(help="既有 AI confirmation report JSON 目录。"),
    ] = DEFAULT_AI_CONFIRMATION_STANDALONE_REPORT_DIR,
    output_dir: Annotated[
        Path,
        typer.Option(help="AI attribution dataset 输出目录。"),
    ] = DEFAULT_AI_ATTRIBUTION_DATASET_DIR,
) -> None:
    """生成 TRADING-072A AI confirmation forward attribution dataset。"""
    config = load_etf_config_bundle()
    prices, quality_report = load_standard_prices(prices_path, config.assets, config.strategy)
    if not quality_report.passed:
        typer.echo(f"ETF 数据质量状态：{quality_report.status}，已停止 AI attribution build。")
        raise typer.Exit(code=1)
    run_date = _resolve_date(as_of, prices=prices)
    start_date = _parse_date(start)
    report_metadata = _quality_metadata(quality_report)
    reports = load_ai_confirmation_report_payloads(
        ai_confirmation_report_dir,
        as_of=run_date,
        start=start_date,
    )
    payload = build_ai_attribution_dataset(
        ai_confirmation_reports=reports,
        prices=prices,
        evaluation_as_of_date=run_date,
        start=start_date,
        data_quality_status=quality_report.status,
        data_quality_report=str(report_metadata["data_quality_report"]).strip("`"),
        market_regime=config.backtest.backtest.regime,
        requested_date_range={"start": start_date.isoformat(), "end": run_date.isoformat()},
    )
    paths = write_ai_attribution_dataset(payload, output_dir=output_dir)
    typer.echo(f"AI attribution dataset JSON：{paths['json']}")
    typer.echo(f"AI attribution dataset CSV：{paths['csv']}")
    typer.echo(f"record_count={payload['record_count']}")
    typer.echo(f"available_sample_count={payload['available_sample_count']}")
    typer.echo(f"data_quality_status={quality_report.status}")
    typer.echo("evaluation_only=true")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@ai_attribution_app.command("report")
def ai_attribution_report_command(
    prices_path: Annotated[Path, typer.Option(help="ETF 标准价格 CSV/Parquet 路径。")] = (
        DEFAULT_ETF_PRICE_PATH
    ),
    as_of: Annotated[str | None, typer.Option("--as-of", help="评估日期或 latest。")] = None,
    start: Annotated[
        str,
        typer.Option(help="attribution 起始日期，默认 AI regime start。"),
    ] = "2022-12-01",
    ai_confirmation_report_dir: Annotated[
        Path,
        typer.Option(help="既有 AI confirmation report JSON 目录。"),
    ] = DEFAULT_AI_CONFIRMATION_STANDALONE_REPORT_DIR,
    dataset_output_dir: Annotated[
        Path,
        typer.Option(help="同步写出的 AI attribution dataset 目录。"),
    ] = DEFAULT_AI_ATTRIBUTION_DATASET_DIR,
    output_dir: Annotated[
        Path,
        typer.Option(help="AI attribution report 输出目录。"),
    ] = DEFAULT_AI_ATTRIBUTION_REVIEW_DIR,
) -> None:
    """生成 TRADING-072H AI confirmation forward attribution report。"""
    config = load_etf_config_bundle()
    prices, quality_report = load_standard_prices(prices_path, config.assets, config.strategy)
    if not quality_report.passed:
        typer.echo(f"ETF 数据质量状态：{quality_report.status}，已停止 AI attribution report。")
        raise typer.Exit(code=1)
    run_date = _resolve_date(as_of, prices=prices)
    start_date = _parse_date(start)
    report_metadata = _quality_metadata(quality_report)
    reports = load_ai_confirmation_report_payloads(
        ai_confirmation_report_dir,
        as_of=run_date,
        start=start_date,
    )
    dataset = build_ai_attribution_dataset(
        ai_confirmation_reports=reports,
        prices=prices,
        evaluation_as_of_date=run_date,
        start=start_date,
        data_quality_status=quality_report.status,
        data_quality_report=str(report_metadata["data_quality_report"]).strip("`"),
        market_regime=config.backtest.backtest.regime,
        requested_date_range={"start": start_date.isoformat(), "end": run_date.isoformat()},
    )
    dataset_paths = write_ai_attribution_dataset(dataset, output_dir=dataset_output_dir)
    payload = build_ai_attribution_report(dataset)
    paths = write_ai_attribution_report(payload, output_dir=output_dir)
    typer.echo(f"AI attribution report JSON：{paths['json']}")
    typer.echo(f"AI attribution report Markdown：{paths['markdown']}")
    typer.echo(f"AI attribution dataset JSON：{dataset_paths['json']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"available_sample_count={dataset['available_sample_count']}")
    typer.echo("evaluation_only=true")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@ai_attribution_app.command("validate")
def ai_attribution_validate_command(
    output_dir: Annotated[
        Path,
        typer.Option(help="AI attribution validation 输出目录。"),
    ] = DEFAULT_AI_ATTRIBUTION_VALIDATION_DIR,
    report_registry_path: Annotated[
        Path,
        typer.Option(help="report registry YAML path。"),
    ] = DEFAULT_REPORT_REGISTRY_PATH,
    report_path: Annotated[
        Path | None,
        typer.Option(help="optional TRADING-072H attribution report JSON path。"),
    ] = None,
) -> None:
    """执行 TRADING-072J AI attribution validation gate。"""
    from ai_trading_system.reports.reader_brief import build_reader_brief_payload

    payload = build_ai_attribution_validation_report(
        report_registry=load_report_registry(report_registry_path),
        reader_brief_available=callable(build_reader_brief_payload),
        report_payload=(
            _load_optional_json_payload(report_path) if report_path is not None else None
        ),
    )
    paths = write_ai_attribution_validation_report(payload, output_dir=output_dir)
    typer.echo(f"AI attribution validation gate：{paths['markdown']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("evaluation_only=true")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@satellite_app.command("evaluate")
def satellite_evaluate_command(
    features_path: Annotated[Path, typer.Option(help="Feature store 路径。")] = (
        DEFAULT_ETF_FEATURE_PATH
    ),
    signals_path: Annotated[Path, typer.Option(help="Signals 路径。")] = DEFAULT_ETF_SIGNAL_PATH,
    regime_path: Annotated[Path, typer.Option(help="Regime 路径。")] = DEFAULT_ETF_REGIME_PATH,
    prices_path: Annotated[Path, typer.Option(help="价格缓存路径，用于质量门禁。")] = (
        DEFAULT_ETF_PRICE_PATH
    ),
    date_option: Annotated[str | None, typer.Option("--date", help="日期或 latest。")] = None,
    output_dir: Annotated[Path, typer.Option(help="输出目录。")] = DEFAULT_ETF_REPORT_DIR / "p1",
) -> None:
    """评估 P1 satellite stock candidates；observe-only，不改 ETF 目标权重。"""
    config = load_etf_config_bundle()
    if config.p1 is None:
        raise typer.BadParameter("缺少 ETF P1 config")
    quality_metadata = _p1_quality_metadata(prices_path, config, include_satellites=True)
    features = load_feature_store(features_path)
    signals = load_signals(signals_path)
    regimes = load_regimes(regime_path)
    run_date = _resolve_feature_date(date_option, features)
    regime = select_regime_for_date(regimes, run_date)
    candidates = evaluate_satellite_candidates(
        features,
        signals,
        config=config,
        p1_config=config.p1,
        run_date=run_date,
        regime=str(regime["regime"]),
    )
    csv_path = output_dir / f"{run_date.isoformat()}_satellite_candidates.csv"
    md_path = output_dir / f"{run_date.isoformat()}_satellite_candidates.md"
    write_frame_and_report(
        candidates,
        csv_path,
        md_path,
        "ETF Satellite Candidate Report",
        metadata=quality_metadata,
    )
    typer.echo(f"ETF satellite candidates：{md_path}")


@satellite_app.command("features")
def satellite_features_command(
    prices_path: Annotated[
        Path,
        typer.Option(help="ETF / satellite 标准价格 CSV/Parquet 路径。"),
    ] = DEFAULT_ETF_PRICE_PATH,
    date_option: Annotated[str | None, typer.Option("--date", help="日期或 latest。")] = None,
    output_dir: Annotated[Path, typer.Option(help="satellite features 输出目录。")] = (
        DEFAULT_SATELLITE_FEATURE_DIR
    ),
    universe_path: Annotated[Path, typer.Option(help="satellite universe config。")] = (
        DEFAULT_SATELLITE_UNIVERSE_CONFIG_PATH
    ),
) -> None:
    """生成 TRADING-067C stock-vs-ETF relative strength features。"""
    config = load_etf_config_bundle()
    satellite_config = load_satellite_universe_config(universe_path)
    extra_symbols = set(satellite_price_symbols(satellite_config)) - set(config.assets.assets)
    prices, quality_report = load_standard_prices(
        prices_path,
        config.assets,
        config.strategy,
        extra_symbols=extra_symbols,
    )
    if not quality_report.passed:
        typer.echo(f"ETF 数据质量状态：{quality_report.status}，已停止 satellite features。")
        raise typer.Exit(code=1)
    run_date = _resolve_date(date_option, prices=prices)
    availability = validate_satellite_data_availability(
        satellite_config,
        _available_price_symbols(prices, run_date),
    )
    if availability["status"] == "FAIL":
        typer.echo("Satellite 数据覆盖状态：FAIL，已停止 feature build。")
        for error in availability["errors"]:
            typer.echo(f"- {error}")
        raise typer.Exit(code=1)
    report_metadata = _quality_metadata(quality_report)
    records = build_satellite_relative_strength_features(
        prices,
        universe_config=satellite_config,
        run_date=run_date,
    )
    paths = write_satellite_features(
        records,
        output_dir=output_dir,
        run_date=run_date,
        data_quality_status=quality_report.status,
        data_quality_report=str(report_metadata["data_quality_report"]).strip("`"),
    )
    typer.echo(f"Satellite features JSON：{paths['json']}")
    typer.echo(f"Satellite features CSV：{paths['csv']}")
    typer.echo(f"data_quality_status={quality_report.status}")
    typer.echo(f"satellite_data_availability_status={availability['status']}")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")


@satellite_app.command("report")
def satellite_report_command(
    prices_path: Annotated[
        Path,
        typer.Option(help="ETF / satellite 标准价格 CSV/Parquet 路径。"),
    ] = DEFAULT_ETF_PRICE_PATH,
    date_option: Annotated[str | None, typer.Option("--date", help="日期或 latest。")] = None,
    output_dir: Annotated[Path, typer.Option(help="satellite report 输出目录。")] = (
        DEFAULT_SATELLITE_STANDALONE_REPORT_DIR
    ),
    universe_path: Annotated[Path, typer.Option(help="satellite universe config。")] = (
        DEFAULT_SATELLITE_UNIVERSE_CONFIG_PATH
    ),
    policy_path: Annotated[Path, typer.Option(help="satellite replacement policy config。")] = (
        DEFAULT_SATELLITE_POLICY_CONFIG_PATH
    ),
    ai_confirmation_report_path: Annotated[
        Path | None,
        typer.Option(help="可选 AI confirmation report JSON。缺失时按 neutral context 处理。"),
    ] = None,
) -> None:
    """生成 TRADING-067I satellite replacement JSON/Markdown report。"""
    payload, run_date = _build_satellite_report_payload(
        prices_path=prices_path,
        date_option=date_option,
        universe_path=universe_path,
        policy_path=policy_path,
        ai_confirmation_report_path=ai_confirmation_report_path,
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    stem = f"satellite_replacement_report_{run_date.isoformat()}"
    json_path = output_dir / f"{stem}.json"
    markdown_path = output_dir / f"{stem}.md"
    write_satellite_replacement_report(payload, json_path=json_path, markdown_path=markdown_path)
    typer.echo(f"Satellite replacement report JSON：{json_path}")
    typer.echo(f"Satellite replacement report Markdown：{markdown_path}")
    typer.echo(f"eligible_stocks={','.join(str(item) for item in payload['eligible_stocks'])}")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@satellite_app.command("run")
def satellite_run_command(
    prices_path: Annotated[
        Path,
        typer.Option(help="ETF / satellite 标准价格 CSV/Parquet 路径。"),
    ] = DEFAULT_ETF_PRICE_PATH,
    date_option: Annotated[str | None, typer.Option("--date", help="日期或 latest。")] = None,
    output_dir: Annotated[Path, typer.Option(help="satellite report 输出目录。")] = (
        DEFAULT_SATELLITE_STANDALONE_REPORT_DIR
    ),
    universe_path: Annotated[Path, typer.Option(help="satellite universe config。")] = (
        DEFAULT_SATELLITE_UNIVERSE_CONFIG_PATH
    ),
    policy_path: Annotated[Path, typer.Option(help="satellite replacement policy config。")] = (
        DEFAULT_SATELLITE_POLICY_CONFIG_PATH
    ),
    ai_confirmation_report_path: Annotated[
        Path | None,
        typer.Option(help="可选 AI confirmation report JSON。"),
    ] = None,
) -> None:
    """执行 satellite replacement report alias；不写 official ETF target weights。"""
    satellite_report_command(
        prices_path=prices_path,
        date_option=date_option,
        output_dir=output_dir,
        universe_path=universe_path,
        policy_path=policy_path,
        ai_confirmation_report_path=ai_confirmation_report_path,
    )


@satellite_app.command("experiment")
def satellite_experiment_command(
    report_path: Annotated[
        Path | None,
        typer.Option(help="satellite replacement report JSON；缺省读取 latest report。"),
    ] = None,
    date_option: Annotated[str | None, typer.Option("--date", help="日期或 latest。")] = None,
    report_dir: Annotated[Path, typer.Option(help="satellite report 查找目录。")] = (
        DEFAULT_SATELLITE_STANDALONE_REPORT_DIR
    ),
    output_dir: Annotated[Path, typer.Option(help="satellite experiment 输出目录。")] = (
        DEFAULT_SATELLITE_EXPERIMENT_DIR
    ),
    universe_path: Annotated[Path, typer.Option(help="satellite universe config。")] = (
        DEFAULT_SATELLITE_UNIVERSE_CONFIG_PATH
    ),
) -> None:
    """生成 TRADING-067G satellite shadow portfolio experiment。"""
    run_date = _parse_date(date_option) if date_option and date_option != "latest" else date.today()
    selected_report_path = report_path or latest_satellite_report_path(
        report_dir,
        as_of=run_date if date_option and date_option != "latest" else None,
    )
    if selected_report_path is None:
        typer.echo("Satellite replacement report not found; run report before experiment.")
        raise typer.Exit(code=1)
    report_payload = json.loads(selected_report_path.read_text(encoding="utf-8"))
    if report_payload.get("date"):
        run_date = date.fromisoformat(str(report_payload["date"]))
    experiment = build_satellite_shadow_portfolio_experiment(
        run_date=run_date,
        replacement_plan=report_payload["replacement_plan"],
        universe_config=load_satellite_universe_config(universe_path),
        base_candidate_id="satellite_replacement_v1",
    )
    stem = f"satellite_shadow_experiment_{run_date.isoformat()}"
    json_path = output_dir / f"{stem}.json"
    markdown_path = output_dir / f"{stem}.md"
    write_satellite_shadow_experiment(
        experiment,
        json_path=json_path,
        markdown_path=markdown_path,
    )
    typer.echo(f"Satellite shadow experiment JSON：{json_path}")
    typer.echo(f"Satellite shadow experiment Markdown：{markdown_path}")
    typer.echo(f"status={experiment['status']}")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")


@satellite_app.command("validate")
def satellite_validate_command(
    output_dir: Annotated[Path, typer.Option(help="validation 输出目录。")] = (
        DEFAULT_SATELLITE_VALIDATION_DIR
    ),
    universe_path: Annotated[Path, typer.Option(help="satellite universe config。")] = (
        DEFAULT_SATELLITE_UNIVERSE_CONFIG_PATH
    ),
    policy_path: Annotated[Path, typer.Option(help="satellite replacement policy config。")] = (
        DEFAULT_SATELLITE_POLICY_CONFIG_PATH
    ),
    report_registry_path: Annotated[Path, typer.Option(help="report registry config。")] = (
        DEFAULT_REPORT_REGISTRY_PATH
    ),
) -> None:
    """生成 TRADING-067K final satellite replacement validation gate。"""
    from ai_trading_system.reports.reader_brief import build_reader_brief_payload

    generated = datetime.now(tz=UTC)
    payload = build_satellite_policy_validation_report(
        universe_config=load_satellite_universe_config(universe_path),
        policy_config=load_satellite_policy_config(policy_path),
        report_registry=load_report_registry(report_registry_path),
        reader_brief_available=callable(build_reader_brief_payload),
        generated_at=generated.isoformat(),
    )
    stem = f"satellite_validation_{generated.date().isoformat()}"
    json_path = output_dir / f"{stem}.json"
    markdown_path = output_dir / f"{stem}.md"
    write_satellite_policy_validation_report(
        payload,
        json_path=json_path,
        markdown_path=markdown_path,
    )
    typer.echo(f"Satellite validation JSON：{json_path}")
    typer.echo(f"Satellite validation Markdown：{markdown_path}")
    typer.echo(f"status={payload['status']}")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@satellite_attribution_app.command("build")
def satellite_attribution_build_command(
    prices_path: Annotated[
        Path,
        typer.Option(help="ETF / satellite 标准价格 CSV/Parquet 路径。"),
    ] = DEFAULT_ETF_PRICE_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of", help="评估日期或 latest。")] = None,
    start: Annotated[
        str,
        typer.Option(help="attribution 起始日期，默认 AI regime start。"),
    ] = "2022-12-01",
    satellite_report_dir: Annotated[
        Path,
        typer.Option(help="既有 satellite replacement report JSON 目录。"),
    ] = DEFAULT_SATELLITE_STANDALONE_REPORT_DIR,
    ai_confirmation_report_dir: Annotated[
        Path,
        typer.Option(help="可选 AI confirmation report JSON 目录。"),
    ] = DEFAULT_AI_CONFIRMATION_STANDALONE_REPORT_DIR,
    universe_path: Annotated[Path, typer.Option(help="satellite universe config。")] = (
        DEFAULT_SATELLITE_UNIVERSE_CONFIG_PATH
    ),
    output_dir: Annotated[
        Path,
        typer.Option(help="satellite attribution dataset 输出目录。"),
    ] = DEFAULT_SATELLITE_ATTRIBUTION_DATASET_DIR,
) -> None:
    """生成 TRADING-073A satellite replacement forward attribution dataset。"""
    config = load_etf_config_bundle()
    satellite_config = load_satellite_universe_config(universe_path)
    extra_symbols = set(satellite_price_symbols(satellite_config)) - set(config.assets.assets)
    prices, quality_report = load_standard_prices(
        prices_path,
        config.assets,
        config.strategy,
        extra_symbols=extra_symbols,
    )
    if not quality_report.passed:
        typer.echo(
            f"ETF 数据质量状态：{quality_report.status}，"
            "已停止 satellite attribution build。"
        )
        raise typer.Exit(code=1)
    run_date = _resolve_date(as_of, prices=prices)
    start_date = _parse_date(start)
    report_metadata = _quality_metadata(quality_report)
    satellite_reports = load_satellite_replacement_report_payloads(
        satellite_report_dir,
        as_of=run_date,
        start=start_date,
    )
    ai_reports = load_ai_confirmation_report_payloads_for_satellite(
        ai_confirmation_report_dir,
        as_of=run_date,
        start=start_date,
    )
    payload = build_satellite_attribution_dataset(
        satellite_reports=satellite_reports,
        prices=prices,
        evaluation_as_of_date=run_date,
        universe_config=satellite_config,
        ai_confirmation_reports=ai_reports,
        start=start_date,
        data_quality_status=quality_report.status,
        data_quality_report=str(report_metadata["data_quality_report"]).strip("`"),
        market_regime=config.backtest.backtest.regime,
        requested_date_range={"start": start_date.isoformat(), "end": run_date.isoformat()},
    )
    paths = write_satellite_attribution_dataset(payload, output_dir=output_dir)
    typer.echo(f"Satellite attribution dataset JSON：{paths['json']}")
    typer.echo(f"Satellite attribution dataset CSV：{paths['csv']}")
    typer.echo(f"record_count={payload['record_count']}")
    typer.echo(f"available_sample_count={payload['available_sample_count']}")
    typer.echo(f"data_quality_status={quality_report.status}")
    typer.echo("evaluation_only=true")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@satellite_attribution_app.command("report")
def satellite_attribution_report_command(
    prices_path: Annotated[
        Path,
        typer.Option(help="ETF / satellite 标准价格 CSV/Parquet 路径。"),
    ] = DEFAULT_ETF_PRICE_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of", help="评估日期或 latest。")] = None,
    start: Annotated[
        str,
        typer.Option(help="attribution 起始日期，默认 AI regime start。"),
    ] = "2022-12-01",
    satellite_report_dir: Annotated[
        Path,
        typer.Option(help="既有 satellite replacement report JSON 目录。"),
    ] = DEFAULT_SATELLITE_STANDALONE_REPORT_DIR,
    ai_confirmation_report_dir: Annotated[
        Path,
        typer.Option(help="可选 AI confirmation report JSON 目录。"),
    ] = DEFAULT_AI_CONFIRMATION_STANDALONE_REPORT_DIR,
    universe_path: Annotated[Path, typer.Option(help="satellite universe config。")] = (
        DEFAULT_SATELLITE_UNIVERSE_CONFIG_PATH
    ),
    dataset_output_dir: Annotated[
        Path,
        typer.Option(help="同步写出的 satellite attribution dataset 目录。"),
    ] = DEFAULT_SATELLITE_ATTRIBUTION_DATASET_DIR,
    output_dir: Annotated[
        Path,
        typer.Option(help="satellite attribution report 输出目录。"),
    ] = DEFAULT_SATELLITE_ATTRIBUTION_REVIEW_DIR,
) -> None:
    """生成 TRADING-073J satellite replacement forward attribution report。"""
    config = load_etf_config_bundle()
    satellite_config = load_satellite_universe_config(universe_path)
    extra_symbols = set(satellite_price_symbols(satellite_config)) - set(config.assets.assets)
    prices, quality_report = load_standard_prices(
        prices_path,
        config.assets,
        config.strategy,
        extra_symbols=extra_symbols,
    )
    if not quality_report.passed:
        typer.echo(
            f"ETF 数据质量状态：{quality_report.status}，"
            "已停止 satellite attribution report。"
        )
        raise typer.Exit(code=1)
    run_date = _resolve_date(as_of, prices=prices)
    start_date = _parse_date(start)
    report_metadata = _quality_metadata(quality_report)
    satellite_reports = load_satellite_replacement_report_payloads(
        satellite_report_dir,
        as_of=run_date,
        start=start_date,
    )
    ai_reports = load_ai_confirmation_report_payloads_for_satellite(
        ai_confirmation_report_dir,
        as_of=run_date,
        start=start_date,
    )
    dataset = build_satellite_attribution_dataset(
        satellite_reports=satellite_reports,
        prices=prices,
        evaluation_as_of_date=run_date,
        universe_config=satellite_config,
        ai_confirmation_reports=ai_reports,
        start=start_date,
        data_quality_status=quality_report.status,
        data_quality_report=str(report_metadata["data_quality_report"]).strip("`"),
        market_regime=config.backtest.backtest.regime,
        requested_date_range={"start": start_date.isoformat(), "end": run_date.isoformat()},
    )
    dataset_paths = write_satellite_attribution_dataset(
        dataset,
        output_dir=dataset_output_dir,
    )
    payload = build_satellite_attribution_report(dataset)
    paths = write_satellite_attribution_report(payload, output_dir=output_dir)
    typer.echo(f"Satellite attribution report JSON：{paths['json']}")
    typer.echo(f"Satellite attribution report Markdown：{paths['markdown']}")
    typer.echo(f"Satellite attribution dataset JSON：{dataset_paths['json']}")
    typer.echo(f"overall_status={payload['evidence_scorecard']['overall_status']}")
    typer.echo(f"available_sample_count={dataset['available_sample_count']}")
    typer.echo("evaluation_only=true")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@satellite_attribution_app.command("validate")
def satellite_attribution_validate_command(
    output_dir: Annotated[
        Path,
        typer.Option(help="satellite attribution validation 输出目录。"),
    ] = DEFAULT_SATELLITE_ATTRIBUTION_VALIDATION_DIR,
    report_registry_path: Annotated[
        Path,
        typer.Option(help="report registry YAML path。"),
    ] = DEFAULT_REPORT_REGISTRY_PATH,
    report_path: Annotated[
        Path | None,
        typer.Option(help="optional TRADING-073J attribution report JSON path。"),
    ] = None,
) -> None:
    """执行 TRADING-073L satellite attribution validation gate。"""
    from ai_trading_system.reports.reader_brief import build_reader_brief_payload

    payload = build_satellite_attribution_validation_report(
        report_registry=load_report_registry(report_registry_path),
        reader_brief_available=callable(build_reader_brief_payload),
        report_payload=(
            _load_optional_json_payload(report_path) if report_path is not None else None
        ),
    )
    paths = write_satellite_attribution_validation_report(payload, output_dir=output_dir)
    typer.echo(f"Satellite attribution validation gate：{paths['markdown']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("evaluation_only=true")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@attribution_app.command("report")
def attribution_report_command(
    prices_path: Annotated[Path, typer.Option(help="价格缓存路径。")] = DEFAULT_ETF_PRICE_PATH,
    allocation_path: Annotated[Path, typer.Option(help="目标权重路径。")] = DEFAULT_ETF_TARGET_PATH,
    date_option: Annotated[str | None, typer.Option("--date", help="日期或 latest。")] = None,
    output_dir: Annotated[Path, typer.Option(help="输出目录。")] = DEFAULT_ETF_REPORT_DIR / "p1",
) -> None:
    """生成 P1 portfolio attribution report。"""
    config = load_etf_config_bundle()
    prices, quality_report = load_standard_prices(prices_path, config.assets, config.strategy)
    if not quality_report.passed:
        typer.echo(f"ETF 数据质量状态：{quality_report.status}，已停止 attribution。")
        raise typer.Exit(code=1)
    quality_metadata = _quality_metadata(quality_report)
    run_date = _resolve_date(date_option, prices=prices)
    allocation = load_allocation(allocation_path)
    allocation_dates = pd.to_datetime(allocation["date"], errors="coerce")
    allocation = allocation.loc[allocation_dates == pd.Timestamp(run_date)]
    attribution = build_portfolio_attribution(allocation, prices, run_date=run_date)
    csv_path = output_dir / f"{run_date.isoformat()}_attribution.csv"
    md_path = output_dir / f"{run_date.isoformat()}_attribution.md"
    write_frame_and_report(
        attribution,
        csv_path,
        md_path,
        "ETF Portfolio Attribution",
        metadata=quality_metadata,
    )
    typer.echo(f"ETF attribution：{md_path}")


@events_app.command("risk-flag")
def event_risk_flag_command(
    date_option: Annotated[str | None, typer.Option("--date", help="日期或 latest。")] = None,
    output_dir: Annotated[Path, typer.Option(help="输出目录。")] = DEFAULT_ETF_REPORT_DIR / "p1",
) -> None:
    """生成 P1 basic event calendar risk flags。"""
    config = load_etf_config_bundle()
    if config.p1 is None:
        raise typer.BadParameter("缺少 ETF P1 config")
    run_date = _parse_date(date_option) if date_option and date_option != "latest" else date.today()
    flags = evaluate_event_risk(p1_config=config.p1, run_date=run_date)
    csv_path = output_dir / f"{run_date.isoformat()}_event_risk_flags.csv"
    md_path = output_dir / f"{run_date.isoformat()}_event_risk_flags.md"
    write_frame_and_report(flags, csv_path, md_path, "ETF Event Risk Flags")
    typer.echo(f"ETF event risk flags：{md_path}")


@governance_app.command("status")
def governance_status_command(
    output_dir: Annotated[Path, typer.Option(help="输出目录。")] = DEFAULT_ETF_REPORT_DIR / "p1",
) -> None:
    """生成 P1 weight governance status；manual-review-only。"""
    config = load_etf_config_bundle()
    status = build_governance_status(config)
    run_date = date.today()
    csv_path = output_dir / f"{run_date.isoformat()}_governance_status.csv"
    md_path = output_dir / f"{run_date.isoformat()}_governance_status.md"
    write_frame_and_report(status, csv_path, md_path, "ETF Weight Governance Status")
    typer.echo(f"ETF governance status：{md_path}")


@governance_app.command("summary")
def governance_summary_command(
    candidate_path: Annotated[
        Path | None,
        typer.Option("--candidate", help="候选参数 JSON；缺省时输出 NO_CANDIDATE。"),
    ] = None,
    date_option: Annotated[str | None, typer.Option("--date", help="报告日期。")] = None,
    policy_path: Annotated[
        Path,
        typer.Option(help="ETF 参数治理 policy。"),
    ] = DEFAULT_ETF_PARAMETER_GOVERNANCE_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option(help="输出目录。"),
    ] = DEFAULT_ETF_REPORT_DIR / "governance",
) -> None:
    """生成 ETF parameter governance summary；只允许人工复核，不自动 promotion。"""
    config = load_etf_config_bundle()
    if candidate_path is not None and not candidate_path.exists():
        raise typer.BadParameter(f"候选参数 JSON 不存在：{candidate_path}")
    run_date = _parse_date(date_option) if date_option and date_option != "latest" else date.today()
    policy = load_parameter_governance_policy(policy_path)
    candidate = read_parameter_candidate(candidate_path)
    payload = evaluate_parameter_candidate(
        config=config,
        policy=policy,
        candidate=candidate,
        candidate_path=candidate_path,
        policy_config_path=policy_path,
    )
    json_path = output_dir / f"{run_date.isoformat()}_parameter_governance.json"
    md_path = output_dir / f"{run_date.isoformat()}_parameter_governance.md"
    write_parameter_governance_summary(payload, json_path=json_path, markdown_path=md_path)
    typer.echo(f"ETF parameter governance：{md_path}")
    typer.echo(f"promotion_status={payload['promotion_status']}")
    typer.echo(f"production_effect={payload['production_effect']}")


@credibility_app.command("validate")
def credibility_validate_command(
    date_option: Annotated[str | None, typer.Option("--date", help="报告日期。")] = None,
    output_dir: Annotated[
        Path,
        typer.Option(help="输出目录。"),
    ] = DEFAULT_ETF_CREDIBILITY_DIR,
) -> None:
    """聚合 TRADING-063A~J credibility checks；fail closed。"""
    config = load_etf_config_bundle()
    run_date = _parse_date(date_option) if date_option and date_option != "latest" else date.today()
    payload = build_credibility_gate(config=config)
    json_path = output_dir / f"{run_date.isoformat()}_credibility_gate.json"
    md_path = output_dir / f"{run_date.isoformat()}_credibility_gate.md"
    write_credibility_gate(payload, json_path=json_path, markdown_path=md_path)
    typer.echo(f"ETF credibility gate：{md_path}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"production_effect={payload['production_effect']}")
    typer.echo(f"broker_action={payload['broker_action']}")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@experiments_app.command("register")
def experiments_register_command(
    status: Annotated[
        str,
        typer.Option(help="实验状态：candidate/shadow/retired 等。"),
    ] = "candidate",
    notes: Annotated[str, typer.Option(help="实验备注。")] = "manual registration",
    registry_path: Annotated[Path, typer.Option(help="实验 registry JSONL。")] = (
        DEFAULT_ETF_REPORT_DIR / "experiments" / "registry.jsonl"
    ),
) -> None:
    """登记 ETF experiment registry 记录；不触发 promotion。"""
    config = load_etf_config_bundle()
    append_experiment_registry(
        registry_path=registry_path,
        model_version=config.strategy.model.version,
        parent_model_version=config.strategy.model.version,
        config_hash=config.config_hash,
        parameter_diff={},
        metrics={},
        status=status,
        notes=notes,
    )
    typer.echo(f"ETF experiment registry 已更新：{registry_path}")


@experiments_app.command("run")
def experiments_run_command(
    config_path: Annotated[
        Path | None,
        typer.Option("--config", help="候选 ETF strategy/config YAML；旧 P1 registry 路径。"),
    ] = None,
    pack: Annotated[
        str | None,
        typer.Option("--pack", help="TRADING-064 experiment pack id。"),
    ] = None,
    experiment: Annotated[
        str | None,
        typer.Option("--experiment", help="TRADING-064 single experiment id。"),
    ] = None,
    start: Annotated[
        str | None,
        typer.Option("--start", help="TRADING-064 batch backtest start YYYY-MM-DD。"),
    ] = None,
    end: Annotated[
        str | None,
        typer.Option("--end", help="TRADING-064 batch backtest end YYYY-MM-DD。"),
    ] = None,
    prices_path: Annotated[Path, typer.Option(help="价格缓存路径。")] = DEFAULT_ETF_PRICE_PATH,
    output_dir: Annotated[
        Path,
        typer.Option(help="TRADING-064 batch run 输出目录。"),
    ] = DEFAULT_ETF_EXPERIMENT_RUN_DIR,
    metrics_path: Annotated[
        Path | None,
        typer.Option("--backtest-summary-path", help="可选：候选回测 summary.json。"),
    ] = None,
    baseline_config_path: Annotated[
        Path,
        typer.Option(help="production baseline strategy config，用于 diff。"),
    ] = DEFAULT_ETF_STRATEGY_CONFIG_PATH,
    status: Annotated[
        str,
        typer.Option(help="实验状态：candidate/shadow/retired 等。"),
    ] = "candidate",
    notes: Annotated[str, typer.Option(help="实验备注。")] = "candidate experiment run",
    registry_path: Annotated[Path, typer.Option(help="实验 registry JSONL。")] = (
        DEFAULT_ETF_REPORT_DIR / "experiments" / "registry.jsonl"
    ),
) -> None:
    """记录 P1 candidate run，或执行 TRADING-064 controlled experiment batch。"""
    config = load_etf_config_bundle()
    if pack is not None or experiment is not None:
        if config_path is not None:
            raise typer.BadParameter("--config cannot be combined with --pack/--experiment")
        if start is None or end is None:
            raise typer.BadParameter("--start and --end are required for batch experiments")
        prices, quality_report = load_standard_prices(prices_path, config.assets, config.strategy)
        if not quality_report.passed:
            typer.echo(f"ETF 数据质量状态：{quality_report.status}，已停止 experiment batch。")
            raise typer.Exit(code=1)
        experiment_registry = load_experiment_registry()
        pack_registry = load_experiment_pack_registry(experiment_registry=experiment_registry)
        batch = run_experiment_batch(
            prices,
            base_config=config,
            quality_report=quality_report,
            experiment_registry=experiment_registry,
            pack_registry=pack_registry,
            pack_id=pack,
            experiment_id=experiment,
            start=_parse_date(start),
            end=_parse_date(end),
            output_root=output_dir,
        )
        typer.echo(f"ETF experiment batch 完成：{batch.run_id}")
        typer.echo(f"Run dir：{batch.run_dir}")
        typer.echo(f"status={batch.diagnostics_summary['status']}")
        typer.echo("production_effect=none")
        typer.echo("broker_action=none")
        if batch.diagnostics_summary["status"] != "PASS":
            raise typer.Exit(code=1)
        return
    if config_path is None:
        raise typer.BadParameter("--config or --pack/--experiment is required")
    append_experiment_run(
        registry_path=registry_path,
        candidate_config_path=config_path,
        baseline_config_path=baseline_config_path,
        config=config,
        metrics_path=metrics_path,
        status=status,
        notes=notes,
    )
    typer.echo(f"ETF experiment run 已登记：{registry_path}")
    typer.echo("production_effect=none")


@experiments_app.command("compare")
def experiments_compare_command(
    run_id: Annotated[
        str | None,
        typer.Option("--run-id", help="TRADING-064 experiment batch run id。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="读取最新 TRADING-064 experiment batch run。"),
    ] = False,
    baseline: Annotated[str, typer.Option(help="比较基准，默认 production。")] = "production",
    baseline_metrics_path: Annotated[
        Path | None,
        typer.Option("--baseline-summary-path", help="可选：production 回测 summary.json。"),
    ] = None,
    registry_path: Annotated[Path, typer.Option(help="实验 registry JSONL。")] = (
        DEFAULT_ETF_REPORT_DIR / "experiments" / "registry.jsonl"
    ),
    output_dir: Annotated[Path, typer.Option(help="比较报告输出目录。")] = (
        DEFAULT_ETF_REPORT_DIR / "experiments"
    ),
) -> None:
    """只读比较 ETF experiment registry 或 TRADING-064 batch run；不自动 promotion。"""
    if run_id is not None or latest:
        if run_id is not None and latest:
            raise typer.BadParameter("--run-id and --latest cannot be combined")
        run_dir = (
            find_latest_experiment_run_dir(output_dir)
            if latest
            else output_dir / str(run_id)
        )
        payload = build_experiment_comparison_report(run_dir)
        pack_id = payload["run_metadata"].get("pack_id")
        if pack_id:
            pack_registry = load_experiment_pack_registry()
            pack_config = pack_registry.experiment_packs.get(str(pack_id))
            if pack_config is not None:
                payload = apply_ranking_policy_to_comparison_report(
                    payload,
                    ranking_policy=pack_registry.ranking_policies[
                        pack_config.ranking_policy
                    ],
                    ranking_policy_id=pack_config.ranking_policy,
                )
        json_path = run_dir / "comparison_report.json"
        md_path = run_dir / "comparison_report.md"
        write_experiment_comparison_report(payload, json_path=json_path, markdown_path=md_path)
        typer.echo(f"ETF experiment comparison report：{md_path}")
        typer.echo(f"run_id={payload['run_metadata']['run_id']}")
        typer.echo(f"production_effect={payload['production_effect']}")
        return
    frame = build_experiment_comparison(
        registry_path=registry_path,
        baseline=baseline,
        baseline_metrics_path=baseline_metrics_path,
    )
    run_date = date.today()
    csv_path = output_dir / f"{run_date.isoformat()}_experiment_compare.csv"
    md_path = output_dir / f"{run_date.isoformat()}_experiment_compare.md"
    write_frame_and_report(
        frame,
        csv_path,
        md_path,
        "ETF Experiment Comparison",
        metadata={
            "baseline": baseline,
            "registry_path": registry_path,
            "production_effect": "none",
            "auto_promotion": False,
        },
    )
    typer.echo(f"ETF experiment comparison：{md_path}")
    typer.echo("production_effect=none")


@experiments_app.command("select-candidates")
def experiments_select_candidates_command(
    run_id: Annotated[
        str | None,
        typer.Option("--run-id", help="TRADING-064 experiment batch run id。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="读取最新 TRADING-064 experiment batch run。"),
    ] = False,
    promotion_policy: Annotated[
        str | None,
        typer.Option("--promotion-policy", help="覆盖默认 pack promotion policy。"),
    ] = None,
    output_dir: Annotated[Path, typer.Option(help="experiment run 输出目录。")] = (
        DEFAULT_ETF_EXPERIMENT_RUN_DIR
    ),
) -> None:
    """生成 TRADING-064 candidate selection gate；只允许 shadow-only/manual-review。"""
    if run_id is None and not latest:
        raise typer.BadParameter("--run-id or --latest is required")
    if run_id is not None and latest:
        raise typer.BadParameter("--run-id and --latest cannot be combined")
    run_dir = find_latest_experiment_run_dir(output_dir) if latest else output_dir / str(run_id)
    selection = _build_experiment_candidate_selection(
        run_dir,
        promotion_policy_override=promotion_policy,
    )
    json_path = run_dir / "candidate_selection_report.json"
    md_path = run_dir / "candidate_selection_report.md"
    write_candidate_selection_report(selection, json_path=json_path, markdown_path=md_path)
    summary = selection["selection_summary"]
    typer.echo(f"ETF experiment candidate selection gate：{md_path}")
    typer.echo(f"run_id={selection['run_metadata'].get('run_id')}")
    typer.echo(f"status={summary['status']}")
    typer.echo(f"eligible_for_shadow={summary['eligible_for_shadow_count']}")
    typer.echo("production_promotion_allowed=false")
    typer.echo(f"production_effect={selection['production_effect']}")


@experiments_app.command("enroll-shadow")
def experiments_enroll_shadow_command(
    run_id: Annotated[
        str | None,
        typer.Option("--run-id", help="TRADING-064 experiment batch run id。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="读取最新 TRADING-064 experiment batch run。"),
    ] = False,
    candidate: Annotated[
        list[str] | None,
        typer.Option("--candidate", help="candidate_id 或 experiment_id，可重复。"),
    ] = None,
    top: Annotated[
        int | None,
        typer.Option("--top", help="登记前 N 个 eligible_for_shadow candidates。"),
    ] = None,
    promotion_policy: Annotated[
        str | None,
        typer.Option("--promotion-policy", help="覆盖默认 pack promotion policy。"),
    ] = None,
    output_dir: Annotated[Path, typer.Option(help="experiment run 输出目录。")] = (
        DEFAULT_ETF_EXPERIMENT_RUN_DIR
    ),
    registry_path: Annotated[
        Path,
        typer.Option(help="shadow candidate registry 输出路径。"),
    ] = DEFAULT_ETF_SHADOW_CANDIDATE_REGISTRY_PATH,
) -> None:
    """把 eligible ETF experiment candidate 登记到 observe-only shadow registry。"""
    if run_id is None and not latest:
        raise typer.BadParameter("--run-id or --latest is required")
    if run_id is not None and latest:
        raise typer.BadParameter("--run-id and --latest cannot be combined")
    run_dir = find_latest_experiment_run_dir(output_dir) if latest else output_dir / str(run_id)
    selection = _build_experiment_candidate_selection(
        run_dir,
        promotion_policy_override=promotion_policy,
    )
    write_candidate_selection_report(
        selection,
        json_path=run_dir / "candidate_selection_report.json",
        markdown_path=run_dir / "candidate_selection_report.md",
    )
    try:
        registry = enroll_shadow_candidates(
            selection,
            registry_path=registry_path,
            candidate_ids=candidate,
            top=top,
        )
    except ValueError as exc:
        typer.echo(f"ETF shadow enrollment blocked: {exc}")
        raise typer.Exit(code=1) from exc
    typer.echo(f"ETF shadow candidates registry：{registry_path}")
    typer.echo(f"candidate_count={registry['candidate_count']}")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")


@experiments_app.command("weekly-review")
def experiments_weekly_review_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="周度复核日期 YYYY-MM-DD。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="使用当前日期生成 latest weekly review。"),
    ] = False,
    registry_path: Annotated[
        Path,
        typer.Option(help="shadow candidate registry 路径。"),
    ] = DEFAULT_ETF_SHADOW_CANDIDATE_REGISTRY_PATH,
    run_root: Annotated[Path, typer.Option(help="experiment run 根目录。")] = (
        DEFAULT_ETF_EXPERIMENT_RUN_DIR
    ),
    output_dir: Annotated[Path, typer.Option(help="weekly review 输出目录。")] = (
        DEFAULT_ETF_EXPERIMENT_WEEKLY_REVIEW_DIR
    ),
    review_policy: Annotated[
        str,
        typer.Option("--review-policy", help="weekly review policy id。"),
    ] = "weekly_shadow_review_v1",
) -> None:
    """生成 observe-only ETF experiment weekly review；不允许 production promotion。"""
    if latest and as_of is not None:
        raise typer.BadParameter("--latest and --as-of cannot be combined")
    review_date = date.today() if latest else _parse_date(as_of)
    pack_registry = load_experiment_pack_registry()
    policy = pack_registry.review_policies.get(review_policy)
    if policy is None:
        raise typer.BadParameter(f"unknown review policy: {review_policy}")
    payload = build_weekly_experiment_review(
        as_of=review_date,
        shadow_registry_path=registry_path,
        run_root=run_root,
        review_policy=policy,
        review_policy_id=review_policy,
    )
    json_path = output_dir / f"weekly_review_{review_date.isoformat()}.json"
    md_path = output_dir / f"weekly_review_{review_date.isoformat()}.md"
    write_weekly_experiment_review_report(payload, json_path=json_path, markdown_path=md_path)
    typer.echo(f"ETF experiment weekly review：{md_path}")
    typer.echo(f"status={payload['summary']['status']}")
    typer.echo(f"candidate_count={payload['summary']['candidate_count']}")
    typer.echo("production_promotion_allowed=false")
    typer.echo(f"production_effect={payload['production_effect']}")


@experiments_app.command("validate")
def experiments_validate_command(
    pack: Annotated[
        str,
        typer.Option("--pack", help="TRADING-064 experiment pack id。"),
    ] = "etf_calibration_v1",
    output_dir: Annotated[Path, typer.Option(help="validation report 输出目录。")] = (
        DEFAULT_ETF_EXPERIMENT_RUN_DIR / "validation"
    ),
    report_registry_path: Annotated[
        Path,
        typer.Option(help="report registry config path。"),
    ] = PROJECT_ROOT / "config" / "report_registry.yaml",
) -> None:
    """生成 TRADING-064 final experiment validation gate；失败时 fail closed。"""
    generated = datetime.now(UTC)
    payload = build_experiment_validation_report(
        pack_id=pack,
        report_registry_path=report_registry_path,
        generated_at=generated,
    )
    safe_pack = pack.replace("/", "_").replace("\\", "_")
    stem = f"{generated.date().isoformat()}_{safe_pack}_experiment_validation"
    json_path = output_dir / f"{stem}.json"
    md_path = output_dir / f"{stem}.md"
    write_experiment_validation_report(payload, json_path=json_path, markdown_path=md_path)
    typer.echo(f"ETF experiment validation gate：{md_path}")
    typer.echo(f"status={payload['status']}")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@forward_app.command("update")
def forward_update_command(
    date_option: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="forward evaluation 日期 YYYY-MM-DD 或 latest。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="使用 ETF price cache 最新日期。"),
    ] = False,
    config_path: Annotated[
        Path,
        typer.Option(help="TRADING-065 forward simulation policy config。"),
    ] = DEFAULT_ETF_FORWARD_CONFIG_PATH,
    registry_path: Annotated[
        Path,
        typer.Option(help="shadow candidate registry 路径。"),
    ] = DEFAULT_ETF_SHADOW_CANDIDATE_REGISTRY_PATH,
    decision_ledger_path: Annotated[
        Path,
        typer.Option(help="decision-time ledger 输出路径。"),
    ] = DEFAULT_ETF_FORWARD_DECISION_LEDGER_PATH,
    prices_path: Annotated[Path, typer.Option(help="ETF 标准价格 CSV/Parquet 路径。")] = (
        DEFAULT_ETF_PRICE_PATH
    ),
    output_dir: Annotated[Path, typer.Option(help="forward update 输出目录。")] = (
        DEFAULT_ETF_FORWARD_REPORT_DIR / "updates"
    ),
) -> None:
    """更新 active shadow candidates 的 evaluation-only forward performance。"""
    if latest and date_option is not None:
        raise typer.BadParameter("--latest and --date cannot be combined")
    run_date = _resolve_date(
        "latest" if latest or date_option is None else date_option,
        prices_path=prices_path,
    )
    try:
        payload = run_forward_update(
            as_of=run_date,
            config_path=config_path,
            registry_path=registry_path,
            decision_ledger_path=decision_ledger_path,
            prices_path=prices_path,
            output_dir=output_dir,
        )
    except ValueError as exc:
        typer.echo(f"ETF forward update blocked: {exc}")
        raise typer.Exit(code=1) from exc
    typer.echo(f"ETF forward update：{output_dir / f'forward_update_{run_date.isoformat()}.md'}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"active_candidate_count={payload['active_candidate_count']}")
    typer.echo("observe_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@forward_app.command("dashboard")
def forward_dashboard_command(
    date_option: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="dashboard 日期 YYYY-MM-DD 或 latest。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="使用 latest forward update artifact。"),
    ] = False,
    registry_path: Annotated[
        Path,
        typer.Option(help="shadow candidate registry 路径。"),
    ] = DEFAULT_ETF_SHADOW_CANDIDATE_REGISTRY_PATH,
    update_dir: Annotated[Path, typer.Option(help="forward update artifacts 目录。")] = (
        DEFAULT_ETF_FORWARD_REPORT_DIR / "updates"
    ),
    output_dir: Annotated[Path, typer.Option(help="dashboard 输出目录。")] = (
        DEFAULT_ETF_FORWARD_REPORT_DIR / "dashboard"
    ),
) -> None:
    """生成 candidate vs baseline vs benchmark forward dashboard。"""
    if latest and date_option is not None:
        raise typer.BadParameter("--latest and --date cannot be combined")
    run_date = None if latest or date_option is None else _parse_date(date_option)
    payload = run_forward_dashboard(
        as_of=run_date,
        latest=latest or date_option is None,
        registry_path=registry_path,
        update_dir=update_dir,
        output_dir=output_dir,
    )
    artifact_date = payload["as_of"]
    typer.echo(f"ETF forward dashboard：{output_dir / f'forward_dashboard_{artifact_date}.md'}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"active_candidate_count={payload['status_summary']['active_candidate_count']}")
    typer.echo("observe_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@forward_app.command("weekly-review")
def forward_weekly_review_command(
    date_option: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="weekly review 日期 YYYY-MM-DD 或 latest。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="使用 latest dashboard artifact。"),
    ] = False,
    dashboard_dir: Annotated[Path, typer.Option(help="forward dashboard artifacts 目录。")] = (
        DEFAULT_ETF_FORWARD_REPORT_DIR / "dashboard"
    ),
    output_dir: Annotated[Path, typer.Option(help="weekly review 输出目录。")] = (
        DEFAULT_ETF_FORWARD_REPORT_DIR / "weekly_reviews"
    ),
) -> None:
    """生成 TRADING-065 observe-only weekly review。"""
    if latest and date_option is not None:
        raise typer.BadParameter("--latest and --date cannot be combined")
    run_date = None if latest or date_option is None else _parse_date(date_option)
    payload = run_forward_weekly_review(
        as_of=run_date,
        dashboard_dir=dashboard_dir,
        output_dir=output_dir,
    )
    artifact_date = payload["as_of"]
    typer.echo(f"ETF forward weekly review：{output_dir / f'weekly_review_{artifact_date}.md'}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"candidate_count={payload['review_period']['candidate_count']}")
    typer.echo("production_promotion_allowed=false")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")


@forward_app.command("watchlist")
def forward_watchlist_command(
    date_option: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="watchlist 日期 YYYY-MM-DD 或 latest。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="使用 latest dashboard artifact。"),
    ] = False,
    dashboard_dir: Annotated[Path, typer.Option(help="forward dashboard artifacts 目录。")] = (
        DEFAULT_ETF_FORWARD_REPORT_DIR / "dashboard"
    ),
    output_dir: Annotated[Path, typer.Option(help="watchlist 输出目录。")] = (
        DEFAULT_ETF_FORWARD_REPORT_DIR / "watchlist"
    ),
) -> None:
    """生成本地 ETF forward watchlist；不发送外部 alert。"""
    if latest and date_option is not None:
        raise typer.BadParameter("--latest and --date cannot be combined")
    run_date = None if latest or date_option is None else _parse_date(date_option)
    payload = run_forward_watchlist(
        as_of=run_date,
        dashboard_dir=dashboard_dir,
        output_dir=output_dir,
    )
    artifact_date = payload["as_of"]
    typer.echo(f"ETF forward watchlist：{output_dir / f'forward_watchlist_{artifact_date}.md'}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"attention_count={payload['summary']['item_count']}")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")


@forward_app.command("validate")
def forward_validate_command(
    config_path: Annotated[
        Path,
        typer.Option(help="TRADING-065 forward simulation policy config。"),
    ] = DEFAULT_ETF_FORWARD_CONFIG_PATH,
    registry_path: Annotated[
        Path,
        typer.Option(help="shadow candidate registry 路径。"),
    ] = DEFAULT_ETF_SHADOW_CANDIDATE_REGISTRY_PATH,
    decision_ledger_path: Annotated[
        Path,
        typer.Option(help="decision-time ledger 路径。"),
    ] = DEFAULT_ETF_FORWARD_DECISION_LEDGER_PATH,
    report_registry_path: Annotated[
        Path,
        typer.Option(help="report registry config path。"),
    ] = PROJECT_ROOT / "config" / "report_registry.yaml",
    output_dir: Annotated[Path, typer.Option(help="validation 输出目录。")] = (
        DEFAULT_ETF_FORWARD_REPORT_DIR / "validation"
    ),
) -> None:
    """生成 TRADING-065 final forward simulation validation gate。"""
    payload = run_forward_validation(
        config_path=config_path,
        registry_path=registry_path,
        decision_ledger_path=decision_ledger_path,
        report_registry_path=report_registry_path,
        output_dir=output_dir,
    )
    typer.echo(f"ETF forward validation gate：{output_dir}")
    typer.echo(f"status={payload['status']}")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@weekly_review_app.command("aggregate")
def weekly_review_aggregate_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="weekly review aggregation 日期 YYYY-MM-DD。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="使用当前日期扫描 latest artifacts。"),
    ] = False,
    report_index_path: Annotated[
        Path | None,
        typer.Option(help="既有 report_index JSON；不传时只读扫描 report registry。"),
    ] = None,
    report_registry_path: Annotated[
        Path,
        typer.Option(help="report registry config path。"),
    ] = DEFAULT_REPORT_REGISTRY_PATH,
    target_weights_path: Annotated[
        Path,
        typer.Option(help="ETF target weights CSV。"),
    ] = DEFAULT_ETF_TARGET_PATH,
    required_report: Annotated[
        list[str] | None,
        typer.Option("--require-report", help="配置为必需的 report_id，可重复。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option(help="weekly review aggregation 输出目录。"),
    ] = DEFAULT_ETF_WEEKLY_REVIEW_AGGREGATION_DIR,
) -> None:
    """只读聚合 ETF weekly review 所需 latest artifacts。"""
    run_date = _weekly_review_date(as_of=as_of, latest=latest)
    payload = build_weekly_review_aggregation(
        as_of=run_date,
        report_index_path=report_index_path,
        report_registry_path=report_registry_path,
        target_weights_path=target_weights_path,
        required_report_ids=required_report,
    )
    json_path = output_dir / f"weekly_review_aggregation_{run_date.isoformat()}.json"
    write_weekly_review_aggregation(payload, json_path)
    typer.echo(f"ETF weekly review aggregation：{json_path}")
    typer.echo(f"aggregation_status={payload['aggregation_status']}")
    typer.echo(f"loaded_sections={len(payload['loaded_sections'])}")
    typer.echo(f"missing_sections={len(payload['missing_sections'])}")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")
    if payload["aggregation_status"] == "FAIL":
        raise typer.Exit(code=1)


@weekly_review_app.command("generate")
def weekly_review_generate_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="weekly review 日期 YYYY-MM-DD。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="使用当前日期生成 weekly review。"),
    ] = False,
    report_index_path: Annotated[
        Path | None,
        typer.Option(help="既有 report_index JSON；不传时只读扫描 report registry。"),
    ] = None,
    report_registry_path: Annotated[
        Path,
        typer.Option(help="report registry config path。"),
    ] = DEFAULT_REPORT_REGISTRY_PATH,
    target_weights_path: Annotated[
        Path,
        typer.Option(help="ETF target weights CSV。"),
    ] = DEFAULT_ETF_TARGET_PATH,
    required_report: Annotated[
        list[str] | None,
        typer.Option("--require-report", help="配置为必需的 report_id，可重复。"),
    ] = None,
    output_dir: Annotated[Path, typer.Option(help="weekly review 输出目录。")] = (
        DEFAULT_ETF_WEEKLY_REVIEW_DIR
    ),
    aggregation_dir: Annotated[
        Path,
        typer.Option(help="aggregation artifact 输出目录。"),
    ] = DEFAULT_ETF_WEEKLY_REVIEW_AGGREGATION_DIR,
) -> None:
    """生成 TRADING-068 ETF weekly portfolio review JSON/Markdown。"""
    _run_weekly_review_generate(
        as_of=as_of,
        latest=latest,
        report_index_path=report_index_path,
        report_registry_path=report_registry_path,
        target_weights_path=target_weights_path,
        required_report=required_report,
        output_dir=output_dir,
        aggregation_dir=aggregation_dir,
    )


@weekly_review_app.command("run")
def weekly_review_run_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="weekly review 日期 YYYY-MM-DD。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="使用当前日期生成 weekly review。"),
    ] = False,
    report_index_path: Annotated[
        Path | None,
        typer.Option(help="既有 report_index JSON；不传时只读扫描 report registry。"),
    ] = None,
    report_registry_path: Annotated[
        Path,
        typer.Option(help="report registry config path。"),
    ] = DEFAULT_REPORT_REGISTRY_PATH,
    target_weights_path: Annotated[
        Path,
        typer.Option(help="ETF target weights CSV。"),
    ] = DEFAULT_ETF_TARGET_PATH,
    required_report: Annotated[
        list[str] | None,
        typer.Option("--require-report", help="配置为必需的 report_id，可重复。"),
    ] = None,
    output_dir: Annotated[Path, typer.Option(help="weekly review 输出目录。")] = (
        DEFAULT_ETF_WEEKLY_REVIEW_DIR
    ),
    aggregation_dir: Annotated[
        Path,
        typer.Option(help="aggregation artifact 输出目录。"),
    ] = DEFAULT_ETF_WEEKLY_REVIEW_AGGREGATION_DIR,
) -> None:
    """`generate` 的别名；只读生成 weekly review package。"""
    _run_weekly_review_generate(
        as_of=as_of,
        latest=latest,
        report_index_path=report_index_path,
        report_registry_path=report_registry_path,
        target_weights_path=target_weights_path,
        required_report=required_report,
        output_dir=output_dir,
        aggregation_dir=aggregation_dir,
    )


@weekly_review_app.command("validate")
def weekly_review_validate_command(
    report_registry_path: Annotated[
        Path,
        typer.Option(help="report registry config path。"),
    ] = DEFAULT_REPORT_REGISTRY_PATH,
    output_dir: Annotated[
        Path,
        typer.Option(help="weekly review validation 输出目录。"),
    ] = DEFAULT_ETF_WEEKLY_REVIEW_VALIDATION_DIR,
) -> None:
    """生成 TRADING-068 weekly review validation gate；失败时 fail closed。"""
    generated = datetime.now(UTC)
    payload = build_weekly_review_validation_report(
        report_registry_path=report_registry_path,
        generated_at=generated,
    )
    stem = f"weekly_review_validation_{generated.date().isoformat()}"
    json_path = output_dir / f"{stem}.json"
    md_path = output_dir / f"{stem}.md"
    write_weekly_review_validation_report(payload, json_path=json_path, markdown_path=md_path)
    typer.echo(f"ETF weekly review validation gate：{md_path}")
    typer.echo(f"status={payload['status']}")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@decision_journal_app.command("add")
def decision_journal_add_command(
    weekly_review_path: Annotated[
        Path,
        typer.Option(help="TRADING-068 weekly review JSON path。"),
    ],
    action_item_id: Annotated[
        str,
        typer.Option(help="weekly review manual_review_actions[].action_id。"),
    ],
    human_decision: Annotated[
        str,
        typer.Option(help="人工决策摘要。"),
    ],
    decision_status: Annotated[
        str,
        typer.Option(help="decision_status enum value。"),
    ],
    rationale: Annotated[
        str,
        typer.Option(help="人工决策依据。"),
    ],
    confidence: Annotated[
        float,
        typer.Option(help="人工信心 0.0-1.0。"),
    ],
    follow_up_task: Annotated[
        str,
        typer.Option(help="后续人工任务。"),
    ],
    linked_candidate: Annotated[
        str,
        typer.Option(help="关联 candidate / portfolio review target。"),
    ],
    linked_report: Annotated[
        Path | None,
        typer.Option(help="可选关联报告；默认使用 weekly review JSON。"),
    ] = None,
    journal_path: Annotated[
        Path,
        typer.Option(help="decision journal state path。"),
    ] = DEFAULT_DECISION_JOURNAL_PATH,
) -> None:
    """追加人工 portfolio decision journal entry。"""
    try:
        journal = load_decision_journal(journal_path)
        entry = build_decision_entry_from_weekly_review(
            weekly_review_path=weekly_review_path,
            action_item_id=action_item_id,
            human_decision=human_decision,
            decision_status=decision_status,
            rationale=rationale,
            confidence=confidence,
            follow_up_task=follow_up_task,
            linked_candidate=linked_candidate,
            linked_report=linked_report,
        )
        updated = add_decision_entry(journal, entry)
        write_decision_journal(updated, journal_path)
    except DecisionJournalError as exc:
        typer.echo(f"ETF decision journal blocked：{exc}")
        typer.echo("production_effect=none")
        typer.echo("broker_action=none")
        raise typer.Exit(code=1) from exc
    typer.echo(f"ETF decision journal entry added：{journal_path}")
    typer.echo(f"decision_id={entry['decision_id']}")
    typer.echo(f"review_id={entry['review_id']}")
    typer.echo(f"action_item_id={entry['action_item_id']}")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@decision_journal_app.command("update")
def decision_journal_update_command(
    decision_id: Annotated[
        str,
        typer.Option(help="decision_id to update。"),
    ],
    journal_path: Annotated[
        Path,
        typer.Option(help="decision journal state path。"),
    ] = DEFAULT_DECISION_JOURNAL_PATH,
    human_decision: Annotated[
        str | None,
        typer.Option(help="更新人工决策摘要。"),
    ] = None,
    decision_status: Annotated[
        str | None,
        typer.Option(help="更新 decision_status enum value。"),
    ] = None,
    rationale: Annotated[
        str | None,
        typer.Option(help="更新 rationale。"),
    ] = None,
    confidence: Annotated[
        float | None,
        typer.Option(help="更新 confidence 0.0-1.0。"),
    ] = None,
    follow_up_task: Annotated[
        str | None,
        typer.Option(help="更新 follow-up task。"),
    ] = None,
    linked_candidate: Annotated[
        str | None,
        typer.Option(help="更新 linked candidate。"),
    ] = None,
    linked_report: Annotated[
        Path | None,
        typer.Option(help="更新 linked report。"),
    ] = None,
) -> None:
    """更新人工 portfolio decision journal entry。"""
    updates = {
        "human_decision": human_decision,
        "decision_status": decision_status,
        "rationale": rationale,
        "confidence": confidence,
        "follow_up_task": follow_up_task,
        "linked_candidate": linked_candidate,
        "linked_report": None if linked_report is None else str(linked_report),
    }
    try:
        if not any(value is not None for value in updates.values()):
            raise DecisionJournalError("update requires at least one field")
        journal = load_decision_journal(journal_path)
        updated = update_decision_entry(journal, decision_id=decision_id, updates=updates)
        write_decision_journal(updated, journal_path)
    except DecisionJournalError as exc:
        typer.echo(f"ETF decision journal blocked：{exc}")
        typer.echo("production_effect=none")
        typer.echo("broker_action=none")
        raise typer.Exit(code=1) from exc
    typer.echo(f"ETF decision journal entry updated：{journal_path}")
    typer.echo(f"decision_id={decision_id}")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@decision_journal_app.command("list")
def decision_journal_list_command(
    journal_path: Annotated[
        Path,
        typer.Option(help="decision journal state path。"),
    ] = DEFAULT_DECISION_JOURNAL_PATH,
    as_json: Annotated[
        bool,
        typer.Option("--json", help="输出 JSON payload。"),
    ] = False,
) -> None:
    """列出 active portfolio decision journal entries。"""
    try:
        journal = load_decision_journal(journal_path)
        entries = decision_entries(journal)
    except DecisionJournalError as exc:
        typer.echo(f"ETF decision journal blocked：{exc}")
        raise typer.Exit(code=1) from exc
    if as_json:
        typer.echo(
            json.dumps(
                {"journal_path": str(journal_path), "entries": entries},
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
        )
        return
    typer.echo(f"ETF decision journal entries：{len(entries)}")
    for entry in entries:
        typer.echo(
            " | ".join(
                [
                    str(entry.get("decision_id")),
                    str(entry.get("review_date")),
                    str(entry.get("decision_status")),
                    str(entry.get("action_item_id")),
                    str(entry.get("linked_candidate")),
                ]
            )
        )
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")


@decision_journal_app.command("remove")
def decision_journal_remove_command(
    decision_id: Annotated[
        str,
        typer.Option(help="decision_id to remove from active entries。"),
    ],
    reason: Annotated[
        str,
        typer.Option(help="remove reason；entry is moved to removed_entries audit trail。"),
    ],
    journal_path: Annotated[
        Path,
        typer.Option(help="decision journal state path。"),
    ] = DEFAULT_DECISION_JOURNAL_PATH,
) -> None:
    """从 active journal 移除 entry，并保留 removed_entries audit trail。"""
    try:
        journal = load_decision_journal(journal_path)
        updated = remove_decision_entry(journal, decision_id=decision_id, reason=reason)
        write_decision_journal(updated, journal_path)
    except DecisionJournalError as exc:
        typer.echo(f"ETF decision journal blocked：{exc}")
        typer.echo("production_effect=none")
        typer.echo("broker_action=none")
        raise typer.Exit(code=1) from exc
    typer.echo(f"ETF decision journal entry removed：{journal_path}")
    typer.echo(f"decision_id={decision_id}")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@decision_journal_app.command("report")
def decision_journal_report_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="decision journal report 日期 YYYY-MM-DD。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="使用当前日期生成 decision journal report。"),
    ] = False,
    journal_path: Annotated[
        Path,
        typer.Option(help="decision journal state path。"),
    ] = DEFAULT_DECISION_JOURNAL_PATH,
    output_dir: Annotated[
        Path,
        typer.Option(help="decision journal report 输出目录。"),
    ] = DEFAULT_DECISION_JOURNAL_REPORT_DIR,
) -> None:
    """生成 portfolio decision journal JSON/Markdown/HTML summary。"""
    run_date = _weekly_review_date(as_of=as_of, latest=latest)
    try:
        journal = load_decision_journal(journal_path)
        payload = build_decision_journal_report(
            journal,
            as_of=run_date,
            journal_path=journal_path,
        )
        json_path = output_dir / f"decision_journal_{run_date.isoformat()}.json"
        md_path = output_dir / f"decision_journal_{run_date.isoformat()}.md"
        html_path = output_dir / f"decision_journal_{run_date.isoformat()}.html"
        write_decision_journal_report(
            payload,
            json_path=json_path,
            markdown_path=md_path,
            html_path=html_path,
        )
    except DecisionJournalError as exc:
        typer.echo(f"ETF decision journal report blocked：{exc}")
        typer.echo("production_effect=none")
        typer.echo("broker_action=none")
        raise typer.Exit(code=1) from exc
    typer.echo(f"ETF decision journal report：{md_path}")
    typer.echo(f"json={json_path}")
    typer.echo(f"html={html_path}")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@decision_journal_app.command("analytics")
def decision_journal_analytics_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="decision journal analytics 日期 YYYY-MM-DD。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="使用当前日期生成 analytics。"),
    ] = False,
    journal_path: Annotated[
        Path,
        typer.Option(help="decision journal state path。"),
    ] = DEFAULT_DECISION_JOURNAL_PATH,
    output_dir: Annotated[
        Path,
        typer.Option(help="decision journal analytics 输出目录。"),
    ] = DEFAULT_DECISION_JOURNAL_REPORT_DIR,
) -> None:
    """生成 portfolio decision journal outcome analytics JSON。"""
    run_date = _weekly_review_date(as_of=as_of, latest=latest)
    try:
        journal = load_decision_journal(journal_path)
        payload = build_decision_journal_analytics(journal)
        output_path = output_dir / f"decision_journal_analytics_{run_date.isoformat()}.json"
        write_decision_journal_analytics(payload, output_path)
    except DecisionJournalError as exc:
        typer.echo(f"ETF decision journal analytics blocked：{exc}")
        typer.echo("production_effect=none")
        typer.echo("broker_action=none")
        raise typer.Exit(code=1) from exc
    typer.echo(f"ETF decision journal analytics：{output_path}")
    typer.echo(f"entry_count={payload['entry_count']}")
    typer.echo(f"follow_up_task_count={payload['follow_up_task_count']}")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")


@decision_journal_app.command("propose-state-updates")
def decision_journal_propose_state_updates_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="proposal 日期 YYYY-MM-DD。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="使用当前日期生成 proposal。"),
    ] = False,
    journal_path: Annotated[
        Path,
        typer.Option(help="decision journal state path。"),
    ] = DEFAULT_DECISION_JOURNAL_PATH,
    output_dir: Annotated[
        Path,
        typer.Option(help="decision state proposal 输出目录。"),
    ] = DEFAULT_DECISION_JOURNAL_PROPOSAL_DIR,
) -> None:
    """生成 candidate state update proposal；不修改 candidate registry。"""
    run_date = _weekly_review_date(as_of=as_of, latest=latest)
    try:
        journal = load_decision_journal(journal_path)
        payload = build_candidate_state_update_proposals(journal)
        json_path = output_dir / f"decision_state_update_proposals_{run_date.isoformat()}.json"
        md_path = output_dir / f"decision_state_update_proposals_{run_date.isoformat()}.md"
        write_decision_state_update_proposals(
            payload,
            json_path=json_path,
            markdown_path=md_path,
        )
    except DecisionJournalError as exc:
        typer.echo(f"ETF decision state proposal blocked：{exc}")
        typer.echo("production_effect=none")
        typer.echo("broker_action=none")
        raise typer.Exit(code=1) from exc
    typer.echo(f"ETF decision state update proposal：{md_path}")
    typer.echo(f"proposal_count={payload['proposal_count']}")
    typer.echo("state_mutation_performed=false")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")


@decision_journal_app.command("validate")
def decision_journal_validate_command(
    journal_path: Annotated[
        Path,
        typer.Option(help="decision journal state path。"),
    ] = DEFAULT_DECISION_JOURNAL_PATH,
    output_dir: Annotated[
        Path,
        typer.Option(help="decision journal validation 输出目录。"),
    ] = DEFAULT_DECISION_JOURNAL_VALIDATION_DIR,
) -> None:
    """生成 TRADING-069 decision journal validation gate；失败时 fail closed。"""
    generated = datetime.now(UTC)
    payload = build_decision_journal_validation_report(
        journal_path=journal_path,
        generated_at=generated,
    )
    stem = f"decision_journal_validation_{generated.date().isoformat()}"
    json_path = output_dir / f"{stem}.json"
    md_path = output_dir / f"{stem}.md"
    write_decision_journal_validation_report(payload, json_path=json_path, markdown_path=md_path)
    typer.echo(f"ETF decision journal validation gate：{md_path}")
    typer.echo(f"status={payload['status']}")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@parameter_review_app.command("aggregate")
def parameter_review_aggregate_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="parameter review aggregation 日期 YYYY-MM-DD。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="使用当前日期扫描 latest artifacts。"),
    ] = False,
    report_index_path: Annotated[
        Path | None,
        typer.Option(help="既有 report_index JSON；不传时只读扫描 report registry。"),
    ] = None,
    report_registry_path: Annotated[
        Path,
        typer.Option(help="report registry config path。"),
    ] = DEFAULT_REPORT_REGISTRY_PATH,
    output_dir: Annotated[
        Path,
        typer.Option(help="parameter review evidence 输出目录。"),
    ] = DEFAULT_PARAMETER_REVIEW_AGGREGATION_DIR,
) -> None:
    """聚合 TRADING-070 ETF parameter review forward evidence。"""
    run_date = _weekly_review_date(as_of=as_of, latest=latest)
    payload = build_parameter_review_aggregation(
        as_of=run_date,
        report_index_path=report_index_path,
        report_registry_path=report_registry_path,
    )
    json_path = output_dir / f"parameter_review_evidence_{run_date.isoformat()}.json"
    md_path = output_dir / f"parameter_review_evidence_{run_date.isoformat()}.md"
    write_parameter_review_aggregation(payload, json_path=json_path, markdown_path=md_path)
    typer.echo(f"ETF parameter review evidence：{md_path}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"candidate_count={payload['candidate_count']}")
    typer.echo(f"evidence_record_count={payload['evidence_record_count']}")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@parameter_review_app.command("report")
def parameter_review_report_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="parameter review report 日期 YYYY-MM-DD。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="使用当前日期扫描 latest artifacts。"),
    ] = False,
    report_index_path: Annotated[
        Path | None,
        typer.Option(help="既有 report_index JSON；不传时只读扫描 report registry。"),
    ] = None,
    report_registry_path: Annotated[
        Path,
        typer.Option(help="report registry config path。"),
    ] = DEFAULT_REPORT_REGISTRY_PATH,
    output_dir: Annotated[
        Path,
        typer.Option(help="parameter review report 输出目录。"),
    ] = DEFAULT_PARAMETER_REVIEW_REVIEW_DIR,
) -> None:
    """生成 TRADING-070 ETF parameter review report。"""
    _run_parameter_review_report_command(
        as_of=as_of,
        latest=latest,
        report_index_path=report_index_path,
        report_registry_path=report_registry_path,
        output_dir=output_dir,
    )


@parameter_review_app.command("run")
def parameter_review_run_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="parameter review report 日期 YYYY-MM-DD。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="使用当前日期扫描 latest artifacts。"),
    ] = False,
    report_index_path: Annotated[
        Path | None,
        typer.Option(help="既有 report_index JSON；不传时只读扫描 report registry。"),
    ] = None,
    report_registry_path: Annotated[
        Path,
        typer.Option(help="report registry config path。"),
    ] = DEFAULT_REPORT_REGISTRY_PATH,
    output_dir: Annotated[
        Path,
        typer.Option(help="parameter review report 输出目录。"),
    ] = DEFAULT_PARAMETER_REVIEW_REVIEW_DIR,
) -> None:
    """运行 TRADING-070 ETF parameter review report workflow。"""
    _run_parameter_review_report_command(
        as_of=as_of,
        latest=latest,
        report_index_path=report_index_path,
        report_registry_path=report_registry_path,
        output_dir=output_dir,
    )


@parameter_review_app.command("validate")
def parameter_review_validate_command(
    report_registry_path: Annotated[
        Path,
        typer.Option(help="report registry config path。"),
    ] = DEFAULT_REPORT_REGISTRY_PATH,
    output_dir: Annotated[
        Path,
        typer.Option(help="parameter review validation 输出目录。"),
    ] = DEFAULT_PARAMETER_REVIEW_VALIDATION_DIR,
) -> None:
    """生成 TRADING-070 parameter review validation gate；失败时 fail closed。"""
    generated = datetime.now(UTC)
    payload = build_parameter_review_validation_report(
        report_registry_path=report_registry_path,
        generated_at=generated,
    )
    stem = f"parameter_review_validation_{generated.date().isoformat()}"
    json_path = output_dir / f"{stem}.json"
    md_path = output_dir / f"{stem}.md"
    write_parameter_review_validation_report(
        payload,
        json_path=json_path,
        markdown_path=md_path,
    )
    typer.echo(f"ETF parameter review validation gate：{md_path}")
    typer.echo(f"status={payload['status']}")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


def _run_parameter_review_report_command(
    *,
    as_of: str | None,
    latest: bool,
    report_index_path: Path | None,
    report_registry_path: Path,
    output_dir: Path,
) -> None:
    run_date = _weekly_review_date(as_of=as_of, latest=latest)
    payload = build_parameter_review_report(
        as_of=run_date,
        report_index_path=report_index_path,
        report_registry_path=report_registry_path,
    )
    json_path = output_dir / f"parameter_review_{run_date.isoformat()}.json"
    md_path = output_dir / f"parameter_review_{run_date.isoformat()}.md"
    write_parameter_review_report(payload, json_path=json_path, markdown_path=md_path)
    typer.echo(f"ETF parameter review report：{md_path}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"candidate_count={payload['summary']['candidate_count']}")
    typer.echo(
        "eligible_for_manual_review_count="
        f"{payload['summary']['eligible_for_manual_review_count']}"
    )
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@weight_calibration_app.command("validate-config")
def weight_calibration_validate_config_command(
    search: Annotated[
        str,
        typer.Option("--search", "--config", help="weight search id。"),
    ] = "etf_initial_weight_search_v1",
    config_path: Annotated[
        Path,
        typer.Option(help="weight search config path。"),
    ] = DEFAULT_ETF_WEIGHT_SEARCH_CONFIG_PATH,
) -> None:
    """校验 TRADING-071A historical weight search config。"""
    registry = load_weight_search_registry(config_path)
    definition = load_weight_search_definition(search, config_path)
    objective = registry.objective_policies[definition.objective_policy]
    benchmark_set = registry.benchmark_sets[definition.benchmark_set]
    typer.echo("ETF weight calibration config 校验通过。")
    typer.echo(f"search_id={definition.search_id}")
    typer.echo(f"config_hash={registry.config_hash}")
    typer.echo(f"universe={','.join(definition.universe)}")
    typer.echo(f"grid_step={definition.grid_step:.4f}")
    typer.echo(f"max_candidate_count={definition.max_candidate_count}")
    typer.echo(f"objective_policy={definition.objective_policy}")
    typer.echo(f"objective_policy_status={objective.policy_status}")
    typer.echo(f"benchmark_set={definition.benchmark_set}")
    typer.echo(f"benchmark_ids={','.join(benchmark_set.benchmark_ids)}")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@weight_calibration_app.command("search")
def weight_calibration_search_command(
    search: Annotated[
        str,
        typer.Option("--search", "--config", help="weight search id。"),
    ] = "etf_initial_weight_search_v1",
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="weight search config YAML path。"),
    ] = DEFAULT_ETF_WEIGHT_SEARCH_CONFIG_PATH,
    preset: Annotated[
        str | None,
        typer.Option("--preset", help="historical range preset id。"),
    ] = None,
    preset_config_path: Annotated[
        Path,
        typer.Option("--preset-config-path", help="historical range preset config YAML path。"),
    ] = DEFAULT_WEIGHT_CALIBRATION_PRESET_CONFIG_PATH,
    prices_path: Annotated[Path, typer.Option(help="ETF 标准价格缓存路径。")] = (
        DEFAULT_ETF_PRICE_PATH
    ),
    start: Annotated[
        str | None,
        typer.Option("--start", help="historical search start YYYY-MM-DD。"),
    ] = None,
    end: Annotated[
        str | None,
        typer.Option("--end", help="historical search end YYYY-MM-DD。"),
    ] = None,
    max_candidates: Annotated[
        int | None,
        typer.Option("--max-candidates", help="lower-than-config candidate evaluation cap。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option(help="weight calibration report 输出目录。"),
    ] = DEFAULT_ETF_WEIGHT_CALIBRATION_REPORT_DIR,
    data_output_dir: Annotated[
        Path,
        typer.Option(help="weight calibration runtime data 输出目录。"),
    ] = DEFAULT_ETF_WEIGHT_CALIBRATION_DATA_DIR,
) -> None:
    """执行 TRADING-071B bounded historical ETF weight search。"""
    if preset is not None and (start is not None or end is not None):
        raise typer.BadParameter("--preset cannot be combined with --start or --end")
    config = load_etf_config_bundle()
    registry = load_weight_search_registry(config_path, etf_config=config)
    prices, quality_report = load_standard_prices(prices_path, config.assets, config.strategy)
    if not quality_report.passed:
        typer.echo(f"ETF 数据质量状态：{quality_report.status}，已停止 weight search。")
        raise typer.Exit(code=1)
    run_start = _parse_date(start) if start else None
    run_end = _parse_date(end) if end else None
    preset_context = None
    if preset is not None:
        historical_preset = load_weight_calibration_preset(
            preset,
            preset_config_path,
            etf_config=config,
            weight_search_registry=registry,
        )
        available_dates = pd.to_datetime(prices["date"], errors="coerce").dropna()
        if available_dates.empty:
            raise typer.BadParameter("prices_path has no valid date values")
        preset_context = resolve_weight_calibration_preset(
            historical_preset,
            available_start=available_dates.min().date(),
            available_end=available_dates.max().date(),
        )
        run_start = preset_context["start_date"]
        run_end = preset_context["end_date"]
    run = run_historical_weight_search(
        prices,
        etf_config=config,
        quality_report=quality_report,
        registry=registry,
        search_id=search,
        start=run_start,
        end=run_end,
        range_preset=preset_context,
        max_candidates=max_candidates,
    )
    paths = write_weight_search_run(
        run,
        report_root=output_dir,
        data_root=data_output_dir,
    )
    generation = run.payload["candidate_generation"]
    typer.echo(f"ETF weight calibration search 完成：{run.run_id}")
    typer.echo(f"report={paths['summary_md']}")
    typer.echo(f"data_dir={paths['data_dir']}")
    typer.echo(f"evaluated_candidate_count={generation['evaluated_candidate_count']}")
    typer.echo(f"total_valid_candidate_count={generation['total_valid_candidate_count']}")
    typer.echo(f"blocked_candidate_count={len(run.payload['blocked_candidates'])}")
    typer.echo(f"data_quality_status={run.payload['data_quality_status']}")
    if preset_context is not None:
        typer.echo(f"preset_id={preset_context['preset_id']}")
        typer.echo(
            "resolved_date_range="
            f"{preset_context['start_date'].isoformat()}:{preset_context['end_date'].isoformat()}"
        )
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@weight_calibration_app.command("register-candidates")
def weight_calibration_register_candidates_command(
    run_id: Annotated[
        str | None,
        typer.Option("--run-id", help="historical weight search run id。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="读取最新 historical weight search run。"),
    ] = False,
    top: Annotated[
        int | None,
        typer.Option("--top", help="登记 ranking 前 N 个 candidates。"),
    ] = None,
    weight_set: Annotated[
        list[str] | None,
        typer.Option("--weight-set", help="candidate_id 或 weight_set_id，可重复。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option(help="weight calibration report 输出目录。"),
    ] = DEFAULT_ETF_WEIGHT_CALIBRATION_REPORT_DIR,
    registry_path: Annotated[
        Path,
        typer.Option(help="candidate initial weight set registry path。"),
    ] = DEFAULT_CANDIDATE_WEIGHT_REGISTRY_PATH,
) -> None:
    """登记 TRADING-071D candidate-only initial weight sets。"""
    if run_id is not None and latest:
        raise typer.BadParameter("--run-id and --latest cannot be combined")
    if weight_set and top is not None:
        raise typer.BadParameter("--weight-set cannot be combined with --top")
    if run_id is None and not latest:
        raise typer.BadParameter("--run-id or --latest is required")
    run_dir = find_latest_weight_search_run_dir(output_dir) if latest else output_dir / str(run_id)
    payload = read_weight_search_run_payload(run_dir)
    registry = register_candidate_weight_sets(
        payload,
        registry_path=registry_path,
        top=None if weight_set else (top or 3),
        weight_set_ids=weight_set,
    )
    typer.echo(f"ETF candidate weight registry：{registry_path}")
    typer.echo(f"source_search_run_id={payload['search_run_id']}")
    typer.echo(f"candidate_count={registry['candidate_count']}")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@weight_calibration_app.command("export-top")
def weight_calibration_export_top_command(
    run_id: Annotated[
        str | None,
        typer.Option("--run-id", help="historical weight search run id。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="读取最新 historical weight search run。"),
    ] = False,
    top: Annotated[
        int,
        typer.Option("--top", help="导出 ranking 前 N 个 candidates。"),
    ] = 10,
    search_output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="weight calibration search report 输出目录。"),
    ] = DEFAULT_ETF_WEIGHT_CALIBRATION_REPORT_DIR,
    overfit_path: Annotated[
        Path | None,
        typer.Option(help="optional overfit diagnostics JSON path。"),
    ] = None,
    export_dir: Annotated[
        Path,
        typer.Option("--export-dir", help="Top-N candidate export 输出目录。"),
    ] = DEFAULT_WEIGHT_TOP_CANDIDATE_EXPORT_DIR,
) -> None:
    """导出 TRADING-078B Top-N historical candidate weight sets。"""
    if run_id is not None and latest:
        raise typer.BadParameter("--run-id and --latest cannot be combined")
    if run_id is None and not latest:
        raise typer.BadParameter("--run-id or --latest is required")
    run_dir = (
        find_latest_weight_search_run_dir(search_output_dir)
        if latest
        else search_output_dir / str(run_id)
    )
    search_payload = read_weight_search_run_payload(run_dir)
    source_paths = {"historical_search": str(run_dir / "summary.json")}
    if overfit_path is not None:
        source_paths["overfit_diagnostics"] = str(overfit_path)
    payload = build_weight_top_candidate_export(
        search_payload,
        top=top,
        overfit_payload=_load_optional_json_payload(overfit_path),
        source_paths=source_paths,
    )
    paths = write_weight_top_candidate_export(payload, output_dir=export_dir)
    typer.echo(f"ETF weight top-N candidate export：{paths['markdown']}")
    typer.echo(f"json={paths['json']}")
    typer.echo(f"csv={paths['csv']}")
    typer.echo(f"exported_candidate_count={payload['exported_candidate_count']}")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@weight_calibration_app.command("comparison")
def weight_calibration_comparison_command(
    run_id: Annotated[
        str | None,
        typer.Option("--run-id", help="historical weight search run id。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="读取最新 historical weight search run。"),
    ] = False,
    top: Annotated[
        int,
        typer.Option("--top", help="包含 ranking 前 N 个 candidates。"),
    ] = 10,
    search_output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="weight calibration search report 输出目录。"),
    ] = DEFAULT_ETF_WEIGHT_CALIBRATION_REPORT_DIR,
    top_export_path: Annotated[
        Path | None,
        typer.Option(help="optional TRADING-078B Top-N JSON path。"),
    ] = None,
    comparison_dir: Annotated[
        Path,
        typer.Option("--comparison-dir", help="candidate comparison table 输出目录。"),
    ] = DEFAULT_WEIGHT_CANDIDATE_COMPARISON_DIR,
) -> None:
    """生成 TRADING-078C candidate weights and benchmarks comparison table。"""
    if run_id is not None and latest:
        raise typer.BadParameter("--run-id and --latest cannot be combined")
    if run_id is None and not latest:
        raise typer.BadParameter("--run-id or --latest is required")
    run_dir = (
        find_latest_weight_search_run_dir(search_output_dir)
        if latest
        else search_output_dir / str(run_id)
    )
    search_payload = read_weight_search_run_payload(run_dir)
    source_paths = {"historical_search": str(run_dir / "summary.json")}
    if top_export_path is not None:
        source_paths["top_candidate_export"] = str(top_export_path)
    payload = build_weight_candidate_comparison_table(
        search_payload,
        top_export_payload=_load_optional_json_payload(top_export_path),
        top=top,
        source_paths=source_paths,
    )
    paths = write_weight_candidate_comparison_table(payload, output_dir=comparison_dir)
    typer.echo(f"ETF weight candidate comparison table：{paths['markdown']}")
    typer.echo(f"json={paths['json']}")
    typer.echo(f"csv={paths['csv']}")
    typer.echo(f"row_count={payload['row_count']}")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@weight_calibration_app.command("regime-robustness")
def weight_calibration_regime_robustness_command(
    run_id: Annotated[
        str | None,
        typer.Option("--run-id", help="historical weight search run id。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="读取最新 historical weight search run。"),
    ] = False,
    top: Annotated[
        int,
        typer.Option("--top", help="包含 ranking 前 N 个 candidates。"),
    ] = 10,
    search_output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="weight calibration search report 输出目录。"),
    ] = DEFAULT_ETF_WEIGHT_CALIBRATION_REPORT_DIR,
    top_export_path: Annotated[
        Path | None,
        typer.Option(help="optional TRADING-078B Top-N JSON path。"),
    ] = None,
    heatmap_dir: Annotated[
        Path,
        typer.Option("--heatmap-dir", help="regime robustness heatmap 输出目录。"),
    ] = DEFAULT_WEIGHT_REGIME_ROBUSTNESS_DIR,
) -> None:
    """生成 TRADING-078D regime robustness heatmap-ready data。"""
    if run_id is not None and latest:
        raise typer.BadParameter("--run-id and --latest cannot be combined")
    if run_id is None and not latest:
        raise typer.BadParameter("--run-id or --latest is required")
    run_dir = (
        find_latest_weight_search_run_dir(search_output_dir)
        if latest
        else search_output_dir / str(run_id)
    )
    search_payload = read_weight_search_run_payload(run_dir)
    source_paths = {"historical_search": str(run_dir / "summary.json")}
    if top_export_path is not None:
        source_paths["top_candidate_export"] = str(top_export_path)
    payload = build_weight_regime_robustness_heatmap(
        search_payload,
        top_export_payload=_load_optional_json_payload(top_export_path),
        top=top,
        source_paths=source_paths,
    )
    paths = write_weight_regime_robustness_heatmap(payload, output_dir=heatmap_dir)
    typer.echo(f"ETF weight regime robustness heatmap：{paths['markdown']}")
    typer.echo(f"json={paths['json']}")
    typer.echo(f"csv={paths['csv']}")
    typer.echo(f"matrix_row_count={payload['matrix_row_count']}")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@weight_calibration_app.command("enroll-forward")
def weight_calibration_enroll_forward_command(
    latest: Annotated[
        bool,
        typer.Option(
            "--latest",
            help="使用当前 candidate weight registry；未指定 --top 时默认登记前三名。",
        ),
    ] = False,
    top: Annotated[
        int | None,
        typer.Option("--top", help="按 registry rank 登记前 N 个 candidates。"),
    ] = None,
    weight_set: Annotated[
        list[str] | None,
        typer.Option("--weight-set", help="weight_set_id 或 source_candidate_id，可重复。"),
    ] = None,
    registry_path: Annotated[
        Path,
        typer.Option(help="candidate initial weight set registry path。"),
    ] = DEFAULT_CANDIDATE_WEIGHT_REGISTRY_PATH,
    enrollment_path: Annotated[
        Path,
        typer.Option(help="dual-track forward enrollment registry path。"),
    ] = DEFAULT_WEIGHT_FORWARD_ENROLLMENT_PATH,
) -> None:
    """登记 TRADING-071E dual-track forward observation candidates。"""
    if weight_set and top is not None:
        raise typer.BadParameter("--weight-set cannot be combined with --top")
    if not weight_set and top is None and not latest:
        raise typer.BadParameter("--weight-set or --latest/--top is required")
    registry = load_candidate_weight_registry(registry_path)
    enrollment = enroll_candidate_weights_forward(
        registry,
        enrollment_path=enrollment_path,
        top=None if weight_set else (top or 3),
        weight_set_ids=weight_set,
    )
    latest_selection = enrollment.get("latest_selection", {})
    typer.echo(f"ETF weight calibration forward enrollment：{enrollment_path}")
    typer.echo(f"enrollment_count={enrollment['enrollment_count']}")
    typer.echo(
        "selected_weight_set_count="
        f"{len(latest_selection.get('weight_set_ids') or [])}"
    )
    typer.echo("shared_shadow_registry_mutated=false")
    typer.echo("production_weights_mutated=false")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@weight_calibration_app.command("enroll-top")
def weight_calibration_enroll_top_command(
    run_id: Annotated[
        str | None,
        typer.Option("--run-id", help="historical weight search run id。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="读取最新 historical weight search run。"),
    ] = False,
    top: Annotated[
        int,
        typer.Option("--top", help="enroll Top-N shadow_ready candidates。"),
    ] = 3,
    search_output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="weight calibration search report 输出目录。"),
    ] = DEFAULT_ETF_WEIGHT_CALIBRATION_REPORT_DIR,
    top_export_path: Annotated[
        Path | None,
        typer.Option(help="optional TRADING-078B Top-N JSON path。"),
    ] = None,
    comparison_path: Annotated[
        Path | None,
        typer.Option(help="optional TRADING-078C comparison JSON path，保留为 source link。"),
    ] = None,
    enrollment_path: Annotated[
        Path,
        typer.Option(help="dual-track forward enrollment registry path。"),
    ] = DEFAULT_WEIGHT_FORWARD_ENROLLMENT_PATH,
) -> None:
    """从 TRADING-078B Top-N shortlist 登记 shadow-ready weight candidates。"""
    if run_id is not None and latest:
        raise typer.BadParameter("--run-id and --latest cannot be combined")
    if run_id is None and not latest:
        raise typer.BadParameter("--run-id or --latest is required")
    run_dir = (
        find_latest_weight_search_run_dir(search_output_dir)
        if latest
        else search_output_dir / str(run_id)
    )
    search_payload = read_weight_search_run_payload(run_dir)
    source_paths = {"historical_search": str(run_dir / "summary.json")}
    if top_export_path is not None:
        source_paths["top_candidate_export"] = str(top_export_path)
    if comparison_path is not None:
        source_paths["comparison_table"] = str(comparison_path)
    enrollment = enroll_top_weight_candidates_forward(
        search_payload,
        top_export_payload=_load_optional_json_payload(top_export_path),
        comparison_payload=_load_optional_json_payload(comparison_path),
        source_paths=source_paths,
        enrollment_path=enrollment_path,
        top=top,
    )
    _echo_weight_shadow_enrollment_summary(enrollment_path, enrollment)


@weight_calibration_app.command("enroll")
def weight_calibration_enroll_command(
    weight_set: Annotated[
        list[str],
        typer.Option("--weight-set", help="weight_set_id 或 source_candidate_id，可重复。"),
    ],
    run_id: Annotated[
        str | None,
        typer.Option("--run-id", help="historical weight search run id。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="读取最新 historical weight search run。"),
    ] = False,
    search_output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="weight calibration search report 输出目录。"),
    ] = DEFAULT_ETF_WEIGHT_CALIBRATION_REPORT_DIR,
    top_export_path: Annotated[
        Path | None,
        typer.Option(help="optional TRADING-078B Top-N JSON path。"),
    ] = None,
    comparison_path: Annotated[
        Path | None,
        typer.Option(help="optional TRADING-078C comparison JSON path，保留为 source link。"),
    ] = None,
    enrollment_path: Annotated[
        Path,
        typer.Option(help="dual-track forward enrollment registry path。"),
    ] = DEFAULT_WEIGHT_FORWARD_ENROLLMENT_PATH,
) -> None:
    """按 weight_set_id 登记单个或多个 shadow-ready weight candidates。"""
    if run_id is not None and latest:
        raise typer.BadParameter("--run-id and --latest cannot be combined")
    if run_id is None and not latest:
        raise typer.BadParameter("--run-id or --latest is required")
    if not weight_set:
        raise typer.BadParameter("--weight-set is required")
    run_dir = (
        find_latest_weight_search_run_dir(search_output_dir)
        if latest
        else search_output_dir / str(run_id)
    )
    search_payload = read_weight_search_run_payload(run_dir)
    source_paths = {"historical_search": str(run_dir / "summary.json")}
    if top_export_path is not None:
        source_paths["top_candidate_export"] = str(top_export_path)
    if comparison_path is not None:
        source_paths["comparison_table"] = str(comparison_path)
    enrollment = enroll_top_weight_candidates_forward(
        search_payload,
        top_export_payload=_load_optional_json_payload(top_export_path),
        comparison_payload=_load_optional_json_payload(comparison_path),
        source_paths=source_paths,
        enrollment_path=enrollment_path,
        weight_set_ids=weight_set,
    )
    _echo_weight_shadow_enrollment_summary(enrollment_path, enrollment)


@weight_calibration_app.command("aggregate-evidence")
def weight_calibration_aggregate_evidence_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="evidence aggregation date YYYY-MM-DD。"),
    ] = None,
    latest_search: Annotated[
        bool,
        typer.Option("--latest-search", help="读取最新 historical weight search run。"),
    ] = False,
    search_run_id: Annotated[
        str | None,
        typer.Option("--search-run-id", help="historical weight search run id。"),
    ] = None,
    search_output_dir: Annotated[
        Path,
        typer.Option(help="weight calibration search report 输出目录。"),
    ] = DEFAULT_ETF_WEIGHT_CALIBRATION_REPORT_DIR,
    candidate_registry_path: Annotated[
        Path,
        typer.Option(help="candidate initial weight set registry path。"),
    ] = DEFAULT_CANDIDATE_WEIGHT_REGISTRY_PATH,
    enrollment_path: Annotated[
        Path,
        typer.Option(help="dual-track forward enrollment registry path。"),
    ] = DEFAULT_WEIGHT_FORWARD_ENROLLMENT_PATH,
    forward_dashboard_path: Annotated[
        Path | None,
        typer.Option(help="optional ETF forward dashboard JSON path。"),
    ] = None,
    weekly_review_path: Annotated[
        Path | None,
        typer.Option(help="optional ETF weekly review JSON path。"),
    ] = None,
    decision_journal_path: Annotated[
        Path | None,
        typer.Option(help="optional ETF decision journal report JSON path。"),
    ] = None,
    parameter_review_path: Annotated[
        Path | None,
        typer.Option(help="optional ETF parameter review evidence/report JSON path。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option(help="backtest-vs-forward evidence 输出目录。"),
    ] = DEFAULT_WEIGHT_FORWARD_EVIDENCE_DIR,
) -> None:
    """聚合 TRADING-071F backtest expectation vs forward evidence。"""
    if search_run_id is not None and latest_search:
        raise typer.BadParameter("--search-run-id and --latest-search cannot be combined")
    run_date = _parse_date(as_of) if as_of else date.today()
    search_payload = None
    source_paths: dict[str, str] = {
        "candidate_registry": str(candidate_registry_path),
        "forward_enrollment": str(enrollment_path),
    }
    if search_run_id is not None or latest_search:
        run_dir = (
            find_latest_weight_search_run_dir(search_output_dir)
            if latest_search
            else search_output_dir / str(search_run_id)
        )
        search_payload = read_weight_search_run_payload(run_dir)
        source_paths["historical_search"] = str(run_dir / "summary.json")
    for key, path in {
        "forward_dashboard": forward_dashboard_path,
        "weekly_review": weekly_review_path,
        "decision_journal": decision_journal_path,
        "parameter_review": parameter_review_path,
    }.items():
        if path is not None:
            source_paths[key] = str(path)
    payload = build_backtest_forward_evidence_aggregation(
        as_of=run_date,
        candidate_registry=load_candidate_weight_registry(candidate_registry_path),
        forward_enrollments=load_weight_forward_enrollments(enrollment_path),
        search_payload=search_payload,
        forward_dashboard=_load_optional_json_payload(forward_dashboard_path),
        weekly_review=_load_optional_json_payload(weekly_review_path),
        decision_journal=_load_optional_json_payload(decision_journal_path),
        parameter_review=_load_optional_json_payload(parameter_review_path),
        source_paths=source_paths,
    )
    paths = write_backtest_forward_evidence_aggregation(payload, output_dir=output_dir)
    typer.echo(f"ETF weight backtest-forward evidence：{paths['markdown']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"evidence_record_count={payload['evidence_record_count']}")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@weight_calibration_app.command("overfit-diagnostics")
def weight_calibration_overfit_diagnostics_command(
    latest_search: Annotated[
        bool,
        typer.Option("--latest-search", help="读取最新 historical weight search run。"),
    ] = False,
    search_run_id: Annotated[
        str | None,
        typer.Option("--search-run-id", help="historical weight search run id。"),
    ] = None,
    search_output_dir: Annotated[
        Path,
        typer.Option(help="weight calibration search report 输出目录。"),
    ] = DEFAULT_ETF_WEIGHT_CALIBRATION_REPORT_DIR,
    candidate_registry_path: Annotated[
        Path,
        typer.Option(help="candidate initial weight set registry path。"),
    ] = DEFAULT_CANDIDATE_WEIGHT_REGISTRY_PATH,
    evidence_path: Annotated[
        Path | None,
        typer.Option(help="optional TRADING-071F evidence JSON path。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option(help="overfit diagnostics 输出目录。"),
    ] = DEFAULT_WEIGHT_OVERFIT_DIAGNOSTICS_DIR,
) -> None:
    """生成 TRADING-071G overfit risk and stability diagnostics。"""
    if search_run_id is not None and latest_search:
        raise typer.BadParameter("--search-run-id and --latest-search cannot be combined")
    search_payload = None
    if search_run_id is not None or latest_search:
        run_dir = (
            find_latest_weight_search_run_dir(search_output_dir)
            if latest_search
            else search_output_dir / str(search_run_id)
        )
        search_payload = read_weight_search_run_payload(run_dir)
    payload = build_weight_overfit_diagnostics(
        candidate_registry=load_candidate_weight_registry(candidate_registry_path),
        search_payload=search_payload,
        evidence_payload=_load_optional_json_payload(evidence_path),
    )
    paths = write_weight_overfit_diagnostics(payload, output_dir=output_dir)
    typer.echo(f"ETF weight overfit diagnostics：{paths['markdown']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"candidate_count={payload['candidate_count']}")
    typer.echo(f"risk_counts={payload['risk_counts']}")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@weight_calibration_app.command("overfit-explain")
def weight_calibration_overfit_explain_command(
    run_id: Annotated[
        str | None,
        typer.Option("--run-id", help="historical weight search run id。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="读取最新 historical weight search run。"),
    ] = False,
    top: Annotated[
        int,
        typer.Option("--top", help="解释 ranking 前 N 个 candidates。"),
    ] = 10,
    search_output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="weight calibration search report 输出目录。"),
    ] = DEFAULT_ETF_WEIGHT_CALIBRATION_REPORT_DIR,
    top_export_path: Annotated[
        Path | None,
        typer.Option(help="optional TRADING-078B Top-N JSON path。"),
    ] = None,
    overfit_path: Annotated[
        Path | None,
        typer.Option(help="optional TRADING-071G overfit diagnostics JSON path。"),
    ] = None,
    explanation_dir: Annotated[
        Path,
        typer.Option("--explanation-dir", help="overfit explanation 输出目录。"),
    ] = DEFAULT_WEIGHT_OVERFIT_EXPLANATION_DIR,
) -> None:
    """生成 TRADING-078E human-readable overfit risk explanation。"""
    if run_id is not None and latest:
        raise typer.BadParameter("--run-id and --latest cannot be combined")
    if run_id is None and not latest:
        raise typer.BadParameter("--run-id or --latest is required")
    run_dir = (
        find_latest_weight_search_run_dir(search_output_dir)
        if latest
        else search_output_dir / str(run_id)
    )
    search_payload = read_weight_search_run_payload(run_dir)
    source_paths = {"historical_search": str(run_dir / "summary.json")}
    if top_export_path is not None:
        source_paths["top_candidate_export"] = str(top_export_path)
    if overfit_path is not None:
        source_paths["overfit_diagnostics"] = str(overfit_path)
    payload = build_weight_overfit_explanations(
        search_payload,
        top_export_payload=_load_optional_json_payload(top_export_path),
        overfit_payload=_load_optional_json_payload(overfit_path),
        top=top,
        source_paths=source_paths,
    )
    paths = write_weight_overfit_explanations(payload, output_dir=explanation_dir)
    typer.echo(f"ETF weight overfit explanation：{paths['markdown']}")
    typer.echo(f"json={paths['json']}")
    typer.echo(f"candidate_count={payload['candidate_count']}")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@weight_calibration_app.command("recommendation")
def weight_calibration_recommendation_command(
    run_id: Annotated[
        str | None,
        typer.Option("--run-id", help="historical weight search run id。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="读取最新 historical weight search run。"),
    ] = False,
    top: Annotated[
        int,
        typer.Option("--top", help="报告包含 ranking 前 N 个 candidates。"),
    ] = 10,
    search_output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="weight calibration search report 输出目录。"),
    ] = DEFAULT_ETF_WEIGHT_CALIBRATION_REPORT_DIR,
    top_export_path: Annotated[
        Path | None,
        typer.Option(help="optional TRADING-078B Top-N JSON path。"),
    ] = None,
    comparison_path: Annotated[
        Path | None,
        typer.Option(help="optional TRADING-078C comparison JSON path。"),
    ] = None,
    regime_robustness_path: Annotated[
        Path | None,
        typer.Option(help="optional TRADING-078D regime robustness JSON path。"),
    ] = None,
    overfit_explanation_path: Annotated[
        Path | None,
        typer.Option(help="optional TRADING-078E overfit explanation JSON path。"),
    ] = None,
    enrollment_path: Annotated[
        Path | None,
        typer.Option(help="optional TRADING-078F forward enrollment registry path。"),
    ] = None,
    report_dir: Annotated[
        Path,
        typer.Option("--report-dir", help="initial recommendation report 输出目录。"),
    ] = DEFAULT_WEIGHT_INITIAL_RECOMMENDATION_DIR,
) -> None:
    """生成 TRADING-078G initial ETF weight candidate recommendation report。"""
    if run_id is not None and latest:
        raise typer.BadParameter("--run-id and --latest cannot be combined")
    if run_id is None and not latest:
        raise typer.BadParameter("--run-id or --latest is required")
    run_dir = (
        find_latest_weight_search_run_dir(search_output_dir)
        if latest
        else search_output_dir / str(run_id)
    )
    search_payload = read_weight_search_run_payload(run_dir)
    source_paths = {"historical_search": str(run_dir / "summary.json")}
    optional_sources = {
        "top_candidate_export": top_export_path,
        "comparison_table": comparison_path,
        "regime_robustness": regime_robustness_path,
        "overfit_explanation": overfit_explanation_path,
        "forward_enrollment": enrollment_path,
    }
    for key, path in optional_sources.items():
        if path is not None:
            source_paths[key] = str(path)
    payload = build_weight_initial_recommendation_report(
        search_payload,
        top_export_payload=_load_optional_json_payload(top_export_path),
        comparison_payload=_load_optional_json_payload(comparison_path),
        regime_robustness_payload=_load_optional_json_payload(regime_robustness_path),
        overfit_explanation_payload=_load_optional_json_payload(overfit_explanation_path),
        enrollment_payload=_load_optional_json_payload(enrollment_path),
        top=top,
        source_paths=source_paths,
    )
    paths = write_weight_initial_recommendation_report(payload, output_dir=report_dir)
    shadow = payload["shadow_enrollment_recommendations"]
    typer.echo(f"ETF initial weight recommendation report：{paths['markdown']}")
    typer.echo(f"json={paths['json']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"suggested_action={shadow['suggested_action']}")
    typer.echo(f"recommended_weight_set_ids={shadow['recommended_weight_set_ids']}")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@weight_calibration_app.command("diagnostics")
def weight_calibration_diagnostics_command(
    search: Annotated[
        list[str] | None,
        typer.Option("--search", help="weight search id，可重复。"),
    ] = None,
    include_robust_packs: Annotated[
        bool,
        typer.Option(
            "--include-robust-packs",
            help="同时运行 TRADING-079 bounded robust search packs。",
        ),
    ] = False,
    preset: Annotated[
        list[str] | None,
        typer.Option("--preset", help="historical range preset id，可重复。"),
    ] = None,
    top: Annotated[
        int,
        typer.Option("--top", help="每个 preset/search 保留 ranking 前 N 个 candidates。"),
    ] = 10,
    max_candidates: Annotated[
        int | None,
        typer.Option("--max-candidates", help="lower-than-config candidate evaluation cap。"),
    ] = None,
    search_config_path: Annotated[
        Path,
        typer.Option(help="weight search config YAML path。"),
    ] = DEFAULT_ETF_WEIGHT_SEARCH_CONFIG_PATH,
    preset_config_path: Annotated[
        Path,
        typer.Option(help="historical range preset config YAML path。"),
    ] = DEFAULT_WEIGHT_CALIBRATION_PRESET_CONFIG_PATH,
    prices_path: Annotated[
        Path,
        typer.Option(help="ETF 标准价格缓存路径。"),
    ] = DEFAULT_ETF_PRICE_PATH,
    output_dir: Annotated[
        Path,
        typer.Option(help="historical weight search diagnostics 输出目录。"),
    ] = DEFAULT_WEIGHT_SEARCH_DIAGNOSTICS_DIR,
    cache: Annotated[
        str,
        typer.Option(
            "--cache",
            help="diagnostics cache mode: read-write, read-only, or disabled。",
        ),
    ] = "read-write",
    no_cache: Annotated[
        bool,
        typer.Option("--no-cache", help="关闭 diagnostics cache。"),
    ] = False,
    force_refresh: Annotated[
        bool,
        typer.Option("--force-refresh", help="忽略可用 cache 并重算。"),
    ] = False,
    workers: Annotated[
        str,
        typer.Option("--workers", help="parallel worker count: auto 或正整数。"),
    ] = "auto",
    resume: Annotated[
        bool,
        typer.Option("--resume", help="按 run manifest / cache 尝试恢复 diagnostics run。"),
    ] = False,
    run_id: Annotated[
        str | None,
        typer.Option("--run-id", help="resume 或固定 run manifest id。"),
    ] = None,
    include_performance_report: Annotated[
        bool,
        typer.Option("--include-performance-report", help="写出 runtime performance report。"),
    ] = False,
    profile: Annotated[
        str,
        typer.Option("--profile", help="profiling mode: off, summary, detailed, or cprofile。"),
    ] = "summary",
    profile_output: Annotated[
        Path | None,
        typer.Option("--profile-output", help="profiling artifacts 输出目录。"),
    ] = None,
    profile_top_n: Annotated[
        int | None,
        typer.Option("--profile-top-n", help="profiling report Top-N rows。"),
    ] = None,
    cache_policy_path: Annotated[
        Path,
        typer.Option(help="weight calibration cache policy YAML path。"),
    ] = DEFAULT_WEIGHT_CALIBRATION_CACHE_POLICY_CONFIG_PATH,
    profiling_policy_path: Annotated[
        Path,
        typer.Option(help="weight calibration profiling policy YAML path。"),
    ] = DEFAULT_WEIGHT_CALIBRATION_PROFILING_POLICY_CONFIG_PATH,
    performance_report_dir: Annotated[
        Path,
        typer.Option(help="weight calibration performance report 输出目录。"),
    ] = DEFAULT_WEIGHT_CALIBRATION_PERFORMANCE_REPORT_DIR,
) -> None:
    """生成 TRADING-079 historical weight search diagnostics and rescue report。"""
    if resume and not run_id:
        typer.echo("--resume requires --run-id for auditable diagnostics resume。")
        raise typer.Exit(code=2)
    if profile_top_n is not None and profile_top_n <= 0:
        raise typer.BadParameter("--profile-top-n must be positive")
    config = load_etf_config_bundle()
    registry = load_weight_search_registry(search_config_path, etf_config=config)
    preset_registry = load_weight_calibration_preset_registry(
        preset_config_path,
        etf_config=config,
        weight_search_registry=registry,
    )
    cache_policy = load_weight_calibration_cache_policy_config(cache_policy_path)
    profiling_policy = load_weight_calibration_profiling_policy_config(profiling_policy_path)
    profile_mode = normalize_weight_calibration_profile_mode(profile, policy=profiling_policy)
    profile_settings = profiling_mode_settings(profiling_policy, profile_mode)
    cache_mode = "disabled" if no_cache else cache
    prices, quality_report = load_standard_prices(prices_path, config.assets, config.strategy)
    if not quality_report.passed:
        typer.echo(
            f"ETF 数据质量状态：{quality_report.status}，已停止 weight diagnostics。"
        )
        raise typer.Exit(code=1)
    selected_searches = list(search or ["etf_initial_weight_search_v1"])
    if include_robust_packs:
        selected_searches.extend(WEIGHT_ROBUST_SEARCH_PACK_IDS)
    selected_searches = list(dict.fromkeys(selected_searches))
    selected_presets = list(preset or WEIGHT_SEARCH_DIAGNOSTICS_DEFAULT_PRESETS)
    builder_kwargs = {
        "etf_config": config,
        "quality_report": quality_report,
        "registry": registry,
        "preset_registry": preset_registry,
        "search_ids": selected_searches,
        "preset_ids": selected_presets,
        "top": top,
        "max_candidates": max_candidates,
        "source_paths": {
            "weight_search_config": str(search_config_path),
            "weight_calibration_presets": str(preset_config_path),
            "prices": str(prices_path),
            "cache_policy": str(cache_policy_path),
            "profiling_policy": str(profiling_policy_path),
        },
        "cache_policy": cache_policy,
        "cache_mode": cache_mode,
        "force_refresh": force_refresh,
        "workers": workers,
        "resume_run_id": run_id if resume else None,
        "include_performance_report": include_performance_report,
        "profiling_policy": profiling_policy,
        "profile_mode": profile_mode,
    }
    payload, cprofile_profiler = run_with_optional_cprofile(
        build_historical_weight_search_diagnostics_report,
        prices,
        enabled=profile_settings.cprofile,
        **builder_kwargs,
    )
    paths = write_historical_weight_search_diagnostics_report(
        payload,
        output_dir=output_dir,
    )
    performance_paths = None
    if include_performance_report and isinstance(payload.get("performance_report"), dict):
        performance_paths = write_weight_calibration_performance_report(
            payload["performance_report"],
            output_dir=performance_report_dir,
        )
    profile_paths = None
    cprofile_paths = None
    candidate_hotspot_paths = None
    if profile_settings.enabled:
        run_manifest = payload.get("run_manifest") or {}
        profile_dir = profile_output or (
            DEFAULT_WEIGHT_CALIBRATION_PROFILING_REPORT_DIR
            / str(run_manifest.get("run_id") or "unknown_run")
        )
        if cprofile_profiler is not None:
            cprofile_paths = write_cprofile_artifacts(
                cprofile_profiler,
                output_dir=profile_dir,
                top_n=profile_top_n or profiling_policy.weight_calibration_profiling.top_n,
            )
        profiling_report = build_weight_calibration_profiling_report(
            payload,
            policy=profiling_policy,
            profile_mode=profile_mode,
            profile_top_n=profile_top_n,
            cprofile_artifacts=cprofile_paths,
        )
        profile_paths = write_weight_calibration_profiling_report(
            profiling_report,
            output_dir=profile_dir,
        )
        candidate_hotspot_paths = write_weight_calibration_candidate_hotspot_table(
            profiling_report["candidate_hotspots"],
            output_dir=profile_dir,
        )
    criteria = payload["shadow_minimum_criteria"]
    cache_summary = payload.get("cache_summary") or {}
    typer.echo(f"ETF historical weight search diagnostics：{paths['markdown']}")
    typer.echo(f"json={paths['json']}")
    typer.echo(f"stable_shapes_csv={paths['stable_shapes_csv']}")
    typer.echo(f"near_shadow_csv={paths['near_shadow_csv']}")
    if performance_paths is not None:
        typer.echo(f"performance_report={performance_paths['markdown']}")
        typer.echo(f"performance_json={performance_paths['json']}")
    if profile_paths is not None:
        typer.echo(f"profiling_report={profile_paths['markdown']}")
        typer.echo(f"profiling_json={profile_paths['json']}")
        typer.echo(f"candidate_hotspots={candidate_hotspot_paths['markdown']}")
    if cprofile_paths is not None:
        typer.echo(f"cprofile_stats={cprofile_paths['stats']}")
        typer.echo(f"cprofile_top_functions={cprofile_paths['markdown']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"preset_result_count={payload['preset_result_count']}")
    typer.echo(f"candidate_observation_count={payload['candidate_observation_count']}")
    typer.echo(f"shadow_ready_count={criteria['shadow_ready_count']}")
    typer.echo(f"minimum_criteria_status={criteria['status']}")
    typer.echo(f"cache_mode={cache_summary.get('cache_mode', cache_mode)}")
    typer.echo(
        "price_returns_matrix_cache_status="
        f"{cache_summary.get('price_returns_matrix_cache_status', 'not_reported')}"
    )
    typer.echo(
        "diagnostics_aggregation_cache_status="
        f"{cache_summary.get('diagnostics_aggregation_cache_status', 'not_reported')}"
    )
    typer.echo(f"cache_hit_count={cache_summary.get('cache_hit_count', 0)}")
    typer.echo(f"cache_miss_count={cache_summary.get('cache_miss_count', 0)}")
    typer.echo(f"cache_write_count={cache_summary.get('cache_write_count', 0)}")
    typer.echo(f"resume_status={cache_summary.get('resume_status', 'not_reported')}")
    typer.echo(f"worker_count={cache_summary.get('worker_count', workers)}")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@weight_calibration_app.command("performance-validate")
def weight_calibration_performance_validate_command(
    cache_policy_path: Annotated[
        Path,
        typer.Option(help="weight calibration cache policy YAML path。"),
    ] = DEFAULT_WEIGHT_CALIBRATION_CACHE_POLICY_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option(help="cache / parallel validation report 输出目录。"),
    ] = DEFAULT_WEIGHT_CALIBRATION_CACHE_VALIDATION_DIR,
) -> None:
    """校验 TRADING-080 cache、parallel runner、resume 和 performance safety gate。"""
    payload = build_weight_calibration_cache_parallel_validation_report(
        policy_config_path=cache_policy_path,
    )
    paths = write_weight_calibration_cache_parallel_validation_report(
        payload,
        output_dir=output_dir,
    )
    typer.echo(f"ETF weight calibration cache / parallel validation：{paths['markdown']}")
    typer.echo(f"json={paths['json']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@weight_calibration_app.command("profiling-validate")
def weight_calibration_profiling_validate_command(
    profiling_policy_path: Annotated[
        Path,
        typer.Option(help="weight calibration profiling policy YAML path。"),
    ] = DEFAULT_WEIGHT_CALIBRATION_PROFILING_POLICY_CONFIG_PATH,
    report_registry_path: Annotated[
        Path,
        typer.Option(help="report registry YAML path。"),
    ] = DEFAULT_REPORT_REGISTRY_PATH,
    output_dir: Annotated[
        Path,
        typer.Option(help="profiling validation report 输出目录。"),
    ] = DEFAULT_WEIGHT_CALIBRATION_PROFILING_VALIDATION_DIR,
) -> None:
    """校验 TRADING-081 profiling workflow 和 safety boundary。"""
    payload = build_weight_calibration_profiling_validation_report(
        policy_config_path=profiling_policy_path,
        report_registry_path=report_registry_path,
    )
    paths = write_weight_calibration_profiling_validation_report(
        payload,
        output_dir=output_dir,
    )
    typer.echo(f"ETF weight calibration profiling validation：{paths['markdown']}")
    typer.echo(f"json={paths['json']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@weight_calibration_app.command("generate-proposals")
def weight_calibration_generate_proposals_command(
    candidate_registry_path: Annotated[
        Path,
        typer.Option(help="candidate initial weight set registry path。"),
    ] = DEFAULT_CANDIDATE_WEIGHT_REGISTRY_PATH,
    evidence_path: Annotated[
        Path | None,
        typer.Option(help="optional TRADING-071F evidence JSON path。"),
    ] = None,
    overfit_path: Annotated[
        Path | None,
        typer.Option(help="optional TRADING-071G overfit diagnostics JSON path。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option(help="candidate weight proposal 输出目录。"),
    ] = DEFAULT_WEIGHT_PROPOSAL_DIR,
) -> None:
    """生成 TRADING-071H candidate weight proposal-only recommendations。"""
    payload = build_candidate_weight_proposals(
        candidate_registry=load_candidate_weight_registry(candidate_registry_path),
        evidence_payload=_load_optional_json_payload(evidence_path),
        overfit_payload=_load_optional_json_payload(overfit_path),
    )
    paths = write_candidate_weight_proposals(payload, output_dir=output_dir)
    typer.echo(f"ETF weight candidate proposals：{paths['markdown']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"proposal_count={payload['proposal_count']}")
    typer.echo(f"proposal_type_counts={payload['proposal_type_counts']}")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@weight_calibration_app.command("report")
def weight_calibration_report_command(
    latest: Annotated[
        bool,
        typer.Option(
            "--latest",
            help="读取最新 historical/evidence/diagnostics/proposal artifacts。",
        ),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option(help="报告日期；默认使用今天。"),
    ] = None,
    search_run_id: Annotated[
        str | None,
        typer.Option("--search-run-id", help="historical weight search run id。"),
    ] = None,
    search_output_dir: Annotated[
        Path,
        typer.Option(help="weight calibration search report 输出目录。"),
    ] = DEFAULT_ETF_WEIGHT_CALIBRATION_REPORT_DIR,
    candidate_registry_path: Annotated[
        Path,
        typer.Option(help="candidate initial weight set registry path。"),
    ] = DEFAULT_CANDIDATE_WEIGHT_REGISTRY_PATH,
    enrollment_path: Annotated[
        Path,
        typer.Option(help="weight calibration forward enrollment ledger path。"),
    ] = DEFAULT_WEIGHT_FORWARD_ENROLLMENT_PATH,
    evidence_path: Annotated[
        Path | None,
        typer.Option(help="TRADING-071F evidence JSON path；`--latest` 时可省略。"),
    ] = None,
    overfit_path: Annotated[
        Path | None,
        typer.Option(help="TRADING-071G overfit diagnostics JSON path；`--latest` 时可省略。"),
    ] = None,
    proposals_path: Annotated[
        Path | None,
        typer.Option(help="TRADING-071H proposal JSON path；`--latest` 时可省略。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option(help="dual-track calibration report 输出目录。"),
    ] = DEFAULT_WEIGHT_DUAL_TRACK_REPORT_DIR,
) -> None:
    """生成 TRADING-071I dual-track calibration JSON/Markdown report。"""
    if search_run_id is not None and latest:
        raise typer.BadParameter("--search-run-id and --latest cannot be combined")
    report_date = _parse_date(as_of) if as_of else date.today()
    source_paths: dict[str, str] = {
        "candidate_registry": str(candidate_registry_path),
        "forward_enrollment": str(enrollment_path),
    }
    search_payload = None
    if search_run_id is not None or latest:
        run_dir = (
            find_latest_weight_search_run_dir(search_output_dir)
            if latest
            else search_output_dir / str(search_run_id)
        )
        search_payload = read_weight_search_run_payload(run_dir)
        source_paths["historical_search"] = str(run_dir / "summary.json")

    resolved_evidence_path = evidence_path or (
        _latest_json_file(DEFAULT_WEIGHT_FORWARD_EVIDENCE_DIR, "backtest_forward_evidence_*.json")
        if latest
        else None
    )
    resolved_overfit_path = overfit_path or (
        _latest_json_file(DEFAULT_WEIGHT_OVERFIT_DIAGNOSTICS_DIR, "overfit_diagnostics_*.json")
        if latest
        else None
    )
    resolved_proposals_path = proposals_path or (
        _latest_json_file(DEFAULT_WEIGHT_PROPOSAL_DIR, "candidate_weight_proposals_*.json")
        if latest
        else None
    )
    for key, path in {
        "backtest_forward_evidence": resolved_evidence_path,
        "overfit_diagnostics": resolved_overfit_path,
        "candidate_weight_proposals": resolved_proposals_path,
    }.items():
        if path is not None:
            source_paths[key] = str(path)

    payload = build_dual_track_weight_calibration_report(
        as_of=report_date,
        candidate_registry=load_candidate_weight_registry(candidate_registry_path),
        forward_enrollments=load_weight_forward_enrollments(enrollment_path),
        search_payload=search_payload,
        evidence_payload=_load_optional_json_payload(resolved_evidence_path),
        overfit_payload=_load_optional_json_payload(resolved_overfit_path),
        proposals_payload=_load_optional_json_payload(resolved_proposals_path),
        source_paths=source_paths,
    )
    paths = write_dual_track_weight_calibration_report(payload, output_dir=output_dir)
    typer.echo(f"ETF weight dual-track calibration report：{paths['markdown']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"candidate_count={payload['summary']['candidate_count']}")
    typer.echo(f"proposal_count={payload['summary']['proposal_count']}")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@weight_calibration_app.command("validate")
def weight_calibration_validate_command(
    search_config_path: Annotated[
        Path,
        typer.Option(help="weight search config YAML path。"),
    ] = DEFAULT_ETF_WEIGHT_SEARCH_CONFIG_PATH,
    report_registry_path: Annotated[
        Path,
        typer.Option(help="report registry YAML path。"),
    ] = DEFAULT_REPORT_REGISTRY_PATH,
    proposals_path: Annotated[
        Path | None,
        typer.Option(help="optional TRADING-071H proposal JSON path。"),
    ] = None,
    report_path: Annotated[
        Path | None,
        typer.Option(help="optional TRADING-071I dual-track calibration report JSON path。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option(help="dual-track calibration validation 输出目录。"),
    ] = DEFAULT_WEIGHT_CALIBRATION_VALIDATION_DIR,
) -> None:
    """执行 TRADING-071K dual-track calibration validation gate。"""
    payload = build_dual_track_weight_calibration_validation_report(
        search_config_path=search_config_path,
        report_registry_path=report_registry_path,
        proposals_payload=(
            _load_optional_json_payload(proposals_path) if proposals_path is not None else None
        ),
        report_payload=(
            _load_optional_json_payload(report_path) if report_path is not None else None
        ),
    )
    paths = write_dual_track_weight_calibration_validation_report(
        payload,
        output_dir=output_dir,
    )
    typer.echo(f"ETF weight dual-track calibration validation gate：{paths['markdown']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@weight_calibration_app.command("usability-validate")
def weight_calibration_usability_validate_command(
    search_config_path: Annotated[
        Path,
        typer.Option(help="weight search config YAML path。"),
    ] = DEFAULT_ETF_WEIGHT_SEARCH_CONFIG_PATH,
    preset_config_path: Annotated[
        Path,
        typer.Option(help="historical range preset config YAML path。"),
    ] = DEFAULT_WEIGHT_CALIBRATION_PRESET_CONFIG_PATH,
    report_registry_path: Annotated[
        Path,
        typer.Option(help="report registry YAML path。"),
    ] = DEFAULT_REPORT_REGISTRY_PATH,
    recommendation_path: Annotated[
        Path | None,
        typer.Option(help="optional TRADING-078G recommendation report JSON path。"),
    ] = None,
    enrollment_path: Annotated[
        Path | None,
        typer.Option(help="optional TRADING-078F forward enrollment registry JSON path。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option(help="historical calibration usability validation 输出目录。"),
    ] = DEFAULT_WEIGHT_CALIBRATION_VALIDATION_DIR,
) -> None:
    """执行 TRADING-078I historical calibration usability validation gate。"""
    payload = build_historical_weight_calibration_usability_validation_report(
        search_config_path=search_config_path,
        preset_config_path=preset_config_path,
        report_registry_path=report_registry_path,
        recommendation_payload=(
            _load_optional_json_payload(recommendation_path)
            if recommendation_path is not None
            else None
        ),
        enrollment_payload=(
            _load_optional_json_payload(enrollment_path) if enrollment_path is not None else None
        ),
    )
    paths = write_historical_weight_calibration_usability_validation_report(
        payload,
        output_dir=output_dir,
    )
    typer.echo(f"ETF historical weight calibration usability validation gate：{paths['markdown']}")
    typer.echo(f"json={paths['json']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@p2_app.command("edgar-text")
def p2_edgar_text_command(
    input_path: Annotated[Path | None, typer.Option(help="EDGAR/text audit input CSV。")] = None,
    date_option: Annotated[str | None, typer.Option("--date", help="日期或 latest。")] = None,
    output_dir: Annotated[Path, typer.Option(help="输出目录。")] = DEFAULT_ETF_REPORT_DIR / "p2",
) -> None:
    """生成 P2 EDGAR / 财报文本解释层 contract report；observe-only。"""
    _write_p2_source_contract(
        source_id="edgar_text",
        title="ETF P2 EDGAR Text Contract",
        input_path=input_path,
        date_option=date_option,
        output_dir=output_dir,
    )


@p2_app.command("derive-edgar-events")
def p2_derive_edgar_events_command(
    timeline_path: Annotated[
        Path,
        typer.Option(help="SEC PIT filing timeline CSV/Parquet。"),
    ] = PROJECT_ROOT / "data" / "processed" / "sec_edgar" / "filing_timeline.csv",
    output_path: Annotated[
        Path | None,
        typer.Option(help="Canonical edgar_text_events 输出路径，默认读取 p2.yaml。"),
    ] = None,
    manifest_path: Annotated[Path, typer.Option(help="P2 source manifest 输出路径。")] = (
        DEFAULT_ETF_P2_MANIFEST_PATH
    ),
    date_option: Annotated[str | None, typer.Option("--date", help="日期或 latest。")] = None,
    symbols: Annotated[
        list[str] | None,
        typer.Option("--symbol", help="只派生指定 ticker，可重复。"),
    ] = None,
    output_dir: Annotated[Path, typer.Option(help="派生报告输出目录。")] = (
        DEFAULT_ETF_REPORT_DIR / "p2"
    ),
) -> None:
    """从本地 SEC PIT filing timeline 派生 P2 EDGAR metadata events；observe-only。"""
    config = load_etf_config_bundle()
    p2_config = _require_p2_config(config)
    source = p2_config.sources["edgar_text"]
    run_date = _resolve_p2_source_date(date_option, timeline_path, "available_for_signal_date")
    frame = derive_edgar_text_events_from_timeline(
        source=source,
        timeline_path=timeline_path,
        output_path=output_path,
        manifest_path=manifest_path,
        run_date=run_date,
        symbols=symbols,
    )
    _write_p2_frame(
        frame,
        output_dir,
        run_date,
        "derive_edgar_events",
        "ETF P2 Derive EDGAR Events",
        config,
    )


@p2_app.command("fetch-edgar-text")
def p2_fetch_edgar_text_command(
    timeline_path: Annotated[
        Path,
        typer.Option(help="SEC PIT filing timeline CSV/Parquet。"),
    ] = PROJECT_ROOT / "data" / "processed" / "sec_edgar" / "filing_timeline.csv",
    document_dir: Annotated[
        Path,
        typer.Option(help="SEC filing 文本缓存目录。"),
    ] = PROJECT_ROOT / "data" / "etf_portfolio" / "p2" / "edgar_text_documents",
    output_path: Annotated[
        Path,
        typer.Option(help="EDGAR text document index 输出路径。"),
    ] = PROJECT_ROOT / "data" / "etf_portfolio" / "p2" / "edgar_text_documents.csv",
    manifest_path: Annotated[Path, typer.Option(help="P2 source manifest 输出路径。")] = (
        DEFAULT_ETF_P2_MANIFEST_PATH
    ),
    date_option: Annotated[str | None, typer.Option("--date", help="日期或 latest。")] = None,
    symbols: Annotated[
        list[str] | None,
        typer.Option("--symbol", help="只获取指定 ticker，可重复。"),
    ] = None,
    filing_types: Annotated[
        list[str] | None,
        typer.Option("--filing-type", help="只获取指定 SEC form，可重复，例如 10-K/10-Q/8-K。"),
    ] = None,
    limit: Annotated[int, typer.Option(help="本次最多获取的 filing 数。", min=1)] = 5,
    user_agent: Annotated[
        str | None,
        typer.Option(help="SEC User-Agent；不填则读取 SEC_USER_AGENT。"),
    ] = None,
    output_dir: Annotated[Path, typer.Option(help="文本获取报告输出目录。")] = (
        DEFAULT_ETF_REPORT_DIR / "p2"
    ),
) -> None:
    """从 SEC PIT filing timeline 获取有限 official filing 文本；observe-only。"""
    config = load_etf_config_bundle()
    p2_config = _require_p2_config(config)
    source = p2_config.sources["edgar_text"]
    run_date = _resolve_p2_source_date(date_option, timeline_path, "available_for_signal_date")
    frame = fetch_edgar_text_documents_from_timeline(
        source=source,
        timeline_path=timeline_path,
        document_dir=document_dir,
        output_path=output_path,
        manifest_path=manifest_path,
        run_date=run_date,
        symbols=symbols,
        filing_types=filing_types,
        limit=limit,
        user_agent=user_agent,
    )
    _write_p2_frame(
        frame,
        output_dir,
        run_date,
        "fetch_edgar_text",
        "ETF P2 Fetch EDGAR Text Documents",
        config,
    )


@p2_app.command("edgar-topics")
def p2_edgar_topics_command(
    input_path: Annotated[
        Path,
        typer.Option(help="EDGAR text document index CSV。"),
    ] = PROJECT_ROOT / "data" / "etf_portfolio" / "p2" / "edgar_text_documents.csv",
    date_option: Annotated[str | None, typer.Option("--date", help="日期或 latest。")] = None,
    output_dir: Annotated[Path, typer.Option(help="EDGAR topic audit 输出目录。")] = (
        DEFAULT_ETF_REPORT_DIR / "p2"
    ),
) -> None:
    """对已缓存 EDGAR filing text 做受治理关键词 topic audit；observe-only。"""
    config = load_etf_config_bundle()
    p2_config = _require_p2_config(config)
    document_index = pd.read_csv(input_path)
    run_date = _resolve_p2_source_date(date_option, input_path, "as_of")
    frame = build_edgar_text_topic_audit(
        document_index=document_index,
        p2_config=p2_config,
        run_date=run_date,
    )
    _write_p2_frame(
        frame,
        output_dir,
        run_date,
        "edgar_topics",
        "ETF P2 EDGAR Text Topic Audit",
        config,
    )


@p2_app.command("import-source")
def p2_import_source_command(
    source_id: Annotated[
        str,
        typer.Argument(help="P2 source id: edgar_text/news_themes/options_iv_skew/etf_holdings。"),
    ],
    input_path: Annotated[Path, typer.Option(help="待导入的审计 CSV/Parquet。")],
    output_path: Annotated[
        Path | None,
        typer.Option(help="Canonical 输出路径，默认读取 p2.yaml。"),
    ] = None,
    manifest_path: Annotated[Path, typer.Option(help="P2 source manifest 输出路径。")] = (
        DEFAULT_ETF_P2_MANIFEST_PATH
    ),
    provider: Annotated[str, typer.Option(help="Provider name。")] = "manual_input",
    source_url: Annotated[str, typer.Option(help="Source URL 或人工输入说明。")] = "manual_input",
    output_dir: Annotated[Path, typer.Option(help="导入报告输出目录。")] = (
        DEFAULT_ETF_REPORT_DIR / "p2"
    ),
) -> None:
    """导入 P2 本地审计输入，写 canonical CSV 和 source manifest。"""
    config = load_etf_config_bundle()
    p2_config = _require_p2_config(config)
    if source_id not in p2_config.sources:
        raise typer.BadParameter(f"未知 P2 source_id：{source_id}")
    source = p2_config.sources[source_id]
    frame = import_p2_source(
        source_id=source_id,
        source=source,
        input_path=input_path,
        output_path=output_path,
        manifest_path=manifest_path,
        provider=provider,
        source_url=source_url,
        request_params={"input_path": str(input_path)},
    )
    run_date = date.today()
    _write_p2_frame(
        frame,
        output_dir,
        run_date,
        f"import_{source_id}",
        f"ETF P2 Import {source_id}",
        config,
    )


@p2_app.command("normalize-holdings")
def p2_normalize_holdings_command(
    input_path: Annotated[Path, typer.Option(help="Issuer/vendor/manual holdings CSV/Parquet。")],
    etf_symbol: Annotated[str, typer.Option(help="ETF symbol，例如 SMH、SOXX、QQQ。")],
    date_option: Annotated[str, typer.Option("--date", help="Holdings as-of 日期 YYYY-MM-DD。")],
    output_path: Annotated[
        Path | None,
        typer.Option(help="Canonical holdings 输出路径，默认读取 p2.yaml。"),
    ] = None,
    manifest_path: Annotated[Path, typer.Option(help="P2 source manifest 输出路径。")] = (
        DEFAULT_ETF_P2_MANIFEST_PATH
    ),
    provider: Annotated[str, typer.Option(help="Provider / issuer name。")] = "manual_input",
    source_url: Annotated[str, typer.Option(help="Source URL 或人工输入说明。")] = "manual_input",
    downloaded_at: Annotated[
        str | None,
        typer.Option(help="下载或接收时间 ISO-8601；默认当前 UTC。"),
    ] = None,
    holding_symbol_column: Annotated[
        str | None,
        typer.Option(help="持仓 symbol 列名；不填则识别常见列名。"),
    ] = None,
    holding_weight_column: Annotated[
        str | None,
        typer.Option(help="持仓权重列名；不填则识别常见列名。"),
    ] = None,
    output_dir: Annotated[Path, typer.Option(help="规范化报告输出目录。")] = (
        DEFAULT_ETF_REPORT_DIR / "p2"
    ),
) -> None:
    """规范化 ETF holdings 输入为 canonical holdings；observe-only。"""
    config = load_etf_config_bundle()
    p2_config = _require_p2_config(config)
    source = p2_config.sources["etf_holdings"]
    run_date = _parse_date(date_option)
    frame = normalize_etf_holdings_source(
        source=source,
        input_path=input_path,
        etf_symbol=etf_symbol,
        provider=provider,
        source_url=source_url,
        as_of=run_date,
        output_path=output_path,
        manifest_path=manifest_path,
        downloaded_at=_parse_datetime(downloaded_at) if downloaded_at else None,
        holding_symbol_column=holding_symbol_column,
        holding_weight_column=holding_weight_column,
    )
    _write_p2_frame(
        frame,
        output_dir,
        run_date,
        f"normalize_holdings_{etf_symbol.upper()}",
        f"ETF P2 Normalize Holdings {etf_symbol.upper()}",
        config,
    )


@p2_app.command("news-themes")
def p2_news_themes_command(
    input_path: Annotated[Path | None, typer.Option(help="News theme audit input CSV。")] = None,
    date_option: Annotated[str | None, typer.Option("--date", help="日期或 latest。")] = None,
    output_dir: Annotated[Path, typer.Option(help="输出目录。")] = DEFAULT_ETF_REPORT_DIR / "p2",
) -> None:
    """生成 P2 新闻摘要和主题事件追踪 report；observe-only。"""
    config = load_etf_config_bundle()
    p2_config = _require_p2_config(config)
    source = p2_config.sources["news_themes"]
    path = input_path or _p2_input_path(source.input_path)
    run_date = _resolve_p2_source_date(date_option, path, source.as_of_column)
    frame = build_news_theme_tracking_report(
        source=source,
        p2_config=p2_config,
        run_date=run_date,
        input_path=path,
    )
    _write_p2_frame(
        frame,
        output_dir,
        run_date,
        "news_themes",
        "ETF P2 News Theme Tracking",
        config,
    )


@p2_app.command("normalize-news")
def p2_normalize_news_command(
    input_path: Annotated[Path, typer.Option(help="Vendor/manual news theme CSV/Parquet。")],
    output_path: Annotated[
        Path | None,
        typer.Option(help="Canonical news_theme_events 输出路径，默认读取 p2.yaml。"),
    ] = None,
    manifest_path: Annotated[Path, typer.Option(help="P2 source manifest 输出路径。")] = (
        DEFAULT_ETF_P2_MANIFEST_PATH
    ),
    provider: Annotated[str, typer.Option(help="Provider / audit source name。")] = (
        "manual_input"
    ),
    source_url: Annotated[str, typer.Option(help="Source URL 或人工输入说明。")] = (
        "manual_input"
    ),
    downloaded_at: Annotated[
        str | None,
        typer.Option(help="下载或接收时间 ISO-8601；默认当前 UTC。"),
    ] = None,
    symbol_column: Annotated[str | None, typer.Option(help="symbol 列名。")] = None,
    theme_column: Annotated[str | None, typer.Option(help="theme/topic 列名。")] = None,
    summary_column: Annotated[str | None, typer.Option(help="summary/headline 列名。")] = None,
    published_at_column: Annotated[str | None, typer.Option(help="published_at 列名。")] = None,
    available_at_column: Annotated[str | None, typer.Option(help="available_at 列名。")] = None,
    sentiment_column: Annotated[str | None, typer.Option(help="sentiment_score 列名。")] = None,
    relevance_column: Annotated[str | None, typer.Option(help="relevance_score 列名。")] = None,
    output_dir: Annotated[Path, typer.Option(help="规范化报告输出目录。")] = (
        DEFAULT_ETF_REPORT_DIR / "p2"
    ),
) -> None:
    """规范化 P2 news/theme 输入；observe-only，不调用 LLM。"""
    config = load_etf_config_bundle()
    p2_config = _require_p2_config(config)
    source = p2_config.sources["news_themes"]
    frame = normalize_news_theme_source(
        source=source,
        p2_config=p2_config,
        input_path=input_path,
        provider=provider,
        source_url=source_url,
        output_path=output_path,
        manifest_path=manifest_path,
        downloaded_at=_parse_datetime(downloaded_at) if downloaded_at else None,
        symbol_column=symbol_column,
        theme_column=theme_column,
        summary_column=summary_column,
        published_at_column=published_at_column,
        available_at_column=available_at_column,
        sentiment_column=sentiment_column,
        relevance_column=relevance_column,
    )
    run_date = _resolve_frame_date("latest", frame, "downloaded_at")
    _write_p2_frame(
        frame,
        output_dir,
        run_date,
        "normalize_news_themes",
        "ETF P2 Normalize News Themes",
        config,
    )


@p2_app.command("options-risk")
def p2_options_risk_command(
    input_path: Annotated[
        Path | None,
        typer.Option(help="Options IV/skew audit input CSV。"),
    ] = None,
    date_option: Annotated[str | None, typer.Option("--date", help="日期或 latest。")] = None,
    output_dir: Annotated[Path, typer.Option(help="输出目录。")] = DEFAULT_ETF_REPORT_DIR / "p2",
) -> None:
    """生成 P2 期权 IV / VXN / skew 过滤 contract report；observe-only。"""
    _write_p2_source_contract(
        source_id="options_iv_skew",
        title="ETF P2 Options IV Skew Contract",
        input_path=input_path,
        date_option=date_option,
        output_dir=output_dir,
    )


@p2_app.command("normalize-options-risk")
def p2_normalize_options_risk_command(
    input_path: Annotated[Path, typer.Option(help="Vendor/manual options IV/VXN/skew CSV。")],
    output_path: Annotated[
        Path | None,
        typer.Option(help="Canonical options_iv_skew 输出路径，默认读取 p2.yaml。"),
    ] = None,
    manifest_path: Annotated[Path, typer.Option(help="P2 source manifest 输出路径。")] = (
        DEFAULT_ETF_P2_MANIFEST_PATH
    ),
    provider: Annotated[str, typer.Option(help="Provider / audit source name。")] = (
        "manual_input"
    ),
    source_url: Annotated[str, typer.Option(help="Source URL 或人工输入说明。")] = (
        "manual_input"
    ),
    downloaded_at: Annotated[
        str | None,
        typer.Option(help="下载或接收时间 ISO-8601；默认当前 UTC。"),
    ] = None,
    symbol_column: Annotated[str | None, typer.Option(help="symbol/ticker 列名。")] = None,
    as_of_column: Annotated[str | None, typer.Option(help="as_of/date 列名。")] = None,
    available_at_column: Annotated[str | None, typer.Option(help="available_at 列名。")] = None,
    iv_rank_column: Annotated[str | None, typer.Option(help="iv_rank 列名。")] = None,
    skew_zscore_column: Annotated[str | None, typer.Option(help="skew_zscore 列名。")] = None,
    vxn_level_column: Annotated[str | None, typer.Option(help="vxn_level 列名。")] = None,
    risk_flag_column: Annotated[str | None, typer.Option(help="可选 risk_flag 列名。")] = None,
    output_dir: Annotated[Path, typer.Option(help="规范化报告输出目录。")] = (
        DEFAULT_ETF_REPORT_DIR / "p2"
    ),
) -> None:
    """规范化 P2 options IV/VXN/skew 输入；observe-only，不下载 provider。"""
    config = load_etf_config_bundle()
    p2_config = _require_p2_config(config)
    source = p2_config.sources["options_iv_skew"]
    frame = normalize_options_risk_source(
        source=source,
        p2_config=p2_config,
        input_path=input_path,
        provider=provider,
        source_url=source_url,
        output_path=output_path,
        manifest_path=manifest_path,
        downloaded_at=_parse_datetime(downloaded_at) if downloaded_at else None,
        symbol_column=symbol_column,
        as_of_column=as_of_column,
        available_at_column=available_at_column,
        iv_rank_column=iv_rank_column,
        skew_zscore_column=skew_zscore_column,
        vxn_level_column=vxn_level_column,
        risk_flag_column=risk_flag_column,
    )
    run_date = _resolve_frame_date("latest", frame, "downloaded_at")
    _write_p2_frame(
        frame,
        output_dir,
        run_date,
        "normalize_options_risk",
        "ETF P2 Normalize Options Risk",
        config,
    )


@p2_app.command("derive-options-risk")
def p2_derive_options_risk_command(
    prices_path: Annotated[Path, typer.Option(help="含 ^VIX 的市场价格缓存路径。")] = (
        DEFAULT_ETF_PRICE_PATH
    ),
    output_path: Annotated[
        Path | None,
        typer.Option(help="Canonical options_iv_skew 输出路径，默认读取 p2.yaml。"),
    ] = None,
    manifest_path: Annotated[Path, typer.Option(help="P2 source manifest 输出路径。")] = (
        DEFAULT_ETF_P2_MANIFEST_PATH
    ),
    date_option: Annotated[str | None, typer.Option("--date", help="日期或 latest。")] = None,
    symbols: Annotated[
        list[str] | None,
        typer.Option("--symbol", help="输出目标 ETF symbol，可重复；默认全部 tradeable ETF。"),
    ] = None,
    output_dir: Annotated[Path, typer.Option(help="派生报告输出目录。")] = (
        DEFAULT_ETF_REPORT_DIR / "p2"
    ),
) -> None:
    """从本地 ^VIX 市场缓存派生 P2 options risk proxy；显式保留 VXN/skew 缺口。"""
    config = load_etf_config_bundle()
    p2_config = _require_p2_config(config)
    source = p2_config.sources["options_iv_skew"]
    run_date = _resolve_date(date_option, prices_path=prices_path)
    raw = read_price_frame(prices_path)
    prices, metadata_issues = standardize_price_frame(
        raw,
        assets=config.assets,
        source_name=str(prices_path),
    )
    quality_report = validate_price_data(
        prices,
        assets=config.assets,
        strategy=config.strategy,
        as_of=run_date,
        extra_issues=metadata_issues,
    )
    if not quality_report.passed:
        typer.echo(f"ETF 数据质量状态：{quality_report.status}，已停止 P2 options-risk 派生。")
        raise typer.Exit(code=1)
    target_symbols = symbols or list(config.assets.tradeable_symbols)
    frame = derive_options_iv_skew_from_vix(
        source=source,
        p2_config=p2_config,
        prices=raw,
        prices_path=prices_path,
        output_path=output_path,
        manifest_path=manifest_path,
        run_date=run_date,
        symbols=target_symbols,
        data_quality_status=quality_report.status,
    )
    metadata = {**p2_metadata(config), **_quality_metadata(quality_report)}
    _write_p2_frame(
        frame,
        output_dir,
        run_date,
        "derive_options_risk",
        "ETF P2 Derive Options Risk",
        config,
        metadata,
    )


@p2_app.command("holdings-lookthrough")
def p2_holdings_lookthrough_command(
    input_path: Annotated[Path | None, typer.Option(help="ETF holdings audit input CSV。")] = None,
    date_option: Annotated[str | None, typer.Option("--date", help="日期或 latest。")] = None,
    output_dir: Annotated[Path, typer.Option(help="输出目录。")] = DEFAULT_ETF_REPORT_DIR / "p2",
) -> None:
    """生成 P2 ETF 持仓穿透 contract/look-through report；observe-only。"""
    config = load_etf_config_bundle()
    p2_config = _require_p2_config(config)
    source = p2_config.sources["etf_holdings"]
    path = input_path or _p2_input_path(source.input_path)
    run_date = _resolve_p2_source_date(date_option, path, source.as_of_column)
    frame = build_holdings_lookthrough_report(
        source=source,
        run_date=run_date,
        input_path=path,
    )
    _write_p2_frame(
        frame,
        output_dir,
        run_date,
        "holdings_lookthrough",
        "ETF P2 Holdings Lookthrough",
        config,
    )


@p2_app.command("advanced-risk")
def p2_advanced_risk_command(
    prices_path: Annotated[Path, typer.Option(help="价格缓存路径。")] = DEFAULT_ETF_PRICE_PATH,
    allocation_path: Annotated[Path, typer.Option(help="目标权重路径。")] = DEFAULT_ETF_TARGET_PATH,
    date_option: Annotated[str | None, typer.Option("--date", help="日期或 latest。")] = None,
    output_dir: Annotated[Path, typer.Option(help="输出目录。")] = DEFAULT_ETF_REPORT_DIR / "p2",
) -> None:
    """生成 P2 高级风险模型基础诊断；observe-only。"""
    config = load_etf_config_bundle()
    _require_p2_config(config)
    prices, quality_report = load_standard_prices(prices_path, config.assets, config.strategy)
    if not quality_report.passed:
        typer.echo(f"ETF 数据质量状态：{quality_report.status}，已停止 P2 advanced-risk。")
        raise typer.Exit(code=1)
    run_date = _resolve_date(date_option, prices=prices)
    allocation = load_allocation(allocation_path)
    frame = build_advanced_risk_report(
        allocation=allocation,
        prices=prices,
        config=config,
        quality_report=quality_report,
        run_date=run_date,
    )
    metadata = {**p2_metadata(config), **_quality_metadata(quality_report)}
    _write_p2_frame(
        frame,
        output_dir,
        run_date,
        "advanced_risk",
        "ETF P2 Advanced Risk",
        config,
        metadata,
    )


@p2_app.command("walk-forward")
def p2_walk_forward_command(
    date_option: Annotated[str | None, typer.Option("--date", help="日期或 latest。")] = None,
    backtest_dir: Annotated[Path, typer.Option(help="ETF backtest 输出根目录。")] = (
        DEFAULT_ETF_BACKTEST_DIR
    ),
    output_dir: Annotated[Path, typer.Option(help="输出目录。")] = DEFAULT_ETF_REPORT_DIR / "p2",
) -> None:
    """生成 P2 walk-forward readiness report；observe-only。"""
    config = load_etf_config_bundle()
    p2_config = _require_p2_config(config)
    run_date = _parse_date(date_option) if date_option and date_option != "latest" else date.today()
    frame = build_walk_forward_readiness_report(
        backtest_dir=backtest_dir,
        p2_config=p2_config,
        run_date=run_date,
    )
    _write_p2_frame(
        frame,
        output_dir,
        run_date,
        "walk_forward",
        "ETF P2 Walk Forward Readiness",
        config,
    )


@p2_app.command("ml-ranking")
def p2_ml_ranking_command(
    signals_path: Annotated[Path, typer.Option(help="Signals 路径。")] = DEFAULT_ETF_SIGNAL_PATH,
    date_option: Annotated[str | None, typer.Option("--date", help="日期或 latest。")] = None,
    output_dir: Annotated[Path, typer.Option(help="输出目录。")] = DEFAULT_ETF_REPORT_DIR / "p2",
) -> None:
    """生成 P2 ML ranking candidate report；不进入 production。"""
    config = load_etf_config_bundle()
    p2_config = _require_p2_config(config)
    signals = load_signals(signals_path)
    run_date = _resolve_frame_date(date_option, signals)
    frame = build_ml_ranking_candidates(signals, p2_config=p2_config, run_date=run_date)
    _write_p2_frame(
        frame,
        output_dir,
        run_date,
        "ml_ranking",
        "ETF P2 ML Ranking Candidates",
        config,
    )


@p2_app.command("weight-optimizer")
def p2_weight_optimizer_command(
    prices_path: Annotated[Path, typer.Option(help="价格缓存路径。")] = DEFAULT_ETF_PRICE_PATH,
    signals_path: Annotated[Path, typer.Option(help="Signals 路径。")] = DEFAULT_ETF_SIGNAL_PATH,
    date_option: Annotated[str | None, typer.Option("--date", help="日期或 latest。")] = None,
    output_dir: Annotated[Path, typer.Option(help="输出目录。")] = DEFAULT_ETF_REPORT_DIR / "p2",
) -> None:
    """生成 P2 candidate-only weight optimizer report；不写正式目标权重。"""
    config = load_etf_config_bundle()
    _require_p2_config(config)
    prices, quality_report = load_standard_prices(prices_path, config.assets, config.strategy)
    if not quality_report.passed:
        typer.echo(f"ETF 数据质量状态：{quality_report.status}，已停止 P2 weight-optimizer。")
        raise typer.Exit(code=1)
    signals = load_signals(signals_path)
    run_date = _resolve_frame_date(date_option, signals)
    frame = build_weight_optimizer_candidates(
        signals=signals,
        prices=prices,
        config=config,
        quality_report=quality_report,
        run_date=run_date,
    )
    metadata = {**p2_metadata(config), **_quality_metadata(quality_report)}
    _write_p2_frame(
        frame,
        output_dir,
        run_date,
        "weight_optimizer",
        "ETF P2 Weight Optimizer Candidates",
        config,
        metadata,
    )


@p2_app.command("ensemble")
def p2_ensemble_command(
    signals_path: Annotated[Path, typer.Option(help="Signals 路径。")] = DEFAULT_ETF_SIGNAL_PATH,
    date_option: Annotated[str | None, typer.Option("--date", help="日期或 latest。")] = None,
    output_dir: Annotated[Path, typer.Option(help="输出目录。")] = DEFAULT_ETF_REPORT_DIR / "p2",
) -> None:
    """生成 P2 ensemble candidate report；不进入 production。"""
    config = load_etf_config_bundle()
    p2_config = _require_p2_config(config)
    signals = load_signals(signals_path)
    run_date = _resolve_frame_date(date_option, signals)
    ml = build_ml_ranking_candidates(signals, p2_config=p2_config, run_date=run_date)
    frame = build_ensemble_candidates(signals, ml, p2_config=p2_config, run_date=run_date)
    _write_p2_frame(frame, output_dir, run_date, "ensemble", "ETF P2 Ensemble Candidates", config)


@p2_app.command("live-preflight")
def p2_live_preflight_command(
    date_option: Annotated[str | None, typer.Option("--date", help="日期或 latest。")] = None,
    output_dir: Annotated[Path, typer.Option(help="输出目录。")] = DEFAULT_ETF_REPORT_DIR / "p2",
) -> None:
    """生成 P2 多账户/实盘接口 preflight；默认阻断 broker route。"""
    config = load_etf_config_bundle()
    p2_config = _require_p2_config(config)
    run_date = _parse_date(date_option) if date_option and date_option != "latest" else date.today()
    frame = build_live_interface_preflight(p2_config=p2_config, run_date=run_date)
    _write_p2_frame(
        frame,
        output_dir,
        run_date,
        "live_preflight",
        "ETF P2 Live Interface Preflight",
        config,
    )


@signals_app.command("generate")
def signals_generate_command(
    features_path: Annotated[Path, typer.Option(help="Feature store 路径。")] = (
        DEFAULT_ETF_FEATURE_PATH
    ),
    date_option: Annotated[str | None, typer.Option("--date", help="信号日期或 latest。")] = None,
    output_path: Annotated[Path, typer.Option(help="信号输出路径。")] = DEFAULT_ETF_SIGNAL_PATH,
) -> None:
    """生成 ETF signals，不输出仓位。"""
    config = load_etf_config_bundle()
    features = load_feature_store(features_path)
    run_date = _resolve_feature_date(date_option, features)
    records = generate_signals_for_date(features, strategy=config.strategy, run_date=run_date)
    write_signals(records, output_path)
    typer.echo(f"ETF signals 已写入：{output_path}（date={run_date.isoformat()}）")


@regime_app.command("generate")
def regime_generate_command(
    features_path: Annotated[Path, typer.Option(help="Feature store 路径。")] = (
        DEFAULT_ETF_FEATURE_PATH
    ),
    signals_path: Annotated[Path, typer.Option(help="Signals 路径。")] = DEFAULT_ETF_SIGNAL_PATH,
    date_option: Annotated[
        str | None,
        typer.Option("--date", help="regime 日期或 latest。"),
    ] = None,
    output_path: Annotated[Path, typer.Option(help="Regime 输出路径。")] = DEFAULT_ETF_REGIME_PATH,
) -> None:
    """生成 ETF market regime。"""
    config = load_etf_config_bundle()
    features = load_feature_store(features_path)
    signals = load_signals(signals_path)
    run_date = _resolve_feature_date(date_option, features)
    record = generate_regime_for_date(
        features,
        signals,
        strategy=config.strategy,
        risk=config.risk,
        run_date=run_date,
    )
    write_regime(record, output_path)
    typer.echo(f"ETF regime 已写入：{output_path}（{record.regime}）")


@portfolio_app.command("allocate")
def portfolio_allocate_command(
    signals_path: Annotated[Path, typer.Option(help="Signals 路径。")] = DEFAULT_ETF_SIGNAL_PATH,
    regime_path: Annotated[Path, typer.Option(help="Regime 路径。")] = DEFAULT_ETF_REGIME_PATH,
    prices_path: Annotated[Path, typer.Option(help="价格缓存路径，用于质量门禁。")] = (
        DEFAULT_ETF_PRICE_PATH
    ),
    date_option: Annotated[str | None, typer.Option("--date", help="组合日期或 latest。")] = None,
    output_path: Annotated[Path, typer.Option(help="目标权重输出路径。")] = DEFAULT_ETF_TARGET_PATH,
) -> None:
    """由 ETF signals + regime 生成目标权重。"""
    config = load_etf_config_bundle()
    prices, quality_report = load_standard_prices(prices_path, config.assets, config.strategy)
    if not quality_report.passed:
        typer.echo(f"ETF 数据质量状态：{quality_report.status}，已停止 allocation。")
        raise typer.Exit(code=1)
    signals = load_signals(signals_path)
    regimes = load_regimes(regime_path)
    run_date = _resolve_date(date_option, prices=prices)
    signal_rows = select_signals_for_date(signals, run_date)
    regime_row = select_regime_for_date(regimes, run_date)
    previous_weights = latest_weights_from_file(output_path)
    records = allocate_portfolio(
        signal_rows,
        assets=config.assets,
        strategy=config.strategy,
        risk=config.risk,
        regime=str(regime_row["regime"]),
        run_date=run_date,
        config_hash=config.config_hash,
        data_quality_report=quality_report,
        previous_weights=previous_weights,
    )
    write_allocation(records, output_path)
    typer.echo(f"ETF target weights 已写入：{output_path}")
    typer.echo(f"sum={sum(record.target_weight for record in records):.6f}")


@backtest_app.command("run")
def backtest_run_command(
    prices_path: Annotated[Path, typer.Option(help="价格缓存路径。")] = DEFAULT_ETF_PRICE_PATH,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="ETF backtest config YAML 路径。"),
    ] = DEFAULT_ETF_BACKTEST_CONFIG_PATH,
    start: Annotated[str | None, typer.Option("--from", help="回测开始日期。")] = None,
    end: Annotated[str | None, typer.Option("--to", help="回测结束日期。")] = None,
    output_dir: Annotated[Path, typer.Option(help="回测输出目录。")] = DEFAULT_ETF_BACKTEST_DIR,
    fast: Annotated[bool, typer.Option("--fast", help="只跑最近约 90 个交易日 smoke。")] = False,
) -> None:
    """运行 ETF 组合级回测。"""
    config = load_etf_config_bundle(backtest_path=config_path)
    prices, quality_report = load_standard_prices(prices_path, config.assets, config.strategy)
    if not quality_report.passed:
        typer.echo(f"ETF 数据质量状态：{quality_report.status}，已停止 backtest。")
        raise typer.Exit(code=1)
    run = run_portfolio_backtest(
        prices,
        config=config,
        quality_report=quality_report,
        start=_parse_date(start) if start else None,
        end=_resolve_date(end, prices=prices) if end else None,
        fast=fast,
    )
    (
        daily_path,
        weights_path,
        trades_path,
        summary_json,
        metrics_json,
        summary_md,
        stability_json,
        stability_md,
    ) = write_backtest_run(run, output_dir)
    typer.echo(f"ETF backtest 完成：{run.run_id}")
    typer.echo(f"Daily：{daily_path}")
    typer.echo(f"Weights：{weights_path}")
    typer.echo(f"Trades：{trades_path}")
    typer.echo(f"Summary JSON：{summary_json}")
    typer.echo(f"Metrics JSON：{metrics_json}")
    typer.echo(f"Summary Markdown：{summary_md}")
    typer.echo(f"Stability JSON：{stability_json}")
    typer.echo(f"Stability Markdown：{stability_md}")


@backtest_app.command("report")
def backtest_report_command(
    run_id: Annotated[str, typer.Option(help="回测 run_id。")],
    output_dir: Annotated[Path, typer.Option(help="回测输出根目录。")] = DEFAULT_ETF_BACKTEST_DIR,
) -> None:
    """显示已生成 ETF backtest summary 路径。"""
    summary = output_dir / run_id / "summary.md"
    if not summary.exists():
        raise typer.BadParameter(f"未找到 ETF backtest summary：{summary}")
    typer.echo(str(summary))


@backtest_app.command("diagnostics")
def backtest_diagnostics_command(
    run_id: Annotated[str | None, typer.Option(help="回测 run_id；省略时使用 latest。")] = None,
    latest: Annotated[bool, typer.Option("--latest", help="读取最新 backtest run。")] = False,
    output_dir: Annotated[Path, typer.Option(help="回测输出根目录。")] = DEFAULT_ETF_BACKTEST_DIR,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="ETF backtest config YAML 路径。"),
    ] = DEFAULT_ETF_BACKTEST_CONFIG_PATH,
) -> None:
    """从已生成 backtest run 计算 allocation stability diagnostics。"""
    selected_run_dir = _resolve_backtest_run_dir(output_dir, run_id=run_id, latest=latest)
    daily_path = selected_run_dir / "daily.csv"
    weights_path = selected_run_dir / "weights.csv"
    if not daily_path.exists() or not weights_path.exists():
        raise typer.BadParameter(f"backtest run 缺少 daily.csv 或 weights.csv：{selected_run_dir}")
    config = load_etf_config_bundle(backtest_path=config_path)
    payload = build_allocation_stability_diagnostics(
        pd.read_csv(daily_path),
        pd.read_csv(weights_path),
        max_daily_turnover=config.risk.portfolio_constraints.max_daily_turnover,
        max_rebalance_trade_weight=config.risk.portfolio_constraints.max_rebalance_trade_weight,
    )
    json_path, markdown_path = write_allocation_stability_diagnostics(
        payload,
        selected_run_dir / "stability_diagnostics.json",
        selected_run_dir / "stability_diagnostics.md",
    )
    typer.echo(f"ETF allocation stability status：{payload['status']}")
    typer.echo(f"JSON：{json_path}")
    typer.echo(f"Markdown：{markdown_path}")


@simulation_app.command("record")
def simulation_record_command(
    allocation_path: Annotated[Path, typer.Option(help="目标权重路径。")] = DEFAULT_ETF_TARGET_PATH,
    ledger_path: Annotated[Path, typer.Option(help="模拟舱 ledger 路径。")] = (
        DEFAULT_ETF_LEDGER_PATH
    ),
    date_option: Annotated[
        str | None,
        typer.Option("--date", help="记录日期或 latest；默认 latest allocation date。"),
    ] = None,
    report_path: Annotated[Path | None, typer.Option(help="关联日报路径。")] = None,
) -> None:
    """记录 ETF 模拟舱快照，按 date/model_version/symbol 幂等 upsert。"""
    allocation = load_allocation(allocation_path)
    run_date = _resolve_frame_date(date_option or "latest", allocation)
    allocation = _select_frame_date(allocation, run_date, label="目标权重")
    records = [_allocation_record_from_row(row) for _, row in allocation.iterrows()]
    record_simulation_snapshot(
        allocation_records=records,
        ledger_path=ledger_path,
        report_path=report_path,
    )
    typer.echo(f"ETF simulation ledger 已更新：{ledger_path}（date={run_date.isoformat()}）")


@simulation_app.command("evaluate")
def simulation_evaluate_command(
    prices_path: Annotated[Path, typer.Option(help="价格缓存路径。")] = DEFAULT_ETF_PRICE_PATH,
    ledger_path: Annotated[Path, typer.Option(help="模拟舱 ledger 路径。")] = (
        DEFAULT_ETF_LEDGER_PATH
    ),
    as_of: Annotated[str | None, typer.Option("--as-of", help="评估日期或 latest。")] = None,
) -> None:
    """补充 ETF 模拟舱 forward return 字段；未来窗口不足保持 null。"""
    config = load_etf_config_bundle()
    prices, quality_report = load_standard_prices(prices_path, config.assets, config.strategy)
    if not quality_report.passed:
        typer.echo(f"ETF 数据质量状态：{quality_report.status}，已停止 simulation evaluate。")
        raise typer.Exit(code=1)
    run_date = _resolve_date(as_of, prices=prices)
    evaluate_simulation_ledger(ledger_path=ledger_path, prices=prices, as_of=run_date)
    typer.echo(f"ETF simulation ledger 已评估：{ledger_path}")


@simulation_app.command("report")
def simulation_report_command(
    ledger_path: Annotated[Path, typer.Option(help="模拟舱 ledger 路径。")] = (
        DEFAULT_ETF_LEDGER_PATH
    ),
    window: Annotated[str, typer.Option(help="报告窗口。")] = "60d",
    output_path: Annotated[Path | None, typer.Option(help="报告输出路径。")] = None,
) -> None:
    """生成 ETF 模拟舱报告。"""
    report_path = output_path or DEFAULT_ETF_REPORT_DIR / f"simulation_report_{window}.md"
    write_simulation_report(ledger_path, report_path, window=window)
    typer.echo(f"ETF simulation report：{report_path}")


@report_app.command("daily")
def report_daily_command(
    prices_path: Annotated[Path, typer.Option(help="价格缓存路径。")] = DEFAULT_ETF_PRICE_PATH,
    signals_path: Annotated[Path, typer.Option(help="Signals 路径。")] = DEFAULT_ETF_SIGNAL_PATH,
    regime_path: Annotated[Path, typer.Option(help="Regime 路径。")] = DEFAULT_ETF_REGIME_PATH,
    allocation_path: Annotated[Path, typer.Option(help="目标权重路径。")] = DEFAULT_ETF_TARGET_PATH,
    ledger_path: Annotated[Path, typer.Option(help="模拟舱 ledger 路径。")] = (
        DEFAULT_ETF_LEDGER_PATH
    ),
    date_option: Annotated[str | None, typer.Option("--date", help="日报日期或 latest。")] = None,
    output_path: Annotated[Path | None, typer.Option(help="日报输出路径。")] = None,
) -> None:
    """生成 ETF Daily Portfolio Brief。"""
    report_path = _generate_daily_report(
        prices_path=prices_path,
        signals_path=signals_path,
        regime_path=regime_path,
        allocation_path=allocation_path,
        ledger_path=ledger_path,
        date_option=date_option,
        output_path=output_path,
    )
    typer.echo(f"ETF daily brief：{report_path}")


@run_app.command("daily")
def run_daily_command(
    prices_path: Annotated[Path, typer.Option(help="价格缓存路径。")] = DEFAULT_ETF_PRICE_PATH,
    date_option: Annotated[str | None, typer.Option("--date", help="运行日期或 latest。")] = None,
    dry_run: Annotated[
        bool,
        typer.Option("--dry-run", help="写入 dry-run artifact 目录。"),
    ] = False,
) -> None:
    """执行 ETF P0 日流程：validate -> features -> signals -> regime -> allocation -> report。"""
    root = (
        PROJECT_ROOT / "artifacts" / "etf_portfolio" / "dry_run"
        if dry_run
        else PROJECT_ROOT / "data" / "etf_portfolio"
    )
    report_root = (
        PROJECT_ROOT / "artifacts" / "etf_portfolio" / "dry_run_reports"
        if dry_run
        else DEFAULT_ETF_REPORT_DIR
    )
    config = load_etf_config_bundle()
    prices, quality_report = load_standard_prices(prices_path, config.assets, config.strategy)
    if not quality_report.passed:
        typer.echo(f"ETF 数据质量状态：{quality_report.status}，已停止 daily run。")
        raise typer.Exit(code=1)
    run_date = _resolve_date(date_option, prices=prices)
    features = build_feature_store(prices, assets=config.assets, strategy=config.strategy)
    features_path = root / "features.csv"
    signals_path = root / "signals.csv"
    regime_path = root / "regimes.csv"
    allocation_path = root / "target_weights.csv"
    write_feature_store(features, features_path)
    signal_records = generate_signals_for_date(
        features,
        strategy=config.strategy,
        run_date=run_date,
    )
    write_signals(signal_records, signals_path)
    regime_record = generate_regime_for_date(
        features,
        signals_to_frame(signal_records),
        strategy=config.strategy,
        risk=config.risk,
        run_date=run_date,
    )
    write_regime(regime_record, regime_path)
    allocation_records = allocate_portfolio(
        signals_to_frame(signal_records),
        assets=config.assets,
        strategy=config.strategy,
        risk=config.risk,
        regime=regime_record.regime,
        run_date=run_date,
        config_hash=config.config_hash,
        data_quality_report=quality_report,
        previous_weights=None if dry_run else latest_weights_from_file(DEFAULT_ETF_TARGET_PATH),
    )
    write_allocation(allocation_records, allocation_path)
    report_path = _generate_daily_report(
        prices_path=prices_path,
        signals_path=signals_path,
        regime_path=regime_path,
        allocation_path=allocation_path,
        ledger_path=DEFAULT_ETF_LEDGER_PATH,
        date_option=run_date.isoformat(),
        output_path=report_root / f"{run_date.isoformat()}_portfolio_brief.md",
    )
    if not dry_run:
        record_simulation_snapshot(
            allocation_records=allocation_records,
            ledger_path=DEFAULT_ETF_LEDGER_PATH,
            report_path=report_path,
        )
        evaluate_simulation_ledger(
            ledger_path=DEFAULT_ETF_LEDGER_PATH,
            prices=prices,
            as_of=run_date,
        )
    typer.echo(f"ETF daily run 完成：{run_date.isoformat()}")
    typer.echo(f"dry_run={str(dry_run).lower()}")
    typer.echo(f"report={report_path}")
    typer.echo(f"data_quality_status={quality_report.status}")


def _generate_daily_report(
    *,
    prices_path: Path,
    signals_path: Path,
    regime_path: Path,
    allocation_path: Path,
    ledger_path: Path | None,
    date_option: str | None,
    output_path: Path | None,
) -> Path:
    config = load_etf_config_bundle()
    prices, quality_report = load_standard_prices(prices_path, config.assets, config.strategy)
    run_date = _resolve_date(date_option, prices=prices)
    signals = select_signals_for_date(load_signals(signals_path), run_date)
    regime = select_regime_for_date(load_regimes(regime_path), run_date)
    allocation = load_allocation(allocation_path)
    allocation_dates = pd.to_datetime(allocation["date"], errors="coerce")
    allocation = allocation.loc[allocation_dates == pd.Timestamp(run_date)]
    if allocation.empty:
        raise typer.BadParameter(f"目标权重缺少日期：{run_date.isoformat()}")
    report_path = (
        output_path
        or DEFAULT_ETF_REPORT_DIR / f"{run_date.isoformat()}_portfolio_brief.md"
    )
    markdown = render_daily_brief(
        run_date=run_date,
        config=config,
        quality_report=quality_report,
        signals=signals,
        regime=regime,
        allocation=allocation,
        simulation_summary=(
            summarize_simulation_for_brief(ledger_path, as_of=run_date)
            if ledger_path is not None
            else None
        ),
    )
    return write_daily_brief(markdown, report_path)


def _build_experiment_candidate_selection(
    run_dir: Path,
    *,
    promotion_policy_override: str | None,
) -> dict[str, object]:
    payload = build_experiment_comparison_report(run_dir)
    pack_registry = load_experiment_pack_registry()
    pack_id = payload["run_metadata"].get("pack_id")
    policy_id = promotion_policy_override or "shadow_only_manual_review"
    if pack_id:
        pack_config = pack_registry.experiment_packs.get(str(pack_id))
        if pack_config is not None:
            payload = apply_ranking_policy_to_comparison_report(
                payload,
                ranking_policy=pack_registry.ranking_policies[pack_config.ranking_policy],
                ranking_policy_id=pack_config.ranking_policy,
            )
            policy_id = promotion_policy_override or pack_config.promotion_policy
    policy = pack_registry.promotion_policies.get(policy_id)
    if policy is None:
        raise typer.BadParameter(f"unknown promotion policy: {policy_id}")
    return build_candidate_selection_report(
        payload,
        promotion_policy=policy,
        promotion_policy_id=policy_id,
    )


def _build_satellite_report_payload(
    *,
    prices_path: Path,
    date_option: str | None,
    universe_path: Path,
    policy_path: Path,
    ai_confirmation_report_path: Path | None,
) -> tuple[dict[str, object], date]:
    config = load_etf_config_bundle()
    satellite_config = load_satellite_universe_config(universe_path)
    policy_config = load_satellite_policy_config(policy_path)
    extra_symbols = set(satellite_price_symbols(satellite_config)) - set(config.assets.assets)
    prices, quality_report = load_standard_prices(
        prices_path,
        config.assets,
        config.strategy,
        extra_symbols=extra_symbols,
    )
    if not quality_report.passed:
        typer.echo(f"ETF 数据质量状态：{quality_report.status}，已停止 satellite report。")
        raise typer.Exit(code=1)
    run_date = _resolve_date(date_option, prices=prices)
    availability = validate_satellite_data_availability(
        satellite_config,
        _available_price_symbols(prices, run_date),
    )
    if availability["status"] == "FAIL":
        typer.echo("Satellite 数据覆盖状态：FAIL，已停止 report build。")
        for error in availability["errors"]:
            typer.echo(f"- {error}")
        raise typer.Exit(code=1)
    report_metadata = _quality_metadata(quality_report)
    ai_confirmation_payload = (
        json.loads(ai_confirmation_report_path.read_text(encoding="utf-8"))
        if ai_confirmation_report_path is not None and ai_confirmation_report_path.exists()
        else None
    )
    payload = build_satellite_replacement_report(
        prices=prices,
        universe_config=satellite_config,
        policy_config=policy_config,
        run_date=run_date,
        data_quality_status=quality_report.status,
        data_quality_report=str(report_metadata["data_quality_report"]).strip("`"),
        base_weights=_default_etf_base_weights(config),
        ai_confirmation_payload=ai_confirmation_payload,
        market_regime=config.backtest.backtest.regime,
        requested_date_range=_price_requested_date_range(prices, run_date),
    )
    return payload, run_date


def _default_etf_base_weights(config) -> dict[str, float]:
    return {
        symbol: float(asset.default_weight)
        for symbol, asset in config.assets.assets.items()
        if symbol in {"SPY", "QQQ", "SMH", "SOXX", "CASH"}
    }


def _weekly_review_date(*, as_of: str | None, latest: bool) -> date:
    if latest and as_of is not None:
        raise typer.BadParameter("--latest and --as-of cannot be combined")
    return date.today() if latest or as_of is None else _parse_date(as_of)


def _parse_operations_graph_cadence(value: str) -> OperationsGraphCadence:
    if value not in {"daily", "weekly", "biweekly", "monthly"}:
        raise typer.BadParameter(
            "--cadence must be one of: daily, weekly, biweekly, monthly"
        )
    return cast(OperationsGraphCadence, value)


def _run_weekly_review_generate(
    *,
    as_of: str | None,
    latest: bool,
    report_index_path: Path | None,
    report_registry_path: Path,
    target_weights_path: Path,
    required_report: list[str] | None,
    output_dir: Path,
    aggregation_dir: Path,
) -> None:
    run_date = _weekly_review_date(as_of=as_of, latest=latest)
    generated = datetime.now(UTC)
    aggregation = build_weekly_review_aggregation(
        as_of=run_date,
        report_index_path=report_index_path,
        report_registry_path=report_registry_path,
        target_weights_path=target_weights_path,
        required_report_ids=required_report,
        generated_at=generated,
    )
    aggregation_path = aggregation_dir / f"weekly_review_aggregation_{run_date.isoformat()}.json"
    write_weekly_review_aggregation(aggregation, aggregation_path)
    if aggregation["aggregation_status"] == "FAIL":
        typer.echo(f"ETF weekly review aggregation blocked：{aggregation_path}")
        typer.echo(f"aggregation_status={aggregation['aggregation_status']}")
        raise typer.Exit(code=1)
    payload = build_weekly_review_report(
        as_of=run_date,
        aggregation_payload=aggregation,
        generated_at=generated,
    )
    json_path = output_dir / f"weekly_review_{run_date.isoformat()}.json"
    md_path = output_dir / f"weekly_review_{run_date.isoformat()}.md"
    write_weekly_review_report(payload, json_path=json_path, markdown_path=md_path)
    typer.echo(f"ETF weekly review：{md_path}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"aggregation={aggregation_path}")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


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


def _resolve_date(
    value: str | None,
    *,
    prices_path: Path | None = None,
    prices: pd.DataFrame | None = None,
) -> date:
    if value is not None and value != "latest":
        return _parse_date(value)
    if prices is None:
        if prices_path is None:
            raise typer.BadParameter("latest 需要 prices 或 prices_path")
        config = load_etf_config_bundle()
        raw = read_price_frame(prices_path)
        prices, _ = standardize_price_frame(raw, assets=config.assets, source_name=str(prices_path))
    return latest_price_date(prices)


def _resolve_feature_date(value: str | None, features: pd.DataFrame) -> date:
    if value is not None and value != "latest":
        return _parse_date(value)
    return latest_feature_date(features)


def _resolve_frame_date(value: str | None, frame: pd.DataFrame, column: str = "date") -> date:
    if value is not None and value != "latest":
        return _parse_date(value)
    if column not in frame.columns:
        raise typer.BadParameter(f"{column} 字段不存在")
    parsed = pd.to_datetime(frame[column], errors="coerce").dropna()
    if parsed.empty:
        raise typer.BadParameter(f"{column} 没有可用日期")
    return parsed.max().date()


def _resolve_backtest_run_dir(output_dir: Path, *, run_id: str | None, latest: bool) -> Path:
    if run_id is not None and latest:
        raise typer.BadParameter("run_id 和 --latest 只能选择一个")
    if run_id is not None:
        run_dir = output_dir / run_id
        if not run_dir.exists():
            raise typer.BadParameter(f"未找到 ETF backtest run：{run_dir}")
        return run_dir
    if not output_dir.exists():
        raise typer.BadParameter(f"ETF backtest 输出目录不存在：{output_dir}")
    candidates = [
        item
        for item in output_dir.iterdir()
        if item.is_dir() and (item / "summary.json").exists()
    ]
    if not candidates:
        raise typer.BadParameter(f"未找到 ETF backtest run：{output_dir}")
    return max(candidates, key=lambda item: (item / "summary.json").stat().st_mtime)


def _select_frame_date(
    frame: pd.DataFrame,
    run_date: date,
    *,
    column: str = "date",
    label: str = "数据",
) -> pd.DataFrame:
    if column not in frame.columns:
        raise typer.BadParameter(f"{label}缺少 {column} 字段")
    parsed = pd.to_datetime(frame[column], errors="coerce")
    selected = frame.loc[parsed == pd.Timestamp(run_date)].copy()
    if selected.empty:
        raise typer.BadParameter(f"{label}缺少日期：{run_date.isoformat()}")
    return selected


def _resolve_p2_source_date(value: str | None, path: Path, as_of_column: str) -> date:
    if value is not None and value != "latest":
        return _parse_date(value)
    if not path.exists():
        return date.today()
    frame = pd.read_parquet(path) if path.suffix.lower() == ".parquet" else pd.read_csv(path)
    if as_of_column not in frame.columns:
        return date.today()
    return _resolve_frame_date("latest", frame, as_of_column)


def _parse_date(value: str | None) -> date:
    if value is None:
        raise typer.BadParameter("日期不能为空")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter("日期必须使用 YYYY-MM-DD 或 latest") from exc


def _parse_datetime(value: str) -> datetime:
    try:
        parsed = pd.Timestamp(value).to_pydatetime()
    except ValueError as exc:
        raise typer.BadParameter("时间必须使用 ISO-8601 格式") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed


def _artifact_stem(value: object) -> str:
    text = str(value).strip().replace(":", "_").replace("/", "_").replace("\\", "_")
    return "".join(
        character if character.isalnum() or character in "._-" else "_"
        for character in text
    )


def _load_optional_json_payload(path: Path | None) -> dict[str, object]:
    if path is None:
        return {}
    if not path.exists():
        raise typer.BadParameter(f"JSON artifact 不存在：{path}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise typer.BadParameter(f"JSON artifact 解析失败：{path}") from exc
    if not isinstance(payload, dict):
        raise typer.BadParameter(f"JSON artifact root 必须是 object：{path}")
    return payload


def _echo_weight_shadow_enrollment_summary(
    enrollment_path: Path,
    enrollment: Mapping[str, object],
) -> None:
    latest_selection = enrollment.get("latest_selection")
    latest = latest_selection if isinstance(latest_selection, Mapping) else {}
    raw_results = latest.get("enrollment_results")
    results = [dict(item) for item in raw_results if isinstance(item, Mapping)] if isinstance(
        raw_results,
        list,
    ) else []
    typer.echo(f"ETF weight candidate shadow enrollment：{enrollment_path}")
    typer.echo(f"enrollment_count={enrollment['enrollment_count']}")
    typer.echo(f"selected_weight_set_count={len(latest.get('weight_set_ids') or [])}")
    for result in results:
        typer.echo(
            "enrollment_result="
            + json.dumps(result, ensure_ascii=False, sort_keys=True)
        )
    typer.echo("shared_shadow_registry_mutated=false")
    typer.echo("production_weights_mutated=false")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


def _latest_json_file(directory: Path, pattern: str) -> Path | None:
    if not directory.exists():
        return None
    candidates = [path for path in directory.glob(pattern) if path.is_file()]
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime)


def _allocation_record_from_row(row: pd.Series) -> ETFAllocationRecord:
    return ETFAllocationRecord(
        date=_parse_date(str(row["date"])),
        symbol=str(row["symbol"]),
        target_weight=float(row["target_weight"]),
        previous_weight=_optional_float(row.get("previous_weight")),
        trade_delta=_optional_float(row.get("trade_delta")),
        composite_score=_optional_float(row.get("composite_score")),
        regime=str(row["regime"]),
        reason_codes=tuple(_json_list(row.get("reason_codes"))),
        constraints_applied=tuple(_json_list(row.get("constraints_applied"))),
        model_version=str(row["model_version"]),
        config_hash=str(row["config_hash"]),
        data_quality_status=str(row["data_quality_status"]),
        created_at=pd.Timestamp(str(row["created_at"])).to_pydatetime(),
        constraint_diagnostics=tuple(_json_records(row.get("constraint_diagnostics"))),
    )


def _optional_float(value: object) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(parsed):
        return None
    return parsed


def _json_list(value: object) -> list[str]:
    if value is None or pd.isna(value):
        return []
    try:
        parsed = json.loads(str(value))
    except ValueError:
        return [str(value)]
    if not isinstance(parsed, list):
        return [str(parsed)]
    return [str(item) for item in parsed]


def _json_records(value: object) -> list[dict[str, object]]:
    if value is None or pd.isna(value):
        return []
    try:
        parsed = json.loads(str(value))
    except ValueError:
        return []
    if not isinstance(parsed, list):
        return []
    return [dict(item) for item in parsed if isinstance(item, dict)]


def _p1_quality_metadata(
    prices_path: Path,
    config,
    *,
    include_satellites: bool,
) -> dict[str, object]:
    _, report = load_standard_prices(
        prices_path,
        config.assets,
        config.strategy,
        extra_symbols=_satellite_symbols(config) if include_satellites else None,
    )
    if not report.passed:
        typer.echo(f"ETF 数据质量状态：{report.status}，已停止 P1 report。")
        raise typer.Exit(code=1)
    return _quality_metadata(report)


def _quality_metadata(report) -> dict[str, object]:
    report_date = report.max_date.isoformat() if report.max_date else "unknown"
    report_path = DEFAULT_ETF_REPORT_DIR / f"data_quality_{report_date}.md"
    write_quality_report(report, report_path)
    return {
        "data_quality_status": report.status,
        "data_quality_report": f"`{report_path}`",
    }


def _require_p2_config(config):
    if config.p2 is None:
        raise typer.BadParameter("缺少 ETF P2 config")
    return config.p2


def _p2_input_path(value: str) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path
    return PROJECT_ROOT / path


def _write_p2_source_contract(
    *,
    source_id: str,
    title: str,
    input_path: Path | None,
    date_option: str | None,
    output_dir: Path,
) -> None:
    config = load_etf_config_bundle()
    p2_config = _require_p2_config(config)
    source = p2_config.sources[source_id]
    path = input_path or _p2_input_path(source.input_path)
    run_date = _resolve_p2_source_date(date_option, path, source.as_of_column)
    frame = build_source_contract_report(
        source_id=source_id,
        source=source,
        run_date=run_date,
        input_path=path,
    )
    _write_p2_frame(frame, output_dir, run_date, source_id, title, config)


def _write_p2_frame(
    frame: pd.DataFrame,
    output_dir: Path,
    run_date: date,
    stem: str,
    title: str,
    config,
    metadata: dict[str, object] | None = None,
) -> None:
    csv_path = output_dir / f"{run_date.isoformat()}_{stem}.csv"
    md_path = output_dir / f"{run_date.isoformat()}_{stem}.md"
    write_frame_and_report(
        frame,
        csv_path,
        md_path,
        title,
        metadata=metadata or p2_metadata(config),
    )
    typer.echo(f"{title}：{md_path}")


def _satellite_symbols(config) -> set[str]:
    if config.p1 is None:
        return set()
    return set(config.p1.satellite_stocks)


def _available_price_symbols(prices: pd.DataFrame, run_date: date) -> set[str]:
    frame = prices.copy()
    if "date" not in frame.columns or "symbol" not in frame.columns:
        return set()
    parsed_dates = pd.to_datetime(frame["date"], errors="coerce")
    selected = frame.loc[parsed_dates.notna() & (parsed_dates <= pd.Timestamp(run_date))]
    return {str(symbol).strip().upper() for symbol in selected["symbol"].dropna()}


def _price_requested_date_range(prices: pd.DataFrame, run_date: date) -> dict[str, str]:
    if "date" not in prices.columns:
        return {"start": "", "end": run_date.isoformat()}
    parsed_dates = pd.to_datetime(prices["date"], errors="coerce")
    selected = parsed_dates.loc[parsed_dates.notna() & (parsed_dates <= pd.Timestamp(run_date))]
    if selected.empty:
        return {"start": "", "end": run_date.isoformat()}
    return {
        "start": selected.min().date().isoformat(),
        "end": run_date.isoformat(),
    }
