from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Annotated, Any, cast

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
from ai_trading_system.etf_portfolio.dynamic_calibration import (
    DEFAULT_DYNAMIC_CALIBRATION_CANDIDATE_DIR,
    DEFAULT_DYNAMIC_CALIBRATION_POLICY_CONFIG_PATH,
    DEFAULT_DYNAMIC_CALIBRATION_REPORT_DIR,
    DEFAULT_DYNAMIC_CALIBRATION_VALIDATION_DIR,
    DynamicCalibrationError,
    build_dynamic_calibration_batch_report,
    build_dynamic_calibration_validation_report,
    latest_dynamic_calibration_report_path,
    load_dynamic_calibration_policy_config,
    load_latest_trend_report,
    write_dynamic_calibration_candidate_packs,
    write_dynamic_calibration_report,
    write_dynamic_calibration_validation_report,
)
from ai_trading_system.etf_portfolio.dynamic_rescue import (
    DEFAULT_DYNAMIC_FAILURE_DIAGNOSTICS_POLICY_CONFIG_PATH,
    DEFAULT_DYNAMIC_RESCUE_DATASET_DIR,
    DEFAULT_DYNAMIC_RESCUE_REPORT_DIR,
    DEFAULT_DYNAMIC_RESCUE_VALIDATION_DIR,
    DynamicRescueError,
    build_dynamic_rescue_batch_report,
    build_dynamic_rescue_validation_report,
    latest_dynamic_rescue_report_path,
    load_dynamic_failure_diagnostics_policy_config,
    load_dynamic_robustness_report,
    load_latest_failed_dynamic_package,
    write_dynamic_failure_dataset,
    write_dynamic_rescue_report,
    write_dynamic_rescue_validation_report,
)
from ai_trading_system.etf_portfolio.dynamic_robustness import (
    DEFAULT_DYNAMIC_ROBUSTNESS_POLICY_CONFIG_PATH,
    DEFAULT_DYNAMIC_ROBUSTNESS_REPORT_DIR,
    DEFAULT_DYNAMIC_ROBUSTNESS_VALIDATION_DIR,
    DynamicRobustnessError,
    build_dynamic_robustness_report,
    build_dynamic_robustness_validation_report,
    latest_dynamic_robustness_report_path,
    load_dynamic_robustness_policy_config,
    load_latest_dynamic_calibration_report,
    write_dynamic_robustness_report,
    write_dynamic_robustness_validation_report,
)
from ai_trading_system.etf_portfolio.dynamic_shadow import (
    DEFAULT_DYNAMIC_SHADOW_APPROVAL_DIR,
    DEFAULT_DYNAMIC_SHADOW_ENROLLMENT_DIR,
    DEFAULT_DYNAMIC_SHADOW_FORWARD_UPDATE_DIR,
    DEFAULT_DYNAMIC_SHADOW_PACKAGE_DIR,
    DEFAULT_DYNAMIC_SHADOW_POLICY_CONFIG_PATH,
    DEFAULT_DYNAMIC_SHADOW_REGISTRY_PATH,
    DEFAULT_DYNAMIC_SHADOW_VALIDATION_DIR,
    DEFAULT_DYNAMIC_SHADOW_WEEKLY_REVIEW_DIR,
    DynamicShadowError,
    build_dynamic_shadow_approved_enrollment,
    build_dynamic_shadow_forward_update,
    build_dynamic_shadow_owner_approval,
    build_dynamic_shadow_review_package,
    build_dynamic_shadow_validation_report,
    build_dynamic_shadow_weekly_review,
    latest_dynamic_shadow_forward_update_path,
    latest_dynamic_shadow_owner_approval_path,
    latest_dynamic_shadow_review_package_path,
    load_dynamic_shadow_candidate_registry,
    load_dynamic_shadow_policy_config,
    upsert_dynamic_shadow_candidate_registry,
    write_dynamic_shadow_approved_enrollment,
    write_dynamic_shadow_candidate_registry,
    write_dynamic_shadow_forward_update,
    write_dynamic_shadow_owner_approval,
    write_dynamic_shadow_review_package,
    write_dynamic_shadow_validation_report,
    write_dynamic_shadow_weekly_review,
)
from ai_trading_system.etf_portfolio.dynamic_v2_review import (
    DEFAULT_DYNAMIC_V2_REVIEW_PACKAGE_DIR,
    DEFAULT_DYNAMIC_V2_REVIEW_POLICY_CONFIG_PATH,
    DEFAULT_DYNAMIC_V2_REVIEW_VALIDATION_DIR,
    DynamicV2ReviewError,
    build_dynamic_v2_review_package,
    build_dynamic_v2_review_validation_report,
    latest_dynamic_v2_review_package_path,
    load_dynamic_v2_review_policy_config,
    load_latest_review_inputs,
    write_dynamic_v2_review_package,
    write_dynamic_v2_review_validation_report,
)
from ai_trading_system.etf_portfolio.dynamic_v3_failure_attribution import (
    DEFAULT_DYNAMIC_V3_FAILURE_ATTRIBUTION_POLICY_CONFIG_PATH,
    DEFAULT_DYNAMIC_V3_FAILURE_ATTRIBUTION_REPORT_DIR,
    DEFAULT_DYNAMIC_V3_FAILURE_ATTRIBUTION_VALIDATION_DIR,
    DynamicV3FailureAttributionError,
    build_dynamic_v3_failure_attribution_report,
    build_dynamic_v3_failure_attribution_validation_report,
    latest_dynamic_v3_failure_attribution_real_evaluation_path,
    latest_dynamic_v3_failure_attribution_report_path,
    load_dynamic_v3_failure_attribution_policy_config,
    write_dynamic_v3_failure_attribution_report,
    write_dynamic_v3_failure_attribution_validation_report,
)
from ai_trading_system.etf_portfolio.dynamic_v3_failure_attribution import (
    load_json_artifact as load_dynamic_v3_failure_attribution_json_artifact,
)
from ai_trading_system.etf_portfolio.dynamic_v3_paper_tracking import (
    DEFAULT_ADVISORY_OUTCOME_DIR,
    DEFAULT_OWNER_ATTRIBUTION_DIR,
    DEFAULT_PAPER_PORTFOLIO_CONFIG_PATH,
    DEFAULT_PAPER_PORTFOLIO_DIR,
    DEFAULT_RATES_CACHE_PATH,
    DEFAULT_SHADOW_AGING_DIR,
    DEFAULT_WEEKLY_ADVISORY_REVIEW_DIR,
    advisory_outcome_report_payload,
    apply_owner_review_to_paper_portfolio,
    init_paper_portfolio,
    owner_attribution_report_payload,
    paper_portfolio_report_payload,
    paper_portfolio_state_payload,
    run_owner_attribution,
    run_shadow_aging,
    run_weekly_advisory_review,
    shadow_aging_report_payload,
    track_advisory_outcome,
    update_advisory_outcome,
    validate_advisory_outcome_artifact,
    validate_owner_attribution_artifact,
    validate_paper_portfolio_artifact,
    validate_shadow_aging_artifact,
    validate_weekly_advisory_review_artifact,
    weekly_advisory_review_report_payload,
)
from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    DEFAULT_CANDIDATE_ATTRIBUTION_DIR,
    DEFAULT_CANDIDATE_CLUSTER_DIR,
    DEFAULT_CANDIDATE_RECOVERY_DIR,
    DEFAULT_CONSENSUS_DRIFT_DIR,
    DEFAULT_DATA_AUDIT_DIR,
    DEFAULT_DATA_PROVENANCE_DIR,
    DEFAULT_DYNAMIC_V3_RESEARCH_ROOT,
    DEFAULT_EVIDENCE_DIAGNOSIS_DIR,
    DEFAULT_EVIDENCE_GATE_POLICY_CONFIG_PATH,
    DEFAULT_EVIDENCE_SUMMARY_DIR,
    DEFAULT_GATE_IMPACT_DIR,
    DEFAULT_GATE_POLICY_DIR,
    DEFAULT_GOVERNANCE_DIR,
    DEFAULT_INJECTION_AUDIT_DIR,
    DEFAULT_INTERPRETATION_PACK_DIR,
    DEFAULT_LATEST_POINTER_DIR,
    DEFAULT_MEDIUM_REAL_DIR,
    DEFAULT_OBSERVE_POOL_DIR,
    DEFAULT_OVERFIT_DIR,
    DEFAULT_OVERNIGHT_READINESS_DIR,
    DEFAULT_OWNER_REVIEW_JOURNAL_DIR,
    DEFAULT_PARAMETER_GOVERNANCE_CONFIG_PATH,
    DEFAULT_PARAMETER_SWEEP_CONFIG_PATH,
    DEFAULT_PARAMETER_SWEEP_PROFILE_CONFIG_PATH,
    DEFAULT_PORTFOLIO_SNAPSHOT_DIR,
    DEFAULT_POSITION_ADVISORY_CONFIG_PATH,
    DEFAULT_POSITION_ADVISORY_DAILY_DIR,
    DEFAULT_POSITION_ADVISORY_DIR,
    DEFAULT_POSITION_REVIEW_DIR,
    DEFAULT_PROMOTION_DIR,
    DEFAULT_REGIME_COVERAGE_DIR,
    DEFAULT_RESEARCH_DECISION_DIR,
    DEFAULT_RESEARCH_DECISION_UPDATE_DIR,
    DEFAULT_RESEARCH_INDEX_DIR,
    DEFAULT_ROBUSTNESS_DIR,
    DEFAULT_SCHEDULE_OBSERVE_DIR,
    DEFAULT_SHADOW_MONITOR_DIR,
    DEFAULT_SHADOW_MONITOR_RUN_DIR,
    DEFAULT_SHADOW_REGISTRY_PATH,
    DEFAULT_SHADOW_REPORT_DIR,
    DEFAULT_SHADOW_SHORTLIST_DIR,
    DEFAULT_SHORTLIST_DIR,
    DEFAULT_SWEEP_OUTPUT_DIR,
    DEFAULT_WALK_FORWARD_DIR,
    DEFAULT_WALK_FORWARD_SELECTION_DIR,
    DEFAULT_WINDOW_AUDIT_DIR,
    DynamicV3ParameterResearchError,
    activate_shadow_monitoring,
    apply_evidence_gate_policy,
    artifacts_latest_payload,
    build_medium_real_report,
    build_observe_pool,
    build_position_review_pack,
    build_promotion_pack,
    build_research_index,
    build_shadow_shortlist,
    build_shadow_shortlist_monitoring_pack,
    build_sweep_config_validation,
    build_sweep_leaderboard_payload,
    build_sweep_report_payload,
    candidate_cluster_report_payload,
    candidate_recovery_report_payload,
    candidate_report_payload,
    consensus_drift_report_payload,
    create_owner_review,
    data_audit_report_payload,
    data_provenance_inspect_price_cache,
    data_provenance_repair_price_manifest,
    data_provenance_validate,
    evidence_diagnosis_report_payload,
    evidence_gate_policy_report_payload,
    evidence_summary_report_payload,
    gate_impact_report_payload,
    governance_diff_payload,
    governance_report_payload,
    injection_audit_report_payload,
    inspect_window_artifact,
    interpretation_report_payload,
    latest_sweep_id,
    medium_real_report_payload,
    observe_pool_report_payload,
    overfit_report_payload,
    overnight_readiness_report_payload,
    owner_review_report_payload,
    owner_review_summary,
    portfolio_snapshot_report_payload,
    position_advisory_daily_report_payload,
    position_advisory_report_payload,
    position_review_report_payload,
    preview_sweep_candidates,
    promotion_review_payload,
    rebuild_observe_pool_from_recovery,
    record_owner_review_decision,
    regime_coverage_report_payload,
    register_shadow_candidate,
    repair_latest_pointers_payload,
    research_compare_payload,
    research_decision_report_payload,
    research_decision_update_report_payload,
    research_history_payload,
    research_query_payload,
    robustness_report_payload,
    run_candidate_attribution,
    run_candidate_clustering,
    run_candidate_recovery,
    run_consensus_drift,
    run_data_audit,
    run_evidence_diagnosis,
    run_evidence_summary,
    run_gate_impact,
    run_injection_audit,
    run_interpretation_pack,
    run_overfit_review,
    run_overnight_readiness,
    run_parameter_sweep,
    run_parameter_sweep_profile,
    run_position_advisory,
    run_position_advisory_daily,
    run_regime_coverage,
    run_research_decision,
    run_robustness_diagnostics,
    run_shadow_monitor,
    run_shadow_shortlist_monitor,
    run_walk_forward_selection,
    run_walk_forward_validation,
    run_window_audit,
    scheduled_observe_payload,
    shadow_list_payload,
    shadow_monitor_report_payload,
    shadow_monitor_run_report_payload,
    shadow_report_payload,
    shadow_shortlist_report_payload,
    shortlist_report_payload,
    stale_artifacts_payload,
    sweep_profile_list_payload,
    sweep_status_payload,
    update_research_decision,
    validate_artifacts_payload,
    validate_candidate_attribution_artifact,
    validate_candidate_cluster_artifact,
    validate_candidate_recovery_artifact,
    validate_consensus_drift_artifact,
    validate_data_audit_artifact,
    validate_evidence_diagnosis_artifact,
    validate_evidence_gate_policy,
    validate_evidence_summary_artifact,
    validate_gate_impact_artifact,
    validate_injection_audit_artifact,
    validate_interpretation_pack_artifact,
    validate_medium_real_sweep,
    validate_observe_pool_artifact,
    validate_overfit_artifact,
    validate_overnight_readiness_artifact,
    validate_owner_review_artifact,
    validate_parameter_governance,
    validate_portfolio_snapshot_file,
    validate_position_advisory_artifact,
    validate_position_advisory_daily_artifact,
    validate_position_review_artifact,
    validate_promotion_pack,
    validate_regime_coverage_artifact,
    validate_research_decision_artifact,
    validate_research_decision_update_artifact,
    validate_robustness_artifact,
    validate_shadow_monitor_artifact,
    validate_shadow_monitor_run_artifact,
    validate_shadow_registry,
    validate_shadow_shortlist_artifact,
    validate_shortlist_artifact,
    validate_sweep_artifact,
    validate_sweep_profiles_payload,
    validate_walk_forward_artifact,
    validate_walk_forward_selection_artifact,
    validate_weight_path_artifact,
    validate_window_audit_artifact,
    walk_forward_report_payload,
    walk_forward_selection_report_payload,
    weight_path_report_payload,
    window_audit_report_payload,
    write_portfolio_snapshot_artifact,
)
from ai_trading_system.etf_portfolio.dynamic_v3_real_evaluation import (
    DEFAULT_DYNAMIC_V3_REAL_EVALUATION_POLICY_CONFIG_PATH,
    DEFAULT_DYNAMIC_V3_REAL_EVALUATION_REPORT_DIR,
    DEFAULT_DYNAMIC_V3_REAL_EVALUATION_VALIDATION_DIR,
    DynamicV3RealEvaluationError,
    build_dynamic_v3_real_evaluation_report,
    build_dynamic_v3_real_evaluation_validation_report,
    latest_dynamic_v3_real_evaluation_report_path,
    load_dynamic_v3_real_evaluation_policy_config,
    write_dynamic_v3_real_evaluation_report,
    write_dynamic_v3_real_evaluation_validation_report,
)
from ai_trading_system.etf_portfolio.dynamic_v3_rescue import (
    DEFAULT_DYNAMIC_V3_RESCUE_POLICY_CONFIG_PATH,
    DEFAULT_DYNAMIC_V3_RESCUE_REPORT_DIR,
    DEFAULT_DYNAMIC_V3_RESCUE_VALIDATION_DIR,
    DynamicV3RescueError,
    build_dynamic_v3_rescue_evaluation_report,
    build_dynamic_v3_rescue_validation_report,
    latest_dynamic_v3_rescue_report_path,
    load_dynamic_v3_rescue_policy_config,
    load_latest_v3_rescue_inputs,
    write_dynamic_v3_rescue_evaluation_report,
    write_dynamic_v3_rescue_validation_report,
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
dynamic_allocation_app = typer.Typer(
    help="ETF candidate-only dynamic allocation policy workflow。",
    no_args_is_help=True,
)
dynamic_calibration_app = typer.Typer(
    help="ETF two-layer dynamic candidate batch/cache workflow。",
    no_args_is_help=True,
)
dynamic_robustness_app = typer.Typer(
    help="ETF dynamic strategy robustness review workflow。",
    no_args_is_help=True,
)
dynamic_rescue_app = typer.Typer(
    help="ETF dynamic failure diagnostics and rescue candidate workflow。",
    no_args_is_help=True,
)
dynamic_v2_review_app = typer.Typer(
    help="ETF dynamic v0.2 review-only robustness and shadow review package。",
    no_args_is_help=True,
)
dynamic_v3_rescue_app = typer.Typer(
    help="ETF dynamic v0.3 constraint-aware rescue candidate workflow。",
    no_args_is_help=True,
)
dynamic_v3_sweep_config_app = typer.Typer(
    help="Dynamic v3 rescue parameter sweep config workflow。",
    no_args_is_help=True,
)
dynamic_v3_sweep_app = typer.Typer(
    help="Dynamic v3 rescue batch parameter sweep workflow。",
    no_args_is_help=True,
)
dynamic_v3_data_audit_app = typer.Typer(
    help="Dynamic v3 rescue research data audit workflow。",
    no_args_is_help=True,
)
dynamic_v3_data_provenance_app = typer.Typer(
    help="Dynamic v3 rescue price cache provenance workflow。",
    no_args_is_help=True,
)
dynamic_v3_window_audit_app = typer.Typer(
    help="Dynamic v3 rescue backtest window audit workflow。",
    no_args_is_help=True,
)
dynamic_v3_weight_path_app = typer.Typer(
    help="Dynamic v3 rescue real evaluator weight path workflow。",
    no_args_is_help=True,
)
dynamic_v3_injection_audit_app = typer.Typer(
    help="Dynamic v3 rescue parameter injection audit workflow。",
    no_args_is_help=True,
)
dynamic_v3_candidate_app = typer.Typer(
    help="Dynamic v3 rescue candidate report workflow。",
    no_args_is_help=True,
)
dynamic_v3_walk_forward_app = typer.Typer(
    help="Dynamic v3 rescue walk-forward validation workflow。",
    no_args_is_help=True,
)
dynamic_v3_robustness_app = typer.Typer(
    help="Dynamic v3 rescue robustness diagnostics workflow。",
    no_args_is_help=True,
)
dynamic_v3_overfit_app = typer.Typer(
    help="Dynamic v3 rescue overfit risk workflow。",
    no_args_is_help=True,
)
dynamic_v3_shadow_app = typer.Typer(
    help="Dynamic v3 rescue observe-only shadow registry workflow。",
    no_args_is_help=True,
)
dynamic_v3_artifacts_app = typer.Typer(
    help="Dynamic v3 rescue artifact latest/validation workflow。",
    no_args_is_help=True,
)
dynamic_v3_schedule_app = typer.Typer(
    help="Dynamic v3 rescue scheduled observation gate。",
    no_args_is_help=True,
)
dynamic_v3_evidence_summary_app = typer.Typer(
    help="Dynamic v3 rescue candidate evidence summary workflow。",
    no_args_is_help=True,
)
dynamic_v3_medium_real_app = typer.Typer(
    help="Dynamic v3 rescue medium real candidate discovery report。",
    no_args_is_help=True,
)
dynamic_v3_regime_coverage_app = typer.Typer(
    help="Dynamic v3 rescue tech/semiconductor regime coverage audit。",
    no_args_is_help=True,
)
dynamic_v3_promotion_app = typer.Typer(
    help="Dynamic v3 rescue promotion review pack workflow。",
    no_args_is_help=True,
)
dynamic_v3_observe_pool_app = typer.Typer(
    help="Dynamic v3 rescue observe-only candidate pool workflow。",
    no_args_is_help=True,
)
dynamic_v3_overnight_readiness_app = typer.Typer(
    help="Dynamic v3 rescue overnight real readiness workflow。",
    no_args_is_help=True,
)
dynamic_v3_governance_app = typer.Typer(
    help="Dynamic v3 rescue parameter governance workflow。",
    no_args_is_help=True,
)
dynamic_v3_research_app = typer.Typer(
    help="Dynamic v3 rescue research index/query workflow。",
    no_args_is_help=True,
)
dynamic_v3_research_decision_app = typer.Typer(
    help="Dynamic v3 rescue research decision pack workflow。",
    no_args_is_help=True,
)
dynamic_v3_evidence_diagnosis_app = typer.Typer(
    help="Dynamic v3 rescue evidence blocking diagnosis workflow。",
    no_args_is_help=True,
)
dynamic_v3_gate_impact_app = typer.Typer(
    help="Dynamic v3 rescue gate impact simulation workflow。",
    no_args_is_help=True,
)
dynamic_v3_gate_policy_app = typer.Typer(
    help="Dynamic v3 rescue evidence gate policy workflow。",
    no_args_is_help=True,
)
dynamic_v3_candidate_recovery_app = typer.Typer(
    help="Dynamic v3 rescue candidate recovery workflow。",
    no_args_is_help=True,
)
dynamic_v3_shortlist_app = typer.Typer(
    help="Dynamic v3 rescue shadow shortlist workflow。",
    no_args_is_help=True,
)
dynamic_v3_candidate_cluster_app = typer.Typer(
    help="Dynamic v3 rescue candidate clustering workflow。",
    no_args_is_help=True,
)
dynamic_v3_shadow_shortlist_app = typer.Typer(
    help="Dynamic v3 rescue shadow shortlist monitoring pack workflow。",
    no_args_is_help=True,
)
dynamic_v3_shadow_monitor_run_app = typer.Typer(
    help="Dynamic v3 rescue shadow shortlist daily/weekly monitor workflow。",
    no_args_is_help=True,
)
dynamic_v3_portfolio_snapshot_app = typer.Typer(
    help="Dynamic v3 rescue manual portfolio snapshot workflow。",
    no_args_is_help=True,
)
dynamic_v3_position_advisory_app = typer.Typer(
    help="Dynamic v3 rescue position advisory workflow。",
    no_args_is_help=True,
)
dynamic_v3_consensus_drift_app = typer.Typer(
    help="Dynamic v3 rescue candidate consensus drift workflow。",
    no_args_is_help=True,
)
dynamic_v3_owner_review_app = typer.Typer(
    help="Dynamic v3 rescue owner review journal workflow。",
    no_args_is_help=True,
)
dynamic_v3_paper_portfolio_app = typer.Typer(
    help="Dynamic v3 rescue paper portfolio workflow。",
    no_args_is_help=True,
)
dynamic_v3_advisory_outcome_app = typer.Typer(
    help="Dynamic v3 rescue advisory outcome workflow。",
    no_args_is_help=True,
)
dynamic_v3_owner_attribution_app = typer.Typer(
    help="Dynamic v3 rescue owner attribution workflow。",
    no_args_is_help=True,
)
dynamic_v3_shadow_aging_app = typer.Typer(
    help="Dynamic v3 rescue shadow aging workflow。",
    no_args_is_help=True,
)
dynamic_v3_weekly_advisory_review_app = typer.Typer(
    help="Dynamic v3 rescue weekly advisory review workflow。",
    no_args_is_help=True,
)
dynamic_v3_position_review_app = typer.Typer(
    help="Dynamic v3 rescue position review workflow。",
    no_args_is_help=True,
)
dynamic_shadow_app = typer.Typer(
    help="ETF owner-approved dynamic candidate forward shadow workflow。",
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
etf_app.add_typer(dynamic_allocation_app, name="dynamic-allocation")
etf_app.add_typer(dynamic_calibration_app, name="dynamic-calibration")
etf_app.add_typer(dynamic_robustness_app, name="dynamic-robustness")
etf_app.add_typer(dynamic_rescue_app, name="dynamic-rescue")
etf_app.add_typer(dynamic_v2_review_app, name="dynamic-v2-review")
dynamic_v3_rescue_app.add_typer(dynamic_v3_sweep_config_app, name="sweep-config")
dynamic_v3_rescue_app.add_typer(dynamic_v3_sweep_app, name="sweep")
dynamic_v3_rescue_app.add_typer(dynamic_v3_data_audit_app, name="data-audit")
dynamic_v3_rescue_app.add_typer(dynamic_v3_data_provenance_app, name="data-provenance")
dynamic_v3_rescue_app.add_typer(dynamic_v3_window_audit_app, name="window-audit")
dynamic_v3_rescue_app.add_typer(dynamic_v3_weight_path_app, name="weight-path")
dynamic_v3_rescue_app.add_typer(dynamic_v3_injection_audit_app, name="injection-audit")
dynamic_v3_rescue_app.add_typer(dynamic_v3_candidate_app, name="candidate")
dynamic_v3_rescue_app.add_typer(dynamic_v3_walk_forward_app, name="walk-forward")
dynamic_v3_rescue_app.add_typer(dynamic_v3_robustness_app, name="robustness")
dynamic_v3_rescue_app.add_typer(dynamic_v3_overfit_app, name="overfit")
dynamic_v3_rescue_app.add_typer(dynamic_v3_shadow_app, name="shadow")
dynamic_v3_rescue_app.add_typer(dynamic_v3_artifacts_app, name="artifacts")
dynamic_v3_rescue_app.add_typer(dynamic_v3_schedule_app, name="schedule")
dynamic_v3_rescue_app.add_typer(dynamic_v3_evidence_summary_app, name="evidence-summary")
dynamic_v3_rescue_app.add_typer(dynamic_v3_medium_real_app, name="medium-real")
dynamic_v3_rescue_app.add_typer(dynamic_v3_regime_coverage_app, name="regime-coverage")
dynamic_v3_rescue_app.add_typer(dynamic_v3_promotion_app, name="promotion")
dynamic_v3_rescue_app.add_typer(dynamic_v3_observe_pool_app, name="observe-pool")
dynamic_v3_rescue_app.add_typer(dynamic_v3_overnight_readiness_app, name="overnight-readiness")
dynamic_v3_rescue_app.add_typer(dynamic_v3_governance_app, name="governance")
dynamic_v3_rescue_app.add_typer(dynamic_v3_research_app, name="research")
dynamic_v3_rescue_app.add_typer(dynamic_v3_research_decision_app, name="research-decision")
dynamic_v3_rescue_app.add_typer(dynamic_v3_evidence_diagnosis_app, name="evidence-diagnosis")
dynamic_v3_rescue_app.add_typer(dynamic_v3_gate_impact_app, name="gate-impact")
dynamic_v3_rescue_app.add_typer(dynamic_v3_gate_policy_app, name="gate-policy")
dynamic_v3_rescue_app.add_typer(dynamic_v3_candidate_recovery_app, name="candidate-recovery")
dynamic_v3_rescue_app.add_typer(dynamic_v3_shortlist_app, name="shortlist")
dynamic_v3_rescue_app.add_typer(dynamic_v3_candidate_cluster_app, name="candidate-cluster")
dynamic_v3_rescue_app.add_typer(dynamic_v3_shadow_shortlist_app, name="shadow-shortlist")
dynamic_v3_rescue_app.add_typer(dynamic_v3_shadow_monitor_run_app, name="shadow-monitor")
dynamic_v3_rescue_app.add_typer(dynamic_v3_portfolio_snapshot_app, name="portfolio-snapshot")
dynamic_v3_rescue_app.add_typer(dynamic_v3_position_advisory_app, name="position-advisory")
dynamic_v3_rescue_app.add_typer(dynamic_v3_consensus_drift_app, name="consensus-drift")
dynamic_v3_rescue_app.add_typer(dynamic_v3_owner_review_app, name="owner-review")
dynamic_v3_rescue_app.add_typer(dynamic_v3_paper_portfolio_app, name="paper-portfolio")
dynamic_v3_rescue_app.add_typer(dynamic_v3_advisory_outcome_app, name="advisory-outcome")
dynamic_v3_rescue_app.add_typer(dynamic_v3_owner_attribution_app, name="owner-attribution")
dynamic_v3_rescue_app.add_typer(dynamic_v3_shadow_aging_app, name="shadow-aging")
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_weekly_advisory_review_app,
    name="weekly-advisory-review",
)
dynamic_v3_rescue_app.add_typer(dynamic_v3_position_review_app, name="position-review")
etf_app.add_typer(dynamic_v3_rescue_app, name="dynamic-v3-rescue")
etf_app.add_typer(dynamic_shadow_app, name="dynamic-shadow")
etf_app.add_typer(governance_app, name="governance")
etf_app.add_typer(events_app, name="events")
etf_app.add_typer(p2_app, name="p2")
etf_app.add_typer(credibility_app, name="credibility")

DEFAULT_ETF_OPERATIONS_DRY_RUN_DIR = PROJECT_ROOT / "outputs" / "dry_runs" / "etf_operations"
DEFAULT_ETF_OPERATIONS_REPORT_DIR = PROJECT_ROOT / "reports" / "etf_portfolio" / "operations"
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
            _load_optional_json_payload(previous_outcome_path) if previous_outcome_path else None
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
            _json_float_mapping_option(scores_json, option_name="--scores-json")
            if scores_json
            else dict(policy.sample_score_profiles[score_profile])
        )
        previous_weights = (
            _json_float_mapping_option(
                previous_weights_json,
                option_name="--previous-weights-json",
            )
            if previous_weights_json
            else None
        )
        previous_scores = (
            _json_float_mapping_option(previous_scores_json, option_name="--previous-scores-json")
            if previous_scores_json
            else policy.sample_score_profiles.get("neutral")
        )
    except KeyError as exc:
        raise typer.BadParameter(f"unknown score profile: {score_profile}") from exc
    except DynamicAllocationError as exc:
        raise typer.BadParameter(str(exc)) from exc
    resolved_date = _parse_date(decision_date) if decision_date else date.today()
    source_trend_report = trend_report_path
    if source_trend_report is None and latest_trend_report:
        source_trend_report = latest_trend_calibration_report_path()
    trend_payload = _load_optional_json_payload(source_trend_report)
    trend_summary = _mapping_obj(trend_payload.get("summary"))
    trend_coverage = _mapping_obj(trend_payload.get("dataset_coverage"))
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
    payload = _load_optional_json_payload(resolved)
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


@dynamic_calibration_app.command("run")
def dynamic_calibration_run_command(
    pack: Annotated[
        str | None,
        typer.Option("--pack", help="Two-layer dynamic candidate pack id。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="dynamic calibration policy config。"),
    ] = DEFAULT_DYNAMIC_CALIBRATION_POLICY_CONFIG_PATH,
    dynamic_allocation_config_path: Annotated[
        Path,
        typer.Option(
            "--dynamic-allocation-config",
            help="TRADING-084 dynamic allocation policy config。",
        ),
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
    cache: Annotated[
        str | None,
        typer.Option("--cache", help="Cache mode: read-write/read-only/disabled。"),
    ] = None,
    cache_root: Annotated[
        Path | None,
        typer.Option("--cache-root", help="dynamic calibration cache root。"),
    ] = None,
    workers: Annotated[
        str | None,
        typer.Option("--workers", help="Worker count or auto。"),
    ] = None,
    top: Annotated[
        int | None,
        typer.Option("--top", help="Top dynamic candidate packs to show。"),
    ] = None,
    candidate_output_dir: Annotated[
        Path,
        typer.Option("--candidate-output-dir", help="candidate pack 输出目录。"),
    ] = DEFAULT_DYNAMIC_CALIBRATION_CANDIDATE_DIR,
    report_output_dir: Annotated[
        Path,
        typer.Option("--report-output-dir", help="dynamic calibration report 输出目录。"),
    ] = DEFAULT_DYNAMIC_CALIBRATION_REPORT_DIR,
) -> None:
    """运行 TRADING-085 two-layer dynamic candidate batch/cache；不写 production weights。"""
    if cache not in {None, "read-write", "read-only", "disabled"}:
        raise typer.BadParameter("--cache must be read-write, read-only, or disabled")
    try:
        policy = load_dynamic_calibration_policy_config(config_path)
        dynamic_policy = load_dynamic_allocation_policy_config(dynamic_allocation_config_path)
        resolved_trend_report_path = trend_report_path
        trend_payload: dict[str, Any] = {}
        if latest_trend_report or trend_report_path is not None:
            resolved_trend_report_path, trend_payload = load_latest_trend_report(trend_report_path)
        report = build_dynamic_calibration_batch_report(
            policy=policy,
            dynamic_policy=dynamic_policy,
            trend_report=trend_payload,
            trend_report_path=resolved_trend_report_path,
            dynamic_policy_path=dynamic_allocation_config_path,
            pack_id=pack,
            cache_mode=cache,
            cache_root=cache_root,
            workers=workers,
            top_n=top,
        )
    except DynamicCalibrationError as exc:
        raise typer.BadParameter(str(exc)) from exc
    candidate_paths = write_dynamic_calibration_candidate_packs(
        report,
        output_dir=candidate_output_dir,
    )
    report_paths = write_dynamic_calibration_report(report, output_dir=report_output_dir)
    summary = _mapping_obj(report.get("summary"))
    cache_summary = _mapping_obj(report.get("cache_summary"))
    typer.echo(f"ETF dynamic calibration candidate packs JSON：{candidate_paths['json']}")
    typer.echo(f"ETF dynamic calibration report JSON：{report_paths['json']}")
    typer.echo(f"status={report['status']}")
    typer.echo(f"candidate_pack_count={report['candidate_pack_count']}")
    typer.echo(f"top_candidate={summary.get('top_dynamic_candidate_pack_id')}")
    typer.echo(f"top_ranking_score={summary.get('top_ranking_score')}")
    typer.echo(f"cache_hit_rate={cache_summary.get('cache_hit_rate')}")
    typer.echo(f"cache_write_count={cache_summary.get('cache_write_count')}")
    typer.echo("calibration_proxy=true")
    typer.echo("full_robustness_backtest_required=true")
    typer.echo("commands_executed=false")
    typer.echo("production_state_mutated=false")
    typer.echo("baseline_config_mutated=false")
    typer.echo("official_target_weights_mutated=false")
    typer.echo("automatic_candidate_promotion=false")
    typer.echo("auto_enrollment_without_owner_approval=false")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@dynamic_calibration_app.command("report")
def dynamic_calibration_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取最新 dynamic calibration report。"),
    ] = True,
    report_path: Annotated[
        Path | None,
        typer.Option("--report-path", help="显式 report JSON path。"),
    ] = None,
    report_dir: Annotated[
        Path,
        typer.Option("--report-dir", help="report artifact directory。"),
    ] = DEFAULT_DYNAMIC_CALIBRATION_REPORT_DIR,
) -> None:
    """只读展示 latest TRADING-085 dynamic calibration report 摘要。"""
    resolved = report_path
    if resolved is None and latest:
        resolved = latest_dynamic_calibration_report_path(report_dir)
    payload = _load_optional_json_payload(resolved)
    if not payload:
        raise typer.BadParameter("dynamic calibration report not found")
    summary = payload.get("summary")
    if not isinstance(summary, dict):
        raise typer.BadParameter(f"invalid dynamic calibration report: {resolved}")
    cache_summary = _mapping_obj(payload.get("cache_summary"))
    typer.echo(f"dynamic_calibration_report={resolved}")
    typer.echo(f"status={payload.get('status')}")
    typer.echo(f"candidate_pack_count={payload.get('candidate_pack_count')}")
    typer.echo(f"top_candidate={summary.get('top_dynamic_candidate_pack_id')}")
    typer.echo(f"top_ranking_score={summary.get('top_ranking_score')}")
    typer.echo(f"data_quality_status={summary.get('data_quality_status')}")
    typer.echo(f"cache_hit_rate={cache_summary.get('cache_hit_rate')}")
    typer.echo("calibration_proxy=true")
    typer.echo("full_robustness_backtest_required=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("official_target_weights_mutated=false")


@dynamic_calibration_app.command("validate")
def dynamic_calibration_validate_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="dynamic calibration policy config。"),
    ] = DEFAULT_DYNAMIC_CALIBRATION_POLICY_CONFIG_PATH,
    dynamic_allocation_config_path: Annotated[
        Path,
        typer.Option(
            "--dynamic-allocation-config",
            help="TRADING-084 dynamic allocation policy config。",
        ),
    ] = DEFAULT_DYNAMIC_ALLOCATION_POLICY_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="validation report 输出目录。"),
    ] = DEFAULT_DYNAMIC_CALIBRATION_VALIDATION_DIR,
) -> None:
    """校验 TRADING-085 dynamic calibration workflow 和 safety boundary。"""
    payload = build_dynamic_calibration_validation_report(
        policy_config_path=config_path,
        dynamic_policy_path=dynamic_allocation_config_path,
    )
    paths = write_dynamic_calibration_validation_report(payload, output_dir=output_dir)
    typer.echo(f"ETF dynamic calibration validation JSON：{paths['json']}")
    typer.echo(f"ETF dynamic calibration validation Markdown：{paths['markdown']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("commands_executed=false")
    typer.echo("production_state_mutated=false")
    typer.echo("baseline_config_mutated=false")
    typer.echo("official_target_weights_mutated=false")
    typer.echo("automatic_candidate_promotion=false")
    typer.echo("auto_enrollment_without_owner_approval=false")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_robustness_app.command("report")
def dynamic_robustness_report_command(
    candidate: Annotated[
        str | None,
        typer.Option(
            "--candidate",
            help="Dynamic candidate pack id；不填则读取 latest calibration top candidate。",
        ),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="只读展示 latest dynamic robustness report。"),
    ] = False,
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
        typer.Option("--start", help="robustness requested start date。"),
    ] = None,
    end: Annotated[
        str | None,
        typer.Option("--end", help="robustness requested end date。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="dynamic robustness policy config。"),
    ] = DEFAULT_DYNAMIC_ROBUSTNESS_POLICY_CONFIG_PATH,
    dynamic_allocation_config_path: Annotated[
        Path,
        typer.Option(
            "--dynamic-allocation-config",
            help="TRADING-084 dynamic allocation policy config。",
        ),
    ] = DEFAULT_DYNAMIC_ALLOCATION_POLICY_CONFIG_PATH,
    dynamic_calibration_report_path: Annotated[
        Path | None,
        typer.Option(
            "--dynamic-calibration-report-path",
            help="Explicit TRADING-085 dynamic calibration report JSON。",
        ),
    ] = None,
    latest_dynamic_calibration_report: Annotated[
        bool,
        typer.Option(
            "--latest-dynamic-calibration-report/--no-latest-dynamic-calibration-report",
            help="没有显式 dynamic calibration report 时读取 latest TRADING-085 report。",
        ),
    ] = True,
    data_quality_output_path: Annotated[
        Path | None,
        typer.Option("--data-quality-output-path", help="validate-data markdown output path。"),
    ] = None,
    report_output_dir: Annotated[
        Path,
        typer.Option("--report-output-dir", help="dynamic robustness report 输出目录。"),
    ] = DEFAULT_DYNAMIC_ROBUSTNESS_REPORT_DIR,
) -> None:
    """生成或只读展示 TRADING-086 dynamic robustness report；不写 production weights。"""
    if latest:
        resolved = latest_dynamic_robustness_report_path(report_output_dir)
        payload = _load_optional_json_payload(resolved)
        if not payload:
            raise typer.BadParameter("dynamic robustness report not found")
        summary = _mapping_obj(payload.get("summary"))
        typer.echo(f"dynamic_robustness_report={resolved}")
        typer.echo(f"status={payload.get('status')}")
        typer.echo(f"candidate={summary.get('dynamic_candidate_id')}")
        typer.echo(f"data_quality_status={summary.get('data_quality_status')}")
        typer.echo(f"dynamic_total_return={summary.get('dynamic_total_return')}")
        typer.echo(f"excess_vs_static_base={summary.get('excess_vs_static_base')}")
        typer.echo(f"false_risk_off_count={summary.get('false_risk_off_count')}")
        typer.echo(f"false_risk_on_count={summary.get('false_risk_on_count')}")
        typer.echo(f"overfit_status={summary.get('overfit_status')}")
        typer.echo("production_effect=none")
        typer.echo("broker_action=none")
        typer.echo("official_target_weights_mutated=false")
        return

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
        policy = load_dynamic_robustness_policy_config(config_path)
        dynamic_policy = load_dynamic_allocation_policy_config(dynamic_allocation_config_path)
        calibration_path: Path | None = None
        calibration_payload: dict[str, Any] = {}
        if dynamic_calibration_report_path is not None or latest_dynamic_calibration_report:
            calibration_path, calibration_payload = load_latest_dynamic_calibration_report(
                dynamic_calibration_report_path
            )
        etf_config = load_etf_config_bundle()
        prices, etf_quality = load_standard_prices(
            prices_path,
            etf_config.assets,
            etf_config.strategy,
        )
        if not etf_quality.passed:
            raise DynamicRobustnessError(
                f"ETF price validation failed before dynamic robustness: {etf_quality.status}"
            )
        report = build_dynamic_robustness_report(
            prices=prices,
            etf_config=etf_config,
            policy=policy,
            dynamic_policy=dynamic_policy,
            candidate_id=candidate,
            dynamic_calibration_report=calibration_payload,
            dynamic_calibration_report_path=calibration_path,
            start=_parse_date(start) if start else None,
            end=_parse_date(end) if end else None,
            data_quality_status=data_quality_report.status,
            data_quality_report=str(quality_output),
            prices_path=prices_path,
        )
    except DynamicRobustnessError as exc:
        raise typer.BadParameter(str(exc)) from exc
    paths = write_dynamic_robustness_report(report, output_dir=report_output_dir)
    summary = _mapping_obj(report.get("summary"))
    typer.echo(f"ETF dynamic robustness report JSON：{paths['json']}")
    typer.echo(f"ETF dynamic robustness report Markdown：{paths['markdown']}")
    typer.echo(f"status={report['status']}")
    typer.echo(f"candidate={summary.get('dynamic_candidate_id')}")
    typer.echo(f"data_quality_status={summary.get('data_quality_status')}")
    typer.echo(f"dynamic_total_return={summary.get('dynamic_total_return')}")
    typer.echo(f"excess_vs_static_base={summary.get('excess_vs_static_base')}")
    typer.echo(f"false_risk_off_count={summary.get('false_risk_off_count')}")
    typer.echo(f"false_risk_on_count={summary.get('false_risk_on_count')}")
    typer.echo(f"overfit_status={summary.get('overfit_status')}")
    typer.echo("shadow_enrollment_allowed=false")
    typer.echo("commands_executed=false")
    typer.echo("production_state_mutated=false")
    typer.echo("baseline_config_mutated=false")
    typer.echo("official_target_weights_mutated=false")
    typer.echo("automatic_candidate_promotion=false")
    typer.echo("auto_enrollment_without_owner_approval=false")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@dynamic_robustness_app.command("validate")
def dynamic_robustness_validate_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="dynamic robustness policy config。"),
    ] = DEFAULT_DYNAMIC_ROBUSTNESS_POLICY_CONFIG_PATH,
    dynamic_allocation_config_path: Annotated[
        Path,
        typer.Option(
            "--dynamic-allocation-config",
            help="TRADING-084 dynamic allocation policy config。",
        ),
    ] = DEFAULT_DYNAMIC_ALLOCATION_POLICY_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="validation report 输出目录。"),
    ] = DEFAULT_DYNAMIC_ROBUSTNESS_VALIDATION_DIR,
) -> None:
    """校验 TRADING-086 dynamic robustness workflow 和 safety boundary。"""
    payload = build_dynamic_robustness_validation_report(
        policy_config_path=config_path,
        dynamic_policy_path=dynamic_allocation_config_path,
    )
    paths = write_dynamic_robustness_validation_report(payload, output_dir=output_dir)
    typer.echo(f"ETF dynamic robustness validation JSON：{paths['json']}")
    typer.echo(f"ETF dynamic robustness validation Markdown：{paths['markdown']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("commands_executed=false")
    typer.echo("production_state_mutated=false")
    typer.echo("baseline_config_mutated=false")
    typer.echo("official_target_weights_mutated=false")
    typer.echo("automatic_candidate_promotion=false")
    typer.echo("auto_enrollment_without_owner_approval=false")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_rescue_app.command("run")
def dynamic_rescue_run_command(
    base_candidate: Annotated[
        str | None,
        typer.Option(
            "--base-candidate",
            help="Failed dynamic v0.1 candidate id；不填则读取 source report summary。",
        ),
    ] = None,
    latest_failed_package: Annotated[
        bool,
        typer.Option(
            "--latest-failed-package/--no-latest-failed-package",
            help="从 latest dynamic shadow package 回溯 failed robustness report。",
        ),
    ] = False,
    dynamic_robustness_report_path: Annotated[
        Path | None,
        typer.Option(
            "--dynamic-robustness-report-path",
            help="Explicit failed TRADING-086 dynamic robustness report JSON。",
        ),
    ] = None,
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
        typer.Option("--start", help="rescue requested start date。"),
    ] = None,
    end: Annotated[
        str | None,
        typer.Option("--end", help="rescue requested end date。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="dynamic failure diagnostics config。"),
    ] = DEFAULT_DYNAMIC_FAILURE_DIAGNOSTICS_POLICY_CONFIG_PATH,
    dynamic_robustness_config_path: Annotated[
        Path,
        typer.Option(
            "--dynamic-robustness-config",
            help="TRADING-086 dynamic robustness policy config。",
        ),
    ] = DEFAULT_DYNAMIC_ROBUSTNESS_POLICY_CONFIG_PATH,
    dynamic_allocation_config_path: Annotated[
        Path,
        typer.Option(
            "--dynamic-allocation-config",
            help="TRADING-084 dynamic allocation policy config。",
        ),
    ] = DEFAULT_DYNAMIC_ALLOCATION_POLICY_CONFIG_PATH,
    data_quality_output_path: Annotated[
        Path | None,
        typer.Option("--data-quality-output-path", help="validate-data markdown output path。"),
    ] = None,
    dataset_output_dir: Annotated[
        Path,
        typer.Option("--dataset-output-dir", help="dynamic failure dataset 输出目录。"),
    ] = DEFAULT_DYNAMIC_RESCUE_DATASET_DIR,
    report_output_dir: Annotated[
        Path,
        typer.Option("--report-output-dir", help="dynamic rescue report 输出目录。"),
    ] = DEFAULT_DYNAMIC_RESCUE_REPORT_DIR,
) -> None:
    """生成 TRADING-088 failure diagnostics 和 bounded rescue candidate report。"""
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
        package_path: Path | None = None
        package_payload: dict[str, Any] = {}
        resolved_robustness_path = dynamic_robustness_report_path
        if latest_failed_package:
            package_path, package_payload = load_latest_failed_dynamic_package()
            source = _mapping_obj(package_payload.get("source_artifacts"))
            source_robustness = source.get("dynamic_robustness_report")
            if resolved_robustness_path is None and source_robustness:
                resolved_robustness_path = Path(str(source_robustness))
        loaded_path, failed_report = load_dynamic_robustness_report(
            resolved_robustness_path,
            report_dir=DEFAULT_DYNAMIC_ROBUSTNESS_REPORT_DIR,
        )
        policy = load_dynamic_failure_diagnostics_policy_config(config_path)
        robustness_policy = load_dynamic_robustness_policy_config(dynamic_robustness_config_path)
        dynamic_policy = load_dynamic_allocation_policy_config(dynamic_allocation_config_path)
        etf_config = load_etf_config_bundle()
        prices, etf_quality = load_standard_prices(
            prices_path,
            etf_config.assets,
            etf_config.strategy,
        )
        if not etf_quality.passed:
            raise DynamicRescueError(
                f"ETF price validation failed before dynamic rescue: {etf_quality.status}"
            )
        report = build_dynamic_rescue_batch_report(
            prices=prices,
            etf_config=etf_config,
            policy=policy,
            dynamic_robustness_policy=robustness_policy,
            dynamic_policy=dynamic_policy,
            failed_robustness_report=failed_report,
            shadow_review_package=package_payload,
            candidate_id=base_candidate,
            start=_parse_date(start) if start else None,
            end=_parse_date(end) if end else None,
            data_quality_status=data_quality_report.status,
            data_quality_report=str(quality_output),
            prices_path=prices_path,
        )
    except DynamicRescueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    dataset_paths = write_dynamic_failure_dataset(
        _mapping_obj(report.get("failure_dataset")),
        output_dir=dataset_output_dir,
    )
    report_paths = write_dynamic_rescue_report(report, output_dir=report_output_dir)
    summary = _mapping_obj(report.get("improvement_summary"))
    typer.echo(f"dynamic_failure_dataset_json={dataset_paths['json']}")
    typer.echo(f"dynamic_failure_dataset_markdown={dataset_paths['markdown']}")
    typer.echo(f"dynamic_rescue_report_json={report_paths['json']}")
    typer.echo(f"dynamic_rescue_report_markdown={report_paths['markdown']}")
    typer.echo(f"source_dynamic_robustness_report={loaded_path or ''}")
    typer.echo(f"source_dynamic_shadow_package={package_path or ''}")
    typer.echo(f"status={report['status']}")
    typer.echo(f"best_rescue_candidate={summary.get('best_candidate')}")
    typer.echo(f"best_status={summary.get('best_status')}")
    typer.echo("shadow_enrollment_allowed=false")
    typer.echo("automatic_enrollment_allowed=false")
    typer.echo("owner_approval_executed=false")
    typer.echo("commands_executed=false")
    typer.echo("production_state_mutated=false")
    typer.echo("baseline_config_mutated=false")
    typer.echo("official_target_weights_mutated=false")
    typer.echo("automatic_candidate_promotion=false")
    typer.echo("auto_enrollment_without_owner_approval=false")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@dynamic_rescue_app.command("report")
def dynamic_rescue_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="只读展示 latest dynamic rescue report。"),
    ] = False,
    report_output_dir: Annotated[
        Path,
        typer.Option("--report-output-dir", help="dynamic rescue report 输出目录。"),
    ] = DEFAULT_DYNAMIC_RESCUE_REPORT_DIR,
) -> None:
    """只读展示 latest TRADING-088 dynamic rescue report。"""
    if not latest:
        raise typer.BadParameter("dynamic-rescue report currently supports --latest")
    resolved = latest_dynamic_rescue_report_path(report_output_dir)
    payload = _load_optional_json_payload(resolved)
    if not payload:
        raise typer.BadParameter("dynamic rescue report not found")
    summary = _mapping_obj(payload.get("improvement_summary"))
    typer.echo(f"dynamic_rescue_report={resolved}")
    typer.echo(f"status={payload.get('status')}")
    typer.echo(f"best_rescue_candidate={summary.get('best_candidate')}")
    typer.echo(f"best_status={summary.get('best_status')}")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("shadow_enrollment_allowed=false")


@dynamic_rescue_app.command("validate")
def dynamic_rescue_validate_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="dynamic failure diagnostics config。"),
    ] = DEFAULT_DYNAMIC_FAILURE_DIAGNOSTICS_POLICY_CONFIG_PATH,
    dynamic_robustness_config_path: Annotated[
        Path,
        typer.Option(
            "--dynamic-robustness-config",
            help="TRADING-086 dynamic robustness policy config。",
        ),
    ] = DEFAULT_DYNAMIC_ROBUSTNESS_POLICY_CONFIG_PATH,
    dynamic_allocation_config_path: Annotated[
        Path,
        typer.Option(
            "--dynamic-allocation-config",
            help="TRADING-084 dynamic allocation policy config。",
        ),
    ] = DEFAULT_DYNAMIC_ALLOCATION_POLICY_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="validation report 输出目录。"),
    ] = DEFAULT_DYNAMIC_RESCUE_VALIDATION_DIR,
) -> None:
    """校验 TRADING-088 dynamic rescue workflow 和 safety boundary。"""
    payload = build_dynamic_rescue_validation_report(
        policy_config_path=config_path,
        dynamic_robustness_policy_path=dynamic_robustness_config_path,
        dynamic_policy_path=dynamic_allocation_config_path,
    )
    paths = write_dynamic_rescue_validation_report(payload, output_dir=output_dir)
    typer.echo(f"ETF dynamic rescue validation JSON：{paths['json']}")
    typer.echo(f"ETF dynamic rescue validation Markdown：{paths['markdown']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("shadow_enrollment_allowed=false")
    typer.echo("automatic_enrollment_allowed=false")
    typer.echo("owner_approval_executed=false")
    typer.echo("commands_executed=false")
    typer.echo("production_state_mutated=false")
    typer.echo("baseline_config_mutated=false")
    typer.echo("official_target_weights_mutated=false")
    typer.echo("automatic_candidate_promotion=false")
    typer.echo("auto_enrollment_without_owner_approval=false")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v2_review_app.command("package")
def dynamic_v2_review_package_command(
    latest_rescue_report: Annotated[
        bool,
        typer.Option(
            "--latest-rescue-report/--no-latest-rescue-report",
            help="读取 latest TRADING-088 dynamic rescue report。",
        ),
    ] = True,
    rescue_report_path: Annotated[
        Path | None,
        typer.Option("--dynamic-rescue-report", "--rescue-report", help="TRADING-088 JSON。"),
    ] = None,
    candidate_robustness_report_path: Annotated[
        Path | None,
        typer.Option(
            "--candidate-robustness-report",
            "--dynamic-robustness-report",
            help="v0.4 candidate TRADING-086 robustness JSON。",
        ),
    ] = None,
    dynamic_shadow_package_path: Annotated[
        Path | None,
        typer.Option("--dynamic-shadow-package", help="optional TRADING-087 package JSON。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="dynamic v0.2 review policy config。"),
    ] = DEFAULT_DYNAMIC_V2_REVIEW_POLICY_CONFIG_PATH,
    rescue_report_dir: Annotated[
        Path,
        typer.Option("--rescue-report-dir", help="dynamic rescue report 目录。"),
    ] = DEFAULT_DYNAMIC_RESCUE_REPORT_DIR,
    candidate_robustness_report_dir: Annotated[
        Path,
        typer.Option("--candidate-robustness-report-dir", help="dynamic robustness report 目录。"),
    ] = DEFAULT_DYNAMIC_ROBUSTNESS_REPORT_DIR,
    dynamic_shadow_package_dir: Annotated[
        Path,
        typer.Option("--dynamic-shadow-package-dir", help="dynamic shadow package 目录。"),
    ] = DEFAULT_DYNAMIC_SHADOW_PACKAGE_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="dynamic v0.2 review package 输出目录。"),
    ] = DEFAULT_DYNAMIC_V2_REVIEW_PACKAGE_DIR,
) -> None:
    """生成 TRADING-089 v0.4 review-only package；不 enroll、不 approval。"""
    if not latest_rescue_report and rescue_report_path is None:
        raise typer.BadParameter("--dynamic-rescue-report or --latest-rescue-report is required")
    try:
        rescue_report, robustness_report, shadow_package, source_paths = load_latest_review_inputs(
            rescue_report_path=rescue_report_path,
            candidate_robustness_report_path=candidate_robustness_report_path,
            shadow_package_path=dynamic_shadow_package_path,
            rescue_report_dir=rescue_report_dir,
            candidate_robustness_report_dir=candidate_robustness_report_dir,
            shadow_package_dir=dynamic_shadow_package_dir,
        )
        payload = build_dynamic_v2_review_package(
            rescue_report=rescue_report,
            candidate_robustness_report=robustness_report,
            shadow_package=shadow_package,
            policy=load_dynamic_v2_review_policy_config(config_path),
            source_paths=source_paths,
        )
    except DynamicV2ReviewError as exc:
        raise typer.BadParameter(str(exc)) from exc
    paths = write_dynamic_v2_review_package(payload, output_dir=output_dir)
    gate = _mapping_obj(payload.get("shadow_review_eligibility_gate"))
    typer.echo(f"ETF dynamic v0.2 review package JSON：{paths['json']}")
    typer.echo(f"ETF dynamic v0.2 review package Markdown：{paths['markdown']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"review_status={payload['review_status']}")
    typer.echo(f"candidate={_mapping_obj(payload.get('candidate_evidence')).get('candidate_id')}")
    typer.echo(f"blockers={','.join(str(item) for item in gate.get('blocking_reason_codes', []))}")
    typer.echo("shadow_enrollment_allowed=false")
    typer.echo("automatic_enrollment_allowed=false")
    typer.echo("owner_approval_executed=false")
    typer.echo("commands_executed=false")
    typer.echo("production_state_mutated=false")
    typer.echo("baseline_config_mutated=false")
    typer.echo("official_target_weights_mutated=false")
    typer.echo("automatic_candidate_promotion=false")
    typer.echo("auto_enrollment_without_owner_approval=false")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@dynamic_v2_review_app.command("report")
def dynamic_v2_review_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="只读展示 latest dynamic v0.2 review package。"),
    ] = True,
    package_output_dir: Annotated[
        Path,
        typer.Option("--package-output-dir", help="dynamic v0.2 review package 目录。"),
    ] = DEFAULT_DYNAMIC_V2_REVIEW_PACKAGE_DIR,
) -> None:
    """只读展示 latest TRADING-089 dynamic v0.2 review package。"""
    if not latest:
        raise typer.BadParameter("dynamic-v2-review report currently supports --latest")
    resolved = latest_dynamic_v2_review_package_path(package_output_dir)
    payload = _load_optional_json_payload(resolved)
    if not payload:
        raise typer.BadParameter("dynamic v0.2 review package not found")
    gate = _mapping_obj(payload.get("shadow_review_eligibility_gate"))
    typer.echo(f"dynamic_v2_review_package={resolved}")
    typer.echo(f"status={payload.get('status')}")
    typer.echo(f"review_status={payload.get('review_status')}")
    typer.echo(f"blockers={','.join(str(item) for item in gate.get('blocking_reason_codes', []))}")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("shadow_enrollment_allowed=false")


@dynamic_v2_review_app.command("validate")
def dynamic_v2_review_validate_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="dynamic v0.2 review policy config。"),
    ] = DEFAULT_DYNAMIC_V2_REVIEW_POLICY_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="validation report 输出目录。"),
    ] = DEFAULT_DYNAMIC_V2_REVIEW_VALIDATION_DIR,
) -> None:
    """校验 TRADING-089 dynamic v0.2 review-only workflow 和 safety boundary。"""
    payload = build_dynamic_v2_review_validation_report(config_path=config_path)
    paths = write_dynamic_v2_review_validation_report(payload, output_dir=output_dir)
    typer.echo(f"ETF dynamic v0.2 review validation JSON：{paths['json']}")
    typer.echo(f"ETF dynamic v0.2 review validation Markdown：{paths['markdown']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("shadow_enrollment_allowed=false")
    typer.echo("automatic_enrollment_allowed=false")
    typer.echo("owner_approval_executed=false")
    typer.echo("commands_executed=false")
    typer.echo("production_state_mutated=false")
    typer.echo("baseline_config_mutated=false")
    typer.echo("official_target_weights_mutated=false")
    typer.echo("automatic_candidate_promotion=false")
    typer.echo("auto_enrollment_without_owner_approval=false")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_rescue_app.command("run")
def dynamic_v3_rescue_run_command(
    latest_v2_review: Annotated[
        bool,
        typer.Option(
            "--latest-v2-review/--no-latest-v2-review",
            help="读取 latest TRADING-089 dynamic v0.2 review package。",
        ),
    ] = True,
    v2_review_package_path: Annotated[
        Path | None,
        typer.Option(
            "--v2-review-package",
            "--v0-4-review-package",
            help="Explicit TRADING-089 v0.4 review package JSON。",
        ),
    ] = None,
    base_candidate: Annotated[
        str | None,
        typer.Option(
            "--base-candidate",
            help="Base candidate policy id；默认 v0.4 lower_turnover。",
        ),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="dynamic v0.3 rescue policy config。"),
    ] = DEFAULT_DYNAMIC_V3_RESCUE_POLICY_CONFIG_PATH,
    v2_review_package_dir: Annotated[
        Path,
        typer.Option("--v2-review-package-dir", help="dynamic v0.2 review package 目录。"),
    ] = DEFAULT_DYNAMIC_V2_REVIEW_PACKAGE_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", "--report-output-dir", help="dynamic v0.3 rescue 输出目录。"),
    ] = DEFAULT_DYNAMIC_V3_RESCUE_REPORT_DIR,
) -> None:
    """生成 TRADING-090 v0.3 constraint-aware rescue evaluation report；不 enroll。"""
    if not latest_v2_review and v2_review_package_path is None:
        raise typer.BadParameter("--v2-review-package or --latest-v2-review is required")
    try:
        policy = load_dynamic_v3_rescue_policy_config(config_path)
        if base_candidate is not None and base_candidate != policy.base_candidate:
            raise DynamicV3RescueError(
                f"TRADING-090 base candidate must be {policy.base_candidate}"
            )
        v2_package, source_paths = load_latest_v3_rescue_inputs(
            v2_review_package_path=v2_review_package_path,
            v2_review_package_dir=v2_review_package_dir,
        )
        payload = build_dynamic_v3_rescue_evaluation_report(
            v04_review_package=v2_package,
            policy=policy,
            v04_review_package_path=source_paths.get("v0_4_review_package"),
        )
    except DynamicV3RescueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    paths = write_dynamic_v3_rescue_evaluation_report(payload, output_dir=output_dir)
    best = _mapping_obj(payload.get("best_candidate"))
    typer.echo(f"ETF dynamic v0.3 rescue report JSON：{paths['json']}")
    typer.echo(f"ETF dynamic v0.3 rescue report Markdown：{paths['markdown']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"review_status={payload['review_status']}")
    typer.echo(f"best_candidate={best.get('policy_id')}")
    typer.echo(f"best_candidate_status={best.get('candidate_status')}")
    typer.echo("shadow_enrollment_allowed=false")
    typer.echo("automatic_enrollment_allowed=false")
    typer.echo("owner_approval_executed=false")
    typer.echo("commands_executed=false")
    typer.echo("production_state_mutated=false")
    typer.echo("baseline_config_mutated=false")
    typer.echo("official_target_weights_mutated=false")
    typer.echo("automatic_candidate_promotion=false")
    typer.echo("auto_enrollment_without_owner_approval=false")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@dynamic_v3_rescue_app.command("report")
def dynamic_v3_rescue_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="只读展示 latest dynamic v0.3 rescue report。"),
    ] = True,
    report_output_dir: Annotated[
        Path,
        typer.Option("--report-output-dir", help="dynamic v0.3 rescue report 目录。"),
    ] = DEFAULT_DYNAMIC_V3_RESCUE_REPORT_DIR,
) -> None:
    """只读展示 latest TRADING-090 dynamic v0.3 rescue report。"""
    if not latest:
        raise typer.BadParameter("dynamic-v3-rescue report currently supports --latest")
    resolved = latest_dynamic_v3_rescue_report_path(report_output_dir)
    payload = _load_optional_json_payload(resolved)
    if not payload:
        raise typer.BadParameter("dynamic v0.3 rescue report not found")
    best = _mapping_obj(payload.get("best_candidate"))
    typer.echo(f"dynamic_v3_rescue_report={resolved}")
    typer.echo(f"status={payload.get('status')}")
    typer.echo(f"review_status={payload.get('review_status')}")
    typer.echo(f"best_candidate={best.get('policy_id')}")
    typer.echo(f"best_candidate_status={best.get('candidate_status')}")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("shadow_enrollment_allowed=false")


@dynamic_v3_rescue_app.command("validate")
def dynamic_v3_rescue_validate_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="dynamic v0.3 rescue policy config。"),
    ] = DEFAULT_DYNAMIC_V3_RESCUE_POLICY_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="validation report 输出目录。"),
    ] = DEFAULT_DYNAMIC_V3_RESCUE_VALIDATION_DIR,
) -> None:
    """校验 TRADING-090 dynamic v0.3 rescue workflow 和 safety boundary。"""
    payload = build_dynamic_v3_rescue_validation_report(config_path=config_path)
    paths = write_dynamic_v3_rescue_validation_report(payload, output_dir=output_dir)
    typer.echo(f"ETF dynamic v0.3 rescue validation JSON：{paths['json']}")
    typer.echo(f"ETF dynamic v0.3 rescue validation Markdown：{paths['markdown']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("shadow_enrollment_allowed=false")
    typer.echo("automatic_enrollment_allowed=false")
    typer.echo("owner_approval_executed=false")
    typer.echo("commands_executed=false")
    typer.echo("production_state_mutated=false")
    typer.echo("baseline_config_mutated=false")
    typer.echo("official_target_weights_mutated=false")
    typer.echo("automatic_candidate_promotion=false")
    typer.echo("auto_enrollment_without_owner_approval=false")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_rescue_app.command("real-evaluate")
def dynamic_v3_rescue_real_evaluate_command(
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
        typer.Option("--start", help="real evaluation requested start date。"),
    ] = None,
    end: Annotated[
        str | None,
        typer.Option("--end", help="real evaluation requested end date。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option(
            "--config-path",
            "--config",
            help="TRADING-091 dynamic v0.3 real evaluation policy config。",
        ),
    ] = DEFAULT_DYNAMIC_V3_REAL_EVALUATION_POLICY_CONFIG_PATH,
    v3_rescue_config_path: Annotated[
        Path,
        typer.Option("--v3-rescue-config", help="TRADING-090 dynamic v0.3 rescue config。"),
    ] = DEFAULT_DYNAMIC_V3_RESCUE_POLICY_CONFIG_PATH,
    dynamic_robustness_config_path: Annotated[
        Path,
        typer.Option("--dynamic-robustness-config", help="TRADING-086 robustness config。"),
    ] = DEFAULT_DYNAMIC_ROBUSTNESS_POLICY_CONFIG_PATH,
    dynamic_allocation_config_path: Annotated[
        Path,
        typer.Option("--dynamic-allocation-config", help="TRADING-084 allocation config。"),
    ] = DEFAULT_DYNAMIC_ALLOCATION_POLICY_CONFIG_PATH,
    failure_diagnostics_config_path: Annotated[
        Path,
        typer.Option(
            "--failure-diagnostics-config",
            help="TRADING-088 dynamic failure diagnostics config。",
        ),
    ] = DEFAULT_DYNAMIC_FAILURE_DIAGNOSTICS_POLICY_CONFIG_PATH,
    data_quality_output_path: Annotated[
        Path | None,
        typer.Option("--data-quality-output-path", help="validate-data markdown output path。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", "--report-output-dir", help="real evaluation 输出目录。"),
    ] = DEFAULT_DYNAMIC_V3_REAL_EVALUATION_REPORT_DIR,
) -> None:
    """生成 TRADING-091 v0.3 rescue 真实历史评估和 promotion gate；不 promotion。"""
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
        policy = load_dynamic_v3_real_evaluation_policy_config(config_path)
        etf_config = load_etf_config_bundle()
        prices, etf_quality = load_standard_prices(
            prices_path,
            etf_config.assets,
            etf_config.strategy,
        )
        if not etf_quality.passed:
            raise DynamicV3RealEvaluationError(
                f"ETF price validation failed before v0.3 real evaluation: {etf_quality.status}"
            )
        payload = build_dynamic_v3_real_evaluation_report(
            prices=prices,
            etf_config=etf_config,
            policy=policy,
            v3_rescue_policy=load_dynamic_v3_rescue_policy_config(v3_rescue_config_path),
            dynamic_robustness_policy=load_dynamic_robustness_policy_config(
                dynamic_robustness_config_path
            ),
            dynamic_policy=load_dynamic_allocation_policy_config(dynamic_allocation_config_path),
            failure_policy=load_dynamic_failure_diagnostics_policy_config(
                failure_diagnostics_config_path
            ),
            start=_parse_date(start) if start else None,
            end=_parse_date(end) if end else None,
            data_quality_status=data_quality_report.status,
            data_quality_report=str(quality_output),
            prices_path=prices_path,
        )
    except (
        DynamicV3RealEvaluationError,
        DynamicV3RescueError,
        DynamicRobustnessError,
        DynamicRescueError,
    ) as exc:
        raise typer.BadParameter(str(exc)) from exc
    paths = write_dynamic_v3_real_evaluation_report(payload, output_dir=output_dir)
    summary = _mapping_obj(payload.get("summary"))
    best = _mapping_obj(payload.get("best_candidate"))
    typer.echo(f"ETF dynamic v0.3 real evaluation JSON：{paths['json']}")
    typer.echo(f"ETF dynamic v0.3 real evaluation Markdown：{paths['markdown']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"promotion_gate_decision={payload['promotion_gate_decision']}")
    typer.echo(f"best_candidate={best.get('policy_id')}")
    typer.echo(
        f"constraint_hit_reduction_vs_v0_4={summary.get('constraint_hit_reduction_vs_v0_4')}"
    )
    typer.echo(f"false_risk_off_delta_vs_v0_4={summary.get('false_risk_off_delta_vs_v0_4')}")
    typer.echo(f"data_quality_status={summary.get('data_quality_status')}")
    typer.echo("shadow_enrollment_allowed=false")
    typer.echo("automatic_enrollment_allowed=false")
    typer.echo("owner_approval_executed=false")
    typer.echo("commands_executed=false")
    typer.echo("production_state_mutated=false")
    typer.echo("baseline_config_mutated=false")
    typer.echo("official_target_weights_mutated=false")
    typer.echo("automatic_candidate_promotion=false")
    typer.echo("auto_enrollment_without_owner_approval=false")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@dynamic_v3_rescue_app.command("real-report")
def dynamic_v3_rescue_real_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="只读展示 latest v0.3 real evaluation。"),
    ] = True,
    report_output_dir: Annotated[
        Path,
        typer.Option("--report-output-dir", help="dynamic v0.3 real evaluation report 目录。"),
    ] = DEFAULT_DYNAMIC_V3_REAL_EVALUATION_REPORT_DIR,
) -> None:
    """只读展示 latest TRADING-091 dynamic v0.3 real evaluation report。"""
    if not latest:
        raise typer.BadParameter("dynamic-v3-rescue real-report currently supports --latest")
    resolved = latest_dynamic_v3_real_evaluation_report_path(report_output_dir)
    payload = _load_optional_json_payload(resolved)
    if not payload:
        raise typer.BadParameter("dynamic v0.3 real evaluation report not found")
    summary = _mapping_obj(payload.get("summary"))
    typer.echo(f"dynamic_v3_real_evaluation_report={resolved}")
    typer.echo(f"status={payload.get('status')}")
    typer.echo(f"promotion_gate_decision={payload.get('promotion_gate_decision')}")
    typer.echo(f"best_candidate={summary.get('best_v0_3_candidate')}")
    typer.echo(f"data_quality_status={summary.get('data_quality_status')}")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("shadow_enrollment_allowed=false")


@dynamic_v3_rescue_app.command("validate-real")
def dynamic_v3_rescue_validate_real_command(
    config_path: Annotated[
        Path,
        typer.Option(
            "--config-path",
            "--config",
            help="TRADING-091 dynamic v0.3 real evaluation policy config。",
        ),
    ] = DEFAULT_DYNAMIC_V3_REAL_EVALUATION_POLICY_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="real evaluation validation report 输出目录。"),
    ] = DEFAULT_DYNAMIC_V3_REAL_EVALUATION_VALIDATION_DIR,
) -> None:
    """校验 TRADING-091 dynamic v0.3 real evaluation workflow 和 safety boundary。"""
    payload = build_dynamic_v3_real_evaluation_validation_report(config_path=config_path)
    paths = write_dynamic_v3_real_evaluation_validation_report(payload, output_dir=output_dir)
    typer.echo(f"ETF dynamic v0.3 real evaluation validation JSON：{paths['json']}")
    typer.echo(f"ETF dynamic v0.3 real evaluation validation Markdown：{paths['markdown']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("shadow_enrollment_allowed=false")
    typer.echo("automatic_enrollment_allowed=false")
    typer.echo("owner_approval_executed=false")
    typer.echo("commands_executed=false")
    typer.echo("production_state_mutated=false")
    typer.echo("baseline_config_mutated=false")
    typer.echo("official_target_weights_mutated=false")
    typer.echo("automatic_candidate_promotion=false")
    typer.echo("auto_enrollment_without_owner_approval=false")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_rescue_app.command("failure-attribution")
def dynamic_v3_rescue_failure_attribution_command(
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
        typer.Option("--as-of", help="数据质量门禁日期，默认使用 real evaluation end date。"),
    ] = None,
    start: Annotated[
        str | None,
        typer.Option("--start", help="failure attribution requested start date override。"),
    ] = None,
    end: Annotated[
        str | None,
        typer.Option("--end", help="failure attribution requested end date override。"),
    ] = None,
    real_evaluation_report_path: Annotated[
        Path | None,
        typer.Option(
            "--real-evaluation-report",
            help="TRADING-091 dynamic v0.3 real evaluation report JSON；默认 latest。",
        ),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option(
            "--config-path",
            "--config",
            help="TRADING-092 dynamic v0.3 failure attribution policy config。",
        ),
    ] = DEFAULT_DYNAMIC_V3_FAILURE_ATTRIBUTION_POLICY_CONFIG_PATH,
    real_evaluation_config_path: Annotated[
        Path,
        typer.Option("--real-evaluation-config", help="TRADING-091 real evaluation config。"),
    ] = DEFAULT_DYNAMIC_V3_REAL_EVALUATION_POLICY_CONFIG_PATH,
    v3_rescue_config_path: Annotated[
        Path,
        typer.Option("--v3-rescue-config", help="TRADING-090 dynamic v0.3 rescue config。"),
    ] = DEFAULT_DYNAMIC_V3_RESCUE_POLICY_CONFIG_PATH,
    dynamic_robustness_config_path: Annotated[
        Path,
        typer.Option("--dynamic-robustness-config", help="TRADING-086 robustness config。"),
    ] = DEFAULT_DYNAMIC_ROBUSTNESS_POLICY_CONFIG_PATH,
    dynamic_allocation_config_path: Annotated[
        Path,
        typer.Option("--dynamic-allocation-config", help="TRADING-084 allocation config。"),
    ] = DEFAULT_DYNAMIC_ALLOCATION_POLICY_CONFIG_PATH,
    failure_diagnostics_config_path: Annotated[
        Path,
        typer.Option(
            "--failure-diagnostics-config",
            help="TRADING-088 dynamic failure diagnostics config。",
        ),
    ] = DEFAULT_DYNAMIC_FAILURE_DIAGNOSTICS_POLICY_CONFIG_PATH,
    data_quality_output_path: Annotated[
        Path | None,
        typer.Option("--data-quality-output-path", help="validate-data markdown output path。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option(
            "--output-dir",
            "--report-output-dir",
            help="failure attribution 输出目录。",
        ),
    ] = DEFAULT_DYNAMIC_V3_FAILURE_ATTRIBUTION_REPORT_DIR,
) -> None:
    """生成 TRADING-092 v0.3 reject 归因和 v0.4 promotion review；不 promotion。"""
    try:
        attribution_policy = load_dynamic_v3_failure_attribution_policy_config(config_path)
        resolved_real_report = (
            real_evaluation_report_path
            or latest_dynamic_v3_failure_attribution_real_evaluation_path(attribution_policy)
        )
        if resolved_real_report is None:
            raise DynamicV3FailureAttributionError(
                "latest dynamic v0.3 real evaluation report not found"
            )
        real_report = load_dynamic_v3_failure_attribution_json_artifact(resolved_real_report)
    except DynamicV3FailureAttributionError as exc:
        raise typer.BadParameter(str(exc)) from exc
    requested = _mapping_obj(real_report.get("requested_range"))
    default_as_of = (
        _parse_date(as_of)
        if as_of
        else (
            _parse_date(end)
            if end
            else _parse_date(str(requested.get("end") or "")) or date.today()
        )
    )
    quality_output = data_quality_output_path or default_quality_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        default_as_of,
    )
    universe = load_universe()
    data_quality_report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=configured_price_tickers(universe),
        expected_rate_series=configured_rate_series(universe),
        quality_config=load_data_quality(),
        as_of=default_as_of,
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
        etf_config = load_etf_config_bundle()
        prices, etf_quality = load_standard_prices(
            prices_path,
            etf_config.assets,
            etf_config.strategy,
        )
        if not etf_quality.passed:
            raise DynamicV3FailureAttributionError(
                f"ETF price validation failed before v0.3 failure attribution: {etf_quality.status}"
            )
        payload = build_dynamic_v3_failure_attribution_report(
            prices=prices,
            etf_config=etf_config,
            policy=attribution_policy,
            real_evaluation_report=real_report,
            real_evaluation_report_path=resolved_real_report,
            real_policy=load_dynamic_v3_real_evaluation_policy_config(real_evaluation_config_path),
            v3_rescue_policy=load_dynamic_v3_rescue_policy_config(v3_rescue_config_path),
            dynamic_robustness_policy=load_dynamic_robustness_policy_config(
                dynamic_robustness_config_path
            ),
            dynamic_policy=load_dynamic_allocation_policy_config(dynamic_allocation_config_path),
            failure_policy=load_dynamic_failure_diagnostics_policy_config(
                failure_diagnostics_config_path
            ),
            start=_parse_date(start) if start else None,
            end=_parse_date(end) if end else None,
            data_quality_status=data_quality_report.status,
            data_quality_report=str(quality_output),
            prices_path=prices_path,
        )
    except (
        DynamicV3FailureAttributionError,
        DynamicV3RealEvaluationError,
        DynamicV3RescueError,
        DynamicRobustnessError,
        DynamicRescueError,
    ) as exc:
        raise typer.BadParameter(str(exc)) from exc
    paths = write_dynamic_v3_failure_attribution_report(payload, output_dir=output_dir)
    summary = _mapping_obj(payload.get("summary"))
    typer.echo(f"ETF dynamic v0.3 failure attribution JSON：{paths['json']}")
    typer.echo(f"ETF dynamic v0.3 failure attribution Markdown：{paths['markdown']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"v0_3_rejection_primary_reason={summary.get('v0_3_rejection_primary_reason')}")
    typer.echo(f"v0_4_promotion_review={summary.get('v0_4_promotion_review')}")
    typer.echo(f"v0_5_design_recommendation={summary.get('v0_5_design_recommendation')}")
    typer.echo(f"data_quality_status={summary.get('data_quality_status')}")
    typer.echo("shadow_enrollment_allowed=false")
    typer.echo("automatic_enrollment_allowed=false")
    typer.echo("owner_approval_executed=false")
    typer.echo("commands_executed=false")
    typer.echo("production_state_mutated=false")
    typer.echo("baseline_config_mutated=false")
    typer.echo("official_target_weights_mutated=false")
    typer.echo("automatic_candidate_promotion=false")
    typer.echo("auto_enrollment_without_owner_approval=false")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@dynamic_v3_rescue_app.command("failure-attribution-report")
def dynamic_v3_rescue_failure_attribution_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="只读展示 latest v0.3 attribution。"),
    ] = True,
    report_output_dir: Annotated[
        Path,
        typer.Option("--report-output-dir", help="dynamic v0.3 attribution report 目录。"),
    ] = DEFAULT_DYNAMIC_V3_FAILURE_ATTRIBUTION_REPORT_DIR,
) -> None:
    """只读展示 latest TRADING-092 dynamic v0.3 failure attribution report。"""
    if not latest:
        raise typer.BadParameter(
            "dynamic-v3-rescue failure-attribution-report currently supports --latest"
        )
    resolved = latest_dynamic_v3_failure_attribution_report_path(report_output_dir)
    payload = _load_optional_json_payload(resolved)
    if not payload:
        raise typer.BadParameter("dynamic v0.3 failure attribution report not found")
    summary = _mapping_obj(payload.get("summary"))
    typer.echo(f"dynamic_v3_failure_attribution_report={resolved}")
    typer.echo(f"status={payload.get('status')}")
    typer.echo(f"v0_3_rejection_primary_reason={summary.get('v0_3_rejection_primary_reason')}")
    typer.echo(f"v0_4_promotion_review={summary.get('v0_4_promotion_review')}")
    typer.echo(f"v0_5_design_recommendation={summary.get('v0_5_design_recommendation')}")
    typer.echo(f"data_quality_status={summary.get('data_quality_status')}")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("shadow_enrollment_allowed=false")


@dynamic_v3_rescue_app.command("validate-attribution")
def dynamic_v3_rescue_validate_attribution_command(
    config_path: Annotated[
        Path,
        typer.Option(
            "--config-path",
            "--config",
            help="TRADING-092 dynamic v0.3 failure attribution policy config。",
        ),
    ] = DEFAULT_DYNAMIC_V3_FAILURE_ATTRIBUTION_POLICY_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="failure attribution validation report 输出目录。"),
    ] = DEFAULT_DYNAMIC_V3_FAILURE_ATTRIBUTION_VALIDATION_DIR,
) -> None:
    """校验 TRADING-092 dynamic failure attribution workflow 和 safety boundary。"""
    payload = build_dynamic_v3_failure_attribution_validation_report(config_path=config_path)
    paths = write_dynamic_v3_failure_attribution_validation_report(
        payload,
        output_dir=output_dir,
    )
    typer.echo(f"ETF dynamic v0.3 failure attribution validation JSON：{paths['json']}")
    typer.echo(f"ETF dynamic v0.3 failure attribution validation Markdown：{paths['markdown']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("shadow_enrollment_allowed=false")
    typer.echo("automatic_enrollment_allowed=false")
    typer.echo("owner_approval_executed=false")
    typer.echo("commands_executed=false")
    typer.echo("production_state_mutated=false")
    typer.echo("baseline_config_mutated=false")
    typer.echo("official_target_weights_mutated=false")
    typer.echo("automatic_candidate_promotion=false")
    typer.echo("auto_enrollment_without_owner_approval=false")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_sweep_config_app.command("validate")
def dynamic_v3_sweep_config_validate_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", "--config-path", help="parameter sweep config。"),
    ] = DEFAULT_PARAMETER_SWEEP_CONFIG_PATH,
) -> None:
    """校验 TRADING-093 parameter sweep config contract。"""
    payload = build_sweep_config_validation(config_path=config_path)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"config_path={config_path}")
    typer.echo(f"candidate_preview_count={payload.get('candidate_preview_count')}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_candidate_generated=false")
    typer.echo("production_effect=none")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_sweep_config_app.command("preview")
def dynamic_v3_sweep_config_preview_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", "--config-path", help="parameter sweep config。"),
    ] = DEFAULT_PARAMETER_SWEEP_CONFIG_PATH,
    limit: Annotated[int, typer.Option("--limit", help="preview candidate count。")] = 20,
) -> None:
    """预览 TRADING-093 parameter sweep candidates。"""
    try:
        payload = preview_sweep_candidates(config_path=config_path, limit=limit)
    except DynamicV3ParameterResearchError as exc:
        raise typer.BadParameter(str(exc)) from exc
    typer.echo(f"status={payload['status']}")
    typer.echo(f"candidate_count={payload['candidate_count']}")
    typer.echo(f"preview_count={payload['preview_count']}")
    for row in payload["candidates"]:
        typer.echo(f"{row['candidate_id']} {json.dumps(row['parameters'], sort_keys=True)}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_data_audit_app.command("run")
def dynamic_v3_data_audit_run_command(
    as_of: Annotated[str, typer.Option("--as-of", help="data audit as-of date。")],
    end: Annotated[str, typer.Option("--end", help="data audit end date。")],
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="标准化 ETF daily price cache。"),
    ] = DEFAULT_ETF_PRICE_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="标准化 FRED rates cache。"),
    ] = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv",
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="data audit artifact root。"),
    ] = DEFAULT_DATA_AUDIT_DIR,
) -> None:
    """运行 TRADING-103 research data manifest / PIT coverage audit。"""
    result = run_data_audit(
        as_of=_parse_date(as_of),
        end=_parse_date(end),
        prices_path=prices_path,
        rates_path=rates_path,
        output_dir=output_dir,
    )
    report = result["report"]
    typer.echo(f"data_audit_id={result['data_audit_id']}")
    typer.echo(f"data_audit_dir={result['data_audit_dir']}")
    typer.echo(f"status={report['status']}")
    typer.echo(f"data_quality_status={report['data_quality_status']}")
    typer.echo(
        "prices_download_manifest_checksum_missing="
        f"{str(report['prices_download_manifest_checksum_missing']).lower()}"
    )
    typer.echo("production_candidate_generated=false")


@dynamic_v3_data_audit_app.command("report")
def dynamic_v3_data_audit_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest data audit pointer。"),
    ] = False,
    audit_id: Annotated[str | None, typer.Option("--audit-id", help="data audit id。")] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="data audit artifact root。"),
    ] = DEFAULT_DATA_AUDIT_DIR,
) -> None:
    """展示 TRADING-103 data audit 摘要。"""
    payload = data_audit_report_payload(
        data_audit_id=audit_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"data_audit_id={payload['data_audit_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"data_quality_status={payload['data_quality_status']}")
    typer.echo(f"report_path={payload['report_path']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_rescue_app.command("validate-data-audit")
def dynamic_v3_validate_data_audit_command(
    audit_id: Annotated[str, typer.Option("--audit-id", help="data audit id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="data audit artifact root。"),
    ] = DEFAULT_DATA_AUDIT_DIR,
) -> None:
    """校验 TRADING-103 data audit artifacts。"""
    payload = validate_data_audit_artifact(data_audit_id=audit_id, output_dir=output_dir)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_candidate_generated=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_data_provenance_app.command("inspect-price-cache")
def dynamic_v3_data_provenance_inspect_price_cache_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="标准化 ETF daily price cache。"),
    ] = DEFAULT_ETF_PRICE_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="标准化 FRED rates cache。"),
    ] = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv",
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="data provenance artifact root。"),
    ] = DEFAULT_DATA_PROVENANCE_DIR,
) -> None:
    """检查 TRADING-113 price cache checksum provenance。"""
    payload = data_provenance_inspect_price_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"prices_sha256={_mapping_obj(payload.get('prices')).get('sha256')}")
    typer.echo(f"download_manifest_status={payload['download_manifest_status']}")
    typer.echo(f"provenance_status={payload['provenance_status']}")
    typer.echo(f"prices_checksum_in_manifest={str(payload['prices_checksum_in_manifest']).lower()}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_data_provenance_app.command("repair-price-manifest")
def dynamic_v3_data_provenance_repair_price_manifest_command(
    mode: Annotated[
        str,
        typer.Option("--mode", help="repair mode；当前支持 reconstruct-from-cache。"),
    ] = "reconstruct-from-cache",
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="标准化 ETF daily price cache。"),
    ] = DEFAULT_ETF_PRICE_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="标准化 FRED rates cache。"),
    ] = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv",
) -> None:
    """从现有 cache 重建下载 manifest，不伪造原始下载事件。"""
    try:
        payload = data_provenance_repair_price_manifest(
            mode=mode,
            prices_path=prices_path,
            rates_path=rates_path,
        )
    except DynamicV3ParameterResearchError as exc:
        raise typer.BadParameter(str(exc)) from exc
    typer.echo(f"status={payload['status']}")
    typer.echo(f"reconstructed_manifest_path={payload['reconstructed_manifest_path']}")
    typer.echo(f"provenance_status={payload['provenance_status']}")
    typer.echo("limitations=original_download_event_not_available")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_data_provenance_app.command("validate")
def dynamic_v3_data_provenance_validate_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="标准化 ETF daily price cache。"),
    ] = DEFAULT_ETF_PRICE_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="标准化 FRED rates cache。"),
    ] = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv",
) -> None:
    """校验 TRADING-113 price cache provenance。"""
    payload = data_provenance_validate(prices_path=prices_path, rates_path=rates_path)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo(f"provenance_status={payload['provenance_status']}")
    typer.echo("production_candidate_generated=false")
    if payload["status"] == "FAIL":
        raise typer.Exit(code=1)


@dynamic_v3_window_audit_app.command("run")
def dynamic_v3_window_audit_run_command(
    as_of: Annotated[str, typer.Option("--as-of", help="requested window start date。")],
    end: Annotated[str, typer.Option("--end", help="requested window end date。")],
    artifact_root: Annotated[
        Path,
        typer.Option("--artifact-root", help="待扫描 artifact root。"),
    ] = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="window audit artifact root。"),
    ] = DEFAULT_WINDOW_AUDIT_DIR,
) -> None:
    """运行 TRADING-111 backtest window audit。"""
    result = run_window_audit(
        as_of=_parse_date(as_of),
        end=_parse_date(end),
        artifact_root=artifact_root,
        output_dir=output_dir,
    )
    report = result["report"]
    typer.echo(f"window_audit_id={result['window_audit_id']}")
    typer.echo(f"window_audit_dir={result['window_audit_dir']}")
    typer.echo(f"status={report['status']}")
    typer.echo(f"configured_backtest_start={report['configured_backtest_start']}")
    typer.echo(f"earliest_actual_evaluation_start={report['earliest_actual_evaluation_start']}")
    typer.echo(f"promotion_blocking_count={report['promotion_blocking_count']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_window_audit_app.command("report")
def dynamic_v3_window_audit_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest window audit pointer。"),
    ] = False,
    audit_id: Annotated[str | None, typer.Option("--audit-id", help="window audit id。")] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="window audit artifact root。"),
    ] = DEFAULT_WINDOW_AUDIT_DIR,
) -> None:
    """展示 TRADING-111 window audit 摘要。"""
    payload = window_audit_report_payload(
        audit_id=audit_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"window_audit_id={payload['window_audit_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"configured_backtest_start={payload['configured_backtest_start']}")
    typer.echo(f"earliest_actual_evaluation_start={payload['earliest_actual_evaluation_start']}")
    typer.echo(f"promotion_blocking_count={payload['promotion_blocking_count']}")
    typer.echo(f"report_path={payload['report_path']}")
    if payload.get("failure_reason"):
        typer.echo(f"failure_reason={payload['failure_reason']}")
    typer.echo("production_candidate_generated=false")
    if payload.get("failure_reason"):
        raise typer.Exit(code=1)


@dynamic_v3_window_audit_app.command("inspect-artifact")
def dynamic_v3_window_audit_inspect_artifact_command(
    artifact_path: Annotated[
        Path,
        typer.Option("--artifact-path", help="artifact JSON path。"),
    ],
) -> None:
    """检查单个 artifact 的 backtest window 状态。"""
    payload = inspect_window_artifact(artifact_path=artifact_path)
    record = _mapping_obj(payload["record"])
    typer.echo(f"status={payload['status']}")
    typer.echo(f"artifact_type={record.get('artifact_type')}")
    typer.echo(f"configured_backtest_start={record.get('configured_backtest_start')}")
    typer.echo(f"actual_evaluation_start={record.get('actual_evaluation_start')}")
    typer.echo(f"actual_evaluation_end={record.get('actual_evaluation_end')}")
    typer.echo(f"promotion_blocking={str(record.get('promotion_blocking')).lower()}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_rescue_app.command("validate-window-audit")
def dynamic_v3_validate_window_audit_command(
    audit_id: Annotated[str, typer.Option("--audit-id", help="window audit id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="window audit artifact root。"),
    ] = DEFAULT_WINDOW_AUDIT_DIR,
) -> None:
    """校验 TRADING-111 window audit artifacts。"""
    payload = validate_window_audit_artifact(audit_id=audit_id, output_dir=output_dir)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_candidate_generated=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_weight_path_app.command("validate")
def dynamic_v3_weight_path_validate_command(
    evaluation_id: Annotated[str, typer.Option("--evaluation-id", help="real evaluation id。")],
    search_root: Annotated[
        Path,
        typer.Option("--search-root", help="weight path 搜索根目录。"),
    ] = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT,
) -> None:
    """校验 TRADING-112 weight path artifacts。"""
    payload = validate_weight_path_artifact(evaluation_id=evaluation_id, search_root=search_root)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"attribution_completeness={payload['attribution_completeness']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_candidate_generated=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_weight_path_app.command("report")
def dynamic_v3_weight_path_report_command(
    evaluation_id: Annotated[str, typer.Option("--evaluation-id", help="real evaluation id。")],
    search_root: Annotated[
        Path,
        typer.Option("--search-root", help="weight path 搜索根目录。"),
    ] = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT,
) -> None:
    """展示 TRADING-112 weight path 摘要。"""
    payload = weight_path_report_payload(evaluation_id=evaluation_id, search_root=search_root)
    typer.echo(f"evaluation_id={evaluation_id}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"candidate_id={payload['candidate_id']}")
    typer.echo(f"daily_weights_path={payload['daily_weights_path']}")
    typer.echo(f"weight_path_metadata_path={payload['weight_path_metadata_path']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_sweep_app.command("profile-list")
def dynamic_v3_sweep_profile_list_command(
    profile_config_path: Annotated[
        Path,
        typer.Option("--profile-config", help="sweep profile config。"),
    ] = DEFAULT_PARAMETER_SWEEP_PROFILE_CONFIG_PATH,
) -> None:
    """列出 TRADING-104 sweep execution profiles。"""
    payload = sweep_profile_list_payload(profile_config_path=profile_config_path)
    typer.echo(f"status={payload['status']}")
    for row in payload["profiles"]:
        typer.echo(
            f"{row['profile']} evaluator_mode={row['evaluator_mode']} "
            f"max_candidates={row['max_candidates']} ci_safe={str(row['ci_safe']).lower()}"
        )
    typer.echo("production_candidate_generated=false")


@dynamic_v3_sweep_app.command("profile-validate")
def dynamic_v3_sweep_profile_validate_command(
    profile_config_path: Annotated[
        Path,
        typer.Option("--profile-config", help="sweep profile config。"),
    ] = DEFAULT_PARAMETER_SWEEP_PROFILE_CONFIG_PATH,
) -> None:
    """校验 TRADING-104 sweep execution profiles。"""
    payload = validate_sweep_profiles_payload(profile_config_path=profile_config_path)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_candidate_generated=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_sweep_app.command("run-profile")
def dynamic_v3_sweep_run_profile_command(
    profile: Annotated[str, typer.Option("--profile", help="profile name。")],
    as_of: Annotated[str | None, typer.Option("--as-of", help="sweep as-of date。")] = None,
    end: Annotated[str | None, typer.Option("--end", help="sweep end date。")] = None,
    profile_config_path: Annotated[
        Path,
        typer.Option("--profile-config", help="sweep profile config。"),
    ] = DEFAULT_PARAMETER_SWEEP_PROFILE_CONFIG_PATH,
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="real evaluator 标准化 ETF daily price cache。"),
    ] = DEFAULT_ETF_PRICE_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="real evaluator FRED rates cache。"),
    ] = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv",
    output_dir: Annotated[
        Path,
        typer.Option("--output", "--output-dir", help="sweep artifact root。"),
    ] = DEFAULT_SWEEP_OUTPUT_DIR,
) -> None:
    """按 TRADING-104 profile 运行 sweep。"""
    try:
        result = run_parameter_sweep_profile(
            profile=profile,
            profile_config_path=profile_config_path,
            as_of=_parse_date(as_of) if as_of else None,
            end=_parse_date(end) if end else None,
            prices_path=prices_path,
            rates_path=rates_path,
            output_dir=output_dir,
        )
    except DynamicV3ParameterResearchError as exc:
        raise typer.BadParameter(str(exc)) from exc
    manifest = result["manifest"]
    data_quality = _mapping_obj(manifest.get("data_quality"))
    typer.echo(f"sweep_id={result['sweep_id']}")
    typer.echo(f"sweep_dir={result['sweep_dir']}")
    typer.echo(f"profile={profile}")
    typer.echo(f"status={result['status']}")
    typer.echo(f"evaluator_mode={manifest.get('evaluator_mode')}")
    typer.echo(f"not_for_investment_decision={manifest.get('not_for_investment_decision')}")
    typer.echo(f"data_quality_status={data_quality.get('status')}")
    typer.echo(f"completed_count={manifest['completed_count']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_injection_audit_app.command("run")
def dynamic_v3_injection_audit_run_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", "--config-path", help="parameter sweep config。"),
    ] = DEFAULT_PARAMETER_SWEEP_CONFIG_PATH,
    as_of: Annotated[str, typer.Option("--as-of", help="audit as-of date。")] = "2026-06-04",
    end: Annotated[str, typer.Option("--end", help="audit end date。")] = "2026-06-04",
    max_candidates: Annotated[
        int,
        typer.Option("--max-candidates", help="audit candidate count。"),
    ] = 20,
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="real evaluator 标准化 ETF daily price cache。"),
    ] = DEFAULT_ETF_PRICE_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="real evaluator FRED rates cache。"),
    ] = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv",
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="injection audit artifact root。"),
    ] = DEFAULT_INJECTION_AUDIT_DIR,
) -> None:
    """运行 TRADING-102 parameter injection audit。"""
    try:
        result = run_injection_audit(
            config_path=config_path,
            as_of=_parse_date(as_of),
            end=_parse_date(end),
            max_candidates=max_candidates,
            prices_path=prices_path,
            rates_path=rates_path,
            output_dir=output_dir,
        )
    except DynamicV3ParameterResearchError as exc:
        raise typer.BadParameter(str(exc)) from exc
    report = result["report"]
    typer.echo(f"audit_id={result['audit_id']}")
    typer.echo(f"audit_dir={result['audit_dir']}")
    typer.echo(f"status={report['status']}")
    typer.echo(f"candidate_count={report['candidate_count']}")
    typer.echo(f"no_observed_effect_parameters={','.join(report['no_observed_effect_parameters'])}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_injection_audit_app.command("report")
def dynamic_v3_injection_audit_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest injection audit pointer。"),
    ] = False,
    audit_id: Annotated[str | None, typer.Option("--audit-id", help="injection audit id。")] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="injection audit artifact root。"),
    ] = DEFAULT_INJECTION_AUDIT_DIR,
) -> None:
    """展示 TRADING-102 injection audit 摘要。"""
    payload = injection_audit_report_payload(
        audit_id=audit_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"audit_id={payload['audit_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"candidate_count={payload['candidate_count']}")
    typer.echo(f"report_path={payload['report_path']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_rescue_app.command("validate-injection-audit")
def dynamic_v3_validate_injection_audit_command(
    audit_id: Annotated[str, typer.Option("--audit-id", help="injection audit id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="injection audit artifact root。"),
    ] = DEFAULT_INJECTION_AUDIT_DIR,
) -> None:
    """校验 TRADING-102 injection audit artifacts。"""
    payload = validate_injection_audit_artifact(audit_id=audit_id, output_dir=output_dir)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_candidate_generated=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_sweep_app.command("run")
def dynamic_v3_sweep_run_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", "--config-path", help="parameter sweep config。"),
    ] = DEFAULT_PARAMETER_SWEEP_CONFIG_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="sweep as-of date。"),
    ] = None,
    end: Annotated[
        str | None,
        typer.Option("--end", help="sweep end date。"),
    ] = None,
    workers: Annotated[
        int | None,
        typer.Option("--workers", help="worker count recorded in manifest。"),
    ] = None,
    evaluator: Annotated[
        str | None,
        typer.Option(
            "--evaluator",
            "--evaluator-mode",
            help="sweep evaluator：tiny_fixture_proxy 或 real_dynamic_v3_rescue。",
        ),
    ] = None,
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="real evaluator 标准化 ETF daily price cache。"),
    ] = DEFAULT_ETF_PRICE_PATH,
    rates_path: Annotated[
        Path,
        typer.Option(
            "--rates-path",
            help="real evaluator FRED rates cache for validate-data gate。",
        ),
    ] = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv",
    data_quality_output_path: Annotated[
        Path | None,
        typer.Option(
            "--data-quality-output-path",
            help="real evaluator validate-data markdown path。",
        ),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output", "--output-dir", help="sweep artifact root。"),
    ] = DEFAULT_SWEEP_OUTPUT_DIR,
    resume: Annotated[
        str | None,
        typer.Option("--resume", help="resume sweep_id。"),
    ] = None,
) -> None:
    """运行 TRADING-094 batch parameter sweep；不 promotion。"""
    try:
        result = run_parameter_sweep(
            config_path=config_path,
            as_of=_parse_date(as_of) if as_of else None,
            end=_parse_date(end) if end else None,
            workers=workers,
            evaluator_mode=evaluator,
            prices_path=prices_path,
            rates_path=rates_path,
            data_quality_output_path=data_quality_output_path,
            output_dir=output_dir,
            resume=resume,
        )
    except DynamicV3ParameterResearchError as exc:
        raise typer.BadParameter(str(exc)) from exc
    manifest = result["manifest"]
    typer.echo(f"sweep_id={result['sweep_id']}")
    typer.echo(f"sweep_dir={result['sweep_dir']}")
    typer.echo(f"status={result['status']}")
    typer.echo(f"evaluator_mode={manifest.get('evaluator_mode')}")
    typer.echo(f"evaluator_version={manifest.get('evaluator_version')}")
    data_quality = _mapping_obj(manifest.get("data_quality"))
    typer.echo(f"data_quality_status={data_quality.get('status')}")
    typer.echo(f"completed_count={manifest['completed_count']}")
    typer.echo(f"failed_count={manifest['failed_count']}")
    typer.echo(f"observe_only_count={manifest['observe_only_count']}")
    typer.echo(f"review_required_count={manifest['review_required_count']}")
    typer.echo(f"rejected_count={manifest['rejected_count']}")
    typer.echo("production_candidate_generated=false")
    typer.echo("production_effect=none")


@dynamic_v3_sweep_app.command("status")
def dynamic_v3_sweep_status_command(
    sweep_id: Annotated[str, typer.Option("--sweep-id", help="sweep id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output", "--output-dir", help="sweep artifact root。"),
    ] = DEFAULT_SWEEP_OUTPUT_DIR,
) -> None:
    """展示 TRADING-094 sweep status。"""
    try:
        payload = sweep_status_payload(sweep_id=sweep_id, output_dir=output_dir)
    except DynamicV3ParameterResearchError as exc:
        raise typer.BadParameter(str(exc)) from exc
    manifest = payload["manifest"]
    checkpoint = payload["checkpoint"]
    typer.echo(f"sweep_id={sweep_id}")
    typer.echo(f"status={manifest.get('status')}")
    typer.echo(f"candidate_count={manifest.get('candidate_count')}")
    typer.echo(f"completed_count={manifest.get('completed_count')}")
    typer.echo(f"failed_count={manifest.get('failed_count')}")
    typer.echo(f"last_candidate_index={checkpoint.get('last_candidate_index')}")


@dynamic_v3_sweep_app.command("validate")
def dynamic_v3_sweep_validate_command(
    sweep_id: Annotated[str, typer.Option("--sweep-id", help="sweep id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output", "--output-dir", help="sweep artifact root。"),
    ] = DEFAULT_SWEEP_OUTPUT_DIR,
) -> None:
    """校验 TRADING-094 sweep artifacts。"""
    payload = validate_sweep_artifact(sweep_id=sweep_id, output_dir=output_dir)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo(f"evaluator_mode={payload.get('evaluator_mode')}")
    typer.echo("production_candidate_generated=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_sweep_app.command("leaderboard")
def dynamic_v3_sweep_leaderboard_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest sweep pointer。"),
    ] = False,
    sweep_id: Annotated[
        str | None,
        typer.Option("--sweep-id", help="sweep id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output", "--output-dir", help="sweep artifact root。"),
    ] = DEFAULT_SWEEP_OUTPUT_DIR,
) -> None:
    """展示 TRADING-095 sweep leaderboard 摘要。"""
    resolved_sweep_id = _resolve_dynamic_v3_sweep_id(latest=latest, sweep_id=sweep_id)
    path = output_dir / resolved_sweep_id / "leaderboard.json"
    payload = _load_optional_json_payload(path) or build_sweep_leaderboard_payload(
        sweep_dir=output_dir / resolved_sweep_id
    )
    typer.echo(f"sweep_id={resolved_sweep_id}")
    typer.echo(f"status={payload.get('status')}")
    typer.echo(f"evaluator_mode={payload.get('evaluator_mode')}")
    typer.echo(f"candidate_count={payload.get('candidate_count')}")
    top = payload.get("top_eligible_candidates") or []
    if top:
        first = top[0]
        typer.echo(f"top_candidate={first.get('candidate_id')}")
        typer.echo(f"top_gate={first.get('gate')}")
        typer.echo(f"top_score={first.get('score')}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_sweep_app.command("report")
def dynamic_v3_sweep_report_command(
    sweep_id: Annotated[str, typer.Option("--sweep-id", help="sweep id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output", "--output-dir", help="sweep artifact root。"),
    ] = DEFAULT_SWEEP_OUTPUT_DIR,
) -> None:
    """展示 TRADING-095 sweep report 摘要。"""
    try:
        payload = build_sweep_report_payload(sweep_dir=output_dir / sweep_id)
    except DynamicV3ParameterResearchError as exc:
        raise typer.BadParameter(str(exc)) from exc
    summary = payload["leaderboard_summary"]
    typer.echo(f"sweep_id={sweep_id}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"top_candidate={summary.get('top_candidate')}")
    typer.echo(f"sweep_report={output_dir / sweep_id / 'sweep_report.md'}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_candidate_app.command("report")
def dynamic_v3_candidate_report_command(
    sweep_id: Annotated[str, typer.Option("--sweep-id", help="sweep id。")],
    candidate_id: Annotated[str, typer.Option("--candidate-id", help="candidate id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output", "--output-dir", help="sweep artifact root。"),
    ] = DEFAULT_SWEEP_OUTPUT_DIR,
) -> None:
    """生成并展示 TRADING-095 candidate report。"""
    try:
        payload = candidate_report_payload(
            sweep_id=sweep_id,
            candidate_id=candidate_id,
            output_dir=output_dir,
            write=True,
        )
    except DynamicV3ParameterResearchError as exc:
        raise typer.BadParameter(str(exc)) from exc
    typer.echo(f"candidate_id={candidate_id}")
    typer.echo(f"source_sweep_id={sweep_id}")
    typer.echo(f"evaluator_mode={payload.get('evaluator_mode')}")
    typer.echo(f"gate={payload['hard_gate_status']}")
    typer.echo(f"score={payload['score']}")
    typer.echo(
        "candidate_report="
        f"{output_dir / sweep_id / 'candidates' / candidate_id / 'candidate_report.json'}"
    )
    typer.echo("production_candidate_generated=false")


@dynamic_v3_candidate_app.command("attribution")
def dynamic_v3_candidate_attribution_command(
    sweep_id: Annotated[str, typer.Option("--sweep-id", help="source sweep id。")],
    candidate_id: Annotated[str, typer.Option("--candidate-id", help="candidate id。")],
    sweep_output_dir: Annotated[
        Path,
        typer.Option("--sweep-output-dir", help="sweep artifact root。"),
    ] = DEFAULT_SWEEP_OUTPUT_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="candidate attribution artifact root。"),
    ] = DEFAULT_CANDIDATE_ATTRIBUTION_DIR,
) -> None:
    """生成 TRADING-105 candidate attribution report。"""
    try:
        result = run_candidate_attribution(
            sweep_id=sweep_id,
            candidate_id=candidate_id,
            sweep_output_dir=sweep_output_dir,
            output_dir=output_dir,
        )
    except DynamicV3ParameterResearchError as exc:
        raise typer.BadParameter(str(exc)) from exc
    report = result["report"]
    typer.echo(f"candidate_id={candidate_id}")
    typer.echo(f"attribution_dir={result['attribution_dir']}")
    typer.echo(f"status={report['status']}")
    typer.echo(f"explainability_status={report['explainability_status']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_rescue_app.command("validate-candidate-attribution")
def dynamic_v3_validate_candidate_attribution_command(
    candidate_id: Annotated[str, typer.Option("--candidate-id", help="candidate id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="candidate attribution artifact root。"),
    ] = DEFAULT_CANDIDATE_ATTRIBUTION_DIR,
) -> None:
    """校验 TRADING-105 candidate attribution artifacts。"""
    payload = validate_candidate_attribution_artifact(
        candidate_id=candidate_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_candidate_generated=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_walk_forward_app.command("run")
def dynamic_v3_walk_forward_run_command(
    sweep_id: Annotated[str, typer.Option("--sweep-id", help="source sweep id。")],
    top_n: Annotated[int, typer.Option("--top-n", help="top candidate count。")] = 20,
    sweep_output_dir: Annotated[
        Path,
        typer.Option("--sweep-output-dir", help="sweep artifact root。"),
    ] = DEFAULT_SWEEP_OUTPUT_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="walk-forward artifact root。"),
    ] = DEFAULT_WALK_FORWARD_DIR,
) -> None:
    """运行 TRADING-096 walk-forward / OOS validation。"""
    try:
        result = run_walk_forward_validation(
            sweep_id=sweep_id,
            top_n=top_n,
            sweep_output_dir=sweep_output_dir,
            output_dir=output_dir,
        )
    except DynamicV3ParameterResearchError as exc:
        raise typer.BadParameter(str(exc)) from exc
    typer.echo(f"walk_forward_id={result['walk_forward_id']}")
    typer.echo(f"walk_forward_dir={result['walk_forward_dir']}")
    typer.echo(f"status={result['report']['status']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_walk_forward_app.command("select-run")
def dynamic_v3_walk_forward_select_run_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", "--config-path", help="parameter sweep config。"),
    ] = DEFAULT_PARAMETER_SWEEP_CONFIG_PATH,
    profile: Annotated[str, typer.Option("--profile", help="profile name。")] = "small_real",
    sweep_id: Annotated[str | None, typer.Option("--sweep-id", help="source sweep id。")] = None,
    sweep_output_dir: Annotated[
        Path,
        typer.Option("--sweep-output-dir", help="sweep artifact root。"),
    ] = DEFAULT_SWEEP_OUTPUT_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="walk-forward selection artifact root。"),
    ] = DEFAULT_WALK_FORWARD_SELECTION_DIR,
) -> None:
    """运行 TRADING-106 true walk-forward selection。"""
    try:
        result = run_walk_forward_selection(
            config_path=config_path,
            profile=profile,
            sweep_id=sweep_id,
            sweep_output_dir=sweep_output_dir,
            output_dir=output_dir,
        )
    except DynamicV3ParameterResearchError as exc:
        raise typer.BadParameter(str(exc)) from exc
    report = result["report"]
    typer.echo(f"wf_selection_id={result['wf_selection_id']}")
    typer.echo(f"wf_selection_dir={result['wf_selection_dir']}")
    typer.echo(f"status={report['status']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_walk_forward_app.command("selection-report")
def dynamic_v3_walk_forward_selection_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest walk-forward selection。"),
    ] = False,
    wf_selection_id: Annotated[
        str | None,
        typer.Option("--wf-selection-id", help="walk-forward selection id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="walk-forward selection artifact root。"),
    ] = DEFAULT_WALK_FORWARD_SELECTION_DIR,
) -> None:
    """展示 TRADING-106 walk-forward selection report。"""
    payload = walk_forward_selection_report_payload(
        wf_selection_id=wf_selection_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"wf_selection_id={payload['wf_selection_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"report_path={payload['report_path']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_rescue_app.command("validate-walk-forward-selection")
def dynamic_v3_validate_walk_forward_selection_command(
    wf_selection_id: Annotated[
        str,
        typer.Option("--wf-selection-id", help="walk-forward selection id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="walk-forward selection artifact root。"),
    ] = DEFAULT_WALK_FORWARD_SELECTION_DIR,
) -> None:
    """校验 TRADING-106 walk-forward selection artifacts。"""
    payload = validate_walk_forward_selection_artifact(
        wf_selection_id=wf_selection_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_candidate_generated=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_walk_forward_app.command("report")
def dynamic_v3_walk_forward_report_command(
    walk_forward_id: Annotated[str, typer.Option("--walk-forward-id", help="walk-forward id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="walk-forward artifact root。"),
    ] = DEFAULT_WALK_FORWARD_DIR,
) -> None:
    """展示 TRADING-096 walk-forward report。"""
    payload = walk_forward_report_payload(walk_forward_id=walk_forward_id, output_dir=output_dir)
    typer.echo(f"walk_forward_id={walk_forward_id}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"report_path={payload['report_path']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_rescue_app.command("validate-walk-forward")
def dynamic_v3_validate_walk_forward_command(
    walk_forward_id: Annotated[str, typer.Option("--walk-forward-id", help="walk-forward id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="walk-forward artifact root。"),
    ] = DEFAULT_WALK_FORWARD_DIR,
) -> None:
    """校验 TRADING-096 walk-forward artifacts。"""
    payload = validate_walk_forward_artifact(walk_forward_id=walk_forward_id, output_dir=output_dir)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_candidate_generated=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_overfit_app.command("run")
def dynamic_v3_overfit_run_command(
    sweep_id: Annotated[str, typer.Option("--sweep-id", help="source sweep id。")],
    candidate_id: Annotated[str, typer.Option("--candidate-id", help="candidate id。")],
    sweep_output_dir: Annotated[
        Path,
        typer.Option("--sweep-output-dir", help="sweep artifact root。"),
    ] = DEFAULT_SWEEP_OUTPUT_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="overfit artifact root。"),
    ] = DEFAULT_OVERFIT_DIR,
) -> None:
    """运行 TRADING-107 overfit risk review。"""
    try:
        result = run_overfit_review(
            sweep_id=sweep_id,
            candidate_id=candidate_id,
            sweep_output_dir=sweep_output_dir,
            output_dir=output_dir,
        )
    except DynamicV3ParameterResearchError as exc:
        raise typer.BadParameter(str(exc)) from exc
    report = result["report"]
    typer.echo(f"overfit_id={result['overfit_id']}")
    typer.echo(f"overfit_dir={result['overfit_dir']}")
    typer.echo(f"status={report['status']}")
    typer.echo(f"overfit_status={report['overfit_status']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_overfit_app.command("report")
def dynamic_v3_overfit_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest overfit pointer。"),
    ] = False,
    overfit_id: Annotated[str | None, typer.Option("--overfit-id", help="overfit id。")] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="overfit artifact root。"),
    ] = DEFAULT_OVERFIT_DIR,
) -> None:
    """展示 TRADING-107 overfit report。"""
    payload = overfit_report_payload(overfit_id=overfit_id, latest=latest, output_dir=output_dir)
    typer.echo(f"overfit_id={payload['overfit_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"overfit_status={payload['overfit_status']}")
    typer.echo(f"report_path={payload['report_path']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_rescue_app.command("validate-overfit")
def dynamic_v3_validate_overfit_command(
    overfit_id: Annotated[str, typer.Option("--overfit-id", help="overfit id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="overfit artifact root。"),
    ] = DEFAULT_OVERFIT_DIR,
) -> None:
    """校验 TRADING-107 overfit artifacts。"""
    payload = validate_overfit_artifact(overfit_id=overfit_id, output_dir=output_dir)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_candidate_generated=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_robustness_app.command("run")
def dynamic_v3_robustness_run_command(
    sweep_id: Annotated[str, typer.Option("--sweep-id", help="source sweep id。")],
    candidate_id: Annotated[str, typer.Option("--candidate-id", help="candidate id。")],
    sweep_output_dir: Annotated[
        Path,
        typer.Option("--sweep-output-dir", help="sweep artifact root。"),
    ] = DEFAULT_SWEEP_OUTPUT_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="robustness artifact root。"),
    ] = DEFAULT_ROBUSTNESS_DIR,
) -> None:
    """运行 TRADING-097 robustness diagnostics。"""
    try:
        result = run_robustness_diagnostics(
            sweep_id=sweep_id,
            candidate_id=candidate_id,
            sweep_output_dir=sweep_output_dir,
            output_dir=output_dir,
        )
    except DynamicV3ParameterResearchError as exc:
        raise typer.BadParameter(str(exc)) from exc
    report = result["report"]
    typer.echo(f"robustness_id={result['robustness_id']}")
    typer.echo(f"robustness_dir={result['robustness_dir']}")
    typer.echo(f"status={report['status']}")
    typer.echo(f"overfit_status={report['overfit_status']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_robustness_app.command("report")
def dynamic_v3_robustness_report_command(
    robustness_id: Annotated[str, typer.Option("--robustness-id", help="robustness id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="robustness artifact root。"),
    ] = DEFAULT_ROBUSTNESS_DIR,
) -> None:
    """展示 TRADING-097 robustness report。"""
    payload = robustness_report_payload(robustness_id=robustness_id, output_dir=output_dir)
    typer.echo(f"robustness_id={robustness_id}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"report_path={payload['report_path']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_rescue_app.command("validate-robustness")
def dynamic_v3_validate_robustness_command(
    robustness_id: Annotated[str, typer.Option("--robustness-id", help="robustness id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="robustness artifact root。"),
    ] = DEFAULT_ROBUSTNESS_DIR,
) -> None:
    """校验 TRADING-097 robustness artifacts。"""
    payload = validate_robustness_artifact(robustness_id=robustness_id, output_dir=output_dir)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_candidate_generated=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_shadow_app.command("register")
def dynamic_v3_shadow_register_command(
    sweep_id: Annotated[str, typer.Option("--sweep-id", help="source sweep id。")],
    candidate_id: Annotated[str, typer.Option("--candidate-id", help="candidate id。")],
    registry_path: Annotated[
        Path,
        typer.Option("--registry", "--registry-path", help="shadow registry path。"),
    ] = DEFAULT_SHADOW_REGISTRY_PATH,
    sweep_output_dir: Annotated[
        Path,
        typer.Option("--sweep-output-dir", help="sweep artifact root。"),
    ] = DEFAULT_SWEEP_OUTPUT_DIR,
) -> None:
    """登记 TRADING-098 observe-only shadow candidate。"""
    try:
        payload = register_shadow_candidate(
            sweep_id=sweep_id,
            candidate_id=candidate_id,
            registry_path=registry_path,
            sweep_output_dir=sweep_output_dir,
        )
    except DynamicV3ParameterResearchError as exc:
        raise typer.BadParameter(str(exc)) from exc
    typer.echo(f"status={payload['status']}")
    typer.echo(f"candidate_id={candidate_id}")
    typer.echo(f"registry_path={registry_path}")
    typer.echo("observe_only=true")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_shadow_app.command("list")
def dynamic_v3_shadow_list_command(
    registry_path: Annotated[
        Path,
        typer.Option("--registry", "--registry-path", help="shadow registry path。"),
    ] = DEFAULT_SHADOW_REGISTRY_PATH,
) -> None:
    """列出 TRADING-098 shadow candidates。"""
    payload = shadow_list_payload(registry_path=registry_path)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"registry_path={registry_path}")
    for row in payload["candidates"]:
        typer.echo(f"{row.get('candidate_id')} status={row.get('status')}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_shadow_app.command("report")
def dynamic_v3_shadow_report_command(
    candidate_id: Annotated[
        str | None,
        typer.Option("--candidate-id", help="candidate id。"),
    ] = None,
    all_candidates: Annotated[
        bool,
        typer.Option("--all/--no-all", help="report all shadow candidates。"),
    ] = False,
    registry_path: Annotated[
        Path,
        typer.Option("--registry", "--registry-path", help="shadow registry path。"),
    ] = DEFAULT_SHADOW_REGISTRY_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="shadow report output root。"),
    ] = DEFAULT_SHADOW_REPORT_DIR,
) -> None:
    """生成 TRADING-098 shadow report。"""
    if not all_candidates and not candidate_id:
        raise typer.BadParameter("--candidate-id or --all is required")
    try:
        payload = shadow_report_payload(
            candidate_id=candidate_id,
            all_candidates=all_candidates,
            registry_path=registry_path,
            output_dir=output_dir,
            write=True,
        )
    except DynamicV3ParameterResearchError as exc:
        raise typer.BadParameter(str(exc)) from exc
    typer.echo(f"status={payload['status']}")
    typer.echo(f"candidate_id={payload['candidate_id']}")
    typer.echo(f"report_count={len(payload['reports'])}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_shadow_app.command("monitor-run")
def dynamic_v3_shadow_monitor_run_command(
    as_of: Annotated[str, typer.Option("--as-of", help="shadow monitor as-of date。")],
    registry_path: Annotated[
        Path,
        typer.Option("--registry", "--registry-path", help="shadow registry path。"),
    ] = DEFAULT_SHADOW_REGISTRY_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="shadow monitor artifact root。"),
    ] = DEFAULT_SHADOW_MONITOR_DIR,
) -> None:
    """运行 TRADING-110 shadow monitor。"""
    result = run_shadow_monitor(
        as_of=_parse_date(as_of),
        registry_path=registry_path,
        output_dir=output_dir,
    )
    report = result["report"]
    summary = _mapping_obj(report.get("summary"))
    typer.echo(f"monitor_id={result['monitor_id']}")
    typer.echo(f"monitor_dir={result['monitor_dir']}")
    typer.echo(f"status={report['status']}")
    typer.echo(f"observe_only_candidate_count={summary.get('observe_only_candidate_count')}")
    typer.echo(f"promotion_review_ready_count={summary.get('promotion_review_ready_count')}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_shadow_app.command("monitor-report")
def dynamic_v3_shadow_monitor_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest shadow monitor。"),
    ] = False,
    monitor_id: Annotated[str | None, typer.Option("--monitor-id", help="monitor id。")] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="shadow monitor artifact root。"),
    ] = DEFAULT_SHADOW_MONITOR_DIR,
) -> None:
    """展示 TRADING-110 shadow monitor report。"""
    payload = shadow_monitor_report_payload(
        monitor_id=monitor_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"monitor_id={payload['monitor_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"candidate_count={payload['candidate_count']}")
    typer.echo(f"report_path={payload['report_path']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_rescue_app.command("validate-shadow-monitor")
def dynamic_v3_validate_shadow_monitor_command(
    monitor_id: Annotated[str, typer.Option("--monitor-id", help="monitor id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="shadow monitor artifact root。"),
    ] = DEFAULT_SHADOW_MONITOR_DIR,
) -> None:
    """校验 TRADING-110 shadow monitor artifacts。"""
    payload = validate_shadow_monitor_artifact(monitor_id=monitor_id, output_dir=output_dir)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_candidate_generated=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_rescue_app.command("validate-shadow-registry")
def dynamic_v3_validate_shadow_registry_command(
    registry_path: Annotated[
        Path,
        typer.Option("--registry", "--registry-path", help="shadow registry path。"),
    ] = DEFAULT_SHADOW_REGISTRY_PATH,
    sweep_output_dir: Annotated[
        Path,
        typer.Option("--sweep-output-dir", help="sweep artifact root。"),
    ] = DEFAULT_SWEEP_OUTPUT_DIR,
) -> None:
    """校验 TRADING-098 shadow registry。"""
    payload = validate_shadow_registry(
        registry_path=registry_path,
        sweep_output_dir=sweep_output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_candidate_generated=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_governance_app.command("validate")
def dynamic_v3_governance_validate_command(
    governance_path: Annotated[
        Path,
        typer.Option("--governance", "--config", help="parameter governance config。"),
    ] = DEFAULT_PARAMETER_GOVERNANCE_CONFIG_PATH,
    sweep_config_path: Annotated[
        Path,
        typer.Option("--sweep-config", help="parameter sweep config。"),
    ] = DEFAULT_PARAMETER_SWEEP_CONFIG_PATH,
) -> None:
    """校验 TRADING-108 parameter governance。"""
    payload = validate_parameter_governance(
        governance_path=governance_path,
        config_path=sweep_config_path,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_candidate_generated=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_governance_app.command("report")
def dynamic_v3_governance_report_command(
    governance_path: Annotated[
        Path,
        typer.Option("--governance", "--config", help="parameter governance config。"),
    ] = DEFAULT_PARAMETER_GOVERNANCE_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="governance artifact root。"),
    ] = DEFAULT_GOVERNANCE_DIR,
) -> None:
    """生成 TRADING-108 governance report。"""
    payload = governance_report_payload(
        governance_path=governance_path,
        output_dir=output_dir,
        write=True,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"policy_id={payload['policy_id']}")
    typer.echo(f"search_space_version={payload['search_space_version']}")
    typer.echo(f"governance_report={output_dir / 'parameter_governance_report.json'}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_governance_app.command("diff")
def dynamic_v3_governance_diff_command(
    old_config: Annotated[Path, typer.Option("--old-config", help="old governance config。")],
    new_config: Annotated[Path, typer.Option("--new-config", help="new governance config。")],
) -> None:
    """比较 TRADING-108 governance configs。"""
    payload = governance_diff_payload(old_config=old_config, new_config=new_config)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"change_count={payload['change_count']}")
    typer.echo(f"manual_review_required={payload['manual_review_required']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_research_app.command("index-build")
def dynamic_v3_research_index_build_command(
    sweep_output_dir: Annotated[
        Path,
        typer.Option("--sweep-output-dir", help="sweep artifact root。"),
    ] = DEFAULT_SWEEP_OUTPUT_DIR,
    registry_path: Annotated[
        Path,
        typer.Option("--registry", "--registry-path", help="shadow registry path。"),
    ] = DEFAULT_SHADOW_REGISTRY_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="research index output dir。"),
    ] = DEFAULT_RESEARCH_INDEX_DIR,
) -> None:
    """重建 TRADING-109 research result index。"""
    payload = build_research_index(
        sweep_output_dir=sweep_output_dir,
        shadow_registry_path=registry_path,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"index_dir={output_dir}")
    typer.echo(f"sweep_count={payload['sweep_count']}")
    typer.echo(f"candidate_count={payload['candidate_count']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_research_app.command("query")
def dynamic_v3_research_query_command(
    candidate_id: Annotated[str, typer.Option("--candidate-id", help="candidate id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="research index output dir。"),
    ] = DEFAULT_RESEARCH_INDEX_DIR,
) -> None:
    """查询 TRADING-109 candidate artifacts。"""
    payload = research_query_payload(candidate_id=candidate_id, output_dir=output_dir)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"candidate_id={candidate_id}")
    typer.echo(f"match_count={len(payload['matches'])}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_research_app.command("compare")
def dynamic_v3_research_compare_command(
    candidate_ids: Annotated[
        list[str],
        typer.Option("--candidate-id", help="candidate id；provide exactly two。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="research index output dir。"),
    ] = DEFAULT_RESEARCH_INDEX_DIR,
) -> None:
    """比较 TRADING-109 two candidates。"""
    if len(candidate_ids) != 2:
        raise typer.BadParameter("research compare requires exactly two --candidate-id values")
    payload = research_compare_payload(
        candidate_a=candidate_ids[0],
        candidate_b=candidate_ids[1],
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"parameter_diff_count={len(payload['parameter_diff'])}")
    typer.echo(f"metric_diff_count={len(payload['metric_diff'])}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_research_app.command("history")
def dynamic_v3_research_history_command(
    parameter: Annotated[str, typer.Option("--parameter", help="parameter name。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="research index output dir。"),
    ] = DEFAULT_RESEARCH_INDEX_DIR,
) -> None:
    """查询 TRADING-109 parameter history。"""
    payload = research_history_payload(parameter=parameter, output_dir=output_dir)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"parameter={parameter}")
    typer.echo(f"observation_count={payload['observation_count']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_artifacts_app.command("latest")
def dynamic_v3_artifacts_latest_command() -> None:
    """展示 TRADING-099 latest pointers。"""
    payload = artifacts_latest_payload()
    typer.echo(f"status={payload['status']}")
    typer.echo(f"pointer_dir={payload['pointer_dir']}")
    for name, pointer in payload["pointers"].items():
        typer.echo(f"{name}={pointer.get('artifact_id')} path={pointer.get('path')}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_artifacts_app.command("validate")
def dynamic_v3_artifacts_validate_command(
    family: Annotated[
        str,
        typer.Option("--family", help="artifact family。"),
    ] = "dynamic_v3_rescue",
) -> None:
    """校验 TRADING-099 latest pointer targets。"""
    payload = validate_artifacts_payload(family=family)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_candidate_generated=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_artifacts_app.command("repair-latest")
def dynamic_v3_artifacts_repair_latest_command(
    pointer_dir: Annotated[
        Path,
        typer.Option("--pointer-dir", help="latest pointer directory。"),
    ] = DEFAULT_LATEST_POINTER_DIR,
    artifact_root: Annotated[
        Path,
        typer.Option("--artifact-root", help="canonical dynamic-v3 artifact root。"),
    ] = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT,
) -> None:
    """从 canonical artifact root 重建 TRADING-099 latest pointers。"""
    payload = repair_latest_pointers_payload(
        pointer_dir=pointer_dir,
        artifact_root=artifact_root,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"repaired_count={payload['repaired_count']}")
    typer.echo(f"skipped_count={payload['skipped_count']}")
    validation = _mapping_obj(payload.get("validation") or {})
    if validation:
        typer.echo(f"validation_status={validation.get('status')}")
        typer.echo(f"validation_failed_check_count={validation.get('failed_check_count')}")
    typer.echo("production_candidate_generated=false")
    if payload["status"] == "FAIL":
        raise typer.Exit(code=1)


@dynamic_v3_artifacts_app.command("stale")
def dynamic_v3_artifacts_stale_command(
    family: Annotated[
        str,
        typer.Option("--family", help="artifact family。"),
    ] = "dynamic_v3_rescue",
    config_path: Annotated[
        Path,
        typer.Option("--config", "--config-path", help="parameter sweep config。"),
    ] = DEFAULT_PARAMETER_SWEEP_CONFIG_PATH,
) -> None:
    """检查 TRADING-099 stale artifacts。"""
    payload = stale_artifacts_payload(family=family, config_path=config_path)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"stale_after_days={payload['stale_after_days']}")
    typer.echo(f"stale_count={len(payload['stale_artifacts'])}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_schedule_app.command("observe")
def dynamic_v3_schedule_observe_command(
    as_of: Annotated[str, typer.Option("--as-of", help="scheduled observation as-of date。")],
    family: Annotated[
        str,
        typer.Option("--family", help="artifact family。"),
    ] = "dynamic_v3_rescue",
    config_path: Annotated[
        Path,
        typer.Option("--config", "--config-path", help="parameter sweep config。"),
    ] = DEFAULT_PARAMETER_SWEEP_CONFIG_PATH,
    pointer_dir: Annotated[
        Path,
        typer.Option("--pointer-dir", help="latest pointer directory。"),
    ] = DEFAULT_LATEST_POINTER_DIR,
    registry_path: Annotated[
        Path,
        typer.Option("--registry", "--registry-path", help="shadow registry path。"),
    ] = DEFAULT_SHADOW_REGISTRY_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="scheduled observe artifact root。"),
    ] = DEFAULT_SCHEDULE_OBSERVE_DIR,
    run_shadow_monitor: Annotated[
        bool,
        typer.Option(
            "--run-shadow-monitor/--skip-shadow-monitor",
            help="Run observe-only shadow monitor when weekly due conditions pass。",
        ),
    ] = True,
    force_due: Annotated[
        bool,
        typer.Option("--force-due", help="Force due=true for manual validation only。"),
    ] = False,
) -> None:
    """运行 TRADING-099 daily scheduler lightweight observe gate。"""
    if not as_of:
        raise typer.BadParameter("--as-of is required")
    try:
        observation_date = date.fromisoformat(as_of)
    except ValueError as exc:
        raise typer.BadParameter("--as-of must use YYYY-MM-DD") from exc
    payload = scheduled_observe_payload(
        as_of=observation_date,
        family=family,
        config_path=config_path,
        pointer_dir=pointer_dir,
        registry_path=registry_path,
        output_dir=output_dir,
        run_shadow_monitor_on_due=run_shadow_monitor,
        force_due=force_due,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"due_status={payload['due_status']}")
    typer.echo(f"pointer_count={payload['pointer_count']}")
    typer.echo(f"json={payload['output_artifacts']['json']}")
    typer.echo(f"markdown={payload['output_artifacts']['markdown']}")
    typer.echo("production_candidate_generated=false")
    if payload["status"] == "FAIL":
        raise typer.Exit(code=1)


@dynamic_v3_evidence_summary_app.command("run")
def dynamic_v3_evidence_summary_run_command(
    sweep_id: Annotated[str, typer.Option("--sweep-id", help="source sweep id。")],
    sweep_output_dir: Annotated[
        Path,
        typer.Option("--sweep-output-dir", help="sweep artifact root。"),
    ] = DEFAULT_SWEEP_OUTPUT_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="evidence summary artifact root。"),
    ] = DEFAULT_EVIDENCE_SUMMARY_DIR,
) -> None:
    """生成 TRADING-114 evidence summary。"""
    try:
        result = run_evidence_summary(
            sweep_id=sweep_id,
            sweep_output_dir=sweep_output_dir,
            output_dir=output_dir,
        )
    except DynamicV3ParameterResearchError as exc:
        raise typer.BadParameter(str(exc)) from exc
    manifest = result["manifest"]
    typer.echo(f"summary_id={result['summary_id']}")
    typer.echo(f"summary_dir={result['summary_dir']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(f"candidate_count={manifest['candidate_count']}")
    typer.echo(f"usable_for_research_count={manifest['usable_for_research_count']}")
    typer.echo(f"can_enter_medium_real={manifest['can_enter_medium_real']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_evidence_summary_app.command("report")
def dynamic_v3_evidence_summary_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest evidence summary pointer。"),
    ] = False,
    summary_id: Annotated[str | None, typer.Option("--summary-id", help="summary id。")] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="evidence summary artifact root。"),
    ] = DEFAULT_EVIDENCE_SUMMARY_DIR,
) -> None:
    """展示 TRADING-114 evidence summary 摘要。"""
    payload = evidence_summary_report_payload(
        summary_id=summary_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"summary_id={payload['summary_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"candidate_count={payload['candidate_count']}")
    typer.echo(f"usable_for_research_count={payload['usable_for_research_count']}")
    typer.echo(f"report_path={payload['report_path']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_rescue_app.command("validate-evidence-summary")
def dynamic_v3_validate_evidence_summary_command(
    summary_id: Annotated[str, typer.Option("--summary-id", help="summary id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="evidence summary artifact root。"),
    ] = DEFAULT_EVIDENCE_SUMMARY_DIR,
) -> None:
    """校验 TRADING-114 evidence summary artifact。"""
    payload = validate_evidence_summary_artifact(summary_id=summary_id, output_dir=output_dir)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_candidate_generated=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_medium_real_app.command("report")
def dynamic_v3_medium_real_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest medium_real report。"),
    ] = False,
    sweep_id: Annotated[str | None, typer.Option("--sweep-id", help="source sweep id。")] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="medium_real report artifact root。"),
    ] = DEFAULT_MEDIUM_REAL_DIR,
    sweep_output_dir: Annotated[
        Path,
        typer.Option("--sweep-output-dir", help="sweep artifact root。"),
    ] = DEFAULT_SWEEP_OUTPUT_DIR,
) -> None:
    """生成或展示 TRADING-115 medium_real report。"""
    payload = (
        medium_real_report_payload(
            latest=True,
            output_dir=output_dir,
            sweep_output_dir=sweep_output_dir,
        )
        if latest and sweep_id is None
        else build_medium_real_report(
            sweep_id=sweep_id,
            latest=latest,
            sweep_output_dir=sweep_output_dir,
            output_dir=output_dir,
        )
    )
    typer.echo(f"medium_real_report_id={payload['medium_real_report_id']}")
    typer.echo(f"source_sweep_id={payload['source_sweep_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"candidate_count={payload['candidate_count']}")
    typer.echo(f"completed_count={payload['completed_count']}")
    typer.echo(f"failed_count={payload['failed_count']}")
    typer.echo(f"observe_only_count={payload['observe_only_count']}")
    typer.echo(f"report_path={payload['medium_real_report_path']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_rescue_app.command("validate-medium-real")
def dynamic_v3_validate_medium_real_command(
    sweep_id: Annotated[str, typer.Option("--sweep-id", help="medium_real sweep id。")],
    sweep_output_dir: Annotated[
        Path,
        typer.Option("--sweep-output-dir", help="sweep artifact root。"),
    ] = DEFAULT_SWEEP_OUTPUT_DIR,
) -> None:
    """校验 TRADING-115 medium_real sweep artifact。"""
    payload = validate_medium_real_sweep(sweep_id=sweep_id, sweep_output_dir=sweep_output_dir)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"completed_count={payload['completed_count']}")
    typer.echo(f"failed_count={payload['failed_count']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_candidate_generated=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_regime_coverage_app.command("run")
def dynamic_v3_regime_coverage_run_command(
    sweep_id: Annotated[str, typer.Option("--sweep-id", help="source sweep id。")],
    focus: Annotated[str, typer.Option("--focus", help="coverage focus。")] = "tech_semiconductor",
    sweep_output_dir: Annotated[
        Path,
        typer.Option("--sweep-output-dir", help="sweep artifact root。"),
    ] = DEFAULT_SWEEP_OUTPUT_DIR,
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="standardized ETF price cache。"),
    ] = DEFAULT_ETF_PRICE_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="regime coverage artifact root。"),
    ] = DEFAULT_REGIME_COVERAGE_DIR,
) -> None:
    """生成 TRADING-116 tech / semiconductor regime coverage audit。"""
    try:
        result = run_regime_coverage(
            sweep_id=sweep_id,
            focus=focus,
            sweep_output_dir=sweep_output_dir,
            prices_path=prices_path,
            output_dir=output_dir,
        )
    except DynamicV3ParameterResearchError as exc:
        raise typer.BadParameter(str(exc)) from exc
    manifest = result["manifest"]
    typer.echo(f"coverage_id={result['coverage_id']}")
    typer.echo(f"coverage_dir={result['coverage_dir']}")
    typer.echo(f"coverage_status={manifest['coverage_status']}")
    typer.echo(f"tech_semiconductor_relevance={manifest['tech_semiconductor_relevance']}")
    typer.echo(f"ai_bull_market_overfit_risk={manifest['ai_bull_market_overfit_risk']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_regime_coverage_app.command("report")
def dynamic_v3_regime_coverage_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest regime coverage pointer。"),
    ] = False,
    coverage_id: Annotated[str | None, typer.Option("--coverage-id", help="coverage id。")] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="regime coverage artifact root。"),
    ] = DEFAULT_REGIME_COVERAGE_DIR,
) -> None:
    """展示 TRADING-116 regime coverage 摘要。"""
    payload = regime_coverage_report_payload(
        coverage_id=coverage_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"coverage_id={payload['coverage_id']}")
    typer.echo(f"coverage_status={payload['coverage_status']}")
    typer.echo(f"tech_semiconductor_relevance={payload['tech_semiconductor_relevance']}")
    typer.echo(f"ai_bull_market_overfit_risk={payload['ai_bull_market_overfit_risk']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_rescue_app.command("validate-regime-coverage")
def dynamic_v3_validate_regime_coverage_command(
    coverage_id: Annotated[str, typer.Option("--coverage-id", help="coverage id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="regime coverage artifact root。"),
    ] = DEFAULT_REGIME_COVERAGE_DIR,
) -> None:
    """校验 TRADING-116 regime coverage artifact。"""
    payload = validate_regime_coverage_artifact(coverage_id=coverage_id, output_dir=output_dir)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_candidate_generated=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_candidate_app.command("interpretation-pack")
def dynamic_v3_candidate_interpretation_pack_command(
    sweep_id: Annotated[str, typer.Option("--sweep-id", help="source sweep id。")],
    top_n: Annotated[int, typer.Option("--top-n", help="top candidate count。")] = 10,
    sweep_output_dir: Annotated[
        Path,
        typer.Option("--sweep-output-dir", help="sweep artifact root。"),
    ] = DEFAULT_SWEEP_OUTPUT_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="interpretation pack artifact root。"),
    ] = DEFAULT_INTERPRETATION_PACK_DIR,
) -> None:
    """生成 TRADING-117 top candidate interpretation pack。"""
    result = run_interpretation_pack(
        sweep_id=sweep_id,
        top_n=top_n,
        sweep_output_dir=sweep_output_dir,
        output_dir=output_dir,
    )
    manifest = result["manifest"]
    typer.echo(f"pack_id={result['pack_id']}")
    typer.echo(f"pack_dir={result['pack_dir']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(f"candidate_count={manifest['candidate_count']}")
    typer.echo(f"incomplete_weight_path_count={manifest['incomplete_weight_path_count']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_candidate_app.command("interpretation-report")
def dynamic_v3_candidate_interpretation_report_command(
    candidate_id: Annotated[str, typer.Option("--candidate-id", help="candidate id。")],
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest interpretation pack。"),
    ] = True,
    pack_id: Annotated[
        str | None, typer.Option("--pack-id", help="interpretation pack id。")
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="interpretation pack artifact root。"),
    ] = DEFAULT_INTERPRETATION_PACK_DIR,
) -> None:
    """展示 TRADING-117 candidate interpretation report 路径。"""
    payload = interpretation_report_payload(
        candidate_id=candidate_id,
        pack_id=pack_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"pack_id={payload['pack_id']}")
    typer.echo(f"candidate_id={payload['candidate_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"report_path={payload['report_path']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_rescue_app.command("validate-interpretation-pack")
def dynamic_v3_validate_interpretation_pack_command(
    pack_id: Annotated[str, typer.Option("--pack-id", help="interpretation pack id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="interpretation pack artifact root。"),
    ] = DEFAULT_INTERPRETATION_PACK_DIR,
) -> None:
    """校验 TRADING-117 interpretation pack。"""
    payload = validate_interpretation_pack_artifact(pack_id=pack_id, output_dir=output_dir)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_candidate_generated=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_observe_pool_app.command("build")
def dynamic_v3_observe_pool_build_command(
    sweep_id: Annotated[str, typer.Option("--sweep-id", help="source sweep id。")],
    top_n: Annotated[int, typer.Option("--top-n", help="top candidate count。")] = 20,
    sweep_output_dir: Annotated[
        Path,
        typer.Option("--sweep-output-dir", help="sweep artifact root。"),
    ] = DEFAULT_SWEEP_OUTPUT_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="observe pool artifact root。"),
    ] = DEFAULT_OBSERVE_POOL_DIR,
) -> None:
    """生成 TRADING-118 observe-only candidate pool。"""
    result = build_observe_pool(
        sweep_id=sweep_id,
        top_n=top_n,
        sweep_output_dir=sweep_output_dir,
        output_dir=output_dir,
    )
    manifest = result["manifest"]
    typer.echo(f"pool_id={result['pool_id']}")
    typer.echo(f"pool_dir={result['pool_dir']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(f"observe_candidate_count={manifest['observe_candidate_count']}")
    typer.echo(f"manual_review_required_count={manifest['manual_review_required_count']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_observe_pool_app.command("report")
def dynamic_v3_observe_pool_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest observe pool pointer。"),
    ] = False,
    pool_id: Annotated[str | None, typer.Option("--pool-id", help="pool id。")] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="observe pool artifact root。"),
    ] = DEFAULT_OBSERVE_POOL_DIR,
) -> None:
    """展示 TRADING-118 observe pool 摘要。"""
    payload = observe_pool_report_payload(pool_id=pool_id, latest=latest, output_dir=output_dir)
    typer.echo(f"pool_id={payload['pool_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"observe_candidate_count={payload['observe_candidate_count']}")
    typer.echo(f"report_path={payload['observe_pool_report_path']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_rescue_app.command("validate-observe-pool")
def dynamic_v3_validate_observe_pool_command(
    pool_id: Annotated[str, typer.Option("--pool-id", help="pool id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="observe pool artifact root。"),
    ] = DEFAULT_OBSERVE_POOL_DIR,
) -> None:
    """校验 TRADING-118 observe pool artifact。"""
    payload = validate_observe_pool_artifact(pool_id=pool_id, output_dir=output_dir)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_candidate_generated=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_overnight_readiness_app.command("run")
def dynamic_v3_overnight_readiness_run_command(
    source_sweep_id: Annotated[
        str,
        typer.Option("--source-sweep-id", help="source medium_real sweep id。"),
    ],
    sweep_output_dir: Annotated[
        Path,
        typer.Option("--sweep-output-dir", help="sweep artifact root。"),
    ] = DEFAULT_SWEEP_OUTPUT_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="overnight readiness artifact root。"),
    ] = DEFAULT_OVERNIGHT_READINESS_DIR,
) -> None:
    """生成 TRADING-119 overnight_real readiness check。"""
    result = run_overnight_readiness(
        source_sweep_id=source_sweep_id,
        sweep_output_dir=sweep_output_dir,
        output_dir=output_dir,
    )
    manifest = result["manifest"]
    typer.echo(f"readiness_id={result['readiness_id']}")
    typer.echo(f"readiness_dir={result['readiness_dir']}")
    typer.echo(f"overnight_readiness={manifest['overnight_readiness']}")
    typer.echo(f"projected_overnight_runtime_hours={manifest['projected_overnight_runtime_hours']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_overnight_readiness_app.command("report")
def dynamic_v3_overnight_readiness_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest overnight readiness pointer。"),
    ] = False,
    readiness_id: Annotated[
        str | None,
        typer.Option("--readiness-id", help="readiness id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="overnight readiness artifact root。"),
    ] = DEFAULT_OVERNIGHT_READINESS_DIR,
) -> None:
    """展示 TRADING-119 overnight readiness 摘要。"""
    payload = overnight_readiness_report_payload(
        readiness_id=readiness_id,
        latest=latest,
        output_dir=output_dir,
    )
    blockers = ",".join(str(item) for item in payload.get("blocking_reasons") or [])
    typer.echo(f"readiness_id={payload['readiness_id']}")
    typer.echo(f"overnight_readiness={payload['overnight_readiness']}")
    typer.echo(f"blocking_reasons={blockers}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_rescue_app.command("validate-overnight-readiness")
def dynamic_v3_validate_overnight_readiness_command(
    readiness_id: Annotated[str, typer.Option("--readiness-id", help="readiness id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="overnight readiness artifact root。"),
    ] = DEFAULT_OVERNIGHT_READINESS_DIR,
) -> None:
    """校验 TRADING-119 overnight readiness artifact。"""
    payload = validate_overnight_readiness_artifact(
        readiness_id=readiness_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_candidate_generated=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_research_decision_app.command("run")
def dynamic_v3_research_decision_run_command(
    sweep_id: Annotated[str, typer.Option("--sweep-id", help="medium_real sweep id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="research decision artifact root。"),
    ] = DEFAULT_RESEARCH_DECISION_DIR,
) -> None:
    """生成 TRADING-120 research decision pack。"""
    result = run_research_decision(sweep_id=sweep_id, output_dir=output_dir)
    manifest = result["manifest"]
    recommendation = result["recommendation"]
    typer.echo(f"decision_id={result['decision_id']}")
    typer.echo(f"decision_dir={result['decision_dir']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(f"recommendation={recommendation['recommendation']}")
    typer.echo(f"priority={recommendation['priority']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_research_decision_app.command("report")
def dynamic_v3_research_decision_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest research decision pointer。"),
    ] = False,
    decision_id: Annotated[
        str | None,
        typer.Option("--decision-id", help="decision id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="research decision artifact root。"),
    ] = DEFAULT_RESEARCH_DECISION_DIR,
) -> None:
    """展示 TRADING-120 research decision 摘要。"""
    payload = research_decision_report_payload(
        decision_id=decision_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"decision_id={payload['decision_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"recommendation={payload['recommendation']}")
    typer.echo(f"report_path={payload['research_decision_report_path']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_rescue_app.command("validate-research-decision")
def dynamic_v3_validate_research_decision_command(
    decision_id: Annotated[str, typer.Option("--decision-id", help="decision id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="research decision artifact root。"),
    ] = DEFAULT_RESEARCH_DECISION_DIR,
) -> None:
    """校验 TRADING-120 research decision artifact。"""
    payload = validate_research_decision_artifact(decision_id=decision_id, output_dir=output_dir)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_candidate_generated=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_evidence_diagnosis_app.command("run")
def dynamic_v3_evidence_diagnosis_run_command(
    sweep_id: Annotated[str, typer.Option("--sweep-id", help="source sweep id。")],
    summary_id: Annotated[
        str | None,
        typer.Option("--summary-id", help="optional source evidence summary id。"),
    ] = None,
    sweep_output_dir: Annotated[
        Path,
        typer.Option("--sweep-output-dir", help="sweep artifact root。"),
    ] = DEFAULT_SWEEP_OUTPUT_DIR,
    evidence_summary_dir: Annotated[
        Path,
        typer.Option("--evidence-summary-dir", help="evidence summary artifact root。"),
    ] = DEFAULT_EVIDENCE_SUMMARY_DIR,
    regime_coverage_dir: Annotated[
        Path,
        typer.Option("--regime-coverage-dir", help="regime coverage artifact root。"),
    ] = DEFAULT_REGIME_COVERAGE_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="evidence diagnosis artifact root。"),
    ] = DEFAULT_EVIDENCE_DIAGNOSIS_DIR,
) -> None:
    """生成 TRADING-121 evidence blocking diagnosis。"""
    result = run_evidence_diagnosis(
        sweep_id=sweep_id,
        summary_id=summary_id,
        sweep_output_dir=sweep_output_dir,
        evidence_summary_dir=evidence_summary_dir,
        regime_coverage_dir=regime_coverage_dir,
        output_dir=output_dir,
    )
    manifest = result["manifest"]
    typer.echo(f"diagnosis_id={result['diagnosis_id']}")
    typer.echo(f"diagnosis_dir={result['diagnosis_dir']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(f"candidate_count={manifest['candidate_count']}")
    typer.echo(f"usable_candidates={manifest['usable_candidates']}")
    typer.echo(f"hard_blocked_candidates={manifest['hard_blocked_candidates']}")
    typer.echo(f"soft_blocked_candidates={manifest['soft_blocked_candidates']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_evidence_diagnosis_app.command("report")
def dynamic_v3_evidence_diagnosis_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest evidence diagnosis pointer。"),
    ] = False,
    diagnosis_id: Annotated[
        str | None,
        typer.Option("--diagnosis-id", help="diagnosis id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="evidence diagnosis artifact root。"),
    ] = DEFAULT_EVIDENCE_DIAGNOSIS_DIR,
) -> None:
    """展示 TRADING-121 evidence diagnosis 摘要。"""
    payload = evidence_diagnosis_report_payload(
        diagnosis_id=diagnosis_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"diagnosis_id={payload['diagnosis_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"candidate_count={payload['candidate_count']}")
    typer.echo(f"usable_candidates={payload['usable_candidates']}")
    typer.echo(f"report_path={payload['evidence_diagnosis_report_path']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_rescue_app.command("validate-evidence-diagnosis")
def dynamic_v3_validate_evidence_diagnosis_command(
    diagnosis_id: Annotated[str, typer.Option("--diagnosis-id", help="diagnosis id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="evidence diagnosis artifact root。"),
    ] = DEFAULT_EVIDENCE_DIAGNOSIS_DIR,
) -> None:
    """校验 TRADING-121 evidence diagnosis artifact。"""
    payload = validate_evidence_diagnosis_artifact(
        diagnosis_id=diagnosis_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_candidate_generated=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_gate_impact_app.command("run")
def dynamic_v3_gate_impact_run_command(
    diagnosis_id: Annotated[str, typer.Option("--diagnosis-id", help="diagnosis id。")],
    diagnosis_dir: Annotated[
        Path,
        typer.Option("--diagnosis-dir", help="evidence diagnosis artifact root。"),
    ] = DEFAULT_EVIDENCE_DIAGNOSIS_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="gate impact artifact root。"),
    ] = DEFAULT_GATE_IMPACT_DIR,
) -> None:
    """生成 TRADING-122 gate impact simulation。"""
    result = run_gate_impact(
        diagnosis_id=diagnosis_id,
        diagnosis_dir=diagnosis_dir,
        output_dir=output_dir,
    )
    manifest = result["manifest"]
    typer.echo(f"impact_id={result['impact_id']}")
    typer.echo(f"impact_dir={result['impact_dir']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(f"baseline_observe_candidates={manifest['baseline_observe_candidates']}")
    typer.echo(f"best_scenario={manifest['best_scenario']}")
    typer.echo(f"best_observe_candidates={manifest['best_observe_candidates']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_gate_impact_app.command("report")
def dynamic_v3_gate_impact_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest gate impact pointer。"),
    ] = False,
    impact_id: Annotated[str | None, typer.Option("--impact-id", help="impact id。")] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="gate impact artifact root。"),
    ] = DEFAULT_GATE_IMPACT_DIR,
) -> None:
    """展示 TRADING-122 gate impact 摘要。"""
    payload = gate_impact_report_payload(impact_id=impact_id, latest=latest, output_dir=output_dir)
    typer.echo(f"impact_id={payload['impact_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"best_scenario={payload['best_scenario']}")
    typer.echo(f"best_observe_candidates={payload['best_observe_candidates']}")
    typer.echo(f"report_path={payload['gate_impact_report_path']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_rescue_app.command("validate-gate-impact")
def dynamic_v3_validate_gate_impact_command(
    impact_id: Annotated[str, typer.Option("--impact-id", help="impact id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="gate impact artifact root。"),
    ] = DEFAULT_GATE_IMPACT_DIR,
) -> None:
    """校验 TRADING-122 gate impact artifact。"""
    payload = validate_gate_impact_artifact(impact_id=impact_id, output_dir=output_dir)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_candidate_generated=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_gate_policy_app.command("validate")
def dynamic_v3_gate_policy_validate_command(
    policy_path: Annotated[
        Path,
        typer.Option("--policy", "--policy-path", help="evidence gate policy YAML。"),
    ] = DEFAULT_EVIDENCE_GATE_POLICY_CONFIG_PATH,
) -> None:
    """校验 TRADING-123 evidence gate policy。"""
    payload = validate_evidence_gate_policy(policy_path=policy_path)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo(f"policy_version={payload['policy_version']}")
    typer.echo("production_candidate_generated=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_gate_policy_app.command("report")
def dynamic_v3_gate_policy_report_command(
    policy_path: Annotated[
        Path,
        typer.Option("--policy", "--policy-path", help="evidence gate policy YAML。"),
    ] = DEFAULT_EVIDENCE_GATE_POLICY_CONFIG_PATH,
) -> None:
    """展示 TRADING-123 evidence gate policy 摘要。"""
    payload = evidence_gate_policy_report_payload(policy_path=policy_path)
    validation = payload["validation"]
    typer.echo(f"status={payload['status']}")
    typer.echo(f"policy_path={payload['policy_path']}")
    typer.echo(f"policy_version={validation['policy_version']}")
    typer.echo(f"manual_review_allowed={','.join(validation['manual_review_allowed'])}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_gate_policy_app.command("apply")
def dynamic_v3_gate_policy_apply_command(
    sweep_id: Annotated[str, typer.Option("--sweep-id", help="source sweep id。")],
    policy_path: Annotated[
        Path,
        typer.Option("--policy", "--policy-path", help="evidence gate policy YAML。"),
    ] = DEFAULT_EVIDENCE_GATE_POLICY_CONFIG_PATH,
    sweep_output_dir: Annotated[
        Path,
        typer.Option("--sweep-output-dir", help="sweep artifact root。"),
    ] = DEFAULT_SWEEP_OUTPUT_DIR,
    evidence_summary_dir: Annotated[
        Path,
        typer.Option("--evidence-summary-dir", help="evidence summary artifact root。"),
    ] = DEFAULT_EVIDENCE_SUMMARY_DIR,
    regime_coverage_dir: Annotated[
        Path,
        typer.Option("--regime-coverage-dir", help="regime coverage artifact root。"),
    ] = DEFAULT_REGIME_COVERAGE_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="gate policy artifact root。"),
    ] = DEFAULT_GATE_POLICY_DIR,
) -> None:
    """应用 TRADING-123 evidence gate policy，写 calibrated candidate status。"""
    result = apply_evidence_gate_policy(
        sweep_id=sweep_id,
        policy_path=policy_path,
        sweep_output_dir=sweep_output_dir,
        evidence_summary_dir=evidence_summary_dir,
        regime_coverage_dir=regime_coverage_dir,
        output_dir=output_dir,
    )
    manifest = result["manifest"]
    typer.echo(f"policy_run_id={result['policy_run_id']}")
    typer.echo(f"policy_dir={result['policy_dir']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(f"observe_only_candidates={manifest['observe_only_candidates']}")
    typer.echo(f"manual_review_required_candidates={manifest['manual_review_required_candidates']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_candidate_recovery_app.command("run")
def dynamic_v3_candidate_recovery_run_command(
    sweep_id: Annotated[str, typer.Option("--sweep-id", help="source sweep id。")],
    policy_run_id: Annotated[str, typer.Option("--policy-run-id", help="policy run id。")],
    sweep_output_dir: Annotated[
        Path,
        typer.Option("--sweep-output-dir", help="sweep artifact root。"),
    ] = DEFAULT_SWEEP_OUTPUT_DIR,
    gate_policy_dir: Annotated[
        Path,
        typer.Option("--gate-policy-dir", help="gate policy artifact root。"),
    ] = DEFAULT_GATE_POLICY_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="candidate recovery artifact root。"),
    ] = DEFAULT_CANDIDATE_RECOVERY_DIR,
) -> None:
    """生成 TRADING-124 recovered observe-only candidates。"""
    result = run_candidate_recovery(
        sweep_id=sweep_id,
        policy_run_id=policy_run_id,
        sweep_output_dir=sweep_output_dir,
        gate_policy_dir=gate_policy_dir,
        output_dir=output_dir,
    )
    manifest = result["manifest"]
    typer.echo(f"recovery_id={result['recovery_id']}")
    typer.echo(f"recovery_dir={result['recovery_dir']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(f"recovered_candidate_count={manifest['recovered_candidate_count']}")
    typer.echo(f"observe_only_candidate_count={manifest['observe_only_candidate_count']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_candidate_recovery_app.command("report")
def dynamic_v3_candidate_recovery_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest candidate recovery pointer。"),
    ] = False,
    recovery_id: Annotated[
        str | None,
        typer.Option("--recovery-id", help="recovery id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="candidate recovery artifact root。"),
    ] = DEFAULT_CANDIDATE_RECOVERY_DIR,
) -> None:
    """展示 TRADING-124 candidate recovery 摘要。"""
    payload = candidate_recovery_report_payload(
        recovery_id=recovery_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"recovery_id={payload['recovery_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"recovered_candidate_count={payload['recovered_candidate_count']}")
    typer.echo(f"report_path={payload['recovery_report_path']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_rescue_app.command("validate-candidate-recovery")
def dynamic_v3_validate_candidate_recovery_command(
    recovery_id: Annotated[str, typer.Option("--recovery-id", help="recovery id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="candidate recovery artifact root。"),
    ] = DEFAULT_CANDIDATE_RECOVERY_DIR,
) -> None:
    """校验 TRADING-124 candidate recovery artifact。"""
    payload = validate_candidate_recovery_artifact(recovery_id=recovery_id, output_dir=output_dir)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_candidate_generated=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_observe_pool_app.command("rebuild")
def dynamic_v3_observe_pool_rebuild_command(
    recovery_id: Annotated[str, typer.Option("--recovery-id", help="candidate recovery id。")],
    recovery_dir: Annotated[
        Path,
        typer.Option("--recovery-dir", help="candidate recovery artifact root。"),
    ] = DEFAULT_CANDIDATE_RECOVERY_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="observe pool artifact root。"),
    ] = DEFAULT_OBSERVE_POOL_DIR,
) -> None:
    """基于 TRADING-124 recovery artifact 重建 observe-only pool。"""
    result = rebuild_observe_pool_from_recovery(
        recovery_id=recovery_id,
        recovery_dir=recovery_dir,
        output_dir=output_dir,
    )
    manifest = result["manifest"]
    typer.echo(f"pool_id={result['pool_id']}")
    typer.echo(f"pool_dir={result['pool_dir']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(f"observe_candidate_count={manifest['observe_candidate_count']}")
    typer.echo(f"manual_review_required_count={manifest['manual_review_required_count']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_research_decision_app.command("update")
def dynamic_v3_research_decision_update_command(
    sweep_id: Annotated[str, typer.Option("--sweep-id", help="source sweep id。")],
    diagnosis_id: Annotated[str, typer.Option("--diagnosis-id", help="diagnosis id。")],
    impact_id: Annotated[str, typer.Option("--impact-id", help="gate impact id。")],
    recovery_id: Annotated[str, typer.Option("--recovery-id", help="candidate recovery id。")],
    diagnosis_dir: Annotated[
        Path,
        typer.Option("--diagnosis-dir", help="evidence diagnosis artifact root。"),
    ] = DEFAULT_EVIDENCE_DIAGNOSIS_DIR,
    gate_impact_dir: Annotated[
        Path,
        typer.Option("--gate-impact-dir", help="gate impact artifact root。"),
    ] = DEFAULT_GATE_IMPACT_DIR,
    recovery_dir: Annotated[
        Path,
        typer.Option("--recovery-dir", help="candidate recovery artifact root。"),
    ] = DEFAULT_CANDIDATE_RECOVERY_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="research decision update artifact root。"),
    ] = DEFAULT_RESEARCH_DECISION_UPDATE_DIR,
) -> None:
    """生成 TRADING-125 research decision update。"""
    result = update_research_decision(
        sweep_id=sweep_id,
        diagnosis_id=diagnosis_id,
        impact_id=impact_id,
        recovery_id=recovery_id,
        diagnosis_dir=diagnosis_dir,
        gate_impact_dir=gate_impact_dir,
        recovery_dir=recovery_dir,
        output_dir=output_dir,
    )
    manifest = result["manifest"]
    go_no_go = result["go_no_go_matrix"]
    typer.echo(f"decision_update_id={result['decision_update_id']}")
    typer.echo(f"decision_update_dir={result['decision_update_dir']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(f"go_no_go={go_no_go['go_no_go']}")
    typer.echo(f"recommended_action={go_no_go['recommended_action']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_research_decision_app.command("update-report")
def dynamic_v3_research_decision_update_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest decision update pointer。"),
    ] = False,
    decision_update_id: Annotated[
        str | None,
        typer.Option("--decision-update-id", help="decision update id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="research decision update artifact root。"),
    ] = DEFAULT_RESEARCH_DECISION_UPDATE_DIR,
) -> None:
    """展示 TRADING-125 research decision update 摘要。"""
    payload = research_decision_update_report_payload(
        decision_update_id=decision_update_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"decision_update_id={payload['decision_update_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"go_no_go={payload['go_no_go']}")
    typer.echo(f"recommended_action={payload['recommended_action']}")
    typer.echo(f"report_path={payload['research_decision_update_report_path']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_rescue_app.command("validate-research-decision-update")
def dynamic_v3_validate_research_decision_update_command(
    decision_update_id: Annotated[
        str,
        typer.Option("--decision-update-id", help="decision update id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="research decision update artifact root。"),
    ] = DEFAULT_RESEARCH_DECISION_UPDATE_DIR,
) -> None:
    """校验 TRADING-125 research decision update artifact。"""
    payload = validate_research_decision_update_artifact(
        decision_update_id=decision_update_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_candidate_generated=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_shortlist_app.command("build")
def dynamic_v3_shortlist_build_command(
    observe_pool_id: Annotated[str, typer.Option("--observe-pool-id", help="observe pool id。")],
    target_size: Annotated[int, typer.Option("--target-size", help="target shortlist size。")] = 10,
    max_size: Annotated[int, typer.Option("--max-size", help="max shortlist size。")] = 20,
    min_size: Annotated[int, typer.Option("--min-size", help="min shortlist size。")] = 5,
    observe_pool_dir: Annotated[
        Path,
        typer.Option("--observe-pool-dir", help="observe pool artifact root。"),
    ] = DEFAULT_OBSERVE_POOL_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="shortlist artifact root。"),
    ] = DEFAULT_SHORTLIST_DIR,
) -> None:
    """生成 TRADING-126 shadow shortlist。"""
    result = build_shadow_shortlist(
        observe_pool_id=observe_pool_id,
        target_size=target_size,
        max_size=max_size,
        min_size=min_size,
        observe_pool_dir=observe_pool_dir,
        output_dir=output_dir,
    )
    manifest = result["manifest"]
    typer.echo(f"shortlist_id={result['shortlist_id']}")
    typer.echo(f"shortlist_dir={result['shortlist_dir']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(f"observe_pool_candidate_count={manifest['observe_pool_candidate_count']}")
    typer.echo(f"shortlist_count={manifest['shortlist_count']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_shortlist_app.command("report")
def dynamic_v3_shortlist_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest shortlist pointer。"),
    ] = False,
    shortlist_id: Annotated[
        str | None,
        typer.Option("--shortlist-id", help="shortlist id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="shortlist artifact root。"),
    ] = DEFAULT_SHORTLIST_DIR,
) -> None:
    """展示 TRADING-126 shortlist 摘要。"""
    payload = shortlist_report_payload(
        shortlist_id=shortlist_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"shortlist_id={payload['shortlist_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"shortlist_count={payload['shortlist_count']}")
    typer.echo(f"report_path={payload['shortlist_report_path']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_rescue_app.command("validate-shortlist")
def dynamic_v3_validate_shortlist_command(
    shortlist_id: Annotated[str, typer.Option("--shortlist-id", help="shortlist id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="shortlist artifact root。"),
    ] = DEFAULT_SHORTLIST_DIR,
) -> None:
    """校验 TRADING-126 shortlist artifact。"""
    payload = validate_shortlist_artifact(shortlist_id=shortlist_id, output_dir=output_dir)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_candidate_generated=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_candidate_cluster_app.command("run")
def dynamic_v3_candidate_cluster_run_command(
    shortlist_id: Annotated[str, typer.Option("--shortlist-id", help="shortlist id。")],
    shortlist_dir: Annotated[
        Path,
        typer.Option("--shortlist-dir", help="shortlist artifact root。"),
    ] = DEFAULT_SHORTLIST_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="candidate cluster artifact root。"),
    ] = DEFAULT_CANDIDATE_CLUSTER_DIR,
) -> None:
    """生成 TRADING-127 candidate cluster。"""
    result = run_candidate_clustering(
        shortlist_id=shortlist_id,
        shortlist_dir=shortlist_dir,
        output_dir=output_dir,
    )
    manifest = result["manifest"]
    typer.echo(f"cluster_id={result['cluster_id']}")
    typer.echo(f"cluster_dir={result['cluster_dir']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(f"cluster_count={manifest['cluster_count']}")
    typer.echo(f"representative_count={manifest['representative_count']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_candidate_cluster_app.command("report")
def dynamic_v3_candidate_cluster_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest candidate cluster pointer。"),
    ] = False,
    cluster_id: Annotated[str | None, typer.Option("--cluster-id", help="cluster id。")] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="candidate cluster artifact root。"),
    ] = DEFAULT_CANDIDATE_CLUSTER_DIR,
) -> None:
    """展示 TRADING-127 candidate cluster 摘要。"""
    payload = candidate_cluster_report_payload(
        cluster_id=cluster_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"cluster_id={payload['cluster_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"cluster_count={payload['cluster_count']}")
    typer.echo(f"report_path={payload['candidate_cluster_report_path']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_rescue_app.command("validate-candidate-cluster")
def dynamic_v3_validate_candidate_cluster_command(
    cluster_id: Annotated[str, typer.Option("--cluster-id", help="cluster id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="candidate cluster artifact root。"),
    ] = DEFAULT_CANDIDATE_CLUSTER_DIR,
) -> None:
    """校验 TRADING-127 candidate cluster artifact。"""
    payload = validate_candidate_cluster_artifact(cluster_id=cluster_id, output_dir=output_dir)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_candidate_generated=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_shadow_shortlist_app.command("build")
def dynamic_v3_shadow_shortlist_build_command(
    shortlist_id: Annotated[str, typer.Option("--shortlist-id", help="shortlist id。")],
    cluster_id: Annotated[str, typer.Option("--cluster-id", help="cluster id。")],
    shortlist_dir: Annotated[
        Path,
        typer.Option("--shortlist-dir", help="shortlist artifact root。"),
    ] = DEFAULT_SHORTLIST_DIR,
    cluster_dir: Annotated[
        Path,
        typer.Option("--cluster-dir", help="candidate cluster artifact root。"),
    ] = DEFAULT_CANDIDATE_CLUSTER_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="shadow shortlist artifact root。"),
    ] = DEFAULT_SHADOW_SHORTLIST_DIR,
) -> None:
    """生成 TRADING-128 shadow shortlist monitoring pack。"""
    result = build_shadow_shortlist_monitoring_pack(
        shortlist_id=shortlist_id,
        cluster_id=cluster_id,
        shortlist_dir=shortlist_dir,
        cluster_dir=cluster_dir,
        output_dir=output_dir,
    )
    manifest = result["manifest"]
    typer.echo(f"shadow_shortlist_id={result['shadow_shortlist_id']}")
    typer.echo(f"shadow_shortlist_dir={result['shadow_shortlist_dir']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(f"shadow_candidate_count={manifest['shadow_candidate_count']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_shadow_shortlist_app.command("report")
def dynamic_v3_shadow_shortlist_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest shadow shortlist pointer。"),
    ] = False,
    shadow_shortlist_id: Annotated[
        str | None,
        typer.Option("--shadow-shortlist-id", help="shadow shortlist id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="shadow shortlist artifact root。"),
    ] = DEFAULT_SHADOW_SHORTLIST_DIR,
) -> None:
    """展示 TRADING-128 shadow shortlist 摘要。"""
    payload = shadow_shortlist_report_payload(
        shadow_shortlist_id=shadow_shortlist_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"shadow_shortlist_id={payload['shadow_shortlist_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"shadow_candidate_count={payload['shadow_candidate_count']}")
    typer.echo(f"report_path={payload['shadow_shortlist_report_path']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_rescue_app.command("validate-shadow-shortlist")
def dynamic_v3_validate_shadow_shortlist_command(
    shadow_shortlist_id: Annotated[
        str,
        typer.Option("--shadow-shortlist-id", help="shadow shortlist id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="shadow shortlist artifact root。"),
    ] = DEFAULT_SHADOW_SHORTLIST_DIR,
) -> None:
    """校验 TRADING-128 shadow shortlist artifact。"""
    payload = validate_shadow_shortlist_artifact(
        shadow_shortlist_id=shadow_shortlist_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_candidate_generated=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_shadow_monitor_run_app.command("activate")
def dynamic_v3_shadow_monitor_activate_command(
    shadow_shortlist_id: Annotated[
        str,
        typer.Option("--shadow-shortlist-id", help="shadow shortlist id。"),
    ],
    shadow_shortlist_dir: Annotated[
        Path,
        typer.Option("--shadow-shortlist-dir", help="shadow shortlist artifact root。"),
    ] = DEFAULT_SHADOW_SHORTLIST_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="shadow monitor run artifact root。"),
    ] = DEFAULT_SHADOW_MONITOR_RUN_DIR,
) -> None:
    """激活 TRADING-131 shadow shortlist monitoring。"""
    result = activate_shadow_monitoring(
        shadow_shortlist_id=shadow_shortlist_id,
        shadow_shortlist_dir=shadow_shortlist_dir,
        output_dir=output_dir,
    )
    manifest = result["manifest"]
    typer.echo(f"activation_id={result['activation_id']}")
    typer.echo(f"activation_dir={result['activation_dir']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(f"monitoring_status={manifest['monitoring_status']}")
    typer.echo(f"candidate_count={manifest['candidate_count']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_shadow_monitor_run_app.command("run")
def dynamic_v3_shadow_monitor_run_from_shortlist_command(
    shadow_shortlist_id: Annotated[
        str,
        typer.Option("--shadow-shortlist-id", help="shadow shortlist id。"),
    ],
    as_of: Annotated[str, typer.Option("--as-of", help="monitor as-of date。")],
    shadow_shortlist_dir: Annotated[
        Path,
        typer.Option("--shadow-shortlist-dir", help="shadow shortlist artifact root。"),
    ] = DEFAULT_SHADOW_SHORTLIST_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="shadow monitor run artifact root。"),
    ] = DEFAULT_SHADOW_MONITOR_RUN_DIR,
) -> None:
    """生成 TRADING-131 shadow monitor daily / weekly artifact。"""
    result = run_shadow_shortlist_monitor(
        shadow_shortlist_id=shadow_shortlist_id,
        as_of=_parse_date(as_of),
        shadow_shortlist_dir=shadow_shortlist_dir,
        output_dir=output_dir,
    )
    summary = result["summary"]
    typer.echo(f"monitor_run_id={result['monitor_run_id']}")
    typer.echo(f"monitor_run_dir={result['monitor_run_dir']}")
    typer.echo(f"active_count={summary['active_count']}")
    typer.echo(f"summary_recommendation={summary['summary_recommendation']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_shadow_monitor_run_app.command("report")
def dynamic_v3_shadow_monitor_run_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest shadow monitor run。"),
    ] = False,
    monitor_run_id: Annotated[
        str | None,
        typer.Option("--monitor-run-id", help="monitor run id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="shadow monitor run artifact root。"),
    ] = DEFAULT_SHADOW_MONITOR_RUN_DIR,
) -> None:
    """展示 TRADING-131 shadow monitor run 摘要。"""
    payload = shadow_monitor_run_report_payload(
        monitor_run_id=monitor_run_id,
        latest=latest,
        output_dir=output_dir,
    )
    summary = _mapping_obj(payload.get("shadow_monitor_summary"))
    typer.echo(f"monitor_run_id={payload['monitor_run_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"active_count={summary.get('active_count')}")
    typer.echo(f"summary_recommendation={summary.get('summary_recommendation')}")
    typer.echo(f"report_path={payload['shadow_monitor_report_path']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_rescue_app.command("validate-shadow-monitor-run")
def dynamic_v3_validate_shadow_monitor_run_command(
    monitor_run_id: Annotated[
        str,
        typer.Option("--monitor-run-id", help="monitor run id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="shadow monitor run artifact root。"),
    ] = DEFAULT_SHADOW_MONITOR_RUN_DIR,
) -> None:
    """校验 TRADING-131 shadow monitor run artifact。"""
    payload = validate_shadow_monitor_run_artifact(
        monitor_run_id=monitor_run_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_portfolio_snapshot_app.command("validate")
def dynamic_v3_portfolio_snapshot_validate_command(
    snapshot: Annotated[Path, typer.Option("--snapshot", help="portfolio snapshot YAML。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="portfolio snapshot artifact root。"),
    ] = DEFAULT_PORTFOLIO_SNAPSHOT_DIR,
) -> None:
    """校验 TRADING-132 manual portfolio snapshot。"""
    payload = validate_portfolio_snapshot_file(snapshot_path=snapshot, output_dir=output_dir)
    typer.echo(f"snapshot_id={payload['snapshot_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo(f"snapshot_dir={payload['snapshot_dir']}")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_portfolio_snapshot_app.command("report")
def dynamic_v3_portfolio_snapshot_report_command(
    snapshot: Annotated[
        Path | None,
        typer.Option("--snapshot", help="portfolio snapshot YAML。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest portfolio snapshot artifact。"),
    ] = False,
    snapshot_id: Annotated[
        str | None,
        typer.Option("--snapshot-id", help="snapshot artifact id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="portfolio snapshot artifact root。"),
    ] = DEFAULT_PORTFOLIO_SNAPSHOT_DIR,
) -> None:
    """生成或展示 TRADING-132 portfolio snapshot report。"""
    payload = portfolio_snapshot_report_payload(
        snapshot_path=snapshot,
        snapshot_id=snapshot_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"snapshot_id={payload['snapshot_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"manual_review_required={payload['manual_review_required']}")
    typer.echo(f"report_path={payload['snapshot_validation_report_path']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_portfolio_snapshot_app.command("normalize")
def dynamic_v3_portfolio_snapshot_normalize_command(
    snapshot: Annotated[Path, typer.Option("--snapshot", help="portfolio snapshot YAML。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="portfolio snapshot artifact root。"),
    ] = DEFAULT_PORTFOLIO_SNAPSHOT_DIR,
) -> None:
    """输出 TRADING-132 normalized portfolio snapshot artifact。"""
    result = write_portfolio_snapshot_artifact(snapshot_path=snapshot, output_dir=output_dir)
    manifest = result["manifest"]
    typer.echo(f"snapshot_id={result['snapshot_id']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(f"normalized_positions_path={manifest['normalized_positions_path']}")
    typer.echo("broker_action_allowed=false")
    if manifest["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_position_advisory_app.command("run")
def dynamic_v3_position_advisory_run_command(
    shadow_shortlist_id: Annotated[
        str,
        typer.Option("--shadow-shortlist-id", help="shadow shortlist id。"),
    ],
    config_path: Annotated[
        Path,
        typer.Option("--config", "--config-path", help="position advisory config。"),
    ] = DEFAULT_POSITION_ADVISORY_CONFIG_PATH,
    portfolio_snapshot: Annotated[
        Path | None,
        typer.Option("--portfolio-snapshot", help="optional current portfolio snapshot YAML。"),
    ] = None,
    shadow_shortlist_dir: Annotated[
        Path,
        typer.Option("--shadow-shortlist-dir", help="shadow shortlist artifact root。"),
    ] = DEFAULT_SHADOW_SHORTLIST_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="position advisory artifact root。"),
    ] = DEFAULT_POSITION_ADVISORY_DIR,
) -> None:
    """生成 TRADING-129 position advisory。"""
    result = run_position_advisory(
        shadow_shortlist_id=shadow_shortlist_id,
        config_path=config_path,
        portfolio_snapshot_path=portfolio_snapshot,
        shadow_shortlist_dir=shadow_shortlist_dir,
        output_dir=output_dir,
    )
    manifest = result["manifest"]
    typer.echo(f"advisory_id={result['advisory_id']}")
    typer.echo(f"advisory_dir={result['advisory_dir']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(f"position_advisory_status={manifest['position_advisory_status']}")
    typer.echo(f"recommended_action={manifest['recommended_action']}")
    typer.echo("owner_approval_required=true")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_position_advisory_app.command("report")
def dynamic_v3_position_advisory_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest position advisory pointer。"),
    ] = False,
    advisory_id: Annotated[str | None, typer.Option("--advisory-id", help="advisory id。")] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="position advisory artifact root。"),
    ] = DEFAULT_POSITION_ADVISORY_DIR,
) -> None:
    """展示 TRADING-129 position advisory 摘要。"""
    payload = position_advisory_report_payload(
        advisory_id=advisory_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"advisory_id={payload['advisory_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"position_advisory_status={payload['position_advisory_status']}")
    typer.echo(f"recommended_action={payload['recommended_action']}")
    typer.echo(f"report_path={payload['position_advisory_report_path']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_rescue_app.command("validate-position-advisory")
def dynamic_v3_validate_position_advisory_command(
    advisory_id: Annotated[str, typer.Option("--advisory-id", help="advisory id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="position advisory artifact root。"),
    ] = DEFAULT_POSITION_ADVISORY_DIR,
) -> None:
    """校验 TRADING-129 position advisory artifact。"""
    payload = validate_position_advisory_artifact(advisory_id=advisory_id, output_dir=output_dir)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("owner_approval_required=true")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_position_advisory_app.command("daily-run")
def dynamic_v3_position_advisory_daily_run_command(
    shadow_monitor_run_id: Annotated[
        str,
        typer.Option("--shadow-monitor-run-id", help="shadow monitor run id。"),
    ],
    config_path: Annotated[
        Path,
        typer.Option("--config", "--config-path", help="position advisory config。"),
    ] = DEFAULT_POSITION_ADVISORY_CONFIG_PATH,
    portfolio_snapshot: Annotated[
        Path | None,
        typer.Option("--portfolio-snapshot", help="optional current portfolio snapshot YAML。"),
    ] = None,
    shadow_monitor_run_dir: Annotated[
        Path,
        typer.Option("--shadow-monitor-run-dir", help="shadow monitor run artifact root。"),
    ] = DEFAULT_SHADOW_MONITOR_RUN_DIR,
    consensus_drift_dir: Annotated[
        Path,
        typer.Option("--consensus-drift-dir", help="consensus drift artifact root。"),
    ] = DEFAULT_CONSENSUS_DRIFT_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="daily advisory artifact root。"),
    ] = DEFAULT_POSITION_ADVISORY_DAILY_DIR,
) -> None:
    """生成 TRADING-133 daily position advisory。"""
    result = run_position_advisory_daily(
        shadow_monitor_run_id=shadow_monitor_run_id,
        config_path=config_path,
        portfolio_snapshot_path=portfolio_snapshot,
        shadow_monitor_run_dir=shadow_monitor_run_dir,
        consensus_drift_dir=consensus_drift_dir,
        output_dir=output_dir,
    )
    actions = result["daily_advisory_actions"]
    typer.echo(f"daily_advisory_id={result['daily_advisory_id']}")
    typer.echo(f"daily_advisory_dir={result['daily_advisory_dir']}")
    typer.echo(f"mode={actions['mode']}")
    typer.echo(f"recommended_action={actions['recommended_action']}")
    typer.echo("owner_approval_required=true")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_position_advisory_app.command("daily-report")
def dynamic_v3_position_advisory_daily_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest daily advisory。"),
    ] = False,
    daily_advisory_id: Annotated[
        str | None,
        typer.Option("--daily-advisory-id", help="daily advisory id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="daily advisory artifact root。"),
    ] = DEFAULT_POSITION_ADVISORY_DAILY_DIR,
) -> None:
    """展示 TRADING-133 daily position advisory 摘要。"""
    payload = position_advisory_daily_report_payload(
        daily_advisory_id=daily_advisory_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"daily_advisory_id={payload['daily_advisory_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"mode={payload['mode']}")
    typer.echo(f"recommended_action={payload['recommended_action']}")
    typer.echo(f"report_path={payload['daily_position_advisory_report_path']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_rescue_app.command("validate-position-advisory-daily")
def dynamic_v3_validate_position_advisory_daily_command(
    daily_advisory_id: Annotated[
        str,
        typer.Option("--daily-advisory-id", help="daily advisory id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="daily advisory artifact root。"),
    ] = DEFAULT_POSITION_ADVISORY_DAILY_DIR,
) -> None:
    """校验 TRADING-133 daily position advisory artifact。"""
    payload = validate_position_advisory_daily_artifact(
        daily_advisory_id=daily_advisory_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("owner_approval_required=true")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_consensus_drift_app.command("run")
def dynamic_v3_consensus_drift_run_command(
    shadow_monitor_run_id: Annotated[
        str,
        typer.Option("--shadow-monitor-run-id", help="shadow monitor run id。"),
    ],
    config_path: Annotated[
        Path,
        typer.Option("--config", "--config-path", help="position advisory config。"),
    ] = DEFAULT_POSITION_ADVISORY_CONFIG_PATH,
    shadow_monitor_run_dir: Annotated[
        Path,
        typer.Option("--shadow-monitor-run-dir", help="shadow monitor run artifact root。"),
    ] = DEFAULT_SHADOW_MONITOR_RUN_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="consensus drift artifact root。"),
    ] = DEFAULT_CONSENSUS_DRIFT_DIR,
) -> None:
    """生成 TRADING-134 consensus drift artifact。"""
    result = run_consensus_drift(
        shadow_monitor_run_id=shadow_monitor_run_id,
        config_path=config_path,
        shadow_monitor_run_dir=shadow_monitor_run_dir,
        output_dir=output_dir,
    )
    summary = result["summary"]
    typer.echo(f"drift_id={result['drift_id']}")
    typer.echo(f"drift_dir={result['drift_dir']}")
    typer.echo(f"disagreement_status={summary['disagreement_status']}")
    typer.echo(f"position_advisory_implication={summary['position_advisory_implication']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_consensus_drift_app.command("report")
def dynamic_v3_consensus_drift_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest consensus drift。"),
    ] = False,
    drift_id: Annotated[str | None, typer.Option("--drift-id", help="drift id。")] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="consensus drift artifact root。"),
    ] = DEFAULT_CONSENSUS_DRIFT_DIR,
) -> None:
    """展示 TRADING-134 consensus drift 摘要。"""
    payload = consensus_drift_report_payload(
        drift_id=drift_id,
        latest=latest,
        output_dir=output_dir,
    )
    summary = _mapping_obj(payload.get("consensus_drift_summary"))
    typer.echo(f"drift_id={payload['drift_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"disagreement_status={summary.get('disagreement_status')}")
    typer.echo(f"position_advisory_implication={summary.get('position_advisory_implication')}")
    typer.echo(f"report_path={payload['consensus_drift_report_path']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_rescue_app.command("validate-consensus-drift")
def dynamic_v3_validate_consensus_drift_command(
    drift_id: Annotated[str, typer.Option("--drift-id", help="drift id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="consensus drift artifact root。"),
    ] = DEFAULT_CONSENSUS_DRIFT_DIR,
) -> None:
    """校验 TRADING-134 consensus drift artifact。"""
    payload = validate_consensus_drift_artifact(drift_id=drift_id, output_dir=output_dir)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_owner_review_app.command("create")
def dynamic_v3_owner_review_create_command(
    daily_advisory_id: Annotated[
        str,
        typer.Option("--daily-advisory-id", help="daily advisory id。"),
    ],
    daily_advisory_dir: Annotated[
        Path,
        typer.Option("--daily-advisory-dir", help="daily advisory artifact root。"),
    ] = DEFAULT_POSITION_ADVISORY_DAILY_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="owner review journal root。"),
    ] = DEFAULT_OWNER_REVIEW_JOURNAL_DIR,
) -> None:
    """创建 TRADING-135 owner review record。"""
    result = create_owner_review(
        daily_advisory_id=daily_advisory_id,
        daily_advisory_dir=daily_advisory_dir,
        output_dir=output_dir,
    )
    review = result["review"]
    typer.echo(f"review_id={result['review_id']}")
    typer.echo(f"owner_decision={review['owner_decision']}")
    typer.echo(f"journal_dir={result['journal_dir']}")
    typer.echo("broker_action_taken=false")


@dynamic_v3_owner_review_app.command("list")
def dynamic_v3_owner_review_list_command(
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="owner review journal root。"),
    ] = DEFAULT_OWNER_REVIEW_JOURNAL_DIR,
) -> None:
    """列出 TRADING-135 owner review journal 摘要。"""
    payload = owner_review_summary(output_dir=output_dir)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"review_count={payload['review_count']}")
    typer.echo(f"pending_owner_review_count={payload['pending_owner_review_count']}")
    typer.echo(f"latest_review_id={payload['latest_review_id']}")
    typer.echo("broker_action_taken=false")


@dynamic_v3_owner_review_app.command("report")
def dynamic_v3_owner_review_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest owner review。"),
    ] = False,
    review_id: Annotated[str | None, typer.Option("--review-id", help="owner review id。")] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="owner review journal root。"),
    ] = DEFAULT_OWNER_REVIEW_JOURNAL_DIR,
) -> None:
    """展示 TRADING-135 owner review report。"""
    payload = owner_review_report_payload(
        latest=latest,
        review_id=review_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"latest_review_id={payload['latest_review_id']}")
    typer.echo(f"latest_owner_decision={payload['latest_owner_decision']}")
    typer.echo(f"pending_owner_review_count={payload['pending_owner_review_count']}")
    typer.echo(f"report_path={payload['owner_review_report_path']}")
    typer.echo("broker_action_taken=false")


@dynamic_v3_owner_review_app.command("record-decision")
def dynamic_v3_owner_review_record_decision_command(
    review_id: Annotated[str, typer.Option("--review-id", help="owner review id。")],
    decision: Annotated[str, typer.Option("--decision", help="owner decision。")],
    manual_notes: Annotated[
        str,
        typer.Option("--manual-notes", help="manual owner notes。"),
    ] = "",
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="owner review journal root。"),
    ] = DEFAULT_OWNER_REVIEW_JOURNAL_DIR,
    daily_advisory_dir: Annotated[
        Path,
        typer.Option("--daily-advisory-dir", help="daily advisory artifact root。"),
    ] = DEFAULT_POSITION_ADVISORY_DAILY_DIR,
) -> None:
    """记录 TRADING-135 owner decision。"""
    result = record_owner_review_decision(
        review_id=review_id,
        decision=decision,
        manual_notes=manual_notes,
        output_dir=output_dir,
        daily_advisory_dir=daily_advisory_dir,
    )
    review = result["review"]
    typer.echo(f"review_id={result['review_id']}")
    typer.echo(f"owner_decision={review['owner_decision']}")
    typer.echo(f"broker_action_taken={review['broker_action_taken']}")


@dynamic_v3_rescue_app.command("validate-owner-review")
def dynamic_v3_validate_owner_review_command(
    review_id: Annotated[str, typer.Option("--review-id", help="owner review id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="owner review journal root。"),
    ] = DEFAULT_OWNER_REVIEW_JOURNAL_DIR,
) -> None:
    """校验 TRADING-135 owner review record。"""
    payload = validate_owner_review_artifact(review_id=review_id, output_dir=output_dir)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_taken=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_paper_portfolio_app.command("init")
def dynamic_v3_paper_portfolio_init_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", "--config-path", help="paper portfolio config。"),
    ] = DEFAULT_PAPER_PORTFOLIO_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="paper portfolio artifact root。"),
    ] = DEFAULT_PAPER_PORTFOLIO_DIR,
) -> None:
    """初始化 TRADING-136 paper portfolio state。"""
    result = init_paper_portfolio(config_path=config_path, output_dir=output_dir)
    state = result["state"]
    typer.echo(f"paper_portfolio_id={result['paper_portfolio_id']}")
    typer.echo(f"paper_portfolio_dir={result['paper_portfolio_dir']}")
    typer.echo(f"state_status={state['state_status']}")
    typer.echo(f"total_weight={state['total_weight']}")
    typer.echo("broker_action_taken=false")


@dynamic_v3_paper_portfolio_app.command("apply-review")
def dynamic_v3_paper_portfolio_apply_review_command(
    review_id: Annotated[str, typer.Option("--review-id", help="owner review id。")],
    paper_portfolio_id: Annotated[
        str | None,
        typer.Option("--paper-portfolio-id", help="optional paper portfolio id。"),
    ] = None,
    manual_deltas_json: Annotated[
        str,
        typer.Option("--manual-deltas-json", help="manual adjustment deltas JSON。"),
    ] = "",
    config_path: Annotated[
        Path,
        typer.Option("--config", "--config-path", help="paper portfolio config。"),
    ] = DEFAULT_PAPER_PORTFOLIO_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="paper portfolio artifact root。"),
    ] = DEFAULT_PAPER_PORTFOLIO_DIR,
    owner_review_dir: Annotated[
        Path,
        typer.Option("--owner-review-dir", help="owner review journal root。"),
    ] = DEFAULT_OWNER_REVIEW_JOURNAL_DIR,
    daily_advisory_dir: Annotated[
        Path,
        typer.Option("--daily-advisory-dir", help="daily advisory artifact root。"),
    ] = DEFAULT_POSITION_ADVISORY_DAILY_DIR,
) -> None:
    """把 owner review 决策应用到 paper-only portfolio ledger。"""
    manual_deltas: Mapping[str, Any] | None = None
    if manual_deltas_json:
        parsed = json.loads(manual_deltas_json)
        if not isinstance(parsed, Mapping):
            raise typer.BadParameter("--manual-deltas-json must be a JSON object")
        manual_deltas = parsed
    result = apply_owner_review_to_paper_portfolio(
        review_id=review_id,
        paper_portfolio_id=paper_portfolio_id,
        manual_deltas=manual_deltas,
        config_path=config_path,
        output_dir=output_dir,
        owner_review_dir=owner_review_dir,
        daily_advisory_dir=daily_advisory_dir,
    )
    state = result["state"]
    typer.echo(f"paper_portfolio_id={result['paper_portfolio_id']}")
    typer.echo(f"paper_action_id={result['paper_action_id']}")
    typer.echo(f"state_status={state['state_status']}")
    typer.echo(f"total_weight={state['total_weight']}")
    typer.echo("broker_action_taken=false")


@dynamic_v3_paper_portfolio_app.command("state")
def dynamic_v3_paper_portfolio_state_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest paper portfolio。"),
    ] = False,
    paper_portfolio_id: Annotated[
        str | None,
        typer.Option("--paper-portfolio-id", help="paper portfolio id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="paper portfolio artifact root。"),
    ] = DEFAULT_PAPER_PORTFOLIO_DIR,
) -> None:
    """展示 TRADING-136 paper portfolio latest state。"""
    payload = paper_portfolio_state_payload(
        paper_portfolio_id=paper_portfolio_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"paper_portfolio_id={payload['paper_portfolio_id']}")
    typer.echo(f"state_status={payload['state_status']}")
    typer.echo(f"as_of={payload['as_of']}")
    typer.echo(f"total_weight={payload['total_weight']}")
    typer.echo(f"positions={json.dumps(payload['positions'], sort_keys=True)}")
    typer.echo("broker_action_taken=false")


@dynamic_v3_paper_portfolio_app.command("report")
def dynamic_v3_paper_portfolio_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest paper portfolio。"),
    ] = False,
    paper_portfolio_id: Annotated[
        str | None,
        typer.Option("--paper-portfolio-id", help="paper portfolio id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="paper portfolio artifact root。"),
    ] = DEFAULT_PAPER_PORTFOLIO_DIR,
) -> None:
    """展示 TRADING-136 paper portfolio report 摘要。"""
    payload = paper_portfolio_report_payload(
        paper_portfolio_id=paper_portfolio_id,
        latest=latest,
        output_dir=output_dir,
    )
    state = _mapping_obj(payload.get("paper_portfolio_state"))
    typer.echo(f"paper_portfolio_id={payload['paper_portfolio_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"state_status={state.get('state_status')}")
    typer.echo(f"paper_action_count={payload['paper_action_count']}")
    typer.echo(f"report_path={payload['paper_portfolio_report_path']}")
    typer.echo("broker_action_taken=false")


@dynamic_v3_rescue_app.command("validate-paper-portfolio")
def dynamic_v3_validate_paper_portfolio_command(
    paper_portfolio_id: Annotated[
        str,
        typer.Option("--paper-portfolio-id", help="paper portfolio id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="paper portfolio artifact root。"),
    ] = DEFAULT_PAPER_PORTFOLIO_DIR,
) -> None:
    """校验 TRADING-136 paper portfolio artifact。"""
    payload = validate_paper_portfolio_artifact(
        paper_portfolio_id=paper_portfolio_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_taken=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_advisory_outcome_app.command("track")
def dynamic_v3_advisory_outcome_track_command(
    daily_advisory_id: Annotated[
        str,
        typer.Option("--daily-advisory-id", help="daily advisory id。"),
    ],
    config_path: Annotated[
        Path,
        typer.Option("--config", "--config-path", help="paper portfolio config。"),
    ] = DEFAULT_PAPER_PORTFOLIO_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="advisory outcome artifact root。"),
    ] = DEFAULT_ADVISORY_OUTCOME_DIR,
    daily_advisory_dir: Annotated[
        Path,
        typer.Option("--daily-advisory-dir", help="daily advisory artifact root。"),
    ] = DEFAULT_POSITION_ADVISORY_DAILY_DIR,
    paper_portfolio_dir: Annotated[
        Path,
        typer.Option("--paper-portfolio-dir", help="paper portfolio artifact root。"),
    ] = DEFAULT_PAPER_PORTFOLIO_DIR,
) -> None:
    """创建 TRADING-137 advisory outcome tracker。"""
    result = track_advisory_outcome(
        daily_advisory_id=daily_advisory_id,
        config_path=config_path,
        output_dir=output_dir,
        daily_advisory_dir=daily_advisory_dir,
        paper_portfolio_dir=paper_portfolio_dir,
    )
    manifest = result["manifest"]
    typer.echo(f"outcome_id={result['outcome_id']}")
    typer.echo(f"outcome_dir={result['outcome_dir']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(f"tracked_windows={','.join(str(item) for item in manifest['tracked_windows'])}")
    typer.echo("broker_action_taken=false")


@dynamic_v3_advisory_outcome_app.command("update")
def dynamic_v3_advisory_outcome_update_command(
    as_of: Annotated[str, typer.Option("--as-of", help="update as-of date。")],
    outcome_id: Annotated[
        str | None,
        typer.Option("--outcome-id", help="optional outcome id；default latest。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="advisory outcome artifact root。"),
    ] = DEFAULT_ADVISORY_OUTCOME_DIR,
    paper_portfolio_dir: Annotated[
        Path,
        typer.Option("--paper-portfolio-dir", help="paper portfolio artifact root。"),
    ] = DEFAULT_PAPER_PORTFOLIO_DIR,
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="cached price path。"),
    ] = DEFAULT_ETF_PRICE_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="cached rates path。"),
    ] = DEFAULT_RATES_CACHE_PATH,
) -> None:
    """更新 TRADING-137 advisory outcome windows。"""
    result = update_advisory_outcome(
        as_of=_parse_date(as_of),
        outcome_id=outcome_id,
        output_dir=output_dir,
        paper_portfolio_dir=paper_portfolio_dir,
        prices_path=prices_path,
        rates_path=rates_path,
    )
    manifest = result["manifest"]
    typer.echo(f"outcome_id={result['outcome_id']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(f"data_quality_status={manifest['data_quality_status']}")
    typer.echo("broker_action_taken=false")


@dynamic_v3_advisory_outcome_app.command("report")
def dynamic_v3_advisory_outcome_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest advisory outcome。"),
    ] = False,
    outcome_id: Annotated[str | None, typer.Option("--outcome-id", help="outcome id。")] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="advisory outcome artifact root。"),
    ] = DEFAULT_ADVISORY_OUTCOME_DIR,
) -> None:
    """展示 TRADING-137 advisory outcome 摘要。"""
    payload = advisory_outcome_report_payload(
        outcome_id=outcome_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"outcome_id={payload['outcome_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"data_quality_status={payload['data_quality_status']}")
    typer.echo(f"report_path={payload['advisory_outcome_report_path']}")
    typer.echo("broker_action_taken=false")


@dynamic_v3_rescue_app.command("validate-advisory-outcome")
def dynamic_v3_validate_advisory_outcome_command(
    outcome_id: Annotated[str, typer.Option("--outcome-id", help="outcome id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="advisory outcome artifact root。"),
    ] = DEFAULT_ADVISORY_OUTCOME_DIR,
) -> None:
    """校验 TRADING-137 advisory outcome artifact。"""
    payload = validate_advisory_outcome_artifact(outcome_id=outcome_id, output_dir=output_dir)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_taken=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_owner_attribution_app.command("run")
def dynamic_v3_owner_attribution_run_command(
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="owner attribution artifact root。"),
    ] = DEFAULT_OWNER_ATTRIBUTION_DIR,
    owner_review_dir: Annotated[
        Path,
        typer.Option("--owner-review-dir", help="owner review journal root。"),
    ] = DEFAULT_OWNER_REVIEW_JOURNAL_DIR,
    outcome_dir: Annotated[
        Path,
        typer.Option("--outcome-dir", help="advisory outcome artifact root。"),
    ] = DEFAULT_ADVISORY_OUTCOME_DIR,
) -> None:
    """生成 TRADING-138 owner attribution report。"""
    result = run_owner_attribution(
        output_dir=output_dir,
        owner_review_dir=owner_review_dir,
        outcome_dir=outcome_dir,
    )
    manifest = result["manifest"]
    typer.echo(f"attribution_id={result['attribution_id']}")
    typer.echo(f"attribution_dir={result['attribution_dir']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(f"total_reviews={manifest['total_reviews']}")
    typer.echo("broker_action_taken=false")


@dynamic_v3_owner_attribution_app.command("report")
def dynamic_v3_owner_attribution_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest owner attribution。"),
    ] = False,
    attribution_id: Annotated[
        str | None,
        typer.Option("--attribution-id", help="attribution id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="owner attribution artifact root。"),
    ] = DEFAULT_OWNER_ATTRIBUTION_DIR,
) -> None:
    """展示 TRADING-138 owner attribution 摘要。"""
    payload = owner_attribution_report_payload(
        attribution_id=attribution_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"attribution_id={payload['attribution_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"total_reviews={payload['total_reviews']}")
    typer.echo(f"report_path={payload['owner_attribution_report_path']}")
    typer.echo("broker_action_taken=false")


@dynamic_v3_rescue_app.command("validate-owner-attribution")
def dynamic_v3_validate_owner_attribution_command(
    attribution_id: Annotated[str, typer.Option("--attribution-id", help="attribution id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="owner attribution artifact root。"),
    ] = DEFAULT_OWNER_ATTRIBUTION_DIR,
) -> None:
    """校验 TRADING-138 owner attribution artifact。"""
    payload = validate_owner_attribution_artifact(
        attribution_id=attribution_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_taken=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_shadow_aging_app.command("run")
def dynamic_v3_shadow_aging_run_command(
    shadow_shortlist_id: Annotated[
        str,
        typer.Option("--shadow-shortlist-id", help="shadow shortlist id。"),
    ],
    config_path: Annotated[
        Path,
        typer.Option("--config", "--config-path", help="paper portfolio config。"),
    ] = DEFAULT_PAPER_PORTFOLIO_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="shadow aging artifact root。"),
    ] = DEFAULT_SHADOW_AGING_DIR,
    shadow_shortlist_dir: Annotated[
        Path,
        typer.Option("--shadow-shortlist-dir", help="shadow shortlist artifact root。"),
    ] = DEFAULT_SHADOW_SHORTLIST_DIR,
    shadow_monitor_run_dir: Annotated[
        Path,
        typer.Option("--shadow-monitor-run-dir", help="shadow monitor run artifact root。"),
    ] = DEFAULT_SHADOW_MONITOR_RUN_DIR,
    consensus_drift_dir: Annotated[
        Path,
        typer.Option("--consensus-drift-dir", help="consensus drift artifact root。"),
    ] = DEFAULT_CONSENSUS_DRIFT_DIR,
    advisory_outcome_dir: Annotated[
        Path,
        typer.Option("--advisory-outcome-dir", help="advisory outcome artifact root。"),
    ] = DEFAULT_ADVISORY_OUTCOME_DIR,
) -> None:
    """生成 TRADING-139 shadow candidate aging v2。"""
    result = run_shadow_aging(
        shadow_shortlist_id=shadow_shortlist_id,
        config_path=config_path,
        output_dir=output_dir,
        shadow_shortlist_dir=shadow_shortlist_dir,
        shadow_monitor_run_dir=shadow_monitor_run_dir,
        consensus_drift_dir=consensus_drift_dir,
        advisory_outcome_dir=advisory_outcome_dir,
    )
    summary = result["promotion_clock_v2_summary"]
    typer.echo(f"aging_id={result['aging_id']}")
    typer.echo(f"aging_dir={result['aging_dir']}")
    typer.echo(f"eligible_for_review_count={summary['eligible_for_review_count']}")
    typer.echo(f"downgrade_recommended_count={summary['downgrade_recommended_count']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_shadow_aging_app.command("report")
def dynamic_v3_shadow_aging_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest shadow aging。"),
    ] = False,
    aging_id: Annotated[str | None, typer.Option("--aging-id", help="aging id。")] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="shadow aging artifact root。"),
    ] = DEFAULT_SHADOW_AGING_DIR,
) -> None:
    """展示 TRADING-139 shadow aging 摘要。"""
    payload = shadow_aging_report_payload(aging_id=aging_id, latest=latest, output_dir=output_dir)
    summary = _mapping_obj(payload.get("promotion_clock_v2_summary"))
    typer.echo(f"aging_id={payload['aging_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"eligible_for_review_count={summary.get('eligible_for_review_count')}")
    typer.echo(f"downgrade_recommended_count={summary.get('downgrade_recommended_count')}")
    typer.echo(f"report_path={payload['shadow_aging_report_path']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_rescue_app.command("validate-shadow-aging")
def dynamic_v3_validate_shadow_aging_command(
    aging_id: Annotated[str, typer.Option("--aging-id", help="aging id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="shadow aging artifact root。"),
    ] = DEFAULT_SHADOW_AGING_DIR,
) -> None:
    """校验 TRADING-139 shadow aging artifact。"""
    payload = validate_shadow_aging_artifact(aging_id=aging_id, output_dir=output_dir)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_candidate_generated=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_weekly_advisory_review_app.command("run")
def dynamic_v3_weekly_advisory_review_run_command(
    week_ending: Annotated[str, typer.Option("--week-ending", help="week ending date。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="weekly advisory review artifact root。"),
    ] = DEFAULT_WEEKLY_ADVISORY_REVIEW_DIR,
    shadow_monitor_run_dir: Annotated[
        Path,
        typer.Option("--shadow-monitor-run-dir", help="shadow monitor run artifact root。"),
    ] = DEFAULT_SHADOW_MONITOR_RUN_DIR,
    daily_advisory_dir: Annotated[
        Path,
        typer.Option("--daily-advisory-dir", help="daily advisory artifact root。"),
    ] = DEFAULT_POSITION_ADVISORY_DAILY_DIR,
    owner_review_dir: Annotated[
        Path,
        typer.Option("--owner-review-dir", help="owner review journal root。"),
    ] = DEFAULT_OWNER_REVIEW_JOURNAL_DIR,
    paper_portfolio_dir: Annotated[
        Path,
        typer.Option("--paper-portfolio-dir", help="paper portfolio artifact root。"),
    ] = DEFAULT_PAPER_PORTFOLIO_DIR,
    advisory_outcome_dir: Annotated[
        Path,
        typer.Option("--advisory-outcome-dir", help="advisory outcome artifact root。"),
    ] = DEFAULT_ADVISORY_OUTCOME_DIR,
    shadow_aging_dir: Annotated[
        Path,
        typer.Option("--shadow-aging-dir", help="shadow aging artifact root。"),
    ] = DEFAULT_SHADOW_AGING_DIR,
) -> None:
    """生成 TRADING-140 weekly advisory review。"""
    result = run_weekly_advisory_review(
        week_ending=_parse_date(week_ending),
        output_dir=output_dir,
        shadow_monitor_run_dir=shadow_monitor_run_dir,
        daily_advisory_dir=daily_advisory_dir,
        owner_review_dir=owner_review_dir,
        paper_portfolio_dir=paper_portfolio_dir,
        advisory_outcome_dir=advisory_outcome_dir,
        shadow_aging_dir=shadow_aging_dir,
    )
    manifest = result["manifest"]
    typer.echo(f"weekly_review_id={result['weekly_review_id']}")
    typer.echo(f"weekly_review_dir={result['weekly_review_dir']}")
    typer.echo(f"weekly_recommendation={manifest['weekly_recommendation']}")
    typer.echo(f"paper_portfolio_status={manifest['paper_portfolio_status']}")
    typer.echo("broker_action_taken=false")


@dynamic_v3_weekly_advisory_review_app.command("report")
def dynamic_v3_weekly_advisory_review_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest weekly advisory review。"),
    ] = False,
    weekly_review_id: Annotated[
        str | None,
        typer.Option("--weekly-review-id", help="weekly review id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="weekly advisory review artifact root。"),
    ] = DEFAULT_WEEKLY_ADVISORY_REVIEW_DIR,
) -> None:
    """展示 TRADING-140 weekly advisory review 摘要。"""
    payload = weekly_advisory_review_report_payload(
        weekly_review_id=weekly_review_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"weekly_review_id={payload['weekly_review_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"weekly_recommendation={payload['weekly_recommendation']}")
    typer.echo(f"paper_portfolio_status={payload['paper_portfolio_status']}")
    typer.echo(f"report_path={payload['weekly_review_report_path']}")
    typer.echo("broker_action_taken=false")


@dynamic_v3_rescue_app.command("validate-weekly-advisory-review")
def dynamic_v3_validate_weekly_advisory_review_command(
    weekly_review_id: Annotated[
        str,
        typer.Option("--weekly-review-id", help="weekly review id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="weekly advisory review artifact root。"),
    ] = DEFAULT_WEEKLY_ADVISORY_REVIEW_DIR,
) -> None:
    """校验 TRADING-140 weekly advisory review artifact。"""
    payload = validate_weekly_advisory_review_artifact(
        weekly_review_id=weekly_review_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_taken=false")
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


@dynamic_v3_promotion_app.command("review")
def dynamic_v3_promotion_review_command(
    candidate_id: Annotated[str, typer.Option("--candidate-id", help="candidate id。")],
    registry_path: Annotated[
        Path,
        typer.Option("--registry", "--registry-path", help="shadow registry path。"),
    ] = DEFAULT_SHADOW_REGISTRY_PATH,
) -> None:
    """展示 TRADING-100 promotion review readiness。"""
    payload = promotion_review_payload(candidate_id=candidate_id, registry_path=registry_path)
    typer.echo(f"candidate_id={candidate_id}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"registry_record_present={payload['registry_record_present']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_promotion_app.command("pack")
def dynamic_v3_promotion_pack_command(
    candidate_id: Annotated[str, typer.Option("--candidate-id", help="candidate id。")],
    registry_path: Annotated[
        Path,
        typer.Option("--registry", "--registry-path", help="shadow registry path。"),
    ] = DEFAULT_SHADOW_REGISTRY_PATH,
    candidate_attribution_dir: Annotated[
        Path,
        typer.Option("--candidate-attribution-dir", help="candidate attribution artifact root。"),
    ] = DEFAULT_CANDIDATE_ATTRIBUTION_DIR,
    data_provenance_dir: Annotated[
        Path,
        typer.Option("--data-provenance-dir", help="data provenance artifact root。"),
    ] = DEFAULT_DATA_PROVENANCE_DIR,
    window_audit_dir: Annotated[
        Path,
        typer.Option("--window-audit-dir", help="window audit artifact root。"),
    ] = DEFAULT_WINDOW_AUDIT_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="promotion artifact root。"),
    ] = DEFAULT_PROMOTION_DIR,
) -> None:
    """生成 TRADING-100 promotion review pack。"""
    try:
        result = build_promotion_pack(
            candidate_id=candidate_id,
            registry_path=registry_path,
            candidate_attribution_dir=candidate_attribution_dir,
            data_provenance_dir=data_provenance_dir,
            window_audit_dir=window_audit_dir,
            output_dir=output_dir,
        )
    except DynamicV3ParameterResearchError as exc:
        raise typer.BadParameter(str(exc)) from exc
    pack = result["pack"]
    typer.echo(f"promotion_id={result['promotion_id']}")
    typer.echo(f"promotion_dir={result['promotion_dir']}")
    typer.echo(f"status={pack['status']}")
    typer.echo("manual_review_required=true")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_rescue_app.command("validate-promotion-pack")
def dynamic_v3_validate_promotion_pack_command(
    candidate_id: Annotated[str, typer.Option("--candidate-id", help="candidate id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="promotion artifact root。"),
    ] = DEFAULT_PROMOTION_DIR,
) -> None:
    """校验 TRADING-100 promotion pack。"""
    payload = validate_promotion_pack(candidate_id=candidate_id, output_dir=output_dir)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_candidate_generated=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


def _resolve_dynamic_v3_sweep_id(*, latest: bool, sweep_id: str | None) -> str:
    if sweep_id:
        return sweep_id
    if latest:
        resolved = latest_sweep_id()
        if resolved:
            return resolved
    raise typer.BadParameter("--sweep-id or --latest is required")


@dynamic_shadow_app.command("package")
def dynamic_shadow_package_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest TRADING-086 robustness report。"),
    ] = True,
    top: Annotated[int, typer.Option("--top", help="review package 包含前 N 个 candidates。")] = 3,
    dynamic_robustness_report_path: Annotated[
        Path | None,
        typer.Option("--dynamic-robustness-report", "--report", help="TRADING-086 report JSON。"),
    ] = None,
    dynamic_calibration_report_path: Annotated[
        Path | None,
        typer.Option("--dynamic-calibration-report", help="TRADING-085 report JSON。"),
    ] = None,
    dynamic_calibration_validation_path: Annotated[
        Path | None,
        typer.Option("--dynamic-calibration-validation", help="TRADING-085 validation JSON。"),
    ] = None,
    dynamic_robustness_validation_path: Annotated[
        Path | None,
        typer.Option("--dynamic-robustness-validation", help="TRADING-086 validation JSON。"),
    ] = None,
    operations_validation_path: Annotated[
        Path | None,
        typer.Option("--operations-validation", help="ETF operations validation JSON。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="dynamic shadow policy config。"),
    ] = DEFAULT_DYNAMIC_SHADOW_POLICY_CONFIG_PATH,
    dynamic_robustness_report_dir: Annotated[
        Path,
        typer.Option("--dynamic-robustness-report-dir", help="TRADING-086 report 目录。"),
    ] = DEFAULT_DYNAMIC_ROBUSTNESS_REPORT_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="dynamic shadow package 输出目录。"),
    ] = DEFAULT_DYNAMIC_SHADOW_PACKAGE_DIR,
) -> None:
    """生成 TRADING-087 dynamic shadow owner review package；不 enroll。"""
    if not latest and dynamic_robustness_report_path is None:
        raise typer.BadParameter("--dynamic-robustness-report or --latest is required")
    resolved_robustness_path = (
        dynamic_robustness_report_path
        or latest_dynamic_robustness_report_path(dynamic_robustness_report_dir)
    )
    if resolved_robustness_path is None:
        raise typer.BadParameter("dynamic robustness report not found")
    calibration_path = dynamic_calibration_report_path
    calibration_payload: dict[str, Any] = {}
    if calibration_path is None:
        calibration_path, calibration_payload = load_latest_dynamic_calibration_report()
    else:
        calibration_payload = _load_optional_json_payload(calibration_path)
    resolved_calibration_validation = dynamic_calibration_validation_path or _latest_json_file(
        PROJECT_ROOT / "reports" / "etf_portfolio" / "dynamic_calibration" / "validation",
        "dynamic-calibration-validation_*.json",
    )
    resolved_robustness_validation = dynamic_robustness_validation_path or _latest_json_file(
        DEFAULT_DYNAMIC_ROBUSTNESS_VALIDATION_DIR,
        "dynamic-robustness-validation_*.json",
    )
    resolved_operations_validation = operations_validation_path or _latest_json_file(
        DEFAULT_ETF_OPERATIONS_VALIDATION_DIR,
        "operations_validation_*.json",
    )
    try:
        package = build_dynamic_shadow_review_package(
            dynamic_robustness_report=_load_optional_json_payload(resolved_robustness_path),
            dynamic_calibration_report=calibration_payload,
            dynamic_calibration_validation=_load_optional_json_payload(
                resolved_calibration_validation
            ),
            dynamic_robustness_validation=_load_optional_json_payload(
                resolved_robustness_validation
            ),
            operations_validation=_load_optional_json_payload(resolved_operations_validation),
            source_paths={
                "dynamic_robustness_report": str(resolved_robustness_path),
                "dynamic_calibration_report": (
                    "" if calibration_path is None else str(calibration_path)
                ),
                "dynamic_calibration_validation": (
                    ""
                    if resolved_calibration_validation is None
                    else str(resolved_calibration_validation)
                ),
                "dynamic_robustness_validation": (
                    ""
                    if resolved_robustness_validation is None
                    else str(resolved_robustness_validation)
                ),
                "operations_validation": (
                    ""
                    if resolved_operations_validation is None
                    else str(resolved_operations_validation)
                ),
            },
            policy=load_dynamic_shadow_policy_config(config_path),
            top=top,
        )
    except DynamicShadowError as exc:
        raise typer.BadParameter(str(exc)) from exc
    paths = write_dynamic_shadow_review_package(package, output_dir=output_dir)
    summary = _mapping_obj(package.get("review_summary"))
    typer.echo(f"ETF dynamic shadow package JSON：{paths['json']}")
    typer.echo(f"ETF dynamic shadow package Markdown：{paths['markdown']}")
    typer.echo(f"status={summary.get('status')}")
    typer.echo(f"top_candidate={summary.get('top_candidate')}")
    typer.echo(
        f"ready_after_owner_approval_count={summary.get('ready_after_owner_approval_count')}"
    )
    typer.echo(f"blocked_count={summary.get('blocked_count')}")
    typer.echo("automatic_enrollment_allowed=false")
    typer.echo("commands_executed=false")
    typer.echo("production_state_mutated=false")
    typer.echo("baseline_config_mutated=false")
    typer.echo("official_target_weights_mutated=false")
    typer.echo("automatic_candidate_promotion=false")
    typer.echo("auto_enrollment_without_owner_approval=false")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@dynamic_shadow_app.command("approve")
def dynamic_shadow_approve_command(
    review_package_path: Annotated[
        Path,
        typer.Option("--review-package-path", "--package", help="dynamic shadow package JSON。"),
    ],
    candidate_id: Annotated[
        str,
        typer.Option("--candidate", "--candidate-id", help="dynamic candidate id。"),
    ],
    owner_decision: Annotated[
        str,
        typer.Option("--owner-decision", help="owner decision。"),
    ] = "approved_for_dynamic_shadow",
    rationale: Annotated[str, typer.Option("--rationale", help="owner rationale。")] = "",
    confidence: Annotated[float, typer.Option("--confidence", help="0.0-1.0 confidence。")] = 0.5,
    decision_journal_link: Annotated[
        str | None,
        typer.Option("--decision-journal-link", help="decision journal entry/link。"),
    ] = None,
    reviewer: Annotated[str, typer.Option("--reviewer", help="reviewer id。")] = "project_owner",
    condition: Annotated[
        list[str] | None,
        typer.Option("--condition", help="approval condition; can be repeated。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="dynamic shadow policy config。"),
    ] = DEFAULT_DYNAMIC_SHADOW_POLICY_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="owner approval 输出目录。"),
    ] = DEFAULT_DYNAMIC_SHADOW_APPROVAL_DIR,
) -> None:
    """捕获 dynamic shadow owner approval；不 enroll、不修改 production。"""
    try:
        approval = build_dynamic_shadow_owner_approval(
            review_package=_load_optional_json_payload(review_package_path),
            candidate_id=candidate_id,
            owner_decision=owner_decision,
            rationale=rationale,
            confidence=confidence,
            decision_journal_link=decision_journal_link,
            conditions=condition,
            reviewer=reviewer,
            policy=load_dynamic_shadow_policy_config(config_path),
        )
    except DynamicShadowError as exc:
        raise typer.BadParameter(str(exc)) from exc
    paths = write_dynamic_shadow_owner_approval(approval, output_dir=output_dir)
    typer.echo(f"ETF dynamic shadow approval JSON：{paths['json']}")
    typer.echo(f"ETF dynamic shadow approval Markdown：{paths['markdown']}")
    typer.echo(f"approval_id={approval['approval_id']}")
    typer.echo(f"owner_decision={approval['owner_decision']}")
    typer.echo(f"approved_for_enrollment={approval['approved_for_enrollment']}")
    typer.echo(f"candidate={approval['candidate_id']}")
    typer.echo("commands_executed=false")
    typer.echo("production_state_mutated=false")
    typer.echo("baseline_config_mutated=false")
    typer.echo("official_target_weights_mutated=false")
    typer.echo("automatic_candidate_promotion=false")
    typer.echo("auto_enrollment_without_owner_approval=false")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@dynamic_shadow_app.command("enroll-approved")
def dynamic_shadow_enroll_approved_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest package 和 approval。"),
    ] = False,
    approval_path: Annotated[
        Path | None,
        typer.Option("--approval-path", "--approval", help="owner approval JSON。"),
    ] = None,
    review_package_path: Annotated[
        Path | None,
        typer.Option("--review-package-path", "--package", help="dynamic shadow package JSON。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="dynamic shadow policy config。"),
    ] = DEFAULT_DYNAMIC_SHADOW_POLICY_CONFIG_PATH,
    registry_path: Annotated[
        Path,
        typer.Option("--registry-path", help="dynamic shadow candidate registry path。"),
    ] = DEFAULT_DYNAMIC_SHADOW_REGISTRY_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="approved enrollment 输出目录。"),
    ] = DEFAULT_DYNAMIC_SHADOW_ENROLLMENT_DIR,
) -> None:
    """只登记 owner-approved dynamic candidates 进入 forward shadow observation。"""
    resolved_approval = approval_path or (
        latest_dynamic_shadow_owner_approval_path() if latest else None
    )
    resolved_package = review_package_path or (
        latest_dynamic_shadow_review_package_path() if latest else None
    )
    if resolved_approval is None or resolved_package is None:
        raise typer.BadParameter("--approval/--package or --latest is required")
    try:
        enrollment = build_dynamic_shadow_approved_enrollment(
            approval=_load_optional_json_payload(resolved_approval),
            review_package=_load_optional_json_payload(resolved_package),
            policy=load_dynamic_shadow_policy_config(config_path),
        )
        registry = upsert_dynamic_shadow_candidate_registry(
            load_dynamic_shadow_candidate_registry(registry_path),
            enrollment,
        )
    except DynamicShadowError as exc:
        raise typer.BadParameter(str(exc)) from exc
    paths = write_dynamic_shadow_approved_enrollment(enrollment, output_dir=output_dir)
    write_dynamic_shadow_candidate_registry(registry, registry_path=registry_path)
    typer.echo(f"ETF dynamic shadow enrollment JSON：{paths['json']}")
    typer.echo(f"ETF dynamic shadow enrollment Markdown：{paths['markdown']}")
    typer.echo(f"dynamic_shadow_registry={registry_path}")
    typer.echo(f"enrollment_id={enrollment['enrollment_id']}")
    typer.echo(f"candidate={enrollment['candidate_id']}")
    typer.echo(f"tracking_status={enrollment['tracking_status']}")
    typer.echo(f"active_candidate_count={registry['candidate_count']}")
    typer.echo("commands_executed=false")
    typer.echo("production_state_mutated=false")
    typer.echo("baseline_config_mutated=false")
    typer.echo("official_target_weights_mutated=false")
    typer.echo("automatic_candidate_promotion=false")
    typer.echo("auto_enrollment_without_owner_approval=false")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@dynamic_shadow_app.command("update")
def dynamic_shadow_update_command(
    date_option: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="forward update 日期 YYYY-MM-DD 或 latest。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="使用 ETF price cache 最新日期。"),
    ] = False,
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="标准化 ETF daily price cache。"),
    ] = DEFAULT_ETF_PRICE_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="标准化 FRED rates cache for validate-data gate。"),
    ] = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv",
    registry_path: Annotated[
        Path,
        typer.Option("--registry-path", help="dynamic shadow candidate registry path。"),
    ] = DEFAULT_DYNAMIC_SHADOW_REGISTRY_PATH,
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="dynamic shadow policy config。"),
    ] = DEFAULT_DYNAMIC_SHADOW_POLICY_CONFIG_PATH,
    dynamic_robustness_config_path: Annotated[
        Path,
        typer.Option("--dynamic-robustness-config", help="TRADING-086 policy config。"),
    ] = DEFAULT_DYNAMIC_ROBUSTNESS_POLICY_CONFIG_PATH,
    dynamic_allocation_config_path: Annotated[
        Path,
        typer.Option("--dynamic-allocation-config", help="TRADING-084 policy config。"),
    ] = DEFAULT_DYNAMIC_ALLOCATION_POLICY_CONFIG_PATH,
    data_quality_output_path: Annotated[
        Path | None,
        typer.Option("--data-quality-output-path", help="validate-data markdown output path。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="dynamic shadow forward update 输出目录。"),
    ] = DEFAULT_DYNAMIC_SHADOW_FORWARD_UPDATE_DIR,
) -> None:
    """更新 approved dynamic shadow candidates 的 forward tracking records。"""
    if latest and date_option is not None:
        raise typer.BadParameter("--latest and --date cannot be combined")
    run_date = _resolve_date(
        "latest" if latest or date_option is None else date_option,
        prices_path=prices_path,
    )
    quality_output = data_quality_output_path or default_quality_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        run_date,
    )
    universe = load_universe()
    data_quality_report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=configured_price_tickers(universe),
        expected_rate_series=configured_rate_series(universe),
        quality_config=load_data_quality(),
        as_of=run_date,
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
        etf_config = load_etf_config_bundle()
        prices, etf_quality = load_standard_prices(
            prices_path,
            etf_config.assets,
            etf_config.strategy,
        )
        if not etf_quality.passed:
            raise DynamicShadowError(f"ETF price validation failed: {etf_quality.status}")
        calibration_path, calibration_payload = load_latest_dynamic_calibration_report()
        payload = build_dynamic_shadow_forward_update(
            registry=load_dynamic_shadow_candidate_registry(registry_path),
            policy=load_dynamic_shadow_policy_config(config_path),
            as_of=run_date,
            data_quality_status=data_quality_report.status,
            data_quality_report=str(quality_output),
            prices=prices,
            etf_config=etf_config,
            dynamic_robustness_policy=load_dynamic_robustness_policy_config(
                dynamic_robustness_config_path
            ),
            dynamic_allocation_policy=load_dynamic_allocation_policy_config(
                dynamic_allocation_config_path
            ),
            dynamic_calibration_report=calibration_payload,
            dynamic_calibration_report_path=calibration_path,
            prices_path=prices_path,
        )
    except DynamicShadowError as exc:
        raise typer.BadParameter(str(exc)) from exc
    paths = write_dynamic_shadow_forward_update(payload, output_dir=output_dir)
    typer.echo(f"ETF dynamic shadow forward update JSON：{paths['json']}")
    typer.echo(f"ETF dynamic shadow forward update Markdown：{paths['markdown']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"active_candidate_count={payload['active_candidate_count']}")
    typer.echo("commands_executed=false")
    typer.echo("production_state_mutated=false")
    typer.echo("baseline_config_mutated=false")
    typer.echo("official_target_weights_mutated=false")
    typer.echo("automatic_candidate_promotion=false")
    typer.echo("auto_enrollment_without_owner_approval=false")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@dynamic_shadow_app.command("weekly-review")
def dynamic_shadow_weekly_review_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest forward update。"),
    ] = True,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="weekly review 日期 YYYY-MM-DD。"),
    ] = None,
    forward_update_path: Annotated[
        Path | None,
        typer.Option("--forward-update", help="dynamic shadow forward update JSON。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="dynamic shadow policy config。"),
    ] = DEFAULT_DYNAMIC_SHADOW_POLICY_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="dynamic shadow weekly review 输出目录。"),
    ] = DEFAULT_DYNAMIC_SHADOW_WEEKLY_REVIEW_DIR,
) -> None:
    """生成 weekly dynamic shadow review；不 promotion、不写 production。"""
    resolved_update = forward_update_path or (
        latest_dynamic_shadow_forward_update_path() if latest else None
    )
    if resolved_update is None:
        raise typer.BadParameter("--forward-update or --latest is required")
    try:
        payload = build_dynamic_shadow_weekly_review(
            forward_update=_load_optional_json_payload(resolved_update),
            policy=load_dynamic_shadow_policy_config(config_path),
            as_of=_parse_date(as_of) if as_of else None,
        )
    except DynamicShadowError as exc:
        raise typer.BadParameter(str(exc)) from exc
    paths = write_dynamic_shadow_weekly_review(payload, output_dir=output_dir)
    summary = _mapping_obj(payload.get("summary"))
    typer.echo(f"ETF dynamic shadow weekly review JSON：{paths['json']}")
    typer.echo(f"ETF dynamic shadow weekly review Markdown：{paths['markdown']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"candidate_count={summary.get('candidate_count')}")
    typer.echo(f"watch_count={summary.get('watch_count')}")
    typer.echo(f"reject_pending_review_count={summary.get('reject_pending_review_count')}")
    typer.echo("commands_executed=false")
    typer.echo("production_state_mutated=false")
    typer.echo("baseline_config_mutated=false")
    typer.echo("official_target_weights_mutated=false")
    typer.echo("automatic_candidate_promotion=false")
    typer.echo("auto_enrollment_without_owner_approval=false")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@dynamic_shadow_app.command("validate")
def dynamic_shadow_validate_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="dynamic shadow policy config。"),
    ] = DEFAULT_DYNAMIC_SHADOW_POLICY_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="validation report 输出目录。"),
    ] = DEFAULT_DYNAMIC_SHADOW_VALIDATION_DIR,
) -> None:
    """校验 TRADING-087 dynamic shadow workflow 和 approved-only safety boundary。"""
    payload = build_dynamic_shadow_validation_report(config_path=config_path)
    paths = write_dynamic_shadow_validation_report(payload, output_dir=output_dir)
    typer.echo(f"ETF dynamic shadow validation JSON：{paths['json']}")
    typer.echo(f"ETF dynamic shadow validation Markdown：{paths['markdown']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("commands_executed=false")
    typer.echo("production_state_mutated=false")
    typer.echo("baseline_config_mutated=false")
    typer.echo("official_target_weights_mutated=false")
    typer.echo("automatic_candidate_promotion=false")
    typer.echo("auto_enrollment_without_owner_approval=false")
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
            f"ETF 数据质量状态：{quality_report.status}，已停止 satellite attribution build。"
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
            f"ETF 数据质量状态：{quality_report.status}，已停止 satellite attribution report。"
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
        run_dir = find_latest_experiment_run_dir(output_dir) if latest else output_dir / str(run_id)
        payload = build_experiment_comparison_report(run_dir)
        pack_id = payload["run_metadata"].get("pack_id")
        if pack_id:
            pack_registry = load_experiment_pack_registry()
            pack_config = pack_registry.experiment_packs.get(str(pack_id))
            if pack_config is not None:
                payload = apply_ranking_policy_to_comparison_report(
                    payload,
                    ranking_policy=pack_registry.ranking_policies[pack_config.ranking_policy],
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
        f"eligible_for_manual_review_count={payload['summary']['eligible_for_manual_review_count']}"
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
    typer.echo(f"selected_weight_set_count={len(latest_selection.get('weight_set_ids') or [])}")
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
        typer.echo(f"ETF 数据质量状态：{quality_report.status}，已停止 weight diagnostics。")
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
    source_url: Annotated[str, typer.Option(help="Source URL 或人工输入说明。")] = ("manual_input"),
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
    source_url: Annotated[str, typer.Option(help="Source URL 或人工输入说明。")] = ("manual_input"),
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
        output_path or DEFAULT_ETF_REPORT_DIR / f"{run_date.isoformat()}_portfolio_brief.md"
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
        raise typer.BadParameter("--cadence must be one of: daily, weekly, biweekly, monthly")
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
        item for item in output_dir.iterdir() if item.is_dir() and (item / "summary.json").exists()
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
        character if character.isalnum() or character in "._-" else "_" for character in text
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
    results = (
        [dict(item) for item in raw_results if isinstance(item, Mapping)]
        if isinstance(
            raw_results,
            list,
        )
        else []
    )
    typer.echo(f"ETF weight candidate shadow enrollment：{enrollment_path}")
    typer.echo(f"enrollment_count={enrollment['enrollment_count']}")
    typer.echo(f"selected_weight_set_count={len(latest.get('weight_set_ids') or [])}")
    for result in results:
        typer.echo("enrollment_result=" + json.dumps(result, ensure_ascii=False, sort_keys=True))
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


def _json_float_mapping_option(value: str | None, *, option_name: str) -> dict[str, float]:
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


def _mapping_obj(value: object) -> dict[str, object]:
    return dict(value) if isinstance(value, dict) else {}


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
