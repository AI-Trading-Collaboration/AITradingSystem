from __future__ import annotations

import json
import os
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Annotated, Literal, cast

import pandas as pd
import typer
import yaml
from rich.console import Console

from ai_trading_system.alerts import (
    build_daily_alert_report,
    build_pipeline_health_alert_report,
    default_alert_report_path,
    default_pipeline_health_alert_report_path,
    render_alert_summary_section,
    write_alert_report,
)
from ai_trading_system.backtest.audit import (
    build_backtest_audit_report,
    default_backtest_audit_report_path,
    write_backtest_audit_report,
)
from ai_trading_system.backtest.daily import (
    DEFAULT_BENCHMARK_TICKERS,
    BacktestRegimeContext,
    DailyBacktestResult,
    default_backtest_daily_path,
    default_backtest_input_coverage_path,
    default_backtest_report_path,
    prepare_daily_score_backtest_context,
    run_daily_score_backtest,
    write_backtest_daily_csv,
    write_backtest_input_coverage_csv,
    write_backtest_report,
)
from ai_trading_system.backtest.gate_attribution import (
    build_gate_event_attribution_report,
    default_gate_event_attribution_report_path,
    infer_input_coverage_path,
    write_gate_event_attribution_report,
)
from ai_trading_system.backtest.input_gaps import (
    build_backtest_input_gap_report,
    default_backtest_input_gap_report_path,
    write_backtest_input_gap_report,
)
from ai_trading_system.backtest.lag_sensitivity import (
    BacktestLagSensitivityReport,
    BacktestLagSensitivityScenario,
    default_backtest_lag_sensitivity_report_path,
    default_backtest_lag_sensitivity_summary_path,
    write_backtest_lag_sensitivity_report,
    write_backtest_lag_sensitivity_summary,
)
from ai_trading_system.backtest.pit_coverage import (
    build_backtest_pit_coverage_report,
    default_backtest_pit_coverage_report_path,
    write_backtest_pit_coverage_report,
)
from ai_trading_system.backtest.promotion_gate import (
    build_model_promotion_report,
    default_model_promotion_report_path,
    default_model_promotion_summary_path,
    render_model_promotion_report,
    write_model_promotion_report,
    write_model_promotion_summary,
)
from ai_trading_system.backtest.robustness import (
    BacktestRobustnessReport,
    BacktestRobustnessScenario,
    default_backtest_robustness_report_path,
    default_backtest_robustness_summary_path,
    fixed_total_asset_exposure_scenario,
    model_target_no_gate_scenario,
    module_subset_baseline_scenario,
    rebalance_interval_scenario,
    same_exposure_random_scenario,
    same_turnover_random_scenario,
    volatility_targeted_exposure_scenario,
    write_backtest_robustness_report,
    write_backtest_robustness_summary,
)
from ai_trading_system.belief_state import (
    DEFAULT_BELIEF_STATE_DIR,
    DEFAULT_BELIEF_STATE_HISTORY_PATH,
    append_belief_state_history,
    build_belief_state,
    default_belief_state_path,
    render_belief_state_summary,
    write_belief_state,
)
from ai_trading_system.benchmark_policy import (
    DEFAULT_BENCHMARK_POLICY_CONFIG_PATH,
    default_benchmark_policy_report_path,
    load_benchmark_policy,
    lookup_benchmark_policy_entry,
    render_benchmark_policy_lookup,
    validate_benchmark_policy,
    write_benchmark_policy_report,
)
from ai_trading_system.calibration_protocol import (
    DEFAULT_CALIBRATION_PROTOCOL_PATH,
    default_calibration_protocol_report_path,
    load_calibration_protocol_manifest,
    validate_calibration_protocol_manifest,
    write_calibration_protocol_report,
)
from ai_trading_system.catalyst_calendar import (
    load_catalyst_calendar,
    validate_catalyst_calendar,
)
from ai_trading_system.cli_commands.catalysts import catalysts_app
from ai_trading_system.cli_commands.data import data_app
from ai_trading_system.cli_commands.data_artifacts import (
    _resolve_market_data_freshness_path,
    _resolve_market_data_refresh_path,
)
from ai_trading_system.cli_commands.data_sources import data_sources_app
from ai_trading_system.cli_commands.docs import docs_app
from ai_trading_system.cli_commands.etf_compat import (
    experiments_app,
    features_app,
    regime_app,
    register_etf_compatibility_aliases,
    report_app,
    run_app,
    simulation_app,
)
from ai_trading_system.cli_commands.etf_portfolio import etf_app
from ai_trading_system.cli_commands.evidence import evidence_app
from ai_trading_system.cli_commands.execution import execution_app
from ai_trading_system.cli_commands.fundamentals import fundamentals_app
from ai_trading_system.cli_commands.industry_chain import industry_chain_app
from ai_trading_system.cli_commands.llm import llm_app
from ai_trading_system.cli_commands.parameter_artifacts import (
    _resolve_shadow_backtest_summary_path,
    _resolve_weight_stability_path,
    _resolve_weight_stability_readiness_path,
    _resolve_weight_tuning_failure_path,
    _resolve_weight_tuning_path,
)
from ai_trading_system.cli_commands.parameters import parameters_app
from ai_trading_system.cli_commands.pit_snapshots import pit_snapshots_app
from ai_trading_system.cli_commands.portfolio import portfolio_app
from ai_trading_system.cli_commands.portfolio_artifacts import (
    _resolve_portfolio_candidate_review_decision_path,
    _resolve_portfolio_candidate_tracking_path,
    _resolve_portfolio_candidates_path,
    _resolve_portfolio_sensitivity_path,
    _resolve_portfolio_tracking_review_path,
    _resolve_portfolio_turnover_attribution_path,
)
from ai_trading_system.cli_commands.risk_event_artifacts import (
    DEFAULT_OPENAI_REQUEST_CACHE_PATH,
    DEFAULT_RISK_EVENT_DAILY_PREREVIEW_PROFILE,
    DEFAULT_RISK_EVENT_OCCURRENCES_PATH,
    DEFAULT_RISK_EVENT_PREREVIEW_QUEUE_PATH,
    _coalesce_profile_value,
    _load_llm_request_profile,
)
from ai_trading_system.cli_commands.risk_events import risk_events_app
from ai_trading_system.cli_commands.scenarios import scenarios_app
from ai_trading_system.cli_commands.sec_pit import sec_pit_app
from ai_trading_system.cli_commands.security import security_app
from ai_trading_system.cli_commands.signals import signals_app
from ai_trading_system.cli_commands.thesis import thesis_app
from ai_trading_system.cli_commands.trace import trace_app
from ai_trading_system.cli_commands.valuation import valuation_app
from ai_trading_system.cli_commands.watchlist import watchlist_app
from ai_trading_system.config import (
    DEFAULT_BACKTEST_VALIDATION_POLICY_CONFIG_PATH,
    DEFAULT_CATALYST_CALENDAR_CONFIG_PATH,
    DEFAULT_CONFIG_PATH,
    DEFAULT_DATA_QUALITY_CONFIG_PATH,
    DEFAULT_DATA_SOURCES_CONFIG_PATH,
    DEFAULT_EXECUTION_POLICY_CONFIG_PATH,
    DEFAULT_FEATURE_CONFIG_PATH,
    DEFAULT_FUNDAMENTAL_FEATURES_CONFIG_PATH,
    DEFAULT_FUNDAMENTAL_METRICS_CONFIG_PATH,
    DEFAULT_INDUSTRY_CHAIN_CONFIG_PATH,
    DEFAULT_MARKET_REGIMES_CONFIG_PATH,
    DEFAULT_PORTFOLIO_CONFIG_PATH,
    DEFAULT_RISK_EVENTS_CONFIG_PATH,
    DEFAULT_SCORING_RULES_CONFIG_PATH,
    DEFAULT_SEC_COMPANIES_CONFIG_PATH,
    DEFAULT_WATCHLIST_CONFIG_PATH,
    PROJECT_ROOT,
    IndustryChainConfig,
    ScoringRulesConfig,
    SecCompaniesConfig,
    UniverseConfig,
    WatchlistConfig,
    configured_price_tickers,
    configured_rate_series,
    load_backtest_validation_policy,
    load_data_quality,
    load_data_sources,
    load_features,
    load_fundamental_features,
    load_fundamental_metrics,
    load_industry_chain,
    load_market_regimes,
    load_portfolio,
    load_risk_events,
    load_scoring_rules,
    load_sec_companies,
    load_universe,
    load_watchlist,
    market_regime_by_id,
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
from ai_trading_system.data.download import (
    default_download_failure_report_path,
    download_daily_data,
    write_download_failure_report,
)
from ai_trading_system.data.market_data import (
    FmpPriceProvider,
    MarketstackPriceProvider,
    YFinancePriceProvider,
)
from ai_trading_system.data.quality import (
    DataQualityReport,
    default_quality_report_path,
    marketstack_reconciliation_path,
    validate_data_cache,
    write_data_quality_report,
)
from ai_trading_system.decision_causal_chains import (
    DEFAULT_DECISION_CAUSAL_CHAIN_PATH,
    build_decision_causal_chain_ledger,
    default_decision_causal_chain_report_path,
    load_decision_causal_chain_ledger,
    load_decision_outcomes_frame,
    lookup_decision_causal_chain,
    render_decision_causal_chain_lookup,
    write_decision_causal_chain_ledger,
    write_decision_causal_chain_report,
)
from ai_trading_system.decision_learning_queue import (
    DEFAULT_DECISION_LEARNING_QUEUE_PATH,
    build_decision_learning_queue,
    default_decision_learning_queue_report_path,
    load_decision_learning_queue,
    lookup_decision_learning_item,
    render_decision_learning_item_lookup,
    write_decision_learning_queue,
    write_decision_learning_queue_report,
)
from ai_trading_system.decision_outcomes import (
    DEFAULT_DECISION_OUTCOMES_PATH,
    DEFAULT_OUTCOME_HORIZONS,
    build_decision_outcomes,
    default_decision_calibration_report_path,
    load_decision_snapshots,
    write_decision_calibration_report,
    write_decision_outcomes_csv,
)
from ai_trading_system.decision_snapshots import (
    DEFAULT_DECISION_SNAPSHOT_DIR,
    build_decision_snapshot,
    default_decision_snapshot_path,
    write_decision_snapshot,
)
from ai_trading_system.documentation_contract import default_documentation_contract_json_path
from ai_trading_system.evidence_dashboard import (
    build_evidence_dashboard_report,
    default_evidence_dashboard_json_path,
    default_evidence_dashboard_path,
    write_evidence_dashboard,
    write_evidence_dashboard_json,
)
from ai_trading_system.execution_policy import (
    build_execution_advisory,
    default_execution_policy_report_path,
    load_execution_policy,
    render_execution_advisory_section,
    validate_execution_policy,
    write_execution_policy_report,
)
from ai_trading_system.explain import (
    DEFAULT_ARTIFACT_CATALOG_PATH,
    DEFAULT_CALCULATION_LOGIC_PATH,
    DEFAULT_FIELDS_PATH,
    explain_query,
    render_explain_result,
)
from ai_trading_system.external_request_cache import sanitize_diagnostic_text
from ai_trading_system.feature_availability import (
    DEFAULT_FEATURE_AVAILABILITY_CONFIG_PATH,
    FeatureAvailabilitySourceCheck,
    build_feature_availability_report,
    build_feature_source_check,
    default_feature_availability_report_path,
    feature_availability_summary_record,
    render_feature_availability_section,
    write_feature_availability_report,
)
from ai_trading_system.features.market import (
    build_market_features,
    default_feature_report_path,
    write_feature_summary,
    write_features_csv,
)
from ai_trading_system.feedback_loop_review import (
    build_feedback_loop_review_report,
    default_feedback_loop_review_report_path,
    write_feedback_loop_review_report,
)
from ai_trading_system.feedback_sample_policy import (
    DEFAULT_FEEDBACK_SAMPLE_POLICY_PATH,
    load_feedback_sample_policy,
)
from ai_trading_system.fmp_forward_pit import (
    DEFAULT_FMP_FORWARD_PIT_NORMALIZED_DIR,
    default_fmp_forward_pit_fetch_report_path,
    default_fmp_forward_pit_normalized_path,
)
from ai_trading_system.focus_stock_trends import (
    build_focus_stock_trend_report,
    render_focus_stock_trend_section,
)
from ai_trading_system.fundamentals.sec_features import (
    SecFundamentalFeaturesReport,
    build_sec_fundamental_features_report,
    build_sec_fundamental_features_report_from_metric_rows,
    default_sec_fundamental_features_csv_path,
    default_sec_fundamental_features_report_path,
    write_sec_fundamental_features_csv,
    write_sec_fundamental_features_report,
)
from ai_trading_system.fundamentals.sec_metrics import (
    build_sec_fundamental_metrics_report,
    default_sec_fundamental_metrics_csv_path,
    default_sec_fundamental_metrics_validation_report_path,
    load_sec_fundamental_metric_rows_csv,
    validate_sec_fundamental_metric_rows,
    validate_sec_fundamental_metrics_csv,
    write_sec_fundamental_metric_rows_csv,
    write_sec_fundamental_metrics_validation_report,
)
from ai_trading_system.fundamentals.sec_pit_backfill import DEFAULT_SEC_EDGAR_PROCESSED_DIR
from ai_trading_system.fundamentals.sec_pit_panel import (
    sec_pit_feature_panel_to_feature_reports,
)
from ai_trading_system.fundamentals.sec_validation import (
    default_sec_companyfacts_validation_report_path,
    validate_sec_companyfacts_cache,
    write_sec_companyfacts_validation_report,
)
from ai_trading_system.fundamentals.tsm_ir import (
    TsmIrQuarterlyMetricRow,
    load_tsm_ir_quarterly_metric_rows_csv,
    merge_tsm_ir_quarterly_rows_into_sec_metrics_as_of,
    select_tsm_ir_quarterly_metric_rows_as_of,
)
from ai_trading_system.historical_inputs import (
    build_historical_risk_event_occurrence_review_report,
    build_historical_valuation_review_report,
)
from ai_trading_system.historical_replay import (
    default_historical_replay_output_root,
    run_historical_day_replay,
    run_historical_replay_window,
)
from ai_trading_system.industry_node_state import (
    build_industry_node_heat_report,
    render_industry_node_heat_section,
)
from ai_trading_system.llm_request_profiles import (
    DEFAULT_LLM_REQUEST_PROFILES_CONFIG_PATH,
)
from ai_trading_system.market_feedback_optimization import (
    DEFAULT_MARKET_FEEDBACK_REPLAY_START,
    build_market_feedback_optimization_report,
    default_market_feedback_optimization_report_path,
    write_market_feedback_optimization_report,
)
from ai_trading_system.official_policy_sources import (
    DEFAULT_OFFICIAL_POLICY_PROCESSED_DIR,
    DEFAULT_OFFICIAL_POLICY_RAW_DIR,
    default_official_policy_candidates_path,
    default_official_policy_fetch_report_path,
    fetch_official_policy_sources,
    write_official_policy_fetch_report,
)
from ai_trading_system.ops_daily import (
    build_daily_ops_plan,
    default_daily_ops_plan_path,
    default_daily_ops_run_metadata_path,
    default_daily_ops_run_report_path,
    resolve_daily_ops_default_as_of,
    run_daily_ops_plan,
    write_daily_ops_plan,
    write_daily_ops_run_report,
)
from ai_trading_system.order_intent_candidates import (
    default_order_intent_candidates_path,
    write_order_intent_candidates_json,
)
from ai_trading_system.parameter_candidates import (
    DEFAULT_PARAMETER_CANDIDATE_LEDGER_PATH,
    build_parameter_candidate_ledger,
    default_parameter_candidate_report_path,
    load_parameter_candidate_ledger,
    write_parameter_candidate_ledger,
    write_parameter_candidate_report,
)
from ai_trading_system.parameter_governance import (
    DEFAULT_PARAMETER_GOVERNANCE_MANIFEST_PATH,
    build_parameter_governance_report,
    default_parameter_governance_report_path,
    default_parameter_governance_summary_path,
    write_parameter_governance_report,
    write_parameter_governance_summary,
)
from ai_trading_system.parameter_replay import (
    DEFAULT_BACKTEST_ROBUSTNESS_DIR,
    build_parameter_replay_report,
    default_parameter_replay_report_path,
    default_parameter_replay_summary_path,
    latest_backtest_robustness_summary_path,
    write_parameter_replay_report,
    write_parameter_replay_summary,
)
from ai_trading_system.periodic_investment_review import (
    DEFAULT_PERIODIC_INVESTMENT_REVIEW_REPORT_DIR,
    DEFAULT_SCORES_DAILY_PATH,
    build_periodic_investment_review_report,
    default_periodic_investment_review_report_path,
    write_periodic_investment_review_report,
)
from ai_trading_system.pipeline_health import (
    PipelineArtifactSpec,
    build_pipeline_health_report,
    build_pit_snapshot_health_checks,
    default_pipeline_health_report_path,
    write_pipeline_health_report,
)
from ai_trading_system.pit_snapshots import (
    DEFAULT_PIT_SNAPSHOT_MANIFEST_PATH,
    default_pit_snapshot_validation_report_path,
    validate_pit_snapshot_manifest,
)
from ai_trading_system.portfolio_exposure import (
    build_portfolio_exposure_report,
    default_portfolio_exposure_report_path,
    render_portfolio_exposure_section,
    write_portfolio_exposure_report,
)
from ai_trading_system.prediction_ledger import (
    DEFAULT_PARAMETER_SHADOW_PREDICTION_LEDGER_PATH,
    DEFAULT_PREDICTION_LEDGER_PATH,
    DEFAULT_PREDICTION_OUTCOMES_PATH,
    append_prediction_records,
    build_parameter_shadow_prediction_records,
    build_prediction_outcomes,
    build_prediction_record_from_decision_snapshot,
    build_shadow_maturity_report,
    build_shadow_prediction_records,
    build_shadow_prediction_run_report,
    default_prediction_outcome_report_path,
    default_shadow_maturity_report_path,
    default_shadow_prediction_report_path,
    load_prediction_ledger,
    load_prediction_outcomes,
    write_prediction_outcome_report,
    write_prediction_outcomes_csv,
    write_shadow_maturity_report,
    write_shadow_prediction_run_report,
)
from ai_trading_system.report_traceability import (
    build_backtest_trace_bundle,
    build_daily_score_trace_bundle,
    default_report_trace_bundle_path,
    render_traceability_section,
    write_trace_bundle,
)
from ai_trading_system.reports.calculation_explainers import (
    DEFAULT_METRIC_EXPLAINERS_CONFIG_PATH,
    build_calculation_explainers_payload,
    default_calculation_explainers_path,
    write_calculation_explainers_json,
)
from ai_trading_system.reports.daily import render_recommendation_markdown
from ai_trading_system.reports.market_panel import (
    build_market_panel_payload,
    default_market_panel_json_path,
    default_market_panel_report_path,
    write_market_panel_json,
    write_market_panel_report,
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
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    build_report_index_payload,
    default_report_index_html_path,
    default_report_index_json_path,
    write_report_index_html,
    write_report_index_json,
)
from ai_trading_system.reports.research_governance_summary import (
    build_research_governance_summary_payload,
    default_research_governance_summary_json_path,
    default_research_governance_summary_report_path,
    write_research_governance_summary_json,
    write_research_governance_summary_report,
)
from ai_trading_system.reports.score_change_attribution import (
    build_score_change_attribution_payload,
    default_score_change_attribution_json_path,
    default_score_change_attribution_report_path,
    write_score_change_attribution_json,
    write_score_change_attribution_report,
)
from ai_trading_system.risk_event_llm_formal import (
    build_llm_formal_assessment_report,
    default_llm_formal_assessment_report_path,
    write_llm_formal_assessment_outputs,
    write_llm_formal_assessment_report,
)
from ai_trading_system.risk_event_prereview import (
    default_risk_event_openai_prereview_report_path,
    run_openai_risk_event_prereview_for_official_candidates,
    write_risk_event_prereview_import_report,
    write_risk_event_prereview_queue,
)
from ai_trading_system.risk_events import (
    RiskEventOccurrenceReviewReport,
    RiskEventsValidationReport,
    build_risk_event_occurrence_review_report,
    default_risk_event_occurrence_report_path,
    load_risk_event_occurrence_store,
    validate_risk_event_occurrence_store,
    validate_risk_events_config,
    write_risk_event_occurrence_review_report,
)
from ai_trading_system.rule_experiments import (
    DEFAULT_RULE_EXPERIMENT_LEDGER_PATH,
    build_rule_experiment_ledger,
    default_rule_experiment_report_path,
    load_rule_experiment_ledger,
    lookup_rule_experiment,
    render_rule_experiment_lookup,
    write_rule_experiment_ledger,
    write_rule_experiment_report,
)
from ai_trading_system.rule_governance import (
    DEFAULT_RULE_CARDS_PATH,
    build_rule_version_manifest,
    default_rule_governance_report_path,
    load_rule_card_store,
    lookup_rule_card,
    promote_rule_card,
    render_rule_card_lookup,
    retire_rule_card,
    validate_rule_card_store,
    write_rule_governance_report,
    write_rule_lifecycle_action_report,
)
from ai_trading_system.run_artifacts import (
    build_run_artifact_paths,
    collect_run_files,
    default_daily_run_id,
    mirror_canonical_daily_ops_outputs_to_legacy,
    mirror_legacy_reports_to_run,
    prepare_run_directories,
    validate_legacy_output_mode,
    write_run_manifest,
)
from ai_trading_system.scoring.baseline_score_backfill import (
    BASELINE_SCORE_BACKFILL_MODE,
    run_baseline_score_backfill,
)
from ai_trading_system.scoring.daily import (
    DailyManualReviewStatus,
    DailyReviewSummary,
    build_daily_score_report,
    build_weight_calibration_context,
    default_daily_score_report_path,
    load_previous_daily_score_snapshot,
    write_daily_score_report,
    write_scores_csv,
)
from ai_trading_system.scoring.position_model import (
    ModuleScore,
    PositionBandRule,
    WeightedScoreModel,
)
from ai_trading_system.shadow_iteration import (
    DEFAULT_SHADOW_ITERATION_REGISTRY_PATH,
    DEFAULT_SHADOW_ITERATION_REPORT_DIR,
    DEFAULT_SHADOW_ITERATION_RUN_ROOT,
    build_forward_shadow_evaluation_report,
    build_shadow_iteration_report,
    register_forward_shadow_candidate,
    write_forward_shadow_evaluation_outputs,
    write_shadow_iteration_outputs,
)
from ai_trading_system.shadow_weight_profiles import (
    DEFAULT_DECISION_SNAPSHOT_SEARCH_DIR,
    DEFAULT_SHADOW_PARAMETER_OBJECTIVE_PATH,
    DEFAULT_SHADOW_PARAMETER_PROMOTION_CONTRACT_PATH,
    DEFAULT_SHADOW_PARAMETER_SEARCH_OUTPUT_ROOT,
    DEFAULT_SHADOW_PARAMETER_SEARCH_SPACE_PATH,
    DEFAULT_SHADOW_POSITION_GATE_PROFILE_MANIFEST_PATH,
    DEFAULT_SHADOW_WEIGHT_PROFILE_MANIFEST_PATH,
    DEFAULT_SHADOW_WEIGHT_PROFILE_OBSERVATION_LEDGER_PATH,
    build_shadow_parameter_promotion_report,
    build_shadow_parameter_search_report,
    build_shadow_weight_performance_report,
    build_shadow_weight_prediction_records,
    build_shadow_weight_profile_run_report,
    default_shadow_parameter_promotion_report_path,
    default_shadow_weight_performance_csv_path,
    default_shadow_weight_performance_report_path,
    default_shadow_weight_profile_report_path,
    write_shadow_parameter_promotion_report,
    write_shadow_parameter_search_bundle,
    write_shadow_weight_observation_ledger,
    write_shadow_weight_performance_csv,
    write_shadow_weight_performance_report,
    write_shadow_weight_profile_report,
)
from ai_trading_system.thesis import (
    ThesisReviewReport,
    build_thesis_review_report,
    load_trade_thesis_store,
    validate_trade_thesis_store,
)
from ai_trading_system.trade_review import (
    build_trade_review_report,
    default_trade_review_report_path,
    load_trade_record_store,
    validate_trade_record_store,
    write_trade_review_report,
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
    latest_signal_ablation_path,
    load_signal_ablation_payload,
    signal_ablation_payload_date,
    validate_signal_ablation_payload,
    write_signal_ablation_report_alias,
)
from ai_trading_system.trading_engine.signal_calibration import (
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
from ai_trading_system.valuation import (
    ValuationReviewReport,
    build_valuation_review_report,
    load_valuation_snapshot_store,
    validate_valuation_snapshot_store,
)
from ai_trading_system.watchlist_lifecycle import (
    DEFAULT_WATCHLIST_LIFECYCLE_PATH,
    default_watchlist_lifecycle_report_path,
    load_watchlist_lifecycle,
    validate_watchlist_lifecycle,
    write_watchlist_lifecycle_report,
)
from ai_trading_system.weight_calibration import (
    DEFAULT_APPROVED_CALIBRATION_OVERLAY_PATH,
    DEFAULT_CURRENT_CONTEXT_PATH,
    DEFAULT_EFFECTIVE_WEIGHTS_PATH,
    DEFAULT_WEIGHT_PROFILE_PATH,
    apply_calibration_overlays,
    load_calibration_overlays,
    load_weight_profile,
    resolve_calibration_application,
    write_calibration_context,
    write_effective_weights,
)

app = typer.Typer(help="AI 产业链趋势分析和仓位管理工具。", no_args_is_help=True)
feedback_app = typer.Typer(help="决策结果观察、校准和因果链查询。", no_args_is_help=True)
reports_app = typer.Typer(help="投资报告和周期复盘。", no_args_is_help=True)
ops_app = typer.Typer(help="运行监控和 pipeline health。", no_args_is_help=True)
score_daily_app = typer.Typer(help="每日评分和 research baseline backfill。", no_args_is_help=False)
app.add_typer(watchlist_app, name="watchlist")
app.add_typer(industry_chain_app, name="industry-chain")
app.add_typer(thesis_app, name="thesis")
app.add_typer(risk_events_app, name="risk-events")
app.add_typer(valuation_app, name="valuation")
app.add_typer(data_app, name="data")
app.add_typer(features_app, name="features")
app.add_typer(data_sources_app, name="data-sources")
app.add_typer(fundamentals_app, name="fundamentals")
app.add_typer(trace_app, name="trace")
app.add_typer(evidence_app, name="evidence")
app.add_typer(feedback_app, name="feedback")
app.add_typer(scenarios_app, name="scenarios")
app.add_typer(catalysts_app, name="catalysts")
app.add_typer(execution_app, name="execution")
app.add_typer(portfolio_app, name="portfolio")
app.add_typer(parameters_app, name="parameters")
app.add_typer(signals_app, name="signals")
app.add_typer(regime_app, name="regime")
app.add_typer(reports_app, name="reports")
app.add_typer(simulation_app, name="simulation")
app.add_typer(report_app, name="report")
app.add_typer(run_app, name="run")
app.add_typer(experiments_app, name="experiments")
app.add_typer(ops_app, name="ops")
app.add_typer(security_app, name="security")
app.add_typer(llm_app, name="llm")
app.add_typer(pit_snapshots_app, name="pit-snapshots")
app.add_typer(score_daily_app, name="score-daily")
app.add_typer(docs_app, name="docs")
app.add_typer(sec_pit_app, name="sec-pit")
app.add_typer(etf_app, name="etf")


@app.command("explain")
def explain_command(
    query: Annotated[str, typer.Argument(help="要解释的字段、gate 或 artifact 名称。")],
    kind: Annotated[
        Literal["auto", "field", "artifact", "gate"],
        typer.Option(help="解释类型；auto 会依次查询字段、gate 和 artifact。"),
    ] = "auto",
    fields_path: Annotated[
        Path,
        typer.Option(help="字段字典 YAML 路径。"),
    ] = DEFAULT_FIELDS_PATH,
    artifact_catalog_path: Annotated[
        Path,
        typer.Option(help="artifact catalog Markdown 路径。"),
    ] = DEFAULT_ARTIFACT_CATALOG_PATH,
    calculation_logic_path: Annotated[
        Path,
        typer.Option(help="计算逻辑文档路径，用于 gate 解释来源标注。"),
    ] = DEFAULT_CALCULATION_LOGIC_PATH,
) -> None:
    """只读解释字段、gate 或 artifact 来源，不运行上游、不重算投资结论。"""
    try:
        result = explain_query(
            query,
            kind=kind,
            fields_path=fields_path,
            artifact_catalog_path=artifact_catalog_path,
            calculation_logic_path=calculation_logic_path,
        )
    except (OSError, ValueError, yaml.YAMLError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    console.print(render_explain_result(result))
    if not result.get("found"):
        raise typer.Exit(code=1)


register_etf_compatibility_aliases(
    data_app=data_app,
    portfolio_app=portfolio_app,
    signals_app=signals_app,
)
console = Console()
DEFAULT_PORTFOLIO_POSITIONS_PATH = (
    PROJECT_ROOT / "data" / "external" / "portfolio_positions" / "current_positions.csv"
)


@feedback_app.command("apply-calibration-overlay")
def apply_calibration_overlay_command(
    context_path: Annotated[
        Path,
        typer.Option(help="当前 decision context JSON 路径。"),
    ] = DEFAULT_CURRENT_CONTEXT_PATH,
    weight_profile_path: Annotated[
        Path,
        typer.Option(help="当前基础权重 profile YAML 路径。"),
    ] = DEFAULT_WEIGHT_PROFILE_PATH,
    overlays_path: Annotated[
        Path,
        typer.Option(help="approved calibration overlay JSON 路径。"),
    ] = DEFAULT_APPROVED_CALIBRATION_OVERLAY_PATH,
    output_path: Annotated[
        Path,
        typer.Option(help="effective weights JSON 输出路径。"),
    ] = DEFAULT_EFFECTIVE_WEIGHTS_PATH,
    as_of: Annotated[
        str | None,
        typer.Option(help="校准匹配日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
) -> None:
    """根据 context、weight profile 和 approved overlays 计算 effective weights。"""
    calibration_date = _parse_date(as_of) if as_of else date.today()
    try:
        profile = load_weight_profile(weight_profile_path)
        overlays = load_calibration_overlays(overlays_path)
        if context_path.exists():
            context = json.loads(context_path.read_text(encoding="utf-8"))
            if not isinstance(context, dict):
                raise ValueError("context JSON must contain an object")
        elif overlays:
            raise ValueError(f"context JSON not found: {context_path}")
        else:
            context = {
                "as_of": calibration_date.isoformat(),
                "context_path": str(context_path),
                "context_status": "missing_no_approved_overlay",
            }
        application = apply_calibration_overlays(
            context=context,
            profile=profile,
            overlays=overlays,
            as_of=calibration_date,
        )
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        console.print(f"[red]历史校准 overlay 计算失败：{exc}[/red]")
        raise typer.Exit(code=1) from exc

    written_path = write_effective_weights(application, output_path)
    console.print("[green]历史校准 effective weights 已生成。[/green]")
    console.print(f"Weight profile version：{application.weight_profile_version}")
    console.print(
        "Matched overlays："
        f"{', '.join(application.matched_overlays) if application.matched_overlays else '无'}"
    )
    console.print(f"Confidence delta：{application.confidence_delta:+.2f}")
    console.print(f"Position multiplier：{application.position_multiplier:.2f}")
    console.print(f"输出：{written_path}")
    console.print("治理边界：本命令只计算校准结果，不修改 production scoring 或 position_gate。")


@feedback_app.command("validate-calibration-protocol")
def validate_calibration_protocol_command(
    manifest_path: Annotated[
        Path,
        typer.Option(help="调权实验 protocol manifest YAML 路径。"),
    ] = DEFAULT_CALIBRATION_PROTOCOL_PATH,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 校验报告输出路径。"),
    ] = None,
    as_of: Annotated[
        str | None,
        typer.Option(help="校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
) -> None:
    """校验回测调权 protocol manifest，防止无审计调参。"""
    validation_date = _parse_date(as_of) if as_of else date.today()
    try:
        manifest = load_calibration_protocol_manifest(manifest_path)
    except (OSError, ValueError, yaml.YAMLError) as exc:
        console.print(f"[red]调权协议 manifest 读取失败：{exc}[/red]")
        raise typer.Exit(code=1) from exc
    report = validate_calibration_protocol_manifest(
        manifest,
        manifest_path=manifest_path,
        as_of=validation_date,
    )
    report_output = output_path or default_calibration_protocol_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        validation_date,
    )
    report_output = write_calibration_protocol_report(report, report_output)
    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]调权协议校验状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_output}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")
    console.print("治理边界：本命令不修改 production scoring、position_gate、overlay 或回测仓位。")
    if not report.passed:
        raise typer.Exit(code=1)


@feedback_app.command("calibrate")
def calibrate_decision_outcomes(
    decision_snapshot_path: Annotated[
        Path,
        typer.Option(help="decision_snapshot JSON 文件或目录路径。"),
    ] = DEFAULT_DECISION_SNAPSHOT_DIR,
    prices_path: Annotated[
        Path,
        typer.Option(help="标准化日线价格 CSV 路径。"),
    ] = PROJECT_ROOT
    / "data"
    / "raw"
    / "prices_daily.csv",
    rates_path: Annotated[
        Path,
        typer.Option(help="标准化日线利率 CSV 路径，用于复用数据质量门禁。"),
    ] = PROJECT_ROOT
    / "data"
    / "raw"
    / "rates_daily.csv",
    as_of: Annotated[
        str | None,
        typer.Option(help="校准截止日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    horizons: Annotated[
        str,
        typer.Option(help="逗号分隔的 outcome 观察窗口，单位为交易日。"),
    ] = ",".join(str(item) for item in DEFAULT_OUTCOME_HORIZONS),
    strategy_ticker: Annotated[
        str,
        typer.Option(help="AI proxy 或策略代理标的。"),
    ] = "SMH",
    benchmarks: Annotated[
        str,
        typer.Option(help="逗号分隔的对比基准 ticker。"),
    ] = ",".join(DEFAULT_BENCHMARK_TICKERS),
    benchmark_policy_path: Annotated[
        Path,
        typer.Option(help="benchmark policy YAML 路径，用于审计 AI proxy / benchmark 解释口径。"),
    ] = DEFAULT_BENCHMARK_POLICY_CONFIG_PATH,
    benchmark_policy_report_path: Annotated[
        Path | None,
        typer.Option(help="可选：Markdown benchmark policy 校验报告输出路径。"),
    ] = None,
    outcomes_path: Annotated[
        Path,
        typer.Option(help="decision_outcomes CSV 输出路径。"),
    ] = DEFAULT_DECISION_OUTCOMES_PATH,
    report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 校准报告输出路径。"),
    ] = None,
    quality_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 数据质量报告输出路径。"),
    ] = None,
) -> None:
    """从历史 decision_snapshot 生成 outcome 和评分校准报告。"""
    calibration_date = _parse_date(as_of) if as_of else date.today()
    horizon_values = _parse_positive_int_csv(horizons, "outcome 观察窗口")
    benchmark_tickers = tuple(_parse_csv_items(benchmarks))
    if not benchmark_tickers:
        raise typer.BadParameter("至少需要一个对比基准 ticker。")
    benchmark_policy_report = validate_benchmark_policy(
        load_benchmark_policy(benchmark_policy_path),
        as_of=calibration_date,
        selected_strategy_ticker=strategy_ticker,
        selected_benchmark_tickers=benchmark_tickers,
    )
    if benchmark_policy_report_path is not None:
        write_benchmark_policy_report(benchmark_policy_report, benchmark_policy_report_path)
    if not benchmark_policy_report.passed:
        console.print("[red]基准政策校验失败，已停止决策校准。[/red]")
        console.print(
            f"错误数：{benchmark_policy_report.error_count}；"
            f"警告数：{benchmark_policy_report.warning_count}"
        )
        if benchmark_policy_report_path is not None:
            console.print(f"基准政策报告：{benchmark_policy_report_path}")
        raise typer.Exit(code=1)
    tickers = list(dict.fromkeys([strategy_ticker, *benchmark_tickers]))
    universe = load_universe()
    data_quality_config = load_data_quality()
    quality_output = quality_report_path or default_quality_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        calibration_date,
    )
    data_quality_report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=tickers,
        expected_rate_series=configured_rate_series(universe),
        quality_config=data_quality_config,
        as_of=calibration_date,
        manifest_path=_download_manifest_path(prices_path),
        secondary_prices_path=_marketstack_prices_path(prices_path),
        require_secondary_prices=_requires_marketstack_prices(prices_path),
    )
    write_data_quality_report(data_quality_report, quality_output)
    if not data_quality_report.passed:
        console.print("[red]数据质量门禁失败，已停止决策校准。[/red]")
        console.print(f"数据质量报告：{quality_output}")
        console.print(
            f"错误数：{data_quality_report.error_count}；"
            f"警告数：{data_quality_report.warning_count}"
        )
        raise typer.Exit(code=1)

    snapshots = load_decision_snapshots(decision_snapshot_path)
    if not snapshots:
        raise typer.BadParameter(f"未找到 decision_snapshot：{decision_snapshot_path}")
    prices_frame = pd.read_csv(prices_path)
    market_regimes = load_market_regimes(DEFAULT_MARKET_REGIMES_CONFIG_PATH)
    default_market_regime = market_regime_by_id(
        market_regimes,
        market_regimes.default_backtest_regime,
    )
    market_regime = BacktestRegimeContext(
        regime_id=default_market_regime.regime_id,
        name=default_market_regime.name,
        start_date=default_market_regime.start_date,
        anchor_date=default_market_regime.anchor_date,
        anchor_event=default_market_regime.anchor_event,
        description=default_market_regime.description,
    )
    result = build_decision_outcomes(
        snapshots=snapshots,
        prices=prices_frame,
        as_of=calibration_date,
        horizons=tuple(horizon_values),
        strategy_ticker=strategy_ticker,
        benchmark_tickers=benchmark_tickers,
        market_regime=market_regime,
        data_quality_report=data_quality_report,
        benchmark_policy_report=benchmark_policy_report,
    )
    outcomes_output = write_decision_outcomes_csv(result, outcomes_path)
    calibration_report_output = report_path or default_decision_calibration_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        calibration_date,
    )
    calibration_report_output = write_decision_calibration_report(
        result,
        outcomes_path=outcomes_output,
        data_quality_report_path=quality_output,
        output_path=calibration_report_output,
    )

    sample_policy = load_feedback_sample_policy()
    decision_diagnostic_floor = sample_policy.decision_outcomes.diagnostic_floor
    status_style = "green" if len(result.available_rows) >= decision_diagnostic_floor else "yellow"
    console.print(
        f"[{status_style}]决策校准完成。可用 outcome："
        f"{len(result.available_rows)}[/{status_style}]"
    )
    console.print(f"校准报告：{calibration_report_output}")
    console.print(f"Outcome CSV：{outcomes_output}")
    console.print(f"数据质量报告：{quality_output}（{data_quality_report.status}）")
    console.print(f"基准政策状态：{benchmark_policy_report.status}")
    if benchmark_policy_report_path is not None:
        console.print(f"基准政策报告：{benchmark_policy_report_path}")


@feedback_app.command("calibrate-predictions")
def calibrate_prediction_outcomes(
    prediction_ledger_path: Annotated[
        Path,
        typer.Option(help="prediction/shadow ledger CSV 路径。"),
    ] = DEFAULT_PREDICTION_LEDGER_PATH,
    prices_path: Annotated[
        Path,
        typer.Option(help="标准化日线价格 CSV 路径。"),
    ] = PROJECT_ROOT
    / "data"
    / "raw"
    / "prices_daily.csv",
    rates_path: Annotated[
        Path,
        typer.Option(help="标准化日线利率 CSV 路径，用于复用数据质量门禁。"),
    ] = PROJECT_ROOT
    / "data"
    / "raw"
    / "rates_daily.csv",
    as_of: Annotated[
        str | None,
        typer.Option(help="校准截止日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    horizons: Annotated[
        str,
        typer.Option(help="逗号分隔的 prediction outcome 观察窗口，单位为交易日。"),
    ] = ",".join(str(item) for item in DEFAULT_OUTCOME_HORIZONS),
    strategy_ticker: Annotated[
        str,
        typer.Option(help="AI proxy 或策略代理标的。"),
    ] = "SMH",
    benchmarks: Annotated[
        str,
        typer.Option(help="逗号分隔的对比基准 ticker。"),
    ] = ",".join(DEFAULT_BENCHMARK_TICKERS),
    outcomes_path: Annotated[
        Path,
        typer.Option(help="prediction_outcomes CSV 输出路径。"),
    ] = DEFAULT_PREDICTION_OUTCOMES_PATH,
    report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown prediction outcome 报告输出路径。"),
    ] = None,
    quality_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 数据质量报告输出路径。"),
    ] = None,
) -> None:
    """从 prediction/shadow ledger 生成前向 outcome 和模型版本分桶报告。"""
    calibration_date = _parse_date(as_of) if as_of else date.today()
    horizon_values = _parse_positive_int_csv(horizons, "prediction outcome 观察窗口")
    benchmark_tickers = tuple(_parse_csv_items(benchmarks))
    if not benchmark_tickers:
        raise typer.BadParameter("至少需要一个对比基准 ticker。")
    prediction_rows = load_prediction_ledger(prediction_ledger_path)
    if not prediction_rows:
        raise typer.BadParameter(f"未找到 prediction ledger：{prediction_ledger_path}")
    tickers = list(dict.fromkeys([strategy_ticker, *benchmark_tickers]))
    universe = load_universe()
    data_quality_config = load_data_quality()
    quality_output = quality_report_path or default_quality_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        calibration_date,
    )
    data_quality_report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=tickers,
        expected_rate_series=configured_rate_series(universe),
        quality_config=data_quality_config,
        as_of=calibration_date,
        manifest_path=_download_manifest_path(prices_path),
        secondary_prices_path=_marketstack_prices_path(prices_path),
        require_secondary_prices=_requires_marketstack_prices(prices_path),
    )
    write_data_quality_report(data_quality_report, quality_output)
    if not data_quality_report.passed:
        console.print("[red]数据质量门禁失败，已停止 prediction outcome 校准。[/red]")
        console.print(f"数据质量报告：{quality_output}")
        console.print(
            f"错误数：{data_quality_report.error_count}；"
            f"警告数：{data_quality_report.warning_count}"
        )
        raise typer.Exit(code=1)

    market_regimes = load_market_regimes(DEFAULT_MARKET_REGIMES_CONFIG_PATH)
    default_market_regime = market_regime_by_id(
        market_regimes,
        market_regimes.default_backtest_regime,
    )
    market_regime = BacktestRegimeContext(
        regime_id=default_market_regime.regime_id,
        name=default_market_regime.name,
        start_date=default_market_regime.start_date,
        anchor_date=default_market_regime.anchor_date,
        anchor_event=default_market_regime.anchor_event,
        description=default_market_regime.description,
    )
    result = build_prediction_outcomes(
        prediction_rows=prediction_rows,
        prices=pd.read_csv(prices_path),
        as_of=calibration_date,
        horizons=tuple(horizon_values),
        strategy_ticker=strategy_ticker,
        benchmark_tickers=benchmark_tickers,
        market_regime=market_regime,
        data_quality_report=data_quality_report,
    )
    outcomes_output = write_prediction_outcomes_csv(result, outcomes_path)
    report_output = report_path or default_prediction_outcome_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        calibration_date,
    )
    report_output = write_prediction_outcome_report(
        result,
        outcomes_path=outcomes_output,
        data_quality_report_path=quality_output,
        output_path=report_output,
    )

    sample_policy = load_feedback_sample_policy()
    prediction_diagnostic_floor = sample_policy.prediction_outcomes.diagnostic_floor
    status_style = (
        "green" if len(result.available_rows) >= prediction_diagnostic_floor else "yellow"
    )
    console.print(
        f"[{status_style}]Prediction outcome 校准完成。可用 outcome："
        f"{len(result.available_rows)}[/{status_style}]"
    )
    console.print(f"Prediction outcome 报告：{report_output}")
    console.print(f"Prediction outcome CSV：{outcomes_output}")
    console.print(f"数据质量报告：{quality_output}（{data_quality_report.status}）")


@feedback_app.command("run-shadow")
def run_shadow_predictions_command(
    rule_experiment_path: Annotated[
        Path,
        typer.Option(help="rule experiment ledger JSON 路径。"),
    ] = DEFAULT_RULE_EXPERIMENT_LEDGER_PATH,
    decision_snapshot_path: Annotated[
        Path | None,
        typer.Option(help="production decision_snapshot JSON 路径；不传时按 as-of 推导。"),
    ] = None,
    trace_bundle_path: Annotated[
        Path | None,
        typer.Option(help="production trace bundle JSON 路径；不传时从 snapshot.trace 推导。"),
    ] = None,
    features_path: Annotated[
        Path | None,
        typer.Option(help="特征快照 CSV 路径；不传时从 trace dataset_refs 推导。"),
    ] = None,
    data_quality_report_path: Annotated[
        Path | None,
        typer.Option(help="数据质量报告路径；不传时从 trace quality_refs 推导。"),
    ] = None,
    prediction_ledger_path: Annotated[
        Path,
        typer.Option(help="append-only prediction ledger CSV 输出路径。"),
    ] = DEFAULT_PREDICTION_LEDGER_PATH,
    candidate_ids: Annotated[
        str | None,
        typer.Option(help="可选：逗号分隔的 candidate_id 白名单。"),
    ] = None,
    as_of: Annotated[
        str | None,
        typer.Option(help="shadow 运行日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown shadow runner 报告输出路径。"),
    ] = None,
) -> None:
    """把 rule experiment challenger 追加到 prediction ledger，production_effect=none。"""
    run_date = _parse_date(as_of) if as_of else date.today()
    snapshot_path = decision_snapshot_path or default_decision_snapshot_path(
        DEFAULT_DECISION_SNAPSHOT_DIR,
        run_date,
    )
    if not snapshot_path.exists():
        raise typer.BadParameter(f"decision_snapshot 不存在：{snapshot_path}")
    snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
    trace_path = trace_bundle_path or _path_from_snapshot_trace(snapshot)
    if trace_path is None or not trace_path.exists():
        raise typer.BadParameter(f"trace bundle 不存在：{trace_path}")
    trace_bundle = json.loads(trace_path.read_text(encoding="utf-8"))
    feature_snapshot_path = features_path or _trace_dataset_path(
        trace_bundle,
        "processed_feature_cache",
    )
    if feature_snapshot_path is None:
        raise typer.BadParameter("无法从 trace bundle 推导 feature snapshot 路径。")
    quality_path = data_quality_report_path or _trace_quality_report_path(trace_bundle)
    if quality_path is None:
        raise typer.BadParameter("无法从 trace bundle 推导 data quality report 路径。")
    try:
        rule_experiment_ledger = load_rule_experiment_ledger(rule_experiment_path)
    except FileNotFoundError as exc:
        raise typer.BadParameter(f"rule experiment ledger 不存在：{rule_experiment_path}") from exc
    selected = tuple(_parse_csv_items(candidate_ids)) if candidate_ids else ()
    records = build_shadow_prediction_records(
        snapshot=snapshot,
        trace_bundle=trace_bundle,
        trace_bundle_path=trace_path,
        features_path=feature_snapshot_path,
        data_quality_report_path=quality_path,
        rule_experiment_ledger=rule_experiment_ledger,
        as_of=run_date,
        selected_candidate_ids=selected,
    )
    ledger_output = append_prediction_records(records, prediction_ledger_path)
    report = build_shadow_prediction_run_report(
        as_of=run_date,
        decision_snapshot_path=snapshot_path,
        trace_bundle_path=trace_path,
        rule_experiment_path=rule_experiment_path,
        prediction_ledger_path=ledger_output,
        records=records,
        candidate_count=len(rule_experiment_ledger.get("candidates", [])),
        warnings=(
            ("没有可运行的 challenger；请检查 forward_shadow_plan 状态和日期窗口。",)
            if not records
            else ()
        ),
    )
    output_report = report_path or default_shadow_prediction_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        run_date,
    )
    output_report = write_shadow_prediction_run_report(report, output_report)
    style = "green" if report.status == "PASS" else "yellow"
    console.print(f"[{style}]Shadow runner 状态：{report.status}[/{style}]")
    console.print(f"写入 prediction：{report.appended_count}")
    console.print(f"Prediction ledger：{ledger_output}")
    console.print(f"Shadow runner 报告：{output_report}")


@feedback_app.command("run-parameter-shadow")
def run_parameter_shadow_predictions_command(
    parameter_candidate_ledger_path: Annotated[
        Path,
        typer.Option(help="parameter candidates ledger JSON 路径。"),
    ] = DEFAULT_PARAMETER_CANDIDATE_LEDGER_PATH,
    decision_snapshot_path: Annotated[
        Path | None,
        typer.Option(help="production decision_snapshot JSON 路径；不传时按 as-of 推导。"),
    ] = None,
    trace_bundle_path: Annotated[
        Path | None,
        typer.Option(help="production trace bundle JSON 路径；不传时从 snapshot.trace 推导。"),
    ] = None,
    features_path: Annotated[
        Path | None,
        typer.Option(help="特征快照 CSV 路径；不传时从 trace dataset_refs 推导。"),
    ] = None,
    data_quality_report_path: Annotated[
        Path | None,
        typer.Option(help="数据质量报告路径；不传时从 trace quality_refs 推导。"),
    ] = None,
    prediction_ledger_path: Annotated[
        Path | None,
        typer.Option(
            help=(
                "append-only parameter shadow prediction ledger CSV 输出路径；"
                "默认写入隔离 flow validation ledger，不写正式 production ledger。"
            )
        ),
    ] = None,
    candidate_ids: Annotated[
        str | None,
        typer.Option(help="可选：逗号分隔的 candidate_id 白名单。"),
    ] = None,
    as_of: Annotated[
        str | None,
        typer.Option(help="shadow 运行日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown parameter shadow runner 报告输出路径。"),
    ] = None,
) -> None:
    """把参数候选追加到 prediction ledger，production_effect=none。"""
    run_date = _parse_date(as_of) if as_of else date.today()
    snapshot_path = decision_snapshot_path or default_decision_snapshot_path(
        DEFAULT_DECISION_SNAPSHOT_DIR,
        run_date,
    )
    if not snapshot_path.exists():
        raise typer.BadParameter(f"decision_snapshot 不存在：{snapshot_path}")
    snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
    trace_path = trace_bundle_path or _path_from_snapshot_trace(snapshot)
    if trace_path is None or not trace_path.exists():
        raise typer.BadParameter(f"trace bundle 不存在：{trace_path}")
    trace_bundle = json.loads(trace_path.read_text(encoding="utf-8"))
    feature_snapshot_path = features_path or _trace_dataset_path(
        trace_bundle,
        "processed_feature_cache",
    )
    if feature_snapshot_path is None:
        raise typer.BadParameter("无法从 trace bundle 推导 feature snapshot 路径。")
    quality_path = data_quality_report_path or _trace_quality_report_path(trace_bundle)
    if quality_path is None:
        raise typer.BadParameter("无法从 trace bundle 推导 data quality report 路径。")
    try:
        parameter_candidate_ledger = load_parameter_candidate_ledger(
            parameter_candidate_ledger_path
        )
    except FileNotFoundError as exc:
        raise typer.BadParameter(
            f"parameter candidate ledger 不存在：{parameter_candidate_ledger_path}"
        ) from exc
    selected = tuple(_parse_csv_items(candidate_ids)) if candidate_ids else ()
    records = build_parameter_shadow_prediction_records(
        snapshot=snapshot,
        trace_bundle=trace_bundle,
        trace_bundle_path=trace_path,
        features_path=feature_snapshot_path,
        data_quality_report_path=quality_path,
        parameter_candidate_ledger=parameter_candidate_ledger,
        selected_candidate_ids=selected,
    )
    parameter_shadow_ledger_path = (
        prediction_ledger_path or DEFAULT_PARAMETER_SHADOW_PREDICTION_LEDGER_PATH
    )
    ledger_output = append_prediction_records(records, parameter_shadow_ledger_path)
    warnings: list[str] = []
    if not records:
        warnings.append(
            "没有可运行的 parameter challenger；请检查 recommendation_status 和门禁模式。"
        )
    if parameter_candidate_ledger.get("evaluation_mode") == "flow_validation":
        warnings.append(
            "本次 parameter shadow 来自 flow_validation candidate ledger；"
            "仅用于接线验证，不得作为生产证据。"
        )
    report = build_shadow_prediction_run_report(
        as_of=run_date,
        decision_snapshot_path=snapshot_path,
        trace_bundle_path=trace_path,
        rule_experiment_path=parameter_candidate_ledger_path,
        prediction_ledger_path=ledger_output,
        records=records,
        candidate_count=len(parameter_candidate_ledger.get("candidates", [])),
        warnings=tuple(warnings),
        source_label="Parameter candidate ledger",
    )
    output_report = report_path or (
        PROJECT_ROOT
        / "outputs"
        / "reports"
        / f"parameter_shadow_predictions_{run_date.isoformat()}.md"
    )
    output_report = write_shadow_prediction_run_report(report, output_report)
    style = "green" if report.status == "PASS" else "yellow"
    console.print(f"[{style}]Parameter shadow runner 状态：{report.status}[/{style}]")
    console.print(f"写入 prediction：{report.appended_count}")
    console.print(f"Prediction ledger：{ledger_output}")
    console.print(f"Parameter shadow runner 报告：{output_report}")


@feedback_app.command("run-shadow-weight-profiles")
def run_shadow_weight_profiles_command(
    manifest_path: Annotated[
        Path,
        typer.Option(help="shadow weight profile manifest YAML 路径。"),
    ] = DEFAULT_SHADOW_WEIGHT_PROFILE_MANIFEST_PATH,
    gate_manifest_path: Annotated[
        Path | None,
        typer.Option(
            help=(
                "可选：shadow position gate profile manifest YAML 路径；"
                "提供后与 weight profile 组合观察 gate cap 参数。"
            )
        ),
    ] = DEFAULT_SHADOW_POSITION_GATE_PROFILE_MANIFEST_PATH,
    decision_snapshot_path: Annotated[
        Path | None,
        typer.Option(help="production decision_snapshot JSON 路径；不传时按 as-of 推导。"),
    ] = None,
    trace_bundle_path: Annotated[
        Path | None,
        typer.Option(help="可选：写 prediction ledger 时使用的 trace bundle JSON 路径。"),
    ] = None,
    features_path: Annotated[
        Path | None,
        typer.Option(help="可选：写 prediction ledger 时使用的 feature snapshot CSV 路径。"),
    ] = None,
    data_quality_report_path: Annotated[
        Path | None,
        typer.Option(help="可选：写 prediction ledger 时使用的数据质量报告路径。"),
    ] = None,
    observation_ledger_path: Annotated[
        Path,
        typer.Option(help="独立 shadow weight observation ledger CSV 输出路径。"),
    ] = DEFAULT_SHADOW_WEIGHT_PROFILE_OBSERVATION_LEDGER_PATH,
    prediction_ledger_path: Annotated[
        Path | None,
        typer.Option(
            help=(
                "可选：隔离 prediction ledger CSV 路径；提供后才追加 "
                "production_effect=none 的 shadow prediction。"
            )
        ),
    ] = None,
    as_of: Annotated[
        str | None,
        typer.Option(help="观察日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown shadow weight profile 报告输出路径。"),
    ] = None,
) -> None:
    """运行隔离 shadow weight profile 与主线评分对比，production_effect=none。"""
    run_date = _parse_date(as_of) if as_of else date.today()
    snapshot_path = decision_snapshot_path or default_decision_snapshot_path(
        DEFAULT_DECISION_SNAPSHOT_DIR,
        run_date,
    )
    if not snapshot_path.exists():
        raise typer.BadParameter(f"decision_snapshot 不存在：{snapshot_path}")
    try:
        report = build_shadow_weight_profile_run_report(
            as_of=run_date,
            decision_snapshot_path=snapshot_path,
            manifest_path=manifest_path,
            gate_manifest_path=gate_manifest_path,
            observation_ledger_path=observation_ledger_path,
            prediction_ledger_path=prediction_ledger_path,
        )
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        console.print(f"[red]Shadow weight profile 运行失败：{exc}[/red]")
        raise typer.Exit(code=1) from exc
    observation_output = write_shadow_weight_observation_ledger(
        report,
        observation_ledger_path,
    )
    appended_predictions = 0
    prediction_output: Path | None = None
    if prediction_ledger_path is not None:
        snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
        trace_path = trace_bundle_path or _path_from_snapshot_trace(snapshot)
        if trace_path is None or not trace_path.exists():
            raise typer.BadParameter(f"trace bundle 不存在：{trace_path}")
        trace_bundle = json.loads(trace_path.read_text(encoding="utf-8"))
        feature_snapshot_path = features_path or _trace_dataset_path(
            trace_bundle,
            "processed_feature_cache",
        )
        if feature_snapshot_path is None:
            raise typer.BadParameter("无法从 trace bundle 推导 feature snapshot 路径。")
        quality_path = data_quality_report_path or _trace_quality_report_path(trace_bundle)
        if quality_path is None:
            raise typer.BadParameter("无法从 trace bundle 推导 data quality report 路径。")
        records = build_shadow_weight_prediction_records(
            report,
            snapshot=snapshot,
            trace_bundle=trace_bundle,
            trace_bundle_path=trace_path,
            features_path=feature_snapshot_path,
            data_quality_report_path=quality_path,
        )
        existing_prediction_ids = {
            str(row.get("prediction_id") or "")
            for row in load_prediction_ledger(prediction_ledger_path)
        }
        new_records = tuple(
            record
            for record in records
            if str(record.get("prediction_id") or "") not in existing_prediction_ids
        )
        prediction_output = append_prediction_records(new_records, prediction_ledger_path)
        appended_predictions = len(new_records)
    output_report = report_path or default_shadow_weight_profile_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        run_date,
    )
    output_report = write_shadow_weight_profile_report(report, output_report)
    style = "green" if report.status == "PASS" else "yellow"
    console.print(f"[{style}]Shadow weight profile 状态：{report.status}[/{style}]")
    console.print(f"Profile 数：{len(report.observations)}")
    console.print(f"Observation ledger：{observation_output}")
    if prediction_output is not None:
        console.print(f"Shadow prediction ledger：{prediction_output}")
        console.print(f"写入 shadow prediction：{appended_predictions}")
    console.print(f"报告：{output_report}")
    console.print("治理边界：本命令不修改生产权重、approved overlay、日报结论或仓位 gate。")


@feedback_app.command("evaluate-shadow-weight-performance")
def evaluate_shadow_weight_performance_command(
    observation_ledger_path: Annotated[
        Path,
        typer.Option(help="shadow weight observation ledger CSV 路径。"),
    ] = DEFAULT_SHADOW_WEIGHT_PROFILE_OBSERVATION_LEDGER_PATH,
    prices_path: Annotated[
        Path,
        typer.Option(help="标准化日线价格 CSV 路径。"),
    ] = PROJECT_ROOT
    / "data"
    / "raw"
    / "prices_daily.csv",
    as_of: Annotated[
        str | None,
        typer.Option(help="评估截止日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    since: Annotated[
        str | None,
        typer.Option(help="评估起始日期，格式为 YYYY-MM-DD；默认读取全部 observation。"),
    ] = None,
    strategy_ticker: Annotated[
        str,
        typer.Option(help="AI proxy 或策略代理标的。"),
    ] = "SMH",
    horizon_days: Annotated[
        int,
        typer.Option(help="非重叠表现评估观察窗口，单位为交易日。"),
    ] = 1,
    cost_bps: Annotated[
        float,
        typer.Option(help="单边交易成本 bps，用于 position-weighted 验证。"),
    ] = 5.0,
    slippage_bps: Annotated[
        float,
        typer.Option(help="线性滑点 bps，用于 position-weighted 验证。"),
    ] = 0.0,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown shadow weight performance 报告输出路径。"),
    ] = None,
    csv_output_path: Annotated[
        Path | None,
        typer.Option(help="机器可读 shadow weight performance CSV 输出路径。"),
    ] = None,
) -> None:
    """比较 shadow 权重与主线 gate 后仓位的 position-weighted 表现。"""
    evaluation_date = _parse_date(as_of) if as_of else date.today()
    since_date = _parse_date(since) if since else None
    try:
        report = build_shadow_weight_performance_report(
            as_of=evaluation_date,
            since=since_date,
            observation_ledger_path=observation_ledger_path,
            prices_path=prices_path,
            strategy_ticker=strategy_ticker,
            horizon_days=horizon_days,
            cost_bps=cost_bps,
            slippage_bps=slippage_bps,
        )
    except (OSError, ValueError) as exc:
        console.print(f"[red]Shadow weight performance 评估失败：{exc}[/red]")
        raise typer.Exit(code=1) from exc
    csv_path = csv_output_path or default_shadow_weight_performance_csv_path(
        PROJECT_ROOT / "outputs" / "reports",
        evaluation_date,
    )
    csv_path = write_shadow_weight_performance_csv(report, csv_path)
    report_path = output_path or default_shadow_weight_performance_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        evaluation_date,
    )
    report_path = write_shadow_weight_performance_report(
        report,
        report_path,
        csv_path=csv_path,
    )
    style = "green" if report.status == "PASS" else "yellow"
    console.print(f"[{style}]Shadow weight performance 状态：{report.status}[/{style}]")
    if report.best_positive_profile is not None:
        best = report.best_positive_profile
        console.print(
            "Return-leading profile：" f"{best.profile_id}（excess={best.excess_total_return:.2%}）"
        )
    elif report.best_profile is not None:
        console.print("当前没有正向 excess 的 shadow weight profile。")
    console.print(f"Performance CSV：{csv_path}")
    console.print(f"Performance 报告：{report_path}")
    console.print("治理边界：本命令不修改生产权重、approved overlay、日报结论或仓位 gate。")


@feedback_app.command("search-shadow-parameters")
def search_shadow_parameters_command(
    from_date: Annotated[
        str,
        typer.Option("--from", help="搜索起始日期，格式为 YYYY-MM-DD。"),
    ],
    to_date: Annotated[
        str,
        typer.Option("--to", help="搜索截止日期，格式为 YYYY-MM-DD。"),
    ],
    decision_snapshot_path: Annotated[
        Path,
        typer.Option(help="decision_snapshot JSON 文件或目录路径。"),
    ] = DEFAULT_DECISION_SNAPSHOT_SEARCH_DIR,
    prices_path: Annotated[
        Path,
        typer.Option(help="标准化日线价格 CSV 路径。"),
    ] = PROJECT_ROOT
    / "data"
    / "raw"
    / "prices_daily.csv",
    search_space_path: Annotated[
        Path,
        typer.Option(help="shadow 参数搜索空间 YAML 路径。"),
    ] = DEFAULT_SHADOW_PARAMETER_SEARCH_SPACE_PATH,
    objective_path: Annotated[
        Path,
        typer.Option(help="shadow 参数目标函数 YAML 路径。"),
    ] = DEFAULT_SHADOW_PARAMETER_OBJECTIVE_PATH,
    output_root: Annotated[
        Path,
        typer.Option(help="参数搜索输出根目录。"),
    ] = DEFAULT_SHADOW_PARAMETER_SEARCH_OUTPUT_ROOT,
    run_id: Annotated[
        str | None,
        typer.Option(help="搜索 run id；不传则按日期和 UTC 时间生成。"),
    ] = None,
    strategy_ticker: Annotated[
        str,
        typer.Option(help="AI proxy 或策略代理标的。"),
    ] = "SMH",
    horizon_days: Annotated[
        int,
        typer.Option(help="表现评估观察窗口，单位为交易日。"),
    ] = 1,
    cost_bps: Annotated[
        float,
        typer.Option(help="单边交易成本 bps。"),
    ] = 5.0,
    slippage_bps: Annotated[
        float,
        typer.Option(help="线性滑点 bps。"),
    ] = 0.0,
    max_trials: Annotated[
        int | None,
        typer.Option(help="可选：最多评估 trial 数，用于快速 smoke。"),
    ] = None,
) -> None:
    """按指定区间搜索 validation-only shadow weight/gate 参数候选。"""
    start = _parse_date(from_date)
    end = _parse_date(to_date)
    effective_run_id = run_id or (
        f"shadow_parameter_search_{start.isoformat()}_{end.isoformat()}_"
        f"{datetime.now(tz=UTC).strftime('%Y%m%dT%H%M%SZ')}"
    )
    output_dir = output_root / effective_run_id
    try:
        report = build_shadow_parameter_search_report(
            run_id=effective_run_id,
            start=start,
            end=end,
            decision_snapshot_path=decision_snapshot_path,
            prices_path=prices_path,
            search_space_path=search_space_path,
            objective_path=objective_path,
            output_dir=output_dir,
            strategy_ticker=strategy_ticker,
            horizon_days=horizon_days,
            cost_bps=cost_bps,
            slippage_bps=slippage_bps,
            max_trials=max_trials,
        )
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        console.print(f"[red]Shadow parameter search 失败：{exc}[/red]")
        raise typer.Exit(code=1) from exc
    paths = write_shadow_parameter_search_bundle(report)
    style = "green" if report.status == "PASS" else "yellow"
    console.print(f"[{style}]Shadow parameter search 状态：{report.status}[/{style}]")
    console.print(f"Trial 数：{len(report.trials)}")
    console.print(f"Pareto front：{len(report.pareto_front)}")
    if report.best_trial is not None:
        best = report.best_trial
        console.print(
            "Best trial："
            f"{best.trial_id}（objective={best.objective_score:.4f}, "
            f"excess={best.excess_total_return:.2%}）"
        )
    else:
        console.print("当前没有 eligible best trial。")
        if report.best_diagnostic_trial is not None:
            diagnostic = report.best_diagnostic_trial
            console.print(
                "Diagnostic-leading trial："
                f"{diagnostic.trial_id}（not eligible: "
                f"{diagnostic.ineligibility_reason or 'not_eligible'}）"
            )
    if report.factorial_attribution is not None:
        console.print("Factorial primary driver：" f"{report.factorial_attribution.primary_driver}")
    console.print(f"输出目录：{paths['output_dir']}")
    console.print(f"搜索报告：{paths['search_report']}")
    console.print(f"Best profile YAML：{paths['best_profiles_yaml']}")
    console.print("治理边界：本命令不修改生产权重、approved overlay、日报结论或仓位 gate。")


@feedback_app.command("evaluate-shadow-parameter-promotion")
def evaluate_shadow_parameter_promotion_command(
    search_output_dir: Annotated[
        Path,
        typer.Option(help="search-shadow-parameters 输出目录，包含 manifest.json 和 trials.csv。"),
    ],
    contract_path: Annotated[
        Path,
        typer.Option(help="shadow parameter promotion contract YAML 路径。"),
    ] = DEFAULT_SHADOW_PARAMETER_PROMOTION_CONTRACT_PATH,
    output_path: Annotated[
        Path | None,
        typer.Option(help="promotion contract 报告输出路径；不传则写入 search output 目录。"),
    ] = None,
) -> None:
    """用独立 contract 检查 shadow parameter search 是否只能进入后续 shadow。"""
    try:
        report = build_shadow_parameter_promotion_report(
            search_output_dir=search_output_dir,
            contract_path=contract_path,
        )
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        console.print(f"[red]Shadow parameter promotion 评估失败：{exc}[/red]")
        raise typer.Exit(code=1) from exc
    run_id = str(report.search_manifest.get("run_id") or search_output_dir.name)
    destination = output_path or default_shadow_parameter_promotion_report_path(
        search_output_dir,
        run_id,
    )
    written = write_shadow_parameter_promotion_report(report, destination)
    style = "green" if report.status == "READY_FOR_OWNER_REVIEW" else "yellow"
    console.print(f"[{style}]Shadow parameter promotion 状态：{report.status}[/{style}]")
    console.print(f"Selected trial：{report.selected_trial_id or 'none'}")
    console.print(f"Contract checks：{len(report.checks)}")
    console.print(f"Promotion 报告：{written}")
    console.print(f"Promotion JSON：{written.with_suffix('.json')}")
    console.print("治理边界：本命令只评估 contract，不修改 production 参数或 ledger。")


@feedback_app.command("run-shadow-iteration")
def run_shadow_iteration_command(
    as_of: Annotated[
        str,
        typer.Option(help="shadow iteration 运行日期，格式为 YYYY-MM-DD。"),
    ],
    search_output_dir: Annotated[
        Path | None,
        typer.Option(help="可选：search-shadow-parameters 输出目录。"),
    ] = None,
    registry_path: Annotated[
        Path,
        typer.Option(
            hidden=True,
            help="测试/隔离用：shadow iteration registry CSV 输出路径。",
        ),
    ] = DEFAULT_SHADOW_ITERATION_REGISTRY_PATH,
    reports_dir: Annotated[
        Path,
        typer.Option(hidden=True, help="测试/隔离用：shadow iteration 报告目录。"),
    ] = DEFAULT_SHADOW_ITERATION_REPORT_DIR,
    run_output_root: Annotated[
        Path,
        typer.Option(hidden=True, help="测试/隔离用：shadow iteration run 输出根目录。"),
    ] = DEFAULT_SHADOW_ITERATION_RUN_ROOT,
    contract_path: Annotated[
        Path,
        typer.Option(
            hidden=True,
            help="测试/隔离用：shadow parameter promotion contract YAML 路径。",
        ),
    ] = DEFAULT_SHADOW_PARAMETER_PROMOTION_CONTRACT_PATH,
) -> None:
    """读取既有 search output，维护 shadow iteration registry 和只读报告。"""
    run_date = _parse_date(as_of)
    try:
        report = build_shadow_iteration_report(
            as_of=run_date,
            search_output_dir=search_output_dir,
            registry_path=registry_path,
            reports_dir=reports_dir,
            run_output_root=run_output_root,
            contract_path=contract_path,
        )
        paths = write_shadow_iteration_outputs(report)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        console.print(f"[red]Shadow iteration 运行失败：{exc}[/red]")
        raise typer.Exit(code=1) from exc
    style = "green" if report.status == "PASS" else "yellow"
    console.print(f"[{style}]Shadow iteration 状态：{report.status}[/{style}]")
    console.print(f"Source search run：{report.source_search_run_id}")
    console.print(f"Active candidates：{len(report.active_candidates)}")
    if report.best_weight_only is not None:
        console.print(f"Best weight-only：{report.best_weight_only.trial_id}")
    if report.best_gate_only is not None:
        console.print(f"Best gate-only：{report.best_gate_only.trial_id}")
    if report.best_weight_gate_bundle is not None:
        console.print(f"Best bundle：{report.best_weight_gate_bundle.trial_id}")
    console.print(f"Registry：{paths['registry']}")
    console.print(f"报告：{paths['markdown_report']}")
    console.print(f"JSON：{paths['json_report']}")
    console.print("治理边界：本命令不修改 production 权重、gate、approved overlay 或正式 ledger。")


@feedback_app.command("register-forward-shadow")
def register_forward_shadow_command(
    iteration_id: Annotated[
        str,
        typer.Option(help="shadow iteration registry 中的 iteration_id。"),
    ],
    candidate_id: Annotated[
        str,
        typer.Option(help="registry trial_id；也可传同一个 iteration_id 作一致性校验。"),
    ],
    as_of: Annotated[
        str | None,
        typer.Option(help="登记日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    registry_path: Annotated[
        Path,
        typer.Option(
            hidden=True,
            help="测试/隔离用：shadow iteration registry CSV 路径。",
        ),
    ] = DEFAULT_SHADOW_ITERATION_REGISTRY_PATH,
) -> None:
    """仅把 shadow iteration registry 中的候选标记为持续 forward shadow。"""
    run_date = _parse_date(as_of) if as_of else date.today()
    try:
        row = register_forward_shadow_candidate(
            registry_path=registry_path,
            iteration_id=iteration_id,
            candidate_id=candidate_id,
            as_of=run_date,
        )
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        console.print(f"[red]Forward shadow 登记失败：{exc}[/red]")
        raise typer.Exit(code=1) from exc
    console.print("[green]Forward shadow 登记完成[/green]")
    console.print(f"Iteration：{row.get('iteration_id')}")
    console.print(f"Trial：{row.get('trial_id')}")
    console.print(f"Status：{row.get('status')}")
    console.print(f"Registry：{registry_path}")
    console.print("治理边界：本命令只更新 shadow_iteration_registry.csv，不修改 production 配置。")


@feedback_app.command("evaluate-forward-shadow")
def evaluate_forward_shadow_command(
    as_of: Annotated[
        str,
        typer.Option(help="forward shadow 评估日期，格式为 YYYY-MM-DD。"),
    ],
    registry_path: Annotated[
        Path,
        typer.Option(
            hidden=True,
            help="测试/隔离用：shadow iteration registry CSV 路径。",
        ),
    ] = DEFAULT_SHADOW_ITERATION_REGISTRY_PATH,
    reports_dir: Annotated[
        Path,
        typer.Option(hidden=True, help="测试/隔离用：forward shadow 评估报告目录。"),
    ] = DEFAULT_SHADOW_ITERATION_REPORT_DIR,
) -> None:
    """每日评估 active forward shadow 候选并更新只读 lifecycle 状态。"""
    run_date = _parse_date(as_of)
    try:
        report = build_forward_shadow_evaluation_report(
            as_of=run_date,
            registry_path=registry_path,
            reports_dir=reports_dir,
        )
        paths = write_forward_shadow_evaluation_outputs(report)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        console.print(f"[red]Forward shadow 评估失败：{exc}[/red]")
        raise typer.Exit(code=1) from exc
    style = "green" if report.status == "PASS" else "yellow"
    console.print(f"[{style}]Forward shadow 评估状态：{report.status}[/{style}]")
    console.print(f"Evaluated candidates：{len(report.evaluations)}")
    console.print(
        "Actions："
        + ", ".join(f"{action}={count}" for action, count in report.action_counts.items())
    )
    console.print(f"Registry：{paths['registry']}")
    console.print(f"报告：{paths['markdown_report']}")
    console.print(f"JSON：{paths['json_report']}")
    console.print("治理边界：本命令不修改 production 权重、gate、approved overlay 或正式 ledger。")


@feedback_app.command("shadow-maturity")
def shadow_maturity_command(
    prediction_outcomes_path: Annotated[
        Path,
        typer.Option(help="prediction_outcomes CSV 路径。"),
    ] = DEFAULT_PREDICTION_OUTCOMES_PATH,
    as_of: Annotated[
        str | None,
        typer.Option(help="成熟度评估日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    min_available_samples: Annotated[
        int | None,
        typer.Option(
            help=(
                "进入 owner/rule card 审批前所需最低可用 outcome 样本数；"
                "promotion mode 默认读取 prediction promotion floor；"
                "validation mode 默认读取 prediction pilot floor。"
            )
        ),
    ] = None,
    review_mode: Annotated[
        str,
        typer.Option(
            help=(
                "成熟度复核模式：promotion 使用生产晋级门槛；validation 只启动"
                "后续验证复核，不允许 production 晋级。"
            )
        ),
    ] = "promotion",
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown shadow maturity 报告输出路径。"),
    ] = None,
) -> None:
    """按 candidate/horizon 汇总 forward shadow outcome 样本成熟度。"""
    report_date = _parse_date(as_of) if as_of else date.today()
    if review_mode not in {"promotion", "validation"}:
        raise typer.BadParameter("review_mode 必须是 promotion 或 validation")
    parsed_review_mode = cast(Literal["promotion", "validation"], review_mode)
    rows = load_prediction_outcomes(prediction_outcomes_path)
    report = build_shadow_maturity_report(
        outcome_rows=rows,
        outcomes_path=prediction_outcomes_path,
        as_of=report_date,
        min_available_samples=min_available_samples,
        review_mode=parsed_review_mode,
    )
    report_output = output_path or default_shadow_maturity_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        report_date,
    )
    report_output = write_shadow_maturity_report(report, report_output)
    style = "green" if report.status == "PASS" else "yellow"
    console.print(f"[{style}]Shadow 样本成熟度：{report.status}[/{style}]")
    console.print(f"Review mode：{report.review_mode}")
    console.print(f"分组数：{len(report.groups)}")
    console.print(f"报告：{report_output}")


@feedback_app.command("build-causal-chain")
def build_decision_causal_chains_command(
    decision_snapshot_path: Annotated[
        Path,
        typer.Option(help="decision_snapshot JSON 文件或目录路径。"),
    ] = DEFAULT_DECISION_SNAPSHOT_DIR,
    outcomes_path: Annotated[
        Path,
        typer.Option(help="decision_outcomes CSV 路径；不存在时只生成 signal-time 链条。"),
    ] = DEFAULT_DECISION_OUTCOMES_PATH,
    output_path: Annotated[
        Path,
        typer.Option(help="decision_causal_chain ledger JSON 输出路径。"),
    ] = DEFAULT_DECISION_CAUSAL_CHAIN_PATH,
    report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 因果链报告输出路径。"),
    ] = None,
    as_of: Annotated[
        str | None,
        typer.Option(help="报告日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
) -> None:
    """构建 decision causal chain ledger。"""
    report_date = _parse_date(as_of) if as_of else date.today()
    snapshots = load_decision_snapshots(decision_snapshot_path)
    if not snapshots:
        raise typer.BadParameter(f"未找到 decision_snapshot：{decision_snapshot_path}")
    outcomes = load_decision_outcomes_frame(outcomes_path)
    ledger = build_decision_causal_chain_ledger(
        snapshots=snapshots,
        outcomes=outcomes,
    )
    ledger_output = write_decision_causal_chain_ledger(ledger, output_path)
    report_output = report_path or default_decision_causal_chain_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        report_date,
    )
    report_output = write_decision_causal_chain_report(
        ledger,
        ledger_path=ledger_output,
        output_path=report_output,
    )

    console.print("[green]决策因果链已生成。[/green]")
    console.print(f"链条数：{ledger.chain_count}")
    console.print(f"Ledger：{ledger_output}")
    console.print(f"报告：{report_output}")


@feedback_app.command("lookup-chain")
def lookup_decision_causal_chain_command(
    chain_id: Annotated[
        str,
        typer.Option("--id", help="decision causal chain id。"),
    ],
    input_path: Annotated[
        Path,
        typer.Option(help="decision_causal_chain ledger JSON 路径。"),
    ] = DEFAULT_DECISION_CAUSAL_CHAIN_PATH,
) -> None:
    """按 chain_id 反查 decision causal chain。"""
    try:
        chain = lookup_decision_causal_chain(input_path, chain_id)
    except FileNotFoundError as exc:
        raise typer.BadParameter(f"decision causal chain ledger 不存在：{input_path}") from exc
    except KeyError as exc:
        raise typer.BadParameter(str(exc)) from exc
    console.print(render_decision_causal_chain_lookup(chain))


@feedback_app.command("build-learning-queue")
def build_decision_learning_queue_command(
    causal_chain_path: Annotated[
        Path,
        typer.Option(help="decision_causal_chain ledger JSON 输入路径。"),
    ] = DEFAULT_DECISION_CAUSAL_CHAIN_PATH,
    output_path: Annotated[
        Path,
        typer.Option(help="decision learning queue JSON 输出路径。"),
    ] = DEFAULT_DECISION_LEARNING_QUEUE_PATH,
    report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 学习队列报告输出路径。"),
    ] = None,
    as_of: Annotated[
        str | None,
        typer.Option(help="报告日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    min_available_windows: Annotated[
        int,
        typer.Option(help="形成非 sample_limited 归因所需的最少可用 outcome 窗口。"),
    ] = 1,
) -> None:
    """从 decision causal chain 生成学习复核队列。"""
    report_date = _parse_date(as_of) if as_of else date.today()
    try:
        causal_ledger = load_decision_causal_chain_ledger(causal_chain_path)
    except FileNotFoundError as exc:
        raise typer.BadParameter(
            f"decision causal chain ledger 不存在：{causal_chain_path}"
        ) from exc
    ledger = build_decision_learning_queue(
        chains=tuple(causal_ledger.get("chains", [])),
        min_available_windows=min_available_windows,
    )
    queue_output = write_decision_learning_queue(ledger, output_path)
    report_output = report_path or default_decision_learning_queue_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        report_date,
    )
    report_output = write_decision_learning_queue_report(
        ledger,
        ledger_path=queue_output,
        output_path=report_output,
    )

    console.print("[green]决策学习队列已生成。[/green]")
    console.print(f"复核项数：{ledger.item_count}")
    console.print(f"Queue：{queue_output}")
    console.print(f"报告：{report_output}")


@feedback_app.command("lookup-learning")
def lookup_decision_learning_queue_command(
    review_id: Annotated[
        str,
        typer.Option("--id", help="learning review id。"),
    ],
    input_path: Annotated[
        Path,
        typer.Option(help="decision learning queue JSON 路径。"),
    ] = DEFAULT_DECISION_LEARNING_QUEUE_PATH,
) -> None:
    """按 review_id 反查 learning queue 项。"""
    try:
        item = lookup_decision_learning_item(input_path, review_id)
    except FileNotFoundError as exc:
        raise typer.BadParameter(f"decision learning queue 不存在：{input_path}") from exc
    except KeyError as exc:
        raise typer.BadParameter(str(exc)) from exc
    console.print(render_decision_learning_item_lookup(item))


@feedback_app.command("build-rule-experiments")
def build_rule_experiments_command(
    learning_queue_path: Annotated[
        Path,
        typer.Option(help="decision learning queue JSON 输入路径。"),
    ] = DEFAULT_DECISION_LEARNING_QUEUE_PATH,
    output_path: Annotated[
        Path,
        typer.Option(help="rule experiment ledger JSON 输出路径。"),
    ] = DEFAULT_RULE_EXPERIMENT_LEDGER_PATH,
    report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 规则实验台账报告输出路径。"),
    ] = None,
    as_of: Annotated[
        str | None,
        typer.Option(help="报告日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    replay_start: Annotated[
        str,
        typer.Option(help="历史 replay 计划起点，格式为 YYYY-MM-DD。"),
    ] = "2022-12-01",
    replay_end: Annotated[
        str | None,
        typer.Option(help="历史 replay 计划终点，格式为 YYYY-MM-DD，默认 as-of。"),
    ] = None,
    shadow_start: Annotated[
        str | None,
        typer.Option(help="前向 shadow 计划起点，格式为 YYYY-MM-DD，默认 as-of。"),
    ] = None,
    shadow_days: Annotated[
        int,
        typer.Option(help="前向 shadow 最少观察天数。"),
    ] = 20,
) -> None:
    """从 learning queue 生成候选规则实验台账。"""
    report_date = _parse_date(as_of) if as_of else date.today()
    try:
        learning_ledger = load_decision_learning_queue(learning_queue_path)
    except FileNotFoundError as exc:
        raise typer.BadParameter(f"decision learning queue 不存在：{learning_queue_path}") from exc
    replay_start_date = _parse_date(replay_start)
    replay_end_date = _parse_date(replay_end) if replay_end else report_date
    shadow_start_date = _parse_date(shadow_start) if shadow_start else report_date
    ledger = build_rule_experiment_ledger(
        learning_items=tuple(learning_ledger.get("items", [])),
        replay_start=replay_start_date,
        replay_end=replay_end_date,
        shadow_start=shadow_start_date,
        shadow_days=shadow_days,
    )
    ledger_output = write_rule_experiment_ledger(ledger, output_path)
    report_output = report_path or default_rule_experiment_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        report_date,
    )
    report_output = write_rule_experiment_report(
        ledger,
        ledger_path=ledger_output,
        output_path=report_output,
    )

    console.print("[green]候选规则实验台账已生成。[/green]")
    console.print(f"候选规则数：{ledger.candidate_count}")
    console.print(f"Ledger：{ledger_output}")
    console.print(f"报告：{report_output}")


@feedback_app.command("lookup-rule-experiment")
def lookup_rule_experiment_command(
    candidate_id: Annotated[
        str,
        typer.Option("--id", help="rule experiment candidate id。"),
    ],
    input_path: Annotated[
        Path,
        typer.Option(help="rule experiment ledger JSON 路径。"),
    ] = DEFAULT_RULE_EXPERIMENT_LEDGER_PATH,
) -> None:
    """按 candidate_id 反查候选规则实验。"""
    try:
        candidate = lookup_rule_experiment(input_path, candidate_id)
    except FileNotFoundError as exc:
        raise typer.BadParameter(f"rule experiment ledger 不存在：{input_path}") from exc
    except KeyError as exc:
        raise typer.BadParameter(str(exc)) from exc
    console.print(render_rule_experiment_lookup(candidate))


@feedback_app.command("validate-rule-cards")
def validate_rule_cards_command(
    input_path: Annotated[
        Path,
        typer.Option(help="rule cards YAML 路径。"),
    ] = DEFAULT_RULE_CARDS_PATH,
    as_of: Annotated[
        str | None,
        typer.Option(help="校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 规则治理校验报告输出路径。"),
    ] = None,
) -> None:
    """校验 production / candidate / retired rule card registry。"""
    validation_date = _parse_date(as_of) if as_of else date.today()
    report = validate_rule_card_store(
        load_rule_card_store(input_path),
        as_of=validation_date,
    )
    report_path = output_path or default_rule_governance_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        validation_date,
    )
    write_rule_governance_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]规则治理状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    console.print(
        f"Rule cards：{report.card_count}；"
        f"Production：{report.production_count}；Candidate：{report.candidate_count}"
    )
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")
    if not report.passed:
        raise typer.Exit(code=1)


@feedback_app.command("lookup-rule-card")
def lookup_rule_card_command(
    rule_id: Annotated[
        str,
        typer.Option("--id", help="rule card id。"),
    ],
    input_path: Annotated[
        Path,
        typer.Option(help="rule cards YAML 路径。"),
    ] = DEFAULT_RULE_CARDS_PATH,
) -> None:
    """按 rule_id 反查 rule card。"""
    try:
        card = lookup_rule_card(input_path, rule_id)
    except FileNotFoundError as exc:
        raise typer.BadParameter(f"rule card registry 不存在：{input_path}") from exc
    except (KeyError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    console.print(render_rule_card_lookup(card))


@feedback_app.command("promote-rule-card")
def promote_rule_card_command(
    rule_id: Annotated[
        str,
        typer.Option("--id", help="candidate rule card id。"),
    ],
    input_path: Annotated[
        Path,
        typer.Option(help="rule cards YAML 输入路径。"),
    ] = DEFAULT_RULE_CARDS_PATH,
    output_path: Annotated[
        Path | None,
        typer.Option(help="rule cards YAML 输出路径；不传则覆盖输入路径。"),
    ] = None,
    as_of: Annotated[
        str | None,
        typer.Option(help="批准日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    approved_by: Annotated[
        str,
        typer.Option(help="owner / reviewer 标识。"),
    ] = "",
    approval_rationale: Annotated[
        str,
        typer.Option(help="owner 批准理由。"),
    ] = "",
    promotion_report_ref: Annotated[
        str,
        typer.Option(help="model promotion report 或等价审计引用。"),
    ] = "",
    outcome_refs: Annotated[
        str,
        typer.Option(help="逗号分隔的 prediction outcome / shadow maturity 引用。"),
    ] = "",
    production_since: Annotated[
        str | None,
        typer.Option(help="production rule 生效日期，格式为 YYYY-MM-DD；默认 as-of。"),
    ] = None,
    report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 生命周期操作报告输出路径。"),
    ] = None,
) -> None:
    """把已批准 candidate rule card 受控升级为 production。"""
    promotion_date = _parse_date(as_of) if as_of else date.today()
    output = output_path or input_path
    try:
        report = promote_rule_card(
            input_path=input_path,
            output_path=output,
            rule_id=rule_id,
            as_of=promotion_date,
            approved_by=approved_by,
            approval_rationale=approval_rationale,
            promotion_report_ref=promotion_report_ref,
            outcome_refs=tuple(_parse_csv_items(outcome_refs)),
            production_since=_parse_date(production_since) if production_since else None,
        )
    except (KeyError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    lifecycle_report_path = report_path or (
        PROJECT_ROOT
        / "outputs"
        / "reports"
        / f"rule_lifecycle_promote_{promotion_date.isoformat()}.md"
    )
    lifecycle_report_path = write_rule_lifecycle_action_report(
        report,
        lifecycle_report_path,
    )
    style = (
        "green"
        if report.status == "PASS"
        else "yellow" if report.validation_report.passed else "red"
    )
    console.print(f"[{style}]Rule promotion 状态：{report.status}[/{style}]")
    console.print(f"Rule cards：{report.output_path}")
    console.print(f"操作报告：{lifecycle_report_path}")
    if not report.validation_report.passed:
        raise typer.Exit(code=1)


@feedback_app.command("retire-rule-card")
def retire_rule_card_command(
    rule_id: Annotated[
        str,
        typer.Option("--id", help="production rule card id。"),
    ],
    input_path: Annotated[
        Path,
        typer.Option(help="rule cards YAML 输入路径。"),
    ] = DEFAULT_RULE_CARDS_PATH,
    output_path: Annotated[
        Path | None,
        typer.Option(help="rule cards YAML 输出路径；不传则覆盖输入路径。"),
    ] = None,
    as_of: Annotated[
        str | None,
        typer.Option(help="操作日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    retired_at: Annotated[
        str | None,
        typer.Option(help="退役生效日期，格式为 YYYY-MM-DD；默认 as-of。"),
    ] = None,
    reason: Annotated[
        str,
        typer.Option(help="退役原因。"),
    ] = "",
    report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 生命周期操作报告输出路径。"),
    ] = None,
) -> None:
    """把 production rule card 标记为 retired。"""
    action_date = _parse_date(as_of) if as_of else date.today()
    output = output_path or input_path
    try:
        report = retire_rule_card(
            input_path=input_path,
            output_path=output,
            rule_id=rule_id,
            as_of=action_date,
            retired_at=_parse_date(retired_at) if retired_at else None,
            reason=reason,
        )
    except (KeyError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    lifecycle_report_path = report_path or (
        PROJECT_ROOT / "outputs" / "reports" / f"rule_lifecycle_retire_{action_date.isoformat()}.md"
    )
    lifecycle_report_path = write_rule_lifecycle_action_report(
        report,
        lifecycle_report_path,
    )
    style = (
        "green"
        if report.status == "PASS"
        else "yellow" if report.validation_report.passed else "red"
    )
    console.print(f"[{style}]Rule retirement 状态：{report.status}[/{style}]")
    console.print(f"Rule cards：{report.output_path}")
    console.print(f"操作报告：{lifecycle_report_path}")
    if not report.validation_report.passed:
        raise typer.Exit(code=1)


@feedback_app.command("validate-benchmark-policy")
def validate_benchmark_policy_command(
    input_path: Annotated[
        Path,
        typer.Option(help="benchmark policy YAML 路径。"),
    ] = DEFAULT_BENCHMARK_POLICY_CONFIG_PATH,
    as_of: Annotated[
        str | None,
        typer.Option(help="校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    strategy_ticker: Annotated[
        str | None,
        typer.Option(help="可选：本次回测或校准使用的 AI proxy / strategy ticker。"),
    ] = None,
    benchmarks: Annotated[
        str | None,
        typer.Option(help="可选：逗号分隔的本次对比基准 ticker。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown benchmark policy 校验报告输出路径。"),
    ] = None,
) -> None:
    """校验 AI proxy 与 benchmark policy registry。"""
    validation_date = _parse_date(as_of) if as_of else date.today()
    benchmark_tickers = tuple(_parse_csv_items(benchmarks)) if benchmarks is not None else None
    report = validate_benchmark_policy(
        load_benchmark_policy(input_path),
        as_of=validation_date,
        selected_strategy_ticker=strategy_ticker,
        selected_benchmark_tickers=benchmark_tickers,
    )
    report_path = output_path or default_benchmark_policy_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        validation_date,
    )
    write_benchmark_policy_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]基准政策状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    console.print(
        f"Benchmark：{report.instrument_count}；" f"Custom AI basket：{report.custom_basket_count}"
    )
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")
    if not report.passed:
        raise typer.Exit(code=1)


@feedback_app.command("lookup-benchmark-policy")
def lookup_benchmark_policy_command(
    entry_id: Annotated[
        str,
        typer.Option("--id", help="benchmark id、ticker 或 custom basket id。"),
    ],
    input_path: Annotated[
        Path,
        typer.Option(help="benchmark policy YAML 路径。"),
    ] = DEFAULT_BENCHMARK_POLICY_CONFIG_PATH,
) -> None:
    """按 ticker、benchmark id 或 basket id 反查 benchmark policy 条目。"""
    try:
        entry = lookup_benchmark_policy_entry(input_path, entry_id)
    except FileNotFoundError as exc:
        raise typer.BadParameter(f"benchmark policy 不存在：{input_path}") from exc
    except (KeyError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    console.print(render_benchmark_policy_lookup(entry))


@feedback_app.command("loop-review")
def feedback_loop_review_command(
    evidence_path: Annotated[
        Path,
        typer.Option(help="market evidence YAML 文件或目录路径。"),
    ] = PROJECT_ROOT
    / "data"
    / "external"
    / "market_evidence",
    decision_snapshot_path: Annotated[
        Path,
        typer.Option(help="decision_snapshot JSON 文件或目录路径。"),
    ] = DEFAULT_DECISION_SNAPSHOT_DIR,
    outcomes_path: Annotated[
        Path,
        typer.Option(help="decision_outcomes CSV 路径。"),
    ] = DEFAULT_DECISION_OUTCOMES_PATH,
    prediction_outcomes_path: Annotated[
        Path,
        typer.Option(help="prediction_outcomes CSV 路径，用于 production vs challenger 复核。"),
    ] = DEFAULT_PREDICTION_OUTCOMES_PATH,
    causal_chain_path: Annotated[
        Path,
        typer.Option(help="decision_causal_chain ledger JSON 路径。"),
    ] = DEFAULT_DECISION_CAUSAL_CHAIN_PATH,
    learning_queue_path: Annotated[
        Path,
        typer.Option(help="decision learning queue JSON 路径。"),
    ] = DEFAULT_DECISION_LEARNING_QUEUE_PATH,
    rule_experiment_path: Annotated[
        Path,
        typer.Option(help="rule experiment ledger JSON 路径。"),
    ] = DEFAULT_RULE_EXPERIMENT_LEDGER_PATH,
    task_register_path: Annotated[
        Path,
        typer.Option(help="任务登记 Markdown 路径。"),
    ] = PROJECT_ROOT
    / "docs"
    / "task_register.md",
    as_of: Annotated[
        str | None,
        typer.Option(help="复核日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    since: Annotated[
        str | None,
        typer.Option(help="复核窗口起始日期，格式为 YYYY-MM-DD，默认 as_of 前 7 天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 闭环复核报告输出路径。"),
    ] = None,
) -> None:
    """生成反馈闭环周期复核报告。"""
    review_date = _parse_date(as_of) if as_of else date.today()
    since_date = _parse_date(since) if since else None
    report = build_feedback_loop_review_report(
        as_of=review_date,
        since=since_date,
        evidence_path=evidence_path,
        decision_snapshot_path=decision_snapshot_path,
        outcomes_path=outcomes_path,
        prediction_outcomes_path=prediction_outcomes_path,
        causal_chain_path=causal_chain_path,
        learning_queue_path=learning_queue_path,
        rule_experiment_path=rule_experiment_path,
        task_register_path=task_register_path,
    )
    report_path = output_path or default_feedback_loop_review_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        review_date,
    )
    write_feedback_loop_review_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow"
    console.print(f"[{status_style}]反馈闭环复核状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    console.print(f"警告数：{report.warning_count}")


@feedback_app.command("build-parameter-replay")
def build_parameter_replay_command(
    robustness_summary_path: Annotated[
        Path | None,
        typer.Option(
            help=(
                "backtest robustness JSON 摘要路径；不传时读取 outputs/backtests 最新 "
                "backtest_robustness_*.json。"
            )
        ),
    ] = None,
    as_of: Annotated[
        str | None,
        typer.Option(help="复核日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 参数 replay 报告输出路径。"),
    ] = None,
    summary_output_path: Annotated[
        Path | None,
        typer.Option(help="JSON 参数 replay 摘要输出路径。"),
    ] = None,
) -> None:
    """把 backtest robustness 参数复测结果接入 feedback 闭环。"""
    review_date = _parse_date(as_of) if as_of else date.today()
    selected_summary_path = robustness_summary_path or latest_backtest_robustness_summary_path(
        DEFAULT_BACKTEST_ROBUSTNESS_DIR
    )
    if selected_summary_path is None:
        console.print(
            "[red]未找到 backtest_robustness_*.json；"
            "请先运行 aits backtest --robustness-report。[/red]"
        )
        raise typer.Exit(code=1)
    try:
        report = build_parameter_replay_report(
            robustness_summary_path=selected_summary_path,
            as_of=review_date,
        )
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        console.print(f"[red]参数 replay 构建失败：{exc}[/red]")
        raise typer.Exit(code=1) from exc
    report_output = output_path or default_parameter_replay_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        review_date,
    )
    summary_output = summary_output_path or default_parameter_replay_summary_path(
        PROJECT_ROOT / "outputs" / "reports",
        review_date,
    )
    write_parameter_replay_report(report, report_output)
    write_parameter_replay_summary(report, summary_output)

    status_style = "green" if report.status == "PASS" else "yellow"
    console.print(f"[{status_style}]参数 replay 状态：{report.status}[/{status_style}]")
    console.print(f"参数复测场景：{report.scenario_count}")
    console.print(f"Material delta：{report.material_delta_count}")
    console.print(f"报告：{report_output}")
    console.print(f"摘要：{summary_output}")
    console.print(
        "治理边界：本命令只读解释参数复测收益变化，" "不修改 production scoring 或仓位闸门。"
    )


@feedback_app.command("build-parameter-candidates")
def build_parameter_candidates_command(
    parameter_replay_summary_path: Annotated[
        Path | None,
        typer.Option(
            help=(
                "parameter replay JSON 摘要路径；默认 "
                "outputs/reports/parameter_replay_YYYY-MM-DD.json。"
            )
        ),
    ] = None,
    as_of: Annotated[
        str | None,
        typer.Option(help="复核日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path,
        typer.Option(help="参数候选 ledger JSON 输出路径。"),
    ] = DEFAULT_PARAMETER_CANDIDATE_LEDGER_PATH,
    report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 参数候选报告输出路径。"),
    ] = None,
    candidate_gate_mode: Annotated[
        str,
        typer.Option(
            help=(
                "候选门禁模式：strict 或 flow_validation。flow_validation "
                "仅用于接线验证，不改变 production。"
            )
        ),
    ] = "strict",
) -> None:
    """从 parameter replay 摘要生成 candidate-only 参数候选台账。"""
    review_date = _parse_date(as_of) if as_of else date.today()
    try:
        ledger = build_parameter_candidate_ledger(
            parameter_replay_summary_path=parameter_replay_summary_path,
            as_of=review_date,
            evaluation_mode=candidate_gate_mode,
        )
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        console.print(f"[red]参数候选台账构建失败：{exc}[/red]")
        raise typer.Exit(code=1) from exc
    ledger_output = write_parameter_candidate_ledger(ledger, output_path)
    report_output = report_path or default_parameter_candidate_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        review_date,
    )
    report_output = write_parameter_candidate_report(
        ledger,
        ledger_output,
        report_output,
    )

    status_style = "green" if ledger.status == "PASS" else "yellow"
    console.print(f"[{status_style}]参数候选状态：{ledger.status}[/{status_style}]")
    console.print(f"Trial 数：{ledger.trial_count}")
    console.print(f"Candidate 数：{ledger.candidate_count}")
    console.print(f"Evaluation mode：{ledger.evaluation_mode}")
    console.print(f"Forward shadow ready：{ledger.ready_for_forward_shadow_count}")
    console.print(f"Blocked：{ledger.blocked_count}")
    console.print(f"Ledger：{ledger_output}")
    console.print(f"报告：{report_output}")
    console.print("治理边界：候选台账不批准参数上线，不修改 production scoring 或仓位闸门。")


@feedback_app.command("evaluate-parameter-governance")
def evaluate_parameter_governance_command(
    as_of: Annotated[
        str | None,
        typer.Option(help="复核日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    manifest_path: Annotated[
        Path,
        typer.Option(help="参数治理 manifest 路径。"),
    ] = DEFAULT_PARAMETER_GOVERNANCE_MANIFEST_PATH,
    parameter_candidate_ledger_path: Annotated[
        Path,
        typer.Option(help="parameter candidates ledger JSON 路径。"),
    ] = DEFAULT_PARAMETER_CANDIDATE_LEDGER_PATH,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 参数治理报告输出路径。"),
    ] = None,
    summary_output_path: Annotated[
        Path | None,
        typer.Option(help="JSON 参数治理摘要输出路径。"),
    ] = None,
) -> None:
    """评估可调参数 manifest 与候选证据，输出只读治理建议。"""
    review_date = _parse_date(as_of) if as_of else date.today()
    try:
        report = build_parameter_governance_report(
            as_of=review_date,
            manifest_path=manifest_path,
            candidate_ledger_path=parameter_candidate_ledger_path,
        )
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        console.print(f"[red]参数治理评估失败：{exc}[/red]")
        raise typer.Exit(code=1) from exc
    report_output = output_path or default_parameter_governance_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        review_date,
    )
    summary_output = summary_output_path or default_parameter_governance_summary_path(
        PROJECT_ROOT / "outputs" / "reports",
        review_date,
    )
    write_parameter_governance_report(report, report_output)
    write_parameter_governance_summary(report, summary_output)

    status_style = "green" if report.status == "PASS" else "yellow"
    if report.status == "FAIL":
        status_style = "red"
    console.print(f"[{status_style}]参数治理状态：{report.status}[/{status_style}]")
    console.print(f"Manifest：{report.manifest.version}")
    console.print(f"Owner quantitative input：{report.manifest.owner_quantitative_input_status}")
    console.print(f"Action 分布：{report.action_counts}")
    console.print(f"报告：{report_output}")
    console.print(f"摘要：{summary_output}")
    console.print("治理边界：本命令不修改 production 参数、overlay、rule card 或日报结论。")
    if report.status == "FAIL":
        raise typer.Exit(code=1)


@feedback_app.command("optimize-market-feedback")
def optimize_market_feedback_command(
    as_of: Annotated[
        str | None,
        typer.Option(help="复核日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    since: Annotated[
        str | None,
        typer.Option(help="复核窗口起始日期，格式为 YYYY-MM-DD，默认 as_of 前 7 天。"),
    ] = None,
    replay_start: Annotated[
        str,
        typer.Option(help="as-if 回放窗口起始日期；默认 AI regime 起点 2022-12-01。"),
    ] = DEFAULT_MARKET_FEEDBACK_REPLAY_START.isoformat(),
    replay_end: Annotated[
        str | None,
        typer.Option(help="as-if 回放窗口结束日期，默认 as_of。"),
    ] = None,
    data_quality_report_path: Annotated[
        Path | None,
        typer.Option(help="数据质量报告路径；默认 outputs/reports/data_quality_YYYY-MM-DD.md。"),
    ] = None,
    decision_outcomes_path: Annotated[
        Path,
        typer.Option(help="decision_outcomes CSV 路径。"),
    ] = DEFAULT_DECISION_OUTCOMES_PATH,
    prediction_outcomes_path: Annotated[
        Path,
        typer.Option(help="prediction_outcomes CSV 路径。"),
    ] = DEFAULT_PREDICTION_OUTCOMES_PATH,
    causal_chain_path: Annotated[
        Path,
        typer.Option(help="decision causal chain ledger JSON 路径。"),
    ] = DEFAULT_DECISION_CAUSAL_CHAIN_PATH,
    learning_queue_path: Annotated[
        Path,
        typer.Option(help="decision learning queue JSON 路径。"),
    ] = DEFAULT_DECISION_LEARNING_QUEUE_PATH,
    rule_experiment_path: Annotated[
        Path,
        typer.Option(help="rule experiment ledger JSON 路径。"),
    ] = DEFAULT_RULE_EXPERIMENT_LEDGER_PATH,
    parameter_replay_summary_path: Annotated[
        Path | None,
        typer.Option(
            help=(
                "parameter replay JSON 摘要路径；默认 "
                "outputs/reports/parameter_replay_YYYY-MM-DD.json。"
            )
        ),
    ] = None,
    parameter_candidate_ledger_path: Annotated[
        Path,
        typer.Option(help="parameter candidates ledger JSON 路径。"),
    ] = DEFAULT_PARAMETER_CANDIDATE_LEDGER_PATH,
    parameter_governance_summary_path: Annotated[
        Path | None,
        typer.Option(
            help=(
                "parameter governance JSON 摘要路径；默认 "
                "outputs/reports/parameter_governance_YYYY-MM-DD.json。"
            )
        ),
    ] = None,
    shadow_maturity_report_path: Annotated[
        Path | None,
        typer.Option(
            help=(
                "shadow maturity Markdown 报告路径；默认 "
                "outputs/reports/shadow_maturity_YYYY-MM-DD.md。"
            )
        ),
    ] = None,
    calibration_overlay_path: Annotated[
        Path,
        typer.Option(help="approved calibration overlay JSON 路径。"),
    ] = DEFAULT_APPROVED_CALIBRATION_OVERLAY_PATH,
    effective_weights_path: Annotated[
        Path,
        typer.Option(help="current effective weights JSON 路径。"),
    ] = DEFAULT_EFFECTIVE_WEIGHTS_PATH,
    sample_policy_path: Annotated[
        Path,
        typer.Option(help="反馈优化样本政策配置路径。"),
    ] = DEFAULT_FEEDBACK_SAMPLE_POLICY_PATH,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 市场反馈优化报告输出路径。"),
    ] = None,
) -> None:
    """生成独立市场反馈优化闭环报告。"""
    review_date = _parse_date(as_of) if as_of else date.today()
    since_date = _parse_date(since) if since else None
    replay_start_date = _parse_date(replay_start)
    replay_end_date = _parse_date(replay_end) if replay_end else None
    report = build_market_feedback_optimization_report(
        as_of=review_date,
        since=since_date,
        replay_start=replay_start_date,
        replay_end=replay_end_date,
        data_quality_report_path=data_quality_report_path,
        decision_outcomes_path=decision_outcomes_path,
        prediction_outcomes_path=prediction_outcomes_path,
        causal_chain_path=causal_chain_path,
        learning_queue_path=learning_queue_path,
        rule_experiment_path=rule_experiment_path,
        parameter_replay_summary_path=parameter_replay_summary_path,
        parameter_candidate_ledger_path=parameter_candidate_ledger_path,
        parameter_governance_summary_path=parameter_governance_summary_path,
        shadow_maturity_report_path=shadow_maturity_report_path,
        calibration_overlay_path=calibration_overlay_path,
        effective_weights_path=effective_weights_path,
        sample_policy_path=sample_policy_path,
    )
    report_path = output_path or default_market_feedback_optimization_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        review_date,
    )
    write_market_feedback_optimization_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow"
    console.print(f"[{status_style}]市场反馈优化状态：{report.status}[/{status_style}]")
    console.print(f"Readiness：{report.readiness}")
    console.print(f"报告：{report_path}")
    console.print(f"警告数：{report.warning_count}")


@app.callback()
def main() -> None:
    """AI 产业链趋势分析和仓位管理工具。"""


def _configured_position_band_rules(
    scoring_rules: ScoringRulesConfig,
) -> tuple[PositionBandRule, ...]:
    return tuple(
        PositionBandRule(
            min_score=band.min_score,
            min_position=band.min_position,
            max_position=band.max_position,
            label=band.label,
        )
        for band in scoring_rules.position_bands
    )


@app.command("score-example")
def score_example() -> None:
    """输出一份示例仓位建议。"""
    scoring_rules = load_scoring_rules()
    model = WeightedScoreModel(position_bands=_configured_position_band_rules(scoring_rules))
    portfolio = load_portfolio()
    recommendation = model.recommend(
        [
            ModuleScore(
                "trend",
                score=72,
                weight=25,
                reason="SMH 和 QQQ 仍在长期均线上方",
            ),
            ModuleScore("fundamentals", score=60, weight=25, reason="MVP 阶段中性占位"),
            ModuleScore("macro_liquidity", score=55, weight=15, reason="利率环境不算友好"),
            ModuleScore("risk_sentiment", score=66, weight=15, reason="VIX 仍可控"),
            ModuleScore("valuation", score=50, weight=10, reason="MVP 阶段中性占位"),
            ModuleScore(
                "policy_geopolitics",
                score=60,
                weight=10,
                reason="MVP 阶段中性占位",
            ),
        ],
        total_risk_asset_min=portfolio.portfolio.total_risk_asset_min,
        total_risk_asset_max=portfolio.portfolio.total_risk_asset_max,
    )
    console.print(render_recommendation_markdown(recommendation))


@app.command("download-data")
def download_data(
    start: Annotated[
        str,
        typer.Option(help="开始日期，包含当天，格式为 YYYY-MM-DD。"),
    ] = "2018-01-01",
    end: Annotated[
        str | None,
        typer.Option(help="结束日期，包含当天，格式为 YYYY-MM-DD。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option(help="输出缓存目录。"),
    ] = PROJECT_ROOT
    / "data"
    / "raw",
    full_universe: Annotated[
        bool,
        typer.Option(
            "--full-universe",
            help="包含配置中的完整 AI 产业链标的，而不只下载核心观察池。",
        ),
    ] = False,
    price_provider: Annotated[
        str,
        typer.Option(help="主价格源：fmp 或 yahoo。默认 fmp。"),
    ] = "fmp",
    fmp_api_key_env: Annotated[
        str,
        typer.Option(help="读取 FMP API key 的环境变量名。"),
    ] = "FMP_API_KEY",
    with_marketstack: Annotated[
        bool,
        typer.Option(
            "--with-marketstack/--without-marketstack",
            help="是否同时下载 Marketstack 第二行情源缓存。",
        ),
    ] = True,
    marketstack_api_key_env: Annotated[
        str,
        typer.Option(help="读取 Marketstack API key 的环境变量名。"),
    ] = "MARKETSTACK_API_KEY",
    failure_report_path: Annotated[
        Path | None,
        typer.Option(help="下载失败诊断报告路径；默认写入 outputs/reports。"),
    ] = None,
) -> None:
    """下载市场日线价格和 FRED 宏观序列到本地 CSV 缓存。"""
    universe = load_universe()
    start_date = _parse_date(start)
    end_date = _parse_date(end) if end else date.today()
    normalized_price_provider = price_provider.strip().lower()
    if normalized_price_provider == "fmp":
        fmp_api_key = os.getenv(fmp_api_key_env, "")
        if not fmp_api_key:
            console.print(f"[red]未设置 {fmp_api_key_env}，无法下载 FMP 主价格源。[/red]")
            raise typer.Exit(code=1)
        primary_price_provider = FmpPriceProvider(api_key=fmp_api_key)
    elif normalized_price_provider == "yahoo":
        primary_price_provider = YFinancePriceProvider()
    else:
        raise typer.BadParameter("主价格源必须是 fmp 或 yahoo。")

    marketstack_provider = None
    if with_marketstack:
        api_key = os.getenv(marketstack_api_key_env, "")
        if not api_key:
            console.print(
                f"[red]未设置 {marketstack_api_key_env}，无法下载 Marketstack 第二行情源。[/red]"
            )
            raise typer.Exit(code=1)
        marketstack_provider = MarketstackPriceProvider(api_key=api_key)

    try:
        summary = download_daily_data(
            universe,
            start=start_date,
            end=end_date,
            output_dir=output_dir,
            include_full_ai_chain=full_universe,
            price_provider=primary_price_provider,
            secondary_price_provider=marketstack_provider,
        )
    except Exception as exc:
        report_path = failure_report_path or default_download_failure_report_path(
            PROJECT_ROOT / "outputs" / "reports",
            end_date,
        )
        write_download_failure_report(
            output_path=report_path,
            start=start_date,
            end=end_date,
            raw_output_dir=output_dir,
            include_full_ai_chain=full_universe,
            price_provider_name=normalized_price_provider,
            with_marketstack=with_marketstack,
            error=exc,
        )
        console.print("[red]数据缓存更新失败，已停止。[/red]")
        console.print(f"下载失败诊断报告：{report_path}")
        console.print(f"脱敏错误摘要：{sanitize_diagnostic_text(str(exc))}")
        raise typer.Exit(code=1) from exc

    console.print("[green]数据缓存已更新。[/green]")
    console.print(f"主价格源：{normalized_price_provider}")
    console.print(f"价格数据：{summary.prices_path}（{summary.price_rows} 行）")
    if summary.secondary_prices_path is not None:
        console.print(
            f"Marketstack 第二行情源：{summary.secondary_prices_path}"
            f"（{summary.secondary_price_rows} 行）"
        )
    console.print(f"FRED 宏观序列：{summary.rates_path}（{summary.rate_rows} 行）")
    console.print(f"下载审计清单：{summary.manifest_path}")
    console.print(f"价格标的：{', '.join(summary.price_tickers)}")
    console.print(f"FRED 宏观序列：{', '.join(summary.rate_series)}")


@app.command("validate-data")
def validate_data(
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
    as_of: Annotated[
        str | None,
        typer.Option(help="校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 报告输出路径。"),
    ] = None,
    full_universe: Annotated[
        bool,
        typer.Option(
            "--full-universe",
            help="校验配置中的完整 AI 产业链标的，而不只校验核心观察池。",
        ),
    ] = False,
    backtest_manifest_path: Annotated[
        Path | None,
        typer.Option(
            "--backtest-manifest",
            help="可选 backtest_input_manifest.json，用于 manifest context 下的数据门禁诊断。",
        ),
    ] = None,
) -> None:
    """校验缓存数据并写入 Markdown 质量报告。"""
    universe = load_universe()
    quality_config = load_data_quality()
    validation_date = _parse_date(as_of) if as_of else date.today()
    report_path = output_path or default_quality_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        validation_date,
    )

    report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=configured_price_tickers(
            universe,
            include_full_ai_chain=full_universe,
        ),
        expected_rate_series=configured_rate_series(universe),
        quality_config=quality_config,
        as_of=validation_date,
        manifest_path=_download_manifest_path(prices_path),
        backtest_manifest_path=backtest_manifest_path,
        secondary_prices_path=_marketstack_prices_path(prices_path),
        require_secondary_prices=_requires_marketstack_prices(prices_path),
    )
    write_data_quality_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]数据质量状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    if report.marketstack_reconciliation_records:
        reconciliation_path = marketstack_reconciliation_path(report_path)
        console.print(f"Marketstack reconciliation：{reconciliation_path}")
    console.print(
        f"错误数：{report.error_count}；"
        f"警告数：{report.warning_count}；"
        f"信息数：{report.info_count}"
    )

    if not report.passed:
        raise typer.Exit(code=1)


@app.command("backtest")
def backtest(
    prices_path: Annotated[
        Path,
        typer.Option(help="标准化日线价格 CSV 路径。"),
    ] = PROJECT_ROOT
    / "data"
    / "raw"
    / "prices_daily.csv",
    rates_path: Annotated[
        Path,
        typer.Option(help="标准化日线利率 CSV 路径。"),
    ] = PROJECT_ROOT
    / "data"
    / "raw"
    / "rates_daily.csv",
    start: Annotated[
        str | None,
        typer.Option(
            "--from",
            help="回测开始日期，格式为 YYYY-MM-DD；未提供时使用所选市场阶段起点。",
        ),
    ] = None,
    end: Annotated[
        str | None,
        typer.Option("--to", help="回测结束日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    strategy_ticker: Annotated[
        str,
        typer.Option(help="用于承载 AI 仓位的策略代理标的。"),
    ] = "SMH",
    benchmarks: Annotated[
        str,
        typer.Option(help="逗号分隔的买入持有基准标的。"),
    ] = ",".join(DEFAULT_BENCHMARK_TICKERS),
    benchmark_policy_path: Annotated[
        Path,
        typer.Option(help="benchmark policy YAML 路径，用于审计 AI proxy / benchmark 解释口径。"),
    ] = DEFAULT_BENCHMARK_POLICY_CONFIG_PATH,
    benchmark_policy_report_path: Annotated[
        Path | None,
        typer.Option(help="可选：Markdown benchmark policy 校验报告输出路径。"),
    ] = None,
    rule_cards_path: Annotated[
        Path,
        typer.Option(help="rule card registry YAML 路径，用于记录本次回测规则版本。"),
    ] = DEFAULT_RULE_CARDS_PATH,
    cost_bps: Annotated[
        float,
        typer.Option(help="单边交易成本，单位 bps。"),
    ] = 5.0,
    spread_bps: Annotated[
        float,
        typer.Option(help="Bid-ask spread 假设，单位 bps；默认不额外扣除。"),
    ] = 0.0,
    slippage_bps: Annotated[
        float,
        typer.Option(help="线性滑点估算，单位 bps；默认不额外扣除。"),
    ] = 0.0,
    market_impact_bps: Annotated[
        float,
        typer.Option(help="市场冲击估算，单位 bps；默认不额外扣除。"),
    ] = 0.0,
    tax_bps: Annotated[
        float,
        typer.Option(help="卖出侧税费假设，单位 bps；默认不额外扣除。"),
    ] = 0.0,
    fx_bps: Annotated[
        float,
        typer.Option(help="换汇成本假设，单位 bps；默认不额外扣除。"),
    ] = 0.0,
    financing_annual_bps: Annotated[
        float,
        typer.Option(help="融资或持仓资金年化成本，单位 bps；默认不额外扣除。"),
    ] = 0.0,
    etf_delay_bps: Annotated[
        float,
        typer.Option(help="ETF 成交延迟或申赎成本假设，单位 bps；默认不额外扣除。"),
    ] = 0.0,
    regime: Annotated[
        str | None,
        typer.Option(
            "--regime",
            help="市场阶段 ID，默认使用 config/market_regimes.yaml 的 default_backtest_regime。",
        ),
    ] = None,
    regimes_path: Annotated[
        Path,
        typer.Option(help="市场阶段配置文件路径。"),
    ] = DEFAULT_MARKET_REGIMES_CONFIG_PATH,
    daily_output_path: Annotated[
        Path | None,
        typer.Option(help="每日回测明细 CSV 输出路径。"),
    ] = None,
    input_coverage_output_path: Annotated[
        Path | None,
        typer.Option(help="机器可读历史输入覆盖诊断 CSV 输出路径。"),
    ] = None,
    audit_output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 回测输入审计报告输出路径。"),
    ] = None,
    trace_bundle_path: Annotated[
        Path | None,
        typer.Option(help="JSON evidence bundle 输出路径。"),
    ] = None,
    minimum_component_coverage: Annotated[
        float | None,
        typer.Option(
            help="审计用评分模块最低平均覆盖率阈值；默认读取 backtest validation policy。"
        ),
    ] = None,
    fail_on_audit_warning: Annotated[
        bool,
        typer.Option(
            "--fail-on-audit-warning",
            help="输入审计状态不是 PASS 时返回非零退出码，适合严格本地门禁。",
        ),
    ] = False,
    report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 回测报告输出路径。"),
    ] = None,
    robustness_report_path: Annotated[
        Path | None,
        typer.Option(
            help=(
                "Markdown 回测稳健性报告输出路径；提供时运行成本压力、起点后移、"
                "固定仓位、再平衡频率、趋势基线、权重扰动和买入持有基准对比。"
            ),
        ),
    ] = None,
    robustness_summary_path: Annotated[
        Path | None,
        typer.Option(help="机器可读回测稳健性 JSON 摘要输出路径。"),
    ] = None,
    robustness_report: Annotated[
        bool,
        typer.Option(
            "--robustness-report",
            help="按默认 outputs/backtests 路径生成回测稳健性报告。",
        ),
    ] = False,
    robustness_shift_days: Annotated[
        int | None,
        typer.Option(help="稳健性报告的起点后移天数；默认读取 backtest validation policy。"),
    ] = None,
    robustness_cost_stress_bps: Annotated[
        float | None,
        typer.Option(
            help="稳健性报告中交易执行成本压力的额外 bps；默认读取 backtest validation policy。"
        ),
    ] = None,
    robustness_weight_perturbation_pct: Annotated[
        float | None,
        typer.Option(
            help="稳健性报告中单模块权重上调/下调扰动比例；默认读取 backtest validation policy。"
        ),
    ] = None,
    robustness_random_seed_start: Annotated[
        int | None,
        typer.Option(
            help="稳健性报告中同换手率随机策略的起始随机种子；默认读取 backtest validation policy。"
        ),
    ] = None,
    robustness_random_seed_count: Annotated[
        int | None,
        typer.Option(
            help="稳健性报告中同换手率随机策略的种子数量；默认读取 backtest validation policy。"
        ),
    ] = None,
    robustness_oos_split_ratio: Annotated[
        float | None,
        typer.Option(
            help=(
                "稳健性报告中时间顺序样本外验证的 in-sample 切分比例；"
                "默认读取 backtest validation policy。"
            )
        ),
    ] = None,
    lag_sensitivity_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 回测滞后敏感性报告输出路径。"),
    ] = None,
    lag_sensitivity_summary_path: Annotated[
        Path | None,
        typer.Option(help="机器可读回测滞后敏感性 JSON 摘要输出路径。"),
    ] = None,
    lag_sensitivity_report: Annotated[
        bool,
        typer.Option(
            "--lag-sensitivity-report",
            help="按默认 outputs/backtests 路径生成回测滞后敏感性报告。",
        ),
    ] = False,
    lag_sensitivity_days: Annotated[
        str,
        typer.Option(help="逗号分隔的 feature/universe 滞后交易日列表。"),
    ] = "0,1,3,5,10,20",
    feature_availability_path: Annotated[
        Path,
        typer.Option(help="PIT feature availability catalog YAML 路径。"),
    ] = DEFAULT_FEATURE_AVAILABILITY_CONFIG_PATH,
    feature_availability_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown PIT 特征可见时间报告输出路径。"),
    ] = None,
    promotion_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 模型晋级门槛报告输出路径。"),
    ] = None,
    promotion_summary_path: Annotated[
        Path | None,
        typer.Option(help="机器可读模型晋级门槛 JSON 摘要输出路径。"),
    ] = None,
    promotion_report: Annotated[
        bool,
        typer.Option(
            "--promotion-report",
            help="按默认 outputs/backtests 路径生成模型晋级门槛报告。",
        ),
    ] = False,
    promotion_prediction_outcomes_path: Annotated[
        Path,
        typer.Option(help="prediction/shadow outcome CSV，用于模型晋级门槛评估。"),
    ] = DEFAULT_PREDICTION_OUTCOMES_PATH,
    quality_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 数据质量报告输出路径。"),
    ] = None,
    sec_companies_path: Annotated[
        Path,
        typer.Option(help="SEC company CIK 配置文件路径，用于 point-in-time 基本面回测。"),
    ] = DEFAULT_SEC_COMPANIES_CONFIG_PATH,
    sec_metrics_path: Annotated[
        Path,
        typer.Option(help="SEC 指标映射配置文件路径，用于 point-in-time 基本面回测。"),
    ] = DEFAULT_FUNDAMENTAL_METRICS_CONFIG_PATH,
    fundamental_feature_config_path: Annotated[
        Path,
        typer.Option(help="SEC 基本面特征公式配置文件路径，用于 point-in-time 基本面回测。"),
    ] = DEFAULT_FUNDAMENTAL_FEATURES_CONFIG_PATH,
    sec_companyfacts_dir: Annotated[
        Path,
        typer.Option(help="SEC companyfacts 原始 JSON 缓存目录。"),
    ] = PROJECT_ROOT
    / "data"
    / "raw"
    / "sec_companyfacts",
    tsm_ir_input_path: Annotated[
        Path,
        typer.Option(help="TSMC IR 季度指标 CSV，用于补齐 TSM point-in-time 回测基本面。"),
    ] = PROJECT_ROOT
    / "data"
    / "processed"
    / "tsm_ir_quarterly_metrics.csv",
    sec_companyfacts_validation_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown SEC companyfacts 缓存校验报告输出路径。"),
    ] = None,
    sec_fundamental_source: Annotated[
        Literal["legacy_companyfacts", "sec_pit_feature_panel"],
        typer.Option(
            "--sec-fundamental-source",
            help=(
                "回测基本面来源；legacy_companyfacts 使用 filed_date as-of，"
                "sec_pit_feature_panel 使用 TRADING-039 reconstructed PIT panel。"
            ),
        ),
    ] = "legacy_companyfacts",
    sec_pit_feature_panel_path: Annotated[
        Path,
        typer.Option(help="SEC reconstructed PIT feature panel CSV 路径。"),
    ] = DEFAULT_SEC_EDGAR_PROCESSED_DIR
    / "sec_pit_feature_panel.csv",
    valuation_path: Annotated[
        Path,
        typer.Option(help="估值快照 YAML 文件或目录路径，用于 point-in-time 回测评分。"),
    ] = PROJECT_ROOT
    / "data"
    / "external"
    / "valuation_snapshots",
    risk_events_path: Annotated[
        Path,
        typer.Option(help="风险事件配置路径，用于 point-in-time 回测评分。"),
    ] = DEFAULT_RISK_EVENTS_CONFIG_PATH,
    risk_event_occurrences_path: Annotated[
        Path,
        typer.Option(help="风险事件发生记录 YAML 文件或目录路径，用于 point-in-time 回测评分。"),
    ] = DEFAULT_RISK_EVENT_OCCURRENCES_PATH,
    watchlist_lifecycle_path: Annotated[
        Path,
        typer.Option(help="观察池 point-in-time lifecycle 配置路径，用于回测按 signal_date 过滤。"),
    ] = DEFAULT_WATCHLIST_LIFECYCLE_PATH,
    watchlist_lifecycle_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 观察池 lifecycle 校验报告输出路径。"),
    ] = None,
    quality_as_of: Annotated[
        str | None,
        typer.Option(help="数据质量校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    full_universe: Annotated[
        bool,
        typer.Option(
            "--full-universe",
            help="校验配置中的完整 AI 产业链标的，而不只校验核心观察池。",
        ),
    ] = False,
) -> None:
    """基于每日评分规则运行历史回测。"""
    universe = load_universe()
    industry_chain = load_industry_chain()
    watchlist = load_watchlist()
    watchlist_lifecycle = load_watchlist_lifecycle(watchlist_lifecycle_path)
    data_quality_config = load_data_quality()
    feature_config = load_features()
    scoring_rules = load_scoring_rules()
    weight_profile = load_weight_profile()
    calibration_overlays = load_calibration_overlays()
    backtest_validation_policy = load_backtest_validation_policy()
    robustness_policy = backtest_validation_policy.robustness
    portfolio = load_portfolio()
    market_regimes = load_market_regimes(regimes_path)
    selected_regime_id = regime or market_regimes.default_backtest_regime
    try:
        selected_regime = market_regime_by_id(market_regimes, selected_regime_id)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc

    start_date = _parse_date(start) if start else selected_regime.start_date
    end_date = _parse_date(end) if end else date.today()
    quality_date = _parse_date(quality_as_of) if quality_as_of else date.today()
    benchmark_tickers = _parse_csv_items(benchmarks)
    robustness_shift_days = (
        robustness_shift_days
        if robustness_shift_days is not None
        else robustness_policy.default_shifted_start_days
    )
    robustness_cost_stress_bps = (
        robustness_cost_stress_bps
        if robustness_cost_stress_bps is not None
        else robustness_policy.default_cost_stress_increment_bps
    )
    robustness_weight_perturbation_pct = (
        robustness_weight_perturbation_pct
        if robustness_weight_perturbation_pct is not None
        else robustness_policy.default_weight_perturbation_pct
    )
    robustness_random_seed_start = (
        robustness_random_seed_start
        if robustness_random_seed_start is not None
        else robustness_policy.default_random_seed_start
    )
    robustness_random_seed_count = (
        robustness_random_seed_count
        if robustness_random_seed_count is not None
        else robustness_policy.default_random_seed_count
    )
    robustness_oos_split_ratio = (
        robustness_oos_split_ratio
        if robustness_oos_split_ratio is not None
        else robustness_policy.default_oos_split_ratio
    )
    minimum_component_coverage = (
        minimum_component_coverage
        if minimum_component_coverage is not None
        else backtest_validation_policy.data_credibility.component_coverage_min
    )
    if not benchmark_tickers:
        raise typer.BadParameter("至少需要一个基准标的。")
    if not 0.0 <= minimum_component_coverage <= 1.0:
        raise typer.BadParameter("审计覆盖率阈值必须在 0 到 1 之间。")
    if robustness_shift_days <= 0:
        raise typer.BadParameter("稳健性报告起点后移天数必须为正数。")
    if robustness_cost_stress_bps < 0:
        raise typer.BadParameter("稳健性报告成本压力 bps 不能为负数。")
    if not 0 < robustness_weight_perturbation_pct < 1:
        raise typer.BadParameter("稳健性报告权重扰动比例必须大于 0 且小于 1。")
    if robustness_random_seed_count <= 0:
        raise typer.BadParameter("同换手率随机策略种子数量必须为正数。")
    if not 0 < robustness_oos_split_ratio < 1:
        raise typer.BadParameter("样本外验证 in-sample 切分比例必须大于 0 且小于 1。")
    lag_days = _parse_backtest_lag_days(lag_sensitivity_days)
    should_run_promotion = (
        promotion_report or promotion_report_path is not None or promotion_summary_path is not None
    )
    should_write_promotion_markdown = promotion_report or promotion_report_path is not None
    should_run_robustness = (
        robustness_report
        or robustness_report_path is not None
        or robustness_summary_path is not None
        or should_run_promotion
    )
    should_write_robustness_markdown = robustness_report or robustness_report_path is not None
    should_run_lag_sensitivity = (
        lag_sensitivity_report
        or lag_sensitivity_report_path is not None
        or lag_sensitivity_summary_path is not None
        or should_run_promotion
    )
    should_write_lag_sensitivity_markdown = (
        lag_sensitivity_report or lag_sensitivity_report_path is not None
    )
    rule_governance_report = validate_rule_card_store(
        load_rule_card_store(rule_cards_path),
        as_of=date.today(),
    )
    if not rule_governance_report.passed:
        console.print("[red]规则治理校验失败，已停止回测。[/red]")
        console.print(
            f"错误数：{rule_governance_report.error_count}；"
            f"警告数：{rule_governance_report.warning_count}"
        )
        raise typer.Exit(code=1)
    backtest_rule_version_manifest = build_rule_version_manifest(
        rule_governance_report,
        applies_to="backtest",
    )
    if backtest_rule_version_manifest["production_rule_count"] == 0:
        console.print("[red]未找到适用于 backtest 的 production rule card，已停止回测。[/red]")
        raise typer.Exit(code=1)
    benchmark_policy_report = validate_benchmark_policy(
        load_benchmark_policy(benchmark_policy_path),
        as_of=quality_date,
        selected_strategy_ticker=strategy_ticker,
        selected_benchmark_tickers=tuple(benchmark_tickers),
    )
    if benchmark_policy_report_path is not None:
        write_benchmark_policy_report(benchmark_policy_report, benchmark_policy_report_path)
    if not benchmark_policy_report.passed:
        console.print("[red]基准政策校验失败，已停止回测。[/red]")
        console.print(
            f"错误数：{benchmark_policy_report.error_count}；"
            f"警告数：{benchmark_policy_report.warning_count}"
        )
        if benchmark_policy_report_path is not None:
            console.print(f"基准政策报告：{benchmark_policy_report_path}")
        raise typer.Exit(code=1)

    quality_output = quality_report_path or default_quality_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        quality_date,
    )
    backtest_daily_output = daily_output_path or default_backtest_daily_path(
        PROJECT_ROOT / "outputs" / "backtests",
        start_date,
        end_date,
    )
    backtest_input_coverage_output = (
        input_coverage_output_path
        or default_backtest_input_coverage_path(
            PROJECT_ROOT / "outputs" / "backtests",
            start_date,
            end_date,
        )
    )
    backtest_report_output = report_path or default_backtest_report_path(
        PROJECT_ROOT / "outputs" / "backtests",
        start_date,
        end_date,
    )
    backtest_robustness_output = robustness_report_path or default_backtest_robustness_report_path(
        PROJECT_ROOT / "outputs" / "backtests",
        start_date,
        end_date,
    )
    backtest_robustness_summary_output = robustness_summary_path or (
        backtest_robustness_output.with_suffix(".json")
        if should_write_robustness_markdown
        else default_backtest_robustness_summary_path(
            PROJECT_ROOT / "outputs" / "backtests",
            start_date,
            end_date,
        )
    )
    backtest_lag_sensitivity_output = (
        lag_sensitivity_report_path
        or default_backtest_lag_sensitivity_report_path(
            PROJECT_ROOT / "outputs" / "backtests",
            start_date,
            end_date,
        )
    )
    backtest_lag_sensitivity_summary_output = lag_sensitivity_summary_path or (
        backtest_lag_sensitivity_output.with_suffix(".json")
        if should_write_lag_sensitivity_markdown
        else default_backtest_lag_sensitivity_summary_path(
            PROJECT_ROOT / "outputs" / "backtests",
            start_date,
            end_date,
        )
    )
    backtest_feature_availability_output = (
        feature_availability_report_path
        or default_feature_availability_report_path(
            PROJECT_ROOT / "outputs" / "backtests",
            quality_date,
        )
    )
    backtest_promotion_output = promotion_report_path or default_model_promotion_report_path(
        PROJECT_ROOT / "outputs" / "backtests",
        start_date,
        end_date,
    )
    backtest_promotion_summary_output = promotion_summary_path or (
        backtest_promotion_output.with_suffix(".json")
        if promotion_report or promotion_report_path is not None
        else default_model_promotion_summary_path(
            PROJECT_ROOT / "outputs" / "backtests",
            start_date,
            end_date,
        )
    )
    backtest_audit_output = audit_output_path or default_backtest_audit_report_path(
        PROJECT_ROOT / "outputs" / "backtests",
        start_date,
        end_date,
    )
    backtest_trace_output = trace_bundle_path or default_report_trace_bundle_path(
        backtest_report_output
    )
    sec_companyfacts_validation_output = (
        sec_companyfacts_validation_report_path
        or default_sec_companyfacts_validation_report_path(
            PROJECT_ROOT / "outputs" / "reports",
            quality_date,
        )
    )
    watchlist_lifecycle_report_output = (
        watchlist_lifecycle_report_path
        or default_watchlist_lifecycle_report_path(
            PROJECT_ROOT / "outputs" / "reports",
            quality_date,
        )
    )

    data_quality_report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=list(
            dict.fromkeys(
                [
                    *configured_price_tickers(
                        universe,
                        include_full_ai_chain=full_universe,
                    ),
                    strategy_ticker,
                    *benchmark_tickers,
                ]
            )
        ),
        expected_rate_series=configured_rate_series(universe),
        quality_config=data_quality_config,
        as_of=quality_date,
        manifest_path=_download_manifest_path(prices_path),
        secondary_prices_path=_marketstack_prices_path(prices_path),
        require_secondary_prices=_requires_marketstack_prices(prices_path),
    )
    write_data_quality_report(data_quality_report, quality_output)
    if not data_quality_report.passed:
        console.print("[red]数据质量门禁失败，已停止回测。[/red]")
        console.print(f"数据质量报告：{quality_output}")
        console.print(
            f"错误数：{data_quality_report.error_count}；"
            f"警告数：{data_quality_report.warning_count}"
        )
        raise typer.Exit(code=1)

    prices_frame = pd.read_csv(prices_path)
    rates_frame = pd.read_csv(rates_path)
    fundamental_source_id = (
        "sec_edgar_reconstructed_pit_features"
        if sec_fundamental_source == "sec_pit_feature_panel"
        else "sec_fundamental_features"
    )
    feature_source_checks = list(
        _market_feature_source_checks(
            prices_frame=prices_frame,
            rates_frame=rates_frame,
            prices_path=prices_path,
            rates_path=rates_path,
            decision_time=quality_date,
        )
    )
    if sec_fundamental_source == "sec_pit_feature_panel":
        feature_source_checks.append(
            _sec_pit_feature_source_check(
                feature_panel_path=sec_pit_feature_panel_path,
                decision_time=quality_date,
            )
        )
    backtest_feature_availability_report = build_feature_availability_report(
        input_path=feature_availability_path,
        as_of=quality_date,
        observed_sources=(
            "prices_daily",
            "rates_daily",
            "watchlist_lifecycle",
            fundamental_source_id,
            "valuation_snapshots",
            "risk_event_occurrences",
        ),
        required_sources=(
            "prices_daily",
            "rates_daily",
            "watchlist_lifecycle",
            fundamental_source_id,
            "valuation_snapshots",
            "risk_event_occurrences",
        ),
        source_checks=tuple(feature_source_checks),
    )
    write_feature_availability_report(
        backtest_feature_availability_report,
        backtest_feature_availability_output,
    )
    if not backtest_feature_availability_report.passed:
        console.print("[red]PIT 特征可见时间校验失败，已停止回测。[/red]")
        console.print(f"PIT 特征可见时间报告：{backtest_feature_availability_output}")
        console.print(
            f"错误数：{backtest_feature_availability_report.error_count}；"
            f"警告数：{backtest_feature_availability_report.warning_count}"
        )
        raise typer.Exit(code=1)
    backtest_feature_availability_section = render_feature_availability_section(
        backtest_feature_availability_report,
        backtest_feature_availability_output,
    )

    watchlist_lifecycle_report = validate_watchlist_lifecycle(
        lifecycle=watchlist_lifecycle,
        input_path=watchlist_lifecycle_path,
        watchlist=watchlist,
        universe=universe,
        as_of=quality_date,
    )
    write_watchlist_lifecycle_report(
        watchlist_lifecycle_report,
        watchlist_lifecycle_report_output,
    )
    if not watchlist_lifecycle_report.passed:
        console.print("[red]观察池 lifecycle 校验失败，已停止回测。[/red]")
        console.print(f"观察池 lifecycle 报告：{watchlist_lifecycle_report_output}")
        console.print(
            f"错误数：{watchlist_lifecycle_report.error_count}；"
            f"警告数：{watchlist_lifecycle_report.warning_count}"
        )
        raise typer.Exit(code=1)

    prices_frame = pd.read_csv(prices_path)
    rates_frame = pd.read_csv(rates_path)
    signal_dates = _backtest_signal_dates(
        prices=prices_frame,
        strategy_ticker=strategy_ticker,
        start=start_date,
        end=end_date,
    )
    input_signal_dates = (
        _backtest_required_input_signal_dates(
            prices=prices_frame,
            strategy_ticker=strategy_ticker,
            start=start_date,
            end=end_date,
            lag_days=lag_days,
        )
        if should_run_lag_sensitivity
        else signal_dates
    )
    sec_fundamental_feature_reports = (
        _build_backtest_sec_pit_feature_reports(
            signal_dates=input_signal_dates,
            sec_companies_path=sec_companies_path,
            sec_pit_feature_panel_path=sec_pit_feature_panel_path,
        )
        if sec_fundamental_source == "sec_pit_feature_panel"
        else _build_backtest_sec_fundamental_feature_reports(
            signal_dates=input_signal_dates,
            sec_companies_path=sec_companies_path,
            sec_metrics_path=sec_metrics_path,
            fundamental_feature_config_path=fundamental_feature_config_path,
            sec_companyfacts_dir=sec_companyfacts_dir,
            tsm_ir_input_path=tsm_ir_input_path,
            validation_as_of=quality_date,
            validation_report_output=sec_companyfacts_validation_output,
        )
    )
    fundamental_validation_report_output = (
        None
        if sec_fundamental_source == "sec_pit_feature_panel"
        else sec_companyfacts_validation_output
    )
    valuation_review_reports = _build_backtest_valuation_review_reports(
        signal_dates=input_signal_dates,
        valuation_path=valuation_path,
        universe=universe,
        watchlist=watchlist,
    )
    risk_event_occurrence_review_reports = _build_backtest_risk_event_occurrence_review_reports(
        signal_dates=input_signal_dates,
        risk_events_path=risk_events_path,
        risk_event_occurrences_path=risk_event_occurrences_path,
        universe=universe,
        industry_chain=industry_chain,
        watchlist=watchlist,
        validation_as_of=quality_date,
    )

    backtest_regime_context = BacktestRegimeContext(
        regime_id=selected_regime.regime_id,
        name=selected_regime.name,
        start_date=selected_regime.start_date,
        anchor_date=selected_regime.anchor_date,
        anchor_event=selected_regime.anchor_event,
        description=selected_regime.description,
    )
    base_prepared_context = prepare_daily_score_backtest_context(
        prices=prices_frame,
        rates=rates_frame,
        feature_config=feature_config,
        data_quality_report=data_quality_report,
        core_watchlist=universe.ai_chain.get("core_watchlist", []),
        start=start_date,
        end=end_date,
        strategy_ticker=strategy_ticker,
        benchmark_tickers=tuple(benchmark_tickers),
        market_regime=backtest_regime_context,
        fundamental_feature_reports=sec_fundamental_feature_reports,
        valuation_review_reports=valuation_review_reports,
        risk_event_occurrence_review_reports=risk_event_occurrence_review_reports,
        watchlist_lifecycle=watchlist_lifecycle,
        benchmark_policy_report=benchmark_policy_report,
    )

    def run_configured_backtest(
        *,
        scenario_start: date,
        scenario_cost_bps: float,
        scenario_spread_bps: float,
        scenario_slippage_bps: float,
        scenario_market_impact_bps: float,
        scenario_tax_bps: float,
        scenario_fx_bps: float,
        scenario_financing_annual_bps: float,
        scenario_etf_delay_bps: float,
        scenario_end: date | None = None,
        scenario_feature_lag_days: int = 0,
        scenario_universe_lag_days: int = 0,
        scenario_scoring_rules: ScoringRulesConfig | None = None,
        scenario_weight_multipliers: dict[str, float] | None = None,
    ) -> DailyBacktestResult:
        prepared_context = (
            base_prepared_context
            if scenario_feature_lag_days == 0 and scenario_universe_lag_days == 0
            else None
        )
        return run_daily_score_backtest(
            prices=prices_frame,
            rates=rates_frame,
            feature_config=feature_config,
            scoring_rules=(
                scoring_rules if scenario_scoring_rules is None else scenario_scoring_rules
            ),
            portfolio_config=portfolio,
            data_quality_report=data_quality_report,
            core_watchlist=universe.ai_chain.get("core_watchlist", []),
            start=scenario_start,
            end=end_date if scenario_end is None else scenario_end,
            strategy_ticker=strategy_ticker,
            benchmark_tickers=tuple(benchmark_tickers),
            cost_bps=scenario_cost_bps,
            spread_bps=scenario_spread_bps,
            slippage_bps=scenario_slippage_bps,
            market_impact_bps=scenario_market_impact_bps,
            tax_bps=scenario_tax_bps,
            fx_bps=scenario_fx_bps,
            financing_annual_bps=scenario_financing_annual_bps,
            etf_delay_bps=scenario_etf_delay_bps,
            fundamental_feature_reports=sec_fundamental_feature_reports,
            valuation_review_reports=valuation_review_reports,
            risk_event_occurrence_review_reports=risk_event_occurrence_review_reports,
            watchlist_lifecycle=watchlist_lifecycle,
            industry_chain=industry_chain,
            watchlist=watchlist,
            benchmark_policy_report=benchmark_policy_report,
            market_regime=backtest_regime_context,
            feature_lag_days=scenario_feature_lag_days,
            universe_lag_days=scenario_universe_lag_days,
            prepared_context=prepared_context,
            weight_profile=weight_profile,
            calibration_overlays=calibration_overlays,
            weight_multipliers=scenario_weight_multipliers,
        )

    result = run_configured_backtest(
        scenario_start=start_date,
        scenario_cost_bps=cost_bps,
        scenario_spread_bps=spread_bps,
        scenario_slippage_bps=slippage_bps,
        scenario_market_impact_bps=market_impact_bps,
        scenario_tax_bps=tax_bps,
        scenario_fx_bps=fx_bps,
        scenario_financing_annual_bps=financing_annual_bps,
        scenario_etf_delay_bps=etf_delay_bps,
    )
    daily_output = write_backtest_daily_csv(result, backtest_daily_output)
    input_coverage_output = write_backtest_input_coverage_csv(
        result,
        backtest_input_coverage_output,
    )
    audit_report = build_backtest_audit_report(
        result=result,
        data_quality_report_path=quality_output,
        backtest_report_path=backtest_report_output,
        daily_output_path=daily_output,
        input_coverage_output_path=input_coverage_output,
        sec_companyfacts_validation_report_path=fundamental_validation_report_output,
        minimum_component_coverage=minimum_component_coverage,
    )
    audit_output = write_backtest_audit_report(audit_report, backtest_audit_output)
    backtest_trace_bundle = build_backtest_trace_bundle(
        result=result,
        audit_report=audit_report,
        report_path=backtest_report_output,
        data_quality_report_path=quality_output,
        daily_output_path=daily_output,
        input_coverage_output_path=input_coverage_output,
        audit_report_path=audit_output,
        config_paths=_backtest_trace_config_paths(
            regimes_path=regimes_path,
            benchmark_policy_path=benchmark_policy_path,
            sec_companies_path=sec_companies_path,
            sec_metrics_path=sec_metrics_path,
            fundamental_feature_config_path=fundamental_feature_config_path,
            risk_events_path=risk_events_path,
            watchlist_lifecycle_path=watchlist_lifecycle_path,
            rule_cards_path=rule_cards_path,
            feature_availability_path=feature_availability_path,
        ),
        rule_version_manifest=backtest_rule_version_manifest,
        sec_companyfacts_validation_report_path=fundamental_validation_report_output,
        feature_availability_report_path=backtest_feature_availability_output,
        feature_availability_summary=feature_availability_summary_record(
            backtest_feature_availability_report,
            backtest_feature_availability_output,
        ),
    )
    backtest_trace_output = write_trace_bundle(
        backtest_trace_bundle,
        backtest_trace_output,
    )
    report_output = write_backtest_report(
        result,
        data_quality_report_path=quality_output,
        daily_output_path=daily_output,
        output_path=backtest_report_output,
        sec_companyfacts_validation_report_path=fundamental_validation_report_output,
        input_coverage_output_path=input_coverage_output,
        audit_report_path=audit_output,
        feature_availability_section=backtest_feature_availability_section,
        traceability_section=render_traceability_section(
            backtest_trace_bundle,
            backtest_trace_output,
        ),
    )
    robustness_report_data = None
    robustness_output = None
    robustness_summary_output = None
    if should_run_robustness:
        configured_position_bands = _configured_position_band_rules(scoring_rules)
        cost_stress_result = run_configured_backtest(
            scenario_start=start_date,
            scenario_cost_bps=cost_bps + robustness_cost_stress_bps,
            scenario_spread_bps=spread_bps + robustness_cost_stress_bps,
            scenario_slippage_bps=slippage_bps + robustness_cost_stress_bps,
            scenario_market_impact_bps=market_impact_bps + robustness_cost_stress_bps,
            scenario_tax_bps=tax_bps,
            scenario_fx_bps=fx_bps,
            scenario_financing_annual_bps=financing_annual_bps,
            scenario_etf_delay_bps=etf_delay_bps + robustness_cost_stress_bps,
        )
        robustness_scenarios = [
            BacktestRobustnessScenario(
                scenario_id="cost_stress_execution",
                label="成本压力",
                category="cost",
                description=(
                    "commission、spread、slippage、market impact 和 ETF delay "
                    f"各增加 {robustness_cost_stress_bps:.1f} bps；税费、FX "
                    "和融资保持基础假设；复用缓存 PIT 输入并调用同一回测执行路径。"
                ),
                result=cost_stress_result,
            ),
            fixed_total_asset_exposure_scenario(
                result,
                exposure=robustness_policy.fixed_total_asset_exposure,
            ),
            volatility_targeted_exposure_scenario(
                result,
                target_annual_volatility=(robustness_policy.volatility_target_annual_volatility),
                lookback_days=robustness_policy.volatility_target_lookback_days,
                fallback_exposure=robustness_policy.fixed_total_asset_exposure,
            ),
            model_target_no_gate_scenario(result),
        ]
        for interval_days in robustness_policy.rebalance_intervals:
            robustness_scenarios.append(
                rebalance_interval_scenario(result, interval_days=interval_days)
            )
        robustness_scenarios.extend(
            [
                module_subset_baseline_scenario(
                    result,
                    scenario_id="trend_only_baseline",
                    label="趋势-only 基线",
                    modules=("trend",),
                    weights=scoring_rules.weights,
                    position_bands=configured_position_bands,
                ),
                module_subset_baseline_scenario(
                    result,
                    scenario_id="trend_plus_risk_sentiment_baseline",
                    label="趋势 + 风险情绪基线",
                    modules=("trend", "risk_sentiment"),
                    weights=scoring_rules.weights,
                    position_bands=configured_position_bands,
                ),
                module_subset_baseline_scenario(
                    result,
                    scenario_id="alpha_only_score_baseline",
                    label="Alpha-only score 基线",
                    modules=("trend", "fundamentals"),
                    weights=scoring_rules.weights,
                    position_bands=configured_position_bands,
                    category="score_architecture_baseline",
                ),
                module_subset_baseline_scenario(
                    result,
                    scenario_id="risk_state_only_score_baseline",
                    label="Risk-state-only score 基线",
                    modules=("macro_liquidity", "risk_sentiment"),
                    weights=scoring_rules.weights,
                    position_bands=configured_position_bands,
                    category="score_architecture_baseline",
                ),
                module_subset_baseline_scenario(
                    result,
                    scenario_id="gate_modules_score_baseline",
                    label="Gate modules score 基线",
                    modules=("valuation", "policy_geopolitics"),
                    weights=scoring_rules.weights,
                    position_bands=configured_position_bands,
                    category="score_architecture_baseline",
                ),
            ]
        )
        for module_name in scoring_rules.weights:
            for direction, multiplier in (
                ("down", 1.0 - robustness_weight_perturbation_pct),
                ("up", 1.0 + robustness_weight_perturbation_pct),
            ):
                direction_label = "下调" if direction == "down" else "上调"
                scenario_result = run_configured_backtest(
                    scenario_start=start_date,
                    scenario_cost_bps=cost_bps,
                    scenario_spread_bps=spread_bps,
                    scenario_slippage_bps=slippage_bps,
                    scenario_market_impact_bps=market_impact_bps,
                    scenario_tax_bps=tax_bps,
                    scenario_fx_bps=fx_bps,
                    scenario_financing_annual_bps=financing_annual_bps,
                    scenario_etf_delay_bps=etf_delay_bps,
                    scenario_weight_multipliers={module_name: multiplier},
                )
                robustness_scenarios.append(
                    BacktestRobustnessScenario(
                        scenario_id=_weight_perturbation_scenario_id(
                            module_name,
                            direction,
                            robustness_weight_perturbation_pct,
                        ),
                        label=(
                            f"{module_name} 权重{direction_label} "
                            f"{robustness_weight_perturbation_pct:.0%}"
                        ),
                        category="module_weight_perturbation",
                        description=(
                            f"将 {module_name} 模块权重{direction_label} "
                            f"{robustness_weight_perturbation_pct:.0%}；"
                            "复用缓存 PIT 输入并调用同一评分/回测执行路径。"
                        ),
                        result=scenario_result,
                    )
                )
        shifted_start = start_date + timedelta(days=robustness_shift_days)
        for seed in range(
            robustness_random_seed_start,
            robustness_random_seed_start + robustness_random_seed_count,
        ):
            robustness_scenarios.append(same_turnover_random_scenario(result, seed=seed))
            robustness_scenarios.append(same_exposure_random_scenario(result, seed=seed))
        if shifted_start >= result.last_signal_date:
            robustness_scenarios.append(
                BacktestRobustnessScenario(
                    scenario_id="shifted_start",
                    label="起点后移",
                    category="window",
                    description="将请求起点后移后复用相同 point-in-time 输入。",
                    skipped_reason=(
                        f"后移 {robustness_shift_days} 天后的起点 "
                        f"{shifted_start.isoformat()} 已不早于基础回测最后信号日 "
                        f"{result.last_signal_date.isoformat()}。"
                    ),
                )
            )
        else:
            shifted_result = run_configured_backtest(
                scenario_start=shifted_start,
                scenario_cost_bps=cost_bps,
                scenario_spread_bps=spread_bps,
                scenario_slippage_bps=slippage_bps,
                scenario_market_impact_bps=market_impact_bps,
                scenario_tax_bps=tax_bps,
                scenario_fx_bps=fx_bps,
                scenario_financing_annual_bps=financing_annual_bps,
                scenario_etf_delay_bps=etf_delay_bps,
            )
            robustness_scenarios.append(
                BacktestRobustnessScenario(
                    scenario_id="shifted_start",
                    label="起点后移",
                    category="window",
                    description=(
                        f"将请求起点后移 {robustness_shift_days} 天，"
                        "复用缓存 PIT 输入并调用同一评分/回测执行路径。"
                    ),
                    result=shifted_result,
                )
            )
        oos_split = _backtest_oos_split_dates(
            signal_dates,
            split_ratio=robustness_oos_split_ratio,
        )
        if oos_split is None:
            skipped_reason = (
                "基础信号样本不足，无法按时间顺序切出至少 5 个 in-sample "
                "和 5 个 out-of-sample 信号日。"
            )
            robustness_scenarios.extend(
                [
                    BacktestRobustnessScenario(
                        scenario_id="in_sample_window",
                        label="in-sample 窗口",
                        category="out_of_sample_validation",
                        description="时间顺序样本外验证的前段窗口。",
                        skipped_reason=skipped_reason,
                    ),
                    BacktestRobustnessScenario(
                        scenario_id="out_of_sample_holdout",
                        label="out-of-sample holdout",
                        category="out_of_sample_validation",
                        description="时间顺序样本外验证的后段 holdout。",
                        skipped_reason=skipped_reason,
                    ),
                ]
            )
        else:
            in_sample_end, out_of_sample_start = oos_split
            in_sample_result = run_configured_backtest(
                scenario_start=start_date,
                scenario_end=in_sample_end,
                scenario_cost_bps=cost_bps,
                scenario_spread_bps=spread_bps,
                scenario_slippage_bps=slippage_bps,
                scenario_market_impact_bps=market_impact_bps,
                scenario_tax_bps=tax_bps,
                scenario_fx_bps=fx_bps,
                scenario_financing_annual_bps=financing_annual_bps,
                scenario_etf_delay_bps=etf_delay_bps,
            )
            out_of_sample_result = run_configured_backtest(
                scenario_start=out_of_sample_start,
                scenario_end=end_date,
                scenario_cost_bps=cost_bps,
                scenario_spread_bps=spread_bps,
                scenario_slippage_bps=slippage_bps,
                scenario_market_impact_bps=market_impact_bps,
                scenario_tax_bps=tax_bps,
                scenario_fx_bps=fx_bps,
                scenario_financing_annual_bps=financing_annual_bps,
                scenario_etf_delay_bps=etf_delay_bps,
            )
            robustness_scenarios.extend(
                [
                    BacktestRobustnessScenario(
                        scenario_id="in_sample_window",
                        label="in-sample 窗口",
                        category="out_of_sample_validation",
                        description=(
                            f"按 {robustness_oos_split_ratio:.0%} 时间顺序切分的"
                            f"前段窗口，区间截至 {in_sample_end.isoformat()}；"
                            "复用缓存 PIT 输入并调用同一评分/回测执行路径。"
                        ),
                        result=in_sample_result,
                    ),
                    BacktestRobustnessScenario(
                        scenario_id="out_of_sample_holdout",
                        label="out-of-sample holdout",
                        category="out_of_sample_validation",
                        description=(
                            f"按 {robustness_oos_split_ratio:.0%} 时间顺序切分的"
                            f"后段 holdout，起点为 {out_of_sample_start.isoformat()}；"
                            "复用缓存 PIT 输入并调用同一评分/回测执行路径。"
                        ),
                        result=out_of_sample_result,
                    ),
                ]
            )
        robustness_report_data = BacktestRobustnessReport(
            base_result=result,
            scenarios=tuple(robustness_scenarios),
            cost_stress_increment_bps=robustness_cost_stress_bps,
            shifted_start_days=robustness_shift_days,
            weight_perturbation_pct=robustness_weight_perturbation_pct,
            random_seed_start=robustness_random_seed_start,
            random_seed_count=robustness_random_seed_count,
            oos_split_ratio=robustness_oos_split_ratio,
            policy_metadata=backtest_validation_policy.policy_metadata.model_dump(mode="json"),
            policy=robustness_policy,
        )
        if should_write_robustness_markdown:
            robustness_output = write_backtest_robustness_report(
                robustness_report_data,
                backtest_robustness_output,
            )
        robustness_summary_output = write_backtest_robustness_summary(
            robustness_report_data,
            backtest_robustness_summary_output,
        )

    lag_sensitivity_report_data = None
    lag_sensitivity_output = None
    lag_sensitivity_summary_output = None
    if should_run_lag_sensitivity:
        lag_scenarios: list[BacktestLagSensitivityScenario] = []
        for lag_day in lag_days:
            if lag_day == 0:
                continue
            for feature_lag_days, universe_lag_days in (
                (lag_day, 0),
                (0, lag_day),
                (lag_day, lag_day),
            ):
                try:
                    scenario_result = run_configured_backtest(
                        scenario_start=start_date,
                        scenario_cost_bps=cost_bps,
                        scenario_spread_bps=spread_bps,
                        scenario_slippage_bps=slippage_bps,
                        scenario_market_impact_bps=market_impact_bps,
                        scenario_tax_bps=tax_bps,
                        scenario_fx_bps=fx_bps,
                        scenario_financing_annual_bps=financing_annual_bps,
                        scenario_etf_delay_bps=etf_delay_bps,
                        scenario_feature_lag_days=feature_lag_days,
                        scenario_universe_lag_days=universe_lag_days,
                    )
                    lag_scenarios.append(
                        BacktestLagSensitivityScenario(
                            feature_lag_days=feature_lag_days,
                            universe_lag_days=universe_lag_days,
                            rebalance_delay_days=1,
                            result=scenario_result,
                        )
                    )
                except ValueError as exc:
                    lag_scenarios.append(
                        BacktestLagSensitivityScenario(
                            feature_lag_days=feature_lag_days,
                            universe_lag_days=universe_lag_days,
                            rebalance_delay_days=1,
                            skipped_reason=str(exc),
                        )
                    )
        lag_sensitivity_report_data = BacktestLagSensitivityReport(
            base_result=result,
            scenarios=tuple(lag_scenarios),
            tested_lag_days=lag_days,
        )
        if should_write_lag_sensitivity_markdown:
            lag_sensitivity_output = write_backtest_lag_sensitivity_report(
                lag_sensitivity_report_data,
                backtest_lag_sensitivity_output,
            )
        lag_sensitivity_summary_output = write_backtest_lag_sensitivity_summary(
            lag_sensitivity_report_data,
            backtest_lag_sensitivity_summary_output,
        )

    promotion_report_data = None
    promotion_output = None
    promotion_summary_output = None
    if should_run_promotion:
        promotion_report_data = build_model_promotion_report(
            result=result,
            as_of=quality_date,
            robustness_report=robustness_report_data,
            robustness_report_path=robustness_output or robustness_summary_output,
            lag_sensitivity_report=lag_sensitivity_report_data,
            lag_sensitivity_report_path=(lag_sensitivity_output or lag_sensitivity_summary_output),
            prediction_outcomes_path=promotion_prediction_outcomes_path,
            rule_governance_status=rule_governance_report.status,
            promotion_policy=backtest_validation_policy.promotion,
            policy_metadata=backtest_validation_policy.policy_metadata.model_dump(mode="json"),
        )
        if should_write_promotion_markdown:
            promotion_output = write_model_promotion_report(
                promotion_report_data,
                backtest_promotion_output,
            )
        promotion_summary_output = write_model_promotion_summary(
            promotion_report_data,
            backtest_promotion_summary_output,
        )
        report_output = write_backtest_report(
            result,
            data_quality_report_path=quality_output,
            daily_output_path=daily_output,
            output_path=backtest_report_output,
            sec_companyfacts_validation_report_path=fundamental_validation_report_output,
            input_coverage_output_path=input_coverage_output,
            audit_report_path=audit_output,
            feature_availability_section=backtest_feature_availability_section,
            promotion_gate_section=render_model_promotion_report(promotion_report_data).replace(
                "# 模型晋级门槛报告", "## 模型晋级门槛", 1
            ),
            traceability_section=render_traceability_section(
                backtest_trace_bundle,
                backtest_trace_output,
            ),
        )

    console.print(f"[yellow]回测状态：{result.status}[/yellow]")
    audit_style = "green" if audit_report.status == "PASS" else "yellow"
    console.print(f"[{audit_style}]输入审计状态：{audit_report.status}[/{audit_style}]")
    if result.market_regime is not None:
        console.print(f"市场阶段：{result.market_regime.name}（{result.market_regime.regime_id}）")
    console.print(f"策略总收益：{result.strategy_metrics.total_return:.1%}")
    console.print(f"策略 CAGR：{result.strategy_metrics.cagr:.1%}")
    console.print(f"策略最大回撤：{result.strategy_metrics.max_drawdown:.1%}")
    console.print(f"回测报告：{report_output}")
    if robustness_output is not None:
        console.print(f"稳健性报告：{robustness_output}")
    if robustness_summary_output is not None:
        console.print(f"稳健性摘要：{robustness_summary_output}")
    if lag_sensitivity_output is not None:
        console.print(f"滞后敏感性报告：{lag_sensitivity_output}")
    if lag_sensitivity_summary_output is not None:
        console.print(f"滞后敏感性摘要：{lag_sensitivity_summary_output}")
    if promotion_output is not None:
        console.print(f"模型晋级门槛报告：{promotion_output}")
    if promotion_summary_output is not None:
        console.print(f"模型晋级门槛摘要：{promotion_summary_output}")
    console.print(
        f"PIT 特征可见时间报告：{backtest_feature_availability_output}"
        f"（{backtest_feature_availability_report.status}）"
    )
    console.print(f"观察池 lifecycle 报告：{watchlist_lifecycle_report_output}")
    console.print(f"Evidence bundle：{backtest_trace_output}")
    console.print(f"输入审计报告：{audit_output}")
    console.print(f"每日明细：{daily_output}")
    console.print(f"历史输入覆盖诊断：{input_coverage_output}")
    console.print(f"SEC 基本面切片：{result.fundamental_feature_report_count} 个 signal_date")
    console.print(f"估值快照切片：{result.valuation_review_report_count} 个 signal_date")
    console.print(
        "风险事件发生记录切片："
        f"{result.risk_event_occurrence_review_report_count} 个 signal_date"
    )
    if sec_fundamental_source == "sec_pit_feature_panel":
        console.print(f"SEC PIT feature panel：{sec_pit_feature_panel_path}")
    else:
        console.print(f"SEC companyfacts 校验报告：{sec_companyfacts_validation_output}")
    console.print(f"数据质量报告：{quality_output}（{data_quality_report.status}）")
    console.print(
        "规则版本："
        f"{backtest_rule_version_manifest['production_rule_count']} 个 production rule cards"
        f"（{rule_governance_report.status}）"
    )
    console.print(f"基准政策状态：{benchmark_policy_report.status}")
    if benchmark_policy_report_path is not None:
        console.print(f"基准政策报告：{benchmark_policy_report_path}")
    if fail_on_audit_warning and audit_report.status != "PASS":
        console.print("[red]输入审计未达到 PASS，严格审计门禁已返回失败。[/red]")
        raise typer.Exit(code=1)


@app.command("backtest-gate-attribution")
def backtest_gate_attribution(
    backtest_daily_path: Annotated[
        Path | None,
        typer.Option(help="backtest_daily CSV 路径；默认使用 outputs/backtests 最新文件。"),
    ] = None,
    input_coverage_path: Annotated[
        Path | None,
        typer.Option(help="backtest_input_coverage CSV 路径；默认按 daily 文件名推断。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 归因报告输出路径。"),
    ] = None,
    as_of: Annotated[
        str | None,
        typer.Option(help="报告日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    left_tail_threshold: Annotated[
        float,
        typer.Option(help="左尾收益阈值，例如 -0.03 表示 -3%。"),
    ] = -0.03,
) -> None:
    """基于已生成回测 CSV 输出 gate 与事件效果归因报告。"""
    report_date = _parse_date(as_of) if as_of else date.today()
    selected_daily_path = backtest_daily_path or _latest_backtest_daily_path(
        PROJECT_ROOT / "outputs" / "backtests"
    )
    if selected_daily_path is None:
        console.print("[red]未找到 backtest_daily_*.csv；请先运行 aits backtest。[/red]")
        raise typer.Exit(code=1)
    selected_coverage_path = input_coverage_path or infer_input_coverage_path(selected_daily_path)
    report = build_gate_event_attribution_report(
        backtest_daily_path=selected_daily_path,
        input_coverage_path=selected_coverage_path,
        as_of=report_date,
        left_tail_threshold=left_tail_threshold,
    )
    report_output = output_path or default_gate_event_attribution_report_path(
        PROJECT_ROOT / "outputs" / "backtests",
        report,
    )
    report_output = write_gate_event_attribution_report(report, report_output)
    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]Gate/event 归因状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_output}")
    console.print(
        f"Gate 数：{len(report.gate_rows)}；"
        f"事件记录：{report.event_summary.risk_event_record_count}"
    )
    console.print("治理边界：本命令只读解释历史样本，不改变回测、评分或仓位闸门。")
    if not report.passed:
        raise typer.Exit(code=1)


@app.command("backtest-input-gaps")
def backtest_input_gaps(
    prices_path: Annotated[
        Path,
        typer.Option(help="标准化日线价格 CSV 路径，用于确定回测 signal_date。"),
    ] = PROJECT_ROOT
    / "data"
    / "raw"
    / "prices_daily.csv",
    rates_path: Annotated[
        Path,
        typer.Option(help="标准化日线利率 CSV 路径，用于数据质量门禁。"),
    ] = PROJECT_ROOT
    / "data"
    / "raw"
    / "rates_daily.csv",
    start: Annotated[
        str | None,
        typer.Option(
            "--from",
            help="诊断开始日期，格式为 YYYY-MM-DD；未提供时使用所选市场阶段起点。",
        ),
    ] = None,
    end: Annotated[
        str | None,
        typer.Option("--to", help="诊断结束日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    strategy_ticker: Annotated[
        str,
        typer.Option(help="用于确定回测信号日的策略代理标的。"),
    ] = "SMH",
    regime: Annotated[
        str | None,
        typer.Option(
            "--regime",
            help="市场阶段 ID，默认使用 config/market_regimes.yaml 的 default_backtest_regime。",
        ),
    ] = None,
    regimes_path: Annotated[
        Path,
        typer.Option(help="市场阶段配置文件路径。"),
    ] = DEFAULT_MARKET_REGIMES_CONFIG_PATH,
    valuation_path: Annotated[
        Path,
        typer.Option(help="估值快照 YAML 文件或目录路径，用于历史覆盖诊断。"),
    ] = PROJECT_ROOT
    / "data"
    / "external"
    / "valuation_snapshots",
    risk_events_path: Annotated[
        Path,
        typer.Option(help="风险事件配置路径，用于历史覆盖诊断。"),
    ] = DEFAULT_RISK_EVENTS_CONFIG_PATH,
    risk_event_occurrences_path: Annotated[
        Path,
        typer.Option(help="风险事件发生记录 YAML 文件或目录路径，用于历史覆盖诊断。"),
    ] = DEFAULT_RISK_EVENT_OCCURRENCES_PATH,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 历史输入缺口报告输出路径。"),
    ] = None,
    quality_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 数据质量报告输出路径。"),
    ] = None,
    quality_as_of: Annotated[
        str | None,
        typer.Option(help="数据质量校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    full_universe: Annotated[
        bool,
        typer.Option(
            "--full-universe",
            help="校验配置中的完整 AI 产业链标的，而不只校验核心观察池。",
        ),
    ] = False,
) -> None:
    """诊断回测所需历史估值和风险事件输入覆盖缺口。"""
    universe = load_universe()
    industry_chain = load_industry_chain()
    watchlist = load_watchlist()
    quality_config = load_data_quality()
    market_regimes = load_market_regimes(regimes_path)
    selected_regime_id = regime or market_regimes.default_backtest_regime
    try:
        selected_regime = market_regime_by_id(market_regimes, selected_regime_id)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc

    start_date = _parse_date(start) if start else selected_regime.start_date
    end_date = _parse_date(end) if end else date.today()
    quality_date = _parse_date(quality_as_of) if quality_as_of else date.today()
    quality_output = quality_report_path or default_quality_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        quality_date,
    )
    gap_output = output_path or default_backtest_input_gap_report_path(
        PROJECT_ROOT / "outputs" / "backtests",
        start_date,
        end_date,
    )
    data_quality_report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=list(
            dict.fromkeys(
                [
                    *configured_price_tickers(
                        universe,
                        include_full_ai_chain=full_universe,
                    ),
                    strategy_ticker,
                ]
            )
        ),
        expected_rate_series=configured_rate_series(universe),
        quality_config=quality_config,
        as_of=quality_date,
        manifest_path=_download_manifest_path(prices_path),
        secondary_prices_path=_marketstack_prices_path(prices_path),
        require_secondary_prices=_requires_marketstack_prices(prices_path),
    )
    write_data_quality_report(data_quality_report, quality_output)
    if not data_quality_report.passed:
        console.print("[red]数据质量门禁失败，已停止历史输入缺口诊断。[/red]")
        console.print(f"数据质量报告：{quality_output}")
        console.print(
            f"错误数：{data_quality_report.error_count}；"
            f"警告数：{data_quality_report.warning_count}"
        )
        raise typer.Exit(code=1)

    prices_frame = pd.read_csv(prices_path)
    signal_dates = _backtest_signal_dates(
        prices=prices_frame,
        strategy_ticker=strategy_ticker,
        start=start_date,
        end=end_date,
    )
    valuation_reports = _build_backtest_valuation_review_reports(
        signal_dates=signal_dates,
        valuation_path=valuation_path,
        universe=universe,
        watchlist=watchlist,
    )
    risk_event_reports = _build_backtest_risk_event_occurrence_review_reports(
        signal_dates=signal_dates,
        risk_events_path=risk_events_path,
        risk_event_occurrences_path=risk_event_occurrences_path,
        universe=universe,
        industry_chain=industry_chain,
        watchlist=watchlist,
        validation_as_of=quality_date,
    )
    report = build_backtest_input_gap_report(
        signal_dates=signal_dates,
        requested_start=start_date,
        requested_end=end_date,
        valuation_reports=valuation_reports,
        risk_event_reports=risk_event_reports,
        valuation_path=valuation_path,
        risk_event_occurrences_path=risk_event_occurrences_path,
        market_regime=BacktestRegimeContext(
            regime_id=selected_regime.regime_id,
            name=selected_regime.name,
            start_date=selected_regime.start_date,
            anchor_date=selected_regime.anchor_date,
            anchor_event=selected_regime.anchor_event,
            description=selected_regime.description,
        ),
    )
    report_output = write_backtest_input_gap_report(report, gap_output)

    status_style = "green" if report.status == "PASS" else "yellow"
    console.print(f"[{status_style}]历史输入缺口状态：{report.status}[/{status_style}]")
    console.print(f"信号日数量：{len(report.signal_dates)}")
    console.print(f"估值缺口信号日：{report.valuation_gap_count}")
    console.print(f"风险事件/复核声明缺口信号日：{report.risk_event_gap_count}")
    console.print(f"历史输入缺口报告：{report_output}")
    console.print(f"数据质量报告：{quality_output}（{data_quality_report.status}）")


@app.command("backtest-pit-coverage")
def backtest_pit_coverage(
    manifest_path: Annotated[
        Path,
        typer.Option(help="PIT raw snapshot manifest CSV 路径。"),
    ] = DEFAULT_PIT_SNAPSHOT_MANIFEST_PATH,
    data_sources_path: Annotated[
        Path,
        typer.Option(help="数据源目录 YAML 路径，用于校验授权和 provider 信息。"),
    ] = DEFAULT_DATA_SOURCES_CONFIG_PATH,
    as_of: Annotated[
        str | None,
        typer.Option(help="评估日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown forward-only PIT 覆盖报告输出路径。"),
    ] = None,
    min_forward_days: Annotated[
        int,
        typer.Option(help="升级为 B 级 forward-only 样本所需的最小覆盖日期数。"),
    ] = 60,
    max_staleness_days: Annotated[
        int,
        typer.Option(help="最新快照最大允许日龄，超出后保持 C 级或警告。"),
    ] = 3,
) -> None:
    """评估 forward-only PIT 快照积累进度和回测输入等级升级日期。"""
    coverage_date = _parse_date(as_of) if as_of else date.today()
    coverage_output = output_path or default_backtest_pit_coverage_report_path(
        PROJECT_ROOT / "outputs" / "backtests",
        coverage_date,
    )
    if min_forward_days <= 0:
        raise typer.BadParameter("B 级最小覆盖日期数必须为正数。")
    if max_staleness_days < 0:
        raise typer.BadParameter("最新快照最大允许日龄不能为负数。")

    validation_report = validate_pit_snapshot_manifest(
        input_path=manifest_path,
        as_of=coverage_date,
        data_sources=load_data_sources(data_sources_path),
    )
    report = build_backtest_pit_coverage_report(
        validation_report,
        min_forward_days=min_forward_days,
        max_staleness_days=max_staleness_days,
    )
    report_output = write_backtest_pit_coverage_report(report, coverage_output)

    status_style = (
        "green" if report.status == "PASS" else "yellow" if report.status != "FAIL" else "red"
    )
    console.print(f"[{status_style}]PIT 覆盖验证状态：{report.status}[/{status_style}]")
    console.print(f"Manifest 状态：{report.manifest_status}")
    console.print(f"快照数：{report.snapshot_count}；原始记录数：{report.row_count}")
    console.print(f"覆盖验证报告：{report_output}")
    if not validation_report.passed:
        raise typer.Exit(code=1)


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
        f"production_effect={payload['production_effect']}；"
        "只读扫描"
    )


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


@ops_app.command("health")
def pipeline_health_command(
    as_of: Annotated[
        str | None,
        typer.Option(help="检查日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    prices_path: Annotated[
        Path,
        typer.Option(help="标准化日线价格 CSV 路径。"),
    ] = PROJECT_ROOT
    / "data"
    / "raw"
    / "prices_daily.csv",
    rates_path: Annotated[
        Path,
        typer.Option(help="标准化日线利率 CSV 路径。"),
    ] = PROJECT_ROOT
    / "data"
    / "raw"
    / "rates_daily.csv",
    features_path: Annotated[
        Path,
        typer.Option(help="每日特征 CSV 路径。"),
    ] = PROJECT_ROOT
    / "data"
    / "processed"
    / "features_daily.csv",
    scores_path: Annotated[
        Path,
        typer.Option(help="每日评分 CSV 路径。"),
    ] = PROJECT_ROOT
    / "data"
    / "processed"
    / "scores_daily.csv",
    data_quality_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 数据质量报告路径。"),
    ] = None,
    daily_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 每日评分报告路径。"),
    ] = None,
    pit_manifest_path: Annotated[
        Path,
        typer.Option(help="PIT raw snapshot manifest 路径。"),
    ] = DEFAULT_PIT_SNAPSHOT_MANIFEST_PATH,
    pit_normalized_path: Annotated[
        Path | None,
        typer.Option(help="FMP PIT 标准化 as-of CSV 路径，默认按 as-of 日期生成。"),
    ] = None,
    pit_validation_report_path: Annotated[
        Path | None,
        typer.Option(help="PIT 快照质量报告路径，默认按 as-of 日期生成。"),
    ] = None,
    pit_fetch_report_path: Annotated[
        Path | None,
        typer.Option(help="FMP PIT 抓取报告路径，默认按 as-of 日期生成。"),
    ] = None,
    min_pit_manifest_records: Annotated[
        int,
        typer.Option(help="PIT manifest 最低记录数。"),
    ] = 1,
    min_pit_normalized_rows: Annotated[
        int,
        typer.Option(help="FMP PIT 标准化 as-of CSV 最低行数。"),
    ] = 1,
    max_pit_snapshot_age_days: Annotated[
        int,
        typer.Option(help="PIT latest available_time 最大允许日龄，超出后告警。"),
    ] = 3,
    non_trading_day: Annotated[
        bool,
        typer.Option(
            "--non-trading-day/--trading-day",
            help="休市日健康检查模式：不要求当日评分产物存在。",
        ),
    ] = False,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown pipeline health 报告输出路径。"),
    ] = None,
    alert_output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown pipeline health 告警报告输出路径。"),
    ] = None,
) -> None:
    """检查关键 pipeline 输入/输出 artifact 和 PIT 快照归档健康。"""
    health_observed_at = datetime.now(tz=UTC)
    health_date = _parse_date(as_of) if as_of else date.today()
    production_health_cutoff = (
        health_observed_at
        if health_date == resolve_daily_ops_default_as_of(health_observed_at)
        else None
    )
    quality_report = data_quality_report_path or default_quality_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        health_date,
    )
    daily_report = daily_report_path or default_daily_score_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        health_date,
    )
    report_path = output_path or default_pipeline_health_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        health_date,
    )
    pit_normalized = pit_normalized_path or default_fmp_forward_pit_normalized_path(
        DEFAULT_FMP_FORWARD_PIT_NORMALIZED_DIR,
        health_date,
    )
    pit_validation_report = (
        pit_validation_report_path
        or default_pit_snapshot_validation_report_path(
            PROJECT_ROOT / "outputs" / "reports",
            health_date,
        )
    )
    pit_fetch_report = pit_fetch_report_path or default_fmp_forward_pit_fetch_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        health_date,
    )
    pipeline_alert_report_path = alert_output_path or default_pipeline_health_alert_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        health_date,
    )
    pit_checks = build_pit_snapshot_health_checks(
        as_of=health_date,
        manifest_path=pit_manifest_path,
        normalized_path=pit_normalized,
        validation_report_path=pit_validation_report,
        fetch_report_path=pit_fetch_report,
        project_root=PROJECT_ROOT,
        min_manifest_records=min_pit_manifest_records,
        min_normalized_rows=min_pit_normalized_rows,
        max_snapshot_age_days=max_pit_snapshot_age_days,
        visibility_cutoff=production_health_cutoff,
    )
    core_artifacts = [
        PipelineArtifactSpec(
            "prices_daily",
            "价格缓存",
            prices_path,
            True,
            "运行 `aits download-data` 并检查 download manifest。",
        ),
        PipelineArtifactSpec(
            "rates_daily",
            "利率缓存",
            rates_path,
            True,
            "运行 `aits download-data` 并检查 FRED 下载状态。",
        ),
    ]
    if not non_trading_day:
        core_artifacts.extend(
            [
                PipelineArtifactSpec(
                    "data_quality_report",
                    "数据质量报告",
                    quality_report,
                    True,
                    "运行 `aits validate-data` 或 `aits score-daily`。",
                ),
                PipelineArtifactSpec(
                    "features_daily",
                    "每日特征缓存",
                    features_path,
                    True,
                    "运行 `aits build-features` 或 `aits score-daily`。",
                ),
                PipelineArtifactSpec(
                    "scores_daily",
                    "每日评分缓存",
                    scores_path,
                    True,
                    "运行 `aits score-daily`。",
                ),
                PipelineArtifactSpec(
                    "daily_score_report",
                    "每日评分报告",
                    daily_report,
                    True,
                    "运行 `aits score-daily` 并检查数据质量、SEC、风险事件和估值报告。",
                ),
            ]
        )
    report = build_pipeline_health_report(
        as_of=health_date,
        artifacts=tuple(core_artifacts),
        extra_checks=pit_checks,
        market_session="CLOSED_MARKET" if non_trading_day else "TRADING_DAY",
        market_session_note=(
            "休市日模式：不要求当日 data_quality、features、scores 或 daily_score 报告。"
            if non_trading_day
            else "交易日模式：要求当日数据质量、特征、评分和日报产物。"
        ),
    )
    write_pipeline_health_report(report, report_path)
    alert_report = build_pipeline_health_alert_report(
        report,
        pipeline_health_report_path=report_path,
    )
    write_alert_report(alert_report, pipeline_alert_report_path)

    style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{style}]Pipeline health：{report.status}[/{style}]")
    console.print(f"报告：{report_path}")
    console.print(f"告警报告：{pipeline_alert_report_path}")
    console.print(f"检查项：{len(report.checks)}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")
    console.print(f"活跃告警数：{len(alert_report.alerts)}")
    if not report.passed:
        raise typer.Exit(code=1)


def _build_daily_ops_plan_from_cli_options(
    *,
    as_of: str | None,
    download_start: str,
    include_download_data: bool,
    include_pit_snapshots: bool,
    include_sec_fundamentals: bool,
    include_valuation_snapshots: bool,
    include_secret_scan: bool,
    risk_event_openai_precheck: bool,
    risk_event_openai_precheck_max_candidates: int | None,
    llm_request_profile: str,
    full_universe: bool,
    run_id: str | None = None,
    default_observed_at: datetime | None = None,
):
    plan_date = (
        _parse_date(as_of) if as_of else resolve_daily_ops_default_as_of(default_observed_at)
    )
    start_date = _parse_date(download_start)
    if (
        risk_event_openai_precheck_max_candidates is not None
        and risk_event_openai_precheck_max_candidates < 0
    ):
        raise typer.BadParameter("OpenAI 风险事件预审候选上限不能为负数。")
    try:
        plan = build_daily_ops_plan(
            as_of=plan_date,
            download_start=start_date,
            include_download_data=include_download_data,
            include_pit_snapshots=include_pit_snapshots,
            include_sec_fundamentals=include_sec_fundamentals,
            include_valuation_snapshots=include_valuation_snapshots,
            include_secret_scan=include_secret_scan,
            skip_risk_event_openai_precheck=not risk_event_openai_precheck,
            full_universe=full_universe,
            llm_request_profile=llm_request_profile,
            risk_event_openai_precheck_max_candidates=(risk_event_openai_precheck_max_candidates),
            run_id=run_id,
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    return plan_date, plan


@ops_app.command("daily-plan")
def daily_ops_plan_command(
    as_of: Annotated[
        str | None,
        typer.Option(help="计划日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    download_start: Annotated[
        str,
        typer.Option(help="市场数据下载起始日期，格式为 YYYY-MM-DD。"),
    ] = "2018-01-01",
    include_download_data: Annotated[
        bool,
        typer.Option(
            "--include-download-data/--skip-download-data",
            help="是否在计划中包含 `aits download-data`。",
        ),
    ] = True,
    include_pit_snapshots: Annotated[
        bool,
        typer.Option(
            "--include-pit-snapshots/--skip-pit-snapshots",
            help="是否在计划中包含 FMP forward-only PIT 抓取和校验。",
        ),
    ] = True,
    include_sec_fundamentals: Annotated[
        bool,
        typer.Option(
            "--include-sec-fundamentals/--skip-sec-fundamentals",
            help="是否在计划中包含 SEC companyfacts 刷新和 SEC metrics 抽取。",
        ),
    ] = True,
    include_valuation_snapshots: Annotated[
        bool,
        typer.Option(
            "--include-valuation-snapshots/--skip-valuation-snapshots",
            help="是否在计划中包含 FMP 估值和预期快照刷新。",
        ),
    ] = True,
    include_secret_scan: Annotated[
        bool,
        typer.Option(
            "--include-secret-scan/--skip-secret-scan",
            help="是否在计划中包含 secret hygiene 扫描。",
        ),
    ] = True,
    risk_event_openai_precheck: Annotated[
        bool,
        typer.Option(
            "--risk-event-openai-precheck/--skip-risk-event-openai-precheck",
            help="是否让计划中的 score-daily 默认执行 OpenAI 风险事件预审。",
        ),
    ] = True,
    risk_event_openai_precheck_max_candidates: Annotated[
        int | None,
        typer.Option(help="覆盖 LLM request profile 中的 OpenAI 风险事件预审候选上限。"),
    ] = None,
    llm_request_profile: Annotated[
        str,
        typer.Option(help="计划中的 score-daily 使用的 LLM request profile。"),
    ] = DEFAULT_RISK_EVENT_DAILY_PREREVIEW_PROFILE,
    full_universe: Annotated[
        bool,
        typer.Option(
            "--full-universe",
            help="计划中对 download-data 使用完整 AI 产业链标的。",
        ),
    ] = False,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 每日运行计划输出路径。"),
    ] = None,
    fail_on_missing_env: Annotated[
        bool,
        typer.Option(
            "--fail-on-missing-env",
            help="如果计划发现缺少关键环境变量，写出报告后返回非零退出码。",
        ),
    ] = False,
) -> None:
    """生成适合本地或云 VM 调度的每日运行计划。"""
    plan_date, plan = _build_daily_ops_plan_from_cli_options(
        as_of=as_of,
        download_start=download_start,
        include_download_data=include_download_data,
        include_pit_snapshots=include_pit_snapshots,
        include_sec_fundamentals=include_sec_fundamentals,
        include_valuation_snapshots=include_valuation_snapshots,
        include_secret_scan=include_secret_scan,
        risk_event_openai_precheck=risk_event_openai_precheck,
        risk_event_openai_precheck_max_candidates=(risk_event_openai_precheck_max_candidates),
        llm_request_profile=llm_request_profile,
        full_universe=full_universe,
    )

    report_path = output_path or default_daily_ops_plan_path(
        PROJECT_ROOT / "outputs" / "reports",
        plan_date,
    )
    write_daily_ops_plan(plan, report_path, env=os.environ)

    status = plan.status(os.environ)
    style = "green" if status == "READY" else "yellow" if status == "READY_WITH_SKIPS" else "red"
    missing_env = plan.missing_env_vars(os.environ)
    console.print(f"[{style}]每日运行计划：{status}[/{style}]")
    console.print(f"报告：{report_path}")
    console.print(f"步骤数：{len(plan.steps)}")
    if missing_env:
        console.print(f"缺失环境变量：{', '.join(missing_env)}")
    if fail_on_missing_env and missing_env:
        raise typer.Exit(code=1)


def _refresh_reader_brief_from_daily_run_summary(
    *,
    as_of: date,
    reports_dir: Path,
    daily_decision_summary_path: Path,
    daily_task_dashboard_json_path: Path,
) -> tuple[Path, ...]:
    daily_report_path = default_daily_score_report_path(reports_dir, as_of)
    reader_brief_html_path = default_reader_brief_html_path(reports_dir, as_of)
    reader_brief_json_path = default_reader_brief_json_path(reports_dir, as_of)
    reader_brief_quality_json_path = default_reader_brief_quality_json_path(reports_dir, as_of)
    reader_brief_quality_md_path = default_reader_brief_quality_markdown_path(reports_dir, as_of)
    payload = build_reader_brief_payload(
        as_of=as_of,
        reports_dir=reports_dir,
        decision_snapshot_path=default_decision_snapshot_path(
            DEFAULT_DECISION_SNAPSHOT_DIR,
            as_of,
        ),
        calculation_explainers_path=default_calculation_explainers_path(reports_dir, as_of),
        daily_decision_summary_path=daily_decision_summary_path,
        evidence_dashboard_json_path=default_evidence_dashboard_json_path(reports_dir, as_of),
        daily_task_dashboard_json_path=daily_task_dashboard_json_path,
        daily_report_path=daily_report_path,
        trace_bundle_path=default_report_trace_bundle_path(daily_report_path),
        score_change_attribution_path=default_score_change_attribution_json_path(
            reports_dir,
            as_of,
        ),
        market_panel_path=default_market_panel_json_path(reports_dir, as_of),
        research_governance_summary_path=default_research_governance_summary_json_path(
            reports_dir,
            as_of,
        ),
        report_index_path=default_report_index_json_path(reports_dir, as_of),
        documentation_contract_path=default_documentation_contract_json_path(reports_dir, as_of),
    )
    html_path = write_reader_brief_html(payload, reader_brief_html_path)
    json_path = write_reader_brief_json(payload, reader_brief_json_path)
    quality_payload = build_reader_brief_quality_payload(
        reader_brief_payload=payload,
        reader_brief_json_path=json_path,
        reader_brief_html_path=html_path,
    )
    quality_json_path = write_reader_brief_quality_json(
        quality_payload,
        reader_brief_quality_json_path,
    )
    quality_md_path = write_reader_brief_quality_markdown(
        quality_payload,
        reader_brief_quality_md_path,
    )
    return html_path, json_path, quality_json_path, quality_md_path


@ops_app.command("daily-run")
def daily_ops_run_command(
    as_of: Annotated[
        str | None,
        typer.Option(help="运行日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    download_start: Annotated[
        str,
        typer.Option(help="市场数据下载起始日期，格式为 YYYY-MM-DD。"),
    ] = "2018-01-01",
    include_download_data: Annotated[
        bool,
        typer.Option(
            "--include-download-data/--skip-download-data",
            help="是否执行 `aits download-data`。",
        ),
    ] = True,
    include_pit_snapshots: Annotated[
        bool,
        typer.Option(
            "--include-pit-snapshots/--skip-pit-snapshots",
            help="是否执行 FMP forward-only PIT 抓取和校验。",
        ),
    ] = True,
    include_sec_fundamentals: Annotated[
        bool,
        typer.Option(
            "--include-sec-fundamentals/--skip-sec-fundamentals",
            help="是否执行 SEC companyfacts 刷新和 SEC metrics 抽取。",
        ),
    ] = True,
    include_valuation_snapshots: Annotated[
        bool,
        typer.Option(
            "--include-valuation-snapshots/--skip-valuation-snapshots",
            help="是否执行 FMP 估值和预期快照刷新。",
        ),
    ] = True,
    include_secret_scan: Annotated[
        bool,
        typer.Option(
            "--include-secret-scan/--skip-secret-scan",
            help="是否执行 secret hygiene 扫描。",
        ),
    ] = True,
    risk_event_openai_precheck: Annotated[
        bool,
        typer.Option(
            "--risk-event-openai-precheck/--skip-risk-event-openai-precheck",
            help="是否让 score-daily 执行 OpenAI 风险事件预审。",
        ),
    ] = True,
    risk_event_openai_precheck_max_candidates: Annotated[
        int | None,
        typer.Option(help="覆盖 LLM request profile 中的 OpenAI 风险事件预审候选上限。"),
    ] = None,
    llm_request_profile: Annotated[
        str,
        typer.Option(help="score-daily 使用的 LLM request profile。"),
    ] = DEFAULT_RISK_EVENT_DAILY_PREREVIEW_PROFILE,
    full_universe: Annotated[
        bool,
        typer.Option(
            "--full-universe",
            help="对 download-data 使用完整 AI 产业链标的。",
        ),
    ] = False,
    plan_output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 每日运行计划输出路径。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 每日执行报告输出路径。"),
    ] = None,
    run_output_root: Annotated[
        Path,
        typer.Option(help="Canonical run bundle 根目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "runs",
    run_id: Annotated[
        str | None,
        typer.Option(help="可选固定 run id；默认由 as_of 和 UTC 时间生成。"),
    ] = None,
    legacy_output_mode: Annotated[
        str,
        typer.Option(help="Legacy outputs/reports 兼容模式：mirror 或 off。"),
    ] = "mirror",
) -> None:
    """按每日运行计划真实执行本地 CLI，并生成脱敏执行报告。"""
    try:
        legacy_mode = validate_legacy_output_mode(legacy_output_mode)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    run_generated_at = datetime.now(tz=UTC)
    plan_date = _parse_date(as_of) if as_of else resolve_daily_ops_default_as_of(run_generated_at)
    resolved_run_id = run_id or default_daily_run_id(
        plan_date,
        generated_at=run_generated_at,
    )
    run_paths = prepare_run_directories(
        build_run_artifact_paths(
            as_of=plan_date,
            run_id=resolved_run_id,
            output_root=run_output_root,
            generated_at=run_generated_at,
        )
    )
    plan_date, plan = _build_daily_ops_plan_from_cli_options(
        as_of=as_of,
        download_start=download_start,
        include_download_data=include_download_data,
        include_pit_snapshots=include_pit_snapshots,
        include_sec_fundamentals=include_sec_fundamentals,
        include_valuation_snapshots=include_valuation_snapshots,
        include_secret_scan=include_secret_scan,
        risk_event_openai_precheck=risk_event_openai_precheck,
        risk_event_openai_precheck_max_candidates=(risk_event_openai_precheck_max_candidates),
        llm_request_profile=llm_request_profile,
        full_universe=full_universe,
        run_id=resolved_run_id,
        default_observed_at=run_generated_at,
    )

    reports_dir = PROJECT_ROOT / "outputs" / "reports"
    plan_report_path = plan_output_path or default_daily_ops_plan_path(
        run_paths.reports_dir,
        plan_date,
    )
    run_report_path = output_path or default_daily_ops_run_report_path(
        run_paths.reports_dir,
        plan_date,
    )
    write_daily_ops_plan(plan, plan_report_path, env=os.environ)
    if legacy_mode == "mirror":
        write_daily_ops_plan(
            plan,
            default_daily_ops_plan_path(reports_dir, plan_date),
            env=os.environ,
        )
    run_report = run_daily_ops_plan(
        plan,
        project_root=PROJECT_ROOT,
        env=os.environ,
        run_id=resolved_run_id,
        diagnostics_dir=run_paths.reports_dir / "diagnostics",
    )
    metadata_path = default_daily_ops_run_metadata_path(
        run_paths.metadata_dir,
        plan_date,
    )
    write_daily_ops_run_report(run_report, run_report_path, metadata_path=metadata_path)
    canonical_outputs = mirror_legacy_reports_to_run(
        as_of=plan_date,
        legacy_reports_dir=reports_dir,
        paths=run_paths,
        min_modified_at=run_report.started_at,
    )
    daily_task_dashboard_report = build_daily_task_dashboard_report(
        as_of=plan_date,
        metadata_path=metadata_path,
        run_report_path=run_report_path,
        reports_dir=run_paths.reports_dir,
    )
    daily_task_dashboard_path = write_daily_task_dashboard(
        daily_task_dashboard_report,
        default_daily_task_dashboard_path(run_paths.reports_dir, plan_date),
    )
    daily_task_dashboard_json_path = write_daily_task_dashboard_json(
        daily_task_dashboard_report,
        default_daily_task_dashboard_json_path(run_paths.reports_dir, plan_date),
    )
    daily_decision_summary_path = write_daily_decision_summary_json(
        daily_task_dashboard_report,
        default_daily_decision_summary_path(run_paths.reports_dir, plan_date),
    )
    order_intent_candidates_path = write_order_intent_candidates_json(
        as_of=plan_date,
        daily_decision_summary_path=daily_decision_summary_path,
        output_path=default_order_intent_candidates_path(run_paths.reports_dir, plan_date),
        project_root=PROJECT_ROOT,
    )
    reader_brief_step = next(
        (result for result in run_report.step_results if result.step_id == "reader_brief"),
        None,
    )
    reader_brief_final_paths = (
        _refresh_reader_brief_from_daily_run_summary(
            as_of=plan_date,
            reports_dir=run_paths.reports_dir,
            daily_decision_summary_path=daily_decision_summary_path,
            daily_task_dashboard_json_path=daily_task_dashboard_json_path,
        )
        if reader_brief_step is not None and reader_brief_step.status == "PASS"
        else ()
    )
    if legacy_mode == "mirror":
        legacy_outputs = mirror_canonical_daily_ops_outputs_to_legacy(
            paths=run_paths,
            legacy_reports_dir=reports_dir,
        )
    else:
        legacy_outputs = ()
    if run_report.metadata is not None:
        write_run_manifest(
            paths=run_paths,
            project_root=PROJECT_ROOT,
            status=run_report.status,
            visibility_cutoff=run_report.metadata.visibility_cutoff,
            visibility_cutoff_source=run_report.metadata.visibility_cutoff_source,
            legacy_output_mode=legacy_mode,
            input_artifacts=(
                artifact.path for artifact in run_report.metadata.pre_run_input_artifacts
            ),
            canonical_output_artifacts=(
                *collect_run_files(run_paths),
                *canonical_outputs,
            ),
            legacy_output_artifacts=(
                *(artifact.path for artifact in run_report.metadata.produced_artifacts),
                *legacy_outputs,
            ),
        )

    status = run_report.status
    style = "green" if status == "PASS" else "yellow" if status == "PASS_WITH_SKIPS" else "red"
    console.print(f"[{style}]每日运行执行：{status}[/{style}]")
    console.print(f"Run ID：{resolved_run_id}")
    console.print(f"Run bundle：{run_paths.run_root}")
    console.print(f"计划报告：{plan_report_path}")
    console.print(f"执行报告：{run_report_path}")
    console.print(f"每日任务 Dashboard：{daily_task_dashboard_path}")
    console.print(f"每日任务 Dashboard JSON：{daily_task_dashboard_json_path}")
    console.print(f"每日决策总线 JSON：{daily_decision_summary_path}")
    console.print(f"Order intent candidates JSON：{order_intent_candidates_path}")
    if reader_brief_final_paths:
        console.print(f"Reader Brief final：{reader_brief_final_paths[0]}")
    if run_report.metadata is not None:
        console.print(f"Metadata JSON：{metadata_path}")
        console.print(f"Run manifest：{run_paths.manifest_path}")
    console.print(f"执行步骤数：{len(run_report.step_results)} / {len(plan.steps)}")
    if run_report.missing_env_vars:
        console.print(f"缺失环境变量：{', '.join(run_report.missing_env_vars)}")
    if run_report.failed_step is not None:
        failed = run_report.failed_step
        console.print(f"失败步骤：{failed.step_id}；return_code={failed.return_code}")
    if status not in {"PASS", "PASS_WITH_SKIPS"}:
        raise typer.Exit(code=1)


@ops_app.command("replay-day")
def historical_replay_day_command(
    as_of: Annotated[
        str,
        typer.Option(help="回放的历史交易日，格式为 YYYY-MM-DD。"),
    ],
    mode: Annotated[
        str,
        typer.Option(help="回放模式；MVP 仅支持 cache-only。"),
    ] = "cache-only",
    visible_at: Annotated[
        str | None,
        typer.Option(help="显式可见时间上限，ISO datetime；不传则使用 as-of 当日 UTC 末尾。"),
    ] = None,
    output_root: Annotated[
        Path | None,
        typer.Option(help="Replay bundle 根目录；默认写入项目 outputs/replays。"),
    ] = None,
    label: Annotated[
        str | None,
        typer.Option(help="可选 replay 标签，用于 run id。"),
    ] = None,
    run_id: Annotated[
        str | None,
        typer.Option(help="可选固定 run id，便于测试或重复验证。"),
    ] = None,
    inventory_only: Annotated[
        bool,
        typer.Option(
            "--inventory-only/--run-score",
            help="只生成 input freeze manifest，不运行 score/health/secret replay。",
        ),
    ] = False,
    allow_incomplete: Annotated[
        bool,
        typer.Option(
            "--allow-incomplete/--strict",
            help="允许缺关键输入时只生成 INCOMPLETE_REPLAY 诊断报告。",
        ),
    ] = False,
    compare_to_production: Annotated[
        bool,
        typer.Option(
            "--compare-to-production/--no-compare-to-production",
            help="生成 replay 输出与 production artifact 的结构化 diff。",
        ),
    ] = False,
    openai_replay_policy: Annotated[
        str,
        typer.Option(
            help=(
                "OpenAI replay 策略：disabled 或 cache-only；cache-only 只复制历史"
                "预审队列/报告，不调用 live OpenAI。"
            ),
        ),
    ] = "disabled",
    full_universe: Annotated[
        bool,
        typer.Option("--full-universe", help="对 replay score-daily 使用完整 AI 产业链标的。"),
    ] = False,
    project_root: Annotated[
        Path,
        typer.Option(help="项目根目录；默认使用当前安装包配置的 PROJECT_ROOT。"),
    ] = PROJECT_ROOT,
) -> None:
    """基于本地归档输入回放某个历史交易日的分析产出。"""
    replay_date = _parse_date(as_of)
    replay_visible_at = _parse_datetime(visible_at) if visible_at else None
    try:
        replay_run = run_historical_day_replay(
            as_of=replay_date,
            project_root=project_root,
            output_root=output_root or default_historical_replay_output_root(project_root),
            mode=mode,
            visible_at=replay_visible_at,
            label=label,
            run_id=run_id,
            inventory_only=inventory_only,
            allow_incomplete=allow_incomplete,
            compare_to_production=compare_to_production,
            openai_replay_policy=openai_replay_policy,
            full_universe=full_universe,
            env=os.environ,
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc

    status = replay_run.status
    style = (
        "green"
        if status in {"PASS", "PASS_INVENTORY"}
        else "yellow" if status == "INCOMPLETE_REPLAY" else "red"
    )
    console.print(f"[{style}]历史交易日回放：{status}[/{style}]")
    console.print(f"Replay bundle：{replay_run.paths.root}")
    console.print(f"回放报告：{replay_run.paths.run_report_path}")
    console.print(f"输入冻结清单：{replay_run.paths.input_manifest_csv_path}")
    if replay_run.production_diff is not None:
        console.print(f"Production diff：{replay_run.production_diff.report_path}")
    if replay_run.errors:
        console.print(f"输入阻断：{len(replay_run.errors)} 项")
    if replay_run.failed_step is not None:
        failed = replay_run.failed_step
        console.print(f"失败步骤：{failed.step_id}；return_code={failed.return_code}")
    if status not in {"PASS", "PASS_INVENTORY"}:
        raise typer.Exit(code=1)


@ops_app.command("replay-window")
def historical_replay_window_command(
    start: Annotated[
        str,
        typer.Option(help="批量回放起始日期，格式为 YYYY-MM-DD。"),
    ],
    end: Annotated[
        str,
        typer.Option(help="批量回放结束日期，格式为 YYYY-MM-DD。"),
    ],
    mode: Annotated[
        str,
        typer.Option(help="回放模式；目前仅支持 cache-only。"),
    ] = "cache-only",
    output_root: Annotated[
        Path | None,
        typer.Option(help="Replay bundle 根目录；默认写入项目 outputs/replays。"),
    ] = None,
    label: Annotated[
        str | None,
        typer.Option(help="可选 replay 标签，用于 window run id 和单日 run id。"),
    ] = None,
    run_id: Annotated[
        str | None,
        typer.Option(help="可选固定 window run id，便于测试或重复验证。"),
    ] = None,
    inventory_only: Annotated[
        bool,
        typer.Option(
            "--inventory-only/--run-score",
            help="只生成每个交易日的 input freeze manifest，不运行 score/health/secret replay。",
        ),
    ] = False,
    allow_incomplete: Annotated[
        bool,
        typer.Option(
            "--allow-incomplete/--strict",
            help="允许缺关键输入时只生成 INCOMPLETE_REPLAY 诊断报告。",
        ),
    ] = False,
    compare_to_production: Annotated[
        bool,
        typer.Option(
            "--compare-to-production/--no-compare-to-production",
            help="为每个交易日生成 replay 输出与 production artifact 的结构化 diff。",
        ),
    ] = False,
    openai_replay_policy: Annotated[
        str,
        typer.Option(
            help=(
                "OpenAI replay 策略：disabled 或 cache-only；cache-only 只复制历史"
                "预审队列/报告，不调用 live OpenAI。"
            ),
        ),
    ] = "disabled",
    full_universe: Annotated[
        bool,
        typer.Option("--full-universe", help="对 replay score-daily 使用完整 AI 产业链标的。"),
    ] = False,
    continue_on_failure: Annotated[
        bool,
        typer.Option(
            "--continue-on-failure/--stop-on-failure",
            help="某个交易日 replay 失败后是否继续后续交易日。",
        ),
    ] = False,
    project_root: Annotated[
        Path,
        typer.Option(help="项目根目录；默认使用当前安装包配置的 PROJECT_ROOT。"),
    ] = PROJECT_ROOT,
) -> None:
    """按交易日窗口批量运行历史交易日归档回放。"""
    start_date = _parse_date(start)
    end_date = _parse_date(end)
    try:
        window_run = run_historical_replay_window(
            start=start_date,
            end=end_date,
            project_root=project_root,
            output_root=output_root or default_historical_replay_output_root(project_root),
            mode=mode,
            label=label,
            run_id=run_id,
            inventory_only=inventory_only,
            allow_incomplete=allow_incomplete,
            compare_to_production=compare_to_production,
            openai_replay_policy=openai_replay_policy,
            full_universe=full_universe,
            continue_on_failure=continue_on_failure,
            env=os.environ,
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc

    status = window_run.status
    style = "green" if status in {"PASS", "PASS_WITH_SKIPS"} else "red"
    console.print(f"[{style}]历史交易日批量回放：{status}[/{style}]")
    console.print(f"Window report：{window_run.report_path}")
    console.print(f"Window JSON：{window_run.json_path}")
    console.print(f"交易日回放数：{len(window_run.day_runs)}")
    console.print(f"跳过非交易日数：{len(window_run.skipped_dates)}")
    if window_run.failed_run is not None:
        failed = window_run.failed_run
        console.print(f"失败日期：{failed.as_of.isoformat()}；status={failed.status}")
    if status not in {"PASS", "PASS_WITH_SKIPS"}:
        raise typer.Exit(code=1)


@app.command("review-trades")
def review_trades(
    prices_path: Annotated[
        Path,
        typer.Option(help="标准化日线价格 CSV 路径。"),
    ] = PROJECT_ROOT
    / "data"
    / "raw"
    / "prices_daily.csv",
    rates_path: Annotated[
        Path,
        typer.Option(help="标准化日线利率 CSV 路径。"),
    ] = PROJECT_ROOT
    / "data"
    / "raw"
    / "rates_daily.csv",
    input_path: Annotated[
        Path,
        typer.Option(help="交易记录 YAML 文件或目录路径。"),
    ] = PROJECT_ROOT
    / "data"
    / "external"
    / "trades",
    as_of: Annotated[
        str | None,
        typer.Option(help="复盘日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    benchmarks: Annotated[
        str,
        typer.Option(help="逗号分隔的归因基准标的。"),
    ] = ",".join(DEFAULT_BENCHMARK_TICKERS),
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 交易复盘报告输出路径。"),
    ] = None,
    quality_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 数据质量报告输出路径。"),
    ] = None,
    full_universe: Annotated[
        bool,
        typer.Option(
            "--full-universe",
            help="校验配置中的完整 AI 产业链标的，而不只校验核心观察池。",
        ),
    ] = False,
) -> None:
    """通过数据质量门禁后复盘交易记录并做基础基准归因。"""
    universe = load_universe()
    review_date = _parse_date(as_of) if as_of else date.today()
    benchmark_tickers = _parse_csv_items(benchmarks)
    if not benchmark_tickers:
        raise typer.BadParameter("至少需要一个归因基准标的。")

    quality_output = quality_report_path or default_quality_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        review_date,
    )
    report_output = output_path or default_trade_review_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        review_date,
    )

    data_quality_report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=configured_price_tickers(
            universe,
            include_full_ai_chain=full_universe,
        ),
        expected_rate_series=configured_rate_series(universe),
        quality_config=load_data_quality(),
        as_of=review_date,
        manifest_path=_download_manifest_path(prices_path),
        secondary_prices_path=_marketstack_prices_path(prices_path),
        require_secondary_prices=_requires_marketstack_prices(prices_path),
    )
    write_data_quality_report(data_quality_report, quality_output)
    if not data_quality_report.passed:
        console.print("[red]数据质量门禁失败，已停止交易复盘。[/red]")
        console.print(f"数据质量报告：{quality_output}")
        console.print(
            f"错误数：{data_quality_report.error_count}；"
            f"警告数：{data_quality_report.warning_count}"
        )
        raise typer.Exit(code=1)

    validation_report = validate_trade_record_store(
        store=load_trade_record_store(input_path),
        universe=universe,
        watchlist=load_watchlist(),
        as_of=review_date,
    )
    review_report = build_trade_review_report(
        validation_report=validation_report,
        prices=pd.read_csv(prices_path),
        data_quality_report=data_quality_report,
        benchmark_tickers=tuple(benchmark_tickers),
    )
    write_trade_review_report(
        review_report,
        data_quality_report_path=quality_output,
        output_path=report_output,
    )

    status_style = (
        "green"
        if review_report.status == "PASS"
        else ("yellow" if validation_report.passed else "red")
    )
    console.print(f"[{status_style}]交易复盘状态：{review_report.status}[/{status_style}]")
    console.print(f"报告：{report_output}")
    console.print(f"交易数：{validation_report.trade_count}")
    console.print(
        f"校验错误数：{validation_report.error_count}；"
        f"校验警告数：{validation_report.warning_count}"
    )
    console.print(f"数据质量报告：{quality_output}（{data_quality_report.status}）")

    if not validation_report.passed:
        raise typer.Exit(code=1)


@app.command("build-features")
def build_features(
    prices_path: Annotated[
        Path,
        typer.Option(help="标准化日线价格 CSV 路径。"),
    ] = PROJECT_ROOT
    / "data"
    / "raw"
    / "prices_daily.csv",
    rates_path: Annotated[
        Path,
        typer.Option(help="标准化日线利率 CSV 路径。"),
    ] = PROJECT_ROOT
    / "data"
    / "raw"
    / "rates_daily.csv",
    as_of: Annotated[
        str | None,
        typer.Option(help="特征日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path,
        typer.Option(help="特征 CSV 输出路径。"),
    ] = PROJECT_ROOT
    / "data"
    / "processed"
    / "features_daily.csv",
    report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 特征摘要输出路径。"),
    ] = None,
    quality_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 数据质量报告输出路径。"),
    ] = None,
    feature_availability_path: Annotated[
        Path,
        typer.Option(help="PIT feature availability catalog YAML 路径。"),
    ] = DEFAULT_FEATURE_AVAILABILITY_CONFIG_PATH,
    feature_availability_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown PIT 特征可见时间报告输出路径。"),
    ] = None,
    full_universe: Annotated[
        bool,
        typer.Option(
            "--full-universe",
            help="校验并构建配置中的完整 AI 产业链标的特征。",
        ),
    ] = False,
) -> None:
    """通过数据质量门禁后构建每日市场特征。"""
    universe = load_universe()
    data_quality_config = load_data_quality()
    feature_config = load_features()
    feature_date = _parse_date(as_of) if as_of else date.today()
    expected_price_tickers = configured_price_tickers(
        universe,
        include_full_ai_chain=full_universe,
    )
    expected_rate_series = configured_rate_series(universe)
    quality_output = quality_report_path or default_quality_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        feature_date,
    )
    feature_report_output = report_path or default_feature_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        feature_date,
    )
    feature_availability_output = (
        feature_availability_report_path
        or default_feature_availability_report_path(
            PROJECT_ROOT / "outputs" / "reports",
            feature_date,
        )
    )

    data_quality_report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=expected_price_tickers,
        expected_rate_series=expected_rate_series,
        quality_config=data_quality_config,
        as_of=feature_date,
        manifest_path=_download_manifest_path(prices_path),
        secondary_prices_path=_marketstack_prices_path(prices_path),
        require_secondary_prices=_requires_marketstack_prices(prices_path),
    )
    write_data_quality_report(data_quality_report, quality_output)
    if not data_quality_report.passed:
        console.print("[red]数据质量门禁失败，已停止特征构建。[/red]")
        console.print(f"数据质量报告：{quality_output}")
        console.print(
            f"错误数：{data_quality_report.error_count}；"
            f"警告数：{data_quality_report.warning_count}"
        )
        raise typer.Exit(code=1)

    prices_frame = pd.read_csv(prices_path)
    rates_frame = pd.read_csv(rates_path)
    feature_set = build_market_features(
        prices=prices_frame,
        rates=rates_frame,
        config=feature_config,
        as_of=feature_date,
        core_watchlist=universe.ai_chain.get("core_watchlist", []),
    )
    feature_availability_report = build_feature_availability_report(
        input_path=feature_availability_path,
        as_of=feature_date,
        observed_sources=tuple(sorted({row.source for row in feature_set.rows})),
        required_sources=("prices_daily", "rates_daily"),
        source_checks=_market_feature_source_checks(
            prices_frame=prices_frame,
            rates_frame=rates_frame,
            prices_path=prices_path,
            rates_path=rates_path,
            decision_time=feature_date,
        ),
    )
    write_feature_availability_report(
        feature_availability_report,
        feature_availability_output,
    )
    if not feature_availability_report.passed:
        console.print("[red]PIT 特征可见时间校验失败，已停止特征构建。[/red]")
        console.print(f"PIT 特征可见时间报告：{feature_availability_output}")
        console.print(
            f"错误数：{feature_availability_report.error_count}；"
            f"警告数：{feature_availability_report.warning_count}"
        )
        raise typer.Exit(code=1)
    features_output = write_features_csv(feature_set, output_path)
    feature_summary_output = write_feature_summary(
        feature_set,
        data_quality_report=data_quality_report,
        data_quality_report_path=quality_output,
        features_path=features_output,
        output_path=feature_report_output,
        feature_availability_section=render_feature_availability_section(
            feature_availability_report,
            feature_availability_output,
        ),
    )

    status_style = "green" if feature_set.status == "PASS" else "yellow"
    console.print(f"[{status_style}]特征构建状态：{feature_set.status}[/{status_style}]")
    console.print(f"特征数据：{features_output}（{feature_date} 共 {len(feature_set.rows)} 行）")
    console.print(f"特征摘要：{feature_summary_output}")
    console.print(
        f"PIT 特征可见时间报告：{feature_availability_output}"
        f"（{feature_availability_report.status}）"
    )
    console.print(f"数据质量报告：{quality_output}（{data_quality_report.status}）")
    console.print(f"特征警告数：{len(feature_set.warnings)}")


@score_daily_app.command(
    "backfill-baseline",
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
)
def score_daily_backfill_baseline_command(
    ctx: typer.Context,
    start: Annotated[
        str,
        typer.Option(help="Backfill 开始日期 YYYY-MM-DD。"),
    ],
    end: Annotated[
        str,
        typer.Option(help="Backfill 结束日期 YYYY-MM-DD。"),
    ],
    tickers: Annotated[
        list[str] | None,
        typer.Option(
            "--tickers",
            help="Backfill ticker；可重复。若使用 `--tickers A B C`，B/C 会作为额外 ticker 读取。",
        ),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(
            help=(
                "Baseline score CSV 输出路径；默认写入 data/processed/research/ "
                "research-only 路径。"
            )
        ),
    ] = None,
    mode: Annotated[
        str,
        typer.Option(help="Backfill 模式；当前只允许 research_backfill。"),
    ] = BASELINE_SCORE_BACKFILL_MODE,
    prices_path: Annotated[
        Path,
        typer.Option(help="标准化日线价格 CSV 路径。"),
    ] = PROJECT_ROOT
    / "data"
    / "raw"
    / "prices_daily.csv",
    rates_path: Annotated[
        Path,
        typer.Option(help="标准化日线利率 CSV 路径。"),
    ] = PROJECT_ROOT
    / "data"
    / "raw"
    / "rates_daily.csv",
    quality_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 数据质量报告路径。"),
    ] = None,
    overwrite: Annotated[
        bool,
        typer.Option("--overwrite", help="显式允许覆盖既有 output path。"),
    ] = False,
    overwrite_production_path: Annotated[
        bool,
        typer.Option(
            "--overwrite-production-path",
            help=(
                "允许把 research backfill 写入 production scores_daily.csv 路径；"
                "仍需在文件存在时同时传 --overwrite。"
            ),
        ),
    ] = False,
) -> None:
    """生成 research-only historical baseline score rows。"""
    start_date = _parse_date(start)
    end_date = _parse_date(end)
    resolved_output_path = (
        _default_baseline_backfill_output_path(start_date, end_date)
        if output_path is None
        else _project_relative_path(output_path)
    )
    if _is_production_scores_daily_path(resolved_output_path) and not overwrite_production_path:
        raise typer.BadParameter(
            "research backfill 默认不得写入 data/processed/scores_daily.csv；"
            "如确需覆盖 production score path，请同时传 --overwrite-production-path。"
        )
    requested_tickers = [*(tickers or []), *[str(item) for item in ctx.args]]
    requested_tickers = [ticker for ticker in requested_tickers if ticker.strip()]
    if not requested_tickers:
        raise typer.BadParameter("至少需要一个 --tickers ticker。")

    universe = load_universe()
    expected_price_tickers = list(
        dict.fromkeys([*configured_price_tickers(universe), *requested_tickers])
    )
    expected_rate_series = configured_rate_series(universe)
    data_quality_config = load_data_quality()
    quality_output = quality_report_path or (
        PROJECT_ROOT
        / "outputs"
        / "reports"
        / f"baseline_score_backfill_data_quality_{end_date.isoformat()}.md"
    )
    data_quality_report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=expected_price_tickers,
        expected_rate_series=expected_rate_series,
        quality_config=data_quality_config,
        as_of=end_date,
        manifest_path=_download_manifest_path(prices_path),
        secondary_prices_path=_marketstack_prices_path(prices_path),
        require_secondary_prices=_requires_marketstack_prices(prices_path),
    )
    write_data_quality_report(data_quality_report, quality_output)
    if not data_quality_report.passed:
        console.print("[red]数据质量门禁失败，已停止 baseline research backfill。[/red]")
        console.print(f"数据质量报告：{quality_output}")
        console.print(
            f"错误数：{data_quality_report.error_count}；"
            f"警告数：{data_quality_report.warning_count}"
        )
        raise typer.Exit(code=1)

    result = run_baseline_score_backfill(
        start=start_date,
        end=end_date,
        tickers=requested_tickers,
        prices_path=prices_path,
        rates_path=rates_path,
        output_path=resolved_output_path,
        feature_config=load_features(),
        scoring_rules=load_scoring_rules(),
        portfolio=load_portfolio(),
        data_quality_status=data_quality_report.status,
        data_quality_report_path=quality_output,
        mode=mode,
        overwrite=overwrite,
    )
    console.print("[green]Baseline research backfill 完成。[/green]")
    console.print(f"输出：{result.output_path}")
    console.print(
        f"行数：{result.row_count}；ticker 数：{result.ticker_count}；日期数：{result.date_count}"
    )
    console.print(f"research_backfill=true；production_effect={result.production_effect}")
    console.print(f"数据质量报告：{quality_output}（{data_quality_report.status}）")


def _default_baseline_backfill_output_path(start_date: date, end_date: date) -> Path:
    return (
        PROJECT_ROOT
        / "data"
        / "processed"
        / "research"
        / ("scores_daily_backfill_sec_pit_" f"{start_date:%Y%m%d}_{end_date:%Y%m%d}.csv")
    )


def _project_relative_path(path: Path) -> Path:
    return path if path.is_absolute() else PROJECT_ROOT / path


def _is_production_scores_daily_path(path: Path) -> bool:
    production_path = PROJECT_ROOT / "data" / "processed" / "scores_daily.csv"
    return path.resolve() == production_path.resolve()


@score_daily_app.callback(invoke_without_command=True)
def score_daily(
    ctx: typer.Context = None,
    prices_path: Annotated[
        Path,
        typer.Option(help="标准化日线价格 CSV 路径。"),
    ] = PROJECT_ROOT
    / "data"
    / "raw"
    / "prices_daily.csv",
    rates_path: Annotated[
        Path,
        typer.Option(help="标准化日线利率 CSV 路径。"),
    ] = PROJECT_ROOT
    / "data"
    / "raw"
    / "rates_daily.csv",
    as_of: Annotated[
        str | None,
        typer.Option(help="评分日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    run_id: Annotated[
        str | None,
        typer.Option(help="可选 run id，用于 evidence bundle 和日报结论卡。"),
    ] = None,
    features_path: Annotated[
        Path,
        typer.Option(help="特征 CSV 输出路径。"),
    ] = PROJECT_ROOT
    / "data"
    / "processed"
    / "features_daily.csv",
    scores_path: Annotated[
        Path,
        typer.Option(help="评分 CSV 输出路径。"),
    ] = PROJECT_ROOT
    / "data"
    / "processed"
    / "scores_daily.csv",
    report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 每日评分报告输出路径。"),
    ] = None,
    alert_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 投资与数据告警报告输出路径。"),
    ] = None,
    portfolio_positions_path: Annotated[
        Path,
        typer.Option(help="真实持仓 CSV 路径，用于日报只读组合暴露分解。"),
    ] = DEFAULT_PORTFOLIO_POSITIONS_PATH,
    portfolio_exposure_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 组合暴露报告输出路径。"),
    ] = None,
    trace_bundle_path: Annotated[
        Path | None,
        typer.Option(help="JSON evidence bundle 输出路径。"),
    ] = None,
    decision_snapshot_path: Annotated[
        Path | None,
        typer.Option(help="JSON 决策快照输出路径。"),
    ] = None,
    calculation_explainers_path: Annotated[
        Path | None,
        typer.Option(help="JSON 计算解释输出路径。"),
    ] = None,
    belief_state_path: Annotated[
        Path | None,
        typer.Option(help="JSON 只读认知状态输出路径。"),
    ] = None,
    belief_state_history_path: Annotated[
        Path | None,
        typer.Option(help="CSV 只读认知状态历史索引输出路径。"),
    ] = None,
    feature_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 特征摘要输出路径。"),
    ] = None,
    quality_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 数据质量报告输出路径。"),
    ] = None,
    feature_availability_path: Annotated[
        Path,
        typer.Option(help="PIT feature availability catalog YAML 路径。"),
    ] = DEFAULT_FEATURE_AVAILABILITY_CONFIG_PATH,
    feature_availability_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown PIT 特征可见时间报告输出路径。"),
    ] = None,
    prediction_ledger_path: Annotated[
        Path,
        typer.Option(help="append-only prediction/shadow ledger CSV 输出路径。"),
    ] = DEFAULT_PREDICTION_LEDGER_PATH,
    prediction_candidate_id: Annotated[
        str,
        typer.Option(help="写入 prediction ledger 的模型候选 ID。"),
    ] = "production",
    prediction_production_effect: Annotated[
        str,
        typer.Option(help="prediction ledger 生产影响标记：production 或 none。"),
    ] = "production",
    execution_policy_path: Annotated[
        Path,
        typer.Option(help="execution policy YAML 路径，用于日报执行建议。"),
    ] = DEFAULT_EXECUTION_POLICY_CONFIG_PATH,
    execution_policy_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown execution policy 校验报告输出路径。"),
    ] = None,
    rule_cards_path: Annotated[
        Path,
        typer.Option(help="rule card registry YAML 路径，用于记录本次日报规则版本。"),
    ] = DEFAULT_RULE_CARDS_PATH,
    sec_companies_path: Annotated[
        Path,
        typer.Option(help="SEC company CIK 配置文件路径，用于日报基本面评分。"),
    ] = DEFAULT_SEC_COMPANIES_CONFIG_PATH,
    sec_metrics_path: Annotated[
        Path,
        typer.Option(help="SEC 指标映射配置文件路径，用于日报基本面评分。"),
    ] = DEFAULT_FUNDAMENTAL_METRICS_CONFIG_PATH,
    fundamental_feature_config_path: Annotated[
        Path,
        typer.Option(help="SEC 基本面特征公式配置文件路径，用于日报基本面评分。"),
    ] = DEFAULT_FUNDAMENTAL_FEATURES_CONFIG_PATH,
    sec_fundamentals_path: Annotated[
        Path | None,
        typer.Option(help="SEC 基本面指标 CSV 输入路径，用于日报基本面评分。"),
    ] = None,
    tsm_ir_quarterly_metrics_path: Annotated[
        Path,
        typer.Option(
            help=("TSMC IR 季度指标 CSV；存在时日报会按 as-of 合并到 " "SEC-style 指标后再校验。")
        ),
    ] = PROJECT_ROOT
    / "data"
    / "processed"
    / "tsm_ir_quarterly_metrics.csv",
    tsm_ir_merge: Annotated[
        bool,
        typer.Option(
            "--tsm-ir-merge/--skip-tsm-ir-merge",
            help="是否在日报基本面校验前合并 TSMC IR 季度指标。",
        ),
    ] = True,
    sec_fundamental_features_path: Annotated[
        Path | None,
        typer.Option(help="SEC 基本面特征 CSV 输出路径，用于日报基本面评分。"),
    ] = None,
    sec_fundamental_feature_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown SEC 基本面特征报告输出路径。"),
    ] = None,
    sec_metrics_validation_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown SEC 基本面指标 CSV 校验报告输出路径。"),
    ] = None,
    sec_fundamental_source: Annotated[
        Literal["legacy_companyfacts", "sec_pit_feature_panel"],
        typer.Option(
            "--sec-fundamental-source",
            help=(
                "日报基本面来源；legacy_companyfacts 使用 SEC-style 指标 CSV，"
                "sec_pit_feature_panel 使用 TRADING-039 reconstructed PIT panel。"
            ),
        ),
    ] = "legacy_companyfacts",
    sec_pit_feature_panel_path: Annotated[
        Path,
        typer.Option(help="SEC reconstructed PIT feature panel CSV 路径。"),
    ] = DEFAULT_SEC_EDGAR_PROCESSED_DIR
    / "sec_pit_feature_panel.csv",
    thesis_path: Annotated[
        Path,
        typer.Option(help="交易 thesis YAML 文件或目录路径，用于写入日报复核摘要。"),
    ] = PROJECT_ROOT
    / "data"
    / "external"
    / "trade_theses",
    risk_events_path: Annotated[
        Path,
        typer.Option(help="风险事件配置路径，用于写入日报复核摘要。"),
    ] = DEFAULT_RISK_EVENTS_CONFIG_PATH,
    risk_event_occurrences_path: Annotated[
        Path,
        typer.Option(help="风险事件发生记录 YAML 文件或目录路径，用于政策/地缘评分。"),
    ] = DEFAULT_RISK_EVENT_OCCURRENCES_PATH,
    risk_event_occurrence_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 风险事件发生记录报告输出路径。"),
    ] = None,
    risk_event_openai_precheck: Annotated[
        bool,
        typer.Option(
            "--risk-event-openai-precheck/--skip-risk-event-openai-precheck",
            help=(
                "默认在日报评分前抓取官方政策/地缘来源并调用 OpenAI 风险事件预审；"
                "输出只进入 llm_extracted / pending_review 队列。"
            ),
        ),
    ] = True,
    risk_event_prereview_queue_path: Annotated[
        Path,
        typer.Option(help="风险事件 OpenAI 预审待复核队列 JSON 输出路径。"),
    ] = DEFAULT_RISK_EVENT_PREREVIEW_QUEUE_PATH,
    risk_event_openai_precheck_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 风险事件 OpenAI 自动预审报告输出路径。"),
    ] = None,
    risk_event_openai_precheck_visibility_cutoff: Annotated[
        str | None,
        typer.Option(
            "--risk-event-openai-precheck-visibility-cutoff",
            help=(
                "生产日报 OpenAI request timestamp 可见时间上限，ISO datetime；"
                "仅允许用于最新已完成美股交易日。"
            ),
        ),
    ] = None,
    llm_request_profiles_path: Annotated[
        Path,
        typer.Option(help="LLM request profile 配置路径。"),
    ] = DEFAULT_LLM_REQUEST_PROFILES_CONFIG_PATH,
    llm_request_profile: Annotated[
        str,
        typer.Option(help="日报前 OpenAI 风险事件预审使用的 profile_id。"),
    ] = DEFAULT_RISK_EVENT_DAILY_PREREVIEW_PROFILE,
    risk_event_llm_formal_assessment: Annotated[
        bool | None,
        typer.Option(
            "--risk-event-llm-formal-assessment/--skip-risk-event-llm-formal-assessment",
            help=(
                "OpenAI 风险事件预审成功后，是否自动写入 LLM formal "
                "occurrence/attestation 作为政策/地缘正式评估输入。"
            ),
        ),
    ] = None,
    risk_event_llm_formal_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 风险事件 LLM formal assessment 报告输出路径。"),
    ] = None,
    risk_event_llm_formal_min_confidence: Annotated[
        float | None,
        typer.Option(
            help="覆盖 profile 中低于该 confidence 的 LLM 预审记录不写入正式 occurrence。"
        ),
    ] = None,
    risk_event_llm_formal_next_review_days: Annotated[
        int | None,
        typer.Option(help="覆盖 profile 中 LLM formal assessment 的下次复核间隔天数。"),
    ] = None,
    risk_event_llm_formal_overwrite: Annotated[
        bool | None,
        typer.Option(
            "--risk-event-llm-formal-overwrite/--no-risk-event-llm-formal-overwrite",
            help="覆盖 profile 中日报重复运行同一 as-of 时是否覆盖同名 LLM formal YAML。",
        ),
    ] = None,
    official_policy_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 官方政策/地缘来源自动抓取报告输出路径。"),
    ] = None,
    official_policy_source_ids: Annotated[
        str | None,
        typer.Option(help="日报前官方来源抓取 source_id 白名单，逗号分隔；为空抓取全部。"),
    ] = None,
    official_policy_limit: Annotated[
        int | None,
        typer.Option(help="覆盖 profile 中日报前官方来源抓取每个可分页来源最多请求的记录数。"),
    ] = None,
    risk_event_openai_precheck_max_candidates: Annotated[
        int | None,
        typer.Option(help="覆盖 profile 中日报前 OpenAI 风险事件预审最多处理的官方候选数。"),
    ] = None,
    openai_api_key_env: Annotated[
        str,
        typer.Option(help="日报前风险事件 OpenAI 预审读取 API key 的环境变量名。"),
    ] = "OPENAI_API_KEY",
    openai_model: Annotated[
        str | None,
        typer.Option(help="覆盖 profile 中日报前风险事件 OpenAI 预审使用的 Responses API 模型。"),
    ] = None,
    openai_reasoning_effort: Annotated[
        str | None,
        typer.Option(help="覆盖 profile 中日报前风险事件 OpenAI 预审 reasoning.effort。"),
    ] = None,
    openai_timeout_seconds: Annotated[
        float | None,
        typer.Option(
            help="覆盖 profile 中日报前风险事件 OpenAI 预审 Responses API 请求读超时秒数。"
        ),
    ] = None,
    openai_http_client: Annotated[
        str | None,
        typer.Option(
            help="覆盖 profile 中日报前风险事件 OpenAI 预审 HTTP 客户端：requests 或 urllib。"
        ),
    ] = None,
    openai_cache_dir: Annotated[
        Path,
        typer.Option(help="日报前 OpenAI 请求/响应本地缓存与审计归档目录。"),
    ] = DEFAULT_OPENAI_REQUEST_CACHE_PATH,
    openai_cache_ttl_hours: Annotated[
        float | None,
        typer.Option(help="覆盖 profile 中完全相同日报前 OpenAI 请求的本地缓存复用时长。"),
    ] = None,
    catalyst_calendar_path: Annotated[
        Path,
        typer.Option(help="未来催化剂日历 YAML 路径，用于日报告警摘要。"),
    ] = DEFAULT_CATALYST_CALENDAR_CONFIG_PATH,
    valuation_path: Annotated[
        Path,
        typer.Option(help="估值快照 YAML 文件或目录路径，用于写入日报复核摘要。"),
    ] = PROJECT_ROOT
    / "data"
    / "external"
    / "valuation_snapshots",
    trades_path: Annotated[
        Path,
        typer.Option(help="交易记录 YAML 文件或目录路径，用于写入日报复盘摘要。"),
    ] = PROJECT_ROOT
    / "data"
    / "external"
    / "trades",
    review_benchmarks: Annotated[
        str,
        typer.Option(help="逗号分隔的日报交易复盘归因基准标的。"),
    ] = ",".join(DEFAULT_BENCHMARK_TICKERS),
    full_universe: Annotated[
        bool,
        typer.Option(
            "--full-universe",
            help="校验并评分配置中的完整 AI 产业链标的。",
        ),
    ] = False,
) -> None:
    """构建特征并生成每日市场评分报告。"""
    if ctx is not None and ctx.invoked_subcommand is not None:
        return
    universe = load_universe()
    industry_chain = load_industry_chain()
    watchlist = load_watchlist()
    data_quality_config = load_data_quality()
    feature_config = load_features()
    scoring_rules = load_scoring_rules()
    portfolio = load_portfolio()
    market_regimes = load_market_regimes(DEFAULT_MARKET_REGIMES_CONFIG_PATH)
    default_market_regime = market_regime_by_id(
        market_regimes,
        market_regimes.default_backtest_regime,
    )
    score_started_at = datetime.now(tz=UTC)
    score_date = _parse_date(as_of) if as_of else resolve_daily_ops_default_as_of(score_started_at)
    production_score_date = resolve_daily_ops_default_as_of(score_started_at)
    explicit_risk_event_precheck_visibility_cutoff = (
        _parse_datetime(risk_event_openai_precheck_visibility_cutoff)
        if risk_event_openai_precheck_visibility_cutoff
        else None
    )
    if (
        explicit_risk_event_precheck_visibility_cutoff is not None
        and score_date != production_score_date
    ):
        raise typer.BadParameter(
            "risk_event_openai_precheck_visibility_cutoff 只能用于最新已完成美股交易日；"
            "历史 as-of 的 OpenAI request timestamp 仍按 as_of 当日 UTC 末尾 fail closed。"
        )
    benchmark_tickers = tuple(_parse_csv_items(review_benchmarks))
    if not benchmark_tickers:
        raise typer.BadParameter("日报交易复盘至少需要一个归因基准标的。")
    if prediction_production_effect not in {"production", "none"}:
        raise typer.BadParameter("prediction_production_effect 只能是 production 或 none。")
    if prediction_candidate_id != "production" and prediction_production_effect == "production":
        raise typer.BadParameter(
            "非 production 候选模型必须使用 prediction_production_effect=none。"
        )
    rule_governance_report = validate_rule_card_store(
        load_rule_card_store(rule_cards_path),
        as_of=date.today(),
    )
    if not rule_governance_report.passed:
        console.print("[red]规则治理校验失败，已停止日报评分。[/red]")
        console.print(
            f"错误数：{rule_governance_report.error_count}；"
            f"警告数：{rule_governance_report.warning_count}"
        )
        raise typer.Exit(code=1)
    daily_rule_version_manifest = build_rule_version_manifest(
        rule_governance_report,
        applies_to="score-daily",
    )
    if daily_rule_version_manifest["production_rule_count"] == 0:
        console.print(
            "[red]未找到适用于 score-daily 的 production rule card，已停止日报评分。[/red]"
        )
        raise typer.Exit(code=1)
    expected_price_tickers = configured_price_tickers(
        universe,
        include_full_ai_chain=full_universe,
    )
    expected_price_tickers = list(dict.fromkeys([*expected_price_tickers, *benchmark_tickers]))
    expected_rate_series = configured_rate_series(universe)
    quality_output = quality_report_path or default_quality_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        score_date,
    )
    feature_report_output = feature_report_path or default_feature_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        score_date,
    )
    feature_availability_output = (
        feature_availability_report_path
        or default_feature_availability_report_path(
            PROJECT_ROOT / "outputs" / "reports",
            score_date,
        )
    )
    score_report_output = report_path or default_daily_score_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        score_date,
    )
    alert_report_output = alert_report_path or default_alert_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        score_date,
    )
    portfolio_exposure_output = (
        portfolio_exposure_report_path
        or default_portfolio_exposure_report_path(
            PROJECT_ROOT / "outputs" / "reports",
            score_date,
        )
    )
    daily_trace_output = trace_bundle_path or default_report_trace_bundle_path(score_report_output)
    decision_snapshot_output = decision_snapshot_path or default_decision_snapshot_path(
        DEFAULT_DECISION_SNAPSHOT_DIR,
        score_date,
    )
    calculation_explainers_output = (
        calculation_explainers_path
        or default_calculation_explainers_path(
            score_report_output.parent,
            score_date,
        )
    )
    belief_state_output = belief_state_path or default_belief_state_path(
        DEFAULT_BELIEF_STATE_DIR,
        score_date,
    )
    belief_state_history_output = belief_state_history_path or DEFAULT_BELIEF_STATE_HISTORY_PATH
    sec_fundamentals_input = sec_fundamentals_path or default_sec_fundamental_metrics_csv_path(
        PROJECT_ROOT / "data" / "processed",
        score_date,
    )
    sec_metrics_validation_output = (
        sec_metrics_validation_report_path
        or default_sec_fundamental_metrics_validation_report_path(
            PROJECT_ROOT / "outputs" / "reports",
            score_date,
        )
    )
    sec_fundamental_features_output = (
        sec_fundamental_features_path
        or default_sec_fundamental_features_csv_path(
            PROJECT_ROOT / "data" / "processed",
            score_date,
        )
    )
    sec_fundamental_feature_report_output = (
        sec_fundamental_feature_report_path
        or default_sec_fundamental_features_report_path(
            PROJECT_ROOT / "outputs" / "reports",
            score_date,
        )
    )
    risk_event_occurrence_report_output = (
        risk_event_occurrence_report_path
        or default_risk_event_occurrence_report_path(
            PROJECT_ROOT / "outputs" / "reports",
            score_date,
        )
    )
    execution_policy_report_output = (
        execution_policy_report_path
        or default_execution_policy_report_path(
            PROJECT_ROOT / "outputs" / "reports",
            score_date,
        )
    )
    official_policy_report_output = (
        official_policy_report_path
        or default_official_policy_fetch_report_path(
            PROJECT_ROOT / "outputs" / "reports",
            score_date,
        )
    )
    risk_event_openai_precheck_report_output = (
        risk_event_openai_precheck_report_path
        or default_risk_event_openai_prereview_report_path(
            PROJECT_ROOT / "outputs" / "reports",
            score_date,
        )
    )
    risk_event_llm_formal_report_output = (
        risk_event_llm_formal_report_path
        or default_llm_formal_assessment_report_path(
            PROJECT_ROOT / "outputs" / "reports",
            score_date,
        )
    )

    data_quality_report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=expected_price_tickers,
        expected_rate_series=expected_rate_series,
        quality_config=data_quality_config,
        as_of=score_date,
        manifest_path=_download_manifest_path(prices_path),
        secondary_prices_path=_marketstack_prices_path(prices_path),
        require_secondary_prices=_requires_marketstack_prices(prices_path),
    )
    write_data_quality_report(data_quality_report, quality_output)
    if not data_quality_report.passed:
        console.print("[red]数据质量门禁失败，已停止每日评分。[/red]")
        console.print(f"数据质量报告：{quality_output}")
        console.print(
            f"错误数：{data_quality_report.error_count}；"
            f"警告数：{data_quality_report.warning_count}"
        )
        raise typer.Exit(code=1)

    execution_policy_report = validate_execution_policy(
        load_execution_policy(execution_policy_path),
        as_of=score_date,
    )
    write_execution_policy_report(
        execution_policy_report,
        execution_policy_report_output,
    )
    if not execution_policy_report.passed:
        console.print("[red]执行政策校验失败，已停止每日评分。[/red]")
        console.print(f"执行政策报告：{execution_policy_report_output}")
        console.print(
            f"错误数：{execution_policy_report.error_count}；"
            f"警告数：{execution_policy_report.warning_count}"
        )
        raise typer.Exit(code=1)
    if execution_policy_report.store.policy is None:
        console.print("[red]执行政策为空，已停止每日评分。[/red]")
        raise typer.Exit(code=1)

    prices_frame = pd.read_csv(prices_path)
    rates_frame = pd.read_csv(rates_path)
    fundamental_source_id = (
        "sec_edgar_reconstructed_pit_features"
        if sec_fundamental_source == "sec_pit_feature_panel"
        else "sec_fundamental_features"
    )
    feature_source_checks = list(
        _market_feature_source_checks(
            prices_frame=prices_frame,
            rates_frame=rates_frame,
            prices_path=prices_path,
            rates_path=rates_path,
            decision_time=score_date,
        )
    )
    if sec_fundamental_source == "sec_pit_feature_panel":
        feature_source_checks.append(
            _sec_pit_feature_source_check(
                feature_panel_path=sec_pit_feature_panel_path,
                decision_time=score_date,
            )
        )
    feature_set = build_market_features(
        prices=prices_frame,
        rates=rates_frame,
        config=feature_config,
        as_of=score_date,
        core_watchlist=universe.ai_chain.get("core_watchlist", []),
    )
    feature_availability_report = build_feature_availability_report(
        input_path=feature_availability_path,
        as_of=score_date,
        observed_sources=tuple(sorted({row.source for row in feature_set.rows})),
        required_sources=(
            "prices_daily",
            "rates_daily",
            fundamental_source_id,
            "valuation_snapshots",
            "risk_event_occurrences",
        ),
        source_checks=tuple(feature_source_checks),
    )
    write_feature_availability_report(
        feature_availability_report,
        feature_availability_output,
    )
    if not feature_availability_report.passed:
        console.print("[red]PIT 特征可见时间校验失败，已停止每日评分。[/red]")
        console.print(f"PIT 特征可见时间报告：{feature_availability_output}")
        console.print(
            f"错误数：{feature_availability_report.error_count}；"
            f"警告数：{feature_availability_report.warning_count}"
        )
        raise typer.Exit(code=1)
    feature_availability_section = render_feature_availability_section(
        feature_availability_report,
        feature_availability_output,
    )
    features_output = write_features_csv(feature_set, features_path)
    feature_summary_output = write_feature_summary(
        feature_set,
        data_quality_report=data_quality_report,
        data_quality_report_path=quality_output,
        features_path=features_output,
        output_path=feature_report_output,
        feature_availability_section=feature_availability_section,
    )
    portfolio_exposure_report = build_portfolio_exposure_report(
        input_path=portfolio_positions_path,
        as_of=score_date,
        industry_chain=industry_chain,
        watchlist=watchlist,
    )
    write_portfolio_exposure_report(
        portfolio_exposure_report,
        portfolio_exposure_output,
    )
    if not portfolio_exposure_report.passed:
        console.print("[red]组合持仓输入校验失败，已停止每日评分。[/red]")
        console.print(f"组合暴露报告：{portfolio_exposure_output}")
        console.print(
            f"错误数：{portfolio_exposure_report.error_count}；"
            f"警告数：{portfolio_exposure_report.warning_count}"
        )
        raise typer.Exit(code=1)
    sec_companies = load_sec_companies(sec_companies_path)
    sec_metrics = load_fundamental_metrics(sec_metrics_path)
    if sec_fundamental_source == "sec_pit_feature_panel":
        sec_fundamental_feature_report = _build_score_daily_sec_pit_feature_report(
            score_date=score_date,
            sec_companies=sec_companies,
            sec_pit_feature_panel_path=sec_pit_feature_panel_path,
        )
        sec_metrics_validation_reference = sec_pit_feature_panel_path
    else:
        if tsm_ir_merge:
            _merge_tsm_ir_for_daily_score(
                sec_fundamentals_path=sec_fundamentals_input,
                tsm_ir_path=tsm_ir_quarterly_metrics_path,
                sec_companies=sec_companies,
                as_of=score_date,
            )
        sec_metrics_validation_report = validate_sec_fundamental_metrics_csv(
            companies=sec_companies,
            metrics=sec_metrics,
            input_path=sec_fundamentals_input,
            as_of=score_date,
        )
        write_sec_fundamental_metrics_validation_report(
            sec_metrics_validation_report,
            sec_metrics_validation_output,
        )
        if not sec_metrics_validation_report.passed:
            console.print("[red]SEC 基本面指标 CSV 校验失败，已停止每日评分。[/red]")
            console.print(f"SEC 指标 CSV 校验报告：{sec_metrics_validation_output}")
            console.print(f"SEC 指标 CSV：{sec_fundamentals_input}")
            console.print(
                f"错误数：{sec_metrics_validation_report.error_count}；"
                f"警告数：{sec_metrics_validation_report.warning_count}"
            )
            raise typer.Exit(code=1)

        sec_fundamental_feature_report = build_sec_fundamental_features_report(
            companies=sec_companies,
            feature_config=load_fundamental_features(fundamental_feature_config_path),
            input_path=sec_fundamentals_input,
            as_of=score_date,
            validation_report=sec_metrics_validation_report,
        )
        sec_metrics_validation_reference = sec_metrics_validation_output
    if not sec_fundamental_feature_report.passed:
        write_sec_fundamental_features_report(
            report=sec_fundamental_feature_report,
            validation_report_path=sec_metrics_validation_reference,
            output_csv_path=sec_fundamental_features_output,
            output_path=sec_fundamental_feature_report_output,
        )
        console.print("[red]SEC 基本面特征构建失败，已停止每日评分。[/red]")
        console.print(f"SEC 基本面特征报告：{sec_fundamental_feature_report_output}")
        console.print(f"SEC 指标 CSV 校验报告：{sec_metrics_validation_output}")
        console.print(
            f"错误数：{sec_fundamental_feature_report.error_count}；"
            f"警告数：{sec_fundamental_feature_report.warning_count}"
        )
        raise typer.Exit(code=1)

    sec_fundamental_features_output = write_sec_fundamental_features_csv(
        sec_fundamental_feature_report,
        sec_fundamental_features_output,
    )
    sec_fundamental_feature_report_output = write_sec_fundamental_features_report(
        report=sec_fundamental_feature_report,
        validation_report_path=sec_metrics_validation_reference,
        output_csv_path=sec_fundamental_features_output,
        output_path=sec_fundamental_feature_report_output,
    )
    valuation_validation_report = validate_valuation_snapshot_store(
        store=load_valuation_snapshot_store(valuation_path),
        universe=universe,
        watchlist=watchlist,
        as_of=score_date,
    )
    valuation_review_report = build_valuation_review_report(valuation_validation_report)
    if not valuation_validation_report.passed:
        console.print("[red]估值快照校验失败，已停止每日评分。[/red]")
        console.print(
            f"错误数：{valuation_validation_report.error_count}；"
            f"警告数：{valuation_validation_report.warning_count}"
        )
        raise typer.Exit(code=1)
    risk_events_config = load_risk_events(risk_events_path)
    risk_events_validation_report = validate_risk_events_config(
        risk_events=risk_events_config,
        industry_chain=industry_chain,
        watchlist=watchlist,
        universe=universe,
        as_of=score_date,
    )
    if not risk_events_validation_report.passed:
        console.print("[red]风险事件规则校验失败，已停止每日评分。[/red]")
        console.print(
            f"错误数：{risk_events_validation_report.error_count}；"
            f"警告数：{risk_events_validation_report.warning_count}"
        )
        raise typer.Exit(code=1)
    official_policy_fetch_report = None
    risk_event_prereview_report = None
    llm_formal_report = None
    if risk_event_openai_precheck:
        llm_profile = _load_llm_request_profile(
            llm_request_profiles_path,
            llm_request_profile,
        )
        effective_openai_model = _coalesce_profile_value(openai_model, llm_profile.model)
        effective_openai_reasoning_effort = _coalesce_profile_value(
            openai_reasoning_effort,
            llm_profile.reasoning_effort,
        )
        effective_openai_timeout_seconds = _coalesce_profile_value(
            openai_timeout_seconds,
            llm_profile.timeout_seconds,
        )
        effective_openai_http_client = _coalesce_profile_value(
            openai_http_client,
            llm_profile.http_client,
        )
        effective_openai_cache_ttl_hours = _coalesce_profile_value(
            openai_cache_ttl_hours,
            llm_profile.cache_ttl_hours,
        )
        effective_risk_event_openai_precheck_max_candidates = _coalesce_profile_value(
            risk_event_openai_precheck_max_candidates,
            llm_profile.max_candidates,
        )
        effective_official_policy_limit = _coalesce_profile_value(
            official_policy_limit,
            llm_profile.official_policy_limit,
        )
        effective_risk_event_llm_formal_assessment = _coalesce_profile_value(
            risk_event_llm_formal_assessment,
            llm_profile.formal_assessment.enabled,
        )
        effective_risk_event_llm_formal_min_confidence = _coalesce_profile_value(
            risk_event_llm_formal_min_confidence,
            llm_profile.formal_assessment.min_confidence,
        )
        effective_risk_event_llm_formal_next_review_days = _coalesce_profile_value(
            risk_event_llm_formal_next_review_days,
            llm_profile.formal_assessment.next_review_days,
        )
        effective_risk_event_llm_formal_overwrite = _coalesce_profile_value(
            risk_event_llm_formal_overwrite,
            llm_profile.formal_assessment.overwrite,
        )
        if effective_risk_event_openai_precheck_max_candidates is None:
            raise typer.BadParameter("LLM request profile 必须设置 max_candidates，或显式传入。")
        if effective_official_policy_limit is None:
            raise typer.BadParameter(
                "LLM request profile 必须设置 official_policy_limit，或显式传入。"
            )
        if effective_risk_event_openai_precheck_max_candidates < 0:
            raise typer.BadParameter("OpenAI 风险事件预审候选上限不能为负数。")
        if effective_openai_timeout_seconds <= 0:
            raise typer.BadParameter("OpenAI 风险事件预审超时秒数必须为正数。")
        if effective_openai_cache_ttl_hours <= 0:
            raise typer.BadParameter("OpenAI 请求缓存 TTL 小时数必须为正数。")
        if (
            effective_risk_event_llm_formal_min_confidence < 0
            or effective_risk_event_llm_formal_min_confidence > 1
        ):
            raise typer.BadParameter("LLM formal min confidence 必须在 0 到 1 之间。")
        if effective_risk_event_llm_formal_next_review_days < 0:
            raise typer.BadParameter("LLM formal next review days 不能为负数。")
        if not os.getenv(openai_api_key_env, ""):
            console.print("[red]缺少 OpenAI API key，已停止日报前风险事件预审。[/red]")
            console.print(f"需要环境变量：{openai_api_key_env}")
            raise typer.Exit(code=1)
        selected_official_source_ids = (
            _parse_csv_items(official_policy_source_ids) if official_policy_source_ids else None
        )
        official_policy_fetch_report = fetch_official_policy_sources(
            as_of=score_date,
            since=None,
            raw_dir=DEFAULT_OFFICIAL_POLICY_RAW_DIR,
            processed_dir=DEFAULT_OFFICIAL_POLICY_PROCESSED_DIR,
            api_keys={
                "CONGRESS_API_KEY": os.getenv("CONGRESS_API_KEY", ""),
                "GOVINFO_API_KEY": os.getenv("GOVINFO_API_KEY", ""),
            },
            selected_source_ids=selected_official_source_ids,
            limit=effective_official_policy_limit,
            download_manifest_path=PROJECT_ROOT / "data" / "raw" / "download_manifest.csv",
        )
        write_official_policy_fetch_report(
            official_policy_fetch_report,
            official_policy_report_output,
        )
        if not official_policy_fetch_report.passed:
            console.print("[red]官方政策/地缘来源抓取失败，已停止每日评分。[/red]")
            console.print(f"官方来源抓取报告：{official_policy_report_output}")
            console.print(
                f"错误数：{official_policy_fetch_report.error_count}；"
                f"警告数：{official_policy_fetch_report.warning_count}"
            )
            raise typer.Exit(code=1)
        official_candidates_path = default_official_policy_candidates_path(
            DEFAULT_OFFICIAL_POLICY_PROCESSED_DIR,
            score_date,
        )
        risk_event_prereview_report = run_openai_risk_event_prereview_for_official_candidates(
            official_policy_fetch_report.candidates,
            api_key=os.getenv(openai_api_key_env, ""),
            data_sources=load_data_sources(DEFAULT_DATA_SOURCES_CONFIG_PATH),
            risk_events=risk_events_config,
            input_path=official_candidates_path,
            as_of=score_date,
            model=effective_openai_model,
            reasoning_effort=effective_openai_reasoning_effort,
            endpoint=llm_profile.endpoint,
            timeout_seconds=effective_openai_timeout_seconds,
            http_client=effective_openai_http_client,
            openai_cache_dir=openai_cache_dir,
            openai_cache_ttl_seconds=effective_openai_cache_ttl_hours * 3600,
            max_retries=llm_profile.max_retries,
            generated_at=explicit_risk_event_precheck_visibility_cutoff,
            request_visibility_cutoff=explicit_risk_event_precheck_visibility_cutoff,
            max_candidates=effective_risk_event_openai_precheck_max_candidates,
        )
        write_risk_event_prereview_import_report(
            risk_event_prereview_report,
            risk_event_openai_precheck_report_output,
        )
        if not risk_event_prereview_report.passed:
            console.print("[red]风险事件 OpenAI 自动预审失败，已停止每日评分。[/red]")
            console.print(f"OpenAI 预审报告：{risk_event_openai_precheck_report_output}")
            console.print(
                f"错误数：{risk_event_prereview_report.error_count}；"
                f"警告数：{risk_event_prereview_report.warning_count}"
            )
            raise typer.Exit(code=1)
        write_risk_event_prereview_queue(
            risk_event_prereview_report,
            risk_event_prereview_queue_path,
        )
        console.print(
            "[green]日报前风险事件 OpenAI 自动预审完成。[/green] "
            f"官方候选 {official_policy_fetch_report.candidate_count}；"
            f"待复核队列 {risk_event_prereview_report.record_count}；"
            f"L2/L3 候选 {risk_event_prereview_report.high_level_candidate_count}"
        )
        console.print(f"官方来源抓取报告：{official_policy_report_output}")
        console.print(f"OpenAI 预审报告：{risk_event_openai_precheck_report_output}")
        console.print(f"预审待复核队列：{risk_event_prereview_queue_path}")
        console.print(
            f"LLM request profile：{llm_profile.profile_id}；"
            f"model={effective_openai_model}；"
            f"reasoning={effective_openai_reasoning_effort}；"
            f"max_candidates={effective_risk_event_openai_precheck_max_candidates}"
        )
        if effective_risk_event_llm_formal_assessment:
            try:
                llm_formal_report = build_llm_formal_assessment_report(
                    risk_event_prereview_queue_path,
                    as_of=score_date,
                    risk_events=risk_events_config,
                    include_attestation=True,
                    next_review_days=effective_risk_event_llm_formal_next_review_days,
                    min_confidence=effective_risk_event_llm_formal_min_confidence,
                )
                write_llm_formal_assessment_report(
                    llm_formal_report,
                    risk_event_llm_formal_report_output,
                )
                if not llm_formal_report.passed:
                    console.print(
                        "[red]风险事件 LLM formal assessment 失败，已停止每日评分。[/red]"
                    )
                    console.print(f"LLM formal 报告：{risk_event_llm_formal_report_output}")
                    console.print(
                        f"错误数：{llm_formal_report.error_count}；"
                        f"警告数：{llm_formal_report.warning_count}"
                    )
                    raise typer.Exit(code=1)
                written_llm_formal_paths = write_llm_formal_assessment_outputs(
                    llm_formal_report,
                    risk_event_occurrences_path,
                    overwrite=effective_risk_event_llm_formal_overwrite,
                )
            except (OSError, ValueError, FileExistsError) as exc:
                console.print(f"[red]风险事件 LLM formal assessment 写入失败：{exc}[/red]")
                raise typer.Exit(code=1) from exc
            console.print(
                "[green]风险事件 LLM formal assessment 已写入。[/green] "
                f"occurrence={llm_formal_report.occurrence_count}；"
                f"attestation={'是' if llm_formal_report.attestation else '否'}；"
                f"YAML={len(written_llm_formal_paths)}"
            )
            console.print(f"LLM formal 报告：{risk_event_llm_formal_report_output}")
    risk_event_occurrence_validation_report = validate_risk_event_occurrence_store(
        store=load_risk_event_occurrence_store(risk_event_occurrences_path),
        risk_events=risk_events_config,
        as_of=score_date,
    )
    risk_event_occurrence_review_report = build_risk_event_occurrence_review_report(
        risk_event_occurrence_validation_report
    )
    write_risk_event_occurrence_review_report(
        risk_event_occurrence_review_report,
        risk_event_occurrence_report_output,
    )
    if not risk_event_occurrence_validation_report.passed:
        console.print("[red]风险事件发生记录校验失败，已停止每日评分。[/red]")
        console.print(f"风险事件发生记录报告：{risk_event_occurrence_report_output}")
        console.print(
            f"错误数：{risk_event_occurrence_validation_report.error_count}；"
            f"警告数：{risk_event_occurrence_validation_report.warning_count}"
        )
        raise typer.Exit(code=1)
    catalyst_calendar_report = validate_catalyst_calendar(
        load_catalyst_calendar(catalyst_calendar_path),
        as_of=score_date,
        industry_chain=industry_chain,
        watchlist=watchlist,
        risk_events=risk_events_config,
    )
    thesis_review_report = _build_daily_thesis_review_report(
        input_path=thesis_path,
        watchlist=watchlist,
        industry_chain=industry_chain,
        score_date=score_date,
    )

    review_summary = _build_daily_review_summary(
        thesis_path=thesis_path,
        thesis_review_report=thesis_review_report,
        risk_events_path=risk_events_path,
        risk_event_occurrences_path=risk_event_occurrences_path,
        risk_events_validation_report=risk_events_validation_report,
        risk_event_occurrence_review_report=risk_event_occurrence_review_report,
        valuation_path=valuation_path,
        valuation_review_report=valuation_review_report,
        trades_path=trades_path,
        score_date=score_date,
        universe=universe,
        industry_chain=industry_chain,
        watchlist=watchlist,
        prices=prices_frame,
        data_quality_report=data_quality_report,
        benchmark_tickers=benchmark_tickers,
    )
    core_watchlist_tickers = tuple(universe.ai_chain.get("core_watchlist", []))
    focus_stock_trend_report = build_focus_stock_trend_report(
        feature_set=feature_set,
        tickers=core_watchlist_tickers,
        watchlist=watchlist,
    )
    focus_stock_trend_section = render_focus_stock_trend_section(focus_stock_trend_report)
    industry_node_heat_report = build_industry_node_heat_report(
        industry_chain=industry_chain,
        watchlist=watchlist,
        feature_set=feature_set,
        fundamental_feature_report=sec_fundamental_feature_report,
        valuation_review_report=valuation_review_report,
        risk_event_occurrence_review_report=risk_event_occurrence_review_report,
        thesis_review_report=thesis_review_report,
    )
    industry_node_heat_section = render_industry_node_heat_section(industry_node_heat_report)
    weight_calibration_context = build_weight_calibration_context(
        feature_set=feature_set,
        data_quality_report=data_quality_report,
        fundamental_feature_report=sec_fundamental_feature_report,
        valuation_review_report=valuation_review_report,
        risk_event_occurrence_review_report=risk_event_occurrence_review_report,
        run_type="score_daily",
        market_regime_id=default_market_regime.regime_id,
    )
    weight_calibration_application = resolve_calibration_application(
        context=weight_calibration_context,
        as_of=score_date,
    )
    current_context_output: Path | None = None
    current_effective_weights_output: Path | None = None
    if report_path is None:
        current_context_output = write_calibration_context(
            weight_calibration_context,
            DEFAULT_CURRENT_CONTEXT_PATH,
        )
        current_effective_weights_output = write_effective_weights(
            weight_calibration_application,
            DEFAULT_EFFECTIVE_WEIGHTS_PATH,
        )
    score_report = build_daily_score_report(
        feature_set=feature_set,
        data_quality_report=data_quality_report,
        rules=scoring_rules,
        total_risk_asset_min=portfolio.portfolio.total_risk_asset_min,
        total_risk_asset_max=portfolio.portfolio.total_risk_asset_max,
        max_total_ai_exposure=portfolio.position_limits.max_total_ai_exposure,
        macro_risk_asset_budget=portfolio.macro_risk_asset_budget,
        risk_budget=portfolio.risk_budget,
        portfolio_exposure_report=portfolio_exposure_report,
        review_summary=review_summary,
        fundamental_feature_report=sec_fundamental_feature_report,
        valuation_review_report=valuation_review_report,
        risk_event_occurrence_review_report=risk_event_occurrence_review_report,
        weight_calibration=weight_calibration_application,
    )
    previous_score_snapshot = load_previous_daily_score_snapshot(scores_path, score_date)
    daily_alert_report = build_daily_alert_report(
        score_report,
        previous_score_snapshot=previous_score_snapshot,
        catalyst_calendar_report=catalyst_calendar_report,
        data_quality_report_path=quality_output,
        risk_event_occurrence_report_path=risk_event_occurrence_report_output,
        valuation_report_path=valuation_path,
        catalyst_calendar_path=catalyst_calendar_path,
    )
    daily_alert_output = write_alert_report(daily_alert_report, alert_report_output)
    previous_execution_band = None
    if (
        previous_score_snapshot is not None
        and previous_score_snapshot.final_risk_asset_ai_min is not None
        and previous_score_snapshot.final_risk_asset_ai_max is not None
    ):
        previous_execution_band = (
            previous_score_snapshot.final_risk_asset_ai_min,
            previous_score_snapshot.final_risk_asset_ai_max,
        )
    execution_advisory = build_execution_advisory(
        policy=execution_policy_report.store.policy,
        current_band=(
            score_report.recommendation.risk_asset_ai_band.min_position,
            score_report.recommendation.risk_asset_ai_band.max_position,
        ),
        previous_band=previous_execution_band,
        confidence_level=score_report.confidence_assessment.level,
        triggered_gate_ids=tuple(
            gate.gate_id for gate in score_report.recommendation.triggered_position_gates
        ),
        report_status=score_report.status,
    )
    execution_advisory_section = render_execution_advisory_section(
        execution_advisory,
        validation_status=execution_policy_report.status,
        validation_report_path=execution_policy_report_output,
    )
    scores_output = write_scores_csv(score_report, scores_path)
    daily_market_regime = BacktestRegimeContext(
        regime_id=default_market_regime.regime_id,
        name=default_market_regime.name,
        start_date=default_market_regime.start_date,
        anchor_date=default_market_regime.anchor_date,
        anchor_event=default_market_regime.anchor_event,
        description=default_market_regime.description,
    )
    daily_config_paths = _daily_trace_config_paths(
        sec_companies_path=sec_companies_path,
        sec_metrics_path=sec_metrics_path,
        fundamental_feature_config_path=fundamental_feature_config_path,
        risk_events_path=risk_events_path,
        execution_policy_path=execution_policy_path,
        rule_cards_path=rule_cards_path,
        feature_availability_path=feature_availability_path,
    )
    daily_trace_bundle = build_daily_score_trace_bundle(
        report=score_report,
        report_path=score_report_output,
        data_quality_report_path=quality_output,
        feature_report_path=feature_summary_output,
        features_path=features_output,
        scores_path=scores_output,
        market_regime=daily_market_regime,
        config_paths=daily_config_paths,
        rule_version_manifest=daily_rule_version_manifest,
        sec_metrics_validation_report_path=sec_metrics_validation_output,
        sec_fundamental_feature_report_path=sec_fundamental_feature_report_output,
        sec_fundamental_features_path=sec_fundamental_features_output,
        risk_event_occurrence_report_path=risk_event_occurrence_report_output,
        belief_state_path=belief_state_output,
        feature_availability_report_path=feature_availability_output,
        feature_availability_summary=feature_availability_summary_record(
            feature_availability_report,
            feature_availability_output,
        ),
        focus_stock_trend_tickers=core_watchlist_tickers,
        run_id=run_id,
    )
    daily_trace_output = write_trace_bundle(daily_trace_bundle, daily_trace_output)
    belief_state = build_belief_state(
        report=score_report,
        trace_bundle_path=daily_trace_output,
        decision_snapshot_path=decision_snapshot_output,
        market_regime=daily_market_regime,
        config_paths=daily_config_paths,
    )
    belief_state_output = write_belief_state(belief_state, belief_state_output)
    belief_state_history_output = append_belief_state_history(
        belief_state,
        belief_state_output,
        belief_state_history_output,
    )
    decision_snapshot = build_decision_snapshot(
        report=score_report,
        trace_bundle_path=daily_trace_output,
        market_regime=daily_market_regime,
        config_paths=daily_config_paths,
        belief_state_path=belief_state_output,
        rule_version_manifest=daily_rule_version_manifest,
    )
    daily_decision_snapshot_output = write_decision_snapshot(
        decision_snapshot,
        decision_snapshot_output,
    )
    calculation_explainers_payload = build_calculation_explainers_payload(
        as_of=score_date,
        decision_snapshot_path=daily_decision_snapshot_output,
        scores_daily_path=scores_output,
    )
    calculation_explainers_output = write_calculation_explainers_json(
        calculation_explainers_payload,
        calculation_explainers_output,
    )
    prediction_ledger_output = append_prediction_records(
        (
            build_prediction_record_from_decision_snapshot(
                snapshot=decision_snapshot,
                trace_bundle=daily_trace_bundle.to_dict(),
                trace_bundle_path=daily_trace_output,
                features_path=features_output,
                data_quality_report_path=quality_output,
                candidate_id=prediction_candidate_id,
                production_effect=prediction_production_effect,
            ),
        ),
        prediction_ledger_path,
    )
    daily_report_output = write_daily_score_report(
        score_report,
        data_quality_report_path=quality_output,
        feature_report_path=feature_summary_output,
        features_path=features_output,
        scores_path=scores_output,
        output_path=score_report_output,
        sec_metrics_validation_report_path=sec_metrics_validation_output,
        sec_fundamental_feature_report_path=sec_fundamental_feature_report_output,
        sec_fundamental_features_path=sec_fundamental_features_output,
        risk_event_occurrence_report_path=risk_event_occurrence_report_output,
        feature_availability_section=feature_availability_section,
        previous_score_snapshot=previous_score_snapshot,
        belief_state_section=render_belief_state_summary(
            belief_state,
            belief_state_output,
        ),
        execution_action_label=execution_advisory.label,
        execution_action_id=execution_advisory.action_id,
        focus_stock_trend_section=focus_stock_trend_section,
        industry_node_heat_section=industry_node_heat_section,
        execution_advisory_section=execution_advisory_section,
        portfolio_exposure_section=(
            render_portfolio_exposure_section(portfolio_exposure_report).rstrip()
            + f"\n- 独立报告：`{portfolio_exposure_output}`"
        ),
        alert_summary_section=render_alert_summary_section(
            daily_alert_report,
            report_path=daily_alert_output,
        ),
        risk_event_openai_precheck_section=(
            _risk_event_openai_precheck_daily_section(
                official_policy_fetch_report=official_policy_fetch_report,
                risk_event_prereview_report=risk_event_prereview_report,
                official_policy_report_output=official_policy_report_output,
                risk_event_openai_precheck_report_output=(risk_event_openai_precheck_report_output),
                risk_event_prereview_queue_path=risk_event_prereview_queue_path,
                llm_formal_report=llm_formal_report,
                risk_event_llm_formal_report_output=(risk_event_llm_formal_report_output),
                llm_profile_id=llm_profile.profile_id,
                llm_formal_enabled=effective_risk_event_llm_formal_assessment,
                model=effective_openai_model,
                reasoning_effort=effective_openai_reasoning_effort,
                timeout_seconds=effective_openai_timeout_seconds,
                http_client=effective_openai_http_client,
                cache_dir=openai_cache_dir,
                cache_ttl_hours=effective_openai_cache_ttl_hours,
                max_candidates=effective_risk_event_openai_precheck_max_candidates,
                request_visibility_cutoff=explicit_risk_event_precheck_visibility_cutoff,
            )
            if risk_event_openai_precheck
            and official_policy_fetch_report is not None
            and risk_event_prereview_report is not None
            else None
        ),
        traceability_section=render_traceability_section(
            daily_trace_bundle,
            daily_trace_output,
        ),
        run_id=run_id or daily_trace_bundle.run_manifest["run_id"],
        trace_bundle_path=daily_trace_output,
    )

    status_style = "green" if score_report.status == "PASS" else "yellow"
    console.print(f"[{status_style}]每日评分状态：{score_report.status}[/{status_style}]")
    console.print(f"AI 产业链评分：{score_report.recommendation.total_score:.1f}")
    console.print(
        "判断置信度："
        f"{score_report.confidence_assessment.score:.1f}"
        f"（{score_report.confidence_assessment.level}）"
    )
    console.print(f"仓位状态：{score_report.recommendation.label}")
    console.print(f"执行建议：{execution_advisory.label}（{execution_advisory.action_id}）")
    console.print(
        f"产业链节点热度与健康度：{industry_node_heat_report.status}"
        f"（{industry_node_heat_report.node_count} 个节点）"
    )
    console.print(
        f"关注股票趋势分析：{focus_stock_trend_report.status}"
        f"（{focus_stock_trend_report.ticker_count} 个 ticker）"
    )
    console.print(
        f"组合暴露：{portfolio_exposure_report.status}"
        f"（AI 占比 {portfolio_exposure_report.ai_exposure_pct_total:.1%}）"
    )
    console.print(
        f"告警状态：{daily_alert_report.status}" f"（{len(daily_alert_report.alerts)} 条）"
    )
    console.print(f"每日评分报告：{daily_report_output}")
    console.print(f"告警报告：{daily_alert_output}")
    console.print(f"Evidence bundle：{daily_trace_output}")
    console.print(f"Decision snapshot：{daily_decision_snapshot_output}")
    console.print(f"Calculation explainers：{calculation_explainers_output}")
    if current_context_output is not None and current_effective_weights_output is not None:
        console.print(f"Current context：{current_context_output}")
        console.print(f"Current effective weights：{current_effective_weights_output}")
    console.print(f"Prediction ledger：{prediction_ledger_output}")
    console.print(f"Belief state：{belief_state_output}")
    console.print(f"Belief state history：{belief_state_history_output}")
    console.print(f"评分数据：{scores_output}")
    console.print(f"特征摘要：{feature_summary_output}")
    console.print(
        f"PIT 特征可见时间报告：{feature_availability_output}"
        f"（{feature_availability_report.status}）"
    )
    console.print(
        f"SEC 基本面特征：{sec_fundamental_features_output}"
        f"（{sec_fundamental_feature_report.status}）"
    )
    console.print(
        f"风险事件发生记录：{risk_event_occurrence_report_output}"
        f"（{risk_event_occurrence_review_report.status}）"
    )
    if risk_event_openai_precheck:
        console.print(f"官方政策/地缘抓取报告：{official_policy_report_output}")
        console.print(f"风险事件 OpenAI 预审报告：{risk_event_openai_precheck_report_output}")
        console.print(f"风险事件预审队列：{risk_event_prereview_queue_path}")
        console.print(f"LLM request profile：{llm_profile.profile_id}")
        if llm_formal_report is not None:
            console.print(f"风险事件 LLM formal 报告：{risk_event_llm_formal_report_output}")
    console.print(
        f"组合暴露报告：{portfolio_exposure_output}" f"（{portfolio_exposure_report.status}）"
    )
    console.print(
        f"执行政策报告：{execution_policy_report_output}" f"（{execution_policy_report.status}）"
    )
    console.print(f"数据质量报告：{quality_output}（{data_quality_report.status}）")
    console.print(
        "规则版本："
        f"{daily_rule_version_manifest['production_rule_count']} 个 production rule cards"
        f"（{rule_governance_report.status}）"
    )


def _merge_tsm_ir_for_daily_score(
    *,
    sec_fundamentals_path: Path,
    tsm_ir_path: Path,
    sec_companies: object,
    as_of: date,
) -> None:
    tsm_ir_required = any(
        company.active
        and company.ticker.upper() == "TSM"
        and "quarterly" in company.sec_metric_periods
        for company in sec_companies.companies
    )
    if not tsm_ir_required or not tsm_ir_path.exists():
        return

    all_tsm_rows = load_tsm_ir_quarterly_metric_rows_csv(tsm_ir_path)
    selected_tsm_rows = select_tsm_ir_quarterly_metric_rows_as_of(all_tsm_rows, as_of)
    if not selected_tsm_rows:
        return

    try:
        existing_rows = load_sec_fundamental_metric_rows_csv(sec_fundamentals_path)
        merged_rows = merge_tsm_ir_quarterly_rows_into_sec_metrics_as_of(
            existing_rows=existing_rows,
            tsm_rows=all_tsm_rows,
            tsm_company=sec_companies,
            as_of=as_of,
        )
    except ValueError as exc:
        console.print("[red]TSMC IR 日报基本面合并失败，已停止每日评分。[/red]")
        console.print(str(exc))
        raise typer.Exit(code=1) from exc

    write_sec_fundamental_metric_rows_csv(merged_rows, sec_fundamentals_path)
    console.print(
        "TSMC IR 已合并到 SEC-style 基本面指标："
        f"{sec_fundamentals_path}（TSM 行数 {len(selected_tsm_rows)}）"
    )


def _risk_event_openai_precheck_daily_section(
    *,
    official_policy_fetch_report,
    risk_event_prereview_report,
    official_policy_report_output: Path,
    risk_event_openai_precheck_report_output: Path,
    risk_event_prereview_queue_path: Path,
    llm_formal_report,
    risk_event_llm_formal_report_output: Path,
    llm_profile_id: str,
    llm_formal_enabled: bool,
    model: str,
    reasoning_effort: str,
    timeout_seconds: float,
    http_client: str,
    cache_dir: Path,
    cache_ttl_hours: float,
    max_candidates: int,
    request_visibility_cutoff: datetime | None,
) -> str:
    as_of_text = (
        risk_event_prereview_report.as_of.isoformat()
        if risk_event_prereview_report.as_of is not None
        else "未记录"
    )
    visibility_cutoff_text = (
        request_visibility_cutoff.astimezone(UTC).isoformat()
        if request_visibility_cutoff is not None
        else "未显式传入，request_timestamp 按 as_of 当日 UTC 末尾 fail closed"
    )
    lines = [
        "## 日报前 OpenAI 风险事件预审",
        "",
        f"- 官方来源抓取状态：{official_policy_fetch_report.status}",
        f"- 官方 payload 数：{official_policy_fetch_report.payload_count}",
        f"- 官方候选数：{official_policy_fetch_report.candidate_count}",
        f"- OpenAI 预审状态：{risk_event_prereview_report.status}",
        f"- LLM request profile：{llm_profile_id}",
        f"- OpenAI 模型：{model}",
        f"- reasoning.effort：{reasoning_effort}",
        f"- 请求读超时：{timeout_seconds:g} 秒",
        f"- HTTP client：{http_client}",
        f"- OpenAI 请求缓存目录：`{cache_dir}`",
        f"- OpenAI 请求缓存 TTL：{cache_ttl_hours:g} 小时",
        f"- OpenAI 请求缓存命中：HIT={risk_event_prereview_report.openai_cache_hit_count} / "
        f"MISS={risk_event_prereview_report.openai_cache_miss_count} / "
        f"EXPIRED={risk_event_prereview_report.openai_cache_expired_count} / "
        f"DISABLED={risk_event_prereview_report.openai_cache_disabled_count}",
        (
            "- request timestamp 可见性："
            f"as_of={as_of_text}；visibility_cutoff={visibility_cutoff_text}"
        ),
        f"- 本次候选上限：{max_candidates}",
        f"- LLM claim 数：{risk_event_prereview_report.row_count}",
        f"- 待人工复核队列记录数：{risk_event_prereview_report.record_count}",
        f"- L2/L3 候选数：{risk_event_prereview_report.high_level_candidate_count}",
        f"- active 候选数：{risk_event_prereview_report.active_candidate_count}",
        f"- LLM formal 自动写入：{'是' if llm_formal_enabled else '否'}",
    ]
    if llm_formal_report is not None:
        lines.extend(
            [
                f"- LLM formal 状态：{llm_formal_report.status}",
                f"- LLM formal occurrence 数：{llm_formal_report.occurrence_count}",
                f"- LLM formal active/watch：{llm_formal_report.active_occurrence_count}/"
                f"{llm_formal_report.watch_occurrence_count}",
                f"- LLM formal attestation：{'是' if llm_formal_report.attestation else '否'}",
                f"- LLM formal 报告：`{risk_event_llm_formal_report_output}`",
            ]
        )
    if llm_formal_enabled:
        mode_line = (
            "- 复核模式：LLM formal trusted by owner；不伪装成人工复核，"
            "不进入 `execution_policy.manual_review_gate_ids`，不会单独把执行动作改成 "
            "`wait_manual_review`。"
        )
        boundary_line = (
            "- 边界：LLM formal 是正式评估输入，但不是人工复核；"
            "LLM formal evidence 默认最高 B 级，可进入普通评分但不能单独触发 "
            "position gate 或 thesis 状态。"
        )
    else:
        mode_line = (
            "- 复核模式：backlog-only；未确认 OpenAI 候选不进入 "
            "`execution_policy.manual_review_gate_ids`，不会单独把执行动作改成 "
            "`wait_manual_review`。"
        )
        boundary_line = (
            "- 边界：预审输出只作为 `llm_extracted / pending_review` 线索，"
            "不会写入 occurrence、复核声明、评分、仓位闸门或 thesis 状态；"
            "无人复核时保留为未确认 backlog。"
        )
    lines.extend(
        [
            mode_line,
            f"- 官方来源抓取报告：`{official_policy_report_output}`",
            f"- OpenAI 预审报告：`{risk_event_openai_precheck_report_output}`",
            f"- 预审待复核队列：`{risk_event_prereview_queue_path}`",
            boundary_line,
        ]
    )
    return "\n".join(lines)


def _base_trace_config_paths() -> dict[str, Path]:
    return {
        "universe": DEFAULT_CONFIG_PATH,
        "portfolio": DEFAULT_PORTFOLIO_CONFIG_PATH,
        "data_quality": DEFAULT_DATA_QUALITY_CONFIG_PATH,
        "features": DEFAULT_FEATURE_CONFIG_PATH,
        "scoring_rules": DEFAULT_SCORING_RULES_CONFIG_PATH,
        "weight_profile": DEFAULT_WEIGHT_PROFILE_PATH,
        "calibration_overlay": DEFAULT_APPROVED_CALIBRATION_OVERLAY_PATH,
        "llm_request_profiles": DEFAULT_LLM_REQUEST_PROFILES_CONFIG_PATH,
        "watchlist": DEFAULT_WATCHLIST_CONFIG_PATH,
        "watchlist_lifecycle": DEFAULT_WATCHLIST_LIFECYCLE_PATH,
        "industry_chain": DEFAULT_INDUSTRY_CHAIN_CONFIG_PATH,
        "data_sources": DEFAULT_DATA_SOURCES_CONFIG_PATH,
    }


def _daily_trace_config_paths(
    *,
    sec_companies_path: Path,
    sec_metrics_path: Path,
    fundamental_feature_config_path: Path,
    risk_events_path: Path,
    execution_policy_path: Path,
    rule_cards_path: Path,
    feature_availability_path: Path,
) -> dict[str, Path]:
    return {
        **_base_trace_config_paths(),
        "market_regimes": DEFAULT_MARKET_REGIMES_CONFIG_PATH,
        "sec_companies": sec_companies_path,
        "fundamental_metrics": sec_metrics_path,
        "fundamental_features": fundamental_feature_config_path,
        "risk_events": risk_events_path,
        "execution_policy": execution_policy_path,
        "rule_cards": rule_cards_path,
        "feature_availability": feature_availability_path,
    }


def _backtest_trace_config_paths(
    *,
    regimes_path: Path,
    benchmark_policy_path: Path,
    sec_companies_path: Path,
    sec_metrics_path: Path,
    fundamental_feature_config_path: Path,
    risk_events_path: Path,
    watchlist_lifecycle_path: Path,
    rule_cards_path: Path,
    feature_availability_path: Path,
) -> dict[str, Path]:
    return {
        **_base_trace_config_paths(),
        "market_regimes": regimes_path,
        "benchmark_policy": benchmark_policy_path,
        "backtest_validation_policy": DEFAULT_BACKTEST_VALIDATION_POLICY_CONFIG_PATH,
        "feedback_sample_policy": DEFAULT_FEEDBACK_SAMPLE_POLICY_PATH,
        "sec_companies": sec_companies_path,
        "fundamental_metrics": sec_metrics_path,
        "fundamental_features": fundamental_feature_config_path,
        "risk_events": risk_events_path,
        "watchlist_lifecycle": watchlist_lifecycle_path,
        "rule_cards": rule_cards_path,
        "feature_availability": feature_availability_path,
    }


def _build_daily_review_summary(
    thesis_path: Path,
    thesis_review_report: ThesisReviewReport,
    risk_events_path: Path,
    risk_event_occurrences_path: Path,
    risk_events_validation_report: RiskEventsValidationReport,
    risk_event_occurrence_review_report: RiskEventOccurrenceReviewReport,
    valuation_path: Path,
    valuation_review_report: ValuationReviewReport,
    trades_path: Path,
    score_date: date,
    universe: UniverseConfig,
    industry_chain: IndustryChainConfig,
    watchlist: WatchlistConfig,
    prices: pd.DataFrame,
    data_quality_report: DataQualityReport,
    benchmark_tickers: tuple[str, ...],
) -> DailyReviewSummary:
    return DailyReviewSummary(
        thesis=_build_daily_thesis_status(
            input_path=thesis_path,
            review_report=thesis_review_report,
        ),
        risk_events=_build_daily_risk_events_status(
            rules_path=risk_events_path,
            occurrences_path=risk_event_occurrences_path,
            rules_validation_report=risk_events_validation_report,
            occurrence_review_report=risk_event_occurrence_review_report,
        ),
        valuation=_build_daily_valuation_status(
            input_path=valuation_path,
            review_report=valuation_review_report,
        ),
        trades=_build_daily_trade_review_status(
            input_path=trades_path,
            universe=universe,
            watchlist=watchlist,
            score_date=score_date,
            prices=prices,
            data_quality_report=data_quality_report,
            benchmark_tickers=benchmark_tickers,
        ),
    )


def _build_daily_thesis_review_report(
    input_path: Path,
    watchlist: WatchlistConfig,
    industry_chain: IndustryChainConfig,
    score_date: date,
) -> ThesisReviewReport:
    validation_report = validate_trade_thesis_store(
        store=load_trade_thesis_store(input_path),
        watchlist=watchlist,
        industry_chain=industry_chain,
        as_of=score_date,
    )
    return build_thesis_review_report(validation_report)


def _build_daily_thesis_status(
    input_path: Path,
    review_report: ThesisReviewReport,
) -> DailyManualReviewStatus:
    validation_report = review_report.validation_report
    watch_count = sum(item.health == "WATCH" for item in review_report.items)
    challenged_count = sum(item.health == "CHALLENGED" for item in review_report.items)
    invalidated_count = sum(item.health == "INVALIDATED" for item in review_report.items)
    summary = (
        f"Thesis {validation_report.thesis_count} 个，活跃 "
        f"{validation_report.active_count} 个；需关注 {watch_count} 个，"
        f"受挑战 {challenged_count} 个，已证伪 {invalidated_count} 个。"
    )
    status = "FAIL" if invalidated_count else review_report.status
    error_count = validation_report.error_count + (1 if invalidated_count else 0)
    return DailyManualReviewStatus(
        name="交易 thesis",
        status=status,
        summary=summary,
        error_count=error_count,
        warning_count=validation_report.warning_count,
        source_path=input_path,
    )


def _build_daily_risk_events_status(
    rules_path: Path,
    occurrences_path: Path,
    rules_validation_report: RiskEventsValidationReport,
    occurrence_review_report: RiskEventOccurrenceReviewReport,
) -> DailyManualReviewStatus:
    active_l2_l3_count = sum(
        1
        for rule in rules_validation_report.config.event_rules
        if rule.active and rule.level in {"L2", "L3"}
    )
    occurrence_validation = occurrence_review_report.validation_report
    error_count = rules_validation_report.error_count + occurrence_validation.error_count
    warning_count = rules_validation_report.warning_count + occurrence_validation.warning_count
    status = "PASS"
    if error_count:
        status = "FAIL"
    elif warning_count or occurrence_review_report.status == "PASS_WITH_WARNINGS":
        status = "PASS_WITH_WARNINGS"
    summary = (
        f"风险规则 {len(rules_validation_report.config.event_rules)} 条，活跃 "
        f"{rules_validation_report.active_rule_count} 条；活跃 L2/L3 规则 "
        f"{active_l2_l3_count} 条。发生记录 {occurrence_validation.occurrence_count} 条，"
        f"活跃/观察 {occurrence_validation.active_occurrence_count} 条，可评分 "
        f"{len(occurrence_review_report.score_eligible_active_items)} 条，可触发仓位闸门 "
        f"{len(occurrence_review_report.position_gate_eligible_active_items)} 条。"
    )
    return DailyManualReviewStatus(
        name="风险事件",
        status=status,
        summary=summary,
        error_count=error_count,
        warning_count=warning_count,
        source_path=occurrences_path if occurrences_path.exists() else rules_path,
    )


def _build_daily_valuation_status(
    input_path: Path,
    review_report: ValuationReviewReport,
) -> DailyManualReviewStatus:
    validation_report = review_report.validation_report
    overheated_count = sum(
        item.health in {"EXPENSIVE_OR_CROWDED", "EXTREME_OVERHEATED"}
        for item in review_report.items
    )
    summary = (
        f"估值快照 {validation_report.snapshot_count} 个，覆盖 "
        f"{validation_report.ticker_count} 个标的；偏贵或拥挤 "
        f"{overheated_count} 个。"
    )
    return DailyManualReviewStatus(
        name="估值与拥挤度",
        status=review_report.status,
        summary=summary,
        error_count=validation_report.error_count,
        warning_count=validation_report.warning_count,
        source_path=input_path,
    )


def _build_daily_trade_review_status(
    input_path: Path,
    universe: UniverseConfig,
    watchlist: WatchlistConfig,
    score_date: date,
    prices: pd.DataFrame,
    data_quality_report: DataQualityReport,
    benchmark_tickers: tuple[str, ...],
) -> DailyManualReviewStatus:
    try:
        validation_report = validate_trade_record_store(
            store=load_trade_record_store(input_path),
            universe=universe,
            watchlist=watchlist,
            as_of=score_date,
        )
        review_report = build_trade_review_report(
            validation_report=validation_report,
            prices=prices,
            data_quality_report=data_quality_report,
            benchmark_tickers=benchmark_tickers,
        )
    except ValueError as exc:
        return _daily_review_exception_status("交易复盘归因", input_path, exc)

    summary = (
        f"交易记录 {validation_report.trade_count} 笔，已关闭 "
        f"{validation_report.closed_count} 笔；生成基准归因 "
        f"{len(review_report.items)} 条。"
    )
    return DailyManualReviewStatus(
        name="交易复盘归因",
        status=review_report.status,
        summary=summary,
        error_count=validation_report.error_count,
        warning_count=validation_report.warning_count,
        source_path=input_path,
    )


def _daily_review_exception_status(
    name: str,
    source_path: Path,
    exc: Exception,
) -> DailyManualReviewStatus:
    return DailyManualReviewStatus(
        name=name,
        status="FAIL",
        summary=f"模块加载或复核失败：{exc}",
        error_count=1,
        warning_count=0,
        source_path=source_path,
    )


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


def _market_feature_source_checks(
    *,
    prices_frame: pd.DataFrame,
    rates_frame: pd.DataFrame,
    prices_path: Path,
    rates_path: Path,
    decision_time: date,
) -> tuple[FeatureAvailabilitySourceCheck, ...]:
    return (
        build_feature_source_check(
            source="prices_daily",
            frame=prices_frame,
            decision_time=decision_time,
            input_path=prices_path,
            event_time_columns=("date",),
            available_time_columns=(
                "available_time",
                "vendor_available_at",
                "ingested_at",
                "downloaded_at",
            ),
            fallback_policy="download_manifest.downloaded_at；若缺失则按下一交易日可用保守处理",
        ),
        build_feature_source_check(
            source="rates_daily",
            frame=rates_frame,
            decision_time=decision_time,
            input_path=rates_path,
            event_time_columns=("date",),
            available_time_columns=(
                "available_time",
                "vendor_available_at",
                "ingested_at",
                "downloaded_at",
            ),
            fallback_policy="download_manifest.downloaded_at；若缺失则按下一交易日可用保守处理",
        ),
    )


def _sec_pit_feature_source_check(
    *,
    feature_panel_path: Path,
    decision_time: date,
) -> FeatureAvailabilitySourceCheck:
    frame = pd.read_csv(feature_panel_path) if feature_panel_path.exists() else pd.DataFrame()
    return build_feature_source_check(
        source="sec_edgar_reconstructed_pit_features",
        frame=frame,
        decision_time=decision_time,
        input_path=feature_panel_path,
        event_time_columns=("period_end", "decision_date"),
        available_time_columns=("max_input_available_time_utc",),
        fallback_policy="",
        notes="TRADING-039 SEC reconstructed filing-time PIT feature panel.",
    )


def _path_from_snapshot_trace(snapshot: dict[str, object]) -> Path | None:
    trace = snapshot.get("trace")
    if not isinstance(trace, dict):
        return None
    path_text = trace.get("trace_bundle_path")
    return Path(str(path_text)) if path_text else None


def _trace_dataset_path(trace_bundle: dict[str, object], dataset_type: str) -> Path | None:
    dataset_refs = trace_bundle.get("dataset_refs")
    if not isinstance(dataset_refs, list):
        return None
    for dataset in dataset_refs:
        if not isinstance(dataset, dict):
            continue
        if dataset.get("dataset_type") == dataset_type and dataset.get("path"):
            return Path(str(dataset["path"]))
    return None


def _trace_quality_report_path(trace_bundle: dict[str, object]) -> Path | None:
    quality_refs = trace_bundle.get("quality_refs")
    if not isinstance(quality_refs, list):
        return None
    for quality in quality_refs:
        if not isinstance(quality, dict):
            continue
        if quality.get("report_path"):
            return Path(str(quality["report_path"]))
    return None


def _backtest_signal_dates(
    prices: pd.DataFrame,
    strategy_ticker: str,
    start: date,
    end: date,
) -> tuple[date, ...]:
    required_columns = {"date", "ticker", "adj_close"}
    missing = sorted(required_columns - set(prices.columns))
    if missing:
        raise typer.BadParameter(f"价格数据缺少必需字段：{', '.join(missing)}")

    frame = prices.loc[prices["ticker"].astype(str) == strategy_ticker].copy()
    if frame.empty:
        raise typer.BadParameter(f"回测缺少策略代理标的价格：{strategy_ticker}")

    frame["_date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame["_adj_close"] = pd.to_numeric(frame["adj_close"], errors="coerce")
    frame = frame.loc[frame["_date"].notna() & frame["_adj_close"].notna()].sort_values("_date")
    timestamps = list(frame["_date"])
    signal_dates: list[date] = []
    for index in range(len(timestamps) - 1):
        signal_date = pd.Timestamp(timestamps[index]).date()
        return_date = pd.Timestamp(timestamps[index + 1]).date()
        if signal_date >= start and return_date <= end:
            signal_dates.append(signal_date)
    if not signal_dates:
        raise typer.BadParameter("回测区间内没有可用的下一交易日收益")
    return tuple(signal_dates)


def _perturbed_scoring_rules(
    rules: ScoringRulesConfig,
    *,
    module_name: str,
    multiplier: float,
) -> ScoringRulesConfig:
    if module_name not in rules.weights:
        raise typer.BadParameter(f"未找到评分模块权重：{module_name}")
    if multiplier <= 0:
        raise typer.BadParameter("权重扰动倍数必须为正数。")
    weights = dict(rules.weights)
    weights[module_name] = weights[module_name] * multiplier
    return rules.model_copy(update={"weights": weights})


def _weight_perturbation_scenario_id(
    module_name: str,
    direction: str,
    perturbation_pct: float,
) -> str:
    percent_token = f"{perturbation_pct * 100:g}".replace(".", "p")
    module_token = "".join(char if char.isalnum() else "_" for char in module_name.lower()).strip(
        "_"
    )
    return f"weight_perturb_{module_token}_{direction}_{percent_token}pct"


def _backtest_oos_split_dates(
    signal_dates: tuple[date, ...],
    *,
    split_ratio: float,
    min_in_sample_signals: int = 5,
    min_out_sample_signals: int = 5,
) -> tuple[date, date] | None:
    if len(signal_dates) < min_in_sample_signals + min_out_sample_signals:
        return None
    split_index = int(len(signal_dates) * split_ratio)
    split_index = max(min_in_sample_signals, split_index)
    split_index = min(split_index, len(signal_dates) - min_out_sample_signals)
    if split_index <= 0 or split_index >= len(signal_dates):
        return None
    return signal_dates[split_index - 1], signal_dates[split_index]


def _parse_backtest_lag_days(value: str) -> tuple[int, ...]:
    lag_days: list[int] = []
    for raw_item in value.split(","):
        item = raw_item.strip()
        if not item:
            continue
        try:
            lag_day = int(item)
        except ValueError as exc:
            raise typer.BadParameter("滞后交易日列表必须为逗号分隔整数。") from exc
        if lag_day < 0:
            raise typer.BadParameter("滞后交易日不能为负数。")
        lag_days.append(lag_day)
    if not lag_days:
        raise typer.BadParameter("至少需要一个滞后交易日。")
    return tuple(sorted(dict.fromkeys(lag_days)))


def _latest_backtest_daily_path(output_dir: Path) -> Path | None:
    candidates = [
        path
        for path in output_dir.glob("backtest_daily_*.csv")
        if path.name.startswith("backtest_daily_")
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime)


def _backtest_required_input_signal_dates(
    prices: pd.DataFrame,
    strategy_ticker: str,
    start: date,
    end: date,
    lag_days: tuple[int, ...],
) -> tuple[date, ...]:
    frame = prices.loc[prices["ticker"].astype(str) == strategy_ticker].copy()
    frame["_date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame["_adj_close"] = pd.to_numeric(frame["adj_close"], errors="coerce")
    frame = frame.loc[frame["_date"].notna() & frame["_adj_close"].notna()].sort_values("_date")
    timestamps = list(frame["_date"])
    trading_signal_dates = [
        pd.Timestamp(timestamps[index]).date() for index in range(len(timestamps) - 1)
    ]
    base_signal_dates = set(_backtest_signal_dates(prices, strategy_ticker, start, end))
    index_by_signal_date = {
        signal_date: index for index, signal_date in enumerate(trading_signal_dates)
    }
    required_dates = set(base_signal_dates)
    for signal_date in base_signal_dates:
        index = index_by_signal_date[signal_date]
        for lag_day in lag_days:
            if index >= lag_day:
                required_dates.add(trading_signal_dates[index - lag_day])
    return tuple(sorted(required_dates))


def _build_backtest_sec_fundamental_feature_reports(
    signal_dates: tuple[date, ...],
    sec_companies_path: Path,
    sec_metrics_path: Path,
    fundamental_feature_config_path: Path,
    sec_companyfacts_dir: Path,
    tsm_ir_input_path: Path,
    validation_as_of: date,
    validation_report_output: Path,
) -> dict[date, SecFundamentalFeaturesReport]:
    sec_companies = load_sec_companies(sec_companies_path)
    sec_metrics = load_fundamental_metrics(sec_metrics_path)
    feature_config = load_fundamental_features(fundamental_feature_config_path)
    companyfacts_validation_report = validate_sec_companyfacts_cache(
        sec_companies,
        input_dir=sec_companyfacts_dir,
        as_of=validation_as_of,
    )
    write_sec_companyfacts_validation_report(
        companyfacts_validation_report,
        validation_report_output,
    )
    if not companyfacts_validation_report.passed:
        console.print("[red]SEC companyfacts 缓存校验失败，已停止回测。[/red]")
        console.print(f"SEC companyfacts 校验报告：{validation_report_output}")
        console.print(
            f"错误数：{companyfacts_validation_report.error_count}；"
            f"警告数：{companyfacts_validation_report.warning_count}"
        )
        raise typer.Exit(code=1)

    tsm_ir_enabled = any(
        company.active
        and company.ticker.upper() == "TSM"
        and "quarterly" in company.sec_metric_periods
        for company in sec_companies.companies
    )
    tsm_ir_rows: tuple[TsmIrQuarterlyMetricRow, ...] = (
        load_tsm_ir_quarterly_metric_rows_csv(tsm_ir_input_path)
        if tsm_ir_enabled and tsm_ir_input_path.exists()
        else tuple()
    )
    reports: dict[date, SecFundamentalFeaturesReport] = {}
    for signal_date in signal_dates:
        metrics_report = build_sec_fundamental_metrics_report(
            companies=sec_companies,
            metrics=sec_metrics,
            input_dir=sec_companyfacts_dir,
            as_of=signal_date,
            validation_report=companyfacts_validation_report,
        )
        if not metrics_report.passed:
            console.print("[red]SEC point-in-time 指标抽取失败，已停止回测。[/red]")
            console.print(f"失败日期：{signal_date.isoformat()}")
            console.print(
                f"错误数：{metrics_report.error_count}；" f"警告数：{metrics_report.warning_count}"
            )
            raise typer.Exit(code=1)
        metric_rows = metrics_report.rows
        if tsm_ir_rows:
            try:
                metric_rows = merge_tsm_ir_quarterly_rows_into_sec_metrics_as_of(
                    existing_rows=metric_rows,
                    tsm_rows=tsm_ir_rows,
                    tsm_company=sec_companies,
                    as_of=signal_date,
                )
            except ValueError as exc:
                console.print("[red]TSMC IR point-in-time 指标合并失败，已停止回测。[/red]")
                console.print(f"失败日期：{signal_date.isoformat()}")
                console.print(str(exc))
                raise typer.Exit(code=1) from exc
        metrics_source_path = (
            sec_companyfacts_dir / f"point_in_time_metrics_{signal_date.isoformat()}.csv"
        )
        metrics_validation_report = validate_sec_fundamental_metric_rows(
            companies=sec_companies,
            metrics=sec_metrics,
            rows=metric_rows,
            source_path=metrics_source_path,
            as_of=signal_date,
        )
        if not metrics_validation_report.passed:
            console.print("[red]SEC point-in-time 指标校验失败，已停止回测。[/red]")
            console.print(f"失败日期：{signal_date.isoformat()}")
            console.print(
                f"错误数：{metrics_validation_report.error_count}；"
                f"警告数：{metrics_validation_report.warning_count}"
            )
            raise typer.Exit(code=1)

        feature_report = build_sec_fundamental_features_report_from_metric_rows(
            companies=sec_companies,
            feature_config=feature_config,
            metric_rows=metric_rows,
            source_path=metrics_source_path,
            as_of=signal_date,
            validation_report=metrics_validation_report,
        )
        if not feature_report.passed:
            console.print("[red]SEC point-in-time 基本面特征构建失败，已停止回测。[/red]")
            console.print(f"失败日期：{signal_date.isoformat()}")
            console.print(
                f"错误数：{feature_report.error_count}；" f"警告数：{feature_report.warning_count}"
            )
            raise typer.Exit(code=1)
        reports[signal_date] = feature_report
    return reports


def _build_backtest_sec_pit_feature_reports(
    signal_dates: tuple[date, ...],
    sec_companies_path: Path,
    sec_pit_feature_panel_path: Path,
) -> dict[date, SecFundamentalFeaturesReport]:
    if not sec_pit_feature_panel_path.exists():
        console.print("[red]SEC PIT feature panel 不存在，已停止回测。[/red]")
        console.print(f"SEC PIT feature panel：{sec_pit_feature_panel_path}")
        raise typer.Exit(code=1)
    sec_companies = load_sec_companies(sec_companies_path)
    tickers = [company.ticker for company in sec_companies.companies if company.active]
    try:
        return sec_pit_feature_panel_to_feature_reports(
            sec_pit_feature_panel_path,
            signal_dates,
            tickers,
        )
    except ValueError as exc:
        console.print("[red]SEC PIT feature panel 可见时间校验失败，已停止回测。[/red]")
        console.print(f"SEC PIT feature panel：{sec_pit_feature_panel_path}")
        console.print(str(exc))
        raise typer.Exit(code=1) from exc


def _build_score_daily_sec_pit_feature_report(
    *,
    score_date: date,
    sec_companies: SecCompaniesConfig,
    sec_pit_feature_panel_path: Path,
) -> SecFundamentalFeaturesReport:
    if not sec_pit_feature_panel_path.exists():
        console.print("[red]SEC PIT feature panel 不存在，已停止每日评分。[/red]")
        console.print(f"SEC PIT feature panel：{sec_pit_feature_panel_path}")
        raise typer.Exit(code=1)
    tickers = [company.ticker for company in sec_companies.companies if company.active]
    try:
        reports = sec_pit_feature_panel_to_feature_reports(
            sec_pit_feature_panel_path,
            (score_date,),
            tickers,
        )
    except ValueError as exc:
        console.print("[red]SEC PIT feature panel 可见时间校验失败，已停止每日评分。[/red]")
        console.print(f"SEC PIT feature panel：{sec_pit_feature_panel_path}")
        console.print(str(exc))
        raise typer.Exit(code=1) from exc
    return reports[score_date]


def _build_backtest_valuation_review_reports(
    signal_dates: tuple[date, ...],
    valuation_path: Path,
    universe: UniverseConfig,
    watchlist: WatchlistConfig,
) -> dict[date, ValuationReviewReport]:
    store = load_valuation_snapshot_store(valuation_path)
    reports: dict[date, ValuationReviewReport] = {}
    for signal_date in signal_dates:
        report = build_historical_valuation_review_report(
            store=store,
            universe=universe,
            watchlist=watchlist,
            as_of=signal_date,
        )
        if not report.validation_report.passed:
            console.print("[red]point-in-time 估值快照校验失败，已停止回测。[/red]")
            console.print(f"失败日期：{signal_date.isoformat()}")
            console.print(
                f"错误数：{report.validation_report.error_count}；"
                f"警告数：{report.validation_report.warning_count}"
            )
            raise typer.Exit(code=1)
        reports[signal_date] = report
    return reports


def _build_backtest_risk_event_occurrence_review_reports(
    signal_dates: tuple[date, ...],
    risk_events_path: Path,
    risk_event_occurrences_path: Path,
    universe: UniverseConfig,
    industry_chain: IndustryChainConfig,
    watchlist: WatchlistConfig,
    validation_as_of: date,
) -> dict[date, RiskEventOccurrenceReviewReport]:
    risk_events_config = load_risk_events(risk_events_path)
    rules_validation_report = validate_risk_events_config(
        risk_events=risk_events_config,
        industry_chain=industry_chain,
        watchlist=watchlist,
        universe=universe,
        as_of=validation_as_of,
    )
    if not rules_validation_report.passed:
        console.print("[red]风险事件规则校验失败，已停止回测。[/red]")
        console.print(
            f"错误数：{rules_validation_report.error_count}；"
            f"警告数：{rules_validation_report.warning_count}"
        )
        raise typer.Exit(code=1)

    store = load_risk_event_occurrence_store(risk_event_occurrences_path)
    reports: dict[date, RiskEventOccurrenceReviewReport] = {}
    for signal_date in signal_dates:
        report = build_historical_risk_event_occurrence_review_report(
            store=store,
            risk_events=risk_events_config,
            as_of=signal_date,
        )
        if not report.validation_report.passed:
            console.print("[red]point-in-time 风险事件发生记录校验失败，已停止回测。[/red]")
            console.print(f"失败日期：{signal_date.isoformat()}")
            console.print(
                f"错误数：{report.validation_report.error_count}；"
                f"警告数：{report.validation_report.warning_count}"
            )
            raise typer.Exit(code=1)
        reports[signal_date] = report
    return reports


def _resolve_signal_snapshot_path(*, latest: bool, as_of: str | None) -> Path:
    root = default_signal_snapshot_root()
    if latest or as_of is None:
        latest_path = latest_signal_snapshot_path(root)
        if latest_path is None:
            raise typer.BadParameter(f"未找到 signal snapshot artifact：{root}")
        return latest_path
    return default_signal_snapshot_json_path(root, _parse_date(as_of))


def _resolve_signal_ablation_path(*, latest: bool, as_of: str | None) -> Path:
    root = PROJECT_ROOT / "artifacts" / "signal_ablation"
    if latest or as_of is None:
        latest_path = latest_signal_ablation_path(root)
        if latest_path is None:
            raise typer.BadParameter(f"未找到 signal ablation artifact：{root}")
        return latest_path
    return root / _parse_date(as_of).isoformat() / "signal_ablation_summary.json"


def _resolve_signal_calibration_path(*, latest: bool, as_of: str | None) -> Path:
    root = PROJECT_ROOT / "artifacts" / "signal_calibration"
    if latest or as_of is None:
        latest_path = latest_signal_calibration_path(root)
        if latest_path is None:
            raise typer.BadParameter(f"未找到 signal calibration artifact：{root}")
        return latest_path
    return root / _parse_date(as_of).isoformat() / "signal_calibration_summary.json"


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


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter("日期必须使用 YYYY-MM-DD 格式。") from exc


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


def _parse_datetime(value: str) -> datetime:
    normalized = value.strip()
    if not normalized:
        raise typer.BadParameter("时间不能为空。")
    if normalized.endswith("Z"):
        normalized = f"{normalized[:-1]}+00:00"
    try:
        if "T" not in normalized and " " not in normalized:
            parsed_date = date.fromisoformat(normalized)
            return datetime.combine(parsed_date, time.min, tzinfo=UTC)
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise typer.BadParameter("时间必须使用 ISO datetime 或 YYYY-MM-DD 格式。") from exc
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed


def _parse_csv_items(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def _parse_positive_int_csv(value: str, label: str) -> list[int]:
    items = _parse_csv_items(value)
    if not items:
        raise typer.BadParameter(f"{label}不能为空。")
    parsed: list[int] = []
    for item in items:
        try:
            integer = int(item)
        except ValueError as exc:
            raise typer.BadParameter(f"{label}必须是逗号分隔的正整数。") from exc
        if integer <= 0:
            raise typer.BadParameter(f"{label}必须是正整数。")
        parsed.append(integer)
    return parsed


if __name__ == "__main__":
    app()
