from __future__ import annotations

import json
import os
from dataclasses import replace
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Annotated

import pandas as pd
import typer
import yaml
from rich.console import Console
from rich.table import Table

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
    default_catalyst_calendar_report_path,
    load_catalyst_calendar,
    lookup_catalyst,
    render_catalyst_lookup,
    validate_catalyst_calendar,
    write_catalyst_calendar_report,
)
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
    DEFAULT_SCENARIO_LIBRARY_CONFIG_PATH,
    DEFAULT_SCORING_RULES_CONFIG_PATH,
    DEFAULT_SEC_COMPANIES_CONFIG_PATH,
    DEFAULT_WATCHLIST_CONFIG_PATH,
    PROJECT_ROOT,
    IndustryChainConfig,
    ScoringRulesConfig,
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
from ai_trading_system.data_sources import (
    build_data_source_health_report,
    default_data_source_health_report_path,
    default_data_sources_report_path,
    validate_data_sources_config,
    write_data_source_health_report,
    write_data_sources_validation_report,
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
from ai_trading_system.docs_freshness import (
    default_docs_freshness_paths,
    validate_docs_freshness,
    write_docs_freshness_report,
)
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
    lookup_execution_action,
    render_execution_action_lookup,
    render_execution_advisory_section,
    validate_execution_policy,
    write_execution_policy_report,
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
    DEFAULT_FMP_FORWARD_PIT_RAW_DIR,
    FmpForwardPitFetchReport,
    FmpForwardPitIssue,
    FmpForwardPitIssueSeverity,
    attach_fmp_forward_pit_raw_paths,
    attach_fmp_forward_pit_report_artifacts,
    build_fmp_forward_pit_failure_report,
    default_fmp_forward_pit_fetch_report_path,
    default_fmp_forward_pit_normalized_path,
    fetch_fmp_forward_pit_snapshots,
    sanitize_fmp_forward_pit_error_message,
    write_fmp_forward_pit_fetch_report,
    write_fmp_forward_pit_normalized_csv_from_payloads,
    write_fmp_forward_pit_raw_payloads,
)
from ai_trading_system.focus_stock_trends import (
    build_focus_stock_trend_report,
    render_focus_stock_trend_section,
)
from ai_trading_system.fundamentals.sec_companyfacts import (
    SecEdgarCompanyFactsProvider,
    download_sec_companyfacts,
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
from ai_trading_system.fundamentals.sec_filings import (
    DEFAULT_SEC_FILING_ARCHIVE_DIR,
    DEFAULT_SEC_SUBMISSIONS_DIR,
    SecEdgarFilingArchiveProvider,
    build_sec_accession_coverage_report,
    default_sec_accession_coverage_report_path,
    download_sec_filing_archive_indexes,
    download_sec_submissions,
    write_sec_accession_coverage_report,
)
from ai_trading_system.fundamentals.sec_metrics import (
    build_sec_fundamental_metrics_report,
    default_sec_fundamental_metrics_csv_path,
    default_sec_fundamental_metrics_report_path,
    default_sec_fundamental_metrics_validation_report_path,
    load_sec_fundamental_metric_rows_csv,
    validate_sec_fundamental_metric_rows,
    validate_sec_fundamental_metrics_csv,
    write_sec_fundamental_metric_rows_csv,
    write_sec_fundamental_metrics_csv,
    write_sec_fundamental_metrics_report,
    write_sec_fundamental_metrics_validation_report,
)
from ai_trading_system.fundamentals.sec_validation import (
    default_sec_companyfacts_validation_report_path,
    validate_sec_companyfacts_cache,
    write_sec_companyfacts_validation_report,
)
from ai_trading_system.fundamentals.tsm_ir import (
    TsmIrHttpProvider,
    TsmIrQuarterlyMetricRow,
    build_tsm_ir_quarterly_batch_import_report,
    build_tsm_ir_sec_metric_conversion_report,
    extract_tsm_ir_pdf_text,
    load_tsm_ir_quarterly_metric_rows_csv,
    merge_tsm_ir_quarterly_rows_into_sec_metrics,
    merge_tsm_ir_quarterly_rows_into_sec_metrics_as_of,
    parse_tsm_ir_management_report_text,
    select_tsm_ir_management_report_resource,
    select_tsm_ir_quarterly_metric_rows_as_of,
    write_tsm_ir_pdf_text_extraction_report,
    write_tsm_ir_quarterly_batch_import_report,
    write_tsm_ir_quarterly_batch_metrics_csv,
    write_tsm_ir_quarterly_metrics_csv,
    write_tsm_ir_quarterly_report,
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
from ai_trading_system.industry_chain import (
    default_industry_chain_report_path,
    validate_industry_chain_config,
    write_industry_chain_validation_report,
)
from ai_trading_system.industry_node_state import (
    build_industry_node_heat_report,
    render_industry_node_heat_section,
)
from ai_trading_system.llm_precheck import (
    DEFAULT_OPENAI_REQUEST_CACHE_DIR,
    default_llm_claim_precheck_report_path,
    load_llm_claim_precheck_input,
    run_openai_claim_precheck,
    write_llm_claim_precheck_report,
    write_llm_claim_prereview_queue,
)
from ai_trading_system.llm_request_profiles import (
    DEFAULT_LLM_REQUEST_PROFILES_CONFIG_PATH,
    LlmRequestProfile,
    load_llm_request_profiles,
)
from ai_trading_system.market_evidence import (
    default_market_evidence_report_path,
    import_market_evidence_csv,
    load_market_evidence_store,
    validate_market_evidence_store,
    write_market_evidence_import_report,
    write_market_evidence_validation_report,
    write_market_evidence_yaml,
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
    load_official_policy_candidates_csv,
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
from ai_trading_system.parameter_candidates import (
    DEFAULT_PARAMETER_CANDIDATE_LEDGER_PATH,
    build_parameter_candidate_ledger,
    default_parameter_candidate_report_path,
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
    discover_existing_pit_raw_snapshots,
    validate_pit_snapshot_manifest,
    write_pit_snapshot_manifest,
    write_pit_snapshot_validation_report,
)
from ai_trading_system.portfolio_exposure import (
    build_portfolio_exposure_report,
    default_portfolio_exposure_report_path,
    render_portfolio_exposure_section,
    write_portfolio_exposure_report,
)
from ai_trading_system.prediction_ledger import (
    DEFAULT_PREDICTION_LEDGER_PATH,
    DEFAULT_PREDICTION_OUTCOMES_PATH,
    append_prediction_records,
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
from ai_trading_system.price_source_diagnostics import (
    build_yahoo_price_diagnostic_report,
    default_yahoo_price_diagnostic_report_path,
    write_yahoo_price_diagnostic_report,
)
from ai_trading_system.report_traceability import (
    build_backtest_trace_bundle,
    build_daily_score_trace_bundle,
    default_report_trace_bundle_path,
    lookup_trace_record,
    render_trace_lookup,
    render_traceability_section,
    write_trace_bundle,
)
from ai_trading_system.reports.daily import render_recommendation_markdown
from ai_trading_system.risk_event_candidate_triage import (
    default_risk_event_candidate_triage_csv_path,
    default_risk_event_candidate_triage_input_path,
    default_risk_event_candidate_triage_report_path,
    load_triaged_candidate_ids,
    triage_official_policy_candidates,
    write_risk_event_candidate_triage_csv,
    write_risk_event_candidate_triage_report,
)
from ai_trading_system.risk_event_llm_formal import (
    build_llm_formal_assessment_report,
    default_llm_formal_assessment_report_path,
    write_llm_formal_assessment_outputs,
    write_llm_formal_assessment_report,
)
from ai_trading_system.risk_event_prereview import (
    default_risk_event_openai_prereview_report_path,
    default_risk_event_prereview_report_path,
    import_risk_event_prereview_csv,
    run_openai_risk_event_prereview,
    run_openai_risk_event_prereview_for_official_candidates,
    write_risk_event_prereview_import_report,
    write_risk_event_prereview_queue,
)
from ai_trading_system.risk_event_sources import (
    import_risk_event_occurrences_csv,
    write_risk_event_occurrence_import_report,
    write_risk_event_occurrences_yaml,
)
from ai_trading_system.risk_events import (
    RiskEventOccurrenceReviewReport,
    RiskEventsValidationReport,
    build_risk_event_occurrence_review_report,
    build_risk_event_review_attestation,
    default_risk_event_occurrence_report_path,
    default_risk_events_report_path,
    load_risk_event_occurrence_store,
    validate_risk_event_occurrence_store,
    validate_risk_events_config,
    write_risk_event_occurrence_review_report,
    write_risk_event_review_attestation,
    write_risk_events_validation_report,
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
from ai_trading_system.scenario_library import (
    default_scenario_library_report_path,
    load_scenario_library,
    lookup_scenario,
    render_scenario_lookup,
    validate_scenario_library,
    write_scenario_library_report,
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
from ai_trading_system.secret_hygiene import (
    default_secret_scan_report_path,
    scan_secrets,
    write_secret_scan_report,
)
from ai_trading_system.thesis import (
    ThesisReviewReport,
    build_thesis_review_report,
    default_thesis_review_report_path,
    default_thesis_validation_report_path,
    load_trade_thesis_store,
    validate_trade_thesis_store,
    write_thesis_review_report,
    write_thesis_validation_report,
)
from ai_trading_system.trade_review import (
    build_trade_review_report,
    default_trade_review_report_path,
    load_trade_record_store,
    validate_trade_record_store,
    write_trade_review_report,
)
from ai_trading_system.valuation import (
    ValuationReviewReport,
    build_valuation_review_report,
    default_valuation_review_report_path,
    default_valuation_validation_report_path,
    load_valuation_snapshot_store,
    validate_valuation_snapshot_store,
    write_valuation_review_report,
    write_valuation_validation_report,
)
from ai_trading_system.valuation_sources import (
    default_eodhd_earnings_trends_fetch_report_path,
    default_eodhd_earnings_trends_raw_dir,
    default_fmp_analyst_estimate_history_dir,
    default_fmp_analyst_history_validation_report_path,
    default_fmp_historical_valuation_fetch_report_path,
    default_fmp_historical_valuation_raw_dir,
    default_fmp_valuation_fetch_report_path,
    fetch_eodhd_earnings_trend_snapshots,
    fetch_fmp_historical_valuation_snapshots,
    fetch_fmp_valuation_snapshots,
    import_valuation_snapshots_from_csv,
    validate_fmp_analyst_estimate_history,
    write_eodhd_earnings_trends_fetch_report,
    write_eodhd_earnings_trends_raw_payload,
    write_fmp_analyst_estimate_history_snapshots,
    write_fmp_analyst_history_validation_report,
    write_fmp_historical_valuation_fetch_report,
    write_fmp_historical_valuation_raw_payloads,
    write_fmp_valuation_fetch_report,
    write_valuation_csv_import_report,
    write_valuation_snapshots_as_yaml,
)
from ai_trading_system.watchlist import (
    default_watchlist_report_path,
    validate_watchlist_config,
    write_watchlist_validation_report,
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
    DEFAULT_EFFECTIVE_WEIGHTS_PATH,
    DEFAULT_WEIGHT_PROFILE_PATH,
    apply_calibration_overlays,
    load_calibration_overlays,
    load_weight_profile,
    resolve_calibration_application,
    write_effective_weights,
)

app = typer.Typer(help="AI 产业链趋势分析和仓位管理工具。", no_args_is_help=True)
watchlist_app = typer.Typer(help="观察池和能力圈管理。", no_args_is_help=True)
industry_chain_app = typer.Typer(help="产业链节点和因果图管理。", no_args_is_help=True)
thesis_app = typer.Typer(help="交易 thesis 和假设验证管理。", no_args_is_help=True)
risk_events_app = typer.Typer(help="风险事件分级和动作规则管理。", no_args_is_help=True)
valuation_app = typer.Typer(help="估值、预期和拥挤度快照管理。", no_args_is_help=True)
data_sources_app = typer.Typer(help="数据源目录和审计规则管理。", no_args_is_help=True)
fundamentals_app = typer.Typer(help="基本面数据源下载和审计。", no_args_is_help=True)
trace_app = typer.Typer(help="报告 evidence bundle 反查。", no_args_is_help=True)
evidence_app = typer.Typer(help="新市场信息 evidence 账本。", no_args_is_help=True)
feedback_app = typer.Typer(help="决策结果观察、校准和因果链查询。", no_args_is_help=True)
scenarios_app = typer.Typer(help="AI 产业链情景压力测试库。", no_args_is_help=True)
catalysts_app = typer.Typer(help="未来催化剂日历和事件前复核。", no_args_is_help=True)
execution_app = typer.Typer(help="Advisory execution policy 和执行纪律。", no_args_is_help=True)
portfolio_app = typer.Typer(help="真实组合持仓和暴露解释。", no_args_is_help=True)
reports_app = typer.Typer(help="投资报告和周期复盘。", no_args_is_help=True)
ops_app = typer.Typer(help="运行监控和 pipeline health。", no_args_is_help=True)
security_app = typer.Typer(help="密钥卫生和供应商权限治理。", no_args_is_help=True)
llm_app = typer.Typer(help="LLM 结构化预审和待复核队列。", no_args_is_help=True)
pit_snapshots_app = typer.Typer(help="Forward-only PIT raw snapshot 归档。", no_args_is_help=True)
docs_app = typer.Typer(help="项目文档治理和新鲜度检查。", no_args_is_help=True)
app.add_typer(watchlist_app, name="watchlist")
app.add_typer(industry_chain_app, name="industry-chain")
app.add_typer(thesis_app, name="thesis")
app.add_typer(risk_events_app, name="risk-events")
app.add_typer(valuation_app, name="valuation")
app.add_typer(data_sources_app, name="data-sources")
app.add_typer(fundamentals_app, name="fundamentals")
app.add_typer(trace_app, name="trace")
app.add_typer(evidence_app, name="evidence")
app.add_typer(feedback_app, name="feedback")
app.add_typer(scenarios_app, name="scenarios")
app.add_typer(catalysts_app, name="catalysts")
app.add_typer(execution_app, name="execution")
app.add_typer(portfolio_app, name="portfolio")
app.add_typer(reports_app, name="reports")
app.add_typer(ops_app, name="ops")
app.add_typer(security_app, name="security")
app.add_typer(llm_app, name="llm")
app.add_typer(pit_snapshots_app, name="pit-snapshots")
app.add_typer(docs_app, name="docs")
console = Console()
DEFAULT_RISK_EVENT_OCCURRENCES_PATH = (
    PROJECT_ROOT / "data" / "external" / "risk_event_occurrences"
)
DEFAULT_RISK_EVENT_PREREVIEW_QUEUE_PATH = (
    PROJECT_ROOT / "data" / "processed" / "risk_event_prereview_queue.json"
)
DEFAULT_LLM_CLAIM_PREREVIEW_QUEUE_PATH = (
    PROJECT_ROOT / "data" / "processed" / "llm_claim_prereview_queue.json"
)
DEFAULT_OPENAI_REQUEST_CACHE_PATH = PROJECT_ROOT / DEFAULT_OPENAI_REQUEST_CACHE_DIR
DEFAULT_LLM_CLAIM_PREREVIEW_PROFILE = "llm_claim_prereview"
DEFAULT_RISK_EVENT_SINGLE_PREREVIEW_PROFILE = "risk_event_single_prereview"
DEFAULT_RISK_EVENT_TRIAGED_PREREVIEW_PROFILE = "risk_event_triaged_official_candidates"
DEFAULT_RISK_EVENT_DAILY_PREREVIEW_PROFILE = "risk_event_daily_official_precheck"
DEFAULT_MARKET_EVIDENCE_PATH = PROJECT_ROOT / "data" / "external" / "market_evidence"
DEFAULT_PORTFOLIO_POSITIONS_PATH = (
    PROJECT_ROOT / "data" / "external" / "portfolio_positions" / "current_positions.csv"
)
DEFAULT_FMP_ANALYST_ESTIMATE_HISTORY_DIR = default_fmp_analyst_estimate_history_dir(
    PROJECT_ROOT / "data" / "raw"
)


def _load_llm_request_profile(
    profiles_path: Path,
    profile_id: str,
) -> LlmRequestProfile:
    try:
        return load_llm_request_profiles(profiles_path).get_profile(profile_id)
    except (OSError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc


def _coalesce_profile_value(value, profile_value):
    return profile_value if value is None else value
DEFAULT_FMP_HISTORICAL_VALUATION_RAW_DIR = default_fmp_historical_valuation_raw_dir(
    PROJECT_ROOT / "data" / "raw"
)
DEFAULT_EODHD_EARNINGS_TRENDS_RAW_DIR = default_eodhd_earnings_trends_raw_dir(
    PROJECT_ROOT / "data" / "raw"
)


@docs_app.command("validate-freshness")
def validate_docs_freshness_command(
    paths: Annotated[
        list[Path] | None,
        typer.Option(
            "--path",
            help="要检查的新鲜度文档路径；不传则检查关键 docs 和 requirements。",
        ),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="可选 Markdown 文档新鲜度报告输出路径。"),
    ] = None,
) -> None:
    """检查关键项目文档的 `最后更新` 是否落后于内部状态记录。"""
    checked_paths = tuple(paths or default_docs_freshness_paths(PROJECT_ROOT))
    report = validate_docs_freshness(checked_paths)
    if output_path is not None:
        write_docs_freshness_report(report, output_path)

    style = "green" if report.passed else "red"
    console.print(f"[{style}]文档新鲜度：{report.status}[/{style}]")
    console.print(f"检查文档数：{len(report.records)}")
    console.print(f"问题数：{len(report.issues)}")
    if output_path is not None:
        console.print(f"报告：{output_path}")
    for issue in report.issues[:10]:
        console.print(f"{issue.path}: {issue.code}: {issue.message}")
    if report.issues:
        raise typer.Exit(code=1)


@pit_snapshots_app.command("validate")
def validate_pit_snapshots_command(
    input_path: Annotated[
        Path,
        typer.Option(help="PIT raw snapshot manifest CSV 路径。"),
    ] = DEFAULT_PIT_SNAPSHOT_MANIFEST_PATH,
    data_sources_path: Annotated[
        Path,
        typer.Option(help="数据源目录 YAML 路径，用于校验授权和 provider 信息。"),
    ] = DEFAULT_DATA_SOURCES_CONFIG_PATH,
    as_of: Annotated[
        str | None,
        typer.Option(help="校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown PIT 快照质量报告输出路径。"),
    ] = None,
) -> None:
    """校验 forward-only PIT raw snapshot manifest。"""
    validation_date = _parse_date(as_of) if as_of else date.today()
    report_path = output_path or default_pit_snapshot_validation_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        validation_date,
    )
    report = validate_pit_snapshot_manifest(
        input_path=input_path,
        as_of=validation_date,
        data_sources=load_data_sources(data_sources_path),
    )
    write_pit_snapshot_validation_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]PIT 快照归档状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    console.print(f"Manifest：{input_path}")
    console.print(f"快照数：{report.snapshot_count}；原始记录数：{report.row_count}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")
    if not report.passed:
        raise typer.Exit(code=1)


@pit_snapshots_app.command("build-manifest")
def build_pit_snapshot_manifest_command(
    output_path: Annotated[
        Path,
        typer.Option(help="生成的 PIT raw snapshot manifest CSV 路径。"),
    ] = DEFAULT_PIT_SNAPSHOT_MANIFEST_PATH,
    data_sources_path: Annotated[
        Path,
        typer.Option(help="数据源目录 YAML 路径，用于补充授权字段。"),
    ] = DEFAULT_DATA_SOURCES_CONFIG_PATH,
    fmp_analyst_history_dir: Annotated[
        Path,
        typer.Option(help="FMP analyst estimates 原始历史快照目录。"),
    ] = DEFAULT_FMP_ANALYST_ESTIMATE_HISTORY_DIR,
    fmp_historical_valuation_dir: Annotated[
        Path,
        typer.Option(help="FMP historical valuation 原始 payload 目录。"),
    ] = DEFAULT_FMP_HISTORICAL_VALUATION_RAW_DIR,
    eodhd_earnings_trends_dir: Annotated[
        Path,
        typer.Option(help="EODHD Earnings Trends 原始 payload 目录。"),
    ] = DEFAULT_EODHD_EARNINGS_TRENDS_RAW_DIR,
    fmp_forward_pit_dir: Annotated[
        Path,
        typer.Option(help="FMP forward-only PIT 原始 payload 目录。"),
    ] = DEFAULT_FMP_FORWARD_PIT_RAW_DIR,
    as_of: Annotated[
        str | None,
        typer.Option(help="校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    validation_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown PIT 快照质量报告输出路径。"),
    ] = None,
) -> None:
    """从现有 FMP/EODHD raw cache 生成通用 PIT raw snapshot manifest。"""
    manifest_date = _parse_date(as_of) if as_of else date.today()
    data_sources = load_data_sources(data_sources_path)
    records = discover_existing_pit_raw_snapshots(
        fmp_analyst_history_dir=fmp_analyst_history_dir,
        fmp_historical_valuation_dir=fmp_historical_valuation_dir,
        eodhd_earnings_trends_dir=eodhd_earnings_trends_dir,
        fmp_forward_pit_dir=fmp_forward_pit_dir,
        data_sources=data_sources,
    )
    manifest_path = write_pit_snapshot_manifest(records, output_path)
    report_path = validation_report_path or default_pit_snapshot_validation_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        manifest_date,
    )
    report = validate_pit_snapshot_manifest(
        input_path=manifest_path,
        as_of=manifest_date,
        data_sources=data_sources,
    )
    write_pit_snapshot_validation_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"生成 PIT manifest：{manifest_path}")
    console.print(f"[{status_style}]PIT 快照归档状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    console.print(f"快照数：{report.snapshot_count}；原始记录数：{report.row_count}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")
    if not report.passed:
        raise typer.Exit(code=1)


@pit_snapshots_app.command("fetch-fmp-forward")
def fetch_fmp_forward_pit_command(
    tickers: Annotated[
        str | None,
        typer.Option(help="逗号分隔 ticker；未提供时使用 universe 的 AI core_watchlist。"),
    ] = None,
    raw_output_dir: Annotated[
        Path,
        typer.Option(help="写入 FMP forward-only PIT 原始 JSON 的目录。"),
    ] = DEFAULT_FMP_FORWARD_PIT_RAW_DIR,
    normalized_output_path: Annotated[
        Path | None,
        typer.Option(help="写入 FMP forward-only PIT 标准化 CSV 的路径。"),
    ] = None,
    manifest_path: Annotated[
        Path,
        typer.Option(help="写入或刷新 PIT raw snapshot manifest CSV 的路径。"),
    ] = DEFAULT_PIT_SNAPSHOT_MANIFEST_PATH,
    as_of: Annotated[
        str | None,
        typer.Option(help="抓取评估日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown FMP forward PIT 抓取报告输出路径。"),
    ] = None,
    pit_validation_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown PIT 快照质量报告输出路径。"),
    ] = None,
    data_sources_path: Annotated[
        Path,
        typer.Option(help="数据源目录 YAML 路径，用于补充 PIT manifest 授权字段。"),
    ] = DEFAULT_DATA_SOURCES_CONFIG_PATH,
    api_key_env: Annotated[
        str,
        typer.Option(help="读取 FMP API key 的环境变量名。"),
    ] = "FMP_API_KEY",
    analyst_estimate_limit: Annotated[
        int,
        typer.Option(help="每个 ticker 拉取的 annual analyst estimate 记录数。"),
    ] = 10,
    earnings_calendar_lookback_days: Annotated[
        int,
        typer.Option(help="earnings-calendar 向前覆盖天数。"),
    ] = 7,
    earnings_calendar_forward_days: Annotated[
        int,
        typer.Option(help="earnings-calendar 向后覆盖天数。"),
    ] = 90,
    continue_on_failure: Annotated[
        bool,
        typer.Option(
            "--continue-on-failure",
            help=(
                "抓取、写入或 PIT 校验失败时写入失败报告并返回 0，"
                "用于每日调度继续执行后续自带质量门禁的步骤。"
            ),
        ),
    ] = False,
) -> None:
    """抓取 FMP forward-only PIT raw archive 和标准化 as-of 索引。"""
    fetch_date = _parse_date(as_of) if as_of else date.today()
    selected_tickers = (
        _parse_csv_items(tickers)
        if tickers
        else load_universe().ai_chain.get("core_watchlist", [])
    )
    fetch_report_output = output_path or default_fmp_forward_pit_fetch_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        fetch_date,
    )
    normalized_output = normalized_output_path or default_fmp_forward_pit_normalized_path(
        DEFAULT_FMP_FORWARD_PIT_NORMALIZED_DIR,
        fetch_date,
    )
    pit_report_output = pit_validation_report_path or default_pit_snapshot_validation_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        fetch_date,
    )

    api_key = os.getenv(api_key_env)
    if not api_key:
        fetch_report = build_fmp_forward_pit_failure_report(
            selected_tickers,
            fetch_date,
            code="fmp_forward_pit_api_key_missing",
            message=f"未找到环境变量 {api_key_env}，无法抓取 FMP PIT。",
            captured_at=fetch_date,
            analyst_estimate_limit=analyst_estimate_limit,
            earnings_calendar_lookback_days=earnings_calendar_lookback_days,
            earnings_calendar_forward_days=earnings_calendar_forward_days,
            include_normalized_rows=False,
        )
        _finish_fmp_forward_pit_failure(
            fetch_report,
            fetch_report_output,
            continue_on_failure=continue_on_failure,
            stage="credential",
        )
        return

    try:
        data_sources = load_data_sources(data_sources_path)
    except Exception as exc:
        fetch_report = build_fmp_forward_pit_failure_report(
            selected_tickers,
            fetch_date,
            code="fmp_forward_pit_data_sources_failed",
            message=(
                "FMP PIT 数据源目录加载失败："
                f"{sanitize_fmp_forward_pit_error_message(exc)}"
            ),
            captured_at=fetch_date,
            analyst_estimate_limit=analyst_estimate_limit,
            earnings_calendar_lookback_days=earnings_calendar_lookback_days,
            earnings_calendar_forward_days=earnings_calendar_forward_days,
        )
        _finish_fmp_forward_pit_failure(
            fetch_report,
            fetch_report_output,
            continue_on_failure=continue_on_failure,
            stage="config",
        )
        return

    try:
        fetch_report = fetch_fmp_forward_pit_snapshots(
            selected_tickers,
            api_key,
            fetch_date,
            captured_at=fetch_date,
            analyst_estimate_limit=analyst_estimate_limit,
            earnings_calendar_lookback_days=earnings_calendar_lookback_days,
            earnings_calendar_forward_days=earnings_calendar_forward_days,
        )
    except ValueError as exc:
        fetch_report = build_fmp_forward_pit_failure_report(
            selected_tickers,
            fetch_date,
            code="fmp_forward_pit_parameter_error",
            message=f"FMP PIT 参数错误：{sanitize_fmp_forward_pit_error_message(exc)}",
            captured_at=fetch_date,
            analyst_estimate_limit=analyst_estimate_limit,
            earnings_calendar_lookback_days=earnings_calendar_lookback_days,
            earnings_calendar_forward_days=earnings_calendar_forward_days,
        )
        _finish_fmp_forward_pit_failure(
            fetch_report,
            fetch_report_output,
            continue_on_failure=continue_on_failure,
            stage="parameter",
        )
        return
    except Exception as exc:
        fetch_report = build_fmp_forward_pit_failure_report(
            selected_tickers,
            fetch_date,
            code="fmp_forward_pit_unhandled_fetch_error",
            message=(
                "FMP PIT 抓取阶段发生未捕获异常："
                f"{sanitize_fmp_forward_pit_error_message(exc)}"
            ),
            captured_at=fetch_date,
            analyst_estimate_limit=analyst_estimate_limit,
            earnings_calendar_lookback_days=earnings_calendar_lookback_days,
            earnings_calendar_forward_days=earnings_calendar_forward_days,
        )
        _finish_fmp_forward_pit_failure(
            fetch_report,
            fetch_report_output,
            continue_on_failure=continue_on_failure,
            stage="fetch",
        )
        return

    if not fetch_report.passed:
        _finish_fmp_forward_pit_failure(
            fetch_report,
            fetch_report_output,
            continue_on_failure=continue_on_failure,
            stage="fetch",
        )
        return

    try:
        raw_paths = write_fmp_forward_pit_raw_payloads(fetch_report.raw_payloads, raw_output_dir)
        attached_payloads = attach_fmp_forward_pit_raw_paths(fetch_report.raw_payloads, raw_paths)
        fetch_report = attach_fmp_forward_pit_report_artifacts(
            fetch_report,
            raw_payloads=attached_payloads,
            normalized_rows=fetch_report.normalized_rows,
        )
        write_fmp_forward_pit_normalized_csv_from_payloads(
            attached_payloads,
            normalized_output,
        )
        write_fmp_forward_pit_fetch_report(fetch_report, fetch_report_output)

        manifest_records = discover_existing_pit_raw_snapshots(
            fmp_analyst_history_dir=DEFAULT_FMP_ANALYST_ESTIMATE_HISTORY_DIR,
            fmp_historical_valuation_dir=DEFAULT_FMP_HISTORICAL_VALUATION_RAW_DIR,
            eodhd_earnings_trends_dir=DEFAULT_EODHD_EARNINGS_TRENDS_RAW_DIR,
            fmp_forward_pit_dir=raw_output_dir,
            data_sources=data_sources,
        )
        manifest_output = write_pit_snapshot_manifest(manifest_records, manifest_path)
        pit_report = validate_pit_snapshot_manifest(
            input_path=manifest_output,
            as_of=fetch_date,
            data_sources=data_sources,
        )
        write_pit_snapshot_validation_report(pit_report, pit_report_output)
    except Exception as exc:
        fetch_report = _append_fmp_forward_pit_failure_issue(
            fetch_report,
            code="fmp_forward_pit_artifact_stage_failed",
            message=(
                "FMP PIT artifact 写入、manifest 刷新或校验阶段失败："
                f"{sanitize_fmp_forward_pit_error_message(exc)}"
            ),
        )
        _finish_fmp_forward_pit_failure(
            fetch_report,
            fetch_report_output,
            continue_on_failure=continue_on_failure,
            stage="artifact",
        )
        return

    status_style = (
        "green" if fetch_report.status == "PASS" else "yellow" if fetch_report.passed else "red"
    )
    console.print(f"[{status_style}]FMP PIT 抓取状态：{fetch_report.status}[/{status_style}]")
    console.print(f"抓取报告：{fetch_report_output}")
    console.print(f"写入 raw payload：{len(raw_paths)} 个文件 -> {raw_output_dir}")
    console.print(f"写入 normalized CSV：{normalized_output}")
    console.print(f"刷新 PIT manifest：{manifest_output}")
    console.print(f"PIT 快照质量报告：{pit_report_output}")
    console.print(
        f"原始记录：{fetch_report.row_count}；标准化行：{fetch_report.normalized_row_count}"
    )
    console.print(
        f"PIT manifest 状态：{pit_report.status}；"
        f"错误数：{pit_report.error_count}；警告数：{pit_report.warning_count}"
    )
    if not pit_report.passed:
        if continue_on_failure:
            console.print(
                "[yellow]PIT manifest 未通过；已保留报告并继续后续流程。"
                "失败快照不得作为可用 PIT 输入。[/yellow]"
            )
            return
        raise typer.Exit(code=1)


def _append_fmp_forward_pit_failure_issue(
    report: FmpForwardPitFetchReport,
    *,
    code: str,
    message: str,
) -> FmpForwardPitFetchReport:
    issue = FmpForwardPitIssue(
        severity=FmpForwardPitIssueSeverity.ERROR,
        code=code,
        message=message,
    )
    return replace(report, issues=(*report.issues, issue))


def _finish_fmp_forward_pit_failure(
    report: FmpForwardPitFetchReport,
    output_path: Path,
    *,
    continue_on_failure: bool,
    stage: str,
) -> None:
    try:
        write_fmp_forward_pit_fetch_report(report, output_path)
        report_written = True
    except Exception as exc:
        report_written = False
        console.print(
            "[red]FMP PIT 失败报告写入失败："
            f"{sanitize_fmp_forward_pit_error_message(exc)}[/red]"
        )

    console.print(f"[red]FMP PIT 抓取状态：{report.status}[/red]")
    console.print(f"失败阶段：{stage}")
    if report_written:
        console.print(f"抓取报告：{output_path}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")
    if continue_on_failure:
        console.print(
            "[yellow]已启用 --continue-on-failure：本步骤不会阻断后续流程；"
            "后续命令仍必须执行自己的质量门禁，失败 PIT 不得作为可用输入。[/yellow]"
        )
        return
    raise typer.Exit(code=1)


@trace_app.command("lookup")
def trace_lookup(
    bundle_path: Annotated[
        Path,
        typer.Option(help="evidence bundle JSON 路径。"),
    ],
    object_id: Annotated[
        str,
        typer.Option("--id", help="claim/evidence/dataset/quality/run id。"),
    ],
) -> None:
    """按 ID 反查报告 evidence bundle 中的上下文。"""
    try:
        record_type, record = lookup_trace_record(bundle_path, object_id)
    except FileNotFoundError as exc:
        raise typer.BadParameter(f"evidence bundle 不存在：{bundle_path}") from exc
    except KeyError as exc:
        raise typer.BadParameter(str(exc)) from exc
    console.print(render_trace_lookup(record_type, record))


@evidence_app.command("validate")
def validate_market_evidence(
    input_path: Annotated[
        Path,
        typer.Option(help="market_evidence YAML 文件或目录路径。"),
    ] = DEFAULT_MARKET_EVIDENCE_PATH,
    as_of: Annotated[
        str | None,
        typer.Option(help="校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown evidence 校验报告输出路径。"),
    ] = None,
) -> None:
    """校验新市场信息 evidence 账本。"""
    validation_date = _parse_date(as_of) if as_of else date.today()
    report_path = output_path or default_market_evidence_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        validation_date,
    )
    report = validate_market_evidence_store(
        load_market_evidence_store(input_path),
        as_of=validation_date,
    )
    write_market_evidence_validation_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]Market evidence 状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    console.print(f"证据数：{report.evidence_count}；待复核：{report.pending_review_count}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")
    if not report.passed:
        raise typer.Exit(code=1)


@evidence_app.command("import-csv")
def import_market_evidence_command(
    input_path: Annotated[
        Path,
        typer.Option(help="人工复核或 LLM 分类后的 market_evidence CSV 路径。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option(help="写入 market_evidence YAML 的目录。"),
    ] = DEFAULT_MARKET_EVIDENCE_PATH,
    report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 导入报告输出路径。"),
    ] = None,
) -> None:
    """从 CSV 导入 market_evidence YAML。"""
    import_report = import_market_evidence_csv(input_path)
    import_report_output = report_path or (
        PROJECT_ROOT / "outputs" / "reports" / f"market_evidence_import_{date.today()}.md"
    )
    write_market_evidence_import_report(import_report, import_report_output)
    if not import_report.passed:
        console.print("[red]Market evidence CSV 导入失败，未写入 YAML。[/red]")
        console.print(f"导入报告：{import_report_output}")
        raise typer.Exit(code=1)

    written_paths = write_market_evidence_yaml(import_report.evidence, output_dir)
    console.print("[green]Market evidence 已导入。[/green]")
    console.print(f"导入报告：{import_report_output}")
    console.print(f"写入证据数：{len(written_paths)}")
    console.print(f"输出目录：{output_dir}")


@llm_app.command("precheck-claims")
def precheck_llm_claims_command(
    input_path: Annotated[
        Path,
        typer.Option(help="LLM 预审输入 JSON/YAML，包含 source_id 或 source_permission envelope。"),
    ],
    queue_path: Annotated[
        Path,
        typer.Option(help="写入 LLM claim 待复核队列 JSON 的路径。"),
    ] = DEFAULT_LLM_CLAIM_PREREVIEW_QUEUE_PATH,
    data_sources_path: Annotated[
        Path,
        typer.Option(help="数据源目录路径，用于解析 provider LLM 权限。"),
    ] = DEFAULT_DATA_SOURCES_CONFIG_PATH,
    llm_request_profiles_path: Annotated[
        Path,
        typer.Option(help="LLM request profile 配置路径。"),
    ] = DEFAULT_LLM_REQUEST_PROFILES_CONFIG_PATH,
    llm_request_profile: Annotated[
        str,
        typer.Option(help="本次 LLM 请求使用的 profile_id。"),
    ] = DEFAULT_LLM_CLAIM_PREREVIEW_PROFILE,
    as_of: Annotated[
        str | None,
        typer.Option(help="报告日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown LLM 预审报告输出路径。"),
    ] = None,
    api_key_env: Annotated[
        str,
        typer.Option(help="读取 OpenAI API key 的环境变量名。"),
    ] = "OPENAI_API_KEY",
    model: Annotated[
        str | None,
        typer.Option(help="覆盖 profile 中的 OpenAI Responses API 模型。"),
    ] = None,
    reasoning_effort: Annotated[
        str | None,
        typer.Option(help="覆盖 profile 中的 OpenAI Responses API reasoning.effort。"),
    ] = None,
    timeout_seconds: Annotated[
        float | None,
        typer.Option(help="覆盖 profile 中的 OpenAI Responses API 请求读超时秒数。"),
    ] = None,
    openai_http_client: Annotated[
        str | None,
        typer.Option(
            help="覆盖 profile 中的 OpenAI Responses API HTTP 客户端：requests 或 urllib。"
        ),
    ] = None,
    openai_cache_dir: Annotated[
        Path,
        typer.Option(help="OpenAI 请求/响应本地缓存与审计归档目录。"),
    ] = DEFAULT_OPENAI_REQUEST_CACHE_PATH,
    openai_cache_ttl_hours: Annotated[
        float | None,
        typer.Option(help="覆盖 profile 中完全相同 OpenAI 请求的本地缓存复用时长，单位小时。"),
    ] = None,
) -> None:
    """调用 OpenAI 结构化输出生成 claim 待复核队列。"""
    profile = _load_llm_request_profile(llm_request_profiles_path, llm_request_profile)
    effective_model = _coalesce_profile_value(model, profile.model)
    effective_reasoning_effort = _coalesce_profile_value(
        reasoning_effort,
        profile.reasoning_effort,
    )
    effective_timeout_seconds = _coalesce_profile_value(
        timeout_seconds,
        profile.timeout_seconds,
    )
    effective_http_client = _coalesce_profile_value(openai_http_client, profile.http_client)
    effective_cache_ttl_hours = _coalesce_profile_value(
        openai_cache_ttl_hours,
        profile.cache_ttl_hours,
    )
    if effective_timeout_seconds <= 0:
        raise typer.BadParameter("OpenAI 请求超时秒数必须为正数。")
    if effective_cache_ttl_hours <= 0:
        raise typer.BadParameter("OpenAI 请求缓存 TTL 小时数必须为正数。")
    report_date = _parse_date(as_of) if as_of else date.today()
    report_path = output_path or default_llm_claim_precheck_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        report_date,
    )
    try:
        packet = load_llm_claim_precheck_input(input_path)
    except (OSError, ValueError) as exc:
        console.print(f"[red]LLM 预审输入无法读取或校验失败：{exc}[/red]")
        raise typer.Exit(code=1) from exc

    report = run_openai_claim_precheck(
        packet,
        api_key=os.getenv(api_key_env, ""),
        data_sources=load_data_sources(data_sources_path),
        input_path=input_path,
        model=effective_model,
        reasoning_effort=effective_reasoning_effort,
        endpoint=profile.endpoint,
        timeout_seconds=effective_timeout_seconds,
        http_client=effective_http_client,
        openai_cache_dir=openai_cache_dir,
        openai_cache_ttl_seconds=effective_cache_ttl_hours * 3600,
        max_retries=profile.max_retries,
    )
    write_llm_claim_precheck_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]LLM 证据预审状态：{report.status}[/{status_style}]")
    console.print(f"预审报告：{report_path}")
    console.print(f"预审记录：{report.record_count}；待复核 claim：{report.pending_review_count}")
    console.print(
        f"LLM request profile：{profile.profile_id}；"
        f"model={effective_model}；reasoning={effective_reasoning_effort}"
    )
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")
    if not report.passed:
        raise typer.Exit(code=1)

    written_path = write_llm_claim_prereview_queue(report, queue_path)
    console.print(f"LLM claim 待复核队列：{written_path}")
    console.print("LLM 输出保持 llm_extracted / pending_review，不进入评分或仓位闸门。")


@feedback_app.command("apply-calibration-overlay")
def apply_calibration_overlay_command(
    context_path: Annotated[
        Path,
        typer.Option(help="当前 decision context JSON 路径。"),
    ] = PROJECT_ROOT / "outputs" / "current_context.json",
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
        context = json.loads(context_path.read_text(encoding="utf-8"))
        if not isinstance(context, dict):
            raise ValueError("context JSON must contain an object")
        profile = load_weight_profile(weight_profile_path)
        overlays = load_calibration_overlays(overlays_path)
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
    ] = PROJECT_ROOT / "data" / "raw" / "prices_daily.csv",
    rates_path: Annotated[
        Path,
        typer.Option(help="标准化日线利率 CSV 路径，用于复用数据质量门禁。"),
    ] = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv",
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
    status_style = (
        "green" if len(result.available_rows) >= decision_diagnostic_floor else "yellow"
    )
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
    ] = PROJECT_ROOT / "data" / "raw" / "prices_daily.csv",
    rates_path: Annotated[
        Path,
        typer.Option(help="标准化日线利率 CSV 路径，用于复用数据质量门禁。"),
    ] = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv",
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
                "默认读取 feedback sample policy 的 prediction promotion floor。"
            )
        ),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown shadow maturity 报告输出路径。"),
    ] = None,
) -> None:
    """按 candidate/horizon 汇总 forward shadow outcome 样本成熟度。"""
    report_date = _parse_date(as_of) if as_of else date.today()
    rows = load_prediction_outcomes(prediction_outcomes_path)
    report = build_shadow_maturity_report(
        outcome_rows=rows,
        outcomes_path=prediction_outcomes_path,
        as_of=report_date,
        min_available_samples=min_available_samples,
    )
    report_output = output_path or default_shadow_maturity_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        report_date,
    )
    report_output = write_shadow_maturity_report(report, report_output)
    style = "green" if report.status == "PASS" else "yellow"
    console.print(f"[{style}]Shadow 样本成熟度：{report.status}[/{style}]")
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
        else "yellow"
        if report.validation_report.passed
        else "red"
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
        PROJECT_ROOT
        / "outputs"
        / "reports"
        / f"rule_lifecycle_retire_{action_date.isoformat()}.md"
    )
    lifecycle_report_path = write_rule_lifecycle_action_report(
        report,
        lifecycle_report_path,
    )
    style = (
        "green"
        if report.status == "PASS"
        else "yellow"
        if report.validation_report.passed
        else "red"
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
    benchmark_tickers = (
        tuple(_parse_csv_items(benchmarks)) if benchmarks is not None else None
    )
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
        f"Benchmark：{report.instrument_count}；"
        f"Custom AI basket：{report.custom_basket_count}"
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
    ] = PROJECT_ROOT / "data" / "external" / "market_evidence",
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
    ] = PROJECT_ROOT / "docs" / "task_register.md",
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
        "治理边界：本命令只读解释参数复测收益变化，"
        "不修改 production scoring 或仓位闸门。"
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
) -> None:
    """从 parameter replay 摘要生成 candidate-only 参数候选台账。"""
    review_date = _parse_date(as_of) if as_of else date.today()
    try:
        ledger = build_parameter_candidate_ledger(
            parameter_replay_summary_path=parameter_replay_summary_path,
            as_of=review_date,
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
    model = WeightedScoreModel(
        position_bands=_configured_position_band_rules(scoring_rules)
    )
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
    ] = PROJECT_ROOT / "data" / "raw",
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
    ] = PROJECT_ROOT / "data" / "raw" / "prices_daily.csv",
    rates_path: Annotated[
        Path,
        typer.Option(help="标准化 FRED 宏观序列 CSV 路径。"),
    ] = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv",
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
    ] = PROJECT_ROOT / "data" / "raw" / "prices_daily.csv",
    rates_path: Annotated[
        Path,
        typer.Option(help="标准化日线利率 CSV 路径。"),
    ] = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv",
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
    ] = PROJECT_ROOT / "data" / "raw" / "sec_companyfacts",
    tsm_ir_input_path: Annotated[
        Path,
        typer.Option(help="TSMC IR 季度指标 CSV，用于补齐 TSM point-in-time 回测基本面。"),
    ] = PROJECT_ROOT / "data" / "processed" / "tsm_ir_quarterly_metrics.csv",
    sec_companyfacts_validation_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown SEC companyfacts 缓存校验报告输出路径。"),
    ] = None,
    valuation_path: Annotated[
        Path,
        typer.Option(help="估值快照 YAML 文件或目录路径，用于 point-in-time 回测评分。"),
    ] = PROJECT_ROOT / "data" / "external" / "valuation_snapshots",
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
        promotion_report
        or promotion_report_path is not None
        or promotion_summary_path is not None
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
    backtest_robustness_output = (
        robustness_report_path
        or default_backtest_robustness_report_path(
            PROJECT_ROOT / "outputs" / "backtests",
            start_date,
            end_date,
        )
    )
    backtest_robustness_summary_output = (
        robustness_summary_path
        or (
            backtest_robustness_output.with_suffix(".json")
            if should_write_robustness_markdown
            else default_backtest_robustness_summary_path(
                PROJECT_ROOT / "outputs" / "backtests",
                start_date,
                end_date,
            )
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
    backtest_lag_sensitivity_summary_output = (
        lag_sensitivity_summary_path
        or (
            backtest_lag_sensitivity_output.with_suffix(".json")
            if should_write_lag_sensitivity_markdown
            else default_backtest_lag_sensitivity_summary_path(
                PROJECT_ROOT / "outputs" / "backtests",
                start_date,
                end_date,
            )
        )
    )
    backtest_feature_availability_output = (
        feature_availability_report_path
        or default_feature_availability_report_path(
            PROJECT_ROOT / "outputs" / "backtests",
            quality_date,
        )
    )
    backtest_promotion_output = (
        promotion_report_path
        or default_model_promotion_report_path(
            PROJECT_ROOT / "outputs" / "backtests",
            start_date,
            end_date,
        )
    )
    backtest_promotion_summary_output = (
        promotion_summary_path
        or (
            backtest_promotion_output.with_suffix(".json")
            if promotion_report or promotion_report_path is not None
            else default_model_promotion_summary_path(
                PROJECT_ROOT / "outputs" / "backtests",
                start_date,
                end_date,
            )
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
    backtest_feature_availability_report = build_feature_availability_report(
        input_path=feature_availability_path,
        as_of=quality_date,
        observed_sources=(
            "prices_daily",
            "rates_daily",
            "watchlist_lifecycle",
            "sec_fundamental_features",
            "valuation_snapshots",
            "risk_event_occurrences",
        ),
        required_sources=(
            "prices_daily",
            "rates_daily",
            "watchlist_lifecycle",
            "sec_fundamental_features",
            "valuation_snapshots",
            "risk_event_occurrences",
        ),
        source_checks=_market_feature_source_checks(
            prices_frame=prices_frame,
            rates_frame=rates_frame,
            prices_path=prices_path,
            rates_path=rates_path,
            decision_time=quality_date,
        ),
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
    sec_fundamental_feature_reports = _build_backtest_sec_fundamental_feature_reports(
        signal_dates=input_signal_dates,
        sec_companies_path=sec_companies_path,
        sec_metrics_path=sec_metrics_path,
        fundamental_feature_config_path=fundamental_feature_config_path,
        sec_companyfacts_dir=sec_companyfacts_dir,
        tsm_ir_input_path=tsm_ir_input_path,
        validation_as_of=quality_date,
        validation_report_output=sec_companyfacts_validation_output,
    )
    valuation_review_reports = _build_backtest_valuation_review_reports(
        signal_dates=input_signal_dates,
        valuation_path=valuation_path,
        universe=universe,
        watchlist=watchlist,
    )
    risk_event_occurrence_review_reports = (
        _build_backtest_risk_event_occurrence_review_reports(
            signal_dates=input_signal_dates,
            risk_events_path=risk_events_path,
            risk_event_occurrences_path=risk_event_occurrences_path,
            universe=universe,
            industry_chain=industry_chain,
            watchlist=watchlist,
            validation_as_of=quality_date,
        )
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
                scoring_rules
                if scenario_scoring_rules is None
                else scenario_scoring_rules
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
        sec_companyfacts_validation_report_path=sec_companyfacts_validation_output,
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
        sec_companyfacts_validation_report_path=sec_companyfacts_validation_output,
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
        sec_companyfacts_validation_report_path=sec_companyfacts_validation_output,
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
                target_annual_volatility=(
                    robustness_policy.volatility_target_annual_volatility
                ),
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
            policy_metadata=backtest_validation_policy.policy_metadata.model_dump(
                mode="json"
            ),
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
            lag_sensitivity_report_path=(
                lag_sensitivity_output or lag_sensitivity_summary_output
            ),
            prediction_outcomes_path=promotion_prediction_outcomes_path,
            rule_governance_status=rule_governance_report.status,
            promotion_policy=backtest_validation_policy.promotion,
            policy_metadata=backtest_validation_policy.policy_metadata.model_dump(
                mode="json"
            ),
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
            sec_companyfacts_validation_report_path=(
                sec_companyfacts_validation_output
            ),
            input_coverage_output_path=input_coverage_output,
            audit_report_path=audit_output,
            feature_availability_section=backtest_feature_availability_section,
            promotion_gate_section=render_model_promotion_report(
                promotion_report_data
            ).replace("# 模型晋级门槛报告", "## 模型晋级门槛", 1),
            traceability_section=render_traceability_section(
                backtest_trace_bundle,
                backtest_trace_output,
            ),
        )

    console.print(f"[yellow]回测状态：{result.status}[/yellow]")
    audit_style = "green" if audit_report.status == "PASS" else "yellow"
    console.print(f"[{audit_style}]输入审计状态：{audit_report.status}[/{audit_style}]")
    if result.market_regime is not None:
        console.print(
            f"市场阶段：{result.market_regime.name}（{result.market_regime.regime_id}）"
        )
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
    console.print(
        f"SEC 基本面切片：{result.fundamental_feature_report_count} 个 signal_date"
    )
    console.print(
        f"估值快照切片：{result.valuation_review_report_count} 个 signal_date"
    )
    console.print(
        "风险事件发生记录切片："
        f"{result.risk_event_occurrence_review_report_count} 个 signal_date"
    )
    console.print(
        f"SEC companyfacts 校验报告：{sec_companyfacts_validation_output}"
    )
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
        console.print(
            "[red]输入审计未达到 PASS，严格审计门禁已返回失败。[/red]"
        )
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
    selected_coverage_path = input_coverage_path or infer_input_coverage_path(
        selected_daily_path
    )
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
    ] = PROJECT_ROOT / "data" / "raw" / "prices_daily.csv",
    rates_path: Annotated[
        Path,
        typer.Option(help="标准化日线利率 CSV 路径，用于数据质量门禁。"),
    ] = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv",
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
    ] = PROJECT_ROOT / "data" / "external" / "valuation_snapshots",
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


@watchlist_app.command("list")
def list_watchlist(
    config_path: Annotated[
        Path,
        typer.Option(help="观察池配置文件路径。"),
    ] = DEFAULT_WATCHLIST_CONFIG_PATH,
    active_only: Annotated[
        bool,
        typer.Option("--active-only/--all", help="只显示活跃标的，或显示全部配置标的。"),
    ] = True,
) -> None:
    """列出观察池和能力圈配置。"""
    watchlist = load_watchlist(config_path)
    items = [item for item in watchlist.items if item.active or not active_only]

    table = Table(title="观察池与能力圈")
    table.add_column("Ticker")
    table.add_column("公司")
    table.add_column("类型")
    table.add_column("阶段")
    table.add_column("能力圈")
    table.add_column("风险")
    table.add_column("Thesis")
    table.add_column("产业链节点")

    for item in sorted(items, key=lambda value: value.ticker):
        table.add_row(
            item.ticker,
            item.company_name,
            item.instrument_type,
            _decision_stage_label(item.decision_stage),
            f"{item.competence_score:.0f}",
            _risk_level_label(item.default_risk_level),
            "需要" if item.thesis_required else "不需要",
            ", ".join(item.ai_chain_nodes),
        )

    console.print(table)


@watchlist_app.command("validate")
def validate_watchlist(
    config_path: Annotated[
        Path,
        typer.Option(help="观察池配置文件路径。"),
    ] = DEFAULT_WATCHLIST_CONFIG_PATH,
    as_of: Annotated[
        str | None,
        typer.Option(help="校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 观察池校验报告输出路径。"),
    ] = None,
) -> None:
    """校验观察池覆盖、能力圈和 thesis 约束。"""
    universe = load_universe()
    watchlist = load_watchlist(config_path)
    validation_date = _parse_date(as_of) if as_of else date.today()
    report_path = output_path or default_watchlist_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        validation_date,
    )

    report = validate_watchlist_config(
        watchlist=watchlist,
        universe=universe,
        as_of=validation_date,
    )
    write_watchlist_validation_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]观察池校验状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    console.print(f"活跃标的数：{report.active_count}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")

    if not report.passed:
        raise typer.Exit(code=1)


@watchlist_app.command("validate-lifecycle")
def validate_watchlist_lifecycle_command(
    input_path: Annotated[
        Path,
        typer.Option(help="观察池 lifecycle YAML 配置路径。"),
    ] = DEFAULT_WATCHLIST_LIFECYCLE_PATH,
    watchlist_path: Annotated[
        Path,
        typer.Option(help="当前观察池配置路径，用于一致性校验。"),
    ] = DEFAULT_WATCHLIST_CONFIG_PATH,
    as_of: Annotated[
        str | None,
        typer.Option(help="校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown lifecycle 校验报告输出路径。"),
    ] = None,
) -> None:
    """校验观察池 point-in-time 生命周期。"""
    universe = load_universe()
    watchlist = load_watchlist(watchlist_path)
    validation_date = _parse_date(as_of) if as_of else date.today()
    report_path = output_path or default_watchlist_lifecycle_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        validation_date,
    )
    report = validate_watchlist_lifecycle(
        lifecycle=load_watchlist_lifecycle(input_path),
        input_path=input_path,
        watchlist=watchlist,
        universe=universe,
        as_of=validation_date,
    )
    write_watchlist_lifecycle_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(
        f"[{status_style}]观察池 lifecycle 校验状态：{report.status}[/{status_style}]"
    )
    console.print(f"报告：{report_path}")
    console.print(f"生命周期记录数：{report.entry_count}；当前活跃：{report.active_entry_count}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")

    if not report.passed:
        raise typer.Exit(code=1)


@industry_chain_app.command("list")
def list_industry_chain(
    config_path: Annotated[
        Path,
        typer.Option(help="产业链配置文件路径。"),
    ] = DEFAULT_INDUSTRY_CHAIN_CONFIG_PATH,
) -> None:
    """列出产业链节点和因果关系。"""
    industry_chain = load_industry_chain(config_path)

    table = Table(title="产业链因果图")
    table.add_column("节点")
    table.add_column("名称")
    table.add_column("父节点")
    table.add_column("周期")
    table.add_column("现金流")
    table.add_column("情绪")
    table.add_column("相关标的")

    for node in sorted(industry_chain.nodes, key=lambda value: value.node_id):
        table.add_row(
            node.node_id,
            node.name,
            ", ".join(node.parent_node_ids) or "无",
            _horizon_label(node.impact_horizon),
            _relevance_label(node.cash_flow_relevance),
            _relevance_label(node.sentiment_relevance),
            ", ".join(node.related_tickers),
        )

    console.print(table)


@industry_chain_app.command("validate")
def validate_industry_chain(
    config_path: Annotated[
        Path,
        typer.Option(help="产业链配置文件路径。"),
    ] = DEFAULT_INDUSTRY_CHAIN_CONFIG_PATH,
    watchlist_path: Annotated[
        Path,
        typer.Option(help="观察池配置文件路径，用于校验节点引用。"),
    ] = DEFAULT_WATCHLIST_CONFIG_PATH,
    as_of: Annotated[
        str | None,
        typer.Option(help="校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 产业链校验报告输出路径。"),
    ] = None,
) -> None:
    """校验产业链节点、父子关系和观察池引用。"""
    industry_chain = load_industry_chain(config_path)
    watchlist = load_watchlist(watchlist_path)
    validation_date = _parse_date(as_of) if as_of else date.today()
    report_path = output_path or default_industry_chain_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        validation_date,
    )

    report = validate_industry_chain_config(
        industry_chain=industry_chain,
        watchlist=watchlist,
        as_of=validation_date,
    )
    write_industry_chain_validation_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]产业链校验状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    console.print(f"节点数：{len(report.nodes)}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")

    if not report.passed:
        raise typer.Exit(code=1)


@scenarios_app.command("validate")
def validate_scenarios_command(
    input_path: Annotated[
        Path,
        typer.Option(help="scenario library YAML 路径。"),
    ] = DEFAULT_SCENARIO_LIBRARY_CONFIG_PATH,
    industry_chain_path: Annotated[
        Path,
        typer.Option(help="产业链配置文件路径，用于校验 affected_nodes。"),
    ] = DEFAULT_INDUSTRY_CHAIN_CONFIG_PATH,
    watchlist_path: Annotated[
        Path,
        typer.Option(help="观察池配置文件路径，用于校验 affected_tickers。"),
    ] = DEFAULT_WATCHLIST_CONFIG_PATH,
    risk_events_path: Annotated[
        Path,
        typer.Option(help="风险事件配置路径，用于校验 linked_risk_event_ids。"),
    ] = DEFAULT_RISK_EVENTS_CONFIG_PATH,
    as_of: Annotated[
        str | None,
        typer.Option(help="校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 情景库校验报告输出路径。"),
    ] = None,
) -> None:
    """校验 AI 产业链情景压力测试库。"""
    validation_date = _parse_date(as_of) if as_of else date.today()
    report = validate_scenario_library(
        load_scenario_library(input_path),
        as_of=validation_date,
        industry_chain=load_industry_chain(industry_chain_path),
        watchlist=load_watchlist(watchlist_path),
        risk_events=load_risk_events(risk_events_path),
    )
    report_path = output_path or default_scenario_library_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        validation_date,
    )
    write_scenario_library_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]情景库状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    console.print(f"情景数：{report.scenario_count}；Active：{report.active_count}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")
    if not report.passed:
        raise typer.Exit(code=1)


@scenarios_app.command("lookup")
def lookup_scenario_command(
    scenario_id: Annotated[
        str,
        typer.Option("--id", help="scenario id。"),
    ],
    input_path: Annotated[
        Path,
        typer.Option(help="scenario library YAML 路径。"),
    ] = DEFAULT_SCENARIO_LIBRARY_CONFIG_PATH,
) -> None:
    """按 scenario_id 反查情景定义。"""
    try:
        scenario = lookup_scenario(input_path, scenario_id)
    except FileNotFoundError as exc:
        raise typer.BadParameter(f"scenario library 不存在：{input_path}") from exc
    except (KeyError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    console.print(render_scenario_lookup(scenario))


@catalysts_app.command("validate")
def validate_catalysts_command(
    input_path: Annotated[
        Path,
        typer.Option(help="catalyst calendar YAML 路径。"),
    ] = DEFAULT_CATALYST_CALENDAR_CONFIG_PATH,
    industry_chain_path: Annotated[
        Path,
        typer.Option(help="产业链配置文件路径，用于校验 related_nodes。"),
    ] = DEFAULT_INDUSTRY_CHAIN_CONFIG_PATH,
    watchlist_path: Annotated[
        Path,
        typer.Option(help="观察池配置文件路径，用于校验 related_tickers。"),
    ] = DEFAULT_WATCHLIST_CONFIG_PATH,
    risk_events_path: Annotated[
        Path,
        typer.Option(help="风险事件配置路径，用于校验 linked_risk_event_ids。"),
    ] = DEFAULT_RISK_EVENTS_CONFIG_PATH,
    as_of: Annotated[
        str | None,
        typer.Option(help="校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    windows: Annotated[
        str,
        typer.Option(help="逗号分隔的 upcoming catalyst 窗口，单位为自然日。"),
    ] = "5,20,60",
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 催化剂日历报告输出路径。"),
    ] = None,
) -> None:
    """校验未来催化剂日历并输出 5/20/60 天窗口。"""
    validation_date = _parse_date(as_of) if as_of else date.today()
    window_values = tuple(_parse_positive_int_csv(windows, "催化剂窗口"))
    report = validate_catalyst_calendar(
        load_catalyst_calendar(input_path),
        as_of=validation_date,
        industry_chain=load_industry_chain(industry_chain_path),
        watchlist=load_watchlist(watchlist_path),
        risk_events=load_risk_events(risk_events_path),
        windows=window_values,
    )
    report_path = output_path or default_catalyst_calendar_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        validation_date,
    )
    write_catalyst_calendar_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]催化剂日历状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    console.print(
        f"事件数：{report.event_count}；"
        f"未来 {max(report.windows)} 天：{report.upcoming_count}"
    )
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")
    if not report.passed:
        raise typer.Exit(code=1)


@catalysts_app.command("upcoming")
def upcoming_catalysts_command(
    input_path: Annotated[
        Path,
        typer.Option(help="catalyst calendar YAML 路径。"),
    ] = DEFAULT_CATALYST_CALENDAR_CONFIG_PATH,
    as_of: Annotated[
        str | None,
        typer.Option(help="评估日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    windows: Annotated[
        str,
        typer.Option(help="逗号分隔的 upcoming catalyst 窗口，单位为自然日。"),
    ] = "5,20,60",
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown upcoming catalyst 报告输出路径。"),
    ] = None,
) -> None:
    """输出 upcoming catalyst 分桶报告。"""
    validation_date = _parse_date(as_of) if as_of else date.today()
    window_values = tuple(_parse_positive_int_csv(windows, "催化剂窗口"))
    report = validate_catalyst_calendar(
        load_catalyst_calendar(input_path),
        as_of=validation_date,
        industry_chain=load_industry_chain(),
        watchlist=load_watchlist(),
        risk_events=load_risk_events(),
        windows=window_values,
    )
    report_path = output_path or default_catalyst_calendar_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        validation_date,
    )
    write_catalyst_calendar_report(report, report_path)
    console.print(f"Upcoming catalyst 报告：{report_path}")
    console.print(f"未来 {max(report.windows)} 天事件数：{report.upcoming_count}")
    if not report.passed:
        console.print("[red]催化剂日历校验失败，upcoming 报告仅供排查。[/red]")
        raise typer.Exit(code=1)


@catalysts_app.command("lookup")
def lookup_catalyst_command(
    catalyst_id: Annotated[
        str,
        typer.Option("--id", help="catalyst id。"),
    ],
    input_path: Annotated[
        Path,
        typer.Option(help="catalyst calendar YAML 路径。"),
    ] = DEFAULT_CATALYST_CALENDAR_CONFIG_PATH,
) -> None:
    """按 catalyst_id 反查催化剂事件。"""
    try:
        event = lookup_catalyst(input_path, catalyst_id)
    except FileNotFoundError as exc:
        raise typer.BadParameter(f"catalyst calendar 不存在：{input_path}") from exc
    except (KeyError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    console.print(render_catalyst_lookup(event))


@execution_app.command("validate")
def validate_execution_policy_command(
    input_path: Annotated[
        Path,
        typer.Option(help="execution policy YAML 路径。"),
    ] = DEFAULT_EXECUTION_POLICY_CONFIG_PATH,
    as_of: Annotated[
        str | None,
        typer.Option(help="校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown execution policy 校验报告输出路径。"),
    ] = None,
) -> None:
    """校验 advisory execution policy 和固定动作词表。"""
    validation_date = _parse_date(as_of) if as_of else date.today()
    report = validate_execution_policy(
        load_execution_policy(input_path),
        as_of=validation_date,
    )
    report_path = output_path or default_execution_policy_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        validation_date,
    )
    write_execution_policy_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]执行政策状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    console.print(f"动作数：{report.action_count}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")
    if not report.passed:
        raise typer.Exit(code=1)


@execution_app.command("lookup")
def lookup_execution_action_command(
    action_id: Annotated[
        str,
        typer.Option("--id", help="execution action id。"),
    ],
    input_path: Annotated[
        Path,
        typer.Option(help="execution policy YAML 路径。"),
    ] = DEFAULT_EXECUTION_POLICY_CONFIG_PATH,
) -> None:
    """按 execution action id 反查动作定义。"""
    try:
        action = lookup_execution_action(input_path, action_id)
    except FileNotFoundError as exc:
        raise typer.BadParameter(f"execution policy 不存在：{input_path}") from exc
    except (KeyError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    console.print(render_execution_action_lookup(action))


@portfolio_app.command("exposure")
def portfolio_exposure_command(
    input_path: Annotated[
        Path,
        typer.Option(help="真实持仓 CSV 路径。"),
    ] = DEFAULT_PORTFOLIO_POSITIONS_PATH,
    as_of: Annotated[
        str | None,
        typer.Option(help="评估日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 组合暴露报告输出路径。"),
    ] = None,
) -> None:
    """基于真实持仓文件生成只读组合暴露分解。"""
    evaluation_date = _parse_date(as_of) if as_of else date.today()
    report = build_portfolio_exposure_report(
        input_path=input_path,
        as_of=evaluation_date,
        industry_chain=load_industry_chain(),
        watchlist=load_watchlist(),
    )
    report_path = output_path or default_portfolio_exposure_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        evaluation_date,
    )
    write_portfolio_exposure_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]组合暴露状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    console.print(
        f"总市值：{report.total_market_value:.2f}；"
        f"AI 名义暴露：{report.ai_market_value:.2f}；"
        f"AI 占比：{report.ai_exposure_pct_total:.1%}"
    )
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")
    if not report.passed:
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


@ops_app.command("health")
def pipeline_health_command(
    as_of: Annotated[
        str | None,
        typer.Option(help="检查日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    prices_path: Annotated[
        Path,
        typer.Option(help="标准化日线价格 CSV 路径。"),
    ] = PROJECT_ROOT / "data" / "raw" / "prices_daily.csv",
    rates_path: Annotated[
        Path,
        typer.Option(help="标准化日线利率 CSV 路径。"),
    ] = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv",
    features_path: Annotated[
        Path,
        typer.Option(help="每日特征 CSV 路径。"),
    ] = PROJECT_ROOT / "data" / "processed" / "features_daily.csv",
    scores_path: Annotated[
        Path,
        typer.Option(help="每日评分 CSV 路径。"),
    ] = PROJECT_ROOT / "data" / "processed" / "scores_daily.csv",
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
    pipeline_alert_report_path = (
        alert_output_path
        or default_pipeline_health_alert_report_path(
            PROJECT_ROOT / "outputs" / "reports",
            health_date,
        )
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
            risk_event_openai_precheck_max_candidates=(
                risk_event_openai_precheck_max_candidates
            ),
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
        risk_event_openai_precheck_max_candidates=(
            risk_event_openai_precheck_max_candidates
        ),
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
    ] = PROJECT_ROOT / "outputs" / "runs",
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
    plan_date = (
        _parse_date(as_of) if as_of else resolve_daily_ops_default_as_of(run_generated_at)
    )
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
        risk_event_openai_precheck_max_candidates=(
            risk_event_openai_precheck_max_candidates
        ),
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
    style = (
        "green"
        if status == "PASS"
        else "yellow"
        if status == "PASS_WITH_SKIPS"
        else "red"
    )
    console.print(f"[{style}]每日运行执行：{status}[/{style}]")
    console.print(f"Run ID：{resolved_run_id}")
    console.print(f"Run bundle：{run_paths.run_root}")
    console.print(f"计划报告：{plan_report_path}")
    console.print(f"执行报告：{run_report_path}")
    if run_report.metadata is not None:
        console.print(f"Metadata JSON：{metadata_path}")
        console.print(f"Run manifest：{run_paths.manifest_path}")
    console.print(f"执行步骤数：{len(run_report.step_results)} / {len(plan.steps)}")
    if run_report.missing_env_vars:
        console.print(f"缺失环境变量：{', '.join(run_report.missing_env_vars)}")
    if run_report.failed_step is not None:
        failed = run_report.failed_step
        console.print(
            f"失败步骤：{failed.step_id}；return_code={failed.return_code}"
        )
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
        else "yellow"
        if status == "INCOMPLETE_REPLAY"
        else "red"
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
        console.print(
            f"失败步骤：{failed.step_id}；return_code={failed.return_code}"
        )
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


@security_app.command("scan-secrets")
def security_scan_secrets_command(
    as_of: Annotated[
        str | None,
        typer.Option(help="扫描日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    scan_paths: Annotated[
        str,
        typer.Option(
            help="逗号分隔的扫描入口；默认扫描 config、docs、outputs 和 download manifest。",
        ),
    ] = "config,docs,outputs,data/raw/download_manifest.csv",
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown secret hygiene 扫描报告输出路径。"),
    ] = None,
) -> None:
    """扫描可提交或报告文件中的疑似 secret literal。"""
    scan_date = _parse_date(as_of) if as_of else date.today()
    selected_paths = tuple(Path(item) for item in _parse_csv_items(scan_paths))
    report_path = output_path or default_secret_scan_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        scan_date,
    )
    report = scan_secrets(paths=selected_paths, as_of=scan_date)
    write_secret_scan_report(report, report_path)

    style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{style}]Secret hygiene：{report.status}[/{style}]")
    console.print(f"报告：{report_path}")
    console.print(f"扫描文件数：{report.scanned_file_count}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")
    if not report.passed:
        raise typer.Exit(code=1)


@thesis_app.command("list")
def list_theses(
    input_path: Annotated[
        Path,
        typer.Option(help="交易 thesis YAML 文件或目录路径。"),
    ] = PROJECT_ROOT / "data" / "external" / "trade_theses",
) -> None:
    """列出本地交易 thesis。"""
    store = load_trade_thesis_store(input_path)

    table = Table(title="交易 Thesis")
    table.add_column("Thesis")
    table.add_column("Ticker")
    table.add_column("方向")
    table.add_column("状态")
    table.add_column("创建日期")
    table.add_column("复核频率")
    table.add_column("文件")

    for loaded in sorted(store.loaded, key=lambda item: item.thesis.thesis_id):
        thesis = loaded.thesis
        table.add_row(
            thesis.thesis_id,
            thesis.ticker,
            _thesis_direction_label(thesis.direction),
            _thesis_status_label(thesis.status),
            thesis.created_at.isoformat(),
            _thesis_review_frequency_label(thesis.review_frequency),
            str(loaded.path),
        )

    console.print(table)
    if not store.loaded:
        console.print("未发现可读取的交易 thesis。")
    if store.load_errors:
        console.print(
            f"[red]存在 {len(store.load_errors)} 个加载错误，请运行 validate 查看。[/red]"
        )


@thesis_app.command("validate")
def validate_theses(
    input_path: Annotated[
        Path,
        typer.Option(help="交易 thesis YAML 文件或目录路径。"),
    ] = PROJECT_ROOT / "data" / "external" / "trade_theses",
    watchlist_path: Annotated[
        Path,
        typer.Option(help="观察池配置文件路径。"),
    ] = DEFAULT_WATCHLIST_CONFIG_PATH,
    industry_chain_path: Annotated[
        Path,
        typer.Option(help="产业链配置文件路径。"),
    ] = DEFAULT_INDUSTRY_CHAIN_CONFIG_PATH,
    as_of: Annotated[
        str | None,
        typer.Option(help="校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown thesis 校验报告输出路径。"),
    ] = None,
) -> None:
    """校验交易 thesis 的结构、引用和复核约束。"""
    validation_date = _parse_date(as_of) if as_of else date.today()
    report_path = output_path or default_thesis_validation_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        validation_date,
    )
    store = load_trade_thesis_store(input_path)
    report = validate_trade_thesis_store(
        store=store,
        watchlist=load_watchlist(watchlist_path),
        industry_chain=load_industry_chain(industry_chain_path),
        as_of=validation_date,
    )
    write_thesis_validation_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]交易 thesis 校验状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    console.print(f"Thesis 数量：{report.thesis_count}；活跃：{report.active_count}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")

    if not report.passed:
        raise typer.Exit(code=1)


@thesis_app.command("review")
def review_theses(
    input_path: Annotated[
        Path,
        typer.Option(help="交易 thesis YAML 文件或目录路径。"),
    ] = PROJECT_ROOT / "data" / "external" / "trade_theses",
    watchlist_path: Annotated[
        Path,
        typer.Option(help="观察池配置文件路径。"),
    ] = DEFAULT_WATCHLIST_CONFIG_PATH,
    industry_chain_path: Annotated[
        Path,
        typer.Option(help="产业链配置文件路径。"),
    ] = DEFAULT_INDUSTRY_CHAIN_CONFIG_PATH,
    as_of: Annotated[
        str | None,
        typer.Option(help="复核日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown thesis 复核报告输出路径。"),
    ] = None,
) -> None:
    """复核交易 thesis 的验证指标、证伪条件和风险事件状态。"""
    review_date = _parse_date(as_of) if as_of else date.today()
    report_path = output_path or default_thesis_review_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        review_date,
    )
    store = load_trade_thesis_store(input_path)
    validation_report = validate_trade_thesis_store(
        store=store,
        watchlist=load_watchlist(watchlist_path),
        industry_chain=load_industry_chain(industry_chain_path),
        as_of=review_date,
    )
    review_report = build_thesis_review_report(validation_report)
    write_thesis_review_report(review_report, report_path)

    status_style = "green" if review_report.status == "PASS" else (
        "yellow" if validation_report.passed else "red"
    )
    console.print(f"[{status_style}]交易 thesis 复核状态：{review_report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    console.print(
        f"Thesis 数量：{validation_report.thesis_count}；"
        f"活跃：{validation_report.active_count}"
    )
    console.print(
        f"校验错误数：{validation_report.error_count}；"
        f"校验警告数：{validation_report.warning_count}"
    )

    if not validation_report.passed:
        raise typer.Exit(code=1)


@risk_events_app.command("list")
def list_risk_events(
    config_path: Annotated[
        Path,
        typer.Option(help="风险事件配置文件路径。"),
    ] = DEFAULT_RISK_EVENTS_CONFIG_PATH,
    active_only: Annotated[
        bool,
        typer.Option("--active-only/--all", help="只显示活跃规则，或显示全部规则。"),
    ] = True,
) -> None:
    """列出风险事件分级规则。"""
    risk_events = load_risk_events(config_path)

    levels_table = Table(title="风险等级")
    levels_table.add_column("等级")
    levels_table.add_column("名称")
    levels_table.add_column("AI 仓位乘数")
    levels_table.add_column("人工复核")
    levels_table.add_column("默认动作")
    for level in sorted(risk_events.levels, key=lambda item: item.level):
        levels_table.add_row(
            level.level,
            level.name,
            f"{level.target_ai_exposure_multiplier:.0%}",
            "需要" if level.requires_manual_review else "不需要",
            level.default_action,
        )
    console.print(levels_table)

    rules_table = Table(title="风险事件规则")
    rules_table.add_column("事件")
    rules_table.add_column("等级")
    rules_table.add_column("活跃")
    rules_table.add_column("影响节点")
    rules_table.add_column("相关标的")
    for rule in sorted(risk_events.event_rules, key=lambda item: item.event_id):
        if active_only and not rule.active:
            continue
        rules_table.add_row(
            rule.event_id,
            rule.level,
            "是" if rule.active else "否",
            ", ".join(rule.affected_nodes),
            ", ".join(rule.related_tickers),
        )
    console.print(rules_table)


@risk_events_app.command("validate")
def validate_risk_events(
    config_path: Annotated[
        Path,
        typer.Option(help="风险事件配置文件路径。"),
    ] = DEFAULT_RISK_EVENTS_CONFIG_PATH,
    industry_chain_path: Annotated[
        Path,
        typer.Option(help="产业链配置文件路径。"),
    ] = DEFAULT_INDUSTRY_CHAIN_CONFIG_PATH,
    watchlist_path: Annotated[
        Path,
        typer.Option(help="观察池配置文件路径。"),
    ] = DEFAULT_WATCHLIST_CONFIG_PATH,
    as_of: Annotated[
        str | None,
        typer.Option(help="校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 风险事件校验报告输出路径。"),
    ] = None,
) -> None:
    """校验风险事件等级、产业链引用和动作规则。"""
    universe = load_universe()
    validation_date = _parse_date(as_of) if as_of else date.today()
    report_path = output_path or default_risk_events_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        validation_date,
    )
    report = validate_risk_events_config(
        risk_events=load_risk_events(config_path),
        industry_chain=load_industry_chain(industry_chain_path),
        watchlist=load_watchlist(watchlist_path),
        universe=universe,
        as_of=validation_date,
    )
    write_risk_events_validation_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]风险事件校验状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    console.print(f"风险事件规则数：{len(report.config.event_rules)}；活跃：{report.active_rule_count}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")

    if not report.passed:
        raise typer.Exit(code=1)


@risk_events_app.command("list-occurrences")
def list_risk_event_occurrences(
    input_path: Annotated[
        Path,
        typer.Option(help="风险事件发生记录 YAML 文件或目录路径。"),
    ] = DEFAULT_RISK_EVENT_OCCURRENCES_PATH,
) -> None:
    """列出本地风险事件发生记录。"""
    store = load_risk_event_occurrence_store(input_path)

    table = Table(title="风险事件发生记录")
    table.add_column("Occurrence", overflow="fold")
    table.add_column("事件", overflow="fold")
    table.add_column("状态")
    table.add_column("触发日期")
    table.add_column("最近确认")
    table.add_column("证据数")
    table.add_column("文件", overflow="fold")
    for loaded in sorted(store.loaded, key=lambda item: item.occurrence.occurrence_id):
        occurrence = loaded.occurrence
        table.add_row(
            occurrence.occurrence_id,
            occurrence.event_id,
            occurrence.status,
            occurrence.triggered_at.isoformat(),
            occurrence.last_confirmed_at.isoformat(),
            str(len(occurrence.evidence_sources)),
            str(loaded.path),
        )
    console.print(table)
    if not store.loaded:
        console.print("未发现可读取的风险事件发生记录。")
    if store.review_attestations:
        attestation_table = Table(title="风险事件复核声明")
        attestation_table.add_column("Attestation", overflow="fold")
        attestation_table.add_column("复核日期")
        attestation_table.add_column("覆盖窗口")
        attestation_table.add_column("复核人", overflow="fold")
        attestation_table.add_column("结论", overflow="fold")
        attestation_table.add_column("文件", overflow="fold")
        for loaded in sorted(
            store.review_attestations,
            key=lambda item: item.attestation.attestation_id,
        ):
            attestation = loaded.attestation
            attestation_table.add_row(
                attestation.attestation_id,
                attestation.review_date.isoformat(),
                (
                    f"{attestation.coverage_start.isoformat()} 至 "
                    f"{attestation.coverage_end.isoformat()}"
                ),
                attestation.reviewer,
                attestation.review_decision,
                str(loaded.path),
            )
        console.print(attestation_table)
    if store.load_errors:
        console.print(
            "[red]存在 "
            f"{len(store.load_errors)} 个加载错误，请运行 validate-occurrences 查看。[/red]"
        )


@risk_events_app.command("record-review-attestation")
def record_risk_event_review_attestation_command(
    output_dir: Annotated[
        Path,
        typer.Option(help="写入风险事件复核声明 YAML 的目录。"),
    ] = DEFAULT_RISK_EVENT_OCCURRENCES_PATH,
    config_path: Annotated[
        Path,
        typer.Option(help="风险事件规则配置文件路径，用于写入后校验。"),
    ] = DEFAULT_RISK_EVENTS_CONFIG_PATH,
    as_of: Annotated[
        str | None,
        typer.Option(help="复核日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    reviewer: Annotated[
        str,
        typer.Option(help="复核人或复核角色，必须是真实人工复核责任方。"),
    ] = "",
    rationale: Annotated[
        str,
        typer.Option(help="复核结论理由，说明为何确认没有未记录重大风险事件。"),
    ] = "",
    checked_sources: Annotated[
        str,
        typer.Option(
            help=(
                "逗号分隔的已检查来源范围，例如 official_sources,"
                "paid_vendor_queue,openai_prereview_queue。"
            )
        ),
    ] = "manual_daily_risk_review",
    review_scope: Annotated[
        str,
        typer.Option(help="逗号分隔的复核范围。"),
    ] = "policy_event_occurrences,geopolitical_event_occurrences,risk_event_prereview_queue",
    coverage_start: Annotated[
        str | None,
        typer.Option(help="复核覆盖窗口开始日期，格式为 YYYY-MM-DD，默认等于 as_of。"),
    ] = None,
    coverage_end: Annotated[
        str | None,
        typer.Option(help="复核覆盖窗口结束日期，格式为 YYYY-MM-DD，默认等于 as_of。"),
    ] = None,
    reviewed_at: Annotated[
        str | None,
        typer.Option(help="人工复核日期，格式为 YYYY-MM-DD，默认等于 as_of。"),
    ] = None,
    next_review_due: Annotated[
        str | None,
        typer.Option(help="下次复核日期，格式为 YYYY-MM-DD，默认 as_of 后 1 天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 风险事件发生记录校验报告输出路径。"),
    ] = None,
) -> None:
    """记录“已复核且未发现未记录重大风险事件”的人工声明。"""
    review_date = _parse_date(as_of) if as_of else date.today()
    if not reviewer.strip():
        raise typer.BadParameter("必须提供 --reviewer，不能由系统匿名生成复核声明。")
    if not rationale.strip():
        raise typer.BadParameter("必须提供 --rationale，说明人工复核结论依据。")
    checked_source_names = tuple(_parse_csv_items(checked_sources))
    if not checked_source_names:
        raise typer.BadParameter("至少需要一个 --checked-sources 来源范围。")
    scope_items = tuple(_parse_csv_items(review_scope))
    if not scope_items:
        raise typer.BadParameter("至少需要一个 --review-scope 复核范围。")

    attestation = build_risk_event_review_attestation(
        as_of=review_date,
        reviewer=reviewer.strip(),
        rationale=rationale.strip(),
        checked_source_names=checked_source_names,
        coverage_start=_parse_date(coverage_start) if coverage_start else review_date,
        coverage_end=_parse_date(coverage_end) if coverage_end else review_date,
        reviewed_at=_parse_date(reviewed_at) if reviewed_at else review_date,
        next_review_due=(
            _parse_date(next_review_due)
            if next_review_due
            else review_date + timedelta(days=1)
        ),
        review_scope=scope_items,
    )
    written_path = write_risk_event_review_attestation(attestation, output_dir)

    report_path = output_path or default_risk_event_occurrence_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        review_date,
    )
    validation_report = validate_risk_event_occurrence_store(
        store=load_risk_event_occurrence_store(output_dir),
        risk_events=load_risk_events(config_path),
        as_of=review_date,
    )
    review_report = build_risk_event_occurrence_review_report(validation_report)
    write_risk_event_occurrence_review_report(review_report, report_path)

    status_style = (
        "green"
        if review_report.status == "PASS"
        else "yellow"
        if validation_report.passed
        else "red"
    )
    console.print(f"风险事件复核声明：{written_path}")
    console.print(
        f"[{status_style}]风险事件发生记录状态：{review_report.status}[/{status_style}]"
    )
    console.print(f"校验报告：{report_path}")
    console.print(
        f"复核声明数：{validation_report.review_attestation_count}；"
        f"当前有效：{validation_report.current_review_attestation_count}"
    )
    if not validation_report.passed:
        raise typer.Exit(code=1)


@risk_events_app.command("import-occurrences-csv")
def import_risk_event_occurrences_csv_command(
    input_path: Annotated[
        Path,
        typer.Option(help="人工复核后的风险事件发生记录 CSV 输入路径。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option(help="写入风险事件发生记录 YAML 的目录。"),
    ] = DEFAULT_RISK_EVENT_OCCURRENCES_PATH,
    risk_events_path: Annotated[
        Path,
        typer.Option(help="风险事件配置路径，用于导入后校验 event_id。"),
    ] = DEFAULT_RISK_EVENTS_CONFIG_PATH,
    as_of: Annotated[
        str | None,
        typer.Option(help="导入和校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown CSV 导入报告输出路径。"),
    ] = None,
    validation_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 风险事件发生记录校验报告输出路径。"),
    ] = None,
) -> None:
    """导入人工复核后的风险事件发生记录 CSV，并写入可审计 YAML。"""
    import_date = _parse_date(as_of) if as_of else date.today()
    import_report_output = output_path or (
        PROJECT_ROOT / "outputs" / "reports" / f"risk_event_occurrence_import_{import_date}.md"
    )
    validation_output = validation_report_path or default_risk_event_occurrence_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        import_date,
    )
    import_report = import_risk_event_occurrences_csv(input_path)
    write_risk_event_occurrence_import_report(import_report, import_report_output)

    status_style = (
        "green" if import_report.status == "PASS" else "yellow" if import_report.passed else "red"
    )
    console.print(
        f"[{status_style}]风险事件发生记录 CSV 导入状态："
        f"{import_report.status}[/{status_style}]"
    )
    console.print(f"导入报告：{import_report_output}")
    console.print(
        f"CSV 行数：{import_report.row_count}；"
        f"发生记录：{import_report.occurrence_count}"
    )
    console.print(f"错误数：{import_report.error_count}；警告数：{import_report.warning_count}")
    if not import_report.passed:
        raise typer.Exit(code=1)

    written_paths = write_risk_event_occurrences_yaml(import_report, output_dir)
    validation_report = validate_risk_event_occurrence_store(
        store=load_risk_event_occurrence_store(output_dir),
        risk_events=load_risk_events(risk_events_path),
        as_of=import_date,
    )
    review_report = build_risk_event_occurrence_review_report(validation_report)
    write_risk_event_occurrence_review_report(review_report, validation_output)

    validation_style = (
        "green"
        if validation_report.status == "PASS"
        else "yellow"
        if validation_report.passed
        else "red"
    )
    console.print(f"写入 YAML：{len(written_paths)} 个文件 -> {output_dir}")
    console.print(
        f"[{validation_style}]风险事件发生记录校验状态："
        f"{validation_report.status}[/{validation_style}]"
    )
    console.print(f"校验报告：{validation_output}")
    if not validation_report.passed:
        raise typer.Exit(code=1)


@risk_events_app.command("fetch-official-sources")
def fetch_official_policy_sources_command(
    raw_dir: Annotated[
        Path,
        typer.Option(help="官方政策/地缘来源 raw payload 输出目录。"),
    ] = DEFAULT_OFFICIAL_POLICY_RAW_DIR,
    processed_dir: Annotated[
        Path,
        typer.Option(help="官方来源待人工复核候选 CSV 输出目录。"),
    ] = DEFAULT_OFFICIAL_POLICY_PROCESSED_DIR,
    as_of: Annotated[
        str | None,
        typer.Option(help="抓取评估日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    since: Annotated[
        str | None,
        typer.Option(help="抓取窗口起始日期，格式为 YYYY-MM-DD，默认 as_of 前 3 天。"),
    ] = None,
    source_ids: Annotated[
        str,
        typer.Option(help="可选：逗号分隔的 source_id 白名单；为空时抓取全部官方来源。"),
    ] = "",
    limit: Annotated[
        int,
        typer.Option(help="每个可分页来源最多请求的记录数。"),
    ] = 50,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 官方来源抓取报告输出路径。"),
    ] = None,
    download_manifest_path: Annotated[
        Path,
        typer.Option(help="统一 download_manifest.csv 路径。"),
    ] = PROJECT_ROOT / "data" / "raw" / "download_manifest.csv",
    congress_api_key_env: Annotated[
        str,
        typer.Option(help="读取 Congress.gov API key 的环境变量名。"),
    ] = "CONGRESS_API_KEY",
    govinfo_api_key_env: Annotated[
        str,
        typer.Option(help="读取 GovInfo API key 的环境变量名。"),
    ] = "GOVINFO_API_KEY",
) -> None:
    """抓取低成本官方政策/地缘来源，生成待人工复核候选。"""
    fetch_date = _parse_date(as_of) if as_of else date.today()
    since_date = _parse_date(since) if since else None
    selected_source_ids = _parse_csv_items(source_ids) if source_ids else None
    report_path = output_path or default_official_policy_fetch_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        fetch_date,
    )
    report = fetch_official_policy_sources(
        as_of=fetch_date,
        since=since_date,
        raw_dir=raw_dir,
        processed_dir=processed_dir,
        api_keys={
            "CONGRESS_API_KEY": os.getenv(congress_api_key_env, ""),
            "GOVINFO_API_KEY": os.getenv(govinfo_api_key_env, ""),
        },
        selected_source_ids=selected_source_ids,
        limit=limit,
        download_manifest_path=download_manifest_path,
    )
    write_official_policy_fetch_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]官方政策/地缘来源抓取状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    console.print(f"Raw payload：{report.payload_count}；待复核候选：{report.candidate_count}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")
    console.print("候选记录保持 pending_review，未写入评分或仓位闸门。")
    if not report.passed:
        raise typer.Exit(code=1)


@risk_events_app.command("triage-official-candidates")
def triage_official_policy_candidates_command(
    processed_dir: Annotated[
        Path,
        typer.Option(help="官方来源候选和 triage CSV 所在 processed 目录。"),
    ] = DEFAULT_OFFICIAL_POLICY_PROCESSED_DIR,
    input_path: Annotated[
        Path | None,
        typer.Option(help="官方来源待复核候选 CSV 输入路径；为空时按 as_of 推导。"),
    ] = None,
    as_of: Annotated[
        str | None,
        typer.Option(help="triage 评估日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    triage_output_path: Annotated[
        Path | None,
        typer.Option(help="AI 模块相关性 triage CSV 输出路径。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown triage 报告输出路径。"),
    ] = None,
) -> None:
    """按 AI 模块相关性分类官方政策/地缘候选，降低无明显联系项优先级。"""
    triage_date = _parse_date(as_of) if as_of else date.today()
    candidate_input_path = input_path or default_risk_event_candidate_triage_input_path(
        processed_dir,
        triage_date,
    )
    report = triage_official_policy_candidates(candidate_input_path, as_of=triage_date)
    report_path = output_path or default_risk_event_candidate_triage_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        report.as_of,
    )
    csv_path = triage_output_path or default_risk_event_candidate_triage_csv_path(
        processed_dir,
        report.as_of,
    )
    write_risk_event_candidate_triage_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(
        f"[{status_style}]官方候选 AI 模块 triage 状态："
        f"{report.status}[/{status_style}]"
    )
    console.print(f"报告：{report_path}")
    console.print(f"输入候选：{candidate_input_path}")
    console.print(
        "Bucket："
        f"must_review={report.bucket_counts.get('must_review', 0)}；"
        f"review_next={report.bucket_counts.get('review_next', 0)}；"
        f"sample_review={report.bucket_counts.get('sample_review', 0)}；"
        f"auto_low_relevance={report.bucket_counts.get('auto_low_relevance', 0)}；"
        f"duplicate_or_noise={report.bucket_counts.get('duplicate_or_noise', 0)}"
    )
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")
    if not report.passed:
        raise typer.Exit(code=1)

    write_risk_event_candidate_triage_csv(report, csv_path)
    console.print(f"Triage CSV：{csv_path}")
    console.print("Triage 结果保持 production_effect=none，未写入评分或仓位闸门。")


@risk_events_app.command("precheck-triaged-official-candidates")
def precheck_triaged_official_candidates_with_openai_command(
    processed_dir: Annotated[
        Path,
        typer.Option(help="官方来源候选、triage CSV 所在 processed 目录。"),
    ] = DEFAULT_OFFICIAL_POLICY_PROCESSED_DIR,
    candidate_input_path: Annotated[
        Path | None,
        typer.Option(help="官方来源待复核候选 CSV 输入路径；为空时按 as_of 推导。"),
    ] = None,
    triage_input_path: Annotated[
        Path | None,
        typer.Option(help="官方候选 AI 模块 triage CSV 输入路径；为空时按 as_of 推导。"),
    ] = None,
    triage_buckets: Annotated[
        str,
        typer.Option(help="逗号分隔的 triage bucket 白名单。"),
    ] = "must_review,review_next",
    max_candidates: Annotated[
        int | None,
        typer.Option(help="覆盖 profile 中本次最多送入 OpenAI 预审的高优先级候选数。"),
    ] = None,
    queue_path: Annotated[
        Path,
        typer.Option(help="写入风险事件预审待复核队列 JSON 的路径。"),
    ] = DEFAULT_RISK_EVENT_PREREVIEW_QUEUE_PATH,
    data_sources_path: Annotated[
        Path,
        typer.Option(help="数据源目录路径，用于解析 provider LLM 权限。"),
    ] = DEFAULT_DATA_SOURCES_CONFIG_PATH,
    risk_events_path: Annotated[
        Path,
        typer.Option(help="风险事件配置路径，用于检查 matched_risk_ids。"),
    ] = DEFAULT_RISK_EVENTS_CONFIG_PATH,
    llm_request_profiles_path: Annotated[
        Path,
        typer.Option(help="LLM request profile 配置路径。"),
    ] = DEFAULT_LLM_REQUEST_PROFILES_CONFIG_PATH,
    llm_request_profile: Annotated[
        str,
        typer.Option(help="本次 LLM 请求使用的 profile_id。"),
    ] = DEFAULT_RISK_EVENT_TRIAGED_PREREVIEW_PROFILE,
    as_of: Annotated[
        str | None,
        typer.Option(help="预审和校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown triage OpenAI 预审报告输出路径。"),
    ] = None,
    api_key_env: Annotated[
        str,
        typer.Option(help="读取 OpenAI API key 的环境变量名。"),
    ] = "OPENAI_API_KEY",
    model: Annotated[
        str | None,
        typer.Option(help="覆盖 profile 中的 OpenAI Responses API 模型。"),
    ] = None,
    reasoning_effort: Annotated[
        str | None,
        typer.Option(help="覆盖 profile 中的 OpenAI Responses API reasoning.effort。"),
    ] = None,
    timeout_seconds: Annotated[
        float | None,
        typer.Option(help="覆盖 profile 中的 OpenAI Responses API 请求读超时秒数。"),
    ] = None,
    openai_http_client: Annotated[
        str | None,
        typer.Option(
            help="覆盖 profile 中的 OpenAI Responses API HTTP 客户端：requests 或 urllib。"
        ),
    ] = None,
    openai_cache_dir: Annotated[
        Path,
        typer.Option(help="OpenAI 请求/响应本地缓存与审计归档目录。"),
    ] = DEFAULT_OPENAI_REQUEST_CACHE_PATH,
    openai_cache_ttl_hours: Annotated[
        float | None,
        typer.Option(help="覆盖 profile 中完全相同 OpenAI 请求的本地缓存复用时长，单位小时。"),
    ] = None,
) -> None:
    """只对 triage 高优先级官方候选调用 OpenAI，输出风险等级预审建议。"""
    profile = _load_llm_request_profile(llm_request_profiles_path, llm_request_profile)
    effective_max_candidates = _coalesce_profile_value(
        max_candidates,
        profile.max_candidates,
    )
    effective_model = _coalesce_profile_value(model, profile.model)
    effective_reasoning_effort = _coalesce_profile_value(
        reasoning_effort,
        profile.reasoning_effort,
    )
    effective_timeout_seconds = _coalesce_profile_value(
        timeout_seconds,
        profile.timeout_seconds,
    )
    effective_http_client = _coalesce_profile_value(openai_http_client, profile.http_client)
    effective_cache_ttl_hours = _coalesce_profile_value(
        openai_cache_ttl_hours,
        profile.cache_ttl_hours,
    )
    if effective_max_candidates is None:
        raise typer.BadParameter("LLM request profile 必须设置 max_candidates，或显式传入。")
    if effective_max_candidates < 0:
        raise typer.BadParameter("OpenAI 预审候选上限不能为负数。")
    if effective_timeout_seconds <= 0:
        raise typer.BadParameter("OpenAI 请求超时秒数必须为正数。")
    if effective_cache_ttl_hours <= 0:
        raise typer.BadParameter("OpenAI 请求缓存 TTL 小时数必须为正数。")
    api_key = os.getenv(api_key_env, "")
    if not api_key:
        console.print("[red]缺少 OpenAI API key，已停止高优先级候选风险等级预审。[/red]")
        console.print(f"需要环境变量：{api_key_env}")
        raise typer.Exit(code=1)

    precheck_date = _parse_date(as_of) if as_of else date.today()
    official_candidates_path = candidate_input_path or default_official_policy_candidates_path(
        processed_dir,
        precheck_date,
    )
    triage_path = triage_input_path or default_risk_event_candidate_triage_csv_path(
        processed_dir,
        precheck_date,
    )
    selected_buckets = tuple(_parse_csv_items(triage_buckets))
    report_path = output_path or (
        PROJECT_ROOT
        / "outputs"
        / "reports"
        / f"risk_event_prereview_triaged_openai_{precheck_date.isoformat()}.md"
    )

    try:
        candidates = load_official_policy_candidates_csv(official_candidates_path)
        selected_ids = set(
            load_triaged_candidate_ids(triage_path, buckets=selected_buckets)
        )
    except (OSError, ValueError) as exc:
        console.print(f"[red]高优先级官方候选输入无法读取或校验失败：{exc}[/red]")
        raise typer.Exit(code=1) from exc

    candidates_by_id = {candidate.candidate_id: candidate for candidate in candidates}
    selected_candidates = tuple(
        candidates_by_id[candidate_id]
        for candidate_id in selected_ids
        if candidate_id in candidates_by_id
    )
    missing_ids = sorted(selected_ids - set(candidates_by_id))
    if missing_ids:
        console.print(
            "[yellow]triage CSV 中有候选未在官方候选 CSV 找到："
            f"{len(missing_ids)} 条；已跳过。[/yellow]"
        )

    report = run_openai_risk_event_prereview_for_official_candidates(
        selected_candidates,
        api_key=api_key,
        data_sources=load_data_sources(data_sources_path),
        risk_events=load_risk_events(risk_events_path),
        input_path=triage_path,
        as_of=precheck_date,
        model=effective_model,
        reasoning_effort=effective_reasoning_effort,
        endpoint=profile.endpoint,
        timeout_seconds=effective_timeout_seconds,
        http_client=effective_http_client,
        openai_cache_dir=openai_cache_dir,
        openai_cache_ttl_seconds=effective_cache_ttl_hours * 3600,
        max_retries=profile.max_retries,
        max_candidates=effective_max_candidates,
    )
    write_risk_event_prereview_import_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(
        f"[{status_style}]高优先级官方候选 OpenAI 预审状态："
        f"{report.status}[/{status_style}]"
    )
    console.print(f"报告：{report_path}")
    console.print(f"官方候选 CSV：{official_candidates_path}")
    console.print(f"Triage CSV：{triage_path}")
    console.print(f"Triage buckets：{', '.join(selected_buckets)}")
    console.print(
        f"LLM request profile：{profile.profile_id}；"
        f"model={effective_model}；reasoning={effective_reasoning_effort}"
    )
    console.print(
        f"送入 OpenAI 候选：{len(selected_candidates)}；"
        f"待复核队列：{report.record_count}；"
        f"L2/L3 候选：{report.high_level_candidate_count}"
    )
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")
    if not report.passed:
        raise typer.Exit(code=1)

    written_path = write_risk_event_prereview_queue(report, queue_path)
    console.print(f"预审待复核队列：{written_path}")
    console.print("LLM 风险等级仅作为 pending_review 建议，未写入正式风险事件或仓位闸门。")


@risk_events_app.command("apply-llm-formal-assessment")
def apply_llm_formal_assessment_command(
    queue_path: Annotated[
        Path,
        typer.Option(help="风险事件 OpenAI 预审队列 JSON 输入路径。"),
    ] = DEFAULT_RISK_EVENT_PREREVIEW_QUEUE_PATH,
    output_dir: Annotated[
        Path,
        typer.Option(help="写入正式风险事件 occurrence YAML 的目录。"),
    ] = DEFAULT_RISK_EVENT_OCCURRENCES_PATH,
    risk_events_path: Annotated[
        Path,
        typer.Option(help="风险事件配置路径，用于检查 matched_risk_ids。"),
    ] = DEFAULT_RISK_EVENTS_CONFIG_PATH,
    as_of: Annotated[
        str | None,
        typer.Option(help="正式评估日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown LLM 正式评估导入报告输出路径。"),
    ] = None,
    validation_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 风险事件发生记录校验报告输出路径。"),
    ] = None,
    min_confidence: Annotated[
        float,
        typer.Option(help="低于该 confidence 的 LLM 预审记录不写入正式 occurrence。"),
    ] = 0.0,
    next_review_days: Annotated[
        int,
        typer.Option(help="LLM formal assessment 的下次复核间隔天数。"),
    ] = 1,
    include_attestation: Annotated[
        bool,
        typer.Option(help="同时写入 LLM formal attestation。"),
    ] = True,
    overwrite: Annotated[
        bool,
        typer.Option(help="允许覆盖同名 LLM formal occurrence/attestation YAML。"),
    ] = False,
) -> None:
    """把 LLM 预审结果作为正式风险评估输入写入 occurrence/attestation。"""
    assessment_date = _parse_date(as_of) if as_of else date.today()
    if min_confidence < 0 or min_confidence > 1:
        raise typer.BadParameter("min_confidence 必须在 0 到 1 之间。")
    if next_review_days < 0:
        raise typer.BadParameter("next_review_days 不能为负数。")
    report_path = output_path or default_llm_formal_assessment_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        assessment_date,
    )
    validation_output = validation_report_path or default_risk_event_occurrence_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        assessment_date,
    )
    try:
        report = build_llm_formal_assessment_report(
            queue_path,
            as_of=assessment_date,
            risk_events=load_risk_events(risk_events_path),
            include_attestation=include_attestation,
            next_review_days=next_review_days,
            min_confidence=min_confidence,
        )
        write_llm_formal_assessment_report(report, report_path)
    except (OSError, ValueError) as exc:
        console.print(f"[red]LLM 正式评估输入无法读取或校验失败：{exc}[/red]")
        raise typer.Exit(code=1) from exc

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]LLM 正式风险评估状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    console.print(f"输入队列：{queue_path}")
    console.print(
        f"预审记录：{report.record_count}；写入 occurrence：{report.occurrence_count}；"
        f"active={report.active_occurrence_count}；watch={report.watch_occurrence_count}"
    )
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")
    if not report.passed:
        raise typer.Exit(code=1)

    try:
        written_paths = write_llm_formal_assessment_outputs(
            report,
            output_dir,
            overwrite=overwrite,
        )
    except FileExistsError as exc:
        console.print(f"[red]{exc}[/red]")
        console.print("如确认要更新同名 LLM formal 记录，请显式传入 --overwrite。")
        raise typer.Exit(code=1) from exc

    validation_report = validate_risk_event_occurrence_store(
        store=load_risk_event_occurrence_store(output_dir),
        risk_events=load_risk_events(risk_events_path),
        as_of=assessment_date,
    )
    review_report = build_risk_event_occurrence_review_report(validation_report)
    write_risk_event_occurrence_review_report(review_report, validation_output)
    validation_style = (
        "green"
        if validation_report.status == "PASS"
        else "yellow"
        if validation_report.passed
        else "red"
    )
    console.print(f"写入 YAML：{len(written_paths)} 个文件 -> {output_dir}")
    console.print(
        f"[{validation_style}]风险事件发生记录校验状态："
        f"{validation_report.status}[/{validation_style}]"
    )
    console.print(f"校验报告：{validation_output}")
    console.print("LLM formal assessment 已作为正式评估输入，但不会被标记为人工复核。")
    if not validation_report.passed:
        raise typer.Exit(code=1)


@risk_events_app.command("precheck-openai")
def precheck_risk_events_with_openai_command(
    input_path: Annotated[
        Path,
        typer.Option(help="LLM 预审输入 JSON/YAML，包含 source_id 或 source_permission envelope。"),
    ],
    queue_path: Annotated[
        Path,
        typer.Option(help="写入风险事件预审待复核队列 JSON 的路径。"),
    ] = DEFAULT_RISK_EVENT_PREREVIEW_QUEUE_PATH,
    data_sources_path: Annotated[
        Path,
        typer.Option(help="数据源目录路径，用于解析 provider LLM 权限。"),
    ] = DEFAULT_DATA_SOURCES_CONFIG_PATH,
    risk_events_path: Annotated[
        Path,
        typer.Option(help="风险事件配置路径，用于检查 matched_risk_ids。"),
    ] = DEFAULT_RISK_EVENTS_CONFIG_PATH,
    llm_request_profiles_path: Annotated[
        Path,
        typer.Option(help="LLM request profile 配置路径。"),
    ] = DEFAULT_LLM_REQUEST_PROFILES_CONFIG_PATH,
    llm_request_profile: Annotated[
        str,
        typer.Option(help="本次 LLM 请求使用的 profile_id。"),
    ] = DEFAULT_RISK_EVENT_SINGLE_PREREVIEW_PROFILE,
    as_of: Annotated[
        str | None,
        typer.Option(help="预审和校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 风险事件 OpenAI 预审报告输出路径。"),
    ] = None,
    api_key_env: Annotated[
        str,
        typer.Option(help="读取 OpenAI API key 的环境变量名。"),
    ] = "OPENAI_API_KEY",
    model: Annotated[
        str | None,
        typer.Option(help="覆盖 profile 中的 OpenAI Responses API 模型。"),
    ] = None,
    reasoning_effort: Annotated[
        str | None,
        typer.Option(help="覆盖 profile 中的 OpenAI Responses API reasoning.effort。"),
    ] = None,
    timeout_seconds: Annotated[
        float | None,
        typer.Option(help="覆盖 profile 中的 OpenAI Responses API 请求读超时秒数。"),
    ] = None,
    openai_http_client: Annotated[
        str | None,
        typer.Option(
            help="覆盖 profile 中的 OpenAI Responses API HTTP 客户端：requests 或 urllib。"
        ),
    ] = None,
    openai_cache_dir: Annotated[
        Path,
        typer.Option(help="OpenAI 请求/响应本地缓存与审计归档目录。"),
    ] = DEFAULT_OPENAI_REQUEST_CACHE_PATH,
    openai_cache_ttl_hours: Annotated[
        float | None,
        typer.Option(help="覆盖 profile 中完全相同 OpenAI 请求的本地缓存复用时长，单位小时。"),
    ] = None,
) -> None:
    """调用 OpenAI API 整理风险事件候选，并写入人工复核队列。"""
    profile = _load_llm_request_profile(llm_request_profiles_path, llm_request_profile)
    effective_model = _coalesce_profile_value(model, profile.model)
    effective_reasoning_effort = _coalesce_profile_value(
        reasoning_effort,
        profile.reasoning_effort,
    )
    effective_timeout_seconds = _coalesce_profile_value(
        timeout_seconds,
        profile.timeout_seconds,
    )
    effective_http_client = _coalesce_profile_value(openai_http_client, profile.http_client)
    effective_cache_ttl_hours = _coalesce_profile_value(
        openai_cache_ttl_hours,
        profile.cache_ttl_hours,
    )
    if effective_timeout_seconds <= 0:
        raise typer.BadParameter("OpenAI 请求超时秒数必须为正数。")
    if effective_cache_ttl_hours <= 0:
        raise typer.BadParameter("OpenAI 请求缓存 TTL 小时数必须为正数。")
    precheck_date = _parse_date(as_of) if as_of else date.today()
    report_path = output_path or default_risk_event_openai_prereview_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        precheck_date,
    )
    try:
        packet = load_llm_claim_precheck_input(input_path)
    except (OSError, ValueError) as exc:
        console.print(f"[red]风险事件 OpenAI 预审输入无法读取或校验失败：{exc}[/red]")
        raise typer.Exit(code=1) from exc

    report = run_openai_risk_event_prereview(
        packet,
        api_key=os.getenv(api_key_env, ""),
        data_sources=load_data_sources(data_sources_path),
        risk_events=load_risk_events(risk_events_path),
        input_path=input_path,
        as_of=precheck_date,
        model=effective_model,
        reasoning_effort=effective_reasoning_effort,
        endpoint=profile.endpoint,
        timeout_seconds=effective_timeout_seconds,
        http_client=effective_http_client,
        openai_cache_dir=openai_cache_dir,
        openai_cache_ttl_seconds=effective_cache_ttl_hours * 3600,
        max_retries=profile.max_retries,
    )
    write_risk_event_prereview_import_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(
        f"[{status_style}]风险事件 OpenAI 预审状态："
        f"{report.status}[/{status_style}]"
    )
    console.print(f"预审报告：{report_path}")
    console.print(
        f"LLM request profile：{profile.profile_id}；"
        f"model={effective_model}；reasoning={effective_reasoning_effort}"
    )
    console.print(
        f"LLM claim 数：{report.row_count}；"
        f"风险事件候选：{report.record_count}；"
        f"L2/L3 候选：{report.high_level_candidate_count}"
    )
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")
    if not report.passed:
        raise typer.Exit(code=1)

    written_path = write_risk_event_prereview_queue(report, queue_path)
    console.print(f"预审待复核队列：{written_path}")
    console.print("OpenAI 输出保持 llm_extracted / pending_review，不进入评分或仓位闸门。")


@risk_events_app.command("import-prereview-csv")
def import_risk_event_prereview_csv_command(
    input_path: Annotated[
        Path,
        typer.Option(help="OpenAI 结构化预审结果 CSV 输入路径。"),
    ],
    queue_path: Annotated[
        Path,
        typer.Option(help="写入风险事件预审待复核队列 JSON 的路径。"),
    ] = DEFAULT_RISK_EVENT_PREREVIEW_QUEUE_PATH,
    risk_events_path: Annotated[
        Path,
        typer.Option(help="风险事件配置路径，用于检查 matched_risk_ids。"),
    ] = DEFAULT_RISK_EVENTS_CONFIG_PATH,
    as_of: Annotated[
        str | None,
        typer.Option(help="导入和校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 预审导入报告输出路径。"),
    ] = None,
) -> None:
    """导入 OpenAI 风险事件预审结果，并写入人工复核队列。"""
    import_date = _parse_date(as_of) if as_of else date.today()
    report_path = output_path or default_risk_event_prereview_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        import_date,
    )
    import_report = import_risk_event_prereview_csv(
        input_path,
        risk_events=load_risk_events(risk_events_path),
        as_of=import_date,
    )
    write_risk_event_prereview_import_report(import_report, report_path)

    status_style = (
        "green" if import_report.status == "PASS" else "yellow" if import_report.passed else "red"
    )
    console.print(
        f"[{status_style}]风险事件 OpenAI 预审导入状态："
        f"{import_report.status}[/{status_style}]"
    )
    console.print(f"导入报告：{report_path}")
    console.print(
        f"CSV 行数：{import_report.row_count}；"
        f"预审记录：{import_report.record_count}；"
        f"L2/L3 候选：{import_report.high_level_candidate_count}"
    )
    console.print(f"错误数：{import_report.error_count}；警告数：{import_report.warning_count}")
    if not import_report.passed:
        raise typer.Exit(code=1)

    written_path = write_risk_event_prereview_queue(import_report, queue_path)
    console.print(f"预审待复核队列：{written_path}")
    console.print("预审记录保持 llm_extracted / pending_review，不进入评分或仓位闸门。")


@risk_events_app.command("validate-occurrences")
def validate_risk_event_occurrences(
    input_path: Annotated[
        Path,
        typer.Option(help="风险事件发生记录 YAML 文件或目录路径。"),
    ] = DEFAULT_RISK_EVENT_OCCURRENCES_PATH,
    config_path: Annotated[
        Path,
        typer.Option(help="风险事件规则配置文件路径。"),
    ] = DEFAULT_RISK_EVENTS_CONFIG_PATH,
    as_of: Annotated[
        str | None,
        typer.Option(help="校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 风险事件发生记录报告输出路径。"),
    ] = None,
) -> None:
    """校验实际发生的风险事件记录，并生成可供日报评分引用的报告。"""
    validation_date = _parse_date(as_of) if as_of else date.today()
    report_path = output_path or default_risk_event_occurrence_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        validation_date,
    )
    validation_report = validate_risk_event_occurrence_store(
        store=load_risk_event_occurrence_store(input_path),
        risk_events=load_risk_events(config_path),
        as_of=validation_date,
    )
    review_report = build_risk_event_occurrence_review_report(validation_report)
    write_risk_event_occurrence_review_report(review_report, report_path)

    status_style = (
        "green"
        if review_report.status == "PASS"
        else "yellow"
        if validation_report.passed
        else "red"
    )
    console.print(
        f"[{status_style}]风险事件发生记录状态：{review_report.status}[/{status_style}]"
    )
    console.print(f"报告：{report_path}")
    console.print(
        f"发生记录数：{validation_report.occurrence_count}；"
        f"活跃/观察：{validation_report.active_occurrence_count}；"
        f"可评分：{len(review_report.score_eligible_active_items)}；"
        f"可触发仓位闸门：{len(review_report.position_gate_eligible_active_items)}"
    )
    console.print(
        f"复核声明数：{validation_report.review_attestation_count}；"
        f"当前有效：{validation_report.current_review_attestation_count}"
    )
    console.print(f"错误数：{validation_report.error_count}；警告数：{validation_report.warning_count}")

    if not validation_report.passed:
        raise typer.Exit(code=1)


@data_sources_app.command("list")
def list_data_sources(
    config_path: Annotated[
        Path,
        typer.Option(help="数据源目录配置文件路径。"),
    ] = DEFAULT_DATA_SOURCES_CONFIG_PATH,
    active_only: Annotated[
        bool,
        typer.Option("--active-only/--all", help="只显示活跃数据源，或显示全部数据源。"),
    ] = False,
) -> None:
    """列出数据源目录。"""
    data_sources = load_data_sources(config_path)

    table = Table(title="数据源目录")
    table.add_column("Source", overflow="fold")
    table.add_column("Provider", overflow="fold")
    table.add_column("类型")
    table.add_column("状态")
    table.add_column("领域", overflow="fold")
    table.add_column("缓存", overflow="fold")
    for source in sorted(data_sources.sources, key=lambda item: item.source_id):
        if active_only and source.status != "active":
            continue
        table.add_row(
            source.source_id,
            source.provider,
            _data_source_type_label(source.source_type),
            _data_source_status_label(source.status),
            ", ".join(source.domains),
            ", ".join(source.cache_paths),
        )
    console.print(table)


@data_sources_app.command("validate")
def validate_data_sources(
    config_path: Annotated[
        Path,
        typer.Option(help="数据源目录配置文件路径。"),
    ] = DEFAULT_DATA_SOURCES_CONFIG_PATH,
    as_of: Annotated[
        str | None,
        typer.Option(help="校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 数据源目录校验报告输出路径。"),
    ] = None,
) -> None:
    """校验数据源目录、审计字段和来源限制。"""
    validation_date = _parse_date(as_of) if as_of else date.today()
    report_path = output_path or default_data_sources_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        validation_date,
    )
    report = validate_data_sources_config(
        config=load_data_sources(config_path),
        as_of=validation_date,
    )
    write_data_sources_validation_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]数据源目录校验状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    console.print(f"数据源数量：{len(report.sources)}；活跃：{report.active_count}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")

    if not report.passed:
        raise typer.Exit(code=1)


@data_sources_app.command("health")
def data_source_health(
    config_path: Annotated[
        Path,
        typer.Option(help="数据源目录配置文件路径。"),
    ] = DEFAULT_DATA_SOURCES_CONFIG_PATH,
    manifest_path: Annotated[
        Path,
        typer.Option(help="download_manifest.csv 路径。"),
    ] = PROJECT_ROOT / "data" / "raw" / "download_manifest.csv",
    as_of: Annotated[
        str | None,
        typer.Option(help="评估日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 数据源健康报告输出路径。"),
    ] = None,
) -> None:
    """生成 provider health 和 reconciliation 覆盖报告。"""
    evaluation_date = _parse_date(as_of) if as_of else date.today()
    report_path = output_path or default_data_source_health_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        evaluation_date,
    )
    report = build_data_source_health_report(
        config=load_data_sources(config_path),
        as_of=evaluation_date,
        manifest_path=manifest_path,
    )
    write_data_source_health_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]数据源健康状态：{report.status}[/{status_style}]")
    console.print(f"Provider health score：{report.health_score}")
    console.print(f"报告：{report_path}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")

    if not report.passed:
        raise typer.Exit(code=1)


@data_sources_app.command("yahoo-price-diagnostic")
def yahoo_price_diagnostic(
    prices_path: Annotated[
        Path,
        typer.Option(help="FMP 主价格缓存 CSV 路径。"),
    ] = PROJECT_ROOT / "data" / "raw" / "prices_daily.csv",
    rates_path: Annotated[
        Path,
        typer.Option(help="FRED 宏观序列 CSV 路径，用于复用数据质量报告。"),
    ] = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv",
    marketstack_prices_path: Annotated[
        Path | None,
        typer.Option(help="Marketstack 第二行情源 CSV 路径；默认跟随主价格缓存目录。"),
    ] = None,
    as_of: Annotated[
        str | None,
        typer.Option(help="诊断日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown Yahoo 诊断报告输出路径。"),
    ] = None,
    full_universe: Annotated[
        bool,
        typer.Option(
            "--full-universe",
            help="复用完整 AI 产业链标的的数据质量上下文。",
        ),
    ] = False,
    window_days: Annotated[
        int,
        typer.Option(help="围绕异常 ticker/date 拉取 Yahoo 样本的前后自然日窗口。"),
    ] = 3,
    max_targets: Annotated[
        int,
        typer.Option(help="最多复查的 Marketstack 异常 ticker/date 数量。"),
    ] = 20,
) -> None:
    """对 Marketstack 自检坏行执行只读 Yahoo 诊断复查。"""
    universe = load_universe()
    quality_config = load_data_quality()
    diagnostic_date = _parse_date(as_of) if as_of else date.today()
    secondary_path = marketstack_prices_path or _marketstack_prices_path(prices_path)
    report_path = output_path or default_yahoo_price_diagnostic_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        diagnostic_date,
    )
    quality_report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=configured_price_tickers(
            universe,
            include_full_ai_chain=full_universe,
        ),
        expected_rate_series=configured_rate_series(universe),
        quality_config=quality_config,
        as_of=diagnostic_date,
        manifest_path=_download_manifest_path(prices_path),
        secondary_prices_path=secondary_path,
        require_secondary_prices=_requires_marketstack_prices(prices_path),
    )
    diagnostic_report = build_yahoo_price_diagnostic_report(
        primary_prices_path=prices_path,
        marketstack_prices_path=secondary_path,
        quality_report=quality_report,
        quality_config=quality_config,
        yahoo_provider=YFinancePriceProvider(),
        as_of=diagnostic_date,
        window_days=window_days,
        max_targets=max_targets,
    )
    write_yahoo_price_diagnostic_report(diagnostic_report, report_path)

    status_style = (
        "green"
        if diagnostic_report.status == "PASS"
        else "yellow"
        if diagnostic_report.status != "DIAGNOSTIC_FAILED"
        else "red"
    )
    console.print(
        f"[{status_style}]Yahoo 价格诊断状态："
        f"{diagnostic_report.status}[/{status_style}]"
    )
    console.print(f"报告：{report_path}")
    console.print(f"诊断目标：{len(diagnostic_report.targets)}")
    console.print(f"Yahoo 返回行数：{diagnostic_report.row_count}")
    console.print("治理边界：diagnostic only / production_effect=none，不写价格缓存或评分。")


@fundamentals_app.command("list-sec-companies")
def list_sec_companies(
    config_path: Annotated[
        Path,
        typer.Option(help="SEC company CIK 配置文件路径。"),
    ] = DEFAULT_SEC_COMPANIES_CONFIG_PATH,
    active_only: Annotated[
        bool,
        typer.Option("--active-only/--all", help="只显示活跃公司，或显示全部公司。"),
    ] = True,
) -> None:
    """列出 SEC companyfacts 下载配置。"""
    config = load_sec_companies(config_path)

    table = Table(title="SEC Company Facts 公司映射")
    table.add_column("Ticker")
    table.add_column("CIK")
    table.add_column("公司")
    table.add_column("活跃")
    table.add_column("Taxonomy")
    table.add_column("SEC 指标周期")
    for company in sorted(config.companies, key=lambda item: item.ticker):
        if active_only and not company.active:
            continue
        table.add_row(
            company.ticker,
            company.cik,
            company.company_name,
            "是" if company.active else "否",
            ", ".join(company.expected_taxonomies),
            ", ".join(company.sec_metric_periods),
        )
    console.print(table)


@fundamentals_app.command("download-sec-companyfacts")
def download_sec_companyfacts_command(
    config_path: Annotated[
        Path,
        typer.Option(help="SEC company CIK 配置文件路径。"),
    ] = DEFAULT_SEC_COMPANIES_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option(help="SEC companyfacts 原始 JSON 输出目录。"),
    ] = PROJECT_ROOT / "data" / "raw" / "sec_companyfacts",
    tickers: Annotated[
        str | None,
        typer.Option(help="逗号分隔的 ticker；未提供时下载全部活跃配置。"),
    ] = None,
    user_agent: Annotated[
        str | None,
        typer.Option(
            "--user-agent",
            envvar="SEC_USER_AGENT",
            help="SEC fair access 要求的 User-Agent；也可使用 SEC_USER_AGENT 环境变量。",
        ),
    ] = None,
) -> None:
    """下载 SEC companyfacts 原始 JSON，并写入审计 manifest。"""
    if user_agent is None or not user_agent.strip():
        raise typer.BadParameter(
            "SEC companyfacts 下载必须提供 --user-agent 或 SEC_USER_AGENT；"
            "格式建议包含项目/组织名称和联系邮箱。"
        )

    selected_tickers = _parse_csv_items(tickers) if tickers else None
    summary = download_sec_companyfacts(
        config=load_sec_companies(config_path),
        output_dir=output_dir,
        provider=SecEdgarCompanyFactsProvider(user_agent=user_agent),
        tickers=selected_tickers,
    )

    console.print("[green]SEC companyfacts 缓存已更新。[/green]")
    console.print(f"公司数量：{summary.company_count}")
    console.print(f"事实数量：{summary.total_fact_count}")
    console.print(f"输出目录：{summary.output_dir}")
    console.print(f"下载审计清单：{summary.manifest_path}")


@fundamentals_app.command("download-sec-submissions")
def download_sec_submissions_command(
    config_path: Annotated[
        Path,
        typer.Option(help="SEC company CIK 配置文件路径。"),
    ] = DEFAULT_SEC_COMPANIES_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option(help="SEC submissions 原始 JSON 输出目录。"),
    ] = DEFAULT_SEC_SUBMISSIONS_DIR,
    tickers: Annotated[
        str | None,
        typer.Option(help="逗号分隔的 ticker；未提供时下载全部活跃配置。"),
    ] = None,
    user_agent: Annotated[
        str | None,
        typer.Option(
            "--user-agent",
            envvar="SEC_USER_AGENT",
            help="SEC fair access 要求的 User-Agent；也可使用 SEC_USER_AGENT 环境变量。",
        ),
    ] = None,
) -> None:
    """下载 SEC submissions filing history，并写入审计 manifest。"""
    if user_agent is None or not user_agent.strip():
        raise typer.BadParameter(
            "SEC submissions 下载必须提供 --user-agent 或 SEC_USER_AGENT；"
            "格式建议包含项目/组织名称和联系邮箱。"
        )

    selected_tickers = _parse_csv_items(tickers) if tickers else None
    summary = download_sec_submissions(
        config=load_sec_companies(config_path),
        output_dir=output_dir,
        provider=SecEdgarFilingArchiveProvider(user_agent=user_agent),
        tickers=selected_tickers,
    )

    console.print("[green]SEC submissions 缓存已更新。[/green]")
    console.print(f"公司数量：{summary.company_count}")
    console.print(f"Filing 数量：{summary.filing_count}")
    console.print(f"输出目录：{summary.output_dir}")
    console.print(f"下载审计清单：{summary.manifest_path}")


@fundamentals_app.command("download-sec-filing-archive")
def download_sec_filing_archive_command(
    as_of: Annotated[
        str | None,
        typer.Option(help="评估日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    metrics_path: Annotated[
        Path | None,
        typer.Option(help="SEC 基本面指标 CSV 输入路径。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option(help="SEC accession directory index.json 输出目录。"),
    ] = DEFAULT_SEC_FILING_ARCHIVE_DIR,
    tickers: Annotated[
        str | None,
        typer.Option(help="逗号分隔的 ticker；未提供时读取当日全部 accession。"),
    ] = None,
    request_delay_seconds: Annotated[
        float,
        typer.Option(help="每个 SEC archive 请求之间的等待秒数。"),
    ] = 0.2,
    user_agent: Annotated[
        str | None,
        typer.Option(
            "--user-agent",
            envvar="SEC_USER_AGENT",
            help="SEC fair access 要求的 User-Agent；也可使用 SEC_USER_AGENT 环境变量。",
        ),
    ] = None,
) -> None:
    """按 SEC 指标 CSV 已使用 accession 下载 archive index.json。"""
    if user_agent is None or not user_agent.strip():
        raise typer.BadParameter(
            "SEC filing archive 下载必须提供 --user-agent 或 SEC_USER_AGENT；"
            "格式建议包含项目/组织名称和联系邮箱。"
        )

    archive_date = _parse_date(as_of) if as_of else date.today()
    metrics_input = metrics_path or default_sec_fundamental_metrics_csv_path(
        PROJECT_ROOT / "data" / "processed",
        archive_date,
    )
    selected_tickers = _parse_csv_items(tickers) if tickers else None
    summary = download_sec_filing_archive_indexes(
        metrics_path=metrics_input,
        as_of=archive_date,
        output_dir=output_dir,
        provider=SecEdgarFilingArchiveProvider(user_agent=user_agent),
        tickers=selected_tickers,
        request_delay_seconds=request_delay_seconds,
    )

    console.print("[green]SEC filing archive index 已更新。[/green]")
    console.print(f"Accession 数量：{summary.accession_count}")
    console.print(f"输出目录：{summary.output_dir}")
    console.print(f"下载审计清单：{summary.manifest_path}")


@fundamentals_app.command("sec-accession-coverage")
def sec_accession_coverage_command(
    as_of: Annotated[
        str | None,
        typer.Option(help="评估日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    metrics_path: Annotated[
        Path | None,
        typer.Option(help="SEC 基本面指标 CSV 输入路径。"),
    ] = None,
    submissions_dir: Annotated[
        Path,
        typer.Option(help="SEC submissions 原始 JSON 输入目录。"),
    ] = DEFAULT_SEC_SUBMISSIONS_DIR,
    filing_archive_dir: Annotated[
        Path,
        typer.Option(help="SEC accession directory index.json 输入目录。"),
    ] = DEFAULT_SEC_FILING_ARCHIVE_DIR,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown SEC accession archive 覆盖报告输出路径。"),
    ] = None,
) -> None:
    """检查 SEC 指标 CSV 已用 accession 的 submissions/archive 覆盖。"""
    coverage_date = _parse_date(as_of) if as_of else date.today()
    metrics_input = metrics_path or default_sec_fundamental_metrics_csv_path(
        PROJECT_ROOT / "data" / "processed",
        coverage_date,
    )
    report_output = output_path or default_sec_accession_coverage_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        coverage_date,
    )
    report = build_sec_accession_coverage_report(
        metrics_path=metrics_input,
        submissions_dir=submissions_dir,
        filing_archive_dir=filing_archive_dir,
        as_of=coverage_date,
    )
    write_sec_accession_coverage_report(report, report_output)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]SEC accession 覆盖状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_output}")
    console.print(f"Accession 数：{report.accession_count}；已覆盖：{report.covered_count}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")
    if not report.passed:
        raise typer.Exit(code=1)


@fundamentals_app.command("validate-sec-companyfacts")
def validate_sec_companyfacts_command(
    config_path: Annotated[
        Path,
        typer.Option(help="SEC company CIK 配置文件路径。"),
    ] = DEFAULT_SEC_COMPANIES_CONFIG_PATH,
    input_dir: Annotated[
        Path,
        typer.Option(help="SEC companyfacts 原始 JSON 输入目录。"),
    ] = PROJECT_ROOT / "data" / "raw" / "sec_companyfacts",
    as_of: Annotated[
        str | None,
        typer.Option(help="校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown SEC companyfacts 校验报告输出路径。"),
    ] = None,
) -> None:
    """校验 SEC companyfacts 原始缓存、CIK、taxonomy 和 manifest。"""
    validation_date = _parse_date(as_of) if as_of else date.today()
    report_path = output_path or default_sec_companyfacts_validation_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        validation_date,
    )
    report = validate_sec_companyfacts_cache(
        config=load_sec_companies(config_path),
        input_dir=input_dir,
        as_of=validation_date,
    )
    write_sec_companyfacts_validation_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]SEC companyfacts 校验状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    console.print(f"配置公司数：{report.file_count}；已缓存：{report.available_count}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")

    if not report.passed:
        raise typer.Exit(code=1)


@fundamentals_app.command("extract-sec-metrics")
def extract_sec_metrics_command(
    sec_companies_path: Annotated[
        Path,
        typer.Option(help="SEC company CIK 配置文件路径。"),
    ] = DEFAULT_SEC_COMPANIES_CONFIG_PATH,
    metrics_path: Annotated[
        Path,
        typer.Option(help="SEC 指标映射配置文件路径。"),
    ] = DEFAULT_FUNDAMENTAL_METRICS_CONFIG_PATH,
    input_dir: Annotated[
        Path,
        typer.Option(help="SEC companyfacts 原始 JSON 输入目录。"),
    ] = PROJECT_ROOT / "data" / "raw" / "sec_companyfacts",
    as_of: Annotated[
        str | None,
        typer.Option(help="抽取日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="SEC 基本面指标 CSV 输出路径。"),
    ] = None,
    report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown SEC 基本面指标报告输出路径。"),
    ] = None,
    validation_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown SEC companyfacts 校验报告输出路径。"),
    ] = None,
) -> None:
    """在 SEC 缓存校验通过后抽取结构化基本面指标。"""
    extraction_date = _parse_date(as_of) if as_of else date.today()
    validation_output = validation_report_path or default_sec_companyfacts_validation_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        extraction_date,
    )
    csv_output = output_path or default_sec_fundamental_metrics_csv_path(
        PROJECT_ROOT / "data" / "processed",
        extraction_date,
    )
    markdown_output = report_path or default_sec_fundamental_metrics_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        extraction_date,
    )

    sec_companies = load_sec_companies(sec_companies_path)
    validation_report = validate_sec_companyfacts_cache(
        config=sec_companies,
        input_dir=input_dir,
        as_of=extraction_date,
    )
    write_sec_companyfacts_validation_report(validation_report, validation_output)
    if not validation_report.passed:
        console.print("[red]SEC companyfacts 质量门禁失败，已停止指标抽取。[/red]")
        console.print(f"SEC 缓存校验报告：{validation_output}")
        console.print(
            f"错误数：{validation_report.error_count}；"
            f"警告数：{validation_report.warning_count}"
        )
        raise typer.Exit(code=1)

    report = build_sec_fundamental_metrics_report(
        companies=sec_companies,
        metrics=load_fundamental_metrics(metrics_path),
        input_dir=input_dir,
        as_of=extraction_date,
        validation_report=validation_report,
    )
    csv_path = write_sec_fundamental_metrics_csv(report, csv_output)
    markdown_path = write_sec_fundamental_metrics_report(
        report=report,
        validation_report_path=validation_output,
        output_csv_path=csv_path,
        output_path=markdown_output,
    )

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]SEC 基本面指标抽取状态：{report.status}[/{status_style}]")
    console.print(f"SEC 缓存校验报告：{validation_output}（{validation_report.status}）")
    console.print(f"指标 CSV：{csv_path}")
    console.print(f"指标报告：{markdown_path}")
    console.print(f"公司数：{report.company_count}；指标行数：{report.row_count}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")

    if not report.passed:
        raise typer.Exit(code=1)


@fundamentals_app.command("validate-sec-metrics")
def validate_sec_metrics_command(
    sec_companies_path: Annotated[
        Path,
        typer.Option(help="SEC company CIK 配置文件路径。"),
    ] = DEFAULT_SEC_COMPANIES_CONFIG_PATH,
    metrics_path: Annotated[
        Path,
        typer.Option(help="SEC 指标映射配置文件路径。"),
    ] = DEFAULT_FUNDAMENTAL_METRICS_CONFIG_PATH,
    input_path: Annotated[
        Path | None,
        typer.Option(help="SEC 基本面指标 CSV 输入路径。"),
    ] = None,
    as_of: Annotated[
        str | None,
        typer.Option(help="校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown SEC 基本面指标 CSV 校验报告输出路径。"),
    ] = None,
) -> None:
    """校验已抽取的 SEC 基本面指标 CSV。"""
    validation_date = _parse_date(as_of) if as_of else date.today()
    csv_input = input_path or default_sec_fundamental_metrics_csv_path(
        PROJECT_ROOT / "data" / "processed",
        validation_date,
    )
    report_output = output_path or default_sec_fundamental_metrics_validation_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        validation_date,
    )
    report = validate_sec_fundamental_metrics_csv(
        companies=load_sec_companies(sec_companies_path),
        metrics=load_fundamental_metrics(metrics_path),
        input_path=csv_input,
        as_of=validation_date,
    )
    write_sec_fundamental_metrics_validation_report(report, report_output)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]SEC 基本面指标 CSV 校验状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_output}")
    console.print(f"输入文件：{csv_input}")
    console.print(
        f"覆盖率：{report.coverage:.0%}；"
        f"当日行数：{report.as_of_row_count}；"
        f"总行数：{report.row_count}"
    )
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")

    if not report.passed:
        raise typer.Exit(code=1)


@fundamentals_app.command("build-sec-features")
def build_sec_features_command(
    sec_companies_path: Annotated[
        Path,
        typer.Option(help="SEC company CIK 配置文件路径。"),
    ] = DEFAULT_SEC_COMPANIES_CONFIG_PATH,
    metrics_path: Annotated[
        Path,
        typer.Option(help="SEC 指标映射配置文件路径，用于先校验指标 CSV。"),
    ] = DEFAULT_FUNDAMENTAL_METRICS_CONFIG_PATH,
    feature_config_path: Annotated[
        Path,
        typer.Option(help="SEC 基本面特征公式配置文件路径。"),
    ] = DEFAULT_FUNDAMENTAL_FEATURES_CONFIG_PATH,
    input_path: Annotated[
        Path | None,
        typer.Option(help="SEC 基本面指标 CSV 输入路径。"),
    ] = None,
    as_of: Annotated[
        str | None,
        typer.Option(help="特征日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="SEC 基本面特征 CSV 输出路径。"),
    ] = None,
    report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown SEC 基本面特征报告输出路径。"),
    ] = None,
    validation_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown SEC 基本面指标 CSV 校验报告输出路径。"),
    ] = None,
) -> None:
    """在 SEC 指标 CSV 校验通过后构建基本面比率特征。"""
    feature_date = _parse_date(as_of) if as_of else date.today()
    csv_input = input_path or default_sec_fundamental_metrics_csv_path(
        PROJECT_ROOT / "data" / "processed",
        feature_date,
    )
    validation_output = (
        validation_report_path
        or default_sec_fundamental_metrics_validation_report_path(
            PROJECT_ROOT / "outputs" / "reports",
            feature_date,
        )
    )
    feature_csv_output = output_path or default_sec_fundamental_features_csv_path(
        PROJECT_ROOT / "data" / "processed",
        feature_date,
    )
    feature_report_output = report_path or default_sec_fundamental_features_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        feature_date,
    )

    sec_companies = load_sec_companies(sec_companies_path)
    metrics = load_fundamental_metrics(metrics_path)
    validation_report = validate_sec_fundamental_metrics_csv(
        companies=sec_companies,
        metrics=metrics,
        input_path=csv_input,
        as_of=feature_date,
    )
    write_sec_fundamental_metrics_validation_report(validation_report, validation_output)
    if not validation_report.passed:
        console.print("[red]SEC 基本面指标 CSV 校验失败，已停止特征构建。[/red]")
        console.print(f"SEC 指标 CSV 校验报告：{validation_output}")
        console.print(
            f"错误数：{validation_report.error_count}；"
            f"警告数：{validation_report.warning_count}"
        )
        raise typer.Exit(code=1)

    report = build_sec_fundamental_features_report(
        companies=sec_companies,
        feature_config=load_fundamental_features(feature_config_path),
        input_path=csv_input,
        as_of=feature_date,
        validation_report=validation_report,
    )
    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    if not report.passed:
        markdown_path = write_sec_fundamental_features_report(
            report=report,
            validation_report_path=validation_output,
            output_csv_path=feature_csv_output,
            output_path=feature_report_output,
        )
        console.print(
            f"[{status_style}]SEC 基本面特征构建状态：{report.status}[/{status_style}]"
        )
        console.print(f"SEC 指标 CSV 校验报告：{validation_output}（{validation_report.status}）")
        console.print(f"基本面特征 CSV 未写入：{feature_csv_output}")
        console.print(f"基本面特征报告：{markdown_path}")
        console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")
        raise typer.Exit(code=1)

    csv_path = write_sec_fundamental_features_csv(report, feature_csv_output)
    markdown_path = write_sec_fundamental_features_report(
        report=report,
        validation_report_path=validation_output,
        output_csv_path=csv_path,
        output_path=feature_report_output,
    )

    console.print(f"[{status_style}]SEC 基本面特征构建状态：{report.status}[/{status_style}]")
    console.print(f"SEC 指标 CSV 校验报告：{validation_output}（{validation_report.status}）")
    console.print(f"基本面特征 CSV：{csv_path}")
    console.print(f"基本面特征报告：{markdown_path}")
    console.print(f"公司数：{report.company_count}；特征行数：{report.row_count}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")

    if not report.passed:
        raise typer.Exit(code=1)


@fundamentals_app.command("extract-tsm-ir-pdf-text")
def extract_tsm_ir_pdf_text_command(
    input_path: Annotated[
        Path,
        typer.Option(help="本地 TSMC IR Management Report 官方 PDF 输入路径。"),
    ],
    source_url: Annotated[
        str,
        typer.Option(help="TSMC Investor Relations 官方 PDF URL。"),
    ],
    output_path: Annotated[
        Path | None,
        typer.Option(help="抽取后的 Management Report 文本输出路径。"),
    ] = None,
    as_of: Annotated[
        str | None,
        typer.Option(help="报告日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    extracted_at: Annotated[
        str | None,
        typer.Option(help="抽取时间，ISO datetime 或 YYYY-MM-DD；默认当前 UTC 时间。"),
    ] = None,
    report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown TSMC IR PDF 文本抽取报告输出路径。"),
    ] = None,
) -> None:
    """从 TSMC IR 官方 PDF 的可抽取文本层生成 Management Report 文本。"""
    report_date = _parse_date(as_of) if as_of else date.today()
    extraction_datetime = _parse_datetime(extracted_at) if extracted_at else datetime.now(tz=UTC)
    text_output = output_path or (
        PROJECT_ROOT
        / "data"
        / "external"
        / "fundamentals"
        / "tsm_ir"
        / f"{input_path.stem}.txt"
    )
    markdown_output = report_path or (
        PROJECT_ROOT / "outputs" / "reports" / f"tsm_ir_pdf_text_{report_date}.md"
    )

    report = extract_tsm_ir_pdf_text(
        input_path=input_path,
        source_url=source_url,
        output_path=text_output,
        extracted_at=extraction_datetime,
    )
    markdown_path = write_tsm_ir_pdf_text_extraction_report(report, markdown_output)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]TSMC IR PDF 文本抽取状态：{report.status}[/{status_style}]")
    console.print(f"Source URL：{source_url}")
    console.print(f"PDF：{input_path}")
    console.print(f"抽取文本：{text_output}")
    console.print(f"报告：{markdown_path}")
    console.print(f"页数：{report.page_count}；字符数：{report.character_count}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")
    if not report.passed:
        console.print("[red]TSMC IR PDF 文本抽取未通过，未生成可用于导入的可信文本。[/red]")
        raise typer.Exit(code=1)


@fundamentals_app.command("import-tsm-ir-quarterly")
def import_tsm_ir_quarterly(
    input_path: Annotated[
        Path,
        typer.Option(help="TSMC IR Management Report 已抽取文本输入路径。"),
    ],
    source_url: Annotated[
        str,
        typer.Option(help="TSMC Investor Relations 官方季度页面或 Management Report URL。"),
    ],
    fiscal_year: Annotated[
        int,
        typer.Option(help="财年，例如 2026。"),
    ],
    fiscal_period: Annotated[
        str,
        typer.Option(help="财期，例如 Q1、Q2、Q3 或 Q4。"),
    ],
    as_of: Annotated[
        str | None,
        typer.Option(help="导入评估日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    captured_at: Annotated[
        str | None,
        typer.Option(help="采集时间，ISO datetime 或 YYYY-MM-DD；默认当前 UTC 时间。"),
    ] = None,
    filed_date: Annotated[
        str | None,
        typer.Option(
            help=(
                "TSMC IR Management Report 公开/披露日期，格式为 YYYY-MM-DD；"
                "未提供时使用 captured_at 日期。"
            ),
        ),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="TSMC IR 季度指标 CSV 输出路径。"),
    ] = None,
    report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown TSMC IR 季度基本面报告输出路径。"),
    ] = None,
) -> None:
    """从 TSMC IR 官方 Management Report 文本导入季度基本面指标。"""
    import_date = _parse_date(as_of) if as_of else date.today()
    captured_datetime = _parse_datetime(captured_at) if captured_at else datetime.now(tz=UTC)
    disclosed_date = _parse_date(filed_date) if filed_date else None
    csv_output = output_path or (
        PROJECT_ROOT / "data" / "processed" / "tsm_ir_quarterly_metrics.csv"
    )
    normalized_period = fiscal_period.upper()
    if normalized_period not in {"Q1", "Q2", "Q3", "Q4"}:
        raise typer.BadParameter("财期必须是 Q1、Q2、Q3 或 Q4。")
    markdown_output = report_path or (
        PROJECT_ROOT
        / "outputs"
        / "reports"
        / f"tsm_ir_quarterly_{fiscal_year}_{normalized_period}_{import_date}.md"
    )
    report = parse_tsm_ir_management_report_text(
        text=input_path.read_text(encoding="utf-8"),
        source_url=source_url,
        fiscal_year=fiscal_year,
        fiscal_period=normalized_period,
        as_of=import_date,
        captured_at=captured_datetime,
        source_path=input_path,
        filed_date=disclosed_date,
    )
    markdown_path = write_tsm_ir_quarterly_report(report, markdown_output)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]TSMC IR 季度基本面导入状态：{report.status}[/{status_style}]")
    console.print(f"报告：{markdown_path}")
    console.print(f"指标行数：{report.row_count}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")
    if not report.passed:
        console.print("[red]TSMC IR 季度基本面报告未通过，指标 CSV 未写入。[/red]")
        raise typer.Exit(code=1)

    csv_path = write_tsm_ir_quarterly_metrics_csv(report, csv_output)
    console.print(f"指标 CSV：{csv_path}")


@fundamentals_app.command("import-tsm-ir-quarterly-batch")
def import_tsm_ir_quarterly_batch(
    manifest_path: Annotated[
        Path,
        typer.Option(
            help=(
                "TSMC IR 批量导入 manifest CSV，字段为 fiscal_year,"
                "fiscal_period,source_url,input_path。"
            ),
        ),
    ],
    as_of: Annotated[
        str | None,
        typer.Option(help="导入评估日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    captured_at: Annotated[
        str | None,
        typer.Option(help="采集时间，ISO datetime 或 YYYY-MM-DD；默认当前 UTC 时间。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="TSMC IR 季度指标 CSV 输出路径。"),
    ] = None,
    report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown TSMC IR 批量季度基本面报告输出路径。"),
    ] = None,
) -> None:
    """按 manifest 批量导入多个 TSMC IR Management Report 文本。"""
    import_date = _parse_date(as_of) if as_of else date.today()
    captured_datetime = _parse_datetime(captured_at) if captured_at else datetime.now(tz=UTC)
    csv_output = output_path or (
        PROJECT_ROOT / "data" / "processed" / "tsm_ir_quarterly_metrics.csv"
    )
    markdown_output = report_path or (
        PROJECT_ROOT
        / "outputs"
        / "reports"
        / f"tsm_ir_quarterly_batch_{import_date}.md"
    )

    report = build_tsm_ir_quarterly_batch_import_report(
        manifest_path=manifest_path,
        as_of=import_date,
        captured_at=captured_datetime,
        output_path=csv_output,
    )
    markdown_path = write_tsm_ir_quarterly_batch_import_report(report, markdown_output)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(
        f"[{status_style}]TSMC IR 批量季度基本面导入状态："
        f"{report.status}[/{status_style}]"
    )
    console.print(f"Manifest：{manifest_path}")
    console.print(f"报告：{markdown_path}")
    console.print(f"季度条目数：{report.entry_count}；指标行数：{report.row_count}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")
    if not report.passed:
        console.print("[red]TSMC IR 批量季度基本面报告未通过，指标 CSV 未写入。[/red]")
        raise typer.Exit(code=1)

    csv_path = write_tsm_ir_quarterly_batch_metrics_csv(report, csv_output)
    console.print(f"指标 CSV：{csv_path}")


@fundamentals_app.command("fetch-tsm-ir-quarterly")
def fetch_tsm_ir_quarterly(
    source_url: Annotated[
        str,
        typer.Option(help="TSMC Investor Relations 官方季度页面 URL。"),
    ],
    fiscal_year: Annotated[
        int,
        typer.Option(help="财年，例如 2026。"),
    ],
    fiscal_period: Annotated[
        str,
        typer.Option(help="财期，例如 Q1、Q2、Q3 或 Q4。"),
    ],
    as_of: Annotated[
        str | None,
        typer.Option(help="导入评估日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    captured_at: Annotated[
        str | None,
        typer.Option(help="采集时间，ISO datetime 或 YYYY-MM-DD；默认当前 UTC 时间。"),
    ] = None,
    filed_date: Annotated[
        str | None,
        typer.Option(
            help=(
                "TSMC IR Management Report 公开/披露日期，格式为 YYYY-MM-DD；"
                "未提供时使用 captured_at 日期。"
            ),
        ),
    ] = None,
    source_text_path: Annotated[
        Path | None,
        typer.Option(help="保存下载到的 Management Report 文本路径。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="TSMC IR 季度指标 CSV 输出路径。"),
    ] = None,
    report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown TSMC IR 季度基本面报告输出路径。"),
    ] = None,
    timeout: Annotated[
        float,
        typer.Option(help="TSMC IR HTTP 请求超时时间，单位秒。"),
    ] = 30.0,
    user_agent: Annotated[
        str,
        typer.Option(
            "--user-agent",
            envvar="TSM_IR_USER_AGENT",
            help="TSMC IR HTTP User-Agent；也可使用 TSM_IR_USER_AGENT 环境变量。",
        ),
    ] = "ai-trading-system tsm-ir/0.1",
) -> None:
    """从 TSMC IR 官方季度页面发现并下载 Management Report 文本后导入指标。"""
    import_date = _parse_date(as_of) if as_of else date.today()
    captured_datetime = _parse_datetime(captured_at) if captured_at else datetime.now(tz=UTC)
    disclosed_date = _parse_date(filed_date) if filed_date else None
    normalized_period = fiscal_period.upper()
    if normalized_period not in {"Q1", "Q2", "Q3", "Q4"}:
        raise typer.BadParameter("财期必须是 Q1、Q2、Q3 或 Q4。")

    text_output = source_text_path or (
        PROJECT_ROOT
        / "data"
        / "external"
        / "fundamentals"
        / "tsm_ir"
        / f"{fiscal_year}_{normalized_period}_management_report_{import_date}.txt"
    )
    csv_output = output_path or (
        PROJECT_ROOT / "data" / "processed" / "tsm_ir_quarterly_metrics.csv"
    )
    markdown_output = report_path or (
        PROJECT_ROOT
        / "outputs"
        / "reports"
        / f"tsm_ir_quarterly_{fiscal_year}_{normalized_period}_{import_date}.md"
    )

    provider = TsmIrHttpProvider(timeout=timeout, user_agent=user_agent)
    try:
        management_resource = select_tsm_ir_management_report_resource(
            provider=provider,
            source_url=source_url,
        )
        management_text = provider.download_text(management_resource.url)
    except Exception as exc:
        console.print("[red]TSMC IR 官方页面或 Management Report 文本下载失败。[/red]")
        console.print(str(exc))
        raise typer.Exit(code=1) from exc

    text_output.parent.mkdir(parents=True, exist_ok=True)
    text_output.write_text(management_text, encoding="utf-8")
    report = parse_tsm_ir_management_report_text(
        text=management_text,
        source_url=management_resource.url,
        fiscal_year=fiscal_year,
        fiscal_period=normalized_period,
        as_of=import_date,
        captured_at=captured_datetime,
        source_path=text_output,
        filed_date=disclosed_date,
    )
    markdown_path = write_tsm_ir_quarterly_report(report, markdown_output)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]TSMC IR 季度基本面抓取状态：{report.status}[/{status_style}]")
    console.print(f"季度页面：{source_url}")
    console.print(f"Management Report URL：{management_resource.url}")
    console.print(f"原始文本：{text_output}")
    console.print(f"报告：{markdown_path}")
    console.print(f"指标行数：{report.row_count}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")
    if not report.passed:
        console.print("[red]TSMC IR 季度基本面报告未通过，指标 CSV 未写入。[/red]")
        raise typer.Exit(code=1)

    csv_path = write_tsm_ir_quarterly_metrics_csv(report, csv_output)
    console.print(f"指标 CSV：{csv_path}")


@fundamentals_app.command("merge-tsm-ir-sec-metrics")
def merge_tsm_ir_sec_metrics(
    input_path: Annotated[
        Path,
        typer.Option(help="TSMC IR 季度指标 CSV 输入路径。"),
    ] = PROJECT_ROOT / "data" / "processed" / "tsm_ir_quarterly_metrics.csv",
    sec_companies_path: Annotated[
        Path,
        typer.Option(help="SEC company CIK 配置文件路径；TSM 必须声明 quarterly。"),
    ] = DEFAULT_SEC_COMPANIES_CONFIG_PATH,
    metrics_path: Annotated[
        Path,
        typer.Option(help="SEC 指标映射配置文件路径，用于合并后校验。"),
    ] = DEFAULT_FUNDAMENTAL_METRICS_CONFIG_PATH,
    sec_input_path: Annotated[
        Path | None,
        typer.Option(help="既有 SEC 基本面指标 CSV；未提供时使用 as_of 默认路径。"),
    ] = None,
    as_of: Annotated[
        str | None,
        typer.Option(help="合并评估日期，格式为 YYYY-MM-DD；默认使用 TSM IR 输入最新 as_of。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="合并后的 SEC-style 基本面指标 CSV 输出路径。"),
    ] = None,
    validation_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown SEC 指标 CSV 校验报告输出路径。"),
    ] = None,
) -> None:
    """把 TSMC IR 季度指标合并到统一 SEC-style 基本面指标 CSV。"""
    if not input_path.exists():
        raise typer.BadParameter(f"TSMC IR 季度指标 CSV 不存在：{input_path}")

    all_tsm_rows = load_tsm_ir_quarterly_metric_rows_csv(input_path)
    if not all_tsm_rows:
        raise typer.BadParameter("TSMC IR 季度指标 CSV 没有可合并的指标行。")
    merge_date = _parse_date(as_of) if as_of else max(row.as_of for row in all_tsm_rows)
    tsm_rows = tuple(
        replace(row, as_of=merge_date)
        for row in select_tsm_ir_quarterly_metric_rows_as_of(
            all_tsm_rows,
            merge_date,
        )
    )
    if not tsm_rows:
        raise typer.BadParameter(
            f"TSMC IR 季度指标 CSV 不包含 {merge_date.isoformat()} 可用的已披露季度记录。"
        )

    sec_input = sec_input_path or default_sec_fundamental_metrics_csv_path(
        PROJECT_ROOT / "data" / "processed",
        merge_date,
    )
    csv_output = output_path or sec_input
    validation_output = (
        validation_report_path
        or default_sec_fundamental_metrics_validation_report_path(
            PROJECT_ROOT / "outputs" / "reports",
            merge_date,
        )
    )
    sec_companies = load_sec_companies(sec_companies_path)
    conversion_report = build_tsm_ir_sec_metric_conversion_report(tsm_rows, sec_companies)
    if not conversion_report.passed:
        console.print("[red]TSMC IR 转换为 SEC-style 指标失败，已停止合并。[/red]")
        console.print(
            f"错误数：{conversion_report.error_count}；"
            f"警告数：{conversion_report.warning_count}"
        )
        raise typer.Exit(code=1)

    existing_rows = load_sec_fundamental_metric_rows_csv(sec_input)
    merged_rows = merge_tsm_ir_quarterly_rows_into_sec_metrics(
        existing_rows=existing_rows,
        tsm_rows=tsm_rows,
        tsm_company=sec_companies,
    )
    csv_path = write_sec_fundamental_metric_rows_csv(merged_rows, csv_output)
    validation_report = validate_sec_fundamental_metric_rows(
        companies=sec_companies,
        metrics=load_fundamental_metrics(metrics_path),
        rows=merged_rows,
        source_path=csv_path,
        as_of=merge_date,
    )
    write_sec_fundamental_metrics_validation_report(validation_report, validation_output)

    status_style = (
        "green"
        if validation_report.status == "PASS"
        else "yellow"
        if validation_report.passed
        else "red"
    )
    console.print(
        f"[{status_style}]TSMC IR 合并后 SEC 指标校验状态："
        f"{validation_report.status}[/{status_style}]"
    )
    console.print(f"合并日期：{merge_date.isoformat()}")
    console.print(f"TSMC IR 转换行数：{len(conversion_report.rows)}")
    console.print(f"既有 SEC 指标行数：{len(existing_rows)}；合并后行数：{len(merged_rows)}")
    console.print(f"输出 CSV：{csv_path}")
    console.print(f"校验报告：{validation_output}")
    console.print(
        f"错误数：{validation_report.error_count}；警告数：{validation_report.warning_count}"
    )
    if not validation_report.passed:
        raise typer.Exit(code=1)


@valuation_app.command("fetch-fmp")
def fetch_fmp_valuations(
    tickers: Annotated[
        str | None,
        typer.Option(help="逗号分隔 ticker；未提供时使用 universe 的 AI core_watchlist。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option(help="写入 FMP 估值快照 YAML 的目录。"),
    ] = PROJECT_ROOT / "data" / "external" / "valuation_snapshots",
    analyst_history_dir: Annotated[
        Path,
        typer.Option(help="读取并写入 FMP analyst-estimates 原始历史快照的目录。"),
    ] = DEFAULT_FMP_ANALYST_ESTIMATE_HISTORY_DIR,
    pit_normalized_path: Annotated[
        Path | None,
        typer.Option(
            help=(
                "读取 FMP forward-only PIT 标准化 CSV 的文件或目录；"
                "默认使用 data/processed/pit_snapshots。"
            )
        ),
    ] = DEFAULT_FMP_FORWARD_PIT_NORMALIZED_DIR,
    as_of: Annotated[
        str | None,
        typer.Option(help="估值评估日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown FMP 拉取报告输出路径。"),
    ] = None,
    validation_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 估值快照校验报告输出路径。"),
    ] = None,
    api_key_env: Annotated[
        str,
        typer.Option(help="读取 FMP API key 的环境变量名。"),
    ] = "FMP_API_KEY",
    analyst_estimate_limit: Annotated[
        int,
        typer.Option(help="每个 ticker 拉取的 annual analyst estimate 记录数。"),
    ] = 10,
) -> None:
    """从 Financial Modeling Prep 拉取估值和预期快照。"""
    fetch_date = _parse_date(as_of) if as_of else date.today()
    selected_tickers = (
        _parse_csv_items(tickers)
        if tickers
        else load_universe().ai_chain.get("core_watchlist", [])
    )
    api_key = os.getenv(api_key_env)
    if not api_key:
        console.print(f"[red]未找到环境变量 {api_key_env}，已停止 FMP 拉取。[/red]")
        raise typer.Exit(code=1)

    fetch_report_output = output_path or default_fmp_valuation_fetch_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        fetch_date,
    )
    validation_output = validation_report_path or default_valuation_validation_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        fetch_date,
    )

    try:
        fetch_report = fetch_fmp_valuation_snapshots(
            selected_tickers,
            api_key,
            fetch_date,
            analyst_history_dir=analyst_history_dir,
            pit_normalized_path=pit_normalized_path,
            valuation_history_dir=output_dir,
            captured_at=fetch_date,
            analyst_estimate_limit=analyst_estimate_limit,
        )
    except ValueError as exc:
        console.print(f"[red]FMP 参数错误：{exc}[/red]")
        raise typer.Exit(code=1) from exc

    write_fmp_valuation_fetch_report(fetch_report, fetch_report_output)
    status_style = (
        "green" if fetch_report.status == "PASS" else "yellow" if fetch_report.passed else "red"
    )
    console.print(f"[{status_style}]FMP 估值拉取状态：{fetch_report.status}[/{status_style}]")
    console.print(f"拉取报告：{fetch_report_output}")
    console.print(
        f"请求标的：{', '.join(fetch_report.requested_tickers)}；"
        f"返回记录：{fetch_report.row_count}；生成快照：{fetch_report.imported_count}"
    )
    console.print(f"错误数：{fetch_report.error_count}；警告数：{fetch_report.warning_count}")
    if not fetch_report.passed:
        raise typer.Exit(code=1)

    history_paths = write_fmp_analyst_estimate_history_snapshots(
        fetch_report.analyst_estimate_history_snapshots,
        analyst_history_dir,
    )
    written_paths = write_valuation_snapshots_as_yaml(fetch_report.snapshots, output_dir)
    validation_report = validate_valuation_snapshot_store(
        store=load_valuation_snapshot_store(output_dir),
        universe=load_universe(),
        watchlist=load_watchlist(),
        as_of=fetch_date,
    )
    write_valuation_validation_report(validation_report, validation_output)

    validation_style = (
        "green"
        if validation_report.status == "PASS"
        else "yellow"
        if validation_report.passed
        else "red"
    )
    console.print(f"写入 YAML：{len(written_paths)} 个文件 -> {output_dir}")
    console.print(f"写入 analyst history：{len(history_paths)} 个文件 -> {analyst_history_dir}")
    console.print(
        f"[{validation_style}]估值快照校验状态："
        f"{validation_report.status}[/{validation_style}]"
    )
    console.print(f"校验报告：{validation_output}")
    if not validation_report.passed:
        raise typer.Exit(code=1)


@valuation_app.command("fetch-fmp-valuation-history")
def fetch_fmp_valuation_history(
    tickers: Annotated[
        str | None,
        typer.Option(help="逗号分隔 ticker；未提供时使用 universe 的 AI core_watchlist。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option(help="写入历史估值快照 YAML 的目录。"),
    ] = PROJECT_ROOT / "data" / "external" / "valuation_snapshots",
    raw_output_dir: Annotated[
        Path,
        typer.Option(help="写入 FMP historical key-metrics/ratios 原始 JSON 的目录。"),
    ] = DEFAULT_FMP_HISTORICAL_VALUATION_RAW_DIR,
    as_of: Annotated[
        str | None,
        typer.Option(help="拉取评估日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown FMP 历史估值拉取报告输出路径。"),
    ] = None,
    validation_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 估值快照校验报告输出路径。"),
    ] = None,
    api_key_env: Annotated[
        str,
        typer.Option(help="读取 FMP API key 的环境变量名。"),
    ] = "FMP_API_KEY",
    period: Annotated[
        str,
        typer.Option(help="FMP historical key-metrics/ratios period，annual 或 quarter。"),
    ] = "annual",
    limit: Annotated[
        int,
        typer.Option(help="每个 ticker 拉取的历史记录数，至少 3。"),
    ] = 5,
) -> None:
    """从 FMP 拉取 historical key-metrics/ratios，回填估值分位历史。"""
    fetch_date = _parse_date(as_of) if as_of else date.today()
    selected_tickers = (
        _parse_csv_items(tickers)
        if tickers
        else load_universe().ai_chain.get("core_watchlist", [])
    )
    api_key = os.getenv(api_key_env)
    if not api_key:
        console.print(f"[red]未找到环境变量 {api_key_env}，已停止 FMP 历史估值拉取。[/red]")
        raise typer.Exit(code=1)

    fetch_report_output = output_path or default_fmp_historical_valuation_fetch_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        fetch_date,
    )
    validation_output = validation_report_path or default_valuation_validation_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        fetch_date,
    )

    try:
        fetch_report = fetch_fmp_historical_valuation_snapshots(
            selected_tickers,
            api_key,
            fetch_date,
            captured_at=fetch_date,
            period=period,
            limit=limit,
        )
    except ValueError as exc:
        console.print(f"[red]FMP 历史估值参数错误：{exc}[/red]")
        raise typer.Exit(code=1) from exc

    write_fmp_historical_valuation_fetch_report(fetch_report, fetch_report_output)
    status_style = (
        "green" if fetch_report.status == "PASS" else "yellow" if fetch_report.passed else "red"
    )
    console.print(
        f"[{status_style}]FMP 历史估值拉取状态："
        f"{fetch_report.status}[/{status_style}]"
    )
    console.print(f"拉取报告：{fetch_report_output}")
    console.print(
        f"请求标的：{', '.join(fetch_report.requested_tickers)}；"
        f"返回记录：{fetch_report.row_count}；生成历史快照：{fetch_report.imported_count}"
    )
    console.print(f"错误数：{fetch_report.error_count}；警告数：{fetch_report.warning_count}")
    if not fetch_report.passed:
        raise typer.Exit(code=1)

    raw_paths = write_fmp_historical_valuation_raw_payloads(
        fetch_report.raw_payloads,
        raw_output_dir,
    )
    written_paths = write_valuation_snapshots_as_yaml(fetch_report.snapshots, output_dir)
    validation_report = validate_valuation_snapshot_store(
        store=load_valuation_snapshot_store(output_dir),
        universe=load_universe(),
        watchlist=load_watchlist(),
        as_of=fetch_date,
    )
    write_valuation_validation_report(validation_report, validation_output)

    validation_style = (
        "green"
        if validation_report.status == "PASS"
        else "yellow"
        if validation_report.passed
        else "red"
    )
    console.print(f"写入原始历史 payload：{len(raw_paths)} 个文件 -> {raw_output_dir}")
    console.print(f"写入历史估值 YAML：{len(written_paths)} 个文件 -> {output_dir}")
    console.print(
        f"[{validation_style}]估值快照校验状态："
        f"{validation_report.status}[/{validation_style}]"
    )
    console.print(f"校验报告：{validation_output}")
    if not validation_report.passed:
        raise typer.Exit(code=1)


@valuation_app.command("fetch-eodhd-trends")
def fetch_eodhd_valuation_trends(
    tickers: Annotated[
        str | None,
        typer.Option(help="逗号分隔 ticker；未提供时使用 universe 的 AI core_watchlist。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option(help="写入 EODHD trend 合并估值快照 YAML 的目录。"),
    ] = PROJECT_ROOT / "data" / "external" / "valuation_snapshots",
    base_valuation_dir: Annotated[
        Path | None,
        typer.Option(help="读取基础估值快照的目录；默认与 output_dir 相同。"),
    ] = None,
    raw_output_dir: Annotated[
        Path,
        typer.Option(help="写入 EODHD Earnings Trends 原始 JSON 的目录。"),
    ] = DEFAULT_EODHD_EARNINGS_TRENDS_RAW_DIR,
    as_of: Annotated[
        str | None,
        typer.Option(help="拉取评估日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown EODHD trends 拉取报告输出路径。"),
    ] = None,
    validation_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 估值快照校验报告输出路径。"),
    ] = None,
    api_key_env: Annotated[
        str,
        typer.Option(help="读取 EODHD API key 的环境变量名。"),
    ] = "EODHD_API_KEY",
) -> None:
    """从 EODHD Earnings Trends 拉取当前 EPS trend，合并进估值快照。"""
    fetch_date = _parse_date(as_of) if as_of else date.today()
    selected_tickers = (
        _parse_csv_items(tickers)
        if tickers
        else load_universe().ai_chain.get("core_watchlist", [])
    )
    api_key = os.getenv(api_key_env)
    if not api_key:
        console.print(f"[red]未找到环境变量 {api_key_env}，已停止 EODHD trends 拉取。[/red]")
        raise typer.Exit(code=1)

    fetch_report_output = output_path or default_eodhd_earnings_trends_fetch_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        fetch_date,
    )
    validation_output = validation_report_path or default_valuation_validation_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        fetch_date,
    )
    base_input_dir = base_valuation_dir or output_dir

    try:
        fetch_report = fetch_eodhd_earnings_trend_snapshots(
            selected_tickers,
            api_key,
            fetch_date,
            base_valuation_dir=base_input_dir,
            captured_at=fetch_date,
        )
    except ValueError as exc:
        console.print(f"[red]EODHD trends 参数错误：{exc}[/red]")
        raise typer.Exit(code=1) from exc

    write_eodhd_earnings_trends_fetch_report(fetch_report, fetch_report_output)
    status_style = (
        "green" if fetch_report.status == "PASS" else "yellow" if fetch_report.passed else "red"
    )
    console.print(
        f"[{status_style}]EODHD trends 拉取状态："
        f"{fetch_report.status}[/{status_style}]"
    )
    console.print(f"拉取报告：{fetch_report_output}")
    console.print(
        f"请求标的：{', '.join(fetch_report.requested_tickers)}；"
        f"返回记录：{fetch_report.row_count}；生成合并快照：{fetch_report.imported_count}"
    )
    console.print(f"错误数：{fetch_report.error_count}；警告数：{fetch_report.warning_count}")
    if not fetch_report.passed:
        raise typer.Exit(code=1)

    raw_paths = write_eodhd_earnings_trends_raw_payload(
        fetch_report.raw_payload,
        raw_output_dir,
    )
    written_paths = write_valuation_snapshots_as_yaml(fetch_report.snapshots, output_dir)
    validation_report = validate_valuation_snapshot_store(
        store=load_valuation_snapshot_store(output_dir),
        universe=load_universe(),
        watchlist=load_watchlist(),
        as_of=fetch_date,
    )
    write_valuation_validation_report(validation_report, validation_output)

    validation_style = (
        "green"
        if validation_report.status == "PASS"
        else "yellow"
        if validation_report.passed
        else "red"
    )
    console.print(f"写入原始 trend payload：{len(raw_paths)} 个文件 -> {raw_output_dir}")
    console.print(f"写入合并估值 YAML：{len(written_paths)} 个文件 -> {output_dir}")
    console.print(
        f"[{validation_style}]估值快照校验状态："
        f"{validation_report.status}[/{validation_style}]"
    )
    console.print(f"校验报告：{validation_output}")
    if not validation_report.passed:
        raise typer.Exit(code=1)


@valuation_app.command("validate-fmp-history")
def validate_fmp_analyst_history_command(
    input_path: Annotated[
        Path,
        typer.Option(help="FMP analyst-estimates 原始历史快照目录或 JSON 文件。"),
    ] = DEFAULT_FMP_ANALYST_ESTIMATE_HISTORY_DIR,
    tickers: Annotated[
        str | None,
        typer.Option(help="逗号分隔 ticker；未提供时校验全部历史快照。"),
    ] = None,
    as_of: Annotated[
        str | None,
        typer.Option(help="校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown FMP analyst history 校验报告输出路径。"),
    ] = None,
    max_snapshot_age_days: Annotated[
        int,
        typer.Option(help="历史快照新鲜度警告阈值，单位天。"),
    ] = 7,
) -> None:
    """校验 FMP analyst-estimates 原始历史缓存。"""
    validation_date = _parse_date(as_of) if as_of else date.today()
    report_path = output_path or default_fmp_analyst_history_validation_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        validation_date,
    )
    selected_tickers = _parse_csv_items(tickers) if tickers else None
    try:
        report = validate_fmp_analyst_estimate_history(
            input_path,
            validation_date,
            tickers=selected_tickers,
            max_snapshot_age_days=max_snapshot_age_days,
        )
    except ValueError as exc:
        console.print(f"[red]FMP analyst history 参数错误：{exc}[/red]")
        raise typer.Exit(code=1) from exc
    write_fmp_analyst_history_validation_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]FMP analyst history 校验状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    console.print(
        f"快照数量：{report.snapshot_count}；覆盖标的：{report.ticker_count}；"
        f"记录数：{report.record_count}"
    )
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")
    if not report.passed:
        raise typer.Exit(code=1)


@valuation_app.command("import-csv")
def import_valuation_csv(
    input_path: Annotated[
        Path,
        typer.Option(help="估值、预期和拥挤度结构化 CSV 输入路径。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option(help="写入估值快照 YAML 的目录。"),
    ] = PROJECT_ROOT / "data" / "external" / "valuation_snapshots",
    as_of: Annotated[
        str | None,
        typer.Option(help="导入后校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown CSV 导入报告输出路径。"),
    ] = None,
    validation_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 估值快照校验报告输出路径。"),
    ] = None,
) -> None:
    """导入结构化估值 CSV，并写入可审计估值快照 YAML。"""
    import_date = _parse_date(as_of) if as_of else date.today()
    import_report_output = output_path or (
        PROJECT_ROOT / "outputs" / "reports" / f"valuation_import_{import_date}.md"
    )
    validation_output = validation_report_path or default_valuation_validation_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        import_date,
    )
    import_report = import_valuation_snapshots_from_csv(input_path)
    write_valuation_csv_import_report(import_report, import_report_output)

    status_style = (
        "green" if import_report.status == "PASS" else "yellow" if import_report.passed else "red"
    )
    console.print(f"[{status_style}]估值 CSV 导入状态：{import_report.status}[/{status_style}]")
    console.print(f"导入报告：{import_report_output}")
    console.print(
        f"CSV 行数：{import_report.row_count}；导入快照：{import_report.imported_count}"
    )
    console.print(f"错误数：{import_report.error_count}；警告数：{import_report.warning_count}")
    if not import_report.passed:
        raise typer.Exit(code=1)

    written_paths = write_valuation_snapshots_as_yaml(import_report.snapshots, output_dir)
    validation_report = validate_valuation_snapshot_store(
        store=load_valuation_snapshot_store(output_dir),
        universe=load_universe(),
        watchlist=load_watchlist(),
        as_of=import_date,
    )
    write_valuation_validation_report(validation_report, validation_output)

    validation_style = (
        "green"
        if validation_report.status == "PASS"
        else "yellow"
        if validation_report.passed
        else "red"
    )
    console.print(f"写入 YAML：{len(written_paths)} 个文件 -> {output_dir}")
    console.print(
        f"[{validation_style}]估值快照校验状态："
        f"{validation_report.status}[/{validation_style}]"
    )
    console.print(f"校验报告：{validation_output}")
    if not validation_report.passed:
        raise typer.Exit(code=1)


@valuation_app.command("list")
def list_valuations(
    input_path: Annotated[
        Path,
        typer.Option(help="估值快照 YAML 文件或目录路径。"),
    ] = PROJECT_ROOT / "data" / "external" / "valuation_snapshots",
) -> None:
    """列出估值、预期和拥挤度快照。"""
    store = load_valuation_snapshot_store(input_path)

    table = Table(title="估值与拥挤度快照")
    table.add_column("Snapshot")
    table.add_column("Ticker")
    table.add_column("日期")
    table.add_column("来源")
    table.add_column("估值分位")
    table.add_column("评估")
    table.add_column("文件")

    for loaded in sorted(store.loaded, key=lambda item: item.snapshot.snapshot_id):
        snapshot = loaded.snapshot
        percentile = (
            "n/a"
            if snapshot.valuation_percentile is None
            else f"{snapshot.valuation_percentile:.0f}"
        )
        table.add_row(
            snapshot.snapshot_id,
            snapshot.ticker,
            snapshot.as_of.isoformat(),
            _valuation_source_type_label(snapshot.source_type),
            percentile,
            _valuation_assessment_label(snapshot.overall_assessment),
            str(loaded.path),
        )

    console.print(table)
    if not store.loaded:
        console.print("未发现可读取的估值快照。")
    if store.load_errors:
        console.print(
            f"[red]存在 {len(store.load_errors)} 个加载错误，请运行 validate 查看。[/red]"
        )


@valuation_app.command("validate")
def validate_valuations(
    input_path: Annotated[
        Path,
        typer.Option(help="估值快照 YAML 文件或目录路径。"),
    ] = PROJECT_ROOT / "data" / "external" / "valuation_snapshots",
    as_of: Annotated[
        str | None,
        typer.Option(help="校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 估值校验报告输出路径。"),
    ] = None,
) -> None:
    """校验估值、预期和拥挤度快照。"""
    validation_date = _parse_date(as_of) if as_of else date.today()
    report_path = output_path or default_valuation_validation_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        validation_date,
    )
    report = validate_valuation_snapshot_store(
        store=load_valuation_snapshot_store(input_path),
        universe=load_universe(),
        watchlist=load_watchlist(),
        as_of=validation_date,
    )
    write_valuation_validation_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]估值快照校验状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    console.print(f"快照数量：{report.snapshot_count}；覆盖标的：{report.ticker_count}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")

    if not report.passed:
        raise typer.Exit(code=1)


@valuation_app.command("review")
def review_valuations(
    input_path: Annotated[
        Path,
        typer.Option(help="估值快照 YAML 文件或目录路径。"),
    ] = PROJECT_ROOT / "data" / "external" / "valuation_snapshots",
    as_of: Annotated[
        str | None,
        typer.Option(help="复核日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 估值复核报告输出路径。"),
    ] = None,
) -> None:
    """复核估值、预期和拥挤度快照。"""
    review_date = _parse_date(as_of) if as_of else date.today()
    report_path = output_path or default_valuation_review_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        review_date,
    )
    validation_report = validate_valuation_snapshot_store(
        store=load_valuation_snapshot_store(input_path),
        universe=load_universe(),
        watchlist=load_watchlist(),
        as_of=review_date,
    )
    review_report = build_valuation_review_report(validation_report)
    write_valuation_review_report(review_report, report_path)

    status_style = "green" if review_report.status == "PASS" else (
        "yellow" if validation_report.passed else "red"
    )
    console.print(f"[{status_style}]估值复核状态：{review_report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    console.print(
        f"快照数量：{validation_report.snapshot_count}；"
        f"覆盖标的：{validation_report.ticker_count}"
    )
    console.print(
        f"校验错误数：{validation_report.error_count}；"
        f"校验警告数：{validation_report.warning_count}"
    )

    if not validation_report.passed:
        raise typer.Exit(code=1)


@app.command("review-trades")
def review_trades(
    prices_path: Annotated[
        Path,
        typer.Option(help="标准化日线价格 CSV 路径。"),
    ] = PROJECT_ROOT / "data" / "raw" / "prices_daily.csv",
    rates_path: Annotated[
        Path,
        typer.Option(help="标准化日线利率 CSV 路径。"),
    ] = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv",
    input_path: Annotated[
        Path,
        typer.Option(help="交易记录 YAML 文件或目录路径。"),
    ] = PROJECT_ROOT / "data" / "external" / "trades",
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

    status_style = "green" if review_report.status == "PASS" else (
        "yellow" if validation_report.passed else "red"
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
    ] = PROJECT_ROOT / "data" / "raw" / "prices_daily.csv",
    rates_path: Annotated[
        Path,
        typer.Option(help="标准化日线利率 CSV 路径。"),
    ] = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv",
    as_of: Annotated[
        str | None,
        typer.Option(help="特征日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path,
        typer.Option(help="特征 CSV 输出路径。"),
    ] = PROJECT_ROOT / "data" / "processed" / "features_daily.csv",
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


@app.command("score-daily")
def score_daily(
    prices_path: Annotated[
        Path,
        typer.Option(help="标准化日线价格 CSV 路径。"),
    ] = PROJECT_ROOT / "data" / "raw" / "prices_daily.csv",
    rates_path: Annotated[
        Path,
        typer.Option(help="标准化日线利率 CSV 路径。"),
    ] = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv",
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
    ] = PROJECT_ROOT / "data" / "processed" / "features_daily.csv",
    scores_path: Annotated[
        Path,
        typer.Option(help="评分 CSV 输出路径。"),
    ] = PROJECT_ROOT / "data" / "processed" / "scores_daily.csv",
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
            help=(
                "TSMC IR 季度指标 CSV；存在时日报会按 as-of 合并到 "
                "SEC-style 指标后再校验。"
            )
        ),
    ] = PROJECT_ROOT / "data" / "processed" / "tsm_ir_quarterly_metrics.csv",
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
    thesis_path: Annotated[
        Path,
        typer.Option(help="交易 thesis YAML 文件或目录路径，用于写入日报复核摘要。"),
    ] = PROJECT_ROOT / "data" / "external" / "trade_theses",
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
        typer.Option(
            help="覆盖 profile 中日报前 OpenAI 风险事件预审最多处理的官方候选数。"
        ),
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
        typer.Option(
            help="覆盖 profile 中完全相同日报前 OpenAI 请求的本地缓存复用时长。"
        ),
    ] = None,
    catalyst_calendar_path: Annotated[
        Path,
        typer.Option(help="未来催化剂日历 YAML 路径，用于日报告警摘要。"),
    ] = DEFAULT_CATALYST_CALENDAR_CONFIG_PATH,
    valuation_path: Annotated[
        Path,
        typer.Option(help="估值快照 YAML 文件或目录路径，用于写入日报复核摘要。"),
    ] = PROJECT_ROOT / "data" / "external" / "valuation_snapshots",
    trades_path: Annotated[
        Path,
        typer.Option(help="交易记录 YAML 文件或目录路径，用于写入日报复盘摘要。"),
    ] = PROJECT_ROOT / "data" / "external" / "trades",
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
    score_date = _parse_date(as_of) if as_of else resolve_daily_ops_default_as_of(
        score_started_at
    )
    production_score_date = resolve_daily_ops_default_as_of(score_started_at)
    benchmark_tickers = tuple(_parse_csv_items(review_benchmarks))
    if not benchmark_tickers:
        raise typer.BadParameter("日报交易复盘至少需要一个归因基准标的。")
    if prediction_production_effect not in {"production", "none"}:
        raise typer.BadParameter("prediction_production_effect 只能是 production 或 none。")
    if (
        prediction_candidate_id != "production"
        and prediction_production_effect == "production"
    ):
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
    daily_trace_output = trace_bundle_path or default_report_trace_bundle_path(
        score_report_output
    )
    decision_snapshot_output = decision_snapshot_path or default_decision_snapshot_path(
        DEFAULT_DECISION_SNAPSHOT_DIR,
        score_date,
    )
    belief_state_output = belief_state_path or default_belief_state_path(
        DEFAULT_BELIEF_STATE_DIR,
        score_date,
    )
    belief_state_history_output = (
        belief_state_history_path or DEFAULT_BELIEF_STATE_HISTORY_PATH
    )
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
            "sec_fundamental_features",
            "valuation_snapshots",
            "risk_event_occurrences",
        ),
        source_checks=_market_feature_source_checks(
            prices_frame=prices_frame,
            rates_frame=rates_frame,
            prices_path=prices_path,
            rates_path=rates_path,
            decision_time=score_date,
        ),
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
    if not sec_fundamental_feature_report.passed:
        write_sec_fundamental_features_report(
            report=sec_fundamental_feature_report,
            validation_report_path=sec_metrics_validation_output,
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
        validation_report_path=sec_metrics_validation_output,
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
            raise typer.BadParameter(
                "LLM request profile 必须设置 max_candidates，或显式传入。"
            )
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
            _parse_csv_items(official_policy_source_ids)
            if official_policy_source_ids
            else None
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
        risk_event_prereview_report = (
            run_openai_risk_event_prereview_for_official_candidates(
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
                generated_at=(
                    score_started_at if score_date == production_score_date else None
                ),
                request_visibility_cutoff=(
                    score_started_at if score_date == production_score_date else None
                ),
                max_candidates=effective_risk_event_openai_precheck_max_candidates,
            )
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
    focus_stock_trend_section = render_focus_stock_trend_section(
        focus_stock_trend_report
    )
    industry_node_heat_report = build_industry_node_heat_report(
        industry_chain=industry_chain,
        watchlist=watchlist,
        feature_set=feature_set,
        fundamental_feature_report=sec_fundamental_feature_report,
        valuation_review_report=valuation_review_report,
        risk_event_occurrence_review_report=risk_event_occurrence_review_report,
        thesis_review_report=thesis_review_report,
    )
    industry_node_heat_section = render_industry_node_heat_section(
        industry_node_heat_report
    )
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
                risk_event_openai_precheck_report_output=(
                    risk_event_openai_precheck_report_output
                ),
                risk_event_prereview_queue_path=risk_event_prereview_queue_path,
                llm_formal_report=llm_formal_report,
                risk_event_llm_formal_report_output=(
                    risk_event_llm_formal_report_output
                ),
                llm_profile_id=llm_profile.profile_id,
                llm_formal_enabled=effective_risk_event_llm_formal_assessment,
                model=effective_openai_model,
                reasoning_effort=effective_openai_reasoning_effort,
                timeout_seconds=effective_openai_timeout_seconds,
                http_client=effective_openai_http_client,
                cache_dir=openai_cache_dir,
                cache_ttl_hours=effective_openai_cache_ttl_hours,
                max_candidates=effective_risk_event_openai_precheck_max_candidates,
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
        f"告警状态：{daily_alert_report.status}"
        f"（{len(daily_alert_report.alerts)} 条）"
    )
    console.print(f"每日评分报告：{daily_report_output}")
    console.print(f"告警报告：{daily_alert_output}")
    console.print(f"Evidence bundle：{daily_trace_output}")
    console.print(f"Decision snapshot：{daily_decision_snapshot_output}")
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
        f"组合暴露报告：{portfolio_exposure_output}"
        f"（{portfolio_exposure_report.status}）"
    )
    console.print(
        f"执行政策报告：{execution_policy_report_output}"
        f"（{execution_policy_report.status}）"
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
) -> str:
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
    warning_count = (
        rules_validation_report.warning_count + occurrence_validation.warning_count
    )
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
    module_token = "".join(
        char if char.isalnum() else "_" for char in module_name.lower()
    ).strip("_")
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
                f"错误数：{metrics_report.error_count}；"
                f"警告数：{metrics_report.warning_count}"
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
                console.print(
                    "[red]TSMC IR point-in-time 指标合并失败，已停止回测。[/red]"
                )
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
                f"错误数：{feature_report.error_count}；"
                f"警告数：{feature_report.warning_count}"
            )
            raise typer.Exit(code=1)
        reports[signal_date] = feature_report
    return reports


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
            console.print(
                "[red]point-in-time 风险事件发生记录校验失败，已停止回测。[/red]"
            )
            console.print(f"失败日期：{signal_date.isoformat()}")
            console.print(
                f"错误数：{report.validation_report.error_count}；"
                f"警告数：{report.validation_report.warning_count}"
            )
            raise typer.Exit(code=1)
        reports[signal_date] = report
    return reports


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter("日期必须使用 YYYY-MM-DD 格式。") from exc


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


def _risk_level_label(level: str) -> str:
    return {
        "low": "低",
        "medium": "中",
        "high": "高",
        "critical": "极高",
    }.get(level, level)


def _decision_stage_label(stage: str) -> str:
    return {
        "watch_only": "仅观察",
        "active_trade": "主动交易",
    }.get(stage, stage)


def _horizon_label(value: str) -> str:
    return {
        "short": "短期",
        "medium": "中期",
        "long": "长期",
    }.get(value, value)


def _relevance_label(value: str) -> str:
    return {
        "low": "低",
        "medium": "中",
        "high": "高",
    }.get(value, value)


def _thesis_direction_label(value: str) -> str:
    return {
        "long": "做多",
        "short": "做空",
        "hedge": "对冲",
        "watch": "观察",
    }.get(value, value)


def _thesis_status_label(value: str) -> str:
    return {
        "draft": "草稿",
        "active": "活跃",
        "warning": "警告",
        "challenged": "受挑战",
        "paused": "暂停",
        "closed": "已关闭",
        "invalidated": "已证伪",
    }.get(value, value)


def _thesis_review_frequency_label(value: str) -> str:
    return {
        "daily": "每日",
        "weekly": "每周",
        "monthly": "每月",
        "quarterly": "每季",
        "event_driven": "事件驱动",
    }.get(value, value)


def _valuation_source_type_label(value: str) -> str:
    return {
        "primary_filing": "一手披露",
        "paid_vendor": "付费供应商",
        "manual_input": "手工录入",
        "public_convenience": "公开便利源",
    }.get(value, value)


def _valuation_assessment_label(value: str) -> str:
    return {
        "cheap": "偏便宜",
        "reasonable": "合理",
        "expensive": "偏贵",
        "extreme": "极端",
        "unknown": "未知",
    }.get(value, value)


def _data_source_type_label(value: str) -> str:
    return {
        "primary_source": "一手来源",
        "paid_vendor": "付费供应商",
        "public_convenience": "公开便利源",
        "manual_input": "手工输入",
    }.get(value, value)


def _data_source_status_label(value: str) -> str:
    return {
        "active": "已启用",
        "planned": "计划中",
        "inactive": "停用",
    }.get(value, value)


if __name__ == "__main__":
    app()
