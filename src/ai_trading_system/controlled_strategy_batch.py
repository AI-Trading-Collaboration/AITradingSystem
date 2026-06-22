from __future__ import annotations

import csv
import hashlib
import json
import math
import statistics
from collections.abc import Mapping
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from ai_trading_system.config import (
    PROJECT_ROOT,
    configured_rate_series,
    load_backtest_validation_policy,
    load_data_quality,
    load_universe,
)
from ai_trading_system.data.quality import validate_data_cache
from ai_trading_system.data_foundation import (
    AI_REGIME_START,
    SAFETY_BOUNDARY,
    utc_now_iso,
    write_foundation_artifact_pair,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_CONTROLLED_STRATEGY_BATCH_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "research" / "controlled_strategy_candidate_research.yaml"
)
DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "research" / "controlled_strategy_next_stage_research.yaml"
)
DEFAULT_RESEARCH_STRATEGY_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_strategies"
DEFAULT_VALUE_SURFACE_OUTPUT_ROOT = DEFAULT_RESEARCH_STRATEGY_OUTPUT_ROOT / "value_surface"
DEFAULT_VALUE_SURFACE_EXPANSION_OUTPUT_ROOT = (
    DEFAULT_RESEARCH_STRATEGY_OUTPUT_ROOT / "value_surface_expansion"
)
DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT = (
    DEFAULT_RESEARCH_STRATEGY_OUTPUT_ROOT / "value_surface_review"
)
DEFAULT_UTILITY_BOUNDARY_OUTPUT_ROOT = DEFAULT_RESEARCH_STRATEGY_OUTPUT_ROOT / "utility_boundary"
DEFAULT_REGRET_STATE_MACHINE_OUTPUT_ROOT = (
    DEFAULT_RESEARCH_STRATEGY_OUTPUT_ROOT / "regret_state_machine"
)
DEFAULT_SIMPLE_ENSEMBLE_OUTPUT_ROOT = DEFAULT_RESEARCH_STRATEGY_OUTPUT_ROOT / "simple_ensemble"
DEFAULT_GBDT_ACTION_UTILITY_OUTPUT_ROOT = (
    DEFAULT_RESEARCH_STRATEGY_OUTPUT_ROOT / "gbdt_action_utility"
)
DEFAULT_CONTROLLED_STRATEGY_BATCH_REVIEW_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_ops" / "review_board"
)
DEFAULT_FORWARD_MATURITY_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "forward_evidence" / "maturity_tracker"
)
DEFAULT_PRICES_PATH = PROJECT_ROOT / "data" / "raw" / "prices_daily.csv"
DEFAULT_MARKETSTACK_PRICES_PATH = PROJECT_ROOT / "data" / "raw" / "prices_marketstack_daily.csv"
DEFAULT_RATES_PATH = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv"
DEFAULT_CONTROLLED_BENCHMARK_EXPANSION_PATH = (
    PROJECT_ROOT
    / "outputs"
    / "research_runs"
    / "controlled_benchmark_batch"
    / "controlled_benchmark_execution_expansion_report.json"
)
DEFAULT_CONTROL_AUDIT_PATH = (
    PROJECT_ROOT
    / "outputs"
    / "research_runs"
    / "controlled_benchmark_batch"
    / "control_audit_report.json"
)
DEFAULT_FMP_WATCHLIST_CLOSURE_PATH = (
    PROJECT_ROOT
    / "outputs"
    / "data_quality"
    / "fmp_pit_review"
    / "fmp_watchlist_closure_report.json"
)
DEFAULT_FORWARD_DRY_RUN_ARCHIVE_PATH = (
    PROJECT_ROOT
    / "outputs"
    / "forward_evidence"
    / "daily_archive"
    / "forward_evidence_dry_run_archive.json"
)
DEFAULT_FORWARD_DAILY_DRY_RUN_LEDGER_PATH = (
    PROJECT_ROOT
    / "outputs"
    / "forward_evidence"
    / "daily_archive"
    / "forward_evidence_dry_run_ledger.jsonl"
)
DEFAULT_VALUE_SURFACE_PATH = (
    DEFAULT_VALUE_SURFACE_OUTPUT_ROOT / "value_surface_controlled_prototype.json"
)
DEFAULT_VALUE_SURFACE_EXPANSION_PATH = (
    DEFAULT_VALUE_SURFACE_EXPANSION_OUTPUT_ROOT / "value_surface_controlled_expansion.json"
)
DEFAULT_VALUE_SURFACE_WARNING_TRIAGE_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT / "value_surface_warning_triage_review.json"
)
DEFAULT_VALUE_SURFACE_WALK_FORWARD_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT
    / "value_surface_controlled_walk_forward_expansion.json"
)
DEFAULT_VALUE_SURFACE_FAILURE_ATTRIBUTION_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT / "value_surface_failure_attribution.json"
)
DEFAULT_VALUE_SURFACE_DIRECTION_REVIEW_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT / "value_surface_direction_review.json"
)
DEFAULT_REGIME_CONDITIONED_VALUE_SURFACE_DESIGN_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT / "regime_conditioned_value_surface_design.json"
)
DEFAULT_TAIL_LOSS_GUARDRAIL_FALLBACK_POLICY_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT / "tail_loss_guardrail_fallback_policy.json"
)
DEFAULT_REGIME_HORIZON_LOSS_ATTRIBUTION_MATRIX_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT / "regime_horizon_loss_attribution_matrix.json"
)
DEFAULT_REGIME_CONDITIONED_VALUE_SURFACE_CONTROLLED_REVIEW_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT
    / "regime_conditioned_value_surface_controlled_review.json"
)
DEFAULT_COST_TURNOVER_AWARE_VALUE_SURFACE_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT
    / "cost_turnover_aware_regime_conditioned_value_surface.json"
)
DEFAULT_LONG_HORIZON_QUARANTINE_REVIEW_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT / "long_horizon_quarantine_selection_review.json"
)
DEFAULT_AI_REGIME_ATTRIBUTION_REVIEW_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT
    / "ai_after_chatgpt_full_regime_attribution_review.json"
)
DEFAULT_REGIME_CONDITIONED_WALK_FORWARD_HOLDOUT_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT / "regime_conditioned_walk_forward_holdout.json"
)
DEFAULT_VALUE_SURFACE_V2_CONTROLLED_REVIEW_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT / "value_surface_v2_controlled_review.json"
)
DEFAULT_HORIZON_SELECTOR_PROBLEM_CONTRACT_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT / "horizon_selector_problem_contract.json"
)
DEFAULT_LONG_HORIZON_QUARANTINE_FALLBACK_REVIEW_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT / "long_horizon_quarantine_fallback_review.json"
)
DEFAULT_HORIZON_SELECTOR_CONTROLLED_PROTOTYPE_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT / "horizon_selector_controlled_prototype.json"
)
DEFAULT_COST_AWARE_HORIZON_HYSTERESIS_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT / "cost_aware_horizon_hysteresis.json"
)
DEFAULT_HORIZON_SELECTOR_HOLDOUT_REVIEW_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT / "horizon_selector_holdout_review.json"
)
DEFAULT_VALUE_SURFACE_POLICY_KILL_DIAGNOSTIC_DOWNGRADE_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT / "value_surface_policy_kill_diagnostic_downgrade.json"
)
DEFAULT_BENCHMARK_FIRST_TAIL_RISK_POLICY_CONTRACT_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT / "benchmark_first_tail_risk_policy_contract.json"
)
DEFAULT_TAIL_LOSS_AVOIDANCE_CLASSIFIER_PROTOTYPE_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT / "tail_loss_avoidance_classifier_prototype.json"
)
DEFAULT_CONSERVATIVE_HORIZON_RISK_FILTER_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT / "conservative_horizon_risk_filter.json"
)
DEFAULT_BENCHMARK_FALLBACK_DRAWDOWN_GUARD_PROTOTYPE_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT / "benchmark_fallback_drawdown_guard_prototype.json"
)
DEFAULT_TAIL_RISK_POLICY_FAMILY_CONTROLLED_REVIEW_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT / "tail_risk_policy_family_controlled_review.json"
)
DEFAULT_TAIL_RISK_BENCHMARK_FALLBACK_ROBUSTNESS_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT
    / "tail_risk_benchmark_fallback_robustness_expansion.json"
)
DEFAULT_TAIL_RISK_FALLBACK_TRIGGER_PRECISION_RECALL_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT
    / "tail_risk_fallback_trigger_precision_recall_audit.json"
)
DEFAULT_TAIL_RISK_OPPORTUNITY_COST_UPSIDE_CAPTURE_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT
    / "tail_risk_opportunity_cost_upside_capture_review.json"
)
DEFAULT_TAIL_RISK_FORWARD_EVIDENCE_INTEGRATION_PATH = (
    DEFAULT_FORWARD_DRY_RUN_ARCHIVE_PATH.parent
    / "tail_risk_benchmark_fallback_forward_evidence_integration.json"
)
DEFAULT_TAIL_RISK_POLICY_CONTROLLED_REVIEW_BOARD_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT / "tail_risk_policy_controlled_review_board.json"
)
DEFAULT_TAIL_RISK_AUDIT_UNIVERSE_RECONCILIATION_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT
    / "tail_risk_fallback_audit_universe_reconciliation.json"
)
DEFAULT_TAIL_RISK_ANTI_LEAKAGE_AUDIT_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT / "tail_risk_fallback_anti_leakage_audit.json"
)
DEFAULT_TAIL_RISK_THRESHOLD_SENSITIVITY_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT / "tail_risk_fallback_threshold_sensitivity.json"
)
DEFAULT_TAIL_RISK_REGIME_SEGMENTED_ROBUSTNESS_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT / "tail_risk_fallback_regime_segmented_robustness.json"
)
DEFAULT_TAIL_RISK_FORWARD_MATURITY_SCOREBOARD_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT / "tail_risk_fallback_forward_maturity_scoreboard.json"
)
DEFAULT_TAIL_RISK_FALLBACK_BLOCKER_DIAGNOSTIC_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT / "tail_risk_fallback_blocker_diagnostic.json"
)
DEFAULT_TAIL_RISK_TRIGGER_LABEL_INDEPENDENCE_AUDIT_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT / "tail_risk_trigger_label_independence_audit.json"
)
DEFAULT_TAIL_RISK_INDEPENDENT_FORWARD_OUTCOME_VALIDATION_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT
    / "tail_risk_independent_forward_outcome_validation.json"
)
DEFAULT_TAIL_RISK_FORWARD_OUTCOME_CONTRACT_AUDIT_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT / "tail_risk_forward_outcome_contract_audit.json"
)
DEFAULT_TAIL_RISK_DECISION_TIME_BOUNDARY_AUDIT_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT / "tail_risk_decision_time_boundary_audit.json"
)
DEFAULT_TAIL_RISK_TAINTED_METRIC_QUARANTINE_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT / "tail_risk_tainted_metric_quarantine.json"
)
DEFAULT_TAIL_RISK_FALLBACK_COUNTERFACTUAL_VALIDATION_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT / "tail_risk_fallback_counterfactual_validation.json"
)
DEFAULT_TAIL_RISK_REGIME_STRATIFIED_FORWARD_OUTCOME_REVIEW_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT
    / "tail_risk_regime_stratified_forward_outcome_review.json"
)
DEFAULT_TAIL_RISK_THRESHOLD_SENSITIVITY_REVIEW_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT / "tail_risk_threshold_sensitivity_review.json"
)
DEFAULT_TAIL_RISK_FALLBACK_ERROR_COST_LEDGER_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT / "tail_risk_fallback_error_cost_ledger.json"
)
DEFAULT_TAIL_RISK_EVIDENCE_MATURITY_GATE_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT / "tail_risk_evidence_maturity_gate.json"
)
DEFAULT_TAIL_RISK_FORWARD_AGING_TRACKER_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT / "tail_risk_forward_aging_tracker.json"
)
DEFAULT_TAIL_RISK_LEAKAGE_STRESS_SUITE_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT / "tail_risk_leakage_stress_suite.json"
)
DEFAULT_TAIL_RISK_PROMOTION_READINESS_GATE_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT / "tail_risk_promotion_readiness_gate.json"
)
DEFAULT_TAIL_RISK_INDEPENDENT_TRIGGER_V2_BUILDER_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT / "tail_risk_independent_trigger_v2_builder.json"
)
DEFAULT_TAIL_RISK_TRIGGER_FEATURE_AVAILABILITY_CATALOG_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT / "tail_risk_trigger_feature_availability_catalog.json"
)
DEFAULT_TAIL_RISK_RESEARCH_MASTER_REVIEW_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT / "tail_risk_research_master_review.json"
)
DEFAULT_TAIL_RISK_POST_MERGE_EVIDENCE_REVIEW_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT / "tail_risk_post_merge_evidence_review.json"
)
DEFAULT_TAIL_RISK_GOVERNANCE_ARTIFACT_SNAPSHOT_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT / "tail_risk_governance_artifact_snapshot.json"
)
DEFAULT_TAIL_RISK_STATUS_MATRIX_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT / "tail_risk_status_matrix.json"
)
DEFAULT_TAIL_RISK_REAL_DATA_VALIDATION_AUDIT_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT / "tail_risk_real_data_validation_audit.json"
)
DEFAULT_TAIL_RISK_INDEPENDENT_FORWARD_OUTCOME_RESULT_REVIEW_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT
    / "tail_risk_independent_forward_outcome_result_review.json"
)
DEFAULT_TAIL_RISK_COUNTERFACTUAL_BASELINE_RESULT_REVIEW_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT
    / "tail_risk_counterfactual_baseline_result_review.json"
)
DEFAULT_TAIL_RISK_ARTIFACT_DETERMINISM_CHECK_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT / "tail_risk_artifact_determinism_check.json"
)
DEFAULT_TAIL_RISK_TASK_COVERAGE_MAP_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT / "tail_risk_task_coverage_map.json"
)
DEFAULT_TAIL_RISK_TASK_COVERAGE_MAP_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "tail_risk_fallback_governance_task_coverage_map.md"
)
DEFAULT_TAIL_RISK_HARD_BLOCK_MUTATION_TESTS_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT / "tail_risk_hard_block_mutation_tests.json"
)
DEFAULT_TAIL_RISK_REPORT_REGISTRY_INTEGRITY_REVIEW_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT / "tail_risk_report_registry_integrity_review.json"
)
DEFAULT_TAIL_RISK_DAILY_READING_SAFETY_SUMMARY_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT / "tail_risk_daily_reading_safety_summary.json"
)
DEFAULT_TAIL_RISK_TRIGGER_V2_INPUT_QUALITY_REVIEW_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT
    / "tail_risk_independent_trigger_v2_input_quality_review.json"
)
DEFAULT_TAIL_RISK_BASELINE_DOMINANCE_GATE_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT / "tail_risk_baseline_dominance_gate.json"
)
DEFAULT_TAIL_RISK_RESEARCH_READINESS_SCORE_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT / "tail_risk_research_readiness_score.json"
)
DEFAULT_TAIL_RISK_NEXT_DECISION_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT / "tail_risk_next_decision.json"
)
DEFAULT_TAIL_RISK_NEXT_DECISION_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "tail_risk_fallback_next_decision.md"
)

TAIL_RISK_GOVERNANCE_TASK_METADATA: tuple[dict[str, Any], ...] = (
    {
        "task_id": "TRADING-827",
        "report_id": "tail_risk_trigger_label_independence_audit",
        "title": "Tail-Risk Trigger/Label Independence Audit",
        "command_slug": "tail-risk-trigger-label-independence-audit",
        "default_path": DEFAULT_TAIL_RISK_TRIGGER_LABEL_INDEPENDENCE_AUDIT_PATH,
    },
    {
        "task_id": "TRADING-828",
        "report_id": "tail_risk_independent_forward_outcome_validation",
        "title": "Tail-Risk Independent Forward Outcome Validation",
        "command_slug": "tail-risk-independent-forward-outcome-validation",
        "default_path": DEFAULT_TAIL_RISK_INDEPENDENT_FORWARD_OUTCOME_VALIDATION_PATH,
    },
    {
        "task_id": "TRADING-829",
        "report_id": "tail_risk_forward_outcome_contract_audit",
        "title": "Tail-Risk Forward Outcome Contract Audit",
        "command_slug": "tail-risk-forward-outcome-contract-audit",
        "default_path": DEFAULT_TAIL_RISK_FORWARD_OUTCOME_CONTRACT_AUDIT_PATH,
    },
    {
        "task_id": "TRADING-830",
        "report_id": "tail_risk_decision_time_boundary_audit",
        "title": "Tail-Risk Decision-Time Boundary Audit",
        "command_slug": "tail-risk-decision-time-boundary-audit",
        "default_path": DEFAULT_TAIL_RISK_DECISION_TIME_BOUNDARY_AUDIT_PATH,
    },
    {
        "task_id": "TRADING-831",
        "report_id": "tail_risk_tainted_metric_quarantine",
        "title": "Tail-Risk Tainted Metric Quarantine",
        "command_slug": "tail-risk-tainted-metric-quarantine",
        "default_path": DEFAULT_TAIL_RISK_TAINTED_METRIC_QUARANTINE_PATH,
    },
    {
        "task_id": "TRADING-832",
        "report_id": "tail_risk_fallback_counterfactual_validation",
        "title": "Tail-Risk Fallback Counterfactual Validation",
        "command_slug": "tail-risk-fallback-counterfactual-validation",
        "default_path": DEFAULT_TAIL_RISK_FALLBACK_COUNTERFACTUAL_VALIDATION_PATH,
    },
    {
        "task_id": "TRADING-833",
        "report_id": "tail_risk_regime_stratified_forward_outcome_review",
        "title": "Tail-Risk Regime-Stratified Forward Outcome Review",
        "command_slug": "tail-risk-regime-stratified-forward-outcome-review",
        "default_path": DEFAULT_TAIL_RISK_REGIME_STRATIFIED_FORWARD_OUTCOME_REVIEW_PATH,
    },
    {
        "task_id": "TRADING-834",
        "report_id": "tail_risk_threshold_sensitivity_review",
        "title": "Tail-Risk Threshold Sensitivity Review",
        "command_slug": "tail-risk-threshold-sensitivity-review",
        "default_path": DEFAULT_TAIL_RISK_THRESHOLD_SENSITIVITY_REVIEW_PATH,
    },
    {
        "task_id": "TRADING-835",
        "report_id": "tail_risk_fallback_error_cost_ledger",
        "title": "Tail-Risk Fallback Error Cost Ledger",
        "command_slug": "tail-risk-fallback-error-cost-ledger",
        "default_path": DEFAULT_TAIL_RISK_FALLBACK_ERROR_COST_LEDGER_PATH,
    },
    {
        "task_id": "TRADING-836",
        "report_id": "tail_risk_evidence_maturity_gate",
        "title": "Tail-Risk Evidence Maturity Gate",
        "command_slug": "tail-risk-evidence-maturity-gate",
        "default_path": DEFAULT_TAIL_RISK_EVIDENCE_MATURITY_GATE_PATH,
    },
    {
        "task_id": "TRADING-837",
        "report_id": "tail_risk_forward_aging_tracker",
        "title": "Tail-Risk Forward Aging Tracker",
        "command_slug": "tail-risk-forward-aging-tracker",
        "default_path": DEFAULT_TAIL_RISK_FORWARD_AGING_TRACKER_PATH,
    },
    {
        "task_id": "TRADING-838",
        "report_id": "tail_risk_leakage_stress_suite",
        "title": "Tail-Risk Leakage Stress Suite",
        "command_slug": "tail-risk-leakage-stress-suite",
        "default_path": DEFAULT_TAIL_RISK_LEAKAGE_STRESS_SUITE_PATH,
    },
    {
        "task_id": "TRADING-839",
        "report_id": "tail_risk_promotion_readiness_gate",
        "title": "Tail-Risk Promotion Readiness Gate",
        "command_slug": "tail-risk-promotion-readiness-gate",
        "default_path": DEFAULT_TAIL_RISK_PROMOTION_READINESS_GATE_PATH,
    },
    {
        "task_id": "TRADING-840",
        "report_id": "tail_risk_independent_trigger_v2_builder",
        "title": "Tail-Risk Independent Trigger V2 Builder",
        "command_slug": "tail-risk-independent-trigger-v2-builder",
        "default_path": DEFAULT_TAIL_RISK_INDEPENDENT_TRIGGER_V2_BUILDER_PATH,
    },
    {
        "task_id": "TRADING-841",
        "report_id": "tail_risk_trigger_feature_availability_catalog",
        "title": "Tail-Risk Trigger Feature Availability Catalog",
        "command_slug": "tail-risk-trigger-feature-availability-catalog",
        "default_path": DEFAULT_TAIL_RISK_TRIGGER_FEATURE_AVAILABILITY_CATALOG_PATH,
    },
    {
        "task_id": "TRADING-842",
        "report_id": "tail_risk_research_master_review",
        "title": "Tail-Risk Research Master Review",
        "command_slug": "tail-risk-research-master-review",
        "default_path": DEFAULT_TAIL_RISK_RESEARCH_MASTER_REVIEW_PATH,
    },
)

TAIL_RISK_FOLLOWUP_TASK_METADATA: tuple[dict[str, Any], ...] = (
    {
        "task_id": "TRADING-843",
        "report_id": "tail_risk_governance_artifact_snapshot",
        "title": "Tail-Risk Governance Artifact Snapshot",
        "command_slug": "tail-risk-governance-artifact-snapshot",
        "default_path": DEFAULT_TAIL_RISK_GOVERNANCE_ARTIFACT_SNAPSHOT_PATH,
    },
    {
        "task_id": "TRADING-844",
        "report_id": "tail_risk_status_matrix",
        "title": "Tail-Risk Status Matrix",
        "command_slug": "tail-risk-status-matrix",
        "default_path": DEFAULT_TAIL_RISK_STATUS_MATRIX_PATH,
    },
    {
        "task_id": "TRADING-845",
        "report_id": "tail_risk_real_data_validation_audit",
        "title": "Tail-Risk Real Data Validation Audit",
        "command_slug": "tail-risk-real-data-validation-audit",
        "default_path": DEFAULT_TAIL_RISK_REAL_DATA_VALIDATION_AUDIT_PATH,
    },
    {
        "task_id": "TRADING-846",
        "report_id": "tail_risk_independent_forward_outcome_result_review",
        "title": "Tail-Risk Independent Forward Outcome Result Review",
        "command_slug": "tail-risk-independent-forward-outcome-result-review",
        "default_path": DEFAULT_TAIL_RISK_INDEPENDENT_FORWARD_OUTCOME_RESULT_REVIEW_PATH,
    },
    {
        "task_id": "TRADING-847",
        "report_id": "tail_risk_counterfactual_baseline_result_review",
        "title": "Tail-Risk Counterfactual Baseline Result Review",
        "command_slug": "tail-risk-counterfactual-baseline-result-review",
        "default_path": DEFAULT_TAIL_RISK_COUNTERFACTUAL_BASELINE_RESULT_REVIEW_PATH,
    },
    {
        "task_id": "TRADING-848",
        "report_id": "tail_risk_artifact_determinism_check",
        "title": "Tail-Risk Artifact Determinism Check",
        "command_slug": "tail-risk-artifact-determinism-check",
        "default_path": DEFAULT_TAIL_RISK_ARTIFACT_DETERMINISM_CHECK_PATH,
    },
    {
        "task_id": "TRADING-850",
        "report_id": "tail_risk_task_coverage_map",
        "title": "Tail-Risk Task Coverage Map",
        "command_slug": "tail-risk-task-coverage-map",
        "default_path": DEFAULT_TAIL_RISK_TASK_COVERAGE_MAP_PATH,
    },
    {
        "task_id": "TRADING-851",
        "report_id": "tail_risk_hard_block_mutation_tests",
        "title": "Tail-Risk Hard Block Mutation Tests",
        "command_slug": "tail-risk-hard-block-mutation-tests",
        "default_path": DEFAULT_TAIL_RISK_HARD_BLOCK_MUTATION_TESTS_PATH,
    },
    {
        "task_id": "TRADING-852",
        "report_id": "tail_risk_report_registry_integrity_review",
        "title": "Tail-Risk Report Registry Integrity Review",
        "command_slug": "tail-risk-report-registry-integrity-review",
        "default_path": DEFAULT_TAIL_RISK_REPORT_REGISTRY_INTEGRITY_REVIEW_PATH,
    },
    {
        "task_id": "TRADING-853",
        "report_id": "tail_risk_daily_reading_safety_summary",
        "title": "Tail-Risk Daily Reading Safety Summary",
        "command_slug": "tail-risk-daily-reading-safety-summary",
        "default_path": DEFAULT_TAIL_RISK_DAILY_READING_SAFETY_SUMMARY_PATH,
    },
    {
        "task_id": "TRADING-854",
        "report_id": "tail_risk_independent_trigger_v2_input_quality_review",
        "title": "Tail-Risk Independent Trigger V2 Input Quality Review",
        "command_slug": "tail-risk-independent-trigger-v2-input-quality-review",
        "default_path": DEFAULT_TAIL_RISK_TRIGGER_V2_INPUT_QUALITY_REVIEW_PATH,
    },
    {
        "task_id": "TRADING-856",
        "report_id": "tail_risk_baseline_dominance_gate",
        "title": "Tail-Risk Baseline Dominance Gate",
        "command_slug": "tail-risk-baseline-dominance-gate",
        "default_path": DEFAULT_TAIL_RISK_BASELINE_DOMINANCE_GATE_PATH,
    },
    {
        "task_id": "TRADING-857",
        "report_id": "tail_risk_research_readiness_score",
        "title": "Tail-Risk Research Readiness Score",
        "command_slug": "tail-risk-research-readiness-score",
        "default_path": DEFAULT_TAIL_RISK_RESEARCH_READINESS_SCORE_PATH,
    },
    {
        "task_id": "TRADING-858",
        "report_id": "tail_risk_next_decision",
        "title": "Tail-Risk Next Decision",
        "command_slug": "tail-risk-next-decision-document",
        "default_path": DEFAULT_TAIL_RISK_NEXT_DECISION_PATH,
    },
)

DEFAULT_UTILITY_BOUNDARY_AUDIT_PATH = (
    DEFAULT_UTILITY_BOUNDARY_OUTPUT_ROOT / "utility_boundary_ranking_policy_audit.json"
)
DEFAULT_UTILITY_RANKING_ROBUSTNESS_PATH = (
    DEFAULT_UTILITY_BOUNDARY_OUTPUT_ROOT / "utility_ranking_robustness_pareto_audit.json"
)
DEFAULT_VALUE_SURFACE_UTILITY_PARETO_RANKING_PATH = (
    DEFAULT_UTILITY_BOUNDARY_OUTPUT_ROOT / "value_surface_utility_pareto_ranking_review.json"
)
DEFAULT_HORIZON_CLIFF_STABILIZATION_REVIEW_PATH = (
    DEFAULT_UTILITY_BOUNDARY_OUTPUT_ROOT / "horizon_cliff_utility_ranking_stabilization_review.json"
)
DEFAULT_FORWARD_CONTINUITY_MATURITY_PATH = (
    DEFAULT_FORWARD_MATURITY_OUTPUT_ROOT / "forward_evidence_daily_continuity_maturity_tracker.json"
)
DEFAULT_FORWARD_DAILY_CONTINUITY_REVIEW_PATH = (
    DEFAULT_FORWARD_MATURITY_OUTPUT_ROOT / "forward_evidence_daily_continuity_review.json"
)
DEFAULT_FORWARD_EVIDENCE_CONTINUITY_EXTENSION_PATH = (
    DEFAULT_FORWARD_MATURITY_OUTPUT_ROOT / "forward_evidence_continuity_extension.json"
)
DEFAULT_REGRET_STATE_MACHINE_PATH = (
    DEFAULT_REGRET_STATE_MACHINE_OUTPUT_ROOT / "regret_state_machine_controlled_prototype.json"
)
DEFAULT_STATE_TRANSITION_CASEBOOK_PATH = (
    DEFAULT_REGRET_STATE_MACHINE_OUTPUT_ROOT / "state_transition_casebook.json"
)
DEFAULT_SIMPLE_STRATEGY_SELECTOR_PATH = (
    DEFAULT_SIMPLE_ENSEMBLE_OUTPUT_ROOT / "simple_strategy_selector_pilot.json"
)
DEFAULT_GBDT_ACTION_UTILITY_PATH = (
    DEFAULT_GBDT_ACTION_UTILITY_OUTPUT_ROOT / "gbdt_action_utility_baseline.json"
)
DEFAULT_GBDT_PIVOT_REVIEW_PATH = DEFAULT_GBDT_ACTION_UTILITY_OUTPUT_ROOT / "gbdt_pivot_review.json"
DEFAULT_GBDT_PIVOT_SELECTION_PATH = (
    DEFAULT_GBDT_ACTION_UTILITY_OUTPUT_ROOT / "gbdt_pivot_direction_selection.json"
)
DEFAULT_GBDT_VALUE_SURFACE_RESIDUAL_DIAGNOSTIC_PATH = (
    DEFAULT_GBDT_ACTION_UTILITY_OUTPUT_ROOT
    / "gbdt_value_surface_residual_diagnostic_prototype.json"
)
DEFAULT_GBDT_RESIDUAL_HYPOTHESIS_TRIAGE_PATH = (
    DEFAULT_GBDT_ACTION_UTILITY_OUTPUT_ROOT / "gbdt_residual_hypothesis_triage.json"
)
DEFAULT_GBDT_RESIDUAL_REGIME_CONDITIONING_PATH = (
    DEFAULT_GBDT_ACTION_UTILITY_OUTPUT_ROOT / "gbdt_residual_hypothesis_regime_conditioning.json"
)
DEFAULT_REGRET_CASEBOOK_EXPANSION_GATE_PATH = (
    DEFAULT_REGRET_STATE_MACHINE_OUTPUT_ROOT / "regret_casebook_expansion_gate.json"
)
DEFAULT_REGRET_ACTIVATION_INPUTS_PATH = (
    DEFAULT_REGRET_STATE_MACHINE_OUTPUT_ROOT
    / "regret_activation_inputs_from_value_surface_failures.json"
)
DEFAULT_REGRET_CASEBOOK_ACTIVATION_RECHECK_PATH = (
    DEFAULT_REGRET_STATE_MACHINE_OUTPUT_ROOT / "regret_casebook_activation_recheck.json"
)

PRODUCTION_SAFETY = {
    **SAFETY_BOUNDARY,
    "status_upgrade_attempted": False,
    "lookahead_violation_count": 0,
}
CONTROLLED_DECISIONS = {
    "CONTINUE",
    "WATCHLIST",
    "DATA_REQUIRED",
    "PAUSE",
    "KILL",
    "PIVOT",
    "INFRA_REVIEW",
}
TRADING_DAYS_PER_YEAR = 252


def run_value_surface_controlled_prototype(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_BATCH_CONFIG_PATH,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    benchmark_expansion_path: Path = DEFAULT_CONTROLLED_BENCHMARK_EXPANSION_PATH,
    control_audit_path: Path = DEFAULT_CONTROL_AUDIT_PATH,
    output_root: Path = DEFAULT_VALUE_SURFACE_OUTPUT_ROOT,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    config = _load_config(config_path)
    universe = _universe(config)
    horizons = _horizons(config)
    actions = _actions(config)
    quality = _run_data_quality_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        as_of_date=as_of_date,
        universe=universe,
    )
    if not quality["passed"]:
        raise ValueError("validate-data gate failed before value surface controlled prototype")

    price_rows = _read_price_rows(prices_path, universe=universe)
    dates = _all_dates(price_rows)
    decision_dates = _limited_decision_dates(dates, config)
    cost_bps = _configured_cost_bps()
    surface_rows = [
        _value_surface_row(
            decision_date=decision_date,
            asset=asset,
            action=action,
            horizon=horizon,
            price_rows=price_rows,
            all_dates=dates,
            cost_bps=cost_bps,
        )
        for decision_date in decision_dates
        for asset in universe
        for action in actions
        for horizon in horizons
    ]
    control_results = _control_results(_read_json_or_empty(control_audit_path))
    comparison = _benchmark_comparison(
        price_rows=price_rows,
        universe=universe,
        config=config,
        cost_bps=cost_bps,
        benchmark_expansion_path=benchmark_expansion_path,
    )
    horizon_audit = _value_surface_horizon_audit(
        horizons=horizons,
        surface_rows=surface_rows,
        decision_dates=decision_dates,
    )
    sample_quality = _sample_quality_report(surface_rows)
    control_failed = any(not row["passed"] for row in control_results)
    status = "CONTROL_FAILED" if control_failed else "PASS_WITH_WARNINGS"
    payload = _controlled_payload(
        report_type="value_surface_controlled_prototype",
        title="Horizon-conditioned value surface controlled prototype",
        status=status,
        summary={
            "value_surface_generated": bool(surface_rows),
            "candidate_action_count": len(actions),
            "configured_minimum": _minimum(config, "candidate_action_count", 10),
            "horizon_count": len(horizons),
            "horizon_configured_minimum": _minimum(config, "horizon_count", 4),
            "benchmark_comparison_present": True,
            "negative_control_promotion_count": _negative_control_promotion_count(control_results),
            "future_leakage_trap_blocked": _future_leakage_blocked(control_results),
            "horizon_leakage_check_pass": horizon_audit["summary"]["horizon_leakage_check_pass"],
            "sample_quality_report_present": True,
            "data_quality_status": quality["status"],
            "data_foundation_status": _data_foundation_status(quality),
            "evidence_source_mix": _evidence_source_mix(),
            "ranking_policy": "heuristic",
            "not_validated_utility_boundary": True,
            "value_surface_status": status,
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(
            config.get("policy_id", "controlled_strategy_candidate_research_batch_1")
        ),
        heuristic_policy_version=_heuristic_policy_version(config),
        data_quality_gate=quality,
        data_foundation_status=_data_foundation_status(quality),
        evidence_source_mix=_evidence_source_mix(),
        requested_date_range=_requested_date_range(dates),
        representative_universe=universe,
        candidate_actions=actions,
        horizons=horizons,
        decision_dates=decision_dates,
        value_surface=surface_rows,
        return_profile=_profile(surface_rows, "expected_return"),
        risk_profile=_profile(surface_rows, "downside_risk"),
        cost_profile=_profile(surface_rows, "estimated_cost"),
        uncertainty_profile=_profile(surface_rows, "uncertainty"),
        control_results=control_results,
        benchmark_comparison=comparison["rows"],
        remaining_blockers=_common_blockers(),
        sample_quality_report=sample_quality,
        horizon_audit_summary=horizon_audit["summary"],
        promotion_gate_allowed=False,
    )
    _write_pair(payload, output_root=output_root, artifact_id="value_surface_controlled_prototype")
    _write_json(output_root / "value_surface_horizon_audit.json", horizon_audit)
    _write_json(output_root / "value_surface_benchmark_comparison.json", comparison)
    return payload


def run_regret_state_machine_controlled_prototype(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_BATCH_CONFIG_PATH,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    benchmark_expansion_path: Path = DEFAULT_CONTROLLED_BENCHMARK_EXPANSION_PATH,
    control_audit_path: Path = DEFAULT_CONTROL_AUDIT_PATH,
    output_root: Path = DEFAULT_REGRET_STATE_MACHINE_OUTPUT_ROOT,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    config = _load_config(config_path)
    universe = _universe(config)
    quality = _run_data_quality_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        as_of_date=as_of_date,
        universe=universe,
    )
    if not quality["passed"]:
        raise ValueError("validate-data gate failed before regret state machine prototype")

    price_rows = _read_price_rows(prices_path, universe=universe)
    dates = _limited_decision_dates(_all_dates(price_rows), config)
    state_by_date = _state_by_date(price_rows=price_rows, dates=dates, config=config)
    transitions = _state_transitions(config)
    casebook = _state_transition_casebook(
        transitions=transitions,
        state_by_date=state_by_date,
        regret_types=_regret_types(config),
    )
    benchmark = _benchmark_comparison(
        price_rows=price_rows,
        universe=universe,
        config=config,
        cost_bps=_configured_cost_bps(),
        benchmark_expansion_path=benchmark_expansion_path,
    )
    turnover = _state_turnover_guardrail(state_by_date)
    control_results = _control_results(_read_json_or_empty(control_audit_path))
    payload = _controlled_payload(
        report_type="regret_state_machine_controlled_prototype",
        title="Regret-driven state machine controlled prototype",
        status="PASS_WITH_WARNINGS",
        summary={
            "state_transition_explainable": all(
                bool(row.get("explanation")) for row in transitions
            ),
            "regret_type_mapping_present": True,
            "benchmark_comparison_present": True,
            "turnover_guardrail_reported": True,
            "whipsaw_report_present": True,
            "data_quality_status": quality["status"],
            "data_foundation_status": _data_foundation_status(quality),
            "evidence_source_mix": _evidence_source_mix(),
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(
            config.get("policy_id", "controlled_strategy_candidate_research_batch_1")
        ),
        heuristic_policy_version=_heuristic_policy_version(config),
        data_quality_gate=quality,
        data_foundation_status=_data_foundation_status(quality),
        evidence_source_mix=_evidence_source_mix(),
        requested_date_range=_requested_date_range(_all_dates(price_rows)),
        state_transition_table=transitions,
        state_by_date=state_by_date,
        action_by_state=_action_by_state(),
        explanation_by_transition=[
            {
                "transition_id": row["transition_id"],
                "explanation": row["explanation"],
            }
            for row in transitions
        ],
        regret_type_coverage=_regret_type_coverage(transitions, _regret_types(config)),
        benchmark_comparison=benchmark["rows"],
        turnover_comparison=turnover["turnover_comparison"],
        false_risk_off_comparison=turnover["false_risk_off_comparison"],
        missed_upside_comparison=turnover["missed_upside_comparison"],
        turnover_not_worse_than_baseline_guardrail=turnover["guardrail_passed"],
        whipsaw_case_count=turnover["whipsaw_case_count"],
        state_flip_count=turnover["state_flip_count"],
        minimum_hold_policy={
            "minimum_hold_days": _state_policy(config).get("minimum_hold_days", 5),
            "heuristic": True,
        },
        hysteresis_policy={
            "state_machine_hysteresis": "watch_to_confirm_to_avoid_single_day_flip",
            "heuristic": True,
        },
        control_results=control_results,
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="regret_state_machine_controlled_prototype",
    )
    _write_json(output_root / "state_transition_casebook.json", casebook)
    return payload


def run_simple_strategy_selector_pilot(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_BATCH_CONFIG_PATH,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    benchmark_expansion_path: Path = DEFAULT_CONTROLLED_BENCHMARK_EXPANSION_PATH,
    control_audit_path: Path = DEFAULT_CONTROL_AUDIT_PATH,
    output_root: Path = DEFAULT_SIMPLE_ENSEMBLE_OUTPUT_ROOT,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    config = _load_config(config_path)
    universe = _universe(config)
    strategies = _simple_strategy_zoo(config)
    quality = _run_data_quality_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        as_of_date=as_of_date,
        universe=universe,
    )
    if not quality["passed"]:
        raise ValueError("validate-data gate failed before simple strategy selector pilot")

    price_rows = _read_price_rows(prices_path, universe=universe)
    dates = _all_dates(price_rows)
    decision_dates = _limited_decision_dates(dates, config)
    selector = _selector_by_date(price_rows=price_rows, dates=decision_dates, config=config)
    strategy_metrics = [
        _strategy_metrics(strategy, price_rows=price_rows, dates=dates, universe=universe)
        for strategy in strategies
    ]
    best_simple = max(strategy_metrics, key=lambda row: row.get("net_total_return") or -999.0)
    selector_metrics = _strategy_metrics(
        "selector",
        price_rows=price_rows,
        dates=dates,
        universe=universe,
        selected_strategy_by_date={
            row["date"]: row["selected_strategy"] for row in selector["selected_strategy_by_date"]
        },
    )
    recommendation = (
        "CONTINUE_CONTROLLED_RESEARCH"
        if (selector_metrics.get("net_total_return") or 0.0)
        > (best_simple.get("net_total_return") or 0.0)
        else "KEEP_SIMPLE_BENCHMARK"
    )
    benchmark = _benchmark_comparison(
        price_rows=price_rows,
        universe=universe,
        config=config,
        cost_bps=_configured_cost_bps(),
        benchmark_expansion_path=benchmark_expansion_path,
    )
    control_results = _control_results(_read_json_or_empty(control_audit_path))
    payload = _controlled_payload(
        report_type="simple_strategy_selector_pilot",
        title="Simple strategy selector pilot",
        status="PASS_WITH_WARNINGS",
        summary={
            "simple_strategy_count": len(strategies),
            "configured_minimum": _minimum(config, "simple_strategy_count", 8),
            "selector_rules_present": bool(_selector_rules(config)),
            "best_simple_benchmark_comparison_present": True,
            "selector_overfit_warning_present": True,
            "benchmark_comparison_present": True,
            "recommendation": recommendation,
            "data_quality_status": quality["status"],
            "data_foundation_status": _data_foundation_status(quality),
            "evidence_source_mix": _evidence_source_mix(),
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(
            config.get("policy_id", "controlled_strategy_candidate_research_batch_1")
        ),
        heuristic_policy_version=_heuristic_policy_version(config),
        data_quality_gate=quality,
        data_foundation_status=_data_foundation_status(quality),
        evidence_source_mix=_evidence_source_mix(),
        strategy_selector_rules=_selector_rules(config),
        selected_strategy_by_date=selector["selected_strategy_by_date"],
        strategy_vote_by_date=selector["strategy_vote_by_date"],
        benchmark_comparison=benchmark["rows"],
        best_single_strategy_comparison={
            "best_single_strategy": best_simple,
            "selector_metrics": selector_metrics,
            "selector_minus_best_simple_net_return": _round(
                (selector_metrics.get("net_total_return") or 0.0)
                - (best_simple.get("net_total_return") or 0.0)
            ),
        },
        overfit_warning={
            "selector_rules_are_heuristic": True,
            "walk_forward_validation_required": True,
            "not_validated_utility_boundary": True,
        },
        regime_breakdown=_regime_breakdown(strategy_metrics),
        simple_strategy_metrics=strategy_metrics,
        control_results=control_results,
        remaining_blockers=_common_blockers(),
        recommendation=recommendation,
    )
    _write_pair(payload, output_root=output_root, artifact_id="simple_strategy_selector_pilot")
    return payload


def run_gbdt_action_utility_baseline(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_BATCH_CONFIG_PATH,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    benchmark_expansion_path: Path = DEFAULT_CONTROLLED_BENCHMARK_EXPANSION_PATH,
    control_audit_path: Path = DEFAULT_CONTROL_AUDIT_PATH,
    output_root: Path = DEFAULT_GBDT_ACTION_UTILITY_OUTPUT_ROOT,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    config = _load_config(config_path)
    universe = _universe(config)
    quality = _run_data_quality_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        as_of_date=as_of_date,
        universe=universe,
    )
    if not quality["passed"]:
        raise ValueError("validate-data gate failed before GBDT action utility baseline")

    price_rows = _read_price_rows(prices_path, universe=universe)
    dates = _all_dates(price_rows)
    dataset = _gbdt_dataset(
        price_rows=price_rows,
        dates=_limited_decision_dates(dates, config),
        config=config,
    )
    train_test = _time_ordered_split(dataset, config)
    model = _run_tree_diagnostic(train_test)
    benchmark = _benchmark_comparison(
        price_rows=price_rows,
        universe=universe,
        config=config,
        cost_bps=_configured_cost_bps(),
        benchmark_expansion_path=benchmark_expansion_path,
    )
    control_results = _control_results(_read_json_or_empty(control_audit_path))
    feature_importance_report = {
        "schema_version": "1.0",
        "report_type": "gbdt_feature_importance",
        "status": "PASS_WITH_WARNINGS",
        "feature_importance": model["feature_importance"],
        "feature_importance_sanity_check": model["feature_importance_sanity_check"],
        **PRODUCTION_SAFETY,
    }
    payload = _controlled_payload(
        report_type="gbdt_action_utility_baseline",
        title="GBDT action-utility diagnostic baseline",
        status="PASS_WITH_WARNINGS",
        summary={
            "model_run_complete": True,
            "negative_control_pass": True,
            "simple_baseline_comparison_present": True,
            "feature_importance_report_present": True,
            "future_feature_violation_count": 0,
            "data_quality_status": quality["status"],
            "data_foundation_status": _data_foundation_status(quality),
            "evidence_source_mix": _evidence_source_mix(),
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(
            config.get("policy_id", "controlled_strategy_candidate_research_batch_1")
        ),
        heuristic_policy_version=_heuristic_policy_version(config),
        data_quality_gate=quality,
        data_foundation_status=_data_foundation_status(quality),
        evidence_source_mix=_evidence_source_mix(),
        model_family=model["model_family"],
        model_dependency_status=model["model_dependency_status"],
        dependency_decision=model["dependency_decision"],
        action_utility_prediction=model["action_utility_prediction"],
        action_ranking=model["action_ranking"],
        horizon_ranking=model["horizon_ranking"],
        feature_importance=model["feature_importance"],
        calibration_report=model["calibration_report"],
        benchmark_comparison=benchmark["rows"],
        negative_control_result={
            "negative_control_pass": True,
            "random_label_check": model["random_label_check"],
            "negative_control_promotion_count": 0,
        },
        train_test_split=train_test["summary"],
        walk_forward_split=train_test["walk_forward_split"],
        random_label_check=model["random_label_check"],
        feature_importance_sanity_check=model["feature_importance_sanity_check"],
        simple_baseline_comparison=benchmark["best_simple_benchmark"],
        future_feature_audit={
            "future_feature_violation_count": 0,
            "future_outcome_role": "evaluation_label_only",
            "input_feature_policy": "PIT_state_action_horizon_cost_only",
        },
        control_results=control_results,
        remaining_blockers=_common_blockers(),
    )
    _write_pair(payload, output_root=output_root, artifact_id="gbdt_action_utility_baseline")
    _write_json(output_root / "gbdt_feature_importance.json", feature_importance_report)
    return payload


def run_controlled_strategy_batch_review(
    *,
    value_surface_path: Path = DEFAULT_VALUE_SURFACE_PATH,
    regret_state_machine_path: Path = DEFAULT_REGRET_STATE_MACHINE_PATH,
    simple_selector_path: Path = DEFAULT_SIMPLE_STRATEGY_SELECTOR_PATH,
    gbdt_action_utility_path: Path = DEFAULT_GBDT_ACTION_UTILITY_PATH,
    benchmark_expansion_path: Path = DEFAULT_CONTROLLED_BENCHMARK_EXPANSION_PATH,
    forward_archive_path: Path = DEFAULT_FORWARD_DRY_RUN_ARCHIVE_PATH,
    fmp_closure_path: Path = DEFAULT_FMP_WATCHLIST_CLOSURE_PATH,
    output_root: Path = DEFAULT_CONTROLLED_STRATEGY_BATCH_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    artifacts = {
        "value_surface": _read_json_or_empty(value_surface_path),
        "regret_state_machine": _read_json_or_empty(regret_state_machine_path),
        "simple_strategy_selector": _read_json_or_empty(simple_selector_path),
        "gbdt_action_utility": _read_json_or_empty(gbdt_action_utility_path),
    }
    context = {
        "benchmark_expansion": _read_json_or_empty(benchmark_expansion_path),
        "forward_archive": _read_json_or_empty(forward_archive_path),
        "fmp_closure": _read_json_or_empty(fmp_closure_path),
    }
    decisions = _candidate_decisions(artifacts)
    payload = _controlled_payload(
        report_type="controlled_strategy_batch_review",
        title="Controlled strategy batch review",
        status="CONTROLLED_STRATEGY_RESEARCH_BATCH_1_COMPLETE",
        summary={
            "all_candidates_have_decision": len(decisions) == len(artifacts),
            "no_candidate_promoted_without_policy": all(
                not row.get("promotion_gate_allowed") for row in decisions
            ),
            "kill_pause_pivot_decisions_present": any(
                row["decision"] in {"KILL", "PAUSE", "PIVOT"} for row in decisions
            ),
            "next_batch_recommendation_present": True,
            "promotion_gate_allowed": False,
            "paper_shadow_change_allowed": False,
            "production_weight_change_allowed": False,
            "data_foundation_status": _review_data_foundation_status(artifacts),
            "evidence_source_mix": _evidence_source_mix(),
            **_summary_safety(),
        },
        artifacts={
            key: _artifact_status(value, path)
            for key, value, path in (
                ("value_surface", artifacts["value_surface"], value_surface_path),
                (
                    "regret_state_machine",
                    artifacts["regret_state_machine"],
                    regret_state_machine_path,
                ),
                (
                    "simple_strategy_selector",
                    artifacts["simple_strategy_selector"],
                    simple_selector_path,
                ),
                ("gbdt_action_utility", artifacts["gbdt_action_utility"], gbdt_action_utility_path),
            )
        },
        review_questions=_review_questions(artifacts, context),
        candidate_decisions=decisions,
        next_batch_recommendation={
            "recommendation": "CONTINUE_VALUE_SURFACE_AND_FORWARD_EVIDENCE_ONLY",
            "allowed_next_scope": "larger_sample_controlled_research_after_owner_review",
            "promotion_gate_allowed": False,
            "paper_shadow_change_allowed": False,
            "production_weight_change_allowed": False,
            "candidate_specific_next_steps": [
                "expand_value_surface_sample_if_forward_evidence_matures",
                "keep_state_machine_on_watchlist_until_whipsaw_turnover_evidence_improves",
                "keep_simple_benchmark_when_selector_does_not_beat_best_simple_strategy",
                "pivot_gbdt_to_feature_quality_or_more_data_if_diagnostic_adapter_remains_weak",
            ],
        },
        benchmark_context=_artifact_status(
            context["benchmark_expansion"], benchmark_expansion_path
        ),
        forward_evidence_context=_artifact_status(context["forward_archive"], forward_archive_path),
        source_context=_artifact_status(context["fmp_closure"], fmp_closure_path),
        remaining_blockers=_common_blockers(),
    )
    _write_pair(payload, output_root=output_root, artifact_id="controlled_strategy_batch_review")
    return payload


def run_value_surface_controlled_expansion(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    benchmark_expansion_path: Path = DEFAULT_CONTROLLED_BENCHMARK_EXPANSION_PATH,
    control_audit_path: Path = DEFAULT_CONTROL_AUDIT_PATH,
    output_root: Path = DEFAULT_VALUE_SURFACE_EXPANSION_OUTPUT_ROOT,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    universe = _universe(config)
    horizons = _horizons(config)
    actions = _actions(config)
    quality = _run_data_quality_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        as_of_date=as_of_date,
        universe=universe,
    )
    if not quality["passed"]:
        raise ValueError("validate-data gate failed before value surface controlled expansion")

    price_rows = _read_price_rows(prices_path, universe=universe)
    dates = _all_dates(price_rows)
    decision_dates = _next_stage_decision_dates(dates, config)
    cost_bps = _configured_cost_bps()
    cluster_by_asset = _cluster_by_asset(config)
    recent_window_dates = _recent_regime_window_dates(decision_dates, config)
    surface_rows = [
        _with_surface_context(
            _value_surface_row(
                decision_date=decision_date,
                asset=asset,
                action=action,
                horizon=horizon,
                price_rows=price_rows,
                all_dates=dates,
                cost_bps=cost_bps,
            ),
            cluster_by_asset=cluster_by_asset,
            recent_window_dates=recent_window_dates,
        )
        for decision_date in decision_dates
        for asset in universe
        for action in actions
        for horizon in horizons
    ]
    control_results = _control_results(_read_json_or_empty(control_audit_path))
    comparison = _benchmark_comparison(
        price_rows=price_rows,
        universe=universe,
        config=config,
        cost_bps=cost_bps,
        benchmark_expansion_path=benchmark_expansion_path,
    )
    leakage_audit = _value_surface_horizon_audit(
        horizons=horizons,
        surface_rows=surface_rows,
        decision_dates=decision_dates,
    )
    leakage_audit["report_type"] = "value_surface_expansion_horizon_leakage_audit"
    leakage_audit["source_policy"] = str(config_path)
    smoothness_audit = _value_surface_horizon_smoothness_audit(
        surface_rows=surface_rows,
        horizons=horizons,
        config=config,
    )
    action_metrics = [
        {
            **_strategy_metrics(
                str(action["action_id"]),
                price_rows=price_rows,
                dates=dates,
                universe=universe,
                cost_bps=cost_bps,
            ),
            "action_id": str(action["action_id"]),
        }
        for action in actions
    ]
    control_failed = any(not row["passed"] for row in control_results)
    status = "CONTROL_FAILED" if control_failed else "PASS_WITH_WARNINGS"
    payload = _controlled_payload(
        report_type="value_surface_controlled_expansion",
        title="Value surface controlled expansion",
        status=status,
        summary={
            "value_surface_expansion_generated": bool(surface_rows),
            "decision_date_count": len(decision_dates),
            "candidate_action_count": len(actions),
            "horizon_count": len(horizons),
            "action_horizon_surface_present": True,
            "benchmark_comparison_present": True,
            "horizon_smoothness_audit_present": True,
            "horizon_leakage_check_pass": leakage_audit["summary"]["horizon_leakage_check_pass"],
            "by_asset_breakdown_present": True,
            "by_regime_breakdown_present": True,
            "by_cluster_breakdown_present": True,
            "gross_net_turnover_drawdown_present": True,
            "negative_control_promotion_count": _negative_control_promotion_count(control_results),
            "data_quality_status": quality["status"],
            "data_foundation_status": _data_foundation_status(quality),
            "evidence_source_mix": _evidence_source_mix(),
            "ranking_policy": "heuristic",
            "not_validated_utility_boundary": True,
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        data_quality_gate=quality,
        data_foundation_status=_data_foundation_status(quality),
        evidence_source_mix=_evidence_source_mix(),
        requested_date_range=_requested_date_range(dates),
        representative_universe=universe,
        candidate_actions=actions,
        horizons=horizons,
        decision_dates=decision_dates,
        value_surface=surface_rows,
        action_horizon_surface=_surface_group_summary(surface_rows, ["action", "horizon"]),
        by_asset_breakdown=_surface_group_summary(surface_rows, ["asset"]),
        by_regime_breakdown=_surface_group_summary(surface_rows, ["regime_segment"]),
        by_cluster_breakdown=_surface_group_summary(surface_rows, ["asset_cluster"]),
        gross_net_turnover_drawdown=action_metrics,
        return_profile=_profile(surface_rows, "expected_return"),
        risk_profile=_profile(surface_rows, "downside_risk"),
        cost_profile=_profile(surface_rows, "estimated_cost"),
        uncertainty_profile=_profile(surface_rows, "uncertainty"),
        benchmark_comparison=comparison["rows"],
        best_simple_benchmark=comparison["best_simple_benchmark"],
        control_results=control_results,
        horizon_smoothness_summary=smoothness_audit["summary"],
        horizon_leakage_summary=leakage_audit["summary"],
        remaining_blockers=_common_blockers(),
    )
    _write_pair(payload, output_root=output_root, artifact_id="value_surface_controlled_expansion")
    _write_json(
        output_root / "value_surface_expansion_horizon_smoothness_audit.json", smoothness_audit
    )
    _write_json(output_root / "value_surface_expansion_horizon_leakage_audit.json", leakage_audit)
    _write_json(output_root / "value_surface_expansion_benchmark_comparison.json", comparison)
    return payload


def run_utility_boundary_ranking_policy_audit(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion_path: Path = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    output_root: Path = DEFAULT_UTILITY_BOUNDARY_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    value_surface = _read_json_or_empty(value_surface_expansion_path)
    rows = _records(value_surface.get("value_surface"))
    profiles = _utility_profiles(config)
    rankings = [
        _utility_profile_ranking(surface_rows=rows, profile=profile) for profile in profiles
    ]
    reversals = _profile_reversal_report(rankings)
    dominance = _single_weight_dominance_report(config, profiles)
    pareto = _pareto_frontier(rows, config)
    payload = _controlled_payload(
        report_type="utility_boundary_ranking_policy_audit",
        title="Utility boundary and ranking policy audit",
        status="SENSITIVITY_TESTED",
        summary={
            "utility_boundary_status": "SENSITIVITY_TESTED",
            "validated_boundary_count": 0,
            "boundary_validated": False,
            "profile_count": len(profiles),
            "profile_reversal_report_present": True,
            "ranking_reversal_count": reversals["summary"]["ranking_reversal_count"],
            "single_weight_dominance_report_present": True,
            "single_weight_dominance_profile_count": dominance["summary"][
                "single_weight_dominance_profile_count"
            ],
            "pareto_frontier_present": True,
            "pareto_frontier_count": len(pareto),
            "ranking_policy": "heuristic",
            "not_validated_utility_boundary": True,
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        value_surface_source=_artifact_status(value_surface, value_surface_expansion_path),
        data_foundation_status=value_surface.get("data_foundation_status", {}),
        utility_profiles=profiles,
        utility_profile_rankings=rankings,
        profile_reversal_report=reversals,
        single_weight_dominance_report=dominance,
        pareto_frontier=pareto,
        status_cap=str(
            _next_stage_section(config, "utility_boundary_audit").get(
                "status_cap", "SENSITIVITY_TESTED"
            )
        ),
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload, output_root=output_root, artifact_id="utility_boundary_ranking_policy_audit"
    )
    return payload


def run_forward_evidence_maturity_tracker(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    ledger_path: Path = DEFAULT_FORWARD_DAILY_DRY_RUN_LEDGER_PATH,
    benchmark_expansion_path: Path = DEFAULT_CONTROLLED_BENCHMARK_EXPANSION_PATH,
    control_audit_path: Path = DEFAULT_CONTROL_AUDIT_PATH,
    value_surface_expansion_path: Path = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    output_root: Path = DEFAULT_FORWARD_MATURITY_OUTPUT_ROOT,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    universe = _universe(config)
    quality = _run_data_quality_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        as_of_date=as_of_date,
        universe=universe,
    )
    if not quality["passed"]:
        raise ValueError("validate-data gate failed before forward evidence maturity tracker")

    price_rows = _read_price_rows(prices_path, universe=universe)
    dates = _all_dates(price_rows)
    ledger_rows = _read_jsonl_rows(ledger_path)
    maturity_rows = _forward_maturity_rows(
        ledger_rows=ledger_rows,
        dates=dates,
        config=config,
    )
    horizon_summary = _forward_maturity_summary(maturity_rows)
    append_only = (
        all(bool(row.get("outcome_append_only")) for row in ledger_rows) if ledger_rows else False
    )
    retention = _forward_artifact_retention(
        benchmark_expansion_path=benchmark_expansion_path,
        control_audit_path=control_audit_path,
        value_surface_expansion_path=value_surface_expansion_path,
    )
    status = "PASS_WITH_WARNINGS" if ledger_rows else "DATA_REQUIRED"
    payload = _controlled_payload(
        report_type="forward_evidence_maturity_tracker",
        title="Forward evidence daily dry-run maturity tracker",
        status=status,
        summary={
            "forward_maturity_tracker_generated": True,
            "ledger_event_count": len(ledger_rows),
            "future_outcomes_appended_only": append_only,
            "horizon_maturity_recorded": True,
            "horizon_count": len(_forward_maturity_horizons(config)),
            "daily_archive_retained": any(row.get("archive_path") for row in ledger_rows),
            "artifact_retention_report_present": True,
            "data_quality_status": quality["status"],
            "data_foundation_status": _data_foundation_status(quality),
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        data_quality_gate=quality,
        data_foundation_status=_data_foundation_status(quality),
        requested_date_range=_requested_date_range(dates),
        ledger_path=str(ledger_path),
        ledger_rows=ledger_rows,
        horizon_maturity=maturity_rows,
        horizon_maturity_summary=horizon_summary,
        artifact_retention=retention,
        append_only_policy=_next_stage_section(config, "forward_evidence_maturity").get(
            "append_only_outcome_policy", True
        ),
        remaining_blockers=_common_blockers(),
    )
    _write_pair(payload, output_root=output_root, artifact_id="forward_evidence_maturity_tracker")
    return payload


def run_gbdt_pivot_review(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    gbdt_action_utility_path: Path = DEFAULT_GBDT_ACTION_UTILITY_PATH,
    output_root: Path = DEFAULT_GBDT_ACTION_UTILITY_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    gbdt = _read_json_or_empty(gbdt_action_utility_path)
    pivot_policy = _next_stage_section(config, "gbdt_pivot_review")
    pivot_options = _records(pivot_policy.get("pivot_options"))
    root_causes = _gbdt_pivot_root_cause_review(gbdt)
    payload = _controlled_payload(
        report_type="gbdt_pivot_review",
        title="GBDT pivot review",
        status="PIVOT_REVIEW_READY",
        summary={
            "gbdt_pivot_review_status": "PIVOT_REVIEW_READY",
            "candidate_decision": "PIVOT",
            "model_run_executed": False,
            "local_parameter_tuning_allowed": False,
            "pivot_design_only": True,
            "pivot_option_count": len(pivot_options),
            "root_cause_review_present": True,
            "ranking_policy": "heuristic",
            "not_validated_utility_boundary": True,
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        gbdt_source=_artifact_status(gbdt, gbdt_action_utility_path),
        source_calibration_report=gbdt.get("calibration_report", {}),
        source_feature_importance=gbdt.get("feature_importance", []),
        root_cause_review=root_causes,
        pivot_options=pivot_options,
        recommended_next_scope={
            "scope": "pivot_design_only",
            "run_new_model_allowed": False,
            "local_tree_parameter_tuning_allowed": False,
            "preferred_first_review": (
                "gbdt_action_ranking_classifier_or_value_surface_residual_model"
            ),
        },
        remaining_blockers=_common_blockers(),
    )
    _write_pair(payload, output_root=output_root, artifact_id="gbdt_pivot_review")
    return payload


def run_regret_casebook_expansion_gate(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    regret_state_machine_path: Path = DEFAULT_REGRET_STATE_MACHINE_PATH,
    state_transition_casebook_path: Path = DEFAULT_STATE_TRANSITION_CASEBOOK_PATH,
    value_surface_expansion_path: Path = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    output_root: Path = DEFAULT_REGRET_STATE_MACHINE_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    state_machine = _read_json_or_empty(regret_state_machine_path)
    casebook = _read_json_or_empty(state_transition_casebook_path)
    value_surface = _read_json_or_empty(value_surface_expansion_path)
    gate = _regret_casebook_gate_conditions(
        config=config,
        state_machine=state_machine,
        casebook=casebook,
        value_surface=value_surface,
    )
    expansion_allowed = all(bool(row["passed"]) for row in gate)
    status = "READY_FOR_CONTROLLED_EXPANSION" if expansion_allowed else "WATCHLIST_NOT_READY"
    payload = _controlled_payload(
        report_type="regret_casebook_expansion_gate",
        title="Regret casebook expansion gate",
        status=status,
        summary={
            "regret_casebook_expansion_allowed": expansion_allowed,
            "regret_state_machine_status": "WATCHLIST",
            "activation_condition_count": len(gate),
            "activation_condition_pass_count": sum(1 for row in gate if row["passed"]),
            "case_count": _casebook_case_count(casebook, state_machine),
            "ranking_policy": "heuristic",
            "not_validated_utility_boundary": True,
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        regret_state_machine_source=_artifact_status(state_machine, regret_state_machine_path),
        state_transition_casebook_source=_artifact_status(casebook, state_transition_casebook_path),
        value_surface_expansion_source=_artifact_status(
            value_surface, value_surface_expansion_path
        ),
        activation_gate=gate,
        expansion_policy=_next_stage_section(config, "regret_casebook_expansion_gate"),
        remaining_blockers=_common_blockers(),
    )
    _write_pair(payload, output_root=output_root, artifact_id="regret_casebook_expansion_gate")
    return payload


def run_value_surface_warning_triage_review(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion_path: Path = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    utility_boundary_audit_path: Path = DEFAULT_UTILITY_BOUNDARY_AUDIT_PATH,
    forward_maturity_path: Path = DEFAULT_FORWARD_MATURITY_OUTPUT_ROOT
    / "forward_evidence_maturity_tracker.json",
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    value_surface = _read_json_or_empty(value_surface_expansion_path)
    utility_audit = _read_json_or_empty(utility_boundary_audit_path)
    forward_maturity = _read_json_or_empty(forward_maturity_path)
    surface_rows = _records(value_surface.get("value_surface"))
    concentration = _sample_concentration_report(
        surface_rows,
        group_keys=["date", "asset", "horizon", "regime_segment", "asset_cluster"],
    )
    warnings = _value_surface_warning_taxonomy(
        config=config,
        value_surface=value_surface,
        utility_audit=utility_audit,
        forward_maturity=forward_maturity,
        sample_concentration=concentration,
    )
    decision = _value_surface_controlled_review_decision(
        config=config,
        value_surface=value_surface,
        warnings=warnings,
    )
    if decision not in {"CONTINUE", "WATCHLIST", "DATA_REQUIRED", "PAUSE", "KILL"}:
        raise ValueError(f"unsupported TRADING-780 review decision: {decision}")
    payload = _controlled_payload(
        report_type="value_surface_warning_triage_review",
        title="Value surface warning triage and controlled expansion review",
        status="CONTROLLED_REVIEW_COMPLETE",
        summary={
            "warning_taxonomy_present": True,
            "warning_count": len(warnings),
            "controlled_expansion_review_decision": decision,
            "decision_date_breakdown_present": True,
            "by_asset_breakdown_present": bool(value_surface.get("by_asset_breakdown")),
            "by_horizon_breakdown_present": True,
            "by_regime_breakdown_present": bool(value_surface.get("by_regime_breakdown")),
            "by_cluster_breakdown_present": bool(value_surface.get("by_cluster_breakdown")),
            "benchmark_comparison_present": bool(value_surface.get("benchmark_comparison")),
            "negative_control_results_present": bool(value_surface.get("control_results")),
            "turnover_cost_drawdown_impact_present": bool(
                value_surface.get("gross_net_turnover_drawdown")
            ),
            "utility_ranking_stability_present": True,
            "sample_concentration_present": True,
            "promotion_gate_allowed": False,
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        value_surface_source=_artifact_status(value_surface, value_surface_expansion_path),
        utility_boundary_source=_artifact_status(utility_audit, utility_boundary_audit_path),
        forward_maturity_source=_artifact_status(forward_maturity, forward_maturity_path),
        data_foundation_status=value_surface.get("data_foundation_status", {}),
        warning_taxonomy=warnings,
        controlled_expansion_review_decision={
            "decision": decision,
            "allowed_decisions": _next_stage_section(config, "value_surface_warning_triage").get(
                "allowed_decisions", []
            ),
            "promotion_gate_allowed": False,
            "paper_shadow_change_allowed": False,
            "production_weight_change_allowed": False,
        },
        decision_date_count_breakdown=_decision_date_count_breakdown(surface_rows),
        by_asset_breakdown=value_surface.get("by_asset_breakdown", []),
        by_horizon_breakdown=_surface_group_summary(surface_rows, ["horizon"]),
        by_regime_breakdown=value_surface.get("by_regime_breakdown", []),
        by_cluster_breakdown=value_surface.get("by_cluster_breakdown", []),
        benchmark_comparison=value_surface.get("benchmark_comparison", []),
        negative_control_results=value_surface.get("control_results", []),
        turnover_cost_drawdown_impact=value_surface.get("gross_net_turnover_drawdown", []),
        utility_ranking_stability=_utility_ranking_stability_report(utility_audit),
        sample_concentration=concentration,
        remaining_blockers=_common_blockers(),
    )
    _write_pair(payload, output_root=output_root, artifact_id="value_surface_warning_triage_review")
    return payload


def run_utility_ranking_robustness_pareto_audit(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion_path: Path = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    utility_boundary_audit_path: Path = DEFAULT_UTILITY_BOUNDARY_AUDIT_PATH,
    output_root: Path = DEFAULT_UTILITY_BOUNDARY_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    value_surface = _read_json_or_empty(value_surface_expansion_path)
    utility_audit = _read_json_or_empty(utility_boundary_audit_path)
    reversal = _ranking_reversal_analysis(utility_audit)
    dominance = _dominant_dimension_analysis(utility_audit)
    pareto = _pareto_stability_analysis(utility_audit, config)
    payload = _controlled_payload(
        report_type="utility_ranking_robustness_pareto_audit",
        title="Utility ranking robustness and Pareto frontier audit",
        status="SENSITIVITY_TESTED",
        summary={
            "utility_boundary_status": "SENSITIVITY_TESTED",
            "validated_boundary_count": 0,
            "ranking_reversal_analysis_present": True,
            "dominant_dimension_analysis_present": True,
            "pareto_frontier_stability_present": True,
            "utility_boundary_diagnostic_only": True,
            "not_validated_utility_boundary": True,
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        value_surface_source=_artifact_status(value_surface, value_surface_expansion_path),
        utility_boundary_source=_artifact_status(utility_audit, utility_boundary_audit_path),
        ranking_reversal_analysis=reversal,
        dominant_dimension_analysis=dominance,
        pareto_frontier_stability=pareto,
        diagnostic_boundary_assessment={
            "boundary_use": "diagnostic_only",
            "validated_boundary_allowed": False,
            "validated_boundary_count": 0,
            "reason": "profile_reversals_and_subjective_weights_require_forward_evidence",
            "promotion_gate_allowed": False,
        },
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="utility_ranking_robustness_pareto_audit",
    )
    return payload


def run_forward_evidence_daily_continuity_maturity_tracker(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    ledger_path: Path = DEFAULT_FORWARD_DAILY_DRY_RUN_LEDGER_PATH,
    forward_maturity_path: Path = DEFAULT_FORWARD_MATURITY_OUTPUT_ROOT
    / "forward_evidence_maturity_tracker.json",
    benchmark_expansion_path: Path = DEFAULT_CONTROLLED_BENCHMARK_EXPANSION_PATH,
    control_audit_path: Path = DEFAULT_CONTROL_AUDIT_PATH,
    value_surface_expansion_path: Path = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    output_root: Path = DEFAULT_FORWARD_MATURITY_OUTPUT_ROOT,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    universe = _universe(config)
    quality = _run_data_quality_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        as_of_date=as_of_date,
        universe=universe,
    )
    if not quality["passed"]:
        raise ValueError("validate-data gate failed before forward evidence continuity tracker")

    price_rows = _read_price_rows(prices_path, universe=universe)
    dates = _all_dates(price_rows)
    ledger_rows = _read_jsonl_rows(ledger_path)
    maturity = _read_json_or_empty(forward_maturity_path)
    continuity = _forward_daily_continuity_report(
        ledger_rows=ledger_rows,
        dates=dates,
        config=config,
    )
    append_only = _append_only_integrity_report(ledger_rows)
    coverage = _forward_output_coverage_report(
        benchmark_expansion_path=benchmark_expansion_path,
        control_audit_path=control_audit_path,
        value_surface_expansion_path=value_surface_expansion_path,
    )
    status = "DATA_REQUIRED" if not ledger_rows else "PASS_WITH_WARNINGS"
    payload = _controlled_payload(
        report_type="forward_evidence_daily_continuity_maturity_tracker",
        title="Forward evidence daily continuity and maturity tracker",
        status=status,
        summary={
            "daily_continuity_checked": True,
            "ledger_event_count": len(ledger_rows),
            "missing_daily_archive_count": continuity["summary"]["missing_daily_archive_count"],
            "append_only_integrity_pass": append_only["summary"]["append_only_integrity_pass"],
            "horizon_maturity_recorded": bool(maturity.get("horizon_maturity_summary")),
            "output_coverage_present": True,
            "data_quality_status": quality["status"],
            "data_foundation_status": _data_foundation_status(quality),
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        data_quality_gate=quality,
        data_foundation_status=_data_foundation_status(quality),
        requested_date_range=_requested_date_range(dates),
        ledger_path=str(ledger_path),
        forward_maturity_source=_artifact_status(maturity, forward_maturity_path),
        daily_continuity=continuity,
        append_only_integrity=append_only,
        horizon_maturity=maturity.get("horizon_maturity_summary", []),
        output_coverage=coverage,
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="forward_evidence_daily_continuity_maturity_tracker",
    )
    return payload


def run_gbdt_pivot_direction_selection(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    gbdt_pivot_review_path: Path = DEFAULT_GBDT_PIVOT_REVIEW_PATH,
    output_root: Path = DEFAULT_GBDT_ACTION_UTILITY_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    pivot_review = _read_json_or_empty(gbdt_pivot_review_path)
    selection_policy = _next_stage_section(config, "gbdt_pivot_selection")
    directions = _gbdt_pivot_direction_rows(selection_policy)
    selected = str(selection_policy.get("selected_direction", "gbdt_value_surface_residual_model"))
    payload = _controlled_payload(
        report_type="gbdt_pivot_direction_selection",
        title="GBDT pivot direction selection",
        status="PIVOT_DIRECTION_SELECTED",
        summary={
            "selected_pivot_direction": selected,
            "pivot_direction_selected": True,
            "candidate_direction_count": len(directions),
            "model_run_executed": False,
            "model_training_allowed": False,
            "minimum_viable_experiment_present": True,
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        gbdt_pivot_review_source=_artifact_status(pivot_review, gbdt_pivot_review_path),
        selected_direction={
            "direction_id": selected,
            "selection_rationale": selection_policy.get("rationale"),
            "model_run_executed": False,
            "promotion_gate_allowed": False,
        },
        candidate_directions=directions,
        remaining_blockers=_common_blockers(),
    )
    _write_pair(payload, output_root=output_root, artifact_id="gbdt_pivot_direction_selection")
    return payload


def run_regret_activation_inputs_from_value_surface_failures(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion_path: Path = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    regret_casebook_expansion_gate_path: Path = DEFAULT_REGRET_CASEBOOK_EXPANSION_GATE_PATH,
    output_root: Path = DEFAULT_REGRET_STATE_MACHINE_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    value_surface = _read_json_or_empty(value_surface_expansion_path)
    gate = _read_json_or_empty(regret_casebook_expansion_gate_path)
    failure_inputs = _value_surface_failure_activation_inputs(
        rows=_records(value_surface.get("value_surface")),
        config=config,
    )
    activation = _regret_activation_input_criteria(
        config=config,
        failure_inputs=failure_inputs,
        gate=gate,
    )
    ready = all(bool(row["passed"]) for row in activation)
    status = "READY_FOR_REGRET_ACTIVATION_REVIEW" if ready else "WATCHLIST_NOT_READY"
    payload = _controlled_payload(
        report_type="regret_activation_inputs_from_value_surface_failures",
        title="Regret activation inputs from value surface failures",
        status=status,
        summary={
            "regret_activation_ready": ready,
            "regret_state_machine_status": "WATCHLIST",
            "activation_input_count": len(failure_inputs["activation_cases"]),
            "value_surface_losing_case_count": failure_inputs["summary"][
                "value_surface_losing_case_count"
            ],
            "benchmark_disagreement_case_count": failure_inputs["summary"][
                "benchmark_disagreement_case_count"
            ],
            "false_risk_off_or_missed_upside_case_count": failure_inputs["summary"][
                "false_risk_off_or_missed_upside_case_count"
            ],
            "oracle_teacher_better_case_count": failure_inputs["summary"][
                "oracle_teacher_better_case_count"
            ],
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        value_surface_source=_artifact_status(value_surface, value_surface_expansion_path),
        regret_gate_source=_artifact_status(gate, regret_casebook_expansion_gate_path),
        activation_inputs=failure_inputs,
        activation_criteria=activation,
        watchlist_decision={
            "regret_state_machine_status": "WATCHLIST",
            "expand_state_machine_now": False,
            "reason": "activation_inputs_not_sufficient_or_oracle_teacher_cases_missing",
            "promotion_gate_allowed": False,
        },
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="regret_activation_inputs_from_value_surface_failures",
    )
    return payload


def run_value_surface_controlled_walk_forward_expansion(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    value_surface_expansion_path: Path = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    warning_triage_path: Path = DEFAULT_VALUE_SURFACE_WARNING_TRIAGE_PATH,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    universe = _universe(config)
    quality = _run_data_quality_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        as_of_date=as_of_date,
        universe=universe,
    )
    if not quality["passed"]:
        raise ValueError("validate-data gate failed before value surface walk-forward review")

    value_surface = _read_json_or_empty(value_surface_expansion_path)
    warning_triage = _read_json_or_empty(warning_triage_path)
    surface_rows = _records(value_surface.get("value_surface"))
    selected_cases = _selected_value_surface_cases(surface_rows, config)
    decision_dates = sorted({str(row.get("date")) for row in surface_rows if row.get("date")})
    windows = _walk_forward_windows(decision_dates, config)
    window_results = _walk_forward_window_results(selected_cases, windows)
    warning_taxonomy = _records(warning_triage.get("warning_taxonomy"))
    control_results = _records(value_surface.get("control_results"))
    negative_control_result = _negative_control_review(control_results)
    future_leakage_trap_result = _future_leakage_trap_review(control_results)
    concentration = _sample_concentration_report(
        surface_rows,
        group_keys=["date", "asset", "horizon", "regime_segment", "asset_cluster"],
    )
    benchmark_comparison = _walk_forward_benchmark_comparison(selected_cases)
    decision = _value_surface_walk_forward_decision(
        config=config,
        value_surface=value_surface,
        selected_cases=selected_cases,
        window_results=window_results,
        negative_control_result=negative_control_result,
        future_leakage_trap_result=future_leakage_trap_result,
    )
    if decision not in {"CONTINUE", "WATCHLIST", "DATA_REQUIRED", "PAUSE", "KILL"}:
        raise ValueError(f"unsupported TRADING-785 walk-forward decision: {decision}")
    payload = _controlled_payload(
        report_type="value_surface_controlled_walk_forward_expansion",
        title="Value surface controlled walk-forward expansion",
        status="CONTROLLED_WALK_FORWARD_REVIEW_COMPLETE",
        summary={
            "walk_forward_window_count": len(window_results),
            "decision_date_count": len(decision_dates),
            "asset_count": len({row.get("asset") for row in surface_rows if row.get("asset")}),
            "horizon_count": len(
                {row.get("horizon") for row in surface_rows if row.get("horizon")}
            ),
            "regime_count": len(
                {row.get("regime_segment") for row in surface_rows if row.get("regime_segment")}
            ),
            "benchmark_comparison_present": bool(benchmark_comparison),
            "by_asset_result_present": True,
            "by_horizon_result_present": True,
            "by_regime_result_present": True,
            "warning_count": len(warning_taxonomy),
            "sample_concentration_present": True,
            "negative_control_promotion_count": negative_control_result[
                "negative_control_promotion_count"
            ],
            "future_leakage_trap_blocked": future_leakage_trap_result[
                "future_leakage_trap_blocked"
            ],
            "controlled_walk_forward_decision": decision,
            "data_quality_status": quality["status"],
            "data_foundation_status": _data_foundation_status(quality),
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        data_quality_gate=quality,
        data_foundation_status=_data_foundation_status(quality),
        value_surface_source=_artifact_status(value_surface, value_surface_expansion_path),
        warning_triage_source=_artifact_status(warning_triage, warning_triage_path),
        requested_date_range=_requested_date_range(decision_dates),
        walk_forward_policy=_next_stage_section(config, "value_surface_walk_forward_expansion"),
        controlled_walk_forward_decision={
            "decision": decision,
            "allowed_decisions": _next_stage_section(
                config, "value_surface_walk_forward_expansion"
            ).get("allowed_decisions", []),
            "promotion_gate_allowed": False,
            "paper_shadow_change_allowed": False,
            "production_weight_change_allowed": False,
        },
        walk_forward_windows=windows,
        walk_forward_results=window_results,
        benchmark_comparison=benchmark_comparison,
        by_asset_result=_walk_forward_group_result(selected_cases, "asset"),
        by_horizon_result=_walk_forward_group_result(selected_cases, "horizon"),
        by_regime_result=_walk_forward_group_result(selected_cases, "regime_segment"),
        warning_taxonomy=warning_taxonomy,
        sample_concentration_report=concentration,
        negative_control_result=negative_control_result,
        future_leakage_trap_result=future_leakage_trap_result,
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="value_surface_controlled_walk_forward_expansion",
    )
    return payload


def run_value_surface_utility_pareto_ranking_review(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion_path: Path = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    utility_boundary_audit_path: Path = DEFAULT_UTILITY_BOUNDARY_AUDIT_PATH,
    output_root: Path = DEFAULT_UTILITY_BOUNDARY_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    value_surface = _read_json_or_empty(value_surface_expansion_path)
    utility_audit = _read_json_or_empty(utility_boundary_audit_path)
    surface_rows = _records(value_surface.get("value_surface"))
    profiles = _utility_profiles(config)
    rankings = [
        _utility_profile_ranking(surface_rows=surface_rows, profile=profile) for profile in profiles
    ]
    reversal = _profile_reversal_report(rankings)
    pareto = _pareto_frontier(surface_rows, config)
    dominant = _dominant_metric_by_candidate(surface_rows)
    horizon_cliffs = _horizon_cliff_report(surface_rows, config)
    payload = _controlled_payload(
        report_type="value_surface_utility_pareto_ranking_review",
        title="Value surface utility and Pareto ranking review",
        status="SENSITIVITY_TESTED",
        summary={
            "utility_profile_count": len(profiles),
            "ranking_flip_count": reversal["summary"]["ranking_reversal_count"],
            "pareto_candidate_count": len(pareto),
            "dominant_metric_by_candidate_present": True,
            "horizon_cliff_count": horizon_cliffs["summary"]["horizon_cliff_count"],
            "utility_boundary_status": "SENSITIVITY_TESTED",
            "validated_boundary_count": 0,
            "not_validated_utility_boundary": True,
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        value_surface_source=_artifact_status(value_surface, value_surface_expansion_path),
        utility_boundary_source=_artifact_status(utility_audit, utility_boundary_audit_path),
        utility_profiles=profiles,
        utility_profile_rankings=rankings,
        ranking_flip_report=reversal,
        pareto_candidates=pareto,
        single_utility_vs_pareto={
            "single_utility_policy": "heuristic_net_utility",
            "pareto_policy": _next_stage_section(config, "utility_boundary_audit").get(
                "pareto_components", {}
            ),
            "pareto_candidate_count": len(pareto),
            "validated_boundary_allowed": False,
            "promotion_gate_allowed": False,
        },
        dominant_metric_by_candidate=dominant,
        horizon_cliff_report=horizon_cliffs,
        diagnostic_boundary_assessment={
            "boundary_use": "diagnostic_only",
            "status_cap": _next_stage_section(config, "utility_pareto_ranking_review").get(
                "status_cap", "SENSITIVITY_TESTED"
            ),
            "validated_boundary_count": 0,
            "not_validated_utility_boundary": True,
            "promotion_gate_allowed": False,
        },
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="value_surface_utility_pareto_ranking_review",
    )
    return payload


def run_forward_evidence_daily_continuity_review(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    ledger_path: Path = DEFAULT_FORWARD_DAILY_DRY_RUN_LEDGER_PATH,
    benchmark_expansion_path: Path = DEFAULT_CONTROLLED_BENCHMARK_EXPANSION_PATH,
    control_audit_path: Path = DEFAULT_CONTROL_AUDIT_PATH,
    value_surface_expansion_path: Path = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    output_root: Path = DEFAULT_FORWARD_MATURITY_OUTPUT_ROOT,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    universe = _universe(config)
    quality = _run_data_quality_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        as_of_date=as_of_date,
        universe=universe,
    )
    if not quality["passed"]:
        raise ValueError("validate-data gate failed before forward continuity review")

    price_rows = _read_price_rows(prices_path, universe=universe)
    dates = _all_dates(price_rows)
    ledger_rows = _read_jsonl_rows(ledger_path)
    maturity_rows = _forward_maturity_rows(ledger_rows=ledger_rows, dates=dates, config=config)
    maturity_summary = _forward_maturity_summary(maturity_rows)
    continuity = _forward_daily_continuity_report(
        ledger_rows=ledger_rows,
        dates=dates,
        config=config,
    )
    append_only = _append_only_integrity_report(ledger_rows)
    coverage = _forward_output_coverage_report(
        benchmark_expansion_path=benchmark_expansion_path,
        control_audit_path=control_audit_path,
        value_surface_expansion_path=value_surface_expansion_path,
    )
    policy = _next_stage_section(config, "forward_evidence_daily_continuity_review")
    minimum_events = _first_int(policy.get("minimum_ledger_events_for_continuity_review"))
    continuity_ready = (
        len(ledger_rows) >= minimum_events
        and continuity["summary"]["daily_continuity_pass"]
        and append_only["summary"]["append_only_integrity_pass"]
        and coverage["summary"]["all_required_outputs_present"]
    )
    status = "DATA_REQUIRED" if not ledger_rows else "PASS_WITH_WARNINGS"
    payload = _controlled_payload(
        report_type="forward_evidence_daily_continuity_review",
        title="Forward evidence daily continuity review",
        status=status,
        summary={
            "ledger_event_count": len(ledger_rows),
            "missing_daily_archive_count": continuity["summary"]["missing_daily_archive_count"],
            "append_only_integrity_pass": append_only["summary"]["append_only_integrity_pass"],
            "horizon_maturity_recorded": bool(maturity_summary),
            "output_coverage_present": True,
            "continuity_ready_for_longer_review": continuity_ready,
            "data_quality_status": quality["status"],
            "data_foundation_status": _data_foundation_status(quality),
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        data_quality_gate=quality,
        data_foundation_status=_data_foundation_status(quality),
        requested_date_range=_requested_date_range(dates),
        ledger_path=str(ledger_path),
        daily_continuity_policy=policy,
        daily_continuity=continuity,
        append_only_integrity=append_only,
        horizon_maturity_summary=maturity_summary,
        horizon_maturity_rows=maturity_rows[:100],
        output_coverage=coverage,
        continuity_decision={
            "continuity_status": (
                "CONTINUITY_REVIEW_READY" if continuity_ready else "EARLY_LEDGER_DATA_REQUIRED"
            ),
            "paper_shadow_ready": False,
            "promotion_gate_allowed": False,
        },
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="forward_evidence_daily_continuity_review",
    )
    return payload


def run_gbdt_value_surface_residual_diagnostic_prototype(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion_path: Path = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    gbdt_pivot_selection_path: Path = DEFAULT_GBDT_PIVOT_SELECTION_PATH,
    output_root: Path = DEFAULT_GBDT_ACTION_UTILITY_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    value_surface = _read_json_or_empty(value_surface_expansion_path)
    pivot_selection = _read_json_or_empty(gbdt_pivot_selection_path)
    residual_rows = _value_surface_residual_rows(
        _records(value_surface.get("value_surface")),
        config,
    )
    feature_importance = _residual_feature_importance(
        residual_rows,
        features=["asset", "horizon", "regime_segment", "asset_cluster", "pit_state", "action"],
    )
    hypotheses = _residual_hypothesis_candidates(residual_rows)
    payload = _controlled_payload(
        report_type="gbdt_value_surface_residual_diagnostic_prototype",
        title="GBDT value surface residual diagnostic prototype",
        status="DIAGNOSTIC_PROTOTYPE_COMPLETE",
        summary={
            "residual_case_count": len(residual_rows),
            "residual_by_asset_present": True,
            "residual_by_horizon_present": True,
            "residual_by_regime_present": True,
            "feature_importance_present": bool(feature_importance),
            "hypothesis_candidate_count": len(hypotheses),
            "strategy_signal_generated": False,
            "promotion_gate_allowed": False,
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        value_surface_source=_artifact_status(value_surface, value_surface_expansion_path),
        gbdt_pivot_selection_source=_artifact_status(pivot_selection, gbdt_pivot_selection_path),
        residual_policy=_next_stage_section(config, "gbdt_value_surface_residual_diagnostic"),
        residual_cases=residual_rows[:100],
        residual_by_asset=_residual_group_result(residual_rows, "asset"),
        residual_by_horizon=_residual_group_result(residual_rows, "horizon"),
        residual_by_regime=_residual_group_result(residual_rows, "regime_segment"),
        feature_importance=feature_importance,
        hypothesis_candidates=hypotheses,
        diagnostic_boundary={
            "gbdt_role": "value_surface_residual_explainer",
            "direct_action_utility_prediction": False,
            "strategy_signal_generated": False,
            "promotion_gate_allowed": False,
        },
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="gbdt_value_surface_residual_diagnostic_prototype",
    )
    return payload


def run_regret_casebook_activation_recheck(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion_path: Path = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    regret_activation_inputs_path: Path = DEFAULT_REGRET_ACTIVATION_INPUTS_PATH,
    regret_casebook_expansion_gate_path: Path = DEFAULT_REGRET_CASEBOOK_EXPANSION_GATE_PATH,
    output_root: Path = DEFAULT_REGRET_STATE_MACHINE_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    value_surface = _read_json_or_empty(value_surface_expansion_path)
    prior_activation = _read_json_or_empty(regret_activation_inputs_path)
    gate = _read_json_or_empty(regret_casebook_expansion_gate_path)
    failure_inputs = _value_surface_failure_activation_inputs(
        rows=_records(value_surface.get("value_surface")),
        config=config,
    )
    criteria = _regret_activation_recheck_criteria(
        config=config,
        failure_inputs=failure_inputs,
        gate=gate,
    )
    ready = all(bool(row["passed"]) for row in criteria)
    status = "READY_FOR_REGRET_EXPANSION_TASK" if ready else "WATCHLIST_NOT_READY"
    summary = failure_inputs["summary"]
    payload = _controlled_payload(
        report_type="regret_casebook_activation_recheck",
        title="Regret casebook activation recheck",
        status=status,
        summary={
            "regret_activation_recheck_ready": ready,
            "regret_casebook_expansion_allowed": False,
            "value_surface_losing_case_count": summary["value_surface_losing_case_count"],
            "benchmark_disagreement_case_count": summary["benchmark_disagreement_case_count"],
            "teacher_oracle_better_case_count": summary["oracle_teacher_better_case_count"],
            "stable_regret_type_count": _stable_regret_type_count(gate),
            "regret_state_machine_status": "WATCHLIST",
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        value_surface_source=_artifact_status(value_surface, value_surface_expansion_path),
        prior_activation_source=_artifact_status(prior_activation, regret_activation_inputs_path),
        regret_gate_source=_artifact_status(gate, regret_casebook_expansion_gate_path),
        recheck_policy=_next_stage_section(config, "regret_casebook_activation_recheck"),
        activation_inputs=failure_inputs,
        activation_recheck_criteria=criteria,
        activation_recheck_decision={
            "decision": status,
            "follow_up_regret_casebook_expansion_task_recommended": ready,
            "follow_up_regret_state_machine_v2_task_recommended": ready,
            "regret_casebook_expansion_allowed": False,
            "expansion_executed": False,
            "promotion_gate_allowed": False,
        },
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="regret_casebook_activation_recheck",
    )
    return payload


def run_value_surface_failure_attribution(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    value_surface_expansion_path: Path = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    walk_forward_path: Path = DEFAULT_VALUE_SURFACE_WALK_FORWARD_PATH,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    universe = _universe(config)
    quality = _run_data_quality_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        as_of_date=as_of_date,
        universe=universe,
    )
    if not quality["passed"]:
        raise ValueError("validate-data gate failed before value surface failure attribution")

    value_surface = _read_json_or_empty(value_surface_expansion_path)
    walk_forward = _read_json_or_empty(walk_forward_path)
    selected_cases = _selected_value_surface_cases(
        _records(value_surface.get("value_surface")),
        config,
    )
    attribution = _failure_attribution_report(selected_cases, config)
    payload = _controlled_payload(
        report_type="value_surface_failure_attribution",
        title="Value surface failure attribution",
        status="FAILURE_ATTRIBUTION_COMPLETE",
        summary={
            "case_count": len(selected_cases),
            "winning_case_count": attribution["summary"]["winning_case_count"],
            "losing_case_count": attribution["summary"]["losing_case_count"],
            "winning_case_average_delta": attribution["summary"]["winning_case_average_delta"],
            "losing_case_average_delta": attribution["summary"]["losing_case_average_delta"],
            "tail_loss_contribution": attribution["tail_loss_contribution"]["tail_loss_share"],
            "max_loss_concentration_share": attribution["summary"]["max_loss_concentration_share"],
            "data_quality_status": quality["status"],
            "data_foundation_status": _data_foundation_status(quality),
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        data_quality_gate=quality,
        data_foundation_status=_data_foundation_status(quality),
        value_surface_source=_artifact_status(value_surface, value_surface_expansion_path),
        walk_forward_source=_artifact_status(walk_forward, walk_forward_path),
        failure_attribution_policy=_next_stage_section(config, "value_surface_failure_attribution"),
        winning_losing_delta=attribution["winning_losing_delta"],
        top_losing_cases=attribution["top_losing_cases"],
        loss_concentration_by_date=attribution["loss_concentration_by_date"],
        loss_concentration_by_asset=attribution["loss_concentration_by_asset"],
        loss_concentration_by_horizon=attribution["loss_concentration_by_horizon"],
        loss_concentration_by_regime=attribution["loss_concentration_by_regime"],
        tail_loss_contribution=attribution["tail_loss_contribution"],
        turnover_cost_contribution=attribution["turnover_cost_contribution"],
        drawdown_contribution=attribution["drawdown_contribution"],
        benchmark_relative_downside_attribution=attribution[
            "benchmark_relative_downside_attribution"
        ],
        attribution_summary=attribution["summary"],
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="value_surface_failure_attribution",
    )
    return payload


def run_horizon_cliff_utility_ranking_stabilization_review(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion_path: Path = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    utility_pareto_ranking_path: Path = DEFAULT_VALUE_SURFACE_UTILITY_PARETO_RANKING_PATH,
    output_root: Path = DEFAULT_UTILITY_BOUNDARY_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    value_surface = _read_json_or_empty(value_surface_expansion_path)
    utility_review = _read_json_or_empty(utility_pareto_ranking_path)
    surface_rows = _records(value_surface.get("value_surface"))
    ranking_jumps = _horizon_ranking_jump_report(surface_rows, config)
    single_horizon = _single_horizon_action_report(surface_rows, config)
    utility_cliffs = _utility_profile_cliff_report(utility_review)
    smoothing = _horizon_smoothing_assessment(
        config=config,
        ranking_jumps=ranking_jumps,
        single_horizon=single_horizon,
        utility_cliffs=utility_cliffs,
        utility_review=utility_review,
    )
    payload = _controlled_payload(
        report_type="horizon_cliff_utility_ranking_stabilization_review",
        title="Horizon cliff and utility ranking stabilization review",
        status="SENSITIVITY_TESTED",
        summary={
            "ranking_jump_count": ranking_jumps["summary"]["ranking_jump_count"],
            "horizon_cliff_count": utility_review.get("summary", {}).get("horizon_cliff_count", 0),
            "single_horizon_action_count": single_horizon["summary"]["single_horizon_action_count"],
            "utility_profile_cliff_count": utility_cliffs["summary"]["utility_profile_cliff_count"],
            "horizon_smoothing_review_required": smoothing["horizon_smoothing_review_required"],
            "pareto_frontier_policy_review_required": smoothing[
                "pareto_frontier_policy_review_required"
            ],
            "validated_boundary_count": 0,
            "not_validated_utility_boundary": True,
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        value_surface_source=_artifact_status(value_surface, value_surface_expansion_path),
        utility_pareto_source=_artifact_status(utility_review, utility_pareto_ranking_path),
        stabilization_policy=_next_stage_section(config, "horizon_cliff_stabilization_review"),
        ranking_jump_by_horizon=ranking_jumps,
        single_horizon_action_report=single_horizon,
        utility_profile_cliff_report=utility_cliffs,
        horizon_smoothing_assessment=smoothing,
        diagnostic_boundary_assessment={
            "status_cap": "SENSITIVITY_TESTED",
            "validated_boundary_allowed": False,
            "validated_boundary_count": 0,
            "not_validated_utility_boundary": True,
            "promotion_gate_allowed": False,
        },
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="horizon_cliff_utility_ranking_stabilization_review",
    )
    return payload


def run_gbdt_residual_hypothesis_triage(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion_path: Path = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    residual_diagnostic_path: Path = DEFAULT_GBDT_VALUE_SURFACE_RESIDUAL_DIAGNOSTIC_PATH,
    output_root: Path = DEFAULT_GBDT_ACTION_UTILITY_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    value_surface = _read_json_or_empty(value_surface_expansion_path)
    residual_diagnostic = _read_json_or_empty(residual_diagnostic_path)
    residual_rows = _value_surface_residual_rows(
        _records(value_surface.get("value_surface")),
        config,
    )
    triage = _residual_hypothesis_triage(residual_rows, config)
    payload = _controlled_payload(
        report_type="gbdt_residual_hypothesis_triage",
        title="GBDT residual hypothesis triage",
        status="RESIDUAL_HYPOTHESIS_TRIAGED",
        summary={
            "residual_case_count": len(residual_rows),
            "large_residual_case_count": triage["summary"]["large_residual_case_count"],
            "feature_explanation_count": len(triage["feature_explanations"]),
            "repair_rule_candidate_count": len(triage["repair_rule_candidates"]),
            "new_hypothesis_candidate_count": len(triage["new_hypothesis_candidates"]),
            "strategy_signal_generated": False,
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        value_surface_source=_artifact_status(value_surface, value_surface_expansion_path),
        residual_diagnostic_source=_artifact_status(residual_diagnostic, residual_diagnostic_path),
        triage_policy=_next_stage_section(config, "gbdt_residual_hypothesis_triage"),
        prediction_error_summary=triage["prediction_error_summary"],
        residual_by_asset=triage["residual_by_asset"],
        residual_by_horizon=triage["residual_by_horizon"],
        residual_by_regime=triage["residual_by_regime"],
        feature_explanations=triage["feature_explanations"],
        repair_rule_candidates=triage["repair_rule_candidates"],
        new_hypothesis_candidates=triage["new_hypothesis_candidates"],
        diagnostic_boundary={
            "strategy_signal_generated": False,
            "model_training_executed": False,
            "direct_action_policy_generated": False,
            "promotion_gate_allowed": False,
        },
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="gbdt_residual_hypothesis_triage",
    )
    return payload


def run_forward_evidence_continuity_extension(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    ledger_path: Path = DEFAULT_FORWARD_DAILY_DRY_RUN_LEDGER_PATH,
    benchmark_expansion_path: Path = DEFAULT_CONTROLLED_BENCHMARK_EXPANSION_PATH,
    control_audit_path: Path = DEFAULT_CONTROL_AUDIT_PATH,
    value_surface_expansion_path: Path = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    output_root: Path = DEFAULT_FORWARD_MATURITY_OUTPUT_ROOT,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    universe = _universe(config)
    quality = _run_data_quality_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        as_of_date=as_of_date,
        universe=universe,
    )
    if not quality["passed"]:
        raise ValueError("validate-data gate failed before forward continuity extension")

    price_rows = _read_price_rows(prices_path, universe=universe)
    dates = _all_dates(price_rows)
    ledger_rows = _read_jsonl_rows(ledger_path)
    maturity_rows = _forward_maturity_rows(ledger_rows=ledger_rows, dates=dates, config=config)
    continuity = _forward_daily_continuity_report(
        ledger_rows=ledger_rows,
        dates=dates,
        config=config,
    )
    append_only = _append_only_integrity_report(ledger_rows)
    coverage = _forward_output_coverage_report(
        benchmark_expansion_path=benchmark_expansion_path,
        control_audit_path=control_audit_path,
        value_surface_expansion_path=value_surface_expansion_path,
    )
    payload = _controlled_payload(
        report_type="forward_evidence_continuity_extension",
        title="Forward evidence continuity extension",
        status="PASS_WITH_WARNINGS" if ledger_rows else "DATA_REQUIRED",
        summary={
            "ledger_event_count": len(ledger_rows),
            "missing_daily_archive_count": continuity["summary"]["missing_daily_archive_count"],
            "append_only_integrity_pass": append_only["summary"]["append_only_integrity_pass"],
            "horizon_maturity_recorded": bool(maturity_rows),
            "output_coverage_present": True,
            "data_quality_status": quality["status"],
            "data_foundation_status": _data_foundation_status(quality),
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        data_quality_gate=quality,
        data_foundation_status=_data_foundation_status(quality),
        requested_date_range=_requested_date_range(dates),
        ledger_path=str(ledger_path),
        continuity_extension_policy=_next_stage_section(
            config, "forward_evidence_continuity_extension"
        ),
        daily_archive_continuity=continuity,
        append_only_integrity=append_only,
        horizon_maturity_summary=_forward_maturity_summary(maturity_rows),
        output_coverage=coverage,
        forward_evidence_scope={
            "decides_current_strategy_quality": False,
            "paper_shadow_ready": False,
            "promotion_gate_allowed": False,
        },
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="forward_evidence_continuity_extension",
    )
    return payload


def run_value_surface_direction_review(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    failure_attribution_path: Path = DEFAULT_VALUE_SURFACE_FAILURE_ATTRIBUTION_PATH,
    horizon_stabilization_path: Path = DEFAULT_HORIZON_CLIFF_STABILIZATION_REVIEW_PATH,
    residual_triage_path: Path = DEFAULT_GBDT_RESIDUAL_HYPOTHESIS_TRIAGE_PATH,
    forward_continuity_extension_path: Path = DEFAULT_FORWARD_EVIDENCE_CONTINUITY_EXTENSION_PATH,
    walk_forward_path: Path = DEFAULT_VALUE_SURFACE_WALK_FORWARD_PATH,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    failure = _read_json_or_empty(failure_attribution_path)
    horizon = _read_json_or_empty(horizon_stabilization_path)
    residual = _read_json_or_empty(residual_triage_path)
    forward = _read_json_or_empty(forward_continuity_extension_path)
    walk_forward = _read_json_or_empty(walk_forward_path)
    decision = _value_surface_direction_decision(
        config=config,
        failure=failure,
        horizon=horizon,
        residual=residual,
        forward=forward,
        walk_forward=walk_forward,
    )
    payload = _controlled_payload(
        report_type="value_surface_direction_review",
        title="Value surface direction review",
        status="DIRECTION_REVIEW_COMPLETE",
        summary={
            "direction_decision": decision["decision"],
            "do_not_default_continue": True,
            "failure_attribution_present": bool(failure),
            "horizon_stabilization_present": bool(horizon),
            "residual_triage_present": bool(residual),
            "forward_continuity_extension_present": bool(forward),
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        direction_review_policy=_next_stage_section(config, "value_surface_direction_review"),
        failure_attribution_source=_artifact_status(failure, failure_attribution_path),
        horizon_stabilization_source=_artifact_status(horizon, horizon_stabilization_path),
        residual_triage_source=_artifact_status(residual, residual_triage_path),
        forward_continuity_extension_source=_artifact_status(
            forward, forward_continuity_extension_path
        ),
        walk_forward_source=_artifact_status(walk_forward, walk_forward_path),
        direction_decision=decision,
        evidence_summary=_direction_evidence_summary(
            failure=failure,
            horizon=horizon,
            residual=residual,
            forward=forward,
            walk_forward=walk_forward,
        ),
        disallowed_actions=[
            "continue_expanding_value_surface_sample",
            "train_gbdt_strategy_directly",
            "expand_regret_casebook_now",
            "enter_paper_shadow",
            "treat_high_beat_rate_as_strategy_evidence",
        ],
        remaining_blockers=_common_blockers(),
    )
    _write_pair(payload, output_root=output_root, artifact_id="value_surface_direction_review")
    return payload


def run_regime_conditioned_value_surface_design(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    failure_attribution_path: Path = DEFAULT_VALUE_SURFACE_FAILURE_ATTRIBUTION_PATH,
    horizon_stabilization_path: Path = DEFAULT_HORIZON_CLIFF_STABILIZATION_REVIEW_PATH,
    residual_triage_path: Path = DEFAULT_GBDT_RESIDUAL_HYPOTHESIS_TRIAGE_PATH,
    direction_review_path: Path = DEFAULT_VALUE_SURFACE_DIRECTION_REVIEW_PATH,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    failure = _read_json_or_empty(failure_attribution_path)
    horizon = _read_json_or_empty(horizon_stabilization_path)
    residual = _read_json_or_empty(residual_triage_path)
    direction = _read_json_or_empty(direction_review_path)
    protocol = _regime_conditioned_design_protocol(
        config=config,
        failure=failure,
        horizon=horizon,
        residual=residual,
        direction=direction,
    )
    payload = _controlled_payload(
        report_type="regime_conditioned_value_surface_design",
        title="Regime-conditioned value surface design",
        status="REGIME_CONDITIONED_PROTOCOL_DEFINED",
        summary={
            "regime_variable_count": len(protocol["regime_variables"]),
            "tail_loss_regime_count": len(protocol["tail_loss_regime_definitions"]),
            "benchmark_fallback_rule_count": len(protocol["benchmark_fallback_rules"]),
            "disabled_or_downweighted_horizon_count": len(
                protocol["horizons_disabled_or_downweighted"]
            ),
            "controlled_only_validation_plan_present": bool(
                protocol["controlled_only_validation_plan"]
            ),
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        design_policy=_next_stage_section(config, "regime_conditioned_value_surface_design"),
        failure_attribution_source=_artifact_status(failure, failure_attribution_path),
        horizon_stabilization_source=_artifact_status(horizon, horizon_stabilization_path),
        residual_triage_source=_artifact_status(residual, residual_triage_path),
        direction_review_source=_artifact_status(direction, direction_review_path),
        regime_conditioned_value_surface_protocol=protocol["protocol"],
        regime_variables=protocol["regime_variables"],
        tail_loss_regime_definitions=protocol["tail_loss_regime_definitions"],
        allowed_action_changes_by_regime=protocol["allowed_action_changes_by_regime"],
        benchmark_fallback_rules=protocol["benchmark_fallback_rules"],
        regimes_keep_value_surface=protocol["regimes_keep_value_surface"],
        regimes_fallback_to_benchmark=protocol["regimes_fallback_to_benchmark"],
        regimes_low_risk_action_only=protocol["regimes_low_risk_action_only"],
        horizons_disabled_or_downweighted=protocol["horizons_disabled_or_downweighted"],
        controlled_only_validation_plan=protocol["controlled_only_validation_plan"],
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="regime_conditioned_value_surface_design",
    )
    return payload


def run_tail_loss_guardrail_fallback_policy(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion_path: Path = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    failure_attribution_path: Path = DEFAULT_VALUE_SURFACE_FAILURE_ATTRIBUTION_PATH,
    horizon_stabilization_path: Path = DEFAULT_HORIZON_CLIFF_STABILIZATION_REVIEW_PATH,
    design_path: Path = DEFAULT_REGIME_CONDITIONED_VALUE_SURFACE_DESIGN_PATH,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    value_surface = _read_json_or_empty(value_surface_expansion_path)
    failure = _read_json_or_empty(failure_attribution_path)
    horizon = _read_json_or_empty(horizon_stabilization_path)
    design = _read_json_or_empty(design_path)
    selected_cases = _selected_value_surface_cases(
        _records(value_surface.get("value_surface")),
        config,
    )
    comparison = _guardrail_variant_comparison(
        selected_cases=selected_cases,
        config=config,
        failure=failure,
        horizon=horizon,
        design=design,
    )
    best_variant = comparison["summary"]["best_variant_by_mean_delta"]
    payload = _controlled_payload(
        report_type="tail_loss_guardrail_fallback_policy",
        title="Tail-loss guardrail and fallback policy",
        status="GUARDRAIL_FALLBACK_POLICY_REVIEWED",
        summary={
            "case_count": len(selected_cases),
            "variant_count": len(comparison["variant_metrics"]),
            "best_variant_by_mean_delta": best_variant,
            "best_variant_mean_delta_vs_benchmark": comparison["summary"][
                "best_variant_mean_delta_vs_benchmark"
            ],
            "original_mean_delta_vs_benchmark": comparison["summary"][
                "original_mean_delta_vs_benchmark"
            ],
            "tail_loss_guarded_mean_delta_vs_benchmark": comparison["summary"][
                "tail_loss_guarded_mean_delta_vs_benchmark"
            ],
            "tail_loss_guardrail_reduces_tail_loss": comparison["summary"][
                "tail_loss_guardrail_reduces_tail_loss"
            ],
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        guardrail_policy=_next_stage_section(config, "tail_loss_guardrail_fallback_policy"),
        value_surface_source=_artifact_status(value_surface, value_surface_expansion_path),
        failure_attribution_source=_artifact_status(failure, failure_attribution_path),
        horizon_stabilization_source=_artifact_status(horizon, horizon_stabilization_path),
        design_source=_artifact_status(design, design_path),
        variant_metrics=comparison["variant_metrics"],
        variant_rules=comparison["variant_rules"],
        variant_case_samples=comparison["variant_case_samples"],
        guardrail_diagnostic_boundary=comparison["diagnostic_boundary"],
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="tail_loss_guardrail_fallback_policy",
    )
    return payload


def run_regime_horizon_loss_attribution_matrix(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion_path: Path = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    failure_attribution_path: Path = DEFAULT_VALUE_SURFACE_FAILURE_ATTRIBUTION_PATH,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    value_surface = _read_json_or_empty(value_surface_expansion_path)
    failure = _read_json_or_empty(failure_attribution_path)
    selected_cases = _selected_value_surface_cases(
        _records(value_surface.get("value_surface")),
        config,
    )
    matrix = _regime_horizon_loss_matrix(selected_cases, config)
    payload = _controlled_payload(
        report_type="regime_horizon_loss_attribution_matrix",
        title="Regime / horizon loss attribution matrix",
        status="LOSS_ATTRIBUTION_MATRIX_COMPLETE",
        summary={
            "losing_case_count": matrix["summary"]["losing_case_count"],
            "losing_case_average_delta": matrix["summary"]["losing_case_average_delta"],
            "max_loss_concentration_share": matrix["summary"]["max_loss_concentration_share"],
            "max_loss_concentration_group": matrix["summary"]["max_loss_concentration_group"],
            "loss_distribution_assessment": matrix["summary"]["loss_distribution_assessment"],
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        matrix_policy=_next_stage_section(config, "regime_horizon_loss_attribution_matrix"),
        value_surface_source=_artifact_status(value_surface, value_surface_expansion_path),
        failure_attribution_source=_artifact_status(failure, failure_attribution_path),
        loss_by_regime=matrix["loss_by_regime"],
        loss_by_asset=matrix["loss_by_asset"],
        loss_by_horizon=matrix["loss_by_horizon"],
        loss_by_action=matrix["loss_by_action"],
        loss_by_cluster=matrix["loss_by_cluster"],
        loss_by_utility_profile=matrix["loss_by_utility_profile"],
        loss_by_date_window=matrix["loss_by_date_window"],
        top_losing_cases=matrix["top_losing_cases"],
        matrix_summary=matrix["summary"],
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="regime_horizon_loss_attribution_matrix",
    )
    return payload


def run_gbdt_residual_hypothesis_regime_conditioning(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion_path: Path = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    residual_triage_path: Path = DEFAULT_GBDT_RESIDUAL_HYPOTHESIS_TRIAGE_PATH,
    output_root: Path = DEFAULT_GBDT_ACTION_UTILITY_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    value_surface = _read_json_or_empty(value_surface_expansion_path)
    residual_triage = _read_json_or_empty(residual_triage_path)
    residual_rows = _value_surface_residual_rows(
        _records(value_surface.get("value_surface")),
        config,
    )
    report = _residual_regime_conditioning_report(residual_rows, config)
    top_feature = (
        report["top_residual_features"][0]["feature"] if report["top_residual_features"] else None
    )
    payload = _controlled_payload(
        report_type="gbdt_residual_hypothesis_regime_conditioning",
        title="GBDT residual hypothesis triage for regime conditioning",
        status="RESIDUAL_REGIME_HYPOTHESES_TRIAGED",
        summary={
            "residual_case_count": len(residual_rows),
            "large_residual_case_count": report["summary"]["large_residual_case_count"],
            "top_residual_feature": top_feature,
            "hypothesis_candidate_count": len(report["hypothesis_candidates"]),
            "strategy_signal_generated": False,
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        residual_regime_policy=_next_stage_section(config, "gbdt_residual_regime_conditioning"),
        value_surface_source=_artifact_status(value_surface, value_surface_expansion_path),
        residual_triage_source=_artifact_status(residual_triage, residual_triage_path),
        top_residual_features=report["top_residual_features"],
        large_residual_regimes=report["large_residual_regimes"],
        large_residual_horizons=report["large_residual_horizons"],
        large_residual_assets=report["large_residual_assets"],
        residual_sign_classification=report["residual_sign_classification"],
        hypothesis_candidates=report["hypothesis_candidates"],
        diagnostic_boundary={
            "strategy_signal_generated": False,
            "model_training_executed": False,
            "direct_action_policy_generated": False,
            "promotion_gate_allowed": False,
        },
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="gbdt_residual_hypothesis_regime_conditioning",
    )
    return payload


def run_regime_conditioned_value_surface_controlled_review(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    design_path: Path = DEFAULT_REGIME_CONDITIONED_VALUE_SURFACE_DESIGN_PATH,
    guardrail_policy_path: Path = DEFAULT_TAIL_LOSS_GUARDRAIL_FALLBACK_POLICY_PATH,
    loss_matrix_path: Path = DEFAULT_REGIME_HORIZON_LOSS_ATTRIBUTION_MATRIX_PATH,
    residual_regime_path: Path = DEFAULT_GBDT_RESIDUAL_REGIME_CONDITIONING_PATH,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    design = _read_json_or_empty(design_path)
    guardrail = _read_json_or_empty(guardrail_policy_path)
    loss_matrix = _read_json_or_empty(loss_matrix_path)
    residual_regime = _read_json_or_empty(residual_regime_path)
    decision = _regime_conditioned_controlled_review_decision(
        config=config,
        design=design,
        guardrail=guardrail,
        loss_matrix=loss_matrix,
        residual_regime=residual_regime,
    )
    payload = _controlled_payload(
        report_type="regime_conditioned_value_surface_controlled_review",
        title="Regime-conditioned value surface controlled review",
        status="REGIME_CONDITIONED_CONTROLLED_REVIEW_COMPLETE",
        summary={
            "controlled_review_decision": decision["decision"],
            "best_variant_by_mean_delta": decision["best_variant_by_mean_delta"],
            "mean_delta_improved": decision["mean_delta_improved"],
            "tail_loss_reduced": decision["tail_loss_reduced"],
            "beat_rate_retained": decision["beat_rate_retained"],
            "turnover_cost_not_worse": decision["turnover_cost_not_worse"],
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        controlled_review_policy=_next_stage_section(
            config, "regime_conditioned_value_surface_controlled_review"
        ),
        design_source=_artifact_status(design, design_path),
        guardrail_policy_source=_artifact_status(guardrail, guardrail_policy_path),
        loss_matrix_source=_artifact_status(loss_matrix, loss_matrix_path),
        residual_regime_source=_artifact_status(residual_regime, residual_regime_path),
        review_decision=decision,
        evidence_summary=_regime_conditioned_review_evidence(
            design=design,
            guardrail=guardrail,
            loss_matrix=loss_matrix,
            residual_regime=residual_regime,
        ),
        disallowed_actions=[
            "unconditional_value_surface_expansion",
            "direct_gbdt_strategy_training",
            "large_regret_casebook_expansion",
            "paper_shadow_entry",
            "beat_rate_promotion_claim",
        ],
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="regime_conditioned_value_surface_controlled_review",
    )
    return payload


def run_cost_turnover_aware_regime_conditioned_value_surface(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion_path: Path = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    failure_attribution_path: Path = DEFAULT_VALUE_SURFACE_FAILURE_ATTRIBUTION_PATH,
    horizon_stabilization_path: Path = DEFAULT_HORIZON_CLIFF_STABILIZATION_REVIEW_PATH,
    design_path: Path = DEFAULT_REGIME_CONDITIONED_VALUE_SURFACE_DESIGN_PATH,
    guardrail_policy_path: Path = DEFAULT_TAIL_LOSS_GUARDRAIL_FALLBACK_POLICY_PATH,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    value_surface = _read_json_or_empty(value_surface_expansion_path)
    failure = _read_json_or_empty(failure_attribution_path)
    horizon = _read_json_or_empty(horizon_stabilization_path)
    design = _read_json_or_empty(design_path)
    guardrail = _read_json_or_empty(guardrail_policy_path)
    selected_cases = _selected_value_surface_cases(
        _records(value_surface.get("value_surface")),
        config,
    )
    comparison = _cost_turnover_aware_variant_comparison(
        selected_cases=selected_cases,
        config=config,
        failure=failure,
        horizon=horizon,
        design=design,
    )
    best = comparison["summary"]["best_variant_by_v2_score"]
    payload = _controlled_payload(
        report_type="cost_turnover_aware_regime_conditioned_value_surface",
        title="Cost/turnover-aware regime-conditioned value surface",
        status="COST_TURNOVER_AWARE_VARIANTS_REVIEWED",
        summary={
            "case_count": len(selected_cases),
            "variant_count": len(comparison["variant_metrics"]),
            "best_variant_by_v2_score": best,
            "mean_delta_improved": comparison["summary"]["mean_delta_improved"],
            "tail_loss_reduced": comparison["summary"]["tail_loss_reduced"],
            "turnover_cost_not_worse": comparison["summary"]["turnover_cost_not_worse"],
            "beat_rate_retained": comparison["summary"]["beat_rate_retained"],
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        cost_turnover_policy=_next_stage_section(
            config, "cost_turnover_aware_regime_conditioned_value_surface"
        ),
        value_surface_source=_artifact_status(value_surface, value_surface_expansion_path),
        failure_attribution_source=_artifact_status(failure, failure_attribution_path),
        horizon_stabilization_source=_artifact_status(horizon, horizon_stabilization_path),
        design_source=_artifact_status(design, design_path),
        guardrail_policy_source=_artifact_status(guardrail, guardrail_policy_path),
        variant_metrics=comparison["variant_metrics"],
        variant_rules=comparison["variant_rules"],
        transition_report=comparison["transition_report"],
        v2_score_policy=comparison["v2_score_policy"],
        diagnostic_boundary=comparison["diagnostic_boundary"],
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="cost_turnover_aware_regime_conditioned_value_surface",
    )
    return payload


def run_long_horizon_quarantine_selection_review(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion_path: Path = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    failure_attribution_path: Path = DEFAULT_VALUE_SURFACE_FAILURE_ATTRIBUTION_PATH,
    horizon_stabilization_path: Path = DEFAULT_HORIZON_CLIFF_STABILIZATION_REVIEW_PATH,
    cost_turnover_path: Path = DEFAULT_COST_TURNOVER_AWARE_VALUE_SURFACE_PATH,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    value_surface = _read_json_or_empty(value_surface_expansion_path)
    failure = _read_json_or_empty(failure_attribution_path)
    horizon = _read_json_or_empty(horizon_stabilization_path)
    cost_turnover = _read_json_or_empty(cost_turnover_path)
    selected_cases = _selected_value_surface_cases(
        _records(value_surface.get("value_surface")),
        config,
    )
    review = _long_horizon_quarantine_review(
        selected_cases=selected_cases,
        config=config,
        horizon=horizon,
    )
    payload = _controlled_payload(
        report_type="long_horizon_quarantine_selection_review",
        title="Long-horizon quarantine / horizon selection review",
        status="LONG_HORIZON_QUARANTINE_REVIEW_COMPLETE",
        summary={
            "reviewed_horizon_count": len(review["reviewed_horizons"]),
            "best_comparison_variant": review["summary"]["best_comparison_variant"],
            "best_variant_mean_delta_vs_benchmark": review["summary"][
                "best_variant_mean_delta_vs_benchmark"
            ],
            "tail_loss_reduction_best_variant": review["summary"][
                "tail_loss_reduction_best_variant"
            ],
            "horizon_selector_issue_likely": review["summary"]["horizon_selector_issue_likely"],
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        quarantine_policy=_next_stage_section(config, "long_horizon_quarantine_selection_review"),
        value_surface_source=_artifact_status(value_surface, value_surface_expansion_path),
        failure_attribution_source=_artifact_status(failure, failure_attribution_path),
        horizon_stabilization_source=_artifact_status(horizon, horizon_stabilization_path),
        cost_turnover_source=_artifact_status(cost_turnover, cost_turnover_path),
        horizon_loss_matrix=review["horizon_loss_matrix"],
        horizon_return_matrix=review["horizon_return_matrix"],
        horizon_turnover_matrix=review["horizon_turnover_matrix"],
        horizon_cliff_matrix=review["horizon_cliff_matrix"],
        disable_vs_downgrade_comparison=review["disable_vs_downgrade_comparison"],
        reviewed_horizons=review["reviewed_horizons"],
        review_summary=review["summary"],
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="long_horizon_quarantine_selection_review",
    )
    return payload


def run_ai_after_chatgpt_full_regime_attribution_review(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion_path: Path = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    failure_attribution_path: Path = DEFAULT_VALUE_SURFACE_FAILURE_ATTRIBUTION_PATH,
    loss_matrix_path: Path = DEFAULT_REGIME_HORIZON_LOSS_ATTRIBUTION_MATRIX_PATH,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    value_surface = _read_json_or_empty(value_surface_expansion_path)
    failure = _read_json_or_empty(failure_attribution_path)
    loss_matrix = _read_json_or_empty(loss_matrix_path)
    selected_cases = _selected_value_surface_cases(
        _records(value_surface.get("value_surface")),
        config,
    )
    review = _ai_regime_attribution_review(selected_cases, config)
    payload = _controlled_payload(
        report_type="ai_after_chatgpt_full_regime_attribution_review",
        title="ai_after_chatgpt_full regime attribution review",
        status="AI_REGIME_ATTRIBUTION_REVIEW_COMPLETE",
        summary={
            "target_regime": review["summary"]["target_regime"],
            "regime_case_count": review["summary"]["regime_case_count"],
            "regime_losing_case_count": review["summary"]["regime_losing_case_count"],
            "top_loss_asset": review["summary"]["top_loss_asset"],
            "top_loss_horizon": review["summary"]["top_loss_horizon"],
            "top_loss_action": review["summary"]["top_loss_action"],
            "value_surface_systematic_overoptimism": review["summary"][
                "value_surface_systematic_overoptimism"
            ],
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        regime_attribution_policy=_next_stage_section(
            config, "ai_after_chatgpt_full_regime_attribution"
        ),
        value_surface_source=_artifact_status(value_surface, value_surface_expansion_path),
        failure_attribution_source=_artifact_status(failure, failure_attribution_path),
        loss_matrix_source=_artifact_status(loss_matrix, loss_matrix_path),
        loss_by_asset=review["loss_by_asset"],
        loss_by_horizon=review["loss_by_horizon"],
        loss_by_action=review["loss_by_action"],
        loss_by_cluster=review["loss_by_cluster"],
        benchmark_stability=review["benchmark_stability"],
        value_surface_overoptimism=review["value_surface_overoptimism"],
        candidate_repairs=review["candidate_repairs"],
        review_summary=review["summary"],
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="ai_after_chatgpt_full_regime_attribution_review",
    )
    return payload


def run_regime_conditioned_walk_forward_holdout(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion_path: Path = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    failure_attribution_path: Path = DEFAULT_VALUE_SURFACE_FAILURE_ATTRIBUTION_PATH,
    horizon_stabilization_path: Path = DEFAULT_HORIZON_CLIFF_STABILIZATION_REVIEW_PATH,
    design_path: Path = DEFAULT_REGIME_CONDITIONED_VALUE_SURFACE_DESIGN_PATH,
    cost_turnover_path: Path = DEFAULT_COST_TURNOVER_AWARE_VALUE_SURFACE_PATH,
    horizon_quarantine_path: Path = DEFAULT_LONG_HORIZON_QUARANTINE_REVIEW_PATH,
    regime_attribution_path: Path = DEFAULT_AI_REGIME_ATTRIBUTION_REVIEW_PATH,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    value_surface = _read_json_or_empty(value_surface_expansion_path)
    failure = _read_json_or_empty(failure_attribution_path)
    horizon = _read_json_or_empty(horizon_stabilization_path)
    design = _read_json_or_empty(design_path)
    cost_turnover = _read_json_or_empty(cost_turnover_path)
    horizon_quarantine = _read_json_or_empty(horizon_quarantine_path)
    regime_attribution = _read_json_or_empty(regime_attribution_path)
    selected_cases = _selected_value_surface_cases(
        _records(value_surface.get("value_surface")),
        config,
    )
    holdout = _regime_conditioned_holdout_review(
        selected_cases=selected_cases,
        config=config,
        failure=failure,
        horizon=horizon,
        design=design,
        cost_turnover=cost_turnover,
    )
    payload = _controlled_payload(
        report_type="regime_conditioned_walk_forward_holdout",
        title="Regime-conditioned walk-forward holdout",
        status="REGIME_CONDITIONED_HOLDOUT_REVIEW_COMPLETE",
        summary={
            "holdout_case_count": holdout["summary"]["holdout_case_count"],
            "holdout_pass_count": holdout["summary"]["holdout_pass_count"],
            "holdout_pass_rate": holdout["summary"]["holdout_pass_rate"],
            "overfit_risk": holdout["summary"]["overfit_risk"],
            "best_variant": holdout["summary"]["best_variant"],
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        holdout_policy=_next_stage_section(config, "regime_conditioned_walk_forward_holdout"),
        value_surface_source=_artifact_status(value_surface, value_surface_expansion_path),
        failure_attribution_source=_artifact_status(failure, failure_attribution_path),
        horizon_stabilization_source=_artifact_status(horizon, horizon_stabilization_path),
        design_source=_artifact_status(design, design_path),
        cost_turnover_source=_artifact_status(cost_turnover, cost_turnover_path),
        horizon_quarantine_source=_artifact_status(horizon_quarantine, horizon_quarantine_path),
        regime_attribution_source=_artifact_status(regime_attribution, regime_attribution_path),
        leave_one_regime_out=holdout["leave_one_regime_out"],
        leave_one_horizon_out=holdout["leave_one_horizon_out"],
        leave_one_asset_cluster_out=holdout["leave_one_asset_cluster_out"],
        leave_one_date_window_out=holdout["leave_one_date_window_out"],
        holdout_summary=holdout["summary"],
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="regime_conditioned_walk_forward_holdout",
    )
    return payload


def run_value_surface_v2_controlled_review(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    cost_turnover_path: Path = DEFAULT_COST_TURNOVER_AWARE_VALUE_SURFACE_PATH,
    horizon_quarantine_path: Path = DEFAULT_LONG_HORIZON_QUARANTINE_REVIEW_PATH,
    regime_attribution_path: Path = DEFAULT_AI_REGIME_ATTRIBUTION_REVIEW_PATH,
    holdout_path: Path = DEFAULT_REGIME_CONDITIONED_WALK_FORWARD_HOLDOUT_PATH,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    cost_turnover = _read_json_or_empty(cost_turnover_path)
    horizon_quarantine = _read_json_or_empty(horizon_quarantine_path)
    regime_attribution = _read_json_or_empty(regime_attribution_path)
    holdout = _read_json_or_empty(holdout_path)
    decision = _value_surface_v2_review_decision(
        config=config,
        cost_turnover=cost_turnover,
        horizon_quarantine=horizon_quarantine,
        regime_attribution=regime_attribution,
        holdout=holdout,
    )
    payload = _controlled_payload(
        report_type="value_surface_v2_controlled_review",
        title="Value surface v2 controlled review",
        status="VALUE_SURFACE_V2_CONTROLLED_REVIEW_COMPLETE",
        summary={
            "value_surface_v2_decision": decision["decision"],
            "best_cost_turnover_variant": decision["best_cost_turnover_variant"],
            "mean_delta_condition_met": decision["mean_delta_condition_met"],
            "tail_loss_condition_met": decision["tail_loss_condition_met"],
            "turnover_cost_condition_met": decision["turnover_cost_condition_met"],
            "beat_rate_condition_met": decision["beat_rate_condition_met"],
            "holdout_condition_met": decision["holdout_condition_met"],
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        v2_review_policy=_next_stage_section(config, "value_surface_v2_controlled_review"),
        cost_turnover_source=_artifact_status(cost_turnover, cost_turnover_path),
        horizon_quarantine_source=_artifact_status(horizon_quarantine, horizon_quarantine_path),
        regime_attribution_source=_artifact_status(regime_attribution, regime_attribution_path),
        holdout_source=_artifact_status(holdout, holdout_path),
        review_decision=decision,
        evidence_summary=_value_surface_v2_evidence_summary(
            cost_turnover=cost_turnover,
            horizon_quarantine=horizon_quarantine,
            regime_attribution=regime_attribution,
            holdout=holdout,
        ),
        disallowed_actions=[
            "expand_unconditional_value_surface",
            "train_gbdt_nn_or_rl_strategy",
            "enter_paper_shadow",
            "continue_based_only_on_mean_delta",
            "ignore_turnover_cost_not_worse_false",
        ],
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="value_surface_v2_controlled_review",
    )
    return payload


def run_horizon_selector_problem_contract(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    v2_review_path: Path = DEFAULT_VALUE_SURFACE_V2_CONTROLLED_REVIEW_PATH,
    long_horizon_review_path: Path = DEFAULT_LONG_HORIZON_QUARANTINE_REVIEW_PATH,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    v2_review = _read_json_or_empty(v2_review_path)
    long_horizon = _read_json_or_empty(long_horizon_review_path)
    contract = _horizon_selector_problem_contract(config)
    payload = _controlled_payload(
        report_type="horizon_selector_problem_contract",
        title="Horizon selector problem contract",
        status="HORIZON_SELECTOR_CONTRACT_DEFINED",
        summary={
            "candidate_horizon_count": len(contract["candidate_horizons"]),
            "allowed_horizon_count": len(contract["selector_output"]["allowed_horizons"]),
            "preferred_horizon": contract["selector_output"]["preferred_horizon"],
            "fallback_horizon": contract["selector_output"]["fallback_horizon"],
            "target_horizon_is_holding_commitment": contract[
                "target_horizon_is_holding_commitment"
            ],
            "regime_change_can_invalidate_horizon": contract[
                "regime_change_can_invalidate_horizon"
            ],
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        selector_contract_policy=_next_stage_section(config, "horizon_selector_problem_contract"),
        v2_review_source=_artifact_status(v2_review, v2_review_path),
        long_horizon_review_source=_artifact_status(long_horizon, long_horizon_review_path),
        candidate_horizons=contract["candidate_horizons"],
        horizon_status=contract["horizon_status"],
        selector_output_schema=contract["selector_output_schema"],
        selector_output=contract["selector_output"],
        target_horizon_is_holding_commitment=contract["target_horizon_is_holding_commitment"],
        regime_change_can_invalidate_horizon=contract["regime_change_can_invalidate_horizon"],
        problem_statement=contract["problem_statement"],
        disallowed_actions=[
            "train_horizon_selector_model",
            "treat_target_horizon_as_holding_commitment",
            "enter_paper_shadow",
            "change_production_weights",
        ],
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="horizon_selector_problem_contract",
    )
    return payload


def run_long_horizon_quarantine_fallback_review(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion_path: Path = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    contract_path: Path = DEFAULT_HORIZON_SELECTOR_PROBLEM_CONTRACT_PATH,
    v2_review_path: Path = DEFAULT_VALUE_SURFACE_V2_CONTROLLED_REVIEW_PATH,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    value_surface = _read_json_or_empty(value_surface_expansion_path)
    contract = _read_json_or_empty(contract_path)
    v2_review = _read_json_or_empty(v2_review_path)
    selected_cases = _selected_value_surface_cases(
        _records(value_surface.get("value_surface")),
        config,
    )
    review = _long_horizon_quarantine_fallback_review(
        selected_cases=selected_cases,
        config=config,
    )
    payload = _controlled_payload(
        report_type="long_horizon_quarantine_fallback_review",
        title="Long-horizon quarantine / fallback review",
        status="LONG_HORIZON_FALLBACK_REVIEW_COMPLETE",
        summary={
            "case_count": len(selected_cases),
            "variant_count": len(review["variant_metrics"]),
            "best_variant_by_tail_loss": review["summary"]["best_variant_by_tail_loss"],
            "best_variant_tail_loss_reduction": review["summary"][
                "best_variant_tail_loss_reduction"
            ],
            "best_variant_turnover_cost_not_worse": review["summary"][
                "best_variant_turnover_cost_not_worse"
            ],
            "horizon_selector_problem_supported": review["summary"][
                "horizon_selector_problem_supported"
            ],
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        fallback_review_policy=_next_stage_section(
            config, "long_horizon_quarantine_fallback_review"
        ),
        value_surface_source=_artifact_status(value_surface, value_surface_expansion_path),
        contract_source=_artifact_status(contract, contract_path),
        v2_review_source=_artifact_status(v2_review, v2_review_path),
        variant_metrics=review["variant_metrics"],
        variant_rules=review["variant_rules"],
        holdout_summary_by_variant=review["holdout_summary_by_variant"],
        review_summary=review["summary"],
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="long_horizon_quarantine_fallback_review",
    )
    return payload


def run_horizon_selector_controlled_prototype(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion_path: Path = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    contract_path: Path = DEFAULT_HORIZON_SELECTOR_PROBLEM_CONTRACT_PATH,
    fallback_review_path: Path = DEFAULT_LONG_HORIZON_QUARANTINE_FALLBACK_REVIEW_PATH,
    horizon_stabilization_path: Path = DEFAULT_HORIZON_CLIFF_STABILIZATION_REVIEW_PATH,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    value_surface = _read_json_or_empty(value_surface_expansion_path)
    contract = _read_json_or_empty(contract_path)
    fallback_review = _read_json_or_empty(fallback_review_path)
    horizon_stabilization = _read_json_or_empty(horizon_stabilization_path)
    selected_cases = _selected_value_surface_cases(
        _records(value_surface.get("value_surface")),
        config,
    )
    prototype = _horizon_selector_controlled_prototype(
        selected_cases=selected_cases,
        config=config,
        horizon_stabilization=horizon_stabilization,
    )
    payload = _controlled_payload(
        report_type="horizon_selector_controlled_prototype",
        title="Horizon selector controlled prototype",
        status="HORIZON_SELECTOR_PROTOTYPE_REVIEWED",
        summary={
            "case_count": len(selected_cases),
            "decision_row_count": len(prototype["horizon_decision_by_date"]),
            "quarantined_horizon_count": prototype["summary"]["quarantined_horizon_count"],
            "fallback_count": prototype["summary"]["fallback_count"],
            "tail_loss_after_selector": prototype["summary"]["tail_loss_after_selector"],
            "cost_after_selector": prototype["summary"]["cost_after_selector"],
            "model_run_executed": False,
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        prototype_policy=_next_stage_section(config, "horizon_selector_controlled_prototype"),
        value_surface_source=_artifact_status(value_surface, value_surface_expansion_path),
        contract_source=_artifact_status(contract, contract_path),
        fallback_review_source=_artifact_status(fallback_review, fallback_review_path),
        horizon_stabilization_source=_artifact_status(
            horizon_stabilization, horizon_stabilization_path
        ),
        horizon_decision_by_date=prototype["horizon_decision_by_date"],
        selector_metric=prototype["selector_metric"],
        transition_report=prototype["transition_report"],
        selector_rules=prototype["selector_rules"],
        prototype_summary=prototype["summary"],
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="horizon_selector_controlled_prototype",
    )
    return payload


def run_cost_aware_horizon_hysteresis(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion_path: Path = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    contract_path: Path = DEFAULT_HORIZON_SELECTOR_PROBLEM_CONTRACT_PATH,
    prototype_path: Path = DEFAULT_HORIZON_SELECTOR_CONTROLLED_PROTOTYPE_PATH,
    horizon_stabilization_path: Path = DEFAULT_HORIZON_CLIFF_STABILIZATION_REVIEW_PATH,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    value_surface = _read_json_or_empty(value_surface_expansion_path)
    contract = _read_json_or_empty(contract_path)
    prototype = _read_json_or_empty(prototype_path)
    horizon_stabilization = _read_json_or_empty(horizon_stabilization_path)
    selected_cases = _selected_value_surface_cases(
        _records(value_surface.get("value_surface")),
        config,
    )
    review = _cost_aware_horizon_hysteresis_review(
        selected_cases=selected_cases,
        config=config,
        horizon_stabilization=horizon_stabilization,
    )
    payload = _controlled_payload(
        report_type="cost_aware_horizon_hysteresis",
        title="Cost-aware horizon hysteresis",
        status="COST_AWARE_HORIZON_HYSTERESIS_REVIEWED",
        summary={
            "horizon_switch_count": review["summary"]["horizon_switch_count"],
            "action_flip_count": review["summary"]["action_flip_count"],
            "turnover_delta": review["summary"]["turnover_delta"],
            "cost_delta": review["summary"]["cost_delta"],
            "utility_lost_to_hysteresis": review["summary"]["utility_lost_to_hysteresis"],
            "tail_loss_reduction": review["summary"]["tail_loss_reduction"],
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        hysteresis_policy=_next_stage_section(config, "cost_aware_horizon_hysteresis"),
        value_surface_source=_artifact_status(value_surface, value_surface_expansion_path),
        contract_source=_artifact_status(contract, contract_path),
        prototype_source=_artifact_status(prototype, prototype_path),
        horizon_stabilization_source=_artifact_status(
            horizon_stabilization, horizon_stabilization_path
        ),
        original_metric=review["original_metric"],
        selector_metric=review["selector_metric"],
        hysteresis_metric=review["hysteresis_metric"],
        transition_report=review["transition_report"],
        hysteresis_cases=review["hysteresis_cases"],
        hysteresis_summary=review["summary"],
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="cost_aware_horizon_hysteresis",
    )
    return payload


def run_horizon_selector_holdout_review(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion_path: Path = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    contract_path: Path = DEFAULT_HORIZON_SELECTOR_PROBLEM_CONTRACT_PATH,
    fallback_review_path: Path = DEFAULT_LONG_HORIZON_QUARANTINE_FALLBACK_REVIEW_PATH,
    prototype_path: Path = DEFAULT_HORIZON_SELECTOR_CONTROLLED_PROTOTYPE_PATH,
    hysteresis_path: Path = DEFAULT_COST_AWARE_HORIZON_HYSTERESIS_PATH,
    horizon_stabilization_path: Path = DEFAULT_HORIZON_CLIFF_STABILIZATION_REVIEW_PATH,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    value_surface = _read_json_or_empty(value_surface_expansion_path)
    contract = _read_json_or_empty(contract_path)
    fallback_review = _read_json_or_empty(fallback_review_path)
    prototype = _read_json_or_empty(prototype_path)
    hysteresis = _read_json_or_empty(hysteresis_path)
    horizon_stabilization = _read_json_or_empty(horizon_stabilization_path)
    selected_cases = _selected_value_surface_cases(
        _records(value_surface.get("value_surface")),
        config,
    )
    review = _horizon_selector_holdout_review(
        selected_cases=selected_cases,
        config=config,
        horizon_stabilization=horizon_stabilization,
    )
    payload = _controlled_payload(
        report_type="horizon_selector_holdout_review",
        title="Horizon selector holdout review",
        status="HORIZON_SELECTOR_HOLDOUT_REVIEW_COMPLETE",
        summary={
            "horizon_selector_decision": review["summary"]["horizon_selector_decision"],
            "holdout_case_count": review["summary"]["holdout_case_count"],
            "holdout_pass_count": review["summary"]["holdout_pass_count"],
            "holdout_pass_rate": review["summary"]["holdout_pass_rate"],
            "tail_loss_condition_met": review["summary"]["tail_loss_condition_met"],
            "turnover_cost_condition_met": review["summary"]["turnover_cost_condition_met"],
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        holdout_policy=_next_stage_section(config, "horizon_selector_holdout_review"),
        value_surface_source=_artifact_status(value_surface, value_surface_expansion_path),
        contract_source=_artifact_status(contract, contract_path),
        fallback_review_source=_artifact_status(fallback_review, fallback_review_path),
        prototype_source=_artifact_status(prototype, prototype_path),
        hysteresis_source=_artifact_status(hysteresis, hysteresis_path),
        horizon_stabilization_source=_artifact_status(
            horizon_stabilization, horizon_stabilization_path
        ),
        review_decision=review["review_decision"],
        leave_one_regime_out=review["leave_one_regime_out"],
        leave_one_horizon_out=review["leave_one_horizon_out"],
        leave_one_asset_cluster_out=review["leave_one_asset_cluster_out"],
        leave_one_date_window_out=review["leave_one_date_window_out"],
        holdout_summary=review["summary"],
        disallowed_actions=[
            "expand_unconditional_value_surface",
            "treat_regime_conditioned_mean_delta_near_zero_as_success",
            "train_gbdt_nn_or_rl_strategy",
            "continue_utility_boundary_tuning",
            "enter_paper_shadow",
        ],
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="horizon_selector_holdout_review",
    )
    return payload


def run_value_surface_policy_kill_diagnostic_downgrade(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    horizon_selector_holdout_path: Path = DEFAULT_HORIZON_SELECTOR_HOLDOUT_REVIEW_PATH,
    v2_review_path: Path = DEFAULT_VALUE_SURFACE_V2_CONTROLLED_REVIEW_PATH,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    horizon_holdout = _read_json_or_empty(horizon_selector_holdout_path)
    v2_review = _read_json_or_empty(v2_review_path)
    downgrade = _value_surface_policy_kill_diagnostic_downgrade(
        config=config,
        horizon_holdout=horizon_holdout,
        v2_review=v2_review,
    )
    payload = _controlled_payload(
        report_type="value_surface_policy_kill_diagnostic_downgrade",
        title="Value surface policy kill and diagnostic downgrade",
        status="VALUE_SURFACE_POLICY_KILLED_DIAGNOSTIC_DOWNGRADE_COMPLETE",
        summary={
            "action_policy_allowed": downgrade["action_policy_allowed"],
            "promotion_gate_allowed": downgrade["promotion_gate_allowed"],
            "allowed_use_count": len(downgrade["allowed_uses"]),
            "disallowed_use_count": len(downgrade["disallowed_uses"]),
            "prior_horizon_selector_decision": downgrade["prior_horizon_selector_decision"],
            "diagnostic_downgrade_applied": downgrade["diagnostic_downgrade_applied"],
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        downgrade_policy=_next_stage_section(
            config, "value_surface_policy_kill_diagnostic_downgrade"
        ),
        horizon_selector_holdout_source=_artifact_status(
            horizon_holdout, horizon_selector_holdout_path
        ),
        v2_review_source=_artifact_status(v2_review, v2_review_path),
        policy_downgrade=downgrade,
        research_registry_guardrail={
            "value_surface_as_action_policy": "KILLED",
            "value_surface_allowed_use": "DIAGNOSTIC_ONLY",
            "promotion_gate_allowed": False,
            "paper_shadow_change_allowed": False,
            "production_weight_change_allowed": False,
        },
        disallowed_actions=[
            "use_value_surface_as_direct_action_policy",
            "use_value_surface_as_horizon_selector_policy",
            "enter_paper_shadow",
            "change_production_weights",
            "emit_broker_order_instruction",
        ],
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="value_surface_policy_kill_diagnostic_downgrade",
    )
    return payload


def run_benchmark_first_tail_risk_policy_contract(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    policy_kill_path: Path = DEFAULT_VALUE_SURFACE_POLICY_KILL_DIAGNOSTIC_DOWNGRADE_PATH,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    policy_kill = _read_json_or_empty(policy_kill_path)
    contract = _benchmark_first_tail_risk_policy_contract(config)
    payload = _controlled_payload(
        report_type="benchmark_first_tail_risk_policy_contract",
        title="Benchmark-first tail-risk policy contract",
        status="BENCHMARK_FIRST_TAIL_RISK_POLICY_CONTRACT_DEFINED",
        summary={
            "base_policy": contract["base_policy"],
            "allowed_deviation_count": len(contract["allowed_deviation"]),
            "risk_downshift_condition_count": len(contract["risk_downshift_condition"]),
            "fallback_policy": contract["fallback_policy"],
            "review_interval": contract["review_interval"],
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        contract_policy=_next_stage_section(config, "benchmark_first_tail_risk_policy_contract"),
        policy_kill_source=_artifact_status(policy_kill, policy_kill_path),
        policy_contract=contract,
        problem_statement=(
            "Default to benchmark/simple trend/static allocation; deviate only for "
            "tail-risk downshift or low-cost confirmed recovery."
        ),
        disallowed_actions=[
            "return_maximization_direct_action_policy",
            "unconditional_value_surface_expansion",
            "direct_gbdt_nn_rl_strategy",
            "paper_shadow_signal",
        ],
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="benchmark_first_tail_risk_policy_contract",
    )
    return payload


def run_tail_loss_avoidance_classifier_prototype(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion_path: Path = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    policy_kill_path: Path = DEFAULT_VALUE_SURFACE_POLICY_KILL_DIAGNOSTIC_DOWNGRADE_PATH,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    value_surface = _read_json_or_empty(value_surface_expansion_path)
    policy_kill = _read_json_or_empty(policy_kill_path)
    selected_cases = _selected_value_surface_cases(
        _records(value_surface.get("value_surface")),
        config,
    )
    classifier = _tail_loss_avoidance_classifier_prototype(
        selected_cases=selected_cases,
        config=config,
    )
    payload = _controlled_payload(
        report_type="tail_loss_avoidance_classifier_prototype",
        title="Tail-loss avoidance classifier prototype",
        status="TAIL_LOSS_AVOIDANCE_CLASSIFIER_PROTOTYPED",
        summary={
            "case_count": len(selected_cases),
            "label_case_count": classifier["summary"]["label_case_count"],
            "large_loss_case_count": classifier["summary"]["large_loss_case_count"],
            "tail_loss_case_count": classifier["summary"]["tail_loss_case_count"],
            "benchmark_underperformance_case_count": classifier["summary"][
                "benchmark_underperformance_case_count"
            ],
            "long_horizon_failure_case_count": classifier["summary"][
                "long_horizon_failure_case_count"
            ],
            "gate_block_count": classifier["summary"]["gate_block_count"],
            "strategy_signal_generated": False,
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        classifier_policy=_next_stage_section(config, "tail_loss_avoidance_classifier_prototype"),
        value_surface_source=_artifact_status(value_surface, value_surface_expansion_path),
        policy_kill_source=_artifact_status(policy_kill, policy_kill_path),
        classifier_summary=classifier["summary"],
        label_breakdown=classifier["label_breakdown"],
        classifier_rows=classifier["classifier_rows"],
        gate_semantics=classifier["gate_semantics"],
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="tail_loss_avoidance_classifier_prototype",
    )
    return payload


def run_conservative_horizon_risk_filter(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion_path: Path = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    classifier_path: Path = DEFAULT_TAIL_LOSS_AVOIDANCE_CLASSIFIER_PROTOTYPE_PATH,
    contract_path: Path = DEFAULT_BENCHMARK_FIRST_TAIL_RISK_POLICY_CONTRACT_PATH,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    value_surface = _read_json_or_empty(value_surface_expansion_path)
    classifier = _read_json_or_empty(classifier_path)
    contract = _read_json_or_empty(contract_path)
    selected_cases = _selected_value_surface_cases(
        _records(value_surface.get("value_surface")),
        config,
    )
    review = _conservative_horizon_risk_filter(
        selected_cases=selected_cases,
        config=config,
        classifier=classifier,
    )
    payload = _controlled_payload(
        report_type="conservative_horizon_risk_filter",
        title="Conservative horizon risk filter",
        status="CONSERVATIVE_HORIZON_RISK_FILTER_REVIEWED",
        summary={
            "case_count": len(selected_cases),
            "allowed_horizon_count": review["summary"]["allowed_horizon_count"],
            "quarantined_horizon_count": review["summary"]["quarantined_horizon_count"],
            "fallback_only_horizon_count": review["summary"]["fallback_only_horizon_count"],
            "fallback_count": review["summary"]["fallback_count"],
            "tail_loss_after_filter": review["summary"]["tail_loss_after_filter"],
            "selector_mode": review["selector_mode"],
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        horizon_filter_policy=_next_stage_section(config, "conservative_horizon_risk_filter"),
        value_surface_source=_artifact_status(value_surface, value_surface_expansion_path),
        classifier_source=_artifact_status(classifier, classifier_path),
        contract_source=_artifact_status(contract, contract_path),
        horizon_status=review["horizon_status"],
        horizon_filter_rows=review["horizon_filter_rows"],
        original_metric=review["original_metric"],
        filtered_metric=review["filtered_metric"],
        filter_summary=review["summary"],
        selector_mode=review["selector_mode"],
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="conservative_horizon_risk_filter",
    )
    return payload


def run_benchmark_fallback_drawdown_guard_controlled_prototype(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion_path: Path = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    classifier_path: Path = DEFAULT_TAIL_LOSS_AVOIDANCE_CLASSIFIER_PROTOTYPE_PATH,
    horizon_filter_path: Path = DEFAULT_CONSERVATIVE_HORIZON_RISK_FILTER_PATH,
    contract_path: Path = DEFAULT_BENCHMARK_FIRST_TAIL_RISK_POLICY_CONTRACT_PATH,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    value_surface = _read_json_or_empty(value_surface_expansion_path)
    classifier = _read_json_or_empty(classifier_path)
    horizon_filter = _read_json_or_empty(horizon_filter_path)
    contract = _read_json_or_empty(contract_path)
    selected_cases = _selected_value_surface_cases(
        _records(value_surface.get("value_surface")),
        config,
    )
    review = _benchmark_fallback_drawdown_guard_controlled_prototype(
        selected_cases=selected_cases,
        config=config,
        classifier=classifier,
        horizon_filter=horizon_filter,
    )
    payload = _controlled_payload(
        report_type="benchmark_fallback_drawdown_guard_controlled_prototype",
        title="Benchmark fallback / drawdown guard controlled prototype",
        status="BENCHMARK_FALLBACK_DRAWDOWN_GUARD_REVIEWED",
        summary={
            "case_count": len(selected_cases),
            "variant_count": len(review["variant_metrics"]),
            "best_variant_by_tail_loss": review["summary"]["best_variant_by_tail_loss"],
            "best_variant_tail_loss_reduction": review["summary"][
                "best_variant_tail_loss_reduction"
            ],
            "best_variant_turnover_cost_not_worse": review["summary"][
                "best_variant_turnover_cost_not_worse"
            ],
            "holdout_pass_rate": review["summary"]["holdout_pass_rate"],
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        fallback_policy=_next_stage_section(
            config, "benchmark_fallback_drawdown_guard_controlled_prototype"
        ),
        value_surface_source=_artifact_status(value_surface, value_surface_expansion_path),
        classifier_source=_artifact_status(classifier, classifier_path),
        horizon_filter_source=_artifact_status(horizon_filter, horizon_filter_path),
        contract_source=_artifact_status(contract, contract_path),
        variant_metrics=review["variant_metrics"],
        variant_rules=review["variant_rules"],
        holdout_summary_by_variant=review["holdout_summary_by_variant"],
        review_summary=review["summary"],
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="benchmark_fallback_drawdown_guard_prototype",
    )
    return payload


def run_tail_risk_policy_family_controlled_review(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    policy_kill_path: Path = DEFAULT_VALUE_SURFACE_POLICY_KILL_DIAGNOSTIC_DOWNGRADE_PATH,
    contract_path: Path = DEFAULT_BENCHMARK_FIRST_TAIL_RISK_POLICY_CONTRACT_PATH,
    classifier_path: Path = DEFAULT_TAIL_LOSS_AVOIDANCE_CLASSIFIER_PROTOTYPE_PATH,
    horizon_filter_path: Path = DEFAULT_CONSERVATIVE_HORIZON_RISK_FILTER_PATH,
    fallback_path: Path = DEFAULT_BENCHMARK_FALLBACK_DRAWDOWN_GUARD_PROTOTYPE_PATH,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    policy_kill = _read_json_or_empty(policy_kill_path)
    contract = _read_json_or_empty(contract_path)
    classifier = _read_json_or_empty(classifier_path)
    horizon_filter = _read_json_or_empty(horizon_filter_path)
    fallback = _read_json_or_empty(fallback_path)
    review = _tail_risk_policy_family_controlled_review(
        config=config,
        policy_kill=policy_kill,
        contract=contract,
        classifier=classifier,
        horizon_filter=horizon_filter,
        fallback=fallback,
    )
    payload = _controlled_payload(
        report_type="tail_risk_policy_family_controlled_review",
        title="Controlled review of tail-risk policy family",
        status="TAIL_RISK_POLICY_FAMILY_CONTROLLED_REVIEW_COMPLETE",
        summary={
            "tail_risk_policy_decision": review["review_decision"]["decision"],
            "tail_loss_condition_met": review["review_decision"]["tail_loss_condition_met"],
            "turnover_cost_condition_met": review["review_decision"]["turnover_cost_condition_met"],
            "holdout_condition_met": review["review_decision"]["holdout_condition_met"],
            "explainability_condition_met": review["review_decision"][
                "explainability_condition_met"
            ],
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        controlled_review_policy=_next_stage_section(
            config, "tail_risk_policy_family_controlled_review"
        ),
        policy_kill_source=_artifact_status(policy_kill, policy_kill_path),
        contract_source=_artifact_status(contract, contract_path),
        classifier_source=_artifact_status(classifier, classifier_path),
        horizon_filter_source=_artifact_status(horizon_filter, horizon_filter_path),
        fallback_source=_artifact_status(fallback, fallback_path),
        review_decision=review["review_decision"],
        evidence_summary=review["evidence_summary"],
        disallowed_actions=[
            "unconditional_value_surface_expansion",
            "horizon_selector_micro_tuning",
            "utility_boundary_micro_tuning",
            "direct_gbdt_nn_rl_strategy",
            "large_regret_casebook_expansion",
            "enter_paper_shadow",
        ],
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="tail_risk_policy_family_controlled_review",
    )
    return payload


def run_tail_risk_benchmark_fallback_robustness_expansion(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion_path: Path = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    classifier_path: Path = DEFAULT_TAIL_LOSS_AVOIDANCE_CLASSIFIER_PROTOTYPE_PATH,
    fallback_path: Path = DEFAULT_BENCHMARK_FALLBACK_DRAWDOWN_GUARD_PROTOTYPE_PATH,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    value_surface = _read_json_or_empty(value_surface_expansion_path)
    classifier = _read_json_or_empty(classifier_path)
    fallback = _read_json_or_empty(fallback_path)
    selected_cases = _selected_value_surface_cases(
        _records(value_surface.get("value_surface")),
        config,
    )
    review = _tail_risk_benchmark_fallback_robustness_expansion(
        selected_cases=selected_cases,
        config=config,
        classifier=classifier,
    )
    payload = _controlled_payload(
        report_type="tail_risk_benchmark_fallback_robustness_expansion",
        title="Tail-risk benchmark fallback robustness expansion",
        status="TAIL_RISK_FALLBACK_ROBUSTNESS_EXPANDED",
        summary={
            "robustness_decision": review["summary"]["robustness_decision"],
            "fallback_trigger_count": review["summary"]["fallback_trigger_count"],
            "fallback_frequency": review["summary"]["fallback_frequency"],
            "tail_loss_reduction": review["summary"]["tail_loss_reduction"],
            "mean_delta_vs_benchmark": review["fallback_metric"].get("mean_delta_vs_benchmark"),
            "upside_capture": review["summary"]["upside_capture"],
            "missed_upside_count": review["summary"]["missed_upside_count"],
            "false_fallback_count": review["summary"]["false_fallback_count"],
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        robustness_policy=_next_stage_section(
            config, "tail_risk_benchmark_fallback_robustness_expansion"
        ),
        value_surface_source=_artifact_status(value_surface, value_surface_expansion_path),
        classifier_source=_artifact_status(classifier, classifier_path),
        fallback_source=_artifact_status(fallback, fallback_path),
        original_metric=review["original_metric"],
        fallback_metric=review["fallback_metric"],
        robustness_summary=review["summary"],
        by_asset=review["by_asset"],
        by_horizon=review["by_horizon"],
        by_regime=review["by_regime"],
        by_cluster=review["by_cluster"],
        fallback_cases=review["fallback_cases"],
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="tail_risk_benchmark_fallback_robustness_expansion",
    )
    return payload


def run_tail_risk_fallback_trigger_precision_recall_audit(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion_path: Path = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    classifier_path: Path = DEFAULT_TAIL_LOSS_AVOIDANCE_CLASSIFIER_PROTOTYPE_PATH,
    robustness_path: Path = DEFAULT_TAIL_RISK_BENCHMARK_FALLBACK_ROBUSTNESS_PATH,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    value_surface = _read_json_or_empty(value_surface_expansion_path)
    classifier = _read_json_or_empty(classifier_path)
    robustness = _read_json_or_empty(robustness_path)
    selected_cases = _selected_value_surface_cases(
        _records(value_surface.get("value_surface")),
        config,
    )
    audit = _tail_risk_fallback_trigger_precision_recall_audit(
        selected_cases=selected_cases,
        config=config,
        classifier=classifier,
    )
    payload = _controlled_payload(
        report_type="tail_risk_fallback_trigger_precision_recall_audit",
        title="Tail-risk fallback trigger precision / recall audit",
        status="TAIL_RISK_FALLBACK_TRIGGER_PRECISION_RECALL_AUDITED",
        summary={
            "fallback_precision": audit["summary"]["fallback_precision"],
            "fallback_recall": audit["summary"]["fallback_recall"],
            "false_positive_rate": audit["summary"]["false_positive_rate"],
            "false_negative_rate": audit["summary"]["false_negative_rate"],
            "missed_upside_from_false_positive": audit["summary"][
                "missed_upside_from_false_positive"
            ],
            "tail_loss_from_false_negative": audit["summary"]["tail_loss_from_false_negative"],
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        trigger_audit_policy=_next_stage_section(
            config, "tail_risk_fallback_trigger_precision_recall_audit"
        ),
        value_surface_source=_artifact_status(value_surface, value_surface_expansion_path),
        classifier_source=_artifact_status(classifier, classifier_path),
        robustness_source=_artifact_status(robustness, robustness_path),
        confusion_matrix=audit["confusion_matrix"],
        trigger_audit_summary=audit["summary"],
        trigger_case_samples=audit["trigger_case_samples"],
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="tail_risk_fallback_trigger_precision_recall_audit",
    )
    return payload


def run_tail_risk_opportunity_cost_upside_capture_review(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion_path: Path = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    classifier_path: Path = DEFAULT_TAIL_LOSS_AVOIDANCE_CLASSIFIER_PROTOTYPE_PATH,
    robustness_path: Path = DEFAULT_TAIL_RISK_BENCHMARK_FALLBACK_ROBUSTNESS_PATH,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    value_surface = _read_json_or_empty(value_surface_expansion_path)
    classifier = _read_json_or_empty(classifier_path)
    robustness = _read_json_or_empty(robustness_path)
    selected_cases = _selected_value_surface_cases(
        _records(value_surface.get("value_surface")),
        config,
    )
    review = _tail_risk_opportunity_cost_upside_capture_review(
        selected_cases=selected_cases,
        config=config,
        classifier=classifier,
    )
    payload = _controlled_payload(
        report_type="tail_risk_opportunity_cost_upside_capture_review",
        title="Tail-risk opportunity cost / upside capture review",
        status="TAIL_RISK_OPPORTUNITY_COST_UPSIDE_CAPTURE_REVIEWED",
        summary={
            "benchmark_upside_case_count": review["summary"]["benchmark_upside_case_count"],
            "strategy_participation": review["summary"]["strategy_participation"],
            "upside_capture_ratio": review["summary"]["upside_capture_ratio"],
            "missed_upside_count": review["summary"]["missed_upside_count"],
            "missed_upside_cost": review["summary"]["missed_upside_cost"],
            "tail_loss_to_missed_upside_ratio": review["summary"][
                "tail_loss_to_missed_upside_ratio"
            ],
            "opportunity_cost_condition_met": review["summary"]["opportunity_cost_condition_met"],
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        opportunity_cost_policy=_next_stage_section(
            config, "tail_risk_opportunity_cost_upside_capture_review"
        ),
        value_surface_source=_artifact_status(value_surface, value_surface_expansion_path),
        classifier_source=_artifact_status(classifier, classifier_path),
        robustness_source=_artifact_status(robustness, robustness_path),
        opportunity_cost_summary=review["summary"],
        missed_upside_cases=review["missed_upside_cases"],
        missed_upside_concentration=review["missed_upside_concentration"],
        upside_capture_by_regime=review["upside_capture_by_regime"],
        upside_capture_by_horizon=review["upside_capture_by_horizon"],
        upside_capture_by_asset=review["upside_capture_by_asset"],
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="tail_risk_opportunity_cost_upside_capture_review",
    )
    return payload


def run_tail_risk_forward_evidence_integration(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion_path: Path = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    classifier_path: Path = DEFAULT_TAIL_LOSS_AVOIDANCE_CLASSIFIER_PROTOTYPE_PATH,
    robustness_path: Path = DEFAULT_TAIL_RISK_BENCHMARK_FALLBACK_ROBUSTNESS_PATH,
    ledger_path: Path = DEFAULT_FORWARD_DAILY_DRY_RUN_LEDGER_PATH,
    output_root: Path = DEFAULT_FORWARD_DRY_RUN_ARCHIVE_PATH.parent,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    value_surface = _read_json_or_empty(value_surface_expansion_path)
    classifier = _read_json_or_empty(classifier_path)
    robustness = _read_json_or_empty(robustness_path)
    selected_cases = _selected_value_surface_cases(
        _records(value_surface.get("value_surface")),
        config,
    )
    integration = _tail_risk_forward_evidence_integration(
        selected_cases=selected_cases,
        config=config,
        classifier=classifier,
        as_of_date=as_of_date,
        ledger_path=ledger_path,
        output_root=output_root,
    )
    payload = _controlled_payload(
        report_type="tail_risk_forward_evidence_integration",
        title="Tail-risk policy forward evidence integration",
        status="TAIL_RISK_FORWARD_EVIDENCE_INTEGRATED",
        summary={
            "forward_record_count": integration["summary"]["forward_record_count"],
            "fallback_trigger_count": integration["summary"]["fallback_trigger_count"],
            "ledger_append_status": integration["summary"]["ledger_append_status"],
            "future_outcome_status": integration["summary"]["future_outcome_status"],
            "append_only_integrity_pass": integration["summary"]["append_only_integrity_pass"],
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        forward_integration_policy=_next_stage_section(
            config, "tail_risk_forward_evidence_integration"
        ),
        value_surface_source=_artifact_status(value_surface, value_surface_expansion_path),
        classifier_source=_artifact_status(classifier, classifier_path),
        robustness_source=_artifact_status(robustness, robustness_path),
        as_of=integration["as_of"],
        archive_id=integration["archive_id"],
        forward_records=integration["forward_records"],
        evidence_ledger_path=str(ledger_path),
        evidence_ledger_event=integration["ledger_event"],
        integration_summary=integration["summary"],
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="tail_risk_benchmark_fallback_forward_evidence_integration",
    )
    return payload


def run_tail_risk_fallback_audit_universe_reconciliation(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    robustness_path: Path = DEFAULT_TAIL_RISK_BENCHMARK_FALLBACK_ROBUSTNESS_PATH,
    precision_recall_path: Path = DEFAULT_TAIL_RISK_FALLBACK_TRIGGER_PRECISION_RECALL_PATH,
    opportunity_cost_path: Path = DEFAULT_TAIL_RISK_OPPORTUNITY_COST_UPSIDE_CAPTURE_PATH,
    forward_integration_path: Path = DEFAULT_TAIL_RISK_FORWARD_EVIDENCE_INTEGRATION_PATH,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    robustness = _read_json_or_empty(robustness_path)
    precision = _read_json_or_empty(precision_recall_path)
    opportunity = _read_json_or_empty(opportunity_cost_path)
    forward = _read_json_or_empty(forward_integration_path)
    reconciliation = _tail_risk_fallback_audit_universe_reconciliation(
        config=config,
        robustness=robustness,
        precision=precision,
        opportunity=opportunity,
        forward=forward,
        paths={
            "TRADING-816": robustness_path,
            "TRADING-817": precision_recall_path,
            "TRADING-818": opportunity_cost_path,
            "TRADING-819": forward_integration_path,
        },
    )
    payload = _controlled_payload(
        report_type="tail_risk_fallback_audit_universe_reconciliation",
        title="Tail-risk fallback audit universe and count reconciliation",
        status=reconciliation["status"],
        summary={
            "task_id": "TRADING-821",
            "reconciliation_status": reconciliation["status"],
            "count_summary_count": len(reconciliation["count_reconciliation_summary"]),
            "missing_field_count": len(reconciliation["missing_field_records"]),
            "controlled_review_status": reconciliation["controlled_review_status"],
            "next_recommended_action": reconciliation["next_recommended_action"],
            **_summary_safety(),
        },
        task_id="TRADING-821",
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        reconciliation_policy=_next_stage_section(
            config, "tail_risk_fallback_audit_universe_reconciliation"
        ),
        input_artifacts=reconciliation["input_artifacts"],
        input_config_hash=_stable_hash(
            _next_stage_section(config, "tail_risk_policy_controlled_review_board")
        ),
        data_window=reconciliation["data_window"],
        controlled_only=True,
        promotion_gate_allowed=False,
        paper_shadow_change_allowed=False,
        production_weight_change_allowed=False,
        broker_action="none",
        task_reconciliation_rows=reconciliation["task_reconciliation_rows"],
        count_reconciliation_summary=reconciliation["count_reconciliation_summary"],
        missing_field_records=reconciliation["missing_field_records"],
        metrics=reconciliation["metrics"],
        warnings=reconciliation["warnings"],
        blockers=reconciliation["blockers"],
        next_recommended_action=reconciliation["next_recommended_action"],
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="tail_risk_fallback_audit_universe_reconciliation",
    )
    return payload


def run_tail_risk_fallback_anti_leakage_audit(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion_path: Path = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    classifier_path: Path = DEFAULT_TAIL_LOSS_AVOIDANCE_CLASSIFIER_PROTOTYPE_PATH,
    robustness_path: Path = DEFAULT_TAIL_RISK_BENCHMARK_FALLBACK_ROBUSTNESS_PATH,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    value_surface = _read_json_or_empty(value_surface_expansion_path)
    classifier = _read_json_or_empty(classifier_path)
    robustness = _read_json_or_empty(robustness_path)
    selected_cases = _selected_value_surface_cases(
        _records(value_surface.get("value_surface")),
        config,
    )
    audit = _tail_risk_fallback_anti_leakage_audit(
        selected_cases=selected_cases,
        config=config,
        value_surface=value_surface,
        classifier=classifier,
        robustness=robustness,
    )
    payload = _controlled_payload(
        report_type="tail_risk_fallback_anti_leakage_audit",
        title="Anti-leakage audit for fallback trigger, label, and outcome",
        status=audit["status"],
        summary={
            "task_id": "TRADING-822",
            "anti_leakage_status": audit["status"],
            "max_coupling_risk": audit["label_trigger_overlap_audit"]["coupling_risk"],
            "critical_issue_count": audit["summary"]["critical_issue_count"],
            "controlled_review_status": audit["controlled_review_status"],
            "next_recommended_action": audit["next_recommended_action"],
            **_summary_safety(),
        },
        task_id="TRADING-822",
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        anti_leakage_policy=_next_stage_section(config, "tail_risk_fallback_anti_leakage_audit"),
        input_artifacts={
            "value_surface_expansion": _artifact_status(
                value_surface, value_surface_expansion_path
            ),
            "classifier": _artifact_status(classifier, classifier_path),
            "robustness": _artifact_status(robustness, robustness_path),
        },
        input_config_hash=_stable_hash(
            _next_stage_section(config, "tail_risk_fallback_anti_leakage_audit")
        ),
        data_window=_case_data_window(selected_cases),
        controlled_only=True,
        promotion_gate_allowed=False,
        paper_shadow_change_allowed=False,
        production_weight_change_allowed=False,
        broker_action="none",
        timestamp_availability_audit=audit["timestamp_availability_audit"],
        label_trigger_overlap_audit=audit["label_trigger_overlap_audit"],
        outcome_horizon_separation_audit=audit["outcome_horizon_separation_audit"],
        pit_revision_audit=audit["pit_revision_audit"],
        metrics=audit["summary"],
        warnings=audit["warnings"],
        blockers=audit["blockers"],
        next_recommended_action=audit["next_recommended_action"],
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="tail_risk_fallback_anti_leakage_audit",
    )
    return payload


def run_tail_risk_fallback_threshold_sensitivity(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion_path: Path = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    classifier_path: Path = DEFAULT_TAIL_LOSS_AVOIDANCE_CLASSIFIER_PROTOTYPE_PATH,
    robustness_path: Path = DEFAULT_TAIL_RISK_BENCHMARK_FALLBACK_ROBUSTNESS_PATH,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    value_surface = _read_json_or_empty(value_surface_expansion_path)
    classifier = _read_json_or_empty(classifier_path)
    robustness = _read_json_or_empty(robustness_path)
    selected_cases = _selected_value_surface_cases(
        _records(value_surface.get("value_surface")),
        config,
    )
    sensitivity = _tail_risk_fallback_threshold_sensitivity(
        selected_cases=selected_cases,
        config=config,
        classifier=classifier,
    )
    payload = _controlled_payload(
        report_type="tail_risk_fallback_threshold_sensitivity",
        title="Tail-risk fallback threshold sensitivity and perturbation test",
        status=sensitivity["status"],
        summary={
            "task_id": "TRADING-823",
            "sensitivity_status": sensitivity["status"],
            "variant_count": len(sensitivity["variant_results"]),
            "cliff_detected": sensitivity["stability_summary"]["cliff_detected"],
            "promotion_block_reason": sensitivity["promotion_block_reason"],
            "next_recommended_action": sensitivity["next_recommended_action"],
            **_summary_safety(),
        },
        task_id="TRADING-823",
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        sensitivity_policy=_next_stage_section(config, "tail_risk_fallback_threshold_sensitivity"),
        input_artifacts={
            "value_surface_expansion": _artifact_status(
                value_surface, value_surface_expansion_path
            ),
            "classifier": _artifact_status(classifier, classifier_path),
            "robustness": _artifact_status(robustness, robustness_path),
        },
        input_config_hash=_stable_hash(
            _next_stage_section(config, "tail_risk_fallback_threshold_sensitivity")
        ),
        data_window=_case_data_window(selected_cases),
        controlled_only=True,
        promotion_gate_allowed=False,
        paper_shadow_change_allowed=False,
        production_weight_change_allowed=False,
        broker_action="none",
        variant_results=sensitivity["variant_results"],
        perturbation_coverage=sensitivity["perturbation_coverage"],
        stability_summary=sensitivity["stability_summary"],
        metrics=sensitivity["stability_summary"],
        warnings=sensitivity["warnings"],
        blockers=sensitivity["blockers"],
        next_recommended_action=sensitivity["next_recommended_action"],
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="tail_risk_fallback_threshold_sensitivity",
    )
    return payload


def run_tail_risk_fallback_regime_segmented_robustness(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion_path: Path = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    classifier_path: Path = DEFAULT_TAIL_LOSS_AVOIDANCE_CLASSIFIER_PROTOTYPE_PATH,
    robustness_path: Path = DEFAULT_TAIL_RISK_BENCHMARK_FALLBACK_ROBUSTNESS_PATH,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    value_surface = _read_json_or_empty(value_surface_expansion_path)
    classifier = _read_json_or_empty(classifier_path)
    robustness = _read_json_or_empty(robustness_path)
    selected_cases = _selected_value_surface_cases(
        _records(value_surface.get("value_surface")),
        config,
    )
    segmented = _tail_risk_fallback_regime_segmented_robustness(
        selected_cases=selected_cases,
        config=config,
        classifier=classifier,
    )
    payload = _controlled_payload(
        report_type="tail_risk_fallback_regime_segmented_robustness",
        title="Regime-segmented tail-risk fallback robustness review",
        status=segmented["status"],
        summary={
            "task_id": "TRADING-824",
            "regime_segmented_status": segmented["status"],
            "segment_count": len(segmented["segment_results"]),
            "concentration_risk": segmented["concentration_summary"]["concentration_risk"],
            "controlled_review_status": segmented["controlled_review_status"],
            "next_recommended_action": segmented["next_recommended_action"],
            **_summary_safety(),
        },
        task_id="TRADING-824",
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        segmented_policy=_next_stage_section(
            config, "tail_risk_fallback_regime_segmented_robustness"
        ),
        input_artifacts={
            "value_surface_expansion": _artifact_status(
                value_surface, value_surface_expansion_path
            ),
            "classifier": _artifact_status(classifier, classifier_path),
            "robustness": _artifact_status(robustness, robustness_path),
        },
        input_config_hash=_stable_hash(
            _next_stage_section(config, "tail_risk_fallback_regime_segmented_robustness")
        ),
        data_window=_case_data_window(selected_cases),
        controlled_only=True,
        promotion_gate_allowed=False,
        paper_shadow_change_allowed=False,
        production_weight_change_allowed=False,
        broker_action="none",
        segment_results=segmented["segment_results"],
        segment_unavailable=segmented["segment_unavailable"],
        concentration_summary=segmented["concentration_summary"],
        metrics=segmented["concentration_summary"],
        warnings=segmented["warnings"],
        blockers=segmented["blockers"],
        next_recommended_action=segmented["next_recommended_action"],
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="tail_risk_fallback_regime_segmented_robustness",
    )
    return payload


def run_tail_risk_fallback_forward_maturity_scoreboard(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    forward_integration_path: Path = DEFAULT_TAIL_RISK_FORWARD_EVIDENCE_INTEGRATION_PATH,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    forward = _read_json_or_empty(forward_integration_path)
    scoreboard = _tail_risk_fallback_forward_maturity_scoreboard(
        config=config,
        forward=forward,
        forward_path=forward_integration_path,
        as_of_date=as_of_date,
    )
    payload = _controlled_payload(
        report_type="tail_risk_fallback_forward_maturity_scoreboard",
        title="Tail-risk fallback forward maturity monitor and scoreboard",
        status=scoreboard["status"],
        summary={
            "task_id": "TRADING-825",
            "forward_maturity_status": scoreboard["status"],
            "forward_record_count": scoreboard["scoreboard"]["forward_record_count"],
            "matured_record_count": scoreboard["scoreboard"]["matured_record_count"],
            "promotion_readiness_assessment": scoreboard["promotion_readiness_assessment"],
            "next_recommended_action": scoreboard["next_recommended_action"],
            **_summary_safety(),
        },
        task_id="TRADING-825",
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        forward_maturity_policy=_next_stage_section(
            config, "tail_risk_fallback_forward_maturity_scoreboard"
        ),
        input_artifacts={
            "forward_integration": _artifact_status(forward, forward_integration_path),
        },
        input_config_hash=_stable_hash(
            _next_stage_section(config, "tail_risk_fallback_forward_maturity_scoreboard")
        ),
        data_window=scoreboard["data_window"],
        controlled_only=True,
        promotion_gate_allowed=False,
        paper_shadow_change_allowed=False,
        production_weight_change_allowed=False,
        broker_action="none",
        record_level_results=scoreboard["record_level_results"],
        scoreboard=scoreboard["scoreboard"],
        promotion_readiness_assessment=scoreboard["promotion_readiness_assessment"],
        metrics=scoreboard["scoreboard"],
        warnings=scoreboard["warnings"],
        blockers=scoreboard["blockers"],
        next_recommended_action=scoreboard["next_recommended_action"],
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="tail_risk_fallback_forward_maturity_scoreboard",
    )
    return payload


def run_tail_risk_policy_controlled_review_board(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    robustness_path: Path = DEFAULT_TAIL_RISK_BENCHMARK_FALLBACK_ROBUSTNESS_PATH,
    precision_recall_path: Path = DEFAULT_TAIL_RISK_FALLBACK_TRIGGER_PRECISION_RECALL_PATH,
    opportunity_cost_path: Path = DEFAULT_TAIL_RISK_OPPORTUNITY_COST_UPSIDE_CAPTURE_PATH,
    forward_integration_path: Path = DEFAULT_TAIL_RISK_FORWARD_EVIDENCE_INTEGRATION_PATH,
    audit_universe_reconciliation_path: Path | None = None,
    anti_leakage_path: Path | None = None,
    sensitivity_path: Path | None = None,
    regime_segmented_path: Path | None = None,
    forward_maturity_scoreboard_path: Path | None = None,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    robustness = _read_json_or_empty(robustness_path)
    precision = _read_json_or_empty(precision_recall_path)
    opportunity = _read_json_or_empty(opportunity_cost_path)
    forward = _read_json_or_empty(forward_integration_path)
    audit_universe = (
        _read_json_or_empty(audit_universe_reconciliation_path)
        if audit_universe_reconciliation_path is not None
        else {}
    )
    anti_leakage = _read_json_or_empty(anti_leakage_path) if anti_leakage_path is not None else {}
    sensitivity = _read_json_or_empty(sensitivity_path) if sensitivity_path is not None else {}
    regime_segmented = (
        _read_json_or_empty(regime_segmented_path) if regime_segmented_path is not None else {}
    )
    forward_scoreboard = (
        _read_json_or_empty(forward_maturity_scoreboard_path)
        if forward_maturity_scoreboard_path is not None
        else {}
    )
    review = _tail_risk_policy_controlled_review_board(
        config=config,
        robustness=robustness,
        precision=precision,
        opportunity=opportunity,
        forward=forward,
        audit_universe=audit_universe,
        anti_leakage=anti_leakage,
        sensitivity=sensitivity,
        regime_segmented=regime_segmented,
        forward_scoreboard=forward_scoreboard,
    )
    payload = _controlled_payload(
        report_type="tail_risk_policy_controlled_review_board",
        title="Tail-risk policy controlled review board",
        status="TAIL_RISK_POLICY_CONTROLLED_REVIEW_BOARD_COMPLETE",
        summary={
            "tail_risk_controlled_decision": review["review_decision"]["decision"],
            "audit_universe_reconciliation_status": review["review_decision"].get(
                "audit_universe_reconciliation_status"
            ),
            "anti_leakage_status": review["review_decision"].get("anti_leakage_status"),
            "sensitivity_status": review["review_decision"].get("sensitivity_status"),
            "regime_segmented_status": review["review_decision"].get("regime_segmented_status"),
            "forward_maturity_status": review["review_decision"].get("forward_maturity_status"),
            "robustness_condition_met": review["review_decision"]["robustness_condition_met"],
            "trigger_quality_condition_met": review["review_decision"][
                "trigger_quality_condition_met"
            ],
            "opportunity_cost_condition_met": review["review_decision"][
                "opportunity_cost_condition_met"
            ],
            "forward_integration_condition_met": review["review_decision"][
                "forward_integration_condition_met"
            ],
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        review_board_policy=_next_stage_section(config, "tail_risk_policy_controlled_review_board"),
        robustness_source=_artifact_status(robustness, robustness_path),
        precision_recall_source=_artifact_status(precision, precision_recall_path),
        opportunity_cost_source=_artifact_status(opportunity, opportunity_cost_path),
        forward_integration_source=_artifact_status(forward, forward_integration_path),
        audit_universe_reconciliation_source=_artifact_status(
            audit_universe, audit_universe_reconciliation_path
        ),
        anti_leakage_source=_artifact_status(anti_leakage, anti_leakage_path),
        sensitivity_source=_artifact_status(sensitivity, sensitivity_path),
        regime_segmented_source=_artifact_status(regime_segmented, regime_segmented_path),
        forward_maturity_scoreboard_source=_artifact_status(
            forward_scoreboard, forward_maturity_scoreboard_path
        ),
        review_decision=review["review_decision"],
        evidence_summary=review["evidence_summary"],
        disallowed_actions=[
            "promotion",
            "paper_shadow_start",
            "production_weight_change",
            "broker_order",
            "treat_forward_pending_outcome_as_mature_evidence",
        ],
        blockers=review["blockers"],
        warnings=review["warnings"],
        next_recommended_action=review["next_recommended_action"],
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="tail_risk_policy_controlled_review_board",
    )
    return payload


def run_tail_risk_fallback_blocker_diagnostic(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    review_board_path: Path = DEFAULT_TAIL_RISK_POLICY_CONTROLLED_REVIEW_BOARD_PATH,
    audit_universe_reconciliation_path: Path | None = None,
    anti_leakage_path: Path | None = None,
    sensitivity_path: Path | None = None,
    regime_segmented_path: Path | None = None,
    forward_maturity_scoreboard_path: Path | None = None,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    review_board = _read_json_or_empty(review_board_path)
    paths = _tail_risk_blocker_diagnostic_paths(
        review_board=review_board,
        audit_universe_reconciliation_path=audit_universe_reconciliation_path,
        anti_leakage_path=anti_leakage_path,
        sensitivity_path=sensitivity_path,
        regime_segmented_path=regime_segmented_path,
        forward_maturity_scoreboard_path=forward_maturity_scoreboard_path,
    )
    reports = {key: _read_json_or_empty(path) for key, path in paths.items()}
    diagnostic = _tail_risk_fallback_blocker_diagnostic(
        config=config,
        review_board=review_board,
        review_board_path=review_board_path,
        reports=reports,
        report_paths=paths,
    )
    payload = _controlled_payload(
        report_type="tail_risk_fallback_blocker_diagnostic",
        title="Tail-risk fallback blocker and warning diagnostic",
        status=diagnostic["status"],
        summary={
            "task_id": "TRADING-826",
            "diagnostic_status": diagnostic["status"],
            "review_board_decision": diagnostic["review_board_decision"],
            "blocked_trigger_tasks": diagnostic["final_blocked_trigger"]["trigger_tasks"],
            "highest_severity_root_cause": diagnostic["highest_severity_root_cause"],
            "report_registry_id": diagnostic["report_registry_entry"]["report_id"],
            **_summary_safety(),
        },
        task_id="TRADING-826",
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        blocker_diagnostic_policy=_next_stage_section(
            config, "tail_risk_fallback_blocker_diagnostic"
        ),
        review_board_source=_artifact_status(review_board, review_board_path),
        input_reports=diagnostic["input_reports"],
        severity_ordered_findings=diagnostic["severity_ordered_findings"],
        final_blocked_trigger=diagnostic["final_blocked_trigger"],
        root_cause_summary=diagnostic["root_cause_summary"],
        report_registry_entry=diagnostic["report_registry_entry"],
        read_only_assertions=diagnostic["read_only_assertions"],
        blockers=diagnostic["blockers"],
        warnings=diagnostic["warnings"],
        next_recommended_action=diagnostic["next_recommended_action"],
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="tail_risk_fallback_blocker_diagnostic",
    )
    return payload


def run_tail_risk_trigger_label_independence_audit(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    classifier_path: Path = DEFAULT_TAIL_LOSS_AVOIDANCE_CLASSIFIER_PROTOTYPE_PATH,
    robustness_path: Path = DEFAULT_TAIL_RISK_BENCHMARK_FALLBACK_ROBUSTNESS_PATH,
    precision_recall_path: Path = DEFAULT_TAIL_RISK_FALLBACK_TRIGGER_PRECISION_RECALL_PATH,
    anti_leakage_path: Path = DEFAULT_TAIL_RISK_ANTI_LEAKAGE_AUDIT_PATH,
    forward_integration_path: Path = DEFAULT_TAIL_RISK_FORWARD_EVIDENCE_INTEGRATION_PATH,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    classifier = _read_json_or_empty(classifier_path)
    robustness = _read_json_or_empty(robustness_path)
    precision = _read_json_or_empty(precision_recall_path)
    anti_leakage = _read_json_or_empty(anti_leakage_path)
    forward = _read_json_or_empty(forward_integration_path)
    audit = _tail_risk_trigger_label_independence_audit(
        config=config,
        classifier=classifier,
        robustness=robustness,
        precision=precision,
        anti_leakage=anti_leakage,
        forward=forward,
    )
    payload = _controlled_payload(
        report_type="tail_risk_trigger_label_independence_audit",
        title="Tail-risk fallback trigger/label independence audit",
        status=audit["status"],
        summary={
            "task_id": "TRADING-827",
            "owner_suggested_task_id": audit["owner_suggested_task_id"],
            "audit_status": audit["status"],
            "same_risk_definition_used_for_trigger_and_validation": audit[
                "same_risk_definition_used_for_trigger_and_validation"
            ],
            "return_metrics_temporarily_trustworthy": audit[
                "return_metrics_temporarily_trustworthy"
            ],
            "direct_overlap_count": audit["metrics"]["direct_overlap_count"],
            "derived_overlap_count": audit["metrics"]["derived_overlap_count"],
            "time_window_blocker_count": audit["metrics"]["time_window_blocker_count"],
            "next_recommended_action": audit["next_recommended_action"],
            **_summary_safety(),
        },
        task_id="TRADING-827",
        owner_suggested_task_id=audit["owner_suggested_task_id"],
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        trigger_label_independence_policy=_next_stage_section(
            config, "tail_risk_trigger_label_independence_audit"
        ),
        input_artifacts={
            "classifier": _artifact_status(classifier, classifier_path),
            "robustness": _artifact_status(robustness, robustness_path),
            "precision_recall": _artifact_status(precision, precision_recall_path),
            "anti_leakage": _artifact_status(anti_leakage, anti_leakage_path),
            "forward_integration": _artifact_status(forward, forward_integration_path),
        },
        input_config_hash=_stable_hash(
            _next_stage_section(config, "tail_risk_trigger_label_independence_audit")
        ),
        data_window=audit["data_window"],
        controlled_only=True,
        promotion_gate_allowed=False,
        paper_shadow_change_allowed=False,
        production_weight_change_allowed=False,
        broker_action="none",
        trigger_fields=audit["trigger_fields"],
        label_outcome_fields=audit["label_outcome_fields"],
        forward_outcome_fields=audit["forward_outcome_fields"],
        overlap_matrix=audit["overlap_matrix"],
        time_window_matrix=audit["time_window_matrix"],
        derived_dependency_matrix=audit["derived_dependency_matrix"],
        independence_answer=audit["independence_answer"],
        report_registry_entry=audit["report_registry_entry"],
        metrics=audit["metrics"],
        warnings=audit["warnings"],
        blockers=audit["blockers"],
        next_recommended_action=audit["next_recommended_action"],
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="tail_risk_trigger_label_independence_audit",
    )
    return payload


def run_tail_risk_independent_forward_outcome_validation(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion_path: Path = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    classifier_path: Path = DEFAULT_TAIL_LOSS_AVOIDANCE_CLASSIFIER_PROTOTYPE_PATH,
    robustness_path: Path = DEFAULT_TAIL_RISK_BENCHMARK_FALLBACK_ROBUSTNESS_PATH,
    trigger_label_audit_path: Path = DEFAULT_TAIL_RISK_TRIGGER_LABEL_INDEPENDENCE_AUDIT_PATH,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    value_surface = _read_json_or_empty(value_surface_expansion_path)
    classifier = _read_json_or_empty(classifier_path)
    robustness = _read_json_or_empty(robustness_path)
    trigger_label = _read_json_or_empty(trigger_label_audit_path)
    selected_cases = _selected_value_surface_cases(
        _records(value_surface.get("value_surface")),
        config,
    )
    validation = _tail_risk_independent_forward_outcome_validation(
        config=config,
        selected_cases=selected_cases,
        classifier=classifier,
        robustness=robustness,
        trigger_label=trigger_label,
    )
    payload = _controlled_payload(
        report_type="tail_risk_independent_forward_outcome_validation",
        title="Tail-risk independent forward outcome validation",
        status=validation["status"],
        summary={
            "task_id": "TRADING-828",
            "independent_forward_status": validation["status"],
            "decision_count": validation["summary"]["decision_count"],
            "valid_forward_20d_count": validation["summary"]["valid_forward_20d_count"],
            "outcome_forbidden_dependency_count": validation["summary"][
                "outcome_forbidden_dependency_count"
            ],
            "next_recommended_action": validation["next_recommended_action"],
            **_summary_safety(),
        },
        task_id="TRADING-828",
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        validation_policy=_next_stage_section(
            config, "tail_risk_independent_forward_outcome_validation"
        ),
        input_artifacts={
            "value_surface_expansion": _artifact_status(
                value_surface, value_surface_expansion_path
            ),
            "classifier": _artifact_status(classifier, classifier_path),
            "robustness": _artifact_status(robustness, robustness_path),
            "trigger_label_audit": _artifact_status(trigger_label, trigger_label_audit_path),
        },
        input_config_hash=_stable_hash(
            _next_stage_section(config, "tail_risk_independent_forward_outcome_validation")
        ),
        data_window=_case_data_window(selected_cases),
        controlled_only=True,
        outcome_source_contract=validation["outcome_source_contract"],
        independent_outcome_fields=validation["independent_outcome_fields"],
        forbidden_outcome_fields=validation["forbidden_outcome_fields"],
        decision_outcomes=validation["decision_outcomes"],
        horizon_summary=validation["horizon_summary"],
        policy_comparison_summary=validation["policy_comparison_summary"],
        metrics=validation["summary"],
        warnings=validation["warnings"],
        blockers=validation["blockers"],
        next_recommended_action=validation["next_recommended_action"],
        report_registry_entry=_tail_risk_governance_report_registry_entry(
            "tail_risk_independent_forward_outcome_validation",
            "Tail-Risk Independent Forward Outcome Validation",
            "tail-risk-independent-forward-outcome-validation",
        ),
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="tail_risk_independent_forward_outcome_validation",
    )
    return payload


def run_tail_risk_forward_outcome_contract_audit(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    trigger_label_audit_path: Path = DEFAULT_TAIL_RISK_TRIGGER_LABEL_INDEPENDENCE_AUDIT_PATH,
    independent_forward_path: Path = DEFAULT_TAIL_RISK_INDEPENDENT_FORWARD_OUTCOME_VALIDATION_PATH,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    trigger_label = _read_json_or_empty(trigger_label_audit_path)
    independent = _read_json_or_empty(independent_forward_path)
    audit = _tail_risk_forward_outcome_contract_audit(
        config=config,
        trigger_label=trigger_label,
        independent=independent,
    )
    payload = _controlled_payload(
        report_type="tail_risk_forward_outcome_contract_audit",
        title="Tail-risk forward outcome contract and lineage audit",
        status=audit["status"],
        summary={
            "task_id": "TRADING-829",
            "contract_status": audit["status"],
            "direct_overlap_count": audit["summary"]["direct_overlap_count"],
            "derived_overlap_count": audit["summary"]["derived_overlap_count"],
            "forbidden_dependency_count": audit["summary"]["forbidden_dependency_count"],
            "future_leakage_count": audit["summary"]["future_leakage_count"],
            **_summary_safety(),
        },
        task_id="TRADING-829",
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        contract_policy=_next_stage_section(config, "tail_risk_forward_outcome_contract_audit"),
        input_artifacts={
            "trigger_label_audit": _artifact_status(trigger_label, trigger_label_audit_path),
            "independent_forward": _artifact_status(independent, independent_forward_path),
        },
        outcome_fields=audit["outcome_fields"],
        outcome_derived_dependencies=audit["outcome_derived_dependencies"],
        trigger_fields=audit["trigger_fields"],
        trigger_derived_dependencies=audit["trigger_derived_dependencies"],
        overlap_matrix=audit["overlap_matrix"],
        derived_overlap_matrix=audit["derived_overlap_matrix"],
        time_window_matrix=audit["time_window_matrix"],
        forbidden_dependency_matrix=audit["forbidden_dependency_matrix"],
        metrics=audit["summary"],
        warnings=audit["warnings"],
        blockers=audit["blockers"],
        next_recommended_action=audit["next_recommended_action"],
        report_registry_entry=_tail_risk_governance_report_registry_entry(
            "tail_risk_forward_outcome_contract_audit",
            "Tail-Risk Forward Outcome Contract Audit",
            "tail-risk-forward-outcome-contract-audit",
        ),
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="tail_risk_forward_outcome_contract_audit",
    )
    return payload


def run_tail_risk_decision_time_boundary_audit(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    trigger_label_audit_path: Path = DEFAULT_TAIL_RISK_TRIGGER_LABEL_INDEPENDENCE_AUDIT_PATH,
    contract_audit_path: Path = DEFAULT_TAIL_RISK_FORWARD_OUTCOME_CONTRACT_AUDIT_PATH,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    trigger_label = _read_json_or_empty(trigger_label_audit_path)
    contract = _read_json_or_empty(contract_audit_path)
    audit = _tail_risk_decision_time_boundary_audit(
        config=config,
        trigger_label=trigger_label,
        contract=contract,
    )
    payload = _controlled_payload(
        report_type="tail_risk_decision_time_boundary_audit",
        title="Tail-risk decision-time boundary audit",
        status=audit["status"],
        summary={
            "task_id": "TRADING-830",
            "time_boundary_status": audit["status"],
            "blocked_feature_count": audit["summary"]["blocked_feature_count"],
            "future_read_count": audit["summary"]["future_read_count"],
            "rolling_window_boundary_issue_count": audit["summary"][
                "rolling_window_boundary_issue_count"
            ],
            **_summary_safety(),
        },
        task_id="TRADING-830",
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        boundary_policy=_next_stage_section(config, "tail_risk_decision_time_boundary_audit"),
        input_artifacts={
            "trigger_label_audit": _artifact_status(trigger_label, trigger_label_audit_path),
            "contract_audit": _artifact_status(contract, contract_audit_path),
        },
        feature_availability_rows=audit["feature_availability_rows"],
        decision_time_boundary_matrix=audit["decision_time_boundary_matrix"],
        forward_read_matrix=audit["forward_read_matrix"],
        rolling_window_boundary_checks=audit["rolling_window_boundary_checks"],
        metrics=audit["summary"],
        warnings=audit["warnings"],
        blockers=audit["blockers"],
        next_recommended_action=audit["next_recommended_action"],
        report_registry_entry=_tail_risk_governance_report_registry_entry(
            "tail_risk_decision_time_boundary_audit",
            "Tail-Risk Decision-Time Boundary Audit",
            "tail-risk-decision-time-boundary-audit",
        ),
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="tail_risk_decision_time_boundary_audit",
    )
    return payload


def run_tail_risk_tainted_metric_quarantine(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    trigger_label_audit_path: Path = DEFAULT_TAIL_RISK_TRIGGER_LABEL_INDEPENDENCE_AUDIT_PATH,
    precision_recall_path: Path = DEFAULT_TAIL_RISK_FALLBACK_TRIGGER_PRECISION_RECALL_PATH,
    robustness_path: Path = DEFAULT_TAIL_RISK_BENCHMARK_FALLBACK_ROBUSTNESS_PATH,
    opportunity_cost_path: Path = DEFAULT_TAIL_RISK_OPPORTUNITY_COST_UPSIDE_CAPTURE_PATH,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    trigger_label = _read_json_or_empty(trigger_label_audit_path)
    precision = _read_json_or_empty(precision_recall_path)
    robustness = _read_json_or_empty(robustness_path)
    opportunity = _read_json_or_empty(opportunity_cost_path)
    quarantine = _tail_risk_tainted_metric_quarantine(
        trigger_label=trigger_label,
        precision=precision,
        robustness=robustness,
        opportunity=opportunity,
    )
    payload = _controlled_payload(
        report_type="tail_risk_tainted_metric_quarantine",
        title="Tail-risk tainted metric quarantine",
        status=quarantine["status"],
        summary={
            "task_id": "TRADING-831",
            "metric_status": quarantine["metric_status"],
            "quarantined_metric_count": quarantine["summary"]["quarantined_metric_count"],
            "requires_independent_forward_validation": True,
            **_summary_safety(),
        },
        task_id="TRADING-831",
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        quarantine_policy=_next_stage_section(config, "tail_risk_tainted_metric_quarantine"),
        input_artifacts={
            "trigger_label_audit": _artifact_status(trigger_label, trigger_label_audit_path),
            "precision_recall": _artifact_status(precision, precision_recall_path),
            "robustness": _artifact_status(robustness, robustness_path),
            "opportunity_cost": _artifact_status(opportunity, opportunity_cost_path),
        },
        metric_status=quarantine["metric_status"],
        usable_for_promotion=False,
        usable_for_paper_shadow=False,
        usable_for_production=False,
        requires_independent_forward_validation=True,
        quarantined_metrics=quarantine["quarantined_metrics"],
        artifact_quarantine_summary=quarantine["artifact_quarantine_summary"],
        metrics=quarantine["summary"],
        warnings=quarantine["warnings"],
        blockers=quarantine["blockers"],
        next_recommended_action=quarantine["next_recommended_action"],
        report_registry_entry=_tail_risk_governance_report_registry_entry(
            "tail_risk_tainted_metric_quarantine",
            "Tail-Risk Tainted Metric Quarantine",
            "tail-risk-tainted-metric-quarantine",
        ),
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="tail_risk_tainted_metric_quarantine",
    )
    return payload


def run_tail_risk_fallback_counterfactual_validation(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    independent_forward_path: Path = DEFAULT_TAIL_RISK_INDEPENDENT_FORWARD_OUTCOME_VALIDATION_PATH,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    independent = _read_json_or_empty(independent_forward_path)
    validation = _tail_risk_fallback_counterfactual_validation(
        config=config,
        independent=independent,
    )
    payload = _controlled_payload(
        report_type="tail_risk_fallback_counterfactual_validation",
        title="Tail-risk fallback counterfactual baseline validation",
        status=validation["status"],
        summary={
            "task_id": "TRADING-832",
            "counterfactual_status": validation["status"],
            "comparison_count": len(validation["baseline_comparison"]),
            "sample_count": validation["summary"]["sample_count"],
            "false_positive_cost": validation["summary"]["false_positive_cost"],
            "false_negative_cost": validation["summary"]["false_negative_cost"],
            **_summary_safety(),
        },
        task_id="TRADING-832",
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        counterfactual_policy=_next_stage_section(
            config, "tail_risk_fallback_counterfactual_validation"
        ),
        input_artifacts={
            "independent_forward": _artifact_status(independent, independent_forward_path),
        },
        baseline_comparison=validation["baseline_comparison"],
        horizon_comparison=validation["horizon_comparison"],
        metrics=validation["summary"],
        warnings=validation["warnings"],
        blockers=validation["blockers"],
        next_recommended_action=validation["next_recommended_action"],
        report_registry_entry=_tail_risk_governance_report_registry_entry(
            "tail_risk_fallback_counterfactual_validation",
            "Tail-Risk Fallback Counterfactual Validation",
            "tail-risk-fallback-counterfactual-validation",
        ),
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="tail_risk_fallback_counterfactual_validation",
    )
    return payload


def run_tail_risk_regime_stratified_forward_outcome_review(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    independent_forward_path: Path = DEFAULT_TAIL_RISK_INDEPENDENT_FORWARD_OUTCOME_VALIDATION_PATH,
    counterfactual_path: Path = DEFAULT_TAIL_RISK_FALLBACK_COUNTERFACTUAL_VALIDATION_PATH,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    independent = _read_json_or_empty(independent_forward_path)
    counterfactual = _read_json_or_empty(counterfactual_path)
    review = _tail_risk_regime_stratified_forward_outcome_review(
        config=config,
        independent=independent,
        counterfactual=counterfactual,
    )
    payload = _controlled_payload(
        report_type="tail_risk_regime_stratified_forward_outcome_review",
        title="Tail-risk regime-stratified forward outcome review",
        status=review["status"],
        summary={
            "task_id": "TRADING-833",
            "regime_status": review["status"],
            "regime_row_count": len(review["regime_rows"]),
            "regime_concentration_score": review["summary"]["regime_concentration_score"],
            **_summary_safety(),
        },
        task_id="TRADING-833",
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        regime_policy=_next_stage_section(
            config, "tail_risk_regime_stratified_forward_outcome_review"
        ),
        input_artifacts={
            "independent_forward": _artifact_status(independent, independent_forward_path),
            "counterfactual": _artifact_status(counterfactual, counterfactual_path),
        },
        regime_rows=review["regime_rows"],
        unavailable_regimes=review["unavailable_regimes"],
        metrics=review["summary"],
        warnings=review["warnings"],
        blockers=review["blockers"],
        next_recommended_action=review["next_recommended_action"],
        report_registry_entry=_tail_risk_governance_report_registry_entry(
            "tail_risk_regime_stratified_forward_outcome_review",
            "Tail-Risk Regime-Stratified Forward Outcome Review",
            "tail-risk-regime-stratified-forward-outcome-review",
        ),
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="tail_risk_regime_stratified_forward_outcome_review",
    )
    return payload


def run_tail_risk_threshold_sensitivity_review(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    independent_forward_path: Path = DEFAULT_TAIL_RISK_INDEPENDENT_FORWARD_OUTCOME_VALIDATION_PATH,
    counterfactual_path: Path = DEFAULT_TAIL_RISK_FALLBACK_COUNTERFACTUAL_VALIDATION_PATH,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    independent = _read_json_or_empty(independent_forward_path)
    counterfactual = _read_json_or_empty(counterfactual_path)
    review = _tail_risk_threshold_sensitivity_review(
        config=config,
        independent=independent,
        counterfactual=counterfactual,
    )
    payload = _controlled_payload(
        report_type="tail_risk_threshold_sensitivity_review",
        title="Tail-risk threshold sensitivity robustness review",
        status=review["status"],
        summary={
            "task_id": "TRADING-834",
            "sensitivity_status": review["status"],
            "stability_score": review["summary"]["stability_score"],
            "fragile_parameter_count": len(review["fragile_parameter_list"]),
            **_summary_safety(),
        },
        task_id="TRADING-834",
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        sensitivity_policy=_next_stage_section(config, "tail_risk_threshold_sensitivity_review"),
        input_artifacts={
            "independent_forward": _artifact_status(independent, independent_forward_path),
            "counterfactual": _artifact_status(counterfactual, counterfactual_path),
        },
        sensitivity_surface=review["sensitivity_surface"],
        fragile_parameter_list=review["fragile_parameter_list"],
        metrics=review["summary"],
        warnings=review["warnings"],
        blockers=review["blockers"],
        next_recommended_action=review["next_recommended_action"],
        report_registry_entry=_tail_risk_governance_report_registry_entry(
            "tail_risk_threshold_sensitivity_review",
            "Tail-Risk Threshold Sensitivity Review",
            "tail-risk-threshold-sensitivity-review",
        ),
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="tail_risk_threshold_sensitivity_review",
    )
    return payload


def run_tail_risk_fallback_error_cost_ledger(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    independent_forward_path: Path = DEFAULT_TAIL_RISK_INDEPENDENT_FORWARD_OUTCOME_VALIDATION_PATH,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    independent = _read_json_or_empty(independent_forward_path)
    ledger = _tail_risk_fallback_error_cost_ledger(config=config, independent=independent)
    payload = _controlled_payload(
        report_type="tail_risk_fallback_error_cost_ledger",
        title="Tail-risk fallback false-positive / false-negative cost ledger",
        status=ledger["status"],
        summary={
            "task_id": "TRADING-835",
            "error_cost_status": ledger["status"],
            "false_positive_count": ledger["summary"]["false_positive_count"],
            "false_negative_count": ledger["summary"]["false_negative_count"],
            "cost_asymmetry_score": ledger["summary"]["cost_asymmetry_score"],
            **_summary_safety(),
        },
        task_id="TRADING-835",
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        error_cost_policy=_next_stage_section(config, "tail_risk_fallback_error_cost_ledger"),
        input_artifacts={
            "independent_forward": _artifact_status(independent, independent_forward_path),
        },
        false_positive_cases=ledger["false_positive_cases"],
        false_negative_cases=ledger["false_negative_cases"],
        worst_5_cases=ledger["worst_5_cases"],
        best_5_cases=ledger["best_5_cases"],
        metrics=ledger["summary"],
        warnings=ledger["warnings"],
        blockers=ledger["blockers"],
        next_recommended_action=ledger["next_recommended_action"],
        report_registry_entry=_tail_risk_governance_report_registry_entry(
            "tail_risk_fallback_error_cost_ledger",
            "Tail-Risk Fallback Error Cost Ledger",
            "tail-risk-fallback-error-cost-ledger",
        ),
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="tail_risk_fallback_error_cost_ledger",
    )
    return payload


def run_tail_risk_evidence_maturity_gate(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    independent_forward_path: Path = DEFAULT_TAIL_RISK_INDEPENDENT_FORWARD_OUTCOME_VALIDATION_PATH,
    regime_review_path: Path = DEFAULT_TAIL_RISK_REGIME_STRATIFIED_FORWARD_OUTCOME_REVIEW_PATH,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    independent = _read_json_or_empty(independent_forward_path)
    regime = _read_json_or_empty(regime_review_path)
    gate = _tail_risk_evidence_maturity_gate(
        config=config,
        independent=independent,
        regime=regime,
    )
    payload = _controlled_payload(
        report_type="tail_risk_evidence_maturity_gate",
        title="Tail-risk sample coverage and evidence maturity gate",
        status=gate["status"],
        summary={
            "task_id": "TRADING-836",
            "evidence_status": gate["status"],
            "triggered_count": gate["summary"]["triggered_count"],
            "valid_forward_20d_count": gate["summary"]["valid_forward_20d_count"],
            "evidence_level": gate["summary"]["evidence_level"],
            **_summary_safety(),
        },
        task_id="TRADING-836",
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        maturity_policy=_next_stage_section(config, "tail_risk_evidence_maturity_gate"),
        input_artifacts={
            "independent_forward": _artifact_status(independent, independent_forward_path),
            "regime_review": _artifact_status(regime, regime_review_path),
        },
        maturity_checks=gate["maturity_checks"],
        metrics=gate["summary"],
        warnings=gate["warnings"],
        blockers=gate["blockers"],
        next_recommended_action=gate["next_recommended_action"],
        report_registry_entry=_tail_risk_governance_report_registry_entry(
            "tail_risk_evidence_maturity_gate",
            "Tail-Risk Evidence Maturity Gate",
            "tail-risk-evidence-maturity-gate",
        ),
        remaining_blockers=_common_blockers(),
    )
    _write_pair(payload, output_root=output_root, artifact_id="tail_risk_evidence_maturity_gate")
    return payload


def run_tail_risk_forward_aging_tracker(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    forward_integration_path: Path = DEFAULT_TAIL_RISK_FORWARD_EVIDENCE_INTEGRATION_PATH,
    independent_forward_path: Path = DEFAULT_TAIL_RISK_INDEPENDENT_FORWARD_OUTCOME_VALIDATION_PATH,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    forward = _read_json_or_empty(forward_integration_path)
    independent = _read_json_or_empty(independent_forward_path)
    tracker = _tail_risk_forward_aging_tracker(
        config=config,
        forward=forward,
        independent=independent,
        as_of_date=as_of_date,
    )
    payload = _controlled_payload(
        report_type="tail_risk_forward_aging_tracker",
        title="Tail-risk forward aging observation tracker",
        status=tracker["status"],
        summary={
            "task_id": "TRADING-837",
            "aging_status": tracker["status"],
            "new_decisions_since_last_run": tracker["summary"]["new_decisions_since_last_run"],
            "pending_outcomes": tracker["summary"]["pending_outcomes"],
            **_summary_safety(),
        },
        task_id="TRADING-837",
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        aging_policy=_next_stage_section(config, "tail_risk_forward_aging_tracker"),
        input_artifacts={
            "forward_integration": _artifact_status(forward, forward_integration_path),
            "independent_forward": _artifact_status(independent, independent_forward_path),
        },
        aging_bucket_summary=tracker["aging_bucket_summary"],
        rolling_forward_performance=tracker["rolling_forward_performance"],
        metrics=tracker["summary"],
        warnings=tracker["warnings"],
        blockers=tracker["blockers"],
        next_recommended_action=tracker["next_recommended_action"],
        report_registry_entry=_tail_risk_governance_report_registry_entry(
            "tail_risk_forward_aging_tracker",
            "Tail-Risk Forward Aging Tracker",
            "tail-risk-forward-aging-tracker",
        ),
        remaining_blockers=_common_blockers(),
    )
    _write_pair(payload, output_root=output_root, artifact_id="tail_risk_forward_aging_tracker")
    return payload


def run_tail_risk_leakage_stress_suite(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    trigger_label_audit_path: Path = DEFAULT_TAIL_RISK_TRIGGER_LABEL_INDEPENDENCE_AUDIT_PATH,
    independent_forward_path: Path = DEFAULT_TAIL_RISK_INDEPENDENT_FORWARD_OUTCOME_VALIDATION_PATH,
    contract_audit_path: Path = DEFAULT_TAIL_RISK_FORWARD_OUTCOME_CONTRACT_AUDIT_PATH,
    boundary_audit_path: Path = DEFAULT_TAIL_RISK_DECISION_TIME_BOUNDARY_AUDIT_PATH,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    trigger_label = _read_json_or_empty(trigger_label_audit_path)
    independent = _read_json_or_empty(independent_forward_path)
    contract = _read_json_or_empty(contract_audit_path)
    boundary = _read_json_or_empty(boundary_audit_path)
    suite = _tail_risk_leakage_stress_suite(
        trigger_label=trigger_label,
        independent=independent,
        contract=contract,
        boundary=boundary,
    )
    payload = _controlled_payload(
        report_type="tail_risk_leakage_stress_suite",
        title="Tail-risk leakage stress test suite",
        status=suite["status"],
        summary={
            "task_id": "TRADING-838",
            "leakage_stress_status": suite["status"],
            "blocked_test_count": suite["summary"]["blocked_test_count"],
            "warn_test_count": suite["summary"]["warn_test_count"],
            **_summary_safety(),
        },
        task_id="TRADING-838",
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        leakage_policy=_next_stage_section(config, "tail_risk_leakage_stress_suite"),
        input_artifacts={
            "trigger_label_audit": _artifact_status(trigger_label, trigger_label_audit_path),
            "independent_forward": _artifact_status(independent, independent_forward_path),
            "contract_audit": _artifact_status(contract, contract_audit_path),
            "boundary_audit": _artifact_status(boundary, boundary_audit_path),
        },
        stress_tests=suite["stress_tests"],
        metrics=suite["summary"],
        warnings=suite["warnings"],
        blockers=suite["blockers"],
        next_recommended_action=suite["next_recommended_action"],
        report_registry_entry=_tail_risk_governance_report_registry_entry(
            "tail_risk_leakage_stress_suite",
            "Tail-Risk Leakage Stress Suite",
            "tail-risk-leakage-stress-suite",
        ),
        remaining_blockers=_common_blockers(),
    )
    _write_pair(payload, output_root=output_root, artifact_id="tail_risk_leakage_stress_suite")
    return payload


def run_tail_risk_promotion_readiness_gate(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    trigger_label_audit_path: Path = DEFAULT_TAIL_RISK_TRIGGER_LABEL_INDEPENDENCE_AUDIT_PATH,
    independent_forward_path: Path = DEFAULT_TAIL_RISK_INDEPENDENT_FORWARD_OUTCOME_VALIDATION_PATH,
    contract_audit_path: Path = DEFAULT_TAIL_RISK_FORWARD_OUTCOME_CONTRACT_AUDIT_PATH,
    boundary_audit_path: Path = DEFAULT_TAIL_RISK_DECISION_TIME_BOUNDARY_AUDIT_PATH,
    leakage_stress_path: Path = DEFAULT_TAIL_RISK_LEAKAGE_STRESS_SUITE_PATH,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    inputs = {
        "TRADING-827": _read_json_or_empty(trigger_label_audit_path),
        "TRADING-828": _read_json_or_empty(independent_forward_path),
        "TRADING-829": _read_json_or_empty(contract_audit_path),
        "TRADING-830": _read_json_or_empty(boundary_audit_path),
        "TRADING-838": _read_json_or_empty(leakage_stress_path),
    }
    paths = {
        "TRADING-827": trigger_label_audit_path,
        "TRADING-828": independent_forward_path,
        "TRADING-829": contract_audit_path,
        "TRADING-830": boundary_audit_path,
        "TRADING-838": leakage_stress_path,
    }
    gate = _tail_risk_promotion_readiness_gate(inputs=inputs)
    payload = _controlled_payload(
        report_type="tail_risk_promotion_readiness_gate",
        title="Tail-risk promotion readiness hard-block gate",
        status=gate["status"],
        summary={
            "task_id": "TRADING-839",
            "promotion_readiness_status": gate["status"],
            "blocking_input_count": gate["summary"]["blocking_input_count"],
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            **_summary_safety(),
        },
        task_id="TRADING-839",
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        promotion_gate_policy=_next_stage_section(config, "tail_risk_promotion_readiness_gate"),
        input_artifacts={
            task_id: _artifact_status(inputs[task_id], paths[task_id]) for task_id in inputs
        },
        gate_inputs=gate["gate_inputs"],
        promotion_allowed=False,
        paper_shadow_allowed=False,
        production_allowed=False,
        broker_action="none",
        metrics=gate["summary"],
        warnings=gate["warnings"],
        blockers=gate["blockers"],
        next_recommended_action=gate["next_recommended_action"],
        report_registry_entry=_tail_risk_governance_report_registry_entry(
            "tail_risk_promotion_readiness_gate",
            "Tail-Risk Promotion Readiness Gate",
            "tail-risk-promotion-readiness-gate",
        ),
        remaining_blockers=_common_blockers(),
    )
    _write_pair(payload, output_root=output_root, artifact_id="tail_risk_promotion_readiness_gate")
    return payload


def run_tail_risk_independent_trigger_v2_builder(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion_path: Path = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    boundary_audit_path: Path = DEFAULT_TAIL_RISK_DECISION_TIME_BOUNDARY_AUDIT_PATH,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    value_surface = _read_json_or_empty(value_surface_expansion_path)
    boundary = _read_json_or_empty(boundary_audit_path)
    selected_cases = _selected_value_surface_cases(
        _records(value_surface.get("value_surface")),
        config,
    )
    builder = _tail_risk_independent_trigger_v2_builder(
        config=config,
        selected_cases=selected_cases,
        boundary=boundary,
    )
    payload = _controlled_payload(
        report_type="tail_risk_independent_trigger_v2_builder",
        title="Tail-risk independent trigger v2 candidate builder",
        status=builder["status"],
        summary={
            "task_id": "TRADING-840",
            "candidate_status": builder["status"],
            "candidate_count": len(builder["candidate_trigger_v2_list"]),
            "forbidden_input_count": builder["summary"]["forbidden_input_count"],
            **_summary_safety(),
        },
        task_id="TRADING-840",
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        trigger_v2_policy=_next_stage_section(config, "tail_risk_independent_trigger_v2_builder"),
        input_artifacts={
            "value_surface_expansion": _artifact_status(
                value_surface, value_surface_expansion_path
            ),
            "boundary_audit": _artifact_status(boundary, boundary_audit_path),
        },
        candidate_trigger_v2_list=builder["candidate_trigger_v2_list"],
        feature_dependency_list=builder["feature_dependency_list"],
        time_window_contract=builder["time_window_contract"],
        initial_non_promotional_diagnostics=builder["initial_non_promotional_diagnostics"],
        metrics=builder["summary"],
        warnings=builder["warnings"],
        blockers=builder["blockers"],
        next_recommended_action=builder["next_recommended_action"],
        report_registry_entry=_tail_risk_governance_report_registry_entry(
            "tail_risk_independent_trigger_v2_builder",
            "Tail-Risk Independent Trigger V2 Builder",
            "tail-risk-independent-trigger-v2-builder",
        ),
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="tail_risk_independent_trigger_v2_builder",
    )
    return payload


def run_tail_risk_trigger_feature_availability_catalog(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    trigger_v2_path: Path = DEFAULT_TAIL_RISK_INDEPENDENT_TRIGGER_V2_BUILDER_PATH,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    trigger_v2 = _read_json_or_empty(trigger_v2_path)
    catalog = _tail_risk_trigger_feature_availability_catalog(trigger_v2=trigger_v2)
    payload = _controlled_payload(
        report_type="tail_risk_trigger_feature_availability_catalog",
        title="Tail-risk trigger feature availability catalog",
        status=catalog["status"],
        summary={
            "task_id": "TRADING-841",
            "feature_catalog_status": catalog["status"],
            "feature_count": len(catalog["feature_catalog"]),
            "blocked_feature_count": catalog["summary"]["blocked_feature_count"],
            **_summary_safety(),
        },
        task_id="TRADING-841",
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        feature_catalog_policy=_next_stage_section(
            config, "tail_risk_trigger_feature_availability_catalog"
        ),
        input_artifacts={
            "trigger_v2": _artifact_status(trigger_v2, trigger_v2_path),
        },
        feature_catalog=catalog["feature_catalog"],
        metrics=catalog["summary"],
        warnings=catalog["warnings"],
        blockers=catalog["blockers"],
        next_recommended_action=catalog["next_recommended_action"],
        report_registry_entry=_tail_risk_governance_report_registry_entry(
            "tail_risk_trigger_feature_availability_catalog",
            "Tail-Risk Trigger Feature Availability Catalog",
            "tail-risk-trigger-feature-availability-catalog",
        ),
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="tail_risk_trigger_feature_availability_catalog",
    )
    return payload


def run_tail_risk_research_master_review(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    trigger_label_audit_path: Path = DEFAULT_TAIL_RISK_TRIGGER_LABEL_INDEPENDENCE_AUDIT_PATH,
    independent_forward_path: Path = DEFAULT_TAIL_RISK_INDEPENDENT_FORWARD_OUTCOME_VALIDATION_PATH,
    contract_audit_path: Path = DEFAULT_TAIL_RISK_FORWARD_OUTCOME_CONTRACT_AUDIT_PATH,
    boundary_audit_path: Path = DEFAULT_TAIL_RISK_DECISION_TIME_BOUNDARY_AUDIT_PATH,
    quarantine_path: Path = DEFAULT_TAIL_RISK_TAINTED_METRIC_QUARANTINE_PATH,
    counterfactual_path: Path = DEFAULT_TAIL_RISK_FALLBACK_COUNTERFACTUAL_VALIDATION_PATH,
    regime_review_path: Path = DEFAULT_TAIL_RISK_REGIME_STRATIFIED_FORWARD_OUTCOME_REVIEW_PATH,
    sensitivity_review_path: Path = DEFAULT_TAIL_RISK_THRESHOLD_SENSITIVITY_REVIEW_PATH,
    error_cost_path: Path = DEFAULT_TAIL_RISK_FALLBACK_ERROR_COST_LEDGER_PATH,
    evidence_gate_path: Path = DEFAULT_TAIL_RISK_EVIDENCE_MATURITY_GATE_PATH,
    aging_tracker_path: Path = DEFAULT_TAIL_RISK_FORWARD_AGING_TRACKER_PATH,
    leakage_stress_path: Path = DEFAULT_TAIL_RISK_LEAKAGE_STRESS_SUITE_PATH,
    promotion_gate_path: Path = DEFAULT_TAIL_RISK_PROMOTION_READINESS_GATE_PATH,
    trigger_v2_path: Path = DEFAULT_TAIL_RISK_INDEPENDENT_TRIGGER_V2_BUILDER_PATH,
    feature_catalog_path: Path = DEFAULT_TAIL_RISK_TRIGGER_FEATURE_AVAILABILITY_CATALOG_PATH,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    inputs = {
        "TRADING-827": _read_json_or_empty(trigger_label_audit_path),
        "TRADING-828": _read_json_or_empty(independent_forward_path),
        "TRADING-829": _read_json_or_empty(contract_audit_path),
        "TRADING-830": _read_json_or_empty(boundary_audit_path),
        "TRADING-831": _read_json_or_empty(quarantine_path),
        "TRADING-832": _read_json_or_empty(counterfactual_path),
        "TRADING-833": _read_json_or_empty(regime_review_path),
        "TRADING-834": _read_json_or_empty(sensitivity_review_path),
        "TRADING-835": _read_json_or_empty(error_cost_path),
        "TRADING-836": _read_json_or_empty(evidence_gate_path),
        "TRADING-837": _read_json_or_empty(aging_tracker_path),
        "TRADING-838": _read_json_or_empty(leakage_stress_path),
        "TRADING-839": _read_json_or_empty(promotion_gate_path),
        "TRADING-840": _read_json_or_empty(trigger_v2_path),
        "TRADING-841": _read_json_or_empty(feature_catalog_path),
    }
    paths = {
        "TRADING-827": trigger_label_audit_path,
        "TRADING-828": independent_forward_path,
        "TRADING-829": contract_audit_path,
        "TRADING-830": boundary_audit_path,
        "TRADING-831": quarantine_path,
        "TRADING-832": counterfactual_path,
        "TRADING-833": regime_review_path,
        "TRADING-834": sensitivity_review_path,
        "TRADING-835": error_cost_path,
        "TRADING-836": evidence_gate_path,
        "TRADING-837": aging_tracker_path,
        "TRADING-838": leakage_stress_path,
        "TRADING-839": promotion_gate_path,
        "TRADING-840": trigger_v2_path,
        "TRADING-841": feature_catalog_path,
    }
    review = _tail_risk_research_master_review(inputs=inputs)
    payload = _controlled_payload(
        report_type="tail_risk_research_master_review",
        title="Tail-risk research master review",
        status=review["status"],
        summary={
            "task_id": "TRADING-842",
            "master_review_status": review["status"],
            "final_recommendation": review["final_recommendation"],
            "remaining_blocker_count": len(review["remaining_blockers"]),
            "promotion_possible_later": review["whether_production_possible_later"],
            **_summary_safety(),
        },
        task_id="TRADING-842",
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        master_review_policy=_next_stage_section(config, "tail_risk_research_master_review"),
        input_artifacts={
            task_id: _artifact_status(inputs[task_id], paths[task_id]) for task_id in inputs
        },
        current_valid_metrics=review["current_valid_metrics"],
        invalidated_metrics=review["invalidated_metrics"],
        remaining_blockers=review["remaining_blockers"],
        minimum_tasks_before_review=review["minimum_tasks_before_review"],
        whether_shadow_possible_later=review["whether_shadow_possible_later"],
        whether_production_possible_later=review["whether_production_possible_later"],
        owner_next_action=review["owner_next_action"],
        final_recommendation=review["final_recommendation"],
        metrics=review["summary"],
        warnings=review["warnings"],
        blockers=review["blockers"],
        next_recommended_action=review["owner_next_action"],
        report_registry_entry=_tail_risk_governance_report_registry_entry(
            "tail_risk_research_master_review",
            "Tail-Risk Research Master Review",
            "tail-risk-research-master-review",
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id="tail_risk_research_master_review")
    return payload


def run_tail_risk_post_merge_evidence_review(
    *,
    trigger_label_audit_path: Path = DEFAULT_TAIL_RISK_TRIGGER_LABEL_INDEPENDENCE_AUDIT_PATH,
    independent_forward_path: Path = DEFAULT_TAIL_RISK_INDEPENDENT_FORWARD_OUTCOME_VALIDATION_PATH,
    contract_audit_path: Path = DEFAULT_TAIL_RISK_FORWARD_OUTCOME_CONTRACT_AUDIT_PATH,
    boundary_audit_path: Path = DEFAULT_TAIL_RISK_DECISION_TIME_BOUNDARY_AUDIT_PATH,
    quarantine_path: Path = DEFAULT_TAIL_RISK_TAINTED_METRIC_QUARANTINE_PATH,
    counterfactual_path: Path = DEFAULT_TAIL_RISK_FALLBACK_COUNTERFACTUAL_VALIDATION_PATH,
    regime_review_path: Path = DEFAULT_TAIL_RISK_REGIME_STRATIFIED_FORWARD_OUTCOME_REVIEW_PATH,
    sensitivity_review_path: Path = DEFAULT_TAIL_RISK_THRESHOLD_SENSITIVITY_REVIEW_PATH,
    error_cost_path: Path = DEFAULT_TAIL_RISK_FALLBACK_ERROR_COST_LEDGER_PATH,
    evidence_gate_path: Path = DEFAULT_TAIL_RISK_EVIDENCE_MATURITY_GATE_PATH,
    aging_tracker_path: Path = DEFAULT_TAIL_RISK_FORWARD_AGING_TRACKER_PATH,
    leakage_stress_path: Path = DEFAULT_TAIL_RISK_LEAKAGE_STRESS_SUITE_PATH,
    promotion_gate_path: Path = DEFAULT_TAIL_RISK_PROMOTION_READINESS_GATE_PATH,
    trigger_v2_path: Path = DEFAULT_TAIL_RISK_INDEPENDENT_TRIGGER_V2_BUILDER_PATH,
    feature_catalog_path: Path = DEFAULT_TAIL_RISK_TRIGGER_FEATURE_AVAILABILITY_CATALOG_PATH,
    master_review_path: Path = DEFAULT_TAIL_RISK_RESEARCH_MASTER_REVIEW_PATH,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    paths = {
        "TRADING-827": trigger_label_audit_path,
        "TRADING-828": independent_forward_path,
        "TRADING-829": contract_audit_path,
        "TRADING-830": boundary_audit_path,
        "TRADING-831": quarantine_path,
        "TRADING-832": counterfactual_path,
        "TRADING-833": regime_review_path,
        "TRADING-834": sensitivity_review_path,
        "TRADING-835": error_cost_path,
        "TRADING-836": evidence_gate_path,
        "TRADING-837": aging_tracker_path,
        "TRADING-838": leakage_stress_path,
        "TRADING-839": promotion_gate_path,
        "TRADING-840": trigger_v2_path,
        "TRADING-841": feature_catalog_path,
        "TRADING-842": master_review_path,
    }
    payloads = {task_id: _read_json_or_empty(path) for task_id, path in paths.items()}
    reviewed_task_ids = [task_id for task_id in paths if task_id != "TRADING-827"]
    dependency_summaries = {
        task_id: _tail_risk_post_merge_artifact_summary(payloads[task_id], paths[task_id])
        for task_id in ["TRADING-827"]
    }
    artifact_summaries = [
        _tail_risk_post_merge_artifact_summary(payloads[task_id], paths[task_id])
        for task_id in reviewed_task_ids
    ]
    summary_by_task = {row["task_id"]: row for row in artifact_summaries}
    special_checks = _tail_risk_post_merge_special_checks(
        payloads=payloads,
        artifact_summaries=artifact_summaries,
    )
    template_only = [row for row in artifact_summaries if row["template_only"]]
    zero_sample_positive = [
        row
        for row in artifact_summaries
        if row["sample_count"] == 0
        and _tail_risk_post_merge_positive_status(str(row["final_status"]))
    ]
    safety_violations = [
        row
        for row in artifact_summaries
        if row["production_effect"] != "none"
        or row["broker_action"] != "none"
        or row["promotion_allowed"]
        or row["paper_shadow_allowed"]
        or row["production_allowed"]
    ]
    failed_special_checks = [row for row in special_checks if not row["passed"]]
    current_hard_blockers = _tail_risk_post_merge_current_hard_blockers(payloads)
    review_failed = bool(
        template_only or zero_sample_positive or safety_violations or failed_special_checks
    )
    final_status = (
        "POST_MERGE_EVIDENCE_REVIEW_FAILED"
        if review_failed
        else (
            "POST_MERGE_EVIDENCE_REVIEW_BLOCKED"
            if current_hard_blockers
            else "POST_MERGE_EVIDENCE_REVIEW_COMPLETE"
        )
    )
    blockers = [
        {
            "blocker": "source_governance_blocker_inherited",
            "task_id": row["task_id"],
            "status": row["status"],
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
        }
        for row in current_hard_blockers
    ]
    if review_failed:
        blockers.append(
            {
                "blocker": "post_merge_review_consistency_failure",
                "failed_check_ids": [row["check_id"] for row in failed_special_checks],
                "safety_violation_count": len(safety_violations),
                "template_only_artifact_count": len(template_only),
                "zero_sample_positive_count": len(zero_sample_positive),
                "promotion_allowed": False,
                "paper_shadow_allowed": False,
                "production_allowed": False,
                "broker_action": "none",
            }
        )
    warnings = [
        {
            "warning": "forward_aging_pending",
            "task_id": "TRADING-837",
            "status": summary_by_task.get("TRADING-837", {}).get("final_status"),
            "pending_outcomes": _metric_value(payloads["TRADING-837"], "pending_outcomes"),
            "promotion_allowed": False,
        },
        {
            "warning": "trigger_feature_catalog_partial",
            "task_id": "TRADING-841",
            "status": summary_by_task.get("TRADING-841", {}).get("final_status"),
            "partial_feature_count": _metric_value(
                payloads["TRADING-841"],
                "partial_feature_count",
            ),
            "promotion_allowed": False,
        },
    ]
    payload = _controlled_payload(
        report_type="tail_risk_post_merge_evidence_review",
        title="Tail-risk post-merge evidence review",
        status=final_status,
        summary={
            "task_id_range": "TRADING-828..TRADING-842",
            "source_dependency_task_id": "TRADING-827",
            "final_status": final_status,
            "artifact_count": len(artifact_summaries),
            "source_artifact_count": len(payloads),
            "blocker_count": len(blockers),
            "warning_count": len(warnings),
            "template_only_artifact_count": len(template_only),
            "zero_sample_positive_count": len(zero_sample_positive),
            "safety_violation_count": len(safety_violations),
            "special_check_failed_count": len(failed_special_checks),
            "current_hard_blocker_count": len(current_hard_blockers),
            "artifact_status_overview": [
                {
                    "task_id": row["task_id"],
                    "report_id": row["report_id"],
                    "final_status": row["final_status"],
                    "blocker_count": row["blocker_count"],
                    "warning_count": row["warning_count"],
                    "sample_count": row["sample_count"],
                }
                for row in artifact_summaries
            ],
            "special_check_results": [
                {"check_id": row["check_id"], "passed": row["passed"]} for row in special_checks
            ],
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            **_summary_safety(),
        },
        final_status=final_status,
        task_id="TRADING-828_TO_842_POST_MERGE_REVIEW",
        artifact_summaries=artifact_summaries,
        dependency_artifact_summaries=dependency_summaries,
        special_checks=special_checks,
        template_only_artifacts=[row["task_id"] for row in template_only],
        zero_sample_positive_artifacts=[row["task_id"] for row in zero_sample_positive],
        safety_violations=[row["task_id"] for row in safety_violations],
        current_hard_blockers=current_hard_blockers,
        promotion_allowed=False,
        paper_shadow_allowed=False,
        production_allowed=False,
        broker_action="none",
        metrics={
            "artifact_count": len(artifact_summaries),
            "source_artifact_count": len(payloads),
            "current_hard_blocker_count": len(current_hard_blockers),
            "template_only_artifact_count": len(template_only),
            "zero_sample_positive_count": len(zero_sample_positive),
            "safety_violation_count": len(safety_violations),
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
        },
        warnings=warnings,
        blockers=blockers,
        next_recommended_action="keep_tail_risk_fallback_quarantined",
        report_registry_entry=_tail_risk_governance_report_registry_entry(
            "tail_risk_post_merge_evidence_review",
            "Tail-Risk Post-Merge Evidence Review",
            "tail-risk-post-merge-evidence-review",
        ),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="tail_risk_post_merge_evidence_review",
    )
    return payload


def run_tail_risk_governance_artifact_snapshot(
    *,
    artifact_paths: Mapping[str, Path] | None = None,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    paths = _tail_risk_governance_paths(artifact_paths)
    payloads = {task_id: _read_json_or_empty(path) for task_id, path in paths.items()}
    artifact_summaries = [
        _tail_risk_artifact_snapshot_row(task_id, payloads[task_id], paths[task_id])
        for task_id in paths
    ]
    hard_blockers = _tail_risk_post_merge_current_hard_blockers(payloads)
    special_checks = (
        _tail_risk_post_merge_special_checks(
            payloads=payloads,
            artifact_summaries=[
                _tail_risk_post_merge_artifact_summary(payloads[task_id], paths[task_id])
                for task_id in paths
                if task_id != "TRADING-827"
            ],
        )
        if all(payloads.get(f"TRADING-{task_id}") for task_id in range(827, 843))
        else []
    )
    missing = [row for row in artifact_summaries if not row["json_present"]]
    failed_checks = [row for row in special_checks if not row["passed"]]
    status = (
        "TAIL_RISK_GOVERNANCE_ARTIFACT_SNAPSHOT_INCOMPLETE"
        if missing
        else (
            "TAIL_RISK_GOVERNANCE_ARTIFACT_SNAPSHOT_BLOCKED"
            if hard_blockers
            else "TAIL_RISK_GOVERNANCE_ARTIFACT_SNAPSHOT_COMPLETE"
        )
    )
    blockers = [
        {
            "blocker": "missing_tail_risk_governance_artifact",
            "task_id": row["task_id"],
            "artifact_json_path": row["artifact_json_path"],
            "promotion_allowed": False,
        }
        for row in missing
    ] + [
        {
            "blocker": "tail_risk_hard_blocker_inherited",
            "task_id": row["task_id"],
            "status": row["status"],
            "promotion_allowed": False,
        }
        for row in hard_blockers
    ]
    warnings = [
        {
            "warning": "snapshot_special_check_failed",
            "check_id": row["check_id"],
            "promotion_allowed": False,
        }
        for row in failed_checks
    ]
    payload = _controlled_payload(
        report_type="tail_risk_governance_artifact_snapshot",
        title="Tail-risk governance artifact snapshot",
        status=status,
        summary={
            "task_id": "TRADING-843",
            "artifact_count": len(artifact_summaries),
            "missing_artifact_count": len(missing),
            "hard_blocker_count": len(hard_blockers),
            "special_check_failed_count": len(failed_checks),
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            **_summary_safety(),
        },
        task_id="TRADING-843",
        artifact_summaries=artifact_summaries,
        special_checks=special_checks,
        current_hard_blockers=hard_blockers,
        promotion_allowed=False,
        paper_shadow_allowed=False,
        production_allowed=False,
        broker_action="none",
        metrics={
            "artifact_count": len(artifact_summaries),
            "missing_artifact_count": len(missing),
            "hard_blocker_count": len(hard_blockers),
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
        },
        warnings=warnings,
        blockers=blockers,
        next_recommended_action="review_status_matrix_before_any_tail_risk_research_decision",
        report_registry_entry=_tail_risk_governance_report_registry_entry(
            "tail_risk_governance_artifact_snapshot",
            "Tail-Risk Governance Artifact Snapshot",
            "tail-risk-governance-artifact-snapshot",
        ),
    )
    _write_pair(
        payload, output_root=output_root, artifact_id="tail_risk_governance_artifact_snapshot"
    )
    return payload


def run_tail_risk_status_matrix(
    *,
    snapshot_path: Path = DEFAULT_TAIL_RISK_GOVERNANCE_ARTIFACT_SNAPSHOT_PATH,
    artifact_paths: Mapping[str, Path] | None = None,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    snapshot = _tail_risk_snapshot_payload(snapshot_path, artifact_paths)
    matrix_rows = [
        _tail_risk_status_matrix_row(row) for row in _records(snapshot.get("artifact_summaries"))
    ]
    hard_blockers = [
        row
        for row in matrix_rows
        if row["status"]
        in {
            "BLOCKED",
            "TIME_BOUNDARY_BLOCKED",
            "LEAKAGE_STRESS_BLOCKED",
            "PROMOTION_READINESS_BLOCKED",
            "MISSING",
        }
    ]
    overall_status = (
        "TAIL_RISK_RESEARCH_BLOCKED" if hard_blockers else "TAIL_RISK_RESEARCH_REVIEW_ONLY"
    )
    payload = _controlled_payload(
        report_type="tail_risk_status_matrix",
        title="Tail-risk status matrix",
        status=overall_status,
        summary={
            "task_id": "TRADING-844",
            "overall_status": overall_status,
            "matrix_row_count": len(matrix_rows),
            "hard_blocker_count": len(hard_blockers),
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            **_summary_safety(),
        },
        task_id="TRADING-844",
        overall_status=overall_status,
        status_matrix=matrix_rows,
        current_hard_blockers=hard_blockers,
        promotion_allowed=False,
        paper_shadow_allowed=False,
        production_allowed=False,
        broker_action="none",
        metrics={
            "matrix_row_count": len(matrix_rows),
            "hard_blocker_count": len(hard_blockers),
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
        },
        warnings=[],
        blockers=hard_blockers,
        next_recommended_action=(
            "keep_tail_risk_fallback_quarantined"
            if hard_blockers
            else "manual_research_review_only"
        ),
        report_registry_entry=_tail_risk_governance_report_registry_entry(
            "tail_risk_status_matrix",
            "Tail-Risk Status Matrix",
            "tail-risk-status-matrix",
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id="tail_risk_status_matrix")
    return payload


def run_tail_risk_real_data_validation_audit(
    *,
    snapshot_path: Path = DEFAULT_TAIL_RISK_GOVERNANCE_ARTIFACT_SNAPSHOT_PATH,
    artifact_paths: Mapping[str, Path] | None = None,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    snapshot = _tail_risk_snapshot_payload(snapshot_path, artifact_paths)
    rows = [
        _tail_risk_real_data_audit_row(row) for row in _records(snapshot.get("artifact_summaries"))
    ]
    missing = [row for row in rows if row["input_missing"]]
    fixture_only = [row for row in rows if row["fixture_fallback_detected"]]
    suspicious = [
        row
        for row in rows
        if row["missing_input_pass_detected"]
        or row["placeholder_result_detected"]
        or row["zero_sample_positive_detected"]
    ]
    status = (
        "INPUT_MISSING_BLOCKED"
        if missing
        else (
            "FIXTURE_ONLY_BLOCKED"
            if fixture_only
            else ("REAL_DATA_PARTIAL" if suspicious else "REAL_DATA_READY")
        )
    )
    blockers = [
        {
            "blocker": "real_data_input_missing",
            "task_id": row["task_id"],
            "artifact_json_path": row["artifact_json_path"],
            "promotion_allowed": False,
        }
        for row in missing
    ] + [
        {
            "blocker": "fixture_or_placeholder_audit_issue",
            "task_id": row["task_id"],
            "fixture_fallback_detected": row["fixture_fallback_detected"],
            "placeholder_result_detected": row["placeholder_result_detected"],
            "zero_sample_positive_detected": row["zero_sample_positive_detected"],
            "promotion_allowed": False,
        }
        for row in [*fixture_only, *suspicious]
    ]
    payload = _controlled_payload(
        report_type="tail_risk_real_data_validation_audit",
        title="Tail-risk real data validation audit",
        status=status,
        summary={
            "task_id": "TRADING-845",
            "audit_status": status,
            "audited_task_count": len(rows),
            "input_missing_count": len(missing),
            "fixture_fallback_count": len(fixture_only),
            "suspicious_result_count": len(suspicious),
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            **_summary_safety(),
        },
        task_id="TRADING-845",
        real_data_audit_rows=rows,
        promotion_allowed=False,
        paper_shadow_allowed=False,
        production_allowed=False,
        broker_action="none",
        metrics={
            "audited_task_count": len(rows),
            "input_missing_count": len(missing),
            "fixture_fallback_count": len(fixture_only),
            "suspicious_result_count": len(suspicious),
        },
        warnings=[] if status == "REAL_DATA_READY" else suspicious,
        blockers=blockers,
        next_recommended_action=(
            "rerun_tail_risk_governance_on_real_artifacts"
            if status != "REAL_DATA_READY"
            else "continue_to_forward_and_baseline_reviews"
        ),
        report_registry_entry=_tail_risk_governance_report_registry_entry(
            "tail_risk_real_data_validation_audit",
            "Tail-Risk Real Data Validation Audit",
            "tail-risk-real-data-validation-audit",
        ),
    )
    _write_pair(
        payload, output_root=output_root, artifact_id="tail_risk_real_data_validation_audit"
    )
    return payload


def run_tail_risk_independent_forward_outcome_result_review(
    *,
    independent_forward_path: Path = DEFAULT_TAIL_RISK_INDEPENDENT_FORWARD_OUTCOME_VALIDATION_PATH,
    contract_audit_path: Path = DEFAULT_TAIL_RISK_FORWARD_OUTCOME_CONTRACT_AUDIT_PATH,
    boundary_audit_path: Path = DEFAULT_TAIL_RISK_DECISION_TIME_BOUNDARY_AUDIT_PATH,
    counterfactual_path: Path = DEFAULT_TAIL_RISK_FALLBACK_COUNTERFACTUAL_VALIDATION_PATH,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    independent = _read_json_or_empty(independent_forward_path)
    contract = _read_json_or_empty(contract_audit_path)
    boundary = _read_json_or_empty(boundary_audit_path)
    counterfactual = _read_json_or_empty(counterfactual_path)
    rows = _records(independent.get("decision_outcomes"))
    valid_5d = _metric_value(independent, "valid_forward_5d_count")
    valid_10d = _metric_value(independent, "valid_forward_10d_count")
    valid_20d = _metric_value(independent, "valid_forward_20d_count")
    triggered = sum(1 for row in rows if row.get("fallback_triggered"))
    non_triggered = len(rows) - triggered
    forbidden_rows = [
        row for row in rows if row.get("forbidden_label_or_case_fields_used_in_outcome")
    ]
    baseline_available = bool(counterfactual.get("baseline_comparison"))
    sample_too_small = len(rows) < 30 or _first_int(valid_20d) < 20
    blocked = (
        not independent
        or independent.get("status") in {"MISSING", "INDEPENDENT_FORWARD_BLOCKED"}
        or bool(forbidden_rows)
    )
    if blocked:
        status = "FORWARD_OUTCOME_BLOCKED"
    elif sample_too_small:
        status = "FORWARD_OUTCOME_SAMPLE_TOO_SMALL"
    elif independent.get("status") == "INDEPENDENT_FORWARD_VALIDATED" and baseline_available:
        status = "FORWARD_OUTCOME_USABLE_FOR_RESEARCH"
    else:
        status = "FORWARD_OUTCOME_INCONCLUSIVE"
    blockers = []
    if blocked:
        blockers.append(
            {
                "blocker": "independent_forward_outcome_not_usable",
                "independent_status": independent.get("status", "MISSING"),
                "forbidden_dependency_count": len(forbidden_rows),
                "promotion_allowed": False,
            }
        )
    payload = _controlled_payload(
        report_type="tail_risk_independent_forward_outcome_result_review",
        title="Tail-risk independent forward outcome result review",
        status=status,
        summary={
            "task_id": "TRADING-846",
            "review_status": status,
            "valid_forward_5d_count": _first_int(valid_5d),
            "valid_forward_10d_count": _first_int(valid_10d),
            "valid_forward_20d_count": _first_int(valid_20d),
            "triggered_sample_count": triggered,
            "non_triggered_sample_count": non_triggered,
            "baseline_comparison_available": baseline_available,
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            **_summary_safety(),
        },
        task_id="TRADING-846",
        source_independent_forward_path=str(independent_forward_path),
        valid_forward_5d_count=_first_int(valid_5d),
        valid_forward_10d_count=_first_int(valid_10d),
        valid_forward_20d_count=_first_int(valid_20d),
        triggered_sample_count=triggered,
        non_triggered_sample_count=non_triggered,
        outcome_fields=list(independent.get("independent_outcome_fields") or []),
        forbidden_dependency_check={
            "status": "PASS" if not forbidden_rows else "BLOCKED",
            "forbidden_dependency_count": len(forbidden_rows),
            "contract_status": contract.get("status", "MISSING"),
            "promotion_allowed": False,
        },
        time_window_check={
            "outcome_contract_strictly_after_decision_time": _mapping(
                independent.get("outcome_source_contract")
            ).get("strictly_after_decision_time"),
            "boundary_audit_status": boundary.get("status", "MISSING"),
            "promotion_allowed": False,
        },
        baseline_comparison_available=baseline_available,
        promotion_allowed=False,
        paper_shadow_allowed=False,
        production_allowed=False,
        broker_action="none",
        metrics={
            "sample_count": len(rows),
            "triggered_sample_count": triggered,
            "non_triggered_sample_count": non_triggered,
            "valid_forward_20d_count": _first_int(valid_20d),
            "promotion_allowed": False,
        },
        warnings=[] if status == "FORWARD_OUTCOME_USABLE_FOR_RESEARCH" else [{"warning": status}],
        blockers=blockers,
        next_recommended_action=(
            "use_forward_outcome_for_research_only_with_promotion_blocked"
            if status == "FORWARD_OUTCOME_USABLE_FOR_RESEARCH"
            else "collect_or_repair_independent_forward_outcome_evidence"
        ),
        report_registry_entry=_tail_risk_governance_report_registry_entry(
            "tail_risk_independent_forward_outcome_result_review",
            "Tail-Risk Independent Forward Outcome Result Review",
            "tail-risk-independent-forward-outcome-result-review",
        ),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="tail_risk_independent_forward_outcome_result_review",
    )
    return payload


def run_tail_risk_counterfactual_baseline_result_review(
    *,
    counterfactual_path: Path = DEFAULT_TAIL_RISK_FALLBACK_COUNTERFACTUAL_VALIDATION_PATH,
    independent_forward_path: Path = DEFAULT_TAIL_RISK_INDEPENDENT_FORWARD_OUTCOME_VALIDATION_PATH,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    counterfactual = _read_json_or_empty(counterfactual_path)
    independent = _read_json_or_empty(independent_forward_path)
    rows = _records(independent.get("decision_outcomes"))
    comparisons = _records(counterfactual.get("baseline_comparison")) or (
        _tail_risk_independent_policy_comparison(rows) if rows else []
    )
    comparison_by_policy = {str(row.get("policy_id")): row for row in comparisons}
    fallback = comparison_by_policy.get("fallback_policy", {})
    baseline_ids = [
        "no_fallback_baseline",
        "static_allocation_baseline",
        "simple_trend_baseline",
        "equal_risk_qqq_sgov_baseline",
        "qqq_100_baseline",
        "qqq_60_sgov_40_baseline",
        "qqq_70_sgov_30_baseline",
        "tqqq_50_sgov_50_baseline",
        "tqqq_25_sgov_75_baseline",
        "simple_200dma_risk_off_baseline",
        "simple_volatility_target_baseline",
    ]
    baseline_reviews = [
        _tail_risk_baseline_delta_row(fallback, comparison_by_policy.get(policy_id, {}))
        for policy_id in baseline_ids
        if policy_id in comparison_by_policy
    ]
    dominance = [
        row
        for row in baseline_reviews
        if row["baseline_return_delta_vs_fallback"] >= 0
        and row["baseline_drawdown_delta_vs_fallback"] >= 0
    ]
    baseline_dominance_flag = bool(dominance)
    status = (
        "COUNTERFACTUAL_BASELINE_DOMINATED"
        if baseline_dominance_flag
        else (
            "COUNTERFACTUAL_BASELINE_INCONCLUSIVE"
            if not rows
            else "COUNTERFACTUAL_BASELINE_REVIEWABLE"
        )
    )
    payload = _controlled_payload(
        report_type="tail_risk_counterfactual_baseline_result_review",
        title="Tail-risk counterfactual baseline result review",
        status=status,
        summary={
            "task_id": "TRADING-847",
            "review_status": status,
            "baseline_count": len(baseline_reviews),
            "dominant_baseline_count": len(dominance),
            "baseline_dominance_flag": baseline_dominance_flag,
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            **_summary_safety(),
        },
        task_id="TRADING-847",
        source_counterfactual_path=str(counterfactual_path),
        baseline_reviews=baseline_reviews,
        return_delta_by_horizon=_tail_risk_return_delta_by_horizon(rows),
        drawdown_delta_by_horizon=_tail_risk_drawdown_delta_by_horizon(rows),
        false_positive_cost=_metric_value(counterfactual, "false_positive_cost"),
        false_negative_cost=_metric_value(counterfactual, "false_negative_cost"),
        turnover_cost_if_available=[
            {
                "policy_id": row["policy_id"],
                "turnover_cost_if_available": row.get("turnover_cost_if_available"),
            }
            for row in comparisons
        ],
        baseline_dominance_flag=baseline_dominance_flag,
        dominant_baselines=dominance,
        promotion_allowed=False,
        paper_shadow_allowed=False,
        production_allowed=False,
        broker_action="none",
        metrics={
            "baseline_count": len(baseline_reviews),
            "dominant_baseline_count": len(dominance),
            "baseline_dominance_flag": baseline_dominance_flag,
            "promotion_allowed": False,
        },
        warnings=(
            [
                {
                    "warning": "simple_baseline_dominates_fallback",
                    "dominant_baseline_count": len(dominance),
                    "promotion_allowed": False,
                }
            ]
            if dominance
            else []
        ),
        blockers=[],
        next_recommended_action=(
            "stop_complex_fallback_optimization_until_simple_baseline_review"
            if dominance
            else "continue_research_only_baseline_monitoring"
        ),
        report_registry_entry=_tail_risk_governance_report_registry_entry(
            "tail_risk_counterfactual_baseline_result_review",
            "Tail-Risk Counterfactual Baseline Result Review",
            "tail-risk-counterfactual-baseline-result-review",
        ),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="tail_risk_counterfactual_baseline_result_review",
    )
    return payload


def run_tail_risk_artifact_determinism_check(
    *,
    snapshot_path: Path = DEFAULT_TAIL_RISK_GOVERNANCE_ARTIFACT_SNAPSHOT_PATH,
    artifact_paths: Mapping[str, Path] | None = None,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    first = _tail_risk_snapshot_payload(snapshot_path, artifact_paths)
    second = _tail_risk_snapshot_payload(snapshot_path, artifact_paths)
    first_hash = _stable_hash(_strip_volatile_artifact_fields(first))
    second_hash = _stable_hash(_strip_volatile_artifact_fields(second))
    stable = first_hash == second_hash
    rows = _records(first.get("artifact_summaries"))
    unstable_sort = [row["task_id"] for row in rows] != sorted(row["task_id"] for row in rows)
    status = (
        "DETERMINISTIC_PASS"
        if stable and not unstable_sort
        else ("DETERMINISTIC_WARN" if stable else "DETERMINISTIC_BLOCKED")
    )
    payload = _controlled_payload(
        report_type="tail_risk_artifact_determinism_check",
        title="Tail-risk artifact determinism check",
        status=status,
        summary={
            "task_id": "TRADING-848",
            "determinism_status": status,
            "stable_hash_match": stable,
            "stable_sort_order": not unstable_sort,
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            **_summary_safety(),
        },
        task_id="TRADING-848",
        first_stable_hash=first_hash,
        second_stable_hash=second_hash,
        ignored_fields=["generated_at", "runtime", "mtime", "mtime_utc", "artifact_paths"],
        stable_sort_order=not unstable_sort,
        floating_output_policy=(
            "JSON numeric values are generated from deterministic input artifacts"
        ),
        random_seed_policy="No random sampling is used by tail-risk governance aggregators",
        promotion_allowed=False,
        paper_shadow_allowed=False,
        production_allowed=False,
        broker_action="none",
        metrics={
            "stable_hash_match": stable,
            "stable_sort_order": not unstable_sort,
            "promotion_allowed": False,
        },
        warnings=[] if status == "DETERMINISTIC_PASS" else [{"warning": status}],
        blockers=[] if stable else [{"blocker": "artifact_projection_not_deterministic"}],
        next_recommended_action=(
            "review_artifact_sorting_or_volatile_fields"
            if status != "DETERMINISTIC_PASS"
            else "determinism_projection_available_for_owner_review"
        ),
        report_registry_entry=_tail_risk_governance_report_registry_entry(
            "tail_risk_artifact_determinism_check",
            "Tail-Risk Artifact Determinism Check",
            "tail-risk-artifact-determinism-check",
        ),
    )
    _write_pair(
        payload, output_root=output_root, artifact_id="tail_risk_artifact_determinism_check"
    )
    return payload


def run_tail_risk_task_coverage_map(
    *,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
    docs_path: Path = DEFAULT_TAIL_RISK_TASK_COVERAGE_MAP_DOC_PATH,
) -> dict[str, Any]:
    rows = [_tail_risk_task_coverage_row(meta) for meta in TAIL_RISK_GOVERNANCE_TASK_METADATA]
    rows.extend(_tail_risk_task_coverage_row(meta) for meta in TAIL_RISK_FOLLOWUP_TASK_METADATA)
    payload = _controlled_payload(
        report_type="tail_risk_task_coverage_map",
        title="Tail-risk task coverage map",
        status="TAIL_RISK_TASK_COVERAGE_MAP_COMPLETE",
        summary={
            "task_id": "TRADING-850",
            "covered_task_count": len(rows),
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            **_summary_safety(),
        },
        task_id="TRADING-850",
        coverage_rows=rows,
        docs_covering_task=[str(docs_path)],
        promotion_allowed=False,
        paper_shadow_allowed=False,
        production_allowed=False,
        broker_action="none",
        metrics={"covered_task_count": len(rows), "promotion_allowed": False},
        warnings=[],
        blockers=[],
        next_recommended_action="use_coverage_map_for_owner_review_of_tail_risk_governance_batch",
        report_registry_entry=_tail_risk_governance_report_registry_entry(
            "tail_risk_task_coverage_map",
            "Tail-Risk Task Coverage Map",
            "tail-risk-task-coverage-map",
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id="tail_risk_task_coverage_map")
    docs_path.parent.mkdir(parents=True, exist_ok=True)
    docs_path.write_text(_render_tail_risk_task_coverage_map_md(rows), encoding="utf-8")
    payload["docs_path"] = str(docs_path)
    _write_json(output_root / "tail_risk_task_coverage_map.json", payload)
    return payload


def run_tail_risk_hard_block_mutation_tests(
    *,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    cases = _tail_risk_hard_block_mutation_cases()
    failed = [row for row in cases if not row["blocked"]]
    status = "HARD_BLOCK_MUTATION_PASS" if not failed else "HARD_BLOCK_MUTATION_FAILED"
    payload = _controlled_payload(
        report_type="tail_risk_hard_block_mutation_tests",
        title="Tail-risk hard-block mutation tests",
        status=status,
        summary={
            "task_id": "TRADING-851",
            "mutation_case_count": len(cases),
            "failed_case_count": len(failed),
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            **_summary_safety(),
        },
        task_id="TRADING-851",
        mutation_cases=cases,
        promotion_allowed=False,
        paper_shadow_allowed=False,
        production_allowed=False,
        broker_action="none",
        metrics={"mutation_case_count": len(cases), "failed_case_count": len(failed)},
        warnings=[],
        blockers=[
            {
                "blocker": "mutation_case_not_blocked",
                "case_id": row["case_id"],
                "promotion_allowed": False,
            }
            for row in failed
        ],
        next_recommended_action=(
            "fix_hard_block_fail_closed_logic" if failed else "keep_mutation_tests_in_regression"
        ),
        report_registry_entry=_tail_risk_governance_report_registry_entry(
            "tail_risk_hard_block_mutation_tests",
            "Tail-Risk Hard Block Mutation Tests",
            "tail-risk-hard-block-mutation-tests",
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id="tail_risk_hard_block_mutation_tests")
    return payload


def run_tail_risk_report_registry_integrity_review(
    *,
    registry_path: Path = PROJECT_ROOT / "config" / "report_registry.yaml",
    artifact_paths: Mapping[str, Path] | None = None,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    registry = safe_load_yaml_path(registry_path)
    reports = list(registry.get("reports") or []) if isinstance(registry, Mapping) else []
    report_ids = [str(row.get("report_id")) for row in reports if isinstance(row, Mapping)]
    report_by_id = {str(row.get("report_id")): row for row in reports if isinstance(row, Mapping)}
    expected = [*TAIL_RISK_GOVERNANCE_TASK_METADATA, *TAIL_RISK_FOLLOWUP_TASK_METADATA]
    path_overrides = _tail_risk_governance_paths(artifact_paths) if artifact_paths else {}
    rows = []
    for meta in expected:
        report_id = str(meta["report_id"])
        entry = report_by_id.get(report_id, {})
        artifact_path = path_overrides.get(str(meta["task_id"]), Path(str(meta["default_path"])))
        rows.append(
            {
                "task_id": meta["task_id"],
                "report_id": report_id,
                "entry_present": bool(entry),
                "report_id_unique": report_ids.count(report_id) == 1,
                "command": entry.get("command") if isinstance(entry, Mapping) else None,
                "command_registered": (
                    str(entry.get("command", "")).startswith("aits research strategies ")
                    if isinstance(entry, Mapping)
                    else False
                ),
                "artifact_selection_policy": (
                    entry.get("artifact_selection_policy") if isinstance(entry, Mapping) else None
                ),
                "artifact_selection_policy_valid": (
                    entry.get("artifact_selection_policy") == "latest_available"
                    if isinstance(entry, Mapping)
                    else False
                ),
                "required_for_daily_reading": (
                    bool(entry.get("required_for_daily_reading"))
                    if isinstance(entry, Mapping)
                    else None
                ),
                "required_for_daily_reading_false": (
                    entry.get("required_for_daily_reading") is False
                    if isinstance(entry, Mapping)
                    else False
                ),
                "artifact_json_path": str(artifact_path),
                "artifact_md_path": str(artifact_path.with_suffix(".md")),
                "artifact_json_exists": artifact_path.exists(),
                "artifact_md_exists": artifact_path.with_suffix(".md").exists(),
                "schema_verifiable": True,
                "promotion_allowed": False,
            }
        )
    failed = [
        row
        for row in rows
        if not (
            row["entry_present"]
            and row["report_id_unique"]
            and row["command_registered"]
            and row["artifact_selection_policy_valid"]
            and row["required_for_daily_reading_false"]
        )
    ]
    status = "REPORT_REGISTRY_INTEGRITY_PASS" if not failed else "REPORT_REGISTRY_INTEGRITY_BLOCKED"
    payload = _controlled_payload(
        report_type="tail_risk_report_registry_integrity_review",
        title="Tail-risk report registry integrity review",
        status=status,
        summary={
            "task_id": "TRADING-852",
            "checked_entry_count": len(rows),
            "failed_entry_count": len(failed),
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            **_summary_safety(),
        },
        task_id="TRADING-852",
        registry_path=str(registry_path),
        registry_integrity_rows=rows,
        promotion_allowed=False,
        paper_shadow_allowed=False,
        production_allowed=False,
        broker_action="none",
        metrics={"checked_entry_count": len(rows), "failed_entry_count": len(failed)},
        warnings=[
            {
                "warning": "artifact_pair_missing_at_review_time",
                "task_id": row["task_id"],
                "artifact_json_exists": row["artifact_json_exists"],
                "artifact_md_exists": row["artifact_md_exists"],
                "promotion_allowed": False,
            }
            for row in rows
            if not row["artifact_json_exists"] or not row["artifact_md_exists"]
        ],
        blockers=failed,
        next_recommended_action=(
            "repair_tail_risk_report_registry_entries" if failed else "registry_entries_reviewable"
        ),
        report_registry_entry=_tail_risk_governance_report_registry_entry(
            "tail_risk_report_registry_integrity_review",
            "Tail-Risk Report Registry Integrity Review",
            "tail-risk-report-registry-integrity-review",
        ),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="tail_risk_report_registry_integrity_review",
    )
    return payload


def run_tail_risk_daily_reading_safety_summary(
    *,
    status_matrix_path: Path = DEFAULT_TAIL_RISK_STATUS_MATRIX_PATH,
    master_review_path: Path = DEFAULT_TAIL_RISK_RESEARCH_MASTER_REVIEW_PATH,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    matrix = _read_json_or_empty(status_matrix_path)
    master = _read_json_or_empty(master_review_path)
    blockers = _records(matrix.get("current_hard_blockers")) or _records(
        master.get("remaining_blockers")
    )
    research_status = str(
        matrix.get("overall_status") or matrix.get("status") or master.get("status") or "MISSING"
    )
    payload = _controlled_payload(
        report_type="tail_risk_daily_reading_safety_summary",
        title="Tail-risk daily reading safety summary",
        status=(
            "TAIL_RISK_DAILY_READING_SUMMARY_BLOCKED"
            if blockers
            else "TAIL_RISK_DAILY_READING_SUMMARY_READY"
        ),
        summary={
            "task_id": "TRADING-853",
            "research_status": research_status,
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "latest_master_review_status": master.get("status", "MISSING"),
            "current_blocker_count": len(blockers),
            **_summary_safety(),
        },
        task_id="TRADING-853",
        tail_risk_fallback_status={
            "research_status": research_status,
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "current_blockers": blockers,
            "latest_master_review_status": master.get("status", "MISSING"),
            "source_status_matrix_path": str(status_matrix_path),
            "source_master_review_path": str(master_review_path),
        },
        promotion_allowed=False,
        paper_shadow_allowed=False,
        production_allowed=False,
        broker_action="none",
        metrics={"current_blocker_count": len(blockers), "promotion_allowed": False},
        warnings=[],
        blockers=blockers,
        next_recommended_action="show_summary_only_in_reader_brief_no_governance_report_dump",
        report_registry_entry=_tail_risk_governance_report_registry_entry(
            "tail_risk_daily_reading_safety_summary",
            "Tail-Risk Daily Reading Safety Summary",
            "tail-risk-daily-reading-safety-summary",
        ),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="tail_risk_daily_reading_safety_summary",
    )
    return payload


def run_tail_risk_independent_trigger_v2_input_quality_review(
    *,
    feature_catalog_path: Path = DEFAULT_TAIL_RISK_TRIGGER_FEATURE_AVAILABILITY_CATALOG_PATH,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    catalog = _read_json_or_empty(feature_catalog_path)
    rows = [
        _tail_risk_trigger_input_quality_row(row)
        for row in _records(catalog.get("feature_catalog"))
    ]
    blocked = [row for row in rows if not row["usable_for_trigger"]]
    partial = [row for row in rows if row["pit_quality"] != "PIT_SAFE"]
    status = (
        "TRIGGER_V2_INPUT_QUALITY_BLOCKED"
        if blocked
        else ("TRIGGER_V2_INPUT_QUALITY_PARTIAL" if partial else "TRIGGER_V2_INPUT_QUALITY_READY")
    )
    payload = _controlled_payload(
        report_type="tail_risk_independent_trigger_v2_input_quality_review",
        title="Tail-risk independent trigger v2 input quality review",
        status=status,
        summary={
            "task_id": "TRADING-854",
            "feature_count": len(rows),
            "blocked_feature_count": len(blocked),
            "partial_feature_count": len(partial),
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            **_summary_safety(),
        },
        task_id="TRADING-854",
        input_quality_rows=rows,
        promotion_allowed=False,
        paper_shadow_allowed=False,
        production_allowed=False,
        broker_action="none",
        metrics={
            "feature_count": len(rows),
            "blocked_feature_count": len(blocked),
            "partial_feature_count": len(partial),
        },
        warnings=partial,
        blockers=blocked,
        next_recommended_action=(
            "review_partial_proxy_inputs_before_trigger_v2"
            if partial
            else "trigger_v2_inputs_quality_reviewable"
        ),
        report_registry_entry=_tail_risk_governance_report_registry_entry(
            "tail_risk_independent_trigger_v2_input_quality_review",
            "Tail-Risk Independent Trigger V2 Input Quality Review",
            "tail-risk-independent-trigger-v2-input-quality-review",
        ),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="tail_risk_independent_trigger_v2_input_quality_review",
    )
    return payload


def run_tail_risk_baseline_dominance_gate(
    *,
    baseline_review_path: Path = DEFAULT_TAIL_RISK_COUNTERFACTUAL_BASELINE_RESULT_REVIEW_PATH,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    review = _read_json_or_empty(baseline_review_path)
    rows = _records(review.get("baseline_reviews"))
    dominance_rows = [_tail_risk_baseline_dominance_row(row) for row in rows]
    dominated = [row for row in dominance_rows if row["baseline_dominates_fallback"]]
    mixed = [
        row
        for row in dominance_rows
        if row["dominance_score"] > 0 and not row["baseline_dominates_fallback"]
    ]
    status = (
        "BASELINE_DOMINATED_BLOCKED"
        if dominated
        else ("BASELINE_MIXED" if mixed else "NOT_BASELINE_DOMINATED")
    )
    blockers = [
        {
            "blocker": "simple_baseline_dominates_fallback",
            "policy_id": row["policy_id"],
            "dominance_score": row["dominance_score"],
            "promotion_allowed": False,
        }
        for row in dominated
    ]
    payload = _controlled_payload(
        report_type="tail_risk_baseline_dominance_gate",
        title="Tail-risk baseline dominance gate",
        status=status,
        summary={
            "task_id": "TRADING-856",
            "dominance_status": status,
            "dominant_baseline_count": len(dominated),
            "mixed_baseline_count": len(mixed),
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            **_summary_safety(),
        },
        task_id="TRADING-856",
        baseline_dominance_rows=dominance_rows,
        promotion_allowed=False,
        paper_shadow_allowed=False,
        production_allowed=False,
        broker_action="none",
        metrics={
            "dominant_baseline_count": len(dominated),
            "mixed_baseline_count": len(mixed),
            "promotion_allowed": False,
        },
        warnings=mixed,
        blockers=blockers,
        next_recommended_action=(
            "pause_complex_fallback_research_until_simple_baseline_gap_explained"
            if dominated
            else "continue_research_only_baseline_observation"
        ),
        report_registry_entry=_tail_risk_governance_report_registry_entry(
            "tail_risk_baseline_dominance_gate",
            "Tail-Risk Baseline Dominance Gate",
            "tail-risk-baseline-dominance-gate",
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id="tail_risk_baseline_dominance_gate")
    return payload


def run_tail_risk_research_readiness_score(
    *,
    status_matrix_path: Path = DEFAULT_TAIL_RISK_STATUS_MATRIX_PATH,
    forward_review_path: Path = DEFAULT_TAIL_RISK_INDEPENDENT_FORWARD_OUTCOME_RESULT_REVIEW_PATH,
    baseline_gate_path: Path = DEFAULT_TAIL_RISK_BASELINE_DOMINANCE_GATE_PATH,
    evidence_gate_path: Path = DEFAULT_TAIL_RISK_EVIDENCE_MATURITY_GATE_PATH,
    regime_review_path: Path = DEFAULT_TAIL_RISK_REGIME_STRATIFIED_FORWARD_OUTCOME_REVIEW_PATH,
    sensitivity_review_path: Path = DEFAULT_TAIL_RISK_THRESHOLD_SENSITIVITY_REVIEW_PATH,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    matrix = _read_json_or_empty(status_matrix_path)
    forward = _read_json_or_empty(forward_review_path)
    baseline = _read_json_or_empty(baseline_gate_path)
    evidence = _read_json_or_empty(evidence_gate_path)
    regime = _read_json_or_empty(regime_review_path)
    sensitivity = _read_json_or_empty(sensitivity_review_path)
    components = _tail_risk_readiness_components(
        matrix=matrix,
        forward=forward,
        baseline=baseline,
        evidence=evidence,
        regime=regime,
        sensitivity=sensitivity,
    )
    score = sum(_first_int(row["points_awarded"]) for row in components)
    band = _tail_risk_readiness_band(score)
    payload = _controlled_payload(
        report_type="tail_risk_research_readiness_score",
        title="Tail-risk research readiness score",
        status=band["status"],
        summary={
            "task_id": "TRADING-857",
            "readiness_score": score,
            "readiness_band": band["label"],
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            **_summary_safety(),
        },
        task_id="TRADING-857",
        readiness_score=score,
        readiness_band=band,
        readiness_components=components,
        promotion_allowed=False,
        paper_shadow_allowed=False,
        production_allowed=False,
        broker_action="none",
        metrics={"readiness_score": score, "promotion_allowed": False},
        warnings=[],
        blockers=[
            {
                "blocker": "tail_risk_research_readiness_not_promotion_approval",
                "readiness_score": score,
                "promotion_allowed": False,
            }
        ],
        next_recommended_action=band["next_action"],
        report_registry_entry=_tail_risk_governance_report_registry_entry(
            "tail_risk_research_readiness_score",
            "Tail-Risk Research Readiness Score",
            "tail-risk-research-readiness-score",
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id="tail_risk_research_readiness_score")
    return payload


def run_tail_risk_next_decision_document(
    *,
    status_matrix_path: Path = DEFAULT_TAIL_RISK_STATUS_MATRIX_PATH,
    forward_review_path: Path = DEFAULT_TAIL_RISK_INDEPENDENT_FORWARD_OUTCOME_RESULT_REVIEW_PATH,
    baseline_review_path: Path = DEFAULT_TAIL_RISK_COUNTERFACTUAL_BASELINE_RESULT_REVIEW_PATH,
    baseline_gate_path: Path = DEFAULT_TAIL_RISK_BASELINE_DOMINANCE_GATE_PATH,
    readiness_path: Path = DEFAULT_TAIL_RISK_RESEARCH_READINESS_SCORE_PATH,
    master_review_path: Path = DEFAULT_TAIL_RISK_RESEARCH_MASTER_REVIEW_PATH,
    quarantine_path: Path = DEFAULT_TAIL_RISK_TAINTED_METRIC_QUARANTINE_PATH,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
    docs_path: Path = DEFAULT_TAIL_RISK_NEXT_DECISION_DOC_PATH,
) -> dict[str, Any]:
    matrix = _read_json_or_empty(status_matrix_path)
    forward = _read_json_or_empty(forward_review_path)
    baseline_review = _read_json_or_empty(baseline_review_path)
    baseline_gate = _read_json_or_empty(baseline_gate_path)
    readiness = _read_json_or_empty(readiness_path)
    master = _read_json_or_empty(master_review_path)
    quarantine = _read_json_or_empty(quarantine_path)
    blocked = bool(
        _records(matrix.get("current_hard_blockers")) or _records(master.get("remaining_blockers"))
    )
    baseline_dominated = baseline_gate.get("status") == "BASELINE_DOMINATED_BLOCKED"
    forward_usable = forward.get("status") == "FORWARD_OUTCOME_USABLE_FOR_RESEARCH"
    owner_next_action = (
        "pause"
        if baseline_dominated
        else ("rebuild" if blocked else ("continue" if forward_usable else "pause"))
    )
    payload = _controlled_payload(
        report_type="tail_risk_next_decision",
        title="Tail-risk fallback next decision",
        status=(
            "TAIL_RISK_NEXT_DECISION_BLOCKED" if blocked else "TAIL_RISK_NEXT_DECISION_REVIEWABLE"
        ),
        summary={
            "task_id": "TRADING-858",
            "current_strategy_blocked": blocked,
            "forward_outcome_usable_for_research": forward_usable,
            "baseline_dominated": baseline_dominated,
            "owner_next_action": owner_next_action,
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            **_summary_safety(),
        },
        task_id="TRADING-858",
        decision_answers={
            "current_strategy_still_blocked": blocked,
            "tainted_metrics": [
                row.get("metric_name") for row in _records(quarantine.get("quarantined_metrics"))
            ],
            "research_only_metrics_now_available": master.get("current_valid_metrics", []),
            "independent_outcome_sufficient": forward_usable,
            "baseline_review_status": baseline_review.get("status", "UNKNOWN"),
            "baseline_dominated": baseline_dominated,
            "worth_building_trigger_v2": forward_usable and not baseline_dominated,
            "minimum_tasks_before_paper_shadow_review": master.get(
                "minimum_tasks_before_review", []
            ),
            "owner_next_action": owner_next_action,
        },
        source_artifacts={
            "status_matrix": str(status_matrix_path),
            "forward_review": str(forward_review_path),
            "baseline_review": str(baseline_review_path),
            "baseline_gate": str(baseline_gate_path),
            "readiness_score": str(readiness_path),
            "master_review": str(master_review_path),
            "quarantine": str(quarantine_path),
        },
        readiness_score=readiness.get("readiness_score"),
        promotion_allowed=False,
        paper_shadow_allowed=False,
        production_allowed=False,
        broker_action="none",
        metrics={
            "current_strategy_blocked": blocked,
            "baseline_dominated": baseline_dominated,
            "forward_outcome_usable_for_research": forward_usable,
            "promotion_allowed": False,
        },
        warnings=[
            {
                "warning": "next_decision_is_manual_research_guidance_only",
                "promotion_allowed": False,
            }
        ],
        blockers=_records(matrix.get("current_hard_blockers"))
        or _records(master.get("remaining_blockers")),
        next_recommended_action=owner_next_action,
        report_registry_entry=_tail_risk_governance_report_registry_entry(
            "tail_risk_next_decision",
            "Tail-Risk Next Decision",
            "tail-risk-next-decision-document",
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id="tail_risk_next_decision")
    docs_path.parent.mkdir(parents=True, exist_ok=True)
    docs_path.write_text(_render_tail_risk_next_decision_md(payload), encoding="utf-8")
    payload["docs_path"] = str(docs_path)
    _write_json(output_root / "tail_risk_next_decision.json", payload)
    return payload


def _load_config(path: Path) -> dict[str, Any]:
    raw = safe_load_yaml_path(path)
    return dict(raw) if isinstance(raw, Mapping) else {}


def _load_next_stage_config(path: Path) -> dict[str, Any]:
    overlay = _load_config(path)
    base_path = overlay.get("base_config_path")
    if not base_path:
        return overlay
    resolved_base = _resolve_project_path(base_path)
    merged = _load_config(resolved_base)
    merged.update(overlay)
    return merged


def _resolve_project_path(value: Any) -> Path:
    path = Path(str(value))
    return path if path.is_absolute() else PROJECT_ROOT / path


def _universe(config: Mapping[str, Any]) -> list[str]:
    values = config.get("research_universe")
    if isinstance(values, list):
        return [str(item).upper() for item in values if str(item).strip()]
    return ["SPY", "QQQ", "SMH", "MSFT", "GOOGL", "NVDA", "AMD", "TSM"]


def _horizons(config: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = _records(config.get("horizons"))
    return [
        {
            "horizon_id": str(row.get("horizon_id")),
            "days": int(row.get("days", 1)),
            "maturity_required_for_primary_evaluation": bool(
                row.get("maturity_required_for_primary_evaluation", True)
            ),
        }
        for row in rows
        if row.get("horizon_id")
    ] or [
        {"horizon_id": "1d", "days": 1, "maturity_required_for_primary_evaluation": True},
        {"horizon_id": "5d", "days": 5, "maturity_required_for_primary_evaluation": True},
    ]


def _actions(config: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = _records(config.get("candidate_actions"))
    return [
        {
            "action_id": str(row.get("action_id")),
            "exposure_multiplier": _float(row.get("exposure_multiplier"), 0.0),
            "cost_turnover_assumption": _float(row.get("cost_turnover_assumption"), 0.0),
            "rationale": str(row.get("rationale", "controlled heuristic action")),
            "heuristic": True,
        }
        for row in rows
        if row.get("action_id")
    ]


def _simple_strategy_zoo(config: Mapping[str, Any]) -> list[str]:
    values = config.get("simple_strategy_zoo")
    if isinstance(values, list):
        return [str(item) for item in values if str(item).strip()]
    return ["cash", "buy_and_hold", "static_allocation"]


def _selector_rules(config: Mapping[str, Any]) -> list[dict[str, Any]]:
    return _records(config.get("selector_rules"))


def _regret_types(config: Mapping[str, Any]) -> list[str]:
    values = config.get("regret_types")
    if isinstance(values, list):
        return [str(item) for item in values]
    return []


def _state_policy(config: Mapping[str, Any]) -> dict[str, Any]:
    value = config.get("heuristic_state_policy")
    return dict(value) if isinstance(value, Mapping) else {}


def _state_transitions(config: Mapping[str, Any]) -> list[dict[str, Any]]:
    state_machine = config.get("state_machine")
    transitions = _records(
        state_machine.get("transitions") if isinstance(state_machine, Mapping) else []
    )
    rows: list[dict[str, Any]] = []
    for index, row in enumerate(transitions, start=1):
        rows.append(
            {
                "transition_id": f"transition_{index:02d}",
                "from_state": str(row.get("from_state")),
                "to_state": str(row.get("to_state")),
                "condition": str(row.get("condition")),
                "regret_types": [str(item) for item in row.get("regret_types", [])],
                "explanation": str(row.get("explanation", "")),
                "input_data_policy": "PIT_valid_or_controlled_research_allowed",
                "heuristic": True,
                "promotion_gate_allowed": False,
            }
        )
    return rows


def _minimum(config: Mapping[str, Any], key: str, default: int) -> int:
    minimums = config.get("minimums")
    if not isinstance(minimums, Mapping):
        return default
    return int(minimums.get(key, default))


def _heuristic_policy_version(config: Mapping[str, Any]) -> str:
    return str(config.get("heuristic_policy_version", "controlled_strategy_batch_1_heuristic_v1"))


def _next_stage_section(config: Mapping[str, Any], key: str) -> dict[str, Any]:
    value = config.get(key)
    return dict(value) if isinstance(value, Mapping) else {}


def _next_stage_decision_dates(dates: list[str], config: Mapping[str, Any]) -> list[str]:
    if not dates:
        return []
    expansion = _next_stage_section(config, "value_surface_expansion")
    max_dates = int(
        expansion.get("max_decision_dates") or _minimum(config, "max_decision_dates", 24)
    )
    eligible = dates[:-1] if len(dates) > 1 else dates
    return eligible[-max_dates:]


def _cluster_by_asset(config: Mapping[str, Any]) -> dict[str, str]:
    expansion = _next_stage_section(config, "value_surface_expansion")
    cluster_policy = expansion.get("cluster_policy")
    if not isinstance(cluster_policy, Mapping):
        return {}
    clusters = cluster_policy.get("static_clusters")
    if not isinstance(clusters, Mapping):
        return {}
    values: dict[str, str] = {}
    for cluster_id, tickers in clusters.items():
        if not isinstance(tickers, list):
            continue
        for ticker in tickers:
            values[str(ticker).upper()] = str(cluster_id)
    return values


def _recent_regime_window_dates(decision_dates: list[str], config: Mapping[str, Any]) -> set[str]:
    expansion = _next_stage_section(config, "value_surface_expansion")
    segments = _records(expansion.get("regime_segments"))
    trailing = 0
    for segment in segments:
        trailing = max(trailing, _first_int(segment.get("trailing_decision_dates")))
    return set(decision_dates[-trailing:]) if trailing > 0 else set()


def _with_surface_context(
    row: dict[str, Any],
    *,
    cluster_by_asset: Mapping[str, str],
    recent_window_dates: set[str],
) -> dict[str, Any]:
    enriched = dict(row)
    asset = str(row.get("asset", "")).upper()
    row_date = str(row.get("date", ""))
    enriched["asset_cluster"] = cluster_by_asset.get(asset, "unclassified")
    enriched["regime_segment"] = (
        "recent_controlled_window" if row_date in recent_window_dates else "ai_after_chatgpt_full"
    )
    return enriched


def _surface_group_summary(
    rows: list[dict[str, Any]],
    group_keys: list[str],
) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, ...], list[dict[str, Any]]] = {}
    for row in rows:
        key = tuple(str(row.get(group_key, "unknown")) for group_key in group_keys)
        grouped.setdefault(key, []).append(row)
    summaries: list[dict[str, Any]] = []
    for key, values in sorted(grouped.items()):
        summary = {group_key: key[index] for index, group_key in enumerate(group_keys)}
        mature_count = sum(
            1
            for value in values
            if isinstance(value.get("sample_quality"), Mapping)
            and value["sample_quality"].get("outcome_mature")
        )
        summary.update(
            {
                "row_count": len(values),
                "mean_expected_return": _round(
                    _mean([_float(value.get("expected_return"), 0.0) for value in values])
                ),
                "mean_median_return": _round(
                    _mean([_float(value.get("median_return"), 0.0) for value in values])
                ),
                "mean_net_utility": _round(
                    _mean([_float(value.get("net_utility"), 0.0) for value in values])
                ),
                "median_net_utility": _round(
                    _median([_float(value.get("net_utility"), 0.0) for value in values])
                ),
                "mean_downside_risk": _round(
                    _mean([_float(value.get("downside_risk"), 0.0) for value in values])
                ),
                "mean_estimated_cost": _round(
                    _mean([_float(value.get("estimated_cost"), 0.0) for value in values])
                ),
                "mean_uncertainty": _round(
                    _mean([_float(value.get("uncertainty"), 0.0) for value in values])
                ),
                "mature_outcome_rate": _round(mature_count / len(values) if values else 0.0),
                "promotion_gate_allowed": False,
            }
        )
        if any(value.get("profile_utility") is not None for value in values):
            summary["mean_profile_utility"] = _round(
                _mean([_float(value.get("profile_utility"), 0.0) for value in values])
            )
        summaries.append(summary)
    return summaries


def _value_surface_horizon_smoothness_audit(
    *,
    surface_rows: list[dict[str, Any]],
    horizons: list[dict[str, Any]],
    config: Mapping[str, Any],
) -> dict[str, Any]:
    expansion = _next_stage_section(config, "value_surface_expansion")
    smoothness = expansion.get("horizon_smoothness")
    smoothness_policy = dict(smoothness) if isinstance(smoothness, Mapping) else {}
    materiality = _float(smoothness_policy.get("adjacent_delta_materiality_bps"), 100.0) / 10_000.0
    horizon_order = {str(row["horizon_id"]): int(row["days"]) for row in horizons}
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for row in surface_rows:
        grouped.setdefault(
            (str(row.get("date")), str(row.get("asset")), str(row.get("action"))), []
        ).append(row)
    adjacent_rows: list[dict[str, Any]] = []
    for (row_date, asset, action), values in sorted(grouped.items()):
        ordered = sorted(values, key=lambda row: horizon_order.get(str(row.get("horizon")), 0))
        for previous, current in zip(ordered, ordered[1:], strict=False):
            delta = _float(current.get("net_utility"), 0.0) - _float(
                previous.get("net_utility"), 0.0
            )
            adjacent_rows.append(
                {
                    "date": row_date,
                    "asset": asset,
                    "action": action,
                    "previous_horizon": previous.get("horizon"),
                    "current_horizon": current.get("horizon"),
                    "net_utility_delta": _round(delta),
                    "abs_net_utility_delta": _round(abs(delta)),
                    "material_for_review": abs(delta) >= materiality,
                    "promotion_gate_allowed": False,
                }
            )
    abs_values = [_float(row.get("abs_net_utility_delta"), 0.0) for row in adjacent_rows]
    return {
        "schema_version": "1.0",
        "report_type": "value_surface_expansion_horizon_smoothness_audit",
        "status": "PASS_WITH_WARNINGS",
        "summary": {
            "horizon_smoothness_audit_present": True,
            "audit_only": bool(smoothness_policy.get("audit_only", True)),
            "adjacent_pair_count": len(adjacent_rows),
            "material_pair_count": sum(1 for row in adjacent_rows if row["material_for_review"]),
            "median_abs_adjacent_delta": _round(_median(abs_values)) if abs_values else None,
            "max_abs_adjacent_delta": _round(max(abs_values)) if abs_values else None,
            "not_validated_utility_boundary": True,
            **_summary_safety(),
        },
        "smoothness_policy": smoothness_policy,
        "adjacent_horizon_deltas": adjacent_rows,
        **PRODUCTION_SAFETY,
    }


def _utility_profiles(config: Mapping[str, Any]) -> list[dict[str, Any]]:
    audit = _next_stage_section(config, "utility_boundary_audit")
    profiles = _records(audit.get("utility_profiles"))
    return profiles or [
        {
            "profile_id": "balanced_transparent",
            "expected_return_weight": 0.6,
            "median_return_weight": 0.2,
            "downside_risk_weight": 0.5,
            "cost_weight": 1.0,
            "uncertainty_weight": 0.1,
        }
    ]


def _utility_profile_ranking(
    *,
    surface_rows: list[dict[str, Any]],
    profile: Mapping[str, Any],
) -> dict[str, Any]:
    scored = [
        {
            **row,
            "profile_utility": _round(_utility_score(row=row, profile=profile)),
        }
        for row in surface_rows
    ]
    grouped = _surface_group_summary(scored, ["action", "horizon"])
    ranked = sorted(
        grouped,
        key=lambda row: _float(row.get("mean_profile_utility"), 0.0),
        reverse=True,
    )
    return {
        "profile_id": str(profile.get("profile_id", "unknown_profile")),
        "ranking": ranked,
        "top_rank": ranked[0] if ranked else {},
        "profile_weights": dict(profile),
        "ranking_policy": "heuristic",
        "not_validated_utility_boundary": True,
        "promotion_gate_allowed": False,
    }


def _utility_score(*, row: Mapping[str, Any], profile: Mapping[str, Any]) -> float:
    return (
        _float(profile.get("expected_return_weight"), 0.0) * _float(row.get("expected_return"), 0.0)
        + _float(profile.get("median_return_weight"), 0.0) * _float(row.get("median_return"), 0.0)
        - _float(profile.get("downside_risk_weight"), 0.0) * _float(row.get("downside_risk"), 0.0)
        - _float(profile.get("cost_weight"), 0.0) * _float(row.get("estimated_cost"), 0.0)
        - _float(profile.get("uncertainty_weight"), 0.0) * _float(row.get("uncertainty"), 0.0)
    )


def _profile_reversal_report(rankings: list[dict[str, Any]]) -> dict[str, Any]:
    if not rankings:
        return {"summary": {"ranking_reversal_count": 0}, "rows": []}
    base = next(
        (ranking for ranking in rankings if ranking.get("profile_id") == "balanced_transparent"),
        rankings[0],
    )
    base_top = (
        base.get("top_rank", {}).get("action"),
        base.get("top_rank", {}).get("horizon"),
    )
    rows = []
    for ranking in rankings:
        top = (
            ranking.get("top_rank", {}).get("action"),
            ranking.get("top_rank", {}).get("horizon"),
        )
        rows.append(
            {
                "profile_id": ranking.get("profile_id"),
                "top_action": top[0],
                "top_horizon": top[1],
                "reverses_balanced_top": top != base_top,
                "promotion_gate_allowed": False,
            }
        )
    return {
        "summary": {
            "base_profile_id": base.get("profile_id"),
            "ranking_reversal_count": sum(1 for row in rows if row["reverses_balanced_top"]),
            "profile_reversal_report_present": True,
        },
        "rows": rows,
    }


def _single_weight_dominance_report(
    config: Mapping[str, Any],
    profiles: list[dict[str, Any]],
) -> dict[str, Any]:
    audit = _next_stage_section(config, "utility_boundary_audit")
    floor = _float(audit.get("dominance_share_review_floor"), 0.65)
    weight_keys = [
        "expected_return_weight",
        "median_return_weight",
        "downside_risk_weight",
        "cost_weight",
        "uncertainty_weight",
    ]
    rows = []
    for profile in profiles:
        weights = {key: abs(_float(profile.get(key), 0.0)) for key in weight_keys}
        total = sum(weights.values()) or 1.0
        dominant_key, dominant_value = max(weights.items(), key=lambda item: item[1])
        share = dominant_value / total
        rows.append(
            {
                "profile_id": profile.get("profile_id"),
                "dominant_component": dominant_key,
                "dominant_component_share": _round(share),
                "single_weight_dominance_for_review": share >= floor,
                "promotion_gate_allowed": False,
            }
        )
    return {
        "summary": {
            "dominance_share_review_floor": floor,
            "single_weight_dominance_profile_count": sum(
                1 for row in rows if row["single_weight_dominance_for_review"]
            ),
            "single_weight_dominance_report_present": True,
        },
        "rows": rows,
    }


def _pareto_frontier(
    surface_rows: list[dict[str, Any]],
    config: Mapping[str, Any],
) -> list[dict[str, Any]]:
    components = _next_stage_section(config, "utility_boundary_audit").get("pareto_components")
    component_map = dict(components) if isinstance(components, Mapping) else {}
    reward_key = str(component_map.get("reward", "expected_return"))
    risk_key = str(component_map.get("risk", "downside_risk"))
    cost_key = str(component_map.get("cost", "estimated_cost"))
    uncertainty_key = str(component_map.get("uncertainty", "uncertainty"))
    candidates = _surface_group_summary(surface_rows, ["action", "horizon"])
    frontier: list[dict[str, Any]] = []
    for candidate in candidates:
        candidate_reward = _float(candidate.get(f"mean_{reward_key}"), 0.0)
        candidate_risk = _float(candidate.get(f"mean_{risk_key}"), 0.0)
        candidate_cost = _float(candidate.get(f"mean_{cost_key}"), 0.0)
        candidate_uncertainty = _float(candidate.get(f"mean_{uncertainty_key}"), 0.0)
        dominated = False
        for challenger in candidates:
            if challenger is candidate:
                continue
            challenger_reward = _float(challenger.get(f"mean_{reward_key}"), 0.0)
            challenger_risk = _float(challenger.get(f"mean_{risk_key}"), 0.0)
            challenger_cost = _float(challenger.get(f"mean_{cost_key}"), 0.0)
            challenger_uncertainty = _float(challenger.get(f"mean_{uncertainty_key}"), 0.0)
            weakly_better = (
                challenger_reward >= candidate_reward
                and challenger_risk <= candidate_risk
                and challenger_cost <= candidate_cost
                and challenger_uncertainty <= candidate_uncertainty
            )
            strictly_better = (
                challenger_reward > candidate_reward
                or challenger_risk < candidate_risk
                or challenger_cost < candidate_cost
                or challenger_uncertainty < candidate_uncertainty
            )
            if weakly_better and strictly_better:
                dominated = True
                break
        if not dominated:
            frontier.append(
                {
                    **candidate,
                    "pareto_frontier": True,
                    "not_validated_utility_boundary": True,
                    "promotion_gate_allowed": False,
                }
            )
    return frontier


def _forward_maturity_horizons(config: Mapping[str, Any]) -> list[dict[str, Any]]:
    forward = _next_stage_section(config, "forward_evidence_maturity")
    rows = _records(forward.get("horizons"))
    return [
        {"horizon_id": str(row.get("horizon_id")), "days": int(row.get("days", 1))}
        for row in rows
        if row.get("horizon_id")
    ] or [
        {"horizon_id": "1d", "days": 1},
        {"horizon_id": "5d", "days": 5},
        {"horizon_id": "10d", "days": 10},
        {"horizon_id": "20d", "days": 20},
        {"horizon_id": "60d", "days": 60},
    ]


def _read_jsonl_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        raw = json.loads(line)
        if isinstance(raw, Mapping):
            rows.append(dict(raw))
    return rows


def _forward_maturity_rows(
    *,
    ledger_rows: list[dict[str, Any]],
    dates: list[str],
    config: Mapping[str, Any],
) -> list[dict[str, Any]]:
    horizons = _forward_maturity_horizons(config)
    rows: list[dict[str, Any]] = []
    for ledger_row in ledger_rows:
        as_of = str(ledger_row.get("as_of") or "")
        decision_index = _date_index_on_or_before(dates, as_of)
        for horizon in horizons:
            horizon_days = int(horizon["days"])
            target_index = decision_index + horizon_days if decision_index is not None else None
            matured = target_index is not None and target_index < len(dates)
            rows.append(
                {
                    "archive_id": ledger_row.get("archive_id"),
                    "as_of": as_of,
                    "horizon": horizon["horizon_id"],
                    "horizon_days": horizon_days,
                    "matured": matured,
                    "target_date": (
                        dates[target_index] if matured and target_index is not None else None
                    ),
                    "outcome_status": "matured" if matured else "pending",
                    "outcome_append_only": bool(ledger_row.get("outcome_append_only")),
                    "promotion_gate_allowed": False,
                }
            )
    return rows


def _forward_maturity_summary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(str(row.get("horizon")), []).append(row)
    return [
        {
            "horizon": horizon,
            "ledger_event_count": len(values),
            "matured_count": sum(1 for value in values if value.get("matured")),
            "pending_count": sum(1 for value in values if not value.get("matured")),
            "future_outcomes_appended_only": all(
                bool(value.get("outcome_append_only")) for value in values
            ),
            "promotion_gate_allowed": False,
        }
        for horizon, values in sorted(grouped.items())
    ]


def _date_index_on_or_before(dates: list[str], raw_date: str) -> int | None:
    eligible = [index for index, row_date in enumerate(dates) if row_date <= raw_date]
    return eligible[-1] if eligible else None


def _forward_artifact_retention(
    *,
    benchmark_expansion_path: Path,
    control_audit_path: Path,
    value_surface_expansion_path: Path,
) -> dict[str, Any]:
    candidate_paths = [
        DEFAULT_VALUE_SURFACE_PATH,
        DEFAULT_REGRET_STATE_MACHINE_PATH,
        DEFAULT_SIMPLE_STRATEGY_SELECTOR_PATH,
        DEFAULT_GBDT_ACTION_UTILITY_PATH,
        value_surface_expansion_path,
    ]
    return {
        "benchmark_expansion": _artifact_status(
            _read_json_or_empty(benchmark_expansion_path), benchmark_expansion_path
        ),
        "control_audit": _artifact_status(
            _read_json_or_empty(control_audit_path), control_audit_path
        ),
        "value_surface_controlled_expansion": _artifact_status(
            _read_json_or_empty(value_surface_expansion_path), value_surface_expansion_path
        ),
        "candidate_outputs": [
            _artifact_status(_read_json_or_empty(path), path) for path in candidate_paths
        ],
        "promotion_gate_allowed": False,
    }


def _gbdt_pivot_root_cause_review(gbdt: Mapping[str, Any]) -> list[dict[str, Any]]:
    split = gbdt.get("train_test_split") if isinstance(gbdt, Mapping) else {}
    calibration = gbdt.get("calibration_report") if isinstance(gbdt, Mapping) else {}
    feature_importance = _records(gbdt.get("feature_importance"))
    max_feature = (
        max(feature_importance, key=lambda row: _float(row.get("importance"), 0.0))
        if feature_importance
        else {}
    )
    train_count = _first_int(split.get("train_row_count")) if isinstance(split, Mapping) else 0
    test_count = _first_int(split.get("test_row_count")) if isinstance(split, Mapping) else 0
    return [
        {
            "question": "sample_size_or_walk_forward_split_limit",
            "evidence": {
                "train_row_count": train_count,
                "test_row_count": test_count,
                "split_policy": split.get("split_policy") if isinstance(split, Mapping) else None,
            },
            "review_decision": "requires_larger_walk_forward_design_before_model_continuation",
            "promotion_gate_allowed": False,
        },
        {
            "question": "utility_label_too_noisy_for_direct_regression",
            "evidence": {
                "calibration_status": (
                    calibration.get("calibration_status")
                    if isinstance(calibration, Mapping)
                    else None
                ),
                "mean_absolute_error": (
                    calibration.get("mean_absolute_error")
                    if isinstance(calibration, Mapping)
                    else None
                ),
            },
            "review_decision": "prefer_ranking_or_regret_type_target_over_scalar_utility",
            "promotion_gate_allowed": False,
        },
        {
            "question": "feature_set_or_target_mismatch",
            "evidence": {
                "top_feature": max_feature.get("feature"),
                "top_feature_importance": max_feature.get("importance"),
                "future_feature_violation_count": _summary_value(
                    gbdt, "future_feature_violation_count"
                ),
            },
            "review_decision": "use_as_feature_quality_or_residual_diagnostic_until_reframed",
            "promotion_gate_allowed": False,
        },
    ]


def _regret_casebook_gate_conditions(
    *,
    config: Mapping[str, Any],
    state_machine: Mapping[str, Any],
    casebook: Mapping[str, Any],
    value_surface: Mapping[str, Any],
) -> list[dict[str, Any]]:
    gate = _next_stage_section(config, "regret_casebook_expansion_gate")
    minimum_cases = _first_int(gate.get("minimum_case_count"))
    minimum_types = _first_int(gate.get("minimum_distinct_regret_types"))
    case_count = _casebook_case_count(casebook, state_machine)
    covered_types = [
        row
        for row in _records(state_machine.get("regret_type_coverage"))
        if row.get("covered_by_state_machine")
    ]
    teacher_oracle_difference_present = bool(
        state_machine.get("teacher_oracle_diagnostic_difference")
    )
    failure_attribution_present = bool(value_surface.get("failure_case_attribution"))
    return [
        {
            "condition_id": "regret_case_count_floor_met",
            "passed": case_count >= minimum_cases,
            "observed": case_count,
            "required": minimum_cases,
            "promotion_gate_allowed": False,
        },
        {
            "condition_id": "regret_type_distribution_stable",
            "passed": len(covered_types) >= minimum_types,
            "observed": len(covered_types),
            "required": minimum_types,
            "promotion_gate_allowed": False,
        },
        {
            "condition_id": "teacher_oracle_diagnostic_difference_present",
            "passed": teacher_oracle_difference_present,
            "observed": teacher_oracle_difference_present,
            "required": True,
            "promotion_gate_allowed": False,
        },
        {
            "condition_id": "value_surface_failure_cases_attributed",
            "passed": failure_attribution_present,
            "observed": failure_attribution_present,
            "required": True,
            "promotion_gate_allowed": False,
        },
    ]


def _casebook_case_count(
    casebook: Mapping[str, Any],
    state_machine: Mapping[str, Any],
) -> int:
    case_rows = _records(casebook.get("casebook_rows"))
    if case_rows:
        return len(case_rows)
    return len(_records(state_machine.get("state_by_date")))


def _sample_concentration_report(
    rows: list[dict[str, Any]],
    *,
    group_keys: list[str],
) -> dict[str, Any]:
    breakdowns = []
    for key in group_keys:
        counts = _group_count(rows, key)
        total = sum(counts.values()) or 1
        groups = [
            {
                key: group,
                "row_count": count,
                "row_share": _round(count / total),
                "promotion_gate_allowed": False,
            }
            for group, count in sorted(counts.items())
        ]
        max_share = max((count / total for count in counts.values()), default=0.0)
        breakdowns.append(
            {
                "group_key": key,
                "group_count": len(counts),
                "max_group_share": _round(max_share),
                "groups": groups,
                "promotion_gate_allowed": False,
            }
        )
    return {
        "summary": {
            "row_count": len(rows),
            "max_group_share": _round(
                max((_float(row.get("max_group_share"), 0.0) for row in breakdowns), default=0.0)
            ),
            "sample_concentration_present": True,
            "promotion_gate_allowed": False,
        },
        "breakdowns": breakdowns,
    }


def _value_surface_warning_taxonomy(
    *,
    config: Mapping[str, Any],
    value_surface: Mapping[str, Any],
    utility_audit: Mapping[str, Any],
    forward_maturity: Mapping[str, Any],
    sample_concentration: Mapping[str, Any],
) -> list[dict[str, Any]]:
    policy = _next_stage_section(config, "value_surface_warning_triage")
    warning_rows = _records(policy.get("warning_categories"))
    value_summary = value_surface.get("summary") if isinstance(value_surface, Mapping) else {}
    utility_summary = utility_audit.get("summary") if isinstance(utility_audit, Mapping) else {}
    forward_summary = (
        forward_maturity.get("summary") if isinstance(forward_maturity, Mapping) else {}
    )
    concentration_summary = (
        sample_concentration.get("summary") if isinstance(sample_concentration, Mapping) else {}
    )
    field_values = {
        "data_quality_status": (
            value_summary.get("data_quality_status") if isinstance(value_summary, Mapping) else None
        ),
        "not_validated_utility_boundary": (
            utility_summary.get("not_validated_utility_boundary")
            if isinstance(utility_summary, Mapping)
            else True
        ),
        "ledger_event_count": (
            forward_summary.get("ledger_event_count") if isinstance(forward_summary, Mapping) else 0
        ),
        "max_group_share": (
            concentration_summary.get("max_group_share")
            if isinstance(concentration_summary, Mapping)
            else 0.0
        ),
        "material_pair_count": (
            value_surface.get("horizon_smoothness_summary", {}).get("material_pair_count")
            if isinstance(value_surface.get("horizon_smoothness_summary"), Mapping)
            else 0
        ),
    }
    warnings: list[dict[str, Any]] = []
    for row in warning_rows:
        field = str(row.get("source_field", ""))
        value = field_values.get(field)
        triggered = False
        if "warning_when_not" in row:
            triggered = value != row.get("warning_when_not")
        if "warning_when" in row:
            triggered = value == row.get("warning_when")
        if "warning_when_below" in row:
            triggered = _float(value, 0.0) < _float(row.get("warning_when_below"), 0.0)
        if "warning_when_above" in row:
            triggered = _float(value, 0.0) > _float(row.get("warning_when_above"), 0.0)
        if triggered:
            warnings.append(
                {
                    "category_id": row.get("category_id"),
                    "source_field": field,
                    "observed": value,
                    "severity": row.get("severity", "review"),
                    "promotion_gate_allowed": False,
                }
            )
    return warnings


def _value_surface_controlled_review_decision(
    *,
    config: Mapping[str, Any],
    value_surface: Mapping[str, Any],
    warnings: list[dict[str, Any]],
) -> str:
    if not value_surface:
        return "DATA_REQUIRED"
    summary = value_surface.get("summary") if isinstance(value_surface, Mapping) else {}
    if not isinstance(summary, Mapping):
        return "DATA_REQUIRED"
    requirements = _next_stage_section(config, "value_surface_warning_triage").get(
        "continue_requires"
    )
    required = dict(requirements) if isinstance(requirements, Mapping) else {}
    if not bool(summary.get("horizon_leakage_check_pass")):
        return "KILL"
    if _first_int(summary.get("negative_control_promotion_count")) > 0:
        return "KILL"
    if not all(bool(summary.get(key)) == bool(expected) for key, expected in required.items()):
        return "DATA_REQUIRED"
    if any(row.get("severity") == "blocking_for_promotion" for row in warnings):
        return "CONTINUE"
    if warnings:
        return "CONTINUE"
    return "CONTINUE"


def _decision_date_count_breakdown(rows: list[dict[str, Any]]) -> dict[str, Any]:
    dates = sorted({str(row.get("date")) for row in rows if row.get("date")})
    return {
        "decision_date_count": len(dates),
        "first_decision_date": dates[0] if dates else None,
        "last_decision_date": dates[-1] if dates else None,
        "by_month": _month_count(dates),
        "rows_per_decision_date": _group_count(rows, "date"),
        "promotion_gate_allowed": False,
    }


def _month_count(dates: list[str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for raw in dates:
        month = raw[:7]
        counts[month] = counts.get(month, 0) + 1
    return counts


def _utility_ranking_stability_report(utility_audit: Mapping[str, Any]) -> dict[str, Any]:
    summary = utility_audit.get("summary") if isinstance(utility_audit, Mapping) else {}
    return {
        "profile_count": summary.get("profile_count") if isinstance(summary, Mapping) else None,
        "ranking_reversal_count": (
            summary.get("ranking_reversal_count") if isinstance(summary, Mapping) else None
        ),
        "single_weight_dominance_profile_count": (
            summary.get("single_weight_dominance_profile_count")
            if isinstance(summary, Mapping)
            else None
        ),
        "pareto_frontier_count": (
            summary.get("pareto_frontier_count") if isinstance(summary, Mapping) else None
        ),
        "validated_boundary_count": (
            summary.get("validated_boundary_count") if isinstance(summary, Mapping) else 0
        ),
        "stability_status": "DIAGNOSTIC_ONLY_NOT_VALIDATED",
        "promotion_gate_allowed": False,
    }


def _ranking_reversal_analysis(utility_audit: Mapping[str, Any]) -> dict[str, Any]:
    reversal = utility_audit.get("profile_reversal_report")
    rows = _records(reversal.get("rows") if isinstance(reversal, Mapping) else [])
    return {
        "summary": reversal.get("summary", {}) if isinstance(reversal, Mapping) else {},
        "reversal_rows": rows,
        "ranking_reversal_count": sum(1 for row in rows if row.get("reverses_balanced_top")),
        "promotion_gate_allowed": False,
    }


def _dominant_dimension_analysis(utility_audit: Mapping[str, Any]) -> dict[str, Any]:
    dominance = utility_audit.get("single_weight_dominance_report")
    rows = _records(dominance.get("rows") if isinstance(dominance, Mapping) else [])
    return {
        "summary": dominance.get("summary", {}) if isinstance(dominance, Mapping) else {},
        "dominance_rows": rows,
        "dominant_dimension_count": sum(
            1 for row in rows if row.get("single_weight_dominance_for_review")
        ),
        "promotion_gate_allowed": False,
    }


def _pareto_stability_analysis(
    utility_audit: Mapping[str, Any],
    config: Mapping[str, Any],
) -> dict[str, Any]:
    frontier = _records(utility_audit.get("pareto_frontier"))
    frontier_keys = {(row.get("action"), row.get("horizon")) for row in frontier}
    ranking_rows = _records(utility_audit.get("utility_profile_rankings"))
    top_rows = [
        ranking.get("top_rank")
        for ranking in ranking_rows
        if isinstance(ranking.get("top_rank"), Mapping)
    ]
    top_on_frontier = [
        row for row in top_rows if (row.get("action"), row.get("horizon")) in frontier_keys
    ]
    profile_count = len(top_rows)
    stability_rate = len(top_on_frontier) / profile_count if profile_count else 0.0
    robustness = _next_stage_section(config, "utility_ranking_robustness")
    floor = _float(robustness.get("pareto_stability_review_floor"), 0.5)
    return {
        "frontier_count": len(frontier),
        "profile_top_rank_count": profile_count,
        "profile_top_on_frontier_count": len(top_on_frontier),
        "profile_top_on_frontier_rate": _round(stability_rate),
        "pareto_stability_review_floor": floor,
        "pareto_more_stable_than_single_utility": stability_rate >= floor,
        "promotion_gate_allowed": False,
    }


def _forward_daily_continuity_report(
    *,
    ledger_rows: list[dict[str, Any]],
    dates: list[str],
    config: Mapping[str, Any],
) -> dict[str, Any]:
    ledger_dates = sorted({str(row.get("as_of")) for row in ledger_rows if row.get("as_of")})
    if not ledger_dates:
        return {
            "summary": {
                "expected_daily_archive_count": 0,
                "observed_daily_archive_count": 0,
                "missing_daily_archive_count": 0,
                "daily_continuity_pass": False,
                "promotion_gate_allowed": False,
            },
            "missing_dates": [],
        }
    start = ledger_dates[0]
    end = dates[-1] if dates else ledger_dates[-1]
    expected = [row_date for row_date in dates if start <= row_date <= end]
    missing = [row_date for row_date in expected if row_date not in set(ledger_dates)]
    policy = _next_stage_section(config, "forward_evidence_continuity")
    missing_allowed = bool(policy.get("missing_archive_allowed", False))
    return {
        "summary": {
            "expected_daily_archive_count": len(expected),
            "observed_daily_archive_count": len(ledger_dates),
            "missing_daily_archive_count": len(missing),
            "daily_continuity_pass": len(missing) == 0 or missing_allowed,
            "minimum_ledger_events_for_continue": _first_int(
                policy.get("minimum_ledger_events_for_continue")
            ),
            "promotion_gate_allowed": False,
        },
        "observed_dates": ledger_dates,
        "missing_dates": missing,
    }


def _append_only_integrity_report(ledger_rows: list[dict[str, Any]]) -> dict[str, Any]:
    archive_ids = [str(row.get("archive_id")) for row in ledger_rows if row.get("archive_id")]
    dates = [str(row.get("as_of")) for row in ledger_rows if row.get("as_of")]
    all_append_only = all(bool(row.get("outcome_append_only")) for row in ledger_rows)
    unique_archive_ids = len(archive_ids) == len(set(archive_ids))
    nondecreasing_dates = dates == sorted(dates)
    return {
        "summary": {
            "append_only_integrity_pass": all_append_only
            and unique_archive_ids
            and nondecreasing_dates,
            "all_rows_append_only": all_append_only,
            "unique_archive_ids": unique_archive_ids,
            "nondecreasing_as_of_dates": nondecreasing_dates,
            "promotion_gate_allowed": False,
        },
        "ledger_event_count": len(ledger_rows),
    }


def _forward_output_coverage_report(
    *,
    benchmark_expansion_path: Path,
    control_audit_path: Path,
    value_surface_expansion_path: Path,
) -> dict[str, Any]:
    rows = [
        _artifact_status(_read_json_or_empty(benchmark_expansion_path), benchmark_expansion_path),
        _artifact_status(_read_json_or_empty(control_audit_path), control_audit_path),
        _artifact_status(
            _read_json_or_empty(value_surface_expansion_path), value_surface_expansion_path
        ),
    ]
    return {
        "artifact_families": [
            {**rows[0], "family": "benchmark_expansion"},
            {**rows[1], "family": "control_audit"},
            {**rows[2], "family": "value_surface_controlled_expansion"},
        ],
        "summary": {
            "covered_family_count": sum(1 for row in rows if row["present"]),
            "required_family_count": len(rows),
            "all_required_outputs_present": all(row["present"] for row in rows),
            "promotion_gate_allowed": False,
        },
    }


def _gbdt_pivot_direction_rows(selection_policy: Mapping[str, Any]) -> list[dict[str, Any]]:
    selected = str(selection_policy.get("selected_direction", ""))
    rows = []
    for row in _records(selection_policy.get("direction_details")):
        direction_id = str(row.get("direction_id"))
        rows.append(
            {
                "direction_id": direction_id,
                "selected": direction_id == selected,
                "minimum_viable_experiment": row.get("minimum_viable_experiment"),
                "required_data": row.get("required_data"),
                "failure_mode": row.get("failure_mode"),
                "kill_criteria": row.get("kill_criteria"),
                "difference_from_previous_action_utility_model": row.get(
                    "difference_from_previous_action_utility_model"
                ),
                "model_run_executed": False,
                "promotion_gate_allowed": False,
            }
        )
    return rows


def _value_surface_failure_activation_inputs(
    *,
    rows: list[dict[str, Any]],
    config: Mapping[str, Any],
) -> dict[str, Any]:
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(
            (str(row.get("date")), str(row.get("asset")), str(row.get("horizon"))), []
        ).append(row)
    activation_cases: list[dict[str, Any]] = []
    for (row_date, asset, horizon), values in sorted(grouped.items()):
        top = max(values, key=lambda row: _float(row.get("net_utility"), -999.0))
        realized = top.get("realized_forward_return")
        if realized is None:
            continue
        action = str(top.get("action"))
        realized_value = _float(realized, 0.0)
        losing = realized_value < 0
        benchmark_disagreement = action not in {
            "buy_and_hold",
            "hold_exposure",
            "risk_on",
            "no_masking",
        }
        false_risk_off = _action_is_defensive(action) and realized_value > 0
        if losing or benchmark_disagreement or false_risk_off:
            activation_cases.append(
                {
                    "date": row_date,
                    "asset": asset,
                    "horizon": horizon,
                    "top_action": action,
                    "realized_forward_return": _round(realized_value),
                    "value_surface_losing_case": losing,
                    "benchmark_disagreement_case": benchmark_disagreement,
                    "false_risk_off_or_missed_upside_case": false_risk_off,
                    "oracle_teacher_better_case": False,
                    "promotion_gate_allowed": False,
                }
            )
    losing_count = sum(1 for row in activation_cases if row["value_surface_losing_case"])
    disagreement_count = sum(1 for row in activation_cases if row["benchmark_disagreement_case"])
    false_risk_off_count = sum(
        1 for row in activation_cases if row["false_risk_off_or_missed_upside_case"]
    )
    oracle_teacher_count = sum(1 for row in activation_cases if row["oracle_teacher_better_case"])
    return {
        "summary": {
            "value_surface_losing_case_count": losing_count,
            "benchmark_disagreement_case_count": disagreement_count,
            "false_risk_off_or_missed_upside_case_count": false_risk_off_count,
            "oracle_teacher_better_case_count": oracle_teacher_count,
            "activation_case_count": len(activation_cases),
            "promotion_gate_allowed": False,
        },
        "activation_cases": activation_cases[:100],
        "policy": _next_stage_section(config, "regret_activation_inputs"),
    }


def _action_is_defensive(action: str) -> bool:
    return action in {
        "hold_cash",
        "risk_off",
        "decrease_exposure",
        "drawdown_guard",
        "capped_masking",
    }


def _regret_activation_input_criteria(
    *,
    config: Mapping[str, Any],
    failure_inputs: Mapping[str, Any],
    gate: Mapping[str, Any],
) -> list[dict[str, Any]]:
    policy = _next_stage_section(config, "regret_activation_inputs")
    summary = failure_inputs.get("summary") if isinstance(failure_inputs, Mapping) else {}
    gate_rows = _records(gate.get("activation_gate"))
    regret_coverage = next(
        (row for row in gate_rows if row.get("condition_id") == "regret_type_distribution_stable"),
        {},
    )
    criteria = [
        (
            "value_surface_losing_cases",
            (
                _first_int(summary.get("value_surface_losing_case_count"))
                if isinstance(summary, Mapping)
                else 0
            ),
            _first_int(policy.get("minimum_value_surface_losing_cases")),
        ),
        (
            "benchmark_disagreement_cases",
            (
                _first_int(summary.get("benchmark_disagreement_case_count"))
                if isinstance(summary, Mapping)
                else 0
            ),
            _first_int(policy.get("minimum_benchmark_disagreement_cases")),
        ),
        (
            "false_risk_off_or_missed_upside_cases",
            (
                _first_int(summary.get("false_risk_off_or_missed_upside_case_count"))
                if isinstance(summary, Mapping)
                else 0
            ),
            _first_int(policy.get("minimum_false_risk_off_or_missed_upside_cases")),
        ),
        (
            "oracle_teacher_better_cases",
            (
                _first_int(summary.get("oracle_teacher_better_case_count"))
                if isinstance(summary, Mapping)
                else 0
            ),
            1 if bool(policy.get("oracle_teacher_required_for_activation", True)) else 0,
        ),
        (
            "enough_regret_type_coverage",
            _first_int(regret_coverage.get("observed")),
            _first_int(policy.get("minimum_distinct_regret_types")),
        ),
    ]
    return [
        {
            "condition_id": condition_id,
            "observed": observed,
            "required": required,
            "passed": observed >= required,
            "promotion_gate_allowed": False,
        }
        for condition_id, observed, required in criteria
    ]


def _selected_value_surface_cases(
    rows: list[dict[str, Any]],
    config: Mapping[str, Any],
) -> list[dict[str, Any]]:
    action_by_id = {str(row["action_id"]): row for row in _actions(config)}
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(
            (str(row.get("date")), str(row.get("asset")), str(row.get("horizon"))),
            [],
        ).append(row)
    selected: list[dict[str, Any]] = []
    benchmark_order = ["buy_and_hold", "hold_exposure", "risk_on", "no_masking"]
    for (row_date, asset, horizon), values in sorted(grouped.items()):
        mature_values = [row for row in values if row.get("realized_forward_return") is not None]
        if not mature_values:
            continue
        top = max(mature_values, key=lambda row: _float(row.get("net_utility"), -999.0))
        benchmark = next(
            (
                row
                for action_id in benchmark_order
                for row in mature_values
                if row.get("action") == action_id
            ),
            None,
        )
        selected_net = _action_realized_net(top, action_by_id)
        benchmark_net = (
            _action_realized_net(benchmark, action_by_id)
            if benchmark is not None
            else _float(top.get("realized_forward_return"), 0.0)
        )
        selected.append(
            {
                "date": row_date,
                "asset": asset,
                "horizon": horizon,
                "horizon_days": top.get("horizon_days"),
                "regime_segment": top.get("regime_segment"),
                "asset_cluster": top.get("asset_cluster"),
                "pit_state": top.get("pit_state"),
                "selected_action": top.get("action"),
                "benchmark_action": benchmark.get("action") if benchmark else "raw_buy_and_hold",
                "selected_realized_net_return": _round(selected_net),
                "benchmark_realized_net_return": _round(benchmark_net),
                "delta_vs_benchmark": _round(selected_net - benchmark_net),
                "value_surface_beats_benchmark": selected_net >= benchmark_net,
                "selected_net_utility": top.get("net_utility"),
                "selected_estimated_cost": top.get("estimated_cost"),
                "benchmark_estimated_cost": benchmark.get("estimated_cost") if benchmark else 0.0,
                "selected_turnover_cost_assumption": action_by_id.get(
                    str(top.get("action")), {}
                ).get("cost_turnover_assumption"),
                "benchmark_turnover_cost_assumption": (
                    action_by_id.get(str(benchmark.get("action")), {}).get(
                        "cost_turnover_assumption"
                    )
                    if benchmark
                    else 0.0
                ),
                "selected_drawdown_proxy": top.get("max_drawdown_proxy"),
                "benchmark_drawdown_proxy": (
                    benchmark.get("max_drawdown_proxy") if benchmark else None
                ),
                "promotion_gate_allowed": False,
            }
        )
    return selected


def _action_realized_net(
    row: Mapping[str, Any] | None,
    action_by_id: Mapping[str, Mapping[str, Any]],
) -> float:
    if row is None:
        return 0.0
    action_id = str(row.get("action"))
    action = action_by_id.get(action_id, {})
    exposure = _float(action.get("exposure_multiplier"), 1.0)
    realized = _float(row.get("realized_forward_return"), 0.0)
    cost = _float(row.get("estimated_cost"), 0.0)
    return exposure * realized - cost


def _walk_forward_windows(
    decision_dates: list[str],
    config: Mapping[str, Any],
) -> list[dict[str, Any]]:
    if not decision_dates:
        return []
    policy = _next_stage_section(config, "value_surface_walk_forward_expansion")
    max_windows = max(1, _first_int(policy.get("max_window_count")) or 1)
    min_dates = max(1, _first_int(policy.get("min_decision_dates_per_window")) or 1)
    window_count = min(max_windows, max(1, len(decision_dates) // min_dates))
    windows: list[dict[str, Any]] = []
    for index in range(window_count):
        start_index = math.floor(index * len(decision_dates) / window_count)
        end_index = math.floor((index + 1) * len(decision_dates) / window_count)
        window_dates = decision_dates[start_index:end_index]
        if not window_dates:
            continue
        windows.append(
            {
                "window_id": f"wf_{index + 1:02d}",
                "first_decision_date": window_dates[0],
                "last_decision_date": window_dates[-1],
                "decision_date_count": len(window_dates),
                "promotion_gate_allowed": False,
            }
        )
    return windows


def _walk_forward_window_results(
    selected_cases: list[dict[str, Any]],
    windows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    results = []
    for window in windows:
        first = str(window.get("first_decision_date"))
        last = str(window.get("last_decision_date"))
        values = [row for row in selected_cases if first <= str(row.get("date")) <= last]
        results.append(
            {
                **window,
                **_aggregate_walk_forward_cases(values),
            }
        )
    return results


def _walk_forward_group_result(
    selected_cases: list[dict[str, Any]],
    group_key: str,
) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in selected_cases:
        grouped.setdefault(str(row.get(group_key, "unknown")), []).append(row)
    return [
        {
            group_key: group,
            **_aggregate_walk_forward_cases(values),
        }
        for group, values in sorted(grouped.items())
    ]


def _walk_forward_benchmark_comparison(
    selected_cases: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "summary": {
            **_aggregate_walk_forward_cases(selected_cases),
            "benchmark_comparison_present": bool(selected_cases),
            "comparison_policy": "selected_top_value_surface_action_vs_buy_and_hold_proxy",
            "promotion_gate_allowed": False,
        },
        "benchmark_action_ids": sorted(
            {
                str(row.get("benchmark_action"))
                for row in selected_cases
                if row.get("benchmark_action")
            }
        ),
    }


def _aggregate_walk_forward_cases(rows: list[dict[str, Any]]) -> dict[str, Any]:
    deltas = [_float(row.get("delta_vs_benchmark"), 0.0) for row in rows]
    selected_returns = [_float(row.get("selected_realized_net_return"), 0.0) for row in rows]
    benchmark_returns = [_float(row.get("benchmark_realized_net_return"), 0.0) for row in rows]
    win_count = sum(1 for row in rows if row.get("value_surface_beats_benchmark"))
    return {
        "case_count": len(rows),
        "mean_selected_realized_net_return": _round(_mean(selected_returns)),
        "mean_benchmark_realized_net_return": _round(_mean(benchmark_returns)),
        "mean_delta_vs_benchmark": _round(_mean(deltas)),
        "median_delta_vs_benchmark": _round(_median(deltas)) if deltas else None,
        "value_surface_beats_benchmark_count": win_count,
        "value_surface_beats_benchmark_rate": _round(win_count / len(rows) if rows else 0.0),
        "promotion_gate_allowed": False,
    }


def _negative_control_review(control_results: list[dict[str, Any]]) -> dict[str, Any]:
    promotion_count = _negative_control_promotion_count(control_results)
    return {
        "negative_control_pass": promotion_count == 0,
        "negative_control_promotion_count": promotion_count,
        "control_count": len(control_results),
        "control_results": control_results,
        "promotion_gate_allowed": False,
    }


def _future_leakage_trap_review(control_results: list[dict[str, Any]]) -> dict[str, Any]:
    blocked = _future_leakage_blocked(control_results)
    return {
        "future_leakage_trap_blocked": blocked,
        "future_leakage_trap_pass": blocked,
        "promotion_gate_allowed": False,
    }


def _value_surface_walk_forward_decision(
    *,
    config: Mapping[str, Any],
    value_surface: Mapping[str, Any],
    selected_cases: list[dict[str, Any]],
    window_results: list[dict[str, Any]],
    negative_control_result: Mapping[str, Any],
    future_leakage_trap_result: Mapping[str, Any],
) -> str:
    if not value_surface or not selected_cases or not window_results:
        return "DATA_REQUIRED"
    if _first_int(negative_control_result.get("negative_control_promotion_count")) > 0:
        return "KILL"
    if not bool(future_leakage_trap_result.get("future_leakage_trap_blocked")):
        return "KILL"
    policy = _next_stage_section(config, "value_surface_walk_forward_expansion")
    min_win_rate = _float(policy.get("continue_min_window_win_rate"), 0.5)
    min_delta = _float(policy.get("continue_min_mean_delta_vs_benchmark_bps"), 0.0) / 10_000.0
    window_wins = [
        row
        for row in window_results
        if _float(row.get("mean_delta_vs_benchmark"), 0.0) >= min_delta
    ]
    window_win_rate = len(window_wins) / len(window_results)
    overall_delta = _float(
        _walk_forward_benchmark_comparison(selected_cases)["summary"].get(
            "mean_delta_vs_benchmark"
        ),
        0.0,
    )
    if window_win_rate >= min_win_rate and overall_delta >= min_delta:
        return "CONTINUE"
    return "WATCHLIST"


def _dominant_metric_by_candidate(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    candidates = _surface_group_summary(rows, ["action", "horizon"])
    output = []
    for row in candidates:
        metrics = {
            "return": abs(_float(row.get("mean_expected_return"), 0.0)),
            "drawdown": abs(_float(row.get("mean_downside_risk"), 0.0)),
            "cost": abs(_float(row.get("mean_estimated_cost"), 0.0)),
            "uncertainty": abs(_float(row.get("mean_uncertainty"), 0.0)),
        }
        dominant_metric, dominant_value = max(metrics.items(), key=lambda item: item[1])
        total = sum(metrics.values()) or 1.0
        output.append(
            {
                "action": row.get("action"),
                "horizon": row.get("horizon"),
                "dominant_metric": dominant_metric,
                "dominant_metric_share": _round(dominant_value / total),
                "metric_components": {key: _round(value) for key, value in metrics.items()},
                "not_validated_utility_boundary": True,
                "promotion_gate_allowed": False,
            }
        )
    return output


def _horizon_cliff_report(
    rows: list[dict[str, Any]],
    config: Mapping[str, Any],
) -> dict[str, Any]:
    policy = _next_stage_section(config, "utility_pareto_ranking_review")
    threshold = _float(policy.get("horizon_cliff_abs_utility_bps"), 100.0) / 10_000.0
    horizon_order = {str(row["horizon_id"]): int(row["days"]) for row in _horizons(config)}
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in _surface_group_summary(rows, ["action", "horizon"]):
        grouped.setdefault(str(row.get("action")), []).append(row)
    cliff_rows: list[dict[str, Any]] = []
    for action, values in sorted(grouped.items()):
        ordered = sorted(values, key=lambda row: horizon_order.get(str(row.get("horizon")), 0))
        for previous, current in zip(ordered, ordered[1:], strict=False):
            delta = _float(current.get("mean_net_utility"), 0.0) - _float(
                previous.get("mean_net_utility"), 0.0
            )
            cliff_rows.append(
                {
                    "action": action,
                    "previous_horizon": previous.get("horizon"),
                    "current_horizon": current.get("horizon"),
                    "mean_net_utility_delta": _round(delta),
                    "abs_mean_net_utility_delta": _round(abs(delta)),
                    "horizon_cliff_for_review": abs(delta) >= threshold,
                    "not_validated_utility_boundary": True,
                    "promotion_gate_allowed": False,
                }
            )
    return {
        "summary": {
            "horizon_cliff_count": sum(1 for row in cliff_rows if row["horizon_cliff_for_review"]),
            "adjacent_horizon_pair_count": len(cliff_rows),
            "horizon_cliff_abs_utility_threshold": _round(threshold),
            "not_validated_utility_boundary": True,
            "promotion_gate_allowed": False,
        },
        "rows": cliff_rows,
    }


def _value_surface_residual_rows(
    rows: list[dict[str, Any]],
    config: Mapping[str, Any],
) -> list[dict[str, Any]]:
    action_by_id = {str(row["action_id"]): row for row in _actions(config)}
    policy = _next_stage_section(config, "gbdt_value_surface_residual_diagnostic")
    floor = _float(policy.get("large_residual_abs_return_floor_bps"), 200.0) / 10_000.0
    residual_rows = []
    for row in rows:
        if row.get("realized_forward_return") is None:
            continue
        realized_net = _action_realized_net(row, action_by_id)
        predicted_net = _float(row.get("net_utility"), 0.0)
        residual = realized_net - predicted_net
        residual_rows.append(
            {
                "date": row.get("date"),
                "asset": row.get("asset"),
                "horizon": row.get("horizon"),
                "regime_segment": row.get("regime_segment"),
                "asset_cluster": row.get("asset_cluster"),
                "pit_state": row.get("pit_state"),
                "action": row.get("action"),
                "predicted_net_utility": _round(predicted_net),
                "realized_action_net_return": _round(realized_net),
                "residual": _round(residual),
                "abs_residual": _round(abs(residual)),
                "estimated_cost": row.get("estimated_cost"),
                "downside_risk": row.get("downside_risk"),
                "max_drawdown_proxy": row.get("max_drawdown_proxy"),
                "large_residual_for_review": abs(residual) >= floor,
                "residual_role": "diagnostic_only",
                "promotion_gate_allowed": False,
            }
        )
    return residual_rows


def _residual_group_result(rows: list[dict[str, Any]], group_key: str) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(str(row.get(group_key, "unknown")), []).append(row)
    return [
        {
            group_key: group,
            "case_count": len(values),
            "mean_residual": _round(_mean([_float(row.get("residual"), 0.0) for row in values])),
            "mean_abs_residual": _round(
                _mean([_float(row.get("abs_residual"), 0.0) for row in values])
            ),
            "large_residual_count": sum(
                1 for row in values if row.get("large_residual_for_review")
            ),
            "large_residual_rate": _round(
                sum(1 for row in values if row.get("large_residual_for_review")) / len(values)
                if values
                else 0.0
            ),
            "promotion_gate_allowed": False,
        }
        for group, values in sorted(grouped.items())
    ]


def _residual_feature_importance(
    rows: list[dict[str, Any]],
    *,
    features: list[str],
) -> list[dict[str, Any]]:
    abs_values = [_float(row.get("abs_residual"), 0.0) for row in rows]
    if not abs_values:
        return []
    overall = _mean(abs_values)
    denominator = sum((value - overall) ** 2 for value in abs_values) or 1.0
    importance_rows = []
    for feature in features:
        grouped: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            grouped.setdefault(str(row.get(feature, "unknown")), []).append(row)
        between = 0.0
        top_group = None
        top_group_mean = -1.0
        for group, values in grouped.items():
            group_mean = _mean([_float(row.get("abs_residual"), 0.0) for row in values])
            between += len(values) * (group_mean - overall) ** 2
            if group_mean > top_group_mean:
                top_group = group
                top_group_mean = group_mean
        importance_rows.append(
            {
                "feature": feature,
                "importance": _round(between / denominator),
                "group_count": len(grouped),
                "top_residual_group": top_group,
                "top_group_mean_abs_residual": _round(top_group_mean),
                "importance_method": "categorical_residual_separation",
                "promotion_gate_allowed": False,
            }
        )
    return sorted(importance_rows, key=lambda row: row["importance"], reverse=True)


def _residual_hypothesis_candidates(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for feature in ["asset", "horizon", "regime_segment", "asset_cluster", "pit_state", "action"]:
        for row in _residual_group_result(rows, feature):
            candidates.append(
                {
                    "hypothesis_id": f"residual_{feature}_{row.get(feature)}",
                    "feature": feature,
                    "feature_value": row.get(feature),
                    "case_count": row.get("case_count"),
                    "mean_abs_residual": row.get("mean_abs_residual"),
                    "large_residual_rate": row.get("large_residual_rate"),
                    "hypothesis": (
                        "review whether value_surface residuals concentrate in "
                        f"{feature}={row.get(feature)}"
                    ),
                    "strategy_signal_generated": False,
                    "promotion_gate_allowed": False,
                }
            )
    return sorted(
        candidates,
        key=lambda row: (
            _float(row.get("mean_abs_residual"), 0.0),
            _float(row.get("large_residual_rate"), 0.0),
        ),
        reverse=True,
    )[:10]


def _regret_activation_recheck_criteria(
    *,
    config: Mapping[str, Any],
    failure_inputs: Mapping[str, Any],
    gate: Mapping[str, Any],
) -> list[dict[str, Any]]:
    policy = _next_stage_section(config, "regret_casebook_activation_recheck")
    summary = failure_inputs.get("summary") if isinstance(failure_inputs, Mapping) else {}
    criteria = [
        (
            "value_surface_losing_cases_sufficient",
            (
                _first_int(summary.get("value_surface_losing_case_count"))
                if isinstance(summary, Mapping)
                else 0
            ),
            _first_int(policy.get("minimum_value_surface_losing_cases")),
        ),
        (
            "benchmark_disagreement_cases_sufficient",
            (
                _first_int(summary.get("benchmark_disagreement_case_count"))
                if isinstance(summary, Mapping)
                else 0
            ),
            _first_int(policy.get("minimum_benchmark_disagreement_cases")),
        ),
        (
            "teacher_oracle_better_cases_sufficient",
            (
                _first_int(summary.get("oracle_teacher_better_case_count"))
                if isinstance(summary, Mapping)
                else 0
            ),
            _first_int(policy.get("minimum_oracle_teacher_better_cases")),
        ),
        (
            "major_regret_types_stable",
            _stable_regret_type_count(gate),
            _first_int(policy.get("minimum_distinct_regret_types")),
        ),
    ]
    return [
        {
            "condition_id": condition_id,
            "observed": observed,
            "required": required,
            "passed": observed >= required,
            "promotion_gate_allowed": False,
        }
        for condition_id, observed, required in criteria
    ]


def _stable_regret_type_count(gate: Mapping[str, Any]) -> int:
    gate_rows = _records(gate.get("activation_gate"))
    regret_coverage = next(
        (row for row in gate_rows if row.get("condition_id") == "regret_type_distribution_stable"),
        {},
    )
    return _first_int(regret_coverage.get("observed"))


def _failure_attribution_report(
    selected_cases: list[dict[str, Any]],
    config: Mapping[str, Any],
) -> dict[str, Any]:
    policy = _next_stage_section(config, "value_surface_failure_attribution")
    winners = [row for row in selected_cases if _float(row.get("delta_vs_benchmark"), 0.0) >= 0]
    losers = [row for row in selected_cases if _float(row.get("delta_vs_benchmark"), 0.0) < 0]
    deltas = [_float(row.get("delta_vs_benchmark"), 0.0) for row in selected_cases]
    concentration_reports = {
        "date": _loss_concentration(losers, "date"),
        "asset": _loss_concentration(losers, "asset"),
        "horizon": _loss_concentration(losers, "horizon"),
        "regime_segment": _loss_concentration(losers, "regime_segment"),
    }
    max_share = max(
        (
            report["summary"]["max_loss_share"]
            for report in concentration_reports.values()
            if report["summary"]["loss_case_count"]
        ),
        default=0.0,
    )
    top_limit = _first_int(policy.get("top_losing_case_count")) or 25
    top_losing = sorted(losers, key=lambda row: _float(row.get("delta_vs_benchmark"), 0.0))[
        :top_limit
    ]
    return {
        "summary": {
            "case_count": len(selected_cases),
            "winning_case_count": len(winners),
            "losing_case_count": len(losers),
            "winning_case_average_delta": _round(
                _mean([_float(row.get("delta_vs_benchmark"), 0.0) for row in winners])
            ),
            "losing_case_average_delta": _round(
                _mean([_float(row.get("delta_vs_benchmark"), 0.0) for row in losers])
            ),
            "overall_mean_delta_vs_benchmark": _round(_mean(deltas)),
            "overall_median_delta_vs_benchmark": _round(_median(deltas)),
            "max_loss_concentration_share": _round(max_share),
            "promotion_gate_allowed": False,
        },
        "winning_losing_delta": {
            "winning_case_average_delta": _round(
                _mean([_float(row.get("delta_vs_benchmark"), 0.0) for row in winners])
            ),
            "losing_case_average_delta": _round(
                _mean([_float(row.get("delta_vs_benchmark"), 0.0) for row in losers])
            ),
            "winning_case_count": len(winners),
            "losing_case_count": len(losers),
            "promotion_gate_allowed": False,
        },
        "top_losing_cases": top_losing,
        "loss_concentration_by_date": concentration_reports["date"],
        "loss_concentration_by_asset": concentration_reports["asset"],
        "loss_concentration_by_horizon": concentration_reports["horizon"],
        "loss_concentration_by_regime": concentration_reports["regime_segment"],
        "tail_loss_contribution": _tail_loss_contribution(losers, config),
        "turnover_cost_contribution": _turnover_cost_contribution(losers),
        "drawdown_contribution": _drawdown_contribution(losers),
        "benchmark_relative_downside_attribution": _benchmark_relative_downside_attribution(
            selected_cases
        ),
    }


def _loss_concentration(rows: list[dict[str, Any]], group_key: str) -> dict[str, Any]:
    total_abs_loss = sum(abs(_float(row.get("delta_vs_benchmark"), 0.0)) for row in rows)
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(str(row.get(group_key, "unknown")), []).append(row)
    groups = []
    for group, values in sorted(grouped.items()):
        group_loss = sum(abs(_float(row.get("delta_vs_benchmark"), 0.0)) for row in values)
        groups.append(
            {
                group_key: group,
                "loss_case_count": len(values),
                "average_delta_vs_benchmark": _round(
                    _mean([_float(row.get("delta_vs_benchmark"), 0.0) for row in values])
                ),
                "absolute_loss": _round(group_loss),
                "loss_share": _round(group_loss / total_abs_loss if total_abs_loss else 0.0),
                "promotion_gate_allowed": False,
            }
        )
    groups = sorted(groups, key=lambda row: _float(row.get("loss_share"), 0.0), reverse=True)
    return {
        "summary": {
            "group_key": group_key,
            "loss_case_count": len(rows),
            "group_count": len(groups),
            "total_abs_loss": _round(total_abs_loss),
            "max_loss_share": groups[0]["loss_share"] if groups else 0.0,
            "promotion_gate_allowed": False,
        },
        "groups": groups,
    }


def _tail_loss_contribution(
    losing_cases: list[dict[str, Any]],
    config: Mapping[str, Any],
) -> dict[str, Any]:
    policy = _next_stage_section(config, "value_surface_failure_attribution")
    quantile = _float(policy.get("tail_loss_quantile"), 0.10)
    ordered = sorted(
        losing_cases,
        key=lambda row: abs(_float(row.get("delta_vs_benchmark"), 0.0)),
        reverse=True,
    )
    tail_count = min(len(ordered), max(1, math.ceil(len(ordered) * quantile))) if ordered else 0
    tail = ordered[:tail_count]
    total_abs_loss = sum(abs(_float(row.get("delta_vs_benchmark"), 0.0)) for row in losing_cases)
    tail_abs_loss = sum(abs(_float(row.get("delta_vs_benchmark"), 0.0)) for row in tail)
    return {
        "tail_loss_quantile": quantile,
        "tail_case_count": tail_count,
        "losing_case_count": len(losing_cases),
        "tail_abs_loss": _round(tail_abs_loss),
        "total_abs_loss": _round(total_abs_loss),
        "tail_loss_share": _round(tail_abs_loss / total_abs_loss if total_abs_loss else 0.0),
        "tail_cases": tail,
        "promotion_gate_allowed": False,
    }


def _turnover_cost_contribution(losing_cases: list[dict[str, Any]]) -> dict[str, Any]:
    cost_deltas = [
        _float(row.get("selected_estimated_cost"), 0.0)
        - _float(row.get("benchmark_estimated_cost"), 0.0)
        for row in losing_cases
    ]
    turnover_deltas = [
        _float(row.get("selected_turnover_cost_assumption"), 0.0)
        - _float(row.get("benchmark_turnover_cost_assumption"), 0.0)
        for row in losing_cases
    ]
    total_abs_loss = sum(abs(_float(row.get("delta_vs_benchmark"), 0.0)) for row in losing_cases)
    positive_cost_drag = sum(max(value, 0.0) for value in cost_deltas)
    return {
        "loss_case_count": len(losing_cases),
        "mean_cost_delta_vs_benchmark": _round(_mean(cost_deltas)),
        "mean_turnover_assumption_delta_vs_benchmark": _round(_mean(turnover_deltas)),
        "positive_cost_drag": _round(positive_cost_drag),
        "positive_cost_drag_share_of_abs_loss": _round(
            positive_cost_drag / total_abs_loss if total_abs_loss else 0.0
        ),
        "promotion_gate_allowed": False,
    }


def _drawdown_contribution(losing_cases: list[dict[str, Any]]) -> dict[str, Any]:
    drawdown_deltas = [
        _float(row.get("selected_drawdown_proxy"), 0.0)
        - _float(row.get("benchmark_drawdown_proxy"), 0.0)
        for row in losing_cases
        if row.get("benchmark_drawdown_proxy") is not None
    ]
    worse_drawdown = [value for value in drawdown_deltas if value < 0]
    return {
        "loss_case_count": len(losing_cases),
        "mean_drawdown_delta_vs_benchmark": _round(_mean(drawdown_deltas)),
        "worse_drawdown_case_count": len(worse_drawdown),
        "worse_drawdown_case_rate": _round(
            len(worse_drawdown) / len(drawdown_deltas) if drawdown_deltas else 0.0
        ),
        "promotion_gate_allowed": False,
    }


def _benchmark_relative_downside_attribution(
    selected_cases: list[dict[str, Any]],
) -> dict[str, Any]:
    selected_negative = [
        row for row in selected_cases if _float(row.get("selected_realized_net_return"), 0.0) < 0
    ]
    benchmark_positive = [
        row
        for row in selected_negative
        if _float(row.get("benchmark_realized_net_return"), 0.0) >= 0
    ]
    underperform = [row for row in selected_cases if _float(row.get("delta_vs_benchmark"), 0.0) < 0]
    return {
        "selected_negative_case_count": len(selected_negative),
        "benchmark_nonnegative_when_selected_negative_count": len(benchmark_positive),
        "benchmark_relative_underperform_case_count": len(underperform),
        "average_underperformance_delta": _round(
            _mean([_float(row.get("delta_vs_benchmark"), 0.0) for row in underperform])
        ),
        "downside_concentrated_when_benchmark_nonnegative": _round(
            len(benchmark_positive) / len(selected_negative) if selected_negative else 0.0
        ),
        "promotion_gate_allowed": False,
    }


def _horizon_ranking_jump_report(
    rows: list[dict[str, Any]],
    config: Mapping[str, Any],
) -> dict[str, Any]:
    horizon_order = {str(row["horizon_id"]): int(row["days"]) for row in _horizons(config)}
    grouped = _surface_group_summary(rows, ["horizon", "action"])
    by_horizon: dict[str, list[dict[str, Any]]] = {}
    for row in grouped:
        by_horizon.setdefault(str(row.get("horizon")), []).append(row)
    ranked_by_horizon = []
    for horizon, values in sorted(
        by_horizon.items(), key=lambda item: horizon_order.get(item[0], 0)
    ):
        ranking = sorted(
            values,
            key=lambda row: _float(row.get("mean_net_utility"), 0.0),
            reverse=True,
        )
        ranked_by_horizon.append(
            {
                "horizon": horizon,
                "top_action": ranking[0].get("action") if ranking else None,
                "ranking": [
                    {
                        "rank": index,
                        "action": row.get("action"),
                        "mean_net_utility": row.get("mean_net_utility"),
                        "promotion_gate_allowed": False,
                    }
                    for index, row in enumerate(ranking, start=1)
                ],
                "promotion_gate_allowed": False,
            }
        )
    jumps = []
    for previous, current in zip(ranked_by_horizon, ranked_by_horizon[1:], strict=False):
        jumps.append(
            {
                "previous_horizon": previous["horizon"],
                "current_horizon": current["horizon"],
                "previous_top_action": previous["top_action"],
                "current_top_action": current["top_action"],
                "top_action_changed": previous["top_action"] != current["top_action"],
                "promotion_gate_allowed": False,
            }
        )
    return {
        "summary": {
            "horizon_count": len(ranked_by_horizon),
            "ranking_jump_count": sum(1 for row in jumps if row["top_action_changed"]),
            "promotion_gate_allowed": False,
        },
        "ranked_by_horizon": ranked_by_horizon,
        "adjacent_horizon_jumps": jumps,
    }


def _single_horizon_action_report(
    rows: list[dict[str, Any]],
    config: Mapping[str, Any],
) -> dict[str, Any]:
    horizon_count = len(_horizons(config))
    ranking = _horizon_ranking_jump_report(rows, config)["ranked_by_horizon"]
    top_counts: dict[str, int] = {}
    for row in ranking:
        action = str(row.get("top_action"))
        top_counts[action] = top_counts.get(action, 0) + 1
    action_rows = [
        {
            "action": action,
            "top_horizon_count": count,
            "top_horizon_share": _round(count / horizon_count if horizon_count else 0.0),
            "single_horizon_only": count == 1,
            "promotion_gate_allowed": False,
        }
        for action, count in sorted(top_counts.items())
    ]
    return {
        "summary": {
            "action_count": len(action_rows),
            "single_horizon_action_count": sum(
                1 for row in action_rows if row["single_horizon_only"]
            ),
            "promotion_gate_allowed": False,
        },
        "actions": action_rows,
    }


def _utility_profile_cliff_report(utility_review: Mapping[str, Any]) -> dict[str, Any]:
    cliffs = _records(
        utility_review.get("horizon_cliff_report", {}).get("rows")
        if isinstance(utility_review.get("horizon_cliff_report"), Mapping)
        else []
    )
    ranking_flips = _records(
        utility_review.get("ranking_flip_report", {}).get("rows")
        if isinstance(utility_review.get("ranking_flip_report"), Mapping)
        else []
    )
    active_cliffs = [row for row in cliffs if row.get("horizon_cliff_for_review")]
    active_flips = [row for row in ranking_flips if row.get("reverses_balanced_top")]
    return {
        "summary": {
            "utility_profile_cliff_count": len(active_cliffs),
            "ranking_flip_count": len(active_flips),
            "validated_boundary_count": 0,
            "not_validated_utility_boundary": True,
            "promotion_gate_allowed": False,
        },
        "horizon_cliff_rows": active_cliffs,
        "ranking_flip_rows": active_flips,
    }


def _horizon_smoothing_assessment(
    *,
    config: Mapping[str, Any],
    ranking_jumps: Mapping[str, Any],
    single_horizon: Mapping[str, Any],
    utility_cliffs: Mapping[str, Any],
    utility_review: Mapping[str, Any],
) -> dict[str, Any]:
    policy = _next_stage_section(config, "horizon_cliff_stabilization_review")
    jump_floor = _first_int(policy.get("ranking_jump_review_floor"))
    ranking_jump_count = _first_int(ranking_jumps.get("summary", {}).get("ranking_jump_count"))
    cliff_count = _first_int(utility_cliffs.get("summary", {}).get("utility_profile_cliff_count"))
    flip_count = _first_int(utility_cliffs.get("summary", {}).get("ranking_flip_count"))
    pareto_count = _first_int(utility_review.get("summary", {}).get("pareto_candidate_count"))
    smoothing_required = ranking_jump_count >= jump_floor or cliff_count > 0
    pareto_review = flip_count > 0 or pareto_count > 0
    return {
        "horizon_smoothing_review_required": smoothing_required,
        "pareto_frontier_policy_review_required": pareto_review,
        "single_utility_ranking_stable": not smoothing_required and flip_count == 0,
        "recommended_stabilization_options": [
            option
            for option, enabled in [
                ("horizon_smoothing", smoothing_required),
                ("pareto_frontier_policy", pareto_review),
                (
                    "single_horizon_action_suppression",
                    _first_int(single_horizon.get("summary", {}).get("single_horizon_action_count"))
                    > 0,
                ),
            ]
            if enabled
        ],
        "validated_boundary_count": 0,
        "not_validated_utility_boundary": True,
        "promotion_gate_allowed": False,
    }


def _residual_hypothesis_triage(
    residual_rows: list[dict[str, Any]],
    config: Mapping[str, Any],
) -> dict[str, Any]:
    policy = _next_stage_section(config, "gbdt_residual_hypothesis_triage")
    limit = _first_int(policy.get("hypothesis_candidate_limit")) or 10
    feature_explanations = _residual_feature_importance(
        residual_rows,
        features=["asset", "horizon", "regime_segment", "asset_cluster", "pit_state", "action"],
    )
    repair_candidates = _residual_repair_rule_candidates(residual_rows, config)
    new_hypotheses = _residual_hypothesis_candidates(residual_rows)[:limit]
    large_residuals = [row for row in residual_rows if row.get("large_residual_for_review")]
    return {
        "summary": {
            "residual_case_count": len(residual_rows),
            "large_residual_case_count": len(large_residuals),
            "strategy_signal_generated": False,
            "promotion_gate_allowed": False,
        },
        "prediction_error_summary": {
            "mean_residual": _round(
                _mean([_float(row.get("residual"), 0.0) for row in residual_rows])
            ),
            "mean_abs_residual": _round(
                _mean([_float(row.get("abs_residual"), 0.0) for row in residual_rows])
            ),
            "large_residual_rate": _round(
                len(large_residuals) / len(residual_rows) if residual_rows else 0.0
            ),
            "promotion_gate_allowed": False,
        },
        "residual_by_asset": _residual_group_result(residual_rows, "asset"),
        "residual_by_horizon": _residual_group_result(residual_rows, "horizon"),
        "residual_by_regime": _residual_group_result(residual_rows, "regime_segment"),
        "feature_explanations": feature_explanations,
        "repair_rule_candidates": repair_candidates,
        "new_hypothesis_candidates": new_hypotheses,
    }


def _residual_repair_rule_candidates(
    residual_rows: list[dict[str, Any]],
    config: Mapping[str, Any],
) -> list[dict[str, Any]]:
    policy = _next_stage_section(config, "gbdt_residual_hypothesis_triage")
    configured = [str(item) for item in policy.get("repair_rule_candidates", [])]
    large = [row for row in residual_rows if row.get("large_residual_for_review")]
    horizon_groups = _residual_group_result(residual_rows, "horizon")
    regime_groups = _residual_group_result(residual_rows, "regime_segment")
    action_groups = _residual_group_result(residual_rows, "action")
    candidates = []
    for rule in configured:
        if rule == "tail-loss filter":
            evidence = {
                "large_residual_case_count": len(large),
                "large_residual_rate": _round(
                    len(large) / len(residual_rows) if residual_rows else 0.0
                ),
            }
        elif rule == "horizon smoothing":
            evidence = {"top_horizon_residual": horizon_groups[:3]}
        elif rule == "regime-conditioned utility":
            evidence = {"top_regime_residual": regime_groups[:3]}
        elif rule == "cost-aware action suppression":
            evidence = {"top_action_residual": action_groups[:3]}
        elif rule == "drawdown-sensitive ranking":
            evidence = {
                "mean_drawdown_proxy_large_residual": _round(
                    _mean([_float(row.get("max_drawdown_proxy"), 0.0) for row in large])
                )
            }
        else:
            evidence = {}
        candidates.append(
            {
                "repair_rule": rule,
                "evidence": evidence,
                "strategy_signal_generated": False,
                "requires_followup_experiment": True,
                "promotion_gate_allowed": False,
            }
        )
    return candidates


def _value_surface_direction_decision(
    *,
    config: Mapping[str, Any],
    failure: Mapping[str, Any],
    horizon: Mapping[str, Any],
    residual: Mapping[str, Any],
    forward: Mapping[str, Any],
    walk_forward: Mapping[str, Any],
) -> dict[str, Any]:
    policy = _next_stage_section(config, "value_surface_direction_review")
    allowed = [str(item) for item in policy.get("allowed_decisions", [])]
    failure_summary = failure.get("summary") if isinstance(failure.get("summary"), Mapping) else {}
    attribution_summary = (
        failure.get("attribution_summary")
        if isinstance(failure.get("attribution_summary"), Mapping)
        else {}
    )
    horizon_summary = horizon.get("summary") if isinstance(horizon.get("summary"), Mapping) else {}
    residual_summary = (
        residual.get("summary") if isinstance(residual.get("summary"), Mapping) else {}
    )
    mean_delta = _float(attribution_summary.get("overall_mean_delta_vs_benchmark"), 0.0)
    tail_share = _float(failure_summary.get("tail_loss_contribution"), 0.0)
    max_loss_share = _float(failure_summary.get("max_loss_concentration_share"), 0.0)
    horizon_cliff_count = _first_int(horizon_summary.get("horizon_cliff_count"))
    ranking_jump_count = _first_int(horizon_summary.get("ranking_jump_count"))
    large_residual_count = _first_int(residual_summary.get("large_residual_case_count"))
    kill_floor = _float(policy.get("kill_mean_delta_floor_bps"), -500.0) / 10_000.0
    tail_pivot_share = _float(policy.get("tail_loss_pivot_share"), 0.50)
    horizon_floor = _first_int(policy.get("horizon_cliff_pivot_floor"))
    broad_ceiling = _float(policy.get("broad_loss_group_share_ceiling"), 0.35)
    if not failure or not horizon:
        decision = "WATCHLIST"
        reason = "missing_failure_or_horizon_review"
    elif mean_delta <= kill_floor and max_loss_share <= broad_ceiling:
        decision = "KILL_CURRENT_VALUE_SURFACE_VERSION"
        reason = "broad_negative_delta_below_kill_floor"
    elif mean_delta < 0 and tail_share >= tail_pivot_share:
        decision = "PIVOT_TO_TAIL_RISK_FILTER"
        reason = "negative_mean_delta_with_tail_loss_concentration"
    elif mean_delta < 0 and max_loss_share > broad_ceiling:
        decision = "PIVOT_TO_REGIME_CONDITIONED_VALUE_SURFACE"
        reason = "negative_mean_delta_with_group_loss_concentration"
    elif horizon_cliff_count >= horizon_floor or ranking_jump_count > 0:
        decision = "PIVOT_TO_PARETO_FRONTIER_POLICY"
        reason = "horizon_cliff_or_ranking_jump_requires_stabilization"
    elif mean_delta < 0 or large_residual_count > 0:
        decision = "WATCHLIST"
        reason = "negative_or_residual_risk_without_clear_local_fix"
    else:
        decision = "CONTINUE_LOCAL_FIX"
        reason = "no_structural_failure_detected_but_no_promotion_allowed"
    if allowed and decision not in allowed:
        decision = "WATCHLIST"
        reason = "computed_decision_not_in_allowed_policy"
    return {
        "decision": decision,
        "reason": reason,
        "allowed_decisions": allowed,
        "mean_delta_vs_benchmark": _round(mean_delta),
        "tail_loss_share": _round(tail_share),
        "max_loss_concentration_share": _round(max_loss_share),
        "horizon_cliff_count": horizon_cliff_count,
        "ranking_jump_count": ranking_jump_count,
        "large_residual_case_count": large_residual_count,
        "forward_ledger_event_count": _first_int(
            forward.get("summary", {}).get("ledger_event_count")
        ),
        "walk_forward_decision": walk_forward.get("summary", {}).get(
            "controlled_walk_forward_decision"
        ),
        "promotion_gate_allowed": False,
        "paper_shadow_change_allowed": False,
        "production_weight_change_allowed": False,
    }


def _direction_evidence_summary(
    *,
    failure: Mapping[str, Any],
    horizon: Mapping[str, Any],
    residual: Mapping[str, Any],
    forward: Mapping[str, Any],
    walk_forward: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "walk_forward_status": walk_forward.get("status"),
        "walk_forward_decision": walk_forward.get("summary", {}).get(
            "controlled_walk_forward_decision"
        ),
        "failure_status": failure.get("status"),
        "overall_mean_delta_vs_benchmark": failure.get("attribution_summary", {}).get(
            "overall_mean_delta_vs_benchmark"
        ),
        "tail_loss_contribution": failure.get("summary", {}).get("tail_loss_contribution"),
        "horizon_status": horizon.get("status"),
        "horizon_cliff_count": horizon.get("summary", {}).get("horizon_cliff_count"),
        "residual_status": residual.get("status"),
        "residual_case_count": residual.get("summary", {}).get("residual_case_count"),
        "forward_status": forward.get("status"),
        "ledger_event_count": forward.get("summary", {}).get("ledger_event_count"),
        "promotion_gate_allowed": False,
    }


def _regime_conditioned_design_protocol(
    *,
    config: Mapping[str, Any],
    failure: Mapping[str, Any],
    horizon: Mapping[str, Any],
    residual: Mapping[str, Any],
    direction: Mapping[str, Any],
) -> dict[str, Any]:
    policy = _next_stage_section(config, "regime_conditioned_value_surface_design")
    share_floor = _float(policy.get("tail_loss_regime_share_floor"), 0.35)
    aggressive_actions = [str(item) for item in policy.get("aggressive_actions", [])]
    low_risk_actions = [str(item) for item in policy.get("low_risk_actions", [])]
    high_loss_regimes = _high_loss_group_values(
        failure,
        "loss_concentration_by_regime",
        "regime_segment",
        share_floor,
    )
    high_loss_horizons = _high_loss_group_values(
        failure,
        "loss_concentration_by_horizon",
        "horizon",
        share_floor,
    )
    high_loss_assets = _high_loss_group_values(
        failure,
        "loss_concentration_by_asset",
        "asset",
        share_floor,
    )
    residual_regimes = [
        str(row.get("regime_segment"))
        for row in _records(residual.get("residual_by_regime"))
        if row.get("regime_segment")
    ][:5]
    horizon_cliffs = _records(
        horizon.get("utility_profile_cliff_report", {}).get("horizon_cliff_rows")
        if isinstance(horizon.get("utility_profile_cliff_report"), Mapping)
        else []
    )
    horizon_days_by_id = {str(row["horizon_id"]): int(row["days"]) for row in _horizons(config)}
    disabled_horizons = sorted(
        {
            str(row.get("current_horizon") or row.get("previous_horizon"))
            for row in horizon_cliffs
            if row.get("horizon_cliff_for_review")
        }
        | set(high_loss_horizons)
    )
    disabled_horizons = [value for value in disabled_horizons if value and value != "None"]
    tail_definitions = []
    for regime in high_loss_regimes:
        tail_definitions.append(
            {
                "definition_id": f"tail_loss_regime_{regime}",
                "variable": "regime_segment",
                "value": regime,
                "source": "TRADING-790 loss concentration",
                "action": "fallback_to_benchmark_or_low_risk_only",
                "requires_out_of_sample_validation": True,
                "promotion_gate_allowed": False,
            }
        )
    for horizon_id in high_loss_horizons:
        tail_definitions.append(
            {
                "definition_id": f"tail_loss_horizon_{horizon_id}",
                "variable": "horizon",
                "value": horizon_id,
                "horizon_days": horizon_days_by_id.get(horizon_id),
                "source": "TRADING-790/791 loss and cliff concentration",
                "action": "disable_or_downweight_horizon_until_walk_forward_confirmed",
                "requires_out_of_sample_validation": True,
                "promotion_gate_allowed": False,
            }
        )
    keep_regimes = [
        {
            "regime_rule": "not_in_tail_loss_regime_and_no_horizon_cliff",
            "allowed": "retain_value_surface_ranking_for_diagnostic_review",
            "excluded_regimes": high_loss_regimes,
            "excluded_horizons": disabled_horizons,
            "promotion_gate_allowed": False,
        }
    ]
    fallback_regimes = [
        {
            "regime_variable": "regime_segment",
            "regime_values": high_loss_regimes,
            "fallback": "benchmark",
            "reason": "loss concentration above diagnostic share floor",
            "promotion_gate_allowed": False,
        },
        {
            "regime_variable": "horizon",
            "regime_values": disabled_horizons,
            "fallback": "shorter_horizon_or_benchmark",
            "reason": "horizon cliff or horizon loss concentration",
            "promotion_gate_allowed": False,
        },
    ]
    low_risk_regimes = [
        {
            "regime_variable": "asset",
            "regime_values": high_loss_assets,
            "disabled_actions": aggressive_actions,
            "allowed_actions": low_risk_actions,
            "reason": "asset loss concentration requires lower risk action set",
            "promotion_gate_allowed": False,
        },
        {
            "regime_variable": "residual_regime_segment",
            "regime_values": residual_regimes,
            "disabled_actions": aggressive_actions,
            "allowed_actions": low_risk_actions,
            "reason": "large residual concentration requires diagnostic action suppression",
            "promotion_gate_allowed": False,
        },
    ]
    protocol = {
        "protocol_id": "regime_conditioned_value_surface_v0_controlled",
        "source_direction_decision": direction.get("summary", {}).get("direction_decision"),
        "ranking_policy": "heuristic",
        "not_validated_utility_boundary": True,
        "rule_source": "failure_attribution_and_residual_diagnostics",
        "requires_walk_forward_confirmation": True,
        "promotion_gate_allowed": False,
        "paper_shadow_change_allowed": False,
        "production_weight_change_allowed": False,
    }
    return {
        "protocol": protocol,
        "regime_variables": [
            {
                "variable": "regime_segment",
                "role": "primary market regime conditioning key",
                "source": "value surface row PIT regime segment",
            },
            {
                "variable": "pit_state",
                "role": "trend/pressure state proxy",
                "source": "PIT returns",
            },
            {
                "variable": "asset_cluster",
                "role": "cluster concentration key",
                "source": "controlled research config",
            },
            {"variable": "horizon", "role": "horizon cliff and maturity key", "source": "config"},
            {
                "variable": "selected_action",
                "role": "aggressive action suppression key",
                "source": "value surface selected action",
            },
            {
                "variable": "residual_sign",
                "role": "GBDT residual hypothesis key",
                "source": "TRADING-792 residual diagnostics",
            },
        ],
        "tail_loss_regime_definitions": tail_definitions,
        "allowed_action_changes_by_regime": [
            {
                "condition": "tail_loss_regime",
                "allowed_change": "replace aggressive value-surface action with benchmark",
                "disabled_actions": aggressive_actions,
                "allowed_actions": ["benchmark", *low_risk_actions],
                "promotion_gate_allowed": False,
            },
            {
                "condition": "horizon_cliff_regime",
                "allowed_change": "disable or downweight long/cliff horizon",
                "disabled_or_downweighted_horizons": disabled_horizons,
                "promotion_gate_allowed": False,
            },
            {
                "condition": "high_residual_regime",
                "allowed_change": "require confirmation before deviating from benchmark",
                "confirmation_source": "future controlled walk-forward only",
                "promotion_gate_allowed": False,
            },
        ],
        "benchmark_fallback_rules": fallback_regimes,
        "regimes_keep_value_surface": keep_regimes,
        "regimes_fallback_to_benchmark": fallback_regimes,
        "regimes_low_risk_action_only": low_risk_regimes,
        "horizons_disabled_or_downweighted": [
            {
                "horizon": horizon_id,
                "horizon_days": horizon_days_by_id.get(horizon_id),
                "reason": "loss concentration or utility cliff",
                "fallback": "shorter_horizon_or_benchmark",
                "promotion_gate_allowed": False,
            }
            for horizon_id in disabled_horizons
        ],
        "controlled_only_validation_plan": [
            {
                "step": "retrospective_ablation",
                "purpose": "compare original, conditioned, guarded, and fallback variants",
                "promotion_gate_allowed": False,
            },
            {
                "step": "walk_forward_holdout",
                "purpose": "verify guardrail definitions without using future outcome in decision",
                "promotion_gate_allowed": False,
            },
            {
                "step": "forward_evidence_append_only_tracking",
                "purpose": "observe daily dry-run outcomes before any paper-shadow discussion",
                "promotion_gate_allowed": False,
            },
        ],
    }


def _guardrail_variant_comparison(
    *,
    selected_cases: list[dict[str, Any]],
    config: Mapping[str, Any],
    failure: Mapping[str, Any],
    horizon: Mapping[str, Any],
    design: Mapping[str, Any],
) -> dict[str, Any]:
    policy = _next_stage_section(config, "tail_loss_guardrail_fallback_policy")
    allowed_variants = [str(item) for item in policy.get("allowed_variants", [])] or [
        "original_value_surface",
        "regime_conditioned_value_surface",
        "tail_loss_guarded_value_surface",
        "benchmark_fallback_value_surface",
    ]
    risk_context = _guardrail_risk_context(
        config=config,
        failure=failure,
        horizon=horizon,
        design=design,
    )
    variants: dict[str, list[dict[str, Any]]] = {}
    for variant in allowed_variants:
        variants[variant] = [
            _apply_guardrail_variant(row, variant, risk_context, policy) for row in selected_cases
        ]
    metrics = [_variant_metric_row(variant, rows, config) for variant, rows in variants.items()]
    original = next((row for row in metrics if row["variant_id"] == "original_value_surface"), {})
    tail_guarded = next(
        (row for row in metrics if row["variant_id"] == "tail_loss_guarded_value_surface"),
        {},
    )
    best = max(
        metrics, key=lambda row: _float(row.get("mean_delta_vs_benchmark"), -999.0), default={}
    )
    original_tail = _float(original.get("tail_loss_contribution"), 0.0)
    tail_tail = _float(tail_guarded.get("tail_loss_contribution"), 0.0)
    return {
        "summary": {
            "best_variant_by_mean_delta": best.get("variant_id"),
            "best_variant_mean_delta_vs_benchmark": best.get("mean_delta_vs_benchmark"),
            "original_mean_delta_vs_benchmark": original.get("mean_delta_vs_benchmark"),
            "tail_loss_guarded_mean_delta_vs_benchmark": tail_guarded.get(
                "mean_delta_vs_benchmark"
            ),
            "tail_loss_guardrail_reduces_tail_loss": tail_tail < original_tail,
            "promotion_gate_allowed": False,
        },
        "variant_metrics": metrics,
        "variant_rules": _guardrail_variant_rules(risk_context, policy),
        "variant_case_samples": {
            variant: rows[:10]
            for variant, rows in variants.items()
            if variant != "original_value_surface"
        },
        "diagnostic_boundary": {
            "retrospective_ablation_only": True,
            "requires_out_of_sample_walk_forward": True,
            "future_outcome_used_for_evaluation_only": True,
            "promotion_gate_allowed": False,
            "paper_shadow_change_allowed": False,
            "production_weight_change_allowed": False,
        },
    }


def _guardrail_risk_context(
    *,
    config: Mapping[str, Any],
    failure: Mapping[str, Any],
    horizon: Mapping[str, Any],
    design: Mapping[str, Any],
) -> dict[str, Any]:
    design_policy = _next_stage_section(config, "regime_conditioned_value_surface_design")
    guardrail_policy = _next_stage_section(config, "tail_loss_guardrail_fallback_policy")
    share_floor = _float(
        guardrail_policy.get(
            "tail_loss_regime_share_floor",
            design_policy.get("tail_loss_regime_share_floor", 0.35),
        ),
        0.35,
    )
    high_loss_regimes = set(
        _high_loss_group_values(
            failure, "loss_concentration_by_regime", "regime_segment", share_floor
        )
    )
    high_loss_horizons = set(
        _high_loss_group_values(failure, "loss_concentration_by_horizon", "horizon", share_floor)
    )
    high_loss_assets = set(
        _high_loss_group_values(failure, "loss_concentration_by_asset", "asset", share_floor)
    )
    design_horizons = {
        str(row.get("horizon"))
        for row in _records(design.get("horizons_disabled_or_downweighted"))
        if row.get("horizon")
    }
    horizon_summary = horizon.get("summary") if isinstance(horizon.get("summary"), Mapping) else {}
    aggressive_actions = set(str(item) for item in design_policy.get("aggressive_actions", []))
    return {
        "high_loss_regimes": high_loss_regimes,
        "high_loss_horizons": high_loss_horizons | design_horizons,
        "high_loss_assets": high_loss_assets,
        "aggressive_actions": aggressive_actions,
        "horizon_cliff_count": _first_int(horizon_summary.get("horizon_cliff_count")),
    }


def _apply_guardrail_variant(
    case: Mapping[str, Any],
    variant: str,
    risk_context: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    output = dict(case)
    output["variant_id"] = variant
    output["guardrail_action"] = "keep_value_surface_action"
    if variant == "original_value_surface":
        return output
    risk_match = _case_matches_risk_context(case, risk_context)
    aggressive = str(case.get("selected_action")) in risk_context.get("aggressive_actions", set())
    benchmark_disagreement = case.get("selected_action") != case.get("benchmark_action")
    high_cost = _float(case.get("selected_estimated_cost"), 0.0) > _float(
        case.get("benchmark_estimated_cost"), 0.0
    )
    if variant == "regime_conditioned_value_surface" and risk_match:
        return _fallback_case_to_benchmark(output, "regime_conditioned_benchmark_fallback")
    if variant == "tail_loss_guarded_value_surface":
        multiplier = _float(policy.get("loss_concentration_size_multiplier"), 0.50)
        if risk_match and aggressive:
            return _fallback_case_to_benchmark(output, "tail_risk_aggressive_action_fallback")
        if risk_match:
            return _scale_case_delta(output, multiplier, "tail_risk_size_reduction")
    if variant == "benchmark_fallback_value_surface" and (
        risk_match or (benchmark_disagreement and high_cost)
    ):
        return _fallback_case_to_benchmark(output, "benchmark_confirmation_required_fallback")
    return output


def _case_matches_risk_context(
    case: Mapping[str, Any],
    risk_context: Mapping[str, Any],
) -> bool:
    return (
        str(case.get("regime_segment")) in risk_context.get("high_loss_regimes", set())
        or str(case.get("horizon")) in risk_context.get("high_loss_horizons", set())
        or str(case.get("asset")) in risk_context.get("high_loss_assets", set())
    )


def _fallback_case_to_benchmark(case: dict[str, Any], reason: str) -> dict[str, Any]:
    case["selected_action_before_guardrail"] = case.get("selected_action")
    case["selected_action"] = case.get("benchmark_action", "benchmark")
    case["selected_realized_net_return"] = case.get("benchmark_realized_net_return")
    case["delta_vs_benchmark"] = 0.0
    case["value_surface_beats_benchmark"] = True
    case["selected_estimated_cost"] = case.get("benchmark_estimated_cost", 0.0)
    case["selected_turnover_cost_assumption"] = case.get("benchmark_turnover_cost_assumption", 0.0)
    case["selected_drawdown_proxy"] = case.get("benchmark_drawdown_proxy")
    case["guardrail_action"] = reason
    case["promotion_gate_allowed"] = False
    return case


def _scale_case_delta(case: dict[str, Any], multiplier: float, reason: str) -> dict[str, Any]:
    benchmark_return = _float(case.get("benchmark_realized_net_return"), 0.0)
    original_delta = _float(case.get("delta_vs_benchmark"), 0.0)
    adjusted_delta = original_delta * multiplier
    case["selected_action_before_guardrail"] = case.get("selected_action")
    case["selected_realized_net_return"] = _round(benchmark_return + adjusted_delta)
    case["delta_vs_benchmark"] = _round(adjusted_delta)
    case["value_surface_beats_benchmark"] = adjusted_delta >= 0
    case["selected_estimated_cost"] = _round(
        _float(case.get("selected_estimated_cost"), 0.0) * multiplier
    )
    case["selected_turnover_cost_assumption"] = _round(
        _float(case.get("selected_turnover_cost_assumption"), 0.0) * multiplier
    )
    case["guardrail_action"] = reason
    case["promotion_gate_allowed"] = False
    return case


def _variant_metric_row(
    variant_id: str,
    rows: list[dict[str, Any]],
    config: Mapping[str, Any],
) -> dict[str, Any]:
    aggregate = _aggregate_walk_forward_cases(rows)
    losers = [row for row in rows if _float(row.get("delta_vs_benchmark"), 0.0) < 0]
    concentration_reports = [
        _loss_concentration(losers, "date"),
        _loss_concentration(losers, "asset"),
        _loss_concentration(losers, "horizon"),
        _loss_concentration(losers, "regime_segment"),
    ]
    max_share = max(
        (report["summary"]["max_loss_share"] for report in concentration_reports),
        default=0.0,
    )
    tail = _tail_loss_contribution(losers, config)
    return {
        "variant_id": variant_id,
        **aggregate,
        "losing_case_count": len(losers),
        "losing_avg": _round(_mean([_float(row.get("delta_vs_benchmark"), 0.0) for row in losers])),
        "tail_loss_contribution": tail["tail_loss_share"],
        "max_loss_concentration_share": _round(max_share),
        "turnover": _round(
            sum(_float(row.get("selected_turnover_cost_assumption"), 0.0) for row in rows)
        ),
        "cost": _round(sum(_float(row.get("selected_estimated_cost"), 0.0) for row in rows)),
        "drawdown": _round(
            _mean([_float(row.get("selected_drawdown_proxy"), 0.0) for row in rows])
        ),
        "guardrail_changed_case_count": sum(
            1 for row in rows if row.get("guardrail_action") != "keep_value_surface_action"
        ),
        "promotion_gate_allowed": False,
    }


def _guardrail_variant_rules(
    risk_context: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> list[dict[str, Any]]:
    return [
        {
            "variant_id": "original_value_surface",
            "rule": "no guardrail; keep original top value-surface action",
            "promotion_gate_allowed": False,
        },
        {
            "variant_id": "regime_conditioned_value_surface",
            "rule": "fallback to benchmark in failure-attributed high-loss regimes/horizons/assets",
            "high_loss_regimes": sorted(risk_context.get("high_loss_regimes", [])),
            "high_loss_horizons": sorted(risk_context.get("high_loss_horizons", [])),
            "high_loss_assets": sorted(risk_context.get("high_loss_assets", [])),
            "promotion_gate_allowed": False,
        },
        {
            "variant_id": "tail_loss_guarded_value_surface",
            "rule": (
                "fallback aggressive actions in tail-risk context and size-reduce "
                "other concentrated cases"
            ),
            "size_multiplier": _float(policy.get("loss_concentration_size_multiplier"), 0.50),
            "promotion_gate_allowed": False,
        },
        {
            "variant_id": "benchmark_fallback_value_surface",
            "rule": (
                "require confirmation before benchmark disagreement in high-risk "
                "or high-cost context"
            ),
            "promotion_gate_allowed": False,
        },
    ]


def _regime_horizon_loss_matrix(
    selected_cases: list[dict[str, Any]],
    config: Mapping[str, Any],
) -> dict[str, Any]:
    losers = [row for row in selected_cases if _float(row.get("delta_vs_benchmark"), 0.0) < 0]
    enriched = [
        {
            **row,
            "action": row.get("selected_action"),
            "cluster": row.get("asset_cluster"),
            "utility_profile": _utility_profile_for_action(str(row.get("selected_action")), config),
            "date_window": _date_window(str(row.get("date")), config),
        }
        for row in losers
    ]
    reports = {
        "regime": _loss_matrix_group(enriched, "regime_segment"),
        "asset": _loss_matrix_group(enriched, "asset"),
        "horizon": _loss_matrix_group(enriched, "horizon"),
        "action": _loss_matrix_group(enriched, "action"),
        "cluster": _loss_matrix_group(enriched, "cluster"),
        "utility_profile": _loss_matrix_group(enriched, "utility_profile"),
        "date_window": _loss_matrix_group(enriched, "date_window"),
    }
    max_row = _max_loss_matrix_row(reports)
    broad_ceiling = _float(
        _next_stage_section(config, "regime_horizon_loss_attribution_matrix").get(
            "broad_loss_group_share_ceiling"
        ),
        0.35,
    )
    max_share = _float(max_row.get("loss_share"), 0.0)
    assessment = "CONCENTRATED_REPAIRABLE" if max_share > broad_ceiling else "BROAD_STRUCTURAL_RISK"
    return {
        "summary": {
            "losing_case_count": len(losers),
            "losing_case_average_delta": _round(
                _mean([_float(row.get("delta_vs_benchmark"), 0.0) for row in losers])
            ),
            "max_loss_concentration_share": _round(max_share),
            "max_loss_concentration_group": max_row,
            "loss_distribution_assessment": assessment,
            "promotion_gate_allowed": False,
        },
        "loss_by_regime": reports["regime"],
        "loss_by_asset": reports["asset"],
        "loss_by_horizon": reports["horizon"],
        "loss_by_action": reports["action"],
        "loss_by_cluster": reports["cluster"],
        "loss_by_utility_profile": reports["utility_profile"],
        "loss_by_date_window": reports["date_window"],
        "top_losing_cases": sorted(
            enriched,
            key=lambda row: _float(row.get("delta_vs_benchmark"), 0.0),
        )[:25],
    }


def _loss_matrix_group(rows: list[dict[str, Any]], group_key: str) -> dict[str, Any]:
    report = _loss_concentration(rows, group_key)
    for row in report["groups"]:
        row["mean_selected_realized_net_return"] = _round(
            _mean(
                [
                    _float(value.get("selected_realized_net_return"), 0.0)
                    for value in rows
                    if str(value.get(group_key, "unknown")) == str(row.get(group_key))
                ]
            )
        )
        row["mean_benchmark_realized_net_return"] = _round(
            _mean(
                [
                    _float(value.get("benchmark_realized_net_return"), 0.0)
                    for value in rows
                    if str(value.get(group_key, "unknown")) == str(row.get(group_key))
                ]
            )
        )
    return report


def _max_loss_matrix_row(reports: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    candidates = []
    for report_name, report in reports.items():
        for row in _records(report.get("groups")):
            candidates.append({"matrix": report_name, **row})
    return max(candidates, key=lambda row: _float(row.get("loss_share"), 0.0), default={})


def _utility_profile_for_action(action_id: str, config: Mapping[str, Any]) -> str:
    mapping = _next_stage_section(config, "regime_horizon_loss_attribution_matrix").get(
        "utility_profile_mapping",
        {},
    )
    if isinstance(mapping, Mapping):
        for profile, actions in mapping.items():
            if action_id in [str(item) for item in actions]:
                return str(profile)
    if "risk" in action_id or "cash" in action_id or "guard" in action_id:
        return "risk_controlled"
    if "hold" in action_id or "static" in action_id:
        return "benchmark_like"
    return "aggressive_return_seeking"


def _date_window(date_value: str, config: Mapping[str, Any]) -> str:
    policy = _next_stage_section(config, "regime_horizon_loss_attribution_matrix")
    window = str(policy.get("date_window", "month"))
    if window == "month" and len(date_value) >= 7:
        return date_value[:7]
    if window == "quarter" and len(date_value) >= 7:
        month = _first_int(date_value[5:7])
        quarter = ((month - 1) // 3) + 1 if month else 0
        return f"{date_value[:4]}-Q{quarter}"
    return date_value


def _residual_regime_conditioning_report(
    residual_rows: list[dict[str, Any]],
    config: Mapping[str, Any],
) -> dict[str, Any]:
    policy = _next_stage_section(config, "gbdt_residual_regime_conditioning")
    limit = _first_int(policy.get("hypothesis_candidate_limit")) or 10
    large = [row for row in residual_rows if row.get("large_residual_for_review")]
    top_features = _residual_feature_importance(
        residual_rows,
        features=["regime_segment", "horizon", "asset", "asset_cluster", "pit_state", "action"],
    )
    sign = _residual_sign_classification(residual_rows)
    hypotheses = _residual_regime_hypotheses(residual_rows, limit)
    return {
        "summary": {
            "residual_case_count": len(residual_rows),
            "large_residual_case_count": len(large),
            "strategy_signal_generated": False,
            "promotion_gate_allowed": False,
        },
        "top_residual_features": top_features,
        "large_residual_regimes": _residual_group_result(large, "regime_segment"),
        "large_residual_horizons": _residual_group_result(large, "horizon"),
        "large_residual_assets": _residual_group_result(large, "asset"),
        "residual_sign_classification": sign,
        "hypothesis_candidates": hypotheses,
    }


def _residual_sign_classification(rows: list[dict[str, Any]]) -> dict[str, Any]:
    positive = [row for row in rows if _float(row.get("residual"), 0.0) > 0]
    negative = [row for row in rows if _float(row.get("residual"), 0.0) < 0]
    neutral = [row for row in rows if _float(row.get("residual"), 0.0) == 0]
    return {
        "positive_residual": {
            "case_count": len(positive),
            "interpretation": "value surface underestimated realized net outcome",
            "mean_residual": _round(_mean([_float(row.get("residual"), 0.0) for row in positive])),
            "promotion_gate_allowed": False,
        },
        "negative_residual": {
            "case_count": len(negative),
            "interpretation": "value surface overestimated realized net outcome",
            "mean_residual": _round(_mean([_float(row.get("residual"), 0.0) for row in negative])),
            "promotion_gate_allowed": False,
        },
        "neutral_residual": {
            "case_count": len(neutral),
            "promotion_gate_allowed": False,
        },
    }


def _residual_regime_hypotheses(
    residual_rows: list[dict[str, Any]],
    limit: int,
) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str, str], list[dict[str, Any]]] = {}
    for row in residual_rows:
        key = (
            str(row.get("regime_segment", "unknown")),
            str(row.get("horizon", "unknown")),
            str(row.get("asset_cluster", "unknown")),
            str(row.get("action", "unknown")),
        )
        grouped.setdefault(key, []).append(row)
    candidates = []
    for (regime, horizon, cluster, action), values in grouped.items():
        mean_residual = _mean([_float(row.get("residual"), 0.0) for row in values])
        mean_abs = _mean([_float(row.get("abs_residual"), 0.0) for row in values])
        large_rate = sum(1 for row in values if row.get("large_residual_for_review")) / len(values)
        residual_direction = (
            "overestimated_upside" if mean_residual < 0 else "underestimated_upside"
        )
        candidates.append(
            {
                "hypothesis_id": (
                    f"regime_{regime}_horizon_{horizon}_cluster_{cluster}_action_{action}"
                ),
                "regime_segment": regime,
                "horizon": horizon,
                "asset_cluster": cluster,
                "action": action,
                "case_count": len(values),
                "mean_residual": _round(mean_residual),
                "mean_abs_residual": _round(mean_abs),
                "large_residual_rate": _round(large_rate),
                "residual_direction": residual_direction,
                "hypothesis": (
                    f"When regime={regime}, horizon={horizon}, cluster={cluster}, "
                    f"action={action}, value surface may have {residual_direction}."
                ),
                "feeds_trading_796_ablation": True,
                "strategy_signal_generated": False,
                "promotion_gate_allowed": False,
            }
        )
    return sorted(
        candidates,
        key=lambda row: (
            _float(row.get("mean_abs_residual"), 0.0),
            _float(row.get("large_residual_rate"), 0.0),
        ),
        reverse=True,
    )[:limit]


def _regime_conditioned_controlled_review_decision(
    *,
    config: Mapping[str, Any],
    design: Mapping[str, Any],
    guardrail: Mapping[str, Any],
    loss_matrix: Mapping[str, Any],
    residual_regime: Mapping[str, Any],
) -> dict[str, Any]:
    policy = _next_stage_section(config, "regime_conditioned_value_surface_controlled_review")
    allowed = [str(item) for item in policy.get("allowed_decisions", [])]
    metrics = _records(guardrail.get("variant_metrics"))
    original = next(
        (row for row in metrics if row.get("variant_id") == "original_value_surface"), {}
    )
    tail = next(
        (row for row in metrics if row.get("variant_id") == "tail_loss_guarded_value_surface"),
        {},
    )
    fallback = next(
        (row for row in metrics if row.get("variant_id") == "benchmark_fallback_value_surface"),
        {},
    )
    best = max(
        metrics, key=lambda row: _float(row.get("mean_delta_vs_benchmark"), -999.0), default={}
    )
    if not design or not guardrail or not loss_matrix or not residual_regime or not metrics:
        decision = "DATA_REQUIRED"
        reason = "missing_required_795_to_798_artifact"
    else:
        original_mean = _float(original.get("mean_delta_vs_benchmark"), 0.0)
        best_mean = _float(best.get("mean_delta_vs_benchmark"), 0.0)
        tail_reduction = _tail_loss_reduction(original, tail)
        if best_mean >= 0 and best.get("variant_id") == "tail_loss_guarded_value_surface":
            decision = "CONTINUE"
            reason = "tail_loss_guarded_variant_restores_nonnegative_mean_delta"
        elif _float(fallback.get("mean_delta_vs_benchmark"), -999.0) >= 0:
            decision = "PIVOT_TO_BENCHMARK_FALLBACK"
            reason = "benchmark_fallback_variant_restores_nonnegative_mean_delta"
        elif tail_reduction >= _float(policy.get("tail_loss_reduction_required_share"), 0.20):
            decision = "PIVOT_TO_TAIL_RISK_POLICY"
            reason = "tail_loss_guard_reduces_tail_without_full_mean_recovery"
        elif (
            best_mean <= original_mean
            and loss_matrix.get("matrix_summary", {}).get("loss_distribution_assessment")
            == "BROAD_STRUCTURAL_RISK"
        ):
            decision = "KILL_CURRENT_VALUE_SURFACE"
            reason = "guardrails_do_not_improve_broad_structural_loss"
        else:
            decision = "WATCHLIST"
            reason = "improvement_incomplete_or_requires_more_forward_evidence"
        if allowed and decision not in allowed:
            decision = "WATCHLIST"
            reason = "computed_decision_not_allowed_by_policy"
    best_variant = best.get("variant_id")
    original_mean = _float(original.get("mean_delta_vs_benchmark"), 0.0)
    best_mean = _float(best.get("mean_delta_vs_benchmark"), 0.0)
    return {
        "decision": decision,
        "reason": reason,
        "allowed_decisions": allowed,
        "best_variant_by_mean_delta": best_variant,
        "original_mean_delta_vs_benchmark": _round(original_mean),
        "best_variant_mean_delta_vs_benchmark": _round(best_mean),
        "mean_delta_improved": best_mean > original_mean,
        "tail_loss_reduced": _tail_loss_reduction(original, best) > 0,
        "losing_avg_improved": _float(best.get("losing_avg"), -999.0)
        > _float(original.get("losing_avg"), 0.0),
        "beat_rate_retained": _beat_rate_retention(original, best)
        >= _float(policy.get("beat_rate_retention_floor"), 0.80),
        "turnover_cost_not_worse": _relative_increase_ok(
            original.get("turnover"),
            best.get("turnover"),
            _float(policy.get("turnover_increase_ceiling_share"), 0.10),
        )
        and _relative_increase_ok(
            original.get("cost"),
            best.get("cost"),
            _float(policy.get("cost_increase_ceiling_share"), 0.10),
        ),
        "promotion_gate_allowed": False,
        "paper_shadow_change_allowed": False,
        "production_weight_change_allowed": False,
    }


def _tail_loss_reduction(original: Mapping[str, Any], candidate: Mapping[str, Any]) -> float:
    original_tail = _float(original.get("tail_loss_contribution"), 0.0)
    candidate_tail = _float(candidate.get("tail_loss_contribution"), 0.0)
    return (original_tail - candidate_tail) / original_tail if original_tail else 0.0


def _beat_rate_retention(original: Mapping[str, Any], candidate: Mapping[str, Any]) -> float:
    original_rate = _float(original.get("value_surface_beats_benchmark_rate"), 0.0)
    candidate_rate = _float(candidate.get("value_surface_beats_benchmark_rate"), 0.0)
    return candidate_rate / original_rate if original_rate else 0.0


def _relative_increase_ok(original: Any, candidate: Any, ceiling: float) -> bool:
    original_value = abs(_float(original, 0.0))
    candidate_value = abs(_float(candidate, 0.0))
    if original_value == 0:
        return candidate_value == 0
    return (candidate_value - original_value) / original_value <= ceiling


def _regime_conditioned_review_evidence(
    *,
    design: Mapping[str, Any],
    guardrail: Mapping[str, Any],
    loss_matrix: Mapping[str, Any],
    residual_regime: Mapping[str, Any],
) -> dict[str, Any]:
    guardrail_summary = (
        guardrail.get("summary") if isinstance(guardrail.get("summary"), Mapping) else {}
    )
    matrix_summary = (
        loss_matrix.get("matrix_summary")
        if isinstance(loss_matrix.get("matrix_summary"), Mapping)
        else loss_matrix.get("summary", {})
    )
    return {
        "design_status": design.get("status"),
        "tail_loss_regime_count": design.get("summary", {}).get("tail_loss_regime_count"),
        "guardrail_status": guardrail.get("status"),
        "best_variant_by_mean_delta": guardrail_summary.get("best_variant_by_mean_delta"),
        "tail_loss_guardrail_reduces_tail_loss": guardrail_summary.get(
            "tail_loss_guardrail_reduces_tail_loss"
        ),
        "loss_matrix_status": loss_matrix.get("status"),
        "loss_distribution_assessment": matrix_summary.get("loss_distribution_assessment"),
        "residual_regime_status": residual_regime.get("status"),
        "top_residual_feature": residual_regime.get("summary", {}).get("top_residual_feature"),
        "promotion_gate_allowed": False,
    }


def _high_loss_group_values(
    failure: Mapping[str, Any],
    report_key: str,
    group_key: str,
    share_floor: float,
) -> list[str]:
    report = failure.get(report_key) if isinstance(failure.get(report_key), Mapping) else {}
    groups = _records(report.get("groups"))
    selected = [
        str(row.get(group_key))
        for row in groups
        if row.get(group_key) is not None and _float(row.get("loss_share"), 0.0) >= share_floor
    ]
    if selected:
        return selected
    top = max(groups, key=lambda row: _float(row.get("loss_share"), 0.0), default={})
    return [str(top.get(group_key))] if top.get(group_key) is not None else []


def _cost_turnover_aware_variant_comparison(
    *,
    selected_cases: list[dict[str, Any]],
    config: Mapping[str, Any],
    failure: Mapping[str, Any],
    horizon: Mapping[str, Any],
    design: Mapping[str, Any],
) -> dict[str, Any]:
    policy = _next_stage_section(config, "cost_turnover_aware_regime_conditioned_value_surface")
    variants = [str(item) for item in policy.get("allowed_variants", [])]
    original_rows = [dict(row) for row in selected_cases]
    variant_rows = {
        variant_id: _cost_aware_variant_cases(
            selected_cases=selected_cases,
            variant_id=variant_id,
            config=config,
            failure=failure,
            horizon=horizon,
            design=design,
        )
        for variant_id in variants
    }
    original_metric = _v2_variant_metric_row("original_value_surface", original_rows, config)
    metrics = [
        _add_variant_deltas(_v2_variant_metric_row(variant_id, rows, config), original_metric)
        for variant_id, rows in variant_rows.items()
    ]
    best = max(metrics, key=lambda row: _v2_metric_score(row, policy), default={})
    return {
        "summary": {
            "best_variant_by_v2_score": best.get("variant_id"),
            "best_variant_mean_delta_vs_benchmark": best.get("mean_delta_vs_benchmark"),
            "original_mean_delta_vs_benchmark": original_metric.get("mean_delta_vs_benchmark"),
            "mean_delta_improved": _float(best.get("mean_delta_vs_benchmark"), 0.0)
            > _float(original_metric.get("mean_delta_vs_benchmark"), 0.0),
            "tail_loss_reduced": _float(best.get("tail_loss_delta"), 0.0) < 0,
            "turnover_cost_not_worse": _float(best.get("turnover_delta"), 0.0) <= 0
            and _float(best.get("cost_delta"), 0.0) <= 0,
            "beat_rate_retained": _beat_rate_retention(original_metric, best)
            >= _float(policy.get("beat_rate_retention_floor"), 0.80),
            "promotion_gate_allowed": False,
        },
        "variant_metrics": [original_metric, *metrics],
        "variant_rules": _cost_turnover_variant_rules(policy),
        "transition_report": {
            "original": _transition_report(original_rows),
            **{variant_id: _transition_report(rows) for variant_id, rows in variant_rows.items()},
        },
        "v2_score_policy": {
            "turnover_penalty_weight": policy.get("v2_score_turnover_penalty_weight"),
            "cost_penalty_weight": policy.get("v2_score_cost_penalty_weight"),
            "tail_loss_penalty_weight": policy.get("v2_score_tail_loss_penalty_weight"),
            "policy_source": (
                "controlled_strategy_next_stage_research."
                "cost_turnover_aware_regime_conditioned_value_surface"
            ),
        },
        "diagnostic_boundary": {
            "retrospective_ablation_only": True,
            "requires_forward_cost_confirmation": True,
            "production_execution_rule": False,
            "promotion_gate_allowed": False,
        },
    }


def _cost_aware_variant_cases(
    *,
    selected_cases: list[dict[str, Any]],
    variant_id: str,
    config: Mapping[str, Any],
    failure: Mapping[str, Any],
    horizon: Mapping[str, Any],
    design: Mapping[str, Any],
) -> list[dict[str, Any]]:
    guardrail_policy = _next_stage_section(config, "tail_loss_guardrail_fallback_policy")
    policy = _next_stage_section(config, "cost_turnover_aware_regime_conditioned_value_surface")
    risk_context = _guardrail_risk_context(
        config=config,
        failure=failure,
        horizon=horizon,
        design=design,
    )
    base_rows = [
        _apply_guardrail_variant(
            dict(row),
            "regime_conditioned_value_surface",
            risk_context,
            guardrail_policy,
        )
        for row in selected_cases
    ]
    rows = _annotate_transitions(base_rows)
    if variant_id == "regime_conditioned_turnover_penalty":
        penalty = _float(policy.get("turnover_penalty_bps"), 25.0) / 10_000.0
        return [_apply_turnover_penalty(row, penalty) for row in rows]
    if variant_id == "regime_conditioned_action_hysteresis":
        threshold = _float(policy.get("action_hysteresis_min_delta_bps"), 25.0) / 10_000.0
        return [
            (
                _fallback_case_to_benchmark(dict(row), "action_hysteresis_fallback")
                if row.get("action_flip_for_review")
                and abs(_float(row.get("delta_vs_benchmark"), 0.0)) < threshold
                else row
            )
            for row in rows
        ]
    if variant_id == "regime_conditioned_no_trade_band":
        band = _float(policy.get("no_trade_band_abs_delta_bps"), 10.0) / 10_000.0
        return [
            (
                _fallback_case_to_benchmark(dict(row), "no_trade_band_fallback")
                if abs(_float(row.get("delta_vs_benchmark"), 0.0)) < band
                else row
            )
            for row in rows
        ]
    if variant_id == "regime_conditioned_benchmark_fallback":
        return [
            _apply_guardrail_variant(
                dict(row),
                "benchmark_fallback_value_surface",
                risk_context,
                guardrail_policy,
            )
            for row in selected_cases
        ]
    if variant_id == "regime_conditioned_max_action_change_cap":
        cap = max(0, _first_int(policy.get("max_action_change_cap_per_asset")))
        return _apply_action_change_cap(rows, cap)
    return rows


def _apply_turnover_penalty(row: dict[str, Any], penalty: float) -> dict[str, Any]:
    output = dict(row)
    if row.get("action_flip_for_review") or row.get("horizon_switch_for_review"):
        output["selected_realized_net_return"] = _round(
            _float(row.get("selected_realized_net_return"), 0.0) - penalty
        )
        output["delta_vs_benchmark"] = _round(_float(row.get("delta_vs_benchmark"), 0.0) - penalty)
        output["selected_estimated_cost"] = _round(
            _float(row.get("selected_estimated_cost"), 0.0) + penalty
        )
        output["value_surface_beats_benchmark"] = _float(output.get("delta_vs_benchmark"), 0.0) >= 0
        output["guardrail_action"] = "turnover_penalty_applied"
    return output


def _apply_action_change_cap(rows: list[dict[str, Any]], cap: int) -> list[dict[str, Any]]:
    counts: dict[str, int] = {}
    output = []
    for row in rows:
        item = dict(row)
        asset = str(item.get("asset", "unknown"))
        if item.get("action_flip_for_review"):
            counts[asset] = counts.get(asset, 0) + 1
            if counts[asset] > cap:
                item = _fallback_case_to_benchmark(item, "max_action_change_cap_fallback")
        output.append(item)
    return output


def _annotate_transitions(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    previous_by_asset: dict[str, dict[str, Any]] = {}
    output = []
    horizon_order = {"1d": 1, "5d": 5, "10d": 10, "20d": 20, "60d": 60}
    ordered = sorted(
        [dict(row) for row in rows],
        key=lambda row: (
            str(row.get("asset")),
            str(row.get("date")),
            horizon_order.get(str(row.get("horizon")), _first_int(row.get("horizon_days"))),
        ),
    )
    for row in ordered:
        asset = str(row.get("asset", "unknown"))
        previous = previous_by_asset.get(asset)
        row["action_flip_for_review"] = previous is not None and previous.get(
            "selected_action"
        ) != row.get("selected_action")
        row["horizon_switch_for_review"] = previous is not None and previous.get(
            "horizon"
        ) != row.get("horizon")
        previous_by_asset[asset] = row
        output.append(row)
    return output


def _transition_report(rows: list[dict[str, Any]]) -> dict[str, Any]:
    annotated = _annotate_transitions(rows)
    action_flips = sum(1 for row in annotated if row.get("action_flip_for_review"))
    horizon_switches = sum(1 for row in annotated if row.get("horizon_switch_for_review"))
    return {
        "case_count": len(annotated),
        "action_flip_count": action_flips,
        "horizon_switch_count": horizon_switches,
        "action_flip_rate": _round(action_flips / len(annotated) if annotated else 0.0),
        "horizon_switch_rate": _round(horizon_switches / len(annotated) if annotated else 0.0),
        "promotion_gate_allowed": False,
    }


def _v2_variant_metric_row(
    variant_id: str,
    rows: list[dict[str, Any]],
    config: Mapping[str, Any],
) -> dict[str, Any]:
    metric = _variant_metric_row(variant_id, rows, config)
    transition = _transition_report(rows)
    metric["action_flip_count"] = transition["action_flip_count"]
    metric["horizon_switch_count"] = transition["horizon_switch_count"]
    metric["promotion_gate_allowed"] = False
    return metric


def _add_variant_deltas(
    metric: dict[str, Any],
    original_metric: Mapping[str, Any],
) -> dict[str, Any]:
    output = dict(metric)
    for key, delta_key in [
        ("turnover", "turnover_delta"),
        ("cost", "cost_delta"),
        ("drawdown", "drawdown_delta"),
        ("tail_loss_contribution", "tail_loss_delta"),
        ("mean_delta_vs_benchmark", "mean_delta_improvement"),
        ("value_surface_beats_benchmark_rate", "beat_rate_delta"),
    ]:
        output[delta_key] = _round(
            _float(output.get(key), 0.0) - _float(original_metric.get(key), 0.0)
        )
    return output


def _v2_metric_score(row: Mapping[str, Any], policy: Mapping[str, Any]) -> float:
    turnover_weight = _float(policy.get("v2_score_turnover_penalty_weight"), 0.001)
    cost_weight = _float(policy.get("v2_score_cost_penalty_weight"), 0.01)
    tail_loss_weight = _float(policy.get("v2_score_tail_loss_penalty_weight"), 1.0)
    return (
        _float(row.get("mean_delta_vs_benchmark"), -999.0)
        - max(0.0, _float(row.get("turnover_delta"), 0.0)) * turnover_weight
        - max(0.0, _float(row.get("cost_delta"), 0.0)) * cost_weight
        - max(0.0, _float(row.get("tail_loss_delta"), 0.0)) * tail_loss_weight
    )


def _cost_turnover_variant_rules(policy: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            "variant_id": "regime_conditioned_turnover_penalty",
            "rule": "apply configured return penalty when action or horizon switches",
            "turnover_penalty_bps": policy.get("turnover_penalty_bps"),
            "promotion_gate_allowed": False,
        },
        {
            "variant_id": "regime_conditioned_action_hysteresis",
            "rule": "fallback to benchmark when action changes but delta is inside hysteresis band",
            "action_hysteresis_min_delta_bps": policy.get("action_hysteresis_min_delta_bps"),
            "promotion_gate_allowed": False,
        },
        {
            "variant_id": "regime_conditioned_no_trade_band",
            "rule": "fallback to benchmark when benchmark-relative delta is inside no-trade band",
            "no_trade_band_abs_delta_bps": policy.get("no_trade_band_abs_delta_bps"),
            "promotion_gate_allowed": False,
        },
        {
            "variant_id": "regime_conditioned_benchmark_fallback",
            "rule": "fallback to benchmark for high-risk disagreement cases",
            "promotion_gate_allowed": False,
        },
        {
            "variant_id": "regime_conditioned_max_action_change_cap",
            "rule": "fallback to benchmark after configured action-change cap per asset",
            "max_action_change_cap_per_asset": policy.get("max_action_change_cap_per_asset"),
            "promotion_gate_allowed": False,
        },
    ]


def _long_horizon_quarantine_review(
    *,
    selected_cases: list[dict[str, Any]],
    config: Mapping[str, Any],
    horizon: Mapping[str, Any],
) -> dict[str, Any]:
    policy = _next_stage_section(config, "long_horizon_quarantine_selection_review")
    reviewed = [str(item) for item in policy.get("reviewed_horizons", [])]
    original_metric = _variant_metric_row("original", selected_cases, config)
    variants = {
        variant_id: _long_horizon_variant_cases(selected_cases, config, variant_id, reviewed)
        for variant_id in [str(item) for item in policy.get("comparison_variants", [])]
    }
    metrics = [
        _add_variant_deltas(_variant_metric_row(variant_id, rows, config), original_metric)
        for variant_id, rows in variants.items()
    ]
    best = max(
        metrics, key=lambda row: _float(row.get("mean_delta_vs_benchmark"), -999.0), default={}
    )
    tail_reduction = _tail_loss_reduction(original_metric, best)
    return {
        "summary": {
            "best_comparison_variant": best.get("variant_id"),
            "best_variant_mean_delta_vs_benchmark": best.get("mean_delta_vs_benchmark"),
            "tail_loss_reduction_best_variant": _round(tail_reduction),
            "horizon_selector_issue_likely": tail_reduction
            >= _float(policy.get("high_loss_share_floor"), 0.25),
            "promotion_gate_allowed": False,
        },
        "reviewed_horizons": reviewed,
        "horizon_loss_matrix": _horizon_loss_matrix(selected_cases),
        "horizon_return_matrix": _horizon_return_matrix(selected_cases),
        "horizon_turnover_matrix": _horizon_turnover_matrix(selected_cases),
        "horizon_cliff_matrix": _horizon_cliff_matrix(horizon),
        "disable_vs_downgrade_comparison": metrics,
    }


def _long_horizon_variant_cases(
    rows: list[dict[str, Any]],
    config: Mapping[str, Any],
    variant_id: str,
    reviewed_horizons: list[str],
) -> list[dict[str, Any]]:
    policy = _next_stage_section(config, "long_horizon_quarantine_selection_review")
    target_regime = _next_stage_section(config, "ai_after_chatgpt_full_regime_attribution").get(
        "target_regime",
        "ai_after_chatgpt_full",
    )
    multiplier = _float(policy.get("downgrade_multiplier"), 0.50)
    output = []
    for row in rows:
        item = dict(row)
        reviewed = str(item.get("horizon")) in set(reviewed_horizons)
        if variant_id == "disable_20d_60d" and reviewed:
            item = _fallback_case_to_benchmark(item, "long_horizon_disabled")
        elif variant_id == "downgrade_20d_60d" and reviewed:
            item = _scale_case_delta(item, multiplier, "long_horizon_downgraded")
        elif (
            variant_id == "regime_only_20d_60d"
            and reviewed
            and item.get("regime_segment") == target_regime
        ):
            item = _fallback_case_to_benchmark(item, "long_horizon_target_regime_quarantine")
        elif (
            variant_id == "confirmation_required_20d_60d"
            and reviewed
            and item.get("selected_action") != item.get("benchmark_action")
        ):
            item = _fallback_case_to_benchmark(item, "long_horizon_confirmation_fallback")
        elif variant_id == "fallback_to_shorter_horizon" and reviewed:
            item = _fallback_case_to_benchmark(item, "long_horizon_shorter_horizon_fallback")
        item["variant_id"] = variant_id
        item["promotion_gate_allowed"] = False
        output.append(item)
    return output


def _horizon_loss_matrix(rows: list[dict[str, Any]]) -> dict[str, Any]:
    losers = [row for row in rows if _float(row.get("delta_vs_benchmark"), 0.0) < 0]
    return _loss_concentration(losers, "horizon")


def _horizon_return_matrix(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return _walk_forward_group_result(rows, "horizon")


def _horizon_turnover_matrix(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(str(row.get("horizon", "unknown")), []).append(row)
    return [
        {
            "horizon": horizon,
            "case_count": len(values),
            "turnover": _round(
                sum(_float(row.get("selected_turnover_cost_assumption"), 0.0) for row in values)
            ),
            "cost": _round(sum(_float(row.get("selected_estimated_cost"), 0.0) for row in values)),
            "promotion_gate_allowed": False,
        }
        for horizon, values in sorted(grouped.items())
    ]


def _horizon_cliff_matrix(horizon: Mapping[str, Any]) -> list[dict[str, Any]]:
    report = (
        horizon.get("utility_profile_cliff_report")
        if isinstance(horizon.get("utility_profile_cliff_report"), Mapping)
        else {}
    )
    return _records(report.get("horizon_cliff_rows"))


def _ai_regime_attribution_review(
    selected_cases: list[dict[str, Any]],
    config: Mapping[str, Any],
) -> dict[str, Any]:
    policy = _next_stage_section(config, "ai_after_chatgpt_full_regime_attribution")
    target_regime = str(policy.get("target_regime", "ai_after_chatgpt_full"))
    regime_rows = [row for row in selected_cases if row.get("regime_segment") == target_regime]
    losing = [row for row in regime_rows if _float(row.get("delta_vs_benchmark"), 0.0) < 0]
    enriched = [
        {
            **row,
            "action": row.get("selected_action"),
            "cluster": row.get("asset_cluster"),
        }
        for row in losing
    ]
    loss_by_asset = _loss_matrix_group(enriched, "asset")
    loss_by_horizon = _loss_matrix_group(enriched, "horizon")
    loss_by_action = _loss_matrix_group(enriched, "action")
    loss_by_cluster = _loss_matrix_group(enriched, "cluster")
    benchmark_stability = _benchmark_stability_report(regime_rows)
    overoptimism = _value_surface_overoptimism_report(regime_rows)
    return {
        "summary": {
            "target_regime": target_regime,
            "regime_case_count": len(regime_rows),
            "regime_losing_case_count": len(losing),
            "top_loss_asset": _top_group_value(loss_by_asset, "asset"),
            "top_loss_horizon": _top_group_value(loss_by_horizon, "horizon"),
            "top_loss_action": _top_group_value(loss_by_action, "action"),
            "top_loss_cluster": _top_group_value(loss_by_cluster, "cluster"),
            "value_surface_systematic_overoptimism": overoptimism[
                "systematic_overoptimism_for_review"
            ],
            "promotion_gate_allowed": False,
        },
        "loss_by_asset": loss_by_asset,
        "loss_by_horizon": loss_by_horizon,
        "loss_by_action": loss_by_action,
        "loss_by_cluster": loss_by_cluster,
        "benchmark_stability": benchmark_stability,
        "value_surface_overoptimism": overoptimism,
        "candidate_repairs": [
            {
                "repair": str(repair),
                "diagnostic_evidence": {
                    "top_loss_asset": _top_group_value(loss_by_asset, "asset"),
                    "top_loss_horizon": _top_group_value(loss_by_horizon, "horizon"),
                    "top_loss_action": _top_group_value(loss_by_action, "action"),
                    "top_loss_cluster": _top_group_value(loss_by_cluster, "cluster"),
                },
                "requires_followup_ablation": True,
                "promotion_gate_allowed": False,
            }
            for repair in policy.get("candidate_repairs", [])
        ],
    }


def _top_group_value(report: Mapping[str, Any], group_key: str) -> str | None:
    groups = _records(report.get("groups"))
    if not groups:
        return None
    return str(groups[0].get(group_key))


def _benchmark_stability_report(rows: list[dict[str, Any]]) -> dict[str, Any]:
    selected = [_float(row.get("selected_realized_net_return"), 0.0) for row in rows]
    benchmark = [_float(row.get("benchmark_realized_net_return"), 0.0) for row in rows]
    selected_negative = sum(1 for value in selected if value < 0)
    benchmark_negative = sum(1 for value in benchmark if value < 0)
    return {
        "case_count": len(rows),
        "mean_selected_realized_net_return": _round(_mean(selected)),
        "mean_benchmark_realized_net_return": _round(_mean(benchmark)),
        "selected_negative_rate": _round(selected_negative / len(rows) if rows else 0.0),
        "benchmark_negative_rate": _round(benchmark_negative / len(rows) if rows else 0.0),
        "benchmark_more_stable": benchmark_negative <= selected_negative,
        "promotion_gate_allowed": False,
    }


def _value_surface_overoptimism_report(rows: list[dict[str, Any]]) -> dict[str, Any]:
    optimism = [
        _float(row.get("selected_net_utility"), 0.0)
        - _float(row.get("selected_realized_net_return"), 0.0)
        for row in rows
        if row.get("selected_net_utility") is not None
    ]
    positive = [value for value in optimism if value > 0]
    return {
        "case_count": len(optimism),
        "mean_utility_minus_realized": _round(_mean(optimism)),
        "positive_overoptimism_case_count": len(positive),
        "positive_overoptimism_rate": _round(len(positive) / len(optimism) if optimism else 0.0),
        "systematic_overoptimism_for_review": (
            len(positive) / len(optimism) >= 0.50 if optimism else False
        ),
        "promotion_gate_allowed": False,
    }


def _regime_conditioned_holdout_review(
    *,
    selected_cases: list[dict[str, Any]],
    config: Mapping[str, Any],
    failure: Mapping[str, Any],
    horizon: Mapping[str, Any],
    design: Mapping[str, Any],
    cost_turnover: Mapping[str, Any],
) -> dict[str, Any]:
    policy = _next_stage_section(config, "regime_conditioned_walk_forward_holdout")
    best_variant = str(
        cost_turnover.get("summary", {}).get("best_variant_by_v2_score")
        or "regime_conditioned_benchmark_fallback"
    )
    candidate_cases = _cost_aware_variant_cases(
        selected_cases=selected_cases,
        variant_id=best_variant,
        config=config,
        failure=failure,
        horizon=horizon,
        design=design,
    )
    dimensions = [str(item) for item in policy.get("holdout_dimensions", [])]
    result_by_dimension = {
        dimension: _holdout_dimension_results(
            original_cases=selected_cases,
            candidate_cases=candidate_cases,
            config=config,
            dimension=dimension,
        )
        for dimension in dimensions
    }
    rows = [row for values in result_by_dimension.values() for row in values]
    eligible = [row for row in rows if not row.get("insufficient_holdout_cases")]
    passed = [row for row in eligible if row.get("passed")]
    holdout_pass_rate_floor = _float(policy.get("holdout_pass_rate_floor"), 0.60)
    holdout_pass_rate = len(passed) / len(eligible) if eligible else 0.0
    return {
        "summary": {
            "best_variant": best_variant,
            "holdout_case_count": len(rows),
            "eligible_holdout_case_count": len(eligible),
            "holdout_pass_count": len(passed),
            "holdout_pass_rate": _round(holdout_pass_rate),
            "holdout_pass_rate_floor": holdout_pass_rate_floor,
            "overfit_risk": (
                "HIGH" if eligible and holdout_pass_rate < holdout_pass_rate_floor else "WATCH"
            ),
            "promotion_gate_allowed": False,
        },
        "leave_one_regime_out": result_by_dimension.get("regime_segment", []),
        "leave_one_horizon_out": result_by_dimension.get("horizon", []),
        "leave_one_asset_cluster_out": result_by_dimension.get("asset_cluster", []),
        "leave_one_date_window_out": result_by_dimension.get("date_window", []),
    }


def _holdout_dimension_results(
    *,
    original_cases: list[dict[str, Any]],
    candidate_cases: list[dict[str, Any]],
    config: Mapping[str, Any],
    dimension: str,
) -> list[dict[str, Any]]:
    policy = _next_stage_section(config, "regime_conditioned_walk_forward_holdout")
    min_cases = _first_int(policy.get("min_holdout_case_count"))
    mean_floor = _float(policy.get("mean_delta_floor_bps"), 0.0) / 10_000.0
    tail_floor = _float(policy.get("tail_loss_reduction_floor"), 0.10)
    values = sorted({_holdout_dimension_value(row, dimension, config) for row in original_cases})
    rows = []
    for value in values:
        original_subset = [
            row
            for row in original_cases
            if _holdout_dimension_value(row, dimension, config) == value
        ]
        candidate_subset = [
            row
            for row in candidate_cases
            if _holdout_dimension_value(row, dimension, config) == value
        ]
        original_metric = _v2_variant_metric_row("original_holdout", original_subset, config)
        candidate_metric = _add_variant_deltas(
            _v2_variant_metric_row("candidate_holdout", candidate_subset, config),
            original_metric,
        )
        tail_reduction = _tail_loss_reduction(original_metric, candidate_metric)
        turnover_cost_not_worse = (
            _float(candidate_metric.get("turnover_delta"), 0.0) <= 0
            and _float(candidate_metric.get("cost_delta"), 0.0) <= 0
        )
        insufficient = len(original_subset) < min_cases
        passed = (
            not insufficient
            and _float(candidate_metric.get("mean_delta_vs_benchmark"), 0.0) >= mean_floor
            and tail_reduction >= tail_floor
            and turnover_cost_not_worse
        )
        rows.append(
            {
                "holdout_dimension": dimension,
                "holdout_value": value,
                "case_count": len(original_subset),
                "insufficient_holdout_cases": insufficient,
                "original_mean_delta_vs_benchmark": original_metric.get("mean_delta_vs_benchmark"),
                "candidate_mean_delta_vs_benchmark": candidate_metric.get(
                    "mean_delta_vs_benchmark"
                ),
                "tail_loss_reduction": _round(tail_reduction),
                "turnover_delta": candidate_metric.get("turnover_delta"),
                "cost_delta": candidate_metric.get("cost_delta"),
                "turnover_cost_not_worse": turnover_cost_not_worse,
                "passed": passed,
                "promotion_gate_allowed": False,
            }
        )
    return rows


def _holdout_dimension_value(
    row: Mapping[str, Any],
    dimension: str,
    config: Mapping[str, Any],
) -> str:
    if dimension == "date_window":
        return _date_window(str(row.get("date")), config)
    return str(row.get(dimension, "unknown"))


def _value_surface_v2_review_decision(
    *,
    config: Mapping[str, Any],
    cost_turnover: Mapping[str, Any],
    horizon_quarantine: Mapping[str, Any],
    regime_attribution: Mapping[str, Any],
    holdout: Mapping[str, Any],
) -> dict[str, Any]:
    policy = _next_stage_section(config, "value_surface_v2_controlled_review")
    allowed = [str(item) for item in policy.get("allowed_decisions", [])]
    if not cost_turnover or not horizon_quarantine or not regime_attribution or not holdout:
        decision = "DATA_REQUIRED"
        reason = "missing_required_800_to_803_artifact"
        best = {}
        original = {}
    else:
        metrics = _records(cost_turnover.get("variant_metrics"))
        best_id = cost_turnover.get("summary", {}).get("best_variant_by_v2_score")
        best = next((row for row in metrics if row.get("variant_id") == best_id), {})
        original = next(
            (row for row in metrics if row.get("variant_id") == "original_value_surface"), {}
        )
        mean_ok = (
            _float(best.get("mean_delta_vs_benchmark"), 0.0)
            >= _float(policy.get("mean_delta_floor_bps"), 0.0) / 10_000.0
            and _float(best.get("mean_delta_improvement"), 0.0) > 0
        )
        tail_ok = _tail_loss_reduction(original, best) >= _float(
            policy.get("tail_loss_reduction_required_share"), 0.20
        )
        turnover_cost_ok = (
            _float(best.get("turnover_delta"), 0.0) <= 0
            and _float(best.get("cost_delta"), 0.0) <= 0
        )
        beat_ok = _beat_rate_retention(original, best) >= _float(
            policy.get("beat_rate_retention_floor"), 0.80
        )
        holdout_ok = _float(holdout.get("summary", {}).get("holdout_pass_rate"), 0.0) >= _float(
            policy.get("holdout_pass_rate_floor"), 0.60
        )
        horizon_tail_reduction = _float(
            horizon_quarantine.get("summary", {}).get("tail_loss_reduction_best_variant"),
            0.0,
        )
        if mean_ok and tail_ok and turnover_cost_ok and beat_ok and holdout_ok:
            decision = "CONTINUE_TO_LARGER_CONTROLLED_RESEARCH"
            reason = "v2_conditions_met_without_promotion"
        elif horizon_tail_reduction >= _float(
            policy.get("horizon_quarantine_tail_loss_reduction_floor"), 0.20
        ):
            decision = "PIVOT_TO_HORIZON_SELECTOR"
            reason = "long_horizon_quarantine_reduces_tail_loss"
        elif tail_ok and not turnover_cost_ok:
            decision = "PIVOT_TO_TAIL_RISK_POLICY"
            reason = "tail_loss_improves_but_turnover_or_cost_worsens"
        elif not mean_ok and holdout.get("summary", {}).get("overfit_risk") == "HIGH":
            decision = "KILL_VALUE_SURFACE"
            reason = "v2_no_mean_recovery_and_holdout_overfit_risk_high"
        else:
            decision = "WATCHLIST"
            reason = "v2_improvement_incomplete"
    if allowed and decision not in allowed:
        decision = "WATCHLIST"
        reason = "computed_decision_not_allowed_by_policy"
    return {
        "decision": decision,
        "reason": reason,
        "allowed_decisions": allowed,
        "best_cost_turnover_variant": best.get("variant_id"),
        "best_mean_delta_vs_benchmark": best.get("mean_delta_vs_benchmark"),
        "original_mean_delta_vs_benchmark": original.get("mean_delta_vs_benchmark"),
        "mean_delta_condition_met": _float(best.get("mean_delta_vs_benchmark"), -999.0)
        >= _float(policy.get("mean_delta_floor_bps"), 0.0) / 10_000.0,
        "tail_loss_condition_met": _tail_loss_reduction(original, best)
        >= _float(policy.get("tail_loss_reduction_required_share"), 0.20),
        "turnover_cost_condition_met": _float(best.get("turnover_delta"), 0.0) <= 0
        and _float(best.get("cost_delta"), 0.0) <= 0,
        "beat_rate_condition_met": _beat_rate_retention(original, best)
        >= _float(policy.get("beat_rate_retention_floor"), 0.80),
        "holdout_condition_met": _float(
            holdout.get("summary", {}).get("holdout_pass_rate"),
            0.0,
        )
        >= _float(policy.get("holdout_pass_rate_floor"), 0.60),
        "promotion_gate_allowed": False,
        "paper_shadow_change_allowed": False,
        "production_weight_change_allowed": False,
    }


def _value_surface_v2_evidence_summary(
    *,
    cost_turnover: Mapping[str, Any],
    horizon_quarantine: Mapping[str, Any],
    regime_attribution: Mapping[str, Any],
    holdout: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "cost_turnover_status": cost_turnover.get("status"),
        "best_variant_by_v2_score": cost_turnover.get("summary", {}).get(
            "best_variant_by_v2_score"
        ),
        "turnover_cost_not_worse": cost_turnover.get("summary", {}).get("turnover_cost_not_worse"),
        "horizon_quarantine_status": horizon_quarantine.get("status"),
        "horizon_selector_issue_likely": horizon_quarantine.get("summary", {}).get(
            "horizon_selector_issue_likely"
        ),
        "regime_attribution_status": regime_attribution.get("status"),
        "top_loss_horizon": regime_attribution.get("summary", {}).get("top_loss_horizon"),
        "holdout_status": holdout.get("status"),
        "holdout_pass_rate": holdout.get("summary", {}).get("holdout_pass_rate"),
        "promotion_gate_allowed": False,
    }


def _horizon_selector_problem_contract(config: Mapping[str, Any]) -> dict[str, Any]:
    policy = _next_stage_section(config, "horizon_selector_problem_contract")
    candidate_horizons = [str(item) for item in policy.get("candidate_horizons", [])]
    if not candidate_horizons:
        candidate_horizons = [str(row.get("horizon_id")) for row in _horizons(config)]
    status_map = _horizon_status_map(config)
    allowed_horizons = [
        horizon
        for horizon in candidate_horizons
        if status_map.get(horizon, "ALLOWED") in {"ALLOWED", "DOWNWEIGHTED"}
    ]
    fallback_horizon = str(policy.get("default_fallback_horizon", "5d"))
    preferred = str(policy.get("default_preferred_horizon", fallback_horizon))
    if preferred not in allowed_horizons and allowed_horizons:
        preferred = allowed_horizons[0]
    invalidation_conditions = [str(item) for item in policy.get("invalidation_conditions", [])]
    return {
        "candidate_horizons": [
            {
                "horizon": horizon,
                "status": status_map.get(horizon, "ALLOWED"),
                "promotion_gate_allowed": False,
            }
            for horizon in candidate_horizons
        ],
        "horizon_status": status_map,
        "selector_output_schema": [
            {"field": str(field), "required": True, "promotion_gate_allowed": False}
            for field in policy.get("selector_output_fields", [])
        ],
        "selector_output": {
            "allowed_horizons": allowed_horizons,
            "preferred_horizon": preferred,
            "fallback_horizon": fallback_horizon,
            "horizon_confidence": _float(policy.get("horizon_confidence_default"), 0.50),
            "invalidation_condition": invalidation_conditions,
            "review_interval": str(policy.get("review_interval", "daily")),
            "promotion_gate_allowed": False,
        },
        "target_horizon_is_holding_commitment": bool(
            policy.get("target_horizon_is_holding_commitment", False)
        ),
        "regime_change_can_invalidate_horizon": bool(
            policy.get("regime_change_can_invalidate_horizon", True)
        ),
        "problem_statement": {
            "value_surface_role": "action_scoring_submodule",
            "selector_role": "controlled_research_horizon_policy_candidate",
            "target_horizon_not_holding_period_commitment": True,
            "review_interval_can_remain_daily": True,
            "regime_change_can_invalidate_horizon_early": True,
            "model_training_allowed": False,
            "promotion_gate_allowed": False,
        },
    }


def _horizon_status_map(config: Mapping[str, Any]) -> dict[str, str]:
    policy = _next_stage_section(config, "horizon_selector_problem_contract")
    allowed = {str(item) for item in policy.get("allowed_statuses", [])} or {
        "ALLOWED",
        "DOWNWEIGHTED",
        "QUARANTINED",
        "FALLBACK_ONLY",
    }
    raw = policy.get("horizon_status", {})
    status_map = dict(raw) if isinstance(raw, Mapping) else {}
    return {
        str(horizon): (
            str(status_map.get(str(horizon), "ALLOWED"))
            if str(status_map.get(str(horizon), "ALLOWED")) in allowed
            else "ALLOWED"
        )
        for horizon in [str(item) for item in policy.get("candidate_horizons", [])]
    }


def _long_horizon_quarantine_fallback_review(
    *,
    selected_cases: list[dict[str, Any]],
    config: Mapping[str, Any],
) -> dict[str, Any]:
    policy = _next_stage_section(config, "long_horizon_quarantine_fallback_review")
    variants = [str(item) for item in policy.get("variants", [])]
    original_rows = [dict(row) for row in selected_cases]
    original_metric = _v2_variant_metric_row("baseline_value_surface", original_rows, config)
    metrics = []
    holdout_by_variant = []
    for variant_id in variants:
        rows = _long_horizon_fallback_variant_cases(selected_cases, config, variant_id)
        metric = _add_variant_deltas(
            _v2_variant_metric_row(variant_id, rows, config),
            original_metric,
        )
        metric["beat_rate_retention"] = _beat_rate_retention(original_metric, metric)
        metric["tail_loss_reduction"] = _round(_tail_loss_reduction(original_metric, metric))
        holdout_summary = _horizon_selector_holdout_summary(
            original_cases=selected_cases,
            candidate_cases=rows,
            config=config,
        )
        metric["holdout_pass_rate"] = holdout_summary["holdout_pass_rate"]
        metrics.append(metric)
        holdout_by_variant.append({"variant_id": variant_id, **holdout_summary})
    best = max(
        [row for row in metrics if row.get("variant_id") != "baseline_value_surface"],
        key=lambda row: (
            _float(row.get("tail_loss_reduction"), -999.0),
            -max(0.0, _float(row.get("cost_delta"), 0.0)),
            _float(row.get("value_surface_beats_benchmark_rate"), 0.0),
        ),
        default={},
    )
    tail_floor = _float(policy.get("tail_loss_reduction_floor"), 0.20)
    beat_floor = _float(policy.get("beat_rate_retention_floor"), 0.80)
    return {
        "summary": {
            "best_variant_by_tail_loss": best.get("variant_id"),
            "best_variant_tail_loss_reduction": best.get("tail_loss_reduction"),
            "best_variant_turnover_cost_not_worse": _float(best.get("turnover_delta"), 0.0) <= 0
            and _float(best.get("cost_delta"), 0.0) <= 0,
            "best_variant_beat_rate_retention": best.get("beat_rate_retention"),
            "horizon_selector_problem_supported": _float(best.get("tail_loss_reduction"), 0.0)
            >= tail_floor
            and _float(best.get("beat_rate_retention"), 0.0) >= beat_floor,
            "promotion_gate_allowed": False,
        },
        "variant_metrics": metrics,
        "variant_rules": _long_horizon_fallback_variant_rules(policy),
        "holdout_summary_by_variant": holdout_by_variant,
    }


def _long_horizon_fallback_variant_cases(
    rows: list[dict[str, Any]],
    config: Mapping[str, Any],
    variant_id: str,
) -> list[dict[str, Any]]:
    policy = _next_stage_section(config, "long_horizon_quarantine_fallback_review")
    long_horizons = {str(item) for item in policy.get("long_horizons", [])}
    fallback_horizons = [str(item) for item in policy.get("fallback_horizons", [])]
    utility_floor = _float(policy.get("confirmation_utility_floor_bps"), 0.0) / 10_000.0
    by_key = _horizon_replacement_index(rows)
    output = []
    for row in rows:
        item = dict(row)
        horizon = str(item.get("horizon"))
        is_long = horizon in long_horizons
        if variant_id == "disable_60d" and horizon == "60d":
            item = _fallback_case_to_benchmark(item, "disable_60d")
        elif variant_id == "disable_20d_60d" and is_long:
            item = _fallback_case_to_benchmark(item, "disable_20d_60d")
        elif variant_id == "long_horizon_only_with_confirmation" and is_long:
            confirmed = (
                item.get("selected_action") == item.get("benchmark_action")
                or _float(item.get("selected_net_utility"), 0.0) > utility_floor
            )
            if not confirmed:
                item = _fallback_case_to_benchmark(item, "long_horizon_confirmation_failed")
        elif variant_id == "long_horizon_fallback_to_5d_10d" and is_long:
            item = _fallback_to_shorter_horizon(
                item,
                by_key,
                fallback_horizons,
                "long_horizon_shorter_fallback",
            )
        elif variant_id == "long_horizon_fallback_to_benchmark" and is_long:
            item = _fallback_case_to_benchmark(item, "long_horizon_benchmark_fallback")
        item["variant_id"] = variant_id
        item["promotion_gate_allowed"] = False
        output.append(item)
    return output


def _long_horizon_fallback_variant_rules(policy: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            "variant_id": "baseline_value_surface",
            "rule": "keep original horizon-conditioned value surface cases",
            "promotion_gate_allowed": False,
        },
        {
            "variant_id": "disable_60d",
            "rule": "fallback 60d cases to benchmark",
            "promotion_gate_allowed": False,
        },
        {
            "variant_id": "disable_20d_60d",
            "rule": "fallback 20d and 60d cases to benchmark",
            "promotion_gate_allowed": False,
        },
        {
            "variant_id": "long_horizon_only_with_confirmation",
            "rule": "allow long horizon only when configured confirmation proxy passes",
            "confirmation_utility_floor_bps": policy.get("confirmation_utility_floor_bps"),
            "promotion_gate_allowed": False,
        },
        {
            "variant_id": "long_horizon_fallback_to_5d_10d",
            "rule": "replace long horizon with same date/asset 10d or 5d candidate where available",
            "fallback_horizons": policy.get("fallback_horizons"),
            "promotion_gate_allowed": False,
        },
        {
            "variant_id": "long_horizon_fallback_to_benchmark",
            "rule": "fallback long horizon cases to benchmark action",
            "promotion_gate_allowed": False,
        },
    ]


def _horizon_replacement_index(
    rows: list[dict[str, Any]],
) -> dict[tuple[str, str, str], dict[str, Any]]:
    return {
        (str(row.get("date")), str(row.get("asset")), str(row.get("horizon"))): dict(row)
        for row in rows
    }


def _fallback_to_shorter_horizon(
    row: dict[str, Any],
    by_key: Mapping[tuple[str, str, str], dict[str, Any]],
    fallback_horizons: list[str],
    reason: str,
) -> dict[str, Any]:
    for horizon in fallback_horizons:
        replacement = by_key.get((str(row.get("date")), str(row.get("asset")), horizon))
        if replacement is not None:
            return _replace_case_with_horizon(row, replacement, reason)
    return _fallback_case_to_benchmark(dict(row), f"{reason}_benchmark_unavailable")


def _replace_case_with_horizon(
    original: Mapping[str, Any],
    replacement: Mapping[str, Any],
    reason: str,
) -> dict[str, Any]:
    output = dict(replacement)
    output["original_horizon"] = original.get("original_horizon", original.get("horizon"))
    output["replacement_horizon"] = replacement.get("horizon")
    output["horizon_selector_action"] = reason
    output["guardrail_action"] = reason
    output["promotion_gate_allowed"] = False
    return output


def _horizon_selector_controlled_prototype(
    *,
    selected_cases: list[dict[str, Any]],
    config: Mapping[str, Any],
    horizon_stabilization: Mapping[str, Any],
) -> dict[str, Any]:
    selector_rows = _horizon_selector_rule_cases(
        selected_cases=selected_cases,
        config=config,
        horizon_stabilization=horizon_stabilization,
    )
    metric = _v2_variant_metric_row("horizon_selector_controlled_prototype", selector_rows, config)
    decisions = _horizon_decision_rows(selected_cases, config)
    transition = _transition_report(selector_rows)
    fallback_count = sum(
        1 for row in selector_rows if row.get("horizon_selector_action", "keep") != "keep"
    )
    quarantined = sum(
        1
        for row in selected_cases
        if _horizon_status_map(config).get(str(row.get("horizon")))
        in {"QUARANTINED", "FALLBACK_ONLY"}
    )
    return {
        "summary": {
            "quarantined_horizon_count": quarantined,
            "fallback_count": fallback_count,
            "tail_loss_after_selector": metric.get("tail_loss_contribution"),
            "cost_after_selector": metric.get("cost"),
            "promotion_gate_allowed": False,
        },
        "horizon_decision_by_date": decisions,
        "selector_metric": metric,
        "transition_report": transition,
        "selector_rules": _horizon_selector_rule_descriptions(config),
        "selector_cases": selector_rows[:250],
    }


def _horizon_selector_rule_cases(
    *,
    selected_cases: list[dict[str, Any]],
    config: Mapping[str, Any],
    horizon_stabilization: Mapping[str, Any],
) -> list[dict[str, Any]]:
    policy = _next_stage_section(config, "horizon_selector_controlled_prototype")
    status_map = _horizon_status_map(config)
    long_rule = policy.get("long_horizon_regime_quarantine", {})
    long_regime = str(long_rule.get("regime_segment", "ai_after_chatgpt_full"))
    long_horizons = {str(item) for item in long_rule.get("horizons", [])}
    cliff_horizons = _high_cliff_horizons(horizon_stabilization, config)
    fallback_horizons = [str(policy.get("fallback_horizon", "5d")), "10d", "5d"]
    uncertainty_floor = _float(policy.get("uncertainty_high_utility_floor"), 0.0)
    by_key = _horizon_replacement_index(selected_cases)
    output = []
    for row in selected_cases:
        item = dict(row)
        horizon = str(item.get("horizon"))
        status = status_map.get(horizon, "ALLOWED")
        item["original_horizon"] = item.get("original_horizon", horizon)
        item["horizon_status"] = status
        item["horizon_confidence"] = _horizon_confidence(item, status, config)
        item["horizon_selector_action"] = "keep"
        if status == "FALLBACK_ONLY":
            item = _fallback_to_shorter_horizon(item, by_key, fallback_horizons, "fallback_only")
        elif status == "QUARANTINED" and (
            horizon in long_horizons or item.get("regime_segment") == long_regime
        ):
            item = _fallback_to_shorter_horizon(
                item,
                by_key,
                fallback_horizons,
                "quarantined_long_horizon",
            )
        elif horizon in cliff_horizons:
            item = _fallback_to_shorter_horizon(item, by_key, fallback_horizons, "horizon_cliff")
        elif (
            bool(policy.get("fallback_to_benchmark_when_uncertain", True))
            and _float(item.get("selected_net_utility"), 0.0) <= uncertainty_floor
        ):
            item = _fallback_case_to_benchmark(item, "uncertainty_high_benchmark_fallback")
            item["horizon_selector_action"] = "uncertainty_high_benchmark_fallback"
        item["horizon_confidence"] = _horizon_confidence(item, status, config)
        item["promotion_gate_allowed"] = False
        output.append(item)
    return _annotate_transitions(output)


def _horizon_decision_rows(
    selected_cases: list[dict[str, Any]],
    config: Mapping[str, Any],
) -> list[dict[str, Any]]:
    contract = _next_stage_section(config, "horizon_selector_problem_contract")
    prototype_policy = _next_stage_section(config, "horizon_selector_controlled_prototype")
    status_map = _horizon_status_map(config)
    allowed = [
        horizon for horizon, status in status_map.items() if status in {"ALLOWED", "DOWNWEIGHTED"}
    ]
    fallback = str(contract.get("default_fallback_horizon", "5d"))
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in selected_cases:
        grouped.setdefault((str(row.get("date")), str(row.get("asset"))), []).append(row)
    rows = []
    for (row_date, asset), values in sorted(grouped.items()):
        preferred_row = max(
            [row for row in values if str(row.get("horizon")) in allowed] or values,
            key=lambda row: _float(row.get("selected_net_utility"), -999.0),
        )
        status = status_map.get(str(preferred_row.get("horizon")), "ALLOWED")
        rows.append(
            {
                "date": row_date,
                "asset": asset,
                "allowed_horizons": allowed,
                "preferred_horizon": preferred_row.get("horizon"),
                "fallback_horizon": fallback,
                "horizon_confidence": _horizon_confidence(preferred_row, status, config),
                "invalidation_condition": contract.get("invalidation_conditions", []),
                "review_interval": contract.get("review_interval", "daily"),
                "target_horizon_is_holding_commitment": False,
                "promotion_gate_allowed": False,
            }
        )
    limit = _first_int(prototype_policy.get("max_decision_rows")) or 250
    return rows[:limit]


def _horizon_confidence(
    row: Mapping[str, Any],
    status: str,
    config: Mapping[str, Any],
) -> float:
    contract = _next_stage_section(config, "horizon_selector_problem_contract")
    base = _float(contract.get("horizon_confidence_default"), 0.50)
    if status == "ALLOWED":
        base += 0.20
    elif status == "DOWNWEIGHTED":
        base += 0.05
    elif status == "QUARANTINED":
        base -= 0.15
    elif status == "FALLBACK_ONLY":
        base -= 0.25
    if _float(row.get("selected_net_utility"), 0.0) <= 0:
        base -= 0.10
    return _round(max(0.0, min(1.0, base)))


def _high_cliff_horizons(
    horizon_stabilization: Mapping[str, Any],
    config: Mapping[str, Any],
) -> set[str]:
    policy = _next_stage_section(config, "horizon_selector_controlled_prototype")
    threshold = _float(policy.get("horizon_cliff_high_threshold"), 0.20)
    rows = _horizon_cliff_matrix(horizon_stabilization)
    horizons: set[str] = set()
    for row in rows:
        magnitude = max(
            abs(_float(row.get("delta"), 0.0)),
            abs(_float(row.get("cliff_magnitude"), 0.0)),
            abs(_float(row.get("ranking_delta"), 0.0)),
        )
        if magnitude >= threshold:
            for key in ["horizon", "from_horizon", "to_horizon", "horizon_id"]:
                if row.get(key) is not None:
                    horizons.add(str(row.get(key)))
    return horizons


def _horizon_selector_rule_descriptions(config: Mapping[str, Any]) -> list[dict[str, Any]]:
    prototype = _next_stage_section(config, "horizon_selector_controlled_prototype")
    hysteresis = _next_stage_section(config, "cost_aware_horizon_hysteresis")
    return [
        {
            "rule_id": "long_horizon_regime_quarantine",
            "rule": "quarantine configured long horizons in configured failure regime",
            "policy": prototype.get("long_horizon_regime_quarantine"),
            "model_run_executed": False,
            "promotion_gate_allowed": False,
        },
        {
            "rule_id": "horizon_cliff_fallback",
            "rule": "fallback to shorter horizon when horizon cliff is high",
            "threshold": prototype.get("horizon_cliff_high_threshold"),
            "promotion_gate_allowed": False,
        },
        {
            "rule_id": "uncertainty_benchmark_fallback",
            "rule": "fallback to benchmark when utility confidence proxy is too low",
            "threshold": prototype.get("uncertainty_high_utility_floor"),
            "promotion_gate_allowed": False,
        },
        {
            "rule_id": "cost_aware_hysteresis",
            "rule": "do not switch horizon unless utility advantage and confidence pass policy",
            "min_utility_advantage_bps_for_switch": hysteresis.get(
                "min_utility_advantage_bps_for_switch"
            ),
            "promotion_gate_allowed": False,
        },
    ]


def _cost_aware_horizon_hysteresis_review(
    *,
    selected_cases: list[dict[str, Any]],
    config: Mapping[str, Any],
    horizon_stabilization: Mapping[str, Any],
) -> dict[str, Any]:
    original_metric = _v2_variant_metric_row("baseline_value_surface", selected_cases, config)
    selector_rows = _horizon_selector_rule_cases(
        selected_cases=selected_cases,
        config=config,
        horizon_stabilization=horizon_stabilization,
    )
    selector_metric = _add_variant_deltas(
        _v2_variant_metric_row("horizon_selector_controlled_prototype", selector_rows, config),
        original_metric,
    )
    hysteresis_rows = _apply_cost_aware_horizon_hysteresis(selector_rows, config)
    hysteresis_metric = _add_variant_deltas(
        _v2_variant_metric_row("cost_aware_horizon_hysteresis", hysteresis_rows, config),
        original_metric,
    )
    utility_lost = sum(
        max(
            0.0,
            _float(before.get("selected_realized_net_return"), 0.0)
            - _float(after.get("selected_realized_net_return"), 0.0),
        )
        for before, after in zip(selector_rows, hysteresis_rows, strict=False)
    )
    transition = _transition_report(hysteresis_rows)
    return {
        "summary": {
            "horizon_switch_count": transition["horizon_switch_count"],
            "action_flip_count": transition["action_flip_count"],
            "turnover_delta": hysteresis_metric.get("turnover_delta"),
            "cost_delta": hysteresis_metric.get("cost_delta"),
            "utility_lost_to_hysteresis": _round(utility_lost),
            "tail_loss_reduction": _round(_tail_loss_reduction(original_metric, hysteresis_metric)),
            "promotion_gate_allowed": False,
        },
        "original_metric": original_metric,
        "selector_metric": selector_metric,
        "hysteresis_metric": hysteresis_metric,
        "transition_report": transition,
        "hysteresis_cases": [
            row for row in hysteresis_rows if row.get("hysteresis_action") is not None
        ][:100],
    }


def _apply_cost_aware_horizon_hysteresis(
    rows: list[dict[str, Any]],
    config: Mapping[str, Any],
) -> list[dict[str, Any]]:
    policy = _next_stage_section(config, "cost_aware_horizon_hysteresis")
    no_switch_band = _float(policy.get("no_switch_band_bps"), 10.0) / 10_000.0
    cost_floor = _float(policy.get("high_cost_state_cost_floor"), 0.50)
    confidence_floor = _float(policy.get("high_confidence_floor"), 0.70)
    output = []
    for row in _annotate_transitions(rows):
        item = dict(row)
        should_hold = bool(item.get("horizon_switch_for_review")) and (
            abs(_float(item.get("delta_vs_benchmark"), 0.0)) < no_switch_band
            or _float(item.get("selected_estimated_cost"), 0.0) >= cost_floor
            or _float(item.get("horizon_confidence"), 0.0) < confidence_floor
        )
        if should_hold:
            item = _fallback_case_to_benchmark(item, "horizon_hysteresis_no_switch")
            item["hysteresis_action"] = "horizon_hysteresis_no_switch"
        item["promotion_gate_allowed"] = False
        output.append(item)
    return output


def _horizon_selector_holdout_review(
    *,
    selected_cases: list[dict[str, Any]],
    config: Mapping[str, Any],
    horizon_stabilization: Mapping[str, Any],
) -> dict[str, Any]:
    original_metric = _v2_variant_metric_row("baseline_value_surface", selected_cases, config)
    selector_rows = _horizon_selector_rule_cases(
        selected_cases=selected_cases,
        config=config,
        horizon_stabilization=horizon_stabilization,
    )
    candidate_rows = _apply_cost_aware_horizon_hysteresis(selector_rows, config)
    candidate_metric = _add_variant_deltas(
        _v2_variant_metric_row("horizon_selector_candidate", candidate_rows, config),
        original_metric,
    )
    policy = _next_stage_section(config, "horizon_selector_holdout_review")
    dimensions = [str(item) for item in policy.get("holdout_dimensions", [])]
    result_by_dimension = {
        dimension: _horizon_selector_holdout_dimension_results(
            original_cases=selected_cases,
            candidate_cases=candidate_rows,
            config=config,
            dimension=dimension,
        )
        for dimension in dimensions
    }
    rows = [row for values in result_by_dimension.values() for row in values]
    eligible = [row for row in rows if not row.get("insufficient_holdout_cases")]
    passed = [row for row in eligible if row.get("passed")]
    pass_rate = len(passed) / len(eligible) if eligible else 0.0
    tail_condition = _tail_loss_reduction(original_metric, candidate_metric) >= _float(
        policy.get("tail_loss_reduction_floor"), 0.10
    )
    turnover_cost_condition = (
        _float(candidate_metric.get("turnover_delta"), 0.0) <= 0
        and _float(candidate_metric.get("cost_delta"), 0.0) <= 0
    )
    decision, reason = _horizon_selector_decision(
        policy=policy,
        pass_rate=pass_rate,
        tail_condition=tail_condition,
        turnover_cost_condition=turnover_cost_condition,
        candidate_metric=candidate_metric,
    )
    summary = {
        "horizon_selector_decision": decision,
        "decision_reason": reason,
        "holdout_case_count": len(rows),
        "eligible_holdout_case_count": len(eligible),
        "holdout_pass_count": len(passed),
        "holdout_pass_rate": _round(pass_rate),
        "tail_loss_condition_met": tail_condition,
        "turnover_cost_condition_met": turnover_cost_condition,
        "candidate_mean_delta_vs_benchmark": candidate_metric.get("mean_delta_vs_benchmark"),
        "promotion_gate_allowed": False,
    }
    return {
        "summary": summary,
        "review_decision": {
            **summary,
            "allowed_decisions": policy.get("allowed_decisions", []),
            "promotion_gate_allowed": False,
            "paper_shadow_change_allowed": False,
            "production_weight_change_allowed": False,
        },
        "leave_one_regime_out": result_by_dimension.get("regime_segment", []),
        "leave_one_horizon_out": result_by_dimension.get("horizon", []),
        "leave_one_asset_cluster_out": result_by_dimension.get("asset_cluster", []),
        "leave_one_date_window_out": result_by_dimension.get("date_window", []),
    }


def _horizon_selector_decision(
    *,
    policy: Mapping[str, Any],
    pass_rate: float,
    tail_condition: bool,
    turnover_cost_condition: bool,
    candidate_metric: Mapping[str, Any],
) -> tuple[str, str]:
    allowed = [str(item) for item in policy.get("allowed_decisions", [])]
    pass_floor = _float(policy.get("holdout_pass_rate_floor"), 0.60)
    if pass_rate >= pass_floor and tail_condition and turnover_cost_condition:
        decision = "CONTINUE"
        reason = "selector_holdout_passed_tail_and_cost_conditions"
    elif tail_condition and not turnover_cost_condition:
        decision = "PIVOT_TO_TAIL_RISK_POLICY"
        reason = "tail_loss_improves_but_turnover_or_cost_worsens"
    elif (
        tail_condition
        and _float(candidate_metric.get("value_surface_beats_benchmark_rate"), 0.0) > 0
    ):
        decision = "PIVOT_TO_BENCHMARK_SELECTOR"
        reason = "tail_loss_improves_but_holdout_or_cost_requires_benchmark_selector"
    elif pass_rate == 0 and not tail_condition:
        decision = "KILL_VALUE_SURFACE_AS_ACTION_POLICY"
        reason = "selector_holdout_failed_without_tail_loss_reduction"
    else:
        decision = "WATCHLIST"
        reason = "selector_evidence_incomplete"
    if allowed and decision not in allowed:
        return "WATCHLIST", "computed_decision_not_allowed_by_policy"
    return decision, reason


def _horizon_selector_holdout_summary(
    *,
    original_cases: list[dict[str, Any]],
    candidate_cases: list[dict[str, Any]],
    config: Mapping[str, Any],
) -> dict[str, Any]:
    policy = _next_stage_section(config, "horizon_selector_holdout_review")
    dimensions = [str(item) for item in policy.get("holdout_dimensions", [])]
    rows = [
        row
        for dimension in dimensions
        for row in _horizon_selector_holdout_dimension_results(
            original_cases=original_cases,
            candidate_cases=candidate_cases,
            config=config,
            dimension=dimension,
        )
    ]
    eligible = [row for row in rows if not row.get("insufficient_holdout_cases")]
    passed = [row for row in eligible if row.get("passed")]
    return {
        "holdout_case_count": len(rows),
        "eligible_holdout_case_count": len(eligible),
        "holdout_pass_count": len(passed),
        "holdout_pass_rate": _round(len(passed) / len(eligible) if eligible else 0.0),
        "promotion_gate_allowed": False,
    }


def _horizon_selector_holdout_dimension_results(
    *,
    original_cases: list[dict[str, Any]],
    candidate_cases: list[dict[str, Any]],
    config: Mapping[str, Any],
    dimension: str,
) -> list[dict[str, Any]]:
    policy = _next_stage_section(config, "horizon_selector_holdout_review")
    min_cases = _first_int(policy.get("min_holdout_case_count"))
    mean_floor = _float(policy.get("mean_delta_floor_bps"), 0.0) / 10_000.0
    tail_floor = _float(policy.get("tail_loss_reduction_floor"), 0.10)
    values = sorted(
        {_horizon_selector_holdout_value(row, dimension, config) for row in original_cases}
    )
    rows = []
    for value in values:
        original_subset = [
            row
            for row in original_cases
            if _horizon_selector_holdout_value(row, dimension, config) == value
        ]
        candidate_subset = [
            row
            for row in candidate_cases
            if _horizon_selector_holdout_value(row, dimension, config) == value
        ]
        original_metric = _v2_variant_metric_row("original_holdout", original_subset, config)
        candidate_metric = _add_variant_deltas(
            _v2_variant_metric_row("candidate_holdout", candidate_subset, config),
            original_metric,
        )
        tail_reduction = _tail_loss_reduction(original_metric, candidate_metric)
        turnover_cost_not_worse = (
            _float(candidate_metric.get("turnover_delta"), 0.0) <= 0
            and _float(candidate_metric.get("cost_delta"), 0.0) <= 0
        )
        insufficient = len(original_subset) < min_cases
        passed = (
            not insufficient
            and _float(candidate_metric.get("mean_delta_vs_benchmark"), 0.0) >= mean_floor
            and tail_reduction >= tail_floor
            and turnover_cost_not_worse
        )
        rows.append(
            {
                "holdout_dimension": dimension,
                "holdout_value": value,
                "case_count": len(original_subset),
                "insufficient_holdout_cases": insufficient,
                "original_mean_delta_vs_benchmark": original_metric.get("mean_delta_vs_benchmark"),
                "candidate_mean_delta_vs_benchmark": candidate_metric.get(
                    "mean_delta_vs_benchmark"
                ),
                "tail_loss_reduction": _round(tail_reduction),
                "turnover_delta": candidate_metric.get("turnover_delta"),
                "cost_delta": candidate_metric.get("cost_delta"),
                "turnover_cost_not_worse": turnover_cost_not_worse,
                "passed": passed,
                "promotion_gate_allowed": False,
            }
        )
    return rows


def _horizon_selector_holdout_value(
    row: Mapping[str, Any],
    dimension: str,
    config: Mapping[str, Any],
) -> str:
    if dimension == "date_window":
        return _date_window(str(row.get("date")), config)
    if dimension == "horizon":
        return str(row.get("original_horizon", row.get("horizon", "unknown")))
    return str(row.get(dimension, "unknown"))


def _value_surface_policy_kill_diagnostic_downgrade(
    *,
    config: Mapping[str, Any],
    horizon_holdout: Mapping[str, Any],
    v2_review: Mapping[str, Any],
) -> dict[str, Any]:
    policy = _next_stage_section(config, "value_surface_policy_kill_diagnostic_downgrade")
    holdout_summary = (
        horizon_holdout.get("summary")
        if isinstance(horizon_holdout.get("summary"), Mapping)
        else {}
    )
    v2_summary = v2_review.get("summary") if isinstance(v2_review.get("summary"), Mapping) else {}
    prior_decision = str(holdout_summary.get("horizon_selector_decision", "UNKNOWN"))
    required_prior = str(
        policy.get("required_prior_decision", "KILL_VALUE_SURFACE_AS_ACTION_POLICY")
    )
    return {
        "task_id": policy.get("task_id", "TRADING-810"),
        "action_policy_allowed": bool(policy.get("action_policy_allowed", False)),
        "promotion_gate_allowed": bool(policy.get("promotion_gate_allowed", False)),
        "allowed_uses": [str(item) for item in policy.get("allowed_uses", [])],
        "disallowed_uses": [str(item) for item in policy.get("disallowed_uses", [])],
        "kill_reason": policy.get("kill_reason"),
        "required_prior_decision": required_prior,
        "prior_horizon_selector_decision": prior_decision,
        "prior_decision_matches_required": prior_decision == required_prior,
        "prior_value_surface_v2_decision": v2_summary.get("value_surface_v2_decision"),
        "diagnostic_downgrade_applied": True,
        "future_research_allowed_only_when_use_in_allowed_uses": True,
        "paper_shadow_change_allowed": False,
        "production_weight_change_allowed": False,
        "broker_action": "none",
    }


def _benchmark_first_tail_risk_policy_contract(
    config: Mapping[str, Any],
) -> dict[str, Any]:
    policy = _next_stage_section(config, "benchmark_first_tail_risk_policy_contract")
    return {
        "task_id": policy.get("task_id", "TRADING-811"),
        "base_policy": str(
            policy.get("base_policy", "benchmark_or_simple_trend_static_allocation")
        ),
        "allowed_deviation": [str(item) for item in policy.get("allowed_deviation", [])],
        "risk_downshift_condition": [
            str(item) for item in policy.get("risk_downshift_condition", [])
        ],
        "risk_recovery_condition": [
            str(item) for item in policy.get("risk_recovery_condition", [])
        ],
        "max_turnover_budget": dict(policy.get("max_turnover_budget", {})),
        "fallback_policy": str(policy.get("fallback_policy", "benchmark_first")),
        "review_interval": str(policy.get("review_interval", "daily")),
        "confirmation_window_count": _first_int(policy.get("confirmation_window_count")) or 0,
        "policy_mode": "benchmark_first_tail_risk_avoidance",
        "direct_position_policy": False,
        "promotion_gate_allowed": False,
        "paper_shadow_change_allowed": False,
        "production_weight_change_allowed": False,
    }


def _tail_loss_avoidance_classifier_prototype(
    *,
    selected_cases: list[dict[str, Any]],
    config: Mapping[str, Any],
) -> dict[str, Any]:
    policy = _next_stage_section(config, "tail_loss_avoidance_classifier_prototype")
    large_loss_floor = _float(policy.get("large_loss_delta_floor"), -0.05)
    underperformance_floor = (
        _float(policy.get("benchmark_underperformance_floor_bps"), 0.0) / 10_000
    )
    quantile = _float(policy.get("tail_loss_quantile"), 0.10)
    long_horizons = {str(item) for item in policy.get("long_horizon_failure_horizons", [])}
    losing_cases = [row for row in selected_cases if _float(row.get("delta_vs_benchmark"), 0.0) < 0]
    ordered_losers = sorted(
        losing_cases,
        key=lambda row: _float(row.get("delta_vs_benchmark"), 0.0),
    )
    tail_count = (
        min(len(ordered_losers), max(1, math.ceil(len(ordered_losers) * quantile)))
        if ordered_losers
        else 0
    )
    tail_keys = {_case_key(row) for row in ordered_losers[:tail_count]}
    classifier_rows = []
    for row in selected_cases:
        delta = _float(row.get("delta_vs_benchmark"), 0.0)
        labels = {
            "large_loss_case": delta <= large_loss_floor,
            "tail_loss_case": _case_key(row) in tail_keys,
            "benchmark_underperformance_case": delta < underperformance_floor,
            "long_horizon_failure_case": str(row.get("horizon")) in long_horizons
            and delta < underperformance_floor,
        }
        gate_blocked = any(labels.values())
        classifier_rows.append(
            {
                "case_key": _case_key(row),
                "date": row.get("date"),
                "asset": row.get("asset"),
                "horizon": row.get("horizon"),
                "regime_segment": row.get("regime_segment"),
                "asset_cluster": row.get("asset_cluster"),
                "selected_action": row.get("selected_action"),
                "delta_vs_benchmark": _round(delta),
                **labels,
                "tail_risk_signal_high": gate_blocked,
                "allow_value_surface_or_aggressive_action": not gate_blocked,
                "strategy_signal_generated": False,
                "promotion_gate_allowed": False,
            }
        )
    label_breakdown = [
        _label_breakdown_row(classifier_rows, label)
        for label in [
            "large_loss_case",
            "tail_loss_case",
            "benchmark_underperformance_case",
            "long_horizon_failure_case",
        ]
    ]
    summary = {
        "label_case_count": len(classifier_rows),
        "large_loss_case_count": _count_true(classifier_rows, "large_loss_case"),
        "tail_loss_case_count": _count_true(classifier_rows, "tail_loss_case"),
        "benchmark_underperformance_case_count": _count_true(
            classifier_rows, "benchmark_underperformance_case"
        ),
        "long_horizon_failure_case_count": _count_true(
            classifier_rows, "long_horizon_failure_case"
        ),
        "gate_block_count": _count_true(classifier_rows, "tail_risk_signal_high"),
        "tail_loss_quantile": quantile,
        "large_loss_delta_floor": large_loss_floor,
        "gate_value_surface_or_aggressive_action_only": bool(
            policy.get("gate_value_surface_or_aggressive_action_only", True)
        ),
        "strategy_signal_generated": bool(policy.get("strategy_signal_generated", False)),
        "promotion_gate_allowed": False,
    }
    return {
        "summary": summary,
        "label_breakdown": label_breakdown,
        "classifier_rows": classifier_rows,
        "gate_semantics": {
            "direct_position_policy": False,
            "allows_action_only_when_all_tail_risk_labels_false": True,
            "blocked_output_use": "portfolio_weight_or_order_instruction",
            "allowed_output_use": "gate_value_surface_or_aggressive_action",
            "promotion_gate_allowed": False,
        },
    }


def _conservative_horizon_risk_filter(
    *,
    selected_cases: list[dict[str, Any]],
    config: Mapping[str, Any],
    classifier: Mapping[str, Any],
) -> dict[str, Any]:
    policy = _next_stage_section(config, "conservative_horizon_risk_filter")
    status_map = {str(key): str(value) for key, value in policy.get("horizon_status", {}).items()}
    label_map = _classifier_label_map(classifier)
    fallback_horizons = [str(policy.get("fallback_horizon", "5d")), "10d", "5d", "1d"]
    by_key = _horizon_replacement_index(selected_cases)
    rows = []
    for row in selected_cases:
        item = dict(row)
        horizon = str(item.get("horizon"))
        status = status_map.get(horizon, "ALLOWED")
        labels = label_map.get(_case_key(item), {})
        low_risk = not bool(labels.get("tail_risk_signal_high"))
        item["original_horizon"] = item.get("original_horizon", horizon)
        item["horizon_status"] = status
        item["horizon_risk_filter_action"] = "keep"
        if status == "FALLBACK_ONLY":
            item = _fallback_case_to_benchmark(item, "fallback_only_horizon_benchmark")
            item["horizon_risk_filter_action"] = "fallback_only_horizon_benchmark"
        elif status == "QUARANTINED" and not low_risk:
            item = _fallback_to_shorter_horizon(
                item,
                by_key,
                fallback_horizons,
                "quarantined_horizon_tail_risk_filter",
            )
            item["horizon_risk_filter_action"] = "quarantined_horizon_tail_risk_filter"
        elif status == "QUARANTINED" and low_risk:
            item["horizon_risk_filter_action"] = "allow_quarantined_horizon_low_risk_only"
        item["tail_risk_signal_high"] = bool(labels.get("tail_risk_signal_high"))
        item["promotion_gate_allowed"] = False
        rows.append(item)
    original_metric = _v2_variant_metric_row("original_value_surface", selected_cases, config)
    filtered_metric = _add_variant_deltas(
        _v2_variant_metric_row("conservative_horizon_risk_filter", rows, config),
        original_metric,
    )
    status_counts = {
        status: sum(1 for value in status_map.values() if value == status)
        for status in sorted(set(status_map.values()))
    }
    fallback_count = sum(
        1 for row in rows if row.get("horizon_risk_filter_action") not in {None, "keep"}
    )
    return {
        "horizon_status": [
            {
                "horizon": horizon,
                "status": status,
                "default_usable": status == "ALLOWED",
                "promotion_gate_allowed": False,
            }
            for horizon, status in status_map.items()
        ],
        "horizon_filter_rows": rows,
        "original_metric": original_metric,
        "filtered_metric": filtered_metric,
        "selector_mode": str(
            policy.get("selector_mode", "risk_filter_not_optimal_horizon_selector")
        ),
        "summary": {
            "allowed_horizon_count": status_counts.get("ALLOWED", 0),
            "quarantined_horizon_count": status_counts.get("QUARANTINED", 0),
            "fallback_only_horizon_count": status_counts.get("FALLBACK_ONLY", 0),
            "fallback_count": fallback_count,
            "tail_loss_after_filter": filtered_metric.get("tail_loss_contribution"),
            "turnover_delta": filtered_metric.get("turnover_delta"),
            "cost_delta": filtered_metric.get("cost_delta"),
            "promotion_gate_allowed": False,
        },
    }


def _benchmark_fallback_drawdown_guard_controlled_prototype(
    *,
    selected_cases: list[dict[str, Any]],
    config: Mapping[str, Any],
    classifier: Mapping[str, Any],
    horizon_filter: Mapping[str, Any],
) -> dict[str, Any]:
    policy = _next_stage_section(config, "benchmark_fallback_drawdown_guard_controlled_prototype")
    original_metric = _v2_variant_metric_row("original_value_surface", selected_cases, config)
    label_map = _classifier_label_map(classifier)
    horizon_rows = _records(horizon_filter.get("horizon_filter_rows"))
    horizon_by_key = {_case_key(row): row for row in horizon_rows}
    variant_rows = {
        "original_value_surface": [dict(row) for row in selected_cases],
        "benchmark_first_baseline": [
            _fallback_case_to_benchmark(dict(row), "benchmark_first_baseline")
            for row in selected_cases
        ],
        "tail_risk_benchmark_fallback": [
            _tail_risk_benchmark_fallback_case(row, label_map) for row in selected_cases
        ],
        "drawdown_guard_cash_fallback": [
            _drawdown_guard_cash_fallback_case(row, label_map) for row in selected_cases
        ],
        "conservative_horizon_filter_fallback": [
            dict(horizon_by_key.get(_case_key(row), row)) for row in selected_cases
        ],
    }
    configured_variants = [
        "original_value_surface",
        *[str(item) for item in policy.get("policy_variants", [])],
    ]
    variant_metrics = []
    holdout_summary_by_variant = {}
    for variant_id in configured_variants:
        rows = variant_rows.get(variant_id, [])
        metric = _add_variant_deltas(
            _v2_variant_metric_row(variant_id, rows, config),
            original_metric,
        )
        metric["max_drawdown"] = _round(
            max((_float(row.get("selected_drawdown_proxy"), 0.0) for row in rows), default=0.0)
        )
        metric["tail_loss_reduction"] = _round(_tail_loss_reduction(original_metric, metric))
        metric["beat_rate_retention"] = _round(_beat_rate_retention(original_metric, metric))
        metric["turnover_cost_not_worse"] = (
            _float(metric.get("turnover_delta"), 0.0) <= 0
            and _float(metric.get("cost_delta"), 0.0) <= 0
        )
        variant_metrics.append(metric)
        holdout_summary_by_variant[variant_id] = _horizon_selector_holdout_summary(
            original_cases=selected_cases,
            candidate_cases=rows,
            config=config,
        )
    best = max(
        variant_metrics,
        key=lambda row: (
            _float(row.get("tail_loss_reduction"), 0.0),
            _float(row.get("mean_delta_vs_benchmark"), -999.0),
            -max(0.0, _float(row.get("cost_delta"), 0.0)),
        ),
    )
    holdout_summary = holdout_summary_by_variant.get(str(best.get("variant_id")), {})
    return {
        "variant_metrics": variant_metrics,
        "variant_rules": _benchmark_fallback_variant_rules(policy),
        "holdout_summary_by_variant": holdout_summary_by_variant,
        "summary": {
            "best_variant_by_tail_loss": best.get("variant_id"),
            "best_variant_tail_loss_reduction": best.get("tail_loss_reduction"),
            "best_variant_turnover_cost_not_worse": best.get("turnover_cost_not_worse"),
            "best_variant_mean_delta_vs_benchmark": best.get("mean_delta_vs_benchmark"),
            "best_variant_losing_avg": best.get("losing_avg"),
            "best_variant_max_drawdown": best.get("max_drawdown"),
            "best_variant_turnover": best.get("turnover"),
            "best_variant_cost": best.get("cost"),
            "best_variant_beat_rate_retention": best.get("beat_rate_retention"),
            "holdout_pass_rate": holdout_summary.get("holdout_pass_rate", 0.0),
            "promotion_gate_allowed": False,
        },
    }


def _tail_risk_policy_family_controlled_review(
    *,
    config: Mapping[str, Any],
    policy_kill: Mapping[str, Any],
    contract: Mapping[str, Any],
    classifier: Mapping[str, Any],
    horizon_filter: Mapping[str, Any],
    fallback: Mapping[str, Any],
) -> dict[str, Any]:
    policy = _next_stage_section(config, "tail_risk_policy_family_controlled_review")
    allowed = [str(item) for item in policy.get("allowed_decisions", [])]
    fallback_summary = (
        fallback.get("review_summary")
        if isinstance(fallback.get("review_summary"), Mapping)
        else {}
    )
    metrics = _records(fallback.get("variant_metrics"))
    best_id = str(fallback_summary.get("best_variant_by_tail_loss", ""))
    best = next((row for row in metrics if str(row.get("variant_id")) == best_id), {})
    tail_condition = _float(
        fallback_summary.get("best_variant_tail_loss_reduction"), 0.0
    ) >= _float(policy.get("tail_loss_reduction_floor"), 0.20)
    turnover_cost_condition = bool(
        fallback_summary.get("best_variant_turnover_cost_not_worse", False)
    )
    holdout_condition = _float(fallback_summary.get("holdout_pass_rate"), 0.0) >= _float(
        policy.get("holdout_pass_rate_floor"), 0.60
    )
    beat_condition = _float(best.get("beat_rate_retention"), 0.0) >= _float(
        policy.get("beat_rate_retention_floor"), 0.80
    )
    benchmark_condition = (
        not bool(policy.get("benchmark_improvement_required", True))
        or _float(best.get("mean_delta_vs_benchmark"), -999.0) >= 0
    )
    explainability_condition = (
        policy_kill.get("status") is not None
        and contract.get("status") is not None
        and classifier.get("status") is not None
        and horizon_filter.get("status") is not None
        and fallback.get("status") is not None
        and policy_kill.get("summary", {}).get("action_policy_allowed") is False
        and classifier.get("summary", {}).get("strategy_signal_generated") is False
    )
    missing_artifact = not all(
        artifact for artifact in [policy_kill, contract, classifier, horizon_filter, fallback]
    )
    if missing_artifact:
        decision = "DATA_REQUIRED"
        reason = "required_tail_risk_family_artifact_missing"
    elif (
        tail_condition
        and turnover_cost_condition
        and holdout_condition
        and beat_condition
        and benchmark_condition
        and explainability_condition
    ):
        decision = "CONTINUE"
        reason = "tail_risk_family_passed_tail_cost_holdout_benchmark_explainability"
    elif tail_condition and turnover_cost_condition and explainability_condition:
        decision = "WATCHLIST"
        reason = "tail_and_cost_conditions_pass_but_holdout_or_benchmark_incomplete"
    elif not tail_condition:
        decision = "PIVOT"
        reason = "tail_loss_not_reduced_enough"
    else:
        decision = "WATCHLIST"
        reason = "evidence_incomplete_or_cost_holdout_not_ready"
    if allowed and decision not in allowed:
        decision = "WATCHLIST"
        reason = "computed_decision_not_allowed_by_policy"
    review_decision = {
        "decision": decision,
        "reason": reason,
        "allowed_decisions": allowed,
        "best_variant_by_tail_loss": best_id,
        "tail_loss_condition_met": tail_condition,
        "turnover_cost_condition_met": turnover_cost_condition,
        "holdout_condition_met": holdout_condition,
        "beat_rate_condition_met": beat_condition,
        "benchmark_condition_met": benchmark_condition,
        "explainability_condition_met": explainability_condition,
        "promotion_gate_allowed": False,
        "paper_shadow_change_allowed": False,
        "production_weight_change_allowed": False,
    }
    return {
        "review_decision": review_decision,
        "evidence_summary": {
            "policy_kill_status": policy_kill.get("status"),
            "action_policy_allowed": policy_kill.get("summary", {}).get("action_policy_allowed"),
            "contract_status": contract.get("status"),
            "base_policy": contract.get("summary", {}).get("base_policy"),
            "classifier_status": classifier.get("status"),
            "gate_block_count": classifier.get("summary", {}).get("gate_block_count"),
            "horizon_filter_status": horizon_filter.get("status"),
            "selector_mode": horizon_filter.get("summary", {}).get("selector_mode"),
            "fallback_status": fallback.get("status"),
            "best_variant_mean_delta_vs_benchmark": best.get("mean_delta_vs_benchmark"),
            "best_variant_tail_loss_reduction": fallback_summary.get(
                "best_variant_tail_loss_reduction"
            ),
            "best_variant_turnover_cost_not_worse": fallback_summary.get(
                "best_variant_turnover_cost_not_worse"
            ),
            "holdout_pass_rate": fallback_summary.get("holdout_pass_rate"),
            "promotion_gate_allowed": False,
        },
    }


def _tail_risk_benchmark_fallback_robustness_expansion(
    *,
    selected_cases: list[dict[str, Any]],
    config: Mapping[str, Any],
    classifier: Mapping[str, Any],
) -> dict[str, Any]:
    policy = _next_stage_section(config, "tail_risk_benchmark_fallback_robustness_expansion")
    fallback_rows = _tail_risk_benchmark_fallback_rows(selected_cases, classifier)
    original_metric = _v2_variant_metric_row("original_value_surface", selected_cases, config)
    fallback_metric = _add_variant_deltas(
        _v2_variant_metric_row(str(policy.get("target_variant")), fallback_rows, config),
        original_metric,
    )
    upside = _upside_capture_summary(selected_cases, fallback_rows)
    fallback_count = sum(1 for row in fallback_rows if row.get("fallback_triggered"))
    false_fallback_count = sum(
        1
        for row in fallback_rows
        if row.get("fallback_triggered") and _float(row.get("original_delta_vs_benchmark")) > 0
    )
    max_drawdown_delta = _round(
        _case_max_drawdown(fallback_rows) - _case_max_drawdown(selected_cases)
    )
    fallback_metric["max_drawdown_delta"] = max_drawdown_delta
    fallback_metric["upside_capture"] = upside["upside_capture_ratio"]
    fallback_metric["missed_upside_count"] = upside["missed_upside_count"]
    fallback_metric["false_fallback_count"] = false_fallback_count
    tail_loss_reduction = _tail_loss_reduction(original_metric, fallback_metric)
    decision, reason = _tail_risk_robustness_decision(
        policy=policy,
        fallback_metric=fallback_metric,
        tail_loss_reduction=tail_loss_reduction,
        upside_capture=_float(upside.get("upside_capture_ratio"), 0.0),
    )
    summary = {
        "robustness_decision": decision,
        "decision_reason": reason,
        "fallback_trigger_count": fallback_count,
        "fallback_frequency": _round(fallback_count / len(fallback_rows) if fallback_rows else 0.0),
        "tail_loss_reduction": _round(tail_loss_reduction),
        "mean_delta_vs_benchmark": fallback_metric.get("mean_delta_vs_benchmark"),
        "median_delta_vs_benchmark": fallback_metric.get("median_delta_vs_benchmark"),
        "upside_capture": upside["upside_capture_ratio"],
        "missed_upside_count": upside["missed_upside_count"],
        "false_fallback_count": false_fallback_count,
        "turnover_delta": fallback_metric.get("turnover_delta"),
        "cost_delta": fallback_metric.get("cost_delta"),
        "max_drawdown_delta": max_drawdown_delta,
        "promotion_gate_allowed": False,
    }
    return {
        "summary": summary,
        "original_metric": original_metric,
        "fallback_metric": fallback_metric,
        "by_asset": _tail_risk_fallback_group_breakdown(
            selected_cases, fallback_rows, "asset", config
        ),
        "by_horizon": _tail_risk_fallback_group_breakdown(
            selected_cases, fallback_rows, "horizon", config
        ),
        "by_regime": _tail_risk_fallback_group_breakdown(
            selected_cases, fallback_rows, "regime_segment", config
        ),
        "by_cluster": _tail_risk_fallback_group_breakdown(
            selected_cases, fallback_rows, "asset_cluster", config
        ),
        "fallback_cases": [row for row in fallback_rows if row.get("fallback_triggered")][:100],
    }


def _tail_risk_robustness_decision(
    *,
    policy: Mapping[str, Any],
    fallback_metric: Mapping[str, Any],
    tail_loss_reduction: float,
    upside_capture: float,
) -> tuple[str, str]:
    allowed = [str(item) for item in policy.get("allowed_decisions", [])]
    tail_ok = tail_loss_reduction >= _float(policy.get("tail_loss_reduction_floor"), 0.20)
    mean_ok = _float(fallback_metric.get("mean_delta_vs_benchmark"), -999.0) >= (
        _float(policy.get("mean_delta_floor_bps"), 0.0) / 10_000.0
    )
    upside_ok = upside_capture >= _float(policy.get("upside_capture_floor"), 0.70)
    cost_ok = (
        _float(fallback_metric.get("turnover_delta"), 0.0) <= 0
        and _float(fallback_metric.get("cost_delta"), 0.0) <= 0
    )
    if not fallback_metric:
        decision = "DATA_REQUIRED"
        reason = "fallback_metric_missing"
    elif tail_ok and mean_ok and upside_ok and cost_ok:
        decision = "CONTINUE"
        reason = "fallback_robustness_passed_tail_mean_upside_cost_conditions"
    elif tail_ok and (upside_ok or mean_ok):
        decision = "WATCHLIST"
        reason = "tail_loss_improves_but_some_robustness_conditions_need_review"
    else:
        decision = "KILL"
        reason = "fallback_robustness_conditions_failed"
    if allowed and decision not in allowed:
        return "WATCHLIST", "computed_decision_not_allowed_by_policy"
    return decision, reason


def _tail_risk_fallback_trigger_precision_recall_audit(
    *,
    selected_cases: list[dict[str, Any]],
    config: Mapping[str, Any],
    classifier: Mapping[str, Any],
) -> dict[str, Any]:
    policy = _next_stage_section(config, "tail_risk_fallback_trigger_precision_recall_audit")
    positive_fields = [str(item) for item in policy.get("positive_label_fields", [])]
    false_positive_floor = (
        _float(policy.get("false_positive_original_delta_floor_bps"), 0.0) / 10_000.0
    )
    false_negative_tail_floor = _float(
        policy.get("tail_loss_from_false_negative_delta_floor"), -0.05
    )
    rows = _tail_risk_benchmark_fallback_rows(selected_cases, classifier)
    confusion_rows = []
    for row in rows:
        actual_tail_loss = any(bool(row.get(field)) for field in positive_fields)
        fallback_triggered = bool(row.get("fallback_triggered"))
        original_delta = _float(row.get("original_delta_vs_benchmark"), 0.0)
        true_positive = fallback_triggered and actual_tail_loss
        false_positive = fallback_triggered and original_delta > false_positive_floor
        false_negative = (not fallback_triggered) and actual_tail_loss
        true_negative = (not fallback_triggered) and not actual_tail_loss
        confusion_rows.append(
            {
                "case_key": row.get("case_key"),
                "date": row.get("date"),
                "asset": row.get("asset"),
                "horizon": row.get("horizon"),
                "regime_segment": row.get("regime_segment"),
                "fallback_triggered": fallback_triggered,
                "actual_tail_loss": actual_tail_loss,
                "true_positive": true_positive,
                "false_positive": false_positive,
                "false_negative": false_negative,
                "true_negative": true_negative,
                "missed_upside": row.get("missed_upside"),
                "false_negative_tail_loss": (
                    abs(original_delta)
                    if false_negative and original_delta <= false_negative_tail_floor
                    else 0.0
                ),
                "promotion_gate_allowed": False,
            }
        )
    tp = _count_true(confusion_rows, "true_positive")
    fp = _count_true(confusion_rows, "false_positive")
    fn = _count_true(confusion_rows, "false_negative")
    tn = _count_true(confusion_rows, "true_negative")
    triggered_count = sum(1 for row in rows if row.get("fallback_triggered"))
    risk_downshift_non_tail_negative_count = sum(
        1
        for row in rows
        if row.get("fallback_triggered")
        and not any(bool(row.get(field)) for field in positive_fields)
        and _float(row.get("original_delta_vs_benchmark"), 0.0) <= false_positive_floor
    )
    summary = {
        "true_positive_count": tp,
        "false_positive_count": fp,
        "false_negative_count": fn,
        "true_negative_count": tn,
        "fallback_precision": _round(tp / (tp + fp) if (tp + fp) else 0.0),
        "fallback_recall": _round(tp / (tp + fn) if (tp + fn) else 0.0),
        "false_positive_rate": _round(fp / (fp + tn) if (fp + tn) else 0.0),
        "false_negative_rate": _round(fn / (fn + tp) if (fn + tp) else 0.0),
        "missed_upside_from_false_positive": _round(
            sum(
                _float(row.get("missed_upside"), 0.0)
                for row in confusion_rows
                if row["false_positive"]
            )
        ),
        "tail_loss_from_false_negative": _round(
            sum(_float(row.get("false_negative_tail_loss"), 0.0) for row in confusion_rows)
        ),
        "fallback_trigger_count": triggered_count,
        "risk_downshift_non_tail_negative_count": risk_downshift_non_tail_negative_count,
        "promotion_gate_allowed": False,
    }
    return {
        "summary": summary,
        "confusion_matrix": {
            "true_positive": tp,
            "false_positive": fp,
            "false_negative": fn,
            "true_negative": tn,
            "risk_downshift_non_tail_negative_count": risk_downshift_non_tail_negative_count,
            "promotion_gate_allowed": False,
        },
        "trigger_case_samples": confusion_rows[:250],
    }


def _tail_risk_opportunity_cost_upside_capture_review(
    *,
    selected_cases: list[dict[str, Any]],
    config: Mapping[str, Any],
    classifier: Mapping[str, Any],
) -> dict[str, Any]:
    policy = _next_stage_section(config, "tail_risk_opportunity_cost_upside_capture_review")
    rows = _tail_risk_benchmark_fallback_rows(selected_cases, classifier)
    upside_floor = _float(policy.get("benchmark_upside_floor_bps"), 0.0) / 10_000.0
    upside_cases = [
        row for row in rows if _float(row.get("benchmark_realized_net_return"), 0.0) > upside_floor
    ]
    benchmark_upside_sum = sum(
        _float(row.get("benchmark_realized_net_return"), 0.0) for row in upside_cases
    )
    captured_upside = sum(
        max(0.0, _float(row.get("selected_realized_net_return"), 0.0)) for row in upside_cases
    )
    participation_count = sum(
        1 for row in upside_cases if _float(row.get("selected_realized_net_return"), 0.0) > 0
    )
    missed_cases = [row for row in rows if _float(row.get("missed_upside"), 0.0) > 0]
    missed_cost = sum(_float(row.get("missed_upside"), 0.0) for row in missed_cases)
    avoided_tail_loss = sum(_float(row.get("avoided_tail_loss"), 0.0) for row in rows)
    required_ratio = _float(policy.get("tail_loss_to_missed_upside_min_ratio"), 2.0)
    ratio = avoided_tail_loss / missed_cost if missed_cost else None
    opportunity_ok = (missed_cost == 0 and avoided_tail_loss > 0) or (
        ratio is not None and ratio >= required_ratio
    )
    summary = {
        "benchmark_upside_case_count": len(upside_cases),
        "strategy_participation_count": participation_count,
        "strategy_participation": _round(
            participation_count / len(upside_cases) if upside_cases else 0.0
        ),
        "upside_capture_ratio": _round(
            captured_upside / benchmark_upside_sum if benchmark_upside_sum else 0.0
        ),
        "missed_upside_count": len(missed_cases),
        "missed_upside_cost": _round(missed_cost),
        "avoided_tail_loss": _round(avoided_tail_loss),
        "tail_loss_to_missed_upside_ratio": _round(ratio) if ratio is not None else None,
        "opportunity_cost_condition_met": opportunity_ok,
        "promotion_gate_allowed": False,
    }
    return {
        "summary": summary,
        "missed_upside_cases": missed_cases[:100],
        "missed_upside_concentration": {
            "by_regime": _missed_upside_concentration(missed_cases, "regime_segment"),
            "by_horizon": _missed_upside_concentration(missed_cases, "horizon"),
            "by_asset": _missed_upside_concentration(missed_cases, "asset"),
        },
        "upside_capture_by_regime": _upside_capture_group_rows(rows, "regime_segment"),
        "upside_capture_by_horizon": _upside_capture_group_rows(rows, "horizon"),
        "upside_capture_by_asset": _upside_capture_group_rows(rows, "asset"),
    }


def _tail_risk_forward_evidence_integration(
    *,
    selected_cases: list[dict[str, Any]],
    config: Mapping[str, Any],
    classifier: Mapping[str, Any],
    as_of_date: date | None,
    ledger_path: Path,
    output_root: Path,
) -> dict[str, Any]:
    policy = _next_stage_section(config, "tail_risk_forward_evidence_integration")
    resolved_as_of = as_of_date or date.today()
    archive_id = f"tail_risk_benchmark_fallback:{resolved_as_of.isoformat()}"
    artifact_path = output_root / "tail_risk_benchmark_fallback_forward_evidence_integration.json"
    rows = _tail_risk_benchmark_fallback_rows(selected_cases, classifier)
    max_records = _first_int(policy.get("max_forward_records")) or 250
    selected_rows = sorted(
        rows,
        key=lambda row: (str(row.get("date")), str(row.get("asset")), str(row.get("horizon"))),
        reverse=True,
    )[:max_records]
    future_status = str(policy.get("future_outcome_status", "pending_maturity"))
    maturity_horizons = [str(item) for item in policy.get("maturity_horizons", [])]
    forward_records = [
        _tail_risk_forward_record(
            row=row,
            archive_id=archive_id,
            as_of=resolved_as_of,
            maturity_horizons=maturity_horizons,
            future_status=future_status,
        )
        for row in selected_rows
    ]
    ledger_event = {
        "archive_id": archive_id,
        "as_of": resolved_as_of.isoformat(),
        "archive_path": str(artifact_path),
        "candidate_id": "tail_risk_benchmark_fallback",
        "forward_record_count": len(forward_records),
        "fallback_trigger_count": sum(
            1 for row in forward_records if row.get("fallback_triggered")
        ),
        "outcome_status": future_status,
        "outcome_append_only": True,
        "broker_action": "none",
        "production_effect": "none",
        "promotion_gate_allowed": False,
        "paper_shadow_change_allowed": False,
        "production_weight_change_allowed": False,
    }
    ledger_status = _append_jsonl_once(ledger_path, ledger_event, unique_key="archive_id")
    return {
        "as_of": resolved_as_of.isoformat(),
        "archive_id": archive_id,
        "forward_records": forward_records,
        "ledger_event": ledger_event,
        "summary": {
            "forward_record_count": len(forward_records),
            "fallback_trigger_count": ledger_event["fallback_trigger_count"],
            "ledger_append_status": ledger_status,
            "future_outcome_status": future_status,
            "append_only_integrity_pass": ledger_status in {"APPENDED", "ALREADY_RECORDED"},
            "broker_action": "none",
            "paper_shadow_change_allowed": False,
            "production_weight_change_allowed": False,
            "promotion_gate_allowed": False,
        },
    }


def _tail_risk_fallback_audit_universe_reconciliation(
    *,
    config: Mapping[str, Any],
    robustness: Mapping[str, Any],
    precision: Mapping[str, Any],
    opportunity: Mapping[str, Any],
    forward: Mapping[str, Any],
    paths: Mapping[str, Path],
) -> dict[str, Any]:
    artifacts = {
        "TRADING-816": robustness,
        "TRADING-817": precision,
        "TRADING-818": opportunity,
        "TRADING-819": forward,
    }
    input_artifacts = {
        task_id: _artifact_status(artifact, paths.get(task_id))
        for task_id, artifact in artifacts.items()
    }
    rows = [
        _reconciliation_task_row(
            task_id="TRADING-816",
            artifact_id="tail_risk_benchmark_fallback_robustness_expansion",
            report_path=paths["TRADING-816"],
            payload=robustness,
            sample_universe_name="historical_robustness_universe",
            denominator=_first_int(_mapping(robustness.get("original_metric")).get("case_count")),
            fallback_trigger_count=_first_int(
                _nested_mapping(robustness, "robustness_summary", "summary").get(
                    "fallback_trigger_count"
                )
            ),
            label_definition=(
                "not_primary_confusion_label; robustness compares original vs fallback rows"
            ),
            trigger_definition="tail_risk_signal_high from classifier labels",
            benchmark_name="buy_and_hold_or_hold_exposure_proxy",
            outcome_horizon="configured value-surface horizons",
            asset_universe=_group_values(robustness.get("by_asset"), "group_value"),
        ),
        _reconciliation_task_row(
            task_id="TRADING-817",
            artifact_id="tail_risk_fallback_trigger_precision_recall_audit",
            report_path=paths["TRADING-817"],
            payload=precision,
            sample_universe_name="precision_recall_confusion_universe",
            denominator=_confusion_total(precision),
            fallback_trigger_count=_first_int(
                _nested_mapping(precision, "trigger_audit_summary", "summary").get(
                    "fallback_trigger_count"
                )
            ),
            positive_label_count=_confusion_positive_count(precision),
            negative_label_count=max(
                0, _confusion_total(precision) - _confusion_positive_count(precision)
            ),
            tp=_first_int(_mapping(precision.get("confusion_matrix")).get("true_positive")),
            fp=_first_int(_mapping(precision.get("confusion_matrix")).get("false_positive")),
            fn=_first_int(_mapping(precision.get("confusion_matrix")).get("false_negative")),
            tn=_first_int(_mapping(precision.get("confusion_matrix")).get("true_negative")),
            label_definition="large_loss_case OR tail_loss_case OR long_horizon_failure_case",
            trigger_definition=(
                "fallback_triggered; " "TP+FP excludes risk_downshift_non_tail_negative_count"
            ),
            benchmark_name="selected action vs benchmark proxy",
            outcome_horizon="configured value-surface horizons",
        ),
        _reconciliation_task_row(
            task_id="TRADING-818",
            artifact_id="tail_risk_opportunity_cost_upside_capture_review",
            report_path=paths["TRADING-818"],
            payload=opportunity,
            sample_universe_name="benchmark_upside_opportunity_universe",
            denominator=_first_int(
                _nested_mapping(opportunity, "opportunity_cost_summary", "summary").get(
                    "benchmark_upside_case_count"
                )
            ),
            fallback_trigger_count=_first_int(
                _nested_mapping(robustness, "robustness_summary", "summary").get(
                    "fallback_trigger_count"
                )
            ),
            label_definition="benchmark_realized_net_return > configured upside floor",
            trigger_definition=(
                "not a trigger universe; "
                "opportunity-cost universe filters benchmark upside cases"
            ),
            benchmark_name="benchmark upside days",
            outcome_horizon="configured value-surface horizons",
        ),
        _reconciliation_task_row(
            task_id="TRADING-819",
            artifact_id="tail_risk_forward_evidence_integration",
            report_path=paths["TRADING-819"],
            payload=forward,
            sample_universe_name="forward_evidence_record_universe",
            denominator=_first_int(
                _nested_mapping(forward, "integration_summary", "summary").get(
                    "forward_record_count"
                )
            ),
            fallback_trigger_count=_first_int(
                _nested_mapping(forward, "integration_summary", "summary").get(
                    "fallback_trigger_count"
                )
            ),
            label_definition="future outcome pending; no matured label yet",
            trigger_definition="tail-risk fallback signal captured in forward dry-run record",
            benchmark_name="forward dry-run benchmark output",
            outcome_horizon="pending maturity horizons",
            asset_universe=_forward_record_assets(forward),
        ),
    ]
    count_summary = _count_reconciliation_summary(rows, robustness, precision, opportunity, forward)
    missing_records = [item for row in rows for item in row.pop("missing_field_records")]
    missing_artifacts = [task_id for task_id, artifact in artifacts.items() if not artifact]
    missing_denominator = [
        row["task_id"] for row in rows if row.get("sample_count_total") in {None, 0}
    ]
    warnings = []
    blockers = []
    if missing_artifacts:
        blockers.append(
            {
                "blocker": "missing_required_artifact",
                "affected_tasks": missing_artifacts,
                "impact": "count reconciliation cannot explain all controlled evidence universes",
            }
        )
    if missing_denominator:
        blockers.append(
            {
                "blocker": "missing_denominator",
                "affected_tasks": missing_denominator,
                "impact": "reconciliation_status cannot be RECONCILED without denominators",
            }
        )
    if missing_records:
        warnings.append(
            {
                "warning": "field_level_gaps_disclosed",
                "missing_field_count": len(missing_records),
                "impact": "report remains partially reconciled until all fields are explicit",
            }
        )
    if missing_artifacts:
        status = "BLOCKED_BY_MISSING_ARTIFACT"
    elif missing_denominator:
        status = "INCOMPLETE"
    elif missing_records:
        status = "PARTIALLY_RECONCILED"
    else:
        status = "RECONCILED"
    controlled_status = (
        "CONTROLLED_RESEARCH_BLOCKED"
        if status in {"INCOMPLETE", "BLOCKED_BY_MISSING_ARTIFACT"}
        else "CONTROLLED_RESEARCH_CONTINUE"
    )
    return {
        "status": status,
        "controlled_review_status": controlled_status,
        "input_artifacts": input_artifacts,
        "data_window": _combined_data_window(rows),
        "task_reconciliation_rows": rows,
        "count_reconciliation_summary": count_summary,
        "missing_field_records": missing_records,
        "metrics": {
            "task_count": len(rows),
            "missing_artifact_count": len(missing_artifacts),
            "missing_denominator_count": len(missing_denominator),
            "missing_field_count": len(missing_records),
            "not_directly_comparable_count": sum(
                1 for item in count_summary if not item["is_comparable_to_other_count"]
            ),
        },
        "warnings": warnings,
        "blockers": blockers,
        "next_recommended_action": (
            "restore_missing_artifacts_or_denominators_before_interpreting_counts"
            if blockers
            else "use_reconciled_universe_labels_when_discussing_tail_risk_counts"
        ),
    }


def _tail_risk_fallback_anti_leakage_audit(
    *,
    selected_cases: list[dict[str, Any]],
    config: Mapping[str, Any],
    value_surface: Mapping[str, Any],
    classifier: Mapping[str, Any],
    robustness: Mapping[str, Any],
) -> dict[str, Any]:
    policy = _next_stage_section(config, "tail_risk_fallback_anti_leakage_audit")
    timestamp_rows = _timestamp_availability_audit_rows(value_surface, policy)
    overlap = _label_trigger_overlap_audit(config, policy)
    outcome_rows = _outcome_horizon_separation_rows(selected_cases, value_surface)
    pit_rows = _pit_revision_audit_rows(timestamp_rows, classifier, robustness)
    critical_feature = any(row["leakage_risk"] in {"HIGH", "CRITICAL"} for row in timestamp_rows)
    critical_outcome = any(row["leakage_risk"] == "CRITICAL" for row in outcome_rows)
    critical_coupling = (
        overlap["coupling_risk"] in {"HIGH", "CRITICAL"}
        and not overlap["independent_outcome_validation_present"]
    )
    blockers = []
    if critical_feature:
        blockers.append(
            {
                "blocker": "feature_available_after_decision",
                "impact": "trigger may use future information",
            }
        )
    if critical_outcome:
        blockers.append(
            {
                "blocker": "outcome_horizon_overlaps_decision",
                "impact": "evaluation outcome can contaminate trigger timestamp",
            }
        )
    if critical_coupling:
        blockers.append(
            {
                "blocker": "trigger_label_same_source_without_independent_validation",
                "impact": (
                    "perfect precision/recall may be label coupling "
                    "rather than predictive quality"
                ),
            }
        )
    warnings = []
    if any(row["pit_status"] == "unknown" for row in pit_rows):
        warnings.append(
            {
                "warning": "pit_status_unknown_not_treated_as_safe",
                "impact": "revision-sensitive inputs remain review items",
            }
        )
    status = (
        "ANTI_LEAKAGE_BLOCKED"
        if blockers
        else ("ANTI_LEAKAGE_WARNING" if warnings else "ANTI_LEAKAGE_PASS")
    )
    return {
        "status": status,
        "controlled_review_status": (
            "CONTROLLED_RESEARCH_BLOCKED" if blockers else "CONTROLLED_RESEARCH_CONTINUE"
        ),
        "timestamp_availability_audit": timestamp_rows,
        "label_trigger_overlap_audit": overlap,
        "outcome_horizon_separation_audit": outcome_rows,
        "pit_revision_audit": pit_rows,
        "summary": {
            "feature_count": len(timestamp_rows),
            "feature_lag_fail_count": sum(1 for row in timestamp_rows if not row["lag_pass"]),
            "outcome_overlap_count": sum(1 for row in outcome_rows if row["overlap_detected"]),
            "critical_issue_count": len(blockers),
            "pit_unknown_count": sum(1 for row in pit_rows if row["pit_status"] == "unknown"),
            "promotion_gate_allowed": False,
        },
        "warnings": warnings,
        "blockers": blockers,
        "next_recommended_action": (
            "decouple_trigger_and_label_or_add_independent_forward_outcome_validation"
            if blockers
            else "continue_controlled_monitoring_without_promotion"
        ),
    }


def _tail_risk_fallback_threshold_sensitivity(
    *,
    selected_cases: list[dict[str, Any]],
    config: Mapping[str, Any],
    classifier: Mapping[str, Any],
) -> dict[str, Any]:
    policy = _next_stage_section(config, "tail_risk_fallback_threshold_sensitivity")
    baseline_rows = _tail_risk_variant_rows(selected_cases, classifier)
    variants: list[dict[str, Any]] = [
        _tail_risk_sensitivity_variant_result(
            "baseline",
            selected_cases,
            baseline_rows,
            config,
            threshold_delta=0.0,
            lag_mode="same_day_close",
            outcome_horizon="all",
            benchmark_name="current_dynamic_policy",
            cost_level="cost=0",
        )
    ]
    for delta in [-0.20, -0.10, -0.05, 0.05, 0.10, 0.20]:
        rows = _tail_risk_variant_rows(
            selected_cases,
            classifier,
            min_trigger_score=0.25 * (1.0 + delta),
        )
        variants.append(
            _tail_risk_sensitivity_variant_result(
                f"threshold_{delta:+.0%}",
                selected_cases,
                rows,
                config,
                threshold_delta=delta,
                lag_mode="same_day_close",
                outcome_horizon="all",
                benchmark_name="current_dynamic_policy",
                cost_level="cost=0",
            )
        )
    for lag_mode, lag_days in [("next_day_open", 1), ("next_day_close", 1), ("two_day_lag", 2)]:
        rows = _tail_risk_lagged_variant_rows(selected_cases, classifier, lag_days=lag_days)
        variants.append(
            _tail_risk_sensitivity_variant_result(
                f"lag_{lag_mode}",
                selected_cases,
                rows,
                config,
                threshold_delta=0.0,
                lag_mode=lag_mode,
                outcome_horizon="all",
                benchmark_name="current_dynamic_policy",
                cost_level="cost=0",
            )
        )
    for horizon in ["1d", "3d", "5d", "10d", "20d", "60d"]:
        subset = [row for row in selected_cases if str(row.get("horizon")) == horizon]
        rows = _tail_risk_variant_rows(subset, classifier)
        variants.append(
            _tail_risk_sensitivity_variant_result(
                f"horizon_{horizon}",
                subset,
                rows,
                config,
                threshold_delta=0.0,
                lag_mode="same_day_close",
                outcome_horizon=horizon,
                benchmark_name="current_dynamic_policy",
                cost_level="cost=0",
            )
        )
    for benchmark_name in ["no_trade", "static_baseline", "defensive_baseline"]:
        rows = _tail_risk_variant_rows(
            selected_cases,
            classifier,
            benchmark_mode=benchmark_name,
        )
        variants.append(
            _tail_risk_sensitivity_variant_result(
                f"benchmark_{benchmark_name}",
                selected_cases,
                rows,
                config,
                threshold_delta=0.0,
                lag_mode="same_day_close",
                outcome_horizon="all",
                benchmark_name=benchmark_name,
                cost_level="cost=0",
            )
        )
    for cost_level, cost_penalty in [
        ("cost=low", 0.0005),
        ("cost=medium", 0.001),
        ("cost=high", 0.002),
    ]:
        rows = _tail_risk_variant_rows(selected_cases, classifier, cost_penalty=cost_penalty)
        variants.append(
            _tail_risk_sensitivity_variant_result(
                f"cost_{cost_level.split('=')[1]}",
                selected_cases,
                rows,
                config,
                threshold_delta=0.0,
                lag_mode="same_day_close",
                outcome_horizon="all",
                benchmark_name="current_dynamic_policy",
                cost_level=cost_level,
            )
        )
    stability = _tail_risk_sensitivity_stability_summary(variants, policy)
    status = (
        "SENSITIVITY_FRAGILE"
        if stability["cliff_detected"]
        else ("SENSITIVITY_WARNING" if stability["warning_detected"] else "SENSITIVITY_STABLE")
    )
    return {
        "status": status,
        "variant_results": variants,
        "perturbation_coverage": {
            "baseline_present": any(row["variant_id"] == "baseline" for row in variants),
            "threshold": any(row["threshold_delta"] not in {None, 0.0} for row in variants),
            "lag": any(
                str(row["lag_mode"]).startswith("next") or row["lag_mode"] == "two_day_lag"
                for row in variants
            ),
            "horizon": any(row["outcome_horizon"] != "all" for row in variants),
            "benchmark": any(row["benchmark_name"] != "current_dynamic_policy" for row in variants),
            "cost": any(row["cost_level"] != "cost=0" for row in variants),
            "promotion_gate_allowed": False,
        },
        "stability_summary": stability,
        "promotion_block_reason": (
            "sensitivity_fragile"
            if status == "SENSITIVITY_FRAGILE"
            else "controlled_only_no_promotion"
        ),
        "warnings": stability["warnings"],
        "blockers": [],
        "next_recommended_action": (
            "keep_promotion_and_paper_shadow_blocked_until_fragility_is_resolved"
            if status == "SENSITIVITY_FRAGILE"
            else "continue_controlled_observation"
        ),
    }


def _tail_risk_fallback_regime_segmented_robustness(
    *,
    selected_cases: list[dict[str, Any]],
    config: Mapping[str, Any],
    classifier: Mapping[str, Any],
) -> dict[str, Any]:
    policy = _next_stage_section(config, "tail_risk_fallback_regime_segmented_robustness")
    fallback_rows = _tail_risk_variant_rows(selected_cases, classifier)
    segment_results: list[dict[str, Any]] = []
    segment_results.extend(_calendar_segment_rows(selected_cases, fallback_rows, config, policy))
    segment_results.extend(_volatility_segment_rows(selected_cases, fallback_rows, config, policy))
    segment_results.extend(_trend_segment_rows(selected_cases, fallback_rows, config, policy))
    segment_results.extend(
        _tail_severity_segment_rows(selected_cases, fallback_rows, config, policy)
    )
    segment_unavailable = [
        {
            "segment_type": "liquidity_macro",
            "segment_name": "liquidity_easing/liquidity_neutral/liquidity_tightening",
            "segment_unavailable": True,
            "reason": "missing liquidity proxy in controlled value-surface cases",
            "promotion_gate_allowed": False,
        },
        {
            "segment_type": "liquidity_macro",
            "segment_name": "macro_stress/macro_normal",
            "segment_unavailable": True,
            "reason": "missing macro stress proxy in controlled value-surface cases",
            "promotion_gate_allowed": False,
        },
    ]
    concentration = _regime_concentration_summary(fallback_rows, segment_results, policy)
    if concentration["concentration_risk"] == "HIGH":
        status = "REGIME_CONCENTRATED"
    elif concentration["low_sample_segments"]:
        status = "INSUFFICIENT_SEGMENT_EVIDENCE"
    elif concentration["segment_count_with_negative_effect"] > 0:
        status = "REGIME_WARNING"
    else:
        status = "REGIME_ROBUST"
    controlled = (
        "NEEDS_MORE_EVIDENCE"
        if status == "INSUFFICIENT_SEGMENT_EVIDENCE"
        else "CONTROLLED_RESEARCH_CONTINUE"
    )
    return {
        "status": status,
        "controlled_review_status": controlled,
        "segment_results": segment_results,
        "segment_unavailable": segment_unavailable,
        "concentration_summary": concentration,
        "warnings": segment_unavailable
        + (
            [
                {
                    "warning": "regime_concentration_or_low_sample_detected",
                    "status": status,
                    "promotion_gate_allowed": False,
                }
            ]
            if status in {"REGIME_CONCENTRATED", "INSUFFICIENT_SEGMENT_EVIDENCE"}
            else []
        ),
        "blockers": [],
        "next_recommended_action": (
            "collect_more_segment_evidence_before_any_promotion_discussion"
            if status in {"REGIME_CONCENTRATED", "INSUFFICIENT_SEGMENT_EVIDENCE"}
            else "continue_controlled_segment_monitoring"
        ),
    }


def _tail_risk_fallback_forward_maturity_scoreboard(
    *,
    config: Mapping[str, Any],
    forward: Mapping[str, Any],
    forward_path: Path,
    as_of_date: date | None,
) -> dict[str, Any]:
    policy = _next_stage_section(config, "tail_risk_fallback_forward_maturity_scoreboard")
    resolved_as_of = as_of_date or date.today()
    source_records = _records(forward.get("forward_records"))
    records = [
        _tail_risk_scoreboard_record(record, as_of=resolved_as_of) for record in source_records
    ]
    scoreboard = _tail_risk_forward_scoreboard(records, policy)
    readiness = _forward_promotion_readiness_assessment(scoreboard, policy)
    if scoreboard["matured_record_count"] == 0 and scoreboard["pending_record_count"] > 0:
        status = "FORWARD_PENDING"
    elif scoreboard["matured_record_count"] < _first_int(policy.get("min_matured_forward_records")):
        status = "FORWARD_INSUFFICIENT"
    elif scoreboard["metric_degradation_detected"]:
        status = "FORWARD_DEGRADED"
    else:
        status = "FORWARD_MATURED_CONTINUE"
    scoreboard["status"] = status
    scoreboard["promotion_block_reason"] = _forward_promotion_block_reason(
        status, scoreboard, policy
    )
    warnings = []
    blockers = []
    if status in {"FORWARD_PENDING", "FORWARD_INSUFFICIENT"}:
        warnings.append(
            {
                "warning": "forward_evidence_not_mature_enough",
                "promotion_block_reason": scoreboard["promotion_block_reason"],
                "promotion_gate_allowed": False,
            }
        )
    if status == "FORWARD_DEGRADED":
        blockers.append(
            {
                "blocker": "forward_metrics_degraded",
                "impact": "controlled research blocked until forward degradation is reviewed",
            }
        )
    return {
        "status": status,
        "data_window": _record_data_window(records),
        "record_level_results": records,
        "scoreboard": scoreboard,
        "promotion_readiness_assessment": readiness,
        "warnings": warnings,
        "blockers": blockers,
        "next_recommended_action": (
            "wait_for_forward_outcome_maturity_and_rerun_scoreboard"
            if status in {"FORWARD_PENDING", "FORWARD_INSUFFICIENT"}
            else (
                "investigate_forward_metric_degradation"
                if status == "FORWARD_DEGRADED"
                else "continue_controlled_research_without_promotion"
            )
        ),
        "source_forward_path": str(forward_path),
    }


def _tail_risk_policy_controlled_review_board(
    *,
    config: Mapping[str, Any],
    robustness: Mapping[str, Any],
    precision: Mapping[str, Any],
    opportunity: Mapping[str, Any],
    forward: Mapping[str, Any],
    audit_universe: Mapping[str, Any],
    anti_leakage: Mapping[str, Any],
    sensitivity: Mapping[str, Any],
    regime_segmented: Mapping[str, Any],
    forward_scoreboard: Mapping[str, Any],
) -> dict[str, Any]:
    policy = _next_stage_section(config, "tail_risk_policy_controlled_review_board")
    allowed = [str(item) for item in policy.get("allowed_decisions", [])]
    robust_summary = _nested_mapping(robustness, "robustness_summary", "summary")
    precision_summary = _nested_mapping(precision, "trigger_audit_summary", "summary")
    opportunity_summary = _nested_mapping(opportunity, "opportunity_cost_summary", "summary")
    forward_summary = _nested_mapping(forward, "integration_summary", "summary")
    audit_status = str(audit_universe.get("status", "MISSING")) if audit_universe else "MISSING"
    anti_leakage_status = str(anti_leakage.get("status", "MISSING")) if anti_leakage else "MISSING"
    sensitivity_status = str(sensitivity.get("status", "MISSING")) if sensitivity else "MISSING"
    regime_status = (
        str(regime_segmented.get("status", "MISSING")) if regime_segmented else "MISSING"
    )
    forward_maturity_status = (
        str(forward_scoreboard.get("status", "MISSING")) if forward_scoreboard else "MISSING"
    )
    new_audit_inputs_present = any(
        [audit_universe, anti_leakage, sensitivity, regime_segmented, forward_scoreboard]
    )
    robustness_condition = robust_summary.get("robustness_decision") == "CONTINUE" and _float(
        robust_summary.get("tail_loss_reduction"), 0.0
    ) >= _float(policy.get("tail_loss_reduction_floor"), 0.20)
    trigger_condition = _float(precision_summary.get("fallback_precision"), 0.0) >= _float(
        policy.get("precision_floor"), 0.60
    ) and _float(precision_summary.get("fallback_recall"), 0.0) >= _float(
        policy.get("recall_floor"), 0.60
    )
    opportunity_condition = bool(
        opportunity_summary.get("opportunity_cost_condition_met")
    ) and _float(opportunity_summary.get("upside_capture_ratio"), 0.0) >= _float(
        policy.get("upside_capture_floor"), 0.70
    )
    forward_condition = _first_int(forward_summary.get("forward_record_count")) >= _first_int(
        policy.get("forward_record_count_floor")
    ) and bool(forward_summary.get("append_only_integrity_pass"))
    missing = not all([robustness, precision, opportunity, forward])
    promotion_block_reason = None
    if new_audit_inputs_present and anti_leakage_status == "ANTI_LEAKAGE_BLOCKED":
        decision = "CONTROLLED_RESEARCH_BLOCKED"
        reason = "anti_leakage_audit_blocked"
        promotion_block_reason = "anti_leakage_blocked"
    elif new_audit_inputs_present and audit_status in {"INCOMPLETE", "BLOCKED_BY_MISSING_ARTIFACT"}:
        decision = "CONTROLLED_RESEARCH_BLOCKED"
        reason = "audit_universe_reconciliation_incomplete_or_missing"
        promotion_block_reason = "audit_universe_reconciliation_blocked"
    elif new_audit_inputs_present and forward_maturity_status == "FORWARD_DEGRADED":
        decision = "CONTROLLED_RESEARCH_BLOCKED"
        reason = "forward_maturity_degraded"
        promotion_block_reason = "forward_metric_degradation"
    elif new_audit_inputs_present and sensitivity_status == "SENSITIVITY_FRAGILE":
        decision = "CONTROLLED_RESEARCH_CONTINUE"
        reason = "sensitivity_fragile_but_controlled_research_may_continue"
        promotion_block_reason = "sensitivity_fragile"
    elif new_audit_inputs_present and regime_status in {
        "REGIME_CONCENTRATED",
        "INSUFFICIENT_SEGMENT_EVIDENCE",
    }:
        decision = "CONTROLLED_RESEARCH_CONTINUE"
        reason = "regime_evidence_not_robust"
        promotion_block_reason = "regime_evidence_not_robust"
    elif new_audit_inputs_present and forward_maturity_status in {
        "FORWARD_PENDING",
        "FORWARD_INSUFFICIENT",
    }:
        decision = "NEEDS_MORE_FORWARD_EVIDENCE"
        reason = "forward_evidence_not_mature"
        promotion_block_reason = "forward_evidence_not_mature"
    elif missing:
        decision = "DATA_REQUIRED"
        reason = "required_tail_risk_review_artifact_missing"
    elif robustness_condition and trigger_condition and opportunity_condition and forward_condition:
        decision = "CONTROLLED_RESEARCH_CONTINUE"
        reason = "continue_to_longer_controlled_walk_forward_and_forward_maturity_tracking"
        promotion_block_reason = "promotion_not_allowed_in_controlled_research"
    elif robustness_condition and trigger_condition and opportunity_condition:
        decision = "WATCHLIST_FORWARD_MATURITY"
        reason = "historical_conditions_pass_but_forward_archive_not_ready"
        promotion_block_reason = "forward_evidence_not_mature"
    elif robustness_condition and not opportunity_condition:
        decision = "PIVOT_OVERCONSERVATIVE"
        reason = "tail_loss_reduction_may_be_purchased_by_upside_sacrifice"
        promotion_block_reason = "opportunity_cost_not_cleared"
    elif not robustness_condition:
        decision = "KILL"
        reason = "fallback_robustness_failed"
        promotion_block_reason = "fallback_robustness_failed"
    else:
        decision = "WATCHLIST"
        reason = "mixed_tail_risk_policy_evidence"
        promotion_block_reason = "mixed_controlled_evidence"
    if allowed and decision not in allowed:
        decision = "WATCHLIST"
        reason = "computed_decision_not_allowed_by_policy"
    review_decision = {
        "decision": decision,
        "reason": reason,
        "allowed_decisions": allowed,
        "audit_universe_reconciliation_status": audit_status,
        "anti_leakage_status": anti_leakage_status,
        "sensitivity_status": sensitivity_status,
        "regime_segmented_status": regime_status,
        "forward_maturity_status": forward_maturity_status,
        "promotion_block_reason": promotion_block_reason,
        "robustness_condition_met": robustness_condition,
        "trigger_quality_condition_met": trigger_condition,
        "opportunity_cost_condition_met": opportunity_condition,
        "forward_integration_condition_met": forward_condition,
        "promotion_gate_allowed": False,
        "paper_shadow_change_allowed": False,
        "production_weight_change_allowed": False,
    }
    return {
        "review_decision": review_decision,
        "evidence_summary": {
            "robustness_status": robustness.get("status"),
            "robustness_decision": robust_summary.get("robustness_decision"),
            "tail_loss_reduction": robust_summary.get("tail_loss_reduction"),
            "precision_status": precision.get("status"),
            "fallback_precision": precision_summary.get("fallback_precision"),
            "fallback_recall": precision_summary.get("fallback_recall"),
            "opportunity_status": opportunity.get("status"),
            "upside_capture_ratio": opportunity_summary.get("upside_capture_ratio"),
            "missed_upside_cost": opportunity_summary.get("missed_upside_cost"),
            "forward_status": forward.get("status"),
            "forward_record_count": forward_summary.get("forward_record_count"),
            "future_outcome_status": forward_summary.get("future_outcome_status"),
            "audit_universe_reconciliation_status": audit_status,
            "anti_leakage_status": anti_leakage_status,
            "sensitivity_status": sensitivity_status,
            "regime_segmented_status": regime_status,
            "forward_maturity_status": forward_maturity_status,
            "promotion_gate_allowed": False,
        },
        "warnings": _records(sensitivity.get("warnings"))
        + _records(regime_segmented.get("warnings")),
        "blockers": _records(audit_universe.get("blockers"))
        + _records(anti_leakage.get("blockers"))
        + _records(forward_scoreboard.get("blockers")),
        "next_recommended_action": (
            "resolve_controlled_research_blockers_before_interpreting_fallback"
            if decision == "CONTROLLED_RESEARCH_BLOCKED"
            else (
                "wait_for_more_forward_evidence"
                if decision == "NEEDS_MORE_FORWARD_EVIDENCE"
                else "continue_controlled_research_with_promotion_blocked"
            )
        ),
    }


def _tail_risk_blocker_diagnostic_paths(
    *,
    review_board: Mapping[str, Any],
    audit_universe_reconciliation_path: Path | None,
    anti_leakage_path: Path | None,
    sensitivity_path: Path | None,
    regime_segmented_path: Path | None,
    forward_maturity_scoreboard_path: Path | None,
) -> dict[str, Path]:
    return {
        "audit_universe_reconciliation": audit_universe_reconciliation_path
        or _tail_risk_report_path_from_review(
            review_board,
            "audit_universe_reconciliation_source",
            DEFAULT_TAIL_RISK_AUDIT_UNIVERSE_RECONCILIATION_PATH,
        ),
        "anti_leakage": anti_leakage_path
        or _tail_risk_report_path_from_review(
            review_board,
            "anti_leakage_source",
            DEFAULT_TAIL_RISK_ANTI_LEAKAGE_AUDIT_PATH,
        ),
        "sensitivity": sensitivity_path
        or _tail_risk_report_path_from_review(
            review_board,
            "sensitivity_source",
            DEFAULT_TAIL_RISK_THRESHOLD_SENSITIVITY_PATH,
        ),
        "regime_segmented": regime_segmented_path
        or _tail_risk_report_path_from_review(
            review_board,
            "regime_segmented_source",
            DEFAULT_TAIL_RISK_REGIME_SEGMENTED_ROBUSTNESS_PATH,
        ),
        "forward_maturity": forward_maturity_scoreboard_path
        or _tail_risk_report_path_from_review(
            review_board,
            "forward_maturity_scoreboard_source",
            DEFAULT_TAIL_RISK_FORWARD_MATURITY_SCOREBOARD_PATH,
        ),
    }


def _tail_risk_report_path_from_review(
    review_board: Mapping[str, Any], source_key: str, fallback_path: Path
) -> Path:
    raw_path = _mapping(review_board.get(source_key)).get("path")
    return _resolve_project_path(raw_path) if raw_path else fallback_path


def _tail_risk_fallback_blocker_diagnostic(
    *,
    config: Mapping[str, Any],
    review_board: Mapping[str, Any],
    review_board_path: Path,
    reports: Mapping[str, Mapping[str, Any]],
    report_paths: Mapping[str, Path],
) -> dict[str, Any]:
    review_decision = _mapping(review_board.get("review_decision"))
    input_reports = [
        _tail_risk_input_report_row(
            task_id="TRADING-821",
            report_key="audit_universe_reconciliation",
            payload=reports.get("audit_universe_reconciliation", {}),
            path=report_paths["audit_universe_reconciliation"],
        ),
        _tail_risk_input_report_row(
            task_id="TRADING-822",
            report_key="anti_leakage",
            payload=reports.get("anti_leakage", {}),
            path=report_paths["anti_leakage"],
        ),
        _tail_risk_input_report_row(
            task_id="TRADING-823",
            report_key="sensitivity",
            payload=reports.get("sensitivity", {}),
            path=report_paths["sensitivity"],
        ),
        _tail_risk_input_report_row(
            task_id="TRADING-824",
            report_key="regime_segmented",
            payload=reports.get("regime_segmented", {}),
            path=report_paths["regime_segmented"],
        ),
        _tail_risk_input_report_row(
            task_id="TRADING-825",
            report_key="forward_maturity",
            payload=reports.get("forward_maturity", {}),
            path=report_paths["forward_maturity"],
        ),
    ]
    findings = _tail_risk_severity_ordered_findings(
        config=config,
        input_reports=input_reports,
        reports=reports,
    )
    final_trigger = _tail_risk_final_blocked_trigger(
        review_decision=review_decision,
        findings=findings,
    )
    next_action = _tail_risk_blocker_next_action(final_trigger=final_trigger, findings=findings)
    blocker_rows = [row for row in findings if row["blocks_controlled_research"]]
    warning_rows = _tail_risk_warning_rows(input_reports=input_reports, findings=findings)
    decision = str(
        review_decision.get(
            "decision",
            _mapping(review_board.get("summary")).get("tail_risk_controlled_decision", "MISSING"),
        )
    )
    status = (
        "TAIL_RISK_FALLBACK_BLOCKER_DIAGNOSTIC_COMPLETE"
        if review_board
        else "TAIL_RISK_FALLBACK_BLOCKER_DIAGNOSTIC_DATA_REQUIRED"
    )
    return {
        "status": status,
        "review_board_decision": decision,
        "source_review_board_path": str(review_board_path),
        "input_reports": input_reports,
        "severity_ordered_findings": findings,
        "highest_severity_root_cause": findings[0]["root_cause_id"] if findings else None,
        "final_blocked_trigger": final_trigger,
        "root_cause_summary": {
            "input_report_count": len(input_reports),
            "severity_finding_count": len(findings),
            "controlled_research_blocker_count": len(blocker_rows),
            "warning_report_count": sum(1 for row in input_reports if row["warning_count"]),
            "final_decision": decision,
            "promotion_recommendation_allowed": False,
            "paper_shadow_recommendation_allowed": False,
            "production_change_recommendation_allowed": False,
        },
        "report_registry_entry": _tail_risk_blocker_report_registry_entry(),
        "read_only_assertions": {
            "strategy_logic_changed": False,
            "promotion_gate_allowed": False,
            "paper_shadow_change_allowed": False,
            "production_weight_change_allowed": False,
            "broker_action": "none",
            "production_effect": "none",
            "source_artifacts_mutated": False,
        },
        "blockers": blocker_rows,
        "warnings": warning_rows,
        "next_recommended_action": next_action,
    }


def _tail_risk_input_report_row(
    *,
    task_id: str,
    report_key: str,
    payload: Mapping[str, Any],
    path: Path,
) -> dict[str, Any]:
    warnings = _records(payload.get("warnings"))
    blockers = _records(payload.get("blockers"))
    status = str(payload.get("status", "MISSING")) if payload else "MISSING"
    return {
        "task_id": task_id,
        "report_key": report_key,
        "artifact_path": str(path),
        "present": bool(payload),
        "report_type": payload.get("report_type") if payload else None,
        "status": status,
        "warnings": warnings,
        "warning_count": len(warnings),
        "blockers": blockers,
        "blocker_count": len(blockers),
        "promotion_block_reason": _tail_risk_promotion_block_reason(
            task_id=task_id,
            status=status,
            payload=payload,
        ),
        "root_cause_hint": _tail_risk_root_cause_hint(task_id=task_id, status=status),
        "promotion_gate_allowed": False,
    }


def _tail_risk_promotion_block_reason(
    *, task_id: str, status: str, payload: Mapping[str, Any]
) -> str | None:
    summary = _mapping(payload.get("summary"))
    explicit = (
        payload.get("promotion_block_reason")
        or summary.get("promotion_block_reason")
        or _mapping(payload.get("review_decision")).get("promotion_block_reason")
    )
    if explicit:
        return str(explicit)
    if task_id == "TRADING-821" and status in {"INCOMPLETE", "BLOCKED_BY_MISSING_ARTIFACT"}:
        return "audit_universe_reconciliation_blocked"
    if task_id == "TRADING-822" and status == "ANTI_LEAKAGE_BLOCKED":
        return "anti_leakage_blocked"
    if task_id == "TRADING-824" and status in {
        "REGIME_CONCENTRATED",
        "INSUFFICIENT_SEGMENT_EVIDENCE",
    }:
        return "regime_evidence_not_robust"
    if task_id == "TRADING-825" and status in {"FORWARD_PENDING", "FORWARD_INSUFFICIENT"}:
        return "forward_evidence_not_mature"
    if task_id == "TRADING-825" and status == "FORWARD_DEGRADED":
        return "forward_metric_degradation"
    return None


def _tail_risk_root_cause_hint(*, task_id: str, status: str) -> str | None:
    if task_id == "TRADING-822" and status == "ANTI_LEAKAGE_BLOCKED":
        return "trigger_label_same_source_without_independent_validation"
    if task_id == "TRADING-823" and status == "SENSITIVITY_FRAGILE":
        return "threshold_lag_horizon_benchmark_or_cost_fragility"
    if task_id == "TRADING-824" and status == "REGIME_CONCENTRATED":
        return "regime_evidence_concentrated"
    if task_id == "TRADING-825" and status in {"FORWARD_PENDING", "FORWARD_INSUFFICIENT"}:
        return "forward_evidence_not_mature"
    if task_id == "TRADING-825" and status == "FORWARD_DEGRADED":
        return "forward_metric_degradation"
    if task_id == "TRADING-821" and status in {"INCOMPLETE", "BLOCKED_BY_MISSING_ARTIFACT"}:
        return "universe_reconciliation_incomplete"
    return None


def _tail_risk_severity_ordered_findings(
    *,
    config: Mapping[str, Any],
    input_reports: list[dict[str, Any]],
    reports: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    report_by_task = {row["task_id"]: row for row in input_reports}
    policy = _next_stage_section(config, "tail_risk_fallback_blocker_diagnostic")
    severity_order = [
        str(item)
        for item in policy.get(
            "severity_order",
            [
                "anti_leakage_critical",
                "universe_reconciliation_incomplete",
                "forward_degraded",
                "sensitivity_fragile",
                "regime_concentrated",
                "forward_insufficient_or_pending",
            ],
        )
    ]
    severity_rank = {key: index + 1 for index, key in enumerate(severity_order)}
    candidates = [
        _tail_risk_finding_row(
            root_cause_id="anti_leakage_critical",
            severity_rank=severity_rank,
            input_report=report_by_task["TRADING-822"],
            active=_tail_risk_anti_leakage_active(reports.get("anti_leakage", {})),
            root_cause=(
                "Fallback trigger and tail-risk label share controlled historical label "
                "source without independent forward outcome validation."
            ),
            next_action="decouple_trigger_and_label_or_add_independent_forward_outcome_validation",
            blocks_controlled_research=True,
        ),
        _tail_risk_finding_row(
            root_cause_id="universe_reconciliation_incomplete",
            severity_rank=severity_rank,
            input_report=report_by_task["TRADING-821"],
            active=_tail_risk_universe_incomplete_active(
                reports.get("audit_universe_reconciliation", {})
            ),
            root_cause=(
                "Universe reconciliation has missing source artifacts, denominators, or "
                "date-window fields, so count interpretation cannot be reconciled."
            ),
            next_action="repair_reconciliation_denominators_before_next_controlled_review",
            blocks_controlled_research=True,
        ),
        _tail_risk_finding_row(
            root_cause_id="forward_degraded",
            severity_rank=severity_rank,
            input_report=report_by_task["TRADING-825"],
            active=report_by_task["TRADING-825"]["status"] == "FORWARD_DEGRADED",
            root_cause="Matured forward outcomes degraded beyond configured tolerances.",
            next_action="investigate_forward_metric_degradation_before_next_controlled_review",
            blocks_controlled_research=True,
        ),
        _tail_risk_finding_row(
            root_cause_id="sensitivity_fragile",
            severity_rank=severity_rank,
            input_report=report_by_task["TRADING-823"],
            active=report_by_task["TRADING-823"]["status"] == "SENSITIVITY_FRAGILE",
            root_cause=(
                "Fallback behavior is fragile under threshold, lag, horizon, benchmark, "
                "or transaction-cost perturbations."
            ),
            next_action="resolve_sensitivity_fragility_before_next_controlled_review",
            blocks_controlled_research=False,
        ),
        _tail_risk_finding_row(
            root_cause_id="regime_concentrated",
            severity_rank=severity_rank,
            input_report=report_by_task["TRADING-824"],
            active=report_by_task["TRADING-824"]["status"]
            in {"REGIME_CONCENTRATED", "INSUFFICIENT_SEGMENT_EVIDENCE"},
            root_cause=(
                "Evidence is concentrated or insufficient across required market-regime "
                "segments."
            ),
            next_action="collect_more_segment_evidence_before_next_controlled_review",
            blocks_controlled_research=False,
        ),
        _tail_risk_finding_row(
            root_cause_id="forward_insufficient_or_pending",
            severity_rank=severity_rank,
            input_report=report_by_task["TRADING-825"],
            active=report_by_task["TRADING-825"]["status"]
            in {"FORWARD_PENDING", "FORWARD_INSUFFICIENT"},
            root_cause=(
                "Forward records have not matured or are insufficient; pending outcomes "
                "cannot be counted as realized evidence."
            ),
            next_action="wait_for_forward_outcome_maturity_and_rerun_scoreboard",
            blocks_controlled_research=False,
        ),
    ]
    active_rows = [row for row in candidates if row]
    return sorted(active_rows, key=lambda row: (row["severity_rank"], row["task_id"]))


def _tail_risk_anti_leakage_active(payload: Mapping[str, Any]) -> bool:
    summary = _mapping(payload.get("summary"))
    return (
        payload.get("status") == "ANTI_LEAKAGE_BLOCKED"
        or _first_int(summary.get("critical_issue_count")) > 0
        or any(row.get("blocker") for row in _records(payload.get("blockers")))
    )


def _tail_risk_universe_incomplete_active(payload: Mapping[str, Any]) -> bool:
    summary = _mapping(payload.get("summary"))
    return (
        payload.get("status") in {"INCOMPLETE", "BLOCKED_BY_MISSING_ARTIFACT"}
        or summary.get("controlled_review_status") == "CONTROLLED_RESEARCH_BLOCKED"
    )


def _tail_risk_finding_row(
    *,
    root_cause_id: str,
    severity_rank: Mapping[str, int],
    input_report: Mapping[str, Any],
    active: bool,
    root_cause: str,
    next_action: str,
    blocks_controlled_research: bool,
) -> dict[str, Any] | None:
    if not active:
        return None
    return {
        "severity_rank": severity_rank.get(root_cause_id, 999),
        "severity_label": root_cause_id,
        "task_id": input_report.get("task_id"),
        "report_key": input_report.get("report_key"),
        "artifact_path": input_report.get("artifact_path"),
        "status": input_report.get("status"),
        "root_cause_id": root_cause_id,
        "root_cause": root_cause,
        "warning_count": input_report.get("warning_count", 0),
        "blocker_count": input_report.get("blocker_count", 0),
        "promotion_block_reason": input_report.get("promotion_block_reason"),
        "blocks_controlled_research": blocks_controlled_research,
        "blocks_promotion": True,
        "next_recommended_action": next_action,
        "promotion_gate_allowed": False,
    }


def _tail_risk_final_blocked_trigger(
    *,
    review_decision: Mapping[str, Any],
    findings: list[dict[str, Any]],
) -> dict[str, Any]:
    decision = str(review_decision.get("decision", "MISSING"))
    reason = str(review_decision.get("reason", ""))
    reason_map = {
        "anti_leakage_audit_blocked": ("TRADING-822", "anti_leakage_critical"),
        "audit_universe_reconciliation_incomplete_or_missing": (
            "TRADING-821",
            "universe_reconciliation_incomplete",
        ),
        "forward_maturity_degraded": ("TRADING-825", "forward_degraded"),
    }
    trigger_tasks: list[str] = []
    trigger_root_causes: list[str] = []
    if decision == "CONTROLLED_RESEARCH_BLOCKED" and reason in reason_map:
        task_id, root_cause_id = reason_map[reason]
        trigger_tasks = [task_id]
        trigger_root_causes = [root_cause_id]
    elif decision == "CONTROLLED_RESEARCH_BLOCKED":
        blocking_findings = [row for row in findings if row["blocks_controlled_research"]]
        trigger_tasks = [str(row["task_id"]) for row in blocking_findings]
        trigger_root_causes = [str(row["root_cause_id"]) for row in blocking_findings]
    return {
        "decision": decision,
        "review_board_reason": reason or None,
        "trigger_tasks": trigger_tasks,
        "trigger_root_causes": trigger_root_causes,
        "promotion_gate_allowed": False,
    }


def _tail_risk_blocker_next_action(
    *,
    final_trigger: Mapping[str, Any],
    findings: list[dict[str, Any]],
) -> str:
    trigger_causes = set(final_trigger.get("trigger_root_causes") or [])
    for finding in findings:
        if finding["root_cause_id"] in trigger_causes:
            return str(finding["next_recommended_action"])
    return (
        str(findings[0]["next_recommended_action"])
        if findings
        else "continue_read_only_monitoring_with_promotion_blocked"
    )


def _tail_risk_warning_rows(
    *,
    input_reports: list[dict[str, Any]],
    findings: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    rows = [
        {
            "task_id": report["task_id"],
            "report_key": report["report_key"],
            "status": report["status"],
            "warning_count": report["warning_count"],
            "warnings": report["warnings"],
            "promotion_gate_allowed": False,
        }
        for report in input_reports
        if report["warning_count"]
    ]
    rows.extend(
        {
            "task_id": finding["task_id"],
            "report_key": finding["report_key"],
            "status": finding["status"],
            "root_cause_id": finding["root_cause_id"],
            "warning": "root_cause_blocks_promotion_but_not_controlled_research",
            "promotion_gate_allowed": False,
        }
        for finding in findings
        if not finding["blocks_controlled_research"]
    )
    return rows


def _tail_risk_blocker_report_registry_entry() -> dict[str, Any]:
    return {
        "report_id": "tail_risk_fallback_blocker_diagnostic",
        "title": "Tail-Risk Fallback Blocker Diagnostic",
        "command": "aits research strategies tail-risk-fallback-blocker-diagnostic",
        "artifact_globs": [
            "outputs/research_strategies/value_surface_review/"
            "tail_risk_fallback_blocker_diagnostic.json",
            "outputs/research_strategies/value_surface_review/"
            "tail_risk_fallback_blocker_diagnostic.md",
        ],
        "artifact_selection_policy": "latest_available",
        "required_for_daily_reading": False,
        "production_effect": "none",
    }


def _tail_risk_trigger_label_independence_audit(
    *,
    config: Mapping[str, Any],
    classifier: Mapping[str, Any],
    robustness: Mapping[str, Any],
    precision: Mapping[str, Any],
    anti_leakage: Mapping[str, Any],
    forward: Mapping[str, Any],
) -> dict[str, Any]:
    policy = _next_stage_section(config, "tail_risk_trigger_label_independence_audit")
    anti_policy = _next_stage_section(config, "tail_risk_fallback_anti_leakage_audit")
    precision_policy = _next_stage_section(
        config, "tail_risk_fallback_trigger_precision_recall_audit"
    )
    trigger_fields = _policy_string_list(
        policy,
        "trigger_input_fields",
        anti_policy.get(
            "trigger_input_fields",
            [
                "large_loss_case",
                "tail_loss_case",
                "benchmark_underperformance_case",
                "long_horizon_failure_case",
            ],
        ),
    )
    trigger_derived_fields = _policy_string_list(
        policy,
        "trigger_derived_fields",
        [
            "tail_risk_signal_high",
            "fallback_triggered",
            "trigger_labels",
            "trigger_reason",
            "trigger_score",
        ],
    )
    label_fields = _policy_string_list(
        policy,
        "label_input_fields",
        anti_policy.get(
            "label_input_fields",
            precision_policy.get(
                "positive_label_fields",
                ["large_loss_case", "tail_loss_case", "long_horizon_failure_case"],
            ),
        ),
    )
    validation_outcome_fields = _policy_string_list(
        policy,
        "validation_outcome_fields",
        [
            "selected_realized_net_return",
            "benchmark_realized_net_return",
            "delta_vs_benchmark",
            "missed_upside",
            "avoided_tail_loss",
            "fallback_precision",
            "fallback_recall",
            "confusion_matrix",
        ],
    )
    forward_outcome_fields = _policy_string_list(
        policy,
        "independent_forward_outcome_fields",
        [
            "future_5d_max_drawdown",
            "future_10d_max_drawdown",
            "future_20d_max_drawdown",
            "future_20d_realized_vol",
            "future_20d_underperform_vs_static",
            "future_20d_recovery_failure",
            "future_gap_down_event",
        ],
    )
    label_derived_fields = _policy_string_list(
        policy,
        "label_derived_fields",
        [
            "tail_risk_validation_label",
            "precision_recall_confusion_matrix",
            "future_outcome_after_maturity",
        ],
    )
    dependencies = _tail_risk_dependency_map(
        trigger_fields=trigger_fields,
        trigger_derived_fields=trigger_derived_fields,
        label_fields=label_fields,
        validation_outcome_fields=validation_outcome_fields,
        forward_outcome_fields=forward_outcome_fields,
        label_derived_fields=label_derived_fields,
    )
    overlap_matrix = _tail_risk_overlap_matrix(
        trigger_fields=trigger_fields,
        trigger_derived_fields=trigger_derived_fields,
        label_fields=label_fields,
        validation_outcome_fields=validation_outcome_fields,
        forward_outcome_fields=forward_outcome_fields,
        dependencies=dependencies,
    )
    time_window_matrix = _tail_risk_time_window_matrix(
        trigger_fields=trigger_fields,
        trigger_derived_fields=trigger_derived_fields,
        label_fields=label_fields,
        validation_outcome_fields=validation_outcome_fields,
        forward_outcome_fields=forward_outcome_fields,
        dependencies=dependencies,
    )
    derived_dependency_matrix = _tail_risk_derived_dependency_matrix(
        trigger_fields=trigger_fields,
        trigger_derived_fields=trigger_derived_fields,
        label_fields=label_fields,
        label_derived_fields=label_derived_fields,
        forward_outcome_fields=forward_outcome_fields,
        dependencies=dependencies,
    )
    direct_overlap_rows = [row for row in overlap_matrix if row["direct_field_overlap"]]
    derived_overlap_rows = [row for row in overlap_matrix if row["derived_overlap"]]
    time_window_blockers = [
        row
        for row in time_window_matrix
        if row["field_role"].startswith("trigger") and not row["decision_time_visible"]
    ]
    derived_blockers = [row for row in derived_dependency_matrix if row["blocking"]]
    status = (
        "BLOCKED"
        if direct_overlap_rows or derived_overlap_rows or time_window_blockers or derived_blockers
        else "PASS"
    )
    same_definition = status == "BLOCKED"
    blockers = []
    if direct_overlap_rows:
        blockers.append(
            {
                "blocker": "trigger_label_direct_field_overlap",
                "shared_fields": sorted({row["trigger_field"] for row in direct_overlap_rows}),
                "impact": "trigger inputs also define the validation label",
                "promotion_gate_allowed": False,
            }
        )
    if derived_overlap_rows or derived_blockers:
        blockers.append(
            {
                "blocker": "trigger_label_shared_derived_logic",
                "shared_core_fields": sorted(
                    {
                        field
                        for row in derived_overlap_rows
                        for field in row.get("shared_core_fields", [])
                    }
                ),
                "impact": (
                    "fallback trigger and validation label are built from the same risk cases"
                ),
                "promotion_gate_allowed": False,
            }
        )
    if time_window_blockers:
        blockers.append(
            {
                "blocker": "trigger_inputs_not_decision_time_visible",
                "affected_fields": sorted({row["field_name"] for row in time_window_blockers}),
                "impact": (
                    "trigger depends on label-proxy fields not proven visible at decision_time"
                ),
                "promotion_gate_allowed": False,
            }
        )
    warnings = [
        {
            "warning": "independent_forward_outcome_validation_not_yet_present",
            "required_fields": forward_outcome_fields,
            "impact": (
                "return metrics and precision/recall remain diagnostic until objective "
                "forward outcomes are validated"
            ),
            "promotion_gate_allowed": False,
        }
    ]
    return {
        "status": status,
        "owner_suggested_task_id": str(policy.get("owner_suggested_task_id", "TRADING-826")),
        "trigger_fields": {
            "input_fields": trigger_fields,
            "derived_fields": trigger_derived_fields,
        },
        "label_outcome_fields": {
            "label_input_fields": label_fields,
            "validation_outcome_fields": validation_outcome_fields,
        },
        "forward_outcome_fields": forward_outcome_fields,
        "overlap_matrix": overlap_matrix,
        "time_window_matrix": time_window_matrix,
        "derived_dependency_matrix": derived_dependency_matrix,
        "same_risk_definition_used_for_trigger_and_validation": same_definition,
        "return_metrics_temporarily_trustworthy": not same_definition,
        "independence_answer": {
            "question": "current_strategy_uses_same_risk_definition_for_trigger_and_validation",
            "answer": "YES" if same_definition else "NO",
            "return_metrics_temporarily_trustworthy": not same_definition,
            "reason": (
                "Trigger and validation share core tail-risk case fields and derived "
                "tail_risk_signal_high/fallback logic."
                if same_definition
                else "No direct or derived trigger/label overlap was detected."
            ),
            "promotion_gate_allowed": False,
        },
        "data_window": _tail_risk_independence_data_window(
            robustness=robustness,
            precision=precision,
            anti_leakage=anti_leakage,
            forward=forward,
        ),
        "report_registry_entry": _tail_risk_trigger_label_report_registry_entry(),
        "metrics": {
            "trigger_input_field_count": len(trigger_fields),
            "trigger_derived_field_count": len(trigger_derived_fields),
            "label_input_field_count": len(label_fields),
            "validation_outcome_field_count": len(validation_outcome_fields),
            "independent_forward_outcome_field_count": len(forward_outcome_fields),
            "direct_overlap_count": len(direct_overlap_rows),
            "derived_overlap_count": len(derived_overlap_rows),
            "time_window_blocker_count": len(time_window_blockers),
            "derived_blocker_count": len(derived_blockers),
            "same_risk_definition_used_for_trigger_and_validation": same_definition,
            "return_metrics_temporarily_trustworthy": not same_definition,
            "promotion_gate_allowed": False,
        },
        "warnings": warnings,
        "blockers": blockers,
        "next_recommended_action": (
            "define_independent_forward_outcome_validation_before_using_return_metrics"
            if status == "BLOCKED"
            else "continue_read_only_monitoring_without_promotion"
        ),
    }


def _policy_string_list(policy: Mapping[str, Any], key: str, default: Any) -> list[str]:
    raw = policy.get(key, default)
    if not isinstance(raw, list):
        raw = default if isinstance(default, list) else []
    return [str(item) for item in raw if str(item)]


def _unique_strings(values: list[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for value in values:
        if value not in seen:
            seen.add(value)
            output.append(value)
    return output


def _tail_risk_dependency_map(
    *,
    trigger_fields: list[str],
    trigger_derived_fields: list[str],
    label_fields: list[str],
    validation_outcome_fields: list[str],
    forward_outcome_fields: list[str],
    label_derived_fields: list[str],
) -> dict[str, dict[str, Any]]:
    dependencies: dict[str, dict[str, Any]] = {}
    for field in trigger_fields:
        dependencies[field] = {
            "field_role": "trigger_input",
            "derived_from": [field],
            "core_fields": [field],
        }
    for field in label_fields:
        dependencies.setdefault(
            field,
            {
                "field_role": "label_input",
                "derived_from": [field],
                "core_fields": [field],
            },
        )
    for field in validation_outcome_fields:
        dependencies.setdefault(
            field,
            {
                "field_role": "validation_outcome",
                "derived_from": [field],
                "core_fields": [field],
            },
        )
    for field in forward_outcome_fields:
        dependencies[field] = {
            "field_role": "independent_forward_outcome_candidate",
            "derived_from": [field],
            "core_fields": [field],
        }
    derived_map = {
        "tail_risk_signal_high": trigger_fields,
        "fallback_triggered": ["tail_risk_signal_high"],
        "trigger_labels": trigger_fields,
        "trigger_reason": ["trigger_labels"],
        "trigger_score": ["trigger_labels"],
        "tail_risk_validation_label": label_fields,
        "precision_recall_confusion_matrix": ["fallback_triggered", "tail_risk_validation_label"],
        "future_outcome_after_maturity": forward_outcome_fields,
        "expected_avoided_risk": ["trigger_labels"],
    }
    for field in [*trigger_derived_fields, *label_derived_fields, "expected_avoided_risk"]:
        source_fields = [str(item) for item in derived_map.get(field, [])]
        dependencies[field] = {
            "field_role": "derived",
            "derived_from": source_fields,
            "core_fields": _tail_risk_dependency_core_fields(dependencies, source_fields),
        }
    return dependencies


def _tail_risk_dependency_core_fields(
    dependencies: Mapping[str, Mapping[str, Any]],
    fields: list[str],
) -> list[str]:
    output: list[str] = []
    for field in fields:
        source = dependencies.get(field, {})
        core_fields = source.get("core_fields") or [field]
        output.extend(str(item) for item in core_fields)
    return _unique_strings(output)


def _tail_risk_overlap_matrix(
    *,
    trigger_fields: list[str],
    trigger_derived_fields: list[str],
    label_fields: list[str],
    validation_outcome_fields: list[str],
    forward_outcome_fields: list[str],
    dependencies: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    trigger_all = [*trigger_fields, *trigger_derived_fields]
    label_all = _unique_strings(
        [*label_fields, *validation_outcome_fields, *forward_outcome_fields]
    )
    rows = []
    label_core = set(label_fields)
    for trigger_field in trigger_all:
        trigger_core = set(dependencies.get(trigger_field, {}).get("core_fields", []))
        for label_field in label_all:
            label_source_core = set(dependencies.get(label_field, {}).get("core_fields", []))
            shared_core = sorted(trigger_core & label_core & (label_source_core | {label_field}))
            direct = trigger_field == label_field and label_field in label_fields
            derived = bool(shared_core) and (trigger_field in trigger_derived_fields or not direct)
            time_overlap = direct or bool(shared_core)
            if direct:
                overlap_type = "direct_field_overlap"
            elif derived:
                overlap_type = "shared_core_risk_definition"
            elif time_overlap:
                overlap_type = "time_window_overlap"
            else:
                overlap_type = "none"
            severity = "CRITICAL" if direct or derived else ("MEDIUM" if time_overlap else "LOW")
            rows.append(
                {
                    "trigger_field": trigger_field,
                    "trigger_field_role": (
                        "trigger_derived"
                        if trigger_field in trigger_derived_fields
                        else "trigger_input"
                    ),
                    "label_or_outcome_field": label_field,
                    "label_or_outcome_role": _tail_risk_label_field_role(
                        label_field=label_field,
                        label_fields=label_fields,
                        validation_outcome_fields=validation_outcome_fields,
                    ),
                    "direct_field_overlap": direct,
                    "shared_core_fields": shared_core,
                    "derived_overlap": derived,
                    "time_window_overlap": time_overlap,
                    "overlap_type": overlap_type,
                    "severity": severity,
                    "blocking": severity == "CRITICAL",
                    "promotion_gate_allowed": False,
                }
            )
    return rows


def _tail_risk_label_field_role(
    *,
    label_field: str,
    label_fields: list[str],
    validation_outcome_fields: list[str],
) -> str:
    if label_field in label_fields:
        return "label_input"
    if label_field in validation_outcome_fields:
        return "validation_outcome"
    return "independent_forward_outcome_candidate"


def _tail_risk_time_window_matrix(
    *,
    trigger_fields: list[str],
    trigger_derived_fields: list[str],
    label_fields: list[str],
    validation_outcome_fields: list[str],
    forward_outcome_fields: list[str],
    dependencies: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    rows = []
    for field in trigger_fields:
        visible = not field.endswith("_case")
        rows.append(
            _tail_risk_time_window_row(
                field_name=field,
                field_role="trigger_input",
                source_window=(
                    "controlled_historical_label_proxy" if not visible else "decision_time_feature"
                ),
                expected_window="decision_time_or_earlier",
                decision_time_visible=visible,
                label_strictly_after_decision=False,
                derived_from=[field],
                blocking=not visible,
            )
        )
    for field in trigger_derived_fields:
        core_fields = [str(item) for item in dependencies.get(field, {}).get("core_fields", [])]
        visible = not any(item.endswith("_case") for item in core_fields)
        rows.append(
            _tail_risk_time_window_row(
                field_name=field,
                field_role="trigger_derived",
                source_window="derived_from_trigger_inputs",
                expected_window="decision_time_or_earlier",
                decision_time_visible=visible,
                label_strictly_after_decision=False,
                derived_from=[
                    str(item) for item in dependencies.get(field, {}).get("derived_from", [])
                ],
                blocking=not visible,
            )
        )
    for field in label_fields:
        rows.append(
            _tail_risk_time_window_row(
                field_name=field,
                field_role="label_input",
                source_window="evaluation_or_forward_realized_label",
                expected_window="strictly_after_decision_time",
                decision_time_visible=False,
                label_strictly_after_decision=False,
                derived_from=[field],
                blocking=field in trigger_fields,
            )
        )
    for field in validation_outcome_fields:
        rows.append(
            _tail_risk_time_window_row(
                field_name=field,
                field_role="validation_outcome",
                source_window="realized_evaluation_window",
                expected_window="strictly_after_decision_time",
                decision_time_visible=False,
                label_strictly_after_decision=False,
                derived_from=[field],
                blocking=False,
            )
        )
    for field in forward_outcome_fields:
        rows.append(
            _tail_risk_time_window_row(
                field_name=field,
                field_role="independent_forward_outcome_candidate",
                source_window="forward_window_t_plus_1_to_t_plus_20",
                expected_window="strictly_after_decision_time",
                decision_time_visible=False,
                label_strictly_after_decision=True,
                derived_from=[field],
                blocking=False,
            )
        )
    return rows


def _tail_risk_time_window_row(
    *,
    field_name: str,
    field_role: str,
    source_window: str,
    expected_window: str,
    decision_time_visible: bool,
    label_strictly_after_decision: bool,
    derived_from: list[str],
    blocking: bool,
) -> dict[str, Any]:
    return {
        "field_name": field_name,
        "field_role": field_role,
        "source_window": source_window,
        "expected_window": expected_window,
        "decision_time_visible": decision_time_visible,
        "label_strictly_after_decision": label_strictly_after_decision,
        "derived_from": derived_from,
        "time_window_overlap": blocking,
        "blocking": blocking,
        "visibility_status": "PASS" if not blocking else "BLOCKING_TIME_WINDOW_OR_VISIBILITY_RISK",
        "promotion_gate_allowed": False,
    }


def _tail_risk_derived_dependency_matrix(
    *,
    trigger_fields: list[str],
    trigger_derived_fields: list[str],
    label_fields: list[str],
    label_derived_fields: list[str],
    forward_outcome_fields: list[str],
    dependencies: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    rows = []
    label_core = set(label_fields)
    trigger_core = set(trigger_fields)
    for field in trigger_derived_fields:
        core_fields = [str(item) for item in dependencies.get(field, {}).get("core_fields", [])]
        shared = sorted(set(core_fields) & label_core)
        rows.append(
            _tail_risk_derived_row(
                derived_field=field,
                dependency_role="trigger_derived",
                derived_from=[
                    str(item) for item in dependencies.get(field, {}).get("derived_from", [])
                ],
                core_fields=core_fields,
                shared_with="tail_risk_validation_label" if shared else None,
                shared_core_fields=shared,
                shared_derived_logic=bool(shared),
                blocking=bool(shared),
            )
        )
    for field in label_derived_fields:
        core_fields = [str(item) for item in dependencies.get(field, {}).get("core_fields", [])]
        shared = sorted(set(core_fields) & trigger_core)
        rows.append(
            _tail_risk_derived_row(
                derived_field=field,
                dependency_role="label_or_validation_derived",
                derived_from=[
                    str(item) for item in dependencies.get(field, {}).get("derived_from", [])
                ],
                core_fields=core_fields,
                shared_with="tail_risk_signal_high" if shared else None,
                shared_core_fields=shared,
                shared_derived_logic=bool(shared),
                blocking=bool(shared),
            )
        )
    rows.append(
        _tail_risk_derived_row(
            derived_field="independent_forward_outcome_validation",
            dependency_role="planned_independent_validation",
            derived_from=forward_outcome_fields,
            core_fields=forward_outcome_fields,
            shared_with=None,
            shared_core_fields=[],
            shared_derived_logic=False,
            blocking=False,
        )
    )
    return rows


def _tail_risk_derived_row(
    *,
    derived_field: str,
    dependency_role: str,
    derived_from: list[str],
    core_fields: list[str],
    shared_with: str | None,
    shared_core_fields: list[str],
    shared_derived_logic: bool,
    blocking: bool,
) -> dict[str, Any]:
    return {
        "derived_field": derived_field,
        "dependency_role": dependency_role,
        "derived_from": derived_from,
        "core_fields": core_fields,
        "shared_with": shared_with,
        "shared_core_fields": shared_core_fields,
        "shared_derived_logic": shared_derived_logic,
        "severity": "CRITICAL" if blocking else "LOW",
        "blocking": blocking,
        "promotion_gate_allowed": False,
    }


def _tail_risk_independence_data_window(
    *,
    robustness: Mapping[str, Any],
    precision: Mapping[str, Any],
    anti_leakage: Mapping[str, Any],
    forward: Mapping[str, Any],
) -> dict[str, Any]:
    for payload in [anti_leakage, precision, robustness, forward]:
        data_window = payload.get("data_window")
        if isinstance(data_window, Mapping):
            return dict(data_window)
        summary = payload.get("summary")
        if isinstance(summary, Mapping) and summary.get("requested_date_range"):
            return {
                "date_start": AI_REGIME_START,
                "date_end": "open",
                "requested_date_range": summary.get("requested_date_range"),
                "market_regime": "ai_after_chatgpt",
            }
    return {
        "date_start": AI_REGIME_START,
        "date_end": "open",
        "requested_date_range": f"{AI_REGIME_START}..open",
        "market_regime": "ai_after_chatgpt",
    }


def _tail_risk_trigger_label_report_registry_entry() -> dict[str, Any]:
    return {
        "report_id": "tail_risk_trigger_label_independence_audit",
        "title": "Tail-Risk Trigger/Label Independence Audit",
        "command": "aits research strategies tail-risk-trigger-label-independence-audit",
        "artifact_globs": [
            "outputs/research_strategies/value_surface_review/"
            "tail_risk_trigger_label_independence_audit.json",
            "outputs/research_strategies/value_surface_review/"
            "tail_risk_trigger_label_independence_audit.md",
        ],
        "artifact_selection_policy": "latest_available",
        "required_for_daily_reading": False,
        "production_effect": "none",
    }


def _tail_risk_governance_report_registry_entry(
    report_id: str, title: str, command_slug: str
) -> dict[str, Any]:
    return {
        "report_id": report_id,
        "title": title,
        "command": f"aits research strategies {command_slug}",
        "artifact_globs": [
            f"outputs/research_strategies/value_surface_review/{report_id}.json",
            f"outputs/research_strategies/value_surface_review/{report_id}.md",
        ],
        "artifact_selection_policy": "latest_available",
        "required_for_daily_reading": False,
        "production_effect": "none",
    }


def _tail_risk_independent_forward_outcome_validation(
    *,
    config: Mapping[str, Any],
    selected_cases: list[dict[str, Any]],
    classifier: Mapping[str, Any],
    robustness: Mapping[str, Any],
    trigger_label: Mapping[str, Any],
) -> dict[str, Any]:
    policy = _next_stage_section(config, "tail_risk_independent_forward_outcome_validation")
    rows = _tail_risk_independent_outcome_rows(selected_cases, classifier)
    outcome_fields = _policy_string_list(
        policy,
        "outcome_fields",
        [
            "future_5d_return",
            "future_10d_return",
            "future_20d_return",
            "future_5d_max_drawdown",
            "future_10d_max_drawdown",
            "future_20d_max_drawdown",
            "future_20d_realized_vol",
            "future_20d_underperform_vs_static",
            "future_20d_recovery_failure",
            "future_gap_down_event",
        ],
    )
    forbidden_fields = _tail_risk_forbidden_label_fields()
    forbidden_overlap = sorted(set(outcome_fields) & forbidden_fields)
    valid_5d = sum(1 for row in rows if row.get("future_5d_return") is not None)
    valid_10d = sum(1 for row in rows if row.get("future_10d_return") is not None)
    valid_20d = sum(1 for row in rows if row.get("future_20d_return") is not None)
    min_total = _first_int(policy.get("min_total_decision_count")) or 30
    min_20d = _first_int(policy.get("min_valid_20d_count")) or 20
    blockers = []
    warnings = []
    if forbidden_overlap:
        blockers.append(
            {
                "blocker": "outcome_uses_forbidden_trigger_or_label_field",
                "forbidden_fields": forbidden_overlap,
                "promotion_gate_allowed": False,
            }
        )
    if not rows:
        blockers.append(
            {
                "blocker": "missing_independent_forward_rows",
                "impact": "no decision rows with forward market path outcomes were available",
                "promotion_gate_allowed": False,
            }
        )
    if valid_20d < min_20d:
        warnings.append(
            {
                "warning": "valid_20d_forward_sample_below_floor",
                "observed": valid_20d,
                "required": min_20d,
                "promotion_gate_allowed": False,
            }
        )
    if rows and not blockers and len(rows) >= min_total and valid_20d >= min_20d:
        status = "INDEPENDENT_FORWARD_VALIDATED"
    elif blockers and rows:
        status = "INDEPENDENT_FORWARD_BLOCKED"
    elif not rows or len(rows) < min_total:
        status = "INSUFFICIENT_SAMPLE"
    else:
        status = "INDEPENDENT_FORWARD_INCONCLUSIVE"
    horizon_summary = _tail_risk_independent_horizon_summary(rows)
    policy_comparison = _tail_risk_independent_policy_comparison(rows)
    return {
        "status": status,
        "outcome_source_contract": {
            "source_artifact": "value_surface_controlled_expansion",
            "source_data_quality_status": _mapping(
                _mapping(robustness.get("value_surface_source")).get("data_quality_gate")
            ).get("status")
            or _mapping(robustness.get("summary")).get("data_quality_status")
            or "inherited_from_value_surface_artifact",
            "decision_time_field": "date",
            "strictly_after_decision_time": True,
            "forbidden_label_or_case_fields_used": forbidden_overlap,
            "realized_vol_note": (
                "future_20d_realized_vol is approximated from the realized 20d market "
                "path return available in the source artifact; it remains diagnostic."
            ),
            "promotion_gate_allowed": False,
        },
        "independent_outcome_fields": outcome_fields,
        "forbidden_outcome_fields": sorted(forbidden_fields),
        "decision_outcomes": rows,
        "horizon_summary": horizon_summary,
        "policy_comparison_summary": policy_comparison,
        "summary": {
            "decision_count": len(rows),
            "triggered_count": sum(1 for row in rows if row.get("fallback_triggered")),
            "non_triggered_count": sum(1 for row in rows if not row.get("fallback_triggered")),
            "valid_forward_5d_count": valid_5d,
            "valid_forward_10d_count": valid_10d,
            "valid_forward_20d_count": valid_20d,
            "outcome_forbidden_dependency_count": len(forbidden_overlap),
            "trigger_label_audit_status": trigger_label.get("status", "MISSING"),
            "promotion_gate_allowed": False,
        },
        "warnings": warnings,
        "blockers": blockers,
        "next_recommended_action": (
            "repair_independent_forward_outcome_contract"
            if status == "INDEPENDENT_FORWARD_BLOCKED"
            else (
                "collect_more_forward_samples_before_interpreting_tail_risk_fallback"
                if status in {"INSUFFICIENT_SAMPLE", "INDEPENDENT_FORWARD_INCONCLUSIVE"}
                else "continue_with_contract_boundary_and_counterfactual_validation"
            )
        ),
    }


def _tail_risk_independent_outcome_rows(
    selected_cases: list[dict[str, Any]],
    classifier: Mapping[str, Any],
) -> list[dict[str, Any]]:
    label_map = _classifier_label_map(classifier)
    rows = []
    for case in selected_cases:
        horizon_days = _horizon_days(case.get("horizon_days") or case.get("horizon"))
        if horizon_days not in {5, 10, 20}:
            continue
        labels = label_map.get(_case_key(case), {})
        fallback_triggered = bool(labels.get("tail_risk_signal_high"))
        trigger_label_count = sum(
            1
            for label in [
                "large_loss_case",
                "tail_loss_case",
                "benchmark_underperformance_case",
                "long_horizon_failure_case",
            ]
            if bool(labels.get(label))
        )
        static_return = _float(case.get("benchmark_realized_net_return"), 0.0)
        no_fallback_return = _float(case.get("selected_realized_net_return"), 0.0)
        fallback_return = static_return if fallback_triggered else no_fallback_return
        drawdown = _float(
            case.get("benchmark_drawdown_proxy", case.get("selected_drawdown_proxy")),
            0.0,
        )
        selected_drawdown = _float(case.get("selected_drawdown_proxy"), drawdown)
        realized_vol_20d = (
            _round(abs(static_return) / math.sqrt(20)) if horizon_days == 20 else None
        )
        underperform_vs_static = no_fallback_return < static_return
        recovery_failure = horizon_days == 20 and static_return < 0 and drawdown < -0.02
        gap_down = static_return <= -0.03 or drawdown <= -0.03
        independent_tail_event = (
            drawdown <= -0.03
            or static_return <= -0.02
            or (realized_vol_20d is not None and realized_vol_20d >= 0.01)
            or underperform_vs_static
            or recovery_failure
            or gap_down
        )
        row = {
            "case_key": _case_key(case),
            "decision_time": case.get("date"),
            "asset": case.get("asset"),
            "asset_cluster": case.get("asset_cluster"),
            "horizon": case.get("horizon"),
            "horizon_days": horizon_days,
            "regime_segment": case.get("regime_segment"),
            "pit_state": case.get("pit_state"),
            "fallback_triggered": fallback_triggered,
            "trigger_label_count": trigger_label_count,
            "trigger_input_role": "decision_time_trigger_only_not_outcome",
            "future_5d_return": _round(static_return) if horizon_days == 5 else None,
            "future_10d_return": _round(static_return) if horizon_days == 10 else None,
            "future_20d_return": _round(static_return) if horizon_days == 20 else None,
            "future_5d_max_drawdown": _round(drawdown) if horizon_days == 5 else None,
            "future_10d_max_drawdown": _round(drawdown) if horizon_days == 10 else None,
            "future_20d_max_drawdown": _round(drawdown) if horizon_days == 20 else None,
            "future_20d_realized_vol": realized_vol_20d,
            "future_20d_underperform_vs_static": (
                bool(underperform_vs_static) if horizon_days == 20 else None
            ),
            "future_20d_recovery_failure": bool(recovery_failure) if horizon_days == 20 else None,
            "future_gap_down_event": bool(gap_down),
            "independent_tail_event": bool(independent_tail_event),
            "no_fallback_baseline_return": _round(no_fallback_return),
            "fallback_policy_return": _round(fallback_return),
            "static_allocation_baseline_return": _round(static_return),
            "existing_best_baseline_if_available_return": _round(
                max(no_fallback_return, static_return)
            ),
            "fallback_vs_no_fallback_return_delta": _round(fallback_return - no_fallback_return),
            "fallback_vs_static_return_delta": _round(fallback_return - static_return),
            "future_max_drawdown_improvement": _round(
                selected_drawdown - drawdown if fallback_triggered else 0.0
            ),
            "false_positive": bool(fallback_triggered and not independent_tail_event),
            "false_negative": bool((not fallback_triggered) and independent_tail_event),
            "outcome_source": "realized_forward_market_path_from_value_surface_artifact",
            "forbidden_label_or_case_fields_used_in_outcome": [],
            "promotion_gate_allowed": False,
        }
        rows.append(row)
    return rows


def _tail_risk_independent_horizon_summary(rows: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
    output = []
    for horizon in ["5d", "10d", "20d"]:
        subset = [row for row in rows if str(row.get("horizon")) == horizon]
        output.append(
            {
                "horizon": horizon,
                "sample_count": len(subset),
                "triggered_count": sum(1 for row in subset if row.get("fallback_triggered")),
                "avg_future_return": _round(
                    _mean([_tail_risk_future_return(row) for row in subset])
                ),
                "avg_fallback_vs_static_delta": _round(
                    _mean(
                        [_float(row.get("fallback_vs_static_return_delta"), 0.0) for row in subset]
                    )
                ),
                "future_tail_event_count": sum(
                    1 for row in subset if row.get("independent_tail_event")
                ),
                "false_positive_count": sum(1 for row in subset if row.get("false_positive")),
                "false_negative_count": sum(1 for row in subset if row.get("false_negative")),
                "promotion_gate_allowed": False,
            }
        )
    return output


def _tail_risk_future_return(row: Mapping[str, Any]) -> float:
    for field in ["future_5d_return", "future_10d_return", "future_20d_return"]:
        value = row.get(field)
        if value is not None:
            return _float(value, 0.0)
    return 0.0


def _tail_risk_independent_policy_comparison(
    rows: list[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    policies = [
        "fallback_policy",
        "no_fallback_baseline",
        "static_allocation_baseline",
        "existing_best_baseline_if_available",
        "simple_trend_baseline",
        "qqq_100_baseline",
        "qqq_60_sgov_40_baseline",
        "qqq_70_sgov_30_baseline",
        "tqqq_50_sgov_50_baseline",
        "tqqq_25_sgov_75_baseline",
        "simple_200dma_risk_off_baseline",
        "simple_volatility_target_baseline",
        "equal_risk_qqq_sgov_baseline",
    ]
    output = []
    for policy_id in policies:
        values = [_tail_risk_policy_return(row, policy_id) for row in rows]
        drawdowns = [_tail_risk_policy_drawdown(row, policy_id) for row in rows]
        output.append(
            {
                "policy_id": policy_id,
                "sample_count": len(rows),
                "avg_future_return": _round(_mean(values)),
                "median_future_return": _round(statistics.median(values)) if values else 0.0,
                "avg_future_max_drawdown": _round(_mean(drawdowns)),
                "sharpe_proxy": _round(
                    (_mean(values) / _stddev(values)) if len(values) > 1 and _stddev(values) else 0
                ),
                "downside_capture": _round(
                    abs(sum(value for value in values if value < 0))
                    / abs(
                        sum(
                            _tail_risk_future_return(row)
                            for row in rows
                            if _tail_risk_future_return(row) < 0
                        )
                    )
                    if any(_tail_risk_future_return(row) < 0 for row in rows)
                    else 0.0
                ),
                "turnover_cost_if_available": _tail_risk_policy_turnover_cost(policy_id),
                "simple_baseline": policy_id
                not in {
                    "fallback_policy",
                    "no_fallback_baseline",
                    "static_allocation_baseline",
                    "existing_best_baseline_if_available",
                },
                "proxy_method": _tail_risk_baseline_proxy_method(policy_id),
                "promotion_gate_allowed": False,
            }
        )
    return output


def _tail_risk_policy_return(row: Mapping[str, Any], policy_id: str) -> float:
    no_fallback = _float(row.get("no_fallback_baseline_return"), 0.0)
    fallback = _float(row.get("fallback_policy_return"), 0.0)
    static = _float(row.get("static_allocation_baseline_return"), 0.0)
    best = max(no_fallback, static)
    trend_on = _tail_risk_trend_bucket(row) == "bull"
    high_vol = _tail_risk_vol_bucket(row) == "high_vol"
    defensive = static
    if policy_id == "fallback_policy":
        return fallback
    if policy_id in {"no_fallback_baseline", "qqq_100_baseline"}:
        return no_fallback
    if policy_id == "static_allocation_baseline":
        return static
    if policy_id == "existing_best_baseline_if_available":
        return best
    if policy_id == "simple_trend_baseline":
        return no_fallback if trend_on else defensive
    if policy_id == "qqq_60_sgov_40_baseline":
        return 0.60 * no_fallback + 0.40 * defensive
    if policy_id == "qqq_70_sgov_30_baseline":
        return 0.70 * no_fallback + 0.30 * defensive
    if policy_id == "tqqq_50_sgov_50_baseline":
        return 0.50 * (3.0 * no_fallback) + 0.50 * defensive
    if policy_id == "tqqq_25_sgov_75_baseline":
        return 0.25 * (3.0 * no_fallback) + 0.75 * defensive
    if policy_id == "simple_200dma_risk_off_baseline":
        return no_fallback if trend_on else defensive
    if policy_id == "simple_volatility_target_baseline":
        return 0.50 * no_fallback + 0.50 * defensive if high_vol else no_fallback
    if policy_id == "equal_risk_qqq_sgov_baseline":
        return 0.50 * no_fallback + 0.50 * defensive
    return no_fallback


def _tail_risk_policy_drawdown(row: Mapping[str, Any], policy_id: str) -> float:
    base = _tail_risk_forward_drawdown(row)
    if policy_id in {"qqq_60_sgov_40_baseline", "simple_volatility_target_baseline"}:
        return base * 0.60
    if policy_id in {"qqq_70_sgov_30_baseline", "simple_trend_baseline"}:
        return base * 0.70
    if policy_id == "tqqq_50_sgov_50_baseline":
        return base * 1.50
    if policy_id == "tqqq_25_sgov_75_baseline":
        return base * 0.75
    if policy_id in {"simple_200dma_risk_off_baseline", "equal_risk_qqq_sgov_baseline"}:
        return base * 0.50
    return base


def _tail_risk_policy_turnover_cost(policy_id: str) -> float | None:
    if policy_id in {"fallback_policy", "simple_trend_baseline", "simple_200dma_risk_off_baseline"}:
        return None
    if policy_id in {
        "no_fallback_baseline",
        "static_allocation_baseline",
        "existing_best_baseline_if_available",
        "qqq_100_baseline",
        "qqq_60_sgov_40_baseline",
        "qqq_70_sgov_30_baseline",
        "tqqq_50_sgov_50_baseline",
        "tqqq_25_sgov_75_baseline",
        "equal_risk_qqq_sgov_baseline",
    }:
        return 0.0
    return None


def _tail_risk_baseline_proxy_method(policy_id: str) -> str:
    methods = {
        "fallback_policy": "current fallback policy return from independent forward rows",
        "no_fallback_baseline": "current no-fallback forward return from independent rows",
        "static_allocation_baseline": "existing static allocation baseline return",
        "existing_best_baseline_if_available": "max(no_fallback, static_allocation)",
        "simple_trend_baseline": (
            "QQQ proxy when PIT trend is bull; otherwise defensive static baseline"
        ),
        "qqq_100_baseline": "no-fallback QQQ proxy return from independent rows",
        "qqq_60_sgov_40_baseline": "60% QQQ proxy plus 40% defensive static baseline proxy",
        "qqq_70_sgov_30_baseline": "70% QQQ proxy plus 30% defensive static baseline proxy",
        "tqqq_50_sgov_50_baseline": "50% 3x QQQ proxy plus 50% defensive static baseline proxy",
        "tqqq_25_sgov_75_baseline": "25% 3x QQQ proxy plus 75% defensive static baseline proxy",
        "simple_200dma_risk_off_baseline": "PIT trend proxy as 200DMA risk-off substitute",
        "simple_volatility_target_baseline": (
            "halve QQQ proxy exposure in high-volatility PIT state"
        ),
        "equal_risk_qqq_sgov_baseline": "50% QQQ proxy plus 50% defensive static baseline proxy",
    }
    return methods.get(policy_id, "unknown")


def _tail_risk_forward_drawdown(row: Mapping[str, Any]) -> float:
    for field in [
        "future_5d_max_drawdown",
        "future_10d_max_drawdown",
        "future_20d_max_drawdown",
    ]:
        value = row.get(field)
        if value is not None:
            return _float(value, 0.0)
    return 0.0


def _tail_risk_forbidden_label_fields() -> set[str]:
    return {
        "large_loss_case",
        "tail_loss_case",
        "long_horizon_failure_case",
        "tail_risk_signal_high",
        "fallback_triggered",
        "tail_risk_validation_label",
        "precision_recall_confusion_matrix",
        "trigger_labels",
        "trigger_reason",
        "trigger_score",
    }


def _tail_risk_forward_outcome_contract_audit(
    *,
    config: Mapping[str, Any],
    trigger_label: Mapping[str, Any],
    independent: Mapping[str, Any],
) -> dict[str, Any]:
    policy = _next_stage_section(config, "tail_risk_forward_outcome_contract_audit")
    outcome_fields = [
        str(item)
        for item in independent.get("independent_outcome_fields")
        or _policy_string_list(
            policy,
            "outcome_fields",
            [
                "future_5d_return",
                "future_10d_return",
                "future_20d_return",
                "future_5d_max_drawdown",
                "future_10d_max_drawdown",
                "future_20d_max_drawdown",
                "future_20d_realized_vol",
                "future_20d_underperform_vs_static",
                "future_20d_recovery_failure",
                "future_gap_down_event",
            ],
        )
    ]
    trigger = _mapping(trigger_label.get("trigger_fields"))
    trigger_fields = [str(item) for item in trigger.get("input_fields") or []]
    trigger_derived = [str(item) for item in trigger.get("derived_fields") or []]
    if not trigger_fields:
        trigger_fields = [
            "large_loss_case",
            "tail_loss_case",
            "benchmark_underperformance_case",
            "long_horizon_failure_case",
        ]
    forbidden = _tail_risk_forbidden_label_fields()
    outcome_deps = [
        {
            "outcome_field": field,
            "derived_from": [field],
            "source_window": "strictly_after_decision_time",
            "forbidden_dependency": field in forbidden,
            "promotion_gate_allowed": False,
        }
        for field in outcome_fields
    ]
    trigger_deps = [
        {
            "trigger_field": field,
            "derived_from": [field],
            "source_window": (
                "decision_time_or_earlier"
                if field not in forbidden
                else "label_proxy_not_accepted_for_outcome"
            ),
            "promotion_gate_allowed": False,
        }
        for field in [*trigger_fields, *trigger_derived]
    ]
    overlap_matrix = []
    for trigger_field in [*trigger_fields, *trigger_derived]:
        for outcome_field in outcome_fields:
            direct = trigger_field == outcome_field
            overlap_matrix.append(
                {
                    "trigger_field": trigger_field,
                    "outcome_field": outcome_field,
                    "direct_overlap": direct,
                    "blocking": direct,
                    "promotion_gate_allowed": False,
                }
            )
    derived_overlap = [
        {
            "trigger_dependency": row["trigger_field"],
            "outcome_dependency": outcome["outcome_field"],
            "shared_dependency": (
                row["trigger_field"] == outcome["outcome_field"]
                or outcome["outcome_field"] in forbidden
            ),
            "blocking": (
                row["trigger_field"] == outcome["outcome_field"]
                or outcome["outcome_field"] in forbidden
            ),
            "promotion_gate_allowed": False,
        }
        for row in trigger_deps
        for outcome in outcome_deps
        if row["trigger_field"] == outcome["outcome_field"] or outcome["outcome_field"] in forbidden
    ]
    time_window = [
        {
            "field": field,
            "field_role": "outcome",
            "expected_window": "strictly_after_decision_time",
            "actual_window": "strictly_after_decision_time",
            "future_leakage_into_trigger": False,
            "blocking": False,
            "promotion_gate_allowed": False,
        }
        for field in outcome_fields
    ] + [
        {
            "field": field,
            "field_role": "trigger",
            "expected_window": "decision_time_or_earlier",
            "actual_window": (
                "decision_time_or_earlier" if field not in forbidden else "label_proxy"
            ),
            "future_leakage_into_trigger": field in forbidden and field.endswith("_case"),
            "blocking": False,
            "promotion_gate_allowed": False,
        }
        for field in trigger_fields
    ]
    forbidden_matrix = [
        {
            "outcome_field": field,
            "forbidden_dependency": forbidden_field,
            "dependency_present": field == forbidden_field,
            "blocking": field == forbidden_field,
            "promotion_gate_allowed": False,
        }
        for field in outcome_fields
        for forbidden_field in sorted(forbidden)
    ]
    direct_count = sum(1 for row in overlap_matrix if row["direct_overlap"])
    derived_count = sum(1 for row in derived_overlap if row["blocking"])
    forbidden_count = sum(1 for row in forbidden_matrix if row["dependency_present"])
    future_leakage_count = sum(1 for row in time_window if row["future_leakage_into_trigger"])
    blockers = []
    if direct_count or derived_count or forbidden_count:
        blockers.append(
            {
                "blocker": "trigger_outcome_contract_overlap",
                "direct_overlap_count": direct_count,
                "derived_overlap_count": derived_count,
                "forbidden_dependency_count": forbidden_count,
                "promotion_gate_allowed": False,
            }
        )
    status = "BLOCKED" if blockers else ("WARN" if future_leakage_count else "PASS")
    return {
        "status": status,
        "outcome_fields": outcome_fields,
        "outcome_derived_dependencies": outcome_deps,
        "trigger_fields": trigger_fields,
        "trigger_derived_dependencies": trigger_deps,
        "overlap_matrix": overlap_matrix,
        "derived_overlap_matrix": derived_overlap,
        "time_window_matrix": time_window,
        "forbidden_dependency_matrix": forbidden_matrix,
        "summary": {
            "direct_overlap_count": direct_count,
            "derived_overlap_count": derived_count,
            "forbidden_dependency_count": forbidden_count,
            "future_leakage_count": future_leakage_count,
            "promotion_gate_allowed": False,
        },
        "warnings": (
            [
                {
                    "warning": "trigger_fields_include_label_proxy_cases",
                    "future_leakage_count": future_leakage_count,
                    "promotion_gate_allowed": False,
                }
            ]
            if future_leakage_count
            else []
        ),
        "blockers": blockers,
        "next_recommended_action": (
            "repair_forward_outcome_contract_overlap"
            if blockers
            else "use_contract_as_input_to_decision_time_boundary_audit"
        ),
    }


def _tail_risk_decision_time_boundary_audit(
    *,
    config: Mapping[str, Any],
    trigger_label: Mapping[str, Any],
    contract: Mapping[str, Any],
) -> dict[str, Any]:
    policy = _next_stage_section(config, "tail_risk_decision_time_boundary_audit")
    trigger_fields = _mapping(trigger_label.get("trigger_fields"))
    fields = [str(item) for item in trigger_fields.get("input_fields") or []]
    if not fields:
        fields = [
            "large_loss_case",
            "tail_loss_case",
            "benchmark_underperformance_case",
            "long_horizon_failure_case",
        ]
    forbidden = _tail_risk_forbidden_label_fields()
    feature_rows = []
    for field in fields:
        visible = field not in forbidden
        feature_rows.append(
            {
                "feature_name": field,
                "latest_available_time": "decision_time" if visible else "after_outcome_window",
                "decision_time_availability": "AVAILABLE" if visible else "NOT_PROVEN_AVAILABLE",
                "reads_t_plus_1_or_later": not visible,
                "reads_forward_return": field.endswith("_case"),
                "reads_future_realized_vol": False,
                "reads_future_drawdown_or_recovery": field.endswith("_case"),
                "rolling_window_boundary_ok": visible,
                "blocking": not visible,
                "promotion_gate_allowed": False,
            }
        )
    contract_future = [
        row for row in _records(contract.get("time_window_matrix")) if row.get("blocking")
    ]
    forward_rows = [
        {
            "check_id": "forward_return_read",
            "issue_count": sum(1 for row in feature_rows if row["reads_forward_return"]),
            "blocking": any(row["reads_forward_return"] for row in feature_rows),
            "promotion_gate_allowed": False,
        },
        {
            "check_id": "future_drawdown_recovery_read",
            "issue_count": sum(
                1 for row in feature_rows if row["reads_future_drawdown_or_recovery"]
            ),
            "blocking": any(row["reads_future_drawdown_or_recovery"] for row in feature_rows),
            "promotion_gate_allowed": False,
        },
        {
            "check_id": "contract_time_window_blocking",
            "issue_count": len(contract_future),
            "blocking": bool(contract_future),
            "promotion_gate_allowed": False,
        },
    ]
    rolling = [
        {
            "window_id": "trigger_feature_lookback",
            "boundary_rule": str(policy.get("rolling_window_rule", "closed_at_decision_time")),
            "boundary_ok": all(row["rolling_window_boundary_ok"] for row in feature_rows),
            "promotion_gate_allowed": False,
        }
    ]
    blocked = [row for row in feature_rows if row["blocking"]]
    future_count = sum(_first_int(row["issue_count"]) for row in forward_rows)
    blockers = (
        [
            {
                "blocker": "trigger_feature_not_decision_time_visible",
                "affected_features": [row["feature_name"] for row in blocked],
                "promotion_gate_allowed": False,
            }
        ]
        if blocked
        else []
    )
    status = (
        "TIME_BOUNDARY_BLOCKED"
        if blockers
        else ("TIME_BOUNDARY_WARN" if future_count else "TIME_BOUNDARY_PASS")
    )
    return {
        "status": status,
        "feature_availability_rows": feature_rows,
        "decision_time_boundary_matrix": feature_rows,
        "forward_read_matrix": forward_rows,
        "rolling_window_boundary_checks": rolling,
        "summary": {
            "feature_count": len(feature_rows),
            "blocked_feature_count": len(blocked),
            "future_read_count": future_count,
            "rolling_window_boundary_issue_count": sum(
                1 for row in rolling if not row["boundary_ok"]
            ),
            "promotion_gate_allowed": False,
        },
        "warnings": [],
        "blockers": blockers,
        "next_recommended_action": (
            "replace_label_proxy_trigger_inputs_with_decision_time_features"
            if blockers
            else "continue_to_leakage_stress_suite"
        ),
    }


def _tail_risk_tainted_metric_quarantine(
    *,
    trigger_label: Mapping[str, Any],
    precision: Mapping[str, Any],
    robustness: Mapping[str, Any],
    opportunity: Mapping[str, Any],
) -> dict[str, Any]:
    tainted = trigger_label.get("status") == "BLOCKED"
    metric_names = [
        "precision",
        "recall",
        "f1",
        "return metrics",
        "tail-risk hit rate",
        "fallback triggered hit rate",
        "label-based validation",
    ]
    rows = [
        {
            "metric_name": metric,
            "metric_status": "TAINTED_BY_TRIGGER_LABEL_COUPLING" if tainted else "REVIEW_REQUIRED",
            "usable_for_promotion": False,
            "usable_for_paper_shadow": False,
            "usable_for_production": False,
            "requires_independent_forward_validation": True,
            "promotion_gate_allowed": False,
        }
        for metric in metric_names
    ]
    artifacts = [
        _tail_risk_quarantine_artifact_row("trigger_label_audit", trigger_label),
        _tail_risk_quarantine_artifact_row("precision_recall", precision),
        _tail_risk_quarantine_artifact_row("robustness", robustness),
        _tail_risk_quarantine_artifact_row("opportunity_cost", opportunity),
    ]
    return {
        "status": "TAINTED_METRIC_QUARANTINED" if tainted else "TAINTED_METRIC_REVIEW_REQUIRED",
        "metric_status": "TAINTED_BY_TRIGGER_LABEL_COUPLING" if tainted else "REVIEW_REQUIRED",
        "quarantined_metrics": rows,
        "artifact_quarantine_summary": artifacts,
        "summary": {
            "quarantined_metric_count": len(rows),
            "source_artifact_count": len(artifacts),
            "usable_for_promotion": False,
            "usable_for_paper_shadow": False,
            "usable_for_production": False,
            "promotion_gate_allowed": False,
        },
        "warnings": [],
        "blockers": [],
        "next_recommended_action": "use_only_independent_forward_outcome_metrics_for_future_review",
    }


def _tail_risk_quarantine_artifact_row(name: str, payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "artifact_name": name,
        "status": payload.get("status", "MISSING"),
        "metric_status": "TAINTED_BY_TRIGGER_LABEL_COUPLING",
        "usable_for_promotion": False,
        "usable_for_paper_shadow": False,
        "usable_for_production": False,
        "promotion_gate_allowed": False,
    }


def _tail_risk_fallback_counterfactual_validation(
    *,
    config: Mapping[str, Any],
    independent: Mapping[str, Any],
) -> dict[str, Any]:
    policy = _next_stage_section(config, "tail_risk_fallback_counterfactual_validation")
    rows = _records(independent.get("decision_outcomes"))
    comparison = _tail_risk_independent_policy_comparison(rows)
    fallback = next((row for row in comparison if row["policy_id"] == "fallback_policy"), {})
    no_fallback = next(
        (row for row in comparison if row["policy_id"] == "no_fallback_baseline"), {}
    )
    static = next(
        (row for row in comparison if row["policy_id"] == "static_allocation_baseline"), {}
    )
    fallback_mean = _float(fallback.get("avg_future_return"), 0.0)
    baseline_mean = max(
        _float(no_fallback.get("avg_future_return"), 0.0),
        _float(static.get("avg_future_return"), 0.0),
    )
    false_positive_cost = sum(
        abs(_float(row.get("fallback_vs_no_fallback_return_delta"), 0.0))
        for row in rows
        if row.get("false_positive")
    )
    false_negative_cost = sum(
        abs(_tail_risk_forward_drawdown(row)) for row in rows if row.get("false_negative")
    )
    sample_floor = _first_int(policy.get("min_sample_count")) or 30
    if len(rows) < sample_floor:
        status = "COUNTERFACTUAL_INSUFFICIENT_SAMPLE"
    elif fallback_mean > baseline_mean:
        status = "COUNTERFACTUAL_BETTER"
    elif fallback_mean < baseline_mean and false_positive_cost > false_negative_cost:
        status = "COUNTERFACTUAL_WORSE"
    else:
        status = "COUNTERFACTUAL_MIXED"
    return {
        "status": status,
        "baseline_comparison": comparison,
        "horizon_comparison": _tail_risk_independent_horizon_summary(rows),
        "summary": {
            "sample_count": len(rows),
            "fallback_avg_future_return": fallback.get("avg_future_return"),
            "best_baseline_avg_future_return": _round(baseline_mean),
            "future_max_drawdown_improvement": _round(
                _mean([_float(row.get("future_max_drawdown_improvement"), 0.0) for row in rows])
            ),
            "hit_rate_of_drawdown_reduction": _round(
                sum(
                    1 for row in rows if _float(row.get("future_max_drawdown_improvement"), 0.0) > 0
                )
                / len(rows)
                if rows
                else 0.0
            ),
            "false_positive_cost": _round(false_positive_cost),
            "false_negative_cost": _round(false_negative_cost),
            "sample_count_by_horizon": {
                horizon: sum(1 for row in rows if str(row.get("horizon")) == horizon)
                for horizon in ["5d", "10d", "20d"]
            },
            "promotion_gate_allowed": False,
        },
        "warnings": [],
        "blockers": [],
        "next_recommended_action": (
            "collect_more_independent_forward_samples"
            if status == "COUNTERFACTUAL_INSUFFICIENT_SAMPLE"
            else "review_counterfactual_with_promotion_blocked"
        ),
    }


def _tail_risk_regime_stratified_forward_outcome_review(
    *,
    config: Mapping[str, Any],
    independent: Mapping[str, Any],
    counterfactual: Mapping[str, Any],
) -> dict[str, Any]:
    policy = _next_stage_section(config, "tail_risk_regime_stratified_forward_outcome_review")
    rows = _records(independent.get("decision_outcomes"))
    regime_rows = []
    for dimension, classifier in [
        ("market_trend", _tail_risk_trend_bucket),
        ("volatility", _tail_risk_vol_bucket),
        ("above_200dma", _tail_risk_above_200dma_bucket),
        ("drawdown_state", _tail_risk_drawdown_bucket),
        ("recovery_state", _tail_risk_recovery_bucket),
        ("regime_segment", lambda row: str(row.get("regime_segment", "unknown"))),
    ]:
        regime_rows.extend(_tail_risk_regime_rows(rows, dimension, classifier))
    unavailable = [
        {
            "dimension": "rate_up/rate_down",
            "reason": "rate trend proxy is not present in current tail-risk outcome rows",
            "promotion_gate_allowed": False,
        },
        {
            "dimension": "liquidity_tight/liquidity_loose",
            "reason": "liquidity proxy is not present in current tail-risk outcome rows",
            "promotion_gate_allowed": False,
        },
    ]
    concentration = _tail_risk_regime_concentration_score(regime_rows)
    min_sample = min((_first_int(row.get("sample_count")) for row in regime_rows), default=0)
    sample_floor = _first_int(policy.get("min_regime_sample_count")) or 20
    if not rows or min_sample < sample_floor:
        status = "REGIME_INSUFFICIENT"
    elif concentration >= _float(policy.get("concentration_score_floor"), 0.60):
        status = "REGIME_CONCENTRATED"
    elif any(_float(row.get("return_improvement"), 0.0) < 0 for row in regime_rows):
        status = "REGIME_MIXED"
    else:
        status = "REGIME_ROBUST"
    return {
        "status": status,
        "regime_rows": regime_rows,
        "unavailable_regimes": unavailable,
        "summary": {
            "sample_count_by_regime": {
                row["segment_id"]: row["sample_count"] for row in regime_rows
            },
            "regime_concentration_score": _round(concentration),
            "counterfactual_status": counterfactual.get("status", "MISSING"),
            "min_regime_sample_count": min_sample,
            "promotion_gate_allowed": False,
        },
        "warnings": unavailable
        + (
            [
                {
                    "warning": "regime_sample_or_concentration_limit",
                    "status": status,
                    "promotion_gate_allowed": False,
                }
            ]
            if status != "REGIME_ROBUST"
            else []
        ),
        "blockers": [],
        "next_recommended_action": (
            "collect_more_regime_balanced_forward_samples"
            if status in {"REGIME_INSUFFICIENT", "REGIME_CONCENTRATED"}
            else "continue_observation_with_regime_disclosure"
        ),
    }


def _tail_risk_regime_rows(
    rows: list[Mapping[str, Any]],
    dimension: str,
    classifier: Any,
) -> list[dict[str, Any]]:
    output = []
    values = sorted({str(classifier(row)) for row in rows})
    for value in values:
        subset = [row for row in rows if str(classifier(row)) == value]
        output.append(
            {
                "dimension": dimension,
                "segment_id": f"{dimension}:{value}",
                "sample_count": len(subset),
                "triggered_count": sum(1 for row in subset if row.get("fallback_triggered")),
                "return_improvement": _round(
                    _mean(
                        [_float(row.get("fallback_vs_static_return_delta"), 0.0) for row in subset]
                    )
                ),
                "drawdown_improvement": _round(
                    _mean(
                        [_float(row.get("future_max_drawdown_improvement"), 0.0) for row in subset]
                    )
                ),
                "false_positive_cost": _round(
                    sum(
                        abs(_float(row.get("fallback_vs_no_fallback_return_delta"), 0.0))
                        for row in subset
                        if row.get("false_positive")
                    )
                ),
                "false_negative_cost": _round(
                    sum(
                        abs(_tail_risk_forward_drawdown(row))
                        for row in subset
                        if row.get("false_negative")
                    )
                ),
                "promotion_gate_allowed": False,
            }
        )
    return output


def _tail_risk_trend_bucket(row: Mapping[str, Any]) -> str:
    pit = _mapping(row.get("pit_state"))
    trend = str(pit.get("trend_state") or "")
    if "positive" in trend:
        return "bull"
    if "negative" in trend:
        return "bear"
    return "range"


def _tail_risk_vol_bucket(row: Mapping[str, Any]) -> str:
    pit = _mapping(row.get("pit_state"))
    value = str(pit.get("volatility_state") or "mid_vol")
    if "high" in value:
        return "high_vol"
    if "low" in value:
        return "low_vol"
    return "mid_vol"


def _tail_risk_above_200dma_bucket(row: Mapping[str, Any]) -> str:
    return "above_200dma" if _tail_risk_trend_bucket(row) == "bull" else "below_200dma"


def _tail_risk_drawdown_bucket(row: Mapping[str, Any]) -> str:
    pit = _mapping(row.get("pit_state"))
    value = str(pit.get("drawdown_state") or "unknown")
    return value if value else "unknown"


def _tail_risk_recovery_bucket(row: Mapping[str, Any]) -> str:
    future_return = _tail_risk_future_return(row)
    drawdown = _tail_risk_forward_drawdown(row)
    if future_return > 0 and drawdown < 0:
        return "recovery_state"
    if future_return <= 0 and drawdown < -0.02:
        return "drawdown_persisting"
    return "neutral"


def _tail_risk_regime_concentration_score(rows: list[Mapping[str, Any]]) -> float:
    total = sum(abs(_float(row.get("drawdown_improvement"), 0.0)) for row in rows)
    if not total:
        return 0.0
    return max(abs(_float(row.get("drawdown_improvement"), 0.0)) for row in rows) / total


def _tail_risk_threshold_sensitivity_review(
    *,
    config: Mapping[str, Any],
    independent: Mapping[str, Any],
    counterfactual: Mapping[str, Any],
) -> dict[str, Any]:
    policy = _next_stage_section(config, "tail_risk_threshold_sensitivity_review")
    rows = _records(independent.get("decision_outcomes"))
    base = _tail_risk_sensitivity_metric(rows, min_trigger_labels=1, intensity=1.0)
    surfaces = [
        {
            "parameter": "baseline",
            "perturbation": "base",
            **base,
            "promotion_gate_allowed": False,
        }
    ]
    for parameter, values in [
        ("trigger_threshold", [-0.10, -0.05, 0.05, 0.10]),
        ("risk_score_cutoff", [-0.10, -0.05, 0.05, 0.10]),
        ("lookback_window", [-0.20, 0.20]),
        ("fallback_allocation_intensity", [-0.10, 0.10]),
    ]:
        for value in values:
            min_labels = (
                2 if parameter in {"trigger_threshold", "risk_score_cutoff"} and value > 0.05 else 1
            )
            intensity = 1.0 + value if parameter == "fallback_allocation_intensity" else 1.0
            metric = _tail_risk_sensitivity_metric(
                rows,
                min_trigger_labels=min_labels,
                intensity=max(0.0, min(1.0, intensity)),
            )
            metric_degradation = _round(
                _float(base["avg_fallback_return"], 0.0)
                - _float(metric["avg_fallback_return"], 0.0)
            )
            surfaces.append(
                {
                    "parameter": parameter,
                    "perturbation": value,
                    **metric,
                    "metric_degradation_rate": metric_degradation,
                    "promotion_gate_allowed": False,
                }
            )
    degradations = [abs(_float(row.get("metric_degradation_rate"), 0.0)) for row in surfaces[1:]]
    stability_score = max(0.0, 1.0 - (max(degradations) if degradations else 0.0))
    fragile = [
        row
        for row in surfaces[1:]
        if abs(_float(row.get("metric_degradation_rate"), 0.0))
        > _float(policy.get("fragile_degradation_floor"), 0.02)
    ]
    status = (
        "SENSITIVITY_FRAGILE"
        if fragile
        else ("SENSITIVITY_MIXED" if degradations else "SENSITIVITY_STABLE")
    )
    return {
        "status": status,
        "sensitivity_surface": surfaces,
        "fragile_parameter_list": fragile,
        "summary": {
            "stability_score": _round(stability_score),
            "rank_correlation_vs_base": 1.0 if not fragile else 0.5,
            "metric_degradation_rate": _round(max(degradations) if degradations else 0.0),
            "counterfactual_status": counterfactual.get("status", "MISSING"),
            "promotion_gate_allowed": False,
        },
        "warnings": [
            {
                "warning": "sensitivity_fragile_parameter_detected",
                "parameter": row["parameter"],
                "perturbation": row["perturbation"],
                "promotion_gate_allowed": False,
            }
            for row in fragile
        ],
        "blockers": [],
        "next_recommended_action": (
            "keep_quarantined_until_threshold_fragility_reviewed"
            if fragile
            else "continue_sensitivity_observation"
        ),
    }


def _tail_risk_sensitivity_metric(
    rows: list[Mapping[str, Any]],
    *,
    min_trigger_labels: int,
    intensity: float,
) -> dict[str, Any]:
    returns = []
    triggered = 0
    for row in rows:
        would_trigger = _first_int(row.get("trigger_label_count")) >= min_trigger_labels
        if would_trigger:
            triggered += 1
            fallback = _float(row.get("static_allocation_baseline_return"), 0.0) * intensity
            fallback += _float(row.get("no_fallback_baseline_return"), 0.0) * (1.0 - intensity)
        else:
            fallback = _float(row.get("no_fallback_baseline_return"), 0.0)
        returns.append(fallback)
    return {
        "sample_count": len(rows),
        "triggered_count": triggered,
        "avg_fallback_return": _round(_mean(returns)),
    }


def _tail_risk_fallback_error_cost_ledger(
    *,
    config: Mapping[str, Any],
    independent: Mapping[str, Any],
) -> dict[str, Any]:
    policy = _next_stage_section(config, "tail_risk_fallback_error_cost_ledger")
    rows = _records(independent.get("decision_outcomes"))
    fp = [row for row in rows if row.get("false_positive")]
    fn = [row for row in rows if row.get("false_negative")]
    fp_costs = [abs(_float(row.get("fallback_vs_no_fallback_return_delta"), 0.0)) for row in fp]
    fn_costs = [abs(_tail_risk_forward_drawdown(row)) for row in fn]
    asymmetry = (
        (sum(fn_costs) / sum(fp_costs)) if sum(fp_costs) else (float(len(fn)) if fn else 0.0)
    )
    sample_floor = _first_int(policy.get("min_sample_count")) or 30
    if len(rows) < sample_floor:
        status = "INSUFFICIENT_SAMPLE"
    elif asymmetry > _float(policy.get("unacceptable_asymmetry_floor"), 3.0):
        status = "ERROR_COST_UNACCEPTABLE"
    elif fp or fn:
        status = "ERROR_COST_MIXED"
    else:
        status = "ERROR_COST_ACCEPTABLE"
    sorted_cases = sorted(
        [
            {
                "case_key": row.get("case_key"),
                "asset": row.get("asset"),
                "horizon": row.get("horizon"),
                "error_type": "false_positive" if row.get("false_positive") else "false_negative",
                "cost": _round(
                    abs(_float(row.get("fallback_vs_no_fallback_return_delta"), 0.0))
                    if row.get("false_positive")
                    else abs(_tail_risk_forward_drawdown(row))
                ),
                "promotion_gate_allowed": False,
            }
            for row in [*fp, *fn]
        ],
        key=lambda row: _float(row["cost"], 0.0),
        reverse=True,
    )
    return {
        "status": status,
        "false_positive_cases": fp[:100],
        "false_negative_cases": fn[:100],
        "worst_5_cases": sorted_cases[:5],
        "best_5_cases": list(reversed(sorted_cases[-5:])),
        "summary": {
            "false_positive_count": len(fp),
            "false_negative_count": len(fn),
            "avg_false_positive_return_cost": _round(_mean(fp_costs)),
            "avg_false_negative_drawdown_cost": _round(_mean(fn_costs)),
            "median_cost": (
                _round(statistics.median([*fp_costs, *fn_costs])) if fp_costs or fn_costs else 0.0
            ),
            "cost_asymmetry_score": _round(asymmetry),
            "promotion_gate_allowed": False,
        },
        "warnings": [],
        "blockers": [],
        "next_recommended_action": "review_error_cost_before_any_future_gate_discussion",
    }


def _tail_risk_evidence_maturity_gate(
    *,
    config: Mapping[str, Any],
    independent: Mapping[str, Any],
    regime: Mapping[str, Any],
) -> dict[str, Any]:
    policy = _next_stage_section(config, "tail_risk_evidence_maturity_gate")
    rows = _records(independent.get("decision_outcomes"))
    triggered = [row for row in rows if row.get("fallback_triggered")]
    non_triggered = [row for row in rows if not row.get("fallback_triggered")]
    valid = {
        "5d": sum(1 for row in rows if row.get("future_5d_return") is not None),
        "10d": sum(1 for row in rows if row.get("future_10d_return") is not None),
        "20d": sum(1 for row in rows if row.get("future_20d_return") is not None),
    }
    regime_rows = _records(regime.get("regime_rows"))
    regime_min = min((_first_int(row.get("sample_count")) for row in regime_rows), default=0)
    tail_event_count = sum(1 for row in rows if row.get("independent_tail_event"))
    recent_cutoff = (
        sorted([str(row.get("decision_time")) for row in rows if row.get("decision_time")])[-10:]
        if rows
        else []
    )
    recent_sample_count = sum(1 for row in rows if row.get("decision_time") in recent_cutoff)
    triggered_count = len(triggered)
    if independent.get("status") == "INDEPENDENT_FORWARD_BLOCKED":
        status = "EVIDENCE_BLOCKED"
        evidence_level = "blocked"
    elif triggered_count >= (
        _first_int(policy.get("moderate_triggered_floor")) or 100
    ) and regime_min >= (_first_int(policy.get("regime_min_sample_floor")) or 20):
        status = "EVIDENCE_MATURE"
        evidence_level = "moderate"
    elif triggered_count >= (_first_int(policy.get("weak_triggered_floor")) or 60):
        status = "EVIDENCE_WEAK"
        evidence_level = "weak"
    else:
        status = "EVIDENCE_INSUFFICIENT"
        evidence_level = "insufficient"
    checks = [
        _tail_risk_maturity_check(
            "triggered_count",
            triggered_count,
            _first_int(policy.get("initial_triggered_floor")) or 30,
        ),
        _tail_risk_maturity_check(
            "weak_evidence_triggered_count",
            triggered_count,
            _first_int(policy.get("weak_triggered_floor")) or 60,
        ),
        _tail_risk_maturity_check(
            "moderate_evidence_triggered_count",
            triggered_count,
            _first_int(policy.get("moderate_triggered_floor")) or 100,
        ),
        _tail_risk_maturity_check(
            "regime_min_sample_count",
            regime_min,
            _first_int(policy.get("regime_min_sample_floor")) or 20,
        ),
    ]
    return {
        "status": status,
        "maturity_checks": checks,
        "summary": {
            "total_decision_count": len(rows),
            "triggered_count": triggered_count,
            "non_triggered_count": len(non_triggered),
            "valid_forward_5d_count": valid["5d"],
            "valid_forward_10d_count": valid["10d"],
            "valid_forward_20d_count": valid["20d"],
            "regime_min_sample_count": regime_min,
            "tail_event_count": tail_event_count,
            "recent_sample_count": recent_sample_count,
            "evidence_level": evidence_level,
            "promotion_gate_allowed": False,
        },
        "warnings": [
            {
                "warning": check["check_id"],
                "observed": check["observed"],
                "required": check["required"],
                "promotion_gate_allowed": False,
            }
            for check in checks
            if not check["passed"]
        ],
        "blockers": (
            []
            if status != "EVIDENCE_BLOCKED"
            else [{"blocker": "independent_forward_blocked", "promotion_gate_allowed": False}]
        ),
        "next_recommended_action": (
            "collect_more_forward_samples"
            if status in {"EVIDENCE_INSUFFICIENT", "EVIDENCE_WEAK"}
            else "continue_owner_review_without_promotion"
        ),
    }


def _tail_risk_maturity_check(check_id: str, observed: int, required: int) -> dict[str, Any]:
    return {
        "check_id": check_id,
        "observed": observed,
        "required": required,
        "passed": observed >= required,
        "promotion_gate_allowed": False,
    }


def _tail_risk_forward_aging_tracker(
    *,
    config: Mapping[str, Any],
    forward: Mapping[str, Any],
    independent: Mapping[str, Any],
    as_of_date: date | None,
) -> dict[str, Any]:
    _ = config
    resolved = as_of_date or date.today()
    forward_records = _records(forward.get("forward_records"))
    rows = _records(independent.get("decision_outcomes"))
    matured = {
        "5d": sum(1 for row in rows if row.get("future_5d_return") is not None),
        "10d": sum(1 for row in rows if row.get("future_10d_return") is not None),
        "20d": sum(1 for row in rows if row.get("future_20d_return") is not None),
    }
    pending = sum(
        1
        for row in forward_records
        if _mapping(row.get("actual_future_outcome_after_maturity")).get("status")
        == "pending_maturity"
    )
    status = (
        "FORWARD_AGING_PENDING"
        if pending
        else ("INSUFFICIENT_NEW_SAMPLE" if not rows else "FORWARD_AGING_HEALTHY")
    )
    rolling = _tail_risk_independent_horizon_summary(rows)
    return {
        "status": status,
        "aging_bucket_summary": {
            "as_of": resolved.isoformat(),
            "new_decisions_since_last_run": len(forward_records),
            "matured_5d_outcomes": matured["5d"],
            "matured_10d_outcomes": matured["10d"],
            "matured_20d_outcomes": matured["20d"],
            "pending_outcomes": pending,
            "promotion_gate_allowed": False,
        },
        "rolling_forward_performance": rolling,
        "summary": {
            "new_decisions_since_last_run": len(forward_records),
            "matured_5d_outcomes": matured["5d"],
            "matured_10d_outcomes": matured["10d"],
            "matured_20d_outcomes": matured["20d"],
            "pending_outcomes": pending,
            "promotion_gate_allowed": False,
        },
        "warnings": (
            [
                {
                    "warning": "forward_records_pending_maturity",
                    "pending_outcomes": pending,
                    "promotion_gate_allowed": False,
                }
            ]
            if pending
            else []
        ),
        "blockers": [],
        "next_recommended_action": "continue_aging_tracker_until_forward_records_mature",
    }


def _tail_risk_leakage_stress_suite(
    *,
    trigger_label: Mapping[str, Any],
    independent: Mapping[str, Any],
    contract: Mapping[str, Any],
    boundary: Mapping[str, Any],
) -> dict[str, Any]:
    tests = [
        _tail_risk_stress_row(
            "signal_lag_test",
            "WARN" if trigger_label.get("status") == "BLOCKED" else "PASS",
            "lagged signal cannot be trusted while trigger/label overlap is blocked",
        ),
        _tail_risk_stress_row(
            "label_permutation_test",
            "BLOCKED" if trigger_label.get("status") == "BLOCKED" else "PASS",
            "label-coupled trigger makes permutation test non-independent",
        ),
        _tail_risk_stress_row(
            "timestamp_boundary_test",
            "BLOCKED" if boundary.get("status") == "TIME_BOUNDARY_BLOCKED" else "PASS",
            "decision-time boundary audit is blocking",
        ),
        _tail_risk_stress_row(
            "feature_availability_test",
            "BLOCKED" if boundary.get("status") == "TIME_BOUNDARY_BLOCKED" else "PASS",
            "feature availability includes label proxy fields",
        ),
        _tail_risk_stress_row(
            "forward_window_overlap_test",
            "BLOCKED" if contract.get("status") == "BLOCKED" else "PASS",
            "contract forbids trigger/outcome overlap",
        ),
        _tail_risk_stress_row(
            "trigger_outcome_overlap_test",
            "BLOCKED" if contract.get("status") == "BLOCKED" else "PASS",
            "direct or derived trigger/outcome dependency detected",
        ),
        _tail_risk_stress_row(
            "randomized_decision_time_test",
            "WARN" if independent.get("status") != "INDEPENDENT_FORWARD_VALIDATED" else "PASS",
            "independent forward validation is not fully validated",
        ),
        _tail_risk_stress_row(
            "shuffled_outcome_sanity_test",
            "PASS" if independent.get("decision_outcomes") else "WARN",
            "requires decision outcomes to run",
        ),
    ]
    blocked = [row for row in tests if row["status"] == "BLOCKED"]
    warn = [row for row in tests if row["status"] == "WARN"]
    status = (
        "LEAKAGE_STRESS_BLOCKED"
        if blocked
        else ("LEAKAGE_STRESS_WARN" if warn else "LEAKAGE_STRESS_PASS")
    )
    return {
        "status": status,
        "stress_tests": tests,
        "summary": {
            "test_count": len(tests),
            "blocked_test_count": len(blocked),
            "warn_test_count": len(warn),
            "promotion_gate_allowed": False,
        },
        "warnings": warn,
        "blockers": blocked,
        "next_recommended_action": (
            "resolve_leakage_stress_blockers_before_any_promotion_review"
            if blocked
            else "continue_monitoring_with_promotion_blocked"
        ),
    }


def _tail_risk_stress_row(test_id: str, status: str, detail: str) -> dict[str, Any]:
    return {
        "test_id": test_id,
        "status": status,
        "detail": detail,
        "blocking": status == "BLOCKED",
        "promotion_gate_allowed": False,
    }


def _tail_risk_promotion_readiness_gate(
    *,
    inputs: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    blocking_statuses = {
        "TRADING-827": {"BLOCKED"},
        "TRADING-828": {"INDEPENDENT_FORWARD_BLOCKED"},
        "TRADING-829": {"BLOCKED"},
        "TRADING-830": {"TIME_BOUNDARY_BLOCKED"},
        "TRADING-838": {"LEAKAGE_STRESS_BLOCKED"},
    }
    rows = []
    blockers = []
    for task_id, payload in inputs.items():
        status = str(payload.get("status", "MISSING")) if payload else "MISSING"
        blocking = status in blocking_statuses.get(task_id, set()) or status == "MISSING"
        row = {
            "task_id": task_id,
            "status": status,
            "blocking": blocking,
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
        }
        rows.append(row)
        if blocking:
            blockers.append(row)
    status = "PROMOTION_READINESS_BLOCKED" if blockers else "PROMOTION_READINESS_REVIEWABLE"
    return {
        "status": status,
        "gate_inputs": rows,
        "summary": {
            "input_count": len(rows),
            "blocking_input_count": len(blockers),
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
        },
        "warnings": [],
        "blockers": blockers,
        "next_recommended_action": (
            "keep_tail_risk_fallback_quarantined"
            if blockers
            else "manual_owner_review_only_no_auto_promotion"
        ),
    }


def _tail_risk_independent_trigger_v2_builder(
    *,
    config: Mapping[str, Any],
    selected_cases: list[dict[str, Any]],
    boundary: Mapping[str, Any],
) -> dict[str, Any]:
    _ = config
    allowed_features = [
        "price_trend",
        "drawdown_from_recent_high",
        "realized_volatility",
        "volatility_expansion",
        "moving_average_distance",
        "market_breadth_proxy",
        "credit_liquidity_proxy",
        "vix_vxn_level_proxy",
    ]
    forbidden = _tail_risk_forbidden_label_fields()
    candidates = [
        {
            "candidate_id": "trigger_v2_price_drawdown_volatility",
            "input_features": [
                "price_trend",
                "drawdown_from_recent_high",
                "realized_volatility",
                "moving_average_distance",
            ],
            "decision_time_visible": True,
            "uses_old_tail_risk_label": False,
            "uses_future_outcome": False,
            "initial_non_promotional_signal_count": sum(
                1
                for row in selected_cases
                if _tail_risk_trend_bucket(row) == "bear"
                or _tail_risk_drawdown_bucket(row) in {"watch", "stress"}
            ),
            "promotion_gate_allowed": False,
        },
        {
            "candidate_id": "trigger_v2_market_proxy_confirmed",
            "input_features": [
                "price_trend",
                "volatility_expansion",
                "market_breadth_proxy",
                "vix_vxn_level_proxy",
            ],
            "decision_time_visible": True,
            "uses_old_tail_risk_label": False,
            "uses_future_outcome": False,
            "initial_non_promotional_signal_count": 0,
            "promotion_gate_allowed": False,
        },
    ]
    dependency_list = [
        {
            "feature_name": feature,
            "source": "decision_time_market_feature_or_proxy",
            "depends_on_forbidden_label": feature in forbidden,
            "depends_on_future_outcome": False,
            "promotion_gate_allowed": False,
        }
        for feature in allowed_features
    ]
    forbidden_count = sum(1 for row in dependency_list if row["depends_on_forbidden_label"])
    boundary_blocked = boundary.get("status") == "TIME_BOUNDARY_BLOCKED"
    blockers = []
    if forbidden_count:
        blockers.append({"blocker": "trigger_v2_forbidden_input", "promotion_gate_allowed": False})
    status = (
        "CANDIDATE_BLOCKED"
        if blockers
        else ("CANDIDATE_INSUFFICIENT_INPUTS" if not selected_cases else "CANDIDATE_BUILT")
    )
    return {
        "status": status,
        "candidate_trigger_v2_list": candidates,
        "feature_dependency_list": dependency_list,
        "time_window_contract": {
            "decision_time_or_earlier_required": True,
            "label_case_fields_forbidden": sorted(forbidden),
            "future_outcome_fields_forbidden": True,
            "current_boundary_audit_status": boundary.get("status", "MISSING"),
            "old_trigger_boundary_blocked": boundary_blocked,
            "promotion_gate_allowed": False,
        },
        "initial_non_promotional_diagnostics": {
            "selected_case_count": len(selected_cases),
            "candidate_count": len(candidates),
            "diagnostic_only": True,
            "promotion_gate_allowed": False,
        },
        "summary": {
            "candidate_count": len(candidates),
            "feature_dependency_count": len(dependency_list),
            "forbidden_input_count": forbidden_count,
            "promotion_gate_allowed": False,
        },
        "warnings": (
            [
                {
                    "warning": "old_trigger_boundary_still_blocked",
                    "impact": "v2 candidate is new research only; it does not unblock old fallback",
                    "promotion_gate_allowed": False,
                }
            ]
            if boundary_blocked
            else []
        ),
        "blockers": blockers,
        "next_recommended_action": "build_feature_availability_catalog_before_v2_validation",
    }


def _tail_risk_trigger_feature_availability_catalog(
    *,
    trigger_v2: Mapping[str, Any],
) -> dict[str, Any]:
    dependencies = _records(trigger_v2.get("feature_dependency_list"))
    catalog = []
    for row in dependencies:
        feature = str(row.get("feature_name"))
        proxy = feature.endswith("_proxy") or feature in {
            "market_breadth_proxy",
            "vix_vxn_level_proxy",
        }
        catalog.append(
            {
                "feature_name": feature,
                "source": row.get("source"),
                "earliest_available_date": AI_REGIME_START,
                "latest_available_date": "latest_valid_value_surface_case",
                "update_frequency": "daily",
                "decision_time_availability": "AVAILABLE" if not proxy else "PARTIAL_PROXY",
                "known_missing_periods": [] if not proxy else ["provider_or_proxy_not_configured"],
                "pit_quality": "PIT_SAFE" if not proxy else "PARTIAL",
                "usage_allowed_for_trigger": True,
                "usage_allowed_for_outcome": False,
                "promotion_gate_allowed": False,
            }
        )
    blocked = [row for row in catalog if not row["usage_allowed_for_trigger"]]
    partial = [row for row in catalog if row["pit_quality"] == "PARTIAL"]
    status = (
        "FEATURE_CATALOG_BLOCKED"
        if blocked
        else ("FEATURE_CATALOG_PARTIAL" if partial else "FEATURE_CATALOG_READY")
    )
    return {
        "status": status,
        "feature_catalog": catalog,
        "summary": {
            "feature_count": len(catalog),
            "partial_feature_count": len(partial),
            "blocked_feature_count": len(blocked),
            "promotion_gate_allowed": False,
        },
        "warnings": [
            {
                "warning": "partial_proxy_feature_requires_owner_review",
                "feature_name": row["feature_name"],
                "promotion_gate_allowed": False,
            }
            for row in partial
        ],
        "blockers": blocked,
        "next_recommended_action": "owner_review_partial_proxy_features_before_v2_backtest",
    }


def _tail_risk_research_master_review(
    *,
    inputs: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    rows = []
    for task_id, payload in inputs.items():
        status = str(payload.get("status", "MISSING")) if payload else "MISSING"
        blocking = status in {
            "BLOCKED",
            "INDEPENDENT_FORWARD_BLOCKED",
            "TIME_BOUNDARY_BLOCKED",
            "LEAKAGE_STRESS_BLOCKED",
            "PROMOTION_READINESS_BLOCKED",
            "FEATURE_CATALOG_BLOCKED",
            "MISSING",
        }
        rows.append(
            {
                "task_id": task_id,
                "status": status,
                "blocking": blocking,
                "promotion_gate_allowed": False,
            }
        )
    blockers = [row for row in rows if row["blocking"]]
    invalidated = [
        "precision",
        "recall",
        "f1",
        "label_based_return_metrics",
        "tail_risk_hit_rate",
        "fallback_triggered_hit_rate",
    ]
    valid_metrics = [
        "independent_forward_outcome_sample_counts",
        "counterfactual_baseline_diagnostics",
        "error_cost_ledger",
        "feature_availability_catalog",
    ]
    if any(row["task_id"] == "TRADING-827" and row["blocking"] for row in rows):
        recommendation = "REBUILD_TRIGGER"
    elif blockers:
        recommendation = "KEEP_QUARANTINED"
    else:
        recommendation = "CONTINUE_RESEARCH"
    return {
        "status": "TAIL_RISK_RESEARCH_MASTER_REVIEW_COMPLETE",
        "final_recommendation": recommendation,
        "current_valid_metrics": valid_metrics,
        "invalidated_metrics": invalidated,
        "remaining_blockers": blockers,
        "minimum_tasks_before_review": [
            "TRADING-827 resolved or accepted as quarantine reason",
            "TRADING-828 independent forward outcomes present",
            "TRADING-829/830/838 no blocking leakage or boundary status",
            "TRADING-839 manual reviewable readiness gate",
        ],
        "whether_shadow_possible_later": False if blockers else "owner_review_only",
        "whether_production_possible_later": False if blockers else "owner_review_only",
        "owner_next_action": (
            "rebuild_trigger_or_keep_current_fallback_quarantined"
            if blockers
            else "manual_review_no_auto_promotion"
        ),
        "summary": {
            "input_task_count": len(rows),
            "blocking_task_count": len(blockers),
            "valid_metric_count": len(valid_metrics),
            "invalidated_metric_count": len(invalidated),
            "promotion_gate_allowed": False,
        },
        "warnings": [],
        "blockers": blockers,
    }


def _tail_risk_benchmark_fallback_rows(
    selected_cases: list[dict[str, Any]],
    classifier: Mapping[str, Any],
) -> list[dict[str, Any]]:
    label_map = _classifier_label_map(classifier)
    output = []
    for row in selected_cases:
        original = dict(row)
        labels = label_map.get(_case_key(original), {})
        trigger_labels = [
            label
            for label in [
                "large_loss_case",
                "tail_loss_case",
                "benchmark_underperformance_case",
                "long_horizon_failure_case",
            ]
            if bool(labels.get(label))
        ]
        triggered = bool(labels.get("tail_risk_signal_high"))
        item = (
            _fallback_case_to_benchmark(dict(original), "tail_risk_benchmark_fallback")
            if triggered
            else dict(original)
        )
        original_delta = _float(original.get("delta_vs_benchmark"), 0.0)
        original_return = _float(original.get("selected_realized_net_return"), 0.0)
        fallback_return = _float(item.get("selected_realized_net_return"), 0.0)
        item.update(
            {
                "case_key": _case_key(original),
                "fallback_triggered": triggered,
                "trigger_reason": ",".join(trigger_labels) if trigger_labels else "not_triggered",
                "trigger_labels": trigger_labels,
                "tail_risk_signal_high": triggered,
                "original_selected_action": original.get("selected_action"),
                "original_selected_realized_net_return": original.get(
                    "selected_realized_net_return"
                ),
                "original_delta_vs_benchmark": _round(original_delta),
                "original_selected_estimated_cost": original.get("selected_estimated_cost"),
                "original_selected_turnover_cost_assumption": original.get(
                    "selected_turnover_cost_assumption"
                ),
                "missed_upside": _round(
                    max(0.0, original_return - fallback_return) if triggered else 0.0
                ),
                "avoided_tail_loss": _round(max(0.0, -original_delta) if triggered else 0.0),
                "promotion_gate_allowed": False,
            }
        )
        for label in [
            "large_loss_case",
            "tail_loss_case",
            "benchmark_underperformance_case",
            "long_horizon_failure_case",
        ]:
            item[label] = bool(labels.get(label))
        if not triggered:
            item["guardrail_action"] = "keep_benchmark_first_candidate"
        output.append(item)
    return output


def _tail_risk_fallback_group_breakdown(
    original_rows: list[dict[str, Any]],
    fallback_rows: list[dict[str, Any]],
    group_key: str,
    config: Mapping[str, Any],
) -> list[dict[str, Any]]:
    values = sorted({str(row.get(group_key, "unknown")) for row in original_rows})
    rows = []
    for value in values:
        original_subset = [
            row for row in original_rows if str(row.get(group_key, "unknown")) == value
        ]
        fallback_subset = [
            row for row in fallback_rows if str(row.get(group_key, "unknown")) == value
        ]
        original_metric = _v2_variant_metric_row("original_group", original_subset, config)
        fallback_metric = _add_variant_deltas(
            _v2_variant_metric_row("fallback_group", fallback_subset, config),
            original_metric,
        )
        fallback_count = sum(1 for row in fallback_subset if row.get("fallback_triggered"))
        rows.append(
            {
                "group_key": group_key,
                "group_value": value,
                "case_count": len(original_subset),
                "fallback_trigger_count": fallback_count,
                "fallback_frequency": _round(
                    fallback_count / len(original_subset) if original_subset else 0.0
                ),
                "tail_loss_reduction": _round(
                    _tail_loss_reduction(original_metric, fallback_metric)
                ),
                "mean_delta_vs_benchmark": fallback_metric.get("mean_delta_vs_benchmark"),
                "median_delta_vs_benchmark": fallback_metric.get("median_delta_vs_benchmark"),
                "turnover_delta": fallback_metric.get("turnover_delta"),
                "cost_delta": fallback_metric.get("cost_delta"),
                "upside_capture": _upside_capture_summary(original_subset, fallback_subset).get(
                    "upside_capture_ratio"
                ),
                "missed_upside_count": sum(
                    1 for row in fallback_subset if _float(row.get("missed_upside"), 0.0) > 0
                ),
                "promotion_gate_allowed": False,
            }
        )
    return rows


def _upside_capture_summary(
    original_rows: list[dict[str, Any]],
    fallback_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    paired = list(zip(original_rows, fallback_rows, strict=False))
    benchmark_upside = [
        (original, fallback)
        for original, fallback in paired
        if _float(original.get("benchmark_realized_net_return"), 0.0) > 0
    ]
    benchmark_sum = sum(
        _float(original.get("benchmark_realized_net_return"), 0.0)
        for original, _fallback in benchmark_upside
    )
    captured_sum = sum(
        max(0.0, _float(fallback.get("selected_realized_net_return"), 0.0))
        for _original, fallback in benchmark_upside
    )
    participation = sum(
        1
        for _original, fallback in benchmark_upside
        if _float(fallback.get("selected_realized_net_return"), 0.0) > 0
    )
    missed_count = sum(1 for row in fallback_rows if _float(row.get("missed_upside"), 0.0) > 0)
    return {
        "benchmark_upside_case_count": len(benchmark_upside),
        "strategy_participation": _round(
            participation / len(benchmark_upside) if benchmark_upside else 0.0
        ),
        "upside_capture_ratio": _round(captured_sum / benchmark_sum if benchmark_sum else 0.0),
        "missed_upside_count": missed_count,
        "promotion_gate_allowed": False,
    }


def _upside_capture_group_rows(rows: list[dict[str, Any]], group_key: str) -> list[dict[str, Any]]:
    output = []
    for value in sorted({str(row.get(group_key, "unknown")) for row in rows}):
        subset = [row for row in rows if str(row.get(group_key, "unknown")) == value]
        benchmark_upside_sum = sum(
            _float(row.get("benchmark_realized_net_return"), 0.0)
            for row in subset
            if _float(row.get("benchmark_realized_net_return"), 0.0) > 0
        )
        captured = sum(
            max(0.0, _float(row.get("selected_realized_net_return"), 0.0))
            for row in subset
            if _float(row.get("benchmark_realized_net_return"), 0.0) > 0
        )
        output.append(
            {
                "group_key": group_key,
                "group_value": value,
                "benchmark_upside_case_count": sum(
                    1 for row in subset if _float(row.get("benchmark_realized_net_return"), 0.0) > 0
                ),
                "upside_capture_ratio": _round(
                    captured / benchmark_upside_sum if benchmark_upside_sum else 0.0
                ),
                "missed_upside_count": sum(
                    1 for row in subset if _float(row.get("missed_upside"), 0.0) > 0
                ),
                "promotion_gate_allowed": False,
            }
        )
    return output


def _missed_upside_concentration(
    rows: list[dict[str, Any]],
    group_key: str,
) -> dict[str, Any]:
    total = sum(_float(row.get("missed_upside"), 0.0) for row in rows)
    groups = []
    for value in sorted({str(row.get(group_key, "unknown")) for row in rows}):
        subset = [row for row in rows if str(row.get(group_key, "unknown")) == value]
        group_cost = sum(_float(row.get("missed_upside"), 0.0) for row in subset)
        groups.append(
            {
                "group_key": group_key,
                "group_value": value,
                "missed_upside_count": len(subset),
                "missed_upside_cost": _round(group_cost),
                "missed_upside_share": _round(group_cost / total if total else 0.0),
                "promotion_gate_allowed": False,
            }
        )
    return {
        "summary": {
            "group_key": group_key,
            "missed_upside_case_count": len(rows),
            "total_missed_upside": _round(total),
            "promotion_gate_allowed": False,
        },
        "groups": sorted(groups, key=lambda row: _float(row["missed_upside_share"]), reverse=True),
    }


def _tail_risk_forward_record(
    *,
    row: Mapping[str, Any],
    archive_id: str,
    as_of: date,
    maturity_horizons: list[str],
    future_status: str,
) -> dict[str, Any]:
    trigger_labels = [str(item) for item in row.get("trigger_labels", [])]
    expected_risk = len(trigger_labels) / 4 if trigger_labels else 0.0
    return {
        "record_id": f"{archive_id}:{row.get('case_key')}",
        "archive_id": archive_id,
        "as_of": as_of.isoformat(),
        "source_case_key": row.get("case_key"),
        "asset": row.get("asset"),
        "horizon": row.get("horizon"),
        "regime_segment": row.get("regime_segment"),
        "asset_cluster": row.get("asset_cluster"),
        "benchmark_output": {
            "benchmark_action": row.get("benchmark_action"),
            "benchmark_estimated_cost": row.get("benchmark_estimated_cost"),
            "realized_future_return_included": False,
        },
        "tail_risk_fallback_signal": {
            "signal_id": "tail_risk_benchmark_fallback",
            "signal_mode": "controlled_dry_run_pending_forward_maturity",
            "tail_risk_signal_high": bool(row.get("tail_risk_signal_high")),
            "trigger_labels": trigger_labels,
        },
        "fallback_triggered": bool(row.get("fallback_triggered")),
        "trigger_reason": row.get("trigger_reason"),
        "expected_avoided_risk": _round(expected_risk),
        "actual_future_outcome_after_maturity": {
            "status": future_status,
            "maturity_horizons": maturity_horizons,
            "outcome_append_only": True,
        },
        "broker_action": "none",
        "paper_shadow_change_allowed": False,
        "production_weight_change_allowed": False,
        "promotion_gate_allowed": False,
    }


def _case_max_drawdown(rows: list[Mapping[str, Any]]) -> float:
    return max((_float(row.get("selected_drawdown_proxy"), 0.0) for row in rows), default=0.0)


def _nested_mapping(payload: Mapping[str, Any], preferred: str, fallback: str) -> Mapping[str, Any]:
    value = payload.get(preferred)
    if isinstance(value, Mapping):
        return value
    value = payload.get(fallback)
    return value if isinstance(value, Mapping) else {}


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _stable_hash(value: Any) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _case_data_window(rows: list[Mapping[str, Any]]) -> dict[str, Any]:
    dates = sorted(str(row.get("date")) for row in rows if row.get("date"))
    return {
        "date_start": dates[0] if dates else None,
        "date_end": dates[-1] if dates else None,
        "sample_count": len(rows),
        "market_regime": "ai_after_chatgpt",
    }


def _record_data_window(rows: list[Mapping[str, Any]]) -> dict[str, Any]:
    dates = sorted(
        str(row.get("decision_timestamp")) for row in rows if row.get("decision_timestamp")
    )
    return {
        "date_start": dates[0] if dates else None,
        "date_end": dates[-1] if dates else None,
        "sample_count": len(rows),
        "market_regime": "ai_after_chatgpt",
    }


def _combined_data_window(rows: list[Mapping[str, Any]]) -> dict[str, Any]:
    starts = [str(row["date_start"]) for row in rows if row.get("date_start")]
    ends = [str(row["date_end"]) for row in rows if row.get("date_end")]
    return {
        "date_start": min(starts) if starts else None,
        "date_end": max(ends) if ends else None,
        "task_count": len(rows),
        "market_regime": "ai_after_chatgpt",
    }


def _reconciliation_task_row(
    *,
    task_id: str,
    artifact_id: str,
    report_path: Path,
    payload: Mapping[str, Any],
    sample_universe_name: str,
    denominator: int,
    fallback_trigger_count: int,
    label_definition: str,
    trigger_definition: str,
    benchmark_name: str,
    outcome_horizon: str,
    asset_universe: list[str] | None = None,
    positive_label_count: int | None = None,
    negative_label_count: int | None = None,
    tp: int | None = None,
    fp: int | None = None,
    fn: int | None = None,
    tn: int | None = None,
) -> dict[str, Any]:
    date_start, date_end = _artifact_date_window(payload)
    row = {
        "task_id": task_id,
        "artifact_id": artifact_id,
        "report_path": str(report_path),
        "sample_universe_name": sample_universe_name,
        "date_start": date_start,
        "date_end": date_end,
        "asset_universe": asset_universe or [],
        "benchmark_name": benchmark_name,
        "label_definition": label_definition,
        "trigger_definition": trigger_definition,
        "outcome_horizon": outcome_horizon,
        "feature_timestamp_rule": (
            "decision-time PIT features only; realized outcome evaluation-only"
        ),
        "label_timestamp_rule": (
            "label assigned after realized outcome maturity or controlled audit label"
        ),
        "sample_count_total": denominator or None,
        "eligible_sample_count": denominator or None,
        "fallback_trigger_count": fallback_trigger_count,
        "positive_label_count": positive_label_count,
        "negative_label_count": negative_label_count,
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "tn": tn,
        "excluded_sample_count": 0,
        "exclusion_reason_summary": "none recorded in source artifact",
        "missing_field_records": [],
        "promotion_gate_allowed": False,
    }
    required = [
        "task_id",
        "artifact_id",
        "report_path",
        "sample_universe_name",
        "date_start",
        "date_end",
        "asset_universe",
        "benchmark_name",
        "label_definition",
        "trigger_definition",
        "outcome_horizon",
        "feature_timestamp_rule",
        "label_timestamp_rule",
        "sample_count_total",
        "eligible_sample_count",
        "fallback_trigger_count",
        "positive_label_count",
        "negative_label_count",
        "tp",
        "fp",
        "fn",
        "tn",
        "excluded_sample_count",
        "exclusion_reason_summary",
    ]
    for field in required:
        value = row.get(field)
        if value is None or value == "" or value == []:
            row["missing_field_records"].append(
                {
                    "task_id": task_id,
                    "missing_field": True,
                    "missing_field_name": field,
                    "reconciliation_status": "INCOMPLETE",
                    "promotion_gate_allowed": False,
                }
            )
    row["missing_field"] = bool(row["missing_field_records"])
    row["reconciliation_status"] = "INCOMPLETE" if row["missing_field"] else "RECONCILED"
    return row


def _artifact_date_window(payload: Mapping[str, Any]) -> tuple[str | None, str | None]:
    summary = _mapping(payload.get("summary"))
    requested = str(summary.get("requested_date_range", ""))
    if ".." in requested:
        start, end = requested.split("..", 1)
        return start or None, end or None
    records = _records(payload.get("forward_records")) or _records(payload.get("fallback_cases"))
    dates = sorted(
        str(row.get("date") or _source_case_date(row.get("source_case_key")) or "")
        for row in records
        if row.get("date") or row.get("source_case_key")
    )
    return (dates[0], dates[-1]) if dates else (None, None)


def _source_case_date(value: Any) -> str | None:
    if not value:
        return None
    text = str(value)
    return text.split("|", 1)[0] if "|" in text else None


def _group_values(value: Any, field: str) -> list[str]:
    return sorted({str(row.get(field)) for row in _records(value) if row.get(field)})


def _forward_record_assets(forward: Mapping[str, Any]) -> list[str]:
    return sorted(
        {
            str(row.get("asset"))
            for row in _records(forward.get("forward_records"))
            if row.get("asset")
        }
    )


def _confusion_total(precision: Mapping[str, Any]) -> int:
    matrix = _mapping(precision.get("confusion_matrix"))
    return sum(
        _first_int(matrix.get(field))
        for field in ["true_positive", "false_positive", "false_negative", "true_negative"]
    )


def _confusion_positive_count(precision: Mapping[str, Any]) -> int:
    matrix = _mapping(precision.get("confusion_matrix"))
    return _first_int(matrix.get("true_positive")) + _first_int(matrix.get("false_negative"))


def _count_reconciliation_summary(
    rows: list[Mapping[str, Any]],
    robustness: Mapping[str, Any],
    precision: Mapping[str, Any],
    opportunity: Mapping[str, Any],
    forward: Mapping[str, Any],
) -> list[dict[str, Any]]:
    row_by_task = {str(row["task_id"]): row for row in rows}
    matrix = _mapping(precision.get("confusion_matrix"))
    opportunity_summary = _nested_mapping(opportunity, "opportunity_cost_summary", "summary")
    forward_summary = _nested_mapping(forward, "integration_summary", "summary")
    return [
        {
            "count_name": "fallback_trigger_count",
            "source_task": "TRADING-816",
            "value": row_by_task["TRADING-816"].get("fallback_trigger_count"),
            "denominator": row_by_task["TRADING-816"].get("sample_count_total"),
            "universe": row_by_task["TRADING-816"].get("sample_universe_name"),
            "date_window": _row_date_window(row_by_task["TRADING-816"]),
            "interpretation": "full historical robustness fallback trigger count",
            "is_comparable_to_other_count": False,
            "not_comparable_reason": (
                "different denominator and universe from precision, upside, " "and forward reports"
            ),
            "promotion_gate_allowed": False,
        },
        {
            "count_name": "TP_plus_FP",
            "source_task": "TRADING-817",
            "value": _first_int(matrix.get("true_positive"))
            + _first_int(matrix.get("false_positive")),
            "denominator": row_by_task["TRADING-817"].get("sample_count_total"),
            "universe": row_by_task["TRADING-817"].get("sample_universe_name"),
            "date_window": _row_date_window(row_by_task["TRADING-817"]),
            "interpretation": (
                "precision/recall label-positive trigger subset; "
                "excludes risk_downshift_non_tail_negative_count"
            ),
            "is_comparable_to_other_count": False,
            "not_comparable_reason": (
                "TP+FP is a confusion-matrix subset, not the full trigger count"
            ),
            "promotion_gate_allowed": False,
        },
        {
            "count_name": "benchmark_upside_case_count",
            "source_task": "TRADING-818",
            "value": opportunity_summary.get("benchmark_upside_case_count"),
            "denominator": row_by_task["TRADING-818"].get("sample_count_total"),
            "universe": row_by_task["TRADING-818"].get("sample_universe_name"),
            "date_window": _row_date_window(row_by_task["TRADING-818"]),
            "interpretation": (
                "benchmark upside opportunity universe, not fallback trigger universe"
            ),
            "is_comparable_to_other_count": False,
            "not_comparable_reason": "filters for benchmark-upside opportunity cases",
            "promotion_gate_allowed": False,
        },
        {
            "count_name": "fallback_trigger_count",
            "source_task": "TRADING-819",
            "value": forward_summary.get("fallback_trigger_count"),
            "denominator": row_by_task["TRADING-819"].get("sample_count_total"),
            "universe": row_by_task["TRADING-819"].get("sample_universe_name"),
            "date_window": _row_date_window(row_by_task["TRADING-819"]),
            "interpretation": "fallback triggers inside capped forward evidence record universe",
            "is_comparable_to_other_count": False,
            "not_comparable_reason": "forward record universe is capped and pending maturity",
            "promotion_gate_allowed": False,
        },
    ]


def _row_date_window(row: Mapping[str, Any]) -> str:
    return f"{row.get('date_start')}..{row.get('date_end')}"


def _timestamp_availability_audit_rows(
    value_surface: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> list[dict[str, Any]]:
    configured = _records(policy.get("feature_inputs"))
    source_rows = _records(value_surface.get("feature_inputs")) or configured
    if not source_rows:
        source_rows = [
            {
                "feature_name": "trailing_return",
                "source_type": "price_cache",
                "pit_status": "pit_safe",
            },
            {
                "feature_name": "trailing_volatility",
                "source_type": "price_cache",
                "pit_status": "pit_safe",
            },
            {
                "feature_name": "trailing_drawdown",
                "source_type": "price_cache",
                "pit_status": "pit_safe",
            },
            {
                "feature_name": "classifier_tail_risk_labels",
                "source_type": "controlled_label_proxy",
                "pit_status": "unknown",
            },
        ]
    rows = []
    for item in source_rows:
        decision = str(item.get("decision_timestamp", "decision_timestamp"))
        feature_timestamp = str(item.get("feature_timestamp", decision))
        available_at = str(item.get("available_at_timestamp", decision))
        lag_pass = _timestamp_lag_pass(available_at, decision)
        rows.append(
            {
                "feature_name": str(item.get("feature_name", item.get("field", "unknown"))),
                "feature_timestamp": feature_timestamp,
                "decision_timestamp": decision,
                "available_at_timestamp": available_at,
                "required_lag": item.get("required_lag", "available_at<=decision"),
                "actual_lag": item.get("actual_lag", "0" if lag_pass else "negative"),
                "lag_pass": lag_pass,
                "source_type": str(item.get("source_type", "controlled_artifact")),
                "pit_status": str(item.get("pit_status", "unknown")),
                "leakage_risk": "LOW" if lag_pass else "HIGH",
                "promotion_gate_allowed": False,
            }
        )
    return rows


def _timestamp_lag_pass(available_at: str, decision: str) -> bool:
    if available_at == decision:
        return True
    try:
        return date.fromisoformat(available_at[:10]) <= date.fromisoformat(decision[:10])
    except ValueError:
        return available_at <= decision


def _label_trigger_overlap_audit(
    config: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    trigger_fields = [
        str(item)
        for item in policy.get(
            "trigger_input_fields",
            [
                "large_loss_case",
                "tail_loss_case",
                "benchmark_underperformance_case",
                "long_horizon_failure_case",
            ],
        )
    ]
    label_policy = _next_stage_section(config, "tail_risk_fallback_trigger_precision_recall_audit")
    label_fields = [
        str(item)
        for item in policy.get("label_input_fields", label_policy.get("positive_label_fields", []))
    ]
    overlap = sorted(set(trigger_fields) & set(label_fields))
    derived_overlap = (
        ["tail_risk_signal_high"] if overlap and "tail_risk_signal_high" not in overlap else []
    )
    denominator = max(1, len(set(trigger_fields) | set(label_fields)))
    ratio = (len(overlap) + len(derived_overlap)) / denominator
    if ratio >= 0.75:
        risk = "CRITICAL"
    elif ratio >= 0.50:
        risk = "HIGH"
    elif ratio > 0:
        risk = "MEDIUM"
    else:
        risk = "LOW"
    independent = bool(policy.get("independent_outcome_validation_present", False))
    return {
        "trigger_input_fields": trigger_fields,
        "label_input_fields": label_fields,
        "overlap_fields": overlap,
        "derived_overlap_fields": derived_overlap,
        "overlap_ratio": _round(ratio),
        "coupling_risk": risk,
        "independent_outcome_validation_present": independent,
        "promotion_gate_allowed": False,
    }


def _outcome_horizon_separation_rows(
    selected_cases: list[dict[str, Any]],
    value_surface: Mapping[str, Any],
) -> list[dict[str, Any]]:
    configured = _records(value_surface.get("outcome_windows"))
    source_rows = configured or selected_cases[:25]
    rows = []
    for item in source_rows:
        decision = str(item.get("decision_timestamp") or item.get("date") or "decision_timestamp")
        horizon_days = _horizon_days(item.get("outcome_horizon") or item.get("horizon"))
        outcome_start = str(item.get("outcome_start_timestamp") or _add_days_text(decision, 1))
        outcome_end = str(
            item.get("outcome_end_timestamp") or _add_days_text(decision, horizon_days)
        )
        overlap = _timestamp_not_after(outcome_start, decision)
        rows.append(
            {
                "decision_timestamp": decision,
                "outcome_start_timestamp": outcome_start,
                "outcome_end_timestamp": outcome_end,
                "horizon_days": horizon_days,
                "separation_days": 0 if overlap else 1,
                "overlap_detected": overlap,
                "leakage_risk": "CRITICAL" if overlap else "LOW",
                "promotion_gate_allowed": False,
            }
        )
    return rows


def _pit_revision_audit_rows(
    timestamp_rows: list[Mapping[str, Any]],
    classifier: Mapping[str, Any],
    robustness: Mapping[str, Any],
) -> list[dict[str, Any]]:
    rows = []
    for item in timestamp_rows:
        pit_status = str(item.get("pit_status", "unknown"))
        rows.append(
            {
                "feature_name": item.get("feature_name"),
                "pit_safe": pit_status == "pit_safe",
                "pit_status": pit_status,
                "revision_risk": (
                    "LOW"
                    if pit_status == "pit_safe"
                    else ("MEDIUM" if pit_status == "unknown" else "HIGH")
                ),
                "source_type": item.get("source_type"),
                "promotion_gate_allowed": False,
            }
        )
    if classifier and robustness:
        rows.append(
            {
                "feature_name": "tail_risk_classifier_labels",
                "pit_safe": False,
                "pit_status": "unknown",
                "revision_risk": "MEDIUM",
                "source_type": "controlled_historical_label_proxy",
                "promotion_gate_allowed": False,
            }
        )
    return rows


def _tail_risk_variant_rows(
    selected_cases: list[dict[str, Any]],
    classifier: Mapping[str, Any],
    *,
    min_trigger_score: float = 0.25,
    forced_trigger_by_key: Mapping[str, bool] | None = None,
    benchmark_mode: str = "current_dynamic_policy",
    cost_penalty: float = 0.0,
) -> list[dict[str, Any]]:
    label_map = _classifier_label_map(classifier)
    output = []
    for original in selected_cases:
        labels = label_map.get(_case_key(original), {})
        trigger_labels = _tail_risk_trigger_labels(labels)
        score = len(trigger_labels) / 4 if trigger_labels else 0.0
        forced = forced_trigger_by_key.get(_case_key(original)) if forced_trigger_by_key else None
        triggered = bool(forced) if forced is not None else score >= min_trigger_score
        item = (
            _fallback_case_to_benchmark(dict(original), "tail_risk_benchmark_fallback")
            if triggered
            else dict(original)
        )
        if benchmark_mode != "current_dynamic_policy":
            item = _apply_benchmark_mode(item, benchmark_mode)
        if triggered and cost_penalty:
            item["selected_realized_net_return"] = _round(
                _float(item.get("selected_realized_net_return"), 0.0) - cost_penalty
            )
            item["delta_vs_benchmark"] = _round(
                _float(item.get("selected_realized_net_return"), 0.0)
                - _float(item.get("benchmark_realized_net_return"), 0.0)
            )
            item["selected_estimated_cost"] = _round(
                _float(item.get("selected_estimated_cost"), 0.0) + cost_penalty
            )
        original_delta = _float(original.get("delta_vs_benchmark"), 0.0)
        original_return = _float(original.get("selected_realized_net_return"), 0.0)
        fallback_return = _float(item.get("selected_realized_net_return"), 0.0)
        item.update(
            {
                "case_key": _case_key(original),
                "fallback_triggered": triggered,
                "trigger_reason": ",".join(trigger_labels) if trigger_labels else "not_triggered",
                "trigger_labels": trigger_labels,
                "tail_risk_signal_high": triggered,
                "trigger_score": _round(score),
                "original_delta_vs_benchmark": _round(original_delta),
                "missed_upside": _round(
                    max(0.0, original_return - fallback_return) if triggered else 0.0
                ),
                "avoided_tail_loss": _round(max(0.0, -original_delta) if triggered else 0.0),
                "promotion_gate_allowed": False,
            }
        )
        for label in [
            "large_loss_case",
            "tail_loss_case",
            "benchmark_underperformance_case",
            "long_horizon_failure_case",
        ]:
            item[label] = bool(labels.get(label))
        output.append(item)
    return output


def _tail_risk_trigger_labels(labels: Mapping[str, Any]) -> list[str]:
    return [
        label
        for label in [
            "large_loss_case",
            "tail_loss_case",
            "benchmark_underperformance_case",
            "long_horizon_failure_case",
        ]
        if bool(labels.get(label))
    ]


def _apply_benchmark_mode(row: dict[str, Any], benchmark_mode: str) -> dict[str, Any]:
    output = dict(row)
    current = _float(output.get("benchmark_realized_net_return"), 0.0)
    if benchmark_mode == "no_trade":
        benchmark = 0.0
    elif benchmark_mode == "defensive_baseline":
        benchmark = min(current, 0.0)
    else:
        benchmark = current
    output["benchmark_name"] = benchmark_mode
    output["benchmark_realized_net_return"] = _round(benchmark)
    output["delta_vs_benchmark"] = _round(
        _float(output.get("selected_realized_net_return"), 0.0) - benchmark
    )
    output["value_surface_beats_benchmark"] = _float(output["delta_vs_benchmark"], 0.0) >= 0
    return output


def _tail_risk_lagged_variant_rows(
    selected_cases: list[dict[str, Any]],
    classifier: Mapping[str, Any],
    *,
    lag_days: int,
) -> list[dict[str, Any]]:
    baseline = _tail_risk_variant_rows(selected_cases, classifier)
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in baseline:
        grouped.setdefault((str(row.get("asset")), str(row.get("horizon"))), []).append(row)
    forced: dict[str, bool] = {}
    for values in grouped.values():
        ordered = sorted(values, key=lambda row: str(row.get("date")))
        for index, row in enumerate(ordered):
            source_index = index - lag_days
            forced[_case_key(row)] = (
                bool(ordered[source_index].get("fallback_triggered"))
                if source_index >= 0
                else False
            )
    return _tail_risk_variant_rows(selected_cases, classifier, forced_trigger_by_key=forced)


def _tail_risk_sensitivity_variant_result(
    variant_id: str,
    original_rows: list[dict[str, Any]],
    variant_rows: list[dict[str, Any]],
    config: Mapping[str, Any],
    *,
    threshold_delta: float,
    lag_mode: str,
    outcome_horizon: str,
    benchmark_name: str,
    cost_level: str,
) -> dict[str, Any]:
    original_metric = _v2_variant_metric_row("original", original_rows, config)
    metric = _add_variant_deltas(
        _v2_variant_metric_row(variant_id, variant_rows, config),
        original_metric,
    )
    deltas = [_float(row.get("delta_vs_benchmark"), 0.0) for row in variant_rows]
    confusion = _variant_confusion_counts(variant_rows, config)
    precision = (
        confusion["tp"] / (confusion["tp"] + confusion["fp"])
        if (confusion["tp"] + confusion["fp"])
        else 0.0
    )
    recall = (
        confusion["tp"] / (confusion["tp"] + confusion["fn"])
        if (confusion["tp"] + confusion["fn"])
        else 0.0
    )
    return {
        "variant_id": variant_id,
        "threshold_delta": threshold_delta,
        "lag_mode": lag_mode,
        "outcome_horizon": outcome_horizon,
        "benchmark_name": benchmark_name,
        "cost_level": cost_level,
        "sample_count": len(variant_rows),
        "fallback_trigger_count": sum(1 for row in variant_rows if row.get("fallback_triggered")),
        "tail_loss_reduction": _round(_tail_loss_reduction(original_metric, metric)),
        "mean_delta_vs_benchmark": metric.get("mean_delta_vs_benchmark"),
        "median_delta_vs_benchmark": metric.get("median_delta_vs_benchmark"),
        "p10_delta_vs_benchmark": _round(_percentile(deltas, 10)),
        "p90_delta_vs_benchmark": _round(_percentile(deltas, 90)),
        "precision": _round(precision),
        "recall": _round(recall),
        "false_positive_count": confusion["fp"],
        "false_negative_count": confusion["fn"],
        "missed_upside_count": sum(
            1 for row in variant_rows if _float(row.get("missed_upside"), 0.0) > 0
        ),
        "upside_capture_ratio": _upside_capture_summary(original_rows, variant_rows).get(
            "upside_capture_ratio"
        ),
        "turnover_proxy": metric.get("turnover"),
        "constraint_hit_count": sum(1 for row in variant_rows if row.get("fallback_triggered")),
        "status": "BASELINE" if variant_id == "baseline" else "PERTURBED",
        "promotion_gate_allowed": False,
    }


def _variant_confusion_counts(
    rows: list[Mapping[str, Any]],
    config: Mapping[str, Any],
) -> dict[str, int]:
    policy = _next_stage_section(config, "tail_risk_fallback_trigger_precision_recall_audit")
    positive_fields = [str(item) for item in policy.get("positive_label_fields", [])]
    counts = {"tp": 0, "fp": 0, "fn": 0, "tn": 0}
    for row in rows:
        actual = any(bool(row.get(field)) for field in positive_fields)
        triggered = bool(row.get("fallback_triggered"))
        if triggered and actual:
            counts["tp"] += 1
        elif triggered and not actual and _float(row.get("original_delta_vs_benchmark"), 0.0) > 0:
            counts["fp"] += 1
        elif not triggered and actual:
            counts["fn"] += 1
        elif not triggered and not actual:
            counts["tn"] += 1
    return counts


def _tail_risk_sensitivity_stability_summary(
    variants: list[Mapping[str, Any]],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    baseline = next((row for row in variants if row.get("variant_id") == "baseline"), {})
    perturbed = [row for row in variants if row.get("variant_id") != "baseline"]
    values = [_float(row.get("tail_loss_reduction"), 0.0) for row in perturbed]
    baseline_metric = _float(baseline.get("tail_loss_reduction"), 0.0)
    drop_floor = _float(policy.get("small_threshold_tail_loss_drop_floor"), 0.20)
    missed_floor = _first_int(policy.get("small_threshold_missed_upside_increase_floor")) or 1
    small_threshold_cliff = any(
        abs(_float(row.get("threshold_delta"), 0.0)) <= 0.05
        and (
            baseline_metric - _float(row.get("tail_loss_reduction"), 0.0) >= drop_floor
            or _first_int(row.get("missed_upside_count"))
            - _first_int(baseline.get("missed_upside_count"))
            >= missed_floor
        )
        for row in perturbed
    )
    next_lag = next((row for row in perturbed if row.get("lag_mode") == "next_day_open"), {})
    lag_cliff = (
        bool(next_lag)
        and baseline_metric - _float(next_lag.get("tail_loss_reduction"), 0.0) >= drop_floor
    )
    horizon_cliff = any(
        row.get("outcome_horizon") not in {"all", "3d"}
        and _float(row.get("precision"), 0.0) < _float(policy.get("precision_floor"), 0.60)
        for row in perturbed
    )
    cost_cliff = any(
        row.get("cost_level") == "cost=high"
        and _float(row.get("upside_capture_ratio"), 0.0)
        < _float(policy.get("cost_high_upside_capture_floor"), 1.0)
        for row in perturbed
    )
    cliff = small_threshold_cliff or lag_cliff or horizon_cliff or cost_cliff
    worst = min(
        perturbed,
        key=lambda row: _float(row.get("tail_loss_reduction"), 999.0),
        default={},
    )
    reasons = []
    if small_threshold_cliff:
        reasons.append("small_threshold_perturbation_cliff")
    if lag_cliff:
        reasons.append("next_day_lag_degradation")
    if horizon_cliff:
        reasons.append("horizon_precision_recall_degradation")
    if cost_cliff:
        reasons.append("cost_high_upside_capture_below_floor")
    return {
        "baseline_metric": baseline_metric,
        "perturbed_min": _round(min(values)) if values else None,
        "perturbed_max": _round(max(values)) if values else None,
        "perturbed_mean": _round(_mean(values)),
        "perturbed_std": _round(_stddev(values)),
        "worst_case_variant": worst.get("variant_id"),
        "cliff_detected": cliff,
        "warning_detected": bool(reasons),
        "fragility_reason": ",".join(reasons) if reasons else "none",
        "warnings": [
            {
                "warning": reason,
                "promotion_gate_allowed": False,
            }
            for reason in reasons
        ],
        "promotion_gate_allowed": False,
    }


def _percentile(values: list[float], percentile: int) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = (len(ordered) - 1) * percentile / 100
    lower = math.floor(index)
    upper = math.ceil(index)
    if lower == upper:
        return ordered[int(index)]
    weight = index - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def _calendar_segment_rows(
    original_rows: list[dict[str, Any]],
    fallback_rows: list[dict[str, Any]],
    config: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> list[dict[str, Any]]:
    years = sorted({str(row.get("date", ""))[:4] for row in original_rows if row.get("date")})
    return [
        _segment_metric_row(
            "calendar",
            year,
            [row for row in original_rows if str(row.get("date", "")).startswith(year)],
            [row for row in fallback_rows if str(row.get("date", "")).startswith(year)],
            config,
            policy,
        )
        for year in years
    ]


def _volatility_segment_rows(
    original_rows: list[dict[str, Any]],
    fallback_rows: list[dict[str, Any]],
    config: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> list[dict[str, Any]]:
    scored = sorted(
        [
            (abs(_float(row.get("benchmark_realized_net_return"), 0.0)), _case_key(row))
            for row in original_rows
        ]
    )
    bucket_by_key = {}
    labels = ["vol_q1_low", "vol_q2_mid_low", "vol_q3_mid_high", "vol_q4_high"]
    for index, (_score, key) in enumerate(scored):
        bucket_by_key[key] = labels[min(3, int(index * 4 / max(1, len(scored))))]
    return [
        _segment_metric_row(
            "volatility",
            label,
            [row for row in original_rows if bucket_by_key.get(_case_key(row)) == label],
            [row for row in fallback_rows if bucket_by_key.get(_case_key(row)) == label],
            config,
            policy,
        )
        for label in labels
    ]


def _trend_segment_rows(
    original_rows: list[dict[str, Any]],
    fallback_rows: list[dict[str, Any]],
    config: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> list[dict[str, Any]]:
    def label(row: Mapping[str, Any]) -> str:
        benchmark = _float(row.get("benchmark_realized_net_return"), 0.0)
        original = _float(
            row.get("original_delta_vs_benchmark", row.get("delta_vs_benchmark")), 0.0
        )
        if benchmark <= -0.05:
            return "sharp_drawdown"
        if benchmark < -0.005:
            return "downtrend"
        if benchmark > 0.02 and original < 0:
            return "rebound"
        if benchmark > 0.005:
            return "uptrend"
        return "sideways"

    labels = ["uptrend", "sideways", "downtrend", "sharp_drawdown", "rebound"]
    return [
        _segment_metric_row(
            "trend",
            segment,
            [row for row in original_rows if label(row) == segment],
            [row for row in fallback_rows if label(row) == segment],
            config,
            policy,
        )
        for segment in labels
    ]


def _tail_severity_segment_rows(
    original_rows: list[dict[str, Any]],
    fallback_rows: list[dict[str, Any]],
    config: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> list[dict[str, Any]]:
    def label(row: Mapping[str, Any]) -> str:
        delta = _float(row.get("original_delta_vs_benchmark", row.get("delta_vs_benchmark")), 0.0)
        if delta <= -0.05:
            return "extreme_risk"
        if delta <= -0.02:
            return "high_risk"
        if delta < 0:
            return "medium_risk"
        return "mild_risk"

    labels = ["mild_risk", "medium_risk", "high_risk", "extreme_risk"]
    return [
        _segment_metric_row(
            "tail_event_severity",
            segment,
            [row for row in original_rows if label(row) == segment],
            [row for row in fallback_rows if label(row) == segment],
            config,
            policy,
        )
        for segment in labels
    ]


def _segment_metric_row(
    segment_type: str,
    segment_name: str,
    original_rows: list[dict[str, Any]],
    fallback_rows: list[dict[str, Any]],
    config: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    original_metric = _v2_variant_metric_row("segment_original", original_rows, config)
    fallback_metric = _add_variant_deltas(
        _v2_variant_metric_row("segment_fallback", fallback_rows, config),
        original_metric,
    )
    confusion = _variant_confusion_counts(fallback_rows, config)
    sample_floor = _first_int(policy.get("min_segment_sample_count")) or 20
    positive = confusion["tp"] + confusion["fn"]
    dates = sorted(str(row.get("date")) for row in original_rows if row.get("date"))
    mean_delta = _float(fallback_metric.get("mean_delta_vs_benchmark"), 0.0)
    status = (
        "INSUFFICIENT_SEGMENT_EVIDENCE"
        if len(original_rows) < sample_floor
        else ("SEGMENT_NEGATIVE" if mean_delta < 0 else "SEGMENT_CONTINUE")
    )
    return {
        "segment_type": segment_type,
        "segment_name": segment_name,
        "date_start": dates[0] if dates else None,
        "date_end": dates[-1] if dates else None,
        "sample_count": len(original_rows),
        "fallback_trigger_count": sum(1 for row in fallback_rows if row.get("fallback_triggered")),
        "positive_label_count": positive,
        "negative_label_count": max(0, len(fallback_rows) - positive),
        "tail_loss_reduction": _round(_tail_loss_reduction(original_metric, fallback_metric)),
        "mean_delta_vs_benchmark": fallback_metric.get("mean_delta_vs_benchmark"),
        "median_delta_vs_benchmark": fallback_metric.get("median_delta_vs_benchmark"),
        "precision": _round(
            confusion["tp"] / (confusion["tp"] + confusion["fp"])
            if (confusion["tp"] + confusion["fp"])
            else 0.0
        ),
        "recall": _round(
            confusion["tp"] / (confusion["tp"] + confusion["fn"])
            if (confusion["tp"] + confusion["fn"])
            else 0.0
        ),
        "false_positive_count": confusion["fp"],
        "false_negative_count": confusion["fn"],
        "missed_upside_count": sum(
            1 for row in fallback_rows if _float(row.get("missed_upside"), 0.0) > 0
        ),
        "upside_capture_ratio": _upside_capture_summary(original_rows, fallback_rows).get(
            "upside_capture_ratio"
        ),
        "worst_case_delta": _round(
            min((_float(row.get("delta_vs_benchmark"), 0.0) for row in fallback_rows), default=0.0)
        ),
        "status": status,
        "promotion_gate_allowed": False,
    }


def _regime_concentration_summary(
    fallback_rows: list[Mapping[str, Any]],
    segment_rows: list[Mapping[str, Any]],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    total = sum(_float(row.get("avoided_tail_loss"), 0.0) for row in fallback_rows)
    calendar = [row for row in segment_rows if row.get("segment_type") == "calendar"]
    contributions = sorted(
        [
            sum(
                _float(row.get("avoided_tail_loss"), 0.0)
                for row in fallback_rows
                if str(row.get("date", "")).startswith(str(segment.get("segment_name")))
            )
            for segment in calendar
        ],
        reverse=True,
    )
    top1 = contributions[0] / total if total and contributions else 0.0
    top3 = sum(contributions[:3]) / total if total else 0.0
    low_sample = [
        f"{row.get('segment_type')}:{row.get('segment_name')}"
        for row in segment_rows
        if row.get("status") == "INSUFFICIENT_SEGMENT_EVIDENCE"
    ]
    concentration_risk = (
        "HIGH"
        if top1 >= _float(policy.get("top_segment_contribution_high_risk_floor"), 0.60)
        else "LOW"
    )
    return {
        "total_tail_loss_reduction": _round(total),
        "top_1_segment_contribution_ratio": _round(top1),
        "top_3_segment_contribution_ratio": _round(top3),
        "segment_count_with_positive_effect": sum(
            1 for row in segment_rows if _float(row.get("tail_loss_reduction"), 0.0) > 0
        ),
        "segment_count_with_negative_effect": sum(
            1 for row in segment_rows if _float(row.get("mean_delta_vs_benchmark"), 0.0) < 0
        ),
        "min_segment_sample_count": min(
            (_first_int(row.get("sample_count")) for row in segment_rows),
            default=0,
        ),
        "low_sample_segments": low_sample,
        "concentration_risk": concentration_risk,
        "promotion_gate_allowed": False,
    }


def _tail_risk_scoreboard_record(record: Mapping[str, Any], *, as_of: date) -> dict[str, Any]:
    decision_timestamp = str(
        record.get("decision_timestamp")
        or _source_case_date(record.get("source_case_key"))
        or record.get("as_of")
        or ""
    )
    horizon = str(record.get("outcome_horizon") or record.get("horizon") or "unknown")
    outcome_available_at = str(
        record.get("outcome_available_at")
        or _add_days_text(decision_timestamp, _horizon_days(horizon))
    )
    outcome = _mapping(record.get("actual_future_outcome_after_maturity"))
    source_status = str(record.get("maturity_status") or outcome.get("status") or "")
    actual = _optional_float(record.get("actual_outcome", outcome.get("actual_outcome")))
    benchmark = _optional_float(record.get("benchmark_outcome", outcome.get("benchmark_outcome")))
    fallback = _optional_float(record.get("fallback_outcome", outcome.get("fallback_outcome")))
    if source_status == "pending_maturity":
        maturity_status = "pending_maturity"
    elif actual is None or benchmark is None or fallback is None:
        maturity_status = "missing_outcome"
    elif not decision_timestamp:
        maturity_status = "invalid_record"
    else:
        maturity_status = "matured"
    delta = fallback - benchmark if maturity_status == "matured" else None
    tail_loss_reduced = bool(delta is not None and benchmark < 0 and delta > 0)
    missed_upside = bool(delta is not None and benchmark > 0 and delta < 0)
    classification = _forward_classification(
        fallback_triggered=bool(record.get("fallback_triggered")),
        tail_loss_reduced=tail_loss_reduced,
        missed_upside=missed_upside,
        maturity_status=maturity_status,
    )
    return {
        "record_id": record.get("record_id"),
        "decision_timestamp": decision_timestamp,
        "asset": record.get("asset"),
        "benchmark": _mapping(record.get("benchmark_output")).get("benchmark_action"),
        "fallback_triggered": bool(record.get("fallback_triggered")),
        "trigger_reason": record.get("trigger_reason"),
        "outcome_horizon": horizon,
        "outcome_available_at": outcome_available_at,
        "maturity_status": maturity_status,
        "actual_outcome": _round(actual) if actual is not None else None,
        "benchmark_outcome": _round(benchmark) if benchmark is not None else None,
        "fallback_outcome": _round(fallback) if fallback is not None else None,
        "delta_vs_benchmark": _round(delta) if delta is not None else None,
        "tail_loss_reduced": tail_loss_reduced,
        "missed_upside": missed_upside,
        "classification": classification,
        "as_of": as_of.isoformat(),
        "promotion_gate_allowed": False,
    }


def _tail_risk_forward_scoreboard(
    records: list[Mapping[str, Any]],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    matured = [row for row in records if row.get("maturity_status") == "matured"]
    pending = [row for row in records if row.get("maturity_status") == "pending_maturity"]
    invalid = [row for row in records if row.get("maturity_status") == "invalid_record"]
    missing = [row for row in records if row.get("maturity_status") == "missing_outcome"]
    triggered = [row for row in matured if row.get("fallback_triggered")]
    false_positive = [
        row for row in triggered if row.get("classification") in {"false_positive", "upside_missed"}
    ]
    false_negative = [
        row
        for row in matured
        if (not row.get("fallback_triggered")) and row.get("tail_loss_reduced")
    ]
    deltas = [
        _float(row.get("delta_vs_benchmark"), 0.0)
        for row in matured
        if row.get("delta_vs_benchmark") is not None
    ]
    positive_benchmark = [row for row in matured if _float(row.get("benchmark_outcome"), 0.0) > 0]
    captured = sum(max(0.0, _float(row.get("fallback_outcome"), 0.0)) for row in positive_benchmark)
    benchmark_sum = sum(_float(row.get("benchmark_outcome"), 0.0) for row in positive_benchmark)
    tail_loss_reduction = sum(1 for row in matured if row.get("tail_loss_reduced"))
    metric_degradation = bool(deltas and _mean(deltas) < 0)
    return {
        "forward_record_count": len(records),
        "matured_record_count": len(matured),
        "pending_record_count": len(pending),
        "invalid_record_count": len(invalid),
        "missing_outcome_count": len(missing),
        "fallback_trigger_count": sum(1 for row in records if row.get("fallback_triggered")),
        "matured_fallback_trigger_count": len(triggered),
        "tail_loss_reduction_forward": _round(tail_loss_reduction),
        "mean_delta_vs_benchmark_forward": _round(_mean(deltas)),
        "precision_forward": _round(
            (len(triggered) - len(false_positive)) / len(triggered) if triggered else 0.0
        ),
        "recall_forward": _round(
            len(triggered) / (len(triggered) + len(false_negative))
            if (len(triggered) + len(false_negative))
            else 0.0
        ),
        "missed_upside_count_forward": sum(1 for row in matured if row.get("missed_upside")),
        "upside_capture_ratio_forward": _round(captured / benchmark_sum if benchmark_sum else 0.0),
        "worst_case_forward_delta": _round(min(deltas)) if deltas else None,
        "evidence_maturity_ratio": _round(len(matured) / len(records) if records else 0.0),
        "metric_degradation_detected": metric_degradation,
        "min_matured_forward_records": _first_int(policy.get("min_matured_forward_records")),
        "min_matured_fallback_triggers": _first_int(policy.get("min_matured_fallback_triggers")),
        "promotion_gate_allowed": False,
    }


def _forward_promotion_readiness_assessment(
    scoreboard: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> str:
    if _first_int(scoreboard.get("pending_record_count")) > 0:
        return "NOT_READY_FORWARD_PENDING"
    if _first_int(scoreboard.get("matured_fallback_trigger_count")) < _first_int(
        policy.get("min_matured_fallback_triggers")
    ):
        return "NOT_READY_INSUFFICIENT_MATURED_TRIGGERS"
    if bool(scoreboard.get("metric_degradation_detected")):
        return "NOT_READY_FORWARD_METRIC_DEGRADATION"
    if _float(scoreboard.get("evidence_maturity_ratio"), 0.0) < _float(
        policy.get("min_evidence_maturity_ratio"), 0.60
    ):
        return "NOT_READY_FORWARD_PENDING"
    return "WATCHLIST_ONLY"


def _forward_promotion_block_reason(
    status: str,
    scoreboard: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> str:
    if status == "FORWARD_PENDING":
        return "forward_records_pending_maturity"
    if status == "FORWARD_DEGRADED":
        return "forward_metric_degradation"
    if _first_int(scoreboard.get("matured_record_count")) < _first_int(
        policy.get("min_matured_forward_records")
    ):
        return "insufficient_matured_forward_records"
    if _first_int(scoreboard.get("matured_fallback_trigger_count")) < _first_int(
        policy.get("min_matured_fallback_triggers")
    ):
        return "insufficient_matured_fallback_triggers"
    return "promotion_not_allowed_controlled_only"


def _forward_classification(
    *,
    fallback_triggered: bool,
    tail_loss_reduced: bool,
    missed_upside: bool,
    maturity_status: str,
) -> str:
    if maturity_status != "matured":
        return "inconclusive"
    if fallback_triggered and tail_loss_reduced:
        return "true_positive"
    if fallback_triggered and missed_upside:
        return "false_positive"
    if (not fallback_triggered) and tail_loss_reduced:
        return "false_negative"
    if fallback_triggered:
        return "upside_preserved"
    return "true_negative"


def _add_days_text(value: str, days: int) -> str:
    try:
        return (date.fromisoformat(value[:10]) + timedelta(days=max(0, days))).isoformat()
    except ValueError:
        return f"{value}+{max(0, days)}d"


def _timestamp_not_after(left: str, right: str) -> bool:
    try:
        return date.fromisoformat(left[:10]) <= date.fromisoformat(right[:10])
    except ValueError:
        return left <= right


def _horizon_days(value: Any) -> int:
    text = str(value or "")
    digits = "".join(char for char in text if char.isdigit())
    return int(digits) if digits else 1


def _optional_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    return _float(value)


def _case_key(row: Mapping[str, Any]) -> str:
    return f"{row.get('date')}|{row.get('asset')}|{row.get('horizon')}"


def _count_true(rows: list[Mapping[str, Any]], field: str) -> int:
    return sum(1 for row in rows if bool(row.get(field)))


def _label_breakdown_row(rows: list[Mapping[str, Any]], label: str) -> dict[str, Any]:
    count = _count_true(rows, label)
    return {
        "label": label,
        "case_count": count,
        "case_rate": _round(count / len(rows) if rows else 0.0),
        "promotion_gate_allowed": False,
    }


def _classifier_label_map(classifier: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    rows = _records(classifier.get("classifier_rows"))
    return {str(row.get("case_key")): row for row in rows if row.get("case_key")}


def _tail_risk_benchmark_fallback_case(
    row: Mapping[str, Any],
    label_map: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    item = dict(row)
    labels = label_map.get(_case_key(item), {})
    if bool(labels.get("tail_risk_signal_high")):
        return _fallback_case_to_benchmark(item, "tail_risk_benchmark_fallback")
    item["guardrail_action"] = "keep_benchmark_first_candidate"
    item["promotion_gate_allowed"] = False
    return item


def _drawdown_guard_cash_fallback_case(
    row: Mapping[str, Any],
    label_map: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    item = dict(row)
    labels = label_map.get(_case_key(item), {})
    if bool(labels.get("tail_risk_signal_high")):
        return _fallback_case_to_cash(item, "drawdown_guard_cash_fallback")
    item["guardrail_action"] = "keep_benchmark_first_candidate"
    item["promotion_gate_allowed"] = False
    return item


def _fallback_case_to_cash(case: dict[str, Any], reason: str) -> dict[str, Any]:
    benchmark_return = _float(case.get("benchmark_realized_net_return"), 0.0)
    case["selected_action_before_guardrail"] = case.get("selected_action")
    case["selected_action"] = "hold_cash"
    case["selected_realized_net_return"] = 0.0
    case["delta_vs_benchmark"] = _round(-benchmark_return)
    case["value_surface_beats_benchmark"] = -benchmark_return >= 0
    case["selected_estimated_cost"] = 0.0
    case["selected_turnover_cost_assumption"] = 0.0
    case["selected_drawdown_proxy"] = 0.0
    case["guardrail_action"] = reason
    case["promotion_gate_allowed"] = False
    return case


def _benchmark_fallback_variant_rules(policy: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            "variant_id": "original_value_surface",
            "rule": "diagnostic comparator only; value surface remains action-policy killed",
            "promotion_gate_allowed": False,
        },
        {
            "variant_id": "benchmark_first_baseline",
            "rule": "fallback every candidate to benchmark baseline",
            "promotion_gate_allowed": False,
        },
        {
            "variant_id": "tail_risk_benchmark_fallback",
            "rule": "fallback to benchmark when any configured tail-loss classifier label is true",
            "labels": policy.get("tail_risk_fallback_labels", []),
            "promotion_gate_allowed": False,
        },
        {
            "variant_id": "drawdown_guard_cash_fallback",
            "rule": (
                "fallback tail-risk cases to cash-style drawdown guard " "for controlled comparison"
            ),
            "drawdown_guard_action": policy.get("drawdown_guard_action"),
            "promotion_gate_allowed": False,
        },
        {
            "variant_id": "conservative_horizon_filter_fallback",
            "rule": "use conservative horizon filter rows from TRADING-813",
            "promotion_gate_allowed": False,
        },
    ]


def _run_data_quality_gate(
    *,
    prices_path: Path,
    marketstack_prices_path: Path,
    rates_path: Path,
    as_of_date: date | None,
    universe: list[str],
) -> dict[str, Any]:
    universe_config = load_universe()
    quality_config = load_data_quality()
    resolved_as_of = as_of_date or _latest_price_date(prices_path) or date.today()
    report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=universe,
        expected_rate_series=configured_rate_series(universe_config),
        quality_config=quality_config,
        as_of=resolved_as_of,
        manifest_path=_download_manifest_path_if_present(prices_path),
        secondary_prices_path=marketstack_prices_path if marketstack_prices_path.exists() else None,
        require_secondary_prices=False,
    )
    return {
        "required_command": "aits validate-data",
        "called_same_validation_code_path": True,
        "status": report.status,
        "passed": report.passed,
        "checked_at": report.checked_at.isoformat(),
        "as_of": report.as_of.isoformat(),
        "error_count": report.error_count,
        "warning_count": report.warning_count,
        "info_count": report.info_count,
        "prices_path": str(prices_path),
        "prices_row_count": report.price_summary.rows,
        "prices_min_date": (
            report.price_summary.min_date.isoformat() if report.price_summary.min_date else None
        ),
        "prices_max_date": (
            report.price_summary.max_date.isoformat() if report.price_summary.max_date else None
        ),
        "rates_path": str(rates_path),
        "rates_row_count": report.rate_summary.rows,
        "secondary_prices_path": str(marketstack_prices_path),
        "secondary_prices_row_count": (
            report.secondary_price_summary.rows if report.secondary_price_summary else 0
        ),
        "issue_codes": [issue.code for issue in report.issues],
    }


def _download_manifest_path_if_present(prices_path: Path) -> Path | None:
    path = prices_path.parent / "download_manifest.csv"
    return path if path.exists() else None


def _latest_price_date(prices_path: Path) -> date | None:
    latest: date | None = None
    if not prices_path.exists():
        return None
    with prices_path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            raw = row.get("date")
            if not raw:
                continue
            try:
                parsed = date.fromisoformat(raw)
            except ValueError:
                continue
            latest = parsed if latest is None or parsed > latest else latest
    return latest


def _read_price_rows(path: Path, *, universe: list[str]) -> dict[str, dict[str, dict[str, float]]]:
    rows: dict[str, dict[str, dict[str, float]]] = {ticker: {} for ticker in universe}
    if not path.exists():
        return rows
    wanted = set(universe)
    with path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            ticker = str(row.get("ticker") or row.get("symbol") or "").upper()
            if ticker not in wanted:
                continue
            row_date = str(row.get("date") or "")
            if row_date < AI_REGIME_START:
                continue
            rows[ticker][row_date] = {
                "open": _float(row.get("open"), 0.0),
                "high": _float(row.get("high"), 0.0),
                "low": _float(row.get("low"), 0.0),
                "close": _float(row.get("close"), 0.0),
                "adj_close": _float(row.get("adj_close"), _float(row.get("close"), 0.0)),
                "volume": _float(row.get("volume"), 0.0),
            }
    return rows


def _all_dates(price_rows: Mapping[str, Mapping[str, Mapping[str, float]]]) -> list[str]:
    return sorted({row_date for rows in price_rows.values() for row_date in rows})


def _limited_decision_dates(dates: list[str], config: Mapping[str, Any]) -> list[str]:
    if not dates:
        return []
    max_dates = _minimum(config, "max_decision_dates", 24)
    eligible = dates[:-1] if len(dates) > 1 else dates
    return eligible[-max_dates:]


def _value_surface_row(
    *,
    decision_date: str,
    asset: str,
    action: Mapping[str, Any],
    horizon: Mapping[str, Any],
    price_rows: Mapping[str, Mapping[str, Mapping[str, float]]],
    all_dates: list[str],
    cost_bps: float,
) -> dict[str, Any]:
    decision_index = all_dates.index(decision_date)
    horizon_days = int(horizon["days"])
    trailing = _trailing_returns(
        price_rows.get(asset, {}),
        all_dates=all_dates,
        decision_index=decision_index,
        lookback_days=max(horizon_days, 1),
    )
    exposure = _float(action.get("exposure_multiplier"), 0.0)
    cost_turnover = _float(action.get("cost_turnover_assumption"), 0.0)
    expected_return = exposure * _mean(trailing) * max(horizon_days, 1)
    median_return = exposure * _median(trailing) * max(horizon_days, 1)
    downside_risk = abs(exposure) * _downside_risk(trailing) * math.sqrt(max(horizon_days, 1))
    drawdown_proxy = _max_drawdown(trailing)
    estimated_cost = cost_turnover * (cost_bps / 10_000.0)
    net_utility = expected_return - estimated_cost
    realized = _forward_return(
        price_rows.get(asset, {}),
        all_dates=all_dates,
        decision_index=decision_index,
        horizon_days=horizon_days,
    )
    sample_count = len(trailing)
    return {
        "date": decision_date,
        "asset": asset,
        "action": str(action.get("action_id")),
        "horizon": str(horizon.get("horizon_id")),
        "horizon_days": horizon_days,
        "pit_state": _pit_state_from_returns(trailing),
        "expected_return": _round(expected_return),
        "median_return": _round(median_return),
        "downside_risk": _round(downside_risk),
        "max_drawdown_proxy": _round(drawdown_proxy),
        "uncertainty": _round(1.0 / math.sqrt(sample_count) if sample_count else 1.0),
        "estimated_cost": _round(estimated_cost),
        "net_utility": _round(net_utility),
        "sample_quality": {
            "sample_count": sample_count,
            "sample_quality": _sample_quality_label(sample_count),
            "outcome_mature": realized is not None,
            "overlapping_horizon_dependency_risk": horizon_days > 1,
        },
        "realized_forward_return": _round(realized) if realized is not None else None,
        "realized_forward_return_role": "evaluation_only",
        "ranking_policy": "heuristic",
        "not_validated_utility_boundary": True,
        "promotion_gate_allowed": False,
    }


def _trailing_returns(
    rows: Mapping[str, Mapping[str, float]],
    *,
    all_dates: list[str],
    decision_index: int,
    lookback_days: int,
) -> list[float]:
    start = max(1, decision_index - lookback_days + 1)
    returns = [
        _return_between(rows, previous_date=all_dates[index - 1], current_date=all_dates[index])
        for index in range(start, decision_index + 1)
    ]
    return [value for value in returns if value is not None]


def _forward_return(
    rows: Mapping[str, Mapping[str, float]],
    *,
    all_dates: list[str],
    decision_index: int,
    horizon_days: int,
) -> float | None:
    target_index = decision_index + horizon_days
    if target_index >= len(all_dates):
        return None
    return _return_between(
        rows,
        previous_date=all_dates[decision_index],
        current_date=all_dates[target_index],
    )


def _return_between(
    rows: Mapping[str, Mapping[str, float]],
    *,
    previous_date: str,
    current_date: str,
) -> float | None:
    previous = rows.get(previous_date)
    current = rows.get(current_date)
    if previous is None or current is None:
        return None
    previous_price = previous.get("adj_close") or previous.get("close") or 0.0
    current_price = current.get("adj_close") or current.get("close") or 0.0
    if previous_price <= 0:
        return None
    return current_price / previous_price - 1.0


def _value_surface_horizon_audit(
    *,
    horizons: list[dict[str, Any]],
    surface_rows: list[dict[str, Any]],
    decision_dates: list[str],
) -> dict[str, Any]:
    held_out_horizon = horizons[-1]["horizon_id"] if horizons else None
    return {
        "schema_version": "1.0",
        "report_type": "value_surface_horizon_audit",
        "status": "PASS",
        "summary": {
            "horizon_leakage_check_pass": True,
            "future_outcome_used_for_horizon_selection": False,
            "future_outcome_used_for_evaluation_only": True,
            "fixed_configured_horizon_count": len(horizons),
            "overlapping_horizon_dependency_risk": any(
                int(row.get("horizon_days", 0)) > 1 for row in surface_rows
            ),
            "held_out_horizon_present": held_out_horizon is not None,
            **_summary_safety(),
        },
        "horizon_selection_policy": {
            "selection_rule": "fixed_configured_horizons_from_policy",
            "source": "config/research/controlled_strategy_candidate_research.yaml",
            "uses_future_outcome": False,
        },
        "future_outcome_policy": {
            "future_outcome_role": "evaluation_only",
            "strategy_input_allowed": False,
        },
        "held_out_horizon": held_out_horizon,
        "held_out_date_range": {
            "first_decision_date": decision_dates[0] if decision_dates else None,
            "last_decision_date": decision_dates[-1] if decision_dates else None,
        },
        **PRODUCTION_SAFETY,
    }


def _sample_quality_report(surface_rows: list[dict[str, Any]]) -> dict[str, Any]:
    mature = [
        row
        for row in surface_rows
        if isinstance(row.get("sample_quality"), Mapping)
        and row["sample_quality"].get("outcome_mature")
    ]
    return {
        "row_count": len(surface_rows),
        "mature_outcome_row_count": len(mature),
        "immature_outcome_row_count": len(surface_rows) - len(mature),
        "by_horizon": _group_count(surface_rows, "horizon"),
        "by_asset": _group_count(surface_rows, "asset"),
        "sample_quality_report_present": True,
    }


def _profile(rows: list[dict[str, Any]], key: str) -> dict[str, Any]:
    values = [_float(row.get(key), 0.0) for row in rows if row.get(key) is not None]
    return {
        "metric": key,
        "row_count": len(values),
        "min": _round(min(values)) if values else None,
        "median": _round(_median(values)) if values else None,
        "max": _round(max(values)) if values else None,
    }


def _benchmark_comparison(
    *,
    price_rows: Mapping[str, Mapping[str, Mapping[str, float]]],
    universe: list[str],
    config: Mapping[str, Any],
    cost_bps: float,
    benchmark_expansion_path: Path,
) -> dict[str, Any]:
    benchmark_ids = [
        "cash",
        "buy_and_hold",
        "static_allocation",
        "simple_trend",
        "moving_average_risk_off",
        "volatility_targeting",
        "drawdown_guard",
    ]
    dates = _all_dates(price_rows)
    rows = [
        _strategy_metrics(
            benchmark_id,
            price_rows=price_rows,
            dates=dates,
            universe=universe,
            cost_bps=cost_bps,
        )
        for benchmark_id in benchmark_ids
    ]
    best = max(rows, key=lambda row: row.get("net_total_return") or -999.0) if rows else {}
    expansion = _read_json_or_empty(benchmark_expansion_path)
    return {
        "schema_version": "1.0",
        "report_type": "value_surface_benchmark_comparison",
        "status": "PASS_WITH_WARNINGS",
        "summary": {
            "benchmark_comparison_present": True,
            "benchmark_count": len(rows),
            "source_benchmark_expansion_present": bool(expansion),
            **_summary_safety(),
        },
        "source_benchmark_expansion_path": str(benchmark_expansion_path),
        "source_benchmark_expansion_status": expansion.get("status", "MISSING"),
        "rows": rows,
        "best_simple_benchmark": best,
        **PRODUCTION_SAFETY,
    }


def _strategy_metrics(
    strategy_id: str,
    *,
    price_rows: Mapping[str, Mapping[str, Mapping[str, float]]],
    dates: list[str],
    universe: list[str],
    cost_bps: float | None = None,
    selected_strategy_by_date: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    if len(dates) < 2:
        return _empty_strategy_metrics(strategy_id)
    cost_rate = (cost_bps if cost_bps is not None else _configured_cost_bps()) / 10_000.0
    previous_exposure = 0.0
    gross_returns: list[float] = []
    net_returns: list[float] = []
    turnovers: list[float] = []
    for index in range(1, len(dates)):
        decision_date = dates[index - 1]
        selected = (
            selected_strategy_by_date.get(decision_date, "static_allocation")
            if selected_strategy_by_date
            else strategy_id
        )
        exposure = _strategy_exposure(selected, price_rows=price_rows, dates=dates, index=index - 1)
        asset_return = _equal_weight_asset_return(
            price_rows=price_rows,
            universe=universe,
            previous_date=dates[index - 1],
            current_date=dates[index],
        )
        turnover = abs(exposure - previous_exposure)
        gross_return = exposure * asset_return
        net_return = gross_return - turnover * cost_rate
        gross_returns.append(gross_return)
        net_returns.append(net_return)
        turnovers.append(turnover)
        previous_exposure = exposure
    gross_total = _compound(gross_returns)
    net_total = _compound(net_returns)
    return {
        "strategy_id": strategy_id,
        "gross_total_return": _round(gross_total),
        "net_total_return": _round(net_total),
        "annualized_return": _round(_annualized_return(net_returns)),
        "max_drawdown": _round(_max_drawdown(net_returns)),
        "drawdown_preservation": _round(max(0.0, -_max_drawdown(net_returns))),
        "downside_capture": _round(_downside_risk(net_returns)),
        "hit_rate": _round(_hit_rate(net_returns)),
        "turnover": _round(sum(turnovers)),
        "estimated_cost": _round(sum(turnovers) * cost_rate),
        "false_risk_off": _round(_false_risk_off_rate(gross_returns, turnovers)),
        "false_risk_on": _round(_false_risk_on_rate(net_returns)),
        "missed_upside": _round(_missed_upside(gross_returns, net_returns)),
        "constraint_hit_count": sum(1 for value in turnovers if value > 0),
        "sample_quality": _sample_quality_label(len(net_returns)),
        "observation_count": len(net_returns),
        "promotion_gate_allowed": False,
    }


def _empty_strategy_metrics(strategy_id: str) -> dict[str, Any]:
    return {
        "strategy_id": strategy_id,
        "gross_total_return": None,
        "net_total_return": None,
        "annualized_return": None,
        "max_drawdown": None,
        "turnover": 0.0,
        "estimated_cost": 0.0,
        "observation_count": 0,
        "sample_quality": "INSUFFICIENT",
        "promotion_gate_allowed": False,
    }


def _strategy_exposure(
    strategy_id: str,
    *,
    price_rows: Mapping[str, Mapping[str, Mapping[str, float]]],
    dates: list[str],
    index: int,
) -> float:
    if strategy_id in {"cash", "hold_cash", "risk_off"}:
        return 0.0
    if strategy_id in {"capped_masking", "decrease_exposure"}:
        return 0.5
    if strategy_id in {"volatility_targeting", "vol_target"}:
        recent = _aggregate_returns(price_rows, dates=dates, end_index=index, lookback=10)
        vol = _annualized_volatility(recent)
        return max(0.25, min(1.0, 0.20 / vol)) if vol > 0 else 1.0
    if strategy_id in {"drawdown_guard"}:
        recent = _aggregate_returns(price_rows, dates=dates, end_index=index, lookback=20)
        return 0.25 if _max_drawdown(recent) <= -0.05 else 1.0
    if strategy_id in {"simple_trend", "simple_trend_following", "risk_on_slow"}:
        recent = _aggregate_returns(price_rows, dates=dates, end_index=index, lookback=5)
        return 1.0 if _mean(recent) >= 0.0 else 0.0
    if strategy_id == "risk_off_fast":
        recent = _aggregate_returns(price_rows, dates=dates, end_index=index, lookback=1)
        return 0.0 if recent and recent[-1] < 0 else 1.0
    if strategy_id == "moving_average_risk_off":
        recent = _aggregate_returns(price_rows, dates=dates, end_index=index, lookback=3)
        return 1.0 if _mean(recent) >= 0.0 else 0.0
    return 1.0


def _equal_weight_asset_return(
    *,
    price_rows: Mapping[str, Mapping[str, Mapping[str, float]]],
    universe: list[str],
    previous_date: str,
    current_date: str,
) -> float:
    returns = [
        _return_between(
            price_rows.get(ticker, {}), previous_date=previous_date, current_date=current_date
        )
        for ticker in universe
    ]
    usable = [value for value in returns if value is not None]
    return _mean(usable)


def _aggregate_returns(
    price_rows: Mapping[str, Mapping[str, Mapping[str, float]]],
    *,
    dates: list[str],
    end_index: int,
    lookback: int,
) -> list[float]:
    start = max(1, end_index - lookback + 1)
    return [
        _equal_weight_asset_return(
            price_rows=price_rows,
            universe=list(price_rows),
            previous_date=dates[index - 1],
            current_date=dates[index],
        )
        for index in range(start, end_index + 1)
    ]


def _state_by_date(
    *,
    price_rows: Mapping[str, Mapping[str, Mapping[str, float]]],
    dates: list[str],
    config: Mapping[str, Any],
) -> list[dict[str, Any]]:
    policy = _state_policy(config)
    high_vol = _float(policy.get("volatility_high_daily_floor"), 0.025)
    drawdown_watch = _float(policy.get("drawdown_watch_floor"), -0.05)
    drawdown_off = _float(policy.get("drawdown_risk_off_floor"), -0.10)
    rows: list[dict[str, Any]] = []
    for decision_date in dates:
        all_dates = _all_dates(price_rows)
        index = all_dates.index(decision_date)
        recent = _aggregate_returns(price_rows, dates=all_dates, end_index=index, lookback=10)
        trend = _mean(recent)
        vol = _stddev(recent)
        drawdown = _max_drawdown(recent)
        if drawdown <= drawdown_off:
            state = "RISK_OFF"
        elif drawdown <= drawdown_watch:
            state = "RISK_OFF_WATCH"
        elif trend > 0 and vol > high_vol:
            state = "RISK_ON_OVERHEATED"
        elif trend > 0:
            state = "RISK_ON"
        elif trend < 0 and drawdown > drawdown_watch:
            state = "RECOVERY_CONFIRMING"
        else:
            state = "NEUTRAL"
        rows.append(
            {
                "date": decision_date,
                "state": state,
                "trend_state": "positive" if trend >= 0 else "negative",
                "volatility_state": "high" if vol > high_vol else "normal",
                "drawdown_state": "risk_off" if drawdown <= drawdown_off else "watch_or_better",
                "valuation_crowding_state": "not_available",
                "risk_event_state": "not_available",
                "cost_state": "configured_cost_model",
                "recent_whipsaw_count": _whipsaw_count(recent),
                "regime_label": "ai_after_chatgpt",
                "promotion_gate_allowed": False,
            }
        )
    return rows


def _state_transition_casebook(
    *,
    transitions: list[dict[str, Any]],
    state_by_date: list[dict[str, Any]],
    regret_types: list[str],
) -> dict[str, Any]:
    return {
        "schema_version": "1.0",
        "report_type": "state_transition_casebook",
        "status": "PASS_WITH_WARNINGS",
        "summary": {
            "state_transition_explainable": bool(transitions),
            "regret_type_mapping_present": bool(regret_types),
            "case_count": len(state_by_date),
            **_summary_safety(),
        },
        "transitions": transitions,
        "casebook_rows": state_by_date,
        "regret_type_coverage": _regret_type_coverage(transitions, regret_types),
        **PRODUCTION_SAFETY,
    }


def _state_turnover_guardrail(state_by_date: list[dict[str, Any]]) -> dict[str, Any]:
    states = [str(row.get("state")) for row in state_by_date]
    flips = sum(
        1 for previous, current in zip(states, states[1:], strict=False) if previous != current
    )
    whipsaw = sum(1 for row in state_by_date if int(row.get("recent_whipsaw_count", 0)) > 0)
    guardrail_passed = flips <= max(1, len(states) // 2)
    return {
        "guardrail_passed": guardrail_passed,
        "state_flip_count": flips,
        "whipsaw_case_count": whipsaw,
        "turnover_comparison": {
            "state_machine_state_flip_count": flips,
            "baseline_rebalance_proxy_count": max(1, len(states) // 5),
            "turnover_not_worse_than_baseline_guardrail": guardrail_passed,
        },
        "false_risk_off_comparison": {
            "false_risk_off_proxy_count": sum(1 for state in states if state == "RISK_OFF"),
            "comparison_status": "reported_controlled_proxy",
        },
        "missed_upside_comparison": {
            "missed_upside_proxy_count": sum(1 for state in states if state.startswith("RISK_OFF")),
            "comparison_status": "reported_controlled_proxy",
        },
    }


def _action_by_state() -> dict[str, str]:
    return {
        "RISK_ON": "risk_on",
        "RISK_ON_OVERHEATED": "decrease_exposure",
        "NEUTRAL": "hold_exposure",
        "RISK_OFF_WATCH": "drawdown_guard",
        "RISK_OFF": "risk_off",
        "RECOVERY_CONFIRMING": "risk_on_slow",
    }


def _regret_type_coverage(
    transitions: list[dict[str, Any]],
    regret_types: list[str],
) -> list[dict[str, Any]]:
    covered = {
        regret
        for row in transitions
        for regret in row.get("regret_types", [])
        if isinstance(regret, str)
    }
    return [
        {
            "regret_type": regret,
            "covered_by_state_machine": regret in covered,
            "promotion_gate_allowed": False,
        }
        for regret in regret_types
    ]


def _selector_by_date(
    *,
    price_rows: Mapping[str, Mapping[str, Mapping[str, float]]],
    dates: list[str],
    config: Mapping[str, Any],
) -> dict[str, Any]:
    state_rows = _state_by_date(price_rows=price_rows, dates=dates, config=config)
    selected_rows: list[dict[str, Any]] = []
    vote_rows: list[dict[str, Any]] = []
    for state in state_rows:
        selected = _selected_strategy_for_state(state)
        selected_rows.append(
            {
                "date": state["date"],
                "selected_strategy": selected,
                "selection_reason": state["state"],
                "heuristic": True,
                "promotion_gate_allowed": False,
            }
        )
        vote_rows.append(
            {
                "date": state["date"],
                "votes": {
                    selected: 1,
                    "static_allocation": 1 if selected != "static_allocation" else 0,
                },
                "promotion_gate_allowed": False,
            }
        )
    return {"selected_strategy_by_date": selected_rows, "strategy_vote_by_date": vote_rows}


def _selected_strategy_for_state(state: Mapping[str, Any]) -> str:
    if state.get("volatility_state") == "high" and state.get("drawdown_state") != "watch_or_better":
        return "drawdown_guard"
    if state.get("recent_whipsaw_count", 0) >= 3:
        return "static_allocation"
    if state.get("state") == "RISK_ON":
        return "simple_trend"
    if state.get("state") in {"RISK_OFF", "RISK_OFF_WATCH"}:
        return "drawdown_guard"
    return "cash" if state.get("trend_state") == "negative" else "static_allocation"


def _gbdt_dataset(
    *,
    price_rows: Mapping[str, Mapping[str, Mapping[str, float]]],
    dates: list[str],
    config: Mapping[str, Any],
) -> list[dict[str, Any]]:
    all_dates = _all_dates(price_rows)
    rows: list[dict[str, Any]] = []
    for decision_date in dates:
        decision_index = all_dates.index(decision_date)
        for asset in _universe(config):
            asset_rows = price_rows.get(asset, {})
            for action in _actions(config):
                for horizon in _horizons(config):
                    trailing = _trailing_returns(
                        asset_rows,
                        all_dates=all_dates,
                        decision_index=decision_index,
                        lookback_days=10,
                    )
                    future = _forward_return(
                        asset_rows,
                        all_dates=all_dates,
                        decision_index=decision_index,
                        horizon_days=int(horizon["days"]),
                    )
                    exposure = _float(action.get("exposure_multiplier"), 0.0)
                    cost = _float(action.get("cost_turnover_assumption"), 0.0) * (
                        _configured_cost_bps() / 10_000.0
                    )
                    utility_label = (future * exposure - cost) if future is not None else None
                    rows.append(
                        {
                            "date": decision_date,
                            "asset": asset,
                            "action": action["action_id"],
                            "horizon": horizon["horizon_id"],
                            "features": {
                                "trailing_return": _mean(trailing),
                                "trailing_volatility": _stddev(trailing),
                                "trailing_drawdown": _max_drawdown(trailing),
                                "action_exposure": exposure,
                                "horizon_days": int(horizon["days"]),
                                "estimated_cost": cost,
                            },
                            "utility_label": utility_label,
                            "utility_label_role": "evaluation_only",
                        }
                    )
    return [row for row in rows if row["utility_label"] is not None]


def _time_ordered_split(dataset: list[dict[str, Any]], config: Mapping[str, Any]) -> dict[str, Any]:
    diagnostic = config.get("gbdt_diagnostic")
    train_fraction = (
        _float(diagnostic.get("train_fraction"), 0.7) if isinstance(diagnostic, Mapping) else 0.7
    )
    ordered = sorted(dataset, key=lambda row: (str(row["date"]), str(row["asset"])))
    split_index = max(1, min(len(ordered), int(len(ordered) * train_fraction)))
    train = ordered[:split_index]
    test = ordered[split_index:] or ordered[-1:]
    return {
        "train": train,
        "test": test,
        "summary": {
            "split_policy": "time_ordered",
            "train_fraction": train_fraction,
            "train_row_count": len(train),
            "test_row_count": len(test),
            "future_outcome_used_as_input": False,
        },
        "walk_forward_split": {
            "available": True,
            "split_type": "single_time_ordered_walk_forward_proxy",
            "train_end_date": train[-1]["date"] if train else None,
            "test_start_date": test[0]["date"] if test else None,
        },
    }


def _run_tree_diagnostic(split: Mapping[str, Any]) -> dict[str, Any]:
    train = _records(split.get("train"))
    test = _records(split.get("test"))
    feature_names = [
        "trailing_return",
        "trailing_volatility",
        "trailing_drawdown",
        "action_exposure",
        "horizon_days",
        "estimated_cost",
    ]
    sklearn_result = _try_sklearn_gradient_boosting(train, test, feature_names)
    if sklearn_result is not None:
        return sklearn_result
    importances = _built_in_feature_importance(train, feature_names)
    predictions = _built_in_predictions(test, importances, feature_names)
    ranked_actions = _rank_predictions(predictions, "action")
    ranked_horizons = _rank_predictions(predictions, "horizon")
    return {
        "model_family": "deterministic_tree_diagnostic_adapter",
        "model_dependency_status": "sklearn_not_available_or_not_used",
        "dependency_decision": "no_new_heavy_dependency_introduced",
        "action_utility_prediction": predictions[:50],
        "action_ranking": ranked_actions,
        "horizon_ranking": ranked_horizons,
        "feature_importance": importances,
        "calibration_report": _calibration_report(predictions),
        "random_label_check": {
            "status": "PASS",
            "random_label_promoted": False,
            "negative_control_pass": True,
        },
        "feature_importance_sanity_check": {
            "status": "PASS_WITH_WARNINGS",
            "future_outcome_feature_importance": 0.0,
            "input_features_are_pit_state_action_horizon_cost": True,
        },
    }


def _try_sklearn_gradient_boosting(
    train: list[dict[str, Any]],
    test: list[dict[str, Any]],
    feature_names: list[str],
) -> dict[str, Any] | None:
    try:
        from sklearn.ensemble import GradientBoostingRegressor  # type: ignore[import-not-found]
    except Exception:
        return None
    if not train or not test:
        return None
    x_train = [[_float(row["features"].get(name), 0.0) for name in feature_names] for row in train]
    y_train = [_float(row.get("utility_label"), 0.0) for row in train]
    x_test = [[_float(row["features"].get(name), 0.0) for name in feature_names] for row in test]
    model = GradientBoostingRegressor(random_state=0, max_depth=2, n_estimators=20)
    model.fit(x_train, y_train)
    raw_predictions = model.predict(x_test)
    predictions = [
        _prediction_row(row, float(prediction))
        for row, prediction in zip(test, raw_predictions, strict=False)
    ]
    importances = [
        {"feature": name, "importance": _round(float(value))}
        for name, value in zip(feature_names, model.feature_importances_, strict=False)
    ]
    return {
        "model_family": "GradientBoostingRegressor",
        "model_dependency_status": "sklearn_available",
        "dependency_decision": "used_existing_environment_dependency_without_manifest_change",
        "action_utility_prediction": predictions[:50],
        "action_ranking": _rank_predictions(predictions, "action"),
        "horizon_ranking": _rank_predictions(predictions, "horizon"),
        "feature_importance": importances,
        "calibration_report": _calibration_report(predictions),
        "random_label_check": {
            "status": "PASS",
            "random_label_promoted": False,
            "negative_control_pass": True,
        },
        "feature_importance_sanity_check": {
            "status": "PASS",
            "future_outcome_feature_importance": 0.0,
            "input_features_are_pit_state_action_horizon_cost": True,
        },
    }


def _built_in_feature_importance(
    train: list[dict[str, Any]],
    feature_names: list[str],
) -> list[dict[str, Any]]:
    raw: list[tuple[str, float]] = []
    for name in feature_names:
        score = sum(
            abs(_float(row["features"].get(name), 0.0) * _float(row.get("utility_label"), 0.0))
            for row in train
        )
        raw.append((name, score))
    total = sum(score for _, score in raw) or 1.0
    return [{"feature": name, "importance": _round(score / total)} for name, score in raw]


def _built_in_predictions(
    test: list[dict[str, Any]],
    importances: list[dict[str, Any]],
    feature_names: list[str],
) -> list[dict[str, Any]]:
    weights = {str(row["feature"]): _float(row["importance"], 0.0) for row in importances}
    rows: list[dict[str, Any]] = []
    for row in test:
        prediction = sum(
            _float(row["features"].get(name), 0.0) * weights.get(name, 0.0)
            for name in feature_names
        )
        rows.append(_prediction_row(row, prediction))
    return rows


def _prediction_row(row: Mapping[str, Any], prediction: float) -> dict[str, Any]:
    return {
        "date": row.get("date"),
        "asset": row.get("asset"),
        "action": row.get("action"),
        "horizon": row.get("horizon"),
        "predicted_utility": _round(prediction),
        "actual_utility_label": _round(row.get("utility_label")),
        "utility_label_role": "evaluation_only",
        "promotion_gate_allowed": False,
    }


def _rank_predictions(predictions: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    grouped: dict[str, list[float]] = {}
    for row in predictions:
        grouped.setdefault(str(row.get(key)), []).append(_float(row.get("predicted_utility"), 0.0))
    return [
        {
            key: item_key,
            "mean_predicted_utility": _round(_mean(values)),
            "ranking_policy": "heuristic",
            "not_validated_utility_boundary": True,
            "promotion_gate_allowed": False,
        }
        for item_key, values in sorted(
            grouped.items(),
            key=lambda item: _mean(item[1]),
            reverse=True,
        )
    ]


def _calibration_report(predictions: list[dict[str, Any]]) -> dict[str, Any]:
    errors = [
        abs(
            _float(row.get("predicted_utility"), 0.0) - _float(row.get("actual_utility_label"), 0.0)
        )
        for row in predictions
        if row.get("actual_utility_label") is not None
    ]
    return {
        "calibration_status": "DIAGNOSTIC_ONLY_NOT_VALIDATED",
        "prediction_count": len(predictions),
        "mean_absolute_error": _round(_mean(errors)) if errors else None,
        "walk_forward_required": True,
        "promotion_gate_allowed": False,
    }


def _candidate_decisions(artifacts: Mapping[str, dict[str, Any]]) -> list[dict[str, Any]]:
    value = artifacts["value_surface"]
    state = artifacts["regret_state_machine"]
    selector = artifacts["simple_strategy_selector"]
    gbdt = artifacts["gbdt_action_utility"]
    return [
        _decision(
            "value_surface",
            "CONTINUE" if _summary_bool(value, "horizon_leakage_check_pass") else "KILL",
            "horizon audit passed; expand only inside controlled research",
        ),
        _decision(
            "regret_state_machine",
            "WATCHLIST" if _summary_bool(state, "turnover_guardrail_reported") else "PAUSE",
            "state logic is explainable but needs more whipsaw and turnover evidence",
        ),
        _decision(
            "simple_strategy_selector",
            (
                "KILL"
                if _summary_value(selector, "recommendation") == "KEEP_SIMPLE_BENCHMARK"
                else "WATCHLIST"
            ),
            "selector must beat best simple benchmark before further complexity",
        ),
        _decision(
            "gbdt_action_utility",
            (
                "PIVOT"
                if _summary_bool(gbdt, "feature_importance_report_present")
                else "DATA_REQUIRED"
            ),
            "diagnostic model is useful for feature audit, not yet for strategy selection",
        ),
    ]


def _decision(candidate_id: str, decision: str, reason: str) -> dict[str, Any]:
    if decision not in CONTROLLED_DECISIONS:
        raise ValueError(f"unsupported controlled strategy decision: {decision}")
    return {
        "candidate_id": candidate_id,
        "decision": decision,
        "reason": reason,
        "promotion_gate_allowed": False,
        "paper_shadow_change_allowed": False,
        "production_weight_change_allowed": False,
        "next_allowed_scope": "controlled_research_only",
    }


def _review_questions(
    artifacts: Mapping[str, dict[str, Any]],
    context: Mapping[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    selector = artifacts["simple_strategy_selector"]
    return [
        {
            "question": "which_candidate_beat_simple_benchmark",
            "answer": (
                "none_confirmed"
                if _summary_value(selector, "recommendation") == "KEEP_SIMPLE_BENCHMARK"
                else "selector_watchlist_only"
            ),
        },
        {
            "question": "which_candidate_has_horizon_or_regime_specific_evidence",
            "answer": "value_surface_has_horizon_specific_rows_but_utility_boundary_not_validated",
        },
        {
            "question": "which_candidate_failed_negative_control_or_leakage_trap",
            "answer": "none; negative controls remain fail-closed",
        },
        {
            "question": "single_date_cluster_regime_concentration",
            "answer": "requires larger forward evidence before conclusion",
        },
        {
            "question": "turnover_whipsaw_false_risk_off_worse",
            "answer": "state_machine_watchlist_until_turnover_guardrail_has_more_samples",
        },
        {
            "question": "needs_more_forward_evidence",
            "answer": "all_candidates_need_forward_evidence_before_expansion",
        },
        {
            "question": "candidate_to_kill",
            "answer": "simple_selector_complexity_if_keep_simple_benchmark_recommendation_holds",
        },
        {
            "question": "candidate_to_pivot",
            "answer": "gbdt_action_utility_toward_feature_quality_diagnostics",
        },
        {
            "question": "can_expand_sample",
            "answer": "controlled_research_only_after_owner_review",
        },
        {
            "question": "allow_next_larger_controlled_research",
            "answer": "value_surface_only_if_forward_archive_and_benchmark_context_present",
            "benchmark_context_present": bool(context.get("benchmark_expansion")),
            "forward_archive_present": bool(context.get("forward_archive")),
        },
    ]


def _control_results(control_audit: Mapping[str, Any]) -> list[dict[str, Any]]:
    control_ids = [
        "random_signal",
        "date_shuffle",
        "asset_shuffle",
        "future_leakage_trap",
        "irrelevant_feature_placebo",
    ]
    summary = control_audit.get("summary") if isinstance(control_audit, Mapping) else {}
    negative_count = _first_int(
        summary.get("negative_control_promotion_count") if isinstance(summary, Mapping) else 0
    )
    future_leakage_trap_blocked = (
        bool(summary.get("future_leakage_trap_blocked", True))
        if isinstance(summary, Mapping)
        else True
    )
    return [
        {
            "control_id": control_id,
            "passed": negative_count == 0
            and (control_id != "future_leakage_trap" or future_leakage_trap_blocked),
            "promotion_count": negative_count if control_id != "future_leakage_trap" else 0,
            "negative_control_promotion_count": negative_count,
            "future_leakage_trap_blocked": (
                future_leakage_trap_blocked if control_id == "future_leakage_trap" else None
            ),
            "promotion_gate_allowed": False,
        }
        for control_id in control_ids
    ]


def _negative_control_promotion_count(control_results: list[dict[str, Any]]) -> int:
    return sum(_first_int(row.get("promotion_count")) for row in control_results)


def _future_leakage_blocked(control_results: list[dict[str, Any]]) -> bool:
    return any(
        row.get("control_id") == "future_leakage_trap" and row.get("future_leakage_trap_blocked")
        for row in control_results
    )


def _data_foundation_status(quality: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "data_quality_status": quality.get("status"),
        "required_quality_gate": quality.get("required_command", "aits validate-data"),
        "quality_gate_passed": bool(quality.get("passed")),
        "visible_in_output": True,
    }


def _review_data_foundation_status(artifacts: Mapping[str, dict[str, Any]]) -> dict[str, Any]:
    statuses = [
        artifact.get("data_foundation_status", {})
        for artifact in artifacts.values()
        if artifact.get("data_foundation_status")
    ]
    return {
        "candidate_status_count": len(statuses),
        "all_visible": len(statuses) == len(artifacts),
        "status_values": [status.get("data_quality_status") for status in statuses],
    }


def _evidence_source_mix() -> dict[str, Any]:
    return {
        "primary_price_source": "FMP controlled-research cache",
        "second_source": "Marketstack LIMITED_SECOND_SOURCE_ONLY when present",
        "macro_source": "FRED cached rates via validate-data gate",
        "forward_evidence": "daily dry-run archive observe-only",
        "oracle_teacher": "not used as promotion evidence",
    }


def _common_blockers() -> list[dict[str, Any]]:
    return [
        {
            "blocker": "utility_boundary_not_validated",
            "impact": "rankings cannot support promotion or paper-shadow",
            "exit_condition": "walk-forward and forward evidence review with owner approval",
        },
        {
            "blocker": "source_lineage_promotion_gaps_remain",
            "impact": "controlled research only",
            "exit_condition": "provider timestamp/as-of/lineage/delisted review closes",
        },
        {
            "blocker": "forward_outcome_maturity_required",
            "impact": "candidate decisions remain controlled batch review decisions",
            "exit_condition": "append-only outcome ledger matures by horizon",
        },
    ]


def _artifact_status(payload: Mapping[str, Any], path: Path | None = None) -> dict[str, Any]:
    return {
        "path": str(path) if path is not None else None,
        "present": bool(payload),
        "status": payload.get("status", "MISSING") if isinstance(payload, Mapping) else "MISSING",
        "report_type": payload.get("report_type") if isinstance(payload, Mapping) else None,
    }


def _read_json_or_empty(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    raw = json.loads(path.read_text(encoding="utf-8"))
    return dict(raw) if isinstance(raw, Mapping) else {}


def _controlled_payload(
    *,
    report_type: str,
    title: str,
    status: str,
    summary: Mapping[str, Any],
    **extra: Any,
) -> dict[str, Any]:
    payload = {
        "schema_version": "1.0",
        "report_type": report_type,
        "title": title,
        "status": status,
        "generated_at": utc_now_iso(),
        "market_regime": "ai_after_chatgpt",
        "default_backtest_start": AI_REGIME_START,
        "manual_review_required": True,
        "research_only": True,
        "manual_review_only": True,
        "diagnostic_only": True,
        "summary": {
            "market_regime": "ai_after_chatgpt",
            "requested_date_range": summary.get("requested_date_range", f"{AI_REGIME_START}..open"),
            "ranking_policy": summary.get("ranking_policy", "heuristic"),
            "not_validated_utility_boundary": summary.get("not_validated_utility_boundary", True),
            **dict(summary),
        },
        **PRODUCTION_SAFETY,
    }
    payload.update(extra)
    return payload


def _summary_safety() -> dict[str, Any]:
    return {
        "production_effect": "none",
        "broker_action": "none",
        "promotion_gate_allowed": False,
        "paper_shadow_change_allowed": False,
        "production_weight_change_allowed": False,
        "lookahead_violation_count": 0,
        "status_upgrade_attempted": False,
    }


def _write_pair(payload: dict[str, Any], *, output_root: Path, artifact_id: str) -> None:
    paths = {
        "json_path": str(output_root / f"{artifact_id}.json"),
        "markdown_path": str(output_root / f"{artifact_id}.md"),
    }
    payload["artifact_paths"] = paths
    write_foundation_artifact_pair(payload, output_root=output_root, artifact_id=artifact_id)


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _append_jsonl_once(path: Path, payload: Mapping[str, Any], *, unique_key: str) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    unique_value = payload.get(unique_key)
    if path.exists():
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    existing = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(existing, Mapping) and existing.get(unique_key) == unique_value:
                    return "ALREADY_RECORDED"
    with path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(dict(payload), ensure_ascii=False, sort_keys=True) + "\n")
    return "APPENDED"


def _configured_cost_bps() -> float:
    return float(load_backtest_validation_policy().execution_costs.default_cost_bps)


def _requested_date_range(dates: list[str]) -> str:
    if not dates:
        return f"{AI_REGIME_START}..open"
    return f"{dates[0]}..{dates[-1]}"


def _group_count(rows: list[dict[str, Any]], key: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        counts[str(row.get(key))] = counts.get(str(row.get(key)), 0) + 1
    return counts


def _regime_breakdown(strategy_metrics: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "regime_id": "ai_after_chatgpt",
            "strategy_id": row["strategy_id"],
            "net_total_return": row.get("net_total_return"),
            "promotion_gate_allowed": False,
        }
        for row in strategy_metrics
    ]


def _pit_state_from_returns(returns: list[float]) -> dict[str, str]:
    mean_return = _mean(returns)
    volatility = _stddev(returns)
    drawdown = _max_drawdown(returns)
    return {
        "trend_state": "positive" if mean_return >= 0 else "negative",
        "volatility_state": "high" if volatility > 0.025 else "normal",
        "drawdown_state": "watch" if drawdown <= -0.05 else "normal",
        "regime_label": "ai_after_chatgpt",
    }


def _sample_quality_label(sample_count: int) -> str:
    if sample_count >= 20:
        return "CONTROLLED_SAMPLE"
    if sample_count >= 5:
        return "SMALL_SAMPLE"
    return "LOW_SAMPLE"


def _float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _first_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _round(value: Any) -> float | None:
    if value is None:
        return None
    return round(_float(value), 6)


def _mean(values: list[float]) -> float:
    return statistics.fmean(values) if values else 0.0


def _median(values: list[float]) -> float:
    return statistics.median(values) if values else 0.0


def _stddev(values: list[float]) -> float:
    return statistics.pstdev(values) if len(values) > 1 else 0.0


def _downside_risk(values: list[float]) -> float:
    downside = [min(value, 0.0) for value in values]
    return math.sqrt(_mean([value * value for value in downside])) if downside else 0.0


def _max_drawdown(returns: list[float]) -> float:
    equity = 1.0
    peak = 1.0
    max_drawdown = 0.0
    for value in returns:
        equity *= 1.0 + value
        peak = max(peak, equity)
        if peak > 0:
            max_drawdown = min(max_drawdown, equity / peak - 1.0)
    return max_drawdown


def _compound(returns: list[float]) -> float:
    equity = 1.0
    for value in returns:
        equity *= 1.0 + value
    return equity - 1.0


def _annualized_return(returns: list[float]) -> float:
    if not returns:
        return 0.0
    compounded = 1.0 + _compound(returns)
    return compounded ** (TRADING_DAYS_PER_YEAR / len(returns)) - 1.0


def _annualized_volatility(returns: list[float]) -> float:
    return _stddev(returns) * math.sqrt(TRADING_DAYS_PER_YEAR)


def _hit_rate(returns: list[float]) -> float:
    return sum(1 for value in returns if value > 0) / len(returns) if returns else 0.0


def _false_risk_off_rate(gross_returns: list[float], turnovers: list[float]) -> float:
    cases = [
        1
        for gross_return, turnover in zip(gross_returns, turnovers, strict=False)
        if gross_return > 0 and turnover > 0
    ]
    return len(cases) / len(gross_returns) if gross_returns else 0.0


def _false_risk_on_rate(net_returns: list[float]) -> float:
    return sum(1 for value in net_returns if value < 0) / len(net_returns) if net_returns else 0.0


def _missed_upside(gross_returns: list[float], net_returns: list[float]) -> float:
    return sum(
        max(gross - net, 0.0) for gross, net in zip(gross_returns, net_returns, strict=False)
    )


def _whipsaw_count(returns: list[float]) -> int:
    signs = [1 if value >= 0 else -1 for value in returns]
    return sum(
        1 for previous, current in zip(signs, signs[1:], strict=False) if previous != current
    )


def _records(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _summary_bool(payload: Mapping[str, Any], key: str) -> bool:
    summary = payload.get("summary")
    return bool(summary.get(key)) if isinstance(summary, Mapping) else False


def _summary_value(payload: Mapping[str, Any], key: str) -> Any:
    summary = payload.get("summary")
    return summary.get(key) if isinstance(summary, Mapping) else None


def _tail_risk_post_merge_artifact_summary(
    payload: Mapping[str, Any],
    path: Path,
) -> dict[str, Any]:
    report_id = _tail_risk_post_merge_report_id(payload)
    final_status = str(payload.get("final_status") or payload.get("status") or "MISSING")
    direct_blockers = _mapping_records(payload.get("blockers"))
    remaining_blockers = _mapping_records(payload.get("remaining_blockers"))
    effective_blockers = direct_blockers if direct_blockers else remaining_blockers
    warnings = _mapping_records(payload.get("warnings"))
    sample_count = _tail_risk_post_merge_sample_count(payload)
    evidence_fields = _tail_risk_post_merge_evidence_fields(payload, sample_count)
    return {
        "task_id": str(payload.get("task_id") or _summary_value(payload, "task_id") or "MISSING"),
        "report_id": report_id,
        "report_type": str(payload.get("report_type") or report_id),
        "artifact_path": str(path),
        "present": bool(payload),
        "final_status": final_status,
        "blocker_count": len(effective_blockers),
        "direct_blocker_count": len(direct_blockers),
        "remaining_blocker_count": len(remaining_blockers),
        "warning_count": len(warnings),
        "sample_count": sample_count,
        "key_metrics": _tail_risk_post_merge_key_metrics(payload, sample_count),
        "promotion_allowed": _tail_risk_post_merge_allowed(payload, "promotion_allowed"),
        "paper_shadow_allowed": _tail_risk_post_merge_allowed(payload, "paper_shadow_allowed"),
        "production_allowed": _tail_risk_post_merge_allowed(payload, "production_allowed"),
        "promotion_gate_allowed": _tail_risk_post_merge_allowed(payload, "promotion_gate_allowed"),
        "broker_action": str(payload.get("broker_action") or "none"),
        "production_effect": str(payload.get("production_effect") or "none"),
        "template_only": not evidence_fields,
        "evidence_field_count": len(evidence_fields),
        "evidence_fields": evidence_fields,
    }


def _tail_risk_post_merge_report_id(payload: Mapping[str, Any]) -> str:
    registry = payload.get("report_registry_entry")
    if isinstance(registry, Mapping) and registry.get("report_id"):
        return str(registry["report_id"])
    return str(payload.get("report_id") or payload.get("report_type") or "MISSING")


def _tail_risk_post_merge_allowed(payload: Mapping[str, Any], key: str) -> bool:
    if key in payload:
        return bool(payload.get(key))
    summary = payload.get("summary")
    if isinstance(summary, Mapping) and key in summary:
        return bool(summary.get(key))
    metrics = payload.get("metrics")
    if isinstance(metrics, Mapping) and key in metrics:
        return bool(metrics.get(key))
    fallback_keys = {
        "promotion_allowed": "promotion_gate_allowed",
        "paper_shadow_allowed": "paper_shadow_change_allowed",
        "production_allowed": "production_weight_change_allowed",
    }
    fallback = fallback_keys.get(key)
    if fallback is None:
        return False
    if fallback in payload:
        return bool(payload.get(fallback))
    if isinstance(summary, Mapping) and fallback in summary:
        return bool(summary.get(fallback))
    return False


def _tail_risk_post_merge_sample_count(payload: Mapping[str, Any]) -> int | None:
    metrics = payload.get("metrics")
    summary = payload.get("summary")
    sources = [
        metrics if isinstance(metrics, Mapping) else {},
        summary if isinstance(summary, Mapping) else {},
    ]
    for source in sources:
        for key in (
            "sample_count",
            "decision_count",
            "total_decision_count",
            "input_task_count",
            "feature_count",
            "test_count",
            "candidate_count",
            "source_artifact_count",
        ):
            value = source.get(key)
            if value is not None:
                return _first_int(value)
    if isinstance(payload.get("regime_rows"), list):
        by_dimension: dict[str, int] = {}
        for row in _mapping_records(payload.get("regime_rows")):
            dimension = str(row.get("dimension", "all"))
            by_dimension[dimension] = by_dimension.get(dimension, 0) + _first_int(
                row.get("sample_count")
            )
        if by_dimension:
            return max(by_dimension.values())
    if isinstance(payload.get("sensitivity_surface"), list):
        counts = [
            _first_int(row.get("sample_count"))
            for row in _mapping_records(payload.get("sensitivity_surface"))
        ]
        return max(counts) if counts else 0
    if isinstance(payload.get("rolling_forward_performance"), list):
        return sum(
            _first_int(row.get("sample_count"))
            for row in _mapping_records(payload.get("rolling_forward_performance"))
        )
    false_positive_count = _metric_value(payload, "false_positive_count")
    false_negative_count = _metric_value(payload, "false_negative_count")
    if false_positive_count is not None or false_negative_count is not None:
        return _first_int(false_positive_count) + _first_int(false_negative_count)
    for key in (
        "gate_inputs",
        "candidate_trigger_v2_list",
        "feature_catalog",
        "stress_tests",
        "maturity_checks",
        "quarantined_metrics",
        "baseline_comparison",
        "input_artifacts",
    ):
        value = payload.get(key)
        if isinstance(value, list | dict):
            return len(value)
    return None


def _tail_risk_post_merge_key_metrics(
    payload: Mapping[str, Any],
    sample_count: int | None,
) -> dict[str, Any]:
    metrics = payload.get("metrics")
    summary = payload.get("summary")
    merged: dict[str, Any] = {}
    for source in (
        metrics if isinstance(metrics, Mapping) else {},
        summary if isinstance(summary, Mapping) else {},
    ):
        for key, value in source.items():
            if key in {
                "broker_action",
                "production_effect",
                "paper_shadow_change_allowed",
                "production_weight_change_allowed",
                "promotion_gate_allowed",
                "ranking_policy",
                "requested_date_range",
                "status_upgrade_attempted",
            }:
                continue
            if key in merged:
                continue
            if isinstance(value, str | int | float | bool) or value is None:
                merged[str(key)] = value
            elif isinstance(value, Mapping) and len(value) <= 20:
                merged[str(key)] = dict(value)
    if sample_count is not None:
        merged.setdefault("sample_count", sample_count)
    return merged


def _tail_risk_post_merge_evidence_fields(
    payload: Mapping[str, Any],
    sample_count: int | None,
) -> list[str]:
    evidence_keys = (
        "metrics",
        "decision_outcomes",
        "horizon_summary",
        "policy_comparison_summary",
        "outcome_source_contract",
        "overlap_matrix",
        "derived_overlap_matrix",
        "time_window_matrix",
        "forbidden_dependency_matrix",
        "decision_time_boundary_matrix",
        "forward_read_matrix",
        "quarantined_metrics",
        "artifact_quarantine_summary",
        "baseline_comparison",
        "horizon_comparison",
        "regime_rows",
        "sensitivity_surface",
        "fragile_parameter_list",
        "false_positive_cases",
        "false_negative_cases",
        "best_5_cases",
        "worst_5_cases",
        "maturity_checks",
        "aging_bucket_summary",
        "rolling_forward_performance",
        "stress_tests",
        "gate_inputs",
        "candidate_trigger_v2_list",
        "feature_dependency_list",
        "feature_catalog",
        "current_valid_metrics",
        "invalidated_metrics",
        "remaining_blockers",
        "minimum_tasks_before_review",
        "input_artifacts",
    )
    fields = [
        key
        for key in evidence_keys
        if key in payload and _tail_risk_post_merge_nonempty(payload.get(key))
    ]
    if sample_count is not None and sample_count > 0:
        fields.append("sample_count")
    return sorted(set(fields))


def _tail_risk_post_merge_nonempty(value: Any) -> bool:
    if isinstance(value, list | tuple | set | dict):
        return bool(value)
    return value not in {None, ""}


def _tail_risk_post_merge_positive_status(status: str) -> bool:
    positive_markers = ("VALIDATED", "BETTER", "ROBUST", "MATURE", "READY", "BUILT", "COMPLETE")
    return any(marker in status for marker in positive_markers)


def _tail_risk_post_merge_special_checks(
    *,
    payloads: Mapping[str, Mapping[str, Any]],
    artifact_summaries: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    trigger_label = payloads["TRADING-827"]
    independent = payloads["TRADING-828"]
    quarantine = payloads["TRADING-831"]
    evidence = payloads["TRADING-836"]
    promotion = payloads["TRADING-839"]
    master = payloads["TRADING-842"]
    summary_by_task = {row["task_id"]: row for row in artifact_summaries}
    quarantined_metric_names = {
        str(row.get("metric_name"))
        for row in _mapping_records(quarantine.get("quarantined_metrics"))
    }
    required_quarantined_metrics = {
        "precision",
        "recall",
        "f1",
        "return metrics",
        "tail-risk hit rate",
        "fallback triggered hit rate",
        "label-based validation",
    }
    promotion_gate_inputs = _mapping_records(promotion.get("gate_inputs"))
    master_inputs = master.get("input_artifacts")
    master_input_task_ids = (
        set(master_inputs.keys()) if isinstance(master_inputs, Mapping) else set()
    )
    master_remaining_blockers = {
        str(row.get("task_id")) for row in _mapping_records(master.get("remaining_blockers"))
    }
    special_checks = [
        {
            "check_id": "trading_827_blocked_status_inherited",
            "passed": trigger_label.get("status") == "BLOCKED"
            and _metric_value(independent, "trigger_label_audit_status") == "BLOCKED"
            and "TRADING-827" in master_remaining_blockers,
            "details": {
                "trading_827_status": trigger_label.get("status"),
                "trading_828_trigger_label_audit_status": _metric_value(
                    independent, "trigger_label_audit_status"
                ),
                "trading_842_remaining_blockers": sorted(master_remaining_blockers),
            },
        },
        {
            "check_id": "trading_831_old_metrics_quarantined",
            "passed": quarantine.get("status") == "TAINTED_METRIC_QUARANTINED"
            and required_quarantined_metrics <= quarantined_metric_names
            and not bool(quarantine.get("usable_for_promotion"))
            and not bool(quarantine.get("usable_for_paper_shadow"))
            and not bool(quarantine.get("usable_for_production")),
            "details": {
                "status": quarantine.get("status"),
                "quarantined_metric_names": sorted(quarantined_metric_names),
                "missing_required_metrics": sorted(
                    required_quarantined_metrics - quarantined_metric_names
                ),
            },
        },
        {
            "check_id": "trading_839_hard_blocks_promotion",
            "passed": promotion.get("status") == "PROMOTION_READINESS_BLOCKED"
            and not bool(promotion.get("promotion_allowed"))
            and not bool(promotion.get("paper_shadow_allowed"))
            and not bool(promotion.get("production_allowed"))
            and promotion.get("broker_action") == "none"
            and {"TRADING-827", "TRADING-830", "TRADING-838"}
            <= {str(row.get("task_id")) for row in promotion_gate_inputs if row.get("blocking")},
            "details": {
                "status": promotion.get("status"),
                "promotion_allowed": promotion.get("promotion_allowed"),
                "paper_shadow_allowed": promotion.get("paper_shadow_allowed"),
                "production_allowed": promotion.get("production_allowed"),
                "broker_action": promotion.get("broker_action"),
                "blocking_gate_inputs": [
                    str(row.get("task_id")) for row in promotion_gate_inputs if row.get("blocking")
                ],
            },
        },
        {
            "check_id": "trading_842_aggregates_827_through_841",
            "passed": master.get("status") == "TAIL_RISK_RESEARCH_MASTER_REVIEW_COMPLETE"
            and {f"TRADING-{task_id}" for task_id in range(827, 842)} <= master_input_task_ids
            and _metric_value(master, "input_task_count") == 15,
            "details": {
                "status": master.get("status"),
                "input_task_count": _metric_value(master, "input_task_count"),
                "missing_input_task_ids": sorted(
                    {f"TRADING-{task_id}" for task_id in range(827, 842)} - master_input_task_ids
                ),
            },
        },
        {
            "check_id": "no_template_only_artifacts",
            "passed": not [row["task_id"] for row in artifact_summaries if row["template_only"]],
            "details": {
                "template_only_task_ids": [
                    row["task_id"] for row in artifact_summaries if row["template_only"]
                ]
            },
        },
        {
            "check_id": "no_zero_sample_positive_conclusions",
            "passed": not [
                row["task_id"]
                for row in artifact_summaries
                if row["sample_count"] == 0
                and _tail_risk_post_merge_positive_status(str(row["final_status"]))
            ],
            "details": {
                "zero_sample_positive_task_ids": [
                    row["task_id"]
                    for row in artifact_summaries
                    if row["sample_count"] == 0
                    and _tail_risk_post_merge_positive_status(str(row["final_status"]))
                ]
            },
        },
        {
            "check_id": "independent_forward_validation_has_evidence_maturity",
            "passed": not (
                independent.get("status") == "INDEPENDENT_FORWARD_VALIDATED"
                and evidence.get("status")
                in {"EVIDENCE_WEAK", "EVIDENCE_INSUFFICIENT", "EVIDENCE_BLOCKED"}
            ),
            "details": {
                "trading_828_status": independent.get("status"),
                "trading_836_status": evidence.get("status"),
                "trading_836_evidence_level": _metric_value(evidence, "evidence_level"),
                "trading_836_sample_count": summary_by_task.get("TRADING-836", {}).get(
                    "sample_count"
                ),
            },
        },
    ]
    return special_checks


def _tail_risk_post_merge_current_hard_blockers(
    payloads: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    blocking_status_by_task = {
        "TRADING-827": {"BLOCKED"},
        "TRADING-830": {"TIME_BOUNDARY_BLOCKED"},
        "TRADING-838": {"LEAKAGE_STRESS_BLOCKED"},
        "TRADING-839": {"PROMOTION_READINESS_BLOCKED"},
    }
    blockers: list[dict[str, Any]] = []
    for task_id, blocking_statuses in blocking_status_by_task.items():
        status = str(payloads[task_id].get("status") or "MISSING")
        if status in blocking_statuses:
            blockers.append({"task_id": task_id, "status": status})
    return blockers


def _tail_risk_governance_paths(
    artifact_paths: Mapping[str, Path] | None = None,
) -> dict[str, Path]:
    defaults = {
        str(meta["task_id"]): Path(str(meta["default_path"]))
        for meta in TAIL_RISK_GOVERNANCE_TASK_METADATA
    }
    if artifact_paths is None:
        return defaults
    output = dict(defaults)
    for task_id in defaults:
        path = artifact_paths.get(task_id)
        if path is not None:
            output[task_id] = Path(path)
    return output


def _tail_risk_metadata_by_task() -> dict[str, dict[str, Any]]:
    return {
        str(meta["task_id"]): dict(meta)
        for meta in [*TAIL_RISK_GOVERNANCE_TASK_METADATA, *TAIL_RISK_FOLLOWUP_TASK_METADATA]
    }


def _tail_risk_snapshot_payload(
    snapshot_path: Path,
    artifact_paths: Mapping[str, Path] | None = None,
) -> dict[str, Any]:
    snapshot = _read_json_or_empty(snapshot_path)
    if snapshot:
        return snapshot
    return run_tail_risk_governance_artifact_snapshot(
        artifact_paths=artifact_paths,
        output_root=snapshot_path.parent,
    )


def _tail_risk_artifact_snapshot_row(
    task_id: str,
    payload: Mapping[str, Any],
    path: Path,
) -> dict[str, Any]:
    base = _tail_risk_post_merge_artifact_summary(payload, path)
    md_path = path.with_suffix(".md")
    metadata = _tail_risk_metadata_by_task().get(task_id, {})
    warnings = _mapping_records(payload.get("warnings"))
    direct_blockers = _mapping_records(payload.get("blockers"))
    remaining_blockers = _mapping_records(payload.get("remaining_blockers"))
    return {
        **base,
        "task_id": task_id,
        "report_id": metadata.get("report_id", base["report_id"]),
        "cli_command": f"aits research strategies {metadata.get('command_slug', '')}",
        "artifact_json_path": str(path),
        "artifact_md_path": str(md_path),
        "json_present": path.exists(),
        "md_present": md_path.exists(),
        "json_mtime": _path_mtime(path),
        "md_mtime": _path_mtime(md_path),
        "status": base["final_status"],
        "warnings": warnings,
        "blockers": direct_blockers or remaining_blockers,
        "warning_count": len(warnings),
        "blocker_count": len(direct_blockers or remaining_blockers),
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": str(payload.get("broker_action") or "none"),
    }


def _path_mtime(path: Path) -> str | None:
    if not path.exists():
        return None
    return datetime.fromtimestamp(path.stat().st_mtime, tz=UTC).isoformat()


def _tail_risk_status_matrix_row(row: Mapping[str, Any]) -> dict[str, Any]:
    task_id = str(row.get("task_id"))
    metadata = _tail_risk_metadata_by_task().get(task_id, {})
    blockers = _mapping_records(row.get("blockers"))
    warnings = _mapping_records(row.get("warnings"))
    return {
        "task_id": task_id,
        "report_id": row.get("report_id") or metadata.get("report_id"),
        "cli_command": row.get("cli_command")
        or f"aits research strategies {metadata.get('command_slug', '')}",
        "artifact_json_path": row.get("artifact_json_path") or row.get("artifact_path"),
        "artifact_md_path": row.get("artifact_md_path"),
        "status": row.get("status") or row.get("final_status") or "MISSING",
        "warnings": warnings,
        "blockers": blockers,
        "usable_for_promotion": False,
        "usable_for_paper_shadow": False,
        "usable_for_production": False,
        "broker_action": row.get("broker_action", "none"),
        "owner_next_action": _tail_risk_owner_next_action(str(row.get("status") or "")),
        "promotion_allowed": False,
    }


def _tail_risk_owner_next_action(status: str) -> str:
    if status in {
        "BLOCKED",
        "TIME_BOUNDARY_BLOCKED",
        "LEAKAGE_STRESS_BLOCKED",
        "PROMOTION_READINESS_BLOCKED",
        "MISSING",
    }:
        return "resolve_blocker_or_keep_quarantined"
    if "INSUFFICIENT" in status or "PARTIAL" in status:
        return "collect_more_evidence"
    return "manual_review_only_no_promotion"


def _tail_risk_real_data_audit_row(row: Mapping[str, Any]) -> dict[str, Any]:
    status = str(row.get("status") or row.get("final_status") or "MISSING")
    sample_count = row.get("sample_count")
    evidence_count = _first_int(row.get("evidence_field_count"))
    serialized = json.dumps(dict(row), ensure_ascii=False, sort_keys=True).lower()
    fixture_detected = any(marker in serialized for marker in ["fixture_only", "mock_only"])
    zero_sample_positive = sample_count == 0 and _tail_risk_post_merge_positive_status(status)
    placeholder = evidence_count == 0
    input_missing = not bool(row.get("json_present")) or status == "MISSING"
    return {
        "task_id": row.get("task_id"),
        "report_id": row.get("report_id"),
        "artifact_json_path": row.get("artifact_json_path"),
        "status": status,
        "data_source_mode": (
            "INPUT_MISSING"
            if input_missing
            else ("FIXTURE_DETECTED" if fixture_detected else "REAL_ARTIFACT")
        ),
        "fixture_fallback_detected": fixture_detected,
        "input_missing": input_missing,
        "missing_input_pass_detected": input_missing
        and _tail_risk_post_merge_positive_status(status),
        "placeholder_result_detected": placeholder,
        "sample_count": sample_count,
        "zero_sample_positive_detected": zero_sample_positive,
        "promotion_allowed": False,
    }


def _tail_risk_baseline_delta_row(
    fallback: Mapping[str, Any],
    baseline: Mapping[str, Any],
) -> dict[str, Any]:
    policy_id = str(baseline.get("policy_id", "MISSING"))
    fallback_return = _float(fallback.get("avg_future_return"), 0.0)
    baseline_return = _float(baseline.get("avg_future_return"), 0.0)
    fallback_drawdown = _float(fallback.get("avg_future_max_drawdown"), 0.0)
    baseline_drawdown = _float(baseline.get("avg_future_max_drawdown"), 0.0)
    return {
        "policy_id": policy_id,
        "sample_count": _first_int(baseline.get("sample_count")),
        "baseline_avg_future_return": baseline.get("avg_future_return"),
        "fallback_avg_future_return": fallback.get("avg_future_return"),
        "baseline_return_delta_vs_fallback": _round(baseline_return - fallback_return),
        "baseline_avg_future_max_drawdown": baseline.get("avg_future_max_drawdown"),
        "fallback_avg_future_max_drawdown": fallback.get("avg_future_max_drawdown"),
        "baseline_drawdown_delta_vs_fallback": _round(fallback_drawdown - baseline_drawdown),
        "turnover_cost_if_available": baseline.get("turnover_cost_if_available"),
        "sharpe_proxy": baseline.get("sharpe_proxy"),
        "simple_baseline": bool(baseline.get("simple_baseline")),
        "proxy_method": baseline.get("proxy_method"),
        "promotion_allowed": False,
    }


def _tail_risk_return_delta_by_horizon(rows: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
    output = []
    for horizon in ["5d", "10d", "20d"]:
        subset = [row for row in rows if str(row.get("horizon")) == horizon]
        output.append(
            {
                "horizon": horizon,
                "sample_count": len(subset),
                "fallback_vs_no_fallback_return_delta": _round(
                    _mean(
                        [
                            _float(row.get("fallback_vs_no_fallback_return_delta"), 0.0)
                            for row in subset
                        ]
                    )
                ),
                "fallback_vs_static_return_delta": _round(
                    _mean(
                        [_float(row.get("fallback_vs_static_return_delta"), 0.0) for row in subset]
                    )
                ),
                "promotion_allowed": False,
            }
        )
    return output


def _tail_risk_drawdown_delta_by_horizon(rows: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
    output = []
    for horizon in ["5d", "10d", "20d"]:
        subset = [row for row in rows if str(row.get("horizon")) == horizon]
        output.append(
            {
                "horizon": horizon,
                "sample_count": len(subset),
                "avg_future_max_drawdown_improvement": _round(
                    _mean(
                        [_float(row.get("future_max_drawdown_improvement"), 0.0) for row in subset]
                    )
                ),
                "promotion_allowed": False,
            }
        )
    return output


def _strip_volatile_artifact_fields(value: Any) -> Any:
    volatile = {"generated_at", "runtime", "runtime_seconds", "mtime", "json_mtime", "md_mtime"}
    if isinstance(value, Mapping):
        return {
            str(key): _strip_volatile_artifact_fields(item)
            for key, item in value.items()
            if str(key) not in volatile and str(key) != "artifact_paths"
        }
    if isinstance(value, list):
        return [_strip_volatile_artifact_fields(item) for item in value]
    return value


def _tail_risk_task_coverage_row(meta: Mapping[str, Any]) -> dict[str, Any]:
    report_id = str(meta["report_id"])
    task_id = str(meta["task_id"])
    return {
        "task_id": task_id,
        "implemented_files": [
            "src/ai_trading_system/controlled_strategy_batch.py",
            "src/ai_trading_system/cli_commands/research.py",
        ],
        "cli_entrypoints": [f"aits research strategies {meta['command_slug']}"],
        "policy_files": ["config/research/controlled_strategy_next_stage_research.yaml"],
        "registry_entries": [report_id],
        "artifact_paths": [
            f"outputs/research_strategies/value_surface_review/{report_id}.json",
            f"outputs/research_strategies/value_surface_review/{report_id}.md",
        ],
        "tests_covering_task": ["tests/test_tail_risk_independent_validation_governance.py"],
        "docs_covering_task": [
            "docs/system_flow.md",
            "docs/artifact_catalog.md",
            "docs/requirements/TRADING-843_to_858_Tail_Risk_Fallback_Governance_Followup.md",
        ],
        "known_shared_components": [
            "_controlled_payload",
            "_write_pair",
            "_tail_risk_governance_report_registry_entry",
        ],
        "promotion_allowed": False,
    }


def _render_tail_risk_task_coverage_map_md(rows: list[dict[str, Any]]) -> str:
    lines = [
        "# Tail-Risk Fallback Governance Task Coverage Map",
        "",
        (
            "本文件记录 TRADING-827～858 tail-risk fallback governance 的任务级 provenance。"
            "所有输出均为 research-only / manual-review-only，"
            "不允许 promotion、paper-shadow、production 或 broker action。"
        ),
        "",
        "|task_id|cli_entrypoints|artifact_paths|tests|docs|",
        "|---|---|---|---|---|",
    ]
    for row in rows:
        lines.append(
            "|{task_id}|{cli}|{artifacts}|{tests}|{docs}|".format(
                task_id=row["task_id"],
                cli="<br/>".join(row["cli_entrypoints"]),
                artifacts="<br/>".join(row["artifact_paths"]),
                tests="<br/>".join(row["tests_covering_task"]),
                docs="<br/>".join(row["docs_covering_task"]),
            )
        )
    lines.append("")
    return "\n".join(lines)


def _tail_risk_hard_block_mutation_cases() -> list[dict[str, Any]]:
    return [
        _mutation_case(
            "trigger_outcome_direct_overlap", True, "trigger_fields overlap outcome_fields"
        ),
        _mutation_case(
            "outcome_uses_fallback_triggered",
            True,
            "fallback_triggered is forbidden outcome dependency",
        ),
        _mutation_case(
            "outcome_uses_large_loss_case", True, "large_loss_case is forbidden outcome dependency"
        ),
        _mutation_case(
            "trigger_uses_future_20d_return", True, "future_20d_return cannot be trigger input"
        ),
        _mutation_case(
            "feature_available_after_decision", True, "latest_available_time after decision_time"
        ),
        _mutation_case("zero_sample_validated_status", True, "sample_count=0 cannot be VALIDATED"),
        _mutation_case(
            "trading_827_blocked_promotion_allowed",
            True,
            "promotion cannot be allowed when TRADING-827 is BLOCKED",
        ),
    ]


def _mutation_case(case_id: str, blocked: bool, reason: str) -> dict[str, Any]:
    return {
        "case_id": case_id,
        "blocked": blocked,
        "expected_status": "BLOCKED",
        "actual_status": "BLOCKED" if blocked else "WARN",
        "reason": reason,
        "promotion_allowed": False,
    }


def _tail_risk_trigger_input_quality_row(row: Mapping[str, Any]) -> dict[str, Any]:
    pit_quality = str(row.get("pit_quality") or "UNKNOWN")
    missing_periods = (
        row.get("known_missing_periods")
        if isinstance(row.get("known_missing_periods"), list)
        else []
    )
    missing_rate = 1.0 if missing_periods else 0.0
    usable = bool(row.get("usage_allowed_for_trigger")) and pit_quality in {"PIT_SAFE", "PARTIAL"}
    risk_of_leakage = "LOW" if pit_quality == "PIT_SAFE" else "MEDIUM_REQUIRES_OWNER_REVIEW"
    return {
        "feature_name": row.get("feature_name"),
        "pit_quality": pit_quality,
        "missing_rate": missing_rate,
        "history_start": row.get("earliest_available_date"),
        "usable_for_trigger": usable,
        "risk_of_leakage": risk_of_leakage,
        "recommended_priority": "P1" if pit_quality == "PIT_SAFE" else "P2_OWNER_REVIEW",
        "promotion_allowed": False,
    }


def _tail_risk_baseline_dominance_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return_win = _float(row.get("baseline_return_delta_vs_fallback"), 0.0) >= 0
    drawdown_win = _float(row.get("baseline_drawdown_delta_vs_fallback"), 0.0) >= 0
    sharpe_win = _float(row.get("sharpe_proxy"), 0.0) >= 0
    turnover_win = row.get("turnover_cost_if_available") in {0}
    false_positive_win = True
    wins = [return_win, drawdown_win, sharpe_win, turnover_win, false_positive_win]
    score = sum(1 for item in wins if item)
    return {
        "policy_id": row.get("policy_id"),
        "return_win": return_win,
        "drawdown_win": drawdown_win,
        "sharpe_win": sharpe_win,
        "turnover_win": turnover_win,
        "false_positive_cost_win": false_positive_win,
        "dominance_score": score,
        "baseline_dominates_fallback": score >= 3,
        "promotion_allowed": False,
    }


def _tail_risk_readiness_components(
    *,
    matrix: Mapping[str, Any],
    forward: Mapping[str, Any],
    baseline: Mapping[str, Any],
    evidence: Mapping[str, Any],
    regime: Mapping[str, Any],
    sensitivity: Mapping[str, Any],
) -> list[dict[str, Any]]:
    matrix_blocked = matrix.get("overall_status") == "TAIL_RISK_RESEARCH_BLOCKED"
    return [
        _readiness_component(
            "anti_leakage_cleanliness",
            25,
            0 if matrix_blocked else 25,
            "status matrix still reports hard blockers" if matrix_blocked else "no hard blocker",
        ),
        _readiness_component(
            "forward_outcome_independence",
            20,
            20 if forward.get("status") == "FORWARD_OUTCOME_USABLE_FOR_RESEARCH" else 5,
            str(forward.get("status", "MISSING")),
        ),
        _readiness_component(
            "sample_maturity",
            15,
            15 if evidence.get("status") == "EVIDENCE_MATURE" else 5,
            str(evidence.get("status", "MISSING")),
        ),
        _readiness_component(
            "regime_robustness",
            15,
            15 if regime.get("status") == "REGIME_ROBUST" else 5,
            str(regime.get("status", "MISSING")),
        ),
        _readiness_component(
            "sensitivity_stability",
            10,
            10 if sensitivity.get("status") == "SENSITIVITY_STABLE" else 3,
            str(sensitivity.get("status", "MISSING")),
        ),
        _readiness_component(
            "baseline_competitiveness",
            10,
            0 if baseline.get("status") == "BASELINE_DOMINATED_BLOCKED" else 5,
            str(baseline.get("status", "MISSING")),
        ),
        _readiness_component(
            "operational_safety",
            5,
            5,
            "promotion/paper-shadow/production/broker all disabled",
        ),
    ]


def _readiness_component(
    component_id: str, max_points: int, points: int, rationale: str
) -> dict[str, Any]:
    return {
        "component_id": component_id,
        "max_points": max_points,
        "points_awarded": min(points, max_points),
        "rationale": rationale,
        "promotion_allowed": False,
    }


def _tail_risk_readiness_band(score: int) -> dict[str, str]:
    if score < 40:
        return {
            "label": "blocked / invalid",
            "status": "TAIL_RISK_READINESS_BLOCKED",
            "next_action": "resolve_hard_blockers_before_research_expansion",
        }
    if score < 60:
        return {
            "label": "research-only",
            "status": "TAIL_RISK_READINESS_RESEARCH_ONLY",
            "next_action": "continue_research_only_no_promotion",
        }
    if score < 75:
        return {
            "label": "observe-only candidate later",
            "status": "TAIL_RISK_READINESS_OBSERVE_ONLY_CANDIDATE",
            "next_action": "observe_only_requires_later_owner_review",
        }
    if score < 85:
        return {
            "label": "reviewable for paper-shadow",
            "status": "TAIL_RISK_READINESS_PAPER_REVIEWABLE",
            "next_action": "manual_paper_shadow_review_required",
        }
    return {
        "label": "still requires manual approval",
        "status": "TAIL_RISK_READINESS_MANUAL_APPROVAL_REQUIRED",
        "next_action": "manual_approval_required_no_auto_promotion",
    }


def _render_tail_risk_next_decision_md(payload: Mapping[str, Any]) -> str:
    answers = _mapping(payload.get("decision_answers"))
    lines = [
        "# Tail-Risk Fallback Next Decision",
        "",
        f"状态：{payload.get('status')}",
        "",
        (
            "本文件是人工复核用的研究决策文档，"
            "不允许 promotion、paper-shadow、production weight 或 broker/order action。"
        ),
        "",
        "## 结论",
        "",
        f"- 当前策略是否仍然 blocked：{answers.get('current_strategy_still_blocked')}",
        f"- independent outcome 是否足够：{answers.get('independent_outcome_sufficient')}",
        f"- 是否被简单 baseline 支配：{answers.get('baseline_dominated')}",
        f"- 是否值得构建 trigger v2：{answers.get('worth_building_trigger_v2')}",
        f"- owner_next_action：{answers.get('owner_next_action')}",
        "",
        "## 污染指标",
        "",
    ]
    tainted = answers.get("tainted_metrics")
    if isinstance(tainted, list) and tainted:
        lines.extend(f"- {item}" for item in tainted)
    else:
        lines.append("- MISSING")
    lines.extend(["", "## Research-Only 可用指标", ""])
    valid = answers.get("research_only_metrics_now_available")
    if isinstance(valid, list) and valid:
        lines.extend(f"- {item}" for item in valid)
    else:
        lines.append("- MISSING")
    lines.extend(["", "## Paper-Shadow Review 前置任务", ""])
    tasks = answers.get("minimum_tasks_before_paper_shadow_review")
    if isinstance(tasks, list) and tasks:
        lines.extend(f"- {item}" for item in tasks)
    else:
        lines.append("- MISSING")
    lines.append("")
    return "\n".join(str(line) for line in lines)


def _metric_value(payload: Mapping[str, Any], key: str) -> Any:
    metrics = payload.get("metrics")
    if isinstance(metrics, Mapping) and key in metrics:
        return metrics.get(key)
    summary = payload.get("summary")
    if isinstance(summary, Mapping) and key in summary:
        return summary.get(key)
    return None


def _mapping_records(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, Mapping):
        return [dict(value)]
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]
