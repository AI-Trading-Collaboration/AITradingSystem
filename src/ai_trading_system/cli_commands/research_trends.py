from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from ai_trading_system.ai_leadership_actual_path_validation import (
    DEFAULT_DOCS_ROOT as DEFAULT_AI_LEADERSHIP_ACTUAL_PATH_DOCS_ROOT,
)
from ai_trading_system.ai_leadership_actual_path_validation import (
    DEFAULT_GENERATOR_ROOT as DEFAULT_AI_LEADERSHIP_ACTUAL_PATH_GENERATOR_ROOT,
)
from ai_trading_system.ai_leadership_actual_path_validation import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_AI_LEADERSHIP_ACTUAL_PATH_OUTPUT_ROOT,
)
from ai_trading_system.ai_leadership_actual_path_validation import (
    DEFAULT_POLICY_PATH as DEFAULT_AI_LEADERSHIP_ACTUAL_PATH_POLICY_PATH,
)
from ai_trading_system.ai_leadership_actual_path_validation import (
    MODE as AI_LEADERSHIP_ACTUAL_PATH_MODE,
)
from ai_trading_system.ai_leadership_actual_path_validation import (
    run_ai_leadership_actual_path_validation,
)
from ai_trading_system.ai_leadership_scope_review import (
    DEFAULT_DOCS_ROOT as DEFAULT_AI_LEADERSHIP_SCOPE_REVIEW_DOCS_ROOT,
)
from ai_trading_system.ai_leadership_scope_review import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_AI_LEADERSHIP_SCOPE_REVIEW_OUTPUT_ROOT,
)
from ai_trading_system.ai_leadership_scope_review import (
    DEFAULT_POLICY_PATH as DEFAULT_AI_LEADERSHIP_SCOPE_REVIEW_POLICY_PATH,
)
from ai_trading_system.ai_leadership_scope_review import (
    DEFAULT_VALIDATION_ROOT as DEFAULT_AI_LEADERSHIP_SCOPE_REVIEW_VALIDATION_ROOT,
)
from ai_trading_system.ai_leadership_scope_review import (
    MODE as AI_LEADERSHIP_SCOPE_REVIEW_MODE,
)
from ai_trading_system.ai_leadership_scope_review import (
    run_ai_leadership_scope_review,
)
from ai_trading_system.ai_semiconductor_leadership_feasibility_audit import (
    DEFAULT_DOCS_ROOT as DEFAULT_AI_SEMICONDUCTOR_LEADERSHIP_DOCS_ROOT,
)
from ai_trading_system.ai_semiconductor_leadership_feasibility_audit import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_AI_SEMICONDUCTOR_LEADERSHIP_OUTPUT_ROOT,
)
from ai_trading_system.ai_semiconductor_leadership_feasibility_audit import (
    MODE as AI_SEMICONDUCTOR_LEADERSHIP_MODE,
)
from ai_trading_system.ai_semiconductor_leadership_feasibility_audit import (
    run_ai_semiconductor_leadership_feasibility_audit,
)
from ai_trading_system.ai_semiconductor_leadership_generator_poc import (
    DEFAULT_DOCS_ROOT as DEFAULT_AI_SEMICONDUCTOR_LEADERSHIP_POC_DOCS_ROOT,
)
from ai_trading_system.ai_semiconductor_leadership_generator_poc import (
    DEFAULT_FEASIBILITY_ROOT as DEFAULT_AI_SEMICONDUCTOR_LEADERSHIP_POC_FEASIBILITY_ROOT,
)
from ai_trading_system.ai_semiconductor_leadership_generator_poc import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_AI_SEMICONDUCTOR_LEADERSHIP_POC_OUTPUT_ROOT,
)
from ai_trading_system.ai_semiconductor_leadership_generator_poc import (
    DEFAULT_POLICY_PATH as DEFAULT_AI_SEMICONDUCTOR_LEADERSHIP_POC_POLICY_PATH,
)
from ai_trading_system.ai_semiconductor_leadership_generator_poc import (
    MODE as AI_SEMICONDUCTOR_LEADERSHIP_POC_MODE,
)
from ai_trading_system.ai_semiconductor_leadership_generator_poc import (
    run_ai_semiconductor_leadership_generator_poc,
)
from ai_trading_system.baseline_frozen_composer_rewrap import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_CANDIDATE_SIGNAL_BINDING_SCHEMA_OUTPUT_ROOT,
)
from ai_trading_system.baseline_frozen_composer_rewrap import (
    DEFAULT_SOURCE_PREDICTIONS_PATH as DEFAULT_CANDIDATE_SIGNAL_BINDING_SOURCE_PATH,
)
from ai_trading_system.baseline_frozen_composer_rewrap import (
    run_candidate_signal_binding_schema_poc,
)
from ai_trading_system.breadth_current_constituents_proxy_diagnostics import (
    DEFAULT_DOCS_ROOT as DEFAULT_CURRENT_CONSTITUENTS_BREADTH_PROXY_DOCS_ROOT,
)
from ai_trading_system.breadth_current_constituents_proxy_diagnostics import (
    DEFAULT_FEASIBILITY_ROOT as DEFAULT_CURRENT_CONSTITUENTS_BREADTH_PROXY_FEASIBILITY_ROOT,
)
from ai_trading_system.breadth_current_constituents_proxy_diagnostics import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_CURRENT_CONSTITUENTS_BREADTH_PROXY_OUTPUT_ROOT,
)
from ai_trading_system.breadth_current_constituents_proxy_diagnostics import (
    MODE as CURRENT_CONSTITUENTS_BREADTH_PROXY_MODE,
)
from ai_trading_system.breadth_current_constituents_proxy_diagnostics import (
    run_current_constituents_breadth_proxy_diagnostics,
)
from ai_trading_system.breadth_participation_feasibility_audit import (
    DEFAULT_DOCS_ROOT as DEFAULT_BREADTH_FEASIBILITY_DOCS_ROOT,
)
from ai_trading_system.breadth_participation_feasibility_audit import (
    MODE as BREADTH_FEASIBILITY_MODE,
)
from ai_trading_system.breadth_participation_feasibility_audit import (
    run_breadth_participation_feasibility_audit,
)
from ai_trading_system.breadth_proxy_signal_concept_selection import (
    DEFAULT_DIAGNOSTICS_ROOT as DEFAULT_BREADTH_PROXY_SIGNAL_SELECTION_DIAGNOSTICS_ROOT,
)
from ai_trading_system.breadth_proxy_signal_concept_selection import (
    DEFAULT_DOCS_ROOT as DEFAULT_BREADTH_PROXY_SIGNAL_SELECTION_DOCS_ROOT,
)
from ai_trading_system.breadth_proxy_signal_concept_selection import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_BREADTH_PROXY_SIGNAL_SELECTION_OUTPUT_ROOT,
)
from ai_trading_system.breadth_proxy_signal_concept_selection import (
    MODE as BREADTH_PROXY_SIGNAL_SELECTION_MODE,
)
from ai_trading_system.breadth_proxy_signal_concept_selection import (
    run_breadth_proxy_signal_concept_selection,
)
from ai_trading_system.candidate_confidence_scaling_refinement_plan import (
    DEFAULT_DIAGNOSTICS_ROOT as DEFAULT_CONFIDENCE_SCALING_DIAGNOSTICS_ROOT,
)
from ai_trading_system.candidate_confidence_scaling_refinement_plan import (
    DEFAULT_DOCS_ROOT as DEFAULT_CONFIDENCE_SCALING_DOCS_ROOT,
)
from ai_trading_system.candidate_confidence_scaling_refinement_plan import (
    DEFAULT_GENERATOR_ROOT as DEFAULT_CONFIDENCE_SCALING_GENERATOR_ROOT,
)
from ai_trading_system.candidate_confidence_scaling_refinement_plan import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_CONFIDENCE_SCALING_OUTPUT_ROOT,
)
from ai_trading_system.candidate_confidence_scaling_refinement_plan import (
    DEFAULT_VALIDATION_ROOT as DEFAULT_CONFIDENCE_SCALING_VALIDATION_ROOT,
)
from ai_trading_system.candidate_confidence_scaling_refinement_plan import (
    run_candidate_generator_confidence_scaling_refinement_plan,
)
from ai_trading_system.candidate_signal_prediction_artifact_audit import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_CANDIDATE_ARTIFACT_AUDIT_OUTPUT_ROOT,
)
from ai_trading_system.candidate_signal_prediction_artifact_audit import (
    run_candidate_signal_prediction_artifact_audit_pack,
)
from ai_trading_system.channel_specific_first_layer_v3 import (
    DEFAULT_ACTION_VALUE_MATRIX_PATH as DEFAULT_CHANNEL_V3_ACTION_VALUE_MATRIX_PATH,
)
from ai_trading_system.channel_specific_first_layer_v3 import (
    DEFAULT_CHANNEL_V3_CONFIG_PATH,
    DEFAULT_CHANNEL_V3_OUTPUT_ROOT,
    DEFAULT_DO_NOT_DERISK_SELECTION_RULE_PATH,
    DEFAULT_FEATURE_SET_LOCKED_PATH,
    DEFAULT_FEATURE_SET_PATH,
    DEFAULT_RISK_ON_VETO_SELECTION_RULE_PATH,
    run_channel_specific_first_layer_v3_pack,
)
from ai_trading_system.channel_specific_first_layer_v3 import (
    DEFAULT_LABELS_PATH as DEFAULT_CHANNEL_V3_LABELS_PATH,
)
from ai_trading_system.channel_specific_first_layer_v3 import (
    DEFAULT_PIT_FEATURE_MATRIX_PATH as DEFAULT_CHANNEL_V3_PIT_FEATURE_MATRIX_PATH,
)
from ai_trading_system.channel_specific_first_layer_v4 import (
    DEFAULT_FREEZE_CONTRACT_PATH as DEFAULT_CHANNEL_V4_FREEZE_CONTRACT_PATH,
)
from ai_trading_system.channel_specific_first_layer_v4 import (
    DEFAULT_GATE_DECISION_PATH as DEFAULT_CHANNEL_V4_GATE_DECISION_PATH,
)
from ai_trading_system.channel_specific_first_layer_v4 import (
    run_channel_specific_first_layer_v4_pack,
)
from ai_trading_system.defensive_preservation_lane import (
    DEFAULT_DEFENSIVE_ACTION_VALUE_POLICY_PATH,
    DEFAULT_DEFENSIVE_LABEL_TAXONOMY_PATH,
    DEFAULT_DEFENSIVE_LANE_POLICY_PATH,
    DEFAULT_LIMITED_ADJUSTMENT_REFERENCE_PATH,
    run_defensive_preservation_lane_pack,
)
from ai_trading_system.equal_weight_proxy_data_fix import (
    DEFAULT_DOWNLOAD_MANIFEST_PATH as DEFAULT_EQUAL_WEIGHT_PROXY_DOWNLOAD_MANIFEST_PATH,
)
from ai_trading_system.equal_weight_proxy_data_fix import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_EQUAL_WEIGHT_PROXY_OUTPUT_ROOT,
)
from ai_trading_system.equal_weight_proxy_data_fix import (
    DEFAULT_REPAIR_START as DEFAULT_EQUAL_WEIGHT_PROXY_REPAIR_START,
)
from ai_trading_system.equal_weight_proxy_data_fix import run_equal_weight_proxy_data_fix_pack
from ai_trading_system.event_calendar_feasibility_audit import (
    DEFAULT_DOCS_ROOT as DEFAULT_EVENT_CALENDAR_FEASIBILITY_DOCS_ROOT,
)
from ai_trading_system.event_calendar_feasibility_audit import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_EVENT_CALENDAR_FEASIBILITY_OUTPUT_ROOT,
)
from ai_trading_system.event_calendar_feasibility_audit import (
    DEFAULT_POLICY_PATH as DEFAULT_EVENT_CALENDAR_FEASIBILITY_POLICY_PATH,
)
from ai_trading_system.event_calendar_feasibility_audit import (
    MODE as EVENT_CALENDAR_FEASIBILITY_MODE,
)
from ai_trading_system.event_calendar_feasibility_audit import (
    run_event_calendar_data_feasibility_audit,
)
from ai_trading_system.event_calendar_gating_generator_poc import (
    DEFAULT_DOCS_ROOT as DEFAULT_EVENT_GATING_GENERATOR_DOCS_ROOT,
)
from ai_trading_system.event_calendar_gating_generator_poc import (
    DEFAULT_FEASIBILITY_ROOT as DEFAULT_EVENT_GATING_GENERATOR_FEASIBILITY_ROOT,
)
from ai_trading_system.event_calendar_gating_generator_poc import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_EVENT_GATING_GENERATOR_OUTPUT_ROOT,
)
from ai_trading_system.event_calendar_gating_generator_poc import (
    DEFAULT_POLICY_PATH as DEFAULT_EVENT_GATING_GENERATOR_POLICY_PATH,
)
from ai_trading_system.event_calendar_gating_generator_poc import (
    MODE as EVENT_GATING_GENERATOR_MODE,
)
from ai_trading_system.event_calendar_gating_generator_poc import (
    run_event_calendar_gating_generator_poc,
)
from ai_trading_system.event_gating_validation import (
    DEFAULT_DOCS_ROOT as DEFAULT_EVENT_GATING_VALIDATION_DOCS_ROOT,
)
from ai_trading_system.event_gating_validation import (
    DEFAULT_GENERATOR_ROOT as DEFAULT_EVENT_GATING_VALIDATION_GENERATOR_ROOT,
)
from ai_trading_system.event_gating_validation import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_EVENT_GATING_VALIDATION_OUTPUT_ROOT,
)
from ai_trading_system.event_gating_validation import (
    DEFAULT_POLICY_PATH as DEFAULT_EVENT_GATING_VALIDATION_POLICY_PATH,
)
from ai_trading_system.event_gating_validation import (
    MODE as EVENT_GATING_VALIDATION_MODE,
)
from ai_trading_system.event_gating_validation import run_event_gating_validation
from ai_trading_system.first_layer_active_selection_policy_v2 import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_ACTIVE_SELECTION_POLICY_V2_OUTPUT_ROOT,
)
from ai_trading_system.first_layer_active_selection_policy_v2 import (
    run_first_layer_active_selection_policy_v2_pack,
)
from ai_trading_system.first_layer_active_selection_rule_audit import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_ACTIVE_SELECTION_AUDIT_OUTPUT_ROOT,
)
from ai_trading_system.first_layer_active_selection_rule_audit import (
    run_first_layer_active_selection_rule_audit_pack,
)
from ai_trading_system.first_layer_boundary_candidate_owner_review import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_BOUNDARY_OWNER_REVIEW_OUTPUT_ROOT,
)
from ai_trading_system.first_layer_boundary_candidate_owner_review import (
    run_first_layer_boundary_candidate_owner_review_pack,
)
from ai_trading_system.first_layer_candidate_actual_path_validation import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_CANDIDATE_ACTUAL_PATH_VALIDATION_OUTPUT_ROOT,
)
from ai_trading_system.first_layer_candidate_actual_path_validation import (
    run_first_layer_candidate_actual_path_validation_pack,
)
from ai_trading_system.first_layer_candidate_generator_runtime import (
    run_candidate_generator,
)
from ai_trading_system.first_layer_candidate_generators_regenerate import (
    DEFAULT_MARKETSTACK_PRICES_PATH as DEFAULT_REGENERATED_MARKETSTACK_PRICES_PATH,
)
from ai_trading_system.first_layer_candidate_generators_regenerate import (
    DEFAULT_PRICES_PATH as DEFAULT_REGENERATED_PRICES_PATH,
)
from ai_trading_system.first_layer_candidate_generators_regenerate import (
    DEFAULT_RATES_PATH as DEFAULT_REGENERATED_RATES_PATH,
)
from ai_trading_system.first_layer_candidate_generators_regenerate import (
    run_first_layer_candidate_generators_regenerate,
)
from ai_trading_system.first_layer_challenger_matrix_v2 import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_CHALLENGER_MATRIX_V2_OUTPUT_ROOT,
)
from ai_trading_system.first_layer_challenger_matrix_v2 import (
    run_first_layer_challenger_matrix_v2_pack,
)
from ai_trading_system.first_layer_channel_closeout import (
    DEFAULT_ARCHIVE_POLICY_PATH as DEFAULT_FIRST_LAYER_CHANNEL_ARCHIVE_POLICY_PATH,
)
from ai_trading_system.first_layer_channel_closeout import (
    DEFAULT_FORWARD_MINIMAL_PLAN_PATH as DEFAULT_FIRST_LAYER_FORWARD_MINIMAL_PLAN_PATH,
)
from ai_trading_system.first_layer_channel_closeout import (
    run_first_layer_channel_closeout_pack,
)
from ai_trading_system.first_layer_current_state import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_FIRST_LAYER_CURRENT_STATE_OUTPUT_ROOT,
)
from ai_trading_system.first_layer_current_state import (
    DEFAULT_POLICY_PATH as DEFAULT_FIRST_LAYER_CURRENT_STATE_POLICY_PATH,
)
from ai_trading_system.first_layer_current_state import (
    DEFAULT_PREDICTIONS_PATH as DEFAULT_FIRST_LAYER_CURRENT_STATE_PREDICTIONS_PATH,
)
from ai_trading_system.first_layer_current_state import (
    run_first_layer_current_state_pack,
)
from ai_trading_system.first_layer_defensive_regression_diagnosis import (
    DEFAULT_REGRESSION_INVENTORY_YAML_PATH,
    run_first_layer_defensive_regression_diagnosis_pack,
)
from ai_trading_system.first_layer_gate_policy_v2_reconciliation import (
    DEFAULT_GATE_ABLATION_MATRIX_PATH as DEFAULT_GATE_POLICY_V2_ABLATION_PATH,
)
from ai_trading_system.first_layer_gate_policy_v2_reconciliation import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_GATE_POLICY_V2_OUTPUT_ROOT,
)
from ai_trading_system.first_layer_gate_policy_v2_reconciliation import (
    DEFAULT_RECOMMENDED_GATE_POLICY_PATH as DEFAULT_GATE_POLICY_V2_RECOMMENDED_PATH,
)
from ai_trading_system.first_layer_gate_policy_v2_reconciliation import (
    DEFAULT_THRESHOLD_SENSITIVITY_PATH as DEFAULT_GATE_POLICY_V2_THRESHOLD_PATH,
)
from ai_trading_system.first_layer_gate_policy_v2_reconciliation import (
    run_first_layer_gate_policy_v2_reconciliation_pack,
)
from ai_trading_system.first_layer_new_candidate_family_prioritization import (
    DEFAULT_DOCS_ROOT as DEFAULT_NEW_CANDIDATE_FAMILY_DOCS_ROOT,
)
from ai_trading_system.first_layer_new_candidate_family_prioritization import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_NEW_CANDIDATE_FAMILY_OUTPUT_ROOT,
)
from ai_trading_system.first_layer_new_candidate_family_prioritization import (
    MODE as NEW_CANDIDATE_FAMILY_MODE,
)
from ai_trading_system.first_layer_new_candidate_family_prioritization import (
    run_first_layer_new_candidate_family_prioritization,
)
from ai_trading_system.first_layer_objective_validation_redesign import (
    DEFAULT_BENCHMARK_CONSISTENCY_PATH as DEFAULT_OBJECTIVE_VALIDATION_BENCHMARK_CONSISTENCY_PATH,
)
from ai_trading_system.first_layer_objective_validation_redesign import (
    DEFAULT_CURRENT_STATE_SUMMARY_PATH as DEFAULT_OBJECTIVE_VALIDATION_CURRENT_STATE_SUMMARY_PATH,
)
from ai_trading_system.first_layer_objective_validation_redesign import (
    DEFAULT_FAILURE_TAXONOMY_PATH as DEFAULT_OBJECTIVE_VALIDATION_FAILURE_TAXONOMY_PATH,
)
from ai_trading_system.first_layer_objective_validation_redesign import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_OBJECTIVE_VALIDATION_OUTPUT_ROOT,
)
from ai_trading_system.first_layer_objective_validation_redesign import (
    DEFAULT_POLICY_PATH as DEFAULT_OBJECTIVE_VALIDATION_POLICY_PATH,
)
from ai_trading_system.first_layer_objective_validation_redesign import (
    DEFAULT_PROXY_AUDIT_PATH as DEFAULT_OBJECTIVE_VALIDATION_PROXY_AUDIT_PATH,
)
from ai_trading_system.first_layer_objective_validation_redesign import (
    run_first_layer_objective_validation_redesign_pack,
)
from ai_trading_system.first_layer_performance_gate_audit import (
    DEFAULT_CHALLENGER_MATRIX_PATH as DEFAULT_PERF_GATE_AUDIT_CHALLENGER_MATRIX_PATH,
)
from ai_trading_system.first_layer_performance_gate_audit import (
    DEFAULT_CURRENT_STATE_SUMMARY_PATH as DEFAULT_PERF_GATE_AUDIT_CURRENT_STATE_PATH,
)
from ai_trading_system.first_layer_performance_gate_audit import (
    DEFAULT_FAILURE_TAXONOMY_PATH as DEFAULT_PERF_GATE_AUDIT_FAILURE_TAXONOMY_PATH,
)
from ai_trading_system.first_layer_performance_gate_audit import (
    DEFAULT_OBJECTIVE_VALIDATION_PATH as DEFAULT_PERF_GATE_AUDIT_OBJECTIVE_PATH,
)
from ai_trading_system.first_layer_performance_gate_audit import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_FIRST_LAYER_PERFORMANCE_GATE_AUDIT_OUTPUT_ROOT,
)
from ai_trading_system.first_layer_performance_gate_audit import (
    DEFAULT_POLICY_PATH as DEFAULT_FIRST_LAYER_PERFORMANCE_GATE_AUDIT_POLICY_PATH,
)
from ai_trading_system.first_layer_performance_gate_audit import (
    DEFAULT_RETURN_SEEKING_2022_CONTRAST_PATH as DEFAULT_PERF_GATE_AUDIT_2022_CONTRAST_PATH,
)
from ai_trading_system.first_layer_performance_gate_audit import (
    DEFAULT_RETURN_SEEKING_BETA_TQQQ_ATTRIBUTION_PATH as DEFAULT_PERF_GATE_AUDIT_ATTRIBUTION_PATH,
)
from ai_trading_system.first_layer_performance_gate_audit import (
    run_first_layer_performance_gate_audit_pack,
)
from ai_trading_system.first_layer_policy_calibration import (
    DEFAULT_EXPANDED_UNIVERSE_CONFIG_PATH,
    DEFAULT_MARKETSTACK_PRICES_PATH,
    DEFAULT_PRICES_PATH,
    DEFAULT_PROBE_REGISTRY_PATH,
    DEFAULT_RATES_PATH,
    DEFAULT_RESEARCH_TRENDS_OUTPUT_ROOT,
    DEFAULT_SCOPE_CONFIG_PATH,
    DEFAULT_SCORE_POLICY_PATH,
    DEFAULT_SCORECARD_CONFIG_PATH,
    run_first_layer_policy_aware_calibration_pack,
)
from ai_trading_system.first_layer_proxy_challenger_experiments import (
    DEFAULT_CURRENT_STATE_SUMMARY_PATH as DEFAULT_PROXY_CHALLENGER_CURRENT_STATE_SUMMARY_PATH,
)
from ai_trading_system.first_layer_proxy_challenger_experiments import (
    DEFAULT_OBJECTIVE_VALIDATION_PATH as DEFAULT_PROXY_CHALLENGER_OBJECTIVE_VALIDATION_PATH,
)
from ai_trading_system.first_layer_proxy_challenger_experiments import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_PROXY_CHALLENGER_OUTPUT_ROOT,
)
from ai_trading_system.first_layer_proxy_challenger_experiments import (
    DEFAULT_POLICY_PATH as DEFAULT_PROXY_CHALLENGER_POLICY_PATH,
)
from ai_trading_system.first_layer_proxy_challenger_experiments import (
    DEFAULT_PROXY_COVERAGE_AUDIT_PATH as DEFAULT_PROXY_CHALLENGER_PROXY_COVERAGE_AUDIT_PATH,
)
from ai_trading_system.first_layer_proxy_challenger_experiments import (
    run_first_layer_proxy_challenger_experiments_pack,
)
from ai_trading_system.first_layer_proxy_coverage_audit import (
    DEFAULT_COVERAGE_MATRIX_PATH as DEFAULT_PROXY_COVERAGE_MATRIX_PATH,
)
from ai_trading_system.first_layer_proxy_coverage_audit import (
    DEFAULT_FEATURE_ROOT as DEFAULT_PROXY_COVERAGE_FEATURE_ROOT,
)
from ai_trading_system.first_layer_proxy_coverage_audit import (
    DEFAULT_FMP_GATE_PATH as DEFAULT_PROXY_COVERAGE_FMP_GATE_PATH,
)
from ai_trading_system.first_layer_proxy_coverage_audit import (
    DEFAULT_FREE_FEATURE_REGISTRY_PATH as DEFAULT_PROXY_COVERAGE_FREE_REGISTRY_PATH,
)
from ai_trading_system.first_layer_proxy_coverage_audit import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_PROXY_COVERAGE_OUTPUT_ROOT,
)
from ai_trading_system.first_layer_proxy_coverage_audit import (
    DEFAULT_PARTICIPATION_PROXY_REGISTRY_PATH as DEFAULT_PROXY_COVERAGE_PARTICIPATION_REGISTRY_PATH,
)
from ai_trading_system.first_layer_proxy_coverage_audit import (
    DEFAULT_PIT_CONTRACT_PATH as DEFAULT_PROXY_COVERAGE_PIT_CONTRACT_PATH,
)
from ai_trading_system.first_layer_proxy_coverage_audit import (
    DEFAULT_POLICY_PATH as DEFAULT_PROXY_COVERAGE_POLICY_PATH,
)
from ai_trading_system.first_layer_proxy_coverage_audit import (
    run_first_layer_proxy_coverage_audit_pack,
)
from ai_trading_system.first_layer_reopen_gate import (
    DEFAULT_CHANNEL_CLOSEOUT_PATH as DEFAULT_REOPEN_GATE_CHANNEL_CLOSEOUT_PATH,
)
from ai_trading_system.first_layer_reopen_gate import (
    DEFAULT_COVERAGE_MATRIX_PATH as DEFAULT_REOPEN_GATE_COVERAGE_MATRIX_PATH,
)
from ai_trading_system.first_layer_reopen_gate import (
    DEFAULT_DEPENDENCY_DIAGNOSTICS_PATH as DEFAULT_REOPEN_GATE_DEPENDENCY_DIAGNOSTICS_PATH,
)
from ai_trading_system.first_layer_reopen_gate import (
    DEFAULT_FREE_FEATURE_FINAL_PATH as DEFAULT_REOPEN_GATE_FREE_FEATURE_FINAL_PATH,
)
from ai_trading_system.first_layer_reopen_gate import (
    DEFAULT_FREE_FEATURE_PIT_AUDIT_PATH as DEFAULT_REOPEN_GATE_PIT_AUDIT_PATH,
)
from ai_trading_system.first_layer_reopen_gate import (
    DEFAULT_PARTICIPATION_FINAL_PATH as DEFAULT_REOPEN_GATE_PARTICIPATION_FINAL_PATH,
)
from ai_trading_system.first_layer_reopen_gate import (
    DEFAULT_POLICY_PATH as DEFAULT_REOPEN_GATE_POLICY_PATH,
)
from ai_trading_system.first_layer_reopen_gate import run_first_layer_reopen_gate_pack
from ai_trading_system.first_layer_up_state_learning import (
    DEFAULT_HIERARCHICAL_CONFIG_PATH,
    DEFAULT_THRESHOLD_POLICY_PATH,
    run_first_layer_up_state_learning_repair_pack,
)
from ai_trading_system.first_layer_walk_forward_coverage import (
    DEFAULT_2022_SLICE_YAML_PATH,
    DEFAULT_ACTUAL_PATH_YAML_PATH,
    DEFAULT_COVERAGE_MODEL_ROOT,
    DEFAULT_COVERAGE_POLICY_PATH,
    DEFAULT_COVERAGE_SELECTION_RULE_PATH,
    DEFAULT_COVERAGE_SIMULATION_YAML_PATH,
    DEFAULT_FAILURE_YAML_PATH,
    DEFAULT_FEATURE_OPTIONALIZATION_POLICY_PATH,
    DEFAULT_FINAL_MATRIX_YAML_PATH,
    DEFAULT_MODEL_MATRIX_YAML_PATH,
    run_first_layer_walk_forward_coverage_rebuild_pack,
)
from ai_trading_system.forward_observe_evidence_accumulation_plan import (
    DEFAULT_DOCS_ROOT as DEFAULT_FORWARD_OBSERVE_EVIDENCE_DOCS_ROOT,
)
from ai_trading_system.forward_observe_evidence_accumulation_plan import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_FORWARD_OBSERVE_EVIDENCE_OUTPUT_ROOT,
)
from ai_trading_system.forward_observe_evidence_accumulation_plan import (
    DEFAULT_READINESS_ROOT as DEFAULT_FORWARD_OBSERVE_EVIDENCE_READINESS_ROOT,
)
from ai_trading_system.forward_observe_evidence_accumulation_plan import (
    MODE as FORWARD_OBSERVE_EVIDENCE_MODE,
)
from ai_trading_system.forward_observe_evidence_accumulation_plan import (
    run_forward_observe_evidence_accumulation_plan,
)
from ai_trading_system.free_feature_family_reablation import (
    DEFAULT_CHANNEL_CLOSEOUT_PATH as DEFAULT_FREE_REABLATION_CHANNEL_CLOSEOUT_PATH,
)
from ai_trading_system.free_feature_family_reablation import (
    DEFAULT_COVERAGE_MATRIX_PATH as DEFAULT_FREE_REABLATION_COVERAGE_MATRIX_PATH,
)
from ai_trading_system.free_feature_family_reablation import (
    DEFAULT_FEATURE_ROOT as DEFAULT_FREE_REABLATION_FEATURE_ROOT,
)
from ai_trading_system.free_feature_family_reablation import (
    DEFAULT_FREE_FEATURE_PIT_AUDIT_PATH as DEFAULT_FREE_REABLATION_PIT_AUDIT_PATH,
)
from ai_trading_system.free_feature_family_reablation import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_FREE_REABLATION_OUTPUT_ROOT,
)
from ai_trading_system.free_feature_family_reablation import (
    DEFAULT_REGISTRY_V2_PATH as DEFAULT_FREE_REABLATION_REGISTRY_PATH,
)
from ai_trading_system.free_feature_family_reablation import (
    run_free_feature_family_reablation_pack,
)
from ai_trading_system.free_pit_data_sources import (
    DEFAULT_FREE_DATA_SOURCE_REGISTRY_PATH,
    DEFAULT_FREE_FEATURE_OUTPUT_ROOT,
    DEFAULT_FREE_FEATURE_POLICY_PATH,
    DEFAULT_FREE_SOURCE_OUTPUT_ROOT,
    DEFAULT_MANIFEST_PATH,
    DEFAULT_PARTICIPATION_PROXY_REGISTRY_PATH,
    DEFAULT_RESEARCH_DOCS_ROOT,
    DEFAULT_RESEARCH_INPUTS_ROOT,
    run_free_feature_readiness,
)
from ai_trading_system.free_pit_data_sources import (
    DEFAULT_MARKETSTACK_PRICES_PATH as DEFAULT_FREE_MARKETSTACK_PRICES_PATH,
)
from ai_trading_system.free_pit_data_sources import (
    DEFAULT_PRICES_PATH as DEFAULT_FREE_PRICES_PATH,
)
from ai_trading_system.free_pit_data_sources import (
    DEFAULT_RATES_PATH as DEFAULT_FREE_RATES_PATH,
)
from ai_trading_system.indicator_family_ablation import (
    DEFAULT_ACTION_VALUE_MATRIX_PATH,
    DEFAULT_ACTION_VALUE_SUMMARY_PATH,
    DEFAULT_INDICATOR_FAMILY_ABLATION_MATRIX_PATH,
    DEFAULT_INDICATOR_FAMILY_ABLATION_OUTPUT_ROOT,
    DEFAULT_INDICATOR_FAMILY_ABLATION_REVIEW_PATH,
    DEFAULT_INDICATOR_FAMILY_REGISTRY_PATH,
    DEFAULT_INDICATOR_FAMILY_SELECTION_RULE_PATH,
    DEFAULT_LABELS_PATH,
    DEFAULT_PIT_FEATURE_MATRIX_PATH,
    run_indicator_family_ablation,
)
from ai_trading_system.liquidity_rates_actual_path_validation import (
    DEFAULT_DOCS_ROOT as DEFAULT_LIQUIDITY_RATES_ACTUAL_PATH_DOCS_ROOT,
)
from ai_trading_system.liquidity_rates_actual_path_validation import (
    DEFAULT_GENERATOR_ROOT as DEFAULT_LIQUIDITY_RATES_ACTUAL_PATH_GENERATOR_ROOT,
)
from ai_trading_system.liquidity_rates_actual_path_validation import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_LIQUIDITY_RATES_ACTUAL_PATH_OUTPUT_ROOT,
)
from ai_trading_system.liquidity_rates_actual_path_validation import (
    DEFAULT_POLICY_PATH as DEFAULT_LIQUIDITY_RATES_ACTUAL_PATH_POLICY_PATH,
)
from ai_trading_system.liquidity_rates_actual_path_validation import (
    MODE as LIQUIDITY_RATES_ACTUAL_PATH_MODE,
)
from ai_trading_system.liquidity_rates_actual_path_validation import (
    run_liquidity_rates_actual_path_validation,
)
from ai_trading_system.liquidity_rates_feasibility_audit import (
    DEFAULT_DOCS_ROOT as DEFAULT_LIQUIDITY_RATES_FEASIBILITY_DOCS_ROOT,
)
from ai_trading_system.liquidity_rates_feasibility_audit import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_LIQUIDITY_RATES_FEASIBILITY_OUTPUT_ROOT,
)
from ai_trading_system.liquidity_rates_feasibility_audit import (
    MODE as LIQUIDITY_RATES_FEASIBILITY_MODE,
)
from ai_trading_system.liquidity_rates_feasibility_audit import (
    run_liquidity_rates_pressure_feasibility_audit,
)
from ai_trading_system.liquidity_rates_generator_poc import (
    DEFAULT_DOCS_ROOT as DEFAULT_LIQUIDITY_RATES_GENERATOR_DOCS_ROOT,
)
from ai_trading_system.liquidity_rates_generator_poc import (
    DEFAULT_FEASIBILITY_ROOT as DEFAULT_LIQUIDITY_RATES_GENERATOR_FEASIBILITY_ROOT,
)
from ai_trading_system.liquidity_rates_generator_poc import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_LIQUIDITY_RATES_GENERATOR_OUTPUT_ROOT,
)
from ai_trading_system.liquidity_rates_generator_poc import (
    DEFAULT_POLICY_PATH as DEFAULT_LIQUIDITY_RATES_GENERATOR_POLICY_PATH,
)
from ai_trading_system.liquidity_rates_generator_poc import (
    MODE as LIQUIDITY_RATES_GENERATOR_MODE,
)
from ai_trading_system.liquidity_rates_generator_poc import (
    run_liquidity_rates_pressure_generator_poc,
)
from ai_trading_system.liquidity_rates_scope_review import (
    DEFAULT_DOCS_ROOT as DEFAULT_LIQUIDITY_RATES_SCOPE_REVIEW_DOCS_ROOT,
)
from ai_trading_system.liquidity_rates_scope_review import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_LIQUIDITY_RATES_SCOPE_REVIEW_OUTPUT_ROOT,
)
from ai_trading_system.liquidity_rates_scope_review import (
    DEFAULT_POLICY_PATH as DEFAULT_LIQUIDITY_RATES_SCOPE_REVIEW_POLICY_PATH,
)
from ai_trading_system.liquidity_rates_scope_review import (
    DEFAULT_VALIDATION_ROOT as DEFAULT_LIQUIDITY_RATES_SCOPE_REVIEW_VALIDATION_ROOT,
)
from ai_trading_system.liquidity_rates_scope_review import (
    MODE as LIQUIDITY_RATES_SCOPE_REVIEW_MODE,
)
from ai_trading_system.liquidity_rates_scope_review import (
    run_liquidity_rates_scope_review,
)
from ai_trading_system.minimal_forward_diagnostic import (
    DEFAULT_POLICY_PATH as DEFAULT_MINIMAL_FORWARD_POLICY_PATH,
)
from ai_trading_system.minimal_forward_diagnostic import (
    DEFAULT_SCHEMA_PATH as DEFAULT_MINIMAL_FORWARD_SCHEMA_PATH,
)
from ai_trading_system.minimal_forward_diagnostic import (
    run_minimal_forward_diagnostic_outcome_backfill,
    run_minimal_forward_diagnostic_pack,
)
from ai_trading_system.participation_proxy_validation import (
    DEFAULT_FEATURE_ROOT as DEFAULT_PARTICIPATION_FEATURE_ROOT,
)
from ai_trading_system.participation_proxy_validation import (
    DEFAULT_PROCESSED_ROOT as DEFAULT_PARTICIPATION_PROCESSED_ROOT,
)
from ai_trading_system.participation_proxy_validation import (
    DEFAULT_REGISTRY_PATH as DEFAULT_PARTICIPATION_REGISTRY_PATH,
)
from ai_trading_system.participation_proxy_validation import (
    run_participation_proxy_validation_pack,
)
from ai_trading_system.refined_candidate_actual_path_validation import (
    DEFAULT_DOCS_ROOT as DEFAULT_REFINED_ACTUAL_PATH_DOCS_ROOT,
)
from ai_trading_system.refined_candidate_actual_path_validation import (
    DEFAULT_ORIGINAL_VALIDATION_ROOT as DEFAULT_REFINED_ACTUAL_PATH_ORIGINAL_VALIDATION_ROOT,
)
from ai_trading_system.refined_candidate_actual_path_validation import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_REFINED_ACTUAL_PATH_OUTPUT_ROOT,
)
from ai_trading_system.refined_candidate_actual_path_validation import (
    DEFAULT_REFINED_GENERATOR_ROOT as DEFAULT_REFINED_ACTUAL_PATH_REFINED_GENERATOR_ROOT,
)
from ai_trading_system.refined_candidate_actual_path_validation import (
    DEFAULT_REFINEMENT_PLAN_ROOT as DEFAULT_REFINED_ACTUAL_PATH_REFINEMENT_PLAN_ROOT,
)
from ai_trading_system.refined_candidate_actual_path_validation import (
    run_refined_candidate_actual_path_validation,
)
from ai_trading_system.refined_candidate_generators_regenerate import (
    DEFAULT_DOCS_ROOT as DEFAULT_REFINED_CANDIDATE_REGENERATION_DOCS_ROOT,
)
from ai_trading_system.refined_candidate_generators_regenerate import (
    DEFAULT_ORIGINAL_GENERATOR_ROOT as DEFAULT_REFINED_CANDIDATE_ORIGINAL_GENERATOR_ROOT,
)
from ai_trading_system.refined_candidate_generators_regenerate import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_REFINED_CANDIDATE_REGENERATION_OUTPUT_ROOT,
)
from ai_trading_system.refined_candidate_generators_regenerate import (
    DEFAULT_REFINEMENT_PLAN_ROOT as DEFAULT_REFINED_CANDIDATE_REFINEMENT_PLAN_ROOT,
)
from ai_trading_system.refined_candidate_generators_regenerate import (
    run_refined_candidate_generators_regenerate,
)
from ai_trading_system.refined_candidate_local_edge_scope_review import (
    DEFAULT_DOCS_ROOT as DEFAULT_REFINED_SCOPE_REVIEW_DOCS_ROOT,
)
from ai_trading_system.refined_candidate_local_edge_scope_review import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_REFINED_SCOPE_REVIEW_OUTPUT_ROOT,
)
from ai_trading_system.refined_candidate_local_edge_scope_review import (
    DEFAULT_REFINED_GENERATOR_ROOT as DEFAULT_REFINED_SCOPE_REVIEW_REFINED_GENERATOR_ROOT,
)
from ai_trading_system.refined_candidate_local_edge_scope_review import (
    DEFAULT_REFINED_VALIDATION_ROOT as DEFAULT_REFINED_SCOPE_REVIEW_VALIDATION_ROOT,
)
from ai_trading_system.refined_candidate_local_edge_scope_review import (
    DEFAULT_REFINEMENT_PLAN_ROOT as DEFAULT_REFINED_SCOPE_REVIEW_REFINEMENT_PLAN_ROOT,
)
from ai_trading_system.refined_candidate_local_edge_scope_review import (
    run_refined_candidate_local_edge_scope_review,
)
from ai_trading_system.regenerated_candidate_actual_path_validation import (
    DEFAULT_INPUT_ROOT as DEFAULT_REGENERATED_ACTUAL_PATH_INPUT_ROOT,
)
from ai_trading_system.regenerated_candidate_actual_path_validation import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_REGENERATED_ACTUAL_PATH_OUTPUT_ROOT,
)
from ai_trading_system.regenerated_candidate_actual_path_validation import (
    run_regenerated_candidate_actual_path_validation,
)
from ai_trading_system.regenerated_candidate_inconclusive_diagnostics import (
    DEFAULT_DOCS_ROOT as DEFAULT_INCONCLUSIVE_DIAGNOSTICS_DOCS_ROOT,
)
from ai_trading_system.regenerated_candidate_inconclusive_diagnostics import (
    DEFAULT_GENERATOR_ROOT as DEFAULT_INCONCLUSIVE_DIAGNOSTICS_GENERATOR_ROOT,
)
from ai_trading_system.regenerated_candidate_inconclusive_diagnostics import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_INCONCLUSIVE_DIAGNOSTICS_OUTPUT_ROOT,
)
from ai_trading_system.regenerated_candidate_inconclusive_diagnostics import (
    DEFAULT_VALIDATION_ROOT as DEFAULT_INCONCLUSIVE_DIAGNOSTICS_VALIDATION_ROOT,
)
from ai_trading_system.regenerated_candidate_inconclusive_diagnostics import (
    run_regenerated_candidate_inconclusive_diagnostics,
)
from ai_trading_system.regime_label_generator_diagnostic_poc import (
    DEFAULT_DESIGN_POLICY_PATH as DEFAULT_REGIME_LABEL_GENERATOR_DESIGN_POLICY_PATH,
)
from ai_trading_system.regime_label_generator_diagnostic_poc import (
    DEFAULT_DOCS_ROOT as DEFAULT_REGIME_LABEL_GENERATOR_DOCS_ROOT,
)
from ai_trading_system.regime_label_generator_diagnostic_poc import (
    DEFAULT_MARKETSTACK_PRICES_PATH as DEFAULT_REGIME_LABEL_GENERATOR_MARKETSTACK_PRICES_PATH,
)
from ai_trading_system.regime_label_generator_diagnostic_poc import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_REGIME_LABEL_GENERATOR_OUTPUT_ROOT,
)
from ai_trading_system.regime_label_generator_diagnostic_poc import (
    DEFAULT_POLICY_PATH as DEFAULT_REGIME_LABEL_GENERATOR_POLICY_PATH,
)
from ai_trading_system.regime_label_generator_diagnostic_poc import (
    DEFAULT_PRICES_PATH as DEFAULT_REGIME_LABEL_GENERATOR_PRICES_PATH,
)
from ai_trading_system.regime_label_generator_diagnostic_poc import (
    DEFAULT_RATES_PATH as DEFAULT_REGIME_LABEL_GENERATOR_RATES_PATH,
)
from ai_trading_system.regime_label_generator_diagnostic_poc import (
    MODE as REGIME_LABEL_GENERATOR_MODE,
)
from ai_trading_system.regime_label_generator_diagnostic_poc import (
    run_regime_label_generator_diagnostic_poc,
)
from ai_trading_system.regime_segmented_candidate_validation import (
    DEFAULT_DOCS_ROOT as DEFAULT_REGIME_SEGMENTED_VALIDATION_DOCS_ROOT,
)
from ai_trading_system.regime_segmented_candidate_validation import (
    DEFAULT_LABEL_SERIES_PATH as DEFAULT_REGIME_SEGMENTED_VALIDATION_LABEL_SERIES_PATH,
)
from ai_trading_system.regime_segmented_candidate_validation import (
    DEFAULT_LABEL_SUMMARY_PATH as DEFAULT_REGIME_SEGMENTED_VALIDATION_LABEL_SUMMARY_PATH,
)
from ai_trading_system.regime_segmented_candidate_validation import (
    DEFAULT_MARKETSTACK_PRICES_PATH as DEFAULT_REGIME_SEGMENTED_VALIDATION_MARKETSTACK_PRICES_PATH,
)
from ai_trading_system.regime_segmented_candidate_validation import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_REGIME_SEGMENTED_VALIDATION_OUTPUT_ROOT,
)
from ai_trading_system.regime_segmented_candidate_validation import (
    DEFAULT_POLICY_PATH as DEFAULT_REGIME_SEGMENTED_VALIDATION_POLICY_PATH,
)
from ai_trading_system.regime_segmented_candidate_validation import (
    DEFAULT_PRICES_PATH as DEFAULT_REGIME_SEGMENTED_VALIDATION_PRICES_PATH,
)
from ai_trading_system.regime_segmented_candidate_validation import (
    DEFAULT_RATES_PATH as DEFAULT_REGIME_SEGMENTED_VALIDATION_RATES_PATH,
)
from ai_trading_system.regime_segmented_candidate_validation import (
    MODE as REGIME_SEGMENTED_VALIDATION_MODE,
)
from ai_trading_system.regime_segmented_candidate_validation import (
    run_regime_segmented_candidate_validation,
)
from ai_trading_system.regime_state_machine_design_audit import (
    DEFAULT_DOCS_ROOT as DEFAULT_REGIME_STATE_MACHINE_DOCS_ROOT,
)
from ai_trading_system.regime_state_machine_design_audit import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_REGIME_STATE_MACHINE_OUTPUT_ROOT,
)
from ai_trading_system.regime_state_machine_design_audit import (
    DEFAULT_POLICY_PATH as DEFAULT_REGIME_STATE_MACHINE_POLICY_PATH,
)
from ai_trading_system.regime_state_machine_design_audit import (
    MODE as REGIME_STATE_MACHINE_MODE,
)
from ai_trading_system.regime_state_machine_design_audit import (
    run_regime_state_machine_design_audit,
)
from ai_trading_system.research_window_extension import (
    DEFAULT_RESEARCH_WINDOW_REGISTRY_PATH,
    DEFAULT_WINDOW_AWARE_WALK_FORWARD_POLICY_PATH,
    DEFAULT_WINDOWED_ACTUAL_PATH_ROOT,
    DEFAULT_WINDOWED_STATIC_FRONTIER_ROOT,
    run_research_window_extension_validation_pack,
)
from ai_trading_system.return_seeking_diagnostic_lane import (
    DEFAULT_RETURN_SEEKING_POLICY_PATH,
    run_return_seeking_diagnostic_lane_pack,
)
from ai_trading_system.risk_cap_cooldown_decay_design import (
    DEFAULT_DOCS_ROOT as DEFAULT_RISK_CAP_COOLDOWN_DECAY_DOCS_ROOT,
)
from ai_trading_system.risk_cap_cooldown_decay_design import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_RISK_CAP_COOLDOWN_DECAY_OUTPUT_ROOT,
)
from ai_trading_system.risk_cap_cooldown_decay_design import (
    DEFAULT_POLICY_PATH as DEFAULT_RISK_CAP_COOLDOWN_DECAY_POLICY_PATH,
)
from ai_trading_system.risk_cap_cooldown_decay_design import (
    DEFAULT_SOURCE_ROOT as DEFAULT_RISK_CAP_COOLDOWN_DECAY_SOURCE_ROOT,
)
from ai_trading_system.risk_cap_cooldown_decay_design import (
    MODE as RISK_CAP_COOLDOWN_DECAY_MODE,
)
from ai_trading_system.risk_cap_cooldown_decay_design import (
    run_risk_cap_cooldown_decay_design,
)
from ai_trading_system.risk_on_veto_diagnostic import (
    DEFAULT_CHANNEL_PIT_MATRIX_PATH as DEFAULT_RISK_ON_VETO_CHANNEL_PIT_MATRIX_PATH,
)
from ai_trading_system.risk_on_veto_diagnostic import (
    DEFAULT_CHANNEL_V3_COMPOSER_PATH as DEFAULT_RISK_ON_VETO_CHANNEL_V3_COMPOSER_PATH,
)
from ai_trading_system.risk_on_veto_diagnostic import (
    DEFAULT_DIAGNOSTIC_CONTRACT_PATH as DEFAULT_RISK_ON_VETO_DIAGNOSTIC_CONTRACT_PATH,
)
from ai_trading_system.risk_on_veto_diagnostic import (
    DEFAULT_FORWARD_LOG_SPEC_PATH as DEFAULT_RISK_ON_VETO_FORWARD_LOG_SPEC_PATH,
)
from ai_trading_system.risk_on_veto_diagnostic import (
    DEFAULT_METRIC_POLICY_PATH as DEFAULT_RISK_ON_VETO_METRIC_POLICY_PATH,
)
from ai_trading_system.risk_on_veto_diagnostic import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_RISK_ON_VETO_DIAGNOSTIC_OUTPUT_ROOT,
)
from ai_trading_system.risk_on_veto_diagnostic import (
    DEFAULT_RISK_VETO_LABELS_PATH as DEFAULT_RISK_ON_VETO_LABELS_PATH,
)
from ai_trading_system.risk_on_veto_diagnostic import run_risk_on_veto_diagnostic_pack
from ai_trading_system.scope_narrowed_candidate_actual_path_validation import (
    DEFAULT_DOCS_ROOT as DEFAULT_SCOPE_NARROWED_ACTUAL_PATH_DOCS_ROOT,
)
from ai_trading_system.scope_narrowed_candidate_actual_path_validation import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_SCOPE_NARROWED_ACTUAL_PATH_OUTPUT_ROOT,
)
from ai_trading_system.scope_narrowed_candidate_actual_path_validation import (
    DEFAULT_REFINED_VALIDATION_ROOT as DEFAULT_SCOPE_NARROWED_ACTUAL_PATH_REFINED_VALIDATION_ROOT,
)
from ai_trading_system.scope_narrowed_candidate_actual_path_validation import (
    DEFAULT_SCOPE_NARROWED_GENERATOR_ROOT as DEFAULT_SCOPE_NARROWED_ACTUAL_PATH_GENERATOR_ROOT,
)
from ai_trading_system.scope_narrowed_candidate_actual_path_validation import (
    DEFAULT_SCOPE_REVIEW_ROOT as DEFAULT_SCOPE_NARROWED_ACTUAL_PATH_SCOPE_REVIEW_ROOT,
)
from ai_trading_system.scope_narrowed_candidate_actual_path_validation import (
    run_scope_narrowed_candidate_actual_path_validation,
)
from ai_trading_system.scope_narrowed_candidate_generators_regenerate import (
    DEFAULT_DOCS_ROOT as DEFAULT_SCOPE_NARROWED_REGENERATION_DOCS_ROOT,
)
from ai_trading_system.scope_narrowed_candidate_generators_regenerate import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_SCOPE_NARROWED_REGENERATION_OUTPUT_ROOT,
)
from ai_trading_system.scope_narrowed_candidate_generators_regenerate import (
    DEFAULT_REFINED_GENERATOR_ROOT as DEFAULT_SCOPE_NARROWED_REFINED_GENERATOR_ROOT,
)
from ai_trading_system.scope_narrowed_candidate_generators_regenerate import (
    DEFAULT_REFINED_VALIDATION_ROOT as DEFAULT_SCOPE_NARROWED_REFINED_VALIDATION_ROOT,
)
from ai_trading_system.scope_narrowed_candidate_generators_regenerate import (
    DEFAULT_SCOPE_REVIEW_ROOT as DEFAULT_SCOPE_NARROWED_SCOPE_REVIEW_ROOT,
)
from ai_trading_system.scope_narrowed_candidate_generators_regenerate import (
    run_scope_narrowed_candidate_generators_regenerate,
)
from ai_trading_system.scope_narrowed_forward_observe_readiness_review import (
    DEFAULT_DOCS_ROOT as DEFAULT_SCOPE_NARROWED_FORWARD_OBSERVE_DOCS_ROOT,
)
from ai_trading_system.scope_narrowed_forward_observe_readiness_review import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_SCOPE_NARROWED_FORWARD_OBSERVE_OUTPUT_ROOT,
)
from ai_trading_system.scope_narrowed_forward_observe_readiness_review import (
    DEFAULT_SCOPE_GENERATOR_ROOT as DEFAULT_SCOPE_NARROWED_FORWARD_OBSERVE_GENERATOR_ROOT,
)
from ai_trading_system.scope_narrowed_forward_observe_readiness_review import (
    DEFAULT_SCOPE_REVIEW_ROOT as DEFAULT_SCOPE_NARROWED_FORWARD_OBSERVE_SCOPE_REVIEW_ROOT,
)
from ai_trading_system.scope_narrowed_forward_observe_readiness_review import (
    DEFAULT_SCOPE_VALIDATION_ROOT as DEFAULT_SCOPE_NARROWED_FORWARD_OBSERVE_VALIDATION_ROOT,
)
from ai_trading_system.scope_narrowed_forward_observe_readiness_review import (
    run_scope_narrowed_forward_observe_readiness_review,
)
from ai_trading_system.second_layer_probe_library_freeze import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_SECOND_LAYER_PROBE_OUTPUT_ROOT,
)
from ai_trading_system.second_layer_probe_library_freeze import (
    DEFAULT_PREDICTIONS_PATH as DEFAULT_SECOND_LAYER_FROZEN_PREDICTIONS_PATH,
)
from ai_trading_system.second_layer_probe_library_freeze import (
    DEFAULT_PROBE_REGISTRY_V2_PATH,
    run_second_layer_probe_library_freeze_pack,
)
from ai_trading_system.two_layer_policy_compiler import (
    DEFAULT_POLICY_SCHEMA_PATH,
    DEFAULT_SIGNAL_USAGE_MATRIX_V2_PATH,
)
from ai_trading_system.upper_state_label_feature_reset import (
    DEFAULT_ACTION_VALUE_SCORE_POLICY_V2_PATH,
    DEFAULT_ALTERNATING_PROTOCOL_PATH,
    DEFAULT_COMPOSER_PREDICTIONS_PATH,
    DEFAULT_FIRST_LAYER_COMPOSER_V2_PATH,
    DEFAULT_FIRST_LAYER_THRESHOLD_POLICY_V2_PATH,
    DEFAULT_FIRST_LAYER_V2_PROBE_REGISTRY_PATH,
    DEFAULT_UPPER_STATE_TAXONOMY_V2_PATH,
    run_first_layer_v2_label_feature_model_reset_pack,
    run_upper_state_label_feature_reset_pack,
)

console = Console()
trends_app = typer.Typer(
    help="Policy-aware first-layer trend calibration research.",
    no_args_is_help=True,
)
minimal_forward_diagnostic_app = typer.Typer(
    help="Disabled minimal forward diagnostic log governance.",
    invoke_without_command=True,
    no_args_is_help=False,
)


@trends_app.command("full-pack")
def first_layer_policy_aware_full_pack_command(
    scope_config_path: Annotated[Path, typer.Option("--scope-config")] = DEFAULT_SCOPE_CONFIG_PATH,
    probe_registry_path: Annotated[
        Path, typer.Option("--probe-registry")
    ] = DEFAULT_PROBE_REGISTRY_PATH,
    score_policy_path: Annotated[Path, typer.Option("--score-policy")] = DEFAULT_SCORE_POLICY_PATH,
    scorecard_config_path: Annotated[
        Path, typer.Option("--scorecard-config")
    ] = DEFAULT_SCORECARD_CONFIG_PATH,
    expanded_config_path: Annotated[
        Path, typer.Option("--expanded-config")
    ] = DEFAULT_EXPANDED_UNIVERSE_CONFIG_PATH,
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_RATES_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_RESEARCH_TRENDS_OUTPUT_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = run_first_layer_policy_aware_calibration_pack(
        scope_config_path=scope_config_path,
        probe_registry_path=probe_registry_path,
        score_policy_path=score_policy_path,
        scorecard_config_path=scorecard_config_path,
        expanded_config_path=expanded_config_path,
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        output_root=output_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_payload("First-layer policy-aware calibration", payload)


@trends_app.command("up-state-repair")
def first_layer_up_state_repair_command(
    scope_config_path: Annotated[Path, typer.Option("--scope-config")] = DEFAULT_SCOPE_CONFIG_PATH,
    probe_registry_path: Annotated[
        Path, typer.Option("--probe-registry")
    ] = DEFAULT_PROBE_REGISTRY_PATH,
    score_policy_path: Annotated[Path, typer.Option("--score-policy")] = DEFAULT_SCORE_POLICY_PATH,
    scorecard_config_path: Annotated[
        Path, typer.Option("--scorecard-config")
    ] = DEFAULT_SCORECARD_CONFIG_PATH,
    threshold_policy_path: Annotated[
        Path, typer.Option("--threshold-policy")
    ] = DEFAULT_THRESHOLD_POLICY_PATH,
    hierarchical_config_path: Annotated[
        Path, typer.Option("--hierarchical-config")
    ] = DEFAULT_HIERARCHICAL_CONFIG_PATH,
    expanded_config_path: Annotated[
        Path, typer.Option("--expanded-config")
    ] = DEFAULT_EXPANDED_UNIVERSE_CONFIG_PATH,
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_RATES_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_RESEARCH_TRENDS_OUTPUT_ROOT,
    refresh_prerequisites: Annotated[
        bool, typer.Option("--refresh-prerequisites/--no-refresh-prerequisites")
    ] = True,
) -> None:
    payload = run_first_layer_up_state_learning_repair_pack(
        scope_config_path=scope_config_path,
        probe_registry_path=probe_registry_path,
        score_policy_path=score_policy_path,
        scorecard_config_path=scorecard_config_path,
        threshold_policy_path=threshold_policy_path,
        hierarchical_config_path=hierarchical_config_path,
        expanded_config_path=expanded_config_path,
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        output_root=output_root,
        refresh_prerequisites=refresh_prerequisites,
    )
    _print_payload("First-layer up-state learning repair", payload)


@trends_app.command("window-extension")
def research_window_extension_command(
    registry_path: Annotated[
        Path, typer.Option("--registry")
    ] = DEFAULT_RESEARCH_WINDOW_REGISTRY_PATH,
    walk_forward_policy_path: Annotated[
        Path, typer.Option("--walk-forward-policy")
    ] = DEFAULT_WINDOW_AWARE_WALK_FORWARD_POLICY_PATH,
    probe_registry_path: Annotated[
        Path, typer.Option("--probe-registry")
    ] = DEFAULT_PROBE_REGISTRY_PATH,
    score_policy_path: Annotated[Path, typer.Option("--score-policy")] = DEFAULT_SCORE_POLICY_PATH,
    expanded_config_path: Annotated[
        Path, typer.Option("--expanded-config")
    ] = DEFAULT_EXPANDED_UNIVERSE_CONFIG_PATH,
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_RATES_PATH,
    static_output_root: Annotated[
        Path, typer.Option("--static-output-root")
    ] = DEFAULT_WINDOWED_STATIC_FRONTIER_ROOT,
    actual_path_output_root: Annotated[
        Path, typer.Option("--actual-path-output-root")
    ] = DEFAULT_WINDOWED_ACTUAL_PATH_ROOT,
) -> None:
    payload = run_research_window_extension_validation_pack(
        registry_path=registry_path,
        walk_forward_policy_path=walk_forward_policy_path,
        probe_registry_path=probe_registry_path,
        score_policy_path=score_policy_path,
        expanded_config_path=expanded_config_path,
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        static_output_root=static_output_root,
        actual_path_output_root=actual_path_output_root,
    )
    _print_payload("Research window extension validation", payload)


@trends_app.command("second-layer-probe-freeze")
def second_layer_probe_freeze_command(
    registry_path: Annotated[
        Path, typer.Option("--registry")
    ] = DEFAULT_RESEARCH_WINDOW_REGISTRY_PATH,
    probe_registry_path: Annotated[
        Path, typer.Option("--probe-registry")
    ] = DEFAULT_PROBE_REGISTRY_V2_PATH,
    expanded_config_path: Annotated[
        Path, typer.Option("--expanded-config")
    ] = DEFAULT_EXPANDED_UNIVERSE_CONFIG_PATH,
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_RATES_PATH,
    predictions_path: Annotated[
        Path, typer.Option("--predictions-path")
    ] = DEFAULT_SECOND_LAYER_FROZEN_PREDICTIONS_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_SECOND_LAYER_PROBE_OUTPUT_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = run_second_layer_probe_library_freeze_pack(
        registry_path=registry_path,
        probe_registry_path=probe_registry_path,
        expanded_config_path=expanded_config_path,
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        predictions_path=predictions_path,
        output_root=output_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_payload("Second-layer probe library freeze", payload)


@trends_app.command("upper-state-reset")
def upper_state_label_feature_reset_command(
    registry_path: Annotated[
        Path, typer.Option("--registry")
    ] = DEFAULT_RESEARCH_WINDOW_REGISTRY_PATH,
    alternating_protocol_path: Annotated[
        Path, typer.Option("--alternating-protocol")
    ] = DEFAULT_ALTERNATING_PROTOCOL_PATH,
    upper_state_taxonomy_path: Annotated[
        Path, typer.Option("--upper-state-taxonomy")
    ] = DEFAULT_UPPER_STATE_TAXONOMY_V2_PATH,
    action_value_policy_path: Annotated[
        Path, typer.Option("--action-value-policy")
    ] = DEFAULT_ACTION_VALUE_SCORE_POLICY_V2_PATH,
    threshold_policy_path: Annotated[
        Path, typer.Option("--threshold-policy")
    ] = DEFAULT_FIRST_LAYER_THRESHOLD_POLICY_V2_PATH,
    composer_config_path: Annotated[
        Path, typer.Option("--composer-config")
    ] = DEFAULT_FIRST_LAYER_COMPOSER_V2_PATH,
    probe_registry_path: Annotated[
        Path, typer.Option("--probe-registry")
    ] = DEFAULT_PROBE_REGISTRY_PATH,
    expanded_config_path: Annotated[
        Path, typer.Option("--expanded-config")
    ] = DEFAULT_EXPANDED_UNIVERSE_CONFIG_PATH,
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_RATES_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_RESEARCH_TRENDS_OUTPUT_ROOT,
) -> None:
    payload = run_upper_state_label_feature_reset_pack(
        registry_path=registry_path,
        alternating_protocol_path=alternating_protocol_path,
        upper_state_taxonomy_path=upper_state_taxonomy_path,
        action_value_policy_path=action_value_policy_path,
        threshold_policy_path=threshold_policy_path,
        composer_config_path=composer_config_path,
        probe_registry_path=probe_registry_path,
        expanded_config_path=expanded_config_path,
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        output_root=output_root,
    )
    _print_payload("Upper-state label feature reset", payload)


@trends_app.command("first-layer-v2-reset")
def first_layer_v2_label_feature_model_reset_command(
    registry_path: Annotated[
        Path, typer.Option("--registry")
    ] = DEFAULT_RESEARCH_WINDOW_REGISTRY_PATH,
    alternating_protocol_path: Annotated[
        Path, typer.Option("--alternating-protocol")
    ] = DEFAULT_ALTERNATING_PROTOCOL_PATH,
    upper_state_taxonomy_path: Annotated[
        Path, typer.Option("--upper-state-taxonomy")
    ] = DEFAULT_UPPER_STATE_TAXONOMY_V2_PATH,
    action_value_policy_path: Annotated[
        Path, typer.Option("--action-value-policy")
    ] = DEFAULT_ACTION_VALUE_SCORE_POLICY_V2_PATH,
    threshold_policy_path: Annotated[
        Path, typer.Option("--threshold-policy")
    ] = DEFAULT_FIRST_LAYER_THRESHOLD_POLICY_V2_PATH,
    composer_config_path: Annotated[
        Path, typer.Option("--composer-config")
    ] = DEFAULT_FIRST_LAYER_COMPOSER_V2_PATH,
    probe_registry_path: Annotated[
        Path, typer.Option("--probe-registry")
    ] = DEFAULT_FIRST_LAYER_V2_PROBE_REGISTRY_PATH,
    expanded_config_path: Annotated[
        Path, typer.Option("--expanded-config")
    ] = DEFAULT_EXPANDED_UNIVERSE_CONFIG_PATH,
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_RATES_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_RESEARCH_TRENDS_OUTPUT_ROOT,
) -> None:
    payload = run_first_layer_v2_label_feature_model_reset_pack(
        registry_path=registry_path,
        alternating_protocol_path=alternating_protocol_path,
        upper_state_taxonomy_path=upper_state_taxonomy_path,
        action_value_policy_path=action_value_policy_path,
        threshold_policy_path=threshold_policy_path,
        composer_config_path=composer_config_path,
        probe_registry_path=probe_registry_path,
        expanded_config_path=expanded_config_path,
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        output_root=output_root,
    )
    _print_payload("First-layer v2 label feature model reset", payload)


@trends_app.command("first-layer-coverage-rebuild")
def first_layer_walk_forward_coverage_rebuild_command(
    registry_path: Annotated[
        Path, typer.Option("--registry")
    ] = DEFAULT_RESEARCH_WINDOW_REGISTRY_PATH,
    coverage_policy_path: Annotated[
        Path, typer.Option("--coverage-policy")
    ] = DEFAULT_COVERAGE_POLICY_PATH,
    feature_optionalization_policy_path: Annotated[
        Path, typer.Option("--feature-optionalization-policy")
    ] = DEFAULT_FEATURE_OPTIONALIZATION_POLICY_PATH,
    coverage_selection_rule_path: Annotated[
        Path, typer.Option("--coverage-selection-rule")
    ] = DEFAULT_COVERAGE_SELECTION_RULE_PATH,
    upper_state_taxonomy_path: Annotated[
        Path, typer.Option("--upper-state-taxonomy")
    ] = DEFAULT_UPPER_STATE_TAXONOMY_V2_PATH,
    action_value_policy_path: Annotated[
        Path, typer.Option("--action-value-policy")
    ] = DEFAULT_ACTION_VALUE_SCORE_POLICY_V2_PATH,
    threshold_policy_path: Annotated[
        Path, typer.Option("--threshold-policy")
    ] = DEFAULT_FIRST_LAYER_THRESHOLD_POLICY_V2_PATH,
    composer_config_path: Annotated[
        Path, typer.Option("--composer-config")
    ] = DEFAULT_FIRST_LAYER_COMPOSER_V2_PATH,
    probe_registry_path: Annotated[
        Path, typer.Option("--probe-registry")
    ] = DEFAULT_FIRST_LAYER_V2_PROBE_REGISTRY_PATH,
    expanded_config_path: Annotated[
        Path, typer.Option("--expanded-config")
    ] = DEFAULT_EXPANDED_UNIVERSE_CONFIG_PATH,
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_RATES_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_RESEARCH_TRENDS_OUTPUT_ROOT,
) -> None:
    payload = run_first_layer_walk_forward_coverage_rebuild_pack(
        registry_path=registry_path,
        coverage_policy_path=coverage_policy_path,
        feature_optionalization_policy_path=feature_optionalization_policy_path,
        coverage_selection_rule_path=coverage_selection_rule_path,
        upper_state_taxonomy_path=upper_state_taxonomy_path,
        action_value_policy_path=action_value_policy_path,
        threshold_policy_path=threshold_policy_path,
        composer_config_path=composer_config_path,
        probe_registry_path=probe_registry_path,
        expanded_config_path=expanded_config_path,
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        output_root=output_root,
    )
    _print_payload("First-layer walk-forward coverage rebuild", payload)


@trends_app.command("first-layer-defensive-regression-diagnosis")
def first_layer_defensive_regression_diagnosis_command(
    actual_path_path: Annotated[
        Path, typer.Option("--actual-path")
    ] = DEFAULT_ACTUAL_PATH_YAML_PATH,
    prior_slice_path: Annotated[Path, typer.Option("--prior-slice")] = DEFAULT_2022_SLICE_YAML_PATH,
    coverage_final_path: Annotated[
        Path, typer.Option("--coverage-final")
    ] = DEFAULT_FINAL_MATRIX_YAML_PATH,
    coverage_failure_path: Annotated[
        Path, typer.Option("--coverage-failure")
    ] = DEFAULT_FAILURE_YAML_PATH,
    coverage_simulation_path: Annotated[
        Path, typer.Option("--coverage-simulation")
    ] = DEFAULT_COVERAGE_SIMULATION_YAML_PATH,
    coverage_model_matrix_path: Annotated[
        Path, typer.Option("--coverage-model-matrix")
    ] = DEFAULT_MODEL_MATRIX_YAML_PATH,
    probe_registry_path: Annotated[
        Path, typer.Option("--probe-registry")
    ] = DEFAULT_FIRST_LAYER_V2_PROBE_REGISTRY_PATH,
    expanded_config_path: Annotated[
        Path, typer.Option("--expanded-config")
    ] = DEFAULT_EXPANDED_UNIVERSE_CONFIG_PATH,
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_RATES_PATH,
    coverage_model_root: Annotated[
        Path, typer.Option("--coverage-model-root")
    ] = DEFAULT_COVERAGE_MODEL_ROOT,
) -> None:
    payload = run_first_layer_defensive_regression_diagnosis_pack(
        actual_path_path=actual_path_path,
        prior_slice_path=prior_slice_path,
        coverage_final_path=coverage_final_path,
        coverage_failure_path=coverage_failure_path,
        coverage_simulation_path=coverage_simulation_path,
        coverage_model_matrix_path=coverage_model_matrix_path,
        probe_registry_path=probe_registry_path,
        expanded_config_path=expanded_config_path,
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        coverage_model_root=coverage_model_root,
    )
    _print_payload("First-layer defensive regression diagnosis", payload)


@trends_app.command("defensive-lane")
def defensive_preservation_lane_command(
    registry_path: Annotated[
        Path, typer.Option("--registry")
    ] = DEFAULT_RESEARCH_WINDOW_REGISTRY_PATH,
    lane_policy_path: Annotated[
        Path, typer.Option("--lane-policy")
    ] = DEFAULT_DEFENSIVE_LANE_POLICY_PATH,
    label_taxonomy_path: Annotated[
        Path, typer.Option("--label-taxonomy")
    ] = DEFAULT_DEFENSIVE_LABEL_TAXONOMY_PATH,
    action_value_policy_path: Annotated[
        Path, typer.Option("--action-value-policy")
    ] = DEFAULT_DEFENSIVE_ACTION_VALUE_POLICY_PATH,
    probe_registry_path: Annotated[
        Path, typer.Option("--probe-registry")
    ] = DEFAULT_FIRST_LAYER_V2_PROBE_REGISTRY_PATH,
    expanded_config_path: Annotated[
        Path, typer.Option("--expanded-config")
    ] = DEFAULT_EXPANDED_UNIVERSE_CONFIG_PATH,
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_RATES_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_RESEARCH_TRENDS_OUTPUT_ROOT,
    limited_adjustment_reference_path: Annotated[
        Path, typer.Option("--limited-adjustment-reference")
    ] = DEFAULT_LIMITED_ADJUSTMENT_REFERENCE_PATH,
) -> None:
    payload = run_defensive_preservation_lane_pack(
        registry_path=registry_path,
        lane_policy_path=lane_policy_path,
        label_taxonomy_path=label_taxonomy_path,
        action_value_policy_path=action_value_policy_path,
        probe_registry_path=probe_registry_path,
        expanded_config_path=expanded_config_path,
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        output_root=output_root,
        limited_adjustment_reference_path=limited_adjustment_reference_path,
    )
    _print_payload("Defensive preservation lane", payload)


@trends_app.command("return-seeking-diagnostic-lane")
def return_seeking_diagnostic_lane_command(
    registry_path: Annotated[
        Path, typer.Option("--registry")
    ] = DEFAULT_RESEARCH_WINDOW_REGISTRY_PATH,
    lane_policy_path: Annotated[
        Path, typer.Option("--lane-policy")
    ] = DEFAULT_RETURN_SEEKING_POLICY_PATH,
    probe_registry_path: Annotated[
        Path, typer.Option("--probe-registry")
    ] = DEFAULT_FIRST_LAYER_V2_PROBE_REGISTRY_PATH,
    composer_predictions_path: Annotated[
        Path, typer.Option("--composer-predictions")
    ] = DEFAULT_COMPOSER_PREDICTIONS_PATH,
    expanded_config_path: Annotated[
        Path, typer.Option("--expanded-config")
    ] = DEFAULT_EXPANDED_UNIVERSE_CONFIG_PATH,
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_RATES_PATH,
) -> None:
    payload = run_return_seeking_diagnostic_lane_pack(
        registry_path=registry_path,
        lane_policy_path=lane_policy_path,
        probe_registry_path=probe_registry_path,
        composer_predictions_path=composer_predictions_path,
        expanded_config_path=expanded_config_path,
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
    )
    _print_payload("Return-seeking diagnostic lane", payload)


@trends_app.command("indicator-family-ablation")
def indicator_family_ablation_command(
    registry_path: Annotated[
        Path, typer.Option("--registry")
    ] = DEFAULT_INDICATOR_FAMILY_REGISTRY_PATH,
    selection_rule_path: Annotated[
        Path, typer.Option("--selection-rule")
    ] = DEFAULT_INDICATOR_FAMILY_SELECTION_RULE_PATH,
    pit_feature_matrix_path: Annotated[
        Path, typer.Option("--pit-feature-matrix")
    ] = DEFAULT_PIT_FEATURE_MATRIX_PATH,
    labels_path: Annotated[Path, typer.Option("--labels-path")] = DEFAULT_LABELS_PATH,
    action_value_matrix_path: Annotated[
        Path, typer.Option("--action-value-matrix")
    ] = DEFAULT_ACTION_VALUE_MATRIX_PATH,
    action_value_summary_path: Annotated[
        Path, typer.Option("--action-value-summary")
    ] = DEFAULT_ACTION_VALUE_SUMMARY_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_INDICATOR_FAMILY_ABLATION_OUTPUT_ROOT,
    matrix_path: Annotated[
        Path, typer.Option("--matrix-path")
    ] = DEFAULT_INDICATOR_FAMILY_ABLATION_MATRIX_PATH,
    review_path: Annotated[
        Path, typer.Option("--review-path")
    ] = DEFAULT_INDICATOR_FAMILY_ABLATION_REVIEW_PATH,
) -> None:
    payload = run_indicator_family_ablation(
        registry_path=registry_path,
        selection_rule_path=selection_rule_path,
        pit_feature_matrix_path=pit_feature_matrix_path,
        labels_path=labels_path,
        action_value_matrix_path=action_value_matrix_path,
        action_value_summary_path=action_value_summary_path,
        output_root=output_root,
        matrix_path=matrix_path,
        review_path=review_path,
    )
    _print_payload("Indicator family ablation", payload)


@trends_app.command("channel-specific-v3")
def channel_specific_first_layer_v3_command(
    feature_set_path: Annotated[Path, typer.Option("--feature-set")] = DEFAULT_FEATURE_SET_PATH,
    feature_set_locked_path: Annotated[
        Path, typer.Option("--feature-set-locked")
    ] = DEFAULT_FEATURE_SET_LOCKED_PATH,
    do_not_selection_rule_path: Annotated[
        Path, typer.Option("--do-not-selection-rule")
    ] = DEFAULT_DO_NOT_DERISK_SELECTION_RULE_PATH,
    risk_veto_selection_rule_path: Annotated[
        Path, typer.Option("--risk-veto-selection-rule")
    ] = DEFAULT_RISK_ON_VETO_SELECTION_RULE_PATH,
    channel_config_path: Annotated[
        Path, typer.Option("--channel-config")
    ] = DEFAULT_CHANNEL_V3_CONFIG_PATH,
    pit_feature_matrix_path: Annotated[
        Path, typer.Option("--pit-feature-matrix")
    ] = DEFAULT_CHANNEL_V3_PIT_FEATURE_MATRIX_PATH,
    labels_path: Annotated[Path, typer.Option("--labels-path")] = DEFAULT_CHANNEL_V3_LABELS_PATH,
    action_value_matrix_path: Annotated[
        Path, typer.Option("--action-value-matrix")
    ] = DEFAULT_CHANNEL_V3_ACTION_VALUE_MATRIX_PATH,
    probe_registry_path: Annotated[
        Path, typer.Option("--probe-registry")
    ] = DEFAULT_PROBE_REGISTRY_V2_PATH,
    composer_predictions_path: Annotated[
        Path, typer.Option("--composer-predictions")
    ] = DEFAULT_SECOND_LAYER_FROZEN_PREDICTIONS_PATH,
    policy_schema_path: Annotated[
        Path, typer.Option("--policy-schema")
    ] = DEFAULT_POLICY_SCHEMA_PATH,
    signal_usage_matrix_path: Annotated[
        Path, typer.Option("--signal-usage-matrix")
    ] = DEFAULT_SIGNAL_USAGE_MATRIX_V2_PATH,
    expanded_config_path: Annotated[
        Path, typer.Option("--expanded-config")
    ] = DEFAULT_EXPANDED_UNIVERSE_CONFIG_PATH,
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_RATES_PATH,
    output_root: Annotated[Path, typer.Option("--output-root")] = DEFAULT_CHANNEL_V3_OUTPUT_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = run_channel_specific_first_layer_v3_pack(
        feature_set_path=feature_set_path,
        feature_set_locked_path=feature_set_locked_path,
        do_not_selection_rule_path=do_not_selection_rule_path,
        risk_veto_selection_rule_path=risk_veto_selection_rule_path,
        channel_config_path=channel_config_path,
        pit_feature_matrix_path=pit_feature_matrix_path,
        labels_path=labels_path,
        action_value_matrix_path=action_value_matrix_path,
        probe_registry_path=probe_registry_path,
        composer_predictions_path=composer_predictions_path,
        policy_schema_path=policy_schema_path,
        signal_usage_matrix_path=signal_usage_matrix_path,
        expanded_config_path=expanded_config_path,
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        output_root=output_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_payload("Channel-specific first-layer v3", payload)


@trends_app.command("risk-on-veto-diagnostic")
def risk_on_veto_diagnostic_command(
    diagnostic_contract_path: Annotated[
        Path, typer.Option("--diagnostic-contract")
    ] = DEFAULT_RISK_ON_VETO_DIAGNOSTIC_CONTRACT_PATH,
    metric_policy_path: Annotated[
        Path, typer.Option("--metric-policy")
    ] = DEFAULT_RISK_ON_VETO_METRIC_POLICY_PATH,
    forward_log_spec_path: Annotated[
        Path, typer.Option("--forward-log-spec")
    ] = DEFAULT_RISK_ON_VETO_FORWARD_LOG_SPEC_PATH,
    risk_veto_labels_path: Annotated[
        Path, typer.Option("--risk-veto-labels")
    ] = DEFAULT_RISK_ON_VETO_LABELS_PATH,
    channel_v3_composer_path: Annotated[
        Path, typer.Option("--channel-v3-composer")
    ] = DEFAULT_RISK_ON_VETO_CHANNEL_V3_COMPOSER_PATH,
    channel_pit_matrix_path: Annotated[
        Path, typer.Option("--channel-pit-matrix")
    ] = DEFAULT_RISK_ON_VETO_CHANNEL_PIT_MATRIX_PATH,
    baseline_composer_path: Annotated[
        Path, typer.Option("--baseline-composer")
    ] = DEFAULT_SECOND_LAYER_FROZEN_PREDICTIONS_PATH,
    expanded_config_path: Annotated[
        Path, typer.Option("--expanded-config")
    ] = DEFAULT_EXPANDED_UNIVERSE_CONFIG_PATH,
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_RATES_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_RISK_ON_VETO_DIAGNOSTIC_OUTPUT_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = run_risk_on_veto_diagnostic_pack(
        diagnostic_contract_path=diagnostic_contract_path,
        metric_policy_path=metric_policy_path,
        forward_log_spec_path=forward_log_spec_path,
        risk_veto_labels_path=risk_veto_labels_path,
        channel_v3_composer_path=channel_v3_composer_path,
        channel_pit_matrix_path=channel_pit_matrix_path,
        baseline_composer_path=baseline_composer_path,
        expanded_config_path=expanded_config_path,
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        output_root=output_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_payload("Risk-on veto observe-only diagnostic", payload)


@trends_app.command("first-layer-channel-closeout")
def first_layer_channel_closeout_command(
    archive_policy_path: Annotated[
        Path, typer.Option("--archive-policy")
    ] = DEFAULT_FIRST_LAYER_CHANNEL_ARCHIVE_POLICY_PATH,
    forward_minimal_plan_path: Annotated[
        Path, typer.Option("--forward-minimal-plan")
    ] = DEFAULT_FIRST_LAYER_FORWARD_MINIMAL_PLAN_PATH,
) -> None:
    payload = run_first_layer_channel_closeout_pack(
        archive_policy_path=archive_policy_path,
        forward_minimal_plan_path=forward_minimal_plan_path,
    )
    _print_payload("First-layer channel closeout", payload)


@trends_app.command("free-feature-readiness")
def free_feature_readiness_command(
    registry_path: Annotated[
        Path, typer.Option("--registry", help="Free data source registry YAML。")
    ] = DEFAULT_FREE_DATA_SOURCE_REGISTRY_PATH,
    feature_policy_path: Annotated[
        Path, typer.Option("--feature-policy", help="Free feature policy YAML。")
    ] = DEFAULT_FREE_FEATURE_POLICY_PATH,
    participation_proxy_registry_path: Annotated[
        Path,
        typer.Option("--participation-registry", help="Participation proxy registry YAML。"),
    ] = DEFAULT_PARTICIPATION_PROXY_REGISTRY_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_FREE_RATES_PATH,
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_FREE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path | None, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_FREE_MARKETSTACK_PRICES_PATH,
    manifest_path: Annotated[Path, typer.Option("--manifest-path")] = DEFAULT_MANIFEST_PATH,
    output_root: Annotated[Path, typer.Option("--output-root")] = DEFAULT_FREE_SOURCE_OUTPUT_ROOT,
    feature_output_root: Annotated[
        Path, typer.Option("--feature-output-root")
    ] = DEFAULT_FREE_FEATURE_OUTPUT_ROOT,
    docs_root: Annotated[Path, typer.Option("--docs-root")] = DEFAULT_RESEARCH_DOCS_ROOT,
    inputs_root: Annotated[Path, typer.Option("--inputs-root")] = DEFAULT_RESEARCH_INPUTS_ROOT,
    calendar_input_path: Annotated[
        Path | None,
        typer.Option("--calendar-input", help="Optional CSV/YAML official macro calendar rows。"),
    ] = None,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = run_free_feature_readiness(
        registry_path=registry_path,
        feature_policy_path=feature_policy_path,
        participation_proxy_registry_path=participation_proxy_registry_path,
        rates_path=rates_path,
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        manifest_path=manifest_path,
        output_root=output_root,
        feature_output_root=feature_output_root,
        docs_root=docs_root,
        inputs_root=inputs_root,
        calendar_input_path=calendar_input_path,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_payload("Free feature family reopen readiness", payload)


@trends_app.command("free-feature-reablation")
def free_feature_reablation_command(
    feature_root: Annotated[
        Path, typer.Option("--feature-root")
    ] = DEFAULT_FREE_REABLATION_FEATURE_ROOT,
    registry_path: Annotated[
        Path, typer.Option("--registry")
    ] = DEFAULT_FREE_REABLATION_REGISTRY_PATH,
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_FREE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path | None, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_FREE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_FREE_RATES_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_FREE_REABLATION_OUTPUT_ROOT,
    free_feature_pit_audit_path: Annotated[
        Path, typer.Option("--free-feature-pit-audit")
    ] = DEFAULT_FREE_REABLATION_PIT_AUDIT_PATH,
    coverage_matrix_path: Annotated[
        Path, typer.Option("--coverage-matrix")
    ] = DEFAULT_FREE_REABLATION_COVERAGE_MATRIX_PATH,
    channel_closeout_path: Annotated[
        Path, typer.Option("--channel-closeout")
    ] = DEFAULT_FREE_REABLATION_CHANNEL_CLOSEOUT_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = run_free_feature_family_reablation_pack(
        feature_root=feature_root,
        registry_path=registry_path,
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        output_root=output_root,
        free_feature_pit_audit_path=free_feature_pit_audit_path,
        coverage_matrix_path=coverage_matrix_path,
        channel_closeout_path=channel_closeout_path,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_payload("Free feature family re-ablation", payload)


@trends_app.command("participation-proxy-validation")
def participation_proxy_validation_command(
    registry_path: Annotated[
        Path, typer.Option("--registry")
    ] = DEFAULT_PARTICIPATION_REGISTRY_PATH,
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_FREE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path | None, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_FREE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_FREE_RATES_PATH,
    feature_root: Annotated[
        Path, typer.Option("--feature-root")
    ] = DEFAULT_PARTICIPATION_FEATURE_ROOT,
    processed_root: Annotated[
        Path, typer.Option("--processed-root")
    ] = DEFAULT_PARTICIPATION_PROCESSED_ROOT,
    alpha_vantage_input_path: Annotated[Path | None, typer.Option("--alpha-vantage-input")] = None,
    allow_network_trials: Annotated[
        bool, typer.Option("--allow-network-trials/--no-allow-network-trials")
    ] = False,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = run_participation_proxy_validation_pack(
        registry_path=registry_path,
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        feature_root=feature_root,
        processed_root=processed_root,
        alpha_vantage_input_path=alpha_vantage_input_path,
        allow_network_trials=allow_network_trials,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_payload("Participation proxy validation", payload)


@trends_app.command("first-layer-reopen-gate")
def first_layer_reopen_gate_command(
    policy_path: Annotated[Path, typer.Option("--policy")] = DEFAULT_REOPEN_GATE_POLICY_PATH,
    free_feature_final_path: Annotated[
        Path, typer.Option("--free-feature-final")
    ] = DEFAULT_REOPEN_GATE_FREE_FEATURE_FINAL_PATH,
    participation_final_path: Annotated[
        Path, typer.Option("--participation-final")
    ] = DEFAULT_REOPEN_GATE_PARTICIPATION_FINAL_PATH,
    channel_closeout_path: Annotated[
        Path, typer.Option("--channel-closeout")
    ] = DEFAULT_REOPEN_GATE_CHANNEL_CLOSEOUT_PATH,
    free_feature_pit_audit_path: Annotated[
        Path, typer.Option("--free-feature-pit-audit")
    ] = DEFAULT_REOPEN_GATE_PIT_AUDIT_PATH,
    coverage_matrix_path: Annotated[
        Path, typer.Option("--coverage-matrix")
    ] = DEFAULT_REOPEN_GATE_COVERAGE_MATRIX_PATH,
    dependency_diagnostics_path: Annotated[
        Path, typer.Option("--dependency-diagnostics")
    ] = DEFAULT_REOPEN_GATE_DEPENDENCY_DIAGNOSTICS_PATH,
    owner_approval: Annotated[bool, typer.Option("--owner-approval/--no-owner-approval")] = False,
) -> None:
    payload = run_first_layer_reopen_gate_pack(
        policy_path=policy_path,
        free_feature_final_path=free_feature_final_path,
        participation_final_path=participation_final_path,
        channel_closeout_path=channel_closeout_path,
        free_feature_pit_audit_path=free_feature_pit_audit_path,
        coverage_matrix_path=coverage_matrix_path,
        dependency_diagnostics_path=dependency_diagnostics_path,
        owner_approval=owner_approval,
    )
    _print_payload("First-layer reopen gate", payload)


@trends_app.command("first-layer-current-state")
def first_layer_current_state_command(
    policy_path: Annotated[
        Path, typer.Option("--policy")
    ] = DEFAULT_FIRST_LAYER_CURRENT_STATE_POLICY_PATH,
    predictions_path: Annotated[
        Path, typer.Option("--predictions")
    ] = DEFAULT_FIRST_LAYER_CURRENT_STATE_PREDICTIONS_PATH,
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_FREE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path | None, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_FREE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_FREE_RATES_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_FIRST_LAYER_CURRENT_STATE_OUTPUT_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = run_first_layer_current_state_pack(
        policy_path=policy_path,
        predictions_path=predictions_path,
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        output_root=output_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_payload("First-layer current state", payload)


@trends_app.command("first-layer-proxy-coverage-audit")
def first_layer_proxy_coverage_audit_command(
    policy_path: Annotated[Path, typer.Option("--policy")] = DEFAULT_PROXY_COVERAGE_POLICY_PATH,
    free_feature_registry_path: Annotated[
        Path, typer.Option("--free-feature-registry")
    ] = DEFAULT_PROXY_COVERAGE_FREE_REGISTRY_PATH,
    participation_proxy_registry_path: Annotated[
        Path, typer.Option("--participation-proxy-registry")
    ] = DEFAULT_PROXY_COVERAGE_PARTICIPATION_REGISTRY_PATH,
    coverage_matrix_path: Annotated[
        Path, typer.Option("--coverage-matrix")
    ] = DEFAULT_PROXY_COVERAGE_MATRIX_PATH,
    pit_contract_path: Annotated[
        Path, typer.Option("--pit-contract")
    ] = DEFAULT_PROXY_COVERAGE_PIT_CONTRACT_PATH,
    fmp_gate_path: Annotated[
        Path, typer.Option("--fmp-gate")
    ] = DEFAULT_PROXY_COVERAGE_FMP_GATE_PATH,
    feature_root: Annotated[
        Path, typer.Option("--feature-root")
    ] = DEFAULT_PROXY_COVERAGE_FEATURE_ROOT,
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_FREE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path | None, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_FREE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_FREE_RATES_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_PROXY_COVERAGE_OUTPUT_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = run_first_layer_proxy_coverage_audit_pack(
        policy_path=policy_path,
        free_feature_registry_path=free_feature_registry_path,
        participation_proxy_registry_path=participation_proxy_registry_path,
        coverage_matrix_path=coverage_matrix_path,
        pit_contract_path=pit_contract_path,
        fmp_gate_path=fmp_gate_path,
        feature_root=feature_root,
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        output_root=output_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_payload("First-layer proxy coverage audit", payload)


@trends_app.command("equal-weight-proxy-data-fix")
def equal_weight_proxy_data_fix_command(
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_FREE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path | None, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_FREE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_FREE_RATES_PATH,
    download_manifest_path: Annotated[
        Path, typer.Option("--download-manifest")
    ] = DEFAULT_EQUAL_WEIGHT_PROXY_DOWNLOAD_MANIFEST_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_EQUAL_WEIGHT_PROXY_OUTPUT_ROOT,
    repair_start: Annotated[
        str, typer.Option("--repair-start")
    ] = DEFAULT_EQUAL_WEIGHT_PROXY_REPAIR_START.isoformat(),
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
    write_price_cache: Annotated[
        bool, typer.Option("--write-price-cache/--no-write-price-cache")
    ] = True,
) -> None:
    payload = run_equal_weight_proxy_data_fix_pack(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        download_manifest_path=download_manifest_path,
        output_root=output_root,
        as_of_date=_parse_optional_date(as_of),
        repair_start=date.fromisoformat(repair_start),
        write_price_cache=write_price_cache,
    )
    _print_payload("Equal-weight proxy data fix", payload)


@trends_app.command("first-layer-objective-validation-redesign")
def first_layer_objective_validation_redesign_command(
    policy_path: Annotated[
        Path, typer.Option("--policy")
    ] = DEFAULT_OBJECTIVE_VALIDATION_POLICY_PATH,
    current_state_summary_path: Annotated[
        Path, typer.Option("--current-state-summary")
    ] = DEFAULT_OBJECTIVE_VALIDATION_CURRENT_STATE_SUMMARY_PATH,
    failure_taxonomy_path: Annotated[
        Path, typer.Option("--failure-taxonomy")
    ] = DEFAULT_OBJECTIVE_VALIDATION_FAILURE_TAXONOMY_PATH,
    benchmark_consistency_path: Annotated[
        Path, typer.Option("--benchmark-consistency")
    ] = DEFAULT_OBJECTIVE_VALIDATION_BENCHMARK_CONSISTENCY_PATH,
    proxy_audit_path: Annotated[
        Path, typer.Option("--proxy-audit")
    ] = DEFAULT_OBJECTIVE_VALIDATION_PROXY_AUDIT_PATH,
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_FREE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path | None, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_FREE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_FREE_RATES_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_OBJECTIVE_VALIDATION_OUTPUT_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = run_first_layer_objective_validation_redesign_pack(
        policy_path=policy_path,
        current_state_summary_path=current_state_summary_path,
        failure_taxonomy_path=failure_taxonomy_path,
        benchmark_consistency_path=benchmark_consistency_path,
        proxy_audit_path=proxy_audit_path,
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        output_root=output_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_payload("First-layer objective validation redesign", payload)


@trends_app.command("first-layer-performance-gate-audit")
def first_layer_performance_gate_audit_command(
    policy_path: Annotated[
        Path, typer.Option("--policy")
    ] = DEFAULT_FIRST_LAYER_PERFORMANCE_GATE_AUDIT_POLICY_PATH,
    current_state_summary_path: Annotated[
        Path, typer.Option("--current-state-summary")
    ] = DEFAULT_PERF_GATE_AUDIT_CURRENT_STATE_PATH,
    failure_taxonomy_path: Annotated[
        Path, typer.Option("--failure-taxonomy")
    ] = DEFAULT_PERF_GATE_AUDIT_FAILURE_TAXONOMY_PATH,
    objective_validation_path: Annotated[
        Path, typer.Option("--objective-validation")
    ] = DEFAULT_PERF_GATE_AUDIT_OBJECTIVE_PATH,
    challenger_matrix_path: Annotated[
        Path, typer.Option("--challenger-matrix")
    ] = DEFAULT_PERF_GATE_AUDIT_CHALLENGER_MATRIX_PATH,
    actual_path_path: Annotated[
        Path, typer.Option("--actual-path")
    ] = DEFAULT_ACTUAL_PATH_YAML_PATH,
    coverage_simulation_path: Annotated[
        Path, typer.Option("--coverage-simulation")
    ] = DEFAULT_COVERAGE_SIMULATION_YAML_PATH,
    slice_matrix_path: Annotated[
        Path, typer.Option("--slice-matrix")
    ] = DEFAULT_2022_SLICE_YAML_PATH,
    defensive_inventory_path: Annotated[
        Path, typer.Option("--defensive-inventory")
    ] = DEFAULT_REGRESSION_INVENTORY_YAML_PATH,
    selection_rule_path: Annotated[
        Path, typer.Option("--selection-rule")
    ] = DEFAULT_COVERAGE_SELECTION_RULE_PATH,
    coverage_final_path: Annotated[
        Path, typer.Option("--coverage-final")
    ] = DEFAULT_FINAL_MATRIX_YAML_PATH,
    return_seeking_2022_contrast_path: Annotated[
        Path, typer.Option("--return-seeking-2022-contrast")
    ] = DEFAULT_PERF_GATE_AUDIT_2022_CONTRAST_PATH,
    return_seeking_beta_tqqq_attribution_path: Annotated[
        Path, typer.Option("--return-seeking-beta-tqqq-attribution")
    ] = DEFAULT_PERF_GATE_AUDIT_ATTRIBUTION_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_FIRST_LAYER_PERFORMANCE_GATE_AUDIT_OUTPUT_ROOT,
) -> None:
    payload = run_first_layer_performance_gate_audit_pack(
        policy_path=policy_path,
        current_state_summary_path=current_state_summary_path,
        failure_taxonomy_path=failure_taxonomy_path,
        objective_validation_path=objective_validation_path,
        challenger_matrix_path=challenger_matrix_path,
        actual_path_path=actual_path_path,
        coverage_simulation_path=coverage_simulation_path,
        slice_matrix_path=slice_matrix_path,
        defensive_inventory_path=defensive_inventory_path,
        selection_rule_path=selection_rule_path,
        coverage_final_path=coverage_final_path,
        return_seeking_2022_contrast_path=return_seeking_2022_contrast_path,
        return_seeking_beta_tqqq_attribution_path=return_seeking_beta_tqqq_attribution_path,
        output_root=output_root,
    )
    _print_payload("First-layer performance gate audit", payload)


@trends_app.command("first-layer-gate-policy-v2-reconciliation")
def first_layer_gate_policy_v2_reconciliation_command(
    recommended_gate_policy_path: Annotated[
        Path, typer.Option("--recommended-gate-policy")
    ] = DEFAULT_GATE_POLICY_V2_RECOMMENDED_PATH,
    gate_ablation_matrix_path: Annotated[
        Path, typer.Option("--gate-ablation-matrix")
    ] = DEFAULT_GATE_POLICY_V2_ABLATION_PATH,
    threshold_sensitivity_path: Annotated[
        Path, typer.Option("--threshold-sensitivity")
    ] = DEFAULT_GATE_POLICY_V2_THRESHOLD_PATH,
    active_selection_rule_path: Annotated[
        Path, typer.Option("--active-selection-rule")
    ] = DEFAULT_COVERAGE_SELECTION_RULE_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_GATE_POLICY_V2_OUTPUT_ROOT,
) -> None:
    payload = run_first_layer_gate_policy_v2_reconciliation_pack(
        recommended_gate_policy_path=recommended_gate_policy_path,
        gate_ablation_matrix_path=gate_ablation_matrix_path,
        threshold_sensitivity_path=threshold_sensitivity_path,
        active_selection_rule_path=active_selection_rule_path,
        output_root=output_root,
    )
    _print_payload("First-layer gate policy v2 reconciliation", payload)


@trends_app.command("first-layer-active-selection-rule-audit")
def first_layer_active_selection_rule_audit_command(
    active_selection_rule_path: Annotated[
        Path, typer.Option("--active-selection-rule")
    ] = DEFAULT_COVERAGE_SELECTION_RULE_PATH,
    actual_path_path: Annotated[
        Path, typer.Option("--actual-path")
    ] = DEFAULT_ACTUAL_PATH_YAML_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_ACTIVE_SELECTION_AUDIT_OUTPUT_ROOT,
) -> None:
    payload = run_first_layer_active_selection_rule_audit_pack(
        active_selection_rule_path=active_selection_rule_path,
        actual_path_path=actual_path_path,
        output_root=output_root,
    )
    _print_payload("First-layer active selection rule audit", payload)


@trends_app.command("first-layer-active-selection-policy-v2")
def first_layer_active_selection_policy_v2_command(
    challenger_matrix_path: Annotated[
        Path, typer.Option("--challenger-matrix")
    ] = DEFAULT_PERF_GATE_AUDIT_CHALLENGER_MATRIX_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_ACTIVE_SELECTION_POLICY_V2_OUTPUT_ROOT,
) -> None:
    payload = run_first_layer_active_selection_policy_v2_pack(
        challenger_matrix_path=challenger_matrix_path,
        output_root=output_root,
    )
    _print_payload("First-layer active selection policy v2", payload)


@trends_app.command("first-layer-challenger-matrix-v2")
def first_layer_challenger_matrix_v2_command(
    challenger_matrix_path: Annotated[
        Path, typer.Option("--challenger-matrix")
    ] = DEFAULT_PERF_GATE_AUDIT_CHALLENGER_MATRIX_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_CHALLENGER_MATRIX_V2_OUTPUT_ROOT,
) -> None:
    payload = run_first_layer_challenger_matrix_v2_pack(
        challenger_matrix_path=challenger_matrix_path,
        output_root=output_root,
    )
    _print_payload("First-layer challenger matrix v2", payload)


@trends_app.command("first-layer-boundary-owner-review")
def first_layer_boundary_owner_review_command(
    challenger_matrix_path: Annotated[
        Path, typer.Option("--challenger-matrix")
    ] = DEFAULT_PERF_GATE_AUDIT_CHALLENGER_MATRIX_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_BOUNDARY_OWNER_REVIEW_OUTPUT_ROOT,
) -> None:
    payload = run_first_layer_boundary_candidate_owner_review_pack(
        challenger_matrix_path=challenger_matrix_path,
        output_root=output_root,
    )
    _print_payload("First-layer boundary candidate owner review", payload)


@trends_app.command("first-layer-candidate-actual-path-validation")
def first_layer_candidate_actual_path_validation_command(
    challenger_matrix_path: Annotated[
        Path, typer.Option("--challenger-matrix")
    ] = DEFAULT_PERF_GATE_AUDIT_CHALLENGER_MATRIX_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_CANDIDATE_ACTUAL_PATH_VALIDATION_OUTPUT_ROOT,
) -> None:
    payload = run_first_layer_candidate_actual_path_validation_pack(
        challenger_matrix_path=challenger_matrix_path,
        output_root=output_root,
    )
    _print_payload("First-layer candidate actual-path validation", payload)


@trends_app.command("candidate-signal-prediction-artifact-audit")
def candidate_signal_prediction_artifact_audit_command(
    challenger_matrix_path: Annotated[
        Path, typer.Option("--challenger-matrix")
    ] = DEFAULT_PERF_GATE_AUDIT_CHALLENGER_MATRIX_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_CANDIDATE_ARTIFACT_AUDIT_OUTPUT_ROOT,
) -> None:
    payload = run_candidate_signal_prediction_artifact_audit_pack(
        challenger_matrix_path=challenger_matrix_path,
        output_root=output_root,
    )
    _print_payload("Candidate signal prediction artifact audit", payload)


@trends_app.command("candidate-signal-binding-schema-poc")
def candidate_signal_binding_schema_poc_command(
    candidate_id: Annotated[str, typer.Option("--candidate-id")] = "baseline",
    source_predictions: Annotated[
        Path,
        typer.Option("--source-predictions"),
    ] = DEFAULT_CANDIDATE_SIGNAL_BINDING_SOURCE_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir"),
    ] = DEFAULT_CANDIDATE_SIGNAL_BINDING_SCHEMA_OUTPUT_ROOT,
    mode: Annotated[str, typer.Option("--mode")] = "schema_migration_poc",
) -> None:
    payload = run_candidate_signal_binding_schema_poc(
        candidate_id=candidate_id,
        source_predictions=source_predictions,
        output_dir=output_dir,
        mode=mode,
    )
    _print_payload("Candidate signal binding schema POC", payload)


@trends_app.command("first-layer-candidate-generator-framework")
def first_layer_candidate_generator_framework_command(
    candidate_id: Annotated[str, typer.Option("--candidate-id")],
    target_asset: Annotated[str, typer.Option("--target-asset")],
    start_date: Annotated[str, typer.Option("--start-date")],
    end_date: Annotated[str, typer.Option("--end-date")],
    horizon: Annotated[str, typer.Option("--horizon")],
    output_dir: Annotated[Path, typer.Option("--output-dir")],
    mode: Annotated[str, typer.Option("--mode")],
) -> None:
    payload = run_candidate_generator(
        candidate_id=candidate_id,
        target_asset=target_asset,
        start_date=date.fromisoformat(start_date),
        end_date=date.fromisoformat(end_date),
        horizon=horizon,
        output_dir=output_dir,
        mode=mode,
    )
    _print_payload("First-layer candidate generator framework", payload)


@trends_app.command("first-layer-candidate-generators-regenerate")
def first_layer_candidate_generators_regenerate_command(
    candidates: Annotated[str, typer.Option("--candidates")],
    target_assets: Annotated[str, typer.Option("--target-assets")],
    start_date: Annotated[str, typer.Option("--start-date")],
    end_date: Annotated[str, typer.Option("--end-date")],
    horizons: Annotated[str, typer.Option("--horizons")],
    output_dir: Annotated[Path, typer.Option("--output-dir")],
    mode: Annotated[str, typer.Option("--mode")],
    prices_path: Annotated[
        Path, typer.Option("--prices-path")
    ] = DEFAULT_REGENERATED_PRICES_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path")
    ] = DEFAULT_REGENERATED_RATES_PATH,
    marketstack_prices_path: Annotated[
        Path | None, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_REGENERATED_MARKETSTACK_PRICES_PATH,
) -> None:
    payload = run_first_layer_candidate_generators_regenerate(
        candidates=candidates,
        target_assets=target_assets,
        start_date=date.fromisoformat(start_date),
        end_date=date.fromisoformat(end_date),
        horizons=horizons,
        output_dir=output_dir,
        mode=mode,
        prices_path=prices_path,
        rates_path=rates_path,
        marketstack_prices_path=marketstack_prices_path,
    )
    _print_payload("First-layer candidate generators regenerate", payload)


@trends_app.command("regenerated-candidate-actual-path-validation")
def regenerated_candidate_actual_path_validation_command(
    input_dir: Annotated[Path, typer.Option("--input-dir")],
    candidates: Annotated[str, typer.Option("--candidates")],
    target_assets: Annotated[str, typer.Option("--target-assets")],
    horizons: Annotated[str, typer.Option("--horizons")],
    output_dir: Annotated[Path, typer.Option("--output-dir")],
    mode: Annotated[str, typer.Option("--mode")],
    prices_path: Annotated[
        Path, typer.Option("--prices-path")
    ] = DEFAULT_REGENERATED_PRICES_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path")
    ] = DEFAULT_REGENERATED_RATES_PATH,
    marketstack_prices_path: Annotated[
        Path | None, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_REGENERATED_MARKETSTACK_PRICES_PATH,
) -> None:
    payload = run_regenerated_candidate_actual_path_validation(
        input_dir=input_dir or DEFAULT_REGENERATED_ACTUAL_PATH_INPUT_ROOT,
        candidates=candidates,
        target_assets=target_assets,
        horizons=horizons,
        output_dir=output_dir or DEFAULT_REGENERATED_ACTUAL_PATH_OUTPUT_ROOT,
        mode=mode,
        prices_path=prices_path,
        rates_path=rates_path,
        marketstack_prices_path=marketstack_prices_path,
    )
    _print_payload("Regenerated candidate actual-path validation", payload)


@trends_app.command("regenerated-candidate-inconclusive-diagnostics")
def regenerated_candidate_inconclusive_diagnostics_command(
    validation_dir: Annotated[Path, typer.Option("--validation-dir")],
    generator_dir: Annotated[Path, typer.Option("--generator-dir")],
    candidates: Annotated[str, typer.Option("--candidates")],
    target_assets: Annotated[str, typer.Option("--target-assets")],
    horizons: Annotated[str, typer.Option("--horizons")],
    output_dir: Annotated[Path, typer.Option("--output-dir")],
    mode: Annotated[str, typer.Option("--mode")],
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = DEFAULT_INCONCLUSIVE_DIAGNOSTICS_DOCS_ROOT,
) -> None:
    payload = run_regenerated_candidate_inconclusive_diagnostics(
        validation_dir=validation_dir or DEFAULT_INCONCLUSIVE_DIAGNOSTICS_VALIDATION_ROOT,
        generator_dir=generator_dir or DEFAULT_INCONCLUSIVE_DIAGNOSTICS_GENERATOR_ROOT,
        candidates=candidates,
        target_assets=target_assets,
        horizons=horizons,
        output_dir=output_dir or DEFAULT_INCONCLUSIVE_DIAGNOSTICS_OUTPUT_ROOT,
        mode=mode,
        docs_root=docs_root,
    )
    _print_payload("Regenerated candidate inconclusive diagnostics", payload)


@trends_app.command("candidate-generator-confidence-scaling-refinement-plan")
def candidate_generator_confidence_scaling_refinement_plan_command(
    diagnostics_dir: Annotated[Path, typer.Option("--diagnostics-dir")],
    validation_dir: Annotated[Path, typer.Option("--validation-dir")],
    generator_dir: Annotated[Path, typer.Option("--generator-dir")],
    candidates: Annotated[str, typer.Option("--candidates")],
    target_assets: Annotated[str, typer.Option("--target-assets")],
    horizons: Annotated[str, typer.Option("--horizons")],
    output_dir: Annotated[Path, typer.Option("--output-dir")],
    mode: Annotated[str, typer.Option("--mode")],
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = DEFAULT_CONFIDENCE_SCALING_DOCS_ROOT,
) -> None:
    payload = run_candidate_generator_confidence_scaling_refinement_plan(
        diagnostics_dir=diagnostics_dir or DEFAULT_CONFIDENCE_SCALING_DIAGNOSTICS_ROOT,
        validation_dir=validation_dir or DEFAULT_CONFIDENCE_SCALING_VALIDATION_ROOT,
        generator_dir=generator_dir or DEFAULT_CONFIDENCE_SCALING_GENERATOR_ROOT,
        candidates=candidates,
        target_assets=target_assets,
        horizons=horizons,
        output_dir=output_dir or DEFAULT_CONFIDENCE_SCALING_OUTPUT_ROOT,
        mode=mode,
        docs_root=docs_root,
    )
    _print_payload("Candidate generator confidence scaling refinement plan", payload)


@trends_app.command("refined-candidate-generators-regenerate")
def refined_candidate_generators_regenerate_command(
    refinement_plan_dir: Annotated[Path, typer.Option("--refinement-plan-dir")],
    original_generator_dir: Annotated[Path, typer.Option("--original-generator-dir")],
    candidates: Annotated[str, typer.Option("--candidates")],
    target_assets: Annotated[str, typer.Option("--target-assets")],
    horizons: Annotated[str, typer.Option("--horizons")],
    output_dir: Annotated[Path, typer.Option("--output-dir")],
    mode: Annotated[str, typer.Option("--mode")],
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = DEFAULT_REFINED_CANDIDATE_REGENERATION_DOCS_ROOT,
) -> None:
    payload = run_refined_candidate_generators_regenerate(
        refinement_plan_dir=refinement_plan_dir
        or DEFAULT_REFINED_CANDIDATE_REFINEMENT_PLAN_ROOT,
        original_generator_dir=original_generator_dir
        or DEFAULT_REFINED_CANDIDATE_ORIGINAL_GENERATOR_ROOT,
        candidates=candidates,
        target_assets=target_assets,
        horizons=horizons,
        output_dir=output_dir or DEFAULT_REFINED_CANDIDATE_REGENERATION_OUTPUT_ROOT,
        mode=mode,
        docs_root=docs_root,
    )
    _print_payload("Refined candidate generators regenerate", payload)


@trends_app.command("refined-candidate-actual-path-validation")
def refined_candidate_actual_path_validation_command(
    refined_generator_dir: Annotated[Path, typer.Option("--refined-generator-dir")],
    original_validation_dir: Annotated[Path, typer.Option("--original-validation-dir")],
    refinement_plan_dir: Annotated[Path, typer.Option("--refinement-plan-dir")],
    candidates: Annotated[str, typer.Option("--candidates")],
    target_assets: Annotated[str, typer.Option("--target-assets")],
    horizons: Annotated[str, typer.Option("--horizons")],
    output_dir: Annotated[Path, typer.Option("--output-dir")],
    mode: Annotated[str, typer.Option("--mode")],
    prices_path: Annotated[
        Path, typer.Option("--prices-path")
    ] = DEFAULT_REGENERATED_PRICES_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path")
    ] = DEFAULT_REGENERATED_RATES_PATH,
    marketstack_prices_path: Annotated[
        Path | None, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_REGENERATED_MARKETSTACK_PRICES_PATH,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = DEFAULT_REFINED_ACTUAL_PATH_DOCS_ROOT,
) -> None:
    payload = run_refined_candidate_actual_path_validation(
        refined_generator_dir=refined_generator_dir
        or DEFAULT_REFINED_ACTUAL_PATH_REFINED_GENERATOR_ROOT,
        original_validation_dir=original_validation_dir
        or DEFAULT_REFINED_ACTUAL_PATH_ORIGINAL_VALIDATION_ROOT,
        refinement_plan_dir=refinement_plan_dir
        or DEFAULT_REFINED_ACTUAL_PATH_REFINEMENT_PLAN_ROOT,
        candidates=candidates,
        target_assets=target_assets,
        horizons=horizons,
        output_dir=output_dir or DEFAULT_REFINED_ACTUAL_PATH_OUTPUT_ROOT,
        mode=mode,
        prices_path=prices_path,
        rates_path=rates_path,
        marketstack_prices_path=marketstack_prices_path,
        docs_root=docs_root,
    )
    _print_payload("Refined candidate actual-path validation", payload)


@trends_app.command("refined-candidate-local-edge-scope-review")
def refined_candidate_local_edge_scope_review_command(
    refined_validation_dir: Annotated[Path, typer.Option("--refined-validation-dir")],
    refined_generator_dir: Annotated[Path, typer.Option("--refined-generator-dir")],
    refinement_plan_dir: Annotated[Path, typer.Option("--refinement-plan-dir")],
    candidates: Annotated[str, typer.Option("--candidates")],
    continue_research_candidates: Annotated[
        str, typer.Option("--continue-research-candidates")
    ],
    reject_candidates: Annotated[str, typer.Option("--reject-candidates")],
    target_assets: Annotated[str, typer.Option("--target-assets")],
    horizons: Annotated[str, typer.Option("--horizons")],
    output_dir: Annotated[Path, typer.Option("--output-dir")],
    mode: Annotated[str, typer.Option("--mode")],
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = DEFAULT_REFINED_SCOPE_REVIEW_DOCS_ROOT,
) -> None:
    payload = run_refined_candidate_local_edge_scope_review(
        refined_validation_dir=refined_validation_dir
        or DEFAULT_REFINED_SCOPE_REVIEW_VALIDATION_ROOT,
        refined_generator_dir=refined_generator_dir
        or DEFAULT_REFINED_SCOPE_REVIEW_REFINED_GENERATOR_ROOT,
        refinement_plan_dir=refinement_plan_dir
        or DEFAULT_REFINED_SCOPE_REVIEW_REFINEMENT_PLAN_ROOT,
        candidates=candidates,
        continue_research_candidates=continue_research_candidates,
        reject_candidates=reject_candidates,
        target_assets=target_assets,
        horizons=horizons,
        output_dir=output_dir or DEFAULT_REFINED_SCOPE_REVIEW_OUTPUT_ROOT,
        mode=mode,
        docs_root=docs_root,
    )
    _print_payload("Refined candidate local edge scope review", payload)


@trends_app.command("scope-narrowed-candidate-generators-regenerate")
def scope_narrowed_candidate_generators_regenerate_command(
    scope_review_dir: Annotated[Path, typer.Option("--scope-review-dir")],
    refined_generator_dir: Annotated[Path, typer.Option("--refined-generator-dir")],
    refined_validation_dir: Annotated[Path, typer.Option("--refined-validation-dir")],
    include_candidates: Annotated[str, typer.Option("--include-candidates")],
    archive_candidates: Annotated[str, typer.Option("--archive-candidates")],
    target_assets: Annotated[str, typer.Option("--target-assets")],
    horizons: Annotated[str, typer.Option("--horizons")],
    output_dir: Annotated[Path, typer.Option("--output-dir")],
    mode: Annotated[str, typer.Option("--mode")],
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = DEFAULT_SCOPE_NARROWED_REGENERATION_DOCS_ROOT,
) -> None:
    payload = run_scope_narrowed_candidate_generators_regenerate(
        scope_review_dir=scope_review_dir or DEFAULT_SCOPE_NARROWED_SCOPE_REVIEW_ROOT,
        refined_generator_dir=refined_generator_dir
        or DEFAULT_SCOPE_NARROWED_REFINED_GENERATOR_ROOT,
        refined_validation_dir=refined_validation_dir
        or DEFAULT_SCOPE_NARROWED_REFINED_VALIDATION_ROOT,
        include_candidates=include_candidates,
        archive_candidates=archive_candidates,
        target_assets=target_assets,
        horizons=horizons,
        output_dir=output_dir or DEFAULT_SCOPE_NARROWED_REGENERATION_OUTPUT_ROOT,
        mode=mode,
        docs_root=docs_root,
    )
    _print_payload("Scope-narrowed candidate generators regenerate", payload)


@trends_app.command("scope-narrowed-candidate-actual-path-validation")
def scope_narrowed_candidate_actual_path_validation_command(
    scope_narrowed_generator_dir: Annotated[
        Path,
        typer.Option("--scope-narrowed-generator-dir"),
    ],
    scope_review_dir: Annotated[Path, typer.Option("--scope-review-dir")],
    refined_validation_dir: Annotated[Path, typer.Option("--refined-validation-dir")],
    include_candidates: Annotated[str, typer.Option("--include-candidates")],
    archived_candidates: Annotated[str, typer.Option("--archived-candidates")],
    target_assets: Annotated[str, typer.Option("--target-assets")],
    horizons: Annotated[str, typer.Option("--horizons")],
    output_dir: Annotated[Path, typer.Option("--output-dir")],
    mode: Annotated[str, typer.Option("--mode")],
    prices_path: Annotated[
        Path, typer.Option("--prices-path")
    ] = DEFAULT_REGENERATED_PRICES_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path")
    ] = DEFAULT_REGENERATED_RATES_PATH,
    marketstack_prices_path: Annotated[
        Path | None, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_REGENERATED_MARKETSTACK_PRICES_PATH,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = DEFAULT_SCOPE_NARROWED_ACTUAL_PATH_DOCS_ROOT,
) -> None:
    payload = run_scope_narrowed_candidate_actual_path_validation(
        scope_narrowed_generator_dir=scope_narrowed_generator_dir
        or DEFAULT_SCOPE_NARROWED_ACTUAL_PATH_GENERATOR_ROOT,
        scope_review_dir=scope_review_dir
        or DEFAULT_SCOPE_NARROWED_ACTUAL_PATH_SCOPE_REVIEW_ROOT,
        refined_validation_dir=refined_validation_dir
        or DEFAULT_SCOPE_NARROWED_ACTUAL_PATH_REFINED_VALIDATION_ROOT,
        include_candidates=include_candidates,
        archived_candidates=archived_candidates,
        target_assets=target_assets,
        horizons=horizons,
        output_dir=output_dir or DEFAULT_SCOPE_NARROWED_ACTUAL_PATH_OUTPUT_ROOT,
        mode=mode,
        prices_path=prices_path,
        rates_path=rates_path,
        marketstack_prices_path=marketstack_prices_path,
        docs_root=docs_root,
    )
    _print_payload("Scope-narrowed candidate actual-path validation", payload)


@trends_app.command("scope-narrowed-forward-observe-readiness-review")
def scope_narrowed_forward_observe_readiness_review_command(
    scope_validation_dir: Annotated[Path, typer.Option("--scope-validation-dir")],
    scope_generator_dir: Annotated[Path, typer.Option("--scope-generator-dir")],
    scope_review_dir: Annotated[Path, typer.Option("--scope-review-dir")],
    candidate: Annotated[str, typer.Option("--candidate")],
    rejected_candidates: Annotated[str, typer.Option("--rejected-candidates")],
    archived_candidates: Annotated[str, typer.Option("--archived-candidates")],
    target_assets: Annotated[str, typer.Option("--target-assets")],
    horizons: Annotated[str, typer.Option("--horizons")],
    output_dir: Annotated[Path, typer.Option("--output-dir")],
    mode: Annotated[str, typer.Option("--mode")],
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = DEFAULT_SCOPE_NARROWED_FORWARD_OBSERVE_DOCS_ROOT,
) -> None:
    payload = run_scope_narrowed_forward_observe_readiness_review(
        scope_validation_dir=scope_validation_dir
        or DEFAULT_SCOPE_NARROWED_FORWARD_OBSERVE_VALIDATION_ROOT,
        scope_generator_dir=scope_generator_dir
        or DEFAULT_SCOPE_NARROWED_FORWARD_OBSERVE_GENERATOR_ROOT,
        scope_review_dir=scope_review_dir
        or DEFAULT_SCOPE_NARROWED_FORWARD_OBSERVE_SCOPE_REVIEW_ROOT,
        candidate=candidate,
        rejected_candidates=rejected_candidates,
        archived_candidates=archived_candidates,
        target_assets=target_assets,
        horizons=horizons,
        output_dir=output_dir or DEFAULT_SCOPE_NARROWED_FORWARD_OBSERVE_OUTPUT_ROOT,
        mode=mode,
        docs_root=docs_root,
    )
    _print_payload("Scope-narrowed forward observe readiness review", payload)


@trends_app.command("forward-observe-evidence-accumulation-plan")
def forward_observe_evidence_accumulation_plan_command(
    readiness_dir: Annotated[
        Path, typer.Option("--readiness-dir")
    ] = DEFAULT_FORWARD_OBSERVE_EVIDENCE_READINESS_ROOT,
    candidate: Annotated[
        str, typer.Option("--candidate")
    ] = "volatility_regime_scope_narrowed_risk_cap_v1",
    target_assets: Annotated[str, typer.Option("--target-assets")] = "QQQ,SPY,SMH",
    horizons: Annotated[str, typer.Option("--horizons")] = "5d,10d,20d",
    output_dir: Annotated[
        Path, typer.Option("--output-dir")
    ] = DEFAULT_FORWARD_OBSERVE_EVIDENCE_OUTPUT_ROOT,
    mode: Annotated[str, typer.Option("--mode")] = FORWARD_OBSERVE_EVIDENCE_MODE,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = DEFAULT_FORWARD_OBSERVE_EVIDENCE_DOCS_ROOT,
) -> None:
    payload = run_forward_observe_evidence_accumulation_plan(
        readiness_dir=readiness_dir,
        candidate=candidate,
        target_assets=target_assets,
        horizons=horizons,
        output_dir=output_dir,
        mode=mode,
        docs_root=docs_root,
    )
    _print_payload("Forward observe evidence accumulation plan", payload)


@trends_app.command("risk-cap-cooldown-decay-design")
def risk_cap_cooldown_decay_design_command(
    policy_path: Annotated[
        Path, typer.Option("--policy")
    ] = DEFAULT_RISK_CAP_COOLDOWN_DECAY_POLICY_PATH,
    source_dir: Annotated[
        Path, typer.Option("--source-dir")
    ] = DEFAULT_RISK_CAP_COOLDOWN_DECAY_SOURCE_ROOT,
    output_dir: Annotated[
        Path, typer.Option("--output-dir")
    ] = DEFAULT_RISK_CAP_COOLDOWN_DECAY_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = DEFAULT_RISK_CAP_COOLDOWN_DECAY_DOCS_ROOT,
    mode: Annotated[str, typer.Option("--mode")] = RISK_CAP_COOLDOWN_DECAY_MODE,
) -> None:
    payload = run_risk_cap_cooldown_decay_design(
        policy_path=policy_path,
        source_dir=source_dir,
        output_dir=output_dir,
        docs_root=docs_root,
        mode=mode,
    )
    _print_payload("Risk-cap cooldown / decay design", payload)


@trends_app.command("first-layer-new-candidate-family-prioritization")
def first_layer_new_candidate_family_prioritization_command(
    output_dir: Annotated[
        Path, typer.Option("--output-dir")
    ] = DEFAULT_NEW_CANDIDATE_FAMILY_OUTPUT_ROOT,
    mode: Annotated[str, typer.Option("--mode")] = NEW_CANDIDATE_FAMILY_MODE,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = DEFAULT_NEW_CANDIDATE_FAMILY_DOCS_ROOT,
) -> None:
    payload = run_first_layer_new_candidate_family_prioritization(
        output_dir=output_dir,
        mode=mode,
        docs_root=docs_root,
    )
    _print_payload("First-layer new candidate family prioritization", payload)


@trends_app.command("breadth-participation-candidate-family-feasibility-audit")
def breadth_participation_candidate_family_feasibility_audit_command(
    target_etfs: Annotated[str, typer.Option("--target-etfs")],
    target_assets: Annotated[str, typer.Option("--target-assets")],
    candidate_family: Annotated[str, typer.Option("--candidate-family")],
    output_dir: Annotated[Path, typer.Option("--output-dir")],
    mode: Annotated[str, typer.Option("--mode")],
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = DEFAULT_BREADTH_FEASIBILITY_DOCS_ROOT,
) -> None:
    payload = run_breadth_participation_feasibility_audit(
        target_etfs=target_etfs,
        target_assets=target_assets,
        candidate_family=candidate_family,
        output_dir=output_dir,
        mode=mode or BREADTH_FEASIBILITY_MODE,
        docs_root=docs_root,
    )
    _print_payload("Breadth participation candidate family feasibility audit", payload)


@trends_app.command("current-constituents-breadth-proxy-diagnostics")
def current_constituents_breadth_proxy_diagnostics_command(
    feasibility_dir: Annotated[
        Path, typer.Option("--feasibility-dir")
    ] = DEFAULT_CURRENT_CONSTITUENTS_BREADTH_PROXY_FEASIBILITY_ROOT,
    current_constituents_dir: Annotated[
        Path | None, typer.Option("--current-constituents-dir")
    ] = None,
    target_etfs: Annotated[str, typer.Option("--target-etfs")] = "QQQ,SPY,SMH",
    target_assets: Annotated[str, typer.Option("--target-assets")] = "QQQ,SPY,SMH",
    horizons: Annotated[str, typer.Option("--horizons")] = "5d,10d,20d",
    output_dir: Annotated[
        Path, typer.Option("--output-dir")
    ] = DEFAULT_CURRENT_CONSTITUENTS_BREADTH_PROXY_OUTPUT_ROOT,
    mode: Annotated[str, typer.Option("--mode")] = CURRENT_CONSTITUENTS_BREADTH_PROXY_MODE,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = DEFAULT_CURRENT_CONSTITUENTS_BREADTH_PROXY_DOCS_ROOT,
) -> None:
    payload = run_current_constituents_breadth_proxy_diagnostics(
        feasibility_dir=feasibility_dir,
        current_constituents_dir=current_constituents_dir,
        target_etfs=target_etfs,
        target_assets=target_assets,
        horizons=horizons,
        output_dir=output_dir,
        docs_root=docs_root,
        mode=mode,
    )
    _print_payload("Current constituents breadth proxy diagnostics", payload)


@trends_app.command("breadth-proxy-signal-concept-selection")
def breadth_proxy_signal_concept_selection_command(
    diagnostics_dir: Annotated[
        Path, typer.Option("--diagnostics-dir")
    ] = DEFAULT_BREADTH_PROXY_SIGNAL_SELECTION_DIAGNOSTICS_ROOT,
    output_dir: Annotated[
        Path, typer.Option("--output-dir")
    ] = DEFAULT_BREADTH_PROXY_SIGNAL_SELECTION_OUTPUT_ROOT,
    mode: Annotated[str, typer.Option("--mode")] = BREADTH_PROXY_SIGNAL_SELECTION_MODE,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = DEFAULT_BREADTH_PROXY_SIGNAL_SELECTION_DOCS_ROOT,
) -> None:
    payload = run_breadth_proxy_signal_concept_selection(
        diagnostics_dir=diagnostics_dir,
        output_dir=output_dir,
        docs_root=docs_root,
        mode=mode,
    )
    _print_payload("Breadth proxy signal concept selection", payload)


@trends_app.command("ai-semiconductor-leadership-feasibility-audit")
def ai_semiconductor_leadership_feasibility_audit_command(
    target_assets: Annotated[str, typer.Option("--target-assets")] = "QQQ,SMH",
    horizons: Annotated[str, typer.Option("--horizons")] = "5d,10d,20d",
    candidate_family: Annotated[
        str, typer.Option("--candidate-family")
    ] = "ai_semiconductor_leadership",
    output_dir: Annotated[
        Path, typer.Option("--output-dir")
    ] = DEFAULT_AI_SEMICONDUCTOR_LEADERSHIP_OUTPUT_ROOT,
    mode: Annotated[str, typer.Option("--mode")] = AI_SEMICONDUCTOR_LEADERSHIP_MODE,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = DEFAULT_AI_SEMICONDUCTOR_LEADERSHIP_DOCS_ROOT,
) -> None:
    payload = run_ai_semiconductor_leadership_feasibility_audit(
        target_assets=target_assets,
        horizons=horizons,
        candidate_family=candidate_family,
        output_dir=output_dir,
        docs_root=docs_root,
        mode=mode,
    )
    _print_payload("AI / 半导体 leadership 可行性审计", payload)


@trends_app.command("ai-semiconductor-leadership-generator-poc")
def ai_semiconductor_leadership_generator_poc_command(
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_RATES_PATH,
    marketstack_prices_path: Annotated[
        Path | None, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    policy_path: Annotated[
        Path, typer.Option("--policy")
    ] = DEFAULT_AI_SEMICONDUCTOR_LEADERSHIP_POC_POLICY_PATH,
    feasibility_dir: Annotated[
        Path, typer.Option("--feasibility-dir")
    ] = DEFAULT_AI_SEMICONDUCTOR_LEADERSHIP_POC_FEASIBILITY_ROOT,
    target_assets: Annotated[str, typer.Option("--target-assets")] = "QQQ,SMH",
    horizons: Annotated[str, typer.Option("--horizons")] = "5d,10d,20d",
    candidates: Annotated[
        str, typer.Option("--candidates")
    ] = (
        "smh_relative_strength_leadership_v1,"
        "ai_semiconductor_leadership_quality_v1,"
        "ai_core_basket_leadership_v1"
    ),
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
    quality_as_of: Annotated[str | None, typer.Option("--quality-as-of")] = None,
    output_dir: Annotated[
        Path, typer.Option("--output-dir")
    ] = DEFAULT_AI_SEMICONDUCTOR_LEADERSHIP_POC_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = DEFAULT_AI_SEMICONDUCTOR_LEADERSHIP_POC_DOCS_ROOT,
    mode: Annotated[str, typer.Option("--mode")] = AI_SEMICONDUCTOR_LEADERSHIP_POC_MODE,
) -> None:
    payload = run_ai_semiconductor_leadership_generator_poc(
        prices_path=prices_path,
        rates_path=rates_path,
        marketstack_prices_path=marketstack_prices_path,
        policy_path=policy_path,
        feasibility_dir=feasibility_dir,
        target_assets=target_assets,
        horizons=horizons,
        candidates=candidates,
        start_date=start_date,
        end_date=end_date,
        quality_as_of=quality_as_of,
        output_dir=output_dir,
        docs_root=docs_root,
        mode=mode,
    )
    _print_payload("AI / 半导体 leadership generator POC", payload)


@trends_app.command("ai-leadership-actual-path-validation")
def ai_leadership_actual_path_validation_command(
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_RATES_PATH,
    marketstack_prices_path: Annotated[
        Path | None, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    generator_dir: Annotated[
        Path, typer.Option("--generator-dir")
    ] = DEFAULT_AI_LEADERSHIP_ACTUAL_PATH_GENERATOR_ROOT,
    policy_path: Annotated[
        Path, typer.Option("--policy")
    ] = DEFAULT_AI_LEADERSHIP_ACTUAL_PATH_POLICY_PATH,
    target_assets: Annotated[str, typer.Option("--target-assets")] = "QQQ,SMH",
    horizons: Annotated[str, typer.Option("--horizons")] = "5d,10d,20d",
    candidates: Annotated[
        str, typer.Option("--candidates")
    ] = (
        "smh_relative_strength_leadership_v1,"
        "ai_semiconductor_leadership_quality_v1,"
        "ai_core_basket_leadership_v1"
    ),
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
    quality_as_of: Annotated[str | None, typer.Option("--quality-as-of")] = None,
    output_dir: Annotated[
        Path, typer.Option("--output-dir")
    ] = DEFAULT_AI_LEADERSHIP_ACTUAL_PATH_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = DEFAULT_AI_LEADERSHIP_ACTUAL_PATH_DOCS_ROOT,
    mode: Annotated[str, typer.Option("--mode")] = AI_LEADERSHIP_ACTUAL_PATH_MODE,
) -> None:
    payload = run_ai_leadership_actual_path_validation(
        prices_path=prices_path,
        rates_path=rates_path,
        marketstack_prices_path=marketstack_prices_path,
        generator_dir=generator_dir,
        policy_path=policy_path,
        target_assets=target_assets,
        horizons=horizons,
        candidates=candidates,
        start_date=start_date,
        end_date=end_date,
        quality_as_of=quality_as_of,
        output_dir=output_dir,
        docs_root=docs_root,
        mode=mode,
    )
    _print_payload("AI / 半导体 leadership actual-path validation", payload)


@trends_app.command("ai-leadership-scope-review")
def ai_leadership_scope_review_command(
    validation_dir: Annotated[
        Path, typer.Option("--validation-dir")
    ] = DEFAULT_AI_LEADERSHIP_SCOPE_REVIEW_VALIDATION_ROOT,
    policy_path: Annotated[
        Path, typer.Option("--policy")
    ] = DEFAULT_AI_LEADERSHIP_SCOPE_REVIEW_POLICY_PATH,
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_RATES_PATH,
    marketstack_prices_path: Annotated[
        Path | None, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    quality_as_of: Annotated[str | None, typer.Option("--quality-as-of")] = None,
    output_dir: Annotated[
        Path, typer.Option("--output-dir")
    ] = DEFAULT_AI_LEADERSHIP_SCOPE_REVIEW_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = DEFAULT_AI_LEADERSHIP_SCOPE_REVIEW_DOCS_ROOT,
    mode: Annotated[str, typer.Option("--mode")] = AI_LEADERSHIP_SCOPE_REVIEW_MODE,
) -> None:
    payload = run_ai_leadership_scope_review(
        validation_dir=validation_dir,
        policy_path=policy_path,
        prices_path=prices_path,
        rates_path=rates_path,
        marketstack_prices_path=marketstack_prices_path,
        quality_as_of=quality_as_of,
        output_dir=output_dir,
        docs_root=docs_root,
        mode=mode,
    )
    _print_payload("AI / 半导体 leadership scope review", payload)


@trends_app.command("liquidity-rates-pressure-feasibility-audit")
def liquidity_rates_pressure_feasibility_audit_command(
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_RATES_PATH,
    marketstack_prices_path: Annotated[
        Path | None, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    target_assets: Annotated[str, typer.Option("--target-assets")] = "QQQ,SMH",
    horizons: Annotated[str, typer.Option("--horizons")] = "10d,20d,1m",
    quality_as_of: Annotated[str | None, typer.Option("--quality-as-of")] = None,
    output_dir: Annotated[
        Path, typer.Option("--output-dir")
    ] = DEFAULT_LIQUIDITY_RATES_FEASIBILITY_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = DEFAULT_LIQUIDITY_RATES_FEASIBILITY_DOCS_ROOT,
    mode: Annotated[str, typer.Option("--mode")] = LIQUIDITY_RATES_FEASIBILITY_MODE,
) -> None:
    payload = run_liquidity_rates_pressure_feasibility_audit(
        prices_path=prices_path,
        rates_path=rates_path,
        marketstack_prices_path=marketstack_prices_path,
        target_assets=target_assets,
        horizons=horizons,
        quality_as_of=quality_as_of,
        output_dir=output_dir,
        docs_root=docs_root,
        mode=mode,
    )
    _print_payload("Liquidity / rates pressure feasibility audit", payload)


@trends_app.command("liquidity-rates-pressure-generator-poc")
def liquidity_rates_pressure_generator_poc_command(
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_RATES_PATH,
    marketstack_prices_path: Annotated[
        Path | None, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    policy_path: Annotated[
        Path, typer.Option("--policy")
    ] = DEFAULT_LIQUIDITY_RATES_GENERATOR_POLICY_PATH,
    feasibility_dir: Annotated[
        Path, typer.Option("--feasibility-dir")
    ] = DEFAULT_LIQUIDITY_RATES_GENERATOR_FEASIBILITY_ROOT,
    target_assets: Annotated[str, typer.Option("--target-assets")] = "QQQ,SMH",
    horizons: Annotated[str, typer.Option("--horizons")] = "10d,20d,1m",
    candidates: Annotated[
        str, typer.Option("--candidates")
    ] = "duration_pressure_proxy_v1,rates_pressure_exposure_cap_modifier_v1",
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
    quality_as_of: Annotated[str | None, typer.Option("--quality-as-of")] = None,
    output_dir: Annotated[
        Path, typer.Option("--output-dir")
    ] = DEFAULT_LIQUIDITY_RATES_GENERATOR_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = DEFAULT_LIQUIDITY_RATES_GENERATOR_DOCS_ROOT,
    mode: Annotated[str, typer.Option("--mode")] = LIQUIDITY_RATES_GENERATOR_MODE,
) -> None:
    payload = run_liquidity_rates_pressure_generator_poc(
        prices_path=prices_path,
        rates_path=rates_path,
        marketstack_prices_path=marketstack_prices_path,
        policy_path=policy_path,
        feasibility_dir=feasibility_dir,
        target_assets=target_assets,
        horizons=horizons,
        candidates=candidates,
        start_date=start_date,
        end_date=end_date,
        quality_as_of=quality_as_of,
        output_dir=output_dir,
        docs_root=docs_root,
        mode=mode,
    )
    _print_payload("Liquidity / rates pressure generator POC", payload)


@trends_app.command("liquidity-rates-actual-path-validation")
def liquidity_rates_actual_path_validation_command(
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_RATES_PATH,
    marketstack_prices_path: Annotated[
        Path | None, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    generator_dir: Annotated[
        Path, typer.Option("--generator-dir")
    ] = DEFAULT_LIQUIDITY_RATES_ACTUAL_PATH_GENERATOR_ROOT,
    policy_path: Annotated[
        Path, typer.Option("--policy")
    ] = DEFAULT_LIQUIDITY_RATES_ACTUAL_PATH_POLICY_PATH,
    target_assets: Annotated[str, typer.Option("--target-assets")] = "QQQ,SMH",
    horizons: Annotated[str, typer.Option("--horizons")] = "10d,20d,1m",
    candidates: Annotated[
        str, typer.Option("--candidates")
    ] = "duration_pressure_proxy_v1,rates_pressure_exposure_cap_modifier_v1",
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
    quality_as_of: Annotated[str | None, typer.Option("--quality-as-of")] = None,
    output_dir: Annotated[
        Path, typer.Option("--output-dir")
    ] = DEFAULT_LIQUIDITY_RATES_ACTUAL_PATH_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = DEFAULT_LIQUIDITY_RATES_ACTUAL_PATH_DOCS_ROOT,
    mode: Annotated[str, typer.Option("--mode")] = LIQUIDITY_RATES_ACTUAL_PATH_MODE,
) -> None:
    payload = run_liquidity_rates_actual_path_validation(
        prices_path=prices_path,
        rates_path=rates_path,
        marketstack_prices_path=marketstack_prices_path,
        generator_dir=generator_dir,
        policy_path=policy_path,
        target_assets=target_assets,
        horizons=horizons,
        candidates=candidates,
        start_date=start_date,
        end_date=end_date,
        quality_as_of=quality_as_of,
        output_dir=output_dir,
        docs_root=docs_root,
        mode=mode,
    )
    _print_payload("Liquidity / rates actual-path validation", payload)


@trends_app.command("liquidity-rates-scope-review")
def liquidity_rates_scope_review_command(
    validation_dir: Annotated[
        Path, typer.Option("--validation-dir")
    ] = DEFAULT_LIQUIDITY_RATES_SCOPE_REVIEW_VALIDATION_ROOT,
    policy_path: Annotated[
        Path, typer.Option("--policy")
    ] = DEFAULT_LIQUIDITY_RATES_SCOPE_REVIEW_POLICY_PATH,
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_RATES_PATH,
    marketstack_prices_path: Annotated[
        Path | None, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    quality_as_of: Annotated[str | None, typer.Option("--quality-as-of")] = None,
    output_dir: Annotated[
        Path, typer.Option("--output-dir")
    ] = DEFAULT_LIQUIDITY_RATES_SCOPE_REVIEW_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = DEFAULT_LIQUIDITY_RATES_SCOPE_REVIEW_DOCS_ROOT,
    mode: Annotated[str, typer.Option("--mode")] = LIQUIDITY_RATES_SCOPE_REVIEW_MODE,
) -> None:
    payload = run_liquidity_rates_scope_review(
        validation_dir=validation_dir,
        policy_path=policy_path,
        prices_path=prices_path,
        rates_path=rates_path,
        marketstack_prices_path=marketstack_prices_path,
        quality_as_of=quality_as_of,
        output_dir=output_dir,
        docs_root=docs_root,
        mode=mode,
    )
    _print_payload("Liquidity / rates scope review", payload)


@trends_app.command("regime-state-machine-design-audit")
def regime_state_machine_design_audit_command(
    policy_path: Annotated[
        Path, typer.Option("--policy")
    ] = DEFAULT_REGIME_STATE_MACHINE_POLICY_PATH,
    output_dir: Annotated[
        Path, typer.Option("--output-dir")
    ] = DEFAULT_REGIME_STATE_MACHINE_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = DEFAULT_REGIME_STATE_MACHINE_DOCS_ROOT,
    mode: Annotated[str, typer.Option("--mode")] = REGIME_STATE_MACHINE_MODE,
) -> None:
    payload = run_regime_state_machine_design_audit(
        policy_path=policy_path,
        output_dir=output_dir,
        docs_root=docs_root,
        mode=mode,
    )
    _print_payload("Regime state machine design audit", payload)


@trends_app.command("regime-label-generator-diagnostic-poc")
def regime_label_generator_diagnostic_poc_command(
    policy_path: Annotated[
        Path, typer.Option("--policy")
    ] = DEFAULT_REGIME_LABEL_GENERATOR_POLICY_PATH,
    design_policy_path: Annotated[
        Path, typer.Option("--design-policy")
    ] = DEFAULT_REGIME_LABEL_GENERATOR_DESIGN_POLICY_PATH,
    prices_path: Annotated[
        Path, typer.Option("--prices-path")
    ] = DEFAULT_REGIME_LABEL_GENERATOR_PRICES_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path")
    ] = DEFAULT_REGIME_LABEL_GENERATOR_RATES_PATH,
    marketstack_prices_path: Annotated[
        Path | None, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_REGIME_LABEL_GENERATOR_MARKETSTACK_PRICES_PATH,
    quality_as_of: Annotated[str | None, typer.Option("--quality-as-of")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
    output_dir: Annotated[
        Path, typer.Option("--output-dir")
    ] = DEFAULT_REGIME_LABEL_GENERATOR_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = DEFAULT_REGIME_LABEL_GENERATOR_DOCS_ROOT,
    mode: Annotated[str, typer.Option("--mode")] = REGIME_LABEL_GENERATOR_MODE,
) -> None:
    payload = run_regime_label_generator_diagnostic_poc(
        policy_path=policy_path,
        design_policy_path=design_policy_path,
        prices_path=prices_path,
        rates_path=rates_path,
        marketstack_prices_path=marketstack_prices_path,
        quality_as_of=quality_as_of,
        start_date=start_date,
        end_date=end_date,
        output_dir=output_dir,
        docs_root=docs_root,
        mode=mode,
    )
    _print_payload("Regime label generator diagnostic POC", payload)


@trends_app.command("regime-segmented-candidate-validation")
def regime_segmented_candidate_validation_command(
    policy_path: Annotated[
        Path, typer.Option("--policy")
    ] = DEFAULT_REGIME_SEGMENTED_VALIDATION_POLICY_PATH,
    label_series_path: Annotated[
        Path, typer.Option("--label-series")
    ] = DEFAULT_REGIME_SEGMENTED_VALIDATION_LABEL_SERIES_PATH,
    label_summary_path: Annotated[
        Path, typer.Option("--label-summary")
    ] = DEFAULT_REGIME_SEGMENTED_VALIDATION_LABEL_SUMMARY_PATH,
    prices_path: Annotated[
        Path, typer.Option("--prices-path")
    ] = DEFAULT_REGIME_SEGMENTED_VALIDATION_PRICES_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path")
    ] = DEFAULT_REGIME_SEGMENTED_VALIDATION_RATES_PATH,
    marketstack_prices_path: Annotated[
        Path | None, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_REGIME_SEGMENTED_VALIDATION_MARKETSTACK_PRICES_PATH,
    quality_as_of: Annotated[str | None, typer.Option("--quality-as-of")] = None,
    output_dir: Annotated[
        Path, typer.Option("--output-dir")
    ] = DEFAULT_REGIME_SEGMENTED_VALIDATION_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = DEFAULT_REGIME_SEGMENTED_VALIDATION_DOCS_ROOT,
    mode: Annotated[str, typer.Option("--mode")] = REGIME_SEGMENTED_VALIDATION_MODE,
) -> None:
    payload = run_regime_segmented_candidate_validation(
        policy_path=policy_path,
        label_series_path=label_series_path,
        label_summary_path=label_summary_path,
        prices_path=prices_path,
        rates_path=rates_path,
        marketstack_prices_path=marketstack_prices_path,
        quality_as_of=quality_as_of,
        output_dir=output_dir,
        docs_root=docs_root,
        mode=mode,
    )
    _print_payload("Regime-segmented candidate validation", payload)


@trends_app.command("event-calendar-data-feasibility-audit")
def event_calendar_data_feasibility_audit_command(
    policy_path: Annotated[
        Path, typer.Option("--policy")
    ] = DEFAULT_EVENT_CALENDAR_FEASIBILITY_POLICY_PATH,
    output_dir: Annotated[
        Path, typer.Option("--output-dir")
    ] = DEFAULT_EVENT_CALENDAR_FEASIBILITY_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = DEFAULT_EVENT_CALENDAR_FEASIBILITY_DOCS_ROOT,
    mode: Annotated[str, typer.Option("--mode")] = EVENT_CALENDAR_FEASIBILITY_MODE,
) -> None:
    payload = run_event_calendar_data_feasibility_audit(
        policy_path=policy_path,
        output_dir=output_dir,
        docs_root=docs_root,
        mode=mode,
    )
    _print_payload("Event calendar data feasibility audit", payload)


@trends_app.command("event-calendar-gating-generator-poc")
def event_calendar_gating_generator_poc_command(
    policy_path: Annotated[
        Path, typer.Option("--policy")
    ] = DEFAULT_EVENT_GATING_GENERATOR_POLICY_PATH,
    feasibility_dir: Annotated[
        Path, typer.Option("--feasibility-dir")
    ] = DEFAULT_EVENT_GATING_GENERATOR_FEASIBILITY_ROOT,
    output_dir: Annotated[
        Path, typer.Option("--output-dir")
    ] = DEFAULT_EVENT_GATING_GENERATOR_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = DEFAULT_EVENT_GATING_GENERATOR_DOCS_ROOT,
    mode: Annotated[str, typer.Option("--mode")] = EVENT_GATING_GENERATOR_MODE,
) -> None:
    payload = run_event_calendar_gating_generator_poc(
        policy_path=policy_path,
        feasibility_dir=feasibility_dir,
        output_dir=output_dir,
        docs_root=docs_root,
        mode=mode,
    )
    _print_payload("Event calendar gating generator POC", payload)


@trends_app.command("event-gating-validation")
def event_gating_validation_command(
    policy_path: Annotated[
        Path, typer.Option("--policy")
    ] = DEFAULT_EVENT_GATING_VALIDATION_POLICY_PATH,
    generator_dir: Annotated[
        Path, typer.Option("--generator-dir")
    ] = DEFAULT_EVENT_GATING_VALIDATION_GENERATOR_ROOT,
    output_dir: Annotated[
        Path, typer.Option("--output-dir")
    ] = DEFAULT_EVENT_GATING_VALIDATION_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = DEFAULT_EVENT_GATING_VALIDATION_DOCS_ROOT,
    mode: Annotated[str, typer.Option("--mode")] = EVENT_GATING_VALIDATION_MODE,
) -> None:
    payload = run_event_gating_validation(
        policy_path=policy_path,
        generator_dir=generator_dir,
        output_dir=output_dir,
        docs_root=docs_root,
        mode=mode,
    )
    _print_payload("Event gating validation", payload)


@trends_app.command("first-layer-proxy-challenger-experiments")
def first_layer_proxy_challenger_experiments_command(
    policy_path: Annotated[Path, typer.Option("--policy")] = DEFAULT_PROXY_CHALLENGER_POLICY_PATH,
    current_state_summary_path: Annotated[
        Path, typer.Option("--current-state-summary")
    ] = DEFAULT_PROXY_CHALLENGER_CURRENT_STATE_SUMMARY_PATH,
    objective_validation_path: Annotated[
        Path, typer.Option("--objective-validation")
    ] = DEFAULT_PROXY_CHALLENGER_OBJECTIVE_VALIDATION_PATH,
    proxy_coverage_audit_path: Annotated[
        Path, typer.Option("--proxy-coverage-audit")
    ] = DEFAULT_PROXY_CHALLENGER_PROXY_COVERAGE_AUDIT_PATH,
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_FREE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path | None, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_FREE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_FREE_RATES_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_PROXY_CHALLENGER_OUTPUT_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = run_first_layer_proxy_challenger_experiments_pack(
        policy_path=policy_path,
        current_state_summary_path=current_state_summary_path,
        objective_validation_path=objective_validation_path,
        proxy_coverage_audit_path=proxy_coverage_audit_path,
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        output_root=output_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_payload("First-layer proxy challenger experiments", payload)


@trends_app.command("channel-specific-v4")
def channel_specific_first_layer_v4_command(
    freeze_contract_path: Annotated[
        Path, typer.Option("--freeze-contract")
    ] = DEFAULT_CHANNEL_V4_FREEZE_CONTRACT_PATH,
    gate_decision_path: Annotated[
        Path, typer.Option("--gate-decision")
    ] = DEFAULT_CHANNEL_V4_GATE_DECISION_PATH,
    owner_approval: Annotated[bool, typer.Option("--owner-approval/--no-owner-approval")] = False,
) -> None:
    payload = run_channel_specific_first_layer_v4_pack(
        freeze_contract_path=freeze_contract_path,
        gate_decision_path=gate_decision_path,
        owner_approval=owner_approval,
    )
    _print_payload("Channel-specific first-layer v4", payload)


@minimal_forward_diagnostic_app.callback()
def minimal_forward_diagnostic_command(
    ctx: typer.Context,
    policy_path: Annotated[Path, typer.Option("--policy")] = DEFAULT_MINIMAL_FORWARD_POLICY_PATH,
    schema_path: Annotated[Path, typer.Option("--schema")] = DEFAULT_MINIMAL_FORWARD_SCHEMA_PATH,
) -> None:
    if ctx.invoked_subcommand is not None:
        return
    payload = run_minimal_forward_diagnostic_pack(
        policy_path=policy_path,
        schema_path=schema_path,
    )
    _print_payload("Minimal forward diagnostic", payload)


@minimal_forward_diagnostic_app.command("outcome-backfill")
def minimal_forward_diagnostic_outcome_backfill_command(
    policy_path: Annotated[Path, typer.Option("--policy")] = DEFAULT_MINIMAL_FORWARD_POLICY_PATH,
    schema_path: Annotated[Path, typer.Option("--schema")] = DEFAULT_MINIMAL_FORWARD_SCHEMA_PATH,
) -> None:
    payload = run_minimal_forward_diagnostic_outcome_backfill(
        policy_path=policy_path,
        schema_path=schema_path,
    )
    _print_payload("Minimal forward diagnostic outcome backfill", payload)


trends_app.add_typer(minimal_forward_diagnostic_app, name="minimal-forward-diagnostic")


def _print_payload(label: str, payload: dict[str, object]) -> None:
    status = str(payload.get("status"))
    style = "green" if "READY" in status or "CANDIDATE" in status else "yellow"
    expected_blocked = (
        "PROMOTION_BLOCKED" in status
        or "ACTUAL_PATH_VALIDATION_BLOCKED" in status
        or "VALIDATION_BLOCKED" in status
        or "SOURCE_BLOCKED" in status
    )
    if "BLOCKED" in status and not expected_blocked:
        style = "red"
    console.print(f"[{style}]{label}: {status}[/{style}]")
    summary = payload.get("summary")
    if isinstance(summary, dict):
        compact = "; ".join(f"{key}={value}" for key, value in list(summary.items())[:8])
        if compact:
            console.print(compact)
    paths = payload.get("artifact_paths")
    if isinstance(paths, dict):
        for key, value in paths.items():
            console.print(f"{key}={value}")
    for field, expected in (
        ("promotion_allowed", False),
        ("paper_shadow_allowed", False),
        ("production_allowed", False),
        ("broker_action", "none"),
        ("dynamic_promotion_status", "BLOCKED"),
    ):
        console.print(f"{field}={payload.get(field, expected)}")
    if "BLOCKED" in status and not expected_blocked:
        raise typer.Exit(code=1)


def _parse_optional_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter("Date must use YYYY-MM-DD.") from exc
